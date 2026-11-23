/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests of the make side of snapshots (src/snapshot.c; design-cluster.md 8.1, R4 D3/D4).
 *
 * The leader here is an engine with jr_ops that ship nothing and commit at once: `bytes` moves the commit point to the
 * end of every record written, `ship` notes the xxh64 of each COMMIT's file and `wait` (the marker is committed) files
 * them under the marker's position, and can hold the writing thread there (the windows below). snap_verify_cut checks a
 * snapshot against the cut it claims:
 *  - every file in the directory is named by SNAPMAN with its size and xxh64, and nothing else is there;
 *  - every part was listed at or below P: its rows' positions end at or below P, and it is not the file of a COMMIT
 *    whose marker lies above P;
 *  - the segments are whole records from S's first position to P's record end, numbered without a gap;
 *  - for every COMMIT marker in them: its files are there, as stage_ files when it is above RSTATE's `done`, as keep_
 *    files when at or below it (matched by size and xxh64);
 *  - every SERIES, STATES, RETENTION and CLASSES marker in them has its record in the dictionary copy, and so does
 *    every series and state the test created with its marker at or below P (those below S included).
 *
 * Cases: xxh (the streaming hash), sweep (snap_release, stale snap_* at open), windows (a large write between its
 * marker's commit and its parts_commit; a SERIES, STATES, RETENTION and CLASSES writer between its marker's commit and
 * its record; a flush and a large write right after the cut; a merge during the links, its parts by their retired_
 * names; a part deleted meanwhile, SNAP_RETRY and a retry), random (4 writers, flushes, merges, large inserts,
 * deletes, class switches, retention, new series and states, a drop; snapshots at random moments; the answers unchanged
 * across a make), power (crashfs, CRASHFS_TEAR, a power loss at every step of an open and a make: the snapshot is
 * whole or the next open removes it; the leader opens with every acknowledged row).
 *
 * --mutant NAME plants snapshot.c's bugs (snap_mutant): each must fail this test (the Makefile's loop).
 */
#define _GNU_SOURCE
#include "internal.h"
#include "marker.h"
#include "snapshot.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static char S[PATH_MAX];
static char SELF[PATH_MAX], SHIM[PATH_MAX];
static int fails;
static bool quiet_fail;

#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            if (!quiet_fail)                                                                                           \
                fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #c);                                        \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

#define DAY_MS 86400000LL
static int64_t T0; /* the data's time: an hour boundary a day ago */

static void sh(const char *fmt, ...)
{
    char cmd[PATH_MAX * 3];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    if (system(cmd) != 0) {
        fprintf(stderr, "failed: %s\n", cmd);
        exit(2);
    }
}

/* ---- the leader ---- */

typedef struct {
    uint64_t pos, size, xxh; /* a COMMIT's file: its marker's last position */
} cfile;
static pthread_mutex_t bk_mu = PTHREAD_MUTEX_INITIALIZER;
static cfile *CF;
static size_t NCF, CAPCF;
typedef struct {
    int kind; /* MK_SERIES, MK_STATE */
    uint32_t id;
    uint64_t pos;
} dchange;
static dchange *DC;
static size_t NDC, CAPDC;

static __thread uint64_t tl_pend_xxh[64], tl_pend_size[64];
static __thread size_t tl_npend;
static __thread bool tl_hold;     /* hold this thread in its next wait */
static __thread uint64_t tl_last; /* the last marker this thread waited for */

static pthread_mutex_t hold_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t hold_cv = PTHREAD_COND_INITIALIZER;
static int held, released;

static void hold_reset(void)
{
    pthread_mutex_lock(&hold_mu);
    held = released = 0;
    pthread_mutex_unlock(&hold_mu);
}

static void hold_release(void)
{
    pthread_mutex_lock(&hold_mu);
    released = 1;
    pthread_cond_broadcast(&hold_cv);
    pthread_mutex_unlock(&hold_mu);
}

static int t_ship(void *ctx, eng *db, const char *file, const char *mname, uint64_t size)
{
    (void)ctx, (void)mname;
    uint8_t *b;
    size_t len;
    if (read_file(db->dirfd, file, &b, &len) != 0 || len != size)
        return TSDB_ERR_IO;
    if (tl_npend < 64)
        tl_pend_xxh[tl_npend] = xxh64(b, len, 0), tl_pend_size[tl_npend++] = len;
    free(b);
    return 0;
}

static int t_wait(void *ctx, eng *db, uint64_t last)
{
    (void)ctx, (void)db;
    tl_last = last;
    pthread_mutex_lock(&bk_mu);
    for (size_t i = 0; i < tl_npend; i++) {
        if (NCF == CAPCF) {
            CAPCF = CAPCF ? CAPCF * 2 : 256;
            CF = realloc(CF, CAPCF * sizeof *CF);
            if (!CF)
                abort();
        }
        CF[NCF++] = (cfile){.pos = last, .size = tl_pend_size[i], .xxh = tl_pend_xxh[i]};
    }
    pthread_mutex_unlock(&bk_mu);
    tl_npend = 0;
    if (tl_hold) {
        tl_hold = false;
        pthread_mutex_lock(&hold_mu);
        held = 1;
        pthread_cond_broadcast(&hold_cv);
        while (!released)
            pthread_cond_wait(&hold_cv, &hold_mu);
        pthread_mutex_unlock(&hold_mu);
    }
    return 0;
}

static void t_bytes(void *ctx, eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len, uint64_t first,
                    uint64_t last)
{
    (void)ctx, (void)seq, (void)off, (void)buf, (void)len, (void)first;
    eng_leader_commit(db, last);
}

static const struct jr_ops OPS = {.ship = t_ship, .wait = t_wait, .bytes = t_bytes};

static void note_dict(int kind, uint32_t id, uint64_t pos)
{
    pthread_mutex_lock(&bk_mu);
    if (NDC == CAPDC) {
        CAPDC = CAPDC ? CAPDC * 2 : 256;
        DC = realloc(DC, CAPDC * sizeof *DC);
        if (!DC)
            abort();
    }
    DC[NDC++] = (dchange){.kind = kind, .id = id, .pos = pos};
    pthread_mutex_unlock(&bk_mu);
}

static void bk_reset(void)
{
    pthread_mutex_lock(&bk_mu);
    NCF = NDC = 0;
    pthread_mutex_unlock(&bk_mu);
}

/* Only a directory whose META says requires=replica attaches as a leader */
static void mark_replica(const char *dir)
{
    char p[PATH_MAX + 8], buf[4096];
    snprintf(p, sizeof p, "%s/META", dir);
    int fd = open(p, O_RDWR | O_APPEND);
    if (fd < 0)
        exit(2);
    ssize_t n = read(fd, buf, sizeof buf - 1);
    buf[n > 0 ? n : 0] = '\0';
    if (n >= 0 && !strstr(buf, "requires=replica")) {
        static const char line[] = "requires=replica\n";
        if (write(fd, line, sizeof line - 1) != (ssize_t)(sizeof line - 1) || fsync(fd) != 0)
            exit(2);
    }
    close(fd);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        (void)fsync(dfd);
        close(dfd);
    }
}

static void opts_of(tsdb_options *o, int merge_threads, int lifetime)
{
    tsdb_options_init(o);
    o->merge_threads = merge_threads;
    o->create_volumes = 0;
    o->wal_sync_ms = 0;
    o->wal_max_batch_rows = 16; /* more rows than this go straight to parts: a COMMIT marker */
    o->obsolete_part_lifetime_s = lifetime;
}

/* A fresh volume directory with `ns` float64 series s0.. */
static void fresh(char *dir, const char *name, int ns)
{
    CHECK(snprintf(dir, PATH_MAX, "%s/%s", S, name) < PATH_MAX);
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    tsdb_options o;
    opts_of(&o, 0, 480);
    eng *db;
    if (eng_open(dir, &o, &db) != 0)
        exit(2);
    for (int i = 0; i < ns; i++) {
        char nm[16];
        uint32_t id;
        snprintf(nm, sizeof nm, "s%d", i);
        if (eng_series(db, nm, TSDB_FLOAT64, &id) != 0 || id != (uint32_t)i)
            exit(2);
    }
    eng_close(db);
    mark_replica(dir);
}

static eng *open_leader(const char *dir, int merge_threads, int lifetime)
{
    tsdb_options o;
    opts_of(&o, merge_threads, lifetime);
    eng *db;
    if (eng_open(dir, &o, &db) != 0) {
        fprintf(stderr, "cannot open %s\n", dir);
        exit(2);
    }
    if (eng_leader_attach(db, &OPS) != 0) {
        fprintf(stderr, "cannot attach %s\n", dir);
        exit(2);
    }
    eng_leader_replay_floor(db, eng_wal_durable_fno(db) + 1);
    return db;
}

static int put(eng *db, uint32_t s, int64_t ts0, int64_t step, size_t n)
{
    int64_t *ts = malloc(n * sizeof *ts);
    double *v = malloc(n * sizeof *v);
    if (!ts || !v)
        abort();
    for (size_t i = 0; i < n; i++)
        ts[i] = ts0 + (int64_t)i * step, v[i] = (double)(ts0 / 1000 % 9973) + (double)i * 0.5;
    int rc = eng_insert_series(db, TSDB_FLOAT64, s, ts, v, n);
    free(ts);
    free(v);
    return rc;
}

static int hfn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    uint64_t *h = c;
    const uint8_t *v = vals;
    for (size_t i = 0; i < n; i++) { /* row by row: the same whatever pieces the query hands over */
        *h = xxh64(&ts[i], sizeof ts[i], *h);
        *h = xxh64(v + i * 8, 8, *h);
    }
    return 0;
}

/* A hash of every series' rows (float64 series only: states have no rows) */
static uint64_t answers(eng *db, size_t *rows)
{
    uint64_t h = 1;
    uint32_t n = eng_series_count(db);
    for (uint32_t s = 0; s < n; s++) {
        uint64_t hs = 0;
        int rc = eng_query_raw_ex(db, s, T0 - 400 * DAY_MS, T0 + 400 * DAY_MS, false, 0, hfn, &hs);
        CHECK(rc == 0);
        h = xxh64(&hs, sizeof hs, h ^ s);
    }
    if (rows)
        *rows = n;
    return h;
}

/* ---- the check of a cut ---- */

static bool vfail(const char *fmt, ...)
{
    if (quiet_fail)
        return false;
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "  verify: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    return false;
}

static const struct snap_file *man_find(const struct snap_man *m, const char *name)
{
    for (size_t i = 0; i < m->nfiles; i++)
        if (strcmp(m->files[i].name, name) == 0)
            return &m->files[i];
    return NULL;
}

static bool man_has(const struct snap_man *m, int kind, uint64_t size, uint64_t xxh)
{
    for (size_t i = 0; i < m->nfiles; i++)
        if (m->files[i].kind == kind && m->files[i].size == size && m->files[i].xxh64 == xxh)
            return true;
    return false;
}

static uint64_t fsize_at(int dfd, const char *name)
{
    struct stat sb;
    return fstatat(dfd, name, &sb, 0) == 0 ? (uint64_t)sb.st_size : 0;
}

static uint32_t dict_count(int dfd, const char *name)
{
    struct stat sb;
    if (fstatat(dfd, name, &sb, 0) != 0)
        return 0;
    series_dict d;
    if (series_open(&d, dfd, name, true, false) != 0)
        return 0;
    uint32_t n = atomic_load(&d.count);
    series_close(&d);
    return n;
}

/* Whether the directory holds exactly SNAPMAN and the files it names, each with its size and xxh64 */
static bool whole(const char *dir, struct snap_man *m)
{
    int rc = snap_man_read(dir, m);
    if (rc != 0)
        return vfail("%s: SNAPMAN: %d", dir, rc);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return vfail("%s: cannot open", dir);
    bool ok = true;
    size_t seen = 0;
    DIR *d = fdopendir(dup(dfd));
    struct dirent *e;
    while (d && (e = readdir(d))) {
        if (e->d_name[0] == '.' || strcmp(e->d_name, SNAP_MAN) == 0)
            continue;
        const struct snap_file *f = man_find(m, e->d_name);
        if (!f) {
            ok = vfail("%s: %s is not in SNAPMAN", dir, e->d_name);
            continue;
        }
        seen++;
        int fd = openat(dfd, e->d_name, O_RDONLY | O_CLOEXEC);
        uint64_t size = 0, xxh = 0;
        if (fd < 0 || snap_xxh64_fd(fd, &size, &xxh) != 0 || size != f->size || xxh != f->xxh64)
            ok = vfail("%s: %s: size %" PRIu64 " xxh %016" PRIx64 ", SNAPMAN says %" PRIu64 " %016" PRIx64, dir,
                       e->d_name, size, xxh, f->size, f->xxh64);
        if (fd >= 0)
            close(fd);
    }
    if (d)
        closedir(d);
    if (seen != m->nfiles)
        ok = vfail("%s: %zu of the %zu files SNAPMAN names are there", dir, seen, m->nfiles);
    close(dfd);
    return ok;
}

static int cmp_seq(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

/* The check of the file comment. `si` is what snap_make said. */
static bool snap_verify_cut(const struct snap_info *si)
{
    struct snap_man m = {0};
    bool ok = whole(si->path, &m);
    if (!m.files && !ok)
        return false;
    uint64_t p = si->p;
    if (m.p != p || m.done != si->done || m.s_first != si->s_first || m.s_seq != si->s_seq || m.p_seq != si->p_seq)
        ok = vfail("SNAPMAN's P %" PRIu64 " done %" PRIu64 " differ from the make's %" PRIu64 " %" PRIu64, m.p, m.done,
                   p, si->done);
    int dfd = open(si->path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) {
        snap_man_free(&m);
        return vfail("cannot open %s", si->path);
    }
    pthread_mutex_lock(&bk_mu);
    /* parts: listed at or below P */
    for (size_t i = 0; i < m.nfiles; i++) {
        const struct snap_file *f = &m.files[i];
        if (f->kind != SNAP_PART)
            continue;
        if (strncmp(f->name, "retired_", 8) == 0)
            ok = vfail("part %s linked under a retired_ name", f->name);
        part_header h;
        int fd = openat(dfd, f->name, O_RDONLY | O_CLOEXEC);
        if (fd < 0 || pread(fd, &h, sizeof h, 0) != (ssize_t)sizeof h)
            ok = vfail("part %s: cannot read its header", f->name);
        else if (h.fno_hi > p)
            ok = vfail("part %s holds rows up to position %" PRIu64 ", past P %" PRIu64, f->name, h.fno_hi, p);
        if (fd >= 0)
            close(fd);
        for (size_t k = 0; k < NCF; k++)
            if (CF[k].pos > p && CF[k].size == f->size && CF[k].xxh == f->xxh64)
                ok = vfail("part %s is the file of a COMMIT at %" PRIu64 ", past P %" PRIu64, f->name, CF[k].pos, p);
    }
    /* series and states the test made with their marker at or below P */
    uint32_t nseries = dict_count(dfd, "SERIES"), nstates = dict_count(dfd, "STATES");
    for (size_t k = 0; k < NDC; k++) {
        if (DC[k].pos > p)
            continue;
        if (DC[k].kind == MK_SERIES && DC[k].id >= nseries)
            ok = vfail("series %u (marker at %" PRIu64 ") is not in SERIES (%u)", DC[k].id, DC[k].pos, nseries);
        if (DC[k].kind == MK_STATE && DC[k].id >= nstates)
            ok = vfail("state %u (marker at %" PRIu64 ") is not in STATES (%u)", DC[k].id, DC[k].pos, nstates);
    }
    pthread_mutex_unlock(&bk_mu);
    /* the log */
    uint64_t seqs[4096];
    size_t ns = 0;
    for (size_t i = 0; i < m.nfiles && ns < 4096; i++) {
        uint64_t seq;
        if (m.files[i].kind == SNAP_SEG && sscanf(m.files[i].name, "wal_%" SCNx64 ".log", &seq) == 1)
            seqs[ns++] = seq;
    }
    qsort(seqs, ns, sizeof seqs[0], cmp_seq);
    if (ns != si->nsegs || (ns && (seqs[0] != m.s_seq || seqs[ns - 1] != m.p_seq)))
        ok = vfail("segments: %zu in SNAPMAN, from %" PRIx64 " to %" PRIx64 "; the make says %zu, %" PRIx64 " .. %" PRIx64,
                   ns, ns ? seqs[0] : 0, ns ? seqs[ns - 1] : 0, si->nsegs, m.s_seq, m.p_seq);
    uint64_t ret_len = fsize_at(dfd, "RETENTION"), cls_len = fsize_at(dfd, "CLASSES");
    uint64_t next = 0, lastpos = 0;
    size_t ncommit = 0;
    for (size_t i = 0; i < ns; i++) {
        char nm[64];
        wal_name(nm, sizeof nm, seqs[i]);
        uint8_t *b;
        size_t len;
        if (read_file(dfd, nm, &b, &len) != 0) {
            ok = vfail("%s: cannot read", nm);
            continue;
        }
        size_t o = 0;
        while (o < len) {
            wal_rec wr;
            size_t sz = wal_record(b + o, len - o, seqs[i], o, &wr);
            if (!sz)
                break;
            if (next && wr.fno != next && i && o == 0 && wr.fno < next)
                ok = vfail("%s: positions go back", nm);
            if (!next && wr.fno != m.s_first)
                ok = vfail("the first record is at %" PRIu64 ", S's first %" PRIu64, wr.fno, m.s_first);
            if (next && o && wr.fno != next)
                ok = vfail("%s: a gap at %" PRIu64 " (%" PRIu64 " expected)", nm, wr.fno, next);
            next = wr.fno + wr.span;
            lastpos = next - 1;
            if (wr.fno + wr.span - 1 > p)
                ok = vfail("%s: a record ends at %" PRIu64 ", past P %" PRIu64, nm, wr.fno + wr.span - 1, p);
            if (wr.kind == WAL_KIND_MARKER) {
                mk mm;
                if (mk_decode(wr.payload, wr.n, &mm) != 0) {
                    ok = vfail("%s: a marker at %" PRIu64 " does not decode", nm, wr.fno);
                } else if (mm.type == MK_COMMIT) {
                    bool above = wr.fno > m.done;
                    size_t pos = 0;
                    mk_file f;
                    ncommit++;
                    while (mk_file_next(&mm.u.commit, &pos, &f))
                        if (!man_has(&m, above ? SNAP_STAGE : SNAP_KEEP, f.size, f.xxh64))
                            ok = vfail("the COMMIT at %" PRIu64 " (%s done %" PRIu64 "): its file %.*s is not there as a "
                                       "%s file", wr.fno, above ? "above" : "at or below", m.done, (int)f.name_len, f.name,
                                       above ? "stage_" : "keep_");
                } else if (mm.type == MK_SERIES && mm.u.series.id >= nseries) {
                    ok = vfail("the SERIES marker at %" PRIu64 " names id %u: SERIES holds %u", wr.fno, mm.u.series.id,
                               nseries);
                } else if (mm.type == MK_STATE && mm.u.state.id >= nstates) {
                    ok = vfail("the STATE marker at %" PRIu64 " names id %u: STATES holds %u", wr.fno, mm.u.state.id,
                               nstates);
                } else if (mm.type == MK_RETENTION && mm.u.retention.record_off + 16 > ret_len) {
                    ok = vfail("the RETENTION marker at %" PRIu64 ": its record at %" PRIu64 " is not in the copy (%" PRIu64
                               " bytes)", wr.fno, mm.u.retention.record_off, ret_len);
                } else if (mm.type == MK_CLASS && mm.u.cls.record_off + 24 > cls_len) {
                    ok = vfail("the CLASS marker at %" PRIu64 ": its record at %" PRIu64 " is not in the copy (%" PRIu64
                               " bytes)", wr.fno, mm.u.cls.record_off, cls_len);
                }
            }
            o += sz;
        }
        if (o != len)
            ok = vfail("%s: %zu bytes, whole records end at %zu", nm, len, o);
        free(b);
    }
    if (ns && lastpos != p)
        ok = vfail("the segments end at %" PRIu64 ", P is %" PRIu64, lastpos, p);
    if (si->nstage + si->nkeep && !ncommit)
        ok = vfail("%zu stage_ and %zu keep_ files, no COMMIT in the segments", si->nstage, si->nkeep);
    close(dfd);
    snap_man_free(&m);
    return ok;
}

/* snap_make, again while it is overtaken (at most 200 times) */
static int make(eng *db, const char *name, struct snap_info *si, int *retries)
{
    int rc;
    for (int i = 0; i < 200; i++) {
        memset(si, 0, sizeof *si);
        rc = snap_make(db, name, si);
        if (rc != SNAP_RETRY)
            return rc;
        if (retries)
            (*retries)++;
    }
    return rc;
}

static bool exists(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0;
}

/* ------------------------------------------------------------ cases */

static void case_xxh(void)
{
    uint8_t buf[4096];
    for (size_t i = 0; i < sizeof buf; i++)
        buf[i] = (uint8_t)(i * 131 + (i >> 3) * 7 + 1);
    for (size_t len = 0; len <= 200; len++)
        for (size_t step = 1; step <= 40; step++)
            CHECK(snap_test_xxh64(buf, len, step) == xxh64(buf, len, 0));
    CHECK(snap_test_xxh64(buf, sizeof buf, 1000) == xxh64(buf, sizeof buf, 0));
    /* a file larger than the reader's buffer */
    char p[PATH_MAX + 16];
    snprintf(p, sizeof p, "%s/xxh", S);
    size_t n = 3u << 20;
    uint8_t *big = malloc(n);
    for (size_t i = 0; i < n; i++)
        big[i] = (uint8_t)(i * 2654435761u >> 13);
    int fd = open(p, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0 && write(fd, big, n) == (ssize_t)n);
    uint64_t size = 0, h = 0;
    CHECK(fd >= 0 && snap_xxh64_fd(fd, &size, &h) == 0 && size == n && h == xxh64(big, n, 0));
    if (fd >= 0)
        close(fd);
    unlink(p);
    free(big);
    printf("  xxh: the streaming hash is xxh64 for every length and piece size\n");
}

/* A plain make on a leader with parts, memtable rows, a published large write; snap_release; stale snap_* at open */
static void case_basic_sweep(void)
{
    char dir[PATH_MAX];
    fresh(dir, "basic", 4);
    bk_reset();
    eng *db = open_leader(dir, 0, 480);
    struct snap_info si = {0};
    CHECK(snap_make(db, "snap_empty", &si) == 0);
    CHECK(snap_verify_cut(&si));
    CHECK(snap_release(si.path) == 0 && !exists(si.path));
    CHECK(snap_make(db, "bad/name", &si) == TSDB_ERR_INVALID);
    CHECK(snap_make(db, "snapx", &si) == TSDB_ERR_INVALID);
    for (int s = 0; s < 4; s++)
        CHECK(put(db, (uint32_t)s, T0 + s, 1000, 10) == 0);
    CHECK(eng_flush(db) == 0);
    CHECK(put(db, 1, T0 + 100000, 1000, 40) == 0); /* a COMMIT, published: keep_ */
    CHECK(put(db, 2, T0 + 200000, 1000, 5) == 0);  /* memtable rows */
    size_t nr;
    uint64_t before = answers(db, &nr);
    CHECK(make(db, "snap_a", &si, NULL) == 0);
    CHECK(snap_verify_cut(&si));
    CHECK(si.nparts >= 2 && si.nsegs >= 1 && si.nkeep >= 1 && si.nstage == 0 && si.p > 0 && si.s_first > 0);
    CHECK(answers(db, NULL) == before);
    struct snap_info s2 = {0};
    CHECK(snap_make(db, "snap_a", &s2) == TSDB_ERR_INVALID); /* it exists */
    /* a half-made one: the next open removes both */
    char half[PATH_MAX + 16];
    snprintf(half, sizeof half, "%s/snap_half", dir);
    sh("mkdir -p '%s' && echo x > '%s/x'", half, half);
    CHECK(eng_close(db) == 0);
    CHECK(exists(si.path));
    tsdb_options o;
    opts_of(&o, 0, 480);
    CHECK(eng_open(dir, &o, &db) == 0);
    CHECK(!exists(si.path) && !exists(half));
    CHECK(answers(db, NULL) == before);
    CHECK(eng_close(db) == 0);
    printf("  basic: parts, keep_ files and the log up to P; answers unchanged; snap_* gone at open\n");
}

/* A large write held between its marker's commit and its parts_commit */
typedef struct {
    eng *db;
    int what; /* 0 large insert, 1 series, 2 state, 3 retention, 4 class switch */
    int rc;
    uint32_t id;
    atomic_int done;
} wjob;

static void *writer_held(void *a)
{
    wjob *j = a;
    tl_hold = true;
    switch (j->what) {
    case 0:
        j->rc = put(j->db, 0, T0 + 500000, 1000, 50);
        break;
    case 1:
        j->rc = eng_series(j->db, "held_series", TSDB_FLOAT64, &j->id);
        if (j->rc == 0)
            note_dict(MK_SERIES, j->id, tl_last);
        break;
    case 2:
        j->rc = eng_state(j->db, "held_state", true, &j->id);
        if (j->rc == 0)
            note_dict(MK_STATE, j->id - 1, tl_last) /* codes are ids + 1 */;
        break;
    case 3:
        j->rc = eng_set_retention(j->db, 1, 300 * DAY_MS);
        break;
    case 4:
        j->rc = eng_class_switch(j->db, 2, 2, NULL);
        break;
    }
    tl_hold = false;
    pthread_mutex_lock(&hold_mu);
    atomic_store(&j->done, 1);
    pthread_cond_broadcast(&hold_cv);
    pthread_mutex_unlock(&hold_mu);
    return NULL;
}

static bool wait_held(wjob *j)
{
    pthread_mutex_lock(&hold_mu);
    while (!held && !atomic_load(&j->done))
        pthread_cond_wait(&hold_cv, &hold_mu);
    bool h = held;
    pthread_mutex_unlock(&hold_mu);
    return h;
}

static void hook_release_at_cut(eng *db, int at, const char *arg)
{
    (void)db, (void)arg;
    if (at == SNAP_AT_CUT)
        hold_release();
}

static void *release_later(void *a)
{
    (void)a;
    struct timespec t = {0, 200 * 1000000L};
    nanosleep(&t, NULL);
    hold_release();
    return NULL;
}

static void case_windows(void)
{
    static const char *what[] = {"large write", "SERIES", "STATES", "RETENTION", "CLASSES"};
    char dir[PATH_MAX];
    fresh(dir, "windows", 4);
    bk_reset();
    eng *db = open_leader(dir, 0, 480);
    for (int s = 0; s < 4; s++)
        CHECK(put(db, (uint32_t)s, T0 + s, 1000, 10) == 0);
    for (int w = 0; w < 5; w++) {
        hold_reset();
        wjob j = {.db = db, .what = w};
        pthread_t t;
        pthread_create(&t, NULL, writer_held, &j);
        bool h = wait_held(&j);
        CHECK(h);
        if (!h) {
            fprintf(stderr, "  windows: the %s writer did not hold (rc %d)\n", what[w], j.rc);
            pthread_join(t, NULL);
            continue;
        }
        struct snap_info si = {0};
        char name[32];
        snprintf(name, sizeof name, "snap_w%d", w);
        int rc;
        pthread_t rl;
        if (w == 0) { /* held across the whole make: its stage_ files go in */
            rc = make(db, name, &si, NULL);
            hold_release();
        } else if (w < 4) { /* released once the cut is taken: the barrier waits for its record */
            atomic_store(&snap_test_hook, hook_release_at_cut);
            rc = make(db, name, &si, NULL);
            atomic_store(&snap_test_hook, NULL);
        } else { /* a switch holds flush_lock: the cut waits for it */
            pthread_create(&rl, NULL, release_later, NULL);
            rc = make(db, name, &si, NULL);
            pthread_join(rl, NULL);
        }
        pthread_join(t, NULL);
        CHECK(rc == 0 && j.rc == 0);
        if (rc == 0) {
            bool ok = snap_verify_cut(&si);
            CHECK(ok);
            if (!ok)
                fprintf(stderr, "  windows: the %s writer's cut failed\n", what[w]);
            if (w == 0)
                CHECK(si.nstage >= 1);
            snap_release(si.path);
        }
    }
    /* the large write is published now: its files go in as keep_ */
    struct snap_info si = {0};
    CHECK(make(db, "snap_wk", &si, NULL) == 0);
    CHECK(snap_verify_cut(&si) && si.nkeep >= 1 && si.nstage == 0);
    snap_release(si.path);
    eng_close(db);
    printf("  windows: a large write's stage_ files, then keep_; SERIES, STATES, RETENTION, CLASSES records past the "
           "cut's barrier\n");
}

/* Right after the cut a flush lists a part and a large write publishes: neither is in the snapshot */
static eng *HOOK_DB;
static void hook_write_after_cut(eng *db, int at, const char *arg)
{
    (void)arg;
    if (at != SNAP_AT_CUT || db != HOOK_DB)
        return;
    CHECK(put(db, 3, T0 + 900000, 1000, 6) == 0);
    CHECK(eng_flush(db) == 0);
    CHECK(put(db, 2, T0 + 950000, 1000, 30) == 0);
}

static void case_after_cut(void)
{
    char dir[PATH_MAX];
    fresh(dir, "aftercut", 4);
    bk_reset();
    eng *db = open_leader(dir, 0, 480);
    for (int s = 0; s < 4; s++)
        CHECK(put(db, (uint32_t)s, T0 + s, 1000, 10) == 0);
    CHECK(eng_flush(db) == 0);
    HOOK_DB = db;
    atomic_store(&snap_test_hook, hook_write_after_cut);
    struct snap_info si = {0};
    int rc = snap_make(db, "snap_ac", &si);
    atomic_store(&snap_test_hook, NULL);
    CHECK(rc == 0);
    if (rc == 0) {
        CHECK(snap_verify_cut(&si));
        snap_release(si.path);
    }
    eng_close(db);
    printf("  after the cut: a flush's part and a large write's part are left out\n");
}

/* A merge during the links: parts by their retired_ names; with lifetime 0 they are gone (SNAP_RETRY, then a retry) */
static int merge_hook_fired;
static void hook_merge_at_part(eng *db, int at, const char *arg)
{
    (void)arg;
    if (at != SNAP_AT_PART || db != HOOK_DB || merge_hook_fired++)
        return;
    CHECK(eng_optimize(db) == 0);
}

static void case_merge(int lifetime)
{
    char dir[PATH_MAX];
    fresh(dir, lifetime ? "merge" : "merge0", 2);
    bk_reset();
    eng *db = open_leader(dir, 0, lifetime);
    for (int k = 0; k < 4; k++) {
        CHECK(put(db, 0, T0 + k * 10000, 1000, 8) == 0);
        CHECK(put(db, 1, T0 + k * 10000, 1000, 8) == 0);
        CHECK(eng_flush(db) == 0);
    }
    uint64_t before = answers(db, NULL);
    HOOK_DB = db;
    merge_hook_fired = 0;
    atomic_store(&snap_test_hook, hook_merge_at_part);
    struct snap_info si = {0};
    int rc = snap_make(db, "snap_m", &si);
    atomic_store(&snap_test_hook, NULL);
    CHECK(merge_hook_fired >= 1);
    if (lifetime) {
        CHECK(rc == 0);
        if (rc == 0) {
            CHECK(snap_verify_cut(&si) && si.nparts >= 4);
            /* the parts linked are the retired ones */
            char rp[PATH_MAX + 128];
            struct snap_man m = {0};
            size_t retired = 0;
            if (snap_man_read(si.path, &m) == 0) {
                for (size_t i = 0; i < m.nfiles; i++) {
                    retired += snprintf(rp, sizeof rp, "%s/retired_%s", dir, m.files[i].name) < (int)sizeof rp &&
                               m.files[i].kind == SNAP_PART && exists(rp);
                }
                snap_man_free(&m);
            }
            CHECK(retired >= 2);
            snap_release(si.path);
        }
    } else {
        CHECK(rc == SNAP_RETRY);
        CHECK(!exists(si.path));
        int retries = 0;
        CHECK(make(db, "snap_m", &si, &retries) == 0 && retries == 0);
        CHECK(snap_verify_cut(&si));
        snap_release(si.path);
    }
    CHECK(answers(db, NULL) == before);
    eng_close(db);
    printf(lifetime ? "  merge during the links: the parts linked by their retired_ names\n"
                    : "  merge during the links, retired parts deleted at once: SNAP_RETRY, then a retry completes\n");
}

/* ---- random: 4 writers and snapshots at random moments ---- */

#define NW 4
static atomic_int stop_writers;
static atomic_long nops[16];
static eng *RDB;

static void *rwriter(void *a)
{
    int w = (int)(intptr_t)a;
    unsigned seed = 1234u + (unsigned)w * 77u;
    int64_t ts = T0 + (int64_t)w * 7;
    int nnew = 0;
    uint32_t own[3] = {(uint32_t)(w * 3), (uint32_t)(w * 3 + 1), (uint32_t)(w * 3 + 2)};
    while (!atomic_load(&stop_writers)) {
        int r = (int)(rand_r(&seed) % 100);
        uint32_t s = own[rand_r(&seed) % 3];
        int rc = 0, k;
        if (r < 45) {
            k = 0, rc = put(RDB, s, ts, 997, 1 + rand_r(&seed) % 8);
            ts += 8 * 997;
        } else if (r < 58) {
            k = 1, rc = put(RDB, s, ts, 331, 20 + rand_r(&seed) % 100);
            ts += 120 * 331;
        } else if (r < 63) {
            k = 2, rc = eng_delete_range(RDB, s, ts - 50000, ts - 20000);
        } else if (r < 70) {
            k = 3, rc = eng_flush(RDB);
        } else if (r < 75) {
            char nm[32];
            uint32_t id;
            snprintf(nm, sizeof nm, "w%d_n%d", w, nnew++);
            k = 4, rc = eng_series(RDB, nm, TSDB_FLOAT64, &id);
            if (rc == 0)
                note_dict(MK_SERIES, id, tl_last);
        } else if (r < 79) {
            char nm[32];
            uint32_t id;
            snprintf(nm, sizeof nm, "st%d_%d", w, nnew++);
            k = 5, rc = eng_state(RDB, nm, true, &id);
            if (rc == 0)
                note_dict(MK_STATE, id - 1, tl_last);
        } else if (r < 84) {
            k = 6, rc = eng_set_retention(RDB, s, (60 + rand_r(&seed) % 300) * DAY_MS);
        } else if (r < 89) {
            k = 7, eng_class_switch(RDB, s, (int)(rand_r(&seed) % 4), NULL); /* may be deferred: not counted */
        } else if (r < 91 && w == 0) {
            k = 8, rc = put(RDB, 12, T0 - 40 * DAY_MS + (int64_t)(rand_r(&seed) % 1000) * 1000, 1000, 3);
            if (rc == 0)
                rc = eng_drop_before(RDB, T0 - 38 * DAY_MS);
        } else {
            k = 0, rc = put(RDB, s, ts, 997, 2);
            ts += 2 * 997;
        }
        if (rc != 0) {
            fprintf(stderr, "  random: writer %d op %d: %d\n", w, k, rc);
            fails++;
            break;
        }
        atomic_fetch_add(&nops[k], 1);
    }
    return NULL;
}

static void case_random(void)
{
    char dir[PATH_MAX];
    fresh(dir, "random", NW * 3 + 1);
    bk_reset();
    RDB = open_leader(dir, 2, 480);
    for (int k = 0; k < 16; k++)
        atomic_store(&nops[k], 0);
    size_t made = 0, stage = 0, keep = 0, parts = 0, segs = 0;
    int retries = 0;
    unsigned seed = 99;
    for (int round = 0; round < 3; round++) {
        atomic_store(&stop_writers, 0);
        pthread_t t[NW];
        for (int w = 0; w < NW; w++)
            pthread_create(&t[w], NULL, rwriter, (void *)(intptr_t)w);
        for (int i = 0; i < 12; i++) {
            struct timespec d = {0, (long)(rand_r(&seed) % 30) * 1000000L};
            nanosleep(&d, NULL);
            if (i % 4 == 3) /* let the leader remove flushed segments: makes are overtaken */
                eng_leader_keep(RDB, atomic_load(&RDB->lead_commit));
            eng_leader_replay_floor(RDB, eng_wal_durable_fno(RDB) + 1);
            struct snap_info si = {0};
            char name[32];
            snprintf(name, sizeof name, "snap_r%d_%d", round, i);
            int rc = make(RDB, name, &si, &retries);
            CHECK(rc == 0);
            if (rc != 0)
                continue;
            bool ok = snap_verify_cut(&si);
            CHECK(ok);
            made++, stage += si.nstage, keep += si.nkeep, parts += si.nparts, segs += si.nsegs;
            snap_release(si.path);
        }
        atomic_store(&stop_writers, 1);
        for (int w = 0; w < NW; w++)
            pthread_join(t[w], NULL);
        /* quiet: the make changes no answer */
        uint64_t before = answers(RDB, NULL);
        struct snap_info si = {0};
        CHECK(make(RDB, "snap_q", &si, &retries) == 0);
        CHECK(snap_verify_cut(&si));
        CHECK(answers(RDB, NULL) == before);
        made++, stage += si.nstage, keep += si.nkeep;
        snap_release(si.path);
    }
    CHECK(made == 39 && parts > 0 && segs > 0 && keep > 0);
    printf("  random: %zu snapshots (%d retries) under 4 writers: %zu parts, %zu segments, %zu stage_, %zu keep_ files; "
           "ops: %ld small %ld large %ld delete %ld flush %ld series %ld state %ld retention %ld switch %ld drop\n",
           made, retries, parts, segs, stage, keep, nops[0], nops[1], nops[2], nops[3], nops[4], nops[5], nops[6], nops[7],
           nops[8]);
    eng_close(RDB);
}

/* ---- power loss at every step of an open and a make ---- */

static void ack(const char *f, const char *what)
{
    int fd = open(f, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
        _exit(5);
    if (write(fd, what, strlen(what)) < 0 || fsync(fd) != 0)
        _exit(5);
    close(fd);
}

static int child(const char *dir, const char *ackf)
{
    eng *db = open_leader(dir, 0, 480);
    if (put(db, 3, T0 + 3000000, 1000, 4) != 0)
        _exit(6);
    ack(ackf, "w");
    struct snap_info si = {0};
    if (snap_make(db, "snap_p", &si) != 0)
        _exit(7);
    ack(ackf, "m");
    _exit(0);
}

static const char *MUTANT; /* --mutant NAME, handed to the power case's child */

static int run_child(const char *dir, const char *ackf, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        char w[16], sd[16];
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        char t0[32];
        snprintf(t0, sizeof t0, "%" PRId64, T0);
        /* the planted bug goes along: the child is the process that makes the snapshot */
        execl(SELF, "test_snapshot", "--child", dir, ackf, t0, MUTANT ? MUTANT : (char *)NULL, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int cfn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    (void)ts, (void)vals;
    *(size_t *)c += n;
    return 0;
}

struct power_count {
    int crashes, wholes, removed;
};

/* One power loss at step k of the child's open and make: <0 when the test must stop, else the child's status (99 lost,
 * 0 finished). */
static int power_step(const char *tmpl, const char *dir, const char *ackf, const char *sp, int k, int seed,
                      struct power_count *pc)
{
    sh("rm -rf '%s' && cp -a '%s' '%s'", dir, tmpl, dir);
    unlink(ackf);
    int code = run_child(dir, ackf, k, seed);
    CHECK(code == 99 || code == 0);
    if (code != 99 && code != 0) {
        fprintf(stderr, "  power: step %d: the child exited %d\n", k, code);
        return -1;
    }
    pc->crashes += code == 99;
    char a[8] = {0};
    int afd = open(ackf, O_RDONLY);
    if (afd >= 0) {
        (void)!read(afd, a, sizeof a - 1);
        close(afd);
    }
    char man[PATH_MAX + 48];
    snprintf(man, sizeof man, "%s/" SNAP_MAN, sp);
    if (exists(man)) {
        struct snap_man m = {0};
        bool ok = whole(sp, &m);
        snap_man_free(&m);
        if (!ok)
            fprintf(stderr, "  power: step %d seed %d: SNAPMAN is there, the snapshot is not whole\n", k, seed);
        CHECK(ok);
        pc->wholes++;
    }
    bool had = exists(sp);
    tsdb_options o;
    opts_of(&o, 0, 480);
    eng *db = NULL;
    int rc = eng_open(dir, &o, &db);
    CHECK(rc == 0);
    if (rc != 0) {
        fprintf(stderr, "  power: step %d: the leader does not open: %d\n", k, rc);
        return -1;
    }
    CHECK(!exists(sp));
    pc->removed += had;
    size_t n = 0;
    CHECK(eng_query_raw_ex(db, 3, T0 + 3000000, T0 + 3010000, false, 0, cfn, &n) == 0);
    if (strchr(a, 'w'))
        CHECK(n == 4);
    eng_close(db);
    return fails ? -1 : code;
}

static void case_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power: skipped (no %s)\n", SHIM);
        return;
    }
    char tmpl[PATH_MAX], dir[PATH_MAX + 16], ackf[PATH_MAX + 16], sp[PATH_MAX + 32];
    fresh(tmpl, "power-template", 4);
    {
        eng *db = open_leader(tmpl, 0, 480);
        for (int s = 0; s < 4; s++)
            CHECK(put(db, (uint32_t)s, T0 + s, 1000, 10) == 0);
        CHECK(eng_flush(db) == 0);
        CHECK(put(db, 1, T0 + 100000, 1000, 40) == 0);
        CHECK(put(db, 2, T0 + 200000, 1000, 5) == 0);
        CHECK(eng_close(db) == 0);
    }
    snprintf(dir, sizeof dir, "%s/power", S);
    snprintf(ackf, sizeof ackf, "%s/power.ack", S);
    snprintf(sp, sizeof sp, "%s/snap_p", dir);
    /* Every step gets a few seeds; the last steps of the make (the links, RSTATE, META, SNAPMAN's temporary, its rename,
     * the directory's syncs) get many more: a step there is caught only when the tear keeps SNAPMAN's directory, its
     * temporary and its rename (one in eight) and cuts a link, so a missing directory sync before SNAPMAN
     * (snapnodirsync) needs dozens of seeds at the one step it shows at. */
    const int seeds = 2, tail_steps = 12, tail_seeds = 48;
    struct power_count pc = {0};
    int kend = -1;
    for (int k = 0; k < 3000 && kend < 0; k++)
        for (int sd = 0; sd < seeds; sd++) {
            int code = power_step(tmpl, dir, ackf, sp, k, k * 31 + sd + 1, &pc);
            if (code < 0)
                return;
            if (code == 0)
                kend = k;
        }
    int base = pc.crashes;
    for (int k = kend > tail_steps ? kend - tail_steps : 0; kend > 0 && k < kend; k++)
        for (int sd = seeds; sd < seeds + tail_seeds; sd++)
            if (power_step(tmpl, dir, ackf, sp, k, k * 31 + sd + 1, &pc) < 0)
                return;
    CHECK(kend > 0 && base > 50);
    printf("  power: lost at each of %d steps (%d seeds each, the last %d steps %d more) of an open and a make: %d "
           "snapshots found whole, %d directories removed at the open\n",
           base / seeds, seeds, tail_steps, tail_seeds, pc.wholes, pc.removed);
}

static void set_mutant(const char *m)
{
    static const struct {
        const char *name;
        int v;
    } t[] = {{"snapafterp", SNAP_MUT_AFTERP},       {"snapdictshort", SNAP_MUT_DICTSHORT},
             {"snapnokeep", SNAP_MUT_NOKEEP},       {"snapnostage", SNAP_MUT_NOSTAGE},
             {"snapnoretired", SNAP_MUT_NORETIRED}, {"snapnodirsync", SNAP_MUT_NODIRSYNC}};
    for (size_t i = 0; i < sizeof t / sizeof t[0]; i++)
        if (strcmp(m, t[i].name) == 0) {
            snap_mutant = t[i].v;
            return;
        }
    fprintf(stderr, "unknown mutant %s\n", m);
    exit(2);
}

int main(int argc, char **argv)
{
#ifdef __APPLE__
    printf("test_snapshot: skipped (crashfs and the leader's links are Linux only)\n");
    return 0;
#endif
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    if ((argc == 5 || argc == 6) && strcmp(argv[1], "--child") == 0) {
        T0 = strtoll(argv[4], NULL, 10);
        if (argc == 6)
            set_mutant(argv[5]);
        return child(argv[2], argv[3]);
    }
    const char *mutant = NULL, *scratch = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            scratch = argv[i];
    }
    if (mutant)
        set_mutant(mutant);
    MUTANT = mutant;
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/test-snapshot.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    T0 = ((int64_t)now.tv_sec * 1000 - DAY_MS) / 3600000 * 3600000;
    printf("test_snapshot%s%s\n", mutant ? " --mutant " : "", mutant ? mutant : "");
    case_xxh();
    case_basic_sweep();
    case_windows();
    case_after_cut();
    case_merge(480);
    case_merge(0);
    case_random();
    case_power();
    if (fails) {
        fprintf(stderr, "test_snapshot: %d checks failed (scratch kept: %s)\n", fails, S);
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_snapshot: ok\n");
    return 0;
}
