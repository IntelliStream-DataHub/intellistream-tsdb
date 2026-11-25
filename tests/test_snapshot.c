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
#include "feed.h"
#include "replfile.h"
#include "replica.h"
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
    char name[96];           /* the name its marker gives it (replica_stage, eng_leader_file_open) */
    uint8_t *bytes;          /* its bytes, while REC is on (the install's replicas stage them) */
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
static __thread uint8_t *tl_pend_bytes[64];
static __thread char tl_pend_name[64][96];
static atomic_bool REC; /* record the log and the files (the install's reference replica is fed from them) */
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
    if (tl_npend < 64) {
        snprintf(tl_pend_name[tl_npend], sizeof tl_pend_name[0], "%s", mname);
        tl_pend_xxh[tl_npend] = xxh64(b, len, 0), tl_pend_size[tl_npend] = len;
        tl_pend_bytes[tl_npend++] = atomic_load(&REC) ? b : (free(b), NULL);
    } else {
        free(b);
    }
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
        CF[NCF] = (cfile){.pos = last, .size = tl_pend_size[i], .xxh = tl_pend_xxh[i], .bytes = tl_pend_bytes[i]};
        memcpy(CF[NCF++].name, tl_pend_name[i], sizeof CF[0].name);
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

/* The leader's log as the WAL writer handed it over (REC on): the install's reference replica gets 1..P from it, and
 * both replicas P + 1.. at the leader's segment and offset */
typedef struct {
    uint64_t seq, off, first, last;
    size_t len;
    uint8_t *b;
} chunk;
static pthread_mutex_t rec_mu = PTHREAD_MUTEX_INITIALIZER;
static chunk *CH;
static size_t NCH, CAPCH;

static void t_bytes(void *ctx, eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len, uint64_t first,
                    uint64_t last)
{
    (void)ctx;
    pthread_mutex_lock(&rec_mu);
    if (atomic_load(&REC)) {
        if (NCH == CAPCH) {
            CAPCH = CAPCH ? CAPCH * 2 : 1024;
            CH = realloc(CH, CAPCH * sizeof *CH);
            if (!CH)
                abort();
        }
        uint8_t *b = malloc(len ? len : 1);
        if (!b)
            abort();
        memcpy(b, buf, len);
        CH[NCH++] = (chunk){.seq = seq, .off = off, .first = first, .last = last, .len = len, .b = b};
    }
    pthread_mutex_unlock(&rec_mu);
    eng_leader_commit(db, last); /* after the record: P (the commit point) is always a chunk's end */
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
    for (size_t i = 0; i < NCF; i++)
        free(CF[i].bytes);
    NCF = NDC = 0;
    pthread_mutex_unlock(&bk_mu);
    pthread_mutex_lock(&rec_mu);
    for (size_t i = 0; i < NCH; i++)
        free(CH[i].b);
    NCH = 0;
    pthread_mutex_unlock(&rec_mu);
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

static bool PUT_NULLS;
static int put(eng *db, uint32_t s, int64_t ts0, int64_t step, size_t n)
{
    int64_t *ts = malloc(n * sizeof *ts);
    double *v = malloc(n * sizeof *v);
    if (!ts || !v)
        abort();
    uint64_t nul = type_null(TSDB_FLOAT64);
    for (size_t i = 0; i < n; i++) {
        ts[i] = ts0 + (int64_t)i * step, v[i] = (double)(ts0 / 1000 % 9973) + (double)i * 0.5;
        if (PUT_NULLS && (ts[i] / step) % 7 == 3) /* the install's comparison is row for row with nulls */
            memcpy(&v[i], &nul, sizeof nul);
    }
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
        struct stat a, b;
        if (f->link && (fstatat(dfd, e->d_name, &a, 0) != 0 || fstatat(dfd, m->files[f->link - 1].name, &b, 0) != 0 ||
                        a.st_ino != b.st_ino))
            ok = vfail("%s: %s is not a link of %s", dir, e->d_name, m->files[f->link - 1].name);
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
/* What the leader had at the cut (snap_test_cut, called under flush_lock and publish_lock): the verify requires the
 * snapshot to hold all of it, not only to hold nothing past P */
static struct {
    uint64_t p;
    char (*parts)[96];
    size_t nparts;
    uint64_t memseq[64], memlo[64]; /* memtables with rows: their segment and first position */
    size_t nmem;
    struct lkfile *lf;
    size_t nlf;
    int ncut;
} CUT;

static void note_cut(eng *db, uint64_t p, const char (*mparts)[96], size_t mn)
{
    free(CUT.parts);
    free(CUT.lf);
    int n = CUT.ncut;
    memset(&CUT, 0, sizeof CUT);
    CUT.ncut = n + 1, CUT.p = p;
    pthread_mutex_lock(&db->lock);
    /* the make's own list: a merge that listed its output since its db->lock section is not in the cut (it would
     * name a merged part the snapshot rightly holds as its sources: a red run with no bug, ~1 in 60) */
    size_t np = mparts ? mn : db->nparts;
    CUT.parts = malloc((np ? np : 1) * sizeof *CUT.parts);
    for (size_t i = 0; CUT.parts && i < np; i++)
        snprintf(CUT.parts[CUT.nparts++], sizeof CUT.parts[0], "%s", mparts ? mparts[i] : db->parts[i]->name);
    pthread_mutex_lock(&db->buf_lock);
    for (size_t i = 0; i <= db->nflushing && CUT.nmem < 64; i++) {
        memtable *mt = i < db->nflushing ? db->flushing[i] : db->active;
        if (mt && mt->rows)
            CUT.memseq[CUT.nmem] = mt->wal_seq, CUT.memlo[CUT.nmem++] = mt->fno_lo;
    }
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    if (leader_links_list(db, p, &CUT.lf, &CUT.nlf) != 0)
        CUT.lf = NULL, CUT.nlf = 0;
}

/* The bytes of the distinct inodes in a directory (SNAPMAN left out) */
static uint64_t distinct_bytes(const char *dir)
{
    uint64_t ino[4096], sum = 0;
    size_t n = 0;
    DIR *d = opendir(dir);
    struct dirent *e;
    while (d && (e = readdir(d))) {
        struct stat st;
        if (e->d_name[0] == '.' || strcmp(e->d_name, SNAP_MAN) == 0 || fstatat(dirfd(d), e->d_name, &st, 0) != 0)
            continue;
        bool seen = false;
        for (size_t i = 0; i < n; i++)
            seen |= ino[i] == (uint64_t)st.st_ino;
        if (!seen && n < 4096)
            ino[n++] = (uint64_t)st.st_ino, sum += (uint64_t)st.st_size;
    }
    if (d)
        closedir(d);
    return sum;
}

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
    /* complete: everything the leader had at the cut */
    if (CUT.p != p || !CUT.ncut) {
        ok = vfail("the cut was not noted (P %" PRIu64 ", noted %" PRIu64 ")", p, CUT.p);
    } else {
        for (size_t i = 0; i < CUT.nparts; i++) {
            const struct snap_file *f = man_find(&m, CUT.parts[i]);
            if (!f || f->kind != SNAP_PART)
                ok = vfail("part %s, listed at the cut, is not in the snapshot", CUT.parts[i]);
        }
        for (size_t i = 0; i < CUT.nmem; i++) {
            if (!CUT.memlo[i] || CUT.memlo[i] > p)
                continue; /* no committed row */
            char nm[64];
            wal_name(nm, sizeof nm, CUT.memseq[i]);
            const struct snap_file *f = man_find(&m, nm);
            if (!m.s_seq || m.s_seq > CUT.memseq[i] || !f || f->kind != SNAP_SEG)
                ok = vfail("a memtable of segment %" PRIx64 " held committed rows from %" PRIu64 " at the cut: S is %" PRIx64
                           ", %s is %sin the snapshot", CUT.memseq[i], CUT.memlo[i], m.s_seq, nm, f ? "" : "not ");
        }
        for (size_t i = 0; i < CUT.nlf; i++)
            if (!CUT.lf[i].keep && CUT.lf[i].fno > m.done && CUT.lf[i].fno <= p &&
                (!m.s_first || m.s_first > CUT.lf[i].fno))
                ok = vfail("the COMMIT at %" PRIu64 " (above done %" PRIu64 ") is below S's first position %" PRIu64,
                           CUT.lf[i].fno, m.done, m.s_first);
    }
    if (si->bytes != distinct_bytes(si->path))
        ok = vfail("the make counts %" PRIu64 " bytes, the directory's distinct files hold %" PRIu64, si->bytes,
                   distinct_bytes(si->path));
    close(dfd);
    snap_man_free(&m);
    return ok;
}

/* the epoch history every make is given (empty: P's epoch 0) */
static const struct snap_epoch *MK_HIST;
static size_t MK_NHIST;

/* snap_make, again while it is overtaken (at most 200 times) */
static int make(eng *db, const char *name, struct snap_info *si, int *retries)
{
    int rc;
    for (int i = 0; i < 200; i++) {
        memset(si, 0, sizeof *si);
        si->hist = MK_HIST, si->nhist = MK_NHIST;
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
    { /* the keep_ file is the published part's inode: listed as its link, its bytes counted once */
        struct snap_man m;
        size_t nlinks = 0;
        if (snap_man_read(si.path, &m) == 0) {
            for (size_t i = 0; i < m.nfiles; i++)
                nlinks += m.files[i].kind == SNAP_KEEP && m.files[i].link;
            snap_man_free(&m);
        }
        CHECK(nlinks >= 1 && si.bytes == distinct_bytes(si.path));
    }
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
        if (w == 0) { /* held across the whole make: its stage_ files go in; a flush meanwhile leaves its marker in an
                       * older segment than P's and the memtable's, so S goes down to it */
            CHECK(put(db, 3, T0 + 700000, 1000, 5) == 0);
            CHECK(eng_flush(db) == 0);
            CHECK(put(db, 3, T0 + 800000, 1000, 5) == 0);
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
    /* the large write is published now: its parts are listed, no stage_ file goes in (its marker is in a segment
     * below S since the flush above: the keep_ files are the basic case's) */
    struct snap_info si = {0};
    CHECK(make(db, "snap_wk", &si, NULL) == 0);
    CHECK(snap_verify_cut(&si) && si.nstage == 0);
    snap_release(si.path);
    eng_close(db);
    printf("  windows: a large write's stage_ files (its marker in an older segment than P's), then its parts; SERIES, STATES, RETENTION, CLASSES records past the "
           "cut's barrier\n");
}

/* Right after the cut a flush lists a part and a large write publishes: neither is in the snapshot */
static eng *HOOK_DB;

/* A flush and the keep bound at the commit point (what the driver does once every member caught up) at `GONE_AT`:
 * the memtable's segment goes. After the links (before the segments are listed) the make must answer SNAP_RETRY,
 * not CORRUPT; after the listing, its link finds it gone (SNAP_RETRY, never a snapshot without it). */
static int GONE_AT, gone_fired;
static void hook_segment_gone(eng *db, int at, const char *arg)
{
    (void)arg;
    if (at != GONE_AT || db != HOOK_DB || gone_fired++)
        return;
    CHECK(eng_flush(db) == 0);
    eng_leader_keep(db, atomic_load(&db->lead_commit));
}

static void case_segment_gone(int at)
{
    char dir[PATH_MAX];
    fresh(dir, at == SNAP_AT_LINKED ? "seggone_l" : "seggone_s", 4);
    bk_reset();
    eng *db = open_leader(dir, 0, 480);
    for (int s = 0; s < 4; s++)
        CHECK(put(db, (uint32_t)s, T0 + s, 1000, 10) == 0);
    CHECK(eng_flush(db) == 0);
    eng_leader_keep(db, atomic_load(&db->lead_commit));
    CHECK(put(db, 2, T0 + 200000, 1000, 5) == 0); /* memtable rows in the active segment */
    char seg[64], path[PATH_MAX + 64];
    pthread_mutex_lock(&db->buf_lock);
    wal_name(seg, sizeof seg, db->active->wal_seq);
    pthread_mutex_unlock(&db->buf_lock);
    HOOK_DB = db, GONE_AT = at, gone_fired = 0;
    atomic_store(&snap_test_hook, hook_segment_gone);
    struct snap_info si = {0};
    int retries = 0;
    int rc = make(db, "snap_gone", &si, &retries);
    atomic_store(&snap_test_hook, NULL);
    snprintf(path, sizeof path, "%s/%s", dir, seg);
    CHECK(gone_fired >= 1 && !exists(path)); /* the window was reached */
    if (!gone_fired || exists(path))
        fprintf(stderr, "  segment gone (hook %d): fired %d, %s %s\n", at, gone_fired, seg, exists(path) ? "still there" : "gone");
    CHECK(rc == 0 && retries == 1);
    if (rc == 0) {
        CHECK(snap_verify_cut(&si));
        snap_release(si.path);
    } else {
        fprintf(stderr, "  segment gone (hook %d): make rc %d after %d retries\n", at, rc, retries);
    }
    eng_close(db);
    printf("  segment gone %s: SNAP_RETRY, then a whole snapshot\n",
           at == SNAP_AT_LINKED ? "before the listing" : "after the listing");
}

/* Every part is linked before any is read: at each part's link nothing has been hashed yet */
static struct snap_info *LF_SI;
static int lf_parts;
static void hook_link_first(eng *db, int at, const char *arg)
{
    (void)arg;
    if (at != SNAP_AT_PART || db != HOOK_DB)
        return;
    lf_parts++;
    if (LF_SI->bytes != 0 || LF_SI->nparts != 0) {
        CHECK(LF_SI->bytes == 0 && LF_SI->nparts == 0);
        fprintf(stderr, "  link first: part %d linked after %" PRIu64 " bytes were read\n", lf_parts, LF_SI->bytes);
    }
}

static void case_link_first(void)
{
    char dir[PATH_MAX];
    fresh(dir, "linkfirst", 4);
    bk_reset();
    eng *db = open_leader(dir, 0, 480);
    for (int r = 0; r < 4; r++) {
        for (int s = 0; s < 4; s++)
            CHECK(put(db, (uint32_t)s, T0 + r * 20000 + s, 1000, 10) == 0);
        CHECK(eng_flush(db) == 0);
    }
    struct snap_info si = {0};
    HOOK_DB = db, LF_SI = &si, lf_parts = 0;
    atomic_store(&snap_test_hook, hook_link_first);
    int rc = snap_make(db, "snap_lf", &si);
    atomic_store(&snap_test_hook, NULL);
    CHECK(rc == 0 && lf_parts >= 3 && si.nparts == (size_t)lf_parts);
    if (rc == 0) {
        CHECK(snap_verify_cut(&si));
        snap_release(si.path);
    }
    eng_close(db);
    printf("  link first: %d parts linked before any was read\n", lf_parts);
}

/* snap_man_read refuses what a forged or damaged SNAPMAN could say (r4-snapinst reads one from another node) */
static int man_try(const char *dir, uint32_t nf_claim, const char *const *names, const uint8_t *kinds,
                   const uint32_t *links, const uint64_t *sizes, size_t n)
{
    uint8_t b[4096];
    size_t o = 0;
    memset(b, 0, sizeof b);
    memcpy(b, "TSNAPMAN", 8), o = 8;
    uint32_t v = SNAP_MAN_VERSION, z = 0;
    memcpy(b + o, &v, 4), o += 4;
    memcpy(b + o, &z, 4), o += 4; /* nhist */
    memcpy(b + o, &nf_claim, 4), o += 4;
    o += 4 + 8 + 16 + 6 * 8;
    for (size_t i = 0; i < n; i++) {
        uint16_t l = (uint16_t)strlen(names[i]);
        uint64_t sz = sizes ? sizes[i] : 10, x = 7;
        memcpy(b + o, &l, 2), o += 2;
        memcpy(b + o, names[i], l), o += l;
        b[o++] = kinds[i];
        memcpy(b + o, &sz, 8), o += 8;
        memcpy(b + o, &x, 8), o += 8;
        memcpy(b + o, &links[i], 4), o += 4;
    }
    uint64_t h = xxh64(b, o, 0);
    memcpy(b + o, &h, 8), o += 8;
    char path[PATH_MAX + 16];
    snprintf(path, sizeof path, "%s/%s", dir, SNAP_MAN);
    FILE *f = fopen(path, "wb");
    if (!f || fwrite(b, 1, o, f) != o)
        abort();
    fclose(f);
    struct snap_man m;
    int rc = snap_man_read(dir, &m);
    if (rc == 0)
        snap_man_free(&m);
    return rc;
}

static void case_man_bad(void)
{
    char dir[PATH_MAX + 16];
    snprintf(dir, sizeof dir, "%s/manbad", S);
    sh("mkdir -p '%s'", dir);
    const uint8_t pk[] = {SNAP_PART, SNAP_KEEP}, bad[] = {SNAP_PART, 9};
    const uint32_t nol[] = {0, 0}, l1[] = {0, 1}, lfwd[] = {2, 0};
    const uint64_t sz2[] = {10, 11};
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "keep_1_2.part"}, pk, l1, NULL, 2) == 0);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", ".."}, pk, nol, NULL, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "."}, pk, nol, NULL, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "SNAPMAN.tmp"}, pk, nol, NULL, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "p1.part"}, pk, nol, NULL, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "p2.part"}, bad, nol, NULL, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "keep_1_2.part"}, pk, lfwd, NULL, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 2, (const char *const[]){"p1.part", "keep_1_2.part"}, pk, l1, sz2, 2) == TSDB_ERR_CORRUPT);
    CHECK(man_try(dir, 0x7fffffff, (const char *const[]){"p1.part"}, pk, nol, NULL, 1) == TSDB_ERR_CORRUPT);
    /* the names the install writes: a copy that carried SNAPDONE would be taken for a finished install */
    for (const char *const *r = (const char *const[]){SNAP_DONE, "REPL", "FNO", "LOCK", "META.tmp", NULL}; *r; r++)
        if (man_try(dir, 2, (const char *const[]){"p1.part", *r}, pk, nol, NULL, 2) != TSDB_ERR_CORRUPT) {
            fprintf(stderr, "  SNAPMAN naming %s was not refused\n", *r);
            fails++;
        }
    printf("  SNAPMAN: \".\", \"..\", its own names, the install's names, a name twice, unknown kinds, bad links and a "
           "huge count refused\n");
}
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
    exit(0); /* not _exit: crashfs's power loss at exit (CRASHFS_AT_EXIT) runs in its destructor, which _exit skips */
}

static const char *MUTANT; /* --mutant NAME, handed to the power case's child */

static int run_child_as(const char *mode, const char *dir, const char *ackf, int warmup, int seed)
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
            if (!getenv("TS_CHILD_STDERR"))
                dup2(null, 2);
        }
        char t0[32];
        snprintf(t0, sizeof t0, "%" PRId64, T0);
        /* the planted bug goes along: the child is the process that makes the snapshot */
        execl(SELF, "test_snapshot", mode, dir, ackf, t0, MUTANT ? MUTANT : (char *)NULL, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int run_child(const char *dir, const char *ackf, int warmup, int seed)
{
    return run_child_as("--child", dir, ackf, warmup, seed);
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

/* ---- the install: snapshots made under writers, installed, opened as replicas ----
 *
 * The leader's log is recorded as its WAL writer hands it over (t_bytes, REC) and every COMMIT's file with its marker's
 * position (t_ship, t_wait). A reference replica, a copy of the leader's directory from before its first write, gets
 * exactly 1..P of that log (and the files of the markers in it); the installed snapshot must answer as it does, row for
 * row with nulls, and once both got P + 1..end (at the leader's segments and offsets) as the leader does. Promoted, the
 * installed replica serves the file of every COMMIT marker it holds. */

/* a path that must fit */
#define PJ(buf, ...)                                                                                                   \
    do {                                                                                                               \
        if (snprintf(buf, sizeof buf, __VA_ARGS__) >= (int)sizeof buf)                                                 \
            abort();                                                                                                   \
    } while (0)

static bool INST_NOVERIFY;

static bool ifail(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "  install: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    fails++;
    return false;
} /* --only install: the make's own verify is left out: a make mutant is caught here or not */

static int open_rep(const char *dir, uint64_t floor, eng **db)
{
    tsdb_options o;
    opts_of(&o, 0, 480);
    return eng_open_replica(dir, &o, floor, db);
}

static int cmp_chunk(const void *a, const void *b)
{
    const chunk *x = a, *y = b;
    return x->first < y->first ? -1 : x->first > y->first;
}

/* The recorded log, sorted; 0 unless it is every position from 1 on, once */
static size_t chunks_sorted(void)
{
    pthread_mutex_lock(&rec_mu);
    size_t n = NCH;
    pthread_mutex_unlock(&rec_mu);
    qsort(CH, n, sizeof *CH, cmp_chunk);
    for (size_t i = 0; i < n; i++)
        if (CH[i].first != (i ? CH[i - 1].last + 1 : 1) || CH[i].last < CH[i].first) {
            ifail("the recorded log is not whole at %" PRIu64 "-%" PRIu64, CH[i].first, CH[i].last);
            return 0;
        }
    return n;
}

/* Hands a replica the recorded log after `lo` up to `hi` (whole chunks: a cut is a chunk's end), the files of the COMMIT
 * markers among it staged first, and applies to hi */
static int feed_range(eng *r, uint64_t lo, uint64_t hi, size_t nch)
{
    int rc = 0;
    for (size_t i = 0; i < NCF && rc == 0; i++)
        if (CF[i].pos > lo && CF[i].pos <= hi)
            rc = CF[i].bytes ? replica_stage(r, CF[i].name, CF[i].bytes, CF[i].size) : TSDB_ERR_IO;
    for (size_t i = 0; i < nch && rc == 0; i++) {
        if (CH[i].last <= lo || CH[i].first > hi)
            continue;
        if (CH[i].first <= lo || CH[i].last > hi) {
            fprintf(stderr, "  install: the chunk %" PRIu64 "-%" PRIu64 " crosses %" PRIu64 " or %" PRIu64 "\n",
                    CH[i].first, CH[i].last, lo, hi);
            return TSDB_ERR_INVALID;
        }
        rc = replica_append(r, CH[i].seq, CH[i].off, CH[i].b, CH[i].len);
    }
    if (rc == 0)
        rc = replica_sync(r);
    if (rc == 0)
        rc = replica_apply(r, hi);
    return rc;
}

struct inst_snap {
    uint64_t p, p_epoch, s_first, done;
    int vol;
    size_t nstage, nkeep;
};

/* Copies the snapshot `sp` to <root>/v<N>.snap, as a receiver would have it */
static bool inst_copy(const char *sp, const char *root, struct inst_snap *is)
{
    struct snap_man m = {0};
    if (snap_man_read(sp, &m) != 0)
        return ifail("%s: no SNAPMAN to copy", sp);
    memset(is, 0, sizeof *is);
    is->vol = m.volume < 0 ? 0 : (int)m.volume;
    is->p = m.p, is->p_epoch = m.p_epoch, is->s_first = m.s_first, is->done = m.done;
    for (size_t i = 0; i < m.nfiles; i++)
        is->nstage += m.files[i].kind == SNAP_STAGE, is->nkeep += m.files[i].kind == SNAP_KEEP;
    snap_man_free(&m);
    sh("mkdir -p '%s' && rm -rf '%s/v%d.snap' && cp -a '%s' '%s/v%d.snap'", root, root, is->vol, sp, root, is->vol);
    return true;
}

static void ident_of(struct snap_ident *id, const struct snap_man *m, uint64_t rid)
{
    static const uint8_t zero[16];
    memset(id, 0, sizeof *id);
    if (memcmp(m->uuid, zero, 16) != 0)
        memcpy(id->volume_uuid, m->uuid, 16);
    else
        memset(id->volume_uuid, 0x5a, 16);
    memset(id->cluster_uuid, 0xc1, 16);
    id->node = 2, id->incarnation = 1, id->rid = rid;
}

/* snap_install of <root>/v<vol>.snap: a fresh learner's (keep NULL, rid) or over the REPL `keep` */
static int inst_do(const char *root, int vol, uint64_t rid, const struct replfile_state *keep)
{
    char st[PATH_MAX + 32];
    PJ(st, "%s/v%d.snap", root, vol);
    struct snap_man m = {0};
    int rc = snap_man_read(st, &m);
    if (rc)
        return rc;
    struct snap_ident id;
    ident_of(&id, &m, rid);
    if (keep) {
        memcpy(id.volume_uuid, keep->volume_uuid, 16), memcpy(id.cluster_uuid, keep->cluster_uuid, 16);
        id.node = keep->node, id.rid = keep->rid;
    }
    rc = snap_install(root, (uint32_t)vol, st, &id, keep);
    snap_man_free(&m);
    return rc;
}

static int load_repl(const char *vd, struct replfile_state *st)
{
    int dfd = open(vd, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return TSDB_ERR_IO;
    struct replfile *rf = NULL;
    bool torn = false;
    memset(st, 0, sizeof *st);
    int rc = replfile_load(dfd, &rf, st, &torn);
    if (rf)
        replfile_close(rf);
    close(dfd);
    return rc;
}

static int save_repl(const char *vd, const struct replfile_state *nst)
{
    int dfd = open(vd, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return TSDB_ERR_IO;
    struct replfile *rf = NULL;
    struct replfile_state cur;
    bool torn = false;
    int rc = replfile_load(dfd, &rf, &cur, &torn);
    if (rc == 0)
        rc = replfile_save(rf, nst);
    if (rf)
        replfile_close(rf);
    replfile_state_free(&cur);
    close(dfd);
    return rc;
}

typedef struct {
    int64_t ts[4096];
    uint64_t v[4096];
    size_t n;
} rows_t;

static int rfn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    rows_t *r = c;
    for (size_t i = 0; i < n && r->n < 4096; i++, r->n++)
        r->ts[r->n] = ts[i], memcpy(&r->v[r->n], (const uint8_t *)vals + i * 8, 8);
    return 0;
}

/* Says where two volumes differ: the series counts, then the first series whose rows differ */
static void diff_vols(eng *a, eng *b, const char *an, const char *bn)
{
    uint32_t na = eng_series_count(a), nb = eng_series_count(b);
    if (na != nb)
        fprintf(stderr, "    %s has %u series, %s %u\n", an, na, bn, nb);
    static rows_t ra, rb;
    for (uint32_t s = 0; s < MIN(na, nb); s++) {
        ra.n = rb.n = 0;
        int x = eng_query_raw_ex(a, s, T0 - 400 * DAY_MS, T0 + 400 * DAY_MS, false, 0, rfn, &ra);
        int y = eng_query_raw_ex(b, s, T0 - 400 * DAY_MS, T0 + 400 * DAY_MS, false, 0, rfn, &rb);
        size_t k = 0;
        while (k < ra.n && k < rb.n && ra.ts[k] == rb.ts[k] && ra.v[k] == rb.v[k])
            k++;
        if (x != y || ra.n != rb.n || k < ra.n) {
            fprintf(stderr, "    series %u: %s %zu rows (%d), %s %zu rows (%d); first difference at row %zu: ts %" PRId64
                            " / %" PRId64 " (T0%+" PRId64 " ms)\n", s, an, ra.n, x, bn, rb.n, y, k,
                    k < ra.n ? ra.ts[k] : -1, k < rb.n ? rb.ts[k] : -1, (k < ra.n ? ra.ts[k] : rb.ts[k]) - T0);
            return;
        }
    }
}

/* The hash of answers() over the series below n only */
static uint64_t answers_n(eng *db, uint32_t n)
{
    uint64_t h = 1;
    for (uint32_t s = 0; s < n; s++) {
        uint64_t hs = 0;
        CHECK(eng_query_raw_ex(db, s, T0 - 400 * DAY_MS, T0 + 400 * DAY_MS, false, 0, hfn, &hs) == 0);
        h = xxh64(&hs, sizeof hs, h ^ s);
    }
    return h;
}

/* Whether the installed volume x answers as the reference r: row for row on r's series; a series x has beyond them
 * must be empty. SERIES is copied to its length after the cut (the file comment's "dictionaries"), so a series whose
 * marker is past P can be there, with its own id and no rows: its marker, applied later, finds it (db.c, by id). */
static bool same_at_p(eng *x, eng *r, size_t *extra)
{
    uint32_t nx = eng_series_count(x), nr = eng_series_count(r);
    if (nx < nr || answers_n(x, nr) != answers_n(r, nr))
        return false;
    for (uint32_t s = nr; s < nx; s++) {
        size_t n = 0;
        if (eng_query_raw_ex(x, s, T0 - 400 * DAY_MS, T0 + 400 * DAY_MS, false, 0, cfn, &n) != 0 || n != 0)
            return false;
        (*extra)++;
    }
    return true;
}

#define NINST 6
static void case_install(void)
{
    char dir[PATH_MAX], ref[PATH_MAX + 16];
    fresh(dir, "inst", NW * 3 + 1);
    snprintf(ref, sizeof ref, "%s_ref", dir);
    sh("rm -rf '%s' && cp -a '%s' '%s'", ref, dir, ref);
    bk_reset();
    static const struct snap_epoch hist[] = {{2, 1}};
    MK_HIST = hist, MK_NHIST = 1;
    PUT_NULLS = true;
    atomic_store(&REC, true);
    RDB = open_leader(dir, 2, 480);
    for (int k = 0; k < 16; k++)
        atomic_store(&nops[k], 0);
    struct inst_snap in[NINST];
    char roots[NINST][PATH_MAX + 32];
    int nin = 0, retries = 0;
    unsigned seed = 4242;
    atomic_store(&stop_writers, 0);
    pthread_t t[NW];
    for (int w = 0; w < NW; w++)
        pthread_create(&t[w], NULL, rwriter, (void *)(intptr_t)w);
    for (int i = 0; i < NINST; i++) {
        struct timespec d = {0, (long)(5 + rand_r(&seed) % 60) * 1000000L};
        nanosleep(&d, NULL);
        if (i % 2 == 1)
            eng_leader_keep(RDB, atomic_load(&RDB->lead_commit));
        eng_leader_replay_floor(RDB, eng_wal_durable_fno(RDB) + 1);
        struct snap_info si = {0};
        char name[32];
        snprintf(name, sizeof name, "snap_i%d", i);
        int rc = make(RDB, name, &si, &retries);
        CHECK(rc == 0);
        if (rc != 0)
            continue;
        if (!INST_NOVERIFY)
            CHECK(snap_verify_cut(&si));
        snprintf(roots[nin], sizeof roots[0], "%s_root%d", dir, nin);
        sh("rm -rf '%s'", roots[nin]);
        if (inst_copy(si.path, roots[nin], &in[nin]))
            nin++;
        snap_release(si.path);
    }
    atomic_store(&stop_writers, 1);
    for (int w = 0; w < NW; w++)
        pthread_join(t[w], NULL);
    atomic_store(&REC, false); /* t_bytes records under rec_mu: none is recording once chunks_sorted took it */
    size_t nch = chunks_sorted();
    CHECK(nch > 0);
    uint64_t end = nch ? CH[nch - 1].last : 0;
    uint64_t want = answers(RDB, NULL);
    eng *r = NULL;
    CHECK(open_rep(ref, 0, &r) == 0);
    uint64_t at = 0;
    size_t held = 0, served = 0, stage = 0, keep = 0, extra = 0;
    for (int i = 0; i < nin && r && nch; i++) {
        int rc = feed_range(r, at, in[i].p, nch);
        CHECK(rc == 0);
        at = in[i].p;
        stage += in[i].nstage, keep += in[i].nkeep;
        char vd[PATH_MAX + 48];
        PJ(vd, "%s/v%d", roots[i], in[i].vol);
        rc = inst_do(roots[i], in[i].vol, 1000 + (uint64_t)i, NULL);
        if (rc != 0) {
            ifail("install %d (P %" PRIu64 "): snap_install: %d", i, in[i].p, rc);
            continue;
        }
        eng *x = NULL;
        /* keep caps (design-cluster.md 8.3) that keep every segment: with none a segment goes once the floor passes
         * it, with its markers' keep_ files, and the promotion below could check only the markers after P */
        replica_keep_next((uint64_t)1 << 40, 7 * (uint64_t)DAY_MS);
        if ((rc = open_rep(vd, in[i].p, &x)) != 0) {
            ifail("install %d (P %" PRIu64 "): the installed directory does not open as a replica: %d", i, in[i].p, rc);
            continue;
        }
        if (replica_applied_end(x) != in[i].p || replica_log_end(x) != in[i].p)
            ifail("install %d: applied end %" PRIu64 ", log end %" PRIu64 ", P %" PRIu64, i, replica_applied_end(x),
                  replica_log_end(x), in[i].p);
        if (!same_at_p(x, r, &extra))
            ifail("install %d (P %" PRIu64 ", S %" PRIu64 ", done %" PRIu64 "): the installed volume differs from a "
                  "replica that received 1..P", i, in[i].p, in[i].s_first, in[i].done),
                diff_vols(x, r, "installed", "reference");
        if ((rc = feed_range(x, in[i].p, end, nch)) != 0)
            ifail("install %d: P + 1..%" PRIu64 " not taken: %d", i, end, rc);
        else if (answers(x, NULL) != want)
            ifail("install %d (P %" PRIu64 "): after P + 1..end it differs from the leader", i, in[i].p);
        /* promoted, it serves the file of every COMMIT marker it holds (the snapshot's keep_ and stage_ files and those
         * staged since) */
        uint64_t pos = 0;
        eng *n = NULL;
        rc = replica_epoch_begin(x, in[i].p_epoch + 1, "node-b", &pos);
        if (rc == 0)
            rc = replica_sync(x);
        if (rc == 0)
            rc = replica_apply(x, pos);
        if (rc == 0)
            rc = replica_promote(&x, pos, &n);
        if (rc == 0)
            rc = eng_leader_attach(n, &OPS);
        if (rc != 0)
            ifail("install %d: the promotion failed: %d", i, rc);
        uint64_t from = in[i].s_first ? in[i].s_first : in[i].p + 1;
        for (size_t j = 0; rc == 0 && j < NCF; j++) {
            if (CF[j].pos < from || CF[j].pos > end)
                continue;
            held++;
            int fd = eng_leader_file_open(n, CF[j].pos, CF[j].name);
            uint64_t sz = 0, h = 0;
            if (fd < 0) {
                ifail("install %d: promoted, it does not serve %s of the marker at %" PRIu64 " (P %" PRIu64 "): %d", i,
                      CF[j].name, CF[j].pos, in[i].p, fd);
                continue;
            }
            if (snap_xxh64_fd(fd, &sz, &h) != 0 || sz != CF[j].size || h != CF[j].xxh)
                ifail("install %d: promoted, it serves other bytes as %s", i, CF[j].name);
            else
                served++;
            close(fd);
        }
        eng_close(n ? n : x);
        sh("rm -rf '%s'", roots[i]);
    }
    if (r) {
        CHECK(feed_range(r, at, end, nch) == 0);
        if (answers(r, NULL) != want)
            ifail("the reference replica differs from the leader at the end");
        eng_close(r);
    }
    CHECK(nin == NINST && held > 0 && served == held);
    printf("  install: %d snapshots (%d retries) installed and opened, each equal to a replica fed 1..P and, fed "
           "P + 1..%" PRIu64 ", to the leader; %zu stage_ and %zu keep_ files in them (%zu empty series past P); promoted, "
           "%zu of %zu files served; "
           "ops: %ld small %ld large %ld delete %ld flush %ld series %ld state %ld retention %ld switch %ld drop\n",
           nin, retries, end, stage, keep, extra, served, held, nops[0], nops[1], nops[2], nops[3], nops[4], nops[5], nops[6],
           nops[7], nops[8]);
    eng_close(RDB);
    PUT_NULLS = false;
    MK_HIST = NULL, MK_NHIST = 0;
    bk_reset();
    sh("rm -rf '%s' '%s'", dir, ref);
}

/* A small leader at epoch 3 whose snapshots A < B < C the keep and power cases install */
static eng *small_leader(char *dir, const char *name)
{
    fresh(dir, name, 4);
    bk_reset();
    static const struct snap_epoch hist[] = {{3, 1}};
    MK_HIST = hist, MK_NHIST = 1;
    eng *db = open_leader(dir, 0, 480);
    return db;
}

static void small_rows(eng *db, int k)
{
    for (int s = 0; s < 4; s++)
        CHECK(put(db, (uint32_t)s, T0 + k * 100000 + s, 1000, 5) == 0);
    CHECK(put(db, (uint32_t)k % 4, T0 + k * 100000 + 50000, 100, 40) == 0); /* a COMMIT */
    if (k % 2)
        CHECK(eng_flush(db) == 0);
}

static void small_snap(eng *db, const char *name, const char *root, struct inst_snap *is)
{
    struct snap_info si = {0};
    CHECK(make(db, name, &si, NULL) == 0);
    CHECK(inst_copy(si.path, root, is));
    snap_release(si.path);
}

static void case_install_keep(void)
{
    char dir[PATH_MAX], root[PATH_MAX + 16], vd[PATH_MAX + 32], sp[PATH_MAX + 32];
    eng *db = small_leader(dir, "ikeep");
    snprintf(root, sizeof root, "%s_root", dir);
    sh("rm -rf '%s'", root);
    struct inst_snap a, b, c;
    small_rows(db, 0);
    small_snap(db, "snap_a", root, &a);
    CHECK(inst_do(root, a.vol, 555, NULL) == 0);
    snprintf(vd, sizeof vd, "%s/v%d", root, a.vol);
    snprintf(sp, sizeof sp, "%s/v%d.snap", root, a.vol);
    struct replfile_state st;
    CHECK(load_repl(vd, &st) == 0);
    CHECK(st.rid == 555 && st.open_gen == 0 && st.fence == 3 && st.led == 0 && st.floor == a.p && !st.unsure);
    CHECK(a.p_epoch == 3);
    /* the replica since: a higher fence, led below it, a raised open generation */
    st.fence = 6, st.led = 1, st.open_gen = 77, st.open_nonce = 99;
    CHECK(save_repl(vd, &st) == 0);
    replfile_state_free(&st);
    struct replfile_state kp;
    CHECK(load_repl(vd, &kp) == 0);
    small_rows(db, 1);
    small_snap(db, "snap_b", root, &b);
    CHECK(b.p > a.p);
    /* a former leader's v<N> holds the snap_* directories of its makes until a writable open sweeps them: the
     * install removes v<N>.old with them (it reported IO after a durable swap, and snap_leftovers failed forever) */
    sh("mkdir -p '%s/snap_x' && echo x > '%s/snap_x/" SNAP_MAN "'", vd, vd);
    CHECK(inst_do(root, b.vol, 999, &kp) == 0);
    char vo[PATH_MAX + 48];
    snprintf(vo, sizeof vo, "%s.old", vd);
    CHECK(!exists(vo));
    CHECK(load_repl(vd, &st) == 0);
    if (st.rid != 555 || st.fence != 6 || st.led != 1 || st.open_gen != 77 || st.open_nonce != 99 || st.unsure ||
        st.floor != b.p)
        ifail("keep: installed over a fence 6, led 1 REPL: rid %" PRIu64 " fence %" PRIu64 " led %" PRIu64
              " gen %" PRIu64 " unsure %d floor %" PRIu64, st.rid, st.fence, st.led, st.open_gen, st.unsure, st.floor);
    st.unsure = true; /* as a torn load leaves it: the next save stores it */
    CHECK(save_repl(vd, &st) == 0);
    replfile_state_free(&st);
    replfile_state_free(&kp);
    CHECK(load_repl(vd, &kp) == 0);
    CHECK(kp.unsure);
    small_rows(db, 2);
    small_snap(db, "snap_c", root, &c);
    CHECK(inst_do(root, c.vol, 999, &kp) == 0);
    CHECK(load_repl(vd, &st) == 0);
    if (!st.unsure || st.open_gen != kp.open_gen + REPLFILE_OPEN_GEN_BELT || st.fence != 6 || st.led != 1 ||
        st.floor != c.p)
        ifail("keep: installed over an unsure REPL: unsure %d gen %" PRIu64 " (was %" PRIu64 ") fence %" PRIu64
              " led %" PRIu64, st.unsure, st.open_gen, kp.open_gen, st.fence, st.led);
    replfile_state_free(&st);
    eng *x = NULL;
    CHECK(open_rep(vd, c.p, &x) == 0);
    if (x)
        eng_close(x);
    /* a snapshot below the floor: refused, v<N> untouched, the staged copy left for snap_leftovers */
    CHECK(load_repl(vd, &st) == 0);
    replfile_state_free(&kp);
    CHECK(load_repl(vd, &kp) == 0);
    struct snap_info si = {0};
    CHECK(make(db, "snap_d", &si, NULL) == 0);
    sh("rm -rf '%s' && cp -a '%s' '%s'", sp, si.path, sp);
    snap_release(si.path);
    kp.floor = si.p + 1000;
    CHECK(inst_do(root, c.vol, 999, &kp) == TSDB_ERR_INVALID);
    replfile_state_free(&st);
    CHECK(load_repl(vd, &st) == 0 && st.floor == c.p);
    CHECK(exists(sp));
    CHECK(snap_leftovers(root, (uint32_t)c.vol) == 0);
    CHECK(!exists(sp) && exists(vd));
    char od[PATH_MAX + 48];
    snprintf(od, sizeof od, "%s.old", vd);
    sh("mkdir -p '%s/snap_y/d' && touch '%s/MANIFEST' '%s/snap_y/" SNAP_MAN "' '%s/snap_y/d/f'", od, od, od, od);
    CHECK(snap_leftovers(root, (uint32_t)c.vol) == 0);
    CHECK(!exists(od) && exists(vd));
    replfile_state_free(&st);
    replfile_state_free(&kp);
    /* a half directory left by a failed release: the next make of that name is refused (its mkdir finds it), a
     * release again removes it, and the make then works */
    CHECK(make(db, "snap_e", &si, NULL) == 0);
    char half[PATH_MAX + 32];
    snprintf(half, sizeof half, "%s", si.path);
    sh("cd '%s' && rm -f " SNAP_MAN " META", half);
    struct snap_info s2 = {0};
    CHECK(make(db, "snap_e", &s2, NULL) == TSDB_ERR_INVALID);
    CHECK(exists(half));
    CHECK(snap_release(half) == 0 && !exists(half));
    CHECK(make(db, "snap_e", &s2, NULL) == 0);
    if (!INST_NOVERIFY)
        CHECK(snap_verify_cut(&s2));
    snap_release(s2.path);
    printf("  install keep: fence 6 and led 1 kept over P's epoch 3, generation 77 kept; unsure kept with the belt; a "
           "cut below the floor refused; a half released name refused until released again\n");
    eng_close(db);
    MK_HIST = NULL, MK_NHIST = 0;
    sh("rm -rf '%s' '%s'", dir, root);
}

static void flip_at(const char *path, int64_t off) /* off < 0: from the end */
{
    int fd = open(path, O_RDWR | O_CLOEXEC);
    struct stat sb;
    uint8_t c = 0;
    if (fd < 0 || fstat(fd, &sb) != 0) {
        ifail("flip %s: %s", path, strerror(errno));
        if (fd >= 0)
            close(fd);
        return;
    }
    off_t o = off < 0 ? sb.st_size + off : off;
    if (pread(fd, &c, 1, o) != 1 || (c ^= 0x5a, pwrite(fd, &c, 1, o) != 1))
        ifail("flip %s at %lld", path, (long long)o);
    close(fd);
}

/* SNAPMAN's entry of `name` made to say the file's bytes as they are now, and SNAPMAN's own hash again: a copy whose
 * damage only the deeper checks can see */
static void man_fix(const char *dir, const char *name)
{
    char mp[PATH_MAX + 16], fp[PATH_MAX + 128];
    PJ(mp, "%s/%s", dir, SNAP_MAN);
    PJ(fp, "%s/%s", dir, name);
    FILE *f = fopen(mp, "rb");
    uint8_t b[1 << 16];
    size_t len = f ? fread(b, 1, sizeof b, f) : 0;
    if (f)
        fclose(f);
    size_t nl = strlen(name);
    bool found = false;
    for (size_t o = 0; !found && len > 8 && o + 2 + nl + 21 <= len - 8; o++) {
        uint16_t l;
        memcpy(&l, b + o, 2);
        if (l != nl || memcmp(b + o + 2, name, nl) != 0 || b[o + 2 + nl] < SNAP_PART || b[o + 2 + nl] > SNAP_META)
            continue;
        int fd = open(fp, O_RDONLY | O_CLOEXEC);
        uint64_t sz = 0, x = 0;
        if (fd < 0 || snap_xxh64_fd(fd, &sz, &x) != 0)
            ifail("man_fix: %s", fp);
        if (fd >= 0)
            close(fd);
        memcpy(b + o + 2 + nl + 1, &sz, 8);
        memcpy(b + o + 2 + nl + 9, &x, 8);
        found = true;
    }
    if (!found) {
        ifail("man_fix: no entry %s in %s", name, mp);
        return;
    }
    uint64_t h = xxh64(b, len - 8, 0);
    memcpy(b + len - 8, &h, 8);
    f = fopen(mp, "wb");
    if (!f || fwrite(b, 1, len, f) != len)
        ifail("man_fix: write %s", mp);
    if (f)
        fclose(f);
}

static bool meta_requires_both(const char *vd)
{
    char mp[PATH_MAX + 16], line[512];
    PJ(mp, "%s/META", vd);
    FILE *f = fopen(mp, "r");
    bool ok = false;
    while (f && fgets(line, sizeof line, f))
        if (strncmp(line, "requires=", 9) == 0)
            ok = strstr(line, "cluster") && strstr(line, "replica");
    if (f)
        fclose(f);
    return ok;
}

/* The install's verify one check at a time: a received copy damaged or foreign in one way is refused, nothing is swapped
 * in; then what a good install writes (FNO = P + 1, REPL's log start and fence, META's requires=), a keep of another
 * identity, a keep whose fence is below P's epoch, and v<N>.old without v<N> */
static void case_install_verify(void)
{
    char dir[PATH_MAX], root[PATH_MAX + 16], st[PATH_MAX + 32], vd[PATH_MAX + 32], od[PATH_MAX + 32], x[PATH_MAX + 128];
    eng *db = small_leader(dir, "iver");
    snprintf(root, sizeof root, "%s_root", dir);
    sh("rm -rf '%s'", root);
    for (int k = 0; k < 4; k++)
        small_rows(db, k);
    struct snap_info si = {0};
    CHECK(make(db, "snap_v", &si, NULL) == 0);
    eng_close(db);
    struct snap_man m = {0};
    CHECK(snap_man_read(si.path, &m) == 0);
    const char *seg = NULL, *dict = NULL, *pname = NULL, *link = NULL;
    for (size_t i = 0; i < m.nfiles; i++) {
        const struct snap_file *f = &m.files[i];
        bool target = false;
        for (size_t j = 0; j < m.nfiles; j++)
            target |= m.files[j].link == i + 1;
        if (f->kind == SNAP_SEG && !seg && f->size > 0)
            seg = f->name;
        if (f->kind == SNAP_DICT && !dict && f->size > 0)
            dict = f->name;
        if (f->kind == SNAP_PART && !pname && !target)
            pname = f->name;
        if (f->link && !link)
            link = f->name;
    }
    CHECK(seg && dict && pname && link);
    if (fails) {
        snap_man_free(&m);
        return;
    }
    struct inst_snap is;
    PJ(st, "%s/v0.snap", root);
    PJ(vd, "%s/v0", root);
    PJ(od, "%s/v0.old", root);
    int n = 0;
#define BAD(what, code, ...)                                                                                           \
    do {                                                                                                               \
        CHECK(inst_copy(si.path, root, &is) && is.vol == 0);                                                           \
        __VA_ARGS__;                                                                                                   \
        int rc_ = inst_do(root, 0, 555, NULL);                                                                         \
        if (rc_ != (code) || exists(vd))                                                                               \
            ifail("install verify: %s: rc %d (want %d), v0 %d", what, rc_, code, exists(vd));                          \
        sh("rm -rf '%s'", root);                                                                                       \
        n++;                                                                                                           \
    } while (0)
    BAD("a segment's byte flipped", TSDB_ERR_CORRUPT, PJ(x, "%s/%s", st, seg); flip_at(x, -1));
    BAD("a dictionary cut short", TSDB_ERR_CORRUPT, PJ(x, "%s/%s", st, dict); CHECK(truncate(x, (off_t)fsize_at(
                                                         AT_FDCWD, x) - 1) == 0));
    BAD("a file SNAPMAN does not name", TSDB_ERR_CORRUPT, sh("echo x > '%s/p99_1_1_0.part'", st));
    BAD("a file SNAPMAN names missing", TSDB_ERR_CORRUPT, sh("rm -f '%s/%s'", st, seg));
    BAD("a link entry a separate, damaged file", TSDB_ERR_CORRUPT,
        sh("cd '%s' && cp '%s' t.x && mv t.x '%s'", st, link, link); PJ(x, "%s/%s", st, link); flip_at(x, -1));
    BAD("a part's header damaged (SNAPMAN says its bytes)", TSDB_ERR_CORRUPT,
        PJ(x, "%s/%s", st, pname); flip_at(x, 0); man_fix(st, pname));
    {
        /* a granule's payload: only the granule crc tells */
        BAD("a granule's payload damaged (SNAPMAN says its bytes)", TSDB_ERR_CORRUPT, {
            int dfd = open(st, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            part *pp = NULL;
            CHECK(dfd >= 0 && part_open_as(dfd, pname, pname, false, false, &pp) == 0 && pp->ng > 0);
            granule g;
            uint64_t off = 0;
            if (pp) {
                gx_load(pp, 0, GW_LOC, &g);
                off = g.off + (g.ts_len + g.val_len) / 2;
                part_unref(pp);
            }
            if (dfd >= 0)
                close(dfd);
            PJ(x, "%s/%s", st, pname);
            flip_at(x, (int64_t)off);
            man_fix(st, pname);
        });
    }
    BAD("META of another volume_uuid", TSDB_ERR_INVALID,
        sh("cd '%s' && sed -i '/^volume_uuid=/d' META && echo volume_uuid=0123456789abcdef0123456789abcdef >> META",
           st);
        man_fix(st, "META"));
    BAD("META of another cluster", TSDB_ERR_INVALID,
        sh("cd '%s' && sed -i '/^cluster=/d' META && echo cluster=0123456789abcdef0123456789abcdef >> META", st);
        man_fix(st, "META"));
    {
        /* a snapshot of volume 0 installed as volume 1 */
        CHECK(inst_copy(si.path, root, &is));
        sh("mv '%s' '%s/v1.snap'", st, root);
        int rc = inst_do(root, 1, 555, NULL);
        PJ(x, "%s/v1", root);
        if (rc != TSDB_ERR_INVALID || exists(x))
            ifail("install verify: volume 0's snapshot as volume 1: rc %d, v1 %d", rc, exists(x));
        sh("rm -rf '%s'", root);
        n++;
    }
    /* a good install: what it wrote */
    CHECK(inst_copy(si.path, root, &is) && inst_do(root, 0, 555, NULL) == 0);
    struct replfile_state rs;
    CHECK(load_repl(vd, &rs) == 0);
    uint64_t ls = is.s_first ? is.s_first : is.p + 1;
    if (rs.floor != is.p || rs.log_start != ls || rs.fence != is.p_epoch || rs.rid != 555 || rs.led != 0)
        ifail("install verify: REPL floor %" PRIu64 " (P %" PRIu64 "), log start %" PRIu64 " (S %" PRIu64
              "), fence %" PRIu64 " (P's epoch %" PRIu64 "), rid %" PRIu64 ", led %" PRIu64,
              rs.floor, is.p, rs.log_start, ls, rs.fence, is.p_epoch, rs.rid, rs.led);
    {
        eng *fk = calloc(1, sizeof *fk);
        uint64_t next = 0, gen = 0;
        int slot = 0;
        bool torn = false;
        if (fk) {
            fk->dirfd = open(vd, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            fk->fno_fd = -1;
            if (fno_load(fk, &next, &gen, &slot, &torn) != 0 || next != is.p + 1 || torn)
                ifail("install verify: FNO says %" PRIu64 " (torn %d), P + 1 is %" PRIu64, next, torn, is.p + 1);
            if (fk->fno_fd >= 0)
                close(fk->fno_fd);
            close(fk->dirfd);
            free(fk);
        }
    }
    if (!meta_requires_both(vd))
        ifail("install verify: META does not require cluster and replica");
    /* a keep of another node, cluster or volume: refused, v0 untouched */
    for (int w = 0; w < 3; w++) {
        struct replfile_state kp;
        CHECK(load_repl(vd, &kp) == 0);
        struct snap_ident id;
        ident_of(&id, &m, 555);
        memcpy(id.volume_uuid, rs.volume_uuid, 16), memcpy(id.cluster_uuid, rs.cluster_uuid, 16), id.node = rs.node;
        if (w == 0)
            kp.node++;
        else if (w == 1)
            kp.cluster_uuid[3] ^= 1;
        else
            kp.volume_uuid[3] ^= 1;
        CHECK(inst_copy(si.path, root, &is));
        int rc = snap_install(root, 0, st, &id, &kp);
        if (rc != TSDB_ERR_INVALID || exists(od))
            ifail("install verify: a keep of another %s: rc %d", w == 0 ? "node" : w == 1 ? "cluster" : "volume", rc);
        replfile_state_free(&kp);
        CHECK(snap_leftovers(root, 0) == 0 && !exists(st));
        n++;
    }
    /* a keep whose fence is below P's epoch: the fence goes up to it (D6: never below the snapshot's epoch) */
    replfile_state_free(&rs);
    struct replfile_state kp;
    CHECK(load_repl(vd, &kp) == 0);
    kp.fence = 1, kp.led = 0; /* as handed in (replfile_save never lowers a fence: no REPL on disk says this) */
    CHECK(inst_copy(si.path, root, &is) && inst_do(root, 0, 999, &kp) == 0);
    CHECK(load_repl(vd, &rs) == 0);
    if (rs.fence != is.p_epoch || rs.led != 0 || rs.rid != 555)
        ifail("install verify: over a fence 1 REPL: fence %" PRIu64 " (P's epoch %" PRIu64 "), led %" PRIu64,
              rs.fence, is.p_epoch, rs.led);
    replfile_state_free(&rs);
    replfile_state_free(&kp);
    /* v0.old without v0 (a swap cut short): refused before anything is written, v0.old kept for snap_leftovers */
    sh("mv '%s' '%s'", vd, od);
    CHECK(inst_copy(si.path, root, &is));
    int rc = inst_do(root, 0, 555, NULL);
    PJ(x, "%s/" REPLFILE_NAME, od);
    if (rc != TSDB_ERR_INVALID || exists(vd) || !exists(x))
        ifail("install verify: v0.old without v0: rc %d, v0 %d, v0.old's REPL %d", rc, exists(vd), exists(x));
    PJ(x, "%s/" SNAP_DONE, st);
    CHECK(!exists(x));
    CHECK(snap_leftovers(root, 0) == 0 && exists(vd) && !exists(od) && !exists(st));
    n++;
#undef BAD
    printf("  install verify: %d damaged or foreign copies refused, nothing swapped in; FNO P + 1, REPL's log start, "
           "requires=, a fence raised to P's epoch\n", n);
    snap_man_free(&m);
    snap_release(si.path);
    MK_HIST = NULL, MK_NHIST = 0;
    sh("rm -rf '%s' '%s'", dir, root);
}

static const char *IACK;
static void ihook(int at)
{
    if (at == SNAP_INST_DONE)
        ack(IACK, "d"); /* SNAPDONE and its directory synced (a failed inst_done ends the child with 7) */
}

/* The power case's child: installs <root>/v0.snap over <root>/v0 (its REPL the keep), acks "d" once SNAPDONE is durable
 * and "i" once it returned 0 */
static int ichild(const char *root, const char *ackf)
{
    IACK = ackf;
    snap_inst_hook = ihook;
    char vd[PATH_MAX + 8];
    snprintf(vd, sizeof vd, "%s/v0", root);
    struct replfile_state kp;
    if (load_repl(vd, &kp) != 0)
        _exit(6);
    int rc = inst_do(root, 0, 999, &kp);
    if (rc != 0) {
        fprintf(stderr, "ichild: snap_install: %d (%s)\n", rc, strerror(errno));
        _exit(7);
    }
    ack(ackf, "i");
    exit(0); /* not _exit: the power loss at exit is crashfs's destructor (what a 0 promised must be on disk) */
}

/* After a crash: snap_leftovers, then v0 is the old directory whole (floor A, A's rows) or the new one whole */
static int ipower_step(const char *tmpl, const char *root, const char *ackf, int k, int seed, uint64_t pa,
                       uint64_t ha, uint64_t pb, uint64_t hb, int *olds, int *news, int *dones)
{
    sh("rm -rf '%s' && cp -a '%s' '%s'", root, tmpl, root);
    unlink(ackf);
    int code = run_child_as("--ichild", root, ackf, k, seed);
    if (code != 99 && code != 0) {
        ifail("install power: step %d: the child exited %d", k, code);
        return -1;
    }
    char a[8] = {0};
    int afd = open(ackf, O_RDONLY);
    if (afd >= 0) {
        (void)!read(afd, a, sizeof a - 1);
        close(afd);
    }
    char vd[PATH_MAX + 8], sn[PATH_MAX + 16], od[PATH_MAX + 16];
    PJ(vd, "%s/v0", root);
    PJ(sn, "%s/v0.snap", root);
    PJ(od, "%s/v0.old", root);
    /* a 0 from snap_install means the swap is durable as it stands: v0 is the new directory before any
     * snap_leftovers (a caller that opens v0 at once must not find the old one, or no v0) */
    if (strchr(a, 'i') && (!exists(vd) || exists(sn) || exists(od))) {
        ifail("install power: step %d seed %d: the install returned 0, after the power loss v0 %d, v0.snap %d, "
              "v0.old %d (the swap was not durable)", k, seed, exists(vd), exists(sn), exists(od));
        return -1;
    }
    int rc = snap_leftovers(root, 0);
    if (rc != 0 || exists(sn) || exists(od) || !exists(vd)) {
        ifail("install power: step %d seed %d: leftovers %d, v0.snap %d, v0.old %d, v0 %d", k, seed, rc, exists(sn),
              exists(od), exists(vd));
        return -1;
    }
    struct replfile_state st;
    if ((rc = load_repl(vd, &st)) != 0) {
        ifail("install power: step %d seed %d: v0's REPL does not load: %d (neither directory whole)", k, seed, rc);
        return -1;
    }
    uint64_t floor = st.floor, fence = st.fence;
    replfile_state_free(&st);
    eng *x = NULL;
    if ((rc = open_rep(vd, floor, &x)) != 0) {
        ifail("install power: step %d seed %d: v0 (floor %" PRIu64 ") does not open: %d", k, seed, floor, rc);
        return -1;
    }
    uint64_t h = answers(x, NULL);
    uint64_t le = replica_log_end(x);
    eng_close(x);
    bool old = floor == pa && h == ha && fence == 6 && le == pa, nw = floor == pb && h == hb && fence == 6 && le == pb;
    if (!old && !nw) {
        ifail("install power: step %d seed %d: v0 is neither whole (floor %" PRIu64 ", log end %" PRIu64
              ", fence %" PRIu64 ", rows %s)", k, seed, floor, le, fence, h == ha ? "old" : h == hb ? "new" : "other");
        return -1;
    }
    if (strchr(a, 'i') && !nw) {
        ifail("install power: step %d seed %d: the install returned 0, v0 is the old directory", k, seed);
        return -1;
    }
    if (strchr(a, 'd') && !nw) { /* SNAPDONE decides: once it is durable the install stands */
        ifail("install power: step %d seed %d: SNAPDONE was durable, v0 is the old directory after leftovers", k, seed);
        return -1;
    }
    *dones += strchr(a, 'd') && !strchr(a, 'i');
    *olds += old, *news += nw;
    return code;
}

static void case_install_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  install power: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX], tmpl[PATH_MAX + 16], root[PATH_MAX + 16], ackf[PATH_MAX + 16], scr[PATH_MAX + 16];
    eng *db = small_leader(dir, "ipow");
    snprintf(tmpl, sizeof tmpl, "%s_tmpl", dir);
    snprintf(root, sizeof root, "%s_root", dir);
    snprintf(scr, sizeof scr, "%s_scr", dir);
    snprintf(ackf, sizeof ackf, "%s.ack", dir);
    sh("rm -rf '%s' '%s'", tmpl, scr);
    struct inst_snap a, b;
    small_rows(db, 0);
    small_rows(db, 1);
    small_snap(db, "snap_a", tmpl, &a);
    CHECK(a.vol == 0);
    CHECK(inst_do(tmpl, 0, 555, NULL) == 0);
    char vd[PATH_MAX + 32];
    snprintf(vd, sizeof vd, "%s/v0", tmpl);
    struct replfile_state st;
    CHECK(load_repl(vd, &st) == 0);
    st.fence = 6, st.led = 1, st.open_gen = 5;
    CHECK(save_repl(vd, &st) == 0);
    replfile_state_free(&st);
    small_rows(db, 2);
    small_rows(db, 3);
    small_snap(db, "snap_b", tmpl, &b);
    eng_close(db);
    /* what each directory answers: A's as the template has it, B's installed into a scratch root */
    uint64_t ha = 0, hb = 0;
    eng *x = NULL;
    sh("mkdir -p '%s' && cp -a '%s' '%s/va'", scr, vd, scr);
    char p[PATH_MAX + 48];
    snprintf(p, sizeof p, "%s/va", scr);
    CHECK(open_rep(p, a.p, &x) == 0);
    if (x)
        ha = answers(x, NULL), eng_close(x);
    sh("cp -a '%s/v0.snap' '%s/v0.snap'", tmpl, scr);
    CHECK(inst_do(scr, 0, 777, NULL) == 0);
    snprintf(p, sizeof p, "%s/v0", scr);
    x = NULL;
    CHECK(open_rep(p, b.p, &x) == 0);
    if (x)
        hb = answers(x, NULL), eng_close(x);
    CHECK(ha != hb);
    if (fails)
        return;
    int olds = 0, news = 0, crashes = 0, kend = -1, dones = 0;
    for (int k = 0; k < 2000 && kend < 0; k++)
        for (int sd = 0; sd < 3; sd++) {
            int code = ipower_step(tmpl, root, ackf, k, k * 17 + sd + 1, a.p, ha, b.p, hb, &olds, &news, &dones);
            if (code < 0)
                return;
            crashes += code == 99;
            if (code == 0)
                kend = k;
        }
    /* the step that ran whole loses power at its exit: more seeds there, each tearing another subset of the
     * root's unsynced renames (an install that skips the root's fsync keeps both renames in 1 seed of 4) */
    for (int sd = 3; kend > 0 && sd < 16; sd++) {
        int code = ipower_step(tmpl, root, ackf, kend, kend * 17 + sd + 1, a.p, ha, b.p, hb, &olds, &news, &dones);
        if (code < 0)
            return;
        crashes += code == 99;
    }
    CHECK(kend > 0 && olds > 0 && news > 0 && dones > 0);
    printf("  install power: %d steps, %d crashes: %d left the old directory whole, %d the new one (%d of them with "
           "SNAPDONE durable and the swap left to snap_leftovers)\n", kend + 1, crashes, olds, news, dones);
    MK_HIST = NULL, MK_NHIST = 0;
    sh("rm -rf '%s' '%s' '%s' '%s' '%s'", dir, tmpl, root, scr, ackf);
}

static void set_mutant(const char *m)
{
    static const struct {
        const char *name;
        int v;
    } t[] = {{"snapafterp", SNAP_MUT_AFTERP},       {"snapdictshort", SNAP_MUT_DICTSHORT},
             {"snapnokeep", SNAP_MUT_NOKEEP},       {"snapnostage", SNAP_MUT_NOSTAGE},
             {"snapnoretired", SNAP_MUT_NORETIRED}, {"snapnodirsync", SNAP_MUT_NODIRSYNC},
             {"snapnoneedseq", SNAP_MUT_NONEEDSEQ}, {"snapnosegretry", SNAP_MUT_NOSEGRETRY},
             {"snaplowerfence", SNAP_MUT_LOWERFENCE}, {"snapraiseled", SNAP_MUT_RAISELED},
             {"snapkeepunsure", SNAP_MUT_KEEPUNSURE}, {"snapswapfirst", SNAP_MUT_SWAPFIRST},
             {"snapinstnodirsync", SNAP_MUT_INSTNODIRSYNC},
             {"snapdroppart", SNAP_MUT_DROPPART}};
    for (size_t i = 0; i < sizeof t / sizeof t[0]; i++)
        if (strcmp(m, t[i].name) == 0) {
            snap_mutant = t[i].v;
            return;
        }
    fprintf(stderr, "unknown mutant %s\n", m);
    exit(2);
}

/* --bench MB DIR: the make time of a leader volume of about MB megabytes of parts (REPORT of r4-snapmake): series
 * written in calls of a million rows of noisy values (about 8 bytes a row), a memtable left with rows, then three makes,
 * each copied (cp -a) and installed into a root of its own (the install timed: r4-snapinst),
 * each released. Not part of make test. */
static void bench_part(void *ctx, const tsdb_part_info *pi)
{
    *(uint64_t *)ctx += pi->bytes;
}

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static bool bench_cold; /* --cold: every part's pages dropped from the cache before each make */

static void drop_cache(const char *dir)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    while (d && (e = readdir(d))) {
        size_t l = strlen(e->d_name);
        if (l < 6 || strcmp(e->d_name + l - 5, ".part") != 0)
            continue;
        int fd = openat(dirfd(d), e->d_name, O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            fdatasync(fd);
            posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
            close(fd);
        }
    }
    if (d)
        closedir(d);
}

static int bench(long mb)
{
    char dir[PATH_MAX];
    fresh(dir, "bench", 16);
    eng *db = open_leader(dir, 0, 480);
    const size_t n = 1000000;
    int64_t *ts = malloc(n * sizeof *ts);
    double *v = malloc(n * sizeof *v);
    if (!ts || !v)
        abort();
    uint64_t x = 88172645463325252ull, bytes = 0, rows = 0;
    double t0 = now_s();
    for (int64_t call = 0; bytes < (uint64_t)mb << 20; call++) {
        for (size_t i = 0; i < n; i++) {
            x ^= x << 13, x ^= x >> 7, x ^= x << 17;
            ts[i] = T0 + (call / 16) * (int64_t)n + (int64_t)i, v[i] = (double)(x >> 11) / 9007199254740992.0;
        }
        if (eng_insert_series(db, TSDB_FLOAT64, (uint32_t)(call % 16), ts, v, n) != 0)
            return fprintf(stderr, "bench: insert failed\n"), 1;
        rows += n;
        bytes = 0;
        eng_foreach_part(db, bench_part, &bytes);
    }
    CHECK(put(db, 3, T0 - 100000, 1, 1000) == 0); /* small writes: a memtable and its segment */
    printf("bench: %" PRIu64 " rows, %.1f MB of parts written in %.1f s\n", rows, (double)bytes / 1048576.0,
           now_s() - t0);
    for (int r = 0; r < 3; r++) {
        struct snap_info si = {0};
        if (bench_cold)
            drop_cache(dir);
        double a = now_s();
        int rc = snap_make(db, "snap_bench", &si);
        double t = now_s() - a;
        printf("bench: make %d%s: rc %d, %.3f s (%.0f MB/s of the %.1f MB read), %zu files (%zu parts, %zu segments, "
               "%zu keep_)\n", r, bench_cold ? " cold" : "", rc, t, (double)si.bytes / 1048576.0 / t,
               (double)si.bytes / 1048576.0, si.nfiles, si.nparts, si.nsegs, si.nkeep);
        if (rc == 0) {
            /* the install of that snapshot, copied for real the way a receiver has it (cp -a: no links into the
             * leader's volume): snap_install's checks of every file against SNAPMAN, META, FNO, REPL, SNAPDONE, swap */
            char iroot[PATH_MAX + 16], vd[PATH_MAX + 32];
            struct inst_snap is;
            snprintf(iroot, sizeof iroot, "%s_iroot", dir);
            sh("rm -rf '%s'", iroot);
            if (inst_copy(si.path, iroot, &is)) {
                snprintf(vd, sizeof vd, "%s/v%d.snap", iroot, is.vol);
                if (bench_cold)
                    drop_cache(vd);
                a = now_s();
                int irc = inst_do(iroot, is.vol, 1000 + (uint64_t)r, NULL);
                t = now_s() - a;
                printf("bench: install %d%s: rc %d, %.3f s (%.0f MB/s of the %.1f MB)\n", r, bench_cold ? " cold" : "",
                       irc, t, (double)si.bytes / 1048576.0 / t, (double)si.bytes / 1048576.0);
                CHECK(irc == 0);
            }
            sh("rm -rf '%s'", iroot);
            a = now_s();
            CHECK(snap_release(si.path) == 0);
            printf("bench: release %d: %.3f s\n", r, now_s() - a);
        }
    }
    free(ts);
    free(v);
    eng_close(db);
    return fails ? 1 : 0;
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
        atomic_store(&snap_test_cut, note_cut);
        return child(argv[2], argv[3]);
    }
    if ((argc == 5 || argc == 6) && strcmp(argv[1], "--ichild") == 0) {
        T0 = strtoll(argv[4], NULL, 10);
        if (argc == 6)
            set_mutant(argv[5]);
        meta_cluster_open_allow(true);
        return ichild(argv[2], argv[3]);
    }
    const char *mutant = NULL, *scratch = "/tmp";
    long bench_mb = 0;
    const char *only = NULL; /* --only install: the install's cases alone, without the make's verify */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else if (strcmp(argv[i], "--bench") == 0 && i + 1 < argc)
            bench_mb = atol(argv[++i]);
        else if (strcmp(argv[i], "--cold") == 0)
            bench_cold = true;
        else if (strcmp(argv[i], "--only") == 0 && i + 1 < argc)
            only = argv[++i];
        else
            scratch = argv[i];
    }
    if (mutant)
        set_mutant(mutant);
    MUTANT = mutant;
    atomic_store(&snap_test_cut, note_cut);
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
    if (bench_mb > 0) {
        T0 -= 30 * DAY_MS; /* a million rows a call at 1 ms: room before now */
        int rc = bench(bench_mb);
        sh("rm -rf '%s'", S);
        return rc;
    }
    printf("test_snapshot%s%s%s\n", mutant ? " --mutant " : "", mutant ? mutant : "", only ? " --only install" : "");
    meta_cluster_open_allow(true); /* an installed directory says requires=cluster */
    if (only && strcmp(only, "install") == 0) {
        INST_NOVERIFY = true;
        case_install();
        case_install_keep();
        case_install_verify();
        case_install_power();
        goto out;
    }
    case_xxh();
    case_basic_sweep();
    case_windows();
    case_after_cut();
    case_segment_gone(SNAP_AT_LINKED);
    case_segment_gone(SNAP_AT_SEGLIST);
    case_link_first();
    case_man_bad();
    case_merge(480);
    case_merge(0);
    case_random();
    case_power();
    case_install();
    case_install_keep();
    case_install_verify();
    case_install_power();
out:
    if (fails) {
        fprintf(stderr, "test_snapshot: %d checks failed (scratch kept: %s)\n", fails, S);
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_snapshot: ok\n");
    return 0;
}
