/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (src/ecgc.c, r7-gc). The objects are real: a settle by hand (chunk_be_put_object,
 * ec_index_write, part_open_ec, ec_swap_commit), so the .ec files are on disk and survive a reopen.
 *
 *   hold     obsolete_part_lifetime_s 0 and 1: a part a reader holds across its drop and deadline keeps its chunks and
 *            its reads succeed; once it lets go, the chunks go, then the retired name. Before the deadline nothing goes.
 *   sweep    an open queues the objects no file names (an abandoned settle's, one attached in memory only) and never a
 *            listed .ec's, a retired one's, or one a settle puts after the open; ecgc_queue_object.
 *   down     a store DOWN at the purge: the entry is kept, nothing unlinked; with the store back every chunk and the
 *            name go.
 *   power    power lost (crashfs, torn at random) at every step of an open (its sweep) and a purge: a listed object
 *            keeps every chunk, a retired name stays until its object's chunks are all gone; a clean open then removes
 *            the retired object and the orphan.
 *
 * Mutants (--mutant NAME; the Makefile requires each to fail): gcunlinkfirst gcunrefleak gcnohold sweeplive
 * sweepinflight (src/ecgc.h).
 */
#include "internal.h"
#include "chunk.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecpart.h"
#include "ecswap.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static const struct {
    const char *name;
    int m;
} MUTANTS[] = {{"none", GC_MUT_NONE},           {"gcunlinkfirst", GC_MUT_UNLINKFIRST}, {"gcunrefleak", GC_MUT_UNREFLEAK},
               {"gcnohold", GC_MUT_NOHOLD},     {"sweeplive", GC_MUT_SWEEPLIVE},
               {"sweepinflight", GC_MUT_SWEEPINFLIGHT}};

static const char *mutant_name(int m)
{
    for (size_t i = 0; i < sizeof MUTANTS / sizeof *MUTANTS; i++)
        if (MUTANTS[i].m == m)
            return MUTANTS[i].name;
    return "none";
}

#ifdef TSDB_EC
#define K 2
#define M 1
#define NST 3
#define ROWS 20000
static const int64_t DAY = 86400000;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX];
static bool quick;

static int sh(const char *fmt, ...)
{
    char cmd[4 * PATH_MAX];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

/* dir/db and dir/st0 .. st<NST-1>, each store initialized */
static void fresh(const char *what, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, what);
    sh("rm -rf '%s'; mkdir -p '%s/db'", dir, dir);
    for (int i = 0; i < NST; i++) {
        char st[PATH_MAX + 16];
        snprintf(st, sizeof st, "%s/st%d", dir, i);
        CHECK(mkdir(st, 0755) == 0);
        uint8_t id[16];
        CHECK(chunk_store_init(st, NULL, id) == 0);
    }
}

typedef struct h {
    tier *t;
    eng *db;
} h;

static bool use_thread; /* the tier's worker works the queue (thread_case); else the test does */

static int h_open(h *x, const char *dir, int lifetime_s)
{
    char spec[8192], db[PATH_MAX + 8];
    int n = snprintf(spec, sizeof spec, "k=%d,m=%d,cache_mb=64,dirs=", K, M);
    for (int i = 0; i < NST; i++)
        n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s/st%d", i ? ":" : "", dir, i);
    snprintf(db, sizeof db, "%s/db", dir);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    x->t = NULL;
    x->db = NULL;
    atomic_store(&ecgc_test_manual, !use_thread);
    int rc = tier_open(db, NULL, &x->t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (rc)
        return rc;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0; /* one key per day */
    o.obsolete_part_lifetime_s = lifetime_s;
    tier_want(x->t, 0);
    rc = eng_open(db, &o, &x->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(x->t);
        x->t = NULL;
    }
    return rc;
}

static void h_close(h *x)
{
    if (x->db)
        eng_close(x->db);
    tier_put(x->t);
    x->db = NULL;
    x->t = NULL;
}

/* purge, delete, purge: the retired names whose chunks went are unlinked */
static void h_gc(h *x)
{
    graveyard_purge(x->db, false);
    ecgc_tier_work(x->t);
    graveyard_purge(x->db, false);
}

static void write_day(eng *db, int d)
{
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    static int64_t ts[ROWS];
    static float v[ROWS];
    for (int i = 0; i < ROWS; i++)
        ts[i] = d * DAY + (int64_t)i * 3000, v[i] = (float)((d * 7919 + i) % 1013);
    CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, ROWS) == 0);
    CHECK(eng_flush(db) == 0);
}

/* the one listed part of day d, referenced */
static part *day_part(eng *db, int d)
{
    part *r = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == d) {
            CHECK(r == NULL);
            r = db->parts[i];
        }
    if (r)
        part_ref(r);
    pthread_mutex_unlock(&db->lock);
    CHECK(r != NULL);
    return r;
}

/* the positions of each object made here (k' + m: a small object has a smaller k') */
static struct {
    uint8_t oid[16];
    int n;
} known[64];
static int nknown;

static void note(const uint8_t oid[16], int n)
{
    if (nknown < 64) {
        memcpy(known[nknown].oid, oid, 16);
        known[nknown++].n = n;
    }
}

static int npos(const uint8_t oid[16])
{
    for (int i = 0; i < nknown; i++)
        if (!memcmp(known[i].oid, oid, 16))
            return known[i].n;
    return -1;
}

/* an object of the tier from bytes (random oid): what a settle puts before any file names it */
static void put_object(h *x, const uint8_t *b, size_t len, uint8_t oid[16])
{
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, K, M, len) == 0);
    uint64_t *sums = malloc(chunk_sums_total(&g) * sizeof *sums);
    chunk_random(oid, 16);
    CHECK(chunk_be_see_epoch(tier_backend(x->t), x->db->tier_vol, 1) == 0);
    CHECK(chunk_be_put_object(tier_backend(x->t), x->db->tier_vol, tier_vuuid(x->db), oid, &g, 1, b, ~0u, sums) == 0);
    note(oid, (int)(g.k + g.m));
    free(sums);
}

/* a settle by hand of day d's .part: the object, the .ec file, the swap. Its oid into oid; the .ec name into name. */
static void settle_day(h *x, int d, uint8_t oid[16], char name[96])
{
    eng *db = x->db;
    part *p = day_part(db, d);
    if (!p)
        return;
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, K, M, p->map_len) == 0);
    ec_record r;
    memset(&r, 0, sizeof r);
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    chunk_random(r.oid, sizeof r.oid);
    r.key = p->partition;
    r.min_block = p->min_block;
    r.max_block = p->max_block;
    r.level = p->level;
    r.k = (uint16_t)g.k;
    r.m = (uint16_t)g.m;
    r.cell_bytes = g.cell;
    r.object_len = p->map_len;
    r.object_xxh64 = xxh64(p->map, p->map_len, 0);
    r.writer_epoch = 1;
    r.settle_ms = eng_now_ms(db);
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    r.sums = malloc(r.nsums * sizeof *r.sums);
    CHECK(chunk_be_see_epoch(tier_backend(x->t), db->tier_vol, 1) == 0);
    CHECK(chunk_be_put_object(tier_backend(x->t), db->tier_vol, tier_vuuid(db), r.oid, &g, 1, p->map, ~0u, r.sums) ==
          0);
    char tmp[96];
    CHECK(ec_index_write(db, p, &r, tmp) == 0);
    snprintf(name, 96, "%.*s.ec", (int)(strlen(p->name) - 5), p->name);
    part *e = NULL;
    CHECK(part_open_ec(db, tmp, name, &e) == 0);
    if (e) {
        snprintf(e->pending, sizeof e->pending, "%s", tmp);
        part *in[1] = {p}, *ec[1] = {e};
        CHECK(ec_swap_commit(db, in, ec, 1) == 0);
        part_unref(e);
    }
    memcpy(oid, r.oid, 16);
    note(oid, (int)(g.k + g.m));
    free(r.sums);
    part_unref(p);
}

/* chunk files of oid in every store */
static int chunks(const char *dir, const uint8_t oid[16])
{
    char hex[33];
    chunk_oid_hex(oid, hex);
    int n = 0;
    for (int i = 0; i < NST; i++) {
        char sd[PATH_MAX + 32];
        snprintf(sd, sizeof sd, "%s/st%d", dir, i);
        DIR *d1 = opendir(sd);
        struct dirent *a;
        while (d1 && (a = readdir(d1))) {
            if (a->d_name[0] != 's' || !strcmp(a->d_name, "st"))
                continue;
            char vd[2 * PATH_MAX];
            snprintf(vd, sizeof vd, "%s/%s", sd, a->d_name);
            DIR *d2 = opendir(vd);
            struct dirent *b;
            while (d2 && (b = readdir(d2)))
                n += strstr(b->d_name, hex) != NULL && strncmp(b->d_name, "tmp", 3) != 0;
            if (d2)
                closedir(d2);
        }
        if (d1)
            closedir(d1);
    }
    return n;
}

static bool exists(const char *dir, const char *name)
{
    char p[2 * PATH_MAX];
    snprintf(p, sizeof p, "%s/db/%s", dir, name);
    return access(p, F_OK) == 0;
}

/* every granule of p through its object (the cache emptied first): 0 and the rows, or the first error */
static int read_all(h *x, part *p, uint64_t *rows)
{
    eccache_test_drop(x->t);
    codec_ctx *cx = codec_new();
    static int64_t ts[8192 + 64];
    static float v[8192 + 64];
    int rc = 0;
    *rows = 0;
    for (uint32_t i = 0; i < p->ix.n && rc == 0; i++) {
        granule g;
        rc = granule_read_at(cx, p, i, TSDB_FLOAT32, ts, v, 8192 + 64, false, &g);
        if (rc == 0)
            *rows += gx_count(p, i);
    }
    codec_free(cx);
    return rc;
}

/* ------------------------------------------------------------ hold */

static void hold_case(int lifetime)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16], oid2[16];
    fresh("hold", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, lifetime) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    write_day(x.db, 2);
    settle_day(&x, 0, oid, name);
    settle_day(&x, 1, oid2, name); /* name: day 1's */
    CHECK(chunks(dir, oid) == npos(oid));
    part *e = day_part(x.db, 0); /* a reader's reference, as a query's snapshot holds */
    CHECK(e && atomic_load(&e->ec) && !atomic_load(&e->ec)->test_mark);
    if (!e)
        return;
    snprintf(rname, sizeof rname, "retired_%s", e->name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    CHECK(exists(dir, rname));
    if (lifetime > 0)
        usleep((useconds_t)lifetime * 1000000 + 200000); /* the deadline passes while the reader holds the part */
    h_gc(&x);
    h_gc(&x);
    CHECK(chunks(dir, oid) == npos(oid)); /* held */
    CHECK(exists(dir, rname));
    uint64_t rows = 0;
    CHECK(read_all(&x, e, &rows) == 0 && rows == e->rows);
    part_unref(e);
    h_gc(&x);
    CHECK(chunks(dir, oid) == 0);
    CHECK(!exists(dir, rname));
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(c.unlinked == 1 && c.pending == 0 && c.holds == 0);

    /* no reader: the deadline alone */
    char rname2[128];
    snprintf(rname2, sizeof rname2, "retired_%s", name);
    CHECK(eng_drop_before(x.db, 2 * DAY) == 0);
    CHECK(exists(dir, rname2));
    if (lifetime > 0) {
        h_gc(&x);
        CHECK(chunks(dir, oid2) == npos(oid2) && exists(dir, rname2)); /* not due */
        usleep((useconds_t)lifetime * 1000000 + 200000);
    }
    h_gc(&x);
    CHECK(chunks(dir, oid2) == 0 && !exists(dir, rname2));
    h_close(&x);
    printf("  hold, lifetime %d s: a held part's chunks outlive its drop and deadline, its reads succeed; then the "
           "chunks go, then the name\n", lifetime);
}

/* ------------------------------------------------------------ sweep */

static void sweep_case(void)
{
    char dir[PATH_MAX], na[96], nb[96];
    uint8_t oa[16], ob[16], orphan[16], attached[16], late[16], queued[16];
    fresh("sweep", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, 3600) == 0);
    if (!x.db)
        return;
    for (int d = 0; d < 4; d++)
        write_day(x.db, d);
    settle_day(&x, 0, ob, nb); /* retired below, kept for its lifetime */
    settle_day(&x, 2, oa, na); /* stays listed */
    CHECK(eng_drop_before(x.db, DAY) == 0);
    char rb[128];
    snprintf(rb, sizeof rb, "retired_%s", nb);
    CHECK(exists(dir, rb));
    static uint8_t junk[300000];
    for (size_t i = 0; i < sizeof junk; i++)
        junk[i] = (uint8_t)(i * 31 + 7);
    put_object(&x, junk, sizeof junk, orphan); /* an abandoned settle */
    part *p3 = day_part(x.db, 3);
    ec_record rec;
    memset(&rec, 0, sizeof rec);
    CHECK(p3 && ec_test_attach(x.db, p3, &rec) == 0); /* in memory only: an orphan after the reopen */
    memcpy(attached, rec.oid, 16);
    note(attached, rec.k + rec.m);
    free(rec.sums);
    part_unref(p3);
    h_close(&x);

    CHECK(h_open(&x, dir, 3600) == 0);
    if (!x.db)
        return;
    put_object(&x, junk, sizeof junk, late); /* a settle that began after the open */
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(c.sweeps == 1 && c.sweeps_skipped == 0);
    h_gc(&x);
    ecgc_get_counters(x.t, &c);
    CHECK(c.swept == 2);
    CHECK(chunks(dir, orphan) == 0 && chunks(dir, attached) == 0);
    CHECK(chunks(dir, oa) == npos(oa) && chunks(dir, ob) == npos(ob) && exists(dir, rb));
    CHECK(chunks(dir, late) == npos(late));
    /* the late settle abandoned: it queues its own object */
    CHECK(ecgc_queue_object(x.db, late, (unsigned)npos(late)) == 0);
    memcpy(queued, late, 16);
    ecgc_tier_work(x.t);
    CHECK(chunks(dir, queued) == 0);
    uint64_t rows = 0;
    part *pa = day_part(x.db, 2);
    CHECK(pa && read_all(&x, pa, &rows) == 0 && rows == ROWS);
    part_unref(pa);
    h_close(&x);
    printf("  sweep: an open queued %" PRIu64 " orphans (an abandoned settle's, an attached object's) and left the "
           "listed, the retired and a later settle's object\n", c.swept);
}

/* ------------------------------------------------------------ down */

static void down_case(void)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16];
    fresh("down", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    h_close(&x); /* its close queued the object, which the tier's close left: the name stays */
    CHECK(exists(dir, rname) && chunks(dir, oid) == npos(oid));
    sh("mv '%s/st2' '%s/st2.away'", dir, dir);
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    h_gc(&x);
    h_gc(&x);
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(c.sweeps_skipped == 1 && c.retries >= 1 && c.deleted == 0);
    CHECK(exists(dir, rname));
    h_close(&x);
    sh("mv '%s/st2.away' '%s/st2'", dir, dir);
    CHECK(exists(dir, rname) && chunks(dir, oid) >= 1);
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    h_gc(&x);
    CHECK(chunks(dir, oid) == 0 && !exists(dir, rname));
    h_close(&x);
    printf("  down: a store DOWN keeps the entry and the name; back UP, every chunk goes, then the name\n");
}

/* ------------------------------------------------------------ thread */

/* the tier's own worker: a drop, and the chunks and the name go with no ecgc_tier_work from the test */
static void thread_case(void)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16];
    fresh("thread", dir, sizeof dir);
    use_thread = true;
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    use_thread = false;
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    int ms = 0;
    for (; ms < 20000 && (chunks(dir, oid) != 0 || exists(dir, rname)); ms += 10) {
        graveyard_purge(x.db, false); /* the flusher's, sooner */
        usleep(10000);
    }
    CHECK(chunks(dir, oid) == 0 && !exists(dir, rname));
    h_close(&x);
    printf("  thread: the tier's worker deleted a dropped part's object, then the purge its name (%d ms)\n", ms);
}

/* ------------------------------------------------------------ power */

static char PBASE[PATH_MAX], PNAME[128];
static uint8_t P_RET[16], P_LIST[16], P_ORPH[16];

static void make_power_base(void)
{
    char name[96];
    fresh("pbase", PBASE, sizeof PBASE);
    h x;
    CHECK(h_open(&x, PBASE, 0) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 1, P_LIST, name);
    settle_day(&x, 0, P_RET, name);
    snprintf(PNAME, sizeof PNAME, "retired_%s", name);
    static uint8_t junk[200000];
    memset(junk, 0x5a, sizeof junk);
    put_object(&x, junk, sizeof junk, P_ORPH);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    h_close(&x);
    CHECK(exists(PBASE, PNAME) && chunks(PBASE, P_RET) == npos(P_RET) && chunks(PBASE, P_ORPH) == npos(P_ORPH));
}

static int child(const char *dir)
{
    h x;
    if (h_open(&x, dir, 0) != 0)
        return 3;
    h_gc(&x);
    h_close(&x);
    return 0;
}

static int run_child(const char *dir, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
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
        execl(SELF, "test_ecgc", "--child", dir, "--mutant", mutant_name(ecgc_mutant), (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void power_case(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    make_power_base();
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/pl", S);
    int crashes = 0, seeds = quick ? 2 : 4;
    bool finished = false;
    for (int k = 0; k < 3000 && !finished; k++) {
        for (int sd = 0; sd < seeds; sd++) {
            sh("rm -rf '%s'; cp -a '%s' '%s'", dir, PBASE, dir);
            int code = run_child(dir, k, k * 7 + sd + 1);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0) {
                fprintf(stderr, "  (power loss at step %d: the child said %d)\n", k, code);
                k = 1 << 20;
                break;
            }
            crashes += code == 99;
            finished = code == 0;
            /* what the power left: the listed object whole; the retired name until its chunks are all gone */
            int lc = chunks(dir, P_LIST), rc = chunks(dir, P_RET);
            bool named = exists(dir, PNAME);
            CHECK(lc == npos(P_LIST));
            CHECK(named || rc == 0);
            if (lc != npos(P_LIST) || !(named || rc == 0)) {
                fprintf(stderr, "  (power loss at step %d, seed %d: listed %d chunks, retired %d chunks, name %d)\n",
                        k, sd, lc, rc, named);
                k = 1 << 20;
                break;
            }
            /* a clean open finishes the job */
            h x;
            CHECK(h_open(&x, dir, 0) == 0);
            if (x.db) {
                h_gc(&x);
                h_close(&x);
            }
            CHECK(chunks(dir, P_RET) == 0 && !exists(dir, PNAME) && chunks(dir, P_ORPH) == 0);
            CHECK(chunks(dir, P_LIST) == npos(P_LIST));
        }
    }
    CHECK(finished);
    printf("  power: lost at every step of an open (its sweep) and a purge, %d power losses: the listed object whole, "
           "the retired name kept until its chunks went\n", crashes);
}

static void run(void)
{
    hold_case(0);
    hold_case(1);
    sweep_case();
    down_case();
    thread_case();
    power_case();
}
#endif

int main(int argc, char **argv)
{
    const char *child_dir = NULL, *scratch = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            bool ok = false;
            for (size_t j = 0; j < sizeof MUTANTS / sizeof *MUTANTS; j++)
                if (!strcmp(MUTANTS[j].name, m))
                    ecgc_mutant = MUTANTS[j].m, ok = true;
            if (!ok) {
                fprintf(stderr, "usage: test_ecgc [--quick] [--mutant gcunlinkfirst|gcunrefleak|gcnohold|sweeplive|"
                                "sweepinflight] [SCRATCH]\n");
                return 2;
            }
        } else if (!strcmp(argv[i], "--child") && i + 1 < argc) {
            child_dir = argv[++i];
        } else if (!strcmp(argv[i], "--quick")) {
#ifdef TSDB_EC
            quick = true;
#endif
        } else {
            scratch = argv[i];
        }
    }
#ifndef TSDB_EC
    (void)child_dir;
    (void)scratch;
    (void)mutant_name;
    printf("test_ecgc: skipped: no TSDB_EC in this build\n");
    return 0;
#else
    if (child_dir)
        return child(child_dir);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    const char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    snprintf(S, sizeof S, "%s/test_ecgc_%d", scratch ? scratch : "/tmp", (int)getpid());
    sh("rm -rf '%s'; mkdir -p '%s'", S, S);
    run();
    if (failures) {
        fprintf(stderr, "test_ecgc: %d failures (mutant %s)\n", failures, mutant_name(ecgc_mutant));
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_ecgc: ok\n");
    return 0;
#endif
}
