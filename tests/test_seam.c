/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The seam between .ec parts and the chunk store (src/eccache.h, R7 r7-seam): every query kind over parts read
 * through it equals the same parts read from their maps, bit for bit; a flipped byte in a chunk (RAW reads it, the
 * crc refuses it, the whole-cell read rebuilds it) gives the right answer; more than m chunks of an object gone is
 * TSDB_ERR_CORRUPT with a line naming volume, partition, part and granule; stores DOWN (moved away, the handle
 * reopened): right answers while at most m are, TSDB_ERR_UNAVAILABLE beyond (the fallback's open refuses; a backend
 * opened with NULL paths reads the DOWN positions), never wrong rows; eviction under a tiny budget while 32 threads
 * query; an fd budget of 8 over 100 objects; residency of an .ec part is the cache's.
 *
 * Parts are attached with ec_test_attach (the part's own bytes encoded into the tier's stores, p->ec set in memory),
 * and after a reopen with ec_test_attach_object (the same objects, nothing written).
 *
 * The audit of item 5 (no seam I/O under db->lock, latest_rw, buf_lock or publish_lock), from the code at 54d9f22:
 * granule_read's callers are src/query.c decode_visible, src_fill (merge_stream) and agg_worker, src/twa.c
 * twa_worker, src/latest.c src_decode (lookups and the warmer's eng_latest_fill), src/classes.c check_granule, and
 * merges (src/merge.c, pw_copy_fits in src/part.c). Queries read the parts of a cut: cut_take (src/query.c) holds
 * db->lock and buf_lock only while it copies references, and the snapshot windows are built from the cut without
 * them; buf_lock is taken around memtable collection only (query.c, latest.c latest_at), never around a part's
 * decode; latest_rw is held by a lookup only to read the table and by the warmer only to take its cut and to store
 * (src/ltab.c warm_batch); publish_lock is a writer's. check_granule skips .ec parts (classes.c: judged at settle).
 * Merges never take an .ec part (r7-skip's barriers; tests that attach run with merge_threads = 0).
 *
 * Mutants (--mutant NAME; the Makefile requires each to fail): seamnoverify (the damage case reads wrong bytes),
 * seamnoretry (the damage case says CORRUPT), cachekeyoff (every part's first granule is at the same offset: the warm
 * run meets another object's bytes and refuses them).
 */
#include "internal.h"
#include "chunk.h"
#include "eccache.h"
#include "ecpart.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static char scratch[512];

#ifdef TSDB_EC
#define K 3
#define M 2
#define NST 5
#define NSER 3
#define DAY 86400000LL

static char base[600], dbdir[700], stores[NST][700];

static void rm_rf(const char *path)
{
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
                continue;
            char p[4096];
            snprintf(p, sizeof p, "%s/%s", path, e->d_name);
            struct stat st;
            if (lstat(p, &st) == 0 && S_ISDIR(st.st_mode))
                rm_rf(p);
            else
                unlink(p);
        }
        closedir(d);
    }
    rmdir(path);
}

/* ------------------------------------------------------------ a handle: tier + engine */

typedef struct h {
    tier *t;
    eng *db;
} h;

static int h_open(h *x, const char *dir, const char *extra)
{
    char spec[8192];
    int n = snprintf(spec, sizeof spec, "k=%d,m=%d,dirs=", K, M);
    for (int i = 0; i < NST; i++)
        n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s", i ? ":" : "", stores[i]);
    snprintf(spec + n, sizeof spec - (size_t)n, "%s", extra ? extra : ",cache_mb=64");
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    x->t = NULL;
    x->db = NULL;
    int rc = tier_open(dir, NULL, &x->t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (rc)
        return rc;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0; /* no merge barrier for an attached part before r7-skip */
    o.create_volumes = 0;
    o.partition_classes = 0;
    o.obsolete_part_lifetime_s = 3600;
    tier_want(x->t, 0);
    rc = eng_open(dir, &o, &x->db);
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

static size_t list_parts(eng *db, part **out, size_t cap)
{
    pthread_mutex_lock(&db->lock);
    size_t n = MIN(db->nparts, cap);
    for (size_t i = 0; i < n; i++)
        out[i] = db->parts[i];
    pthread_mutex_unlock(&db->lock);
    return n;
}

/* ------------------------------------------------------------ the queries, serialised */

typedef struct buf {
    uint8_t *b;
    size_t n, cap;
} buf;

static void put(buf *o, const void *p, size_t n)
{
    if (o->n + n > o->cap) {
        size_t c = MAX(o->cap * 2, o->n + n + 4096);
        uint8_t *nb = realloc(o->b, c);
        if (!nb)
            abort();
        o->b = nb;
        o->cap = c;
    }
    memcpy(o->b + o->n, p, n);
    o->n += n;
}

typedef struct hsum {
    uint64_t h;
    size_t n;
    int w; /* bytes a value */
} hsum;
static int hash_fn(void *ctx, const int64_t *ts, const void *v, size_t n);
static hsum rawref[NSER]; /* each series' rows, read from the maps */

typedef struct rawctx {
    buf *o;
    int width;
} rawctx;

static int raw_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    rawctx *r = ctx;
    put(r->o, ts, n * 8);
    put(r->o, v, n * (size_t)r->width);
    return 0;
}

static const int types[NSER] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64};
static uint32_t sids[NSER];

/* Every query kind over every series; the first failing return code is put in *rc. */
static void run_all(eng *db, buf *o, int *rc)
{
    *rc = 0;
#define Q(call)                                                                                                        \
    do {                                                                                                               \
        int r_ = (call);                                                                                               \
        put(o, &r_, sizeof r_);                                                                                        \
        if (r_ && !*rc)                                                                                                \
            *rc = r_;                                                                                                  \
    } while (0)
    for (int s = 0; s < NSER; s++) {
        rawctx rc8 = {o, types[s] == TSDB_FLOAT32 ? 4 : 8};
        Q(eng_query_raw_ex(db, sids[s], INT64_MIN, INT64_MAX, false, 0, raw_fn, &rc8));
        Q(eng_query_raw_ex(db, sids[s], INT64_MIN, INT64_MAX, true, 3000, raw_fn, &rc8));
        Q(eng_query_raw_ex(db, sids[s], DAY + 12345, 3 * DAY + 777, false, 0, raw_fn, &rc8));
        Q(eng_query_lttb(db, sids[s], 0, 8 * DAY, 500, false, 0, raw_fn, &rc8));
        tsdb_group gs[3] = {{TSDB_HOUR, 1, NULL}, {TSDB_MINUTE, 7, NULL}, {TSDB_DAY, 1, NULL}};
        for (int gi = 0; gi < 3; gi++) {
            bucketer bk;
            CHECK(bucketer_init(&bk, 0, &gs[gi]) == 0);
            tsdb_bucket *b = NULL;
            size_t nb = 0;
            Q(eng_query_group(db, sids[s], 1000, 7 * DAY - 1000, &bk, gi == 1, 0, &b, &nb));
            put(o, b, nb * sizeof *b);
            free(b);
            tsdb_spread_bucket *sb = NULL;
            Q(eng_query_spread(db, sids[s], 0, 8 * DAY, &bk, false, 0, &sb, &nb));
            put(o, sb, nb * sizeof *sb);
            free(sb);
            tsdb_twa_opts to = {.method = gi ? TSDB_TWA_LINEAR : TSDB_TWA_STEP, .no_stats = 1};
            tsdb_twa_bucket *tb = NULL;
            Q(eng_query_twa(db, sids[s], 5000, 7 * DAY, &bk, &to, &tb, &nb));
            put(o, tb, nb * sizeof *tb);
            free(tb);
        }
    }
    latest_row lr[NSER];
    int64_t ends[NSER] = {DAY, 3 * DAY + 5, INT64_MAX};
    for (int wn = 0; wn < 2; wn++) {
        memset(lr, 0, sizeof lr);
        Q(eng_latest_search(db, sids, NSER, NULL, INT64_MAX, wn, lr));
        put(o, lr, sizeof lr);
        memset(lr, 0, sizeof lr);
        Q(eng_latest_search(db, sids, NSER, ends, INT64_MAX, wn, lr));
        put(o, lr, sizeof lr);
    }
#undef Q
}

/* ------------------------------------------------------------ data */

static uint64_t rng = 88172645463325252ull;
static uint64_t xs(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static void fill_db(eng *db)
{
    const char *names[NSER] = {"f32", "f64", "i64"};
    size_t n = 100000; /* 6 s apart: about 7 days, a part per day and series */
    int64_t *ts = malloc(n * 8);
    uint8_t *v = malloc(n * 8);
    for (int s = 0; s < NSER; s++) {
        CHECK(eng_series(db, names[s], types[s], &sids[s]) == 0);
        int64_t t = 0;
        for (size_t i = 0; i < n; i++) {
            t += 6000 - 500 + (int64_t)(xs() % 1000);
            ts[i] = t;
            double d = 20 + 5 * (double)(i % 3600) / 3600.0 + (double)(xs() % 1000) / 100.0;
            if (types[s] == TSDB_FLOAT32)
                ((float *)v)[i] = (float)d;
            else if (types[s] == TSDB_FLOAT64)
                ((double *)v)[i] = d;
            else
                ((int64_t *)v)[i] = (int64_t)(d * 1000);
        }
        CHECK(eng_insert_series(db, types[s], sids[s], ts, v, n) == 0);
    }
    /* a few late small writes, through the WAL, so some days have two parts */
    for (int s = 0; s < NSER; s++)
        for (int i = 0; i < 50; i++) {
            int64_t t1 = 2 * DAY + 333 + i * 1000;
            uint64_t z = 0;
            if (types[s] == TSDB_FLOAT32) {
                float f = (float)i;
                memcpy(&z, &f, 4);
            } else if (types[s] == TSDB_FLOAT64) {
                double d = i;
                memcpy(&z, &d, 8);
            } else {
                z = (uint64_t)i;
            }
            CHECK(eng_insert_series(db, types[s], sids[s], &t1, &z, 1) == 0);
        }
    CHECK(eng_flush(db) == 0);
    free(ts);
    free(v);
}

/* the objects attached, by part name, so a reopened handle attaches the same ones */
#define MAXP 256
static struct {
    char name[96];
    ec_record rec;
} objs[MAXP];
static size_t nobjs;

static int attach_all(eng *db, bool fresh_objects)
{
    part *ps[MAXP];
    size_t n = list_parts(db, ps, MAXP);
    int attached = 0;
    for (size_t i = 0; i < n; i++) {
        if (fresh_objects) {
            CHECK(nobjs < MAXP);
            if (nobjs >= MAXP)
                break;
            int rc = ec_test_attach(db, ps[i], &objs[nobjs].rec);
            CHECK(rc == 0);
            if (rc)
                continue;
            snprintf(objs[nobjs].name, sizeof objs[nobjs].name, "%s", ps[i]->name);
            nobjs++;
            attached++;
        } else {
            for (size_t j = 0; j < nobjs; j++)
                if (!strcmp(objs[j].name, ps[i]->name)) {
                    CHECK(ec_test_attach_object(db, ps[i], &objs[j].rec) == 0);
                    attached++;
                }
        }
    }
    return attached;
}

static void free_objs(void)
{
    for (size_t i = 0; i < nobjs; i++)
        free(objs[i].rec.sums); /* ec_record_free is r7-ecfile's */
    nobjs = 0;
}

/* the chunk file of position pos of object oid, wherever it was placed */
static bool chunk_path(const uint8_t oid[16], unsigned pos, char *out, size_t len)
{
    char hex[33];
    chunk_oid_hex(oid, hex);
    for (int i = 0; i < NST; i++) {
        snprintf(out, len, "%s/s0/o%s.%u.chunk", stores[i], hex, pos);
        if (access(out, F_OK) == 0)
            return true;
    }
    return false;
}

/* stderr to a file while a block runs, to read the lines a refusal printed */
static int saved_err = -1;
static void cap_begin(const char *path)
{
    fflush(stderr);
    saved_err = dup(2);
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    dup2(fd, 2);
    close(fd);
}
static void cap_end(void)
{
    fflush(stderr);
    dup2(saved_err, 2);
    close(saved_err);
}
static char *slurp(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return strdup("");
    char *b = calloc(1, 1 << 20);
    size_t n = fread(b, 1, (1 << 20) - 1, f);
    b[n] = 0;
    fclose(f);
    return b;
}

/* ------------------------------------------------------------ the cases */

static buf ref; /* every query over the maps */

static bool same_as_ref(eng *db, const char *what)
{
    buf o = {0};
    int rc;
    run_all(db, &o, &rc);
    bool same = rc == 0 && o.n == ref.n && memcmp(o.b, ref.b, ref.n) == 0;
    if (!same)
        fprintf(stderr, "test_seam: %s: rc %d, %zu bytes against %zu: the answers differ\n", what, rc, o.n, ref.n);
    free(o.b);
    return same;
}

static void case_equal(h *x)
{
    eng *db = x->db;
    int rc;
    run_all(db, &ref, &rc);
    CHECK(rc == 0 && ref.n > 1000000);
    for (int s = 0; s < NSER; s++) {
        rawref[s] = (hsum){0, 0, types[s] == TSDB_FLOAT32 ? 4 : 8};
        CHECK(eng_query_raw_ex(db, sids[s], INT64_MIN, INT64_MAX, false, 0, hash_fn, &rawref[s]) == 0);
    }
    part *ps[MAXP];
    size_t np = list_parts(db, ps, MAXP);
    CHECK(np >= 2 * NSER);
    /* residency before: the map's; the part bytes are the map's length */
    int n = attach_all(db, true);
    CHECK(n == (int)np);
    uint64_t c0 = atomic_load(&ec_calls[EC_CALL_GRANULE_READ]);
    eccache_counters k0, k1, k2;
    eccache_get_counters(x->t, &k0);
    CHECK(k0.misses == 0 && k0.bytes == 0);
    for (size_t i = 0; i < np; i++) {
        CHECK(ps[i]->ec && !ps[i]->ec->test_mark);
        CHECK(eccache_part_bytes(ps[i]) == ps[i]->map_len);
        CHECK(eccache_resident(ps[i], 0, ps[i]->map_len) == 0); /* nothing in the cache yet */
    }
    CHECK(same_as_ref(db, "cold"));
    eccache_get_counters(x->t, &k1);
    CHECK(k1.misses > 0 && k1.bytes > 0 && k1.obj_opens > 0);
    CHECK(atomic_load(&ec_calls[EC_CALL_GRANULE_READ]) > c0);
    CHECK(same_as_ref(db, "warm"));
    eccache_get_counters(x->t, &k2);
    CHECK(k2.hits > k1.hits && k2.misses == k1.misses); /* the warm run read nothing (64 MB holds it all) */
    uint64_t res = 0;
    for (size_t i = 0; i < np; i++)
        res += eccache_resident(ps[i], 0, ps[i]->map_len);
    CHECK(res == k2.bytes); /* the cache's bytes, part by part */
    printf("test_seam: equal: %zu parts, %" PRIu64 " granules read, %" PRIu64 " hits, %" PRIu64 " bytes cached\n",
           np, k2.misses, k2.hits, k2.bytes);
}

/* bytes flipped in two positions of every object (m = 2): RAW reads them, the crc refuses, the whole cells decode */
static void case_damage(h *x)
{
    int flipped = 0;
    for (size_t i = 0; i < nobjs; i++) {
        chunk_geom g;
        CHECK(chunk_geom_make(&g, objs[i].rec.k, objs[i].rec.m, objs[i].rec.cell_bytes, objs[i].rec.object_len) == 0);
        for (unsigned pos = 0; pos < 2; pos++) {
            char p[4096];
            if (!chunk_path(objs[i].rec.oid, pos, p, sizeof p))
                continue;
            int fd = open(p, O_RDWR);
            CHECK(fd >= 0);
            uint64_t len = chunk_pos_len(&g, pos);
            for (uint64_t off = 100 + pos * 37; off < len; off += 1500) {
                uint8_t b;
                CHECK(pread(fd, &b, 1, CHUNK_HDR_BYTES + off) == 1);
                b ^= 0x5a;
                CHECK(pwrite(fd, &b, 1, CHUNK_HDR_BYTES + off) == 1);
                flipped++;
            }
            close(fd);
        }
    }
    CHECK(flipped > 100);
    eccache_test_drop(x->t);
    eccache_counters k0, k1;
    eccache_get_counters(x->t, &k0);
    CHECK(k0.bytes == 0 && k0.objs_open == 0);
    CHECK(same_as_ref(x->db, "damaged cells"));
    eccache_get_counters(x->t, &k1);
    CHECK(k1.retries > k0.retries && k1.corrupt == k0.corrupt);
    printf("test_seam: damage: %d bytes flipped, %" PRIu64 " second reads\n", flipped, k1.retries - k0.retries);
}

static void move_store(int i, bool away)
{
    char a[800];
    snprintf(a, sizeof a, "%s.away", stores[i]);
    CHECK(rename(away ? stores[i] : a, away ? a : stores[i]) == 0);
}

/* stores moved away and the handle reopened */
static void case_down(h *x)
{
    h_close(x);
    move_store(0, true);
    move_store(3, true);
    CHECK(h_open(x, dbdir, NULL) == 0);
    if (!x->db)
        return;
    CHECK(attach_all(x->db, false) == (int)nobjs);
    CHECK(same_as_ref(x->db, "two stores down"));
    h_close(x);

    /* three down: the backend cannot open (min_up = stores - m) */
    move_store(1, true);
    CHECK(h_open(x, dbdir, NULL) == 0);
    if (!x->db)
        return;
    CHECK(attach_all(x->db, false) == (int)nobjs);
    char log[800];
    snprintf(log, sizeof log, "%s/err.log", base);
    for (int s = 0; s < NSER; s++) {
        buf o = {0};
        rawctx r = {&o, 8};
        cap_begin(log);
        int rc = eng_query_raw_ex(x->db, sids[s], INT64_MIN, INT64_MAX, false, 0, raw_fn, &r);
        cap_end();
        CHECK(rc == TSDB_ERR_UNAVAILABLE && o.n == 0);
        free(o.b);
    }
    /* a backend that opened with the three as NULL paths: the reads meet DOWN positions */
    const char *pp[NST];
    for (int i = 0; i < NST; i++)
        pp[i] = i == 0 || i == 1 || i == 3 ? NULL : stores[i];
    chunk_backend *be = NULL;
    chunk_be_opts bo = {.min_up = 0};
    CHECK(chunk_be_open2(&be, pp, NST, NULL, &bo) == 0);
    if (be) {
        eccache_set_backend(x->t, be);
        int unavail = 0;
        for (int s = 0; s < NSER; s++) {
            /* an object whose k' positions are on the stores left is still read; the rest are away, never lost */
            hsum o = {0, 0, rawref[s].w};
            cap_begin(log);
            int rc = eng_query_raw_ex(x->db, sids[s], INT64_MIN, INT64_MAX, false, 0, hash_fn, &o);
            cap_end();
            CHECK(rc == 0 || rc == TSDB_ERR_UNAVAILABLE);
            CHECK(rc != 0 || (o.h == rawref[s].h && o.n == rawref[s].n));
            unavail += rc == TSDB_ERR_UNAVAILABLE;
        }
        CHECK(unavail > 0);
        eccache_counters k;
        eccache_get_counters(x->t, &k);
        CHECK(k.corrupt == 0); /* objects whose k' positions are up were read */
    }
    h_close(x);
    if (be)
        chunk_be_close(be);
    move_store(0, false);
    move_store(1, false);
    move_store(3, false);
    CHECK(h_open(x, dbdir, NULL) == 0);
    if (x->db)
        CHECK(attach_all(x->db, false) == (int)nobjs);
    printf("test_seam: down: right answers with 2 of 5 stores away, UNAVAILABLE with 3\n");
}

/* 32 threads, random windows, a cache smaller than a few granules */
typedef struct qjob {
    int s;
    int64_t a, b;
    uint64_t sum;
    size_t n;
} qjob;
#define NJ 48
static qjob jobs[NJ];

static int hash_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    hsum *x = ctx;
    x->h = xxh64(ts, n * 8, x->h) ^ xxh64(v, n * (size_t)x->w, x->h + 1);
    x->n += n;
    return 0;
}

static int job_run(eng *db, const qjob *j, hsum *out)
{
    out->h = 0;
    out->n = 0;
    out->w = types[j->s] == TSDB_FLOAT32 ? 4 : 8;
    return eng_query_raw_ex(db, sids[j->s], j->a, j->b, false, 0, hash_fn, out);
}

static eng *thr_db;
static atomic_int thr_bad;

static void *thr(void *arg)
{
    uint64_t r = (uint64_t)(uintptr_t)arg * 0x9e3779b97f4a7c15ull + 1;
    for (int it = 0; it < 40; it++) {
        r ^= r << 13, r ^= r >> 7, r ^= r << 17;
        const qjob *j = &jobs[r % NJ];
        hsum o;
        if (job_run(thr_db, j, &o) != 0 || o.h != j->sum || o.n != j->n)
            atomic_fetch_add(&thr_bad, 1);
    }
    return NULL;
}

static void case_threads(h *x)
{
    for (int i = 0; i < NJ; i++) {
        jobs[i].s = i % NSER;
        jobs[i].a = (int64_t)(xs() % (uint64_t)(6 * DAY));
        jobs[i].b = jobs[i].a + 1 + (int64_t)(xs() % (uint64_t)DAY);
        hsum o;
        CHECK(job_run(x->db, &jobs[i], &o) == 0);
        jobs[i].sum = o.h;
        jobs[i].n = o.n;
    }
    eccache_test_drop(x->t);
    eccache_test_budget(x->t, 48 << 10);
    eccache_counters k0, k1;
    eccache_get_counters(x->t, &k0);
    thr_db = x->db;
    pthread_t th[32];
    for (int i = 0; i < 32; i++)
        pthread_create(&th[i], NULL, thr, (void *)(uintptr_t)(i + 1));
    for (int i = 0; i < 32; i++)
        pthread_join(th[i], NULL);
    eccache_get_counters(x->t, &k1);
    CHECK(atomic_load(&thr_bad) == 0);
    CHECK(k1.evictions > k0.evictions && k1.bytes <= (48 << 10) && k1.corrupt == 0);
    /* a budget of nothing: every granule read, used once and dropped */
    eccache_test_budget(x->t, 0);
    for (int i = 0; i < NJ; i++) {
        hsum o;
        CHECK(job_run(x->db, &jobs[i], &o) == 0 && o.h == jobs[i].sum && o.n == jobs[i].n);
    }
    eccache_counters k2;
    eccache_get_counters(x->t, &k2);
    CHECK(k2.bytes == 0 && k2.entries == 0 && k2.uncached > k1.uncached);
    eccache_test_budget(x->t, 64 << 20);
    printf("test_seam: threads: 32 x 40 queries, %" PRIu64 " evictions, %" PRIu64 " waits\n",
           k1.evictions - k0.evictions, k1.waits - k0.waits);
}

/* more than m chunks of one object gone: CORRUPT, with the line; the other parts still answer */
static void case_lost(h *x)
{
    part *ps[MAXP];
    size_t np = list_parts(x->db, ps, MAXP);
    part *victim = NULL;
    size_t vi = 0;
    for (size_t i = 0; i < np && !victim; i++)
        for (size_t j = 0; j < nobjs; j++)
            if (!strcmp(objs[j].name, ps[i]->name) && ps[i]->ix.n > 0) {
                victim = ps[i];
                vi = j;
                break;
            }
    CHECK(victim);
    if (!victim)
        return;
    int gone = 0;
    for (unsigned pos = 0; pos < 3; pos++) {
        char p[4096];
        if (chunk_path(objs[vi].rec.oid, pos, p, sizeof p) && unlink(p) == 0)
            gone++;
    }
    CHECK(gone == 3);
    eccache_test_drop(x->t);
    /* the series of the victim's first granule */
    uint32_t local = victim->ix.rser[0];
    int s = 0;
    while (s < NSER && sids[s] != local)
        s++;
    CHECK(s < NSER);
    char log[800];
    snprintf(log, sizeof log, "%s/err.log", base);
    buf o = {0};
    rawctx r = {&o, 8};
    cap_begin(log);
    int rc = eng_query_raw_ex(x->db, local, victim->min_ts, victim->max_ts + 1, false, 0, raw_fn, &r);
    cap_end();
    CHECK(rc == TSDB_ERR_CORRUPT);
    free(o.b);
    char *t = slurp(log);
    char want[300];
    snprintf(want, sizeof want, "volume 0, partition %" PRId64 ", part %s, granule ", victim->partition, victim->name);
    CHECK(strstr(t, want) != NULL);
    CHECK(strstr(t, "TSDB_ERR_CORRUPT") != NULL);
    if (failures)
        fprintf(stderr, "test_seam: the lines were: %s\n", t);
    free(t);
    CHECK(atomic_load(&victim->damaged) == false); /* never part_mark_payload */
    printf("test_seam: lost: CORRUPT naming the granule\n");
}

/* 100 objects (a part per day) under an fd budget of 8 */
static void case_fds(void)
{
    char dir[800];
    snprintf(dir, sizeof dir, "%s/fds", base);
    h x;
    CHECK(h_open(&x, dir, NULL) == 0);
    if (!x.db)
        return;
    uint32_t s;
    CHECK(eng_series(x.db, "d", TSDB_FLOAT32, &s) == 0);
    enum { DAYS = 100, PER = 300 };
    int64_t ts[PER];
    float v[PER];
    for (int d = 0; d < DAYS; d++) {
        for (int i = 0; i < PER; i++)
            ts[i] = d * DAY + i * 60000, v[i] = (float)(d * 1000 + i);
        CHECK(eng_insert_series(x.db, TSDB_FLOAT32, s, ts, v, PER) == 0);
    }
    CHECK(eng_flush(x.db) == 0);
    hsum want[DAYS];
    for (int d = 0; d < DAYS; d++) {
        want[d] = (hsum){0, 0, 4};
        CHECK(eng_query_raw_ex(x.db, s, d * DAY, (d + 1) * DAY, false, 0, hash_fn, &want[d]) == 0);
    }
    part *ps[MAXP];
    size_t np = list_parts(x.db, ps, MAXP);
    CHECK(np == DAYS);
    for (size_t i = 0; i < np; i++)
        CHECK(ec_test_attach(x.db, ps[i], NULL) == 0);
    eccache_test_fd_budget(x.t, 8);
    eccache_test_budget(x.t, 0); /* every granule from its object */
    for (int round = 0; round < 2; round++)
        for (int d = 0; d < DAYS; d++) {
            hsum o = {0, 0, 4};
            CHECK(eng_query_raw_ex(x.db, s, d * DAY, (d + 1) * DAY, false, 0, hash_fn, &o) == 0);
            CHECK(o.h == want[d].h && o.n == PER);
        }
    eccache_counters k;
    eccache_get_counters(x.t, &k);
    CHECK(k.fds_peak <= 8 && k.fds_open <= 8 && k.obj_opens >= 2 * DAYS && k.obj_closes + k.objs_open == k.obj_opens);
    printf("test_seam: fds: %zu objects, %" PRIu64 " opens, at most %" PRIu64 " fds\n", np, k.obj_opens, k.fds_peak);
    h_close(&x);
}

static void run(void)
{
    snprintf(base, sizeof base, "%s/test_seam_%d", scratch, (int)getpid());
    rm_rf(base);
    CHECK(mkdir(base, 0755) == 0);
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    for (int i = 0; i < NST; i++) {
        snprintf(stores[i], sizeof stores[i], "%s/st%d", base, i);
        CHECK(mkdir(stores[i], 0755) == 0);
        uint8_t id[16];
        CHECK(chunk_store_init(stores[i], NULL, id) == 0);
    }
    h x;
    CHECK(h_open(&x, dbdir, NULL) == 0);
    if (!x.db)
        return;
    fill_db(x.db);
    case_equal(&x);
    case_threads(&x);
    case_down(&x); /* before the damage: two stores away and two damaged positions would be more than m */
    if (x.db)
        case_damage(&x);
    if (x.db)
        case_lost(&x);
    h_close(&x);
    case_fds();
    free_objs();
    free(ref.b);
    if (!failures)
        rm_rf(base);
}
#endif

int main(int argc, char **argv)
{
    int a = 1;
    if (a < argc && !strcmp(argv[a], "--mutant")) {
        const char *m = a + 1 < argc ? argv[a + 1] : "";
        if (!strcmp(m, "seamnoverify"))
            eccache_mutant = ECC_MUT_NOVERIFY;
        else if (!strcmp(m, "seamnoretry"))
            eccache_mutant = ECC_MUT_NORETRY;
        else if (!strcmp(m, "cachekeyoff"))
            eccache_mutant = ECC_MUT_KEYOFF;
        else {
            fprintf(stderr, "usage: test_seam [--mutant seamnoverify|seamnoretry|cachekeyoff] [SCRATCH]\n");
            return 2;
        }
        a += 2;
    }
    snprintf(scratch, sizeof scratch, "%s", a < argc ? argv[a] : "/tmp");
#ifndef TSDB_EC
    printf("test_seam: skipped: no TSDB_EC in this build\n");
    return 0;
#else
    run();
    if (failures) {
        fprintf(stderr, "test_seam: %d failures\n", failures);
        return 1;
    }
    printf("test_seam: ok\n");
    return 0;
#endif
}
