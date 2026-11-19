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
 * seamnoretry (the damage case says CORRUPT), cachekeyoff (every part's first granule is at the same offset: the cold
 * run meets another object's bytes and the len/crc check of a hit refuses them; that check, not the key, is what
 * kills it), seamnorecheck (case_bad's self-consistent wrong object decodes), seamretryerr (case_down counts second
 * reads after UNAVAILABLE), seamkeepstale (case_heal reads the damaged inode again), forgetnoop (case_heal's
 * eccache_reopen and case_forget). An object closed under a reader (case_threads with an fd budget of 8) is found by
 * ASan (make asan), not by a plain run.
 * Not here: tsdb_query_multi (volumes.c, the same engine queries per series) and the statistics' byte columns of an
 * .ec part (eccache_part_bytes is the map's length until r7-ecfile gives the object its own).
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
#define NSER 5
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

static const int types[NSER] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL, TSDB_STATE};
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
        rawctx rc8 = {o, (int)type_width(types[s])};
        Q(eng_query_raw_ex(db, sids[s], INT64_MIN, INT64_MAX, false, 0, raw_fn, &rc8));
        Q(eng_query_raw_ex(db, sids[s], INT64_MIN, INT64_MAX, true, 3000, raw_fn, &rc8));
        Q(eng_query_raw_ex(db, sids[s], DAY + 12345, 3 * DAY + 777, false, 0, raw_fn, &rc8));
        bool state = types[s] == TSDB_STATE; /* no LTTB and no TWA of a state (TSDB_ERR_INVALID) */
        if (!state)
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
            if (state)
                continue;
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
    const char *names[NSER] = {"f32", "f64", "i64", "bool", "state"};
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
            uint64_t z;
            if (types[s] == TSDB_FLOAT32) {
                float f = (float)d;
                uint32_t u;
                memcpy(&u, &f, 4);
                z = u;
            } else if (types[s] == TSDB_FLOAT64) {
                memcpy(&z, &d, 8);
            } else if (types[s] == TSDB_INT64) {
                z = (uint64_t)(int64_t)(d * 1000);
            } else if (types[s] == TSDB_BOOL) {
                z = (i / 37) & 1;
            } else {
                z = (i / 50) % 5;
            }
            if (i % 101 == 7)
                z = type_null(types[s]); /* nulls in every type */
            memcpy(v + i * type_width(types[s]), &z, type_width(types[s]));
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
            } else if (types[s] == TSDB_BOOL) {
                z = (uint64_t)i & 1;
            } else {
                z = (uint64_t)i % 3;
            }
            if (i % 9 == 4)
                z = type_null(types[s]);
            CHECK(eng_insert_series(db, types[s], sids[s], &t1, &z, 1) == 0);
        }
    CHECK(eng_flush(db) == 0);
    /* an hour deleted on day 3: its tombstone part hides rows of attached parts (decode_visible reads them) */
    for (int s = 0; s < NSER; s++)
        CHECK(eng_delete_range(db, sids[s], 3 * DAY + 1000, 3 * DAY + 3600000) == 0);
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
        if (ps[i]->ix.n == 0)
            continue; /* a tombstone part: no granules to read */
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
        rawref[s] = (hsum){0, 0, (int)type_width(types[s])};
        CHECK(eng_query_raw_ex(db, sids[s], INT64_MIN, INT64_MAX, false, 0, hash_fn, &rawref[s]) == 0);
    }
    part *all[MAXP], *ps[MAXP];
    size_t na = list_parts(db, all, MAXP), np = 0, ntomb = 0;
    for (size_t i = 0; i < na; i++) {
        if (all[i]->ix.n)
            ps[np++] = all[i];
        ntomb += all[i]->nt > 0;
    }
    CHECK(np >= 2 * NSER && ntomb >= NSER); /* the delete's tombstones are parts of their own */
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
    /* the cache's granule bytes, part by part; the budget counts each entry's overhead as well */
    CHECK(res > 0 && res + k2.entries * ECCACHE_ENTRY_OVERHEAD == k2.bytes);
    /* the statistics' residency (tsdb_series_stats resident) of the .ec parts is the cache's */
    uint32_t ids[NSER];
    memcpy(ids, sids, sizeof ids);
    for (int i = 1; i < NSER; i++) /* ascending */
        for (int j = i; j > 0 && ids[j - 1] > ids[j]; j--) {
            uint32_t t = ids[j];
            ids[j] = ids[j - 1];
            ids[j - 1] = t;
        }
    sscx *sx = sscx_open(db);
    CHECK(sx != NULL);
    if (sx) {
        tsdb_series_stat st[NSER];
        uint64_t probes = 0;
        memset(st, 0, sizeof st);
        sscx_run(sx, ids, NSER, st, &probes, true);
        uint64_t sres = 0;
        for (int i = 0; i < NSER; i++)
            sres += st[i].resident_bytes;
        CHECK(sres == res);
        sscx_close(sx);
    }
    /* the fallback backend is open: a handover now is refused, never closes it under readers */
    static char not_a_backend;
    CHECK(eccache_set_backend(x->t, (chunk_backend *)(void *)&not_a_backend) == TSDB_ERR_LOCKED);
    CHECK(same_as_ref(db, "after a refused handover"));
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
        CHECK(eccache_set_backend(x->t, be) == 0); /* the fallback never opened */
        eccache_counters kd;
        eccache_get_counters(x->t, &kd);
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
        CHECK(k.retries == kd.retries); /* an UNAVAILABLE RAW read is not read again whole (seamretryerr) */
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
    out->w = (int)type_width(types[j->s]);
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

/* fdb: an fd budget (0: the default); 8 holds one object of 5 chunks, so objects are closed while other threads
 * read through theirs (an object a reader holds must never be closed under it) */
static void case_threads(h *x, uint64_t fdb)
{
    eccache_counters kb;
    eccache_get_counters(x->t, &kb);
    if (fdb)
        eccache_test_fd_budget(x->t, fdb);
    atomic_store(&thr_bad, 0);
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
    if (fdb) {
        CHECK(k1.obj_closes > k0.obj_closes + 32); /* objects were closed while the threads read */
        eccache_test_fd_budget(x->t, kb.fd_budget);
    }
    printf("test_seam: threads (fd budget %" PRIu64 "): 32 x 40 queries, %" PRIu64 " evictions, %" PRIu64
           " waits, %" PRIu64 " objects closed\n",
           fdb ? fdb : kb.fd_budget, k1.evictions - k0.evictions, k1.waits - k0.waits, k1.obj_closes - k0.obj_closes);
}

/* the attached part of object j, and the series of its first granule */
static part *obj_part(eng *db, size_t j, int *ser)
{
    part *ps[MAXP];
    size_t np = list_parts(db, ps, MAXP);
    for (size_t i = 0; i < np; i++)
        if (!strcmp(objs[j].name, ps[i]->name)) {
            *ser = 0;
            while (*ser < NSER && sids[*ser] != ps[i]->ix.rser[0])
                (*ser)++;
            return *ser < NSER ? ps[i] : NULL;
        }
    return NULL;
}

static int read_part(eng *db, const part *p, int s, hsum *o)
{
    *o = (hsum){0, 0, (int)type_width(types[s])};
    return eng_query_raw_ex(db, sids[s], p->min_ts, p->max_ts + 1, false, 0, hash_fn, o);
}

static bool copy_file(const char *from, const char *to)
{
    char *b = NULL;
    FILE *f = fopen(from, "rb"), *g = NULL;
    bool ok = false;
    if (f && fseek(f, 0, SEEK_END) == 0) {
        long n = ftell(f);
        rewind(f);
        b = malloc((size_t)n + 1);
        g = fopen(to, "wb");
        ok = b && g && fread(b, 1, (size_t)n, f) == (size_t)n && fwrite(b, 1, (size_t)n, g) == (size_t)n;
    }
    if (f)
        fclose(f);
    if (g && fclose(g) != 0)
        ok = false;
    free(b);
    return ok;
}

/* fds of this process open on a chunk file of the object (whatever its name now) */
static int fds_on(const uint8_t oid[16])
{
    char hex[33];
    chunk_oid_hex(oid, hex);
    DIR *d = opendir("/proc/self/fd");
    if (!d)
        return 0; /* not Linux: not counted */
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d))) {
        char p[300], t[4096];
        snprintf(p, sizeof p, "/proc/self/fd/%s", de->d_name);
        ssize_t k = readlink(p, t, sizeof t - 1);
        if (k > 0) {
            t[k] = 0;
            n += strstr(t, hex) != NULL;
        }
    }
    closedir(d);
    return n;
}

/* A chunk healed (a new file renamed over the damaged one, as chunkheal.c writes it) is read once the object is
 * opened again: a read that saw the object degraded drops it (seamkeepstale keeps it: the old inode stays read). */
static void case_heal(h *x)
{
    int s;
    part *p = obj_part(x->db, 0, &s);
    CHECK(p);
    if (!p)
        return;
    hsum want, o;
    CHECK(read_part(x->db, p, s, &want) == 0 && want.n > 0);
    char path[4096], good[4200];
    CHECK(chunk_path(objs[0].rec.oid, 0, path, sizeof path));
    snprintf(good, sizeof good, "%s.good", path);
    CHECK(copy_file(path, good));
    chunk_geom g;
    CHECK(chunk_geom_make(&g, objs[0].rec.k, objs[0].rec.m, objs[0].rec.cell_bytes, objs[0].rec.object_len) == 0);
    int fd = open(path, O_RDWR);
    CHECK(fd >= 0);
    for (uint64_t off = 10; off < chunk_pos_len(&g, 0); off += 700) {
        uint8_t b;
        CHECK(pread(fd, &b, 1, CHUNK_HDR_BYTES + off) == 1);
        b ^= 0xa5;
        CHECK(pwrite(fd, &b, 1, CHUNK_HDR_BYTES + off) == 1);
    }
    close(fd);
    eccache_test_drop(x->t);
    eccache_counters k0, k1, k2, k3;
    eccache_get_counters(x->t, &k0);
    CHECK(read_part(x->db, p, s, &o) == 0 && o.h == want.h && o.n == want.n);
    eccache_get_counters(x->t, &k1);
    CHECK(k1.retries > k0.retries && k1.corrupt == k0.corrupt);
    CHECK(rename(good, path) == 0); /* healed: a new inode under the chunk's name */
    eccache_test_drop_entries(x->t);
    CHECK(read_part(x->db, p, s, &o) == 0 && o.h == want.h && o.n == want.n);
    eccache_get_counters(x->t, &k2);
    CHECK(k2.retries == k1.retries); /* the healed chunk was read, not the old inode */
    /* eccache_reopen: the object open now is closed (no reader holds it) and opened again by the next read */
    CHECK(fds_on(objs[0].rec.oid) > 0);
    eccache_reopen(x->t, 0, objs[0].rec.oid);
    eccache_get_counters(x->t, &k3);
    CHECK(k3.obj_closes == k2.obj_closes + 1 && fds_on(objs[0].rec.oid) == 0);
    eccache_test_drop_entries(x->t);
    CHECK(read_part(x->db, p, s, &o) == 0 && o.h == want.h);
    printf("test_seam: heal: %" PRIu64 " second reads while damaged, none after the heal\n", k1.retries - k0.retries);
}

/* eccache_forget (r7-gc): the object's chunks go away (renamed here, so they can come back), then forget. A read in
 * flight at the forget finishes and is not cached, an object that was opening is closed by its reader, and nothing of
 * the object stays: no entry, no fd. First without threads, the forget run from the hooks; then under 8 readers. */
static const uint8_t *fg_oid;
static h *fg_h;
static hsum fg_want;
static int fg_where, fg_done;

static void fg_hide(const uint8_t oid[16], bool hide)
{
    for (unsigned pos = 0; pos < K + M; pos++)
        for (int i = 0; i < NST; i++) {
            char a[1000], b[1100];
            char hex[33];
            chunk_oid_hex(oid, hex);
            snprintf(a, sizeof a, "%s/s0/o%s.%u.chunk", stores[i], hex, pos);
            snprintf(b, sizeof b, "%s.hid", a);
            (void)rename(hide ? a : b, hide ? b : a);
        }
}

static void fg_hook(struct tier *t, int where, const uint8_t oid[16])
{
    if (fg_done || where != fg_where || memcmp(oid, fg_oid, 16) != 0)
        return;
    fg_done = 1;
    fg_hide(oid, true);
    eccache_forget(t, 0, oid);
}

static void *fg_thr(void *arg)
{
    const part *p = arg;
    int s = 0;
    while (s < NSER && sids[s] != p->ix.rser[0])
        s++;
    for (int it = 0; it < 200 && !atomic_load(&thr_bad); it++) {
        hsum o;
        int rc = read_part(fg_h->db, p, s, &o);
        if (rc == TSDB_ERR_CORRUPT)
            break; /* gone */
        if (rc != 0 || o.h != fg_want.h || o.n != fg_want.n)
            atomic_fetch_add(&thr_bad, 1);
    }
    return NULL;
}

static void case_forget(h *x)
{
    char log[800];
    snprintf(log, sizeof log, "%s/forget.log", base);
    for (int where = ECC_HOOK_OPENED; where <= ECC_HOOK_FILLED; where++) {
        size_t j = (size_t)where; /* objects 1 and 2 */
        int s;
        part *p = obj_part(x->db, j, &s);
        CHECK(p);
        if (!p)
            continue;
        eccache_test_drop(x->t);
        fg_oid = objs[j].rec.oid;
        fg_where = where;
        fg_done = 0;
        eccache_test_hook = fg_hook;
        hsum o;
        cap_begin(log);
        int rc = read_part(x->db, p, s, &o); /* the first granule is read, the next ones find the object gone */
        cap_end();
        eccache_test_hook = NULL;
        CHECK(fg_done && (rc == 0 || rc == TSDB_ERR_CORRUPT));
        CHECK(eccache_resident(p, 0, p->map_len) == 0); /* the granule in flight was not cached */
        CHECK(fds_on(fg_oid) == 0);                      /* and the object was closed by its reader */
        fg_hide(fg_oid, false);
        eccache_test_drop(x->t);
    }
    /* under 8 readers */
    int s;
    part *p = obj_part(x->db, 3, &s);
    CHECK(p);
    if (!p)
        return;
    hsum want;
    CHECK(read_part(x->db, p, s, &want) == 0);
    fg_want = want;
    eccache_test_drop(x->t);
    fg_h = x;
    atomic_store(&thr_bad, 0);
    pthread_t th[8];
    cap_begin(log);
    for (int i = 0; i < 8; i++)
        pthread_create(&th[i], NULL, fg_thr, p);
    usleep(20000);
    fg_hide(objs[3].rec.oid, true);
    eccache_forget(x->t, 0, objs[3].rec.oid);
    for (int i = 0; i < 8; i++)
        pthread_join(th[i], NULL);
    cap_end();
    CHECK(atomic_load(&thr_bad) == 0);
    CHECK(eccache_resident(p, 0, p->map_len) == 0);
    CHECK(fds_on(objs[3].rec.oid) == 0);
    fg_hide(objs[3].rec.oid, false);
    hsum o;
    CHECK(read_part(x->db, p, s, &o) == 0 && o.h == want.h && o.n == want.n); /* back: opened again */
    printf("test_seam: forget: nothing of the object left after a forget during its open, its fill, 8 readers\n");
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

/* Objects and indexes that are wrong in themselves, on a handle of their own: an object encoded from bytes with one
 * granule's byte flipped (its record matches its chunks, so the whole-cell read rebuilds the same wrong bytes: the
 * second check must refuse them, seamnorecheck); a granule placed outside the object's granules by its index entry;
 * a record whose sums table does not fit its geometry. All CORRUPT with the line, never rows, never a crash. */
static void case_bad(void)
{
    char dir[800], log[800];
    snprintf(dir, sizeof dir, "%s/bad", base);
    snprintf(log, sizeof log, "%s/bad.log", base);
    h x;
    CHECK(h_open(&x, dir, NULL) == 0);
    if (!x.db)
        return;
    uint32_t s;
    CHECK(eng_series(x.db, "b", TSDB_FLOAT32, &s) == 0);
    enum { DAYS = 3, PER = 3000 };
    static int64_t ts[PER];
    static float v[PER];
    for (int d = 0; d < DAYS; d++) {
        for (int i = 0; i < PER; i++)
            ts[i] = d * DAY + i * 20000, v[i] = (float)(d * 7 + i % 113) / 4;
        CHECK(eng_insert_series(x.db, TSDB_FLOAT32, s, ts, v, PER) == 0);
    }
    CHECK(eng_flush(x.db) == 0);
    part *ps[MAXP];
    size_t np = list_parts(x.db, ps, MAXP);
    CHECK(np == DAYS);
    if (np != DAYS) {
        h_close(&x);
        return;
    }
    /* 1: the object of day 0 from a copy with a byte of its first granule's values flipped */
    granule g;
    gx_load(ps[0], 0, GW_LOC, &g);
    uint8_t *cp = malloc(ps[0]->map_len);
    memcpy(cp, ps[0]->map, ps[0]->map_len);
    cp[g.off + g.ts_len + g.val_len / 2] ^= 0x10;
    CHECK(ec_test_attach_from(x.db, ps[0], cp, NULL) == 0);
    free(cp);
    eccache_counters k0, k1;
    eccache_get_counters(x.t, &k0);
    buf o = {0};
    rawctx r = {&o, 4};
    cap_begin(log);
    int rc = eng_query_raw_ex(x.db, s, 0, DAY, false, 0, raw_fn, &r);
    cap_end();
    eccache_get_counters(x.t, &k1);
    CHECK(rc == TSDB_ERR_CORRUPT && k1.retries > k0.retries && k1.corrupt > k0.corrupt);
    char *t = slurp(log);
    CHECK(strstr(t, "fail its checksum after a decode of whole cells") && strstr(t, "granule 0 "));
    free(t);
    free(o.b);
    /* 2: index entries outside the object's granules (the reader is called with them directly) */
    ec_record rec1;
    CHECK(ec_test_attach(x.db, ps[1], &rec1) == 0);
    codec_ctx *cx = codec_new();
    static int64_t gts[8192 + 64];
    static float gv[8192 + 64];
    granule g1;
    gx_load(ps[1], 0, GW_LOC, &g1);
    CHECK(ec_granule_read(cx, ps[1], &g1, TSDB_FLOAT32, gts, gv, 8192 + 64, false) == 0);
    eccache_test_drop_entries(x.t);
    granule bad[3] = {g1, g1, g1};
    bad[0].off = ps[1]->index_off;           /* runs into the index */
    bad[1].off = ps[1]->hdr_len - 1;         /* inside the header */
    bad[2].val_len = (uint32_t)ps[1]->map_len; /* longer than the object */
    cap_begin(log);
    for (int i = 0; i < 3; i++)
        CHECK(ec_granule_read(cx, ps[1], &bad[i], TSDB_FLOAT32, gts, gv, 8192 + 64, false) == TSDB_ERR_CORRUPT);
    cap_end();
    codec_free(cx);
    /* 3: day 2 attached to day 1's object with a record whose sums do not fit its geometry */
    ec_record bad_rec = rec1;
    bad_rec.nsums = 1;
    CHECK(ec_test_attach_object(x.db, ps[2], &bad_rec) == 0);
    o = (buf){0};
    cap_begin(log);
    rc = eng_query_raw_ex(x.db, s, 2 * DAY, 3 * DAY, false, 0, raw_fn, &r);
    cap_end();
    CHECK(rc == TSDB_ERR_CORRUPT && o.n == 0);
    free(o.b);
    free(rec1.sums);
    printf("test_seam: bad: a self-consistent wrong object, three misplaced index entries, a bad record: CORRUPT\n");
    h_close(&x);
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
    case_threads(&x, 0);
    case_threads(&x, 8);
    case_down(&x); /* before the damage: two stores away and two damaged positions would be more than m */
    if (x.db)
        case_heal(&x);
    if (x.db)
        case_forget(&x);
    if (x.db)
        case_damage(&x);
    if (x.db)
        case_lost(&x);
    h_close(&x);
    case_fds();
    case_bad();
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
        else if (!strcmp(m, "seamnorecheck"))
            eccache_mutant = ECC_MUT_NORECHECK;
        else if (!strcmp(m, "seamretryerr"))
            eccache_mutant = ECC_MUT_RETRYERR;
        else if (!strcmp(m, "seamkeepstale"))
            eccache_mutant = ECC_MUT_KEEPSTALE;
        else if (!strcmp(m, "forgetnoop"))
            eccache_mutant = ECC_MUT_FORGETNOOP;
        else {
            fprintf(stderr, "usage: test_seam [--mutant seamnoverify|seamnoretry|cachekeyoff|seamnorecheck|"
                            "seamretryerr|seamkeepstale|forgetnoop] [SCRATCH]\n");
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
