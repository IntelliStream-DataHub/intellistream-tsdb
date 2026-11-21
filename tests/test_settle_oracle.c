/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled answers equal unsettled answers, bit for bit (docs/design-cluster-data.md 10.2, R7 and E7).
 *
 * Two databases, A and B, get the same random history: small and large inserts (duplicate timestamps, nulls, NaN
 * payloads, int64 beyond 2^53), deletes, replaces, retention with a test clock, drop_before, class switches, flushes,
 * optimize before the first settle, reopens, over four value types in two volumes. After random steps B "settles"
 * some of its partitions; A never does. Both run the same battery of queries after the steps, and every answer is
 * compared byte for byte: raw rows (both orders, limits), aggregates over intervals, calendar groups and client
 * bounds, the spread, TWA (step and linear, with a staleness limit), LTTB, tsdb_latest (with nulls, with end bounds),
 * tsdb_query_multi of every kind over all series (and ids that do not exist), the return code of each call, and the
 * row counts of the query statistics; at one thread and at many (query_rows_per_thread 1 and request threads 4).
 *
 * Why bit for bit holds: a settle keeps the object the part file unchanged (E7), so both databases have the same
 * parts, granules and statistics, and the float sums, variances and TWA areas that combine per granule come out the
 * same. That is also why optimize and merges stop in both once B has settled anything: a merge skips a settled part
 * (r7-skip), A would merge across it, the layouts would differ and so would the last bits of a float sum (tsdb.h:
 * "Float sums are not bit-reproducible"). Everything else (writes, deletes, replaces, retention, class switches)
 * goes on.
 *
 * Stages (the task's list): 1. eng_test_mark_ec, the skip rules alone: a marked part's bytes are still in its map and
 * every R7 stub reads it as a .part; `settle_step` is one function with one case per stage. 3. eng_settle through
 * tsdb_test_settle: B is opened with a tier of three directory stores (k=2, m=1) whose own settle thread never finds a
 * part due, and a settle step either marks (stage 1) or settles for real; real .ec parts stay across reopens and are
 * read through their objects. Store 0 is moved away (DOWN) at some reopens and back at the next, and chunks on it are
 * damaged at rest: one store holds one chunk per object, so every answer must still be the same.
 *
 * Usage: test_settle_oracle [--mutant NAME] [--seeds N] [--first S] [--steps N] [SCRATCH]
 *   the seeds and steps also from TSDB_ORACLE_SEEDS, TSDB_ORACLE_FIRST and TSDB_ORACLE_STEPS (make oracle-soak)
 * Mutants (each must fail the test: the Makefile requires it): skipdelete, skipreplace, skipretention (B leaves out
 * every delete, replace or retention change), nomark (the settle step settles nothing: a run that never reaches a
 * settled state fails), and flipvalues, flipagg, flipgroup, flipbounds, flipspread, fliptwa, fliplttb, fliplatest,
 * flipmulti, flipstats (one bit of B's answer of that kind is wrong: the comparison of that kind must see it), noretry
 * (the cache never decodes around a chunk damaged on store 0) and rankdown (the settle's ecrankdown). A compare after
 * a real settle or damage also reads every series whole, so these two are met far more often than by the random
 * windows alone, but not on every run: a damaged chunk may be parity, belong to a part already retired and not yet
 * collected, or hold bytes no granule read touches (the part's own index). tests/oracle_mutants.sh therefore tries a
 * few seed windows and counts a mutant as caught only by its own line (DIVERGED), never by any failure;
 * test_seam's seamnoretry and test_settle's ecrankdown catch the two bugs deterministically.
 */
#define _GNU_SOURCE
#include "internal.h"
#include "ecpart.h"
#include "eccache.h"

#include <fcntl.h>
#include <ftw.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            exit(1);                                                                 \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                          \
    do {                                                                                                  \
        int rc_ = (expr);                                                                                 \
        if (rc_ != 0) {                                                                                   \
            fprintf(stderr, "%s:%d: %s = %d (%s)\n", __FILE__, __LINE__, #expr, rc_, tsdb_strerror(rc_)); \
            exit(1);                                                                                      \
        }                                                                                                 \
    } while (0)

#define HOUR 3600000LL
#define DAY (24 * HOUR)

static int64_t clock_now = 20 * DAY;
static int64_t test_clock(void *arg)
{
    (void)arg;
    return __atomic_load_n(&clock_now, __ATOMIC_SEQ_CST);
}

static int rm_cb(const char *p, const struct stat *st, int flag, struct FTW *f)
{
    (void)st, (void)flag, (void)f;
    return remove(p);
}
static void rmrf(const char *dir) { nftw(dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS); }

typedef struct rng {
    uint64_t s;
} rng;
static uint64_t rnd(rng *r)
{
    r->s ^= r->s << 13;
    r->s ^= r->s >> 7;
    r->s ^= r->s << 17;
    return r->s;
}
static uint64_t mix(uint64_t a, uint64_t b)
{
    uint64_t x = a * 0x9e3779b97f4a7c15ull + b + 0x7f4a7c15ull;
    x ^= x >> 29;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 32;
    return x ? x : 1;
}

/* ------------------------------------------------------------------ mutants */

static const char *const mutant_names[] = {"none",        "skipdelete", "skipreplace", "skipretention", "nomark",
                                           "flipvalues",  "flipagg",    "flipgroup",   "flipbounds",    "flipspread",
                                           "fliptwa",     "fliplttb",   "fliplatest",  "flipmulti",     "flipstats",
                                           "noretry",     "rankdown"};
static int mutant; /* index into mutant_names */
static bool is(const char *name) { return strcmp(mutant_names[mutant], name) == 0; }

/* ------------------------------------------------------------------ fingerprints */

typedef struct rec {
    char tag[56];
    size_t off, len;
    int rc;
} rec;
typedef struct fp {
    uint8_t *b;
    size_t n, cap;
    rec *r;
    size_t nr, rcap;
} fp;

static void fp_free(fp *f)
{
    free(f->b);
    free(f->r);
    memset(f, 0, sizeof *f);
}

static void fp_begin(fp *f, const char *fmt, ...)
{
    if (f->nr == f->rcap) {
        f->rcap = f->rcap ? f->rcap * 2 : 256;
        f->r = realloc(f->r, f->rcap * sizeof *f->r);
        CHECK(f->r);
    }
    rec *r = &f->r[f->nr++];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r->tag, sizeof r->tag, fmt, ap);
    va_end(ap);
    r->off = f->n;
    r->len = 0;
    r->rc = 0;
}

static void fp_add(fp *f, const void *p, size_t n)
{
    if (f->n + n > f->cap) {
        f->cap = (f->n + n) * 2 + 4096;
        f->b = realloc(f->b, f->cap);
        CHECK(f->b);
    }
    if (n)
        memcpy(f->b + f->n, p, n);
    f->n += n;
    f->r[f->nr - 1].len += n;
}
static void fp_u64(fp *f, uint64_t v) { fp_add(f, &v, 8); }
static void fp_rc(fp *f, int rc)
{
    fp_u64(f, (uint64_t)(int64_t)rc);
    f->r[f->nr - 1].rc = rc;
}

static uint64_t slot_of(tsdb_type t, const tsdb_values *c, size_t i)
{
    switch (t) {
    case TSDB_FLOAT32: { uint32_t b; memcpy(&b, (const float *)c->values + i, 4); return b; }
    case TSDB_FLOAT64: { uint64_t b; memcpy(&b, (const double *)c->values + i, 8); return b; }
    case TSDB_INT64: return (uint64_t)((const int64_t *)c->values)[i];
    case TSDB_BOOL: return ((const uint8_t *)c->values)[i];
    default: return ((const uint32_t *)c->values)[i];
    }
}

static void fp_rows(fp *f, const int64_t *ts, const tsdb_values *v, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        bool known = !v->valid || (v->valid[i >> 3] >> (i & 7) & 1);
        fp_u64(f, (uint64_t)ts[i]);
        fp_u64(f, known ? slot_of(v->type, v, i) + 1 : 0); /* a null is 0, a value is its slot + 1 */
    }
}

static int rows_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    fp_rows(ctx, ts, v, n);
    return 0;
}

/* The contract of the statistics: rows, memtable_rows and result_rows, what the answer is made of. The rest says how
 * the plan got there (rows decoded or answered from statistics, bytes, parts, granules): at stage 1 a settled part is
 * read exactly as a .part, so A and B must agree on it too, and that is checked (the stronger test); when a real
 * settle (stage 3) reads a part through its object, it may legitimately decode a granule it answered from statistics or
 * count other bytes, and then ORACLE_PLAN=0 compares the contract only. Never loosen it by a failing run: decide here. */
static bool plan_exact(void)
{
    static int v = -1;
    if (v < 0) {
        const char *e = getenv("ORACLE_PLAN");
        v = !(e && *e == '0');
    }
    return v;
}

/* the row counts of the call that just ran (never the times, and never the compressed bytes) */
static void fp_stats(fp *f, tsdb_sid id)
{
    tsdb_query_stats s;
    tsdb_last_query_stats(&s);
    fp_begin(f, "stats %llx", (unsigned long long)id);
    uint64_t v[] = {s.rows, s.memtable_rows, s.result_rows, 0, 0, 0, 0, 0, 0};
    if (plan_exact()) {
        v[3] = s.rows_decoded, v[4] = s.rows_from_stats, v[5] = s.bytes, v[6] = s.parts, v[7] = s.granules;
        v[8] = s.granules_from_stats;
    }
    fp_add(f, v, sizeof v);
}

/* ------------------------------------------------------------------ the world */

#define NS 8
#define NSTATE 5

typedef struct world {
    tsdb *db[2]; /* 0: A, never settled; 1: B */
    char dir[2][700], vol2[2][700];
    char st[3][720], away[730]; /* B's tier: three directory stores (k=2, m=1); store 0 moved to `away` while DOWN */
    tsdb_options opt;
    tsdb_sid id[NS], never;
    tsdb_type type[NS];
    uint32_t code[NS][NSTATE];
    rng r;
    uint64_t seed;
    int step;
    bool settled_any;
    bool real; /* B holds .ec parts that eng_settle made: they stay settled across a reopen */
    bool down; /* store 0 is DOWN (moved away) since B's last open */
    bool whole; /* the next compare also reads every series whole: a real settle or damage came before it */
    /* what the run reached */
    size_t marked, settle_steps, compares, nonempty, failed;
    size_t real_settled, real_steps, unavailable, damaged, downs, retries;
} world;

static const tsdb_type types[NS] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL,
                                    TSDB_STATE,   TSDB_FLOAT64, TSDB_INT64, TSDB_FLOAT32};

static void make_series(world *w, int d)
{
    for (size_t i = 0; i < NS; i++) {
        char name[32];
        snprintf(name, sizeof name, "o.s%zu", i);
        tsdb_sid id;
        OK(tsdb_series_create(w->db[d], name, types[i], i >= 5 ? w->vol2[d] : NULL, &id));
        if (d == 0) {
            w->id[i] = id;
            w->type[i] = types[i];
        } else {
            CHECK(w->id[i] == id);
        }
        if (types[i] == TSDB_STATE)
            for (int k = 0; k < NSTATE; k++) {
                char sn[16];
                snprintf(sn, sizeof sn, "st%d", k);
                OK(tsdb_state(w->db[d], id, sn, &w->code[i][k]));
            }
    }
    tsdb_sid nv;
    OK(tsdb_series_create(w->db[d], "o.never", TSDB_FLOAT64, NULL, &nv));
    if (d == 0)
        w->never = nv;
    else
        CHECK(w->never == nv);
}

/* B's tier: never settles by itself (grace_s, its limit of 10 years, is far past any age here), only through tsdb_test_settle */
static void tier_env(world *w, bool on)
{
    if (!on) {
        unsetenv("TSDB_TEST_TIER");
        unsetenv("TSDB_TEST_TIER_I_KNOW");
        return;
    }
    char spec[3 * 720 + 128];
    snprintf(spec, sizeof spec, "k=2,m=1,dirs=%s:%s:%s,grace_s=315360000,coarse_s=86400,eager=0", w->st[0], w->st[1],
             w->st[2]);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
}

static void open_both(world *w, bool fresh)
{
    if (fresh)
        for (int i = 0; i < 3; i++) {
            rmrf(w->st[i]);
            CHECK(mkdir(w->st[i], 0755) == 0);
        }
    for (int d = 0; d < 2; d++) {
        if (fresh) {
            rmrf(w->dir[d]);
            rmrf(w->vol2[d]);
        }
        tier_env(w, d == 1);
        OK(tsdb_open(w->dir[d], &w->opt, &w->db[d]));
        tier_env(w, false);
        if (fresh) {
            uint32_t vi;
            OK(tsdb_volume_add(w->db[d], w->vol2[d], &vi));
            make_series(w, d);
        }
    }
}

static void close_both(world *w)
{
    for (int d = 0; d < 2; d++)
        OK(tsdb_close(w->db[d]));
}

/* ------------------------------------------------------------------ rows */

typedef struct batch {
    int64_t *ts;
    uint8_t *v, *valid;
    tsdb_type t;
    size_t n;
} batch;


static void batch_alloc(batch *b, size_t n, tsdb_type t)
{
    b->t = t;
    b->n = n;
    b->ts = calloc(n ? n : 1, sizeof *b->ts);
    b->v = calloc(n ? n : 1, 8);
    b->valid = calloc((n + 7) / 8 + 1, 1);
    CHECK(b->ts && b->v && b->valid);
}
static void batch_free(batch *b)
{
    free(b->ts);
    free(b->v);
    free(b->valid);
}

static int64_t rnd_ts(rng *r)
{
    uint64_t x = rnd(r);
    int64_t t;
    switch (x % 14) {
    case 0: t = 0; break;
    case 1: t = -(int64_t)((x >> 8) % 48) * HOUR; break;
    case 2: t = (int64_t)((x >> 8) % 400) * DAY; break;
    case 3: t = (int64_t)((x >> 8) % 20) * 28 * DAY + (int64_t)((x >> 20) % 5) * HOUR; break;
    default: t = (int64_t)((x >> 8) % (24 * 16)) * HOUR; break;
    }
    if ((x >> 40) % 3 == 0)
        t += (int64_t)((x >> 44) % 3600) * 1000 + (int64_t)((x >> 56) % 4) * 250;
    return t;
}

static void put_value(world *w, size_t si, batch *b, size_t i, int nullp, int64_t prev)
{
    uint64_t x = rnd(&w->r);
    if ((int)(x % 100) >= nullp)
        b->valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    x = rnd(&w->r);
    switch (b->t) {
    case TSDB_FLOAT32: {
        float f = x % 9 < 5 ? (float)((int)(x >> 8) % 20000 - 10000) / 100.0f
                            : x % 9 < 8 ? 20.0f + (float)(prev % 1000) * 0.001f : (float)((x >> 8) % 7);
        if (x % 211 == 0) {
            uint32_t nb = 0x7fc00001u + (uint32_t)(x >> 40 & 0xff);
            memcpy(&f, &nb, 4);
        } else if (x % 307 == 0)
            f = INFINITY;
        memcpy(b->v + 4 * i, &f, 4);
        break;
    }
    case TSDB_FLOAT64: {
        double f = x % 9 < 5 ? (double)((int)(x >> 8) % 2000000 - 1000000) / 1000.0
                             : x % 9 < 8 ? 1e9 + (double)(prev % 100000) * 0.01 : (double)((x >> 8) % 7);
        if (x % 211 == 0) {
            uint64_t nb = 0x7ff8000000000001ull + (x >> 40 & 0xff);
            memcpy(&f, &nb, 8);
        } else if (x % 307 == 0)
            f = -INFINITY;
        memcpy(b->v + 8 * i, &f, 8);
        break;
    }
    case TSDB_INT64: {
        int64_t v = x % 11 == 0 ? ((int64_t)1 << 53) + (int64_t)(x >> 8) % 9 - 4
                                : (int64_t)(x >> 8) % 100000 - 50000 + (x % 5 == 0 ? prev % 100 : 0);
        memcpy(b->v + 8 * i, &v, 8);
        break;
    }
    case TSDB_BOOL: b->v[i] = (uint8_t)(x >> 8 & 1); break;
    default: {
        uint32_t c = w->code[si][(x >> 8) % NSTATE];
        memcpy(b->v + 4 * i, &c, 4);
        break;
    }
    }
}

/* ------------------------------------------------------------------ the steps */

static void apply_insert(world *w, size_t si, bool large, int nullp)
{
    uint64_t x = rnd(&w->r);
    size_t n = large ? 100 + x % 300 : 1 + x % 12;
    batch b;
    batch_alloc(&b, n, w->type[si]);
    bool dense = x % 3 == 0;
    int64_t start = rnd_ts(&w->r);
    int64_t step = 1000 + (int64_t)(x >> 20) % 4 * 29000; /* 1 s to about 90 s */
    for (size_t i = 0; i < n; i++) {
        b.ts[i] = dense ? start + (int64_t)i * step : rnd_ts(&w->r);
        put_value(w, si, &b, i, nullp, b.ts[i]);
    }
    tsdb_values vals = {b.t, b.v, b.valid};
    for (int d = 0; d < 2; d++)
        OK(tsdb_insert_series_values(w->db[d], w->id[si], b.ts, &vals, n));
    batch_free(&b);
}

static int collect_ts(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)v;
    fp *f = ctx; /* only its ts array: a scratch fp with one open record */
    fp_add(f, ts, n * 8);
    return 0;
}

static void apply_null_over_newest(world *w, size_t si)
{
    fp f = {0};
    fp_begin(&f, "scan");
    OK(tsdb_query_values(w->db[0], w->id[si], TSDB_TS_MIN, TSDB_TS_MAX, NULL, collect_ts, &f));
    size_t n = f.n / 8;
    if (n) {
        const int64_t *ts = (const int64_t *)f.b;
        batch b;
        batch_alloc(&b, 1, w->type[si]);
        b.ts[0] = ts[n - 1 - rnd(&w->r) % (n < 5 ? n : 5)];
        put_value(w, si, &b, 0, 100, 0);
        tsdb_values vals = {b.t, b.v, b.valid};
        for (int d = 0; d < 2; d++)
            OK(tsdb_insert_series_values(w->db[d], w->id[si], b.ts, &vals, 1));
        batch_free(&b);
    }
    fp_free(&f);
}

static void apply_delete(world *w, size_t si)
{
    uint64_t x = rnd(&w->r);
    int64_t a = x % 5 == 0 ? TSDB_TS_MIN : rnd_ts(&w->r);
    int64_t b = x % 7 == 0 ? TSDB_TS_MAX : a + (int64_t)((x >> 8) % 60) * HOUR;
    for (int d = 0; d < 2; d++) {
        if (d == 1 && is("skipdelete"))
            continue;
        OK(tsdb_delete_range(w->db[d], w->id[si], a, b));
    }
}

static void apply_replace(world *w, size_t si, int nullp)
{
    int64_t a = rnd_ts(&w->r), len = (int64_t)(rnd(&w->r) % 30) * HOUR;
    size_t n = rnd(&w->r) % 8;
    batch b;
    batch_alloc(&b, n + 1, w->type[si]);
    int64_t step = len > 0 ? len / (int64_t)(n + 1) + 1 : 1;
    for (size_t i = 0; i < n; i++) { /* sorted, distinct, inside [a, a + len] */
        b.ts[i] = a + (int64_t)i * step;
        put_value(w, si, &b, i, nullp, b.ts[i]);
    }
    tsdb_values vals = {b.t, b.v, b.valid};
    int rc[2] = {0, 0};
    for (int d = 0; d < 2; d++) {
        if (d == 1 && is("skipreplace"))
            continue;
        rc[d] = tsdb_replace_range(w->db[d], w->id[si], a, a + len + 1 + (int64_t)n * step, b.ts, &vals, n);
        CHECK(rc[d] == 0 || rc[d] == TSDB_ERR_INVALID);
    }
    CHECK(rc[0] == rc[1] || is("skipreplace"));
    batch_free(&b);
}

static void apply_retention(world *w, size_t si)
{
    int64_t c = __atomic_load_n(&clock_now, __ATOMIC_SEQ_CST) + (int64_t)(rnd(&w->r) % 3) * DAY;
    __atomic_store_n(&clock_now, c, __ATOMIC_SEQ_CST);
    int64_t lim = rnd(&w->r) % 3 == 0 ? 0 : (int64_t)(5 + rnd(&w->r) % 25) * DAY;
    for (int d = 0; d < 2; d++) {
        if (d == 1 && is("skipretention"))
            continue;
        OK(tsdb_set_retention(w->db[d], w->id[si], lim));
    }
}

typedef struct pickctx {
    uint64_t salt;
    unsigned mod;
    bool per_part; /* a part by itself, not the partition it is in */
} pickctx;

static bool pick_partition(void *ctx, uint32_t vol, int64_t partition, uint32_t level, uint64_t rows)
{
    const pickctx *p = ctx;
    uint64_t key = (uint64_t)partition;
    if (p->per_part) /* parts of one partition differ in their rows (and level): about half of them, any of them */
        key = mix(key, rows * 0x9e3779b97f4a7c15ull + level);
    return mix(p->salt ^ ((uint64_t)vol << 56), key) % p->mod == 0;
}

/* Stage 1: marks the listed parts of some partitions of B, either all of a partition's parts (a settle takes the whole
 * key) or single parts: then a marked part can be newer than an unmarked one with the same (series, ts) (a part a merge
 * held is left unmarked, so a real settle may leave such a pair), which the ranking of settled parts must get right
 * (review F1: "ec parts below every non-ec part" is only the same order while marks are a block prefix).
 * Stage 2 and 3 are cases here when ec_test_attach and eng_settle exist. */
static void settle_step(world *w)
{
    if (is("nomark"))
        return;
    pickctx pc = {.salt = rnd(&w->r), .mod = 2 + (unsigned)(rnd(&w->r) % 3)};
    pc.per_part = rnd(&w->r) % 2 == 0;
    if (pc.per_part)
        pc.mod = 2;
    bool real = rnd(&w->r) % 3 != 0;
    size_t n;
    w->settle_steps++;
    if (real) {
        /* stage 3: the parts turn .ec on disk; the query then reads them through their objects (the cache and the
         * decode), also after a reopen. With store 0 DOWN eng_settle refuses (tier_writable): nothing changes. */
        int rc = tsdb_test_settle(w->db[1], pick_partition, &pc, &n);
        if (rc == TSDB_ERR_UNAVAILABLE && w->down)
            w->unavailable++;
        else if (rc) {
            fprintf(stderr, "seed %llu step %d: tsdb_test_settle: %s\n", (unsigned long long)w->seed, w->step,
                    tsdb_strerror(rc));
            exit(1);
        }
        w->real_steps++;
        w->real_settled += n;
        if (n)
            w->real = true, w->whole = true;
    } else
        n = tsdb_test_mark_ec(w->db[1], EC_MARK_EC, pick_partition, &pc);
    w->marked += n;
    if (n)
        w->settled_any = true;
}

/* Damage at rest on store 0 only: one store holds one chunk of each object (k + m = 3 stores), so with any number of
 * its chunks damaged, and store 0 DOWN as well, every object still has k = 2 good chunks and decodes to the same
 * bytes. A byte in the middle of the file (a cell, or the sums table) changes: the chunk is an erasure. */
static world *damage_w; /* nftw passes no context */
static rng damage_r;     /* its own draws: how many chunk files there are depends on the collector's timing */
static int damage_cb(const char *p, const struct stat *st, int flag, struct FTW *f)
{
    (void)f;
    world *w = damage_w;
    size_t len = strlen(p);
    if (flag != FTW_F || len < 6 || strcmp(p + len - 6, ".chunk") != 0 || st->st_size < 2 || rnd(&damage_r) % 4)
        return 0;
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0);
    off_t at = st->st_size / 2;
    unsigned char c;
    CHECK(pread(fd, &c, 1, at) == 1);
    c ^= 0x20;
    CHECK(pwrite(fd, &c, 1, at) == 1);
    close(fd);
    w->damaged++;
    return 0;
}

static void damage_store0(world *w)
{
    if (w->down)
        return; /* the store is away: its chunks are not read now */
    damage_w = w;
    damage_r.s = rnd(&w->r);
    nftw(w->st[0], damage_cb, 16, FTW_PHYS);
    tsdb_test_ec_retries(w->db[1], true); /* the next battery reads the chunks again */
    w->whole = true;
}

/* ------------------------------------------------------------------ the battery */

static void rnd_window(rng *q, int64_t *s, int64_t *e)
{
    switch (rnd(q) % 5) {
    case 0: *s = TSDB_TS_MIN, *e = TSDB_TS_MAX; break;
    case 1: *s = rnd_ts(q), *e = *s + (int64_t)(rnd(q) % 200) * HOUR + 1; break;
    case 2: *s = rnd_ts(q), *e = TSDB_TS_MAX; break;
    case 3: *s = TSDB_TS_MIN, *e = rnd_ts(q); break;
    default: *s = 0, *e = 16 * DAY; break;
    }
}

static const int64_t intervals[] = {0, 60000, HOUR, 7 * 60000 + 13, DAY, 3 * HOUR + 17, 1000};

static void rnd_opts(rng *q, tsdb_query_opts *o)
{
    uint64_t x = rnd(q);
    o->order = (x & 3) == 0 ? TSDB_DESC : TSDB_ASC;
    o->limit = (x >> 2) % 3 == 0 ? 1 + (x >> 8) % 20 : 0;
}

static size_t make_bounds(rng *q, int64_t *bounds)
{
    size_t n = 1 + rnd(q) % 6;
    int64_t t = rnd_ts(q);
    for (size_t i = 0; i < n; i++) {
        bounds[i] = t;
        t += 1 + (int64_t)(rnd(q) % 40) * HOUR + (int64_t)(rnd(q) % 3) * 60000;
    }
    return n;
}

/* every answer of one handle, as records; the same qseed makes the same queries on both handles */
static void battery(world *w, tsdb *db, uint64_t qseed, int mode, fp *f)
{
    rng q = {qseed};
    query_rows_per_thread = mode ? 1 : UINT64_MAX;
    size_t nall = 0;
    tsdb_sid all[NS + 6];
    for (size_t i = 0; i < NS; i++)
        all[nall++] = w->id[i];
    all[nall++] = w->never;
    all[nall++] = TSDB_SID_NONE;
    all[nall++] = TSDB_SID(0, 100000);
    all[nall++] = TSDB_SID(9, 0);

    for (size_t i = 0; i < NS + 1; i++) {
        tsdb_sid id = i < NS ? w->id[i] : w->never;
        int64_t s, e;
        tsdb_query_opts o;
        int rc;
        void *out;
        size_t n;
        /* raw rows */
        rnd_window(&q, &s, &e);
        rnd_opts(&q, &o);
        fp_begin(f, "values s%zu [%lld,%lld) o%d l%zu", i, (long long)s, (long long)e, (int)o.order, o.limit);
        rc = tsdb_query_values(db, id, s, e, &o, rows_cb, f);
        fp_rc(f, rc);
        fp_stats(f, id);
        /* aggregates: an interval, a calendar group, client bounds */
        rnd_window(&q, &s, &e);
        rnd_opts(&q, &o);
        int64_t iv = intervals[rnd(&q) % (sizeof intervals / sizeof *intervals)];
        fp_begin(f, "agg s%zu [%lld,%lld) iv%lld", i, (long long)s, (long long)e, (long long)iv);
        rc = tsdb_query_agg_ex(db, id, s, e, iv, &o, (tsdb_bucket **)&out, &n);
        fp_rc(f, rc);
        if (rc == 0) {
            fp_add(f, out, n * sizeof(tsdb_bucket));
            tsdb_free(out);
        }
        fp_stats(f, id);
        tsdb_group g = {.unit = (tsdb_unit)(TSDB_HOUR + rnd(&q) % 4), .count = (uint32_t)(1 + rnd(&q) % 3)};
        rnd_window(&q, &s, &e);
        rnd_opts(&q, &o);
        fp_begin(f, "group s%zu u%d c%u", i, (int)g.unit, g.count);
        rc = tsdb_query_group_ex(db, id, s, e, &g, &o, (tsdb_bucket **)&out, &n);
        fp_rc(f, rc);
        if (rc == 0) {
            fp_add(f, out, n * sizeof(tsdb_bucket));
            tsdb_free(out);
        }
        fp_stats(f, id);
        int64_t bounds[8];
        size_t nb = make_bounds(&q, bounds);
        rnd_window(&q, &s, &e);
        rnd_opts(&q, &o);
        fp_begin(f, "bounds s%zu nb%zu", i, nb);
        rc = tsdb_query_bounds(db, id, s, e, bounds, nb, &o, (tsdb_bucket **)&out, &n);
        fp_rc(f, rc);
        if (rc == 0) {
            fp_add(f, out, n * sizeof(tsdb_bucket));
            tsdb_free(out);
        }
        fp_stats(f, id);
        /* the spread */
        rnd_window(&q, &s, &e);
        rnd_opts(&q, &o);
        iv = intervals[rnd(&q) % (sizeof intervals / sizeof *intervals)];
        fp_begin(f, "spread s%zu iv%lld", i, (long long)iv);
        rc = tsdb_query_spread(db, id, s, e, iv, &o, (tsdb_spread_bucket **)&out, &n);
        fp_rc(f, rc);
        if (rc == 0) {
            fp_add(f, out, n * sizeof(tsdb_spread_bucket));
            tsdb_free(out);
        }
        fp_stats(f, id);
        nb = make_bounds(&q, bounds);
        rnd_window(&q, &s, &e);
        fp_begin(f, "spread s%zu bounds", i);
        rc = tsdb_query_spread_bounds(db, id, s, e, bounds, nb, NULL, (tsdb_spread_bucket **)&out, &n);
        fp_rc(f, rc);
        if (rc == 0) {
            fp_add(f, out, n * sizeof(tsdb_spread_bucket));
            tsdb_free(out);
        }
        fp_stats(f, id);
        /* TWA, both methods, with and without a staleness limit */
        for (int meth = 0; meth < 2; meth++) {
            tsdb_twa_opts to = {.method = meth ? TSDB_TWA_LINEAR : TSDB_TWA_STEP,
                                .max_gap_ms = rnd(&q) % 3 == 0 ? HOUR : 0};
            rnd_window(&q, &s, &e);
            iv = intervals[rnd(&q) % (sizeof intervals / sizeof *intervals)];
            fp_begin(f, "twa s%zu m%d iv%lld", i, meth, (long long)iv);
            rc = tsdb_query_twa(db, id, s, e, iv, &to, (tsdb_twa_bucket **)&out, &n);
            fp_rc(f, rc);
            if (rc == 0) {
                fp_add(f, out, n * sizeof(tsdb_twa_bucket));
                tsdb_free(out);
            }
            fp_stats(f, id);
        }
        rnd_window(&q, &s, &e);
        fp_begin(f, "twa s%zu group", i);
        tsdb_twa_opts to = {.method = TSDB_TWA_LINEAR};
        rc = tsdb_query_twa_group(db, id, s, e, &g, &to, (tsdb_twa_bucket **)&out, &n);
        fp_rc(f, rc);
        if (rc == 0) {
            fp_add(f, out, n * sizeof(tsdb_twa_bucket));
            tsdb_free(out);
        }
        fp_stats(f, id);
        /* LTTB */
        rnd_window(&q, &s, &e);
        rnd_opts(&q, &o);
        size_t thr = 3 + rnd(&q) % 30;
        fp_begin(f, "lttb s%zu thr%zu", i, thr);
        rc = tsdb_query_lttb_ex(db, id, s, e, thr, &o, rows_cb, f);
        fp_rc(f, rc);
        fp_stats(f, id);
        /* the newest row: no bound, with nulls, an end bound */
        for (int k = 0; k < 3; k++) {
            tsdb_latest_opts lo = {.end = k == 2 ? rnd_ts(&q) : TSDB_TS_MAX, .with_nulls = k == 1};
            tsdb_latest_result r;
            fp_begin(f, "latest s%zu k%d", i, k);
            rc = tsdb_latest(db, &id, 1, &lo, &r);
            fp_rc(f, rc);
            fp_u64(f, (uint64_t)(int64_t)r.status);
            fp_u64(f, r.kind);
            fp_u64(f, r.type);
            fp_u64(f, (uint64_t)r.ts);
            fp_u64(f, r.v.i64);
        }
    }
    /* every series in one call, every kind */
    {
        tsdb_latest_opts lo = {.end = TSDB_TS_MAX, .with_nulls = (int)(rnd(&q) & 1), .threads = mode ? 4 : 1};
        tsdb_latest_result r[NS + 6];
        fp_begin(f, "latest all");
        int rc = tsdb_latest(db, all, nall, &lo, r);
        fp_rc(f, rc);
        for (size_t i = 0; i < nall; i++) {
            fp_u64(f, (uint64_t)(int64_t)r[i].status);
            fp_u64(f, r[i].kind);
            fp_u64(f, r[i].type);
            fp_u64(f, (uint64_t)r[i].ts);
            fp_u64(f, r[i].v.i64);
        }
    }
    static const tsdb_multi_kind kinds[] = {TSDB_MULTI_VALUES, TSDB_MULTI_BUCKETS, TSDB_MULTI_SPREAD, TSDB_MULTI_TWA,
                                            TSDB_MULTI_LTTB};
    static const size_t bsize[] = {0, sizeof(tsdb_bucket), sizeof(tsdb_spread_bucket), sizeof(tsdb_twa_bucket), 0};
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++) {
        int64_t bounds[8];
        tsdb_multi_query mq = {.kind = kinds[k], .threshold = 3 + rnd(&q) % 25};
        rnd_window(&q, &mq.start, &mq.end);
        rnd_opts(&q, &mq.opts);
        mq.interval_ms = intervals[rnd(&q) % (sizeof intervals / sizeof *intervals)];
        mq.twa.method = (rnd(&q) & 1) ? TSDB_TWA_LINEAR : TSDB_TWA_STEP;
        size_t nb = 0;
        if (kinds[k] != TSDB_MULTI_VALUES && kinds[k] != TSDB_MULTI_LTTB && rnd(&q) % 2) {
            nb = make_bounds(&q, bounds);
            mq.bounds = bounds;
            mq.nbounds = nb;
        }
        if (kinds[k] == TSDB_MULTI_TWA) {
            mq.twa.order = mq.opts.order;
            mq.twa.limit = mq.opts.limit;
        }
        tsdb_multi_opts mo = {.threads = mode ? 4 : 1};
        tsdb_series_result *res = NULL;
        fp_begin(f, "multi k%zu nb%zu", k, nb);
        int rc = tsdb_query_multi_collect(db, &mq, all, nall, &mo, &res);
        fp_rc(f, rc);
        if (rc == 0 && res)
            for (size_t i = 0; i < nall; i++) {
                fp_u64(f, (uint64_t)(int64_t)res[i].status);
                fp_u64(f, res[i].n);
                if (res[i].status)
                    continue;
                if (kinds[k] == TSDB_MULTI_VALUES || kinds[k] == TSDB_MULTI_LTTB)
                    fp_rows(f, res[i].ts, &res[i].values, res[i].n);
                else if (res[i].buckets)
                    fp_add(f, res[i].buckets, res[i].n * bsize[k]);
            }
        tsdb_series_results_free(res, res ? nall : 0);
        /* the statistics of a multi call are the sums over its series, which helpers run ahead of the head: a series
         * deferred and run again is counted by whichever thread ran it, so the sums vary from run to run under load
         * (found by this test with nothing settled at all). Compared with one thread only. */
        if (!mode || getenv("ORACLE_MULTI_STATS_MT")) /* the reproducer: ORACLE_MULTI_STATS_MT=1 with --mutant nomark */
            fp_stats(f, all[0]);
    }
    /* After a real settle or damage on store 0 the random windows may miss every damaged granule or every key a new
     * .ec part overwrote, so the mutants noretry and rankdown were caught only on some runs (under TSan one survived:
     * which chunk files exist when store 0 is damaged depends on the collector's timing). Every series read whole
     * reads every granule of every listed part, so each overwritten key and each damaged data cell of a listed part
     * that holds granule bytes is met (not parity, nor a retired part's object). No draw from q: the windows above stay
     * the same. */
    for (size_t i = 0; w->whole && i < NS; i++) {
        fp_begin(f, "whole s%zu", i);
        int rc = tsdb_query_values(db, w->id[i], TSDB_TS_MIN, TSDB_TS_MAX, NULL, rows_cb, f);
        fp_rc(f, rc);
    }
}

/* B's answers wrong in one bit, for the mutants that test the comparison of each kind */
static void tamper(fp *f)
{
    static const char *const kinds[] = {"values", "agg", "group", "bounds", "spread", "twa", "lttb", "latest",
                                        "multi", "stats"};
    if (strncmp(mutant_names[mutant], "flip", 4) != 0)
        return;
    const char *kind = mutant_names[mutant] + 4;
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++)
        if (strcmp(kind, kinds[k]) == 0) {
            for (size_t i = 0; i < f->nr; i++)
                if (strncmp(f->r[i].tag, kind, strlen(kind)) == 0 && f->r[i].len > 8) {
                    f->b[f->r[i].off + f->r[i].len - 1] ^= 1;
                    return;
                }
        }
}

/* Error answers per kind of call (review F4): an error equals an error, so records that drift into answering errors
 * would compare equal for ever. Some are legitimate (TWA or LTTB on a series of another type or a window with
 * nothing in it); a kind that mostly errors is not. */
#define NKIND 16
static struct kind_stat {
    char name[16];
    size_t calls, errs;
} kstat[NKIND];

static void kind_count(const char *tag, int rc)
{
    size_t n = strcspn(tag, " ");
    if (n >= sizeof kstat[0].name || strncmp(tag, "stats", 5) == 0)
        return;
    for (size_t k = 0; k < NKIND; k++) {
        if (!kstat[k].name[0])
            memcpy(kstat[k].name, tag, n);
        if (strlen(kstat[k].name) == n && strncmp(kstat[k].name, tag, n) == 0) {
            kstat[k].calls++;
            kstat[k].errs += rc != 0;
            return;
        }
    }
    fprintf(stderr, "test_settle_oracle: more than %d kinds of call\n", NKIND);
    exit(1);
}

/* batteries of B after a settle that read a granule of a marked part at all, and ones that never did (review F3) */
static size_t reached, unreached;

static void compare(world *w, const char *why)
{
    w->compares++;
    for (int mode = 0; mode < 2; mode++) {
        uint64_t qs = mix(w->seed, (uint64_t)w->step * 4 + (uint64_t)mode + 1000 * w->compares);
        fp fa = {0}, fb = {0};
        battery(w, w->db[0], qs, mode, &fa);
        uint64_t g0 = atomic_load(&ec_calls[EC_CALL_GRANULE_READ]);
        battery(w, w->db[1], qs, mode, &fb);
        if (w->settled_any) {
            if (atomic_load(&ec_calls[EC_CALL_GRANULE_READ]) != g0)
                reached++;
            else
                unreached++;
        }
        tamper(&fb);
        for (size_t i = 0; i < fa.nr; i++) {
            kind_count(fa.r[i].tag, fa.r[i].rc);
            if (fa.r[i].rc) {
                w->failed++;
                if (getenv("ORACLE_RCS")) /* which calls answered an error, for the author: sort | uniq -c */
                    fprintf(stderr, "rc %d %.*s\n", fa.r[i].rc, (int)strcspn(fa.r[i].tag, " "), fa.r[i].tag);
            }
            if (fa.r[i].len > 8 && strncmp(fa.r[i].tag, "stats", 5) != 0) /* more than a return code */
                w->nonempty++;
        }
        bool same = fa.nr == fb.nr;
        for (size_t i = 0; same && i < fa.nr; i++)
            if (strcmp(fa.r[i].tag, fb.r[i].tag) != 0 || fa.r[i].len != fb.r[i].len ||
                memcmp(fa.b + fa.r[i].off, fb.b + fb.r[i].off, fa.r[i].len) != 0) {
                size_t k = 0;
                if (fa.r[i].len == fb.r[i].len)
                    while (fa.b[fa.r[i].off + k] == fb.b[fb.r[i].off + k])
                        k++;
                fprintf(stderr,
                        "DIVERGED seed %llu step %d (%s) mode %d record %zu '%s': settled and unsettled answers "
                        "differ (lengths %zu and %zu, first byte %zu; marked so far %zu)\n",
                        (unsigned long long)w->seed, w->step, why, mode, i, fa.r[i].tag, fa.r[i].len, fb.r[i].len, k,
                        w->marked);
                exit(1);
            }
        if (!same) {
            fprintf(stderr, "DIVERGED seed %llu step %d (%s): %zu records against %zu\n", (unsigned long long)w->seed,
                    w->step, why, fa.nr, fb.nr);
            exit(1);
        }
        fp_free(&fa);
        fp_free(&fb);
    }
    w->whole = false;
}

/* ------------------------------------------------------------------ the walk */

static void run_seed(world *w, const char *scratch, uint64_t seed, int steps)
{
    memset(w, 0, sizeof *w);
    w->seed = seed;
    w->r.s = mix(seed, 77);
    __atomic_store_n(&clock_now, 20 * DAY, __ATOMIC_SEQ_CST);
    for (int d = 0; d < 2; d++) {
        snprintf(w->dir[d], sizeof w->dir[d], "%s/oracle_%d_%llu_%c", scratch, (int)getpid(), (unsigned long long)seed,
                 "ab"[d]);
        snprintf(w->vol2[d], sizeof w->vol2[d], "%s_v2", w->dir[d]);
    }
    for (int i = 0; i < 3; i++)
        snprintf(w->st[i], sizeof w->st[i], "%s_st%d", w->dir[1], i);
    snprintf(w->away, sizeof w->away, "%s_away", w->st[0]);
    tsdb_options_init(&w->opt);
    w->opt.merge_threads = 0; /* the layout is a function of the history alone */
    w->opt.query_threads = 4;
    w->opt.fsync = 0;
    w->opt.granule_rows = 32;
    w->opt.buffer_rows = (size_t)1 << 24;
    w->opt.flush_interval_ms = 1 << 30;
    w->opt.wal_max_batch_rows = 100;
    w->opt.clock_ms = test_clock;
    w->opt.create_volumes = 1;
    w->opt.partition_classes = seed % 3 != 0;
    open_both(w, true);
    for (w->step = 0; w->step < steps; w->step++) {
        size_t si = rnd(&w->r) % NS;
        uint64_t r = rnd(&w->r) % 100;
        int nullp = (int)(rnd(&w->r) % 4) * 15;
        const char *what;
        bool force = false;
        if (r < 28)
            apply_insert(w, si, false, nullp), what = "insert";
        else if (r < 42)
            apply_insert(w, si, true, nullp), what = "large insert";
        else if (r < 49)
            apply_null_over_newest(w, si), what = "null over newest";
        else if (r < 56)
            apply_delete(w, si), what = "delete";
        else if (r < 61)
            apply_replace(w, si, nullp), what = "replace";
        else if (r < 65)
            apply_retention(w, si), what = "retention";
        else if (r < 68) {
            for (int d = 0; d < 2; d++)
                OK(tsdb_flush(w->db[d]));
            what = "flush";
        } else if (r < 70) {
            for (int d = 0; d < 2; d++)
                OK(tsdb_commit(w->db[d]));
            what = "commit";
        } else if (r < 73) {
            /* a settled part is out of merges (r7-skip) and A would merge across it: no layout change after it */
            for (int d = 0; d < 2; d++)
                OK(w->settled_any ? tsdb_flush(w->db[d]) : tsdb_optimize(w->db[d]));
            what = "optimize";
        } else if (r < 75 && w->opt.partition_classes) {
            int cls = (int)(rnd(&w->r) % 4);
            int rc0 = tsdb_test_class_switch(w->db[0], w->id[si], cls, NULL);
            int rc1 = tsdb_test_class_switch(w->db[1], w->id[si], cls, NULL);
            CHECK(rc0 == rc1);
            what = "class switch";
        } else if (r < 76) {
            int64_t t = (int64_t)(rnd(&w->r) % 30) * DAY - 5 * DAY;
            for (int d = 0; d < 2; d++)
                OK(tsdb_drop_before(w->db[d], t));
            what = "drop_before";
        } else if (r < 80) {
            w->retries += tsdb_test_ec_retries(w->db[1], false);
            close_both(w);
            /* store 0 goes DOWN (moved away) or comes back; while it is away B reads every object around it */
            if (w->down || (w->real && rnd(&w->r) % 4 == 0)) {
                CHECK(rename(w->down ? w->away : w->st[0], w->down ? w->st[0] : w->away) == 0);
                w->down = !w->down;
                w->downs += w->down;
            }
            open_both(w, false);
            /* a reopen drops every mark: merges of B are the same as A's again, unless B holds real .ec parts */
            w->settled_any = w->real;
            what = "reopen", force = true;
        } else if (r < 90 && w->step >= 8) {
            settle_step(w);
            what = "settle", force = true;
            if (w->real && rnd(&w->r) % 3 == 0)
                damage_store0(w), what = "settle and damage";
        } else
            apply_insert(w, si, rnd(&w->r) % 4 == 0, nullp), what = "insert";
        if (force || w->step % 4 == 3)
            compare(w, what);
    }
    compare(w, "end");
    w->retries += tsdb_test_ec_retries(w->db[1], false);
    close_both(w);
    for (int d = 0; d < 2; d++) {
        rmrf(w->dir[d]);
        rmrf(w->vol2[d]);
    }
    for (int i = 0; i < 3; i++)
        rmrf(w->st[i]);
    rmrf(w->away);
}

int main(int argc, char **argv)
{
    const char *scratch = "/tmp";
    uint64_t seeds = getenv("TSDB_ORACLE_SEEDS") ? strtoull(getenv("TSDB_ORACLE_SEEDS"), NULL, 10) : 3;
    uint64_t first = getenv("TSDB_ORACLE_FIRST") ? strtoull(getenv("TSDB_ORACLE_FIRST"), NULL, 10) : 1;
    int steps = getenv("TSDB_ORACLE_STEPS") ? atoi(getenv("TSDB_ORACLE_STEPS")) : 100;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            const char *m = argv[++i];
            mutant = -1;
            for (size_t k = 0; k < sizeof mutant_names / sizeof *mutant_names; k++)
                if (strcmp(m, mutant_names[k]) == 0)
                    mutant = (int)k;
            if (mutant < 0) {
                fprintf(stderr, "test_settle_oracle: unknown mutant %s\n", m);
                return 2;
            }
        } else if (strcmp(argv[i], "--seeds") == 0 && i + 1 < argc)
            seeds = strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--first") == 0 && i + 1 < argc)
            first = strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--steps") == 0 && i + 1 < argc)
            steps = atoi(argv[++i]);
        else
            scratch = argv[i];
    }
    /* stage 3's mutants live in the engine: the cache answers a granule whose RAW bytes fail their crc as CORRUPT at
     * once (a damaged chunk on store 0 is then an error, never decoded around), and an .ec part ranks below every
     * other part of its partition (a settled newer part loses to an older one) */
    if (is("noretry"))
        eccache_mutant = ECC_MUT_NORETRY;
    if (is("rankdown"))
        setenv("TSDB_SETTLE_MUTANT", "ecrankdown", 1);
#ifndef TSDB_EC
    printf("test_settle_oracle: skipped: no TSDB_EC in this build\n");
    return 0;
#else
    world w;
    size_t marked = 0, settle_steps = 0, compares = 0, nonempty = 0;
    size_t real_settled = 0, real_steps = 0, unavailable = 0, damaged = 0, downs = 0, retries = 0;
    for (uint64_t s = first; s < first + seeds; s++) {
        run_seed(&w, scratch, s, steps);
        marked += w.marked, settle_steps += w.settle_steps, compares += w.compares, nonempty += w.nonempty;
        real_settled += w.real_settled, real_steps += w.real_steps, unavailable += w.unavailable;
        damaged += w.damaged, downs += w.downs, retries += w.retries;
    }
    /* stage 3: a run whose settles never made an .ec part tests the marks only */
    if (real_settled == 0 && !is("nomark")) {
        fprintf(stderr, "test_settle_oracle: no eng_settle made an .ec part (%zu steps)\n", real_steps);
        return 1;
    }
    /* a run that never settled a part, or never had an answer to compare, proves nothing */
    if (marked == 0 || nonempty == 0) {
        fprintf(stderr, "test_settle_oracle: no part was settled (%zu) or no answer had rows (%zu)\n", marked,
                nonempty);
        return 1;
    }
    /* a battery after a settle that never read a marked part's granule tests nothing about settled parts; the rest do */
    if (reached == 0) {
        fprintf(stderr, "test_settle_oracle: no battery ever read a granule of a settled part\n");
        return 1;
    }
    /* No "damaged, so some granule was read again" check: it does not hold (int-h REVIEW F1: 4 of 80 unmutated windows
     * damaged chunks that no read meets, see the top of the file), and a false failure here would also count as a
     * mutant caught. */
    for (size_t k = 0; k < NKIND && kstat[k].name[0]; k++)
        if (kstat[k].calls >= 50 && kstat[k].errs * 2 > kstat[k].calls) {
            fprintf(stderr, "test_settle_oracle: %zu of %zu %s calls answered an error: the battery drifted\n",
                    kstat[k].errs, kstat[k].calls, kstat[k].name);
            return 1;
        }
    printf("test_settle_oracle: %llu seeds from %llu, %d steps each: %zu settle steps turned %zu parts settled, %zu "
           "comparisons (%zu answers with rows; %zu batteries of B read a settled part, %zu did not), settled and "
           "unsettled answers identical; error answers:",
           (unsigned long long)seeds, (unsigned long long)first, steps, settle_steps, marked, compares, nonempty, reached,
           unreached);
    for (size_t k = 0; k < NKIND && kstat[k].name[0]; k++)
        printf(" %s %zu/%zu", kstat[k].name, kstat[k].errs, kstat[k].calls);
    printf("\n");
    printf("test_settle_oracle: stage 3: %zu eng_settle steps made %zu .ec parts (%zu refused with store 0 DOWN); store 0 "
           "went DOWN %zu times, %zu chunks on it damaged, %zu granules read again as whole cells\n",
           real_steps, real_settled, unavailable, downs, damaged, retries);
    return 0;
#endif
}
