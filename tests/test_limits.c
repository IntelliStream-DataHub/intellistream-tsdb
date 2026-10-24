/*
 * Limits that stop early. A bucketed query with a limit (aggregates, calendar
 * groups, bounds, the spread, TWA) computes only the buckets it returns plus
 * what it needs to find them: ascending it stops after the limit-th non-empty
 * bucket, descending it walks back from the end. Its result must be exactly
 * that of computing every bucket and then cutting, bit for bit: so every form
 * is checked with memcmp against the full query cut by hand, over random
 * ranges, intervals, groups, bounds, orders and limits, on data with replaced
 * values in overlapping parts, deletes, rows still in the memtable, nulls,
 * NaN and infinities, from a follower, after retention and after a full
 * merge, with one query thread and with several. LTTB descending must be the
 * ascending points reversed.
 *
 * Then the point of it: on a year of one row a minute, "the last (or first)
 * 10 hours, an hour per bucket, limit 10" must touch a few granules, not the
 * year's (query statistics). Built with -DLIMITS_STATS_ONLY it uses only
 * functions that predate early stop, so the statistics check can be run
 * against the old code (which computes every bucket and fails it).
 */
#define _GNU_SOURCE
#include "tsdb.h"

#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                    \
    do {                                                                                            \
        int rc_ = (expr);                                                                           \
        if (rc_ != 0) {                                                                             \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                                \
        }                                                                                           \
    } while (0)

static char base[512];

static void rmrf(const char *d)
{
    char cmd[1100];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", d);
    if (system(cmd) != 0)
        fprintf(stderr, "rm failed\n");
}

/* ------------------------------------------------- the statistics check */

#define MIN_MS 60000LL
#define HOUR_MS 3600000LL
#define DAY_MS 86400000LL
#define YEAR_T0 1704067200000LL /* 2024-01-01 */

static tsdb_query_stats stats(void)
{
    tsdb_query_stats s;
    tsdb_last_query_stats(&s);
    return s;
}

/* A limited query must cover a few granules where the full one covers the year's; and return the same
 * buckets as the full one cut. */
static void few_granules(const char *what, const tsdb_query_stats *lim, const tsdb_query_stats *full)
{
    printf("  %-18s limit 10: %4llu granules, %6llu rows, %7.3f ms; all: %4llu granules, %6llu rows, %7.3f ms\n",
           what, (unsigned long long)lim->granules, (unsigned long long)lim->rows, lim->elapsed_s * 1e3,
           (unsigned long long)full->granules, (unsigned long long)full->rows, full->elapsed_s * 1e3);
    if (!(lim->granules * 50 < full->granules && lim->rows * 50 < full->rows)) {
        fprintf(stderr, "%s: a limit of 10 hourly buckets covered %llu granules, %llu rows (all: %llu, %llu)\n",
                what, (unsigned long long)lim->granules, (unsigned long long)lim->rows,
                (unsigned long long)full->granules, (unsigned long long)full->rows);
        failures++;
    }
}

static void same_cut(const char *what, const void *got, size_t ngot, const void *all, size_t nall, size_t sz,
                     bool desc, size_t limit);

static void test_stats(void)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/stats", base);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.wal_max_batch_rows = 64;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t s;
    OK(tsdb_series_create(db, "year", TSDB_FLOAT64, NULL, &s));
    int64_t ts[1440];
    double v[1440];
    for (int d = 0; d < 366; d++) {
        for (int i = 0; i < 1440; i++) {
            ts[i] = YEAR_T0 + d * DAY_MS + i * MIN_MS;
            v[i] = 20 + 5 * sin((double)(d * 1440 + i) / 300.0);
        }
        OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, 1440));
    }
    OK(tsdb_flush(db));
    int64_t start = YEAR_T0, end = YEAR_T0 + 366 * DAY_MS;
    printf("test_limits: a year of one row a minute, 10 hourly buckets\n");
    tsdb_group hour = {TSDB_HOUR, 1, NULL};
    size_t nb = 366 * 24;
    int64_t *bounds = malloc(nb * sizeof *bounds);
    for (size_t i = 0; i < nb; i++)
        bounds[i] = start + (int64_t)i * HOUR_MS;
    for (int desc = 0; desc < 2; desc++) {
        tsdb_query_opts lim = {desc ? TSDB_DESC : TSDB_ASC, 10};
        tsdb_twa_opts tlim = {.order = lim.order, .limit = 10}, tall = {0};
        tsdb_query_stats sl, sa;
        char what[64];
        tsdb_bucket *a, *b;
        size_t na, nb2;

        OK(tsdb_query_group_ex(db, s, start, end, &hour, &lim, &b, &nb2));
        sl = stats();
        OK(tsdb_query_group_ex(db, s, start, end, &hour, NULL, &a, &na));
        sa = stats();
        snprintf(what, sizeof what, "group hour %s", desc ? "desc" : "asc");
        few_granules(what, &sl, &sa);
        same_cut(what, b, nb2, a, na, sizeof *a, desc, 10);
        free(a), free(b);

        OK(tsdb_query_bounds(db, s, start, end, bounds, nb, &lim, &b, &nb2));
        sl = stats();
        OK(tsdb_query_bounds(db, s, start, end, bounds, nb, NULL, &a, &na));
        sa = stats();
        snprintf(what, sizeof what, "bounds %s", desc ? "desc" : "asc");
        few_granules(what, &sl, &sa);
        same_cut(what, b, nb2, a, na, sizeof *a, desc, 10);
        free(a), free(b);

#ifndef LIMITS_STATS_ONLY
        OK(tsdb_query_agg_ex(db, s, start, end, HOUR_MS, &lim, &b, &nb2));
        sl = stats();
        OK(tsdb_query_agg_ex(db, s, start, end, HOUR_MS, NULL, &a, &na));
        sa = stats();
        snprintf(what, sizeof what, "agg_ex %s", desc ? "desc" : "asc");
        few_granules(what, &sl, &sa);
        same_cut(what, b, nb2, a, na, sizeof *a, desc, 10);
        free(a), free(b);
#endif

        tsdb_spread_bucket *sx, *sy;
        OK(tsdb_query_spread(db, s, start, end, HOUR_MS, &lim, &sx, &nb2));
        sl = stats();
        OK(tsdb_query_spread(db, s, start, end, HOUR_MS, NULL, &sy, &na));
        sa = stats();
        snprintf(what, sizeof what, "spread %s", desc ? "desc" : "asc");
        few_granules(what, &sl, &sa);
        same_cut(what, sx, nb2, sy, na, sizeof *sx, desc, 10);
        free(sx), free(sy);

        tsdb_twa_bucket *tx, *ty;
        OK(tsdb_query_twa(db, s, start, end, HOUR_MS, &tlim, &tx, &nb2));
        sl = stats();
        OK(tsdb_query_twa(db, s, start, end, HOUR_MS, &tall, &ty, &na));
        sa = stats();
        snprintf(what, sizeof what, "twa %s", desc ? "desc" : "asc");
        few_granules(what, &sl, &sa);
        same_cut(what, tx, nb2, ty, na, sizeof *tx, desc, 10);
        free(tx), free(ty);
    }
    free(bounds);
    OK(tsdb_close(db));
    rmrf(dir);
}

/* ---------------------------------------- equality with compute-then-cut */

static void same_cut(const char *what, const void *got, size_t ngot, const void *all, size_t nall, size_t sz,
                     bool desc, size_t limit)
{
    size_t want = limit && limit < nall ? limit : nall;
    const char *g = got, *a = all;
    size_t bad = want;
    if (ngot == want)
        for (size_t i = 0; i < want; i++)
            if (memcmp(g + i * sz, a + (desc ? nall - 1 - i : i) * sz, sz)) {
                bad = i;
                break;
            }
    if (ngot != want || bad != want) {
        fprintf(stderr, "%s (%s, limit %zu): %zu results, %zu expected, first difference at %zu\n", what,
                desc ? "desc" : "asc", limit, ngot, want, bad);
        failures++;
    }
}

#ifndef LIMITS_STATS_ONLY
static uint64_t rng = 0x2545f4914f6cdd1dull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}
static int64_t rndr(int64_t n) { return (int64_t)(rnd() % (uint64_t)n); }

#define T0 1700000000000LL
#define SPAN (3 * DAY_MS)
#define NSERIES 4
static const tsdb_type types[NSERIES] = {TSDB_FLOAT64, TSDB_FLOAT32, TSDB_INT64, TSDB_BOOL};

static int64_t clock_now;
static int64_t test_clock(void *arg)
{
    (void)arg;
    return __atomic_load_n(&clock_now, __ATOMIC_SEQ_CST);
}

static tsdb *open_rand(const char *dir, int threads, bool follow)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 64;
    o.partition_ms = 6 * HOUR_MS;
    o.merge_threads = 0; /* no merge between the limited query and the full one: same rows, same bits */
    o.query_threads = threads;
    o.flush_interval_ms = 600000;
    o.buffer_rows = 1 << 22;
    o.wal_max_batch_rows = 256;
    o.clock_ms = test_clock;
    o.follow = follow;
    o.refresh_ms = 20;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

/* irregular timestamps, with gaps of hours now and then: empty buckets, and TWA values carried in */
static int64_t *times;
static size_t ntimes;

static void make_times(void)
{
    size_t cap = 1 << 16;
    times = malloc(cap * sizeof *times);
    ntimes = 0;
    for (int64_t t = T0 + rndr(1000); t < T0 + SPAN; t += 1 + (rnd() % 40 ? rndr(60000) : rndr(5 * HOUR_MS))) {
        if (ntimes == cap)
            times = realloc(times, (cap *= 2) * sizeof *times);
        times[ntimes++] = t;
    }
}

/* a round of writes: every k-th row of a random stretch, with fresh values (replacing older ones), nulls,
 * and for floats NaN and infinities now and then */
static void write_round(tsdb *db, const uint32_t *ids)
{
    size_t a = (size_t)rndr((int64_t)ntimes), b = a + 1 + (size_t)rndr((int64_t)(ntimes - a));
    size_t k = 1 + (size_t)rndr(3);
    size_t cap = (b - a) / k + 1;
    int64_t *ts = malloc(cap * sizeof *ts);
    uint8_t *vals = malloc(cap * 8), *valid = calloc(cap / 8 + 1, 1);
    for (int s = 0; s < NSERIES; s++) {
        size_t n = 0;
        memset(valid, 0, cap / 8 + 1);
        for (size_t i = a; i < b; i += k, n++) {
            ts[n] = times[i];
            if (rnd() % 50)
                valid[n >> 3] |= (uint8_t)(1u << (n & 7));
            double x = (double)rndr(2000000) / 1000.0 - 1000.0;
            uint64_t r = rnd() % 400;
            if (r == 0)
                x = NAN;
            else if (r == 1)
                x = INFINITY;
            else if (r == 2)
                x = -INFINITY;
            switch (types[s]) {
            case TSDB_FLOAT64: ((double *)vals)[n] = x; break;
            case TSDB_FLOAT32: ((float *)vals)[n] = (float)x; break;
            case TSDB_INT64: ((int64_t *)vals)[n] = (int64_t)(rnd() % 2000001) - 1000000; break;
            default: vals[n] = rnd() & 1; break;
            }
        }
        OK(tsdb_insert_series_values(db, ids[s], ts, &(tsdb_values){types[s], vals, valid}, n));
    }
    free(ts), free(vals), free(valid);
}

static void rand_range(int64_t *start, int64_t *end)
{
    switch (rnd() % 6) {
    case 0: *start = T0 - DAY_MS, *end = T0 + SPAN + DAY_MS; break; /* everything */
    case 1: *start = TSDB_TS_MIN, *end = T0 + rndr(SPAN + DAY_MS); break;
    default:
        *start = T0 - HOUR_MS + rndr(SPAN + 2 * HOUR_MS);
        *end = *start + 1 + rndr(rnd() % 3 ? SPAN / 4 : SPAN);
    }
}

static int64_t rand_interval(void)
{
    static const int64_t iv[] = {0, MIN_MS, 5 * MIN_MS, HOUR_MS, 7 * HOUR_MS + 13, DAY_MS};
    return rnd() % 4 ? iv[rnd() % 6] : 1000 + rndr(10 * HOUR_MS);
}

static const tsdb_group *rand_group(void)
{
    static const tsdb_group g[] = {{TSDB_MINUTE, 30, NULL}, {TSDB_HOUR, 1, NULL}, {TSDB_HOUR, 5, NULL},
                                   {TSDB_DAY, 1, "UTC"}, {TSDB_SECOND, 7200, NULL}};
    return rnd() % 8 ? &g[rnd() % 5] : NULL;
}

static size_t rand_bounds(int64_t start, int64_t end, int64_t *b)
{
    size_t n = 1 + (size_t)rndr(40);
    int64_t lo = start == TSDB_TS_MIN ? T0 - HOUR_MS : start - HOUR_MS, w = (end - lo) / (int64_t)n;
    if (w < 1)
        w = 1;
    b[0] = lo + rndr(w + HOUR_MS);
    for (size_t i = 1; i < n; i++)
        b[i] = b[i - 1] + 1 + rndr(2 * w);
    return n;
}

static size_t rand_limit(size_t nall)
{
    switch (rnd() % 5) {
    case 0: return 1;
    case 1: return 2 + (size_t)rndr(5);
    case 2: return nall + (size_t)rndr(3);
    default: return 1 + (size_t)rndr((int64_t)nall + 1);
    }
}

/* LTTB: collect what the callback gets */
typedef struct {
    int64_t ts;
    uint64_t v;
    int valid;
} lpoint;
typedef struct {
    lpoint *p;
    size_t n, cap;
} lpoints;

static int collect(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    lpoints *l = ctx;
    size_t w = v->type == TSDB_FLOAT64 || v->type == TSDB_INT64 ? 8 : 4;
    if (v->type == TSDB_BOOL)
        w = 1;
    for (size_t i = 0; i < n; i++) {
        if (l->n == l->cap)
            l->p = realloc(l->p, (l->cap = l->cap ? 2 * l->cap : 256) * sizeof *l->p);
        lpoint *p = &l->p[l->n++];
        memset(p, 0, sizeof *p);
        p->ts = ts[i];
        memcpy(&p->v, (const char *)v->values + i * w, w);
        p->valid = !v->valid || (v->valid[i >> 3] >> (i & 7) & 1);
    }
    return 0;
}

static int stop_at_once(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n) { return 1; }

/* LTTB with a threshold above the point count keeps every point: newest first is then a backwards read that stops
 * at the limit, not every point of the year buffered and turned round. A caller that stops on the chunk that
 * reaches the limit still gets TSDB_ERR_STOPPED. */
static void test_lttb_stats(void)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/lttb", base);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t s;
    OK(tsdb_series_create(db, "year", TSDB_FLOAT64, NULL, &s));
    int64_t ts[1440];
    double v[1440];
    for (int d = 0; d < 366; d++) {
        for (int i = 0; i < 1440; i++)
            ts[i] = YEAR_T0 + d * DAY_MS + i * MIN_MS, v[i] = d * 1440 + i;
        OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, 1440));
    }
    OK(tsdb_flush(db));
    int64_t start = YEAR_T0, end = YEAR_T0 + 366 * DAY_MS;
    const size_t every = (size_t)1 << 40;
    for (int desc = 0; desc < 2; desc++) {
        tsdb_query_opts lim = {desc ? TSDB_DESC : TSDB_ASC, 10};
        lpoints a = {0}, b = {0};
        OK(tsdb_query_lttb_ex(db, s, start, end, every, &lim, collect, &b));
        tsdb_query_stats sl = stats();
        OK(tsdb_query_lttb_ex(db, s, start, end, every, NULL, collect, &a));
        tsdb_query_stats sa = stats();
        /* raw reads newest first go in windows of desc_window_rows (64 K rows), not granule by granule */
        if (!(sl.rows * 4 < sa.rows))
            fprintf(stderr, "  lttb %s limit 10, every point: %llu of %llu rows read\n", desc ? "desc" : "asc",
                    (unsigned long long)sl.rows, (unsigned long long)sa.rows);
        CHECK(sl.rows * 4 < sa.rows);
        same_cut("lttb every point", b.p, b.n, a.p, a.n, sizeof *a.p, desc, 10);
        free(a.p), free(b.p);
        CHECK(tsdb_query_lttb_ex(db, s, start, end, every, &lim, stop_at_once, NULL) == TSDB_ERR_STOPPED);
        CHECK(tsdb_query_lttb_ex(db, s, start, end, 1000, &lim, stop_at_once, NULL) == TSDB_ERR_STOPPED);
    }
    OK(tsdb_close(db));
    rmrf(dir);
}

/* TWA where the only readings near the range are memtable rows: the window bounds must see the memtable, or a
 * limited query finds nothing (the random rounds always had part rows within reach). */
static void test_twa_memtable(void)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/twamem", base);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t s;
    OK(tsdb_series_create(db, "twamem", TSDB_FLOAT64, NULL, &s));
    int64_t ts[8];
    double v[8];
    for (int i = 0; i < 8; i++)
        ts[i] = T0 + i * MIN_MS, v[i] = i;
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, 8));
    OK(tsdb_flush(db)); /* old readings in a part, days before the range */
    for (int i = 0; i < 8; i++)
        ts[i] = T0 + 5 * DAY_MS + i * 20 * MIN_MS, v[i] = 10 + i;
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, 8)); /* the memtable */
    int64_t start = T0 + 4 * DAY_MS, end = T0 + 6 * DAY_MS;
    tsdb_twa_bucket *all, *got;
    size_t nall, ngot;
    tsdb_twa_opts none = {0};
    OK(tsdb_query_twa(db, s, start, end, HOUR_MS, &none, &all, &nall));
    CHECK(nall >= 2);
    for (int desc = 0; desc < 2; desc++) {
        tsdb_twa_opts lim = {.order = desc ? TSDB_DESC : TSDB_ASC, .limit = 2};
        OK(tsdb_query_twa(db, s, start, end, HOUR_MS, &lim, &got, &ngot));
        same_cut(desc ? "twa memtable only, desc" : "twa memtable only, asc", got, ngot, all, nall, sizeof *all, desc,
                 2);
        free(got);
    }
    free(all);
    OK(tsdb_close(db));
    rmrf(dir);
}

/* An oracle that shares no code with the aggregation: every row of the range read raw (last write wins, deletes
 * and retention applied by the raw path), folded here into the interval's buckets. Counts and integer sums,
 * minimums and maximums must match exactly. `stride` steps over tsdb_bucket or tsdb_spread_bucket. */
static int64_t fldiv(int64_t a, int64_t b) { return a / b - (a % b != 0 && (a < 0) != (b < 0)); }

static void naive_agg(tsdb *db, uint32_t id, int64_t start, int64_t end, int64_t iv, const void *res, size_t n,
                      size_t stride, const char *what)
{
    tsdb_type type;
    OK(tsdb_series_type(db, id, &type));
    lpoints raw = {0};
    OK(tsdb_query_values(db, id, start, end, NULL, collect, &raw));
#define NB(k) ((const tsdb_bucket *)((const char *)res + (k) * stride))
    uint64_t *cnt = calloc(n + 1, sizeof *cnt);
    int64_t *sum = calloc(n + 1, sizeof *sum), *mn = calloc(n + 1, sizeof *mn), *mx = calloc(n + 1, sizeof *mx);
    size_t lost = 0, bad = 0;
    for (size_t i = 0; i < raw.n; i++) {
        if (!raw.p[i].valid)
            continue;
        int64_t key = iv ? fldiv(raw.p[i].ts, iv) : 0;
        size_t lo = 0, hi = n; /* the bucket of key: results ascend */
        while (lo < hi) {
            size_t m = lo + (hi - lo) / 2;
            if ((iv ? fldiv(NB(m)->start, iv) : 0) < key)
                lo = m + 1;
            else
                hi = m;
        }
        if (lo == n || (iv ? fldiv(NB(lo)->start, iv) : 0) != key) {
            lost++;
            continue;
        }
        if (type == TSDB_INT64) { /* only int64 sums are compared; other types' bits would overflow the sum */
            int64_t x = (int64_t)raw.p[i].v;
            if (cnt[lo] == 0 || x < mn[lo])
                mn[lo] = x;
            if (cnt[lo] == 0 || x > mx[lo])
                mx[lo] = x;
            sum[lo] += x;
        }
        cnt[lo]++;
    }
    for (size_t k = 0; k < n; k++)
        bad += NB(k)->count != cnt[k] || (type == TSDB_INT64 && cnt[k] &&
                                          (NB(k)->isum != sum[k] || NB(k)->imin != mn[k] || NB(k)->imax != mx[k]));
#undef NB
    if (lost || bad) {
        fprintf(stderr, "%s: against the raw rows, %zu rows in no bucket, %zu buckets differ\n", what, lost, bad);
        failures++;
    }
    free(cnt), free(sum), free(mn), free(mx), free(raw.p);
}

static int queries;

static void check_series(tsdb *db, uint32_t id, const char *stage, int rounds)
{
    char what[256];
    int64_t bounds[64];
    for (int r = 0; r < rounds; r++) {
        int64_t start, end;
        rand_range(&start, &end);
        bool desc = rnd() & 1;
        tsdb_order ord = desc ? TSDB_DESC : TSDB_ASC;
        int kind = (int)(rnd() % 9);
        int64_t iv = rand_interval();
        const tsdb_group *g = rand_group();
        size_t nbo = rand_bounds(start, end, bounds);
        size_t na, nl;
        int rc;
        snprintf(what, sizeof what, "%s, series %u, kind %d, [%lld, %lld), interval %lld, group %d/%u, %zu bounds",
                 stage, id, kind, (long long)start, (long long)end, (long long)iv, g ? (int)g->unit : 0,
                 g ? g->count : 0, nbo);
        queries++;
        if (kind < 3) { /* agg_ex, group_ex, bounds */
            tsdb_bucket *a = NULL, *l = NULL;
            rc = kind == 0   ? tsdb_query_agg_ex(db, id, start, end, iv, NULL, &a, &na)
                 : kind == 1 ? tsdb_query_group_ex(db, id, start, end, g, NULL, &a, &na)
                             : tsdb_query_bounds(db, id, start, end, bounds, nbo, NULL, &a, &na);
            if (rc == TSDB_ERR_TOO_MANY_BUCKETS)
                continue;
            OK(rc);
            tsdb_query_opts o = {ord, rand_limit(na)};
            rc = kind == 0   ? tsdb_query_agg_ex(db, id, start, end, iv, &o, &l, &nl)
                 : kind == 1 ? tsdb_query_group_ex(db, id, start, end, g, &o, &l, &nl)
                             : tsdb_query_bounds(db, id, start, end, bounds, nbo, &o, &l, &nl);
            OK(rc);
            same_cut(what, l, nl, a, na, sizeof *a, desc, o.limit);
            if (kind == 0)
                naive_agg(db, id, start, end, iv, a, na, sizeof *a, what);
            if (kind == 0) { /* tsdb_query_agg is agg_ex without options */
                tsdb_bucket *p;
                size_t np;
                OK(tsdb_query_agg(db, id, start, end, iv, &p, &np));
                same_cut(what, p, np, a, na, sizeof *a, false, 0);
                free(p);
            }
            free(a), free(l);
        } else if (kind < 6) { /* the spread in its three forms */
            tsdb_spread_bucket *a = NULL, *l = NULL;
            rc = kind == 3   ? tsdb_query_spread(db, id, start, end, iv, NULL, &a, &na)
                 : kind == 4 ? tsdb_query_spread_group(db, id, start, end, g, NULL, &a, &na)
                             : tsdb_query_spread_bounds(db, id, start, end, bounds, nbo, NULL, &a, &na);
            if (rc == TSDB_ERR_TOO_MANY_BUCKETS)
                continue;
            OK(rc);
            tsdb_query_opts o = {ord, rand_limit(na)};
            rc = kind == 3   ? tsdb_query_spread(db, id, start, end, iv, &o, &l, &nl)
                 : kind == 4 ? tsdb_query_spread_group(db, id, start, end, g, &o, &l, &nl)
                             : tsdb_query_spread_bounds(db, id, start, end, bounds, nbo, &o, &l, &nl);
            OK(rc);
            same_cut(what, l, nl, a, na, sizeof *a, desc, o.limit);
            if (kind == 3)
                naive_agg(db, id, start, end, iv, a, na, sizeof *a, what);
            free(a), free(l);
        } else { /* TWA: STEP or LINEAR, with or without max_gap */
            tsdb_twa_opts all = {.method = rnd() & 1 ? TSDB_TWA_LINEAR : TSDB_TWA_STEP,
                                 .max_gap_ms = rnd() & 1 ? 0 : 1 + rndr(2 * HOUR_MS),
                                 .no_stats = rnd() % 8 == 0};
            tsdb_twa_bucket *a = NULL, *l = NULL;
            rc = kind == 6   ? tsdb_query_twa(db, id, start, end, iv, &all, &a, &na)
                 : kind == 7 ? tsdb_query_twa_group(db, id, start, end, g, &all, &a, &na)
                             : tsdb_query_twa_bounds(db, id, start, end, bounds, nbo, &all, &a, &na);
            if (rc == TSDB_ERR_TOO_MANY_BUCKETS)
                continue;
            OK(rc);
            tsdb_twa_opts lo = all;
            lo.order = ord;
            lo.limit = rand_limit(na);
            rc = kind == 6   ? tsdb_query_twa(db, id, start, end, iv, &lo, &l, &nl)
                 : kind == 7 ? tsdb_query_twa_group(db, id, start, end, g, &lo, &l, &nl)
                             : tsdb_query_twa_bounds(db, id, start, end, bounds, nbo, &lo, &l, &nl);
            OK(rc);
            char w2[320];
            snprintf(w2, sizeof w2, "%s, twa %s max_gap %lld%s", what, all.method ? "linear" : "step",
                     (long long)all.max_gap_ms, all.no_stats ? " no_stats" : "");
            same_cut(w2, l, nl, a, na, sizeof *a, desc, lo.limit);
            free(a), free(l);
        }
    }
    /* LTTB: newest first is the oldest-first choice reversed, and a limit cuts it */
    for (int r = 0; r < rounds / 8 + 1; r++) {
        int64_t start, end;
        rand_range(&start, &end);
        size_t thr = 3 + (size_t)rndr(rnd() & 1 ? 20 : 400);
        lpoints a = {0}, l = {0};
        int rc = tsdb_query_lttb_ex(db, id, start, end, thr, NULL, collect, &a);
        if (rc == TSDB_ERR_INVALID)
            continue;
        OK(rc);
        bool desc = rnd() & 1;
        tsdb_query_opts o = {desc ? TSDB_DESC : TSDB_ASC, rnd() & 1 ? 0 : rand_limit(a.n)};
        OK(tsdb_query_lttb_ex(db, id, start, end, thr, &o, collect, &l));
        snprintf(what, sizeof what, "%s, series %u, lttb [%lld, %lld) threshold %zu", stage, id, (long long)start,
                 (long long)end, thr);
        same_cut(what, l.p, l.n, a.p, a.n, sizeof *a.p, desc, o.limit);
        queries++;
        free(a.p), free(l.p);
    }
}

static void check_all(tsdb *db, const uint32_t *ids, const char *stage, int rounds)
{
    for (int s = 0; s < NSERIES; s++)
        check_series(db, ids[s], stage, rounds);
}

/* Float sums do not depend on how a query is split over threads: the writer (`threads` query threads) and
 * the follower (one) give the same bits for the same parts. */
static void same_threads(tsdb *db, tsdb *f, const uint32_t *ids, int threads)
{
    char what[128];
    for (int s = 0; s < NSERIES; s++)
        for (int k = 0; k < 4; k++) {
            int64_t iv = k == 0 ? 0 : k == 1 ? MIN_MS : k == 2 ? HOUR_MS : DAY_MS;
            tsdb_spread_bucket *a, *b;
            size_t na, nb;
            OK(tsdb_query_spread(db, ids[s], TSDB_TS_MIN, TSDB_TS_MAX, iv, NULL, &a, &na));
            OK(tsdb_query_spread(f, ids[s], TSDB_TS_MIN, TSDB_TS_MAX, iv, NULL, &b, &nb));
            snprintf(what, sizeof what, "%d threads against 1, series %u, interval %lld", threads, ids[s],
                     (long long)iv);
            same_cut(what, a, na, b, nb, sizeof *a, false, 0);
            free(a), free(b);
        }
}

static void test_random(int threads, int rounds)
{
    char dir[600], stage[64];
    snprintf(dir, sizeof dir, "%s/rand%d", base, threads);
    rmrf(dir);
    __atomic_store_n(&clock_now, T0 + SPAN + DAY_MS, __ATOMIC_SEQ_CST);
    tsdb *db = open_rand(dir, threads, false);
    uint32_t ids[NSERIES];
    for (int s = 0; s < NSERIES; s++) {
        char name[32];
        snprintf(name, sizeof name, "s%d", s);
        OK(tsdb_series_create(db, name, types[s], NULL, &ids[s]));
    }
    make_times();
    /* overlapping parts with replaced values, deletes and replaces between them, the last rounds in the
     * memtable (on top of the deletes' parts) */
    for (int round = 0; round < 12; round++) {
        write_round(db, ids);
        if (round % 3 == 2) {
            uint32_t id = ids[rnd() % NSERIES];
            int64_t a = T0 + rndr(SPAN), b = a + 1 + rndr(rnd() & 1 ? MIN_MS : 6 * HOUR_MS);
            OK(tsdb_delete_range(db, id, a, b));
        }
        if (round < 9)
            OK(tsdb_flush(db));
    }
    snprintf(stage, sizeof stage, "%d threads, parts and memtable", threads);
    check_all(db, ids, stage, rounds);

    tsdb *f = open_rand(dir, 1, true);
    OK(tsdb_refresh(f));
    snprintf(stage, sizeof stage, "%d threads, follower", threads);
    check_all(f, ids, stage, rounds / 2);

    OK(tsdb_flush(db));
    snprintf(stage, sizeof stage, "%d threads, flushed", threads);
    check_all(db, ids, stage, rounds / 2);

    /* retention: the first day of every series expires, merged away later */
    for (int s = 0; s < NSERIES; s++)
        OK(tsdb_set_retention(db, ids[s], SPAN));
    snprintf(stage, sizeof stage, "%d threads, retention", threads);
    check_all(db, ids, stage, rounds / 2);
    OK(tsdb_refresh(f));
    snprintf(stage, sizeof stage, "%d threads, retention, follower", threads);
    check_all(f, ids, stage, rounds / 4);

    OK(tsdb_optimize(db));
    snprintf(stage, sizeof stage, "%d threads, optimized", threads);
    check_all(db, ids, stage, rounds / 2);
    OK(tsdb_refresh(f));
    same_threads(db, f, ids, threads);

    OK(tsdb_close(f));
    OK(tsdb_close(db));
    free(times);
    rmrf(dir);
}
#endif

/* Rows at the ends of int64 time, decoded from a part with 1 ms buckets: the bucket of TSDB_TS_MIN has the key
 * INT64_MIN, which the edge folding once took for "no edge" and dropped. Every form, every order. */
static void test_ends(void)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/ends", base);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t s;
    OK(tsdb_series_create(db, "ends", TSDB_FLOAT64, NULL, &s));
    enum { N = 10 };
    int64_t ts[N];
    double v[N];
    for (int i = 0; i < 5; i++) {
        ts[i] = TSDB_TS_MIN + i, v[i] = 1 + i;
        ts[5 + i] = TSDB_TS_MAX - 5 + i, v[5 + i] = 10 + i;
    }
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, N));
    OK(tsdb_flush(db));
    for (int k = 0; k < 12; k++) { /* each end on its own: 1 ms buckets over all of int64 are too many */
        int e = k / 6;
        int64_t lo = e ? TSDB_TS_MAX - 100 : TSDB_TS_MIN, hi = e ? TSDB_TS_MAX : TSDB_TS_MIN + 100;
        tsdb_query_opts q = {k & 1 ? TSDB_DESC : TSDB_ASC, (size_t)(k % 6 >> 1) * 2}; /* limits 0, 2, 4 */
        size_t want = q.limit ? q.limit : 5;
        tsdb_bucket *b;
        tsdb_spread_bucket *sb;
        size_t n, ns;
        OK(tsdb_query_agg_ex(db, s, lo, hi, 1, &q, &b, &n));
        OK(tsdb_query_spread(db, s, lo, hi, 1, &q, &sb, &ns));
        if (n != want || ns != want)
            fprintf(stderr, "  ends: %s order %d limit %zu: %zu and %zu spread buckets, want %zu\n", e ? "max" : "min",
                    k & 1, q.limit, n, ns, want);
        CHECK(n == want && ns == want);
        for (size_t i = 0; i < n && i < ns && n == want; i++) {
            size_t r = 5 * (size_t)e + (k & 1 ? 4 - i : i);
            CHECK(b[i].start == ts[r] && b[i].count == 1 && b[i].sum == v[r] && b[i].min == v[r]);
            CHECK(sb[i].b.start == ts[r] && sb[i].b.count == 1 && sb[i].b.sum == v[r] && sb[i].range == 0);
        }
        free(b);
        free(sb);
    }
    OK(tsdb_close(db));
    rmrf(dir);
}

int main(int argc, char **argv)
{
    snprintf(base, sizeof base, "%s/tsdb_limits_%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rmrf(base);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "mkdir -p '%s'", base);
    if (system(cmd) != 0)
        return 1;
#ifndef LIMITS_STATS_ONLY
    if (argc > 2) /* another seed */
        rng = strtoull(argv[2], NULL, 0) | 1;
    test_random(1, 300);
    test_random(4, 200);
    printf("test_limits: %d random queries against compute-all-then-cut\n", queries);
#endif
    test_stats();
    test_ends();
    test_lttb_stats();
    test_twa_memtable();
    rmrf(base);
    if (failures) {
        fprintf(stderr, "test_limits: %d failures\n", failures);
        return 1;
    }
    printf("test_limits: ok\n");
    return 0;
}
