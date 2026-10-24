/*
 * Time-weighted averages (tsdb_query_twa*): hand-computed cases for each rule,
 * then random data of every type against a reference implementation that
 * follows the documented rules on plain arrays in long double: bucket by
 * bucket, covered_ms and count exactly, areas within a relative tolerance
 * (summation order differs: granule statistics are summed inside the granule).
 * The data is spread over many parts and the memtable, with corrections (last
 * write wins), and checked again after merges and from a follower. One test
 * checks that whole granules inside a bucket are answered from statistics.
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

static uint64_t rng = 88172645463325252ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}
static int64_t rndr(int64_t n) { return (int64_t)(rnd() % (uint64_t)n); }

static char base[512];

static void rmrf(const char *d)
{
    char cmd[1100];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", d);
    if (system(cmd) != 0)
        fprintf(stderr, "rm failed\n");
}

static tsdb *open_db(const char *name, uint32_t granule_rows, char *dir)
{
    snprintf(dir, 600, "%s/%s", base, name);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = granule_rows;
    o.merge_threads = 0;
    o.flush_interval_ms = 600000;
    o.buffer_rows = 1 << 22;
    o.wal_max_batch_rows = 64;
    o.partition_ms = 86400000LL * 3650;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

/* ---------------------------------------------------------- reference */

/* the final state of a series: sorted unique timestamps */
typedef struct ref_series {
    int type;
    size_t n;
    int64_t *ts;
    long double *v;
    bool *ok;
} ref_series;

typedef struct ref_bk {
    int kind; /* 0: one bucket, 1: fixed interval, 2: bounds */
    int64_t interval;
    const int64_t *bounds;
    size_t nb;
} ref_bk;

typedef struct rbucket {
    int64_t start;
    long double area, absarea;
    int64_t ms;
    uint64_t count;
    bool nan;
} rbucket;

static int64_t floordiv64(int64_t a, int64_t b) { return a / b - (a % b < 0); }

/* bucket [lo, hi) holding t, and its reported start */
static void ref_bucket(const ref_bk *b, int64_t qstart, int64_t t, int64_t *lo, int64_t *hi, int64_t *rep)
{
    if (b->kind == 0) {
        *lo = INT64_MIN;
        *hi = INT64_MAX;
        *rep = qstart;
    } else if (b->kind == 1) {
        *lo = floordiv64(t, b->interval) * b->interval;
        *hi = *lo + b->interval;
        *rep = *lo;
    } else {
        size_t i = 0;
        while (i + 1 < b->nb && b->bounds[i + 1] <= t)
            i++;
        *lo = b->bounds[i];
        *hi = i + 1 < b->nb ? b->bounds[i + 1] : INT64_MAX;
        *rep = *lo;
    }
}

static rbucket *rb_get(rbucket **v, size_t *n, size_t *cap, int64_t start)
{
    if (*n && (*v)[*n - 1].start == start) /* buckets come in time order */
        return &(*v)[*n - 1];
    if (*n == *cap) {
        *cap = *cap ? *cap * 2 : 64;
        *v = realloc(*v, *cap * sizeof **v);
    }
    rbucket *r = &(*v)[(*n)++];
    memset(r, 0, sizeof *r);
    r->start = start;
    return r;
}

static int cmp_rb(const void *a, const void *b)
{
    const rbucket *x = a, *y = b;
    return (x->start > y->start) - (x->start < y->start);
}

static size_t reference(const ref_series *s, int64_t start, int64_t end, const ref_bk *b, bool linear,
                        int64_t max_gap, rbucket **out)
{
    rbucket *v = NULL;
    size_t n = 0, cap = 0;
    if (b->kind == 2 && start < b->bounds[0])
        start = b->bounds[0];
    for (size_t i = 0; i < s->n; i++) {
        int64_t lo, hi, rep;
        if (s->ok[i] && s->ts[i] >= start && s->ts[i] < end) {
            ref_bucket(b, start, s->ts[i], &lo, &hi, &rep);
            rb_get(&v, &n, &cap, rep)->count++;
        }
        if (i + 1 == s->n || !s->ok[i] || (linear && !s->ok[i + 1]))
            continue;
        int64_t t0 = s->ts[i], t1 = s->ts[i + 1];
        if (max_gap && t1 - t0 > max_gap)
            continue;
        long double v0 = s->v[i], v1 = s->v[i + 1];
        int64_t x = t0 > start ? t0 : start, y = t1 < end ? t1 : end;
        while (x < y) {
            ref_bucket(b, start, x, &lo, &hi, &rep);
            int64_t e = hi < y ? hi : y;
            long double a, len = (long double)(e - x);
            if (linear) {
                long double va = v0 + (v1 - v0) * (long double)(x - t0) / (long double)(t1 - t0);
                long double vb = v0 + (v1 - v0) * (long double)(e - t0) / (long double)(t1 - t0);
                a = (va + vb) / 2 * len;
            } else {
                a = v0 * len;
            }
            rbucket *r = rb_get(&v, &n, &cap, rep);
            if (isnan(a))
                r->nan = true;
            else
                r->area += a;
            r->absarea += fabsl(a);
            r->ms += e - x;
            x = e;
        }
    }
    if (n)
        qsort(v, n, sizeof *v, cmp_rb);
    *out = v;
    return n;
}

static int compare(const char *what, const rbucket *r, size_t nr, const tsdb_twa_bucket *b, size_t nb)
{
    int bad = 0;
    if (nr != nb) {
        fprintf(stderr, "%s: %zu buckets, reference %zu\n", what, nb, nr);
        return 1;
    }
    for (size_t i = 0; i < nr && bad < 3; i++) {
        bool area_ok;
        if (r[i].nan || !isfinite(r[i].area)) /* inf - inf, inf * 0: NaN or an infinity, both not finite */
            area_ok = !isfinite(b[i].area);
        else
            area_ok = fabsl((long double)b[i].area - r[i].area) <= 1e-9L * r[i].absarea + 1e-9L;
        bool twa_ok = r[i].ms ? (isnan(b[i].twa) == isnan(b[i].area)) : isnan(b[i].twa);
        if (b[i].start != r[i].start || b[i].count != r[i].count || b[i].covered_ms != r[i].ms || !area_ok ||
            !twa_ok) {
            fprintf(stderr,
                    "%s: bucket %zu start %lld/%lld count %llu/%llu ms %lld/%lld area %.17g/%.17Lg twa %.17g\n",
                    what, i, (long long)b[i].start, (long long)r[i].start, (unsigned long long)b[i].count,
                    (unsigned long long)r[i].count, (long long)b[i].covered_ms, (long long)r[i].ms, b[i].area,
                    r[i].area, b[i].twa);
            bad++;
        }
    }
    return bad;
}

/* ------------------------------------------------------- hand cases */

static void query1(tsdb *db, uint32_t s, int64_t start, int64_t end, int64_t interval, int method, int64_t gap,
                   tsdb_twa_bucket **b, size_t *n)
{
    tsdb_twa_opts o = {.method = method, .max_gap_ms = gap};
    OK(tsdb_query_twa(db, s, start, end, interval, &o, b, n));
}

static bool near(double a, double b) { return fabs(a - b) <= 1e-9 * (fabs(b) + 1); }

static void test_rules(void)
{
    char dir[600];
    tsdb *db = open_db("rules", 4, dir);
    uint32_t s;
    OK(tsdb_series_create(db, "x", TSDB_FLOAT64, NULL, &s));
    /* 10 at 0, 20 at 1000, null at 2000, 30 at 3000, 40 at 10000 */
    int64_t ts[] = {0, 1000, 2000, 3000, 10000};
    double v[] = {10, 20, 0, 30, 40};
    uint8_t valid = 0xff & ~(1u << 2);
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, &valid}, 5));
    tsdb_twa_bucket *b;
    size_t n;
    /* STEP, one bucket: 10*1000 + 20*1000 + (null: nothing) + 30*7000 */
    query1(db, s, 0, 20000, 0, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 1 && b[0].start == 0 && b[0].covered_ms == 9000 && b[0].count == 4);
    CHECK(n == 1 && near(b[0].area, 240000) && near(b[0].twa, 240000.0 / 9000));
    free(b);
    /* LINEAR: 15*1000 + (null both sides: nothing) + 35*7000 */
    query1(db, s, 0, 20000, 0, TSDB_TWA_LINEAR, 0, &b, &n);
    CHECK(n == 1 && b[0].covered_ms == 8000 && near(b[0].area, 15000 + 245000));
    free(b);
    /* max_gap 5000: the 7 s segment is stale */
    query1(db, s, 0, 20000, 0, TSDB_TWA_STEP, 5000, &b, &n);
    CHECK(n == 1 && b[0].covered_ms == 2000 && near(b[0].area, 30000));
    free(b);
    /* buckets of 4 s: the 30 carried across the edges at 4000 and 8000 */
    query1(db, s, 0, 20000, 4000, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 3);
    if (n == 3) {
        CHECK(b[0].start == 0 && b[0].covered_ms == 3000 && near(b[0].area, 10000 + 20000 + 30000) &&
              b[0].count == 3);
        CHECK(b[1].start == 4000 && b[1].covered_ms == 4000 && near(b[1].twa, 30) && b[1].count == 0);
        CHECK(b[2].start == 8000 && b[2].covered_ms == 2000 && near(b[2].twa, 30) && b[2].count == 1);
    }
    free(b);
    /* LINEAR cut at 4000 and 8000: 30 -> 40 over [3000, 10000) */
    query1(db, s, 0, 20000, 4000, TSDB_TWA_LINEAR, 0, &b, &n);
    CHECK(n == 3);
    if (n == 3) {
        double v4 = 30 + 10.0 / 7, v8 = 30 + 50.0 / 7;
        CHECK(near(b[1].area, (v4 + v8) / 2 * 4000) && b[1].covered_ms == 4000);
        CHECK(near(b[2].area, (v8 + 40) / 2 * 2000));
    }
    free(b);
    /* a range starting between readings takes the value in force from before it */
    query1(db, s, 5000, 6000, 0, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 1 && b[0].start == 5000 && b[0].covered_ms == 1000 && near(b[0].twa, 30) && b[0].count == 0);
    free(b);
    /* but not past the staleness limit */
    query1(db, s, 5000, 6000, 0, TSDB_TWA_STEP, 5000, &b, &n);
    CHECK(n == 0);
    free(b);
    /* the last reading holds nothing: after it, no buckets */
    query1(db, s, 10000, 20000, 0, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 1 && b[0].covered_ms == 0 && b[0].count == 1 && isnan(b[0].twa));
    free(b);
    query1(db, s, 10001, 20000, 0, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 0);
    free(b);
    /* a null right at the range start: nothing until 3000 */
    query1(db, s, 2000, 3500, 0, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 1 && b[0].covered_ms == 500 && near(b[0].twa, 30));
    free(b);
    /* bounds: the first bound clips the range; desc and limit */
    int64_t bounds[] = {500, 2500, 9000};
    tsdb_twa_opts o = {.order = TSDB_DESC, .limit = 2};
    OK(tsdb_query_twa_bounds(db, s, 0, 20000, bounds, 3, &o, &b, &n));
    CHECK(n == 2 && b[0].start == 9000 && b[1].start == 2500);
    if (n == 2) {
        CHECK(b[0].covered_ms == 1000 && near(b[0].twa, 30));
        CHECK(b[1].covered_ms == 6000 && near(b[1].twa, 30));
    }
    free(b);
    /* calendar days in UTC */
    OK(tsdb_query_twa_group(db, s, 0, 20000, &(tsdb_group){TSDB_DAY, 1, NULL}, NULL, &b, &n));
    CHECK(n == 1 && b[0].start == 0 && b[0].covered_ms == 9000);
    free(b);
    CHECK(tsdb_query_twa_group(db, s, 0, 20000, &(tsdb_group){TSDB_DAY, 1, "Europe/Oslo"}, NULL, &b, &n) ==
          TSDB_ERR_INVALID);
    /* invalid: state series, a method that does not exist, a negative limit */
    uint32_t st;
    OK(tsdb_series_create(db, "st", TSDB_STATE, NULL, &st));
    CHECK(tsdb_query_twa(db, st, 0, 1, 0, NULL, &b, &n) == TSDB_ERR_INVALID);
    CHECK(tsdb_query_twa(db, s, 0, 1, 0, &(tsdb_twa_opts){.method = 7}, &b, &n) == TSDB_ERR_INVALID);
    CHECK(tsdb_query_twa(db, s, 0, 1, 0, &(tsdb_twa_opts){.max_gap_ms = -1}, &b, &n) == TSDB_ERR_INVALID);
    CHECK(tsdb_query_twa(db, s, 0, 1, -5, NULL, &b, &n) == TSDB_ERR_INVALID);

    /* bool: the fraction of time true */
    uint32_t on;
    OK(tsdb_series_create(db, "on", TSDB_BOOL, NULL, &on));
    int64_t bt[] = {0, 3000, 4000};
    uint8_t bv[] = {1, 0, 1};
    OK(tsdb_insert_series_values(db, on, bt, &(tsdb_values){TSDB_BOOL, bv, NULL}, 3));
    query1(db, on, 0, 4000, 0, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 1 && near(b[0].twa, 0.75) && b[0].count == 2);
    free(b);
    /* NaN held for a while makes the result NaN; held for nothing it does not matter */
    uint32_t f;
    OK(tsdb_series_create(db, "f", TSDB_FLOAT32, NULL, &f));
    int64_t ft[] = {0, 1000, 2000};
    float fv[] = {1, NAN, 3};
    OK(tsdb_insert_series_values(db, f, ft, &(tsdb_values){TSDB_FLOAT32, fv, NULL}, 3));
    query1(db, f, 0, 3000, 1000, TSDB_TWA_STEP, 0, &b, &n);
    CHECK(n == 3 && near(b[0].twa, 1) && isnan(b[1].twa) && isnan(b[1].area) && b[2].covered_ms == 0);
    free(b);
    tsdb_close(db);
    rmrf(dir);
}

/* ------------------------------------------------------ random data */

enum { NSER = 4 };
static const int types[NSER] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL};

static long double gen_value(int type, int64_t i, int shape)
{
    switch (type) {
    case TSDB_BOOL: return (long double)(rnd() % 2);
    case TSDB_INT64: return (long double)(rndr(2000001) - 1000000);
    default: {
        if (shape == 2 && rndr(200) == 0) /* NaN and infinities now and then */
            return rndr(3) == 0 ? NAN : rndr(2) ? INFINITY : -INFINITY;
        long double x = 100 * sinl((long double)i / 50) + (long double)rndr(1000) / 10;
        return type == TSDB_FLOAT32 ? (long double)(float)x : (long double)(double)x;
    }
    }
}

/* writes rows [0, n) of s into the db in shuffled batches, with wrong values first for some rows */
static void write_series(tsdb *db, uint32_t id, const ref_series *s)
{
    size_t n = s->n;
    size_t w = s->type == TSDB_FLOAT32 ? 4 : s->type == TSDB_BOOL ? 1 : 8;
    int64_t *ts = malloc(n * sizeof *ts);
    uint8_t *vals = malloc(n * 8);
    uint8_t *valid = calloc((n + 7) / 8 + 1, 1);
    /* batches over consecutive slices of time, in random order; big ones become parts, small ones the memtable */
    size_t nb = 0, cuts[512];
    for (size_t a = 0; a < n && nb < 511;) {
        size_t len = 1 + (size_t)rndr(rndr(4) ? 400 : 20);
        cuts[nb++] = a;
        a += len;
    }
    cuts[nb] = n;
    for (int pass = 0; pass < 2; pass++) {
        for (size_t k = 0; k < nb; k++) {
            size_t bi = (k * 7919 + (size_t)pass * 31) % nb;
            size_t a = cuts[bi], e = bi + 1 < nb ? cuts[bi + 1] : n;
            if (bi + 1 == nb)
                e = n;
            size_t m = 0;
            memset(valid, 0, (n + 7) / 8 + 1);
            for (size_t i = a; i < e; i++) {
                /* pass 0 writes a wrong value for about a third of the rows, pass 1 every row */
                bool wrong = pass == 0;
                if (wrong && rndr(3))
                    continue;
                long double v = wrong ? 12345 : s->v[i];
                bool ok = wrong ? true : s->ok[i];
                ts[m] = s->ts[i];
                if (ok)
                    valid[m >> 3] |= (uint8_t)(1u << (m & 7));
                switch (s->type) {
                case TSDB_FLOAT32: ((float *)vals)[m] = (float)v; break;
                case TSDB_FLOAT64: ((double *)vals)[m] = (double)v; break;
                case TSDB_INT64: ((int64_t *)vals)[m] = (int64_t)v; break;
                default: vals[m] = (uint8_t)(v != 0); break;
                }
                m++;
            }
            (void)w;
            if (m)
                OK(tsdb_insert_series_values(db, id, ts, &(tsdb_values){(tsdb_type)s->type, vals, valid}, m));
        }
    }
    free(ts);
    free(vals);
    free(valid);
}

static void gen_series(ref_series *s, int type, int shape, size_t n)
{
    s->type = type;
    s->n = n;
    s->ts = malloc(n * sizeof *s->ts);
    s->v = malloc(n * sizeof *s->v);
    s->ok = malloc(n * sizeof *s->ok);
    int64_t t = 1700000000000LL + rndr(100000);
    long double held = 0;
    for (size_t i = 0; i < n; i++) {
        switch (shape) {
        case 0: t += 1000; break;                                            /* regular */
        case 1: t += 1 + rndr(3000); break;                                  /* irregular */
        default: t += rndr(20) ? 1 + rndr(5000) : 60000 + rndr(3600000); break; /* on change, with gaps */
        }
        s->ts[i] = t;
        s->ok[i] = rndr(25) != 0;
        if (shape == 2 && i && rndr(4)) /* on change: the same value again now and then */
            s->v[i] = held;
        else
            s->v[i] = gen_value(type, (int64_t)i, shape);
        if (type == TSDB_FLOAT32 || type == TSDB_FLOAT64)
            if (isnan(s->v[i]) && shape != 2)
                s->v[i] = 0;
        held = s->v[i];
    }
}

static int check_all(tsdb *db, const uint32_t *ids, const ref_series *ss, size_t nser, const char *stage)
{
    int bad = 0;
    for (size_t k = 0; k < nser && bad < 5; k++) {
        const ref_series *s = &ss[k];
        int64_t t0 = s->ts[0], t1 = s->ts[s->n - 1];
        for (int q = 0; q < 24 && bad < 5; q++) {
            int64_t span = t1 - t0 + 1;
            int64_t start = t0 - span / 10 + rndr(span / 2 + 1), end = start + 1 + rndr(span);
            if (q == 0) {
                start = t0;
                end = t1 + 1;
            }
            if (q == 1) {
                start = t0 + rndr(span); /* likely between readings */
                end = INT64_MAX;
            }
            static const int64_t ivs[] = {0, 1000, 7000, 60000, 3600000, 86400000};
            static const int64_t gaps[] = {0, 1500, 4000, 120000};
            ref_bk b = {.kind = q % 5 == 4 ? 2 : (q % 3 ? 1 : 0)};
            b.interval = ivs[1 + rndr(5)];
            int64_t bounds[16];
            if (b.kind == 2) {
                b.nb = 1 + (size_t)rndr(15);
                bounds[0] = start - rndr(span / 4 + 1);
                for (size_t i = 1; i < b.nb; i++)
                    bounds[i] = bounds[i - 1] + 1 + rndr(span / 8 + 1);
                b.bounds = bounds;
            }
            bool linear = rndr(2);
            int64_t gap = gaps[rndr(4)];
            tsdb_twa_opts o = {.method = linear ? TSDB_TWA_LINEAR : TSDB_TWA_STEP, .max_gap_ms = gap};
            tsdb_twa_bucket *res = NULL;
            size_t nres = 0;
            int rc;
            if (b.kind == 0)
                rc = tsdb_query_twa(db, ids[k], start, end, 0, &o, &res, &nres);
            else if (b.kind == 1)
                rc = tsdb_query_twa(db, ids[k], start, end, b.interval, &o, &res, &nres);
            else
                rc = tsdb_query_twa_bounds(db, ids[k], start, end, bounds, b.nb, &o, &res, &nres);
            if (rc == TSDB_ERR_TOO_MANY_BUCKETS) {
                free(res);
                continue;
            }
            CHECK(rc == 0);
            rbucket *r;
            size_t nr = reference(s, start, end, &b, linear, gap, &r);
            char what[256];
            snprintf(what, sizeof what, "%s: series %zu type %d q %d kind %d iv %lld %s gap %lld [%lld, %lld)", stage,
                     k, s->type, q, b.kind, (long long)b.interval, linear ? "linear" : "step", (long long)gap,
                     (long long)start, (long long)end);
            int e = compare(what, r, nr, res, nres);
            if (e)
                failures++;
            bad += e;
            free(r);
            free(res);
        }
    }
    return bad;
}

static void test_random(void)
{
    char dir[600];
    tsdb *db = open_db("random", 64, dir);
    enum { NS = NSER * 3 };
    ref_series ss[NS];
    uint32_t ids[NS];
    for (int k = 0; k < NS; k++) {
        char name[32];
        snprintf(name, sizeof name, "s%d", k);
        OK(tsdb_series_create(db, name, (tsdb_type)types[k % NSER], NULL, &ids[k]));
        gen_series(&ss[k], types[k % NSER], k / NSER, 3000 + (size_t)rndr(3000));
        write_series(db, ids[k], &ss[k]);
    }
    check_all(db, ids, ss, NS, "parts+memtable");
    OK(tsdb_flush(db));
    check_all(db, ids, ss, NS, "parts");
    /* a follower sees the same */
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.refresh_ms = 20;
    tsdb *f;
    OK(tsdb_open(dir, &fo, &f));
    check_all(f, ids, ss, NS, "follower");
    tsdb_close(f);
    OK(tsdb_optimize(db));
    check_all(db, ids, ss, NS, "merged");
    /* corrections after the merge: new values for some rows, in the memtable over merged parts */
    for (int k = 0; k < NS; k++) {
        ref_series *s = &ss[k];
        for (int c = 0; c < 20; c++) {
            size_t i = (size_t)rndr((int64_t)s->n);
            s->v[i] = gen_value(s->type, (int64_t)i, 0);
            s->ok[i] = rndr(5) != 0;
            double dv = (double)s->v[i];
            float fv = (float)s->v[i];
            int64_t iv = (int64_t)s->v[i];
            uint8_t bv = s->v[i] != 0, valid = s->ok[i];
            const void *p = s->type == TSDB_FLOAT32 ? (const void *)&fv
                            : s->type == TSDB_FLOAT64 ? (const void *)&dv
                            : s->type == TSDB_INT64   ? (const void *)&iv
                                                      : (const void *)&bv;
            OK(tsdb_insert_series_values(db, ids[k], &s->ts[i], &(tsdb_values){(tsdb_type)s->type, p, &valid}, 1));
        }
    }
    check_all(db, ids, ss, NS, "corrected");
    OK(tsdb_flush(db));
    check_all(db, ids, ss, NS, "corrected, flushed");
    OK(tsdb_optimize(db));
    check_all(db, ids, ss, NS, "corrected, merged");
    tsdb_close(db);
    for (int k = 0; k < NS; k++) {
        free(ss[k].ts);
        free(ss[k].v);
        free(ss[k].ok);
    }
    rmrf(dir);
}

/* Whole granules inside a bucket come from statistics; only the edges decode. */
static void test_stats_only(void)
{
    char dir[600];
    tsdb *db = open_db("stats", 64, dir);
    uint32_t s;
    OK(tsdb_series_create(db, "x", TSDB_FLOAT32, NULL, &s));
    enum { N = 64 * 100 };
    int64_t *ts = malloc(N * sizeof *ts);
    float *v = malloc(N * sizeof *v);
    for (int i = 0; i < N; i++) {
        ts[i] = 1000LL * i;
        v[i] = (float)(i % 17);
    }
    OK(tsdb_insert_series(db, s, ts, v, N));
    OK(tsdb_flush(db));
    OK(tsdb_optimize(db));
    tsdb_twa_bucket *b;
    size_t n;
    tsdb_query_stats st;
    for (int linear = 0; linear < 2; linear++) {
        tsdb_twa_opts o = {.method = linear};
        OK(tsdb_query_twa(db, s, 0, 1000LL * N, 0, &o, &b, &n));
        tsdb_last_query_stats(&st);
        CHECK(st.rows_decoded == 0 && st.granules_from_stats == 100 && st.rows_from_stats == N);
        long double area = 0;
        for (int i = 0; i + 1 < N; i++)
            area += linear ? ((long double)v[i] + v[i + 1]) / 2 * 1000 : (long double)v[i] * 1000;
        CHECK(n == 1 && b[0].covered_ms == 1000LL * (N - 1) && b[0].count == N &&
              fabsl(b[0].area - area) < 1e-9L * area);
        free(b);
        /* a staleness limit above the sampling interval keeps the fast path; below it decodes */
        o.max_gap_ms = 1000;
        OK(tsdb_query_twa(db, s, 0, 1000LL * N, 0, &o, &b, &n));
        tsdb_last_query_stats(&st);
        CHECK(st.rows_decoded == 0 && n == 1 && b[0].covered_ms == 1000LL * (N - 1));
        free(b);
        o.max_gap_ms = 999;
        OK(tsdb_query_twa(db, s, 0, 1000LL * N, 0, &o, &b, &n));
        tsdb_last_query_stats(&st);
        CHECK(st.rows_decoded == N && n == 1 && b[0].covered_ms == 0 && b[0].count == N);
        free(b);
    }
    /* buckets of 640 s hold 10 granules each; a range cut inside granules decodes only those */
    tsdb_twa_opts o = {0};
    OK(tsdb_query_twa(db, s, 32000, 1000LL * N - 32000, 640000, &o, &b, &n));
    tsdb_last_query_stats(&st);
    CHECK(n == 10 && st.granules_from_stats == 98 && st.rows_decoded == 128);
    free(b);
    tsdb_close(db);
    free(ts);
    free(v);
    rmrf(dir);
}

int main(int argc, char **argv)
{
    snprintf(base, sizeof base, "%s/tsdb_twa_%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rmrf(base);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "mkdir -p '%s'", base);
    if (system(cmd) != 0)
        return 1;
    test_rules();
    test_stats_only();
    test_random();
    rmrf(base);
    if (failures) {
        fprintf(stderr, "test_twa: %d failures\n", failures);
        return 1;
    }
    printf("test_twa: ok\n");
    return 0;
}
