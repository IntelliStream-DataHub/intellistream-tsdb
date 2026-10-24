/*
 * The spread in aggregates: range, population and sample variance and
 * standard deviation (tsdb_query_spread), for float32, float64, int64 and bool.
 *
 * Hand-computed cases first (exact results, the int64 cases that a double
 * could not hold, NaN rules). Then hard data against a reference that runs
 * Welford's method in long double over the rows of each bucket: a large offset
 * with a small spread (101325 Pa +- 5; a raw sum of squares loses ~7 digits
 * there and fails the tolerance), a tiny spread on 1e9, int64 near 4e18 and
 * at the extremes (the sums of squares need more than 128 bits), NaN and
 * infinities, nulls, single values, constant data. The rows are spread over
 * parts with replaced values and the memtable, checked again flushed, from a
 * follower and merged; int64 and bool must give the same bits at every stage
 * (their variances come from exact integer sums), and every CPU level and
 * kernel must give the same bits as the baseline (query_threads 1). One test
 * checks the granule-statistics fast path against the decoding fold.
 */
#define _GNU_SOURCE
#include "internal.h"
#include "tsdb.h"

#include <float.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
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

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}
static int64_t rndr(int64_t n) { return (int64_t)(rnd() % (uint64_t)n); }
static double rndu(void) { return (double)(rnd() >> 11) * 0x1p-53; }

static char base[512];

static void rmrf(const char *d)
{
    char cmd[1100];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", d);
    if (system(cmd) != 0)
        fprintf(stderr, "rm failed\n");
}

static tsdb *open_db(const char *name, char *dir)
{
    snprintf(dir, 600, "%s/%s", base, name);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 64;
    o.merge_threads = 0;
    o.query_threads = 1; /* one order of combination: results comparable bit for bit across CPU levels */
    o.flush_interval_ms = 600000;
    o.buffer_rows = 1 << 22;
    o.wal_max_batch_rows = 64;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

static bool is_canon_nan(double x)
{
    uint64_t b;
    memcpy(&b, &x, 8);
    return b == 0x7ff8000000000000ull;
}

/* equal, or both NaN (the canonical one in the result) */
static bool same_bits(double result, double x) { return result == x || (x != x && is_canon_nan(result)); }

/* ------------------------------------------------------------- hand cases */

static void insert(tsdb *db, uint32_t s, int type, const double *v, const bool *ok, size_t n, int64_t t0)
{
    int64_t ts[64];
    uint8_t vals[64 * 8], valid[8] = {0};
    for (size_t i = 0; i < n; i++) {
        ts[i] = t0 + (int64_t)i;
        if (!ok || ok[i])
            valid[i >> 3] |= (uint8_t)(1u << (i & 7));
        switch (type) {
        case TSDB_FLOAT32: ((float *)vals)[i] = (float)v[i]; break;
        case TSDB_FLOAT64: ((double *)vals)[i] = v[i]; break;
        case TSDB_INT64: ((int64_t *)vals)[i] = (int64_t)v[i]; break;
        case TSDB_BOOL: vals[i] = v[i] != 0; break;
        default: ((uint32_t *)vals)[i] = (uint32_t)v[i]; break;
        }
    }
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){(tsdb_type)type, vals, valid}, n));
}

/* tsdb_query_spread; and tsdb_query_agg, which skips the spread, must give the same buckets bit for bit */
static int spread_agg(tsdb *db, uint32_t s, int64_t start, int64_t end, int64_t interval, tsdb_spread_bucket **out,
                      size_t *nout)
{
    int rc = tsdb_query_spread(db, s, start, end, interval, NULL, out, nout);
    tsdb_bucket *p;
    size_t np;
    OK(tsdb_query_agg(db, s, start, end, interval, &p, &np));
    bool same = rc == 0 && np == *nout;
    for (size_t i = 0; same && i < np; i++)
        same = memcmp(&p[i], &(*out)[i].b, sizeof *p) == 0;
    if (!same)
        fprintf(stderr, "series %u interval %lld: tsdb_query_agg differs from tsdb_query_spread\n", s,
                (long long)interval);
    failures += !same;
    free(p);
    return rc;
}

static tsdb_spread_bucket one(tsdb *db, uint32_t s)
{
    tsdb_spread_bucket *b;
    size_t n;
    OK(spread_agg(db, s, 0, INT64_MAX, 0, &b, &n));
    tsdb_spread_bucket r;
    memset(&r, 0, sizeof r);
    CHECK(n == 1);
    if (n)
        r = b[0];
    free(b);
    return r;
}

static void test_hand(void)
{
    char dir[600];
    tsdb *db = open_db("hand", dir);
    uint32_t s[12];
    for (int pass = 0; pass < 2; pass++) { /* memtable, then parts */
        char name[32];
        int k = 0;
#define SERIES(type)                                                    \
    do {                                                                \
        snprintf(name, sizeof name, "h%d", k);                          \
        if (!pass)                                                      \
            OK(tsdb_series_create(db, name, (tsdb_type)(type), NULL, &s[k])); \
    } while (0)
        /* 1 2 3 4 */
        SERIES(TSDB_FLOAT64);
        if (!pass)
            insert(db, s[k], TSDB_FLOAT64, (double[]){1, 2, 3, 4}, NULL, 4, 0);
        tsdb_spread_bucket b = one(db, s[k++]);
        CHECK(b.range == 3 && b.var_pop == 1.25 && b.var_samp == 5.0 / 3 && b.stddev_pop == sqrt(1.25));
        CHECK(b.stddev_samp == sqrt(5.0 / 3));
        /* the same in float32 and int64 */
        SERIES(TSDB_FLOAT32);
        if (!pass)
            insert(db, s[k], TSDB_FLOAT32, (double[]){4, 2, 3, 1}, NULL, 4, 0);
        b = one(db, s[k++]);
        CHECK(b.range == 3 && b.var_pop == 1.25 && b.var_samp == 5.0 / 3);
        SERIES(TSDB_INT64);
        if (!pass)
            insert(db, s[k], TSDB_INT64, (double[]){3, 1, 4, 2}, NULL, 4, 0);
        b = one(db, s[k++]);
        CHECK(b.range == 3 && b.var_pop == 1.25 && b.var_samp == 5.0 / 3);
        /* int64 beyond 2^53: 4e18 + {0, 1, 2}, variance 2/3 exactly (a double holds none of the values) */
        SERIES(TSDB_INT64);
        if (!pass) {
            int64_t ts[3] = {0, 1, 2}, iv[3] = {4000000000000000002, 4000000000000000000, 4000000000000000001};
            OK(tsdb_insert_series_values(db, s[k], ts, &(tsdb_values){TSDB_INT64, iv, NULL}, 3));
        }
        b = one(db, s[k++]);
        CHECK(b.range == 2 && b.var_pop == 2.0 / 3 && b.var_samp == 1 && b.stddev_samp == 1);
        /* the extremes (INT64_MIN is null): range 2^64 - 2, var_pop (2^63 - 1)^2 */
        SERIES(TSDB_INT64);
        if (!pass) {
            int64_t ts[2] = {0, 1}, iv[2] = {INT64_MIN + 1, INT64_MAX};
            OK(tsdb_insert_series_values(db, s[k], ts, &(tsdb_values){TSDB_INT64, iv, NULL}, 2));
        }
        b = one(db, s[k++]);
        long double r = 18446744073709551614.0L;
        CHECK(b.range == (double)r && b.var_pop == (double)(r * r / 4) && b.var_samp == (double)(r * r / 2));
        /* bool 0 1 1 1 and a null */
        SERIES(TSDB_BOOL);
        if (!pass)
            insert(db, s[k], TSDB_BOOL, (double[]){0, 1, 1, 1, 0}, (bool[]){1, 1, 1, 1, 0}, 5, 0);
        b = one(db, s[k++]);
        CHECK(b.b.count == 4 && b.range == 1 && b.var_pop == 0.1875 && b.var_samp == 0.25);
        /* a single value: sample variance NaN */
        SERIES(TSDB_FLOAT64);
        if (!pass)
            insert(db, s[k], TSDB_FLOAT64, (double[]){101325.37, 0}, (bool[]){1, 0}, 2, 0);
        b = one(db, s[k++]);
        CHECK(b.range == 0 && b.var_pop == 0 && b.stddev_pop == 0 && is_canon_nan(b.var_samp));
        CHECK(is_canon_nan(b.stddev_samp));
        /* only nulls: everything NaN */
        SERIES(TSDB_INT64);
        if (!pass)
            insert(db, s[k], TSDB_INT64, (double[]){1, 2}, (bool[]){0, 0}, 2, 0);
        b = one(db, s[k++]);
        CHECK(b.b.count == 0 && is_canon_nan(b.range) && is_canon_nan(b.var_pop) && is_canon_nan(b.stddev_samp));
        /* NaN among the values: variances NaN, the range from min and max */
        SERIES(TSDB_FLOAT64);
        if (!pass)
            insert(db, s[k], TSDB_FLOAT64, (double[]){1, NAN, 5}, NULL, 3, 0);
        b = one(db, s[k++]);
        CHECK(b.range == 4 && is_canon_nan(b.var_pop) && is_canon_nan(b.stddev_pop) && is_canon_nan(b.var_samp));
        /* an infinity: range inf, variances NaN */
        SERIES(TSDB_FLOAT32);
        if (!pass)
            insert(db, s[k], TSDB_FLOAT32, (double[]){1, INFINITY, 5}, NULL, 3, 0);
        b = one(db, s[k++]);
        CHECK(b.range == INFINITY && is_canon_nan(b.var_pop) && is_canon_nan(b.stddev_samp));
        /* states: not meaningful */
        SERIES(TSDB_STATE);
        if (!pass)
            insert(db, s[k], TSDB_STATE, (double[]){1, 2, 3}, NULL, 3, 0);
        b = one(db, s[k++]);
        CHECK(b.b.count == 3 && is_canon_nan(b.range) && is_canon_nan(b.var_pop) && is_canon_nan(b.stddev_pop));
        /* constant data that no double sum holds exactly: exactly 0 */
        SERIES(TSDB_FLOAT64);
        if (!pass) {
            double v[64];
            for (int i = 0; i < 64; i++)
                v[i] = 101325.37;
            insert(db, s[k], TSDB_FLOAT64, v, NULL, 64, 0);
            insert(db, s[k], TSDB_FLOAT64, v, NULL, 37, 1000);
        }
        b = one(db, s[k++]);
        CHECK(b.b.count == 101 && b.range == 0 && b.var_pop == 0 && b.var_samp == 0 && b.stddev_samp == 0);
#undef SERIES
        if (!pass)
            OK(tsdb_flush(db));
    }
    tsdb_close(db);
    rmrf(dir);
}

/* -------------------------------------------------------------- reference */

enum { NROWS = 6000 };
typedef struct ref_series {
    int type, shape;
    int64_t ts[NROWS];
    long double v[NROWS]; /* as stored: float32 values rounded to float */
    bool ok[NROWS];
    bool gone[NROWS]; /* deleted, replaced away or expired (test_deletes) */
} ref_series;

static long double gen_value(int type, int shape, size_t i)
{
    switch (type) {
    case TSDB_FLOAT32:
    case TSDB_FLOAT64: {
        long double v;
        switch (shape) {
        case 0: v = 101325 + (rndu() * 10 - 5); break;             /* pressure in Pa */
        case 1: v = 1e9 + (double)rndr(1000) * 1e-6; break;       /* tiny spread on a large offset */
        case 2: v = rndr(50) ? rndu() * 100 - 50 : rndr(2) ? NAN : rndr(2) ? INFINITY : -INFINITY; break;
        case 3: v = 21.3; break;                                  /* constant */
        default: v = (double)(i % 1000) * 0.001 - 0.5 + rndu(); break;
        }
        return type == TSDB_FLOAT32 ? (long double)(float)v : (long double)(double)v;
    }
    case TSDB_INT64:
        switch (shape) {
        case 0: return 4000000000000000000LL + rndr(2001) - 1000; /* squares of ~1.6e37: past 128 bits in sum */
        case 1: return rndr(2) ? INT64_MAX - rndr(5) : INT64_MIN + 1 + rndr(5); /* INT64_MIN is null */
        case 2: return rndr(1000001) - 500000;
        case 3: return -7;
        default: return (int64_t)(i % 97);
        }
    default: return shape == 3 ? 1 : rndr(4) != 0;
    }
}

static void gen_series(ref_series *s, int type, int shape)
{
    s->type = type;
    s->shape = shape;
    for (size_t i = 0; i < NROWS; i++) {
        s->ts[i] = (int64_t)i * 10;
        s->v[i] = gen_value(type, shape, i);
        /* shape 4: sparse, mostly nulls (single values in a bucket); else a few */
        s->ok[i] = shape == 4 ? rndr(9) == 0 : rndr(40) != 0;
        s->gone[i] = false;
    }
}

static void put_rows1(tsdb *db, uint32_t id, const ref_series *s, size_t a, size_t e, bool wrong);
/* batches of at most 64 rows go to the memtable (wal_max_batch_rows), bigger ones straight into parts */
static void put_rows(tsdb *db, uint32_t id, const ref_series *s, size_t a, size_t e, bool wrong, bool small)
{
    if (!small) {
        put_rows1(db, id, s, a, e, wrong);
        return;
    }
    for (size_t k = a; k < e; k += 50)
        put_rows1(db, id, s, k, k + 50 < e ? k + 50 : e, wrong);
}

static void put_rows1(tsdb *db, uint32_t id, const ref_series *s, size_t a, size_t e, bool wrong)
{
    int64_t ts[NROWS];
    static uint8_t vals[NROWS * 8], valid[NROWS / 8 + 1];
    memset(valid, 0, sizeof valid);
    size_t m = 0;
    for (size_t i = a; i < e; i++) {
        if (wrong && rndr(3))
            continue;
        long double v = wrong ? 12345 : s->v[i];
        ts[m] = s->ts[i];
        if (wrong || s->ok[i])
            valid[m >> 3] |= (uint8_t)(1u << (m & 7));
        switch (s->type) {
        case TSDB_FLOAT32: ((float *)vals)[m] = (float)v; break;
        case TSDB_FLOAT64: ((double *)vals)[m] = (double)v; break;
        case TSDB_INT64: ((int64_t *)vals)[m] = (int64_t)v; break;
        default: vals[m] = v != 0; break;
        }
        m++;
    }
    if (m)
        OK(tsdb_insert_series_values(db, id, ts, &(tsdb_values){(tsdb_type)s->type, vals, valid}, m));
}

typedef struct ref_bucket {
    int64_t start;
    uint64_t count;
    bool special; /* a NaN or an infinity among the values */
    long double mean, m2, mn, mx, maxabs;
} ref_bucket;

/* The corrected two-pass method in long double over the known values of each bucket of [0, end): with
 * values close to the mean x - m is exact, and the correction makes up for the rounding of m (at 4e18 a
 * long double mean is off by up to 0.25, which Welford's method would carry into every term). */
static size_t reference_n(const ref_series *s, size_t nrows, int64_t interval, ref_bucket *out)
{
    size_t n = 0, i0 = 0;
    for (size_t i = 0; i <= nrows; i++) {
        int64_t st = i < nrows && interval ? s->ts[i] / interval * interval : 0;
        if (i < nrows && (i == 0 || out[n - 1].start == st))
            goto open;
        if (n) { /* close bucket n - 1: rows [i0, i) */
            ref_bucket *b = &out[n - 1];
            long double sum = 0, s1 = 0, s2 = 0;
            uint64_t c = 0;
            for (size_t k = i0; k < i; k++)
                if (s->ok[k] && isfinite(s->v[k])) {
                    sum += s->v[k];
                    c++;
                }
            long double m = c ? sum / (long double)c : 0;
            for (size_t k = i0; k < i; k++)
                if (s->ok[k] && isfinite(s->v[k])) {
                    long double d = s->v[k] - m;
                    s1 += d;
                    s2 += d * d;
                    b->maxabs = fabsl(s->v[k]) > b->maxabs ? fabsl(s->v[k]) : b->maxabs;
                }
            b->m2 = c ? s2 - s1 * s1 / (long double)c : 0;
        }
        if (i == nrows)
            break;
        i0 = i;
    open:
        if (!n || out[n - 1].start != st) {
            memset(&out[n], 0, sizeof out[n]);
            out[n].start = st;
            out[n].mn = INFINITY;
            out[n].mx = -INFINITY;
            n++;
        }
        ref_bucket *b = &out[n - 1];
        if (!s->ok[i])
            continue;
        long double x = s->v[i];
        b->count++;
        b->special |= !isfinite(x);
        if (x == x) {
            b->mn = x < b->mn ? x : b->mn;
            b->mx = x > b->mx ? x : b->mx;
        }
    }
    return n;
}

/* the reference over the rows in [a, e) that are left: a deleted row is not a null, it opens no bucket */
static size_t reference_in(const ref_series *s, int64_t interval, int64_t a, int64_t e, ref_bucket *out)
{
    static ref_series live;
    size_t n = 0;
    live.type = s->type;
    live.shape = s->shape;
    for (size_t i = 0; i < NROWS; i++)
        if (!s->gone[i] && s->ts[i] >= a && s->ts[i] < e) {
            live.ts[n] = s->ts[i];
            live.v[n] = s->v[i];
            live.ok[n] = s->ok[i];
            n++;
        }
    return reference_n(&live, n, interval, out);
}

static size_t reference(const ref_series *s, int64_t interval, ref_bucket *out)
{
    return reference_in(s, interval, INT64_MIN, INT64_MAX, out);
}

/* tolerance of a float standard deviation: relative, and a few ulps of the largest value (the means
 * of the runs combined are sums over counts, rounded at the scale of the values) */
static bool sd_near(double got, long double ref, long double maxabs)
{
    long double tol = 1e-10L * ref + 64 * DBL_EPSILON * maxabs;
    return fabsl((long double)got - ref) <= tol;
}

static int compare(const char *what, const ref_series *s, const ref_bucket *r, size_t nr, const tsdb_spread_bucket *b,
                   size_t nb)
{
    int bad = 0;
    if (nr != nb) {
        fprintf(stderr, "%s: %zu buckets, reference %zu\n", what, nb, nr);
        return 1;
    }
    bool isint = s->type == TSDB_INT64 || s->type == TSDB_BOOL;
    for (size_t k = 0; k < nb && bad < 5; k++) {
        const ref_bucket *x = &r[k];
        const tsdb_spread_bucket *y = &b[k];
        bool ok = y->b.start == x->start && y->b.count == x->count;
        long double n = (long double)x->count;
        if (ok && x->count == 0)
            ok = is_canon_nan(y->range) && is_canon_nan(y->var_pop) && is_canon_nan(y->stddev_samp);
        else if (ok) {
            double range = x->mn <= x->mx ? (double)x->mx - (double)x->mn : NAN;
            if (isint)
                range = (double)(x->mx - x->mn);
            ok = range == y->range || (range != range && is_canon_nan(y->range));
            ok = ok && same_bits(y->stddev_pop, sqrt(y->var_pop));
            ok = ok && (x->count > 1 ? same_bits(y->stddev_samp, sqrt(y->var_samp)) : is_canon_nan(y->stddev_samp));
            if (x->special)
                ok = ok && is_canon_nan(y->var_pop) && is_canon_nan(y->var_samp);
            else if (isint) {
                /* exact up to the final rounding; the reference is long double */
                long double vp = x->m2 / n;
                ok = ok && fabsl(y->var_pop - vp) <= 1e-14L * vp;
                if (x->count > 1)
                    ok = ok && fabsl(y->var_samp - x->m2 / (n - 1)) <= 1e-14L * vp * 2;
            } else {
                ok = ok && sd_near(y->stddev_pop, sqrtl(x->m2 / n), x->maxabs);
                if (x->count > 1)
                    ok = ok && sd_near(y->stddev_samp, sqrtl(x->m2 / (n - 1)), x->maxabs);
            }
        }
        if (!ok) {
            fprintf(stderr,
                    "%s: type %d shape %d bucket %lld: count %llu/%llu range %.17g/%.17Lg sd_pop %.17g/%.17Lg "
                    "sd_samp %.17g/%.17Lg%s\n",
                    what, s->type, s->shape, (long long)y->b.start, (unsigned long long)y->b.count,
                    (unsigned long long)x->count, y->range, x->mx - x->mn, y->stddev_pop, sqrtl(x->m2 / n),
                    y->stddev_samp, x->count > 1 ? sqrtl(x->m2 / (n - 1)) : 0.0L, x->special ? " (special)" : "");
            bad++;
        }
    }
    return bad;
}

/* ---------------------------------------------------------- random data */

static const int types[] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL};
enum { NT = 4, NSHAPE = 5, NS = NT * NSHAPE };
static const int64_t intervals[] = {0, 640, 6400, 777, 30, 10};
enum { NI = sizeof intervals / sizeof intervals[0] };

/* every query of every series; results appended to *all (for bit comparisons), their types to btype */
static int *btype;
static void check_all(tsdb *db, const uint32_t *ids, ref_series *const *ss, const char *stage, tsdb_spread_bucket **all,
                      size_t *nall)
{
    static ref_bucket r[NROWS];
    size_t cap = 0;
    *all = NULL;
    *nall = 0;
    for (int k = 0; k < NS; k++)
        for (int q = 0; q < NI; q++) {
            tsdb_spread_bucket *b;
            size_t nb;
            OK(spread_agg(db, ids[k], 0, INT64_MAX, intervals[q], &b, &nb));
            size_t nr = reference(ss[k], intervals[q], r);
            char what[128];
            snprintf(what, sizeof what, "%s, interval %lld", stage, (long long)intervals[q]);
            failures += compare(what, ss[k], r, nr, b, nb) != 0;
            if (*nall + nb > cap) {
                cap = (*nall + nb) * 2;
                *all = realloc(*all, cap * sizeof **all);
                btype = realloc(btype, cap * sizeof *btype);
            }
            for (size_t i = 0; i < nb; i++)
                btype[*nall + i] = ss[k]->type;
            memcpy(*all + *nall, b, nb * sizeof *b);
            *nall += nb;
            free(b);
        }
}

/* the same buckets, in reverse when desc, and only the first limit of them (0: all) */
static void same_order(const char *what, const char *form, const tsdb_spread_bucket *asc, size_t na,
                       const tsdb_spread_bucket *b, size_t nb, bool desc, size_t limit)
{
    size_t want = limit && limit < na ? limit : na;
    bool ok = nb == want;
    for (size_t i = 0; ok && i < nb; i++)
        ok = memcmp(&b[i], &asc[desc ? na - 1 - i : i], sizeof *b) == 0;
    if (!ok) {
        fprintf(stderr, "%s, %s (%s, limit %zu): %zu buckets, not the %zu expected or not the same\n", what, form,
                desc ? "desc" : "asc", limit, nb, want);
        failures++;
    }
}

/* The interval, group and bounds forms over a part of the range, ascending, descending and limited, against
 * the reference. bounds[0] lies after the first rows: those belong to no bucket even though start is 0. */
static void check_forms(tsdb *db, const uint32_t *ids, ref_series *const *ss, const char *stage)
{
    static ref_bucket r[NROWS];
    static int64_t bounds[NROWS];
    static const int64_t ivs[] = {640, 777, 7000};
    for (int k = 0; k < NS; k++)
        for (size_t q = 0; q < sizeof ivs / sizeof ivs[0]; q++) {
            int64_t iv = ivs[q], a = iv * (1 + rndr(4)), m = 1 + rndr(60000 / iv);
            int64_t e = a + m * iv - rndr(2) * rndr(iv); /* the last bucket whole or cut short */
            for (int64_t j = 0; j < m; j++)
                bounds[j] = a + j * iv;
            size_t nr = reference_in(ss[k], iv, a, e, r);
            tsdb_spread_bucket *asc, *b;
            size_t na, nb;
            char what[160];
            snprintf(what, sizeof what, "%s, series %d, interval %lld from %lld", stage, k, (long long)iv,
                     (long long)a);
            OK(tsdb_query_spread(db, ids[k], a, e, iv, NULL, &asc, &na));
            failures += compare(what, ss[k], r, nr, asc, na) != 0;
            tsdb_group g = {.unit = TSDB_SECOND, .count = (uint32_t)(iv / 1000)};
            if (iv % 1000 == 0) {
                OK(tsdb_query_spread_group(db, ids[k], a, e, &g, NULL, &b, &nb));
                same_order(what, "group", asc, na, b, nb, false, 0);
                free(b);
            }
            OK(tsdb_query_spread_bounds(db, ids[k], 0, e, bounds, (size_t)m, NULL, &b, &nb));
            char wb[200];
            snprintf(wb, sizeof wb, "%s, bounds", what);
            failures += compare(wb, ss[k], r, nr, b, nb) != 0;
            free(b);
            for (int d = 0; d < 2; d++) {
                size_t limit = d && !rndr(3) ? 0 : 1 + (size_t)rndr((int64_t)na + 1);
                tsdb_query_opts o = {.order = d ? TSDB_DESC : TSDB_ASC, .limit = limit};
                OK(tsdb_query_spread(db, ids[k], a, e, iv, &o, &b, &nb));
                same_order(what, "interval", asc, na, b, nb, d, limit);
                free(b);
                OK(tsdb_query_spread_bounds(db, ids[k], 0, e, bounds, (size_t)m, &o, &b, &nb));
                same_order(what, "bounds", asc, na, b, nb, d, limit);
                free(b);
                if (iv % 1000 == 0) {
                    OK(tsdb_query_spread_group(db, ids[k], a, e, &g, &o, &b, &nb));
                    same_order(what, "group", asc, na, b, nb, d, limit);
                    free(b);
                }
            }
            free(asc);
        }
}

/* the spread fields of int64 and bool series: bit for bit the same as at the first stage */
static void same_ints(const tsdb_spread_bucket *a, size_t na, const tsdb_spread_bucket *b, size_t nb, const char *stage)
{
    if (na != nb) {
        fprintf(stderr, "%s: %zu buckets, first stage %zu\n", stage, nb, na);
        failures++;
        return;
    }
    int diff = 0;
    for (size_t o = 0; o < na; o++)
        if (btype[o] == TSDB_INT64 || btype[o] == TSDB_BOOL)
            diff += memcmp(&a[o].range, &b[o].range, 5 * sizeof(double)) != 0 || a[o].b.count != b[o].b.count;
    if (diff) {
        fprintf(stderr, "%s: %d int64/bool buckets differ in bits from the first stage\n", stage, diff);
        failures++;
    }
}

/* every CPU level and kernel (TSDB_CPU, TSDB_SIMD): the same bits as the first one */
static void check_levels(tsdb *db, const uint32_t *ids, ref_series *const *ss, const char *stage)
{
    tsdb_spread_bucket *b;
    size_t nb;
    static const char *const kernels[] = {"scalar", "avx2", "avx512"};
    tsdb_spread_bucket *lv0 = NULL;
    size_t nlv0 = 0;
    int configs = 0;
    for (int level = CPU_BASE; level <= cpu_level_max(); level++)
        for (int kk = 0; kk <= level && kk < 3; kk++) {
            setenv("TSDB_SIMD", kernels[kk], 1);
            if (cpu_set_level(level) != 0) {
                simd_select(level);
                if (cpu_level_max() != level && level != CPU_BASE)
                    break;
            }
            if (strcmp(simd_name(), kernels[kk]) != 0)
                continue;
            char label[160];
            snprintf(label, sizeof label, "%s, level %s, kernel %s", stage, cpu_level_name(level), simd_name());
            check_all(db, ids, ss, label, &b, &nb);
            if (!configs++) {
                lv0 = b;
                nlv0 = nb;
                continue;
            }
            bool same = nb == nlv0 && memcmp(b, lv0, nb * sizeof *b) == 0;
            printf("%s: %s\n", label, same ? "identical" : "DIFFERENT");
            failures += !same;
            free(b);
        }
    unsetenv("TSDB_SIMD");
    cpu_set_level(cpu_level_max());
    simd_select(cpu_level_max());
    printf("CPU levels and kernels compared: %d\n", configs);
    free(lv0);
}

static void test_random(void)
{
    char dir[600];
    tsdb *db = open_db("random", dir);
    static ref_series store[NS];
    ref_series *ss[NS];
    uint32_t ids[NS];
    for (int k = 0; k < NS; k++) {
        char name[32];
        snprintf(name, sizeof name, "s%d", k);
        ss[k] = &store[k];
        OK(tsdb_series_create(db, name, (tsdb_type)types[k % NT], NULL, &ids[k]));
        gen_series(ss[k], types[k % NT], k / NT);
        /* wrong values for a third of the rows, in parts; then the right ones in slices of random
         * order, some flushed into parts of their own, the rest left in the memtable */
        put_rows(db, ids[k], ss[k], 0, NROWS, true, false);
    }
    OK(tsdb_flush(db));
    for (int k = 0; k < NS; k++) {
        size_t cuts[64], nc = 0;
        for (size_t a = 0; a < NROWS && nc < 63; a += 1 + (size_t)rndr(500))
            cuts[nc++] = a;
        cuts[nc] = NROWS;
        for (size_t j = 0; j < nc; j++) {
            size_t c = (j * 37) % nc;
            put_rows(db, ids[k], ss[k], cuts[c], cuts[c + 1], false, j % 2);
            if (j % 5 == 4 && k % 3 == 0)
                OK(tsdb_flush(db));
        }
    }
    tsdb_spread_bucket *first, *b;
    size_t nfirst, nb;
    check_all(db, ids, ss, "parts+memtable", &first, &nfirst);
    OK(tsdb_flush(db));
    check_all(db, ids, ss, "parts", &b, &nb);
    same_ints(first, nfirst, b, nb, "parts");
    free(b);

    check_levels(db, ids, ss, "parts");

    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.refresh_ms = 20;
    fo.query_threads = 1;
    tsdb *f;
    OK(tsdb_open(dir, &fo, &f));
    check_all(f, ids, ss, "follower", &b, &nb);
    same_ints(first, nfirst, b, nb, "follower");
    free(b);
    tsdb_close(f);
    OK(tsdb_optimize(db));
    check_all(db, ids, ss, "merged", &b, &nb);
    same_ints(first, nfirst, b, nb, "merged");
    free(b);
    free(first);
    tsdb_close(db);
    rmrf(dir);
}

/* ------------------------------------------- deletes, replaces, retention */

/* Writes the rows idx[0..m) of s (sorted): a replace of [a, b) when `replace`, else an insert. */
static void write_rows(tsdb *db, uint32_t id, const ref_series *s, const size_t *idx, size_t m, bool replace,
                       int64_t a, int64_t b)
{
    int64_t *ts = malloc((m + 1) * sizeof *ts);
    uint8_t *vals = malloc(m * 8 + 8), *valid = calloc(m / 8 + 2, 1);
    for (size_t k = 0; k < m; k++) {
        size_t i = idx[k];
        ts[k] = s->ts[i];
        if (s->ok[i])
            valid[k >> 3] |= (uint8_t)(1u << (k & 7));
        switch (s->type) {
        case TSDB_FLOAT32: ((float *)vals)[k] = (float)s->v[i]; break;
        case TSDB_FLOAT64: ((double *)vals)[k] = (double)s->v[i]; break;
        case TSDB_INT64: ((int64_t *)vals)[k] = (int64_t)s->v[i]; break;
        default: vals[k] = s->v[i] != 0; break;
        }
    }
    tsdb_values tv = {(tsdb_type)s->type, vals, valid};
    if (replace)
        OK(tsdb_replace_range(db, id, a, b, ts, &tv, m));
    else if (m)
        OK(tsdb_insert_series_values(db, id, ts, &tv, m));
    free(ts);
    free(vals);
    free(valid);
}

/* One random change to a slice of s's history, in the database and the model alike: a delete, a replace
 * (some rows of the slice come back with new values) or inserts over the old values. The range's ends fall
 * between rows or on them; small writes land in the memtable, big ones in parts. */
static void spread_change(tsdb *db, uint32_t id, ref_series *s)
{
    size_t i = (size_t)rndr(NROWS);
    size_t j = i + 1 + (size_t)rndr(rndr(4) ? 200 : 1500);
    if (j > NROWS)
        j = NROWS;
    int64_t a = s->ts[i] - rndr(10); /* the rows [i, j) are in [a, b): the grid is 10 ms */
    int64_t b = j < NROWS ? s->ts[j] - rndr(10) : s->ts[NROWS - 1] + 1 + rndr(10);
    if (i == 0 && rndr(4) == 0)
        a = INT64_MIN;
    static size_t idx[NROWS];
    size_t m = 0;
    int op = (int)rndr(3);
    for (size_t k = i; k < j; k++) {
        if (op == 0 || rndr(2)) {
            if (op != 2)
                s->gone[k] = true;
            continue;
        }
        s->v[k] = gen_value(s->type, s->shape, k);
        s->ok[k] = rndr(40) != 0;
        s->gone[k] = false;
        idx[m++] = k;
    }
    if (op == 0 && rndr(2))
        OK(tsdb_delete_range(db, id, a, b));
    else
        write_rows(db, id, s, idx, m, op != 2, a, b);
}

static int64_t spread_now;
static int64_t spread_clock(void *arg)
{
    (void)arg;
    return __atomic_load_n(&spread_now, __ATOMIC_SEQ_CST);
}

static tsdb *open_del(const char *dir, bool merges, bool follow)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 64;
    o.merge_threads = merges ? 2 : 0;
    o.max_part_bytes = 40000; /* background merges of a few parts, not the whole history */
    o.merge_idle_s = 1;
    o.query_threads = 1;
    o.flush_interval_ms = 600000;
    o.buffer_rows = 1 << 22;
    o.wal_max_batch_rows = 64;
    o.clock_ms = spread_clock;
    o.follow = follow;
    o.refresh_ms = 20;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

/* The spread must never include a deleted, replaced-away or expired row: granules a tombstone overlaps
 * do not answer from their statistics, merges that drop rows compute m2 and the sums of squares anew, and
 * the retention cutoff applies. Checked against the reference with tombstones in parts, the memtable on
 * top, a follower, background merges, a full merge, then retention; without background merges every CPU
 * level and kernel must give the same bits. */
static void test_deletes(bool merges)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/deletes%d", base, merges);
    rmrf(dir);
    __atomic_store_n(&spread_now, 1000000, __ATOMIC_SEQ_CST);
    tsdb *db = open_del(dir, merges, false);
    static ref_series store[NS];
    ref_series *ss[NS];
    uint32_t ids[NS];
    for (int k = 0; k < NS; k++) {
        char name[32];
        snprintf(name, sizeof name, "d%d", k);
        ss[k] = &store[k];
        OK(tsdb_series_create(db, name, (tsdb_type)types[k % NT], NULL, &ids[k]));
        gen_series(ss[k], types[k % NT], k / NT);
        put_rows(db, ids[k], ss[k], 0, NROWS, false, false);
    }
    OK(tsdb_flush(db));
    tsdb_spread_bucket *b, *b2;
    size_t nb, nb2;
    for (int round = 0; round < 6; round++) {
        for (int k = 0; k < NS; k++)
            for (int c = 0; c < 3; c++)
                spread_change(db, ids[k], ss[k]);
        if (round % 2)
            OK(tsdb_flush(db)); /* else the writes stay in the memtable, newer than the tombstones */
        char st[64];
        snprintf(st, sizeof st, "deletes%d round %d", merges, round);
        check_all(db, ids, ss, st, &b, &nb);
        check_forms(db, ids, ss, st);
        free(b);
    }
    if (!merges)
        check_levels(db, ids, ss, "deletes, memtable");
    OK(tsdb_flush(db));
    if (!merges)
        check_levels(db, ids, ss, "deletes, parts");
    check_all(db, ids, ss, "deletes, flushed", &b, &nb);
    check_forms(db, ids, ss, "deletes, flushed");
    tsdb *fo = open_del(dir, false, true);
    OK(tsdb_refresh(fo));
    check_all(fo, ids, ss, "deletes, follower", &b2, &nb2);
    same_ints(b, nb, b2, nb2, "deletes, follower");
    free(b2);
    OK(tsdb_optimize(db));
    check_all(db, ids, ss, "deletes, optimized", &b2, &nb2);
    same_ints(b, nb, b2, nb2, "deletes, optimized");
    free(b2);
    free(b);
    /* merged: the rows are gone from the files and the statistics exact, so granules answer from them */
    for (int k = 0; k < NS; k++) {
        OK(spread_agg(db, ids[k], 0, INT64_MAX, 0, &b, &nb));
        free(b);
        tsdb_query_stats qs;
        tsdb_last_query_stats(&qs);
        CHECK(qs.granules_from_stats > 0);
    }
    if (!merges)
        check_levels(db, ids, ss, "deletes, optimized");
    OK(tsdb_refresh(fo));
    check_all(fo, ids, ss, "deletes, follower after optimize", &b, &nb);
    check_forms(fo, ids, ss, "deletes, follower after optimize");
    free(b);

    /* retention: every row before a cutoff inside each series is gone, merged away or not */
    for (int k = 0; k < NS; k++) {
        int64_t cut = ss[k]->ts[NROWS / 3 + (size_t)rndr(NROWS / 3)] + rndr(2); /* at a row or just after it */
        OK(tsdb_set_retention(db, ids[k], __atomic_load_n(&spread_now, __ATOMIC_SEQ_CST) - cut));
        for (size_t i = 0; i < NROWS && ss[k]->ts[i] < cut; i++)
            ss[k]->gone[i] = true;
    }
    check_all(db, ids, ss, "retention", &b, &nb);
    check_forms(db, ids, ss, "retention");
    if (!merges)
        check_levels(db, ids, ss, "retention");
    OK(tsdb_optimize(db)); /* merges drop the expired rows */
    check_all(db, ids, ss, "retention, optimized", &b2, &nb2);
    same_ints(b, nb, b2, nb2, "retention, optimized");
    free(b2);
    OK(tsdb_close(fo));
    fo = open_del(dir, false, true);
    check_all(fo, ids, ss, "retention, follower", &b2, &nb2);
    check_forms(fo, ids, ss, "retention, follower");
    same_ints(b, nb, b2, nb2, "retention, follower");
    free(b2);
    free(b);
    OK(tsdb_close(fo));
    OK(tsdb_close(db));
    rmrf(dir);
}

/* The fast path (whole granules from their statistics) against the fold over decoded rows: the same
 * rows in the memtable, then flushed into a part. */
static void test_fast_path(void)
{
    char dir[600];
    tsdb *db = open_db("fast", dir);
    static ref_series s;
    for (int t = 0; t < NT; t++)
        for (int shape = 0; shape < NSHAPE; shape++) {
            char name[32];
            snprintf(name, sizeof name, "f%d_%d", t, shape);
            uint32_t id;
            OK(tsdb_series_create(db, name, (tsdb_type)types[t], NULL, &id));
            gen_series(&s, types[t], shape);
            put_rows(db, id, &s, 0, NROWS, false, true);
            static ref_bucket r[NROWS];
            tsdb_spread_bucket *mb, *pb;
            size_t nm, np;
            tsdb_query_stats st;
            /* 6400 ms: whole granules of 64 rows 10 ms apart */
            OK(spread_agg(db, id, 0, INT64_MAX, 6400, &mb, &nm));
            tsdb_last_query_stats(&st);
            CHECK(st.granules_from_stats == 0 && st.memtable_rows == NROWS);
            if (st.memtable_rows != NROWS)
                fprintf(stderr, "memtable rows %llu\n", (unsigned long long)st.memtable_rows);
            OK(tsdb_flush(db));
            OK(spread_agg(db, id, 0, INT64_MAX, 6400, &pb, &np));
            tsdb_last_query_stats(&st);
            CHECK(st.granules_from_stats == (NROWS + 63) / 64 && st.rows_decoded <= 64);
            if (st.granules_from_stats != (NROWS + 63) / 64 || st.rows_decoded > 64)
                fprintf(stderr, "from stats %llu, decoded %llu\n", (unsigned long long)st.granules_from_stats,
                        (unsigned long long)st.rows_decoded);
            size_t nr = reference(&s, 6400, r);
            failures += compare("memtable", &s, r, nr, mb, nm) != 0;
            failures += compare("granule statistics", &s, r, nr, pb, np) != 0;
            if (types[t] == TSDB_INT64 || types[t] == TSDB_BOOL)
                CHECK(nm == np && memcmp(mb, pb, nm * sizeof *mb) == 0);
            free(mb);
            free(pb);
        }
    tsdb_close(db);
    rmrf(dir);
}

/* The kernels bit for bit, on what reaches them: float32 runs without nulls and NaN, of varied values (the
 * data the database-level tests feed them is mostly constant, NaN-ridden or null-ridden in float32, and FMA
 * in the deviation pass happens not to change 101325 +- 5). Each kernel's agg_state of every run, with and
 * without the spread, and the granule entries written at each CPU level must equal the scalar ones. */
static void test_kernels(void)
{
    enum { NRUN = 24, MAXN = 8192 };
    static float v[NRUN][MAXN];
    static size_t len[NRUN];
    static const double offsets[] = {0, 101325, 1e6, -273.15};
    for (int r = 0; r < NRUN; r++) {
        static const size_t lens[] = {1, 2, 31, 32, 33, 63, 64, 65, 100, 777, 1000, 8192};
        len[r] = lens[r % 12];
        double off = offsets[r % 4], amp = r < 12 ? 50 : 0.001 * (r + 1);
        for (size_t i = 0; i < len[r]; i++)
            v[r][i] = (float)(off + amp * ((double)rndr(2000001) / 1e6 - 1) + (r % 3 == 0 ? (double)i * 0.25 : 0));
    }
    static const char *const kernels[] = {"scalar", "avx2", "avx512"};
    typedef struct {
        agg_state a;
        agg_dev x;
    } state;
    state base[NRUN][2];
    granule gbase[NRUN];
    int configs = 0;
    for (int level = CPU_BASE; level <= cpu_level_max(); level++)
        for (int kk = 0; kk <= level && kk < 3; kk++) {
            setenv("TSDB_SIMD", kernels[kk], 1);
            if (cpu_set_level(level) != 0) {
                simd_select(level);
                if (cpu_level_max() != level && level != CPU_BASE)
                    break;
            }
            if (strcmp(simd_name(), kernels[kk]) != 0)
                continue;
            int diff = 0;
            for (int r = 0; r < NRUN; r++) {
                for (int sp = 0; sp < 2; sp++) {
                    state a;
                    memset(&a, 0, sizeof a);
                    agg_init(&a.a, TSDB_FLOAT32);
                    agg_dev_init(&a.x);
                    simd_agg_f32(v[r], len[r], &a.a, sp ? &a.x : NULL);
                    if (!configs)
                        base[r][sp] = a;
                    else
                        diff += memcmp(&a, &base[r][sp], sizeof a) != 0;
                }
                granule g;
                memset(&g, 0, sizeof g);
                granule_stats(TSDB_FLOAT32, v[r], len[r], &g);
                if (!configs)
                    gbase[r] = g;
                else
                    diff += memcmp(&g, &gbase[r], sizeof g) != 0;
            }
            if (diff)
                fprintf(stderr, "kernels: level %s, kernel %s: %d of %d runs differ from the scalar kernel\n",
                        cpu_level_name(level), simd_name(), diff, NRUN * 3);
            failures += diff != 0;
            configs++;
        }
    unsetenv("TSDB_SIMD");
    cpu_set_level(cpu_level_max());
    simd_select(cpu_level_max());
    printf("kernels compared on varied float32 runs: %d\n", configs);
}

/* m2 of one run, long double two-pass with the correction term: the reference for test_single_runs */
static long double ref_m2(const double *x, size_t n)
{
    long double s = 0, d1 = 0, d2 = 0;
    for (size_t i = 0; i < n; i++)
        s += x[i];
    long double m = s / n;
    for (size_t i = 0; i < n; i++) {
        long double d = x[i] - m;
        d1 += d;
        d2 += d * d;
    }
    return d2 - d1 * d1 / n;
}

/* The precision of one run, where no merge of rounded means adds its error: a small spread on a large offset
 * (1e9 + k * 1e-6, 101325 +- 5), where the mean's rounding is of the order of the spread. The corrected
 * two-pass m2 stays within a few ulps times n; without the correction term (d1) it is off by n * delta^2,
 * ~1e-5 relative here. The database level (compare) has to allow for merges and cannot check this tightly. */
static void test_single_runs(void)
{
    enum { N = 1000 };
    static double x[N];
    static float xf[N];
    int bad = 0, runs = 0;
    for (int shape = 0; shape < 4; shape++)
        for (int rep = 0; rep < 8; rep++) {
            size_t n = 2 + (size_t)rndr(N - 1);
            for (size_t i = 0; i < n; i++) {
                switch (shape) {
                case 0: x[i] = 1e9 + (double)rndr(1000) * 1e-6; break;
                case 1: x[i] = 101325 + (double)rndr(1000001) / 1e5 - 5; break;
                case 2: x[i] = -1e12 + (double)rndr(100) * 1e-3; break;
                default: xf[i] = (float)(101325 + (double)rndr(1001) / 100 - 5); x[i] = xf[i]; break;
                }
            }
            long double ref = ref_m2(x, n);
            for (int path = 0; path < (shape == 3 ? 2 : 1); path++) {
                agg_state a;
                agg_dev ax;
                agg_init(&a, shape == 3 ? TSDB_FLOAT32 : TSDB_FLOAT64);
                agg_dev_init(&ax);
                if (path)
                    simd_agg_f32(xf, n, &a, &ax);
                else if (shape == 3)
                    agg_fold(TSDB_FLOAT32, xf, n, &a, &ax);
                else
                    agg_fold(TSDB_FLOAT64, x, n, &a, &ax);
                runs++;
                if (a.count != n || !(fabsl((long double)ax.m2 - ref) <= 1e-11L * ref)) {
                    if (bad++ < 5)
                        fprintf(stderr, "single run: shape %d n %zu path %d: m2 %.17g, reference %.17Lg\n", shape, n,
                                path, ax.m2, ref);
                }
            }
        }
    failures += bad;
    printf("single runs, m2 within 1e-11 of the long double reference: %d of %d\n", runs - bad, runs);
}

/* Queries that do not ask for the spread must not pay for it: a query of 10-row buckets folds into one state
 * per bucket, and 80 bytes instead of 48 cost it 20-30% (tsdb-bench, buckets of 10 samples). The spread keeps
 * its m2 and sums of squares beside the state (agg_dev), only where it is asked for. */
static void test_state_size(void)
{
    if (sizeof(agg_state) > 48) {
        fprintf(stderr, "agg_state is %zu bytes: the spread's state belongs in agg_dev\n", sizeof(agg_state));
        failures++;
    }
}

static long maxrss_mb(void)
{
    struct rusage u;
    getrusage(RUSAGE_SELF, &u);
    return u.ru_maxrss / 1024;
}

/* The buckets of a series bit for bit the same with one dense array and in chunks of `dense` buckets: aggregates
 * and the spread, by interval, bounds and with a limit either way. */
static void same_in_chunks(tsdb *db, uint32_t id, int64_t iv, size_t dense, const char *what)
{
    static int64_t bounds[NROWS];
    size_t nbd = 0;
    for (int64_t t = 5; t < NROWS * 10 && nbd < NROWS; t += iv * 3 + 1)
        bounds[nbd++] = t;
    tsdb_query_opts o = {.order = rndr(2) ? TSDB_DESC : TSDB_ASC, .limit = 1 + (size_t)rndr(50)};
    for (int form = 0; form < 4; form++) {
        void *r[2];
        size_t n[2], row = form == 0 ? sizeof(tsdb_bucket) : sizeof(tsdb_spread_bucket);
        for (int k = 0; k < 2; k++) {
            agg_dense_buckets = k ? dense : 0;
            switch (form) {
            case 0: OK(tsdb_query_agg(db, id, 0, INT64_MAX, iv, (tsdb_bucket **)&r[k], &n[k])); break;
            case 1: OK(tsdb_query_spread(db, id, 0, INT64_MAX, iv, NULL, (tsdb_spread_bucket **)&r[k], &n[k])); break;
            case 2:
                OK(tsdb_query_spread_bounds(db, id, 0, INT64_MAX, bounds, nbd, NULL, (tsdb_spread_bucket **)&r[k],
                                            &n[k]));
                break;
            default: OK(tsdb_query_spread(db, id, 0, INT64_MAX, iv, &o, (tsdb_spread_bucket **)&r[k], &n[k]));
            }
        }
        agg_dense_buckets = 0;
        if (n[0] != n[1] || (n[0] && memcmp(r[0], r[1], n[0] * row))) {
            fprintf(stderr, "%s, interval %lld, form %d: %zu buckets in chunks of %zu, %zu in one array, or not the "
                            "same\n", what, (long long)iv, form, n[1], dense, n[0]);
            failures++;
        }
        free(r[0]);
        free(r[1]);
    }
}

/* A query's memory follows the buckets that have rows, not the span between its first and last row: two rows
 * 64 M buckets apart took 3 GB of bucket states for the aggregates and 5 GB for the spread. A span longer than
 * one dense array is computed in chunks, with the same results bit for bit. */
static void test_sparse_span(void)
{
    char dir[600];
    tsdb *db = open_db("sparse", dir);
    uint32_t id;
    OK(tsdb_series_create(db, "two", TSDB_FLOAT32, NULL, &id));
    int64_t ts[2] = {0, (64 << 20) - 1};
    float v[2] = {1.5f, 2.5f};
    OK(tsdb_insert_series(db, id, ts, v, 2));
    long before = maxrss_mb();
    for (int flushed = 0; flushed < 2; flushed++) { /* memtable rows, then one granule reaching across */
        if (flushed)
            OK(tsdb_flush(db));
        tsdb_bucket *b;
        tsdb_spread_bucket *sb;
        size_t nb, ns;
        OK(tsdb_query_agg(db, id, 0, INT64_MAX, 1, &b, &nb));
        CHECK(nb == 2 && b[0].start == 0 && b[0].sum == 1.5 && b[1].start == ts[1] && b[1].count == 1);
        free(b);
        OK(tsdb_query_spread(db, id, 0, INT64_MAX, 1, NULL, &sb, &ns));
        CHECK(ns == 2 && sb[1].b.start == ts[1] && sb[1].b.max == 2.5 && sb[1].range == 0);
        free(sb);
        tsdb_query_opts o = {.order = TSDB_DESC, .limit = 1};
        OK(tsdb_query_spread(db, id, 0, INT64_MAX, 1, &o, &sb, &ns));
        CHECK(ns == 1 && sb[0].b.start == ts[1]);
        free(sb);
    }
    long grew = maxrss_mb() - before;
    if (grew > 256) {
        fprintf(stderr, "two rows 64 M buckets apart: the queries took %ld MB\n", grew);
        failures++;
    }
    /* the span is still capped */
    int64_t t3 = 64 << 20;
    OK(tsdb_insert_series(db, id, &t3, v, 1));
    tsdb_bucket *b;
    size_t nb;
    CHECK(tsdb_query_agg(db, id, 0, INT64_MAX, 1, &b, &nb) == TSDB_ERR_TOO_MANY_BUCKETS);
    CHECK(tsdb_query_agg(db, id, 0, INT64_MAX, 2, &b, &nb) == 0 && nb == 3);
    free(b);

    /* The work follows the rows too: 40 granules of 64 rows, 1000 s apart, by the millisecond (39 M buckets).
     * A chunk takes whole granules and ends at a gap, so the dense arrays hold about the 2560 buckets with rows,
     * not 699 K buckets per chunk, nor the span. */
    OK(tsdb_series_create(db, "bursts", TSDB_FLOAT32, NULL, &id));
    enum { BURSTS = 40, BURST = 64 };
    static int64_t bt[BURSTS * BURST];
    static float bv[BURSTS * BURST];
    for (int i = 0; i < BURSTS * BURST; i++)
        bt[i] = (int64_t)(i / BURST) * 1000000 + i % BURST, bv[i] = (float)i;
    OK(tsdb_insert_series(db, id, bt, bv, BURSTS * BURST));
    OK(tsdb_flush(db));
    for (int k = 0; k < 4; k++) {
        size_t states = agg_dense_states;
        tsdb_spread_bucket *sb = NULL;
        tsdb_query_opts o = {k == 3 ? TSDB_DESC : TSDB_ASC, k >= 2 ? 1000 : 0};
        b = NULL;
        if (k == 0)
            OK(tsdb_query_agg(db, id, INT64_MIN, INT64_MAX, 1, &b, &nb));
        else
            OK(tsdb_query_spread(db, id, INT64_MIN, INT64_MAX, 1, &o, &sb, &nb));
        states = agg_dense_states - states;
        if (states > 2 * BURSTS * BURST)
            fprintf(stderr, "  %d bursts of %d rows, query %d: %zu bucket states\n", BURSTS, BURST, k, states);
        CHECK(states <= 2 * BURSTS * BURST);
        size_t want = k >= 2 ? 1000 : BURSTS * BURST, last = k == 3 ? BURSTS * BURST - 1000 : want - 1;
        CHECK(nb == want);
        if (nb == want && k == 0)
            CHECK(b[0].start == 0 && b[last].start == bt[last] && b[last].sum == bv[last]);
        if (nb == want && k > 0)
            CHECK(sb[0].b.start == bt[k == 3 ? BURSTS * BURST - 1 : 0] && sb[nb - 1].b.start == bt[last] &&
                  sb[nb - 1].b.count == 1);
        free(b);
        free(sb);
    }
    tsdb_close(db);

    /* many granules, parts that overlap (merged), memtable rows, deletes, several threads */
    tsdb_options opt;
    tsdb_options_init(&opt);
    opt.fsync = 0;
    opt.granule_rows = 64;
    opt.merge_threads = 0;
    opt.query_threads = 4;
    opt.flush_interval_ms = 600000;
    opt.buffer_rows = 1 << 22;
    opt.wal_max_batch_rows = 64;
    snprintf(dir, sizeof dir, "%s/chunks", base);
    rmrf(dir);
    OK(tsdb_open(dir, &opt, &db));
    static ref_series s;
    for (int t = 0; t < NT; t++)
        for (int shape = 0; shape < NSHAPE; shape += 2) {
            char name[32];
            snprintf(name, sizeof name, "c%d_%d", t, shape);
            OK(tsdb_series_create(db, name, (tsdb_type)types[t], NULL, &id));
            gen_series(&s, types[t], shape);
            put_rows(db, id, &s, 0, NROWS, true, false);
            put_rows(db, id, &s, 0, NROWS / 2, false, false);
            put_rows(db, id, &s, NROWS / 3, NROWS, false, true);
            OK(tsdb_delete_range(db, id, 20000, 20000 + rndr(5000)));
            static const int64_t ivs[] = {1, 7, 640, 777};
            for (size_t q = 0; q < sizeof ivs / sizeof ivs[0]; q++)
                same_in_chunks(db, id, ivs[q], 1 + (size_t)rndr(ivs[q] < 100 ? 3000 : 20), name);
        }
    tsdb_close(db);
}

int main(int argc, char **argv)
{
    snprintf(base, sizeof base, "%s/tsdb_spread_%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rmrf(base);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "mkdir -p '%s'", base);
    if (system(cmd) != 0)
        return 1;
    test_state_size();
    test_hand();
    test_kernels();
    test_single_runs();
    test_random();
    test_fast_path();
    test_sparse_span();
    test_deletes(false);
    test_deletes(true);
    free(btype);
    rmrf(base);
    if (failures) {
        fprintf(stderr, "test_spread: %d failures\n", failures);
        return 1;
    }
    printf("test_spread: ok\n");
    return 0;
}
