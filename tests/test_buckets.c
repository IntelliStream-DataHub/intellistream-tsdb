/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Small buckets: where each bucket's run of rows ends (query.c run_end, which steps from the previous run's length
 * before it searches) and the passes over the dense array of bucket states, which run by chunks on the query's
 * threads (dense_run). Rows regular, jittered, in bursts of 1 to 300 rows with gaps of whole buckets, on bucket
 * edges and a millisecond before them, dense; nulls (a bucket of nothing but nulls among them), range deletes
 * inside a bucket and across an edge, a later part over some rows, and rows still in the memtable. Every bucket of
 * fixed intervals from 1 s to 1 h and of irregular bounds is checked against the live rows (tsdb_query_values):
 * its start and count exactly (any row in the wrong run changes them), min and max exactly, the sum to rounding;
 * and every answer must be the same bytes at query_threads 1 and 4 and chunks of 1, 3, 1000 buckets and the
 * default.
 *
 *   test_buckets [scratch dir]
 */
#include "internal.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fails;

#define CHECK(c, ...)                                                                                                  \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);                                                       \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fprintf(stderr, "\n");                                                                                     \
            if (++fails > 20)                                                                                          \
                exit(1);                                                                                               \
        }                                                                                                              \
    } while (0)

static void must(int rc, const char *what)
{
    if (rc) {
        fprintf(stderr, "FAIL %s: %s\n", what, tsdb_strerror(rc));
        exit(1);
    }
}

static const int64_t T0 = 1700000040000; /* a minute edge */

static uint64_t rnd_state = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rnd_state ^= rnd_state << 13, rnd_state ^= rnd_state >> 7, rnd_state ^= rnd_state << 17;
    return rnd_state;
}

static tsdb *open_db(const char *dir, int threads)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.granule_rows = 256; /* buckets of a minute and more span granules */
    o.query_threads = threads;
    tsdb *db;
    must(tsdb_open(dir, &o, &db), "open");
    return db;
}

/* rows in time order: n of them from t on, step apart, each moved by up to +-jitter */
static size_t add_rows(int64_t *ts, size_t n, size_t count, int64_t *t, int64_t step, int64_t jitter)
{
    for (size_t i = 0; i < count; i++) {
        int64_t j = jitter ? (int64_t)(rnd() % (uint64_t)(2 * jitter + 1)) - jitter : 0;
        ts[n++] = *t + j;
        *t += step;
    }
    return n;
}

static int64_t nulls_from, nulls_to; /* one minute of d holds only nulls */

static void load(tsdb *db, tsdb_sid *f, tsdb_sid *d)
{
    must(tsdb_series_create(db, "f", TSDB_FLOAT32, NULL, f), "create f");
    must(tsdb_series_create(db, "d", TSDB_FLOAT64, NULL, d), "create d");
    size_t cap = 100000, n = 0;
    int64_t *ts = malloc(cap * sizeof *ts), t = T0;
    n = add_rows(ts, n, 3000, &t, 1000, 0); /* regular, a row on every minute edge */
    t += 2 * 3600000 + 17000;
    n = add_rows(ts, n, 3000, &t, 1000, 400); /* jittered */
    t += 60000;
    for (int k = 0; k < 200; k++) { /* bursts: runs far from the length of the one before, gaps of whole buckets */
        static const size_t sizes[] = {1, 2, 5, 100, 300};
        n = add_rows(ts, n, sizes[rnd() % 5], &t, 7, 0);
        t += 1000 + (int64_t)(rnd() % 200000);
    }
    t = (t / 60000 + 1) * 60000;
    nulls_from = t + 5 * 60000, nulls_to = nulls_from + 60000;
    for (int m = 0; m < 100; m++) { /* a millisecond before an edge and on it */
        ts[n++] = t + m * 60000 - 1;
        ts[n++] = t + m * 60000;
    }
    t += 100 * 60000 + 1234;
    n = add_rows(ts, n, 20000, &t, 3, 0); /* dense: many rows per second */
    float *v = malloc(n * sizeof *v);
    double *dv = malloc(n * sizeof *dv);
    uint8_t *valid = calloc((n + 7) / 8, 1);
    for (size_t i = 0; i < n; i++) {
        v[i] = (float)((double)(rnd() % 100000) / 8.0 - 4000.0);
        dv[i] = (double)(rnd() % 1000003) * 1.000001e-3 + 1e6;
        bool null = i % 7 == 0 || (ts[i] >= nulls_from && ts[i] < nulls_to);
        if (!null)
            valid[i / 8] |= (uint8_t)(1u << (i % 8));
    }
    must(tsdb_insert_series(db, *f, ts, v, n), "insert f");
    tsdb_values vals = {.type = TSDB_FLOAT64, .values = dv, .valid = valid};
    must(tsdb_insert_series_values(db, *d, ts, &vals, n), "insert d");
    must(tsdb_flush(db), "flush");
    /* a later part over every 3rd jittered row, other values */
    size_t m = 0;
    for (size_t i = 3000; i < 6000; i += 3) {
        ts[m] = ts[i];
        v[m] = v[i] + 0.5f;
        dv[m] = dv[i] - 0.25;
        m++;
    }
    must(tsdb_insert_series(db, *f, ts, v, m), "overlap f");
    vals.valid = NULL;
    must(tsdb_insert_series_values(db, *d, ts, &vals, m), "overlap d");
    must(tsdb_flush(db), "flush");
    for (int s = 0; s < 2; s++) {
        tsdb_sid id = s ? *d : *f;
        must(tsdb_delete_range(db, id, T0 + 600000 + 10000, T0 + 600000 + 40000), "delete inside a minute");
        int64_t e = (ts[4500] / 60000) * 60000;
        must(tsdb_delete_range(db, id, e - 5000, e + 5000), "delete across an edge");
        e = (ts[n - 5000] / 60000) * 60000;
        must(tsdb_delete_range(db, id, e, e + 60000), "delete a minute");
    }
    /* rows still in the memtable after the last part */
    t = ts[n - 1] + 1000;
    for (int i = 0; i < 500; i++)
        must(tsdb_append(db, *f, t + i * 250, (float)i), "append");
    for (int i = 0; i < 500; i++)
        ts[i] = t + i * 250, dv[i] = i * 0.5;
    vals.valid = NULL;
    must(tsdb_insert_series_values(db, *d, ts, &vals, 500), "insert d tail");
    free(ts);
    free(v);
    free(dv);
    free(valid);
}

typedef struct live {
    int64_t *ts;
    double *v;
    uint8_t *null;
    size_t n, cap;
} live;

static int take(void *ctx, const int64_t *ts, const tsdb_values *vals, size_t n)
{
    live *l = ctx;
    if (l->n + n > l->cap) {
        l->cap = (l->n + n) * 2;
        l->ts = realloc(l->ts, l->cap * sizeof *l->ts);
        l->v = realloc(l->v, l->cap * sizeof *l->v);
        l->null = realloc(l->null, l->cap);
    }
    for (size_t i = 0; i < n; i++) {
        l->ts[l->n] = ts[i];
        l->v[l->n] = vals->type == TSDB_FLOAT32 ? ((const float *)vals->values)[i] : ((const double *)vals->values)[i];
        l->null[l->n] = vals->valid && !(vals->valid[i / 8] >> (i % 8) & 1);
        l->n++;
    }
    return 0;
}

static int64_t floor_div64(int64_t a, int64_t b) { return a / b - (a % b < 0); }

/* bucket i of the live rows: interval > 0, else the last bound at or before ts */
static int64_t ref_start(int64_t ts, int64_t iv, const int64_t *bounds, size_t nb)
{
    if (iv)
        return floor_div64(ts, iv) * iv;
    size_t i = 0;
    while (i + 1 < nb && bounds[i + 1] <= ts)
        i++;
    return bounds[i];
}

/* the answer against the live rows of [start, end) */
static void check(tsdb *db, tsdb_sid s, int64_t start, int64_t end, int64_t iv, const int64_t *bounds, size_t nbd,
                  const tsdb_bucket *b, size_t nb, const char *what)
{
    live l = {0};
    must(tsdb_query_values(db, s, start, end, NULL, take, &l), "values");
    size_t k = 0;
    for (size_t i = 0; i < l.n && k <= nb;) {
        int64_t bs = ref_start(l.ts[i], iv, bounds, nbd);
        uint64_t count = 0;
        double sum = 0, mn = INFINITY, mx = -INFINITY;
        size_t j = i;
        for (; j < l.n && ref_start(l.ts[j], iv, bounds, nbd) == bs; j++) {
            if (l.null[j])
                continue;
            count++;
            sum += l.v[j];
            mn = l.v[j] < mn ? l.v[j] : mn;
            mx = l.v[j] > mx ? l.v[j] : mx;
        }
        if (k == nb) {
            CHECK(0, "%s: series %u iv %lld: bucket at %lld missing (answer has %zu)", what, TSDB_SID_LOCAL(s), (long long)iv,
                  (long long)bs, nb);
            break;
        }
        CHECK(b[k].start == bs && b[k].count == count, "%s: series %u iv %lld bucket %zu: start %lld count %llu, "
              "want %lld %llu", what, TSDB_SID_LOCAL(s), (long long)iv, k, (long long)b[k].start, (unsigned long long)b[k].count,
              (long long)bs, (unsigned long long)count);
        if (count)
            CHECK(b[k].min == mn && b[k].max == mx && fabs(b[k].sum - sum) <= 1e-9 * (fabs(sum) + 1),
                  "%s: series %u iv %lld bucket at %lld: min %g max %g sum %.17g, want %g %g %.17g", what,
                  TSDB_SID_LOCAL(s),
                  (long long)iv, (long long)bs, b[k].min, b[k].max, b[k].sum, mn, mx, sum);
        k++;
        i = j;
    }
    CHECK(k == nb, "%s: series %u iv %lld: %zu buckets, want %zu", what, TSDB_SID_LOCAL(s), (long long)iv, nb, k);
    free(l.ts);
    free(l.v);
    free(l.null);
}

typedef struct blob {
    uint8_t *p;
    size_t n, cap;
} blob;

static void put(blob *b, const void *p, size_t n)
{
    if (b->n + n + 8 > b->cap) {
        b->cap = (b->n + n + 8) * 2;
        b->p = realloc(b->p, b->cap);
    }
    memcpy(b->p + b->n, &n, 8);
    if (n)
        memcpy(b->p + b->n + 8, p, n);
    b->n += n + 8;
}

static void answers(tsdb *db, const tsdb_sid *ids, bool verify, blob *out)
{
    static const int64_t iv[] = {1000, 7000, 60000, 3600000};
    int64_t lo = T0 + 12345, hi = T0 + 1000LL * 86400000;
    /* irregular bounds: steps of 1 ms to 10 minutes, some on rows */
    int64_t bounds[2000];
    size_t nbd = 0;
    for (int64_t t = T0 - 5000; nbd < 2000; nbd++) {
        bounds[nbd] = t;
        t += 1 + (int64_t)(rnd_state % 600000);
        rnd_state = rnd_state * 6364136223846793005ull + 1442695040888963407ull;
    }
    for (int s = 0; s < 2; s++) {
        for (size_t k = 0; k < sizeof iv / sizeof *iv; k++)
            for (int w = 0; w < 2; w++) {
                int64_t start = w ? lo : INT64_MIN, end = w ? hi : INT64_MAX;
                tsdb_bucket *b;
                size_t nb;
                must(tsdb_query_agg(db, ids[s], start, end, iv[k], &b, &nb), "agg");
                if (verify)
                    check(db, ids[s], start, end, iv[k], NULL, 0, b, nb, "agg");
                put(out, b, nb * sizeof *b);
                free(b);
                tsdb_spread_bucket *sb;
                must(tsdb_query_spread(db, ids[s], start, end, iv[k], NULL, &sb, &nb), "spread");
                put(out, sb, nb * sizeof *sb);
                free(sb);
            }
        tsdb_bucket *b;
        size_t nb;
        must(tsdb_query_bounds(db, ids[s], bounds[0], INT64_MAX, bounds, nbd, NULL, &b, &nb), "bounds");
        if (verify)
            check(db, ids[s], bounds[0], INT64_MAX, 0, bounds, nbd, b, nb, "bounds");
        put(out, b, nb * sizeof *b);
        free(b);
    }
}

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/test_buckets_%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    tsdb *db = open_db(dir, 1);
    tsdb_sid ids[2];
    load(db, &ids[0], &ids[1]);
    uint64_t seed = rnd_state;
    blob ref = {0};
    /* Checked against the live rows with the tail still in the memtable. Its bytes are not the reference: hour
     * cuts let an hourly bucket take a granule's statistics once the tail is in a part, and variances from them can
     * differ in the last bits from the same rows summed one by one (CHANGELOG, hour cuts). */
    blob mem = {0};
    answers(db, ids, true, &mem);
    free(mem.p);
    tsdb_close(db);
    rnd_state = seed;
    db = open_db(dir, 1); /* the close put the tail in a part */
    answers(db, ids, true, &ref);
    tsdb_close(db);
    static const int qt[] = {1, 4};
    static const size_t chunks[] = {1, 3, 1000, 8192};
    int configs = 0;
    for (size_t a = 0; a < sizeof qt / sizeof *qt; a++) {
        db = open_db(dir, qt[a]);
        for (size_t c = 0; c < sizeof chunks / sizeof *chunks; c++) {
            dense_chunk = chunks[c];
            rnd_state = seed; /* the same bounds */
            blob got = {0};
            answers(db, ids, false, &got);
            CHECK(got.n == ref.n && memcmp(got.p, ref.p, ref.n) == 0,
                  "answers at query_threads %d, chunks of %zu differ from the first", qt[a], chunks[c]);
            free(got.p);
            configs++;
        }
        tsdb_close(db);
    }
    free(ref.p);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        fprintf(stderr, "could not remove %s\n", dir);
    if (fails) {
        fprintf(stderr, "test_buckets: %d failures\n", fails);
        return 1;
    }
    printf("test_buckets: OK (%d configurations)\n", configs);
    return 0;
}
