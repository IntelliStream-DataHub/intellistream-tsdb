/*
 * Threads per query (query_threads_for): a query's answer must not depend on how many threads decoded it.
 * Aggregates, spreads and TWAs (step and linear) over windows of 1 to 40 granules, cut by buckets that do not
 * line up with granules, over parts that overlap in places, are compared byte for byte at query_threads 1, 2 and
 * 32 with query_rows_per_thread at one thread, one thread per granule, and around the default.
 *
 *   test_threads [scratch dir]
 */
#include "internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define GR 8192
#define NG 48 /* granules of the base series */

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

static tsdb *open_db(const char *dir, int threads)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.query_threads = threads;
    tsdb *db;
    must(tsdb_open(dir, &o, &db), "open");
    return db;
}

static void load(tsdb *db)
{
    tsdb_sid a, b;
    must(tsdb_series_create(db, "a", TSDB_FLOAT32, NULL, &a), "create a");
    must(tsdb_series_create(db, "b", TSDB_FLOAT64, NULL, &b), "create b");
    size_t n = (size_t)NG * GR;
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    double *d = malloc(n * sizeof *d);
    uint8_t *valid = calloc((n + 7) / 8, 1); /* b: one row in 19 null */
    uint64_t x = 0x9e3779b97f4a7c15ull;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13, x ^= x >> 7, x ^= x << 17;
        ts[i] = (int64_t)i * 1000 + (int64_t)(x % 700); /* irregular steps */
        v[i] = (float)((double)(x % 100000) / 7.0 - 3000.0);
        d[i] = (double)(x % 1000003) * 1.000001e-3 + 1e9;
        if (i % 19)
            valid[i / 8] |= (uint8_t)(1u << (i % 8));
    }
    must(tsdb_insert_series(db, a, ts, v, n), "insert a");
    tsdb_values vals = {.type = TSDB_FLOAT64, .values = d, .valid = valid};
    must(tsdb_insert_series_values(db, b, ts, &vals, n), "insert b");
    /* overlap: every 5th row of granules 10 to 13 again, other values, as a later part */
    size_t m = 0;
    for (size_t i = 10 * GR; i < 14 * GR; i += 5) {
        ts[m] = ts[i];
        v[m] = v[i] + 1.5f;
        d[m] = d[i] - 2.25;
        m++;
    }
    must(tsdb_insert_series(db, a, ts, v, m), "overlap a");
    vals.valid = NULL;
    must(tsdb_insert_series_values(db, b, ts, &vals, m), "overlap b");
    must(tsdb_flush(db), "flush");
    free(ts);
    free(v);
    free(d);
    free(valid);
}

/* every answer of one configuration, concatenated */
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

static void answers(tsdb *db, blob *out)
{
    static const int ng[] = {1, 2, 3, 4, 5, 7, 8, 9, 16, 17, 33, 40};
    static const int64_t iv[] = {420000, 60000, 3600000, 0};
    size_t buckets = 0;
    for (tsdb_sid s = TSDB_SID(0, 0); s <= TSDB_SID(0, 1); s++) /* "a" and "b", the first two series */
        for (size_t gi = 0; gi < sizeof ng / sizeof *ng; gi++)
            for (size_t k = 0; k < sizeof iv / sizeof *iv; k++) {
                int64_t start = 6 * GR * 1000LL + 12345, end = start + (int64_t)ng[gi] * GR * 1000;
                if (ng[gi] == 40)
                    start = INT64_MIN, end = INT64_MAX;
                void *r;
                size_t n;
                must(tsdb_query_agg(db, s, start, end, iv[k], (tsdb_bucket **)&r, &n), "agg");
                buckets += n;
                put(out, r, n * sizeof(tsdb_bucket));
                free(r);
                must(tsdb_query_spread(db, s, start, end, iv[k], NULL, (tsdb_spread_bucket **)&r, &n), "spread");
                put(out, r, n * sizeof(tsdb_spread_bucket));
                free(r);
                for (int meth = 0; meth < 2; meth++) {
                    tsdb_twa_opts o = {.method = meth ? TSDB_TWA_LINEAR : TSDB_TWA_STEP};
                    must(tsdb_query_twa(db, s, start, end, iv[k], &o, (tsdb_twa_bucket **)&r, &n), "twa");
                    put(out, r, n * sizeof(tsdb_twa_bucket));
                    free(r);
                }
            }
    if (buckets == 0) { /* ids that name no series answer nothing, alike at every thread count */
        fprintf(stderr, "FAIL: the queries found no rows\n");
        exit(1);
    }
}

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/test_threads_%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    tsdb *db = open_db(dir, 1);
    load(db);
    tsdb_close(db);

    static const int qt[] = {1, 2, 32};
    static const uint64_t per[] = {UINT64_MAX, 1, GR, 2 * GR, QUERY_ROWS_PER_THREAD, 3 * GR + 1};
    blob ref = {0};
    int configs = 0;
    for (size_t a = 0; a < sizeof qt / sizeof *qt; a++) {
        db = open_db(dir, qt[a]);
        for (size_t b = 0; b < sizeof per / sizeof *per; b++) {
            query_rows_per_thread = per[b];
            blob got = {0};
            answers(db, &got);
            if (!ref.p)
                ref = got;
            else {
                CHECK(got.n == ref.n && memcmp(got.p, ref.p, ref.n) == 0,
                      "answers at query_threads %d, rows per thread %llu differ from one thread's", qt[a],
                      (unsigned long long)per[b]);
                free(got.p);
            }
            configs++;
        }
        tsdb_close(db);
    }
    query_rows_per_thread = QUERY_ROWS_PER_THREAD;
    free(ref.p);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd)) {
    }
    if (fails)
        return 1;
    printf("test_threads: %d configurations, identical answers (%zu bytes each)\n", configs, ref.n);
    return 0;
}
