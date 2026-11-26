/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Threads per query (query_threads_for): a query's answer must not depend on how many threads decoded it.
 * Aggregates, spreads and TWAs (step and linear) over windows of 1 to 40 granules, cut by buckets that do not
 * line up with granules, over parts that overlap in places, are compared byte for byte at query_threads 1, 2 and
 * 32 with query_rows_per_thread at one thread, one thread per granule, and around the default. Besides two float
 * series over overlapping parts, an int64 series with deleted ranges, a bool series with a replaced range, a float64
 * series with rows still in the memtable and a float32 series under retention make the snapshot windows
 * (tombstones, memtable runs, the cutoff) part of what the thread counts must agree on. The multi-thread
 * configurations must in fact have used more than one thread (query_threads_peak), or they prove nothing.
 *
 *   test_threads [scratch dir]
 */
#include "internal.h"

#include <stdio.h>
#include "watchdog.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define GR 8192
#define NG 48 /* granules of the base series */
#define NSERIES 6 /* a, b: float32 and float64 over overlapping parts; c, d, e, f: see load() */

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

/* a stopped clock: retention's "now" is the end of the data, so the cutoff falls inside it */
static int64_t clock_at_end(void *arg)
{
    (void)arg;
    return (int64_t)NG * GR * 1000;
}

static tsdb *open_db(const char *dir, int threads)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.clock_ms = clock_at_end;
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

    /* c (int64): ranges in granules 20 to 22 and 30 deleted; d (bool): granules 25 to 27 replaced by a sparse set;
     * e (float64): a tail of rows, written after the flush, that stays in the memtable; f (float32): retention
     * cuts at granule 28 (the clock stands at the end of the data) */
    tsdb_sid c, bl, e, f;
    must(tsdb_series_create(db, "c", TSDB_INT64, NULL, &c), "create c");
    must(tsdb_series_create(db, "d", TSDB_BOOL, NULL, &bl), "create d");
    must(tsdb_series_create(db, "e", TSDB_FLOAT64, NULL, &e), "create e");
    must(tsdb_series_create(db, "f", TSDB_FLOAT32, NULL, &f), "create f");
    int64_t *iv = malloc(n * sizeof *iv);
    uint8_t *bv = malloc(n);
    x = 0x9e3779b97f4a7c15ull; /* the overlap pass overwrote the head of ts: the same base timestamps again */
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13, x ^= x >> 7, x ^= x << 17;
        ts[i] = (int64_t)i * 1000 + (int64_t)(x % 700);
        iv[i] = (int64_t)(x % 2000003) - 1000000 + (int64_t)i * 3;
        bv[i] = (uint8_t)((x >> 20) % 3 == 0);
        d[i] = (double)(x % 1000003) * 1.000001e-3 - 5.0;
        v[i] = (float)((double)(x % 100000) / 7.0 - 3000.0);
    }
    tsdb_values ivals = {.type = TSDB_INT64, .values = iv};
    must(tsdb_insert_series_values(db, c, ts, &ivals, n), "insert c");
    tsdb_values bvals = {.type = TSDB_BOOL, .values = bv};
    must(tsdb_insert_series_values(db, bl, ts, &bvals, n), "insert d");
    must(tsdb_insert_series(db, f, ts, v, n), "insert f");
    tsdb_values dvals = {.type = TSDB_FLOAT64, .values = d};
    must(tsdb_insert_series_values(db, e, ts, &dvals, n - 300), "insert e");
    must(tsdb_set_retention(db, f, (int64_t)(NG - 28) * GR * 1000), "retention f");
    must(tsdb_delete_range(db, c, 20 * (int64_t)GR * 1000 + 777, 23 * (int64_t)GR * 1000 - 4242), "delete c");
    must(tsdb_delete_range(db, c, 30 * (int64_t)GR * 1000, 30 * (int64_t)GR * 1000 + 5000000), "delete c 2");
    size_t lo = 25 * (size_t)GR, hi = 28 * (size_t)GR, m2 = 0;
    int64_t *rts = malloc((hi - lo) / 7 * 8 + 8);
    uint8_t *rbv = malloc((hi - lo) / 7 + 1);
    for (size_t i = lo; i < hi; i += 7)
        rts[m2] = ts[i], rbv[m2] = (uint8_t)!bv[i], m2++;
    tsdb_values rvals = {.type = TSDB_BOOL, .values = rbv};
    must(tsdb_replace_range(db, bl, ts[lo], ts[hi - 1] + 1, rts, &rvals, m2), "replace d");
    free(rts);
    free(rbv);
    must(tsdb_flush(db), "flush 2");
    /* the memtable tail of e: its last 300 rows, then 40 of them again, newer, over rows already in parts */
    tsdb_values tail = {.type = TSDB_FLOAT64, .values = d + (n - 300)};
    must(tsdb_insert_series_values(db, e, ts + (n - 300), &tail, 300), "tail e");
    for (size_t i = 0; i < 40; i++)
        d[n - 340 + i] += 9.0;
    tail.values = d + (n - 340);
    must(tsdb_insert_series_values(db, e, ts + (n - 340), &tail, 40), "tail e over parts");
    free(ts);
    free(v);
    free(d);
    free(valid);
    free(iv);
    free(bv);
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
    size_t per_series[NSERIES] = {0};
    for (tsdb_sid s = TSDB_SID(0, 0); s <= TSDB_SID(0, NSERIES - 1); s++) /* a to f, the first six series */
        for (size_t gi = 0; gi < sizeof ng / sizeof *ng; gi++)
            for (size_t k = 0; k < sizeof iv / sizeof *iv; k++) {
                int64_t start = 6 * GR * 1000LL + 12345, end = start + (int64_t)ng[gi] * GR * 1000;
                if (ng[gi] == 40)
                    start = INT64_MIN, end = INT64_MAX;
                void *r;
                size_t n;
                must(tsdb_query_agg(db, s, start, end, iv[k], (tsdb_bucket **)&r, &n), "agg");
                buckets += n;
                per_series[TSDB_SID_LOCAL(s)] += n;
                put(out, r, n * sizeof(tsdb_bucket));
                free(r);
                /* the other kinds may refuse a type; the refusal must be the same at every thread count */
                int rc = tsdb_query_spread(db, s, start, end, iv[k], NULL, (tsdb_spread_bucket **)&r, &n);
                put(out, &rc, sizeof rc);
                if (rc == 0) {
                    put(out, r, n * sizeof(tsdb_spread_bucket));
                    free(r);
                }
                for (int meth = 0; meth < 2; meth++) {
                    tsdb_twa_opts o = {.method = meth ? TSDB_TWA_LINEAR : TSDB_TWA_STEP};
                    rc = tsdb_query_twa(db, s, start, end, iv[k], &o, (tsdb_twa_bucket **)&r, &n);
                    put(out, &rc, sizeof rc);
                    if (rc == 0) {
                        put(out, r, n * sizeof(tsdb_twa_bucket));
                        free(r);
                    }
                }
            }
    for (int s = 0; s < NSERIES; s++)
        if (per_series[s] == 0) {
            fprintf(stderr, "FAIL: the queries found no rows of series %d\n", s);
            exit(1);
        }
    if (buckets == 0) { /* ids that name no series answer nothing, alike at every thread count */
        fprintf(stderr, "FAIL: the queries found no rows\n");
        exit(1);
    }
}

int main(int argc, char **argv)
{
    th_watchdog("test_threads", 1200);
    char dir[512];
    snprintf(dir, sizeof dir, "%s/test_threads_%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    tsdb *db = open_db(dir, 1);
    load(db);
    tsdb_close(db);

    static const int qt[] = {1, 2, 32};
    static const uint64_t per[] = {UINT64_MAX, 1, GR, 2 * GR, QUERY_ROWS_PER_THREAD, 3 * GR + 1};
    blob ref = {0};
    int configs = 0, fanned = 0;
    for (size_t a = 0; a < sizeof qt / sizeof *qt; a++) {
        db = open_db(dir, qt[a]);
        for (size_t b = 0; b < sizeof per / sizeof *per; b++) {
            query_rows_per_thread = per[b];
            blob got = {0};
            atomic_store(&query_threads_peak, 0);
            answers(db, &got);
            /* the configuration did what its name says: one thread where it must, several where it can. At one
             * granule or two per thread over windows of 1 to 40 granules, query_threads 2 and 32 must fan out. */
            int peak = atomic_load(&query_threads_peak);
            CHECK(peak >= 1 && peak <= qt[a], "query_threads %d, rows per thread %llu: peak %d threads", qt[a],
                  (unsigned long long)per[b], peak);
            if (qt[a] == 1 || per[b] == UINT64_MAX)
                CHECK(peak == 1, "query_threads %d, rows per thread %llu: %d threads, expected one", qt[a],
                      (unsigned long long)per[b], peak);
            else if (per[b] <= 2 * GR) {
                CHECK(peak >= 2, "query_threads %d, rows per thread %llu: only %d thread(s) ever asked for: the "
                                 "comparison proves nothing",
                      qt[a], (unsigned long long)per[b], peak);
                if (qt[a] == 32)
                    CHECK(peak >= 8, "query_threads 32, rows per thread %llu: peak only %d", (unsigned long long)per[b],
                          peak);
            }
            if (qt[a] > 1 && per[b] <= 2 * GR)
                fanned++;
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

    /* Verify worker-local qstats and granule_checks folding produce identical counters at 1 vs 8 threads. */
    {
        tsdb_options o;
        tsdb_options_init(&o);
        o.clock_ms = clock_at_end;
        o.fsync = 0;
        o.merge_threads = 0;
        o.verify_checksums = 1;
        tsdb_sid s = TSDB_SID(0, 0);
        int64_t start = 6 * GR * 1000LL + 12345, end = start + 33LL * GR * 1000;
        tsdb_twa_opts to = {.method = TSDB_TWA_STEP};
        tsdb_query_stats q1[3], q8[3];
        uint64_t gc1[3] = {0}, gc8[3] = {0};
        void *r1[3] = {0}, *r8[3] = {0};
        size_t n1[3] = {0}, n8[3] = {0};

        query_rows_per_thread = GR;
        for (int pass = 0; pass < 2; pass++) {
            o.query_threads = pass == 0 ? 1 : 8;
            tsdb *vdb = NULL;
            must(tsdb_open(dir, &o, &vdb), pass == 0 ? "open verify 1" : "open verify 8");
            tsdb_query_stats *qs = pass == 0 ? q1 : q8;
            uint64_t *gc = pass == 0 ? gc1 : gc8;
            void **res = pass == 0 ? r1 : r8;
            size_t *ns = pass == 0 ? n1 : n8;

            uint64_t b = atomic_load(&granule_checks);
            must(tsdb_query_agg(vdb, s, start, end, 420000, (tsdb_bucket **)&res[0], &ns[0]), "agg verify");
            gc[0] = atomic_load(&granule_checks) - b;
            tsdb_last_query_stats(&qs[0]);

            b = atomic_load(&granule_checks);
            must(tsdb_query_spread(vdb, s, start, end, 420000, NULL, (tsdb_spread_bucket **)&res[1], &ns[1]),
                 "spread verify");
            gc[1] = atomic_load(&granule_checks) - b;
            tsdb_last_query_stats(&qs[1]);

            b = atomic_load(&granule_checks);
            must(tsdb_query_twa(vdb, s, start, end, 420000, &to, (tsdb_twa_bucket **)&res[2], &ns[2]), "twa verify");
            gc[2] = atomic_load(&granule_checks) - b;
            tsdb_last_query_stats(&qs[2]);

            tsdb_close(vdb);
        }

        static const size_t rsz[3] = {sizeof(tsdb_bucket), sizeof(tsdb_spread_bucket), sizeof(tsdb_twa_bucket)};
        static const char *const kname[3] = {"agg", "spread", "twa"};
        for (int k = 0; k < 3; k++) {
            CHECK(n1[k] == n8[k] && memcmp(r1[k], r8[k], n1[k] * rsz[k]) == 0, "%s verify results differ", kname[k]);
            CHECK(q1[k].rows_decoded == q8[k].rows_decoded && q1[k].bytes_read == q8[k].bytes_read &&
                      q1[k].rows_decoded > 0,
                  "%s verify qstats differ: %llu/%llu vs %llu/%llu", kname[k], (unsigned long long)q1[k].rows_decoded,
                  (unsigned long long)q1[k].bytes_read, (unsigned long long)q8[k].rows_decoded,
                  (unsigned long long)q8[k].bytes_read);
#ifdef TSDB_TEST_HOOKS
            CHECK(gc1[k] == gc8[k] && gc1[k] > 0, "%s verify granule_checks differ: %llu vs %llu", kname[k],
                  (unsigned long long)gc1[k], (unsigned long long)gc8[k]);
#endif
            free(r1[k]);
            free(r8[k]);
        }
        query_rows_per_thread = QUERY_ROWS_PER_THREAD;
    }

    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd)) {
    }
    if (fails)
        return 1;
    printf("test_threads: %d configurations (%d fanned out over several threads), identical answers (%zu bytes each)\n",
           configs, fanned, ref.n);
    return 0;
}
