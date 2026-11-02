/*
 * Time-weighted averages: the granule-statistics fast path against decoding
 * every granule, next to the plain average, on one irregular on-change series.
 *
 *   twa-bench --dir /path --rows 100000000 [--load] [--repeat 3]
 *
 * --load (re)creates the series: readings about every second (jittered, with
 * an occasional gap of minutes) of a slowly drifting value rounded to 0.1 that
 * is written only when it changes, as on-change sensors report, in float32;
 * 30-day partitions, merged down. Without --load the queries run on what is
 * there. Each query runs --repeat times and the best time is printed: TWA by
 * hour and by day over all rows, STEP and LINEAR, from statistics and with
 * tsdb_twa_opts.no_stats (every granule decoded), and tsdb_query_agg's avg.
 */
#include "common.h"
#include "tsdb.h"

#include <math.h>
#include <stdlib.h>

#define BATCH (1u << 20)

static void fail(const char *what, int rc)
{
    fprintf(stderr, "twa-bench: %s: %s\n", what, tsdb_strerror(rc));
    exit(1);
}

static void load(tsdb *db, uint64_t rows)
{
    tsdb_sid s;
    int rc = tsdb_series_create(db, "twa", TSDB_FLOAT32, NULL, &s);
    if (rc)
        fail("create", rc);
    int64_t *ts = malloc(BATCH * sizeof *ts);
    float *v = malloc(BATCH * sizeof *v);
    uint64_t rng = 0x9e3779b97f4a7c15ull;
    double x = 20, last = NAN;
    int64_t t = 0;
    double t0 = now_s();
    for (uint64_t done = 0; done < rows;) {
        size_t n = 0;
        while (n < BATCH && done + n < rows) {
            rng ^= rng << 13, rng ^= rng >> 7, rng ^= rng << 17;
            t += 1000 + (int64_t)(rng % 401) - 200;
            if (rng % 50000 == 0)
                t += 60000 * (int64_t)(1 + rng % 30); /* an outage */
            x += ((double)(rng >> 40) / (1 << 24) - 0.5) * 0.2;
            double r = round(x * 10) / 10;
            if (r == last && rng % 64) /* on change, with a heartbeat now and then */
                continue;
            last = r;
            ts[n] = t;
            v[n++] = (float)r;
        }
        if ((rc = tsdb_insert_series(db, s, ts, v, n)))
            fail("insert", rc);
        done += n;
    }
    if ((rc = tsdb_flush(db)) || (rc = tsdb_optimize(db)))
        fail("optimize", rc);
    fprintf(stderr, "loaded %" PRIu64 " rows in %.1f s\n", rows, now_s() - t0);
    free(ts);
    free(v);
}

static double best_twa(tsdb *db, tsdb_sid s, int64_t interval, int method, bool decode, int repeat, double *sum,
                       tsdb_query_stats *st)
{
    double best = 1e30;
    for (int r = 0; r < repeat; r++) {
        tsdb_twa_opts o = {.method = method, .no_stats = decode};
        tsdb_twa_bucket *b;
        size_t n;
        double t0 = now_s();
        int rc = tsdb_query_twa(db, s, INT64_MIN, INT64_MAX, interval, &o, &b, &n);
        double t = now_s() - t0;
        if (rc)
            fail("twa", rc);
        tsdb_last_query_stats(st);
        double area = 0, ms = 0;
        for (size_t i = 0; i < n; i++)
            area += b[i].area, ms += (double)b[i].covered_ms;
        *sum = area / ms;
        free(b);
        best = t < best ? t : best;
    }
    return best;
}

int main(int argc, char **argv)
{
    const char *dir = NULL;
    uint64_t rows = 100000000;
    int repeat = 3;
    bool do_load = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc)
            dir = argv[++i];
        else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc)
            rows = strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--repeat") == 0 && i + 1 < argc)
            repeat = atoi(argv[++i]);
        else if (strcmp(argv[i], "--load") == 0)
            do_load = true;
        else
            dir = NULL, i = argc;
    }
    if (!dir || repeat < 1) {
        fprintf(stderr, "usage: twa-bench --dir /path [--rows N] [--load] [--repeat N]\n");
        return 2;
    }
    tsdb_options opts;
    tsdb_options_init(&opts);
    opts.partition_ms = 30 * 86400000LL;
    opts.fsync = 0;
    opts.merge_idle_s = -1;
    tsdb *db;
    int rc = tsdb_open(dir, &opts, &db);
    if (rc)
        fail(dir, rc);
    tsdb_sid s;
    if (do_load) {
        if (tsdb_series_lookup(db, "twa", &s) == 0) {
            fprintf(stderr, "twa-bench: %s already holds the series; load into an empty directory\n", dir);
            return 1;
        }
        load(db, rows);
    }
    if ((rc = tsdb_series_lookup(db, "twa", &s)))
        fail("lookup", rc);
    static const struct {
        const char *name;
        int64_t ms;
    } groups[] = {{"hour", 3600000}, {"day", 86400000}};
    printf("query\tmethod\tpath\tseconds\trows\tgranules\tfrom_stats\trows_decoded\tMrows/s\tresult\n");
    for (size_t gi = 0; gi < 2; gi++) {
        for (int m = 0; m < 2; m++)
            for (int d = 0; d < 2; d++) {
                double res;
                tsdb_query_stats st;
                double t = best_twa(db, s, groups[gi].ms, m, d, repeat, &res, &st);
                printf("twa/%s\t%s\t%s\t%.4f\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%.0f\t%.9g\n",
                       groups[gi].name, m ? "linear" : "step", d ? "decode" : "stats", t, st.rows, st.granules,
                       st.granules_from_stats, st.rows_decoded, (double)st.rows / t / 1e6, res);
            }
        double best = 1e30;
        tsdb_query_stats st;
        double avg = 0;
        for (int r = 0; r < repeat; r++) {
            tsdb_bucket *b;
            size_t n;
            double t0 = now_s();
            if ((rc = tsdb_query_agg(db, s, INT64_MIN, INT64_MAX, groups[gi].ms, &b, &n)))
                fail("agg", rc);
            double t = now_s() - t0;
            tsdb_last_query_stats(&st);
            double sum = 0, cnt = 0;
            for (size_t i = 0; i < n; i++)
                sum += b[i].sum, cnt += (double)b[i].count;
            avg = sum / cnt;
            free(b);
            best = t < best ? t : best;
        }
        printf("avg/%s\t-\tstats\t%.4f\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%.0f\t%.9g\n",
               groups[gi].name, best, st.rows, st.granules, st.granules_from_stats, st.rows_decoded,
               (double)st.rows / best / 1e6, avg);
    }
    tsdb_close(db);
    return 0;
}
