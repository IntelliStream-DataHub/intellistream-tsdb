/*
 * How many threads a query should use for the rows it decodes: the sweep behind QUERY_ROWS_PER_THREAD
 * (src/internal.h, query_threads_for).
 *
 *   threads-bench --dir D [--granules 1024] [--threads N] [--runs 5] [--ms 100] [--per 8192,16384,...]
 *
 * One float32 series of --granules x 8192 rows at 1 s (a slow sensor wave with noise, at 0.01), merged into
 * one part per day. For windows of 1 granule to all of them, and two query kinds that decode every granule
 * (count/sum/min/max and the STEP TWA, both in 7-minute buckets, which cut through the 8192-second granules),
 * it times the query with query_rows_per_thread at each --per value ("one": a single thread; "all": one thread
 * per granule up to N), each configuration --ms long per run, the configurations interleaved, --runs times. It
 * prints the mean and sd per query in microseconds and each configuration's time over one thread's, then the
 * smallest --per value at which no window is slower than one thread by more than max(2 %, 2 sd).
 */
#include "../src/internal.h"
#include "common.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define GR 8192
#define BUCKET_MS 420000
#define MAXC 16
#define MAXS 32

static tsdb *db;
static tsdb_sid sid;
static int kind; /* 0 agg, 1 twa */

static void die(const char *what, int rc)
{
    fprintf(stderr, "threads-bench: %s: %s\n", what, tsdb_strerror(rc));
    exit(1);
}

static void load(size_t granules)
{
    size_t n = granules * GR;
    int rc = tsdb_series_lookup(db, "sweep", &sid);
    if (rc == 0)
        return; /* loaded by an earlier run */
    if ((rc = tsdb_series_create(db, "sweep", TSDB_FLOAT32, NULL, &sid)))
        die("create", rc);
    const size_t chunk = 1 << 20;
    int64_t *ts = malloc(chunk * sizeof *ts);
    float *v = malloc(chunk * sizeof *v);
    uint64_t x = 88172645463325252ull;
    for (size_t off = 0; off < n; off += chunk) {
        size_t m = n - off < chunk ? n - off : chunk;
        for (size_t i = 0; i < m; i++) {
            size_t k = off + i;
            x ^= x << 13, x ^= x >> 7, x ^= x << 17;
            double w = 40 + 10 * sin((double)k / 3600.0) + (double)(x % 200) / 100.0;
            ts[i] = (int64_t)k * 1000;
            v[i] = (float)(round(w * 100) / 100);
        }
        if ((rc = tsdb_insert_series(db, sid, ts, v, m)))
            die("insert", rc);
    }
    free(ts);
    free(v);
    if ((rc = tsdb_flush(db)) || (rc = tsdb_optimize(db)))
        die("optimize", rc);
}

static void query(size_t granules)
{
    int64_t end = (int64_t)(granules * GR) * 1000;
    int rc;
    if (kind == 0) {
        tsdb_bucket *b;
        size_t nb;
        rc = tsdb_query_agg(db, sid, 0, end, BUCKET_MS, &b, &nb);
        free(b);
    } else {
        tsdb_twa_opts o = {.method = TSDB_TWA_STEP};
        tsdb_twa_bucket *b;
        size_t nb;
        rc = tsdb_query_twa(db, sid, 0, end, BUCKET_MS, &o, &b, &nb);
        free(b);
    }
    if (rc)
        die("query", rc);
}

/* microseconds per query over ms milliseconds */
static double timed(size_t granules, int ms)
{
    query(granules);
    size_t k = 0;
    double a = now_s(), t;
    do {
        query(granules);
        k++;
    } while ((t = now_s() - a) * 1e3 < ms);
    return t / (double)k * 1e6;
}

int main(int argc, char **argv)
{
    const char *dir = NULL;
    size_t granules = 1024;
    int threads = 0, runs = 5, ms = 100;
    uint64_t per[MAXC] = {UINT64_MAX, 8192, 16384, 32768, 65536, 131072, 262144, 1};
    int nper = 8;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--dir") && i + 1 < argc)
            dir = argv[++i];
        else if (!strcmp(argv[i], "--granules") && i + 1 < argc)
            granules = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--threads") && i + 1 < argc)
            threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--runs") && i + 1 < argc)
            runs = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--ms") && i + 1 < argc)
            ms = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--per") && i + 1 < argc) {
            nper = 1;
            for (char *p = strtok(argv[++i], ","); p && nper < MAXC; p = strtok(NULL, ","))
                per[nper++] = strtoull(p, NULL, 10);
        } else {
            fprintf(stderr, "usage: threads-bench --dir D [--granules N] [--threads N] [--runs N] [--ms N] "
                            "[--per A,B,...]\n");
            return 2;
        }
    }
    if (!dir || granules < 1 || granules > 65536 || runs < 2 || runs > 100 || ms < 1) {
        fprintf(stderr, "threads-bench: --dir is required; --granules 1 to 65536, --runs 2 to 100\n");
        return 2;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    if (threads > 0)
        o.query_threads = threads;
    int rc = tsdb_open(dir, &o, &db);
    if (rc)
        die(dir, rc);
    load(granules);
    printf("threads-bench: %zu granules of %d rows, query_threads %d, %d runs of %d ms per cell\n", granules, GR,
           o.query_threads, runs, ms);

    size_t sizes[MAXS], ns = 0;
    static const size_t want[] = {1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 128, 256, 512, 1024, 4096, 16384, 65536};
    for (size_t i = 0; i < sizeof want / sizeof *want && want[i] <= granules; i++)
        sizes[ns++] = want[i];
    if (sizes[ns - 1] != granules)
        sizes[ns++] = granules;

    uint64_t ok_from[2] = {0, 0}; /* per kind: the smallest --per with no window slower */
    for (kind = 0; kind < 2; kind++) {
        printf("\n%s, us per query (mean sd), ratio to one thread\n%9s", kind ? "twa STEP 7m" : "agg 7m", "granules");
        for (int c = 0; c < nper; c++) {
            char h[24];
            if (per[c] == UINT64_MAX)
                snprintf(h, sizeof h, "one");
            else if (per[c] == 1)
                snprintf(h, sizeof h, "all");
            else
                snprintf(h, sizeof h, "%" PRIu64, per[c]);
            printf(" %22s", h);
        }
        printf("\n");
        bool slower[MAXC] = {0};
        for (size_t si = 0; si < ns; si++) {
            double t[MAXC][100];
            for (int r = 0; r < runs; r++)
                for (int c = 0; c < nper; c++) {
                    query_rows_per_thread = per[c];
                    t[c][r] = timed(sizes[si], ms);
                }
            double mean[MAXC], sd[MAXC];
            for (int c = 0; c < nper; c++) {
                double s = 0, q = 0;
                for (int r = 0; r < runs; r++)
                    s += t[c][r];
                mean[c] = s / runs;
                for (int r = 0; r < runs; r++)
                    q += (t[c][r] - mean[c]) * (t[c][r] - mean[c]);
                sd[c] = sqrt(q / (runs - 1));
            }
            printf("%9zu", sizes[si]);
            for (int c = 0; c < nper; c++) {
                double tol = fmax(0.02 * mean[0], 2 * fmax(sd[0], sd[c]));
                if (mean[c] > mean[0] + tol)
                    slower[c] = true;
                printf(" %9.1f %5.1f %5.2f%s", mean[c], sd[c], mean[c] / mean[0], mean[c] > mean[0] + tol ? "!" : " ");
            }
            printf("\n");
            fflush(stdout);
        }
        for (int c = 1; c < nper; c++)
            if (!slower[c] && per[c] != 1 && (!ok_from[kind] || per[c] < ok_from[kind]))
                ok_from[kind] = per[c];
        printf("(! marks a configuration slower than one thread beyond max(2 %%, 2 sd))\n");
    }
    printf("\nsmallest rows per thread with no window slower: agg %" PRIu64 ", twa %" PRIu64 " (0: none)\n", ok_from[0],
           ok_from[1]);
    tsdb_close(db);
    return 0;
}
