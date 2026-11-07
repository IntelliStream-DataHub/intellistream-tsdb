/* latest-bench: what the latest table costs a flush, and what lookups cost it (design-latest.md 2.3, 9), embedded.
 *
 *   latest-bench --dir D [--mode on|off|nowin] [--series 1000000] [--rounds 6] [--readers 1] [--ids 256]
 *                [--with-nulls] [--null-tenth]
 *
 * Every round writes one row for each of --series series (a tenth of the rows null with --null-tenth), then flushes
 * (tsdb_flush: the publish) while --readers threads look up --ids random ids each call (--with-nulls forces the slow
 * path). Per round it prints the flush time, the lookups that completed during the flush (count, p50, p99, max) and,
 * from a writer thread appending one row per call during the flush, the p50 / p99 / max of those appends.
 * Modes: on = default table with the 3.2 window, nowin = table without the window, off = latest_cache 0 (slow path).
 * The first round is a warm-up (printed, to be left out by the caller). Not part of `make`: build/latest-bench. */
#include "internal.h"
#include "tsdb.h"
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAXLAT (1 << 22)
static tsdb *db;
static tsdb_sid *ids;
static long N = 1000000;
static int nids = 256;
static bool with_nulls;
static atomic_int run;
static atomic_int cur_round;

typedef struct lat {
    double *v;
    atomic_long n;
} lat;

static double now_us(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e6 + t.tv_nsec / 1e3;
}

static void lat_add(lat *l, double d)
{
    long k = atomic_fetch_add(&l->n, 1);
    if (k < MAXLAT)
        l->v[k] = d;
}

static void *reader(void *arg)
{
    lat *l = arg;
    uint64_t r = 88172645463325252ull ^ (uint64_t)(uintptr_t)l;
    tsdb_sid *q = malloc((size_t)nids * sizeof *q);
    tsdb_latest_result *out = malloc((size_t)nids * sizeof *out);
    tsdb_latest_opts op = {.end = TSDB_TS_MAX, .with_nulls = with_nulls, .threads = 1};
    while (atomic_load(&run)) {
        for (int i = 0; i < nids; i++) {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            q[i] = ids[r % (uint64_t)N];
        }
        double t0 = now_us();
        if (tsdb_latest(db, q, (size_t)nids, &op, out))
            break;
        lat_add(l, now_us() - t0);
    }
    free(q);
    free(out);
    return NULL;
}

static void *appender(void *arg)
{
    lat *l = arg;
    uint64_t r = 1442695040888963407ull;
    int64_t k = 0;
    while (atomic_load(&run)) {
        r = r * 6364136223846793005ull + 1;
        double v = 1.5;
        tsdb_values vals = {TSDB_FLOAT64, &v, NULL};
        int64_t ts = (int64_t)atomic_load(&cur_round) * 1000 + 2 + k++ % 900; /* between this round's rows and the next's */
        double t0 = now_us();
        if (tsdb_insert_series_values(db, ids[r % (uint64_t)N], &ts, &vals, 1))
            break;
        lat_add(l, now_us() - t0);
        struct timespec d = {0, 200000};
        nanosleep(&d, NULL);
    }
    return NULL;
}

static int cmpd(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* the entries [from, to) of l as "n p50 p99 max" */
static void report(const char *what, lat *l, long from, long to)
{
    long n = to - from;
    if (n <= 0) {
        printf(" %s: none", what);
        return;
    }
    double *x = malloc((size_t)n * sizeof *x);
    memcpy(x, l->v + from, (size_t)n * sizeof *x);
    qsort(x, (size_t)n, sizeof *x, cmpd);
    printf(" %s: n=%ld p50=%.0fus p99=%.0fus max=%.0fus", what, n, x[n / 2], x[(long)((double)n * 0.99)], x[n - 1]);
    free(x);
}

int main(int argc, char **argv)
{
    const char *dir = NULL, *mode = "on";
    int rounds = 6, readers = 1;
    bool null_tenth = false;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--dir") && i + 1 < argc)
            dir = argv[++i];
        else if (!strcmp(argv[i], "--mode") && i + 1 < argc)
            mode = argv[++i];
        else if (!strcmp(argv[i], "--series") && i + 1 < argc)
            N = atol(argv[++i]);
        else if (!strcmp(argv[i], "--rounds") && i + 1 < argc)
            rounds = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--readers") && i + 1 < argc)
            readers = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--ids") && i + 1 < argc)
            nids = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--with-nulls"))
            with_nulls = true;
        else if (!strcmp(argv[i], "--null-tenth"))
            null_tenth = true;
        else {
            fprintf(stderr, "latest-bench: unknown argument %s\n", argv[i]);
            return 2;
        }
    }
    if (!dir || N < 1 || readers < 0 || nids < 1 || nids > 100000) {
        fprintf(stderr, "usage: latest-bench --dir D [--mode on|off|nowin] [--series N] [--rounds R] [--readers K] "
                        "[--ids M] [--with-nulls] [--null-tenth]\n");
        return 2;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.buffer_rows = 8u << 20;
    o.flush_interval_ms = 3600000;
    if (!strcmp(mode, "off"))
        o.latest_cache = 0;
    if (!strcmp(mode, "nowin"))
        tsdb_test_latest_window(INT_MAX);
    if (tsdb_open(dir, &o, &db)) {
        fprintf(stderr, "latest-bench: open failed\n");
        return 1;
    }
    ids = malloc((size_t)N * sizeof *ids);
    for (long i = 0; i < N; i++) {
        char n[32];
        snprintf(n, sizeof n, "s%ld", i);
        if (tsdb_series_create(db, n, TSDB_FLOAT64, NULL, &ids[i]))
            return 2;
    }
    enum { B = 4096 };
    tsdb_sid sv[B];
    int64_t tv[B];
    double vv[B];
    uint8_t valid[B / 8];
    lat rl = {malloc(MAXLAT * sizeof(double)), 0}, wl = {malloc(MAXLAT * sizeof(double)), 0};
    for (int round = 1; round <= rounds; round++) {
        for (long b = 0; b < N; b += B) {
            int m = (int)(N - b < B ? N - b : B);
            memset(valid, 0xff, sizeof valid);
            for (int k = 0; k < m; k++) {
                sv[k] = ids[b + k];
                tv[k] = (int64_t)round * 1000 + 1;
                vv[k] = round + k * 0.5;
                if (null_tenth && (b + k) % 10 == 0)
                    valid[k >> 3] &= (uint8_t)~(1u << (k & 7));
            }
            tsdb_values vals = {TSDB_FLOAT64, vv, null_tenth ? valid : NULL};
            if (tsdb_insert_values(db, sv, tv, &vals, (size_t)m))
                return 3;
        }
        atomic_store(&cur_round, round);
        atomic_store(&rl.n, 0);
        atomic_store(&wl.n, 0);
        atomic_store(&run, 1);
        pthread_t th[64], wt;
        for (int i = 0; i < readers && i < 64; i++)
            pthread_create(&th[i], NULL, reader, &rl);
        pthread_create(&wt, NULL, appender, &wl);
        struct timespec d = {0, 100000000};
        nanosleep(&d, NULL);
        long rb = atomic_load(&rl.n), wb = atomic_load(&wl.n);
        double t0 = now_us();
        int frc = tsdb_flush(db);
        double t1 = now_us();
        long ra = atomic_load(&rl.n), wa = atomic_load(&wl.n);
        nanosleep(&d, NULL);
        atomic_store(&run, 0);
        for (int i = 0; i < readers && i < 64; i++)
            pthread_join(th[i], NULL);
        pthread_join(wt, NULL);
        printf("%s round %d%s: flush %.1f ms (rc %d);", mode, round, round == 1 ? " (warm-up)" : "", (t1 - t0) / 1e3, frc);
        report("lookups", &rl, rb, ra);
        report("appends", &wl, wb, wa);
        printf("\n");
        fflush(stdout);
    }
    tsdb_close(db);
    return 0;
}
