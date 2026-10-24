/*
 * tsdbd harness over the C client, on the same query spec as the other systems.
 *   tsdbq qps  SPEC CLASS THREADS SECONDS     random windows / point lookups of CLASS, like qdrive
 *   tsdbq dump SPEC N                         reference answers of the first N queries of each class
 *                                             (JSON lines on stdout: ref/windows.jsonl for check.py)
 *   tsdbq full CONNS REPS [dump]              whole range: count/sum/min/max and buckets of 3600 / 60
 *                                             samples, series spread over CONNS connections; median of
 *                                             REPS after a warm-up; with dump, per-series digests on
 *                                             stderr (ref/full.jsonl)
 * tsdbd is at TSDB_HOST:TSDB_PORT (127.0.0.1:7070). Series ids are taken to equal the series
 * numbers of the RowBinary file, which holds when tsdbc-bench load created them in a fresh database.
 * NSERIES (100) is the number of series for the whole-range queries, as for spec.py.
 */
#include "tsdb_client.h"
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static const int64_t STEP[7] = {60000, 1000, 10000, 10000, 1, 1000, 1000};
typedef struct q { char cls[8]; uint32_t s; int64_t a, b, bucket; } q;
static q *qs;
static size_t nq;
static int threads;
static double seconds;
static uint32_t nseries = 100;
#define MAXSERIES 1024

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}
static tsdbc *conn(void)
{
    tsdbc *c;
    const char *h = getenv("TSDB_HOST"), *p = getenv("TSDB_PORT");
    int rc = tsdbc_connect(h ? h : "127.0.0.1", p ? atoi(p) : 7070, &c);
    if (rc) { fprintf(stderr, "connect: %s\n", tsdbc_strerror(rc)); exit(1); }
    return c;
}
static void load(const char *path, const char *cls)
{
    FILE *f = fopen(path, "r");
    q x;
    size_t cap = 0;
    while (fscanf(f, "%7s %u %ld %ld %ld", x.cls, &x.s, &x.a, &x.b, &x.bucket) == 5) {
        if (cls && strcmp(cls, x.cls)) continue;
        if (nq == cap) qs = realloc(qs, (cap = cap ? cap * 2 : 1024) * sizeof *qs);
        qs[nq++] = x;
    }
    fclose(f);
}

typedef struct rawacc { uint64_t n; double sum; int64_t first, last; float v; } rawacc;
static int raw_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    rawacc *r = ctx;
    if (!r->n && n) r->first = ts[0];
    for (size_t i = 0; i < n; i++) r->sum += v[i];
    if (n) { r->last = ts[n - 1]; r->v = v[n - 1]; }
    r->n += n;
    return 0;
}
static int run_one(tsdbc *c, const q *x, rawacc *r, tsdb_bucket **b, size_t *nb)
{
    memset(r, 0, sizeof *r);
    *b = NULL;
    *nb = 0;
    if (x->bucket == 0) return tsdbc_query_raw(c, x->s, x->a, x->b, raw_cb, r);
    return tsdbc_query_agg(c, x->s, x->a, x->b, x->bucket, b, nb);
}

typedef struct job { int id; double *lat; size_t n; double secs; } job;
static void *qps_run(void *arg)
{
    job *j = arg;
    tsdbc *c = conn();
    size_t cap = 0;
    double t0 = now();
    for (size_t i = (size_t)j->id; now() - t0 < seconds; i += (size_t)threads) {
        rawacc r;
        tsdb_bucket *b;
        size_t nb;
        double a = now();
        int rc = run_one(c, &qs[i % nq], &r, &b, &nb);
        if (rc) { fprintf(stderr, "query: %s\n", tsdbc_strerror(rc)); exit(1); }
        free(b);
        if (j->n == cap) j->lat = realloc(j->lat, (cap = cap ? cap * 2 : 4096) * sizeof(double));
        j->lat[j->n++] = now() - a;
    }
    j->secs = now() - t0;
    tsdbc_close(c);
    return NULL;
}
static int cmpd(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

/* ---- whole range ---- */
typedef struct fjob { int id, conns; int64_t nsamp; /* 0: one bucket */ double digest[MAXSERIES][5]; } fjob;
static void *full_run(void *arg)
{
    fjob *j = arg;
    tsdbc *c = conn();
    for (uint32_t s = (uint32_t)j->id; s < nseries; s += (uint32_t)j->conns) {
        tsdb_bucket *b;
        size_t nb;
        int rc = tsdbc_query_agg(c, s, INT64_MIN, INT64_MAX, j->nsamp * STEP[s % 7], &b, &nb);
        if (rc) { fprintf(stderr, "agg: %s\n", tsdbc_strerror(rc)); exit(1); }
        double *d = j->digest[s];
        memset(d, 0, 5 * sizeof *d);
        if (j->nsamp == 0) {
            d[0] = (double)b[0].count; d[1] = b[0].sum; d[2] = b[0].min; d[3] = b[0].max; d[4] = 1;
        } else
            for (size_t i = 0; i < nb; i++) { d[0] += b[i].count; d[1] += b[i].sum; d[2] += b[i].min; d[3] += b[i].max; d[4] += 1; }
        free(b);
    }
    tsdbc_close(c);
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc >= 6 && !strcmp(argv[1], "qps")) {
        load(argv[2], argv[3]);
        threads = atoi(argv[4]);
        seconds = atof(argv[5]);
        job *jobs = calloc((size_t)threads, sizeof *jobs);
        pthread_t th[512];
        for (int i = 0; i < threads; i++) { jobs[i].id = i; pthread_create(&th[i], NULL, qps_run, &jobs[i]); }
        size_t total = 0;
        double secs = 0;
        for (int i = 0; i < threads; i++) {
            pthread_join(th[i], NULL);
            total += jobs[i].n;
            secs = fmax(secs, jobs[i].secs);
        }
        double *all = malloc(total * sizeof *all);
        size_t m = 0;
        for (int i = 0; i < threads; i++) for (size_t k = 0; k < jobs[i].n; k++) all[m++] = jobs[i].lat[k];
        qsort(all, m, sizeof *all, cmpd);
        printf("%s c=%d: %zu queries in %.2f s = %.1f q/s, median %.3f ms, p99 %.3f ms\n", argv[3], threads, total, secs,
               total / secs, all[m / 2] * 1e3, all[(size_t)(m * 0.99)] * 1e3);
        return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "dump")) {
        load(argv[2], NULL);
        size_t per = strtoull(argv[3], NULL, 10);
        tsdbc *c = conn();
        const char *seen = "";
        size_t k = 0;
        for (size_t i = 0; i < nq; i++) {
            if (strcmp(seen, qs[i].cls)) { seen = qs[i].cls; k = 0; }
            if (k++ >= per) continue;
            rawacc r;
            tsdb_bucket *b;
            size_t nb;
            int rc = run_one(c, &qs[i], &r, &b, &nb);
            if (rc) { fprintf(stderr, "query: %s\n", tsdbc_strerror(rc)); return 1; }
            printf("{\"i\":%zu,\"cls\":\"%s\"", i, qs[i].cls);
            if (qs[i].bucket == 0)
                printf(",\"n\":%lu,\"sum\":%.17g,\"first\":%ld,\"last\":%ld,\"v\":%.9g}\n", r.n, r.sum, r.first, r.last, r.v);
            else {
                printf(",\"b\":[");
                for (size_t x = 0; x < nb; x++)
                    printf("%s[%ld,%lu,%.17g,%.9g,%.9g]", x ? "," : "", b[x].start, b[x].count, b[x].sum, b[x].min, b[x].max);
                printf("]}\n");
            }
            free(b);
        }
        tsdbc_close(c);
        return 0;
    }
    if (argc >= 4 && !strcmp(argv[1], "full")) {
        int conns = atoi(argv[2]), reps = atoi(argv[3]);
        int dump = argc > 4;
        if (getenv("NSERIES")) nseries = (uint32_t)atoi(getenv("NSERIES"));
        if (nseries < 1 || nseries > MAXSERIES) { fprintf(stderr, "NSERIES: 1 to %d\n", MAXSERIES); return 2; }
        int64_t sizes[3] = {0, 3600, 60};
        const char *names[3] = {"count/sum/min/max per series", "buckets of 3600 samples", "buckets of 60 samples"};
        for (int k = 0; k < 3; k++) {
            double t[64];
            fjob *jobs = calloc((size_t)conns, sizeof *jobs);
            for (int r = 0; r <= reps; r++) {
                pthread_t th[256];
                double a = now();
                for (int i = 0; i < conns; i++) {
                    jobs[i].id = i; jobs[i].conns = conns; jobs[i].nsamp = sizes[k];
                    pthread_create(&th[i], NULL, full_run, &jobs[i]);
                }
                for (int i = 0; i < conns; i++) pthread_join(th[i], NULL);
                if (r) t[r - 1] = now() - a; /* run 0 is the warm-up */
            }
            qsort(t, (size_t)reps, sizeof *t, cmpd);
            printf("full %-30s %3d conns: median %.4f s (min %.4f, max %.4f, %d runs)\n", names[k], conns, t[reps / 2],
                   t[0], t[reps - 1], reps);
            if (dump)
                for (uint32_t s = 0; s < nseries; s++) {
                    double *d = jobs[s % conns].digest[s];
                    fprintf(stderr, "{\"full\":%ld,\"s\":%u,\"d\":[%.17g,%.17g,%.17g,%.17g,%.17g]}\n", sizes[k], s, d[0],
                            d[1], d[2], d[3], d[4]);
                }
            free(jobs);
        }
        return 0;
    }
    fprintf(stderr, "usage: see source\n");
    return 2;
}
