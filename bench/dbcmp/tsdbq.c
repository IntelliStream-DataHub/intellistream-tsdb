/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd harness over the C client, on the same query spec as the other systems.
 *   tsdbq qps  SPEC CLASS THREADS SECONDS     random windows / point lookups of CLASS, like qdrive
 *   tsdbq dump SPEC N                         reference answers of the first N queries of each class
 *                                             (JSON lines on stdout: ref/windows.jsonl for check.py)
 *   tsdbq full CONNS REPS [dump] [--batch K]  whole range: count/sum/min/max and buckets of 3600 / 60
 *                                             samples, series spread over CONNS connections; mean, sd,
 *                                             median, min and max of REPS after a warm-up; with dump,
 *                                             per-series digests on stderr (ref/full.jsonl)
 *   tsdbq twa CONNS REPS [OUT] [--batch K]    STEP time-weighted average by hour over the whole range;
 *                                             OUT gets the buckets (ref/twa.jsonl, relq's format)
 * --batch K sends a connection's series K at a time as one QUERY_MULTI with per-series intervals (K = 1:
 * one plain request per series, as without it), so --batch 100 on 1 connection is one request. A qps
 * CLASS of the form NAME.bK (raw.b10) runs NAME's windows K at a time as one QUERY_MULTI with
 * per-series starts and ends; q/s then counts windows, and the latencies are of whole requests.
 * tsdbd is at TSDB_HOST:TSDB_PORT (127.0.0.1:7070). The spec and the RowBinary file number the series;
 * their ids (which carry the volume in their high bits) are looked up by the names tsdbc-bench load gave them.
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
static size_t batch = 1; /* --batch K, or K of a qps class NAME.bK */
#define MAXSERIES 1024
static const char *KIND[7] = {"hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate", "flow_total"};
static tsdb_sid sids[MAXSERIES]; /* series number -> id */

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
        if (x.s >= MAXSERIES) { fprintf(stderr, "%s: series %u, past %d\n", path, x.s, MAXSERIES); exit(2); }
        if (nq == cap) qs = realloc(qs, (cap = cap ? cap * 2 : 1024) * sizeof *qs);
        qs[nq++] = x;
    }
    fclose(f);
}

/* sids[0 .. n) by name */
static void resolve(uint32_t n)
{
    tsdbc *c = conn();
    for (uint32_t s = 0; s < n; s++) {
        char name[64];
        snprintf(name, sizeof name, "%s.%u", KIND[s % 7], s);
        int rc = tsdbc_series(c, name, 0, &sids[s]);
        if (rc) { fprintf(stderr, "series %s: %s\n", name, tsdbc_strerror(rc)); exit(1); }
    }
    tsdbc_close(c);
}

static void resolve_queries(void)
{
    uint32_t n = 0;
    for (size_t i = 0; i < nq; i++)
        if (qs[i].s >= n) n = qs[i].s + 1;
    resolve(n);
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
    if (x->bucket == 0) return tsdbc_query_raw(c, sids[x->s], x->a, x->b, raw_cb, r);
    return tsdbc_query_agg(c, sids[x->s], x->a, x->b, x->bucket, b, nb);
}

static void die_rc(const char *what, int rc)
{
    fprintf(stderr, "%s: %s\n", what, tsdbc_strerror(rc));
    exit(1);
}
static int count_fn(void *ctx, const tsdb_series_result *r)
{
    if (r->status) die_rc("query multi, series", r->status);
    *(size_t *)ctx += r->n;
    return 0;
}
/* qs[at], qs[at + 1], ... (k of them, wrapping) as one QUERY_MULTI */
static void run_batch(tsdbc *c, size_t at, size_t k)
{
    tsdb_sid ss[k];
    int64_t st[k], en[k], iv[k];
    for (size_t i = 0; i < k; i++) {
        const q *x = &qs[(at + i) % nq];
        ss[i] = sids[x->s]; st[i] = x->a; en[i] = x->b; iv[i] = x->bucket;
    }
    int agg = qs[at % nq].bucket != 0;
    tsdb_multi_query m = {.kind = agg ? TSDB_MULTI_BUCKETS : TSDB_MULTI_VALUES, .starts = st, .ends = en,
                          .intervals = agg ? iv : NULL};
    size_t rows = 0;
    int rc = tsdbc_query_multi(c, &m, ss, k, NULL, count_fn, &rows);
    if (rc) die_rc("query multi", rc);
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
        if (batch > 1) {
            run_batch(c, i * batch, batch);
        } else {
            int rc = run_one(c, &qs[i % nq], &r, &b, &nb);
            if (rc) { fprintf(stderr, "query: %s\n", tsdbc_strerror(rc)); exit(1); }
            free(b);
        }
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
typedef struct fjob { int id, conns, twa; int64_t nsamp; /* 0: one bucket */ double digest[MAXSERIES][5]; } fjob;
static tsdb_twa_bucket *twa_out[MAXSERIES];
static size_t twa_n[MAXSERIES];
static void full_digest(fjob *j, uint32_t s, const tsdb_bucket *b, size_t nb)
{
    double *d = j->digest[s];
    memset(d, 0, 5 * sizeof *d);
    if (j->nsamp == 0 && nb) {
        d[0] = (double)b[0].count; d[1] = b[0].sum; d[2] = b[0].min; d[3] = b[0].max; d[4] = 1;
    } else
        for (size_t i = 0; i < nb; i++) { d[0] += b[i].count; d[1] += b[i].sum; d[2] += b[i].min; d[3] += b[i].max; d[4] += 1; }
}
static void full_keep(fjob *j, uint32_t s, void *b, size_t nb)
{
    if (j->twa) {
        free(twa_out[s]);
        twa_out[s] = b;
        twa_n[s] = nb;
        return;
    }
    full_digest(j, s, b, nb);
    free(b);
}
static int64_t full_interval(const fjob *j, uint32_t s) { return j->twa ? 3600000 : j->nsamp * STEP[s % 7]; }
static void *full_run(void *arg)
{
    fjob *j = arg;
    tsdbc *c = conn();
    tsdb_twa_opts to = {.method = TSDB_TWA_STEP};
    tsdb_sid ss[MAXSERIES];
    uint32_t sn[MAXSERIES]; /* the series numbers of ss[] */
    int64_t iv[MAXSERIES];
    size_t k = 0;
    for (uint32_t s = (uint32_t)j->id; s < nseries; s += (uint32_t)j->conns) {
        if (batch > 1) {
            ss[k] = sids[s];
            sn[k] = s;
            iv[k++] = full_interval(j, s);
            continue;
        }
        void *b;
        size_t nb;
        int rc = j->twa ? tsdbc_query_twa(c, sids[s], INT64_MIN, INT64_MAX, 3600000, &to, (tsdb_twa_bucket **)&b, &nb)
                        : tsdbc_query_agg(c, sids[s], INT64_MIN, INT64_MAX, full_interval(j, s), (tsdb_bucket **)&b, &nb);
        if (rc) die_rc(j->twa ? "twa" : "agg", rc);
        full_keep(j, s, b, nb);
    }
    for (size_t at = 0; at < k; at += batch) {
        size_t m = k - at < batch ? k - at : batch;
        tsdb_multi_query mq = {.kind = j->twa ? TSDB_MULTI_TWA : TSDB_MULTI_BUCKETS, .start = INT64_MIN,
                               .end = INT64_MAX, .intervals = iv + at, .twa = to};
        tsdb_series_result *r;
        int rc = tsdbc_query_multi_collect(c, &mq, ss + at, m, NULL, &r);
        if (rc) die_rc("multi", rc);
        for (size_t i = 0; i < m; i++) {
            if (r[i].status) die_rc("multi, series", r[i].status);
            size_t row = j->twa ? sizeof(tsdb_twa_bucket) : sizeof(tsdb_bucket);
            void *b = malloc(r[i].n * row + 1);
            if (!b) die_rc("multi", TSDB_ERR_NOMEM);
            if (r[i].n) memcpy(b, r[i].buckets, r[i].n * row);
            full_keep(j, sn[at + i], b, r[i].n);
        }
        tsdbc_series_results_free(r, m);
    }
    tsdbc_close(c);
    return NULL;
}
/* REPS timed runs of one class after a warm-up; prints mean, sd, median, min and max */
static void full_time(const char *name, int conns, int reps, int twa, int64_t nsamp, fjob *jobs)
{
    double t[101], sum = 0, ss = 0;
    for (int r = 0; r <= reps; r++) {
        pthread_t th[256];
        double a = now();
        for (int i = 0; i < conns; i++) {
            jobs[i].id = i; jobs[i].conns = conns; jobs[i].nsamp = nsamp; jobs[i].twa = twa;
            pthread_create(&th[i], NULL, full_run, &jobs[i]);
        }
        for (int i = 0; i < conns; i++) pthread_join(th[i], NULL);
        if (r) t[r - 1] = now() - a; /* run 0 is the warm-up */
    }
    for (int r = 0; r < reps; r++) sum += t[r];
    double mean = sum / reps;
    for (int r = 0; r < reps; r++) ss += (t[r] - mean) * (t[r] - mean);
    qsort(t, (size_t)reps, sizeof *t, cmpd);
    printf("%s %-30s %3d conns batch %zu: mean %.4f s sd %.4f, median %.4f (min %.4f, max %.4f, %d runs)\n",
           twa ? "twa " : "full", name, conns, batch, mean, reps > 1 ? sqrt(ss / (reps - 1)) : 0.0, t[reps / 2], t[0],
           t[reps - 1], reps);
}
/* trailing --batch K */
static int take_batch(int *argc, char **argv)
{
    if (*argc >= 2 && !strcmp(argv[*argc - 2], "--batch")) {
        long k = atol(argv[*argc - 1]);
        if (k < 1 || k > MAXSERIES) { fprintf(stderr, "--batch: 1 to %d\n", MAXSERIES); return -1; }
        batch = (size_t)k;
        *argc -= 2;
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (take_batch(&argc, argv)) return 2;
    if (argc >= 6 && !strcmp(argv[1], "qps")) {
        char base[16];
        snprintf(base, sizeof base, "%s", argv[3]);
        char *dot = strstr(base, ".b");
        if (dot) { batch = (size_t)atol(dot + 2); *dot = 0; }
        if (batch < 1 || batch > 65536) { fprintf(stderr, "class NAME.bK: K from 1 to 65536\n"); return 2; }
        load(argv[2], base);
        if (!nq) { fprintf(stderr, "no queries of class %s\n", base); return 2; }
        resolve_queries();
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
        printf("%s c=%d: %zu queries in %.2f s = %.1f q/s, median %.3f ms, p99 %.3f ms%s\n", argv[3], threads,
               total * batch, secs, total * batch / secs, all[m / 2] * 1e3, all[(size_t)(m * 0.99)] * 1e3,
               batch > 1 ? " per request" : "");
        return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "dump")) {
        load(argv[2], NULL);
        resolve_queries();
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
        resolve(nseries);
        int64_t sizes[3] = {0, 3600, 60};
        const char *names[3] = {"count/sum/min/max per series", "buckets of 3600 samples", "buckets of 60 samples"};
        for (int k = 0; k < 3; k++) {
            fjob *jobs = calloc((size_t)conns, sizeof *jobs);
            full_time(names[k], conns, reps, 0, sizes[k], jobs);
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
    if (argc >= 4 && !strcmp(argv[1], "twa")) {
        int conns = atoi(argv[2]), reps = atoi(argv[3]);
        if (getenv("NSERIES")) nseries = (uint32_t)atoi(getenv("NSERIES"));
        if (nseries < 1 || nseries > MAXSERIES || conns < 1 || conns > 256 || reps < 1 || reps > 100) return 2;
        resolve(nseries);
        fjob *jobs = calloc((size_t)conns, sizeof *jobs);
        full_time("STEP by hour", conns, reps, 1, 0, jobs);
        size_t tot = 0;
        for (uint32_t s = 0; s < nseries; s++) tot += twa_n[s];
        printf("twa: %zu buckets\n", tot);
        if (argc > 4) {
            FILE *f = fopen(argv[4], "w");
            if (!f) { perror(argv[4]); return 1; }
            for (uint32_t s = 0; s < nseries; s++) {
                fprintf(f, "{\"s\":%u,\"b\":[", s);
                for (size_t i = 0; i < twa_n[s]; i++)
                    fprintf(f, "%s[%ld,%.17g,%ld,%lu]", i ? "," : "", (long)twa_out[s][i].start, twa_out[s][i].area,
                            (long)twa_out[s][i].covered_ms, (unsigned long)twa_out[s][i].count);
                fprintf(f, "]}\n");
            }
            fclose(f);
        }
        free(jobs);
        return 0;
    }
    fprintf(stderr, "usage: see source\n");
    return 2;
}
