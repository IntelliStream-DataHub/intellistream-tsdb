/*
 * Release benchmark: LTTB and time-weighted averages over the whole range, every series, for tsdb.
 *   relq lttb CONNS REPS [OUT]   LTTB, 1000 points per series (tsdb_query_lttb)
 *   relq twa  CONNS REPS [OUT]   STEP time-weighted average by hour (tsdb_query_twa, interval 3600000)
 *   relq twad CONNS REPS [OUT]   STEP by day (86400000): every bucket covers whole granules, so it is answered
 *                                from granule statistics
 *   relq twal CONNS REPS [OUT]   LINEAR by hour (the method InfluxDB 2's timeWeightedAvg has)
 *   relq twa1d|twa7d CONNS REPS [OUT]   STEP over random windows: the first TWA_N (1000) agg1d / agg7d windows of
 *                                TWA_SPEC (spec.tsv), each in its own buckets (300 / 3600 samples), the reading
 *                                before the window carried in; one window per request, spread over CONNS threads
 *   ... --batch K                sends the requests K at a time as one QUERY_MULTI (protocol 10: per-series bounds
 *                                and intervals, op QUERY_TWA; TWA kinds only): twa ... 1 REPS --batch 100 answers every
 *                                series in one request, as one SQL query does in the other systems; windows go in
 *                                groups of K consecutive ones (the batches of batches.py), the last group shorter
 *   relq qps KIND.bK THREADS SECONDS   the windows of KIND (twa1d, twa7d) K at a time, for SECONDS on THREADS
 *                                connections, like qdrive: "queries" and q/s count requests (as for every other
 *                                system's batches), each of K windows; only whole groups (1000 / K of them), in
 *                                turn; no warm-up (run.sh runs one) and no answers kept (check them with --batch)
 * Built twice from this file: relq talks to tsdbd (TSDB_HOST:TSDB_PORT, default 127.0.0.1:7070) through the C
 * client, one connection per thread; relemb (-DEMBEDDED, DB=directory in TSDB_DIR) calls the library in-process
 * with one shared handle, like embq. The series are spread over CONNS threads; a warm-up run, then REPS timed
 * runs, each printed, then mean, sd, min and max. OUT gets the answers of the last run, one JSON line per series:
 *   lttb: {"s":S,"t":[...],"v":[...]}      twa*: {"s":S,"b":[[start,area,covered_ms,count],...]}
 * (windows also carry "i", the window's 0-based line in TWA_SPEC, the key of the reference answers)
 * NSERIES (100) series, ids 0..NSERIES-1 as tsdbc-bench load / embq load create them.
 */
#define _GNU_SOURCE
#ifdef EMBEDDED
#include "tsdb.h"
#else
#include "tsdb_client.h"
#endif
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAXSERIES 1024
static uint32_t nseries = 100;
static int conns, mode_twa;
static tsdb_twa_method method = TSDB_TWA_STEP;
typedef struct pts { int64_t *t; double *v; size_t n, cap; } pts;
typedef struct ans { pts p; tsdb_twa_bucket *b; size_t nb; } ans;
typedef struct job { uint32_t s; int64_t a, b, interval; long line; } job; /* one request */
static job *jobs;
static size_t njobs;
static ans *res;
static size_t batch = 1;    /* --batch K, or K of qps KIND.bK */
static double seconds;      /* qps */
typedef struct lat { double *v; size_t n, cap; } lat;
static lat *lats;           /* qps: per thread, the latency of every request */

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static int pts_cb(void *ctx, const int64_t *ts, const tsdb_values *vals, size_t n)
{
    pts *p = ctx;
    if (p->n + n > p->cap) {
        p->cap = (p->n + n) * 2;
        p->t = realloc(p->t, p->cap * sizeof *p->t);
        p->v = realloc(p->v, p->cap * sizeof *p->v);
    }
    for (size_t i = 0; i < n; i++) {
        p->t[p->n] = ts[i];
        p->v[p->n] = vals->type == TSDB_FLOAT32 ? ((const float *)vals->values)[i] : ((const double *)vals->values)[i];
        p->n++;
    }
    return 0;
}

#ifdef EMBEDDED
static tsdb *db;
#define H db
#define LTTB tsdb_query_lttb
#define TWA tsdb_query_twa
#define ERR tsdb_strerror
#define HT tsdb *
#define MULTI(h, q, s, n, out) tsdb_query_multi_collect(h, q, s, n, NULL, out)
#define MFREE tsdb_series_results_free
#else
#define H c
#define LTTB tsdbc_query_lttb
#define TWA tsdbc_query_twa
#define ERR tsdbc_strerror
#define HT tsdbc *
#define MULTI(h, q, s, n, out) tsdbc_query_multi_collect(h, q, s, n, NULL, out)
#define MFREE tsdbc_series_results_free
#endif

/* jobs[at], ..., jobs[at + n - 1] as one QUERY_MULTI of TWA; the buckets into out[0..n) (NULL: not kept) */
static int twa_multi(HT h, size_t at, size_t n, ans *out)
{
    uint32_t ss[n];
    int64_t st[n], en[n], iv[n];
    for (size_t i = 0; i < n; i++) {
        const job *j = &jobs[at + i];
        ss[i] = j->s; st[i] = j->a; en[i] = j->b; iv[i] = j->interval;
    }
    tsdb_multi_query m = {.kind = TSDB_MULTI_TWA, .twa = {.method = method}};
    if (jobs[at].line < 0) { /* whole range: one start, end and interval for all (the series differ only) */
        m.start = INT64_MIN; m.end = INT64_MAX; m.interval_ms = jobs[at].interval;
    } else {
        m.starts = st; m.ends = en; m.intervals = iv;
    }
    tsdb_series_result *r;
    int e = MULTI(h, &m, ss, n, &r);
    if (e) return e;
    for (size_t i = 0; i < n && !e; i++) {
        if (r[i].status) { e = r[i].status; break; }
        if (!out) continue;
        ans *a = &out[i];
        free(a->b);
        a->b = malloc(r[i].n * sizeof *a->b + 1);
        if (!a->b) { e = TSDB_ERR_NOMEM; break; }
        if (r[i].n) memcpy(a->b, r[i].buckets, r[i].n * sizeof *a->b);
        a->nb = r[i].n;
    }
    MFREE(r, n);
    return e;
}

static int cmpd(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static void *run(void *arg)
{
    int id = (int)(intptr_t)arg;
#ifndef EMBEDDED
    tsdbc *c;
    const char *h = getenv("TSDB_HOST"), *p = getenv("TSDB_PORT");
    int rc = tsdbc_connect(h ? h : "127.0.0.1", p ? atoi(p) : 7070, &c);
    if (rc) { fprintf(stderr, "connect: %s\n", tsdbc_strerror(rc)); exit(1); }
#endif
    tsdb_twa_opts o = {.method = method};
    if (seconds > 0) { /* qps: whole groups of batch windows, in turn, until the time is up */
        size_t ng = njobs / batch;
        lat *l = &lats[id];
        double t0 = now();
        for (size_t g = (size_t)id; now() - t0 < seconds; g += (size_t)conns) {
            double a = now();
            int e;
            if (batch > 1) e = twa_multi(H, g % ng * batch, batch, NULL);
            else {
                tsdb_twa_bucket *b = NULL;
                size_t nb;
                const job *j = &jobs[g % ng];
                e = TWA(H, j->s, j->a, j->b, j->interval, &o, &b, &nb);
                free(b);
            }
            if (e) { fprintf(stderr, "request %zu: %s\n", g % ng, ERR(e)); exit(1); }
            if (l->n == l->cap) l->v = realloc(l->v, (l->cap = l->cap ? l->cap * 2 : 4096) * sizeof *l->v);
            l->v[l->n++] = now() - a;
        }
#ifndef EMBEDDED
        tsdbc_close(c);
#endif
        return NULL;
    }
    if (batch > 1) { /* groups of batch consecutive jobs, one request each, spread over the threads */
        for (size_t at = (size_t)id * batch; at < njobs; at += (size_t)conns * batch) {
            size_t n = njobs - at < batch ? njobs - at : batch;
            int e = twa_multi(H, at, n, &res[at]);
            if (e) { fprintf(stderr, "series %u (multi of %zu): %s\n", jobs[at].s, n, ERR(e)); exit(1); }
        }
#ifndef EMBEDDED
        tsdbc_close(c);
#endif
        return NULL;
    }
    for (size_t k = (size_t)id; k < njobs; k += (size_t)conns) {
        ans *a = &res[k];
        job *j = &jobs[k];
        int e;
        if (mode_twa) {
            free(a->b);
            a->b = NULL;
            e = TWA(H, j->s, j->a, j->b, j->interval, &o, &a->b, &a->nb);
        } else {
            a->p.n = 0;
            e = LTTB(H, j->s, j->a, j->b, 1000, pts_cb, &a->p);
        }
        if (e) { fprintf(stderr, "series %u: %s\n", j->s, ERR(e)); exit(1); }
    }
#ifndef EMBEDDED
    tsdbc_close(c);
#endif
    return NULL;
}

int main(int argc, char **argv)
{
    int qps = argc == 5 && !strcmp(argv[1], "qps");
    if (argc >= 3 && !strcmp(argv[argc - 2], "--batch")) {
        batch = (size_t)atol(argv[argc - 1]);
        argc -= 2;
    }
    char kind[16];
    snprintf(kind, sizeof kind, "%s", argc > 1 ? argv[qps ? 2 : 1] : "");
    if (qps) { /* KIND.bK */
        char *dot = strstr(kind, ".b");
        batch = dot ? (size_t)atol(dot + 2) : 0;
        if (dot) *dot = 0;
        seconds = atof(argv[4]);
    }
    const char *m = kind;
    int64_t interval = 3600000;
    const char *wcls = NULL;
    if (!strcmp(m, "twa")) mode_twa = 1;
    else if (!strcmp(m, "twad")) mode_twa = 1, interval = 86400000;
    else if (!strcmp(m, "twal")) mode_twa = 1, method = TSDB_TWA_LINEAR;
    else if (!strcmp(m, "twa1d")) mode_twa = 1, wcls = "agg1d";
    else if (!strcmp(m, "twa7d")) mode_twa = 1, wcls = "agg7d";
    else if (strcmp(m, "lttb")) argc = 0;
    if (argc < 4 || batch < 1 || batch > 65536 || (batch > 1 && !mode_twa) || (qps && (!wcls || seconds <= 0))) {
        fprintf(stderr, "usage: relq lttb|twa|twad|twal|twa1d|twa7d CONNS REPS [OUT] [--batch K]\n"
                        "       relq qps twa1d.bK|twa7d.bK THREADS SECONDS\n");
        return 2;
    }
    conns = atoi(argv[qps ? 3 : 2]);
    int reps = qps ? 1 : atoi(argv[3]);
    if (getenv("NSERIES")) nseries = (uint32_t)atoi(getenv("NSERIES"));
    if (conns < 1 || conns > 256 || reps < 1 || reps > 100 || nseries < 1 || nseries > MAXSERIES) return 2;
    if (wcls) { /* windows of spec.tsv: class series start end bucket */
        const char *sp = getenv("TWA_SPEC") ? getenv("TWA_SPEC") : "spec.tsv";
        size_t want = getenv("TWA_N") ? (size_t)atol(getenv("TWA_N")) : 1000;
        FILE *f = fopen(sp, "r");
        if (!f) { perror(sp); return 1; }
        jobs = calloc(want ? want : 1, sizeof *jobs);
        char cls[32];
        unsigned s;
        long long a, b, bk;
        for (long line = 0; njobs < want && fscanf(f, "%31s %u %lld %lld %lld", cls, &s, &a, &b, &bk) == 5; line++)
            if (!strcmp(cls, wcls)) jobs[njobs++] = (job){s, a, b, bk, line};
        fclose(f);
        if (!njobs) { fprintf(stderr, "no %s windows in %s\n", wcls, sp); return 1; }
    } else {
        jobs = calloc(nseries, sizeof *jobs);
        for (uint32_t s = 0; s < nseries; s++) jobs[njobs++] = (job){s, INT64_MIN, INT64_MAX, interval, -1};
    }
    res = calloc(njobs, sizeof *res);
    if (qps && njobs < batch) { fprintf(stderr, "%zu windows: no group of %zu\n", njobs, batch); return 1; }
#ifdef EMBEDDED
    const char *dir = getenv("TSDB_DIR");
    tsdb_options opt;
    tsdb_options_init(&opt);
    int rc = dir ? tsdb_open(dir, &opt, &db) : -1;
    if (rc) { fprintf(stderr, "open %s: %s\n", dir ? dir : "(TSDB_DIR unset)", dir ? tsdb_strerror(rc) : ""); return 1; }
#endif
    if (qps) {
        pthread_t th[256];
        lats = calloc((size_t)conns, sizeof *lats);
        double a = now();
        for (int i = 0; i < conns; i++) pthread_create(&th[i], NULL, run, (void *)(intptr_t)i);
        for (int i = 0; i < conns; i++) pthread_join(th[i], NULL);
        double secs = now() - a, sum = 0;
        size_t n = 0, k = 0;
        for (int i = 0; i < conns; i++) n += lats[i].n;
        double *all = malloc((n + 1) * sizeof *all);
        for (int i = 0; i < conns; i++)
            for (size_t x = 0; x < lats[i].n; x++) sum += (all[k++] = lats[i].v[x]);
        qsort(all, n, sizeof *all, cmpd);
        /* qdrive's line: queries and q/s are requests, as in every other system's batch classes */
        printf("%s c=%d: %zu queries in %.2f s = %.1f q/s, mean %.3f ms, median %.3f ms, p99 %.3f ms "
               "(requests of %zu windows: %.1f windows/s)\n", argv[2], conns, n, secs, n / secs,
               n ? sum / n * 1e3 : 0.0, n ? all[n / 2] * 1e3 : 0.0, n ? all[(size_t)(n * 0.99)] * 1e3 : 0.0, batch,
               n * batch / secs);
        return 0;
    }
    char bstr[64] = "";
    if (batch > 1) /* the request count says which form this was: 1 for --batch 100 on 1 connection */
        snprintf(bstr, sizeof bstr, ", %zu requests of up to %zu", (njobs + batch - 1) / batch, batch);
    double t[101], sum = 0;
    for (int r = 0; r <= reps; r++) {
        pthread_t th[256];
        double a = now();
        for (int i = 0; i < conns; i++) pthread_create(&th[i], NULL, run, (void *)(intptr_t)i);
        for (int i = 0; i < conns; i++) pthread_join(th[i], NULL);
        t[r] = now() - a;
        if (r) {
            sum += t[r];
            printf("%s %d conns%s run %d: %.4f s\n", argv[1], conns, bstr, r, t[r]);
        }
    }
    double mean = sum / reps, ss = 0, mn = 1e300, mx = 0;
    for (int r = 1; r <= reps; r++) {
        ss += (t[r] - mean) * (t[r] - mean);
        mn = t[r] < mn ? t[r] : mn;
        mx = t[r] > mx ? t[r] : mx;
    }
    size_t tot = 0;
    for (size_t k = 0; k < njobs; k++) tot += mode_twa ? res[k].nb : res[k].p.n;
    printf("%s %d conns%s: mean %.4f s sd %.4f (%.1f%%) min %.4f max %.4f, %d runs after a warm-up; %zu %s\n", argv[1],
           conns, bstr, mean, reps > 1 ? sqrt(ss / (reps - 1)) : 0.0, reps > 1 ? 100 * sqrt(ss / (reps - 1)) / mean : 0.0, mn,
           mx, reps, tot, mode_twa ? "buckets" : "points");
    if (argc > 4) {
        FILE *f = fopen(argv[4], "w");
        if (!f) { perror(argv[4]); return 1; }
        for (size_t k = 0; k < njobs; k++) {
            ans *a = &res[k];
            fprintf(f, "{\"s\":%u,", jobs[k].s);
            if (jobs[k].line >= 0) fprintf(f, "\"i\":%ld,", jobs[k].line);
            if (mode_twa) {
                fprintf(f, "\"b\":[");
                for (size_t i = 0; i < a->nb; i++)
                    fprintf(f, "%s[%ld,%.17g,%ld,%lu]", i ? "," : "", (long)a->b[i].start, a->b[i].area,
                            (long)a->b[i].covered_ms, (unsigned long)a->b[i].count);
                fprintf(f, "]}\n");
            } else {
                fprintf(f, "\"t\":[");
                for (size_t i = 0; i < a->p.n; i++) fprintf(f, "%s%ld", i ? "," : "", (long)a->p.t[i]);
                fprintf(f, "],\"v\":[");
                for (size_t i = 0; i < a->p.n; i++) fprintf(f, "%s%.9g", i ? "," : "", a->p.v[i]);
                fprintf(f, "]}\n");
            }
        }
        fclose(f);
    }
#ifdef EMBEDDED
    tsdb_close(db);
#endif
    return 0;
}
