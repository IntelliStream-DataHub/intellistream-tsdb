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
#else
#define H c
#define LTTB tsdbc_query_lttb
#define TWA tsdbc_query_twa
#define ERR tsdbc_strerror
#endif

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
    const char *m = argc > 1 ? argv[1] : "";
    int64_t interval = 3600000;
    const char *wcls = NULL;
    if (!strcmp(m, "twa")) mode_twa = 1;
    else if (!strcmp(m, "twad")) mode_twa = 1, interval = 86400000;
    else if (!strcmp(m, "twal")) mode_twa = 1, method = TSDB_TWA_LINEAR;
    else if (!strcmp(m, "twa1d")) mode_twa = 1, wcls = "agg1d";
    else if (!strcmp(m, "twa7d")) mode_twa = 1, wcls = "agg7d";
    else if (strcmp(m, "lttb")) argc = 0;
    if (argc < 4) {
        fprintf(stderr, "usage: relq lttb|twa|twad|twal|twa1d|twa7d CONNS REPS [OUT]\n");
        return 2;
    }
    conns = atoi(argv[2]);
    int reps = atoi(argv[3]);
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
#ifdef EMBEDDED
    const char *dir = getenv("TSDB_DIR");
    tsdb_options opt;
    tsdb_options_init(&opt);
    int rc = dir ? tsdb_open(dir, &opt, &db) : -1;
    if (rc) { fprintf(stderr, "open %s: %s\n", dir ? dir : "(TSDB_DIR unset)", dir ? tsdb_strerror(rc) : ""); return 1; }
#endif
    double t[101], sum = 0;
    for (int r = 0; r <= reps; r++) {
        pthread_t th[256];
        double a = now();
        for (int i = 0; i < conns; i++) pthread_create(&th[i], NULL, run, (void *)(intptr_t)i);
        for (int i = 0; i < conns; i++) pthread_join(th[i], NULL);
        t[r] = now() - a;
        if (r) {
            sum += t[r];
            printf("%s %d conns run %d: %.4f s\n", argv[1], conns, r, t[r]);
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
    printf("%s %d conns: mean %.4f s sd %.4f (%.1f%%) min %.4f max %.4f, %d runs after a warm-up; %zu %s\n", argv[1],
           conns, mean, reps > 1 ? sqrt(ss / (reps - 1)) : 0.0, reps > 1 ? 100 * sqrt(ss / (reps - 1)) / mean : 0.0, mn,
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
