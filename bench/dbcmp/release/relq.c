/*
 * Release benchmark: LTTB and time-weighted averages over the whole range, every series, for tsdb.
 *   relq lttb CONNS REPS [OUT]   LTTB, 1000 points per series (tsdb_query_lttb)
 *   relq twa  CONNS REPS [OUT]   STEP time-weighted average by hour (tsdb_query_twa, interval 3600000)
 * Built twice from this file: relq talks to tsdbd (TSDB_HOST:TSDB_PORT, default 127.0.0.1:7070) through the C
 * client, one connection per thread; relemb (-DEMBEDDED, DB=directory in TSDB_DIR) calls the library in-process
 * with one shared handle, like embq. The series are spread over CONNS threads; a warm-up run, then REPS timed
 * runs, each printed, then mean, sd, min and max. OUT gets the answers of the last run, one JSON line per series:
 *   lttb: {"s":S,"t":[...],"v":[...]}      twa: {"s":S,"b":[[start,area,covered_ms,count],...]}
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
typedef struct pts { int64_t *t; double *v; size_t n, cap; } pts;
typedef struct ans { pts p; tsdb_twa_bucket *b; size_t nb; } ans;
static ans res[MAXSERIES];

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
    tsdb_twa_opts o = {.method = TSDB_TWA_STEP};
    for (uint32_t s = (uint32_t)id; s < nseries; s += (uint32_t)conns) {
        ans *a = &res[s];
        int e;
        if (mode_twa) {
            free(a->b);
            a->b = NULL;
            e = TWA(H, s, INT64_MIN, INT64_MAX, 3600000, &o, &a->b, &a->nb);
        } else {
            a->p.n = 0;
            e = LTTB(H, s, INT64_MIN, INT64_MAX, 1000, pts_cb, &a->p);
        }
        if (e) { fprintf(stderr, "series %u: %s\n", s, ERR(e)); exit(1); }
    }
#ifndef EMBEDDED
    tsdbc_close(c);
#endif
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 4 || (strcmp(argv[1], "lttb") && strcmp(argv[1], "twa"))) {
        fprintf(stderr, "usage: relq lttb|twa CONNS REPS [OUT]\n");
        return 2;
    }
    mode_twa = !strcmp(argv[1], "twa");
    conns = atoi(argv[2]);
    int reps = atoi(argv[3]);
    if (getenv("NSERIES")) nseries = (uint32_t)atoi(getenv("NSERIES"));
    if (conns < 1 || conns > 256 || reps < 1 || reps > 100 || nseries < 1 || nseries > MAXSERIES) return 2;
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
    for (uint32_t s = 0; s < nseries; s++) tot += mode_twa ? res[s].nb : res[s].p.n;
    printf("%s %d conns: mean %.4f s sd %.4f (%.1f%%) min %.4f max %.4f, %d runs after a warm-up; %zu %s\n", argv[1],
           conns, mean, reps > 1 ? sqrt(ss / (reps - 1)) : 0.0, reps > 1 ? 100 * sqrt(ss / (reps - 1)) / mean : 0.0, mn,
           mx, reps, tot, mode_twa ? "buckets" : "points");
    if (argc > 4) {
        FILE *f = fopen(argv[4], "w");
        if (!f) { perror(argv[4]); return 1; }
        for (uint32_t s = 0; s < nseries; s++) {
            ans *a = &res[s];
            fprintf(f, "{\"s\":%u,", s);
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
