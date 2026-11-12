/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Embedded ClickHouse (chDB, libchdb's C API) on the bench/dbcmp query spec, the counterpart of embq's DuckDB side.
 *   chq load DIR DATA CODECS          fresh table t from the RowBinary file (INSERT ... SELECT FROM file()), then
 *                                     OPTIMIZE TABLE t FINAL (timed apart), size from system.parts.
 *                                     CODECS 1: the server run's codecs (DoubleDelta/Gorilla + ZSTD(1)); 0: defaults (LZ4)
 *   chq qps  DIR SPEC CLASS THREADS SECONDS
 *   chq dump DIR SPEC [N]             answers in tsdbq's JSON lines, for check.py --dumps
 *   chq full DIR REPS OUT             whole range: count/sum/min/max, buckets of 3600 and 60 samples; each run
 *                                     printed, then median and mean; OUT gets per-series digests
 *   chq sqlt DIR REPS OUT SQL         one statement REPS times after a warm-up; the last answer as TSV to OUT
 * Every query thread has its own connection to the same path (chdb_connect, which chDB allows concurrently).
 * Queries are literal SQL, the same text as bench/dbcmp/mkq.py's ClickHouse dialect, and results come back as
 * RowBinary (chDB hands results back as a buffer in a named output format; RowBinary is its cheapest to write and
 * read), parsed and reduced like embq's (raw values summed, buckets added up). max_threads is chDB's default (all
 * cores) for every query.
 */
#define _GNU_SOURCE
#include "chdb.h"

#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* s: the series number of spec.tsv and of the RowBinary file's u32 column, an index into the bench's id table (it
 * decides the kind: s % 7, and indexes the digests); tsdb's own ids are not used here */
typedef struct q { char cls[8]; uint32_t s; int64_t a, b, bucket; } q;
typedef struct bk { int64_t start; uint64_t count; double sum; float mn, mx; } bk;
typedef struct ans { uint64_t n; double sum; int64_t first, last; float v; bk *b; size_t nb, cap; } ans;
static const char *path;
static q *qs;
static size_t nq;

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}
static void die(const char *what, const char *msg)
{
    fprintf(stderr, "chq: %s: %s\n", what, msg);
    exit(1);
}
static chdb_connection *conn(void)
{
    char arg[4200];
    snprintf(arg, sizeof arg, "--path=%s", path);
    char *argv[] = {"chq", arg};
    chdb_connection *c = chdb_connect(2, argv);
    if (!c) die(path, "chdb_connect failed");
    return c;
}
static chdb_result *run(chdb_connection *c, const char *sql, const char *fmt)
{
    chdb_result *r = chdb_query(*c, sql, fmt);
    const char *e = r ? chdb_result_error(r) : "no result";
    if (e) die(sql, e);
    return r;
}
static void exec(chdb_connection *c, const char *sql) { chdb_destroy_query_result(run(c, sql, "TSV")); }
static void load_spec(const char *p, const char *cls)
{
    FILE *f = fopen(p, "r");
    if (!f) die(p, "cannot open");
    q x;
    size_t cap = 0;
    while (fscanf(f, "%7s %u %" SCNd64 " %" SCNd64 " %" SCNd64, x.cls, &x.s, &x.a, &x.b, &x.bucket) == 5) {
        if (cls && strcmp(cls, x.cls)) continue;
        if (nq == cap) qs = realloc(qs, (cap = cap ? cap * 2 : 1024) * sizeof *qs);
        qs[nq++] = x;
    }
    fclose(f);
}
static int cmpd(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

/* mkq.py ch(): the same text, with explicit result types so RowBinary has a fixed layout */
static void sql_of(const q *x, char *o, size_t len)
{
    char w[512];
    snprintf(w, sizeof w,
             "series = %u AND ts >= fromUnixTimestamp64Milli(toInt64(%" PRId64 ")) AND ts < fromUnixTimestamp64Milli(toInt64(%" PRId64 "))",
             x->s, x->a, x->b);
    if (!strcmp(x->cls, "point"))
        snprintf(o, len, "SELECT toUnixTimestamp64Milli(ts), value FROM t WHERE series = %u AND ts = fromUnixTimestamp64Milli(toInt64(%" PRId64 "))", x->s, x->a);
    else if (x->bucket == 0)
        snprintf(o, len, "SELECT toUnixTimestamp64Milli(ts), value FROM t WHERE %s ORDER BY ts", w);
    else
        snprintf(o, len,
                 "SELECT toInt64(intDiv(toUnixTimestamp64Milli(ts), %" PRId64 ") * %" PRId64 ") AS b, toUInt64(count()), "
                 "toFloat64(sum(value)), toFloat32(min(value)), toFloat32(max(value)) FROM t WHERE %s GROUP BY b ORDER BY b",
                 x->bucket, x->bucket, w);
}
static void run_one(chdb_connection *c, const q *x, ans *r)
{
    char sql[1024];
    sql_of(x, sql, sizeof sql);
    free(r->b);
    memset(r, 0, sizeof *r);
    chdb_result *res = run(c, sql, "RowBinary");
    const char *p = chdb_result_buffer(res);
    size_t n = chdb_result_length(res);
    if (x->bucket == 0) {
        for (size_t o = 0; o + 12 <= n; o += 12) {
            int64_t t; float v;
            memcpy(&t, p + o, 8); memcpy(&v, p + o + 8, 4);
            if (!r->n) r->first = t;
            r->sum += v; r->last = t; r->v = v; r->n++;
        }
    } else {
        for (size_t o = 0; o + 32 <= n; o += 32) {
            if (r->nb == r->cap) r->b = realloc(r->b, (r->cap = r->cap ? r->cap * 2 : 64) * sizeof *r->b);
            bk *b = &r->b[r->nb++];
            memcpy(&b->start, p + o, 8); memcpy(&b->count, p + o + 8, 8); memcpy(&b->sum, p + o + 16, 8);
            memcpy(&b->mn, p + o + 24, 4); memcpy(&b->mx, p + o + 28, 4);
            r->n += b->count;
        }
    }
    chdb_destroy_query_result(res);
}

typedef struct job { int id, threads; double seconds, secs; double *lat; size_t n; uint64_t rows; } job;
static void *qps_run(void *arg)
{
    job *j = arg;
    chdb_connection *c = conn();
    ans r = {0};
    size_t cap = 0;
    double t0 = now();
    for (size_t i = (size_t)j->id; now() - t0 < j->seconds; i += (size_t)j->threads) {
        double a = now();
        run_one(c, &qs[i % nq], &r);
        j->rows += r.n;
        if (j->n == cap) j->lat = realloc(j->lat, (cap = cap ? cap * 2 : 4096) * sizeof(double));
        j->lat[j->n++] = now() - a;
    }
    j->secs = now() - t0;
    free(r.b);
    chdb_close_conn(c);
    return NULL;
}

static const int64_t STEPS[3] = {0, 3600, 60};
static void full_sql(int64_t nsamp, char *o, size_t len)
{
    if (nsamp == 0)
        snprintf(o, len, "SELECT series, toInt64(0), toUInt64(count()), toFloat64(sum(value)), toFloat32(min(value)), "
                         "toFloat32(max(value)) FROM t GROUP BY series ORDER BY series");
    else
        snprintf(o, len,
                 "SELECT series, toInt64(intDiv(toUnixTimestamp64Milli(ts), multiIf(series %% 7 = 0, 60000, series %% 7 = 4, 1, "
                 "series %% 7 IN (2, 3), 10000, 1000) * %" PRId64 ")) AS b, toUInt64(count()), toFloat64(sum(value)), "
                 "toFloat32(min(value)), toFloat32(max(value)) FROM t GROUP BY series, b ORDER BY series, b",
                 nsamp);
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: chq load DIR DATA CODECS | qps DIR SPEC CLASS THREADS SECONDS | dump DIR SPEC [N] |\n"
                        "       full DIR REPS OUT | sqlt DIR REPS OUT SQL\n");
        return 2;
    }
    const char *cmd = argv[1];
    path = argv[2];
    chdb_connection *c = conn(); /* held for the whole run: the engine starts once */
    fprintf(stderr, "chdb %s\n", chdb_version());
    if (!strcmp(cmd, "load") && argc == 5) {
        int codecs = atoi(argv[4]);
        char sql[2048];
        snprintf(sql, sizeof sql,
                 "CREATE TABLE t (series UInt32, ts DateTime64(3, 'UTC')%s, value Float32%s) ENGINE = MergeTree "
                 "PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)",
                 codecs ? " CODEC(DoubleDelta, ZSTD(1))" : "", codecs ? " CODEC(Gorilla, ZSTD(1))" : "");
        exec(c, sql);
        snprintf(sql, sizeof sql,
                 "INSERT INTO t SELECT * FROM file('%s', RowBinary, 'series UInt32, ts DateTime64(3, \\'UTC\\'), value Float32')",
                 argv[3]);
        double t0 = now();
        exec(c, sql);
        double tl = now() - t0;
        chdb_result *r = run(c, "SELECT count(), sum(bytes_on_disk), count(DISTINCT partition) FROM system.parts WHERE table = 't' AND active", "TSV");
        printf("chdb load (%s): %.3f s; parts, bytes, partitions: %.*s", codecs ? "codecs" : "default LZ4", tl,
               (int)chdb_result_length(r), chdb_result_buffer(r));
        chdb_destroy_query_result(r);
        t0 = now();
        exec(c, "OPTIMIZE TABLE t FINAL");
        double to = now() - t0;
        r = run(c, "SELECT count(), sum(bytes_on_disk), sum(rows) FROM system.parts WHERE table = 't' AND active", "TSV");
        printf("chdb optimize final: %.3f s; parts, bytes, rows: %.*s", to, (int)chdb_result_length(r), chdb_result_buffer(r));
        chdb_destroy_query_result(r);
    } else if (!strcmp(cmd, "qps") && argc == 7) {
        load_spec(argv[3], argv[4]);
        int threads = atoi(argv[5]);
        double seconds = atof(argv[6]);
        job *jobs = calloc((size_t)threads, sizeof *jobs);
        pthread_t th[512];
        for (int i = 0; i < threads; i++) {
            jobs[i] = (job){.id = i, .threads = threads, .seconds = seconds};
            pthread_create(&th[i], NULL, qps_run, &jobs[i]);
        }
        size_t total = 0;
        double secs = 0;
        uint64_t rows = 0;
        for (int i = 0; i < threads; i++) {
            pthread_join(th[i], NULL);
            total += jobs[i].n; rows += jobs[i].rows; secs = fmax(secs, jobs[i].secs);
        }
        double *all = malloc(total * sizeof *all);
        size_t m = 0;
        for (int i = 0; i < threads; i++)
            for (size_t k = 0; k < jobs[i].n; k++) all[m++] = jobs[i].lat[k];
        qsort(all, m, sizeof *all, cmpd);
        printf("chdb %s c=%d: %zu queries in %.2f s = %.1f q/s, median %.3f ms, p99 %.3f ms (%" PRIu64 " rows)\n", argv[4],
               threads, total, secs, total / secs, all[m / 2] * 1e3, all[(size_t)(m * 0.99)] * 1e3, rows);
    } else if (!strcmp(cmd, "dump") && argc >= 4) {
        load_spec(argv[3], NULL);
        size_t per = argc > 4 ? strtoull(argv[4], NULL, 10) : SIZE_MAX, k = 0;
        const char *seen = "";
        ans r = {0};
        for (size_t i = 0; i < nq; i++) {
            if (strcmp(seen, qs[i].cls)) { seen = qs[i].cls; k = 0; }
            if (k++ >= per) continue;
            run_one(c, &qs[i], &r);
            printf("{\"i\":%zu,\"cls\":\"%s\"", i, qs[i].cls);
            if (qs[i].bucket == 0)
                printf(",\"n\":%" PRIu64 ",\"sum\":%.17g,\"first\":%" PRId64 ",\"last\":%" PRId64 ",\"v\":%.9g}\n", r.n, r.sum,
                       r.first, r.last, r.v);
            else {
                printf(",\"b\":[");
                for (size_t x = 0; x < r.nb; x++)
                    printf("%s[%" PRId64 ",%" PRIu64 ",%.17g,%.9g,%.9g]", x ? "," : "", r.b[x].start, r.b[x].count, r.b[x].sum,
                           r.b[x].mn, r.b[x].mx);
                printf("]}\n");
            }
        }
    } else if (!strcmp(cmd, "full") && argc == 5) {
        int reps = atoi(argv[3]);
        FILE *out = fopen(argv[4], "w");
        static const char *names[3] = {"count/sum/min/max per series", "buckets of 3600 samples", "buckets of 60 samples"};
        for (int k = 0; k < 3; k++) {
            char sql[1024];
            full_sql(STEPS[k], sql, sizeof sql);
            double t[101], mean = 0, sd = 0, dg[1024][5];
            for (int r = 0; r <= reps; r++) {
                double a = now();
                memset(dg, 0, sizeof dg);
                chdb_result *res = run(c, sql, "RowBinary");
                const char *p = chdb_result_buffer(res);
                size_t n = chdb_result_length(res);
                for (size_t o = 0; o + 36 <= n; o += 36) {
                    uint32_t s; uint64_t cnt; double sum; float mn, mx;
                    memcpy(&s, p + o, 4); memcpy(&cnt, p + o + 12, 8); memcpy(&sum, p + o + 20, 8);
                    memcpy(&mn, p + o + 28, 4); memcpy(&mx, p + o + 32, 4);
                    if (s < 1024) { dg[s][0] += (double)cnt; dg[s][1] += sum; dg[s][2] += mn; dg[s][3] += mx; dg[s][4] += 1; }
                }
                chdb_destroy_query_result(res);
                t[r] = now() - a;
                if (r) printf("  run %d: %.4f s\n", r, t[r]);
            }
            for (int r = 1; r <= reps; r++) mean += t[r] / reps;
            for (int r = 1; r <= reps; r++) sd += (t[r] - mean) * (t[r] - mean);
            sd = reps > 1 ? sqrt(sd / (reps - 1)) : 0;
            qsort(t + 1, (size_t)reps, sizeof *t, cmpd);
            printf("chdb full %-30s one query: median %.4f s (min %.4f, max %.4f, %d runs) mean %.4f sd %.4f\n", names[k],
                   t[1 + reps / 2], t[1], t[reps], reps, mean, sd);
            for (int s = 0; out && s < (getenv("NSERIES") ? atoi(getenv("NSERIES")) : 100); s++)
                fprintf(out, "{\"full\":%" PRId64 ",\"s\":%d,\"d\":[%.17g,%.17g,%.17g,%.17g,%.17g]}\n", STEPS[k], s, dg[s][0],
                        dg[s][1], dg[s][2], dg[s][3], dg[s][4]);
        }
        if (out) fclose(out);
    } else if (!strcmp(cmd, "sqlt") && argc == 6) {
        int reps = atoi(argv[3]);
        double t[101], mean = 0, sd = 0;
        for (int r = 0; r <= reps; r++) {
            double a = now();
            chdb_result *res = run(c, argv[5], "RowBinary");
            size_t len = chdb_result_length(res);
            chdb_destroy_query_result(res);
            t[r] = now() - a;
            if (r) printf("sqlt run %d: %.4f s (%zu bytes of RowBinary)\n", r, t[r], len);
        }
        for (int r = 1; r <= reps; r++) mean += t[r] / reps;
        for (int r = 1; r <= reps; r++) sd += (t[r] - mean) * (t[r] - mean);
        printf("sqlt: mean %.4f s sd %.4f, %d runs after a warm-up\n", mean, reps > 1 ? sqrt(sd / (reps - 1)) : 0.0, reps);
        chdb_result *res = run(c, argv[5], "TSV"); /* the answer, untimed */
        FILE *o = fopen(argv[4], "w");
        if (o) { fwrite(chdb_result_buffer(res), 1, chdb_result_length(res), o); fclose(o); }
        chdb_destroy_query_result(res);
    } else
        die(cmd, "bad arguments");
    chdb_close_conn(c);
    return 0;
}
