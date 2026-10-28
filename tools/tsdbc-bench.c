/*
 * Network benchmark against tsdbd.
 *
 *   tsdbc-bench load  FILE.rowbinary [--conns 4] [--batch 1000000] [--compression 1]
 *   tsdbc-bench verify FILE.rowbinary     the server holds exactly the rows of FILE (count, sum, every timestamp and value)
 *   tsdbc-bench optimize                  flush and merge, as tsdb-bench does after its load
 *   tsdbc-bench query [--series 100] [--points 1000000] [--conns 1] [--seconds 3] [--multi K [--multi-one]]
 *   tsdbc-bench connections --conns 5000 [--seconds 5]
 *
 * load sends a RowBinary file (u32 series, i64 ts, f32 value per row), split
 * into contiguous slices, one per connection, with up to 4 batches in flight.
 * query runs the random windows of cbench.h (mixed dataset intervals); tsdb-bench --qps runs the same ones
 * in-process, and the Rust, Python and Java query benchmarks the same again; then TWA by the hour and the whole
 * range. --multi K sends K windows per QUERY_MULTI request; 0 is one request per window as always, 1 the plain op
 * that QUERY_MULTI wraps (QUERY_VALUES for raw rows), and --multi-one with K = 1 QUERY_MULTI of one series (its
 * overhead over the plain op).
 * connections holds --conns connections open at once (from up to 64 threads)
 * and keeps sending small aggregate queries through all of them.
 */
#include "../client/tsdb_client.h"
#include "cbench.h"

#include <getopt.h>
#include <pthread.h>
#include <stdatomic.h>

static const char *host = "127.0.0.1";
static int port = 7070, conns = 1;
static size_t batch = 1000000;
static uint32_t nseries = 100;
static uint64_t points = 1000000;
static double seconds = CB_DEFAULT_SECONDS;
static int compression = 1; /* zstd level of insert batches; 0 sends plain arrays */

static void die(const char *what, int rc)
{
    fprintf(stderr, "tsdbc-bench: %s: %s\n", what, tsdbc_strerror(rc));
    exit(1);
}

typedef struct load_job {
    const rbrow *rows;
    size_t n;
    uint64_t sent;
} load_job;

static void *loader(void *arg)
{
    load_job *j = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    if ((rc = tsdbc_set_compression(c, compression)) != 0)
        die("compression", rc);
    uint32_t *s = malloc(batch * 4);
    int64_t *t = malloc(batch * 8);
    float *v = malloc(batch * 4);
    int inflight = 0;
    for (size_t off = 0; off < j->n; off += batch) {
        size_t m = j->n - off < batch ? j->n - off : batch;
        if (inflight == 4) { /* bound the pipeline (and our reuse of the arrays below) */
            if ((rc = tsdbc_wait(c)) != 0)
                die("insert", rc);
            inflight = 0;
        }
        for (size_t k = 0; k < m; k++) {
            s[k] = j->rows[off + k].s;
            t[k] = j->rows[off + k].t;
            v[k] = j->rows[off + k].v;
        }
        /* the send completes before returning, so the arrays may be refilled */
        if ((rc = tsdbc_insert_async(c, s, t, v, m)) != 0)
            die("insert", rc);
        inflight++;
    }
    if ((rc = tsdbc_wait(c)) != 0)
        die("insert", rc);
    j->sent = tsdbc_bytes_sent(c);
    tsdbc_close(c);
    free(s);
    free(t);
    free(v);
    return NULL;
}

static int load(const char *path)
{
    cb_file f;
    if (cb_file_open(path, &f) != 0)
        return 1;
    const rbrow *rows = f.rows;
    size_t n = f.n;
    uint32_t maxs = 0;
    for (size_t i = 0; i < n && i < 10000000; i++)
        maxs = rows[i].s > maxs ? rows[i].s : maxs;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    for (uint32_t i = 0; i <= maxs; i++) {
        char name[64];
        uint32_t id;
        snprintf(name, sizeof name, "%s.%u", ds_info[i % (DS_KINDS - 1)].name, i);
        if ((rc = tsdbc_series(c, name, 1, &id)) != 0)
            die("series", rc);
    }
    double t0 = now_s(), cpu0 = cb_cpu_s();
    pthread_t th[256];
    load_job jobs[256];
    for (int i = 0; i < conns; i++) {
        size_t a = n * (size_t)i / (size_t)conns, b = n * (size_t)(i + 1) / (size_t)conns;
        jobs[i] = (load_job){rows + a, b - a, 0};
        pthread_create(&th[i], NULL, loader, &jobs[i]);
    }
    double sent = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(th[i], NULL);
        sent += (double)jobs[i].sent;
    }
    double dt = now_s() - t0, cpu = cb_cpu_s() - cpu0;
    char extra[64];
    snprintf(extra, sizeof extra, ", batches of %zu", batch);
    cb_report_load("c", conns, compression, n, dt, sent, cpu, extra);
    tsdbc_close(c);
    return 0;
}

static int verify_agg(void *ctx, uint32_t series, int64_t start, int64_t end, int64_t interval, tsdb_bucket **out,
                      size_t *nout)
{
    return tsdbc_query_agg(ctx, series, start, end, interval, out, nout);
}

static int verify_raw(void *ctx, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *fnctx)
{
    return tsdbc_query_raw(ctx, series, start, end, fn, fnctx);
}

/* The server holds the rows of the file: the same rows in every series: count, sum, and every timestamp and value. Series ids
 * are the file's, which is what a fresh server gives to series created in order (all clients create them that way). */
static int verify(const char *path)
{
    cb_file f;
    if (cb_file_open(path, &f) != 0)
        return 1;
    cb_expect e;
    cb_expect_make(&f, &e);
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    cb_source src = {c, verify_agg, verify_raw};
    rc = cb_expect_verify_all(&e, f.n, &src, "on the server");
    tsdbc_close(c);
    cb_expect_free(&e);
    return rc;
}

static int optimize(void)
{
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    double t0 = now_s();
    if ((rc = tsdbc_flush(c)) != 0)
        die("flush", rc);
    if ((rc = tsdbc_optimize(c)) != 0)
        die("optimize", rc);
    printf("flush and optimize: %.2f s\n", now_s() - t0);
    tsdbc_close(c);
    return 0;
}

/* ---------------------------------------------------------------- query */

/* With --multi K (K >= 2, or K = 1 with --multi-one) a connection sends its windows K at a time as one
 * QUERY_MULTI with per-series starts, ends and intervals; queries still count windows, so the q/s of K = 0
 * (one request per window) and of K are the same work. The extra classes are tsdbc-bench's own (tsdb-bench
 * --qps and the other clients run cb_classes only): TWA by the hour of a week, and the whole range. */
static int multi_k = 0, multi_one = 0;

static const cb_class extra_classes[] = {
    {"twa3600", "twa, 604800 samples in buckets of 3600", 2, 604800, 3600},
    {"full0", "agg, the whole range in one bucket", 1, 0, 0},
};
#define EXTRA_NCLASSES 2

typedef struct qjob {
    const cb_class *k;
    int seed;
    uint64_t queries, rows;
    double secs;
} qjob;

static int count_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts;
    (void)v;
    *(uint64_t *)ctx += n;
    return 0;
}

static void next_window(uint64_t *r, const cb_class *k, uint32_t *s, int64_t *start, int64_t *end)
{
    if (k->window == 0) { /* the whole range of a random series */
        *r = ds_mix(*r);
        *s = (uint32_t)(*r % nseries);
        *start = INT64_MIN;
        *end = INT64_MAX;
        return;
    }
    cb_window(r, nseries, points, k->window, s, start, end);
}

typedef struct multi_ctx {
    int agg; /* as cb_class.agg */
    uint64_t rows;
} multi_ctx;

static int values_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts;
    (void)v;
    *(uint64_t *)ctx += n;
    return 0;
}

static int multi_fn(void *ctx, const tsdb_series_result *r)
{
    multi_ctx *m = ctx;
    if (r->status)
        die("query multi, series", r->status);
    if (!m->agg)
        m->rows += r->n;
    else if (m->agg == 2)
        for (size_t i = 0; i < r->n; i++)
            m->rows += ((const tsdb_twa_bucket *)r->buckets)[i].count;
    else
        for (size_t i = 0; i < r->n; i++)
            m->rows += ((const tsdb_bucket *)r->buckets)[i].count;
    return 0;
}

static void *querier(void *arg)
{
    qjob *q = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    uint64_t r = cb_seed(q->seed);
    tsdb_twa_opts twa = {.method = TSDB_TWA_STEP};
    int use_multi = multi_k >= 2 || (multi_k == 1 && multi_one);
    int kk = use_multi ? multi_k : 1;
    uint32_t *ss = malloc((size_t)kk * sizeof *ss);
    int64_t *st = malloc((size_t)kk * 3 * sizeof *st), *en = st + kk, *iv = en + kk;
    double t0 = now_s();
    while (now_s() - t0 < seconds) {
        for (int k = 0; k < 16; k++) {
            uint32_t s;
            int64_t start, end;
            if (use_multi) {
                for (int i = 0; i < kk; i++) {
                    next_window(&r, q->k, &ss[i], &st[i], &en[i]);
                    iv[i] = (int64_t)q->k->bucket * cb_step_ms(ss[i]);
                }
                tsdb_multi_query mq = {.kind = !q->k->agg ? TSDB_MULTI_VALUES
                                               : q->k->agg == 2 ? TSDB_MULTI_TWA
                                                                : TSDB_MULTI_BUCKETS,
                                       .starts = st, .ends = en, .intervals = q->k->agg ? iv : NULL, .twa = twa};
                multi_ctx mc = {q->k->agg, 0};
                if ((rc = tsdbc_query_multi(c, &mq, ss, (size_t)kk, NULL, multi_fn, &mc)) != 0)
                    die("query multi", rc);
                q->rows += mc.rows;
                q->queries += (uint64_t)kk;
                continue;
            }
            next_window(&r, q->k, &s, &start, &end);
            if (!q->k->agg) {
                uint64_t n = 0; /* --multi 1: QUERY_VALUES, the op QUERY_MULTI wraps; else QUERY_RAW as always */
                if ((rc = multi_k == 1 ? tsdbc_query_values(c, s, start, end, NULL, values_cb, &n)
                                       : tsdbc_query_raw(c, s, start, end, count_cb, &n)) != 0)
                    die("query", rc);
                q->rows += n;
            } else if (q->k->agg == 2) {
                tsdb_twa_bucket *b;
                size_t nb;
                if ((rc = tsdbc_query_twa(c, s, start, end, (int64_t)q->k->bucket * cb_step_ms(s), &twa, &b, &nb)) != 0)
                    die("query", rc);
                for (size_t i = 0; i < nb; i++)
                    q->rows += b[i].count;
                free(b);
            } else {
                tsdb_bucket *b;
                size_t nb;
                if ((rc = tsdbc_query_agg(c, s, start, end, (int64_t)q->k->bucket * cb_step_ms(s), &b, &nb)) != 0)
                    die("query", rc);
                for (size_t i = 0; i < nb; i++)
                    q->rows += b[i].count;
                free(b);
            }
            q->queries++;
        }
    }
    q->secs = now_s() - t0;
    free(ss);
    free(st);
    tsdbc_close(c);
    return NULL;
}

static int run_queries(const cb_class *k)
{
    pthread_t th[256];
    qjob jobs[256];
    double cpu0 = cb_cpu_s();
    for (int i = 0; i < conns; i++) {
        jobs[i] = (qjob){k, i, 0, 0, 0};
        pthread_create(&th[i], NULL, querier, &jobs[i]);
    }
    uint64_t nq = 0, rows = 0;
    double secs = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(th[i], NULL);
        nq += jobs[i].queries;
        rows += jobs[i].rows;
        secs = jobs[i].secs > secs ? jobs[i].secs : secs;
    }
    cb_class shown = *k;
    char id[64], label[96];
    if (multi_k >= 1) {
        snprintf(id, sizeof id, "%s.%s%d", k->id, multi_one || multi_k > 1 ? "m" : "p", multi_k);
        snprintf(label, sizeof label, "%.32s %s%d", k->label, multi_one || multi_k > 1 ? "x" : "plain ", multi_k);
        shown.id = id;
        shown.label = label;
    }
    cb_report_query("c", &shown, "conn", conns, nq, rows, secs, cb_cpu_s() - cpu0);
    if (rows == 0) {
        fprintf(stderr, "query: class %s returned no rows: the server is empty or does not hold the data set\n", k->id);
        return 1;
    }
    return 0;
}

/* ---------------------------------------------------------- connections */

typedef struct cjob {
    int idx, nthreads, total;
    int opened, failed;
    uint64_t queries, errors;
    atomic_int *ready;
} cjob;

static void *holder(void *arg)
{
    cjob *j = arg;
    int mine = j->total / j->nthreads + (j->idx < j->total % j->nthreads);
    tsdbc **cs = calloc((size_t)mine, sizeof *cs);
    for (int i = 0; i < mine; i++) {
        if (tsdbc_connect(host, port, &cs[i]) == 0)
            j->opened++;
        else
            j->failed++;
    }
    atomic_fetch_add(j->ready, 1);
    while (atomic_load(j->ready) < j->nthreads) /* everyone connected: all are open at once */
        usleep(1000);
    double t0 = now_s();
    uint64_t r = ds_mix((uint64_t)j->idx + 7);
    while (now_s() - t0 < seconds) {
        for (int i = 0; i < mine; i++) {
            if (!cs[i])
                continue;
            r = ds_mix(r);
            uint32_t s = (uint32_t)(r % nseries);
            int64_t step = ds_info[s % (DS_KINDS - 1)].step_ms;
            int64_t start = 1704067200000 + (int64_t)((r >> 20) % (points - 3600)) * step;
            tsdb_bucket *b;
            size_t nb;
            if (tsdbc_query_agg(cs[i], s, start, start + 3600 * step, 0, &b, &nb) == 0) {
                free(b);
                j->queries++;
            } else {
                j->errors++;
                tsdbc_close(cs[i]);
                cs[i] = NULL;
            }
        }
    }
    for (int i = 0; i < mine; i++)
        tsdbc_close(cs[i]);
    free(cs);
    return NULL;
}

static int connections(void)
{
    int nthreads = conns < 64 ? conns : 64;
    pthread_t th[64];
    cjob jobs[64];
    atomic_int ready = 0;
    double t0 = now_s();
    for (int i = 0; i < nthreads; i++) {
        jobs[i] = (cjob){i, nthreads, conns, 0, 0, 0, 0, &ready};
        pthread_create(&th[i], NULL, holder, &jobs[i]);
    }
    int opened = 0, failed = 0;
    uint64_t q = 0, errors = 0;
    for (int i = 0; i < nthreads; i++) {
        pthread_join(th[i], NULL);
        opened += jobs[i].opened;
        failed += jobs[i].failed;
        q += jobs[i].queries;
        errors += jobs[i].errors;
    }
    double dt = now_s() - t0;
    printf("%d connections open at once (%d refused), %llu queries in %.1f s = %.0f q/s, %llu failed\n", opened,
           failed, (unsigned long long)q, dt, q / dt, (unsigned long long)errors);
    return failed || errors ? 1 : 0;
}

int main(int argc, char **argv)
{
    static struct option opts[] = {{"host", required_argument, 0, 'h'},   {"port", required_argument, 0, 'p'},
                                   {"conns", required_argument, 0, 'c'},  {"batch", required_argument, 0, 'b'},
                                   {"series", required_argument, 0, 's'}, {"points", required_argument, 0, 'n'},
                                   {"seconds", required_argument, 0, 't'}, {"compression", required_argument, 0, 'z'},
                                   {"multi", required_argument, 0, 'm'}, {"multi-one", no_argument, 0, 'o'},
                                   {0, 0, 0, 0}};
    int ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'h': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 'c': conns = atoi(optarg); break;
        case 'b': batch = strtoull(optarg, NULL, 10); break;
        case 's': nseries = (uint32_t)atoi(optarg); break;
        case 'n': points = strtoull(optarg, NULL, 10); break;
        case 't': seconds = atof(optarg); break;
        case 'z': compression = atoi(optarg); break;
        case 'm': multi_k = atoi(optarg); break;
        case 'o': multi_one = 1; break;
        default: return 2;
        }
    }
    if (optind < argc && strcmp(argv[optind], "connections") == 0 && conns >= 1)
        return connections();
    if (conns < 1 || conns > 256 || optind >= argc || multi_k < 0 || multi_k > 65536) {
        fprintf(stderr, "usage: tsdbc-bench load FILE [--conns N] [--batch ROWS] [--compression LEVEL]\n"
                        "       tsdbc-bench verify FILE | optimize\n"
                        "       tsdbc-bench query [--series N] [--points N] [--conns N] [--seconds S] [--multi K [--multi-one]]\n"
                        "       tsdbc-bench connections --conns N [--seconds S]\n"
                        "       [--host H] [--port P]\n");
        return 2;
    }
    if (strcmp(argv[optind], "load") == 0 && optind + 1 < argc)
        return load(argv[optind + 1]);
    if (strcmp(argv[optind], "verify") == 0 && optind + 1 < argc)
        return verify(argv[optind + 1]);
    if (strcmp(argv[optind], "optimize") == 0)
        return optimize();
    if (strcmp(argv[optind], "query") == 0) {
        int bad = 0;
        for (int k = 0; k < CB_NCLASSES; k++)
            bad |= run_queries(&cb_classes[k]);
        for (int k = 0; k < EXTRA_NCLASSES; k++)
            bad |= run_queries(&extra_classes[k]);
        return bad;
    }
    return 2;
}
