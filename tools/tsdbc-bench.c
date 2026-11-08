/*
 * Network benchmark against tsdbd.
 *
 *   tsdbc-bench load  FILE.rowbinary [--conns 4] [--batch 1000000] [--compression 1]
 *   tsdbc-bench verify FILE.rowbinary     the server holds exactly the rows of FILE (count, sum, every timestamp and value)
 *   tsdbc-bench optimize                  flush and merge, as tsdb-bench does after its load
 *   tsdbc-bench query [--series 100] [--points 1000000] [--conns 1] [--seconds 3] [--multi K [--multi-one]]
 *   tsdbc-bench latest [--series 100] [--ids 4096] [--conns 1] [--seconds 3] [--sorted] [--flush] [--with-nulls] [--dataset]
 *   tsdbc-bench connections --conns 5000 [--seconds 5]
 *
 * load sends a RowBinary file (u32 series, i64 ts, f32 value per row), split
 * into contiguous slices, one per connection, with up to 4 batches in flight.
 * query runs the random windows of cbench.h (mixed dataset intervals); tsdb-bench --qps runs the same ones
 * in-process, and the Rust, Python and Java query benchmarks the same again; then TWA by the hour and the whole
 * range. --multi K sends K windows per QUERY_MULTI request; 0 is one request per window as always, 1 the plain op
 * that QUERY_MULTI wraps (QUERY_VALUES for raw rows), and --multi-one with K = 1 QUERY_MULTI of one series (its
 * overhead over the plain op).
 * latest sends QUERY_LATEST requests of --ids random ids on --conns connections and reports p50, p99 and max of
 * a request, with a connection writing and flushing every series in a loop under --flush (see run_latest).
 * connections holds --conns connections open at once (from up to 64 threads)
 * and keeps sending small aggregate queries through all of them.
 * --user U --password-file F logs every connection in (a tsdbd without --no-auth); the keys are derived once per
 * process, so a connect costs the SCRAM exchange's round trips and four HMACs.
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
static tsdbc_options login;  /* user NULL: no login */

static int bench_connect(const char *h, int p, tsdbc **out)
{
    return login.user ? tsdbc_connect_opts(h, p, &login, out) : tsdbc_connect(h, p, out);
}
#define tsdbc_connect bench_connect /* every connection below, with the login when one is given */

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

/* Series are numbered 0 .. n - 1 here and in the row files, named "<kind>.<number>"; sids[] holds their ids, looked
 * up by name (ids carry the volume, and the server may hold other series). */
static tsdb_sid *sids;

static void resolve_ids(tsdbc *c, uint32_t n, int create)
{
    sids = calloc(n ? n : 1, sizeof *sids);
    for (uint32_t i = 0; i < n; i++) {
        char name[64];
        snprintf(name, sizeof name, "%s.%u", ds_info[i % (DS_KINDS - 1)].name, i);
        int rc = tsdbc_series(c, name, create, &sids[i]);
        if (rc)
            die(name, rc);
    }
}

/* the ids of the query benchmarks' series (--series), which a load made */
static void resolve_query_ids(void)
{
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    resolve_ids(c, nseries, 0);
    tsdbc_close(c);
}

static void *loader(void *arg)
{
    load_job *j = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    if ((rc = tsdbc_set_compression(c, compression)) != 0)
        die("compression", rc);
    tsdb_sid *s = malloc(batch * sizeof *s);
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
            s[k] = sids[j->rows[off + k].s];
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
    resolve_ids(c, maxs + 1, 1);
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

static int verify_agg(void *ctx, tsdb_sid series, int64_t start, int64_t end, int64_t interval, tsdb_bucket **out,
                      size_t *nout)
{
    return tsdbc_query_agg(ctx, series, start, end, interval, out, nout);
}

static int verify_raw(void *ctx, tsdb_sid series, int64_t start, int64_t end, tsdb_raw_fn fn, void *fnctx)
{
    return tsdbc_query_raw(ctx, series, start, end, fn, fnctx);
}

/* The server holds the rows of the file: the same rows in every series: count, sum, and every timestamp and value. The
 * file's series numbers are names ("<kind>.<number>", as every client creates them), looked up for their ids. */
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
    resolve_ids(c, e.nseries, 0);
    cb_source src = {c, verify_agg, verify_raw, sids};
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

/* a window of a random series: its number (for its rate) and its id */
static void next_window(uint64_t *r, const cb_class *k, uint32_t *ix, tsdb_sid *s, int64_t *start, int64_t *end)
{
    if (k->window == 0) { /* the whole range of a random series */
        *r = ds_mix(*r);
        *ix = (uint32_t)(*r % nseries);
        *start = INT64_MIN;
        *end = INT64_MAX;
    } else {
        cb_window(r, nseries, points, k->window, ix, start, end);
    }
    *s = sids[*ix];
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
    tsdb_sid *ss = malloc((size_t)kk * sizeof *ss);
    int64_t *st = malloc((size_t)kk * 3 * sizeof *st), *en = st + kk, *iv = en + kk;
    double t0 = now_s();
    while (now_s() - t0 < seconds) {
        for (int k = 0; k < 16; k++) {
            tsdb_sid s;
            uint32_t ix;
            int64_t start, end;
            if (use_multi) {
                for (int i = 0; i < kk; i++) {
                    uint32_t ix;
                    next_window(&r, q->k, &ix, &ss[i], &st[i], &en[i]);
                    iv[i] = (int64_t)q->k->bucket * cb_step_ms(ix);
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
            next_window(&r, q->k, &ix, &s, &start, &end);
            if (!q->k->agg) {
                uint64_t n = 0; /* --multi 1: QUERY_VALUES, the op QUERY_MULTI wraps; else QUERY_RAW as always */
                if ((rc = multi_k == 1 ? tsdbc_query_values(c, s, start, end, NULL, values_cb, &n)
                                       : tsdbc_query_raw(c, s, start, end, count_cb, &n)) != 0)
                    die("query", rc);
                q->rows += n;
            } else if (q->k->agg == 2) {
                tsdb_twa_bucket *b;
                size_t nb;
                if ((rc = tsdbc_query_twa(c, s, start, end, (int64_t)q->k->bucket * cb_step_ms(ix), &twa, &b, &nb)) != 0)
                    die("query", rc);
                for (size_t i = 0; i < nb; i++)
                    q->rows += b[i].count;
                free(b);
            } else {
                tsdb_bucket *b;
                size_t nb;
                if ((rc = tsdbc_query_agg(c, s, start, end, (int64_t)q->k->bucket * cb_step_ms(ix), &b, &nb)) != 0)
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

/* ---------------------------------------------------------- latest */

/* QUERY_LATEST (design-latest.md 9): --conns connections each send requests of --ids random ids (--sorted: in
 * ascending order) for --seconds, timing every request. Series "lb.<i>", i < --series, are created and given a row
 * first (--dataset: the series of a load instead, as the query benchmarks use). --flush adds one more connection that
 * writes a row to every series and flushes, again and again, while the lookups run (steady flushes: the lookups'
 * p99 under a publish). --with-nulls forces the slow path. */
static int lat_ids = 4096, lat_sorted, lat_dataset, lat_flush, lat_with_nulls;
static tsdb_sid *lat_sids;
static atomic_int lat_run;

enum { LAT_MAX = 1 << 21, LAT_CHUNK = 100000 };

typedef struct ljob {
    int seed;
    double *us;
    uint64_t n, ids, errors;
} ljob;

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

static int cmp_dbl(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static void *lat_querier(void *arg)
{
    ljob *j = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    uint64_t r = cb_seed(j->seed) | 1;
    tsdb_sid *q = malloc((size_t)lat_ids * sizeof *q);
    uint64_t *ix = malloc((size_t)lat_ids * sizeof *ix);
    tsdb_latest_result *out = malloc((size_t)lat_ids * sizeof *out);
    tsdb_latest_opts op = {.end = TSDB_TS_MAX, .with_nulls = lat_with_nulls, .threads = 1};
    j->us = malloc(LAT_MAX * sizeof *j->us);
    while (atomic_load(&lat_run)) {
        for (int i = 0; i < lat_ids; i++) {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            ix[i] = r % nseries;
        }
        if (lat_sorted)
            qsort(ix, (size_t)lat_ids, sizeof *ix, cmp_u64);
        for (int i = 0; i < lat_ids; i++)
            q[i] = lat_sids[ix[i]];
        double t0 = now_s();
        rc = tsdbc_latest(c, q, (size_t)lat_ids, &op, out);
        double dt = now_s() - t0;
        if (rc) {
            j->errors++;
            continue;
        }
        for (int i = 0; i < lat_ids; i++)
            if (out[i].status)
                j->errors++;
        if (j->n < LAT_MAX)
            j->us[j->n] = dt * 1e6;
        j->n++;
        j->ids += (uint64_t)lat_ids;
    }
    tsdbc_close(c);
    free(q);
    free(ix);
    free(out);
    return NULL;
}

typedef struct fjob {
    uint64_t rounds;
    double *ms;
} fjob;

/* one row for every series per round, at a time that grows, then a flush */
static void *lat_flusher(void *arg)
{
    fjob *f = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    tsdb_sid *s = malloc(LAT_CHUNK * sizeof *s);
    int64_t *t = malloc(LAT_CHUNK * 8);
    float *v = malloc(LAT_CHUNK * 4);
    f->ms = malloc(4096 * sizeof *f->ms);
    while (atomic_load(&lat_run) && f->rounds < 4096) {
        double t0 = now_s();
        for (uint32_t off = 0; off < nseries; off += LAT_CHUNK) {
            uint32_t m = nseries - off < LAT_CHUNK ? nseries - off : LAT_CHUNK;
            for (uint32_t k = 0; k < m; k++) {
                s[k] = lat_sids[off + k];
                t[k] = 1700000000000LL + (int64_t)(f->rounds + 1) * 1000;
                v[k] = (float)(f->rounds + k);
            }
            if ((rc = tsdbc_insert(c, s, t, v, m)) != 0)
                die("insert", rc);
        }
        if ((rc = tsdbc_flush(c)) != 0)
            die("flush", rc);
        f->ms[f->rounds++] = (now_s() - t0) * 1e3;
    }
    tsdbc_close(c);
    free(s);
    free(t);
    free(v);
    return NULL;
}

static int run_latest(void)
{
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    if (lat_dataset) {
        resolve_ids(c, nseries, 0);
        lat_sids = sids;
    } else {
        lat_sids = malloc(nseries * sizeof *lat_sids);
        tsdb_sid *s = malloc(LAT_CHUNK * sizeof *s);
        int64_t *t = malloc(LAT_CHUNK * 8);
        float *v = malloc(LAT_CHUNK * 4);
        for (uint32_t i = 0; i < nseries; i++) {
            char name[64];
            snprintf(name, sizeof name, "lb.%u", i);
            if ((rc = tsdbc_series(c, name, 1, &lat_sids[i])) != 0)
                die(name, rc);
        }
        for (uint32_t off = 0; off < nseries; off += LAT_CHUNK) {
            uint32_t m = nseries - off < LAT_CHUNK ? nseries - off : LAT_CHUNK;
            for (uint32_t k = 0; k < m; k++) {
                s[k] = lat_sids[off + k];
                t[k] = 1700000000000LL;
                v[k] = (float)k;
            }
            if ((rc = tsdbc_insert(c, s, t, v, m)) != 0)
                die("insert", rc);
        }
        if ((rc = tsdbc_flush(c)) != 0)
            die("flush", rc);
        free(s);
        free(t);
        free(v);
    }
    tsdbc_close(c);
    pthread_t th[256], ft;
    ljob jobs[256];
    fjob fj = {0, NULL};
    atomic_store(&lat_run, 1);
    double cpu0 = cb_cpu_s();
    for (int i = 0; i < conns; i++) {
        jobs[i] = (ljob){i, NULL, 0, 0, 0};
        pthread_create(&th[i], NULL, lat_querier, &jobs[i]);
    }
    if (lat_flush)
        pthread_create(&ft, NULL, lat_flusher, &fj);
    double t0 = now_s();
    while (now_s() - t0 < seconds)
        usleep(10000);
    atomic_store(&lat_run, 0);
    uint64_t nq = 0, nids = 0, errors = 0, kept = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(th[i], NULL);
        nq += jobs[i].n;
        nids += jobs[i].ids;
        errors += jobs[i].errors;
        kept += jobs[i].n < LAT_MAX ? jobs[i].n : LAT_MAX;
    }
    if (lat_flush)
        pthread_join(ft, NULL);
    double secs = now_s() - t0;
    double *all = malloc((kept ? kept : 1) * sizeof *all);
    size_t k = 0;
    for (int i = 0; i < conns; i++) {
        uint64_t m = jobs[i].n < LAT_MAX ? jobs[i].n : LAT_MAX;
        memcpy(all + k, jobs[i].us, m * sizeof *all);
        k += m;
    }
    qsort(all, k, sizeof *all, cmp_dbl);
    double p50 = k ? all[k / 2] : 0, p99 = k ? all[(size_t)((double)k * 0.99)] : 0, mx = k ? all[k - 1] : 0;
    printf("  latest %u series, %d ids per request%s, %d conns%s%s: %.0f requests/s %.2f M ids/s, p50 %.0f us, p99 %.0f us, "
           "max %.0f us, %" PRIu64 " errors\n",
           nseries, lat_ids, lat_sorted ? " sorted" : "", conns, lat_flush ? ", flushing" : "",
           lat_with_nulls ? ", with nulls" : "", (double)nq / secs, (double)nids / secs / 1e6, p50, p99, mx, errors);
    double fmean = 0, fmax = 0;
    if (lat_flush && fj.rounds) {
        qsort(fj.ms, fj.rounds, sizeof *fj.ms, cmp_dbl);
        for (uint64_t i = 0; i < fj.rounds; i++)
            fmean += fj.ms[i];
        fmean /= (double)fj.rounds;
        fmax = fj.ms[fj.rounds - 1];
        printf("  %" PRIu64 " write+flush rounds of %u series: mean %.1f ms, median %.1f ms, max %.1f ms\n", fj.rounds,
               nseries, fmean, fj.ms[fj.rounds / 2], fmax);
    }
    printf("RESULT kind=latest client=c series=%u ids=%d conns=%d sorted=%d flush=%d nulls=%d requests=%" PRIu64
           " idcount=%" PRIu64 " secs=%.4f p50_us=%.1f p99_us=%.1f max_us=%.1f errors=%" PRIu64 " rounds=%" PRIu64
           " flush_mean_ms=%.1f cpu=%.4f\n",
           nseries, lat_ids, conns, lat_sorted, lat_flush, lat_with_nulls, nq, nids, secs, p50, p99, mx, errors,
           fj.rounds, fmean, cb_cpu_s() - cpu0);
    fflush(stdout);
    free(all);
    return errors ? 1 : 0;
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
            if (tsdbc_query_agg(cs[i], sids[s], start, start + 3600 * step, 0, &b, &nb) == 0) {
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
                                   {"ids", required_argument, 0, 'i'}, {"sorted", no_argument, 0, 'S'},
                                   {"flush", no_argument, 0, 'F'}, {"with-nulls", no_argument, 0, 'N'},
                                   {"dataset", no_argument, 0, 'D'},
                                   {"user", required_argument, 0, 'u'},  {"password-file", required_argument, 0, 'w'},
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
        case 'i': lat_ids = atoi(optarg); break;
        case 'S': lat_sorted = 1; break;
        case 'F': lat_flush = 1; break;
        case 'N': lat_with_nulls = 1; break;
        case 'D': lat_dataset = 1; break;
        case 'u': login.user = optarg; break;
        case 'w': login.password_file = optarg; break;
        default: return 2;
        }
    }
    if (optind < argc && strcmp(argv[optind], "connections") == 0 && conns >= 1) {
        resolve_query_ids();
        return connections();
    }
    if (conns < 1 || conns > 256 || optind >= argc || multi_k < 0 || multi_k > 65536 ||
        lat_ids < 1 || lat_ids > (1 << 20)) {
        fprintf(stderr, "usage: tsdbc-bench load FILE [--conns N] [--batch ROWS] [--compression LEVEL]\n"
                        "       tsdbc-bench verify FILE | optimize\n"
                        "       tsdbc-bench query [--series N] [--points N] [--conns N] [--seconds S] [--multi K [--multi-one]]\n"
                        "       tsdbc-bench latest [--series N] [--ids N] [--conns N] [--seconds S] [--sorted] [--flush] [--with-nulls] [--dataset]\n"
                        "       tsdbc-bench connections --conns N [--seconds S]\n"
                        "       [--host H] [--port P] [--user U --password-file F]\n");
        return 2;
    }
    if (strcmp(argv[optind], "load") == 0 && optind + 1 < argc)
        return load(argv[optind + 1]);
    if (strcmp(argv[optind], "verify") == 0 && optind + 1 < argc)
        return verify(argv[optind + 1]);
    if (strcmp(argv[optind], "optimize") == 0)
        return optimize();
    if (strcmp(argv[optind], "latest") == 0)
        return run_latest();
    if (strcmp(argv[optind], "query") == 0) {
        resolve_query_ids();
        int bad = 0;
        for (int k = 0; k < CB_NCLASSES; k++)
            bad |= run_queries(&cb_classes[k]);
        for (int k = 0; k < EXTRA_NCLASSES; k++)
            bad |= run_queries(&extra_classes[k]);
        return bad;
    }
    return 2;
}
