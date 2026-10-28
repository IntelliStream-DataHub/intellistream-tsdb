/*
 * Embedded comparison: tsdb through its C library and DuckDB through its C API, in one process,
 * with the same query spec, timing and result handling, so neither side pays for a network.
 *
 *   embq ENGINE load DB DATA THREADS        fresh load of the RowBinary file, then settle
 *   embq ENGINE qps  DB SPEC CLASS THREADS SECONDS
 *   embq ENGINE dump DB SPEC [N]            answers of the first N queries of each class (default
 *                                           all), JSON lines in tsdbq's format, for check.py --dumps
 *   embq ENGINE full DB REPS THREADS [OUT]  whole range: count/sum/min/max, buckets of 3600 and 60
 *                                           samples; mean, sd, median, min and max of REPS after a
 *                                           warm-up; OUT gets the per-series digests
 *   embq tsdb twa DB REPS THREADS [OUT]     STEP time-weighted average by hour over the whole range;
 *                                           OUT gets the buckets (ref/twa.jsonl, relq's format)
 *   embq duckdb sql DB "SQL"                run one statement and print the result (EXPLAIN, sizes)
 * ENGINE is tsdb or duckdb; DB is a directory for tsdb and a file for DuckDB.
 * tsdb only, anywhere after ENGINE: --batch K hands a thread's series to tsdb_query_multi K at a time, with
 * per-series intervals (full, twa; K = 1: one plain query per series, as without it), and --threads T is
 * that call's thread cap (tsdb_multi_opts.threads; 0, the default: request_threads). A qps CLASS of the form
 * NAME.bK (raw.b10) runs NAME's windows K at a time through tsdb_query_multi with per-series starts and
 * ends; q/s then counts windows, and the latencies are of whole calls.
 *
 * Loading. tsdb takes the file as it is (time-major: every series at sample i, then i + 1),
 * THREADS slices of it through tsdb_insert in batches of 1 M rows, and settles with tsdb_optimize
 * (one part per partition). DuckDB keeps rows in insertion order and prunes with per-row-group
 * zone maps, so the rows are first sorted by (series, ts) in memory. That sort is timed on its own
 * and is a counting sort by series: within a series the file is already in time order. THREADS
 * appenders, each on its own connection, then append contiguous ranges of series as data chunks;
 * a CHECKPOINT follows, timed apart from the load.
 *
 * Queries. Every query thread has its own DuckDB connection with its statements prepared once
 * (EMBQ_NOPREP=1 sends literal SQL through duckdb_query instead); tsdb threads share one handle.
 * Results are read completely: raw values are summed, bucket counts added up, so both engines
 * hand every row to the caller. DuckDB runs with threads = 32 for every query; the whole-range
 * queries are one SQL statement each, while tsdb answers them one series at a time, from 1 or
 * from THREADS threads.
 */
#define _GNU_SOURCE
#include "duckdb.h"
#include "tsdb.h"

#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static const int64_t STEP[7] = {60000, 1000, 10000, 10000, 1, 1000, 1000}; /* kind = series % 7 */
static const char *KIND[7] = {"hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate", "flow_total"};

#pragma pack(push, 1)
typedef struct rbrow {
    uint32_t s;
    int64_t t;
    float v;
} rbrow;
#pragma pack(pop)

typedef struct q {
    char cls[8];
    uint32_t s;
    int64_t a, b, bucket; /* bucket 0: raw rows (point lookups are raw over [a, a + 1)) */
} q;

/* One answer, reduced the way tsdbq dump reduces it. */
typedef struct ans {
    uint64_t n;
    double sum;
    int64_t first, last;
    float v;
    tsdb_bucket *b;
    size_t nb, cap;
} ans;

static int is_duck;
static const char *dbpath;
static q *qs;
static size_t nq;
static size_t batch = 1;  /* --batch K, or K of a qps class NAME.bK */
static int mthreads;      /* --threads T: tsdb_multi_opts.threads */

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static void die(const char *what, const char *msg)
{
    fprintf(stderr, "embq: %s: %s\n", what, msg);
    exit(1);
}

static void load_spec(const char *path, const char *cls)
{
    FILE *f = fopen(path, "r");
    if (!f)
        die(path, "cannot open");
    q x;
    size_t cap = 0;
    while (fscanf(f, "%7s %u %" SCNd64 " %" SCNd64 " %" SCNd64, x.cls, &x.s, &x.a, &x.b, &x.bucket) == 5) {
        if (cls && strcmp(cls, x.cls))
            continue;
        if (nq == cap)
            qs = realloc(qs, (cap = cap ? cap * 2 : 1024) * sizeof *qs);
        qs[nq++] = x;
    }
    fclose(f);
}

static int cmpd(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static void add_bucket(ans *r, int64_t start, uint64_t count, double sum, double mn, double mx)
{
    if (r->nb == r->cap)
        r->b = realloc(r->b, (r->cap = r->cap ? r->cap * 2 : 64) * sizeof *r->b);
    r->b[r->nb++] = (tsdb_bucket){.start = start, .count = count, .sum = sum, .min = mn, .max = mx};
}

/* ------------------------------------------------------------------ tsdb */

static tsdb *tdb;

static tsdb *tsdb_open_or_die(const char *dir)
{
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc)
        die(dir, tsdb_strerror(rc));
    return db;
}

static int raw_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    ans *r = ctx;
    if (!r->n && n)
        r->first = ts[0];
    for (size_t i = 0; i < n; i++)
        r->sum += v[i];
    if (n) {
        r->last = ts[n - 1];
        r->v = v[n - 1];
    }
    r->n += n;
    return 0;
}

static void tsdb_run(const q *x, ans *r)
{
    int rc;
    if (x->bucket == 0)
        rc = tsdb_query_raw(tdb, x->s, x->a, x->b, raw_cb, r);
    else {
        tsdb_bucket *b;
        size_t nb;
        rc = tsdb_query_agg(tdb, x->s, x->a, x->b, x->bucket, &b, &nb);
        if (!rc) {
            free(r->b);
            r->b = b;
            r->nb = r->cap = nb;
            for (size_t i = 0; i < nb; i++)
                r->n += b[i].count;
        }
    }
    if (rc)
        die("query", tsdb_strerror(rc));
}

static int multi_count(void *ctx, const tsdb_series_result *r)
{
    if (r->status)
        die("query multi, series", tsdb_strerror(r->status));
    uint64_t *rows = ctx;
    if (r->buckets) /* rows in buckets, as tsdb_run counts them */
        for (size_t i = 0; i < r->n; i++)
            *rows += ((const tsdb_bucket *)r->buckets)[i].count;
    else
        *rows += r->n;
    return 0;
}

/* qs[at], qs[at + 1], ... (k of them, wrapping) as one tsdb_query_multi; returns the rows */
static uint64_t tsdb_batch(size_t at, size_t k)
{
    uint32_t *ss = malloc(k * sizeof *ss);
    int64_t *st = malloc(k * sizeof *st), *en = malloc(k * sizeof *en), *iv = malloc(k * sizeof *iv);
    for (size_t i = 0; i < k; i++) {
        const q *x = &qs[(at + i) % nq];
        ss[i] = x->s;
        st[i] = x->a;
        en[i] = x->b;
        iv[i] = x->bucket;
    }
    int agg = qs[at % nq].bucket != 0;
    tsdb_multi_query m = {.kind = agg ? TSDB_MULTI_BUCKETS : TSDB_MULTI_VALUES, .starts = st, .ends = en,
                          .intervals = agg ? iv : NULL};
    tsdb_multi_opts mo = {.threads = mthreads};
    uint64_t rows = 0;
    int rc = tsdb_query_multi(tdb, &m, ss, k, &mo, multi_count, &rows);
    if (rc)
        die("query multi", tsdb_strerror(rc));
    free(ss);
    free(st);
    free(en);
    free(iv);
    return rows;
}

typedef struct tload {
    const rbrow *rows;
    size_t n;
} tload;

static void *tsdb_loader(void *arg)
{
    tload *j = arg;
    const size_t batch = 1000000;
    uint32_t *s = malloc(batch * 4);
    int64_t *t = malloc(batch * 8);
    float *v = malloc(batch * 4);
    for (size_t off = 0; off < j->n; off += batch) {
        size_t m = j->n - off < batch ? j->n - off : batch;
        for (size_t k = 0; k < m; k++) {
            s[k] = j->rows[off + k].s;
            t[k] = j->rows[off + k].t;
            v[k] = j->rows[off + k].v;
        }
        int rc = tsdb_insert(tdb, s, t, v, m);
        if (rc)
            die("insert", tsdb_strerror(rc));
    }
    free(s);
    free(t);
    free(v);
    return NULL;
}

static void tsdb_load(const rbrow *rows, size_t n, int threads)
{
    struct stat st;
    if (stat(dbpath, &st) == 0)
        die(dbpath, "exists; the load wants a fresh directory");
    tdb = tsdb_open_or_die(dbpath);
    for (uint32_t i = 0; i < 100; i++) { /* ids in file order, as tsdbc-bench load creates them */
        char name[64];
        uint32_t id;
        snprintf(name, sizeof name, "%s.%u", KIND[i % 7], i);
        if (tsdb_series(tdb, name, &id) || id != i)
            die(name, "series id does not match the file's series number");
    }
    pthread_t th[256];
    tload jobs[256];
    double t0 = now();
    for (int i = 0; i < threads; i++) {
        size_t a = n * (size_t)i / (size_t)threads, b = n * (size_t)(i + 1) / (size_t)threads;
        jobs[i] = (tload){rows + a, b - a};
        pthread_create(&th[i], NULL, tsdb_loader, &jobs[i]);
    }
    for (int i = 0; i < threads; i++)
        pthread_join(th[i], NULL);
    double tl = now() - t0;
    tsdb_stats s;
    tsdb_get_stats(tdb, &s);
    printf("tsdb load: %zu rows in %.3f s = %.1f M rows/s (%d threads, batches of 1 M rows, fsync on); "
           "%" PRIu64 " parts, %" PRIu64 " merges so far\n",
           n, tl, n / tl / 1e6, threads, s.parts, s.merges);
    t0 = now();
    int rc = tsdb_optimize(tdb);
    if (rc)
        die("optimize", tsdb_strerror(rc));
    double to = now() - t0;
    tsdb_get_stats(tdb, &s);
    printf("tsdb optimize: %.3f s -> %" PRIu64 " parts in %" PRIu64 " partitions, %" PRIu64 " bytes = %.3f bytes/row\n",
           to, s.parts, s.partitions, s.bytes, (double)s.bytes / s.rows);
    tsdb_close(tdb);
}

/* ---------------------------------------------------------------- DuckDB */

static duckdb_database ddb;

/* threads = 32 for every query; the memory limit leaves room for the rest of a shared machine */
static void duck_open(const char *path)
{
    duckdb_config cfg;
    char *err = NULL;
    duckdb_create_config(&cfg);
    duckdb_set_config(cfg, "threads", getenv("DUCK_THREADS") ? getenv("DUCK_THREADS") : "32");
    duckdb_set_config(cfg, "memory_limit", getenv("DUCK_MEMORY") ? getenv("DUCK_MEMORY") : "24GB");
    if (duckdb_open_ext(path, &ddb, cfg, &err) == DuckDBError)
        die(path, err ? err : "cannot open");
    duckdb_destroy_config(&cfg);
}

static duckdb_connection duck_connect(void)
{
    duckdb_connection c;
    if (duckdb_connect(ddb, &c) == DuckDBError)
        die("duckdb", "cannot connect");
    return c;
}

static void duck_exec(duckdb_connection c, const char *sql)
{
    duckdb_result r;
    if (duckdb_query(c, sql, &r) == DuckDBError)
        die(sql, duckdb_result_error(&r));
    duckdb_destroy_result(&r);
}

/* The time bounds are TIMESTAMP_MS like the column, so the comparison needs no cast of the
 * column and the filter reaches the scan's zone maps. Buckets are aligned to the epoch. */
#define TSMS(p) "CAST(make_timestamp_ms(" p ") AS TIMESTAMP_MS)"
static const char *SQL_RAW = "SELECT ts, value FROM t WHERE series = $1 AND ts >= " TSMS("$2") " AND ts < " TSMS("$3")
                             " ORDER BY ts";
static const char *SQL_AGG = "SELECT epoch_ms(ts) // $4 * $4 AS b, count(*), sum(value), min(value), max(value) FROM t "
                             "WHERE series = $1 AND ts >= " TSMS("$2") " AND ts < " TSMS("$3") " GROUP BY b ORDER BY b";

typedef struct dctx {
    duckdb_connection c;
    duckdb_prepared_statement raw, agg;
    int noprep;
} dctx;

static duckdb_prepared_statement duck_prepare(duckdb_connection c, const char *sql)
{
    duckdb_prepared_statement p;
    if (duckdb_prepare(c, sql, &p) == DuckDBError)
        die(sql, duckdb_prepare_error(p));
    return p;
}

static dctx *duck_ctx(void)
{
    dctx *d = calloc(1, sizeof *d);
    d->c = duck_connect();
    d->noprep = getenv("EMBQ_NOPREP") != NULL;
    if (!d->noprep) {
        d->raw = duck_prepare(d->c, SQL_RAW);
        d->agg = duck_prepare(d->c, SQL_AGG);
    }
    return d;
}

static void duck_ctx_free(dctx *d)
{
    if (!d->noprep) {
        duckdb_destroy_prepare(&d->raw);
        duckdb_destroy_prepare(&d->agg);
    }
    duckdb_disconnect(&d->c);
    free(d);
}

/* literal SQL: the prepared text with the parameters spelled out */
static void literal(char *out, size_t len, const q *x)
{
    const char *tmpl = x->bucket ? SQL_AGG : SQL_RAW;
    char vals[4][32];
    snprintf(vals[0], 32, "%u", x->s);
    snprintf(vals[1], 32, "%" PRId64, x->a);
    snprintf(vals[2], 32, "%" PRId64, x->b);
    snprintf(vals[3], 32, "%" PRId64, x->bucket);
    size_t o = 0;
    for (const char *p = tmpl; *p && o + 32 < len; p++) {
        if (*p == '$' && p[1] >= '1' && p[1] <= '4') {
            o += (size_t)snprintf(out + o, len - o, "%s", vals[p[1] - '1']);
            p++;
        } else
            out[o++] = *p;
    }
    out[o] = 0;
}

static void duck_run(dctx *d, const q *x, ans *r)
{
    duckdb_result res;
    duckdb_state st;
    if (d->noprep) {
        char sql[1024];
        literal(sql, sizeof sql, x);
        st = duckdb_query(d->c, sql, &res);
    } else {
        duckdb_prepared_statement p = x->bucket ? d->agg : d->raw;
        duckdb_bind_int32(p, 1, (int32_t)x->s);
        duckdb_bind_int64(p, 2, x->a);
        duckdb_bind_int64(p, 3, x->b);
        if (x->bucket)
            duckdb_bind_int64(p, 4, x->bucket);
        st = duckdb_execute_prepared(p, &res);
    }
    if (st == DuckDBError)
        die("query", duckdb_result_error(&res));
    duckdb_data_chunk ch;
    while ((ch = duckdb_fetch_chunk(res)) != NULL) {
        idx_t n = duckdb_data_chunk_get_size(ch);
        if (x->bucket == 0) {
            const int64_t *ts = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 0));
            const float *v = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 1));
            raw_cb(r, ts, v, n);
        } else {
            const int64_t *b = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 0));
            const int64_t *c = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 1));
            const double *s = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 2));
            const float *mn = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 3));
            const float *mx = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 4));
            for (idx_t i = 0; i < n; i++) {
                add_bucket(r, b[i], (uint64_t)c[i], s[i], mn[i], mx[i]);
                r->n += (uint64_t)c[i];
            }
        }
        duckdb_destroy_data_chunk(&ch);
    }
    duckdb_destroy_result(&res);
}

typedef struct dload {
    duckdb_connection c;
    const uint32_t *s;
    const int64_t *t;
    const float *v;
    size_t n;
} dload;

static void *duck_appender(void *arg)
{
    dload *j = arg;
    duckdb_appender app;
    if (duckdb_appender_create(j->c, NULL, "t", &app) == DuckDBError)
        die("appender", duckdb_appender_error(app));
    duckdb_logical_type types[3] = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
                                    duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_MS),
                                    duckdb_create_logical_type(DUCKDB_TYPE_FLOAT)};
    duckdb_data_chunk ch = duckdb_create_data_chunk(types, 3);
    idx_t cap = duckdb_vector_size();
    for (size_t off = 0; off < j->n; off += cap) {
        size_t m = j->n - off < cap ? j->n - off : cap;
        int32_t *s = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 0));
        int64_t *t = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 1));
        float *v = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 2));
        for (size_t k = 0; k < m; k++)
            s[k] = (int32_t)j->s[off + k];
        memcpy(t, j->t + off, m * 8);
        memcpy(v, j->v + off, m * 4);
        duckdb_data_chunk_set_size(ch, m);
        if (duckdb_append_data_chunk(app, ch) == DuckDBError)
            die("append", duckdb_appender_error(app));
        duckdb_data_chunk_reset(ch);
    }
    if (duckdb_appender_close(app) == DuckDBError) /* flushes and commits */
        die("appender close", duckdb_appender_error(app));
    duckdb_appender_destroy(&app);
    duckdb_destroy_data_chunk(&ch);
    for (int i = 0; i < 3; i++)
        duckdb_destroy_logical_type(&types[i]);
    return NULL;
}

static long long file_size(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 ? (long long)st.st_size : 0;
}

static void duck_load(const rbrow *rows, size_t n, int threads)
{
    if (file_size(dbpath) > 0)
        die(dbpath, "exists; the load wants a fresh file");
    duck_open(dbpath);
    duckdb_connection c = duck_connect();
    /* no primary key: DuckDB would check it on every append, and it has no last-write-wins anyway */
    duck_exec(c, "CREATE TABLE t (series INTEGER, ts TIMESTAMP_MS, value FLOAT)");

    double t0 = now();
    size_t cnt[101] = {0};
    for (size_t i = 0; i < n; i++)
        cnt[rows[i].s + 1]++;
    for (int i = 1; i <= 100; i++)
        cnt[i] += cnt[i - 1];
    size_t start[101];
    memcpy(start, cnt, sizeof start);
    uint32_t *s = malloc(n * 4);
    int64_t *t = malloc(n * 8);
    float *v = malloc(n * 4);
    for (size_t i = 0; i < n; i++) { /* stable: each series stays in time order */
        size_t k = cnt[rows[i].s]++;
        s[k] = rows[i].s;
        t[k] = rows[i].t;
        v[k] = rows[i].v;
    }
    double tsort = now() - t0;

    pthread_t th[256];
    dload jobs[256];
    t0 = now();
    for (int i = 0; i < threads; i++) { /* contiguous ranges of series */
        size_t a = start[100 * i / threads], b = start[100 * (i + 1) / threads];
        jobs[i] = (dload){duck_connect(), s + a, t + a, v + a, b - a};
        pthread_create(&th[i], NULL, duck_appender, &jobs[i]);
    }
    for (int i = 0; i < threads; i++) {
        pthread_join(th[i], NULL);
        duckdb_disconnect(&jobs[i].c);
    }
    double tl = now() - t0;
    char wal[4096];
    snprintf(wal, sizeof wal, "%s.wal", dbpath);
    long long fb = file_size(dbpath), wb = file_size(wal);
    t0 = now();
    duck_exec(c, "CHECKPOINT");
    double tc = now() - t0;
    printf("duckdb sort by (series, ts), in memory, not part of the load: %.3f s\n", tsort);
    printf("duckdb load: %zu rows in %.3f s = %.1f M rows/s (%d appenders, data chunks of %llu rows); "
           "file %lld + WAL %lld bytes\n",
           n, tl, n / tl / 1e6, threads, (unsigned long long)duckdb_vector_size(), fb, wb);
    fb = file_size(dbpath);
    wb = file_size(wal);
    printf("duckdb checkpoint: %.3f s -> file %lld + WAL %lld bytes = %.3f bytes/row\n", tc, fb, wb,
           (double)(fb + wb) / n);
    duckdb_disconnect(&c);
    duckdb_close(&ddb);
    free(s);
    free(t);
    free(v);
}

/* ---------------------------------------------------------- both engines */

static void *thread_ctx(void) { return is_duck ? (void *)duck_ctx() : NULL; }
static void thread_ctx_free(void *c)
{
    if (is_duck)
        duck_ctx_free(c);
}
static void run_one(void *c, const q *x, ans *r)
{
    free(r->b);
    memset(r, 0, sizeof *r);
    if (is_duck)
        duck_run(c, x, r);
    else
        tsdb_run(x, r);
}

typedef struct job {
    int id, threads;
    double seconds, secs;
    double *lat;
    size_t n;
    uint64_t rows;
} job;

static void *qps_run(void *arg)
{
    job *j = arg;
    void *c = thread_ctx();
    ans r = {0};
    size_t cap = 0;
    double t0 = now();
    for (size_t i = (size_t)j->id; now() - t0 < j->seconds; i += (size_t)j->threads) {
        double a = now();
        if (batch > 1)
            j->rows += tsdb_batch(i * batch, batch);
        else {
            run_one(c, &qs[i % nq], &r);
            j->rows += r.n;
        }
        if (j->n == cap)
            j->lat = realloc(j->lat, (cap = cap ? cap * 2 : 4096) * sizeof(double));
        j->lat[j->n++] = now() - a;
    }
    j->secs = now() - t0;
    free(r.b);
    thread_ctx_free(c);
    return NULL;
}

static void qps(const char *cls, int threads, double seconds)
{
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
        total += jobs[i].n;
        rows += jobs[i].rows;
        secs = fmax(secs, jobs[i].secs);
    }
    double *all = malloc(total * sizeof *all);
    size_t m = 0;
    for (int i = 0; i < threads; i++)
        for (size_t k = 0; k < jobs[i].n; k++)
            all[m++] = jobs[i].lat[k];
    qsort(all, m, sizeof *all, cmpd);
    printf("%s %s c=%d: %zu queries in %.2f s = %.1f q/s, median %.3f ms, p99 %.3f ms%s (%" PRIu64 " rows)\n",
           is_duck ? "duckdb" : "tsdb", cls, threads, total * batch, secs, total * batch / secs, all[m / 2] * 1e3,
           all[(size_t)(m * 0.99)] * 1e3, batch > 1 ? " per call" : "", rows);
}

static void dump(size_t per)
{
    void *c = thread_ctx();
    ans r = {0};
    const char *seen = "";
    size_t k = 0;
    for (size_t i = 0; i < nq; i++) {
        if (strcmp(seen, qs[i].cls)) {
            seen = qs[i].cls;
            k = 0;
        }
        if (k++ >= per)
            continue;
        run_one(c, &qs[i], &r);
        printf("{\"i\":%zu,\"cls\":\"%s\"", i, qs[i].cls);
        if (qs[i].bucket == 0)
            printf(",\"n\":%" PRIu64 ",\"sum\":%.17g,\"first\":%" PRId64 ",\"last\":%" PRId64 ",\"v\":%.9g}\n", r.n,
                   r.sum, r.first, r.last, r.v);
        else {
            printf(",\"b\":[");
            for (size_t x = 0; x < r.nb; x++)
                printf("%s[%" PRId64 ",%" PRIu64 ",%.17g,%.9g,%.9g]", x ? "," : "", r.b[x].start, r.b[x].count,
                       r.b[x].sum, r.b[x].min, r.b[x].max);
            printf("]}\n");
        }
    }
    free(r.b);
    thread_ctx_free(c);
}

/* ---- whole range: digest per series = count, sum, sum of mins, sum of maxes, buckets */

typedef double digest[100][5];

static const char *duck_full_sql(int64_t nsamp, char *buf, size_t len)
{
    if (nsamp == 0)
        return "SELECT series, NULL::BIGINT AS b, count(*), sum(value), min(value), max(value) FROM t "
               "GROUP BY series ORDER BY series";
    snprintf(buf, len,
             "SELECT series, epoch_ms(ts) // ((CASE series %% 7 WHEN 0 THEN 60000 WHEN 4 THEN 1 WHEN 2 THEN 10000 "
             "WHEN 3 THEN 10000 ELSE 1000 END) * %" PRId64 ") AS b, count(*), sum(value), min(value), max(value) "
             "FROM t GROUP BY series, b ORDER BY series, b",
             nsamp);
    return buf;
}

static void duck_full(dctx *d, int64_t nsamp, digest out)
{
    char buf[1024];
    duckdb_result res;
    memset(out, 0, sizeof(digest));
    if (duckdb_query(d->c, duck_full_sql(nsamp, buf, sizeof buf), &res) == DuckDBError)
        die("full", duckdb_result_error(&res));
    duckdb_data_chunk ch;
    while ((ch = duckdb_fetch_chunk(res)) != NULL) {
        idx_t n = duckdb_data_chunk_get_size(ch);
        const int32_t *s = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 0));
        const int64_t *c = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 2));
        const double *sum = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 3));
        const float *mn = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 4));
        const float *mx = duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch, 5));
        for (idx_t i = 0; i < n; i++) {
            double *o = out[s[i]];
            o[0] += (double)c[i];
            o[1] += sum[i];
            o[2] += mn[i];
            o[3] += mx[i];
            o[4] += 1;
        }
        duckdb_destroy_data_chunk(&ch);
    }
    duckdb_destroy_result(&res);
}

typedef struct fjob {
    int id, threads, twa;
    int64_t nsamp;
    double (*out)[5];
} fjob;

static tsdb_twa_bucket *twa_out[100];
static size_t twa_n[100];

static void full_keep(const fjob *j, uint32_t s, const void *b, size_t nb)
{
    if (j->twa) {
        free(twa_out[s]);
        twa_out[s] = malloc(nb * sizeof(tsdb_twa_bucket) + 1);
        if (nb)
            memcpy(twa_out[s], b, nb * sizeof(tsdb_twa_bucket));
        twa_n[s] = nb;
        return;
    }
    double *o = j->out[s];
    memset(o, 0, 5 * sizeof *o);
    for (size_t i = 0; i < nb; i++) {
        const tsdb_bucket *x = (const tsdb_bucket *)b + i;
        o[0] += (double)x->count;
        o[1] += x->sum;
        o[2] += x->min;
        o[3] += x->max;
        o[4] += 1;
    }
}

static int64_t full_interval(const fjob *j, uint32_t s) { return j->twa ? 3600000 : j->nsamp * STEP[s % 7]; }

static void *tsdb_full_run(void *arg)
{
    fjob *j = arg;
    tsdb_twa_opts to = {.method = TSDB_TWA_STEP};
    uint32_t ss[100];
    int64_t iv[100];
    size_t k = 0;
    for (uint32_t s = (uint32_t)j->id; s < 100; s += (uint32_t)j->threads) {
        if (batch > 1) {
            ss[k] = s;
            iv[k++] = full_interval(j, s);
            continue;
        }
        void *b;
        size_t nb;
        int rc = j->twa ? tsdb_query_twa(tdb, s, INT64_MIN, INT64_MAX, 3600000, &to, (tsdb_twa_bucket **)&b, &nb)
                        : tsdb_query_agg(tdb, s, INT64_MIN, INT64_MAX, full_interval(j, s), (tsdb_bucket **)&b, &nb);
        if (rc)
            die(j->twa ? "twa" : "agg", tsdb_strerror(rc));
        full_keep(j, s, b, nb);
        free(b);
    }
    for (size_t at = 0; at < k; at += batch) {
        size_t m = k - at < batch ? k - at : batch;
        tsdb_multi_query mq = {.kind = j->twa ? TSDB_MULTI_TWA : TSDB_MULTI_BUCKETS, .start = INT64_MIN,
                               .end = INT64_MAX, .intervals = iv + at, .twa = to};
        tsdb_multi_opts mo = {.threads = mthreads};
        tsdb_series_result *r;
        int rc = tsdb_query_multi_collect(tdb, &mq, ss + at, m, &mo, &r);
        if (rc)
            die("query multi", tsdb_strerror(rc));
        for (size_t i = 0; i < m; i++) {
            if (r[i].status)
                die("query multi, series", tsdb_strerror(r[i].status));
            full_keep(j, ss[at + i], r[i].buckets, r[i].n);
        }
        tsdb_series_results_free(r, m);
    }
    return NULL;
}

/* REPS timed runs after a warm-up (run 0); prints mean, sd, median, min and max */
static void full_time(const char *name, int reps, int threads, int twa, int64_t nsamp, dctx *d, digest dg)
{
    double t[101], sum = 0, ss = 0;
    if (reps < 1 || reps > 100 || threads < 1 || threads > 256)
        die("full", "REPS from 1 to 100, THREADS from 1 to 256");
    for (int r = 0; r <= reps; r++) {
        double a = now();
        if (is_duck)
            duck_full(d, nsamp, dg);
        else {
            pthread_t th[256];
            fjob jobs[256];
            for (int i = 0; i < threads; i++) {
                jobs[i] = (fjob){.id = i, .threads = threads, .twa = twa, .nsamp = nsamp, .out = dg};
                pthread_create(&th[i], NULL, tsdb_full_run, &jobs[i]);
            }
            for (int i = 0; i < threads; i++)
                pthread_join(th[i], NULL);
        }
        if (r)
            t[r - 1] = now() - a;
    }
    for (int r = 0; r < reps; r++)
        sum += t[r];
    double mean = sum / reps;
    for (int r = 0; r < reps; r++)
        ss += (t[r] - mean) * (t[r] - mean);
    qsort(t, (size_t)reps, sizeof *t, cmpd);
    char how[64];
    if (is_duck)
        snprintf(how, sizeof how, "one query");
    else
        snprintf(how, sizeof how, "%d thread%s batch %zu threads %d", threads, threads == 1 ? "" : "s", batch,
                 mthreads);
    printf("%s %s %-30s %s: mean %.4f s sd %.4f, median %.4f (min %.4f, max %.4f, %d runs)\n",
           is_duck ? "duckdb" : "tsdb", twa ? "twa " : "full", name, how, mean,
           reps > 1 ? sqrt(ss / (reps - 1)) : 0.0, t[reps / 2], t[0], t[reps - 1], reps);
}

static void full(int reps, int threads, const char *outpath)
{
    static const int64_t sizes[3] = {0, 3600, 60};
    static const char *names[3] = {"count/sum/min/max per series", "buckets of 3600 samples", "buckets of 60 samples"};
    FILE *out = outpath ? fopen(outpath, "w") : NULL;
    dctx *d = is_duck ? duck_ctx() : NULL;
    static digest dg;
    for (int k = 0; k < 3; k++) {
        full_time(names[k], reps, threads, 0, sizes[k], d, dg);
        if (out)
            for (int s = 0; s < 100; s++)
                fprintf(out, "{\"full\":%" PRId64 ",\"s\":%d,\"d\":[%.17g,%.17g,%.17g,%.17g,%.17g]}\n", sizes[k], s,
                        dg[s][0], dg[s][1], dg[s][2], dg[s][3], dg[s][4]);
    }
    if (d)
        duck_ctx_free(d);
    if (out)
        fclose(out);
}

static void twa(int reps, int threads, const char *outpath)
{
    static digest dg;
    full_time("STEP by hour", reps, threads, 1, 0, NULL, dg);
    size_t tot = 0;
    for (int s = 0; s < 100; s++)
        tot += twa_n[s];
    printf("twa: %zu buckets\n", tot);
    if (!outpath)
        return;
    FILE *f = fopen(outpath, "w");
    if (!f)
        die(outpath, "cannot open");
    for (int s = 0; s < 100; s++) {
        fprintf(f, "{\"s\":%d,\"b\":[", s);
        for (size_t i = 0; i < twa_n[s]; i++)
            fprintf(f, "%s[%ld,%.17g,%ld,%lu]", i ? "," : "", (long)twa_out[s][i].start, twa_out[s][i].area,
                    (long)twa_out[s][i].covered_ms, (unsigned long)twa_out[s][i].count);
        fprintf(f, "]}\n");
    }
    fclose(f);
}

static void sql(const char *text)
{
    duckdb_connection c = duck_connect();
    duckdb_result r;
    if (duckdb_query(c, text, &r) == DuckDBError)
        die(text, duckdb_result_error(&r));
    idx_t cols = duckdb_column_count(&r), rows = duckdb_row_count(&r);
    for (idx_t i = 0; i < rows; i++) {
        for (idx_t k = 0; k < cols; k++) {
            char *v = duckdb_value_varchar(&r, k, i);
            printf("%s%s", k ? "\t" : "", v ? v : "NULL");
            duckdb_free(v);
        }
        printf("\n");
    }
    duckdb_destroy_result(&r);
    duckdb_disconnect(&c);
}

/* removes --batch K and --threads T from argv */
static int take_opts(int *argc, char **argv)
{
    int o = 1;
    for (int i = 1; i < *argc; i++) {
        if (i + 1 < *argc && (!strcmp(argv[i], "--batch") || !strcmp(argv[i], "--threads"))) {
            long v = atol(argv[i + 1]);
            if (argv[i][2] == 'b' ? v < 1 || v > 100 : v < 0 || v > 1024) {
                fprintf(stderr, "embq: %s %s: out of range\n", argv[i], argv[i + 1]);
                return -1;
            }
            if (argv[i][2] == 'b')
                batch = (size_t)v;
            else
                mthreads = (int)v;
            i++;
            continue;
        }
        argv[o++] = argv[i];
    }
    *argc = o;
    return 0;
}

int main(int argc, char **argv)
{
    if (take_opts(&argc, argv))
        return 2;
    if (argc < 4 || (strcmp(argv[1], "tsdb") && strcmp(argv[1], "duckdb"))) {
        fprintf(stderr, "usage: embq tsdb|duckdb load DB DATA THREADS | qps DB SPEC CLASS THREADS SECONDS |\n"
                        "       dump DB SPEC [N] | full DB REPS THREADS [OUT] | twa DB REPS THREADS [OUT] (tsdb) |\n"
                        "       sql DB SQL (duckdb); tsdb: --batch K, --threads T, qps class NAME.bK\n");
        return 2;
    }
    is_duck = !strcmp(argv[1], "duckdb");
    if (is_duck && (batch > 1 || mthreads))
        die("duckdb", "--batch and --threads are for tsdb");
    const char *cmd = argv[2];
    dbpath = argv[3];
    if (!strcmp(cmd, "load") && argc == 6) {
        int fd = open(argv[4], O_RDONLY);
        struct stat st;
        if (fd < 0 || fstat(fd, &st) != 0)
            die(argv[4], "cannot open");
        const rbrow *rows = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
        size_t n = (size_t)st.st_size / sizeof(rbrow);
        if (is_duck)
            duck_load(rows, n, atoi(argv[5]));
        else
            tsdb_load(rows, n, atoi(argv[5]));
        return 0;
    }
    if (is_duck) {
        duck_open(dbpath);
        fprintf(stderr, "duckdb %s\n", duckdb_library_version());
    } else
        tdb = tsdb_open_or_die(dbpath);
    if (!strcmp(cmd, "qps") && argc == 8) {
        char base[16];
        snprintf(base, sizeof base, "%s", argv[5]);
        char *dot = strstr(base, ".b");
        if (dot) {
            long k = atol(dot + 2);
            if (is_duck || k < 1 || k > 65536)
                die(argv[5], "class NAME.bK: tsdb only, K from 1 to 65536");
            batch = (size_t)k;
            *dot = 0;
        }
        load_spec(argv[4], base);
        if (!nq)
            die(argv[5], "no queries of that class");
        qps(argv[5], atoi(argv[6]), atof(argv[7]));
    } else if (!strcmp(cmd, "dump") && argc >= 5) {
        load_spec(argv[4], NULL);
        dump(argc > 5 ? strtoull(argv[5], NULL, 10) : SIZE_MAX);
    } else if (!strcmp(cmd, "full") && argc >= 6)
        full(atoi(argv[4]), atoi(argv[5]), argc > 6 ? argv[6] : NULL);
    else if (!strcmp(cmd, "twa") && argc >= 6 && !is_duck)
        twa(atoi(argv[4]), atoi(argv[5]), argc > 6 ? argv[6] : NULL);
    else if (!strcmp(cmd, "sql") && argc == 5 && is_duck)
        sql(argv[4]);
    else {
        fprintf(stderr, "embq: bad arguments\n");
        return 2;
    }
    if (is_duck)
        duckdb_close(&ddb);
    else
        tsdb_close(tdb);
    return 0;
}
