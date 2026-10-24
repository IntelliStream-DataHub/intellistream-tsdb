/*
 * Benchmark: insert throughput, merge cost, compression and query speed on
 * the sensor workloads of datasets.h.
 *
 *   tsdb-bench --dir /path [--series 100] [--points 1000000] [--dataset mixed]
 *   tsdb-bench --export data.rowbinary ...   writes the same rows for ClickHouse
 *   tsdb-bench --dir /path --load data.rowbinary [--threads 4] [--batch 1000000] [--no-fsync]
 *                                           loads an exported file in-process (the embedded counterpart of
 *                                           tsdbc-bench load), then checks count, sum and every
 *                                           (timestamp, value) of every series against the file
 *   tsdb-bench --dir /path --verify data.rowbinary   the same check on an existing database
 *   tsdb-bench --dir /path --qps THREADS [--series N] [--points N] [--seconds 3]
 *                                           the random windows of tsdbc-bench query on an existing database,
 *                                           in-process, with the same windows, seeds and output
 *
 * bench/clients/run.sh runs both next to the TCP clients.
 *
 * Query windows are given in samples, so every sensor kind gets comparable
 * work: a "3600 sample" window is an hour of 1 s data, 1.2 s of vibration.
 */
#include "cbench.h"
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <getopt.h>
#include <math.h>
#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct cfg {
    const char *dir, *export_path;
    uint32_t series;
    uint64_t points; /* per series */
    int dataset;     /* a ds_kind, or -1 for mixed */
    size_t batch;    /* rows per insert call */
    int threads;
    int zstd;
    int fsync;
    bool keep, query_only;
    int query_threads; /* 0: library default */
    const char *load_path;
    const char *verify_path; /* --verify */
    int qps;        /* --qps: threads of the random-window queries; 0: off */
    double seconds; /* per window class, --qps */
    bool optimize;  /* --load: merge after the load, as the server benchmark does */
} cfg;

static const int64_t T0 = 1704067200000; /* 2024-01-01 */

static ds_kind kind_of(const cfg *c, uint32_t s)
{
    return c->dataset >= 0 ? (ds_kind)c->dataset : (ds_kind)(s % (DS_KINDS - 1)); /* mixed: no random */
}

static int64_t step_of(const cfg *c, uint32_t s) { return ds_info[kind_of(c, s)].step_ms; }

typedef struct wjob {
    const cfg *c;
    tsdb *db;
    FILE *out; /* export instead of insert */
    uint32_t s0, s1;
    uint64_t rows;
} wjob;

/* Each writer owns a range of series and sends them time-major, the way a
 * collector does: every series at sample i, then sample i + 1. */
static void *writer(void *arg)
{
    wjob *j = arg;
    const cfg *c = j->c;
    uint32_t ns = j->s1 - j->s0;
    ds_state *st = calloc(ns, sizeof *st);
    for (uint32_t s = 0; s < ns; s++)
        ds_init(&st[s], kind_of(c, j->s0 + s), j->s0 + s, T0);
    uint32_t *sv = malloc(c->batch * sizeof *sv);
    int64_t *tv = malloc(c->batch * sizeof *tv);
    float *vv = malloc(c->batch * sizeof *vv);
    uint64_t window = c->batch / ns ? c->batch / ns : 1;
    for (uint64_t i0 = 0; i0 < c->points; i0 += window) {
        size_t n = 0;
        uint64_t i1 = i0 + window < c->points ? i0 + window : c->points;
        for (uint64_t i = i0; i < i1; i++)
            for (uint32_t s = 0; s < ns; s++) {
                sv[n] = j->s0 + s;
                ds_next(&st[s], &tv[n], &vv[n]);
                n++;
            }
        if (j->out) {
            for (size_t k = 0; k < n; k++) { /* RowBinary: UInt32, DateTime64(3), Float32 */
                fwrite(&sv[k], 4, 1, j->out);
                fwrite(&tv[k], 8, 1, j->out);
                fwrite(&vv[k], 4, 1, j->out);
            }
        } else {
            int rc = tsdb_insert(j->db, sv, tv, vv, n);
            if (rc) {
                fprintf(stderr, "insert: %s\n", tsdb_strerror(rc));
                exit(1);
            }
        }
        j->rows += n;
    }
    free(sv);
    free(tv);
    free(vv);
    free(st);
    return NULL;
}

static int count_rows(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    double *acc = ctx;
    acc[0] += (double)n;
    acc[1] += v[n - 1]; /* touch the data */
    (void)ts;
    return 0;
}

typedef struct qjob {
    tsdb *db;
    const cfg *c;
    int kind;              /* 0 raw, 1 agg */
    uint64_t window, bucket; /* in samples */
    double seconds;
    uint64_t queries, rows;
    int seed;
} qjob;

static void *query_loop(void *arg)
{
    qjob *q = arg;
    uint64_t r = ds_mix((uint64_t)q->seed + 77);
    double t0 = now_s();
    while (now_s() - t0 < q->seconds) {
        for (int k = 0; k < 64; k++) {
            r = ds_mix(r);
            uint32_t s = (uint32_t)(r % q->c->series);
            int64_t step = step_of(q->c, s);
            uint64_t span = q->c->points > q->window ? q->c->points - q->window : 1;
            int64_t start = T0 + (int64_t)((r >> 20) % span) * step;
            int64_t end = start + (int64_t)q->window * step;
            if (q->kind == 0) {
                double acc[2] = {0, 0};
                tsdb_query_raw(q->db, s, start, end, count_rows, acc);
                q->rows += (uint64_t)acc[0];
            } else {
                tsdb_bucket *b;
                size_t nb;
                tsdb_query_agg(q->db, s, start, end, (int64_t)q->bucket * step, &b, &nb);
                for (size_t i = 0; i < nb; i++)
                    q->rows += b[i].count;
                free(b);
            }
            q->queries++;
        }
    }
    q->seconds = now_s() - t0;
    return NULL;
}

static void run_qps(tsdb *db, const cfg *c, const char *label, int kind, uint64_t window, uint64_t bucket,
                    int threads)
{
    qjob *q = calloc((size_t)threads, sizeof *q);
    pthread_t *th = calloc((size_t)threads, sizeof *th);
    for (int i = 0; i < threads; i++) {
        q[i] = (qjob){db, c, kind, window, bucket, 2.0, 0, 0, i};
        pthread_create(&th[i], NULL, query_loop, &q[i]);
    }
    uint64_t nq = 0, rows = 0;
    double secs = 0;
    for (int i = 0; i < threads; i++) {
        pthread_join(th[i], NULL);
        nq += q[i].queries;
        rows += q[i].rows;
        secs = fmax(secs, q[i].seconds);
    }
    printf("  %-46s %3d thr %10.0f q/s  %8.1f us/q  %7.1f Mrows/s\n", label, threads, nq / secs,
           secs * threads / nq * 1e6, rows / secs / 1e6);
    free(q);
    free(th);
}

/* ---------------------------------------------- load / qps modes (see cbench.h) */

typedef struct lload {
    tsdb *db;
    const rbrow *rows;
    size_t n, batch;
    int rc;
} lload;

static void *file_loader(void *arg)
{
    lload *j = arg;
    uint32_t *s = malloc(j->batch * sizeof *s);
    int64_t *t = malloc(j->batch * sizeof *t);
    float *v = malloc(j->batch * sizeof *v);
    for (size_t off = 0; off < j->n && j->rc == 0; off += j->batch) {
        size_t m = j->n - off < j->batch ? j->n - off : j->batch;
        for (size_t k = 0; k < m; k++) {
            s[k] = j->rows[off + k].s;
            t[k] = j->rows[off + k].t;
            v[k] = j->rows[off + k].v;
        }
        j->rc = tsdb_insert(j->db, s, t, v, m);
    }
    free(s);
    free(t);
    free(v);
    return NULL;
}

static int verify_agg(void *ctx, uint32_t series, int64_t start, int64_t end, int64_t interval, tsdb_bucket **out,
                      size_t *nout)
{
    return tsdb_query_agg(ctx, series, start, end, interval, out, nout);
}

static int verify_raw(void *ctx, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *fnctx)
{
    return tsdb_query_raw(ctx, series, start, end, fn, fnctx);
}

/* The database in c->dir holds exactly the rows of the file (count, sum, every timestamp and value). */
static int mode_verify(const cfg *c)
{
    cb_file f;
    if (cb_file_open(c->verify_path, &f) != 0)
        return 1;
    struct stat st;
    if (stat(c->dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "verify: %s is not a database directory\n", c->dir);
        return 1;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    int rc = tsdb_open(c->dir, &o, &db);
    if (rc) {
        fprintf(stderr, "open: %s\n", tsdb_strerror(rc));
        return 1;
    }
    cb_expect e;
    cb_expect_make(&f, &e);
    cb_source src = {db, verify_agg, verify_raw};
    rc = cb_expect_verify_all(&e, f.n, &src, "in the database");
    tsdb_close(db);
    cb_expect_free(&e);
    return rc;
}

/* rm -rf DIR without a shell, so that a path with a quote in it is just a path. */
static int remove_tree(const char *dir)
{
    pid_t pid = fork();
    if (pid < 0)
        return -1;
    if (pid == 0) {
        execlp("rm", "rm", "-rf", "--", dir, (char *)NULL);
        _exit(127);
    }
    int st;
    if (waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "could not remove %s\n", dir);
        return -1;
    }
    return 0;
}

static int mode_load(const cfg *c)
{
    cb_file f;
    if (cb_file_open(c->load_path, &f) != 0)
        return 1;
    if (remove_tree(c->dir) != 0)
        return 1;
    tsdb_options o;
    tsdb_options_init(&o);
    o.zstd_level = c->zstd;
    o.fsync = c->fsync;
    tsdb *db;
    int rc = tsdb_open(c->dir, &o, &db);
    if (rc) {
        fprintf(stderr, "open: %s\n", tsdb_strerror(rc));
        return 1;
    }
    cb_expect e;
    cb_expect_make(&f, &e);
    for (uint32_t i = 0; i < e.nseries; i++) { /* named as tsdbc-bench names them; ids 0.. in order */
        char name[64];
        uint32_t id;
        snprintf(name, sizeof name, "%s.%u", ds_info[i % (DS_KINDS - 1)].name, i);
        rc = tsdb_series(db, name, &id);
        if (rc || id != i) { /* verification and the query benchmarks rely on ids equal to the file's indexes */
            fprintf(stderr, "series %s: %s (id %u, expected %u)\n", name, rc ? tsdb_strerror(rc) : "wrong id", id, i);
            return 1;
        }
    }
    int nt = c->threads;
    lload *jobs = calloc((size_t)nt, sizeof *jobs);
    pthread_t *th = calloc((size_t)nt, sizeof *th);
    double t0 = now_s(), cpu0 = cb_cpu_s();
    for (int i = 0; i < nt; i++) {
        size_t a = f.n * (size_t)i / (size_t)nt, b = f.n * (size_t)(i + 1) / (size_t)nt;
        jobs[i] = (lload){db, f.rows + a, b - a, c->batch, 0};
        pthread_create(&th[i], NULL, file_loader, &jobs[i]);
    }
    for (int i = 0; i < nt; i++) {
        pthread_join(th[i], NULL);
        if (jobs[i].rc) {
            fprintf(stderr, "insert: %s\n", tsdb_strerror(jobs[i].rc));
            return 1;
        }
    }
    double dt = now_s() - t0, cpu = cb_cpu_s() - cpu0;
    char extra[96];
    snprintf(extra, sizeof extra, ", batches of %zu, fsync %s, zstd %d", c->batch, c->fsync ? "on" : "off", c->zstd);
    cb_report_load(c->fsync ? "embedded-fsync" : "embedded-nofsync", nt, 0, f.n, dt, 0, cpu, extra);
    if (c->optimize) {
        t0 = now_s();
        rc = tsdb_optimize(db);
        printf("optimize %.2f s\n", now_s() - t0);
        if (rc) {
            fprintf(stderr, "optimize: %s\n", tsdb_strerror(rc));
            return 1;
        }
    }
    cb_source vsrc = {db, verify_agg, verify_raw};
    int vrc = cb_expect_verify_all(&e, f.n, &vsrc, "in the database");
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    printf("storage %.2f MiB, %.3f bytes/row\n", st.bytes / 1048576.0, (double)st.bytes / (double)st.rows);
    tsdb_close(db);
    cb_expect_free(&e);
    free(jobs);
    free(th);
    return vrc;
}

typedef struct qwjob {
    tsdb *db;
    const cfg *c;
    const cb_class *k;
    int seed;
    uint64_t queries, rows;
    double secs;
} qwjob;

static void *qw_loop(void *arg)
{
    qwjob *q = arg;
    uint64_t r = cb_seed(q->seed);
    double t0 = now_s();
    while (now_s() - t0 < q->c->seconds) {
        for (int k = 0; k < 16; k++) {
            uint32_t s;
            int64_t start, end;
            cb_window(&r, q->c->series, q->c->points, q->k->window, &s, &start, &end);
            if (!q->k->agg) {
                double acc[2] = {0, 0};
                int rc = tsdb_query_raw(q->db, s, start, end, count_rows, acc);
                if (rc != 0) {
                    fprintf(stderr, "query: series %u: %s\n", s, tsdb_strerror(rc));
                    exit(1);
                }
                q->rows += (uint64_t)acc[0];
            } else {
                tsdb_bucket *b;
                size_t nb;
                int rc = tsdb_query_agg(q->db, s, start, end, (int64_t)q->k->bucket * cb_step_ms(s), &b, &nb);
                if (rc != 0) {
                    fprintf(stderr, "query: series %u: %s\n", s, tsdb_strerror(rc));
                    exit(1);
                }
                for (size_t i = 0; i < nb; i++)
                    q->rows += b[i].count;
                free(b);
            }
            q->queries++;
        }
    }
    q->secs = now_s() - t0;
    return NULL;
}

static int mode_qps(const cfg *c)
{
    struct stat dst;
    if (stat(c->dir, &dst) != 0 || !S_ISDIR(dst.st_mode)) { /* tsdb_open would create an empty database */
        fprintf(stderr, "qps: %s is not a database directory\n", c->dir);
        return 1;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    if (c->query_threads > 0)
        o.query_threads = c->query_threads;
    tsdb *db;
    int rc = tsdb_open(c->dir, &o, &db);
    if (rc) {
        fprintf(stderr, "open: %s\n", tsdb_strerror(rc));
        return 1;
    }
    int nt = c->qps;
    qwjob *q = calloc((size_t)nt, sizeof *q);
    pthread_t *th = calloc((size_t)nt, sizeof *th);
    for (int k = 0; k < CB_NCLASSES; k++) {
        double cpu0 = cb_cpu_s();
        for (int i = 0; i < nt; i++) {
            q[i] = (qwjob){db, c, &cb_classes[k], i, 0, 0, 0};
            pthread_create(&th[i], NULL, qw_loop, &q[i]);
        }
        uint64_t nq = 0, rows = 0;
        double secs = 0;
        for (int i = 0; i < nt; i++) {
            pthread_join(th[i], NULL);
            nq += q[i].queries;
            rows += q[i].rows;
            secs = fmax(secs, q[i].secs);
        }
        cb_report_query("embedded", &cb_classes[k], "thr", nt, nq, rows, secs, cb_cpu_s() - cpu0);
        if (rows == 0) {
            fprintf(stderr, "qps: class %s returned no rows: the database is empty or does not hold the data set\n",
                    cb_classes[k].id);
            return 1;
        }
    }
    tsdb_close(db);
    free(q);
    free(th);
    return 0;
}

int main(int argc, char **argv)
{
    cfg c = {.series = 100, .points = 1000000, .dataset = -1, .batch = 1000000, .threads = 4, .zstd = 1, .fsync = 1,
             .seconds = CB_DEFAULT_SECONDS};
    static struct option opts[] = {
        {"dir", required_argument, 0, 'd'},     {"series", required_argument, 0, 's'},
        {"points", required_argument, 0, 'p'},  {"dataset", required_argument, 0, 'v'},
        {"batch", required_argument, 0, 'b'},   {"threads", required_argument, 0, 't'},
        {"zstd", required_argument, 0, 'z'},    {"no-fsync", no_argument, 0, 'F'},
        {"keep", no_argument, 0, 'k'},          {"query-only", no_argument, 0, 'q'},
        {"export", required_argument, 0, 'e'},  {"query-threads", required_argument, 0, 'Q'},
        {"load", required_argument, 0, 'L'},    {"qps", required_argument, 0, 'S'},
        {"seconds", required_argument, 0, 'T'}, {"optimize", no_argument, 0, 'O'},
        {"verify", required_argument, 0, 'V'},
        {0, 0, 0, 0}};
    int ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': c.dir = optarg; break;
        case 's': c.series = (uint32_t)atoi(optarg); break;
        case 'p': c.points = strtoull(optarg, NULL, 10); break;
        case 'v':
            c.dataset = strcmp(optarg, "mixed") == 0 ? -1 : ds_parse(optarg);
            if (c.dataset < -1 || (c.dataset == -1 && strcmp(optarg, "mixed") != 0)) {
                fprintf(stderr, "unknown dataset %s\n", optarg);
                return 2;
            }
            break;
        case 'b': c.batch = strtoull(optarg, NULL, 10); break;
        case 't': c.threads = atoi(optarg); break;
        case 'z': c.zstd = atoi(optarg); break;
        case 'F': c.fsync = 0; break;
        case 'k': c.keep = true; break;
        case 'q': c.query_only = c.keep = true; break;
        case 'e': c.export_path = optarg; break;
        case 'Q': c.query_threads = atoi(optarg); break;
        case 'L': c.load_path = optarg; break;
        case 'S': c.qps = atoi(optarg); break;
        case 'T': c.seconds = atof(optarg); break;
        case 'O': c.optimize = true; break;
        case 'V': c.verify_path = optarg; break;
        default:
            fprintf(stderr,
                    "usage: tsdb-bench --dir DIR [--series N] [--points N] [--dataset mixed|hdd_temp|cpu_temp|...]\n"
                    "       [--batch ROWS] [--threads N] [--zstd LEVEL] [--no-fsync] [--keep] [--query-only]\n"
                    "       tsdb-bench --export FILE [--series N] [--points N] [--dataset ...]\n"
                    "       tsdb-bench --dir DIR --load FILE [--threads N] [--batch ROWS] [--no-fsync] [--optimize]\n"
                    "       tsdb-bench --dir DIR --verify FILE\n"
                    "       tsdb-bench --dir DIR --qps THREADS [--series N] [--points N] [--seconds S]\n");
            return 2;
        }
    }
    if ((!c.dir && !c.export_path) || c.series == 0 || c.points == 0 || c.threads < 1)
        return 2;
    if (c.verify_path && c.dir)
        return mode_verify(&c);
    if (c.load_path && c.dir)
        return mode_load(&c);
    if (c.qps > 0 && c.dir)
        return mode_qps(&c);
    if ((uint32_t)c.threads > c.series)
        c.threads = (int)c.series;
    uint64_t total = (uint64_t)c.series * c.points;

    wjob *jobs = calloc((size_t)c.threads, sizeof *jobs);
    pthread_t *th = calloc((size_t)c.threads, sizeof *th);
    if (c.export_path) {
        /* one thread, so the file is time-major across all series */
        FILE *f = fopen(c.export_path, "wb");
        if (!f)
            return 1;
        jobs[0] = (wjob){&c, NULL, f, 0, c.series, 0};
        writer(&jobs[0]);
        fclose(f);
        printf("exported %" PRIu64 " rows to %s\n", jobs[0].rows, c.export_path);
        return 0;
    }

    char cmd[1024];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", c.dir);
    if (!c.query_only && system(cmd) != 0)
        return 1;
    tsdb_options o;
    tsdb_options_init(&o);
    o.zstd_level = c.zstd;
    o.fsync = c.fsync;
    if (c.query_threads > 0)
        o.query_threads = c.query_threads;
    tsdb *db;
    int rc = tsdb_open(c.dir, &o, &db);
    if (rc) {
        fprintf(stderr, "open: %s\n", tsdb_strerror(rc));
        return 1;
    }
    for (uint32_t s = 0; s < c.series && !c.query_only; s++) {
        char name[64];
        uint32_t id;
        snprintf(name, sizeof name, "%s.%u", ds_info[kind_of(&c, s)].name, s);
        tsdb_series(db, name, &id);
    }
    printf("tsdb-bench: %u series x %" PRIu64 " points = %.1f M rows, dataset %s,\n"
           "            batch %zu rows, %d writer threads, zstd %d, fsync %d, simd %s\n\n",
           c.series, c.points, total / 1e6, c.dataset < 0 ? "mixed" : ds_info[c.dataset].name, c.batch, c.threads,
           c.zstd, c.fsync, tsdb_simd_level());

    tsdb_stats st;
    if (!c.query_only) {
        double t0 = now_s();
        for (int i = 0; i < c.threads; i++) {
            jobs[i] = (wjob){&c, db, NULL, (uint32_t)((uint64_t)c.series * i / c.threads),
                             (uint32_t)((uint64_t)c.series * (i + 1) / c.threads), 0};
            pthread_create(&th[i], NULL, writer, &jobs[i]);
        }
        for (int i = 0; i < c.threads; i++)
            pthread_join(th[i], NULL);
        double ti = now_s() - t0;
        tsdb_get_stats(db, &st);
        printf("insert+generate %6.2f s  %7.2f M rows/s   (%" PRIu64 " parts, %" PRIu64 " merges so far)\n", ti,
               total / ti / 1e6, st.parts, st.merges);
        t0 = now_s();
        rc = tsdb_optimize(db);
        double to = now_s() - t0;
        tsdb_get_stats(db, &st);
        printf("optimize        %6.2f s  -> %" PRIu64 " parts in %" PRIu64 " partitions, %" PRIu64
               " granules copied as-is\n",
               to, st.parts, st.partitions, st.passthrough_granules);
    }
    tsdb_get_stats(db, &st);
    printf("storage    %8.2f MiB  %.3f bytes/row  ratio %.1fx vs 12 bytes/row (DateTime64(3)+Float32)\n\n",
           st.bytes / 1048576.0, (double)st.bytes / (double)st.rows, st.rows * 12.0 / (double)st.bytes);

    double t0 = now_s();
    double acc[2] = {0, 0};
    for (uint32_t s = 0; s < c.series; s++)
        tsdb_query_raw(db, s, INT64_MIN, INT64_MAX, count_rows, acc);
    double tr = now_s() - t0;
    printf("raw scan, all series, 1 thread            %8.3f s  %7.1f M rows/s  (%.0f rows)\n", tr, acc[0] / tr / 1e6,
           acc[0]);

    struct {
        const char *label;
        uint64_t bucket; /* samples; 0 = whole range */
    } aggs[] = {{"agg whole range, 1 bucket per series", 0},
                {"agg buckets of 86400 samples", 86400},
                {"agg buckets of 3600 samples", 3600},
                {"agg buckets of 60 samples (decodes all)", 60},
                {"agg buckets of 10 samples (decodes all)", 10}};
    for (size_t k = 0; k < sizeof aggs / sizeof *aggs; k++) {
        t0 = now_s();
        uint64_t rows = 0, buckets = 0;
        for (uint32_t s = 0; s < c.series; s++) {
            tsdb_bucket *b;
            size_t nb;
            rc = tsdb_query_agg(db, s, INT64_MIN, INT64_MAX, (int64_t)aggs[k].bucket * step_of(&c, s), &b, &nb);
            if (rc) {
                fprintf(stderr, "agg: %s\n", tsdb_strerror(rc));
                return 1;
            }
            for (size_t i = 0; i < nb; i++)
                rows += b[i].count;
            buckets += nb;
            free(b);
        }
        double ta = now_s() - t0;
        printf("%-42s %8.3f s  %7.1f M rows/s  %6.2f ms/series  (%" PRIu64 " buckets)\n", aggs[k].label, ta,
               rows / ta / 1e6, ta / c.series * 1e3, buckets);
        if (rows != total)
            printf("  !! row count mismatch: %" PRIu64 " != %" PRIu64 "\n", rows, total);
    }

    /* calendar grouping over the whole range, every series */
    struct {
        const char *label;
        tsdb_group g;
    } groups[] = {{"group by 15 minutes (UTC)", {TSDB_MINUTE, 15, NULL}},
                  {"group by hour (UTC)", {TSDB_HOUR, 1, NULL}},
                  {"group by day (Europe/Oslo)", {TSDB_DAY, 1, "Europe/Oslo"}},
                  {"group by week (Europe/Oslo)", {TSDB_WEEK, 1, "Europe/Oslo"}},
                  {"group by month (Europe/Oslo)", {TSDB_MONTH, 1, "Europe/Oslo"}},
                  {"group by year (Europe/Oslo)", {TSDB_YEAR, 1, "Europe/Oslo"}}};
    for (size_t k = 0; k < sizeof groups / sizeof *groups; k++) {
        double best = 1e9;
        uint64_t rows = 0, buckets = 0;
        for (int rep = 0; rep < 3; rep++) {
            t0 = now_s();
            rows = buckets = 0;
            for (uint32_t s = 0; s < c.series; s++) {
                tsdb_bucket *b;
                size_t nb;
                if ((rc = tsdb_query_group_local(db, s, INT64_MIN, INT64_MAX, &groups[k].g, NULL, &b, &nb)) != 0) {
                    fprintf(stderr, "group: %s\n", tsdb_strerror(rc));
                    return 1;
                }
                for (size_t i = 0; i < nb; i++)
                    rows += b[i].count;
                buckets += nb;
                free(b);
            }
            double dt = now_s() - t0;
            best = dt < best ? dt : best;
        }
        printf("%-42s %8.3f s  %7.1f M rows/s  (%" PRIu64 " buckets)\n", groups[k].label, best, rows / best / 1e6,
               buckets);
    }

    printf("\nrandom windows (2 s each):\n");
    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    run_qps(db, &c, "raw, 3600 samples", 0, 3600, 0, 1);
    run_qps(db, &c, "raw, 3600 samples", 0, 3600, 0, nthreads);
    run_qps(db, &c, "agg, 86400 samples in buckets of 300", 1, 86400, 300, 1);
    run_qps(db, &c, "agg, 86400 samples in buckets of 300", 1, 86400, 300, nthreads);
    if (c.points > 7 * 86400) {
        run_qps(db, &c, "agg, 604800 samples in buckets of 3600", 1, 604800, 3600, 1);
        run_qps(db, &c, "agg, 604800 samples in buckets of 3600", 1, 604800, 3600, nthreads);
    }

    tsdb_close(db);
    if (!c.keep) {
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", c.dir);
        if (system(cmd) != 0)
            return 1;
    }
    free(jobs);
    free(th);
    return 0;
}
