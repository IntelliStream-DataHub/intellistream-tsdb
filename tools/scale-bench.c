/*
 * Benchmark at scale: load a large data set, then run a fixed query suite, in
 * tsdb and, through the SQL it writes, in ClickHouse.
 *
 *   scale-bench load   --dir D [--series 10000] [--points 10000000] [--threads 8]
 *   scale-bench export [--series ...] [--from S0] [--to S1] > rows.rowbinary
 *   scale-bench query  --dir D | --port P  [--series ...] [--threads 32] [--sql DIR] [--only NAME]
 *
 * The data are the seven sensor kinds of datasets.h, mixed over the series,
 * with their usual value streams but all stamped at 1 Hz, so every series
 * covers the same days (10 M samples are 115.7 days from 2024-01-01).
 *
 * load and export produce the same rows in the same order: each thread or
 * stream takes groups of --group series and sends them --window samples at a
 * time, series by series: a historical bulk load, where every batch fills
 * whole granules. export writes RowBinary (u32 series, i64 ts, f32 value) for
 * `INSERT INTO big FORMAT RowBinary`. After loading, load waits until the
 * background merges have settled and prints the part count and size.
 *
 * query runs each class of the suite on a fixed seed, either embedded (--dir)
 * or against tsdbd (--port, one connection per thread), and with --sql DIR
 * writes the same queries, one per line, to DIR/<class>.sql for
 * clickhouse-benchmark. Windows are [T0 + i s - 0.5 s, T0 + (i + n) s - 0.5 s),
 * which holds exactly n samples despite the clock jitter of some kinds.
 * Queries over many series run one query per series on --threads threads.
 */
#include "../client/tsdb_client.h"
#include "common.h"
#include "datasets.h"
#include "tsdb.h"

#include <getopt.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <unistd.h>

static const int64_t T0 = 1704067200000; /* 2024-01-01 */
static const int64_t STEP = 1000;

static const char *dir, *host = "127.0.0.1", *sql_dir, *only;
static int port, threads = 8;
static uint32_t nseries = 10000, s_from, s_to, group_series = 100;
static uint64_t points = 10000000, window = 100000;

static ds_kind kind_of(uint32_t s) { return (ds_kind)(s % (DS_KINDS - 1)); } /* mixed: no random */

/* Rows of series [s0, s0 + ns) for samples [i0, i0 + n), series by series. */
static size_t gen(ds_state *st, uint32_t s0, uint32_t ns, uint64_t n, uint32_t *sv, int64_t *tv, float *vv)
{
    size_t k = 0;
    for (uint32_t s = 0; s < ns; s++)
        for (uint64_t i = 0; i < n; i++, k++) {
            sv[k] = s0 + s;
            ds_next(&st[s], &tv[k], &vv[k]);
        }
    return k;
}

static void init_states(ds_state *st, uint32_t s0, uint32_t ns)
{
    for (uint32_t s = 0; s < ns; s++) {
        ds_init(&st[s], kind_of(s0 + s), s0 + s, T0);
        st[s].stamp_ms = STEP;
    }
}

/* ---- load ---- */

static atomic_uint next_group;
static atomic_ullong loaded;

static void *load_worker(void *arg)
{
    tsdb *db = arg;
    size_t cap = (size_t)group_series * window;
    uint32_t *sv = malloc(cap * sizeof *sv);
    int64_t *tv = malloc(cap * sizeof *tv);
    float *vv = malloc(cap * sizeof *vv);
    ds_state *st = malloc(group_series * sizeof *st);
    uint32_t ngroups = (nseries + group_series - 1) / group_series;
    for (uint32_t g; (g = atomic_fetch_add(&next_group, 1)) < ngroups;) {
        uint32_t s0 = g * group_series, ns = nseries - s0 < group_series ? nseries - s0 : group_series;
        init_states(st, s0, ns);
        for (uint64_t i0 = 0; i0 < points; i0 += window) {
            uint64_t n = points - i0 < window ? points - i0 : window;
            size_t rows = gen(st, s0, ns, n, sv, tv, vv);
            int rc = tsdb_insert(db, sv, tv, vv, rows);
            if (rc) {
                fprintf(stderr, "insert: %s\n", tsdb_strerror(rc));
                exit(1);
            }
            atomic_fetch_add(&loaded, rows);
        }
    }
    free(sv);
    free(tv);
    free(vv);
    free(st);
    return NULL;
}

static int cmd_load(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc) {
        fprintf(stderr, "open %s: %s\n", dir, tsdb_strerror(rc));
        return 1;
    }
    for (uint32_t s = 0; s < nseries; s++) { /* ids are assigned in order, so series s gets id s */
        char name[64];
        uint32_t id;
        snprintf(name, sizeof name, "%s.%u", ds_info[kind_of(s)].name, s);
        if ((rc = tsdb_series(db, name, &id)) != 0 || id != s) {
            fprintf(stderr, "series %s: %s\n", name, rc ? tsdb_strerror(rc) : "unexpected id");
            return 1;
        }
    }
    uint64_t total = (uint64_t)nseries * points;
    printf("loading %u series x %" PRIu64 " samples = %.1f G rows, %d threads, %u series x %" PRIu64
           " samples per insert\n",
           nseries, points, total / 1e9, threads, group_series, window);
    fflush(stdout);
    pthread_t *th = calloc((size_t)threads, sizeof *th);
    double t0 = now_s();
    for (int i = 0; i < threads; i++)
        pthread_create(&th[i], NULL, load_worker, db);
    for (uint64_t last = 0, done; (done = atomic_load(&loaded)) < total;) {
        sleep(1);
        if ((int)(now_s() - t0) % 60 == 0 && done != last) {
            tsdb_stats s;
            tsdb_get_stats(db, &s);
            printf("  %6.0f s  %6.2f G rows  %6.1f M rows/s  %" PRIu64 " parts  %.1f GB\n", now_s() - t0,
                   done / 1e9, done / (now_s() - t0) / 1e6, s.parts, s.bytes / 1e9);
            fflush(stdout);
            last = done;
        }
    }
    for (int i = 0; i < threads; i++)
        pthread_join(th[i], NULL);
    double t_ins = now_s() - t0;
    tsdb_stats s;
    tsdb_get_stats(db, &s);
    printf("inserted %.2f G rows in %.0f s: %.1f M rows/s; %" PRIu64 " parts, %.2f GB\n", total / 1e9, t_ins,
           total / t_ins / 1e6, s.parts, s.bytes / 1e9);
    fflush(stdout);
    /* settled: no merge finished and the part count did not move for a minute */
    for (int quiet = 0; quiet < 6;) {
        tsdb_stats p = s;
        sleep(10);
        tsdb_get_stats(db, &s);
        quiet = s.parts == p.parts && s.merges == p.merges ? quiet + 1 : 0;
    }
    double t_all = now_s() - t0 - 60;
    printf("merges settled after %.0f s: %" PRIu64 " parts in %" PRIu64 " partitions, %" PRIu64
           " merges, %.2f GB = %.0f MB, %.3f bytes/row\n",
           t_all, s.parts, s.partitions, s.merges, s.bytes / 1e9, s.bytes / 1e6, (double)s.bytes / s.rows);
    tsdb_close(db);
    free(th);
    return 0;
}

/* ---- export ---- */

static int cmd_export(void)
{
    if (!s_to)
        s_to = nseries;
    size_t cap = (size_t)group_series * window;
    uint32_t *sv = malloc(cap * sizeof *sv);
    int64_t *tv = malloc(cap * sizeof *tv);
    float *vv = malloc(cap * sizeof *vv);
    char *buf = malloc(cap * 16);
    ds_state *st = malloc(group_series * sizeof *st);
    for (uint32_t s0 = s_from; s0 < s_to; s0 += group_series) {
        uint32_t ns = s_to - s0 < group_series ? s_to - s0 : group_series;
        init_states(st, s0, ns);
        for (uint64_t i0 = 0; i0 < points; i0 += window) {
            uint64_t n = points - i0 < window ? points - i0 : window;
            size_t rows = gen(st, s0, ns, n, sv, tv, vv);
            for (size_t k = 0; k < rows; k++) {
                memcpy(buf + k * 16, &sv[k], 4);
                memcpy(buf + k * 16 + 4, &tv[k], 8);
                memcpy(buf + k * 16 + 12, &vv[k], 4);
            }
            if (fwrite(buf, 16, rows, stdout) != rows) {
                perror("export");
                return 1;
            }
        }
    }
    fflush(stdout);
    return 0;
}

/* ---- query ---- */

enum { Q_RAW, Q_AGG };
enum { G_NONE, G_MINUTE, G_HOUR, G_DAY_OSLO, G_DAY_UTC };

typedef struct qclass {
    const char *name, *label;
    int kind;
    uint32_t series; /* 0: all */
    uint64_t samples; /* 0: the whole range */
    int group;
    int queries;
} qclass;

static const qclass classes[] = {
    {"point", "random single row", Q_RAW, 1, 1, G_NONE, 1000},
    {"agg_100k", "aggregate 100 K rows (1 series)", Q_AGG, 1, 100000, G_NONE, 100},
    {"agg_1m", "aggregate 1 M rows (1 series)", Q_AGG, 1, 1000000, G_NONE, 50},
    {"agg_10m", "aggregate 10 M rows (1 series, all of it)", Q_AGG, 1, 0, G_NONE, 20},
    {"agg_1b", "aggregate 1 G rows (100 series)", Q_AGG, 100, 0, G_NONE, 3},
    {"agg_all", "aggregate the whole table", Q_AGG, 0, 0, G_NONE, 2},
    {"grp_1s_hour", "1 series by hour", Q_AGG, 1, 0, G_HOUR, 20},
    {"grp_1s_minute", "1 series by minute", Q_AGG, 1, 0, G_MINUTE, 20},
    {"grp_100s_day", "100 series by series, day (Europe/Oslo)", Q_AGG, 100, 0, G_DAY_OSLO, 3},
    {"grp_100s_hour", "100 series by series, hour", Q_AGG, 100, 0, G_HOUR, 3},
    {"grp_all_day", "whole table by series, day (Europe/Oslo)", Q_AGG, 0, 0, G_DAY_OSLO, 2},
    {"grp_all_hour", "whole table by series, hour", Q_AGG, 0, 0, G_HOUR, 2},
    {"grp_all_day_utc", "whole table by series, day (UTC)", Q_AGG, 0, 0, G_DAY_UTC, 2},
};

static const tsdb_group groups[] = {{0, 0, NULL}, {TSDB_MINUTE, 1, NULL}, {TSDB_HOUR, 1, NULL},
                                    {TSDB_DAY, 1, "Europe/Oslo"}, {TSDB_DAY, 1, NULL}};
static const char *ch_bucket[] = {NULL, "toStartOfMinute(ts)", "toStartOfHour(ts)",
                                  "toStartOfDay(ts, 'Europe/Oslo')", "toStartOfDay(ts)"};

typedef struct target {
    tsdb *db;
    tsdbc **conns; /* one per thread over TCP */
} target;

typedef struct tally {
    uint64_t rows, buckets;
    double sum;
} tally;

static int count_raw(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    tally *t = ctx;
    for (size_t i = 0; i < n; i++)
        t->sum += v[i];
    t->rows += n;
    (void)ts;
    return 0;
}

static int run_one(const target *tg, int slot, const qclass *qc, uint32_t s, int64_t start, int64_t end, tally *t)
{
    tsdbc *c = tg->conns ? tg->conns[slot] : NULL;
    if (qc->kind == Q_RAW)
        return c ? tsdbc_query_raw(c, s, start, end, count_raw, t) : tsdb_query_raw(tg->db, s, start, end, count_raw, t);
    const tsdb_group *g = qc->group == G_NONE ? NULL : &groups[qc->group];
    tsdb_bucket *b = NULL;
    size_t nb = 0;
    int rc = c ? tsdbc_query_group(c, s, start, end, g, NULL, &b, &nb) : tsdb_query_group(tg->db, s, start, end, g, &b, &nb);
    for (size_t i = 0; i < nb; i++) {
        t->rows += b[i].count;
        t->sum += b[i].sum;
    }
    t->buckets += nb;
    free(b);
    return rc;
}

typedef struct span_job {
    const target *tg;
    const qclass *qc;
    int slot;
    atomic_uint *next;
    uint32_t s1;
    int64_t start, end;
    tally t;
} span_job;

static void *span_worker(void *arg)
{
    span_job *j = arg;
    for (uint32_t s; (s = atomic_fetch_add(j->next, 1)) < j->s1;)
        if (run_one(j->tg, j->slot, j->qc, s, j->start, j->end, &j->t)) {
            fprintf(stderr, "query %s on series %u failed\n", j->qc->name, s);
            exit(1);
        }
    return NULL;
}

/* Series [s0, s1), one query each, on all threads. */
static void run_span(const target *tg, const qclass *qc, uint32_t s0, uint32_t s1, int64_t start, int64_t end,
                     tally *t)
{
    atomic_uint next = s0;
    span_job *j = calloc((size_t)threads, sizeof *j);
    pthread_t *th = calloc((size_t)threads, sizeof *th);
    for (int i = 0; i < threads; i++) {
        j[i] = (span_job){tg, qc, i, &next, s1, start, end, {0, 0, 0}};
        pthread_create(&th[i], NULL, span_worker, &j[i]);
    }
    for (int i = 0; i < threads; i++) {
        pthread_join(th[i], NULL);
        t->rows += j[i].t.rows;
        t->buckets += j[i].t.buckets;
        t->sum += j[i].t.sum;
    }
    free(j);
    free(th);
}

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static void sql_ts(FILE *f, const char *op, int64_t t) { fprintf(f, "ts %s fromUnixTimestamp64Milli(toInt64(%" PRId64 "))", op, t); }

static void write_sql(FILE *f, const qclass *qc, uint32_t s0, uint32_t s1, int64_t start, int64_t end)
{
    const char *aggs = "count(), sum(value), min(value), max(value), avg(value)";
    if (qc->kind == Q_RAW)
        fprintf(f, "SELECT ts, value FROM big WHERE series = %u AND ", s0);
    else if (qc->group == G_NONE)
        fprintf(f, "SELECT %s FROM big WHERE series >= %u AND series < %u AND ", aggs, s0, s1);
    else if (s1 - s0 == 1)
        fprintf(f, "SELECT %s AS b, %s FROM big WHERE series = %u AND ", ch_bucket[qc->group], aggs, s0);
    else
        fprintf(f, "SELECT series, %s AS b, %s FROM big WHERE series >= %u AND series < %u AND ", ch_bucket[qc->group],
                aggs, s0, s1);
    sql_ts(f, ">=", start);
    fputs(" AND ", f);
    sql_ts(f, "<", end);
    if (qc->kind == Q_AGG && qc->group != G_NONE)
        fputs(s1 - s0 == 1 ? " GROUP BY b ORDER BY b" : " GROUP BY series, b ORDER BY series, b", f);
    fputc('\n', f);
}

static void run_class(const target *tg, const qclass *qc)
{
    uint64_t r = ds_mix(0x5eed ^ (uint64_t)(qc - classes));
    double *lat = malloc((size_t)qc->queries * sizeof *lat);
    tally t = {0, 0, 0};
    FILE *f = NULL;
    if (sql_dir) {
        char path[4096];
        snprintf(path, sizeof path, "%s/%s.sql", sql_dir, qc->name);
        f = fopen(path, "w");
        if (!f) {
            perror(path);
            exit(1);
        }
    }
    for (int q = 0; q < qc->queries; q++) {
        r = ds_mix(r);
        uint32_t span = qc->series ? qc->series : nseries;
        uint32_t s0 = (uint32_t)(r % (nseries - span + 1)), s1 = s0 + span;
        uint64_t n = qc->samples ? qc->samples : points;
        uint64_t i = (r >> 24) % (points - n + 1);
        int64_t start = T0 + (int64_t)i * STEP - STEP / 2, end = start + (int64_t)n * STEP;
        if (f)
            write_sql(f, qc, s0, s1, start, end);
        double t0 = now_s();
        if (span == 1) {
            if (run_one(tg, 0, qc, s0, start, end, &t)) {
                fprintf(stderr, "query %s failed\n", qc->name);
                exit(1);
            }
        } else {
            run_span(tg, qc, s0, s1, start, end, &t);
        }
        lat[q] = now_s() - t0;
    }
    if (f)
        fclose(f);
    double total = 0;
    for (int q = 0; q < qc->queries; q++)
        total += lat[q];
    qsort(lat, (size_t)qc->queries, sizeof *lat, cmp_double);
    double mean = total / qc->queries;
    printf("%-14s %-44s %5d q  mean %10.3f ms  p50 %10.3f ms  min %10.3f ms  %12.0f rows/q  %8.1f M rows/s"
           "  %" PRIu64 " buckets  sum %.6g\n",
           qc->name, qc->label, qc->queries, mean * 1e3, lat[qc->queries / 2] * 1e3, lat[0] * 1e3,
           (double)t.rows / qc->queries, t.rows / total / 1e6, t.buckets, t.sum);
    fflush(stdout);
    free(lat);
}

static int cmd_query(void)
{
    target tg = {NULL, NULL};
    if (port) {
        tg.conns = calloc((size_t)threads, sizeof *tg.conns);
        for (int i = 0; i < threads; i++) {
            int rc = tsdbc_connect(host, port, &tg.conns[i]);
            if (rc) {
                fprintf(stderr, "connect %s:%d: %s\n", host, port, tsdbc_strerror(rc));
                return 1;
            }
        }
    } else {
        tsdb_options o;
        tsdb_options_init(&o);
        o.read_only = true;
        int rc = tsdb_open(dir, &o, &tg.db);
        if (rc) {
            fprintf(stderr, "open %s: %s\n", dir, tsdb_strerror(rc));
            return 1;
        }
    }
    if (sql_dir)
        mkdir(sql_dir, 0755);
    printf("%s, %d threads for multi-series queries\n", port ? "tsdbd over TCP" : "tsdb embedded", threads);
    for (size_t k = 0; k < sizeof classes / sizeof *classes; k++)
        if (!only || strstr(classes[k].name, only))
            run_class(&tg, &classes[k]);
    if (tg.conns) {
        for (int i = 0; i < threads; i++)
            tsdbc_close(tg.conns[i]);
        free(tg.conns);
    } else {
        tsdb_close(tg.db);
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: scale-bench load|export|query [options]\n");
        return 2;
    }
    const char *cmd = argv[1];
    static struct option opts[] = {
        {"dir", required_argument, 0, 'd'},    {"series", required_argument, 0, 's'},
        {"points", required_argument, 0, 'p'}, {"threads", required_argument, 0, 't'},
        {"group", required_argument, 0, 'g'},  {"window", required_argument, 0, 'w'},
        {"from", required_argument, 0, 'f'},   {"to", required_argument, 0, 'T'},
        {"host", required_argument, 0, 'h'},   {"port", required_argument, 0, 'P'},
        {"sql", required_argument, 0, 'S'},    {"only", required_argument, 0, 'o'},
        {0, 0, 0, 0}};
    int ch;
    optind = 2;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': dir = optarg; break;
        case 's': nseries = (uint32_t)strtoul(optarg, NULL, 10); break;
        case 'p': points = strtoull(optarg, NULL, 10); break;
        case 't': threads = atoi(optarg); break;
        case 'g': group_series = (uint32_t)strtoul(optarg, NULL, 10); break;
        case 'w': window = strtoull(optarg, NULL, 10); break;
        case 'f': s_from = (uint32_t)strtoul(optarg, NULL, 10); break;
        case 'T': s_to = (uint32_t)strtoul(optarg, NULL, 10); break;
        case 'h': host = optarg; break;
        case 'P': port = atoi(optarg); break;
        case 'S': sql_dir = optarg; break;
        case 'o': only = optarg; break;
        default: return 2;
        }
    }
    if (strcmp(cmd, "export") == 0)
        return cmd_export();
    if (strcmp(cmd, "query") == 0 && (dir || port))
        return cmd_query();
    if (strcmp(cmd, "load") == 0 && dir)
        return cmd_load();
    fprintf(stderr, "scale-bench: unknown command or missing --dir\n");
    return 2;
}
