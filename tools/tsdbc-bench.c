/*
 * Network benchmark against tsdbd.
 *
 *   tsdbc-bench load  FILE.rowbinary [--conns 4] [--batch 1000000]
 *   tsdbc-bench query [--series 100] [--points 1000000] [--conns 1] [--seconds 3]
 *   tsdbc-bench connections --conns 5000 [--seconds 5]
 *
 * load sends a RowBinary file (u32 series, i64 ts, f32 value per row), split
 * into contiguous slices, one per connection, with up to 4 batches in flight.
 * query runs the random windows of tsdb-bench (mixed dataset intervals).
 * connections holds --conns connections open at once (from up to 64 threads)
 * and keeps sending small aggregate queries through all of them.
 */
#include "../client/tsdb_client.h"
#include "common.h"
#include "datasets.h"

#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const char *host = "127.0.0.1";
static int port = 7070, conns = 1;
static size_t batch = 1000000;
static uint32_t nseries = 100;
static uint64_t points = 1000000;
static double seconds = 3;

#pragma pack(push, 1)
typedef struct rbrow {
    uint32_t s;
    int64_t t;
    float v;
} rbrow;
#pragma pack(pop)

static void die(const char *what, int rc)
{
    fprintf(stderr, "tsdbc-bench: %s: %s\n", what, tsdbc_strerror(rc));
    exit(1);
}

typedef struct load_job {
    const rbrow *rows;
    size_t n;
} load_job;

static void *loader(void *arg)
{
    load_job *j = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
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
    tsdbc_close(c);
    free(s);
    free(t);
    free(v);
    return NULL;
}

static int load(const char *path)
{
    int fd = open(path, O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0) {
        perror(path);
        return 1;
    }
    const rbrow *rows = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    size_t n = (size_t)st.st_size / sizeof(rbrow);
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
    double t0 = now_s();
    pthread_t th[256];
    load_job jobs[256];
    for (int i = 0; i < conns; i++) {
        size_t a = n * (size_t)i / (size_t)conns, b = n * (size_t)(i + 1) / (size_t)conns;
        jobs[i] = (load_job){rows + a, b - a};
        pthread_create(&th[i], NULL, loader, &jobs[i]);
    }
    for (int i = 0; i < conns; i++)
        pthread_join(th[i], NULL);
    double dt = now_s() - t0;
    printf("load over TCP: %zu rows in %.2f s = %.1f M rows/s (%d connections, batches of %zu)\n", n, dt,
           n / dt / 1e6, conns, batch);
    tsdbc_close(c);
    return 0;
}

/* ---------------------------------------------------------------- query */

typedef struct qjob {
    int kind; /* 0 raw, 1 agg */
    uint64_t window, bucket;
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

static void *querier(void *arg)
{
    qjob *q = arg;
    tsdbc *c;
    int rc = tsdbc_connect(host, port, &c);
    if (rc)
        die("connect", rc);
    uint64_t r = ds_mix((uint64_t)q->seed + 991);
    double t0 = now_s();
    while (now_s() - t0 < seconds) {
        for (int k = 0; k < 16; k++) {
            r = ds_mix(r);
            uint32_t s = (uint32_t)(r % nseries);
            int64_t step = ds_info[s % (DS_KINDS - 1)].step_ms;
            uint64_t span = points > q->window ? points - q->window : 1;
            int64_t start = 1704067200000 + (int64_t)((r >> 20) % span) * step;
            int64_t end = start + (int64_t)q->window * step;
            if (q->kind == 0) {
                uint64_t n = 0;
                if ((rc = tsdbc_query_raw(c, s, start, end, count_cb, &n)) != 0)
                    die("query", rc);
                q->rows += n;
            } else {
                tsdb_bucket *b;
                size_t nb;
                if ((rc = tsdbc_query_agg(c, s, start, end, (int64_t)q->bucket * step, &b, &nb)) != 0)
                    die("query", rc);
                for (size_t i = 0; i < nb; i++)
                    q->rows += b[i].count;
                free(b);
            }
            q->queries++;
        }
    }
    q->secs = now_s() - t0;
    tsdbc_close(c);
    return NULL;
}

static void run_queries(const char *label, int kind, uint64_t window, uint64_t bucket)
{
    pthread_t th[256];
    qjob jobs[256];
    for (int i = 0; i < conns; i++) {
        jobs[i] = (qjob){kind, window, bucket, i, 0, 0, 0};
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
    printf("  %-44s %3d conn %10.0f q/s  %8.1f us/q  %7.1f Mrows/s\n", label, conns, nq / secs,
           secs * conns / nq * 1e6, rows / secs / 1e6);
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
                                   {"seconds", required_argument, 0, 't'}, {0, 0, 0, 0}};
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
        default: return 2;
        }
    }
    if (optind < argc && strcmp(argv[optind], "connections") == 0 && conns >= 1)
        return connections();
    if (conns < 1 || conns > 256 || optind >= argc) {
        fprintf(stderr, "usage: tsdbc-bench load FILE [--conns N] [--batch ROWS]\n"
                        "       tsdbc-bench query [--series N] [--points N] [--conns N] [--seconds S]\n"
                        "       tsdbc-bench connections --conns N [--seconds S]\n"
                        "       [--host H] [--port P]\n");
        return 2;
    }
    if (strcmp(argv[optind], "load") == 0 && optind + 1 < argc)
        return load(argv[optind + 1]);
    if (strcmp(argv[optind], "query") == 0) {
        run_queries("raw, 3600 samples", 0, 3600, 0);
        run_queries("agg, 86400 samples in buckets of 300", 1, 86400, 300);
        run_queries("agg, 604800 samples in buckets of 3600", 1, 604800, 3600);
        return 0;
    }
    return 2;
}
