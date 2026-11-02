/*
 * Durable insert throughput from single values up to one huge batch, with
 * fsync on. Data is the mixed sensor workload of datasets.h, arriving the
 * way collectors send it: time-major (every series at time t, then t + 1).
 *
 *   insert-bench --dir /var/tmp/x [--series 1000] [--max-batch 1000000000]
 */
#include "common.h"
#include "datasets.h"
#include "tsdb.h"

#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>

static uint32_t nseries = 1000;
static tsdb_sid *sids; /* the id of series number i (ids carry the volume: they are not the numbers) */
static const char *dir;
static int nvolumes = 1; /* series spread round-robin over this many volumes (directories) */

/* A time-major stream over nseries series of mixed kinds. Kinds have
 * different intervals, so time-major here means "sample index major". */
typedef struct stream {
    ds_state *st;
    uint64_t pos; /* rows produced */
} stream;

static void stream_init(stream *s)
{
    s->st = calloc(nseries, sizeof *s->st);
    for (uint32_t i = 0; i < nseries; i++)
        ds_init(&s->st[i], (ds_kind)(i % (DS_KINDS - 1)), i, 1704067200000); /* no random */
    s->pos = 0;
}

static void stream_free(stream *s) { free(s->st); }

static void stream_fill(stream *s, tsdb_sid *sv, int64_t *tv, float *vv, size_t n)
{
    for (size_t k = 0; k < n; k++, s->pos++) {
        uint32_t i = (uint32_t)(s->pos % nseries);
        sv[k] = sids[i];
        ds_next(&s->st[i], &tv[k], &vv[k]);
    }
}

typedef struct fill_job {
    stream *s;
    tsdb_sid *sv;
    int64_t *tv;
    float *vv;
    size_t n;
    uint64_t base;
    int idx, nthreads;
} fill_job;

/* Parallel fill for huge batches: each thread owns a subset of the series. */
static void *fill_part(void *arg)
{
    fill_job *j = arg;
    for (uint32_t id = (uint32_t)j->idx; id < nseries; id += (uint32_t)j->nthreads) {
        uint64_t first = (j->base / nseries) * nseries + id;
        if (first < j->base)
            first += nseries;
        for (uint64_t p = first; p < j->base + j->n; p += nseries) {
            size_t k = p - j->base;
            j->sv[k] = sids[id];
            ds_next(&j->s->st[id], &j->tv[k], &j->vv[k]);
        }
    }
    return NULL;
}

static void stream_fill_parallel(stream *s, tsdb_sid *sv, int64_t *tv, float *vv, size_t n)
{
    int nt = (int)sysconf(_SC_NPROCESSORS_ONLN);
    pthread_t th[256];
    fill_job jobs[256];
    if (nt > 256)
        nt = 256;
    for (int i = 0; i < nt; i++) {
        jobs[i] = (fill_job){s, sv, tv, vv, n, s->pos, i, nt};
        pthread_create(&th[i], NULL, fill_part, &jobs[i]);
    }
    for (int i = 0; i < nt; i++)
        pthread_join(th[i], NULL);
    s->pos += n;
}

static tsdb *open_fresh(int wal_sync_ms, size_t wal_max_batch)
{
    char cmd[1024];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'.vol*", dir, dir);
    if (system(cmd) != 0)
        exit(1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 1;
    o.wal_sync_ms = wal_sync_ms;
    o.wal_max_batch_rows = wal_max_batch;
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc) {
        fprintf(stderr, "open: %s\n", tsdb_strerror(rc));
        exit(1);
    }
    char abs[PATH_MAX]; /* volume paths are absolute */
    if (!realpath(dir, abs)) {
        perror(dir);
        exit(1);
    }
    sids = calloc(nseries, sizeof *sids);
    for (uint32_t i = 0; i < nseries; i++) {
        char name[64], vol[PATH_MAX + 32];
        snprintf(name, sizeof name, "%s.%u", ds_info[i % (DS_KINDS - 1)].name, i);
        snprintf(vol, sizeof vol, "%s.vol%u", abs, i % (uint32_t)nvolumes);
        rc = tsdb_series_at(db, name, nvolumes > 1 ? vol : NULL, &sids[i]);
        if (rc) {
            fprintf(stderr, "series %s in %s: %s\n", name, nvolumes > 1 ? vol : dir, tsdb_strerror(rc));
            exit(1);
        }
    }
    return db;
}

static int cmp_d(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static void report(const char *label, size_t batch, uint64_t inserts, uint64_t rows, double secs, double *lat,
                   size_t nlat, tsdb *db)
{
    double p50 = 0, p99 = 0;
    if (nlat) {
        qsort(lat, nlat, sizeof *lat, cmp_d);
        p50 = lat[nlat / 2];
        p99 = lat[(size_t)(nlat * 0.99)];
    }
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    char b[32];
    if (batch >= 1000000000)
        snprintf(b, sizeof b, "%zuG", batch / 1000000000);
    else if (batch >= 1000000)
        snprintf(b, sizeof b, "%zuM", batch / 1000000);
    else if (batch >= 1000)
        snprintf(b, sizeof b, "%zuk", batch / 1000);
    else
        snprintf(b, sizeof b, "%zu", batch);
    printf("%-34s %6s %12.0f %12.0f %9.1f %10.3f %10.3f %7" PRIu64 "\n", label, b, inserts / secs, rows / secs,
           rows * 12.0 / secs / 1e6, p50 * 1e3, p99 * 1e3, st.parts);
    fflush(stdout);
}

/* Single-row tsdb_append calls from `threads` threads for `secs` seconds. */
typedef struct app_job {
    tsdb *db;
    int idx, nthreads;
    double secs;
    uint64_t n;
    double *lat;
    size_t nlat, cap;
} app_job;

static void *appender(void *arg)
{
    app_job *j = arg;
    ds_state *st = calloc(nseries, sizeof *st);
    for (uint32_t i = 0; i < nseries; i++)
        ds_init(&st[i], (ds_kind)(i % (DS_KINDS - 1)), i, 1704067200000);
    double t0 = now_s();
    uint64_t k = (uint64_t)j->idx;
    while (now_s() - t0 < j->secs) {
        uint32_t id = (uint32_t)(k % nseries);
        int64_t ts;
        float v;
        ds_next(&st[id], &ts, &v);
        double a = now_s();
        int rc = tsdb_append(j->db, sids[id], ts, v);
        double b = now_s();
        if (rc) {
            fprintf(stderr, "append: %s\n", tsdb_strerror(rc));
            exit(1);
        }
        if (j->nlat < j->cap)
            j->lat[j->nlat++] = b - a;
        j->n++;
        k += (uint64_t)j->nthreads;
    }
    free(st);
    return NULL;
}

static void run_appends(const char *label, int wal_sync_ms, int threads, double secs)
{
    tsdb *db = open_fresh(wal_sync_ms, 65536);
    app_job *jobs = calloc((size_t)threads, sizeof *jobs);
    pthread_t *th = calloc((size_t)threads, sizeof *th);
    for (int i = 0; i < threads; i++) {
        jobs[i] = (app_job){db, i, threads, secs, 0, NULL, 0, 200000};
        jobs[i].lat = malloc(jobs[i].cap * sizeof *jobs[i].lat);
        pthread_create(&th[i], NULL, appender, &jobs[i]);
    }
    uint64_t n = 0;
    size_t nlat = 0;
    for (int i = 0; i < threads; i++) {
        pthread_join(th[i], NULL);
        n += jobs[i].n;
        nlat += jobs[i].nlat;
    }
    double *lat = malloc((nlat + 1) * sizeof *lat);
    size_t k = 0;
    for (int i = 0; i < threads; i++) {
        memcpy(lat + k, jobs[i].lat, jobs[i].nlat * sizeof *lat);
        k += jobs[i].nlat;
        free(jobs[i].lat);
    }
    char l[80];
    snprintf(l, sizeof l, "%s, %d thr", label, threads);
    report(l, 1, n, n, secs, lat, nlat, db);
    free(lat);
    free(jobs);
    free(th);
    tsdb_close(db);
}

/* tsdb_insert with batches of `batch` rows until `min_secs` and `min_rows`. */
static void run_batches(const char *label, size_t batch, size_t wal_max_batch, double min_secs, uint64_t max_rows)
{
    tsdb *db = open_fresh(0, wal_max_batch);
    stream s;
    stream_init(&s);
    tsdb_sid *sv = malloc(batch * sizeof *sv);
    int64_t *tv = malloc(batch * sizeof *tv);
    float *vv = malloc(batch * sizeof *vv);
    if (!sv || !tv || !vv) {
        fprintf(stderr, "cannot allocate %zu rows\n", batch);
        exit(1);
    }
    size_t cap = 100000, nlat = 0;
    double *lat = malloc(cap * sizeof *lat), busy = 0;
    uint64_t inserts = 0, rows = 0;
    uint64_t limit = max_rows > batch ? max_rows : batch;
    while ((busy < min_secs || inserts == 0) && rows + batch <= limit) {
        if (batch >= (1u << 22))
            stream_fill_parallel(&s, sv, tv, vv, batch);
        else
            stream_fill(&s, sv, tv, vv, batch);
        double a = now_s();
        int rc = tsdb_insert(db, sv, tv, vv, batch);
        double d = now_s() - a;
        if (rc) {
            fprintf(stderr, "insert: %s\n", tsdb_strerror(rc));
            exit(1);
        }
        busy += d;
        if (nlat < cap)
            lat[nlat++] = d;
        inserts++;
        rows += batch;
    }
    report(label, batch, inserts, rows, busy, lat, nlat, db);
    free(lat);
    free(sv);
    free(tv);
    free(vv);
    stream_free(&s);
    tsdb_close(db);
}

int main(int argc, char **argv)
{
    uint64_t max_batch = 1000000000;
    double secs = 3;
    static struct option opts[] = {{"dir", required_argument, 0, 'd'},
                                   {"series", required_argument, 0, 's'},
                                   {"max-batch", required_argument, 0, 'm'},
                                   {"seconds", required_argument, 0, 't'},
                                   {"volumes", required_argument, 0, 'v'},
                                   {"appends-only", no_argument, 0, 'a'},
                                   {0, 0, 0, 0}};
    bool appends_only = false;
    int ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': dir = optarg; break;
        case 's': nseries = (uint32_t)atoi(optarg); break;
        case 'm': max_batch = strtoull(optarg, NULL, 10); break;
        case 't': secs = atof(optarg); break;
        case 'v': nvolumes = atoi(optarg) > 0 ? atoi(optarg) : 1; break;
        case 'a': appends_only = true; break;
        default: return 2;
        }
    }
    if (!dir) {
        fprintf(stderr, "usage: insert-bench --dir DIR [--series N] [--max-batch ROWS] [--seconds S]\n");
        return 2;
    }
    printf("durable inserts, fsync on, %u series of mixed sensors in %d volume(s), dir %s\n\n", nseries, nvolumes, dir);
    printf("%-34s %6s %12s %12s %9s %10s %10s %7s\n", "mode", "batch", "inserts/s", "rows/s", "MB/s", "p50 ms",
           "p99 ms", "parts");
    run_appends("append, fsync each (group commit)", 0, 1, secs);
    run_appends("append, fsync each (group commit)", 0, 16, secs);
    run_appends("append, fsync each (group commit)", 0, 64, secs);
    if (appends_only) {
        run_appends("append, fsync each (group commit)", 0, 256, secs);
        char cmd[1024];
        snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'.vol*", dir, dir);
        return system(cmd) != 0;
    }
    static const int intervals[] = {10, 50, 100};
    for (int k = 0; k < 3; k++) {
        char label[64];
        snprintf(label, sizeof label, "append, WAL fsync every %d ms", intervals[k]);
        run_appends(label, intervals[k], 1, secs);
        run_appends(label, intervals[k], 16, secs);
    }
    run_batches("insert 1 row as a part (no WAL)", 1, 1, secs, 2000);
    for (uint64_t b = 10; b <= max_batch; b *= 10) {
        const char *how = b < 65536 ? "insert batch -> WAL + memtable" : "insert batch -> parts";
        run_batches(how, (size_t)b, 65536, secs, b >= 100000000 ? b : (uint64_t)-1);
    }
    char cmd[1024];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'.vol*", dir, dir);
    return system(cmd) != 0;
}
