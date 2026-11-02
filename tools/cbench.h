/*
 * What tsdb-bench (embedded) and tsdbc-bench (TCP) share, and what the Rust, Python and Java
 * query benchmarks copy line by line, so that all of them do the same work:
 *
 *  - the random query windows (cb_window): series, start and end in samples of the series' own
 *    step, from a splitmix-style generator seeded per thread;
 *  - the three window classes (cb_classes);
 *  - the one-line result and the machine-readable RESULT line that bench/clients/run.sh reads;
 *  - the expected contents of an exported RowBinary file (cb_expect), to check a load: row count, sum and a
 *    hash of the (timestamp, value) pairs per series (cb_expect_verify);
 *  - process CPU time.
 */
#ifndef TSDB_TOOLS_CBENCH_H
#define TSDB_TOOLS_CBENCH_H

#include "common.h"
#include "datasets.h"
#include "tsdb.h"

#include <fcntl.h>
#include <math.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#define CB_T0 INT64_C(1704067200000) /* 2024-01-01, where tsdb-bench starts every series */
#define CB_SEED_BASE 991             /* thread i starts from ds_mix(i + CB_SEED_BASE) */
#define CB_DEFAULT_SECONDS 3.0

typedef struct cb_class {
    const char *id;    /* in RESULT lines */
    const char *label; /* in the human line */
    int agg;           /* 0 raw rows, 1 aggregate buckets */
    uint64_t window;   /* samples */
    uint64_t bucket;   /* samples per bucket (agg) */
} cb_class;

static const cb_class cb_classes[] = {
    {"raw", "raw, 3600 samples", 0, 3600, 0},
    {"agg300", "agg, 86400 samples in buckets of 300", 1, 86400, 300},
    {"agg3600", "agg, 604800 samples in buckets of 3600", 1, 604800, 3600},
};
#define CB_NCLASSES 3

static inline int64_t cb_step_ms(uint32_t series) { return ds_info[series % (DS_KINDS - 1)].step_ms; }

/* The next random window of a thread whose generator state is *r. */
static inline void cb_window(uint64_t *r, uint32_t nseries, uint64_t points, uint64_t window, uint32_t *series,
                             int64_t *start, int64_t *end)
{
    *r = ds_mix(*r);
    uint32_t s = (uint32_t)(*r % nseries);
    int64_t step = cb_step_ms(s);
    uint64_t span = points > window ? points - window : 1;
    *series = s;
    *start = CB_T0 + (int64_t)((*r >> 20) % span) * step;
    *end = *start + (int64_t)window * step;
}

static inline uint64_t cb_seed(int thread) { return ds_mix((uint64_t)thread + CB_SEED_BASE); }

/* CPU time (user + system, all threads) the process has used so far. */
static inline double cb_cpu_s(void)
{
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    return (double)ru.ru_utime.tv_sec + (double)ru.ru_stime.tv_sec + 1e-6 * (double)(ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
}

/* Result of one class at one thread count: the human line, then the RESULT line. `unit` is "conn" or "thr". */
static inline void cb_report_query(const char *client, const cb_class *k, const char *unit, int threads,
                                   uint64_t queries, uint64_t rows, double secs, double cpu)
{
    printf("  %-44s %3d %s %10.0f q/s  %8.1f us/q  %7.1f Mrows/s\n", k->label, threads, unit, queries / secs,
           secs * threads / queries * 1e6, rows / secs / 1e6);
    printf("RESULT kind=query client=%s class=%s threads=%d queries=%" PRIu64 " rows=%" PRIu64 " secs=%.4f cpu=%.4f\n",
           client, k->id, threads, queries, rows, secs, cpu);
    fflush(stdout);
}

static inline void cb_report_load(const char *client, int conns, int compression, uint64_t rows, double secs,
                                  double bytes, double cpu, const char *extra)
{
    printf("load (%s): %" PRIu64 " rows in %.2f s = %.2f M rows/s, %.2f bytes/row, client CPU %.2f s (%.2f s per M rows)"
           ", %d connections/threads%s\n",
           client, rows, secs, rows / secs / 1e6, bytes / (double)rows, cpu, cpu / ((double)rows / 1e6), conns,
           extra ? extra : "");
    printf("RESULT kind=load client=%s conns=%d compression=%d rows=%" PRIu64 " secs=%.4f bytes=%.0f cpu=%.4f\n", client,
           conns, compression, rows, secs, bytes, cpu);
    fflush(stdout);
}

/* ----------------------------------------------------- expected contents */

/* One row of a bench row file (the RowBinary ClickHouse loads). s is the series NUMBER, an index into the bench's id
 * table (sids[s] holds the tsdb_sid that tsdb_series / tsdbc_series returned for the series' name), never a tsdb id:
 * ids are 64-bit and carry the volume. It stays 32 bits so files already generated and ClickHouse's tables do not
 * change. */
typedef struct rbrow {
    uint32_t s;
    int64_t t;
    float v;
} __attribute__((packed)) rbrow;

typedef struct cb_file {
    const rbrow *rows;
    size_t n;
    size_t bytes;
} cb_file;

static inline int cb_file_open(const char *path, cb_file *f)
{
    int fd = open(path, O_RDONLY);
    struct stat st;
    if (fd < 0) {
        perror(path);
        return -1;
    }
    if (fstat(fd, &st) != 0) {
        perror(path);
        close(fd);
        return -1;
    }
    if (st.st_size == 0 || st.st_size % (off_t)sizeof(rbrow) != 0) {
        fprintf(stderr, "%s: %lld bytes: not a RowBinary file of 16-byte rows (series u32, ts i64, value f32)\n", path,
                (long long)st.st_size);
        close(fd);
        return -1;
    }
    void *p = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) {
        perror(path);
        return -1;
    }
    f->rows = p;
    f->bytes = (size_t)st.st_size;
    f->n = f->bytes / sizeof(rbrow);
    return 0;
}

typedef struct cb_expect {
    uint32_t nseries;
    uint64_t *count;
    double *sum, *abs;
    uint64_t *hash; /* sum over the rows of cb_row_hash: independent of the order, sensitive to every timestamp */
} cb_expect;

static inline uint64_t cb_row_hash(int64_t t, float v)
{
    uint32_t bits;
    memcpy(&bits, &v, sizeof bits);
    return ds_mix((uint64_t)t * UINT64_C(0x9e3779b97f4a7c15) ^ ((uint64_t)bits << 17 | bits));
}

/* Row count, sum, sum of magnitudes and hash of (timestamp, value) per series of the file. */
static inline void cb_expect_make(const cb_file *f, cb_expect *e)
{
    uint32_t maxs = 0;
    for (size_t i = 0; i < f->n; i++)
        maxs = f->rows[i].s > maxs ? f->rows[i].s : maxs;
    e->nseries = f->n ? maxs + 1 : 0;
    size_t ns = e->nseries ? e->nseries : 1;
    e->count = calloc(ns, sizeof *e->count);
    e->sum = calloc(ns, sizeof *e->sum);
    e->abs = calloc(ns, sizeof *e->abs);
    e->hash = calloc(ns, sizeof *e->hash);
    for (size_t i = 0; i < f->n; i++) {
        uint32_t s = f->rows[i].s;
        e->count[s]++;
        e->sum[s] += (double)f->rows[i].v;
        e->abs[s] += fabs((double)f->rows[i].v);
        e->hash[s] += cb_row_hash(f->rows[i].t, f->rows[i].v);
    }
}

static inline void cb_expect_free(cb_expect *e)
{
    free(e->count);
    free(e->sum);
    free(e->abs);
    free(e->hash);
}

/* 0 if series s holds the file's rows: the same count, and a sum equal up to summation order. */
static inline int cb_expect_check(const cb_expect *e, uint32_t s, uint64_t count, double sum)
{
    if (count != e->count[s])
        return 1;
    return fabs(sum - e->sum[s]) > 1e-9 * e->abs[s] + 1e-9;
}

/* The two queries verification needs, of whatever is verified (the library or a client), and the id of each of the
 * file's series numbers (ids are not those numbers: they carry the volume, and a database may hold other series). */
typedef struct cb_source {
    void *ctx;
    int (*agg)(void *ctx, tsdb_sid series, int64_t start, int64_t end, int64_t interval, tsdb_bucket **out,
               size_t *nout);
    int (*raw)(void *ctx, tsdb_sid series, int64_t start, int64_t end, tsdb_raw_fn fn, void *fnctx);
    const tsdb_sid *ids;
} cb_source;

typedef struct cb_rawsum {
    uint64_t count, hash;
} cb_rawsum;

static inline int cb_rawsum_fn(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    cb_rawsum *r = ctx;
    for (size_t i = 0; i < n; i++)
        r->hash += cb_row_hash(ts[i], v[i]);
    r->count += n;
    return 0;
}

/* Does series s hold exactly the file's rows? The count and sum from the aggregate path (which the query benchmarks
 * use), and the rows themselves read back: their count and the hash of every (timestamp, value), so that a loader that
 * stores the right values at other timestamps, or moves one row, does not pass. Returns 0 if so, 1 with a reason in
 * why[] if not, a negative error code if a query failed. *count gets the rows found. */
static inline int cb_expect_verify(const cb_expect *e, uint32_t s, const cb_source *src, uint64_t *count, char *why,
                                   size_t whylen)
{
    tsdb_bucket *b;
    size_t nb;
    int rc = src->agg(src->ctx, src->ids[s], INT64_MIN, INT64_MAX, 0, &b, &nb);
    if (rc)
        return rc;
    uint64_t c = nb ? b[0].count : 0;
    double sum = nb ? b[0].sum : 0;
    free(b);
    *count = c;
    if (cb_expect_check(e, s, c, sum)) {
        snprintf(why, whylen, "holds %" PRIu64 " rows, sum %.17g; the file has %" PRIu64 ", sum %.17g", c, sum,
                 e->count[s], e->sum[s]);
        return 1;
    }
    cb_rawsum r = {0, 0};
    rc = src->raw(src->ctx, src->ids[s], INT64_MIN, INT64_MAX, cb_rawsum_fn, &r);
    if (rc)
        return rc;
    if (r.count != e->count[s] || r.hash != e->hash[s]) {
        snprintf(why, whylen, "reads back %" PRIu64 " rows with timestamps or values other than the file's (%" PRIu64
                 " rows)", r.count, e->count[s]);
        return 1;
    }
    return 0;
}

/* Verifies every series; prints up to 5 reasons and the verdict line; returns 0 if all ok, 1 if not, 2 on a query error.
 * `where` completes "rows on the server" / "in the database". */
static inline int cb_expect_verify_all(const cb_expect *e, size_t file_rows, const cb_source *src, const char *where)
{
    uint64_t total = 0, bad = 0;
    for (uint32_t s = 0; s < e->nseries; s++) {
        uint64_t c = 0;
        char why[256];
        int rc = cb_expect_verify(e, s, src, &c, why, sizeof why);
        if (rc < 0) {
            fprintf(stderr, "verify: query of series %u failed (%d)\n", s, rc);
            return 2;
        }
        total += c;
        if (rc && bad++ < 5)
            fprintf(stderr, "verify: series %u %s\n", s, why);
    }
    printf("verify: %u series, %" PRIu64 " rows %s, %zu in the file: %s\n", e->nseries, total, where, file_rows,
           bad || total != file_rows ? "MISMATCH" : "ok");
    return bad || total != file_rows ? 1 : 0;
}

#endif
