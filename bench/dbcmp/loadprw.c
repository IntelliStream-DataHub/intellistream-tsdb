/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Prometheus remote write loader (remote write 1.0: prometheus.WriteRequest, protobuf + snappy):
 *   loadprw FILE HOST PORT SAMPLES_PER_REQUEST SHARDS
 *
 * Reads the RowBinary file (u32 series, i64 ts ms, f32 value) and sends every row as a sample of
 * sensors{series="N"}, the value as the float32's exact double. It sends like Prometheus' own
 * remote write queue: series are spread over SHARDS by series % SHARDS, each shard sends its
 * series' samples in file order, one request at a time over its own keep-alive connection, at
 * most SAMPLES_PER_REQUEST samples per request (Prometheus: max_samples_per_send 2000, up to
 * max_shards 50). A series never goes to two shards, so its samples arrive in order.
 *
 * All requests are encoded and compressed before the clock starts, in parallel, as the other
 * loaders send files converted ahead of time; the time runs from the first request to the last
 * acknowledgement. The loader prints its own CPU time during that phase, to show it is not the
 * bottleneck. 429, 5xx and dropped connections are retried (and counted), any other error stops
 * the load: Prometheus answers 400 for samples it rejects (out of bounds, out of order).
 *
 * The protobuf is written by hand (five message types, a few lines); snappy is the reference
 * library's C interface, declared here so only the shared library is needed, not its headers.
 */
#define _GNU_SOURCE
#include "httpc.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>

typedef enum { SNAPPY_OK = 0, SNAPPY_INVALID_INPUT = 1, SNAPPY_BUFFER_TOO_SMALL = 2 } snappy_status;
snappy_status snappy_compress(const char *input, size_t input_length, char *compressed, size_t *compressed_length);
size_t snappy_max_compressed_length(size_t source_length);

#pragma pack(push, 1)
typedef struct { uint32_t s; int64_t t; float v; } row;
#pragma pack(pop)

static const row *rows;
static size_t nrows, per_req;
static uint32_t maxseries;
static int shards, port;
static const char *host;
static const char *HEADERS = "Content-Encoding: snappy\r\nContent-Type: application/x-protobuf\r\n"
                             "X-Prometheus-Remote-Write-Version: 0.1.0\r\nUser-Agent: dbcmp-loadprw\r\n";
static atomic_ulong posts, retries;
static atomic_ullong raw_bytes, wire_bytes;

typedef struct body { char *p; size_t n; } body;
typedef struct shard {
    int id;
    body *b;
    size_t nb, cap;
} shard;

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}
static double cpu(void)
{
    struct rusage u;
    getrusage(RUSAGE_SELF, &u);
    return u.ru_utime.tv_sec + u.ru_utime.tv_usec / 1e6 + u.ru_stime.tv_sec + u.ru_stime.tv_usec / 1e6;
}

static size_t vlen(uint64_t x)
{
    size_t n = 1;
    while (x >= 0x80) { x >>= 7; n++; }
    return n;
}
static char *vput(char *p, uint64_t x)
{
    while (x >= 0x80) { *p++ = (char)(x | 0x80); x >>= 7; }
    *p++ = (char)x;
    return p;
}

/* A WriteRequest of the rows idx[0..n) (file order); one TimeSeries per series, its samples in order. */
static body encode(const uint32_t *idx, size_t n, char *pb, size_t *order, uint32_t *ser, size_t *cnt, int32_t *slot)
{
    /* group by series, keeping file order within each: a counting sort over the series present */
    size_t ns = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t s = rows[idx[i]].s;
        if (slot[s] < 0) { slot[s] = (int32_t)ns; ser[ns] = s; cnt[ns++] = 0; }
        cnt[slot[s]]++;
    }
    size_t off = 0;
    for (size_t k = 0; k < ns; k++) { size_t c = cnt[k]; cnt[k] = off; off += c; }
    for (size_t i = 0; i < n; i++) order[cnt[slot[rows[idx[i]].s]]++] = idx[i];
    for (size_t k = 0; k < ns; k++) slot[ser[k]] = -1;
    char *p = pb;
    size_t a = 0;
    for (size_t k = 0; k < ns; k++) {
        size_t e = cnt[k]; /* after the sort cnt[k] is the end of series k's samples */
        char sv[16];
        int svl = snprintf(sv, sizeof sv, "%u", ser[k]);
        size_t l2 = 2 + 6 + 2 + (size_t)svl;            /* Label{name: "series", value: sv} */
        size_t ts = 2 + 19 + 2 + l2;                    /* labels: __name__="sensors", series=sv */
        for (size_t i = a; i < e; i++) {
            size_t sl = 1 + 8 + 1 + vlen((uint64_t)rows[order[i]].t);  /* Sample{value, timestamp} */
            ts += 1 + vlen(sl) + sl;
        }
        *p++ = 0x0a; /* WriteRequest.timeseries */
        p = vput(p, ts);
        memcpy(p, "\x0a\x13\x0a\x08__name__\x12\x07sensors", 21);
        p += 21;
        *p++ = 0x0a;
        p = vput(p, l2);
        *p++ = 0x0a; *p++ = 6; memcpy(p, "series", 6); p += 6;
        *p++ = 0x12; *p++ = (char)svl; memcpy(p, sv, (size_t)svl); p += svl;
        for (size_t i = a; i < e; i++) {
            const row *r = &rows[order[i]];
            size_t sl = 1 + 8 + 1 + vlen((uint64_t)r->t);
            *p++ = 0x12; /* TimeSeries.samples */
            p = vput(p, sl);
            double v = r->v;
            *p++ = 0x09; /* Sample.value, fixed64 */
            memcpy(p, &v, 8); /* little endian, as protobuf */
            p += 8;
            *p++ = 0x10; /* Sample.timestamp, varint */
            p = vput(p, (uint64_t)r->t);
        }
        a = e;
    }
    size_t raw = (size_t)(p - pb), cl = snappy_max_compressed_length(raw);
    body b = {malloc(cl), cl};
    if (snappy_compress(pb, raw, b.p, &b.n) != SNAPPY_OK) { fprintf(stderr, "snappy_compress failed\n"); exit(1); }
    b.p = realloc(b.p, b.n);
    atomic_fetch_add(&raw_bytes, raw);
    atomic_fetch_add(&wire_bytes, b.n);
    return b;
}

static void *prepare(void *arg)
{
    shard *sh = arg;
    uint32_t *idx = malloc(per_req * sizeof *idx), *ser = malloc(per_req * sizeof *ser);
    size_t *order = malloc(per_req * sizeof *order), *cnt = malloc(per_req * sizeof *cnt), n = 0;
    char *pb = malloc(per_req * (22 + 64) + 64); /* a sample takes at most 22 bytes, a series' header 64 */
    int32_t *slot = malloc(((size_t)maxseries + 1) * sizeof *slot);
    memset(slot, 0xff, ((size_t)maxseries + 1) * sizeof *slot);
    for (size_t i = 0;; i++) {
        if (i < nrows && rows[i].s % (uint32_t)shards == (uint32_t)sh->id) idx[n++] = (uint32_t)i;
        if (n == per_req || (i == nrows && n)) {
            if (sh->nb == sh->cap) sh->b = realloc(sh->b, (sh->cap = sh->cap ? sh->cap * 2 : 1024) * sizeof *sh->b);
            sh->b[sh->nb++] = encode(idx, n, pb, order, ser, cnt, slot);
            n = 0;
        }
        if (i == nrows) break;
    }
    free(idx); free(ser); free(order); free(cnt); free(pb); free(slot);
    return NULL;
}

static void *send_shard(void *arg)
{
    shard *sh = arg;
    hconn c;
    if (h_init(&c, host, port) != 0) exit(1);
    hbuf resp = {0};
    for (size_t k = 0; k < sh->nb; k++) {
        for (int tries = 0;; tries++) {
            resp.n = 0;
            int st = h_request(&c, "POST", "/api/v1/write", HEADERS, sh->b[k].p, sh->b[k].n, &resp, NULL);
            if (st >= 200 && st < 300) break;
            if ((st == 429 || st >= 500 || st < 0) && tries < 50) {
                atomic_fetch_add(&retries, 1);
                fprintf(stderr, "retry shard %d request %zu: status %d %.200s\n", sh->id, k, st, resp.p ? resp.p : "");
                usleep(200000);
                continue;
            }
            fprintf(stderr, "shard %d request %zu: status %d: %.500s\n", sh->id, k, st, resp.p ? resp.p : "");
            exit(1);
        }
        atomic_fetch_add(&posts, 1);
    }
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc != 6) {
        fprintf(stderr, "usage: loadprw FILE HOST PORT SAMPLES_PER_REQUEST SHARDS\n");
        return 2;
    }
    host = argv[2];
    port = atoi(argv[3]);
    per_req = strtoull(argv[4], NULL, 10);
    shards = atoi(argv[5]);
    if (per_req < 1 || shards < 1 || shards > 256) {
        fprintf(stderr, "SAMPLES_PER_REQUEST >= 1, SHARDS 1 to 256\n");
        return 2;
    }
    int fd = open(argv[1], O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0 || st.st_size == 0) {
        fprintf(stderr, "%s: missing, empty or unreadable\n", argv[1]);
        return 1;
    }
    rows = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    if (rows == MAP_FAILED) {
        perror(argv[1]);
        return 1;
    }
    nrows = (size_t)st.st_size / sizeof(row);
    if (nrows > UINT32_MAX) {
        fprintf(stderr, "at most %u rows\n", UINT32_MAX);
        return 1;
    }
    for (size_t i = 0; i < nrows; i++) maxseries = rows[i].s > maxseries ? rows[i].s : maxseries;
    shard *sh = calloc((size_t)shards, sizeof *sh);
    pthread_t th[256];
    double t0 = now();
    for (int i = 0; i < shards; i++) {
        sh[i].id = i;
        pthread_create(&th[i], NULL, prepare, &sh[i]);
    }
    size_t reqs = 0;
    for (int i = 0; i < shards; i++) {
        pthread_join(th[i], NULL);
        reqs += sh[i].nb;
    }
    fprintf(stderr, "%zu rows in %zu requests of up to %zu samples, %d shards; encoded in %.2f s (not counted): "
            "%.1f MB protobuf, %.1f MB snappy\n", nrows, reqs, per_req, shards, now() - t0,
            atomic_load(&raw_bytes) / 1e6, atomic_load(&wire_bytes) / 1e6);
    double c0 = cpu();
    t0 = now();
    for (int i = 0; i < shards; i++) pthread_create(&th[i], NULL, send_shard, &sh[i]);
    for (int i = 0; i < shards; i++) pthread_join(th[i], NULL);
    double dt = now() - t0, dc = cpu() - c0;
    printf("loaded %zu rows in %.2f s = %.3f M rows/s (%lu requests, %lu retries, %d shards, %zu samples/request; "
           "loader CPU %.2f s = %.2f cores)\n", nrows, dt, nrows / dt / 1e6, (unsigned long)posts,
           (unsigned long)retries, shards, per_req, dc, dc / dt);
    return 0;
}
