/*
 * Compression and decode speed of the value types beyond float32: the
 * sensor streams of datasets.h as float64, integer counters, flags and
 * states, each coded per granule of 8192 values the way parts store them
 * (AUTO, with ZSTD(1) where it pays).
 *
 *   type-bench
 */
#include "../src/internal.h"
#include "common.h"
#include "datasets.h"

#include <math.h>
#include <stdlib.h>

#define N (1u << 20)
#define GR 8192

static const char *kind_name(int k)
{
    switch (k & 0x0f) {
    case BLK_F64_DECIMAL: return "decimal";
    case BLK_XOR64: return "gorilla";
    case BLK_BYTESPLIT64: return "bytesplit";
    case BLK_INT: return "int-bp";
    default: return "?";
    }
}

/* bits per value and decode speed of n values of `type`, granule by granule */
static void measure(const char *name, int type, const void *v, size_t n)
{
    size_t w = type_width(type);
    codec_ctx *c = codec_new();
    uint8_t *blk = malloc(block_bound(GR) + BLK_SLACK);
    uint8_t *out = malloc(GR * 8 + BLK_SLACK);
    size_t bytes = 0, kinds[16] = {0};
    for (size_t g = 0; g < n; g += GR) {
        size_t m = MIN((size_t)GR, n - g);
        size_t len = block_encode_vals(c, 1, TSDB_VALUES_AUTO, type, (const uint8_t *)v + g * w, m, blk);
        bytes += len;
        kinds[blk[0] & 0x0f]++;
    }
    /* decode speed: one granule decoded repeatedly, each in turn */
    double t0 = now_s(), decoded = 0;
    for (int rep = 0; rep < 3; rep++)
        for (size_t g = 0; g < n; g += GR) {
            size_t m = MIN((size_t)GR, n - g);
            size_t len = block_encode_vals(c, 1, TSDB_VALUES_AUTO, type, (const uint8_t *)v + g * w, m, blk);
            double d0 = now_s();
            if (block_decode_vals(c, type, blk, len, m, out) != 0 || memcmp(out, (const uint8_t *)v + g * w, m * w)) {
                fprintf(stderr, "%s: round trip failed\n", name);
                exit(1);
            }
            decoded += now_s() - d0;
        }
    (void)t0;
    int top = 0;
    for (int k = 0; k < 16; k++)
        top = kinds[k] > kinds[top] ? k : top;
    printf("%-24s %-8s %8.2f %7.1fx %8.0f   %s\n", name, (const char *[]){"float32", "float64", "int64", "bool", "state"}[type],
           8.0 * (double)bytes / (double)n, (double)(n * (type == TSDB_BOOL ? 1 : w)) / (double)bytes,
           3.0 * (double)n / decoded / 1e6, kind_name(top));
    free(blk);
    free(out);
    codec_free(c);
}

int main(void)
{
    double *d = malloc(N * sizeof *d);
    int64_t *iv = malloc(N * sizeof *iv);
    uint32_t *u = malloc(N * sizeof *u);
    printf("%-24s %-8s %8s %8s %8s   %s\n", "dataset", "type", "bits/val", "ratio", "dec M/s", "codec (most granules)");
    for (int k = 0; k < DS_KINDS; k++) {
        ds_state st;
        ds_init(&st, (ds_kind)k, 7, 1700000000000);
        int64_t ts;
        for (size_t i = 0; i < N; i++)
            ds_next_f64(&st, &ts, &d[i]);
        char name[64];
        snprintf(name, sizeof name, "%s", ds_info[k].name);
        measure(name, TSDB_FLOAT64, d, N);
    }
    uint64_t r = 1;
    double flow = 0, liters = 0;
    for (size_t i = 0; i < N; i++) { /* a totalizer in whole liters, 1 Hz at ~245 m3/h */
        r = ds_mix(r);
        flow = 245 + 5 * sin((double)i / 3600) + (double)(r % 100) / 100;
        liters += flow / 3.6;
        iv[i] = (int64_t)liters;
    }
    measure("totalizer, liters", TSDB_INT64, iv, N);
    for (size_t i = 0; i < N; i++) { /* packets per second */
        r = ds_mix(r);
        iv[i] = 50000 + (int64_t)(r % 20000);
    }
    measure("rate, random 50k-70k", TSDB_INT64, iv, N);
    for (size_t i = 0; i < N; i++) /* an event id */
        iv[i] = 9000000000000 + (int64_t)i * 3;
    measure("sequence", TSDB_INT64, iv, N);
    for (size_t i = 0; i < N; i++) { /* a door: changes a few times an hour */
        r = ds_mix(r);
        u[i] = i ? (r % 1200 == 0 ? !u[i - 1] : u[i - 1]) : 0;
    }
    measure("door, 1 Hz", TSDB_BOOL, u, N);
    for (size_t i = 0; i < N; i++) {
        r = ds_mix(r);
        u[i] = r & 1;
    }
    measure("coin flips", TSDB_BOOL, u, N);
    for (size_t i = 0; i < N; i++) { /* a machine mode of 5: changes every few minutes */
        r = ds_mix(r);
        u[i] = i ? (r % 300 == 0 ? 1 + (uint32_t)(r >> 32) % 5 : u[i - 1]) : 1;
    }
    measure("machine mode", TSDB_STATE, u, N);
    free(d);
    free(iv);
    free(u);
    return 0;
}
