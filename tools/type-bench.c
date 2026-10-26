/*
 * Compression and decode speed of the value types beyond float32: the
 * sensor streams of datasets.h as float64, integer counters, flags and
 * states, each coded per granule of 8192 values the way parts store them
 * (AUTO, with ZSTD(1) where it pays).
 *
 *   type-bench
 *   type-bench --file F float32|float64   F holds raw values of that type (little-endian), one series in
 *                                         time order: AUTO as above, the block kinds it chose, and for
 *                                         float64 each transform alone and with ZSTD(1)
 *   type-bench --merge [...]              AUTO as background merges write (CODEC_MERGE: with the
 *                                         dictionary) instead of as inserts write (CODEC_FAST)
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
    switch (k & BLK_KIND_MASK) {
    case BLK_F64_DECIMAL: return "decimal";
    case BLK_XOR64: return "gorilla";
    case BLK_BYTESPLIT64: return "bytesplit";
    case BLK_INT: return "int-bp";
    case BLK_DELTA_BP: return "bit-delta";
    case BLK_ALPRD: return "alp_rd";
    case BLK_DICT: return "dict";
    case BLK_F32_WIDE: return "float32";
    default: return "?";
    }
}

static int show_kinds;
static int effort = CODEC_FAST;

/* bits per value and decode speed of n values of `type`, granule by granule */
static void measure(const char *name, int type, const void *v, size_t n)
{
    size_t w = type_width(type);
    codec_ctx *c = codec_new();
    c->effort = effort;
    uint8_t *blk = malloc(block_bound(GR) + BLK_SLACK);
    uint8_t *out = malloc(GR * 8 + BLK_SLACK);
    size_t bytes = 0, kinds[BLK_KIND_MASK + 1] = {0};
    for (size_t g = 0; g < n; g += GR) {
        size_t m = MIN((size_t)GR, n - g);
        size_t len = block_encode_vals(c, 1, TSDB_VALUES_AUTO, type, (const uint8_t *)v + g * w, m, blk);
        bytes += len;
        kinds[blk[0] & BLK_KIND_MASK]++;
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
    for (int k = 0; k <= BLK_KIND_MASK; k++)
        top = kinds[k] > kinds[top] ? k : top;
    printf("%-24s %-8s %8.2f %7.1fx %8.0f   %s\n", name, (const char *[]){"float32", "float64", "int64", "bool", "state"}[type],
           8.0 * (double)bytes / (double)n, (double)(n * (type == TSDB_BOOL ? 1 : w)) / (double)bytes,
           3.0 * (double)n / decoded / 1e6, kind_name(top));
    if (show_kinds) {
        printf("    kinds:");
        for (int k = 0; k <= BLK_KIND_MASK; k++)
            if (kinds[k])
                printf(" %s(%d)=%zu", kind_name(k), k, kinds[k]);
        printf("\n");
    }
    free(blk);
    free(out);
    codec_free(c);
}

/* one float64 transform alone and + ZSTD(1), granule by granule; NAN if it refuses a granule */
static void one64(const char *name, size_t (*enc)(const uint64_t *, size_t, uint8_t *), const double *v, size_t n)
{
    uint8_t *o = malloc(GR * 16 + 4096), *z = malloc(ZSTD_compressBound(GR * 16 + 4096));
    ZSTD_CCtx *zc = ZSTD_createCCtx();
    size_t raw = 0, zs = 0;
    for (size_t g = 0; g < n; g += GR) {
        size_t m = MIN((size_t)GR, n - g), len = enc((const uint64_t *)(v + g), m, o);
        if (!len) {
            printf("    %-10s n/a\n", name);
            goto out;
        }
        size_t zl = ZSTD_compressCCtx(zc, z, ZSTD_compressBound(len), o, len, 1);
        raw += len;
        zs += MIN(zl, len);
    }
    printf("    %-10s alone %.3f  +zstd %.3f bits/value\n", name, 8.0 * raw / n, 8.0 * zs / n);
out:
    ZSTD_freeCCtx(zc);
    free(o);
    free(z);
}
static size_t dec64_w(const uint64_t *in, size_t n, uint8_t *o) { return f64_decimal_encode((const double *)in, n, o); }
static size_t dict64_w(const uint64_t *in, size_t n, uint8_t *o) { return dict_encode(in, n, 8, o); }
static size_t bss64_w(const uint64_t *in, size_t n, uint8_t *o)
{
    const uint8_t *b = (const uint8_t *)in;
    for (size_t i = 0; i < n; i++)
        for (int k = 0; k < 8; k++)
            o[k * n + i] = b[8 * i + k];
    return n * 8;
}

int main(int argc, char **argv)
{
    if (argc > 1 && !strcmp(argv[1], "--merge")) {
        effort = CODEC_MERGE;
        argv++;
        argc--;
    }
    if (argc == 4 && !strcmp(argv[1], "--file")) { /* raw values of type argv[3]: float32 or float64 */
        if (strcmp(argv[3], "float64") && strcmp(argv[3], "float32")) {
            fprintf(stderr, "type-bench --file: the type is float32 or float64\n");
            return 2;
        }
        int type = !strcmp(argv[3], "float64") ? TSDB_FLOAT64 : TSDB_FLOAT32;
        size_t w = type_width(type);
        FILE *f = fopen(argv[2], "rb");
        size_t n;
        if (!f || fseek(f, 0, SEEK_END) || (n = (size_t)ftell(f) / w) == 0) {
            fprintf(stderr, "%s: cannot read, or empty\n", argv[2]);
            return 1;
        }
        rewind(f);
        void *v = malloc(n * w);
        if (!v || fread(v, w, n, f) != n) {
            fprintf(stderr, "%s: short read\n", argv[2]);
            return 1;
        }
        fclose(f);
        show_kinds = 1;
        const char *base = strrchr(argv[2], '/');
        measure(base ? base + 1 : argv[2], type, v, n);
        if (type == TSDB_FLOAT64) {
            one64("decimal", dec64_w, v, n);
            one64("gorilla64", xor64_encode, v, n);
            one64("bytesplit", bss64_w, v, n);
            one64("alp_rd64", alprd64_encode, v, n);
            one64("bitdelta64", delta_bp64_encode, v, n);
            one64("dict64", dict64_w, v, n);
        }
        return 0;
    }
    if (argc > 1) {
        fprintf(stderr, "usage: type-bench [--merge] [--file FILE float32|float64]\n");
        return 2;
    }
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
