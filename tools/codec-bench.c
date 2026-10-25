/*
 * Float32 codec comparison: size and speed per 8192-value granule, the unit
 * tsdb stores. Every result is checked to round-trip bit for bit.
 *
 *   codec-bench                 the synthetic sensor streams of datasets.h
 *   codec-bench --file F        F holds raw float32 values (little-endian), one series in time order;
 *                               prints one line per setting (codec alone, + ZSTD(1)): codec=bits per value
 */
#include "../src/internal.h"
#include "common.h"
#include "datasets.h"

#include <math.h>

#define GR 8192
static size_t NGR = 128;
static size_t N = GR * 128;

/* ---- datasets: the sensor streams of datasets.h ---- */
static void gen(int kind, float *v)
{
    ds_state st;
    ds_init(&st, (ds_kind)kind, 3, 1704067200000);
    for (size_t i = 0; i < N; i++) {
        int64_t t;
        ds_next(&st, &t, &v[i]);
    }
}
#define NDS DS_KINDS

/* ---- codecs (transform only; zstd is layered on in main) ---- */
static size_t raw_enc(const float *in, size_t n, uint8_t *o) { memcpy(o, in, n * 4); return n * 4; }
static int raw_dec(const uint8_t *in, size_t len, size_t n, float *o) { memcpy(o, in, n * 4); return len == n * 4 ? 0 : -1; }

static size_t bss_enc(const float *in, size_t n, uint8_t *o)
{
    const uint8_t *b = (const uint8_t *)in;
    for (size_t i = 0; i < n; i++)
        for (int k = 0; k < 4; k++)
            o[k * n + i] = b[4 * i + k];
    return n * 4;
}
static int bss_dec(const uint8_t *in, size_t len, size_t n, float *o)
{
    uint8_t *b = (uint8_t *)o;
    for (size_t i = 0; i < n; i++)
        for (int k = 0; k < 4; k++)
            b[4 * i + k] = in[k * n + i];
    return len == n * 4 ? 0 : -1;
}

static size_t delta_enc(const float *in, size_t n, uint8_t *o)
{
    const uint32_t *u = (const uint32_t *)in;
    uint32_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t d = u[i] - prev;
        prev = u[i];
        memcpy(o + 4 * i, &d, 4);
    }
    return n * 4;
}
static int delta_dec(const uint8_t *in, size_t len, size_t n, float *o)
{
    uint32_t *u = (uint32_t *)o, prev = 0;
    for (size_t i = 0; i < n; i++) {
        prev += load32(in + 4 * i);
        u[i] = prev;
    }
    return len == n * 4 ? 0 : -1;
}

#define XOR_WRAP(name)                                                                                  \
    static size_t name##_e(const float *in, size_t n, uint8_t *o) { return name##_encode((const uint32_t *)in, n, o); } \
    static int name##_d(const uint8_t *in, size_t len, size_t n, float *o) { return name##_decode(in, len, n, (uint32_t *)o); }
XOR_WRAP(gorilla)
XOR_WRAP(chimp)
XOR_WRAP(chimp128)
XOR_WRAP(patas)

static size_t alp_e(const float *in, size_t n, uint8_t *o) { return alp_encode(in, n, o); }
static int alp_d(const uint8_t *in, size_t len, size_t n, float *o) { return alp_decode(in, len, n, o); }
static size_t alprd_e(const float *in, size_t n, uint8_t *o) { return alprd_encode((const uint32_t *)in, n, o); }
static int alprd_d(const uint8_t *in, size_t len, size_t n, float *o) { return alprd_decode(in, len, n, (uint32_t *)o); }
/* the paper's scheme: ALP where it applies, ALP_rd otherwise (1 tag byte) */
static size_t alpx_e(const float *in, size_t n, uint8_t *o)
{
    size_t r = alp_encode(in, n, o + 1);
    o[0] = r != 0;
    return 1 + (r ? r : alprd_encode((const uint32_t *)in, n, o + 1));
}
static int alpx_d(const uint8_t *in, size_t len, size_t n, float *o)
{
    return in[0] ? alp_decode(in + 1, len - 1, n, o) : alprd_decode(in + 1, len - 1, n, (uint32_t *)o);
}

static size_t dbp_e(const float *in, size_t n, uint8_t *o) { return decimal_bp_encode(in, n, o); }
static int dbp_d(const uint8_t *in, size_t len, size_t n, float *o) { return decimal_bp_decode(in, len, n, o); }

static size_t dbd_e(const float *in, size_t n, uint8_t *o) { return delta_bp32_encode((const uint32_t *)in, n, o); }
static int dbd_d(const uint8_t *in, size_t len, size_t n, float *o) { return delta_bp32_decode(in, len, n, (uint32_t *)o); }

static size_t dec_e(const float *in, size_t n, uint8_t *o) { return decimal_encode(in, n, o); }
static int dec_d(const uint8_t *in, size_t len, size_t n, float *o) { return decimal_decode(in, len, n, o); }

/* tsdb's production AUTO block (includes its own ZSTD choice; see run()) */
static codec_ctx *auto_cc;
static int auto_level;
static size_t auto_e(const float *in, size_t n, uint8_t *o) { return block_encode_f32(auto_cc, auto_level, TSDB_VALUES_AUTO, in, n, o); }
static int auto_d(const uint8_t *in, size_t len, size_t n, float *o) { return block_decode_f32(auto_cc, in, len, n, o); }

typedef struct codec {
    const char *name;
    size_t (*enc)(const float *, size_t, uint8_t *);
    int (*dec)(const uint8_t *, size_t, size_t, float *);
} codec;

static const codec codecs[] = {
    {"raw", raw_enc, raw_dec},          {"bytesplit", bss_enc, bss_dec}, {"delta", delta_enc, delta_dec},
    {"gorilla", gorilla_e, gorilla_d},  {"chimp", chimp_e, chimp_d},     {"chimp128", chimp128_e, chimp128_d},
    {"patas", patas_e, patas_d},        {"alp", alp_e, alp_d},           {"alp_rd", alprd_e, alprd_d},
    {"alp|rd", alpx_e, alpx_d},         {"decimal", dec_e, dec_d},       {"dec-bp", dbp_e, dbp_d},
    {"bitdelta", dbd_e, dbd_d},         {"auto", auto_e, auto_d},
};
#define NC (sizeof codecs / sizeof *codecs)

typedef struct result {
    double bits;      /* per value; NAN if not applicable */
    double enc_mvs, dec_mvs;
} result;

static result run(const codec *c, const float *v, int zlevel, ZSTD_CCtx *cc, ZSTD_DCtx *dc)
{
    bool self_zstd = c->enc == auto_e; /* the block encoder applies ZSTD itself */
    auto_level = zlevel;
    if (self_zstd)
        zlevel = 0;
    size_t cap = MAX(MAX(block_bound(GR), decimal_bp_bound(GR)), MAX(alp_bound(GR), alprd_bound(GR))) + 64;
    cap = MAX(cap, delta_bp32_bound(GR) + 64);
    uint8_t *inner = calloc(1, cap + BLK_SLACK), *z = calloc(1, ZSTD_compressBound(cap) + BLK_SLACK);
    uint8_t **blocks = calloc(NGR, sizeof *blocks);
    size_t *blen = calloc(NGR, sizeof *blen), *ilen = calloc(NGR, sizeof *ilen);
    float *out = malloc(GR * sizeof *out);
    result r = {0, 0, 0};
    size_t total = 0;
    double t0 = now_s();
    for (size_t g = 0; g < NGR; g++) {
        size_t m = MIN((size_t)GR, N - (size_t)g * GR);
        size_t n = c->enc(v + (size_t)g * GR, m, inner);
        if (!n) {
            r.bits = NAN;
            goto out;
        }
        ilen[g] = n;
        if (zlevel) {
            size_t zn = ZSTD_compressCCtx(cc, z, ZSTD_compressBound(n), inner, n, zlevel);
            blocks[g] = malloc(zn + BLK_SLACK);
            memcpy(blocks[g], z, zn);
            blen[g] = zn;
        } else {
            blocks[g] = malloc(n + BLK_SLACK);
            memcpy(blocks[g], inner, n);
            blen[g] = n;
        }
        total += blen[g];
    }
    double te = now_s() - t0;
    /* decode several rounds for stable timing */
    int rounds = 5;
    t0 = now_s();
    for (int k = 0; k < rounds; k++) {
        for (size_t g = 0; g < NGR; g++) {
            const uint8_t *src = blocks[g];
            size_t len = blen[g];
            if (zlevel) {
                ZSTD_decompressDCtx(dc, inner, cap, src, len);
                memset(inner + ilen[g], 0, BLK_SLACK);
                src = inner;
                len = ilen[g];
            }
            size_t m = MIN((size_t)GR, N - (size_t)g * GR);
            if (c->dec(src, len, m, out) != 0 || (k == 0 && memcmp(out, v + (size_t)g * GR, m * 4) != 0)) {
                fprintf(stderr, "ROUND TRIP FAILED: %s granule %zu\n", c->name, g);
                exit(1);
            }
        }
    }
    double td = (now_s() - t0) / rounds;
    r.bits = total * 8.0 / N;
    r.enc_mvs = N / te / 1e6;
    r.dec_mvs = N / td / 1e6;
out:
    for (size_t g = 0; g < NGR; g++)
        free(blocks[g]);
    free(blocks);
    free(blen);
    free(ilen);
    free(inner);
    free(z);
    free(out);
    return r;
}

int main(int argc, char **argv)
{
    if (argc == 3 && !strcmp(argv[1], "--file")) { /* raw little-endian float32 values: one line of bits/value */
        FILE *f = fopen(argv[2], "rb");
        if (!f || fseek(f, 0, SEEK_END) || (N = (size_t)ftell(f) / 4) == 0) {
            fprintf(stderr, "%s: cannot read, or empty\n", argv[2]);
            return 1;
        }
        rewind(f);
        NGR = (N + GR - 1) / GR;
        float *fv = malloc(N * 4);
        if (!fv || fread(fv, 4, N, f) != N) {
            fprintf(stderr, "%s: short read\n", argv[2]);
            return 1;
        }
        fclose(f);
        ZSTD_CCtx *zc = ZSTD_createCCtx();
        ZSTD_DCtx *zd = ZSTD_createDCtx();
        auto_cc = codec_new();
        for (int z = 0; z < 2; z++) {
            const char *base = strrchr(argv[2], '/');
            printf("%s zstd=%d", base ? base + 1 : argv[2], z);
            for (size_t c = 0; c < NC; c++)
                printf(" %s=%.3f", codecs[c].name, run(&codecs[c], fv, z, zc, zd).bits);
            printf("\n");
        }
        return 0;
    }
    if (argc > 1) {
        fprintf(stderr, "usage: codec-bench [--file FLOAT32_FILE]\n");
        return 2;
    }
    float *v = malloc(N * sizeof *v);
    ZSTD_CCtx *cc = ZSTD_createCCtx();
    ZSTD_DCtx *dc = ZSTD_createDCtx();
    auto_cc = codec_new();
    static result res[NDS][NC][2];
    for (size_t d = 0; d < NDS; d++) {
        gen((int)d, v);
        for (size_t c = 0; c < NC; c++)
            for (int z = 0; z < 2; z++)
                res[d][c][z] = run(&codecs[c], v, z, cc, dc);
    }
    for (int z = 0; z < 2; z++) {
        printf("\nbits per value, %s (float32 = 32 bits; granules of %d values)\n", z ? "codec + ZSTD(1)" : "codec alone", GR);
        printf("%-24s", "dataset");
        for (size_t c = 0; c < NC; c++)
            printf("%10s", codecs[c].name);
        printf("\n");
        for (size_t d = 0; d < NDS; d++) {
            printf("%-24s", ds_info[d].name);
            double best = INFINITY;
            for (size_t c = 0; c < NC; c++)
                if (!isnan(res[d][c][z].bits))
                    best = fmin(best, res[d][c][z].bits);
            for (size_t c = 0; c < NC; c++) {
                double b = res[d][c][z].bits;
                if (isnan(b))
                    printf("%10s", "n/a");
                else
                    printf("%9.2f%s", b, b == best ? "*" : " ");
            }
            printf("\n");
        }
    }
    for (int z = 0; z < 2; z++) {
        printf("\nspeed, million values/s, 1 thread, median over datasets (%s)\n",
               z ? "codec + ZSTD(1)" : "codec alone");
        printf("%-24s", "");
        for (size_t c = 0; c < NC; c++)
            printf("%10s", codecs[c].name);
        printf("\n");
        for (int which = 0; which < 2; which++) {
            printf("%-24s", which ? "encode" : "decode");
            for (size_t c = 0; c < NC; c++) {
                double xs[NDS];
                int k = 0;
                for (size_t d = 0; d < NDS; d++)
                    if (!isnan(res[d][c][z].bits))
                        xs[k++] = which ? res[d][c][z].enc_mvs : res[d][c][z].dec_mvs;
                for (int i = 1; i < k; i++)
                    for (int j = i; j > 0 && xs[j - 1] > xs[j]; j--) {
                        double t = xs[j];
                        xs[j] = xs[j - 1];
                        xs[j - 1] = t;
                    }
                printf("%10.0f", k ? xs[k / 2] : 0.0);
            }
            printf("\n");
        }
    }
    ZSTD_freeCCtx(cc);
    ZSTD_freeDCtx(dc);
    free(v);
    return 0;
}
