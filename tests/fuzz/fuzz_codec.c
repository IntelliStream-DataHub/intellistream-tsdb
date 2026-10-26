/*
 * Every block decoder on arbitrary bytes. Granule payloads are hashed only
 * with verify_checksums, so a damaged or crafted part hands its bytes straight
 * to these: they must not read or write out of bounds, whatever the bytes.
 *
 * Input: byte 0 picks the decoder (the block dispatchers by type, or one
 * transform directly), bytes 1-2 the row count, the rest is the block. Like a
 * mapped part, the block is followed by BLK_SLACK readable bytes.
 */
#include "fuzz.h"
#include "internal.h"

static codec_ctx *cc;
static uint8_t *buf;
static void *out;

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    cc = codec_new();
    out = malloc(MAX_GRANULE_ROWS * 8);
    return cc && out ? 0 : -1;
}

enum { NSEL = 27 };

static int decode(int sel, const uint8_t *p, size_t len, size_t n)
{
    switch (sel) {
    case 0: return block_decode_ts(cc, p, len, n, out);
    case 1: case 2: case 3: case 4: case 5: return block_decode_vals(cc, sel - 1, p, len, n, out);
    case 6: return dd_decode(p, len, n, out);
    case 7: return gorilla_decode(p, len, n, out);
    case 8: return chimp_decode(p, len, n, out);
    case 9: return chimp128_decode(p, len, n, out);
    case 10: return patas_decode(p, len, n, out);
    case 11: return delta32_decode(p, len, n, out);
    case 12: return bytesplit_decode(p, len, n, out);
    case 13: return ts_grid_decode(p, len, n, out);
    case 14: return ts_delta_decode(p, len, n, out);
    case 15: return decimal_bp_decode(p, len, n, out);
    case 16: return alp_decode(p, len, n, out);
    case 17: return alprd_decode(p, len, n, out);
    case 18: return decimal_decode(p, len, n, out);
    case 19: return int_bp_decode(p, len, n, out);
    case 20: return f64_decimal_decode(p, len, n, out);
    case 21: return xor64_decode(p, len, n, out);
    case 22: return delta_bp32_decode(p, len, n, out);
    case 23: return delta_bp64_decode(p, len, n, out);
    case 24: return alprd64_decode(p, len, n, out);
    case 25: return dict_decode(p, len, n, 4, out);
    default: return dict_decode(p, len, n, 8, out);
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!cc)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size < 3)
        return 0;
    int sel = data[0] % NSEL;
    size_t n = 1 + (size_t)(data[1] | data[2] << 8) % MAX_GRANULE_ROWS;
    size_t len = size - 3;
    free(buf);
    buf = malloc(len + BLK_SLACK);
    memcpy(buf, data + 3, len);
    memset(buf + len, 0xa5, BLK_SLACK);
    decode(sel, buf, len, n);
    return 0;
}

/* ------------------------------------------------------------- seeds */

static void seed(const char *dir, int sel, size_t n, const uint8_t *blk, size_t len)
{
    uint8_t *b = malloc(len + 3);
    b[0] = (uint8_t)sel;
    b[1] = (uint8_t)((n - 1) & 0xff);
    b[2] = (uint8_t)((n - 1) >> 8);
    memcpy(b + 3, blk, len);
    char name[64];
    static int k;
    snprintf(name, sizeof name, "seed-%02d-%03d", sel, k++);
    fuzz_put(dir, name, b, len + 3);
    free(b);
}

/* the raw decoder for a block kind of a type, or -1 */
static int raw_sel(int kind, int type)
{
    bool f64 = type == TSDB_FLOAT64;
    switch (kind) {
    case BLK_DD: return 6;
    case BLK_GORILLA: return 7;
    case BLK_DECIMAL: return 18;
    case BLK_CHIMP: return 8;
    case BLK_DELTA32: return 11;
    case BLK_BYTESPLIT: return 12;
    case BLK_TS_DELTA: return 14;
    case BLK_TS_GRID: return 13;
    case BLK_ALPRD: return f64 ? 24 : 17;
    case BLK_DELTA_BP: return f64 ? 23 : 22;
    case BLK_DECIMAL_BP: return 15;
    case BLK_F64_DECIMAL: return 20;
    case BLK_XOR64: return 21;
    case BLK_INT: return 19;
    case BLK_DICT: return f64 ? 26 : 25;
    default: return -1;
    }
}

static void seed_block(const char *dir, int sel, size_t n, const uint8_t *blk, size_t len)
{
    seed(dir, sel, n, blk, len);
    int kind;
    const uint8_t *p;
    size_t plen;
    int type = sel - 1; /* the block dispatchers by type */
    if (block_open(cc, blk, len, n, &kind, &p, &plen) == 0 && raw_sel(kind, type) >= 0)
        seed(dir, raw_sel(kind, type), n, p, plen);
}

void fuzz_seeds(const char *dir)
{
    enum { N = 200 };
    int64_t ts[N];
    float f[N];
    double d[N];
    int64_t iv[N];
    uint32_t st[N];
    uint8_t *blk = malloc(block_bound(N) + BLK_SLACK);
    uint64_t r = 1;
    for (int shape = 0; shape < 5; shape++) {
        for (int i = 0; i < N; i++) {
            r = r * 6364136223846793005ull + 1442695040888963407ull;
            int jitter = (int)(r >> 60);
            ts[i] = 1700000000000 + (int64_t)i * (shape == 2 ? 997 : 1000) + (shape == 1 ? jitter : 0);
            double x = shape == 0 ? 20 + i * 0.1 : shape == 1 ? (double)(r >> 40) / 1e3 : (double)(r >> 11) / 1e15;
            if (shape == 3) /* converted units, unrounded: packed bit deltas */
                x = 14.503773773 * (1.5 + 0.001 * (double)(i % 37) + 0.0001 * (double)(r >> 61));
            if (shape == 4) /* the same, of few distinct values: a dictionary at merge effort */
                x = 14.503773773 * (1.5 + 0.001 * (double)(i % 23));
            d[i] = shape == 0 ? (double)(int64_t)(x * 10) / 10 : x;
            f[i] = (float)d[i];
            iv[i] = shape == 0 ? 1000 + i : shape == 1 ? (int64_t)(r >> 45) : (int64_t)r;
            st[i] = shape == 0 ? (uint32_t)(i / 50 & 1) : (uint32_t)(r >> 62);
        }
        /* the transforms AUTO may pass over on these values, directly */
        seed(dir, 22, N, blk, delta_bp32_encode((const uint32_t *)f, N, blk));
        seed(dir, 23, N, blk, delta_bp64_encode((const uint64_t *)d, N, blk));
        seed(dir, 24, N, blk, alprd64_encode((const uint64_t *)d, N, blk));
        seed(dir, 25, N, blk, dict_encode(f, N, 4, blk));
        seed(dir, 26, N, blk, dict_encode(d, N, 8, blk));
        for (int k = 0; k < 4; k++) {
            int lvl = k & 1;
            cc->effort = k >> 1 ? CODEC_MERGE : CODEC_FAST; /* the kinds of both efforts */
            size_t len = block_encode_ts(cc, lvl, ts, N, blk);
            seed_block(dir, 0, N, blk, len);
            for (int vc = 0; vc <= 2; vc++) {
                len = block_encode_f32(cc, lvl, vc, f, N, blk);
                seed_block(dir, 1 + TSDB_FLOAT32, N, blk, len);
            }
            len = block_encode_vals(cc, lvl, 0, TSDB_FLOAT64, d, N, blk);
            seed_block(dir, 1 + TSDB_FLOAT64, N, blk, len);
            len = block_encode_vals(cc, lvl, 0, TSDB_INT64, iv, N, blk);
            seed_block(dir, 1 + TSDB_INT64, N, blk, len);
            len = block_encode_vals(cc, lvl, 0, TSDB_BOOL, st, N, blk);
            seed_block(dir, 1 + TSDB_BOOL, N, blk, len);
            len = block_encode_vals(cc, lvl, 0, TSDB_STATE, st, N, blk);
            seed_block(dir, 1 + TSDB_STATE, N, blk, len);
        }
    }
    free(blk);
}
