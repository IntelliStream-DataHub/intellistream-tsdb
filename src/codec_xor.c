/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * More XOR codecs of the Gorilla family, for comparison (tools/codec-bench).
 * All three were published for float64; these are float32 adaptations:
 *
 * Chimp (Liakos et al., VLDB 2022): leading zeros are rounded to one of 8
 * values (3-bit code) and reused when unchanged; XORs with many trailing zeros
 * store only their center bits.
 *   00                          identical to previous
 *   01 lz:3 len:5 center:len    trailing zeros > CHIMP_THR
 *   10 xor:(32 - lz)            same rounded leading zeros as stored
 *   11 lz:3 xor:(32 - lz)       new leading zeros
 *
 * Chimp128: as Chimp, but the reference may be any of the previous 128
 * values, found through a hash of the low bits; the 7-bit index is written
 * in the 00 and 01 cases.
 *
 * Patas (DuckDB): Chimp128's reference search with a byte-aligned layout: a
 * u16 header per value (ref:7 bytes:3 tz:6) and a stream of significant bytes.
 * Trades size for branch-light decoding.
 */
#include "internal.h"
#include "bits.h"

#include <stdlib.h>

#define CHIMP_THR 5
#define CHIMP128_THR (CHIMP_THR + 7)
#define RING 128
#define KEY_BITS 13
#define NO_LZ 33

/* leading zeros rounded down to one of 8 values */
static const uint8_t lz_round_val[8] = {0, 8, 10, 12, 14, 16, 18, 20};

static inline int lz_code(int lz)
{
    if (lz >= 20) return 7;
    if (lz < 8) return 0;
    return 1 + (lz - 8) / 2;
}

size_t chimp_bound(size_t n) { return 4 + (n * 37 + 7) / 8 + 8; }
size_t chimp128_bound(size_t n) { return 4 + (n * 49 + 7) / 8 + 8; }
size_t patas_bound(size_t n) { return 4 + n * 6 + 8; }

/* ------------------------------------------------------------------ Chimp */

size_t chimp_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    store32(out, in[0]);
    bitwriter w;
    bw_init(&w, out + 4);
    uint32_t prev = in[0];
    int stored = NO_LZ;
    for (size_t i = 1; i < n; i++) {
        uint32_t x = in[i] ^ prev;
        prev = in[i];
        if (!x) {
            bw_put(&w, 0, 2);
            stored = NO_LZ;
            continue;
        }
        int code = lz_code(__builtin_clz(x)), lz = lz_round_val[code], tz = __builtin_ctz(x);
        if (tz > CHIMP_THR) {
            int len = 32 - lz - tz;
            bw_put(&w, ((uint64_t)((1u << 8) | (code << 5) | len) << len) | (x >> tz), 10 + len);
            stored = NO_LZ;
        } else if (lz == stored) {
            bw_put(&w, (2ull << (32 - lz)) | x, 2 + 32 - lz);
        } else {
            bw_put(&w, ((uint64_t)((3u << 3) | code) << (32 - lz)) | x, 5 + 32 - lz);
            stored = lz;
        }
    }
    return 4 + bw_finish(&w);
}

int chimp_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (n == 0)
        return 0;
    if (len < 4)
        return TSDB_ERR_CORRUPT;
    uint32_t prev = load32(in);
    out[0] = prev;
    const uint8_t *b = in + 4;
    const uint64_t limit = (uint64_t)(len - 4) * 8;
    uint64_t pos = 0;
    int stored = NO_LZ;
    for (size_t i = 1; i < n;) {
        if (UNLIKELY(pos >= limit))
            return TSDB_ERR_CORRUPT;
        uint64_t w = br_peek(b, pos);
        uint32_t x;
        switch (w >> 62) {
        case 0: { /* runs of "00": repeat the previous value */
            size_t z = w ? (size_t)__builtin_clzll(w) / 2 : 32;
            z = MIN(MIN(z, (size_t)28), n - i);
            for (size_t j = 0; j < z; j++)
                out[i + j] = prev;
            i += z;
            pos += 2 * z;
            stored = NO_LZ;
            continue;
        }
        case 1: {
            int lz = lz_round_val[(w >> 59) & 7], l = (int)((w >> 54) & 31);
            if (UNLIKELY(l == 0 || lz + l > 32))
                return TSDB_ERR_CORRUPT;
            x = (uint32_t)(((w << 10) >> (64 - l)) << (32 - lz - l));
            pos += 10 + (uint64_t)l;
            stored = NO_LZ;
            break;
        }
        case 2:
            if (UNLIKELY(stored == NO_LZ))
                return TSDB_ERR_CORRUPT;
            x = (uint32_t)((w << 2) >> (64 - (32 - stored)));
            pos += 2 + 32 - (uint64_t)stored;
            break;
        default:
            stored = lz_round_val[(w >> 59) & 7];
            x = (uint32_t)((w << 5) >> (64 - (32 - stored)));
            pos += 5 + 32 - (uint64_t)stored;
            break;
        }
        prev ^= x;
        out[i++] = prev;
    }
    return pos <= limit ? 0 : TSDB_ERR_CORRUPT;
}

/* --------------------------------------------------------------- Chimp128 */

typedef struct ref_finder {
    int32_t last[1 << KEY_BITS]; /* absolute index + 1 of the last value with this key */
} ref_finder;

/* Picks the reference for in[i]: the last value sharing the low KEY_BITS bits
 * when it is still in the window and gives enough trailing zeros, otherwise
 * the previous value. Returns the absolute index of the reference. */
static inline size_t find_ref(ref_finder *f, const uint32_t *in, size_t i, int thr)
{
    uint32_t key = in[i] & ((1u << KEY_BITS) - 1);
    int32_t j = f->last[key] - 1;
    if (j >= 0 && i - (size_t)j <= RING) {
        uint32_t t = in[i] ^ in[j];
        if (!t || __builtin_ctz(t) > thr)
            return (size_t)j;
    }
    return i - 1;
}

size_t chimp128_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    ref_finder *f = calloc(1, sizeof *f);
    store32(out, in[0]);
    bitwriter w;
    bw_init(&w, out + 4);
    f->last[in[0] & ((1u << KEY_BITS) - 1)] = 1;
    int stored = NO_LZ;
    for (size_t i = 1; i < n; i++) {
        size_t j = find_ref(f, in, i, CHIMP128_THR);
        uint32_t ref = (uint32_t)(j % RING), x = in[i] ^ in[j];
        f->last[in[i] & ((1u << KEY_BITS) - 1)] = (int32_t)i + 1;
        if (!x) {
            bw_put(&w, ref, 9); /* 00 ref:7 */
            stored = NO_LZ;
            continue;
        }
        /* a non-previous reference is only chosen when it gives 00 or 01 */
        int code = lz_code(__builtin_clz(x)), lz = lz_round_val[code], tz = __builtin_ctz(x);
        if (tz > CHIMP128_THR) {
            int len = 32 - lz - tz;
            uint64_t hdr = (1u << 15) | (ref << 8) | ((uint32_t)code << 5) | (uint32_t)len;
            bw_put(&w, (hdr << len) | (x >> tz), 17 + len);
            stored = NO_LZ;
        } else if (lz == stored) {
            bw_put(&w, (2ull << (32 - lz)) | x, 2 + 32 - lz);
        } else {
            bw_put(&w, ((uint64_t)((3u << 3) | code) << (32 - lz)) | x, 5 + 32 - lz);
            stored = lz;
        }
    }
    free(f);
    return 4 + bw_finish(&w);
}

int chimp128_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (n == 0)
        return 0;
    if (len < 4)
        return TSDB_ERR_CORRUPT;
    out[0] = load32(in);
    const uint8_t *b = in + 4;
    const uint64_t limit = (uint64_t)(len - 4) * 8;
    uint64_t pos = 0;
    int stored = NO_LZ;
    for (size_t i = 1; i < n; i++) {
        if (UNLIKELY(pos >= limit))
            return TSDB_ERR_CORRUPT;
        uint64_t w = br_peek(b, pos);
        uint32_t ref, x;
        switch (w >> 62) {
        case 0:
            ref = (uint32_t)((w >> 55) & 127);
            x = 0;
            pos += 9;
            stored = NO_LZ;
            break;
        case 1: {
            ref = (uint32_t)((w >> 55) & 127);
            int lz = lz_round_val[(w >> 52) & 7], l = (int)((w >> 47) & 31);
            if (UNLIKELY(l == 0 || lz + l > 32))
                return TSDB_ERR_CORRUPT;
            x = (uint32_t)(((w << 17) >> (64 - l)) << (32 - lz - l));
            pos += 17 + (uint64_t)l;
            stored = NO_LZ;
            break;
        }
        case 2:
            if (UNLIKELY(stored == NO_LZ))
                return TSDB_ERR_CORRUPT;
            ref = (uint32_t)((i - 1) % RING);
            x = (uint32_t)((w << 2) >> (64 - (32 - stored)));
            pos += 2 + 32 - (uint64_t)stored;
            break;
        default:
            ref = (uint32_t)((i - 1) % RING);
            stored = lz_round_val[(w >> 59) & 7];
            x = (uint32_t)((w << 5) >> (64 - (32 - stored)));
            pos += 5 + 32 - (uint64_t)stored;
            break;
        }
        /* ring slot -> absolute index within the last RING values */
        size_t j = i - 1 - ((i - 1 - ref) % RING);
        if (UNLIKELY(j >= i))
            return TSDB_ERR_CORRUPT;
        out[i] = out[j] ^ x;
    }
    return pos <= limit ? 0 : TSDB_ERR_CORRUPT;
}

/* ------------------------------------------------------------------ Patas */

size_t patas_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    ref_finder *f = calloc(1, sizeof *f);
    store32(out, in[0]);
    f->last[in[0] & ((1u << KEY_BITS) - 1)] = 1;
    uint8_t *hdr = out + 4, *bytes = out + 4 + 2 * (n - 1);
    for (size_t i = 1; i < n; i++) {
        /* take whichever reference needs fewer significant bytes */
        size_t j = i - 1;
        uint32_t key = in[i] & ((1u << KEY_BITS) - 1);
        int32_t c = f->last[key] - 1;
        uint32_t x = in[i] ^ in[j];
        if (c >= 0 && i - (size_t)c <= RING) {
            uint32_t t = in[i] ^ in[c];
            int bt = t ? 32 - __builtin_clz(t) - __builtin_ctz(t) : 0;
            int bx = x ? 32 - __builtin_clz(x) - __builtin_ctz(x) : 0;
            if ((bt + 7) / 8 < (bx + 7) / 8 || !t) {
                j = (size_t)c;
                x = t;
            }
        }
        f->last[key] = (int32_t)i + 1;
        int tz = x ? __builtin_ctz(x) : 0;
        int nb = x ? (32 - __builtin_clz(x) - tz + 7) / 8 : 0;
        uint16_t h = (uint16_t)(((j % RING) << 9) | ((uint32_t)nb << 6) | (uint32_t)tz);
        memcpy(hdr + 2 * (i - 1), &h, 2);
        uint32_t v = x >> tz;
        memcpy(bytes, &v, 4); /* bound leaves room for the overwrite */
        bytes += nb;
    }
    free(f);
    return (size_t)(bytes - out);
}

int patas_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (n == 0)
        return 0;
    if (len < 4 + 2 * (n - 1))
        return TSDB_ERR_CORRUPT;
    out[0] = load32(in);
    const uint8_t *hdr = in + 4, *p = in + 4 + 2 * (n - 1), *end = in + len;
    for (size_t i = 1; i < n; i++) {
        uint16_t h;
        memcpy(&h, hdr + 2 * (i - 1), 2);
        uint32_t ref = h >> 9, nb = (h >> 6) & 7, tz = h & 63;
        if (UNLIKELY(nb > 4 || tz > 31 || p + nb > end))
            return TSDB_ERR_CORRUPT;
        uint32_t v = load32(p) & (uint32_t)((1ull << (8 * nb)) - 1);
        p += nb;
        size_t j = i - 1 - ((i - 1 - ref) % RING);
        if (UNLIKELY(j >= i)) /* a slot before the first value (as in chimp128_decode) */
            return TSDB_ERR_CORRUPT;
        out[i] = out[j] ^ (v << tz);
    }
    return p == end ? 0 : TSDB_ERR_CORRUPT;
}
