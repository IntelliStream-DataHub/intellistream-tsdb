/*
 * DoubleDelta (int64) and Gorilla (float32) codecs, bit compatible in spirit
 * with ClickHouse's CompressionCodecDoubleDelta / CompressionCodecGorilla
 * (minus the leading item count, which lives in the granule index), plus the
 * block wrapper that layers ZSTD on top.
 *
 * Bit streams are MSB first. Decoders read 64-bit big-endian words and may
 * read up to BLK_SLACK bytes beyond the end of their input.
 */
#include "internal.h"
#include "bitpack.h"
#include "bits.h"

#include <math.h>
#include <stdlib.h>

/* ---------------------------------------------------------------- xxh64 */

#define P1 11400714785074694791ULL
#define P2 14029467366897019727ULL
#define P3 1609587929392839161ULL
#define P4 9650029242287828579ULL
#define P5 2870177450012600261ULL

static inline uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static inline uint64_t xxh_round(uint64_t acc, uint64_t in) { return rotl64(acc + in * P2, 31) * P1; }
static inline uint64_t xxh_merge(uint64_t acc, uint64_t v) { return (acc ^ xxh_round(0, v)) * P1 + P4; }

uint64_t xxh64(const void *data, size_t len, uint64_t seed)
{
    const uint8_t *p = data, *end = p + len;
    uint64_t h;
    if (len >= 32) {
        const uint8_t *limit = end - 32;
        uint64_t v1 = seed + P1 + P2, v2 = seed + P2, v3 = seed, v4 = seed - P1;
        do {
            v1 = xxh_round(v1, rd64(p));
            v2 = xxh_round(v2, rd64(p + 8));
            v3 = xxh_round(v3, rd64(p + 16));
            v4 = xxh_round(v4, rd64(p + 24));
            p += 32;
        } while (p <= limit);
        h = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
        h = xxh_merge(h, v1);
        h = xxh_merge(h, v2);
        h = xxh_merge(h, v3);
        h = xxh_merge(h, v4);
    } else {
        h = seed + P5;
    }
    h += len;
    for (; p + 8 <= end; p += 8)
        h = rotl64(h ^ xxh_round(0, rd64(p)), 27) * P1 + P4;
    if (p + 4 <= end) {
        h = rotl64(h ^ ((uint64_t)load32(p) * P1), 23) * P2 + P3;
        p += 4;
    }
    for (; p < end; p++)
        h = rotl64(h ^ (*p * P5), 11) * P1;
    h ^= h >> 33;
    h *= P2;
    h ^= h >> 29;
    h *= P3;
    h ^= h >> 32;
    return h;
}

/* ----------------------------------------------------------- DoubleDelta */

size_t dd_bound(size_t n) { return 16 + (n * 69 + 7) / 8 + 8; }

size_t dd_encode(const int64_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    memcpy(out, &in[0], 8);
    if (n == 1)
        return 8;
    uint64_t prev = (uint64_t)in[1];
    uint64_t prev_delta = (uint64_t)in[1] - (uint64_t)in[0];
    memcpy(out + 8, &prev_delta, 8);

    bitwriter w;
    bw_init(&w, out + 16);
    size_t zeros = 0;
    for (size_t i = 2; i < n; i++) {
        uint64_t cur = (uint64_t)in[i];
        uint64_t delta = cur - prev;
        int64_t dd = (int64_t)(delta - prev_delta);
        prev_delta = delta;
        prev = cur;
        if (dd == 0) {
            zeros++;
            continue;
        }
        if (zeros) {
            bw_zeros(&w, zeros);
            zeros = 0;
        }
        /* sign bit + (|dd| - 1); ~dd == -dd - 1 avoids overflow at INT64_MIN */
        uint64_t s = dd < 0;
        uint64_t m = dd > 0 ? (uint64_t)dd - 1 : ~(uint64_t)dd;
        if (m < (1u << 6))
            bw_put(&w, (0x2ull << 7) | (s << 6) | m, 9);
        else if (m < (1u << 8))
            bw_put(&w, (0x6ull << 9) | (s << 8) | m, 12);
        else if (m < (1u << 11))
            bw_put(&w, (0xeull << 12) | (s << 11) | m, 16);
        else if (m < (1ull << 31))
            bw_put(&w, (0x1eull << 32) | (s << 31) | m, 37);
        else {
            bw_put(&w, (0x1full << 1) | s, 6);
            bw_put(&w, m, 63);
        }
    }
    bw_zeros(&w, zeros);
    return 16 + bw_finish(&w);
}

int dd_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    if (n == 0)
        return 0;
    if (len < 8)
        return TSDB_ERR_CORRUPT;
    uint64_t prev, delta;
    memcpy(&prev, in, 8);
    out[0] = (int64_t)prev;
    if (n == 1)
        return 0;
    if (len < 16)
        return TSDB_ERR_CORRUPT;
    memcpy(&delta, in + 8, 8);
    prev += delta;
    out[1] = (int64_t)prev;

    const uint8_t *b = in + 16;
    const uint64_t limit = (uint64_t)(len - 16) * 8;
    uint64_t pos = 0;
    size_t i = 2;
    while (i < n) {
        if (UNLIKELY(pos >= limit))
            return TSDB_ERR_CORRUPT;
        uint64_t w = br_peek(b, pos);
        if (!(w >> 63)) {
            /* a run of zero bits is a run of unchanged deltas: emit it at once */
            size_t z = w ? (size_t)__builtin_clzll(w) : 64;
            if (z > 57)
                z = 57;
            size_t r = MIN(z, n - i);
            for (size_t j = 0; j < r; j++)
                out[i + j] = (int64_t)(prev + (j + 1) * delta);
            prev += r * delta;
            i += r;
            pos += r;
            continue;
        }
        int ones = ~w ? __builtin_clzll(~w) : 64;
        uint64_t s, m;
        switch (ones) {
        case 1: /* 10 s m6 */
            s = (w >> 61) & 1;
            m = (w >> 55) & 0x3f;
            pos += 9;
            break;
        case 2: /* 110 s m8 */
            s = (w >> 60) & 1;
            m = (w >> 52) & 0xff;
            pos += 12;
            break;
        case 3: /* 1110 s m11 */
            s = (w >> 59) & 1;
            m = (w >> 48) & 0x7ff;
            pos += 16;
            break;
        case 4: /* 11110 s m31 */
            s = (w >> 58) & 1;
            m = (w >> 27) & 0x7fffffff;
            pos += 37;
            break;
        default: /* 11111 s m63 */
            s = (w >> 58) & 1;
            pos += 6;
            m = (br_peek(b, pos) >> 32) << 31;
            pos += 32;
            m |= br_peek(b, pos) >> 33;
            pos += 31;
            break;
        }
        delta += s ? ~m : m + 1;
        prev += delta;
        out[i++] = (int64_t)prev;
    }
    return pos <= limit ? 0 : TSDB_ERR_CORRUPT;
}

/* --------------------------------------------------------------- Gorilla */

size_t gorilla_bound(size_t n) { return 4 + (n * 45 + 7) / 8 + 8; }

size_t gorilla_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    store32(out, in[0]);
    bitwriter w;
    bw_init(&w, out + 4);
    uint32_t prev = in[0];
    int plz = 0, ptz = 0, pbits = 0;
    size_t zeros = 0;
    for (size_t i = 1; i < n; i++) {
        uint32_t x = in[i] ^ prev;
        prev = in[i];
        if (!x) {
            zeros++;
            continue;
        }
        if (zeros) {
            bw_zeros(&w, zeros);
            zeros = 0;
        }
        int lz = __builtin_clz(x), tz = __builtin_ctz(x);
        if (pbits && lz >= plz && tz >= ptz) {
            bw_put(&w, (0x2ull << pbits) | (x >> ptz), 2 + pbits);
        } else {
            int bits = 32 - lz - tz;
            uint64_t hdr = (0x3ull << 11) | ((uint64_t)lz << 6) | (uint64_t)bits;
            bw_put(&w, (hdr << bits) | (x >> tz), 13 + bits);
            plz = lz;
            ptz = tz;
            pbits = bits;
        }
    }
    bw_zeros(&w, zeros);
    return 4 + bw_finish(&w);
}

int gorilla_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
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
    int tz = 0, bits = 0;
    size_t i = 1;
    while (i < n) {
        if (UNLIKELY(pos >= limit))
            return TSDB_ERR_CORRUPT;
        uint64_t w = br_peek(b, pos);
        if (!(w >> 63)) {
            size_t z = w ? (size_t)__builtin_clzll(w) : 64;
            if (z > 57)
                z = 57;
            size_t r = MIN(z, n - i);
            for (size_t j = 0; j < r; j++)
                out[i + j] = prev;
            i += r;
            pos += r;
            continue;
        }
        uint64_t x;
        if (!((w >> 62) & 1)) { /* 10: reuse the previous window */
            if (UNLIKELY(!bits))
                return TSDB_ERR_CORRUPT;
            x = (w << 2) >> (64 - bits);
            pos += 2 + (uint64_t)bits;
        } else { /* 11: lz5 bits6 data */
            int lz = (int)((w >> 57) & 0x1f);
            bits = (int)((w >> 51) & 0x3f);
            if (UNLIKELY(bits == 0 || lz + bits > 32))
                return TSDB_ERR_CORRUPT;
            tz = 32 - lz - bits;
            x = (w << 13) >> (64 - bits);
            pos += 13 + (uint64_t)bits;
        }
        prev ^= (uint32_t)(x << tz);
        out[i++] = prev;
    }
    return pos <= limit ? 0 : TSDB_ERR_CORRUPT;
}

/* --------------------------------------------------------------- Decimal */
/*
 * For floats that are really fixed-point numbers: sensor readings like 23.4
 * (decimal) or ADC counts times a power of two (binary). Find a scale s such
 * that the values are exactly (float)(k * s) for integers k, and store k as
 * zigzag deltas of 1, 2 or 4 bytes, left to ZSTD's entropy coder.
 *
 * Lossless by construction: every value is checked to round-trip bit for bit;
 * the few that do not (NaN, -0.0, an odd reading) are stored as exceptions,
 * position and raw bits, and patched in after decoding.
 *
 * The scale is chosen on samples first, then verified on all values. Both
 * ideas, exceptions and sampling, come from ALP (Afroozeh, Kuffo, Boncz;
 * SIGMOD 2024).
 *
 *   u8 scale | u8 width | i64 first | u32 nexc | (n - 1) deltas | nexc x (u32 pos, u32 bits)
 */

#define DEC_NDEC 11 /* 10^0 .. 10^10 */
#define DEC_NBIN 24 /* 2^1 .. 2^24 */
#define DEC_MAX_EXP (DEC_NDEC + DEC_NBIN - 1)
#define DEC_LIMIT 4503599627370496.0 /* 2^52: integers are exact in a double */
#define DEC_HDR 14

static double dec_scale[DEC_MAX_EXP + 1], dec_inv[DEC_MAX_EXP + 1];

__attribute__((constructor)) static void dec_tables(void)
{
    static const double inv10[DEC_NDEC] = {1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10};
    double p = 1;
    for (int e = 0; e < DEC_NDEC; e++, p *= 10) {
        dec_scale[e] = p;
        dec_inv[e] = inv10[e]; /* literals: the decoder multiplies by exactly these */
    }
    for (int b = 1; b <= DEC_NBIN; b++) {
        dec_scale[DEC_NDEC + b - 1] = ldexp(1.0, b);
        dec_inv[DEC_NDEC + b - 1] = ldexp(1.0, -b);
    }
}

static inline float dec_value(int64_t k, int e) { return (float)((double)k * dec_inv[e]); }
static inline uint64_t zigzag(int64_t x) { return ((uint64_t)x << 1) ^ (uint64_t)(x >> 63); }
static inline int64_t unzigzag(uint64_t z) { return (int64_t)(z >> 1) ^ -(int64_t)(z & 1); }

size_t decimal_bound(size_t n) { return DEC_HDR + n * 4 + (n / 16 + 1) * 8; }

/* Integer for v at scale e, or false when v is not exactly representable. */
static inline bool dec_int(float v, int e, int64_t *k)
{
    double d = (double)v * dec_scale[e];
    if (!(d > -DEC_LIMIT && d < DEC_LIMIT))
        return false;
    *k = (int64_t)__builtin_llrint(d);
    float back = dec_value(*k, e);
    uint32_t a, b;
    memcpy(&a, &back, 4);
    memcpy(&b, &v, 4);
    return a == b; /* bitwise: -0.0 and NaN become exceptions */
}

static int dec_width(uint64_t maxz) { return maxz < (1u << 8) ? 1 : maxz < (1u << 16) ? 2 : maxz < (1ull << 32) ? 4 : 8; }

/* Cost in bytes of scale e on a few runs of consecutive values. */
static double dec_sample_cost(const float *in, size_t n, int e)
{
    enum { RUNS = 4, RUN = 16 };
    size_t run = MIN((size_t)RUN, n), tried = 0, exc = 0;
    uint64_t maxz = 0;
    for (int r = 0; r < RUNS; r++) {
        if (r == 1 && exc * 4 > tried)
            return INFINITY; /* failing already: skip the other runs */
        size_t at = n > run ? (n - run) * (size_t)r / (RUNS - 1) : 0;
        int64_t prev = 0, k;
        bool have = false;
        for (size_t i = at; i < at + run; i++, tried++) {
            if (!dec_int(in[i], e, &k)) {
                exc++;
                continue;
            }
            if (have)
                maxz |= zigzag(k - prev);
            prev = k;
            have = true;
        }
        if (n <= run)
            break;
    }
    if (exc * 4 > tried)
        return INFINITY;
    return (double)dec_width(maxz) + 8.0 * (double)exc / (double)tried;
}

/* The integers of in at the best scale, as zigzag deltas z[0..n-1) (z[i] is
 * value i+1 minus value i), plus exceptions (u32 position, u32 raw bits each)
 * in exc. Returns false when the values are not fixed point. */
typedef struct dec_res {
    int e;
    int64_t first;
    uint64_t maxz;
    size_t nexc;
} dec_res;

static bool dec_core(const float *in, size_t n, uint32_t *z, uint8_t *exc, dec_res *r)
{
    int best_e = -1;
    double best = INFINITY;
    for (int e = 0; e <= DEC_MAX_EXP && best > 1.0; e++) { /* 1.0: 1-byte deltas, no exceptions */
        double cost = dec_sample_cost(in, n, e);
        if (cost < best - 1e-9) {
            best = cost;
            best_e = e;
        }
    }
    if (best_e < 0 || best >= 4.5)
        return false;
    /* two loops: a branch-free one the compiler vectorizes (integer and
     * exactness of every value), then deltas and exceptions */
    int64_t *kk = malloc(n * sizeof *kk); /* small enough for malloc's fast path */
    uint8_t *okv = malloc(n);
    if (!kk || !okv) {
        free(kk);
        free(okv);
        return false;
    }
    const double sc = dec_scale[best_e], inv = dec_inv[best_e];
    for (size_t i = 0; i < n; i++) {
        double d = (double)in[i] * sc;
        bool in_range = (d > -DEC_LIMIT) & (d < DEC_LIMIT); /* false for NaN too */
        int64_t k = (int64_t)__builtin_rint(in_range ? d : 0.0);
        /* rebuild from the integer, as the decoder will: -0.0 must not pass as 0 */
        float back = (float)((double)k * inv);
        uint32_t x, y;
        memcpy(&x, &back, 4);
        memcpy(&y, &in[i], 4);
        okv[i] = (x == y) & in_range;
        kk[i] = k;
    }
    size_t max_exc = n / 16, nexc = 0;
    int64_t prev = 0, first = 0;
    uint64_t maxz = 0;
    bool ok = true;
    for (size_t i = 0; i < n; i++) {
        int64_t k = kk[i];
        if (UNLIKELY(!okv[i])) {
            if (nexc == max_exc) {
                ok = false;
                break;
            }
            store32(exc + 8 * nexc, (uint32_t)i);
            memcpy(exc + 8 * nexc + 4, &in[i], 4);
            nexc++;
            k = i ? prev : 0; /* a zero delta keeps the stream smooth */
        }
        if (i == 0) {
            first = k;
        } else {
            uint64_t zz = zigzag(k - prev);
            maxz |= zz;
            z[i - 1] = (uint32_t)zz;
        }
        prev = k;
    }
    free(kk);
    free(okv);
    if (!ok || maxz >= (1ull << 32))
        return false;
    *r = (dec_res){best_e, first, maxz, nexc};
    return true;
}

size_t decimal_encode(const float *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    uint32_t *z = malloc(n * sizeof *z);
    uint8_t *exc = malloc((n / 16 + 1) * 8);
    dec_res r;
    if (!z || !exc || !dec_core(in, n, z, exc, &r)) {
        free(z);
        free(exc);
        return 0;
    }
    int width = dec_width(r.maxz);
    out[0] = (uint8_t)r.e;
    out[1] = (uint8_t)width;
    memcpy(out + 2, &r.first, 8);
    store32(out + 10, (uint32_t)r.nexc);
    uint8_t *o = out + DEC_HDR;
    for (size_t i = 0; i + 1 < n; i++, o += width) {
        if (width == 1)
            *o = (uint8_t)z[i];
        else if (width == 2) {
            uint16_t z16 = (uint16_t)z[i];
            memcpy(o, &z16, 2);
        } else
            store32(o, z[i]);
    }
    memcpy(o, exc, r.nexc * 8);
    o += r.nexc * 8;
    free(z);
    free(exc);
    return (size_t)(o - out);
}

/*
 * The same integers with their zigzag deltas bit-packed per vector of 1024,
 * ALP style, instead of whole bytes left to ZSTD: decoding is unpacking plus
 * a running sum, with no entropy decoder.
 *
 *   u8 scale | u8 0 | i64 first | u32 nexc | per vector: u8 bits, packed deltas | exceptions
 */
#define DBP_VEC 1024

size_t decimal_bp_bound(size_t n) { return DEC_HDR + n * 4 + (n / DBP_VEC + 1) * 5 + (n / 16 + 1) * 8 + 16; }

size_t decimal_bp_encode(const float *in, size_t n, uint8_t *out)
{
    if (n < 2)
        return 0;
    uint32_t *z = malloc(n * sizeof *z);
    uint8_t *exc = malloc((n / 16 + 1) * 8);
    dec_res r;
    if (!z || !exc || !dec_core(in, n, z, exc, &r)) {
        free(z);
        free(exc);
        return 0;
    }
    out[0] = (uint8_t)r.e;
    out[1] = 0;
    memcpy(out + 2, &r.first, 8);
    store32(out + 10, (uint32_t)r.nexc);
    uint8_t *o = out + DEC_HDR;
    for (size_t v0 = 0; v0 + 1 < n; v0 += DBP_VEC) {
        size_t nv = MIN((size_t)DBP_VEC, n - 1 - v0);
        uint32_t all = 0;
        for (size_t i = 0; i < nv; i++)
            all |= z[v0 + i];
        int w = bits_for(all);
        *o++ = (uint8_t)w;
        pack(z + v0, nv, w, o);
        o += packed_bytes(nv, w);
    }
    memcpy(o, exc, r.nexc * 8);
    o += r.nexc * 8;
    free(z);
    free(exc);
    return (size_t)(o - out);
}

int decimal_bp_decode(const uint8_t *in, size_t len, size_t n, float *out)
{
    if (n == 0)
        return 0;
    if (len < DEC_HDR || n < 2)
        return TSDB_ERR_CORRUPT;
    int e = in[0];
    uint32_t nexc = load32(in + 10);
    if (e > DEC_MAX_EXP || nexc > n)
        return TSDB_ERR_CORRUPT;
    int64_t k;
    memcpy(&k, in + 2, 8);
    const double inv = dec_inv[e];
    const uint8_t *p = in + DEC_HDR, *end = in + len;
    uint32_t z[DBP_VEC];
    out[0] = (float)((double)k * inv);
    for (size_t v0 = 0; v0 + 1 < n; v0 += DBP_VEC) {
        size_t nv = MIN((size_t)DBP_VEC, n - 1 - v0);
        if (p >= end || *p > 32)
            return TSDB_ERR_CORRUPT;
        int w = *p++;
        size_t pb = packed_bytes(nv, w);
        if (p + pb > end)
            return TSDB_ERR_CORRUPT;
        unpack(p, nv, w, z);
        p += pb;
        float *o = out + v0 + 1;
        for (size_t i = 0; i < nv; i++) {
            k += unzigzag(z[i]);
            o[i] = (float)((double)k * inv);
        }
    }
    if (p + (size_t)nexc * 8 != end)
        return TSDB_ERR_CORRUPT;
    for (uint32_t j = 0; j < nexc; j++, p += 8) {
        uint32_t pos = load32(p);
        if (pos >= n)
            return TSDB_ERR_CORRUPT;
        memcpy(&out[pos], p + 4, 4);
    }
    return 0;
}

int decimal_decode(const uint8_t *in, size_t len, size_t n, float *out)
{
    if (n == 0)
        return 0;
    if (len < DEC_HDR)
        return TSDB_ERR_CORRUPT;
    int e = in[0], width = in[1];
    uint32_t nexc = load32(in + 10);
    if (e > DEC_MAX_EXP || (width != 1 && width != 2 && width != 4) || nexc > n ||
        len != DEC_HDR + (n - 1) * (size_t)width + (size_t)nexc * 8)
        return TSDB_ERR_CORRUPT;
    int64_t k;
    memcpy(&k, in + 2, 8);
    const double inv = dec_inv[e];
    const uint8_t *p = in + DEC_HDR;
    out[0] = (float)((double)k * inv);
    if (width == 1) {
        for (size_t i = 1; i < n; i++) {
            k += unzigzag(p[i - 1]);
            out[i] = (float)((double)k * inv);
        }
    } else if (width == 2) {
        for (size_t i = 1; i < n; i++) {
            uint16_t z;
            memcpy(&z, p + 2 * (i - 1), 2);
            k += unzigzag(z);
            out[i] = (float)((double)k * inv);
        }
    } else {
        for (size_t i = 1; i < n; i++) {
            k += unzigzag(load32(p + 4 * (i - 1)));
            out[i] = (float)((double)k * inv);
        }
    }
    const uint8_t *x = p + (n - 1) * (size_t)width;
    for (uint32_t j = 0; j < nexc; j++, x += 8) {
        uint32_t pos = load32(x);
        if (pos >= n)
            return TSDB_ERR_CORRUPT;
        memcpy(&out[pos], x + 4, 4);
    }
    return 0;
}

/* ------------------------------------------------- bit delta, byte split */
/* Delta of the raw float bits (ClickHouse's Delta codec on Float32) and
 * byte stream split (Parquet's BYTE_STREAM_SPLIT): both only reshape the
 * data so ZSTD finds more redundancy. */

size_t delta32_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    uint32_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        store32(out + 4 * i, in[i] - prev);
        prev = in[i];
    }
    return 4 * n;
}

int delta32_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (len != 4 * n)
        return TSDB_ERR_CORRUPT;
    uint32_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        prev += load32(in + 4 * i);
        out[i] = prev;
    }
    return 0;
}

size_t bytesplit_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    for (size_t i = 0; i < n; i++) {
        uint32_t x = in[i];
        out[i] = (uint8_t)x;
        out[n + i] = (uint8_t)(x >> 8);
        out[2 * n + i] = (uint8_t)(x >> 16);
        out[3 * n + i] = (uint8_t)(x >> 24);
    }
    return 4 * n;
}

int bytesplit_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (len != 4 * n)
        return TSDB_ERR_CORRUPT;
    for (size_t i = 0; i < n; i++)
        out[i] = (uint32_t)in[i] | (uint32_t)in[n + i] << 8 | (uint32_t)in[2 * n + i] << 16 |
                 (uint32_t)in[3 * n + i] << 24;
    return 0;
}

/* ------------------------------------------------ timestamps with jitter */
/*
 * DoubleDelta is ideal for a regular clock, but i.i.d. jitter doubles in
 * variance under each difference. Two frame-of-reference alternatives:
 *
 * grid:  r[i] = ts[i] - (ts[0] + i * step), step = (ts[n-1] - ts[0]) / (n-1);
 *        the jitter itself.          u8 width | i64 first | i64 step | i64 base | n x width
 * delta: d[i] = ts[i] - ts[i-1];     u8 width | i64 first | i64 base | (n-1) x width
 *
 * Both store (x - base) in 1, 2, 4 or 8 bytes; ZSTD entropy-codes the bytes.
 */

size_t tsfor_bound(size_t n) { return 25 + 8 * n; }

static int for_width(uint64_t range) { return range < (1u << 8) ? 1 : range < (1u << 16) ? 2 : range < (1ull << 32) ? 4 : 8; }

static void for_put(uint8_t *o, uint64_t x, int width)
{
    if (width == 1)
        *o = (uint8_t)x;
    else if (width == 2) {
        uint16_t y = (uint16_t)x;
        memcpy(o, &y, 2);
    } else if (width == 4)
        store32(o, (uint32_t)x);
    else
        memcpy(o, &x, 8);
}

static inline uint64_t for_get(const uint8_t *p, int width)
{
    if (width == 1)
        return *p;
    if (width == 2) {
        uint16_t y;
        memcpy(&y, p, 2);
        return y;
    }
    if (width == 4)
        return load32(p);
    uint64_t y;
    memcpy(&y, p, 8);
    return y;
}

static inline uint64_t grid_at(uint64_t first, uint64_t step, size_t i) { return first + (uint64_t)i * step; }

size_t ts_grid_encode(const int64_t *ts, size_t n, uint8_t *out)
{
    if (n < 2)
        return 0;
    uint64_t first = (uint64_t)ts[0];
    /* unsigned difference: no overflow for extreme spans (any step decodes exactly) */
    int64_t span = (int64_t)((uint64_t)ts[n - 1] - (uint64_t)ts[0]);
    uint64_t step = (uint64_t)(span / (int64_t)(n - 1));
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    for (size_t i = 0; i < n; i++) {
        int64_t r = (int64_t)((uint64_t)ts[i] - grid_at(first, step, i));
        lo = r < lo ? r : lo;
        hi = r > hi ? r : hi;
    }
    int width = for_width((uint64_t)hi - (uint64_t)lo);
    out[0] = (uint8_t)width;
    memcpy(out + 1, &first, 8);
    memcpy(out + 9, &step, 8);
    memcpy(out + 17, &lo, 8);
    uint8_t *o = out + 25;
    for (size_t i = 0; i < n; i++, o += width)
        for_put(o, (uint64_t)ts[i] - grid_at(first, step, i) - (uint64_t)lo, width);
    return (size_t)(o - out);
}

int ts_grid_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    if (len < 25)
        return TSDB_ERR_CORRUPT;
    int width = in[0];
    uint64_t first, step, lo;
    memcpy(&first, in + 1, 8);
    memcpy(&step, in + 9, 8);
    memcpy(&lo, in + 17, 8);
    if ((width != 1 && width != 2 && width != 4 && width != 8) || len != 25 + n * (size_t)width)
        return TSDB_ERR_CORRUPT;
    const uint8_t *p = in + 25;
    if (width == 1) {
        for (size_t i = 0; i < n; i++)
            out[i] = (int64_t)(grid_at(first, step, i) + lo + p[i]);
    } else {
        for (size_t i = 0; i < n; i++)
            out[i] = (int64_t)(grid_at(first, step, i) + lo + for_get(p + i * (size_t)width, width));
    }
    return 0;
}

size_t ts_delta_encode(const int64_t *ts, size_t n, uint8_t *out)
{
    if (n < 2)
        return 0;
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    for (size_t i = 1; i < n; i++) {
        int64_t d = (int64_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1]);
        lo = d < lo ? d : lo;
        hi = d > hi ? d : hi;
    }
    int width = for_width((uint64_t)hi - (uint64_t)lo);
    out[0] = (uint8_t)width;
    memcpy(out + 1, &ts[0], 8);
    memcpy(out + 9, &lo, 8);
    uint8_t *o = out + 17;
    for (size_t i = 1; i < n; i++, o += width)
        for_put(o, (uint64_t)ts[i] - (uint64_t)ts[i - 1] - (uint64_t)lo, width);
    return (size_t)(o - out);
}

int ts_delta_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    if (n == 0)
        return 0;
    if (len < 17)
        return TSDB_ERR_CORRUPT;
    int width = in[0];
    uint64_t cur, lo;
    memcpy(&cur, in + 1, 8);
    memcpy(&lo, in + 9, 8);
    if ((width != 1 && width != 2 && width != 4 && width != 8) || len != 17 + (n - 1) * (size_t)width)
        return TSDB_ERR_CORRUPT;
    const uint8_t *p = in + 17;
    out[0] = (int64_t)cur;
    for (size_t i = 1; i < n; i++) {
        cur += lo + for_get(p + (i - 1) * (size_t)width, width);
        out[i] = (int64_t)cur;
    }
    return 0;
}

/* ---------------------------------------------------------------- blocks */
/*
 * Block layout: 1 header byte (transform | BLK_ZSTD), then either the raw
 * transform output, or a u32 transform output length followed by a ZSTD frame.
 */

codec_ctx *codec_new(void)
{
    codec_ctx *c = calloc(1, sizeof *c);
    if (!c)
        return NULL;
    c->cctx = ZSTD_createCCtx();
    c->dctx = ZSTD_createDCtx();
    if (!c->cctx || !c->dctx) {
        codec_free(c);
        return NULL;
    }
    return c;
}

void codec_free(codec_ctx *c)
{
    if (!c)
        return;
    ZSTD_freeCCtx(c->cctx);
    ZSTD_freeDCtx(c->dctx);
    free(c->scratch);
    free(c->alt);
    free(c);
}

int codec_scratch_reserve(codec_ctx *c, size_t need)
{
    if (c->scratch_cap >= need)
        return 0;
    size_t cap = MAX(need, c->scratch_cap * 2);
    uint8_t *s = realloc(c->scratch, cap);
    if (!s)
        return TSDB_ERR_NOMEM;
    c->scratch = s;
    c->scratch_cap = cap;
    return 0;
}

static size_t inner_bound(int kind, size_t n)
{
    switch (kind) {
    case BLK_DD: return dd_bound(n);
    case BLK_GORILLA: return gorilla_bound(n);
    case BLK_DECIMAL: return decimal_bound(n);
    case BLK_CHIMP: return chimp_bound(n);
    case BLK_DELTA32:
    case BLK_BYTESPLIT: return 4 * n;
    case BLK_TS_GRID:
    case BLK_TS_DELTA: return tsfor_bound(n);
    case BLK_ALPRD: return alprd_bound(n);
    case BLK_DECIMAL_BP: return decimal_bp_bound(n);
    case BLK_F64_DECIMAL: return f64_decimal_bound(n);
    case BLK_XOR64: return xor64_bound(n);
    case BLK_BYTESPLIT64: return 8 * n;
    case BLK_INT: return int_bp_bound(n);
    default: return 0;
    }
}

size_t block_bound(size_t n)
{
    size_t inner = MAX(MAX(MAX(dd_bound(n), tsfor_bound(n)), chimp_bound(n)), MAX(gorilla_bound(n), decimal_bound(n)));
    inner = MAX(inner, MAX(alprd_bound(n), decimal_bp_bound(n)));
    inner = MAX(inner, MAX(MAX(f64_decimal_bound(n), xor64_bound(n)), MAX(8 * n, int_bp_bound(n))));
    return 5 + ZSTD_compressBound(inner);
}

/* Wraps `inner` bytes of transform output (in c->scratch) into a block. */
size_t block_wrap(codec_ctx *c, int level, int kind, size_t inner, uint8_t *out)
{
    if (level > 0 && inner >= 64) {
        size_t z = ZSTD_compressCCtx(c->cctx, out + 5, ZSTD_compressBound(inner), c->scratch, inner, level);
        /* keep zstd only when it saves a few percent: decoding costs too */
        if (!ZSTD_isError(z) && z + 4 < inner - inner / 32) {
            out[0] = (uint8_t)(kind | BLK_ZSTD);
            store32(out + 1, (uint32_t)inner);
            return 5 + z;
        }
    }
    out[0] = (uint8_t)kind;
    memcpy(out + 1, c->scratch, inner);
    return 1 + inner;
}

static double entropy_bytes(const uint8_t *p, size_t len);
static double entropy_words(const uint8_t *p, size_t len, size_t width);

static size_t encode_ts_kind(codec_ctx *c, int kind, const int64_t *ts, size_t n)
{
    switch (kind) {
    case BLK_TS_GRID: return ts_grid_encode(ts, n, c->scratch);
    case BLK_TS_DELTA: return ts_delta_encode(ts, n, c->scratch);
    default: return dd_encode(ts, n, c->scratch);
    }
}

/* DoubleDelta when the clock is regular (about a bit per row before ZSTD);
 * otherwise the candidate with the smallest estimated size. */
size_t block_encode_ts(codec_ctx *c, int level, const int64_t *ts, size_t n, uint8_t *out)
{
    if (codec_scratch_reserve(c, MAX(dd_bound(n), tsfor_bound(n))) != 0)
        return 0;
    size_t dd = dd_encode(ts, n, c->scratch);
    if (level <= 0 || n < 2 || dd <= 24 + n / 8 + 8)
        return block_wrap(c, level, BLK_DD, dd, out);
    int best = BLK_DD;
    double best_est = MIN((double)dd, entropy_bytes(c->scratch, dd));
    static const int kinds[] = {BLK_TS_GRID, BLK_TS_DELTA};
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++) {
        size_t len = encode_ts_kind(c, kinds[k], ts, n);
        size_t hdr = kinds[k] == BLK_TS_GRID ? 25 : 17;
        double est = len > hdr ? MIN((double)len, hdr + entropy_words(c->scratch + hdr, len - hdr, c->scratch[0]))
                               : (double)len;
        if (len && est < best_est) {
            best_est = est;
            best = kinds[k];
        }
    }
    return block_wrap(c, level, best, encode_ts_kind(c, best, ts, n), out);
}

/* Encodes v with one transform into c->scratch; returns 0 if not applicable. */
static size_t encode_f32_kind(codec_ctx *c, int kind, const float *v, size_t n)
{
    switch (kind) {
    case BLK_GORILLA: return gorilla_encode((const uint32_t *)v, n, c->scratch);
    case BLK_CHIMP: return chimp_encode((const uint32_t *)v, n, c->scratch);
    case BLK_DECIMAL: return decimal_encode(v, n, c->scratch);
    case BLK_DELTA32: return delta32_encode((const uint32_t *)v, n, c->scratch);
    case BLK_BYTESPLIT: return bytesplit_encode((const uint32_t *)v, n, c->scratch);
    case BLK_ALPRD: return alprd_encode((const uint32_t *)v, n, c->scratch);
    case BLK_DECIMAL_BP: return decimal_bp_encode(v, n, c->scratch);
    default: return 0;
    }
}

/* Order-0 entropy of every `stride`-th byte starting at p, in bytes (scaled
 * up from a 1-in-3 sample), as an estimate of their size after ZSTD's entropy
 * stage. Much cheaper than compressing every candidate. */
static double entropy_lane(const uint8_t *p, size_t len, size_t stride)
{
    uint32_t h[4][256];
    memset(h, 0, sizeof h);
    size_t step = 3 * stride, m = 0, i = 0;
    for (; i + 3 * step < len; i += 4 * step, m += 4) {
        h[0][p[i]]++;
        h[1][p[i + step]]++;
        h[2][p[i + 2 * step]]++;
        h[3][p[i + 3 * step]]++;
    }
    for (; i < len; i += step, m++)
        h[0][p[i]]++;
    if (!m)
        return 0;
    double bits = 0, n = (double)m;
    for (int k = 0; k < 256; k++) {
        uint32_t c = h[0][k] + h[1][k] + h[2][k] + h[3][k];
        if (c)
            bits -= c * __builtin_log2(c / n);
    }
    size_t total = (len + stride - 1) / stride;
    return bits * ((double)total / n) / 8;
}

static double entropy_bytes(const uint8_t *p, size_t len) { return entropy_lane(p, len, 1); }

/* Fixed-width little-endian values: each byte lane has its own statistics. */
static double entropy_words(const uint8_t *p, size_t len, size_t width)
{
    double e = 0;
    for (size_t j = 0; j < width && j < len; j++)
        e += entropy_lane(p + j, len - j, width);
    return e;
}

/*
 * AUTO picks per granule (see tools/codec-bench):
 *  - values that are fixed point (decimal or binary scale, a few exceptions
 *    allowed): bit-packed zigzag deltas of the integers, with ZSTD on top when
 *    it helps. This won on every such sensor stream measured.
 *  - anything else: Chimp, bit delta, byte-stream split or ALP_rd. Cheap
 *    entropy estimates shortlist two, both are compressed, the smaller stays.
 */
size_t block_encode_f32(codec_ctx *c, int level, int value_codec, const float *v, size_t n, uint8_t *out)
{
    size_t need = MAX(MAX(chimp_bound(n), gorilla_bound(n)), MAX(alprd_bound(n), decimal_bp_bound(n)));
    if (codec_scratch_reserve(c, need) != 0)
        return 0;
    if (value_codec == TSDB_VALUES_GORILLA || value_codec == TSDB_VALUES_CHIMP) {
        int kind = value_codec == TSDB_VALUES_GORILLA ? BLK_GORILLA : BLK_CHIMP;
        return block_wrap(c, level, kind, encode_f32_kind(c, kind, v, n), out);
    }
    size_t inner = decimal_bp_encode(v, n, c->scratch);
    if (inner && inner * 8 <= 16 * n) /* compact fixed point: nothing else came close */
        return block_wrap(c, level, BLK_DECIMAL_BP, inner, out);
    /* every float32 is "decimal" at ~8 digits; then it is just one more candidate */
    static const int kinds[] = {BLK_CHIMP, BLK_DELTA32, BLK_BYTESPLIT, BLK_ALPRD, BLK_DECIMAL_BP};
    enum { NK = sizeof kinds / sizeof *kinds };
    double est[NK];
    for (size_t k = 0; k < NK; k++) {
        size_t len = encode_f32_kind(c, kinds[k], v, n);
        est[k] = len ? (double)len : INFINITY;
        if (!len || level <= 0)
            continue;
        if (kinds[k] == BLK_BYTESPLIT) /* planes differ (exponent vs mantissa): estimate each */
            est[k] = entropy_bytes(c->scratch, n) + entropy_bytes(c->scratch + n, n) +
                     entropy_bytes(c->scratch + 2 * n, n) + entropy_bytes(c->scratch + 3 * n, n);
        else
            est[k] = MIN(est[k], entropy_bytes(c->scratch, len));
    }
    /* estimates only shortlist: the three most promising are really compressed */
    size_t order[NK];
    for (size_t k = 0; k < NK; k++)
        order[k] = k;
    for (size_t i = 1; i < NK; i++)
        for (size_t j = i; j > 0 && est[order[j]] < est[order[j - 1]]; j--) {
            size_t t = order[j];
            order[j] = order[j - 1];
            order[j - 1] = t;
        }
    size_t best = block_wrap(c, level, kinds[order[0]], encode_f32_kind(c, kinds[order[0]], v, n), out);
    if (level <= 0)
        return best;
    if (c->alt_cap < block_bound(n)) {
        uint8_t *alt = realloc(c->alt, block_bound(n));
        if (!alt)
            return best;
        c->alt = alt;
        c->alt_cap = block_bound(n);
    }
    for (size_t r = 1; r < 3 && est[order[r]] < INFINITY; r++) {
        int kind = kinds[order[r]];
        size_t len = block_wrap(c, level, kind, encode_f32_kind(c, kind, v, n), c->alt);
        if (len < best) {
            memcpy(out, c->alt, len);
            best = len;
        }
    }
    return best;
}

/* Resolves a block to its transform kind and (decompressed) transform bytes. */
int block_open(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int *kind, const uint8_t **p,
               size_t *plen)
{
    if (len < 1)
        return TSDB_ERR_CORRUPT;
    *kind = blk[0] & 0x0f;
    *p = blk + 1;
    *plen = len - 1;
    if (!(blk[0] & BLK_ZSTD))
        return 0;
    if (*plen < 4)
        return TSDB_ERR_CORRUPT;
    size_t inner = load32(*p);
    if (inner > inner_bound(*kind, n))
        return TSDB_ERR_CORRUPT;
    if (codec_scratch_reserve(c, inner + BLK_SLACK) != 0)
        return TSDB_ERR_NOMEM;
    size_t r = ZSTD_decompressDCtx(c->dctx, c->scratch, inner, *p + 4, *plen - 4);
    if (ZSTD_isError(r) || r != inner)
        return TSDB_ERR_CORRUPT;
    memset(c->scratch + inner, 0, BLK_SLACK);
    *p = c->scratch;
    *plen = inner;
    return 0;
}

int block_decode_ts(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int64_t *out)
{
    int kind, rc;
    const uint8_t *p;
    size_t plen;
    if ((rc = block_open(c, blk, len, n, &kind, &p, &plen)) != 0)
        return rc;
    switch (kind) {
    case BLK_DD: return dd_decode(p, plen, n, out);
    case BLK_TS_GRID: return ts_grid_decode(p, plen, n, out);
    case BLK_TS_DELTA: return ts_delta_decode(p, plen, n, out);
    default: return TSDB_ERR_CORRUPT;
    }
}

int block_decode_f32(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, float *out)
{
    int kind, rc;
    const uint8_t *p;
    size_t plen;
    if ((rc = block_open(c, blk, len, n, &kind, &p, &plen)) != 0)
        return rc;
    switch (kind) {
    case BLK_GORILLA: return gorilla_decode(p, plen, n, (uint32_t *)out);
    case BLK_CHIMP: return chimp_decode(p, plen, n, (uint32_t *)out);
    case BLK_DECIMAL: return decimal_decode(p, plen, n, out);
    case BLK_DELTA32: return delta32_decode(p, plen, n, (uint32_t *)out);
    case BLK_BYTESPLIT: return bytesplit_decode(p, plen, n, (uint32_t *)out);
    case BLK_ALPRD: return alprd_decode(p, plen, n, (uint32_t *)out);
    case BLK_DECIMAL_BP: return decimal_bp_decode(p, plen, n, out);
    default: return TSDB_ERR_CORRUPT;
    }
}
