/*
 * Codecs for the value types beyond float32.
 *
 * int64, bool and state codes (BLK_INT): zigzag deltas bit-packed per vector
 * of 1024, like the integers of BLK_DECIMAL_BP. Counters, codes that rarely
 * change and flags all come down to a few bits per value, constant runs to
 * nothing; ZSTD on top takes what is left of repetitive patterns.
 *
 *   i64 first | per vector: u8 bits (0..32, or 64 for raw zigzags), packed deltas
 *
 * float64:
 *  - BLK_F64_DECIMAL: values that are exact decimals (or binary fractions) at
 *    one scale, as integers in BLK_INT form, with exceptions. A value is
 *    rebuilt from its integer k one of two ways, whichever the block records:
 *    k / 10^e, correctly rounded, matches values parsed from decimal text;
 *    k * 10^-e (the double nearest), matches values computed as count times
 *    resolution, as instruments and converters often do.
 *      u8 scale | 0x80 for the second way | u32 nexc | nexc x (u32 pos, u64 bits) | BLK_INT stream
 *  - BLK_XOR64: Gorilla's XOR encoding of the 64-bit patterns.
 *  - BLK_BYTESPLIT64: the eight byte planes, for ZSTD.
 *  - BLK_DELTA_BP: zigzag deltas of the 64-bit patterns, packed per vector of
 *    1024 at up to 64 bits (values converted to other units, unrounded).
 *  - BLK_ALPRD: ALP_rd at a cut of 48 to 63 bits (codec_alp.c; random-looking
 *    doubles, where only the exponent repeats).
 * Decimal wins outright when it needs at most 16 bits a value; otherwise the
 * candidates are all compressed and the smallest stays.
 */
#include "internal.h"

#include "bitpack.h"

#include <math.h>
#include <stdlib.h>

#define IBP_VEC 1024
_Static_assert(IBP_VEC <= 1024, "zz_prefix32 sums at most 1024 deltas");

static inline uint64_t zz(int64_t x) { return ((uint64_t)x << 1) ^ (uint64_t)(x >> 63); }
static inline int64_t unzz(uint64_t z) { return (int64_t)(z >> 1) ^ -(int64_t)(z & 1); }

/* ------------------------------------------------------------------ BLK_INT */

size_t int_bp_bound(size_t n) { return 8 + (n / IBP_VEC + 1) * (1 + 8 * IBP_VEC) + 8; }

size_t int_bp_encode(const int64_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    memcpy(out, &in[0], 8);
    uint8_t *o = out + 8;
    uint64_t z[IBP_VEC];
    uint32_t z32[IBP_VEC];
    for (size_t v0 = 1; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        uint64_t all = 0;
        for (size_t i = 0; i < nv; i++) {
            /* wrapping difference: zigzag of it decodes back exactly */
            z[i] = zz((int64_t)((uint64_t)in[v0 + i] - (uint64_t)in[v0 + i - 1]));
            all |= z[i];
        }
        if (all >> 32) {
            *o++ = 64;
            memcpy(o, z, nv * 8);
            o += nv * 8;
            continue;
        }
        int w = bits_for((uint32_t)all);
        *o++ = (uint8_t)w;
        for (size_t i = 0; i < nv; i++)
            z32[i] = (uint32_t)z[i];
        pack(z32, nv, w, o);
        o += packed_bytes(nv, w);
    }
    return (size_t)(o - out);
}

/* Decodes n integers; returns the bytes consumed, or 0 if corrupt. */
static size_t int_bp_decode_prefix(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    if (n == 0)
        return 0;
    if (len < 8)
        return 0;
    int64_t k;
    memcpy(&k, in, 8);
    out[0] = k;
    const uint8_t *p = in + 8, *end = in + len;
    uint32_t z[IBP_VEC];
    for (size_t v0 = 1; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        if (p >= end)
            return 0;
        int w = *p++;
        if (w == 64) {
            if ((size_t)(end - p) < nv * 8)
                return 0;
            for (size_t i = 0; i < nv; i++) {
                uint64_t zi;
                memcpy(&zi, p + 8 * i, 8);
                k = (int64_t)((uint64_t)k + (uint64_t)unzz(zi));
                out[v0 + i] = k;
            }
            p += nv * 8;
            continue;
        }
        size_t pb = packed_bytes(nv, w);
        if (w > 32 || (size_t)(end - p) < pb)
            return 0;
        unpack(p, nv, w, z);
        p += pb;
        if (w <= ZZ_PREFIX32_MAX_W) { /* the sums relative to k fit an int32: no add chain (bitpack.h) */
            uint32_t total = zz_prefix32(z, nv);
            for (size_t i = 0; i < nv; i++)
                out[v0 + i] = (int64_t)((uint64_t)k + (uint64_t)(int64_t)(int32_t)z[i]);
            k = (int64_t)((uint64_t)k + (uint64_t)(int64_t)(int32_t)total);
            continue;
        }
        for (size_t i = 0; i < nv; i++) {
            k = (int64_t)((uint64_t)k + (uint64_t)unzz(z[i]));
            out[v0 + i] = k;
        }
    }
    return (size_t)(p - in);
}

int int_bp_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    if (n == 0)
        return 0;
    size_t used = int_bp_decode_prefix(in, len, n, out);
    return used && used == len ? 0 : TSDB_ERR_CORRUPT;
}

/* ---------------------------------------------------------- BLK_F64_DECIMAL */

#define F64_NDEC 16 /* 10^0 .. 10^15 */
#define F64_NBIN 30 /* 2^1 .. 2^30 */
#define F64_MAX_EXP (F64_NDEC + F64_NBIN - 1)
#define F64_LIMIT 4503599627370496.0 /* 2^52 */

#define F64_MUL 0x80 /* scale byte: rebuild as k * 10^-e rather than k / 10^e */

/* Constants rather than a constructor's tables: see dec_scale in codec.c (levels). */
#define P2S(b) ((double)(1L << (b)))
#define P2I(b) (1.0 / (double)(1L << (b)))
/* powers of ten up to 10^22 are exact doubles */
static const double f64_scale[] = {1e0, 1e1, 1e2,  1e3,  1e4,  1e5,  1e6,  1e7,
                                   1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
                                   P2S(1), P2S(2), P2S(3), P2S(4), P2S(5), P2S(6), P2S(7), P2S(8), P2S(9), P2S(10),
                                   P2S(11), P2S(12), P2S(13), P2S(14), P2S(15), P2S(16), P2S(17), P2S(18), P2S(19), P2S(20),
                                   P2S(21), P2S(22), P2S(23), P2S(24), P2S(25), P2S(26), P2S(27), P2S(28), P2S(29), P2S(30)};
/* decimal inverses are literals: the nearest doubles, as a program computing k * 0.1 has */
static const double f64_inv[] = {1e0,  1e-1, 1e-2,  1e-3,  1e-4,  1e-5,  1e-6,  1e-7,
                                 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15,
                                 P2I(1), P2I(2), P2I(3), P2I(4), P2I(5), P2I(6), P2I(7), P2I(8), P2I(9), P2I(10),
                                 P2I(11), P2I(12), P2I(13), P2I(14), P2I(15), P2I(16), P2I(17), P2I(18), P2I(19), P2I(20),
                                 P2I(21), P2I(22), P2I(23), P2I(24), P2I(25), P2I(26), P2I(27), P2I(28), P2I(29), P2I(30)};
_Static_assert(sizeof f64_scale / sizeof *f64_scale == F64_MAX_EXP + 1, "f64_scale");
_Static_assert(sizeof f64_inv / sizeof *f64_inv == F64_MAX_EXP + 1, "f64_inv");

static inline double f64_back(int64_t k, int e, bool mul)
{
    return mul ? (double)k * f64_inv[e] : (double)k / f64_scale[e];
}

/* the integer for v at scale e, and whether it rebuilds v bit for bit */
static inline bool f64_int(double v, int e, bool mul, int64_t *k)
{
    double d = v * f64_scale[e];
    if (!(d > -F64_LIMIT && d < F64_LIMIT))
        return false;
    *k = (int64_t)__builtin_llrint(d);
    double back = f64_back(*k, e, mul);
    uint64_t a, b;
    memcpy(&a, &back, 8);
    memcpy(&b, &v, 8);
    return a == b; /* bitwise: -0.0 and NaN (nulls too) become exceptions */
}

size_t f64_decimal_bound(size_t n) { return 5 + (n / 16 + 1) * 12 + int_bp_bound(n); }

/* Cost in bits a value of scale e (and way of rebuilding), on a few runs of
 * consecutive values: the width of their deltas plus 96 per exception. */
static double f64_sample_cost(const double *in, size_t n, int e, bool mul)
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
            if (!f64_int(in[i], e, mul, &k)) {
                exc++;
                continue;
            }
            if (have)
                maxz |= zz((int64_t)((uint64_t)k - (uint64_t)prev));
            prev = k;
            have = true;
        }
        if (n <= run)
            break;
    }
    if (exc * 8 > tried)
        return INFINITY;
    int width = maxz >> 32 ? 64 : bits_for((uint32_t)maxz);
    return (double)width + 96.0 * (double)exc / (double)tried;
}

/* The cheapest scale and way of rebuilding; -1 if none is close to fixed point. */
static int f64_pick_scale(const double *in, size_t n, bool *mul)
{
    int best_e = -1;
    double best = INFINITY;
    for (int e = 0; e <= F64_MAX_EXP && best > 0; e++)
        for (int m = 0; m < 2; m++) {
            if (e >= F64_NDEC && m) /* binary scales rebuild exactly either way */
                continue;
            double cost = f64_sample_cost(in, n, e, m);
            if (cost < best - 1e-9) {
                best = cost;
                best_e = e;
                *mul = m;
            }
        }
    if (best >= 48)
        return -1; /* beyond that, XOR or byte split do better */
    if (best_e < F64_NDEC) {
        /* the two ways of rebuilding often both pass the runs and part on a few
         * percent of values: settle it on a wider sample */
        enum { WIDE = 512 };
        size_t step = n > WIDE ? n / WIDE : 1, exc[2] = {0, 0};
        int64_t k;
        for (size_t i = 0; i < n; i += step)
            for (int m = 0; m < 2; m++)
                exc[m] += !f64_int(in[i], best_e, m, &k);
        *mul = exc[1] < exc[0];
    }
    return best_e;
}

size_t f64_decimal_encode(const double *in, size_t n, uint8_t *out)
{
    if (n < 2)
        return 0;
    bool mul = false;
    int e = f64_pick_scale(in, n, &mul);
    if (e < 0)
        return 0;
    int64_t *k = malloc(n * sizeof *k);
    if (!k)
        return 0;
    size_t max_exc = n / 16, nexc = 0;
    uint8_t *x = out + 5;
    for (size_t i = 0; i < n; i++) {
        if (f64_int(in[i], e, mul, &k[i]))
            continue;
        if (nexc == max_exc) {
            free(k);
            return 0;
        }
        store32(x, (uint32_t)i);
        memcpy(x + 4, &in[i], 8);
        x += 12;
        nexc++;
        k[i] = i ? k[i - 1] : 0; /* a zero delta keeps the stream smooth */
    }
    out[0] = (uint8_t)(e | (mul ? F64_MUL : 0));
    store32(out + 1, (uint32_t)nexc);
    size_t len = int_bp_encode(k, n, x);
    free(k);
    return len ? (size_t)(x - out) + len : 0;
}

int f64_decimal_decode(const uint8_t *in, size_t len, size_t n, double *out)
{
    if (n == 0)
        return 0;
    if (len < 5)
        return TSDB_ERR_CORRUPT;
    int e = in[0] & ~F64_MUL;
    bool mul = in[0] & F64_MUL;
    uint32_t nexc = load32(in + 1);
    if (e > F64_MAX_EXP || nexc > n || 5 + (size_t)nexc * 12 > len)
        return TSDB_ERR_CORRUPT;
    const uint8_t *x = in + 5, *ints = x + (size_t)nexc * 12;
    /* same size: the integers are decoded into out, then turned into doubles in place */
    if (int_bp_decode(ints, len - (size_t)(ints - in), n, (int64_t *)(void *)out) != 0)
        return TSDB_ERR_CORRUPT;
    for (size_t i = 0; i < n; i++) {
        int64_t ki;
        memcpy(&ki, &out[i], 8); /* the slot is read as an integer, then written as a double */
        out[i] = f64_back(ki, e, mul);
    }
    for (uint32_t j = 0; j < nexc; j++, x += 12) {
        uint32_t pos = load32(x);
        if (pos >= n)
            return TSDB_ERR_CORRUPT;
        memcpy(&out[pos], x + 4, 8);
    }
    return 0;
}

/* ------------------------------------------------- BLK_DELTA_BP (float64) */

/* As delta_bp32 (codec.c), on the 64-bit patterns: zigzag deltas packed at
 * up to 64 bits a value.
 *   u64 first | per vector of up to 1024 deltas: u8 bits (0..64), packed zigzags */
size_t delta_bp64_bound(size_t n) { return 8 + (n / IBP_VEC + 1) + 8 * n + 8; }

size_t delta_bp64_encode(const uint64_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    memcpy(out, &in[0], 8);
    uint8_t *o = out + 8;
    uint64_t z[IBP_VEC];
    for (size_t v0 = 1; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        uint64_t all = 0;
        for (size_t i = 0; i < nv; i++) {
            z[i] = zz((int64_t)(in[v0 + i] - in[v0 + i - 1])); /* wraps; the decoder's sum wraps back */
            all |= z[i];
        }
        int w = bits_for64(all);
        *o++ = (uint8_t)w;
        pack64(z, nv, w, o);
        o += packed_bytes(nv, w);
    }
    return (size_t)(o - out);
}

int delta_bp64_decode(const uint8_t *in, size_t len, size_t n, uint64_t *out)
{
    if (n == 0)
        return 0;
    if (len < 8)
        return TSDB_ERR_CORRUPT;
    uint64_t k;
    memcpy(&k, in, 8);
    out[0] = k;
    const uint8_t *p = in + 8, *end = in + len;
    uint64_t z[IBP_VEC];
    for (size_t v0 = 1; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        if (p >= end || *p > 64)
            return TSDB_ERR_CORRUPT;
        int w = *p++;
        size_t pb = packed_bytes(nv, w);
        if ((size_t)(end - p) < pb)
            return TSDB_ERR_CORRUPT;
        unpack64(p, nv, w, z);
        p += pb;
        uint64_t total = zz_prefix64(z, nv);
        for (size_t i = 0; i < nv; i++)
            out[v0 + i] = k + z[i];
        k += total;
    }
    return p == end ? 0 : TSDB_ERR_CORRUPT;
}

/* ---------------------------------------------------------------- BLK_XOR64 */

typedef struct bitw {
    uint8_t *p;
    uint64_t acc;
    int bits;
} bitw;

static inline void bw_put(bitw *w, uint64_t v, int n) /* n <= 64, MSB first */
{
    if (n == 0)
        return;
    if (n < 64)
        v &= (1ull << n) - 1;
    int room = 64 - w->bits;
    if (n <= room) {
        w->acc = n == 64 ? v : (w->acc << n) | v;
        w->bits += n;
    } else {
        w->acc = (w->acc << room) | (v >> (n - room));
        for (int b = 7; b >= 0; b--)
            *w->p++ = (uint8_t)(w->acc >> (8 * b));
        w->acc = v;
        w->bits = n - room;
    }
    if (w->bits == 64) {
        for (int b = 7; b >= 0; b--)
            *w->p++ = (uint8_t)(w->acc >> (8 * b));
        w->acc = 0;
        w->bits = 0;
    }
}

static inline void bw_flush(bitw *w)
{
    while (w->bits > 0) {
        int take = w->bits >= 8 ? 8 : w->bits;
        *w->p++ = (uint8_t)((w->acc >> (w->bits - take)) << (8 - take));
        w->bits -= take;
    }
}

typedef struct bitr {
    const uint8_t *p, *end;
    uint64_t pos; /* bit position */
} bitr;

static inline bool br_get(bitr *r, int n, uint64_t *out)
{
    uint64_t v = 0;
    if (r->pos + (uint64_t)n > (uint64_t)(r->end - r->p) * 8)
        return false;
    for (int i = 0; i < n; i++, r->pos++)
        v = (v << 1) | ((r->p[r->pos >> 3] >> (7 - (r->pos & 7))) & 1);
    *out = v;
    return true;
}

size_t xor64_bound(size_t n) { return 8 + (n * 78 + 7) / 8 + 16; }

size_t xor64_encode(const uint64_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    memcpy(out, &in[0], 8);
    bitw w = {out + 8, 0, 0};
    int lead = -1, trail = 0;
    for (size_t i = 1; i < n; i++) {
        uint64_t x = in[i] ^ in[i - 1];
        if (!x) {
            bw_put(&w, 0, 1);
            continue;
        }
        int l = __builtin_clzll(x), t = __builtin_ctzll(x);
        if (l > 63)
            l = 63;
        if (lead >= 0 && l >= lead && t >= trail) {
            bw_put(&w, 2, 2); /* 10: inside the previous window */
            bw_put(&w, x >> trail, 64 - lead - trail);
        } else {
            bw_put(&w, 3, 2); /* 11: new window */
            bw_put(&w, (uint64_t)l, 6);
            int len = 64 - l - t;
            bw_put(&w, (uint64_t)(len - 1), 6);
            bw_put(&w, x >> t, len);
            lead = l;
            trail = t;
        }
    }
    bw_flush(&w);
    return (size_t)(w.p - out);
}

int xor64_decode(const uint8_t *in, size_t len, size_t n, uint64_t *out)
{
    if (n == 0)
        return 0;
    if (len < 8)
        return TSDB_ERR_CORRUPT;
    memcpy(&out[0], in, 8);
    bitr r = {in + 8, in + len, 0};
    int lead = -1, trail = 0;
    for (size_t i = 1; i < n; i++) {
        uint64_t b, x;
        if (!br_get(&r, 1, &b))
            return TSDB_ERR_CORRUPT;
        if (!b) {
            out[i] = out[i - 1];
            continue;
        }
        if (!br_get(&r, 1, &b))
            return TSDB_ERR_CORRUPT;
        if (b) {
            uint64_t l, m;
            if (!br_get(&r, 6, &l) || !br_get(&r, 6, &m))
                return TSDB_ERR_CORRUPT;
            int len2 = (int)m + 1;
            if ((int)l + len2 > 64)
                return TSDB_ERR_CORRUPT;
            lead = (int)l;
            trail = 64 - lead - len2;
        } else if (lead < 0) {
            return TSDB_ERR_CORRUPT;
        }
        if (!br_get(&r, 64 - lead - trail, &x))
            return TSDB_ERR_CORRUPT;
        out[i] = out[i - 1] ^ (x << trail);
    }
    return 0;
}

/* ------------------------------------------------------------------ blocks */

static void bytesplit64(const uint64_t *in, size_t n, uint8_t *out)
{
    for (size_t i = 0; i < n; i++)
        for (int b = 0; b < 8; b++)
            out[(size_t)b * n + i] = (uint8_t)(in[i] >> (8 * b));
}

static int unsplit64(const uint8_t *in, size_t len, size_t n, uint64_t *out)
{
    if (len != 8 * n)
        return TSDB_ERR_CORRUPT;
    for (size_t i = 0; i < n; i++) {
        uint64_t x = 0;
        for (int b = 0; b < 8; b++)
            x |= (uint64_t)in[(size_t)b * n + i] << (8 * b);
        out[i] = x;
    }
    return 0;
}

static size_t encode_f64_kind(codec_ctx *c, int kind, const double *v, size_t n)
{
    switch (kind) {
    case BLK_F64_DECIMAL: return f64_decimal_encode(v, n, c->scratch);
    case BLK_XOR64: return xor64_encode((const uint64_t *)v, n, c->scratch);
    case BLK_DELTA_BP: return delta_bp64_encode((const uint64_t *)v, n, c->scratch);
    case BLK_ALPRD: return alprd64_encode((const uint64_t *)v, n, c->scratch);
    default: bytesplit64((const uint64_t *)v, n, c->scratch); return 8 * n;
    }
}

static size_t encode_f64(codec_ctx *c, int level, const double *v, size_t n, uint8_t *out)
{
    size_t need = MAX(MAX(f64_decimal_bound(n), xor64_bound(n)), 8 * n);
    need = MAX(need, MAX(delta_bp64_bound(n), alprd64_bound(n)));
    if (codec_scratch_reserve(c, need) != 0)
        return 0;
    size_t inner = f64_decimal_encode(v, n, c->scratch);
    if (inner && inner * 8 <= 16 * n)
        return block_wrap(c, level, BLK_F64_DECIMAL, inner, out);
    if (c->alt_cap < block_bound(n)) {
        uint8_t *alt = realloc(c->alt, block_bound(n));
        if (!alt)
            return 0;
        c->alt = alt;
        c->alt_cap = block_bound(n);
    }
    static const int kinds[] = {BLK_F64_DECIMAL, BLK_XOR64, BLK_BYTESPLIT64, BLK_DELTA_BP, BLK_ALPRD};
    size_t best = 0;
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++) {
        if (kinds[k] == BLK_F64_DECIMAL && !inner)
            continue;
        size_t len = encode_f64_kind(c, kinds[k], v, n);
        if (!len)
            continue;
        len = block_wrap(c, level, kinds[k], len, best ? c->alt : out);
        if (!best) {
            best = len;
        } else if (len < best) {
            memcpy(out, c->alt, len);
            best = len;
        }
    }
    return best;
}

static size_t encode_int(codec_ctx *c, int level, const int64_t *v, size_t n, uint8_t *out)
{
    if (codec_scratch_reserve(c, int_bp_bound(n)) != 0)
        return 0;
    return block_wrap(c, level, BLK_INT, int_bp_encode(v, n, c->scratch), out);
}

size_t block_encode_vals(codec_ctx *c, int level, int value_codec, int type, const void *v, size_t n, uint8_t *out)
{
    switch (type) {
    case TSDB_FLOAT32: return block_encode_f32(c, level, value_codec, v, n, out);
    case TSDB_FLOAT64: return encode_f64(c, level, v, n, out);
    case TSDB_INT64: return encode_int(c, level, v, n, out);
    default: {
        /* bool and state: 4-byte slots, coded as integers */
        int64_t *w = malloc(n * sizeof *w);
        if (!w)
            return 0;
        const uint32_t *u = v;
        for (size_t i = 0; i < n; i++)
            w[i] = u[i];
        size_t len = encode_int(c, level, w, n, out);
        free(w);
        return len;
    }
    }
}

int block_decode_vals(codec_ctx *c, int type, const uint8_t *blk, size_t len, size_t n, void *out)
{
    if (type == TSDB_FLOAT32)
        return block_decode_f32(c, blk, len, n, out);
    int kind, rc;
    const uint8_t *p;
    size_t plen;
    if ((rc = block_open(c, blk, len, n, &kind, &p, &plen)) != 0)
        return rc;
    if (type == TSDB_FLOAT64) {
        switch (kind) {
        case BLK_F64_DECIMAL: return f64_decimal_decode(p, plen, n, out);
        case BLK_XOR64: return xor64_decode(p, plen, n, out);
        case BLK_BYTESPLIT64: return unsplit64(p, plen, n, out);
        case BLK_DELTA_BP: return delta_bp64_decode(p, plen, n, out);
        case BLK_ALPRD: return alprd64_decode(p, plen, n, out);
        default: return TSDB_ERR_CORRUPT;
        }
    }
    if (kind != BLK_INT)
        return TSDB_ERR_CORRUPT;
    if (type == TSDB_INT64)
        return int_bp_decode(p, plen, n, out);
    int64_t *w = malloc(n * sizeof *w);
    if (!w)
        return TSDB_ERR_NOMEM;
    rc = int_bp_decode(p, plen, n, w);
    uint32_t *u = out;
    for (size_t i = 0; rc == 0 && i < n; i++)
        u[i] = (uint32_t)w[i];
    free(w);
    return rc;
}
