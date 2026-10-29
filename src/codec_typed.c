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

#include <float.h>
#include <math.h>
#include <stdlib.h>

/* AVX2 (the v3 level, and -march=native on CPUs without AVX-512DQ): there is no int64 <-> double conversion
 * below AVX-512DQ, so the scalar loops of the decimal codec convert one value at a time and divide one at a
 * time. With AVX-512DQ the compiler vectorizes them itself. */
#if defined(__AVX2__) && !defined(__AVX512DQ__)
#define F64_AVX2 1
#include <immintrin.h>
#endif

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

#ifdef F64_AVX2
/* Integers of magnitude below 2^51 convert exactly through the mantissa: 1.5 * 2^52 plus k is the double
 * 0x4338000000000000 + k, and adding a double d to 1.5 * 2^52 rounds it to the nearest integer (ties to even,
 * as llrint does in the default rounding mode) for |d| < 2^51. Both give what the scalar code gives. */
#define F64_MAGIC_BITS INT64_C(0x4338000000000000)
#define F64_FAST_LIMIT 2251799813685248.0 /* 2^51 */

/* Four values at once: true, with their integers in k, if every lane is in range and rebuilds its value bit for
 * bit (-0.0, NaN and the rest are left to f64_int, one at a time, as exceptions). */
static inline bool f64_int4(const double *in, int e, bool mul, int64_t *k)
{
    const __m256d magic = _mm256_castsi256_pd(_mm256_set1_epi64x(F64_MAGIC_BITS));
    __m256d v = _mm256_loadu_pd(in);
    __m256d d = _mm256_mul_pd(v, _mm256_set1_pd(f64_scale[e])); /* a separate multiply: no FMA */
    __m256d inr = _mm256_cmp_pd(_mm256_andnot_pd(_mm256_set1_pd(-0.0), d), _mm256_set1_pd(F64_FAST_LIMIT), _CMP_LT_OQ);
    if (_mm256_movemask_pd(inr) != 15)
        return false;
    __m256d t = _mm256_add_pd(d, magic);
    __m256d kd = _mm256_sub_pd(t, magic); /* the rounded integer, exact */
    __m256d back = mul ? _mm256_mul_pd(kd, _mm256_set1_pd(f64_inv[e])) : _mm256_div_pd(kd, _mm256_set1_pd(f64_scale[e]));
    __m256i same = _mm256_cmpeq_epi64(_mm256_castpd_si256(back), _mm256_castpd_si256(v));
    if (_mm256_movemask_pd(_mm256_castsi256_pd(same)) != 15)
        return false;
    _mm256_storeu_si256((__m256i *)(void *)k,
                        _mm256_sub_epi64(_mm256_castpd_si256(t), _mm256_set1_epi64x(F64_MAGIC_BITS)));
    return true;
}
#endif

/* One value: its integer into k[i], or an exception record at *xp. False when there are too many exceptions. */
static inline bool f64_enc_one(const double *in, size_t i, int e, bool mul, int64_t *k, uint8_t **xp, size_t *nexc,
                               size_t max_exc)
{
    if (f64_int(in[i], e, mul, &k[i]))
        return true;
    if (*nexc == max_exc)
        return false;
    store32(*xp, (uint32_t)i);
    memcpy(*xp + 4, &in[i], 8);
    *xp += 12;
    (*nexc)++;
    k[i] = i ? k[i - 1] : 0; /* a zero delta keeps the stream smooth */
    return true;
}

/* In place: the n integers in out (read as int64) become the doubles they stand for. */
static void f64_back_all(double *out, size_t n, int e, bool mul)
{
    size_t i = 0;
#ifdef F64_AVX2
    /* Whole block in [-2^51, 2^51) (damaged streams can hold any integer): (k + 2^51) < 2^52 as unsigned */
    const __m256i bias = _mm256_set1_epi64x(INT64_C(1) << 51);
    __m256i acc = _mm256_setzero_si256();
    size_t n4 = n & ~(size_t)3;
    for (; i < n4; i += 4)
        acc = _mm256_or_si256(acc, _mm256_add_epi64(_mm256_loadu_si256((const __m256i *)(const void *)&out[i]), bias));
    uint64_t tail = 0;
    for (size_t j = n4; j < n; j++) {
        int64_t kj;
        memcpy(&kj, &out[j], 8);
        tail |= (uint64_t)kj + (UINT64_C(1) << 51);
    }
    if (_mm256_testz_si256(_mm256_srli_epi64(acc, 52), _mm256_set1_epi64x(-1)) && !(tail >> 52)) {
        const __m256d magic = _mm256_castsi256_pd(_mm256_set1_epi64x(F64_MAGIC_BITS));
        const __m256i mbits = _mm256_set1_epi64x(F64_MAGIC_BITS);
        if (mul) {
            const __m256d f = _mm256_set1_pd(f64_inv[e]);
            for (i = 0; i < n4; i += 4) {
                __m256i ki = _mm256_loadu_si256((const __m256i *)(const void *)&out[i]);
                __m256d kd = _mm256_sub_pd(_mm256_castsi256_pd(_mm256_add_epi64(ki, mbits)), magic);
                _mm256_storeu_pd(&out[i], _mm256_mul_pd(kd, f));
            }
        } else {
            const __m256d f = _mm256_set1_pd(f64_scale[e]);
            for (i = 0; i < n4; i += 4) {
                __m256i ki = _mm256_loadu_si256((const __m256i *)(const void *)&out[i]);
                __m256d kd = _mm256_sub_pd(_mm256_castsi256_pd(_mm256_add_epi64(ki, mbits)), magic);
                _mm256_storeu_pd(&out[i], _mm256_div_pd(kd, f));
            }
        }
    } else
        i = 0;
#endif
    for (; i < n; i++) {
        int64_t ki;
        memcpy(&ki, &out[i], 8); /* the slot is read as an integer, then written as a double */
        out[i] = f64_back(ki, e, mul);
    }
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
    size_t nexc;
    return f64_decimal_encode_keep(in, n, out, NULL, &nexc);
}

size_t f64_decimal_encode_keep(const double *in, size_t n, uint8_t *out, int64_t *keep, size_t *nexcp)
{
    if (n < 2)
        return 0;
    bool mul = false;
    int e = f64_pick_scale(in, n, &mul);
    if (e < 0)
        return 0;
    int64_t *k = keep ? keep : malloc(n * sizeof *k);
    if (!k)
        return 0;
    size_t max_exc = n / 16, nexc = 0;
    uint8_t *x = out + 5;
    size_t i = 0;
    while (i < n) {
#ifdef F64_AVX2
        if (i + 4 <= n && f64_int4(&in[i], e, mul, &k[i])) {
            i += 4;
            continue;
        }
        /* a lane that is not a clean integer: these four one at a time, the exceptions in order */
        size_t stop = MIN(i + 4, n);
#else
        size_t stop = n;
#endif
        for (; i < stop; i++)
            if (!f64_enc_one(in, i, e, mul, k, &x, &nexc, max_exc)) {
                if (!keep)
                    free(k);
                return 0;
            }
    }
    out[0] = (uint8_t)(e | (mul ? F64_MUL : 0));
    store32(out + 1, (uint32_t)nexc);
    size_t len = int_bp_encode(k, n, x);
    if (!keep)
        free(k);
    *nexcp = nexc;
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
    f64_back_all(out, n, e, mul);
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

/* ----------------------------------------------------------------- BLK_DICT */

/*
 * Values that take few distinct bit patterns (readings converted to other units, unrounded: each is
 * one of the few hundred the sensor's integer steps map to). The distinct values, in numeric order,
 * then each value's rank among them as zigzag deltas packed per vector of 1024, as BLK_INT does:
 * neighbouring readings have neighbouring ranks, so a rank delta takes as few bits as the sensor's
 * integer step would, where deltas of the bit patterns pay for the scale as well.
 *   u32 nd | BLK_INT stream of the nd sorted keys | per vector: u8 bits (0..22), packed rank deltas
 * A key is the bit pattern made unsigned-monotone (sign bit flipped, or all bits of a negative
 * value), so equal values with other patterns (0.0 and -0.0, NaN payloads) stay apart.
 */
#define DICT_MAX 2048 /* at most this many distinct values, and at most a quarter of the block */
_Static_assert(DICT_MAX < 65535 && 2 * DICT_MAX < (1 << ZZ_PREFIX32_MAX_W), "ranks: uint16 slots, prefix sums");

static inline uint64_t dict_key(uint64_t u, int width)
{
    uint64_t sign = width == 4 ? 0x80000000u : 0x8000000000000000u, all = width == 4 ? 0xffffffffu : ~0ull;
    return u & sign ? ~u & all : u | sign;
}

static inline uint64_t dict_unkey(uint64_t k, int width)
{
    uint64_t sign = width == 4 ? 0x80000000u : 0x8000000000000000u, all = width == 4 ? 0xffffffffu : ~0ull;
    return k & sign ? k ^ sign : ~k & all;
}

static inline uint64_t dict_load(const void *v, size_t i, int width)
{
    if (width == 4) {
        uint32_t x;
        memcpy(&x, (const uint8_t *)v + 4 * i, 4);
        return x;
    }
    uint64_t x;
    memcpy(&x, (const uint8_t *)v + 8 * i, 8);
    return x;
}

static inline size_t dict_hash(uint64_t k, int bits) { return (size_t)((k * 0x9E3779B97F4A7C15ull) >> (64 - bits)); }

size_t dict_bound(size_t n) { return 4 + int_bp_bound(MIN(n, (size_t)DICT_MAX)) + (n / IBP_VEC + 1) + 4 * n + 8; }

/* LSD radix sort of n keys, a byte at a time, skipping bytes all keys share. */
static void dict_sort(uint64_t *k, size_t n, int width)
{
    uint64_t tmp[DICT_MAX], *a = k, *b = tmp;
    for (int sh = 0; sh < 8 * width; sh += 8) {
        size_t cnt[256] = {0};
        for (size_t i = 0; i < n; i++)
            cnt[a[i] >> sh & 255]++;
        if (cnt[a[0] >> sh & 255] == n)
            continue;
        size_t pos = 0;
        for (int d = 0; d < 256; d++) {
            size_t c = cnt[d];
            cnt[d] = pos;
            pos += c;
        }
        for (size_t i = 0; i < n; i++)
            b[cnt[a[i] >> sh & 255]++] = a[i];
        uint64_t *t = a;
        a = b;
        b = t;
    }
    if (a != k)
        memcpy(k, a, n * sizeof *k);
}

/* One vector of ranks as zigzag deltas from the previous rank, packed at the width of the widest. */
static uint8_t *dict_put_ranks(const uint32_t *r, size_t nv, int64_t *prev, uint8_t *o)
{
    uint32_t z[IBP_VEC], all = 0;
    int64_t p = *prev;
    for (size_t i = 0; i < nv; i++) {
        z[i] = (uint32_t)zz((int64_t)r[i] - p);
        all |= z[i];
        p = r[i];
    }
    *prev = p;
    int w = bits_for(all);
    *o++ = (uint8_t)w;
    pack(z, nv, w, o);
    return o + packed_bytes(nv, w);
}

/* Encodes n values of `width` bytes; 0 when they have more than cap distinct values (at most DICT_MAX).
 * *table: the bytes of the header and the distinct values. */
static size_t dict_encode_t(const void *v, size_t n, int width, uint8_t *out, size_t *table, size_t cap)
{
    cap = MIN(MIN(cap, n), (size_t)DICT_MAX);
    if (cap == 0)
        return 0;
    int bits = 1;
    while (((size_t)1 << bits) < 2 * cap)
        bits++;
    const size_t mask = ((size_t)1 << bits) - 1;
    uint64_t keys[2 * DICT_MAX], sorted[DICT_MAX];
    sorted[0] = 0; /* n > 0 puts a key there; the compiler cannot tell */
    uint16_t slot[2 * DICT_MAX]; /* 0: empty; 1 + the key's rank once sorted */
    memset(slot, 0, (mask + 1) * sizeof *slot);
    size_t nd = 0;
    for (size_t i = 0; i < n; i++) {
        uint64_t k = dict_key(dict_load(v, i, width), width);
        size_t h = dict_hash(k, bits);
        while (slot[h] && keys[h] != k)
            h = (h + 1) & mask;
        if (!slot[h]) {
            if (nd == cap)
                return 0;
            slot[h] = 1;
            keys[h] = k;
            sorted[nd++] = k;
        }
    }
    dict_sort(sorted, nd, width);
    for (size_t r = 0; r < nd; r++) {
        size_t h = dict_hash(sorted[r], bits);
        while (keys[h] != sorted[r])
            h = (h + 1) & mask;
        slot[h] = (uint16_t)(1 + r);
    }
    uint32_t nd32 = (uint32_t)nd;
    memcpy(out, &nd32, 4);
    uint8_t *o = out + 4;
    o += int_bp_encode((const int64_t *)sorted, nd, o);
    *table = (size_t)(o - out);
    uint32_t r[IBP_VEC];
    int64_t prev = 0;
    for (size_t v0 = 0; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        for (size_t i = 0; i < nv; i++) {
            uint64_t k = dict_key(dict_load(v, v0 + i, width), width);
            size_t h = dict_hash(k, bits);
            while (keys[h] != k)
                h = (h + 1) & mask;
            r[i] = slot[h] - 1u;
        }
        o = dict_put_ranks(r, nv, &prev, o);
    }
    return (size_t)(o - out);
}

/*
 * The same block from the integers q of its fixed point: equal integers are equal values, in the
 * values' order, so a table indexed by integer replaces the hash and the sort. The nx exceptions
 * (positions xpos, ascending; q holds a neighbour's integer there) are values off the grid: sorted
 * apart and merged in. tab: span entries (span at most DICT_SPAN) and n / 64 + 1 words after them;
 * n below 65536.
 */
static size_t dict_encode_q(const int64_t *q, const void *v, size_t n, int width, int64_t lo, size_t span,
                            const uint32_t *xpos, size_t nx, uint16_t *tab, uint8_t *out, size_t cap)
{
    cap = MIN(MIN(cap, n), (size_t)DICT_MAX);
    if (nx > cap)
        return 0;
    size_t span4 = (span + 3) / 4 * 4;
    memset(tab, 0, span4 * sizeof *tab);
    for (size_t i = 0; i < n; i++)
        tab[q[i] - lo] = (uint16_t)(i + 1); /* any of the equal values will do */
    uint64_t xk[DICT_MAX], sorted[DICT_MAX];
    uint16_t xr[DICT_MAX];
    size_t nu = 0;
    if (nx) {
        uint64_t *isx = (uint64_t *)(void *)(tab + span4);
        memset(isx, 0, (n / 64 + 1) * sizeof *isx);
        for (size_t t = 0; t < nx; t++) {
            isx[xpos[t] >> 6] |= 1ull << (xpos[t] & 63);
            xk[t] = dict_key(dict_load(v, xpos[t], width), width);
        }
        for (size_t t = 0; t < nx; t++) { /* an exception standing for an integer: another value of it, or none */
            uint16_t *e = &tab[q[xpos[t]] - lo];
            if (*e != xpos[t] + 1)
                continue;
            *e = 0;
            for (size_t j = xpos[t]; j-- > 0;)
                if (q[j] == q[xpos[t]] && !(isx[j >> 6] >> (j & 63) & 1)) {
                    *e = (uint16_t)(j + 1);
                    break;
                }
        }
        dict_sort(xk, nx, width);
        for (size_t t = 0; t < nx; t++)
            if (t == 0 || xk[t] != xk[nu - 1])
                xk[nu++] = xk[t];
    }
    size_t nd = 0, u = 0;
    for (size_t j = 0; j < span; j += 4) {
        uint64_t four;
        memcpy(&four, tab + j, 8);
        if (!four)
            continue; /* sparse: most integers in the span are unused */
        for (size_t t = j; t < j + 4; t++) {
            if (!tab[t])
                continue;
            uint64_t k = dict_key(dict_load(v, tab[t] - 1u, width), width);
            for (; u < nu && xk[u] < k; u++) {
                if (nd == cap)
                    return 0;
                xr[u] = (uint16_t)nd;
                sorted[nd++] = xk[u];
            }
            if (nd == cap)
                return 0;
            sorted[nd] = k;
            tab[t] = (uint16_t)nd++;
        }
    }
    for (; u < nu; u++) {
        if (nd == cap)
            return 0;
        xr[u] = (uint16_t)nd;
        sorted[nd++] = xk[u];
    }
    uint32_t nd32 = (uint32_t)nd;
    memcpy(out, &nd32, 4);
    uint8_t *o = out + 4;
    o += int_bp_encode((const int64_t *)sorted, nd, o);
    uint32_t r[IBP_VEC];
    int64_t prev = 0;
    size_t x = 0;
    for (size_t v0 = 0; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        for (size_t i = 0; i < nv; i++)
            r[i] = tab[q[v0 + i] - lo];
        for (; x < nx && xpos[x] < v0 + nv; x++) {
            uint64_t k = dict_key(dict_load(v, xpos[x], width), width);
            size_t a = 0, b = nu; /* k is among xk[0..nu) */
            while (b - a > 1) {
                size_t m = (a + b) / 2;
                if (xk[m] <= k)
                    a = m;
                else
                    b = m;
            }
            r[xpos[x] - v0] = xr[a];
        }
        o = dict_put_ranks(r, nv, &prev, o);
    }
    return (size_t)(o - out);
}

size_t dict_encode(const void *v, size_t n, int width, uint8_t *out)
{
    size_t table;
    return dict_encode_t(v, n, width, out, &table, n / 4);
}

int dict_decode(const uint8_t *in, size_t len, size_t n, int width, void *out)
{
    if (len < 4)
        return TSDB_ERR_CORRUPT;
    uint32_t nd;
    memcpy(&nd, in, 4);
    if (nd == 0 || nd > DICT_MAX || nd > n)
        return TSDB_ERR_CORRUPT;
    int64_t tab[DICT_MAX];
    size_t used = int_bp_decode_prefix(in + 4, len - 4, nd, tab);
    if (!used)
        return TSDB_ERR_CORRUPT;
    for (size_t r = 0; r < nd; r++)
        tab[r] = (int64_t)dict_unkey((uint64_t)tab[r], width);
    const uint8_t *p = in + 4 + used, *end = in + len;
    uint32_t z[IBP_VEC];
    uint64_t k = 0;
    for (size_t v0 = 0; v0 < n; v0 += IBP_VEC) {
        size_t nv = MIN((size_t)IBP_VEC, n - v0);
        if (p >= end)
            return TSDB_ERR_CORRUPT;
        int w = *p++;
        size_t pb = packed_bytes(nv, w);
        /* rank deltas stay below 2 * DICT_MAX: anything wider is damage, and the int32 sums below hold */
        if (w > ZZ_PREFIX32_MAX_W || (size_t)(end - p) < pb)
            return TSDB_ERR_CORRUPT;
        unpack(p, nv, w, z);
        p += pb;
        uint32_t total = zz_prefix32(z, nv);
        for (size_t i = 0; i < nv; i++) {
            uint64_t r = k + (uint64_t)(int64_t)(int32_t)z[i];
            if (r >= nd)
                return TSDB_ERR_CORRUPT;
            if (width == 4)
                ((uint32_t *)out)[v0 + i] = (uint32_t)tab[r];
            else
                ((uint64_t *)out)[v0 + i] = (uint64_t)tab[r];
        }
        k += (uint64_t)(int64_t)(int32_t)total;
    }
    return p == end ? 0 : TSDB_ERR_CORRUPT;
}

static size_t encode_f64_kind(int kind, const void *v, size_t n, uint8_t *out)
{
    switch (kind) {
    case BLK_DICT: return dict_encode(v, n, 8, out);
    case BLK_XOR64: return xor64_encode((const uint64_t *)v, n, out);
    case BLK_DELTA_BP: return delta_bp64_encode((const uint64_t *)v, n, out);
    case BLK_ALPRD: return alprd64_encode((const uint64_t *)v, n, out);
    default: bytesplit64((const uint64_t *)v, n, out); return 8 * n;
    }
}

/*
 * AUTO's choice among transforms, for float32 (block_encode_f32) and float64
 * (encode_f64), once fixed point that fits in 16 bits a value has been ruled
 * out. Each candidate encodes a sample (runs of AUTO_RUN consecutive values
 * spread over the block, AUTO_SAMPLE_BYTES in all: 16 runs of float32, 8 of
 * float64), compressed as its block would be: the sample's size, scaled to
 * the block, ranks them. Only the best, and any other within AUTO_CLOSE of it,
 * are encoded in full (with ZSTD where it saves). Half the sample (8 runs of
 * float32) chose blocks 5% larger on real pressures in psi.
 * Encoding and compressing all five float64 candidates in full halved the
 * encode speed of random doubles for nothing: with the sample ranking them,
 * sizes stayed within 0.11% on every test set. An entropy estimate instead of
 * ZSTD ranked badly where ZSTD's matches decide: repeated deltas of calibrated
 * readings, the low byte planes of a byte-stream split, and float32 pressures
 * converted from bar to psi (real data: bit-packed deltas, which hide the
 * repeats, chosen where plain deltas and ZSTD were 27% smaller). ZSTD is tried on the full
 * blocks even where it gained nothing on the sample: matches far apart in the
 * block (a random walk that comes back) can save 17% that eight short runs
 * never show. Where a full block comes out more than 10% below its estimate,
 * such matches decide and may favour another transform, so every candidate is
 * encoded in full.
 */
enum { AUTO_RUN = 128, AUTO_SAMPLE_BYTES = 8192, AUTO_SAMPLE_MAX = AUTO_SAMPLE_BYTES / 4 };
#define AUTO_CLOSE 1.03
/* a candidate estimated this far above a block in hand (the dictionary) is not encoded: psi in merges went from
 * 15 to 53-66 M values/s at the same sizes */
#define AUTO_HAVE_MARGIN 1.25

/*
 * Whether ZSTD can save on `len` bytes at src, judged by its fastest
 * level: where it finds less than 1/AUTO_PROBE_GAIN (most of what is left
 * after the transforms of noisy values), `level` would not gain the 1/32
 * block_wrap asks for either, and a probe costs a tenth of a real attempt
 * on such bytes. Sizes stayed the same on every test set.
 */
#define AUTO_PROBE_LEVEL (-1)
#define AUTO_PROBE_GAIN 128
static bool zstd_may_gain(codec_ctx *c, const uint8_t *src, size_t len, uint8_t *tmp)
{
    size_t z = zstd_pack(c->cctx, tmp, ZSTD_compressBound(len), src, len, AUTO_PROBE_LEVEL);
    return !ZSTD_isError(z) && z + 4 < len - len / AUTO_PROBE_GAIN;
}

/* The size of block_wrap's block of the `len` transform bytes at c->alt (ZSTD's output goes to tmp). */
static size_t sample_block_size(codec_ctx *c, int level, size_t len, uint8_t *tmp)
{
    if (level <= 0 || len < 64 || !zstd_may_gain(c, c->alt, len, tmp))
        return 1 + len;
    size_t z = zstd_pack(c->cctx, tmp, ZSTD_compressBound(len), c->alt, len, level);
    if (ZSTD_isError(z))
        return 1 + len;
    return z + 4 < len - len / 32 ? 5 + z : 1 + len;
}

/*
 * Picks among kinds[0..nk) for the n values of `width` bytes at v, encoding
 * each with enc. pre_kind's transform is already in c->scratch (pre_inner
 * bytes, 0 if it failed): its real block competes, no sample needed. Below
 * all_bits a value, ZSTD's matches decide, and a sample (a smaller window)
 * underrates them more for some transforms than for others: there every
 * candidate is encoded. Such blocks also compress fast. Returns the block's
 * size in out, 0 when out of memory.
 */
size_t auto_pick(codec_ctx *c, int level, const int *kinds, size_t nk, codec_kind_fn enc, const void *v, size_t n,
                 size_t width, int pre_kind, size_t pre_inner, size_t have, double all_bits, double surprise,
                 uint8_t *out)
{
    enum { NK_MAX = 10 };
    if (nk > NK_MAX)
        nk = NK_MAX;
    /* c->alt: a sample's transform, its ZSTD output after it; later the second block */
    size_t sb = block_bound(AUTO_SAMPLE_MAX); /* any width */
    if (codec_alt_reserve(c, MAX(block_bound(n), 2 * sb)) != 0)
        return have;
    _Alignas(8) uint8_t smp[AUTO_SAMPLE_BYTES];
    const size_t runs = AUTO_SAMPLE_BYTES / (AUTO_RUN * width), sample = runs * AUTO_RUN;
    const uint8_t *s = v;
    size_t ns = n;
    if (n > sample) {
        for (size_t r = 0; r < runs; r++)
            memcpy(smp + r * AUTO_RUN * width, (const uint8_t *)v + (n - AUTO_RUN) * r / (runs - 1) * width,
                   AUTO_RUN * width);
        s = smp;
        ns = sample;
    }
    double est[NK_MAX];
    for (size_t k = 0; k < NK_MAX; k++)
        est[k] = INFINITY;
    bool zs[NK_MAX] = {false}; /* ZSTD saved on the sample */
    size_t best = have;
    for (size_t k = 0; k < nk; k++) {
        size_t len = 0;
        if (kinds[k] == pre_kind) {
            /* already encoded in full: its real block (a sample can miss the scale or fail) */
            if (pre_inner) {
                len = block_wrap(c, level, pre_kind, pre_inner, best ? c->alt : out);
                if (best && len < best)
                    memcpy(out, c->alt, len);
                best = best ? MIN(best, len) : len;
                est[k] = (double)len;
            }
            continue;
        }
        if (kinds[k] == BLK_DELTA_BP && s == smp) {
            /* deltas across the seams of the runs would widen whole vectors: one run at a time */
            for (size_t r = 0; r < runs; r++)
                len += enc(kinds[k], smp + r * AUTO_RUN * width, AUTO_RUN, c->alt + len);
        } else {
            len = enc(kinds[k], s, ns, c->alt);
        }
        if (len) {
            size_t z = sample_block_size(c, level, len, c->alt + sb);
            zs[k] = z != 1 + len;
            est[k] = (double)z * (double)n / (double)ns;
        }
    }
    size_t b1 = 0;
    for (size_t k = 1; k < nk; k++)
        if (est[k] < est[b1])
            b1 = k;
    double limit = level > 0 && est[b1] < all_bits / 8 * (double)n ? DBL_MAX : est[b1] * AUTO_CLOSE;
    if (have) /* a dictionary block, in full: only what may beat it, its estimate's error allowed for */
        limit = MIN(limit, (double)have * AUTO_HAVE_MARGIN);
    bool done[NK_MAX] = {false};
    for (int pass = 0; pass < 2; pass++) {
        for (size_t k = 0; k < nk; k++) {
            if (kinds[k] == pre_kind || done[k] || !(est[k] <= limit))
                continue;
            done[k] = true;
            uint8_t *dst = best ? c->alt : out;
            size_t inner_k = enc(kinds[k], v, n, c->scratch);
            if (!inner_k) { /* the sample passed, the block does not: the others get their turn */
                limit = DBL_MAX;
                continue;
            }
            /* ZSTD that saved nothing on the sample is still tried (far matches), behind a probe */
            int lv = zs[k] || (level > 0 && inner_k >= 64 && zstd_may_gain(c, c->scratch, inner_k, dst)) ? level : 0;
            size_t len = block_wrap(c, lv, kinds[k], inner_k, dst);
            /* far below its estimate: matches the sample cannot see decide, and may favour another kind */
            if ((double)len < est[k] * surprise)
                limit = DBL_MAX;
            if (!best) {
                best = len;
            } else if (len < best) {
                memcpy(out, c->alt, len);
                best = len;
            }
        }
    }
    return best;
}

/*
 * Whether the integers fill most of the range they span where sampled (noise moves a reading by a
 * step or two): their ranks then pack no tighter than the integers do. Two runs of 64, about 1% of
 * a block's encode; nearly constant runs say nothing.
 */
static bool dict_dense(const int64_t *q, size_t n)
{
    enum { RUNS = 2, RUN = 64, SPAN = 1024 };
    size_t d = 0, span = 0;
    for (size_t r = 0; r < RUNS; r++) {
        const int64_t *p = q + (n - RUN) * (2 * r + 1) / (2 * RUNS);
        int64_t lo = p[0], hi = p[0];
        for (size_t i = 1; i < RUN; i++) {
            lo = MIN(lo, p[i]);
            hi = MAX(hi, p[i]);
        }
        uint64_t s = (uint64_t)hi - (uint64_t)lo + 1;
        if (s < 8)
            continue;
        if (s > SPAN) {
            span += SPAN;
            continue;
        }
        uint64_t bm[SPAN / 64];
        size_t nw = (size_t)(s + 63) / 64;
        memset(bm, 0, nw * sizeof *bm);
        for (size_t i = 0; i < RUN; i++) {
            uint64_t o = (uint64_t)p[i] - (uint64_t)lo;
            bm[o >> 6] |= 1ull << (o & 63);
        }
        for (size_t w = 0; w < nw; w++)
            d += (size_t)__builtin_popcountll(bm[w]);
        span += (size_t)s;
    }
    return 2 * d > span; /* more than half the codes taken */
}

/*
 * Fixed point that fits in 16 bits a value (pre_kind's pre_inner bytes in c->scratch, its integers
 * in q) is kept, unless the values take so few distinct codes that their ranks pack tighter than the
 * integers: readings printed at a finer resolution than the instrument has (pressures at 0.001 bar
 * from an ADC step of about 0.05 bar), or slow sensors that stay on a few values. Dense integers
 * (dict_dense) are kept at once; otherwise the dictionary is built from the integers, and it is
 * compressed and compared only if it came out small enough to have a chance.
 */
size_t fixed_or_dict(codec_ctx *c, int level, int pre_kind, size_t pre_inner, const int64_t *q, size_t nexc,
                     const void *v, size_t n, int width, uint8_t *out)
{
    size_t len = block_wrap(c, level, pre_kind, pre_inner, out);
    if (len == 0 || !q || n < 64 || n > 65535 || c->work_cap < DICT_WORK(n) || dict_dense(q, n))
        return len;
    int64_t lo = q[0], hi = q[0];
    for (size_t i = 1; i < n; i++) {
        lo = MIN(lo, q[i]);
        hi = MAX(hi, q[i]);
    }
    if ((uint64_t)hi - (uint64_t)lo >= DICT_SPAN || nexc > DICT_MAX || codec_alt_reserve(c, block_bound(n)) != 0)
        return len;
    /* the exceptions' positions, from the fixed-point block before c->scratch is overwritten */
    uint32_t xpos[DICT_MAX];
    const uint8_t *x = pre_kind == BLK_DECIMAL_BP ? c->scratch + pre_inner - 8 * nexc : c->scratch + 5;
    for (size_t t = 0; t < nexc; t++)
        xpos[t] = load32(x + (pre_kind == BLK_DECIMAL_BP ? 8 : 12) * t);
    size_t inner = dict_encode_q(q, v, n, width, lo, (size_t)((uint64_t)hi - (uint64_t)lo) + 1, xpos, nexc,
                                 (uint16_t *)(void *)(c->work + 8 * n), c->scratch, n / 4);
    /* ZSTD saves about as much on ranks as on the integers (PS2: 36% on both): the dictionary is compressed
     * when it is smaller than the fixed point before compression */
    if (!inner || inner >= pre_inner)
        return len;
    size_t dl = block_wrap(c, level, BLK_DICT, inner, c->alt);
    if (dl && dl < len) {
        memcpy(out, c->alt, dl);
        len = dl;
    }
    return len;
}

/*
 * The dictionary block of values that are not compact fixed point, in out (0 if they have too many
 * distinct values), built from c->alt so that c->scratch keeps what the caller put there. A sample
 * cannot tell: runs of consecutive values see a fraction of the distinct values a block has (on a
 * sine converted to psi, 700 of 2000), so the whole block is tried; it stops at the first value past
 * the cap, after about n / 4 values where nearly every one is new (random bits: 5% of their encode).
 */
size_t dict_block(codec_ctx *c, int level, const void *v, size_t n, int width, uint8_t *out)
{
    if (codec_alt_reserve(c, block_bound(n)) != 0)
        return 0;
    size_t inner = dict_encode(v, n, width, c->alt);
    return inner ? block_wrap_src(c, level, BLK_DICT, c->alt, inner, out) : 0;
}

/*
 * float64 AUTO at insert speed (CODEC_FAST), beyond compact fixed point (its transform, `inner` bytes, in
 * c->scratch): wider fixed point, Gorilla's XOR and the byte split, each compressed in full, the smallest
 * kept. A sample ranking with ALP_rd and bit-packed deltas among the candidates (merges, auto_pick) makes
 * pressures converted to psi three times smaller, but ZSTD then has far more to find: 0.7 times the encode
 * speed on those, and where two kinds come close the sample is paid on top of both full encodes.
 */
static size_t f64_fast(codec_ctx *c, int level, const double *v, size_t n, size_t inner, uint8_t *out)
{
    if (codec_alt_reserve(c, block_bound(n)) != 0)
        return 0;
    size_t best = inner ? block_wrap(c, level, BLK_F64_DECIMAL, inner, out) : 0;
    static const int kinds[] = {BLK_XOR64, BLK_BYTESPLIT64};
    for (size_t k = 0; k < 2; k++) {
        size_t inner_k = encode_f64_kind(kinds[k], v, n, c->scratch);
        if (!inner_k)
            continue;
        size_t len = block_wrap(c, level, kinds[k], inner_k, best ? c->alt : out);
        if (!best) {
            best = len;
        } else if (len < best) {
            memcpy(out, c->alt, len);
            best = len;
        }
    }
    return best;
}

/* ------------------------------------------------------------- BLK_F32_WIDE */

/*
 * A float64 series fed float32 readings (a collector that widens what the sensor gives it) holds doubles like
 * 35.099998474121094: no longer decimals, and with 29 zero bits at the bottom of every mantissa that XOR and
 * byte split pay for anyway. Such a block is stored as the float32 values: a whole float32 block (its own kind,
 * ZSTD and all, at the block's effort) behind one header byte, decoded and widened back to the same bits.
 * Only values whose float32 widens back to their exact bits qualify, so NaN payloads, -0.0 and infinities
 * survive; the float64 null maps to the float32 null, and the double that the float32 null widens to does not
 * qualify, so the mapping is one to one. Compact float64 fixed point is kept as such before this is tried,
 * and 32 probes across the block turn real doubles away (zeros and whole numbers are float32 too, so a scan from
 * the start can run far): inserts of real doubles pay for a few dozen conversions a block. Inserts take only a
 * compact fixed-point float32 block, decided on a sample before the full pass; float32 values that are not
 * (pressures converted to psi, widened) go on to f64_fast at the speed they had, as the full float32 AUTO there
 * ran at 0.6-0.75 times it, and merges, which pick among all the float32 kinds, store them as float32.
 */
static int narrow_reserve(codec_ctx *c, size_t need)
{
    if (c->narrow_cap >= need)
        return 0;
    uint8_t *p = realloc(c->narrow, need);
    if (!p)
        return TSDB_ERR_NOMEM;
    c->narrow = p;
    c->narrow_cap = need;
    return 0;
}

/* Whether 32 values spread over the block are all float32 (or the null). */
static bool f64_probe_f32(const double *v, size_t n)
{
    enum { PROBES = 32 };
    size_t step = n > PROBES ? n / PROBES : 1;
    uint32_t bad = 0;
    for (size_t i = 0; i < n; i += step) {
        uint64_t b, wb;
        double w = (double)(float)v[i];
        memcpy(&b, &v[i], 8);
        memcpy(&wb, &w, 8);
        bad |= (uint32_t)(wb != b) & (uint32_t)(b != NULL_F64);
    }
    return !bad;
}

/* The float32 bits of n doubles into f; false as soon as a run of them holds one that is not a float32. */
static bool f64_narrow(const double *v, size_t n, uint32_t *f)
{
    enum { RUN = 64 }; /* vectorized within, an early exit between */
    for (size_t i0 = 0; i0 < n; i0 += RUN) {
        size_t end = MIN(n, i0 + RUN);
        uint32_t bad = 0;
        for (size_t i = i0; i < end; i++) {
            uint64_t b, wb;
            uint32_t u;
            float x = (float)v[i];
            double w = (double)x;
            memcpy(&b, &v[i], 8);
            memcpy(&u, &x, 4);
            memcpy(&wb, &w, 8);
            bool null = b == NULL_F64;
            f[i] = null ? NULL_F32 : u;
            bad |= null ? 0u : (uint32_t)(wb != b) | (uint32_t)(u == NULL_F32);
        }
        if (bad)
            return false;
    }
    return true;
}

static void f32_widen(const uint32_t *f, size_t n, double *out)
{
    for (size_t i = 0; i < n; i++) {
        float x;
        memcpy(&x, &f[i], 4);
        double w = (double)x;
        uint64_t b;
        memcpy(&b, &w, 8);
        b = f[i] == NULL_F32 ? NULL_F64 : b;
        memcpy(&out[i], &b, 8);
    }
}

/* float32 values as a float32 block (at insert only compact fixed point), or 0. The first `keep` bytes of
 * c->scratch survive at insert (they are copied aside); *tried: in merges, c->scratch and c->work were used. */
static size_t f32_wide(codec_ctx *c, int level, const double *v, size_t n, uint8_t *out, size_t keep, bool *tried)
{
    if (!f64_probe_f32(v, n) || narrow_reserve(c, 4 * n) != 0 || !f64_narrow(v, n, (uint32_t *)(void *)c->narrow))
        return 0;
    bool merge = c->effort == CODEC_MERGE;
    if (!merge && keep) {
        if (codec_alt_reserve(c, MAX(keep, block_bound(n))) != 0)
            return 0;
        memcpy(c->alt, c->scratch, keep);
    }
    *tried = merge;
    size_t len = block_encode_f32(c, level, merge ? TSDB_VALUES_AUTO : VALUES_FIXED_ONLY,
                                  (const float *)(void *)c->narrow, n, out + 1);
    if (len) {
        out[0] = BLK_F32_WIDE;
        return 1 + len;
    }
    if (!merge && keep)
        memcpy(c->scratch, c->alt, keep);
    return 0;
}

/* AUTO for float64: fixed point that fits in 16 bits a value is kept at once, then float32 values as a float32
 * block, otherwise f64_fast or, in merges, auto_pick with a dictionary. */
static size_t encode_f64(codec_ctx *c, int level, const double *v, size_t n, uint8_t *out)
{
    size_t need = MAX(MAX(f64_decimal_bound(n), xor64_bound(n)), 8 * n);
    need = MAX(need, MAX(MAX(delta_bp64_bound(n), alprd64_bound(n)), dict_bound(n)));
    if (codec_scratch_reserve(c, need) != 0)
        return 0;
    bool merge = c->effort == CODEC_MERGE;
    int64_t *q = merge && codec_work_reserve(c, DICT_WORK(n)) == 0 ? (int64_t *)(void *)c->work : NULL;
    size_t nexc = 0, inner = f64_decimal_encode_keep(v, n, c->scratch, q, &nexc);
    if (inner && inner * 8 <= 16 * n)
        return merge ? fixed_or_dict(c, level, BLK_F64_DECIMAL, inner, q, nexc, v, n, 8, out)
                     : block_wrap(c, level, BLK_F64_DECIMAL, inner, out);
    bool tried = false;
    size_t wide = f32_wide(c, level, v, n, out, inner, &tried);
    if (wide)
        return wide;
    if (inner && tried) /* the float32 encoder wrote over it */
        inner = f64_decimal_encode_keep(v, n, c->scratch, q, &nexc);
    if (!merge)
        return f64_fast(c, level, v, n, inner, out);
    static const int kinds[] = {BLK_F64_DECIMAL, BLK_XOR64, BLK_BYTESPLIT64, BLK_DELTA_BP, BLK_ALPRD};
    return auto_pick(c, level, kinds, sizeof kinds / sizeof *kinds, encode_f64_kind, v, n, 8, BLK_F64_DECIMAL, inner,
                     dict_block(c, level, v, n, 8, out), 24, 0.9, out);
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
    if (type == TSDB_FLOAT64 && len > 0 && blk[0] == BLK_F32_WIDE) { /* the inner block cannot be one */
        if (narrow_reserve(c, 4 * n) != 0)
            return TSDB_ERR_NOMEM;
        if ((rc = block_decode_f32(c, blk + 1, len - 1, n, (float *)(void *)c->narrow)) != 0)
            return rc;
        f32_widen((const uint32_t *)(void *)c->narrow, n, out);
        return 0;
    }
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
        case BLK_DICT: return dict_decode(p, plen, n, 8, out);
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
