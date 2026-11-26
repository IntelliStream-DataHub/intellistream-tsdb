/* SPDX-License-Identifier: AGPL-3.0-only */
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
#include "fpmode.h"
#include "bits.h"

#include <math.h>
#include <stdlib.h>

/* AVX2 without AVX-512DQ (the v3 level, and -march=native on AVX2-only CPUs): there is no int64 <-> double
 * conversion below AVX-512DQ, so dec_core stays scalar without the 1.5 * 2^52 mantissa trick (as f64_int4
 * in codec_typed.c). With AVX-512DQ GCC auto-vectorizes dec_core directly with vcvtpd2qq. */
#if defined(__AVX2__) && !defined(__AVX512DQ__)
#define DEC_AVX2 1
#include <immintrin.h>
#endif

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

/*
 * The timestamp decoders come in two forms: the plain one decodes any bytes to any values, the _sorted one
 * (block_decode_ts_sorted, which granule_decode uses) also refuses a block whose values do not strictly increase as
 * int64. Damage that the checksum did not see (verify_checksums = 0) decodes to anything, and the readers index
 * their bucket arrays by the rows' times on the strength of the order and the entry's window. The check costs least
 * where the values are made: a DoubleDelta run of unchanged deltas, the bulk of a regular series, is increasing by
 * one overflow test per run, and the other rows compare while the value is in a register.
 * In biased form (u = v ^ 2^63) int64 order is uint64 order, so a run v + D, v + 2D, ... v + rD increases iff
 * D != 0 and u + rD does not pass 2^64.
 */
static inline bool ts_run_bad(uint64_t prev, uint64_t delta, size_t r)
{
    uint64_t span;
    return delta == 0 || __builtin_mul_overflow((uint64_t)r, delta, &span) ||
           __builtin_add_overflow(prev ^ (1ull << 63), span, &span);
}

static inline __attribute__((always_inline)) int dd_dec(const uint8_t *in, size_t len, size_t n, int64_t *out,
                                                        const bool chk)
{
    if (n == 0)
        return 0;
    if (len < 8)
        return TSDB_ERR_CORRUPT;
    uint64_t prev, delta;
    bool bad = false;
    memcpy(&prev, in, 8);
    out[0] = (int64_t)prev;
    if (n == 1)
        return 0;
    if (len < 16)
        return TSDB_ERR_CORRUPT;
    memcpy(&delta, in + 8, 8);
    if (chk)
        bad = ts_run_bad(prev, delta, 1);
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
            if (chk)
                bad |= ts_run_bad(prev, delta, r);
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
        if (chk)
            bad |= (int64_t)(prev + delta) <= (int64_t)prev;
        prev += delta;
        out[i++] = (int64_t)prev;
    }
    return pos <= limit && !bad ? 0 : TSDB_ERR_CORRUPT;
}

int dd_decode(const uint8_t *in, size_t len, size_t n, int64_t *out) { return dd_dec(in, len, n, out, false); }
int dd_decode_sorted(const uint8_t *in, size_t len, size_t n, int64_t *out) { return dd_dec(in, len, n, out, true); }

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

/*
 * Constants, not tables a constructor fills: codec.c is compiled once per CPU
 * level (levels.h), and a constructor in a v3/v4 copy would run on every CPU
 * at startup, before any dispatch, with AVX in it.
 */
#define P2S(b) ((double)(1L << (b)))
#define P2I(b) (1.0 / (double)(1L << (b)))
static const double dec_scale[] = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
                                   P2S(1), P2S(2), P2S(3), P2S(4), P2S(5), P2S(6), P2S(7), P2S(8), P2S(9), P2S(10),
                                   P2S(11), P2S(12), P2S(13), P2S(14), P2S(15), P2S(16), P2S(17), P2S(18), P2S(19), P2S(20),
                                   P2S(21), P2S(22), P2S(23), P2S(24)};
/* the decimal inverses are literals: the decoder multiplies by exactly these */
static const double dec_inv[] = {1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10,
                                 P2I(1), P2I(2), P2I(3), P2I(4), P2I(5), P2I(6), P2I(7), P2I(8), P2I(9), P2I(10),
                                 P2I(11), P2I(12), P2I(13), P2I(14), P2I(15), P2I(16), P2I(17), P2I(18), P2I(19), P2I(20),
                                 P2I(21), P2I(22), P2I(23), P2I(24)};
_Static_assert(sizeof dec_scale / sizeof *dec_scale == DEC_MAX_EXP + 1, "dec_scale");
_Static_assert(sizeof dec_inv / sizeof *dec_inv == DEC_MAX_EXP + 1, "dec_inv");

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

/* Cost in bytes of scale e on a few runs of consecutive values: whole bytes a delta (as `decimal` stores them), or
 * with `bits` the bits bit-packing needs, in bytes. */
static double dec_sample_cost(const float *in, size_t n, int e, bool bits)
{
    enum { RUNS = 4, RUN = 16 };
    size_t run = MIN((size_t)RUN, n), tried = 0, exc = 0;
    size_t max_exc_all = n > run ? (size_t)RUNS * run : run;
    uint64_t maxz = 0;
    for (int r = 0; r < RUNS; r++) {
        size_t at = n > run ? (n - run) * (size_t)r / (RUNS - 1) : 0;
        int64_t prev = 0, k;
        bool have = false;
        for (size_t i = at; i < at + run; i++, tried++) {
            if (!dec_int(in[i], e, &k)) {
                exc++;
                if ((r == 0 && exc * 4 > run) || exc * 4 > max_exc_all)
                    return INFINITY;
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
    double w = !bits ? dec_width(maxz) : maxz >> 32 ? 8.0 : bits_for((uint32_t)maxz) / 8.0;
    return w + 8.0 * (double)exc / (double)tried;
}

/* Picks the best fixed-point scale on the sample (or -1 if none applies). For binary scales 2^1..2^24, a single
 * bit-twiddle pass over the sample computes the exact interval [lo, hi] of valid shifts for each value and skips
 * every scale whose exception count exceeds dec_sample_cost's 25% cap. */
static int dec_pick_scale(const float *in, size_t n, bool compact)
{
    int best_e = -1;
    double best = INFINITY;
    for (int e = 0; e < DEC_NDEC && best > 1.0; e++) { /* 1.0: 1-byte deltas, no exceptions */
        double cost = dec_sample_cost(in, n, e, false);
        if (cost < best - 1e-9) {
            best = cost;
            best_e = e;
        }
    }
    if (best > 1.0) {
        enum { RUNS = 4, RUN = 16 };
        size_t run = MIN((size_t)RUN, n), tried = 0;
        int diff0[DEC_NBIN + 2] = {0}, diff[DEC_NBIN + 2] = {0};
        for (int r = 0; r < RUNS; r++) {
            size_t at = n > run ? (n - run) * (size_t)r / (RUNS - 1) : 0;
            for (size_t i = at; i < at + run; i++, tried++) {
                uint32_t u;
                memcpy(&u, &in[i], 4);
                int lo, hi;
                if (u == 0) {
                    lo = 1;
                    hi = DEC_NBIN;
                } else {
                    uint32_t exp = (u >> 23) & 0xff, mant = u & 0x7fffff;
                    if (exp == 0 || exp == 0xff)
                        continue;
                    lo = 150 - (int)exp - __builtin_ctz(mant | (1u << 23));
                    hi = 178 - (int)exp;
                    if (lo < 1)
                        lo = 1;
                    if (hi > DEC_NBIN)
                        hi = DEC_NBIN;
                    if (lo > hi)
                        continue;
                }
                diff[lo]++;
                diff[hi + 1]--;
                if (r == 0) {
                    diff0[lo]++;
                    diff0[hi + 1]--;
                }
            }
            if (n <= run)
                break;
        }
        int ok0 = 0, ok = 0;
        for (int b = 1; b <= DEC_NBIN && best > 1.0; b++) {
            ok0 += diff0[b];
            ok += diff[b];
            if ((run - (size_t)ok0) * 4 > run || (tried - (size_t)ok) * 4 > tried)
                continue;
            int e = DEC_NDEC - 1 + b;
            double cost = dec_sample_cost(in, n, e, false);
            if (cost < best - 1e-9) {
                best = cost;
                best_e = e;
            }
        }
    }
    if (best_e < 0 || best >= 4.5 || (compact && dec_sample_cost(in, n, best_e, true) >= 2.0))
        return -1;
    return best_e;
}

#ifdef DEC_AVX2
/* 1.5 * 2^52 mantissa trick (matching f64_int4 in codec_typed.c): exact for |d| < 2^51 in round-to-nearest. */
#define DEC_MAGIC_BITS INT64_C(0x4338000000000000)
#define DEC_FAST_LIMIT 2251799813685248.0 /* 2^51 */

static inline __attribute__((always_inline)) bool dec_int4(const float *in, __m256d vsc, __m256d vinv, int64_t *k)
{
    const __m256d magic = _mm256_castsi256_pd(_mm256_set1_epi64x(DEC_MAGIC_BITS));
    __m128 vf = _mm_loadu_ps(in);
    __m256d d = _mm256_mul_pd(_mm256_cvtps_pd(vf), vsc);
    __m256d inr = _mm256_cmp_pd(_mm256_andnot_pd(_mm256_set1_pd(-0.0), d), _mm256_set1_pd(DEC_FAST_LIMIT), _CMP_LT_OQ);
    if (_mm256_movemask_pd(inr) != 15)
        return false;
    __m256d t = _mm256_add_pd(d, magic);
    __m256d kd = _mm256_sub_pd(t, magic); /* exact rounded integer */
    __m128 back = _mm256_cvtpd_ps(_mm256_mul_pd(kd, vinv));
    __m128i same = _mm_cmpeq_epi32(_mm_castps_si128(back), _mm_castps_si128(vf));
    if (_mm_movemask_ps(_mm_castsi128_ps(same)) != 15)
        return false;
    _mm256_storeu_si256((__m256i *)(void *)k,
                        _mm256_sub_epi64(_mm256_castpd_si256(t), _mm256_set1_epi64x(DEC_MAGIC_BITS)));
    return true;
}
#endif

#define DBP_VEC 1024
#define DBP_REL_LIMIT 9007199254740992ll /* 2^53: integers up to it are exact doubles */
_Static_assert(DBP_VEC <= 1024, "zz_prefix32 sums at most 1024 deltas");

/* Converts a chunk of nv values starting at in[base] into zigzag deltas z[0..nv), updating prev, maxz, nexc,
 * exc and (when has_keep) keep[base..base+nv). Always inlined so keep==NULL constant-propagates away. */
static inline __attribute__((always_inline)) bool dec_chunk(const float *in, size_t base, size_t nv, int best_e,
                                                            uint32_t *z, uint8_t *exc, size_t max_exc, size_t *nexc,
                                                            int64_t *prevp, uint64_t *maxzp, uint32_t *allp,
                                                            int64_t *keep, const bool has_keep)
{
    const double sc = dec_scale[best_e], inv = dec_inv[best_e];
    int64_t prev = *prevp;
    uint64_t cmaxz = 0;
    size_t nx = *nexc;
#ifdef DEC_AVX2
    const __m256d vsc = _mm256_set1_pd(sc), vinv = _mm256_set1_pd(inv);
    size_t i = 0;
    while (i < nv) {
        int64_t k4[4];
        if (i + 4 <= nv && dec_int4(&in[base + i], vsc, vinv, k4)) {
            for (size_t j = 0; j < 4; j++, i++) {
                int64_t k = k4[j];
                if (has_keep)
                    keep[base + i] = k;
                uint64_t zz = zigzag(k - prev);
                cmaxz |= zz;
                z[i] = (uint32_t)zz;
                prev = k;
            }
            continue;
        }
        size_t stop = MIN(i + 4, nv);
        for (; i < stop; i++) {
            int64_t k;
            if (!dec_int(in[base + i], best_e, &k)) {
                if (nx == max_exc)
                    return false;
                store32(exc + 8 * nx, (uint32_t)(base + i));
                memcpy(exc + 8 * nx + 4, &in[base + i], 4);
                nx++;
                k = prev; /* a zero delta keeps the stream smooth */
            }
            if (has_keep)
                keep[base + i] = k;
            uint64_t zz = zigzag(k - prev);
            cmaxz |= zz;
            z[i] = (uint32_t)zz;
            prev = k;
        }
    }
#else
    /* two loops over a stack-resident chunk: a branch-free one the compiler vectorizes (integer and
     * exactness of every value), then deltas and exceptions */
    int64_t kk[DBP_VEC];
    uint8_t okv[DBP_VEC];
    const float *src = in + base;
    for (size_t i = 0; i < nv; i++) {
        double d = (double)src[i] * sc;
        bool in_range = (d > -DEC_LIMIT) & (d < DEC_LIMIT); /* false for NaN too */
        int64_t k = (int64_t)__builtin_rint(in_range ? d : 0.0);
        /* rebuild from the integer, as the decoder will: -0.0 must not pass as 0 */
        float back = (float)((double)k * inv);
        uint32_t x, y;
        memcpy(&x, &back, 4);
        memcpy(&y, &src[i], 4);
        okv[i] = (x == y) & in_range;
        kk[i] = k;
    }
    for (size_t i = 0; i < nv; i++) {
        int64_t k = kk[i];
        if (UNLIKELY(!okv[i])) {
            if (nx == max_exc)
                return false;
            store32(exc + 8 * nx, (uint32_t)(base + i));
            memcpy(exc + 8 * nx + 4, &src[i], 4);
            nx++;
            k = prev;
        }
        if (has_keep)
            keep[base + i] = k;
        uint64_t zz = zigzag(k - prev);
        cmaxz |= zz;
        z[i] = (uint32_t)zz;
        prev = k;
    }
#endif
    if (cmaxz >= (1ull << 32))
        return false;
    *prevp = prev;
    *maxzp |= cmaxz;
    *nexc = nx;
    *allp = (uint32_t)cmaxz;
    return true;
}

size_t decimal_encode(const float *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    int best_e = dec_pick_scale(in, n, false);
    if (best_e < 0)
        return 0;
    uint32_t z_stk[8192];
    uint32_t *z = n <= 8192 ? z_stk : malloc(n * sizeof *z);
    uint8_t exc_stk[513 * 8];
    uint8_t *exc = n <= 8192 ? exc_stk : malloc((n / 16 + 1) * 8);
    if (!z || !exc) {
        if (z != z_stk)
            free(z);
        if (exc != exc_stk)
            free(exc);
        return 0;
    }
    size_t max_exc = n / 16, nexc = 0;
    int64_t first = 0;
    bool ok = true;
    if (!dec_int(in[0], best_e, &first)) {
        if (nexc == max_exc)
            ok = false;
        else {
            store32(exc, 0);
            memcpy(exc + 4, &in[0], 4);
            nexc++;
            first = 0;
        }
    }
    int64_t prev = first;
    uint64_t maxz = 0;
    for (size_t v0 = 0; ok && v0 + 1 < n; v0 += DBP_VEC) {
        size_t nv = MIN((size_t)DBP_VEC, n - 1 - v0);
        uint32_t all;
        ok = dec_chunk(in, v0 + 1, nv, best_e, z + v0, exc, max_exc, &nexc, &prev, &maxz, &all, NULL, false);
    }
    if (!ok) {
        if (z != z_stk)
            free(z);
        if (exc != exc_stk)
            free(exc);
        return 0;
    }
    int width = dec_width(maxz);
    out[0] = (uint8_t)best_e;
    out[1] = (uint8_t)width;
    memcpy(out + 2, &first, 8);
    store32(out + 10, (uint32_t)nexc);
    uint8_t *o = out + DEC_HDR;
    if (width == 1) {
        for (size_t i = 0; i + 1 < n; i++)
            o[i] = (uint8_t)z[i];
        o += n - 1;
    } else if (width == 2) {
        for (size_t i = 0; i + 1 < n; i++, o += 2) {
            uint16_t z16 = (uint16_t)z[i];
            memcpy(o, &z16, 2);
        }
    } else {
        memcpy(o, z, (n - 1) * 4);
        o += (n - 1) * 4;
    }
    memcpy(o, exc, nexc * 8);
    o += nexc * 8;
    if (z != z_stk)
        free(z);
    if (exc != exc_stk)
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
size_t decimal_bp_bound(size_t n) { return DEC_HDR + n * 4 + (n / DBP_VEC + 1) * 5 + (n / 16 + 1) * 8 + 16; }

size_t decimal_bp_encode(const float *in, size_t n, uint8_t *out)
{
    size_t nexc;
    return decimal_bp_encode_keep(in, n, out, NULL, &nexc);
}

/* compact: see dec_pick_scale. Encodes in 1024-value stack vectors with zero heap allocations for n <= 8192. */
static size_t dbp_encode(const float *in, size_t n, uint8_t *out, int64_t *keep, size_t *nexcp, bool compact)
{
    if (n < 2)
        return 0;
    int best_e = dec_pick_scale(in, n, compact);
    if (best_e < 0)
        return 0;
    size_t max_exc = n / 16, nexc = 0;
    uint8_t exc_stk[513 * 8];
    uint8_t *exc = n <= 8192 ? exc_stk : malloc((max_exc + 1) * 8);
    if (!exc)
        return 0;
    int64_t first = 0;
    if (!dec_int(in[0], best_e, &first)) {
        if (nexc == max_exc) {
            if (exc != exc_stk)
                free(exc);
            return 0;
        }
        store32(exc, 0);
        memcpy(exc + 4, &in[0], 4);
        nexc++;
        first = 0;
    }
    if (keep)
        keep[0] = first;
    int64_t prev = first;
    uint64_t maxz = 0;
    uint32_t z[DBP_VEC];
    uint8_t *o = out + DEC_HDR;
    for (size_t v0 = 0; v0 + 1 < n; v0 += DBP_VEC) {
        size_t nv = MIN((size_t)DBP_VEC, n - 1 - v0);
        uint32_t all;
        bool ok = keep ? dec_chunk(in, v0 + 1, nv, best_e, z, exc, max_exc, &nexc, &prev, &maxz, &all, keep, true)
                       : dec_chunk(in, v0 + 1, nv, best_e, z, exc, max_exc, &nexc, &prev, &maxz, &all, NULL, false);
        if (!ok) {
            if (exc != exc_stk)
                free(exc);
            return 0;
        }
        int w = bits_for(all);
        *o++ = (uint8_t)w;
        pack(z, nv, w, o);
        o += packed_bytes(nv, w);
    }
    out[0] = (uint8_t)best_e;
    out[1] = 0;
    memcpy(out + 2, &first, 8);
    store32(out + 10, (uint32_t)nexc);
    memcpy(o, exc, nexc * 8);
    o += nexc * 8;
    if (exc != exc_stk)
        free(exc);
    *nexcp = nexc;
    return (size_t)(o - out);
}

size_t decimal_bp_encode_keep(const float *in, size_t n, uint8_t *out, int64_t *keep, size_t *nexc)
{
    return dbp_encode(in, n, out, keep, nexc, false);
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
    /* unsigned, so that a damaged block wraps instead of overflowing (with
     * verify_checksums off the decoder is the first to see damaged bytes) */
    uint64_t k;
    memcpy(&k, in + 2, 8);
    const double inv = dec_inv[e];
    const uint8_t *p = in + DEC_HDR, *end = in + len;
    uint32_t z[DBP_VEC];
    out[0] = (float)((double)(int64_t)k * inv);
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
        int64_t ks = (int64_t)k;
        if (w <= ZZ_PREFIX32_MAX_W && ks >= -DBP_REL_LIMIT && ks <= DBP_REL_LIMIT) {
            /* the sums r relative to k fit an int32 (zz_prefix32); k is exact in a double, so base + r
             * is one rounding of the integer k + r, as (double)(k + r) is: the running sum's bits, with
             * its dependency chain gone and the conversion vectorized */
            uint32_t total = zz_prefix32(z, nv);
            const double base = (double)ks;
            for (size_t i = 0; i < nv; i++)
                o[i] = (float)((base + (double)(int32_t)z[i]) * inv);
            k += (uint64_t)(int64_t)(int32_t)total;
            continue;
        }
        for (size_t i = 0; i < nv; i++) { /* wide deltas, or a damaged first value */
            k += (uint64_t)unzigzag(z[i]);
            o[i] = (float)((double)(int64_t)k * inv);
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
    uint64_t k; /* unsigned: damage wraps (decimal_bp_decode) */
    memcpy(&k, in + 2, 8);
    const double inv = dec_inv[e];
    const uint8_t *p = in + DEC_HDR;
    out[0] = (float)((double)(int64_t)k * inv);
    if (width == 1) {
        for (size_t i = 1; i < n; i++) {
            k += (uint64_t)unzigzag(p[i - 1]);
            out[i] = (float)((double)(int64_t)k * inv);
        }
    } else if (width == 2) {
        for (size_t i = 1; i < n; i++) {
            uint16_t z;
            memcpy(&z, p + 2 * (i - 1), 2);
            k += (uint64_t)unzigzag(z);
            out[i] = (float)((double)(int64_t)k * inv);
        }
    } else {
        for (size_t i = 1; i < n; i++) {
            k += (uint64_t)unzigzag(load32(p + 4 * (i - 1)));
            out[i] = (float)((double)(int64_t)k * inv);
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
    uint8_t *p0 = out, *p1 = out + n, *p2 = out + 2 * n, *p3 = out + 3 * n;
    size_t i = 0;
#if defined(__AVX2__)
    const __m256i shuf = _mm256_setr_epi8(0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15,
                                          0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15);
    for (; i + 32 <= n; i += 32) {
        __m256i s0 = _mm256_shuffle_epi8(_mm256_loadu_si256((const __m256i *)(const void *)(in + i)), shuf);
        __m256i s1 = _mm256_shuffle_epi8(_mm256_loadu_si256((const __m256i *)(const void *)(in + i + 8)), shuf);
        __m256i s2 = _mm256_shuffle_epi8(_mm256_loadu_si256((const __m256i *)(const void *)(in + i + 16)), shuf);
        __m256i s3 = _mm256_shuffle_epi8(_mm256_loadu_si256((const __m256i *)(const void *)(in + i + 24)), shuf);
        __m256i a0 = _mm256_permute2x128_si256(s0, s1, 0x20);
        __m256i a1 = _mm256_permute2x128_si256(s0, s1, 0x31);
        __m256i a2 = _mm256_permute2x128_si256(s2, s3, 0x20);
        __m256i a3 = _mm256_permute2x128_si256(s2, s3, 0x31);
        __m256i b0 = _mm256_unpacklo_epi32(a0, a1);
        __m256i b1 = _mm256_unpackhi_epi32(a0, a1);
        __m256i b2 = _mm256_unpacklo_epi32(a2, a3);
        __m256i b3 = _mm256_unpackhi_epi32(a2, a3);
        _mm256_storeu_si256((__m256i *)(void *)(p0 + i), _mm256_permute4x64_epi64(_mm256_unpacklo_epi64(b0, b2), 0xd8));
        _mm256_storeu_si256((__m256i *)(void *)(p1 + i), _mm256_permute4x64_epi64(_mm256_unpackhi_epi64(b0, b2), 0xd8));
        _mm256_storeu_si256((__m256i *)(void *)(p2 + i), _mm256_permute4x64_epi64(_mm256_unpacklo_epi64(b1, b3), 0xd8));
        _mm256_storeu_si256((__m256i *)(void *)(p3 + i), _mm256_permute4x64_epi64(_mm256_unpackhi_epi64(b1, b3), 0xd8));
    }
#endif
#if defined(__SSE2__)
    for (; i + 16 <= n; i += 16) {
        __m128i v0 = _mm_loadu_si128((const __m128i *)(const void *)(in + i));
        __m128i v1 = _mm_loadu_si128((const __m128i *)(const void *)(in + i + 4));
        __m128i v2 = _mm_loadu_si128((const __m128i *)(const void *)(in + i + 8));
        __m128i v3 = _mm_loadu_si128((const __m128i *)(const void *)(in + i + 12));
        __m128i a0 = _mm_unpacklo_epi8(v0, v2), a1 = _mm_unpackhi_epi8(v0, v2);
        __m128i a2 = _mm_unpacklo_epi8(v1, v3), a3 = _mm_unpackhi_epi8(v1, v3);
        __m128i b0 = _mm_unpacklo_epi8(a0, a2), b1 = _mm_unpackhi_epi8(a0, a2);
        __m128i b2 = _mm_unpacklo_epi8(a1, a3), b3 = _mm_unpackhi_epi8(a1, a3);
        __m128i c0 = _mm_unpacklo_epi8(b0, b2), c1 = _mm_unpackhi_epi8(b0, b2);
        __m128i c2 = _mm_unpacklo_epi8(b1, b3), c3 = _mm_unpackhi_epi8(b1, b3);
        _mm_storeu_si128((__m128i *)(void *)(p0 + i), _mm_unpacklo_epi8(c0, c2));
        _mm_storeu_si128((__m128i *)(void *)(p1 + i), _mm_unpackhi_epi8(c0, c2));
        _mm_storeu_si128((__m128i *)(void *)(p2 + i), _mm_unpacklo_epi8(c1, c3));
        _mm_storeu_si128((__m128i *)(void *)(p3 + i), _mm_unpackhi_epi8(c1, c3));
    }
#endif
    for (; i < n; i++) {
        uint32_t x = in[i];
        p0[i] = (uint8_t)x;
        p1[i] = (uint8_t)(x >> 8);
        p2[i] = (uint8_t)(x >> 16);
        p3[i] = (uint8_t)(x >> 24);
    }
    return 4 * n;
}

int bytesplit_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (len != 4 * n)
        return TSDB_ERR_CORRUPT;
    const uint8_t *p0 = in, *p1 = in + n, *p2 = in + 2 * n, *p3 = in + 3 * n;
    size_t i = 0;
#if defined(__AVX2__)
    for (; i + 32 <= n; i += 32) {
        __m256i r0 = _mm256_loadu_si256((const __m256i *)(const void *)(p0 + i));
        __m256i r1 = _mm256_loadu_si256((const __m256i *)(const void *)(p1 + i));
        __m256i r2 = _mm256_loadu_si256((const __m256i *)(const void *)(p2 + i));
        __m256i r3 = _mm256_loadu_si256((const __m256i *)(const void *)(p3 + i));
        __m256i l01 = _mm256_unpacklo_epi8(r0, r1), h01 = _mm256_unpackhi_epi8(r0, r1);
        __m256i l23 = _mm256_unpacklo_epi8(r2, r3), h23 = _mm256_unpackhi_epi8(r2, r3);
        __m256i a0 = _mm256_unpacklo_epi16(l01, l23), a1 = _mm256_unpackhi_epi16(l01, l23);
        __m256i a2 = _mm256_unpacklo_epi16(h01, h23), a3 = _mm256_unpackhi_epi16(h01, h23);
        _mm256_storeu_si256((__m256i *)(void *)(out + i), _mm256_permute2x128_si256(a0, a1, 0x20));
        _mm256_storeu_si256((__m256i *)(void *)(out + i + 8), _mm256_permute2x128_si256(a2, a3, 0x20));
        _mm256_storeu_si256((__m256i *)(void *)(out + i + 16), _mm256_permute2x128_si256(a0, a1, 0x31));
        _mm256_storeu_si256((__m256i *)(void *)(out + i + 24), _mm256_permute2x128_si256(a2, a3, 0x31));
    }
#endif
#if defined(__SSE2__)
    for (; i + 16 <= n; i += 16) {
        __m128i r0 = _mm_loadu_si128((const __m128i *)(const void *)(p0 + i));
        __m128i r1 = _mm_loadu_si128((const __m128i *)(const void *)(p1 + i));
        __m128i r2 = _mm_loadu_si128((const __m128i *)(const void *)(p2 + i));
        __m128i r3 = _mm_loadu_si128((const __m128i *)(const void *)(p3 + i));
        __m128i l01 = _mm_unpacklo_epi8(r0, r1), h01 = _mm_unpackhi_epi8(r0, r1);
        __m128i l23 = _mm_unpacklo_epi8(r2, r3), h23 = _mm_unpackhi_epi8(r2, r3);
        _mm_storeu_si128((__m128i *)(void *)(out + i), _mm_unpacklo_epi16(l01, l23));
        _mm_storeu_si128((__m128i *)(void *)(out + i + 4), _mm_unpackhi_epi16(l01, l23));
        _mm_storeu_si128((__m128i *)(void *)(out + i + 8), _mm_unpacklo_epi16(h01, h23));
        _mm_storeu_si128((__m128i *)(void *)(out + i + 12), _mm_unpackhi_epi16(h01, h23));
    }
#endif
    for (; i < n; i++)
        out[i] = (uint32_t)p0[i] | (uint32_t)p1[i] << 8 | (uint32_t)p2[i] << 16 | (uint32_t)p3[i] << 24;
    return 0;
}

/*
 * Packed bit delta (BLK_DELTA_BP): zigzag deltas of the raw bit patterns,
 * bit-packed per vector of 1024 like decimal_bp's integers. For values that
 * are not decimals but move smoothly (a reading converted to other units and
 * left unrounded): neighbours share sign, exponent and the top of the
 * mantissa, so their difference is narrow where XOR (Chimp) still pays for a
 * window of changed bits on every value. delta32 is the same difference left
 * whole for ZSTD; packing to the vector's width does better and decodes
 * without an entropy decoder. Values that are not smooth widen the vectors
 * to 32 bits and lose: AUTO compares it with the others.
 *
 *   u32 first | per vector of up to 1024 deltas: u8 bits (0..32), packed zigzags
 */
#define DBP32_VEC 1024

size_t delta_bp32_bound(size_t n) { return 4 + (n / DBP32_VEC + 1) + 4 * n + 4; }

size_t delta_bp32_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    store32(out, in[0]);
    uint8_t *o = out + 4;
    uint32_t z[DBP32_VEC];
    for (size_t v0 = 1; v0 < n; v0 += DBP32_VEC) {
        size_t nv = MIN((size_t)DBP32_VEC, n - v0);
        uint32_t all = 0;
        for (size_t i = 0; i < nv; i++) {
            uint32_t d = in[v0 + i] - in[v0 + i - 1]; /* wraps; the decoder's sum wraps back */
            z[i] = (d << 1) ^ (uint32_t)((int32_t)d >> 31);
            all |= z[i];
        }
        int w = bits_for(all);
        *o++ = (uint8_t)w;
        pack(z, nv, w, o);
        o += packed_bytes(nv, w);
    }
    return (size_t)(o - out);
}

int delta_bp32_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (n == 0)
        return 0;
    if (len < 4)
        return TSDB_ERR_CORRUPT;
    uint32_t prev = load32(in);
    out[0] = prev;
    const uint8_t *p = in + 4, *end = in + len;
    uint32_t z[DBP32_VEC];
    for (size_t v0 = 1; v0 < n; v0 += DBP32_VEC) {
        size_t nv = MIN((size_t)DBP32_VEC, n - v0);
        if (p >= end || *p > 32)
            return TSDB_ERR_CORRUPT;
        int w = *p++;
        size_t pb = packed_bytes(nv, w);
        if ((size_t)(end - p) < pb)
            return TSDB_ERR_CORRUPT;
        unpack(p, nv, w, z);
        p += pb;
        uint32_t total = zz_prefix32(z, nv); /* modulo 2^32, as the encoder's differences */
        for (size_t i = 0; i < nv; i++)
            out[v0 + i] = prev + z[i];
        prev += total;
    }
    return p == end ? 0 : TSDB_ERR_CORRUPT;
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
    uint64_t base = first + (uint64_t)lo;
    if (width == 1) {
        for (size_t i = 0; i < n; i++)
            o[i] = (uint8_t)((uint64_t)ts[i] - base - (uint64_t)i * step);
    } else if (width == 2) {
        for (size_t i = 0; i < n; i++) {
            uint16_t y = (uint16_t)((uint64_t)ts[i] - base - (uint64_t)i * step);
            memcpy(o + 2 * i, &y, 2);
        }
    } else if (width == 4) {
        for (size_t i = 0; i < n; i++)
            store32(o + 4 * i, (uint32_t)((uint64_t)ts[i] - base - (uint64_t)i * step));
    } else {
        for (size_t i = 0; i < n; i++) {
            uint64_t y = (uint64_t)ts[i] - base - (uint64_t)i * step;
            memcpy(o + 8 * i, &y, 8);
        }
    }
    return 25 + n * (size_t)width;
}

static inline __attribute__((always_inline)) int ts_grid_dec(const uint8_t *in, size_t len, size_t n, int64_t *out,
                                                             const bool chk)
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
    uint64_t base = first + lo, bad = 0;
    if (width == 1) {
        for (size_t i = 0; i < n; i++)
            out[i] = (int64_t)(base + (uint64_t)i * step + p[i]);
    } else if (width == 2) {
        for (size_t i = 0; i < n; i++) {
            uint16_t y;
            memcpy(&y, p + 2 * i, 2);
            out[i] = (int64_t)(base + (uint64_t)i * step + y);
        }
    } else if (width == 4) {
        for (size_t i = 0; i < n; i++)
            out[i] = (int64_t)(base + (uint64_t)i * step + load32(p + 4 * i));
    } else {
        for (size_t i = 0; i < n; i++) {
            uint64_t y;
            memcpy(&y, p + 8 * i, 8);
            out[i] = (int64_t)(base + (uint64_t)i * step + y);
        }
    }
    /* a separate pass over the output, which is in L1: it vectorizes, and costs a third of rebuilding each pair from
     * the input (0.07 against 0.22 ns per row on 1 s with 20 ms jitter, Ryzen 9 7950X) */
    if (chk)
        for (size_t i = 1; i < n; i++)
            bad |= (uint64_t)(out[i] <= out[i - 1]);
    return bad ? TSDB_ERR_CORRUPT : 0;
}

int ts_grid_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    return ts_grid_dec(in, len, n, out, false);
}

int ts_grid_decode_sorted(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    return ts_grid_dec(in, len, n, out, true);
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
    if (width == 1) {
        for (size_t i = 1; i < n; i++)
            o[i - 1] = (uint8_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1] - (uint64_t)lo);
    } else if (width == 2) {
        for (size_t i = 1; i < n; i++) {
            uint16_t y = (uint16_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1] - (uint64_t)lo);
            memcpy(o + 2 * (i - 1), &y, 2);
        }
    } else if (width == 4) {
        for (size_t i = 1; i < n; i++)
            store32(o + 4 * (i - 1), (uint32_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1] - (uint64_t)lo));
    } else {
        for (size_t i = 1; i < n; i++) {
            uint64_t y = (uint64_t)ts[i] - (uint64_t)ts[i - 1] - (uint64_t)lo;
            memcpy(o + 8 * (i - 1), &y, 8);
        }
    }
    return 17 + (n - 1) * (size_t)width;
}

#if defined(__AVX2__)
#define TS_DELTA_PREFIX4(LOAD_EXPR)                                                                                \
    do {                                                                                                           \
        const __m256i vlo = _mm256_set1_epi64x((long long)lo), zero = _mm256_setzero_si256();                      \
        __m256i carry = _mm256_set1_epi64x((long long)cur);                                                        \
        for (; i + 4 <= n; i += 4) {                                                                               \
            __m256i x = _mm256_add_epi64((LOAD_EXPR), vlo);                                                        \
            x = _mm256_add_epi64(x, _mm256_slli_si256(x, 8));                                                      \
            __m128i lo128 = _mm256_castsi256_si128(x);                                                             \
            x = _mm256_add_epi64(x, _mm256_inserti128_si256(zero, _mm_unpackhi_epi64(lo128, lo128), 1));           \
            __m256i tot = _mm256_permute4x64_epi64(x, 0xff);                                                       \
            _mm256_storeu_si256((__m256i *)(void *)(out + i), _mm256_add_epi64(x, carry));                         \
            carry = _mm256_add_epi64(carry, tot);                                                                  \
        }                                                                                                          \
        cur = (uint64_t)_mm_cvtsi128_si64(_mm256_castsi256_si128(carry));                                          \
    } while (0)
#elif defined(__SSE2__)
#define TS_DELTA_PREFIX2(LOAD_EXPR)                                                                                \
    do {                                                                                                           \
        const __m128i vlo = _mm_set1_epi64x((long long)lo);                                                        \
        __m128i carry = _mm_set1_epi64x((long long)cur);                                                           \
        for (; i + 2 <= n; i += 2) {                                                                               \
            __m128i x = _mm_add_epi64((LOAD_EXPR), vlo);                                                           \
            x = _mm_add_epi64(x, _mm_slli_si128(x, 8));                                                            \
            x = _mm_add_epi64(x, carry);                                                                           \
            _mm_storeu_si128((__m128i *)(void *)(out + i), x);                                                     \
            carry = _mm_unpackhi_epi64(x, x);                                                                      \
        }                                                                                                          \
        cur = (uint64_t)_mm_cvtsi128_si64(carry);                                                                  \
    } while (0)
#endif

static inline __attribute__((always_inline)) int ts_delta_dec(const uint8_t *in, size_t len, size_t n,
                                                              int64_t *out, const bool chk)
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
    uint64_t bad = 0;
    size_t i = 1;
    if (width == 1) {
#if defined(__AVX2__)
        TS_DELTA_PREFIX4(_mm256_cvtepu8_epi64(_mm_cvtsi32_si128((int)load32(p + i - 1))));
#elif defined(__SSE2__)
        TS_DELTA_PREFIX2(_mm_set_epi64x(p[i], p[i - 1]));
#endif
        for (; i < n; i++) {
            cur += lo + p[i - 1];
            out[i] = (int64_t)cur;
        }
    } else if (width == 2) {
#if defined(__AVX2__)
        TS_DELTA_PREFIX4(_mm256_cvtepu16_epi64(_mm_cvtsi64_si128((long long)rd64(p + 2 * (i - 1)))));
#elif defined(__SSE2__)
        TS_DELTA_PREFIX2(({
            uint32_t u32_ = load32(p + 2 * (i - 1));
            _mm_set_epi64x((long long)(u32_ >> 16), (long long)(u32_ & 0xffffu));
        }));
#endif
        for (; i < n; i++) {
            uint16_t y;
            memcpy(&y, p + 2 * (i - 1), 2);
            cur += lo + y;
            out[i] = (int64_t)cur;
        }
    } else if (width == 4) {
#if defined(__AVX2__)
        TS_DELTA_PREFIX4(_mm256_cvtepu32_epi64(_mm_loadu_si128((const __m128i *)(const void *)(p + 4 * (i - 1)))));
#elif defined(__SSE2__)
        TS_DELTA_PREFIX2(({
            uint64_t u64_ = rd64(p + 4 * (i - 1));
            _mm_set_epi64x((long long)(u64_ >> 32), (long long)(uint32_t)u64_);
        }));
#endif
        for (; i < n; i++) {
            cur += lo + load32(p + 4 * (i - 1));
            out[i] = (int64_t)cur;
        }
    } else {
#if defined(__AVX2__)
        TS_DELTA_PREFIX4(_mm256_loadu_si256((const __m256i *)(const void *)(p + 8 * (i - 1))));
#elif defined(__SSE2__)
        TS_DELTA_PREFIX2(_mm_loadu_si128((const __m128i *)(const void *)(p + 8 * (i - 1))));
#endif
        for (; i < n; i++) {
            cur += lo + rd64(p + 8 * (i - 1));
            out[i] = (int64_t)cur;
        }
    }
    /* a separate pass, not a compare beside the serial sum: that one cost 0.39 ns per row on an i5-13400 (0.03 on
     * a Ryzen), this vectorized one 0.16 (0.07) */
    if (chk)
        for (size_t j = 1; j < n; j++)
            bad |= (uint64_t)(out[j] <= out[j - 1]);
    return bad ? TSDB_ERR_CORRUPT : 0;
}

int ts_delta_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    return ts_delta_dec(in, len, n, out, false);
}

int ts_delta_decode_sorted(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    return ts_delta_dec(in, len, n, out, true);
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
    if (!c->cctx || !c->dctx || ZSTD_isError(ZSTD_CCtx_setParameter(c->cctx, ZSTD_c_format, ZSTD_f_zstd1_magicless)) ||
        ZSTD_isError(ZSTD_CCtx_setParameter(c->cctx, ZSTD_c_contentSizeFlag, 0)) ||
        ZSTD_isError(ZSTD_DCtx_setParameter(c->dctx, ZSTD_d_format, ZSTD_f_zstd1_magicless))) {
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
    free(c->work);
    free(c->narrow);
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
    case BLK_ALPRD: return MAX(alprd_bound(n), alprd64_bound(n)); /* by type: float32 or float64 */
    case BLK_DECIMAL_BP: return decimal_bp_bound(n);
    case BLK_F64_DECIMAL: return f64_decimal_bound(n);
    case BLK_XOR64: return xor64_bound(n);
    case BLK_BYTESPLIT64: return 8 * n;
    case BLK_INT: return int_bp_bound(n);
    case BLK_DELTA_BP: return MAX(delta_bp32_bound(n), delta_bp64_bound(n));
    case BLK_DICT: return dict_bound(n);
    default: return 0;
    }
}

size_t block_bound(size_t n)
{
    size_t inner = MAX(MAX(MAX(dd_bound(n), tsfor_bound(n)), chimp_bound(n)), MAX(gorilla_bound(n), decimal_bound(n)));
    inner = MAX(inner, MAX(alprd_bound(n), decimal_bp_bound(n)));
    inner = MAX(inner, MAX(MAX(f64_decimal_bound(n), xor64_bound(n)), MAX(8 * n, int_bp_bound(n))));
    inner = MAX(inner, MAX(alprd64_bound(n), MAX(delta_bp32_bound(n), delta_bp64_bound(n))));
    inner = MAX(inner, dict_bound(n));
    return 5 + ZSTD_compressBound(inner);
}

int codec_alt_reserve(codec_ctx *c, size_t need)
{
    if (c->alt_cap >= need)
        return 0;
    uint8_t *a = realloc(c->alt, need);
    if (!a)
        return TSDB_ERR_NOMEM;
    c->alt = a;
    c->alt_cap = need;
    return 0;
}

int codec_work_reserve(codec_ctx *c, size_t need)
{
    if (c->work_cap >= need)
        return 0;
    uint8_t *w = realloc(c->work, need);
    if (!w)
        return TSDB_ERR_NOMEM;
    c->work = w;
    c->work_cap = need;
    return 0;
}

/* Wraps `inner` bytes of transform output (in c->scratch) into a block. */
size_t block_wrap(codec_ctx *c, int level, int kind, size_t inner, uint8_t *out)
{
    return block_wrap_src(c, level, kind, c->scratch, inner, out);
}

/* As block_wrap, of the transform output at src (not overlapping out). */
size_t block_wrap_src(codec_ctx *c, int level, int kind, const uint8_t *src, size_t inner, uint8_t *out)
{
    if (level > 0 && inner >= 64) {
        size_t z = zstd_pack(c->cctx, out + 5, ZSTD_compressBound(inner), src, inner, level);
        if (!ZSTD_isError(z) && z + 4 < inner - blk_zstd_min_saving(kind, inner)) {
            out[0] = (uint8_t)(kind | BLK_ZSTD);
            store32(out + 1, (uint32_t)inner);
            return 5 + z;
        }
    }
    out[0] = (uint8_t)kind;
    memcpy(out + 1, src, inner);
    return 1 + inner;
}

static double entropy_bytes(const uint8_t *p, size_t len);
static double entropy_words(const uint8_t *p, size_t len, size_t width);

static size_t encode_ts_kind(int kind, const int64_t *ts, size_t n, uint8_t *dst)
{
    switch (kind) {
    case BLK_TS_GRID: return ts_grid_encode(ts, n, dst);
    case BLK_TS_DELTA: return ts_delta_encode(ts, n, dst);
    default: return dd_encode(ts, n, dst);
    }
}

/* DoubleDelta when the clock is regular (about a bit per row before ZSTD);
 * otherwise the candidate with the smallest estimated size (kept in scratch/alt without re-encoding). */
size_t block_encode_ts(codec_ctx *c, int level, const int64_t *ts, size_t n, uint8_t *out)
{
    size_t need = MAX(dd_bound(n), tsfor_bound(n));
    if (codec_scratch_reserve(c, need) != 0)
        return 0;
    size_t dd = dd_encode(ts, n, c->scratch);
    if (level <= 0 || n < 2 || dd <= 24 + n / 8 + 8)
        return block_wrap(c, level, BLK_DD, dd, out);
    if (codec_alt_reserve(c, need) != 0)
        return 0;
    int best = BLK_DD;
    size_t best_len = dd;
    uint8_t *best_buf = c->scratch, *try_buf = c->alt;
    double best_est = MIN((double)dd, entropy_bytes(best_buf, dd));
    static const int kinds[] = {BLK_TS_GRID, BLK_TS_DELTA};
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++) {
        size_t len = encode_ts_kind(kinds[k], ts, n, try_buf);
        size_t hdr = kinds[k] == BLK_TS_GRID ? 25 : 17;
        double est = len > hdr ? MIN((double)len, hdr + entropy_words(try_buf + hdr, len - hdr, try_buf[0]))
                               : (double)len;
        if (len && est < best_est) {
            best_est = est;
            best = kinds[k];
            best_len = len;
            uint8_t *tmp = best_buf;
            best_buf = try_buf;
            try_buf = tmp;
        }
    }
    return block_wrap_src(c, level, best, best_buf, best_len, out);
}

/* Encodes v with one transform into out; returns 0 if not applicable. */
static size_t encode_f32_kind(int kind, const void *v, size_t n, uint8_t *out)
{
    switch (kind) {
    case BLK_GORILLA: return gorilla_encode(v, n, out);
    case BLK_CHIMP: return chimp_encode(v, n, out);
    case BLK_DECIMAL: return decimal_encode(v, n, out);
    case BLK_DELTA32: return delta32_encode(v, n, out);
    case BLK_BYTESPLIT: return bytesplit_encode(v, n, out);
    case BLK_ALPRD: return alprd_encode(v, n, out);
    case BLK_DECIMAL_BP: return decimal_bp_encode(v, n, out);
    case BLK_DELTA_BP: return delta_bp32_encode(v, n, out);
    case BLK_DICT: return dict_encode(v, n, 4, out);
    default: return 0;
    }
}

/*
 * log2 by plain arithmetic, for the entropy estimate that picks codecs. libm's
 * log2 is an IFUNC with an FMA variant on newer CPUs, so the same binary could
 * round it differently on two machines and, in a near tie, write other bytes.
 * Only + - * / here, which round the same at every level (-ffp-contract=off):
 * log2(m * 2^e) = e + 2/ln 2 * atanh(s), s = (m - 1) / (m + 1), |s| < 0.172,
 * whose series is far below double precision after 11 terms. x > 0, normal.
 */
static double log2_portable(double x)
{
    uint64_t b;
    memcpy(&b, &x, 8);
    int e = (int)(b >> 52 & 0x7ff) - 1023;
    b = (b & 0x000fffffffffffffull) | 0x3ff0000000000000ull;
    double m;
    memcpy(&m, &b, 8); /* [1, 2) */
    if (m > 1.4142135623730951) {
        m *= 0.5;
        e++;
    }
    double s = (m - 1) / (m + 1), s2 = s * s, t = 1.0 / 21;
    for (int k = 19; k >= 1; k -= 2)
        t = t * s2 + 1.0 / k;
    return e + s * t * 2.8853900817779268; /* 2 / ln 2 */
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
            bits -= c * log2_portable(c / n);
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
 *  - anything else: Chimp, bit delta (whole, for ZSTD, or packed), byte-stream
 *    split or ALP_rd, ranked by compressing a sample of each (auto_pick,
 *    codec_typed.c; docs/design.md, "How AUTO picks").
 */
/*
 * Unlike float64, float32 skips auto_pick's two ways of encoding every
 * candidate in full: on the pressures of a real hydraulic rig converted to
 * psi, full blocks came out more than 10% below their sample's estimate in
 * most granules (values repeat across the whole block), yet encoding every
 * candidate changed the sizes by under 0.1% and halved the encode speed;
 * encoding all below 12 bits a value saved 1-2% at 20-40% of the speed.
 */
#define F32_ALL_BITS 0
#define F32_SURPRISE 0
static __attribute__((noinline)) size_t block_encode_f32_nearest(codec_ctx *c, int level, int value_codec,
                                                                  const float *v, size_t n, uint8_t *out)
{
    size_t need = MAX(MAX(chimp_bound(n), gorilla_bound(n)), MAX(alprd_bound(n), decimal_bp_bound(n)));
    need = MAX(need, MAX(delta_bp32_bound(n), dict_bound(n)));
    if (codec_scratch_reserve(c, need) != 0)
        return 0;
    if (value_codec == TSDB_VALUES_GORILLA || value_codec == TSDB_VALUES_CHIMP) {
        int kind = value_codec == TSDB_VALUES_GORILLA ? BLK_GORILLA : BLK_CHIMP;
        return block_wrap(c, level, kind, encode_f32_kind(kind, v, n, c->scratch), out);
    }
    bool merge = c->effort == CODEC_MERGE, fixed_only = value_codec == VALUES_FIXED_ONLY;
    int64_t *q = merge && codec_work_reserve(c, DICT_WORK(n)) == 0 ? (int64_t *)(void *)c->work : NULL;
    size_t nexc = 0, inner = dbp_encode(v, n, c->scratch, q, &nexc, fixed_only);
    if (inner && inner * 8 <= 16 * n) /* compact fixed point: only a dictionary comes close */
        return merge ? fixed_or_dict(c, level, BLK_DECIMAL_BP, inner, q, nexc, v, n, 4, out)
                     : block_wrap(c, level, BLK_DECIMAL_BP, inner, out);
    if (fixed_only)
        return 0;
    /* every float32 is "decimal" at ~8 digits; then it is just one more candidate */
    static const int kinds[] = {BLK_DECIMAL_BP, BLK_CHIMP, BLK_DELTA32, BLK_BYTESPLIT, BLK_ALPRD, BLK_DELTA_BP};
    return auto_pick(c, level, kinds, sizeof kinds / sizeof *kinds, encode_f32_kind, v, n, 4, BLK_DECIMAL_BP, inner,
                     merge ? dict_block(c, level, v, n, 4, out) : 0, F32_ALL_BITS, F32_SURPRISE, out);
}

/* run with round-to-nearest, the caller's mode put back (fpmode.h) */
size_t block_encode_f32(codec_ctx *c, int level, int value_codec, const float *v, size_t n, uint8_t *out)
{
    fp_mode m_ = fp_nearest();
    size_t r_ = block_encode_f32_nearest(c, level, value_codec, v, n, out);
    fp_restore(m_);
    return r_;
}

/* Resolves a block to its transform kind and (decompressed) transform bytes. */
int block_open(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int *kind, const uint8_t **p,
               size_t *plen)
{
    if (len < 1)
        return TSDB_ERR_CORRUPT;
    *kind = blk[0] & BLK_KIND_MASK;
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

static inline __attribute__((always_inline)) int block_dec_ts(codec_ctx *c, const uint8_t *blk, size_t len, size_t n,
                                                              int64_t *out, const bool chk)
{
    int kind, rc;
    const uint8_t *p;
    size_t plen;
    if ((rc = block_open(c, blk, len, n, &kind, &p, &plen)) != 0)
        return rc;
    switch (kind) {
    case BLK_DD: return dd_dec(p, plen, n, out, chk);
    case BLK_TS_GRID: return ts_grid_dec(p, plen, n, out, chk);
    case BLK_TS_DELTA: return ts_delta_dec(p, plen, n, out, chk);
    default: return TSDB_ERR_CORRUPT;
    }
}

int block_decode_ts(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int64_t *out)
{
    return block_dec_ts(c, blk, len, n, out, false);
}

/* also TSDB_ERR_CORRUPT unless the values strictly increase (see dd_dec) */
int block_decode_ts_sorted(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int64_t *out)
{
    return block_dec_ts(c, blk, len, n, out, true);
}

static __attribute__((noinline)) int block_decode_f32_nearest(codec_ctx *c, const uint8_t *blk, size_t len,
                                                                size_t n, float *out)
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
    case BLK_DELTA_BP: return delta_bp32_decode(p, plen, n, (uint32_t *)out);
    case BLK_DICT: return dict_decode(p, plen, n, 4, out);
    default: return TSDB_ERR_CORRUPT;
    }
}

/* run with round-to-nearest, the caller's mode put back (fpmode.h) */
int block_decode_f32(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, float *out)
{
    fp_mode m_ = fp_nearest();
    int r_ = block_decode_f32_nearest(c, blk, len, n, out);
    fp_restore(m_);
    return r_;
}
