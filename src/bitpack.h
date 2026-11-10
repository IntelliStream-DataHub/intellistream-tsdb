/* SPDX-License-Identifier: AGPL-3.0-only */
/* Bit packing shared by the ALP-style codecs: values of w bits, LSB first. */
#ifndef TSDB_BITPACK_H
#define TSDB_BITPACK_H

#include <stdint.h>
#include <string.h>
#if defined(__SSE2__)
#include <emmintrin.h> /* x86-64 baseline: every CPU level has it */
#endif
/* AVX2 unpack (below): the v3 level, and -march=native on CPUs without AVX-512BW. The v4 level keeps the
 * portable loops: it has not been measured against them (no AVX-512 machine at the time of writing).
 * -DTSDB_UNPACK_AVX2 takes the kernel at v4 too, to measure it there (tools/decode-bench.c, tests/test_levels.c). */
#if defined(__AVX2__) && (!defined(__AVX512BW__) || defined(TSDB_UNPACK_AVX2))
#define BITPACK_AVX2 1
#include <immintrin.h>
#endif


static inline int bits_for(uint32_t x) { return x ? 32 - __builtin_clz(x) : 0; }

static inline size_t packed_bytes(size_t n, int w) { return (n * (size_t)w + 7) / 8; }

/* Streams values of w bits into out, LSB first; writes packed_bytes(n, w)
 * bytes plus up to 3 of padding. */
static inline void pack(const uint32_t *in, size_t n, int w, uint8_t *out)
{
    if (!w)
        return;
    uint64_t acc = 0;
    int bits = 0;
    for (size_t i = 0; i < n; i++) {
        acc |= (uint64_t)in[i] << bits; /* bits < 32 and w <= 32: fits */
        bits += w;
        if (bits >= 32) {
            uint32_t word = (uint32_t)acc;
            memcpy(out, &word, 4);
            out += 4;
            acc >>= 32;
            bits -= 32;
        }
    }
    if (bits > 0) {
        uint32_t word = (uint32_t)acc;
        memcpy(out, &word, 4);
    }
}

/* One 8-byte load per value, w a constant. GCC does not vectorize this (the -O3 assembly is one value per
 * iteration at every CPU level); SIMD unpacking is on the roadmap. */
#define UNPACK_W(W)                                                                     \
    static inline void unpack_##W(const uint8_t *in, size_t n, uint32_t *out)                 \
    {                                                                                   \
        const uint64_t mask = (W) == 32 ? 0xffffffffull : ((1ull << (W)) - 1);          \
        for (size_t i = 0; i < n; i++) {                                                \
            uint64_t bit = (uint64_t)i * (W), x;                                        \
            memcpy(&x, in + (bit >> 3), 8);                                             \
            out[i] = (uint32_t)((x >> (bit & 7)) & mask);                               \
        }                                                                               \
    }
UNPACK_W(1) UNPACK_W(2) UNPACK_W(3) UNPACK_W(4) UNPACK_W(5) UNPACK_W(6) UNPACK_W(7) UNPACK_W(8)
UNPACK_W(9) UNPACK_W(10) UNPACK_W(11) UNPACK_W(12) UNPACK_W(13) UNPACK_W(14) UNPACK_W(15) UNPACK_W(16)
UNPACK_W(17) UNPACK_W(18) UNPACK_W(19) UNPACK_W(20) UNPACK_W(21) UNPACK_W(22) UNPACK_W(23) UNPACK_W(24)
UNPACK_W(25) UNPACK_W(26) UNPACK_W(27) UNPACK_W(28) UNPACK_W(29) UNPACK_W(30) UNPACK_W(31) UNPACK_W(32)

static void (*const unpack_fn[33])(const uint8_t *, size_t, uint32_t *) = {
    NULL,       unpack_1,  unpack_2,  unpack_3,  unpack_4,  unpack_5,  unpack_6,  unpack_7,  unpack_8,
    unpack_9,   unpack_10, unpack_11, unpack_12, unpack_13, unpack_14, unpack_15, unpack_16, unpack_17,
    unpack_18,  unpack_19, unpack_20, unpack_21, unpack_22, unpack_23, unpack_24, unpack_25, unpack_26,
    unpack_27,  unpack_28, unpack_29, unpack_30, unpack_31, unpack_32};

#ifdef BITPACK_AVX2
/* Eight values per step, w bytes of the stream, for w = 1..25: a 128-bit load for each half (the second at byte
 * w / 2, the bit offset w odd leaves in the second half is in its shifts), PSHUFB moves the four bytes each value
 * starts in into its 32-bit lane, a variable shift drops the bits below it and a mask the bits above (w + 7 <= 32,
 * so a lane holds the whole value). Neither PDEP nor any CPU-specific choice: on a Comet Lake core it decodes
 * 13-15 G values/s against 1.7-2.4 for the loops above, and PDEP (one 64-bit result per 8 values at best, w <= 8)
 * is no faster beyond w = 8 and slow before Zen 3. The tables are constants (no startup code, see levels.h):
 * lane k starts at bit k * w, or (k - 4) * w + 4 * (w & 1) from the second load. Reads up to 16 - ceil(w / 2)
 * bytes (at most 15) past the packed bytes, the loops above up to 7 (BLK_SLACK covers both), and writes exactly
 * n values. */
#define BP_R(W, K) ((K) < 4 ? (K) * (W) : ((K) - 4) * (W) + 4 * ((W) & 1))
#define BP_B(W, K, J) ((uint8_t)((BP_R(W, K) >> 3) + (J)))
#define BP_C(W, K) BP_B(W, K, 0), BP_B(W, K, 1), BP_B(W, K, 2), BP_B(W, K, 3)
#define BP_CTL(W) {BP_C(W, 0), BP_C(W, 1), BP_C(W, 2), BP_C(W, 3), BP_C(W, 4), BP_C(W, 5), BP_C(W, 6), BP_C(W, 7)}
#define BP_S(W, K) ((uint32_t)(BP_R(W, K) & 7))
#define BP_SHR(W) {BP_S(W, 0), BP_S(W, 1), BP_S(W, 2), BP_S(W, 3), BP_S(W, 4), BP_S(W, 5), BP_S(W, 6), BP_S(W, 7)}
#define BP_ROWS(M) {{0}, M(1), M(2), M(3), M(4), M(5), M(6), M(7), M(8), M(9), M(10), M(11), M(12), M(13), \
                    M(14), M(15), M(16), M(17), M(18), M(19), M(20), M(21), M(22), M(23), M(24), M(25)}
#define BITPACK_AVX2_MAX_W 25
static const uint8_t bp_ctl[BITPACK_AVX2_MAX_W + 1][32] = BP_ROWS(BP_CTL);
static const uint32_t bp_shr[BITPACK_AVX2_MAX_W + 1][8] = BP_ROWS(BP_SHR);

static inline void unpack_avx2(const uint8_t *in, size_t n, int w, uint32_t *out)
{
    const __m256i ctl = _mm256_loadu_si256((const __m256i *)(const void *)bp_ctl[w]);
    const __m256i shr = _mm256_loadu_si256((const __m256i *)(const void *)bp_shr[w]);
    const __m256i mask = _mm256_set1_epi32((int)((1u << w) - 1));
    const size_t hi = (size_t)w >> 1;
    size_t i = 0;
    for (; i + 8 <= n; i += 8, in += w) {
        __m256i x = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128((const __m128i *)(const void *)in)),
                                            _mm_loadu_si128((const __m128i *)(const void *)(in + hi)), 1);
        x = _mm256_and_si256(_mm256_srlv_epi32(_mm256_shuffle_epi8(x, ctl), shr), mask);
        _mm256_storeu_si256((__m256i *)(void *)(out + i), x);
    }
    for (; i < n; i++) { /* fewer than eight left: i & 7 values into the group that `in` points at */
        uint64_t bit = (uint64_t)(i & 7) * (uint64_t)w, x;
        memcpy(&x, in + (bit >> 3), 8);
        out[i] = (uint32_t)((x >> (bit & 7)) & ((1ull << w) - 1));
    }
}
#endif

static inline void unpack(const uint8_t *in, size_t n, int w, uint32_t *out)
{
    if (w == 0)
        memset(out, 0, n * sizeof *out);
#ifdef BITPACK_AVX2
    else if (w <= BITPACK_AVX2_MAX_W && n >= 8)
        unpack_avx2(in, n, w, out);
#endif
    else
        unpack_fn[w](in, n, out);
}

/* Values of w bits, w up to 64, in the same LSB-first stream as pack(); writes
 * packed_bytes(n, w) bytes plus up to 7 of padding. */
static inline void pack64(const uint64_t *in, size_t n, int w, uint8_t *out)
{
    if (!w)
        return;
    uint64_t acc = 0;
    int bits = 0;
    for (size_t i = 0; i < n; i++) {
        uint64_t v = in[i];
        acc |= v << bits; /* bits < 64 */
        bits += w;
        if (bits >= 64) {
            memcpy(out, &acc, 8);
            out += 8;
            bits -= 64;
            acc = bits ? v >> (w - bits) : 0; /* the part of v that did not fit; w - bits is 1..63 */
        }
    }
    if (bits > 0)
        memcpy(out, &acc, 8);
}

/* One 8-byte load per value, and a ninth byte when the value straddles it
 * (only possible beyond 57 bits). Reads up to 7 bytes past the packed data. */
#define UNPACK64_W(W)                                                                   \
    static inline void unpack64_##W(const uint8_t *in, size_t n, uint64_t *out)         \
    {                                                                                   \
        const uint64_t mask = (W) == 64 ? ~0ull : ((1ull << ((W) & 63)) - 1);          \
        for (size_t i = 0; i < n; i++) {                                                \
            uint64_t bit = (uint64_t)i * (W), x;                                        \
            unsigned s = (unsigned)(bit & 7);                                           \
            const uint8_t *p = in + (bit >> 3);                                         \
            memcpy(&x, p, 8);                                                           \
            x >>= s;                                                                    \
            if ((W) > 57 && (W) + s > 64)                                               \
                x |= (uint64_t)p[8] << ((64 - s) & 63);                                 \
            out[i] = x & mask;                                                          \
        }                                                                               \
    }
UNPACK64_W(1) UNPACK64_W(2) UNPACK64_W(3) UNPACK64_W(4) UNPACK64_W(5) UNPACK64_W(6) UNPACK64_W(7) UNPACK64_W(8)
UNPACK64_W(9) UNPACK64_W(10) UNPACK64_W(11) UNPACK64_W(12) UNPACK64_W(13) UNPACK64_W(14) UNPACK64_W(15)
UNPACK64_W(16) UNPACK64_W(17) UNPACK64_W(18) UNPACK64_W(19) UNPACK64_W(20) UNPACK64_W(21) UNPACK64_W(22)
UNPACK64_W(23) UNPACK64_W(24) UNPACK64_W(25) UNPACK64_W(26) UNPACK64_W(27) UNPACK64_W(28) UNPACK64_W(29)
UNPACK64_W(30) UNPACK64_W(31) UNPACK64_W(32) UNPACK64_W(33) UNPACK64_W(34) UNPACK64_W(35) UNPACK64_W(36)
UNPACK64_W(37) UNPACK64_W(38) UNPACK64_W(39) UNPACK64_W(40) UNPACK64_W(41) UNPACK64_W(42) UNPACK64_W(43)
UNPACK64_W(44) UNPACK64_W(45) UNPACK64_W(46) UNPACK64_W(47) UNPACK64_W(48) UNPACK64_W(49) UNPACK64_W(50)
UNPACK64_W(51) UNPACK64_W(52) UNPACK64_W(53) UNPACK64_W(54) UNPACK64_W(55) UNPACK64_W(56) UNPACK64_W(57)
UNPACK64_W(58) UNPACK64_W(59) UNPACK64_W(60) UNPACK64_W(61) UNPACK64_W(62) UNPACK64_W(63) UNPACK64_W(64)

static void (*const unpack64_fn[65])(const uint8_t *, size_t, uint64_t *) = {
    NULL,        unpack64_1,  unpack64_2,  unpack64_3,  unpack64_4,  unpack64_5,  unpack64_6,  unpack64_7,
    unpack64_8,  unpack64_9,  unpack64_10, unpack64_11, unpack64_12, unpack64_13, unpack64_14, unpack64_15,
    unpack64_16, unpack64_17, unpack64_18, unpack64_19, unpack64_20, unpack64_21, unpack64_22, unpack64_23,
    unpack64_24, unpack64_25, unpack64_26, unpack64_27, unpack64_28, unpack64_29, unpack64_30, unpack64_31,
    unpack64_32, unpack64_33, unpack64_34, unpack64_35, unpack64_36, unpack64_37, unpack64_38, unpack64_39,
    unpack64_40, unpack64_41, unpack64_42, unpack64_43, unpack64_44, unpack64_45, unpack64_46, unpack64_47,
    unpack64_48, unpack64_49, unpack64_50, unpack64_51, unpack64_52, unpack64_53, unpack64_54, unpack64_55,
    unpack64_56, unpack64_57, unpack64_58, unpack64_59, unpack64_60, unpack64_61, unpack64_62, unpack64_63,
    unpack64_64};

static inline void unpack64(const uint8_t *in, size_t n, int w, uint64_t *out)
{
    if (w == 0)
        memset(out, 0, n * sizeof *out);
    else
        unpack64_fn[w](in, n, out);
}

static inline int bits_for64(uint64_t x) { return x ? 64 - __builtin_clzll(x) : 0; }

/*
 * In place: z[i], a zigzag delta, becomes the sum of the decoded deltas
 * z[0..i] modulo 2^32; returns the sum of all n. Four lanes at a time (a
 * prefix sum by two shifted adds, then the carry of the previous four): the
 * running sum is the loop-carried dependency that made decoding run at the
 * speed of one scalar add chain. Integer arithmetic, so every CPU level gets
 * the same bits. Callers that need the sums as signed integers must keep them
 * within int32: zigzag deltas below 2^22 decode to [-2^21, 2^21), and 1024 of
 * them sum to [-2^31, 2^31 - 1024].
 */
#define ZZ_PREFIX32_MAX_W 22 /* for up to 1024 values */
static inline uint32_t zz_prefix32(uint32_t *z, size_t n)
{
    uint32_t acc = 0;
    size_t i = 0;
#if defined(__SSE2__)
    const __m128i one = _mm_set1_epi32(1), zero = _mm_setzero_si128();
    __m128i carry = zero;
    for (; i + 4 <= n; i += 4) {
        __m128i x = _mm_loadu_si128((const __m128i *)(const void *)(z + i));
        x = _mm_xor_si128(_mm_srli_epi32(x, 1), _mm_sub_epi32(zero, _mm_and_si128(x, one)));
        x = _mm_add_epi32(x, _mm_slli_si128(x, 4));
        x = _mm_add_epi32(x, _mm_slli_si128(x, 8));
        x = _mm_add_epi32(x, carry);
        _mm_storeu_si128((__m128i *)(void *)(z + i), x);
        carry = _mm_shuffle_epi32(x, 0xff);
    }
    acc = (uint32_t)_mm_cvtsi128_si32(carry);
#endif
    for (; i < n; i++) {
        acc += (z[i] >> 1) ^ (0u - (z[i] & 1));
        z[i] = acc;
    }
    return acc;
}

/* As zz_prefix32 for 64-bit zigzags, two lanes at a time, modulo 2^64. */
static inline uint64_t zz_prefix64(uint64_t *z, size_t n)
{
    uint64_t acc = 0;
    size_t i = 0;
#if defined(__SSE2__)
    const __m128i one = _mm_set1_epi64x(1), zero = _mm_setzero_si128();
    __m128i carry = zero;
    for (; i + 2 <= n; i += 2) {
        __m128i x = _mm_loadu_si128((const __m128i *)(const void *)(z + i));
        x = _mm_xor_si128(_mm_srli_epi64(x, 1), _mm_sub_epi64(zero, _mm_and_si128(x, one)));
        x = _mm_add_epi64(x, _mm_slli_si128(x, 8));
        x = _mm_add_epi64(x, carry);
        _mm_storeu_si128((__m128i *)(void *)(z + i), x);
        carry = _mm_unpackhi_epi64(x, x);
    }
    acc = (uint64_t)_mm_cvtsi128_si64(carry);
#endif
    for (; i < n; i++) {
        acc += (z[i] >> 1) ^ (0ull - (z[i] & 1));
        z[i] = acc;
    }
    return acc;
}

#endif
