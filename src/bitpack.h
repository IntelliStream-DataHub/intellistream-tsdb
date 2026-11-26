/* SPDX-License-Identifier: AGPL-3.0-only */
/* Bit packing shared by the ALP-style codecs: values of w bits, LSB first. */
#ifndef TSDB_BITPACK_H
#define TSDB_BITPACK_H

#include <stdint.h>
#include <string.h>
#if defined(__SSE2__)
#include <emmintrin.h> /* x86-64 baseline: every CPU level has it */
#endif
/* AVX2 unpack (below): every AVX2 build (the v3 and v4 levels, and -march=native on AVX2 and AVX-512 CPUs).
 * 256-bit vpshufb/vpsrlvd uses no 512-bit lanes and matches v4's -mprefer-vector-width=256. */
#if defined(__AVX2__)
#define BITPACK_AVX2 1
#include <immintrin.h>
#endif


static inline int bits_for(uint32_t x) { return x ? 32 - __builtin_clz(x) : 0; }

static inline size_t packed_bytes(size_t n, int w) { return (n * (size_t)w + 7) / 8; }

/* Streams values of w bits into out, LSB first; writes packed_bytes(n, w)
 * bytes plus up to 3 of padding. Width-specialized branchless 8-value groups:
 * 8 values of W bits are exactly W bytes, stored with at most 3 bytes overshoot. */
#define PACK_STEP(W, K)                                                                 \
    do {                                                                                \
        const unsigned b_ = ((K) * (W)) & 63;                                           \
        const unsigned q_ = ((K) * (W)) >> 6;                                           \
        uint64_t v_ = in[i + (K)];                                                      \
        if (b_ == 0)                                                                    \
            w_[q_] = v_;                                                                \
        else {                                                                          \
            w_[q_] |= v_ << b_;                                                         \
            if (b_ + (W) > 64)                                                          \
                w_[q_ + 1] = v_ >> ((64 - b_) & 63);                                    \
        }                                                                               \
    } while (0)

#define PACK_W(W)                                                                       \
    static inline void pack_##W(const uint32_t *in, size_t n, uint8_t *out)             \
    {                                                                                   \
        size_t i = 0;                                                                   \
        for (; i + 8 <= n; i += 8, out += (W)) {                                        \
            uint64_t w_[4] = {0, 0, 0, 0};                                              \
            PACK_STEP(W, 0); PACK_STEP(W, 1); PACK_STEP(W, 2); PACK_STEP(W, 3);         \
            PACK_STEP(W, 4); PACK_STEP(W, 5); PACK_STEP(W, 6); PACK_STEP(W, 7);         \
            if ((W) >= 8)                                                               \
                memcpy(out, &w_[0], 8);                                                 \
            if ((W) >= 16)                                                              \
                memcpy(out + 8, &w_[1], 8);                                             \
            if ((W) >= 24)                                                              \
                memcpy(out + 16, &w_[2], 8);                                            \
            if (((W) & 7) >= 5) {                                                       \
                memcpy(out + (((W) >> 3) * 8), &w_[(W) >> 3], 8);                       \
            } else if (((W) & 7) >= 1) {                                                \
                uint32_t t_ = (uint32_t)w_[(W) >> 3];                                   \
                memcpy(out + (((W) >> 3) * 8), &t_, 4);                                 \
            }                                                                           \
        }                                                                               \
        uint64_t acc = 0;                                                               \
        int bits = 0;                                                                   \
        for (; i < n; i++) {                                                            \
            acc |= (uint64_t)in[i] << bits;                                             \
            bits += (W);                                                                \
            if (bits >= 32) {                                                           \
                uint32_t word = (uint32_t)acc;                                          \
                memcpy(out, &word, 4);                                                  \
                out += 4;                                                               \
                acc >>= 32;                                                             \
                bits -= 32;                                                             \
            }                                                                           \
        }                                                                               \
        if (bits > 0) {                                                                 \
            uint32_t word = (uint32_t)acc;                                              \
            memcpy(out, &word, 4);                                                      \
        }                                                                               \
    }

PACK_W(1) PACK_W(2) PACK_W(3)
#ifdef BITPACK_AVX2
static inline void pack_4(const uint32_t *in, size_t n, uint8_t *out)
{
    size_t i = 0;
    const __m256i perm = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);
    const __m256i mul = _mm256_set1_epi16(0x1001);
    for (; i + 32 <= n; i += 32, out += 16) {
        __m256i v0 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i));
        __m256i v1 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 8));
        __m256i v2 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 16));
        __m256i v3 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 24));
        __m256i b = _mm256_permutevar8x32_epi32(
            _mm256_packus_epi16(_mm256_packus_epi32(v0, v1), _mm256_packus_epi32(v2, v3)), perm);
        __m256i m = _mm256_maddubs_epi16(b, mul);
        __m128i p = _mm_packus_epi16(_mm256_castsi256_si128(m), _mm256_extracti128_si256(m, 1));
        _mm_storeu_si128((__m128i *)(void *)out, p);
    }
    for (; i + 8 <= n; i += 8, out += 4) {
        uint32_t w_ = in[i] | (in[i + 1] << 4) | (in[i + 2] << 8) | (in[i + 3] << 12) |
                      (in[i + 4] << 16) | (in[i + 5] << 20) | (in[i + 6] << 24) | (in[i + 7] << 28);
        memcpy(out, &w_, 4);
    }
    if (i < n) {
        uint32_t acc = 0;
        int bits = 0;
        for (; i < n; i++, bits += 4)
            acc |= in[i] << bits;
        memcpy(out, &acc, 4);
    }
}
#else
PACK_W(4)
#endif
PACK_W(5) PACK_W(6) PACK_W(7)
#ifdef BITPACK_AVX2
static inline void pack_8(const uint32_t *in, size_t n, uint8_t *out)
{
    size_t i = 0;
    const __m256i perm = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);
    for (; i + 32 <= n; i += 32, out += 32) {
        __m256i v0 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i));
        __m256i v1 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 8));
        __m256i v2 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 16));
        __m256i v3 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 24));
        __m256i b = _mm256_permutevar8x32_epi32(
            _mm256_packus_epi16(_mm256_packus_epi32(v0, v1), _mm256_packus_epi32(v2, v3)), perm);
        _mm256_storeu_si256((__m256i *)(void *)out, b);
    }
    for (; i < n; i++)
        *out++ = (uint8_t)in[i];
}
#else
PACK_W(8)
#endif
PACK_W(9) PACK_W(10) PACK_W(11) PACK_W(12) PACK_W(13) PACK_W(14) PACK_W(15)
#ifdef BITPACK_AVX2
static inline void pack_16(const uint32_t *in, size_t n, uint8_t *out)
{
    size_t i = 0;
    for (; i + 16 <= n; i += 16, out += 32) {
        __m256i v0 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i));
        __m256i v1 = _mm256_loadu_si256((const __m256i *)(const void *)(in + i + 8));
        __m256i p = _mm256_permute4x64_epi64(_mm256_packus_epi32(v0, v1), 0xd8);
        _mm256_storeu_si256((__m256i *)(void *)out, p);
    }
    for (; i < n; i++, out += 2) {
        uint16_t v = (uint16_t)in[i];
        memcpy(out, &v, 2);
    }
}
#else
PACK_W(16)
#endif
PACK_W(17) PACK_W(18) PACK_W(19) PACK_W(20) PACK_W(21) PACK_W(22) PACK_W(23) PACK_W(24)
PACK_W(25) PACK_W(26) PACK_W(27) PACK_W(28) PACK_W(29) PACK_W(30) PACK_W(31)
static inline void pack_32(const uint32_t *in, size_t n, uint8_t *out) { memcpy(out, in, n * 4); }

static void (*const pack_fn[33])(const uint32_t *, size_t, uint8_t *) = {
    NULL,    pack_1,  pack_2,  pack_3,  pack_4,  pack_5,  pack_6,  pack_7,  pack_8,
    pack_9,  pack_10, pack_11, pack_12, pack_13, pack_14, pack_15, pack_16, pack_17,
    pack_18, pack_19, pack_20, pack_21, pack_22, pack_23, pack_24, pack_25, pack_26,
    pack_27, pack_28, pack_29, pack_30, pack_31, pack_32};

static inline void pack(const uint32_t *in, size_t n, int w, uint8_t *out)
{
    if (w)
        pack_fn[w](in, n, out);
}

/* One 8-byte load per value, w a constant (fallback on non-AVX2 or n < 8). */
#define UNPACK_W(W)                                                                     \
    static inline void unpack_##W(const uint8_t *in, size_t n, uint32_t *out)           \
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
UNPACK_W(25) UNPACK_W(26) UNPACK_W(27) UNPACK_W(28) UNPACK_W(29) UNPACK_W(30) UNPACK_W(31)
static inline void unpack_32(const uint8_t *in, size_t n, uint32_t *out) { memcpy(out, in, n * 4); }

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

/* Widths 26..31: eight values per step via two 4x64-bit AVX2 vectors (w + 7 <= 38 fits a 64-bit lane, and four
 * values fit a 128-bit load: 4 + 4*31 = 128 bits). Reads at most 16 - ceil(w/2) <= 3 bytes past the packed bytes. */
#define BPW_B(R, J) ((((R) >> 3) + (J)) < 16 ? (uint8_t)(((R) >> 3) + (J)) : (uint8_t)0x80)
#define BPW_C(R) BPW_B(R, 0), BPW_B(R, 1), BPW_B(R, 2), BPW_B(R, 3), BPW_B(R, 4), 0x80, 0x80, 0x80
#define BPW_CTL0(W) {BPW_C(0), BPW_C(W), BPW_C(2 * (W)), BPW_C(3 * (W))}
#define BPW_CTL1(W) {BPW_C(4 * ((W) & 1)), BPW_C((W) + 4 * ((W) & 1)), \
                     BPW_C(2 * (W) + 4 * ((W) & 1)), BPW_C(3 * (W) + 4 * ((W) & 1))}
#define BPW_SHR0(W) {0, (uint64_t)((W) & 7), (uint64_t)((2 * (W)) & 7), (uint64_t)((3 * (W)) & 7)}
#define BPW_SHR1(W) {(uint64_t)(4 * ((W) & 1)), (uint64_t)(((W) + 4 * ((W) & 1)) & 7), \
                     (uint64_t)((2 * (W) + 4 * ((W) & 1)) & 7), (uint64_t)((3 * (W) + 4 * ((W) & 1)) & 7)}
#define BPW_ROWS(M) {M(26), M(27), M(28), M(29), M(30), M(31)}
static const uint8_t bpw_ctl0[6][32] = BPW_ROWS(BPW_CTL0);
static const uint8_t bpw_ctl1[6][32] = BPW_ROWS(BPW_CTL1);
static const uint64_t bpw_shr0[6][4] = BPW_ROWS(BPW_SHR0);
static const uint64_t bpw_shr1[6][4] = BPW_ROWS(BPW_SHR1);

static inline void unpack_avx2_wide(const uint8_t *in, size_t n, int w, uint32_t *out)
{
    const int idx = w - 26;
    const __m256i ctl0 = _mm256_loadu_si256((const __m256i *)(const void *)bpw_ctl0[idx]);
    const __m256i ctl1 = _mm256_loadu_si256((const __m256i *)(const void *)bpw_ctl1[idx]);
    const __m256i shr0 = _mm256_loadu_si256((const __m256i *)(const void *)bpw_shr0[idx]);
    const __m256i shr1 = _mm256_loadu_si256((const __m256i *)(const void *)bpw_shr1[idx]);
    const uint64_t mask64 = (1ull << w) - 1;
    const __m256i mask = _mm256_set1_epi64x((long long)mask64);
    const size_t hi = (size_t)w >> 1;
    size_t i = 0;
    for (; i + 8 <= n; i += 8, in += w) {
        __m256i x0 = _mm256_broadcastsi128_si256(_mm_loadu_si128((const __m128i *)(const void *)in));
        __m256i x1 = _mm256_broadcastsi128_si256(_mm_loadu_si128((const __m128i *)(const void *)(in + hi)));
        x0 = _mm256_and_si256(_mm256_srlv_epi64(_mm256_shuffle_epi8(x0, ctl0), shr0), mask);
        x1 = _mm256_and_si256(_mm256_srlv_epi64(_mm256_shuffle_epi8(x1, ctl1), shr1), mask);
        __m256i lo = _mm256_permute2x128_si256(x0, x1, 0x20);
        __m256i hi2 = _mm256_permute2x128_si256(x0, x1, 0x31);
        __m256i res = _mm256_castps_si256(
            _mm256_shuffle_ps(_mm256_castsi256_ps(lo), _mm256_castsi256_ps(hi2), _MM_SHUFFLE(2, 0, 2, 0)));
        _mm256_storeu_si256((__m256i *)(void *)(out + i), res);
    }
    for (; i < n; i++) {
        uint64_t bit = (uint64_t)(i & 7) * (uint64_t)w, x;
        memcpy(&x, in + (bit >> 3), 8);
        out[i] = (uint32_t)((x >> (bit & 7)) & mask64);
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
    else if (w < 32 && n >= 8)
        unpack_avx2_wide(in, n, w, out);
#endif
    else
        unpack_fn[w](in, n, out);
}

/* Values of w bits, w up to 64, in the same LSB-first stream as pack(); writes
 * packed_bytes(n, w) bytes plus up to 7 of padding. Width-specialized branchless
 * 8-value groups: 8 values of W bits are W bytes, stored as ceil(W/8) 64-bit words
 * (overshoot 8*ceil(W/8) - W <= 7). */
#define PACK64_STEP(W, K)                                                               \
    do {                                                                                \
        const unsigned b_ = ((K) * (W)) & 63;                                           \
        const unsigned q_ = ((K) * (W)) >> 6;                                           \
        uint64_t v_ = in[i + (K)];                                                      \
        if (b_ == 0)                                                                    \
            w_[q_] = v_;                                                                \
        else {                                                                          \
            w_[q_] |= v_ << b_;                                                         \
            if (b_ + (W) > 64)                                                          \
                w_[q_ + 1] = v_ >> ((64 - b_) & 63);                                    \
        }                                                                               \
    } while (0)

#define PACK64_W(W)                                                                     \
    static inline void pack64_##W(const uint64_t *in, size_t n, uint8_t *out)           \
    {                                                                                   \
        size_t i = 0;                                                                   \
        for (; i + 8 <= n; i += 8, out += (W)) {                                        \
            uint64_t w_[8] = {0, 0, 0, 0, 0, 0, 0, 0};                                  \
            PACK64_STEP(W, 0); PACK64_STEP(W, 1); PACK64_STEP(W, 2); PACK64_STEP(W, 3); \
            PACK64_STEP(W, 4); PACK64_STEP(W, 5); PACK64_STEP(W, 6); PACK64_STEP(W, 7); \
            memcpy(out, &w_[0], 8);                                                     \
            if ((W) > 8)  memcpy(out + 8,  &w_[1], 8);                                  \
            if ((W) > 16) memcpy(out + 16, &w_[2], 8);                                  \
            if ((W) > 24) memcpy(out + 24, &w_[3], 8);                                  \
            if ((W) > 32) memcpy(out + 32, &w_[4], 8);                                  \
            if ((W) > 40) memcpy(out + 40, &w_[5], 8);                                  \
            if ((W) > 48) memcpy(out + 48, &w_[6], 8);                                  \
            if ((W) > 56) memcpy(out + 56, &w_[7], 8);                                  \
        }                                                                               \
        uint64_t acc = 0;                                                               \
        int bits = 0;                                                                   \
        for (; i < n; i++) {                                                            \
            uint64_t v = in[i];                                                         \
            acc |= v << bits;                                                           \
            bits += (W);                                                                \
            if (bits >= 64) {                                                           \
                memcpy(out, &acc, 8);                                                   \
                out += 8;                                                               \
                bits -= 64;                                                             \
                acc = bits ? v >> ((W) - bits) : 0;                                     \
            }                                                                           \
        }                                                                               \
        if (bits > 0)                                                                   \
            memcpy(out, &acc, 8);                                                       \
    }
PACK64_W(1)  PACK64_W(2)  PACK64_W(3)  PACK64_W(4)  PACK64_W(5)  PACK64_W(6)  PACK64_W(7)  PACK64_W(8)
PACK64_W(9)  PACK64_W(10) PACK64_W(11) PACK64_W(12) PACK64_W(13) PACK64_W(14) PACK64_W(15) PACK64_W(16)
PACK64_W(17) PACK64_W(18) PACK64_W(19) PACK64_W(20) PACK64_W(21) PACK64_W(22) PACK64_W(23) PACK64_W(24)
PACK64_W(25) PACK64_W(26) PACK64_W(27) PACK64_W(28) PACK64_W(29) PACK64_W(30) PACK64_W(31) PACK64_W(32)
PACK64_W(33) PACK64_W(34) PACK64_W(35) PACK64_W(36) PACK64_W(37) PACK64_W(38) PACK64_W(39) PACK64_W(40)
PACK64_W(41) PACK64_W(42) PACK64_W(43) PACK64_W(44) PACK64_W(45) PACK64_W(46) PACK64_W(47) PACK64_W(48)
PACK64_W(49) PACK64_W(50) PACK64_W(51) PACK64_W(52) PACK64_W(53) PACK64_W(54) PACK64_W(55) PACK64_W(56)
PACK64_W(57) PACK64_W(58) PACK64_W(59) PACK64_W(60) PACK64_W(61) PACK64_W(62) PACK64_W(63)
static inline void pack64_64(const uint64_t *in, size_t n, uint8_t *out) { memcpy(out, in, n * 8); }

static void (*const pack64_fn[65])(const uint64_t *, size_t, uint8_t *) = {
    NULL,      pack64_1,  pack64_2,  pack64_3,  pack64_4,  pack64_5,  pack64_6,  pack64_7,
    pack64_8,  pack64_9,  pack64_10, pack64_11, pack64_12, pack64_13, pack64_14, pack64_15,
    pack64_16, pack64_17, pack64_18, pack64_19, pack64_20, pack64_21, pack64_22, pack64_23,
    pack64_24, pack64_25, pack64_26, pack64_27, pack64_28, pack64_29, pack64_30, pack64_31,
    pack64_32, pack64_33, pack64_34, pack64_35, pack64_36, pack64_37, pack64_38, pack64_39,
    pack64_40, pack64_41, pack64_42, pack64_43, pack64_44, pack64_45, pack64_46, pack64_47,
    pack64_48, pack64_49, pack64_50, pack64_51, pack64_52, pack64_53, pack64_54, pack64_55,
    pack64_56, pack64_57, pack64_58, pack64_59, pack64_60, pack64_61, pack64_62, pack64_63,
    pack64_64};

static inline void pack64(const uint64_t *in, size_t n, int w, uint8_t *out)
{
    if (w)
        pack64_fn[w](in, n, out);
}

/* One 8-byte load per value, and a ninth byte when the value straddles it
 * (only possible beyond 57 bits). Widths 58..63 unroll 8 values per step so
 * the shift and ninth-byte condition are compile-time constants. Reads up to 7
 * bytes past the packed data. */
#define UNPACK64_W(W)                                                                   \
    static inline void unpack64_##W(const uint8_t *in, size_t n, uint64_t *out)         \
    {                                                                                   \
        const uint64_t mask = (W) == 64 ? ~0ull : ((1ull << ((W) & 63)) - 1);          \
        for (size_t i = 0; i < n; i++) {                                                \
            uint64_t bit = (uint64_t)i * (W), x;                                        \
            unsigned s = (unsigned)(bit & 7);                                           \
            const uint8_t *p = in + (bit >> 3);                                         \
            memcpy(&x, p, 8);                                                           \
            out[i] = (x >> s) & mask;                                                   \
        }                                                                               \
    }

#define UNPACK64_HI_STEP(W, K)                                                          \
    do {                                                                                \
        const unsigned bit_ = (K) * (W);                                                \
        const unsigned s_ = bit_ & 7;                                                   \
        const uint8_t *p_ = in + (bit_ >> 3);                                           \
        uint64_t x_;                                                                    \
        memcpy(&x_, p_, 8);                                                             \
        x_ >>= s_;                                                                      \
        if ((W) + s_ > 64)                                                              \
            x_ |= (uint64_t)p_[8] << ((64 - s_) & 63);                                  \
        out[i + (K)] = x_ & mask;                                                       \
    } while (0)

#define UNPACK64_HI(W)                                                                  \
    static inline void unpack64_##W(const uint8_t *in, size_t n, uint64_t *out)         \
    {                                                                                   \
        const uint64_t mask = (1ull << (W)) - 1;                                        \
        size_t i = 0;                                                                   \
        for (; i + 8 <= n; i += 8, in += (W)) {                                         \
            UNPACK64_HI_STEP(W, 0); UNPACK64_HI_STEP(W, 1);                             \
            UNPACK64_HI_STEP(W, 2); UNPACK64_HI_STEP(W, 3);                             \
            UNPACK64_HI_STEP(W, 4); UNPACK64_HI_STEP(W, 5);                             \
            UNPACK64_HI_STEP(W, 6); UNPACK64_HI_STEP(W, 7);                             \
        }                                                                               \
        for (; i < n; i++) {                                                            \
            uint64_t bit = (uint64_t)(i & 7) * (W), x;                                  \
            unsigned s = (unsigned)(bit & 7);                                           \
            const uint8_t *p = in + (bit >> 3);                                         \
            memcpy(&x, p, 8);                                                           \
            x >>= s;                                                                    \
            if ((W) + s > 64)                                                           \
                x |= (uint64_t)p[8] << ((64 - s) & 63);                                 \
            out[i] = x & mask;                                                          \
        }                                                                               \
    }
UNPACK64_W(1)  UNPACK64_W(2)  UNPACK64_W(3)  UNPACK64_W(4)  UNPACK64_W(5)  UNPACK64_W(6)  UNPACK64_W(7)  UNPACK64_W(8)
UNPACK64_W(9)  UNPACK64_W(10) UNPACK64_W(11) UNPACK64_W(12) UNPACK64_W(13) UNPACK64_W(14) UNPACK64_W(15) UNPACK64_W(16)
UNPACK64_W(17) UNPACK64_W(18) UNPACK64_W(19) UNPACK64_W(20) UNPACK64_W(21) UNPACK64_W(22) UNPACK64_W(23) UNPACK64_W(24)
UNPACK64_W(25) UNPACK64_W(26) UNPACK64_W(27) UNPACK64_W(28) UNPACK64_W(29) UNPACK64_W(30) UNPACK64_W(31) UNPACK64_W(32)
UNPACK64_W(33) UNPACK64_W(34) UNPACK64_W(35) UNPACK64_W(36) UNPACK64_W(37) UNPACK64_W(38) UNPACK64_W(39) UNPACK64_W(40)
UNPACK64_W(41) UNPACK64_W(42) UNPACK64_W(43) UNPACK64_W(44) UNPACK64_W(45) UNPACK64_W(46) UNPACK64_W(47) UNPACK64_W(48)
UNPACK64_W(49) UNPACK64_W(50) UNPACK64_W(51) UNPACK64_W(52) UNPACK64_W(53) UNPACK64_W(54) UNPACK64_W(55) UNPACK64_W(56)
UNPACK64_W(57)
UNPACK64_HI(58) UNPACK64_HI(59) UNPACK64_HI(60) UNPACK64_HI(61) UNPACK64_HI(62) UNPACK64_HI(63)
static inline void unpack64_64(const uint8_t *in, size_t n, uint64_t *out) { memcpy(out, in, n * 8); }

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

#ifdef BITPACK_AVX2
/* Eight 64-bit values per step (w bytes) for w = 1..57: four 128-bit loads at bytes 0, floor(2w/8),
 * floor(4w/8), floor(6w/8), two values per 128-bit lane (since (2k*w)&7 <= 6 and w <= 57, every pair's
 * 8-byte spans stay inside bytes 0..14). Over-reads ((6*w)>>3) + 16 - w <= 15 bytes past packed_bytes
 * (inside BLK_SLACK = 32), and writes exactly n values. */
#define BP64_B(R, J) ((uint8_t)(((R) >> 3) + (J)))
#define BP64_C(R) BP64_B(R, 0), BP64_B(R, 1), BP64_B(R, 2), BP64_B(R, 3), \
                  BP64_B(R, 4), BP64_B(R, 5), BP64_B(R, 6), BP64_B(R, 7)
#define BP64_CTL01(W) {BP64_C(0), BP64_C(W), BP64_C((2 * (W)) & 7), BP64_C(((2 * (W)) & 7) + (W))}
#define BP64_CTL23(W) {BP64_C((4 * (W)) & 7), BP64_C(((4 * (W)) & 7) + (W)), \
                       BP64_C((6 * (W)) & 7), BP64_C(((6 * (W)) & 7) + (W))}
#define BP64_SHR01(W) {0, (uint64_t)((W) & 7), (uint64_t)((2 * (W)) & 7), (uint64_t)((3 * (W)) & 7)}
#define BP64_SHR23(W) {(uint64_t)((4 * (W)) & 7), (uint64_t)((5 * (W)) & 7), \
                       (uint64_t)((6 * (W)) & 7), (uint64_t)((7 * (W)) & 7)}
#define BP64_ROWS(M) {{0}, \
    M(1),  M(2),  M(3),  M(4),  M(5),  M(6),  M(7),  M(8),  M(9),  M(10), M(11), M(12), M(13), M(14), M(15), M(16), \
    M(17), M(18), M(19), M(20), M(21), M(22), M(23), M(24), M(25), M(26), M(27), M(28), M(29), M(30), M(31), M(32), \
    M(33), M(34), M(35), M(36), M(37), M(38), M(39), M(40), M(41), M(42), M(43), M(44), M(45), M(46), M(47), M(48), \
    M(49), M(50), M(51), M(52), M(53), M(54), M(55), M(56), M(57)}
#define BITPACK64_AVX2_MAX_W 57
static const uint8_t bp64_ctl01[BITPACK64_AVX2_MAX_W + 1][32] = BP64_ROWS(BP64_CTL01);
static const uint8_t bp64_ctl23[BITPACK64_AVX2_MAX_W + 1][32] = BP64_ROWS(BP64_CTL23);
static const uint64_t bp64_shr01[BITPACK64_AVX2_MAX_W + 1][4] = BP64_ROWS(BP64_SHR01);
static const uint64_t bp64_shr23[BITPACK64_AVX2_MAX_W + 1][4] = BP64_ROWS(BP64_SHR23);

static inline void unpack64_avx2(const uint8_t *in, size_t n, int w, uint64_t *out)
{
    const __m256i c01 = _mm256_loadu_si256((const __m256i *)(const void *)bp64_ctl01[w]);
    const __m256i c23 = _mm256_loadu_si256((const __m256i *)(const void *)bp64_ctl23[w]);
    const __m256i s01 = _mm256_loadu_si256((const __m256i *)(const void *)bp64_shr01[w]);
    const __m256i s23 = _mm256_loadu_si256((const __m256i *)(const void *)bp64_shr23[w]);
    const uint64_t mask64 = (1ull << w) - 1;
    const __m256i mask = _mm256_set1_epi64x((long long)mask64);
    const size_t b1 = ((size_t)w * 2) >> 3, b2 = ((size_t)w * 4) >> 3, b3 = ((size_t)w * 6) >> 3;
    size_t i = 0;
    for (; i + 8 <= n; i += 8, in += w) {
        __m256i x01 = _mm256_inserti128_si256(
            _mm256_castsi128_si256(_mm_loadu_si128((const __m128i *)(const void *)in)),
            _mm_loadu_si128((const __m128i *)(const void *)(in + b1)), 1);
        __m256i x23 = _mm256_inserti128_si256(
            _mm256_castsi128_si256(_mm_loadu_si128((const __m128i *)(const void *)(in + b2))),
            _mm_loadu_si128((const __m128i *)(const void *)(in + b3)), 1);
        x01 = _mm256_and_si256(_mm256_srlv_epi64(_mm256_shuffle_epi8(x01, c01), s01), mask);
        x23 = _mm256_and_si256(_mm256_srlv_epi64(_mm256_shuffle_epi8(x23, c23), s23), mask);
        _mm256_storeu_si256((__m256i *)(void *)(out + i), x01);
        _mm256_storeu_si256((__m256i *)(void *)(out + i + 4), x23);
    }
    for (; i < n; i++) {
        uint64_t bit = (uint64_t)(i & 7) * (uint64_t)w, x;
        memcpy(&x, in + (bit >> 3), 8);
        out[i] = (x >> (bit & 7)) & mask64;
    }
}
#endif

static inline void unpack64(const uint8_t *in, size_t n, int w, uint64_t *out)
{
    if (w == 0)
        memset(out, 0, n * sizeof *out);
#ifdef BITPACK_AVX2
    else if (w <= BITPACK64_AVX2_MAX_W && n >= 8)
        unpack64_avx2(in, n, w, out);
#endif
    else
        unpack64_fn[w](in, n, out);
}

static inline int bits_for64(uint64_t x) { return x ? 64 - __builtin_clzll(x) : 0; }

/*
 * In place: z[i], a zigzag delta, becomes the sum of the decoded deltas
 * z[0..i] modulo 2^32; returns the sum of all n. Eight lanes at a time on AVX2
 * (four on SSE2), with the running carry updated by one independent add per
 * vector so the loop-carried dependency is a single cycle. Integer arithmetic,
 * so every CPU level gets the same bits. Callers that need the sums as signed
 * integers must keep them within int32: zigzag deltas below 2^22 decode to
 * [-2^21, 2^21), and 1024 of them sum to [-2^31, 2^31 - 1024].
 */
#define ZZ_PREFIX32_MAX_W 22 /* for up to 1024 values */
static inline uint32_t zz_prefix32(uint32_t *z, size_t n)
{
    uint32_t acc = 0;
    size_t i = 0;
#if defined(__AVX2__)
    const __m256i one = _mm256_set1_epi32(1), zero = _mm256_setzero_si256(), seven = _mm256_set1_epi32(7);
    __m256i carry = zero;
    for (; i + 8 <= n; i += 8) {
        __m256i x = _mm256_loadu_si256((const __m256i *)(const void *)(z + i));
        x = _mm256_xor_si256(_mm256_srli_epi32(x, 1), _mm256_sub_epi32(zero, _mm256_and_si256(x, one)));
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 4));
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 8));
        __m128i lo_sum = _mm_shuffle_epi32(_mm256_castsi256_si128(x), 0xff);
        x = _mm256_add_epi32(x, _mm256_inserti128_si256(zero, lo_sum, 1));
        __m256i tot = _mm256_permutevar8x32_epi32(x, seven);
        _mm256_storeu_si256((__m256i *)(void *)(z + i), _mm256_add_epi32(x, carry));
        carry = _mm256_add_epi32(carry, tot);
    }
    acc = (uint32_t)_mm_cvtsi128_si32(_mm256_castsi256_si128(carry));
#elif defined(__SSE2__)
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

/* As zz_prefix32 for 64-bit zigzags, four lanes at a time on AVX2 (two on SSE2), modulo 2^64. */
static inline uint64_t zz_prefix64(uint64_t *z, size_t n)
{
    uint64_t acc = 0;
    size_t i = 0;
#if defined(__AVX2__)
    const __m256i one = _mm256_set1_epi64x(1), zero = _mm256_setzero_si256();
    __m256i carry = zero;
    for (; i + 4 <= n; i += 4) {
        __m256i x = _mm256_loadu_si256((const __m256i *)(const void *)(z + i));
        x = _mm256_xor_si256(_mm256_srli_epi64(x, 1), _mm256_sub_epi64(zero, _mm256_and_si256(x, one)));
        x = _mm256_add_epi64(x, _mm256_slli_si256(x, 8));
        __m128i lo = _mm256_castsi256_si128(x);
        x = _mm256_add_epi64(x, _mm256_inserti128_si256(zero, _mm_unpackhi_epi64(lo, lo), 1));
        __m256i tot = _mm256_permute4x64_epi64(x, 0xff);
        _mm256_storeu_si256((__m256i *)(void *)(z + i), _mm256_add_epi64(x, carry));
        carry = _mm256_add_epi64(carry, tot);
    }
    acc = (uint64_t)_mm_cvtsi128_si64(_mm256_castsi256_si128(carry));
#elif defined(__SSE2__)
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
