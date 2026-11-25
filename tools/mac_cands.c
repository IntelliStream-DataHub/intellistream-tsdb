// SPDX-License-Identifier: AGPL-3.0-only
/* The frame MAC candidates (mac_cands.h). Each one is written for the measurement and checked against known answers
 * (mc_selftest; the vectors come from Python's hmac and the `cryptography` package, and BLAKE3's own test vectors).
 * The x86 kernels carry target attributes, so the file builds at any -march and a candidate runs only where cpuid
 * says its instructions are there. */
#include "mac_cands.h"

#include <stdio.h>
#include <string.h>

#include "../client/tsdb_crypto.h"
#include "blake3/blake3.h"
#include "../server/plcheck.h"

#if defined(__x86_64__)
#include <cpuid.h>
#include <immintrin.h>
#define MC_X86 1
#endif

static const char *const names[MC_N] = {"none", "hmac", "hmac-shani", "gmac", "poly1305", "blake3", "xxh3-64", "xxh3-128"};

const char *mc_name(int kind)
{
    return kind >= 0 && kind < MC_N ? names[kind] : "?";
}

int mc_kind(const char *name)
{
    for (int i = 0; i < MC_N; i++)
        if (!strcmp(name, names[i]))
            return i;
    return -1;
}

static void le64put(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}
static uint64_t le64get(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}
static uint32_t le32get(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

/* ------------------------------------------------------------------ CPU features */

static int cpu_has(int what)
{
#ifdef MC_X86
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d))
        return 0;
    int aes = (c >> 25) & 1, pclmul = (c >> 1) & 1, sse41 = (c >> 19) & 1, ssse3 = (c >> 9) & 1;
    if (what == MC_GMAC)
        return aes && pclmul && sse41 && ssse3;
    if (what == MC_HMAC_SHANI) {
        if (!__get_cpuid_count(7, 0, &a, &b, &c, &d))
            return 0;
        return ((b >> 29) & 1) && sse41 && ssse3;
    }
#else
    (void)what;
#endif
    return 0;
}

/* ------------------------------------------------------------------ HMAC-SHA256 (portable: tsdb_crypto.c) */

struct hk {
    tsdb_sha256 in, out;
};

static void hk_init(struct hk *k, const uint8_t key[32])
{
    uint8_t pad[64] = {0};
    for (int i = 0; i < 64; i++)
        pad[i] = (uint8_t)((i < 32 ? key[i] : 0) ^ 0x36);
    tsdb_sha256_init(&k->in);
    tsdb_sha256_update(&k->in, pad, 64);
    for (int i = 0; i < 64; i++)
        pad[i] = (uint8_t)((i < 32 ? key[i] : 0) ^ 0x5c);
    tsdb_sha256_init(&k->out);
    tsdb_sha256_update(&k->out, pad, 64);
}

static void hk_tag(const struct hk *k, const uint8_t s[9], const void *p1, size_t n1, const void *p2, size_t n2,
                   uint8_t out[32])
{
    uint8_t ih[32];
    tsdb_sha256 x = k->in;
    tsdb_sha256_update(&x, s, 9);
    tsdb_sha256_update(&x, p1, n1);
    tsdb_sha256_update(&x, p2, n2);
    tsdb_sha256_final(&x, ih);
    x = k->out;
    tsdb_sha256_update(&x, ih, 32);
    tsdb_sha256_final(&x, out);
}

/* ------------------------------------------------------------------ SHA-256 with the SHA extensions */

struct sha {
    uint32_t h[8];
    uint8_t b[64];
    size_t bl;
    uint64_t len;
};

static const uint32_t K256[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
    0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
    0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2};

#ifdef MC_X86
__attribute__((target("sha,sse4.1,ssse3"))) static void sha_blocks_ni(uint32_t st[8], const uint8_t *p, size_t nb)
{
    const __m128i mask = _mm_set_epi64x(0x0c0d0e0f08090a0bULL, 0x0405060700010203ULL);
    __m128i t = _mm_loadu_si128((const __m128i *)&st[0]);
    __m128i s1 = _mm_loadu_si128((const __m128i *)&st[4]);
    t = _mm_shuffle_epi32(t, 0xB1);        /* CDAB */
    s1 = _mm_shuffle_epi32(s1, 0x1B);      /* EFGH */
    __m128i s0 = _mm_alignr_epi8(t, s1, 8); /* ABEF */
    s1 = _mm_blend_epi16(s1, t, 0xF0);     /* CDGH */
    for (; nb; nb--, p += 64) {
        __m128i a0 = s0, a1 = s1, w[4];
        for (int i = 0; i < 4; i++)
            w[i] = _mm_shuffle_epi8(_mm_loadu_si128((const __m128i *)(p + 16 * i)), mask);
        for (int i = 0; i < 16; i++) {
            __m128i cur = w[i & 3];
            __m128i m = _mm_add_epi32(cur, _mm_loadu_si128((const __m128i *)&K256[4 * i]));
            s1 = _mm_sha256rnds2_epu32(s1, s0, m);
            m = _mm_shuffle_epi32(m, 0x0E);
            s0 = _mm_sha256rnds2_epu32(s0, s1, m);
            if (i < 12) { /* w[i+4] = msg2(msg1(w[i], w[i+1]) + alignr(w[i+3], w[i+2], 4), w[i+3]) */
                __m128i x = _mm_sha256msg1_epu32(cur, w[(i + 1) & 3]);
                x = _mm_add_epi32(x, _mm_alignr_epi8(w[(i + 3) & 3], w[(i + 2) & 3], 4));
                w[i & 3] = _mm_sha256msg2_epu32(x, w[(i + 3) & 3]);
            }
        }
        s0 = _mm_add_epi32(s0, a0);
        s1 = _mm_add_epi32(s1, a1);
    }
    t = _mm_shuffle_epi32(s0, 0x1B);       /* FEBA */
    s1 = _mm_shuffle_epi32(s1, 0xB1);      /* DCHG */
    s0 = _mm_blend_epi16(t, s1, 0xF0);     /* DCBA */
    s1 = _mm_alignr_epi8(s1, t, 8);        /* HGFE */
    _mm_storeu_si128((__m128i *)&st[0], s0);
    _mm_storeu_si128((__m128i *)&st[4], s1);
}
#else
static void sha_blocks_ni(uint32_t st[8], const uint8_t *p, size_t nb)
{
    (void)st, (void)p, (void)nb;
}
#endif

static void sha_init(struct sha *s)
{
    static const uint32_t iv[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                   0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    memcpy(s->h, iv, sizeof iv);
    s->bl = 0;
    s->len = 0;
}

static void sha_update(struct sha *s, const void *v, size_t n)
{
    const uint8_t *p = v;
    s->len += n;
    if (s->bl) {
        size_t t = 64 - s->bl < n ? 64 - s->bl : n;
        memcpy(s->b + s->bl, p, t);
        s->bl += t, p += t, n -= t;
        if (s->bl < 64)
            return;
        sha_blocks_ni(s->h, s->b, 1);
        s->bl = 0;
    }
    if (n >= 64) {
        sha_blocks_ni(s->h, p, n / 64);
        p += n / 64 * 64;
        n %= 64;
    }
    memcpy(s->b, p, n);
    s->bl = n;
}

static void sha_final(struct sha *s, uint8_t out[32])
{
    uint64_t bits = s->len * 8;
    uint8_t pad[72] = {0x80};
    size_t pn = (s->bl < 56 ? 56 : 120) - s->bl;
    for (int i = 0; i < 8; i++)
        pad[pn + i] = (uint8_t)(bits >> (56 - 8 * i));
    sha_update(s, pad, pn + 8);
    for (int i = 0; i < 8; i++)
        for (int j = 0; j < 4; j++)
            out[4 * i + j] = (uint8_t)(s->h[i] >> (24 - 8 * j));
}

struct hkn {
    struct sha in, out;
};

static void hkn_init(struct hkn *k, const uint8_t key[32])
{
    uint8_t pad[64];
    for (int i = 0; i < 64; i++)
        pad[i] = (uint8_t)((i < 32 ? key[i] : 0) ^ 0x36);
    sha_init(&k->in);
    sha_update(&k->in, pad, 64);
    for (int i = 0; i < 64; i++)
        pad[i] = (uint8_t)((i < 32 ? key[i] : 0) ^ 0x5c);
    sha_init(&k->out);
    sha_update(&k->out, pad, 64);
}

static void hkn_tag(const struct hkn *k, const uint8_t s[9], const void *p1, size_t n1, const void *p2, size_t n2,
                    uint8_t out[32])
{
    uint8_t ih[32];
    struct sha x = k->in;
    sha_update(&x, s, 9);
    sha_update(&x, p1, n1);
    sha_update(&x, p2, n2);
    sha_final(&x, ih);
    x = k->out;
    sha_update(&x, ih, 32);
    sha_final(&x, out);
}

/* ------------------------------------------------------------------ AES-128-GMAC */

/* the key schedule and H^1..H^8 in GHASH's byte-reflected form (Intel's CLMUL white paper, Gueron and Kounavis) */
struct gk {
    _Alignas(16) uint8_t rk[11][16];
    _Alignas(16) uint8_t hp[8][16];
};

#ifdef MC_X86
#define GT __attribute__((target("aes,pclmul,sse4.1,ssse3")))

GT static inline __m128i bswap128(__m128i x)
{
    return _mm_shuffle_epi8(x, _mm_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
}

GT static inline void clmul_acc(__m128i a, __m128i b, __m128i *lo, __m128i *mid, __m128i *hi)
{
    *lo = _mm_xor_si128(*lo, _mm_clmulepi64_si128(a, b, 0x00));
    *hi = _mm_xor_si128(*hi, _mm_clmulepi64_si128(a, b, 0x11));
    *mid = _mm_xor_si128(*mid, _mm_xor_si128(_mm_clmulepi64_si128(a, b, 0x10), _mm_clmulepi64_si128(a, b, 0x01)));
}

/* the 256-bit product (lo, mid, hi) shifted left by one (the reflection) and reduced modulo x^128+x^7+x^2+x+1 */
GT static inline __m128i gh_reduce(__m128i lo, __m128i mid, __m128i hi)
{
    __m128i t3 = _mm_xor_si128(lo, _mm_slli_si128(mid, 8));
    __m128i t6 = _mm_xor_si128(hi, _mm_srli_si128(mid, 8));
    __m128i t7 = _mm_srli_epi32(t3, 31), t8 = _mm_srli_epi32(t6, 31);
    t3 = _mm_slli_epi32(t3, 1);
    t6 = _mm_slli_epi32(t6, 1);
    __m128i t9 = _mm_srli_si128(t7, 12);
    t8 = _mm_slli_si128(t8, 4);
    t7 = _mm_slli_si128(t7, 4);
    t3 = _mm_or_si128(t3, t7);
    t6 = _mm_or_si128(_mm_or_si128(t6, t8), t9);
    t7 = _mm_xor_si128(_mm_xor_si128(_mm_slli_epi32(t3, 31), _mm_slli_epi32(t3, 30)), _mm_slli_epi32(t3, 25));
    t8 = _mm_srli_si128(t7, 4);
    t7 = _mm_slli_si128(t7, 12);
    t3 = _mm_xor_si128(t3, t7);
    __m128i t2 = _mm_xor_si128(_mm_xor_si128(_mm_srli_epi32(t3, 1), _mm_srli_epi32(t3, 2)), _mm_srli_epi32(t3, 7));
    t2 = _mm_xor_si128(t2, t8);
    t3 = _mm_xor_si128(t3, t2);
    return _mm_xor_si128(t6, t3);
}

GT static inline __m128i gfmul(__m128i a, __m128i b)
{
    __m128i lo = _mm_setzero_si128(), mid = lo, hi = lo;
    clmul_acc(a, b, &lo, &mid, &hi);
    return gh_reduce(lo, mid, hi);
}

#define KEXP(k, r) aes_kexp(k, _mm_aeskeygenassist_si128(k, r))
GT static inline __m128i aes_kexp(__m128i k, __m128i g)
{
    g = _mm_shuffle_epi32(g, 0xff);
    k = _mm_xor_si128(k, _mm_slli_si128(k, 4));
    k = _mm_xor_si128(k, _mm_slli_si128(k, 4));
    k = _mm_xor_si128(k, _mm_slli_si128(k, 4));
    return _mm_xor_si128(k, g);
}

GT static __m128i aes_enc(const struct gk *k, __m128i x)
{
    x = _mm_xor_si128(x, _mm_load_si128((const __m128i *)k->rk[0]));
    for (int i = 1; i < 10; i++)
        x = _mm_aesenc_si128(x, _mm_load_si128((const __m128i *)k->rk[i]));
    return _mm_aesenclast_si128(x, _mm_load_si128((const __m128i *)k->rk[10]));
}

GT static void gk_init(struct gk *k, const uint8_t key[16])
{
    __m128i r[11];
    r[0] = _mm_loadu_si128((const __m128i *)key);
    r[1] = KEXP(r[0], 0x01);
    r[2] = KEXP(r[1], 0x02);
    r[3] = KEXP(r[2], 0x04);
    r[4] = KEXP(r[3], 0x08);
    r[5] = KEXP(r[4], 0x10);
    r[6] = KEXP(r[5], 0x20);
    r[7] = KEXP(r[6], 0x40);
    r[8] = KEXP(r[7], 0x80);
    r[9] = KEXP(r[8], 0x1b);
    r[10] = KEXP(r[9], 0x36);
    for (int i = 0; i < 11; i++)
        _mm_store_si128((__m128i *)k->rk[i], r[i]);
    __m128i h = bswap128(aes_enc(k, _mm_setzero_si128())), p = h;
    for (int i = 0; i < 8; i++) {
        _mm_store_si128((__m128i *)k->hp[i], p);
        p = gfmul(p, h);
    }
}

/* X <- (X + B1) H^n + B2 H^(n-1) ... over whole blocks, eight at a time with one reduction */
GT static __m128i gh_blocks(const struct gk *k, __m128i x, const uint8_t *p, size_t nb)
{
    __m128i h[8];
    for (int i = 0; i < 8; i++)
        h[i] = _mm_load_si128((const __m128i *)k->hp[i]);
    for (; nb >= 8; nb -= 8, p += 128) {
        __m128i lo = _mm_setzero_si128(), mid = lo, hi = lo;
        clmul_acc(_mm_xor_si128(x, bswap128(_mm_loadu_si128((const __m128i *)p))), h[7], &lo, &mid, &hi);
        for (int i = 1; i < 8; i++)
            clmul_acc(bswap128(_mm_loadu_si128((const __m128i *)(p + 16 * i))), h[7 - i], &lo, &mid, &hi);
        x = gh_reduce(lo, mid, hi);
    }
    for (; nb; nb--, p += 16)
        x = gfmul(_mm_xor_si128(x, bswap128(_mm_loadu_si128((const __m128i *)p))), h[0]);
    return x;
}

struct gh {
    __m128i x;
    uint8_t b[16];
    size_t bl;
    uint64_t len;
};

GT static void gh_update(const struct gk *k, struct gh *g, const uint8_t *p, size_t n)
{
    g->len += n;
    if (g->bl) {
        size_t t = 16 - g->bl < n ? 16 - g->bl : n;
        memcpy(g->b + g->bl, p, t);
        g->bl += t, p += t, n -= t;
        if (g->bl < 16)
            return;
        g->x = gh_blocks(k, g->x, g->b, 1);
        g->bl = 0;
    }
    g->x = gh_blocks(k, g->x, p, n / 16);
    p += n / 16 * 16;
    n %= 16;
    memcpy(g->b, p, n);
    g->bl = n;
}

GT static void gmac_tag(const struct gk *k, const uint8_t nonce[12], const void *p1, size_t n1, const void *p2,
                        size_t n2, uint8_t out[16])
{
    struct gh g = {.x = _mm_setzero_si128()};
    gh_update(k, &g, p1, n1);
    gh_update(k, &g, p2, n2);
    if (g.bl) {
        memset(g.b + g.bl, 0, 16 - g.bl);
        g.x = gh_blocks(k, g.x, g.b, 1);
    }
    uint8_t lb[16] = {0};
    uint64_t bits = g.len * 8;
    for (int i = 0; i < 8; i++)
        lb[i] = (uint8_t)(bits >> (56 - 8 * i));
    g.x = gh_blocks(k, g.x, lb, 1);
    uint8_t j0[16] = {0};
    memcpy(j0, nonce, 12);
    j0[15] = 1;
    __m128i t = _mm_xor_si128(bswap128(g.x), aes_enc(k, _mm_loadu_si128((const __m128i *)j0)));
    _mm_storeu_si128((__m128i *)out, t);
}
#endif

/* ------------------------------------------------------------------ ChaCha20 (one block) and Poly1305 */

#define ROTL32(v, n) ((v) << (n) | (v) >> (32 - (n)))
#define QR(a, b, c, d)                                                                                                 \
    a += b, d ^= a, d = ROTL32(d, 16), c += d, b ^= c, b = ROTL32(b, 12), a += b, d ^= a, d = ROTL32(d, 8), c += d,    \
        b ^= c, b = ROTL32(b, 7)

/* the first 32 bytes of ChaCha20's block 0 under key and a 96-bit nonce: Poly1305's one-time key (RFC 8439 2.6) */
static void chacha_otk(const uint8_t key[32], const uint8_t nonce[12], uint8_t out[32])
{
    uint32_t s[16] = {0x61707865, 0x3320646e, 0x79622d32, 0x6b206574};
    for (int i = 0; i < 8; i++)
        s[4 + i] = le32get(key + 4 * i);
    s[12] = 0;
    for (int i = 0; i < 3; i++)
        s[13 + i] = le32get(nonce + 4 * i);
    uint32_t x[16];
    memcpy(x, s, sizeof s);
    for (int i = 0; i < 10; i++) {
        QR(x[0], x[4], x[8], x[12]), QR(x[1], x[5], x[9], x[13]), QR(x[2], x[6], x[10], x[14]),
            QR(x[3], x[7], x[11], x[15]);
        QR(x[0], x[5], x[10], x[15]), QR(x[1], x[6], x[11], x[12]), QR(x[2], x[7], x[8], x[13]),
            QR(x[3], x[4], x[9], x[14]);
    }
    for (int i = 0; i < 8; i++) {
        uint32_t v = x[i] + s[i];
        out[4 * i] = (uint8_t)v, out[4 * i + 1] = (uint8_t)(v >> 8), out[4 * i + 2] = (uint8_t)(v >> 16),
                    out[4 * i + 3] = (uint8_t)(v >> 24);
    }
}

typedef unsigned __int128 u128;
#define M44 0xfffffffffffULL
#define M42 0x3ffffffffffULL

struct poly {
    uint64_t r0, r1, r2, h0, h1, h2, pad0, pad1;
    uint8_t b[16];
    size_t bl;
};

static void poly_init(struct poly *p, const uint8_t k[32])
{
    uint64_t t0 = le64get(k), t1 = le64get(k + 8);
    p->r0 = t0 & 0xffc0fffffffULL;
    p->r1 = ((t0 >> 44) | (t1 << 20)) & 0xfffffc0ffffULL;
    p->r2 = (t1 >> 24) & 0x00ffffffc0fULL;
    p->h0 = p->h1 = p->h2 = 0;
    p->pad0 = le64get(k + 16);
    p->pad1 = le64get(k + 24);
    p->bl = 0;
}

static void poly_blocks(struct poly *p, const uint8_t *m, size_t nb, uint64_t hibit)
{
    uint64_t r0 = p->r0, r1 = p->r1, r2 = p->r2, h0 = p->h0, h1 = p->h1, h2 = p->h2;
    uint64_t s1 = r1 * (5 << 2), s2 = r2 * (5 << 2);
    for (; nb; nb--, m += 16) {
        uint64_t t0 = le64get(m), t1 = le64get(m + 8);
        h0 += t0 & M44;
        h1 += ((t0 >> 44) | (t1 << 20)) & M44;
        h2 += ((t1 >> 24) & M42) | hibit;
        u128 d0 = (u128)h0 * r0 + (u128)h1 * s2 + (u128)h2 * s1;
        u128 d1 = (u128)h0 * r1 + (u128)h1 * r0 + (u128)h2 * s2;
        u128 d2 = (u128)h0 * r2 + (u128)h1 * r1 + (u128)h2 * r0;
        uint64_t c = (uint64_t)(d0 >> 44);
        h0 = (uint64_t)d0 & M44;
        d1 += c;
        c = (uint64_t)(d1 >> 44);
        h1 = (uint64_t)d1 & M44;
        d2 += c;
        c = (uint64_t)(d2 >> 42);
        h2 = (uint64_t)d2 & M42;
        h0 += c * 5;
        c = h0 >> 44;
        h0 &= M44;
        h1 += c;
    }
    p->h0 = h0, p->h1 = h1, p->h2 = h2;
}

static void poly_update(struct poly *p, const uint8_t *m, size_t n)
{
    if (p->bl) {
        size_t t = 16 - p->bl < n ? 16 - p->bl : n;
        memcpy(p->b + p->bl, m, t);
        p->bl += t, m += t, n -= t;
        if (p->bl < 16)
            return;
        poly_blocks(p, p->b, 1, 1ULL << 40);
        p->bl = 0;
    }
    poly_blocks(p, m, n / 16, 1ULL << 40);
    m += n / 16 * 16;
    n %= 16;
    memcpy(p->b, m, n);
    p->bl = n;
}

static void poly_final(struct poly *p, uint8_t out[16])
{
    if (p->bl) {
        p->b[p->bl] = 1;
        memset(p->b + p->bl + 1, 0, 15 - p->bl);
        poly_blocks(p, p->b, 1, 0);
    }
    uint64_t h0 = p->h0, h1 = p->h1, h2 = p->h2, c;
    c = h1 >> 44, h1 &= M44, h2 += c, c = h2 >> 42, h2 &= M42, h0 += c * 5, c = h0 >> 44, h0 &= M44, h1 += c;
    c = h1 >> 44, h1 &= M44, h2 += c, c = h2 >> 42, h2 &= M42, h0 += c * 5, c = h0 >> 44, h0 &= M44, h1 += c;
    uint64_t g0 = h0 + 5;
    c = g0 >> 44, g0 &= M44;
    uint64_t g1 = h1 + c;
    c = g1 >> 44, g1 &= M44;
    uint64_t g2 = h2 + c - (1ULL << 42);
    c = (g2 >> 63) - 1;
    g0 &= c, g1 &= c, g2 &= c, c = ~c;
    h0 = (h0 & c) | g0, h1 = (h1 & c) | g1, h2 = (h2 & c) | g2;
    uint64_t t0 = p->pad0, t1 = p->pad1;
    h0 += t0 & M44;
    c = h0 >> 44, h0 &= M44;
    h1 += (((t0 >> 44) | (t1 << 20)) & M44) + c;
    c = h1 >> 44, h1 &= M44;
    h2 += ((t1 >> 24) & M42) + c;
    h2 &= M42;
    le64put(out, h0 | (h1 << 44));
    le64put(out + 8, (h1 >> 20) | (h2 << 24));
}

/* ------------------------------------------------------------------ the interface */

int mc_init(struct mc *m, int kind, const uint8_t key[32])
{
    memset(m, 0, sizeof *m);
    m->kind = kind;
    switch (kind) {
    case MC_NONE:
        return 0;
    case MC_HMAC:
        _Static_assert(sizeof(struct hk) <= sizeof m->st, "st");
        m->taglen = 32;
        hk_init((struct hk *)m->st, key);
        return 0;
    case MC_HMAC_SHANI:
        _Static_assert(sizeof(struct hkn) <= sizeof m->st, "st");
        if (!cpu_has(MC_HMAC_SHANI))
            return -1;
        m->taglen = 32;
        hkn_init((struct hkn *)m->st, key);
        return 0;
    case MC_GMAC:
        _Static_assert(sizeof(struct gk) <= sizeof m->st, "st");
        if (!cpu_has(MC_GMAC))
            return -1;
#ifdef MC_X86
        m->taglen = 16;
        gk_init((struct gk *)m->st, key);
        return 0;
#else
        return -1;
#endif
    case MC_POLY1305:
    case MC_BLAKE3:
        m->taglen = 16;
        memcpy(m->st, key, 32);
        return 0;
    case MC_XXH3_64:
    case MC_XXH3_128:
        m->taglen = kind == MC_XXH3_64 ? 8 : 16;
        memcpy(m->st, key, 8); /* the salt */
        return 0;
    }
    return -1;
}

void mc_tag(const struct mc *m, uint64_t seq, uint8_t type, const void *p1, size_t n1, const void *p2, size_t n2,
            uint8_t *out)
{
    uint8_t s[12] = {0};
    le64put(s, seq);
    s[8] = type;
    switch (m->kind) {
    case MC_HMAC:
        hk_tag((const struct hk *)m->st, s, p1, n1, p2, n2, out);
        break;
    case MC_HMAC_SHANI:
        hkn_tag((const struct hkn *)m->st, s, p1, n1, p2, n2, out);
        break;
#ifdef MC_X86
    case MC_GMAC: /* the nonce is seq and type: unique per key, which is per connection and direction */
        gmac_tag((const struct gk *)m->st, s, p1, n1, p2, n2, out);
        break;
#endif
    case MC_POLY1305: {
        uint8_t otk[32];
        struct poly p;
        chacha_otk(m->st, s, otk);
        poly_init(&p, otk);
        poly_update(&p, p1, n1);
        poly_update(&p, p2, n2);
        poly_final(&p, out);
        break;
    }
    case MC_BLAKE3: {
        blake3_hasher h;
        blake3_hasher_init_keyed(&h, m->st);
        blake3_hasher_update(&h, s, 9);
        blake3_hasher_update(&h, p1, n1);
        blake3_hasher_update(&h, p2, n2);
        blake3_hasher_finalize(&h, out, 16);
        break;
    }
    case MC_XXH3_64:
    case MC_XXH3_128: {
        uint64_t salt;
        memcpy(&salt, m->st, 8);
        uint64_t seed = salt ^ seq ^ (uint64_t)type << 56;
        if (n1 && n2) /* two pieces: chained */
            seed = XXH3_64bits_withSeed(p1, n1, seed);
        else if (n1)
            p2 = p1, n2 = n1;
        if (m->kind == MC_XXH3_128) {
            pl_check(seed, p2, n2, out); /* PL_CHECK_FULL's check */
        } else {
            uint64_t h = XXH3_64bits_withSeed(p2, n2, seed);
            le64put(out, h);
        }
        break;
    }
    default:
        break;
    }
}

/* ------------------------------------------------------------------ known answers */

#include "mac_cands_kat.h"

int mc_selftest(void)
{
    uint8_t key[32], msg[1100], t[32], u[32];
    for (int i = 0; i < 32; i++)
        key[i] = (uint8_t)i;
    for (size_t i = 0; i < sizeof msg; i++)
        msg[i] = (uint8_t)(i * 7 + 3);
    int bad = 0, ran = 0;
    for (size_t v = 0; v < sizeof kat / sizeof kat[0]; v++) {
        int kind = mc_kind(kat[v].mac);
        struct mc m;
        if (mc_init(&m, kind, key) != 0) {
            continue;
        }
        ran++;
        size_t n = kat[v].len;
        /* every split of the payload into header and body must give the same tag */
        size_t splits[4] = {0, n / 3, n > 0 ? n - 1 : 0, n};
        for (int s = 0; s < 4; s++) {
            mc_tag(&m, KAT_SEQ, KAT_TYPE, msg, splits[s], msg + splits[s], n - splits[s], t);
            if (memcmp(t, kat[v].tag, m.taglen)) {
                fprintf(stderr, "mac selftest: %s len %zu split %zu: wrong tag\n", kat[v].mac, n, splits[s]);
                bad = 1;
                break;
            }
        }
    }
    /* BLAKE3's own keyed vector: input i % 251 of 1023 bytes, key "whats the Elvish word for friend" */
    {
        uint8_t in[1023];
        for (int i = 0; i < 1023; i++)
            in[i] = (uint8_t)(i % 251);
        blake3_hasher h;
        blake3_hasher_init_keyed(&h, (const uint8_t *)"whats the Elvish word for friend");
        blake3_hasher_update(&h, in, sizeof in);
        blake3_hasher_finalize(&h, u, 32);
        if (memcmp(u, b3_keyed_1023, 32)) {
            fprintf(stderr, "mac selftest: blake3 keyed 1023: wrong\n");
            bad = 1;
        }
    }
    if (!ran)
        bad = 1;
    return bad ? -1 : 0;
}
