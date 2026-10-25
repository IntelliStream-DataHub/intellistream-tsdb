/*
 * ALP and ALP_rd (Afroozeh, Kuffo, Boncz: "ALP: Adaptive Lossless
 * floating-Point Compression", SIGMOD 2024), adapted to float32 as DuckDB does.
 *
 * ALP, per vector of up to 1024 values: find exponents (e, f) such that
 *   enc = round(v * 10^e * 10^-f)      and      (float)enc * 10^f * 10^-e == v
 * for most values; store enc with frame of reference + bit-packing, and the
 * values that do not round-trip as exceptions (position, raw bits). The slots
 * of exceptions get the vector's first good integer, so they do not widen the
 * bit width. (e, f) is chosen in two levels, as in the paper: the best few
 * combinations for the granule from a sample, then the best of those for each
 * vector from 32 of its values.
 *
 * ALP_rd, for floats that are not decimals: split each value into the low
 * `r` bits, bit-packed as they are, and the high 32 - r bits, which repeat a
 * lot and are dictionary encoded (up to 8 entries, index of 0-3 bits); high
 * parts missing from the dictionary are exceptions.
 *
 * All arithmetic is in float32, so the check in the encoder is exactly what
 * the decoder computes. Decoders may read 8 bytes past the packed data
 * (BLK_SLACK covers it).
 */
#include "internal.h"

#include <math.h>
#include <stdlib.h>

#define ALP_VEC 1024
#define ALP_MAX_E 10
#define ALP_TOP 5
#define ALP_LIMIT 8388607.0f /* 2^23 - 1: integers stay exact in a float */
#define ALP_VEC_HDR 12      /* u8 e, u8 f, u8 width, u8 0, i32 base, u16 nexc, u16 0 */

static const float exp10f_tab[ALP_MAX_E + 1] = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f,
                                               1e6f, 1e7f, 1e8f, 1e9f, 1e10f};
static const float frac10f_tab[ALP_MAX_E + 1] = {1e0f,  1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f,
                                                1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f};

#include "bitpack.h"

/* ------------------------------------------------------------------- ALP */

static inline bool alp_enc1(float v, int e, int f, int32_t *k)
{
    float t = v * exp10f_tab[e] * frac10f_tab[f];
    bool in_range = t > -ALP_LIMIT && t < ALP_LIMIT; /* false for NaN */
    int32_t r = (int32_t)__builtin_lrintf(in_range ? t : 0.0f);
    float back = (float)r * exp10f_tab[f] * frac10f_tab[e];
    uint32_t a, b;
    memcpy(&a, &back, 4);
    memcpy(&b, &v, 4);
    *k = r;
    return in_range & (a == b);
}

/* Estimated bits for values in[0..n) at (e, f). */
static double alp_cost(const float *in, size_t n, size_t stride, int e, int f)
{
    int32_t lo = INT32_MAX, hi = INT32_MIN, k;
    size_t exc = 0, m = 0;
    for (size_t i = 0; i < n; i += stride, m++) {
        if (!alp_enc1(in[i], e, f, &k)) {
            exc++;
            continue;
        }
        lo = k < lo ? k : lo;
        hi = k > hi ? k : hi;
    }
    int w = exc == m ? 0 : bits_for((uint32_t)((int64_t)hi - lo));
    return (double)m * w + (double)exc * 48;
}

size_t alp_bound(size_t n)
{
    size_t vecs = (n + ALP_VEC - 1) / ALP_VEC;
    return 4 + vecs * (ALP_VEC_HDR + 8) + n * 4 + n * 6;
}

typedef struct combo {
    uint8_t e, f;
    double cost;
} combo;

size_t alp_encode(const float *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    /* level 1: the best few (e, f) for the whole granule, from a sample */
    combo top[ALP_TOP];
    int ntop = 0;
    size_t stride = n > 256 ? n / 256 : 1;
    for (int e = 0; e <= ALP_MAX_E; e++) {
        for (int f = 0; f <= e; f++) {
            double c = alp_cost(in, n, stride, e, f);
            int pos = ntop;
            while (pos > 0 && top[pos - 1].cost > c)
                pos--;
            if (pos >= ALP_TOP)
                continue;
            if (ntop < ALP_TOP)
                ntop++;
            memmove(top + pos + 1, top + pos, (size_t)(ntop - 1 - pos) * sizeof *top);
            top[pos] = (combo){(uint8_t)e, (uint8_t)f, c};
        }
    }
    /* mostly exceptions: not a decimal-like granule, leave it to ALP_rd */
    if (top[0].cost >= (double)((n + stride - 1) / stride) * 32)
        return 0;

    uint32_t vecs = (uint32_t)((n + ALP_VEC - 1) / ALP_VEC);
    store32(out, vecs);
    uint8_t *o = out + 4;
    int32_t enc[ALP_VEC];
    uint32_t u[ALP_VEC];
    uint16_t epos[ALP_VEC];
    uint32_t ebits[ALP_VEC];
    for (size_t v0 = 0; v0 < n; v0 += ALP_VEC) {
        size_t nv = n - v0 < ALP_VEC ? n - v0 : ALP_VEC;
        const float *x = in + v0;
        /* level 2: the best of those for this vector, from 32 of its values */
        int best = 0;
        if (ntop > 1) {
            size_t s2 = nv > 32 ? nv / 32 : 1;
            double bc = INFINITY;
            for (int t = 0; t < ntop; t++) {
                double c = alp_cost(x, nv, s2, top[t].e, top[t].f);
                if (c < bc) {
                    bc = c;
                    best = t;
                }
            }
        }
        int e = top[best].e, f = top[best].f;
        size_t nexc = 0;
        bool have = false;
        int32_t fill = 0, lo = 0, hi = 0;
        for (size_t i = 0; i < nv; i++) {
            if (alp_enc1(x[i], e, f, &enc[i])) {
                if (!have) {
                    fill = lo = hi = enc[i];
                    have = true;
                }
                lo = enc[i] < lo ? enc[i] : lo;
                hi = enc[i] > hi ? enc[i] : hi;
            } else {
                epos[nexc] = (uint16_t)i;
                memcpy(&ebits[nexc], &x[i], 4);
                nexc++;
                enc[i] = INT32_MIN; /* replaced below */
            }
        }
        for (size_t j = 0; j < nexc; j++)
            enc[epos[j]] = fill;
        int w = bits_for((uint32_t)((int64_t)hi - lo));
        for (size_t i = 0; i < nv; i++)
            u[i] = (uint32_t)((int64_t)enc[i] - lo);
        o[0] = (uint8_t)e;
        o[1] = (uint8_t)f;
        o[2] = (uint8_t)w;
        o[3] = 0;
        memcpy(o + 4, &lo, 4);
        uint16_t ne = (uint16_t)nexc;
        memcpy(o + 8, &ne, 2);
        memset(o + 10, 0, 2);
        o += ALP_VEC_HDR;
        size_t pb = packed_bytes(nv, w);
        pack(u, nv, w, o);
        o += pb;
        memcpy(o, epos, nexc * 2);
        o += nexc * 2;
        memcpy(o, ebits, nexc * 4);
        o += nexc * 4;
    }
    return (size_t)(o - out);
}

int alp_decode(const uint8_t *in, size_t len, size_t n, float *out)
{
    if (n == 0)
        return 0;
    if (len < 4 || load32(in) != (n + ALP_VEC - 1) / ALP_VEC)
        return TSDB_ERR_CORRUPT;
    const uint8_t *p = in + 4, *end = in + len;
    uint32_t u[ALP_VEC];
    for (size_t v0 = 0; v0 < n; v0 += ALP_VEC) {
        size_t nv = n - v0 < ALP_VEC ? n - v0 : ALP_VEC;
        if (p + ALP_VEC_HDR > end)
            return TSDB_ERR_CORRUPT;
        int e = p[0], f = p[1], w = p[2];
        int32_t base;
        uint16_t nexc;
        memcpy(&base, p + 4, 4);
        memcpy(&nexc, p + 8, 2);
        if (e > ALP_MAX_E || f > e || w > 32 || nexc > nv)
            return TSDB_ERR_CORRUPT;
        p += ALP_VEC_HDR;
        size_t pb = packed_bytes(nv, w);
        if (p + pb + (size_t)nexc * 6 > end)
            return TSDB_ERR_CORRUPT;
        unpack(p, nv, w, u);
        p += pb;
        float *o = out + v0;
        const float fe = exp10f_tab[f], ff = frac10f_tab[e];
        for (size_t i = 0; i < nv; i++)
            o[i] = (float)(int32_t)(u[i] + (uint32_t)base) * fe * ff;
        const uint8_t *ep = p, *eb = p + (size_t)nexc * 2;
        for (size_t j = 0; j < nexc; j++) {
            uint16_t pos;
            memcpy(&pos, ep + 2 * j, 2);
            if (pos >= nv)
                return TSDB_ERR_CORRUPT;
            memcpy(&o[pos], eb + 4 * j, 4);
        }
        p += (size_t)nexc * 6;
    }
    return p == end ? 0 : TSDB_ERR_CORRUPT;
}

/* ---------------------------------------------------------------- ALP_rd */

#define RD_DICT 8
#define RD_HDR 24 /* u8 r, u8 b, u8 ndict, u8 0, u16 dict[8], u32 nexc */

size_t alprd_bound(size_t n) { return RD_HDR + n * 4 + n * 4 + 16; }

/* Most frequent high parts of a sample for split r; returns the estimated bits. */
static double rd_plan(const void *in, int width, size_t n, size_t stride, int r, uint16_t *dict, int *ndict)
{
    uint16_t vals[512];
    uint32_t cnt[512];
    size_t nv = 0, m = 0;
    for (size_t i = 0; i < n; i += stride, m++) {
        uint16_t left = (uint16_t)(width == 4 ? ((const uint32_t *)in)[i] >> r : ((const uint64_t *)in)[i] >> r);
        size_t k = 0;
        while (k < nv && vals[k] != left)
            k++;
        if (k == nv) {
            if (nv == 512)
                continue;
            vals[nv] = left;
            cnt[nv++] = 0;
        }
        cnt[k]++;
    }
    /* the RD_DICT most frequent */
    int nd = 0;
    size_t covered = 0;
    for (; nd < RD_DICT && nd < (int)nv; nd++) {
        size_t bi = (size_t)nd;
        for (size_t k = (size_t)nd + 1; k < nv; k++)
            if (cnt[k] > cnt[bi])
                bi = k;
        uint16_t tv = vals[nd];
        uint32_t tc = cnt[nd];
        vals[nd] = vals[bi];
        cnt[nd] = cnt[bi];
        vals[bi] = tv;
        cnt[bi] = tc;
        dict[nd] = vals[nd];
        covered += cnt[nd];
    }
    *ndict = nd;
    int b = nd <= 1 ? 0 : bits_for((uint32_t)(nd - 1));
    return (double)m * (r + b) + (double)(m - covered) * 32; /* an exception: u16 position, u16 high part */
}

size_t alprd_encode(const uint32_t *in, size_t n, uint8_t *out)
{
    if (n == 0)
        return 0;
    size_t stride = n > 256 ? n / 256 : 1;
    int best_r = 31, nd = 0;
    uint16_t dict[RD_DICT], d2[RD_DICT];
    double best = INFINITY;
    for (int r = 16; r <= 31; r++) { /* the high part keeps 1 to 16 bits */
        int n2;
        double c = rd_plan(in, 4, n, stride, r, d2, &n2);
        if (c < best) {
            best = c;
            best_r = r;
            nd = n2;
            memcpy(dict, d2, sizeof dict);
        }
    }
    int r = best_r, b = nd <= 1 ? 0 : bits_for((uint32_t)(nd - 1));
    uint32_t *idx = malloc(n * sizeof *idx), *right = malloc(n * sizeof *right);
    uint8_t *exc = malloc(n * 4);
    if (!idx || !right || !exc) {
        free(idx);
        free(right);
        free(exc);
        return 0;
    }
    uint32_t nexc = 0;
    for (size_t i = 0; i < n; i++) {
        uint16_t left = (uint16_t)(in[i] >> r);
        int k = 0;
        while (k < nd && dict[k] != left)
            k++;
        if (k == nd) {
            uint16_t pos16 = (uint16_t)i, lv = left;
            memcpy(exc + 4 * nexc, &pos16, 2); /* granules are at most 65536 rows */
            memcpy(exc + 4 * nexc + 2, &lv, 2);
            nexc++;
            k = 0;
        }
        idx[i] = (uint32_t)k;
        right[i] = r == 32 ? in[i] : in[i] & ((1u << r) - 1);
    }
    out[0] = (uint8_t)r;
    out[1] = (uint8_t)b;
    out[2] = (uint8_t)nd;
    out[3] = 0;
    memset(out + 4, 0, 16);
    memcpy(out + 4, dict, (size_t)nd * 2);
    store32(out + 20, nexc);
    uint8_t *o = out + RD_HDR;
    size_t pi = packed_bytes(n, b), pr = packed_bytes(n, r);
    pack(idx, n, b, o);
    o += pi;
    pack(right, n, r, o);
    o += pr;
    memcpy(o, exc, (size_t)nexc * 4);
    o += (size_t)nexc * 4;
    free(idx);
    free(right);
    free(exc);
    return (size_t)(o - out);
}

int alprd_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out)
{
    if (n == 0)
        return 0;
    if (len < RD_HDR)
        return TSDB_ERR_CORRUPT;
    int r = in[0], b = in[1], nd = in[2];
    uint16_t dict[RD_DICT];
    memcpy(dict, in + 4, 16);
    uint32_t nexc = load32(in + 20);
    size_t pi = packed_bytes(n, b), pr = packed_bytes(n, r);
    if (r < 1 || r > 31 || b > 3 || nd > RD_DICT || nexc > n || len != RD_HDR + pi + pr + (size_t)nexc * 4)
        return TSDB_ERR_CORRUPT;
    uint32_t *idx = malloc(n * sizeof *idx);
    if (!idx)
        return TSDB_ERR_NOMEM;
    const uint8_t *p = in + RD_HDR;
    unpack(p, n, b, idx);
    unpack(p + pi, n, r, out); /* the low parts, high parts OR'd in below */
    uint32_t left[RD_DICT];
    for (int k = 0; k < RD_DICT; k++)
        left[k] = k < nd ? (uint32_t)dict[k] << r : 0;
    for (size_t i = 0; i < n; i++)
        out[i] |= left[idx[i] & (RD_DICT - 1)];
    free(idx);
    const uint8_t *x = p + pi + pr;
    for (uint32_t j = 0; j < nexc; j++) {
        uint16_t pos, lv;
        memcpy(&pos, x + 4 * j, 2);
        memcpy(&lv, x + 4 * j + 2, 2);
        if (pos >= n)
            return TSDB_ERR_CORRUPT;
        out[pos] = ((uint32_t)lv << r) | (out[pos] & ((1u << r) - 1));
    }
    return 0;
}

/*
 * ALP_rd for float64, as the paper has it: the high part keeps 1 to 16 bits
 * (the sign, the exponent and a few mantissa bits: a cut at 48 to 63), the
 * low 48 to 63 bits are packed as they are. For random-looking doubles, where
 * byte split and XOR find next to nothing, the dictionary still takes the
 * exponent's few values down to 0-3 bits.
 *
 *   u8 r | u8 b | u8 ndict | u8 0 | u16 dict[8] | u32 nexc | packed indexes (b bits) |
 *   packed low parts (r bits) | nexc x (u16 position, u16 high part)
 */
#define RD64_MAX_N 65536 /* u16 positions: a granule's rows */

size_t alprd64_bound(size_t n) { return RD_HDR + n / 2 + 8 + n * 8 + 8 + n * 4 + 16; }

size_t alprd64_encode(const uint64_t *in, size_t n, uint8_t *out)
{
    if (n == 0 || n > RD64_MAX_N)
        return 0;
    size_t stride = n > 256 ? n / 256 : 1;
    int best_r = 63, nd = 0;
    uint16_t dict[RD_DICT], d2[RD_DICT];
    double best = INFINITY;
    for (int r = 48; r <= 63; r++) {
        int n2;
        double c = rd_plan(in, 8, n, stride, r, d2, &n2);
        if (c < best) {
            best = c;
            best_r = r;
            nd = n2;
            memcpy(dict, d2, sizeof dict);
        }
    }
    int r = best_r, b = nd <= 1 ? 0 : bits_for((uint32_t)(nd - 1));
    uint32_t *idx = malloc(n * sizeof *idx);
    uint64_t *right = malloc(n * sizeof *right);
    uint8_t *exc = malloc(n * 4);
    if (!idx || !right || !exc) {
        free(idx);
        free(right);
        free(exc);
        return 0;
    }
    uint32_t nexc = 0;
    const uint64_t low = (1ull << r) - 1;
    for (size_t i = 0; i < n; i++) {
        uint16_t left = (uint16_t)(in[i] >> r);
        int k = 0;
        while (k < nd && dict[k] != left)
            k++;
        if (k == nd) {
            uint16_t pos16 = (uint16_t)i, lv = left;
            memcpy(exc + 4 * nexc, &pos16, 2);
            memcpy(exc + 4 * nexc + 2, &lv, 2);
            nexc++;
            k = 0;
        }
        idx[i] = (uint32_t)k;
        right[i] = in[i] & low;
    }
    out[0] = (uint8_t)r;
    out[1] = (uint8_t)b;
    out[2] = (uint8_t)nd;
    out[3] = 0;
    memset(out + 4, 0, 16);
    memcpy(out + 4, dict, (size_t)nd * 2);
    store32(out + 20, nexc);
    uint8_t *o = out + RD_HDR;
    pack(idx, n, b, o);
    o += packed_bytes(n, b);
    pack64(right, n, r, o);
    o += packed_bytes(n, r);
    memcpy(o, exc, (size_t)nexc * 4);
    o += (size_t)nexc * 4;
    free(idx);
    free(right);
    free(exc);
    return (size_t)(o - out);
}

int alprd64_decode(const uint8_t *in, size_t len, size_t n, uint64_t *out)
{
    if (n == 0)
        return 0;
    if (len < RD_HDR)
        return TSDB_ERR_CORRUPT;
    int r = in[0], b = in[1], nd = in[2];
    uint16_t dict[RD_DICT];
    memcpy(dict, in + 4, 16);
    uint32_t nexc = load32(in + 20);
    size_t pi = packed_bytes(n, b), pr = packed_bytes(n, r);
    if (r < 48 || r > 63 || b > 3 || nd > RD_DICT || nexc > n || len != RD_HDR + pi + pr + (size_t)nexc * 4)
        return TSDB_ERR_CORRUPT;
    uint32_t *idx = malloc(n * sizeof *idx);
    if (!idx)
        return TSDB_ERR_NOMEM;
    const uint8_t *p = in + RD_HDR;
    unpack(p, n, b, idx);
    unpack64(p + pi, n, r, out); /* the low parts, high parts OR'd in below */
    uint64_t left[RD_DICT];
    for (int k = 0; k < RD_DICT; k++)
        left[k] = k < nd ? (uint64_t)dict[k] << r : 0;
    for (size_t i = 0; i < n; i++)
        out[i] |= left[idx[i] & (RD_DICT - 1)];
    free(idx);
    const uint8_t *x = p + pi + pr;
    const uint64_t low = (1ull << r) - 1;
    for (uint32_t j = 0; j < nexc; j++) {
        uint16_t pos, lv;
        memcpy(&pos, x + 4 * j, 2);
        memcpy(&lv, x + 4 * j + 2, 2);
        if (pos >= n)
            return TSDB_ERR_CORRUPT;
        out[pos] = ((uint64_t)lv << r) | (out[pos] & low);
    }
    return 0;
}
