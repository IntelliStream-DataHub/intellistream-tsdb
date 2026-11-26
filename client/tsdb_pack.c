/* SPDX-License-Identifier: Apache-2.0 */
/* The packed INSERT_VALUES body (see tsdb_pack.h). */
#include "tsdb_pack.h"

#include "tsdb_protocol.h"

#include <string.h>

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

/* Tile width for L1D-resident byte-plane transpose: 1024 values * 8 planes = 8 KiB, with a 64-byte pad per row so
 * the 8 plane buffers never alias in an 8-way 32 KiB L1D cache (4 KiB set stride). */
#define PACK_TILE 1024u
#define PACK_TILE_STRIDE (PACK_TILE + 64u)

static void transpose_to_planes(const uint8_t *v, uint8_t *o, size_t n, unsigned w)
{
    if (n == 0)
        return;
    if (w == 1) {
        memcpy(o, v, n);
        return;
    }
    uint8_t tile[8][PACK_TILE_STRIDE];
    for (size_t i0 = 0; i0 < n; i0 += PACK_TILE) {
        size_t m = n - i0 < PACK_TILE ? n - i0 : PACK_TILE;
        const uint8_t *src = v + i0 * w;
        size_t j = 0;
        if (w == 8) {
#if defined(__SSE2__)
            for (; j + 16 <= m; j += 16) {
                const uint8_t *s = src + j * 8;
                __m128i p0 = _mm_loadu_si128((const __m128i *)(const void *)(s + 0));
                __m128i p1 = _mm_loadu_si128((const __m128i *)(const void *)(s + 16));
                __m128i p2 = _mm_loadu_si128((const __m128i *)(const void *)(s + 32));
                __m128i p3 = _mm_loadu_si128((const __m128i *)(const void *)(s + 48));
                __m128i p4 = _mm_loadu_si128((const __m128i *)(const void *)(s + 64));
                __m128i p5 = _mm_loadu_si128((const __m128i *)(const void *)(s + 80));
                __m128i p6 = _mm_loadu_si128((const __m128i *)(const void *)(s + 96));
                __m128i p7 = _mm_loadu_si128((const __m128i *)(const void *)(s + 112));

                __m128i u0 = _mm_unpacklo_epi64(p0, p4);
                __m128i u1 = _mm_unpackhi_epi64(p0, p4);
                __m128i u2 = _mm_unpacklo_epi64(p1, p5);
                __m128i u3 = _mm_unpackhi_epi64(p1, p5);
                __m128i u4 = _mm_unpacklo_epi64(p2, p6);
                __m128i u5 = _mm_unpackhi_epi64(p2, p6);
                __m128i u6 = _mm_unpacklo_epi64(p3, p7);
                __m128i u7 = _mm_unpackhi_epi64(p3, p7);

                __m128i a0 = _mm_unpacklo_epi8(u0, u1);
                __m128i a1 = _mm_unpacklo_epi8(u2, u3);
                __m128i a2 = _mm_unpacklo_epi8(u4, u5);
                __m128i a3 = _mm_unpacklo_epi8(u6, u7);
                __m128i b0 = _mm_unpackhi_epi8(u0, u1);
                __m128i b1 = _mm_unpackhi_epi8(u2, u3);
                __m128i b2 = _mm_unpackhi_epi8(u4, u5);
                __m128i b3 = _mm_unpackhi_epi8(u6, u7);

                __m128i c0 = _mm_unpacklo_epi16(a0, a1);
                __m128i c1 = _mm_unpackhi_epi16(a0, a1);
                __m128i c2 = _mm_unpacklo_epi16(a2, a3);
                __m128i c3 = _mm_unpackhi_epi16(a2, a3);
                __m128i d0 = _mm_unpacklo_epi16(b0, b1);
                __m128i d1 = _mm_unpackhi_epi16(b0, b1);
                __m128i d2 = _mm_unpacklo_epi16(b2, b3);
                __m128i d3 = _mm_unpackhi_epi16(b2, b3);

                __m128i e0 = _mm_unpacklo_epi32(c0, c2);
                __m128i e1 = _mm_unpackhi_epi32(c0, c2);
                __m128i e2 = _mm_unpacklo_epi32(c1, c3);
                __m128i e3 = _mm_unpackhi_epi32(c1, c3);
                __m128i f0 = _mm_unpacklo_epi32(d0, d2);
                __m128i f1 = _mm_unpackhi_epi32(d0, d2);
                __m128i f2 = _mm_unpacklo_epi32(d1, d3);
                __m128i f3 = _mm_unpackhi_epi32(d1, d3);

                _mm_storeu_si128((__m128i *)(void *)(tile[0] + j), _mm_unpacklo_epi64(e0, f0));
                _mm_storeu_si128((__m128i *)(void *)(tile[1] + j), _mm_unpackhi_epi64(e0, f0));
                _mm_storeu_si128((__m128i *)(void *)(tile[2] + j), _mm_unpacklo_epi64(e1, f1));
                _mm_storeu_si128((__m128i *)(void *)(tile[3] + j), _mm_unpackhi_epi64(e1, f1));
                _mm_storeu_si128((__m128i *)(void *)(tile[4] + j), _mm_unpacklo_epi64(e2, f2));
                _mm_storeu_si128((__m128i *)(void *)(tile[5] + j), _mm_unpackhi_epi64(e2, f2));
                _mm_storeu_si128((__m128i *)(void *)(tile[6] + j), _mm_unpacklo_epi64(e3, f3));
                _mm_storeu_si128((__m128i *)(void *)(tile[7] + j), _mm_unpackhi_epi64(e3, f3));
            }
#endif
            for (; j < m; j++) {
                const uint8_t *s = src + j * 8;
                tile[0][j] = s[0];
                tile[1][j] = s[1];
                tile[2][j] = s[2];
                tile[3][j] = s[3];
                tile[4][j] = s[4];
                tile[5][j] = s[5];
                tile[6][j] = s[6];
                tile[7][j] = s[7];
            }
        } else if (w == 4) {
#if defined(__SSE2__)
            for (; j + 16 <= m; j += 16) {
                const uint8_t *s = src + j * 4;
                __m128i x0 = _mm_loadu_si128((const __m128i *)(const void *)(s + 0));
                __m128i x1 = _mm_loadu_si128((const __m128i *)(const void *)(s + 16));
                __m128i x2 = _mm_loadu_si128((const __m128i *)(const void *)(s + 32));
                __m128i x3 = _mm_loadu_si128((const __m128i *)(const void *)(s + 48));

                __m128i c0 = _mm_unpacklo_epi32(x0, x2);
                __m128i c1 = _mm_unpackhi_epi32(x0, x2);
                __m128i c2 = _mm_unpacklo_epi32(x1, x3);
                __m128i c3 = _mm_unpackhi_epi32(x1, x3);

                __m128i d0 = _mm_unpacklo_epi64(c0, c2);
                __m128i d1 = _mm_unpackhi_epi64(c0, c2);
                __m128i d2 = _mm_unpacklo_epi64(c1, c3);
                __m128i d3 = _mm_unpackhi_epi64(c1, c3);

                __m128i e0 = _mm_unpacklo_epi8(d0, d1);
                __m128i e1 = _mm_unpackhi_epi8(d0, d1);
                __m128i e2 = _mm_unpacklo_epi8(d2, d3);
                __m128i e3 = _mm_unpackhi_epi8(d2, d3);

                __m128i f0 = _mm_unpacklo_epi16(e0, e2);
                __m128i f1 = _mm_unpackhi_epi16(e0, e2);
                __m128i f2 = _mm_unpacklo_epi16(e1, e3);
                __m128i f3 = _mm_unpackhi_epi16(e1, e3);

                __m128i g0 = _mm_unpacklo_epi32(f0, f2);
                __m128i g1 = _mm_unpackhi_epi32(f0, f2);
                __m128i g2 = _mm_unpacklo_epi32(f1, f3);
                __m128i g3 = _mm_unpackhi_epi32(f1, f3);

                _mm_storeu_si128((__m128i *)(void *)(tile[0] + j), _mm_unpacklo_epi64(g0, g2));
                _mm_storeu_si128((__m128i *)(void *)(tile[1] + j), _mm_unpackhi_epi64(g0, g2));
                _mm_storeu_si128((__m128i *)(void *)(tile[2] + j), _mm_unpacklo_epi64(g1, g3));
                _mm_storeu_si128((__m128i *)(void *)(tile[3] + j), _mm_unpackhi_epi64(g1, g3));
            }
#endif
            for (; j < m; j++) {
                const uint8_t *s = src + j * 4;
                tile[0][j] = s[0];
                tile[1][j] = s[1];
                tile[2][j] = s[2];
                tile[3][j] = s[3];
            }
        } else {
            for (; j < m; j++)
                for (unsigned b = 0; b < w; b++)
                    tile[b][j] = src[j * w + b];
        }
        for (unsigned b = 0; b < w; b++)
            memcpy(o + (size_t)b * n + i0, tile[b], m);
    }
}

static void transpose_from_planes(const uint8_t *p, uint8_t *values, size_t n, unsigned w)
{
    if (n == 0)
        return;
    if (w == 1) {
        memcpy(values, p, n);
        return;
    }
    uint8_t tile[8][PACK_TILE_STRIDE];
    for (size_t i0 = 0; i0 < n; i0 += PACK_TILE) {
        size_t m = n - i0 < PACK_TILE ? n - i0 : PACK_TILE;
        for (unsigned b = 0; b < w; b++)
            memcpy(tile[b], p + (size_t)b * n + i0, m);
        uint8_t *dst = values + i0 * w;
        size_t j = 0;
        if (w == 8) {
#if defined(__SSE2__)
            for (; j + 16 <= m; j += 16) {
                __m128i p0 = _mm_loadu_si128((const __m128i *)(const void *)(tile[0] + j));
                __m128i p1 = _mm_loadu_si128((const __m128i *)(const void *)(tile[1] + j));
                __m128i p2 = _mm_loadu_si128((const __m128i *)(const void *)(tile[2] + j));
                __m128i p3 = _mm_loadu_si128((const __m128i *)(const void *)(tile[3] + j));
                __m128i p4 = _mm_loadu_si128((const __m128i *)(const void *)(tile[4] + j));
                __m128i p5 = _mm_loadu_si128((const __m128i *)(const void *)(tile[5] + j));
                __m128i p6 = _mm_loadu_si128((const __m128i *)(const void *)(tile[6] + j));
                __m128i p7 = _mm_loadu_si128((const __m128i *)(const void *)(tile[7] + j));

                __m128i a0 = _mm_unpacklo_epi8(p0, p1);
                __m128i b0 = _mm_unpackhi_epi8(p0, p1);
                __m128i a1 = _mm_unpacklo_epi8(p2, p3);
                __m128i b1 = _mm_unpackhi_epi8(p2, p3);
                __m128i a2 = _mm_unpacklo_epi8(p4, p5);
                __m128i b2 = _mm_unpackhi_epi8(p4, p5);
                __m128i a3 = _mm_unpacklo_epi8(p6, p7);
                __m128i b3 = _mm_unpackhi_epi8(p6, p7);

                __m128i c0 = _mm_unpacklo_epi16(a0, a1);
                __m128i c1 = _mm_unpackhi_epi16(a0, a1);
                __m128i c2 = _mm_unpacklo_epi16(a2, a3);
                __m128i c3 = _mm_unpackhi_epi16(a2, a3);
                __m128i d0 = _mm_unpacklo_epi16(b0, b1);
                __m128i d1 = _mm_unpackhi_epi16(b0, b1);
                __m128i d2 = _mm_unpacklo_epi16(b2, b3);
                __m128i d3 = _mm_unpackhi_epi16(b2, b3);

                uint8_t *d = dst + j * 8;
                _mm_storeu_si128((__m128i *)(void *)(d + 0), _mm_unpacklo_epi32(c0, c2));
                _mm_storeu_si128((__m128i *)(void *)(d + 16), _mm_unpackhi_epi32(c0, c2));
                _mm_storeu_si128((__m128i *)(void *)(d + 32), _mm_unpacklo_epi32(c1, c3));
                _mm_storeu_si128((__m128i *)(void *)(d + 48), _mm_unpackhi_epi32(c1, c3));
                _mm_storeu_si128((__m128i *)(void *)(d + 64), _mm_unpacklo_epi32(d0, d2));
                _mm_storeu_si128((__m128i *)(void *)(d + 80), _mm_unpackhi_epi32(d0, d2));
                _mm_storeu_si128((__m128i *)(void *)(d + 96), _mm_unpacklo_epi32(d1, d3));
                _mm_storeu_si128((__m128i *)(void *)(d + 112), _mm_unpackhi_epi32(d1, d3));
            }
#endif
            for (; j < m; j++) {
                uint8_t *d = dst + j * 8;
                d[0] = tile[0][j];
                d[1] = tile[1][j];
                d[2] = tile[2][j];
                d[3] = tile[3][j];
                d[4] = tile[4][j];
                d[5] = tile[5][j];
                d[6] = tile[6][j];
                d[7] = tile[7][j];
            }
        } else if (w == 4) {
#if defined(__SSE2__)
            for (; j + 16 <= m; j += 16) {
                __m128i p0 = _mm_loadu_si128((const __m128i *)(const void *)(tile[0] + j));
                __m128i p1 = _mm_loadu_si128((const __m128i *)(const void *)(tile[1] + j));
                __m128i p2 = _mm_loadu_si128((const __m128i *)(const void *)(tile[2] + j));
                __m128i p3 = _mm_loadu_si128((const __m128i *)(const void *)(tile[3] + j));

                __m128i a0 = _mm_unpacklo_epi8(p0, p1);
                __m128i a1 = _mm_unpackhi_epi8(p0, p1);
                __m128i a2 = _mm_unpacklo_epi8(p2, p3);
                __m128i a3 = _mm_unpackhi_epi8(p2, p3);

                uint8_t *d = dst + j * 4;
                _mm_storeu_si128((__m128i *)(void *)(d + 0), _mm_unpacklo_epi16(a0, a2));
                _mm_storeu_si128((__m128i *)(void *)(d + 16), _mm_unpackhi_epi16(a0, a2));
                _mm_storeu_si128((__m128i *)(void *)(d + 32), _mm_unpacklo_epi16(a1, a3));
                _mm_storeu_si128((__m128i *)(void *)(d + 48), _mm_unpackhi_epi16(a1, a3));
            }
#endif
            for (; j < m; j++) {
                uint8_t *d = dst + j * 4;
                d[0] = tile[0][j];
                d[1] = tile[1][j];
                d[2] = tile[2][j];
                d[3] = tile[3][j];
            }
        } else {
            for (; j < m; j++)
                for (unsigned b = 0; b < w; b++)
                    dst[j * w + b] = tile[b][j];
        }
    }
}

static inline uint64_t zigzag(int64_t x) { return ((uint64_t)x << 1) ^ (uint64_t)(x >> 63); }
static inline int64_t unzigzag(uint64_t z) { return (int64_t)(z >> 1) ^ -(int64_t)(z & 1); }

static inline uint8_t *put_varint(uint8_t *o, uint64_t x)
{
    while (x >= 0x80) {
        *o++ = (uint8_t)(x | 0x80);
        x >>= 7;
    }
    *o++ = (uint8_t)x;
    return o;
}

/* false when the varint runs past end or over 10 bytes */
static inline int get_varint(const uint8_t **p, const uint8_t *end, uint64_t *x)
{
    uint64_t v = 0;
    for (int shift = 0; shift < 70 && *p < end; shift += 7) {
        uint8_t b = *(*p)++;
        v |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) {
            *x = v;
            return 1;
        }
    }
    return 0;
}

size_t tsdb_pack_raw_bound(size_t n, unsigned type, int valid, int many)
{
    return n * (10 + tsdb_wire_width(type)) + (valid ? (n + 7) / 8 : 0) + (many ? n * 10 : 0);
}

size_t tsdb_pack_reshape(size_t n, unsigned type, const int64_t *ts, const void *values, const uint8_t *valid,
                         const uint64_t *series, uint8_t *out)
{
    uint8_t *o = out;
    int64_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        o = put_varint(o, zigzag((int64_t)((uint64_t)ts[i] - (uint64_t)prev)));
        prev = ts[i];
    }
    unsigned w = tsdb_wire_width(type);
    transpose_to_planes(values, o, n, w);
    o += n * w;
    if (valid) {
        memcpy(o, valid, (n + 7) / 8);
        o += (n + 7) / 8;
    }
    if (series) {
        uint64_t ps = 0;
        for (size_t i = 0; i < n; i++) {
            o = put_varint(o, zigzag((int64_t)(series[i] - ps))); /* wrapping: 0 -> UINT64_MAX is -1 */
            ps = series[i];
        }
    }
    return (size_t)(o - out);
}

int tsdb_pack_unshape(const uint8_t *in, size_t len, size_t n, unsigned type, int valid, int many, int64_t *ts,
                      uint8_t *values, uint8_t *valid_out, uint64_t *series)
{
    const uint8_t *p = in, *end = in + len;
    uint64_t z;
    int64_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        if (!get_varint(&p, end, &z))
            return -1;
        prev = (int64_t)((uint64_t)prev + (uint64_t)unzigzag(z));
        ts[i] = prev;
    }
    unsigned w = tsdb_wire_width(type);
    if ((size_t)(end - p) < n * w)
        return -1;
    transpose_from_planes(p, values, n, w);
    p += n * w;
    if (valid) {
        size_t vb = (n + 7) / 8;
        if ((size_t)(end - p) < vb)
            return -1;
        memcpy(valid_out, p, vb);
        p += vb;
    }
    if (many) {
        uint64_t ps = 0;
        for (size_t i = 0; i < n; i++) {
            if (!get_varint(&p, end, &z))
                return -1;
            ps += (uint64_t)unzigzag(z); /* every u64 is an id on the wire; tsdbd refuses ones it does not know */
            series[i] = ps;
        }
    }
    return p == end ? 0 : -1;
}
