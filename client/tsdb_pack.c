/* The packed INSERT_VALUES body (see tsdb_pack.h). */
#include "tsdb_pack.h"

#include "tsdb_protocol.h"

#include <string.h>

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
    return n * (10 + tsdb_wire_width(type)) + (valid ? (n + 7) / 8 : 0) + (many ? n * 5 : 0);
}

size_t tsdb_pack_reshape(size_t n, unsigned type, const int64_t *ts, const void *values, const uint8_t *valid,
                         const uint32_t *series, uint8_t *out)
{
    uint8_t *o = out;
    int64_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        o = put_varint(o, zigzag((int64_t)((uint64_t)ts[i] - (uint64_t)prev)));
        prev = ts[i];
    }
    unsigned w = tsdb_wire_width(type);
    const uint8_t *v = values;
    for (unsigned b = 0; b < w; b++, o += n)
        for (size_t i = 0; i < n; i++)
            o[i] = v[i * w + b];
    if (valid) {
        memcpy(o, valid, (n + 7) / 8);
        o += (n + 7) / 8;
    }
    if (series) {
        uint32_t ps = 0;
        for (size_t i = 0; i < n; i++) {
            o = put_varint(o, zigzag((int64_t)series[i] - (int64_t)ps));
            ps = series[i];
        }
    }
    return (size_t)(o - out);
}

int tsdb_pack_unshape(const uint8_t *in, size_t len, size_t n, unsigned type, int valid, int many, int64_t *ts,
                      uint8_t *values, uint8_t *valid_out, uint32_t *series)
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
    for (unsigned b = 0; b < w; b++, p += n)
        for (size_t i = 0; i < n; i++)
            values[i * w + b] = p[i];
    if (valid) {
        size_t vb = (n + 7) / 8;
        if ((size_t)(end - p) < vb)
            return -1;
        memcpy(valid_out, p, vb);
        p += vb;
    }
    if (many) {
        int64_t ps = 0;
        for (size_t i = 0; i < n; i++) {
            if (!get_varint(&p, end, &z))
                return -1;
            ps += unzigzag(z);
            if (ps < 0 || ps > UINT32_MAX)
                return -1;
            series[i] = (uint32_t)ps;
        }
    }
    return p == end ? 0 : -1;
}
