/* MSB-first bit streams shared by the codecs. */
#ifndef TSDB_BITS_H
#define TSDB_BITS_H

#include <stdint.h>
#include <string.h>

static inline uint64_t rd64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

static inline uint64_t load_be64(const uint8_t *p) { return __builtin_bswap64(rd64(p)); }

static inline void store_be64(uint8_t *p, uint64_t v)
{
    v = __builtin_bswap64(v);
    memcpy(p, &v, 8);
}

typedef struct bitwriter {
    uint8_t *start, *p;
    uint64_t acc; /* pending bits, left aligned */
    int n;        /* number of pending bits, always < 64 */
} bitwriter;

static inline void bw_init(bitwriter *w, uint8_t *p)
{
    w->start = w->p = p;
    w->acc = 0;
    w->n = 0;
}

/* Appends the low k bits of v (which must have no higher bits set), 1 <= k <= 64. */
static inline void bw_put(bitwriter *w, uint64_t v, int k)
{
    int free_bits = 64 - w->n;
    if (k < free_bits) {
        w->acc |= v << (free_bits - k);
        w->n += k;
    } else {
        int rest = k - free_bits;
        store_be64(w->p, w->acc | (v >> rest));
        w->p += 8;
        w->acc = rest ? v << (64 - rest) : 0;
        w->n = rest;
    }
}

static inline void bw_zeros(bitwriter *w, size_t k)
{
    while (k >= 64) {
        bw_put(w, 0, 64);
        k -= 64;
    }
    if (k)
        bw_put(w, 0, (int)k);
}

/* Writes out the pending bits (8 bytes, so the output needs 8 bytes of room)
 * and returns the number of bytes used. */
static inline size_t bw_finish(bitwriter *w)
{
    store_be64(w->p, w->acc);
    return (size_t)(w->p - w->start) + (size_t)(w->n + 7) / 8;
}

/* At least 57 valid bits starting at bit pos, left aligned. */
static inline uint64_t br_peek(const uint8_t *base, uint64_t pos)
{
    return load_be64(base + (pos >> 3)) << (pos & 7);
}

#endif
