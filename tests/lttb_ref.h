/*
 * The reference LTTB for the tests: the straightforward array version of
 * Steinarsson's algorithm, with the paper's bucket boundaries and
 * area, and the first of equal areas winning. Points are the known, finite
 * values of a tsdb_values column (col_point).
 */
#ifndef LTTB_REF_H
#define LTTB_REF_H
#include "tsdb.h"

#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

typedef struct pt {
    int64_t ts;
    uint64_t bits; /* the value as stored: float/double bits, int64, bool 0/1 */
    double y;
} pt;

static inline bool col_point(const tsdb_values *c, size_t i, uint64_t *bits, double *y)
{
    if (c->valid && !(c->valid[i >> 3] >> (i & 7) & 1))
        return false;
    switch (c->type) {
    case TSDB_FLOAT32: {
        float f = ((const float *)c->values)[i];
        uint32_t u;
        memcpy(&u, &f, 4);
        *bits = u;
        *y = f;
        return isfinite(f);
    }
    case TSDB_FLOAT64: {
        double d = ((const double *)c->values)[i];
        memcpy(bits, &d, 8);
        *y = d;
        return isfinite(d);
    }
    case TSDB_INT64: {
        int64_t x = ((const int64_t *)c->values)[i];
        *bits = (uint64_t)x;
        *y = (double)x;
        return true;
    }
    case TSDB_BOOL: {
        uint8_t b = ((const uint8_t *)c->values)[i];
        *bits = b;
        *y = b;
        return true;
    }
    default: return false;
    }
}

static inline double xof(const pt *d, size_t i) { return (double)((uint64_t)d[i].ts - (uint64_t)d[0].ts); }

/* The reference: indices of the chosen points, written like the paper's code
 * (and ClickHouse's and the TimescaleDB toolkit's): bucket boundaries are
 * floor(b * every) + 1 with every = (n - 2) / (t - 2) in doubles. */
static inline size_t ref_lttb(const pt *d, size_t n, size_t t, size_t *out)
{
    if (n <= t) {
        for (size_t i = 0; i < n; i++)
            out[i] = i;
        return n;
    }
    double every = (double)(n - 2) / (double)(t - 2);
    size_t k = 0, a = 0;
    out[k++] = 0;
    for (size_t i = 0; i < t - 2; i++) {
        size_t avg_start = (size_t)floor((double)(i + 1) * every) + 1;
        size_t avg_end = (size_t)floor((double)(i + 2) * every) + 1;
        avg_end = avg_end < n ? avg_end : n;
        double sx = 0, sy = 0;
        for (size_t j = avg_start; j < avg_end; j++) {
            sx += xof(d, j);
            sy += d[j].y;
        }
        double cx = sx / (double)(avg_end - avg_start), cy = sy / (double)(avg_end - avg_start);
        size_t lo = (size_t)floor((double)i * every) + 1, hi = (size_t)floor((double)(i + 1) * every) + 1;
        double ax = xof(d, a), ay = d[a].y, best = -1;
        size_t sel = lo;
        for (size_t j = lo; j < hi; j++) {
            double area = fabs((ax - cx) * (d[j].y - ay) - (ax - xof(d, j)) * (cy - ay)) * 0.5;
            if (area > best) {
                best = area;
                sel = j;
            }
        }
        out[k++] = sel;
        a = sel;
    }
    out[k++] = n - 1;
    return k;
}

#endif
