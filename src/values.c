/*
 * Value types: aggregation that skips nulls, granule statistics, and the
 * statistics as query results.
 *
 * Floats aggregate in double (sum, min, max; NaN is ignored by min/max and
 * propagates into sum). Integers (int64, bool, state codes) aggregate exactly:
 * a 128-bit sum and int64 min/max. Float32 without nulls uses the SIMD
 * kernels; with nulls, or for the other types, a scalar loop the compiler
 * vectorizes where it can.
 */
#include "internal.h"

static size_t count_null_f32(const uint32_t *v, size_t n)
{
    size_t k = 0;
    for (size_t i = 0; i < n; i++)
        k += v[i] == NULL_F32;
    return k;
}

static void fold_f32(const float *v, size_t n, bool may_be_null, agg_state *a)
{
    if (!may_be_null || !count_null_f32((const uint32_t *)v, n)) {
        simd_agg_f32(v, n, a);
        return;
    }
    double s = 0, mn = a->min, mx = a->max;
    uint64_t c = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t bits;
        memcpy(&bits, &v[i], 4);
        if (bits == NULL_F32)
            continue;
        double x = v[i];
        c++;
        s += x;
        mn = x < mn ? x : mn;
        mx = x > mx ? x : mx;
    }
    a->count += c;
    a->sum += s;
    a->min = mn;
    a->max = mx;
}

static void fold_f64(const double *v, size_t n, agg_state *a)
{
    double s = 0, mn = a->min, mx = a->max;
    uint64_t c = 0;
    for (size_t i = 0; i < n; i++) {
        uint64_t bits;
        memcpy(&bits, &v[i], 8);
        if (bits == NULL_F64)
            continue;
        double x = v[i];
        c++;
        s += x;
        mn = x < mn ? x : mn;
        mx = x > mx ? x : mx;
    }
    a->count += c;
    a->sum += s;
    a->min = mn;
    a->max = mx;
}

static void fold_i64(const int64_t *v, size_t n, agg_state *a)
{
    __int128 s = 0;
    int64_t mn = a->imin, mx = a->imax;
    uint64_t c = 0;
    for (size_t i = 0; i < n; i++) {
        int64_t x = v[i];
        if (x == NULL_I64)
            continue;
        c++;
        s += x;
        mn = x < mn ? x : mn;
        mx = x > mx ? x : mx;
    }
    a->count += c;
    a->isum += s;
    a->imin = mn;
    a->imax = mx;
}

/* bool (0, 1) and state codes: 4-byte slots with their own null */
static void fold_u32(const uint32_t *v, size_t n, uint32_t null, agg_state *a)
{
    int64_t s = 0, mn = a->imin, mx = a->imax;
    uint64_t c = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t x = v[i];
        if (x == null)
            continue;
        c++;
        s += x;
        mn = (int64_t)x < mn ? (int64_t)x : mn;
        mx = (int64_t)x > mx ? (int64_t)x : mx;
    }
    a->count += c;
    a->isum += s;
    a->imin = mn;
    a->imax = mx;
}

void agg_fold_hint(int type, const void *v, size_t n, bool may_be_null, agg_state *a)
{
    a->rows += n;
    switch (type) {
    case TSDB_FLOAT32: fold_f32(v, n, may_be_null, a); break;
    case TSDB_FLOAT64: fold_f64(v, n, a); break;
    case TSDB_INT64: fold_i64(v, n, a); break;
    case TSDB_BOOL: fold_u32(v, n, NULL_BOOL, a); break;
    default: fold_u32(v, n, NULL_STATE, a); break;
    }
}

void agg_fold(int type, const void *v, size_t n, agg_state *a) { agg_fold_hint(type, v, n, true, a); }

void granule_stats(int type, const void *v, size_t n, granule *g)
{
    agg_state a;
    agg_init(&a, type);
    agg_fold(type, v, n, &a);
    g->nulls = (uint32_t)(n - a.count);
    if (type_is_int(type)) {
        g->imin = a.imin;
        g->imax = a.imax;
        g->isum_lo = (uint64_t)a.isum;
        g->isum_hi = (int64_t)(a.isum >> 64);
    } else {
        g->fmin = a.min;
        g->fmax = a.max;
        g->fsum = canon_nan(a.sum); /* a merge rewriting the granule writes the same bits */
        g->isum_hi = 0;
    }
}

void agg_from_granule(int type, const granule *g, agg_state *a)
{
    agg_init(a, type);
    a->rows = g->count;
    a->count = g->count - g->nulls;
    if (type_is_int(type)) {
        a->isum = (__int128)(((unsigned __int128)(uint64_t)g->isum_hi << 64) | g->isum_lo);
        a->imin = g->imin;
        a->imax = g->imax;
    } else {
        a->sum = g->fsum;
        a->min = g->fmin;
        a->max = g->fmax;
    }
}
