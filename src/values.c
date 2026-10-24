/*
 * Value types: aggregation that skips nulls, granule statistics, and the
 * statistics as query results.
 *
 * Floats aggregate in double (sum, min, max; NaN is ignored by min/max and
 * propagates into sum), and their spread as m2 in two passes over each run
 * (see agg_state). Integers (int64, bool, state codes) aggregate exactly:
 * a 128-bit sum, a 192-bit sum of squares and int64 min/max. Floats use the
 * SIMD kernels of simd.c, whose order of additions is fixed (lanes, see there)
 * and does not change with the CPU level. The integers too where simd.c has
 * AVX2 or AVX-512 kernels for them (exact, so in any order); the loops here
 * are the reference and the code for CPUs without AVX2.
 */
#include "internal.h"

static size_t count_null_f32(const uint32_t *v, size_t n)
{
    size_t k = 0;
    for (size_t i = 0; i < n; i++)
        k += v[i] == NULL_F32;
    return k;
}

static size_t count_null_f64(const uint64_t *v, size_t n)
{
    size_t k = 0;
    for (size_t i = 0; i < n; i++)
        k += v[i] == NULL_F64;
    return k;
}

/* the null count first (a quick pass the compiler vectorizes): without nulls the kernels skip the masking.
 * Runs shorter than the lanes take simd_small_* (same bits, less fixed cost: buckets of a few rows). */
static void fold_f32(const float *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x)
{
    if (n < simd_short)
        simd_small_f32(v, n, may_be_null, a, x);
    else
        simd_fold_f32(v, n, may_be_null ? count_null_f32((const uint32_t *)v, n) : 0, a, x);
}

static void fold_f64(const double *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x)
{
    if (n < simd_short)
        simd_small_f64(v, n, may_be_null, a, x);
    else
        simd_fold_f64(v, n, may_be_null ? count_null_f64((const uint64_t *)v, n) : 0, a, x);
}

/* Below these many values the loops here beat the integer kernels, whose setup and final combine (lanes of
 * 128-bit sums, min/max across lanes) cost more than a few values (fold-bench --rows 8, 16, 32: int64 without
 * the spread 0.75x of the loop at 8 rows, 1.25x at 16; with the spread the kernel is on par at 8 and the loop's
 * 128-bit squares slower; bool and state 0.6x at 16, 2x at 32). */
#define I64_KERNEL_MIN 16
#define U32_KERNEL_MIN 32

static void fold_i64(const int64_t *v, size_t n, agg_state *a, agg_dev *dx)
{
    if (simd_fold_i64 && (dx || n >= I64_KERNEL_MIN)) {
        simd_fold_i64(v, n, a, dx);
        return;
    }
    if (!dx) {
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
        return;
    }
    __int128 s = 0;
    unsigned __int128 q = 0; /* squares: up to 2^126 each, so a carry word */
    uint64_t qh = 0;
    int64_t mn = a->imin, mx = a->imax;
    uint64_t c = 0;
    for (size_t i = 0; i < n; i++) {
        int64_t x = v[i];
        if (x == NULL_I64)
            continue;
        c++;
        s += x;
        uint64_t u = x < 0 ? 0 - (uint64_t)x : (uint64_t)x;
        unsigned __int128 sq = (unsigned __int128)u * u;
        q += sq;
        qh += q < sq;
        mn = x < mn ? x : mn;
        mx = x > mx ? x : mx;
    }
    a->count += c;
    a->isum += s;
    isq_add(dx, q, qh);
    a->imin = mn;
    a->imax = mx;
}

/* bool (0, 1) and state codes: 4-byte slots with their own null */
static void fold_u32(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx)
{
    if (simd_fold_u32 && n >= U32_KERNEL_MIN) {
        simd_fold_u32(v, n, null, a, dx);
        return;
    }
    if (!dx) {
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
        return;
    }
    int64_t s = 0, mn = a->imin, mx = a->imax;
    unsigned __int128 q = 0; /* squares below 2^64: no carry word for 2^64 of them */
    uint64_t c = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t x = v[i];
        if (x == null)
            continue;
        c++;
        s += x;
        q += (uint64_t)x * x;
        mn = (int64_t)x < mn ? (int64_t)x : mn;
        mx = (int64_t)x > mx ? (int64_t)x : mx;
    }
    a->count += c;
    a->isum += s;
    isq_add(dx, q, 0);
    a->imin = mn;
    a->imax = mx;
}

void agg_fold_hint(int type, const void *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x)
{
    a->rows += n;
    switch (type) {
    case TSDB_FLOAT32: fold_f32(v, n, may_be_null, a, x); break;
    case TSDB_FLOAT64: fold_f64(v, n, may_be_null, a, x); break;
    case TSDB_INT64: fold_i64(v, n, a, x); break;
    case TSDB_BOOL: fold_u32(v, n, NULL_BOOL, a, x); break;
    default: fold_u32(v, n, NULL_STATE, a, x); break;
    }
}

void agg_fold(int type, const void *v, size_t n, agg_state *a, agg_dev *x) { agg_fold_hint(type, v, n, true, a, x); }

/* LTTB counts a granule's points from its index entry only when fsum is finite:
 * NaN and infinities must keep reaching the float sum here, or that count takes
 * them for points and every LTTB query over them fails with CORRUPT. */
void granule_stats(int type, const void *v, size_t n, granule *g)
{
    agg_state a;
    agg_dev x;
    agg_init(&a, type);
    agg_dev_init(&x);
    agg_fold(type, v, n, &a, &x);
    g->nulls = (uint32_t)(n - a.count);
    if (type_is_int(type)) {
        g->imin = a.imin;
        g->imax = a.imax;
        g->isum_lo = (uint64_t)a.isum;
        g->isum_hi = (int64_t)(a.isum >> 64);
        g->isq0 = (uint64_t)x.isq;
        g->isq1 = (uint64_t)(x.isq >> 64);
        g->isq2 = x.isq_hi;
    } else {
        g->fmin = a.min;
        g->fmax = a.max;
        g->fsum = canon_nan(a.sum); /* a merge rewriting the granule writes the same bits */
        g->isum_hi = 0;
        g->fm2 = canon_nan(x.m2);
        g->isq1 = g->isq2 = 0;
    }
}

void agg_from_granule(int type, const granule *g, agg_state *a, agg_dev *x)
{
    agg_init(a, type);
    a->rows = g->count;
    a->count = g->count - g->nulls;
    if (type_is_int(type)) {
        a->isum = (__int128)(((unsigned __int128)(uint64_t)g->isum_hi << 64) | g->isum_lo);
        a->imin = g->imin;
        a->imax = g->imax;
        if (x) {
            x->isq = ((unsigned __int128)g->isq1 << 64) | g->isq0;
            x->isq_hi = g->isq2;
        }
    } else {
        a->sum = g->fsum;
        a->min = g->fmin;
        a->max = g->fmax;
        if (x)
            x->m2 = g->fm2;
    }
}
