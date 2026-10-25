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

static inline bool null_f64_at(const double *v, size_t j, uint64_t null)
{
    uint64_t b;
    memcpy(&b, v + j, 8);
    return b == null;
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

/*
 * float64 runs of at most TINY_F64 = 12 values (buckets of a few rows; above, the short-run kernels are faster):
 * every value has a lane of its own (simd.c), so the kernels' result is a fixed tree over the n values: the
 * halving of k = the next power of two >= n lanes, where lanes n to k - 1 hold +0.0 (sums, deviations) or a's
 * bounds (min, max). Evaluated as that tree directly, it costs n - 1 operations per result, like a loop, without
 * the kernels' setup or an indirect call. The padding only ever meets a lane below n in the first halving
 * (k / 2 < n), as the right operand:
 *  - min/max (the second operand wins unless the first is strictly beyond it, MINPD/MAXPD with the accumulator
 *    second): a lane holds a value or a's bound, never beyond the bound, so the padding (the bound) loses and
 *    the lane is kept as it is. Its order also decides which of -0.0 and +0.0 wins: kept exactly.
 *  - sums: x + +0.0 is x except for x = -0.0. A kernel lane starts at +0.0, so no lane and no sum of lanes is
 *    ever -0.0; leaving out the +0.0s (here and in each lane's first addition) changes only the sign of a zero
 *    result, and only to -0.0 when every value is -0.0: adding +0.0 to the root gives the kernels' bits. The
 *    deviations' sign does not matter (d1 is squared, and d * d is never -0.0).
 * A null adds +0.0 to its lane and is NaN to min/max, as in the kernels; overflow to inf and NaN follow the same
 * tree. test_levels compares this with every set's kernels at every n, signed zeros and bounds included.
 */
#define TINY_F64 12
_Static_assert(TINY_F64 <= SIMD_LANES_F64 && SIMD_LANES_F64 == 16, "one value per lane, a tree of up to 16");

/* The lanes as plain variables (GCC turned arrays of them into stack slots and reloaded pairs as vectors, a store
 * forwarding stall per call). min/max: lane j holds x_j if x_j < a->min, else a->min itself (NaN: a->min), and a
 * halving step keeps the lower lane unless the upper one is strictly less; so the tree returns, among the lanes
 * holding the least value, the one first in the tree's priority order (the bit-reversed order: 0, 8, 4, 12, 2,
 * 10, ... for sixteen lanes; 0, 2, 1, 3 for four), and a->min when no value is below it. A chain from a->min through the values in that order, each
 * replacing the bound only when strictly less, returns the same lane: n operations instead of 2n. */
/* MINSD/MAXSD (x < y ? x : y, x > y ? x : y) in asm: GCC turned the conditionals into branches, and the
 * intrinsics into extra register moves (zeroing the upper lane) */
#ifdef __AVX__
#define TMINMAX(op, x, y)                                                                                          \
    ({                                                                                                             \
        double r_;                                                                                                 \
        __asm__("v" op " %2, %1, %0" : "=x"(r_) : "x"(x), "x"(y));                                                 \
        r_;                                                                                                        \
    })
#else
#define TMINMAX(op, x, y)                                                                                          \
    ({                                                                                                             \
        double r_ = (x);                                                                                           \
        __asm__(op " %1, %0" : "+x"(r_) : "x"(y));                                                                 \
        r_;                                                                                                        \
    })
#endif
#define TMIN(x, y) TMINMAX("minsd", x, y)
#define TMAX(x, y) TMINMAX("maxsd", x, y)
#define TLEAF(j)                                                                                                   \
    double x##j = v[(j) < n ? (j) : 0], s##j = x##j; /* lanes from n on: unused, optimised out */                \
    bool z##j = (j) < n && null_f64_at(v, j, nl); /* the bits from memory: no move from an xmm */          \
    if (z##j) { /* a null adds +0.0 to its lane (rare: a branch, as in main's loop) */                    \
        s##j = 0.0;                                                                                                \
        c--;                                                                                                       \
    }
#define TDEV(j) double e##j = z##j ? 0.0 : x##j - m, q##j = e##j * e##j;
/* r = the tree over lanes 0..n-1 of l (n = 1 to 16): a halving pairs lane j with lane j + 8, then j + 4, j + 2 and
 * j + 1, where there is one (lanes from n on are padding, left out) */
#define TH(a, b, k) ((k) < n ? (a) + (b) : (a))
#define TTREE(l, r)                                                                                                \
    double r;                                                                                                      \
    {                                                                                                              \
        double h0 = TH(l##0, l##8, 8), h1 = TH(l##1, l##9, 9), h2 = TH(l##2, l##10, 10), h3 = TH(l##3, l##11, 11);  \
        double h4 = TH(l##4, l##12, 12), h5 = TH(l##5, l##13, 13), h6 = TH(l##6, l##14, 14);                       \
        double h7 = TH(l##7, l##15, 15);                                                                           \
        double g0 = TH(h0, h4, 4), g1 = TH(h1, h5, 5), g2 = TH(h2, h6, 6), g3 = TH(h3, h7, 7);                     \
        double f0 = TH(g0, g2, 2), f1 = TH(g1, g3, 3);                                                             \
        r = TH(f0, f1, 1);                                                                                         \
    }
#define TCHAIN(j)                                                                                                  \
    if ((j) < n) {                                                                                                 \
        mn = TMIN(x##j, mn);                                                                                       \
        mx = TMAX(x##j, mx);                                                                                       \
    }
#define TLANES(m) m(0) m(1) m(2) m(3) m(4) m(5) m(6) m(7) m(8) m(9) m(10) m(11) m(12) m(13) m(14) m(15)

static inline __attribute__((always_inline)) void tiny_f64_n(const double *v, const int n, agg_state *a,
                                                             agg_dev *dv)
{
    uint64_t c = (uint64_t)n, nl = NULL_F64;
    __asm__("" : "+r"(nl)); /* the null in one register (GCC loaded the 64-bit constant again for every value) */
    TLANES(TLEAF)
    double mn = TMIN(x0, a->min), mx = TMAX(x0, a->max);
    TCHAIN(8) TCHAIN(4) TCHAIN(12) TCHAIN(2) TCHAIN(10) TCHAIN(6) TCHAIN(14) /* the bit-reversed order */
    TCHAIN(1) TCHAIN(9) TCHAIN(5) TCHAIN(13) TCHAIN(3) TCHAIN(11) TCHAIN(7) TCHAIN(15)
    a->min = mn;
    a->max = mx;
    TTREE(s, sum0)
    double sum = sum0 + 0.0; /* -0.0 to +0.0 (above) */
    if (!dv) {
        a->count += c;
        a->sum = canon_nan(a->sum + sum);
        return;
    }
    if (!c)
        return;
    double m = sum / (double)c;
    TLANES(TDEV)
    TTREE(e, d1)
    TTREE(q, d2)
    agg_add_run(a, dv, c, sum, d1, d2);
    a->sum = canon_nan(a->sum);
}

static void fold_f64(const double *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x)
{
    /* inlined, n a constant in each, behind compares (a jump table or an indirect call cost more); nulls are looked
     * for anyway */
    if (n <= TINY_F64) {
        if (!n)
            return;
        if (n >= 7) {
            if (n >= 10) {
                if (n >= 11) {
                    if (n >= 12) {
                        tiny_f64_n(v, 12, a, x);
                    } else {
                        tiny_f64_n(v, 11, a, x);
                    }
                } else {
                    tiny_f64_n(v, 10, a, x);
                }
            } else {
                if (n >= 8) {
                    if (n >= 9) {
                        tiny_f64_n(v, 9, a, x);
                    } else {
                        tiny_f64_n(v, 8, a, x);
                    }
                } else {
                    tiny_f64_n(v, 7, a, x);
                }
            }
        } else {
            if (n >= 4) {
                if (n >= 5) {
                    if (n >= 6) {
                        tiny_f64_n(v, 6, a, x);
                    } else {
                        tiny_f64_n(v, 5, a, x);
                    }
                } else {
                    tiny_f64_n(v, 4, a, x);
                }
            } else {
                if (n >= 2) {
                    if (n >= 3) {
                        tiny_f64_n(v, 3, a, x);
                    } else {
                        tiny_f64_n(v, 2, a, x);
                    }
                } else {
                    tiny_f64_n(v, 1, a, x);
                }
            }
        }
        return;
    }
    if (n < simd_short_f64)
        simd_small_f64(v, n, may_be_null, a, x);
    else
        simd_fold_f64(v, n, may_be_null ? count_null_f64((const uint64_t *)v, n) : 0, a, x);
}

/* The loops below the kernels' thresholds, out of line: inlined into agg_fold_hint behind the dispatch check, GCC
 * rematerialised the 64-bit null (movabs) on every row, 7-9% slower than main's same loop. */
__attribute__((noinline)) static void fold_i64_dev(const int64_t *v, size_t n, agg_state *a, agg_dev *dx)
{
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
        qh += __builtin_add_overflow(q, sq, &q); /* add, adc, adc: no compare of 128-bit values */
        mn = x < mn ? x : mn;
        mx = x > mx ? x : mx;
    }
    a->count += c;
    a->isum += s;
    isq_add(dx, q, qh);
    a->imin = mn;
    a->imax = mx;
}

__attribute__((noinline)) static void fold_i64_agg(const int64_t *v, size_t n, agg_state *a)
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
__attribute__((noinline)) static void fold_u32_agg(const uint32_t *v, size_t n, uint32_t null, agg_state *a)
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

__attribute__((noinline)) static void fold_u32_dev(const uint32_t *v, size_t n, uint32_t null, agg_state *a,
                                                   agg_dev *dx)
{
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

/* the thresholds are SIZE_MAX where a set has no such kernel: one compare decides */
static inline void fold_i64(const int64_t *v, size_t n, agg_state *a, agg_dev *dx)
{
    if (n >= (dx ? simd_i64_dev_min : simd_i64_min))
        simd_fold_i64(v, n, a, dx);
    else if (dx)
        fold_i64_dev(v, n, a, dx);
    else
        fold_i64_agg(v, n, a);
}

static inline void fold_u32(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx)
{
    if (n >= simd_u32_min)
        simd_fold_u32(v, n, null, a, dx);
    else if (dx)
        fold_u32_dev(v, n, null, a, dx);
    else
        fold_u32_agg(v, n, null, a);
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
