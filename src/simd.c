/*
 * Aggregation kernels with runtime dispatch, five sets: AVX-512 (v4), AVX2
 * (v3), AVX without AVX2 (Sandy Bridge, Ivy Bridge), SSE4.2 (x86-64-v2) and
 * scalar. The AVX-512 kernels are written here, the AVX2 and AVX ones for
 * float32 and float64 in simd_avx.inc (included once per set), the SSE ones,
 * and AVX's integer, TWA and LTTB kernels (AVX has no 256-bit integer
 * operations: the same 128-bit code, VEX-encoded), in simd_sse.inc.
 *
 * Gorilla and DoubleDelta are bit-serial (each value's bit position depends on
 * the previous one), so they are decoded with scalar code. What vectorizes is
 * everything after decoding: count/sum/min/max over the values, and the
 * spread's deviations (floats) or squares (integers). float32 and float64 have
 * a scalar kernel that shares the vector kernels' order (below); int64, bool
 * and state codes are exact, so their scalar reference is values.c's loop and
 * the vector kernels may add in any order (further down). Further: a
 * granule's time-weighted areas (float64 lanes by segment) and LTTB's argmax
 * (the first largest area), each identical on every path as well.
 *
 * min/max follow MINPS/MAXPS semantics with the accumulator as the second
 * operand, so NaN inputs are ignored; NaN still propagates into the sum.
 *
 * Nulls are reserved values (internal.h): for floats a NaN with a payload, so
 * min/max ignore them already. The sums take +0.0 in their place, in the null's
 * own lane, without a branch: a lane starts at +0.0 and an addition rounding to
 * nearest never turns it into -0.0, so adding +0.0 leaves it as it is and the
 * order stays by position (lane j: positions j, j + LANES, ...). The same for
 * the spread's deviations. The caller counts the nulls first (values.c) and
 * passes the count; with none the kernels do no masking at all.
 */
#include "internal.h"

#include <math.h>
#include <stdlib.h>

#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#include <immintrin.h>
#define TSDB_X86 1
#endif

/*
 * All five sets give bit-identical results: stored granule statistics and
 * query results must not depend on the CPU that computed them (a part written
 * on one machine is read on another; `make dist` runs the same binary on v2,
 * v3 and v4 CPUs). Floating-point addition is not associative, so they share
 * one order: 32 double lanes, lane j summing values j, j+32, j+64, ... in turn,
 * then halved (lane j + lane j+16, then j + j+8, ...). 32 lanes because that
 * is what AVX-512 needs to hide the latency of its adds and min/max (chains of
 * 16 floats or 8 doubles) and AVX2 can just hold in its 16 registers. min and
 * max keep 32 float lanes the same way, combined with MINPS/MAXPS semantics
 * (the second operand wins on NaN and on equal values, which decides the sign
 * of a zero). A last partial group of 32 goes into the lanes one value at a time.
 * AVX2 and AVX hold the 32 lanes in 8 vectors of four doubles (sums) and 4 of
 * eight floats (min/max), SSE in 16 of two and 8 of four: lane j is always
 * value j of each group of 32, whatever the width.
 *
 * float64 takes the same scheme with 16 lanes (L64): lane j sums values j,
 * j+16, ...; halved 8, 4, 2, 1. 16 because AVX2 then still holds sums, minima
 * and maxima (12 vectors) and the null mask in its 16 registers, and AVX-512
 * runs two chains each; the scalar code keeps the same lanes.
 *
 * The spread takes a second pass (see agg_state), only where it is asked for: with the mean m of the run
 * (its sum, reduced as above, over n), lane j sums d = (double)x - m and d * d
 * for the same values j, j+32, ..., reduced by the same halving. The kernels
 * share that order too, and -ffp-contract=off keeps d * d from fusing into an
 * FMA, so m2 is bit-identical across them: granule statistics are written with
 * it. (What is not reproducible is the combination of runs, as for sums: it
 * follows merges and threads.)
 */
#define LANES SIMD_LANES_F32
#define INL static inline __attribute__((always_inline))

typedef struct lanes {
    double s[LANES];
    float mn[LANES], mx[LANES];
} lanes;

static inline bool null_f32(float x)
{
    uint32_t b;
    memcpy(&b, &x, 4);
    return b == NULL_F32;
}

static inline bool null_f64(double x)
{
    uint64_t b;
    memcpy(&b, &x, 8);
    return b == NULL_F64;
}

static void lanes_init(lanes *l, const agg_state *a)
{
    for (int j = 0; j < LANES; j++) {
        l->s[j] = 0;
        l->mn[j] = (float)a->min; /* the state holds float32 values here */
        l->mx[j] = (float)a->max;
    }
}

typedef struct dev_lanes {
    double d1[LANES], d2[LANES];
} dev_lanes;

/* the halving reduction of k lanes: lane j + lane j + k/2, then j + j + k/4, ... */
static double lanes_reduce(double *s, int k)
{
    for (int w = k / 2; w >= 1; w /= 2)
        for (int j = 0; j < w; j++)
            s[j] = s[j] + s[j + w];
    return s[0];
}

/* pass two, values [i, n) with n - i < LANES, then the reduction; and the run of c known values into a */
INL void dev_finish(dev_lanes *l, const float *v, size_t i, size_t n, double m, uint64_t c, double s,
                    agg_state *a, agg_dev *dv, const bool nl, const int k)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        float x = v[i + (size_t)j];
        double d = nl && null_f32(x) ? 0.0 : (double)x - m;
        l->d1[j] += d;
        l->d2[j] += d * d;
    }
    double d1 = lanes_reduce(l->d1, k), d2 = lanes_reduce(l->d2, k);
    agg_add_run(a, dv, c, s, d1, d2);
    a->sum = canon_nan(a->sum);
}

/* the run of c known values into a without the spread: count and sum only */
static void sum_finish(uint64_t c, double s, agg_state *a)
{
    a->count += c;
    a->sum = canon_nan(a->sum + s);
}

INL void dev_scalar(const float *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev_lanes l = {0};
    double m = s / (double)c;
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int j = 0; j < LANES; j++) {
            float x = v[i + (size_t)j];
            double d = nl && null_f32(x) ? 0.0 : (double)x - m;
            l.d1[j] += d;
            l.d2[j] += d * d;
        }
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl, LANES);
}

/* values [i, n) with n - i < LANES, then the reduction of the first k lanes (LANES, or fewer for a short run,
 * see small_f32): min and max into a; returns the sum */
INL double lanes_finish(lanes *l, const float *v, size_t i, size_t n, agg_state *a, const bool nl, const int k)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        float x = v[i + (size_t)j];
        l->s[j] += nl && null_f32(x) ? 0.0 : (double)x;
        l->mn[j] = x < l->mn[j] ? x : l->mn[j];
        l->mx[j] = x > l->mx[j] ? x : l->mx[j];
    }
    for (int w = k / 2; w >= 1; w /= 2)
        for (int j = 0; j < w; j++) {
            l->s[j] = l->s[j] + l->s[j + w];
            l->mn[j] = l->mn[j + w] < l->mn[j] ? l->mn[j + w] : l->mn[j];
            l->mx[j] = l->mx[j + w] > l->mx[j] ? l->mx[j + w] : l->mx[j];
        }
    a->min = l->mn[0] < a->min ? l->mn[0] : a->min;
    a->max = l->mx[0] > a->max ? l->mx[0] : a->max;
    return l->s[0];
}

INL void agg_scalar_impl(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes l;
    lanes_init(&l, a);
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int j = 0; j < LANES; j++) {
            float x = v[i + (size_t)j];
            l.s[j] += nl && null_f32(x) ? 0.0 : (double)x;
            l.mn[j] = x < l.mn[j] ? x : l.mn[j];
            l.mx[j] = x > l.mx[j] ? x : l.mx[j];
        }
    double s = lanes_finish(&l, v, i, n, a, nl, LANES);
    if (dv)
        dev_scalar(v, n, n - nulls, s, a, dv, nl);
    else
        sum_finish(n - nulls, s, a);
}

static void agg_scalar(const float *v, size_t n, agg_state *a, agg_dev *dv) { agg_scalar_impl(v, n, 0, a, dv, false); }

static void fold_f32_scalar(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        agg_scalar_impl(v, n, nulls, a, dv, true);
    else
        agg_scalar_impl(v, n, 0, a, dv, false);
}

/* ---------------------------------------------------------------- float64 */

#define L64 SIMD_LANES_F64

typedef struct lanes64 {
    double s[L64], mn[L64], mx[L64];
} lanes64;

typedef struct dev64 {
    double d1[L64], d2[L64];
} dev64;

static void lanes64_init(lanes64 *l, const agg_state *a)
{
    for (int j = 0; j < L64; j++) {
        l->s[j] = 0;
        l->mn[j] = a->min;
        l->mx[j] = a->max;
    }
}

/* values [i, n) with n - i < L64, then the reduction of the first k lanes (as lanes_finish): min and max into
 * a; returns the sum */
INL double lanes64_finish(lanes64 *l, const double *v, size_t i, size_t n, agg_state *a, const bool nl, const int k)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        double x = v[i + (size_t)j];
        l->s[j] += nl && null_f64(x) ? 0.0 : x;
        l->mn[j] = x < l->mn[j] ? x : l->mn[j];
        l->mx[j] = x > l->mx[j] ? x : l->mx[j];
    }
    for (int w = k / 2; w >= 1; w /= 2)
        for (int j = 0; j < w; j++) {
            l->s[j] = l->s[j] + l->s[j + w];
            l->mn[j] = l->mn[j + w] < l->mn[j] ? l->mn[j + w] : l->mn[j];
            l->mx[j] = l->mx[j + w] > l->mx[j] ? l->mx[j + w] : l->mx[j];
        }
    a->min = l->mn[0] < a->min ? l->mn[0] : a->min;
    a->max = l->mx[0] > a->max ? l->mx[0] : a->max;
    return l->s[0];
}

INL void dev64_finish(dev64 *l, const double *v, size_t i, size_t n, double m, uint64_t c, double s, agg_state *a,
                      agg_dev *dv, const bool nl, const int k)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        double x = v[i + (size_t)j];
        double d = nl && null_f64(x) ? 0.0 : x - m;
        l->d1[j] += d;
        l->d2[j] += d * d;
    }
    double d1 = lanes_reduce(l->d1, k), d2 = lanes_reduce(l->d2, k);
    agg_add_run(a, dv, c, s, d1, d2);
    a->sum = canon_nan(a->sum);
}

INL void dev64_scalar(const double *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev64 l = {0};
    double m = s / (double)c;
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int j = 0; j < L64; j++) {
            double x = v[i + (size_t)j];
            double d = nl && null_f64(x) ? 0.0 : x - m;
            l.d1[j] += d;
            l.d2[j] += d * d;
        }
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl, L64);
}

INL void agg64_scalar_impl(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes64 l;
    lanes64_init(&l, a);
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int j = 0; j < L64; j++) {
            double x = v[i + (size_t)j];
            l.s[j] += nl && null_f64(x) ? 0.0 : x;
            l.mn[j] = x < l.mn[j] ? x : l.mn[j];
            l.mx[j] = x > l.mx[j] ? x : l.mx[j];
        }
    double s = lanes64_finish(&l, v, i, n, a, nl, L64);
    if (dv)
        dev64_scalar(v, n, n - nulls, s, a, dv, nl);
    else
        sum_finish(n - nulls, s, a);
}

static void fold_f64_scalar(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        agg64_scalar_impl(v, n, nulls, a, dv, true);
    else
        agg64_scalar_impl(v, n, 0, a, dv, false);
}

/* ------------------------------------------------------ short runs */

/*
 * A run shorter than the lanes (a bucket of a few rows: query.c's fold_run
 * folds once per bucket) puts value j alone into lane j; every lane from n on
 * stays +0.0 for the sums and the deviations and at a's min and max. Such a
 * lane changes nothing in the halving: x + +0.0 is x, since no lane is ever
 * -0.0 (above), and min/max keep the other operand, which is a's own bound or
 * a value beyond it. So the reduction can start at the smallest power of two
 * k >= n, with the same bits as the kernels' full reduction, and without their
 * fixed cost (setting up and reducing 32 lanes, the vector setup, the null
 * count as a pass of its own), which is larger than the fold itself for a few
 * values. values.c and twa.c take these below SIMD_LANES_F32 / _F64 on every
 * CPU level; test_levels compares them with the kernels.
 */
static int lanes_for(size_t n)
{
    int k = 1;
    while ((size_t)k < n)
        k *= 2;
    return k;
}

/*
 * K (the lanes in use) is a constant in each copy, so the loops unroll and the lanes stay in registers: a loop
 * over a variable number of lanes kept them in memory and cost more than the kernels it replaces. The null
 * count comes from the same loop (a null adds +0.0 to its lane as in the kernels; with nl false, none is
 * looked for). T is float or double; F32 says which, for the float32 lanes of min and max.
 */
#define SMALL_FOLD(NAME, T, MT, NULLF, NL_LANES)                                                              \
    INL void NAME##_k(const T *v, size_t n, agg_state *a, agg_dev *dv, const bool nl, const int K, const int R)\
    {                                                                                                       \
        double s[NL_LANES];                                                                                 \
        MT mn[NL_LANES], mx[NL_LANES];                                                                      \
        const MT lo = (MT)a->min, hi = (MT)a->max;                                                          \
        size_t nulls = 0;                                                                                   \
        _Pragma("GCC unroll 32") for (int j = 0; j < K; j++)                                                \
        {                                                                                                   \
            s[j] = 0.0;                                                                                     \
            mn[j] = lo;                                                                                     \
            mx[j] = hi;                                                                                     \
            _Pragma("GCC unroll 4") for (int r = 0; r < R; r++)                                             \
            {                                                                                               \
                size_t i = (size_t)(r * K + j);                                                      \
                bool in = i < n;                                                                            \
                T x = in ? v[i] : 0;                                                                        \
                bool nul = in && nl && NULLF(x);                                                            \
                nulls += nul;                                                                               \
                s[j] += in && !nul ? (double)x : 0.0;                                                       \
                mn[j] = in && x < mn[j] ? x : mn[j];                                                        \
                mx[j] = in && x > mx[j] ? x : mx[j];                                                        \
            }                                                                                               \
        }                                                                                                   \
        _Pragma("GCC unroll 8") for (int w = K / 2; w >= 1; w /= 2)                                         \
            _Pragma("GCC unroll 16") for (int j = 0; j < w; j++)                                            \
        {                                                                                                   \
            s[j] = s[j] + s[j + w];                                                                         \
            mn[j] = mn[j + w] < mn[j] ? mn[j + w] : mn[j];                                                  \
            mx[j] = mx[j + w] > mx[j] ? mx[j + w] : mx[j];                                                  \
        }                                                                                                   \
        a->min = mn[0] < a->min ? mn[0] : a->min;                                                           \
        a->max = mx[0] > a->max ? mx[0] : a->max;                                                           \
        uint64_t c = n - nulls;                                                                             \
        if (!dv) {                                                                                          \
            sum_finish(c, s[0], a);                                                                         \
            return;                                                                                         \
        }                                                                                                   \
        if (!c)                                                                                             \
            return;                                                                                         \
        double m = s[0] / (double)c, d1[NL_LANES], d2[NL_LANES];                                            \
        _Pragma("GCC unroll 32") for (int j = 0; j < K; j++)                                                \
        {                                                                                                   \
            d1[j] = d2[j] = 0.0;                                                                            \
            _Pragma("GCC unroll 4") for (int r = 0; r < R; r++)                                             \
            {                                                                                               \
                size_t i = (size_t)(r * K + j);                                                      \
                bool in = i < n;                                                                            \
                T x = in ? v[i] : 0;                                                                        \
                double d = in && !(nl && NULLF(x)) ? (double)x - m : 0.0;                                   \
                d1[j] += d;                                                                                 \
                d2[j] += d * d;                                                                             \
            }                                                                                               \
        }                                                                                                   \
        _Pragma("GCC unroll 8") for (int w = K / 2; w >= 1; w /= 2)                                         \
            _Pragma("GCC unroll 16") for (int j = 0; j < w; j++)                                            \
        {                                                                                                   \
            d1[j] = d1[j] + d1[j + w];                                                                      \
            d2[j] = d2[j] + d2[j + w];                                                                      \
        }                                                                                                   \
        agg_add_run(a, dv, c, s[0], d1[0], d2[0]);                                                          \
        a->sum = canon_nan(a->sum);                                                                         \
    }

SMALL_FOLD(small_f32, float, float, null_f32, 16)
SMALL_FOLD(small_f64, double, double, null_f64, 16)

/* n < 32: up to 16 lanes of one value, or 16 of two (values j and j + 16). For float32 (32 lanes), 16 < n < 32
 * is the same: the first halving adds lane j + 16 (one value, never -0.0 after +0.0 + x) to lane j, which is
 * what adding the value itself does, as an empty lane's +0.0 does nothing; min and max likewise. */
#define SMALL_SWITCH(NAME, NL)                                                                              \
    switch (n <= 16 ? lanes_for(n) : 32) {                                                                  \
    case 1: NAME##_k(v, n, a, dv, NL, 1, 1); break;                                                         \
    case 2: NAME##_k(v, n, a, dv, NL, 2, 1); break;                                                         \
    case 4: NAME##_k(v, n, a, dv, NL, 4, 1); break;                                                         \
    case 8: NAME##_k(v, n, a, dv, NL, 8, 1); break;                                                         \
    case 16: NAME##_k(v, n, a, dv, NL, 16, 1); break;                                                       \
    default: NAME##_k(v, n, a, dv, NL, 16, 2); break;                                                       \
    }

static void small_f32_scalar(const float *v, size_t n, bool may_be_null, agg_state *a, agg_dev *dv)
{
    if (!n)
        return;
    if (may_be_null)
        SMALL_SWITCH(small_f32, true)
    else
        SMALL_SWITCH(small_f32, false)
}

static void small_f64_scalar(const double *v, size_t n, bool may_be_null, agg_state *a, agg_dev *dv)
{
    if (!n)
        return;
    if (may_be_null)
        SMALL_SWITCH(small_f64, true)
    else
        SMALL_SWITCH(small_f64, false)
}

#ifdef TSDB_X86
#define K_AVX2 static inline __attribute__((target("avx2"), always_inline))
#define K_AVX512 static inline __attribute__((target("avx512f,avx512dq"), always_inline))

/* the 256-bit float32 and float64 kernels: AVX2, and AVX without AVX2 (Sandy Bridge, Ivy Bridge) */
#define KSUF avx2
#define KTARGET "avx2"
#define KS_AVX2 1
#include "simd_avx.inc"
#define KSUF avx
#define KTARGET "avx"
#define KS_AVX2 0
#include "simd_avx.inc"

K_AVX512 void dev_avx512(const float *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev_lanes l;
    double m = s / (double)c;
    const __m512d mm = _mm512_set1_pd(m);
    const __m256i nv = _mm256_set1_epi32((int)NULL_F32);
    __m512d d1[4], d2[4]; /* lanes 8q .. 8q+7 */
    for (int q = 0; q < 4; q++)
        d1[q] = d2[q] = _mm512_setzero_pd();
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int q = 0; q < 4; q++) {
            __m256 x = _mm256_loadu_ps(v + i + 8 * q);
            __m512d d = _mm512_sub_pd(_mm512_cvtps_pd(x), mm);
            if (nl) {
                __mmask8 null =
                    (__mmask8)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_castps_si256(x), nv)));
                d = _mm512_maskz_mov_pd((__mmask8)~null, d);
            }
            d1[q] = _mm512_add_pd(d1[q], d);
            d2[q] = _mm512_add_pd(d2[q], _mm512_mul_pd(d, d));
        }
    for (int q = 0; q < 4; q++) {
        _mm512_storeu_pd(l.d1 + 8 * q, d1[q]);
        _mm512_storeu_pd(l.d2 + 8 * q, d2[q]);
    }
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl, LANES);
}

K_AVX512 void agg_avx512_impl(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes l;
    lanes_init(&l, a);
    const __m512i nv = _mm512_set1_epi32((int)NULL_F32);
    /* sum lanes 0-7, 8-15, 16-23, 24-31 */
    __m512d s0 = _mm512_setzero_pd(), s1 = s0, s2 = s0, s3 = s0;
    __m512 mn = _mm512_loadu_ps(l.mn), mx = _mm512_loadu_ps(l.mx); /* min/max lanes 0-15 */
    __m512 mn2 = mn, mx2 = mx;                                      /* min/max lanes 16-31 */
    size_t i = 0;
    for (; i + LANES <= n; i += LANES) {
        __m512 x = _mm512_loadu_ps(v + i), y = _mm512_loadu_ps(v + i + 16), xs = x, ys = y;
        if (nl) {
            xs = _mm512_maskz_mov_ps(_mm512_cmpneq_epi32_mask(_mm512_castps_si512(x), nv), x);
            ys = _mm512_maskz_mov_ps(_mm512_cmpneq_epi32_mask(_mm512_castps_si512(y), nv), y);
        }
        s0 = _mm512_add_pd(s0, _mm512_cvtps_pd(_mm512_castps512_ps256(xs)));
        s1 = _mm512_add_pd(s1, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(xs), 1))));
        s2 = _mm512_add_pd(s2, _mm512_cvtps_pd(_mm512_castps512_ps256(ys)));
        s3 = _mm512_add_pd(s3, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(ys), 1))));
        mn = _mm512_min_ps(x, mn);
        mx = _mm512_max_ps(x, mx);
        mn2 = _mm512_min_ps(y, mn2);
        mx2 = _mm512_max_ps(y, mx2);
    }
    _mm512_storeu_pd(l.s, s0);
    _mm512_storeu_pd(l.s + 8, s1);
    _mm512_storeu_pd(l.s + 16, s2);
    _mm512_storeu_pd(l.s + 24, s3);
    _mm512_storeu_ps(l.mn, mn);
    _mm512_storeu_ps(l.mn + 16, mn2);
    _mm512_storeu_ps(l.mx, mx);
    _mm512_storeu_ps(l.mx + 16, mx2);
    double s = lanes_finish(&l, v, i, n, a, nl, LANES);
    if (dv)
        dev_avx512(v, n, n - nulls, s, a, dv, nl);
    else
        sum_finish(n - nulls, s, a);
}

__attribute__((target("avx512f,avx512dq"))) static void agg_avx512(const float *v, size_t n, agg_state *a,
                                                                   agg_dev *dv)
{
    agg_avx512_impl(v, n, 0, a, dv, false);
}

__attribute__((target("avx512f,avx512dq"))) static void fold_f32_avx512(const float *v, size_t n, size_t nulls,
                                                                        agg_state *a, agg_dev *dv)
{
    if (nulls)
        agg_avx512_impl(v, n, nulls, a, dv, true);
    else
        agg_avx512_impl(v, n, 0, a, dv, false);
}

K_AVX512 void dev64_avx512(const double *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv,
                           const bool nl)
{
    if (!c)
        return;
    dev64 l;
    double m = s / (double)c;
    const __m512d mm = _mm512_set1_pd(m);
    const __m512i nv = _mm512_set1_epi64((long long)NULL_F64);
    __m512d d1[2], d2[2];
    for (int q = 0; q < 2; q++)
        d1[q] = d2[q] = _mm512_setzero_pd();
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 2; q++) {
            __m512d x = _mm512_loadu_pd(v + i + 8 * q), d = _mm512_sub_pd(x, mm);
            if (nl)
                d = _mm512_maskz_mov_pd(_mm512_cmpneq_epi64_mask(_mm512_castpd_si512(x), nv), d);
            d1[q] = _mm512_add_pd(d1[q], d);
            d2[q] = _mm512_add_pd(d2[q], _mm512_mul_pd(d, d));
        }
    for (int q = 0; q < 2; q++) {
        _mm512_storeu_pd(l.d1 + 8 * q, d1[q]);
        _mm512_storeu_pd(l.d2 + 8 * q, d2[q]);
    }
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl, L64);
}

K_AVX512 void agg64_avx512_impl(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes64 l;
    lanes64_init(&l, a);
    const __m512i nv = _mm512_set1_epi64((long long)NULL_F64);
    __m512d s[2], mn[2], mx[2]; /* lanes 8q .. 8q+7 */
    for (int q = 0; q < 2; q++) {
        s[q] = _mm512_setzero_pd();
        mn[q] = _mm512_loadu_pd(l.mn + 8 * q);
        mx[q] = _mm512_loadu_pd(l.mx + 8 * q);
    }
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 2; q++) {
            __m512d x = _mm512_loadu_pd(v + i + 8 * q), xs = x;
            if (nl)
                xs = _mm512_maskz_mov_pd(_mm512_cmpneq_epi64_mask(_mm512_castpd_si512(x), nv), x);
            s[q] = _mm512_add_pd(s[q], xs);
            mn[q] = _mm512_min_pd(x, mn[q]);
            mx[q] = _mm512_max_pd(x, mx[q]);
        }
    for (int q = 0; q < 2; q++) {
        _mm512_storeu_pd(l.s + 8 * q, s[q]);
        _mm512_storeu_pd(l.mn + 8 * q, mn[q]);
        _mm512_storeu_pd(l.mx + 8 * q, mx[q]);
    }
    double sum = lanes64_finish(&l, v, i, n, a, nl, L64);
    if (dv)
        dev64_avx512(v, n, n - nulls, sum, a, dv, nl);
    else
        sum_finish(n - nulls, sum, a);
}

__attribute__((target("avx512f,avx512dq"))) static void fold_f64_avx512(const double *v, size_t n, size_t nulls,
                                                                        agg_state *a, agg_dev *dv)
{
    if (nulls)
        agg64_avx512_impl(v, n, nulls, a, dv, true);
    else
        agg64_avx512_impl(v, n, 0, a, dv, false);
}
#endif

/* ---------------------------------------------------------------- int64 */

/*
 * Integer results are exact, so the order of the additions is free and these
 * kernels need not share lanes with anything; the scalar loops of values.c
 * (v2, TSDB_SIMD=scalar) are the reference. The sum: a value x is
 * lo + 2^32 hi - 2^64 neg, with lo and hi its unsigned 32-bit halves and neg
 * its sign bit. 64-bit lane sums of lo, hi and neg cannot overflow within
 * 2^32 values per lane; they are combined into 128 bits per block of
 * I64_BLOCK vectors. A null (INT64_MIN) is masked to 0 for the sums and to
 * INT64_MAX for the minimum; for the maximum it is the identity already.
 * The spread's squares take a second pass: when the run's known values all
 * fit in int32 one multiply each (a square below 2^62, summed in halves like
 * the values), else three (i64_sq_wide).
 */
#define I64_BLOCK ((size_t)1 << 26)

/* the 192-bit sum of the squares of v's known values into dx, one at a time */
static void i64_squares(const int64_t *v, size_t i, size_t n, agg_dev *dx)
{
    unsigned __int128 q = 0;
    uint64_t qh = 0;
    for (; i < n; i++) {
        int64_t x = v[i];
        if (x == NULL_I64)
            continue;
        uint64_t u = x < 0 ? 0 - (uint64_t)x : (uint64_t)x;
        unsigned __int128 sq = (unsigned __int128)u * u;
        q += sq;
        qh += q < sq;
    }
    isq_add(dx, q, qh);
}

/* v[i, n) one at a time into the run's sum, known count, min and max */
static void i64_tail(const int64_t *v, size_t i, size_t n, __int128 *s, uint64_t *c, int64_t *mn, int64_t *mx)
{
    for (; i < n; i++) {
        int64_t x = v[i];
        if (x == NULL_I64)
            continue;
        ++*c;
        *s += x;
        *mn = x < *mn ? x : *mn;
        *mx = x > *mx ? x : *mx;
    }
}

static void i64_finish(agg_state *a, __int128 s, uint64_t c, int64_t mn, int64_t mx)
{
    a->count += c;
    a->isum += s;
    a->imin = mn < a->imin ? mn : a->imin;
    a->imax = mx > a->imax ? mx : a->imax;
}

/* The squares of values beyond int32: |x| = h 2^32 + l, so x^2 = l^2 + hl 2^33 + h^2 2^64, each product from
 * a 32x32 multiply and summed in its 32-bit halves: a0, a1 (l^2), b0, b1 (hl), c0, c1 (h^2), reduced over the
 * lanes, below 2^60 each, into the 192-bit sum */
static void i64_sq_wide(agg_dev *dx, uint64_t a0, uint64_t a1, uint64_t b0, uint64_t b1, uint64_t c0, uint64_t c1)
{
    unsigned __int128 t = (unsigned __int128)a0 + ((unsigned __int128)a1 << 32) + ((unsigned __int128)b0 << 33);
    unsigned __int128 h = (unsigned __int128)c0 + ((unsigned __int128)b1 << 1) + ((unsigned __int128)c1 << 32);
    isq_add(dx, t, 0);
    isq_add(dx, (unsigned __int128)(uint64_t)h << 64, (uint64_t)(h >> 64));
}

static inline __int128 i64_combine(const uint64_t *lo, const uint64_t *hi, const uint64_t *neg, int k)
{
    uint64_t l = 0, h = 0, g = 0;
    for (int j = 0; j < k; j++) {
        l += lo[j];
        h += hi[j];
        g += neg[j];
    }
    return (__int128)l + ((__int128)h << 32) - ((__int128)g << 64);
}

#ifdef TSDB_X86
K_AVX2 int64_t hmin4(__m256i x)
{
    int64_t t[4];
    _mm256_storeu_si256((__m256i *)t, x);
    int64_t m = t[0];
    for (int j = 1; j < 4; j++)
        m = t[j] < m ? t[j] : m;
    return m;
}

K_AVX2 int64_t hmax4(__m256i x)
{
    int64_t t[4];
    _mm256_storeu_si256((__m256i *)t, x);
    int64_t m = t[0];
    for (int j = 1; j < 4; j++)
        m = t[j] > m ? t[j] : m;
    return m;
}

__attribute__((target("avx2"))) static void fold_i64_avx2(const int64_t *v, size_t n, agg_state *a, agg_dev *dx)
{
    const __m256i nv = _mm256_set1_epi64x(INT64_MIN), lom = _mm256_set1_epi64x(0xffffffff),
                  maxv = _mm256_set1_epi64x(INT64_MAX);
    __m256i mn0 = maxv, mn1 = maxv, mx0 = nv, mx1 = nv;
    __int128 s = 0;
    uint64_t c = 0;
    size_t i = 0;
    while (n - i >= 8) {
        size_t end = i + ((n - i) / 8 < I64_BLOCK ? (n - i) / 8 : I64_BLOCK) * 8, start = i;
        __m256i slo = _mm256_setzero_si256(), shi = slo, sneg = slo, snull = slo;
#define STEP(x, mn, mx)                                                                                            \
    do {                                                                                                           \
        __m256i m = _mm256_cmpeq_epi64(x, nv), xs = _mm256_andnot_si256(m, x);                                     \
        slo = _mm256_add_epi64(slo, _mm256_and_si256(xs, lom));                                                    \
        shi = _mm256_add_epi64(shi, _mm256_srli_epi64(xs, 32));                                                    \
        sneg = _mm256_add_epi64(sneg, _mm256_srli_epi64(xs, 63));                                                  \
        snull = _mm256_sub_epi64(snull, m);                                                                        \
        __m256i xm = _mm256_blendv_epi8(x, maxv, m);                                                               \
        mn = _mm256_blendv_epi8(mn, xm, _mm256_cmpgt_epi64(mn, xm));                                               \
        mx = _mm256_blendv_epi8(mx, x, _mm256_cmpgt_epi64(x, mx));                                                 \
    } while (0)
        for (; i < end; i += 8) {
            __m256i x = _mm256_loadu_si256((const __m256i *)(v + i));
            __m256i y = _mm256_loadu_si256((const __m256i *)(v + i + 4));
            STEP(x, mn0, mx0);
            STEP(y, mn1, mx1);
        }
#undef STEP
        uint64_t lo[4], hi[4], neg[4], nul[4];
        _mm256_storeu_si256((__m256i *)lo, slo);
        _mm256_storeu_si256((__m256i *)hi, shi);
        _mm256_storeu_si256((__m256i *)neg, sneg);
        _mm256_storeu_si256((__m256i *)nul, snull);
        s += i64_combine(lo, hi, neg, 4);
        c += (end - start) - (nul[0] + nul[1] + nul[2] + nul[3]);
    }
    int64_t mn = hmin4(mn0), mx = hmax4(mx0), mn_1 = hmin4(mn1), mx_1 = hmax4(mx1);
    mn = mn_1 < mn ? mn_1 : mn;
    mx = mx_1 > mx ? mx_1 : mx;
    i64_tail(v, i, n, &s, &c, &mn, &mx);
    i64_finish(a, s, c, mn, mx);
    if (!dx || !c)
        return;
    i = 0;
    if (mn < INT32_MIN || mx > INT32_MAX) {
        while (n - i >= 4) {
            size_t end = i + ((n - i) / 4 < I64_BLOCK ? (n - i) / 4 : I64_BLOCK) * 4;
            __m256i a0 = _mm256_setzero_si256(), a1 = a0, b0 = a0, b1 = a0, c0 = a0, c1 = a0, zero = a0;
            for (; i < end; i += 4) {
                __m256i x = _mm256_loadu_si256((const __m256i *)(v + i));
                __m256i xs = _mm256_andnot_si256(_mm256_cmpeq_epi64(x, nv), x), sg = _mm256_cmpgt_epi64(zero, xs);
                __m256i u = _mm256_sub_epi64(_mm256_xor_si256(xs, sg), sg), h = _mm256_srli_epi64(u, 32);
                __m256i ll = _mm256_mul_epu32(u, u), hl = _mm256_mul_epu32(h, u), hh = _mm256_mul_epu32(h, h);
                a0 = _mm256_add_epi64(a0, _mm256_and_si256(ll, lom));
                a1 = _mm256_add_epi64(a1, _mm256_srli_epi64(ll, 32));
                b0 = _mm256_add_epi64(b0, _mm256_and_si256(hl, lom));
                b1 = _mm256_add_epi64(b1, _mm256_srli_epi64(hl, 32));
                c0 = _mm256_add_epi64(c0, _mm256_and_si256(hh, lom));
                c1 = _mm256_add_epi64(c1, _mm256_srli_epi64(hh, 32));
            }
            uint64_t t[6][4];
            _mm256_storeu_si256((__m256i *)t[0], a0);
            _mm256_storeu_si256((__m256i *)t[1], a1);
            _mm256_storeu_si256((__m256i *)t[2], b0);
            _mm256_storeu_si256((__m256i *)t[3], b1);
            _mm256_storeu_si256((__m256i *)t[4], c0);
            _mm256_storeu_si256((__m256i *)t[5], c1);
            uint64_t r[6];
            for (int j = 0; j < 6; j++)
                r[j] = t[j][0] + t[j][1] + t[j][2] + t[j][3];
            i64_sq_wide(dx, r[0], r[1], r[2], r[3], r[4], r[5]);
        }
        i64_squares(v, i, n, dx);
        return;
    }
    unsigned __int128 q = 0;
    while (n - i >= 4) {
        size_t end = i + ((n - i) / 4 < I64_BLOCK ? (n - i) / 4 : I64_BLOCK) * 4;
        __m256i qlo = _mm256_setzero_si256(), qhi = qlo;
        for (; i < end; i += 4) {
            __m256i x = _mm256_loadu_si256((const __m256i *)(v + i));
            __m256i xs = _mm256_andnot_si256(_mm256_cmpeq_epi64(x, nv), x), sq = _mm256_mul_epi32(xs, xs);
            qlo = _mm256_add_epi64(qlo, _mm256_and_si256(sq, lom));
            qhi = _mm256_add_epi64(qhi, _mm256_srli_epi64(sq, 32));
        }
        uint64_t lo[4], hi[4];
        _mm256_storeu_si256((__m256i *)lo, qlo);
        _mm256_storeu_si256((__m256i *)hi, qhi);
        q += (unsigned __int128)(lo[0] + lo[1] + lo[2] + lo[3]) +
             ((unsigned __int128)(hi[0] + hi[1] + hi[2] + hi[3]) << 32);
    }
    isq_add(dx, q, 0);
    i64_squares(v, i, n, dx);
}

__attribute__((target("avx512f,avx512dq"))) static void fold_i64_avx512(const int64_t *v, size_t n, agg_state *a,
                                                                        agg_dev *dx)
{
    const __m512i nv = _mm512_set1_epi64(INT64_MIN), lom = _mm512_set1_epi64(0xffffffff);
    __m512i mn0 = _mm512_set1_epi64(INT64_MAX), mn1 = mn0, mx0 = nv, mx1 = nv;
    __int128 s = 0;
    uint64_t c = 0;
    size_t i = 0;
    while (n - i >= 16) {
        size_t end = i + ((n - i) / 16 < I64_BLOCK ? (n - i) / 16 : I64_BLOCK) * 16;
        __m512i slo = _mm512_setzero_si512(), shi = slo, sneg = slo;
#define STEP(x, mn, mx)                                                                                            \
    do {                                                                                                           \
        __mmask8 k = _mm512_cmpneq_epi64_mask(x, nv);                                                              \
        __m512i xs = _mm512_maskz_mov_epi64(k, x);                                                                 \
        slo = _mm512_add_epi64(slo, _mm512_and_si512(xs, lom));                                                    \
        shi = _mm512_add_epi64(shi, _mm512_srli_epi64(xs, 32));                                                    \
        sneg = _mm512_add_epi64(sneg, _mm512_srli_epi64(xs, 63));                                                  \
        c += (uint64_t)__builtin_popcount(k);                                                                      \
        mn = _mm512_mask_min_epi64(mn, k, mn, x);                                                                  \
        mx = _mm512_max_epi64(mx, x);                                                                              \
    } while (0)
        for (; i < end; i += 16) {
            __m512i x = _mm512_loadu_si512(v + i), y = _mm512_loadu_si512(v + i + 8);
            STEP(x, mn0, mx0);
            STEP(y, mn1, mx1);
        }
#undef STEP
        uint64_t lo[8], hi[8], neg[8];
        _mm512_storeu_si512(lo, slo);
        _mm512_storeu_si512(hi, shi);
        _mm512_storeu_si512(neg, sneg);
        s += i64_combine(lo, hi, neg, 8);
    }
    int64_t mn = _mm512_reduce_min_epi64(_mm512_min_epi64(mn0, mn1));
    int64_t mx = _mm512_reduce_max_epi64(_mm512_max_epi64(mx0, mx1));
    i64_tail(v, i, n, &s, &c, &mn, &mx);
    i64_finish(a, s, c, mn, mx);
    if (!dx || !c)
        return;
    i = 0;
    if (mn < INT32_MIN || mx > INT32_MAX) {
        while (n - i >= 8) {
            size_t end = i + ((n - i) / 8 < I64_BLOCK ? (n - i) / 8 : I64_BLOCK) * 8;
            __m512i a0 = _mm512_setzero_si512(), a1 = a0, b0 = a0, b1 = a0, c0 = a0, c1 = a0;
            for (; i < end; i += 8) {
                __m512i x = _mm512_loadu_si512(v + i);
                __m512i u = _mm512_abs_epi64(_mm512_maskz_mov_epi64(_mm512_cmpneq_epi64_mask(x, nv), x));
                __m512i h = _mm512_srli_epi64(u, 32);
                __m512i ll = _mm512_mul_epu32(u, u), hl = _mm512_mul_epu32(h, u), hh = _mm512_mul_epu32(h, h);
                a0 = _mm512_add_epi64(a0, _mm512_and_si512(ll, lom));
                a1 = _mm512_add_epi64(a1, _mm512_srli_epi64(ll, 32));
                b0 = _mm512_add_epi64(b0, _mm512_and_si512(hl, lom));
                b1 = _mm512_add_epi64(b1, _mm512_srli_epi64(hl, 32));
                c0 = _mm512_add_epi64(c0, _mm512_and_si512(hh, lom));
                c1 = _mm512_add_epi64(c1, _mm512_srli_epi64(hh, 32));
            }
            i64_sq_wide(dx, (uint64_t)_mm512_reduce_add_epi64(a0), (uint64_t)_mm512_reduce_add_epi64(a1),
                        (uint64_t)_mm512_reduce_add_epi64(b0), (uint64_t)_mm512_reduce_add_epi64(b1),
                        (uint64_t)_mm512_reduce_add_epi64(c0), (uint64_t)_mm512_reduce_add_epi64(c1));
        }
        i64_squares(v, i, n, dx);
        return;
    }
    unsigned __int128 q = 0;
    while (n - i >= 8) {
        size_t end = i + ((n - i) / 8 < I64_BLOCK ? (n - i) / 8 : I64_BLOCK) * 8;
        __m512i qlo = _mm512_setzero_si512(), qhi = qlo;
        for (; i < end; i += 8) {
            __m512i x = _mm512_loadu_si512(v + i);
            __m512i xs = _mm512_maskz_mov_epi64(_mm512_cmpneq_epi64_mask(x, nv), x), sq = _mm512_mul_epi32(xs, xs);
            qlo = _mm512_add_epi64(qlo, _mm512_and_si512(sq, lom));
            qhi = _mm512_add_epi64(qhi, _mm512_srli_epi64(sq, 32));
        }
        q += (unsigned __int128)(uint64_t)_mm512_reduce_add_epi64(qlo) +
             ((unsigned __int128)(uint64_t)_mm512_reduce_add_epi64(qhi) << 32);
    }
    isq_add(dx, q, 0);
    i64_squares(v, i, n, dx);
}
#endif

/*
 * bool and state codes: 4-byte unsigned slots with their own null (NULL_BOOL,
 * NULL_STATE). Exact like int64, so the order is free and values.c's loop is
 * the reference. A null is masked to 0 for the sum, the squares and the
 * maximum (0 is the identity of an unsigned maximum; a run without known
 * values changes nothing) and to UINT32_MAX for the minimum. The 32-bit
 * values are summed in 64-bit lanes as even and odd halves; a square (below
 * 2^64, from the unsigned 32x32 multiply) as its two 32-bit halves, so no
 * lane overflows within U32_BLOCK slots. The squares cost little here, so
 * they come in the same pass when the spread is asked for.
 */
#define U32_BLOCK ((size_t)1 << 26)

static void u32_tail(const uint32_t *v, size_t i, size_t n, uint32_t null, unsigned __int128 *s, uint64_t *c,
                     unsigned __int128 *q, uint32_t *mn, uint32_t *mx)
{
    for (; i < n; i++) {
        uint32_t x = v[i];
        if (x == null)
            continue;
        ++*c;
        *s += x;
        *q += (uint64_t)x * x;
        *mn = x < *mn ? x : *mn;
        *mx = x > *mx ? x : *mx;
    }
}

static void u32_finish(agg_state *a, agg_dev *dx, unsigned __int128 s, uint64_t c, unsigned __int128 q, uint32_t mn,
                       uint32_t mx)
{
    if (!c)
        return;
    a->count += c;
    a->isum += (__int128)s;
    a->imin = (int64_t)mn < a->imin ? (int64_t)mn : a->imin;
    a->imax = (int64_t)mx > a->imax ? (int64_t)mx : a->imax;
    if (dx)
        isq_add(dx, q, 0);
}

#ifdef TSDB_X86
K_AVX2 void fold_u32_avx2_impl(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx, bool sq)
{
    const __m256i nv = _mm256_set1_epi32((int)null), lom = _mm256_set1_epi64x(0xffffffff);
    __m256i mn0 = _mm256_set1_epi32(-1), mn1 = mn0, mx0 = _mm256_setzero_si256(), mx1 = mx0;
    unsigned __int128 s = 0, q = 0;
    uint64_t c = 0;
    size_t i = 0;
    while (n - i >= 16) {
        size_t end = i + ((n - i) < U32_BLOCK ? (n - i) : U32_BLOCK) / 16 * 16, start = i;
        __m256i s0 = _mm256_setzero_si256(), s1 = s0, nul0 = s0, nul1 = s0, ql0 = s0, ql1 = s0, qh0 = s0, qh1 = s0;
#define STEP(x, sm, nul, mn, mx, ql, qh)                                                                            \
    do {                                                                                                           \
        __m256i m = _mm256_cmpeq_epi32(x, nv), xs = _mm256_andnot_si256(m, x), od = _mm256_srli_epi64(xs, 32);     \
        nul = _mm256_sub_epi32(nul, m);                                                                            \
        mn = _mm256_min_epu32(mn, _mm256_or_si256(x, m));                                                          \
        mx = _mm256_max_epu32(mx, xs);                                                                             \
        sm = _mm256_add_epi64(sm, _mm256_add_epi64(_mm256_and_si256(xs, lom), od));                                \
        if (sq) {                                                                                                  \
            __m256i e = _mm256_mul_epu32(xs, xs), o = _mm256_mul_epu32(od, od);                                    \
            ql = _mm256_add_epi64(ql, _mm256_add_epi64(_mm256_and_si256(e, lom), _mm256_and_si256(o, lom)));      \
            qh = _mm256_add_epi64(qh, _mm256_add_epi64(_mm256_srli_epi64(e, 32), _mm256_srli_epi64(o, 32)));      \
        }                                                                                                          \
    } while (0)
        for (; i < end; i += 16) {
            __m256i x = _mm256_loadu_si256((const __m256i *)(v + i));
            __m256i y = _mm256_loadu_si256((const __m256i *)(v + i + 8));
            STEP(x, s0, nul0, mn0, mx0, ql0, qh0);
            STEP(y, s1, nul1, mn1, mx1, ql1, qh1);
        }
#undef STEP
        uint64_t t[4], u[4], w[4];
        uint32_t k[8];
        _mm256_storeu_si256((__m256i *)t, _mm256_add_epi64(s0, s1));
        _mm256_storeu_si256((__m256i *)u, _mm256_add_epi64(ql0, ql1));
        _mm256_storeu_si256((__m256i *)w, _mm256_add_epi64(qh0, qh1));
        _mm256_storeu_si256((__m256i *)k, _mm256_add_epi32(nul0, nul1));
        uint64_t nulls = 0;
        for (int j = 0; j < 4; j++) {
            s += t[j];
            q += u[j] + ((unsigned __int128)w[j] << 32);
        }
        for (int j = 0; j < 8; j++)
            nulls += k[j];
        c += (end - start) - nulls;
    }
    uint32_t lo[8], hi[8], mn = UINT32_MAX, mx = 0;
    _mm256_storeu_si256((__m256i *)lo, _mm256_min_epu32(mn0, mn1));
    _mm256_storeu_si256((__m256i *)hi, _mm256_max_epu32(mx0, mx1));
    for (int j = 0; j < 8; j++) {
        mn = lo[j] < mn ? lo[j] : mn;
        mx = hi[j] > mx ? hi[j] : mx;
    }
    u32_tail(v, i, n, null, &s, &c, &q, &mn, &mx);
    u32_finish(a, dx, s, c, q, mn, mx);
}

__attribute__((target("avx2"))) static void fold_u32_avx2(const uint32_t *v, size_t n, uint32_t null, agg_state *a,
                                                          agg_dev *dx)
{
    if (dx)
        fold_u32_avx2_impl(v, n, null, a, dx, true);
    else
        fold_u32_avx2_impl(v, n, null, a, NULL, false);
}

K_AVX512 void fold_u32_avx512_impl(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx, bool sq)
{
    const __m512i nv = _mm512_set1_epi32((int)null), lom = _mm512_set1_epi64(0xffffffff),
                  one = _mm512_set1_epi32(1);
    __m512i mn0 = _mm512_set1_epi32(-1), mn1 = mn0, mx0 = _mm512_setzero_si512(), mx1 = mx0;
    unsigned __int128 s = 0, q = 0;
    uint64_t c = 0;
    size_t i = 0;
    while (n - i >= 32) {
        size_t end = i + ((n - i) < U32_BLOCK ? (n - i) : U32_BLOCK) / 32 * 32;
        __m512i s0 = _mm512_setzero_si512(), s1 = s0, c0 = s0, c1 = s0, ql0 = s0, ql1 = s0, qh0 = s0, qh1 = s0;
#define STEP(x, sm, cn, mn, mx, ql, qh)                                                                             \
    do {                                                                                                           \
        __mmask16 k = _mm512_cmpneq_epu32_mask(x, nv);                                                             \
        __m512i xs = _mm512_maskz_mov_epi32(k, x), od = _mm512_srli_epi64(xs, 32);                                 \
        cn = _mm512_mask_add_epi32(cn, k, cn, one);                                                                \
        mn = _mm512_mask_min_epu32(mn, k, mn, x);                                                                  \
        mx = _mm512_max_epu32(mx, xs);                                                                             \
        sm = _mm512_add_epi64(sm, _mm512_add_epi64(_mm512_and_si512(xs, lom), od));                                \
        if (sq) {                                                                                                  \
            __m512i e = _mm512_mul_epu32(xs, xs), o = _mm512_mul_epu32(od, od);                                    \
            ql = _mm512_add_epi64(ql, _mm512_add_epi64(_mm512_and_si512(e, lom), _mm512_and_si512(o, lom)));      \
            qh = _mm512_add_epi64(qh, _mm512_add_epi64(_mm512_srli_epi64(e, 32), _mm512_srli_epi64(o, 32)));      \
        }                                                                                                          \
    } while (0)
        for (; i < end; i += 32) {
            __m512i x = _mm512_loadu_si512((const void *)(v + i));
            __m512i y = _mm512_loadu_si512((const void *)(v + i + 16));
            STEP(x, s0, c0, mn0, mx0, ql0, qh0);
            STEP(y, s1, c1, mn1, mx1, ql1, qh1);
        }
#undef STEP
        s += (uint64_t)_mm512_reduce_add_epi64(_mm512_add_epi64(s0, s1));
        if (sq)
            q += (uint64_t)_mm512_reduce_add_epi64(_mm512_add_epi64(ql0, ql1)) +
                 ((unsigned __int128)(uint64_t)_mm512_reduce_add_epi64(_mm512_add_epi64(qh0, qh1)) << 32);
        c += (uint32_t)_mm512_reduce_add_epi32(_mm512_add_epi32(c0, c1));
    }
    uint32_t mn = (uint32_t)_mm512_reduce_min_epu32(_mm512_min_epu32(mn0, mn1));
    uint32_t mx = (uint32_t)_mm512_reduce_max_epu32(_mm512_max_epu32(mx0, mx1));
    u32_tail(v, i, n, null, &s, &c, &q, &mn, &mx);
    u32_finish(a, dx, s, c, q, mn, mx);
}

__attribute__((target("avx512f,avx512dq"))) static void fold_u32_avx512(const uint32_t *v, size_t n, uint32_t null,
                                                                        agg_state *a, agg_dev *dx)
{
    if (dx)
        fold_u32_avx512_impl(v, n, null, a, dx, true);
    else
        fold_u32_avx512_impl(v, n, null, a, NULL, false);
}
#endif

/* ----------------------------------------------------------------- TWA */

/*
 * A granule's step and linear areas (granule_twa, stored with the granule):
 * segment s runs from point s to point s + 1 and adds its areas to lane
 * s % L64, the float64 lanes; a segment that does not count (the reading at
 * its start, or for the linear area at either end, is null) adds +0.0, so
 * the order stays by position. Reduced by halving like the sums. The
 * products are those of seg() in twa.c: v * dt and (v + x) * 0.5 * dt, with
 * dt converted to double rounded once (the vector paths build it from two
 * exact halves added once), int64 values likewise. The scalar code keeps the
 * same lanes; the milliseconds and the longest gap are integers.
 */
static inline double twa_y(int type, const void *v, size_t i, bool *ok)
{
    switch (type) {
    case TSDB_FLOAT32: {
        uint32_t u;
        float f;
        memcpy(&u, (const uint8_t *)v + 4 * i, 4);
        memcpy(&f, &u, 4);
        *ok = u != NULL_F32;
        return f;
    }
    case TSDB_FLOAT64: {
        uint64_t u;
        double d;
        memcpy(&u, (const uint8_t *)v + 8 * i, 8);
        memcpy(&d, &u, 8);
        *ok = u != NULL_F64;
        return d;
    }
    case TSDB_INT64: {
        int64_t x;
        memcpy(&x, (const uint8_t *)v + 8 * i, 8);
        *ok = x != NULL_I64;
        return (double)x;
    }
    default: {
        uint32_t u;
        memcpy(&u, (const uint8_t *)v + 4 * i, 4);
        *ok = u != NULL_BOOL;
        return (double)u;
    }
    }
}

/* segments [s, n - 1) one at a time into the lanes, then the reduction of the first k lanes (L64, or fewer for
 * a short granule: see twa_small) */
INL void twa_tail(int type, const int64_t *ts, const void *v, size_t s, size_t n, double *st, double *li,
                  twa_sums *o, const int k)
{
    bool okp;
    double vp = twa_y(type, v, s, &okp);
    for (size_t i = s + 1; i < n; i++) {
        uint64_t dt = (uint64_t)ts[i] - (uint64_t)ts[i - 1];
        o->gap = dt > o->gap ? dt : o->gap;
        bool ok;
        double x = twa_y(type, v, i, &ok), d = (double)dt;
        size_t j = (i - 1) % L64;
        st[j] += okp ? vp * d : 0.0;
        li[j] += okp && ok ? (vp + x) * 0.5 * d : 0.0;
        o->sms += okp ? dt : 0;
        o->lms += okp && ok ? dt : 0;
        vp = x;
        okp = ok;
    }
    o->step = canon_nan(lanes_reduce(st, k));
    o->lin = canon_nan(lanes_reduce(li, k));
}

/* whole groups of L64 segments with the lane as a constant (the compiler keeps the lanes in registers and
 * may vectorize them; it cannot reorder the additions), then the tail */
INL void twa_scalar_impl(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    double st[L64] = {0}, li[L64] = {0};
    uint64_t sms = 0, lms = 0, gap = 0;
    size_t s = 0;
    for (; s + L64 + 1 <= n; s += L64)
        for (size_t j = 0; j < L64; j++) {
            size_t i = s + j + 1;
            uint64_t dt = (uint64_t)ts[i] - (uint64_t)ts[i - 1];
            gap = dt > gap ? dt : gap;
            bool okp, ok;
            double vp = twa_y(type, v, i - 1, &okp), x = twa_y(type, v, i, &ok), d = (double)dt;
            st[j] += okp ? vp * d : 0.0;
            li[j] += okp && ok ? (vp + x) * 0.5 * d : 0.0;
            sms += okp ? dt : 0;
            lms += okp && ok ? dt : 0;
        }
    *o = (twa_sums){.sms = sms, .lms = lms, .gap = gap};
    twa_tail(type, ts, v, s, n, st, li, o, L64);
}

static void twa_scalar(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    switch (type) {
    case TSDB_FLOAT32: twa_scalar_impl(TSDB_FLOAT32, ts, v, n, o); break;
    case TSDB_FLOAT64: twa_scalar_impl(TSDB_FLOAT64, ts, v, n, o); break;
    case TSDB_INT64: twa_scalar_impl(TSDB_INT64, ts, v, n, o); break;
    default: twa_scalar_impl(TSDB_BOOL, ts, v, n, o); break;
    }
}

/* At most L64 points, so at most one segment per lane: as for the short runs above, the reduction of the k
 * lanes in use gives the same bits, and with K a constant the lanes stay in registers. */
INL void twa_small_k(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o, const int K)
{
    double st[L64], li[L64];
    uint64_t sms = 0, lms = 0, gap = 0;
    bool okp;
    double vp = twa_y(type, v, 0, &okp);
    _Pragma("GCC unroll 16") for (int j = 0; j < K; j++)
    {
        size_t i = (size_t)j + 1;
        bool in = i < n, ok = false;
        uint64_t dt = in ? (uint64_t)ts[i] - (uint64_t)ts[i - 1] : 0;
        double x = in ? twa_y(type, v, i, &ok) : 0.0, d = (double)dt;
        ok = in && ok;
        gap = dt > gap ? dt : gap;
        st[j] = 0.0 + (in && okp ? vp * d : 0.0);
        li[j] = 0.0 + (okp && ok ? (vp + x) * 0.5 * d : 0.0);
        sms += in && okp ? dt : 0;
        lms += okp && ok ? dt : 0;
        vp = x;
        okp = ok;
    }
    _Pragma("GCC unroll 8") for (int w = K / 2; w >= 1; w /= 2)
        _Pragma("GCC unroll 16") for (int j = 0; j < w; j++)
    {
        st[j] = st[j] + st[j + w];
        li[j] = li[j] + li[j + w];
    }
    *o = (twa_sums){.sms = sms, .lms = lms, .gap = gap, .step = canon_nan(st[0]), .lin = canon_nan(li[0])};
}

INL void twa_small_impl(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    switch (lanes_for(n - 1)) {
    case 1: twa_small_k(type, ts, v, n, o, 1); break;
    case 2: twa_small_k(type, ts, v, n, o, 2); break;
    case 4: twa_small_k(type, ts, v, n, o, 4); break;
    case 8: twa_small_k(type, ts, v, n, o, 8); break;
    default: twa_small_k(type, ts, v, n, o, 16); break;
    }
}

void simd_twa_run(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    if (n > L64) {
        simd_twa(type, ts, v, n, o);
        return;
    }
    switch (type) {
    case TSDB_FLOAT32: twa_small_impl(TSDB_FLOAT32, ts, v, n, o); break;
    case TSDB_FLOAT64: twa_small_impl(TSDB_FLOAT64, ts, v, n, o); break;
    case TSDB_INT64: twa_small_impl(TSDB_INT64, ts, v, n, o); break;
    default: twa_small_impl(TSDB_BOOL, ts, v, n, o); break;
    }
}

#ifdef TSDB_X86
/* (double) of unsigned and signed 64-bit lanes, rounded once as the scalar conversion: the high half scaled by
 * 2^32 minus a constant is exact, and adding the low half rounds */
K_AVX2 __m256d u64_pd_avx2(__m256i x)
{
    __m256i lo = _mm256_blend_epi32(x, _mm256_castpd_si256(_mm256_set1_pd(0x1p52)), 0xaa);
    __m256i hi = _mm256_or_si256(_mm256_srli_epi64(x, 32), _mm256_castpd_si256(_mm256_set1_pd(0x1p84)));
    __m256d h = _mm256_sub_pd(_mm256_castsi256_pd(hi), _mm256_set1_pd(0x1p84 + 0x1p52));
    return _mm256_add_pd(h, _mm256_castsi256_pd(lo));
}

K_AVX2 __m256d i64_pd_avx2(__m256i x)
{
    x = _mm256_xor_si256(x, _mm256_set1_epi64x(INT64_MIN)); /* x + 2^63, unsigned */
    __m256i lo = _mm256_blend_epi32(x, _mm256_castpd_si256(_mm256_set1_pd(0x1p52)), 0xaa);
    __m256i hi = _mm256_or_si256(_mm256_srli_epi64(x, 32), _mm256_castpd_si256(_mm256_set1_pd(0x1p84)));
    __m256d h = _mm256_sub_pd(_mm256_castsi256_pd(hi), _mm256_set1_pd(0x1p84 + 0x1p63 + 0x1p52));
    return _mm256_add_pd(h, _mm256_castsi256_pd(lo));
}

/* points p .. p + 3 of a granule's slots as doubles, and which are not null (all ones) */
K_AVX2 __m256d twa_ld_avx2(int type, const void *v, size_t p, __m256i *ok)
{
    const __m256i ones = _mm256_set1_epi64x(-1);
    switch (type) {
    case TSDB_FLOAT32: {
        __m128i b = _mm_loadu_si128((const __m128i *)((const uint32_t *)v + p));
        *ok = _mm256_xor_si256(_mm256_cvtepi32_epi64(_mm_cmpeq_epi32(b, _mm_set1_epi32((int)NULL_F32))), ones);
        return _mm256_cvtps_pd(_mm_castsi128_ps(b));
    }
    case TSDB_FLOAT64: {
        __m256i b = _mm256_loadu_si256((const __m256i *)((const uint64_t *)v + p));
        *ok = _mm256_xor_si256(_mm256_cmpeq_epi64(b, _mm256_set1_epi64x((long long)NULL_F64)), ones);
        return _mm256_castsi256_pd(b);
    }
    case TSDB_INT64: {
        __m256i b = _mm256_loadu_si256((const __m256i *)((const int64_t *)v + p));
        *ok = _mm256_xor_si256(_mm256_cmpeq_epi64(b, _mm256_set1_epi64x(NULL_I64)), ones);
        return i64_pd_avx2(b);
    }
    default: {
        __m256i b = _mm256_cvtepu32_epi64(_mm_loadu_si128((const __m128i *)((const uint32_t *)v + p)));
        *ok = _mm256_xor_si256(_mm256_cmpeq_epi64(b, _mm256_set1_epi64x(NULL_BOOL)), ones);
        return u64_pd_avx2(b);
    }
    }
}

K_AVX2 void twa_avx2_impl(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    const __m256i bias = _mm256_set1_epi64x(INT64_MIN);
    const __m256d half = _mm256_set1_pd(0.5);
    __m256d st[4], li[4];
    __m256i sms = _mm256_setzero_si256(), lms = sms, gf = bias; /* the gap biased, for signed compares */
    for (int q = 0; q < 4; q++)
        st[q] = li[q] = _mm256_setzero_pd();
    size_t s = 0;
    for (; s + L64 + 1 <= n; s += L64) {
        for (int q = 0; q < 4; q++) {
            size_t p = s + 4 * (size_t)q;
            __m256i dt = _mm256_sub_epi64(_mm256_loadu_si256((const __m256i *)(ts + p + 1)),
                                          _mm256_loadu_si256((const __m256i *)(ts + p)));
            __m256i db = _mm256_xor_si256(dt, bias), okp, ok;
            gf = _mm256_blendv_epi8(gf, db, _mm256_cmpgt_epi64(db, gf));
            __m256d vp = twa_ld_avx2(type, v, p, &okp), x = twa_ld_avx2(type, v, p + 1, &ok), d = u64_pd_avx2(dt);
            __m256i both = _mm256_and_si256(okp, ok);
            st[q] = _mm256_add_pd(st[q], _mm256_and_pd(_mm256_castsi256_pd(okp), _mm256_mul_pd(vp, d)));
            li[q] = _mm256_add_pd(li[q], _mm256_and_pd(_mm256_castsi256_pd(both),
                                                       _mm256_mul_pd(_mm256_mul_pd(_mm256_add_pd(vp, x), half), d)));
            sms = _mm256_add_epi64(sms, _mm256_and_si256(okp, dt));
            lms = _mm256_add_epi64(lms, _mm256_and_si256(both, dt));
        }
    }
    double sl[L64], ll[L64];
    uint64_t a[4], b[4], g[4];
    for (int q = 0; q < 4; q++) {
        _mm256_storeu_pd(sl + 4 * q, st[q]);
        _mm256_storeu_pd(ll + 4 * q, li[q]);
    }
    _mm256_storeu_si256((__m256i *)a, sms);
    _mm256_storeu_si256((__m256i *)b, lms);
    _mm256_storeu_si256((__m256i *)g, _mm256_xor_si256(gf, bias));
    *o = (twa_sums){0};
    for (int j = 0; j < 4; j++) {
        o->sms += a[j];
        o->lms += b[j];
        o->gap = g[j] > o->gap ? g[j] : o->gap;
    }
    twa_tail(type, ts, v, s, n, sl, ll, o, L64);
}

__attribute__((target("avx2"))) static void twa_avx2(int type, const int64_t *ts, const void *v, size_t n,
                                                    twa_sums *o)
{
    switch (type) {
    case TSDB_FLOAT32: twa_avx2_impl(TSDB_FLOAT32, ts, v, n, o); break;
    case TSDB_FLOAT64: twa_avx2_impl(TSDB_FLOAT64, ts, v, n, o); break;
    case TSDB_INT64: twa_avx2_impl(TSDB_INT64, ts, v, n, o); break;
    default: twa_avx2_impl(TSDB_BOOL, ts, v, n, o); break;
    }
}

K_AVX512 __m512d twa_ld_avx512(int type, const void *v, size_t p, __mmask8 *ok)
{
    switch (type) {
    case TSDB_FLOAT32: {
        __m256i b = _mm256_loadu_si256((const __m256i *)((const uint32_t *)v + p));
        *ok = _mm512_cmpneq_epi64_mask(_mm512_cvtepu32_epi64(b), _mm512_set1_epi64(NULL_F32));
        return _mm512_cvtps_pd(_mm256_castsi256_ps(b));
    }
    case TSDB_FLOAT64: {
        __m512i b = _mm512_loadu_si512((const uint64_t *)v + p);
        *ok = _mm512_cmpneq_epi64_mask(b, _mm512_set1_epi64((long long)NULL_F64));
        return _mm512_castsi512_pd(b);
    }
    case TSDB_INT64: {
        __m512i b = _mm512_loadu_si512((const int64_t *)v + p);
        *ok = _mm512_cmpneq_epi64_mask(b, _mm512_set1_epi64(NULL_I64));
        return _mm512_cvtepi64_pd(b);
    }
    default: {
        __m512i b = _mm512_cvtepu32_epi64(_mm256_loadu_si256((const __m256i *)((const uint32_t *)v + p)));
        *ok = _mm512_cmpneq_epi64_mask(b, _mm512_set1_epi64(NULL_BOOL));
        return _mm512_cvtepu64_pd(b);
    }
    }
}

K_AVX512 void twa_avx512_impl(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    const __m512d half = _mm512_set1_pd(0.5);
    __m512d st[2], li[2];
    __m512i sms = _mm512_setzero_si512(), lms = sms, gap = sms;
    for (int q = 0; q < 2; q++)
        st[q] = li[q] = _mm512_setzero_pd();
    size_t s = 0;
    for (; s + L64 + 1 <= n; s += L64) {
        for (int q = 0; q < 2; q++) {
            size_t p = s + 8 * (size_t)q;
            __m512i dt = _mm512_sub_epi64(_mm512_loadu_si512(ts + p + 1), _mm512_loadu_si512(ts + p));
            __mmask8 okp, ok;
            gap = _mm512_max_epu64(gap, dt);
            __m512d vp = twa_ld_avx512(type, v, p, &okp), x = twa_ld_avx512(type, v, p + 1, &ok);
            __m512d d = _mm512_cvtepu64_pd(dt);
            __mmask8 both = okp & ok;
            st[q] = _mm512_add_pd(st[q], _mm512_maskz_mov_pd(okp, _mm512_mul_pd(vp, d)));
            li[q] = _mm512_add_pd(li[q],
                                  _mm512_maskz_mov_pd(both, _mm512_mul_pd(_mm512_mul_pd(_mm512_add_pd(vp, x), half), d)));
            sms = _mm512_mask_add_epi64(sms, okp, sms, dt);
            lms = _mm512_mask_add_epi64(lms, both, lms, dt);
        }
    }
    double sl[L64], ll[L64];
    for (int q = 0; q < 2; q++) {
        _mm512_storeu_pd(sl + 8 * q, st[q]);
        _mm512_storeu_pd(ll + 8 * q, li[q]);
    }
    *o = (twa_sums){0};
    o->sms = (uint64_t)_mm512_reduce_add_epi64(sms);
    o->lms = (uint64_t)_mm512_reduce_add_epi64(lms);
    o->gap = (uint64_t)_mm512_reduce_max_epu64(gap);
    twa_tail(type, ts, v, s, n, sl, ll, o, L64);
}

__attribute__((target("avx512f,avx512dq"))) static void twa_avx512(int type, const int64_t *ts, const void *v,
                                                                   size_t n, twa_sums *o)
{
    switch (type) {
    case TSDB_FLOAT32: twa_avx512_impl(TSDB_FLOAT32, ts, v, n, o); break;
    case TSDB_FLOAT64: twa_avx512_impl(TSDB_FLOAT64, ts, v, n, o); break;
    case TSDB_INT64: twa_avx512_impl(TSDB_INT64, ts, v, n, o); break;
    default: twa_avx512_impl(TSDB_BOOL, ts, v, n, o); break;
    }
}
#endif

void (*simd_twa)(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o) = twa_scalar;

/* ---------------------------------------------------------------- LTTB */

/* a point's value as LTTB takes it (query.c, lttb_value) */
static inline double lttb_y(int type, uint64_t x)
{
    switch (type) {
    case TSDB_FLOAT32: {
        float f;
        uint32_t u = (uint32_t)x;
        memcpy(&f, &u, 4);
        return f;
    }
    case TSDB_FLOAT64: {
        double d;
        memcpy(&d, &x, 8);
        return d;
    }
    case TSDB_INT64: return (double)(int64_t)x;
    default: return (double)x;
    }
}

static size_t lttb_scalar(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax,
                          double ay, double cx, double cy)
{
    double best = -1;
    size_t k = 0;
    for (size_t j = 0; j < n; j++) {
        double x = (double)((uint64_t)ts[j] - (uint64_t)t0), y = lttb_y(type, v[j]);
        double a = fabs((ax - cx) * (y - ay) - (ax - x) * (cy - ay)) * 0.5; /* the paper's expression */
        if (a > best) { /* the first of equal areas wins */
            best = a;
            k = j;
        }
    }
    return k;
}

#ifdef TSDB_X86
/* LTTB in vectors: the same expression per point (x and y converted rounded once, as lttb_y), each lane keeps
 * its first largest area (NaN never compares greater); then the largest over the lanes, the lowest index on
 * equal areas, and the rest one at a time, so the pick is the scalar loop's */
K_AVX2 __m256d lttb_y_avx2(int type, __m256i x)
{
    switch (type) {
    case TSDB_FLOAT32:
        return _mm256_cvtps_pd(_mm_castsi128_ps(
            _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(x, _mm256_setr_epi32(0, 2, 4, 6, 0, 2, 4, 6)))));
    case TSDB_FLOAT64: return _mm256_castsi256_pd(x);
    case TSDB_INT64: return i64_pd_avx2(x);
    default: return u64_pd_avx2(x);
    }
}

K_AVX2 size_t lttb_avx2_impl(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax,
                             double ay, double cx, double cy)
{
    const __m256d kx = _mm256_set1_pd(ax - cx), ky = _mm256_set1_pd(cy - ay), vax = _mm256_set1_pd(ax),
                  vay = _mm256_set1_pd(ay), half = _mm256_set1_pd(0.5), sign = _mm256_set1_pd(-0.0);
    const __m256i vt0 = _mm256_set1_epi64x(t0), four = _mm256_set1_epi64x(4);
    __m256d b0 = _mm256_set1_pd(-1), b1 = b0;
    __m256i k0 = _mm256_setr_epi64x(0, 1, 2, 3), k1 = _mm256_setr_epi64x(4, 5, 6, 7), i0 = k0, i1 = k1;
#define AREA(j)                                                                                                    \
    _mm256_mul_pd(                                                                                                 \
        _mm256_andnot_pd(sign, _mm256_sub_pd(                                                                      \
                                   _mm256_mul_pd(kx, _mm256_sub_pd(lttb_y_avx2(type, _mm256_loadu_si256(           \
                                                                                     (const __m256i *)(v + (j)))), \
                                                                   vay)),                                          \
                                   _mm256_mul_pd(_mm256_sub_pd(vax, u64_pd_avx2(_mm256_sub_epi64(                  \
                                                                        _mm256_loadu_si256(                        \
                                                                            (const __m256i *)(ts + (j))),          \
                                                                        vt0))),                                    \
                                                 ky))),                                                            \
        half)
    size_t j = 0;
    for (; j + 8 <= n; j += 8) {
        __m256d a0 = AREA(j), a1 = AREA(j + 4);
        __m256d g0 = _mm256_cmp_pd(a0, b0, _CMP_GT_OQ), g1 = _mm256_cmp_pd(a1, b1, _CMP_GT_OQ);
        b0 = _mm256_blendv_pd(b0, a0, g0);
        b1 = _mm256_blendv_pd(b1, a1, g1);
        k0 = _mm256_blendv_epi8(k0, i0, _mm256_castpd_si256(g0));
        k1 = _mm256_blendv_epi8(k1, i1, _mm256_castpd_si256(g1));
        i0 = _mm256_add_epi64(i0, _mm256_add_epi64(four, four));
        i1 = _mm256_add_epi64(i1, _mm256_add_epi64(four, four));
    }
#undef AREA
    double bl[8];
    uint64_t kl[8];
    _mm256_storeu_pd(bl, b0);
    _mm256_storeu_pd(bl + 4, b1);
    _mm256_storeu_si256((__m256i *)kl, k0);
    _mm256_storeu_si256((__m256i *)(kl + 4), k1);
    double best = -1;
    size_t k = 0;
    for (int l = 0; l < 8; l++)
        if (bl[l] > best || (bl[l] == best && bl[l] >= 0 && kl[l] < k)) {
            best = bl[l];
            k = kl[l];
        }
    for (; j < n; j++) {
        double x = (double)((uint64_t)ts[j] - (uint64_t)t0), y = lttb_y(type, v[j]);
        double a = fabs((ax - cx) * (y - ay) - (ax - x) * (cy - ay)) * 0.5;
        if (a > best) {
            best = a;
            k = j;
        }
    }
    return k;
}

__attribute__((target("avx2"))) static size_t lttb_avx2(int type, const int64_t *ts, const uint64_t *v, size_t n,
                                                       int64_t t0, double ax, double ay, double cx, double cy)
{
    switch (type) {
    case TSDB_FLOAT32: return lttb_avx2_impl(TSDB_FLOAT32, ts, v, n, t0, ax, ay, cx, cy);
    case TSDB_FLOAT64: return lttb_avx2_impl(TSDB_FLOAT64, ts, v, n, t0, ax, ay, cx, cy);
    case TSDB_INT64: return lttb_avx2_impl(TSDB_INT64, ts, v, n, t0, ax, ay, cx, cy);
    default: return lttb_avx2_impl(TSDB_BOOL, ts, v, n, t0, ax, ay, cx, cy);
    }
}

K_AVX512 __m512d lttb_y_avx512(int type, __m512i x)
{
    switch (type) {
    case TSDB_FLOAT32: return _mm512_cvtps_pd(_mm256_castsi256_ps(_mm512_cvtepi64_epi32(x)));
    case TSDB_FLOAT64: return _mm512_castsi512_pd(x);
    case TSDB_INT64: return _mm512_cvtepi64_pd(x);
    default: return _mm512_cvtepu64_pd(x);
    }
}

K_AVX512 size_t lttb_avx512_impl(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax,
                                 double ay, double cx, double cy)
{
    const __m512d kx = _mm512_set1_pd(ax - cx), ky = _mm512_set1_pd(cy - ay), vax = _mm512_set1_pd(ax),
                  vay = _mm512_set1_pd(ay), half = _mm512_set1_pd(0.5);
    const __m512i vt0 = _mm512_set1_epi64(t0), step = _mm512_set1_epi64(16);
    __m512d b0 = _mm512_set1_pd(-1), b1 = b0;
    __m512i k0 = _mm512_setr_epi64(0, 1, 2, 3, 4, 5, 6, 7), k1 = _mm512_add_epi64(k0, _mm512_set1_epi64(8));
    __m512i i0 = k0, i1 = k1;
#define AREA(j)                                                                                                    \
    _mm512_mul_pd(_mm512_abs_pd(_mm512_sub_pd(                                                                     \
                      _mm512_mul_pd(kx, _mm512_sub_pd(lttb_y_avx512(type, _mm512_loadu_si512(v + (j))), vay)),     \
                      _mm512_mul_pd(_mm512_sub_pd(vax, _mm512_cvtepu64_pd(                                         \
                                                           _mm512_sub_epi64(_mm512_loadu_si512(ts + (j)), vt0))),   \
                                    ky))),                                                                         \
                  half)
    size_t j = 0;
    for (; j + 16 <= n; j += 16) {
        __m512d a0 = AREA(j), a1 = AREA(j + 8);
        __mmask8 g0 = _mm512_cmp_pd_mask(a0, b0, _CMP_GT_OQ), g1 = _mm512_cmp_pd_mask(a1, b1, _CMP_GT_OQ);
        b0 = _mm512_mask_mov_pd(b0, g0, a0);
        b1 = _mm512_mask_mov_pd(b1, g1, a1);
        k0 = _mm512_mask_mov_epi64(k0, g0, i0);
        k1 = _mm512_mask_mov_epi64(k1, g1, i1);
        i0 = _mm512_add_epi64(i0, step);
        i1 = _mm512_add_epi64(i1, step);
    }
#undef AREA
    double bl[16];
    uint64_t kl[16];
    _mm512_storeu_pd(bl, b0);
    _mm512_storeu_pd(bl + 8, b1);
    _mm512_storeu_si512(kl, k0);
    _mm512_storeu_si512(kl + 8, k1);
    double best = -1;
    size_t k = 0;
    for (int l = 0; l < 16; l++)
        if (bl[l] > best || (bl[l] == best && bl[l] >= 0 && kl[l] < k)) {
            best = bl[l];
            k = kl[l];
        }
    for (; j < n; j++) {
        double x = (double)((uint64_t)ts[j] - (uint64_t)t0), y = lttb_y(type, v[j]);
        double a = fabs((ax - cx) * (y - ay) - (ax - x) * (cy - ay)) * 0.5;
        if (a > best) {
            best = a;
            k = j;
        }
    }
    return k;
}

__attribute__((target("avx512f,avx512dq"))) static size_t lttb_avx512(int type, const int64_t *ts,
                                                                      const uint64_t *v, size_t n, int64_t t0,
                                                                      double ax, double ay, double cx, double cy)
{
    switch (type) {
    case TSDB_FLOAT32: return lttb_avx512_impl(TSDB_FLOAT32, ts, v, n, t0, ax, ay, cx, cy);
    case TSDB_FLOAT64: return lttb_avx512_impl(TSDB_FLOAT64, ts, v, n, t0, ax, ay, cx, cy);
    case TSDB_INT64: return lttb_avx512_impl(TSDB_INT64, ts, v, n, t0, ax, ay, cx, cy);
    default: return lttb_avx512_impl(TSDB_BOOL, ts, v, n, t0, ax, ay, cx, cy);
    }
}
#endif

#ifdef TSDB_X86
/* the 128-bit kernels: x86-64-v2 (SSE4.2), and the integer, TWA and LTTB ones VEX-encoded for AVX without AVX2 */
#define KSUF sse
#define KTARGET "sse4.2"
#include "simd_sse.inc"
#define KSUF avx
#define KTARGET "avx"
#include "simd_sse.inc"
#endif

/* points per call below which lttb_scalar is faster than the kernels (fold-bench: 8 points 0.6x, 16 on par) */
#define LTTB_SMALL 16
/* the SSE and AVX sets: runs shorter than this take simd_sse.inc's short-run kernels (small_*), whose fixed cost is
 * lower than that of the long-run ones (and for AVX of its 256-bit ones): fold-bench --rows 32, 64 */
#define AVX_SHORT 64

static size_t (*simd_lttb)(int, const int64_t *, const uint64_t *, size_t, int64_t, double, double, double,
                           double) = lttb_scalar;

size_t lttb_argmax(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax, double ay,
                   double cx, double cy)
{
    if (n < LTTB_SMALL) /* the same pick (the first largest area) without the kernels' setup and lane combine */
        return lttb_scalar(type, ts, v, n, t0, ax, ay, cx, cy);
    return simd_lttb(type, ts, v, n, t0, ax, ay, cx, cy);
}

void (*simd_agg_f32)(const float *v, size_t n, agg_state *a, agg_dev *dv) = agg_scalar;
void (*simd_fold_f32)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv) = fold_f32_scalar;
void (*simd_fold_f64)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv) = fold_f64_scalar;
void (*simd_fold_i64)(const int64_t *v, size_t n, agg_state *a, agg_dev *dx);
void (*simd_fold_u32)(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx);
void (*simd_small_f32)(const float *v, size_t n, bool may_be_null, agg_state *a, agg_dev *dv) = small_f32_scalar;
void (*simd_small_f64)(const double *v, size_t n, bool may_be_null, agg_state *a, agg_dev *dv) = small_f64_scalar;
size_t simd_short = SIMD_SHORT;
static const char *simd_level = "scalar";

const char *simd_name(void) { return simd_level; }
/* with CPU levels, also the compile of the codecs in use: "avx2, codecs x86-64-v3" */
static char simd_desc[48] = "scalar";
const char *tsdb_simd_level(void) { return simd_desc; }

static pthread_once_t simd_once = PTHREAD_ONCE_INIT;

#ifdef TSDB_X86
/* Skylake-SP/X, Cascade Lake and Cooper Lake (family 6, model 0x55) drop the
 * clock of the whole core for a while after 512-bit instructions, which slows
 * everything else running there. Ice Lake and later Intel cores, and AMD
 * Zen 4/5, have no meaningful penalty. */
static bool avx512_downclocks(void)
{
    unsigned eax, ebx, ecx, edx;
    char vendor[13];
    if (!__get_cpuid(0, &eax, &ebx, &ecx, &edx))
        return false;
    memcpy(vendor, &ebx, 4);
    memcpy(vendor + 4, &edx, 4);
    memcpy(vendor + 8, &ecx, 4);
    vendor[12] = '\0';
    if (strcmp(vendor, "GenuineIntel") != 0 || !__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        return false;
    unsigned family = (eax >> 8) & 0xf, model = (eax >> 4) & 0xf;
    if (family == 6 || family == 15)
        model |= ((eax >> 16) & 0xf) << 4;
    return family == 6 && model == 0x55;
}
#endif

/* The best kernels for at most `level`: AVX-512 from v4, AVX2 from v3; AVX
 * (Sandy Bridge, Ivy Bridge) and SSE4.2 at every level, as a CPU with them
 * is still v2 (TSDB_CPU caps it through cpu.c). TSDB_SIMD caps the choice
 * further, for benchmarks and tests: "scalar", "sse", "avx", "avx2" or
 * "avx512" picks the best kernels up to that one this CPU has. */
void simd_select(int level)
{
    void (*f)(const float *, size_t, agg_state *, agg_dev *) = agg_scalar;
    void (*f32)(const float *, size_t, size_t, agg_state *, agg_dev *) = fold_f32_scalar;
    void (*f64)(const double *, size_t, size_t, agg_state *, agg_dev *) = fold_f64_scalar;
    void (*i64)(const int64_t *, size_t, agg_state *, agg_dev *) = NULL;
    void (*u32)(const uint32_t *, size_t, uint32_t, agg_state *, agg_dev *) = NULL;
    void (*twa)(int, const int64_t *, const void *, size_t, twa_sums *) = twa_scalar;
    size_t (*lttb)(int, const int64_t *, const uint64_t *, size_t, int64_t, double, double, double, double) =
        lttb_scalar;
    void (*sf32)(const float *, size_t, bool, agg_state *, agg_dev *) = small_f32_scalar;
    void (*sf64)(const double *, size_t, bool, agg_state *, agg_dev *) = small_f64_scalar;
    size_t shortn = SIMD_SHORT;
    const char *name = "scalar";
#ifdef TSDB_X86
    static const char *const names[] = {"scalar", "sse", "avx", "avx2", "avx512"};
    const char *force = getenv("TSDB_SIMD");
    int cap = 4;
    for (int k = 0; force && k < 5; k++)
        if (strcmp(force, names[k]) == 0)
            cap = k;
    __builtin_cpu_init();
    int best = 0;
    if (__builtin_cpu_supports("sse4.2"))
        best = 1;
    if (best == 1 && __builtin_cpu_supports("avx"))
        best = 2;
    if (best == 2 && level >= CPU_V3 && __builtin_cpu_supports("avx2"))
        best = 3;
    if (best == 3 && level >= CPU_V4 && __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512dq") &&
        (!avx512_downclocks() || (force && strcmp(force, "avx512") == 0)))
        best = 4;
    switch (best < cap ? best : cap) {
    case 4:
        f = agg_avx512;
        f32 = fold_f32_avx512;
        f64 = fold_f64_avx512;
        i64 = fold_i64_avx512;
        u32 = fold_u32_avx512;
        twa = twa_avx512;
        lttb = lttb_avx512;
        name = "avx512";
        break;
    case 3:
        f = agg_avx2;
        f32 = fold_f32_avx2;
        f64 = fold_f64_avx2;
        i64 = fold_i64_avx2;
        u32 = fold_u32_avx2;
        twa = twa_avx2;
        lttb = lttb_avx2;
        name = "avx2";
        break;
    case 2:
        f = agg_avx;
        f32 = fold_f32_avx;
        f64 = fold_f64_avx;
        sf32 = small_f32_avx; /* the 128-bit kernels up to AVX_SHORT, then the 256-bit ones */
        sf64 = small_f64_avx;
        shortn = AVX_SHORT;
        i64 = fold_i64_avx;
        u32 = fold_u32_avx;
        twa = twa_avx;
        lttb = lttb_avx;
        name = "avx";
        break;
    case 1:
        f = agg128_sse;
        f32 = fold_f32_128_sse;
        f64 = fold_f64_128_sse;
        sf32 = small_f32_sse;
        sf64 = small_f64_sse;
        shortn = AVX_SHORT;
        i64 = fold_i64_sse;
        u32 = fold_u32_sse;
        twa = twa_sse;
        lttb = lttb_sse;
        name = "sse";
        break;
    default: break;
    }
#endif
    simd_agg_f32 = f;
    simd_fold_f32 = f32;
    simd_fold_f64 = f64;
    simd_fold_i64 = i64;
    simd_fold_u32 = u32;
    simd_twa = twa;
    simd_lttb = lttb;
    simd_small_f32 = sf32;
    simd_small_f64 = sf64;
    simd_short = shortn;
    simd_level = name;
#ifdef TSDB_LEVELS
    snprintf(simd_desc, sizeof simd_desc, "%s, codecs x86-64-%s", name, cpu_level_name(level));
#else
    snprintf(simd_desc, sizeof simd_desc, "%s", name);
#endif
}

static void simd_pick(void) { simd_select(cpu_level()); }

void simd_init(void) { pthread_once(&simd_once, simd_pick); }
