/*
 * Aggregation kernels with runtime dispatch: AVX-512, AVX2 or scalar.
 *
 * Gorilla and DoubleDelta are bit-serial (each value's bit position depends on
 * the previous one), so they are decoded with scalar code. What vectorizes is
 * everything after decoding: count/sum/min/max over the values.
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
 * The three kernels give bit-identical results: stored granule statistics and
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
#define LANES 32
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
                    agg_state *a, agg_dev *dv, const bool nl)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        float x = v[i + (size_t)j];
        double d = nl && null_f32(x) ? 0.0 : (double)x - m;
        l->d1[j] += d;
        l->d2[j] += d * d;
    }
    double d1 = lanes_reduce(l->d1, LANES), d2 = lanes_reduce(l->d2, LANES);
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
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl);
}

/* values [i, n) with n - i < LANES, then the reduction: min and max into a; returns the sum */
INL double lanes_finish(lanes *l, const float *v, size_t i, size_t n, agg_state *a, const bool nl)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        float x = v[i + (size_t)j];
        l->s[j] += nl && null_f32(x) ? 0.0 : (double)x;
        l->mn[j] = x < l->mn[j] ? x : l->mn[j];
        l->mx[j] = x > l->mx[j] ? x : l->mx[j];
    }
    for (int w = LANES / 2; w >= 1; w /= 2)
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
    double s = lanes_finish(&l, v, i, n, a, nl);
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

#define L64 16

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

/* values [i, n) with n - i < L64, then the reduction: min and max into a; returns the sum */
INL double lanes64_finish(lanes64 *l, const double *v, size_t i, size_t n, agg_state *a, const bool nl)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        double x = v[i + (size_t)j];
        l->s[j] += nl && null_f64(x) ? 0.0 : x;
        l->mn[j] = x < l->mn[j] ? x : l->mn[j];
        l->mx[j] = x > l->mx[j] ? x : l->mx[j];
    }
    for (int w = L64 / 2; w >= 1; w /= 2)
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
                      agg_dev *dv, const bool nl)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        double x = v[i + (size_t)j];
        double d = nl && null_f64(x) ? 0.0 : x - m;
        l->d1[j] += d;
        l->d2[j] += d * d;
    }
    double d1 = lanes_reduce(l->d1, L64), d2 = lanes_reduce(l->d2, L64);
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
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl);
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
    double s = lanes64_finish(&l, v, i, n, a, nl);
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

#ifdef TSDB_X86
#define K_AVX2 static inline __attribute__((target("avx2"), always_inline))
#define K_AVX512 static inline __attribute__((target("avx512f,avx512dq"), always_inline))

K_AVX2 void dev_avx2(const float *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev_lanes l;
    double m = s / (double)c;
    const __m256d mm = _mm256_set1_pd(m);
    const __m128i nv = _mm_set1_epi32((int)NULL_F32);
    __m256d d1[8], d2[8]; /* lanes 4q .. 4q+3 */
    for (int q = 0; q < 8; q++)
        d1[q] = d2[q] = _mm256_setzero_pd();
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int q = 0; q < 8; q++) {
            __m128 x = _mm_loadu_ps(v + i + 4 * q);
            __m256d d = _mm256_sub_pd(_mm256_cvtps_pd(x), mm);
            if (nl) /* a null adds +0.0 to its lane: see the top of the file */
                d = _mm256_andnot_pd(
                    _mm256_castsi256_pd(_mm256_cvtepi32_epi64(_mm_cmpeq_epi32(_mm_castps_si128(x), nv))), d);
            d1[q] = _mm256_add_pd(d1[q], d);
            d2[q] = _mm256_add_pd(d2[q], _mm256_mul_pd(d, d));
        }
    for (int q = 0; q < 8; q++) {
        _mm256_storeu_pd(l.d1 + 4 * q, d1[q]);
        _mm256_storeu_pd(l.d2 + 4 * q, d2[q]);
    }
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl);
}

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
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl);
}

/* sum of the 4 floats as doubles into s */
#define ADD4(s, q) s = _mm256_add_pd(s, _mm256_cvtps_pd(q))

K_AVX2 void agg_avx2_impl(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes l;
    lanes_init(&l, a);
    const __m256i nv = _mm256_set1_epi32((int)NULL_F32);
    /* sum lanes 0-3, 4-7, ..., 28-31 */
    __m256d s0 = _mm256_setzero_pd(), s1 = s0, s2 = s0, s3 = s0, s4 = s0, s5 = s0, s6 = s0, s7 = s0;
    /* min/max lanes 0-7, 8-15, 16-23, 24-31 */
    __m256 mn0 = _mm256_loadu_ps(l.mn), mn1 = mn0, mn2 = mn0, mn3 = mn0;
    __m256 mx0 = _mm256_loadu_ps(l.mx), mx1 = mx0, mx2 = mx0, mx3 = mx0;
    size_t i = 0;
    /* one quarter at a time, so that no loaded vector stays live: 16 accumulators fill the registers; min/max
     * take the null as it is (a NaN: the accumulator wins), the sums a +0.0 in its place */
#define QUARTER(q, sa, sb, mnq, mxq)                                                                               \
    do {                                                                                                           \
        __m256 x = _mm256_loadu_ps(v + i + 8 * (q)), xs = x;                                                       \
        if (nl)                                                                                                    \
            xs = _mm256_andnot_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_castps_si256(x), nv)), x);         \
        ADD4(sa, _mm256_castps256_ps128(xs));                                                                      \
        ADD4(sb, _mm256_extractf128_ps(xs, 1));                                                                    \
        mnq = _mm256_min_ps(x, mnq);                                                                               \
        mxq = _mm256_max_ps(x, mxq);                                                                               \
    } while (0)
    for (; i + LANES <= n; i += LANES) {
        QUARTER(0, s0, s1, mn0, mx0);
        QUARTER(1, s2, s3, mn1, mx1);
        QUARTER(2, s4, s5, mn2, mx2);
        QUARTER(3, s6, s7, mn3, mx3);
    }
#undef QUARTER
    _mm256_storeu_pd(l.s, s0);
    _mm256_storeu_pd(l.s + 4, s1);
    _mm256_storeu_pd(l.s + 8, s2);
    _mm256_storeu_pd(l.s + 12, s3);
    _mm256_storeu_pd(l.s + 16, s4);
    _mm256_storeu_pd(l.s + 20, s5);
    _mm256_storeu_pd(l.s + 24, s6);
    _mm256_storeu_pd(l.s + 28, s7);
    _mm256_storeu_ps(l.mn, mn0);
    _mm256_storeu_ps(l.mn + 8, mn1);
    _mm256_storeu_ps(l.mn + 16, mn2);
    _mm256_storeu_ps(l.mn + 24, mn3);
    _mm256_storeu_ps(l.mx, mx0);
    _mm256_storeu_ps(l.mx + 8, mx1);
    _mm256_storeu_ps(l.mx + 16, mx2);
    _mm256_storeu_ps(l.mx + 24, mx3);
    double s = lanes_finish(&l, v, i, n, a, nl);
    if (dv)
        dev_avx2(v, n, n - nulls, s, a, dv, nl);
    else
        sum_finish(n - nulls, s, a);
}

__attribute__((target("avx2"))) static void agg_avx2(const float *v, size_t n, agg_state *a, agg_dev *dv)
{
    agg_avx2_impl(v, n, 0, a, dv, false);
}

__attribute__((target("avx2"))) static void fold_f32_avx2(const float *v, size_t n, size_t nulls, agg_state *a,
                                                         agg_dev *dv)
{
    if (nulls)
        agg_avx2_impl(v, n, nulls, a, dv, true);
    else
        agg_avx2_impl(v, n, 0, a, dv, false);
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
    double s = lanes_finish(&l, v, i, n, a, nl);
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

/* float64: 16 lanes, 4 vectors of AVX2 or 2 of AVX-512 */
K_AVX2 void dev64_avx2(const double *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev64 l;
    double m = s / (double)c;
    const __m256d mm = _mm256_set1_pd(m);
    const __m256i nv = _mm256_set1_epi64x((long long)NULL_F64);
    __m256d d1[4], d2[4];
    for (int q = 0; q < 4; q++)
        d1[q] = d2[q] = _mm256_setzero_pd();
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 4; q++) {
            __m256d x = _mm256_loadu_pd(v + i + 4 * q), d = _mm256_sub_pd(x, mm);
            if (nl)
                d = _mm256_andnot_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(_mm256_castpd_si256(x), nv)), d);
            d1[q] = _mm256_add_pd(d1[q], d);
            d2[q] = _mm256_add_pd(d2[q], _mm256_mul_pd(d, d));
        }
    for (int q = 0; q < 4; q++) {
        _mm256_storeu_pd(l.d1 + 4 * q, d1[q]);
        _mm256_storeu_pd(l.d2 + 4 * q, d2[q]);
    }
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl);
}

K_AVX2 void agg64_avx2_impl(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes64 l;
    lanes64_init(&l, a);
    const __m256i nv = _mm256_set1_epi64x((long long)NULL_F64);
    __m256d s[4], mn[4], mx[4]; /* lanes 4q .. 4q+3 */
    for (int q = 0; q < 4; q++) {
        s[q] = _mm256_setzero_pd();
        mn[q] = _mm256_loadu_pd(l.mn + 4 * q);
        mx[q] = _mm256_loadu_pd(l.mx + 4 * q);
    }
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 4; q++) {
            __m256d x = _mm256_loadu_pd(v + i + 4 * q), xs = x;
            if (nl)
                xs = _mm256_andnot_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(_mm256_castpd_si256(x), nv)), x);
            s[q] = _mm256_add_pd(s[q], xs);
            mn[q] = _mm256_min_pd(x, mn[q]);
            mx[q] = _mm256_max_pd(x, mx[q]);
        }
    for (int q = 0; q < 4; q++) {
        _mm256_storeu_pd(l.s + 4 * q, s[q]);
        _mm256_storeu_pd(l.mn + 4 * q, mn[q]);
        _mm256_storeu_pd(l.mx + 4 * q, mx[q]);
    }
    double sum = lanes64_finish(&l, v, i, n, a, nl);
    if (dv)
        dev64_avx2(v, n, n - nulls, sum, a, dv, nl);
    else
        sum_finish(n - nulls, sum, a);
}

__attribute__((target("avx2"))) static void fold_f64_avx2(const double *v, size_t n, size_t nulls, agg_state *a,
                                                         agg_dev *dv)
{
    if (nulls)
        agg64_avx2_impl(v, n, nulls, a, dv, true);
    else
        agg64_avx2_impl(v, n, 0, a, dv, false);
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
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl);
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
    double sum = lanes64_finish(&l, v, i, n, a, nl);
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
 * The spread's squares take a second pass: in vectors when the run's known
 * values all fit in int32 (each square then below 2^62, summed in halves
 * like the values), else the exact 128-bit scalar loop.
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
    if (mn < INT32_MIN || mx > INT32_MAX) {
        i64_squares(v, 0, n, dx);
        return;
    }
    unsigned __int128 q = 0;
    i = 0;
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
    if (mn < INT32_MIN || mx > INT32_MAX) {
        i64_squares(v, 0, n, dx);
        return;
    }
    unsigned __int128 q = 0;
    i = 0;
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

static size_t (*simd_lttb)(int, const int64_t *, const uint64_t *, size_t, int64_t, double, double, double,
                           double) = lttb_scalar;

size_t lttb_argmax(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax, double ay,
                   double cx, double cy)
{
    return simd_lttb(type, ts, v, n, t0, ax, ay, cx, cy);
}

void (*simd_agg_f32)(const float *v, size_t n, agg_state *a, agg_dev *dv) = agg_scalar;
void (*simd_fold_f32)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv) = fold_f32_scalar;
void (*simd_fold_f64)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv) = fold_f64_scalar;
void (*simd_fold_i64)(const int64_t *v, size_t n, agg_state *a, agg_dev *dx);
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

/* The best kernels for at most `level` (TSDB_CPU caps it through cpu.c, so a
 * forced v2 run uses no AVX at all, as on Sandy Bridge without AVX2). */
void simd_select(int level)
{
    void (*f)(const float *, size_t, agg_state *, agg_dev *) = agg_scalar;
    void (*f32)(const float *, size_t, size_t, agg_state *, agg_dev *) = fold_f32_scalar;
    void (*f64)(const double *, size_t, size_t, agg_state *, agg_dev *) = fold_f64_scalar;
    void (*i64)(const int64_t *, size_t, agg_state *, agg_dev *) = NULL;
    const char *name = "scalar";
#ifdef TSDB_X86
    const char *force = getenv("TSDB_SIMD"); /* "scalar", "avx2" or "avx512", for benchmarking */
    __builtin_cpu_init();
    bool avx512 = level >= CPU_V4 && __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512dq");
    bool avx2 = level >= CPU_V3 && __builtin_cpu_supports("avx2");
    if (avx512 && avx512_downclocks() && !(force && strcmp(force, "avx512") == 0))
        avx512 = false;
    if (force && strcmp(force, "scalar") == 0)
        avx512 = avx2 = false;
    else if (force && strcmp(force, "avx2") == 0)
        avx512 = false;
    if (avx512) {
        f = agg_avx512;
        f32 = fold_f32_avx512;
        f64 = fold_f64_avx512;
        i64 = fold_i64_avx512;
        name = "avx512";
    } else if (avx2) {
        f = agg_avx2;
        f32 = fold_f32_avx2;
        f64 = fold_f64_avx2;
        i64 = fold_i64_avx2;
        name = "avx2";
    }
#endif
    simd_agg_f32 = f;
    simd_fold_f32 = f32;
    simd_fold_f64 = f64;
    simd_fold_i64 = i64;
    simd_level = name;
#ifdef TSDB_LEVELS
    snprintf(simd_desc, sizeof simd_desc, "%s, codecs x86-64-%s", name, cpu_level_name(level));
#else
    snprintf(simd_desc, sizeof simd_desc, "%s", name);
#endif
}

static void simd_pick(void) { simd_select(cpu_level()); }

void simd_init(void) { pthread_once(&simd_once, simd_pick); }
