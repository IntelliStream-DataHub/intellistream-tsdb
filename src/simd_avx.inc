/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The 256-bit float32 and float64 kernels, included twice by simd.c: for AVX2 (KSUF avx2) and for AVX without
 * AVX2 (KSUF avx: Sandy Bridge and Ivy Bridge). These kernels are float work: loads, conversions, adds, min/max,
 * all in AVX. Only the null masks compare integers, which AVX does in 128-bit halves (KS_AVX2 0). The lanes and
 * the order of every addition are simd.c's (32 lanes for float32, 16 for float64), so both copies give the
 * scalar code's bits.
 *
 * Defined by the includer: KSUF (the name suffix), KTARGET (the target attribute), KS_AVX2 (1 or 0).
 */
#define KCAT_(a, b) a##_##b
#define KCAT(a, b) KCAT_(a, b)
#define KN(f) KCAT(f, KSUF)
#define KI static inline __attribute__((target(KTARGET), always_inline))
#define KF __attribute__((target(KTARGET))) static

/* all ones in the float32 lanes of x that hold the null */
KI __m256 KN(null_ps)(__m256 x)
{
#if KS_AVX2
    return _mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_castps_si256(x), _mm256_set1_epi32((int)NULL_F32)));
#else
    const __m128i nv = _mm_set1_epi32((int)NULL_F32);
    __m128i lo = _mm_cmpeq_epi32(_mm_castps_si128(_mm256_castps256_ps128(x)), nv);
    __m128i hi = _mm_cmpeq_epi32(_mm_castps_si128(_mm256_extractf128_ps(x, 1)), nv);
    return _mm256_castsi256_ps(_mm256_insertf128_si256(_mm256_castsi128_si256(lo), hi, 1));
#endif
}

/* the same for 4 float32 values, as a mask of the 4 float64 lanes they convert to */
KI __m256d KN(null_ps_pd)(__m128 x)
{
    __m128i e = _mm_cmpeq_epi32(_mm_castps_si128(x), _mm_set1_epi32((int)NULL_F32));
#if KS_AVX2
    return _mm256_castsi256_pd(_mm256_cvtepi32_epi64(e));
#else
    return _mm256_castsi256_pd(
        _mm256_insertf128_si256(_mm256_castsi128_si256(_mm_unpacklo_epi32(e, e)), _mm_unpackhi_epi32(e, e), 1));
#endif
}

/* all ones in the float64 lanes of x that hold the null */
KI __m256d KN(null_pd)(__m256d x)
{
#if KS_AVX2
    return _mm256_castsi256_pd(_mm256_cmpeq_epi64(_mm256_castpd_si256(x), _mm256_set1_epi64x((long long)NULL_F64)));
#else
    const __m128i nv = _mm_set1_epi64x((long long)NULL_F64);
    __m128i lo = _mm_cmpeq_epi64(_mm_castpd_si128(_mm256_castpd256_pd128(x)), nv);
    __m128i hi = _mm_cmpeq_epi64(_mm_castpd_si128(_mm256_extractf128_pd(x, 1)), nv);
    return _mm256_castsi256_pd(_mm256_insertf128_si256(_mm256_castsi128_si256(lo), hi, 1));
#endif
}

KI void KN(dev)(const float *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev_lanes l;
    double m = s / (double)c;
    const __m256d mm = _mm256_set1_pd(m);
    __m256d d1[8], d2[8]; /* lanes 4q .. 4q+3 */
    for (int q = 0; q < 8; q++)
        d1[q] = d2[q] = _mm256_setzero_pd();
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int q = 0; q < 8; q++) {
            __m128 x = _mm_loadu_ps(v + i + 4 * q);
            __m256d d = _mm256_sub_pd(_mm256_cvtps_pd(x), mm);
            if (nl) /* a null adds +0.0 to its lane: see the top of simd.c */
                d = _mm256_andnot_pd(KN(null_ps_pd)(x), d);
            d1[q] = _mm256_add_pd(d1[q], d);
            d2[q] = _mm256_add_pd(d2[q], _mm256_mul_pd(d, d));
        }
    for (int q = 0; q < 8; q++) {
        _mm256_storeu_pd(l.d1 + 4 * q, d1[q]);
        _mm256_storeu_pd(l.d2 + 4 * q, d2[q]);
    }
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl, LANES);
}

/* sum of the 4 floats as doubles into s */
#define ADD4(s, q) s = _mm256_add_pd(s, _mm256_cvtps_pd(q))

KI void KN(agg_impl)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes l;
    lanes_init(&l, a);
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
            xs = _mm256_andnot_ps(KN(null_ps)(x), x);                                                              \
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
    double s = lanes_finish(&l, v, i, n, a, nl, LANES);
    if (dv)
        KN(dev)(v, n, n - nulls, s, a, dv, nl);
    else
        sum_finish(n - nulls, s, a);
}
#undef ADD4

KF void KN(agg)(const float *v, size_t n, agg_state *a, agg_dev *dv) { KN(agg_impl)(v, n, 0, a, dv, false); }

KF void KN(fold_f32)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        KN(agg_impl)(v, n, nulls, a, dv, true);
    else
        KN(agg_impl)(v, n, 0, a, dv, false);
}

/* float64: 16 lanes, 4 vectors */
KI void KN(dev64)(const double *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev64 l;
    double m = s / (double)c;
    const __m256d mm = _mm256_set1_pd(m);
    __m256d d1[4], d2[4];
    for (int q = 0; q < 4; q++)
        d1[q] = d2[q] = _mm256_setzero_pd();
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 4; q++) {
            __m256d x = _mm256_loadu_pd(v + i + 4 * q), d = _mm256_sub_pd(x, mm);
            if (nl)
                d = _mm256_andnot_pd(KN(null_pd)(x), d);
            d1[q] = _mm256_add_pd(d1[q], d);
            d2[q] = _mm256_add_pd(d2[q], _mm256_mul_pd(d, d));
        }
    for (int q = 0; q < 4; q++) {
        _mm256_storeu_pd(l.d1 + 4 * q, d1[q]);
        _mm256_storeu_pd(l.d2 + 4 * q, d2[q]);
    }
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl, L64);
}

KI void KN(agg64_impl)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes64 l;
    lanes64_init(&l, a);
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
                xs = _mm256_andnot_pd(KN(null_pd)(x), x);
            s[q] = _mm256_add_pd(s[q], xs);
            mn[q] = _mm256_min_pd(x, mn[q]);
            mx[q] = _mm256_max_pd(x, mx[q]);
        }
    for (int q = 0; q < 4; q++) {
        _mm256_storeu_pd(l.s + 4 * q, s[q]);
        _mm256_storeu_pd(l.mn + 4 * q, mn[q]);
        _mm256_storeu_pd(l.mx + 4 * q, mx[q]);
    }
    double sum = lanes64_finish(&l, v, i, n, a, nl, L64);
    if (dv)
        KN(dev64)(v, n, n - nulls, sum, a, dv, nl);
    else
        sum_finish(n - nulls, sum, a);
}

KF void KN(fold_f64)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        KN(agg64_impl)(v, n, nulls, a, dv, true);
    else
        KN(agg64_impl)(v, n, 0, a, dv, false);
}

#undef KF
#undef KI
#undef KN
#undef KCAT
#undef KCAT_
#undef KSUF
#undef KTARGET
#undef KS_AVX2
