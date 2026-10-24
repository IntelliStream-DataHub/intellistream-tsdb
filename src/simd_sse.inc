/*
 * The 128-bit kernels, included twice by simd.c: for x86-64-v2 (KSUF sse, SSE4.2) and for AVX without AVX2
 * (KSUF avx: Sandy Bridge and Ivy Bridge, the same code VEX-encoded). AVX has 256-bit float operations but no
 * 256-bit integer ones, and these kernels are mostly integer work (int64 sums in 32-bit halves, bool and state
 * codes, timestamps): split into 128-bit halves and joined again, a 256-bit version would add more shuffles than
 * it saves. The float32 and float64 folds, which are float work, take simd_avx.inc on AVX; here they are built
 * for SSE only (KS_FLOAT). Every kernel keeps simd.c's lanes and order of float additions, so the results are the
 * scalar code's bits; the integer results are exact in any order.
 *
 * Defined by the includer: KSUF (the name suffix), KTARGET (the target attribute), KS_FLOAT (1 or 0).
 */
#define KCAT_(a, b) a##_##b
#define KCAT(a, b) KCAT_(a, b)
#define KN(f) KCAT(f, KSUF)
#define KI static inline __attribute__((target(KTARGET), always_inline))
#define KF __attribute__((target(KTARGET))) static

#if KS_FLOAT
/* float32: the 32 lanes as 16 vectors of two doubles (lanes 2r, 2r + 1) for the sums and the deviations, 8 of
 * four floats (lanes 4q .. 4q + 3) for min and max */
KI void KN(dev)(const float *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev_lanes l;
    double m = s / (double)c;
    const __m128d mm = _mm_set1_pd(m);
    const __m128i nv = _mm_set1_epi32((int)NULL_F32);
    __m128d d1[16], d2[16];
    for (int r = 0; r < 16; r++)
        d1[r] = d2[r] = _mm_setzero_pd();
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int q = 0; q < 8; q++) {
            __m128 x = _mm_loadu_ps(v + i + 4 * q);
            __m128d lo = _mm_sub_pd(_mm_cvtps_pd(x), mm), hi = _mm_sub_pd(_mm_cvtps_pd(_mm_movehl_ps(x, x)), mm);
            if (nl) { /* a null adds +0.0 to its lane: see the top of simd.c */
                __m128i e = _mm_cmpeq_epi32(_mm_castps_si128(x), nv);
                lo = _mm_andnot_pd(_mm_castsi128_pd(_mm_unpacklo_epi32(e, e)), lo);
                hi = _mm_andnot_pd(_mm_castsi128_pd(_mm_unpackhi_epi32(e, e)), hi);
            }
            d1[2 * q] = _mm_add_pd(d1[2 * q], lo);
            d2[2 * q] = _mm_add_pd(d2[2 * q], _mm_mul_pd(lo, lo));
            d1[2 * q + 1] = _mm_add_pd(d1[2 * q + 1], hi);
            d2[2 * q + 1] = _mm_add_pd(d2[2 * q + 1], _mm_mul_pd(hi, hi));
        }
    for (int r = 0; r < 16; r++) {
        _mm_storeu_pd(l.d1 + 2 * r, d1[r]);
        _mm_storeu_pd(l.d2 + 2 * r, d2[r]);
    }
    dev_finish(&l, v, i, n, m, c, s, a, dv, nl, LANES);
}

KI void KN(agg_impl)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes l;
    lanes_init(&l, a);
    const __m128i nv = _mm_set1_epi32((int)NULL_F32);
    __m128d s[16];
    __m128 mn[8], mx[8];
    for (int r = 0; r < 16; r++)
        s[r] = _mm_setzero_pd();
    for (int q = 0; q < 8; q++) {
        mn[q] = _mm_loadu_ps(l.mn + 4 * q);
        mx[q] = _mm_loadu_ps(l.mx + 4 * q);
    }
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int q = 0; q < 8; q++) {
            __m128 x = _mm_loadu_ps(v + i + 4 * q), xs = x;
            if (nl)
                xs = _mm_andnot_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(_mm_castps_si128(x), nv)), x);
            s[2 * q] = _mm_add_pd(s[2 * q], _mm_cvtps_pd(xs));
            s[2 * q + 1] = _mm_add_pd(s[2 * q + 1], _mm_cvtps_pd(_mm_movehl_ps(xs, xs)));
            mn[q] = _mm_min_ps(x, mn[q]);
            mx[q] = _mm_max_ps(x, mx[q]);
        }
    for (int r = 0; r < 16; r++)
        _mm_storeu_pd(l.s + 2 * r, s[r]);
    for (int q = 0; q < 8; q++) {
        _mm_storeu_ps(l.mn + 4 * q, mn[q]);
        _mm_storeu_ps(l.mx + 4 * q, mx[q]);
    }
    double sum = lanes_finish(&l, v, i, n, a, nl, LANES);
    if (dv)
        KN(dev)(v, n, n - nulls, sum, a, dv, nl);
    else
        sum_finish(n - nulls, sum, a);
}

KF void KN(agg)(const float *v, size_t n, agg_state *a, agg_dev *dv) { KN(agg_impl)(v, n, 0, a, dv, false); }

KF void KN(fold_f32)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        KN(agg_impl)(v, n, nulls, a, dv, true);
    else
        KN(agg_impl)(v, n, 0, a, dv, false);
}

/* float64: the 16 lanes as 8 vectors of two (lanes 2q, 2q + 1) */
KI void KN(dev64)(const double *v, size_t n, uint64_t c, double s, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!c)
        return;
    dev64 l;
    double m = s / (double)c;
    const __m128d mm = _mm_set1_pd(m);
    const __m128i nv = _mm_set1_epi64x((long long)NULL_F64);
    __m128d d1[8], d2[8];
    for (int q = 0; q < 8; q++)
        d1[q] = d2[q] = _mm_setzero_pd();
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 8; q++) {
            __m128d x = _mm_loadu_pd(v + i + 2 * q), d = _mm_sub_pd(x, mm);
            if (nl)
                d = _mm_andnot_pd(_mm_castsi128_pd(_mm_cmpeq_epi64(_mm_castpd_si128(x), nv)), d);
            d1[q] = _mm_add_pd(d1[q], d);
            d2[q] = _mm_add_pd(d2[q], _mm_mul_pd(d, d));
        }
    for (int q = 0; q < 8; q++) {
        _mm_storeu_pd(l.d1 + 2 * q, d1[q]);
        _mm_storeu_pd(l.d2 + 2 * q, d2[q]);
    }
    dev64_finish(&l, v, i, n, m, c, s, a, dv, nl, L64);
}

KI void KN(agg64_impl)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl)
{
    if (!n)
        return;
    lanes64 l;
    lanes64_init(&l, a);
    const __m128i nv = _mm_set1_epi64x((long long)NULL_F64);
    __m128d s[8], mn[8], mx[8];
    for (int q = 0; q < 8; q++) {
        s[q] = _mm_setzero_pd();
        mn[q] = _mm_loadu_pd(l.mn + 2 * q);
        mx[q] = _mm_loadu_pd(l.mx + 2 * q);
    }
    size_t i = 0;
    for (; i + L64 <= n; i += L64)
        for (int q = 0; q < 8; q++) {
            __m128d x = _mm_loadu_pd(v + i + 2 * q), xs = x;
            if (nl)
                xs = _mm_andnot_pd(_mm_castsi128_pd(_mm_cmpeq_epi64(_mm_castpd_si128(x), nv)), x);
            s[q] = _mm_add_pd(s[q], xs);
            mn[q] = _mm_min_pd(x, mn[q]);
            mx[q] = _mm_max_pd(x, mx[q]);
        }
    for (int q = 0; q < 8; q++) {
        _mm_storeu_pd(l.s + 2 * q, s[q]);
        _mm_storeu_pd(l.mn + 2 * q, mn[q]);
        _mm_storeu_pd(l.mx + 2 * q, mx[q]);
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
#endif /* KS_FLOAT */

/* int64: as fold_i64_avx2 in vectors of two; per lane and block at most 2^27 values (x and y), as there */
KI int64_t KN(hmin2)(__m128i x)
{
    int64_t t[2];
    _mm_storeu_si128((__m128i *)t, x);
    return t[1] < t[0] ? t[1] : t[0];
}

KI int64_t KN(hmax2)(__m128i x)
{
    int64_t t[2];
    _mm_storeu_si128((__m128i *)t, x);
    return t[1] > t[0] ? t[1] : t[0];
}

KF void KN(fold_i64)(const int64_t *v, size_t n, agg_state *a, agg_dev *dx)
{
    const __m128i nv = _mm_set1_epi64x(INT64_MIN), lom = _mm_set1_epi64x(0xffffffff),
                  maxv = _mm_set1_epi64x(INT64_MAX);
    __m128i mn0 = maxv, mn1 = maxv, mx0 = nv, mx1 = nv;
    __int128 s = 0;
    uint64_t c = 0;
    size_t i = 0;
    while (n - i >= 4) {
        size_t end = i + ((n - i) / 4 < I64_BLOCK ? (n - i) / 4 : I64_BLOCK) * 4, start = i;
        __m128i slo = _mm_setzero_si128(), shi = slo, sneg = slo, snull = slo;
#define STEP(x, mn, mx)                                                                                            \
    do {                                                                                                           \
        __m128i m = _mm_cmpeq_epi64(x, nv), xs = _mm_andnot_si128(m, x);                                           \
        slo = _mm_add_epi64(slo, _mm_and_si128(xs, lom));                                                          \
        shi = _mm_add_epi64(shi, _mm_srli_epi64(xs, 32));                                                          \
        sneg = _mm_add_epi64(sneg, _mm_srli_epi64(xs, 63));                                                        \
        snull = _mm_sub_epi64(snull, m);                                                                           \
        __m128i xm = _mm_blendv_epi8(x, maxv, m);                                                                  \
        mn = _mm_blendv_epi8(mn, xm, _mm_cmpgt_epi64(mn, xm));                                                     \
        mx = _mm_blendv_epi8(mx, x, _mm_cmpgt_epi64(x, mx));                                                       \
    } while (0)
        for (; i < end; i += 4) {
            __m128i x = _mm_loadu_si128((const __m128i *)(v + i));
            __m128i y = _mm_loadu_si128((const __m128i *)(v + i + 2));
            STEP(x, mn0, mx0);
            STEP(y, mn1, mx1);
        }
#undef STEP
        uint64_t lo[2], hi[2], neg[2], nul[2];
        _mm_storeu_si128((__m128i *)lo, slo);
        _mm_storeu_si128((__m128i *)hi, shi);
        _mm_storeu_si128((__m128i *)neg, sneg);
        _mm_storeu_si128((__m128i *)nul, snull);
        s += i64_combine(lo, hi, neg, 2);
        c += (end - start) - (nul[0] + nul[1]);
    }
    int64_t mn = KN(hmin2)(mn0), mx = KN(hmax2)(mx0), mn_1 = KN(hmin2)(mn1), mx_1 = KN(hmax2)(mx1);
    mn = mn_1 < mn ? mn_1 : mn;
    mx = mx_1 > mx ? mx_1 : mx;
    i64_tail(v, i, n, &s, &c, &mn, &mx);
    i64_finish(a, s, c, mn, mx);
    if (!dx || !c)
        return;
    i = 0;
    if (mn < INT32_MIN || mx > INT32_MAX) {
        while (n - i >= 2) {
            size_t end = i + ((n - i) / 2 < I64_BLOCK ? (n - i) / 2 : I64_BLOCK) * 2;
            __m128i a0 = _mm_setzero_si128(), a1 = a0, b0 = a0, b1 = a0, c0 = a0, c1 = a0, zero = a0;
            for (; i < end; i += 2) {
                __m128i x = _mm_loadu_si128((const __m128i *)(v + i));
                __m128i xs = _mm_andnot_si128(_mm_cmpeq_epi64(x, nv), x), sg = _mm_cmpgt_epi64(zero, xs);
                __m128i u = _mm_sub_epi64(_mm_xor_si128(xs, sg), sg), h = _mm_srli_epi64(u, 32);
                __m128i ll = _mm_mul_epu32(u, u), hl = _mm_mul_epu32(h, u), hh = _mm_mul_epu32(h, h);
                a0 = _mm_add_epi64(a0, _mm_and_si128(ll, lom));
                a1 = _mm_add_epi64(a1, _mm_srli_epi64(ll, 32));
                b0 = _mm_add_epi64(b0, _mm_and_si128(hl, lom));
                b1 = _mm_add_epi64(b1, _mm_srli_epi64(hl, 32));
                c0 = _mm_add_epi64(c0, _mm_and_si128(hh, lom));
                c1 = _mm_add_epi64(c1, _mm_srli_epi64(hh, 32));
            }
            uint64_t t[6][2];
            _mm_storeu_si128((__m128i *)t[0], a0);
            _mm_storeu_si128((__m128i *)t[1], a1);
            _mm_storeu_si128((__m128i *)t[2], b0);
            _mm_storeu_si128((__m128i *)t[3], b1);
            _mm_storeu_si128((__m128i *)t[4], c0);
            _mm_storeu_si128((__m128i *)t[5], c1);
            i64_sq_wide(dx, t[0][0] + t[0][1], t[1][0] + t[1][1], t[2][0] + t[2][1], t[3][0] + t[3][1],
                        t[4][0] + t[4][1], t[5][0] + t[5][1]);
        }
        i64_squares(v, i, n, dx);
        return;
    }
    unsigned __int128 q = 0;
    while (n - i >= 2) {
        size_t end = i + ((n - i) / 2 < I64_BLOCK ? (n - i) / 2 : I64_BLOCK) * 2;
        __m128i qlo = _mm_setzero_si128(), qhi = qlo;
        for (; i < end; i += 2) {
            __m128i x = _mm_loadu_si128((const __m128i *)(v + i));
            __m128i xs = _mm_andnot_si128(_mm_cmpeq_epi64(x, nv), x), sq = _mm_mul_epi32(xs, xs);
            qlo = _mm_add_epi64(qlo, _mm_and_si128(sq, lom));
            qhi = _mm_add_epi64(qhi, _mm_srli_epi64(sq, 32));
        }
        uint64_t lo[2], hi[2];
        _mm_storeu_si128((__m128i *)lo, qlo);
        _mm_storeu_si128((__m128i *)hi, qhi);
        q += (unsigned __int128)(lo[0] + lo[1]) + ((unsigned __int128)(hi[0] + hi[1]) << 32);
    }
    isq_add(dx, q, 0);
    i64_squares(v, i, n, dx);
}

/* bool and state codes: as fold_u32_avx2 in vectors of four; per 32-bit null counter and block 2^23 */
KI void KN(fold_u32_impl)(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx, const bool sq)
{
    const __m128i nv = _mm_set1_epi32((int)null), lom = _mm_set1_epi64x(0xffffffff);
    __m128i mn0 = _mm_set1_epi32(-1), mn1 = mn0, mx0 = _mm_setzero_si128(), mx1 = mx0;
    unsigned __int128 s = 0, q = 0;
    uint64_t c = 0;
    size_t i = 0;
    while (n - i >= 8) {
        size_t end = i + ((n - i) < U32_BLOCK ? (n - i) : U32_BLOCK) / 8 * 8, start = i;
        __m128i s0 = _mm_setzero_si128(), s1 = s0, nul0 = s0, nul1 = s0, ql0 = s0, ql1 = s0, qh0 = s0, qh1 = s0;
#define STEP(x, sm, nul, mn, mx, ql, qh)                                                                            \
    do {                                                                                                           \
        __m128i m = _mm_cmpeq_epi32(x, nv), xs = _mm_andnot_si128(m, x), od = _mm_srli_epi64(xs, 32);              \
        nul = _mm_sub_epi32(nul, m);                                                                               \
        mn = _mm_min_epu32(mn, _mm_or_si128(x, m));                                                                \
        mx = _mm_max_epu32(mx, xs);                                                                                \
        sm = _mm_add_epi64(sm, _mm_add_epi64(_mm_and_si128(xs, lom), od));                                         \
        if (sq) {                                                                                                  \
            __m128i e = _mm_mul_epu32(xs, xs), o = _mm_mul_epu32(od, od);                                          \
            ql = _mm_add_epi64(ql, _mm_add_epi64(_mm_and_si128(e, lom), _mm_and_si128(o, lom)));                   \
            qh = _mm_add_epi64(qh, _mm_add_epi64(_mm_srli_epi64(e, 32), _mm_srli_epi64(o, 32)));                   \
        }                                                                                                          \
    } while (0)
        for (; i < end; i += 8) {
            __m128i x = _mm_loadu_si128((const __m128i *)(v + i));
            __m128i y = _mm_loadu_si128((const __m128i *)(v + i + 4));
            STEP(x, s0, nul0, mn0, mx0, ql0, qh0);
            STEP(y, s1, nul1, mn1, mx1, ql1, qh1);
        }
#undef STEP
        uint64_t t[2], u[2], w[2];
        uint32_t k[4];
        _mm_storeu_si128((__m128i *)t, _mm_add_epi64(s0, s1));
        _mm_storeu_si128((__m128i *)u, _mm_add_epi64(ql0, ql1));
        _mm_storeu_si128((__m128i *)w, _mm_add_epi64(qh0, qh1));
        _mm_storeu_si128((__m128i *)k, _mm_add_epi32(nul0, nul1));
        for (int j = 0; j < 2; j++) {
            s += t[j];
            q += u[j] + ((unsigned __int128)w[j] << 32);
        }
        c += (end - start) - ((uint64_t)k[0] + k[1] + k[2] + k[3]);
    }
    uint32_t lo[4], hi[4], mn = UINT32_MAX, mx = 0;
    _mm_storeu_si128((__m128i *)lo, _mm_min_epu32(mn0, mn1));
    _mm_storeu_si128((__m128i *)hi, _mm_max_epu32(mx0, mx1));
    for (int j = 0; j < 4; j++) {
        mn = lo[j] < mn ? lo[j] : mn;
        mx = hi[j] > mx ? hi[j] : mx;
    }
    u32_tail(v, i, n, null, &s, &c, &q, &mn, &mx);
    u32_finish(a, dx, s, c, q, mn, mx);
}

KF void KN(fold_u32)(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *dx)
{
    if (dx)
        KN(fold_u32_impl)(v, n, null, a, dx, true);
    else
        KN(fold_u32_impl)(v, n, null, a, NULL, false);
}

/* (double) of unsigned and signed 64-bit lanes, rounded once: as u64_pd_avx2 */
KI __m128d KN(u64_pd)(__m128i x)
{
    __m128i lo = _mm_blend_epi16(x, _mm_castpd_si128(_mm_set1_pd(0x1p52)), 0xcc); /* the high 32 bits */
    __m128i hi = _mm_or_si128(_mm_srli_epi64(x, 32), _mm_castpd_si128(_mm_set1_pd(0x1p84)));
    __m128d h = _mm_sub_pd(_mm_castsi128_pd(hi), _mm_set1_pd(0x1p84 + 0x1p52));
    return _mm_add_pd(h, _mm_castsi128_pd(lo));
}

KI __m128d KN(i64_pd)(__m128i x)
{
    x = _mm_xor_si128(x, _mm_set1_epi64x(INT64_MIN)); /* x + 2^63, unsigned */
    __m128i lo = _mm_blend_epi16(x, _mm_castpd_si128(_mm_set1_pd(0x1p52)), 0xcc);
    __m128i hi = _mm_or_si128(_mm_srli_epi64(x, 32), _mm_castpd_si128(_mm_set1_pd(0x1p84)));
    __m128d h = _mm_sub_pd(_mm_castsi128_pd(hi), _mm_set1_pd(0x1p84 + 0x1p63 + 0x1p52));
    return _mm_add_pd(h, _mm_castsi128_pd(lo));
}

/* points p, p + 1 of a granule's slots as doubles, and which are not null (all ones) */
KI __m128d KN(twa_ld)(int type, const void *v, size_t p, __m128i *ok)
{
    const __m128i ones = _mm_set1_epi64x(-1);
    switch (type) {
    case TSDB_FLOAT32: {
        __m128i b = _mm_loadl_epi64((const __m128i *)((const uint32_t *)v + p));
        *ok = _mm_xor_si128(_mm_cvtepi32_epi64(_mm_cmpeq_epi32(b, _mm_set1_epi32((int)NULL_F32))), ones);
        return _mm_cvtps_pd(_mm_castsi128_ps(b));
    }
    case TSDB_FLOAT64: {
        __m128i b = _mm_loadu_si128((const __m128i *)((const uint64_t *)v + p));
        *ok = _mm_xor_si128(_mm_cmpeq_epi64(b, _mm_set1_epi64x((long long)NULL_F64)), ones);
        return _mm_castsi128_pd(b);
    }
    case TSDB_INT64: {
        __m128i b = _mm_loadu_si128((const __m128i *)((const int64_t *)v + p));
        *ok = _mm_xor_si128(_mm_cmpeq_epi64(b, _mm_set1_epi64x(NULL_I64)), ones);
        return KN(i64_pd)(b);
    }
    default: {
        __m128i b = _mm_cvtepu32_epi64(_mm_loadl_epi64((const __m128i *)((const uint32_t *)v + p)));
        *ok = _mm_xor_si128(_mm_cmpeq_epi64(b, _mm_set1_epi64x(NULL_BOOL)), ones);
        return KN(u64_pd)(b);
    }
    }
}

/* TWA: segment s into lane s % 16, as twa_avx2_impl; lanes 2q, 2q + 1 in vector q */
KI void KN(twa_impl)(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    const __m128i bias = _mm_set1_epi64x(INT64_MIN);
    const __m128d half = _mm_set1_pd(0.5);
    __m128d st[8], li[8];
    __m128i sms = _mm_setzero_si128(), lms = sms, gf = bias; /* the gap biased, for signed compares */
    for (int q = 0; q < 8; q++)
        st[q] = li[q] = _mm_setzero_pd();
    size_t s = 0;
    for (; s + L64 + 1 <= n; s += L64)
        for (int q = 0; q < 8; q++) {
            size_t p = s + 2 * (size_t)q;
            __m128i dt = _mm_sub_epi64(_mm_loadu_si128((const __m128i *)(ts + p + 1)),
                                       _mm_loadu_si128((const __m128i *)(ts + p)));
            __m128i db = _mm_xor_si128(dt, bias), okp, ok;
            gf = _mm_blendv_epi8(gf, db, _mm_cmpgt_epi64(db, gf));
            __m128d vp = KN(twa_ld)(type, v, p, &okp), x = KN(twa_ld)(type, v, p + 1, &ok), d = KN(u64_pd)(dt);
            __m128i both = _mm_and_si128(okp, ok);
            st[q] = _mm_add_pd(st[q], _mm_and_pd(_mm_castsi128_pd(okp), _mm_mul_pd(vp, d)));
            li[q] = _mm_add_pd(li[q],
                               _mm_and_pd(_mm_castsi128_pd(both), _mm_mul_pd(_mm_mul_pd(_mm_add_pd(vp, x), half), d)));
            sms = _mm_add_epi64(sms, _mm_and_si128(okp, dt));
            lms = _mm_add_epi64(lms, _mm_and_si128(both, dt));
        }
    double sl[L64], ll[L64];
    uint64_t a[2], b[2], g[2];
    for (int q = 0; q < 8; q++) {
        _mm_storeu_pd(sl + 2 * q, st[q]);
        _mm_storeu_pd(ll + 2 * q, li[q]);
    }
    _mm_storeu_si128((__m128i *)a, sms);
    _mm_storeu_si128((__m128i *)b, lms);
    _mm_storeu_si128((__m128i *)g, _mm_xor_si128(gf, bias));
    *o = (twa_sums){.sms = a[0] + a[1], .lms = b[0] + b[1], .gap = g[0] > g[1] ? g[0] : g[1]};
    twa_tail(type, ts, v, s, n, sl, ll, o, L64);
}

KF void KN(twa)(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    switch (type) {
    case TSDB_FLOAT32: KN(twa_impl)(TSDB_FLOAT32, ts, v, n, o); break;
    case TSDB_FLOAT64: KN(twa_impl)(TSDB_FLOAT64, ts, v, n, o); break;
    case TSDB_INT64: KN(twa_impl)(TSDB_INT64, ts, v, n, o); break;
    default: KN(twa_impl)(TSDB_BOOL, ts, v, n, o); break;
    }
}

/* LTTB: as lttb_avx2_impl, 8 lanes in 4 vectors of two */
KI __m128d KN(lttb_y)(int type, __m128i x)
{
    switch (type) {
    case TSDB_FLOAT32: return _mm_cvtps_pd(_mm_castsi128_ps(_mm_shuffle_epi32(x, _MM_SHUFFLE(2, 0, 2, 0))));
    case TSDB_FLOAT64: return _mm_castsi128_pd(x);
    case TSDB_INT64: return KN(i64_pd)(x);
    default: return KN(u64_pd)(x);
    }
}

KI size_t KN(lttb_impl)(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax, double ay,
                        double cx, double cy)
{
    const __m128d kx = _mm_set1_pd(ax - cx), ky = _mm_set1_pd(cy - ay), vax = _mm_set1_pd(ax),
                  vay = _mm_set1_pd(ay), half = _mm_set1_pd(0.5), sign = _mm_set1_pd(-0.0);
    const __m128i vt0 = _mm_set1_epi64x(t0), eight = _mm_set1_epi64x(8);
    __m128d b[4];
    __m128i k[4], ix[4];
    for (int q = 0; q < 4; q++) {
        b[q] = _mm_set1_pd(-1);
        k[q] = ix[q] = _mm_set_epi64x(2 * q + 1, 2 * q);
    }
    size_t j = 0;
    for (; j + 8 <= n; j += 8)
        for (int q = 0; q < 4; q++) {
            size_t p = j + 2 * (size_t)q;
            __m128d y = KN(lttb_y)(type, _mm_loadu_si128((const __m128i *)(v + p)));
            __m128d x = KN(u64_pd)(_mm_sub_epi64(_mm_loadu_si128((const __m128i *)(ts + p)), vt0));
            __m128d a = _mm_mul_pd(
                _mm_andnot_pd(sign, _mm_sub_pd(_mm_mul_pd(kx, _mm_sub_pd(y, vay)), _mm_mul_pd(_mm_sub_pd(vax, x), ky))),
                half);
            __m128d g = _mm_cmpgt_pd(a, b[q]);
            b[q] = _mm_blendv_pd(b[q], a, g);
            k[q] = _mm_blendv_epi8(k[q], ix[q], _mm_castpd_si128(g));
            ix[q] = _mm_add_epi64(ix[q], eight);
        }
    double bl[8];
    uint64_t kl[8];
    for (int q = 0; q < 4; q++) {
        _mm_storeu_pd(bl + 2 * q, b[q]);
        _mm_storeu_si128((__m128i *)(kl + 2 * q), k[q]);
    }
    double best = -1;
    size_t kk = 0;
    for (int l = 0; l < 8; l++)
        if (bl[l] > best || (bl[l] == best && bl[l] >= 0 && kl[l] < kk)) {
            best = bl[l];
            kk = kl[l];
        }
    for (; j < n; j++) {
        double x = (double)((uint64_t)ts[j] - (uint64_t)t0), y = lttb_y(type, v[j]);
        double a = fabs((ax - cx) * (y - ay) - (ax - x) * (cy - ay)) * 0.5;
        if (a > best) {
            best = a;
            kk = j;
        }
    }
    return kk;
}

KF size_t KN(lttb)(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax, double ay,
                   double cx, double cy)
{
    switch (type) {
    case TSDB_FLOAT32: return KN(lttb_impl)(TSDB_FLOAT32, ts, v, n, t0, ax, ay, cx, cy);
    case TSDB_FLOAT64: return KN(lttb_impl)(TSDB_FLOAT64, ts, v, n, t0, ax, ay, cx, cy);
    case TSDB_INT64: return KN(lttb_impl)(TSDB_INT64, ts, v, n, t0, ax, ay, cx, cy);
    default: return KN(lttb_impl)(TSDB_BOOL, ts, v, n, t0, ax, ay, cx, cy);
    }
}

#undef KF
#undef KI
#undef KN
#undef KCAT
#undef KCAT_
#undef KSUF
#undef KTARGET
#undef KS_FLOAT
