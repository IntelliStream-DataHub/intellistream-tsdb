/*
 * The 128-bit kernels, included twice by simd.c: for x86-64-v2 (KSUF sse, SSE4.2) and for AVX without AVX2
 * (KSUF avx: Sandy Bridge and Ivy Bridge, the same code VEX-encoded). AVX has 256-bit float operations but no
 * 256-bit integer ones, and these kernels are mostly integer work (int64 sums in 32-bit halves, bool and state
 * codes, timestamps): split into 128-bit halves and joined again, a 256-bit version would add more shuffles than
 * it saves. The AVX set takes simd_avx.inc's 256-bit float32 and float64 folds for long runs and these for short
 * ones. Every kernel keeps simd.c's lanes and order of float additions, so the results are the scalar code's
 * bits; the integer results are exact in any order.
 *
 * Defined by the includer: KSUF (the name suffix), KTARGET (the target attribute).
 */
#define KCAT_(a, b) a##_##b
#define KCAT(a, b) KCAT_(a, b)
#define KN(f) KCAT(f, KSUF)
#define KI static inline __attribute__((target(KTARGET), always_inline))
#define KF __attribute__((target(KTARGET), unused)) static

/*
 * float32 and float64. The lanes stay in registers to the end: a last partial group goes into its vectors under
 * a mask of the lanes it fills (an empty lane adds +0.0 to its sum and an all-ones NaN to min and max, which
 * MINPD/MAXPD pass over: the scalar code's untouched lane), and the halving runs on the vectors. So a short run,
 * a bucket of a few rows, costs little more than its values, and these also serve values.c's short runs
 * (KN(small_*), counting the nulls on the way; the AVX set's 256-bit kernels keep the long ones). Lanes and
 * order as in simd.c: float64 16 lanes as 8 vectors of two (lanes 2q, 2q + 1); float32 32 lanes as 16 vectors of
 * two doubles (lanes 2r, 2r + 1) for the sums and the deviations, 8 of four floats (lanes 4q .. 4q + 3) for min
 * and max.
 */

/* values v[i + j] for lanes j < k of a vector of four floats (1 <= k <= 3), the others +0.0; without reading on */
KI __m128 KN(part_ps)(const float *p, size_t k)
{
    if (k == 1)
        return _mm_load_ss(p);
    __m128 x = _mm_castsi128_ps(_mm_loadl_epi64((const __m128i *)p));
    return k == 2 ? x : _mm_movelh_ps(x, _mm_load_ss(p + 2));
}

/* all ones in the lanes from k on */
KI __m128i KN(beyond)(size_t k) { return _mm_cmpgt_epi32(_mm_setr_epi32(0, 1, 2, 3), _mm_set1_epi32((int)k - 1)); }

/* pass one over four floats at p (part: k of them, the lanes from k on past the run) into group q */
#define K32_SUM(p, k, part, q)                                                                                      \
    do {                                                                                                           \
        __m128 x_ = (part) ? KN(part_ps)(p, k) : _mm_loadu_ps(p), xm_ = x_, xs_ = x_;                             \
        if (part)                                                                                                  \
            xm_ = _mm_or_ps(x_, _mm_castsi128_ps(KN(beyond)(k)));                                                  \
        if (nl) {                                                                                                  \
            __m128i e_ = _mm_cmpeq_epi32(_mm_castps_si128(x_), nv);                                                \
            xs_ = _mm_andnot_ps(_mm_castsi128_ps(e_), x_);                                                         \
            if (cnt)                                                                                               \
                nc = _mm_sub_epi32(nc, e_);                                                                        \
        }                                                                                                          \
        s[2 * (q)] = _mm_add_pd(s[2 * (q)], _mm_cvtps_pd(xs_));                                                   \
        s[2 * (q) + 1] = _mm_add_pd(s[2 * (q) + 1], _mm_cvtps_pd(_mm_movehl_ps(xs_, xs_)));                       \
        mn[q] = _mm_min_ps(xm_, mn[q]);                                                                            \
        mx[q] = _mm_max_ps(xm_, mx[q]);                                                                            \
    } while (0)

/* pass two: a null or a lane past the run adds +0.0 (see the top of simd.c) */
#define K32_DEV(p, k, part, q)                                                                                      \
    do {                                                                                                           \
        __m128 x_ = (part) ? KN(part_ps)(p, k) : _mm_loadu_ps(p);                                                 \
        __m128d lo_ = _mm_sub_pd(_mm_cvtps_pd(x_), mm), hi_ = _mm_sub_pd(_mm_cvtps_pd(_mm_movehl_ps(x_, x_)), mm); \
        if (nl || (part)) {                                                                                        \
            __m128i e_ = (part) ? KN(beyond)(k) : _mm_setzero_si128();                                             \
            if (nl)                                                                                                \
                e_ = _mm_or_si128(e_, _mm_cmpeq_epi32(_mm_castps_si128(x_), nv));                                  \
            lo_ = _mm_andnot_pd(_mm_castsi128_pd(_mm_unpacklo_epi32(e_, e_)), lo_);                                \
            hi_ = _mm_andnot_pd(_mm_castsi128_pd(_mm_unpackhi_epi32(e_, e_)), hi_);                                \
        }                                                                                                          \
        d1[2 * (q)] = _mm_add_pd(d1[2 * (q)], lo_);                                                               \
        d2[2 * (q)] = _mm_add_pd(d2[2 * (q)], _mm_mul_pd(lo_, lo_));                                              \
        d1[2 * (q) + 1] = _mm_add_pd(d1[2 * (q) + 1], hi_);                                                       \
        d2[2 * (q) + 1] = _mm_add_pd(d2[2 * (q) + 1], _mm_mul_pd(hi_, hi_));                                      \
    } while (0)

/* G groups of four lanes in use (8: all 32, with whole groups of 32 values; fewer: a run of at most 4G values,
 * whose lanes from 4G on would stay empty and change nothing in the halving). G is a constant in each copy, so
 * the loops unroll and the lanes stay in registers. */
KI void KN(f32_core)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl,
                     const bool cnt, const int G)
{
    if (!n)
        return;
    const __m128i nv = _mm_set1_epi32((int)NULL_F32);
    __m128d s[16];
    __m128 mn[8], mx[8];
    __m128i nc = _mm_setzero_si128();
    _Pragma("GCC unroll 8") for (int q = 0; q < G; q++)
    {
        s[2 * q] = s[2 * q + 1] = _mm_setzero_pd();
        mn[q] = _mm_set1_ps((float)a->min); /* the state holds float32 values here */
        mx[q] = _mm_set1_ps((float)a->max);
    }
    size_t i = 0;
    if (G == 8)
        for (; i + LANES <= n; i += LANES)
            _Pragma("GCC unroll 8") for (int q = 0; q < 8; q++) K32_SUM(v + i + 4 * q, 4, false, q);
    _Pragma("GCC unroll 8") for (int q = 0; q < G; q++)
    {
        size_t j = i + (size_t)(4 * q);
        if (j + 4 <= n)
            K32_SUM(v + j, 4, false, q);
        else if (j < n)
            K32_SUM(v + j, n - j, true, q);
    }
    _Pragma("GCC unroll 8") for (int w = G; w >= 1; w /= 2)
        _Pragma("GCC unroll 8") for (int r = 0; r < w; r++) s[r] = _mm_add_pd(s[r], s[r + w]);
    double sum = _mm_cvtsd_f64(_mm_add_sd(s[0], _mm_unpackhi_pd(s[0], s[0])));
    _Pragma("GCC unroll 8") for (int w = G / 2; w >= 1; w /= 2)
        _Pragma("GCC unroll 8") for (int q = 0; q < w; q++)
    {
        mn[q] = _mm_min_ps(mn[q + w], mn[q]);
        mx[q] = _mm_max_ps(mx[q + w], mx[q]);
    }
    __m128 m0 = _mm_min_ps(_mm_movehl_ps(mn[0], mn[0]), mn[0]), x0 = _mm_max_ps(_mm_movehl_ps(mx[0], mx[0]), mx[0]);
    float lo = _mm_cvtss_f32(_mm_min_ss(_mm_shuffle_ps(m0, m0, 1), m0));
    float hi = _mm_cvtss_f32(_mm_max_ss(_mm_shuffle_ps(x0, x0, 1), x0));
    a->min = lo < a->min ? lo : a->min;
    a->max = hi > a->max ? hi : a->max;
    if (cnt) {
        nc = _mm_add_epi32(nc, _mm_shuffle_epi32(nc, _MM_SHUFFLE(1, 0, 3, 2)));
        nc = _mm_add_epi32(nc, _mm_shuffle_epi32(nc, _MM_SHUFFLE(2, 3, 0, 1)));
        nulls = (uint32_t)_mm_cvtsi128_si32(nc);
    }
    uint64_t c = n - nulls;
    if (!dv) {
        sum_finish(c, sum, a);
        return;
    }
    if (!c)
        return;
    const __m128d mm = _mm_set1_pd(sum / (double)c);
    __m128d d1[16], d2[16];
    _Pragma("GCC unroll 16") for (int r = 0; r < 2 * G; r++) d1[r] = d2[r] = _mm_setzero_pd();
    i = 0;
    if (G == 8)
        for (; i + LANES <= n; i += LANES)
            _Pragma("GCC unroll 8") for (int q = 0; q < 8; q++) K32_DEV(v + i + 4 * q, 4, false, q);
    _Pragma("GCC unroll 8") for (int q = 0; q < G; q++)
    {
        size_t j = i + (size_t)(4 * q);
        if (j + 4 <= n)
            K32_DEV(v + j, 4, false, q);
        else if (j < n)
            K32_DEV(v + j, n - j, true, q);
    }
    _Pragma("GCC unroll 8") for (int w = G; w >= 1; w /= 2)
        _Pragma("GCC unroll 8") for (int r = 0; r < w; r++)
    {
        d1[r] = _mm_add_pd(d1[r], d1[r + w]);
        d2[r] = _mm_add_pd(d2[r], d2[r + w]);
    }
    double r1 = _mm_cvtsd_f64(_mm_add_sd(d1[0], _mm_unpackhi_pd(d1[0], d1[0])));
    double r2 = _mm_cvtsd_f64(_mm_add_sd(d2[0], _mm_unpackhi_pd(d2[0], d2[0])));
    agg_add_run(a, dv, c, sum, r1, r2);
    a->sum = canon_nan(a->sum);
}

/* float64: two lanes at p (part: lane 1 is past the run) into pair q */
#define K64_SUM(p, part, q)                                                                                         \
    do {                                                                                                           \
        __m128d x_ = (part) ? _mm_load_sd(p) : _mm_loadu_pd(p), xm_ = x_, xs_ = x_;                               \
        if (part)                                                                                                  \
            xm_ = _mm_or_pd(x_, _mm_castsi128_pd(_mm_set_epi64x(-1, 0)));                                          \
        if (nl) {                                                                                                  \
            __m128i e_ = _mm_cmpeq_epi64(_mm_castpd_si128(x_), nv);                                                \
            xs_ = _mm_andnot_pd(_mm_castsi128_pd(e_), x_);                                                         \
            if (cnt)                                                                                               \
                nc = _mm_sub_epi64(nc, e_);                                                                        \
        }                                                                                                          \
        s[q] = _mm_add_pd(s[q], xs_);                                                                              \
        mn[q] = _mm_min_pd(xm_, mn[q]);                                                                            \
        mx[q] = _mm_max_pd(xm_, mx[q]);                                                                            \
    } while (0)

#define K64_DEV(p, part, q)                                                                                         \
    do {                                                                                                           \
        __m128d x_ = (part) ? _mm_load_sd(p) : _mm_loadu_pd(p), d_ = _mm_sub_pd(x_, mm);                          \
        if (nl)                                                                                                    \
            d_ = _mm_andnot_pd(_mm_castsi128_pd(_mm_cmpeq_epi64(_mm_castpd_si128(x_), nv)), d_);                   \
        if (part)                                                                                                  \
            d_ = _mm_move_sd(_mm_setzero_pd(), d_);                                                                \
        d1[q] = _mm_add_pd(d1[q], d_);                                                                             \
        d2[q] = _mm_add_pd(d2[q], _mm_mul_pd(d_, d_));                                                             \
    } while (0)

/* P pairs of lanes in use (8: all 16; fewer: a run of at most 2P values), as G above */
KI void KN(f64_core)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv, const bool nl,
                     const bool cnt, const int P)
{
    if (!n)
        return;
    const __m128i nv = _mm_set1_epi64x((long long)NULL_F64);
    __m128d s[8], mn[8], mx[8];
    __m128i nc = _mm_setzero_si128();
    _Pragma("GCC unroll 8") for (int q = 0; q < P; q++)
    {
        s[q] = _mm_setzero_pd();
        mn[q] = _mm_set1_pd(a->min);
        mx[q] = _mm_set1_pd(a->max);
    }
    size_t i = 0;
    if (P == 8)
        for (; i + L64 <= n; i += L64)
            _Pragma("GCC unroll 8") for (int q = 0; q < 8; q++) K64_SUM(v + i + 2 * q, false, q);
    _Pragma("GCC unroll 8") for (int q = 0; q < P; q++)
    {
        size_t j = i + (size_t)(2 * q);
        if (j + 2 <= n)
            K64_SUM(v + j, false, q);
        else if (j < n)
            K64_SUM(v + j, true, q);
    }
    _Pragma("GCC unroll 8") for (int w = P / 2; w >= 1; w /= 2)
        _Pragma("GCC unroll 8") for (int q = 0; q < w; q++)
    {
        s[q] = _mm_add_pd(s[q], s[q + w]);
        mn[q] = _mm_min_pd(mn[q + w], mn[q]);
        mx[q] = _mm_max_pd(mx[q + w], mx[q]);
    }
    double sum = _mm_cvtsd_f64(_mm_add_sd(s[0], _mm_unpackhi_pd(s[0], s[0])));
    double lo = _mm_cvtsd_f64(_mm_min_sd(_mm_unpackhi_pd(mn[0], mn[0]), mn[0]));
    double hi = _mm_cvtsd_f64(_mm_max_sd(_mm_unpackhi_pd(mx[0], mx[0]), mx[0]));
    a->min = lo < a->min ? lo : a->min;
    a->max = hi > a->max ? hi : a->max;
    if (cnt)
        nulls = (size_t)(_mm_cvtsi128_si64(nc) + _mm_extract_epi64(nc, 1));
    uint64_t c = n - nulls;
    if (!dv) {
        sum_finish(c, sum, a);
        return;
    }
    if (!c)
        return;
    const __m128d mm = _mm_set1_pd(sum / (double)c);
    __m128d d1[8], d2[8];
    _Pragma("GCC unroll 8") for (int q = 0; q < P; q++) d1[q] = d2[q] = _mm_setzero_pd();
    i = 0;
    if (P == 8)
        for (; i + L64 <= n; i += L64)
            _Pragma("GCC unroll 8") for (int q = 0; q < 8; q++) K64_DEV(v + i + 2 * q, false, q);
    _Pragma("GCC unroll 8") for (int q = 0; q < P; q++)
    {
        size_t j = i + (size_t)(2 * q);
        if (j + 2 <= n)
            K64_DEV(v + j, false, q);
        else if (j < n)
            K64_DEV(v + j, true, q);
    }
    _Pragma("GCC unroll 8") for (int w = P / 2; w >= 1; w /= 2)
        _Pragma("GCC unroll 8") for (int q = 0; q < w; q++)
    {
        d1[q] = _mm_add_pd(d1[q], d1[q + w]);
        d2[q] = _mm_add_pd(d2[q], d2[q + w]);
    }
    double r1 = _mm_cvtsd_f64(_mm_add_sd(d1[0], _mm_unpackhi_pd(d1[0], d1[0])));
    double r2 = _mm_cvtsd_f64(_mm_add_sd(d2[0], _mm_unpackhi_pd(d2[0], d2[0])));
    agg_add_run(a, dv, c, sum, r1, r2);
    a->sum = canon_nan(a->sum);
}

/* values.c's short runs: pairs q and q + H (H = P / 2) built side by side, each over its values j, j + 16, ...,
 * and added at once (the first halving), so that 3H vectors stay live rather than 3P: SSE has 16 registers,
 * and the 24 accumulators of f64_core spill. The same lanes and order. */
/* the pair at v[j] (chk: it may hold one value only, as the last of the run) into sv, nv_, xv; and the pairs of
 * lanes q: v[2q] (f0: known to be a whole pair) and, with more, the rows after it */
#define K64_X(j, sv, nv_, xv, chk)                                                                                  \
    do {                                                                                                           \
        bool p_ = (chk) && (j) + 2 > n;                                                                            \
        __m128d x_ = p_ ? _mm_load_sd(v + (j)) : _mm_loadu_pd(v + (j)), xm_ = x_, xs_ = x_;                       \
        if (p_)                                                                                                    \
            xm_ = _mm_or_pd(x_, _mm_castsi128_pd(_mm_set_epi64x(-1, 0)));                                          \
        if (nl) {                                                                                                  \
            __m128i e_ = _mm_cmpeq_epi64(_mm_castpd_si128(x_), nul);                                               \
            xs_ = _mm_andnot_pd(_mm_castsi128_pd(e_), x_);                                                         \
            nc = _mm_sub_epi64(nc, e_);                                                                            \
        }                                                                                                          \
        sv = _mm_add_pd(sv, xs_);                                                                                  \
        nv_ = _mm_min_pd(xm_, nv_);                                                                                \
        xv = _mm_max_pd(xm_, xv);                                                                                  \
    } while (0)

#define K64_PAIR(q, sv, nv_, xv, f0, more)                                                                          \
    do {                                                                                                           \
        size_t j_ = (size_t)(2 * (q));                                                                             \
        if (f0)                                                                                                    \
            K64_X(j_, sv, nv_, xv, false);                                                                         \
        else if (j_ < n)                                                                                           \
            K64_X(j_, sv, nv_, xv, true);                                                                          \
        if (more)                                                                                                  \
            for (j_ += L64; j_ < n; j_ += L64)                                                                     \
                K64_X(j_, sv, nv_, xv, true);                                                                      \
    } while (0)

#define K64_XDEV(j, d1v, d2v, chk)                                                                                  \
    do {                                                                                                           \
        bool p_ = (chk) && (j) + 2 > n;                                                                            \
        __m128d x_ = p_ ? _mm_load_sd(v + (j)) : _mm_loadu_pd(v + (j)), d_ = _mm_sub_pd(x_, mm);                  \
        if (nl)                                                                                                    \
            d_ = _mm_andnot_pd(_mm_castsi128_pd(_mm_cmpeq_epi64(_mm_castpd_si128(x_), nul)), d_);                  \
        if (p_)                                                                                                    \
            d_ = _mm_move_sd(_mm_setzero_pd(), d_);                                                                \
        d1v = _mm_add_pd(d1v, d_);                                                                                 \
        d2v = _mm_add_pd(d2v, _mm_mul_pd(d_, d_));                                                                 \
    } while (0)

#define K64_PAIR_DEV(q, d1v, d2v, f0, more)                                                                         \
    do {                                                                                                           \
        size_t j_ = (size_t)(2 * (q));                                                                             \
        if (f0)                                                                                                    \
            K64_XDEV(j_, d1v, d2v, false);                                                                         \
        else if (j_ < n)                                                                                           \
            K64_XDEV(j_, d1v, d2v, true);                                                                          \
        if (more)                                                                                                  \
            for (j_ += L64; j_ < n; j_ += L64)                                                                     \
                K64_XDEV(j_, d1v, d2v, true);                                                                      \
    } while (0)

/* P pairs of lanes in use: 1 (n <= 2), 2 (n <= 4), 4 (n <= 8) or 8; M: more than one row (n > 16). The pairs
 * of the first half hold whole pairs from P = 2 on, and with M those of the second half too. */
KI void KN(f64_short)(const double *v, size_t n, agg_state *a, agg_dev *dv, const bool nl, const int P, const bool M)
{
    size_t nulls = 0;
    const int H = P > 1 ? P / 2 : 1;
    const __m128i nul = _mm_set1_epi64x((long long)NULL_F64);
    const __m128d lo0 = _mm_set1_pd(a->min), hi0 = _mm_set1_pd(a->max);
    __m128d s[4], mn[4], mx[4];
    __m128i nc = _mm_setzero_si128();
    _Pragma("GCC unroll 4") for (int q = 0; q < H; q++)
    {
        __m128d sa = _mm_setzero_pd(), na = lo0, xa = hi0;
        K64_PAIR(q, sa, na, xa, P > 1, M);
        if (P > 1) {
            __m128d sb = _mm_setzero_pd(), nb = lo0, xb = hi0;
            K64_PAIR(q + H, sb, nb, xb, M, M);
            sa = _mm_add_pd(sa, sb);
            na = _mm_min_pd(nb, na);
            xa = _mm_max_pd(xb, xa);
        }
        s[q] = sa;
        mn[q] = na;
        mx[q] = xa;
    }
    _Pragma("GCC unroll 4") for (int w = H / 2; w >= 1; w /= 2)
        _Pragma("GCC unroll 4") for (int q = 0; q < w; q++)
    {
        s[q] = _mm_add_pd(s[q], s[q + w]);
        mn[q] = _mm_min_pd(mn[q + w], mn[q]);
        mx[q] = _mm_max_pd(mx[q + w], mx[q]);
    }
    double sum = _mm_cvtsd_f64(_mm_add_sd(s[0], _mm_unpackhi_pd(s[0], s[0])));
    double lo = _mm_cvtsd_f64(_mm_min_sd(_mm_unpackhi_pd(mn[0], mn[0]), mn[0]));
    double hi = _mm_cvtsd_f64(_mm_max_sd(_mm_unpackhi_pd(mx[0], mx[0]), mx[0]));
    a->min = lo < a->min ? lo : a->min;
    a->max = hi > a->max ? hi : a->max;
    if (nl)
        nulls = (size_t)(_mm_cvtsi128_si64(nc) + _mm_extract_epi64(nc, 1));
    uint64_t c = n - nulls;
    if (!dv) {
        sum_finish(c, sum, a);
        return;
    }
    if (!c)
        return;
    const __m128d mm = _mm_set1_pd(sum / (double)c);
    __m128d d1[4], d2[4];
    _Pragma("GCC unroll 4") for (int q = 0; q < H; q++)
    {
        __m128d a1 = _mm_setzero_pd(), a2 = _mm_setzero_pd();
        K64_PAIR_DEV(q, a1, a2, P > 1, M);
        if (P > 1) {
            __m128d b1 = _mm_setzero_pd(), b2 = _mm_setzero_pd();
            K64_PAIR_DEV(q + H, b1, b2, M, M);
            a1 = _mm_add_pd(a1, b1);
            a2 = _mm_add_pd(a2, b2);
        }
        d1[q] = a1;
        d2[q] = a2;
    }
    _Pragma("GCC unroll 4") for (int w = H / 2; w >= 1; w /= 2)
        _Pragma("GCC unroll 4") for (int q = 0; q < w; q++)
    {
        d1[q] = _mm_add_pd(d1[q], d1[q + w]);
        d2[q] = _mm_add_pd(d2[q], d2[q + w]);
    }
    double r1 = _mm_cvtsd_f64(_mm_add_sd(d1[0], _mm_unpackhi_pd(d1[0], d1[0])));
    double r2 = _mm_cvtsd_f64(_mm_add_sd(d2[0], _mm_unpackhi_pd(d2[0], d2[0])));
    agg_add_run(a, dv, c, sum, r1, r2);
    a->sum = canon_nan(a->sum);
}

/* the copies by the lanes a run of n values fills (a short run: fewer than the 32 or 16 lanes) */
#define KBY_WIDTH(core, n, per, ...)                                                                               \
    do {                                                                                                           \
        if ((n) <= (per))                                                                                          \
            core(__VA_ARGS__, 1);                                                                                  \
        else if ((n) <= 2 * (per))                                                                                 \
            core(__VA_ARGS__, 2);                                                                                  \
        else if ((n) <= 4 * (per))                                                                                 \
            core(__VA_ARGS__, 4);                                                                                  \
        else                                                                                                       \
            core(__VA_ARGS__, 8);                                                                                  \
    } while (0)

KF void KN(agg128)(const float *v, size_t n, agg_state *a, agg_dev *dv)
{
    KN(f32_core)(v, n, 0, a, dv, false, false, 8);
}

KF void KN(fold_f32_128)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        KN(f32_core)(v, n, nulls, a, dv, true, false, 8);
    else
        KN(f32_core)(v, n, 0, a, dv, false, false, 8);
}

KF void KN(fold_f64_128)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *dv)
{
    if (nulls)
        KN(f64_core)(v, n, nulls, a, dv, true, false, 8);
    else
        KN(f64_core)(v, n, 0, a, dv, false, false, 8);
}

KF void KN(small_f32)(const float *v, size_t n, bool may_be_null, agg_state *a, agg_dev *dv)
{
    if (may_be_null)
        KBY_WIDTH(KN(f32_core), n, 4, v, n, 0, a, dv, true, true);
    else
        KBY_WIDTH(KN(f32_core), n, 4, v, n, 0, a, dv, false, false);
}

#define K64_SHORT(nl)                                                                                               \
    do {                                                                                                           \
        if (n <= 2)                                                                                                \
            KN(f64_short)(v, n, a, dv, nl, 1, false);                                                              \
        else if (n <= 4)                                                                                           \
            KN(f64_short)(v, n, a, dv, nl, 2, false);                                                              \
        else if (n <= 8)                                                                                           \
            KN(f64_short)(v, n, a, dv, nl, 4, false);                                                              \
        else if (n <= L64)                                                                                         \
            KN(f64_short)(v, n, a, dv, nl, 8, false);                                                              \
        else                                                                                                       \
            KN(f64_short)(v, n, a, dv, nl, 8, true);                                                               \
    } while (0)

KF void KN(small_f64)(const double *v, size_t n, bool may_be_null, agg_state *a, agg_dev *dv)
{
    if (!n)
        return;
    if (may_be_null)
        K64_SHORT(true);
    else
        K64_SHORT(false);
}

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

#undef K32_SUM
#undef K32_DEV
#undef K64_SUM
#undef K64_DEV
#undef K64_PAIR
#undef K64_X
#undef K64_XDEV
#undef K64_SHORT
#undef K64_PAIR_DEV
#undef KBY_WIDTH
#undef KF
#undef KI
#undef KN
#undef KCAT
#undef KCAT_
#undef KSUF
#undef KTARGET
