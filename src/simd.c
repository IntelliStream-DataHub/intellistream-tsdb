/*
 * Aggregation kernels with runtime dispatch: AVX-512, AVX2 or scalar.
 *
 * Gorilla and DoubleDelta are bit-serial (each value's bit position depends on
 * the previous one), so they are decoded with scalar code. What vectorizes is
 * everything after decoding: count/sum/min/max over the values.
 *
 * min/max follow MINPS/MAXPS semantics with the accumulator as the second
 * operand, so NaN inputs are ignored; NaN still propagates into the sum.
 */
#include "internal.h"

#include <stdlib.h>

#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#include <immintrin.h>
#define TSDB_X86 1
#endif

static void agg_scalar(const float *v, size_t n, agg_state *a)
{
    double s0 = 0, s1 = 0, s2 = 0, s3 = 0;
    float mn = (float)a->min, mx = (float)a->max; /* the state holds float32 values here */
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        s0 += v[i];
        s1 += v[i + 1];
        s2 += v[i + 2];
        s3 += v[i + 3];
        for (int k = 0; k < 4; k++) {
            mn = v[i + k] < mn ? v[i + k] : mn;
            mx = v[i + k] > mx ? v[i + k] : mx;
        }
    }
    for (; i < n; i++) {
        s0 += v[i];
        mn = v[i] < mn ? v[i] : mn;
        mx = v[i] > mx ? v[i] : mx;
    }
    a->count += n;
    a->sum += (s0 + s1) + (s2 + s3);
    a->min = mn;
    a->max = mx;
}

#ifdef TSDB_X86
__attribute__((target("avx2"))) static void agg_avx2(const float *v, size_t n, agg_state *a)
{
    __m256d s0 = _mm256_setzero_pd(), s1 = _mm256_setzero_pd();
    __m256d s2 = _mm256_setzero_pd(), s3 = _mm256_setzero_pd();
    __m256 mn = _mm256_set1_ps((float)a->min), mx = _mm256_set1_ps((float)a->max);
    __m256 mn2 = mn, mx2 = mx;
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        __m256 x = _mm256_loadu_ps(v + i);
        __m256 y = _mm256_loadu_ps(v + i + 8);
        s0 = _mm256_add_pd(s0, _mm256_cvtps_pd(_mm256_castps256_ps128(x)));
        s1 = _mm256_add_pd(s1, _mm256_cvtps_pd(_mm256_extractf128_ps(x, 1)));
        s2 = _mm256_add_pd(s2, _mm256_cvtps_pd(_mm256_castps256_ps128(y)));
        s3 = _mm256_add_pd(s3, _mm256_cvtps_pd(_mm256_extractf128_ps(y, 1)));
        mn = _mm256_min_ps(x, mn);
        mx = _mm256_max_ps(x, mx);
        mn2 = _mm256_min_ps(y, mn2);
        mx2 = _mm256_max_ps(y, mx2);
    }
    s0 = _mm256_add_pd(_mm256_add_pd(s0, s1), _mm256_add_pd(s2, s3));
    mn = _mm256_min_ps(mn2, mn);
    mx = _mm256_max_ps(mx2, mx);
    double sums[4];
    float mins[8], maxs[8];
    _mm256_storeu_pd(sums, s0);
    _mm256_storeu_ps(mins, mn);
    _mm256_storeu_ps(maxs, mx);
    agg_state t;
    agg_init(&t);
    t.sum = (sums[0] + sums[1]) + (sums[2] + sums[3]);
    t.min = a->min;
    t.max = a->max;
    for (int k = 0; k < 8; k++) {
        t.min = mins[k] < t.min ? mins[k] : t.min;
        t.max = maxs[k] > t.max ? maxs[k] : t.max;
    }
    agg_scalar(v + i, n - i, &t);
    a->count += n;
    a->sum += t.sum;
    a->min = t.min;
    a->max = t.max;
}

__attribute__((target("avx512f,avx512dq"))) static void agg_avx512(const float *v, size_t n, agg_state *a)
{
    __m512d s0 = _mm512_setzero_pd(), s1 = _mm512_setzero_pd();
    __m512d s2 = _mm512_setzero_pd(), s3 = _mm512_setzero_pd();
    __m512 mn = _mm512_set1_ps((float)a->min), mx = _mm512_set1_ps((float)a->max);
    __m512 mn2 = mn, mx2 = mx;
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m512 x = _mm512_loadu_ps(v + i);
        __m512 y = _mm512_loadu_ps(v + i + 16);
        s0 = _mm512_add_pd(s0, _mm512_cvtps_pd(_mm512_castps512_ps256(x)));
        s1 = _mm512_add_pd(s1, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(x), 1))));
        s2 = _mm512_add_pd(s2, _mm512_cvtps_pd(_mm512_castps512_ps256(y)));
        s3 = _mm512_add_pd(s3, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(y), 1))));
        mn = _mm512_min_ps(x, mn);
        mx = _mm512_max_ps(x, mx);
        mn2 = _mm512_min_ps(y, mn2);
        mx2 = _mm512_max_ps(y, mx2);
    }
    /* masked tail: inactive lanes keep the accumulator */
    if (i < n) {
        __mmask16 m = (__mmask16)((1u << MIN(n - i, 16)) - 1);
        __m512 x = _mm512_maskz_loadu_ps(m, v + i);
        s0 = _mm512_add_pd(s0, _mm512_cvtps_pd(_mm512_castps512_ps256(x)));
        s1 = _mm512_add_pd(s1, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(x), 1))));
        mn = _mm512_mask_min_ps(mn, m, x, mn);
        mx = _mm512_mask_max_ps(mx, m, x, mx);
        if (n - i > 16) {
            __mmask16 m2 = (__mmask16)((1u << (n - i - 16)) - 1);
            __m512 y = _mm512_maskz_loadu_ps(m2, v + i + 16);
            s2 = _mm512_add_pd(s2, _mm512_cvtps_pd(_mm512_castps512_ps256(y)));
            s3 = _mm512_add_pd(s3, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(y), 1))));
            mn2 = _mm512_mask_min_ps(mn2, m2, y, mn2);
            mx2 = _mm512_mask_max_ps(mx2, m2, y, mx2);
        }
    }
    double sum = _mm512_reduce_add_pd(_mm512_add_pd(_mm512_add_pd(s0, s1), _mm512_add_pd(s2, s3)));
    float lo = _mm512_reduce_min_ps(_mm512_min_ps(mn2, mn));
    float hi = _mm512_reduce_max_ps(_mm512_max_ps(mx2, mx));
    a->count += n;
    a->sum += sum;
    a->min = lo < a->min ? lo : a->min;
    a->max = hi > a->max ? hi : a->max;
}
#endif

void (*simd_agg_f32)(const float *v, size_t n, agg_state *a) = agg_scalar;
static const char *simd_level = "scalar";

const char *simd_name(void) { return simd_level; }
const char *tsdb_simd_level(void) { return simd_level; }

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

static void simd_pick(void)
{
#ifdef TSDB_X86
    const char *force = getenv("TSDB_SIMD"); /* "scalar", "avx2" or "avx512", for benchmarking */
    __builtin_cpu_init();
    bool avx512 = __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512dq");
    bool avx2 = __builtin_cpu_supports("avx2");
    if (avx512 && avx512_downclocks() && !(force && strcmp(force, "avx512") == 0))
        avx512 = false;
    if (force && strcmp(force, "scalar") == 0)
        avx512 = avx2 = false;
    else if (force && strcmp(force, "avx2") == 0)
        avx512 = false;
    if (avx512) {
        simd_agg_f32 = agg_avx512;
        simd_level = "avx512";
    } else if (avx2) {
        simd_agg_f32 = agg_avx2;
        simd_level = "avx2";
    }
#endif
}

void simd_init(void) { pthread_once(&simd_once, simd_pick); }
