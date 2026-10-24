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
 */
#define LANES 32

typedef struct lanes {
    double s[LANES];
    float mn[LANES], mx[LANES];
} lanes;

static void lanes_init(lanes *l, const agg_state *a)
{
    for (int j = 0; j < LANES; j++) {
        l->s[j] = 0;
        l->mn[j] = (float)a->min; /* the state holds float32 values here */
        l->mx[j] = (float)a->max;
    }
}

/* values [i, n) with n - i < LANES, then the reduction into a */
static void lanes_finish(lanes *l, const float *v, size_t i, size_t n, agg_state *a)
{
    for (int j = 0; i + (size_t)j < n; j++) {
        float x = v[i + (size_t)j];
        l->s[j] += x;
        l->mn[j] = x < l->mn[j] ? x : l->mn[j];
        l->mx[j] = x > l->mx[j] ? x : l->mx[j];
    }
    for (int w = LANES / 2; w >= 1; w /= 2)
        for (int j = 0; j < w; j++) {
            l->s[j] = l->s[j] + l->s[j + w];
            l->mn[j] = l->mn[j + w] < l->mn[j] ? l->mn[j + w] : l->mn[j];
            l->mx[j] = l->mx[j + w] > l->mx[j] ? l->mx[j + w] : l->mx[j];
        }
    a->count += n;
    a->sum = canon_nan(a->sum + l->s[0]);
    a->min = l->mn[0] < a->min ? l->mn[0] : a->min;
    a->max = l->mx[0] > a->max ? l->mx[0] : a->max;
}

static void agg_scalar(const float *v, size_t n, agg_state *a)
{
    lanes l;
    lanes_init(&l, a);
    size_t i = 0;
    for (; i + LANES <= n; i += LANES)
        for (int j = 0; j < LANES; j++) {
            float x = v[i + (size_t)j];
            l.s[j] += x;
            l.mn[j] = x < l.mn[j] ? x : l.mn[j];
            l.mx[j] = x > l.mx[j] ? x : l.mx[j];
        }
    lanes_finish(&l, v, i, n, a);
}

#ifdef TSDB_X86
/* sum of the 4 floats as doubles into s */
#define ADD4(s, q) s = _mm256_add_pd(s, _mm256_cvtps_pd(q))

__attribute__((target("avx2"))) static void agg_avx2(const float *v, size_t n, agg_state *a)
{
    lanes l;
    lanes_init(&l, a);
    /* sum lanes 0-3, 4-7, ..., 28-31 */
    __m256d s0 = _mm256_setzero_pd(), s1 = s0, s2 = s0, s3 = s0, s4 = s0, s5 = s0, s6 = s0, s7 = s0;
    /* min/max lanes 0-7, 8-15, 16-23, 24-31 */
    __m256 mn0 = _mm256_loadu_ps(l.mn), mn1 = mn0, mn2 = mn0, mn3 = mn0;
    __m256 mx0 = _mm256_loadu_ps(l.mx), mx1 = mx0, mx2 = mx0, mx3 = mx0;
    size_t i = 0;
    /* one quarter at a time, so that no loaded vector stays live: 16 accumulators fill the registers */
#define QUARTER(q, sa, sb, mnq, mxq)                                      \
    do {                                                                  \
        __m256 x = _mm256_loadu_ps(v + i + 8 * (q));                      \
        ADD4(sa, _mm256_castps256_ps128(x));                              \
        ADD4(sb, _mm256_extractf128_ps(x, 1));                            \
        mnq = _mm256_min_ps(x, mnq);                                      \
        mxq = _mm256_max_ps(x, mxq);                                      \
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
    lanes_finish(&l, v, i, n, a);
}

__attribute__((target("avx512f,avx512dq"))) static void agg_avx512(const float *v, size_t n, agg_state *a)
{
    lanes l;
    lanes_init(&l, a);
    /* sum lanes 0-7, 8-15, 16-23, 24-31 */
    __m512d s0 = _mm512_setzero_pd(), s1 = s0, s2 = s0, s3 = s0;
    __m512 mn = _mm512_loadu_ps(l.mn), mx = _mm512_loadu_ps(l.mx); /* min/max lanes 0-15 */
    __m512 mn2 = mn, mx2 = mx;                                      /* min/max lanes 16-31 */
    size_t i = 0;
    for (; i + LANES <= n; i += LANES) {
        __m512 x = _mm512_loadu_ps(v + i), y = _mm512_loadu_ps(v + i + 16);
        s0 = _mm512_add_pd(s0, _mm512_cvtps_pd(_mm512_castps512_ps256(x)));
        s1 = _mm512_add_pd(s1, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(x), 1))));
        s2 = _mm512_add_pd(s2, _mm512_cvtps_pd(_mm512_castps512_ps256(y)));
        s3 = _mm512_add_pd(s3, _mm512_cvtps_pd(_mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(y), 1))));
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
    lanes_finish(&l, v, i, n, a);
}
#endif

void (*simd_agg_f32)(const float *v, size_t n, agg_state *a) = agg_scalar;
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
    void (*f)(const float *, size_t, agg_state *) = agg_scalar;
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
        name = "avx512";
    } else if (avx2) {
        f = agg_avx2;
        name = "avx2";
    }
#endif
    simd_agg_f32 = f;
    simd_level = name;
#ifdef TSDB_LEVELS
    snprintf(simd_desc, sizeof simd_desc, "%s, codecs x86-64-%s", name, cpu_level_name(level));
#else
    snprintf(simd_desc, sizeof simd_desc, "%s", name);
#endif
}

static void simd_pick(void) { simd_select(cpu_level()); }

void simd_init(void) { pthread_once(&simd_once, simd_pick); }
