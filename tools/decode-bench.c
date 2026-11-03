/*
 * Decode speed of the bit-unpacking kernels and of the codecs built on them, for comparing CPUs.
 *
 *   decode-bench [--rounds 7] [--ms 100]
 *
 * unpack: one vector of 1024 values at each width 1..25 (what int_bp, decimal_bp, delta_bp, ALP and the float64
 * decimal codec spend their time in), in millions of values per second, mean and sd of the rounds, the variants
 * interleaved in every round:
 *   loops   bitpack.h's portable loops (unpack_fn): one 8-byte load, shift and mask per value
 *   kernel  unpack(): what this build uses (the AVX2 shuffle kernel where bitpack.h compiles it in)
 *   avx2    the shuffle kernel itself, where compiled in (v3 build, or -DTSDB_UNPACK_AVX2)
 *   pdep    BMI2 PDEP, widths up to 16 (the alternative tried and not used; fast on Intel since Haswell and on AMD
 *           since Zen 3, microcoded before), where this build has BMI2
 * codecs: int_bp decode and the float64 decimal encode and decode of one granule of 8192 values, at the CPU level
 * in use (TSDB_CPU=v2|v3|v4 in a build with CPU_LEVELS).
 * The unpack variants are compiled into this tool with the flags of the build (-march=native by default): build
 * with the CPU's own flags, and with -DTSDB_UNPACK_AVX2 to see the shuffle kernel on an AVX-512 CPU.
 */
#include "../src/internal.h"
#include "common.h"

#include "../src/bitpack.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#define NV 1024
#define MAXR 64
#define NVAR 4
#define GRN 8192

#if defined(__BMI2__) && defined(__AVX2__)
#define HAVE_PDEP 1
#include <immintrin.h>
/* PDEP prototypes; n is a multiple of 8. w <= 8: one pdep spreads 8 values into bytes. w 9..16: two spread 4 into 16
 * bit lanes each (the second from byte w / 2, shifted by the 4 bits an odd w leaves). */
static void unpack_pdep(const uint8_t *in, size_t n, int w, uint32_t *out)
{
    if (w <= 8) {
        const uint64_t m = ((1ull << w) - 1) * 0x0101010101010101ull;
        for (size_t i = 0; i < n; i += 8, in += w) {
            uint64_t x;
            memcpy(&x, in, 8);
            _mm256_storeu_si256((__m256i *)(void *)(out + i),
                                _mm256_cvtepu8_epi32(_mm_cvtsi64_si128((long long)_pdep_u64(x, m))));
        }
    } else {
        const uint64_t m = ((1ull << w) - 1) * 0x0001000100010001ull;
        const unsigned sh = (unsigned)(w & 1) * 4;
        for (size_t i = 0; i < n; i += 8, in += w) {
            uint64_t x0, x1;
            memcpy(&x0, in, 8);
            memcpy(&x1, in + (w >> 1), 8);
            x1 >>= sh;
            _mm256_storeu_si256((__m256i *)(void *)(out + i),
                                _mm256_cvtepu16_epi32(_mm_set_epi64x((long long)_pdep_u64(x1, m),
                                                                     (long long)_pdep_u64(x0, m))));
        }
    }
}
#endif

static void mean_sd(const double *r, int n, double *m, double *sd)
{
    double s = 0, q = 0;
    for (int i = 0; i < n; i++)
        s += r[i];
    *m = s / n;
    for (int i = 0; i < n; i++)
        q += (r[i] - *m) * (r[i] - *m);
    *sd = n > 1 ? sqrt(q / (n - 1)) : 0;
}

static uint32_t sink;
#define TIME_MS(ms, vals, body)                                                  \
    ({                                                                           \
        double t0_ = now_s(), t_;                                                \
        size_t it_ = 0;                                                          \
        do {                                                                     \
            for (int k_ = 0; k_ < 16; k_++) {                                    \
                body;                                                            \
            }                                                                    \
            it_ += 16;                                                           \
        } while ((t_ = now_s() - t0_) * 1e3 < (ms));                             \
        (double)it_ * (double)(vals) / t_ / 1e6;                                 \
    })

int main(int argc, char **argv)
{
    int rounds = 7, ms = 100;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--rounds") && i + 1 < argc)
            rounds = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--ms") && i + 1 < argc)
            ms = atoi(argv[++i]);
        else {
            fprintf(stderr, "usage: decode-bench [--rounds N<=%d] [--ms MS]\n", MAXR);
            return 2;
        }
    }
    if (rounds < 1 || rounds > MAXR || ms < 1)
        return 2;
    simd_init();
    printf("# kernels: %s\n", tsdb_simd_level());
#ifdef BITPACK_AVX2
    printf("# unpack(): the AVX2 shuffle kernel\n");
#else
    printf("# unpack(): the portable loops (no AVX2 kernel in this build)\n");
#endif

    uint32_t v[NV], o[NV + 8], ref[NV];
    uint8_t pk[NV * 4 + 64];
    uint64_t s = 88172645463325252ull;
    printf("\nunpack, M values/s, mean (sd) of %d rounds of %d ms, vector of %d\n", rounds, ms, NV);
    printf("%3s %16s %16s %16s %16s\n", "w", "loops", "kernel", "avx2", "pdep");
    for (int w = 1; w <= 25; w++) {
        uint32_t mask = (1u << w) - 1;
        for (int i = 0; i < NV; i++) {
            s ^= s << 13, s ^= s >> 7, s ^= s << 17;
            v[i] = (uint32_t)s & mask;
        }
        memset(pk, 0, sizeof pk);
        pack(v, NV, w, pk);
        memcpy(ref, v, sizeof ref);
        double r[NVAR][MAXR] = {{0}};
        int have[NVAR] = {1, 1, 0, 0};
#ifdef BITPACK_AVX2
        have[2] = 1;
#endif
#ifdef HAVE_PDEP
        have[3] = w <= 16;
#endif
        for (int rd = 0; rd < rounds; rd++) {
            r[0][rd] = TIME_MS(ms, NV, { unpack_fn[w](pk, NV, o); sink += o[7]; __asm__ volatile("" ::: "memory"); });
            r[1][rd] = TIME_MS(ms, NV, { unpack(pk, NV, w, o); sink += o[7]; __asm__ volatile("" ::: "memory"); });
#ifdef BITPACK_AVX2
            r[2][rd] = TIME_MS(ms, NV, { unpack_avx2(pk, NV, w, o); sink += o[7]; __asm__ volatile("" ::: "memory"); });
#endif
#ifdef HAVE_PDEP
            if (w <= 16)
                r[3][rd] = TIME_MS(ms, NV, { unpack_pdep(pk, NV, w, o); sink += o[7]; __asm__ volatile("" ::: "memory"); });
#endif
        }
        /* every variant must give the values back */
        memset(o, 0, sizeof o);
        unpack_fn[w](pk, NV, o);
        int ok = !memcmp(o, ref, sizeof ref);
        unpack(pk, NV, w, o);
        ok &= !memcmp(o, ref, sizeof ref);
#ifdef BITPACK_AVX2
        unpack_avx2(pk, NV, w, o);
        ok &= !memcmp(o, ref, sizeof ref);
#endif
#ifdef HAVE_PDEP
        if (w <= 16) {
            unpack_pdep(pk, NV, w, o);
            ok &= !memcmp(o, ref, sizeof ref);
        }
#endif
        printf("%3d", w);
        for (int k = 0; k < NVAR; k++) {
            double m, sd;
            if (have[k]) {
                mean_sd(r[k], rounds, &m, &sd);
                printf(" %9.0f (%4.0f)", m, sd);
            } else {
                printf(" %16s", "-");
            }
        }
        printf("%s\n", ok ? "" : "   WRONG VALUES");
        if (!ok)
            return 1;
        fflush(stdout);
    }

    /* codecs, one granule */
    static int64_t k[GRN], kd[GRN + 8];
    static double d[GRN], dd[GRN + 8];
    static uint8_t ib[GRN * 16 + 4096], blk[GRN * 16 + 4096];
    int64_t x = 2000;
    for (int i = 0; i < GRN; i++) {
        s ^= s << 13, s ^= s >> 7, s ^= s << 17;
        x += (int64_t)(s % 7) - 3;
        k[i] = x;
        d[i] = (double)x / 100.0; /* two decimals */
    }
    size_t il = int_bp_encode(k, GRN, ib), dl = f64_decimal_encode(d, GRN, blk);
    if (!il || !dl || int_bp_decode(ib, il, GRN, kd) || memcmp(k, kd, sizeof k) ||
        f64_decimal_decode(blk, dl, GRN, dd) || memcmp(d, dd, sizeof d)) {
        fprintf(stderr, "decode-bench: the codecs do not round-trip\n");
        return 1;
    }
    double r0[MAXR], r1[MAXR], r2[MAXR];
    for (int rd = 0; rd < rounds; rd++) {
        r0[rd] = TIME_MS(ms, GRN, { int_bp_decode(ib, il, GRN, kd); __asm__ volatile("" ::: "memory"); });
        r1[rd] = TIME_MS(ms, GRN, { f64_decimal_decode(blk, dl, GRN, dd); __asm__ volatile("" ::: "memory"); });
        r2[rd] = TIME_MS(ms, GRN, { f64_decimal_encode(d, GRN, blk); __asm__ volatile("" ::: "memory"); });
    }
    printf("\ncodecs, M values/s, mean (sd) of %d rounds of %d ms, granule of %d\n", rounds, ms, GRN);
    const char *names[3] = {"int_bp decode", "float64 decimal decode", "float64 decimal encode"};
    double *rr[3] = {r0, r1, r2};
    for (int c = 0; c < 3; c++) {
        double m, sd;
        mean_sd(rr[c], rounds, &m, &sd);
        printf("%-24s %9.0f (%4.0f)\n", names[c], m, sd);
    }
    return (int)(sink == 0xdeadbeef); /* keeps sink live */
}
