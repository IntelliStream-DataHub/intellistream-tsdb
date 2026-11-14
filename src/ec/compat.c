/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The functions behind src/ec/compat.h: the CPU features the kernels may use, capped by the level src/cpu.c selected,
 * and the few allocation, string, clock and abort helpers the copied codec calls. See compat.h for why none of this
 * is isod's code, and AGENTS.md "CPU levels" for why nothing here may run before main.
 */
#include "compat.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <cpuid.h>

#include "../internal.h"
#include "gf256.h"

#if !(defined(__x86_64__) || defined(__i386__))
#error "src/ec is the x86-64 codec: the Makefile builds it on x86-64 only"
#endif

static int ec_level = CPU_BASE;

bool tsdb_ec_cpu_has(iso_cpu_feature f)
{
    __builtin_cpu_init();
    switch (f) {
    case ISO_CPU_SSSE3:
        return __builtin_cpu_supports("ssse3");
    case ISO_CPU_AVX2:
        return ec_level >= CPU_V3 && __builtin_cpu_supports("avx2");
    case ISO_CPU_AVX512F:
        return ec_level >= CPU_V4 && __builtin_cpu_supports("avx512f");
    case ISO_CPU_AVX512BW:
        return ec_level >= CPU_V4 && __builtin_cpu_supports("avx512bw");
    case ISO_CPU_GFNI:
        /* the two GFNI kernels also ask for AVX2 or AVX-512BW, which are capped above */
        return ec_level >= CPU_V3 && __builtin_cpu_supports("gfni");
    default:
        return false;
    }
}

/* Skylake-SP, Cascade Lake and Cooper Lake (Intel family 6, model 0x55) slow the whole socket down on sustained
 * 512-bit work; isod leaves the 512-bit kernels off the ladder there, and so do we. Everything else, AMD included,
 * uses them. isod's ISO_CPU_PREFER_512 is not read: TSDB_CPU is the knob here. */
bool tsdb_ec_prefer_512(void)
{
    unsigned eax, ebx, ecx, edx;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        return true;
    unsigned family = (eax >> 8) & 0xf, model = (eax >> 4) & 0xf;
    if (family == 0xf)
        family += (eax >> 20) & 0xff;
    if (family == 6 || family == 0xf)
        model += ((eax >> 16) & 0xf) << 4;
    return !(__builtin_cpu_is("intel") && family == 6 && model == 0x55);
}

void tsdb_ec_select(int level)
{
    ec_level = level;
    iso_gf256_init();
}

void *tsdb_ec_xmalloc(size_t n)
{
    void *p = malloc(n ? n : 1);
    if (!p)
        ISO_PANIC("out of memory (%zu bytes)", n);
    return p;
}

size_t tsdb_ec_strlcpy(char *dst, const char *src, size_t dstsize)
{
    size_t n = strlen(src);
    if (dstsize) {
        size_t c = n < dstsize - 1 ? n : dstsize - 1;
        memcpy(dst, src, c);
        dst[c] = 0;
    }
    return n;
}

uint64_t tsdb_ec_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

void tsdb_ec_panic(const char *file, int line, const char *fmt, ...)
{
    va_list ap;
    fprintf(stderr, "tsdb: ec: panic at %s:%d: ", file, line);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
    abort();
}
