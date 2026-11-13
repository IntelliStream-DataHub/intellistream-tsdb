/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * What the erasure codec copied from intellistream-objects (src/ec/UPSTREAM) includes from isod's own src/common,
 * written for tsdb. The copied files include "common/util.h", "common/log.h", "common/clock.h", "common/cpu.h" and
 * "common/kcheck.h", and each other as "codec/gf256.h" and "codec/rs.h"; every one of those is a one-line header
 * under compat/ that points here or at the copy, so the copies stay byte for byte as upstream has them
 * (tools/sync-ec.sh) and are built with -Isrc/ec/compat -DISO_GF256_NO_BOOT.
 *
 * Only what the six files use is here, with isod's meaning, except where tsdb's rules differ:
 *
 *  - Names. src/raft/compat.c defines iso_xmalloc, iso_strlcpy and iso_now_ns for the Raft copy, and both live in
 *    tsdbd, so the functions of this file are named tsdb_ec_* and the copies' names are macros for them.
 *  - The CPU. isod reads CPUID in a constructor; tsdb allows no startup code in its codec objects (check-isa.sh
 *    refuses an .init_array in ec_*.o; the rule comes from the codecs built once per level, AGENTS.md "CPU levels"),
 *    and the kernels must follow the level src/cpu.c picks. So `iso_cpu_has` asks the compiler's
 *    __builtin_cpu_supports and answers no to anything above the level src/cpu.c selected (tsdb_ec_select): a
 *    TSDB_CPU=v2 process never executes an AVX instruction, v3 never an AVX-512 one. gf256.c's dispatch looks only
 *    at iso_cpu_has, so this is the whole cap.
 *  - Panics. A failed ISO_ASSERT or ISO_PANIC (a coefficient of 0 divided by, a singular Cauchy submatrix) is a bug
 *    of the caller or of the code: it logs and aborts, as upstream.
 *
 * Nothing here runs before main, and none of it is in the public API (include/tsdb.h): tools/libtsdb.map exports
 * tsdb_*, so it lists tsdb_ec_* as local; tools/check-exports.sh (make test) compares libtsdb.so's exports with the headers.
 */
#ifndef TSDB_EC_COMPAT_H
#define TSDB_EC_COMPAT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define ISO_LIKELY(x)   __builtin_expect(!!(x), 1)
#define ISO_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define ISO_NORETURN    __attribute__((noreturn))
#define ISO_PRINTF(f, a) __attribute__((format(printf, f, a)))
#define ISO_MALLOC_LIKE __attribute__((malloc))

#define ISO_ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))
#define ISO_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)

/* The numbers are isod's; rs.h returns ISO_OK or ISO_ERR_RANGE. */
typedef enum {
    ISO_OK = 0,
    ISO_ERR_NOMEM = -1,
    ISO_ERR_INVAL = -2,
    ISO_ERR_IO = -3,
    ISO_ERR_NOTFOUND = -4,
    ISO_ERR_EXISTS = -5,
    ISO_ERR_CORRUPT = -6,
    ISO_ERR_FULL = -7,
    ISO_ERR_AGAIN = -8,
    ISO_ERR_TIMEOUT = -9,
    ISO_ERR_CLOSED = -10,
    ISO_ERR_PERM = -11,
    ISO_ERR_RANGE = -12,
} iso_err;

/* common/kcheck.h: what a known-answer check of a family of kernels found. Plain data. */
#define ISO_KCHECK_MAX 8
#define ISO_KCHECK_NAME_LEN 16
typedef struct {
    char family[ISO_KCHECK_NAME_LEN];
    char reference[ISO_KCHECK_NAME_LEN];
    char preferred[ISO_KCHECK_NAME_LEN];
    char chosen[ISO_KCHECK_NAME_LEN];
    unsigned chosen_index;
    unsigned n_checked;
    unsigned n_failed;
    char failed[ISO_KCHECK_MAX][ISO_KCHECK_NAME_LEN];
    char why[256];
    bool reference_failed;
    uint64_t ns;
} iso_kcheck;

/* common/cpu.h: the features the kernels ask for. */
typedef enum {
    ISO_CPU_SSSE3,
    ISO_CPU_AVX2,
    ISO_CPU_AVX512F,
    ISO_CPU_AVX512BW,
    ISO_CPU_GFNI,
    ISO_CPU_FEATURE_COUNT,
} iso_cpu_feature;

bool tsdb_ec_cpu_has(iso_cpu_feature f);
bool tsdb_ec_prefer_512(void);
#define iso_cpu_has tsdb_ec_cpu_has
#define iso_cpu_prefer_512 tsdb_ec_prefer_512

/* The level (CPU_BASE, CPU_V3, CPU_V4 of internal.h) the kernels may use, and the dispatch chosen again under it:
 * iso_gf256_init(). src/cpu.c calls it at startup and at every cpu_set_level, before any thread encodes. */
void tsdb_ec_select(int level);

/* common/util.h, log.h, clock.h */
void *tsdb_ec_xmalloc(size_t n) ISO_MALLOC_LIKE;
size_t tsdb_ec_strlcpy(char *dst, const char *src, size_t dstsize);
uint64_t tsdb_ec_now_ns(void);
ISO_NORETURN void tsdb_ec_panic(const char *file, int line, const char *fmt, ...) ISO_PRINTF(3, 4);
#define iso_xmalloc tsdb_ec_xmalloc
#define iso_strlcpy tsdb_ec_strlcpy
#define iso_now_ns tsdb_ec_now_ns
#define iso_panic tsdb_ec_panic

#define ISO_PANIC(...) tsdb_ec_panic(__FILE__, __LINE__, __VA_ARGS__)
#define ISO_ASSERT(cond)                                                                                               \
    do {                                                                                                               \
        if (ISO_UNLIKELY(!(cond)))                                                                                     \
            ISO_PANIC("assertion failed: %s", #cond);                                                                  \
    } while (0)

#endif
