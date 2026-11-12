/* SPDX-License-Identifier: AGPL-3.0-only */
/* x87 extended precision (a 64-bit mantissa, round to nearest even) emulated with integers, for the one result
 * that rests on it: the integer variances of int_variance (query.c), which convert an exact 256-bit numerator
 * through long double. On x86 long double is the x87 format and query.c uses it directly; where it is not (arm64
 * macOS: a plain double; arm64 Linux: binary128) the same steps run here, so every platform answers with the same
 * bits. tests/test_spread.c compares this with the x87 arithmetic on x86, and with known x87 answers everywhere.
 *
 * Only what int_variance needs: nonnegative finite values far from the exponent limits (the numerator is below
 * 2^257, the denominators below 2^128, the quotients between 2^-128 and 2^257), always rounded to nearest even
 * (the default mode; the x87 path follows the thread's mode, this one does not). Integers only: no libm, the same
 * bits at every CPU level. */
#ifndef TSDB_EXT80_H
#define TSDB_EXT80_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

typedef struct {
    uint64_t m; /* 0, or bit 63 set */
    int e;      /* the value is m * 2^e */
} ext80;

/* bit `pos` and up of the integer u[0..k-1] (64-bit limbs, least significant first), 64 bits */
static inline uint64_t ext80_bits(const uint64_t *u, int k, int pos)
{
    int l = pos / 64, o = pos % 64;
    uint64_t r = l < k ? u[l] >> o : 0;
    if (o && l + 1 < k)
        r |= u[l + 1] << (64 - o);
    return r;
}

/* any bit below `pos` set */
static inline bool ext80_below(const uint64_t *u, int pos)
{
    int l = pos / 64, o = pos % 64;
    for (int i = 0; i < l; i++)
        if (u[i])
            return true;
    return o && (u[l] & ((UINT64_C(1) << o) - 1));
}

/* the integer u[0..k-1], scaled by 2^e, rounded once to 64 bits (nearest, ties to even) */
static inline ext80 ext80_round(const uint64_t *u, int k, int e)
{
    int top = k - 1;
    while (top >= 0 && !u[top])
        top--;
    if (top < 0)
        return (ext80){0, 0};
    int lz = __builtin_clzll(u[top]);
    int sh = 64 * top - lz; /* the bit length less 64 */
    if (sh <= 0)
        return (ext80){u[0] << -sh, e + sh};
    uint64_t q = ext80_bits(u, k, sh);
    if (((u[(sh - 1) / 64] >> ((sh - 1) % 64)) & 1) && (ext80_below(u, sh - 1) || (q & 1)) && ++q == 0) {
        q = UINT64_C(1) << 63;
        sh++;
    }
    return (ext80){q, e + sh};
}

static inline ext80 ext80_u64(uint64_t v)
{
    return ext80_round(&v, 1, 0);
}

static inline ext80 ext80_mul(ext80 a, ext80 b)
{
    unsigned __int128 p = (unsigned __int128)a.m * b.m;
    uint64_t u[2] = {(uint64_t)p, (uint64_t)(p >> 64)};
    return ext80_round(u, 2, a.e + b.e);
}

static inline ext80 ext80_div(ext80 a, ext80 b) /* b != 0 */
{
    if (!a.m)
        return a;
    /* 128 bits of quotient past the 65 of a.m * 2^64 / b.m, and below them one bit for a remainder: enough to round
     * (the quotient has at least 191 bits, so the bit that decides a tie is never that last one) */
    unsigned __int128 n = (unsigned __int128)a.m << 64, q1 = n / b.m;
    uint64_t r1 = (uint64_t)(n % b.m);
    unsigned __int128 n2 = (unsigned __int128)r1 << 64;
    uint64_t q2 = (uint64_t)(n2 / b.m), r2 = (uint64_t)(n2 % b.m);
    uint64_t u[4] = {r2 != 0, q2, (uint64_t)q1, (uint64_t)(q1 >> 64)};
    return ext80_round(u, 4, a.e - b.e - 192);
}

/* to double, rounded once more (nearest, ties to even), as (double) of a long double does; normal results only */
static inline double ext80_to_double(ext80 a)
{
    if (!a.m)
        return 0;
    uint64_t q = a.m >> 11;
    int e = a.e + 11;
    if (((a.m >> 10) & 1) && ((a.m & 0x3ff) || (q & 1)) && ++q == UINT64_C(1) << 53) {
        q >>= 1;
        e++;
    }
    uint64_t bits = (uint64_t)(e + 52 + 1023) << 52 | (q & ((UINT64_C(1) << 52) - 1));
    double d;
    memcpy(&d, &bits, sizeof d);
    return d;
}

/* What int_variance does in long double, for the exact numerator d[0..3] and n rows (n >= 1):
 * num = ((d3 * 2^64 + d2) * 2^64 + d1) * 2^64 + d0, rounded after each addition; vp = num / (n * n),
 * vs = num / (n * (n - 1)) (n > 1), each product and quotient rounded, then each quotient rounded to double. */
static inline void ext80_variance(const uint64_t d[4], uint64_t n, double *vp, double *vs)
{
    ext80 num = ext80_u64(d[3]);
    for (int i = 2; i >= 0; i--) {
        /* num * 2^64 + d[i]: num is an integer here (it was rounded from one), so exact in 5 limbs */
        uint64_t u[5] = {d[i], 0, 0, 0, 0};
        if (num.m) {
            if (num.e >= 0) {
                int l = 1 + num.e / 64, o = num.e % 64;
                u[l] |= num.m << o;
                if (o)
                    u[l + 1] |= num.m >> (64 - o);
            } else {
                u[1] = num.m >> -num.e;
            }
        }
        num = ext80_round(u, 5, 0);
    }
    ext80 ln = ext80_u64(n);
    *vp = ext80_to_double(ext80_div(num, ext80_mul(ln, ln)));
    if (n > 1)
        *vs = ext80_to_double(ext80_div(num, ext80_mul(ln, ext80_u64(n - 1))));
}

#endif
