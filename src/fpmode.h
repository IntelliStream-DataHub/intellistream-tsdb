/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The codecs' floating-point arithmetic needs round-to-nearest.
 *
 * The decimal codecs find an integer k for a value v (llrint of v * 10^e) and keep k only when k / 10^e (or
 * k * 10^-e) gives v back bit for bit, so what a block holds depends on how the CPU rounds: the encoder
 * under FE_DOWNWARD stores other integers and other exceptions than under FE_TONEAREST, and the decoder under any
 * other mode returns values one ulp from what was written. A host application that sets the rounding mode (the engine
 * never does) would store blocks that later decode to other values, silently, or get other values from the same
 * block in the thread that calls the query than in the scheduler's helper threads, which keep the mode of the thread
 * that opened the database (the mode is per thread, and a new thread inherits its creator's).
 *
 * So every entry of the block layer that does such arithmetic (block_encode_f32, block_encode_vals,
 * block_decode_f32, block_decode_vals) runs with round-to-nearest and puts the caller's mode back:
 *
 *     fp_mode m = fp_nearest();  ...  fp_restore(m);
 *
 * On x86 that is the SSE control register (MXCSR, bits 13 and 14), which every scalar and vector double and float
 * operation of x86-64 uses (glibc's fegetround reads the x87 word only, which an application that sets MXCSR
 * itself leaves alone). Already at nearest, the cost is one STMXCSR; otherwise one LDMXCSR in and one out. Nested
 * calls are as cheap. Only the rounding bits are written, never the exception flags or masks, and the restore
 * keeps flags the arithmetic raised, as if the mode had not been changed.
 *
 * The code between the two calls must be in a function the compiler cannot inline into the caller (GCC does not
 * model the control register, and would move arithmetic across the writes): the block_* wrappers call a noinline body.
 */
#ifndef TSDB_FPMODE_H
#define TSDB_FPMODE_H

#if defined(__SSE2__)
#include <xmmintrin.h>
#define FP_RC_MASK 0x6000u
typedef unsigned fp_mode;
static inline fp_mode fp_nearest(void)
{
    unsigned m = _mm_getcsr();
    if (m & FP_RC_MASK)
        _mm_setcsr(m & ~FP_RC_MASK);
    return m;
}
static inline void fp_restore(fp_mode m)
{
    if (m & FP_RC_MASK)
        _mm_setcsr((_mm_getcsr() & ~FP_RC_MASK) | (m & FP_RC_MASK));
}
#else
#include <fenv.h>
typedef int fp_mode;
static inline fp_mode fp_nearest(void)
{
    int m = fegetround();
    if (m != FE_TONEAREST)
        fesetround(FE_TONEAREST);
    return m;
}
static inline void fp_restore(fp_mode m)
{
    if (m != FE_TONEAREST)
        fesetround(m);
}
#endif

#endif
