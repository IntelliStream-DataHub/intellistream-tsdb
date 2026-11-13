/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* Arithmetic in GF(2^8), the field the Reed-Solomon codec works over.
 *
 * Primitive polynomial 0x11d (x^8 + x^4 + x^3 + x^2 + 1) -- the same choice as Jerasure, ISA-L
 * and klauspost/reedsolomon, so shards produced here are interoperable with the usual reference
 * implementations and the field tables can be checked against published ones.
 *
 * The hot operation is not single-element multiply; it is `dst ^= c * src` over a whole shard,
 * which is what encoding and decoding reduce to. Six kernels implement it:
 *
 *   scalar    256-byte row of the product table per multiplier; stays in L1.
 *   ssse3     the classic split-table trick: multiply the low and high nibbles separately with
 *             two 16-entry tables and one PSHUFB each, 16 bytes per iteration.
 *   avx2      the same, 32 bytes per iteration.
 *   avx512    the same, 64 bytes per iteration (AVX512F + AVX512BW).
 *   gfni256   VGF2P8AFFINEQB: one instruction *is* the GF(2^8) multiply by a constant, so the
 *   gfni512   nibble split disappears entirely. 32 or 64 bytes per iteration.
 *
 * Encoding and decoding need more than that: every output shard is a sum of k products. Calling
 * the kernel above once per (output, source) pair reads each output k times and writes it k times,
 * and reads each source once per output. iso_gf256_dot() does the whole sum, for a group of
 * outputs at once, in registers: every source vector is loaded once per group, every output
 * vector is stored once and never read (the ISA-L gf_Nvect_dot_prod idea). Each of the six
 * kernels below has a dot kernel beside it, picked by the same ladder entry.
 *
 * The wide kernels live in gf256_avx2.c and gf256_avx512.c behind function target attributes, so
 * this file and the rest of the build stay at the -march=x86-64-v2 baseline and one binary runs
 * everywhere. The Ivy Bridge reference host reaches only the ssse3 kernel; Broadwell and Skylake
 * add avx2, Skylake-SP adds avx512, and Zen 4 / Zen 5 / Ice Lake and later reach the gfni ones.
 *
 * iso_gf256_mul_add() dispatches once, when iso_gf256_init() runs at start-up. Every kernel is also
 * exported individually so the unit tests can assert they agree byte for byte with the scalar one
 * and bench_gf256 can time them side by side.
 */
#ifndef ISO_CODEC_GF256_H
#define ISO_CODEC_GF256_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "common/kcheck.h"

#define ISO_GF256_POLY 0x11d

/* Builds the field tables (once) and dispatches to the top of the ladder, unchecked. Idempotent:
 * the tables are built by the first call and only read after; EVERY call picks the ladder again
 * from what iso_cpu_has() says then, so a caller whose features change after the first call calls
 * it again and gets the kernel for the new answer, within any cap. Not thread-safe: before any
 * thread and before the first product, as iso_gf256_use() is. The contract is at the definition.
 *
 * gf256.c calls it from a constructor (priority 150, after cpu.c's 101), so a program that links
 * this repository's library never calls it. Compiled with -DISO_GF256_NO_BOOT the constructor is
 * not there (the object has no .init_array entry), and the program calls it itself. That is for a
 * program that copies this file into a binary which may have no startup code and chooses the
 * kernel by a CPU level of its own, as tsdb does. Without the constructor and before the call the
 * field tables are empty and every product is 0, so a program built that way must call this
 * before its first use. */
void iso_gf256_init(void);

/* ------------------------------------------------------------------ scalar field ops */

/* Product table row for `a`: iso_gf256_mul(a, b) == iso_gf256_row(a)[b]. 64 KiB in total. */
const uint8_t *iso_gf256_row(uint8_t a);

uint8_t iso_gf256_mul(uint8_t a, uint8_t b);

/* Division; b must not be 0. */
uint8_t iso_gf256_div(uint8_t a, uint8_t b);

/* Multiplicative inverse; a must not be 0. */
uint8_t iso_gf256_inv(uint8_t a);

/* a raised to an integer power (negative exponents allowed for non-zero a). */
uint8_t iso_gf256_pow(uint8_t a, int n);

/* ------------------------------------------------------------------ vector ops */

/* dst[i] ^= c * src[i] for i < len. The workhorse: RS encode and decode are a sequence of these.
 * c == 0 is a no-op and c == 1 degenerates to XOR, both handled without a table lookup. */
void iso_gf256_mul_add(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);

/* dst[i] = c * src[i]; used for the first term of an accumulation so the caller need not clear
 * the destination first. */
void iso_gf256_mul_set(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);

/* dst[i] ^= src[i]. */
void iso_gf256_xor(uint8_t *dst, const uint8_t *src, size_t len);

/* dst[i][x] = XOR over j < k of coef[i * k + j] * src[j][x], for every i < m and every x in
 * [off, off + len) (so each buffer is touched from byte `off` for `len` bytes and not past it).
 * The outputs are SET, not accumulated: what was in them is lost, and an empty sum (k == 0) is
 * zeros. `coef` is the m x k matrix, row-major. No output may overlap a source or another output.
 *
 * Any m >= 0 and k >= 0 are valid, and any off, len and alignment. The kernel passes over the
 * range once for every group of up to four outputs (all the library's geometries, m <= 4, are one
 * group), so a caller with more outputs and a long range cuts the range into tiles that stay in
 * cache, as rs.c does. */
typedef void (*iso_gf256_dot_fn)(uint8_t *const *dst, unsigned m, const uint8_t *const *src,
				 unsigned k, const uint8_t *coef, size_t off, size_t len);

/* Dispatches to the dot kernel of the kernel iso_gf256_use() set. A kernel handed to
 * iso_gf256_use() without a dot kernel (a test's planted kernel, say) gets the sum made of its
 * mul_set and mul_add, as the codec did before the dot kernels. */
void iso_gf256_dot(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
		   const uint8_t *coef, size_t off, size_t len);

/* ------------------------------------------------------------------ kernels
 *
 * Exported for the equality tests and for bench_gf256. Prefer iso_gf256_mul_add() in real code.
 */
void iso_gf256_mul_add_scalar(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);
void iso_gf256_mul_add_ssse3(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);
void iso_gf256_mul_add_avx2(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);
void iso_gf256_mul_add_avx512(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);
void iso_gf256_mul_add_gfni256(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);
void iso_gf256_mul_add_gfni512(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);

/* The dot kernels of iso_gf256_dot_fn, one per family. A wide kernel hands the bytes after its last
 * whole vector to the next narrower one (avx512 and gfni256 to avx2, gfni512 to gfni256, avx2 and
 * gfni256 to ssse3, ssse3 to scalar), as the mul_add kernels do. */
void iso_gf256_dot_scalar(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			  const uint8_t *coef, size_t off, size_t len);
void iso_gf256_dot_ssse3(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			 const uint8_t *coef, size_t off, size_t len);
void iso_gf256_dot_avx2(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			const uint8_t *coef, size_t off, size_t len);
void iso_gf256_dot_avx512(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			  const uint8_t *coef, size_t off, size_t len);
void iso_gf256_dot_gfni256(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			   const uint8_t *coef, size_t off, size_t len);
void iso_gf256_dot_gfni512(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			   const uint8_t *coef, size_t off, size_t len);

/* Every kernel, in one table, so tests and benchmarks can iterate rather than repeat a ladder.
 * `available` is false when this CPU cannot run it; calling such a kernel is undefined. */
typedef struct {
	const char *name;
	void (*fn)(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c);
	bool available;
	unsigned width; /* bytes per iteration, for reporting */
	/* The kernel's dot kernel; NULL for a kernel that has none (the dispatch then composes the
	 * sum from `fn`, and the check has nothing more to check). Last, so a positional initializer
	 * of the first four fields leaves it NULL. */
	iso_gf256_dot_fn dot;
} iso_gf256_kernel;

/* Returns the number of kernels and fills `out` (which must hold at least ISO_GF256_MAX_KERNELS). */
#define ISO_GF256_MAX_KERNELS 6
size_t iso_gf256_kernels(iso_gf256_kernel *out);

/* Name of the kernel iso_gf256_mul_add() dispatches to. */
const char *iso_gf256_impl(void);

/* ------------------------------------------------------------------ the check where it runs
 *
 * Section 26k, #181: the kernel the dispatch picks is checked on the machine it runs on, before a
 * node starts, and one that disagrees with the scalar kernel is never used. */

/* The kernels this CPU can run, in the order the dispatch prefers them, the scalar one last: what
 * iso_gf256_init() chooses from, and what the check walks. Returns how many; `out` must hold
 * ISO_GF256_MAX_KERNELS. */
size_t iso_gf256_ladder(iso_gf256_kernel *out);

/* `k[n-1]` is the reference -- the ladder puts the scalar kernel last -- and is held to a few
 * products of the field itself first: one that gets them wrong sets `reference_failed`, and
 * nothing else is checked. Then `k[0..n-1)` in order against the reference, stopping at the first
 * that agrees -- `fn`, and then `dot` when it has one (see below): all 256 coefficients over every length from 0 to 257 -- each vector width's every
 * tail, and four rounds of the widest -- aligned for even coefficients and not for odd ones, a
 * guard past the end that must not be written, and 64 KiB + 3 at an unaligned address under eight
 * coefficients. `chosen_index` is `n - 1`, the reference, when none agrees. Switches nothing. */
void iso_gf256_check(const iso_gf256_kernel *k, size_t n, iso_kcheck *out);
/* The dot kernel of a kernel that has one is held to the reference's (the scalar kernel's `dot`, or
 * the built-in scalar dot kernel where that is NULL) the same way: m from 1 to 5 (a second group
 * of outputs, and a short last one), k from 1 to 17, random coefficients and the edge ones (0, 1,
 * 0x80, 0xff), every length from 0 to 130 at ranges that start unaligned, a guard past the end of
 * every output, and a long range. One that disagrees is not chosen, and `why` names the shape. */

/* Dispatch to `k` from now on; NULL is the scalar kernel. Not thread-safe: before any thread, as
 * iso_gf256_init() is. */
void iso_gf256_use(const iso_gf256_kernel *k);

/* ------------------------------------------------------------------ GFNI matrix
 *
 * VGF2P8AFFINEQB computes, per byte, an 8x8 GF(2) matrix-vector product. Multiplication by a
 * constant c in GF(2^8) is GF(2)-linear, so it is exactly such a product -- the matrix is built
 * once per multiplier and the instruction does the rest.
 *
 * Intel's operand order puts the row for output bit k in byte (7 - k) of the qword.
 */

/* The affine matrix for multiplication by `c` under ISO_GF256_POLY. */
uint64_t iso_gf256_gfni_matrix(uint8_t c);

/* Portable reference implementation of what VGF2P8AFFINEQB does to one byte. Only the tests use
 * it -- but it lets the matrix construction above be verified exhaustively on a host with no GFNI
 * at all, leaving only the instruction encoding untested there. */
uint8_t iso_gf256_gfni_affine_ref(uint64_t matrix, uint8_t x);

/* The nibble tables the SIMD kernels use: 16 low-nibble products and 16 high-nibble products per
 * multiplier. Exposed so gf256_avx2.c can share them. The 256 rows of each are consecutive, so
 * the dot kernels take iso_gf256_shuf_lo(0) once and index it by 16 * c. */
extern const uint8_t *iso_gf256_shuf_lo(uint8_t c);
extern const uint8_t *iso_gf256_shuf_hi(uint8_t c);

/* The GFNI matrices of all 256 multipliers, iso_gf256_gfni_matrix(c) == iso_gf256_gfni_matrices()[c],
 * built once with the tables; the dot kernels broadcast one per product from here. */
const uint64_t *iso_gf256_gfni_matrices(void);

#endif /* ISO_CODEC_GF256_H */
