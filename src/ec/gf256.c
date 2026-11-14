/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
#include "codec/gf256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tmmintrin.h> /* SSSE3: _mm_shuffle_epi8 */

#include "common/clock.h"
#include "common/cpu.h"
#include "common/log.h"
#include "common/util.h"

/* ------------------------------------------------------------------ tables
 *
 * Generated at start-up rather than checked in: the generator is short enough to read and audit,
 * and a table typo would otherwise be nearly impossible to spot. Cost is ~80 us once.
 *
 *   exp/log      the discrete logarithm base 2 (2 is primitive for 0x11d)
 *   mul_table    full 256x256 product table, 64 KiB; a row is 256 bytes and stays in L1
 *   shuf_lo/hi   nibble tables for PSHUFB: c*x split as c*(x & 0xf) ^ c*(x >> 4 << 4)
 *   gfni_mat     the VGF2P8AFFINEQB matrix of every multiplier, for the dot kernels
 */

static uint8_t gf_exp[512];
static uint8_t gf_log[256];
static uint8_t gf_mul_table[256][256];
static uint8_t gf_shuf_lo[256][16];
static uint8_t gf_shuf_hi[256][16];
static uint64_t gf_gfni_mat[256];

/* Carry-less multiply followed by reduction modulo the primitive polynomial. Used only to build
 * the tables, so clarity beats speed. */
static uint8_t mul_slow(uint8_t a, uint8_t b)
{
	unsigned r = 0;
	unsigned x = a;
	for (unsigned i = 0; i < 8; i++) {
		if (b & (1u << i))
			r ^= x << i;
	}
	/* Fold bits 8..14 back down. */
	for (int bit = 14; bit >= 8; bit--) {
		if (r & (1u << bit))
			r ^= (unsigned)ISO_GF256_POLY << (bit - 8);
	}
	return (uint8_t)r;
}

/* ------------------------------------------------------------------ GFNI matrix (built with the tables)
 *
 * VGF2P8AFFINEQB computes, per byte of the data operand, output bit k as
 *
 *     parity( data_byte AND matrix.byte[7 - k] )
 *
 * so the matrix row selecting which input bits contribute to output bit k lives in byte (7 - k).
 * Multiplication by c is GF(2)-linear, so row k has bit j set exactly when bit k of c * 2^j is
 * set -- the multiplier's action on each basis element, transposed.
 */
static uint64_t gfni_matrix_build(uint8_t c)
{
	uint64_t m = 0;
	for (unsigned k = 0; k < 8; k++) {
		uint8_t row = 0;
		for (unsigned j = 0; j < 8; j++) {
			if ((gf_mul_table[c][1u << j] >> k) & 1u)
				row |= (uint8_t)(1u << j);
		}
		m |= (uint64_t)row << (8 * (7 - k));
	}
	return m;
}

uint64_t iso_gf256_gfni_matrix(uint8_t c)
{
	return gfni_matrix_build(c);
}

const uint64_t *iso_gf256_gfni_matrices(void)
{
	return gf_gfni_mat;
}

static void gf256_tables_init(void)
{
	/* exp[i] = 2^i. The multiplicative group has order 255, so the sequence repeats there;
	 * the table is doubled so a log sum up to 508 can be looked up without a modulo. */
	uint8_t x = 1;
	for (unsigned i = 0; i < 255; i++) {
		gf_exp[i] = x;
		gf_log[x] = (uint8_t)i;
		x = mul_slow(x, 2);
	}
	for (unsigned i = 255; i < 512; i++)
		gf_exp[i] = gf_exp[i - 255];
	gf_log[0] = 0; /* undefined; never read through a path that can reach 0 */

	for (unsigned a = 0; a < 256; a++)
		for (unsigned b = 0; b < 256; b++)
			gf_mul_table[a][b] = mul_slow((uint8_t)a, (uint8_t)b);

	for (unsigned c = 0; c < 256; c++) {
		for (unsigned n = 0; n < 16; n++) {
			gf_shuf_lo[c][n] = gf_mul_table[c][n];
			gf_shuf_hi[c][n] = gf_mul_table[c][n << 4];
		}
		gf_gfni_mat[c] = gfni_matrix_build((uint8_t)c);
	}
}

const uint8_t *iso_gf256_row(uint8_t a)
{
	return gf_mul_table[a];
}

const uint8_t *iso_gf256_shuf_lo(uint8_t c)
{
	return gf_shuf_lo[c];
}

const uint8_t *iso_gf256_shuf_hi(uint8_t c)
{
	return gf_shuf_hi[c];
}

uint8_t iso_gf256_mul(uint8_t a, uint8_t b)
{
	return gf_mul_table[a][b];
}

uint8_t iso_gf256_gfni_affine_ref(uint64_t matrix, uint8_t x)
{
	uint8_t out = 0;
	for (unsigned k = 0; k < 8; k++) {
		uint8_t row = (uint8_t)(matrix >> (8 * (7 - k)));
		out |= (uint8_t)((unsigned)__builtin_parity((unsigned)(row & x)) << k);
	}
	return out;
}

uint8_t iso_gf256_div(uint8_t a, uint8_t b)
{
	ISO_ASSERT(b != 0);
	if (a == 0)
		return 0;
	return gf_exp[gf_log[a] + 255 - gf_log[b]];
}

uint8_t iso_gf256_inv(uint8_t a)
{
	ISO_ASSERT(a != 0);
	return gf_exp[255 - gf_log[a]];
}

uint8_t iso_gf256_pow(uint8_t a, int n)
{
	if (a == 0)
		return n == 0 ? 1 : 0;
	if (n == 0)
		return 1;
	/* Exponents are taken modulo the group order 255, keeping the result in the table. */
	int e = ((int)gf_log[a] * n) % 255;
	if (e < 0)
		e += 255;
	return gf_exp[e];
}

/* ------------------------------------------------------------------ scalar kernel */

void iso_gf256_xor(uint8_t *dst, const uint8_t *src, size_t len)
{
	size_t i = 0;
	/* Unaligned 8-byte words: the compiler turns this into wide loads and stores, and shards
	 * are 4 KiB-aligned in practice anyway. */
	for (; i + 8 <= len; i += 8) {
		uint64_t a, b;
		memcpy(&a, dst + i, 8);
		memcpy(&b, src + i, 8);
		a ^= b;
		memcpy(dst + i, &a, 8);
	}
	for (; i < len; i++)
		dst[i] ^= src[i];
}

void iso_gf256_mul_add_scalar(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	if (c == 0)
		return;
	if (c == 1) {
		iso_gf256_xor(dst, src, len);
		return;
	}

	const uint8_t *row = gf_mul_table[c];
	size_t i = 0;
	/* Four at a time: independent table lookups let the out-of-order engine overlap the
	 * ~4-cycle L1 latencies instead of serialising on them. */
	for (; i + 4 <= len; i += 4) {
		dst[i + 0] ^= row[src[i + 0]];
		dst[i + 1] ^= row[src[i + 1]];
		dst[i + 2] ^= row[src[i + 2]];
		dst[i + 3] ^= row[src[i + 3]];
	}
	for (; i < len; i++)
		dst[i] ^= row[src[i]];
}

/* ------------------------------------------------------------------ SSSE3 kernel
 *
 * PSHUFB is a 16-entry parallel table lookup. A GF multiply by a constant c is linear over the
 * bits of the operand, so it splits across nibbles:
 *
 *     c * x  =  c * (x & 0x0f)  ^  c * (x & 0xf0)
 *
 * Each half is a 16-entry table, which is exactly what PSHUFB can index. -march=x86-64-v2
 * guarantees SSSE3, so no target attribute is needed here.
 */
void iso_gf256_mul_add_ssse3(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	if (c == 0)
		return;
	if (c == 1) {
		iso_gf256_xor(dst, src, len);
		return;
	}

	const __m128i lo_tbl = _mm_loadu_si128((const __m128i *)gf_shuf_lo[c]);
	const __m128i hi_tbl = _mm_loadu_si128((const __m128i *)gf_shuf_hi[c]);
	const __m128i mask = _mm_set1_epi8(0x0f);

	size_t i = 0;
	for (; i + 16 <= len; i += 16) {
		__m128i s = _mm_loadu_si128((const __m128i *)(src + i));
		__m128i d = _mm_loadu_si128((const __m128i *)(dst + i));

		__m128i lo = _mm_and_si128(s, mask);
		__m128i hi = _mm_and_si128(_mm_srli_epi64(s, 4), mask);

		__m128i prod = _mm_xor_si128(_mm_shuffle_epi8(lo_tbl, lo),
					     _mm_shuffle_epi8(hi_tbl, hi));
		_mm_storeu_si128((__m128i *)(dst + i), _mm_xor_si128(d, prod));
	}

	if (i < len)
		iso_gf256_mul_add_scalar(dst + i, src + i, len - i, c);
}

/* ------------------------------------------------------------------ dot kernels
 *
 * dst[i][x] = XOR_j coef[i][j] * src[j][x]: the sum encoding and decoding are made of, for m
 * outputs at once. The mul_add kernels above compute it one product at a time, so every output
 * is read and written once per source and every source is read once per output. A dot kernel
 * keeps a group of up to DOT_G output vectors in registers, runs through all k sources for one
 * vector position, and stores the group once: each source vector is loaded once per group, each
 * output vector is written once and never read. What that buys is memory traffic -- on shards
 * that do not stay in cache the mul_add loop was 4+ times slower per byte than the multiply
 * (docs/tasks/ec-bench.md of tsdb) -- and the arithmetic per product is the same.
 *
 * The shape is the same in every family (this file: scalar and ssse3; gf256_avx2.c; gf256_avx512.c):
 * a loop over groups of outputs, inside it a loop over vector positions, inside it a loop over
 * the sources; the bytes after the last whole vector go to the next narrower kernel for all m
 * outputs, with `off` moved up. The group size is 4 in every family: it is the most outputs
 * whose accumulators, with a source vector, its two nibbles, a mask and two tables, fit the 16
 * registers of SSE and AVX2 (the AVX-512 and GFNI kernels have room for two vectors of each at
 * once and use it), and it is the largest m the library has (ISO_RS_MAX_M), so one pass is all
 * an encode needs. A coefficient of 0 or 1 is not special-cased: its table gives 0 or x, and a
 * branch in the loop would cost more than the product it skips.
 */

#define DOT_G 4

void iso_gf256_dot_scalar(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			  const uint8_t *coef, size_t off, size_t len)
{
	for (unsigned i = 0; i < m; i++) {
		uint8_t *out = dst[i] + off;
		if (k == 0) {
			memset(out, 0, len);
			continue;
		}
		const uint8_t *row = gf_mul_table[coef[(size_t)i * k]];
		const uint8_t *in = src[0] + off;
		for (size_t x = 0; x < len; x++)
			out[x] = row[in[x]];
		for (unsigned j = 1; j < k; j++) {
			row = gf_mul_table[coef[(size_t)i * k + j]];
			in = src[j] + off;
			for (size_t x = 0; x < len; x++)
				out[x] ^= row[in[x]];
		}
	}
}

/* One group of G outputs (G a constant at every call, so the accumulators are registers) over the
 * 16-byte vectors from `from` up to `end`. `coef` is the group's first row, rows k apart. */
static inline __attribute__((always_inline)) void
dot_ssse3_group(uint8_t *const *dst, const uint8_t *const *src, unsigned k, const uint8_t *coef,
		size_t from, size_t end, const unsigned G)
{
	const __m128i mask = _mm_set1_epi8(0x0f);
	for (size_t x = from; x < end; x += 16) {
		__m128i acc[DOT_G];
#pragma GCC unroll 4
		for (unsigned i = 0; i < G; i++)
			acc[i] = _mm_setzero_si128();
		for (unsigned j = 0; j < k; j++) {
			__m128i s = _mm_loadu_si128((const __m128i *)(src[j] + x));
			__m128i lo = _mm_and_si128(s, mask);
			__m128i hi = _mm_and_si128(_mm_srli_epi64(s, 4), mask);
#pragma GCC unroll 4
			for (unsigned i = 0; i < G; i++) {
				size_t c16 = (size_t)coef[(size_t)i * k + j] << 4;
				__m128i tl = _mm_loadu_si128((const __m128i *)(&gf_shuf_lo[0][0] + c16));
				__m128i th = _mm_loadu_si128((const __m128i *)(&gf_shuf_hi[0][0] + c16));
				acc[i] = _mm_xor_si128(acc[i], _mm_xor_si128(_mm_shuffle_epi8(tl, lo),
									     _mm_shuffle_epi8(th, hi)));
			}
		}
#pragma GCC unroll 4
		for (unsigned i = 0; i < G; i++)
			_mm_storeu_si128((__m128i *)(dst[i] + x), acc[i]);
	}
}

void iso_gf256_dot_ssse3(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			 const uint8_t *coef, size_t off, size_t len)
{
	size_t body = len & ~(size_t)15;
	if (body) {
		for (unsigned g = 0; g < m; g += DOT_G) {
			unsigned n = m - g < DOT_G ? m - g : DOT_G;
			const uint8_t *rows = coef + (size_t)g * k;
			switch (n) {
			case 1:
				dot_ssse3_group(dst + g, src, k, rows, off, off + body, 1);
				break;
			case 2:
				dot_ssse3_group(dst + g, src, k, rows, off, off + body, 2);
				break;
			case 3:
				dot_ssse3_group(dst + g, src, k, rows, off, off + body, 3);
				break;
			default:
				dot_ssse3_group(dst + g, src, k, rows, off, off + body, 4);
				break;
			}
		}
	}
	if (body < len)
		iso_gf256_dot_scalar(dst, m, src, k, coef, off + body, len - body);
}

/* ------------------------------------------------------------------ dispatch */

static void (*mul_add_impl)(uint8_t *, const uint8_t *, size_t, uint8_t) =
	iso_gf256_mul_add_scalar;
static const char *mul_add_name = "scalar";
/* NULL: the sum is made of mul_set and mul_add (a kernel without a dot kernel; the state before
 * the first iso_gf256_use). */
static iso_gf256_dot_fn dot_impl;

/* The sum of dot kernels made of the mul_add kernel in force: what a kernel with no dot kernel
 * of its own gets, and what the codec did before there were any. */
static void dot_compose(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
			const uint8_t *coef, size_t off, size_t len)
{
	for (unsigned i = 0; i < m; i++) {
		uint8_t *out = dst[i] + off;
		if (k == 0) {
			memset(out, 0, len);
			continue;
		}
		iso_gf256_mul_set(out, src[0] + off, len, coef[(size_t)i * k]);
		for (unsigned j = 1; j < k; j++)
			iso_gf256_mul_add(out, src[j] + off, len, coef[(size_t)i * k + j]);
	}
}

void iso_gf256_dot(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
		   const uint8_t *coef, size_t off, size_t len)
{
	if (dot_impl)
		dot_impl(dst, m, src, k, coef, off, len);
	else
		dot_compose(dst, m, src, k, coef, off, len);
}

void iso_gf256_mul_add(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	mul_add_impl(dst, src, len, c);
}

void iso_gf256_mul_set(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	if (c == 0) {
		memset(dst, 0, len);
		return;
	}
	if (c == 1) {
		memcpy(dst, src, len);
		return;
	}
	memset(dst, 0, len);
	mul_add_impl(dst, src, len, c);
}

const char *iso_gf256_impl(void)
{
	return mul_add_name;
}

static bool have_avx512bw(void)
{
	return iso_cpu_has(ISO_CPU_AVX512F) && iso_cpu_has(ISO_CPU_AVX512BW);
}

void iso_gf256_use(const iso_gf256_kernel *k)
{
	mul_add_impl = k ? k->fn : iso_gf256_mul_add_scalar;
	mul_add_name = k ? k->name : "scalar";
	/* NULL for no kernel (the scalar one, as above) and for a kernel without a dot kernel: the
	 * sum is then composed from the mul_add kernel just set. */
	dot_impl = k ? k->dot : NULL;
}

size_t iso_gf256_kernels(iso_gf256_kernel *out)
{
	const iso_gf256_kernel all[] = {
		{ "scalar", iso_gf256_mul_add_scalar, true, 1, iso_gf256_dot_scalar },
		{ "ssse3", iso_gf256_mul_add_ssse3, iso_cpu_has(ISO_CPU_SSSE3), 16,
		  iso_gf256_dot_ssse3 },
		{ "avx2", iso_gf256_mul_add_avx2, iso_cpu_has(ISO_CPU_AVX2), 32,
		  iso_gf256_dot_avx2 },
		{ "avx512", iso_gf256_mul_add_avx512, have_avx512bw(), 64, iso_gf256_dot_avx512 },
		{ "gfni256", iso_gf256_mul_add_gfni256,
		  iso_cpu_has(ISO_CPU_GFNI) && iso_cpu_has(ISO_CPU_AVX2), 32,
		  iso_gf256_dot_gfni256 },
		{ "gfni512", iso_gf256_mul_add_gfni512,
		  iso_cpu_has(ISO_CPU_GFNI) && have_avx512bw(), 64, iso_gf256_dot_gfni512 },
	};
	ISO_STATIC_ASSERT(ISO_ARRAY_LEN(all) == ISO_GF256_MAX_KERNELS,
			  "kernel table and ISO_GF256_MAX_KERNELS disagree");
	memcpy(out, all, sizeof all);
	return ISO_ARRAY_LEN(all);
}

/* Order of preference: GFNI beats the nibble-table kernels outright (one instruction per 8-bit
 * multiply, no table loads), and 512-bit beats 256-bit except on the Skylake-SP generation, where
 * iso_cpu_prefer_512() says otherwise because sustained zmm work costs frequency across the whole
 * socket -- there the 512-bit kernels are not on the ladder at all. bench_gf256 measures every
 * available kernel, so this ladder is checkable rather than folklore. */
size_t iso_gf256_ladder(iso_gf256_kernel *out)
{
	bool avx512bw = have_avx512bw();
	bool wide = avx512bw && iso_cpu_prefer_512();
	bool gfni = iso_cpu_has(ISO_CPU_GFNI);
	const iso_gf256_kernel all[] = {
		{ "gfni512", iso_gf256_mul_add_gfni512, gfni && wide, 64, iso_gf256_dot_gfni512 },
		{ "gfni256", iso_gf256_mul_add_gfni256, gfni && iso_cpu_has(ISO_CPU_AVX2), 32,
		  iso_gf256_dot_gfni256 },
		{ "avx512", iso_gf256_mul_add_avx512, wide, 64, iso_gf256_dot_avx512 },
		{ "avx2", iso_gf256_mul_add_avx2, iso_cpu_has(ISO_CPU_AVX2), 32, iso_gf256_dot_avx2 },
		{ "ssse3", iso_gf256_mul_add_ssse3, iso_cpu_has(ISO_CPU_SSSE3), 16,
		  iso_gf256_dot_ssse3 },
		{ "scalar", iso_gf256_mul_add_scalar, true, 1, iso_gf256_dot_scalar },
	};
	ISO_STATIC_ASSERT(ISO_ARRAY_LEN(all) == ISO_GF256_MAX_KERNELS,
			  "the ladder and ISO_GF256_MAX_KERNELS disagree");
	size_t n = 0;
	for (size_t i = 0; i < ISO_ARRAY_LEN(all); i++)
		if (all[i].available)
			out[n++] = all[i];
	return n;
}

/* ------------------------------------------------------------------ the check (section 26k)
 *
 * What it costs is the budget: under 20 ms of a node's start, measured at the step, which is why
 * it stops at the first kernel that agrees and why the long buffer is not run under every
 * coefficient -- a coefficient's own error (a table row, a GFNI matrix) shows at 257 bytes, which
 * is four rounds of the widest kernel, and the long buffer is for the loop run long. */

#define GF_CHECK_SHORT 257
#define GF_CHECK_LONG (64u * 1024u + 3u)
#define GF_CHECK_GUARD 64u
#define GF_CHECK_PAD (GF_CHECK_GUARD + 64u)

static const uint8_t gf_long_coefs[] = { 0x02, 0x03, 0x1d, 0x53, 0x8e, 0xca, 0xfe, 0xff };

static void gf_fill(uint8_t *p, size_t n, uint64_t seed)
{
	for (size_t i = 0; i < n; i++) {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		p[i] = (uint8_t)(seed >> 24);
	}
}

/* `k` on `len` bytes under `c`, against `want` (what the reference made of the same `dst0`), and
 * against `dst0` itself for the guard past the end. */
static bool gf_check_one(const iso_gf256_kernel *k, const iso_gf256_kernel *ref, const uint8_t *src,
			 const uint8_t *dst0, const uint8_t *want, uint8_t *work, size_t len,
			 uint8_t c, iso_kcheck *out)
{
	memcpy(work, dst0, len + GF_CHECK_GUARD);
	k->fn(work, src, len, c);
	if (memcmp(work, want, len) == 0 &&
	    memcmp(work + len, dst0 + len, GF_CHECK_GUARD) == 0)
		return true;
	size_t at = 0;
	while (at < len && work[at] == want[at])
		at++;
	if (!out->why[0]) {
		if (at < len)
			snprintf(out->why, sizeof out->why,
				 "the %s GF(256) kernel disagrees with the %s one: coefficient 0x%02x "
				 "over %zu bytes, byte %zu is 0x%02x and not 0x%02x",
				 k->name, ref->name, c, len, at, work[at], want[at]);
		else
			snprintf(out->why, sizeof out->why,
				 "the %s GF(256) kernel writes past the end: coefficient 0x%02x over "
				 "%zu bytes",
				 k->name, c, len);
	}
	return false;
}

static bool gf_check_kernel(const iso_gf256_kernel *k, const iso_gf256_kernel *ref,
			    uint8_t *const buf[4], iso_kcheck *out)
{
	uint8_t *src = buf[0], *dst0 = buf[1], *want = buf[2], *work = buf[3];
	for (unsigned c = 0; c < 256; c++) {
		/* Aligned for the even coefficients, and not for the odd ones -- by different amounts,
		 * so a kernel that assumes the two pointers move together is caught too. */
		size_t so = c & 1 ? 3 : 0, doff = c & 1 ? 1 : 0;
		memcpy(want + doff, dst0 + doff, GF_CHECK_SHORT + GF_CHECK_GUARD);
		ref->fn(want + doff, src + so, GF_CHECK_SHORT, (uint8_t)c);
		for (size_t len = 0; len <= GF_CHECK_SHORT; len++)
			if (!gf_check_one(k, ref, src + so, dst0 + doff, want + doff, work + doff, len,
					  (uint8_t)c, out))
				return false;
	}
	for (size_t i = 0; i < ISO_ARRAY_LEN(gf_long_coefs); i++) {
		uint8_t c = gf_long_coefs[i];
		size_t so = 11, doff = 5;
		memcpy(want + doff, dst0 + doff, GF_CHECK_LONG + GF_CHECK_GUARD);
		ref->fn(want + doff, src + so, GF_CHECK_LONG, c);
		if (!gf_check_one(k, ref, src + so, dst0 + doff, want + doff, work + doff,
				  GF_CHECK_LONG, c, out))
			return false;
	}
	return true;
}

/* ------------------------------------------------------------------ the check of a dot kernel
 *
 * The same idea as above, for the sums: the dot kernel against the reference's, over the shapes
 * a bug of a dot kernel hides in. A group is four outputs, so m runs to five (a second group, and
 * one of a single output); the sources are read one at a time, so k takes one, a few, and one
 * past the 16 the library has; the vector steps are 16 to 128 bytes, so the lengths are every one
 * up to 40 and then the steps' edges, and the buffers start at unaligned addresses a different
 * amount apart, with `off` moving the range further in. A long range runs the loops long, and
 * four calls with every coefficient in a matrix (64 of them a call) cover all 256 in the dot path,
 * where a table index could be wrong for a few of them only. The bytes before the range and a
 * guard after it are filled and must come back as they were. */

#define GF_DOT_MAXM 5
#define GF_DOT_MAXK 17
#define GF_DOT_BUF 5200u
#define GF_DOT_LONG 5003u

typedef struct {
	uint8_t *src[GF_DOT_MAXK], *dst[GF_DOT_MAXM], *want[GF_DOT_MAXM];
} gf_dot_bufs;

static bool gf_dot_one(const iso_gf256_kernel *k, iso_gf256_dot_fn ref, const iso_gf256_kernel *refk,
		       const gf_dot_bufs *b, const uint8_t *dst0, unsigned m, unsigned kk,
		       const uint8_t *coef, size_t off, size_t len, iso_kcheck *out)
{
	const uint8_t *src[GF_DOT_MAXK];
	uint8_t *got[GF_DOT_MAXM], *want[GF_DOT_MAXM];
	for (unsigned j = 0; j < kk; j++)
		src[j] = b->src[j] + ((j * 3 + 1) & 7);
	size_t span = off + len + GF_CHECK_GUARD;
	for (unsigned i = 0; i < m; i++) {
		size_t d = (i * 5 + 2) & 7;
		got[i] = b->dst[i] + d;
		want[i] = b->want[i] + d;
		memcpy(got[i], dst0 + i * GF_DOT_BUF, span);
		memcpy(want[i], dst0 + i * GF_DOT_BUF, span);
	}
	ref(want, m, src, kk, coef, off, len);
	k->dot(got, m, src, kk, coef, off, len);
	for (unsigned i = 0; i < m; i++) {
		if (memcmp(got[i], want[i], span) == 0)
			continue;
		size_t at = 0;
		while (at < span && got[i][at] == want[i][at])
			at++;
		if (!out->why[0]) {
			if (at >= off && at < off + len)
				snprintf(out->why, sizeof out->why,
					 "the %s GF(256) dot kernel disagrees with the %s one: %u outputs "
					 "of %u sources over %zu bytes from %zu, output %u byte %zu is "
					 "0x%02x and not 0x%02x",
					 k->name, refk->name, m, kk, len, off, i, at - off, got[i][at],
					 want[i][at]);
			else
				snprintf(out->why, sizeof out->why,
					 "the %s GF(256) dot kernel writes outside its range: %u outputs "
					 "of %u sources over %zu bytes from %zu, output %u byte %zu",
					 k->name, m, kk, len, off, i, at);
		}
		return false;
	}
	return true;
}

static bool gf_check_dot(const iso_gf256_kernel *k, const iso_gf256_kernel *refk,
			 const gf_dot_bufs *b, const uint8_t *dst0, iso_kcheck *out)
{
	iso_gf256_dot_fn ref = refk->dot ? refk->dot : iso_gf256_dot_scalar;
	static const unsigned ks[] = { 1, 2, 3, 5, 16, 17 };
	static const size_t big[] = { 47, 48, 49, 63, 64, 65, 95, 96, 97, 127, 128, 129, 130,
				      191, 192, 193, 255, 256, 257 };
	static const uint8_t edge[] = { 0x00, 0x01, 0x80, 0xff, 0x02, 0x8e, 0x1d };
	uint8_t coef[GF_DOT_MAXM * GF_DOT_MAXK];
	uint64_t seed = 0x2545f4914f6cdd1dull;
	unsigned round = 0;

	for (unsigned m = 1; m <= GF_DOT_MAXM; m++) {
		for (size_t ki = 0; ki < ISO_ARRAY_LEN(ks); ki++) {
			unsigned kk = ks[ki];
			gf_fill(coef, (size_t)m * kk, seed + m * 977 + kk);
			/* Edge multipliers where a short-circuit would sit: a whole row of ones, and
			 * 0, 1, 0x80 and 0xff spread over the rest. */
			for (unsigned j = 0; j < kk; j++)
				coef[j] = 1;
			for (size_t n = 1; n < (size_t)m * kk; n += 3)
				coef[n] = edge[(n + m) % ISO_ARRAY_LEN(edge)];
			for (size_t li = 0; li < 41 + ISO_ARRAY_LEN(big); li++) {
				size_t len = li < 41 ? li : big[li - 41];
				size_t off = (round++ % 5) * 3;
				if (!gf_dot_one(k, ref, refk, b, dst0, m, kk, coef, off, len, out))
					return false;
			}
		}
	}
	/* Every multiplier in the dot path: 4 outputs of 16 sources are 64 of them a call. */
	for (unsigned t = 0; t < 4; t++) {
		for (unsigned n = 0; n < 64; n++)
			coef[n] = (uint8_t)(t * 64 + n);
		if (!gf_dot_one(k, ref, refk, b, dst0, 4, 16, coef, 1, 129, out))
			return false;
	}
	/* A long range, through two groups of outputs. */
	gf_fill(coef, sizeof coef, seed ^ 0x5bd1e995);
	return gf_dot_one(k, ref, refk, b, dst0, GF_DOT_MAXM, 16, coef, 7, GF_DOT_LONG, out);
}

/* The reference, held to the field itself: x^8 = x^4 + x^3 + x^2 + 1 under 0x11d, so twice 0x80
 * is 0x1d, twice 0xff is 0xe3, and 0x8e is the inverse of 2 -- products that hold for any correct
 * implementation of this field and are not read from the tables the kernels use. */
static bool gf_known_answers(const iso_gf256_kernel *ref, iso_kcheck *out)
{
	static const uint8_t src[5] = { 0x80, 0x40, 0xff, 0x8e, 0x01 };
	static const struct {
		uint8_t c, want[5];
	} cases[] = { { 0x02, { 0x1d, 0x80, 0xe3, 0x01, 0x02 } },
		      { 0x8e, { 0x40, 0x20, 0xf1, 0x47, 0x8e } } };
	for (size_t i = 0; i < ISO_ARRAY_LEN(cases); i++) {
		uint8_t dst[5] = { 0 };
		ref->fn(dst, src, sizeof dst, cases[i].c);
		if (memcmp(dst, cases[i].want, sizeof dst) != 0) {
			if (!out->why[0])
				snprintf(out->why, sizeof out->why,
					 "the %s GF(256) kernel is wrong about the field itself: "
					 "products by 0x%02x do not hold",
					 ref->name, cases[i].c);
			return false;
		}
	}
	/* The same products summed through the reference's dot kernel (two outputs of two sources, both
	 * sources the same bytes): row 0 is 2 and 0x8e, row 1 is 1 and 2, so the sums are the XORs of
	 * the cases above. Without it only the dot kernels' agreement with the reference stands, and a
	 * reference dot that is wrong the way every faster one is not would be the one chosen. */
	static const uint8_t drow[4] = { 0x02, 0x8e, 0x01, 0x02 };
	static const uint8_t dwant[2][5] = { { 0x5d, 0xa0, 0x12, 0x46, 0x8c },
					     { 0x9d, 0xc0, 0x1c, 0x8f, 0x03 } };
	uint8_t d0[5] = { 0 }, d1[5] = { 0 };
	uint8_t *dd[2] = { d0, d1 };
	const uint8_t *ds[2] = { src, src };
	(ref->dot ? ref->dot : iso_gf256_dot_scalar)(dd, 2, ds, 2, drow, 0, sizeof d0);
	if (memcmp(d0, dwant[0], sizeof d0) != 0 || memcmp(d1, dwant[1], sizeof d1) != 0) {
		if (!out->why[0])
			snprintf(out->why, sizeof out->why,
				 "the %s GF(256) dot kernel is wrong about the field itself", ref->name);
		return false;
	}
	return true;
}

void iso_gf256_check(const iso_gf256_kernel *k, size_t n, iso_kcheck *out)
{
	uint64_t t0 = iso_now_ns();
	memset(out, 0, sizeof *out);
	static const iso_gf256_kernel scalar = { "scalar", iso_gf256_mul_add_scalar, true, 1,
						 iso_gf256_dot_scalar };
	/* The last entry is the reference, as the ladder puts the scalar kernel last. */
	const iso_gf256_kernel *ref = n ? &k[n - 1] : &scalar;
	iso_strlcpy(out->family, "gf256", sizeof out->family);
	iso_strlcpy(out->reference, ref->name, sizeof out->reference);
	iso_strlcpy(out->preferred, n ? k[0].name : ref->name, sizeof out->preferred);
	out->chosen_index = n ? (unsigned)(n - 1) : 0;

	if (!gf_known_answers(ref, out)) {
		out->reference_failed = true;
		iso_strlcpy(out->failed[out->n_failed++], ref->name, sizeof out->failed[0]);
	} else {
		uint8_t *buf[4];
		for (unsigned i = 0; i < 4; i++)
			buf[i] = iso_xmalloc(GF_CHECK_LONG + GF_CHECK_PAD);
		gf_fill(buf[0], GF_CHECK_LONG + GF_CHECK_PAD, 0x9e3779b97f4a7c15ull);
		gf_fill(buf[1], GF_CHECK_LONG + GF_CHECK_PAD, 0xd1b54a32d192ed03ull);
		/* The dot kernels' buffers, made when the first kernel that has one is reached. */
		gf_dot_bufs db;
		uint8_t *dot_mem = NULL, *dst0 = NULL;
		for (size_t i = 0; i + 1 < n; i++) {
			out->n_checked++;
			bool ok = gf_check_kernel(&k[i], ref, buf, out);
			if (ok && k[i].dot) {
				if (!dot_mem) {
					size_t per = GF_DOT_BUF;
					dot_mem = iso_xmalloc(per * (GF_DOT_MAXK + 3 * GF_DOT_MAXM));
					for (unsigned j = 0; j < GF_DOT_MAXK; j++)
						db.src[j] = dot_mem + per * j;
					for (unsigned j = 0; j < GF_DOT_MAXM; j++) {
						db.dst[j] = dot_mem + per * (GF_DOT_MAXK + j);
						db.want[j] = dot_mem + per * (GF_DOT_MAXK + GF_DOT_MAXM + j);
					}
					dst0 = dot_mem + per * (GF_DOT_MAXK + 2 * GF_DOT_MAXM);
					gf_fill(dot_mem, per * GF_DOT_MAXK, 0x6a09e667f3bcc909ull);
					gf_fill(dst0, per * GF_DOT_MAXM, 0xbb67ae8584caa73bull);
				}
				ok = gf_check_dot(&k[i], ref, &db, dst0, out);
			}
			if (ok) {
				out->chosen_index = (unsigned)i;
				break;
			}
			if (out->n_failed < ISO_KCHECK_MAX)
				iso_strlcpy(out->failed[out->n_failed++], k[i].name,
					    sizeof out->failed[0]);
		}
		free(dot_mem);
		for (unsigned i = 0; i < 4; i++)
			free(buf[i]);
	}
	if (n && out->chosen_index == n - 1)
		out->n_checked++; /* the reference, held to the field's own products */
	iso_strlcpy(out->chosen, n ? k[out->chosen_index].name : ref->name, sizeof out->chosen);
	out->ns = iso_now_ns() - t0;
}

/*
 * iso_gf256_init
 *		Build the field tables (once) and dispatch to the top of the ladder, unchecked.
 *
 * The top of the ladder, unchecked: a process that never runs the check -- a test, a tool, a
 * benchmark -- runs what the CPU says it has, as it always did; `isod` checks before it serves.
 *
 * Idempotent, and not thread-safe: call it before any thread, and before the first product. The
 * tables are written the first time and only read after: a later call must not touch them (a
 * rebuild writes the same bytes back, so only a sentinel written through iso_gf256_row() shows
 * it; the unit test and tests/codec/gf256_noboot.c plant one). The ladder is picked again on
 * EVERY call, from what iso_cpu_has() says at that moment, so a caller whose features change
 * after the first call calls this again and gets the kernel for the new answer, within any cap.
 *
 * Who calls it: the constructor below in this repository. A program that copies this file into
 * a binary that must have no startup code compiles it with -DISO_GF256_NO_BOOT and calls this
 * itself. tsdb is that program, and three things make the public entry point necessary there:
 *
 *   - tsdb allows no startup code in the objects of its codecs, and checks the objects for an
 *     .init_array entry. The rule exists for the codecs it compiles once per CPU level, where a
 *     constructor of a v3 or v4 copy would run on every CPU before the level is chosen (SIGILL
 *     on one without AVX); this file is not built per level there, but the rule has no exception.
 *   - Which kernels may run follows the level tsdb picks at start, from the CPU and an override,
 *     after this file's constructor would have run, and can be picked again later. The program
 *     must say when the kernel is chosen, and choose again.
 *   - The table builder is static, so once the constructor is gone nothing else can build the
 *     tables.
 *
 * The copy is kept byte for byte what this repository has, so tsdb cannot patch the constructor
 * out: the flag is how the file says so itself.
 *
 * NOTE: the constructor is in this file and not in a file of its own on purpose. A static
 * archive pulls a member only to resolve an undefined symbol, so a boot file nothing references
 * is never linked and the constructor would silently vanish (tables empty, every product 0).
 */
void iso_gf256_init(void)
{
	static bool tables_done;
	if (!tables_done) {
		gf256_tables_init();
		tables_done = true;
	}
	iso_gf256_kernel ladder[ISO_GF256_MAX_KERNELS];
	iso_gf256_use(iso_gf256_ladder(ladder) ? &ladder[0] : NULL);
}

#ifndef ISO_GF256_NO_BOOT
/* Priority 150: after cpu.c (101), which iso_gf256_init() asks what the CPU may use, and before
 * anything that encodes at start-up. Compiled out under -DISO_GF256_NO_BOOT, so that the object
 * has no .init_array entry at all (tests/codec/gf256_noboot_obj.sh checks the object). */
__attribute__((constructor(150))) static void gf256_boot(void)
{
	iso_gf256_init();
}
#endif
