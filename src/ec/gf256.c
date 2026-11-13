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
 */

static uint8_t gf_exp[512];
static uint8_t gf_log[256];
static uint8_t gf_mul_table[256][256];
static uint8_t gf_shuf_lo[256][16];
static uint8_t gf_shuf_hi[256][16];

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

/* ------------------------------------------------------------------ GFNI matrix
 *
 * VGF2P8AFFINEQB computes, per byte of the data operand, output bit k as
 *
 *     parity( data_byte AND matrix.byte[7 - k] )
 *
 * so the matrix row selecting which input bits contribute to output bit k lives in byte (7 - k).
 * Multiplication by c is GF(2)-linear, so row k has bit j set exactly when bit k of c * 2^j is
 * set -- the multiplier's action on each basis element, transposed.
 */
uint64_t iso_gf256_gfni_matrix(uint8_t c)
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

/* ------------------------------------------------------------------ dispatch */

static void (*mul_add_impl)(uint8_t *, const uint8_t *, size_t, uint8_t) =
	iso_gf256_mul_add_scalar;
static const char *mul_add_name = "scalar";

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
}

size_t iso_gf256_kernels(iso_gf256_kernel *out)
{
	const iso_gf256_kernel all[] = {
		{ "scalar", iso_gf256_mul_add_scalar, true, 1 },
		{ "ssse3", iso_gf256_mul_add_ssse3, iso_cpu_has(ISO_CPU_SSSE3), 16 },
		{ "avx2", iso_gf256_mul_add_avx2, iso_cpu_has(ISO_CPU_AVX2), 32 },
		{ "avx512", iso_gf256_mul_add_avx512, have_avx512bw(), 64 },
		{ "gfni256", iso_gf256_mul_add_gfni256,
		  iso_cpu_has(ISO_CPU_GFNI) && iso_cpu_has(ISO_CPU_AVX2), 32 },
		{ "gfni512", iso_gf256_mul_add_gfni512,
		  iso_cpu_has(ISO_CPU_GFNI) && have_avx512bw(), 64 },
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
		{ "gfni512", iso_gf256_mul_add_gfni512, gfni && wide, 64 },
		{ "gfni256", iso_gf256_mul_add_gfni256, gfni && iso_cpu_has(ISO_CPU_AVX2), 32 },
		{ "avx512", iso_gf256_mul_add_avx512, wide, 64 },
		{ "avx2", iso_gf256_mul_add_avx2, iso_cpu_has(ISO_CPU_AVX2), 32 },
		{ "ssse3", iso_gf256_mul_add_ssse3, iso_cpu_has(ISO_CPU_SSSE3), 16 },
		{ "scalar", iso_gf256_mul_add_scalar, true, 1 },
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
	return true;
}

void iso_gf256_check(const iso_gf256_kernel *k, size_t n, iso_kcheck *out)
{
	uint64_t t0 = iso_now_ns();
	memset(out, 0, sizeof *out);
	static const iso_gf256_kernel scalar = { "scalar", iso_gf256_mul_add_scalar, true, 1 };
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
		for (size_t i = 0; i + 1 < n; i++) {
			out->n_checked++;
			if (gf_check_kernel(&k[i], ref, buf, out)) {
				out->chosen_index = (unsigned)i;
				break;
			}
			if (out->n_failed < ISO_KCHECK_MAX)
				iso_strlcpy(out->failed[out->n_failed++], k[i].name,
					    sizeof out->failed[0]);
		}
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
 * tables are written the first time and only read after. The ladder is picked again on EVERY call,
 * from what iso_cpu_has() says at that moment, so a caller that narrows the permitted features
 * later (tsdb caps them by the CPU level it chose, which it knows only after the CPU is
 * probed) calls this again and gets a kernel within the cap, never one the cap forbids.
 *
 * Who calls it: the constructor below in this repository. A program that copies this file into a
 * binary that may have no startup code compiles it with -DISO_GF256_NO_BOOT and calls this itself.
 * tsdb is that program: it compiles its copy once per CPU level (x86-64-v2, v3, v4 in one binary)
 * and a constructor in a v3 or v4 copy would run on every CPU before the dispatch has chosen a
 * level, which is SIGILL on a CPU without AVX. It also keeps the copy byte for byte, so it cannot
 * patch the constructor out: the flag is how the file says so itself.
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
