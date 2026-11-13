/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* AVX2 kernel for `dst ^= c * src`, in its own translation unit.
 *
 * The function carries a target attribute instead of the file being compiled with -mavx2, so the
 * whole build stays at -march=x86-64-v2 and one binary runs on the Ivy Bridge reference host
 * (which has no AVX2) as well as on newer machines. iso_gf256_mul_add() only routes here when
 * iso_cpu_has(ISO_CPU_AVX2) says so, and the unit tests assert this agrees with the scalar
 * kernel byte for byte on hosts that can run it.
 */
#include "codec/gf256.h"

#include <immintrin.h>

#include "common/util.h"

__attribute__((target("avx2"))) void iso_gf256_mul_add_avx2(uint8_t *dst, const uint8_t *src,
							   size_t len, uint8_t c)
{
	if (c == 0)
		return;
	if (c == 1) {
		iso_gf256_xor(dst, src, len);
		return;
	}

	/* PSHUFB works within 128-bit lanes, so the 16-entry nibble table is broadcast to both
	 * halves of the 256-bit register and each lane indexes its own copy. */
	const __m128i lo128 = _mm_loadu_si128((const __m128i *)iso_gf256_shuf_lo(c));
	const __m128i hi128 = _mm_loadu_si128((const __m128i *)iso_gf256_shuf_hi(c));
	const __m256i lo_tbl = _mm256_broadcastsi128_si256(lo128);
	const __m256i hi_tbl = _mm256_broadcastsi128_si256(hi128);
	const __m256i mask = _mm256_set1_epi8(0x0f);

	size_t i = 0;
	for (; i + 32 <= len; i += 32) {
		__m256i s = _mm256_loadu_si256((const __m256i *)(src + i));
		__m256i d = _mm256_loadu_si256((const __m256i *)(dst + i));

		__m256i lo = _mm256_and_si256(s, mask);
		__m256i hi = _mm256_and_si256(_mm256_srli_epi64(s, 4), mask);

		__m256i prod = _mm256_xor_si256(_mm256_shuffle_epi8(lo_tbl, lo),
						_mm256_shuffle_epi8(hi_tbl, hi));
		_mm256_storeu_si256((__m256i *)(dst + i), _mm256_xor_si256(d, prod));
	}

	/* AVX2 code must not leave the upper halves dirty before SSE code runs, or every
	 * subsequent SSE instruction pays a transition penalty. */
	_mm256_zeroupper();

	if (i < len)
		iso_gf256_mul_add_ssse3(dst + i, src + i, len - i, c);
}

/* ------------------------------------------------------------------ dot kernel
 *
 * The sum of up to four outputs over all sources, in registers; see the dot kernels' comment in
 * gf256.c for why and for the shape. One 32-byte vector position of a group of G outputs is G
 * accumulators; for each source the vector is loaded and split into nibbles once, and each output
 * takes its two table rows (broadcast from the 16-byte rows, which are consecutive in memory: row
 * c is at 16 * c) and two shuffles. That is 4 + 4 + 2 registers for G = 4 and nothing spills; a
 * second vector position per step would need 17. The table rows come from L1 (they are 8 KiB in
 * all, and a call touches the 2 * k * G of its coefficients); the shuffles, two per product, are
 * what limits the loop on Intel (port 5).
 */
static inline __attribute__((always_inline, target("avx2"))) void
dot_avx2_group(uint8_t *const *dst, const uint8_t *const *src, unsigned k, const uint8_t *coef,
	       size_t from, size_t end, const unsigned G)
{
	const uint8_t *lo_rows = iso_gf256_shuf_lo(0);
	const uint8_t *hi_rows = iso_gf256_shuf_hi(0);
	const __m256i mask = _mm256_set1_epi8(0x0f);
	for (size_t x = from; x < end; x += 32) {
		__m256i acc[4];
#pragma GCC unroll 4
		for (unsigned i = 0; i < G; i++)
			acc[i] = _mm256_setzero_si256();
		for (unsigned j = 0; j < k; j++) {
			__m256i s = _mm256_loadu_si256((const __m256i *)(src[j] + x));
			__m256i lo = _mm256_and_si256(s, mask);
			__m256i hi = _mm256_and_si256(_mm256_srli_epi64(s, 4), mask);
#pragma GCC unroll 4
			for (unsigned i = 0; i < G; i++) {
				size_t c16 = (size_t)coef[(size_t)i * k + j] << 4;
				__m256i tl = _mm256_broadcastsi128_si256(
					_mm_loadu_si128((const __m128i *)(lo_rows + c16)));
				__m256i th = _mm256_broadcastsi128_si256(
					_mm_loadu_si128((const __m128i *)(hi_rows + c16)));
				acc[i] = _mm256_xor_si256(
					acc[i], _mm256_xor_si256(_mm256_shuffle_epi8(tl, lo),
								 _mm256_shuffle_epi8(th, hi)));
			}
		}
#pragma GCC unroll 4
		for (unsigned i = 0; i < G; i++)
			_mm256_storeu_si256((__m256i *)(dst[i] + x), acc[i]);
	}
}

__attribute__((target("avx2"))) void
iso_gf256_dot_avx2(uint8_t *const *dst, unsigned m, const uint8_t *const *src, unsigned k,
		   const uint8_t *coef, size_t off, size_t len)
{
	size_t body = len & ~(size_t)31;
	for (unsigned g = 0; body && g < m; g += 4) {
		unsigned n = m - g < 4 ? m - g : 4;
		const uint8_t *rows = coef + (size_t)g * k;
		switch (n) {
		case 1:
			dot_avx2_group(dst + g, src, k, rows, off, off + body, 1);
			break;
		case 2:
			dot_avx2_group(dst + g, src, k, rows, off, off + body, 2);
			break;
		case 3:
			dot_avx2_group(dst + g, src, k, rows, off, off + body, 3);
			break;
		default:
			dot_avx2_group(dst + g, src, k, rows, off, off + body, 4);
			break;
		}
	}
	_mm256_zeroupper();
	if (body < len)
		iso_gf256_dot_ssse3(dst, m, src, k, coef, off + body, len - body);
}
