/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* AVX-512 and GFNI kernels for `dst ^= c * src`.
 *
 * Each function carries its own target attribute rather than the file being compiled with
 * -mavx512f, so the build stays at -march=x86-64-v2 and a single release binary runs on the Ivy
 * Bridge reference host (which reaches none of these) as well as on Skylake-SP, Ice Lake, Zen 4
 * and Zen 5. iso_gf256_mul_add() only routes here when iso_cpu_has() confirms the feature.
 *
 * Two families live here:
 *
 *   avx512     the same nibble-split PSHUFB trick as the SSSE3 and AVX2 kernels, 64 bytes wide.
 *   gfni       VGF2P8AFFINEQB, which computes an 8x8 GF(2) matrix-vector product per byte. Since
 *              multiplying by a constant in GF(2^8) *is* such a product, one instruction replaces
 *              the whole mask/shift/two-shuffle/xor sequence -- no tables are loaded at all, and
 *              the multiplier reduces to a single broadcast 64-bit matrix.
 *
 * The matrix construction is in gf256.c and is verified exhaustively against the field tables by
 * a portable reference implementation, so on a host without GFNI the only untested part is the
 * instruction encoding itself.
 */
#include "codec/gf256.h"

#include <immintrin.h>

#include "common/util.h"

/* ------------------------------------------------------------------ avx512 (nibble tables) */

__attribute__((target("avx512f,avx512bw"))) void
iso_gf256_mul_add_avx512(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	if (c == 0)
		return;
	if (c == 1) {
		iso_gf256_xor(dst, src, len);
		return;
	}

	/* VPSHUFB indexes within each 128-bit lane, so the 16-entry table is broadcast to all four
	 * lanes and every lane looks up its own copy. */
	const __m128i lo128 = _mm_loadu_si128((const __m128i *)iso_gf256_shuf_lo(c));
	const __m128i hi128 = _mm_loadu_si128((const __m128i *)iso_gf256_shuf_hi(c));
	const __m512i lo_tbl = _mm512_broadcast_i32x4(lo128);
	const __m512i hi_tbl = _mm512_broadcast_i32x4(hi128);
	const __m512i mask = _mm512_set1_epi8(0x0f);

	size_t i = 0;
	for (; i + 64 <= len; i += 64) {
		__m512i s = _mm512_loadu_si512((const void *)(src + i));
		__m512i d = _mm512_loadu_si512((const void *)(dst + i));

		__m512i lo = _mm512_and_si512(s, mask);
		__m512i hi = _mm512_and_si512(_mm512_srli_epi64(s, 4), mask);

		__m512i prod = _mm512_xor_si512(_mm512_shuffle_epi8(lo_tbl, lo),
						_mm512_shuffle_epi8(hi_tbl, hi));
		_mm512_storeu_si512((void *)(dst + i), _mm512_xor_si512(d, prod));
	}

	if (i < len)
		iso_gf256_mul_add_avx2(dst + i, src + i, len - i, c);
}

/* ------------------------------------------------------------------ gfni, 256-bit (VEX)
 *
 * The VEX encoding needs only GFNI + AVX, so this is the kernel for Zen 4/Zen 5 and Ice Lake when
 * 512-bit work is not wanted, and the only GFNI kernel on a part with GFNI but no AVX-512.
 */

__attribute__((target("gfni,avx2"))) void
iso_gf256_mul_add_gfni256(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	if (c == 0)
		return;
	if (c == 1) {
		iso_gf256_xor(dst, src, len);
		return;
	}

	const __m256i mat = _mm256_set1_epi64x((long long)iso_gf256_gfni_matrix(c));

	size_t i = 0;
	for (; i + 32 <= len; i += 32) {
		__m256i s = _mm256_loadu_si256((const __m256i *)(src + i));
		__m256i d = _mm256_loadu_si256((const __m256i *)(dst + i));
		__m256i prod = _mm256_gf2p8affine_epi64_epi8(s, mat, 0);
		_mm256_storeu_si256((__m256i *)(dst + i), _mm256_xor_si256(d, prod));
	}

	_mm256_zeroupper();

	if (i < len)
		iso_gf256_mul_add_ssse3(dst + i, src + i, len - i, c);
}

/* ------------------------------------------------------------------ gfni, 512-bit (EVEX) */

__attribute__((target("gfni,avx512f,avx512bw"))) void
iso_gf256_mul_add_gfni512(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
	if (c == 0)
		return;
	if (c == 1) {
		iso_gf256_xor(dst, src, len);
		return;
	}

	const __m512i mat = _mm512_set1_epi64((long long)iso_gf256_gfni_matrix(c));

	size_t i = 0;
	for (; i + 64 <= len; i += 64) {
		__m512i s = _mm512_loadu_si512((const void *)(src + i));
		__m512i d = _mm512_loadu_si512((const void *)(dst + i));
		__m512i prod = _mm512_gf2p8affine_epi64_epi8(s, mat, 0);
		_mm512_storeu_si512((void *)(dst + i), _mm512_xor_si512(d, prod));
	}

	/* A 32-byte remainder still gets the wide instruction via the VEX kernel; anything smaller
	 * falls through to SSSE3 from there. */
	if (i < len)
		iso_gf256_mul_add_gfni256(dst + i, src + i, len - i, c);
}
