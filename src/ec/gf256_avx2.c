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
