/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
#include "codec/rs.h"

#include <stdlib.h>
#include <string.h>

#include "codec/gf256.h"
#include "common/log.h"

struct iso_rs {
	unsigned k;
	unsigned m;
	/* Row-major (k+m) x k generator. Rows 0..k-1 are the identity, rows k..k+m-1 are Cauchy. */
	uint8_t gen[ISO_RS_MAX_SHARDS * ISO_RS_MAX_K];
};

struct iso_rs_plan {
	unsigned k;
	unsigned nsrc;                       /* always k */
	unsigned src[ISO_RS_MAX_K];          /* which shards to read */
	unsigned nout;
	unsigned out[ISO_RS_MAX_SHARDS];     /* which shards to write */
	uint8_t coef[ISO_RS_MAX_SHARDS * ISO_RS_MAX_K]; /* nout x k, row-major */
};

/* ------------------------------------------------------------------ matrix helpers */

bool iso_rs_invert(uint8_t *a, unsigned n)
{
	/* Gauss-Jordan on [a | I]. n <= 16 so the augmented matrix fits comfortably on the stack
	 * and the O(n^3) cost is a few thousand table lookups. */
	uint8_t inv[ISO_RS_MAX_K * ISO_RS_MAX_K];
	memset(inv, 0, (size_t)n * n);
	for (unsigned i = 0; i < n; i++)
		inv[i * n + i] = 1;

	for (unsigned col = 0; col < n; col++) {
		/* Find a pivot. */
		unsigned pivot = col;
		while (pivot < n && a[pivot * n + col] == 0)
			pivot++;
		if (pivot == n)
			return false; /* singular */

		if (pivot != col) {
			for (unsigned j = 0; j < n; j++) {
				uint8_t t = a[col * n + j];
				a[col * n + j] = a[pivot * n + j];
				a[pivot * n + j] = t;
				t = inv[col * n + j];
				inv[col * n + j] = inv[pivot * n + j];
				inv[pivot * n + j] = t;
			}
		}

		/* Scale the pivot row so the pivot becomes 1. */
		uint8_t p = a[col * n + col];
		if (p != 1) {
			uint8_t ip = iso_gf256_inv(p);
			for (unsigned j = 0; j < n; j++) {
				a[col * n + j] = iso_gf256_mul(a[col * n + j], ip);
				inv[col * n + j] = iso_gf256_mul(inv[col * n + j], ip);
			}
		}

		/* Eliminate the column from every other row. */
		for (unsigned row = 0; row < n; row++) {
			if (row == col)
				continue;
			uint8_t f = a[row * n + col];
			if (f == 0)
				continue;
			for (unsigned j = 0; j < n; j++) {
				a[row * n + j] ^= iso_gf256_mul(a[col * n + j], f);
				inv[row * n + j] ^= iso_gf256_mul(inv[col * n + j], f);
			}
		}
	}

	memcpy(a, inv, (size_t)n * n);
	return true;
}

/* ------------------------------------------------------------------ construction */

iso_rs *iso_rs_new(unsigned k, unsigned m)
{
	if (k == 0 || k > ISO_RS_MAX_K || m == 0 || m > ISO_RS_MAX_M)
		return NULL;

	iso_rs *rs = calloc(1, sizeof *rs);
	if (!rs)
		return NULL;
	rs->k = k;
	rs->m = m;

	/* Identity on top: the code is systematic, so data shards pass through untouched. */
	for (unsigned i = 0; i < k; i++)
		rs->gen[i * k + i] = 1;

	/* Cauchy below: g[i][j] = 1 / (x_i ^ y_j) with the two index sets disjoint, which is what
	 * makes every element well defined. Taking y_j = j and x_i = k + i keeps them disjoint for
	 * any k + m <= 256, and gives every square submatrix an inverse -- the MDS property. */
	for (unsigned i = 0; i < m; i++) {
		uint8_t x = (uint8_t)(k + i);
		for (unsigned j = 0; j < k; j++) {
			uint8_t y = (uint8_t)j;
			rs->gen[(k + i) * k + j] = iso_gf256_inv((uint8_t)(x ^ y));
		}
	}

	return rs;
}

void iso_rs_free(iso_rs *rs)
{
	free(rs);
}

unsigned iso_rs_k(const iso_rs *rs)
{
	return rs->k;
}

unsigned iso_rs_m(const iso_rs *rs)
{
	return rs->m;
}

const uint8_t *iso_rs_matrix_row(const iso_rs *rs, unsigned shard)
{
	ISO_ASSERT(shard < rs->k + rs->m);
	return &rs->gen[shard * rs->k];
}

/* ------------------------------------------------------------------ encode */

void iso_rs_encode(const iso_rs *rs, uint8_t *const *shards, size_t shard_len)
{
	unsigned k = rs->k, m = rs->m;

	for (unsigned i = 0; i < m; i++) {
		const uint8_t *row = &rs->gen[(k + i) * k];
		uint8_t *out = shards[k + i];

		/* mul_set for the first term avoids a separate memset of the output shard. */
		iso_gf256_mul_set(out, shards[0], shard_len, row[0]);
		for (unsigned j = 1; j < k; j++)
			iso_gf256_mul_add(out, shards[j], shard_len, row[j]);
	}
}

/* ------------------------------------------------------------------ plans */

iso_rs_plan *iso_rs_plan_new(const iso_rs *rs, const bool *present, bool data_only)
{
	unsigned k = rs->k, m = rs->m, n = k + m;

	/* Pick the first k present shards as the source set. Preferring data shards would let a
	 * fully-present-data decode skip the matrix entirely, but the caller reaches decode only
	 * when something *is* missing, so the simple choice is the right one. */
	unsigned src[ISO_RS_MAX_K];
	unsigned nsrc = 0;
	for (unsigned i = 0; i < n && nsrc < k; i++) {
		if (present[i])
			src[nsrc++] = i;
	}
	if (nsrc < k)
		return NULL; /* fewer than k shards survive: unrecoverable */

	/* The k x k matrix mapping data -> the shards we actually hold. Inverting it maps what we
	 * hold back to the original data. */
	uint8_t sub[ISO_RS_MAX_K * ISO_RS_MAX_K];
	for (unsigned i = 0; i < k; i++)
		memcpy(&sub[i * k], &rs->gen[src[i] * k], k);

	if (!iso_rs_invert(sub, k)) {
		/* A Cauchy generator makes this unreachable; if it ever fires, the field tables or
		 * the matrix construction are wrong, and silently returning garbage would be far
		 * worse than a crash. */
		ISO_PANIC("RS submatrix singular for k=%u: generator is not MDS", k);
	}

	iso_rs_plan *plan = calloc(1, sizeof *plan);
	if (!plan)
		return NULL;
	plan->k = k;
	plan->nsrc = k;
	memcpy(plan->src, src, sizeof src);

	unsigned limit = data_only ? k : n;
	for (unsigned want = 0; want < limit; want++) {
		if (present[want])
			continue;

		uint8_t *coef = &plan->coef[plan->nout * k];
		if (want < k) {
			/* A data shard is row `want` of the inverse applied to the source shards. */
			memcpy(coef, &sub[want * k], k);
		} else {
			/* A parity shard is its generator row composed with the inverse, so it is
			 * rebuilt straight from the surviving shards without materialising the
			 * data first. */
			const uint8_t *grow = &rs->gen[want * k];
			for (unsigned c = 0; c < k; c++) {
				uint8_t acc = 0;
				for (unsigned t = 0; t < k; t++)
					acc ^= iso_gf256_mul(grow[t], sub[t * k + c]);
				coef[c] = acc;
			}
		}
		plan->out[plan->nout++] = want;
	}

	return plan;
}

void iso_rs_plan_free(iso_rs_plan *plan)
{
	free(plan);
}

void iso_rs_plan_apply(const iso_rs_plan *plan, uint8_t *const *shards, size_t shard_len)
{
	unsigned k = plan->k;

	for (unsigned o = 0; o < plan->nout; o++) {
		const uint8_t *coef = &plan->coef[o * k];
		uint8_t *out = shards[plan->out[o]];

		iso_gf256_mul_set(out, shards[plan->src[0]], shard_len, coef[0]);
		for (unsigned j = 1; j < k; j++)
			iso_gf256_mul_add(out, shards[plan->src[j]], shard_len, coef[j]);
	}
}

/* ------------------------------------------------------------------ decode */

static iso_err decode(const iso_rs *rs, uint8_t *const *shards, const bool *present,
		      size_t shard_len, bool data_only)
{
	unsigned n = rs->k + rs->m;
	unsigned missing = 0;
	for (unsigned i = 0; i < (data_only ? rs->k : n); i++)
		missing += !present[i];
	if (missing == 0)
		return ISO_OK;

	iso_rs_plan *plan = iso_rs_plan_new(rs, present, data_only);
	if (!plan)
		return ISO_ERR_RANGE;

	iso_rs_plan_apply(plan, shards, shard_len);
	iso_rs_plan_free(plan);
	return ISO_OK;
}

iso_err iso_rs_decode(const iso_rs *rs, uint8_t *const *shards, const bool *present,
		      size_t shard_len)
{
	return decode(rs, shards, present, shard_len, false);
}

iso_err iso_rs_decode_data(const iso_rs *rs, uint8_t *const *shards, const bool *present,
			   size_t shard_len)
{
	return decode(rs, shards, present, shard_len, true);
}
