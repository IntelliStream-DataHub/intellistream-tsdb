/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* Systematic Reed-Solomon erasure coding over GF(2^8).
 *
 * k data shards produce m parity shards; any k of the k+m shards reconstruct everything. The
 * pool defaults are k <= 16 and m = 4 (section 1), so a segment survives any four simultaneous
 * device losses.
 *
 * "Systematic" means the first k output shards *are* the input data, unchanged. That is what
 * makes a healthy read free: the gateway fetches k data shards and concatenates them, touching
 * the codec only when a shard is missing or fails its hash.
 *
 * The generator is a Cauchy matrix stacked under a k x k identity. A Cauchy matrix has the
 * property that *every* square submatrix is invertible, which is exactly the MDS guarantee the
 * store depends on: any k surviving shards suffice, with no unlucky combinations. The unit tests
 * prove it by exhaustive decode over every erasure set of size 0..4 for every k in 1..16 rather
 * than taking it on faith.
 */
#ifndef ISO_CODEC_RS_H
#define ISO_CODEC_RS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "common/util.h"

#define ISO_RS_MAX_K 16
#define ISO_RS_MAX_M 4
#define ISO_RS_MAX_SHARDS (ISO_RS_MAX_K + ISO_RS_MAX_M)

/* Encode and decode work on a shard in column tiles of this many bytes (the last one shorter), each
 * tile through every output before the next: see the comment above rs_sums() in rs.c for why this
 * size. A multiple of 128, the widest step of any GF(256) kernel. Public so the tests can put shard
 * lengths on both sides of a tile edge; a build may give another with -DISO_RS_TILE=. */
#ifndef ISO_RS_TILE
#define ISO_RS_TILE (8u * 1024u)
#endif
ISO_STATIC_ASSERT(ISO_RS_TILE % 128 == 0, "a tile is a whole number of the widest kernel's steps");

typedef struct iso_rs iso_rs;

/* Build a codec for k data and m parity shards. Returns NULL when k or m is out of range.
 *
 * The result is immutable and therefore safe to share across threads: encode and decode keep all
 * their working state on the stack.
 */
iso_rs *iso_rs_new(unsigned k, unsigned m);
void iso_rs_free(iso_rs *rs);

unsigned iso_rs_k(const iso_rs *rs);
unsigned iso_rs_m(const iso_rs *rs);

/* Encode: `shards` holds k+m pointers of `shard_len` bytes each. The first k are read, the last m
 * are written. Shard buffers must not overlap. */
void iso_rs_encode(const iso_rs *rs, uint8_t *const *shards, size_t shard_len);

/* Reconstruct every shard marked absent in `present` (an array of k+m flags).
 *
 * Buffers for the absent shards must already be allocated; their contents are overwritten.
 * Returns ISO_OK, or ISO_ERR_RANGE when fewer than k shards are present (unrecoverable).
 *
 * Cost note: this inverts a k x k matrix per call, roughly k^3 table lookups -- around 4k
 * operations at k = 16, negligible beside the k * shard_len bytes it then has to move. Callers
 * decoding many segments with the *same* erasure pattern can hoist that with iso_rs_plan().
 */
iso_err iso_rs_decode(const iso_rs *rs, uint8_t *const *shards, const bool *present,
		      size_t shard_len);

/* Reconstruct only the data shards, leaving missing parity untouched. This is the read path: a
 * GET that hits a bad shard needs the data back, not a repaired parity shard. */
iso_err iso_rs_decode_data(const iso_rs *rs, uint8_t *const *shards, const bool *present,
			   size_t shard_len);

/* ------------------------------------------------------------------ plans
 *
 * A decode plan is the inverted matrix for one erasure pattern. Recovery jobs rebuild thousands
 * of segments that lost the same device, so hoisting the inversion out of the loop turns decode
 * into pure data movement.
 */
typedef struct iso_rs_plan iso_rs_plan;

/* Build a plan for the given `present` pattern. Returns NULL when fewer than k shards are
 * present. The plan borrows nothing from `present` and is immutable once built. */
iso_rs_plan *iso_rs_plan_new(const iso_rs *rs, const bool *present, bool data_only);
void iso_rs_plan_free(iso_rs_plan *plan);

/* Apply a plan. `shards` must match the `present` pattern the plan was built from. */
void iso_rs_plan_apply(const iso_rs_plan *plan, uint8_t *const *shards, size_t shard_len);

/* ------------------------------------------------------------------ introspection
 *
 * The generator matrix row for shard `i` (k bytes), for tests and for the on-disk format
 * documentation. Rows 0..k-1 are the identity.
 */
const uint8_t *iso_rs_matrix_row(const iso_rs *rs, unsigned shard);

/* Invert a k x k matrix in GF(2^8) in place. Exposed for tests; returns false if singular. */
bool iso_rs_invert(uint8_t *matrix, unsigned n);

#endif /* ISO_CODEC_RS_H */
