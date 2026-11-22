/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The logical digest of a settle (design-cluster-data.md E26, the R8 plan's D7): rows per series and an xxh64 over the
 * rows of a set of parts and objects visible under the retention limits and the MK_NOW clock as of Z, in (series, ts)
 * order, plus a canonical form of the tombstones that reach below the set. The leader appends it as MK_DIGEST; every
 * log replica computes its own retired set's and compares (ecdigest_apply): a match lets eckeep release the retired
 * parts at settle_keep_s, a mismatch keeps them and the volume goes DIVERGED (src/ecdiverge.h).
 *
 * STUB (r8a1-mk): filled by r8j-digest. Until then a MK_DIGEST is acknowledged and compared with nothing, and the
 * digest answers TSDB_ERR_INVALID ("not built yet").
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECDIGEST_H
#define TSDB_ECDIGEST_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "marker.h"

struct eng;
struct part;

typedef struct ec_ldigest {
    uint64_t rows, series, xxh64;
} ec_ldigest;

/* The logical digest of set[0 .. n) (parts and .ec parts of key `key`) at the retention clock now_ms. */
int ec_logical_digest(struct eng *db, struct part **set, size_t n, int64_t key, int64_t now_ms, ec_ldigest *out);
/* A committed MK_DIGEST at log position pos (m decoded): 0 once its comparison is queued (or done). */
int ecdigest_apply(struct eng *db, uint64_t pos, const mk *m);

#endif
