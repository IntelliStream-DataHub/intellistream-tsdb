/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The apply of a committed MK_COLD on every log replica, the leader included (design-cluster-data.md 2.3 step 6, the
 * R8 plan's D5-D6): one intent record under publish_lock that flushes the key, retires its parts with ppos <= Z as
 * settled_*, stops (DIVERGED) at a part above Z, lists the .ec files under fresh block numbers in the leader's order
 * and the Y files above them, and retires the objects the marker names; the swap's hooks (ecapply_swap_pre before the
 * intent's commit point, ecapply_swap_post after its renames) and the recovery's decision (ecapply_cold_decide). The
 * replica API r8a2-hooks adds (rs_cold_begin, rs_cold_done, replica_stage_path, replica_flush_through) is declared
 * here and built in src/replica.c.
 *
 * STUB (r8a1-mk): filled by r8h-apply. Until then a MK_COLD is acknowledged and changes nothing, as the reserved type
 * was skipped before; the decision answers TSDB_ERR_INVALID ("not built yet": no swap record can name one yet).
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECAPPLY_H
#define TSDB_ECAPPLY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "marker.h"

struct eng;
struct part;

/* A committed MK_COLD at log position pos (m decoded): 0 when applied. */
int ecapply_cold(struct eng *db, uint64_t pos, const mk *m);
/* Recovery of a swap record of key `key` naming oids[0 .. n) (n x MK_OID bytes): 1 done (every oid's .ec listed),
 * 0 not (none), TSDB_ERR_CORRUPT for some. */
int ecapply_cold_decide(struct eng *db, int64_t key, const uint8_t *oids, uint32_t n);
/* Around a settle's swap (src/ecswap.c, ec_swap_commit: the inputs `in`, their .ec parts `ec`, n of each). */
int ecapply_swap_pre(struct eng *db, struct part **in, struct part **ec, size_t n);
void ecapply_swap_post(struct eng *db, struct part **in, struct part **ec, size_t n);

#endif
