/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The apply of a committed MK_COLD on every log replica, the leader included (design-cluster-data.md 2.3 step 6, the
 * R8 plan's D5-D6): one intent record under publish_lock that flushes the key, retires its parts with ppos <= Z as
 * settled_*, stops (DIVERGED) at a part above Z, lists the .ec files under fresh block numbers in the leader's order
 * and the Y files above them, and retires the objects the marker names; the swap's hooks (ecapply_swap_pre before the
 * intent's commit point, ecapply_swap_post after its renames) and the recovery's decision (ecapply_cold_decide). The
 * replica API r8a2-hooks adds (rs_cold_begin, rs_cold_done, the cut generation and rp->mu, the stage lookups,
 * replica_flush_through) is declared here and built in src/replica.c.
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

/* ---- the replica API (r8a2-hooks, built in src/replica.c) ----
 * ecapply_cold runs on the replica's applier (rp->apply_mu held) with rp->mu held, as apply_commit does. Lock order:
 * apply_mu > stage_mu > rp->mu > flush_lock > publish_lock > db->lock. */

/* ecapply_cold's answer when a cut came while rp->mu was let go: nothing was done, the record is read again. */
#define RP_AGAIN 1

/* RSTATE's COLD in progress, as apply_commit's `doing`: begin before the swap intent's commit point (rp->mu held,
 * publish_lock may be), done after its renames (sets `done` to pos and clears it). A failed save fails the engine. A
 * replay finds a COLD whose pos is in `doing` and asks ecapply_cold_decide; one at or below `done` is skipped. */
int rs_cold_begin(struct eng *db, uint64_t pos, int64_t key);
int rs_cold_done(struct eng *db, uint64_t pos);
/* The cut generation (rp->mu held). replica_mu_drop lets go of rp->mu for slow work (a flush, reads of staged files,
 * the swap's I/O) and gives the generation; replica_mu_retake takes it again and says whether a cut came meanwhile:
 * true -> drop what was made and return RP_AGAIN. */
uint64_t replica_cut_gen(struct eng *db);
uint64_t replica_mu_drop(struct eng *db);
bool replica_mu_retake(struct eng *db, uint64_t gen);
/* While held, no replica_stage renames a staged file (take it with rp->mu let go: stage_mu sits above rp->mu). */
void replica_stage_hold(struct eng *db);
void replica_stage_release(struct eng *db);
/* The staged file of `name` (a file of a pending COMMIT or COLD, with the size and checksum the marker names) as a
 * name in the volume's directory (stage_<key>.part, < 40 bytes): 0, or TSDB_ERR_NOTFOUND when it is not staged (the
 * driver fetches it: replica_missing in src/replica.h names the first missing file of any pending marker). Takes
 * rp->mu: call it with rp->mu let go (replica_mu_drop), stage_mu held or not. */
int replica_stage_path(struct eng *db, const char *name, char *buf, size_t cap);
/* Flushes the sealed memtables (every one holds a segment below the marker being applied), never the active one: the
 * marker's flush_through F must lie below its segment (TSDB_ERR_CORRUPT otherwise, a line). Inside ecapply_cold only;
 * with rp->mu let go. */
int replica_flush_through(struct eng *db, uint64_t f);

#endif
