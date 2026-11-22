/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A leader's settle of a cluster ec volume (design-cluster-data.md 2.3 steps 3-5, the R8 plan's D4, D21): the gate
 * (jr_serial, like a large write), the cut at Z, the late parts Y, and the MK_COLD marker; and whether a cluster
 * volume may attach a tier at all (eccold_may_attach, which tier.c asks in place of its refusal of requires=cluster).
 * The leader API r8a2-hooks adds (jr_cold, jr_cold_precommit, jr_cold_commit) is declared here and built in
 * src/replica.c.
 *
 * STUB (r8a1-mk): filled by r8g-cut. Until then no cluster volume attaches a tier and nothing settles in a cluster:
 * db->cold stays NULL.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECCOLD_H
#define TSDB_ECCOLD_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct eng;
struct eccold; /* the leader's settle state (r8g), eng.cold */

/* Whether this engine (a cluster volume: requires=cluster or replica) may attach a tier. false: refused, as R7 does. */
bool eccold_may_attach(struct eng *db);
int eccold_open(struct eng *db);
void eccold_close(struct eng *db);

/* ---- the leader API (r8a2-hooks, built in src/replica.c) ---- */
struct mk;
/* A COLD marker m (its file list, objects, Y and retired names filled in; flush_through 0: the leader's flushed
 * segment, as a COMMIT's) carried to the replicas as a COMMIT is: the i-th file the marker names is shipped from
 * src[i] (a name in the volume's directory, size and checksum as the marker says) and linked as stage_<key>.part,
 * the directory synced, then the marker appended and waited for. 0 with *pos its position; TSDB_ERR_INDETERMINATE
 * when the leader was deposed meanwhile (a later open as a replica decides); any other error: refused (no marker), or
 * the engine failed. The caller holds jr_serial (eccold.c's gate), never publish_lock, and keeps it until
 * jr_cold_commit: RSTATE's `done` is a high-water mark, so a COMMIT or DROP whose `done` passed a COLD still in `doing`
 * would make a reopened replica skip that COLD. */
int jr_cold(struct eng *db, struct mk *m, const char *const *src, size_t n, uint64_t *pos);
/* The leader's RSTATE record of the swap that carries the COLD at pos out (key: its partition key): `doing` before the
 * swap intent's commit point, `done` after its renames, both with publish_lock held (ecapply_swap_pre/post). A
 * deposed leader reopened as a replica skips a COLD at or below `done` and decides one in `doing` by what is listed
 * (ecapply_cold_decide). A failed save fails the engine. Nothing on an engine that is not a leader. */
int jr_cold_precommit(struct eng *db, uint64_t pos, int64_t key);
int jr_cold_commit(struct eng *db, uint64_t pos);
/* The COLD in progress RSTATE names (pos 0: none), a leader's or a replica's, read at open (or with publish_lock). */
int jr_cold_pending(struct eng *db, uint64_t *pos, int64_t *key);

#endif
