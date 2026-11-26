/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster EC garbage collection via committed MK_GC (design-cluster-data.md E14, §5.7, the R8 plan's D13):
 *
 * - Every member applies a committed MK_GC (ecgcmk_apply): for MK_GC_RETIRED and MK_GC_ORPHAN, any .ec of those oids
 *   still listed is retired first (a replica whose TTL is behind must never promote listing an .ec whose chunks are
 *   going), the oids are appended to the durable checksummed pending-delete file GCMK (like CLASSES/PPOS), and every
 *   retired_ name of those oids is unlinked with a directory fsync. MK_GC_DONE removes the oids from GCMK (compacting
 *   when empty or large).
 * - The leader (ecgcmk_work, driven by the tier's GC worker thread) processes pending oids in GCMK (including those
 *   inherited across a failover by a promoted leader) by sending CHUNK_DEL to every stripe position with its own lead
 *   epoch and the MK_GC marker's position, then appends MK_GC_DONE once every position answered (ABSENT counts). A
 *   FENCED answer stops GC on this deposed leader; any other error keeps the oid queued.
 * - The leader appends MK_GC_RETIRED for retired .ec objects past obsolete_part_lifetime_s and unheld (continues even
 *   when gc_suspended is set), and MK_GC_ORPHAN for orphans satisfying E14's four conditions (not named by any listed
 *   or retired .ec, not named by any record in its WAL committed or not, written in an earlier epoch and older than
 *   the orphan grace, and gc_suspended false).
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECGCMK_H
#define TSDB_ECGCMK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "marker.h"

struct eng;
struct ecgcmk; /* the pending-delete list and leader GC state (r8k), eng.gcmk */

/* A committed MK_GC at log position pos (m decoded): 0 when applied. */
int ecgcmk_apply(struct eng *db, uint64_t pos, const mk *m);
int ecgcmk_open(struct eng *db);
void ecgcmk_stop(struct eng *db);
void ecgcmk_close(struct eng *db);

/* One leader GC round for a cluster EC engine (called from ecgc_tier_work). */
void ecgcmk_work(struct eng *db);
/* Queue an explicitly aborted-settle object for MK_GC_ORPHAN (never direct delete). */
int ecgcmk_queue_orphan(struct eng *db, const uint8_t oid[16]);

/* Test helpers & introspection (tests/test_ecgc.c, TSDB_ECGC_TEST). */
void ecgcmk_test_opt(const char *tok);
size_t ecgcmk_pending_count(const struct eng *db);
bool ecgcmk_pending_has(const struct eng *db, const uint8_t oid[16]);
bool ecgcmk_is_fenced(const struct eng *db);

#endif
