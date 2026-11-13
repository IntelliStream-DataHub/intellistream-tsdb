/* SPDX-License-Identifier: AGPL-3.0-only */
/* The engine as a replica of a cluster volume (docs/design-cluster.md 4-5, 8.2; build step 3 = design-ha-modes.md R2).
 *
 * What a driver (tsdbd's peer loop in R3, tests/test_replica.c here) does with one replica engine:
 *   eng_open_replica     open the volume directory; `floor` is the commit floor the driver saved (REPL file), applied at once
 *   replica_append       a leader's records, byte for byte, with the segment and offset they have there
 *   replica_sync         make what was appended durable; replica_durable_end says how far (the number the core's ACK reports)
 *   replica_apply        apply the records up to the commit point (rows visible, markers carried out)
 *   replica_floor_saved  the driver saved `floor` durably: segments at or below it may be unlinked once flushed
 *   replica_cut          drop the records above a position (a new leader's history)
 * Positions are log numbers (fno): a record covers fno .. fno + span - 1, and every position the functions take or
 * give is the last position of a record (a record end), 0 for the empty log.
 *
 * The log is the leader's WAL: a record keeps its bytes, its fno and its commit_ms, and so keeps the binding to its
 * segment and offset that the WAL's checks use. Nothing on a replica assigns an fno. Writes through the engine's own
 * calls (insert, delete, drop, retention, classes) are refused with TSDB_ERR_READONLY: they arrive as records.
 */
#ifndef TSDB_REPLICA_H
#define TSDB_REPLICA_H

#include "internal.h"

/* eng_open_replica (internal.h): wal_sync_ms must be 0 (a periodic sync would acknowledge before durability, 5.1). */

/* Appends records [buf, buf + len) that lie at offset `off` of segment `seq` at the leader. Validates every record
 * (its checks bind it to seq and the offset) and that the numbers continue the log without a gap. `seq` is the
 * current segment (off is where it ends) or a higher one (off 0: the old segment is synced first, the new one made).
 * TSDB_ERR_INVALID: wrong place or numbers (nothing written); TSDB_ERR_CORRUPT: a record that fails its checks. */
int replica_append(struct eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len);
/* Makes the appended records durable (a group commit: concurrent callers share it). */
int replica_sync(struct eng *db);
uint64_t replica_log_end(struct eng *db);     /* the last position written (0: empty) */
uint64_t replica_durable_end(struct eng *db); /* the last position known durable: after a cut, never above the log end */
uint64_t replica_applied_end(struct eng *db); /* the last position applied */
/* Applies, in order, every whole record whose last position is <= commit and that is durable. Returns 0, or the error
 * (a record that fails its checks is TSDB_ERR_CORRUPT; an apply that cannot finish fails the engine). */
int replica_apply(struct eng *db, uint64_t commit);
/* The driver saved the commit floor `floor` (>= every floor before): segments at or below it that are in parts are
 * unlinked. Never called with a floor above the applied end. */
int replica_floor_saved(struct eng *db, uint64_t floor);
/* Cuts the log after position `last` (a record end, or 0): the segment that holds it is truncated there, later
 * segments are removed, the cut is durable before it returns. TSDB_ERR_CORRUPT (nothing changed) when `last` is below
 * the applied end (the core never asks: damage or a bug) or is not a record end. */
int replica_cut(struct eng *db, uint64_t last);
/* The leader shipped the pending part file `name` (its tmp name, `len` bytes) for a COMMIT marker: it is kept, synced
 * (file and directory), until the marker is applied or cut. The positions of a COMMIT marker are not durable here (not
 * acknowledged, not applied) until every file it names is staged with the size and xxh64 it says: call replica_sync
 * after the last replica_stage of a marker. */
int replica_stage(struct eng *db, const char *name, const void *buf, size_t len);
/* The first file of an appended, unapplied COMMIT marker that is not staged (or staged wrongly): its leader name and
 * size, for the driver to fetch and replica_stage. Returns 1, or 0 when none is missing. */
int replica_missing(struct eng *db, char *name, size_t cap, uint64_t *size);
/* The records from the one that starts at position `from` (a record start; the end of the log + 1 gives len 0):
 * whole records of one segment, at most cap bytes unless the first one is larger (then it alone). *seq and *off say
 * where they lie, *last the last position covered. TSDB_ERR_NOTFOUND: the log no longer holds `from` (it is in
 * parts: a snapshot is needed). For a sender (catch-up, tests). A record the buffer cannot take is returned alone. */
extern size_t replica_read_win_min; /* test hook */
extern void (*replica_test_sync_hook)(struct eng *db); /* test hook: between a sync's fdatasync and its bookkeeping */
int replica_read(struct eng *db, uint64_t from, uint8_t **buf, size_t *len, size_t cap, uint64_t *seq, uint64_t *off,
                 uint64_t *last);

#endif
