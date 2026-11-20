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
/* The retention caps of a replica's flushed segments (design-cluster.md 8.3, the volume's repl_retain_mb and
 * repl_retain_s): a segment at or below the saved floor stays while the segments from it to the newest flushed one hold
 * at most max_bytes and it was flushed at most max_ms ago (a zero cap is not used), so that a member a failover promotes
 * can still send it to a lagging member. Both zero, or never set: a segment goes once the floor passes it.
 * replica_keep_next sets them for the next eng_open_replica on this thread (its open flushes what it replays). */
void replica_keep_cap(struct eng *db, uint64_t max_bytes, uint64_t max_ms);
void replica_keep_next(uint64_t max_bytes, uint64_t max_ms);
/* tests: plants mutant replicadropsfloor (a replica's flushed segments go by the floor alone) in this process; tsdbd
 * calls it for TSDB_REPLD_MUTANT=replicadropsfloor (tests/test_cluster.c failoverkeep) */
void tsdb_test_replica_drops_floor(void);
/* Cuts the log after position `last` (a record end, or 0): the segment that holds it is truncated there, later
 * segments are removed, the cut is durable before it returns. TSDB_ERR_CORRUPT (nothing changed) when `last` is below
 * the applied end (the core never asks: damage or a bug) or is not a record end. */
int replica_cut(struct eng *db, uint64_t last);
/* The leader shipped the pending part file `name` (its tmp name, `len` bytes) for a COMMIT marker: it is kept, synced
 * (file and directory), until the marker is applied or cut. The positions of a COMMIT marker are not durable here (not
 * acknowledged, not applied) until every file it names is staged with the size and xxh64 it says: call replica_sync
 * after the last replica_stage of a marker. */
int replica_stage(struct eng *db, const char *name, const void *buf, size_t len);
/* The same for a file too large to hold in memory while it arrives (r3d review M3): replica_spill_open makes a new
 * file stage_s<pid>_<n>.tmp in the volume's directory (its name in tmp, cap >= 48; removed by the next open like every
 * stage tmp) and returns its descriptor or a negative error; the driver writes the bytes there, and
 * replica_stage_spilled syncs it and names it as replica_stage would (it consumes the tmp name: renamed, or removed on
 * failure; the caller closes fd). replica_spill_drop removes a spill file whose transfer failed. */
int replica_spill_open(struct eng *db, char *tmp, size_t cap);
int replica_stage_spilled(struct eng *db, const char *name, int fd, const char *tmp, size_t len);
void replica_spill_drop(struct eng *db, const char *tmp);
/* The first file of an appended, unapplied COMMIT marker that is not staged (or staged wrongly): its leader name and
 * size and the marker's position (*pos, for eng_leader_file_open), for the driver to fetch and replica_stage. Returns 1,
 * or 0 when none is missing. */
int replica_missing(struct eng *db, uint64_t *pos, char *name, size_t cap, uint64_t *size);
/* The records from the one that starts at position `from` (a record start; the end of the log + 1 gives len 0):
 * whole records of one segment, at most cap bytes unless the first one is larger (then it alone). *seq and *off say
 * where they lie, *last the last position covered. TSDB_ERR_NOTFOUND: the log no longer holds `from` (it is in
 * parts: a snapshot is needed). For a sender (catch-up, tests). A record the buffer cannot take is returned alone.
 * On a writer (a leader) it reads the segments it keeps and the current one as far as it is written (len 0: `from` is
 * not written yet). */
extern size_t replica_read_win_min; /* test hook */
extern void (*replica_test_sync_hook)(struct eng *db); /* test hook: between a sync's fdatasync and its bookkeeping */
/* test hook: the applier's windows. RH_COMMIT_FLUSHED (a COMMIT's prepare after its flush), RH_COMMIT_PREPARED (after
 * its staging) RH_DROP (after a DROP's flush), RH_CLASS (after a CLASS purge), RH_ROWS_YIELD (between records): rp->mu
 * free. RH_COMMIT_OPEN (a staged file linked, before its open; stage_mu held; nonzero refuses the open), RH_ROWS_HELD
 * (before a row record's apply): rp->mu held. Returns 0 except to refuse at RH_COMMIT_OPEN. */
enum { RH_COMMIT_FLUSHED, RH_COMMIT_OPEN, RH_COMMIT_PREPARED, RH_DROP, RH_CLASS, RH_ROWS_HELD, RH_ROWS_YIELD };
extern int (*replica_test_apply_hook)(struct eng *db, int at);
extern void (*leader_test_marker_hook)(struct eng *db, uint64_t fno); /* test hook: a COMMIT's marker appended and
                                                                         * synced, before leader_commit goes on */
int replica_read(struct eng *db, uint64_t from, uint8_t **buf, size_t *len, size_t cap, uint64_t *seq, uint64_t *off,
                 uint64_t *last);

/* Promotion (design-cluster.md 6.2): the new leader appends its EPOCH marker (epoch >= 1: the segment epoch << 40, above
 * every segment of the log; its position is *pos) after the log it holds, tail included; the driver ships the log and
 * syncs, and when the commit point has passed the marker and replica_apply has applied it (to the end of the log)
 * replica_promote closes *dbp (set to NULL) and opens the directory as a writer in *out: see the function. */
int replica_epoch_begin(struct eng *db, uint64_t epoch, const char *owner, uint64_t *pos);
extern void (*replica_test_promote_hook)(void); /* test hook: after the segments are removed, before the engine closes */
int replica_promote(struct eng **dbp, uint64_t epoch_pos, struct eng **out);
/* A deposed leader (design-cluster.md 6.3), or a replica whose promotion was fenced: *dbp is closed without a flush (a
 * leader's close deposes it first, eng_close) and set to NULL, `between` (may be NULL; tsdbd's crash point) runs, and
 * the directory opens as a replica in *out with commit floor `floor` (the REPL file's), whose open cuts the tail the
 * commit point never passed; the driver's QUERY/ANSWER then cuts what the new leader's history does not hold. */
int replica_demote(struct eng **dbp, uint64_t floor, void (*between)(void), struct eng **out);

#endif
