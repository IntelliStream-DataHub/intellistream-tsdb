/* SPDX-License-Identifier: AGPL-3.0-only */
/* Cluster volumes in a tsdb handle (docs/design-cluster.md 3.1, 8.5; DECISION D2 of the R3 plan).
 *
 * A cluster node's data directory keeps its root for USERS only: no series in the root, no VOLUMES, no local
 * `volume add`. Volume numbers come from the metadata group (VOLUME_ADD), so the handle's volume table has holes: a
 * number this server does not hold is declared (its cluster name known) and has no engine, and every request that
 * reaches it is TSDB_ERR_UNAVAILABLE. A held volume lives in `<data>/v<N>/` (made by tsdbd, META with
 * `requires=cluster,replica`) and is opened here as the record's leader (a writer) or as a replica
 * (`eng_open_replica`). Ids carry the number as usual (TSDB_SID); names are unique per volume, and a client names
 * the volume of a creation by its cluster name (the `path` of tsdb_series_at / tsdb_series_create).
 *
 * The engine swap (tsdb_cvol_replace) is what a driver (tsdbd's peer side, r3d/r3e) uses to turn a replica into the
 * writer (tsdb_cvol_promote, through replica_promote) and back: the volume is quiesced (requests get UNAVAILABLE),
 * every reference, lookup and multi call's cut on the old engine is waited for, the name strings it handed out are
 * kept until tsdb_close, and the new engine is listed under the handle's write lock.
 */
#ifndef TSDB_CVOL_H
#define TSDB_CVOL_H

#include "tsdb.h"

#include <stdbool.h>
#include <stdint.h>

struct eng;

enum { CVOL_NONE = 0, CVOL_WRITER = 1, CVOL_REPLICA = 2, CVOL_SWAP = 3 /* internal: being replaced */ };

/* Makes t a cluster node's handle. Refuses (TSDB_ERR_INVALID, naming them on stderr) a root that holds series or a
 * VOLUMES file with lines: a node of the 0.10.0 preview is initialised again, not upgraded. Call once, right after
 * tsdb_open, before any request. */
int tsdb_cvol_mode(tsdb *t);
/* The cluster name of volume `number` (1 .. 65534), held here or not. A name never changes (TSDB_ERR_INVALID if
 * another was declared, or the name is taken by another number). */
int tsdb_cvol_declare(tsdb *t, uint32_t number, const char *name);
/* Opens the declared volume `number` from `dir` (META must say volume=number): a writer, or a replica whose commit
 * floor is `floor` (the REPL file's). TSDB_ERR_INVALID if it is open already. */
int tsdb_cvol_open(tsdb *t, uint32_t number, const char *dir, bool replica, uint64_t floor);
/* The number of a declared volume by its cluster name (TSDB_ERR_NOTFOUND). */
/* FNO in a new cluster volume directory (synced with it); 0 or TSDB_ERR_IO */
int tsdb_cvol_fno_create(const char *dir);
int tsdb_cvol_find(tsdb *t, const char *name, uint32_t *number);
/* CVOL_NONE (not held, or not open), CVOL_WRITER or CVOL_REPLICA. */
int tsdb_cvol_role(tsdb *t, uint32_t number);
/* The engine of a held volume with a reference (NULL when not held, or quiesced); give it back with
 * tsdb_cvol_unref. A driver never keeps it across a tsdb_cvol_replace of the volume: that waits for it. */
struct eng *tsdb_cvol_ref(tsdb *t, uint32_t number);
void tsdb_cvol_unref(struct eng **e);
/* Replaces the engine of volume `number`: quiesces it, waits until no call, lookup or cut is inside the old engine,
 * then calls fn(ctx, &old, &neu) with the old engine idle. fn closes *old (and sets it to NULL) and opens *neu, or
 * fails with *old untouched (the old engine stays, and the volume is resumed). With *old closed and no *neu the
 * volume stays held but not open (UNAVAILABLE). The open runs with cluster directories allowed. */
int tsdb_cvol_replace(tsdb *t, uint32_t number, int (*fn)(void *ctx, struct eng **old, struct eng **neu), void *ctx,
                      int role);
/* replica -> writer: tsdb_cvol_replace with replica_promote (the caller has applied its EPOCH marker at epoch_pos). */
int tsdb_cvol_promote(tsdb *t, uint32_t number, uint64_t epoch_pos);
/* Test hook: called inside tsdb_cvol_replace once the volume is quiesced, before it waits for references. */
extern void (*tsdb_cvol_test_replace_hook)(void *arg);
extern void *tsdb_cvol_test_replace_arg;

/* Whether volume `vol` is DIVERGED (R8, design-cluster-data.md E26, D8: a settle's digest did not match; refused like a
 * quiesced volume): 1 or 0, TSDB_ERR_INVALID when the handle has no such volume. Any volume, not only a cluster one;
 * tsdbd reports it (r8i). */
int tsdb_volume_diverged(tsdb *t, size_t vol);

#endif
