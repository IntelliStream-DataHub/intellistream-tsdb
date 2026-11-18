/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (R7, r7-gc): retired_*.ec in the graveyard (GY_EC), the delete queue of objects
 * whose last reference went, and the sweep of orphan chunks at open, before the merge and settle threads start (D6).
 * r7-iface left stubs that do what the engine does today with these names (nothing: it never wrote one).
 */
#ifndef TSDB_ECGC_H
#define TSDB_ECGC_H

#include "internal.h"

/* load_parts met retired_<p...>.ec (ec_retired_name). Stub: 0, the file left alone (a name the engine does not
 * know). r7-gc: a graveyard entry (graveyard_add, GY_EC). */
int ecgc_load_retired(eng *db, const char *name, bool read_only);
/* db_retire_locked is retiring the .ec part p (renamed retired_<name>, db->lock held). Stub: nothing. */
void ecgc_retire_locked(eng *db, part *p);
/* graveyard_purge, for a GY_EC entry (db->lock held): true when the entry is done and leaves the list. Stub: the
 * file is unlinked once due, as a retired part is. */
bool ecgc_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now);
/* part_unref's last reference of an .ec part: r7-gc queues its object's deletion here, but only when the part is
 * obsolete (it is also called for parts that were never listed, as load_parts' refusals, and for parts that are only
 * closed). It runs after munmap and after the unlink of an obsolete part's file: never touch p->map. p->ec->tier holds
 * no reference of its own (the engine's does; eng_close puts it after its parts went). Frees p->ec. Stub:
 * ecpart_free. */
void ecgc_last_unref(part *p);

/* The tier's and the engine's hooks (src/tier.h): t->gc, db->ecstate->gc. Stubs: nothing (counted). */
int ecgc_tier_open(struct tier *t);
void ecgc_tier_close(struct tier *t);
int ecgc_eng_open(eng *db);
void ecgc_eng_close(eng *db);
/* D6's sweep of the engine's orphan chunks: eng_open_sched calls it after recovery, load_parts and the CLASSES check,
 * before the merge, flush and settle threads start, for a writer with a tier (never a read-only open or a follower).
 * Non-zero fails the open. Stub: 0. */
int ecgc_open_sweep(eng *db);
#endif
