/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (R7, r7-gc; design-cluster-data.md E14, 2.4, 15.7; the plan's D6): retired_*.ec
 * in the graveyard (GY_EC), the delete queue of objects whose last reference went, and the sweep of orphan chunks at
 * open, before the merge and settle threads start. src/ecgc.c has the design.
 */
#ifndef TSDB_ECGC_H
#define TSDB_ECGC_H

#include "internal.h"

/* load_parts met retired_<p...>.ec (ec_retired_name): its record is read (the object's id and geometry) and it gets a
 * graveyard entry (GY_EC) with a whole lifetime again, or none with obsolete_part_lifetime_s <= 0 (due at once). A
 * record that cannot be read fails the open (damage is never silent); a read-only open leaves the file alone. */
int ecgc_load_retired(eng *db, const char *name, bool read_only);
/* db_retire_locked is retiring the .ec part p (renamed retired_<name>, db->lock held): its object is held until p's
 * last part_unref, and with obsolete_part_lifetime_s <= 0 its graveyard entry (due now) is added here. */
void ecgc_retire_locked(eng *db, part *p);
/* graveyard_purge, for a GY_EC entry (db->lock held): true when the entry is done and leaves the list. Once due and
 * no part of the object is referenced (in any engine of the tier) the object goes to the tier's delete queue; once
 * the queue has deleted every chunk, the name is unlinked and the directory synced (eng_sync_dir), and only then does
 * the entry leave. No I/O on the chunk stores here. A test-marked part (no object) is unlinked once due. */
bool ecgc_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now);
/* part_unref's last reference of an .ec part: lets go of the hold ecgc_retire_locked took (no I/O: the purge queues
 * the deletion once the deadline passed too). Also called for parts that were never listed and parts that are only
 * closed (no hold: nothing). It runs after munmap: never touch p->map. p->ec->tier holds no reference of its own (the
 * engine's does; eng_close puts it after its parts went). Frees p->ec. */
void ecgc_last_unref(part *p);
/* part_unref of an obsolete part: whether the name is the delete queue's to unlink (after the chunks), not
 * part_unref's. True for an .ec part with an object (a test-marked one has none). */
bool ecgc_owns_name(const part *p);
/* A swap that did not happen (ec_swap_commit refused it: a drop took its key, or its intent could not be written)
 * unlinked the temporary of the .ec part p, which was never listed and never named: its object's chunks belong to
 * no one now: their deletion is queued (p stays the caller's; read only its record). */
void ecgc_unlisted_object(eng *db, const part *p);

/* An object that no file names and never will (an abandoned settle, r7-settle): its npos positions are deleted by the
 * queue. No I/O here; TSDB_ERR_NOMEM (its chunks then stay for the next open's sweep), TSDB_ERR_INVALID without a
 * tier. */
int ecgc_queue_object(eng *db, const uint8_t oid[16], unsigned npos);

/* The tier's and the engine's hooks (src/tier.h): t->gc (the queue, its worker thread and the holds), db->ecstate->gc
 * (the retired names of the engine). */
int ecgc_tier_open(struct tier *t);
void ecgc_tier_close(struct tier *t);
int ecgc_eng_open(eng *db);
void ecgc_eng_close(eng *db);
/* D6's sweep of the engine's orphan chunks: eng_open_sched calls it after recovery, load_parts and the CLASSES check,
 * before the merge, flush and settle threads start, for a writer with a tier (never a read-only open or a follower).
 * The objects of the volume that no listed .ec part and no retired_*.ec names are queued; only with every store UP
 * (tier_writable), else skipped with a line. The count goes to stderr. Non-zero fails the open (TSDB_ERR_NOMEM only:
 * a store that cannot be listed skips the sweep, which the next open runs again). */
int ecgc_open_sweep(eng *db);

/* Works the tier's delete queue once (the worker thread does, every second or when woken): per object, every chunk is
 * deleted (NOTFOUND counts as gone) with every store UP before and after; else the entry stays for a later round.
 * Returns the entries left. Never touches an engine. */
size_t ecgc_tier_work(struct tier *t);

/* tests */
typedef struct ecgc_counters {
    uint64_t queued;    /* objects queued */
    uint64_t deleted;   /* objects whose every chunk is gone */
    uint64_t retries;   /* rounds an entry was kept (a store not UP, an error) */
    uint64_t swept;     /* orphans the sweeps queued */
    uint64_t sweeps, sweeps_skipped;
    uint64_t unlinked;  /* retired names unlinked after their chunks */
    uint64_t pending;   /* entries in the queue now */
    uint64_t holds;     /* parts holding an object now */
} ecgc_counters;
void ecgc_get_counters(struct tier *t, ecgc_counters *out);
/* Set before tier_open: no worker thread; the test calls ecgc_tier_work itself. */
extern atomic_bool ecgc_test_manual;
/* Planted bugs (tests/test_ecgc.c --mutant NAME; the Makefile requires each to fail it): the purge unlinks the retired
 * name before the chunks are deleted (gcunlinkfirst); the last part_unref keeps its hold, so the object never goes
 * (gcunrefleak); the purge ignores the holds, deleting the chunks of a part a query still reads (gcnohold); the sweep
 * ignores the retired names (sweeplive); the sweep lists the stores at the first round of the queue, after the threads
 * started, instead of in the open (sweepinflight). */
enum { GC_MUT_NONE = 0, GC_MUT_UNLINKFIRST, GC_MUT_UNREFLEAK, GC_MUT_NOHOLD, GC_MUT_SWEEPLIVE, GC_MUT_SWEEPINFLIGHT };
extern int ecgc_mutant;
#endif
