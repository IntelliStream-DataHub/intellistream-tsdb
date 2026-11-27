/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (R7, r7-gc; R8, r8k-gc; design-cluster-data.md E14, 2.4, 15.7; the plan's D6,
 * D13): retired_*.ec in the graveyard (GY_EC), the delete queue of objects whose last reference went, the O(1) hash of
 * holds taken at part_open_ec, the sweep of orphan chunks at open on single-server tiers, and the tier worker that
 * drives cluster MK_GC (src/ecgcmk.h). src/ecgc.c has the design.
 */
#ifndef TSDB_ECGC_H
#define TSDB_ECGC_H

#include "internal.h"

/* load_parts met retired_<p...>.ec (ec_retired_name): its record is read (the object's id and geometry) and it gets a
 * graveyard entry (GY_EC) with a whole lifetime again, or none with obsolete_part_lifetime_s <= 0 (due at once). A
 * record that cannot be read fails the open (damage is never silent); a read-only open leaves the file alone. */
int ecgc_load_retired(eng *db, const char *name, bool read_only);
/* db_retire_locked is retiring the .ec part p (renamed retired_<name>, db->lock held): records the retired name for
 * GC, and with obsolete_part_lifetime_s <= 0 adds its graveyard entry (due now). The hold itself is taken in
 * part_open_ec (ecgc_hold) so a reopen's old engine reading p keeps its object alive. */
void ecgc_retire_locked(eng *db, part *p);
/* graveyard_purge, for a GY_EC entry (db->lock held): true when the entry is done and leaves the list. On a single
 * server: once due and no part of the object is referenced (in any engine of the tier) the object goes to the tier's
 * delete queue; once the queue has deleted every chunk, the name is unlinked and only then does the entry leave. On a
 * cluster EC volume: returns false (cluster retired objects are collected by the leader's MK_GC, src/ecgcmk.h). No
 * I/O on the chunk stores here. A test-marked part (no object) is unlinked once due. */
bool ecgc_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now);
/* part_open_ec: takes a hold for p on (volume, oid) in the tier's O(1) hash (r7-gc F5 / r8k-gc). */
int ecgc_hold(struct tier *t, const part *p, uint32_t volume, const uint8_t oid[16]);
/* Whether any open .ec part in the tier holds (volume, oid). */
bool ecgc_held(struct tier *t, uint32_t volume, const uint8_t oid[16]);
/* part_unref's last reference of an .ec part: lets go of the hold part_open_ec took (no I/O: the purge / MK_GC queues
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
 * queue (single server) or queued as a cluster orphan for MK_GC (cluster EC volume). No I/O here; TSDB_ERR_NOMEM,
 * TSDB_ERR_INVALID without a tier. */
int ecgc_queue_object(eng *db, const uint8_t oid[16], unsigned npos);

/* The tier's and the engine's hooks (src/tier.h): t->gc (the queue, its worker thread, the O(1) holds hash, and the
 * registry of cluster EC engines), db->ecstate->gc (the retired names of the engine). */
int ecgc_tier_open(struct tier *t);
void ecgc_tier_quiesce(struct tier *t);
void ecgc_tier_close(struct tier *t);
int ecgc_eng_open(eng *db);
void ecgc_eng_ready(eng *db); /* registers a cluster EC engine with the tier's GC worker */
void ecgc_eng_stop(eng *db);  /* unregisters and waits for any in-flight cluster GC round on db */
void ecgc_eng_close(eng *db);
/* D6's sweep of the engine's orphan chunks: eng_open_sched calls it after recovery, load_parts and the CLASSES check,
 * before the merge, flush and settle threads start, for a writer with a tier (never a read-only open or a follower).
 * Skipped on cluster EC volumes (orphans are collected by the leader via committed MK_GC, E14). */
int ecgc_open_sweep(eng *db);

/* Works the tier's delete queue once (and cluster MK_GC on registered cluster EC engines). Returns entries left in
 * the single-server queue. */
size_t ecgc_tier_work(struct tier *t);
void ecgc_kick_tier(struct tier *t);

/* Helpers for src/ecgcmk.c (cluster EC MK_GC). */
int ecgc_take_due_retired(eng *db, int64_t now, uint8_t (**oids_out)[16], size_t *n_out);
int ecgc_collect_oids_locked(eng *db, const uint8_t *oids, size_t noids, char (**unlinks_out)[256],
                             size_t *nunlinks_out, bool *retired_listed_out);
int ecgc_list_retired_locked(eng *db, uint8_t (**oids_out)[16], size_t *n_out);
void ecgc_note_unlinked(struct tier *t, uint64_t n);

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
/* Set before an open: no sweep. Objects a test attached in memory only (ec_test_attach, tests/test_seam.c) are named by
 * no .ec file, so a reopen's sweep would delete them as orphans. */
extern atomic_bool ecgc_test_nosweep;
/* A transient delete error: while positive, a round keeps each entry it reaches (and counts this down). */
extern atomic_int ecgc_test_del_fail;
/* Called by the worker after each position's delete (a test fails a store in the middle of a round). */
extern void (*ecgc_test_after_del)(struct tier *t, unsigned pos);
/* Test overrides read from TSDB_ECGC_TEST (once per process; or set directly in unit tests). */
extern _Atomic int64_t ecgc_test_orphan_grace_ms; /* -1: default (60000 ms) */
extern _Atomic int64_t ecgc_test_poll_ms;         /* -1: default (2000 ms) */
extern _Atomic int64_t ecgc_test_lifetime_s;      /* -1: use db->opt.obsolete_part_lifetime_s */
/* Planted bugs (tests/test_ecgc.c --mutant NAME, or TSDB_ECGC_MUTANT=NAME in tsdbd). */
enum {
    GC_MUT_NONE = 0,
    GC_MUT_UNLINKFIRST,
    GC_MUT_UNREFLEAK,
    GC_MUT_NOHOLD,
    GC_MUT_SWEEPLIVE,
    GC_MUT_SWEEPINFLIGHT,
    GC_MUT_SWEEPFOREIGN,
    GC_MUT_UNLISTEDNOOP,
    GC_MUT_NORETRYTIMER,
    GC_MUT_NOAFTER,
    /* R8 (r8k-gc) planted bugs: */
    GC_MUT_UNCOMMITTED,    /* gcuncommitted: leader deletes chunks without waiting for MK_GC to commit */
    GC_MUT_NAMEDORPHAN,    /* gcnamedorphan: cluster orphan sweep ignores oids named by records in the WAL */
    GC_MUT_SUSPENDIGNORED, /* gcsuspendignored: cluster orphan sweep ignores gc_suspended */
    GC_MUT_HOLDATRET,      /* holdatretire: holds taken at ecgc_retire_locked instead of part_open_ec */
    GC_MUT_REPLICANOOP,    /* gcreplicanoop: ecgcmk_apply on a replica is a no-op */
    /* R9 (r9-whole) planted bug: */
    GC_MUT_RELEASE_NOMARK, /* releasenomark: deletes released holder's chunks without waiting for committed MK_GC */
    /* R15 (r15-follow) planted bug: */
    GC_MUT_FOLLOWDEL,      /* followdel: writer deletes retired chunks immediately ignoring obsolete_part_lifetime_s */
};
extern int ecgc_mutant;
int ecgc_mutant_parse(const char *name); /* GC_MUT_* or -1 */
void ecgc_env_init(void);                /* reads TSDB_ECGC_MUTANT and TSDB_ECGC_TEST once */
#endif
