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
 * src/ecapply.c has the apply's design; src/ecswap.c the intent v5 and its recovery (ecapply_recover).
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
 * 0 not (none), TSDB_ERR_CORRUPT for some. ecapply_cold_decide_ex also handles an empty-output re-encode (n == 0,
 * nretired > 0) by checking whether all retired oids are still listed (0), none (1), or some (CORRUPT). */
int ecapply_cold_decide(struct eng *db, int64_t key, const uint8_t *oids, uint32_t n);
int ecapply_cold_decide_ex(struct eng *db, int64_t key, const uint8_t *oids, uint32_t n, const uint8_t *retired,
                           uint32_t nretired);
/* Around a settle's swap (src/ecswap.c, ec_swap_commit: the inputs `in`, their .ec parts `ec`, n of each). */
/* ecapply_swap_pre sets *logged (first, also when it fails) when a committed COLD marker names the swap's objects (every
 * swap of a cluster ec volume, leader or replica): ec_swap_commit then never hands them to r7-gc or back to its caller;
 * a refusal from there on (the hook's error, out of memory, the intent's write, a source dropped, merging or unlisted
 * under db->lock) fails the engine with a line and returns TSDB_ERR_IO, the temporaries and any intent left for the
 * next open. Refusals before the hook (READONLY, INVALID: bad arguments) hand nothing over themselves; a caller of a
 * logged swap must never give the objects to r7-gc on any error. */
int ecapply_swap_pre(struct eng *db, struct part **in, struct part **ec, size_t n, bool *logged);
void ecapply_swap_post(struct eng *db, struct part **in, struct part **ec, size_t n);

/* planted bugs (tests/test_ecapply.c --mutant NAME; the Makefile requires each to fail it, D14) */
enum {
    ECAPPLY_MUT_NONE = 0,
    ECAPPLY_MUT_YCOLD,              /* ycold: Y retired with the input set (its parts renamed settled_, never listed) */
    ECAPPLY_MUT_ECABOVEY,           /* ecabovey: the .ec parts numbered above Y */
    ECAPPLY_MUT_PPOSIGNORED,        /* pposignored: a part above Z is retired like the others, no stop */
    ECAPPLY_MUT_V5NODIRSYNC,        /* v5nodirsync: no directory fsync after the v5 intent */
    ECAPPLY_MUT_COLDNOFLUSHTHROUGH, /* coldnoflushthrough: the active memtable flushed at the marker too */
    ECAPPLY_MUT_LEADERSWAPNORS,     /* leaderswapnors: a leader's swap writes no RSTATE `doing`/`done` */
    ECAPPLY_MUT_PPOSAFTERSWAP,      /* pposafterswap: a leader's swap records its .ec parts' PPOS after its renames */
    ECAPPLY_MUT_MERGEKEPT,          /* mergekept: a retired own part is not marked dropped (a merge in flight lists) */
    ECAPPLY_MUT_NOTFOUNDZERO,       /* notfoundzero: a listed part with no PPOS record counts as position 0 (<= Z) */
    ECAPPLY_MUT_NOORDER,            /* noorder: the new parts numbered in the marker's order, not the leader's blocks */
    ECAPPLY_MUT_CHECKZ,             /* checkz: an object record whose ppos is not the marker's Z passes */
    ECAPPLY_MUT_SEENSKIP,           /* seenskip: two .ec files of the marker may match one object */
    ECAPPLY_MUT_NOYHCHECK,          /* noyhcheck: a staged Y's header (index hash, granule rows, key) is not checked */
    ECAPPLY_MUT_DECIDESOME,         /* decidesome: decide answers "not done" when only some of the oids are listed */
    ECAPPLY_MUT_RECROLLMISSING,     /* recrollmissing: v5 recovery rolls forward with nothing renamed and a file gone */
    ECAPPLY_MUT_RECNODIVERGE,       /* recnodiverge: v5 recovery rolls back a lost own part without DIVERGED */
    ECAPPLY_MUT_RECNODIRSYNC,       /* recnodirsync: v5 recovery does not sync the directory before the intent goes */
};
extern int ecapply_mutant;
/* Crash points of a replica's apply (tsdbd's server/crashpt.h installs the hook: cold-before-apply, cold-mid-intent):
 * BEFORE_APPLY once the marker is checked, nothing done; MID_INTENT with the intent durable and the .ec renames done,
 * Y and R not (db->lock held: the hook only ends the process). */
enum { ECAPPLY_CRASH_BEFORE_APPLY = 1, ECAPPLY_CRASH_MID_INTENT = 2 };
extern void (*_Atomic ecapply_crash_hook)(struct eng *db, int which);
int ecapply_mutant_parse(const char *name); /* -1 when unknown */
/* test hook (tests/test_ecapply.c): STAGED once the marker's files are read and checked, before the step that decides
 * under publish_lock and db->lock (rp->mu, stage_mu and every engine lock free: the hook may drop or merge the key) */
enum { ECAPPLY_STEP_STAGED = 1 };
extern void (*_Atomic ecapply_test_hook)(struct eng *db, int step);
/* test seam (test_ecapply badft): nonzero, the segment a COLD's apply asks replica_flush_through for in place of the
 * marker's F (a marker whose F is not below its own segment, which the leader never writes) */
extern _Atomic uint64_t ecapply_test_ft;

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
 * marker's flush_through F must lie below its segment (TSDB_ERR_CORRUPT otherwise, a line). F only bounds: every
 * sealed memtable is flushed, whatever segment F names (safe because the leader flushes through its cut before the
 * marker, so sealed memtables above F hold no rows of the key; as apply_commit's flush). Inside ecapply_cold only;
 * with rp->mu let go. */
int replica_flush_through(struct eng *db, uint64_t f);

#endif
