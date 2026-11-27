/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The swap of a settle (R7, r7-swap): intent v4 ("tsdb-intent 4 <n>", then n lines `L <tmp_*.ec> <pK_A_B_L.ec>`
 * and n lines `R <pK_A_B_L.part> <settled_pK_A_B_L.part>`, the i-th R the source of the i-th L, then "end <xxh64>":
 * src/part.c intent_unseal), its commit, and its recovery at open. intents_recover hands every intent that begins
 * with INTENT_V4_HEAD to ecswap_recover. src/ecswap.c has the design.
 */
#ifndef TSDB_ECSWAP_H
#define TSDB_ECSWAP_H

#include "internal.h"

/* Swaps each listed .part in[i] for the .ec part ec[i], all or nothing, for n parts of one partition key.
 * ec[i] is opened from its temporary: ec[i]->pending names tmp_<pid>_<n>.ec (written and fsynced), ec[i]->name is
 * in[i]->name with ".ec" for ".part" (the same key and block range), ec[i]->ec is set. The caller keeps its own
 * references to every in[i] and ec[i] whatever the result (the list takes one of ec[i] and drops its one of in[i]).
 * 0: the swap is durable, each ec[i] listed where in[i] was, in[i]'s file renamed settled_<name> (whatever the
 * MANIFEST publish after it says: a failed write of it failed the engine, which the next call sees). TSDB_ERR_STOPPED:
 * an input was no longer listed, was dropped or was in a merge when the swap took db->lock (a drop retired the key
 * meanwhile): nothing renamed, the temporaries unlinked, their objects handed to r7-gc. TSDB_ERR_INVALID: the
 * arguments break the rules above (nothing written). TSDB_ERR_IO: the engine failed (fail-stop; the files are left
 * for the next open to sort out), or the intent could not be written (nothing committed, the temporaries unlinked).
 * TSDB_ERR_NOMEM, TSDB_ERR_READONLY: nothing written. Only 0 means the .ec parts are listed; only STOPPED and
 * INVALID, READONLY, NOMEM mean they are not and never will be (after IO the next open decides).
 * Lock order: publish_lock > latest_rw > db->lock, as a publisher's; the caller holds none of them. */
int ec_swap_commit(eng *db, part **in, part **ec, size_t n);

/* R10: swaps the contiguous sources in[0 .. nin) (one partition key, at least one .ec, in increasing block order) for
 * one combined output `out` (an .ec part opened from tmp_*.ec, or an empty .part with ng == 0 opened from tmp_*.part,
 * covering [in[0]->min_block, in[nin-1]->max_block]). Writes intent v4 ("tsdb-intent 4 1 <nin>", L <tmp> <fin>,
 * R <p.ec> retired_<p.ec> for .ec sources, R <p.part> settled_<p.part> for .part sources), runs lt_merge_marks in its
 * write section, hands retired .ec sources to r7-gc and settled .part sources to r7-keep, and wakes db_wait_merges.
 * Same return contract and lock order (publish_lock > latest_rw > db->lock) as ec_swap_commit. */
int ec_swap_reencode(eng *db, part **in, size_t nin, part *out);

/* R15 (r15-reverse, D14): swaps each listed .ec part in[i] for its local .part out[i] (all or nothing, one partition
 * key). out[i]->pending names either settled_<P.part> (fast path) or tmp_<pid>_<n>.part (decode path, fsynced).
 * Serializes against concurrent drops via db_pub_begin/db_pub_end, verifies inputs, retired_<P>.ec absence and source
 * files before writing and syncing intent v5 ("tsdb-intent 5 <2n>", n lines `L <src> <P.part>` and n lines
 * `U <P.ec> retired_<P.ec>`, "end <xxh64>") outside latest_rw/db->lock, then under latest_rw > db->lock performs only
 * the 2n renames (L then U), eckeep/r7-gc updates and in-place list swap, followed by a post-rename directory fsync,
 * intent unlink (and corrupt settled_ cleanup + dir fsync on the decode path), db_pub_end, and MANIFEST publish.
 * (Crash recovery in unsettle_recover syncs between L and U.) Returns TSDB_ERR_NOTFOUND if a source disappeared before
 * writing the intent (caller may retry via decode), TSDB_ERR_LOCKED if retired_<P>.ec is still awaiting GC,
 * TSDB_ERR_STOPPED if an input was dropped/merged/unlisted. Lock order: publish_lock > latest_rw > db->lock. */
int ec_swap_unsettle(eng *db, part **in, part **out, size_t n);

/* The open's recovery of the v4 intent `name` (its whole text in buf[0 .. len), which may be changed). 0: done, and
 * intents_recover unlinks the intent (and syncs the directory); else the open fails with it. */
int ecswap_recover(eng *db, const char *name, char *buf, size_t len);
/* The same for a replica's cold apply, intent v5 (INTENT_V5_HEAD; src/ecswap.c has the rules) */
int ecapply_recover(eng *db, const char *name, char *buf, size_t len);

/* The engine's hooks (src/tier.h): db->ecstate->swap. Nothing (counted): the swap keeps no state of its own. */
int ecswap_eng_open(eng *db);
void ecswap_eng_close(eng *db);

/* tests: runs in ec_swap_commit with publish_lock held and the intent durable, before latest_rw and db->lock (a drop
 * can complete there: its retire takes neither publish_lock nor anything else the swap holds). Set it before the
 * threads that swap start. */
extern void (*ec_swap_hook)(eng *db);

/* The settle's own crash points (src/settle.c) of TSDB_SETTLE_CRASH_AT, which this file reads: after-put (the chunks
 * are durable) and after-ec-tmp (the tmp_*.ec is durable too, no intent written) */
enum { ECSWAP_CRASH_AFTER_PUT = 1, ECSWAP_CRASH_AFTER_EC_TMP = 2 };
void ecswap_crash_point(int which);

/* tests: what publishes MANIFEST after a swap (manifest_publish) */
extern int (*ec_swap_publish)(eng *db);

/* tests: one planted failure, used once: AT_NOMEM the swap's own allocation (before anything is written), INTENT_NOMEM
 * intent_write's buffer (nothing written), INTENT_IO the intent written whole and then reported failed (as a failed
 * fdatasync: its name is removed and never synced) */
enum { ECSWAP_FAIL_NONE = 0, ECSWAP_FAIL_AT_NOMEM, ECSWAP_FAIL_INTENT_NOMEM, ECSWAP_FAIL_INTENT_IO };
extern atomic_int ecswap_test_fail;
/* tests: every swap is one whose objects a committed COLD marker names, as ecapply_swap_pre says on a cluster ec volume
 * (R8): a refusal after the hook fails the engine and hands nothing to r7-gc or to the caller */
extern atomic_bool ecswap_test_logged;
/* tests: runs in a refused swap's cleanup after the temporaries were unlinked and before the directory sync that makes
 * that durable (and before any object goes to r7-gc) */
extern void (*ecswap_discard_hook)(eng *db);
/* tests: runs in a refused swap's cleanup right after its objects were handed to r7-gc */
extern void (*ecswap_handed_hook)(eng *db);
/* tests: runs in a failed intent write with the intent still named, before its unlink (another thread's directory
 * sync can come exactly there) */
extern void (*ecswap_intent_fail_hook)(eng *db);

/* planted bugs (tests/test_ecswap.c --mutant NAME; the Makefile requires each to fail it) */
enum {
    SWAP_MUT_NONE = 0,
    SWAP_MUT_EARLY,       /* swapearly: the renames run before the intent is written */
    SWAP_MUT_PARTIAL,     /* swappartial: recovery's roll forward renames the .ec names only, the sources stay */
    SWAP_MUT_NODIRSYNC,   /* swapnodirsync: no directory fsync after the intent: its name may not be durable yet */
    SWAP_MUT_NODROPCHECK, /* swapnodropcheck: no check of the inputs under db->lock */
    SWAP_MUT_NOLISTED,    /* swapnolisted: the check does not ask whether an input is still listed */
    SWAP_MUT_NOFAILCHECK, /* swapnofailcheck: the check does not ask whether the engine failed */
    SWAP_MUT_NOSTATS,     /* swapnostats: no statistics events for the swapped parts */
    SWAP_MUT_MANIFESTRC,  /* swapmanifestrc: a durable swap returns what manifest_publish said */
    SWAP_MUT_BACKONGONE,  /* swapbackongone: recovery rolls back whenever a source is gone, whatever was renamed */
    SWAP_MUT_LOSTSETTLED, /* swaplostsettled: an .ec under none of its names fails the open even when its source is
                           * settled (a drop after the swap, lifetime 0) */
    SWAP_MUT_LOGGEDGC,    /* swaploggedgc: a refused swap hands objects a committed COLD names to r7-gc (R8) */
};
extern int ecswap_mutant;
#endif
