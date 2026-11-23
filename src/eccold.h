/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A leader's settle of a cluster ec volume (design-cluster-data.md 2.3 steps 3-5, the R8 plan's D4, D21): the gate
 * (jr_serial, like a large write), the cut at Z, the late parts Y, and the MK_COLD marker; and whether a cluster
 * volume may attach a tier at all (eccold_may_attach, which tier.c asks in place of its refusal of requires=cluster).
 * The leader API r8a2-hooks adds (jr_cold, jr_cold_precommit, jr_cold_commit) is declared here and built in
 * src/replica.c.
 *
 * Built by r8g-cut (src/eccold.c has the design of the gate and the cut). db->cold points at the settle in progress on
 * this engine (its key, cut and marker position) only while that settle's ec_swap_commit runs: r8h's swap hooks read
 * it through eccold_swap_cur on the same thread; NULL otherwise. eccold_open and eccold_close have nothing to do.
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

struct part;
/* Whether db is the leader of a cluster ec volume now (jr attached, not a replica, not deposed): eng_settle settles it
 * through eccold_settle, the settle thread asks every round. */
bool eccold_leader(struct eng *db);
/* eng_settle's leader branch: the parts pin[0 .. np) of key `key` are pinned (settling, referenced) by the caller,
 * which unpins them whatever the result. ec[0 .. np) comes back with the .ec parts (references the caller drops) on 0
 * only; on any error eccold_settle has dropped what it opened and ec[] is NULL. Results: 0 (swapped);
 * TSDB_ERR_UNAVAILABLE (a position not UP under the stripe version and epoch the objects were put by, within
 * settle_wait_max_s, or at the re-check after the gate: the objects go to r7-gc, the policy retries later);
 * TSDB_ERR_STOPPED (closing, or a late part of the key was merging); TSDB_ERR_INDETERMINATE (deposed after the marker
 * went out: nothing is cleaned up, the reopen as a replica decides); TSDB_ERR_IO (the engine failed: after the marker
 * committed, any refusal of the swap fails it); the errors of the puts, the flush and jr_cold before the marker. */
int eccold_settle(struct eng *db, int64_t key, struct part **pin, size_t np, struct part **ec);
/* The COLD being carried out by ec_swap_commit on this engine, for r8h's swap hooks (the same thread): its marker
 * position, key and cut. false when no settle of this engine is between its marker and the end of its swap. */
bool eccold_swap_cur(struct eng *db, uint64_t *pos, int64_t *key, uint64_t *z);

#define ECCOLD_FORCE_S_DEFAULT 3600   /* tier_settings.force_s 0 */
#define ECCOLD_WAIT_MAX_S_DEFAULT 600 /* tier_settings.wait_max_s 0 */
#define ECCOLD_RETAIN_S_DEFAULT 1800  /* repl_retain_s when the leader has no keep cap (meta.h's default) */

/* r8l's crash point cold-after-marker (server/crashpt.h, for the midcommit nemesis): tsdbd sets it; called right after
 * a COLD marker was appended and synced, before its commit point came (appended, not committed). It is reached
 * through jr_cold's own hook: whoever installs eccold_marker_hook sets replica.h's leader_test_marker_hook to
 * eccold_after_append once, before any engine opens; eccold_after_append passes on only the COLD that this thread is
 * appending (a COMMIT's marker passes the same hook). */
extern void (*_Atomic eccold_marker_hook)(struct eng *db, uint64_t pos);
void eccold_after_append(struct eng *db, uint64_t fno);
/* reads TSDB_ECCOLD_MUTANT once (the settle and the open of a settling engine call it, so the TTL pass's mutant
 * works before the first settle) */
void eccold_mutant_init(void);

/* ---- tests (tests/test_eccold.c) ---- */
/* What the last leader settle of any engine decided (eccold_test_last copies it): the log end L after lw_enter, F, Z,
 * the stripe version, the key, the marker's position, the number of Y parts and their names, whether the gate
 * flushed the memtable. */
#define ECCOLD_TEST_Y_MAX 16
typedef struct eccold_cut {
    uint64_t l, f, z, stripe_version, pos;
    int64_t key;
    size_t nobjs, ny;
    char y[ECCOLD_TEST_Y_MAX][64];
    bool flushed, gated;
    int rc;
    int64_t hold_us; /* from lw_enter to lw_leave (0: never gated) */
} eccold_cut;
void eccold_test_last(eccold_cut *out);
/* Steps a settle passes, for tests to act at (the hook runs on the settle's thread, no engine lock held but those
 * named): PUT (objects put, nothing taken), GATED (lw_enter done: jr_serial held, the fence up), CUT (Z and Y taken),
 * MARKER (the marker committed, before the swap). */
enum { ECCOLD_STEP_PUT = 1, ECCOLD_STEP_GATED, ECCOLD_STEP_CUT, ECCOLD_STEP_MARKER };
extern void (*_Atomic eccold_test_hook)(struct eng *db, int step);
/* Planted bugs (tests/test_eccold.c --mutant NAME, or TSDB_ECCOLD_MUTANT=NAME): cutnogate: Y and Z taken without
 * lw_enter (no fence, no jr_serial); cutnowait: Z without waiting for the commit point to pass L; ttlsettling: the TTL
 * pass takes a key with a settling part in a cluster; cutnorotate: no flush at the cut (F inside the active segment,
 * the key's memtable rows in no part); fencekeyonly: the fence covers only the buckets of the series in the settled
 * parts; stripewaitfenced: the wait for the positions runs under the fence. */
enum { ECCOLD_MUT_NONE = 0, ECCOLD_MUT_CUTNOGATE, ECCOLD_MUT_CUTNOWAIT, ECCOLD_MUT_TTLSETTLING,
       ECCOLD_MUT_CUTNOROTATE, ECCOLD_MUT_FENCEKEYONLY, ECCOLD_MUT_STRIPEWAITFENCED };
extern int eccold_mutant;
/* the number of files one marker may name in place of MK_FILES_MAX (0: MK_FILES_MAX), for test_eccold's toomany */
extern _Atomic size_t eccold_test_files_max;
/* What the next markers carry (test_ecapply; a test hook, never set by the engine): the files in reverse order (the
 * apply must number them by the leader's blocks, not by the marker's order), or one field damaged for the replica's
 * checks to refuse: Z one above the object records' ppos, the second .ec file named as the first, Y's header check. */
enum { ECCOLD_TM_NONE = 0, ECCOLD_TM_REVERSE, ECCOLD_TM_Z, ECCOLD_TM_DUPEC, ECCOLD_TM_YHC };
extern _Atomic int eccold_test_marker;
int eccold_mutant_parse(const char *name); /* -1 when unknown */

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
