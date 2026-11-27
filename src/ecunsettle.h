/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Reverse intent and eng_unsettle (R15 r15-reverse, D14; r8-from-r7 item 7).
 *
 * Puts local .part files back in place of settled .ec parts for one partition key, preserving each part's exact
 * name, block range and level (rank unchanged), and retiring the .ec parts via intent v5 (`L` and `U` lines,
 * src/ecswap.c).
 *
 * Per .ec part P of the key:
 *   - Source 1 (fast path): `settled_P.part` when present and verified (header, index, tombstones, footer hash,
 *     granule layout, every granule's xxh64 checksum, zero padding, and index hash matching P.ec).
 *   - Source 2 (decode path): when `settled_P.part` is absent or fails verification (e.g. rot / flipped byte),
 *     decode the object through the tier seam into `tmp_<pid>_<n>.part`, verifying encode-time cell sums,
 *     object byte digest (`object_xxh64`), and full `.part` footer/granule integrity, then fsync before swap.
 *
 * Used by `tsdb DIR volume diverged --clear` on single-server volumes (clearing DIVERGED only after the unsettle
 * swap is durable on disk) and by R10's MK_UNSETTLE.
 *
 * Builds with and without TSDB_EC: without TSDB_EC, eng_unsettle refuses cleanly with TSDB_ERR_INVALID.
 */
#ifndef TSDB_ECUNSETTLE_H
#define TSDB_ECUNSETTLE_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

struct eng;
struct mk;

/* Progress counters exported for r10-driver (`ec -> replicas=3`) and observability. */
typedef struct ecunsettle_progress {
    uint64_t keys_done;
    uint64_t objs_done;
    uint64_t bytes_done;
    uint64_t keys_failed;
    uint64_t from_settled;
    uint64_t from_decode;
} ecunsettle_progress;

/* Collects distinct partition keys that currently have at least one listed (non-dropped, non-test-mark) .ec part
 * in `db`, sorted ascending. Caller frees *keys_out. 0 on success (*nkeys_out == 0 if none). */
int eng_ec_keys(struct eng *db, int64_t **keys_out, size_t *nkeys_out);

/* Unsettles every listed .ec part of `partition_key` in `db`: puts P.part back in P.ec's place with the same
 * name, block range and level, and retires P.ec (`U` and `L` intent v5, src/ecswap.c).
 * On a cluster EC leader (db->ec_cluster && !db->rp && eng_jr(db)): decodes/prepares local .part files, appends
 * and waits for commit of MK_UNSETTLE{partition_key, oids[]} under jr_serial, then swaps via ec_swap_unsettle_at
 * so MK_GC waits until every log replica has applied MK_UNSETTLE before deleting chunks.
 * Optional out-parameters report objects and bytes unsettled for this key (NULL to ignore).
 * 0: all .ec parts of `partition_key` are now local .part files (or there were none).
 * TSDB_ERR_READONLY: read-only, follower, or replica engine.
 * TSDB_ERR_LOCKED: a part of `partition_key` is already settling, re-encoding or merging, or retired_ awaits GC.
 * TSDB_ERR_CORRUPT: decoded object failed sum/digest/part verification.
 * TSDB_ERR_UNAVAILABLE: no tier or too many stores down to decode when settled_ is absent/corrupt.
 * TSDB_ERR_STOPPED: partition was dropped concurrently before the swap took db->lock.
 * TSDB_ERR_INDETERMINATE: cluster leader was deposed while committing MK_UNSETTLE (nothing swapped).
 * TSDB_ERR_INVALID: built without TSDB_EC, non-EC replicated volume, unattached cluster volume, or invalid handle.
 * Caller holds no engine locks. */
int eng_unsettle_key(struct eng *db, int64_t partition_key, uint64_t *objs_out, uint64_t *bytes_out);
int eng_unsettle(struct eng *db, int64_t partition_key);

/* Replica apply of a committed MK_UNSETTLE (called from src/replica.c apply_marker with rp->apply_mu and rp->mu held).
 * Drops rp->mu across slow decode I/O via replica_mu_drop/replica_mu_retake (returning RP_AGAIN on cut), verifies
 * cell sums, object_xxh64 and full part/granule integrity, and swaps .ec -> .part via ec_swap_unsettle_at.
 * Returns 0 only after the swap is durable on disk (or if all named oids are already absent: idempotent replay). */
int ec_unsettle_apply(struct eng *db, uint64_t pos, const struct mk *m);

/* Reads process-wide unsettle progress counters. */
void ecunsettle_get_progress(ecunsettle_progress *out);

/* Initializes TSDB_ECUNSETTLE_MUTANT once per process (TSDB_TEST_HOOKS only). */
void ecunsettle_env_init(void);

/* `tsdb DIR volume diverged [PATH] --clear` on a single-server volume: verifies volume/META/LOCK/DIVERGED,
 * unsettles any diverged keys that still have .ec parts in `vol_dir` via eng_unsettle, and clears the durable
 * DIVERGED file ONLY AFTER every unsettle swap is durable on disk. Returns the number of cleared records (>= 0)
 * or a negative TSDB_ERR_* code. */
int ec_unsettle_diverged_cli(const char *root_dir, const char *vol_dir, FILE *out);

/* Telemetry counters (tests: verify which source path was taken). */
extern atomic_uint_fast64_t ecunsettle_from_settled;
extern atomic_uint_fast64_t ecunsettle_from_decode;

/* Crash hooks (tests/server: wired in server/repld.c):
 *   ecunsettle_crash_hook:        replica in ec_unsettle_apply after decode, before durable swap (CP_UNSETTLE_MID_APPLY)
 *   ecunsettle_leader_crash_hook: leader right after MK_UNSETTLE commits, before its swap (CP_UNSETTLE_AFTER_MARKER) */
extern void (*_Atomic ecunsettle_crash_hook)(struct eng *db);
extern void (*_Atomic ecunsettle_leader_crash_hook)(struct eng *db);

/* Test override for replica apply transient-UNAVAILABLE retry budget in ms (-1: default 30000 ms; 0: no retry). */
extern _Atomic int64_t ecunsettle_test_retry_ms;

/* Planted bugs (tests/test_ecunsettle.c --mutant NAME, TSDB_ECUNSETTLE_MUTANT=NAME in tsdbd / test_ecapply):
 *   unsettlenodecodecheck: skips verifying the decoded part's sums, byte digest and granule checksums
 *   unsettlerank:          allocates new block numbers instead of preserving P's rank
 *   unsettleleaderonly:    replicas skip applying MK_UNSETTLE (Makefile unsettle-mutants requires it to fail)
 *   unsettlenogcgate:      drops the lead_applied_floor >= need_applied check in MK_GC (unsettle-mutants)
 *   unsettlenofloorclamp:  drops the REPL.floor clamp for in-flight leader MK_UNSETTLE (unsettle-mutants)
 *   unsettlesnapnohold:    drops the snap_make SNAP_RETRY hold for in-flight leader MK_UNSETTLE (unsettle-mutants) */
enum {
    ECUNSETTLE_MUT_NONE = 0,
    ECUNSETTLE_MUT_NODECODECHECK,
    ECUNSETTLE_MUT_RANK,
    ECUNSETTLE_MUT_LEADERONLY,
    ECUNSETTLE_MUT_NOGCGATE,
    ECUNSETTLE_MUT_NOFLOORCLAMP,
    ECUNSETTLE_MUT_SNAPNOHOLD,
};
extern int ecunsettle_mutant;
int ecunsettle_mutant_parse(const char *name);

static inline int ecunsettle_mut(void)
{
#ifdef TSDB_TEST_HOOKS
    return ecunsettle_mutant;
#else
    return 0;
#endif
}

#endif
