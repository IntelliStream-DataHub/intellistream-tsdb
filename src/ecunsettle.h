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

/* Unsettles every listed .ec part of `partition_key` in `db`: puts P.part back in P.ec's place with the same
 * name, block range and level, and retires P.ec (`U` and `L` intent v5, src/ecswap.c).
 * 0: all .ec parts of `partition_key` are now local .part files (or there were none).
 * TSDB_ERR_READONLY: read-only or follower engine.
 * TSDB_ERR_LOCKED: a part of `partition_key` is already settling, re-encoding or merging.
 * TSDB_ERR_CORRUPT: decoded object failed sum/digest/part verification.
 * TSDB_ERR_UNAVAILABLE: no tier or too many stores down to decode when settled_ is absent/corrupt.
 * TSDB_ERR_STOPPED: partition was dropped concurrently before the swap took db->lock.
 * TSDB_ERR_INVALID: built without TSDB_EC, or invalid handle.
 * Caller holds no engine locks. */
int eng_unsettle(struct eng *db, int64_t partition_key);

/* `tsdb DIR volume diverged [PATH] --clear` on a single-server volume: verifies volume/META/LOCK/DIVERGED,
 * unsettles any diverged keys that still have .ec parts in `vol_dir` via eng_unsettle, and clears the durable
 * DIVERGED file ONLY AFTER every unsettle swap is durable on disk. Returns the number of cleared records (>= 0)
 * or a negative TSDB_ERR_* code. */
int ec_unsettle_diverged_cli(const char *root_dir, const char *vol_dir, FILE *out);

/* Telemetry counters (tests: verify which source path was taken). */
extern atomic_uint_fast64_t ecunsettle_from_settled;
extern atomic_uint_fast64_t ecunsettle_from_decode;

/* Planted bugs (tests/test_ecunsettle.c --mutant NAME; Makefile ecunsettle-mutants requires each to fail):
 *   unsettlenodecodecheck: skips verifying the decoded part's sums, byte digest and granule checksums
 *   unsettlerank:          allocates new block numbers instead of preserving P's rank */
enum {
    ECUNSETTLE_MUT_NONE = 0,
    ECUNSETTLE_MUT_NODECODECHECK,
    ECUNSETTLE_MUT_RANK,
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
