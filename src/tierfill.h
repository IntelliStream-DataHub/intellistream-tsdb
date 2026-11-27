/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Single-server tier FULL hysteresis before ENOSPC (R15 r15-full, design-cluster-data.md E53, §15.7, PLAN.md D7).
 *
 * Each single-server engine attached to a TIERS-configured tier carries an atomic `full` flag:
 *   - seeded synchronously on engine open/attach (tierfill_eng_seed) so a freshly opened or reopened volume on a
 *     >= young_high_pct filesystem refuses inserts immediately;
 *   - kept fresh every <= 1 s by a dedicated per-volume sampler thread (tierfill_eng_start / tierfill_eng_stop) that
 *     never holds t->eng_active across statvfs I/O, never runs settle or chunk-store I/O, isolates each volume from
 *     another volume's NFS hang, and detaches cleanly on close if stuck in the kernel;
 *   - enters FULL when integer fill_pct (floor of used * 100 / total) >= young_high_pct (default 90%), clears only
 *     when fill_pct < young_high_pct - 2;
 *   - the route (src/volumes.c) checks the atomic flag with zero syscalls and refuses inserts, appends, and non-empty
 *     replaces with TSDB_ERR_UNAVAILABLE before any engine call; deletes (including replace n == 0) and drops pass.
 *
 * Naming uses tierfill_* (not tsdb_*) so tools/libtsdb.map keeps all helpers internal to libtsdb.so.
 */
#ifndef TSDB_TIERFILL_H
#define TSDB_TIERFILL_H

#include "internal.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

struct tier;
struct tierfill_worker;

typedef struct tierfill_state {
    atomic_bool full;               /* true while volume fs fill >= young_high_pct (cleared at < young_high_pct - 2) */
    atomic_int_fast64_t since_ms;   /* wall-clock Unix ms when FULL was entered (0 when not full) */
    atomic_uint_fast64_t refusals;  /* writes refused because this volume was FULL */
    struct tierfill_worker *worker; /* per-engine detached-capable 1 s sampler (NULL if not started) */
} tierfill_state;

/* Planted bug (tests/test_tier.c --mutant fullnoflag): route ignores the FULL flag. Active only under TSDB_TEST_HOOKS. */
enum {
    TIERFILL_MUT_NONE = 0,
    TIERFILL_MUT_FULLNOFLAG = 1,
};
extern int tierfill_mutant;

/* Per-engine 1 s volume-fill sampler lifecycle (isolated per volume; never holds t->eng_active across statvfs I/O). */
int tierfill_eng_start(eng *db);
void tierfill_eng_stop(eng *db);

/* Seed FULL state synchronously at engine ready/open before any caller can write to db. */
void tierfill_eng_seed(eng *db);

/* Update db's FULL hysteresis flag from a newly sampled volume filesystem fill % (0..100). Never called on the append
 * path. Logs once on each enter/clear transition. */
void tierfill_update(eng *db, uint32_t fill_pct);

/* Lock-free, syscall-free check of whether db is currently refusing inserts due to FULL. Always false for engines
 * without a tier or on cluster volumes. */
bool tierfill_is_full(const eng *db);

/* Record a FULL refusal on volume `vol` (bumps db's counter and latches thread-local state so tsdbd reports
 * TSDB_WHY_FULL even if FULL clears between the route refusal and reply encoding). */
void tierfill_note_refusal(eng *db, uint32_t vol);
void tierfill_clear_last_refused(void);
bool tierfill_take_last_refused(uint32_t *vol_out);

/* Handle-level lock-free/rdlock queries (implemented in src/volumes.c, no I/O): whether volume `vol` is FULL
 * (optionally filling *since_ms_out with wall-clock ms when FULL started) and its cumulative FULL refusal count. */
bool tierfill_vol_full(tsdb *t, uint32_t vol, int64_t *since_ms_out);
uint64_t tierfill_vol_refusals(tsdb *t, uint32_t vol);

/* Per-store cached filesystem byte snapshot for Prometheus tsdb_tier_bytes{store,use}: reads cached atomics only
 * (zero syscalls, never blocks on a hanging store). Returns 0 on success, -1 if store is DOWN or TSDB_EC is off. */
int tierfill_store_bytes(const struct tier *t, int store, uint64_t *used_out, uint64_t *free_out, uint64_t *total_out);

#endif
