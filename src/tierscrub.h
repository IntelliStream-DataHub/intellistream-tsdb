/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local tier background chunk scrubber and repair (R15 r15-scrub; docs/design-cluster-data.md E25, §15.7;
 * PLAN.md D12, D20).
 *
 * Runs on the tierctl nice-19 worker thread (skipped on read-only follower tiers):
 *  - Scrubs each active UP store every `scrub_days` (default 14), oldest first, paced at `scrub_mb_s` (default 50 MB/s)
 *    via a virtual-time token bucket without burst accumulation across pauses.
 *  - Calls chunk_be_heal with scrub = true and `live` from the registered engines' live .ec records (never repairing
 *    non-live/deleted objects). Damaged data or parity cells on EC stores (m > 0) are rebuilt from k verified
 *    survivors and checked against encode-time sums; eccache handles are reopened after any repair.
 *  - Single-copy stores (m == 0, --single-copy): report only (increment damaged & unrepairable counters, log a clear
 *    line naming store, object and partition; never modify or delete the chunk).
 *  - Persists per-store progress and counters in `<store>/SCRUB` atomically (SCRUB.tmp -> fsync -> rename -> dir fsync
 *    on an independent dirfd) so a restart resumes from the saved cursor instead of starting over.
 *  - Yields/pauses promptly whenever a rebuild, shrink, health attach, or TIERS update is active/pending on the tier.
 */
#ifndef TSDB_TIERSCRUB_H
#define TSDB_TIERSCRUB_H

#include "tiers.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct chunk_heal_report;
struct tier;

#define TIERSCRUB_FILE "SCRUB"

typedef struct tierscrub_store_stats {
    int store;                /* backend store index (or -1 when loaded offline by path) */
    uint64_t chunks_checked;  /* total chunks checked */
    uint64_t damaged;         /* total damaged chunks detected (repaired + unrepairable + failed) */
    uint64_t repaired;        /* damaged chunks repaired cleanly */
    uint64_t unrepairable;    /* damaged chunks that could not be repaired (m = 0 or > m bad) */
    uint64_t bytes_checked;   /* total chunk bytes read and checksummed */
    uint64_t passes_done;     /* completed whole passes */
    uint64_t pass_start_s;    /* UTC wall seconds when current pass started */
    uint64_t last_pass_s;     /* UTC wall seconds when last completed pass started (0 if none) */
    uint64_t first_pass_s;    /* UTC wall seconds when first pass started */
    uint64_t oldest_age_s;    /* age in seconds of oldest unscrubbed object/pass */
    bool cursor_valid;        /* true while a pass is in progress */
    uint32_t cursor_vol;
    uint8_t cursor_oid[16];
    bool has_data;            /* true if SCRUB existed or scrub has initialized this store */
} tierscrub_store_stats;

typedef struct tierscrub_snapshot {
    bool active;              /* currently scrubbing a store right now */
    bool paused;              /* paused because rebuild/shrink/attach is active */
    int active_store;         /* backend store index being scrubbed, or -1 */
    uint32_t scrub_mb_s;
    uint32_t scrub_days;
    uint64_t oldest_age_s;    /* max oldest_age_s across active UP stores */
    bool has_oldest_age;
    int nstores;
    tierscrub_store_stats stores[TIERS_STORES_MAX];
} tierscrub_snapshot;

/* Lifecycle (invoked from tierctl_open / tierctl_close; no-op on read-only tiers). */
int tierscrub_open(struct tier *t);
void tierscrub_close(struct tier *t);

/* Notify scrub when a backend store slot is added or replaced (resets in-memory state and loads its SCRUB file). */
void tierscrub_on_store_changed(struct tier *t, int be_idx, const char *path);

/* Runs at most one store scrub slice on the tierctl nice-19 worker when no rebuild/shrink/attach is pending.
 * Returns true if work was performed or yielded (worker should re-check high-priority work immediately),
 * false if nothing is due now (updating *wait_ms_out with the time until the next store is due). */
bool tierscrub_step(struct tier *t, int64_t *wait_ms_out);
void tierscrub_set_paused(struct tier *t, bool paused);
void tierscrub_quiesce(struct tier *t);

/* Thread-safe, zero-I/O snapshot of live per-store counters and oldest unscrubbed age (for /metrics and tests). */
bool tierscrub_get_snapshot(const struct tier *t, tierscrub_snapshot *out);

/* Reads `<store_path>/SCRUB` directly from disk for offline/CLI `tier status`.
 * Returns 0 if a valid SCRUB file was parsed (*out populated with has_data = true), TSDB_ERR_NOTFOUND if absent,
 * or TSDB_ERR_CORRUPT / TSDB_ERR_IO if unreadable/malformed. */
int tierscrub_read_store_file(const char *store_path, tierscrub_store_stats *out);

/* Test helpers (active only under TSDB_TEST_HOOKS; clean no-ops when disabled). */
enum {
    TIERSCRUB_MUT_NONE = 0,
    TIERSCRUB_MUT_NOREPAIR, /* scrubnorepair: finds damage, increments damaged, does not repair */
    TIERSCRUB_MUT_NOPACE,   /* scrubnopace: ignores scrub_mb_s pacing */
};
extern int tierscrub_mutant;
int tierscrub_mutant_parse(const char *name);

typedef void (*tierscrub_obj_hook_fn)(void *ctx, int store, uint32_t vol, const uint8_t oid[16],
                                      const struct chunk_heal_report *delta);
void tierscrub_set_test_opts(uint32_t interval_ms, uint64_t rate_bps, uint32_t save_every);
void tierscrub_set_wall_fn(uint64_t (*fn)(void *ctx), void *ctx);
void tierscrub_set_obj_hook(tierscrub_obj_hook_fn fn, void *ctx);
void tierscrub_set_test_pause(bool paused);
int tierscrub_wait_pass(struct tier *t, int store, uint64_t want_passes, int timeout_ms);

#endif /* TSDB_TIERSCRUB_H */
