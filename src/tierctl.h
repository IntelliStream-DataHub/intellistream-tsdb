/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local tier controller: single background tier worker per handle (nice 19, never holding engine locks across
 * chunk I/O), periodic TIERS re-read, paced replacement rebuilds, crash/power-loss resume via durable REBUILDING
 * marker, and coordination with r15-health's queued attaches (docs/design-cluster-data.md §15.7, E15, E55; R15
 * D9, D10, D20).
 */
#ifndef TSDB_TIERCTL_H
#define TSDB_TIERCTL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct eng;
struct tier;
struct tiers_view;

#define TIERCTL_REBUILDING_FILE "REBUILDING"

typedef struct tierctl_progress {
    bool active;
    int store;              /* store index currently rebuilding, or -1 when idle */
    uint64_t bytes_done;    /* chunk bytes written so far in the active (or last) rebuild */
    uint64_t bytes_est;     /* estimated total chunk bytes for this store */
    uint64_t objects_done;  /* objects processed in last completed pass */
    uint64_t rebuilt_chunks;/* chunks rebuilt in last completed pass */
    uint32_t rate_mb_s;     /* active rebuild_mb_s pace cap */
    uint64_t rebuilds_done; /* total rebuilds completed by this handle */
    /* R15 r15-shrink progress */
    bool shrink_active;
    int shrink_store;            /* TIERS store index currently shrinking, or -1 when idle */
    uint64_t shrink_objects_left;/* live .ec objects still remaining to re-encode off shrinking store */
    uint64_t shrink_objects_done;/* live .ec objects re-encoded by shrink so far */
    uint64_t shrinks_done;       /* total shrinks completed (marked REMOVED) by this handle */
} tierctl_progress;

/* Lifecycle (called from src/tier.c beside tier_health). */
int tierctl_open(struct tier *t);
void tierctl_eng_stop(struct eng *db); /* waits only if shrink is currently re-encoding `db` */
void tierctl_quiesce(struct tier *t);
bool tierctl_close(struct tier *t); /* returns true if a detached worker still references t */

/* Wakes the tier worker immediately (e.g., after an in-process tsdb_tier_lost or health attach). */
void tierctl_wake(struct tier *t);

/* Snapshots live rebuild progress for tier status. Returns false if no tierctl is attached. */
bool tierctl_get_progress(const struct tier *t, tierctl_progress *out);

/* True if `<store_path>/REBUILDING` exists on disk (replacement rebuild not yet durably finished). */
bool tierctl_store_rebuilding(const char *store_path);

/* Counts live .ec objects across `vol_paths[0..nvols-1]` that still need re-encoding onto the remaining active stores
 * listed in `tv` (used by `tier status` while a store is in TIERS_STATE_SHRINKING). Works offline or beside tsdbd. */
int tierctl_count_shrink_objects(const struct tiers_view *tv, const char *const *vol_paths, size_t nvols,
                                 uint64_t *objects_out);

/* Waits until no TIERS reload or replacement rebuild is pending (0) or timeout expires (TSDB_ERR_UNAVAILABLE). */
int tierctl_wait_idle(struct tier *t, int timeout_ms);

/* Helpers used by src/tierscrub.c running on the tierctl nice-19 worker thread (R15 r15-scrub). */
bool tierctl_is_closing(const struct tier *t);
bool tierctl_scrub_should_yield(struct tier *t);
bool tierctl_wait_interruptible_ms(struct tier *t, uint64_t wait_ms);
uint32_t tierctl_scrub_mb_s(const struct tier *t);
uint32_t tierctl_scrub_days(const struct tier *t);

/* Records STATE LOST and either:
 *  - replace_path != NULL: validates/initializes the replacement store with db_id binding and durable REBUILDING
 *    marker, then appends STATE REPLACED_BY to TIERS under the VOLUMES OFD write lock; or
 *  - replace_path == NULL (--shrink, R15 D9): verifies remaining active stores >= k + m of current global and
 *    per-volume geometry, then appends STATE LOST and STATE SHRINKING to TIERS under the VOLUMES OFD write lock.
 * `store` matches index ("0", "s0"), 32-hex store id, or store path.
 * `opt_class` (NULL inherits lost store's class; must be NULL for --shrink) accepts "unknown", "nvme", "ssd", "hdd". */
int tierctl_lost_impl(int rootfd, const char *root, bool sync, const char *store, const char *replace_path,
                      const char *opt_class, uint32_t flags, uint8_t new_id_out[16], int *slot_out);

/* Same as tierctl_lost_impl without an open tsdb handle (works offline or beside a running tsdbd). */
int tierctl_lost_dir(const char *dir, const char *store, const char *replace_path, const char *opt_class,
                     uint32_t flags, uint8_t new_id_out[16], int *slot_out);

/* Test hooks (active only under TSDB_TEST_HOOKS). */
void tierctl_set_poll_ms(int ms);
void tierctl_set_rebuild_hook(void (*fn)(void *ctx, int store, uint64_t bytes_done), void *ctx);

enum {
    TIERCTL_MUT_NONE = 0,
    TIERCTL_MUT_LOSTNOREPLACE, /* LOST store's positions never rebuilt yet reported UP */
    TIERCTL_MUT_REBUILDFAST,   /* rebuild_mb_s pacing ignored */
    TIERCTL_MUT_SHRINKEARLY,   /* marks STATE REMOVED while a live object still names S */
};
#ifdef TSDB_TEST_HOOKS
extern int tierctl_mutant;
#endif

#endif
