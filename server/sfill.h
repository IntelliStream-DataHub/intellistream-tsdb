/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 stripe fill worker and startup/return reconcile API (docs/design-cluster-data.md §5.2, §5.3; R9 plan D10, D11,
 * D15; tracks r9-fill, r9-whole, r9-reconcile).
 *
 * When a member takes over a stripe position `(volume, pos)` at `stripe_version` V in state `filling`:
 *   - new settles at version >= V put their position-`pos` chunks directly onto this member;
 *   - sfill fetches the volume leader's committed object list (OBJLIST, objects with version < V), copies from
 *     `prev_node` when UP and sum-verified (1x) or decodes whole cells from k survivors over PL_KIND_CHUNKR (kind 8),
 *     verifies every rebuilt cell's sum, writes atomically at the object's writer epoch, and reports progress via
 *     `sfill_report` (sent in heartbeat section HBS_FILL). Once every object below V is verified, `whole` becomes 1
 *     so the Raft leader commits STRIPE_WHOLE(volume, pos, V) and releases the prior holder via committed MK_GC.
 *
 * On member start or return (sreconcile.c), CHUNK_LIST is compared both ways against the leader's OBJLIST: missing
 * chunks (degraded settles, fsck, replaced stores) are queued into sfill at their margin's priority, while retired
 * objects are left to committed MK_GC.
 *
 * Stub implementation until r9-fill, r9-whole and r9-reconcile fill it in.
 */
#ifndef TSDB_SFILL_H
#define TSDB_SFILL_H

#include <stddef.h>
#include <stdint.h>

struct sfill;

/* Parameters to start or update a fill job for one (volume, pos). */
struct sfill_job_cfg {
    uint32_t volume;
    uint32_t pos;
    uint64_t stripe_version; /* barrier version V (D10) */
    uint32_t prev_node;      /* previous holder for 1x copy attempt (0: none) */
    uint32_t leader;         /* volume leader for OBJLIST */
    uint8_t ec_k;
    uint8_t ec_m;
    int margin;              /* current volume margin for priority & pacing (E13) */
    uint32_t concurrency;    /* objects in flight (0: default rebuild_concurrency = 4) */
};

/* Snapshot of one active or completed fill job, carried in HBS_FILL. */
struct sfill_report {
    uint32_t volume;
    uint32_t pos;
    uint64_t stripe_version;
    uint64_t done;
    uint64_t total;
    uint8_t whole;
};

/* Lifecycle of the per-server fill worker pool. */
struct sfill *sfill_new(void);
void sfill_free(struct sfill *sf);

/* Start (or update stripe_version/leader/margin of) a fill job for (volume, pos). 0 on success, -1 on error. */
int sfill_start(struct sfill *sf, const struct sfill_job_cfg *cfg);

/* Stop and remove any fill job running for (volume, pos). Safe if none is running. */
void sfill_stop(struct sfill *sf, uint32_t volume, uint32_t pos);

/* Collect up to `cap` current fill reports into `out` (non-blocking, called on the cluster heartbeat path).
 * Returns the number of entries written. */
uint32_t sfill_report(struct sfill *sf, struct sfill_report *out, uint32_t cap);

/* Startup / return reconciliation (server/sreconcile.c; r9-reconcile, D15). */
int sreconcile_trigger(struct sfill *sf, uint32_t volume, uint32_t pos, uint64_t stripe_version);
void sreconcile_on_start(struct sfill *sf);

/* Record a missing chunk position from a degraded settle (E10, r9-degsettle) for r9-reconcile to fill when its
 * holder returns or goes OUT. Thread-safe; records in a bounded in-memory ring and logs (not durable across restarts:
 * r9-reconcile rebuilds missing positions from committed .ec records' `positions` bitmaps on start/return). */
void sreconcile_note_missing(uint32_t volume, const uint8_t oid[16], unsigned pos);
uint64_t sreconcile_missing_count(void);

#endif
