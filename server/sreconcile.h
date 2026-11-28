/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 startup and return chunk reconciler (docs/design-cluster-data.md §5.2, E10, E14, E27, E28; R9 plan D14, D15;
 * track r9-reconcile).
 *
 * On every member start and every return (UP after DOWN, OUT -> UP, or lease-lapse reconnect), per stripe position
 * this member holds in WHOLE state:
 *   1. Checks local chunk stores against expected store ids (persisted in <state-dir>/chunk_stores with an XXH64
 *      checksum trailer; read/parse/checksum errors log loudly and refuse to overwrite):
 *        - a store answering with another store id (writable replacement) is logged once with the store path and both
 *          ids; every chunk placed on it is treated as missing and forced through sfill, and the new store id is
 *          committed to <state-dir>/chunk_stores only after all affected positions finish reconciling cleanly;
 *        - a non-writable DOWN store (or one whose on-disk META diverged from its open handle while running) is logged
 *          once with path and ids; chunks placed on it are counted as missing-on-down and wait for store recovery or
 *          restart (never re-streaming OBJLIST or spamming sfill).
 *   2. Streams full OBJLIST records (max_version = 0, with sums) page-by-page against local CHUNK_LIST (keeping
 *      compact 16-byte leader oids and local CHUNK_LIST metadata for Direction 2):
 *        - missing / damaged / wrong-epoch / replaced-store chunks (excluding oids already on committed MK_GC) are
 *          handed to sfill's priority worker queue (sfill_enqueue_obj) at the volume's margin priority, with a cancel
 *          predicate if the position moves away mid-pass;
 *        - local chunks of oids absent from OBJLIST but on the leader's committed MK_GC pending list are left to
 *          r8k's pending-delete worker (never deleted directly here);
 *        - local chunks of oids neither in OBJLIST nor on the pending MK_GC list (and confirmed still present on disk
 *          after the GC check) are reported as orphans (left to r8k's E14 orphan sweep, never deleted here).
 *   3. Publishes active reconcile progress in HBS_FILL (same struct sfill_report format, version = current
 *      stripe_version).
 *   4. `sreconcile_note_missing` (hooked by r9-degsettle) queues degraded objects' positions for their holders' next
 *      return; the reconciler's own coordinator thread nudges returning holders without blocking the sfill watch loop.
 */
#ifndef TSDB_SRECONCILE_H
#define TSDB_SRECONCILE_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "sfill.h"

struct sreconcile;

/* Store snapshot returned by sreconcile_ops.store_info. */
struct sreconcile_store_info {
    char path[PATH_MAX];
    int state;           /* CHUNK_STORE_* */
    uint8_t live_id[16]; /* store_id of open chunk_store (zeroed if not UP) */
    uint8_t disk_id[16]; /* store_id currently on disk META (zeroed if unreadable) */
    bool disk_ok;        /* true if META on disk parsed cleanly */
    bool is_out;         /* R13 r13-dev: true if store is committed OUT/DESTROYED (rebuild lost chunks locally) */
    bool is_draining;    /* R13 r13-lmove/r13-dev: true if store is draining (regular puts skip to another store) */
};

/* Current metadata view for one (volume, pos) on this member. */
struct sreconcile_vol_view {
    bool held_whole;         /* true iff this server currently holds (volume, pos) in META_STRIPE_WHOLE */
    uint64_t stripe_version; /* current stripe_version */
    uint32_t leader;         /* volume leader node id (0: none) */
    int margin;              /* current volume settled margin (E13) */
    uint32_t concurrency;    /* rebuild_concurrency (0: default) */
};

/* One WHOLE stripe position held by this server (returned by sreconcile_ops.list_held). */
struct sreconcile_held_pos {
    uint32_t volume;
    uint32_t pos;
    uint64_t stripe_version;
};

/* Cluster/storage operations injected into sreconcile (real cluster glue in chunkpeer_cluster.c, mocks in
 * tests/test_sreconcile.c). All callbacks are invoked with no sreconcile lock held. */
struct sreconcile_ops {
    /* Paged OBJLIST(volume, max_version = 0, cursor_oid) from the volume leader. */
    int (*objlist)(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                   const uint8_t cursor_oid[16], uint32_t max_entries,
                   uint8_t vuuid_out[16], struct sfill_obj **items_out, uint32_t *n_out, bool *more_out);

    /* Paged local CHUNK_LIST for `volume`. Returns 0 with malloc'ed *items_out (caller frees), or TSDB_ERR_*. */
    int (*local_list)(void *arg, uint32_t volume, bool has_cursor, const uint8_t cursor_oid[16],
                      uint16_t cursor_pos, uint32_t max_entries, chunk_listed **items_out, uint32_t *n_out,
                      bool *more_out);

    /* Optional fast check whether (volume, oid, pos) still exists on disk locally (re-checks after gc_pending
     * to avoid false orphan reports when leader GC deletes a chunk concurrently). */
    bool (*local_exists)(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos);

    /* Leader's committed MK_GC pending-delete oids for `volume` (also kicks leader GC).
     * Returns 0 with malloc'ed *oids_out (caller frees), *n_out, and *truncated_out, or TSDB_ERR_UNAVAILABLE.
     * Optional (may be NULL). */
    int (*gc_pending)(void *arg, uint32_t volume, uint8_t (**oids_out)[16], uint32_t *n_out, bool *truncated_out);

    /* Current metadata state for (volume, pos) on this server. Returns 0 on success, -1 if unavailable. */
    int (*vol_view)(void *arg, uint32_t volume, uint32_t pos, struct sreconcile_vol_view *out);

    /* Enumerate all WHOLE stripe positions currently held by this server.
     * Returns 0 with malloc'ed *out (caller frees) and *n_out, or -1 if unavailable. */
    int (*list_held)(void *arg, struct sreconcile_held_pos **out, uint32_t *n_out);

    /* Local chunk store inspection and placement (optional; may be NULL if single/virtual store). */
    int (*store_count)(void *arg);
    int (*store_info)(void *arg, int idx, struct sreconcile_store_info *out);
    int (*place_store)(void *arg, const uint8_t oid[16], unsigned pos);
    /* R13 r13-dev: pick a local target store for chunks lost on OUT store `lost_slot` (-1 if none has room). Optional. */
    int (*pick_target_store)(void *arg, int lost_slot);
    /* R13 r13-dev: invoked once all held positions have reconciled cleanly for CHUNK_STORE_REPLACING slots. Optional. */
    void (*on_replacing_settled)(void *arg, uint64_t settled_mask);

    /* Nudge the current holder of (volume, pos) after a degraded settle (sreconcile_note_missing).
     * Returns 1 when dispatched or no longer needed (remove from pending queue), 0 to keep waiting. Optional. */
    int (*notify_holder)(void *arg, uint32_t volume, uint32_t pos);

    /* Optional progress notification callback whenever reconcile reports change (merged into HBS_FILL). */
    void (*on_report)(void *arg, const struct sfill_report *reps, uint32_t nreps);

    void *arg;
};

/* Counters for diagnostics and unit tests. */
struct sreconcile_stats {
    uint64_t triggers;
    uint64_t passes_ok;
    uint64_t passes_err;
    uint64_t missing_queued;
    uint64_t missing_on_down_store;
    uint64_t filled;
    uint64_t already_present;
    uint64_t gc_pending;
    uint64_t orphans_reported;
    uint64_t store_id_mismatches;
    uint64_t store_down_events;
    uint64_t bytes_filled;
    uint64_t failed;
    uint64_t holder_nudges;
};

/* Lifecycle. Shutdown order when sharing `sf`: call sreconcile_stop(rec), then sfill_free(sf), then
 * sreconcile_free(rec) — or call sreconcile_free(rec) while `sf` is still running (waits for inflight tasks). */
struct sreconcile *sreconcile_new(struct sfill *sf, const struct sreconcile_ops *ops, const char *state_dir);
void sreconcile_stop(struct sreconcile *rec);
void sreconcile_free(struct sreconcile *rec);

/* Trigger reconciliation for one (volume, pos) at `stripe_version` (coalesced if already queued). 0 or -1. */
int sreconcile_trigger_ex(struct sreconcile *rec, uint32_t volume, uint32_t pos, uint64_t stripe_version);
/* Check stores and trigger reconciliation for every WHOLE stripe position held by this server (on start / return). */
void sreconcile_on_start_ex(struct sreconcile *rec);
void sreconcile_on_return(struct sreconcile *rec);
/* Stop/clear any active reconcile state for (volume, pos) (e.g., when entering FILLING or moving away). */
void sreconcile_stop_pos(struct sreconcile *rec, uint32_t volume, uint32_t pos);
/* Non-blocking wakeup for the reconciler thread to drain degraded-settle nudges (safe on watch threads). */
void sreconcile_kick_noted(struct sreconcile *rec);
/* Synchronously drain degraded-settle queue once (for unit tests; production runs this on reconcile's own thread). */
void sreconcile_drain_noted(struct sreconcile *rec);

/* Non-blocking snapshot of active reconcile progress reports (for HBS_FILL, version = current stripe_version). */
uint32_t sreconcile_report(struct sreconcile *rec, struct sfill_report *out, uint32_t cap);
void sreconcile_get_stats(struct sreconcile *rec, struct sreconcile_stats *out);

/* Planted bugs (TSDB_SRECONCILE_MUTANT env var or sreconcile_set_mutant; tests/test_sreconcile.c, make
 * reconcile-mutants). */
enum sreconcile_mutant {
    SRECONCILE_MUT_NONE = 0,
    SRECONCILE_MUT_FULLCOPY = 1,    /* backfullcopy: ignores CHUNK_LIST and re-fills every object in OBJLIST */
    SRECONCILE_MUT_SKIPMISSING = 2, /* backskipmissing: skips queuing missing chunks discovered by the diff */
};
extern _Atomic int sreconcile_mutant;
int sreconcile_mutant_parse(const char *name);
void sreconcile_set_mutant(int mut);

#ifdef TSDB_TEST_HOOKS
extern int sreconcile_test_retry_ms; /* 0: default (300 ms) */
void sreconcile_test_reset_missing(void);
#endif

#endif
