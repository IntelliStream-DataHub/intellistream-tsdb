/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 stripe fill worker and startup/return reconcile API (docs/design-cluster-data.md §5.2, §5.3; R9 plan D10, D11,
 * D15, D18; tracks r9-fill, r9-whole, r9-reconcile).
 *
 * When a member takes over a stripe position `(volume, pos)` at `stripe_version` V in state `filling`:
 *   - new settles at version >= V put their position-`pos` chunks directly onto this member;
 *   - sfill fetches the volume's lease-holding leader's committed object list (CS_OP_OBJLIST, objects with version < V),
 *     copies from `prev_node` when UP and sum-verified (1x) or
 *     decodes whole cells from k other written positions over PL_KIND_CHUNKR (kind 8), verifies every survivor cell and
 *     every rebuilt cell against its encode-time xxh64 sum, writes atomically (tmp -> fsync -> rename -> dir-fsync) at
 *     the object's writer epoch (fenced at the volume's current epoch), reads back and scrubs the written chunk, fires
 *     CP_FILL_WRITTEN, notifies log replicas via CS_OP_REOPEN (`eccache_reopen`), and reports progress via
 *     `sfill_report` (published into HBS_FILL). Once every object below V is verified, `whole` becomes 1 so the Raft
 *     leader commits STRIPE_WHOLE(volume, pos, V) and releases the prior holder via committed MK_GC.
 *
 * Every byte read and written is paced through server/pace.h at the volume's settled margin, and queued object fills
 * run in lowest-margin-first order across a pool of `rebuild_concurrency` worker threads.
 */
#ifndef TSDB_SFILL_H
#define TSDB_SFILL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../src/chunk.h"

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
    uint64_t failures;
};

/* Active stripe fill byte progress per (volume, pos) for HBS_PROGRESS (R14). */
struct sfill_progress {
    uint32_t volume;
    uint32_t pos;
    uint64_t bytes_done;
    uint64_t bytes_total;
    uint64_t started_ms;       /* Unix epoch ms */
    uint64_t last_progress_ms; /* Unix epoch ms */
};

/* One committed .ec object returned by OBJLIST. */
struct sfill_obj {
    uint8_t oid[16];
    chunk_geom g;
    uint32_t positions;      /* written-positions bitmap (0 = all k'+m; E10) */
    uint64_t writer_epoch;
    uint64_t stripe_version;
    uint64_t nsums;
    uint64_t *sums;          /* nsums entries, malloc'ed */
};

void sfill_objs_free(struct sfill_obj *objs, uint32_t n);

/* Transport/cluster operations injected into sfill (real cluster glue in chunkpeer_cluster.c, fake source in
 * tests/test_sfill.c). All callbacks are invoked from sfill's coordinator/worker threads with no sfill lock held. */
struct sfill_ops {
    /* Paged OBJLIST(volume, stripe_version < max_version, cursor_oid).
     * Returns 0 with vuuid_out[16], *items_out (sfill frees via sfill_objs_free), *n_out, *more_out, or TSDB_ERR_*. */
    int (*objlist)(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                   const uint8_t cursor_oid[16], uint32_t max_entries,
                   uint8_t vuuid_out[16], struct sfill_obj **items_out, uint32_t *n_out, bool *more_out);

    /* Read `len` bytes from cell area of (volume, vuuid, oid, pos) at `off` on member `node` over PL_KIND_CHUNKR (8).
     * Returns CHUNK_ST_OK (0), or CHUNK_ST_ABSENT / CHUNK_ST_DAMAGED / CHUNK_ST_IOERR / CHUNK_ST_DOWN. */
    int (*read_cell)(void *arg, uint32_t node, uint32_t volume, uint64_t epoch,
                     const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos,
                     uint64_t off, void *buf, size_t len);

    /* Snapshot the volume's current epoch, stripe[0..nstripe-1], and whether prev_node is UP (for 1x copy).
     * Returns 0 on success, -1 if unavailable. */
    int (*vol_view)(void *arg, uint32_t volume, uint64_t *epoch_out,
                    uint32_t stripe_out[CHUNK_POS_MAX], unsigned *nstripe_out,
                    bool *prev_up_out, uint32_t prev_node);

    /* Hold/release the local chunk_backend for durable writes and read-back verification. */
    chunk_backend *(*local_be_hold)(void *arg);
    void (*local_be_release)(void *arg, chunk_backend *be);

    /* Best-effort notify log replicas of `volume` to call eccache_reopen(tier, volume, oid). Optional (may be NULL). */
    void (*notify_reopen)(void *arg, uint32_t volume, const uint8_t oid[16]);

    /* Optional progress notification callback whenever fill reports change (e.g. cluster_set_fills). */
    void (*on_report)(void *arg, const struct sfill_report *reps, uint32_t nreps);

    void *arg;
};

/* Counters for diagnostics and unit tests. */
struct sfill_stats {
    uint64_t copied;
    uint64_t decoded;
    uint64_t already_held;
    uint64_t skipped_small;
    uint64_t failed;
    uint64_t unavailable;
    uint64_t corrupt;
    uint64_t survivor_bad;
    uint64_t reopened;
    uint64_t bytes_read;
    uint64_t bytes_written;
};

/* Lifecycle of the per-server fill worker pool. */
struct sfill *sfill_new(void);
struct sfill *sfill_new_with_ops(const struct sfill_ops *ops);
void sfill_free(struct sfill *sf);

/* Start (or update stripe_version/leader/margin/concurrency of) a fill job for (volume, pos). 0 on success, -1 error. */
int sfill_start(struct sfill *sf, const struct sfill_job_cfg *cfg);

/* Stop and remove any fill job running for (volume, pos). Safe if none is running. */
void sfill_stop(struct sfill *sf, uint32_t volume, uint32_t pos);

/* Enqueue one specific missing object onto sfill's priority worker pool (server/sreconcile.c, D15).
 * Runs the checked copy/decode + read-back + notify_reopen pipeline at `margin`'s priority without creating a
 * FILLING job or touching sfill's whole-barrier state. Takes ownership of `obj->sums` (setting it to NULL) on 0.
 * If `force_write` is true, bypasses the local_chunk_intact shortcut. Optional `cancel_cb(done_arg)` is polled
 * without holding sf->mu before and during execution so a stopped reconcile job aborts queued/in-flight work.
 * Calls `done_cb(done_arg, rc)` on completion, cancellation, or shutdown drop (rc == 0 on verified durable write,
 * non-zero otherwise). Returns 0 on success, -1 on error. */
int sfill_enqueue_obj(struct sfill *sf, uint32_t volume, uint32_t pos, uint32_t prev_node, int margin,
                      uint32_t concurrency, const uint8_t vuuid[16], struct sfill_obj *obj, bool force_write,
                      bool (*cancel_cb)(void *arg), void (*done_cb)(void *arg, int rc), void *done_arg);

/* Collect up to `cap` current fill reports into `out` (non-blocking, called on the cluster heartbeat path).
 * Returns the number of entries written. */
uint32_t sfill_report(struct sfill *sf, struct sfill_report *out, uint32_t cap);
/* Collect up to `cap` active (non-whole) fill byte-progress entries (non-blocking, under sf->mu). */
uint32_t sfill_get_progress(struct sfill *sf, struct sfill_progress *out, uint32_t cap);

void sfill_get_stats(struct sfill *sf, struct sfill_stats *out);

/* R13 r13-fill (E53, §15.4): optional local store put-admission hook for rebuild writes (up to 95%). */
typedef int (*sfill_store_allow_fn)(int store_idx, int is_rebuild);
void sfill_set_store_allow(sfill_store_allow_fn fn);

/* Planted bugs (TSDB_SFILL_MUTANT env var or sfill_set_mutant; tests/test_sfill.c, make sfill-mutants, whole-mutants). */
enum sfill_mutant {
    SFILL_MUT_NONE = 0,
    SFILL_MUT_NOCHECK = 1,       /* fillnocheck: skips verifying rebuilt cells against encode-time sums */
    SFILL_MUT_FEWERK = 2,        /* fillfewerk: attempts decode with fewer than k good survivors */
    SFILL_MUT_SURVIVOR_TRUST = 3, /* fillsurvivortrust: skips verifying survivor cells against encode-time sums */
    SFILL_MUT_WHOLE_EARLY = 4,   /* wholeearly: reports whole=1 on the first report before all objects finish */
    SFILL_MUT_BALANCE_DECODE = 5, /* balancedecode: skips 1x copy on balancer fill and decodes instead */
};
extern _Atomic int sfill_mutant;
int sfill_mutant_parse(const char *name);
void sfill_set_mutant(int mut);

#ifdef TSDB_TEST_HOOKS
extern _Atomic int sfill_test_readback_flip; /* > 0: flips a byte in the next committed chunk before read-back */
extern uint32_t sfill_test_objlist_page;     /* 0: default (256); override OBJLIST page size for pagination tests */
extern int sfill_test_retry_ms;              /* 0: default (300 ms); retry delay after failed OBJLIST or object */
#endif

/* Startup / return reconciliation (server/sreconcile.c; r9-reconcile, D15). */
int sreconcile_trigger(struct sfill *sf, uint32_t volume, uint32_t pos, uint64_t stripe_version);
void sreconcile_on_start(struct sfill *sf);

/* Record a missing chunk position from a degraded settle (E10, r9-degsettle) for r9-reconcile to fill when its
 * holder returns or goes OUT. Thread-safe; records in a bounded in-memory ring and logs (not durable across restarts:
 * r9-reconcile rebuilds missing positions from committed .ec records' `positions` bitmaps on start/return). */
void sreconcile_note_missing(uint32_t volume, const uint8_t oid[16], unsigned pos);
uint64_t sreconcile_missing_count(void);

#endif
