/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Volume-leader chunk repair (R12, r12-repair; docs/design-cluster-data.md E11, E14, E25, E26, §5.9; PLAN.md D5, D9):
 *
 *   - Bounded, deduplicated `(volume, oid, pos)` repair queue on the volume leader, fed by `CS_OP_DAMAGED` reports from
 *     storage servers' background scrubbers and by reads that observe a bad survivor (`CHUNK_ST_DAMAGED`).
 *   - Worker pool enforcing at most one active repair targeting any given storage node at a time without head-of-line
 *     blocking across distinct nodes.
 *   - Live / not-retired / not-pending-MK_GC verification BEFORE decoding AND right BEFORE the replace-put under the
 *     same per-oid lock `ecgcmk` uses (D9: never resurrect a deleted object).
 *   - Rebuilds the target position from `k` verified surviving positions using Reed-Solomon decoding, checks every
 *     rebuilt cell against the object record's `sums` (E26); `< k` good writes nothing, increments `unrecoverable`,
 *     and emits a health log line.
 *   - Issues a replace-put (`CS_PUT_F_REPLACE`: tmp, fsync, rename, dir fsync, read-back + scrub) carrying the object's
 *     original `writer_epoch` (`hdr_epoch`) under the leader's pinned fence epoch (`epoch`), comparing `PUT_OK`'s hash.
 */
#ifndef TSDB_CHUNKREPAIR_H
#define TSDB_CHUNKREPAIR_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../src/chunk.h"
#include "chunksrv.h"

#define CHUNKREPAIR_QUEUE_MAX 1024u
#define CHUNKREPAIR_DEFAULT_WORKERS 4u
#define CHUNKREPAIR_MAX_WORKERS 16u

enum {
    CHUNKREPAIR_RES_REPAIRED = 0,
    CHUNKREPAIR_RES_UNRECOVERABLE = 1,
    CHUNKREPAIR_RES_UNAVAILABLE = 2,
    CHUNKREPAIR_RES_NOT_LIVE = 3,
    CHUNKREPAIR_RES_FENCED = 4,
    CHUNKREPAIR_RES_FAILED = 5,
    CHUNKREPAIR_RES_DROPPED = 6,
    CHUNKREPAIR_RES_COUNT = 7,
};

const char *chunkrepair_res_name(int res);

struct chunkrepair_obj {
    uint8_t vuuid[16];
    uint8_t oid[16];
    chunk_geom g;
    uint32_t positions;
    uint64_t writer_epoch;
    uint64_t stripe_version;
    uint64_t nsums;
    uint64_t *sums; /* malloc'ed; freed by chunkrepair */
};

struct chunkrepair_vol_view {
    uint64_t fence_epoch; /* leader's pinned epoch (repld_leading) */
    uint64_t stripe_version;
    unsigned k, m, nstripe;
    uint32_t stripe[CHUNK_POS_MAX];
    uint8_t stripe_filling[CHUNK_POS_MAX];
    uint32_t stripe_prev[CHUNK_POS_MAX];
    uint8_t stripe_prev_up[CHUNK_POS_MAX];
    uint64_t stripe_set_ver[CHUNK_POS_MAX];
    bool is_leader;
};

struct chunkrepair_ops {
    /* Step 1: verify leadership and that `oid` is live (listed .ec, not retired, not pending MK_GC), populating *out.
     * Returns 0 if live, TSDB_ERR_NOTFOUND if not live/retired/pending, TSDB_ERR_UNAVAILABLE if not leader. */
    int (*lookup_live)(void *arg, uint32_t volume, const uint8_t oid[16], struct chunkrepair_obj *out);

    /* Current volume view (pinned fence epoch and stripe map). Returns 0, or TSDB_ERR_UNAVAILABLE. */
    int (*vol_view)(void *arg, uint32_t volume, struct chunkrepair_vol_view *out);

    /* Read one cell of survivor position `pos` from `node`. Returns CHUNK_ST_* (CHUNK_ST_OK on success). */
    int (*read_cell)(void *arg, uint32_t node, uint32_t volume, uint64_t epoch, const uint8_t vuuid[16],
                     const uint8_t oid[16], unsigned pos, uint64_t off, void *buf, size_t len);

    /* Step 4 (D9): under per-oid GC lock, re-verify leadership and live/not-retired/not-pending-MK_GC (unless
     * skip_live_check is set by the `repairnolive` mutant), send replace-put (CS_PUT_F_REPLACE) to `node` and verify
     * PUT_OK's answer hash. Returns 0 on verified PUT_OK, TSDB_ERR_NOTFOUND if not live, CHUNK_ERR_FENCED if fenced,
     * TSDB_ERR_UNAVAILABLE if deposed/unreachable, or TSDB_ERR_IO / TSDB_ERR_CORRUPT. */
    int (*check_and_put)(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum,
                         bool skip_live_check);

    /* Optional: returns true if filling node already reported `oid` filled at `(volume, pos, set_ver)`. */
    bool (*is_filled)(void *arg, uint32_t node, uint32_t volume, unsigned pos, uint64_t set_ver, const uint8_t oid[16]);

    /* Optional: notify replicas (and local cache) to reopen (volume, oid) after a successful repair. */
    void (*notify_reopen)(void *arg, uint32_t volume, const uint8_t oid[16]);

    /* Optional: completion hook for tests (`result` is CHUNKREPAIR_RES_*). */
    void (*on_done)(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos, int result);

    void *arg;
};

struct chunkrepair_cfg {
    char stage_dir[PATH_MAX]; /* temp staging dir fallback for >16 MiB chunks */
    unsigned nworkers;        /* 0: CHUNKREPAIR_DEFAULT_WORKERS */
    size_t queue_max;         /* 0: CHUNKREPAIR_QUEUE_MAX */
    int max_retries;          /* 0: default 3 retries on transient UNAVAILABLE/FAILED; <0: no retries */
    uint32_t retry_base_ms;   /* 0: default 500 ms exponential backoff base */
    int cooldown_ms;          /* 0: default 30000 ms post-failure cooldown per (vol,oid,pos); <0: disabled */
};

struct chunkrepair_stats {
    uint64_t results[CHUNKREPAIR_RES_COUNT];
    uint64_t bytes_repaired;
    uint64_t deduped;
    size_t queue_depth;
};

struct chunkrepair;

struct chunkrepair *chunkrepair_new(const struct chunkrepair_cfg *cfg, const struct chunkrepair_ops *ops);
void chunkrepair_free(struct chunkrepair *cr);

/* Enqueue `(volume, oid, pos)` targeting `target_node` (0 if unknown; resolved from vol_view). Deduplicated against
 * queued and in-flight items. Returns 0 if enqueued or deduplicated, TSDB_ERR_UNAVAILABLE if dropped (queue full), or
 * TSDB_ERR_INVALID if invalid/stopping. */
int chunkrepair_enqueue(struct chunkrepair *cr, uint32_t volume, const uint8_t oid[16], unsigned pos,
                        uint32_t target_node);

void chunkrepair_get_stats(struct chunkrepair *cr, struct chunkrepair_stats *out);
size_t chunkrepair_queue_depth(struct chunkrepair *cr);
void chunkrepair_wait_idle(struct chunkrepair *cr);

/* Planted bugs (`TSDB_REPAIR_MUTANT`; tests/test_chunkrepair.c and test_cluster scrubrepair --mutant). */
enum {
    CR_MUT_NONE = 0,
    CR_MUT_NOLIVE = 1,   /* repairnolive: skip live/GC check before replace-put */
    CR_MUT_NOSUMS = 2,   /* repairnosums: skip survivor and rebuilt cell sum verification */
    CR_MUT_OWNEPOCH = 3, /* repairownepoch: write leader's fence epoch instead of object's writer_epoch */
};
extern _Atomic int chunkrepair_mutant;
int chunkrepair_mutant_parse(const char *name);
void chunkrepair_env_init(void);
#ifdef TSDB_TEST_HOOKS
void chunkrepair_set_pause_path(const char *path);
uint64_t chunkrepair_pause_hits(void);
#endif

#endif
