/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 rebuild and balancer traffic pacer (docs/design-cluster-data.md E13, §8.4; R9 plan D18; track r9-pace).
 *
 * One token bucket per server and direction (PACE_SEND, PACE_RECV) covering rebuild traffic (PL_KIND_SNAP kind 5
 * snapshots wired in server/rsnap.c, and PL_KIND_CHUNKR kind 8 fill reads/writes to be wired in r9-fill).
 * The bucket rate tracks the lowest margin active on that direction:
 *   - margin == 0: unlimited (PACE_RATE_UNLIMITED) and never halved by p99 load;
 *   - margin == 1: at least 4 x rebuild_mb_s (halved on p99 load);
 *   - margin >= 2: rebuild_mb_s (halved on p99 load);
 *   - balance traffic (is_balance / PACE_MARGIN_BALANCE): balance_mb_s (0 = rebuild_mb_s), halved on p99 load;
 *     locally refused (returns PACE_REFUSED) whenever any non-balance rebuild job is registered on that direction
 *     (cross-end deferral is coordinated by the balancer planner via HBS_FILL in r9-balance).
 *
 * While query p99 or WAL sync p99 (1-minute window) exceeds twice its 1-hour median, non-emergency rates halve.
 */
#ifndef TSDB_PACE_H
#define TSDB_PACE_H

#include <stddef.h>
#include <stdint.h>

enum pace_dir {
    PACE_SEND = 0,
    PACE_RECV = 1,
};

/* Special margin sentinels. */
#define PACE_MARGIN_BALANCE (-1) /* balancer traffic (yields to any rebuild job on the same direction) */
#define PACE_MARGIN_NONE (-2)    /* reported in active_margin when no job is registered on that direction */

#define PACE_RATE_UNLIMITED UINT64_MAX
#define PACE_REFUSED UINT64_MAX /* returned by pace_take / pace_try_take when a balance job yields to a rebuild */

#define PACE_DEFAULT_REBUILD_BYTES_S (200ull << 20) /* 200 MiB/s */

/* Margin classes for per-class paced byte counters (docs/metrics.md). */
enum pace_class {
    PACE_CLS_MARGIN0 = 0,     /* margin == 0 */
    PACE_CLS_MARGIN1 = 1,     /* margin == 1 */
    PACE_CLS_MARGIN2PLUS = 2, /* margin >= 2 */
    PACE_CLS_BALANCE = 3,     /* balance job */
    PACE_CLS_MAX = 4,
};

struct pace_cfg {
    uint64_t rebuild_bytes_s; /* bytes/s from rebuild_mb_s (0: default 200 MiB/s) */
    uint64_t balance_bytes_s; /* bytes/s from balance_mb_s (0: equals rebuild_bytes_s) */
};

struct pace_stats {
    uint64_t taken_bytes[2];
    uint64_t wait_ns[2];
    uint64_t rate_bytes_s[2];                     /* current effective rate (PACE_RATE_UNLIMITED when margin 0) */
    uint64_t class_bytes[2][PACE_CLS_MAX];        /* bytes paced per direction and margin class */
    uint64_t balance_refused[2];                  /* balance takes refused while a rebuild job was active */
    int active_margin[2];                         /* lowest active margin, PACE_MARGIN_BALANCE, or PACE_MARGIN_NONE */
    int halved;                                   /* 1 while foreground p99 halving is active, else 0 */
};

/* Configure node-wide rebuild and balance rates (thread-safe). */
void pace_configure(const struct pace_cfg *cfg);

/* Update the p99 load-halving signal (1 while 1-min p99 > 2x 1-hr median, else 0). */
void pace_set_p99_halved(int halved);

/* Register / unregister an active rebuild or balance job by margin (margin == PACE_MARGIN_BALANCE for balance). */
void pace_job_begin(enum pace_dir dir, int margin);
void pace_job_end(enum pace_dir dir, int margin);

/* Register / update / unregister an active job with an explicit job id (returns > 0, or 0 on error). */
uint64_t pace_job_begin_ex(enum pace_dir dir, int margin, int is_balance);
void pace_job_update_ex(enum pace_dir dir, uint64_t job_id, int margin);
void pace_job_end_ex(enum pace_dir dir, uint64_t job_id);

/* Non-blocking token check for `bytes` on `dir` at `margin`:
 *   - returns 0 when `bytes` are admitted immediately (tokens consumed and stats counted);
 *   - returns wait_ns > 0 (< PACE_REFUSED) when the caller must wait before retrying (nothing consumed);
 *   - returns PACE_REFUSED when `margin == PACE_MARGIN_BALANCE` and any rebuild job is active on `dir`. */
uint64_t pace_try_take(enum pace_dir dir, uint64_t bytes, int margin);

/* Blocking token consumption for `bytes` on `dir` at `margin`:
 * waits until tokens are available (waking promptly if a lower-margin job raises the rate).
 * Margin 0 never waits. Returns total nanoseconds waited, or PACE_REFUSED if a balance job yields to a rebuild. */
uint64_t pace_take(enum pace_dir dir, uint64_t bytes, int margin);
/* Like pace_take, but caps blocking to `max_wait_ns` (0 = unbounded) so link I/O callbacks never stall past dead_ms.
 * If the cap is reached, charges `bytes` to the virtual clock and returns the elapsed nanoseconds waited. */
uint64_t pace_take_capped(enum pace_dir dir, uint64_t bytes, int margin, uint64_t max_wait_ns);

/* Peerlink rate-hook adapter (pl_pace_fn signature): delegates PL_KIND_SNAP / PL_KIND_CHUNKR sends to PACE_SEND. */
uint64_t pace_peerlink_send(void *arg, int kind, size_t n);

/* Snapshot current bucket counters and effective rates for metrics and tests. */
void pace_stats(struct pace_stats *out);

/* Test hooks and planted mutants (TSDB_PACE_MUTANT env var or pace_set_mutant). */
enum {
    PACE_MUT_NONE = 0,
    PACE_MUT_FIFO = 1,    /* pacefifo: rate follows the FIRST registered job instead of the LOWEST margin */
    PACE_MUT_NOHALVE = 2, /* nohalve: ignores foreground p99 halving signal */
};
void pace_set_mutant(int mut);
int pace_mutant_parse(const char *name);
void pace_reset(void);

#endif
