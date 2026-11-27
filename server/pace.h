/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 rebuild and balancer traffic pacer (docs/design-cluster-data.md E13, §8.4; R9 plan D18; track r9-pace).
 *
 * One token bucket per server and direction (PACE_SEND, PACE_RECV) covering rebuild traffic (PL_KIND_SNAP kind 5
 * snapshots and PL_KIND_CHUNKR kind 8 fill reads). The bucket rate tracks the most endangered job running:
 *   - margin == 0: unlimited and never halved by p99 load;
 *   - margin == 1 (m >= 2): at least 4 x rebuild_mb_s;
 *   - margin >= 2: rebuild_mb_s;
 *   - balance traffic (margin == PACE_MARGIN_BALANCE): balance_mb_s (0 = rebuild_mb_s), yielding whenever any
 *     rebuild job is queued on either end.
 * While query p99 or WAL sync p99 (1-minute window) exceeds twice its 1-hour median, non-emergency rates halve.
 *
 * No-op stub until r9-pace fills it in.
 */
#ifndef TSDB_PACE_H
#define TSDB_PACE_H

#include <stdint.h>

enum pace_dir {
    PACE_SEND = 0,
    PACE_RECV = 1,
};

/* Special margin sentinel for balancer traffic (yields to any rebuild job). */
#define PACE_MARGIN_BALANCE (-1)

struct pace_cfg {
    uint64_t rebuild_bytes_s; /* bytes/s from rebuild_mb_s (0: default 200 MiB/s) */
    uint64_t balance_bytes_s; /* bytes/s from balance_mb_s (0: equals rebuild_bytes_s) */
};

struct pace_stats {
    uint64_t taken_bytes[2];
    uint64_t wait_ns[2];
    uint64_t rate_bytes_s[2];
    int active_margin[2];
    int halved;
};

/* Configure node-wide rebuild and balance rates (thread-safe). */
void pace_configure(const struct pace_cfg *cfg);

/* Update the p99 load-halving signal (1 while 1-min p99 > 2x 1-hr median, else 0). */
void pace_set_p99_halved(int halved);

/* Register / unregister an active rebuild or balance job so the bucket tracks the minimum active margin. */
void pace_job_begin(enum pace_dir dir, int margin);
void pace_job_end(enum pace_dir dir, int margin);

/* Consume `bytes` from the `dir` bucket at `margin` (blocking until tokens are available; margin 0 never waits).
 * Returns nanoseconds waited (0 in the stub). */
uint64_t pace_take(enum pace_dir dir, uint64_t bytes, int margin);

/* Snapshot current bucket counters for metrics and tests. */
void pace_stats(struct pace_stats *out);

#endif
