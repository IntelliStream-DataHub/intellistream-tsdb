/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Per-node background young-part and .ec file scrubber (R12, r12-young; design-cluster-data.md §5.9, PLAN.md D4/D7/D10/D27).
 *
 * Cluster mode only: one thread per server walking its held cluster volumes at scrub_mb_s, persisting advisory
 * progress in v<N>/SCRUB, reporting damaged replicas (CL_HEALTH_DAMAGED), and exporting young scrub metrics through
 * server/creport.h.
 */
#ifndef TSDBD_SCRUBD_H
#define TSDBD_SCRUBD_H

#include <stdint.h>

#include "tsdb.h"

/* Starts the per-node young-part scrub thread for cluster handle `t` whose data root is `dir`, and registers the
 * tsdb_young_scrub_* Prometheus metrics via creport_metric_collector(). Returns 0, or -1 on error. */
int scrubd_start(tsdb *t, const char *dir);

/* Stops and joins the scrub thread and unregisters its creport collectors. Safe no-op if not started. */
void scrubd_stop(void);

/* Lock-free O(1) check: 1 if scrub detected young-part or .ec damage on held volume `vol` in this run, else 0. */
int scrubd_vol_damaged(uint32_t vol);

/* Lock-free O(1) update called by server/repld.c: 1 while damaged replica `vol` is held in the ISR because leaving
 * would drop below min_isr, 0 otherwise. Exported as tsdb_young_scrub_min_isr_held{volume}. */
void scrubd_set_min_isr_held(uint32_t vol, int held);

/* Clears in-memory damage and cached progress for `vol` when its directory is reset, removed, or replaced by a
 * snapshot install, so the fresh replica never advertises stale CL_HEALTH_DAMAGED before scrubd notices the new inode. */
void scrubd_vol_reset(uint32_t vol);

#endif /* TSDBD_SCRUBD_H */
