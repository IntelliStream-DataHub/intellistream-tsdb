/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Per-server WAL retention budget for DOWN or held members (R9 E29, PLAN D16; server/repld.c).
 *
 * While any replica member of a volume has record state META_DOWN (any reason), is held and out of the ISR
 * (hold_until_ms > wall_ms && !in_isr), or just returned META_UP from META_DOWN and has not rejoined the ISR yet, both
 * the volume's leader and every replica ignore repl_retain_s and retain segments within the per-server budget
 * repl_retain_down_gb (counted over every volume this server keeps for, as one atomic total of kept bytes above the
 * normal caps). Each active volume is proactively capped at its normal byte cap plus the server budget remaining after
 * other active volumes, and whenever the measured server total exceeds the budget (checked on SAVE and every 500 ms),
 * volumes are evicted back to normal caps largest-excess first so the lagging member installs an R4 snapshot.
 */
#ifndef TSDB_RETAIN_H
#define TSDB_RETAIN_H

#include <stdint.h>

#include "cluster.h"

/* Resets the per-server budget table and parses test overrides (TSDB_CLUSTER_TEST / TSDB_REPLD_TEST). Call from
 * repld_init(). */
void retain_init(void);

/* Clears all volume slots (called from repld_stop()). */
void retain_reset(void);

/* Wall clock in milliseconds (CLOCK_REALTIME), matching meta.c's hold_until_ms. */
uint64_t retain_wall_ms(void);

/* Converts repl_retain_down_gb to bytes (0 disables DOWN/held retention immediately; test overrides apply when > 0). */
uint64_t retain_budget_bytes(uint32_t repl_retain_down_gb);

/* Pure evaluation of which peers (other than `self`) currently need DOWN/held retention.
 * Writes their node IDs into down_ids[0..*ndown_out-1] (capacity META_REPLICAS_MAX) and returns 1 if any peer needs
 * extended retention, 0 otherwise. */
int retain_vol_wants(const struct cluster_vol_rec *rec, int self, const int *was_down, const uint64_t *peer_acked,
                     const int *peer_heard, int is_writer, uint64_t log_start, uint64_t wall_ms, int keep_after_out,
                     uint32_t *down_ids, uint32_t *ndown_out);

/* Reports volume `vol`'s excess bytes and active status to the per-server budget tracker.
 * When active && !nocap and the per-server total exceeds budget_bytes, evicts active non-evicted volumes largest-excess
 * first until within budget, writing newly evicted volume numbers into evicted_vols[0..*nevicted-1] (up to evcap).
 * Sets *evicted_out to 1 if `vol` is evicted (must fall back to normal caps), 0 otherwise, and sets *rem_out to the
 * server budget available to `vol` (budget_bytes minus other active non-evicted volumes' excess, clamped at 0). */
void retain_report(uint32_t vol, uint64_t excess, uint64_t budget_bytes, int active, int nocap, int *evicted_out,
                   uint64_t *rem_out, uint32_t *evicted_vols, uint32_t *nevicted, uint32_t evcap);

/* Clears `vol`'s sticky eviction flag (e.g. when a new member goes DOWN/held). */
void retain_clear_evicted(uint32_t vol);

/* Removes `vol` from the per-server budget table on volume close. */
void retain_forget(uint32_t vol);

/* Current atomic total of excess kept bytes across all active volumes on this server. */
uint64_t retain_total_excess(void);

#endif
