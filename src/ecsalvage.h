/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster EC volume salvage (R14, docs/design-cluster-data.md E15, §5.8, D26; track r14-salvage).
 *
 * When more than m stripe holders of an erasure-coded volume are declared destroyed, affected settled .ec objects
 * are permanently lost: reads of them fail with TSDB_ERR_IO naming the object and writes fail with UNAVAILABLE
 * (TSDB_WHY_LOST). `volume salvage` runs on the volume leader:
 *   1. Inspects every listed .ec part in the leader's engine using `pos_fn` to classify each stripe position
 *      (UP/DRAINING, DOWN, OUT, DESTROYED). Refuses while any OUT holder may still return unless `force` is set.
 *   2. Collects objects with fewer than k' surviving (non-destroyed, written) positions (equivalently > m positions
 *      destroyed/unwritten). Young .part files and surviving .ec objects (>= k' positions left) are untouched.
 *   3. Commits one MK_DROP_OBJ marker per batch of up to MK_FILES_MAX oids under jr_serial (raising unsettle_hold_pos
 *      before append so REPL.floor and snap_make cannot pass MK_DROP_OBJ until the leader's own retire is durable).
 *   4. Retires the named .ec parts on the leader via ec_swap_salvage_at (intent v5 X lines, lt_salvage_marks,
 *      stats_ev_remove, eccache_forget, ecgc_retire_locked_at gated on all log replicas applying past MK_DROP_OBJ).
 *   5. Every log replica applies MK_DROP_OBJ via ec_salvage_apply (called from src/replica.c apply_marker), retiring
 *      its own listed .ec parts matching the marker's oids; GC then deletes any remaining chunks on surviving stores.
 *
 * Planted mutant (TSDB_SALVAGE_MUTANT under TSDB_TEST_HOOKS):
 *   - salvagenotlost: drops objects with <= k' surviving positions (dropping an object that still has k' left).
 */
#ifndef TSDB_ECSALVAGE_H
#define TSDB_ECSALVAGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "ecpart.h"
#include "marker.h"

struct eng;

/* Effective holder state of one stripe position for an object record. */
enum ec_salvage_pos {
    EC_SALV_UP = 0,        /* UP or DRAINING (or replacement already filled this oid) */
    EC_SALV_DOWN = 1,      /* DOWN (may still return) */
    EC_SALV_OUT = 2,       /* OUT (may still return: refuses salvage without --force) */
    EC_SALV_DESTROYED = 3, /* DESTROYED (permanent loss) */
};

typedef int (*ec_salvage_pos_fn)(void *ctx, const ec_record *rec, unsigned pos);

typedef struct ec_salvage_obj {
    uint8_t oid[16];
    int64_t partition;
    uint64_t rows;
} ec_salvage_obj;

typedef struct ec_salvage_report {
    size_t nobjs;
    size_t shadowed; /* salvaged parts that shadowed lower-block parts of the same partition */
    uint64_t rows_lost;
    ec_salvage_obj *objs;
} ec_salvage_report;

/* Crash hook (tests/server: wired in server/repld.c): runs on the leader right after MK_DROP_OBJ commits and before
 * the leader's own ec_swap_salvage_at (CP_SALVAGE_AFTER_MARKER). */
extern void (*_Atomic ecsalvage_leader_crash_hook)(struct eng *db);

void ec_salvage_report_free(ec_salvage_report *rep);

/* Monotonic part-list generation of `db` (bumped on every part insert/remove); used to cache ec_eng_has_lost_obj. */
uint64_t ec_eng_parts_gen(const struct eng *db);

/* Returns true if `db` currently lists at least one .ec part with fewer than k' non-destroyed written positions. */
bool ec_eng_has_lost_obj(struct eng *db, ec_salvage_pos_fn pos_fn, void *pos_ctx);

/* Leader entry point for `tsdb cluster volume salvage V [--force]`. Commits MK_DROP_OBJ for every permanently lost
 * .ec object and retires them durably on the leader. Returns 0 on success (rep->nobjs may be 0 if none lost),
 * TSDB_ERR_LOCKED if refused because an OUT holder may still return (without force) or a part is busy,
 * TSDB_ERR_INDETERMINATE if deposed while committing MK_DROP_OBJ, or another TSDB_ERR_*. */
int ec_salvage_leader(struct eng *db, ec_salvage_pos_fn pos_fn, void *pos_ctx, bool force, ec_salvage_report *rep,
                      char *why, size_t wn);

/* Replica apply of a committed MK_DROP_OBJ marker (called from src/replica.c apply_marker with rp->apply_mu and
 * rp->mu held). Idempotent on replay. Returns 0, RP_AGAIN (cut_gen moved), or TSDB_ERR_*. */
int ec_salvage_apply(struct eng *db, uint64_t pos, const struct mk *m);

#endif
