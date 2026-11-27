/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local tier salvage past m LOST stores (R15 r15-salvage; docs/design-cluster-data.md E15, §15.7; PLAN.md D11).
 *
 * When more than m stores of an object's erasure-coded stripe are permanently LOST, reads of its .ec part fail
 * with TSDB_ERR_IO while statistics over its local index still answer. Salvage drops only those unrecoverable .ec
 * parts (retiring each partition key atomically via intent v5 `X <pX.ec> <retired_pX.ec>` under publish_lock >
 * latest_rw > db->lock), keeps all surviving young .part files and unaffected .ec parts intact, invalidates the
 * latest table (bumping lgen and hinting the warmer), pushes statistics removal events, hands retired objects to
 * ecgc (whose deletes of unreachable/LOST chunks are no-ops), logs rows lost per partition from the local index,
 * and increments the tier's salvage metrics.
 *
 * Refused while any store is DOWN and not permanently LOST (it may still come back), unless `force` is set.
 */
#ifndef TSDB_TIERSALV_H
#define TSDB_TIERSALV_H

#include "internal.h"

#include <stdbool.h>
#include <stdint.h>

struct tier;

typedef struct tiersalv_report {
    uint64_t partitions; /* distinct partition keys that had >= 1 lost .ec part retired */
    uint64_t parts;      /* lost .ec parts retired */
    uint64_t restored;   /* lost .ec parts restored from intact local settled_ copies (0 rows lost) */
    uint64_t rows_lost;  /* upper-bound raw rows lost across retired .ec parts (from their local indices) */
} tiersalv_report;

typedef struct tiersalv_counters {
    uint64_t partitions;
    uint64_t parts;
    uint64_t rows_lost;
} tiersalv_counters;

/* Refuses (TSDB_ERR_UNAVAILABLE, logging the store and state) if any store of t's backend is DOWN and not
 * permanently LOST, unless `force` is true. Returns 0 when salvage may proceed. */
int tiersalv_check_stores(const struct tier *t, bool force);

/* Salvages lost .ec parts on a single volume engine `db` (must be attached to a single-server local tier).
 * When `all_partitions` is false, inspects only partition key `partition`; when true, inspects every partition
 * key in `db`. Accumulates counts into `rep` when non-NULL. */
int tiersalv_eng(eng *db, int64_t partition, bool all_partitions, bool force, tiersalv_report *rep);

/* Offline CLI salvage on database directory `dir` (opens the tier with tier_open_salvage, checks store states,
 * opens root + every volume in VOLUMES, salvages each, and closes cleanly). Refuses with TSDB_ERR_LOCKED if a
 * writer (tsdbd) holds the database or stores. */
int tiersalv_cli(const char *dir, int64_t partition, bool all_partitions, bool force, tiersalv_report *rep);

/* Lock-free snapshot of cumulative salvage counters on tier `t` (for /metrics and tests). */
void tiersalv_get_counters(const struct tier *t, tiersalv_counters *out);

/* Planted bugs (tests/test_tiersalv.c --mutant NAME; Makefile tiersalv-mutants). Active only under TSDB_TEST_HOOKS. */
enum {
    TIERSALV_MUT_NONE = 0,
    TIERSALV_MUT_DOWN,   /* salvagedown: allows salvage while a store is DOWN (not LOST) without force */
    TIERSALV_MUT_LATEST, /* salvagelatest: skips lgen bump in latest table write section */
};
extern int tiersalv_mutant;
int tiersalv_mutant_parse(const char *name);

#endif
