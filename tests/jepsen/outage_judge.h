/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * jepsen's judgement of a TOPOLOGY volume entry after an outage's heal (jepsen.c outage_volume_failed): failed for
 * good, so a volume that acknowledges no write is a problem of tsdb, not a stall of the disk or the machine. DIVERGED
 * (refused until an operator acts) and LOST (settled data whose holders were declared destroyed, design-cluster-data.md
 * E15) never come back by themselves. test_margins checks it; OUTAGE_JUDGE_MUT (its --mutant jepsenlost) forgets LOST.
 */
#ifndef TSDB_JEPSEN_OUTAGE_JUDGE_H
#define TSDB_JEPSEN_OUTAGE_JUDGE_H

#include <stdbool.h>
#include <stdint.h>

#include "tsdb_protocol.h"

#ifndef OUTAGE_JUDGE_MUT
#define OUTAGE_JUDGE_MUT 0
#endif

static inline bool outage_entry_failed(uint8_t health, uint8_t reason)
{
    if (reason == TSDB_WHY_LOST && !OUTAGE_JUDGE_MUT)
        return true; /* whatever the health says: the data is gone */
    return health == TSDB_HEALTH_UNAVAILABLE && reason == TSDB_WHY_DIVERGED;
}

#endif
