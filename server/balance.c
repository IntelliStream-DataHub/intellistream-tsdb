/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure cluster balancer (server/place.h; track r9-balance; R9 plan D19; design-cluster.md §3.2, §6.2).
 *
 * Plans balancing moves across log replicas, stripe positions, and volume leaders after node add, drain, or OUT:
 * at most 5% of a group's positions moving at once (at least one) and no automatic plan moving more than 25%.
 * Pure function: no I/O, no clock, no locks.
 *
 * No-op stub until r9-balance fills it in.
 */
#include "place.h"

void balance_plan(const struct place_in *in, struct place_out *out)
{
    /* Stub until r9-balance fills this in. */
    (void)in;
    (void)out;
}
