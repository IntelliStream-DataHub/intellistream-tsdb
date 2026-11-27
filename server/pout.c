/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure DOWN/OUT timer planner (server/place.h; track r9-out; R9 plan D3-D6, E12).
 *
 * Evaluates DOWN-to-OUT timers (`rebuild_after_s`, `failstop_out_s`), opening holds (`opening_hold_s`), operator
 * holds (`hold_until_ms`), the computed mass-OUT guard (`out_guard_pct`, `out_guard_hold_s`, single-rack guard),
 * and per-replica LOCAL_FAULT escalation. Pure function: no I/O, no clock, no locks.
 *
 * No-op stub until r9-out fills it in.
 */
#include "place.h"

void pout_plan(const struct place_in *in, struct place_out *out)
{
    /* Stub until r9-out fills this in. */
    (void)in;
    (void)out;
}
