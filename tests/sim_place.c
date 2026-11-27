/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Placement and failure-handling simulator (docs/design-cluster-data.md §10.2; R9 plan D7; track r9-simplace).
 *
 * Drives the real pure planners (server/pout.c, server/place.c, server/balance.c) over simulated clusters,
 * failures, heals and rebuilds, checking every placement and durability invariant and computing MTTDL per geometry.
 *
 * Minimal stub main until r9-simplace fills it in.
 */
#include <stdio.h>

#include "meta.h"
#include "place.h"

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    struct meta_state sm;
    meta_init(&sm);
    struct place_in in = {0};
    in.sm = &sm;
    place_settings_default(&in.set);
    struct place_out out;
    place_out_init(&out);
    pout_plan(&in, &out);
    place_plan(&in, &out);
    balance_plan(&in, &out);
    int bad = out.nprops != 0 || out.nlogs != 0;
    place_out_free(&out);
    meta_free(&sm);
    if (bad) {
        fprintf(stderr, "sim_place: stub planners emitted unexpected output\n");
        return 1;
    }
    printf("sim_place: stub ok\n");
    return 0;
}
