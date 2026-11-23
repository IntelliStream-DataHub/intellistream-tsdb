/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Planned leader transfer, the volume leader's side (rtransfer.h): NOT BUILT (r4-transfer). Until then every record is
 * admitted as before and a TRANSFER or CANCEL is said once per volume and ignored.
 */
#include <inttypes.h>

#include "rtransfer.h"

int rtr_admit_ok(struct rvol *rv)
{
    (void)rv;
    return 1;
}

void rtr_cmd(struct rvol *rv, const struct cluster_volcmd *c)
{
    if (!rv->said_cmd_tr)
        say("volume %" PRIu32 ": %s to node %" PRIu32 " at epoch %" PRIu64 ": leader transfer is not built yet; ignored",
            rv->number, c->kind == CL_VOLCMD_CANCEL ? "CANCEL" : "TRANSFER", c->node, c->epoch);
    rv->said_cmd_tr = 1;
}

void rtr_tick(struct rvol *rv) { (void)rv; }
