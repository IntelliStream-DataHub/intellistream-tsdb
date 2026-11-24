/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of a snapshot's install that runs at prepare (snapinst.h; R4 plan D5): what a crash left of an install
 * (v<N>.snap, a swap half done) is finished or undone by snap_leftovers before anything of v<N> is looked at. The
 * install itself is server/snapinst_run.c (tsdbd only: it takes the volume from the manager).
 */
#include <inttypes.h>

#include "tsdb.h"
#include "../src/snapshot.h"
#include "cvolfs.h"
#include "snapinst.h"

int snapinst_leftovers(const char *root, uint32_t number)
{
    int rc = snap_leftovers(root, number);
    if (rc != 0)
        cvolfs_say("volume %" PRIu32 ": what an install left in %s cannot be finished or undone: %s", number, root,
                   tsdb_strerror(rc));
    return rc ? -1 : 0;
}
