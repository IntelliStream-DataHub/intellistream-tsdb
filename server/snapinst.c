/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of a snapshot's install that runs at prepare (snapinst.h; R4 plan D5): what a crash left of an install
 * (v<N>.snap, a swap half done) is finished or undone by snap_leftovers before anything of v<N> is looked at. The
 * install itself is server/snapinst_run.c (tsdbd only: it takes the volume from the manager).
 */
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "tsdb.h"
#include "../src/snapshot.h"
#include "cvolfs.h"
#include "snapinst.h"

/* TSDB_REPLD_TEST=snapleftoversrace (test_cluster snapfence): a v<N>.snap without SNAPDONE is left 400 ms before
 * snap_leftovers runs, and one that is gone after that wait was removed by another thread meanwhile, which in
 * snap_leftovers (exists, then an open) is an I/O error: refused as it would be (review F1; mutant snapdropafter) */
static int leftovers_race(const char *root, uint32_t number)
{
    const char *te = getenv("TSDB_REPLD_TEST");
    if (!te || !strstr(te, "snapleftoversrace"))
        return 0;
    char snap[PATH_MAX], done[PATH_MAX + 16];
    snprintf(snap, sizeof snap, "%s/v%" PRIu32 ".snap", root, number);
    snprintf(done, sizeof done, "%s/SNAPDONE", snap);
    struct stat sb;
    if (lstat(snap, &sb) != 0 || lstat(done, &sb) == 0)
        return 0;
    usleep(400000);
    if (lstat(snap, &sb) == 0 || errno != ENOENT)
        return 0;
    cvolfs_say("TEST: volume %" PRIu32 ": %s was removed by another thread during its leftovers (snapleftoversrace)",
               number, snap);
    return TSDB_ERR_IO;
}

int snapinst_leftovers(const char *root, uint32_t number)
{
    const char *mu = getenv("TSDB_REPLD_MUTANT");
    if (mu && !strcmp(mu, "snapnoleftovers"))
        return 0; /* planted: a crash's v<N>.snap, v<N>.old left as they are */
    int rc = leftovers_race(root, number);
    if (rc == 0)
        rc = snap_leftovers(root, number);
    if (rc != 0)
        cvolfs_say("volume %" PRIu32 ": what an install left in %s cannot be finished or undone: %s", number, root,
                   tsdb_strerror(rc));
    return rc ? -1 : 0;
}
