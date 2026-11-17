/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled sources (src/eckeep.h). r7-iface's stubs; r7-keep fills them.
 */
#include "eckeep.h"

#include <unistd.h>

int eckeep_load_settled(eng *db, const char *name, bool read_only)
{
    (void)db;
    (void)name;
    (void)read_only;
    return 0;
}

bool eckeep_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now)
{
    if (!all && e->deadline > now)
        return false;
    unlinkat(db->dirfd, e->name, 0);
    return true;
}
