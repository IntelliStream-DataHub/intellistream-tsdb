/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (src/ecgc.h). r7-iface's stubs; r7-gc fills them.
 */
#include "ecgc.h"
#include "ecpart.h"

#include <unistd.h>

int ecgc_load_retired(eng *db, const char *name, bool read_only)
{
    (void)db;
    (void)name;
    (void)read_only;
    return 0;
}

void ecgc_retire_locked(eng *db, part *p)
{
    (void)db;
    (void)p;
}

bool ecgc_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now)
{
    if (!all && e->deadline > now)
        return false;
    unlinkat(db->dirfd, e->name, 0);
    return true;
}

void ecgc_last_unref(part *p)
{
    ecpart_free(p->ec);
    p->ec = NULL;
}
