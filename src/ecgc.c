/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (src/ecgc.h). r7-iface's stubs; r7-gc fills them.
 */
#include "ecgc.h"
#include "ecpart.h"

#include <unistd.h>

int ecgc_load_retired(eng *db, const char *name, bool read_only)
{
    EC_CALLED(EC_CALL_GC_LOAD);
    (void)db;
    (void)name;
    (void)read_only;
    return 0;
}

void ecgc_retire_locked(eng *db, part *p)
{
    EC_CALLED(EC_CALL_GC_RETIRE);
    (void)db;
    (void)p;
}

bool ecgc_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now)
{
    EC_CALLED(EC_CALL_GC_PURGE);
    if (!all && e->deadline > now)
        return false;
    unlinkat(db->dirfd, e->name, 0);
    return true;
}

void ecgc_last_unref(part *p)
{
    EC_CALLED(EC_CALL_GC_LAST_UNREF);
    ecpart_free(p->ec);
    p->ec = NULL;
}

void ecgc_unlisted_object(eng *db, const part *p)
{
    (void)db;
    (void)p;
}

int ecgc_tier_open(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_OPEN);
    return 0;
}

void ecgc_tier_close(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_CLOSE);
}

int ecgc_eng_open(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_OPEN);
    return 0;
}

void ecgc_eng_close(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_CLOSE);
}

int ecgc_open_sweep(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_GC_SWEEP);
    return 0;
}
