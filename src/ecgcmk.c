/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ecgcmk.h: r8k-gc fills it. Every call leaves main's behaviour as it is. */
#include "internal.h"
#include "ecgcmk.h"

int ecgcmk_apply(struct eng *db, uint64_t pos, const mk *m)
{
    (void)db, (void)pos, (void)m;
    return 0;
}

int ecgcmk_open(struct eng *db)
{
    (void)db;
    return 0;
}

void ecgcmk_close(struct eng *db)
{
    (void)db;
}
