/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ecdiverge.h: r8i-diverged fills it. Every call leaves main's behaviour as it is. */
#include "internal.h"
#include "ecdiverge.h"

int ec_diverge(struct eng *db, int64_t key, const uint8_t *oid, int why)
{
    (void)key, (void)oid, (void)why;
    atomic_store_explicit(&db->diverged, true, memory_order_release); /* memory only until r8i */
    return 0;
}

int ecdiverge_open(struct eng *db)
{
    (void)db;
    return 0;
}

void ecdiverge_close(struct eng *db)
{
    (void)db;
}
