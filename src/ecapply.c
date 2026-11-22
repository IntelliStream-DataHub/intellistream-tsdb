/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ecapply.h: r8h-apply fills it. Every call leaves main's behaviour as it is. */
#include "internal.h"
#include "ecapply.h"

int ecapply_cold(struct eng *db, uint64_t pos, const mk *m)
{
    (void)db, (void)pos, (void)m;
    return 0;
}

int ecapply_cold_decide(struct eng *db, int64_t key, const uint8_t *oids, uint32_t n)
{
    (void)db, (void)key, (void)oids, (void)n;
    return TSDB_ERR_INVALID; /* not built yet */
}

int ecapply_swap_pre(struct eng *db, struct part **in, struct part **ec, size_t n, bool *logged)
{
    (void)db, (void)in, (void)ec, (void)n;
    *logged = false;
    return 0;
}

void ecapply_swap_post(struct eng *db, struct part **in, struct part **ec, size_t n)
{
    (void)db, (void)in, (void)ec, (void)n;
}
