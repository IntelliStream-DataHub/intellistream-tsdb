/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ecdigest.h: r8j-digest fills it. Every call leaves main's behaviour as it is. */
#include "internal.h"
#include "ecdigest.h"

int ec_logical_digest(struct eng *db, struct part **set, size_t n, int64_t key, int64_t now_ms, ec_ldigest *out)
{
    (void)db, (void)set, (void)n, (void)key, (void)now_ms;
    memset(out, 0, sizeof *out);
    return TSDB_ERR_INVALID; /* not built yet */
}

int ecdigest_apply(struct eng *db, uint64_t pos, const mk *m)
{
    (void)db, (void)pos, (void)m;
    return 0;
}
