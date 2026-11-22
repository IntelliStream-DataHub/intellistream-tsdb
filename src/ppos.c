/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ppos.h: r8d-ppos fills it. Every call leaves main's behaviour as it is. */
#include "internal.h"
#include "ppos.h"

int ppos_open(struct eng *db)
{
    (void)db;
    return 0;
}

void ppos_close(struct eng *db)
{
    (void)db;
}

void ppos_note(struct eng *db, int64_t ts, size_t n, uint64_t fno)
{
    (void)db, (void)ts, (void)n, (void)fno;
}

int ppos_record(struct eng *db, const char *const *names, size_t n, uint64_t pos)
{
    (void)db, (void)names, (void)n, (void)pos;
    return 0;
}

int ppos_set(struct eng *db, const char *name, uint64_t pos)
{
    (void)db, (void)name, (void)pos;
    return TSDB_ERR_INVALID; /* not built yet */
}

int ppos_of(struct eng *db, const char *name, uint64_t *pos)
{
    (void)db, (void)name;
    *pos = 0;
    return TSDB_ERR_INVALID; /* not built yet */
}
