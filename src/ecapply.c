/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ecapply.h: r8h-apply fills it. Every call leaves main's behaviour as it is, but for the swap's
 * PPOS records of its .ec parts (swap_pre), which an ec cluster volume's open requires and r8h keeps. */
#include "internal.h"
#include "ecapply.h"
#include "ecpart.h"
#include "ppos.h"

#include <stdatomic.h>
#include <stdlib.h>

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

/* The .ec parts' PPOS records (src/ppos.h, design-cluster-data.md 2.3 "Part position"), each at its object record's
 * ppos (the settle's cut Z; 0, as in no cluster record, is recorded as unknown), one append and fdatasync per run of
 * equal positions. Not stub behaviour: r8h keeps it. They go out before the intent, so every state a power cut can
 * leave holds at the next open: no intent (the .ec never named; its record is dead and the compaction drops it), an
 * intent rolled back (the sources keep their own records), rolled forward or standing (the .ec names have theirs).
 * A failed write or sync failed the engine (ppos.c); nothing of the swap is visible. Nothing on other volumes. */
static int swap_ppos(struct eng *db, struct part **ec, size_t n)
{
    if (!db->ppos)
        return 0;
    const char **nm = malloc(n * sizeof *nm);
    if (!nm)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (size_t i = 0, j; rc == 0 && i < n; i = j) {
        struct ecpart *e = atomic_load(&ec[i]->ec);
        uint64_t z = e ? e->rec.ppos : 0;
        for (j = i; j < n; j++) {
            struct ecpart *f = atomic_load(&ec[j]->ec);
            if ((f ? f->rec.ppos : 0) != z)
                break;
            nm[j - i] = ec[j]->name;
        }
        rc = ppos_record(db, nm, j - i, z ? z : PPOS_UNKNOWN);
    }
    free(nm);
    return rc;
}

int ecapply_swap_pre(struct eng *db, struct part **in, struct part **ec, size_t n, bool *logged)
{
    (void)in;
    *logged = false;
    if (ppos_mutant == PPOS_MUT_SWAPNONE || ppos_mutant == PPOS_MUT_SWAPAFTER)
        return 0;
    return swap_ppos(db, ec, n);
}

void ecapply_swap_post(struct eng *db, struct part **in, struct part **ec, size_t n)
{
    (void)in;
    if (ppos_mutant == PPOS_MUT_SWAPAFTER) /* the planted bug: the records after the names */
        swap_ppos(db, ec, n);
}
