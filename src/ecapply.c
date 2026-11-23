/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of ecapply.h: r8h-apply fills it. Every call leaves main's behaviour as it is, but for the swap's
 * PPOS records of its .ec parts (swap_pre), which an ec cluster volume's open requires and r8h keeps. */
#include "internal.h"
#include "ecapply.h"
#include "ecpart.h"
#include "ppos.h"
#include "eccold.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include <stdatomic.h>
#include <stdlib.h>

int ecapply_cold(struct eng *db, uint64_t pos, const mk *m)
{
    (void)db, (void)pos, (void)m;
    return 0;
}

int ecapply_mutant;

int ecapply_mutant_parse(const char *name)
{
    static const char *const names[] = {"none", "ycold", "ecabovey", "pposignored", "v5nodirsync", "coldnoflushthrough",
                                        "leaderswapnors", "pposafterswap"};
    for (int i = 0; i < (int)(sizeof names / sizeof names[0]); i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}

/* Whether the COLD in `doing` was carried out: its swap takes no block numbers a replay could look for (a leader's keeps
 * its sources' names, D2; a replica's numbers are fresh and unknown to the log), so the answer is what is listed: an
 * .ec part of key `key` whose record names the oid. Every oid listed: done; none: not; some: the intent that lists them
 * all or none (v4 or v5) was not honoured, which is damage. A COLD with no objects is not (nothing to look for; its
 * apply lists nothing either). An oid listed twice is damage too: the apply that this decision guards would have
 * listed an object twice. */
int ecapply_cold_decide(struct eng *db, int64_t key, const uint8_t *oids, uint32_t n)
{
    if (n == 0)
        return 0;
    uint32_t *seen = calloc(n, sizeof *seen);
    if (!seen)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        const part *p = db->parts[i];
        const struct ecpart *e = atomic_load(&((part *)p)->ec);
        if (!e || e->test_mark || p->partition != key)
            continue;
        for (uint32_t k = 0; k < n; k++)
            if (memcmp(e->rec.oid, oids + (size_t)k * MK_OID, MK_OID) == 0)
                seen[k]++;
    }
    pthread_mutex_unlock(&db->lock);
    uint32_t listed = 0, twice = 0;
    for (uint32_t k = 0; k < n; k++)
        listed += seen[k] > 0, twice += seen[k] > 1;
    free(seen);
    if (twice || (listed && listed < n)) {
        fprintf(stderr, "tsdb: %s: of the %u objects of a COLD of key %" PRId64 ", %u are listed (%u more than once): "
                        "its swap was all or nothing\n", db->dir, n, key, listed, twice);
        return TSDB_ERR_CORRUPT;
    }
    return listed == n ? 1 : 0;
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

/* A leader's swap of a settle (r8g's eccold_settle -> ec_swap_commit; publish_lock held): the COLD this engine is
 * carrying out, if any (eccold_swap_cur: between its marker and the end of its swap). Its objects are named by a
 * committed marker (`logged`): the replicas list them whatever this swap does. Before the intent's commit point: the
 * .ec parts' PPOS records, then RSTATE's `doing` (jr_cold_precommit), so a leader that loses power anywhere after it
 * and reopens as a replica asks ecapply_cold_decide what is listed instead of applying the COLD a second time (its swap
 * took no block number that rs_committed could find). A failed save failed the engine; the swap refuses (logged). */
int ecapply_swap_pre(struct eng *db, struct part **in, struct part **ec, size_t n, bool *logged)
{
    (void)in;
    *logged = false;
    uint64_t pos = 0;
    int64_t key = 0;
    if (eccold_swap_cur(db, &pos, &key, NULL) && pos)
        *logged = true;
    int rc = 0;
    if (ppos_mutant != PPOS_MUT_SWAPNONE && ppos_mutant != PPOS_MUT_SWAPAFTER &&
        ecapply_mutant != ECAPPLY_MUT_PPOSAFTERSWAP)
        rc = swap_ppos(db, ec, n);
    if (rc == 0 && pos && ecapply_mutant != ECAPPLY_MUT_LEADERSWAPNORS)
        rc = jr_cold_precommit(db, pos, key);
    return rc;
}

/* After the swap's renames and their directory sync (publish_lock held): RSTATE's `done` (a failed save fails the
 * engine: reopened as a replica it would find `doing` and decide by what is listed, which is still right). */
void ecapply_swap_post(struct eng *db, struct part **in, struct part **ec, size_t n)
{
    (void)in;
    if (ppos_mutant == PPOS_MUT_SWAPAFTER || ecapply_mutant == ECAPPLY_MUT_PPOSAFTERSWAP) /* planted: records late */
        swap_ppos(db, ec, n);
    uint64_t pos = 0;
    if (eccold_swap_cur(db, &pos, NULL, NULL) && pos && ecapply_mutant != ECAPPLY_MUT_LEADERSWAPNORS)
        (void)jr_cold_commit(db, pos);
}
