/* SPDX-License-Identifier: AGPL-3.0-only */
/* The margins of a cluster volume: margins.h has the rules. */
#include "margins.h"

#include <string.h>

static int member_state(const struct cluster_copy *cp, uint32_t id)
{
    size_t lo = 0, hi = cp->nmembers; /* members ascend by id */
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (cp->members[mid].id == id)
            return cp->members[mid].state;
        if (cp->members[mid].id < id)
            lo = mid + 1;
        else
            hi = mid;
    }
    return -1;
}

void margins_of(const struct cluster_copy *cp, const struct cluster_copy_vol *v, const uint32_t *destroyed, size_t nd,
                bool corrupt, int mutant, struct margins *out)
{
    memset(out, 0, sizeof *out);
    uint32_t n = mutant == MARGIN_MUT_YOUNGREP ? v->nrep : v->nisr;
    out->young = (int)n - 1;
    if (v->kind == META_VOL_SHARED && mutant != MARGIN_MUT_SHAREDCOPIES) {
        out->young = 0;
        out->young_na = true;
    }
    if (v->redundancy != META_RED_EC || v->nstripe == 0) {
        out->tier = MARGIN_TIER_NONE;
        return;
    }
    for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
        bool gone = false;
        for (size_t i = 0; i < nd && !gone; i++)
            gone = destroyed[i] == v->stripe[p];
        int st = member_state(cp, v->stripe[p]);
        if (gone && mutant != MARGIN_MUT_LOSTASDOWN)
            out->destroyed++;
        else if (!gone && (st == META_UP || st == META_DRAINING))
            out->present++;
        else
            out->away++;
    }
    int base = mutant == MARGIN_MUT_MARGINOFF ? (int)v->ec_k + (int)v->ec_m : (int)v->ec_k;
    out->settled = (int)out->present - base;
    if (out->destroyed > v->ec_m)
        out->tier = MARGIN_TIER_LOST;
    else if (corrupt && mutant == MARGIN_MUT_CORRUPTFIRST)
        out->tier = MARGIN_TIER_CORRUPT;
    else if (out->present < v->ec_k)
        out->tier = MARGIN_TIER_UNAVAILABLE;
    else if (corrupt)
        out->tier = MARGIN_TIER_CORRUPT;
    else
        out->tier = MARGIN_TIER_OK;
}

const char *margin_tier_name(int tier)
{
    static const char *const names[] = {"none", "ok", "unavailable", "corrupt", "lost"};
    _Static_assert(sizeof names / sizeof *names == MARGIN_TIER_LOST + 1, "a name per enum margin_tier");
    return tier >= 0 && tier <= MARGIN_TIER_LOST ? names[tier] : "?";
}

int margin_mutant_parse(const char *name)
{
    static const char *const names[] = {"",           "marginoff",   "youngrep",     "lostasdown",
                                        "sharedcopies", "lostnowait", "youngcorrupt", "corruptfirst"};
    _Static_assert(sizeof names / sizeof *names == MARGIN_MUT_COUNT, "a name per enum margin_mutant");
    if (!name || !*name)
        return MARGIN_MUT_NONE;
    for (int i = 1; i < MARGIN_MUT_COUNT; i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}
