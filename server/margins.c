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

static void eval_geom(const struct cluster_copy *cp, const struct cluster_copy_vol *v, const uint32_t *destroyed,
                      size_t nd, bool corrupt, int mutant, uint32_t npos, uint8_t k, uint8_t m, int *settled_out,
                      uint8_t *tier_out, uint8_t *present_out, uint8_t *away_out, uint8_t *destroyed_out)
{
    uint8_t pres = 0, away = 0, dest = 0;
    for (uint32_t p = 0; p < npos && p < v->nstripe && p < META_STRIPE_MAX; p++) {
        bool gone = false;
        for (size_t i = 0; i < nd && !gone; i++)
            gone = destroyed[i] == v->stripe[p];
        int st = member_state(cp, v->stripe[p]);
        if (st == META_DESTROYED)
            gone = true;
        if (gone && mutant != MARGIN_MUT_LOSTASDOWN)
            dest++;
        else if (!gone && (st == META_UP || st == META_DRAINING))
            pres++;
        else
            away++;
    }
    int base = mutant == MARGIN_MUT_MARGINOFF ? (int)k + (int)m : (int)k;
    int settled = (int)pres - base;
    uint8_t tier;
    if (dest > m)
        tier = MARGIN_TIER_LOST;
    else if (corrupt && mutant == MARGIN_MUT_CORRUPTFIRST)
        tier = MARGIN_TIER_CORRUPT;
    else if (pres < k)
        tier = MARGIN_TIER_UNAVAILABLE;
    else if (corrupt)
        tier = MARGIN_TIER_CORRUPT;
    else
        tier = MARGIN_TIER_OK;
    *settled_out = settled;
    *tier_out = tier;
    *present_out = pres;
    *away_out = away;
    *destroyed_out = dest;
}

static int tier_rank(uint8_t tier, int mutant)
{
    if (tier == MARGIN_TIER_LOST)
        return 4;
    if (mutant == MARGIN_MUT_CORRUPTFIRST) {
        if (tier == MARGIN_TIER_CORRUPT)
            return 3;
        if (tier == MARGIN_TIER_UNAVAILABLE)
            return 2;
    } else {
        if (tier == MARGIN_TIER_UNAVAILABLE)
            return 3;
        if (tier == MARGIN_TIER_CORRUPT)
            return 2;
    }
    if (tier == MARGIN_TIER_OK)
        return 1;
    return 0;
}

static void merge_geom(struct margins *out, bool *have, int settled, uint8_t tier, uint8_t pres, uint8_t away,
                       uint8_t dest, int mutant)
{
    if (!*have) {
        out->settled = settled;
        out->tier = tier;
        out->present = pres;
        out->away = away;
        out->destroyed = dest;
        *have = true;
        return;
    }
    if (settled < out->settled) {
        out->settled = settled;
        out->present = pres;
        out->away = away;
        out->destroyed = dest;
    }
    if (tier_rank(tier, mutant) > tier_rank(out->tier, mutant))
        out->tier = tier;
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
    if (v->nstripe == 0) {
        out->tier = MARGIN_TIER_NONE;
        return;
    }
    bool have = false;
    int s;
    uint8_t t, pres, away, dest;
    if (v->redundancy == META_RED_EC && v->ec_k > 0) {
        uint32_t npos = (uint32_t)v->ec_k + (uint32_t)v->ec_m;
        if (npos > v->nstripe)
            npos = v->nstripe;
        eval_geom(cp, v, destroyed, nd, corrupt, mutant, npos, v->ec_k, v->ec_m, &s, &t, &pres, &away, &dest);
        merge_geom(out, &have, s, t, pres, away, dest, mutant);
    }
    /* R10 (D29): during REENCODE/DROP, old EC objects still use orig_k+orig_m — take minimum margin across geometries. */
    if ((v->conv.phase == META_CONV_REENCODE || v->conv.phase == META_CONV_DROP) &&
        v->conv.orig_redundancy == META_RED_EC && v->conv.orig_k > 0 &&
        v->nstripe >= (uint32_t)v->conv.orig_k + (uint32_t)v->conv.orig_m) {
        uint32_t npos = (uint32_t)v->conv.orig_k + (uint32_t)v->conv.orig_m;
        eval_geom(cp, v, destroyed, nd, corrupt, mutant, npos, v->conv.orig_k, v->conv.orig_m, &s, &t, &pres, &away,
                  &dest);
        merge_geom(out, &have, s, t, pres, away, dest, mutant);
    }
    /* R10 (D29): after a cancelled REENCODE, leftover EC objects keep capping settled margin until cleared. */
    if (v->mixed_redundancy == META_RED_EC && v->mixed_k > 0 &&
        v->nstripe >= (uint32_t)v->mixed_k + (uint32_t)v->mixed_m) {
        uint32_t npos = (uint32_t)v->mixed_k + (uint32_t)v->mixed_m;
        eval_geom(cp, v, destroyed, nd, corrupt, mutant, npos, v->mixed_k, v->mixed_m, &s, &t, &pres, &away, &dest);
        merge_geom(out, &have, s, t, pres, away, dest, mutant);
    }
    if (!have)
        out->tier = MARGIN_TIER_NONE;
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
