/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure placement planner (server/place.h; tracks r9-plan and r9-return).
 *
 * Pure module (D7): NO I/O, NO clock reads, NO locks, NO globals mutated inside place_plan, place_volume,
 * place_stripe, or place_rack_survivable. Every input arrives in `struct place_in`; every decision leaves in
 * `struct place_out` (or output arrays for initial placement).
 *
 * Implements:
 *   - Initial log-group (`place_volume`) and EC stripe-group (`place_stripe`) placement obeying §3.2 rules 1-4,
 *     E9 (one position per server; best-effort rack cap minimizing per-rack counts toward ceil((k+m)/racks),
 *     exceeding it only when no other rack has a spare server), and D20.
 *   - Single-rack-loss survivability check (`place_rack_survivable`) for STATUS (E9, D20).
 *   - `place_plan`:
 *       1. `STRIPE_WHOLE` proposals for `META_STRIPE_FILLING` positions whose fill report reports `whole == 1` at the
 *          volume's current `stripe_version` (D10 version barrier).
 *       2. `place_return` empty hook (filled by r9-return, D13).
 *       3. Re-placement of log replicas and EC stripe positions on `META_OUT` / `META_DESTROYED` / `META_DRAINING`
 *          members, faulted replicas (D6, either forwarded from `pout_plan` or reported in `in->members[].faults`),
 *          and active `node replace OLD NEW` intents (D22), ordered by E13 (smallest margin first via
 *          `meta_vol_margins`, OUT/fault/replace before DRAINING, larger byte size first, stable by volume number;
 *          margin <= 0 preempts higher-margin queued and in-flight jobs), respecting `rebuild_max` per server sending
 *          and receiving, one log move per volume, and `rebuild_concurrency` per server for stripe fills.
 *
 * Planted mutants (`in->mutant`; `TSDB_PLACE_MUTANT` parsed once by `place_mutant_env` for tsdbd):
 *   - `orderfifo`      (PLACE_MUT_ORDER_FIFO):       skip E13 margin sorting (FIFO by volume number).
 *   - `rackignored`    (PLACE_MUT_RACK_IGNORED):     ignore E9 / §3.2 rack spreading.
 *   - `twoperserver`   (PLACE_MUT_TWO_PER_SERVER):   place a second replica/position on a server already holding one.
 *   - `moveshrinks`    (PLACE_MUT_MOVE_SHRINKS):     emit REPLICA_REMOVE before the replacement joins.
 *   - `wholenobarrier` (PLACE_MUT_WHOLE_NO_BARRIER): emit STRIPE_WHOLE ignoring `whole == 1` and `stripe_version`.
 */
#include "place.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void place_settings_default(struct place_settings *s)
{
    if (!s)
        return;
    *s = (struct place_settings){
        .rebuild_after_s = 1800,
        .opening_hold_s = 3600,
        .failstop_out_s = 60,
        .out_guard_pct = 25,
        .out_guard_hold_s = 7200,
        .repl_retain_down_gb = 64,
        .young_high_pct = 90,
        .balance_mb_s = 0,
        .rebuild_concurrency = 4,
    };
}

void place_out_init(struct place_out *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof *out);
}

void place_out_free(struct place_out *out)
{
    if (!out)
        return;
    free(out->props);
    free(out->logs);
    memset(out, 0, sizeof *out);
}

int place_out_push(struct place_out *out, const struct place_prop *p)
{
    if (!out || !p)
        return -1;
    if (out->nprops == out->cap_props) {
        uint32_t ncap = out->cap_props ? out->cap_props * 2 : 16;
        struct place_prop *np = realloc(out->props, (size_t)ncap * sizeof *np);
        if (!np)
            return -1;
        out->props = np;
        out->cap_props = ncap;
    }
    out->props[out->nprops++] = *p;
    return 0;
}

int place_out_log(struct place_out *out, const char *fmt, ...)
{
    if (!out || !fmt)
        return -1;
    if (out->nlogs == out->cap_logs) {
        uint32_t ncap = out->cap_logs ? out->cap_logs * 2 : 8;
        struct place_log *nl = realloc(out->logs, (size_t)ncap * sizeof *nl);
        if (!nl)
            return -1;
        out->logs = nl;
        out->cap_logs = ncap;
    }
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(out->logs[out->nlogs].msg, sizeof out->logs[out->nlogs].msg, fmt, ap);
    va_end(ap);
    out->nlogs++;
    return 0;
}

int place_mutant_parse(const char *name)
{
    if (!name || !*name)
        return PLACE_MUT_NONE;
    if (!strcmp(name, "orderfifo"))
        return PLACE_MUT_ORDER_FIFO;
    if (!strcmp(name, "rackignored"))
        return PLACE_MUT_RACK_IGNORED;
    if (!strcmp(name, "twoperserver"))
        return PLACE_MUT_TWO_PER_SERVER;
    if (!strcmp(name, "moveshrinks"))
        return PLACE_MUT_MOVE_SHRINKS;
    if (!strcmp(name, "wholenobarrier"))
        return PLACE_MUT_WHOLE_NO_BARRIER;
    if (!strcmp(name, "deleteunmarked"))
        return PLACE_MUT_DELETE_UNMARKED;
    if (!strcmp(name, "guardunbounded"))
        return PLACE_MUT_GUARD_UNBOUNDED;
    if (!strcmp(name, "noguard"))
        return PLACE_MUT_NO_GUARD;
    if (!strcmp(name, "holdignored"))
        return PLACE_MUT_HOLD_IGNORED;
    if (!strcmp(name, "openingunbounded"))
        return PLACE_MUT_OPENING_UNBOUNDED;
    if (!strcmp(name, "failstopslow"))
        return PLACE_MUT_FAILSTOP_SLOW;
    if (!strcmp(name, "balanceunbounded"))
        return PLACE_MUT_BALANCE_UNBOUNDED;
    return -1;
}

int place_mutant_env(void)
{
#ifdef TSDB_TEST_HOOKS
    static int cached = -1;
    if (cached < 0) {
        int m = place_mutant_parse(getenv("TSDB_PLACE_MUTANT"));
        cached = m > 0 ? m : PLACE_MUT_NONE;
    }
    return cached;
#else
    return PLACE_MUT_NONE;
#endif
}

/* ------------------------------------------------------------------ pure helpers */

static int32_t member_idx(const struct meta_state *sm, uint32_t id)
{
    if (!sm || !id)
        return -1;
    for (uint32_t i = 0; i < sm->nmembers; i++)
        if (sm->members[i].id == id)
            return (int32_t)i;
    return -1;
}

static const struct place_member *find_pm(const struct place_in *in, uint32_t id)
{
    if (!in || !in->members)
        return NULL;
    for (uint32_t i = 0; i < in->nmembers; i++)
        if (in->members[i].id == id)
            return &in->members[i];
    return NULL;
}

/* Returns 1 if `id` was heard within node_timeout_ms. Callers that pass an empty heartbeat snapshot
 * (members == NULL && nmembers == 0, minimal unit tests only) treat every member as freshly heard. */
static int member_heard_fresh(const struct place_in *in, uint32_t id)
{
    if (!in->members && !in->nmembers)
        return 1;
    if (in->self_id && id == in->self_id)
        return 1;
    const struct place_member *pm = find_pm(in, id);
    if (!pm)
        return 0;
    uint64_t timeout_ms = in->sm->set.node_timeout_ms ? in->sm->set.node_timeout_ms : 3000ull;
    return pm->heard_age_ms < timeout_ms;
}

static uint32_t member_chunk_stores(const struct place_in *in, uint32_t id)
{
    const struct place_member *pm = find_pm(in, id);
    return pm ? pm->chunk_stores : 0;
}

static uint32_t replace_target_of(const struct place_in *in, uint32_t old_node)
{
    if (!in->replaces)
        return 0;
    for (uint32_t i = 0; i < in->nreplaces; i++)
        if (in->replaces[i].old_node == old_node && in->replaces[i].new_node != 0)
            return in->replaces[i].new_node;
    return 0;
}

static int is_replace_old(const struct place_in *in, uint32_t id)
{
    return replace_target_of(in, id) != 0;
}

static int member_placeable(const struct place_in *in, const struct meta_member *mb)
{
    if (!mb || mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
        return 0;
    if (is_replace_old(in, mb->id))
        return 0;
    return member_heard_fresh(in, mb->id);
}

static int vol_has_rep(const struct meta_volume *v, uint32_t node)
{
    for (uint32_t i = 0; i < v->nrep; i++)
        if (v->rep[i].node == node)
            return 1;
    return 0;
}

static int vol_has_stripe(const struct meta_volume *v, uint32_t node)
{
    for (uint32_t i = 0; i < v->nstripe && i < META_STRIPE_MAX; i++)
        if (v->stripe[i] == node)
            return 1;
    return 0;
}

static int vol_has_move(const struct meta_volume *v)
{
    for (uint32_t i = 0; i < v->nrep; i++)
        if (v->rep[i].learner || v->rep[i].replaces)
            return 1;
    return 0;
}

/* Precompute symmetric nm x nm co-location matrices for §3.2 rule 4 pair spreading in O(nvolumes * group^2). */
static void build_pair_matrices(const struct meta_state *sm, uint32_t *log_pairs, uint32_t *stripe_pairs)
{
    uint32_t nm = sm->nmembers;
    if (!nm)
        return;
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (log_pairs && v->kind == META_VOL_REPLICATED) {
            int32_t idx[META_REPLICAS_MAX];
            uint32_t cnt = 0;
            for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++) {
                int32_t mi = member_idx(sm, v->rep[k].node);
                if (mi >= 0)
                    idx[cnt++] = mi;
            }
            for (uint32_t a = 0; a < cnt; a++)
                for (uint32_t b = a + 1; b < cnt; b++) {
                    log_pairs[(size_t)idx[a] * nm + (size_t)idx[b]]++;
                    log_pairs[(size_t)idx[b] * nm + (size_t)idx[a]]++;
                }
        }
        if (stripe_pairs && v->redundancy == META_RED_EC) {
            int32_t idx[META_STRIPE_MAX];
            uint32_t cnt = 0;
            for (uint32_t k = 0; k < v->nstripe && k < META_STRIPE_MAX; k++) {
                int32_t mi = member_idx(sm, v->stripe[k]);
                if (mi >= 0)
                    idx[cnt++] = mi;
            }
            for (uint32_t a = 0; a < cnt; a++)
                for (uint32_t b = a + 1; b < cnt; b++) {
                    stripe_pairs[(size_t)idx[a] * nm + (size_t)idx[b]]++;
                    stripe_pairs[(size_t)idx[b] * nm + (size_t)idx[a]]++;
                }
        }
    }
}

/* ------------------------------------------------------------------ initial placement (moved from cluster.c) */

int place_volume(const struct place_in *in, uint32_t n, uint32_t *nodes)
{
    if (!in || !in->sm || !nodes || n == 0 || n > META_REPLICAS_MAX)
        return -1;
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers;
    if (nm < n)
        return -1;
    uint32_t *cand = calloc(nm, sizeof *cand);
    uint32_t *leads = calloc(nm, sizeof *leads);
    uint32_t *holds = calloc(nm, sizeof *holds);
    uint8_t *used = calloc(nm, sizeof *used);
    uint32_t *log_pairs = calloc((size_t)nm * nm, sizeof *log_pairs);
    if (!cand || !leads || !holds || !used || !log_pairs) {
        free(cand);
        free(leads);
        free(holds);
        free(used);
        free(log_pairs);
        return -1;
    }
    build_pair_matrices(sm, log_pairs, NULL);
    uint32_t nc = 0;
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (!member_placeable(in, mb))
            continue;
        for (uint32_t v = 0; v < sm->nvolumes; v++) {
            const struct meta_volume *mv = &sm->volumes[v];
            for (uint32_t k = 0; k < mv->nrep; k++)
                if (mv->rep[k].node == mb->id) {
                    holds[nc]++;
                    leads[nc] += (k == 0);
                }
        }
        cand[nc++] = i;
    }
    int rc = -1;
    if (nc < n)
        goto done;
    int racks = (in->mutant != PLACE_MUT_RACK_IGNORED);
    for (uint32_t i = 0; i < nc && racks; i++)
        if (!sm->members[cand[i]].rack[0])
            racks = 0;
    int32_t picked_mi[META_REPLICAS_MAX];
    for (uint32_t k = 0; k < n; k++) {
        int best = -1;
        uint32_t best_rack = UINT32_MAX, best_pairs = UINT32_MAX;
        for (uint32_t i = 0; i < nc; i++) {
            if (in->mutant == PLACE_MUT_TWO_PER_SERVER && k > 0) {
                /* Planted mutant: intentionally pick an already-used server if one exists. */
                if (!used[i])
                    continue;
            } else if (used[i]) {
                continue;
            }
            const struct meta_member *mb = &sm->members[cand[i]];
            uint32_t rack_used = 0;
            if (racks) {
                for (uint32_t j = 0; j < k; j++)
                    if (!strcmp(mb->rack, sm->members[picked_mi[j]].rack))
                        rack_used++;
            }
            uint32_t pairs = 0;
            for (uint32_t j = 0; j < k; j++)
                pairs += log_pairs[(size_t)cand[i] * nm + (size_t)picked_mi[j]];
            int better = 0;
            if (best < 0) {
                better = 1;
            } else if (rack_used != best_rack) {
                better = rack_used < best_rack;
            } else if (k == 0) {
                if (leads[i] != leads[best])
                    better = leads[i] < leads[best];
                else if (holds[i] != holds[best])
                    better = holds[i] < holds[best];
                else
                    better = mb->id < sm->members[cand[best]].id;
            } else {
                if (holds[i] != holds[best])
                    better = holds[i] < holds[best];
                else if (pairs != best_pairs)
                    better = pairs < best_pairs;
                else
                    better = mb->id < sm->members[cand[best]].id;
            }
            if (better) {
                best = (int)i;
                best_rack = rack_used;
                best_pairs = pairs;
            }
        }
        if (best < 0)
            goto done;
        used[best] = 1;
        picked_mi[k] = (int32_t)cand[best];
        nodes[k] = sm->members[cand[best]].id;
    }
    rc = 0;
done:
    free(log_pairs);
    free(cand);
    free(leads);
    free(holds);
    free(used);
    return rc;
}

int place_stripe(const struct place_in *in, uint32_t ns, uint32_t *stripe, uint32_t *qualify)
{
    if (qualify)
        *qualify = 0;
    if (!in || !in->sm || !stripe || ns == 0 || ns > META_STRIPE_MAX)
        return -1;
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers;
    if (nm == 0)
        return -1;
    uint32_t *cand = calloc(nm, sizeof *cand);
    uint32_t *holds = calloc(nm, sizeof *holds);
    uint8_t *used = calloc(nm, sizeof *used);
    uint32_t *stripe_pairs = calloc((size_t)nm * nm, sizeof *stripe_pairs);
    if (!cand || !holds || !used || !stripe_pairs) {
        free(cand);
        free(holds);
        free(used);
        free(stripe_pairs);
        return -1;
    }
    build_pair_matrices(sm, NULL, stripe_pairs);
    uint32_t nc = 0;
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (!member_placeable(in, mb))
            continue;
        if (member_chunk_stores(in, mb->id) == 0)
            continue;
        for (uint32_t v = 0; v < sm->nvolumes; v++) {
            const struct meta_volume *mv = &sm->volumes[v];
            for (uint32_t k = 0; k < mv->nstripe && k < META_STRIPE_MAX; k++)
                holds[nc] += (mv->stripe[k] == mb->id);
        }
        cand[nc++] = i;
    }
    if (qualify)
        *qualify = nc;
    int rc = -1;
    if (nc < ns)
        goto done;
    int racks = (in->mutant != PLACE_MUT_RACK_IGNORED);
    for (uint32_t i = 0; i < nc && racks; i++)
        if (!sm->members[cand[i]].rack[0])
            racks = 0;
    int32_t picked_mi[META_STRIPE_MAX];
    for (uint32_t k = 0; k < ns; k++) {
        int best = -1;
        uint32_t best_rack = UINT32_MAX, best_pairs = UINT32_MAX;
        for (uint32_t i = 0; i < nc; i++) {
            if (in->mutant == PLACE_MUT_TWO_PER_SERVER && k > 0) {
                if (!used[i])
                    continue;
            } else if (used[i]) {
                continue;
            }
            const struct meta_member *mb = &sm->members[cand[i]];
            uint32_t rack_cnt = 0;
            if (racks) {
                for (uint32_t j = 0; j < k; j++)
                    if (!strcmp(mb->rack, sm->members[picked_mi[j]].rack))
                        rack_cnt++;
            }
            uint32_t pairs = 0;
            for (uint32_t j = 0; j < k; j++)
                pairs += stripe_pairs[(size_t)cand[i] * nm + (size_t)picked_mi[j]];
            int better = 0;
            if (best < 0) {
                better = 1;
            } else if (rack_cnt != best_rack) {
                better = rack_cnt < best_rack;
            } else if (holds[i] != holds[best]) {
                better = holds[i] < holds[best];
            } else if (pairs != best_pairs) {
                better = pairs < best_pairs;
            } else {
                better = mb->id < sm->members[cand[best]].id;
            }
            if (better) {
                best = (int)i;
                best_rack = rack_cnt;
                best_pairs = pairs;
            }
        }
        if (best < 0)
            goto done;
        used[best] = 1;
        picked_mi[k] = (int32_t)cand[best];
        stripe[k] = sm->members[cand[best]].id;
    }
    rc = 0;
done:
    free(stripe_pairs);
    free(cand);
    free(holds);
    free(used);
    return rc;
}

/* ------------------------------------------------------------------ rack-loss survivability (E9, D20) */

int place_rack_survivable(const struct meta_state *sm, const struct meta_volume *v)
{
    if (!sm || !v || v->kind != META_VOL_REPLICATED)
        return 0;
    /* Check log group (non-learner replicas): every member must carry a rack, and losing any one rack must keep at
     * least max(1, min_isr) replicas. */
    uint32_t nfull = 0, rep_nodes[META_REPLICAS_MAX];
    for (uint32_t i = 0; i < v->nrep && i < META_REPLICAS_MAX; i++) {
        if (v->rep[i].learner)
            continue;
        int32_t mi = member_idx(sm, v->rep[i].node);
        if (mi < 0 || !sm->members[mi].rack[0])
            return 0;
        rep_nodes[nfull++] = (uint32_t)mi;
    }
    if (nfull < 2)
        return 0;
    uint32_t max_rep_rack = 0;
    for (uint32_t i = 0; i < nfull; i++) {
        uint32_t cnt = 0;
        for (uint32_t j = 0; j < nfull; j++)
            if (!strcmp(sm->members[rep_nodes[i]].rack, sm->members[rep_nodes[j]].rack))
                cnt++;
        if (cnt > max_rep_rack)
            max_rep_rack = cnt;
    }
    uint32_t floor_rep = v->min_isr ? v->min_isr : 1;
    if (max_rep_rack >= nfull || (nfull - max_rep_rack) < floor_rep)
        return 0;

    /* For EC volumes, also check stripe group: every holder must carry a rack, and at most m positions may sit on
     * any single rack (E9: losing any one rack leaves at least k chunks). */
    if (v->redundancy == META_RED_EC) {
        if (v->nstripe == 0 || v->nstripe > META_STRIPE_MAX || v->nstripe != (uint32_t)v->ec_k + v->ec_m)
            return 0;
        uint32_t sp_idx[META_STRIPE_MAX];
        for (uint32_t i = 0; i < v->nstripe; i++) {
            int32_t mi = member_idx(sm, v->stripe[i]);
            if (mi < 0 || !sm->members[mi].rack[0])
                return 0;
            sp_idx[i] = (uint32_t)mi;
        }
        uint32_t max_stripe_rack = 0;
        for (uint32_t i = 0; i < v->nstripe; i++) {
            uint32_t cnt = 0;
            for (uint32_t j = 0; j < v->nstripe; j++)
                if (!strcmp(sm->members[sp_idx[i]].rack, sm->members[sp_idx[j]].rack))
                    cnt++;
            if (cnt > max_stripe_rack)
                max_stripe_rack = cnt;
        }
        if (max_stripe_rack > v->ec_m)
            return 0;
    }
    return 1;
}

/* ------------------------------------------------------------------ place_plan */

/* Empty hook owned by r9-return (D13, E28): called after STRIPE_WHOLE and before new re-placements. */
static void place_return(const struct place_in *in, struct place_out *out)
{
    (void)in;
    (void)out;
}

struct plan_live_ctx {
    const struct place_in *in;
    const uint8_t *pend_gone; /* per sm->members index: 1 if pout_plan proposed DOWN/OUT/DESTROYED this tick */
};

static int plan_member_live(const void *vctx, uint32_t node)
{
    const struct plan_live_ctx *ctx = vctx;
    int32_t mi = member_idx(ctx->in->sm, node);
    if (mi < 0 || ctx->pend_gone[mi])
        return 0;
    return member_heard_fresh(ctx->in, node);
}

static uint64_t vol_bytes_of(const struct place_in *in, uint32_t volume)
{
    if (!in->vol_sizes)
        return 0;
    for (uint32_t i = 0; i < in->nvol_sizes; i++)
        if (in->vol_sizes[i].volume == volume)
            return in->vol_sizes[i].bytes;
    return 0;
}

struct fault_req {
    uint32_t volume;
    uint32_t node;
};

static int fault_req_has(const struct fault_req *reqs, uint32_t nreqs, uint32_t volume, uint32_t node)
{
    for (uint32_t i = 0; i < nreqs; i++)
        if (reqs[i].volume == volume && reqs[i].node == node)
            return 1;
    return 0;
}

enum job_kind { JOB_LOG = 0, JOB_STRIPE = 1 };

struct place_job {
    enum job_kind kind;
    uint32_t vol_idx;
    uint32_t volume;
    uint32_t pos;           /* rep index (JOB_LOG) or stripe pos (JOB_STRIPE) */
    uint32_t old_node;      /* node being replaced */
    uint32_t forced_target; /* node replace OLD NEW target (0: planner picks) */
    int margin;             /* young margin (JOB_LOG) or settled margin (JOB_STRIPE) */
    uint8_t is_drain;       /* 1 if triggered only by META_DRAINING, 0 for OUT/DESTROYED/fault/replace */
    uint64_t bytes;         /* from in->vol_sizes (larger bytes first on equal margin) */
};

static int job_cmp(const void *va, const void *vb)
{
    const struct place_job *a = va, *b = vb;
    if (a->margin != b->margin)
        return a->margin < b->margin ? -1 : 1;
    if (a->is_drain != b->is_drain)
        return a->is_drain < b->is_drain ? -1 : 1;
    if (a->bytes != b->bytes)
        return a->bytes > b->bytes ? -1 : 1;
    if (a->volume != b->volume)
        return a->volume < b->volume ? -1 : 1;
    if (a->kind != b->kind)
        return a->kind < b->kind ? -1 : 1;
    if (a->pos != b->pos)
        return a->pos < b->pos ? -1 : 1;
    return 0;
}

/* Pick a log-replica target for volume `v`, replacing `v->rep[rep_idx]` (node A). Returns target node id or 0. */
static uint32_t pick_log_target(const struct place_in *in, const struct meta_volume *v, uint32_t rep_idx,
                                const uint8_t *pend_gone, const uint32_t *log_holds, const uint32_t *log_recv,
                                const uint32_t *log_recv_urgent, const uint32_t *log_pairs, uint32_t rebuild_max,
                                int urgent)
{
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers;
    uint32_t old_node = v->rep[rep_idx].node;
    int32_t rem_mi[META_REPLICAS_MAX];
    uint32_t nrem = 0;
    int racks = (in->mutant != PLACE_MUT_RACK_IGNORED);
    /* Racks count only when every remaining replica and every eligible candidate has a rack (D20). */
    for (uint32_t j = 0; j < v->nrep && j < META_REPLICAS_MAX; j++) {
        if (j == rep_idx)
            continue;
        int32_t ji = member_idx(sm, v->rep[j].node);
        if (ji >= 0) {
            rem_mi[nrem++] = ji;
            if (!sm->members[ji].rack[0])
                racks = 0;
        } else {
            racks = 0;
        }
    }
    for (uint32_t i = 0; i < nm && racks; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (pend_gone[i] || !member_placeable(in, mb))
            continue;
        if (!vol_has_rep(v, mb->id) && !mb->rack[0])
            racks = 0;
    }

    int32_t best_mi = -1;
    uint32_t best_rack = UINT32_MAX, best_holds = UINT32_MAX, best_pairs = UINT32_MAX;
    int best_dup = 0;
    for (uint32_t i = 0; i < nm; i++) {
        uint32_t used_recv = urgent ? log_recv_urgent[i] : log_recv[i];
        if (used_recv >= rebuild_max || pend_gone[i])
            continue;
        const struct meta_member *mb = &sm->members[i];
        if (!member_placeable(in, mb) || mb->id == old_node)
            continue;
        int dup = vol_has_rep(v, mb->id);
        if (dup && in->mutant != PLACE_MUT_TWO_PER_SERVER)
            continue;
        uint32_t rack_used = 0;
        if (racks) {
            for (uint32_t j = 0; j < nrem; j++)
                if (!strcmp(mb->rack, sm->members[rem_mi[j]].rack))
                    rack_used++;
        }
        uint32_t pairs = 0;
        for (uint32_t j = 0; j < nrem; j++)
            pairs += log_pairs[(size_t)i * nm + (size_t)rem_mi[j]];
        int better = 0;
        if (best_mi < 0) {
            better = 1;
        } else if (in->mutant == PLACE_MUT_TWO_PER_SERVER && dup != best_dup) {
            better = dup > best_dup; /* planted mutant: actively pick an existing holder of v */
        } else if (rack_used != best_rack) {
            better = rack_used < best_rack;
        } else if (log_holds[i] != best_holds) {
            better = log_holds[i] < best_holds;
        } else if (pairs != best_pairs) {
            better = pairs < best_pairs;
        } else {
            better = mb->id < sm->members[best_mi].id;
        }
        if (better) {
            best_mi = (int32_t)i;
            best_rack = rack_used;
            best_holds = log_holds[i];
            best_pairs = pairs;
            best_dup = dup;
        }
    }
    return best_mi >= 0 ? sm->members[best_mi].id : 0;
}

/* Pick a stripe-position target for EC volume `v`, replacing `v->stripe[pos]`. Returns target node id or 0. */
static uint32_t pick_stripe_target(const struct place_in *in, const struct meta_volume *v, uint32_t pos,
                                   const uint8_t *pend_gone, const uint32_t *stripe_holds, const uint32_t *stripe_recv,
                                   const uint32_t *stripe_recv_urgent, const uint32_t *stripe_pairs,
                                   uint32_t stripe_cap, int urgent)
{
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers;
    uint32_t old_node = v->stripe[pos];
    int32_t rem_mi[META_STRIPE_MAX];
    uint32_t nrem = 0;
    int racks = (in->mutant != PLACE_MUT_RACK_IGNORED);
    for (uint32_t j = 0; j < v->nstripe && j < META_STRIPE_MAX; j++) {
        if (j == pos)
            continue;
        int32_t ji = member_idx(sm, v->stripe[j]);
        if (ji >= 0) {
            rem_mi[nrem++] = ji;
            if (!sm->members[ji].rack[0])
                racks = 0;
        } else {
            racks = 0;
        }
    }
    for (uint32_t i = 0; i < nm && racks; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (pend_gone[i] || !member_placeable(in, mb) || member_chunk_stores(in, mb->id) == 0)
            continue;
        if (!vol_has_stripe(v, mb->id) && !mb->rack[0])
            racks = 0;
    }

    int32_t best_mi = -1;
    uint32_t best_rack = UINT32_MAX, best_holds = UINT32_MAX, best_pairs = UINT32_MAX;
    int best_dup = 0;
    for (uint32_t i = 0; i < nm; i++) {
        uint32_t used_recv = urgent ? stripe_recv_urgent[i] : stripe_recv[i];
        if (used_recv >= stripe_cap || pend_gone[i])
            continue;
        const struct meta_member *mb = &sm->members[i];
        if (!member_placeable(in, mb) || mb->id == old_node)
            continue;
        if (member_chunk_stores(in, mb->id) == 0)
            continue;
        int dup = vol_has_stripe(v, mb->id);
        if (dup && in->mutant != PLACE_MUT_TWO_PER_SERVER)
            continue;
        uint32_t rack_cnt = 0;
        if (racks) {
            for (uint32_t j = 0; j < nrem; j++)
                if (!strcmp(mb->rack, sm->members[rem_mi[j]].rack))
                    rack_cnt++;
        }
        uint32_t pairs = 0;
        for (uint32_t j = 0; j < nrem; j++)
            pairs += stripe_pairs[(size_t)i * nm + (size_t)rem_mi[j]];
        int better = 0;
        if (best_mi < 0) {
            better = 1;
        } else if (in->mutant == PLACE_MUT_TWO_PER_SERVER && dup != best_dup) {
            better = dup > best_dup;
        } else if (rack_cnt != best_rack) {
            better = rack_cnt < best_rack;
        } else if (stripe_holds[i] != best_holds) {
            better = stripe_holds[i] < best_holds;
        } else if (pairs != best_pairs) {
            better = pairs < best_pairs;
        } else {
            better = mb->id < sm->members[best_mi].id;
        }
        if (better) {
            best_mi = (int32_t)i;
            best_rack = rack_cnt;
            best_holds = stripe_holds[i];
            best_pairs = pairs;
            best_dup = dup;
        }
    }
    return best_mi >= 0 ? sm->members[best_mi].id : 0;
}

static uint32_t log_move_sender(const struct place_in *in, const struct meta_volume *v, const uint8_t *pend_gone)
{
    const struct meta_state *sm = in->sm;
    if (v->leader) {
        int32_t li = member_idx(sm, v->leader);
        if (li >= 0 && !pend_gone[li] &&
            (sm->members[li].state == META_UP || sm->members[li].state == META_DRAINING) &&
            member_heard_fresh(in, v->leader) && meta_volume_in_isr(v, v->leader))
            return v->leader;
    }
    for (uint32_t i = 0; i < v->nisr; i++) {
        int32_t mi = member_idx(sm, v->isr[i]);
        if (mi >= 0 && !pend_gone[mi] &&
            (sm->members[mi].state == META_UP || sm->members[mi].state == META_DRAINING) &&
            member_heard_fresh(in, v->isr[i]))
            return v->isr[i];
    }
    /* A live OUT leader that is the sole remaining ISR member (e.g. replicas=1 marked OUT) can still ship a snapshot
     * to its replacement before cmove transfers leadership off it. */
    if (v->leader) {
        int32_t li = member_idx(sm, v->leader);
        if (li >= 0 && !pend_gone[li] && sm->members[li].state == META_OUT && member_heard_fresh(in, v->leader) &&
            meta_volume_in_isr(v, v->leader))
            return v->leader;
    }
    return 0;
}

void place_plan(const struct place_in *in, struct place_out *out)
{
    if (!in || !in->sm || !out || !in->sm->initialised || in->sm->mode != META_MODE_REPLICATED)
        return;
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers, nv = sm->nvolumes;
    if (nm == 0 || nv == 0)
        return;

    uint8_t *pend_gone = calloc(nm, sizeof *pend_gone);
    uint32_t *log_holds = calloc(nm, sizeof *log_holds);
    uint32_t *log_recv = calloc(nm, sizeof *log_recv);
    uint32_t *log_send = calloc(nm, sizeof *log_send);
    uint32_t *log_recv_urg = calloc(nm, sizeof *log_recv_urg);
    uint32_t *log_send_urg = calloc(nm, sizeof *log_send_urg);
    uint32_t *stripe_holds = calloc(nm, sizeof *stripe_holds);
    uint32_t *stripe_recv = calloc(nm, sizeof *stripe_recv);
    uint32_t *stripe_recv_urg = calloc(nm, sizeof *stripe_recv_urg);
    uint8_t *vol_log_moved = calloc(nv, sizeof *vol_log_moved);
    uint8_t *vol_stripe_moved = calloc(nv, sizeof *vol_stripe_moved);
    uint32_t *whole_mask = calloc(nv, sizeof *whole_mask);
    int *vol_ym = calloc(nv, sizeof *vol_ym);
    int *vol_sm = calloc(nv, sizeof *vol_sm);
    uint32_t *log_pairs = calloc((size_t)nm * nm, sizeof *log_pairs);
    uint32_t *stripe_pairs = calloc((size_t)nm * nm, sizeof *stripe_pairs);
    struct fault_req *faults = NULL;
    uint32_t nfaults = 0, cap_faults = 0;
    struct place_job *jobs = NULL;
    uint32_t njobs = 0, cap_jobs = 0;

    if (!pend_gone || !log_holds || !log_recv || !log_send || !log_recv_urg || !log_send_urg || !stripe_holds ||
        !stripe_recv || !stripe_recv_urg || !vol_log_moved || !vol_stripe_moved || !whole_mask || !vol_ym || !vol_sm ||
        !log_pairs || !stripe_pairs)
        goto cleanup;

    /* 1. Inspect proposals emitted earlier this tick by pout_plan: note members being marked DOWN/OUT/DESTROYED,
     * and consume unresolved fault-replace requests (PLACE_PROP_REPLICA_ADD with node == 0 and other_node != 0). */
    uint32_t kept = 0;
    for (uint32_t i = 0; i < out->nprops; i++) {
        struct place_prop p = out->props[i];
        if (p.kind == PLACE_PROP_MEMBER_STATE && p.state >= META_DOWN && p.state != META_DRAINING) {
            int32_t mi = member_idx(sm, p.node);
            if (mi >= 0)
                pend_gone[mi] = 1;
            out->props[kept++] = p;
        } else if (p.kind == PLACE_PROP_REPLICA_ADD && p.node == 0 && p.other_node != 0) {
            if (!fault_req_has(faults, nfaults, p.volume, p.other_node)) {
                if (nfaults == cap_faults) {
                    uint32_t nc = cap_faults ? cap_faults * 2 : 16;
                    struct fault_req *nf = realloc(faults, (size_t)nc * sizeof *nf);
                    if (!nf)
                        goto cleanup;
                    faults = nf;
                    cap_faults = nc;
                }
                faults[nfaults++] = (struct fault_req){.volume = p.volume, .node = p.other_node};
            }
        } else {
            out->props[kept++] = p;
        }
    }
    out->nprops = kept;

    /* Also detect persistent per-replica LOCAL_FAULTs directly from in->members[].faults (D6) when UP & unheld. */
    if (in->members && in->set.failstop_out_s > 0) {
        for (uint32_t i = 0; i < in->nmembers; i++) {
            const struct place_member *pm = &in->members[i];
            int32_t mi = member_idx(sm, pm->id);
            if (mi < 0 || pend_gone[mi] || sm->members[mi].state != META_UP)
                continue;
            if (sm->members[mi].hold_until_ms && in->now_ms < sm->members[mi].hold_until_ms)
                continue;
            for (uint32_t j = 0; j < pm->nfaults && j < PLACE_FAULTS_MAX; j++) {
                if (pm->faults[j].fault_s < in->set.failstop_out_s)
                    continue;
                if (!fault_req_has(faults, nfaults, pm->faults[j].volume, pm->id)) {
                    if (nfaults == cap_faults) {
                        uint32_t nc = cap_faults ? cap_faults * 2 : 16;
                        struct fault_req *nf = realloc(faults, (size_t)nc * sizeof *nf);
                        if (!nf)
                            goto cleanup;
                        faults = nf;
                        cap_faults = nc;
                    }
                    faults[nfaults++] = (struct fault_req){.volume = pm->faults[j].volume, .node = pm->id};
                }
            }
        }
    }

    struct plan_live_ctx lctx = {.in = in, .pend_gone = pend_gone};
    struct meta_liveness liveness = {.live = plan_member_live, .ctx = &lctx};
    for (uint32_t vi = 0; vi < nv; vi++)
        meta_vol_margins(sm, &sm->volumes[vi], &liveness, &vol_ym[vi], &vol_sm[vi]);
    build_pair_matrices(sm, log_pairs, stripe_pairs);

    /* 2. Propose STRIPE_WHOLE for every FILLING position whose fill report says whole at the current version (D10).
     * Emitted before place_return and STRIPE_SET so whole commits aren't invalidated by a later stripe_version bump. */
    if (in->fills && in->nfills > 0) {
        for (uint32_t vi = 0; vi < nv; vi++) {
            const struct meta_volume *v = &sm->volumes[vi];
            if (v->redundancy != META_RED_EC)
                continue;
            int smarg = vol_sm[vi];
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                if (v->stripe_pos[p].state != META_STRIPE_FILLING)
                    continue;
                uint32_t holder = v->stripe[p];
                int commit_whole = 0;
                for (uint32_t fi = 0; fi < in->nfills; fi++) {
                    const struct place_fill *f = &in->fills[fi];
                    if (f->volume != v->number || f->pos != p || f->node != holder)
                        continue;
                    if (in->mutant == PLACE_MUT_WHOLE_NO_BARRIER) {
                        /* Planted mutant: ignores both `whole == 1` and the `stripe_version` barrier. */
                        commit_whole = 1;
                        break;
                    }
                    if (f->whole == 1 && f->stripe_version == v->stripe_version) {
                        commit_whole = 1;
                        break;
                    }
                }
                if (commit_whole) {
                    struct place_prop prop = {
                        .kind = PLACE_PROP_STRIPE_WHOLE,
                        .volume = v->number,
                        .node = holder,
                        .pos = p,
                        .version = v->stripe_version,
                        .margin = smarg,
                    };
                    if (place_out_push(out, &prop) != 0)
                        goto cleanup;
                    whole_mask[vi] |= (1u << p);
                }
            }
        }
    }

    /* 3. Return-during-rebuild hook (owned by r9-return, D13). */
    place_return(in, out);

    /* 4. Count current replica/stripe holdings and in-flight rebuilds per server. */
    for (uint32_t vi = 0; vi < nv; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        int ym = vol_ym[vi], smarg = vol_sm[vi];
        uint32_t sender = vol_has_move(v) ? log_move_sender(in, v, pend_gone) : 0;
        int32_t si = sender ? member_idx(sm, sender) : -1;
        for (uint32_t k = 0; k < v->nrep; k++) {
            int32_t mi = member_idx(sm, v->rep[k].node);
            if (mi < 0)
                continue;
            log_holds[mi]++;
            if (v->rep[k].learner || v->rep[k].replaces) {
                log_recv[mi]++;
                if (ym <= 0)
                    log_recv_urg[mi]++;
                if (si >= 0) {
                    log_send[si]++;
                    if (ym <= 0)
                        log_send_urg[si]++;
                }
            }
        }
        if (v->redundancy == META_RED_EC) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                int32_t mi = member_idx(sm, v->stripe[p]);
                if (mi < 0)
                    continue;
                stripe_holds[mi]++;
                if (v->stripe_pos[p].state == META_STRIPE_FILLING && !(whole_mask[vi] & (1u << p))) {
                    stripe_recv[mi]++;
                    if (smarg <= 0)
                        stripe_recv_urg[mi]++;
                }
            }
        }
    }

    /* 5. Collect candidate re-placement jobs (log moves and stripe fills). */
    for (uint32_t vi = 0; vi < nv; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        int ym = vol_ym[vi], smarg = vol_sm[vi];
        uint64_t vbytes = vol_bytes_of(in, v->number);

        /* Log replica re-placement: at most one move per volume (never start while a learner/move is in flight). */
        if (!vol_has_move(v) && v->nrep < META_REPLICAS_MAX) {
            int best_prio = 99; /* 1: OUT/DESTROYED, 2: fault, 3: replace intent, 4: DRAINING */
            uint32_t best_k = 0, best_forced = 0;
            for (uint32_t k = 0; k < v->nrep; k++) {
                uint32_t a = v->rep[k].node;
                int32_t ai = member_idx(sm, a);
                const struct meta_member *mb = ai >= 0 ? &sm->members[ai] : NULL;
                uint32_t rtarget = replace_target_of(in, a);
                int prio = 99;
                if (rtarget) {
                    const struct meta_member *nt = meta_member(sm, rtarget);
                    if (nt && nt->state < META_OUT)
                        prio = (!mb || mb->state >= META_OUT) ? 1 : 3;
                    else if (!mb || mb->state >= META_OUT) {
                        rtarget = 0;
                        prio = 1;
                    }
                } else if (!mb || mb->state == META_OUT || mb->state == META_DESTROYED) {
                    prio = 1;
                } else if (fault_req_has(faults, nfaults, v->number, a)) {
                    prio = 2;
                } else if (mb->state == META_DRAINING) {
                    prio = 4;
                }
                if (prio < best_prio) {
                    best_prio = prio;
                    best_k = k;
                    best_forced = rtarget;
                }
            }
            if (best_prio < 99) {
                if (njobs == cap_jobs) {
                    uint32_t nc = cap_jobs ? cap_jobs * 2 : 32;
                    struct place_job *nj = realloc(jobs, (size_t)nc * sizeof *nj);
                    if (!nj)
                        goto cleanup;
                    jobs = nj;
                    cap_jobs = nc;
                }
                jobs[njobs++] = (struct place_job){
                    .kind = JOB_LOG,
                    .vol_idx = vi,
                    .volume = v->number,
                    .pos = best_k,
                    .old_node = v->rep[best_k].node,
                    .forced_target = best_forced,
                    .margin = ym,
                    .is_drain = (uint8_t)(best_prio == 4),
                    .bytes = vbytes,
                };
            }
        }

        /* Stripe position re-placement for EC volumes. */
        if (v->redundancy == META_RED_EC) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                uint32_t a = v->stripe[p];
                int32_t ai = member_idx(sm, a);
                const struct meta_member *mb = ai >= 0 ? &sm->members[ai] : NULL;
                uint32_t rtarget = replace_target_of(in, a);
                int need = 0, is_drain = 0;
                if (rtarget) {
                    const struct meta_member *nt = meta_member(sm, rtarget);
                    if (nt && nt->state < META_OUT) {
                        need = 1;
                    } else if (!mb || mb->state >= META_OUT) {
                        rtarget = 0;
                        need = 1;
                    }
                } else if (!mb || mb->state == META_OUT || mb->state == META_DESTROYED) {
                    need = 1;
                } else if (mb->state == META_DRAINING) {
                    need = 1;
                    is_drain = 1;
                }
                if (!need)
                    continue;
                if (njobs == cap_jobs) {
                    uint32_t nc = cap_jobs ? cap_jobs * 2 : 32;
                    struct place_job *nj = realloc(jobs, (size_t)nc * sizeof *nj);
                    if (!nj)
                        goto cleanup;
                    jobs = nj;
                    cap_jobs = nc;
                }
                jobs[njobs++] = (struct place_job){
                    .kind = JOB_STRIPE,
                    .vol_idx = vi,
                    .volume = v->number,
                    .pos = p,
                    .old_node = a,
                    .forced_target = rtarget,
                    .margin = smarg,
                    .is_drain = (uint8_t)is_drain,
                    .bytes = vbytes,
                };
            }
        }
    }

    /* 6. Order jobs by E13 (smallest margin first, then non-drain, then larger bytes, then volume number).
     * Mutant PLACE_MUT_ORDER_FIFO leaves jobs in volume-number (FIFO) order. */
    if (njobs > 1 && in->mutant != PLACE_MUT_ORDER_FIFO)
        qsort(jobs, njobs, sizeof *jobs, job_cmp);

    uint32_t rebuild_max = sm->set.rebuild_max ? sm->set.rebuild_max : 2;
    uint32_t stripe_cap = in->set.rebuild_concurrency
                              ? in->set.rebuild_concurrency
                              : (sm->set.rebuild_concurrency ? sm->set.rebuild_concurrency : 4);

    /* 7. Emit proposals respecting concurrency limits (margin <= 0 preempts higher-margin jobs). */
    for (uint32_t ji = 0; ji < njobs; ji++) {
        const struct place_job *j = &jobs[ji];
        const struct meta_volume *v = &sm->volumes[j->vol_idx];
        int urgent = j->margin <= 0;

        if (j->kind == JOB_LOG) {
            if (vol_log_moved[j->vol_idx])
                continue;
            uint32_t sender = log_move_sender(in, v, pend_gone);
            if (!sender)
                continue; /* no reachable ISR member to ship a snapshot/log from: wait instead of wedging move slot */
            int32_t si = member_idx(sm, sender);
            if (si < 0)
                continue;
            uint32_t used_send = urgent ? log_send_urg[si] : log_send[si];
            if (used_send >= rebuild_max)
                continue;
            uint32_t target = 0;
            if (j->forced_target) {
                int32_t ti = member_idx(sm, j->forced_target);
                if (ti >= 0 && !pend_gone[ti] && member_placeable(in, &sm->members[ti]) &&
                    !vol_has_rep(v, j->forced_target)) {
                    uint32_t used_recv = urgent ? log_recv_urg[ti] : log_recv[ti];
                    if (used_recv < rebuild_max)
                        target = j->forced_target;
                }
            } else {
                target = pick_log_target(in, v, j->pos, pend_gone, log_holds, log_recv, log_recv_urg, log_pairs,
                                         rebuild_max, urgent);
            }
            if (!target)
                continue;
            int32_t ti = member_idx(sm, target);
            if (in->mutant == PLACE_MUT_MOVE_SHRINKS) {
                /* Planted mutant: drops old replica before replacement joins the ISR. */
                struct place_prop rem = {
                    .kind = PLACE_PROP_REPLICA_REMOVE,
                    .volume = v->number,
                    .node = j->old_node,
                    .epoch = v->epoch,
                    .margin = j->margin,
                };
                if (place_out_push(out, &rem) != 0)
                    goto cleanup;
                struct place_prop add = {
                    .kind = PLACE_PROP_REPLICA_ADD,
                    .volume = v->number,
                    .node = target,
                    .other_node = 0,
                    .epoch = v->epoch,
                    .margin = j->margin,
                };
                if (place_out_push(out, &add) != 0)
                    goto cleanup;
            } else {
                struct place_prop add = {
                    .kind = PLACE_PROP_REPLICA_ADD,
                    .volume = v->number,
                    .node = target,
                    .other_node = j->old_node,
                    .epoch = v->epoch,
                    .margin = j->margin,
                };
                if (place_out_push(out, &add) != 0)
                    goto cleanup;
            }
            vol_log_moved[j->vol_idx] = 1;
            if (ti >= 0) {
                log_holds[ti]++;
                log_recv[ti]++;
                if (urgent)
                    log_recv_urg[ti]++;
            }
            if (si >= 0) {
                log_send[si]++;
                if (urgent)
                    log_send_urg[si]++;
            }
        } else {
            if (vol_stripe_moved[j->vol_idx])
                continue;
            uint32_t target = 0;
            if (j->forced_target) {
                int32_t ti = member_idx(sm, j->forced_target);
                if (ti >= 0 && !pend_gone[ti] && member_placeable(in, &sm->members[ti]) &&
                    member_chunk_stores(in, j->forced_target) > 0 && !vol_has_stripe(v, j->forced_target)) {
                    uint32_t used_recv = urgent ? stripe_recv_urg[ti] : stripe_recv[ti];
                    if (used_recv < stripe_cap)
                        target = j->forced_target;
                }
            } else {
                target = pick_stripe_target(in, v, j->pos, pend_gone, stripe_holds, stripe_recv, stripe_recv_urg,
                                            stripe_pairs, stripe_cap, urgent);
            }
            if (!target)
                continue;
            int32_t ti = member_idx(sm, target);
            uint32_t prev = j->old_node;
            if (v->stripe_pos[j->pos].state == META_STRIPE_FILLING && v->stripe_pos[j->pos].prev != 0 &&
                v->stripe_pos[j->pos].prev != target && member_idx(sm, v->stripe_pos[j->pos].prev) >= 0)
                prev = v->stripe_pos[j->pos].prev;
            if (prev == target)
                prev = 0;
            struct place_prop prop = {
                .kind = PLACE_PROP_STRIPE_SET,
                .volume = v->number,
                .node = target,
                .other_node = prev,
                .pos = j->pos,
                .version = v->stripe_version,
                .reason = META_STRIPE_FILLING,
                .margin = j->margin,
            };
            if (place_out_push(out, &prop) != 0)
                goto cleanup;
            vol_stripe_moved[j->vol_idx] = 1;
            if (ti >= 0) {
                stripe_holds[ti]++;
                stripe_recv[ti]++;
                if (urgent)
                    stripe_recv_urg[ti]++;
            }
        }
    }

cleanup:
    free(jobs);
    free(faults);
    free(stripe_pairs);
    free(log_pairs);
    free(vol_sm);
    free(vol_ym);
    free(whole_mask);
    free(vol_stripe_moved);
    free(vol_log_moved);
    free(stripe_recv_urg);
    free(stripe_recv);
    free(stripe_holds);
    free(log_send_urg);
    free(log_recv_urg);
    free(log_send);
    free(log_recv);
    free(log_holds);
    free(pend_gone);
}
