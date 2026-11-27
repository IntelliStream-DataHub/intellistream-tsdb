/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure DOWN/OUT timer planner (server/place.h; track r9-out; R9 plan D2-D6, E12).
 *
 * Evaluates DOWN-to-OUT timers (`rebuild_after_s`, `failstop_out_s`), opening holds (`opening_hold_s`), operator
 * holds (`hold_until_ms`), the computed mass-OUT guard (`out_guard_pct`, `out_guard_hold_s`, single-rack guard),
 * and per-replica LOCAL_FAULT escalation. Pure function: no I/O, no clock reads, no locks, no mutable globals.
 *
 * Planted mutants (via `in->mutant`, or `TSDB_PLACE_MUTANT` under TSDB_TEST_HOOKS):
 *   - noguard          (PLACE_MUT_NO_GUARD):          skips the mass-OUT guard (percentage and rack)
 *   - guardunbounded   (PLACE_MUT_GUARD_UNBOUNDED):   holds OUT forever while guarded, ignoring out_guard_hold_s
 *   - holdignored      (PLACE_MUT_HOLD_IGNORED):      ignores member hold_until_ms
 *   - openingunbounded (PLACE_MUT_OPENING_UNBOUNDED): holds OUT forever while opening, ignoring opening_hold_s
 *   - failstopslow     (PLACE_MUT_FAILSTOP_SLOW):     uses rebuild_after_s instead of failstop_out_s
 */
#include "place.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static int pout_mutant(const struct place_in *in)
{
    if (in->mutant != PLACE_MUT_NONE)
        return in->mutant;
#ifdef TSDB_TEST_HOOKS
    const char *e = getenv("TSDB_PLACE_MUTANT");
    if (!e || !*e)
        return PLACE_MUT_NONE;
    if (!strcmp(e, "noguard"))
        return PLACE_MUT_NO_GUARD;
    if (!strcmp(e, "guardunbounded"))
        return PLACE_MUT_GUARD_UNBOUNDED;
    if (!strcmp(e, "holdignored"))
        return PLACE_MUT_HOLD_IGNORED;
    if (!strcmp(e, "openingunbounded"))
        return PLACE_MUT_OPENING_UNBOUNDED;
    if (!strcmp(e, "failstopslow"))
        return PLACE_MUT_FAILSTOP_SLOW;
#endif
    return PLACE_MUT_NONE;
}

static uint64_t add_sat_u64(uint64_t a, uint64_t b)
{
    return b > UINT64_MAX - a ? UINT64_MAX : a + b;
}

static const struct place_member *find_pm(const struct place_in *in, uint32_t id)
{
    for (uint32_t i = 0; i < in->nmembers; i++)
        if (in->members[i].id == id)
            return &in->members[i];
    return NULL;
}

static int pm_heard_fresh(const struct place_member *pm, uint64_t node_to_ms)
{
    if (!pm || pm->heard_age_ms == UINT64_MAX)
        return 0;
    return node_to_ms == 0 || pm->heard_age_ms < node_to_ms;
}

/* 1 if `node` is currently the target of an active log replica move (v->rep[k].replaces != 0) on any volume. */
static int is_mid_move_target(const struct meta_state *sm, uint32_t node)
{
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        for (uint32_t k = 0; k < v->nrep; k++)
            if (v->rep[k].node == node && v->rep[k].replaces != 0)
                return 1;
    }
    return 0;
}

/* 1 if `node` is in the ISR of a replicated volume that has no other UP/DRAINING member in its ISR. */
static int is_last_isr_member(const struct meta_state *sm, uint32_t node)
{
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !meta_volume_in_isr(v, node))
            continue;
        int other_alive = 0;
        for (uint32_t j = 0; j < v->nisr; j++) {
            uint32_t u = v->isr[j];
            if (u == node)
                continue;
            const struct meta_member *um = meta_member(sm, u);
            if (um && (um->state == META_UP || um->state == META_DRAINING)) {
                other_alive = 1;
                break;
            }
        }
        if (!other_alive)
            return 1;
    }
    return 0;
}

static int vol_has_move(const struct meta_volume *v)
{
    for (uint32_t k = 0; k < v->nrep; k++)
        if (v->rep[k].replaces != 0)
            return 1;
    return 0;
}

static int out_has_member_prop(const struct place_out *out, uint32_t node)
{
    for (uint32_t i = 0; i < out->nprops; i++)
        if (out->props[i].kind == PLACE_PROP_MEMBER_STATE && out->props[i].node == node)
            return 1;
    return 0;
}

static int out_has_replica_add(const struct place_out *out, uint32_t vol)
{
    for (uint32_t i = 0; i < out->nprops; i++)
        if (out->props[i].kind == PLACE_PROP_REPLICA_ADD && out->props[i].volume == vol)
            return 1;
    return 0;
}

static void sort_u64(uint64_t *a, uint32_t n)
{
    for (uint32_t i = 1; i < n; i++) {
        uint64_t v = a[i];
        uint32_t j = i;
        while (j > 0 && a[j - 1] > v) {
            a[j] = a[j - 1];
            j--;
        }
        a[j] = v;
    }
}

static uint64_t kth_down_since(const struct meta_state *sm, uint32_t k_1based)
{
    /* Selection without heap allocation for large clusters (> 128 DOWN members): O(n^2) finds the k-th smallest
     * down_since_ms with ties handled by counting strictly smaller values and equal values at earlier indices. */
    uint32_t target = k_1based - 1;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mi = &sm->members[i];
        if (mi->state != META_DOWN || mi->down_since_ms == 0)
            continue;
        uint32_t rank = 0;
        for (uint32_t j = 0; j < sm->nmembers; j++) {
            const struct meta_member *mj = &sm->members[j];
            if (mj->state != META_DOWN || mj->down_since_ms == 0)
                continue;
            if (mj->down_since_ms < mi->down_since_ms || (mj->down_since_ms == mi->down_since_ms && j < i))
                rank++;
        }
        if (rank == target)
            return mi->down_since_ms;
    }
    return 0;
}

void pout_plan(const struct place_in *in, struct place_out *out)
{
    if (!in || !in->sm || !out || !in->sm->initialised)
        return;

    const struct meta_state *sm = in->sm;
    int mut = pout_mutant(in);
    uint64_t node_to_ms = sm->set.node_timeout_ms;

    /* 1. Fail-stop fast path (D5): an UP member reporting a fresh failstop beat goes DOWN with reason failstop.
     *    In tsdbd, cluster.c's member_states + place_duty still gate any UP -> DOWN transition on promote_safe_ns so
     *    a volume leader's read lease runs out before promotion. */
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->id == in->self_id || m->state != META_UP)
            continue;
        const struct place_member *pm = find_pm(in, m->id);
        if (!pm || !pm->failstop || !pm_heard_fresh(pm, node_to_ms))
            continue;
        struct place_prop p = {
            .kind = PLACE_PROP_MEMBER_STATE,
            .node = m->id,
            .version = m->incarnation,
            .state = META_DOWN,
            .reason = META_DOWN_FAILSTOP,
        };
        (void)place_out_push(out, &p);
    }

    /* 2. Mass-OUT guard (D4): computed solely from the metadata record so every Raft leader agrees.
     *    Active while > out_guard_pct % of non-OUT/non-DESTROYED members are DOWN, OR every non-OUT/non-DESTROYED
     *    member of one multi-server rack (>= 2 members in the rack, every non-OUT/non-DESTROYED member has a rack,
     *    and at least two distinct racks exist) is DOWN. Its start is the down_since_ms of the member that crossed
     *    the line. */
    uint32_t non_out = 0, ndown = 0;
    uint64_t downs_stack[128];
    const uint32_t downs_cap = (uint32_t)(sizeof downs_stack / sizeof downs_stack[0]);

    int all_have_rack = 1, multi_rack = 0;
    const char *first_rack = NULL;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->state == META_OUT || m->state == META_DESTROYED)
            continue;
        non_out++;
        if (m->rack[0] == '\0') {
            all_have_rack = 0;
        } else if (!first_rack) {
            first_rack = m->rack;
        } else if (strcmp(m->rack, first_rack) != 0) {
            multi_rack = 1;
        }
        if (m->state == META_DOWN && m->down_since_ms > 0) {
            if (ndown < downs_cap)
                downs_stack[ndown] = m->down_since_ms;
            ndown++;
        }
    }

    int pct_guarded = 0;
    uint64_t pct_guard_start_ms = 0;
    if (non_out > 0 && (uint64_t)ndown * 100ull > (uint64_t)non_out * (uint64_t)in->set.out_guard_pct) {
        pct_guarded = 1;
        uint32_t k_cross = (uint32_t)(((uint64_t)non_out * (uint64_t)in->set.out_guard_pct) / 100ull + 1ull);
        if (k_cross == 0)
            k_cross = 1;
        if (k_cross > ndown)
            k_cross = ndown;
        if (ndown <= downs_cap) {
            sort_u64(downs_stack, ndown);
            pct_guard_start_ms = downs_stack[k_cross - 1];
        } else {
            pct_guard_start_ms = kth_down_since(sm, k_cross);
        }
    }

    int rack_guarded = 0;
    uint64_t rack_guard_start_ms = 0;
    if (all_have_rack && multi_rack) {
        for (uint32_t i = 0; i < sm->nmembers; i++) {
            const struct meta_member *mi = &sm->members[i];
            if (mi->state == META_OUT || mi->state == META_DESTROYED)
                continue;
            int seen = 0;
            for (uint32_t j = 0; j < i; j++) {
                const struct meta_member *mj = &sm->members[j];
                if (mj->state != META_OUT && mj->state != META_DESTROYED && strcmp(mj->rack, mi->rack) == 0) {
                    seen = 1;
                    break;
                }
            }
            if (seen)
                continue;
            uint32_t r_tot = 0, r_down = 0;
            uint64_t r_max_since = 0;
            for (uint32_t k = i; k < sm->nmembers; k++) {
                const struct meta_member *mk = &sm->members[k];
                if (mk->state == META_OUT || mk->state == META_DESTROYED)
                    continue;
                if (strcmp(mk->rack, mi->rack) != 0)
                    continue;
                r_tot++;
                if (mk->state == META_DOWN && mk->down_since_ms > 0) {
                    r_down++;
                    if (mk->down_since_ms > r_max_since)
                        r_max_since = mk->down_since_ms;
                }
            }
            if (r_tot >= 2 && r_down == r_tot) {
                if (!rack_guarded || r_max_since < rack_guard_start_ms) {
                    rack_guarded = 1;
                    rack_guard_start_ms = r_max_since;
                }
            }
        }
    }

    int guarded = pct_guarded || rack_guarded;
    if (mut == PLACE_MUT_NO_GUARD)
        guarded = 0;

    uint64_t guard_start_ms = 0;
    if (pct_guarded && rack_guarded)
        guard_start_ms = pct_guard_start_ms < rack_guard_start_ms ? pct_guard_start_ms : rack_guard_start_ms;
    else if (pct_guarded)
        guard_start_ms = pct_guard_start_ms;
    else if (rack_guarded)
        guard_start_ms = rack_guard_start_ms;

    uint64_t guard_due_ms = add_sat_u64(guard_start_ms, (uint64_t)in->set.out_guard_hold_s * 1000ull);
    int guard_holding = 0, guard_passed = 0;
    if (guarded) {
        if (mut == PLACE_MUT_GUARD_UNBOUNDED || in->now_ms < guard_due_ms)
            guard_holding = 1;
        else
            guard_passed = 1;
    }

    /* 3. Evaluate DOWN -> OUT timers for each DOWN member (D3, D4, D5, E12). */
    int logged_guard_passed = 0;
    if (!guard_holding) {
        for (uint32_t i = 0; i < sm->nmembers; i++) {
            const struct meta_member *m = &sm->members[i];
            if (m->state != META_DOWN || m->down_since_ms == 0 || m->id == in->self_id)
                continue;
            if (mut != PLACE_MUT_HOLD_IGNORED && m->hold_until_ms > in->now_ms)
                continue;

            uint32_t base_s = (m->down_reason == META_DOWN_FAILSTOP && mut != PLACE_MUT_FAILSTOP_SLOW)
                                  ? in->set.failstop_out_s
                                  : in->set.rebuild_after_s;
            uint64_t due_ms = add_sat_u64(m->down_since_ms, (uint64_t)base_s * 1000ull);

            const struct place_member *pm = find_pm(in, m->id);
            int is_opening = pm && pm->opening && pm_heard_fresh(pm, node_to_ms);
            if (is_opening) {
                if (mut == PLACE_MUT_OPENING_UNBOUNDED)
                    continue;
                due_ms = add_sat_u64(due_ms, (uint64_t)in->set.opening_hold_s * 1000ull);
            }

            if (in->now_ms < due_ms)
                continue;

            /* Safety exclusions: never OUT a mid-move target; never OUT the last ISR member while merely DOWN under a
             * guard (even after the guard bound passes). */
            if (is_mid_move_target(sm, m->id))
                continue;
            if (guarded && m->down_reason != META_DOWN_FAILSTOP && is_last_isr_member(sm, m->id))
                continue;

            struct place_prop p = {
                .kind = PLACE_PROP_MEMBER_STATE,
                .node = m->id,
                .version = m->incarnation,
                .state = META_OUT,
                .reason = META_DOWN_NONE,
            };
            (void)place_out_push(out, &p);

            if (guard_passed && !logged_guard_passed) {
                logged_guard_passed = 1;
                (void)place_out_log(out,
                                    "pout: guard bound passed (%" PRIu32 "/%" PRIu32 " down, held %" PRIu32
                                    " s); proceeding with OUT",
                                    ndown, non_out, in->set.out_guard_hold_s);
            }
        }
    }

    /* 3b. Escalate a DOWN member's reason to failstop if a failstop beat arrived and OUT was not proposed yet. */
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->id == in->self_id || m->state != META_DOWN || m->down_reason == META_DOWN_FAILSTOP)
            continue;
        if (out_has_member_prop(out, m->id))
            continue;
        const struct place_member *pm = find_pm(in, m->id);
        if (!pm || !pm->failstop || pm->heard_age_ms == UINT64_MAX)
            continue;
        struct place_prop p = {
            .kind = PLACE_PROP_MEMBER_STATE,
            .node = m->id,
            .version = m->incarnation,
            .state = META_DOWN,
            .reason = META_DOWN_FAILSTOP,
        };
        (void)place_out_push(out, &p);
    }

    /* 4. Per-replica LOCAL_FAULT escalation (D6): an UP member reporting LOCAL_FAULT on a replicated volume
     *    continuously for >= failstop_out_s yields a replace-replica request (consumed by place.c as a log move with
     *    replaces = that node). */
    uint32_t fault_s_lim = (mut == PLACE_MUT_FAILSTOP_SLOW) ? in->set.rebuild_after_s : in->set.failstop_out_s;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->state != META_UP)
            continue;
        const struct place_member *pm = find_pm(in, m->id);
        if (!pm || !pm->nfaults || !pm_heard_fresh(pm, node_to_ms))
            continue;
        uint32_t nf = pm->nfaults < PLACE_FAULTS_MAX ? pm->nfaults : PLACE_FAULTS_MAX;
        for (uint32_t j = 0; j < nf; j++) {
            if (pm->faults[j].fault_s < fault_s_lim)
                continue;
            const struct meta_volume *v = meta_volume(sm, pm->faults[j].volume);
            if (!v || v->kind != META_VOL_REPLICATED)
                continue;
            if (!meta_replica(v, m->id) || vol_has_move(v))
                continue;
            if (out_has_replica_add(out, v->number))
                continue;
            struct place_prop p = {
                .kind = PLACE_PROP_REPLICA_ADD,
                .volume = v->number,
                .node = 0, /* target chosen by place_plan (r9-plan) */
                .other_node = m->id,
                .epoch = v->epoch,
                .reason = META_DOWN_FAULT,
                .margin = (int)v->nisr - (int)v->min_isr,
            };
            (void)place_out_push(out, &p);
        }
    }
}
