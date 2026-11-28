/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure cluster balancer (server/place.h; track r9-balance; R9 plan D19;
 * docs/design-cluster.md §3.2, docs/design-cluster-data.md §6.2, §6.3, E9, E51).
 *
 * Pure module (D7): NO I/O, NO clock reads, NO locks, NO globals mutated.
 *
 * Balances:
 *   1. Log replica lists (§3.2 rules 1-4): even replica counts across compute-capable UP members,
 *      preferred-leader (rep[0]) balance, pair spreading, E9 rack survivability never broken,
 *      and R13 capability/role/young_move_pct respected via place_member_log_ok().
 *   2. EC stripe positions (§6.2, E9, E51): chunk bytes per capacity (when meaningful byte-usage
 *      imbalance >= 5% is reported) and capacity-weighted position counts across storage-capable
 *      UP members of the same disk class, never worsening class mix, never breaking E9 rack rule,
 *      and never exceeding the chunk fill mark (place_member_stripe_ok()).
 *   3. Volume leaders (§3.2 rule 2): planned leader transfers (PLACE_PROP_VOLUME_TRANSFER) when
 *      leader_balance_s == 0 (when leader_balance_s > 0, R4's ctransfer.c owns preferred-leader
 *      restoration, and moving rep[0] in step 1 shifts leadership cleanly via cmove without ping-pong).
 *
 * Bounds & safety invariants:
 *   - NEVER runs while any rebuild, drain, OUT evacuation, node replace, replica fault, degraded
 *     ISR/stripe, or redundancy conversion is queued or active.
 *   - NEVER lowers any margin: log moves emit REPLICA_ADD(replaces=src) (add-before-drop), stripe
 *     moves emit STRIPE_SET(FILLING, prev=src) (copy from UP prev, whole before MK_GC release),
 *     and endangered volumes (margin <= 0) are never touched.
 *   - Concurrent bound: at most 5% of a group's positions moving at once (minimum 1), enforced both
 *     per volume and cluster-wide alongside rebuild_max and rebuild_concurrency.
 *   - Automatic plan cap: no automatic plan moves more than 25% of the cluster's positions
 *     (`max_plan = total_pos / 4`, intentionally 0 for tiny clusters with < 4 total positions so
 *     moving >= 1 of <= 3 positions requires operator `--apply`) — when exceeded it stops and logs;
 *     the operator runs `tsdb cluster balance --apply` (in->balance_apply = 1) to override the 25%
 *     cap (still respecting the 5% concurrent bound).
 *
 * Planted mutant:
 *   - `balanceunbounded` (PLACE_MUT_BALANCE_UNBOUNDED): ignores the 25% automatic plan cap.
 */
#include "place.h"
#include "pace.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

/* 5% utilization gap threshold (50,000 ppm) for byte-driven EC stripe balancing. */
#define BAL_UTIL_GAP_PPM 50000ull

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
    if (!in->members && !in->nmembers)
        return 1;
    const struct place_member *pm = find_pm(in, id);
    return pm ? pm->chunk_stores : 0;
}

static int is_replace_old(const struct place_in *in, uint32_t id)
{
    if (!in->replaces)
        return 0;
    for (uint32_t i = 0; i < in->nreplaces; i++)
        if (in->replaces[i].old_node == id && in->replaces[i].new_node != 0)
            return 1;
    return 0;
}

static int member_placeable(const struct place_in *in, const struct meta_member *mb)
{
    if (!mb || mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
        return 0;
    if (mb->replaced_by != 0 || is_replace_old(in, mb->id))
        return 0;
    return member_heard_fresh(in, mb->id);
}

static uint64_t sat_add_u64(uint64_t a, uint64_t b)
{
    return UINT64_MAX - a < b ? UINT64_MAX : a + b;
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

static const struct place_disk_usage *find_disk_usage(const struct place_member *pm, const uint8_t store_id[16])
{
    if (!pm || !pm->disks || pm->ndisks == 0)
        return NULL;
    for (uint32_t i = 0; i < pm->ndisks; i++)
        if (memcmp(pm->disks[i].store_id, store_id, 16) == 0)
            return &pm->disks[i];
    return NULL;
}

static void member_chunk_stats(const struct place_in *in, const struct meta_member *mb, uint64_t *cap_out,
                               uint64_t *used_out, uint8_t *have_usage_out)
{
    uint64_t cap = 0, used = 0;
    uint32_t nup = 0, nusage = 0;
    const struct place_member *pm = find_pm(in, mb->id);
    for (uint32_t d = 0; d < mb->ndisks && d < META_DISKS_MAX; d++) {
        const struct meta_disk *dk = &mb->disks[d];
        if (dk->state != META_STORE_UP || !(dk->uses & META_DISK_USE_CHUNK))
            continue;
        nup++;
        const struct place_disk_usage *du = find_disk_usage(pm, dk->store_id);
        uint64_t sz = (du && du->size_bytes > 0) ? du->size_bytes : dk->size_bytes;
        cap = sat_add_u64(cap, sz);
        if (du && du->have_usage && sz > 0) {
            used = sat_add_u64(used, du->chunk_bytes);
            nusage++;
        }
    }
    *cap_out = cap;
    *used_out = used;
    *have_usage_out = (nup > 0 && nusage == nup && cap > 0) ? 1 : 0;
}

static uint64_t chunk_fill_mark_ppm(const struct place_in *in)
{
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers;
    int all_racked = 1;
    uint64_t total_cap = 0;
    uint32_t nstorage = 0;
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->state >= META_OUT || mb->role == META_ROLE_COMPUTE || mb->ndisks == 0)
            continue;
        uint64_t mcap = 0, mused = 0;
        uint8_t hu = 0;
        member_chunk_stats(in, mb, &mcap, &mused, &hu);
        if (mcap == 0)
            continue;
        nstorage++;
        total_cap = sat_add_u64(total_cap, mcap);
        if (!mb->rack[0])
            all_racked = 0;
    }
    if (total_cap == 0 || nstorage <= 1)
        return 900000ull;
    uint64_t max_dom = 0;
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->state >= META_OUT || mb->role == META_ROLE_COMPUTE || mb->ndisks == 0)
            continue;
        uint64_t dcap = 0;
        if (all_racked) {
            int seen = 0;
            for (uint32_t j = 0; j < i; j++) {
                if (sm->members[j].state < META_OUT && sm->members[j].role != META_ROLE_COMPUTE &&
                    sm->members[j].ndisks > 0 && !strcmp(sm->members[j].rack, mb->rack)) {
                    seen = 1;
                    break;
                }
            }
            if (seen)
                continue;
            for (uint32_t j = i; j < nm; j++) {
                const struct meta_member *mj = &sm->members[j];
                if (mj->state >= META_OUT || mj->role == META_ROLE_COMPUTE || mj->ndisks == 0 ||
                    strcmp(mj->rack, mb->rack) != 0)
                    continue;
                uint64_t mcap = 0, mused = 0;
                uint8_t hu = 0;
                member_chunk_stats(in, mj, &mcap, &mused, &hu);
                dcap = sat_add_u64(dcap, mcap);
            }
        } else {
            uint64_t mused = 0;
            uint8_t hu = 0;
            member_chunk_stats(in, mb, &dcap, &mused, &hu);
        }
        if (dcap > max_dom)
            max_dom = dcap;
    }
    if (max_dom >= total_cap)
        return 900000ull;
    return (uint64_t)(((__uint128_t)950000ull * (total_cap - max_dom)) / total_cap);
}

static int bal_member_live(const void *vctx, uint32_t node)
{
    const struct place_in *in = vctx;
    int32_t mi = member_idx(in->sm, node);
    if (mi < 0)
        return 0;
    return member_heard_fresh(in, node);
}

static uint32_t max_per_rack_mi(const struct meta_state *sm, const int32_t *mis, uint32_t n)
{
    uint32_t mx = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (mis[i] < 0)
            continue;
        uint32_t c = 0;
        for (uint32_t j = 0; j < n; j++)
            if (mis[j] >= 0 && !strcmp(sm->members[mis[i]].rack, sm->members[mis[j]].rack))
                c++;
        if (c > mx)
            mx = c;
    }
    return mx;
}

static int all_have_racks_mi(const struct meta_state *sm, const int32_t *mis, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++)
        if (mis[i] < 0 || !sm->members[mis[i]].rack[0])
            return 0;
    return n > 0;
}

static uint32_t log_move_sender(const struct place_in *in, const struct meta_volume *v)
{
    const struct meta_state *sm = in->sm;
    if (v->leader) {
        int32_t li = member_idx(sm, v->leader);
        if (li >= 0 && sm->members[li].state == META_UP && member_heard_fresh(in, v->leader) &&
            meta_volume_in_isr(v, v->leader))
            return v->leader;
    }
    for (uint32_t i = 0; i < v->nisr; i++) {
        int32_t mi = member_idx(sm, v->isr[i]);
        if (mi >= 0 && sm->members[mi].state == META_UP && member_heard_fresh(in, v->isr[i]))
            return v->isr[i];
    }
    return 0;
}

/* Strict Lyapunov check for weighted load (holds / cap): returns 1 if moving 1 unit from src to dst
 * strictly lowers max load and strictly reduces |load_src - load_dst| (guaranteed convergence, zero ping-pong). */
static int weighted_improves(uint64_t h_src, uint64_t c_src, uint64_t h_dst, uint64_t c_dst, uint64_t *score_out)
{
    if (h_src == 0 || c_src == 0 || c_dst == 0)
        return 0;
    __uint128_t bs = (__uint128_t)h_src * c_dst;
    __uint128_t bd = (__uint128_t)h_dst * c_src;
    if (bs <= bd)
        return 0;
    __uint128_t as = (__uint128_t)(h_src - 1) * c_dst;
    __uint128_t ad = (__uint128_t)(h_dst + 1) * c_src;
    if (ad > bs)
        return 0;
    __uint128_t diff_before = bs - bd;
    __uint128_t diff_after = as >= ad ? (as - ad) : (ad - as);
    if (diff_after >= diff_before)
        return 0;
    if (score_out) {
        /* Normalized reduction scaled to ppm: (diff_before - diff_after) / (c_src * c_dst). */
        __uint128_t red = diff_before - diff_after;
        /* Avoid 128-bit overflow on huge byte sizes by scaling caps down if needed. */
        uint64_t cs = c_src, cd = c_dst;
        while (cs > (1ull << 30) || cd > (1ull << 30)) {
            cs = (cs >> 10) ? (cs >> 10) : 1;
            cd = (cd >> 10) ? (cd >> 10) : 1;
        }
        __uint128_t bsn = (__uint128_t)h_src * cd;
        __uint128_t bdn = (__uint128_t)h_dst * cs;
        __uint128_t asn = (__uint128_t)(h_src - 1) * cd;
        __uint128_t adn = (__uint128_t)(h_dst + 1) * cs;
        __uint128_t db = bsn > bdn ? bsn - bdn : 0;
        __uint128_t da = asn >= adn ? asn - adn : adn - asn;
        __uint128_t r = db > da ? db - da : (red ? 1 : 0);
        __uint128_t denom = (__uint128_t)cs * cd;
        uint64_t sc = denom ? (uint64_t)((r * 1000000ull) / denom) : 1;
        *score_out = sc ? sc : 1;
    }
    return 1;
}

/* ------------------------------------------------------------------ core planner */

struct bal_move {
    enum place_prop_kind kind; /* PLACE_PROP_REPLICA_ADD or PLACE_PROP_STRIPE_SET */
    uint32_t vol_idx;
    uint32_t volume;
    uint32_t pos;       /* stripe pos (STRIPE_SET) or rep slot (REPLICA_ADD) */
    uint32_t src_mi;
    uint32_t dst_mi;
    uint32_t src_node;
    uint32_t dst_node;
    uint64_t epoch;
    uint64_t stripe_ver;
};

struct bal_xfer {
    uint32_t vol_idx;
    uint32_t volume;
    uint32_t src_mi;
    uint32_t dst_mi;
    uint32_t dst_node;
};

static int plan_push(struct bal_move **arr, uint32_t *n, uint32_t *cap, const struct bal_move *m)
{
    if (*n == *cap) {
        uint32_t nc = *cap ? *cap * 2 : 32;
        struct bal_move *na = realloc(*arr, (size_t)nc * sizeof *na);
        if (!na)
            return -1;
        *arr = na;
        *cap = nc;
    }
    (*arr)[(*n)++] = *m;
    return 0;
}

static int xfer_push(struct bal_xfer **arr, uint32_t *n, uint32_t *cap, const struct bal_xfer *x)
{
    if (*n == *cap) {
        uint32_t nc = *cap ? *cap * 2 : 16;
        struct bal_xfer *na = realloc(*arr, (size_t)nc * sizeof *na);
        if (!na)
            return -1;
        *arr = na;
        *cap = nc;
    }
    (*arr)[(*n)++] = *x;
    return 0;
}

static void balance_run(const struct place_in *in, struct place_out *out, struct balance_summary *sum)
{
    if (sum)
        memset(sum, 0, sizeof *sum);
    if (!in || !in->sm || !in->sm->initialised || in->sm->mode != META_MODE_REPLICATED)
        return;

    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers, nv = sm->nvolumes;
    if (nm < 2 || nv == 0)
        return;

    /* When emitting proposals (out != NULL), respect balance setting and trigger gate unless --apply. */
    if (out && !in->balance_apply) {
        if (!sm->set.balance)
            return;
        if (in->balance_gate)
            return;
    }

    /* 1. Never run while pout_plan or place_plan emitted any state-drop or rebuild proposal this tick. */
    if (out) {
        for (uint32_t i = 0; i < out->nprops; i++) {
            const struct place_prop *p = &out->props[i];
            if (p->kind == PLACE_PROP_MEMBER_STATE && p->state >= META_DOWN) {
                if (sum)
                    sum->rebuild_busy = 1;
                return;
            }
            if (p->kind == PLACE_PROP_REPLICA_ADD || p->kind == PLACE_PROP_REPLICA_REMOVE ||
                p->kind == PLACE_PROP_STRIPE_SET || p->kind == PLACE_PROP_STRIPE_WHOLE ||
                p->kind == PLACE_PROP_MOVE_CANCEL || p->kind == PLACE_PROP_GC_RELEASE) {
                if (sum)
                    sum->rebuild_busy = 1;
                return;
            }
        }
    }

    /* 2. Check member-level rebuild blockers (active replace, replica faults, or DRAINING/OUT/DOWN holding data). */
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->replaced_by != 0 || is_replace_old(in, mb->id)) {
            if (sum)
                sum->rebuild_busy = 1;
            return;
        }
        if (mb->state == META_UP && in->members) {
            const struct place_member *pm = find_pm(in, mb->id);
            if (pm && pm->nfaults > 0) {
                if (sum)
                    sum->rebuild_busy = 1;
                return;
            }
        }
    }

    /* 3. Allocate scratch state for virtual greedy pass. */
    uint8_t *elig_log = calloc(nm, sizeof *elig_log);
    uint8_t *fast_log = calloc(nm, sizeof *fast_log);
    uint8_t *elig_stripe = calloc(nm, sizeof *elig_stripe);
    uint8_t *chunk_cls = calloc(nm, sizeof *chunk_cls);
    uint64_t *chunk_cap = calloc(nm, sizeof *chunk_cap);
    uint64_t *chunk_used = calloc(nm, sizeof *chunk_used);
    uint64_t *real_used_init = calloc(nm, sizeof *real_used_init);
    uint64_t *real_used = calloc(nm, sizeof *real_used);
    uint8_t *have_usage = calloc(nm, sizeof *have_usage);
    uint32_t *log_holds = calloc(nm, sizeof *log_holds);
    uint32_t *rep0_holds = calloc(nm, sizeof *rep0_holds);
    uint32_t *leads = calloc(nm, sizeof *leads);
    uint32_t *stripe_holds = calloc(nm, sizeof *stripe_holds);
    uint32_t *real_stripe_holds = calloc(nm, sizeof *real_stripe_holds);
    uint32_t *log_recv = calloc(nm, sizeof *log_recv);
    uint32_t *log_send = calloc(nm, sizeof *log_send);
    uint32_t *stripe_recv = calloc(nm, sizeof *stripe_recv);
    uint32_t *log_pairs = calloc((size_t)nm * nm, sizeof *log_pairs);
    uint32_t *stripe_pairs = calloc((size_t)nm * nm, sizeof *stripe_pairs);
    int *vol_ym = calloc(nv, sizeof *vol_ym);
    int *vol_sm = calloc(nv, sizeof *vol_sm);
    uint32_t *vol_inflight = calloc(nv, sizeof *vol_inflight);
    uint8_t *vol_busy_log = calloc(nv, sizeof *vol_busy_log);
    uint8_t *vol_busy_stripe = calloc(nv, sizeof *vol_busy_stripe);
    uint32_t *sim_nrep = calloc(nv, sizeof *sim_nrep);
    int32_t (*sim_rep)[META_REPLICAS_MAX] = calloc(nv, sizeof *sim_rep);
    int32_t (*sim_stripe)[META_STRIPE_MAX] = calloc(nv, sizeof *sim_stripe);
    struct bal_move *plan = NULL;
    uint32_t nplan = 0, cap_plan = 0;
    struct bal_xfer *xfers = NULL;
    uint32_t nxfers = 0, cap_xfers = 0;

    if (!elig_log || !fast_log || !elig_stripe || !chunk_cls || !chunk_cap || !chunk_used || !real_used_init ||
        !real_used || !have_usage || !log_holds || !rep0_holds || !leads || !stripe_holds || !real_stripe_holds ||
        !log_recv || !log_send || !stripe_recv || !log_pairs || !stripe_pairs || !vol_ym || !vol_sm || !vol_inflight ||
        !vol_busy_log || !vol_busy_stripe || !sim_nrep || !sim_rep || !sim_stripe)
        goto cleanup;

    /* 4. Inspect every replicated volume: detect any queued/active rebuild, compute margins and total positions. */
    struct meta_liveness liveness = {.live = bal_member_live, .ctx = (void *)in};
    uint32_t total_pos = 0;
    uint32_t inflight_bal = 0;
    int rebuild_busy = 0;

    for (uint32_t vi = 0; vi < nv; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        if (v->conv.phase != META_CONV_NONE) {
            rebuild_busy = 1;
            break;
        }
        meta_vol_margins(sm, v, &liveness, &vol_ym[vi], &vol_sm[vi]);

        uint32_t moving_node = 0, moving_replaces = 0;
        uint32_t full_nrep = 0;
        for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++) {
            const struct meta_replica *r = &v->rep[k];
            int32_t mi = member_idx(sm, r->node);
            if (mi < 0 || sm->members[mi].state != META_UP || !member_heard_fresh(in, r->node)) {
                rebuild_busy = 1;
                break;
            }
            if (r->learner || r->replaces) {
                if (r->replaces == 0) {
                    rebuild_busy = 1; /* reset/catch-up rebuild */
                    break;
                }
                int32_t ri = member_idx(sm, r->replaces);
                if (ri < 0 || sm->members[ri].state != META_UP || !member_heard_fresh(in, r->replaces)) {
                    rebuild_busy = 1; /* replacing a DOWN/DRAINING/OUT/DESTROYED member */
                    break;
                }
                moving_node = r->node;
                moving_replaces = r->replaces;
            }
            if (!r->replaces)
                full_nrep++;
        }
        if (rebuild_busy)
            break;
        if (v->nisr < full_nrep) {
            rebuild_busy = 1; /* degraded log group */
            break;
        }

        /* Build effective post-move log replica set for balance accounting. */
        uint32_t sn = 0;
        for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++) {
            const struct meta_replica *r = &v->rep[k];
            if (moving_replaces && r->node == moving_replaces) {
                /* Moving-in node B takes A's slot (meta.c REMOVE puts B into A's slot). */
                int32_t bi = member_idx(sm, moving_node);
                if (bi >= 0 && sn < META_REPLICAS_MAX)
                    sim_rep[vi][sn++] = bi;
            } else if (moving_replaces && r->node == moving_node && r->replaces == moving_replaces) {
                continue; /* already placed into A's slot above */
            } else {
                int32_t mi = member_idx(sm, r->node);
                if (mi >= 0 && sn < META_REPLICAS_MAX)
                    sim_rep[vi][sn++] = mi;
            }
        }
        sim_nrep[vi] = sn;
        total_pos += sn;

        for (uint32_t k = 0; k < sn; k++) {
            int32_t mi = sim_rep[vi][k];
            log_holds[mi]++;
            if (k == 0)
                rep0_holds[mi]++;
        }
        for (uint32_t a = 0; a < sn; a++)
            for (uint32_t b = a + 1; b < sn; b++) {
                log_pairs[(size_t)sim_rep[vi][a] * nm + (size_t)sim_rep[vi][b]]++;
                log_pairs[(size_t)sim_rep[vi][b] * nm + (size_t)sim_rep[vi][a]]++;
            }

        if (v->leader) {
            int32_t li = member_idx(sm, v->leader);
            if (li >= 0)
                leads[li]++;
        }

        if (moving_node) {
            vol_busy_log[vi] = 1;
            vol_inflight[vi]++;
            inflight_bal++;
            int32_t bi = member_idx(sm, moving_node);
            if (bi >= 0)
                log_recv[bi]++;
            uint32_t snd = log_move_sender(in, v);
            int32_t si = snd ? member_idx(sm, snd) : -1;
            if (si >= 0)
                log_send[si]++;
        }

        if (v->redundancy == META_RED_EC) {
            uint32_t ns = v->nstripe <= META_STRIPE_MAX ? v->nstripe : META_STRIPE_MAX;
            total_pos += ns;
            for (uint32_t p = 0; p < ns; p++) {
                if (v->stripe_pos[p].retired_ver != 0) {
                    rebuild_busy = 1;
                    break;
                }
                int32_t hi = member_idx(sm, v->stripe[p]);
                if (hi < 0 || sm->members[hi].state != META_UP || !member_heard_fresh(in, v->stripe[p]) ||
                    member_chunk_stores(in, v->stripe[p]) == 0) {
                    rebuild_busy = 1;
                    break;
                }
                sim_stripe[vi][p] = hi;
                stripe_holds[hi]++;
                if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
                    uint32_t pr = v->stripe_pos[p].prev;
                    int32_t pi = pr ? member_idx(sm, pr) : -1;
                    if (pi < 0 || sm->members[pi].state != META_UP || !member_heard_fresh(in, pr) ||
                        member_chunk_stores(in, pr) == 0 || vol_sm[vi] <= 0) {
                        rebuild_busy = 1;
                        break;
                    }
                    vol_busy_stripe[vi] = 1;
                    vol_inflight[vi]++;
                    inflight_bal++;
                    stripe_recv[hi]++;
                }
            }
            if (rebuild_busy)
                break;
            uint32_t vcap = ns / 20;
            if (vcap < 1)
                vcap = 1;
            if (vol_inflight[vi] > vcap) {
                rebuild_busy = 1; /* >5% in flight on one group must be an active rebuild/return wave */
                break;
            }
            for (uint32_t a = 0; a < ns; a++)
                for (uint32_t b = a + 1; b < ns; b++) {
                    stripe_pairs[(size_t)sim_stripe[vi][a] * nm + (size_t)sim_stripe[vi][b]]++;
                    stripe_pairs[(size_t)sim_stripe[vi][b] * nm + (size_t)sim_stripe[vi][a]]++;
                }
        }
    }

    if (rebuild_busy || total_pos == 0) {
        if (sum) {
            sum->total_pos = total_pos;
            sum->rebuild_busy = (uint8_t)rebuild_busy;
        }
        goto cleanup;
    }

    uint32_t max_plan = total_pos / 4;
    uint32_t max_conc = total_pos / 20;
    if (max_conc < 1)
        max_conc = 1;

    /* 5. Classify eligible UP members for log and EC stripe balancing using r13-place helpers. */
    uint32_t n_log_elig = 0, n_fast_elig = 0, n_stripe_elig = 0;
    uint64_t mark_ppm = chunk_fill_mark_ppm(in);
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *mb = &sm->members[i];
        chunk_cls[i] = place_member_chunk_class(mb);
        member_chunk_stats(in, mb, &chunk_cap[i], &chunk_used[i], &have_usage[i]);
        real_used_init[i] = chunk_used[i];
        real_used[i] = chunk_used[i];
        real_stripe_holds[i] = stripe_holds[i];
        fast_log[i] = (uint8_t)((mb->ndisks == 0) || meta_member_has_fast_disk(mb));
        if (!member_placeable(in, mb))
            continue;
        if (place_member_log_ok(in, mb, 0, NULL)) {
            elig_log[i] = 1;
            n_log_elig++;
            n_fast_elig += fast_log[i];
        }
        if (place_member_stripe_ok(in, mb, 0, NULL)) {
            elig_stripe[i] = 1;
            n_stripe_elig++;
        }
    }

    /* 6. Stage A: Greedy pass over log replica lists (§3.2 rules 1-4, E9, R13). */
    /* When running on the automatic emission path (out != NULL, !balance_apply), stop as soon as we exceed
     * max_plan so large clusters never burn O(total_pos * nv * nm) on the cluster thread once over the 25% cap. */
    int stop_over_cap = (out != NULL && !in->balance_apply && in->mutant != PLACE_MUT_BALANCE_UNBOUNDED);
    uint32_t plan_log = 0;
    if (n_log_elig >= 2) {
        uint32_t max_iter = total_pos;
        for (uint32_t iter = 0; iter < max_iter; iter++) {
            if (stop_over_cap && (plan_log + inflight_bal) > max_plan)
                break;
            int found = 0;
            uint32_t best_vi = 0, best_slot = 0, best_src = 0, best_dst = 0;
            int best_hdd_evac = 0;
            int best_gap = 0;
            uint32_t best_src_h = 0, best_dst_h = UINT32_MAX;
            int best_rack_imp = 0;
            int best_rep0_score = -2;
            int64_t best_dpairs = INT64_MAX;

            for (uint32_t vi = 0; vi < nv; vi++) {
                const struct meta_volume *v = &sm->volumes[vi];
                if (v->kind != META_VOL_REPLICATED || vol_ym[vi] <= 0 || vol_busy_log[vi])
                    continue;
                uint32_t sn = sim_nrep[vi];
                if (sn == 0)
                    continue;
                uint64_t vbytes = vol_bytes_of(in, v->number);
                int racks_v = (in->mutant != PLACE_MUT_RACK_IGNORED) && all_have_racks_mi(sm, sim_rep[vi], sn);
                uint32_t rack_before = racks_v ? max_per_rack_mi(sm, sim_rep[vi], sn) : 0;
                /* Build a temporary meta_volume snapshot of current virtual replica set for survivability check. */
                struct meta_volume v_cur = *v;
                v_cur.nrep = sn;
                for (uint32_t k = 0; k < sn; k++) {
                    v_cur.rep[k].node = sm->members[sim_rep[vi][k]].id;
                    v_cur.rep[k].learner = 0;
                    v_cur.rep[k].replaces = 0;
                }
                int surv_before = (in->mutant != PLACE_MUT_RACK_IGNORED) ? place_rack_survivable(sm, &v_cur) : 0;

                for (uint32_t k = 0; k < sn; k++) {
                    uint32_t smi = (uint32_t)sim_rep[vi][k];
                    for (uint32_t dmi = 0; dmi < nm; dmi++) {
                        if (!elig_log[dmi] || dmi == smi)
                            continue;
                        /* Never move from a fast server onto an HDD-only server (E50). */
                        if (in->mutant != PLACE_MUT_LOG_HDD && fast_log[smi] && !fast_log[dmi])
                            continue;
                        int hdd_evac = (in->mutant != PLACE_MUT_LOG_HDD && !fast_log[smi] && fast_log[dmi] &&
                                        n_fast_elig >= sn);
                        int gap = (int)log_holds[smi] - (int)log_holds[dmi];
                        if (!hdd_evac && gap < 2)
                            continue;
                        /* Destination must not already hold a replica of v. */
                        int dup = 0;
                        for (uint32_t j = 0; j < sn; j++)
                            if (sim_rep[vi][j] == (int32_t)dmi) {
                                dup = 1;
                                break;
                            }
                        if (dup)
                            continue;
                        if (vbytes > 0 && !place_member_log_ok(in, &sm->members[dmi], vbytes, NULL))
                            continue;

                        /* E9 / §3.2 rule 1: never worsen rack spread or break rack survivability. */
                        int rack_imp = 0;
                        if (in->mutant != PLACE_MUT_RACK_IGNORED) {
                            int32_t after[META_REPLICAS_MAX];
                            memcpy(after, sim_rep[vi], sn * sizeof(int32_t));
                            after[k] = (int32_t)dmi;
                            if (racks_v && sm->members[dmi].rack[0]) {
                                uint32_t rack_after = max_per_rack_mi(sm, after, sn);
                                if (rack_after > rack_before)
                                    continue;
                                if (rack_after < rack_before)
                                    rack_imp = 1;
                            }
                            if (surv_before) {
                                struct meta_volume v_after = v_cur;
                                v_after.rep[k].node = sm->members[dmi].id;
                                if (!place_rack_survivable(sm, &v_after))
                                    continue;
                            }
                        }

                        /* §3.2 rule 2: preferred-leader (rep[0]) balance preference. */
                        int rep0_score = 0;
                        if (k == 0) {
                            if (rep0_holds[smi] >= rep0_holds[dmi] + 2)
                                rep0_score = 1;
                            else if (rep0_holds[smi] <= rep0_holds[dmi])
                                rep0_score = -1;
                        }

                        /* §3.2 rule 4: pair spread delta (lower is better). */
                        int64_t dpairs = 0;
                        for (uint32_t j = 0; j < sn; j++) {
                            if (j == k)
                                continue;
                            uint32_t omi = (uint32_t)sim_rep[vi][j];
                            dpairs += (int64_t)log_pairs[(size_t)dmi * nm + omi] -
                                      (int64_t)log_pairs[(size_t)smi * nm + omi];
                        }

                        int better = 0;
                        if (!found) {
                            better = 1;
                        } else if (hdd_evac != best_hdd_evac) {
                            better = hdd_evac > best_hdd_evac;
                        } else if (gap != best_gap) {
                            better = gap > best_gap;
                        } else if (log_holds[smi] != best_src_h) {
                            better = log_holds[smi] > best_src_h;
                        } else if (log_holds[dmi] != best_dst_h) {
                            better = log_holds[dmi] < best_dst_h;
                        } else if (rack_imp != best_rack_imp) {
                            better = rack_imp > best_rack_imp;
                        } else if (rep0_score != best_rep0_score) {
                            better = rep0_score > best_rep0_score;
                        } else if (dpairs != best_dpairs) {
                            better = dpairs < best_dpairs;
                        } else if (v->number != sm->volumes[best_vi].number) {
                            better = v->number < sm->volumes[best_vi].number;
                        } else if (sm->members[dmi].id != sm->members[best_dst].id) {
                            better = sm->members[dmi].id < sm->members[best_dst].id;
                        } else if (sm->members[smi].id != sm->members[best_src].id) {
                            better = sm->members[smi].id > sm->members[best_src].id;
                        }
                        if (better) {
                            found = 1;
                            best_vi = vi;
                            best_slot = k;
                            best_src = smi;
                            best_dst = dmi;
                            best_hdd_evac = hdd_evac;
                            best_gap = gap;
                            best_src_h = log_holds[smi];
                            best_dst_h = log_holds[dmi];
                            best_rack_imp = rack_imp;
                            best_rep0_score = rep0_score;
                            best_dpairs = dpairs;
                        }
                    }
                }
            }

            if (!found)
                break;

            /* Record planned move and update virtual log state. */
            const struct meta_volume *bv = &sm->volumes[best_vi];
            struct bal_move mv = {
                .kind = PLACE_PROP_REPLICA_ADD,
                .vol_idx = best_vi,
                .volume = bv->number,
                .pos = best_slot,
                .src_mi = best_src,
                .dst_mi = best_dst,
                .src_node = sm->members[best_src].id,
                .dst_node = sm->members[best_dst].id,
                .epoch = bv->epoch,
            };
            if (plan_push(&plan, &nplan, &cap_plan, &mv) != 0)
                goto cleanup;
            plan_log++;

            uint32_t sn = sim_nrep[best_vi];
            for (uint32_t j = 0; j < sn; j++) {
                if (j == best_slot)
                    continue;
                uint32_t omi = (uint32_t)sim_rep[best_vi][j];
                if (log_pairs[(size_t)best_src * nm + omi] > 0)
                    log_pairs[(size_t)best_src * nm + omi]--;
                if (log_pairs[(size_t)omi * nm + best_src] > 0)
                    log_pairs[(size_t)omi * nm + best_src]--;
                log_pairs[(size_t)best_dst * nm + omi]++;
                log_pairs[(size_t)omi * nm + best_dst]++;
            }
            sim_rep[best_vi][best_slot] = (int32_t)best_dst;
            log_holds[best_src]--;
            log_holds[best_dst]++;
            if (best_slot == 0) {
                rep0_holds[best_src]--;
                rep0_holds[best_dst]++;
            }
        }
    }

    /* 7. Stage B: Greedy pass over EC stripe positions (§6.2, E9, E51, R13). */
    uint32_t plan_stripe = 0;
    if (n_stripe_elig >= 2) {
        /* Determine per-class effective capacity (use real chunk_cap when all eligible members in the class report it;
         * else fall back to weight 1 for uniform count balancing). */
        int cls_all_cap[META_DISK_CLASS_LAST + 1];
        for (int c = 0; c <= META_DISK_CLASS_LAST; c++)
            cls_all_cap[c] = 1;
        for (uint32_t i = 0; i < nm; i++) {
            if (!elig_stripe[i])
                continue;
            uint8_t c = chunk_cls[i];
            if (c <= META_DISK_CLASS_LAST && chunk_cap[i] == 0)
                cls_all_cap[c] = 0;
        }

        uint32_t max_iter = total_pos;
        for (uint32_t iter = 0; iter < max_iter; iter++) {
            if (stop_over_cap && (plan_log + plan_stripe + inflight_bal) > max_plan)
                break;
            int found = 0;
            uint32_t best_vi = 0, best_pos = 0, best_src = 0, best_dst = 0;
            int best_by_bytes = 0;
            uint64_t best_score = 0;
            int best_rack_imp = 0;
            int64_t best_dpairs = INT64_MAX;
            uint64_t best_est_bytes = 0;

            for (uint32_t vi = 0; vi < nv; vi++) {
                const struct meta_volume *v = &sm->volumes[vi];
                if (v->kind != META_VOL_REPLICATED || v->redundancy != META_RED_EC || vol_sm[vi] <= 0)
                    continue;
                uint32_t ns = v->nstripe <= META_STRIPE_MAX ? v->nstripe : META_STRIPE_MAX;
                if (ns == 0)
                    continue;
                int racks_v = (in->mutant != PLACE_MUT_RACK_IGNORED) && all_have_racks_mi(sm, sim_stripe[vi], ns);
                uint32_t rack_before = racks_v ? max_per_rack_mi(sm, sim_stripe[vi], ns) : 0;
                struct meta_volume v_cur = *v;
                for (uint32_t k = 0; k < ns; k++)
                    v_cur.stripe[k] = sm->members[sim_stripe[vi][k]].id;
                int surv_before = (in->mutant != PLACE_MUT_RACK_IGNORED) ? place_rack_survivable(sm, &v_cur) : 0;

                for (uint32_t p = 0; p < ns; p++) {
                    if (v->stripe_pos[p].state != META_STRIPE_WHOLE)
                        continue;
                    uint32_t smi = (uint32_t)sim_stripe[vi][p];
                    if (stripe_holds[smi] == 0)
                        continue;
                    uint8_t scls = chunk_cls[smi];
                    uint64_t est_b = (have_usage[smi] && stripe_holds[smi] > 0) ? (chunk_used[smi] / stripe_holds[smi]) : 0;

                    for (uint32_t dmi = 0; dmi < nm; dmi++) {
                        if (!elig_stripe[dmi] || dmi == smi)
                            continue;
                        /* E51 / §15.3: balance within the same storage disk class (never worsen class mix). */
                        if (in->mutant != PLACE_MUT_MIX_STRIPE && chunk_cls[dmi] != scls)
                            continue;
                        /* Destination must not already hold a stripe position of v. */
                        int dup = 0;
                        for (uint32_t j = 0; j < ns; j++)
                            if (sim_stripe[vi][j] == (int32_t)dmi) {
                                dup = 1;
                                break;
                            }
                        if (dup)
                            continue;

                        int by_bytes = 0;
                        uint64_t score = 0;
                        int both_usage = have_usage[smi] && have_usage[dmi] && chunk_cap[smi] > 0 && chunk_cap[dmi] > 0;
                        if (both_usage && est_b > 0) {
                            uint64_t us = (uint64_t)(((__uint128_t)chunk_used[smi] * 1000000ull) / chunk_cap[smi]);
                            uint64_t ud = (uint64_t)(((__uint128_t)chunk_used[dmi] * 1000000ull) / chunk_cap[dmi]);
                            uint64_t nu_d = (uint64_t)(((__uint128_t)sat_add_u64(chunk_used[dmi], est_b) * 1000000ull) /
                                                       chunk_cap[dmi]);
                            /* Never push destination past the chunk fill mark (E53 / §5.6). */
                            if (in->mutant != PLACE_MUT_PLACE_FULL && nu_d >= mark_ppm)
                                continue;
                            if (us > ud + BAL_UTIL_GAP_PPM) {
                                uint64_t nu_s = (uint64_t)(((__uint128_t)(chunk_used[smi] - est_b) * 1000000ull) /
                                                           chunk_cap[smi]);
                                uint64_t diff_b = us - ud;
                                uint64_t diff_a = nu_s >= nu_d ? (nu_s - nu_d) : (nu_d - nu_s);
                                if (nu_d < us && diff_a < diff_b) {
                                    by_bytes = 1;
                                    score = diff_b - diff_a;
                                }
                            }
                        }

                        if (!by_bytes) {
                            uint64_t cs = (scls <= META_DISK_CLASS_LAST && cls_all_cap[scls]) ? chunk_cap[smi] : 1ull;
                            uint64_t cd = (scls <= META_DISK_CLASS_LAST && cls_all_cap[scls]) ? chunk_cap[dmi] : 1ull;
                            if (!weighted_improves(stripe_holds[smi], cs, stripe_holds[dmi], cd, &score))
                                continue;
                            /* Do not fight byte utilization by pushing dst >= 5% fuller than src. */
                            if (both_usage && est_b > 0) {
                                uint64_t nu_s = (uint64_t)(((__uint128_t)(chunk_used[smi] - est_b) * 1000000ull) /
                                                           chunk_cap[smi]);
                                uint64_t nu_d = (uint64_t)(((__uint128_t)sat_add_u64(chunk_used[dmi], est_b) *
                                                            1000000ull) /
                                                           chunk_cap[dmi]);
                                if (nu_d > nu_s + BAL_UTIL_GAP_PPM)
                                    continue;
                            }
                        }

                        /* E9: never worsen rack spread or break rack survivability. */
                        int rack_imp = 0;
                        if (in->mutant != PLACE_MUT_RACK_IGNORED) {
                            int32_t after[META_STRIPE_MAX];
                            memcpy(after, sim_stripe[vi], ns * sizeof(int32_t));
                            after[p] = (int32_t)dmi;
                            if (racks_v && sm->members[dmi].rack[0]) {
                                uint32_t rack_after = max_per_rack_mi(sm, after, ns);
                                if (rack_after > rack_before)
                                    continue;
                                if (rack_after < rack_before)
                                    rack_imp = 1;
                            }
                            if (surv_before) {
                                struct meta_volume v_after = v_cur;
                                v_after.stripe[p] = sm->members[dmi].id;
                                if (!place_rack_survivable(sm, &v_after))
                                    continue;
                            }
                        }

                        int64_t dpairs = 0;
                        for (uint32_t j = 0; j < ns; j++) {
                            if (j == p)
                                continue;
                            uint32_t omi = (uint32_t)sim_stripe[vi][j];
                            dpairs += (int64_t)stripe_pairs[(size_t)dmi * nm + omi] -
                                      (int64_t)stripe_pairs[(size_t)smi * nm + omi];
                        }

                        int better = 0;
                        if (!found) {
                            better = 1;
                        } else if (by_bytes != best_by_bytes) {
                            better = by_bytes > best_by_bytes;
                        } else if (score != best_score) {
                            better = score > best_score;
                        } else if (rack_imp != best_rack_imp) {
                            better = rack_imp > best_rack_imp;
                        } else if (dpairs != best_dpairs) {
                            better = dpairs < best_dpairs;
                        } else if (v->number != sm->volumes[best_vi].number) {
                            better = v->number < sm->volumes[best_vi].number;
                        } else if (p != best_pos) {
                            better = p < best_pos;
                        } else if (sm->members[dmi].id != sm->members[best_dst].id) {
                            better = sm->members[dmi].id < sm->members[best_dst].id;
                        } else if (sm->members[smi].id != sm->members[best_src].id) {
                            better = sm->members[smi].id > sm->members[best_src].id;
                        }
                        if (better) {
                            found = 1;
                            best_vi = vi;
                            best_pos = p;
                            best_src = smi;
                            best_dst = dmi;
                            best_by_bytes = by_bytes;
                            best_score = score;
                            best_rack_imp = rack_imp;
                            best_dpairs = dpairs;
                            best_est_bytes = est_b;
                        }
                    }
                }
            }

            if (!found)
                break;

            const struct meta_volume *bv = &sm->volumes[best_vi];
            struct bal_move mv = {
                .kind = PLACE_PROP_STRIPE_SET,
                .vol_idx = best_vi,
                .volume = bv->number,
                .pos = best_pos,
                .src_mi = best_src,
                .dst_mi = best_dst,
                .src_node = sm->members[best_src].id,
                .dst_node = sm->members[best_dst].id,
                .stripe_ver = bv->stripe_version,
            };
            if (plan_push(&plan, &nplan, &cap_plan, &mv) != 0)
                goto cleanup;
            plan_stripe++;

            uint32_t ns = bv->nstripe <= META_STRIPE_MAX ? bv->nstripe : META_STRIPE_MAX;
            for (uint32_t j = 0; j < ns; j++) {
                if (j == best_pos)
                    continue;
                uint32_t omi = (uint32_t)sim_stripe[best_vi][j];
                if (stripe_pairs[(size_t)best_src * nm + omi] > 0)
                    stripe_pairs[(size_t)best_src * nm + omi]--;
                if (stripe_pairs[(size_t)omi * nm + best_src] > 0)
                    stripe_pairs[(size_t)omi * nm + best_src]--;
                stripe_pairs[(size_t)best_dst * nm + omi]++;
                stripe_pairs[(size_t)omi * nm + best_dst]++;
            }
            sim_stripe[best_vi][best_pos] = (int32_t)best_dst;
            stripe_holds[best_src]--;
            stripe_holds[best_dst]++;
            if (best_est_bytes > 0) {
                chunk_used[best_src] = chunk_used[best_src] > best_est_bytes ? chunk_used[best_src] - best_est_bytes : 0;
                chunk_used[best_dst] = sat_add_u64(chunk_used[best_dst], best_est_bytes);
            }
        }
    }

    /* 8. Stage C: Greedy leader balance (§3.2 rule 2, via R4 planned transfer) when leader_balance_s == 0.
     * When leader_balance_s > 0, R4's ctransfer.c restores rep[0] after leader_balance_s; transferring away from
     * rep[0] here would ping-pong with ctransfer.c, and transferring to rep[0] would preempt leader_balance_s. */
    if (sm->set.leader_balance_s == 0) {
        uint8_t *vol_xferred = calloc(nv, sizeof *vol_xferred);
        if (!vol_xferred)
            goto cleanup;
        for (uint32_t iter = 0; iter < nv; iter++) {
            int found = 0;
            uint32_t best_vi = 0, best_src = 0, best_dst = 0;
            int best_gap = 0, best_pref = 0;
            uint32_t best_src_l = 0, best_dst_l = UINT32_MAX;

            for (uint32_t vi = 0; vi < nv; vi++) {
                const struct meta_volume *v = &sm->volumes[vi];
                if (v->kind != META_VOL_REPLICATED || !v->leader || vol_xferred[vi] || vol_busy_log[vi])
                    continue;
                int32_t lmi = member_idx(sm, v->leader);
                if (lmi < 0)
                    continue;
                for (uint32_t j = 0; j < v->nisr; j++) {
                    uint32_t u = v->isr[j];
                    if (u == v->leader)
                        continue;
                    int32_t umi = member_idx(sm, u);
                    if (umi < 0 || !member_placeable(in, &sm->members[umi]))
                        continue;
                    int gap = (int)leads[lmi] - (int)leads[umi];
                    if (gap < 2)
                        continue;
                    int pref = (v->nrep > 0 && v->rep[0].node == u) ? 1 : 0;
                    int better = 0;
                    if (!found) {
                        better = 1;
                    } else if (gap != best_gap) {
                        better = gap > best_gap;
                    } else if (leads[lmi] != best_src_l) {
                        better = leads[lmi] > best_src_l;
                    } else if (leads[umi] != best_dst_l) {
                        better = leads[umi] < best_dst_l;
                    } else if (pref != best_pref) {
                        better = pref > best_pref;
                    } else if (v->number != sm->volumes[best_vi].number) {
                        better = v->number < sm->volumes[best_vi].number;
                    } else {
                        better = u < sm->members[best_dst].id;
                    }
                    if (better) {
                        found = 1;
                        best_vi = vi;
                        best_src = (uint32_t)lmi;
                        best_dst = (uint32_t)umi;
                        best_gap = gap;
                        best_src_l = leads[lmi];
                        best_dst_l = leads[umi];
                        best_pref = pref;
                    }
                }
            }
            if (!found)
                break;
            struct bal_xfer xf = {
                .vol_idx = best_vi,
                .volume = sm->volumes[best_vi].number,
                .src_mi = best_src,
                .dst_mi = best_dst,
                .dst_node = sm->members[best_dst].id,
            };
            if (xfer_push(&xfers, &nxfers, &cap_xfers, &xf) != 0) {
                free(vol_xferred);
                goto cleanup;
            }
            vol_xferred[best_vi] = 1;
            leads[best_src]--;
            leads[best_dst]++;
        }
        free(vol_xferred);
    }

    /* 9. Check 25% automatic plan cap and fill summary. */
    uint32_t total_data_moves = plan_log + plan_stripe;
    int over_cap = (total_data_moves + inflight_bal) > max_plan;
    if (sum) {
        sum->total_pos = total_pos;
        sum->max_plan = max_plan;
        sum->max_conc = max_conc;
        sum->inflight_bal = inflight_bal;
        sum->log_moves = plan_log;
        sum->stripe_moves = plan_stripe;
        sum->leader_moves = nxfers;
        sum->rebuild_busy = 0;
        sum->over_cap = (uint8_t)(over_cap != 0);
    }
    if (!out)
        goto cleanup;

    if (total_data_moves > 0 && over_cap && !in->balance_apply && in->mutant != PLACE_MUT_BALANCE_UNBOUNDED) {
        double pct = total_pos ? (100.0 * (double)(total_data_moves + inflight_bal) / (double)total_pos) : 0.0;
        (void)place_out_log(out,
                            "balance: plan moves %" PRIu32 " of %" PRIu32
                            " positions (%.1f%% > 25%% automatic cap); stopped — run 'tsdb cluster balance --apply' to proceed",
                            total_data_moves + inflight_bal, total_pos, pct);
        goto cleanup;
    }

    /* 10. Emit proposals bounded by 5% per-group concurrent cap (both per-volume and cluster-wide) and server caps. */
    uint32_t rebuild_max = sm->set.rebuild_max ? sm->set.rebuild_max : 2;
    uint32_t stripe_cap = in->set.rebuild_concurrency
                              ? in->set.rebuild_concurrency
                              : (sm->set.rebuild_concurrency ? sm->set.rebuild_concurrency : 4);
    uint32_t emitted_data = 0;
    uint8_t *vol_emitted_log = calloc(nv, sizeof *vol_emitted_log);
    uint8_t *vol_emitted_stripe = calloc(nv, sizeof *vol_emitted_stripe);
    if (!vol_emitted_log || !vol_emitted_stripe) {
        free(vol_emitted_log);
        free(vol_emitted_stripe);
        goto cleanup;
    }

    for (uint32_t i = 0; i < nplan; i++) {
        if (emitted_data + inflight_bal >= max_conc)
            break;
        const struct bal_move *m = &plan[i];
        const struct meta_volume *v = &sm->volumes[m->vol_idx];
        uint32_t grp = (v->redundancy == META_RED_EC) ? v->nstripe : sim_nrep[m->vol_idx];
        uint32_t vol_cap = grp / 20;
        if (vol_cap < 1)
            vol_cap = 1;
        if (vol_inflight[m->vol_idx] >= vol_cap)
            continue;

        if (m->kind == PLACE_PROP_REPLICA_ADD) {
            if (vol_emitted_log[m->vol_idx] || vol_busy_log[m->vol_idx])
                continue;
            /* Verify source still holds the replica in the real (non-virtual) metadata state. */
            if (!meta_replica(v, m->src_node) || meta_replica(v, m->dst_node))
                continue;
            if (in->mutant != PLACE_MUT_RACK_IGNORED && place_rack_survivable(sm, v)) {
                struct meta_volume va = *v;
                for (uint32_t k = 0; k < va.nrep && k < META_REPLICAS_MAX; k++)
                    if (va.rep[k].node == m->src_node)
                        va.rep[k].node = m->dst_node;
                if (!place_rack_survivable(sm, &va))
                    continue;
            }
            uint32_t sender = log_move_sender(in, v);
            int32_t si = sender ? member_idx(sm, sender) : -1;
            if (si < 0 || log_send[si] >= rebuild_max || log_recv[m->dst_mi] >= rebuild_max)
                continue;
            struct place_prop prop = {
                .kind = PLACE_PROP_REPLICA_ADD,
                .volume = m->volume,
                .node = m->dst_node,
                .other_node = m->src_node,
                .epoch = m->epoch,
                .is_balance = 1,
                .margin = PACE_MARGIN_BALANCE,
            };
            if (place_out_push(out, &prop) != 0)
                break;
            vol_emitted_log[m->vol_idx] = 1;
            vol_inflight[m->vol_idx]++;
            log_send[si]++;
            log_recv[m->dst_mi]++;
            emitted_data++;
        } else {
            if (vol_emitted_stripe[m->vol_idx])
                continue;
            if (m->pos >= v->nstripe || v->stripe[m->pos] != m->src_node ||
                v->stripe_pos[m->pos].state != META_STRIPE_WHOLE)
                continue;
            int dup = 0;
            for (uint32_t k = 0; k < v->nstripe && k < META_STRIPE_MAX; k++)
                if (v->stripe[k] == m->dst_node)
                    dup = 1;
            if (dup || stripe_recv[m->dst_mi] >= stripe_cap)
                continue;
            if (in->mutant != PLACE_MUT_RACK_IGNORED && place_rack_survivable(sm, v)) {
                struct meta_volume va = *v;
                va.stripe[m->pos] = m->dst_node;
                if (!place_rack_survivable(sm, &va))
                    continue;
            }
            /* Re-check chunk fill mark against REAL (non-virtual) current bytes plus same-tick emitted incoming
             * moves: a concurrent outgoing move off dst_mi has not freed its chunks until STRIPE_WHOLE + MK_GC
             * completes (E53 / §5.6). Earlier-tick FILLING positions are bounded by max_conc/stripe_cap and
             * progressively reflected in reported chunk_bytes without double-counting already-copied bytes. */
            uint64_t real_est_b = (have_usage[m->src_mi] && real_stripe_holds[m->src_mi] > 0)
                                      ? (real_used_init[m->src_mi] / real_stripe_holds[m->src_mi])
                                      : 0;
            if (in->mutant != PLACE_MUT_PLACE_FULL && have_usage[m->dst_mi] && chunk_cap[m->dst_mi] > 0 &&
                real_est_b > 0) {
                uint64_t nu_d = (uint64_t)(((__uint128_t)sat_add_u64(real_used[m->dst_mi], real_est_b) * 1000000ull) /
                                           chunk_cap[m->dst_mi]);
                if (nu_d >= mark_ppm)
                    continue;
            }
            struct place_prop prop = {
                .kind = PLACE_PROP_STRIPE_SET,
                .volume = m->volume,
                .node = m->dst_node,
                .other_node = m->src_node,
                .pos = m->pos,
                .version = v->stripe_version,
                .reason = META_STRIPE_FILLING,
                .is_balance = 1,
                .margin = PACE_MARGIN_BALANCE,
            };
            if (place_out_push(out, &prop) != 0)
                break;
            vol_emitted_stripe[m->vol_idx] = 1;
            vol_inflight[m->vol_idx]++;
            stripe_recv[m->dst_mi]++;
            if (real_est_b > 0)
                real_used[m->dst_mi] = sat_add_u64(real_used[m->dst_mi], real_est_b);
            emitted_data++;
        }
    }

    /* Emit at most one planned leader transfer per tick (gentle cadence; never on a volume with a log move). */
    for (uint32_t i = 0; i < nxfers; i++) {
        const struct bal_xfer *x = &xfers[i];
        if (vol_emitted_log[x->vol_idx] || vol_busy_log[x->vol_idx])
            continue;
        struct place_prop prop = {
            .kind = PLACE_PROP_VOLUME_TRANSFER,
            .volume = x->volume,
            .node = x->dst_node,
            .is_balance = 1,
            .margin = PACE_MARGIN_BALANCE,
        };
        (void)place_out_push(out, &prop);
        break;
    }

    free(vol_emitted_stripe);
    free(vol_emitted_log);

cleanup:
    free(xfers);
    free(plan);
    free(sim_stripe);
    free(sim_rep);
    free(sim_nrep);
    free(vol_busy_stripe);
    free(vol_busy_log);
    free(vol_inflight);
    free(vol_sm);
    free(vol_ym);
    free(stripe_pairs);
    free(log_pairs);
    free(stripe_recv);
    free(log_send);
    free(log_recv);
    free(real_stripe_holds);
    free(stripe_holds);
    free(leads);
    free(rep0_holds);
    free(log_holds);
    free(have_usage);
    free(real_used);
    free(real_used_init);
    free(chunk_used);
    free(chunk_cap);
    free(chunk_cls);
    free(elig_stripe);
    free(fast_log);
    free(elig_log);
}

void balance_plan(const struct place_in *in, struct place_out *out)
{
    if (!in || !out)
        return;
    balance_run(in, out, NULL);
}

void balance_plan_ex(const struct place_in *in, struct place_out *out, struct balance_summary *sum)
{
    if (!in)
        return;
    balance_run(in, out, sum);
}

void balance_plan_summary(const struct place_in *in, struct balance_summary *sum)
{
    if (!sum)
        return;
    balance_run(in, NULL, sum);
}
