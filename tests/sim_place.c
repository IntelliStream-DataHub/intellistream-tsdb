/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Placement, failure-handling, and MTTDL simulator (docs/design-cluster-data.md §5.3, §10.2; R9 plan D7;
 * track r9-simplace).
 *
 * Drives the real pure planners (`pout_plan`, `place_plan`, `balance_plan`, `place_volume`, `place_stripe`,
 * `place_rack_survivable`) and applies their proposals through the real metadata state machine (`meta_apply`,
 * checked by `meta_check_step`) over seeded discrete-event clusters with:
 *   - N servers across racks, 1-4 chunk disks per server (one storage class), mixed geometries:
 *     `replicas=3` and EC `4+2`, `8+2`, `8+4`, `16+4`;
 *   - Settled objects over time (`MK_COLD` pending -> committed, carrying `stripe_version`, including E10 degraded
 *     settles with k+m-1 positions) and `MK_GC` releases;
 *   - Faults: server down/back, slow opening past `opening_hold_s`, single-disk and full-store loss, fail-stop
 *     last-gasp beats, per-replica `LOCAL_FAULT`, rack-down and >25% mass outages, `m` and `m+1` simultaneous losses;
 *   - Operator operations: `node hold` (including refusal of >24 h holds), `node replace`, `node drain`,
 *     `node destroyed`, `volume move --pos`, and `node add`;
 *   - Modelled proposal delay and loss, and margin-paced log catch-up and EC stripe fills;
 *   - Checker after every step verifying all placement, barrier, durability, GC, hold, and guard invariants;
 *   - Planted mutants (`--mutant NAME` / `TSDB_PLACE_MUTANT`) verifying the checker catches every bug;
 *   - `--mttdl` Monte Carlo MTTDL over topologies built by real `place_volume`/`place_stripe` with E13 margin-first
 *     ordering and pacing beside §5.3's closed form k*D / ((N-1)*r).
 *
 * Usage:
 *   sim_place [--seeds N] [--first S] [--seed S] [--ticks T] [--mutant NAME] [--trace]
 *   sim_place --mttdl GEOM|all [--mttdl-table] [--disks-afr P] [--servers-afr P] [--disks-per-node D]
 *             [--tb-per-node T] [--rate MB/s] [--nodes N] [--trials N] [--seed S]
 */
#include <inttypes.h>
#include <math.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "meta.h"
#include "place.h"

/* ------------------------------------------------------------------ constants */

#define SIM_MAX_NODES 32
#define SIM_MAX_DISKS 4
#define SIM_MAX_VOLS 8
#define SIM_MAX_OBJS 36
#define SIM_MAX_RELEASES 48
#define SIM_MAX_INFLIGHT 128

/* Simulator-side mutant ids not in enum place_mutant:
 *   - SIM_MUT_RELEASE_NOMARK: hook for r9-whole (`releasenomark`)
 *   - SIM_MUT_BELOW_K:        proves Rule 1 ("below k readable positions") fires by injecting m+1 faults without
 *                             marking allow_below_k */
#define SIM_MUT_RELEASE_NOMARK 101
#define SIM_MUT_BELOW_K 102

/* ------------------------------------------------------------------ PRNG */

struct rng {
    uint64_t s;
};

static uint64_t splitmix64(uint64_t x)
{
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

static void rng_init(struct rng *r, uint64_t seed)
{
    r->s = splitmix64(seed ? seed : 1ull);
    if (!r->s)
        r->s = 0xdeadbeefcafebabeull;
}

static uint64_t rng_u64(struct rng *r)
{
    uint64_t x = r->s;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    r->s = x;
    return x * 0x2545f4914f6cdd1dull;
}

static uint32_t rng_u32(struct rng *r, uint32_t bound)
{
    return bound ? (uint32_t)(rng_u64(r) % bound) : 0;
}

static int rng_pct(struct rng *r, uint32_t pct)
{
    return rng_u32(r, 100) < pct;
}

static double rng_unit(struct rng *r)
{
    /* Strictly in (0, 1) for safe log() calls. */
    uint64_t v = rng_u64(r) >> 11;
    return ((double)v + 0.5) * (1.0 / 9007199254740992.0);
}

/* ------------------------------------------------------------------ model types */

enum obj_state {
    OBJ_EMPTY = 0,
    OBJ_PENDING = 1, /* MK_COLD written to stores, awaiting commit */
    OBJ_LIVE = 2,    /* MK_COLD committed */
    OBJ_RETIRED = 3, /* MK_GC committed and retired */
};

struct sim_obj {
    uint64_t oid;
    uint64_t stripe_version;
    uint64_t epoch;
    uint8_t state;
    uint8_t mk_gc_committed;                            /* 1 once an orphan/retirement MK_GC committed for this oid */
    uint32_t commit_tick;
    uint32_t positions_mask;
    uint32_t size_mb;
    uint32_t chunk_nodes[META_STRIPE_MAX];              /* bit (node_id-1): node holds valid chunk for pos */
    uint8_t chunk_disk[META_STRIPE_MAX][SIM_MAX_NODES]; /* disk index on node holding chunk for pos */
    uint8_t deg_unwritten[META_STRIPE_MAX];             /* 1: position omitted at E10 degraded settle, not yet filled */
    uint8_t disk_lost[META_STRIPE_MAX][SIM_MAX_NODES];  /* 1: node lost the disk that held its chunk for (o, pos) */
};

struct sim_release {
    uint32_t pos;
    uint32_t old_node;
    uint64_t release_ver;
    uint8_t mk_gc_committed; /* 1 once the release MK_GC marker has committed on the volume leader */
    uint8_t done;
};

struct sim_node {
    uint32_t id;
    char rack[META_RACK_MAX];
    int ndisks;
    uint8_t disk_ok[SIM_MAX_DISKS];
    uint64_t disk_store_id[SIM_MAX_DISKS];
    int alive;
    int destroyed;
    uint64_t incarnation;
    int opening;
    uint32_t opening_end_tick;
    int failstop_beat;
    uint64_t last_heard_ms;
    uint32_t fault_vol;
    uint32_t fault_ticks;
    uint32_t recover_tick; /* 0: no scheduled auto-recovery */
};

struct sim_vol {
    uint32_t number;
    uint8_t redundancy, ec_k, ec_m;
    uint64_t bytes_mb;
    uint64_t log_end;
    uint32_t log_move_rem_mb;
    uint32_t deg_wait_ticks;
    uint32_t fill_rem_mb[META_STRIPE_MAX];
    uint32_t reconcile_rem_mb[META_STRIPE_MAX];
    uint32_t nobjs;
    struct sim_obj objs[SIM_MAX_OBJS];
    uint32_t nrel;
    struct sim_release rels[SIM_MAX_RELEASES];
    int allow_below_k; /* 1 during intentional m+1 outage scenarios */
};

struct sim_inflight {
    struct place_prop prop;
    uint32_t deliver_tick;
    uint64_t proposed_ms;
    uint8_t from_balance;
};

struct sim_cov {
    uint64_t objs_settled;
    uint64_t deg_settled;
    uint64_t fills_done;
    uint64_t gc_releases;
    uint64_t log_moves;
    uint64_t guards_active;
    uint64_t guards_passed;
    uint64_t holds_applied;
    uint64_t returns_after_out;
    uint64_t op_replace;
    uint64_t op_drain;
    uint64_t op_destroyed;
    uint64_t op_move_pos;
    uint64_t op_node_add;
    uint64_t m_faults;
    uint64_t mplus1_faults;
    uint64_t props_lost;
    uint64_t props_delayed;
    uint64_t failstops;
    uint64_t repl_faults;
    uint64_t disk_losses;
    uint64_t slow_opens;
};

struct sim {
    struct rng r;
    struct meta_state sm;
    uint64_t log_idx;
    uint64_t now_ms;
    uint32_t self_id;
    uint64_t next_rid;
    uint64_t next_store_id;
    uint64_t next_oid;
    struct place_settings pset;
    uint32_t nnodes;
    struct sim_node nodes[SIM_MAX_NODES];
    uint32_t nvols;
    struct sim_vol vols[SIM_MAX_VOLS];
    uint32_t nracks;
    int use_racks;
    uint32_t ninflight;
    struct sim_inflight inflight[SIM_MAX_INFLIGHT];
    int mutant;
    const char *mutant_name;
    int trace;
    uint64_t seed;
    uint32_t tick;
    int failed;
    char fail_why[512];
    struct sim_cov cov;
};

/* ------------------------------------------------------------------ violation reporting */

static void sim_fail(struct sim *s, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void sim_fail(struct sim *s, const char *fmt, ...)
{
    if (s->failed)
        return;
    s->failed = 1;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(s->fail_why, sizeof s->fail_why, fmt, ap);
    va_end(ap);
}

static void sim_trace(const struct sim *s, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void sim_trace(const struct sim *s, const char *fmt, ...)
{
    if (!s->trace)
        return;
    printf("[seed=%" PRIu64 " t=%" PRIu32 "] ", s->seed, s->tick);
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
}

/* ------------------------------------------------------------------ small helpers */

static uint64_t add_sat_u64(uint64_t a, uint64_t b)
{
    return b > UINT64_MAX - a ? UINT64_MAX : a + b;
}

static struct sim_node *sim_node_by_id(struct sim *s, uint32_t id)
{
    if (id < 1 || id > s->nnodes)
        return NULL;
    return &s->nodes[id - 1];
}

static const struct sim_node *sim_node_by_id_c(const struct sim *s, uint32_t id)
{
    if (id < 1 || id > s->nnodes)
        return NULL;
    return &s->nodes[id - 1];
}

static struct sim_vol *sim_vol_by_num(struct sim *s, uint32_t num)
{
    if (num < 1 || num > s->nvols)
        return NULL;
    return &s->vols[num - 1];
}

static uint32_t node_healthy_disks(const struct sim_node *n)
{
    if (!n || !n->alive || n->destroyed)
        return 0;
    uint32_t c = 0;
    for (int d = 0; d < n->ndisks; d++)
        c += n->disk_ok[d] != 0;
    return c;
}

static int node_pick_disk(struct sim *s, const struct sim_node *n)
{
    int ok[SIM_MAX_DISKS], cnt = 0;
    for (int d = 0; d < n->ndisks; d++)
        if (n->disk_ok[d])
            ok[cnt++] = d;
    if (!cnt)
        return -1;
    return ok[rng_u32(&s->r, (uint32_t)cnt)];
}

static int node_is_heard_fresh(const struct sim *s, const struct sim_node *n)
{
    if (!n)
        return 0;
    if (n->id == s->self_id)
        return n->alive != 0;
    if (n->last_heard_ms == 0)
        return 0;
    uint64_t age = s->now_ms >= n->last_heard_ms ? s->now_ms - n->last_heard_ms : 0;
    uint64_t timeout_ms = s->sm.set.node_timeout_ms ? s->sm.set.node_timeout_ms : 3000ull;
    return age < timeout_ms;
}

/* Matches server/pace.h (D18): margin 0 unlimited (line rate), margin 1 -> 4 * rebuild_mb_s, else rebuild_mb_s. */
static uint32_t pace_rate_for_margin(const struct sim *s, int margin)
{
    uint32_t base = s->sm.set.rebuild_mb_s ? s->sm.set.rebuild_mb_s : 200;
    if (margin <= 0)
        return 2500; /* margin 0: unlimited / line rate */
    if (margin == 1)
        return base * 4;
    return base;
}

static int sim_apply_entry(struct sim *s, struct meta_entry *e, int *res, uint32_t nres)
{
    struct meta_state prev;
    meta_copy(&prev, &s->sm);

    meta_entry_finish(e);
    int rc = meta_apply(&s->sm, ++s->log_idx, 1, e->buf, e->len, res, nres);
    meta_entry_free(e);

    if (rc != META_OK) {
        sim_fail(s, "meta_apply format error rc=%d", rc);
        meta_free(&prev);
        return rc;
    }
    char why[256];
    if (prev.initialised && meta_check_step(&prev, &s->sm, why, sizeof why) != 0)
        sim_fail(s, "meta_check_step failed: %s", why);
    meta_free(&prev);
    return rc;
}

/* ------------------------------------------------------------------ build place_in snapshot */

struct plan_snap {
    struct place_member pm[SIM_MAX_NODES];
    uint32_t npm;
    struct place_fill fills[SIM_MAX_VOLS * META_STRIPE_MAX];
    uint32_t nfills;
    struct place_replace replaces[SIM_MAX_NODES];
    uint32_t nreplaces;
    struct place_vol_size vsizes[SIM_MAX_VOLS];
    uint32_t nvsizes;
};

static void build_place_in(const struct sim *s, struct plan_snap *snap, struct place_in *in)
{
    memset(snap, 0, sizeof *snap);
    memset(in, 0, sizeof *in);

    for (uint32_t i = 0; i < s->nnodes; i++) {
        const struct sim_node *n = &s->nodes[i];
        struct place_member *pm = &snap->pm[snap->npm++];
        pm->id = n->id;
        if (n->last_heard_ms == 0)
            pm->heard_age_ms = UINT64_MAX;
        else
            pm->heard_age_ms = s->now_ms >= n->last_heard_ms ? s->now_ms - n->last_heard_ms : 0;
        pm->opening = (uint8_t)(n->opening != 0);
        pm->failstop = (uint8_t)(n->failstop_beat != 0);
        pm->fill_pct = 25;
        uint32_t stores = 0;
        for (int d = 0; d < n->ndisks; d++) {
            if (n->disk_ok[d] && !n->destroyed) {
                if (pm->nstore_ids < PLACE_STORES_MAX)
                    pm->store_ids[pm->nstore_ids++] = n->disk_store_id[d];
                stores++;
            }
        }
        pm->chunk_stores = n->alive ? stores : 0;
        if (n->alive && n->fault_vol > 0 && n->fault_ticks > 0) {
            pm->nfaults = 1;
            pm->faults[0].volume = n->fault_vol;
            pm->faults[0].fault_s = n->fault_ticks;
        }
    }

    for (uint32_t i = 0; i < s->sm.nmembers; i++) {
        const struct meta_member *mb = &s->sm.members[i];
        if (mb->replaced_by != 0 && snap->nreplaces < SIM_MAX_NODES) {
            snap->replaces[snap->nreplaces++] = (struct place_replace){
                .old_node = mb->id,
                .new_node = mb->replaced_by,
            };
        }
    }

    for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
        const struct meta_volume *v = &s->sm.volumes[vi];
        const struct sim_vol *sv = sim_vol_by_num((struct sim *)s, v->number);
        if (!sv)
            continue;
        if (snap->nvsizes < SIM_MAX_VOLS) {
            snap->vsizes[snap->nvsizes++] = (struct place_vol_size){
                .volume = v->number,
                .bytes = sv->bytes_mb << 20,
            };
        }
        if (v->redundancy != META_RED_EC)
            continue;
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
            if (v->stripe_pos[p].state != META_STRIPE_FILLING)
                continue;
            uint32_t holder = v->stripe[p];
            const struct sim_node *hn = sim_node_by_id_c(s, holder);
            if (!hn || !hn->alive || hn->destroyed)
                continue;
            uint64_t total = 0, done = 0;
            uint32_t hbit = 1u << (holder - 1);
            for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                const struct sim_obj *o = &sv->objs[oi];
                if (o->state != OBJ_LIVE)
                    continue;
                if (o->stripe_version >= v->stripe_version)
                    continue;
                total++;
                if (o->chunk_nodes[p] & hbit)
                    done++;
            }
            if (snap->nfills < SIM_MAX_VOLS * META_STRIPE_MAX) {
                snap->fills[snap->nfills++] = (struct place_fill){
                    .node = holder,
                    .volume = v->number,
                    .pos = p,
                    .stripe_version = v->stripe_version,
                    .done = done,
                    .total = total,
                    .whole = (uint8_t)(done == total ? 1 : 0),
                };
            }
        }
    }

    in->sm = &s->sm;
    in->members = snap->pm;
    in->nmembers = snap->npm;
    in->fills = snap->fills;
    in->nfills = snap->nfills;
    in->now_ms = s->now_ms;
    in->mono_ms = s->now_ms;
    in->self_id = s->self_id;
    in->set = s->pset;
    in->mutant = (s->mutant >= 0 && s->mutant <= PLACE_MUT_BALANCE_UNBOUNDED) ? s->mutant : PLACE_MUT_NONE;
    in->replaces = snap->nreplaces ? snap->replaces : NULL;
    in->nreplaces = snap->nreplaces;
    in->vol_sizes = snap->nvsizes ? snap->vsizes : NULL;
    in->nvol_sizes = snap->nvsizes;
}

/* ------------------------------------------------------------------ chunk deletion & GC checker (Rule 6) */

static void sim_delete_chunk(struct sim *s, struct sim_vol *sv, uint32_t oi, uint32_t pos, uint32_t node)
{
    struct sim_obj *o = &sv->objs[oi];
    const struct meta_volume *v = meta_volume(&s->sm, sv->number);
    uint32_t bit = 1u << (node - 1);
    if (!(o->chunk_nodes[pos] & bit))
        return;

    /* Checker Rule 6 (independent of caller arguments; checks real model & metadata state directly):
     *   - No chunk named by a pending (uncommitted) MK_COLD marker may be deleted.
     *   - For a retired/abandoned object, deletion requires a committed MK_GC marker (`o->mk_gc_committed`).
     *   - For a live committed object, deletion requires a committed release MK_GC matching `(pos, node, ver)`,
     *     the active holder `v->stripe[pos]` must not be `node`, and the current holder must have committed
     *     `META_STRIPE_WHOLE` in metadata. */
    if (o->state == OBJ_PENDING) {
        sim_fail(s, "chunk deleted while pending MK_COLD names it (vol=%" PRIu32 " oid=%" PRIu64 " pos=%" PRIu32
                    " node=%" PRIu32 ")",
                 sv->number, o->oid, pos, node);
        return;
    }
    if (o->state == OBJ_RETIRED) {
        if (!o->mk_gc_committed) {
            sim_fail(s, "retired chunk deleted without committed MK_GC (vol=%" PRIu32 " oid=%" PRIu64 " pos=%" PRIu32
                        " node=%" PRIu32 ")",
                     sv->number, o->oid, pos, node);
            return;
        }
    } else if (o->state == OBJ_LIVE && v) {
        if (v->stripe[pos] == node) {
            sim_fail(s, "live object chunk deleted on active holder (vol=%" PRIu32 " oid=%" PRIu64 " pos=%" PRIu32
                        " node=%" PRIu32 ")",
                     sv->number, o->oid, pos, node);
            return;
        }
        int has_committed_rel = 0;
        for (uint32_t ri = 0; ri < sv->nrel; ri++) {
            const struct sim_release *rel = &sv->rels[ri];
            if (rel->mk_gc_committed && rel->pos == pos && rel->old_node == node &&
                o->stripe_version < rel->release_ver) {
                has_committed_rel = 1;
                break;
            }
        }
        if (!has_committed_rel) {
            sim_fail(s, "chunk deleted without committed MK_GC (vol=%" PRIu32 " oid=%" PRIu64 " pos=%" PRIu32
                        " node=%" PRIu32 ")",
                     sv->number, o->oid, pos, node);
            return;
        }
        if (v->stripe_pos[pos].state != META_STRIPE_WHOLE) {
            sim_fail(s, "chunk released before current holder reported whole (vol=%" PRIu32 " oid=%" PRIu64
                        " pos=%" PRIu32 " node=%" PRIu32 ")",
                     sv->number, o->oid, pos, node);
            return;
        }
        uint32_t cur_bit = 1u << (v->stripe[pos] - 1);
        if (!o->deg_unwritten[pos] && !(o->chunk_nodes[pos] & cur_bit)) {
            sim_fail(s, "chunk released while active holder %" PRIu32 " lacks chunk (vol=%" PRIu32 " oid=%" PRIu64
                        " pos=%" PRIu32 " old=%" PRIu32 ")",
                     v->stripe[pos], sv->number, o->oid, pos, node);
            return;
        }
    } else {
        sim_fail(s, "chunk deleted on invalid object state=%u (vol=%" PRIu32 " oid=%" PRIu64 " pos=%" PRIu32
                    " node=%" PRIu32 ")",
                 o->state, sv->number, o->oid, pos, node);
        return;
    }

    o->chunk_nodes[pos] &= ~bit;
    o->chunk_disk[pos][node - 1] = 0;
}

/* ------------------------------------------------------------------ independent checker helpers */

static int chk_is_mid_move_target(const struct meta_state *sm, uint32_t node)
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

static int chk_is_last_isr_member(const struct meta_state *sm, uint32_t node)
{
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !meta_volume_in_isr(v, node))
            continue;
        int other = 0;
        for (uint32_t j = 0; j < v->nisr; j++) {
            uint32_t u = v->isr[j];
            if (u == node)
                continue;
            const struct meta_member *um = meta_member(sm, u);
            if (um && (um->state == META_UP || um->state == META_DRAINING)) {
                other = 1;
                break;
            }
        }
        if (!other)
            return 1;
    }
    return 0;
}

static int chk_vol_has_move(const struct meta_volume *v)
{
    for (uint32_t k = 0; k < v->nrep; k++)
        if (v->rep[k].learner || v->rep[k].replaces)
            return 1;
    return 0;
}

static int chk_vol_has_replaces(const struct meta_volume *v)
{
    for (uint32_t k = 0; k < v->nrep; k++)
        if (v->rep[k].replaces != 0)
            return 1;
    return 0;
}

static int chk_has_dup_u32(const uint32_t *a, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++)
        for (uint32_t j = i + 1; j < n; j++)
            if (a[i] == a[j])
                return 1;
    return 0;
}

/* Compute exact minimum achievable max-per-rack when choosing `need` nodes from eligible candidates `cands[0..nc-1]`. */
static uint32_t optimal_max_per_rack(const struct meta_state *sm, const uint32_t *cands, uint32_t nc, uint32_t need)
{
    const char *rnames[SIM_MAX_NODES];
    uint32_t rcap[SIM_MAX_NODES];
    uint32_t rused[SIM_MAX_NODES];
    uint32_t nr = 0;
    for (uint32_t i = 0; i < nc; i++) {
        const struct meta_member *mb = &sm->members[cands[i]];
        uint32_t r = 0;
        for (; r < nr; r++)
            if (!strcmp(rnames[r], mb->rack))
                break;
        if (r == nr) {
            rnames[nr] = mb->rack;
            rcap[nr] = 0;
            rused[nr] = 0;
            nr++;
        }
        rcap[r]++;
    }
    for (uint32_t k = 0; k < need; k++) {
        int best = -1;
        for (uint32_t r = 0; r < nr; r++) {
            if (rused[r] >= rcap[r])
                continue;
            if (best < 0 || rused[r] < rused[best])
                best = (int)r;
        }
        if (best >= 0)
            rused[best]++;
    }
    uint32_t mx = 0;
    for (uint32_t r = 0; r < nr; r++)
        if (rused[r] > mx)
            mx = rused[r];
    return mx;
}

static uint32_t actual_max_per_rack(const struct meta_state *sm, const uint32_t *nodes, uint32_t n)
{
    uint32_t mx = 0;
    for (uint32_t i = 0; i < n; i++) {
        const struct meta_member *mi = meta_member(sm, nodes[i]);
        if (!mi)
            continue;
        uint32_t c = 0;
        for (uint32_t j = 0; j < n; j++) {
            const struct meta_member *mj = meta_member(sm, nodes[j]);
            if (mj && !strcmp(mi->rack, mj->rack))
                c++;
        }
        if (c > mx)
            mx = c;
    }
    return mx;
}

/* ------------------------------------------------------------------ Checker: initial placement */

static void check_initial_placement(struct sim *s, const struct place_in *in, const uint32_t *nodes, uint32_t n,
                                    int is_stripe)
{
    const struct meta_state *sm = in->sm;
    if (chk_has_dup_u32(nodes, n)) {
        sim_fail(s, "initial %s placement has duplicate server (E9 violated)", is_stripe ? "stripe" : "log");
        return;
    }
    uint32_t cands[SIM_MAX_NODES], nc = 0;
    int all_racks = 1;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
            continue;
        const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
        if (!sn || !node_is_heard_fresh(s, sn))
            continue;
        if (is_stripe && node_healthy_disks(sn) == 0)
            continue;
        cands[nc++] = i;
        if (!mb->rack[0])
            all_racks = 0;
    }
    if (all_racks && nc >= n) {
        uint32_t opt = optimal_max_per_rack(sm, cands, nc, n);
        uint32_t act = actual_max_per_rack(sm, nodes, n);
        if (act > opt) {
            sim_fail(s, "initial %s placement violated E9 rack cap: max_per_rack=%" PRIu32 " > optimal %" PRIu32,
                     is_stripe ? "stripe" : "log", act, opt);
        }
    }
}

/* ------------------------------------------------------------------ Checker: pout_plan output (Rule 7) */

/* Independent D4 mass-OUT guard oracle (does not trust pout_guard_eval; cross-checked against it below). */
static void chk_guard_eval(const struct meta_state *sm, uint32_t out_guard_pct, uint32_t out_guard_hold_s,
                           uint64_t now_ms, struct pout_guard *g)
{
    memset(g, 0, sizeof *g);
    uint64_t downs[SIM_MAX_NODES];
    uint32_t non_out = 0, ndown = 0;
    int all_racks = 1, multi_rack = 0;
    const char *first_rack = NULL;

    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->state == META_OUT || m->state == META_DESTROYED)
            continue;
        non_out++;
        if (!m->rack[0]) {
            all_racks = 0;
        } else if (!first_rack) {
            first_rack = m->rack;
        } else if (strcmp(m->rack, first_rack) != 0) {
            multi_rack = 1;
        }
        if (m->state == META_DOWN && m->down_since_ms > 0 && ndown < SIM_MAX_NODES)
            downs[ndown++] = m->down_since_ms;
    }

    int pct_trig = 0;
    uint64_t pct_start = 0;
    if (non_out > 0 && (uint64_t)ndown * 100ull > (uint64_t)non_out * (uint64_t)out_guard_pct) {
        pct_trig = 1;
        for (uint32_t i = 1; i < ndown; i++) {
            uint64_t key = downs[i];
            uint32_t j = i;
            while (j > 0 && downs[j - 1] > key) {
                downs[j] = downs[j - 1];
                j--;
            }
            downs[j] = key;
        }
        uint32_t k_cross = (uint32_t)(((uint64_t)non_out * (uint64_t)out_guard_pct) / 100ull + 1ull);
        if (k_cross < 1)
            k_cross = 1;
        if (k_cross > ndown)
            k_cross = ndown;
        pct_start = downs[k_cross - 1];
    }

    int rack_trig = 0;
    uint64_t rack_start = 0;
    if (all_racks && multi_rack) {
        for (uint32_t i = 0; i < sm->nmembers; i++) {
            const struct meta_member *mi = &sm->members[i];
            if (mi->state == META_OUT || mi->state == META_DESTROYED)
                continue;
            uint32_t rtot = 0, rdown = 0;
            uint64_t rmax = 0;
            for (uint32_t k = 0; k < sm->nmembers; k++) {
                const struct meta_member *mk = &sm->members[k];
                if (mk->state == META_OUT || mk->state == META_DESTROYED || strcmp(mk->rack, mi->rack) != 0)
                    continue;
                rtot++;
                if (mk->state == META_DOWN && mk->down_since_ms > 0) {
                    rdown++;
                    if (mk->down_since_ms > rmax)
                        rmax = mk->down_since_ms;
                }
            }
            if (rtot >= 2 && rdown == rtot) {
                if (!rack_trig || rmax < rack_start) {
                    rack_trig = 1;
                    rack_start = rmax;
                }
            }
        }
    }

    g->non_out = non_out;
    g->ndown = ndown;
    g->pct_triggered = pct_trig;
    g->rack_triggered = rack_trig;
    g->active = pct_trig || rack_trig;
    if (g->active) {
        g->start_ms = (pct_trig && rack_trig) ? (pct_start < rack_start ? pct_start : rack_start)
                                              : (pct_trig ? pct_start : rack_start);
        g->due_ms = add_sat_u64(g->start_ms, (uint64_t)out_guard_hold_s * 1000ull);
        g->holding = now_ms < g->due_ms;
    }
}

static void check_pout_output(struct sim *s, const struct place_in *in, const struct place_out *out)
{
    const struct meta_state *sm = in->sm;
    uint64_t node_to_ms = sm->set.node_timeout_ms;

    struct pout_guard g, g_real;
    chk_guard_eval(sm, in->set.out_guard_pct, in->set.out_guard_hold_s, in->now_ms, &g);
    pout_guard_eval(sm, in->set.out_guard_pct, in->set.out_guard_hold_s, in->now_ms, &g_real);
    if (g_real.active != g.active || g_real.holding != g.holding ||
        (g.active && (g_real.start_ms != g.start_ms || g_real.due_ms != g.due_ms))) {
        sim_fail(s, "pout_guard_eval disagreed with independent D4 guard oracle (active=%d/%d hold=%d/%d due=%" PRIu64
                    "/%" PRIu64 ")",
                 g_real.active, g.active, g_real.holding, g.holding, g_real.due_ms, g.due_ms);
        return;
    }
    if (g.active)
        s->cov.guards_active++;
    if (g.active && !g.holding)
        s->cov.guards_passed++;

    /* 1. Validate every META_OUT proposal emitted by pout_plan. */
    for (uint32_t i = 0; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind != PLACE_PROP_MEMBER_STATE || p->state != META_OUT)
            continue;
        const struct meta_member *m = meta_member(sm, p->node);
        if (!m) {
            sim_fail(s, "pout_plan proposed OUT for unknown member %" PRIu32, p->node);
            return;
        }
        if (m->id == in->self_id) {
            sim_fail(s, "pout_plan proposed OUT for self %" PRIu32, m->id);
            return;
        }
        if (m->state != META_DOWN || m->down_since_ms == 0) {
            sim_fail(s, "pout_plan proposed OUT for non-DOWN member %" PRIu32 " (state=%u)", m->id, m->state);
            return;
        }
        if (m->hold_until_ms > in->now_ms) {
            sim_fail(s, "pout_plan proposed OUT for held member %" PRIu32 " (hold_until=%" PRIu64 " > now=%" PRIu64 ")",
                     m->id, m->hold_until_ms, in->now_ms);
            return;
        }
        if (g.holding) {
            sim_fail(s, "pout_plan proposed OUT for member %" PRIu32 " while mass-OUT guard holds (now=%" PRIu64
                        " < due=%" PRIu64 ")",
                     m->id, in->now_ms, g.due_ms);
            return;
        }
        uint32_t base_s = (m->down_reason == META_DOWN_FAILSTOP) ? in->set.failstop_out_s : in->set.rebuild_after_s;
        uint64_t due_ms = add_sat_u64(m->down_since_ms, (uint64_t)base_s * 1000ull);
        const struct sim_node *sn = sim_node_by_id_c(s, m->id);
        int is_open = sn && sn->opening && node_is_heard_fresh(s, sn);
        if (is_open)
            due_ms = add_sat_u64(due_ms, (uint64_t)in->set.opening_hold_s * 1000ull);
        if (in->now_ms < due_ms) {
            sim_fail(s, "pout_plan proposed OUT early for member %" PRIu32 " (now=%" PRIu64 " < due=%" PRIu64 ")",
                     m->id, in->now_ms, due_ms);
            return;
        }
        if (chk_is_mid_move_target(sm, m->id)) {
            sim_fail(s, "pout_plan proposed OUT for mid-move target %" PRIu32, m->id);
            return;
        }
        if (g.active && m->down_reason != META_DOWN_FAILSTOP && chk_is_last_isr_member(sm, m->id)) {
            sim_fail(s, "pout_plan proposed OUT for last ISR member %" PRIu32 " under guard", m->id);
            return;
        }
    }

    /* 2. Liveness / bound check: guard never holds past its bound, opening hold never past opening_hold_s,
     *    fail-stop never slower than failstop_out_s. */
    if (!g.holding) {
        for (uint32_t i = 0; i < sm->nmembers; i++) {
            const struct meta_member *m = &sm->members[i];
            if (m->state != META_DOWN || m->down_since_ms == 0 || m->id == in->self_id)
                continue;
            if (m->hold_until_ms > in->now_ms)
                continue;
            if (chk_is_mid_move_target(sm, m->id))
                continue;
            if (g.active && m->down_reason != META_DOWN_FAILSTOP && chk_is_last_isr_member(sm, m->id))
                continue;

            uint32_t base_s =
                (m->down_reason == META_DOWN_FAILSTOP) ? in->set.failstop_out_s : in->set.rebuild_after_s;
            uint64_t due_ms = add_sat_u64(m->down_since_ms, (uint64_t)base_s * 1000ull);
            const struct sim_node *sn = sim_node_by_id_c(s, m->id);
            int is_open = sn && sn->opening && node_is_heard_fresh(s, sn);
            if (is_open)
                due_ms = add_sat_u64(due_ms, (uint64_t)in->set.opening_hold_s * 1000ull);
            if (in->now_ms < due_ms)
                continue;

            int found = 0;
            for (uint32_t k = 0; k < out->nprops; k++) {
                if (out->props[k].kind == PLACE_PROP_MEMBER_STATE && out->props[k].node == m->id &&
                    out->props[k].state == META_OUT) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                if (is_open) {
                    sim_fail(s, "opening hold kept member %" PRIu32 " past opening_hold_s (now=%" PRIu64
                                " >= due=%" PRIu64 ")",
                             m->id, in->now_ms, due_ms);
                } else if (g.active && !g.holding) {
                    sim_fail(s, "mass-OUT guard held member %" PRIu32 " past bound (now=%" PRIu64 " >= due=%" PRIu64 ")",
                             m->id, in->now_ms, g.due_ms);
                } else if (m->down_reason == META_DOWN_FAILSTOP) {
                    sim_fail(s, "fail-stop member %" PRIu32 " not marked OUT at failstop_out_s (now=%" PRIu64
                                " >= due=%" PRIu64 ")",
                             m->id, in->now_ms, due_ms);
                } else {
                    sim_fail(s, "DOWN member %" PRIu32 " not marked OUT at rebuild_after_s (now=%" PRIu64
                                " >= due=%" PRIu64 ")",
                             m->id, in->now_ms, due_ms);
                }
                return;
            }
        }
    }

    /* 3. Per-replica LOCAL_FAULT fast path check (catches failstopslow on replica faults). */
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->state != META_UP)
            continue;
        const struct place_member *pm = NULL;
        for (uint32_t j = 0; j < in->nmembers; j++)
            if (in->members[j].id == m->id) {
                pm = &in->members[j];
                break;
            }
        if (!pm || !pm->nfaults || pm->heard_age_ms == UINT64_MAX || (node_to_ms && pm->heard_age_ms >= node_to_ms))
            continue;
        for (uint32_t j = 0; j < pm->nfaults && j < PLACE_FAULTS_MAX; j++) {
            if (pm->faults[j].fault_s < in->set.failstop_out_s)
                continue;
            const struct meta_volume *v = meta_volume(sm, pm->faults[j].volume);
            if (!v || v->kind != META_VOL_REPLICATED || !meta_replica(v, m->id) || chk_vol_has_replaces(v))
                continue;
            int found = 0;
            for (uint32_t k = 0; k < out->nprops; k++) {
                if (out->props[k].kind == PLACE_PROP_REPLICA_ADD && out->props[k].volume == v->number) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                sim_fail(s, "replica LOCAL_FAULT on vol %" PRIu32 " node %" PRIu32 " (fault_s=%" PRIu64
                            ") not escalated at failstop_out_s=%" PRIu32,
                         v->number, m->id, pm->faults[j].fault_s, in->set.failstop_out_s);
                return;
            }
        }
    }
}

/* ------------------------------------------------------------------ Checker: place_plan output (Rules 2, 3, 4, 5) */

static int chk_member_placeable(const struct sim *s, const struct place_in *in, const struct meta_member *mb,
                                const uint8_t *pend_gone, uint32_t mi)
{
    if (!mb || pend_gone[mi] || mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
        return 0;
    if (in->replaces) {
        for (uint32_t i = 0; i < in->nreplaces; i++)
            if (in->replaces[i].old_node == mb->id && in->replaces[i].new_node != 0)
                return 0;
    }
    const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
    return sn && node_is_heard_fresh(s, sn);
}

static uint32_t chk_forced_target(const struct place_in *in, uint32_t old_node)
{
    if (!in->replaces)
        return 0;
    for (uint32_t i = 0; i < in->nreplaces; i++)
        if (in->replaces[i].old_node == old_node && in->replaces[i].new_node != 0)
            return in->replaces[i].new_node;
    return 0;
}

struct chk_live_ctx {
    const struct sim *s;
    const uint8_t *pend_gone;
};

static int chk_member_live(const void *vctx, uint32_t node)
{
    const struct chk_live_ctx *ctx = vctx;
    const struct meta_state *sm = &ctx->s->sm;
    for (uint32_t mi = 0; mi < sm->nmembers; mi++) {
        if (sm->members[mi].id == node) {
            if (ctx->pend_gone[mi])
                return 0;
            const struct sim_node *sn = sim_node_by_id_c(ctx->s, node);
            return sn && node_is_heard_fresh(ctx->s, sn);
        }
    }
    return 0;
}

static void check_place_output(struct sim *s, const struct place_in *in, const struct place_out *out)
{
    const struct meta_state *sm = in->sm;
    uint32_t nm = sm->nmembers;

    uint8_t pend_gone[SIM_MAX_NODES] = {0};
    uint32_t log_recv[SIM_MAX_NODES] = {0}, log_recv_urg[SIM_MAX_NODES] = {0};
    uint32_t stripe_recv[SIM_MAX_NODES] = {0}, stripe_recv_urg[SIM_MAX_NODES] = {0};

    /* Track pend_gone from MEMBER_STATE proposals emitted earlier this tick. */
    for (uint32_t i = 0; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind == PLACE_PROP_MEMBER_STATE && p->state >= META_DOWN && p->state != META_DRAINING) {
            for (uint32_t mi = 0; mi < nm; mi++)
                if (sm->members[mi].id == p->node)
                    pend_gone[mi] = 1;
        }
    }

    /* Track existing in-flight rebuilds per receiver (mirroring place.c step 4 for capacity-aware rack checks). */
    uint32_t whole_mask[SIM_MAX_VOLS] = {0};
    for (uint32_t i = 0; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind == PLACE_PROP_STRIPE_WHOLE && p->volume >= 1 && p->volume <= SIM_MAX_VOLS &&
            p->pos < META_STRIPE_MAX)
            whole_mask[p->volume - 1] |= (1u << p->pos);
    }

    struct chk_live_ctx lctx = {.s = s, .pend_gone = pend_gone};
    struct meta_liveness liveness = {.live = chk_member_live, .ctx = &lctx};
    int vol_ym[SIM_MAX_VOLS] = {0}, vol_sm[SIM_MAX_VOLS] = {0};
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        int ym = 0, smarg = 0;
        meta_vol_margins(sm, v, &liveness, &ym, &smarg);
        if (v->number >= 1 && v->number <= SIM_MAX_VOLS) {
            vol_ym[v->number - 1] = ym;
            vol_sm[v->number - 1] = smarg;
        }
        for (uint32_t k = 0; k < v->nrep; k++) {
            if (!v->rep[k].learner && !v->rep[k].replaces)
                continue;
            for (uint32_t mi = 0; mi < nm; mi++) {
                if (sm->members[mi].id == v->rep[k].node) {
                    log_recv[mi]++;
                    if (ym <= 0)
                        log_recv_urg[mi]++;
                }
            }
        }
        if (v->redundancy == META_RED_EC) {
            uint32_t wm = (v->number >= 1 && v->number <= SIM_MAX_VOLS) ? whole_mask[v->number - 1] : 0;
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                if (v->stripe_pos[p].state != META_STRIPE_FILLING || (wm & (1u << p)))
                    continue;
                for (uint32_t mi = 0; mi < nm; mi++) {
                    if (sm->members[mi].id == v->stripe[p]) {
                        stripe_recv[mi]++;
                        if (smarg <= 0)
                            stripe_recv_urg[mi]++;
                    }
                }
            }
        }
    }

    uint32_t rebuild_max = sm->set.rebuild_max ? sm->set.rebuild_max : 2;
    uint32_t stripe_cap = in->set.rebuild_concurrency
                              ? in->set.rebuild_concurrency
                              : (sm->set.rebuild_concurrency ? sm->set.rebuild_concurrency : 4);

    int have_prev_margin = 0;
    int prev_margin = 0;
    uint8_t stripe_emitted[SIM_MAX_VOLS] = {0};

    /* Note: place_plan may have compacted pout_plan's unresolved fault REPLICA_ADD proposals, so scan all proposals
     * starting from 0 and check every non-MEMBER_STATE proposal emitted by place_plan. */
    for (uint32_t i = 0; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind == PLACE_PROP_STRIPE_SET && p->volume >= 1 && p->volume <= SIM_MAX_VOLS)
            stripe_emitted[p->volume - 1] = 1;
    }

    for (uint32_t i = 0; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind == PLACE_PROP_MEMBER_STATE)
            continue;

        const struct meta_volume *v = meta_volume(sm, p->volume);
        if (!v) {
            sim_fail(s, "place_plan emitted proposal kind=%d for unknown volume %" PRIu32, p->kind, p->volume);
            return;
        }

        /* Rule 2: planned log move must never remove old replica before replacement joins ISR (moveshrinks). */
        if (p->kind == PLACE_PROP_REPLICA_REMOVE) {
            sim_fail(s, "place_plan emitted REPLICA_REMOVE on vol %" PRIu32 " node %" PRIu32
                        " (shrinks replica set / lowers margin before replacement joins)",
                     p->volume, p->node);
            return;
        }

        /* Rule 5: STRIPE_WHOLE only after every live object below stripe_version is on the holder (wholenobarrier). */
        if (p->kind == PLACE_PROP_STRIPE_WHOLE) {
            if (v->redundancy != META_RED_EC || p->pos >= v->nstripe) {
                sim_fail(s, "STRIPE_WHOLE invalid vol %" PRIu32 " pos %" PRIu32, p->volume, p->pos);
                return;
            }
            if (v->stripe_pos[p->pos].state != META_STRIPE_FILLING) {
                sim_fail(s, "STRIPE_WHOLE on non-filling vol %" PRIu32 " pos %" PRIu32, p->volume, p->pos);
                return;
            }
            if (p->version != v->stripe_version) {
                sim_fail(s, "STRIPE_WHOLE stale version %" PRIu64 " != %" PRIu64 " on vol %" PRIu32 " pos %" PRIu32,
                         p->version, v->stripe_version, p->volume, p->pos);
                return;
            }
            const struct sim_vol *sv = sim_vol_by_num(s, v->number);
            uint32_t holder = v->stripe[p->pos];
            uint32_t hbit = 1u << (holder - 1);
            for (uint32_t oi = 0; sv && oi < sv->nobjs; oi++) {
                const struct sim_obj *o = &sv->objs[oi];
                if (o->state != OBJ_LIVE || o->stripe_version >= p->version)
                    continue;
                if (!(o->chunk_nodes[p->pos] & hbit)) {
                    sim_fail(s, "STRIPE_WHOLE before barrier: vol %" PRIu32 " pos %" PRIu32 " holder %" PRIu32
                                " missing oid %" PRIu64 " (obj_ver=%" PRIu64 " < V=%" PRIu64 ")",
                             v->number, p->pos, holder, o->oid, o->stripe_version, p->version);
                    return;
                }
            }
            continue;
        }

        /* Rule 3: E13 rebuild order by margin (independent margin check + non-decreasing margin + no starvation). */
        if (p->kind == PLACE_PROP_REPLICA_ADD || p->kind == PLACE_PROP_STRIPE_SET) {
            if (v->number >= 1 && v->number <= SIM_MAX_VOLS) {
                int exp_m = (p->kind == PLACE_PROP_REPLICA_ADD) ? vol_ym[v->number - 1] : vol_sm[v->number - 1];
                if (p->margin != exp_m) {
                    sim_fail(s, "E13 margin mismatch on vol %" PRIu32 " kind=%d: reported %d != expected %d",
                             p->volume, p->kind, p->margin, exp_m);
                    return;
                }
            }
            if (have_prev_margin && p->margin < prev_margin) {
                sim_fail(s, "E13 rebuild order violated: vol %" PRIu32 " (margin=%d) emitted after higher margin %d",
                         p->volume, p->margin, prev_margin);
                return;
            }
            have_prev_margin = 1;
            prev_margin = p->margin;

            /* Even if only one STRIPE_SET fits this tick, verify no unstarted eligible lower-margin OUT/DESTROYED
             * stripe job was starved behind this higher-margin one on the same target server. */
            if (p->kind == PLACE_PROP_STRIPE_SET && p->margin > 0) {
                for (uint32_t vi2 = 0; vi2 < sm->nvolumes; vi2++) {
                    const struct meta_volume *v2 = &sm->volumes[vi2];
                    if (v2->redundancy != META_RED_EC || v2->number < 1 || v2->number > SIM_MAX_VOLS)
                        continue;
                    if (stripe_emitted[v2->number - 1] || vol_sm[v2->number - 1] >= p->margin)
                        continue;
                    int t_in_v2 = 0, has_unforced_out = 0;
                    for (uint32_t k = 0; k < v2->nstripe; k++) {
                        if (v2->stripe[k] == p->node)
                            t_in_v2 = 1;
                        const struct meta_member *hm = meta_member(sm, v2->stripe[k]);
                        if ((!hm || hm->state == META_OUT || hm->state == META_DESTROYED) &&
                            !chk_forced_target(in, v2->stripe[k]))
                            has_unforced_out = 1;
                    }
                    if (has_unforced_out && !t_in_v2) {
                        sim_fail(s, "E13 rebuild starvation: vol %" PRIu32 " (margin=%d) waited behind vol %" PRIu32
                                    " (margin=%d) for target %" PRIu32,
                                 v2->number, vol_sm[v2->number - 1], p->volume, p->margin, p->node);
                        return;
                    }
                }
            }
        }

        /* Rule 4: valid list, one position per server, and E9 rack optimality for REPLICA_ADD. */
        if (p->kind == PLACE_PROP_REPLICA_ADD) {
            if (meta_replica(v, p->node) != NULL) {
                sim_fail(s, "REPLICA_ADD duplicate server: vol %" PRIu32 " already has node %" PRIu32, p->volume,
                         p->node);
                return;
            }
            const struct meta_member *tmb = meta_member(sm, p->node);
            if (!tmb || tmb->state != META_UP) {
                sim_fail(s, "REPLICA_ADD non-UP target node %" PRIu32 " on vol %" PRIu32, p->node, p->volume);
                return;
            }
            int urgent = p->margin <= 0;
            uint32_t forced = p->other_node ? chk_forced_target(in, p->other_node) : 0;
            if (!forced) {
                /* Verify rack-spreading optimality (catches rackignored). */
                int racks_ok = 1;
                const char *rem_racks[META_REPLICAS_MAX];
                uint32_t nrem = 0;
                for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++) {
                    if (v->rep[k].node == p->other_node)
                        continue;
                    const struct meta_member *rm = meta_member(sm, v->rep[k].node);
                    if (!rm || !rm->rack[0]) {
                        racks_ok = 0;
                        break;
                    }
                    rem_racks[nrem++] = rm->rack;
                }
                for (uint32_t mi = 0; mi < nm && racks_ok; mi++) {
                    const struct meta_member *mb = &sm->members[mi];
                    if (!chk_member_placeable(s, in, mb, pend_gone, mi))
                        continue;
                    if (!meta_replica(v, mb->id) && !mb->rack[0])
                        racks_ok = 0;
                }
                if (racks_ok) {
                    uint32_t chosen_rc = 0;
                    for (uint32_t j = 0; j < nrem; j++)
                        if (!strcmp(tmb->rack, rem_racks[j]))
                            chosen_rc++;
                    uint32_t min_rc = UINT32_MAX;
                    for (uint32_t mi = 0; mi < nm; mi++) {
                        uint32_t used = urgent ? log_recv_urg[mi] : log_recv[mi];
                        if (used >= rebuild_max)
                            continue;
                        const struct meta_member *mb = &sm->members[mi];
                        if (!chk_member_placeable(s, in, mb, pend_gone, mi) || mb->id == p->other_node)
                            continue;
                        if (meta_replica(v, mb->id))
                            continue;
                        uint32_t rc = 0;
                        for (uint32_t j = 0; j < nrem; j++)
                            if (!strcmp(mb->rack, rem_racks[j]))
                                rc++;
                        if (rc < min_rc)
                            min_rc = rc;
                    }
                    if (min_rc < chosen_rc) {
                        sim_fail(s, "REPLICA_ADD violated E9 rack rule on vol %" PRIu32 ": picked node %" PRIu32
                                    " (rack %s, count %" PRIu32 ") while rack count %" PRIu32 " was available",
                                 p->volume, p->node, tmb->rack, chosen_rc, min_rc);
                        return;
                    }
                }
            }
            for (uint32_t mi = 0; mi < nm; mi++) {
                if (sm->members[mi].id == p->node) {
                    log_recv[mi]++;
                    if (urgent)
                        log_recv_urg[mi]++;
                }
            }
        }

        /* Rule 4: valid list, one position per server, and E9 rack optimality for STRIPE_SET. */
        if (p->kind == PLACE_PROP_STRIPE_SET) {
            if (v->redundancy != META_RED_EC || p->pos >= v->nstripe) {
                sim_fail(s, "STRIPE_SET invalid vol %" PRIu32 " pos %" PRIu32, p->volume, p->pos);
                return;
            }
            for (uint32_t k = 0; k < v->nstripe; k++) {
                if (k != p->pos && v->stripe[k] == p->node) {
                    sim_fail(s, "STRIPE_SET duplicate server: vol %" PRIu32 " pos %" PRIu32 " node %" PRIu32
                                " already holds pos %" PRIu32,
                             p->volume, p->pos, p->node, k);
                    return;
                }
            }
            /* Future r9-return hook (takebackwhole): never take back a position that is already WHOLE and UP
             * to put it back into FILLING, which would lower settled margin. */
            if (v->stripe_pos[p->pos].state == META_STRIPE_WHOLE && p->reason == META_STRIPE_FILLING) {
                const struct meta_member *cur = meta_member(sm, v->stripe[p->pos]);
                if (cur && cur->state == META_UP && !chk_forced_target(in, cur->id)) {
                    sim_fail(s, "STRIPE_SET took back WHOLE UP position vol %" PRIu32 " pos %" PRIu32
                                " (cur=%" PRIu32 " -> new=%" PRIu32 ")",
                             p->volume, p->pos, cur->id, p->node);
                    return;
                }
            }
            const struct meta_member *tmb = meta_member(sm, p->node);
            if (!tmb || tmb->state != META_UP) {
                sim_fail(s, "STRIPE_SET non-UP target node %" PRIu32 " on vol %" PRIu32, p->node, p->volume);
                return;
            }
            int urgent = p->margin <= 0;
            uint32_t old_holder = v->stripe[p->pos];
            uint32_t forced = chk_forced_target(in, old_holder);
            if (!forced) {
                int racks_ok = 1;
                const char *rem_racks[META_STRIPE_MAX];
                uint32_t nrem = 0;
                for (uint32_t k = 0; k < v->nstripe && k < META_STRIPE_MAX; k++) {
                    if (k == p->pos)
                        continue;
                    const struct meta_member *rm = meta_member(sm, v->stripe[k]);
                    if (!rm || !rm->rack[0]) {
                        racks_ok = 0;
                        break;
                    }
                    rem_racks[nrem++] = rm->rack;
                }
                for (uint32_t mi = 0; mi < nm && racks_ok; mi++) {
                    const struct meta_member *mb = &sm->members[mi];
                    const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
                    if (!chk_member_placeable(s, in, mb, pend_gone, mi) || !sn || node_healthy_disks(sn) == 0)
                        continue;
                    int has_sp = 0;
                    for (uint32_t k = 0; k < v->nstripe; k++)
                        if (v->stripe[k] == mb->id)
                            has_sp = 1;
                    if (!has_sp && !mb->rack[0])
                        racks_ok = 0;
                }
                if (racks_ok) {
                    uint32_t chosen_rc = 0;
                    for (uint32_t j = 0; j < nrem; j++)
                        if (!strcmp(tmb->rack, rem_racks[j]))
                            chosen_rc++;
                    uint32_t min_rc = UINT32_MAX;
                    for (uint32_t mi = 0; mi < nm; mi++) {
                        uint32_t used = urgent ? stripe_recv_urg[mi] : stripe_recv[mi];
                        if (used >= stripe_cap)
                            continue;
                        const struct meta_member *mb = &sm->members[mi];
                        const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
                        if (!chk_member_placeable(s, in, mb, pend_gone, mi) || mb->id == old_holder || !sn ||
                            node_healthy_disks(sn) == 0)
                            continue;
                        int has_sp = 0;
                        for (uint32_t k = 0; k < v->nstripe; k++)
                            if (v->stripe[k] == mb->id)
                                has_sp = 1;
                        if (has_sp)
                            continue;
                        uint32_t rc = 0;
                        for (uint32_t j = 0; j < nrem; j++)
                            if (!strcmp(mb->rack, rem_racks[j]))
                                rc++;
                        if (rc < min_rc)
                            min_rc = rc;
                    }
                    if (min_rc < chosen_rc) {
                        sim_fail(s, "STRIPE_SET violated E9 rack rule on vol %" PRIu32 " pos %" PRIu32
                                    ": picked node %" PRIu32 " (rack %s, count %" PRIu32 ") while count %" PRIu32
                                    " was available",
                                 p->volume, p->pos, p->node, tmb->rack, chosen_rc, min_rc);
                        return;
                    }
                }
            }
            for (uint32_t mi = 0; mi < nm; mi++) {
                if (sm->members[mi].id == p->node) {
                    stripe_recv[mi]++;
                    if (urgent)
                        stripe_recv_urg[mi]++;
                }
            }
        }
    }
}

/* ------------------------------------------------------------------ Checker: balance_plan output (D19 hook) */

static void check_balance_output(struct sim *s, const struct place_in *in, const struct place_out *out,
                                 uint32_t place_end_idx)
{
    uint32_t nbal = out->nprops > place_end_idx ? out->nprops - place_end_idx : 0;
    if (nbal == 0)
        return;

    const struct meta_state *sm = in->sm;
    /* Total positions across all volumes (for 25% plan cap). */
    uint32_t total_pos = 0;
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        total_pos += v->nrep + (v->redundancy == META_RED_EC ? v->nstripe : 0);
    }
    uint32_t max_plan = total_pos / 4;
    if (max_plan < 1)
        max_plan = 1;

    uint32_t vol_moving[SIM_MAX_VOLS] = {0};
    uint32_t bal_moves = 0;
    for (uint32_t i = place_end_idx; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind == PLACE_PROP_REPLICA_REMOVE) {
            sim_fail(s, "balance_plan emitted REPLICA_REMOVE on vol %" PRIu32 " (lowers margin)", p->volume);
            return;
        }
        if (p->kind == PLACE_PROP_REPLICA_ADD || p->kind == PLACE_PROP_STRIPE_SET) {
            const struct meta_volume *v = meta_volume(sm, p->volume);
            if (!v) {
                sim_fail(s, "balance_plan targeted unknown vol %" PRIu32, p->volume);
                return;
            }
            const struct meta_member *tmb = meta_member(sm, p->node);
            if (!tmb || tmb->state != META_UP) {
                sim_fail(s, "balance_plan targeted non-UP node %" PRIu32 " on vol %" PRIu32, p->node, p->volume);
                return;
            }
            int ym = 0, smarg = 0;
            meta_vol_margins(sm, v, NULL, &ym, &smarg);
            if ((p->kind == PLACE_PROP_REPLICA_ADD && ym <= 0) || (p->kind == PLACE_PROP_STRIPE_SET && smarg <= 0)) {
                sim_fail(s, "balance_plan moved endangered vol %" PRIu32 " (ym=%d sm=%d)", p->volume, ym, smarg);
                return;
            }
            if (p->kind == PLACE_PROP_REPLICA_ADD && meta_replica(v, p->node)) {
                sim_fail(s, "balance_plan duplicate log replica node %" PRIu32 " on vol %" PRIu32, p->node, p->volume);
                return;
            }
            if (p->kind == PLACE_PROP_STRIPE_SET) {
                for (uint32_t k = 0; k < v->nstripe; k++) {
                    if (k != p->pos && v->stripe[k] == p->node) {
                        sim_fail(s, "balance_plan duplicate stripe server %" PRIu32 " on vol %" PRIu32, p->node,
                                 p->volume);
                        return;
                    }
                }
            }
            bal_moves++;
            if (p->volume >= 1 && p->volume <= SIM_MAX_VOLS)
                vol_moving[p->volume - 1]++;
        }
    }
    if (bal_moves > max_plan) {
        sim_fail(s, "balance_plan exceeded 25%% plan cap: %" PRIu32 " moves > cap %" PRIu32, bal_moves, max_plan);
        return;
    }
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->number < 1 || v->number > SIM_MAX_VOLS)
            continue;
        uint32_t grp = v->redundancy == META_RED_EC ? v->nstripe : v->nrep;
        uint32_t cap5 = grp / 20;
        if (cap5 < 1)
            cap5 = 1;
        if (vol_moving[v->number - 1] > cap5) {
            sim_fail(s, "balance_plan exceeded 5%% per-group concurrent cap on vol %" PRIu32 ": %" PRIu32 " > %" PRIu32,
                     v->number, vol_moving[v->number - 1], cap5);
            return;
        }
    }
}

/* ------------------------------------------------------------------ Checker: global state invariants (Rules 1, 4, 7) */

static void check_global_state(struct sim *s)
{
    const struct meta_state *sm = &s->sm;

    /* Rule 7: holds never exceed 24 h (META_HOLD_MAX_MS). */
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *m = &sm->members[i];
        if (m->hold_until_ms > 0) {
            if (m->hold_until_ms < m->hold_clock_ms || m->hold_until_ms - m->hold_clock_ms > META_HOLD_MAX_MS) {
                sim_fail(s, "member %" PRIu32 " hold exceeds 24h bound (until=%" PRIu64 " clock=%" PRIu64 ")", m->id,
                         m->hold_until_ms, m->hold_clock_ms);
                return;
            }
        }
    }

    /* Rule 4 & Rule 1: every volume list valid and every live EC object >= k readable when faults allow. */
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        uint32_t rnodes[META_REPLICAS_MAX];
        for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++) {
            rnodes[k] = v->rep[k].node;
            if (!meta_member(sm, rnodes[k])) {
                sim_fail(s, "vol %" PRIu32 " rep[%" PRIu32 "] references unknown member %" PRIu32, v->number, k,
                         rnodes[k]);
                return;
            }
        }
        if (chk_has_dup_u32(rnodes, v->nrep)) {
            sim_fail(s, "vol %" PRIu32 " has duplicate node in rep[]", v->number);
            return;
        }
        if (!meta_volume_in_isr(v, v->leader)) {
            sim_fail(s, "vol %" PRIu32 " leader %" PRIu32 " not in ISR", v->number, v->leader);
            return;
        }
        for (uint32_t j = 0; j < v->nisr; j++) {
            if (!meta_replica(v, v->isr[j])) {
                sim_fail(s, "vol %" PRIu32 " ISR member %" PRIu32 " not in rep[]", v->number, v->isr[j]);
                return;
            }
        }

        if (v->redundancy != META_RED_EC)
            continue;
        if (v->nstripe != (uint32_t)v->ec_k + v->ec_m) {
            sim_fail(s, "vol %" PRIu32 " nstripe=%" PRIu32 " != k+m=%u", v->number, v->nstripe, v->ec_k + v->ec_m);
            return;
        }
        if (chk_has_dup_u32(v->stripe, v->nstripe)) {
            sim_fail(s, "vol %" PRIu32 " has duplicate node in stripe[] (E9 violated)", v->number);
            return;
        }
        for (uint32_t p = 0; p < v->nstripe; p++) {
            if (!meta_member(sm, v->stripe[p])) {
                sim_fail(s, "vol %" PRIu32 " stripe[%" PRIu32 "] references unknown member %" PRIu32, v->number, p,
                         v->stripe[p]);
                return;
            }
        }

        /* Rule 1: check every OBJ_LIVE object's readable positions. */
        const struct sim_vol *sv = sim_vol_by_num(s, v->number);
        if (!sv)
            continue;
        for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
            const struct sim_obj *o = &sv->objs[oi];
            if (o->state != OBJ_LIVE)
                continue;
            uint32_t readable = 0;
            for (uint32_t p = 0; p < v->nstripe; p++) {
                uint32_t h = v->stripe[p];
                const struct sim_node *hn = sim_node_by_id_c(s, h);
                uint32_t hbit = 1u << (h - 1);
                int pos_readable = 0;
                if (hn && hn->alive && !hn->destroyed && (o->chunk_nodes[p] & hbit))
                    pos_readable = 1;
                if (!pos_readable && v->stripe_pos[p].state == META_STRIPE_FILLING) {
                    /* While FILLING, prev and any unreleased intermediate filling holder retain chunks until
                     * STRIPE_WHOLE + committed MK_GC release. */
                    for (uint32_t u = 1; u <= s->nnodes; u++) {
                        if (!(o->chunk_nodes[p] & (1u << (u - 1))))
                            continue;
                        const struct sim_node *un = sim_node_by_id_c(s, u);
                        if (un && un->alive && !un->destroyed) {
                            pos_readable = 1;
                            break;
                        }
                    }
                }
                if (pos_readable) {
                    readable++;
                    continue;
                }
                /* Position p is not currently readable: fail immediately if no scenario fault explains it. */
                int h_faulted = !hn || !hn->alive || hn->destroyed || o->disk_lost[p][h - 1];
                if (o->deg_unwritten[p]) {
                    /* Legitimate E10 degraded-settle unwritten position, awaiting reconcile. */
                } else if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
                    /* While FILLING, every remaining holder in chunk_nodes[p] is down/destroyed (otherwise
                     * pos_readable would be 1 above). If chunk_nodes[p] is empty, only a disk loss can explain it. */
                    if (o->chunk_nodes[p] == 0) {
                        int any_disk_loss = 0;
                        for (uint32_t u = 1; u <= s->nnodes; u++)
                            if (o->disk_lost[p][u - 1])
                                any_disk_loss = 1;
                        if (!any_disk_loss) {
                            sim_fail(s, "vol %" PRIu32 " oid %" PRIu64 " pos %" PRIu32
                                        " is FILLING with no remaining chunk copy and no disk loss",
                                     v->number, o->oid, p);
                            return;
                        }
                    }
                } else if (!h_faulted) {
                    sim_fail(s, "vol %" PRIu32 " oid %" PRIu64 " pos %" PRIu32 " holder %" PRIu32
                                " is WHOLE and unfaulted but missing chunk",
                             v->number, o->oid, p, h);
                    return;
                }
            }
            if (!sv->allow_below_k && readable < v->ec_k) {
                sim_fail(s, "vol %" PRIu32 " oid %" PRIu64 " below k=%u readable positions (readable=%" PRIu32
                            ", m=%u)",
                         v->number, o->oid, v->ec_k, readable, v->ec_m);
                return;
            }
        }
    }
}

/* ------------------------------------------------------------------ cluster setup per seed */

struct geom_spec {
    uint8_t redundancy;
    uint8_t k, m;
    uint32_t nrep;
    uint32_t min_nodes;
};

static const struct geom_spec k_geoms[] = {
    {META_RED_REPLICAS, 0, 0, 3, 4},
    {META_RED_EC, 4, 2, 3, 7},
    {META_RED_EC, 8, 2, 3, 11},
    {META_RED_EC, 8, 4, 5, 13},
    {META_RED_EC, 16, 4, 5, 21},
};

static void sim_add_member_meta(struct sim *s, uint32_t id, const char *rack)
{
    struct sim_node *n = &s->nodes[id - 1];
    memset(n, 0, sizeof *n);
    n->id = id;
    if (rack && rack[0])
        snprintf(n->rack, sizeof n->rack, "%s", rack);
    n->ndisks = 1 + (int)rng_u32(&s->r, SIM_MAX_DISKS);
    for (int d = 0; d < n->ndisks; d++) {
        n->disk_ok[d] = 1;
        n->disk_store_id[d] = ++s->next_store_id;
    }
    n->alive = 1;
    n->incarnation = 1;
    n->last_heard_ms = s->now_ms;

    struct meta_member mb = {
        .id = id,
        .state = META_UP,
        .incarnation = 1,
        .flags = META_MF_ALL,
    };
    snprintf(mb.addr, sizeof mb.addr, "127.0.0.1:%" PRIu32, 10000 + id);
    snprintf(mb.peer, sizeof mb.peer, "127.0.0.1:%" PRIu32, 20000 + id);
    if (n->rack[0])
        snprintf(mb.rack, sizeof mb.rack, "%s", n->rack);
    mb.ident[0] = (uint8_t)id;

    struct meta_entry e;
    meta_entry_init(&e);
    if (id == 1) {
        uint8_t uuid[16] = {0};
        for (int i = 0; i < 16; i++)
            uuid[i] = (uint8_t)(s->seed + (uint64_t)i + 1);
        struct meta_settings ms;
        meta_settings_default(&ms);
        ms.node_timeout_ms = 3000;
        ms.rebuild_after_s = s->pset.rebuild_after_s;
        ms.opening_hold_s = s->pset.opening_hold_s;
        ms.failstop_out_s = s->pset.failstop_out_s;
        ms.out_guard_pct = s->pset.out_guard_pct;
        ms.out_guard_hold_s = s->pset.out_guard_hold_s;
        ms.rebuild_concurrency = s->pset.rebuild_concurrency;
        ms.rebuild_max = 1 + rng_u32(&s->r, 2); /* 1..2 to exercise E13 queueing contention */
        ms.rebuild_mb_s = 200;
        meta_op_init(&e, uuid, &ms, &mb, META_MODE_REPLICATED, META_LEVEL);
    } else {
        meta_op_member_add(&e, &mb);
    }
    int res[4] = {0};
    sim_apply_entry(s, &e, res, 4);
    if (res[0] != META_OK)
        sim_fail(s, "init/add member %" PRIu32 " failed: %s", id, meta_strerror(res[0]));
}

static void sim_init_cluster(struct sim *s, uint64_t seed, int mutant, const char *mutant_name, int trace)
{
    memset(s, 0, sizeof *s);
    s->seed = seed;
    s->mutant = mutant;
    s->mutant_name = mutant_name;
    s->trace = trace;
    rng_init(&s->r, seed);
    meta_init(&s->sm);
    s->now_ms = 10000000ull;
    s->self_id = 1;
    s->next_rid = 100;
    s->next_store_id = 1000;
    s->next_oid = 1;

    place_settings_default(&s->pset);
    /* Scale timers so 500-900 ticks thoroughly exercise every timeout, hold, guard, and recovery path. */
    s->pset.rebuild_after_s = 25 + rng_u32(&s->r, 30);   /* 25..54 s */
    s->pset.failstop_out_s = 5 + rng_u32(&s->r, 6);      /* 5..10 s */
    s->pset.opening_hold_s = 20 + rng_u32(&s->r, 25);    /* 20..44 s */
    s->pset.out_guard_pct = 25;
    s->pset.out_guard_hold_s = 35 + rng_u32(&s->r, 45);  /* 35..79 s */
    s->pset.rebuild_concurrency = 1 + rng_u32(&s->r, 2); /* 1..2 for contention */

    /* Choose cluster geometry profile across seeds: rotate through all 5 geometries + mixed clusters. */
    uint32_t prof = (uint32_t)((seed - 1) % 6);
    uint32_t gidx = prof < 5 ? prof : (1 + rng_u32(&s->r, 2)); /* prof 5 = mixed replicas=3 + EC */
    const struct geom_spec *gs = &k_geoms[gidx];

    uint32_t extra = 1 + rng_u32(&s->r, 3); /* 1..3 extra spare servers for rebuild/replace */
    s->nnodes = gs->min_nodes + extra;
    if (s->nnodes > SIM_MAX_NODES - 4)
        s->nnodes = SIM_MAX_NODES - 4; /* leave room for node add ops */

    /* Racks: 90% multi-rack (3..6 racks, >=2 servers per rack so rack guard can trigger!), 10% unracked. */
    s->use_racks = (seed % 10 != 0);
    if (s->use_racks) {
        uint32_t max_racks = s->nnodes / 2;
        if (max_racks > 6)
            max_racks = 6;
        if (max_racks < 2)
            max_racks = 2;
        uint32_t min_racks = gs->m >= 4 ? 4 : 3;
        if (min_racks > max_racks)
            min_racks = max_racks;
        s->nracks = min_racks + rng_u32(&s->r, max_racks - min_racks + 1);
    } else {
        s->nracks = 0;
    }

    for (uint32_t id = 1; id <= s->nnodes; id++) {
        char rbuf[16] = "";
        if (s->use_racks)
            snprintf(rbuf, sizeof rbuf, "r%" PRIu32, (id - 1) % s->nracks);
        sim_add_member_meta(s, id, rbuf);
    }

    /* Create 4..7 volumes using the real place_volume / place_stripe! */
    uint32_t nv = 4 + rng_u32(&s->r, 4);
    if (nv > SIM_MAX_VOLS)
        nv = SIM_MAX_VOLS;

    for (uint32_t vi = 1; vi <= nv && !s->failed; vi++) {
        struct plan_snap snap;
        struct place_in in;
        build_place_in(s, &snap, &in);

        const struct geom_spec *vg = gs;
        if (prof == 5) {
            /* Mixed cluster: alternate replicas=3 and EC (4+2 or 8+2). */
            vg = (vi % 2 == 1) ? &k_geoms[0] : gs;
        }

        uint32_t reps[META_REPLICAS_MAX] = {0};
        if (place_volume(&in, vg->nrep, reps) != 0) {
            sim_fail(s, "initial place_volume failed for vol %" PRIu32, vi);
            break;
        }
        check_initial_placement(s, &in, reps, vg->nrep, 0);
        if (s->failed)
            break;

        uint32_t stripe[META_STRIPE_MAX] = {0}, qual = 0;
        if (vg->redundancy == META_RED_EC) {
            uint32_t ns = (uint32_t)vg->k + vg->m;
            if (place_stripe(&in, ns, stripe, &qual) != 0) {
                sim_fail(s, "initial place_stripe failed for vol %" PRIu32 " (qual=%" PRIu32 ")", vi, qual);
                break;
            }
            check_initial_placement(s, &in, stripe, ns, 1);
            if (s->failed)
                break;
        }

        struct meta_volume_add va;
        memset(&va, 0, sizeof va);
        va.number = vi;
        snprintf(va.name, sizeof va.name, "v%" PRIu32, vi);
        va.uuid[0] = (uint8_t)vi;
        va.uuid[15] = 0x5a;
        va.kind = META_VOL_REPLICATED;
        va.n = vg->nrep;
        for (uint32_t k = 0; k < vg->nrep; k++) {
            va.nodes[k] = reps[k];
            va.rid[k] = ++s->next_rid;
        }
        if (vg->redundancy == META_RED_EC) {
            va.redundancy = META_RED_EC;
            va.ec_k = vg->k;
            va.ec_m = vg->m;
            va.nstripe = (uint32_t)vg->k + vg->m;
            memcpy(va.stripe, stripe, va.nstripe * sizeof(uint32_t));
        }
        va.min_isr = meta_volume_min_isr(va.redundancy, va.ec_m, va.n, s->sm.set.min_isr);

        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_volume_add(&e, &va);
        int res[4] = {0};
        sim_apply_entry(s, &e, res, 4);
        if (res[0] != META_OK) {
            sim_fail(s, "initial VOLUME_ADD vol %" PRIu32 " failed: %s", vi, meta_strerror(res[0]));
            break;
        }

        struct sim_vol *sv = &s->vols[s->nvols++];
        memset(sv, 0, sizeof *sv);
        sv->number = vi;
        sv->redundancy = va.redundancy;
        sv->ec_k = va.ec_k;
        sv->ec_m = va.ec_m;
        sv->bytes_mb = 500 + rng_u32(&s->r, 4500); /* 500..5000 MB */
        sv->log_end = 10;

        /* Verify place_rack_survivable matches rack configuration when racks are configured and nracks >= m+1. */
        if (s->use_racks && s->mutant == PLACE_MUT_NONE) {
            const struct meta_volume *mv = meta_volume(&s->sm, vi);
            uint32_t eff_m = va.redundancy == META_RED_EC ? va.ec_m : (va.n - va.min_isr);
            if (s->nracks >= va.n && (va.redundancy != META_RED_EC || (va.nstripe + s->nracks - 1) / s->nracks <= eff_m)) {
                if (!place_rack_survivable(&s->sm, mv))
                    sim_fail(s, "vol %" PRIu32 " expected rack-survivable on %" PRIu32 " racks", vi, s->nracks);
            }
        }
    }
}

/* ------------------------------------------------------------------ volume leadership & ISR maintenance */

static int sim_promote_volume(struct sim *s, const struct meta_volume *v, uint32_t preferred_target, uint8_t reason)
{
    struct sim_vol *sv = sim_vol_by_num(s, v->number);
    if (!sv)
        return -1;
    struct meta_grant g;
    memset(&g, 0, sizeof g);
    g.number = v->number;
    g.epoch = v->epoch;
    g.new_epoch = v->epoch + 1;
    g.isr_version = v->isr_version;
    g.reason = reason;
    g.start = (v->nhist ? v->hist[v->nhist - 1].start : 0) + 1;
    g.nfenced = v->nisr;
    for (uint32_t i = 0; i < v->nisr; i++)
        g.fenced[i] = v->isr[i];
    if (preferred_target && reason == META_GR_PLANNED && meta_volume_in_isr(v, preferred_target) &&
        preferred_target != v->leader) {
        g.target = preferred_target;
        g.min_end = sv->log_end;
    }

    for (uint32_t i = 0; i < v->nisr; i++) {
        uint32_t u = v->isr[i];
        const struct sim_node *un = sim_node_by_id_c(s, u);
        if (!un || !un->alive || un->destroyed || !node_is_heard_fresh(s, un))
            continue;
        if (un->fault_vol == v->number)
            continue;
        const struct meta_replica *rep = meta_replica(v, u);
        if (!rep)
            continue;
        g.reply[g.nreplies++] = (struct meta_fence_reply){
            .node = u,
            .rid = rep->rid,
            .durable = sv->log_end,
            .last_epoch = v->epoch,
        };
    }
    if (g.nreplies == 0)
        return -1;
    if (meta_promote_pick(v, &g, &g.leader) != META_OK)
        return -1;

    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_leader(&e, &g);
    int res[4] = {0};
    sim_apply_entry(s, &e, res, 4);
    return res[0] == META_OK ? 0 : -1;
}

static void sim_step_volumes_isr(struct sim *s)
{
    for (uint32_t vi = 0; vi < s->sm.nvolumes && !s->failed; vi++) {
        const struct meta_volume *v = &s->sm.volumes[vi];
        struct sim_vol *sv = sim_vol_by_num(s, v->number);
        if (!sv)
            continue;

        /* 1. Fail over leader if current leader is down, faulted, or OUT/DESTROYED. */
        const struct sim_node *ln = sim_node_by_id_c(s, v->leader);
        const struct meta_member *lmb = meta_member(&s->sm, v->leader);
        int leader_bad = !ln || !ln->alive || ln->destroyed || !node_is_heard_fresh(s, ln) ||
                         ln->fault_vol == v->number || !lmb || lmb->state >= META_OUT;
        if (leader_bad) {
            (void)sim_promote_volume(s, v, 0, META_GR_UNHEARD);
            v = &s->sm.volumes[vi];
        }

        /* Re-fetch leader liveness after possible promotion. */
        ln = sim_node_by_id_c(s, v->leader);
        if (!ln || !ln->alive || ln->destroyed || !node_is_heard_fresh(s, ln))
            continue;
        if (v->nisr >= v->min_isr)
            sv->log_end++;

        /* 2. Shrink ISR for down/OUT/faulted non-leader ISR members. */
        uint32_t new_isr[META_REPLICAS_MAX], nnew = 0;
        uint64_t new_rids[META_REPLICAS_MAX];
        int changed = 0;
        for (uint32_t j = 0; j < v->nisr; j++) {
            uint32_t u = v->isr[j];
            const struct sim_node *un = sim_node_by_id_c(s, u);
            const struct meta_member *umb = meta_member(&s->sm, u);
            const struct meta_replica *rep = meta_replica(v, u);
            int keep = (u == v->leader) ||
                       (un && un->alive && !un->destroyed && node_is_heard_fresh(s, un) &&
                        un->fault_vol != v->number && umb && umb->state < META_OUT);
            if (keep && rep) {
                new_isr[nnew] = u;
                new_rids[nnew] = rep->rid;
                nnew++;
            } else {
                changed = 1;
            }
        }
        /* Also rejoin healthy non-learner replicas back into ISR (not ones being replaced). */
        for (uint32_t k = 0; k < v->nrep; k++) {
            uint32_t u = v->rep[k].node;
            if (v->rep[k].learner || meta_volume_in_isr(v, u))
                continue;
            int being_replaced = 0;
            for (uint32_t j = 0; j < v->nrep; j++)
                if (v->rep[j].replaces == u)
                    being_replaced = 1;
            if (being_replaced)
                continue;
            const struct sim_node *un = sim_node_by_id_c(s, u);
            const struct meta_member *umb = meta_member(&s->sm, u);
            if (un && un->alive && !un->destroyed && node_is_heard_fresh(s, un) && un->fault_vol != v->number &&
                umb && (umb->state == META_UP || umb->state == META_DRAINING) && nnew < META_REPLICAS_MAX) {
                new_isr[nnew] = u;
                new_rids[nnew] = v->rep[k].rid;
                nnew++;
                changed = 1;
            }
        }
        if (changed && nnew >= 1) {
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_isr_change(&e, v->number, v->epoch, v->isr_version, new_isr, new_rids, nnew, sv->log_end);
            int res[4] = {0};
            sim_apply_entry(s, &e, res, 4);
            v = &s->sm.volumes[vi];
        }

        /* 3. Advance active log-replica learner catch-up and complete move (R4 D10). */
        int learner_idx = -1;
        for (uint32_t k = 0; k < v->nrep; k++)
            if (v->rep[k].learner) {
                learner_idx = (int)k;
                break;
            }
        if (learner_idx >= 0) {
            uint32_t b = v->rep[learner_idx].node;
            const struct sim_node *bn = sim_node_by_id_c(s, b);
            if (bn && bn->alive && !bn->destroyed && node_is_heard_fresh(s, bn)) {
                int ym = 0, smarg = 0;
                meta_vol_margins(&s->sm, v, NULL, &ym, &smarg);
                uint32_t rate = pace_rate_for_margin(s, ym);
                if (sv->log_move_rem_mb > rate) {
                    sv->log_move_rem_mb -= rate;
                } else {
                    sv->log_move_rem_mb = 0;
                    /* Learner B joins ISR first (never lower in-sync copies!). */
                    uint32_t isr2[META_REPLICAS_MAX], n2 = 0;
                    uint64_t rids2[META_REPLICAS_MAX];
                    for (uint32_t j = 0; j < v->nisr; j++) {
                        const struct meta_replica *r = meta_replica(v, v->isr[j]);
                        if (r) {
                            isr2[n2] = v->isr[j];
                            rids2[n2] = r->rid;
                            n2++;
                        }
                    }
                    if (!meta_volume_in_isr(v, b) && n2 < META_REPLICAS_MAX) {
                        isr2[n2] = b;
                        rids2[n2] = v->rep[learner_idx].rid;
                        n2++;
                    }
                    struct meta_entry e;
                    meta_entry_init(&e);
                    meta_op_isr_change(&e, v->number, v->epoch, v->isr_version, isr2, rids2, n2, sv->log_end);
                    int res[4] = {0};
                    sim_apply_entry(s, &e, res, 4);
                    v = &s->sm.volumes[vi];
                }
            }
        }

        /* 4. If a replacement has joined ISR (learner == 0, replaces == A), transfer off A if leader, drop A from ISR,
         *    and REPLICA_REMOVE A to complete the move cleanly. */
        uint32_t rep_old = 0, rep_new = 0;
        for (uint32_t k = 0; k < v->nrep; k++) {
            if (!v->rep[k].learner && v->rep[k].replaces != 0) {
                rep_old = v->rep[k].replaces;
                rep_new = v->rep[k].node;
                break;
            }
        }
        if (rep_old && rep_new && meta_volume_in_isr(v, rep_new)) {
            if (v->leader == rep_old) {
                sim_promote_volume(s, v, rep_new, META_GR_PLANNED);
                v = &s->sm.volumes[vi];
            }
            if (v->leader != rep_old && meta_volume_in_isr(v, rep_old)) {
                uint32_t isr2[META_REPLICAS_MAX], n2 = 0;
                uint64_t rids2[META_REPLICAS_MAX];
                for (uint32_t j = 0; j < v->nisr; j++) {
                    if (v->isr[j] == rep_old)
                        continue;
                    const struct meta_replica *r = meta_replica(v, v->isr[j]);
                    if (r) {
                        isr2[n2] = v->isr[j];
                        rids2[n2] = r->rid;
                        n2++;
                    }
                }
                if (n2 >= v->min_isr) {
                    struct meta_entry e;
                    meta_entry_init(&e);
                    meta_op_isr_change(&e, v->number, v->epoch, v->isr_version, isr2, rids2, n2, sv->log_end);
                    int res[4] = {0};
                    sim_apply_entry(s, &e, res, 4);
                    v = &s->sm.volumes[vi];
                }
            }
            if (v->leader != rep_old && !meta_volume_in_isr(v, rep_old) && v->nrep > v->min_isr) {
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_replica_remove(&e, v->number, v->epoch, rep_old);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
                v = &s->sm.volumes[vi];
                if (res[0] == META_OK) {
                    s->cov.log_moves++;
                    /* Clear local fault on old node if it was replaced for that volume. */
                    struct sim_node *on = sim_node_by_id(s, rep_old);
                    if (on && on->fault_vol == v->number) {
                        on->fault_vol = 0;
                        on->fault_ticks = 0;
                    }
                }
            }
        }
    }
}

/* ------------------------------------------------------------------ settles (MK_COLD), fills & MK_GC releases */

static int sim_obj_slot_alloc(struct sim_vol *sv)
{
    for (uint32_t i = 0; i < sv->nobjs; i++)
        if (sv->objs[i].state == OBJ_EMPTY || sv->objs[i].state == OBJ_RETIRED)
            return (int)i;
    if (sv->nobjs < SIM_MAX_OBJS)
        return (int)(sv->nobjs++);
    return -1;
}

static uint32_t obj_readable_others(const struct sim *s, const struct meta_volume *v, const struct sim_obj *o,
                                    uint32_t skip_pos)
{
    uint32_t cnt = 0;
    for (uint32_t p = 0; p < v->nstripe; p++) {
        if (p == skip_pos)
            continue;
        uint32_t h = v->stripe[p];
        const struct sim_node *hn = sim_node_by_id_c(s, h);
        if (hn && hn->alive && !hn->destroyed && (o->chunk_nodes[p] & (1u << (h - 1)))) {
            cnt++;
            continue;
        }
        if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
            for (uint32_t u = 1; u <= s->nnodes; u++) {
                if (!(o->chunk_nodes[p] & (1u << (u - 1))))
                    continue;
                const struct sim_node *un = sim_node_by_id_c(s, u);
                if (un && un->alive && !un->destroyed) {
                    cnt++;
                    break;
                }
            }
        }
    }
    return cnt;
}

static void sim_step_ec_data(struct sim *s)
{
    for (uint32_t vi = 0; vi < s->sm.nvolumes && !s->failed; vi++) {
        const struct meta_volume *v = &s->sm.volumes[vi];
        if (v->redundancy != META_RED_EC)
            continue;
        struct sim_vol *sv = sim_vol_by_num(s, v->number);
        if (!sv)
            continue;

        const struct sim_node *ln = sim_node_by_id_c(s, v->leader);
        int leader_ok = ln && ln->alive && !ln->destroyed && node_is_heard_fresh(s, ln) && v->nisr >= v->min_isr;

        /* 1. Commit or abandon pending MK_COLD markers. */
        for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
            struct sim_obj *o = &sv->objs[oi];
            if (o->state != OBJ_PENDING || s->tick < o->commit_tick)
                continue;
            int all_written_up = leader_ok && o->epoch == v->epoch;
            for (uint32_t p = 0; p < v->nstripe && all_written_up; p++) {
                if (!(o->positions_mask & (1u << p)))
                    continue;
                uint32_t h = v->stripe[p];
                const struct sim_node *hn = sim_node_by_id_c(s, h);
                if (!hn || !hn->alive || hn->destroyed || !(o->chunk_nodes[p] & (1u << (h - 1))))
                    all_written_up = 0;
            }
            if (all_written_up) {
                o->state = OBJ_LIVE;
                s->cov.objs_settled++;
                uint32_t full_mask = (1u << v->nstripe) - 1;
                if (o->positions_mask != full_mask)
                    s->cov.deg_settled++;
            } else {
                /* Abandon uncommitted settle; commit orphan MK_GC once not pending, then delete chunks. */
                o->state = OBJ_RETIRED;
                o->mk_gc_committed = 1;
                for (uint32_t p = 0; p < v->nstripe; p++) {
                    for (uint32_t u = 1; u <= s->nnodes; u++)
                        if (o->chunk_nodes[p] & (1u << (u - 1)))
                            sim_delete_chunk(s, sv, oi, p, u);
                }
                o->state = OBJ_EMPTY;
                o->mk_gc_committed = 0;
            }
        }

        /* 2. Start new settles periodically (including E10 degraded settle after deg_wait_ticks >= 12). */
        uint32_t live_cnt = 0;
        for (uint32_t oi = 0; oi < sv->nobjs; oi++)
            if (sv->objs[oi].state == OBJ_LIVE || sv->objs[oi].state == OBJ_PENDING)
                live_cnt++;

        if (leader_ok && live_cnt < SIM_MAX_OBJS - 4 && (live_cnt < 3 || rng_pct(&s->r, 15))) {
            uint32_t up_mask = 0, nup = 0;
            for (uint32_t p = 0; p < v->nstripe; p++) {
                const struct sim_node *hn = sim_node_by_id_c(s, v->stripe[p]);
                if (hn && hn->alive && !hn->destroyed && node_healthy_disks(hn) > 0) {
                    up_mask |= (1u << p);
                    nup++;
                }
            }
            int do_settle = 0;
            if (nup == v->nstripe) {
                sv->deg_wait_ticks = 0;
                do_settle = 1;
            } else if (nup == v->nstripe - 1) {
                sv->deg_wait_ticks++;
                if (sv->deg_wait_ticks >= 10) {
                    sv->deg_wait_ticks = 0;
                    do_settle = 1; /* E10 degraded settle (k+m-1 positions) */
                }
            } else {
                sv->deg_wait_ticks = 0;
            }
            if (do_settle) {
                int slot = sim_obj_slot_alloc(sv);
                if (slot >= 0) {
                    struct sim_obj *o = &sv->objs[slot];
                    memset(o, 0, sizeof *o);
                    o->oid = ++s->next_oid;
                    o->stripe_version = v->stripe_version;
                    o->epoch = v->epoch;
                    o->state = OBJ_PENDING;
                    o->commit_tick = s->tick + 1 + rng_u32(&s->r, 2);
                    o->positions_mask = up_mask;
                    o->size_mb = 128 + rng_u32(&s->r, 384);
                    for (uint32_t p = 0; p < v->nstripe; p++) {
                        if (up_mask & (1u << p)) {
                            uint32_t h = v->stripe[p];
                            const struct sim_node *hn = sim_node_by_id_c(s, h);
                            int d = node_pick_disk(s, hn);
                            if (d >= 0) {
                                o->chunk_nodes[p] |= (1u << (h - 1));
                                o->chunk_disk[p][h - 1] = (uint8_t)d;
                            }
                        } else {
                            o->deg_unwritten[p] = 1;
                        }
                    }
                    /* Mutant deleteunmarked: occasionally delete a chunk of a pending MK_COLD marker! */
                    if (s->mutant == PLACE_MUT_DELETE_UNMARKED && rng_pct(&s->r, 20)) {
                        for (uint32_t p = 0; p < v->nstripe; p++) {
                            if (up_mask & (1u << p)) {
                                sim_delete_chunk(s, sv, (uint32_t)slot, p, v->stripe[p]);
                                break;
                            }
                        }
                    }
                }
            }
        }

        /* 3. Advance fills on FILLING positions and reconcile missing chunks on WHOLE positions. */
        int ym = 0, smarg = 0;
        meta_vol_margins(&s->sm, v, NULL, &ym, &smarg);
        uint32_t rate = pace_rate_for_margin(s, smarg);

        for (uint32_t p = 0; p < v->nstripe && !s->failed; p++) {
            uint32_t h = v->stripe[p];
            const struct sim_node *hn = sim_node_by_id_c(s, h);
            if (!hn || !hn->alive || hn->destroyed || node_healthy_disks(hn) == 0)
                continue;
            uint32_t hbit = 1u << (h - 1);

            if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
                /* Find next OBJ_LIVE object with stripe_version < v->stripe_version missing on holder h. */
                int target_oi = -1;
                for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                    struct sim_obj *o = &sv->objs[oi];
                    if (o->state != OBJ_LIVE || o->stripe_version >= v->stripe_version)
                        continue;
                    if (!(o->chunk_nodes[p] & hbit)) {
                        target_oi = (int)oi;
                        break;
                    }
                }
                if (target_oi >= 0) {
                    struct sim_obj *o = &sv->objs[target_oi];
                    /* Copy from prev if alive and holding chunk (1x), else decode from >= k other positions (k*x). */
                    int can_copy = 0;
                    if (v->stripe_pos[p].prev) {
                        uint32_t pr = v->stripe_pos[p].prev;
                        const struct sim_node *pn = sim_node_by_id_c(s, pr);
                        if (pn && pn->alive && !pn->destroyed && (o->chunk_nodes[p] & (1u << (pr - 1))))
                            can_copy = 1;
                    }
                    int can_decode = !can_copy && obj_readable_others(s, v, o, p) >= v->ec_k;
                    if (can_copy || can_decode) {
                        if (sv->fill_rem_mb[p] == 0)
                            sv->fill_rem_mb[p] = can_copy ? o->size_mb : o->size_mb * v->ec_k;
                        if (sv->fill_rem_mb[p] > rate) {
                            sv->fill_rem_mb[p] -= rate;
                        } else {
                            sv->fill_rem_mb[p] = 0;
                            int d = node_pick_disk(s, hn);
                            if (d >= 0) {
                                o->chunk_nodes[p] |= hbit;
                                o->chunk_disk[p][h - 1] = (uint8_t)d;
                                o->disk_lost[p][h - 1] = 0;
                                o->deg_unwritten[p] = 0;
                            }
                        }
                    }
                }
            } else {
                /* Reconcile degraded-settle or single-disk-loss missing chunks on WHOLE holder h. */
                int target_oi = -1;
                for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                    struct sim_obj *o = &sv->objs[oi];
                    if (o->state != OBJ_LIVE)
                        continue;
                    if (!(o->chunk_nodes[p] & hbit) && (o->deg_unwritten[p] || o->disk_lost[p][h - 1])) {
                        target_oi = (int)oi;
                        break;
                    }
                }
                if (target_oi >= 0) {
                    struct sim_obj *o = &sv->objs[target_oi];
                    if (obj_readable_others(s, v, o, p) >= v->ec_k) {
                        if (sv->reconcile_rem_mb[p] == 0)
                            sv->reconcile_rem_mb[p] = o->size_mb;
                        if (sv->reconcile_rem_mb[p] > rate) {
                            sv->reconcile_rem_mb[p] -= rate;
                        } else {
                            sv->reconcile_rem_mb[p] = 0;
                            int d = node_pick_disk(s, hn);
                            if (d >= 0) {
                                o->chunk_nodes[p] |= hbit;
                                o->chunk_disk[p][h - 1] = (uint8_t)d;
                                o->disk_lost[p][h - 1] = 0;
                                o->deg_unwritten[p] = 0;
                            }
                        }
                    }
                }
            }
        }

        /* 4. Execute committed MK_GC releases once replacement holder is META_STRIPE_WHOLE. */
        if (leader_ok && !v->gc_suspended) {
            for (uint32_t ri = 0; ri < sv->nrel && !s->failed; ri++) {
                struct sim_release *rel = &sv->rels[ri];
                if (rel->done)
                    continue;
                uint32_t p = rel->pos;
                int holder_whole = (v->stripe_pos[p].state == META_STRIPE_WHOLE);
                if (s->mutant == SIM_MUT_RELEASE_NOMARK) {
                    /* Hook mutant (r9-whole): attempt release before whole and without committing MK_GC. */
                    for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                        if (sv->objs[oi].state == OBJ_LIVE && sv->objs[oi].stripe_version < rel->release_ver)
                            sim_delete_chunk(s, sv, oi, p, rel->old_node);
                    }
                    rel->done = 1;
                    continue;
                }
                if (!holder_whole || v->stripe[p] == rel->old_node)
                    continue;
                /* Leader verifies current holder has every live object < release_ver before committing MK_GC. */
                uint32_t hbit = 1u << (v->stripe[p] - 1);
                int verified = 1;
                for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                    const struct sim_obj *o = &sv->objs[oi];
                    if (o->state == OBJ_LIVE && o->stripe_version < rel->release_ver && !o->deg_unwritten[p] &&
                        !(o->chunk_nodes[p] & hbit)) {
                        verified = 0;
                        break;
                    }
                }
                if (!verified)
                    continue;
                rel->mk_gc_committed = 1;
                for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                    struct sim_obj *o = &sv->objs[oi];
                    if (o->state == OBJ_LIVE && o->stripe_version < rel->release_ver)
                        sim_delete_chunk(s, sv, oi, p, rel->old_node);
                }
                rel->done = 1;
                s->cov.gc_releases++;
            }
        }

        /* 5. Occasionally retire oldest live object via committed MK_GC to keep object table fresh. */
        if (leader_ok && !v->gc_suspended && live_cnt > SIM_MAX_OBJS - 8) {
            for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                struct sim_obj *o = &sv->objs[oi];
                if (o->state != OBJ_LIVE)
                    continue;
                o->state = OBJ_RETIRED;
                o->mk_gc_committed = 1;
                for (uint32_t p = 0; p < v->nstripe; p++) {
                    for (uint32_t u = 1; u <= s->nnodes; u++)
                        if (o->chunk_nodes[p] & (1u << (u - 1)))
                            sim_delete_chunk(s, sv, oi, p, u);
                }
                o->state = OBJ_EMPTY;
                o->mk_gc_committed = 0;
                break;
            }
        }
    }
}

/* ------------------------------------------------------------------ proposal queue & delivery */

static void enqueue_prop(struct sim *s, const struct place_prop *p, uint8_t from_balance)
{
    /* Modelled proposal loss (12% drop rate; pure planner will re-propose on the next tick). */
    if (rng_pct(&s->r, 12)) {
        s->cov.props_lost++;
        return;
    }
    if (s->ninflight >= SIM_MAX_INFLIGHT) {
        s->cov.props_lost++;
        return;
    }
    uint32_t delay = 0;
    uint32_t roll = rng_u32(&s->r, 100);
    if (roll < 25)
        delay = 1;
    else if (roll < 38)
        delay = 2 + rng_u32(&s->r, 2); /* 2..3 ticks delay */
    if (delay > 0)
        s->cov.props_delayed++;

    s->inflight[s->ninflight++] = (struct sim_inflight){
        .prop = *p,
        .deliver_tick = s->tick + delay,
        .proposed_ms = s->now_ms,
        .from_balance = from_balance,
    };
}

static void record_release_obligation(struct sim *s, struct sim_vol *sv, uint32_t pos, uint32_t old_node,
                                      uint64_t new_ver)
{
    if (!old_node)
        return;
    if (s->mutant == PLACE_MUT_DELETE_UNMARKED) {
        /* Planted mutant: deletes old_node's chunks immediately on STRIPE_SET without committing MK_GC! */
        for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
            if (sv->objs[oi].state == OBJ_LIVE && sv->objs[oi].stripe_version < new_ver)
                sim_delete_chunk(s, sv, oi, pos, old_node);
        }
        return;
    }
    if (sv->nrel == SIM_MAX_RELEASES) {
        uint32_t w = 0;
        for (uint32_t i = 0; i < sv->nrel; i++) {
            if (sv->rels[i].done)
                continue;
            /* Also drop stale release obligations whose old_node holds no remaining chunks for pos. */
            uint32_t bit = 1u << (sv->rels[i].old_node - 1);
            int any = 0;
            for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                if (sv->objs[oi].state == OBJ_LIVE && sv->objs[oi].stripe_version < sv->rels[i].release_ver &&
                    (sv->objs[oi].chunk_nodes[sv->rels[i].pos] & bit)) {
                    any = 1;
                    break;
                }
            }
            if (any)
                sv->rels[w++] = sv->rels[i];
        }
        sv->nrel = w;
    }
    if (sv->nrel < SIM_MAX_RELEASES) {
        sv->rels[sv->nrel++] = (struct sim_release){
            .pos = pos,
            .old_node = old_node,
            .release_ver = new_ver,
            .done = 0,
        };
    } else {
        sim_trace(s, "WARN release table full on vol %" PRIu32 " (dropping pos=%" PRIu32 " old=%" PRIu32 ")",
                  sv->number, pos, old_node);
    }
}

static void deliver_inflight(struct sim *s)
{
    uint32_t keep = 0;
    for (uint32_t i = 0; i < s->ninflight && !s->failed; i++) {
        struct sim_inflight item = s->inflight[i];
        if (item.deliver_tick > s->tick) {
            s->inflight[keep++] = item;
            continue;
        }
        const struct place_prop *p = &item.prop;
        struct meta_entry e;
        meta_entry_init(&e);
        int emit = 0;

        if (p->kind == PLACE_PROP_MEMBER_STATE) {
            const struct meta_member *mb = meta_member(&s->sm, p->node);
            if (mb) {
                /* Drop stale pre-hold OUT proposal if an operator hold was applied while proposal was delayed. */
                if (p->state == META_OUT && mb->hold_until_ms > s->now_ms && s->mutant != PLACE_MUT_HOLD_IGNORED) {
                    meta_entry_free(&e);
                    continue;
                }
                uint64_t since = (p->state == META_DOWN) ? item.proposed_ms : 0;
                uint64_t clk = (p->state == META_DOWN) ? item.proposed_ms : 0;
                meta_op_member_state(&e, p->node, p->state, p->version, p->reason, since, clk);
                emit = 1;
            }
        } else if (p->kind == PLACE_PROP_REPLICA_ADD) {
            const struct meta_volume *v = meta_volume(&s->sm, p->volume);
            if (v && p->node != 0) {
                meta_op_replica_add(&e, p->volume, p->epoch, p->node, ++s->next_rid, p->other_node);
                emit = 1;
            }
        } else if (p->kind == PLACE_PROP_REPLICA_REMOVE) {
            meta_op_replica_remove(&e, p->volume, p->epoch, p->node);
            emit = 1;
        } else if (p->kind == PLACE_PROP_STRIPE_SET) {
            meta_op_stripe_set(&e, p->volume, p->pos, p->node, p->reason, p->other_node, p->version);
            emit = 1;
        } else if (p->kind == PLACE_PROP_STRIPE_WHOLE) {
            meta_op_stripe_whole(&e, p->volume, p->pos, p->version);
            emit = 1;
        } else if (p->kind == PLACE_PROP_VOLUME_TRANSFER) {
            const struct meta_volume *v = meta_volume(&s->sm, p->volume);
            if (v)
                (void)sim_promote_volume(s, v, p->node, META_GR_PLANNED);
            meta_entry_free(&e);
            continue;
        }

        if (!emit) {
            meta_entry_free(&e);
            continue;
        }

        uint32_t prev_holder_before = 0;
        if (p->kind == PLACE_PROP_STRIPE_SET) {
            const struct meta_volume *vb = meta_volume(&s->sm, p->volume);
            if (vb && p->pos < vb->nstripe)
                prev_holder_before = vb->stripe[p->pos];
        }

        int res[4] = {0};
        sim_apply_entry(s, &e, res, 4);
        if (res[0] == META_OK) {
            if (p->kind == PLACE_PROP_REPLICA_ADD) {
                struct sim_vol *sv = sim_vol_by_num(s, p->volume);
                if (sv)
                    sv->log_move_rem_mb = (uint32_t)(sv->bytes_mb / 4 + 200);
                sim_trace(s, "applied REPLICA_ADD vol=%" PRIu32 " node=%" PRIu32 " replaces=%" PRIu32 " margin=%d",
                          p->volume, p->node, p->other_node, p->margin);
            } else if (p->kind == PLACE_PROP_STRIPE_SET) {
                struct sim_vol *sv = sim_vol_by_num(s, p->volume);
                const struct meta_volume *v = meta_volume(&s->sm, p->volume);
                if (sv && v) {
                    sv->fill_rem_mb[p->pos] = 0;
                    record_release_obligation(s, sv, p->pos, p->other_node, v->stripe_version);
                    if (prev_holder_before && prev_holder_before != p->other_node && prev_holder_before != p->node)
                        record_release_obligation(s, sv, p->pos, prev_holder_before, v->stripe_version);
                }
                sim_trace(s, "applied STRIPE_SET vol=%" PRIu32 " pos=%" PRIu32 " node=%" PRIu32 " prev=%" PRIu32
                             " margin=%d",
                          p->volume, p->pos, p->node, p->other_node, p->margin);
            } else if (p->kind == PLACE_PROP_STRIPE_WHOLE) {
                s->cov.fills_done++;
                sim_trace(s, "applied STRIPE_WHOLE vol=%" PRIu32 " pos=%" PRIu32 " ver=%" PRIu64, p->volume, p->pos,
                          p->version);
            } else if (p->kind == PLACE_PROP_MEMBER_STATE) {
                sim_trace(s, "applied MEMBER_STATE node=%" PRIu32 " state=%s reason=%s", p->node,
                          meta_member_state_name(p->state), meta_down_reason_name(p->reason));
            }
        }
    }
    s->ninflight = keep;
}

/* ------------------------------------------------------------------ heartbeat & leader detection */

static void sim_step_heartbeats(struct sim *s)
{
    /* Ensure self_id (Raft leader) is alive; if it went down, elect another alive non-OUT member. */
    struct sim_node *self = sim_node_by_id(s, s->self_id);
    if (!self || !self->alive || self->destroyed) {
        for (uint32_t i = 0; i < s->nnodes; i++) {
            const struct meta_member *mb = meta_member(&s->sm, s->nodes[i].id);
            if (s->nodes[i].alive && !s->nodes[i].destroyed && !s->nodes[i].opening && mb && mb->state < META_OUT) {
                s->self_id = s->nodes[i].id;
                break;
            }
        }
    }

    for (uint32_t i = 0; i < s->nnodes && !s->failed; i++) {
        struct sim_node *n = &s->nodes[i];
        const struct meta_member *mb = meta_member(&s->sm, n->id);
        if (!mb)
            continue;

        if (n->alive && !n->destroyed) {
            n->last_heard_ms = s->now_ms;
            if (n->opening && s->tick >= n->opening_end_tick)
                n->opening = 0;
            if (n->fault_vol > 0)
                n->fault_ticks++;

            /* Member heard again and done opening -> transition DOWN or OUT back to UP (D2). */
            if (!n->opening && (mb->state == META_DOWN || mb->state == META_OUT)) {
                uint64_t inc = mb->state == META_OUT ? mb->incarnation + 1 : n->incarnation;
                if (inc < mb->incarnation)
                    inc = mb->incarnation;
                n->incarnation = inc;
                int was_out = (mb->state == META_OUT);
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_member_state(&e, n->id, META_UP, inc, META_DOWN_NONE, 0, 0);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
                if (res[0] == META_OK && was_out)
                    s->cov.returns_after_out++;
            }
        } else {
            /* Down node: clear failstop last-gasp beat once applied or once node_timeout_ms expires (D5: a lost last
             * beat falls back to a plain timeout DOWN). */
            if (n->failstop_beat &&
                ((mb->state == META_DOWN && mb->down_reason == META_DOWN_FAILSTOP) || !node_is_heard_fresh(s, n)))
                n->failstop_beat = 0;

            /* Raft leader marks unheard UP/DRAINING member DOWN after node_timeout_ms (unless failstop beat handles
             * it). */
            if (!n->failstop_beat && n->id != s->self_id && (mb->state == META_UP || mb->state == META_DRAINING) &&
                !node_is_heard_fresh(s, n)) {
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_member_state(&e, n->id, META_DOWN, mb->incarnation, META_DOWN_TIMEOUT, s->now_ms, s->now_ms);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
            }
        }
    }
}

/* ------------------------------------------------------------------ fault & operator event injector */

static uint32_t count_alive_nodes(const struct sim *s)
{
    uint32_t c = 0;
    for (uint32_t i = 0; i < s->nnodes; i++)
        if (s->nodes[i].alive && !s->nodes[i].destroyed)
            c++;
    return c;
}

static uint32_t obj_readable_if_down(const struct sim *s, const struct meta_volume *v, const struct sim_obj *o,
                                     uint32_t extra_down)
{
    uint32_t readable = 0;
    for (uint32_t p = 0; p < v->nstripe; p++) {
        uint32_t h = v->stripe[p];
        const struct sim_node *hn = sim_node_by_id_c(s, h);
        uint32_t hbit = 1u << (h - 1);
        int ok = 0;
        if (h != extra_down && hn && hn->alive && !hn->destroyed && (o->chunk_nodes[p] & hbit))
            ok = 1;
        if (!ok && v->stripe_pos[p].state == META_STRIPE_FILLING) {
            for (uint32_t u = 1; u <= s->nnodes; u++) {
                if (u == extra_down || !(o->chunk_nodes[p] & (1u << (u - 1))))
                    continue;
                const struct sim_node *un = sim_node_by_id_c(s, u);
                if (un && un->alive && !un->destroyed) {
                    ok = 1;
                    break;
                }
            }
        }
        if (ok)
            readable++;
    }
    return readable;
}

static int vol_any_obj_below_k(const struct sim *s, const struct meta_volume *v, uint32_t extra_down)
{
    const struct sim_vol *sv = sim_vol_by_num((struct sim *)s, v->number);
    if (!sv || v->redundancy != META_RED_EC)
        return 0;
    for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
        const struct sim_obj *o = &sv->objs[oi];
        if (o->state != OBJ_LIVE)
            continue;
        if (obj_readable_if_down(s, v, o, extra_down) < v->ec_k)
            return 1;
    }
    return 0;
}

static uint32_t count_down_in_vol_stripe(const struct sim *s, const struct meta_volume *v)
{
    uint32_t c = 0;
    const struct sim_vol *sv = sim_vol_by_num((struct sim *)s, v->number);
    for (uint32_t p = 0; p < v->nstripe; p++) {
        uint32_t h = v->stripe[p];
        const struct sim_node *n = sim_node_by_id_c(s, h);
        if (!n || !n->alive || n->destroyed || node_healthy_disks(n) == 0) {
            c++;
            continue;
        }
        if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
            uint32_t pr = v->stripe_pos[p].prev;
            const struct sim_node *pn = pr ? sim_node_by_id_c(s, pr) : NULL;
            if (!pn || !pn->alive || pn->destroyed || node_healthy_disks(pn) == 0) {
                c++;
                continue;
            }
            /* Also count FILLING pos if any live object lacks a readable chunk on any alive holder (e.g. disk wiped
             * on prev before holder h filled it). */
            if (sv) {
                int obj_miss = 0;
                for (uint32_t oi = 0; oi < sv->nobjs && !obj_miss; oi++) {
                    const struct sim_obj *o = &sv->objs[oi];
                    if (o->state != OBJ_LIVE)
                        continue;
                    int has_alive = 0;
                    for (uint32_t u = 1; u <= s->nnodes; u++) {
                        if (!(o->chunk_nodes[p] & (1u << (u - 1))))
                            continue;
                        const struct sim_node *un = sim_node_by_id_c(s, u);
                        if (un && un->alive && !un->destroyed) {
                            has_alive = 1;
                            break;
                        }
                    }
                    if (!has_alive)
                        obj_miss = 1;
                }
                if (obj_miss) {
                    c++;
                    continue;
                }
            }
        }
        /* Also count if any live object still has an unreconciled missing chunk at pos p. */
        if (sv) {
            uint32_t hbit = 1u << (h - 1);
            for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                const struct sim_obj *o = &sv->objs[oi];
                if (o->state == OBJ_LIVE && !(o->chunk_nodes[p] & hbit) &&
                    (o->deg_unwritten[p] || o->disk_lost[p][h - 1])) {
                    c++;
                    break;
                }
            }
        }
    }
    return c;
}

/* Returns 1 if taking `node_id` down would push any EC volume above m (or m+1 when `allow_mplus1`) down positions,
 * push any live object below k readable positions (when !allow_mplus1), or lose the last alive ISR member. */
static int taking_down_exceeds(const struct sim *s, uint32_t node_id, int allow_mplus1)
{
    if (node_id == s->self_id)
        return 1;
    for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
        const struct meta_volume *v = &s->sm.volumes[vi];
        /* Keep at least 1 alive ISR member so Raft/vol stays live. */
        if (meta_volume_in_isr(v, node_id)) {
            uint32_t alive_isr = 0;
            for (uint32_t j = 0; j < v->nisr; j++) {
                if (v->isr[j] == node_id)
                    continue;
                const struct sim_node *u = sim_node_by_id_c(s, v->isr[j]);
                if (u && u->alive && !u->destroyed)
                    alive_isr++;
            }
            if (alive_isr == 0)
                return 1;
        }
        if (v->redundancy == META_RED_EC) {
            if (!allow_mplus1 && vol_any_obj_below_k(s, v, node_id))
                return 1;
            int in_stripe = 0;
            for (uint32_t p = 0; p < v->nstripe; p++)
                if (v->stripe[p] == node_id ||
                    (v->stripe_pos[p].state == META_STRIPE_FILLING && v->stripe_pos[p].prev == node_id))
                    in_stripe = 1;
            if (in_stripe) {
                uint32_t cur_down = count_down_in_vol_stripe(s, v);
                uint32_t limit = allow_mplus1 ? (uint32_t)v->ec_m + 1 : (uint32_t)v->ec_m;
                if (cur_down + 1 > limit)
                    return 1;
            }
        }
    }
    return 0;
}

static void wipe_node_disk(struct sim *s, struct sim_node *n, int d)
{
    if (!n || d < 0 || d >= n->ndisks)
        return;
    n->disk_ok[d] = 0;
    n->disk_store_id[d] = ++s->next_store_id;
    uint32_t bit = 1u << (n->id - 1);
    for (uint32_t vi = 0; vi < s->nvols; vi++) {
        struct sim_vol *sv = &s->vols[vi];
        for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
            struct sim_obj *o = &sv->objs[oi];
            if (o->state == OBJ_EMPTY || o->state == OBJ_RETIRED)
                continue;
            for (uint32_t p = 0; p < META_STRIPE_MAX; p++) {
                if ((o->chunk_nodes[p] & bit) && o->chunk_disk[p][n->id - 1] == (uint8_t)d) {
                    o->chunk_nodes[p] &= ~bit;
                    o->disk_lost[p][n->id - 1] = 1;
                }
            }
        }
    }
}

static void wipe_node_all_disks(struct sim *s, struct sim_node *n)
{
    for (int d = 0; d < n->ndisks; d++)
        wipe_node_disk(s, n, d);
}

static void sim_step_events(struct sim *s, uint32_t total_ticks)
{
    /* 1. Scheduled auto-recoveries of transient faults. */
    for (uint32_t i = 0; i < s->nnodes; i++) {
        struct sim_node *n = &s->nodes[i];
        if (!n->alive && !n->destroyed && n->recover_tick > 0 && s->tick >= n->recover_tick) {
            n->alive = 1;
            n->incarnation++;
            n->recover_tick = 0;
            n->failstop_beat = 0;
            /* Repair any failed disks on restart (fresh empty store if lost). */
            for (int d = 0; d < n->ndisks; d++)
                if (!n->disk_ok[d])
                    n->disk_ok[d] = 1;
            /* 35% of recoveries enter `opening` state (sometimes past opening_hold_s to exercise openingunbounded). */
            if (rng_pct(&s->r, 35)) {
                n->opening = 1;
                uint32_t open_dur = rng_pct(&s->r, 40)
                                        ? (s->pset.rebuild_after_s + s->pset.opening_hold_s + 10)
                                        : (5 + rng_u32(&s->r, 20));
                n->opening_end_tick = s->tick + open_dur;
                s->cov.slow_opens++;
            }
            sim_trace(s, "node %" PRIu32 " recovered (opening=%d)", n->id, n->opening);
        }
    }

    /* Update allow_below_k flags once m+1 faults heal and every live object has >= k readable positions again. */
    for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
        const struct meta_volume *v = &s->sm.volumes[vi];
        struct sim_vol *sv = sim_vol_by_num(s, v->number);
        if (sv && sv->allow_below_k && v->redundancy == META_RED_EC) {
            if (count_down_in_vol_stripe(s, v) <= v->ec_m && !vol_any_obj_below_k(s, v, 0))
                sv->allow_below_k = 0;
        }
    }

    /* Clear finished drain or replace intents cleanly. */
    for (uint32_t i = 0; i < s->sm.nmembers && !s->failed; i++) {
        const struct meta_member *mb = &s->sm.members[i];
        if (mb->state == META_DRAINING || mb->replaced_by != 0) {
            int holds_any = 0;
            for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
                const struct meta_volume *v = &s->sm.volumes[vi];
                if (meta_replica(v, mb->id))
                    holds_any = 1;
                if (v->redundancy == META_RED_EC) {
                    for (uint32_t p = 0; p < v->nstripe; p++)
                        if (v->stripe[p] == mb->id)
                            holds_any = 1;
                }
            }
            if (!holds_any) {
                if (mb->replaced_by != 0) {
                    struct meta_entry e;
                    meta_entry_init(&e);
                    meta_op_node_replace(&e, mb->id, 0);
                    int res[4] = {0};
                    sim_apply_entry(s, &e, res, 4);
                    mb = &s->sm.members[i];
                }
                if (mb->state == META_DRAINING) {
                    struct meta_entry e;
                    meta_entry_init(&e);
                    meta_op_member_state(&e, mb->id, META_OUT, mb->incarnation, META_DOWN_NONE, 0, 0);
                    int res[4] = {0};
                    sim_apply_entry(s, &e, res, 4);
                }
            }
        }
    }

    /* Leave final 60 ticks quiet so in-flight rebuilds/settles can finish cleanly. */
    if (s->tick < 8 || s->tick + 60 >= total_ticks)
        return;

    /* Inject faults & operator ops probabilistically (~12% chance per tick). */
    if (!rng_pct(&s->r, 12))
        return;

    uint32_t ev = rng_u32(&s->r, 100);

    if (ev < 22) {
        /* Server down (timeout): transient or staying past rebuild_after_s. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        if (n && n->alive && !n->destroyed && count_alive_nodes(s) > 3 && !taking_down_exceeds(s, id, 0)) {
            n->alive = 0;
            n->failstop_beat = 0;
            uint32_t dur = rng_pct(&s->r, 45) ? (10 + rng_u32(&s->r, s->pset.rebuild_after_s))
                                              : (s->pset.rebuild_after_s + 20 + rng_u32(&s->r, 80));
            n->recover_tick = s->tick + dur;
            sim_trace(s, "FAULT server down id=%" PRIu32 " dur=%" PRIu32 "s", id, dur);
        }
    } else if (ev < 34) {
        /* Fail-stop fast path (D5). */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (n && n->alive && !n->destroyed && mb && mb->state == META_UP && count_alive_nodes(s) > 3 &&
            !taking_down_exceeds(s, id, 0)) {
            n->alive = 0;
            n->failstop_beat = 1;
            n->last_heard_ms = s->now_ms; /* last-gasp beat arrives fresh! */
            n->recover_tick = s->tick + s->pset.rebuild_after_s + 30;
            s->cov.failstops++;
            sim_trace(s, "FAULT fail-stop id=%" PRIu32, id);
        }
    } else if (ev < 44) {
        /* Per-replica LOCAL_FAULT on an UP server (D6). */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (n && n->alive && !n->destroyed && n->fault_vol == 0 && mb && mb->state == META_UP) {
            for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
                const struct meta_volume *v = &s->sm.volumes[vi];
                if (meta_replica(v, id) && !chk_vol_has_move(v) && v->nisr > v->min_isr) {
                    n->fault_vol = v->number;
                    n->fault_ticks = 1;
                    s->cov.repl_faults++;
                    sim_trace(s, "FAULT replica LOCAL_FAULT node=%" PRIu32 " vol=%" PRIu32, id, v->number);
                    break;
                }
            }
        }
    } else if (ev < 52) {
        /* Single-disk loss (or lost store when ndisks == 1). */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        if (n && n->alive && !n->destroyed && node_healthy_disks(n) > 0 && !taking_down_exceeds(s, id, 0)) {
            int d = node_pick_disk(s, n);
            if (d >= 0) {
                wipe_node_disk(s, n, d);
                s->cov.disk_losses++;
                /* Replace/remount empty disk after 10 ticks if all disks failed. */
                if (node_healthy_disks(n) == 0) {
                    n->alive = 0;
                    n->recover_tick = s->tick + s->pset.rebuild_after_s + 25;
                }
                sim_trace(s, "FAULT lost disk node=%" PRIu32 " disk=%d", id, d);
            }
        }
    } else if (ev < 62) {
        /* Rack down or >25% mass outage (triggers mass-OUT guard; stays past out_guard_hold_s half the time!). */
        int stay_past_bound = rng_pct(&s->r, 55);
        uint32_t dur = stay_past_bound ? (s->pset.out_guard_hold_s + s->pset.rebuild_after_s + 20)
                                       : (15 + rng_u32(&s->r, s->pset.out_guard_hold_s));
        if (s->use_racks && s->nracks >= 2 && rng_pct(&s->r, 60)) {
            uint32_t rk = rng_u32(&s->r, s->nracks);
            char rname[16];
            snprintf(rname, sizeof rname, "r%" PRIu32, rk);
            /* Pick a rack that does not hold self_id (or migrate self_id first). */
            int ok = 1;
            uint32_t targets[SIM_MAX_NODES], nt = 0;
            for (uint32_t i = 0; i < s->nnodes; i++) {
                if (!strcmp(s->nodes[i].rack, rname)) {
                    if (s->nodes[i].id == s->self_id)
                        ok = 0;
                    targets[nt++] = s->nodes[i].id;
                }
            }
            if (ok && nt >= 2 && count_alive_nodes(s) > nt + 2) {
                for (uint32_t t = 0; t < nt; t++) {
                    struct sim_node *n = sim_node_by_id(s, targets[t]);
                    if (n && n->alive && !n->destroyed) {
                        n->alive = 0;
                        n->recover_tick = s->tick + dur;
                    }
                }
                /* If the rack loss pushed any volume past m missing positions or any live object below k, mark
                 * allow_below_k while that outage lasts (unless mutant belowk suppresses it to prove Rule 1). */
                for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
                    const struct meta_volume *v = &s->sm.volumes[vi];
                    if (v->redundancy != META_RED_EC)
                        continue;
                    if (count_down_in_vol_stripe(s, v) > v->ec_m || vol_any_obj_below_k(s, v, 0)) {
                        struct sim_vol *sv = sim_vol_by_num(s, v->number);
                        if (sv && s->mutant != SIM_MUT_BELOW_K)
                            sv->allow_below_k = 1;
                    }
                }
                sim_trace(s, "FAULT rack down %s (%" PRIu32 " nodes, dur=%" PRIu32 "s)", rname, nt, dur);
            }
        } else {
            /* >25% mass outage across nodes. */
            uint32_t need = (s->nnodes * s->pset.out_guard_pct) / 100 + 1;
            if (need < 2)
                need = 2;
            uint32_t downed = 0;
            for (uint32_t i = 0; i < s->nnodes && downed < need; i++) {
                struct sim_node *n = &s->nodes[i];
                if (n->id != s->self_id && n->alive && !n->destroyed && count_alive_nodes(s) > 3 &&
                    !taking_down_exceeds(s, n->id, 0)) {
                    n->alive = 0;
                    n->recover_tick = s->tick + dur;
                    downed++;
                }
            }
            if (downed >= 2)
                sim_trace(s, "FAULT mass outage %" PRIu32 " nodes (dur=%" PRIu32 "s)", downed, dur);
        }
    } else if (ev < 70) {
        /* Simultaneous m or m+1 fault on one EC volume (exercises margin 0 priority and §5.8!). */
        int do_mplus1 = rng_pct(&s->r, 25);
        for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
            /* Prefer a higher-numbered volume so E13 margin-0 ordering differs from FIFO volume order! */
            uint32_t idx = s->sm.nvolumes - 1 - vi;
            const struct meta_volume *v = &s->sm.volumes[idx];
            if (v->redundancy != META_RED_EC)
                continue;
            struct sim_vol *sv = sim_vol_by_num(s, v->number);
            uint32_t want = do_mplus1 ? (uint32_t)v->ec_m + 1 : (uint32_t)v->ec_m;
            uint32_t cur = count_down_in_vol_stripe(s, v);
            if (cur >= want)
                continue;
            uint32_t dur = do_mplus1 ? 25 : (s->pset.rebuild_after_s + 30);
            uint32_t added = 0;
            for (uint32_t p = 0; p < v->nstripe && cur + added < want; p++) {
                uint32_t id = v->stripe[p];
                struct sim_node *n = sim_node_by_id(s, id);
                if (n && n->alive && !n->destroyed && id != s->self_id && count_alive_nodes(s) > 3 &&
                    !taking_down_exceeds(s, id, do_mplus1)) {
                    n->alive = 0;
                    n->recover_tick = s->tick + dur;
                    added++;
                }
            }
            if (added > 0) {
                if (do_mplus1 && (cur + added > v->ec_m || vol_any_obj_below_k(s, v, 0))) {
                    /* Mutant belowk omits allow_below_k to prove Rule 1 ("below k readable positions") fires. */
                    if (s->mutant != SIM_MUT_BELOW_K)
                        sv->allow_below_k = 1;
                    s->cov.mplus1_faults++;
                } else if (cur + added == v->ec_m) {
                    s->cov.m_faults++;
                }
                /* Also mark any other EC volume pushed below k by shared nodes in an intentional m+1 fault. */
                if (do_mplus1 && s->mutant != SIM_MUT_BELOW_K) {
                    for (uint32_t vi2 = 0; vi2 < s->sm.nvolumes; vi2++) {
                        const struct meta_volume *v2 = &s->sm.volumes[vi2];
                        if (v2->redundancy == META_RED_EC && vol_any_obj_below_k(s, v2, 0)) {
                            struct sim_vol *sv2 = sim_vol_by_num(s, v2->number);
                            if (sv2)
                                sv2->allow_below_k = 1;
                        }
                    }
                }
                sim_trace(s, "FAULT %s on vol %" PRIu32 " (down=%" PRIu32 ")", do_mplus1 ? "m+1" : "m", v->number,
                          cur + added);
            }
            break;
        }
    } else if (ev < 78) {
        /* Operator: node hold (plus occasional >24h hold refusal check). */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (mb && mb->state < META_OUT) {
            if (rng_pct(&s->r, 20)) {
                /* Verify meta_apply refuses hold > 24 h (META_HOLD_MAX_MS). */
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_node_hold(&e, id, s->now_ms + META_HOLD_MAX_MS + 1000ull, s->now_ms);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
                if (res[0] == META_OK)
                    sim_fail(s, "meta_apply accepted hold > 24h on member %" PRIu32, id);
            } else {
                uint64_t hold_s = (uint64_t)(s->pset.rebuild_after_s + 15 + rng_u32(&s->r, 60));
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_node_hold(&e, id, s->now_ms + hold_s * 1000ull, s->now_ms);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
                if (res[0] == META_OK) {
                    s->cov.holds_applied++;
                    sim_trace(s, "OP node hold id=%" PRIu32 " for=%" PRIu64 "s", id, hold_s);
                }
            }
        }
    } else if (ev < 84) {
        /* Operator: node drain. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        const struct meta_member *mb = meta_member(&s->sm, id);
        const struct sim_node *n = sim_node_by_id_c(s, id);
        if (mb && mb->state == META_UP && n && n->alive && id != s->self_id && count_alive_nodes(s) > 4) {
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_member_state(&e, id, META_DRAINING, mb->incarnation, META_DOWN_NONE, 0, 0);
            int res[4] = {0};
            sim_apply_entry(s, &e, res, 4);
            if (res[0] == META_OK) {
                s->cov.op_drain++;
                sim_trace(s, "OP node drain id=%" PRIu32, id);
            }
        }
    } else if (ev < 89) {
        /* Operator: node add (adds fresh server for replace / rebalancing). */
        if (s->nnodes < SIM_MAX_NODES) {
            uint32_t nid = s->nnodes + 1;
            s->nnodes = nid;
            char rbuf[16] = "";
            if (s->use_racks && s->nracks > 0)
                snprintf(rbuf, sizeof rbuf, "r%" PRIu32, (nid - 1) % s->nracks);
            sim_add_member_meta(s, nid, rbuf);
            s->cov.op_node_add++;
            sim_trace(s, "OP node add id=%" PRIu32 " rack=%s", nid, rbuf);
        }
    } else if (ev < 93) {
        /* Operator: node replace OLD NEW (find a fresh UP node holding none of OLD's volumes). */
        uint32_t old_id = 1 + rng_u32(&s->r, s->nnodes);
        const struct meta_member *omb = meta_member(&s->sm, old_id);
        if (omb && omb->state != META_DESTROYED && omb->replaced_by == 0 && old_id != s->self_id) {
            for (uint32_t nid = 1; nid <= s->nnodes; nid++) {
                if (nid == old_id)
                    continue;
                const struct meta_member *nmb = meta_member(&s->sm, nid);
                const struct sim_node *nn = sim_node_by_id_c(s, nid);
                if (!nmb || nmb->state != META_UP || !nn || !nn->alive || node_healthy_disks(nn) == 0)
                    continue;
                int overlap = 0, old_holds = 0;
                for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
                    const struct meta_volume *v = &s->sm.volumes[vi];
                    int oh = meta_replica(v, old_id) != NULL;
                    int nh = meta_replica(v, nid) != NULL;
                    if (v->redundancy == META_RED_EC) {
                        for (uint32_t p = 0; p < v->nstripe; p++) {
                            if (v->stripe[p] == old_id)
                                oh = 1;
                            if (v->stripe[p] == nid)
                                nh = 1;
                        }
                    }
                    if (oh)
                        old_holds = 1;
                    if (oh && nh) {
                        overlap = 1;
                        break;
                    }
                }
                if (old_holds && !overlap) {
                    if (omb->state < META_OUT) {
                        struct meta_entry eh;
                        meta_entry_init(&eh);
                        meta_op_node_hold(&eh, old_id, s->now_ms + 300000ull, s->now_ms);
                        int rh[4] = {0};
                        sim_apply_entry(s, &eh, rh, 4);
                    }
                    struct meta_entry e;
                    meta_entry_init(&e);
                    meta_op_node_replace(&e, old_id, nid);
                    int res[4] = {0};
                    sim_apply_entry(s, &e, res, 4);
                    if (res[0] == META_OK) {
                        s->cov.op_replace++;
                        sim_trace(s, "OP node replace %" PRIu32 " -> %" PRIu32, old_id, nid);
                    }
                    break;
                }
            }
        }
    } else if (ev < 97) {
        /* Operator: volume move --pos P --to B. */
        for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
            const struct meta_volume *v = &s->sm.volumes[vi];
            if (v->redundancy != META_RED_EC)
                continue;
            uint32_t p = rng_u32(&s->r, v->nstripe);
            if (v->stripe_pos[p].state != META_STRIPE_WHOLE)
                continue;
            uint32_t old_h = v->stripe[p];
            for (uint32_t nid = 1; nid <= s->nnodes; nid++) {
                const struct meta_member *mb = meta_member(&s->sm, nid);
                const struct sim_node *nn = sim_node_by_id_c(s, nid);
                if (!mb || mb->state != META_UP || !nn || !nn->alive || node_healthy_disks(nn) == 0)
                    continue;
                int in_sp = 0;
                for (uint32_t k = 0; k < v->nstripe; k++)
                    if (v->stripe[k] == nid)
                        in_sp = 1;
                if (in_sp)
                    continue;
                uint32_t vnum = v->number;
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_stripe_set(&e, vnum, p, nid, META_STRIPE_FILLING, old_h, v->stripe_version);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
                if (res[0] == META_OK) {
                    struct sim_vol *sv = sim_vol_by_num(s, vnum);
                    const struct meta_volume *v2 = meta_volume(&s->sm, vnum);
                    if (sv && v2) {
                        sv->fill_rem_mb[p] = 0;
                        record_release_obligation(s, sv, p, old_h, v2->stripe_version);
                    }
                    s->cov.op_move_pos++;
                    sim_trace(s, "OP volume move vol=%" PRIu32 " --pos %" PRIu32 " --to %" PRIu32, vnum, p, nid);
                }
                break;
            }
            break;
        }
    } else {
        /* Operator: node destroyed (on an OUT node; allowed while holding positions if every affected volume retains
         * >= k readable survivors and > 0 alive ISR members so the planner must rebuild from survivors). */
        for (uint32_t i = 0; i < s->sm.nmembers; i++) {
            const struct meta_member *mb = &s->sm.members[i];
            if (mb->state != META_OUT)
                continue;
            struct sim_node *n = sim_node_by_id(s, mb->id);
            if (!n || n->destroyed)
                continue;
            int safe = 1;
            for (uint32_t vi = 0; vi < s->sm.nvolumes && safe; vi++) {
                const struct meta_volume *v = &s->sm.volumes[vi];
                if (meta_volume_in_isr(v, mb->id)) {
                    uint32_t alive_isr = 0;
                    for (uint32_t j = 0; j < v->nisr; j++) {
                        if (v->isr[j] == mb->id)
                            continue;
                        const struct sim_node *u = sim_node_by_id_c(s, v->isr[j]);
                        if (u && u->alive && !u->destroyed)
                            alive_isr++;
                    }
                    if (alive_isr == 0)
                        safe = 0;
                }
                if (safe && v->redundancy == META_RED_EC) {
                    if (vol_any_obj_below_k(s, v, mb->id)) {
                        safe = 0;
                    } else {
                        int holds_stripe = 0;
                        for (uint32_t p = 0; p < v->nstripe; p++)
                            if (v->stripe[p] == mb->id ||
                                (v->stripe_pos[p].state == META_STRIPE_FILLING && v->stripe_pos[p].prev == mb->id))
                                holds_stripe = 1;
                        if (holds_stripe && count_down_in_vol_stripe(s, v) >= v->ec_m)
                            safe = 0;
                    }
                }
            }
            if (!safe)
                continue;
            wipe_node_all_disks(s, n);
            n->alive = 0;
            n->destroyed = 1;
            n->recover_tick = 0;
            uint32_t mid = mb->id;
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_member_state(&e, mid, META_DESTROYED, mb->incarnation, META_DOWN_NONE, 0, 0);
            int res[4] = {0};
            sim_apply_entry(s, &e, res, 4);
            if (res[0] == META_OK) {
                s->cov.op_destroyed++;
                sim_trace(s, "OP node destroyed id=%" PRIu32, mid);
            }
            break;
        }
    }
}

/* ------------------------------------------------------------------ single seed driver */

static int run_seed(uint64_t seed, uint32_t ticks, int mutant, const char *mutant_name, int trace,
                    struct sim_cov *total_cov)
{
    struct sim *s = calloc(1, sizeof *s);
    if (!s) {
        fprintf(stderr, "sim_place: out of memory\n");
        return 2;
    }
    sim_init_cluster(s, seed, mutant, mutant_name, trace);

    for (uint32_t t = 1; t <= ticks && !s->failed; t++) {
        s->tick = t;
        s->now_ms += 1000ull;

        /* 1. Inject faults / recoveries / operator ops. */
        sim_step_events(s, ticks);
        if (s->failed)
            break;

        /* 2. Update heartbeats, DOWN timeout detection, and OUT/DOWN -> UP return. */
        sim_step_heartbeats(s);
        if (s->failed)
            break;

        /* 3. Advance volume failovers, ISR maintenance, and log learner catch-up. */
        sim_step_volumes_isr(s);
        if (s->failed)
            break;

        /* 4. Advance EC settles (MK_COLD), stripe fills, reconcile, and MK_GC releases. */
        sim_step_ec_data(s);
        if (s->failed)
            break;

        /* 5. Feed real pout_plan -> place_plan -> balance_plan and check every output. */
        struct plan_snap snap;
        struct place_in in;
        build_place_in(s, &snap, &in);
        struct place_out out;
        place_out_init(&out);

        pout_plan(&in, &out);
        check_pout_output(s, &in, &out);

        if (!s->failed) {
            place_plan(&in, &out);
            check_place_output(s, &in, &out);
        }
        uint32_t place_end = out.nprops;

        if (!s->failed) {
            balance_plan(&in, &out);
            check_balance_output(s, &in, &out, place_end);
        }

        if (!s->failed) {
            for (uint32_t i = 0; i < out.nprops; i++)
                enqueue_prop(s, &out.props[i], (uint8_t)(i >= place_end));
        }
        place_out_free(&out);
        if (s->failed)
            break;

        /* 6. Deliver in-flight proposals through real meta_apply + meta_check_step. */
        deliver_inflight(s);
        if (s->failed)
            break;

        /* 7. Verify global invariants (durability >= k, valid lists, holds <= 24h). */
        check_global_state(s);
    }

    int rc = 0;
    if (s->failed) {
        if (mutant != PLACE_MUT_NONE) {
            printf("KILLED %s seed=%" PRIu64 " tick=%" PRIu32 ": %s\n", mutant_name ? mutant_name : "?", seed, s->tick,
                   s->fail_why);
        } else {
            fprintf(stderr, "FAIL seed=%" PRIu64 " tick=%" PRIu32 ": %s\n", seed, s->tick, s->fail_why);
        }
        rc = 1;
    } else if (total_cov) {
        total_cov->objs_settled += s->cov.objs_settled;
        total_cov->deg_settled += s->cov.deg_settled;
        total_cov->fills_done += s->cov.fills_done;
        total_cov->gc_releases += s->cov.gc_releases;
        total_cov->log_moves += s->cov.log_moves;
        total_cov->guards_active += s->cov.guards_active;
        total_cov->guards_passed += s->cov.guards_passed;
        total_cov->holds_applied += s->cov.holds_applied;
        total_cov->returns_after_out += s->cov.returns_after_out;
        total_cov->op_replace += s->cov.op_replace;
        total_cov->op_drain += s->cov.op_drain;
        total_cov->op_destroyed += s->cov.op_destroyed;
        total_cov->op_move_pos += s->cov.op_move_pos;
        total_cov->op_node_add += s->cov.op_node_add;
        total_cov->m_faults += s->cov.m_faults;
        total_cov->mplus1_faults += s->cov.mplus1_faults;
        total_cov->props_lost += s->cov.props_lost;
        total_cov->props_delayed += s->cov.props_delayed;
        total_cov->failstops += s->cov.failstops;
        total_cov->repl_faults += s->cov.repl_faults;
        total_cov->disk_losses += s->cov.disk_losses;
        total_cov->slow_opens += s->cov.slow_opens;
    }

    meta_free(&s->sm);
    free(s);
    return rc;
}

/* ------------------------------------------------------------------ MTTDL Monte Carlo (§5.3) */

struct mttdl_geom {
    const char *name;
    uint8_t redundancy;
    uint32_t k, m;
    uint32_t default_nodes;
};

static const struct mttdl_geom k_mttdl_geoms[] = {
    {"replicas=3", META_RED_REPLICAS, 1, 2, 4},
    {"4+2", META_RED_EC, 4, 2, 7},
    {"8+2", META_RED_EC, 8, 2, 11},
    {"8+4", META_RED_EC, 8, 4, 13},
    {"16+4", META_RED_EC, 16, 4, 21},
};

static const struct mttdl_geom *parse_mttdl_geom(const char *s)
{
    if (!s)
        return NULL;
    if (!strncmp(s, "ec=", 3))
        s += 3;
    for (size_t i = 0; i < sizeof k_mttdl_geoms / sizeof k_mttdl_geoms[0]; i++)
        if (!strcmp(s, k_mttdl_geoms[i].name))
            return &k_mttdl_geoms[i];
    return NULL;
}

/* Compute exact probability that one object in a (k+m)-wide stripe suffers > m missing positions, given per-position
 * failure probabilities p[0..S-1] (p[i] = 1.0 for a dead server, else dead_disks / disks_per_node). */
static double prob_obj_gt_m(const double *p, uint32_t S, uint32_t m)
{
    double dp[META_STRIPE_MAX + 1];
    memset(dp, 0, sizeof dp);
    dp[0] = 1.0;
    for (uint32_t i = 0; i < S; i++) {
        double pi = p[i];
        if (pi <= 0.0)
            continue;
        for (int j = (int)i + 1; j >= 1; j--)
            dp[j] = dp[j] * (1.0 - pi) + dp[j - 1] * pi;
        dp[0] *= (1.0 - pi);
    }
    double loss = 0.0;
    for (uint32_t j = m + 1; j <= S; j++)
        loss += dp[j];
    return loss;
}

struct mttdl_result {
    double closed_rebuild_h; /* §5.3 closed form: k*D / ((N-1)*r) in hours */
    double sim_rebuild_h;    /* mean single-node rebuild time with planner's E13 pace + rebuild_after_s, hours */
    double mttdl_years;      /* Monte Carlo MTTDL in years */
    double rel_se;           /* relative standard error of p_loss */
    uint64_t loss_samples;   /* trials that hit data loss under importance sampling */
};

/* Active rebuild job tracked during one degraded episode of the MTTDL Monte Carlo. */
struct mttdl_job {
    uint32_t node;      /* failed node id (1..N) */
    int is_server;      /* 1: whole server failure (T TB), 0: single disk failure (T/D TB) */
    double wait_rem_s;  /* remaining rebuild_after_s timer before OUT/rebuild starts */
    double bytes_rem_mb;/* remaining MB of missing chunks to rebuild */
    int margin;         /* current worst margin of volumes affected by this node over placed topology */
};

static double eval_cluster_loss(uint32_t nvols, const uint32_t vol_groups[][META_STRIPE_MAX], uint32_t S, uint32_t m,
                                const uint8_t *srv_down, const uint32_t *disk_down, uint32_t disks_per_node,
                                double objs_per_vol)
{
    double any_vol_loss = 0.0;
    for (uint32_t vi = 0; vi < nvols; vi++) {
        double pp[META_STRIPE_MAX];
        uint32_t touched = 0;
        for (uint32_t p = 0; p < S; p++) {
            uint32_t u = vol_groups[vi][p];
            if (srv_down[u - 1]) {
                pp[p] = 1.0;
                touched++;
            } else if (disk_down[u - 1] > 0) {
                double frac = (double)disk_down[u - 1] / (double)disks_per_node;
                pp[p] = fmin(1.0, frac);
                touched++;
            } else {
                pp[p] = 0.0;
            }
        }
        if (touched > m) {
            double pobj = prob_obj_gt_m(pp, S, m);
            if (pobj > 0.0) {
                double pv = (pobj >= 1.0) ? 1.0 : -expm1(objs_per_vol * log1p(-pobj));
                any_vol_loss = 1.0 - (1.0 - any_vol_loss) * (1.0 - pv);
            }
        }
    }
    return any_vol_loss;
}

static int mttdl_run_one(const struct mttdl_geom *g, uint32_t nodes, double disks_afr, double servers_afr,
                         uint32_t disks_per_node, double tb_per_node, double rate_mb_s, uint32_t trials, uint64_t seed,
                         struct mttdl_result *out)
{
    memset(out, 0, sizeof *out);
    const double SEC_PER_YEAR = 365.25 * 86400.0;
    const double REBUILD_AFTER_S = 1800.0; /* §5.3 / D4 default 30 min */
    const double LINE_RATE_MB_S = fmax(2500.0, rate_mb_s); /* 25 GbE line rate floor for margin 0 */

    /* §5.3 closed form: k * D / ((N - 1) * r), decimal TB (1e6 MB) and decimal MB/s. */
    double closed_s = ((double)g->k * (tb_per_node * 1e6)) / ((double)(nodes - 1) * rate_mb_s);
    out->closed_rebuild_h = closed_s / 3600.0;

    /* Build a cluster topology with real place_volume / place_stripe via meta_apply across V = 2*N volumes, then run
     * the renewal Monte Carlo over that topology with E13 margin-first ordering and E13 margin pacing (aggregate
     * cluster bandwidth focused on the most endangered job). */
    struct meta_state base_sm;
    meta_init(&base_sm);
    struct meta_settings ms;
    meta_settings_default(&ms);
    uint8_t uuid[16] = {9, 9, 9, 9};
    struct place_member pm[SIM_MAX_NODES];
    memset(pm, 0, sizeof pm);
    uint64_t lidx = 0;

    for (uint32_t id = 1; id <= nodes; id++) {
        struct meta_member mb = {
            .id = id,
            .state = META_UP,
            .incarnation = 1,
            .flags = META_MF_ALL,
        };
        snprintf(mb.addr, sizeof mb.addr, "127.0.0.1:%" PRIu32, 10000 + id);
        snprintf(mb.peer, sizeof mb.peer, "127.0.0.1:%" PRIu32, 20000 + id);
        mb.ident[0] = (uint8_t)id;
        struct meta_entry e;
        meta_entry_init(&e);
        if (id == 1)
            meta_op_init(&e, uuid, &ms, &mb, META_MODE_REPLICATED, META_LEVEL);
        else
            meta_op_member_add(&e, &mb);
        meta_entry_finish(&e);
        int res[2] = {0};
        int rc = meta_apply(&base_sm, ++lidx, 1, e.buf, e.len, res, 2);
        meta_entry_free(&e);
        if (rc != META_OK || res[0] != META_OK) {
            fprintf(stderr, "sim_place: mttdl member %" PRIu32 " init failed (rc=%d op=%d)\n", id, rc, res[0]);
            meta_free(&base_sm);
            return -1;
        }

        pm[id - 1].id = id;
        pm[id - 1].heard_age_ms = 50;
        pm[id - 1].chunk_stores = disks_per_node;
    }

    uint32_t nvols = nodes * 2;
    if (nvols > 42)
        nvols = 42;
    uint32_t S = g->k + g->m;
    uint32_t vol_groups[48][META_STRIPE_MAX];

    for (uint32_t vi = 0; vi < nvols; vi++) {
        struct place_in pin = {
            .sm = &base_sm,
            .members = pm,
            .nmembers = nodes,
            .now_ms = 100000,
            .mono_ms = 100000,
            .self_id = 1,
        };
        place_settings_default(&pin.set);
        uint32_t reps[META_REPLICAS_MAX] = {0};
        uint32_t stripe[META_STRIPE_MAX] = {0}, qual = 0;
        uint32_t nrep = g->m + 1;
        if (place_volume(&pin, nrep, reps) != 0) {
            fprintf(stderr, "sim_place: mttdl place_volume failed for %s (N=%" PRIu32 ")\n", g->name, nodes);
            meta_free(&base_sm);
            return -1;
        }
        if (g->redundancy == META_RED_EC && place_stripe(&pin, S, stripe, &qual) != 0) {
            fprintf(stderr, "sim_place: mttdl place_stripe failed for %s (N=%" PRIu32 " qual=%" PRIu32 ")\n", g->name,
                    nodes, qual);
            meta_free(&base_sm);
            return -1;
        }

        struct meta_volume_add va;
        memset(&va, 0, sizeof va);
        va.number = vi + 1;
        snprintf(va.name, sizeof va.name, "m%" PRIu32, vi + 1);
        va.uuid[0] = (uint8_t)(vi + 1);
        va.kind = META_VOL_REPLICATED;
        va.n = nrep;
        for (uint32_t j = 0; j < nrep; j++) {
            va.nodes[j] = reps[j];
            va.rid[j] = (uint64_t)(vi + 1) * 100 + j + 1;
        }
        if (g->redundancy == META_RED_EC) {
            va.redundancy = META_RED_EC;
            va.ec_k = (uint8_t)g->k;
            va.ec_m = (uint8_t)g->m;
            va.nstripe = S;
            memcpy(va.stripe, stripe, S * sizeof(uint32_t));
            memcpy(vol_groups[vi], stripe, S * sizeof(uint32_t));
        } else {
            memcpy(vol_groups[vi], reps, S * sizeof(uint32_t));
        }
        va.min_isr = meta_volume_min_isr(va.redundancy, va.ec_m, va.n, base_sm.set.min_isr);
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_volume_add(&e, &va);
        meta_entry_finish(&e);
        int res[2] = {0};
        int rc = meta_apply(&base_sm, ++lidx, 1, e.buf, e.len, res, 2);
        meta_entry_free(&e);
        if (rc != META_OK || res[0] != META_OK) {
            fprintf(stderr, "sim_place: mttdl VOLUME_ADD %" PRIu32 " failed (rc=%d op=%d)\n", vi + 1, rc, res[0]);
            meta_free(&base_sm);
            return -1;
        }
    }

    /* Hazard rates per second: lambda = -ln(1 - AFR) / SEC_PER_YEAR. */
    double lam_s = servers_afr > 0.0 ? (-log1p(-fmin(servers_afr, 0.999)) / SEC_PER_YEAR) : 0.0;
    double lam_d = disks_afr > 0.0 ? (-log1p(-fmin(disks_afr, 0.999)) / SEC_PER_YEAR) : 0.0;
    double lam_total = (double)nodes * (lam_s + (double)disks_per_node * lam_d);
    if (lam_total <= 0.0) {
        meta_free(&base_sm);
        out->mttdl_years = INFINITY;
        return 0;
    }

    /* Approximate number of 1 GB settled objects per volume for disk-overlap probability. */
    double total_cluster_gb = (double)nodes * tb_per_node * 1000.0;
    double obj_chunks_gb = 1.0 + (double)g->m / (double)g->k; /* 1 GB data part + parity */
    double objs_per_vol = total_cluster_gb / ((double)nvols * obj_chunks_gb);
    if (objs_per_vol < 1.0)
        objs_per_vol = 1.0;

    /* Single-node rebuild time under E13 pace (margin = m - 1):
     *   - m = 2 -> margin 1 -> 4 * rate_mb_s (capped at line rate)
     *   - m = 4 -> margin 3 -> rate_mb_s */
    double single_rate = (g->m == 2) ? fmin(LINE_RATE_MB_S, 4.0 * rate_mb_s) : rate_mb_s;
    out->sim_rebuild_h = (REBUILD_AFTER_S + ((double)g->k * tb_per_node * 1e6) / ((double)(nodes - 1) * single_rate)) /
                         3600.0;

    /* Importance-sampled renewal Monte Carlo:
     *   MTTDL = (1 / lam_total + E[T_ep]) / E[W * I_loss]
     * Each trial starts with one failure (drawn proportionally to server vs disk rate) and runs until either
     * all rebuilds finish (return to healthy) or a volume loses > m positions (data loss). */
    struct rng r;
    rng_init(&r, seed ^ ((uint64_t)g->k << 32) ^ ((uint64_t)g->m << 16) ^ (uint64_t)tb_per_node);

    double sum_w = 0.0, sum_w2 = 0.0, sum_ep_s = 0.0;
    uint64_t loss_hits = 0;

    for (uint32_t tr = 0; tr < trials; tr++) {
        uint8_t srv_down[SIM_MAX_NODES] = {0};
        uint32_t disk_down[SIM_MAX_NODES] = {0};
        struct mttdl_job jobs[64];
        uint32_t njobs = 0;

        /* Initial failure event at t = 0 of the degraded episode. */
        uint32_t n0 = 1 + rng_u32(&r, nodes);
        double p_srv0 = lam_s / (lam_s + (double)disks_per_node * lam_d);
        int is_srv0 = rng_unit(&r) < p_srv0;
        if (is_srv0) {
            srv_down[n0 - 1] = 1;
            jobs[njobs++] = (struct mttdl_job){
                .node = n0,
                .is_server = 1,
                .wait_rem_s = REBUILD_AFTER_S,
                .bytes_rem_mb = tb_per_node * 1e6,
                .margin = (int)g->m - 1,
            };
        } else {
            disk_down[n0 - 1] = 1;
            jobs[njobs++] = (struct mttdl_job){
                .node = n0,
                .is_server = 0,
                .wait_rem_s = REBUILD_AFTER_S,
                .bytes_rem_mb = (tb_per_node * 1e6) / (double)disks_per_node,
                .margin = (int)g->m - 1,
            };
        }

        double W = 1.0;
        double ep_s = 0.0;
        double prob_surviving = 1.0; /* tracks non-loss probability for partial disk overlaps */
        double p_state = 0.0;        /* loss probability of current failure set (prevents double-charging on timers) */
        double trial_w = 0.0;        /* total loss-weight contribution of this trial (for exact variance) */
        int new_failure = 1;

        for (uint32_t step = 0; step < 64 && njobs > 0; step++) {
            /* Update each job's worst margin using the real volume group topology (E13 ordering). */
            int cluster_min_margin = (int)g->m;
            for (uint32_t j = 0; j < njobs; j++) {
                int jmin = (int)g->m;
                for (uint32_t vi = 0; vi < nvols; vi++) {
                    int has_j = 0, miss = 0;
                    for (uint32_t p = 0; p < S; p++) {
                        uint32_t u = vol_groups[vi][p];
                        if (u == jobs[j].node)
                            has_j = 1;
                        if (srv_down[u - 1] || disk_down[u - 1] > 0)
                            miss++;
                    }
                    if (has_j) {
                        int marg = (int)g->m - miss;
                        if (marg < jmin)
                            jmin = marg;
                    }
                }
                jobs[j].margin = jmin;
                if (jmin < cluster_min_margin)
                    cluster_min_margin = jmin;
            }

            /* Check for incremental data loss only when a new failure changed the failure set (never re-charge on
             * timer expiry or rebuild completion). */
            if (new_failure) {
                double p_new =
                    eval_cluster_loss(nvols, vol_groups, S, g->m, srv_down, disk_down, disks_per_node, objs_per_vol);
                if (p_new > p_state) {
                    double delta = (p_state >= 1.0) ? 1.0 : fmin(1.0, (p_new - p_state) / (1.0 - p_state));
                    double contrib = W * prob_surviving * delta;
                    trial_w += contrib;
                    loss_hits++;
                    prob_surviving *= (1.0 - delta);
                    p_state = p_new;
                    if (prob_surviving < 1e-12)
                        break;
                }
            }
            new_failure = 0;

            /* E13: pick highest-priority job (lowest margin first; if waiting vs active, compute next event time). */
            uint32_t n_up = 0;
            for (uint32_t i = 0; i < nodes; i++)
                if (!srv_down[i])
                    n_up++;
            if (n_up < g->k) {
                /* Cannot rebuild while fewer than k servers are up; wait for another event or terminate. */
                break;
            }
            double agg_peers = (double)(n_up > 1 ? n_up - 1 : 1);

            /* Effective E13 rebuild rate (MB/s per server) based on the most endangered job running. */
            double eff_rate = rate_mb_s;
            if (cluster_min_margin <= 0)
                eff_rate = LINE_RATE_MB_S;
            else if (cluster_min_margin == 1 && g->m >= 2)
                eff_rate = fmin(LINE_RATE_MB_S, 4.0 * rate_mb_s);
            double net_mb_s = (agg_peers * eff_rate) / (double)g->k;

            /* Find earliest timer completion or active highest-priority job completion. */
            int best_active = -1;
            double dt_next = 1e30;
            int next_is_timer = 0;
            int next_idx = -1;

            for (uint32_t j = 0; j < njobs; j++) {
                if (jobs[j].wait_rem_s > 0.0) {
                    if (jobs[j].wait_rem_s < dt_next) {
                        dt_next = jobs[j].wait_rem_s;
                        next_is_timer = 1;
                        next_idx = (int)j;
                    }
                } else {
                    if (best_active < 0 || jobs[j].margin < jobs[best_active].margin)
                        best_active = (int)j;
                }
            }
            if (best_active >= 0) {
                double dt_fill = jobs[best_active].bytes_rem_mb / net_mb_s;
                if (dt_fill < dt_next) {
                    dt_next = dt_fill;
                    next_is_timer = 0;
                    next_idx = best_active;
                }
            }

            /* Remaining cluster hazard rate (healthy servers + healthy disks on live servers). */
            double lam_rem = 0.0;
            for (uint32_t i = 0; i < nodes; i++) {
                if (!srv_down[i]) {
                    lam_rem += lam_s;
                    uint32_t hdisks = disks_per_node > disk_down[i] ? disks_per_node - disk_down[i] : 0;
                    lam_rem += (double)hdisks * lam_d;
                }
            }
            if (lam_rem <= 0.0 || dt_next <= 0.0)
                break;

            /* True probability of another failure arriving before dt_next. */
            double q = -expm1(-lam_rem * dt_next);
            /* Importance sampling bias: force failure branch with prob p_bias when q is small so rare m=4 losses
             * are sampled accurately while keeping the estimator 100% mathematically unbiased. */
            double p_bias = (q < 0.5 && njobs < g->m + 2) ? 0.5 : q;
            int fail_happens = rng_unit(&r) < p_bias;

            if (fail_happens) {
                W *= (q / p_bias);
                double u = rng_unit(&r);
                double tau = (q > 1e-12) ? (-log1p(-u * q) / lam_rem) : (u * dt_next);
                if (tau > dt_next)
                    tau = dt_next;
                ep_s += tau;

                /* Advance waiting timers and the highest-priority active rebuild by tau. */
                for (uint32_t j = 0; j < njobs; j++) {
                    if (jobs[j].wait_rem_s > 0.0) {
                        jobs[j].wait_rem_s -= tau;
                        if (jobs[j].wait_rem_s < 0.0)
                            jobs[j].wait_rem_s = 0.0;
                    }
                }
                if (best_active >= 0) {
                    jobs[best_active].bytes_rem_mb -= tau * net_mb_s;
                    if (jobs[best_active].bytes_rem_mb < 0.0)
                        jobs[best_active].bytes_rem_mb = 0.0;
                }

                /* Sample which server or disk failed proportionally to its hazard rate (FP-rounding safe). */
                double draw = rng_unit(&r) * lam_rem;
                double acc = 0.0;
                int pick_node = -1, pick_srv = 0;
                for (uint32_t i = 0; i < nodes; i++) {
                    if (srv_down[i])
                        continue;
                    if (lam_s > 0.0) {
                        pick_node = (int)i;
                        pick_srv = 1;
                        acc += lam_s;
                        if (draw <= acc)
                            break;
                    }
                    uint32_t hdisks = disks_per_node > disk_down[i] ? disks_per_node - disk_down[i] : 0;
                    if (hdisks > 0 && lam_d > 0.0) {
                        pick_node = (int)i;
                        pick_srv = 0;
                        acc += (double)hdisks * lam_d;
                        if (draw <= acc)
                            break;
                    }
                }
                if (pick_node >= 0) {
                    if (njobs >= 64)
                        break;
                    if (pick_srv) {
                        srv_down[pick_node] = 1;
                        jobs[njobs++] = (struct mttdl_job){
                            .node = (uint32_t)pick_node + 1,
                            .is_server = 1,
                            .wait_rem_s = REBUILD_AFTER_S,
                            .bytes_rem_mb = tb_per_node * 1e6,
                            .margin = cluster_min_margin - 1,
                        };
                    } else {
                        disk_down[pick_node]++;
                        jobs[njobs++] = (struct mttdl_job){
                            .node = (uint32_t)pick_node + 1,
                            .is_server = 0,
                            .wait_rem_s = REBUILD_AFTER_S,
                            .bytes_rem_mb = (tb_per_node * 1e6) / (double)disks_per_node,
                            .margin = cluster_min_margin - 1,
                        };
                    }
                    new_failure = 1;
                }
            } else {
                if (p_bias < 1.0)
                    W *= ((1.0 - q) / (1.0 - p_bias));
                ep_s += dt_next;

                for (uint32_t j = 0; j < njobs; j++) {
                    if (jobs[j].wait_rem_s > 0.0) {
                        jobs[j].wait_rem_s -= dt_next;
                        if (jobs[j].wait_rem_s < 1e-9)
                            jobs[j].wait_rem_s = 0.0;
                    }
                }
                if (best_active >= 0) {
                    jobs[best_active].bytes_rem_mb -= dt_next * net_mb_s;
                    if (jobs[best_active].bytes_rem_mb < 1e-6)
                        jobs[best_active].bytes_rem_mb = 0.0;
                }

                if (!next_is_timer && next_idx >= 0) {
                    /* Active rebuild completed! Restore its server/disk redundancy and remove job. */
                    struct mttdl_job done = jobs[next_idx];
                    if (done.is_server)
                        srv_down[done.node - 1] = 0;
                    else if (disk_down[done.node - 1] > 0)
                        disk_down[done.node - 1]--;
                    memmove(&jobs[next_idx], &jobs[next_idx + 1], (njobs - (uint32_t)next_idx - 1) * sizeof jobs[0]);
                    njobs--;
                    p_state = eval_cluster_loss(nvols, vol_groups, S, g->m, srv_down, disk_down, disks_per_node,
                                                objs_per_vol);
                }
            }
        }
        sum_w += trial_w;
        sum_w2 += trial_w * trial_w;
        sum_ep_s += ep_s * W;
    }

    double p_loss = sum_w / (double)trials;
    double mean_ep_s = sum_ep_s / (double)trials;
    double cycle_yr = (1.0 / lam_total + mean_ep_s) / SEC_PER_YEAR;
    out->loss_samples = loss_hits;
    if (p_loss > 0.0) {
        out->mttdl_years = cycle_yr / p_loss;
        double var = (sum_w2 / (double)trials) - p_loss * p_loss;
        if (var < 0.0)
            var = 0.0;
        out->rel_se = sqrt(var / (double)trials) / p_loss;
    } else {
        out->mttdl_years = INFINITY;
        out->rel_se = 0.0;
    }
    meta_free(&base_sm);
    return 0;
}

static void fmt_hours(double h, char *buf, size_t n)
{
    if (h >= 48.0)
        snprintf(buf, n, "%.1f d", h / 24.0);
    else if (h >= 1.0)
        snprintf(buf, n, "%.1f h", h);
    else
        snprintf(buf, n, "%.0f m", h * 60.0);
}

static void fmt_years(double yr, char *buf, size_t n)
{
    if (!isfinite(yr))
        snprintf(buf, n, "inf");
    else if (yr >= 1e6)
        snprintf(buf, n, "%.2e yr", yr);
    else if (yr >= 100.0)
        snprintf(buf, n, "%.0f yr", yr);
    else
        snprintf(buf, n, "%.1f yr", yr);
}

static int run_mttdl(const char *geom_arg, double disks_afr, double servers_afr, uint32_t disks_per_node,
                     double tb_per_node, int tb_explicit, double rate_mb_s, uint32_t nodes_override, uint32_t trials,
                     uint64_t seed)
{
    int all = !strcmp(geom_arg, "all");
    const struct mttdl_geom *single = all ? NULL : parse_mttdl_geom(geom_arg);
    if (!all && !single) {
        fprintf(stderr, "sim_place: unknown --mttdl geometry '%s' (want replicas=3, 4+2, 8+2, 8+4, 16+4, or all)\n",
                geom_arg);
        return 2;
    }

    printf("MTTDL Monte Carlo (disks_afr=%.3g/yr x %u disks/node, servers_afr=%.3g/yr, rate=%.0f MB/s, trials=%u)\n",
           disks_afr, disks_per_node, servers_afr, rate_mb_s, trials);
    printf("| Mode | N | TB/node | Closed k*D/((N-1)*r) | Planner rebuild (E13) | Monte Carlo MTTDL | Rel SE |\n");
    printf("|---|---:|---:|---:|---:|---:|---:|\n");

    double tbs[2] = {10.0, 100.0};
    int ntbs = (all && !tb_explicit) ? 2 : 1;
    if (tb_explicit || !all)
        tbs[0] = tb_per_node;

    size_t ngs = all ? (sizeof k_mttdl_geoms / sizeof k_mttdl_geoms[0]) : 1;
    for (int ti = 0; ti < ntbs; ti++) {
        for (size_t gi = 0; gi < ngs; gi++) {
            const struct mttdl_geom *g = all ? &k_mttdl_geoms[gi] : single;
            uint32_t N = nodes_override ? nodes_override : g->default_nodes;
            if (N < g->k + g->m)
                N = g->k + g->m + 1;
            struct mttdl_result res;
            if (mttdl_run_one(g, N, disks_afr, servers_afr, disks_per_node, tbs[ti], rate_mb_s, trials, seed, &res) !=
                0)
                return 2;
            char cbuf[32], sbuf[32], ybuf[32];
            fmt_hours(res.closed_rebuild_h, cbuf, sizeof cbuf);
            fmt_hours(res.sim_rebuild_h, sbuf, sizeof sbuf);
            fmt_years(res.mttdl_years, ybuf, sizeof ybuf);
            printf("| %s | %u | %.0f TB | %s | %s | %s | %.1f%% |\n", g->name, N, tbs[ti], cbuf, sbuf, ybuf,
                   res.rel_se * 100.0);
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ main */

static int parse_sim_mutant(const char *name)
{
    if (!name || !*name)
        return PLACE_MUT_NONE;
    int m = place_mutant_parse(name);
    if (m >= 0)
        return m;
    if (!strcmp(name, "releasenomark"))
        return SIM_MUT_RELEASE_NOMARK;
    if (!strcmp(name, "belowk"))
        return SIM_MUT_BELOW_K;
    return -1;
}

static void usage(void)
{
    fprintf(stderr,
            "usage:\n"
            "  sim_place [--seeds N] [--first S] [--seed S] [--ticks T] [--mutant NAME] [--trace]\n"
            "  sim_place --mttdl GEOM|all [--mttdl-table] [--disks-afr P] [--servers-afr P] [--disks-per-node D]\n"
            "            [--tb-per-node T] [--rate MB/s] [--nodes N] [--trials N] [--seed S]\n");
}

int main(int argc, char **argv)
{
    uint64_t first = 1, seeds = 100;
    uint32_t ticks = 600;
    const char *mutant_name = NULL;
    int trace = 0;

    const char *mttdl_geom = NULL;
    double disks_afr = 0.02;
    double servers_afr = 0.01;
    uint32_t disks_per_node = 12;
    double tb_per_node = 10.0;
    int tb_explicit = 0;
    double rate_mb_s = 200.0;
    uint32_t mttdl_nodes = 0;
    uint32_t mttdl_trials = 20000;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--seeds") && i + 1 < argc) {
            seeds = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--first") && i + 1 < argc) {
            first = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--seed") && i + 1 < argc) {
            first = strtoull(argv[++i], NULL, 10);
            seeds = 1;
        } else if (!strcmp(argv[i], "--ticks") && i + 1 < argc) {
            ticks = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            mutant_name = argv[++i];
        } else if (!strcmp(argv[i], "--trace")) {
            trace = 1;
        } else if (!strcmp(argv[i], "--mttdl") && i + 1 < argc) {
            mttdl_geom = argv[++i];
        } else if (!strcmp(argv[i], "--mttdl-table")) {
            mttdl_geom = "all";
        } else if (!strcmp(argv[i], "--disks-afr") && i + 1 < argc) {
            disks_afr = strtod(argv[++i], NULL);
        } else if (!strcmp(argv[i], "--servers-afr") && i + 1 < argc) {
            servers_afr = strtod(argv[++i], NULL);
        } else if (!strcmp(argv[i], "--disks-per-node") && i + 1 < argc) {
            disks_per_node = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--tb-per-node") && i + 1 < argc) {
            tb_per_node = strtod(argv[++i], NULL);
            tb_explicit = 1;
        } else if (!strcmp(argv[i], "--rate") && i + 1 < argc) {
            rate_mb_s = strtod(argv[++i], NULL);
        } else if (!strcmp(argv[i], "--nodes") && i + 1 < argc) {
            mttdl_nodes = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--trials") && i + 1 < argc) {
            mttdl_trials = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
            usage();
            return 0;
        } else {
            fprintf(stderr, "sim_place: unknown option '%s'\n", argv[i]);
            usage();
            return 2;
        }
    }

    if (mttdl_geom) {
        if (disks_per_node == 0 || tb_per_node <= 0.0 || rate_mb_s <= 0.0 || mttdl_trials == 0 ||
            disks_afr < 0.0 || disks_afr >= 1.0 || servers_afr < 0.0 || servers_afr >= 1.0 ||
            mttdl_nodes > SIM_MAX_NODES) {
            fprintf(stderr, "sim_place: invalid MTTDL parameter\n");
            return 2;
        }
        return run_mttdl(mttdl_geom, disks_afr, servers_afr, disks_per_node, tb_per_node, tb_explicit, rate_mb_s,
                         mttdl_nodes, mttdl_trials, first);
    }

    if (ticks == 0) {
        fprintf(stderr, "sim_place: --ticks must be > 0\n");
        return 2;
    }

    if (!mutant_name) {
        const char *env = getenv("TSDB_PLACE_MUTANT");
        if (env && *env)
            mutant_name = env;
    }
    int mutant = parse_sim_mutant(mutant_name);
    if (mutant < 0) {
        fprintf(stderr, "sim_place: unknown mutant '%s'\n", mutant_name);
        return 2;
    }

    struct sim_cov cov;
    memset(&cov, 0, sizeof cov);
    for (uint64_t k = 0; k < seeds; k++) {
        uint64_t seed = first + k;
        int rc = run_seed(seed, ticks, mutant, mutant_name, trace, &cov);
        if (rc != 0)
            return rc;
    }

    if (mutant != PLACE_MUT_NONE) {
        printf("SURVIVED %s after %" PRIu64 " seeds (first=%" PRIu64 ")\n", mutant_name, seeds, first);
        return 0;
    }
    /* Coverage floor on multi-seed honest runs so silent regressions that stop exercising key paths fail loudly. */
    if (seeds >= 50 && ticks >= 400) {
        if (!cov.objs_settled || !cov.deg_settled || !cov.fills_done || !cov.gc_releases || !cov.log_moves ||
            !cov.guards_active || !cov.guards_passed || !cov.holds_applied || !cov.returns_after_out ||
            !cov.m_faults || !cov.op_destroyed || !cov.props_lost || !cov.props_delayed) {
            fprintf(stderr,
                    "sim_place: coverage floor failed over %" PRIu64 " seeds (settled=%" PRIu64 " deg=%" PRIu64
                    " fills=%" PRIu64 " gc=%" PRIu64 " logmoves=%" PRIu64 " guards=%" PRIu64 "/%" PRIu64
                    " holds=%" PRIu64 " returns=%" PRIu64 " m=%" PRIu64 " dst=%" PRIu64 " lost=%" PRIu64
                    " dly=%" PRIu64 ")\n",
                    seeds, cov.objs_settled, cov.deg_settled, cov.fills_done, cov.gc_releases, cov.log_moves,
                    cov.guards_active, cov.guards_passed, cov.holds_applied, cov.returns_after_out, cov.m_faults,
                    cov.op_destroyed, cov.props_lost, cov.props_delayed);
            return 1;
        }
    }
    printf("sim_place: %" PRIu64 " seeds ok (first=%" PRIu64 " ticks=%" PRIu32
           " settled=%" PRIu64 " deg=%" PRIu64 " fills=%" PRIu64 " gc=%" PRIu64 " logmoves=%" PRIu64
           " guards=%" PRIu64 "/%" PRIu64 " holds=%" PRIu64 " returns=%" PRIu64
           " ops[rep=%" PRIu64 " drn=%" PRIu64 " dst=%" PRIu64 " mov=%" PRIu64 " add=%" PRIu64 "]"
           " faults[m=%" PRIu64 " m1=%" PRIu64 " fs=%" PRIu64 " rf=%" PRIu64 " dk=%" PRIu64 " op=%" PRIu64 "]"
           " net[lost=%" PRIu64 " dly=%" PRIu64 "])\n",
           seeds, first, ticks, cov.objs_settled, cov.deg_settled, cov.fills_done, cov.gc_releases, cov.log_moves,
           cov.guards_active, cov.guards_passed, cov.holds_applied, cov.returns_after_out, cov.op_replace,
           cov.op_drain, cov.op_destroyed, cov.op_move_pos, cov.op_node_add, cov.m_faults, cov.mplus1_faults,
           cov.failstops, cov.repl_faults, cov.disk_losses, cov.slow_opens, cov.props_lost, cov.props_delayed);
    return 0;
}
