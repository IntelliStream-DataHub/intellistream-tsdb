/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Placement, failure-handling, disk/class, and MTTDL simulator (docs/design-cluster-data.md §5.3, §10.2, §15.9;
 * R9 plan D7, track r9-simplace; R13 track r13-sim).
 *
 * Drives the real pure planners (`pout_plan`, `place_plan`, `balance_plan`, `place_volume`, `place_stripe`,
 * `place_rack_survivable`) and applies their proposals through the real metadata state machine (`meta_apply`,
 * checked by `meta_check_step`) over seeded discrete-event clusters with:
 *   - N servers across racks, 1-4 chunk disks per server (default mode) or realistic R13 multi-disk/multi-class
 *     topologies (`--classes`: NVMe+HDD mixed, HDD-only storage/both, NVMe-only compute, SSD/NVMe all-flash shared
 *     disks with per-disk `class`, `uses`, `size_bytes`, `state`, and live `young_bytes`/`chunk_bytes` usage),
 *     mixed geometries: `replicas=3` and EC `4+2`, `8+2`, `8+4`, `16+4`;
 *   - Settled objects over time (`MK_COLD` pending -> committed, carrying `stripe_version`, including E10 degraded
 *     settles with k+m-1 positions) and `MK_GC` releases;
 *   - Faults & R13 disk/class events: server down/back, slow opening past `opening_hold_s`, single-disk and
 *     full-store loss, fail-stop last-gasp beats, per-replica `LOCAL_FAULT`, rack-down and >25% mass outages, `m` and
 *     `m+1` simultaneous losses; in `--classes`: `STORE_STATE` disk DOWN/OUT/recovery, `STORE_ADD`, `STORE_CLASS`,
 *     fast-disk fill past `young_move_pct` and chunk-store fill past §5.6 mark (then drain), HDD-only storage joins,
 *     and live `log_on_hdd` toggling via `META_OP_SETTINGS`;
 *   - Operator operations: `node hold` (including refusal of >24 h holds), `node replace`, `node drain`,
 *     `node destroyed`, `volume move --pos`, and `node add`;
 *   - Modelled proposal delay and loss, and margin-paced log catch-up and EC stripe fills;
 *   - Checker after every step verifying all placement, barrier, durability, GC, hold, guard, and R13 disk/class/fill
 *     invariants;
 *   - Planted mutants (`--mutant NAME` / `TSDB_PLACE_MUTANT`) verifying the checker catches every bug;
 *   - `--mttdl` Monte Carlo MTTDL over topologies built by real `place_volume`/`place_stripe` with E13 margin-first
 *     ordering and pacing beside §5.3's closed form k*D / ((N-1)*r).
 *
 * Usage:
 *   sim_place [--classes] [--seeds N] [--first S] [--seed S] [--ticks T] [--mutant NAME] [--trace]
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
#define SIM_MAX_DISKS 8
#define SIM_STD_DISKS 4
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

struct sim_disk {
    uint8_t store_uuid[16]; /* registered meta_disk.store_id */
    uint8_t disk_class;     /* enum meta_disk_class */
    uint8_t uses;           /* enum meta_disk_uses bitmask */
    uint8_t state;          /* enum meta_store_state */
    uint64_t size_bytes;
    uint64_t young_bytes;
    uint64_t chunk_bytes;
    uint64_t young_peak_7d;
    uint32_t out_tick;        /* tick when DOWN disk escalates to OUT (0: none) */
    uint32_t recover_tick;    /* tick when DOWN/OUT disk recovers to UP (0: none) */
    uint32_t fill_until_tick; /* tick when a simulated fill spike drains back (0: none) */
};

struct sim_node {
    uint32_t id;
    char rack[META_RACK_MAX];
    uint8_t class_mode;
    uint8_t role; /* enum meta_role (used in --classes mode) */
    int ndisks;
    uint8_t disk_ok[SIM_MAX_DISKS];
    uint64_t disk_store_id[SIM_MAX_DISKS];
    uint64_t expected_store_id[SIM_MAX_DISKS];
    struct sim_disk disks[SIM_MAX_DISKS];
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
    uint64_t log_start;
    uint64_t log_end;
    uint64_t rep_log_end[SIM_MAX_NODES];
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
    uint64_t stripe_returns;
    uint64_t log_cancels;
    uint64_t op_replace;
    uint64_t op_drain;
    uint64_t op_destroyed;
    uint64_t op_move_pos;
    uint64_t op_node_add;
    uint64_t bal_moves;
    uint64_t m_faults;
    uint64_t mplus1_faults;
    uint64_t props_lost;
    uint64_t props_delayed;
    uint64_t failstops;
    uint64_t repl_faults;
    uint64_t disk_losses;
    uint64_t slow_opens;
    /* R13 (--classes) coverage counters */
    uint64_t cls_disk_down;
    uint64_t cls_disk_out;
    uint64_t cls_disk_recover;
    uint64_t cls_disk_add;
    uint64_t cls_disk_class;
    uint64_t cls_fast_fill;
    uint64_t cls_chunk_fill;
    uint64_t cls_hdd_join;
    uint64_t cls_log_on_hdd_on;
    uint64_t cls_log_on_hdd_off;
    uint64_t cls_single_stripes;
    uint64_t cls_mixed_stripes;
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
    int use_classes;
    uint32_t ninflight;
    struct sim_inflight inflight[SIM_MAX_INFLIGHT];
    uint32_t nret_cands;
    struct place_return_cand ret_cands[SIM_MAX_VOLS * (META_STRIPE_MAX + 2)];
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
    for (int d = 0; d < n->ndisks; d++) {
        if (!n->disk_ok[d])
            continue;
        if (n->class_mode && (!(n->disks[d].uses & META_DISK_USE_CHUNK) || n->disks[d].state != META_STORE_UP))
            continue;
        c++;
    }
    return c;
}

static int node_pick_disk(struct sim *s, const struct sim_node *n)
{
    int ok[SIM_MAX_DISKS], cnt = 0;
    for (int d = 0; d < n->ndisks; d++) {
        if (!n->disk_ok[d])
            continue;
        if (n->class_mode && (!(n->disks[d].uses & META_DISK_USE_CHUNK) || n->disks[d].state != META_STORE_UP))
            continue;
        ok[cnt++] = d;
    }
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

static void sim_ret_cands_add(struct sim *s, uint32_t vol, uint32_t pos, uint32_t prev_node, uint64_t version,
                              uint8_t is_log)
{
    for (uint32_t i = 0; i < s->nret_cands; i++) {
        const struct place_return_cand *c = &s->ret_cands[i];
        if (c->is_log == is_log && c->volume == vol && c->prev_node == prev_node && c->version == version &&
            (is_log || c->pos == pos))
            return;
    }
    if (s->nret_cands < sizeof s->ret_cands / sizeof s->ret_cands[0]) {
        s->ret_cands[s->nret_cands++] = (struct place_return_cand){
            .volume = vol,
            .pos = pos,
            .prev_node = prev_node,
            .version = version,
            .is_log = is_log,
        };
    }
}

static void sim_ret_cands_update(struct sim *s, const struct meta_state *prev)
{
    if (!prev->initialised || !s->sm.initialised)
        return;
    for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
        const struct meta_volume *v = &s->sm.volumes[vi];
        const struct meta_volume *pv = meta_volume(prev, v->number);
        if (v->redundancy == META_RED_EC) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                if (v->stripe_pos[p].retired_ver != 0 || v->stripe_pos[p].state != META_STRIPE_FILLING)
                    continue;
                uint32_t pr = v->stripe_pos[p].prev;
                if (!pr || pr == v->stripe[p])
                    continue;
                if (pv && p < pv->nstripe && pv->stripe_pos[p].set_ver == v->stripe_pos[p].set_ver)
                    continue;
                const struct meta_member *pmb = meta_member(prev, pr);
                if (pmb && (pmb->state == META_OUT || pmb->state == META_DOWN) && pmb->replaced_by == 0)
                    sim_ret_cands_add(s, v->number, p, pr, v->stripe_pos[p].set_ver, 0);
            }
        }
        for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++) {
            if (!v->rep[k].learner || !v->rep[k].replaces)
                continue;
            int existed = 0;
            for (uint32_t j = 0; pv && j < pv->nrep; j++)
                if (pv->rep[j].rid == v->rep[k].rid)
                    existed = 1;
            if (!existed) {
                const struct meta_member *pmb = meta_member(prev, v->rep[k].replaces);
                if (pmb && (pmb->state == META_OUT || pmb->state == META_DOWN) && pmb->replaced_by == 0)
                    sim_ret_cands_add(s, v->number, 0, v->rep[k].replaces, v->rep[k].rid, 1);
            }
        }
    }
    uint32_t w = 0;
    for (uint32_t i = 0; i < s->nret_cands; i++) {
        struct place_return_cand c = s->ret_cands[i];
        const struct meta_volume *v = meta_volume(&s->sm, c.volume);
        int keep = 0;
        if (v && c.is_log) {
            for (uint32_t k = 0; k < v->nrep && k < META_REPLICAS_MAX; k++)
                if (v->rep[k].learner && v->rep[k].replaces == c.prev_node && v->rep[k].rid == c.version)
                    keep = 1;
        } else if (v && v->redundancy == META_RED_EC && c.pos < v->nstripe && c.pos < META_STRIPE_MAX) {
            if (v->stripe_pos[c.pos].retired_ver == 0 && v->stripe_pos[c.pos].prev == c.prev_node &&
                v->stripe[c.pos] != c.prev_node && v->stripe_pos[c.pos].set_ver == c.version)
                keep = 1;
        }
        if (keep)
            s->ret_cands[w++] = c;
    }
    s->nret_cands = w;
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
    sim_ret_cands_update(s, &prev);
    meta_free(&prev);
    return rc;
}

/* ------------------------------------------------------------------ build place_in snapshot */

struct plan_snap {
    struct place_member pm[SIM_MAX_NODES];
    uint32_t npm;
    struct place_disk_usage disk_usages[SIM_MAX_NODES][SIM_MAX_DISKS];
    struct place_fill fills[SIM_MAX_VOLS * META_STRIPE_MAX];
    uint32_t nfills;
    struct place_replace replaces[SIM_MAX_NODES];
    uint32_t nreplaces;
    struct place_vol_size vsizes[SIM_MAX_VOLS];
    uint32_t nvsizes;
    struct place_vol_log vlogs[SIM_MAX_VOLS * META_REPLICAS_MAX];
    uint32_t nvlogs;
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
        if (s->use_classes) {
            pm->disks = snap->disk_usages[i];
            pm->ndisks = (uint32_t)n->ndisks;
            for (int d = 0; d < n->ndisks; d++) {
                struct place_disk_usage *du = &snap->disk_usages[i][d];
                memcpy(du->store_id, n->disks[d].store_uuid, 16);
                du->disk_class = n->disks[d].disk_class;
                du->uses = n->disks[d].uses;
                du->state = n->disks[d].state;
                du->have_usage = 1;
                du->size_bytes = n->disks[d].size_bytes;
                du->young_bytes = n->disks[d].young_bytes;
                du->chunk_bytes = n->disks[d].chunk_bytes;
                du->young_peak_7d = n->disks[d].young_peak_7d;
                if ((n->disks[d].uses & META_DISK_USE_CHUNK) && n->disks[d].state == META_STORE_UP && n->disk_ok[d] &&
                    !n->destroyed) {
                    if (pm->nstore_ids < PLACE_STORES_MAX)
                        pm->store_ids[pm->nstore_ids++] = n->disk_store_id[d];
                    stores++;
                }
            }
        } else {
            for (int d = 0; d < n->ndisks; d++) {
                if (n->expected_store_id[d] && pm->expected_nstore_ids < PLACE_STORES_MAX)
                    pm->expected_store_ids[pm->expected_nstore_ids++] = n->expected_store_id[d];
                if (n->disk_ok[d] && !n->destroyed) {
                    if (pm->nstore_ids < PLACE_STORES_MAX)
                        pm->store_ids[pm->nstore_ids++] = n->disk_store_id[d];
                    stores++;
                }
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
        for (uint32_t k = 0; k < v->nrep && snap->nvlogs < SIM_MAX_VOLS * META_REPLICAS_MAX; k++) {
            uint32_t u = v->rep[k].node;
            const struct sim_node *un = sim_node_by_id_c(s, u);
            if (!un || !un->alive || un->destroyed || !node_is_heard_fresh(s, un))
                continue;
            snap->vlogs[snap->nvlogs++] = (struct place_vol_log){
                .node = u,
                .volume = v->number,
                .rid = v->rep[k].rid,
                .epoch = v->epoch,
                .log_start = sv->log_start ? sv->log_start : 1,
                .log_end = u >= 1 && u <= SIM_MAX_NODES ? sv->rep_log_end[u - 1] : 0,
                .is_leader = (uint8_t)(u == v->leader),
                .healthy = (uint8_t)(un->fault_vol != v->number),
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
    in->mutant = (s->mutant >= 0 && s->mutant < SIM_MUT_RELEASE_NOMARK) ? s->mutant : PLACE_MUT_NONE;
    in->replaces = snap->nreplaces ? snap->replaces : NULL;
    in->nreplaces = snap->nreplaces;
    in->vol_sizes = snap->nvsizes ? snap->vsizes : NULL;
    in->nvol_sizes = snap->nvsizes;
    in->vol_logs = snap->nvlogs ? snap->vlogs : NULL;
    in->nvol_logs = snap->nvlogs;
    in->return_cands = s->nret_cands ? s->ret_cands : NULL;
    in->nreturn_cands = s->nret_cands;
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

/* ------------------------------------------------------------------ independent R13 class & fill oracles */

enum chk_elig_err {
    CHK_ELIG_OK = 0,
    CHK_ELIG_ROLE = 1,
    CHK_ELIG_CAP = 2,
    CHK_ELIG_FULL = 3,
    CHK_ELIG_NO_STORE = 4,
};

static const struct place_member *chk_find_pm(const struct place_in *in, uint32_t id)
{
    if (!in || !in->members)
        return NULL;
    for (uint32_t i = 0; i < in->nmembers; i++)
        if (in->members[i].id == id)
            return &in->members[i];
    return NULL;
}

static const struct place_disk_usage *chk_find_du(const struct place_member *pm, const uint8_t store_id[16])
{
    if (!pm || !pm->disks || pm->ndisks == 0)
        return NULL;
    for (uint32_t i = 0; i < pm->ndisks; i++)
        if (memcmp(pm->disks[i].store_id, store_id, 16) == 0)
            return &pm->disks[i];
    return NULL;
}

/* Independent effective chunk disk class oracle (HDD > SSD > NVME across UP chunk disks, else non-DESTROYED). */
static uint8_t chk_chunk_class(const struct meta_member *mb)
{
    if (!mb || mb->ndisks == 0)
        return META_DISK_NONE;
    uint8_t cls = META_DISK_NONE;
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++) {
        const struct meta_disk *d = &mb->disks[i];
        if (d->state != META_STORE_UP || !(d->uses & META_DISK_USE_CHUNK))
            continue;
        if (d->disk_class > cls)
            cls = d->disk_class;
    }
    if (cls != META_DISK_NONE)
        return cls;
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++) {
        const struct meta_disk *d = &mb->disks[i];
        if (d->state == META_STORE_DESTROYED || !(d->uses & META_DISK_USE_CHUNK))
            continue;
        if (d->disk_class > cls)
            cls = d->disk_class;
    }
    return cls;
}

static uint64_t chk_vol_bytes(const struct place_in *in, uint32_t volume)
{
    if (!in || !in->vol_sizes)
        return 0;
    for (uint32_t i = 0; i < in->nvol_sizes; i++)
        if (in->vol_sizes[i].volume == volume)
            return in->vol_sizes[i].bytes;
    return 0;
}

static void chk_node_chunk_stats(const struct place_in *in, const struct meta_member *mb, uint64_t *cap_out,
                                 uint64_t *used_out, int *have_usage_out)
{
    uint64_t cap = 0, used = 0;
    uint32_t nup = 0, nusage = 0;
    const struct place_member *pm = chk_find_pm(in, mb->id);
    for (uint32_t d = 0; d < mb->ndisks && d < META_DISKS_MAX; d++) {
        const struct meta_disk *dk = &mb->disks[d];
        if (dk->state != META_STORE_UP || !(dk->uses & META_DISK_USE_CHUNK))
            continue;
        nup++;
        const struct place_disk_usage *du = chk_find_du(pm, dk->store_id);
        uint64_t sz = (du && du->size_bytes > 0) ? du->size_bytes : dk->size_bytes;
        cap = add_sat_u64(cap, sz);
        if (du && du->have_usage && sz > 0) {
            used = add_sat_u64(used, du->chunk_bytes);
            nusage++;
        }
    }
    *cap_out = cap;
    *used_out = used;
    *have_usage_out = (nup > 0 && nusage == nup && cap > 0) ? 1 : 0;
}

/* Independent §5.6 / E53 chunk store fill mark oracle: 95% * (1 - largest_domain_share), fallback 90%. */
static uint64_t chk_chunk_fill_mark_ppm(const struct place_in *in)
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
        uint64_t cap = 0, used = 0;
        int hu = 0;
        chk_node_chunk_stats(in, mb, &cap, &used, &hu);
        if (cap == 0)
            continue;
        nstorage++;
        total_cap = add_sat_u64(total_cap, cap);
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
                uint64_t cap = 0, used = 0;
                int hu = 0;
                chk_node_chunk_stats(in, mj, &cap, &used, &hu);
                dcap = add_sat_u64(dcap, cap);
            }
        } else {
            uint64_t used = 0;
            int hu = 0;
            chk_node_chunk_stats(in, mb, &dcap, &used, &hu);
        }
        if (dcap > max_dom)
            max_dom = dcap;
    }
    if (max_dom >= total_cap)
        return 900000ull;
    return (uint64_t)(((__uint128_t)950000ull * (total_cap - max_dom)) / total_cap);
}

/* Independent R13 log placement eligibility oracle (never consults in->mutant). */
static int chk_member_log_elig(const struct place_in *in, const struct meta_member *mb, uint64_t add_bytes,
                               int *fast_out, enum chk_elig_err *err_out)
{
    if (fast_out)
        *fast_out = 1;
    if (err_out)
        *err_out = CHK_ELIG_OK;
    if (!mb) {
        if (err_out)
            *err_out = CHK_ELIG_CAP;
        return 0;
    }
    int has_fast = (mb->ndisks == 0) || meta_member_has_fast_disk(mb);
    if (fast_out)
        *fast_out = has_fast;
    if (mb->role == META_ROLE_STORAGE) {
        if (err_out)
            *err_out = CHK_ELIG_ROLE;
        return 0;
    }
    if (mb->ndisks == 0)
        return 1;
    if (!(meta_member_effective_cap(mb) & META_CAP_COMPUTE)) {
        if (err_out)
            *err_out = CHK_ELIG_CAP;
        return 0;
    }
    uint32_t mark = in->set.young_move_pct ? in->set.young_move_pct
                                           : (in->sm->set.young_move_pct ? in->sm->set.young_move_pct : 80u);
    const struct place_member *pm = chk_find_pm(in, mb->id);
    uint32_t n_elig = 0, n_avail = 0;
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++) {
        const struct meta_disk *d = &mb->disks[i];
        if (d->state != META_STORE_UP)
            continue;
        if (has_fast) {
            if (!meta_disk_class_fast(d->disk_class))
                continue;
        } else if (in->sm->set.log_on_hdd) {
            if (d->disk_class != META_DISK_HDD)
                continue;
        } else {
            continue;
        }
        n_elig++;
        const struct place_disk_usage *du = chk_find_du(pm, d->store_id);
        if (!du || !du->have_usage || du->size_bytes == 0) {
            n_avail++;
            continue;
        }
        uint64_t denom = du->size_bytes;
        if ((d->uses & META_DISK_USE_CHUNK) && du->chunk_bytes > 0)
            denom = du->size_bytes > du->chunk_bytes ? du->size_bytes - du->chunk_bytes : 0;
        uint64_t proj = add_sat_u64(du->young_bytes, add_bytes);
        int full = (denom == 0) || ((__uint128_t)proj * 100u >= (__uint128_t)mark * denom);
        if (!full)
            n_avail++;
    }
    if (n_elig == 0) {
        if (err_out)
            *err_out = CHK_ELIG_CAP;
        return 0;
    }
    if (n_avail == 0) {
        if (err_out)
            *err_out = CHK_ELIG_FULL;
        return 0;
    }
    return 1;
}

/* Independent R13 EC stripe placement eligibility oracle (never consults in->mutant). */
static int chk_member_stripe_elig(const struct place_in *in, const struct meta_member *mb, int is_rebuild,
                                  uint64_t mark_ppm, enum chk_elig_err *err_out)
{
    if (err_out)
        *err_out = CHK_ELIG_OK;
    if (!mb) {
        if (err_out)
            *err_out = CHK_ELIG_CAP;
        return 0;
    }
    if (mb->role == META_ROLE_COMPUTE) {
        if (err_out)
            *err_out = CHK_ELIG_ROLE;
        return 0;
    }
    if (mb->ndisks > 0 && !(meta_member_effective_cap(mb) & META_CAP_STORAGE)) {
        if (err_out)
            *err_out = CHK_ELIG_CAP;
        return 0;
    }
    const struct place_member *pm = chk_find_pm(in, mb->id);
    if (!pm || pm->chunk_stores == 0) {
        if (err_out)
            *err_out = CHK_ELIG_NO_STORE;
        return 0;
    }
    if (mb->ndisks == 0)
        return 1;
    uint64_t eff_mark = is_rebuild ? 950000ull : mark_ppm;
    uint32_t n_elig = 0, n_avail = 0;
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++) {
        const struct meta_disk *d = &mb->disks[i];
        if (d->state != META_STORE_UP || !(d->uses & META_DISK_USE_CHUNK))
            continue;
        n_elig++;
        const struct place_disk_usage *du = chk_find_du(pm, d->store_id);
        if (!du || !du->have_usage || du->size_bytes == 0) {
            n_avail++;
            continue;
        }
        uint64_t cppm = (uint64_t)(((__uint128_t)du->chunk_bytes * 1000000ull) / du->size_bytes);
        if (cppm > 1000000ull)
            cppm = 1000000ull;
        int full = (cppm >= eff_mark);
        if (!full && !is_rebuild && (d->uses & META_DISK_USE_LOG)) {
            uint64_t reserve = du->size_bytes / 10u;
            uint64_t peak = du->young_peak_7d > du->young_bytes ? du->young_peak_7d : du->young_bytes;
            uint64_t y2 = peak > (UINT64_MAX / 2u) ? UINT64_MAX : peak * 2u;
            if (y2 > reserve)
                reserve = y2;
            if (du->size_bytes <= du->chunk_bytes || (du->size_bytes - du->chunk_bytes) < reserve)
                full = 1;
        }
        if (!full)
            n_avail++;
    }
    if (n_elig == 0) {
        if (err_out)
            *err_out = CHK_ELIG_NO_STORE;
        return 0;
    }
    if (n_avail == 0) {
        if (err_out)
            *err_out = CHK_ELIG_FULL;
        return 0;
    }
    return 1;
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
    uint64_t mark_ppm = s->use_classes ? chk_chunk_fill_mark_ppm(in) : 900000ull;

    /* R13 per-member capability, class and fill-mark checks on every chosen node. */
    if (s->use_classes) {
        uint32_t nfast_elig = 0;
        for (uint32_t i = 0; i < sm->nmembers; i++) {
            const struct meta_member *mb = &sm->members[i];
            if (mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
                continue;
            const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
            if (!sn || !node_is_heard_fresh(s, sn))
                continue;
            int is_fast = 0;
            if (!is_stripe && chk_member_log_elig(in, mb, 0, &is_fast, NULL) && is_fast)
                nfast_elig++;
        }
        for (uint32_t k = 0; k < n; k++) {
            const struct meta_member *mb = meta_member(sm, nodes[k]);
            enum chk_elig_err err = CHK_ELIG_OK;
            if (!is_stripe) {
                int is_fast = 0;
                if (!chk_member_log_elig(in, mb, 0, &is_fast, &err)) {
                    if (err == CHK_ELIG_FULL)
                        sim_fail(s, "initial log placement violated R13 young_move_pct fill mark on node %" PRIu32,
                                 nodes[k]);
                    else
                        sim_fail(s,
                                 "initial log placement violated R13 log capability/class on node %" PRIu32
                                 " (role=%s cap=%s log_on_hdd=%u)",
                                 nodes[k], mb ? meta_role_name(mb->role) : "?",
                                 mb ? meta_cap_name(mb->capability) : "?", sm->set.log_on_hdd);
                    return;
                }
                if (nfast_elig >= n && !is_fast) {
                    sim_fail(s,
                             "initial log placement picked HDD-only node %" PRIu32
                             " while %" PRIu32 " fast compute nodes qualified (E50 violated)",
                             nodes[k], nfast_elig);
                    return;
                }
            } else {
                if (!chk_member_stripe_elig(in, mb, 0, mark_ppm, &err)) {
                    if (err == CHK_ELIG_FULL)
                        sim_fail(s, "initial stripe placement violated R13 chunk fill mark on node %" PRIu32, nodes[k]);
                    else
                        sim_fail(s,
                                 "initial stripe placement violated disk use/capability on node %" PRIu32
                                 " (role=%s cap=%s)",
                                 nodes[k], mb ? meta_role_name(mb->role) : "?",
                                 mb ? meta_cap_name(mb->capability) : "?");
                    return;
                }
            }
        }
    }

    uint32_t cands[SIM_MAX_NODES], nc = 0;
    uint32_t fast_cands[SIM_MAX_NODES], nfc = 0;
    int all_racks = 1;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
            continue;
        const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
        if (!sn || !node_is_heard_fresh(s, sn))
            continue;
        if (s->use_classes) {
            if (!is_stripe) {
                int is_fast = 0;
                if (!chk_member_log_elig(in, mb, 0, &is_fast, NULL))
                    continue;
                if (is_fast)
                    fast_cands[nfc++] = i;
            } else {
                if (!chk_member_stripe_elig(in, mb, 0, mark_ppm, NULL))
                    continue;
            }
        } else {
            if (is_stripe && node_healthy_disks(sn) == 0)
                continue;
        }
        cands[nc++] = i;
        if (!mb->rack[0])
            all_racks = 0;
    }
    const uint32_t *rcands = cands;
    uint32_t nrc = nc;
    if (s->use_classes && !is_stripe && nfc >= n) {
        rcands = fast_cands;
        nrc = nfc;
        all_racks = 1;
        for (uint32_t i = 0; i < nrc; i++)
            if (!sm->members[rcands[i]].rack[0])
                all_racks = 0;
    }
    uint32_t opt = 0;
    if (all_racks && nrc >= n) {
        opt = optimal_max_per_rack(sm, rcands, nrc, n);
        uint32_t act = actual_max_per_rack(sm, nodes, n);
        if (act > opt) {
            sim_fail(s, "initial %s placement violated E9 rack cap: max_per_rack=%" PRIu32 " > optimal %" PRIu32,
                     is_stripe ? "stripe" : "log", act, opt);
            return;
        }
    }

    /* R13 E51 / §15.3: single disk class per stripe group whenever k + m eligible same-class members satisfy E9. */
    if (s->use_classes && is_stripe) {
        uint8_t first_cls = META_DISK_NONE;
        int mixed = 0;
        for (uint32_t k = 0; k < n; k++) {
            uint8_t cls = chk_chunk_class(meta_member(sm, nodes[k]));
            if (cls == META_DISK_NONE)
                continue;
            if (first_cls == META_DISK_NONE)
                first_cls = cls;
            else if (cls != first_cls)
                mixed = 1;
        }
        if (mixed) {
            s->cov.cls_mixed_stripes++;
            static const uint8_t cls_list[3] = {META_DISK_HDD, META_DISK_SSD, META_DISK_NVME};
            for (size_t ci = 0; ci < 3; ci++) {
                uint32_t cc[SIM_MAX_NODES], ncc = 0;
                for (uint32_t i = 0; i < nc; i++)
                    if (chk_chunk_class(&sm->members[cands[i]]) == cls_list[ci])
                        cc[ncc++] = cands[i];
                if (ncc < n)
                    continue;
                if (!all_racks || optimal_max_per_rack(sm, cc, ncc, n) <= opt) {
                    sim_fail(s,
                             "initial stripe placement mixed disk classes when single class %s (%" PRIu32
                             " eligible) satisfied E9 rack spread",
                             meta_disk_class_name(cls_list[ci]), ncc);
                    return;
                }
            }
        } else {
            s->cov.cls_single_stripes++;
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

        /* Rule 2b (r9-return): MOVE_CANCEL only for an unjoined learner when returning replica is UP & can catch up. */
        if (p->kind == PLACE_PROP_MOVE_CANCEL) {
            const struct meta_replica *rb = meta_replica(v, p->node);
            const struct meta_member *ma = meta_member(sm, p->other_node);
            if (!rb || !rb->learner || rb->replaces != p->other_node || meta_volume_in_isr(v, p->node)) {
                sim_fail(s, "MOVE_CANCEL invalid learner %" PRIu32 " on vol %" PRIu32, p->node, p->volume);
                return;
            }
            if (!ma || ma->state != META_UP) {
                sim_fail(s, "MOVE_CANCEL non-UP returning node %" PRIu32 " on vol %" PRIu32, p->other_node, p->volume);
                return;
            }
            continue;
        }

        /* Rule 3: E13 rebuild order by margin (independent margin check + non-decreasing margin + no starvation). */
        if ((p->kind == PLACE_PROP_REPLICA_ADD || p->kind == PLACE_PROP_STRIPE_SET) && !p->is_return) {
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

        /* Rule 4 & R13: valid list, one position per server, capability/fill/class, and E9 rack optimality for REPLICA_ADD. */
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
            uint64_t vbytes = chk_vol_bytes(in, p->volume);
            int target_fast = 1;
            if (s->use_classes) {
                enum chk_elig_err err = CHK_ELIG_OK;
                if (!chk_member_log_elig(in, tmb, vbytes, &target_fast, &err)) {
                    if (err == CHK_ELIG_FULL)
                        sim_fail(s,
                                 "REPLICA_ADD violated R13 young_move_pct fill mark on vol %" PRIu32
                                 ": target node %" PRIu32,
                                 p->volume, p->node);
                    else
                        sim_fail(s,
                                 "REPLICA_ADD violated R13 log capability/class on vol %" PRIu32
                                 ": target node %" PRIu32 " (role=%s cap=%s log_on_hdd=%u)",
                                 p->volume, p->node, meta_role_name(tmb->role), meta_cap_name(tmb->capability),
                                 sm->set.log_on_hdd);
                    return;
                }
                if (!forced && !target_fast) {
                    /* E50: HDD-only member may receive a log replica only when no fast compute node qualifies. */
                    for (uint32_t mi = 0; mi < nm; mi++) {
                        uint32_t used = urgent ? log_recv_urg[mi] : log_recv[mi];
                        if (used >= rebuild_max)
                            continue;
                        const struct meta_member *mb = &sm->members[mi];
                        if (!chk_member_placeable(s, in, mb, pend_gone, mi) || mb->id == p->other_node ||
                            meta_replica(v, mb->id))
                            continue;
                        int cf = 0;
                        if (chk_member_log_elig(in, mb, vbytes, &cf, NULL) && cf) {
                            sim_fail(s,
                                     "REPLICA_ADD placed log on HDD-only node %" PRIu32 " on vol %" PRIu32
                                     " while fast compute node %" PRIu32 " was available (E50 violated)",
                                     p->node, p->volume, mb->id);
                            return;
                        }
                    }
                }
            }
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
                    if (s->use_classes && !chk_member_log_elig(in, mb, vbytes, NULL, NULL))
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
                        if (s->use_classes) {
                            int cf = 0;
                            if (!chk_member_log_elig(in, mb, vbytes, &cf, NULL))
                                continue;
                            if (target_fast && !cf)
                                continue; /* fast nodes always beat HDD-only regardless of rack (E50) */
                        }
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

        /* Rule 4 & R13: valid list, one position per server, capability/fill/class, and E9 rack optimality for STRIPE_SET. */
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
            const struct meta_member *omb = meta_member(sm, old_holder);
            int is_drain = (!forced && !p->is_return && omb && omb->state == META_DRAINING);
            int is_rebuild = !is_drain;
            uint64_t mark_ppm = s->use_classes ? chk_chunk_fill_mark_ppm(in) : 900000ull;

            if (s->use_classes) {
                enum chk_elig_err err = CHK_ELIG_OK;
                int elig = chk_member_stripe_elig(in, tmb, is_rebuild, mark_ppm, &err);
                if (!elig) {
                    if (err == CHK_ELIG_FULL && !p->is_return) {
                        sim_fail(s,
                                 "STRIPE_SET violated R13 chunk fill mark on vol %" PRIu32 " pos %" PRIu32
                                 ": target node %" PRIu32 " (is_rebuild=%d)",
                                 p->volume, p->pos, p->node, is_rebuild);
                        return;
                    } else if (err != CHK_ELIG_FULL) {
                        sim_fail(s,
                                 "STRIPE_SET violated disk use/capability on vol %" PRIu32 " pos %" PRIu32
                                 ": target node %" PRIu32 " is not storage-capable (role=%s cap=%s)",
                                 p->volume, p->pos, p->node, meta_role_name(tmb->role),
                                 meta_cap_name(tmb->capability));
                        return;
                    }
                }
            }

            if (!forced && !p->is_return) {
                int racks_ok = 1;
                const char *rem_racks[META_STRIPE_MAX];
                uint32_t nrem = 0;
                uint8_t want_cls = META_DISK_NONE;
                int cls_mixed = 0;
                for (uint32_t k = 0; k < v->nstripe && k < META_STRIPE_MAX; k++) {
                    if (k == p->pos)
                        continue;
                    const struct meta_member *rm = meta_member(sm, v->stripe[k]);
                    if (!rm || !rm->rack[0]) {
                        racks_ok = 0;
                    } else {
                        rem_racks[nrem++] = rm->rack;
                    }
                    if (s->use_classes && rm) {
                        uint8_t rc = chk_chunk_class(rm);
                        if (rc != META_DISK_NONE) {
                            if (want_cls == META_DISK_NONE)
                                want_cls = rc;
                            else if (want_cls != rc)
                                cls_mixed = 1;
                        }
                    }
                }
                if (cls_mixed)
                    want_cls = META_DISK_NONE;
                for (uint32_t mi = 0; mi < nm && racks_ok; mi++) {
                    const struct meta_member *mb = &sm->members[mi];
                    const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
                    if (!chk_member_placeable(s, in, mb, pend_gone, mi) || !sn || node_healthy_disks(sn) == 0)
                        continue;
                    if (s->use_classes && !chk_member_stripe_elig(in, mb, is_rebuild, mark_ppm, NULL))
                        continue;
                    int has_sp = 0;
                    for (uint32_t k = 0; k < v->nstripe; k++)
                        if (v->stripe[k] == mb->id)
                            has_sp = 1;
                    if (!has_sp && !mb->rack[0])
                        racks_ok = 0;
                }
                uint32_t chosen_rc = 0;
                if (racks_ok) {
                    for (uint32_t j = 0; j < nrem; j++)
                        if (!strcmp(tmb->rack, rem_racks[j]))
                            chosen_rc++;
                }
                uint32_t min_rc = UINT32_MAX;
                int same_cls_avail = 0;
                uint32_t same_cls_node = 0;
                for (uint32_t mi = 0; mi < nm; mi++) {
                    uint32_t used = urgent ? stripe_recv_urg[mi] : stripe_recv[mi];
                    if (used >= stripe_cap)
                        continue;
                    const struct meta_member *mb = &sm->members[mi];
                    const struct sim_node *sn = sim_node_by_id_c(s, mb->id);
                    if (!chk_member_placeable(s, in, mb, pend_gone, mi) || mb->id == old_holder || !sn ||
                        node_healthy_disks(sn) == 0)
                        continue;
                    if (s->use_classes && !chk_member_stripe_elig(in, mb, is_rebuild, mark_ppm, NULL))
                        continue;
                    int has_sp = 0;
                    for (uint32_t k = 0; k < v->nstripe; k++)
                        if (v->stripe[k] == mb->id)
                            has_sp = 1;
                    if (has_sp)
                        continue;
                    uint32_t rc = 0;
                    if (racks_ok) {
                        for (uint32_t j = 0; j < nrem; j++)
                            if (!strcmp(mb->rack, rem_racks[j]))
                                rc++;
                        if (rc < min_rc)
                            min_rc = rc;
                    }
                    if (s->use_classes && want_cls != META_DISK_NONE && chk_chunk_class(mb) == want_cls &&
                        (!racks_ok || rc <= chosen_rc)) {
                        same_cls_avail = 1;
                        same_cls_node = mb->id;
                    }
                }
                if (racks_ok && min_rc < chosen_rc) {
                    sim_fail(s, "STRIPE_SET violated E9 rack rule on vol %" PRIu32 " pos %" PRIu32
                                ": picked node %" PRIu32 " (rack %s, count %" PRIu32 ") while count %" PRIu32
                                " was available",
                             p->volume, p->pos, p->node, tmb->rack, chosen_rc, min_rc);
                    return;
                }
                if (s->use_classes && want_cls != META_DISK_NONE && chk_chunk_class(tmb) != want_cls &&
                    same_cls_avail) {
                    sim_fail(s,
                             "STRIPE_SET mixed disk classes on vol %" PRIu32 " pos %" PRIu32 ": picked node %" PRIu32
                             " (%s) while same-class node %" PRIu32 " (%s) was available",
                             p->volume, p->pos, p->node, meta_disk_class_name(chk_chunk_class(tmb)), same_cls_node,
                             meta_disk_class_name(want_cls));
                    return;
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
    /* Balance must never run if pout_plan / place_plan emitted any state-drop or rebuild proposal this tick. */
    for (uint32_t i = 0; i < place_end_idx; i++) {
        enum place_prop_kind k = out->props[i].kind;
        if ((k == PLACE_PROP_MEMBER_STATE && out->props[i].state >= META_DOWN) || k == PLACE_PROP_REPLICA_ADD ||
            k == PLACE_PROP_REPLICA_REMOVE || k == PLACE_PROP_STRIPE_SET || k == PLACE_PROP_STRIPE_WHOLE ||
            k == PLACE_PROP_MOVE_CANCEL || k == PLACE_PROP_GC_RELEASE) {
            sim_fail(s, "balance_plan emitted moves while rebuild/state proposal kind=%d was active", (int)k);
            return;
        }
    }

    /* Total positions across all volumes (for 25% plan cap and 5% cluster-wide concurrent cap) and in-flight moves. */
    uint32_t total_pos = 0, inflight_bal = 0;
    uint32_t vol_moving[SIM_MAX_VOLS] = {0};
    uint32_t vol_bal_emitted[SIM_MAX_VOLS] = {0};
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        uint32_t mv_node = 0, base_nrep = 0;
        for (uint32_t k = 0; k < v->nrep; k++) {
            if (v->rep[k].replaces)
                mv_node = v->rep[k].node;
            else
                base_nrep++;
        }
        total_pos += base_nrep + (v->redundancy == META_RED_EC ? v->nstripe : 0);
        if (mv_node) {
            inflight_bal++;
            if (v->number >= 1 && v->number <= SIM_MAX_VOLS)
                vol_moving[v->number - 1]++;
        }
        if (v->redundancy == META_RED_EC) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
                    inflight_bal++;
                    if (v->number >= 1 && v->number <= SIM_MAX_VOLS)
                        vol_moving[v->number - 1]++;
                }
            }
        }
    }
    uint32_t max_plan = total_pos / 4;
    uint32_t max_conc = total_pos / 20;
    if (max_conc < 1)
        max_conc = 1;

    uint32_t bal_moves = 0;
    uint64_t bal_dst_added[SIM_MAX_NODES] = {0};
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
            if (s->use_classes) {
                const struct meta_member *smb = p->other_node ? meta_member(sm, p->other_node) : NULL;
                if (p->kind == PLACE_PROP_REPLICA_ADD) {
                    uint64_t vbytes = chk_vol_bytes(in, p->volume);
                    int dst_fast = 1;
                    enum chk_elig_err err = CHK_ELIG_OK;
                    if (!chk_member_log_elig(in, tmb, vbytes, &dst_fast, &err)) {
                        if (err == CHK_ELIG_FULL)
                            sim_fail(s,
                                     "balance_plan violated R13 young_move_pct fill mark on vol %" PRIu32
                                     " target %" PRIu32,
                                     p->volume, p->node);
                        else
                            sim_fail(s,
                                     "balance_plan violated R13 log capability/class on vol %" PRIu32
                                     " target %" PRIu32 " (role=%s cap=%s)",
                                     p->volume, p->node, meta_role_name(tmb->role), meta_cap_name(tmb->capability));
                        return;
                    }
                    if (smb && meta_member_has_fast_disk(smb) && !dst_fast) {
                        sim_fail(s,
                                 "balance_plan moved log replica from fast node %" PRIu32 " to HDD-only node %" PRIu32
                                 " on vol %" PRIu32 " (E50 violated)",
                                 smb->id, tmb->id, p->volume);
                        return;
                    }
                } else {
                    uint64_t mark_ppm = chk_chunk_fill_mark_ppm(in);
                    enum chk_elig_err err = CHK_ELIG_OK;
                    if (!chk_member_stripe_elig(in, tmb, 0, mark_ppm, &err)) {
                        if (err == CHK_ELIG_FULL)
                            sim_fail(s,
                                     "balance_plan violated R13 chunk fill mark on vol %" PRIu32 " pos %" PRIu32
                                     " target %" PRIu32,
                                     p->volume, p->pos, p->node);
                        else
                            sim_fail(s,
                                     "balance_plan violated disk use/capability on vol %" PRIu32 " pos %" PRIu32
                                     " target %" PRIu32 " (role=%s cap=%s)",
                                     p->volume, p->pos, p->node, meta_role_name(tmb->role),
                                     meta_cap_name(tmb->capability));
                        return;
                    }
                    if (smb) {
                        uint8_t scls = chk_chunk_class(smb);
                        uint8_t dcls = chk_chunk_class(tmb);
                        if (scls != META_DISK_NONE && dcls != META_DISK_NONE && scls != dcls) {
                            sim_fail(s,
                                     "balance_plan moved stripe position across disk classes on vol %" PRIu32
                                     " pos %" PRIu32 " (%s -> %s)",
                                     p->volume, p->pos, meta_disk_class_name(scls), meta_disk_class_name(dcls));
                            return;
                        }
                        uint64_t scap = 0, sused = 0, dcap = 0, dused = 0;
                        int shu = 0, dhu = 0;
                        chk_node_chunk_stats(in, smb, &scap, &sused, &shu);
                        chk_node_chunk_stats(in, tmb, &dcap, &dused, &dhu);
                        uint32_t sholds = 0;
                        for (uint32_t vj = 0; vj < sm->nvolumes; vj++) {
                            const struct meta_volume *mv = &sm->volumes[vj];
                            if (mv->redundancy != META_RED_EC)
                                continue;
                            for (uint32_t k = 0; k < mv->nstripe && k < META_STRIPE_MAX; k++)
                                if (mv->stripe[k] == smb->id)
                                    sholds++;
                        }
                        if (shu && dhu && dcap > 0 && sholds > 0) {
                            uint64_t est_b = sused / sholds;
                            if (est_b > 0) {
                                uint64_t dst_prev =
                                    (tmb->id >= 1 && tmb->id <= SIM_MAX_NODES) ? bal_dst_added[tmb->id - 1] : 0;
                                uint64_t proj_d = add_sat_u64(add_sat_u64(dused, dst_prev), est_b);
                                uint64_t nu_d = (uint64_t)(((__uint128_t)proj_d * 1000000ull) / dcap);
                                if (nu_d >= mark_ppm) {
                                    sim_fail(s,
                                             "balance_plan violated R13 chunk fill mark: projected target %" PRIu32
                                             " to %" PRIu64 " ppm >= %" PRIu64 " ppm on vol %" PRIu32,
                                             tmb->id, nu_d, mark_ppm, p->volume);
                                    return;
                                }
                                if (tmb->id >= 1 && tmb->id <= SIM_MAX_NODES)
                                    bal_dst_added[tmb->id - 1] = add_sat_u64(dst_prev, est_b);
                            }
                        }
                    }
                }
            }
            /* E9 rack survivability must never be broken by a balancer move. */
            if (place_rack_survivable(sm, v)) {
                struct meta_volume va = *v;
                if (p->kind == PLACE_PROP_REPLICA_ADD) {
                    for (uint32_t k = 0; k < va.nrep; k++)
                        if (va.rep[k].node == p->other_node)
                            va.rep[k].node = p->node;
                } else if (p->pos < va.nstripe) {
                    va.stripe[p->pos] = p->node;
                }
                if (!place_rack_survivable(sm, &va)) {
                    sim_fail(s, "balance_plan broke E9 rack survivability on vol %" PRIu32, p->volume);
                    return;
                }
            }
            bal_moves++;
            s->cov.bal_moves++;
            if (p->volume >= 1 && p->volume <= SIM_MAX_VOLS) {
                vol_moving[p->volume - 1]++;
                vol_bal_emitted[p->volume - 1]++;
            }
        }
    }
    if (bal_moves > 0 && !in->balance_apply) {
        /* Compute the clean (non-mutant) rack/class/fill-aware balance plan size (catches balanceunbounded). */
        struct place_in clean_in = *in;
        clean_in.mutant = PLACE_MUT_NONE;
        clean_in.balance_apply = 0;
        struct balance_summary sum;
        balance_plan_summary(&clean_in, &sum);
        uint32_t plan_total = sum.log_moves + sum.stripe_moves;
        if (bal_moves > max_plan || sum.over_cap || (plan_total + inflight_bal) > max_plan) {
            sim_fail(s, "balance_plan exceeded 25%% plan cap: emitted %" PRIu32 " moves (plan=%" PRIu32
                        " inflight=%" PRIu32 " > cap %" PRIu32 ")",
                     bal_moves, plan_total, inflight_bal, max_plan);
            return;
        }
    }
    if (bal_moves > 0 && (bal_moves + inflight_bal) > max_conc) {
        sim_fail(s, "balance_plan exceeded 5%% cluster concurrent cap: %" PRIu32 " + %" PRIu32 " > %" PRIu32,
                 bal_moves, inflight_bal, max_conc);
        return;
    }
    for (uint32_t vi = 0; vi < sm->nvolumes; vi++) {
        const struct meta_volume *v = &sm->volumes[vi];
        if (v->number < 1 || v->number > SIM_MAX_VOLS || vol_bal_emitted[v->number - 1] == 0)
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
        if (s->use_classes) {
            const struct sim_node *sn = sim_node_by_id_c(s, m->id);
            if (!sn || m->ndisks != (uint32_t)sn->ndisks) {
                sim_fail(s, "member %" PRIu32 " disk count drifted from metadata (meta=%" PRIu32 " sim=%d)", m->id,
                         m->ndisks, sn ? sn->ndisks : -1);
                return;
            }
            for (uint32_t d = 0; d < m->ndisks; d++) {
                if (memcmp(m->disks[d].store_id, sn->disks[d].store_uuid, 16) != 0 ||
                    m->disks[d].disk_class != sn->disks[d].disk_class || m->disks[d].state != sn->disks[d].state ||
                    m->disks[d].uses != sn->disks[d].uses) {
                    sim_fail(s, "member %" PRIu32 " disk %" PRIu32 " state drifted from metadata", m->id, d);
                    return;
                }
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

static void make_store_uuid(uint64_t sid, uint8_t out[16])
{
    memset(out, 0, 16);
    out[0] = 0xd1;
    for (int i = 0; i < 8; i++)
        out[1 + i] = (uint8_t)((sid >> (i * 8)) & 0xffu);
    out[15] = (uint8_t)((sid ^ 0x5au) & 0xffu);
}

enum sim_node_layout {
    NL_MIXED_NVME_HDD = 0, /* 1-2 NVMe(LOG) + 2-3 HDD(CHUNK), role=BOTH */
    NL_HDD_STORAGE = 1,    /* 2-3 HDD(CHUNK), role=STORAGE */
    NL_HDD_BOTH = 2,       /* 2-3 HDD(CHUNK), role=BOTH (compute only if log_on_hdd=1) */
    NL_NVME_COMPUTE = 3,   /* 1-2 NVMe(LOG), role=COMPUTE */
    NL_ALL_FLASH_NVME = 4, /* 2 NVMe(ALL), role=BOTH */
    NL_ALL_FLASH_SSD = 5,  /* 2 SSD(ALL), role=BOTH */
    NL_MIXED_NVME_SSD = 6, /* 1 NVMe(LOG) + 2 SSD(CHUNK), role=BOTH */
};

static int sim_register_disk(struct sim *s, struct sim_node *n, uint8_t disk_class, uint8_t uses, uint64_t size_gib)
{
    if (!n || n->ndisks >= SIM_MAX_DISKS)
        return -1;
    uint64_t sid = ++s->next_store_id;
    struct meta_disk md;
    memset(&md, 0, sizeof md);
    make_store_uuid(sid, md.store_id);
    md.disk_class = disk_class;
    md.uses = uses;
    md.state = META_STORE_UP;
    md.size_bytes = size_gib << 30;
    md.sync_us = (disk_class == META_DISK_NVME) ? 80 : (disk_class == META_DISK_SSD ? 400 : 4000);

    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_store_add(&e, n->id, &md);
    int res[4] = {0};
    sim_apply_entry(s, &e, res, 4);
    if (res[0] != META_OK)
        return -1;

    int d = n->ndisks++;
    n->disk_ok[d] = 1;
    n->disk_store_id[d] = sid;
    n->expected_store_id[d] = sid;
    struct sim_disk *sd = &n->disks[d];
    memset(sd, 0, sizeof *sd);
    memcpy(sd->store_uuid, md.store_id, 16);
    sd->disk_class = md.disk_class;
    sd->uses = md.uses;
    sd->state = META_STORE_UP;
    sd->size_bytes = md.size_bytes;
    /* Moderate baseline usage (15..45%) so fill spikes and volume sizes interact cleanly with marks. */
    if (uses & META_DISK_USE_CHUNK) {
        uint64_t pct = 15 + rng_u32(&s->r, 31);
        sd->chunk_bytes = (sd->size_bytes / 100u) * pct;
    }
    if (uses & META_DISK_USE_LOG) {
        uint64_t room = (uses & META_DISK_USE_CHUNK) && sd->size_bytes > sd->chunk_bytes
                            ? (sd->size_bytes - sd->chunk_bytes)
                            : sd->size_bytes;
        uint64_t pct = 10 + rng_u32(&s->r, 30);
        sd->young_bytes = (room / 100u) * pct;
        sd->young_peak_7d = sd->young_bytes;
    }
    return d;
}

static void sim_add_member_meta_ex(struct sim *s, uint32_t id, const char *rack, uint8_t init_log_on_hdd,
                                   enum sim_node_layout layout)
{
    struct sim_node *n = &s->nodes[id - 1];
    memset(n, 0, sizeof *n);
    n->id = id;
    n->class_mode = (uint8_t)(s->use_classes != 0);
    if (rack && rack[0])
        snprintf(n->rack, sizeof n->rack, "%s", rack);
    if (!s->use_classes) {
        n->ndisks = 1 + (int)rng_u32(&s->r, SIM_STD_DISKS);
        for (int d = 0; d < n->ndisks; d++) {
            n->disk_ok[d] = 1;
            n->disk_store_id[d] = ++s->next_store_id;
            n->expected_store_id[d] = n->disk_store_id[d];
        }
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
        ms.log_on_hdd = init_log_on_hdd;
        ms.young_move_pct = s->pset.young_move_pct;
        ms.young_high_pct = s->pset.young_high_pct;
        meta_op_init(&e, uuid, &ms, &mb, META_MODE_REPLICATED, META_LEVEL);
    } else {
        meta_op_member_add(&e, &mb);
    }
    int res[4] = {0};
    sim_apply_entry(s, &e, res, 4);
    if (res[0] != META_OK) {
        sim_fail(s, "init/add member %" PRIu32 " failed: %s", id, meta_strerror(res[0]));
        return;
    }

    if (!s->use_classes)
        return;

    /* Register R13 disks via real META_OP_STORE_ADD and declared role via real META_OP_MEMBER_ROLE. */
    uint8_t role = META_ROLE_BOTH;
    switch (layout) {
    case NL_MIXED_NVME_HDD: {
        int nnvme = 1 + (int)rng_u32(&s->r, 2);
        int nhdd = 2 + (int)rng_u32(&s->r, 2);
        for (int i = 0; i < nnvme; i++)
            sim_register_disk(s, n, META_DISK_NVME, META_DISK_USE_LOG, 120 + rng_u32(&s->r, 120));
        for (int i = 0; i < nhdd; i++)
            sim_register_disk(s, n, META_DISK_HDD, META_DISK_USE_CHUNK, 400 + rng_u32(&s->r, 600));
        role = META_ROLE_BOTH;
        break;
    }
    case NL_HDD_STORAGE: {
        int nhdd = 2 + (int)rng_u32(&s->r, 2);
        for (int i = 0; i < nhdd; i++)
            sim_register_disk(s, n, META_DISK_HDD, META_DISK_USE_CHUNK, 400 + rng_u32(&s->r, 600));
        role = META_ROLE_STORAGE;
        break;
    }
    case NL_HDD_BOTH: {
        int nhdd = 2 + (int)rng_u32(&s->r, 2);
        for (int i = 0; i < nhdd; i++)
            sim_register_disk(s, n, META_DISK_HDD, META_DISK_USE_CHUNK, 400 + rng_u32(&s->r, 600));
        role = META_ROLE_BOTH;
        break;
    }
    case NL_NVME_COMPUTE: {
        int nnvme = 1 + (int)rng_u32(&s->r, 2);
        for (int i = 0; i < nnvme; i++)
            sim_register_disk(s, n, META_DISK_NVME, META_DISK_USE_LOG, 120 + rng_u32(&s->r, 120));
        role = META_ROLE_COMPUTE;
        break;
    }
    case NL_ALL_FLASH_NVME: {
        for (int i = 0; i < 2; i++)
            sim_register_disk(s, n, META_DISK_NVME, META_DISK_USE_ALL, 200 + rng_u32(&s->r, 200));
        role = META_ROLE_BOTH;
        break;
    }
    case NL_ALL_FLASH_SSD: {
        for (int i = 0; i < 2; i++)
            sim_register_disk(s, n, META_DISK_SSD, META_DISK_USE_ALL, 240 + rng_u32(&s->r, 240));
        role = META_ROLE_BOTH;
        break;
    }
    case NL_MIXED_NVME_SSD: {
        sim_register_disk(s, n, META_DISK_NVME, META_DISK_USE_LOG, 120 + rng_u32(&s->r, 120));
        for (int i = 0; i < 2; i++)
            sim_register_disk(s, n, META_DISK_SSD, META_DISK_USE_CHUNK, 300 + rng_u32(&s->r, 300));
        role = META_ROLE_BOTH;
        break;
    }
    }
    n->role = role;
    if (role != META_ROLE_BOTH) {
        struct meta_entry er;
        meta_entry_init(&er);
        meta_op_member_role(&er, id, role);
        int r2[4] = {0};
        sim_apply_entry(s, &er, r2, 4);
        if (r2[0] != META_OK)
            sim_fail(s, "member_role node %" PRIu32 " -> %u failed: %s", id, role, meta_strerror(r2[0]));
    }
}

static void sim_add_member_meta(struct sim *s, uint32_t id, const char *rack)
{
    sim_add_member_meta_ex(s, id, rack, 0, NL_MIXED_NVME_HDD);
}

static void sim_init_cluster(struct sim *s, uint64_t seed, int mutant, const char *mutant_name, int trace,
                             int use_classes)
{
    memset(s, 0, sizeof *s);
    s->seed = seed;
    s->mutant = mutant;
    s->mutant_name = mutant_name;
    s->trace = trace;
    s->use_classes = use_classes;
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

    enum sim_node_layout layouts[SIM_MAX_NODES];
    uint8_t init_log_on_hdd = 0;

    if (!s->use_classes) {
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
    } else {
        /* R13 (--classes): build 6 diverse multi-disk / multi-class / role topologies across seeds. */
        uint32_t cprof = (uint32_t)((seed - 1) % 6);
        uint32_t nn = 0;
        if (cprof == 0) {
            /* Mixed NVMe+HDD cluster + HDD-only storage + NVMe-only compute (4+2 or 8+2). */
            gidx = (seed & 1) ? 1 : 2;
            gs = &k_geoms[gidx];
            uint32_t nmix = (uint32_t)gs->k + gs->m + 2;
            for (uint32_t i = 0; i < nmix; i++)
                layouts[nn++] = NL_MIXED_NVME_HDD;
            layouts[nn++] = NL_HDD_STORAGE;
            layouts[nn++] = NL_HDD_BOTH;
            layouts[nn++] = NL_NVME_COMPUTE;
            layouts[nn++] = NL_NVME_COMPUTE;
        } else if (cprof == 1) {
            /* Multi-class storage: >= k+m HDD nodes PLUS SSD and NVMe all-flash nodes (catches mixstripe). */
            gidx = 1; /* 4+2 -> k+m = 6 */
            gs = &k_geoms[gidx];
            for (uint32_t i = 0; i < 6; i++)
                layouts[nn++] = NL_MIXED_NVME_HDD;
            for (uint32_t i = 0; i < 3; i++)
                layouts[nn++] = NL_ALL_FLASH_SSD;
            for (uint32_t i = 0; i < 3; i++)
                layouts[nn++] = NL_ALL_FLASH_NVME;
            layouts[nn++] = NL_HDD_STORAGE;
            layouts[nn++] = NL_HDD_STORAGE;
            layouts[nn++] = NL_NVME_COMPUTE;
            layouts[nn++] = NL_NVME_COMPUTE;
        } else if (cprof == 2) {
            /* All-flash shared disks (NVMe + SSD with USE_ALL) + mixed NVMe+SSD + HDD storage. */
            gidx = (seed & 2) ? 1 : 0; /* 4+2 or replicas=3 */
            gs = &k_geoms[gidx];
            for (uint32_t i = 0; i < 6; i++)
                layouts[nn++] = NL_ALL_FLASH_SSD;
            for (uint32_t i = 0; i < 4; i++)
                layouts[nn++] = NL_ALL_FLASH_NVME;
            for (uint32_t i = 0; i < 2; i++)
                layouts[nn++] = NL_MIXED_NVME_SSD;
            layouts[nn++] = NL_HDD_STORAGE;
            layouts[nn++] = NL_HDD_BOTH;
            layouts[nn++] = NL_NVME_COMPUTE;
        } else if (cprof == 3) {
            /* Separated compute + storage (§9 / E20 / E49): NVMe-only compute + HDD-only storage. */
            gidx = 1; /* 4+2 */
            gs = &k_geoms[gidx];
            for (uint32_t i = 0; i < 6; i++)
                layouts[nn++] = NL_NVME_COMPUTE;
            for (uint32_t i = 0; i < 6; i++)
                layouts[nn++] = NL_HDD_STORAGE;
            for (uint32_t i = 0; i < 3; i++)
                layouts[nn++] = NL_HDD_BOTH;
        } else if (cprof == 4) {
            /* log_on_hdd enabled cluster: few fast nodes + many HDD-only nodes (BOTH & STORAGE). */
            gidx = 1; /* 4+2 */
            gs = &k_geoms[gidx];
            init_log_on_hdd = 1;
            for (uint32_t i = 0; i < 5; i++)
                layouts[nn++] = NL_MIXED_NVME_HDD;
            for (uint32_t i = 0; i < 4; i++)
                layouts[nn++] = NL_HDD_BOTH;
            for (uint32_t i = 0; i < 3; i++)
                layouts[nn++] = NL_HDD_STORAGE;
            layouts[nn++] = NL_NVME_COMPUTE;
            layouts[nn++] = NL_ALL_FLASH_SSD;
            layouts[nn++] = NL_ALL_FLASH_SSD;
        } else {
            /* Large EC (8+4 or 16+4) with mixed NVMe+HDD, SSD, compute-only, and storage-only nodes. */
            gidx = (seed & 1) ? 3 : 4; /* 8+4 or 16+4 */
            gs = &k_geoms[gidx];
            uint32_t nmix = (uint32_t)gs->k + gs->m;
            for (uint32_t i = 0; i < nmix; i++)
                layouts[nn++] = NL_MIXED_NVME_HDD;
            layouts[nn++] = NL_HDD_STORAGE;
            layouts[nn++] = NL_HDD_BOTH;
            layouts[nn++] = NL_NVME_COMPUTE;
            layouts[nn++] = NL_NVME_COMPUTE;
            layouts[nn++] = NL_ALL_FLASH_SSD;
            layouts[nn++] = NL_ALL_FLASH_SSD;
            if (nn > SIM_MAX_NODES - 4)
                nn = SIM_MAX_NODES - 4;
        }
        s->nnodes = nn;
        s->use_racks = (seed % 10 != 0);
        if (s->use_racks) {
            uint32_t max_racks = s->nnodes / 3;
            if (max_racks > 6)
                max_racks = 6;
            if (max_racks < 3)
                max_racks = 3;
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
            sim_add_member_meta_ex(s, id, rbuf, init_log_on_hdd, layouts[id - 1]);
            if (s->failed)
                return;
        }
    }

    /* Create 4..7 volumes using the real place_volume / place_stripe! */
    uint32_t nv = 4 + rng_u32(&s->r, 4);
    if (nv > SIM_MAX_VOLS)
        nv = SIM_MAX_VOLS;

    for (uint32_t vi = 1; vi <= nv && !s->failed; vi++) {
        /* In --classes mode, pre-fill 1 compute node's fast disks above young_move_pct and 1 storage node's chunk
         * stores past fill mark before the last volume so initial placement also exercises fill avoidance! */
        if (s->use_classes && vi == nv && (seed % 2 == 0)) {
            uint32_t fast_cnt = 0, stor_cnt = 0;
            for (uint32_t i = 0; i < s->sm.nmembers; i++) {
                const struct meta_member *mb = &s->sm.members[i];
                if (mb->role != META_ROLE_STORAGE && meta_member_has_fast_disk(mb))
                    fast_cnt++;
                if (mb->role != META_ROLE_COMPUTE && (meta_member_effective_cap(mb) & META_CAP_STORAGE))
                    stor_cnt++;
            }
            if (fast_cnt > gs->nrep + 1) {
                uint32_t pick = rng_u32(&s->r, s->nnodes);
                for (uint32_t off = 0; off < s->nnodes; off++) {
                    struct sim_node *fn = &s->nodes[(pick + off) % s->nnodes];
                    const struct meta_member *fmb = meta_member(&s->sm, fn->id);
                    if (fmb && fmb->role != META_ROLE_STORAGE && meta_member_has_fast_disk(fmb)) {
                        for (int d = 0; d < fn->ndisks; d++) {
                            if (meta_disk_class_fast(fn->disks[d].disk_class)) {
                                uint64_t room = (fn->disks[d].uses & META_DISK_USE_CHUNK) &&
                                                        fn->disks[d].size_bytes > fn->disks[d].chunk_bytes
                                                    ? (fn->disks[d].size_bytes - fn->disks[d].chunk_bytes)
                                                    : fn->disks[d].size_bytes;
                                fn->disks[d].young_bytes = (room / 100u) * 86u;
                                if (fn->disks[d].young_bytes > fn->disks[d].young_peak_7d)
                                    fn->disks[d].young_peak_7d = fn->disks[d].young_bytes;
                                fn->disks[d].fill_until_tick = 25 + rng_u32(&s->r, 30);
                            }
                        }
                        s->cov.cls_fast_fill++;
                        break;
                    }
                }
            }
            if (gs->redundancy == META_RED_EC && stor_cnt > (uint32_t)gs->k + gs->m + 1) {
                uint32_t pick = rng_u32(&s->r, s->nnodes);
                for (uint32_t off = 0; off < s->nnodes; off++) {
                    struct sim_node *sn = &s->nodes[(pick + off) % s->nnodes];
                    const struct meta_member *smb = meta_member(&s->sm, sn->id);
                    if (smb && smb->role != META_ROLE_COMPUTE && (meta_member_effective_cap(smb) & META_CAP_STORAGE)) {
                        for (int d = 0; d < sn->ndisks; d++) {
                            if (sn->disks[d].uses & META_DISK_USE_CHUNK) {
                                sn->disks[d].chunk_bytes = (sn->disks[d].size_bytes / 100u) * 96u;
                                sn->disks[d].fill_until_tick = 25 + rng_u32(&s->r, 30);
                            }
                        }
                        s->cov.cls_chunk_fill++;
                        break;
                    }
                }
            }
        }

        struct plan_snap snap;
        struct place_in in;
        build_place_in(s, &snap, &in);

        const struct geom_spec *vg = gs;
        if (prof == 5 || (s->use_classes && vi == 1 && gs->redundancy == META_RED_EC)) {
            /* Mixed cluster: include both replicas=3 and EC volumes. */
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
        sv->log_start = 1;
        sv->log_end = 10;
        for (uint32_t k = 0; k < va.n; k++)
            if (va.nodes[k] >= 1 && va.nodes[k] <= SIM_MAX_NODES)
                sv->rep_log_end[va.nodes[k] - 1] = sv->log_end;

        /* Verify place_rack_survivable matches rack configuration when racks are configured and nracks >= m+1. */
        if (s->use_racks && !s->use_classes && s->mutant == PLACE_MUT_NONE) {
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
        if (v->nisr >= v->min_isr) {
            sv->log_end++;
            for (uint32_t j = 0; j < v->nisr; j++) {
                uint32_t u = v->isr[j];
                const struct sim_node *un = sim_node_by_id_c(s, u);
                if (un && un->alive && !un->destroyed && node_is_heard_fresh(s, un) && un->fault_vol != v->number &&
                    u >= 1 && u <= SIM_MAX_NODES)
                    sv->rep_log_end[u - 1] = sv->log_end;
            }
        }
        /* Occasionally trim old WAL segments past an OUT replica (models repl_retain_down_gb cap being reached on a
         * subset of moves so sim_place exercises both retained-segment cancel and trimmed-segment non-cancel). */
        if (sv->log_end > sv->log_start + 5) {
            int has_out_rep = 0;
            for (uint32_t k = 0; k < v->nrep; k++) {
                const struct meta_member *rmb = meta_member(&s->sm, v->rep[k].node);
                if (!v->rep[k].learner && rmb && rmb->state == META_OUT)
                    has_out_rep = 1;
            }
            if (has_out_rep && rng_pct(&s->r, 4))
                sv->log_start = sv->log_end;
        }

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
                if (u >= 1 && u <= SIM_MAX_NODES)
                    sv->rep_log_end[u - 1] = sv->log_end;
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
        } else if (p->kind == PLACE_PROP_MOVE_CANCEL) {
            const struct meta_volume *v = meta_volume(&s->sm, p->volume);
            const struct meta_replica *rb = v ? meta_replica(v, p->node) : NULL;
            if (v && rb && rb->learner && rb->replaces == p->other_node && !meta_volume_in_isr(v, p->node)) {
                meta_op_replica_remove(&e, p->volume, v->epoch, p->node);
                emit = 1;
            }
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
                    if (p->is_return) {
                        s->cov.stripe_returns++;
                        if (s->mutant == PLACE_MUT_PARTIAL_DEL_NOW && prev_holder_before &&
                            prev_holder_before != p->node) {
                            /* Planted mutant `partialdelnow`: deletes B's partial fill chunks immediately at return
                             * STRIPE_SET instead of waiting for A's WHOLE + committed MK_GC release. */
                            for (uint32_t oi = 0; oi < sv->nobjs; oi++) {
                                if (sv->objs[oi].state == OBJ_LIVE && sv->objs[oi].stripe_version < v->stripe_version)
                                    sim_delete_chunk(s, sv, oi, p->pos, prev_holder_before);
                            }
                        }
                    }
                    record_release_obligation(s, sv, p->pos, p->other_node, v->stripe_version);
                    if (prev_holder_before && prev_holder_before != p->other_node && prev_holder_before != p->node)
                        record_release_obligation(s, sv, p->pos, prev_holder_before, v->stripe_version);
                }
                sim_trace(s, "applied %sSTRIPE_SET vol=%" PRIu32 " pos=%" PRIu32 " node=%" PRIu32 " prev=%" PRIu32
                             " margin=%d",
                          p->is_return ? "return " : "", p->volume, p->pos, p->node, p->other_node, p->margin);
            } else if (p->kind == PLACE_PROP_MOVE_CANCEL) {
                s->cov.log_cancels++;
                sim_trace(s, "applied MOVE_CANCEL vol=%" PRIu32 " from=%" PRIu32 " cancel_learner=%" PRIu32, p->volume,
                          p->other_node, p->node);
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
    if (!n->class_mode)
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

static void snapshot_node_stores(struct sim_node *n)
{
    if (!n)
        return;
    for (int d = 0; d < n->ndisks; d++)
        n->expected_store_id[d] = n->disk_store_id[d];
}

static void sim_step_class_timers(struct sim *s)
{
    if (!s->use_classes)
        return;
    for (uint32_t i = 0; i < s->nnodes && !s->failed; i++) {
        struct sim_node *n = &s->nodes[i];
        const struct meta_member *mb = meta_member(&s->sm, n->id);
        if (!mb || mb->state == META_DESTROYED)
            continue;
        for (int d = 0; d < n->ndisks && !s->failed; d++) {
            struct sim_disk *sd = &n->disks[d];
            /* Drain temporary fill spikes back below mark. */
            if (sd->fill_until_tick > 0 && s->tick >= sd->fill_until_tick) {
                sd->fill_until_tick = 0;
                if (sd->uses & META_DISK_USE_CHUNK) {
                    uint64_t pct = 20 + rng_u32(&s->r, 30);
                    sd->chunk_bytes = (sd->size_bytes / 100u) * pct;
                }
                if (sd->uses & META_DISK_USE_LOG) {
                    uint64_t room = (sd->uses & META_DISK_USE_CHUNK) && sd->size_bytes > sd->chunk_bytes
                                        ? (sd->size_bytes - sd->chunk_bytes)
                                        : sd->size_bytes;
                    uint64_t pct = 15 + rng_u32(&s->r, 25);
                    sd->young_bytes = (room / 100u) * pct;
                }
            }
            /* Escalate DOWN disk -> OUT when its timer expires (if safe for live EC objects). */
            if (sd->state == META_STORE_DOWN && sd->out_tick > 0 && s->tick >= sd->out_tick) {
                sd->out_tick = 0;
                int safe = !(sd->uses & META_DISK_USE_CHUNK) || !taking_down_exceeds(s, n->id, 0);
                if (safe) {
                    struct meta_entry e;
                    meta_entry_init(&e);
                    meta_op_store_state(&e, n->id, sd->store_uuid, META_STORE_OUT);
                    int res[4] = {0};
                    sim_apply_entry(s, &e, res, 4);
                    if (res[0] == META_OK) {
                        sd->state = META_STORE_OUT;
                        if (sd->uses & META_DISK_USE_CHUNK)
                            wipe_node_disk(s, n, d);
                        else
                            n->disk_ok[d] = 0;
                        sd->recover_tick = s->tick + 25 + rng_u32(&s->r, 40);
                        s->cov.cls_disk_out++;
                        sim_trace(s, "R13 disk OUT node=%" PRIu32 " disk=%d class=%s", n->id, d,
                                  meta_disk_class_name(sd->disk_class));
                    }
                } else {
                    sd->recover_tick = s->tick + 10;
                }
            }
            /* Recover DOWN or OUT disk back to UP. */
            if ((sd->state == META_STORE_DOWN || sd->state == META_STORE_OUT) && sd->recover_tick > 0 &&
                s->tick >= sd->recover_tick) {
                sd->recover_tick = 0;
                int was_out = (sd->state == META_STORE_OUT);
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_store_state(&e, n->id, sd->store_uuid, META_STORE_UP);
                int res[4] = {0};
                sim_apply_entry(s, &e, res, 4);
                if (res[0] == META_OK) {
                    sd->state = META_STORE_UP;
                    n->disk_ok[d] = 1;
                    if (was_out) {
                        sd->chunk_bytes = (sd->uses & META_DISK_USE_CHUNK) ? (sd->size_bytes / 100u) * 15u : 0;
                        sd->young_bytes = (sd->uses & META_DISK_USE_LOG) ? (sd->size_bytes / 100u) * 10u : 0;
                    }
                    s->cov.cls_disk_recover++;
                    sim_trace(s, "R13 disk RECOVER node=%" PRIu32 " disk=%d", n->id, d);
                }
            }
        }
    }
}

static void sim_inject_class_event(struct sim *s)
{
    uint32_t cev = rng_u32(&s->r, 100);
    if (cev < 24) {
        /* Disk DOWN via META_OP_STORE_STATE (fast or capacity disk). */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (!n || !mb || !n->alive || n->destroyed || mb->state != META_UP || n->ndisks == 0)
            return;
        int cands[SIM_MAX_DISKS], nc = 0;
        for (int d = 0; d < n->ndisks; d++)
            if (n->disks[d].state == META_STORE_UP && n->disk_ok[d])
                cands[nc++] = d;
        if (!nc)
            return;
        int d = cands[rng_u32(&s->r, (uint32_t)nc)];
        struct sim_disk *sd = &n->disks[d];
        if ((sd->uses & META_DISK_USE_CHUNK) && taking_down_exceeds(s, id, 0))
            return;
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_store_state(&e, id, sd->store_uuid, META_STORE_DOWN);
        int res[4] = {0};
        sim_apply_entry(s, &e, res, 4);
        if (res[0] == META_OK) {
            sd->state = META_STORE_DOWN;
            n->disk_ok[d] = 0;
            if (rng_pct(&s->r, 55)) {
                sd->out_tick = s->tick + s->pset.failstop_out_s + 2 + rng_u32(&s->r, 10);
                sd->recover_tick = 0;
            } else {
                sd->out_tick = 0;
                sd->recover_tick = s->tick + 8 + rng_u32(&s->r, 25);
            }
            /* E57: a DOWN fast disk fail-stops a log replica on that node so it rebuilds elsewhere. */
            if (meta_disk_class_fast(sd->disk_class) && n->fault_vol == 0) {
                for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
                    const struct meta_volume *v = &s->sm.volumes[vi];
                    if (meta_replica(v, id) && !chk_vol_has_move(v) && v->nisr > v->min_isr) {
                        n->fault_vol = v->number;
                        n->fault_ticks = 1;
                        s->cov.repl_faults++;
                        break;
                    }
                }
            }
            s->cov.cls_disk_down++;
            sim_trace(s, "R13 disk DOWN node=%" PRIu32 " disk=%d class=%s", id, d,
                      meta_disk_class_name(sd->disk_class));
        }
    } else if (cev < 38) {
        /* Disk added via META_OP_STORE_ADD. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (!n || !mb || !n->alive || n->destroyed || mb->state != META_UP || n->ndisks >= SIM_MAX_DISKS)
            return;
        uint8_t cls, uses;
        uint64_t gib;
        if (n->role == META_ROLE_COMPUTE) {
            cls = rng_pct(&s->r, 70) ? META_DISK_NVME : META_DISK_SSD;
            uses = META_DISK_USE_LOG;
            gib = 150 + rng_u32(&s->r, 150);
        } else if (n->role == META_ROLE_STORAGE) {
            cls = rng_pct(&s->r, 80) ? META_DISK_HDD : META_DISK_SSD;
            uses = META_DISK_USE_CHUNK;
            gib = 400 + rng_u32(&s->r, 600);
        } else {
            uint32_t pick = rng_u32(&s->r, 3);
            cls = pick == 0 ? META_DISK_HDD : (pick == 1 ? META_DISK_SSD : META_DISK_NVME);
            uses = (cls == META_DISK_HDD) ? META_DISK_USE_CHUNK
                                          : (rng_pct(&s->r, 50) ? META_DISK_USE_LOG : META_DISK_USE_ALL);
            gib = (cls == META_DISK_HDD) ? (400 + rng_u32(&s->r, 600)) : (180 + rng_u32(&s->r, 200));
        }
        if (sim_register_disk(s, n, cls, uses, gib) >= 0) {
            s->cov.cls_disk_add++;
            sim_trace(s, "R13 disk ADD node=%" PRIu32 " class=%s uses=%u", id, meta_disk_class_name(cls), uses);
        }
    } else if (cev < 50) {
        /* Disk class changed via META_OP_STORE_CLASS. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (!n || !mb || !n->alive || n->destroyed || mb->state == META_DESTROYED || n->ndisks == 0)
            return;
        int d = (int)rng_u32(&s->r, (uint32_t)n->ndisks);
        struct sim_disk *sd = &n->disks[d];
        if (sd->state == META_STORE_DESTROYED)
            return;
        uint8_t new_cls;
        if (sd->disk_class == META_DISK_NVME)
            new_cls = META_DISK_SSD;
        else if (sd->disk_class == META_DISK_SSD)
            new_cls = (sd->uses & META_DISK_USE_CHUNK) && n->role != META_ROLE_COMPUTE ? META_DISK_HDD : META_DISK_NVME;
        else
            new_cls = META_DISK_SSD;
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_store_class(&e, id, sd->store_uuid, new_cls);
        int res[4] = {0};
        sim_apply_entry(s, &e, res, 4);
        if (res[0] == META_OK) {
            sd->disk_class = new_cls;
            s->cov.cls_disk_class++;
            sim_trace(s, "R13 disk CLASS node=%" PRIu32 " disk=%d -> %s", id, d, meta_disk_class_name(new_cls));
        }
    } else if (cev < 66) {
        /* Fast disk filling above young_move_pct (80%), then draining back after 25..65 ticks. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (!n || !mb || !n->alive || n->destroyed || mb->state != META_UP)
            return;
        int spiked = 0;
        for (int d = 0; d < n->ndisks; d++) {
            struct sim_disk *sd = &n->disks[d];
            if (sd->state != META_STORE_UP || !meta_disk_class_fast(sd->disk_class))
                continue;
            uint64_t room = (sd->uses & META_DISK_USE_CHUNK) && sd->size_bytes > sd->chunk_bytes
                                ? (sd->size_bytes - sd->chunk_bytes)
                                : sd->size_bytes;
            uint64_t pct = s->pset.young_move_pct + 2 + rng_u32(&s->r, 12); /* 82..93% */
            if (pct > 95)
                pct = 95;
            sd->young_bytes = (room / 100u) * pct;
            if (sd->young_bytes > sd->young_peak_7d)
                sd->young_peak_7d = sd->young_bytes;
            sd->fill_until_tick = s->tick + 25 + rng_u32(&s->r, 40);
            spiked = 1;
        }
        if (spiked) {
            s->cov.cls_fast_fill++;
            sim_trace(s, "R13 fast disk FILL spike node=%" PRIu32, id);
        }
    } else if (cev < 80) {
        /* Chunk store filling past §5.6 fill mark (and sometimes past 95% rebuild full), then draining back. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        const struct meta_member *mb = meta_member(&s->sm, id);
        if (!n || !mb || !n->alive || n->destroyed || mb->state != META_UP)
            return;
        int spiked = 0;
        uint64_t pct = rng_pct(&s->r, 60) ? (96 + rng_u32(&s->r, 3)) : (88 + rng_u32(&s->r, 6));
        for (int d = 0; d < n->ndisks; d++) {
            struct sim_disk *sd = &n->disks[d];
            if (sd->state != META_STORE_UP || !(sd->uses & META_DISK_USE_CHUNK))
                continue;
            sd->chunk_bytes = (sd->size_bytes / 100u) * pct;
            sd->fill_until_tick = s->tick + 25 + rng_u32(&s->r, 40);
            spiked = 1;
        }
        if (spiked) {
            s->cov.cls_chunk_fill++;
            sim_trace(s, "R13 chunk store FILL spike node=%" PRIu32 " (%" PRIu64 "%%)", id, pct);
        }
    } else if (cev < 90) {
        /* HDD-only storage-role (or HDD-only BOTH) server joining. */
        if (s->nnodes < SIM_MAX_NODES) {
            uint32_t nid = s->nnodes + 1;
            s->nnodes = nid;
            char rbuf[16] = "";
            if (s->use_racks && s->nracks > 0)
                snprintf(rbuf, sizeof rbuf, "r%" PRIu32, (nid - 1) % s->nracks);
            enum sim_node_layout lay = rng_pct(&s->r, 60) ? NL_HDD_STORAGE : NL_HDD_BOTH;
            sim_add_member_meta_ex(s, nid, rbuf, s->sm.set.log_on_hdd, lay);
            s->cov.cls_hdd_join++;
            s->cov.op_node_add++;
            sim_trace(s, "R13 HDD-only node JOIN id=%" PRIu32 " role=%s rack=%s", nid,
                      lay == NL_HDD_STORAGE ? "storage" : "both", rbuf);
        }
    } else {
        /* Toggle log_on_hdd on/off via real META_OP_SETTINGS (CAS on settings_version). */
        struct meta_settings ns = s->sm.set;
        ns.log_on_hdd = ns.log_on_hdd ? 0 : 1;
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_settings(&e, &ns, s->sm.settings_version);
        int res[4] = {0};
        sim_apply_entry(s, &e, res, 4);
        if (res[0] == META_OK) {
            if (ns.log_on_hdd)
                s->cov.cls_log_on_hdd_on++;
            else
                s->cov.cls_log_on_hdd_off++;
            sim_trace(s, "R13 log_on_hdd -> %u", ns.log_on_hdd);
        }
    }
}

static void sim_step_events(struct sim *s, uint32_t total_ticks)
{
    /* 0. R13 disk fill drains, DOWN->OUT escalations, and disk recoveries. */
    sim_step_class_timers(s);
    if (s->failed)
        return;

    /* 1. Scheduled auto-recoveries of transient faults. */
    for (uint32_t i = 0; i < s->nnodes; i++) {
        struct sim_node *n = &s->nodes[i];
        if (!n->alive && !n->destroyed && n->recover_tick > 0 && s->tick >= n->recover_tick) {
            n->alive = 1;
            n->incarnation++;
            n->recover_tick = 0;
            n->failstop_beat = 0;
            /* Repair any failed disks on restart (fresh empty store if lost; in --classes mode keep META_STORE_UP
             * disks healthy and leave DOWN/OUT/DESTROYED disks to STORE_STATE recovery). */
            for (int d = 0; d < n->ndisks; d++) {
                if (n->class_mode) {
                    if (n->disks[d].state == META_STORE_UP)
                        n->disk_ok[d] = 1;
                } else if (!n->disk_ok[d]) {
                    n->disk_ok[d] = 1;
                }
            }
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

    /* Inject faults & operator ops probabilistically (~12% chance per tick, plus R13 disk/class events in --classes). */
    if (s->use_classes && rng_pct(&s->r, 12)) {
        sim_inject_class_event(s);
        if (s->failed)
            return;
    }
    if (!rng_pct(&s->r, 12))
        return;

    uint32_t ev = rng_u32(&s->r, 100);

    if (ev < 22) {
        /* Server down (timeout): transient or staying past rebuild_after_s. */
        uint32_t id = 1 + rng_u32(&s->r, s->nnodes);
        struct sim_node *n = sim_node_by_id(s, id);
        if (n && n->alive && !n->destroyed && count_alive_nodes(s) > 3 && !taking_down_exceeds(s, id, 0)) {
            snapshot_node_stores(n);
            n->alive = 0;
            n->failstop_beat = 0;
            uint32_t dur = rng_pct(&s->r, 40)
                               ? (10 + rng_u32(&s->r, s->pset.rebuild_after_s))
                               : (s->pset.rebuild_after_s + 4 + rng_u32(&s->r, 50));
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
            snapshot_node_stores(n);
            n->alive = 0;
            n->failstop_beat = 1;
            n->last_heard_ms = s->now_ms; /* last-gasp beat arrives fresh! */
            n->recover_tick = s->tick + s->pset.failstop_out_s + 4 + rng_u32(&s->r, 45);
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
                int do_wipe = 1;
                if (s->use_classes) {
                    do_wipe = 0;
                    struct sim_disk *sd = &n->disks[d];
                    if (sd->state == META_STORE_UP) {
                        struct meta_entry e1, e2;
                        int r1[4] = {0}, r2[4] = {0};
                        meta_entry_init(&e1);
                        meta_op_store_state(&e1, id, sd->store_uuid, META_STORE_DOWN);
                        sim_apply_entry(s, &e1, r1, 4);
                        if (r1[0] == META_OK) {
                            sd->state = META_STORE_DOWN;
                            n->disk_ok[d] = 0;
                            meta_entry_init(&e2);
                            meta_op_store_state(&e2, id, sd->store_uuid, META_STORE_OUT);
                            sim_apply_entry(s, &e2, r2, 4);
                            if (r2[0] == META_OK) {
                                sd->state = META_STORE_OUT;
                                sd->out_tick = 0;
                                sd->recover_tick = s->tick + 25 + rng_u32(&s->r, 40);
                                s->cov.cls_disk_out++;
                                do_wipe = 1;
                            } else {
                                sd->out_tick = 0;
                                sd->recover_tick = s->tick + 15;
                            }
                        }
                    }
                }
                if (do_wipe) {
                    wipe_node_disk(s, n, d);
                    s->cov.disk_losses++;
                    /* Replace/remount empty disk after 10 ticks if all disks failed. */
                    if (!s->use_classes && node_healthy_disks(n) == 0) {
                        n->alive = 0;
                        n->recover_tick = s->tick + s->pset.rebuild_after_s + 25;
                    }
                    sim_trace(s, "FAULT lost disk node=%" PRIu32 " disk=%d", id, d);
                }
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
                        snapshot_node_stores(n);
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
                    snapshot_node_stores(n);
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
                    snapshot_node_stores(n);
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
            if (s->use_classes) {
                enum sim_node_layout lay = (enum sim_node_layout)rng_u32(&s->r, 7);
                sim_add_member_meta_ex(s, nid, rbuf, s->sm.set.log_on_hdd, lay);
                if (lay == NL_HDD_STORAGE || lay == NL_HDD_BOTH)
                    s->cov.cls_hdd_join++;
            } else {
                sim_add_member_meta(s, nid, rbuf);
            }
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
                if (!nmb || nmb->state != META_UP || !nn || !nn->alive)
                    continue;
                if (!s->use_classes && node_healthy_disks(nn) == 0)
                    continue;
                int overlap = 0, old_holds = 0, old_log = 0, old_stripe = 0;
                for (uint32_t vi = 0; vi < s->sm.nvolumes; vi++) {
                    const struct meta_volume *v = &s->sm.volumes[vi];
                    int oh = meta_replica(v, old_id) != NULL;
                    int nh = meta_replica(v, nid) != NULL;
                    if (oh)
                        old_log = 1;
                    if (v->redundancy == META_RED_EC) {
                        for (uint32_t p = 0; p < v->nstripe; p++) {
                            if (v->stripe[p] == old_id) {
                                oh = 1;
                                old_stripe = 1;
                            }
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
                if (s->use_classes) {
                    if (old_log && (nmb->role == META_ROLE_STORAGE || !(meta_member_effective_cap(nmb) & META_CAP_COMPUTE)))
                        continue;
                    if (old_stripe && (nmb->role == META_ROLE_COMPUTE ||
                                       !(meta_member_effective_cap(nmb) & META_CAP_STORAGE) || node_healthy_disks(nn) == 0))
                        continue;
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
                if (s->use_classes &&
                    (mb->role == META_ROLE_COMPUTE || !(meta_member_effective_cap(mb) & META_CAP_STORAGE)))
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

static int run_seed(uint64_t seed, uint32_t ticks, int mutant, const char *mutant_name, int trace, int use_classes,
                    struct sim_cov *total_cov)
{
    struct sim *s = calloc(1, sizeof *s);
    if (!s) {
        fprintf(stderr, "sim_place: out of memory\n");
        return 2;
    }
    sim_init_cluster(s, seed, mutant, mutant_name, trace, use_classes);

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
        total_cov->stripe_returns += s->cov.stripe_returns;
        total_cov->log_cancels += s->cov.log_cancels;
        total_cov->op_replace += s->cov.op_replace;
        total_cov->op_drain += s->cov.op_drain;
        total_cov->op_destroyed += s->cov.op_destroyed;
        total_cov->op_move_pos += s->cov.op_move_pos;
        total_cov->op_node_add += s->cov.op_node_add;
        total_cov->bal_moves += s->cov.bal_moves;
        total_cov->m_faults += s->cov.m_faults;
        total_cov->mplus1_faults += s->cov.mplus1_faults;
        total_cov->props_lost += s->cov.props_lost;
        total_cov->props_delayed += s->cov.props_delayed;
        total_cov->failstops += s->cov.failstops;
        total_cov->repl_faults += s->cov.repl_faults;
        total_cov->disk_losses += s->cov.disk_losses;
        total_cov->slow_opens += s->cov.slow_opens;
        total_cov->cls_disk_down += s->cov.cls_disk_down;
        total_cov->cls_disk_out += s->cov.cls_disk_out;
        total_cov->cls_disk_recover += s->cov.cls_disk_recover;
        total_cov->cls_disk_add += s->cov.cls_disk_add;
        total_cov->cls_disk_class += s->cov.cls_disk_class;
        total_cov->cls_fast_fill += s->cov.cls_fast_fill;
        total_cov->cls_chunk_fill += s->cov.cls_chunk_fill;
        total_cov->cls_hdd_join += s->cov.cls_hdd_join;
        total_cov->cls_log_on_hdd_on += s->cov.cls_log_on_hdd_on;
        total_cov->cls_log_on_hdd_off += s->cov.cls_log_on_hdd_off;
        total_cov->cls_single_stripes += s->cov.cls_single_stripes;
        total_cov->cls_mixed_stripes += s->cov.cls_mixed_stripes;
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
            "  sim_place [--classes] [--seeds N] [--first S] [--seed S] [--ticks T] [--mutant NAME] [--trace]\n"
            "  sim_place --mttdl GEOM|all [--mttdl-table] [--disks-afr P] [--servers-afr P] [--disks-per-node D]\n"
            "            [--tb-per-node T] [--rate MB/s] [--nodes N] [--trials N] [--seed S]\n");
}

int main(int argc, char **argv)
{
    uint64_t first = 1, seeds = 100;
    uint32_t ticks = 600;
    const char *mutant_name = NULL;
    int trace = 0;
    int use_classes = 0;

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
        } else if (!strcmp(argv[i], "--classes")) {
            use_classes = 1;
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
        int rc = run_seed(seed, ticks, mutant, mutant_name, trace, use_classes, &cov);
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
            !cov.stripe_returns || !cov.log_cancels || !cov.m_faults || !cov.op_destroyed || !cov.props_lost ||
            !cov.props_delayed) {
            fprintf(stderr,
                    "sim_place: coverage floor failed over %" PRIu64 " seeds (settled=%" PRIu64 " deg=%" PRIu64
                    " fills=%" PRIu64 " gc=%" PRIu64 " logmoves=%" PRIu64 " guards=%" PRIu64 "/%" PRIu64
                    " holds=%" PRIu64 " returns=%" PRIu64 " sret=%" PRIu64 " lcan=%" PRIu64 " m=%" PRIu64
                    " dst=%" PRIu64 " lost=%" PRIu64 " dly=%" PRIu64 ")\n",
                    seeds, cov.objs_settled, cov.deg_settled, cov.fills_done, cov.gc_releases, cov.log_moves,
                    cov.guards_active, cov.guards_passed, cov.holds_applied, cov.returns_after_out, cov.stripe_returns,
                    cov.log_cancels, cov.m_faults, cov.op_destroyed, cov.props_lost, cov.props_delayed);
            return 1;
        }
        if (use_classes && (!cov.cls_disk_down || !cov.cls_disk_out || !cov.cls_disk_recover || !cov.cls_disk_add ||
                            !cov.cls_disk_class || !cov.cls_fast_fill || !cov.cls_chunk_fill || !cov.cls_hdd_join ||
                            !cov.cls_log_on_hdd_on || !cov.cls_log_on_hdd_off || !cov.cls_single_stripes)) {
            fprintf(stderr,
                    "sim_place --classes: R13 coverage floor failed over %" PRIu64 " seeds (down=%" PRIu64
                    " out=%" PRIu64 " rec=%" PRIu64 " add=%" PRIu64 " cls=%" PRIu64 " ffill=%" PRIu64
                    " cfill=%" PRIu64 " hddjoin=%" PRIu64 " lhdd=%" PRIu64 "/%" PRIu64 " single=%" PRIu64 ")\n",
                    seeds, cov.cls_disk_down, cov.cls_disk_out, cov.cls_disk_recover, cov.cls_disk_add,
                    cov.cls_disk_class, cov.cls_fast_fill, cov.cls_chunk_fill, cov.cls_hdd_join,
                    cov.cls_log_on_hdd_on, cov.cls_log_on_hdd_off, cov.cls_single_stripes);
            return 1;
        }
    }
    printf("sim_place%s: %" PRIu64 " seeds ok (first=%" PRIu64 " ticks=%" PRIu32
           " settled=%" PRIu64 " deg=%" PRIu64 " fills=%" PRIu64 " gc=%" PRIu64 " logmoves=%" PRIu64
           " guards=%" PRIu64 "/%" PRIu64 " holds=%" PRIu64 " returns=%" PRIu64 " sret=%" PRIu64 " lcan=%" PRIu64
           " ops[rep=%" PRIu64 " drn=%" PRIu64 " dst=%" PRIu64 " mov=%" PRIu64 " add=%" PRIu64 "]"
           " faults[m=%" PRIu64 " m1=%" PRIu64 " fs=%" PRIu64 " rf=%" PRIu64 " dk=%" PRIu64 " op=%" PRIu64 "]"
           " net[lost=%" PRIu64 " dly=%" PRIu64 "]",
           use_classes ? " --classes" : "", seeds, first, ticks, cov.objs_settled, cov.deg_settled, cov.fills_done,
           cov.gc_releases, cov.log_moves, cov.guards_active, cov.guards_passed, cov.holds_applied,
           cov.returns_after_out, cov.stripe_returns, cov.log_cancels, cov.op_replace, cov.op_drain, cov.op_destroyed,
           cov.op_move_pos, cov.op_node_add, cov.m_faults, cov.mplus1_faults, cov.failstops, cov.repl_faults,
           cov.disk_losses, cov.slow_opens, cov.props_lost, cov.props_delayed);
    if (use_classes) {
        printf(" r13[down=%" PRIu64 " out=%" PRIu64 " rec=%" PRIu64 " add=%" PRIu64 " cls=%" PRIu64
               " ffill=%" PRIu64 " cfill=%" PRIu64 " hddjoin=%" PRIu64 " lhdd=%" PRIu64 "/%" PRIu64
               " stripes=%" PRIu64 "/%" PRIu64 "]",
               cov.cls_disk_down, cov.cls_disk_out, cov.cls_disk_recover, cov.cls_disk_add, cov.cls_disk_class,
               cov.cls_fast_fill, cov.cls_chunk_fill, cov.cls_hdd_join, cov.cls_log_on_hdd_on, cov.cls_log_on_hdd_off,
               cov.cls_single_stripes, cov.cls_mixed_stripes);
    }
    printf(")\n");
    return 0;
}
