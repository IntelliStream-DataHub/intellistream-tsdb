/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Automatic damage healing on the Raft leader (R12, r12-heal; PLAN.md D8, cluster_int.h).
 *
 * Runs on the cluster thread only (cheal_duty called from cluster.c's leader_duties). Exposes Prometheus metrics via
 * server/creport.h (registered on cluster_start, unregistered on cluster_stop so standalone /metrics is untouched):
 *   - tsdb_cluster_auto_resets_total{why}    (damaged, failed, leader_transfer, no_candidate, below_min_isr)
 *   - tsdb_cluster_heal_blocked{volume,why}  (1 while blocked: no_candidate or below_min_isr)
 *
 * Rules (PLAN.md D8):
 *   1. A replica reported CL_HEALTH_DAMAGED in HBS_VOLUMES that is NOT the volume leader and NOT in the ISR:
 *      proposes R4's REPLICA_RESET (new rid) once and lets R4's snapshot rebuild it (the server sets the old
 *      directory aside as v<N>.lost-<rid>, never deleted automatically).
 *   2. A CL_HEALTH_DAMAGED volume leader: starts R4's planned leader transfer (ctransfer.c) to a healthy
 *      (non-DAMAGED, UP, fresh) ISR member; once leadership moves and the old leader leaves the ISR, item 1 resets
 *      and rebuilds it. While the leader itself is DAMAGED, no follower is reset (snapshots copy from the leader).
 *      If no non-DAMAGED ISR member exists, nothing moves: logs why once and exposes the blocked metric.
 *   3. Safety & rate limiting (in memory on the Raft leader; restarts on a Raft leader change):
 *      - Never reset while the remaining healthy non-DAMAGED ISR count is < min_isr or < 1.
 *      - Never reset while another replica of the volume is already a learner or moving (heals one at a time).
 *      - At most one automatic reset per volume per cooldown window (default 10 min; configurable in tests via
 *        TSDB_CLUSTER_TEST heal_cooldown_ms=N); failed or recurring resets back off exponentially (doubling up to 1 h),
 *        and the backoff streak clears only after the volume stays undamaged for a full cooldown window.
 *
 * Planted mutants (TSDB_CLUSTER_MUTANT, TSDB_TEST_HOOKS only):
 *   - healleader:   skips planned leader transfer for a DAMAGED leader and proposes REPLICA_RESET for the leader
 *                   directly (meta.c:2058 still refuses it at apply, so the leader never transfers or heals)
 *   - healbelowmin: skips the remaining healthy non-DAMAGED ISR >= min_isr check before resetting
 *   - healrepeat:   skips cooldown and exponential backoff (allows immediate repeat resets)
 */
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cluster_int.h"
#include "creport.h"

#define MS UINT64_C(1000000)
#define HEAL_COOLDOWN_DEFAULT_NS (600ull * 1000ull * MS) /* 10 min default per-volume auto-reset cooldown */
#define HEAL_BACKOFF_MAX_NS (3600ull * 1000ull * MS)     /* 1 h cap on exponential failure backoff */
#define HEAL_APPLY_TIMEOUT_NS (5000ull * MS)             /* wait for a queued REPLICA_RESET to commit */
#define HEAL_XFER_RETRY_NS (2000ull * MS)                /* minimum gap between planned transfer attempts */

enum heal_blocked_kind {
    HEAL_BLK_NONE = 0,
    HEAL_BLK_NO_CAND = 1,
    HEAL_BLK_BELOW_MIN = 2,
};

struct heal_vol {
    uint32_t vol;
    uint64_t recovered_ns;   /* monotonic ns when last reset finished; streak clears after g_cooldown_ns healthy */
    uint32_t last_healed_node; /* node of the last completed reset (recurrence backoff is per-node) */
    uint64_t cooldown_until;
    uint32_t fail_streak;
    uint64_t pending_rid;
    uint32_t pending_node;
    uint64_t pending_epoch;
    uint64_t pending_ns;
    int pending_applied;
    uint64_t xfer_cooldown_until;
    uint8_t blocked_reason; /* enum heal_blocked_kind */
    int no_cand_logged;
    int below_min_logged;
};

struct heal_blk_snap {
    uint32_t vol;
    uint8_t reason;
};

static struct heal_vol *g_hv;
static uint32_t g_nhv, g_chv;

/* Lock-free counters for tsdb_cluster_auto_resets_total{why} */
static atomic_uint_fast64_t n_damaged;
static atomic_uint_fast64_t n_failed;
static atomic_uint_fast64_t n_leader_transfer;
static atomic_uint_fast64_t n_no_candidate;
static atomic_uint_fast64_t n_below_min_isr;

/* Snapshot of currently blocked volumes for tsdb_cluster_heal_blocked{volume,why} */
static pthread_mutex_t g_blk_mu = PTHREAD_MUTEX_INITIALIZER;
static struct heal_blk_snap g_blk[CLUSTER_REPORT_VOLS];
static uint32_t g_nblk;
static int g_started;

static uint64_t g_cooldown_ns = HEAL_COOLDOWN_DEFAULT_NS;
static int g_heal_off;

enum heal_mutant_kind {
    HM_NONE = 0,
    HM_HEALLEADER = 1,
    HM_HEALBELOWMIN = 2,
    HM_HEALREPEAT = 3,
};
static int g_mutant;

static uint64_t since(uint64_t now, uint64_t t) { return now > t ? now - t : 0; }

/* Exponential backoff: base * 2^streak, capped at max(base, HEAL_BACKOFF_MAX_NS). */
static uint64_t backoff_ns(uint64_t base_ns, uint32_t streak)
{
    uint64_t cap = base_ns > HEAL_BACKOFF_MAX_NS ? base_ns : HEAL_BACKOFF_MAX_NS;
    uint64_t d = base_ns;
    for (uint32_t i = 0; i < streak && d < cap; i++)
        d = d > cap / 2 ? cap : d * 2;
    return d;
}

static struct heal_vol *hv_get(uint32_t vol)
{
    for (uint32_t i = 0; i < g_nhv; i++)
        if (g_hv[i].vol == vol)
            return &g_hv[i];
    if (g_nhv == g_chv) {
        uint32_t nc = g_chv ? g_chv * 2 : 16;
        struct heal_vol *nv = realloc(g_hv, nc * sizeof *nv);
        if (!nv)
            abort();
        g_hv = nv;
        g_chv = nc;
    }
    struct heal_vol *h = &g_hv[g_nhv++];
    memset(h, 0, sizeof *h);
    h->vol = vol;
    return h;
}

static void blk_publish(const struct heal_blk_snap *items, uint32_t n)
{
    if (n > CLUSTER_REPORT_VOLS)
        n = CLUSTER_REPORT_VOLS;
    pthread_mutex_lock(&g_blk_mu);
    int same = (n == g_nblk);
    for (uint32_t i = 0; same && i < n; i++)
        if (g_blk[i].vol != items[i].vol || g_blk[i].reason != items[i].reason)
            same = 0;
    if (same) {
        pthread_mutex_unlock(&g_blk_mu);
        return;
    }
    if (n > 0)
        memcpy(g_blk, items, n * sizeof items[0]);
    g_nblk = n;
    pthread_mutex_unlock(&g_blk_mu);
}

static void blk_clear(void)
{
    pthread_mutex_lock(&g_blk_mu);
    g_nblk = 0;
    pthread_mutex_unlock(&g_blk_mu);
}

static void cheal_collect_resets(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct creport_label l_dmg = {"why", "damaged"};
    struct creport_label l_fail = {"why", "failed"};
    struct creport_label l_xfer = {"why", "leader_transfer"};
    struct creport_label l_noc = {"why", "no_candidate"};
    struct creport_label l_min = {"why", "below_min_isr"};
    creport_sink_put_u64(sink, &l_dmg, 1, atomic_load_explicit(&n_damaged, memory_order_relaxed));
    creport_sink_put_u64(sink, &l_fail, 1, atomic_load_explicit(&n_failed, memory_order_relaxed));
    creport_sink_put_u64(sink, &l_xfer, 1, atomic_load_explicit(&n_leader_transfer, memory_order_relaxed));
    creport_sink_put_u64(sink, &l_noc, 1, atomic_load_explicit(&n_no_candidate, memory_order_relaxed));
    creport_sink_put_u64(sink, &l_min, 1, atomic_load_explicit(&n_below_min_isr, memory_order_relaxed));
}

static void cheal_collect_blocked(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct heal_blk_snap snap[CLUSTER_REPORT_VOLS];
    uint32_t n = 0;
    pthread_mutex_lock(&g_blk_mu);
    n = g_nblk;
    if (n > CLUSTER_REPORT_VOLS)
        n = CLUSTER_REPORT_VOLS;
    if (n > 0)
        memcpy(snap, g_blk, n * sizeof snap[0]);
    pthread_mutex_unlock(&g_blk_mu);

    for (uint32_t i = 0; i < n; i++) {
        char vbuf[16];
        snprintf(vbuf, sizeof vbuf, "%" PRIu32, snap[i].vol);
        const char *why = snap[i].reason == HEAL_BLK_NO_CAND ? "no_candidate" : "below_min_isr";
        struct creport_label lbl[2] = {{"volume", vbuf}, {"why", why}};
        creport_sink_put_u64(sink, lbl, 2, 1);
    }
}

void cheal_start(void)
{
    g_cooldown_ns = HEAL_COOLDOWN_DEFAULT_NS;
    g_heal_off = 0;
    g_mutant = HM_NONE;
#ifdef TSDB_TEST_HOOKS
    const char *ct = getenv("TSDB_CLUSTER_TEST");
    if (ct) {
        if (strstr(ct, "heal_off=1"))
            g_heal_off = 1;
        const char *p = strstr(ct, "heal_cooldown_ms=");
        unsigned long long ms = 0;
        if (p && sscanf(p, "heal_cooldown_ms=%llu", &ms) == 1 && ms > 0)
            g_cooldown_ns = ms * MS;
    }
    const char *m = getenv("TSDB_CLUSTER_MUTANT");
    if (m) {
        if (!strcmp(m, "healleader"))
            g_mutant = HM_HEALLEADER;
        else if (!strcmp(m, "healbelowmin"))
            g_mutant = HM_HEALBELOWMIN;
        else if (!strcmp(m, "healrepeat"))
            g_mutant = HM_HEALREPEAT;
        if (g_mutant != HM_NONE)
            cl_say("MUTANT %s (tests only)", m);
    }
#endif
    atomic_store_explicit(&n_damaged, 0, memory_order_relaxed);
    atomic_store_explicit(&n_failed, 0, memory_order_relaxed);
    atomic_store_explicit(&n_leader_transfer, 0, memory_order_relaxed);
    atomic_store_explicit(&n_no_candidate, 0, memory_order_relaxed);
    atomic_store_explicit(&n_below_min_isr, 0, memory_order_relaxed);
    blk_clear();
    if (!g_started) {
        creport_metric_collector("tsdb_cluster_auto_resets_total", CREPORT_COUNTER,
                                 "Automatic replica resets and damage-heal actions on the Raft leader, by reason.",
                                 cheal_collect_resets, NULL);
        creport_metric_collector("tsdb_cluster_heal_blocked", CREPORT_GAUGE,
                                 "1 while automatic damage healing for a volume is blocked on the Raft leader, by reason.",
                                 cheal_collect_blocked, NULL);
        g_started = 1;
    }
}

void cheal_stop(void)
{
    if (g_started) {
        creport_metric_collector_del("tsdb_cluster_auto_resets_total");
        creport_metric_collector_del("tsdb_cluster_heal_blocked");
        g_started = 0;
    }
    blk_clear();
    free(g_hv);
    g_hv = NULL;
    g_nhv = g_chv = 0;
}

/* 1 if member `node` is UP, heard fresh, and reports CL_HEALTH_OK for volume `v` (matching its current rid). */
static int is_isr_healthy(const struct meta_state *sm, const struct meta_volume *v, uint32_t node, uint64_t now)
{
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb || mb->state != META_UP || !cl_heard_fresh(node, now))
        return 0;
    const struct meta_replica *rp = meta_replica(v, node);
    if (!rp)
        return 0;
    uint64_t vrid = 0;
    int h = cl_vol_health(node, v->number, now, &vrid);
    if (h != CL_HEALTH_OK)
        return 0;
    if (vrid && rp->rid && vrid != rp->rid)
        return 0;
    return 1;
}

/* 1 if replica `rp` is heard fresh and reports CL_HEALTH_DAMAGED for its current rid. */
static int is_rep_damaged(const struct meta_volume *v, const struct meta_replica *rp, uint64_t now)
{
    if (!rp)
        return 0;
    uint64_t vrid = 0;
    int h = cl_vol_health(rp->node, v->number, now, &vrid);
    if (h != CL_HEALTH_DAMAGED)
        return 0;
    if (vrid && rp->rid && vrid != rp->rid)
        return 0; /* stale heartbeat from before an earlier reset */
    return 1;
}

/* 1 if another replica of `v` (other than `skip_node`) is rebuilding as a learner, or any replica is in a move. */
static int any_other_learner_or_move(const struct meta_volume *v, uint32_t skip_node)
{
    for (uint32_t i = 0; i < v->nrep; i++)
        if (v->rep[i].replaces || (v->rep[i].learner && v->rep[i].node != skip_node))
            return 1;
    return 0;
}

/* Reconcile the outcome of an earlier automatic REPLICA_RESET proposal for volume `v`. */
static void check_pending(const struct meta_volume *v, struct heal_vol *hv, uint64_t now)
{
    if (!hv->pending_rid)
        return;
    const struct meta_replica *pr = meta_replica(v, hv->pending_node);
    if (!pr) {
        hv->pending_rid = 0;
        hv->pending_applied = 0;
        hv->fail_streak = 0;
        hv->recovered_ns = 0;
        return;
    }
    if (pr->rid == hv->pending_rid) {
        hv->pending_applied = 1;
        if (!pr->learner) {
            /* Rebuilt and joined ISR (meta.c clears learner only on ISR join); record recovery time and node so
             * recurring damage on the same node (including immediate recurrence) grows the exponential backoff until
             * the volume stays undamaged for a full cooldown window. */
            hv->last_healed_node = hv->pending_node;
            hv->recovered_ns = now;
            hv->pending_rid = 0;
            hv->pending_applied = 0;
            return;
        }
        /* Still a learner: if the cooldown elapsed (or healrepeat) and it reports DAMAGED, back off and clear
         * pending_rid so the damaged learner can be retried after backoff rather than getting stuck. */
        if ((g_mutant == HM_HEALREPEAT || now >= hv->cooldown_until) && is_rep_damaged(v, pr, now)) {
            hv->fail_streak++;
            uint64_t wait_ns = backoff_ns(g_cooldown_ns, hv->fail_streak);
            hv->cooldown_until = now + wait_ns;
            hv->pending_rid = 0;
            hv->pending_applied = 0;
            hv->recovered_ns = 0;
            atomic_fetch_add_explicit(&n_failed, 1, memory_order_relaxed);
            cl_say("heal: volume %" PRIu32 ": reset replica node %" PRIu32 " did not recover (streak %" PRIu32
                   ", backoff %" PRIu64 " ms)", v->number, pr->node, hv->fail_streak, wait_ns / MS);
        }
        return;
    }
    if (hv->pending_applied) {
        /* Superseded by another set change or operator reset. */
        hv->pending_rid = 0;
        hv->pending_applied = 0;
        return;
    }
    if (v->epoch != hv->pending_epoch) {
        /* Unrelated leader change/transfer moved the epoch before apply (META_ERR_STALE): retry after a short gap
         * without penalising fail_streak with a full doubled backoff. */
        hv->pending_rid = 0;
        hv->pending_applied = 0;
        hv->cooldown_until = now + HEAL_XFER_RETRY_NS;
        cl_say("heal: volume %" PRIu32 ": auto-reset proposal for node %" PRIu32
               " superseded by epoch %" PRIu64 "; retrying shortly", v->number, hv->pending_node, v->epoch);
        return;
    }
    if (since(now, hv->pending_ns) >= HEAL_APPLY_TIMEOUT_NS) {
        hv->fail_streak++;
        uint64_t wait_ns = backoff_ns(g_cooldown_ns, hv->fail_streak);
        hv->cooldown_until = now + wait_ns;
        hv->pending_rid = 0;
        hv->pending_applied = 0;
        hv->recovered_ns = 0;
        atomic_fetch_add_explicit(&n_failed, 1, memory_order_relaxed);
        cl_say("heal: volume %" PRIu32 ": auto-reset proposal for node %" PRIu32 " did not apply (streak %" PRIu32
               ", backoff %" PRIu64 " ms)", v->number, hv->pending_node, hv->fail_streak, wait_ns / MS);
    }
}

static void propose_reset(const struct meta_volume *v, struct heal_vol *hv, uint32_t target, uint64_t now)
{
    uint64_t new_rid = cset_fresh_rid(v);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_replica_reset(&e, v->number, v->epoch, target, new_rid);
    if (cl_propose(&e) != 0) {
        hv->fail_streak++;
        uint64_t wait_ns = backoff_ns(g_cooldown_ns, hv->fail_streak);
        hv->cooldown_until = now + wait_ns;
        hv->recovered_ns = 0;
        atomic_fetch_add_explicit(&n_failed, 1, memory_order_relaxed);
        cl_say("heal: volume %" PRIu32 ": auto-reset of damaged node %" PRIu32 " failed to propose (streak %" PRIu32
               ", backoff %" PRIu64 " ms)", v->number, target, hv->fail_streak, wait_ns / MS);
        return;
    }
    /* If damage returned on the same node before staying healthy for a full cooldown window, grow the backoff. */
    if (hv->recovered_ns && hv->last_healed_node == target) {
        hv->fail_streak++;
        hv->recovered_ns = 0;
    } else if (hv->last_healed_node != target) {
        hv->recovered_ns = 0;
    }
    atomic_fetch_add_explicit(&n_damaged, 1, memory_order_relaxed);
    hv->pending_rid = new_rid;
    hv->pending_node = target;
    hv->pending_epoch = v->epoch;
    hv->pending_ns = now;
    hv->pending_applied = 0;
    uint64_t wait_ns = backoff_ns(g_cooldown_ns, hv->fail_streak);
    hv->cooldown_until = now + wait_ns;
    cl_say("heal: volume %" PRIu32 ": proposed automatic REPLICA_RESET for damaged node %" PRIu32
           " (new instance %016" PRIx64 ", cooldown %" PRIu64 " ms)", v->number, target, new_rid, wait_ns / MS);
}

/* Prune entries from g_hv for volumes no longer in the metadata state. */
static void hv_prune(const struct meta_state *sm)
{
    for (uint32_t i = 0; i < g_nhv;) {
        if (!meta_volume(sm, g_hv[i].vol))
            g_hv[i] = g_hv[--g_nhv];
        else
            i++;
    }
}

void cheal_duty(uint64_t now)
{
    if (!cl_leading()) {
        if (g_nhv > 0) {
            for (uint32_t i = 0; i < g_nhv; i++) {
                g_hv[i].pending_rid = 0;
                g_hv[i].pending_applied = 0;
                g_hv[i].blocked_reason = HEAL_BLK_NONE;
                g_hv[i].no_cand_logged = 0;
                g_hv[i].below_min_logged = 0;
            }
        }
        blk_clear();
        return;
    }
    if (g_heal_off || !cl_leader_settled(now))
        return;

    const struct meta_state *sm = cl_meta();
    hv_prune(sm);
    struct heal_blk_snap blk[CLUSTER_REPORT_VOLS];
    uint32_t nblk = 0;

    for (uint32_t i = 0; i < sm->nvolumes && cl_leading(); i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !v->nrep || !v->nhist)
            continue;

        struct heal_vol *hv = hv_get(v->number);
        check_pending(v, hv, now);

        /* Step 1: Check whether the volume leader itself reports CL_HEALTH_DAMAGED. */
        const struct meta_replica *lrp = v->leader ? meta_replica(v, v->leader) : NULL;
        if (lrp && is_rep_damaged(v, lrp, now)) {
            hv->below_min_logged = 0;
            if (g_mutant == HM_HEALLEADER) {
                /* Planted mutant: skips planned leader transfer and proposes REPLICA_RESET for the leader directly. */
                if (!hv->pending_rid && now >= hv->cooldown_until) {
                    cl_say("heal: volume %" PRIu32 ": MUTANT healleader: proposing REPLICA_RESET for leader node %"
                           PRIu32 " without transfer", v->number, v->leader);
                    propose_reset(v, hv, v->leader, now);
                }
                continue;
            }
            /* Pick a healthy non-DAMAGED ISR member to transfer leadership to. */
            uint32_t cand = 0;
            for (uint32_t k = 0; k < v->nrep; k++) {
                uint32_t m = v->rep[k].node;
                if (m != v->leader && meta_volume_in_isr(v, m) && is_isr_healthy(sm, v, m, now)) {
                    cand = m;
                    break;
                }
            }
            if (cand) {
                hv->no_cand_logged = 0;
                hv->blocked_reason = HEAL_BLK_NONE;
                if (!ctr_running(v->number) && !cl_promo_running(v->number) && now >= hv->xfer_cooldown_until) {
                    const char *no = ctr_move_start(v->number, cand, now);
                    if (!no) {
                        atomic_fetch_add_explicit(&n_leader_transfer, 1, memory_order_relaxed);
                        cl_say("heal: volume %" PRIu32 ": leader node %" PRIu32
                               " is DAMAGED; starting planned transfer to healthy ISR member node %" PRIu32,
                               v->number, v->leader, cand);
                        hv->xfer_cooldown_until = now + HEAL_XFER_RETRY_NS;
                    } else {
                        hv->xfer_cooldown_until = now + 1000ull * MS;
                    }
                }
            } else {
                hv->blocked_reason = HEAL_BLK_NO_CAND;
                if (nblk < CLUSTER_REPORT_VOLS)
                    blk[nblk++] = (struct heal_blk_snap){.vol = v->number, .reason = HEAL_BLK_NO_CAND};
                if (!hv->no_cand_logged) {
                    hv->no_cand_logged = 1;
                    atomic_fetch_add_explicit(&n_no_candidate, 1, memory_order_relaxed);
                    cl_say("heal: volume %" PRIu32 ": leader node %" PRIu32
                           " is DAMAGED with no non-DAMAGED ISR member; transfer withheld", v->number, v->leader);
                }
            }
            /* Never reset any follower while the leader itself is DAMAGED (snapshots copy from the leader). */
            continue;
        }

        hv->no_cand_logged = 0;
        if (hv->blocked_reason == HEAL_BLK_NO_CAND)
            hv->blocked_reason = HEAL_BLK_NONE;

        /* Only reset followers while the volume has a healthy leader able to serve a clean snapshot. */
        if (!v->leader || !is_isr_healthy(sm, v, v->leader, now))
            continue;

        /* Step 2: Find a CL_HEALTH_DAMAGED replica that is neither the leader nor in the ISR.
         * A learner that already opened its fresh directory (open_gen > 0) and is no longer tracked as pending_rid
         * is also eligible after backoff so a chronically damaged learner does not stay stuck indefinitely. */
        uint32_t target = 0;
        for (uint32_t k = 0; k < v->nrep; k++) {
            const struct meta_replica *rp = &v->rep[k];
            if (rp->node == v->leader || meta_volume_in_isr(v, rp->node))
                continue;
            if (rp->learner && (rp->open_gen == 0 || hv->pending_rid == rp->rid))
                continue;
            if (is_rep_damaged(v, rp, now)) {
                target = rp->node;
                break;
            }
        }
        if (!target) {
            hv->below_min_logged = 0;
            if (hv->blocked_reason == HEAL_BLK_BELOW_MIN)
                hv->blocked_reason = HEAL_BLK_NONE;
            /* Clear the failure backoff streak once the volume stays undamaged for a full cooldown window. */
            if (hv->recovered_ns && since(now, hv->recovered_ns) >= g_cooldown_ns) {
                hv->fail_streak = 0;
                hv->recovered_ns = 0;
            }
            continue;
        }

        /* One rebuild at a time per volume (never overlap with another rebuilding learner or an active move). */
        if (any_other_learner_or_move(v, target))
            continue;

        /* Count remaining healthy non-DAMAGED ISR members. */
        uint32_t good_isr = 0;
        for (uint32_t k = 0; k < v->nisr; k++) {
            uint32_t m = v->isr[k];
            if (m != target && is_isr_healthy(sm, v, m, now))
                good_isr++;
        }
        uint32_t min_isr = v->min_isr ? v->min_isr : 1;
        if (good_isr < 1 || (good_isr < min_isr && g_mutant != HM_HEALBELOWMIN)) {
            hv->blocked_reason = HEAL_BLK_BELOW_MIN;
            if (nblk < CLUSTER_REPORT_VOLS)
                blk[nblk++] = (struct heal_blk_snap){.vol = v->number, .reason = HEAL_BLK_BELOW_MIN};
            if (!hv->below_min_logged) {
                hv->below_min_logged = 1;
                atomic_fetch_add_explicit(&n_below_min_isr, 1, memory_order_relaxed);
                cl_say("heal: volume %" PRIu32 ": cannot auto-reset damaged node %" PRIu32
                       ": healthy non-DAMAGED ISR %" PRIu32 " < min_isr %" PRIu32, v->number, target, good_isr, min_isr);
            }
            continue;
        }

        hv->below_min_logged = 0;
        hv->blocked_reason = HEAL_BLK_NONE;

        if (cl_promo_running(v->number) || ctr_running(v->number))
            continue;
        if (hv->pending_rid)
            continue;
        if (g_mutant != HM_HEALREPEAT && hv->cooldown_until && now < hv->cooldown_until)
            continue;

        propose_reset(v, hv, target, now);
    }

    blk_publish(blk, nblk);
}
