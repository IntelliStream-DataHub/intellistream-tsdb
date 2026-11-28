/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Redundancy conversion driver on the Raft leader (R10 r10-driver; docs/design-cluster-data.md §6.4, E2, E38;
 * PLAN.md D26-D29). Runs on the cluster thread only, like server/cmove.c and server/cnode.c.
 *
 * Drives every volume with an active `v->conv` record (persisted in metadata by r10-meta) through the five ordered
 * phases, committing each step before the next and retrying every second:
 *   1. GROW:     REPLICA_ADD learners (replaces = 0) until nrep >= target_replicas and nisr >= target_replicas.
 *   2. RECORD:   commit target redundancy, min_isr, and new stripe group (preserving existing positions, E9).
 *   3. REENCODE: send CL_VOLCMD_CONV_WORK to the volume leader to re-encode old stripe_version objects at rebuild_mb_s
 *                (or MK_UNSETTLE per key for ec -> replicas=3); advance only once live old-object count == 0.
 *                For replicas -> ec, trigger a clean planned transfer (when nisr >= 2) so the volume leader attaches
 *                its EC tier cleanly without indeterminate writes, then advance once HBS_LIVE_OBJS confirms attach.
 *   4. DROP:     STRIPE_SET dropping retired positions (refused by meta.c until live count == 0, D28).
 *   5. SHRINK:   transfer off / DROP_MEMBER (nomove) / REPLICA_REMOVE excess log replicas down to target_replicas.
 *
 * All durable conversion state lives in `v->conv`, so any Raft leader change resumes cleanly from metadata.
 * Pauses before the next key / step while `cluster_versions_mixed()` is true (E38).
 *
 * Crash points (server/crashpt.h): conv-grow, conv-record, conv-drop.
 * Planted mutants (TSDB_CLUSTER_MUTANT under TSDB_TEST_HOOKS):
 *   - dropearly:   attempts DROP / STRIPE_SET drop before old-object count == 0 in REENCODE.
 *   - shrinkfirst: runs log-replica SHRINK before RECORD/DROP when nrep > target_replicas.
 */
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cluster_int.h"
#include "crashpt.h"
#include "credund.h"
#include "place.h"

#define MS UINT64_C(1000000)
#define STEP_NS (1000 * MS)
#define ATTACH_WAIT_NS (300 * MS)

enum { CRD_MUT_NONE = 0, CRD_MUT_DROPEARLY = 1, CRD_MUT_SHRINKFIRST = 2 };

static int crd_mutant(void)
{
#ifdef TSDB_TEST_HOOKS
    static int m = -1;
    if (m < 0) {
        const char *e = getenv("TSDB_CLUSTER_MUTANT");
        m = !e                            ? CRD_MUT_NONE
            : !strcmp(e, "dropearly")     ? CRD_MUT_DROPEARLY
            : !strcmp(e, "shrinkfirst")   ? CRD_MUT_SHRINKFIRST
                                          : CRD_MUT_NONE;
        if (m != CRD_MUT_NONE)
            cl_say("credund MUTANT %s (tests only)", e);
    }
    return m;
#else
    return CRD_MUT_NONE;
#endif
}

struct crd_track {
    uint32_t vol;
    uint8_t phase;
    uint32_t nrep, nisr, nstripe;
    uint64_t epoch, sv, live_ep, live_sv_at;
    uint64_t entered_ns;
    uint64_t sent_ns;
    uint64_t live_sent_ns;
    int mixed_said;
};

static struct crd_track *g_tr;
static uint32_t g_ntr, g_captr;
static uint64_t g_term;

static uint64_t since(uint64_t now, uint64_t t) { return now > t ? now - t : 0; }

static struct crd_track *tr_get(uint32_t vol, uint64_t now)
{
    for (uint32_t i = 0; i < g_ntr; i++)
        if (g_tr[i].vol == vol)
            return &g_tr[i];
    if (g_ntr == g_captr) {
        uint32_t nc = g_captr ? g_captr * 2 : 32;
        struct crd_track *nt = realloc(g_tr, (size_t)nc * sizeof *nt);
        if (!nt)
            return NULL;
        g_tr = nt;
        g_captr = nc;
    }
    struct crd_track *t = &g_tr[g_ntr++];
    memset(t, 0, sizeof *t);
    t->vol = vol;
    t->entered_ns = now;
    return t;
}

static void reply(uint64_t conn, uint32_t reqid, int32_t rc, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));
static void reply(uint64_t conn, uint32_t reqid, int32_t rc, const char *fmt, ...)
{
    char b[320];
    va_list ap;
    va_start(ap, fmt);
    int l = vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    cl_admin_reply(conn, reqid, rc, b, l < 0 ? 0 : (size_t)l < sizeof b ? (size_t)l : sizeof b - 1);
}

static uint32_t vol_active_nstripe(const struct meta_volume *v)
{
    return v->redundancy == META_RED_EC ? (uint32_t)v->ec_k + (uint32_t)v->ec_m : 0;
}

static int member_up(uint32_t node, uint64_t now)
{
    const struct meta_member *mb = meta_member(cl_meta(), node);
    return mb && mb->state == META_UP && cl_heard_fresh(node, now);
}

static int has_u32(const uint32_t *a, uint32_t n, uint32_t x)
{
    for (uint32_t i = 0; i < n; i++)
        if (a[i] == x)
            return 1;
    return 0;
}

static uint32_t count_log_holds(const struct meta_state *sm, uint32_t node)
{
    uint32_t c = 0;
    for (uint32_t i = 0; i < sm->nvolumes; i++)
        if (meta_replica(&sm->volumes[i], node))
            c++;
    return c;
}

static uint32_t count_stripe_holds(const struct meta_state *sm, uint32_t node)
{
    uint32_t c = 0;
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
            if (v->stripe[p] == node)
                c++;
    }
    return c;
}

/* Pick one UP, compute-capable member not already in `v->rep[]`, preferring distinct rack and fewest log replicas. */
static uint32_t pick_extra_log(const struct meta_state *sm, const struct meta_volume *v, uint64_t now)
{
    uint32_t best = 0;
    uint32_t best_rack_col = UINT32_MAX, best_holds = UINT32_MAX;
    int best_fast = -1;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK) || mb->replaced_by != 0)
            continue;
        if (meta_replica(v, mb->id))
            continue;
        if (!cl_heard_fresh(mb->id, now) || !cl_member_log_ok(mb->id, now, NULL))
            continue;
        uint32_t rcol = 0;
        if (mb->rack[0]) {
            for (uint32_t k = 0; k < v->nrep; k++) {
                const struct meta_member *rm = meta_member(sm, v->rep[k].node);
                if (rm && rm->rack[0] && !strcmp(rm->rack, mb->rack))
                    rcol++;
            }
        }
        int fast = (mb->ndisks == 0 || meta_member_has_fast_disk(mb)) ? 1 : 0;
        uint32_t holds = count_log_holds(sm, mb->id);
        if (!best || fast > best_fast ||
            (fast == best_fast && (rcol < best_rack_col || (rcol == best_rack_col && holds < best_holds)))) {
            best = mb->id;
            best_fast = fast;
            best_rack_col = rcol;
            best_holds = holds;
        }
    }
    return best;
}

/* Pick one UP, storage-capable member not already in `cur[0..ncur)`, preferring same chunk disk class as `cur`,
 * fewest rack collisions with `cur`, and fewest stripe holdings overall. */
static uint32_t pick_extra_stripe(const struct meta_state *sm, const uint32_t *cur, uint32_t ncur, uint64_t now)
{
    uint8_t pref_cls = META_DISK_NONE;
    for (uint32_t i = 0; i < ncur && pref_cls == META_DISK_NONE; i++)
        pref_cls = place_member_chunk_class(meta_member(sm, cur[i]));

    uint32_t best = 0;
    int best_cls_match = -1;
    uint32_t best_rack_col = UINT32_MAX, best_holds = UINT32_MAX;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK) || mb->replaced_by != 0)
            continue;
        if (has_u32(cur, ncur, mb->id))
            continue;
        if (!cl_heard_fresh(mb->id, now) || cl_member_chunk_stores(mb->id) == 0 ||
            !cl_member_stripe_ok(mb->id, now, NULL))
            continue;
        uint8_t cls = place_member_chunk_class(mb);
        int cls_match = (pref_cls == META_DISK_NONE || cls == pref_cls) ? 1 : 0;
        uint32_t rcol = 0;
        if (mb->rack[0]) {
            for (uint32_t k = 0; k < ncur; k++) {
                const struct meta_member *sm2 = meta_member(sm, cur[k]);
                if (sm2 && sm2->rack[0] && !strcmp(sm2->rack, mb->rack))
                    rcol++;
            }
        }
        uint32_t holds = count_stripe_holds(sm, mb->id);
        if (!best || cls_match > best_cls_match ||
            (cls_match == best_cls_match && (rcol < best_rack_col || (rcol == best_rack_col && holds < best_holds)))) {
            best = mb->id;
            best_cls_match = cls_match;
            best_rack_col = rcol;
            best_holds = holds;
        }
    }
    return best;
}

/* Count live compute-capable candidates available for `v` (including existing replicas). */
static uint32_t count_log_cands(const struct meta_state *sm, const struct meta_volume *v, uint64_t now)
{
    uint32_t n = v->nrep;
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (meta_replica(v, mb->id))
            continue;
        if (mb->state == META_UP && (mb->flags & META_MF_VOLUMES_OK) && mb->replaced_by == 0 &&
            cl_heard_fresh(mb->id, now) && cl_member_log_ok(mb->id, now, NULL))
            n++;
    }
    return n;
}

/* Count live storage-capable candidates available for `v` (including existing non-OUT stripe holders). */
static uint32_t count_stripe_cands(const struct meta_state *sm, const struct meta_volume *v, uint64_t now)
{
    uint32_t n = 0;
    for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
        const struct meta_member *mb = meta_member(sm, v->stripe[p]);
        if (mb && mb->state < META_OUT)
            n++;
    }
    for (uint32_t i = 0; i < sm->nmembers; i++) {
        const struct meta_member *mb = &sm->members[i];
        if (has_u32(v->stripe, v->nstripe, mb->id))
            continue;
        if (mb->state == META_UP && (mb->flags & META_MF_VOLUMES_OK) && mb->replaced_by == 0 &&
            cl_heard_fresh(mb->id, now) && cl_member_chunk_stores(mb->id) > 0 &&
            cl_member_stripe_ok(mb->id, now, NULL))
            n++;
    }
    return n;
}

static int live_objs_diff(const struct meta_volume *v, const struct cluster_live_objs_report *rep)
{
    if (v->live_epoch != rep->epoch || v->live_sv_at != rep->stripe_version || v->live_overflow != rep->overflow ||
        v->nlive_sv != rep->nsv)
        return 1;
    for (uint32_t i = 0; i < rep->nsv && i < META_LIVE_SV_MAX; i++)
        if (v->live_sv[i].stripe_version != rep->sv[i].stripe_version || v->live_sv[i].count != rep->sv[i].count)
            return 1;
    return 0;
}

static int reencode_done_local(const struct meta_volume *v)
{
    if (v->conv.orig_redundancy != META_RED_EC)
        return 1;
    if (!v->conv.new_stripe_version || v->live_epoch != v->epoch || v->live_overflow ||
        v->live_sv_at < v->conv.new_stripe_version)
        return 0;
    for (uint32_t i = 0; i < v->nlive_sv && i < META_LIVE_SV_MAX; i++)
        if (v->live_sv[i].stripe_version < v->conv.new_stripe_version && v->live_sv[i].count > 0)
            return 0;
    return 1;
}

static int stripe_drop_safe_local(const struct meta_volume *v, uint32_t from)
{
    if (v->live_epoch != v->epoch || v->live_overflow)
        return 0;
    for (uint32_t p = from; p < v->nstripe && p < META_STRIPE_MAX; p++) {
        uint64_t rv = v->stripe_pos[p].retired_ver;
        if (!rv || v->live_sv_at < rv)
            return 0;
        for (uint32_t i = 0; i < v->nlive_sv && i < META_LIVE_SV_MAX; i++)
            if (v->live_sv[i].count > 0 && v->live_sv[i].stripe_version < rv)
                return 0;
    }
    return 1;
}

static void sync_live_objs(const struct meta_volume *v, struct crd_track *t, uint64_t now)
{
    if (!v->leader || !member_up(v->leader, now))
        return;
    struct cluster_live_objs_report rep;
    if (!cl_heard_live_objs(v->leader, v->number, &rep))
        return;
    if (rep.epoch != v->epoch || rep.stripe_version != v->stripe_version)
        return;
    if (!live_objs_diff(v, &rep))
        return;
    if (t->live_sent_ns && since(now, t->live_sent_ns) < STEP_NS)
        return;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_live_objs(&e, v->number, rep.epoch, rep.stripe_version, rep.overflow, rep.sv, rep.nsv);
    if (cl_propose(&e) == 0)
        t->live_sent_ns = now;
}

static void propose_step(uint32_t vol, uint8_t from, uint8_t to, const uint32_t *stripe, uint32_t nstripe)
{
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_conv_step(&e, vol, from, to, stripe, nstripe);
    (void)cl_propose(&e);
}

static void drive_shrink_one(const struct meta_volume *v, struct crd_track *t, uint64_t now, uint32_t keep_min)
{
    /* Prefer removing an excess replica that is already out of ISR, else the last non-leader replica. */
    uint32_t out = 0;
    for (uint32_t i = v->nrep; i-- > 0;) {
        uint32_t nid = v->rep[i].node;
        if (nid != v->leader && !meta_volume_in_isr(v, nid)) {
            out = nid;
            break;
        }
    }
    if (!out) {
        for (uint32_t i = v->nrep; i-- > 0;) {
            if (v->rep[i].node != v->leader) {
                out = v->rep[i].node;
                break;
            }
        }
    }
    if (!out)
        out = v->rep[v->nrep - 1].node;

    if (out == v->leader) {
        if (cl_promo_running(v->number) || ctr_running(v->number))
            return;
        uint32_t tgt = 0;
        for (uint32_t i = 0; i < v->nisr; i++) {
            if (v->isr[i] != out && member_up(v->isr[i], now)) {
                tgt = v->isr[i];
                break;
            }
        }
        if (tgt) {
            t->sent_ns = now;
            (void)ctr_move_start(v->number, tgt, now);
        }
        return;
    }

    if (meta_volume_in_isr(v, out)) {
        if (!v->leader)
            return;
        uint32_t min_k = keep_min;
        if (min_k < v->min_isr)
            min_k = v->min_isr;
        if (min_k > 0x7fu)
            min_k = 0x7fu;
        struct cluster_volcmd cmd = {
            .kind = CL_VOLCMD_DROP_MEMBER,
            .volume = v->number,
            .epoch = v->epoch,
            .node = out,
            .arg = (cl_term() << 8) | 0x80u | (min_k & 0x7fu),
        };
        t->sent_ns = now;
        (void)cl_volcmd_send(v->leader, &cmd);
        return;
    }

    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_replica_remove(&e, v->number, v->epoch, out);
    t->sent_ns = now;
    (void)cl_propose(&e);
}

static void drive_vol(const struct meta_state *sm, const struct meta_volume *v, uint64_t now)
{
    struct crd_track *t = tr_get(v->number, now);
    if (!t)
        return;

    /* Reset retry throttle immediately whenever the committed volume state advances. */
    if (t->phase != v->conv.phase || t->nrep != v->nrep || t->nisr != v->nisr || t->nstripe != v->nstripe ||
        t->epoch != v->epoch || t->sv != v->stripe_version || t->live_ep != v->live_epoch ||
        t->live_sv_at != v->live_sv_at) {
        if (t->phase != v->conv.phase)
            t->entered_ns = now;
        t->phase = v->conv.phase;
        t->nrep = v->nrep;
        t->nisr = v->nisr;
        t->nstripe = v->nstripe;
        t->epoch = v->epoch;
        t->sv = v->stripe_version;
        t->live_ep = v->live_epoch;
        t->live_sv_at = v->live_sv_at;
        t->sent_ns = 0;
        t->live_sent_ns = 0;
    }

    /* Always keep VOLUME_LIVE_OBJS synced when converting (REENCODE/DROP) or clearing cancelled mixed geometry. */
    if (v->conv.phase >= META_CONV_REENCODE || v->mixed_redundancy == META_RED_EC ||
        v->nstripe > vol_active_nstripe(v)) {
        sync_live_objs(v, t, now);
    }

    /* Idle volume cleanup after a cancelled REENCODE: first drive re-encode/unsettle of leftover objects in
     * [mixed_sv_lo .. mixed_sv_hi] until VOLUME_LIVE_OBJS clears mixed_redundancy, then drop retired stripe slots. */
    if (v->conv.phase == META_CONV_NONE) {
        if (t->sent_ns && since(now, t->sent_ns) < STEP_NS)
            return;
        if (v->mixed_redundancy == META_RED_EC) {
            if (!cluster_versions_mixed() && v->leader && member_up(v->leader, now)) {
                uint32_t mode = v->redundancy == META_RED_EC ? 1u : 2u;
                struct cluster_volcmd cmd = {
                    .kind = CL_VOLCMD_CONV_WORK,
                    .volume = v->number,
                    .epoch = v->epoch,
                    .node = mode,
                    .arg = v->stripe_version,
                };
                t->sent_ns = now;
                (void)cl_volcmd_send(v->leader, &cmd);
            }
            return;
        }
        uint32_t active = vol_active_nstripe(v);
        if (v->mixed_redundancy == META_RED_REPLICAS && v->nstripe > active && stripe_drop_safe_local(v, active)) {
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_stripe_drop(&e, v->number, active, v->stripe_version);
            t->sent_ns = now;
            (void)cl_propose(&e);
        }
        return;
    }

    if (t->sent_ns && since(now, t->sent_ns) < STEP_NS)
        return;

    /* E38: pause conversion before the next key/step while rolling-upgrade versions are mixed. */
    if (cluster_versions_mixed()) {
        if (!t->mixed_said) {
            cl_say("volume %" PRIu32 ": redundancy conversion paused in phase %s (cluster versions mixed, E38)",
                   v->number, meta_conv_phase_name(v->conv.phase));
            t->mixed_said = 1;
        }
        t->sent_ns = now;
        return;
    }
    t->mixed_said = 0;

    int mut = crd_mutant();

    /* Planted mutant `shrinkfirst`: runs log-replica SHRINK before RECORD/DROP when excess replicas exist. */
    if (mut == CRD_MUT_SHRINKFIRST && v->conv.phase < META_CONV_SHRINK && v->nrep > v->conv.target_replicas) {
        drive_shrink_one(v, t, now, v->conv.target_replicas);
        return;
    }

    switch (v->conv.phase) {
    case META_CONV_GROW: {
        crash_point(CP_CONV_GROW);
        if (v->nrep < v->conv.target_replicas) {
            /* Add one learner at a time (wait until any existing learner joins ISR) to keep catch-up bounded. */
            int has_learner = 0;
            for (uint32_t i = 0; i < v->nrep; i++)
                if (v->rep[i].learner)
                    has_learner = 1;
            if (has_learner)
                return;
            uint32_t add = pick_extra_log(sm, v, now);
            if (!add) {
                t->sent_ns = now;
                return;
            }
            uint64_t rid = cset_fresh_rid(v);
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_replica_add(&e, v->number, v->epoch, add, rid, 0);
            t->sent_ns = now;
            if (cl_propose(&e) == 0)
                cl_say("volume %" PRIu32 ": conversion GROW adding learner node %" PRIu32 " (rid %016" PRIx64
                       ", target %" PRIu32 ")",
                       v->number, add, rid, v->conv.target_replicas);
            return;
        }
        if (v->nisr < v->conv.target_replicas)
            return; /* wait for added learner(s) to join ISR */
        t->sent_ns = now;
        propose_step(v->number, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
        cl_say("volume %" PRIu32 ": conversion GROW -> RECORD (nrep %" PRIu32 ", nisr %" PRIu32 ")", v->number, v->nrep,
               v->nisr);
        return;
    }
    case META_CONV_RECORD: {
        crash_point(CP_CONV_RECORD);
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
            if (v->stripe_pos[p].state == META_STRIPE_FILLING)
                return;
        if (v->conv.target_redundancy == META_RED_EC) {
            uint32_t need = (uint32_t)v->conv.target_k + (uint32_t)v->conv.target_m;
            if (need > v->nstripe) {
                uint32_t stripe[META_STRIPE_MAX];
                for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
                    stripe[p] = v->stripe[p];
                for (uint32_t p = v->nstripe; p < need; p++) {
                    uint32_t pick = pick_extra_stripe(sm, stripe, p, now);
                    if (!pick) {
                        t->sent_ns = now;
                        return;
                    }
                    stripe[p] = pick;
                }
                t->sent_ns = now;
                propose_step(v->number, META_CONV_RECORD, META_CONV_REENCODE, stripe, need);
                cl_say("volume %" PRIu32 ": conversion RECORD -> REENCODE (ec=%u+%u, %" PRIu32 " stripe positions)",
                       v->number, v->conv.target_k, v->conv.target_m, need);
                return;
            }
            t->sent_ns = now;
            propose_step(v->number, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
            cl_say("volume %" PRIu32 ": conversion RECORD -> REENCODE (ec=%u+%u)", v->number, v->conv.target_k,
                   v->conv.target_m);
            return;
        }
        t->sent_ns = now;
        propose_step(v->number, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
        cl_say("volume %" PRIu32 ": conversion RECORD -> REENCODE (replicas=%" PRIu32 ")", v->number,
               v->conv.target_replicas);
        return;
    }
    case META_CONV_REENCODE: {
        /* D27: replicas -> ec has nothing to re-encode; ensure the volume leader attaches its EC tier cleanly via
         * planned transfer (promote_fn / demote_fn write requires=tier,ec without indeterminate writes), confirmed by
         * HBS_LIVE_OBJS from the leader at (v->epoch, v->stripe_version). */
        if (v->conv.orig_redundancy == META_RED_REPLICAS && v->conv.target_redundancy == META_RED_EC) {
            struct cluster_live_objs_report rep;
            int attached = v->leader && cl_heard_live_objs(v->leader, v->number, &rep) && rep.epoch == v->epoch &&
                           rep.stripe_version == v->stripe_version;
            if (attached) {
                t->sent_ns = now;
                propose_step(v->number, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
                cl_say("volume %" PRIu32 ": conversion REENCODE -> DROP (replicas -> ec=%u+%u tier attached)", v->number,
                       v->ec_k, v->ec_m);
                return;
            }
            /* Wait briefly so followers finish their ~30 ms EC reopen before we transfer leadership onto one. */
            if (since(now, t->entered_ns) < ATTACH_WAIT_NS)
                return;
            if (v->nisr >= 2 && !cl_promo_running(v->number) && !ctr_running(v->number)) {
                uint32_t tgt = 0;
                for (uint32_t i = 0; i < v->nisr; i++) {
                    if (v->isr[i] != v->leader && member_up(v->isr[i], now)) {
                        tgt = v->isr[i];
                        break;
                    }
                }
                if (tgt) {
                    t->sent_ns = now;
                    const char *no = ctr_move_start(v->number, tgt, now);
                    if (!no)
                        cl_say("volume %" PRIu32 ": conversion replicas -> ec transferring leader %" PRIu32
                               " -> %" PRIu32 " to attach EC tier",
                               v->number, v->leader, tgt);
                }
            }
            return;
        }

        if (reencode_done_local(v)) {
            t->sent_ns = now;
            propose_step(v->number, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
            cl_say("volume %" PRIu32 ": conversion REENCODE -> DROP (0 old objects below sv %" PRIu64 ")", v->number,
                   v->conv.new_stripe_version);
            return;
        }

        /* Planted mutant `dropearly`: attempts DROP / STRIPE_SET drop before old-object count reaches 0. */
        if (mut == CRD_MUT_DROPEARLY) {
            t->sent_ns = now;
            uint32_t active = vol_active_nstripe(v);
            if (v->nstripe > active) {
                struct meta_entry ed;
                meta_entry_init(&ed);
                meta_op_stripe_drop(&ed, v->number, active, v->stripe_version);
                (void)cl_propose(&ed);
            }
            propose_step(v->number, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
            cl_say("volume %" PRIu32 ": MUTANT dropearly proposed premature DROP before old-object count == 0",
                   v->number);
            return;
        }

        if (v->leader && member_up(v->leader, now)) {
            uint32_t mode = v->conv.target_redundancy == META_RED_EC ? 1u : 2u;
            struct cluster_volcmd cmd = {
                .kind = CL_VOLCMD_CONV_WORK,
                .volume = v->number,
                .epoch = v->epoch,
                .node = mode,
                .arg = v->conv.new_stripe_version,
            };
            t->sent_ns = now;
            (void)cl_volcmd_send(v->leader, &cmd);
        }
        return;
    }
    case META_CONV_DROP: {
        crash_point(CP_CONV_DROP);
        uint32_t active = vol_active_nstripe(v);
        if (v->nstripe > active) {
            if (!stripe_drop_safe_local(v, active))
                return; /* wait for current-epoch VOLUME_LIVE_OBJS confirmation (e.g. after volume failover) */
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_stripe_drop(&e, v->number, active, v->stripe_version);
            t->sent_ns = now;
            if (cl_propose(&e) == 0)
                cl_say("volume %" PRIu32 ": conversion DROP retiring stripe positions %" PRIu32 "..%" PRIu32
                       " (sv %" PRIu64 ")",
                       v->number, active, v->nstripe - 1, v->stripe_version);
            return;
        }
        t->sent_ns = now;
        propose_step(v->number, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
        cl_say("volume %" PRIu32 ": conversion DROP -> SHRINK (nstripe %" PRIu32 ")", v->number, v->nstripe);
        return;
    }
    case META_CONV_SHRINK: {
        if (v->nrep > v->conv.target_replicas) {
            drive_shrink_one(v, t, now, v->conv.target_replicas);
            return;
        }
        t->sent_ns = now;
        propose_step(v->number, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
        cl_say("volume %" PRIu32 ": conversion SHRINK -> DONE (redundancy %s, nrep %" PRIu32 ", nstripe %" PRIu32 ")",
               v->number, v->redundancy == META_RED_EC ? "ec" : "replicas", v->nrep, v->nstripe);
        return;
    }
    default:
        return;
    }
}

void credund_duty(uint64_t now)
{
    if (!cl_leading() || cl_term() != g_term) {
        g_ntr = 0;
        g_term = cl_leading() ? cl_term() : 0;
        if (!g_term)
            return;
    }
    const struct meta_state *sm = cl_meta();
    for (uint32_t i = 0; i < g_ntr;) {
        const struct meta_volume *v = meta_volume(sm, g_tr[i].vol);
        if (!v || (v->conv.phase == META_CONV_NONE && v->mixed_redundancy != META_RED_EC &&
                   v->nstripe <= vol_active_nstripe(v))) {
            g_tr[i] = g_tr[--g_ntr];
            continue;
        }
        i++;
    }
    for (uint32_t i = 0; i < sm->nvolumes && cl_leading(); i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        if (v->conv.phase == META_CONV_NONE && v->mixed_redundancy != META_RED_EC &&
            v->nstripe <= vol_active_nstripe(v))
            continue;
        drive_vol(sm, v, now);
    }
}

void credund_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t vol = 0, flags = 0;
    struct meta_redundancy_target target;
    if (cluster_volume_redundancy_decode(body, n, &vol, &target, &flags) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "volume redundancy: a body of 16 bytes is expected (got %zu)", n);
        return;
    }
    if (flags & ~CLUSTER_REDUND_CANCEL) {
        reply(conn, reqid, META_ERR_INVALID, "volume redundancy: unknown flags 0x%" PRIx32, flags);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_volume *v = meta_volume(sm, vol);
    if (!v) {
        reply(conn, reqid, META_ERR_NOTFOUND, "volume redundancy: volume %" PRIu32 " does not exist", vol);
        return;
    }
    if (v->kind != META_VOL_REPLICATED) {
        reply(conn, reqid, META_ERR_INVALID, "volume redundancy: volume %" PRIu32 " (%s) is not a replicated volume",
              vol, v->name);
        return;
    }

    if (flags & CLUSTER_REDUND_CANCEL) {
        if (v->conv.phase == META_CONV_NONE) {
            reply(conn, reqid, META_ERR_STATE, "volume redundancy --cancel: no conversion is running on volume %" PRIu32,
                  vol);
            return;
        }
        if (v->conv.phase >= META_CONV_DROP) {
            reply(conn, reqid, META_ERR_STATE,
                  "volume redundancy --cancel: too late on volume %" PRIu32 " (phase %s; --cancel only before drop)",
                  vol, meta_conv_phase_name(v->conv.phase));
            return;
        }
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_volume_conv_cancel(&e, vol);
        char ok[160];
        snprintf(ok, sizeof ok, "volume %" PRIu32 " (%s): redundancy conversion cancelled (was phase %s)\n", vol,
                 v->name, meta_conv_phase_name(v->conv.phase));
        if (cl_propose_reply(&e, conn, reqid, 0, ok) == 0)
            cl_say("volume %" PRIu32 " (%s): redundancy conversion cancel proposed (was phase %s)", vol, v->name,
                   meta_conv_phase_name(v->conv.phase));
        return;
    }

    if (cluster_versions_mixed()) {
        reply(conn, reqid, META_ERR_STATE,
              "volume redundancy: refused while cluster runs mixed versions (finish upgrade first, E38)");
        return;
    }
    if (v->conv.phase != META_CONV_NONE) {
        reply(conn, reqid, META_ERR_EXISTS,
              "volume redundancy: volume %" PRIu32 " (%s) already has a conversion running (phase %s)", vol, v->name,
              meta_conv_phase_name(v->conv.phase));
        return;
    }
    if (v->mixed_redundancy != META_RED_REPLICAS) {
        reply(conn, reqid, META_ERR_STATE,
              "volume redundancy: volume %" PRIu32 " (%s) still has leftover objects from a cancelled conversion", vol,
              v->name);
        return;
    }
    for (uint32_t i = 0; i < v->nrep; i++) {
        if (v->rep[i].replaces || v->rep[i].learner) {
            reply(conn, reqid, META_ERR_STATE,
                  "volume redundancy: volume %" PRIu32 " (%s) has a replica move or learner in flight", vol, v->name);
            return;
        }
    }
    for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
        if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
            reply(conn, reqid, META_ERR_STATE,
                  "volume redundancy: volume %" PRIu32 " (%s) has a stripe fill in flight on pos %" PRIu32, vol,
                  v->name, p);
            return;
        }
    }

    uint64_t now = cl_now_ns();
    uint32_t trep = 0;
    char desc[64];
    if (target.redundancy == META_RED_EC) {
        if (target.ec_k < 1 || target.ec_k > META_EC_K_MAX || target.ec_m < 1 || target.ec_m > META_EC_M_MAX) {
            reply(conn, reqid, META_ERR_INVALID, "volume redundancy: invalid EC geometry %u+%u", target.ec_k,
                  target.ec_m);
            return;
        }
        trep = target.replicas ? target.replicas : (uint32_t)target.ec_m + 1;
        if (trep != (uint32_t)target.ec_m + 1) {
            reply(conn, reqid, META_ERR_INVALID, "volume redundancy: ec=%u+%u requires %" PRIu32 " log replicas",
                  target.ec_k, target.ec_m, (uint32_t)target.ec_m + 1);
            return;
        }
        if (v->redundancy == META_RED_EC && v->ec_k == target.ec_k && v->ec_m == target.ec_m) {
            reply(conn, reqid, META_ERR_INVALID, "volume redundancy: volume %" PRIu32 " (%s) is already ec=%u+%u", vol,
                  v->name, target.ec_k, target.ec_m);
            return;
        }
        uint32_t log_cands = count_log_cands(sm, v, now);
        if (log_cands < trep) {
            reply(conn, reqid, CLUSTER_ERR_FEW,
                  "volume redundancy: only %" PRIu32 " live compute members available (need %" PRIu32 " for ec=%u+%u)",
                  log_cands, trep, target.ec_k, target.ec_m);
            return;
        }
        uint32_t need_stripe = (uint32_t)target.ec_k + (uint32_t)target.ec_m;
        uint32_t stripe_cands = count_stripe_cands(sm, v, now);
        if (stripe_cands < need_stripe) {
            reply(conn, reqid, CLUSTER_ERR_FEW,
                  "volume redundancy: only %" PRIu32 " live storage members available (need %" PRIu32 " for ec=%u+%u)",
                  stripe_cands, need_stripe, target.ec_k, target.ec_m);
            return;
        }
        target.replicas = trep;
        snprintf(desc, sizeof desc, "ec=%u+%u", target.ec_k, target.ec_m);
    } else if (target.redundancy == META_RED_REPLICAS) {
        if (target.ec_k || target.ec_m) {
            reply(conn, reqid, META_ERR_INVALID, "volume redundancy: replicas target must have k=0, m=0");
            return;
        }
        trep = target.replicas ? target.replicas : sm->set.replication;
        if (trep < 1 || trep > META_REPLICAS_MAX) {
            reply(conn, reqid, META_ERR_INVALID, "volume redundancy: replicas %" PRIu32 " out of range (1..%d)", trep,
                  META_REPLICAS_MAX);
            return;
        }
        if (v->redundancy == META_RED_REPLICAS && v->nrep == trep && v->nstripe == 0) {
            reply(conn, reqid, META_ERR_INVALID, "volume redundancy: volume %" PRIu32 " (%s) is already replicas=%" PRIu32,
                  vol, v->name, trep);
            return;
        }
        uint32_t log_cands = count_log_cands(sm, v, now);
        if (log_cands < trep) {
            reply(conn, reqid, CLUSTER_ERR_FEW,
                  "volume redundancy: only %" PRIu32 " live compute members available (need %" PRIu32 " for replicas=%"
                  PRIu32 ")",
                  log_cands, trep, trep);
            return;
        }
        target.replicas = trep;
        snprintf(desc, sizeof desc, "replicas=%" PRIu32, trep);
    } else {
        reply(conn, reqid, META_ERR_INVALID, "volume redundancy: unknown redundancy kind %u", target.redundancy);
        return;
    }

    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_redundancy(&e, vol, &target, v->epoch);
    char ok[320];
    snprintf(ok, sizeof ok, "volume %" PRIu32 " (%s): redundancy conversion to %s started (runs in background)\n", vol,
             v->name, desc);
    if (cl_propose_reply(&e, conn, reqid, 0, ok) == 0)
        cl_say("volume %" PRIu32 " (%s): redundancy conversion to %s proposed (epoch %" PRIu64 ")", vol, v->name, desc,
               v->epoch);
}
