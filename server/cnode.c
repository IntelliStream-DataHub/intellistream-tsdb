/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 operator admin handlers and duty on the Raft leader (R9 plan D2, D22; cluster_int.h; track r9-ops).
 *
 * Handles CLUSTER_OP_NODE_HOLD (15), CLUSTER_OP_NODE_OUT (16), CLUSTER_OP_NODE_DRAIN (17),
 * CLUSTER_OP_NODE_REPLACE (18), CLUSTER_OP_NODE_DESTROYED (19), and CLUSTER_OP_VOLUME_MOVE_POS (20).
 * Runs on the cluster thread only, after cluster.c verifies leadership (returning CLUSTER_ERR_NOTLEADER on a
 * follower so the CLI follows once). Proposals are answered once committed and applied on the Raft leader via
 * cl_propose_reply().
 *
 * cnode_duty() runs once a second on the settled Raft leader (called from place_duty):
 *   - transitions a META_DRAINING member to META_OUT once it holds zero log replicas, zero stripe positions, and no
 *     in-flight log moves;
 *   - completes an active `node replace OLD NEW` (`replaced_by != 0`) once OLD holds zero log replicas, zero stripe
 *     positions, and no in-flight log moves (clears `replaced_by` and marks OLD META_OUT if still live/DOWN/DRAINING).
 *
 * Planted mutants (TSDB_CLUSTER_MUTANT under TSDB_TEST_HOOKS):
 *   - drainleaves:  marks a DRAINING member OUT even while it still holds a log replica, stripe position, or filling prev.
 *   - replaceclear: clears replaced_by (and marks OLD OUT) even while OLD still holds a log replica or stripe position.
 */
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cluster_int.h"
#include "disks.h"

static int cnode_mutant(const char *name)
{
#ifdef TSDB_TEST_HOOKS
    const char *m = getenv("TSDB_CLUSTER_MUTANT");
    return m && !strcmp(m, name);
#else
    (void)name;
    return 0;
#endif
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

static void node_positions(const struct meta_state *sm, uint32_t node, uint32_t *nlog, uint32_t *nstripe,
                           uint32_t *ninflight, uint32_t *nprev)
{
    uint32_t nl = 0, ns = 0, ni = 0, np = 0;
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        for (uint32_t k = 0; k < v->nrep; k++) {
            if (v->rep[k].node == node)
                nl++;
            if (v->rep[k].replaces == node)
                ni++;
        }
        if (v->nstripe > 0) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                if (v->stripe[p] == node)
                    ns++;
                if (v->stripe_pos[p].state == META_STRIPE_FILLING && v->stripe_pos[p].prev == node)
                    np++;
            }
        }
    }
    *nlog = nl;
    *nstripe = ns;
    *ninflight = ni;
    *nprev = np;
}

void cnode_hold_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0, for_s = 0, flags = 0;
    if (cluster_node_hold_decode(body, n, &node, &for_s, &flags) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node hold: a body of 12 bytes is expected (got %zu)", n);
        return;
    }
    if (flags & ~CLUSTER_HOLD_RELEASE) {
        reply(conn, reqid, META_ERR_INVALID, "node hold: unknown flags 0x%" PRIx32, flags);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "node hold: unknown node %" PRIu32, node);
        return;
    }
    int rel = (flags & CLUSTER_HOLD_RELEASE) != 0;
    if (mb->state == META_OUT || mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "node hold: node %" PRIu32 " is %s (cannot %s a hold)", node,
              meta_member_state_name(mb->state), rel ? "release" : "set");
        return;
    }
    if (rel) {
        if (for_s != 0) {
            reply(conn, reqid, META_ERR_INVALID, "node hold: --release and --for cannot be combined");
            return;
        }
    } else if (for_s == 0) {
        reply(conn, reqid, META_ERR_INVALID, "node hold: duration must be at least 1 s (expected 1 to 86400 s)");
        return;
    } else if ((uint64_t)for_s * 1000ull > META_HOLD_MAX_MS) {
        reply(conn, reqid, META_ERR_INVALID, "node hold: duration %" PRIu32 " s exceeds 1 day (expected 1 to 86400 s)",
              for_s);
        return;
    }
    uint64_t wms = cl_wall_ms();
    uint64_t until_ms = rel ? 0 : wms + (uint64_t)for_s * 1000ull;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_node_hold(&e, node, until_ms, wms);
    char ok[160];
    if (rel)
        snprintf(ok, sizeof ok, "node %" PRIu32 ": hold released", node);
    else
        snprintf(ok, sizeof ok, "node %" PRIu32 ": held against automatic OUT for %" PRIu32 " s", node, for_s);
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0)
        cl_say("%s", ok);
}

void cnode_out_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    if (cluster_node_op_decode(body, n, &node) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node out: a body of 4 bytes is expected (got %zu)", n);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "node out: unknown node %" PRIu32, node);
        return;
    }
    if (mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "node out: node %" PRIu32 " is destroyed (final)", node);
        return;
    }
    if (mb->state == META_OUT) {
        reply(conn, reqid, META_ERR_STATE, "node out: node %" PRIu32 " is already out", node);
        return;
    }
    uint64_t wms = cl_wall_ms();
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_state(&e, node, META_OUT, mb->incarnation, META_DOWN_NONE, 0, wms);
    char ok[160];
    snprintf(ok, sizeof ok, "node %" PRIu32 ": marked out (reason: operator)", node);
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0)
        cl_say("%s", ok);
}

void cnode_drain_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0, flags = 0;
    if (cluster_node_drain_decode(body, n, &node, &flags) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node drain: a body of 4 or 8 bytes is expected (got %zu)", n);
        return;
    }
    if (flags & ~CLUSTER_DRAIN_CANCEL) {
        reply(conn, reqid, META_ERR_INVALID, "node drain: unknown flags 0x%" PRIx32, flags);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "node drain: unknown node %" PRIu32, node);
        return;
    }
    int cancel = (flags & CLUSTER_DRAIN_CANCEL) != 0;
    uint64_t now = cl_now_ns();
    uint64_t wms = cl_wall_ms();
    if (cancel) {
        if (mb->state != META_DRAINING) {
            reply(conn, reqid, META_ERR_STATE, "node drain --cancel: node %" PRIu32 " is %s, not draining", node,
                  meta_member_state_name(mb->state));
            return;
        }
        if (!cl_heard_fresh(node, now)) {
            reply(conn, reqid, META_ERR_STATE, "node drain --cancel: node %" PRIu32 " is not heard (cannot return up)",
                  node);
            return;
        }
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_member_state(&e, node, META_UP, mb->incarnation, META_DOWN_NONE, 0, wms);
        char ok[160];
        snprintf(ok, sizeof ok, "node %" PRIu32 ": drain cancelled (state: up)", node);
        if (cl_propose_reply(&e, conn, reqid, node, ok) == 0)
            cl_say("%s", ok);
        return;
    }
    if (mb->state == META_DRAINING) {
        reply(conn, reqid, META_ERR_STATE, "node drain: node %" PRIu32 " is already draining", node);
        return;
    }
    if (mb->state != META_UP) {
        reply(conn, reqid, META_ERR_STATE,
              "node drain: node %" PRIu32 " is %s (only an up member can be drained; use node out for a down member)",
              node, meta_member_state_name(mb->state));
        return;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_state(&e, node, META_DRAINING, mb->incarnation, META_DOWN_NONE, 0, wms);
    char ok[160];
    snprintf(ok, sizeof ok, "node %" PRIu32 ": draining (positions will move off, then node goes out)", node);
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0)
        cl_say("%s", ok);
}

void cnode_replace_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t old_node = 0, new_node = 0, flags = 0;
    if (cluster_node_replace_decode_ex(body, n, &old_node, &new_node, &flags) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node replace: a body of 8 or 12 bytes is expected (got %zu)", n);
        return;
    }
    if (flags & ~CLUSTER_REPLACE_CANCEL) {
        reply(conn, reqid, META_ERR_INVALID, "node replace: unknown flags 0x%" PRIx32, flags);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *old_mb = meta_member(sm, old_node);
    if (!old_mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "node replace: unknown old node %" PRIu32, old_node);
        return;
    }
    if (flags & CLUSTER_REPLACE_CANCEL) {
        if (new_node != 0) {
            reply(conn, reqid, META_ERR_INVALID, "node replace --cancel: NEW must not be given with --cancel");
            return;
        }
        if (old_mb->replaced_by == 0) {
            reply(conn, reqid, META_ERR_STATE, "node replace --cancel: node %" PRIu32 " has no active replace",
                  old_node);
            return;
        }
        uint64_t wms = cl_wall_ms();
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_node_replace(&e, old_node, 0);
        if (old_mb->state < META_OUT && old_mb->hold_until_ms > 0)
            meta_op_node_hold(&e, old_node, 0, wms);
        char ok[160];
        snprintf(ok, sizeof ok, "node %" PRIu32 ": replace by node %" PRIu32 " cancelled", old_node,
                 old_mb->replaced_by);
        if (cl_propose_reply(&e, conn, reqid, old_node, ok) == 0)
            cl_say("%s", ok);
        return;
    }
    if (new_node == 0) {
        reply(conn, reqid, META_ERR_INVALID, "node replace: new node id must be above 0");
        return;
    }
    if (old_node == new_node) {
        reply(conn, reqid, META_ERR_INVALID, "node replace: old node %" PRIu32 " and new node %" PRIu32 " must differ",
              old_node, new_node);
        return;
    }
    if (old_mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "node replace: old node %" PRIu32 " is destroyed", old_node);
        return;
    }
    if (old_mb->replaced_by != 0) {
        reply(conn, reqid, META_ERR_STATE, "node replace: old node %" PRIu32 " is already being replaced by node %" PRIu32,
              old_node, old_mb->replaced_by);
        return;
    }
    const struct meta_member *new_mb = meta_member(sm, new_node);
    if (!new_mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "node replace: unknown new node %" PRIu32, new_node);
        return;
    }
    uint64_t now = cl_now_ns();
    if (new_mb->state != META_UP) {
        reply(conn, reqid, META_ERR_STATE, "node replace: new node %" PRIu32 " is %s (must be up)", new_node,
              meta_member_state_name(new_mb->state));
        return;
    }
    if (!cl_heard_fresh(new_node, now)) {
        reply(conn, reqid, META_ERR_STATE, "node replace: new node %" PRIu32 " is not heard", new_node);
        return;
    }
    if (!(new_mb->flags & META_MF_VOLUMES_OK)) {
        reply(conn, reqid, META_ERR_INVALID, "node replace: new node %" PRIu32 " does not accept volumes (volumes_ok=0)",
              new_node);
        return;
    }
    if (new_mb->replaced_by != 0) {
        reply(conn, reqid, META_ERR_STATE, "node replace: new node %" PRIu32 " is itself being replaced by node %" PRIu32,
              new_node, new_mb->replaced_by);
        return;
    }
    uint32_t adopt_cs = 0;
    uint32_t adopt_target = cl_member_adopt_info(new_node, &adopt_cs);
    if (adopt_target != 0) {
        if (adopt_target != old_node) {
            reply(conn, reqid, META_ERR_INVALID,
                  "node replace: new node %" PRIu32 " is pending adoption of node %" PRIu32 ", not node %" PRIu32,
                  new_node, adopt_target, old_node);
            return;
        }
        int mut_heard = 0;
#ifdef TSDB_TEST_HOOKS
        mut_heard = (disks_mutant_env() == DISKS_MUT_ADOPT_HEARD);
#endif
        if (!mut_heard && (cl_heard_fresh(old_node, now) || old_mb->state == META_UP)) {
            reply(conn, reqid, META_ERR_STATE,
                  "node replace: old node %" PRIu32 " is still heard (state %s): refusing adoption by node %" PRIu32,
                  old_node, meta_member_state_name(old_mb->state), new_node);
            return;
        }
    }
    /* NEW must hold NO position (log or stripe) of any volume OLD holds; if OLD holds stripe positions, NEW must have
     * at least one chunk store UP (or pending adoption for OLD). */
    int need_stores = 0;
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        int old_log = meta_replica(v, old_node) != NULL;
        int old_stripe = 0;
        if (v->nstripe > 0) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
                if (v->stripe[p] == old_node)
                    old_stripe = 1;
        }
        if (!old_log && !old_stripe)
            continue;
        if (old_stripe)
            need_stores = 1;
        int new_log = meta_replica(v, new_node) != NULL;
        int new_stripe = 0;
        if (v->nstripe > 0) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
                if (v->stripe[p] == new_node)
                    new_stripe = 1;
        }
        if (new_log || new_stripe) {
            reply(conn, reqid, META_ERR_INVALID,
                  "node replace: new node %" PRIu32 " already holds a %s of volume %" PRIu32 " (%s) held by node %" PRIu32,
                  new_node, new_log ? "log replica" : "stripe position", v->number, v->name, old_node);
            return;
        }
    }
    uint32_t new_cs = cl_member_chunk_stores(new_node);
    if (adopt_target == old_node && adopt_cs > new_cs)
        new_cs = adopt_cs;
    if (need_stores && new_cs == 0) {
        reply(conn, reqid, META_ERR_INVALID,
              "node replace: old node %" PRIu32 " holds stripe positions, but new node %" PRIu32
              " has no chunk stores up",
              old_node, new_node);
        return;
    }
    uint64_t wms = cl_wall_ms();
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_node_replace(&e, old_node, new_node);
    /* Hold OLD against automatic OUT while replace runs (when OLD is not already OUT), so Raft failover resumes the
     * replace directly onto NEW rather than scattering positions to arbitrary servers (D22). If OLD races to META_OUT
     * before apply, NODE_REPLACE still commits (op 0 META_OK) while NODE_HOLD returns META_ERR_STATE (benign: OUT
     * needs no hold, and apply_entry logs any non-OK secondary op). */
    if (old_mb->state < META_OUT)
        meta_op_node_hold(&e, old_node, wms + META_HOLD_MAX_MS, wms);
    char ok[192];
    snprintf(ok, sizeof ok, "node %" PRIu32 ": replacing with node %" PRIu32 " (positions will move directly to node %" PRIu32 ")",
             old_node, new_node, new_node);
    if (cl_propose_reply(&e, conn, reqid, old_node, ok) == 0)
        cl_say("%s", ok);
}

void cnode_destroyed_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    if (cluster_node_op_decode(body, n, &node) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node destroyed: a body of 4 bytes is expected (got %zu)", n);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "node destroyed: unknown node %" PRIu32, node);
        return;
    }
    if (mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "node destroyed: node %" PRIu32 " is already destroyed", node);
        return;
    }
    if (mb->state != META_OUT) {
        reply(conn, reqid, META_ERR_STATE,
              "node destroyed: node %" PRIu32 " is %s, not out (only an out member may be declared destroyed)", node,
              meta_member_state_name(mb->state));
        return;
    }
    uint64_t wms = cl_wall_ms();
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_state(&e, node, META_DESTROYED, mb->incarnation, META_DOWN_NONE, 0, wms);
    char ok[160];
    snprintf(ok, sizeof ok, "node %" PRIu32 ": declared destroyed (final)", node);
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0)
        cl_say("%s", ok);
}

void cnode_move_pos_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t vol = 0, pos = 0, to = 0;
    if (cluster_volume_move_pos_decode(body, n, &vol, &pos, &to) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "volume move --pos: a body of 12 bytes is expected (got %zu)", n);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_volume *v = meta_volume(sm, vol);
    if (!v) {
        reply(conn, reqid, META_ERR_NOTFOUND, "volume move --pos: volume %" PRIu32 " does not exist", vol);
        return;
    }
    if (v->redundancy != META_RED_EC) {
        reply(conn, reqid, META_ERR_INVALID, "volume move --pos: volume %" PRIu32 " (%s) is not erasure-coded", vol,
              v->name);
        return;
    }
    if (v->conv.phase != META_CONV_NONE) {
        reply(conn, reqid, META_ERR_STATE,
              "volume move --pos: volume %" PRIu32 " (%s) has a redundancy conversion in progress (phase %s)", vol,
              v->name, meta_conv_phase_name(v->conv.phase));
        return;
    }
    if (pos >= v->nstripe) {
        reply(conn, reqid, META_ERR_INVALID,
              "volume move --pos: position %" PRIu32 " out of range for volume %" PRIu32 " (0 to %" PRIu32 ")", pos, vol,
              v->nstripe ? v->nstripe - 1 : 0);
        return;
    }
    if (v->stripe_pos[pos].retired_ver != 0) {
        reply(conn, reqid, META_ERR_STATE,
              "volume move --pos: volume %" PRIu32 " pos %" PRIu32 " is retired (waiting for drop)", vol, pos);
        return;
    }
    if (v->stripe_pos[pos].state == META_STRIPE_FILLING) {
        reply(conn, reqid, META_ERR_STATE,
              "volume move --pos: volume %" PRIu32 " pos %" PRIu32 " is already filling on node %" PRIu32
              " (stripe_version %" PRIu64 ")",
              vol, pos, v->stripe[pos], v->stripe_version);
        return;
    }
    uint32_t old_holder = v->stripe[pos];
    if (to == old_holder) {
        reply(conn, reqid, META_ERR_INVALID, "volume move --pos: node %" PRIu32 " already holds volume %" PRIu32 " pos %" PRIu32,
              to, vol, pos);
        return;
    }
    const struct meta_member *mb = meta_member(sm, to);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "volume move --pos: unknown target node %" PRIu32, to);
        return;
    }
    uint64_t now = cl_now_ns();
    if (mb->state != META_UP) {
        reply(conn, reqid, META_ERR_STATE, "volume move --pos: target node %" PRIu32 " is %s (must be up)", to,
              meta_member_state_name(mb->state));
        return;
    }
    if (!cl_heard_fresh(to, now)) {
        reply(conn, reqid, META_ERR_STATE, "volume move --pos: target node %" PRIu32 " is not heard", to);
        return;
    }
    if (!(mb->flags & META_MF_VOLUMES_OK)) {
        reply(conn, reqid, META_ERR_INVALID, "volume move --pos: target node %" PRIu32 " does not accept volumes", to);
        return;
    }
    if (mb->replaced_by != 0) {
        reply(conn, reqid, META_ERR_STATE, "volume move --pos: target node %" PRIu32 " is being replaced", to);
        return;
    }
    const char *stripe_why = NULL;
    if (!cl_member_stripe_ok(to, now, &stripe_why)) {
        reply(conn, reqid, META_ERR_INVALID, "volume move --pos: target node %" PRIu32 ": %s", to,
              stripe_why ? stripe_why : "not eligible for stripe positions");
        return;
    }
    for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
        if (v->stripe[p] == to) {
            reply(conn, reqid, META_ERR_INVALID,
                  "volume move --pos: target node %" PRIu32 " already holds stripe position %" PRIu32 " of volume %" PRIu32,
                  to, p, vol);
            return;
        }
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_stripe_set(&e, vol, pos, to, META_STRIPE_FILLING, old_holder, v->stripe_version);
    char ok[192];
    snprintf(ok, sizeof ok, "volume %" PRIu32 " pos %" PRIu32 ": moving from node %" PRIu32 " to node %" PRIu32
                            " (filling, stripe_version %" PRIu64 ")",
             vol, pos, old_holder, to, v->stripe_version + 1);
    if (cl_propose_reply(&e, conn, reqid, 0, ok) == 0)
        cl_say("%s", ok);
}

void cnode_disk_out_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    uint8_t sid[16];
    if (cluster_disk_op_decode(body, n, &node, sid) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "disk out: a body of 20 bytes is expected (got %zu)", n);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "disk out: unknown node %" PRIu32, node);
        return;
    }
    if (mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "disk out: node %" PRIu32 " is destroyed", node);
        return;
    }
    char hex[33];
    cl_sid_hex(sid, hex);
    const struct meta_disk *d = meta_member_disk(mb, sid);
    if (!d) {
        reply(conn, reqid, META_ERR_NOTFOUND, "disk out: unknown store %s on node %" PRIu32, hex, node);
        return;
    }
    if (d->state == META_STORE_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "disk out: store %s on node %" PRIu32 " is destroyed (final)", hex, node);
        return;
    }
    if (d->state == META_STORE_OUT) {
        reply(conn, reqid, META_ERR_STATE, "disk out: store %s on node %" PRIu32 " is already out", hex, node);
        return;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    if (d->state == META_STORE_UP)
        meta_op_store_state(&e, node, sid, META_STORE_DOWN);
    meta_op_store_state(&e, node, sid, META_STORE_OUT);
    char ok[192];
    snprintf(ok, sizeof ok, "node %" PRIu32 " store %s: marked out (reason: operator)", node, hex);
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0) {
        cl_disk_out_mark(node, sid);
        cl_say("%s", ok);
    }
}

void cnode_disk_replace_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    uint8_t sid[16];
    if (cluster_disk_op_decode(body, n, &node, sid) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "disk replace: a body of 20 bytes is expected (got %zu)", n);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "disk replace: unknown node %" PRIu32, node);
        return;
    }
    if (mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "disk replace: node %" PRIu32 " is destroyed", node);
        return;
    }
    char hex[33];
    cl_sid_hex(sid, hex);
    const struct meta_disk *d = meta_member_disk(mb, sid);
    if (!d) {
        reply(conn, reqid, META_ERR_NOTFOUND, "disk replace: unknown store %s on node %" PRIu32, hex, node);
        return;
    }
    if (d->state == META_STORE_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "disk replace: store %s on node %" PRIu32 " is already destroyed", hex, node);
        return;
    }
    if (d->state == META_STORE_UP) {
        reply(conn, reqid, META_ERR_STATE,
              "disk replace: store %s on node %" PRIu32 " is up (mark it out first with `disk out`)", hex, node);
        return;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    if (d->state == META_STORE_DOWN)
        meta_op_store_state(&e, node, sid, META_STORE_OUT);
    meta_op_store_state(&e, node, sid, META_STORE_DESTROYED);
    char ok[224];
    snprintf(ok, sizeof ok, "node %" PRIu32 " store %s: marked destroyed for replacement", node, hex);
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0) {
        cl_disk_out_mark(node, sid);
        cl_say("%s", ok);
    }
}

void cnode_disk_class_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    uint8_t sid[16], cls = 0;
    if (cluster_disk_class_decode(body, n, &node, sid, &cls) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "disk class: a body of 21 bytes is expected (got %zu)", n);
        return;
    }
    if (cls < META_DISK_NVME || cls > META_DISK_CLASS_LAST) {
        reply(conn, reqid, META_ERR_INVALID, "disk class: invalid class %u (expected nvme, ssd, or hdd)",
              (unsigned)cls);
        return;
    }
    const struct meta_state *sm = cl_meta();
    const struct meta_member *mb = meta_member(sm, node);
    if (!mb) {
        reply(conn, reqid, META_ERR_NOTFOUND, "disk class: unknown node %" PRIu32, node);
        return;
    }
    if (mb->state == META_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "disk class: node %" PRIu32 " is destroyed", node);
        return;
    }
    char hex[33];
    cl_sid_hex(sid, hex);
    const struct meta_disk *d = meta_member_disk(mb, sid);
    if (!d) {
        reply(conn, reqid, META_ERR_NOTFOUND, "disk class: unknown store %s on node %" PRIu32, hex, node);
        return;
    }
    if (d->state == META_STORE_DESTROYED) {
        reply(conn, reqid, META_ERR_STATE, "disk class: store %s on node %" PRIu32 " is destroyed", hex, node);
        return;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_store_class(&e, node, sid, cls);
    char ok[192];
    snprintf(ok, sizeof ok, "node %" PRIu32 " store %s: class set to %s (was %s)", node, hex, meta_disk_class_name(cls),
             meta_disk_class_name(d->disk_class));
    if (cl_propose_reply(&e, conn, reqid, node, ok) == 0)
        cl_say("%s", ok);
}

void cnode_duty(uint64_t now)
{
    if (!cl_leading())
        return;
    uint64_t wms = cl_wall_ms();
    int mut_drainleaves = cnode_mutant("drainleaves");
    int mut_replaceclear = cnode_mutant("replaceclear");

    /* If a live volume leader was marked OUT (e.g. `node out` on a running leader), hand off leadership immediately
     * to another UP in-sync replica so the volume does not sit in NO_OWNER until learner catch-up finishes. */
    const struct meta_state *sm0 = cl_meta();
    for (uint32_t i = 0; i < sm0->nvolumes; i++) {
        const struct meta_volume *v = &sm0->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !v->leader)
            continue;
        const struct meta_member *lm = meta_member(sm0, v->leader);
        if (!lm || lm->state != META_OUT || !cl_heard_fresh(v->leader, now))
            continue;
        if (ctr_running(v->number) || cl_promo_running(v->number))
            continue;
        uint32_t dst = 0;
        for (uint32_t j = 0; j < v->nisr; j++) {
            uint32_t u = v->isr[j];
            if (u == v->leader)
                continue;
            const struct meta_member *um = meta_member(sm0, u);
            if (um && um->state == META_UP && cl_heard_fresh(u, now)) {
                dst = u;
                break;
            }
        }
        if (dst && ctr_move_start(v->number, dst, now) == NULL)
            cl_say("volume %" PRIu32 ": transferring leadership off OUT node %" PRIu32 " to node %" PRIu32,
                   v->number, v->leader, dst);
    }

    for (uint32_t i = 0; cl_leading() && i < cl_meta()->nmembers; i++) {
        const struct meta_state *sm = cl_meta();
        const struct meta_member *mb = &sm->members[i];
        if (cl_pend_member(mb->id))
            continue;
        uint32_t id = mb->id;
        uint64_t inc = mb->incarnation;
        uint8_t st = mb->state;
        uint32_t repl = mb->replaced_by;
        uint64_t hold = mb->hold_until_ms;

        if (repl != 0) {
            const struct meta_member *nt = meta_member(sm, repl);
            if (!nt || nt->state >= META_OUT) {
                const char *nt_st = nt ? meta_member_state_name(nt->state) : "missing";
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_node_replace(&e, id, 0);
                if (st < META_OUT && hold > 0)
                    meta_op_node_hold(&e, id, 0, wms);
                if (cl_propose_reply(&e, 0, 0, id, NULL) == 0)
                    cl_say("proposes node %" PRIu32 " replace clear (target node %" PRIu32 " is %s)", id, repl, nt_st);
                continue;
            }
            uint32_t nlog = 0, nstripe = 0, ninflight = 0, nprev = 0;
            node_positions(sm, id, &nlog, &nstripe, &ninflight, &nprev);
            /* If OLD is still live (UP or DRAINING), it serves as copy source (`prev`) for FILLING stripe positions,
             * so keep it out of OUT until every such fill finishes (`nprev == 0`). */
            int live_source = st == META_UP || st == META_DRAINING;
            int adopting = cl_member_adopt_pending(sm, id, repl, now);
            int empty = !adopting && nlog == 0 && nstripe == 0 && ninflight == 0 && (!live_source || nprev == 0);
            if (empty || mut_replaceclear) {
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_node_replace(&e, id, 0);
                if (st < META_OUT)
                    meta_op_member_state(&e, id, META_OUT, inc, META_DOWN_NONE, 0, wms);
                if (cl_propose_reply(&e, 0, 0, id, NULL) == 0)
                    cl_say("proposes node %" PRIu32 " replace by node %" PRIu32 " complete%s", id, repl,
                           st < META_OUT ? " (out)" : "");
            }
            continue;
        }

        if (st == META_DRAINING) {
            uint32_t nlog = 0, nstripe = 0, ninflight = 0, nprev = 0;
            node_positions(sm, id, &nlog, &nstripe, &ninflight, &nprev);
            if ((nlog == 0 && nstripe == 0 && ninflight == 0 && nprev == 0) || mut_drainleaves) {
                struct meta_entry e;
                meta_entry_init(&e);
                meta_op_member_state(&e, id, META_OUT, inc, META_DOWN_NONE, 0, wms);
                if (cl_propose_reply(&e, 0, 0, id, NULL) == 0)
                    cl_say("proposes node %" PRIu32 " out (drain complete)", id);
            }
        }
    }
}
