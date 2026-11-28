/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * What R4/R9/R12's files on the Raft leader's side (server/ctransfer.c, cmove.c, cset.c, cnode.c, cheal.c) need of
 * cluster.c, and their entry points, which cluster.c calls. Everything here runs on the cluster thread only (the admin
 * ops, the duty round, the frames), as cluster.c's own state is that thread's; only cheal.c takes a private leaf lock
 * (g_blk_mu) to publish its blocked-volume snapshot to the metrics collector thread.
 */
#ifndef TSDB_CLUSTER_INT_H
#define TSDB_CLUSTER_INT_H

#include <stddef.h>
#include <stdint.h>

#include "cluster.h"
#include "meta.h"

/* one line on stderr, "tsdbd: cluster: ..." */
void cl_say(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
/* the cluster's clock (ns, monotonic): the `now` of the duty round */
uint64_t cl_now_ns(void);
/* this server's node id (0: in no cluster yet) */
uint32_t cl_self(void);
/* 1 on the Raft leader with an initialised metadata copy: where the admin ops and the duties act */
int cl_leading(void);
/* the Raft leader's peer address ("" when none is known): CLUSTER_ERR_NOTLEADER's body */
const char *cl_leader_peer(void);
/* the applied metadata state (meta_volume, meta_member and the rest of meta.h read it) */
const struct meta_state *cl_meta(void);
/* 1 if member `id` was heard within the node timeout (this server: always) */
int cl_heard_fresh(uint32_t id, uint64_t now);
/* 1 once this Raft leader has held leadership long enough to have heard all live members and outlast prior leases */
int cl_leader_settled(uint64_t now);
/* Reported health (enum cluster_vol_health) of `vol` on `node` from HBS_VOLUMES (or local reporter when node == self),
 * or -1 if `node` is not heard fresh or does not report `vol`. If rid_out != NULL, stores the reported replica rid. */
int cl_vol_health(uint32_t node, uint32_t vol, uint64_t now, uint64_t *rid_out);
/* proposes `e` to the group (the leader only): 0 queued (applied later, through the watch), else CLUSTER_ERR_RETRY */
int cl_propose(struct meta_entry *e);
/* proposes `e` (and frees it) on behalf of an admin op: once applied, replies to (conn, reqid) with ok_text (if rc0 == 0)
 * or rc0/CLUSTER_ERR_RETRY; `member` (0: none) registers with pend_member. 0 if queued (caller does not reply), else
 * CLUSTER_ERR_RETRY (already replied). */
int cl_propose_reply(struct meta_entry *e, uint64_t conn, uint32_t reqid, uint32_t member, const char *ok_text);
/* proposer's wall-clock time in ms (CLOCK_REALTIME, >= 1) */
uint64_t cl_wall_ms(void);
/* 1 if a proposal for member `id` is already in flight on this Raft leader */
int cl_pend_member(uint32_t id);
/* number of chunk stores currently UP on member `id` as last reported in HBS_STORES (0 if none/unknown) */
uint32_t cl_member_chunk_stores(uint32_t id);
/* R13 (r13-adopt): return member `id`'s pending adoption target from HBS_DISKS (0 if not pending adoption),
 * and optionally its count of pending-adoption chunk stores in `*chunk_stores_out`. */
uint32_t cl_member_adopt_info(uint32_t id, uint32_t *chunk_stores_out);
/* R13 (r13-adopt): 1 if `node replace old_node new_node` must not complete yet (new_node is not heard fresh yet,
 * still reports pending adoption of old_node, or reports any UP store_id still owned non-DESTROYED by old_node in sm). */
int cl_member_adopt_pending(const struct meta_state *sm, uint32_t old_node, uint32_t new_node, uint64_t now);
/* R13 (r13-place; E49, E50, E51, E53): 1 if `node` passes log-replica / stripe-position capability, role and disk fill
 * rules under the current heartbeat view, else 0 with optional reason in *why_out. */
int cl_member_log_ok(uint32_t node, uint64_t now, const char **why_out);
int cl_member_stripe_ok(uint32_t node, uint64_t now, const char **why_out);
/* R10 (D26, D28): last HBS_LIVE_OBJS report heard from `node` for `volume` (1 if found and copied to *out, else 0) */
int cl_heard_live_objs(uint32_t node, uint32_t volume, struct cluster_live_objs_report *out);
/* the answer to an admin op (conn and reqid as the op's entry point got them) */
void cl_admin_reply(uint64_t conn, uint32_t reqid, int32_t rc, const void *body, size_t len);
/* sends a VOLCMD to member `node` (to this server's own handler when it is this server): 0, or -1 with no connection
 * to it now (the caller's duty round sends again) */
int cl_volcmd_send(uint32_t node, const struct cluster_volcmd *c);
/* starts a promotion of `v` (as the failure detector does) that prefers `target` with a durable end at or above
 * `min_end` (0, 0: none, any ISR member as before). The filter on the target is r4-transfer's (cluster.c :5850) */
void promo_start_target(const struct meta_volume *v, uint8_t reason, uint32_t target, uint64_t min_end, uint64_t now);

/* the Raft term this server is in (a transfer's VOLCMD carries it in `arg`: a volume leader ignores an older one) */
uint64_t cl_term(void);
/* how long this server has led the group (0: it does not) */
uint64_t cl_leader_age_ns(uint64_t now);
/* the group's lease (ms) */
uint32_t cl_lease_ms(void);
/* 1 while a promotion of volume `vol` is under way here (the Raft leader's) */
int cl_promo_running(uint32_t vol);
/* TSDB_CLUSTER_TEST lose_ready_file: 1 while READYs and a timed-out transfer's CANCEL are lost */
int cl_lose_ready(void);

/* ---- the entry points */
/* server/ctransfer.c (r4-transfer): CLUSTER_OP_VOLUME_TRANSFER, the duty round (leader balance, CANCEL after an
 * election, the transfer's timeouts), a volume leader's READY */
void ctr_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void ctr_duty(uint64_t now);
void ctr_ready(uint32_t from, const struct cluster_vmsg *m);
/* the candidate filter of a transfer's promotion (cluster.c promo_step): 1 if `node`'s fence reply goes into the grant */
int ctr_reply_passes(uint32_t target, uint32_t node);
int ctr_drop_proposed(void);
/* a transfer's promotion of `vol` at basis `epoch` was dropped (no grant within PROMO_MOVE_NS) */
void ctr_promo_dropped(uint32_t vol, uint64_t epoch, uint64_t now);
/* STATUS's counters of the Raft leader's side (this server's, since it started) */
struct ctr_counts {
    unsigned long started, done, timedout, cancelled, fallback, balance;
};
void ctr_counts(struct ctr_counts *c);
/* a move's transfer (cmove.c): starts a planned transfer of `vol` to `node` as the admin op would, with no answer to
 * send; NULL when started, else why not (tr_check's reasons). ctr_running: 1 while a transfer of `vol` is under way */
const char *ctr_move_start(uint32_t vol, uint32_t node, uint64_t now);
int ctr_running(uint32_t vol);
/* server/cmove.c (r4-move): CLUSTER_OP_VOLUME_MOVE and the move's steps (add -> join -> shrink -> remove) */
void cmove_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cmove_duty(uint64_t now);
/* R9 (r9-plan): start an automatic log replica move tracked by cmove (NULL when proposed, else reason not started). */
extern const char cmove_err_already_proposed[];
const char *cmove_start_auto(uint32_t vol, uint32_t from, uint32_t to, uint64_t now);
/* R9 (r9-return, D13): cancel an in-flight automatic log move before learner `to` joins the ISR. */
const char *cmove_cancel_auto(uint32_t vol, uint32_t from, uint32_t to, uint64_t now);
/* STATUS: a line per move this Raft leader drives ("move volume V node A -> node B: STEP, AGE s"); counters */
size_t cmove_status(char *out, size_t n, uint64_t now);
void cmove_counts(unsigned long *started, unsigned long *done, unsigned long *cancelled);
/* TSDB_CLUSTER_TEST vol_trace=1: after every applied entry of the server's own state, a line per replicated volume
 * whose set, epoch, leader or ISR moved, appended to `path` */
void cmove_trace(const struct meta_state *sm, uint64_t index, const char *path);
/* server/cset.c (r4-setchg): CLUSTER_OP_REPLICA_RESET */
void cset_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
/* a fresh non-zero replica rid distinct from every current replica of `v` */
uint64_t cset_fresh_rid(const struct meta_volume *v);
/* TSDB_CLUSTER_TEST setop_file=1: the Raft leader proposes the REPLICA_ADD/REMOVE/RESET lines of <state-dir>/setop */
void cset_setop_file(const char *path);
/* server/cheal.c (R12, r12-heal; PLAN.md D8): automatic REPLICA_RESET and planned leader transfer for DAMAGED replicas */
void cheal_start(void);
void cheal_stop(void);
void cheal_duty(uint64_t now);
/* server/cnode.c (R9, r9-ops): CLUSTER_OP_NODE_* and CLUSTER_OP_VOLUME_MOVE_POS on the Raft leader */
void cnode_hold_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_out_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_drain_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_replace_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_destroyed_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_move_pos_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
/* server/cnode.c (R13, r13-dev): CLUSTER_OP_DISK_* on the Raft leader */
void cnode_disk_out_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_disk_replace_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cnode_disk_class_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
/* Record an in-flight STORE_STATE OUT proposal on the Raft leader so propose_disks never races DOWN -> UP ahead of it. */
void cl_disk_out_mark(uint32_t node, const uint8_t store_id[16]);
static inline void cl_sid_hex(const uint8_t id[16], char out[33])
{
    static const char hx[] = "0123456789abcdef";
    for (int i = 0; i < 16; i++) {
        out[i * 2] = hx[id[i] >> 4];
        out[i * 2 + 1] = hx[id[i] & 0xf];
    }
    out[32] = '\0';
}
/* once-per-second Raft leader duty: DRAINING -> OUT when empty, and node replace completion (r9-ops) */
void cnode_duty(uint64_t now);

#endif
