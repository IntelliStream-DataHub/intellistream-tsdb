/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * What R4's files on the Raft leader's side (server/ctransfer.c, cmove.c, cset.c) need of cluster.c, and their entry
 * points, which cluster.c calls. Everything here runs on the cluster thread only (the admin ops, the duty round, the
 * frames), as cluster.c's own state is that thread's; nothing here takes a lock.
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
/* proposes `e` to the group (the leader only): 0 queued (applied later, through the watch), else CLUSTER_ERR_RETRY */
int cl_propose(struct meta_entry *e);
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

/* ---- the entry points (stubs answer META_ERR_INVALID "not built" until their track fills them) */
/* server/ctransfer.c (r4-transfer): CLUSTER_OP_VOLUME_TRANSFER, the duty round (leader balance, CANCEL after an
 * election, the transfer's timeouts), a volume leader's READY */
void ctr_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void ctr_duty(uint64_t now);
void ctr_ready(uint32_t from, const struct cluster_vmsg *m);
/* the candidate filter of a transfer's promotion (cluster.c promo_step): 1 if `node`'s fence reply goes into the grant */
int ctr_reply_passes(uint32_t target, uint32_t node);
/* a transfer's promotion of `vol` at basis `epoch` was dropped (no grant within PROMO_MOVE_NS) */
void ctr_promo_dropped(uint32_t vol, uint64_t epoch, uint64_t now);
/* STATUS's counters of the Raft leader's side (this server's, since it started) */
struct ctr_counts {
    unsigned long started, done, timedout, cancelled, fallback, balance;
};
void ctr_counts(struct ctr_counts *c);
/* server/cmove.c (r4-move): CLUSTER_OP_VOLUME_MOVE and the move's steps (add -> join -> shrink -> remove) */
void cmove_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void cmove_duty(uint64_t now);
/* server/cset.c (r4-setchg): CLUSTER_OP_REPLICA_RESET */
void cset_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);

#endif
