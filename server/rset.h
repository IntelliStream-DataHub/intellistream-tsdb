/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the replication driver (R4 plan D8, D10-D12): the core's reconfiguration when the record
 * names another replica set, the set version on every message, and the Raft leader's DROP_MEMBER of a move.
 * Implemented in server/rset.c; called from repld.c.
 */
#ifndef TSDB_RSET_H
#define TSDB_RSET_H

#include "repld_int.h"

/* repld_update, every push of an open volume (the manager's thread), rv->mu held: a record of the driver's set version
 * is taken at once (rec_set), 0; one of another set version is kept for the volume's thread, 1: post IN_META. */
int rset_change(struct rvol *rv, const struct cluster_vol_rec *r);
/* The volume's thread, rv->mu held (at the head of IN_META and at every tick): applies a kept set change (drains the
 * core's queue through the old record, repl_reconfig, repl_set_version, the driver's per-index state remapped). 0
 * nothing waits, 1 it still waits (a snapshot in progress, actions still queued, this server not named), 2 applied now
 * (the caller hands the core the record: a META). */
int rset_apply(struct rvol *rv);
/* 1 if a replica set change is waiting for rset_apply(), 0 otherwise. rv->mu held. */
int rset_pending(const struct rvol *rv);
/* peer messages dropped for another set version (the trace line) */
uint64_t rset_dropped(const struct rvol *rv);
/* A peer message of this volume, decoded and checked to be from `from` to this replica: 1 to hand it to the core, 0 to
 * drop it (another set version). rv->mu held. */
int rset_msg_ok(struct rvol *rv, const struct repl_msg *m);
/* On a message the driver builds by hand (the APPEND of send_records, the META and FENCE it hands to the core): stamps
 * the set version. Never on the core's own messages (send_msg sends them as they are): the core stamps those, and one
 * queued before a reconfiguration carries the OLD version in the old record's indexes, which a restamp would relabel
 * (r4-core's hand-off: such actions are carried out through the old record). rv->mu held. */
void rset_stamp(struct rvol *rv, struct repl_msg *m);
/* An IN_CMD of kind CL_VOLCMD_DROP_MEMBER, on the volume's thread, rv->mu held. `from` is the member that sent it (the
 * frame carries no term: cluster.h, check it against the Raft leader). */
void rset_cmd(struct rvol *rv, const struct cluster_volcmd *c, uint32_t from);

#endif
