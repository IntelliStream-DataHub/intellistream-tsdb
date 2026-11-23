/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the replication driver (R4 plan D8, D10-D12): the core's reconfiguration when the record
 * names another replica set, the set version on every message, and the Raft leader's DROP_MEMBER of a move. A stub
 * until r4-setchg (and r4-move: DROP_MEMBER) fill server/rset.c; the call sites in repld.c are final.
 */
#ifndef TSDB_RSET_H
#define TSDB_RSET_H

#include "repld_int.h"

/* repld_update: the record names another replica set than the driver's (by node, in order). rv->mu held. */
void rset_change(struct rvol *rv, const struct cluster_vol_rec *r);
/* A peer message of this volume, decoded and checked to be from `from` to this replica: 1 to hand it to the core, 0 to
 * drop it (another set version). rv->mu held. */
int rset_msg_ok(struct rvol *rv, const struct repl_msg *m);
/* Before every message the driver sends (APPEND, META and the core's own): stamps the set version. rv->mu held. */
void rset_stamp(struct rvol *rv, struct repl_msg *m);
/* An IN_CMD of kind CL_VOLCMD_DROP_MEMBER, on the volume's thread, rv->mu held. */
void rset_cmd(struct rvol *rv, const struct cluster_volcmd *c);

#endif
