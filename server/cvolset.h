/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The volume manager's side of replica set changes (R4 plan D8, D12, D13): a replica directory set aside when the
 * record no longer names this server (v<N>.removed-<rid>, deleted once the change is committed and the new replica is
 * in sync) or resets it (v<N>.lost-<rid>, never deleted automatically). A stub until r4-setchg fills server/cvolset.c;
 * the call sites in cvol.c and cvolfs.c are final.
 */
#ifndef TSDB_CVOLSET_H
#define TSDB_CVOLSET_H

#include <stdint.h>

#include "cvolfs.h"

struct cluster_vol_rec; /* cluster.h */

enum {
    CVS_NAMED = 1, /* a record names this server and the manager has no state for its volume yet (before prepare) */
    CVS_UNNAMED,   /* the metadata copy's latest push does not name this server for a volume it knew (r: NULL); once.
                    * A HINT, never a reason to delete by itself: a push whose records the cluster thread could not all
                    * send is not used for it (cvolset_unnamed), but the push is this server's view of a copy that may
                    * lag or be rebuilt; the directory goes only once the committed record shows the removal (D12) */
    CVS_RESET      /* the record names this server with another rid than it did when the manager prepared the volume
                    * (REPLICA_RESET), whether that open succeeded or was refused (a missing or CORRUPT REPL: the replica
                    * an operator resets); at every push while it differs */
};
#define CVS_AGAIN 1 /* cvolset_record of a CVS_RESET: forget the volume and prepare it again (cvolset_prepare sets the
                     * old directory aside); an open one is closed through cvolset_close first */
/* The manager's thread, no lock held: 0, or CVS_AGAIN. */
int cvolset_record(int change, uint32_t number, const struct cluster_vol_rec *r);
/* An open volume the manager is to forget (CVS_AGAIN): closes its driver and engine. 0 closed, -1 kept open (a line
 * said why). The manager's thread, no lock held. */
int cvolset_close(uint32_t number);
/* 1 if a push of the records `recs` (n of them, ascending by number, at most CLUSTER_REPORT_VOLS: cluster.h) shows that
 * `number` is not named here: it is not among them, and not past the end of a push that was cut at the limit. */
int cvolset_unnamed(const struct cluster_vol_rec *recs, uint32_t n, uint32_t number);
/* In cvolfs_prepare before v<N> is looked at or its REPL loaded: sets aside a directory the record no longer
 * describes (D12, D13). 0 to go on, -1 to refuse the volume (a line said why). */
int cvolset_prepare(const char *root, uint32_t number, const struct cvolfs_rec *r);

#endif
