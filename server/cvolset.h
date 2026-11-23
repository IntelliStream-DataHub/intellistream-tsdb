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
    CVS_UNNAMED,   /* the metadata copy's push no longer names this server for a volume it knew (r: NULL); once */
    CVS_RESET      /* the record names this server with another rid than its directory's (REPLICA_RESET); at every
                    * push while it differs */
};
/* The manager's thread, no lock held. */
void cvolset_record(int change, uint32_t number, const struct cluster_vol_rec *r);
/* In cvolfs_prepare before v<N> is looked at or its REPL loaded: sets aside a directory the record no longer
 * describes (D12, D13). 0 to go on, -1 to refuse the volume (a line said why). */
int cvolset_prepare(const char *root, uint32_t number, const struct cvolfs_rec *r);

#endif
