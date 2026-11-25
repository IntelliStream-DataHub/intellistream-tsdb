/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The volume manager's side of replica set changes (R4 plan D8, D12, D13): a replica directory set aside when the
 * record no longer names this server (v<N>.removed-<rid>, deleted once the change is committed and the new replica is
 * in sync) or resets it (v<N>.lost-<rid>, never deleted automatically). Implemented in server/cvolset.c; called from
 * cvol.c and cvolfs.c.
 */
#ifndef TSDB_CVOLSET_H
#define TSDB_CVOLSET_H

#include <stddef.h>
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
/* A volume the manager is to forget (CVS_AGAIN; any state but MV_NONE): closes its driver and engine when they are open,
 * and sets the directory of a committed removal aside (v<N>.removed-<rid>, root fsynced, deleted in the background).
 * 0 done, -1 kept (a line said why). The manager's thread, no lock held. */
int cvolset_close(uint32_t number);
/* 1 if a push of the records `recs` (n of them, ascending by number, at most CLUSTER_REPORT_VOLS: cluster.h) shows that
 * `number` is not named here: it is not among them, and not past the end of a push that was cut at the limit. */
int cvolset_unnamed(const struct cluster_vol_rec *recs, uint32_t n, uint32_t number);
/* In cvolfs_prepare before v<N> is looked at or its REPL loaded: sets aside a directory the record no longer
 * describes (D12, D13). 0 to go on, -1 to refuse the volume (a line said why). */
int cvolset_prepare(const char *root, uint32_t number, const struct cvolfs_rec *r);

/* cvolset.c's helpers, shared with server/cvolset_mgr.c (the manager side) */
/* notes of the manager (any thread): kind CVS_RESET with the record's new rid, CVS_UNNAMED a committed removal */
void cvolset_note(uint32_t number, int kind, uint64_t rid);
/* 1 if noted (its rid in *rid when not NULL), and taken out when `take` */
int cvolset_noted(uint32_t number, int kind, uint64_t *rid, int take);
/* renames root/v<N> to v<N>.<kind>-<rid hex> (a free name: .1, .2 ... after it) and fsyncs root; the new path in `to`.
 * 0, or -1 with a line */
int cvolset_setaside(const char *root, uint32_t number, const char *kind, uint64_t rid, char *to, size_t cap);
/* the rid in vdir/REPL: 0, or -1 (none, unreadable) */
int cvolset_rid_of(const char *vdir, uint64_t *rid);
/* removes `path` and everything under it on a detached thread (a line if it fails) */
void cvolset_delete_bg(const char *path);
/* the manager, on its first push after the start and after a committed removal: every v<N> of root that names a
 * volume the metadata copy holds without this server is set aside as removed (as cvolset_close does) */
void cvolset_sweep(const char *root);

#endif
