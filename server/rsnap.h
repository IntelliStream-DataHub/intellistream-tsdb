/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshot transfer of a replicated volume (design-cluster.md §8.1, R4 plan D2-D7, D18): the replica's request, the
 * leader's send and the receiver's side on the kind-5 link (PL_KIND_SNAP); server/rsnap.c has the design. The install
 * and reopen (r4-xferinst) are not built: a received copy is removed.
 */
#ifndef TSDB_RSNAP_H
#define TSDB_RSNAP_H

#include "repld_int.h"

struct plink;

/* The core's REPL_A_SNAPSHOT: the leader (src, its index in rv->rec.rep) no longer has the records this replica
 * needs. rv->mu held; the core repeats the notice until the replica catches up. */
void rsnap_need(struct rvol *rv, int src);
/* A failed replica's thread (rv->mu held), every REBUILD_MS: a DIVERGED replica (rv->div_rebuild) asks the volume's
 * leader for a snapshot, whose install closes this driver and opens a fresh copy without the DIVERGED file. Also the
 * test hook TSDB_REPLD_TEST=snapask (<dir>/snapask: a running replica asks as if it were behind; test_cluster
 * snapcatch). */
void rsnap_rebuild(struct rvol *rv);
/* An IN_SNAP item (posted by rsnap.c itself from the kind-5 link's callbacks), on the volume's thread, rv->mu held.
 * IN_SNAP is a peer kind: it shares the volume's bounded inbox with the log frames and is DROPPED after INBOX_WAIT_MS
 * when the inbox stays full. The core resends its own messages; nothing resends a snapshot's, so rsnap.c must post only
 * what it can lose and retry by itself (a request again on its tick, a transfer resumed from the receiver's offset). */
void rsnap_in(struct rvol *rv, uint32_t node, const uint8_t *p, size_t n);
/* Every tick of the volume's thread, rv->mu held. */
void rsnap_tick(struct rvol *rv);
/* repld_stop, after every volume's thread has been joined AND the links freed (peerlink_free joins the link threads,
 * which call rsnap_frame and rsnap_bin_* until then): nothing of rsnap.c runs any more; rv->mu not held. */
void rsnap_stop(struct rvol *rv);
/* repld_close (never an install's repld_close_end), the driver joined: ends the volume's snapshot jobs (a receiver's
 * v<N>.snap removed by the worker, a source's snapshot released); no lock held. */
void rsnap_closed(uint32_t vol);
/* A frame on a kind-5 link (the link's thread; never the driver's or the cluster thread). */
void rsnap_frame(struct plink *l, uint8_t type, const uint8_t *p, size_t n);
/* The bulk callbacks of a kind-5 link (peerlink.h struct pl_bulk), as bin_open, bin_data and bin_end are for kind 2. */
int rsnap_bin_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                   uint64_t *resume, int *prefix_fd);
int rsnap_bin_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n);
void rsnap_bin_end(void *arg, struct plink *l, uint64_t file, int status);
/* repld_stop, before peerlink_free: stops the snapshot worker (a make in progress finishes first); what the links ask
 * after it is dropped */
void rsnap_quiesce(void);
/* RM_SNAPSAMELINK (a planted bug: the files on kind 2): 1 when a kind-2 file is a snapshot's (meta at its begin, else
 * (l, file)); always 0 otherwise */
int rsnap_owns(struct plink *l, uint64_t file, const void *meta, size_t meta_len);

/* struct rsnap_stats and rsnap_get_stats are in repld.h (tsdbd's STATUS and metrics read them) */

#endif
