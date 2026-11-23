/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshot transfer of a replicated volume (design-cluster.md §8.1, R4 plan D2-D7, D18): the replica's request, the
 * leader's send and the receiver's side on the kind-5 link (PL_KIND_SNAP). A stub until r4-xferlink (request, source,
 * transfer) and r4-xferinst (install, reopen) fill server/rsnap.c; the call sites in repld.c are final.
 */
#ifndef TSDB_RSNAP_H
#define TSDB_RSNAP_H

#include "repld_int.h"

struct plink;

/* The core's REPL_A_SNAPSHOT: the leader (src, its index in rv->rec.rep) no longer has the records this replica
 * needs. rv->mu held; the core repeats the notice until the replica catches up. */
void rsnap_need(struct rvol *rv, int src);
/* An IN_SNAP item (posted by rsnap.c itself from the kind-5 link's callbacks), on the volume's thread, rv->mu held. */
void rsnap_in(struct rvol *rv, uint32_t node, const uint8_t *p, size_t n);
/* Every tick of the volume's thread, rv->mu held. */
void rsnap_tick(struct rvol *rv);
/* repld_stop, after the volume's thread has been joined, before the links are freed; rv->mu not held. */
void rsnap_stop(struct rvol *rv);
/* A frame on a kind-5 link (the link's thread; never the driver's or the cluster thread). */
void rsnap_frame(struct plink *l, uint8_t type, const uint8_t *p, size_t n);
/* The bulk callbacks of a kind-5 link (peerlink.h struct pl_bulk), as bin_open, bin_data and bin_end are for kind 2. */
int rsnap_bin_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                   uint64_t *resume, int *prefix_fd);
int rsnap_bin_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n);
void rsnap_bin_end(void *arg, struct plink *l, uint64_t file, int status);

#endif
