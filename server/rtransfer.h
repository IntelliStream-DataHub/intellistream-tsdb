/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A planned leader transfer, the volume leader's side (R4 plan D14, D15): the drain that stops admitting records,
 * READY to the Raft leader, CANCEL. The call sites in repld.c are final.
 */
#ifndef TSDB_RTRANSFER_H
#define TSDB_RTRANSFER_H

#include "repld_int.h"

/* op_admit (jr_ops.admit, every WAL record, under buf_lock and rv->mu): 0 refuses the record (TSDB_ERR_UNAVAILABLE,
 * definite: never admitted) while a transfer drains, 1 lets the usual checks decide. Takes no lock. */
int rtr_admit_ok(struct rvol *rv);
/* An IN_CMD of kind CL_VOLCMD_TRANSFER or CL_VOLCMD_CANCEL, on the volume's thread, rv->mu held. */
/* `from`: the member that sent it (the frame carries no term: cluster.h; check it against the Raft leader) */
void rtr_cmd(struct rvol *rv, const struct cluster_volcmd *c, uint32_t from);
/* Every tick of the volume's thread, rv->mu held. */
void rtr_tick(struct rvol *rv);
/* 1 while a transfer refuses this volume's records (rv->mu held) */
int rtr_draining(struct rvol *rv);
/* A replica's log cut that removed records (repld.c's REPL_A_TRUNCATE below the index's end): STATUS's CL_TR_CUTS. */
void rtr_note_cut(void);

#endif
