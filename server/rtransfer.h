/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A planned leader transfer, the volume leader's side (R4 plan D14, D15): the drain that stops admitting records,
 * READY to the Raft leader, CANCEL. A stub until r4-transfer fills server/rtransfer.c; the call sites in repld.c are
 * final.
 */
#ifndef TSDB_RTRANSFER_H
#define TSDB_RTRANSFER_H

#include "repld_int.h"

/* op_admit (jr_ops.admit, every WAL record, under buf_lock and rv->mu): 0 refuses the record (TSDB_ERR_UNAVAILABLE,
 * definite: never admitted) while a transfer drains, 1 lets the usual checks decide. Takes no lock. */
int rtr_admit_ok(struct rvol *rv);
/* An IN_CMD of kind CL_VOLCMD_TRANSFER or CL_VOLCMD_CANCEL, on the volume's thread, rv->mu held. */
void rtr_cmd(struct rvol *rv, const struct cluster_volcmd *c);
/* Every tick of the volume's thread, rv->mu held. */
void rtr_tick(struct rvol *rv);

#endif
