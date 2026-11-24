/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of a snapshot's install (R4 plan D5, D6): a volume's directory swapped for the one received into
 * v<N>.snap/ (server/snapinst.c).
 */
#ifndef TSDB_SNAPINST_H
#define TSDB_SNAPINST_H

#include <stdint.h>

/* At the start of cvolfs_prepare (the manager's thread, before anything of v<N> is looked at): finishes or undoes what
 * a crash left of an install of volume `number` under `root` (v<N>.snap/, a swap half done). 0 to go on, -1 to refuse
 * the volume (a line said why). */
int snapinst_leftovers(const char *root, uint32_t number);

/* What an install did (STATUS, the log, tests) */
struct snapinst_result {
    int installed, stale;      /* swapped in; refused: the receiver is fenced above the snapshot's epoch */
    uint64_t keep_fence;       /* the old REPL's fence (0: none) */
    uint64_t fence, open_gen;  /* the installed REPL's */
};
/* rsnap's worker, a complete copy in `staged` (<root>/v<N>.snap, every file synced) of P = p in epoch p_epoch: the
 * volume closed through cvol_install, the old REPL read, snap_install, the volume handed back to be opened. 0
 * installed, 1 refused (fenced above p_epoch), CVOL_NOT_TAKEN (not the manager's volume), or a TSDB_ERR_* of the
 * install. */
int snapinst_install(const char *root, uint32_t number, const char *staged, uint64_t p, uint64_t p_epoch,
                     struct snapinst_result *out);

#endif
