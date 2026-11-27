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
    int installed, stale;      /* swapped in; refused: fenced above the snapshot's epoch, or P at or below the log end
                                * the stopped driver left */
    int taken;                 /* not installed, and staged removed (or left with its SNAPDONE for snap_leftovers)
                                * before the volume went back: the caller never removes it */
    uint64_t keep_fence;       /* the old REPL's fence (0: none) */
    uint64_t fence, open_gen;  /* the installed REPL's */
};
/* rsnap's worker, a complete copy in `staged` (<root>/v<N>.snap, every file synced) of P = p in epoch p_epoch: the
 * volume closed through cvol_install, the old REPL read, snap_install, the volume handed back to be opened. 0
 * installed, 1 refused (fenced above p_epoch, or P <= the stopped driver's log end), CVOL_NOT_TAKEN (not the manager's volume), or a TSDB_ERR_* of the
 * install. */
int snapinst_install(const char *root, uint32_t number, const char *staged, uint64_t p, uint64_t p_epoch,
                     struct snapinst_result *out);

/* Active snapshot install progress (server/snapinst.c; HBS_PROGRESS, R14). */
struct snapinst_progress {
    uint32_t volume;
    uint64_t bytes_done;
    uint64_t bytes_total;
    uint64_t started_ms;       /* Unix epoch ms */
    uint64_t last_progress_ms; /* Unix epoch ms */
};
/* Lock-safe getter (leaf mutex, no I/O). Returns entries written (<= cap). */
uint32_t snapinst_get_progress(struct snapinst_progress *out, uint32_t cap);
void snapinst_progress_begin(uint32_t volume, uint64_t bytes_total, uint64_t started_ms);
void snapinst_progress_update(uint32_t volume, uint64_t bytes_done);
void snapinst_progress_touch(uint32_t volume);
void snapinst_progress_end(uint32_t volume);

#endif
