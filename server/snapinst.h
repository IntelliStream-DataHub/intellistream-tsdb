/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of a snapshot's install (R4 plan D5, D6): a volume's directory swapped for the one received into
 * v<N>.snap/. A stub until r4-xferinst fills server/snapinst.c; the call site in cvolfs.c is final.
 */
#ifndef TSDB_SNAPINST_H
#define TSDB_SNAPINST_H

#include <stdint.h>

/* At the start of cvolfs_prepare (the manager's thread, before anything of v<N> is looked at): finishes or undoes what
 * a crash left of an install of volume `number` under `root` (v<N>.snap/, a swap half done). 0 to go on, -1 to refuse
 * the volume (a line said why). */
int snapinst_leftovers(const char *root, uint32_t number);

#endif
