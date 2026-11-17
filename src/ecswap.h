/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The swap of a settle (R7, r7-swap): intent v4 ("tsdb-intent 4 <n>", then `L <tmp_*.ec> <pK_A_B_L.ec>` and
 * `R <pK_A_B_L.part> <settled_pK_A_B_L.part>` lines, then "end <xxh64>": src/part.c intent_unseal), its commit, and
 * its recovery at open. intents_recover hands every intent that begins with INTENT_V4_HEAD to ecswap_recover.
 */
#ifndef TSDB_ECSWAP_H
#define TSDB_ECSWAP_H

#include "internal.h"

/* The open's recovery of the v4 intent `name` (its whole text in buf[0 .. len), which may be changed). 0: done, and
 * intents_recover unlinks the intent; else the open fails with it. Stub: what intents_recover does today with an
 * intent it cannot parse: a line, and 0 (rolled back: nothing was renamed by it, since nothing writes one yet). */
int ecswap_recover(eng *db, const char *name, char *buf, size_t len);

#endif
