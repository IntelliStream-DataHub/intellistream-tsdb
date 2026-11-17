/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled sources (R7, r7-keep): settled_<part>.part, kept from the .ec record's settle time for settle_keep_s and
 * while its digest is unchecked (GY_SETTLED in the graveyard); a digest that does not match sets the volume DIVERGED
 * (D5). r7-iface left stubs that do what the engine does today with these names (nothing: it never wrote one).
 */
#ifndef TSDB_ECKEEP_H
#define TSDB_ECKEEP_H

#include "internal.h"

/* load_parts met settled_<p...>.part (settled_part_name). Stub: 0, the file left alone. */
int eckeep_load_settled(eng *db, const char *name, bool read_only);
/* graveyard_purge, for a GY_SETTLED entry (db->lock held): true when it leaves the list. Stub: unlinked once due. */
bool eckeep_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now);

#endif
