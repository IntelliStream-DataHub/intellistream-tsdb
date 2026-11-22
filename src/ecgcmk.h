/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The apply of a committed MK_GC on every member (design-cluster-data.md E14, the R8 plan's D13): an .ec of those oids
 * still listed is retired first, the retired_ names of them are unlinked (directory fsync), and the oids go on a
 * durable pending-delete list (a checksummed append-only file, like CLASSES), cleared by MK_GC{done}; a promoted
 * leader re-issues the deletes of every oid on its list. (src/ecgc.c, the tier's own collection, is r8b's while this
 * stub stands.)
 *
 * STUB (r8a1-mk): filled by r8k-gc. Until then a MK_GC is acknowledged and deletes nothing (an orphan stays, which is
 * safe), and the open reads no list.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECGCMK_H
#define TSDB_ECGCMK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "marker.h"

struct eng;
struct ecgcmk; /* the pending-delete list (r8k), eng.gcmk */

/* A committed MK_GC at log position pos (m decoded): 0 when applied. */
int ecgcmk_apply(struct eng *db, uint64_t pos, const mk *m);
int ecgcmk_open(struct eng *db);
void ecgcmk_close(struct eng *db);

#endif
