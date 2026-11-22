/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * DIVERGED made durable (design-cluster-data.md E26, the R8 plan's D8, D23): ec_diverge appends a checksummed record
 * (key, oid, reason, time) to the volume's DIVERGED file, fdatasyncs it and its directory, then sets db->diverged (a
 * failed write is fail-stop); the open reads it and sets the flag before any thread starts. tsdbd reports it through
 * tsdb_volume_diverged (src/cvol.h).
 *
 * STUB (r8a1-mk): filled by r8i-diverged. Until then the flag is memory only, as R7 keeps it (src/eckeep.c): ec_diverge
 * sets it and writes nothing, and the open reads nothing.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECDIVERGE_H
#define TSDB_ECDIVERGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct eng;
struct ecdiverge; /* the DIVERGED file's state (r8i), eng.diverge */

enum { EC_DIV_DIGEST = 1 /* a settle's digest did not match */, EC_DIV_PPOS = 2 /* a part above Z at a cold apply */ };

/* The volume diverged at key `key` (oid: the object, may be NULL) for `why` (EC_DIV_*): durable, then the flag. */
int ec_diverge(struct eng *db, int64_t key, const uint8_t *oid, int why);
int ecdiverge_open(struct eng *db);
void ecdiverge_close(struct eng *db);

#endif
