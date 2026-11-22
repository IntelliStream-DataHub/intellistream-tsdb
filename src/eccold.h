/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A leader's settle of a cluster ec volume (design-cluster-data.md 2.3 steps 3-5, the R8 plan's D4, D21): the gate
 * (jr_serial, like a large write), the cut at Z, the late parts Y, and the MK_COLD marker; and whether a cluster
 * volume may attach a tier at all (eccold_may_attach, which tier.c asks in place of its refusal of requires=cluster).
 * The leader API r8a2-hooks adds (jr_cold, jr_cold_precommit, jr_cold_commit) is declared here and built in
 * src/replica.c.
 *
 * STUB (r8a1-mk): filled by r8g-cut. Until then no cluster volume attaches a tier and nothing settles in a cluster:
 * db->cold stays NULL.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECCOLD_H
#define TSDB_ECCOLD_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct eng;
struct eccold; /* the leader's settle state (r8g), eng.cold */

/* Whether this engine (a cluster volume: requires=cluster or replica) may attach a tier. false: refused, as R7 does. */
bool eccold_may_attach(struct eng *db);
int eccold_open(struct eng *db);
void eccold_close(struct eng *db);

#endif
