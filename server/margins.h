/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The margins of a cluster volume (R8-2, design-cluster-data.md E15, E24): how many more losses each tier survives
 * now, and the state of the settled tier. STATUS and the metrics port print them (TOPOLOGY gets them with protocol 16:
 * no client decoder skips bytes it does not know, so a new field there needs a protocol number; owner, 2026-11-25).
 *
 *   young margin    ISR size - 1: an acknowledged row is on every ISR member, so it survives that many losses (E4, E24).
 *                   -1 when the ISR is empty (nothing is acknowledged then).
 *   settled margin  (an EC volume only) the positions of the stripe group that are present, minus k: the fewest
 *                   positions beyond k over the volume's objects. Every object is placed on the volume's one stripe
 *                   group (E9: position -> node, never renumbered), so an object of the full k loses exactly the
 *                   positions the group loses and is the minimum; an object of k' < k data cells spans k' + m of the
 *                   positions and has at least this margin. Negative: more than m positions away, settled reads of a
 *                   whole object cannot be decoded.
 *   settled state   ok; unavailable (fewer than k positions present: retryable, TSDB_WHY_UNREACHABLE); corrupt (a
 *                   read of the volume answered TSDB_ERR_CORRUPT on this server: enough positions, checks failed beyond
 *                   what decoding repairs; until a restart, R9's repair clears it); lost (more than m positions on
 *                   nodes the operator declared destroyed: TSDB_WHY_LOST, never retried). Lost wins over unavailable,
 *                   which wins over corrupt.
 *
 * A position is present while its member is UP or DRAINING in the metadata copy (DOWN and OUT are away, a member the
 * copy does not know is away) and is not declared destroyed. Every member computes the same from its copy: the member
 * states are the metadata group's judgement of the heartbeats. No member can be declared destroyed before R9
 * (`tsdb cluster node destroyed`); until then the list comes only from tsdbd's test hook TSDBD_TEST_COLD.
 *
 * No I/O, no locks: callers hold the copy.
 */
#ifndef TSDB_MARGINS_H
#define TSDB_MARGINS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cluster.h"

enum margin_tier {
    MARGIN_TIER_NONE = 0, /* not an erasure-coded volume: no settled tier in the cluster */
    MARGIN_TIER_OK,
    MARGIN_TIER_UNAVAILABLE,
    MARGIN_TIER_CORRUPT,
    MARGIN_TIER_LOST
};

/* Planted bugs (TSDBD_MARGIN_MUTANT=NAME in tsdbd; tests/margin_mutants.sh requires test_cluster margins to fail each,
 * and test_margins its unit cases). */
enum margin_mutant {
    MARGIN_MUT_NONE = 0,
    MARGIN_MUT_MARGINOFF,  /* the settled margin counted from k + m: a whole stripe group reads 0 */
    MARGIN_MUT_YOUNGREP,   /* the young margin counted from the replicas, not the ISR: a lagging replica changes nothing */
    MARGIN_MUT_LOSTASDOWN, /* destroyed positions taken for merely away: lost reads as unavailable, and is retried */
    MARGIN_MUT_COUNT
};

struct margins {
    int young;      /* ISR size - 1 */
    int settled;    /* present positions - k (MARGIN_TIER_NONE: 0) */
    uint8_t tier;   /* enum margin_tier */
    uint8_t present, away, destroyed; /* positions of the stripe group (an EC volume) */
};

/* The margins of v in copy cp. destroyed: the node ids declared destroyed (nd of them, any order); corrupt: a read of
 * v answered CORRUPT on this server. */
void margins_of(const struct cluster_copy *cp, const struct cluster_copy_vol *v, const uint32_t *destroyed, size_t nd,
                bool corrupt, int mutant, struct margins *out);
const char *margin_tier_name(int tier); /* "none", "ok", "unavailable", "corrupt", "lost" */
int margin_mutant_parse(const char *name); /* MARGIN_MUT_*, -1: unknown (NULL or "": none) */

#endif
