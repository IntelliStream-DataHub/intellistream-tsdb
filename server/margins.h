/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The margins of a cluster volume (R8-2, design-cluster-data.md E15, E24): how many more losses each tier survives
 * now, and the state of the settled tier. STATUS and the metrics port print them (TOPOLOGY gets them with protocol 16:
 * no client decoder skips bytes it does not know, so a new field there needs a protocol number; owner, 2026-11-25).
 *
 *   young margin    ISR size - 1: an acknowledged row is on every ISR member, so it survives that many losses (E4, E24).
 *                   -1 when the ISR is empty (nothing is acknowledged then). None for a shared volume (NFS failover,
 *                   META_VOL_SHARED): every candidate is in its ISR, but they all see one storage and hold no copies,
 *                   so candidates - 1 would read as redundancy that does not exist (young_na; STATUS and the metrics
 *                   port leave it out).
 *   settled margin  (an EC volume only) the positions of the stripe group that are present, minus k: the fewest
 *                   positions beyond k over the volume's objects. Every object is placed on the volume's one stripe
 *                   group (E9: position -> node, never renumbered), so an object of the full k loses exactly the
 *                   positions the group loses and is the minimum; an object of k' < k data cells spans k' + m of the
 *                   positions and has at least this margin. Negative: more than m positions away, settled reads of a
 *                   whole object cannot be decoded.
 *                   Two limits, both overstating it (docs/tasks/r8o-margins-followups.md): (1) it counts MEMBERS, not
 *                   stores: a member UP in the copy whose chunk store failed (chunk_store_failed) or is DOWN, or whose
 *                   stores came back empty (E15's second LOST cause), still counts as present; the leader's report of
 *                   its cold ops' positions per object (r8f, r8n) replaces this. (2) "the minimum over objects" holds
 *                   while every object has all k + m positions (the object record's `positions` is 0, ecpart.h): R9's
 *                   degraded settle (E10, k + m - 1 positions) and E28's replacement positions not yet whole make an
 *                   object's margin lower than the group's, and R9 must count them.
 *   settled state   ok; unavailable (fewer than k positions present: retryable, TSDB_WHY_UNREACHABLE); corrupt (a
 *                   settled read of the volume answered TSDB_ERR_CORRUPT on this server: enough positions, checks
 *                   failed beyond what decoding repairs; a young part's CORRUPT does not count; until a restart, R9's
 *                   repair clears it); lost (more than m positions on nodes the operator declared destroyed:
 *                   TSDB_WHY_LOST, never retried: tsdbd sends it with retry_after_ms = TSDB_RETRY_MAX_MS, so a client
 *                   that does not know LOST gives up at once within its default budget, or waits a minute between
 *                   tries). Lost wins over unavailable, which wins over corrupt.
 *
 * A position is present while its member is UP or DRAINING in the metadata copy (DOWN and OUT are away, a member the
 * copy does not know is away) and is not declared destroyed. Every member computes the same from its copy: the member
 * states are the metadata group's judgement of the heartbeats. A member cut off from the metadata group keeps its last
 * copy and reports the margins of that copy (STATUS prints its `cluster.applied` beside them): the metadata leader's
 * numbers are the current ones. No member can be declared destroyed before R9
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
    MARGIN_MUT_SHAREDCOPIES, /* a shared volume's candidates counted as copies: young margin candidates - 1 */
    MARGIN_MUT_LOSTNOWAIT,   /* tsdbd: LOST sent with retry_after 0 (a client that does not know LOST loops at 1 ms) */
    MARGIN_MUT_YOUNGCORRUPT, /* tsdbd: any CORRUPT read of the volume, a young part's too, marks the settled tier */
    MARGIN_MUT_CORRUPTFIRST, /* corrupt ranked above unavailable: a volume short of k positions reads corrupt */
    MARGIN_MUT_COUNT
};

struct margins {
    int young;      /* ISR size - 1 (young_na: 0) */
    bool young_na;  /* a shared volume: no young margin */
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
