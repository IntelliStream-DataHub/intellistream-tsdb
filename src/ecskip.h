/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R7 (r7-skip, design-cluster-data.md 2.4): which parts merges, rewrites, optimize and the backpressure of inserts
 * leave alone. An .ec part (p->ec) holds no granule bytes in its map: a merge that takes it as a source fails
 * (pw_copy_granule) or, through the seam, would rewrite a settled object into a .part again. A settling part
 * (p->settling) is being turned into one: a merge that took it would list an output the swap does not know, and the
 * swap would then bring back rows the output already holds, ranked by the source's old block range. So both are
 * barriers for merge ranges (as a damaged part is: the ranges on either side merge, never one across), and neither
 * is ever a solo rewrite, an optimize input or a TTL rewrite. Drops are different: they take every part of a
 * partition, .ec and settling ones included, and never wait for a settle (the single-server swap checks its inputs
 * under db->lock and rolls back over retired sources, r7-swap items 2-4; a cluster settle gates drops in its steps
 * 3-6). Callers hold db->lock (p->settling is under it; p->ec is set before a part is listed, or by the test hook
 * under db->lock).
 *
 * What a settle must therefore keep (r7-settle): set `settling` only under db->lock, on a listed part that is not
 * merging and not dropped, in a partition that is not dropping. Settle no part that rewrite_alone() (merge.c) would
 * still take: a v8-header part or one only inserts wrote is never rewritten once it is .ec, so it would keep
 * parts_v8=1 in META, or fast-codec blocks without zstd, for ever. And a class switch's purge (cls_purge,
 * eng_optimize_keys) cannot merge across an .ec part: a row hidden by a tombstone in a part above an .ec part
 * that holds it is not merged away there.
 */
#ifndef TSDB_ECSKIP_H
#define TSDB_ECSKIP_H

#include "internal.h"

/* r7-skip's planted bugs (tests/test_ecskip.c --mutant NAME; the Makefile requires each to fail it):
 *  mergeec        an .ec part is no barrier for merge ranges (the merge thread's and optimize's)
 *  soloec         an .ec part may be rewritten alone (idle rewrite, optimize's lone part, the TTL rewrite)
 *  dropwaitsettle a drop waits for a settling part of a partition it drops, like for a merging one
 *  waitcountec    db_wait_merges counts .ec parts towards PARTS_TO_DELAY_INSERT
 *  waitnobreak    db_wait_merges waits (until wait_stall_ms) when only .ec or settling barriers break the pairs
 *  waitwall       db_wait_merges takes .ec and settling parts for no barrier when it looks for a pair
 *  waitsettle     db_wait_merges keeps an insert waiting while a settle of the partition is in flight
 *  mergecountec   merge selection counts .ec parts towards the partition's parts (lowering the merge base) */
enum {
    ECSKIP_MUT_NONE = 0,
    ECSKIP_MUT_MERGEEC,
    ECSKIP_MUT_SOLOEC,
    ECSKIP_MUT_DROPWAITSETTLE,
    ECSKIP_MUT_WAITCOUNTEC,
    ECSKIP_MUT_WAITNOBREAK,
    ECSKIP_MUT_WAITWALL,
    ECSKIP_MUT_WAITSETTLE,
    ECSKIP_MUT_MERGECOUNTEC,
};
extern int ecskip_mutant; /* src/merge.c */

static inline bool part_is_ec(const part *p) { return atomic_load_explicit(&p->ec, memory_order_acquire) != NULL; }

/* No merge range may span it (select_merge_locked, optimize_where, db_wait_merges's pairs) */
static inline bool part_merge_barrier(const part *p)
{
    return p->settling || (part_is_ec(p) && ecskip_mutant != ECSKIP_MUT_MERGEEC);
}

/* Never rewritten alone (the idle rewrite, optimize's lone part, the TTL rewrite) */
static inline bool part_no_rewrite(const part *p)
{
    return p->settling || (part_is_ec(p) && ecskip_mutant != ECSKIP_MUT_SOLOEC);
}

#endif
