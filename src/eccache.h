/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The seam between an .ec part's granules and the chunk store (R7, r7-seam): the granule cache of the tsdb handle's
 * tier (keyed by oid and granule offset, LRU, a byte budget) and the LRU of open objects, reached through p->ec. No
 * read here runs under db->lock, latest_rw, buf_lock or publish_lock. r7-iface left stubs that read a test-marked
 * part's map (src/ecpart.h, eng_test_mark_ec), as granule_read does for a .part.
 */
#ifndef TSDB_ECCACHE_H
#define TSDB_ECCACHE_H

#include "internal.h"

/* granule_read for an .ec part (p->ec set): the same contract (src/internal.h). Stub: granule_read_map. */
int ec_granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                    bool verify);
/* The bytes an .ec part counts as its file in part_walk (statistics: the four byte columns add up to it). Stub:
 * p->map_len. */
uint64_t eccache_part_bytes(const part *p);
/* Bytes of the object's [off, off + len) that are resident (in the cache, for an .ec part): the residency of the
 * statistics. Stub: the page cache's residency of the map, as for a .part (0 for a range outside it). */
uint64_t eccache_resident(const part *p, uint64_t off, uint64_t len);

/* The tier's hooks (src/tier.h): t->cache. Stubs: nothing (counted). eccache_part_bytes runs under db->lock as well
 * (stats_walk, eng_foreach_part): a number the part keeps, no I/O and no lock. */
int eccache_tier_open(struct tier *t);
void eccache_tier_close(struct tier *t);
#endif
