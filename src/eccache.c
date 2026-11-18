/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The granule cache and the open objects of a tier (src/eccache.h). r7-iface's stubs read a test-marked part's map;
 * r7-seam fills them.
 */
#include "eccache.h"
#include "ecpart.h"
#include "stats_priv.h"

int ec_granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                    bool verify)
{
    EC_CALLED(EC_CALL_GRANULE_READ);
    return granule_read_map(c, p, g, type, ts, v, cap, verify);
}

uint64_t eccache_part_bytes(const part *p)
{
    EC_CALLED(EC_CALL_PART_BYTES);
    return p->map_len;
}

uint64_t eccache_resident(const part *p, uint64_t off, uint64_t len)
{
    EC_CALLED(EC_CALL_RESIDENT);
    if (off > p->map_len || len > p->map_len - off)
        return 0;
    return resident_bytes_range(p->map + off, (size_t)len);
}

int eccache_tier_open(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_OPEN);
    return 0;
}

void eccache_tier_close(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_CLOSE);
}
