/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The granule cache and the open objects of a tier (src/eccache.h). r7-iface's stubs read a test-marked part's map;
 * r7-seam fills them.
 */
#include "eccache.h"
#include "stats_priv.h"

int ec_granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                    bool verify)
{
    return granule_read_map(c, p, g, type, ts, v, cap, verify);
}

uint64_t eccache_part_bytes(const part *p) { return p->map_len; }

uint64_t eccache_resident(const part *p, uint64_t off, uint64_t len)
{
    if (off > p->map_len || len > p->map_len - off)
        return 0;
    return resident_bytes_range(p->map + off, (size_t)len);
}
