/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 startup and return chunk reconciler (server/sfill.h; track r9-reconcile; R9 plan D15; design-cluster-data.md
 * §5.2).
 *
 * Compares local CHUNK_LIST against the volume leader's committed OBJLIST on every start and return: missing chunks
 * (degraded settles, fsck, replaced stores) are handed to sfill at their margin's priority, and retired chunks are
 * left to committed MK_GC.
 *
 * No-op stub until r9-reconcile fills it in.
 */
#include "sfill.h"

int sreconcile_trigger(struct sfill *sf, uint32_t volume, uint32_t pos, uint64_t stripe_version)
{
    (void)sf;
    (void)volume;
    (void)pos;
    (void)stripe_version;
    return 0;
}

void sreconcile_on_start(struct sfill *sf)
{
    (void)sf;
}
