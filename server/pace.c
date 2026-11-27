/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 rebuild and balancer traffic pacer (server/pace.h; track r9-pace; R9 plan D18).
 *
 * No-op stub until r9-pace fills in the token buckets and p99 halving logic.
 */
#include "pace.h"

#include <string.h>

void pace_configure(const struct pace_cfg *cfg)
{
    (void)cfg;
}

void pace_set_p99_halved(int halved)
{
    (void)halved;
}

void pace_job_begin(enum pace_dir dir, int margin)
{
    (void)dir;
    (void)margin;
}

void pace_job_end(enum pace_dir dir, int margin)
{
    (void)dir;
    (void)margin;
}

uint64_t pace_take(enum pace_dir dir, uint64_t bytes, int margin)
{
    /* Stub until r9-pace fills this in: never blocks. */
    (void)dir;
    (void)bytes;
    (void)margin;
    return 0;
}

void pace_stats(struct pace_stats *out)
{
    if (out)
        memset(out, 0, sizeof *out);
}
