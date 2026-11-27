/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 stripe fill worker (server/sfill.h; tracks r9-fill and r9-whole).
 *
 * No-op stub until r9-fill implements the OBJLIST / kind-8 checked fill worker and r9-whole adds the whole barrier
 * and accept-while-filling paths (D10, D11).
 */
#include "sfill.h"

#include <stdlib.h>

struct sfill {
    int stub;
};

struct sfill *sfill_new(void)
{
    return calloc(1, sizeof(struct sfill));
}

void sfill_free(struct sfill *sf)
{
    free(sf);
}

int sfill_start(struct sfill *sf, const struct sfill_job_cfg *cfg)
{
    (void)sf;
    (void)cfg;
    return 0;
}

void sfill_stop(struct sfill *sf, uint32_t volume, uint32_t pos)
{
    (void)sf;
    (void)volume;
    (void)pos;
}

uint32_t sfill_report(struct sfill *sf, struct sfill_report *out, uint32_t cap)
{
    (void)sf;
    (void)out;
    (void)cap;
    return 0;
}
