/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The settle thread and its hooks (src/settle.h). r7-iface's stubs; r7-settle fills them.
 */
#include "settle.h"
#include "ecpart.h"

int ecsettle_tier_open(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_OPEN);
    return 0;
}

void ecsettle_tier_quiesce(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_QUIESCE);
}

void ecsettle_tier_close(struct tier *t)
{
    (void)t;
    EC_CALLED(EC_CALL_TIER_CLOSE);
}

int ecsettle_eng_open(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_OPEN);
    return 0;
}

void ecsettle_eng_close(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_CLOSE);
}
