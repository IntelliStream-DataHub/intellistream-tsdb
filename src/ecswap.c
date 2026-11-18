/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The swap of a settle and its recovery (src/ecswap.h). r7-iface's stub; r7-swap fills it.
 */
#include "ecswap.h"
#include "ecpart.h"

#include <stdio.h>

int ecswap_recover(eng *db, const char *name, char *buf, size_t len)
{
    EC_CALLED(EC_CALL_SWAP_RECOVER);
    (void)db;
    (void)buf;
    (void)len;
    fprintf(stderr, "tsdb: insert intent %s is torn: the insert is rolled back\n", name);
    return 0;
}

int ecswap_eng_open(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_OPEN);
    return 0;
}

void ecswap_eng_close(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_CLOSE);
}
