/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The swap of a settle and its recovery (src/ecswap.h). r7-iface's stub; r7-swap fills it.
 */
#include "ecswap.h"

#include <stdio.h>

int ecswap_recover(eng *db, const char *name, char *buf, size_t len)
{
    (void)db;
    (void)buf;
    (void)len;
    fprintf(stderr, "tsdb: insert intent %s is torn: the insert is rolled back\n", name);
    return 0;
}
