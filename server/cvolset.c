/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the volume manager (cvolset.h): NOT BUILT (r4-setchg). Nothing is set aside: a directory
 * stays as it is, as before R4.
 */
#include "cvolset.h"

void cvolset_record(int change, uint32_t number, const struct cluster_vol_rec *r)
{
    (void)change, (void)number, (void)r;
}

int cvolset_prepare(const char *root, uint32_t number, const struct cvolfs_rec *r)
{
    (void)root, (void)number, (void)r;
    return 0;
}
