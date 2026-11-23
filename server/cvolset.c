/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the volume manager (cvolset.h): NOT BUILT (r4-setchg). Nothing is set aside: a directory
 * stays as it is, as before R4.
 */
#include "cvolset.h"
#include "cluster.h"

int cvolset_record(int change, uint32_t number, const struct cluster_vol_rec *r)
{
    (void)change, (void)number, (void)r;
    return 0;
}

int cvolset_close(uint32_t number)
{
    (void)number;
    return -1;
}

/* built by r4-iface (not a stub): test_cvolfs `unnamed` */
int cvolset_unnamed(const struct cluster_vol_rec *recs, uint32_t n, uint32_t number)
{
    for (uint32_t i = 0; i < n; i++)
        if (recs[i].number == number)
            return 0;
    return !(n >= CLUSTER_REPORT_VOLS && number > recs[n - 1].number);
}

int cvolset_prepare(const char *root, uint32_t number, const struct cvolfs_rec *r)
{
    (void)root, (void)number, (void)r;
    return 0;
}
