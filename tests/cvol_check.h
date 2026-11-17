/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef TSDB_CVOL_CHECK_H
#define TSDB_CVOL_CHECK_H
#include <stdint.h>
/* The rows and a digest of (ts, value bits) of each named series in the cluster volume directory `dir`, opened
 * read-only in-process (a missing series: 0 rows, digest 0). 0 or the open's or a query's TSDB_ERR_*. */
int cvol_digest(const char *dir, const char *const *names, int n, uint64_t *rows, uint64_t *hash);
#endif
