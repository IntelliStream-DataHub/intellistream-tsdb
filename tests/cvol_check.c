/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The end check of a cluster volume's replica directory (tests/test_cluster.c, scen_replicate): it opens a copy of the
 * directory in-process with the cluster path's leave (meta_cluster_open_allow) and digests what a query sees: every
 * record of its log, applied or not. A copy, opened as a writer, because a read-only (follower) open reads the WAL
 * segments MANIFEST names, and a replica's MANIFEST does not follow the leader's segments (its wal_active stays at the
 * segment it opened on). Its own file because test_cluster speaks the client's headers and this one the engine's.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cvol_check.h"
#include "feed.h"
#include "internal.h"

struct dg {
    uint64_t rows, h;
    int width;
};

static int dg_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    struct dg *d = ctx;
    for (size_t i = 0; i < n; i++) {
        uint64_t x = 0;
        memcpy(&x, (const char *)v + i * (size_t)d->width, (size_t)d->width);
        d->h = (d->h ^ (uint64_t)ts[i]) * 1099511628211ull;
        d->h = (d->h ^ x) * 1099511628211ull;
    }
    d->rows += n;
    return 0;
}

int cvol_digest(const char *dir, const char *const *names, int n, uint64_t *rows, uint64_t *hash)
{
    char copy[4400], cmd[18000];
    snprintf(copy, sizeof copy, "%s.check", dir);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s' && rm -f '%s/LOCK'", copy, dir, copy, copy);
    if (system(cmd) != 0)
        return TSDB_ERR_IO;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    eng *db = NULL;
    meta_cluster_open_allow(true);
    int rc = eng_open(copy, &o, &db);
    meta_cluster_open_allow(false);
    if (rc != 0)
        return rc;
    for (int k = 0; k < n; k++) {
        uint32_t id;
        struct dg d = {0, 14695981039346656037ull, 4};
        rc = eng_series_lookup(db, names[k], &id);
        if (rc == 0) {
            int type = eng_series_type(db, id);
            d.width = type == TSDB_FLOAT64 || type == TSDB_INT64 ? 8 : 4;
            rc = eng_query_raw_ex(db, id, INT64_MIN / 2, INT64_MAX / 2, false, 0, dg_fn, &d);
        }
        if (rc == TSDB_ERR_NOTFOUND)
            rc = 0, d.rows = 0, d.h = 0;
        if (rc != 0) {
            eng_close(db);
            return rc;
        }
        rows[k] = d.rows;
        hash[k] = d.h;
    }
    rc = eng_close(db);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", copy);
    if (system(cmd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    return rc;
}
