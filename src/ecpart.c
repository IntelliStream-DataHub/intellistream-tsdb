/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled (.ec) parts: the record codec, the writer and the open (src/ecpart.h has the layout). r7-iface left the
 * stubs below, each doing what the engine does today for the names and parts they are given (an .ec file is not a
 * part to it, and no part has p->ec but one eng_test_mark_ec marked); r7-ecfile fills them. eng_test_mark_ec is
 * r7-iface's test hook and stays.
 */
#include "ecpart.h"
#include "tier.h"

#include <stdio.h>
#include <stdlib.h>

atomic_uint_fast64_t ec_calls[EC_CALL_N];

int ecpart_load(eng *db, const char *name, bool read_only, part **out)
{
    EC_CALLED(EC_CALL_LOAD);
    (void)read_only;
    *out = NULL;
    fprintf(stderr, "tsdb: %s/%s is an erasure-coded part, which this build cannot read: the open fails\n", db->dir,
            name);
    return TSDB_ERR_CORRUPT;
}

int ecpart_follower_open(eng *db, const char *name, part **out)
{
    EC_CALLED(EC_CALL_FOLLOWER_OPEN);
    *out = NULL;
    return tier_follower_ec(db, name); /* r7-tier: the follower stops with one line (D7) */
}

bool ec_index_intact(const part *p)
{
    EC_CALLED(EC_CALL_INDEX_INTACT);
    return part_map_intact(p);
}

int ec_cls_record(eng *db, const part *p)
{
    EC_CALLED(EC_CALL_CLS_RECORD);
    (void)db;
    (void)p;
    return 0;
}

void ecpart_free(struct ecpart *e)
{
    if (!e)
        return;
    free(e->rec.sums);
    free(e);
}

int eng_test_mark_ec(eng *db, part *p, unsigned flags)
{
    int rc = TSDB_ERR_NOTFOUND;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i] == p) {
            rc = 0;
            break;
        }
    if (rc == 0 && p->merging)
        rc = TSDB_ERR_LOCKED; /* the merge would meet an .ec source (pw_copy_granule) and fail */
    if (rc == 0 && (flags & EC_MARK_EC) && !p->ec) {
        struct ecpart *e = calloc(1, sizeof *e);
        if (!e) {
            rc = TSDB_ERR_NOMEM;
        } else {
            e->test_mark = true;
            e->tier = db->tier;
            e->volume = db->tier_vol;
            atomic_store_explicit(&p->ec, e, memory_order_release); /* readers run beside this store */
        }
    }
    if (rc == 0)
        p->settling = (flags & EC_MARK_SETTLING) != 0;
    pthread_mutex_unlock(&db->lock);
    return rc;
}
