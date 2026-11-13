/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Deletes and replaces of a time range of one series (tomb.c has the design).
 *
 * A delete is committed like a large insert: the memtable is flushed first, so
 * every row acknowledged before the delete is in a part with a lower block
 * number, then the tombstone parts are committed through parts_commit (one
 * intent for several) and published under publish_lock. Which partitions get
 * one: those whose listed parts hold rows of the series in the range, once
 * before publish_lock (their tombstone parts are written and synced outside
 * it, so writers do not wait for that) and again under it, for parts listed in
 * between. Block numbers are assigned under that lock, so a part published
 * later is newer than the delete and needs no tombstone.
 *
 * A replace commits its tombstone parts and the parts of its new rows as one
 * commit, the tombstone parts first: they get the lower block numbers, so the
 * tombstones hide the old rows and not the new ones, and readers see the old
 * range or the new one, never a mix.
 */
#include "internal.h"

#include <stdlib.h>

/* whether part p holds rows of series s in [start, end) */
static bool part_has(const part *p, uint32_t s, int64_t start, int64_t end)
{
    uint32_t gi, gj;
    part_find(p, s, start, end, &gi, &gj);
    return gi < gj;
}

/* Writes a pending tombstone-only part for [start, end) of series in one partition. */
static int tomb_part(eng *db, uint32_t series, int64_t partition, int64_t start, int64_t end, part **out)
{
    int64_t lo, hi;
    partition_range(db, partition, &lo, &hi);
    tomb t = {.series = series, .start = MAX(start, lo), .end = MIN(end, hi)};
    part_writer w;
    int rc = pw_begin(&w, db, partition, 0);
    if (rc)
        return rc;
    if ((rc = pw_set_tombs(&w, &t, 1)) != 0) {
        pw_abort(&w);
        return rc;
    }
    return pw_finish_pending(&w, 0, 0, 0, out);
}

/* The partitions, ascending, whose listed parts hold rows of the series in [start, end). */
static int range_partitions(eng *db, uint32_t series, int64_t start, int64_t end, int64_t **out, size_t *n)
{
    *n = 0;
    pthread_mutex_lock(&db->lock);
    int64_t *ps = malloc((db->nparts ? db->nparts : 1) * sizeof *ps);
    for (size_t i = 0; ps && i < db->nparts; i++) {
        part *p = db->parts[i]; /* sorted by partition */
        if (p->max_ts < start || p->min_ts >= end || (*n && ps[*n - 1] == p->partition))
            continue;
        if (part_has(p, series, start, end))
            ps[(*n)++] = p->partition;
    }
    pthread_mutex_unlock(&db->lock);
    *out = ps;
    return ps ? 0 : TSDB_ERR_NOMEM;
}

static int add_tomb_part(eng *db, uint32_t series, int64_t partition, int64_t start, int64_t end, part_list *l)
{
    if (l->n == l->cap) {
        part **x = realloc(l->p, (l->cap * 2 + 1) * sizeof *x);
        if (!x)
            return TSDB_ERR_NOMEM;
        l->p = x;
        l->cap = l->cap * 2 + 1;
    }
    part *t = NULL;
    int rc = tomb_part(db, series, partition, start, end, &t);
    if (rc == 0 && t)
        l->p[l->n++] = t;
    return rc;
}

int eng_replace_range(eng *db, int type, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                      const void *v, size_t n)
{
    if (db->opt.read_only || db->rp) /* a replica gets deletes as markers (replica.c) */
        return TSDB_ERR_READONLY;
    int st = eng_series_type(db, series);
    if (st < 0 || (n && (type != st || !ts || !v)))
        return TSDB_ERR_INVALID;
    for (size_t i = 0; i < n; i++)
        if (ts[i] < start || ts[i] >= end)
            return TSDB_ERR_INVALID;
    if (start >= end)
        return 0;
    int rc = eng_check(db);
    if (rc)
        return rc;

    /* rows acknowledged before this call get older block numbers than its parts; small writes into the range wait
     * from here until they are published (lw_fence) */
    lw_fence f;
    lw_fence_range(&f, series, start, end);
    if (lw_enter(db, &f) && (rc = mt_flush(db)) != 0) {
        lw_leave(db, &f);
        return rc;
    }
    /* A fresh series gets its class from these rows, as from a large insert's: placed undecided in class 0, they
     * would stay there when a later insert classifies the series from the beginning of time (two copies of one
     * row in two partitions, classes.c) */
    if (n && (rc = cls_classify_insert(db, NULL, series, ts, n)) != 0) {
        lw_leave(db, &f);
        return rc;
    }

    /* one frozen class history from placing the rows until they are listed (classes.c) */
    cls_freeze(db);
    part_list rows = {0}, all = {0};
    if (n && (rc = insert_rows(db, st, NULL, series, ts, v, n, 0, &rows, true, cls_uniform(db, NULL, series, 0),
                               NULL, NULL)) != 0) {
        lw_leave(db, &f);
        cls_thaw(db);
        parts_discard(db, rows.p, rows.n);
        free(rows.p);
        return rc;
    }

    /* The tombstone parts are written before publish_lock, for the partitions listed now: writers and flushes
     * are not held up while they are synced. Under the lock, a partition that got rows of the series since
     * then (a part published in between is older than this delete) gets one more, written there: rare. */
    int64_t *before = NULL, *now = NULL;
    size_t nb = 0, nn = 0;
    if ((rc = range_partitions(db, series, start, end, &before, &nb)) == 0) {
        all.cap = nb + rows.n + 1;
        if (!(all.p = malloc(all.cap * sizeof *all.p)))
            rc = TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; rc == 0 && i < nb; i++)
        rc = add_tomb_part(db, series, before[i], start, end, &all);
    if (rc == 0 && db->test_precommit)
        db->test_precommit(db->test_precommit_arg);
    if (rc == 0 && db->jr) { /* the replicated backend: the marker names every file, tombstones first, then the rows */
        part **mp = malloc((all.n + rows.n + 1) * sizeof *mp);
        if (!mp) {
            rc = TSDB_ERR_NOMEM;
        } else {
            memcpy(mp, all.p, all.n * sizeof *mp);
            memcpy(mp + all.n, rows.p, rows.n * sizeof *mp);
            struct jr_commit_arg ja = {mp, all.n + rows.n, rows.n ? MK_OP_REPLACE : MK_OP_DELETE};
            rc = jr_intent(db, JR_COMMIT, &ja);
            free(mp);
        }
    }
    pthread_mutex_lock(&db->publish_lock);
    if (rc == 0)
        rc = range_partitions(db, series, start, end, &now, &nn);
    for (size_t i = 0, j = 0; rc == 0 && i < nn; i++) { /* both sorted */
        while (j < nb && before[j] < now[i])
            j++;
        if (j == nb || before[j] != now[i]) {
            if (db->jr) { /* its marker is out already (jr_serial makes this impossible: a bug) */
                eng_fail(db, "a partition appeared between a replicated delete's marker and its commit", EIO);
                rc = TSDB_ERR_IO;
            } else {
                rc = add_tomb_part(db, series, now[i], start, end, &all);
            }
        }
    }
    if (rc == 0 && all.n + rows.n > all.cap) {
        part **x = realloc(all.p, (all.n + rows.n) * sizeof *x);
        if (x) {
            all.p = x;
            all.cap = all.n + rows.n;
        } else {
            rc = TSDB_ERR_NOMEM;
        }
    }
    if (rc == 0) {
        for (size_t i = 0; i < rows.n; i++)
            all.p[all.n++] = rows.p[i]; /* after the tombstones: newer */
        rows.n = 0; /* `all` owns them now */
        rc = eng_abandoned(); /* see cur_abandon: before any block number is taken */
        if (rc == 0)
            rc = parts_commit(db, all.p, all.n);
        if (rc == 0)
            jr_commit(db, JR_COMMIT, &all);
    }
    if (rc == 0) {
        lt_sec lsec; /* the latest table: tombstones, then the rows, in the section that lists them (3.4) */
        lt_begin(db, &lsec);
        lt_fold(db, &lsec, all.p, all.n);
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < all.n; i++)
            db_parts_insert_locked(db, all.p[i]);
        pthread_cond_signal(&db->merge_cv);
        pthread_mutex_unlock(&db->lock);
        lt_end(db, &lsec);
    }
    pthread_mutex_unlock(&db->publish_lock);
    lw_leave(db, &f); /* published (newer small writes win from now on) or failed */
    cls_thaw(db);
    free(before);
    free(now);
    if (rc == 0 && all.n) {
        manifest_publish(db); /* published: like an insert, whatever happens to MANIFEST (it fails the engine) */
    } else if (rc) {
        parts_discard(db, all.p, all.n);
        parts_discard(db, rows.p, rows.n);
    }
    free(all.p);
    free(rows.p);
    return rc;
}

int eng_delete_range(eng *db, uint32_t series, int64_t start, int64_t end)
{
    return eng_replace_range(db, -1, series, start, end, NULL, NULL, 0);
}
