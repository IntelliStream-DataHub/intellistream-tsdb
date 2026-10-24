/*
 * Deletes and replaces of a time range of one series (tomb.c has the design).
 *
 * A delete is committed like a large insert: the memtable is flushed first, so
 * every row acknowledged before the delete is in a part with a lower block
 * number, then the tombstone parts are committed through parts_commit (one
 * intent for several) and published under publish_lock. Which partitions get
 * one is decided under publish_lock too: those whose listed parts hold rows of
 * the series in the range. Block numbers are assigned under that lock, so a
 * part published later is newer than the delete and needs no tombstone.
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
    uint32_t lo = 0, hi = p->ng;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (p->g[mid].series < s || (p->g[mid].series == s && p->g[mid].ts_max < start))
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < p->ng && p->g[lo].series == s && p->g[lo].ts_min < end;
}

/* Writes a pending tombstone-only part for [start, end) of series in one partition. */
static int tomb_part(eng *db, uint32_t series, int64_t partition, int64_t start, int64_t end, part **out)
{
    int64_t lo, hi;
    if (__builtin_mul_overflow(partition, db->partition_ms, &lo))
        lo = partition < 0 ? INT64_MIN : INT64_MAX;
    if (__builtin_add_overflow(lo, db->partition_ms, &hi))
        hi = INT64_MAX;
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

int eng_replace_range(eng *db, int type, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                      const void *v, size_t n)
{
    if (db->opt.read_only)
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

    /* rows acknowledged before this call get older block numbers than its parts */
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    bool pending = db->active->rows > 0 || db->nflushing > 0;
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    if (pending && (rc = mt_flush(db)) != 0)
        return rc;

    part_list rows = {0}, all = {0};
    if (n && (rc = insert_rows(db, st, NULL, series, ts, v, n, 0, &rows)) != 0) {
        parts_discard(db, rows.p, rows.n);
        free(rows.p);
        return rc;
    }

    pthread_mutex_lock(&db->publish_lock);
    int64_t *parts_of = NULL;
    size_t np = 0;
    pthread_mutex_lock(&db->lock);
    parts_of = malloc((db->nparts ? db->nparts : 1) * sizeof *parts_of);
    for (size_t i = 0; parts_of && i < db->nparts; i++) {
        part *p = db->parts[i]; /* sorted by partition */
        if (p->max_ts < start || p->min_ts >= end || (np && parts_of[np - 1] == p->partition))
            continue;
        if (part_has(p, series, start, end))
            parts_of[np++] = p->partition;
    }
    pthread_mutex_unlock(&db->lock);
    all.cap = np + rows.n + 1;
    if (!parts_of || !(all.p = malloc(all.cap * sizeof *all.p)))
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < np; i++) {
        part *t = NULL;
        rc = tomb_part(db, series, parts_of[i], start, end, &t);
        if (rc == 0 && t)
            all.p[all.n++] = t;
    }
    if (rc == 0) {
        for (size_t i = 0; i < rows.n; i++)
            all.p[all.n++] = rows.p[i]; /* after the tombstones: newer */
        rows.n = 0; /* `all` owns them now */
        rc = parts_commit(db, all.p, all.n);
    }
    if (rc == 0) {
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < all.n; i++)
            db_parts_insert_locked(db, all.p[i]);
        pthread_cond_signal(&db->merge_cv);
        pthread_mutex_unlock(&db->lock);
    }
    pthread_mutex_unlock(&db->publish_lock);
    free(parts_of);
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
