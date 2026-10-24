/*
 * Background merges, modelled on ClickHouse's SimpleMergeSelector.
 *
 * Within a partition, parts are ordered by block number and only contiguous
 * ranges are merged, so a merged part's block range never overlaps another
 * part's. A range is allowed when its parts are balanced enough:
 *
 *     (sum_size + count * fixed_cost) / (max_size + fixed_cost) >= base
 *
 * where base drops from 5 towards 2 as the parts age or the partition fills
 * up, and the best allowed range minimizes (sum + fixed * count) / (count - 1.9).
 *
 * Merging is a k-way merge on (series, ts). Equal keys collapse to the row of
 * the newest source part (highest block number), which is what makes the
 * table behave like a ReplacingMergeTree. When a whole source granule sorts
 * strictly before everything else that remains, it is copied as compressed bytes.
 */
#include "internal.h"

#include <math.h>
#include <stdlib.h>

#define MERGE_BASE 5.0
#define MERGE_FIXED_COST (1u << 20) /* small parts compress well; ClickHouse uses 5 MiB */
#define MIN_SIZE_TO_LOWER_BASE (1u << 20)
#define MAX_SIZE_TO_LOWER_BASE (100ull << 30)
#define MIN_PARTS_TO_LOWER_BASE 10
#define MAX_PARTS_TO_LOWER_BASE 50

static double unit(double v, double lo, double hi) { return v <= lo ? 0 : v >= hi ? 1 : (v - lo) / (hi - lo); }
static double lerp(double a, double b, double t) { return a + (b - a) * t; }

static bool merge_allowed(double sum, double max_size, double min_age, double partition_parts, double count)
{
    double size_norm = unit(log1p(sum), log1p(MIN_SIZE_TO_LOWER_BASE), log1p((double)MAX_SIZE_TO_LOWER_BASE));
    double max_age = lerp(3600, 30 * 86400, size_norm);
    double age_norm = unit(min_age, 10, max_age);
    double parts_norm = unit(partition_parts, MIN_PARTS_TO_LOWER_BASE, MAX_PARTS_TO_LOWER_BASE);
    double base = lerp(MERGE_BASE, 2, MIN(1.0, age_norm + parts_norm));
    return (sum + count * MERGE_FIXED_COST) / (max_size + MERGE_FIXED_COST) >= base;
}

static double merge_score(double count, double sum) { return (sum + MERGE_FIXED_COST * count) / (count - 1.9); }

/* Picks the best range over all partitions. Caller holds db->lock. The chosen
 * parts are marked merging and referenced. */
static size_t select_merge_locked(eng *db, part **sel)
{
    time_t now = time(NULL);
    double best = INFINITY;
    size_t best_i = 0, best_n = 0;
    for (size_t a = 0; a < db->nparts;) {
        size_t b = a + 1;
        while (b < db->nparts && db->parts[b]->partition == db->parts[a]->partition)
            b++;
        size_t nparts = b - a;
        if (nparts >= 2) {
            time_t newest = 0;
            for (size_t i = a; i < b; i++)
                newest = MAX(newest, db->parts[i]->written);
            bool idle = db->opt.merge_idle_s >= 0 && now - newest >= db->opt.merge_idle_s;
            for (size_t i = a; i < b; i++) {
                double sum = 0, max_size = 0, min_age = INFINITY;
                for (size_t j = i; j < b && j - i < MAX_MERGE_PARTS; j++) {
                    part *p = db->parts[j];
                    if (p->merging)
                        break;
                    double size = (double)p->map_len;
                    if (sum + size > (double)db->opt.max_part_bytes && j > i)
                        break;
                    sum += size;
                    max_size = MAX(max_size, size);
                    min_age = MIN(min_age, (double)(now - p->created));
                    double count = (double)(j - i + 1);
                    if (count < 2)
                        continue;
                    if (!idle && !merge_allowed(sum, max_size, min_age, (double)nparts, count))
                        continue;
                    double s = merge_score(count, sum);
                    if (s < best) {
                        best = s;
                        best_i = i;
                        best_n = j - i + 1;
                    }
                }
            }
        }
        a = b;
    }
    for (size_t k = 0; k < best_n; k++) {
        sel[k] = db->parts[best_i + k];
        sel[k]->merging = true;
        part_ref(sel[k]);
    }
    return best_n;
}

/* ------------------------------------------------------------- k-way merge */

typedef struct cursor {
    part *p;
    uint32_t gi; /* current granule */
    bool loaded; /* rows of granule gi are decoded into ts/v */
    size_t pos, n;
    int64_t *ts;
    uint8_t *v; /* slots of type */
    int type;   /* of the loaded granule's series */
} cursor;

static inline bool cur_done(const cursor *c) { return c->gi >= c->p->ng; }

/* Smallest remaining key; for an undecoded granule its first row is (series, ts_min). */
static inline void cur_key(const cursor *c, uint32_t *s, int64_t *t)
{
    const granule *g = &c->p->g[c->gi];
    *s = g->series;
    *t = c->loaded ? c->ts[c->pos] : g->ts_min;
}

static inline bool key_lt(uint32_t s1, int64_t t1, uint32_t s2, int64_t t2)
{
    return s1 < s2 || (s1 == s2 && t1 < t2);
}

static int cur_load(eng *db, codec_ctx *cc, cursor *c)
{
    if (!c->ts) {
        c->ts = malloc(c->p->grows * sizeof(int64_t));
        c->v = malloc(c->p->grows * sizeof(uint64_t));
        if (!c->ts || !c->v)
            return TSDB_ERR_NOMEM;
    }
    const granule *g = &c->p->g[c->gi];
    c->type = series_type(&db->series, g->series);
    if (c->type < 0)
        return TSDB_ERR_CORRUPT; /* a granule of a series the dictionary does not have */
    int rc = granule_read(cc, c->p, g, c->type, c->ts, c->v);
    if (rc)
        return rc;
    c->loaded = true;
    c->pos = 0;
    c->n = g->count;
    return 0;
}

/* Drops the cursor's current row (a duplicate key replaced by a newer part). */
static int cur_skip(eng *db, codec_ctx *cc, cursor *c)
{
    int rc;
    if (!c->loaded && (rc = cur_load(db, cc, c)) != 0)
        return rc;
    if (++c->pos == c->n) {
        c->loaded = false;
        c->gi++;
    }
    return 0;
}

int merge_parts(eng *db, part **src, size_t k, part **out)
{
    *out = NULL;
    cursor *cur = calloc(k, sizeof *cur);
    if (!cur)
        return TSDB_ERR_NOMEM;
    uint64_t min_block = UINT64_MAX, max_block = 0;
    uint32_t level = 0;
    for (size_t i = 0; i < k; i++) {
        cur[i].p = src[i];
        min_block = MIN(min_block, src[i]->min_block);
        max_block = MAX(max_block, src[i]->max_block);
        level = MAX(level, src[i]->level);
    }
    uint64_t wal_seq = 0;
    for (size_t i = 0; i < k; i++)
        wal_seq = MAX(wal_seq, src[i]->wal_seq);
    part_writer w;
    int rc = pw_begin(&w, db, src[0]->partition, wal_seq);
    if (rc) {
        free(cur);
        return rc;
    }
    codec_ctx *cc = cc_get(db);
    uint64_t rows = 0, copied = 0;
    if (!cc)
        rc = TSDB_ERR_NOMEM;

    while (rc == 0) {
        if (atomic_load_explicit(&db->stop, memory_order_relaxed)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        /* the cursor with the smallest key (the newest part on ties), and the
         * next smallest key as a bound; an undecoded granule's key is exact:
         * its first row is (series, ts_min) */
        cursor *c = NULL;
        uint32_t cs = 0, bs = UINT32_MAX;
        int64_t ct = 0, bt = INT64_MAX;
        bool has_bound = false;
        for (size_t i = 0; i < k; i++) {
            if (cur_done(&cur[i]))
                continue;
            uint32_t s;
            int64_t t;
            cur_key(&cur[i], &s, &t);
            bool better = !c || key_lt(s, t, cs, ct) ||
                          (s == cs && t == ct && cur[i].p->max_block > c->p->max_block);
            if (better) {
                if (c) {
                    bs = cs;
                    bt = ct;
                    has_bound = true;
                }
                c = &cur[i];
                cs = s;
                ct = t;
            } else if (!has_bound || key_lt(s, t, bs, bt)) {
                bs = s;
                bt = t;
                has_bound = true;
            }
        }
        if (!c)
            break;
        const granule *g = &c->p->g[c->gi];
        if (!c->loaded) {
            bool before_rest = !has_bound || g->series < bs || (g->series == bs && g->ts_max < bt);
            bool full = g->count >= w.grows;
            if (before_rest && full && (w.pend_n == 0 || w.pend_series != g->series)) {
                rc = pw_copy_granule(&w, c->p, g);
                rows += g->count;
                copied++;
                c->gi++;
                continue;
            }
            if ((rc = cur_load(db, cc, c)) != 0)
                break;
        }
        size_t end = c->n;
        bool tie = false;
        if (has_bound && g->series == bs) {
            /* rows strictly before the bound are unique to this cursor */
            end = c->pos + lower_bound_i64(c->ts + c->pos, c->n - c->pos, bt);
            if (end == c->pos) {
                end = c->pos + 1; /* this row's key equals the bound: it wins, the others are dropped */
                tie = true;
            }
        }
        rc = pw_add(&w, g->series, c->type, c->ts + c->pos, c->v + c->pos * type_width(c->type), end - c->pos);
        rows += end - c->pos;
        c->pos = end;
        if (c->pos == c->n) {
            c->loaded = false;
            c->gi++;
        }
        for (size_t i = 0; tie && rc == 0 && i < k; i++) {
            if (&cur[i] == c || cur_done(&cur[i]))
                continue;
            uint32_t s;
            int64_t t;
            cur_key(&cur[i], &s, &t);
            if (s == bs && t == bt)
                rc = cur_skip(db, cc, &cur[i]);
        }
    }

    if (cc)
        cc_put(db, cc);
    for (size_t i = 0; i < k; i++) {
        free(cur[i].ts);
        free(cur[i].v);
    }
    free(cur);
    if (rc) {
        pw_abort(&w);
        return rc;
    }
    rc = pw_finish(&w, min_block, max_block, level + 1, out);
    if (rc == 0 && *out) {
        time_t written = 0;
        for (size_t i = 0; i < k; i++)
            written = MAX(written, src[i]->written);
        (*out)->written = written; /* a merge is not a write: the partition stays as idle as it was */
    }
    if (rc == 0) {
        atomic_fetch_add(&db->st_merges, 1);
        atomic_fetch_add(&db->st_merged_rows, rows);
        atomic_fetch_add(&db->st_passthrough, copied);
    }
    return rc;
}

/* Replaces src with out in the parts list. Caller holds db->lock and still
 * owns one reference to each source part (dropped later by the caller). */
void merge_commit_locked(eng *db, part **src, size_t k, part *out)
{
    bool dropped = false;
    for (size_t i = 0; i < k; i++) {
        src[i]->merging = false;
        dropped |= src[i]->dropped;
    }
    if (out && !dropped && eng_check(db)) {
        part_unref(out); /* failed engine: no renames; the next open sees what a crash here leaves */
    } else if (out && !dropped) {
        db_parts_insert_locked(db, out);
        for (size_t i = 0; i < k; i++)
            db_retire_locked(db, src[i]);
    } else if (out) {
        /* a source was dropped meanwhile: the merged data must not come back */
        atomic_store(&out->obsolete, true);
        part_unref(out);
    }
    pthread_cond_broadcast(&db->merged_cv);
}

void *merge_thread_main(void *arg)
{
    eng *db = arg;
    part *sel[MAX_MERGE_PARTS];
    pthread_mutex_lock(&db->lock);
    while (!atomic_load(&db->stop)) {
        size_t k = eng_check(db) ? 0 : select_merge_locked(db, sel); /* a failed engine writes nothing more */
        if (!k) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&db->merge_cv, &db->lock, &ts);
            continue;
        }
        pthread_mutex_unlock(&db->lock);
        part *out = NULL;
        int rc = merge_parts(db, sel, k, &out);
        if (rc != 0 && rc != TSDB_ERR_STOPPED)
            fprintf(stderr, "tsdb: merge of %s..%s failed: %s\n", sel[0]->name, sel[k - 1]->name,
                    tsdb_strerror(rc));
        pthread_mutex_lock(&db->lock);
        merge_commit_locked(db, sel, k, out);
        pthread_mutex_unlock(&db->lock);
        if (out)
            manifest_publish(db);
        for (size_t i = 0; i < k; i++)
            part_unref(sel[i]);
        if (rc != 0 && rc != TSDB_ERR_STOPPED) {
            /* back off instead of retrying a failing merge in a tight loop */
            struct timespec d = {1, 0};
            nanosleep(&d, NULL);
        }
        pthread_mutex_lock(&db->lock);
    }
    pthread_mutex_unlock(&db->lock);
    return NULL;
}

int eng_optimize(eng *db)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    int rc = eng_flush(db);
    if (rc)
        return rc;
    part *sel[MAX_MERGE_PARTS];
    pthread_mutex_lock(&db->lock);
    for (;;) {
        /* first partition with several parts and no merge in flight */
        size_t a = SIZE_MAX, k = 0;
        bool waiting = false;
        for (size_t i = 0, j; i < db->nparts && a == SIZE_MAX; i = j) {
            bool busy = db->parts[i]->merging;
            for (j = i + 1; j < db->nparts && db->parts[j]->partition == db->parts[i]->partition; j++)
                busy |= db->parts[j]->merging;
            if (j - i < 2)
                continue;
            if (busy)
                waiting = true;
            else
                a = i;
        }
        if (a == SIZE_MAX) {
            if (!waiting)
                break;
            pthread_cond_wait(&db->merged_cv, &db->lock);
            continue;
        }
        int64_t partition = db->parts[a]->partition;
        for (size_t i = a; i < db->nparts && db->parts[i]->partition == partition && k < MAX_MERGE_PARTS; i++) {
            sel[k] = db->parts[i];
            sel[k]->merging = true;
            part_ref(sel[k]);
            k++;
        }
        pthread_mutex_unlock(&db->lock);
        part *out = NULL;
        rc = merge_parts(db, sel, k, &out);
        pthread_mutex_lock(&db->lock);
        merge_commit_locked(db, sel, k, out);
        pthread_mutex_unlock(&db->lock);
        if (out)
            manifest_publish(db);
        for (size_t i = 0; i < k; i++)
            part_unref(sel[i]);
        pthread_mutex_lock(&db->lock);
        if (rc)
            break;
    }
    pthread_mutex_unlock(&db->lock);
    return rc;
}
