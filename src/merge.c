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
 * strictly before everything else that remains, it is copied as compressed bytes
 * where one writer would cut its rows the same way: a full one (granule_rows) that
 * crosses no cut (day, or hour cut: HOUR_CUT_MIN), or a shorter one that ends where
 * the writer ends it (an hour cut, a day cut, the series' last row) with nothing of
 * the series before that edge left.
 *
 * Deletes and retention: a source's rows covered by the tombstones of a newer
 * source, or older than their series' retention cutoff, are dropped (a granule
 * that has any is decoded, never copied). The output keeps the union of the
 * sources' tombstones, which still hide older parts of the partition, unless
 * no older part is listed: block numbers only grow, so none can come later. An
 * output with no rows is still written, with no granules, so that it replaces
 * its sources and keeps their wal_seq: WAL replay skips a partition by it.
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

/* A part no merge has written holds blocks encoded at insert speed (CODEC_FAST), and by default without zstd
 * (insert_zstd_level 0). Once its partition is idle, or on tsdb_optimize, it is rewritten alone when it has
 * nothing to merge with, so that every part at rest ends up encoded at CODEC_MERGE and zstd_level. Such a part
 * is never copied granule by granule into a merge (merge_parts): level 0 is how a part says its blocks may lack
 * zstd, so no flag in the part is needed. */
static bool insert_written(const part *p) { return p->level == 0 && p->ng > 0; }

/* Picks the best range over all partitions, or with `solo`, when there is none, one part to rewrite alone.
 * Solo rewrites come last (merge_thread_main): after real merges and after the retention work, so that an
 * expired partition is dropped rather than rewritten and the WAL's removal is made durable without waiting
 * for every part of a bulk load. Caller holds db->lock. The chosen parts are marked merging and referenced. */
static size_t select_merge_locked(eng *db, part **sel, bool solo_ok)
{
    time_t now = time(NULL);
    double best = INFINITY;
    size_t best_i = 0, best_n = 0, solo = SIZE_MAX;
    for (size_t a = 0; a < db->nparts;) {
        size_t b = a + 1;
        while (b < db->nparts && db->parts[b]->partition == db->parts[a]->partition)
            b++;
        size_t nparts = b - a;
        time_t newest = 0;
        bool damaged = false; /* a merge of it would fail again (part_checked): the other partitions go on */
        for (size_t i = a; i < b; i++) {
            newest = MAX(newest, db->parts[i]->written);
            damaged |= atomic_load(&db->parts[i]->damaged);
        }
        if (damaged || partition_dropping(db, db->parts[a]->partition)) { /* the drop retires it: start nothing */
            a = b;
            continue;
        }
        bool idle = db->opt.merge_idle_s >= 0 && now - newest >= db->opt.merge_idle_s;
        for (size_t i = a; i < b && solo_ok && idle && solo == SIZE_MAX; i++)
            if (insert_written(db->parts[i]) && !db->parts[i]->merging)
                solo = i;
        if (nparts >= 2) {
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
    if (best_n == 0 && solo != SIZE_MAX) { /* nothing to merge: re-encode a part only inserts wrote */
        best_i = solo;
        best_n = 1;
    }
    for (size_t k = 0; k < best_n; k++) {
        sel[k] = db->parts[best_i + k];
        sel[k]->merging = true;
        sel[k]->merge_len_gen = atomic_load(&db->ret_len_gen);
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
    tomb_set hide; /* tombstones of newer sources */
    uint32_t series, run_end; /* the series of the run holding gi, which ends before entry run_end (0: unknown) */
} cursor;

static inline bool cur_done(const cursor *c) { return c->gi >= c->p->ng; }

/* The series of granule gi: cursors only move forward, so the run is looked up once per run. */
static inline uint32_t cur_series(cursor *c)
{
    if (c->gi >= c->run_end) {
        const gix *x = &c->p->ix;
        uint32_t r = gix_run_of(x, c->gi);
        c->series = x->rser[r];
        c->run_end = r + 1 < x->nruns ? gix_rstart(x, r + 1) : x->n;
    }
    return c->series;
}

/* Smallest remaining key; for an undecoded granule its first row is (series, ts_min). */
static inline void cur_key(cursor *c, uint32_t *s, int64_t *t)
{
    *s = cur_series(c);
    *t = c->loaded ? c->ts[c->pos] : gx_ts_min(c->p, c->gi);
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
    granule gb;
    gx_load(c->p, c->gi, GW_LOC, &gb);
    const granule *g = &gb;
    c->type = series_type(&db->series, cur_series(c));
    if (c->type < 0)
        return TSDB_ERR_CORRUPT; /* a granule of a series the dictionary does not have */
    int rc = granule_read(cc, c->p, g, c->type, c->ts, c->v, c->p->grows, true);
    if (rc) {
        /* only the source's own bytes: not NOMEM or IO, and not the part writer's refusals (pw_add) */
        if (rc == TSDB_ERR_CORRUPT)
            part_mark_payload(db, c->p, "merge");
        return rc;
    }
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

void (*_Atomic merge_hook)(int at);

int merge_parts(eng *db, part **src, size_t k, part **out)
{
    *out = NULL;
    cursor *cur = calloc(k, sizeof *cur);
    if (!cur)
        return TSDB_ERR_NOMEM;
    uint64_t min_block = UINT64_MAX, max_block = 0;
    uint32_t level = 0;
    for (size_t i = 0; i < k; i++) {
        /* Without pin_index a source's index is read in the mapping, where damage after its open shows. The merge
         * copies granule statistics from it verbatim and takes series and time ranges from it for the rows it
         * decodes, and would write either under the new part's fresh hash: wrong answers for good, which no later
         * check could see. So the index is hashed again first (it is about 0.3% of the part); on a mismatch the
         * merge fails and the part stays, for the next open to refuse. Decoding instead would still trust its run
         * table and offsets. */
        if (!part_checked(db, src[i], "merge")) {
            free(cur);
            return TSDB_ERR_CORRUPT;
        }
        cur[i].p = src[i];
        min_block = MIN(min_block, src[i]->min_block);
        max_block = MAX(max_block, src[i]->max_block);
        level = MAX(level, src[i]->level);
    }
    uint64_t wal_seq = 0;
    bool tombs = false;
    int64_t min_ts = INT64_MAX;
    for (size_t i = 0; i < k; i++) {
        wal_seq = MAX(wal_seq, src[i]->wal_seq);
        tombs |= src[i]->nt > 0;
        min_ts = MIN(min_ts, src[i]->min_ts);
    }
    int64_t now = eng_now_ms(db); /* one instant for the whole merge */
    bool ttl = atomic_load(&db->has_retention);
    part_writer w;
    int rc = pw_begin(&w, db, src[0]->partition, wal_seq);
    if (rc) {
        free(cur);
        return rc;
    }
    w.keep_empty = true; /* replaces its sources even when nothing is left */
    w.lf_on = false;     /* a merge keeps every live last-write-wins value: the latest table is not told (L8) */
    w.effort = CODEC_MERGE;
    w.zstd_level = db->opt.zstd_level; /* pw_begin chose the insert level */
    w.empty_ts = min_ts;
    if (tombs) {
        bool older = false; /* a listed part of the partition below the merge: the tombstones still hide it */
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < db->nparts && !older; i++)
            older = db->parts[i]->partition == src[0]->partition && db->parts[i]->max_block < min_block;
        pthread_mutex_unlock(&db->lock);
        tomb_set keep = {0};
        for (size_t i = 0; i < k && rc == 0; i++) {
            for (size_t j = 0; j < k && rc == 0; j++)
                if (src[j]->min_block > src[i]->max_block)
                    rc = tomb_set_add(&cur[i].hide, src[j]->t, src[j]->nt);
            tomb_set_finish(&cur[i].hide);
            if (older && rc == 0)
                rc = tomb_set_add(&keep, src[i]->t, src[i]->nt);
        }
        tomb_set_finish(&keep);
        size_t o = 0;
        for (size_t i = 0; i < keep.n; i++) { /* what retention removes needs no tombstone */
            int64_t cut = ttl ? eng_cutoff_at(db, keep.t[i].series, now) : INT64_MIN;
            if (keep.t[i].end <= cut)
                continue;
            keep.t[o] = keep.t[i];
            keep.t[o].start = MAX(keep.t[o].start, cut);
            o++;
        }
        if (rc == 0)
            rc = pw_set_tombs(&w, keep.t, o);
        tomb_set_free(&keep);
    }
    uint32_t cut_series = UINT32_MAX;
    int64_t cut = INT64_MIN;
    int64_t *fts = NULL;
    uint8_t *fv = NULL;
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
        granule gb;
        gx_load(c->p, c->gi, GW_TIME, &gb);
        gb.series = cur_series(c);
        const granule *g = &gb;
        if (ttl && g->series != cut_series) {
            cut_series = g->series;
            cut = eng_cutoff_at(db, g->series, now);
        }
        int64_t gcut = ttl ? cut : INT64_MIN;
        bool filter = gcut > g->ts_min || (c->hide.n && tomb_set_hits(&c->hide, g->series, g->ts_min, g->ts_max));
        if (!c->loaded) {
            bool before_rest = !has_bound || g->series < bs || (g->series == bs && g->ts_max < bt);
            /* Copied only where one writer given these rows would cut them the same way (pw_copy_fits): a granule
             * across a cut (written before the cut, by another GRANULE_CUT, or by a writer that saw less of the
             * hour) is re-cut. Pending rows of its series go first as a granule of their own: only where the
             * writer would end them, at a day cut or an hour cut. */
            bool day = !w.cut_ms || floor_div(g->ts_min, w.cut_ms) == floor_div(g->ts_max, w.cut_ms);
            bool pend_ok = w.pend_n == 0 || w.pend_series != g->series ||
                           (w.cut_ms && floor_div(w.pend_ts[w.pend_n - 1], w.cut_ms) < floor_div(g->ts_min, w.cut_ms)) ||
                           pw_hcut_before(&w, g->ts_min);
            /* a part no merge has written was encoded at insert speed (CODEC_FAST), maybe without zstd: re-encoded
             * once here, never copied, so that no merged part carries an insert's blocks */
            bool merged = c->p->level > 0;
            int fit = 0;
            if (!filter && before_rest && day && merged && pend_ok) {
                bool shrt = g->count < w.grows;
                int64_t end, d;
                if ((fit = pw_copy_fits(&w, c->p, c->gi, g, shrt, &end)) < 0) {
                    rc = fit;
                    break;
                }
                if (fit && shrt) {
                    /* a short one ends where the writer ends it: at an hour edge where it cuts, at a day cut, or at
                     * the end of the series' rows; nothing else of the series may come before that, in another
                     * cursor or after it in its own part */
                    if (w.cut_ms && (d = floor_div(g->ts_min, w.cut_ms) + 1) <= INT64_MAX / w.cut_ms)
                        end = MIN(end, d * w.cut_ms);
                    bool open = end == INT64_MAX; /* no edge: only the end of the series' rows ends it */
                    uint32_t nx = c->gi + 1;      /* cur_series set run_end: entries before it are of g's series */
                    fit = !(has_bound && bs == g->series && (open || bt < end)) &&
                          !(nx < c->run_end && (open || gx_ts_min(c->p, nx) < end));
                }
            }
            if (fit) {
                rc = pw_copy_granule(&w, c->p, c->gi);
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
        if (filter) {
            size_t wd = type_width(c->type), m = end - c->pos;
            if (!fts && (!(fts = malloc(MAX_GRANULE_ROWS * sizeof *fts)) || !(fv = malloc(MAX_GRANULE_ROWS * 8)))) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            memcpy(fts, c->ts + c->pos, m * sizeof *fts);
            memcpy(fv, c->v + c->pos * wd, m * wd);
            m = tomb_set_filter(&c->hide, g->series, gcut, fts, fv, wd, m);
            rc = m ? pw_add(&w, g->series, c->type, fts, fv, m) : 0;
            rows += m;
        } else {
            rc = pw_add(&w, g->series, c->type, c->ts + c->pos, c->v + c->pos * type_width(c->type), end - c->pos);
            rows += end - c->pos;
        }
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
        tomb_set_free(&cur[i].hide);
    }
    free(cur);
    free(fts);
    free(fv);
    if (rc) {
        pw_abort(&w);
        return rc;
    }
    void (*hook)(int) = atomic_load(&merge_hook);
    if (hook)
        hook(0);
    rc = pw_finish(&w, min_block, max_block, level + 1, out);
    if (hook)
        hook(1);
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
void merge_commit_locked(eng *db, part **src, size_t k, part *out, int rc)
{
    bool dropped = false;
    /* A limit lengthened since this merge's sources were selected (db->ret_len_gen, bumped under db->lock): the
     * merge may have dropped rows the new limit keeps, and the latest table may name one of them. Its output is
     * discarded like one whose source was dropped, and the sources merge again (a retention change during a merge
     * is rare; design-latest.md 3.5 lists across it instead, which takes latest_rw here) */
    bool lengthened = false;
    for (size_t i = 0; i < k; i++) {
        src[i]->merging = false;
        dropped |= src[i]->dropped;
        lengthened |= src[i]->merge_len_gen != atomic_load(&db->ret_len_gen);
    }
    if (lengthened && out && !dropped && db->lt)
        dropped = true; /* the output goes the way of a dropped source's, the sources stay listed */
    if (!out && rc != 0 && rc != TSDB_ERR_STOPPED && !dropped) {
        /* a failed merge is no progress, whatever the reason (a full disk, EIO on the output, NOMEM): db_wait_merges
         * counts the time of its partition's merges as idle once one has failed since its wait began */
        db->merge_fail_seq++;
        for (size_t i = 0; i < k; i++)
            src[i]->fail_seq = db->merge_fail_seq;
    }
    if (out && !dropped && eng_check(db)) {
        part_unref(out); /* failed engine: no renames; the next open sees what a crash here leaves */
    } else if (out && !dropped) {
        db_parts_insert_locked(db, out);
        db_stall_clear_locked(db, out->partition);
        for (size_t i = 0; i < k; i++)
            db_retire_locked(db, src[i]);
    } else if (out) {
        /* A source was dropped meanwhile: the merged data must not come back. Neither eng_drop_before nor the
         * retention drop lets this happen (they wait for, or leave, a partition with a merging part), so this is a
         * backstop. The output already has its listed name, durable (pw_finish): it goes by rename and a directory
         * fsync, as a dropped part does, never by an unlink a power loss can undo. A failed engine names no new
         * file, so there it stays as it is (the next open sees what a crash here leaves, and the sources are
         * retired or listed as the open finds them). */
        if (!eng_check(db)) {
            db_retire_unlisted_locked(db, out);
            atomic_store(&out->obsolete, true);
        }
        part_unref(out);
    }
    pthread_cond_broadcast(&db->merged_cv);
}

/* ------------------------------------------------------------ retention */

static int64_t sat_add64(int64_t a, int64_t b)
{
    int64_t r;
    return __builtin_add_overflow(a, b, &r) ? (b > 0 ? INT64_MAX : INT64_MIN) : r;
}

/* When some row of p expires (ttl_any) and when all of it, tombstones included, has expired past the end of
 * its partition (ttl_all), in retention "now"; cached per part until a limit changes. Caller holds db->lock. */
static void ttl_compute_locked(eng *db, part *p, uint32_t gen)
{
    int64_t any = INT64_MAX, all = INT64_MIN;
    int64_t lo, pend;
    partition_range(db, p->partition, &lo, &pend);
    for (uint32_t r = 0; r < p->ix.nruns; r++) { /* a run's earliest row is its first granule's */
        int64_t lim = eng_retention(db, p->ix.rser[r]);
        if (lim <= 0) {
            all = INT64_MAX;
            continue;
        }
        uint32_t g0 = gix_rstart(&p->ix, r);
        any = MIN(any, sat_add64(gx_ts_min(p, g0 < p->ng ? g0 : 0), lim));
        all = MAX(all, sat_add64(pend, lim));
    }
    for (uint32_t i = 0; i < p->nt; i++) {
        int64_t lim = eng_retention(db, p->t[i].series);
        all = MAX(all, lim <= 0 ? INT64_MAX : sat_add64(pend, lim));
    }
    p->ttl_any = any;
    p->ttl_all = all;
    p->ttl_gen = gen;
}

/* Parts [a, b) of one partition looked expired. Without pin_index, ttl_all came from series ids read in the mapping,
 * where damage since the open shows (a run of a series without retention renamed to one with a short limit), and a
 * drop is for good: the files go after obsolete_part_lifetime_s, where no later open can see them. So each part's
 * index is checked first and the decision made again from the checked bytes; a damaged part keeps its partition
 * (part_checked). Hashing every part at every retention change would hold db->lock for a pass over all indexes;
 * only a drop needs it, and a TTL rewrite is checked by merge_parts. Caller holds db->lock. */
static bool partition_expired_checked_locked(eng *db, size_t a, size_t b, uint32_t gen, int64_t now)
{
    int64_t all = INT64_MIN;
    for (size_t i = a; i < b; i++) {
        part *p = db->parts[i];
        if (!p->ng && !p->nt)
            continue;
        if (!part_checked(db, p, "retention"))
            return false;
        ttl_compute_locked(db, p, gen);
        all = MAX(all, p->ttl_all);
    }
    return now >= all;
}

/* Expired partitions and partitions left with nothing but empty parts are retired (*dropped); one part with
 * expired rows is selected for a rewrite. An empty part (a merge's wal_seq anchor) holds its partition only while
 * a WAL segment up to its wal_seq may still be replayed: *sync_wal asks for a directory fsync that makes the
 * removal of such segments durable, after which it can go. Caller holds db->lock. */
size_t ttl_select_locked(eng *db, part **sel, bool *dropped, bool *sync_wal)
{
    *dropped = *sync_wal = false;
    if (db->opt.read_only)
        return 0;
    bool ret = atomic_load_explicit(&db->has_retention, memory_order_relaxed);
    struct timespec mono;
    clock_gettime(CLOCK_MONOTONIC, &mono);
    int64_t mono_ms = (int64_t)mono.tv_sec * 1000 + mono.tv_nsec / 1000000;
    if (db->last_ttl_ms && mono_ms - db->last_ttl_ms < 1000)
        return 0;
    db->last_ttl_ms = mono_ms;
    uint32_t gen = atomic_load(&db->ret_gen);
    int64_t now = ret ? eng_now_ms(db) : 0;
    time_t wall = time(NULL);
    size_t k = 0;
    for (size_t a = 0, b; a < db->nparts; a = b) {
        int64_t all = INT64_MIN;
        bool busy = partition_dropping(db, db->parts[a]->partition); /* a drop retires it: no rewrite, no second drop */
        for (b = a; b < db->nparts && db->parts[b]->partition == db->parts[a]->partition; b++) {
            part *p = db->parts[b];
            busy |= p->merging;
            if (!p->ng && !p->nt) {
                if (p->wal_seq > db->wal_removed_synced) {
                    all = INT64_MAX;
                    *sync_wal |= p->wal_seq <= db->wal_removed;
                }
                continue;
            }
            if (!ret) {
                all = INT64_MAX;
                continue;
            }
            if (p->ttl_gen != gen)
                ttl_compute_locked(db, p, gen);
            all = MAX(all, p->ttl_all);
        }
        if (busy) /* a merge of it is in flight: its output would be named after the drop (merge_commit_locked) */
            continue;
        if (now >= all && !eng_check(db) && partition_expired_checked_locked(db, a, b, gen, now)) {
            /* every series in the partition has expired past its end, or nothing is left in it: drop it like
             * tsdb_drop_before. Memtable rows of it are hidden by the same cutoffs; flushed later, they go the
             * same way (or, for an empty partition, are newer than what was deleted). */
            for (size_t i = b; i-- > a;) {
                db->parts[i]->dropped = true; /* an in-flight merge of it discards its result */
                db_retire_locked(db, db->parts[i]);
            }
            *dropped = true;
            b = a; /* the list shrank */
            continue;
        }
        for (size_t i = a; ret && i < b && !k; i++) {
            part *p = db->parts[i];
            if (p->merging || atomic_load(&p->damaged) || (!p->ng && !p->nt) || now <= p->ttl_any ||
                db->opt.retention_merge_s < 0 ||
                wall - p->created < db->opt.retention_merge_s)
                continue;
            p->merging = true;
            p->merge_len_gen = atomic_load(&db->ret_len_gen);
            part_ref(p);
            sel[k++] = p;
        }
    }
    if (k || *sync_wal)
        db->last_ttl_ms = 0; /* there may be more: look again once this one is done */
    return k;
}

void *merge_thread_main(void *arg)
{
    eng *db = arg;
    part *sel[MAX_MERGE_PARTS];
    tsdb_thread_name("tsdb-merge");
    pthread_mutex_lock(&db->lock);
    while (!atomic_load(&db->stop)) {
        size_t k = eng_check(db) ? 0 : select_merge_locked(db, sel, false); /* a failed engine writes nothing more */
        bool dropped = false, sync_wal = false;
        if (!k && !eng_check(db))
            k = ttl_select_locked(db, sel, &dropped, &sync_wal);
        if (!k && !dropped && !sync_wal && !eng_check(db))
            k = select_merge_locked(db, sel, true);
        if (dropped || sync_wal) {
            pthread_mutex_unlock(&db->lock);
            int src = eng_sync_dir(db, "fsync of the directory after retiring expired partitions");
            if (src == 0 && dropped)
                manifest_publish(db);
            pthread_mutex_lock(&db->lock); /* eng_sync_dir moved wal_removed_synced on */
            dropped |= sync_wal && src == 0; /* look again at once */
        }
        if (!k) {
            if (dropped)
                continue;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&db->merge_cv, &db->lock, &ts);
            continue;
        }
        pthread_mutex_unlock(&db->lock);
        part *out = NULL;
        int fault_ms = atomic_load(&merge_fault_ms);
        int rc = 0;
        for (int slept = 0; slept < fault_ms && !atomic_load(&db->stop); slept += 50)
            usleep(50000);
        if (fault_ms > 0)
            rc = TSDB_ERR_IO;
        else
            rc = merge_parts(db, sel, k, &out);
        if (rc != 0 && rc != TSDB_ERR_STOPPED)
            fprintf(stderr, "tsdb: merge of %s..%s failed: %s\n", sel[0]->name, sel[k - 1]->name,
                    tsdb_strerror(rc));
        pthread_mutex_lock(&db->lock);
        merge_commit_locked(db, sel, k, out, rc);
        part *watch = NULL; /* an idle partition merged down to one part: its series' rates are measured */
        if (out && !atomic_load(&out->obsolete) && db->opt.merge_idle_s >= 0 &&
            time(NULL) - out->written >= db->opt.merge_idle_s && partition_part_count_locked(db, out->partition) == 1) {
            watch = out;
            part_ref(watch);
        }
        pthread_mutex_unlock(&db->lock);
        if (out)
            manifest_publish(db);
        for (size_t i = 0; i < k; i++)
            part_unref(sel[i]);
        if (watch) {
            int crc = cls_observe(db, watch);
            if (crc && crc != TSDB_ERR_IO)
                fprintf(stderr, "tsdb: measuring the series of %s failed: %s\n", watch->name, tsdb_strerror(crc));
            part_unref(watch);
        }
        pthread_mutex_lock(&db->lock);
        if (rc != 0 && rc != TSDB_ERR_STOPPED) {
            /* back off instead of retrying a failing merge in a tight loop, but on the condition variable:
             * close must not wait out the back-off (tsdbd closes right after a storage failure) */
            struct timespec until;
            clock_gettime(CLOCK_REALTIME, &until);
            until.tv_sec += 1;
            while (!atomic_load(&db->stop) && pthread_cond_timedwait(&db->merge_cv, &db->lock, &until) == 0) {
            }
        }
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
    bool skipped = false; /* a partition with a damaged part was left as it is: TSDB_ERR_CORRUPT at the end */
    pthread_mutex_lock(&db->lock);
    for (;;) {
        /* first partition with several parts, or with one only inserts wrote, and no merge in flight; one a merge
         * thread is rewriting alone right now is waited for like any other busy partition */
        size_t a = SIZE_MAX, k = 0;
        bool waiting = false;
        for (size_t i = 0, j; i < db->nparts && a == SIZE_MAX; i = j) {
            bool busy = db->parts[i]->merging, damaged = atomic_load(&db->parts[i]->damaged);
            for (j = i + 1; j < db->nparts && db->parts[j]->partition == db->parts[i]->partition; j++) {
                busy |= db->parts[j]->merging;
                damaged |= atomic_load(&db->parts[j]->damaged);
            }
            if (j - i < 2 && !insert_written(db->parts[i]))
                continue;
            if (partition_dropping(db, db->parts[i]->partition)) /* the drop retires it: nothing to optimize */
                continue;
            if (damaged) { /* found by a merge before (part_checked): the rest is merged, and the call fails */
                skipped = true;
                continue;
            }
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
            sel[k]->merge_len_gen = atomic_load(&db->ret_len_gen);
            part_ref(sel[k]);
            k++;
        }
        pthread_mutex_unlock(&db->lock);
        part *out = NULL;
        rc = merge_parts(db, sel, k, &out);
        pthread_mutex_lock(&db->lock);
        merge_commit_locked(db, sel, k, out, rc);
        pthread_mutex_unlock(&db->lock);
        if (out) {
            int mrc = manifest_publish(db);
            rc = rc ? rc : mrc;
        }
        bool marked = false;
        for (size_t i = 0; i < k; i++) {
            marked |= rc == TSDB_ERR_CORRUPT && atomic_load(&sel[i]->damaged);
            part_unref(sel[i]);
        }
        if (marked) { /* the loop leaves its partition out from now on */
            skipped = true;
            rc = 0;
        }
        pthread_mutex_lock(&db->lock);
        if (rc || (rc = eng_check(db)) != 0) /* failed meanwhile: the output was dropped, do not select again */
            break;
    }
    pthread_mutex_unlock(&db->lock);
    return rc == 0 && skipped ? TSDB_ERR_CORRUPT : rc;
}
