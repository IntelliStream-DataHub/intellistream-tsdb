/*
 * Insert path: bring rows into (partition, series, ts) order and write one
 * part per partition, like a MergeTree INSERT. All sorting is stable, and
 * only the last row of equal (series, ts) keys is written.
 *
 * Already sorted input is written straight from the caller's arrays. Common
 * "one row per series per timestamp" batches are sorted by time but not by
 * series; the radix sort then skips every timestamp digit because a stable
 * sort by series keeps the time order.
 *
 * Values are slots of the batch's type (4 or 8 bytes). The sort moves 16-byte
 * records whatever the type: their payload is the value itself for 4-byte
 * types, and the row's index for 8-byte ones, gathered after sorting.
 */
#include "internal.h"

#include <stdlib.h>

#define SORT_CHUNK_ROWS (8u << 20) /* bounds sort memory to ~256 MiB */

/* Grow-only per-thread buffers for sorting, so steady inserts do not fault
 * in fresh pages every batch. Freed when the thread exits. */
enum { SCR_A, SCR_B, SCR_S, SCR_T, SCR_V, SCR_CNT, SCR_N };
typedef struct scratch {
    void *p[SCR_N];
    size_t cap[SCR_N];
} scratch;

static pthread_key_t scratch_key;
static pthread_once_t scratch_once = PTHREAD_ONCE_INIT;

static void scratch_free(void *arg)
{
    scratch *s = arg;
    for (int i = 0; i < SCR_N; i++)
        free(s->p[i]);
    free(s);
}

static void scratch_init(void) { pthread_key_create(&scratch_key, scratch_free); }

static void *scratch_get(int slot, size_t bytes)
{
    pthread_once(&scratch_once, scratch_init);
    scratch *s = pthread_getspecific(scratch_key);
    if (!s) {
        s = calloc(1, sizeof *s);
        if (!s || pthread_setspecific(scratch_key, s) != 0) {
            free(s);
            return NULL;
        }
    }
    if (s->cap[slot] < bytes) {
        free(s->p[slot]);
        s->p[slot] = malloc(bytes);
        s->cap[slot] = s->p[slot] ? bytes : 0;
    }
    return s->p[slot];
}

typedef struct rec {
    int64_t ts;
    uint32_t series;
    uint32_t v; /* 4-byte types: the value's bits; 8-byte types: the row (chunks have < 2^32 rows) */
} rec;

typedef struct prange {
    int64_t p, lo, hi; /* partition and its time range [lo, hi) */
} prange;

static inline void prange_of(const eng *db, int64_t ts, prange *r)
{
    r->p = floor_div(ts, db->partition_ms);
    r->lo = sat_mul(r->p, db->partition_ms);
    r->hi = sat_add(r->lo, db->partition_ms);
}

/* hi saturates at INT64_MAX for the last partition, which then includes INT64_MAX itself */

static inline bool in_prange(const prange *r, int64_t ts)
{
    return ts >= r->lo && (ts < r->hi || (r->hi == INT64_MAX && ts == INT64_MAX));
}

int part_list_push(part_list *l, part *p)
{
    if (l->n == l->cap) {
        size_t cap = l->cap ? l->cap * 2 : 8;
        part **a = realloc(l->p, cap * sizeof *a);
        if (!a)
            return TSDB_ERR_NOMEM;
        l->p = a;
        l->cap = cap;
    }
    l->p[l->n++] = p;
    return 0;
}

/* Adds one series run to w, keeping only the last row of equal timestamps. */
static int add_run_unique(part_writer *w, uint32_t s, int type, const int64_t *ts, const uint8_t *v, size_t n)
{
    size_t dup = 1, wd = type_width(type);
    while (dup < n && ts[dup] != ts[dup - 1])
        dup++;
    if (dup >= n)
        return pw_add(w, s, type, ts, v, n); /* the common case: no duplicates */
    int64_t *t2 = malloc(n * sizeof *t2);
    uint8_t *v2 = malloc(n * wd);
    int rc = TSDB_ERR_NOMEM;
    if (t2 && v2) {
        size_t o = 0;
        for (size_t i = 0; i < n; i++) {
            if (o && t2[o - 1] == ts[i])
                o--;
            t2[o] = ts[i];
            memcpy(v2 + o * wd, v + i * wd, wd);
            o++;
        }
        rc = pw_add(w, s, type, t2, v2, o);
    }
    free(t2);
    free(v2);
    return rc;
}

/* Writes rows already in (partition, series, ts) order, equal keys in
 * insertion order, as pending parts appended to `collect`. */
static int write_sorted(eng *db, int type, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v,
                        size_t n, uint64_t wal_seq, part_list *collect)
{
    size_t wd = type_width(type);
    size_t i = 0;
    while (i < n) {
        prange r;
        prange_of(db, ts[i], &r);
        size_t j = i + 1;
        while (j < n && in_prange(&r, ts[j]))
            j++;
        part_writer w;
        int rc = pw_begin(&w, db, r.p, wal_seq);
        if (rc)
            return rc;
        for (size_t k = i; k < j && rc == 0;) {
            uint32_t s = series ? series[k] : cs;
            size_t m = k + 1;
            if (series)
                while (m < j && series[m] == s)
                    m++;
            else
                m = j;
            rc = add_run_unique(&w, s, type, ts + k, (const uint8_t *)v + k * wd, m - k);
            k = m;
        }
        if (rc) {
            pw_abort(&w);
            return rc;
        }
        part *p; /* numbered when committed */
        if ((rc = pw_finish_pending(&w, 0, 0, 0, &p)) != 0)
            return rc;
        if (p && (rc = part_list_push(collect, p)) != 0) {
            parts_discard(db, &p, 1);
            return rc;
        }
        i = j;
    }
    return 0;
}

static void insertion_sort(rec *a, size_t n)
{
    for (size_t i = 1; i < n; i++) {
        rec x = a[i];
        size_t j = i;
        while (j > 0 && (a[j - 1].series > x.series || (a[j - 1].series == x.series && a[j - 1].ts > x.ts))) {
            a[j] = a[j - 1];
            j--;
        }
        a[j] = x;
    }
}

/* LSD radix sort by (series, ts) within one partition. Returns the buffer
 * holding the result (a or tmp). Digits that are constant are skipped. */
static rec *lsd_sort(rec *a, rec *tmp, size_t n, bool ts_sorted)
{
    if (n < 64) {
        insertion_sort(a, n);
        return a;
    }
    int64_t tmin = a[0].ts;
    for (size_t i = 1; i < n; i++)
        if (a[i].ts < tmin)
            tmin = a[i].ts;
    static __thread uint32_t h[12][256];
    memset(h, 0, sizeof h);
    for (size_t i = 0; i < n; i++) {
        uint64_t u = (uint64_t)a[i].ts - (uint64_t)tmin;
        uint32_t s = a[i].series;
        for (int b = 0; b < 8; b++)
            h[b][(u >> (8 * b)) & 0xff]++;
        for (int b = 0; b < 4; b++)
            h[8 + b][(s >> (8 * b)) & 0xff]++;
    }
    for (int d = ts_sorted ? 8 : 0; d < 12; d++) {
        uint64_t u = (uint64_t)a[0].ts - (uint64_t)tmin;
        uint32_t first = d < 8 ? (uint32_t)((u >> (8 * d)) & 0xff) : (a[0].series >> (8 * (d - 8))) & 0xff;
        if (h[d][first] == n)
            continue; /* every row has the same digit */
        uint32_t off[256], sum = 0;
        for (int k = 0; k < 256; k++) {
            off[k] = sum;
            sum += h[d][k];
        }
        if (d < 8) {
            int shift = 8 * d;
            for (size_t i = 0; i < n; i++) {
                uint64_t x = (uint64_t)a[i].ts - (uint64_t)tmin;
                tmp[off[(x >> shift) & 0xff]++] = a[i];
            }
        } else {
            int shift = 8 * (d - 8);
            for (size_t i = 0; i < n; i++)
                tmp[off[(a[i].series >> shift) & 0xff]++] = a[i];
        }
        rec *t = a;
        a = tmp;
        tmp = t;
    }
    return a;
}

static inline bool rec_less(const eng *db, const rec *a, const rec *b)
{
    int64_t pa = floor_div(a->ts, db->partition_ms), pb = floor_div(b->ts, db->partition_ms);
    if (pa != pb)
        return pa < pb;
    if (a->series != b->series)
        return a->series < b->series;
    return a->ts < b->ts;
}

/* Stable bottom-up merge sort, for batches spanning very many partitions.
 * Returns the buffer holding the result. */
static rec *merge_sort_recs(const eng *db, rec *a, rec *tmp, size_t n)
{
    for (size_t width = 1; width < n; width *= 2) {
        for (size_t lo = 0; lo < n; lo += 2 * width) {
            size_t mid = MIN(lo + width, n), hi = MIN(lo + 2 * width, n), i = lo, j = mid, k = lo;
            while (i < mid && j < hi)
                tmp[k++] = rec_less(db, &a[j], &a[i]) ? a[j++] : a[i++];
            while (i < mid)
                tmp[k++] = a[i++];
            while (j < hi)
                tmp[k++] = a[j++];
        }
        rec *t = a;
        a = tmp;
        tmp = t;
    }
    return a;
}

/* Rows where every series is in time order (what collectors send, even when
 * series have different intervals): a stable counting sort on (partition,
 * series) keeps each series' time order, so one counting pass and one scatter
 * pass straight into the output arrays sort the batch. Returns 1 when a series
 * goes back in time or the key space is too sparse, and the caller falls back
 * to the radix sort. */
static int bucket_write(eng *db, int type, const uint32_t *series, const int64_t *ts, const void *v, size_t n,
                        uint64_t wal_seq, part_list *collect, bool refuse)
{
    size_t wd = type_width(type);
    uint32_t smin = UINT32_MAX, smax = 0;
    int64_t tmin = INT64_MAX, tmax = INT64_MIN;
    for (size_t i = 0; i < n; i++) {
        smin = series[i] < smin ? series[i] : smin;
        smax = series[i] > smax ? series[i] : smax;
        tmin = ts[i] < tmin ? ts[i] : tmin;
        tmax = ts[i] > tmax ? ts[i] : tmax;
    }
    if (refuse && tmax == TS_UNREACHABLE)
        return TSDB_ERR_INVALID;
    int64_t pmin = floor_div(tmin, db->partition_ms), pmax = floor_div(tmax, db->partition_ms);
    uint64_t ns = (uint64_t)smax - smin + 1, np = (uint64_t)pmax - (uint64_t)pmin + 1;
    if (np > (1u << 20) || ns * np > MAX(n / 2, (size_t)65536))
        return 1;
    size_t nb = (size_t)(ns * np);
    size_t *cnt = scratch_get(SCR_CNT, (nb + 1) * sizeof *cnt);
    prange *pr = scratch_get(SCR_A, ns * sizeof *pr); /* each series' current partition */
    int64_t *last = scratch_get(SCR_B, ns * sizeof *last);
    uint32_t *os = scratch_get(SCR_S, n * sizeof *os);
    int64_t *ot = scratch_get(SCR_T, n * sizeof *ot);
    uint8_t *ov = scratch_get(SCR_V, n * wd);
    if (!cnt || !pr || !last || !os || !ot || !ov)
        return TSDB_ERR_NOMEM;
    memset(cnt, 0, (nb + 1) * sizeof *cnt);
    for (size_t k = 0; k < ns; k++) {
        pr[k].lo = INT64_MAX; /* forces a lookup on first use */
        pr[k].hi = INT64_MIN;
        last[k] = INT64_MIN;
    }
    /* a series' partition only moves forward, so it is tracked rather than divided out per row */
    for (size_t i = 0; i < n; i++) {
        size_t k = series[i] - smin;
        if (UNLIKELY(ts[i] < last[k]))
            return 1;
        last[k] = ts[i];
        if (UNLIKELY(!in_prange(&pr[k], ts[i])))
            prange_of(db, ts[i], &pr[k]);
        cnt[(size_t)(pr[k].p - pmin) * ns + k + 1]++;
    }
    for (size_t k = 1; k <= nb; k++)
        cnt[k] += cnt[k - 1];
    for (size_t k = 0; k < ns; k++) {
        pr[k].lo = INT64_MAX;
        pr[k].hi = INT64_MIN;
    }
    for (size_t i = 0; i < n; i++) {
        size_t k = series[i] - smin;
        if (UNLIKELY(!in_prange(&pr[k], ts[i])))
            prange_of(db, ts[i], &pr[k]);
        size_t o = cnt[(size_t)(pr[k].p - pmin) * ns + k]++;
        os[o] = series[i];
        ot[o] = ts[i];
        if (wd == 4)
            memcpy(ov + 4 * o, (const uint8_t *)v + 4 * i, 4);
        else
            memcpy(ov + 8 * o, (const uint8_t *)v + 8 * i, 8);
    }
    return write_sorted(db, type, os, 0, ot, ov, n, wal_seq, collect);
}

/* The sorted records' values into ov: in the record, or gathered by row. */
static inline void gather(const rec *r, size_t n, const void *v, size_t wd, uint8_t *ov)
{
    if (wd == 4) {
        for (size_t i = 0; i < n; i++)
            memcpy(ov + 4 * i, &r[i].v, 4);
    } else {
        for (size_t i = 0; i < n; i++)
            memcpy(ov + 8 * i, (const uint8_t *)v + 8 * (size_t)r[i].v, 8);
    }
}

static int sort_and_write(eng *db, int type, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v,
                          size_t n, bool ts_sorted, uint64_t wal_seq, part_list *collect, bool refuse)
{
    if (series) {
        int rc = bucket_write(db, type, series, ts, v, n, wal_seq, collect, refuse);
        if (rc != 1)
            return rc;
    }
    size_t wd = type_width(type);
    rec *a = scratch_get(SCR_A, n * sizeof *a), *b = scratch_get(SCR_B, n * sizeof *b);
    uint32_t *os = scratch_get(SCR_S, n * sizeof *os);
    int64_t *ot = scratch_get(SCR_T, n * sizeof *ot);
    uint8_t *ov = scratch_get(SCR_V, n * wd);
    uint32_t *pk = NULL;
    int rc = TSDB_ERR_NOMEM;
    if (!a || !b || !os || !ot || !ov)
        goto out;

    int64_t tmin = INT64_MAX, tmax = INT64_MIN;
    for (size_t i = 0; i < n; i++) {
        a[i].ts = ts[i];
        a[i].series = series ? series[i] : cs;
        if (wd == 4)
            memcpy(&a[i].v, (const uint8_t *)v + 4 * i, 4);
        else
            a[i].v = (uint32_t)i;
        tmin = MIN(tmin, ts[i]);
        tmax = MAX(tmax, ts[i]);
    }
    if (refuse && tmax == TS_UNREACHABLE) {
        rc = TSDB_ERR_INVALID;
        goto out;
    }
    int64_t pmin = floor_div(tmin, db->partition_ms), pmax = floor_div(tmax, db->partition_ms);
    uint64_t np = (uint64_t)pmax - (uint64_t)pmin + 1;

    /* each partition group ends up in either a or b; the result is gathered into os/ot/ov */
    rec *res;
    if (np == 1) {
        res = lsd_sort(a, b, n, ts_sorted);
        for (size_t i = 0; i < n; i++) {
            os[i] = res[i].series;
            ot[i] = res[i].ts;
        }
        gather(res, n, v, wd, ov);
    } else if (np <= (1u << 20) && (pk = malloc(n * sizeof *pk)) != NULL) {
        size_t *cnt = calloc(np + 1, sizeof *cnt);
        if (!cnt)
            goto out;
        for (size_t i = 0; i < n; i++) {
            pk[i] = (uint32_t)(floor_div(a[i].ts, db->partition_ms) - pmin);
            cnt[pk[i] + 1]++;
        }
        for (uint64_t k = 1; k <= np; k++)
            cnt[k] += cnt[k - 1];
        size_t *pos = malloc(np * sizeof *pos);
        if (!pos) {
            free(cnt);
            goto out;
        }
        memcpy(pos, cnt, np * sizeof *pos);
        for (size_t i = 0; i < n; i++)
            b[pos[pk[i]]++] = a[i];
        free(pos);
        for (uint64_t k = 0; k < np; k++) {
            size_t lo = cnt[k], len = cnt[k + 1] - lo;
            if (!len)
                continue;
            res = lsd_sort(b + lo, a + lo, len, ts_sorted);
            for (size_t i = 0; i < len; i++) {
                os[lo + i] = res[i].series;
                ot[lo + i] = res[i].ts;
            }
            gather(res, len, v, wd, ov + lo * wd);
        }
        free(cnt);
    } else {
        res = merge_sort_recs(db, a, b, n);
        for (size_t i = 0; i < n; i++) {
            os[i] = res[i].series;
            ot[i] = res[i].ts;
        }
        gather(res, n, v, wd, ov);
    }
    rc = write_sorted(db, type, os, 0, ot, ov, n, wal_seq, collect);
out:
    free(pk);
    return rc;
}

/* `refuse`: the rows come from a caller, and one at TS_UNREACHABLE fails the whole insert. A flush writes what the
 * WAL holds, as it was acknowledged (an older build accepted such rows), so it does not refuse. The checks ride on
 * passes made anyway: the sort's minimum and maximum per chunk, and here a partition change, since only the last
 * partition can hold TS_UNREACHABLE; a batch that reaches it (292 million years from now) is scanned once more. */
static int insert_rows_into(eng *db, int type, const uint32_t *series, uint32_t cs, const int64_t *ts,
                            const void *v, size_t n, uint64_t wal_seq, part_list *collect, bool refuse)
{
    size_t wd = type_width(type);
    int64_t ptop = floor_div(TS_UNREACHABLE, db->partition_ms);
    bool sorted = true;
    prange r;
    prange_of(db, ts[0], &r);
    bool top = r.p == ptop;
    for (size_t i = 1; i < n && sorted; i++) {
        if (!in_prange(&r, ts[i])) {
            int64_t prev = r.p;
            prange_of(db, ts[i], &r);
            top |= r.p == ptop;
            if (r.p < prev)
                sorted = false;
        } else if (series && series[i] != series[i - 1]) {
            if (series[i] < series[i - 1])
                sorted = false;
        } else if (ts[i] < ts[i - 1]) {
            sorted = false;
        }
    }
    if (sorted) {
        for (size_t i = 0; refuse && top && i < n; i++)
            if (ts[i] == TS_UNREACHABLE)
                return TSDB_ERR_INVALID;
        return write_sorted(db, type, series, cs, ts, v, n, wal_seq, collect);
    }
    /* chunks are written in order, so a later chunk's part is newer: last write still wins */
    for (size_t off = 0; off < n; off += SORT_CHUNK_ROWS) {
        size_t m = MIN(n - off, SORT_CHUNK_ROWS);
        bool ts_sorted = true; /* lets the radix sort skip the timestamp digits */
        for (size_t i = off + 1; i < off + m && ts_sorted; i++)
            ts_sorted = ts[i] >= ts[i - 1];
        int rc = sort_and_write(db, type, series ? series + off : NULL, cs, ts + off, (const uint8_t *)v + off * wd, m,
                                ts_sorted, wal_seq, collect, refuse);
        if (rc)
            return rc;
    }
    return 0;
}

/* Commits and publishes pending parts as one step (see publish_lock). */
int parts_publish(eng *db, part **ps, size_t n)
{
    if (n == 0)
        return 0;
    pthread_mutex_lock(&db->publish_lock);
    int rc = parts_commit(db, ps, n);
    if (rc == 0)
        db_add_parts(db, ps, n); /* published: whatever happens to MANIFEST, the caller must not discard them */
    pthread_mutex_unlock(&db->publish_lock);
    return rc;
}

/* All parts of one insert are committed together (all or nothing) and
 * published together. With `collect`, they are handed back pending instead,
 * for the caller to publish with parts_commit under publish_lock. */
int insert_rows(eng *db, int type, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t n,
                uint64_t wal_seq, part_list *collect, bool refuse)
{
    if (n == 0)
        return 0;
    if (collect)
        return insert_rows_into(db, type, series, cs, ts, v, n, wal_seq, collect, refuse);
    part_list mine = {0};
    int rc = insert_rows_into(db, type, series, cs, ts, v, n, wal_seq, &mine, refuse);
    /* their partitions, for the wait below, are read now: once published, a
     * part can be merged away and freed at any moment */
    int64_t *partitions = rc == 0 ? malloc((mine.n ? mine.n : 1) * sizeof *partitions) : NULL;
    if (rc == 0 && !partitions)
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < mine.n; i++)
        partitions[i] = mine.p[i]->partition;
    if (rc == 0)
        rc = parts_publish(db, mine.p, mine.n);
    if (rc) {
        parts_discard(db, mine.p, mine.n);
    } else {
        /* like parts_to_delay_insert: let merges catch up instead of piling up parts */
        for (size_t i = 0; i < mine.n; i++)
            db_wait_merges(db, partitions[i]);
    }
    free(partitions);
    free(mine.p);
    return rc;
}
