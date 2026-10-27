/*
 * Queries.
 *
 * A query reads one series from a consistent snapshot: the parts that
 * overlap the range plus the rows of the series in the memtables. Each
 * part's granule index is sorted by (series, ts), so finding the granules of
 * one series in a time range is two binary searches per part (the sparse
 * primary index of MergeTree).
 *
 * (series, ts) is unique: when several sources hold the same timestamp, only
 * the newest source's row counts (memtables, newest first, then parts by
 * block number), so results never contain duplicates, as if every read used
 * ReplacingMergeTree's FINAL.
 *
 * Aggregates: granules are grouped into clusters of overlapping time ranges.
 * A granule that overlaps nothing from another source cannot hold a replaced
 * row, so when it lies inside the range and inside one bucket it is answered
 * from its stored count/sum/min/max without decoding; otherwise it is decoded
 * in parallel and folded with the SIMD kernels. Clusters spanning several
 * sources go through the deduplicating merge.
 *
 * Raw: sources whose time ranges are disjoint are concatenated; otherwise
 * they are merged by timestamp, again keeping the newest row per timestamp.
 *
 * Values move as slots of the series' type (4 or 8 bytes); aggregation skips
 * nulls, and a bucket whose rows are all null is reported with count 0.
 */
#include "internal.h"
#include "tsdb_protocol.h"

#include <math.h>
#include <stdlib.h>

/* The buckets one query may compute (TSDB_ERR_TOO_MANY_BUCKETS beyond), counted from the first to the last
 * reading in range: two readings far apart in 1 ms buckets ask for one per millisecond between them. 64 Mi let a
 * 37-byte request make tsdbd allocate gigabytes before it found that the reply would not fit a frame; the working
 * memory now follows the buckets with rows (bq_window), and 4 Mi, as for TWA, keep the result within a frame. */
#define MAX_BUCKETS (4u << 20)
_Static_assert((uint64_t)MAX_BUCKETS * TSDB_AGG_ROW + 64 <= TSDB_MAX_FRAME, "an aggregate reply must fit a frame");
#define AGG_DENSE_BYTES (32u << 20) /* one dense array of bucket states (bq_window) */
#define AGG_STRIPES 64
#define AGG_STRIPE_SHIFT 10 /* 1024 consecutive buckets share a lock */

/* --------------------------------------------------------------- snapshot */


void snapshot_release(snapshot *s)
{
    db_release(s->parts, s->np);
    for (size_t i = 0; i < s->nmem; i++)
        mem_run_free(&s->mem[i]);
    free(s->mem);
    free(s->gi);
    free(s->gj);
    free(s->mprio);
    memset(s, 0, sizeof *s);
}

static int snapshot_prepare(eng *db, snapshot *s, uint32_t series, int64_t start, int64_t end);

/* --------------------------------------------------------------- cuts */

__thread qcuts *cur_cuts;

void qcuts_init(qcuts *cs)
{
    memset(cs, 0, sizeof *cs);
    pthread_mutex_init(&cs->lock, NULL);
}

static void cut_release(qcut *c)
{
    db_release(c->parts, c->np); /* frees the array */
    for (size_t i = 0; i < c->nms; i++)
        mt_unref(c->ms[i]);
    for (size_t i = 0; i < c->ntails; i++)
        mt_unref(c->tails[i]);
}

void qcuts_free(qcuts *cs)
{
    for (size_t i = 0; i < cs->n; i++) {
        cut_release(cs->c[i]);
        free(cs->c[i]);
    }
    free(cs->c);
    pthread_mutex_destroy(&cs->lock);
    memset(cs, 0, sizeof *cs);
}

/* The engine as snapshot_take sees it, for every series and time (TWA reads before its start): what a later
 * snapshot_take would add is a newer part, a new memtable, or rows with a higher lsn. */
static int cut_take(eng *db, qcut *c)
{
    memset(c, 0, sizeof *c);
    c->db = db;
    pthread_mutex_lock(&db->lock);
    c->parts = malloc((db->nparts ? db->nparts : 1) * sizeof *c->parts);
    if (!c->parts) {
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < db->nparts; i++) {
        part_ref(db->parts[i]);
        c->parts[c->np++] = db->parts[i];
    }
    for (size_t i = 0; i < db->nflushing; i++) {
        mt_ref(db->flushing[i]);
        c->ms[c->nms++] = db->flushing[i];
    }
    uint64_t visible = db->wal_enabled && db->opt.wal_sync_ms == 0 ? atomic_load(&db->wal_durable) : UINT64_MAX;
    pthread_mutex_lock(&db->buf_lock);
    c->visible = MIN(visible, db->wal_appended);
    if (db->active) {
        mt_ref(db->active);
        c->ms[c->nms++] = db->active;
        c->has_active = true;
    }
    for (size_t i = 0; i < db->ntails; i++) {
        mt_ref(db->tails[i].m);
        c->tails[c->ntails++] = db->tails[i].m;
    }
    c->tail_visible = db->tail_lsn;
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    return 0;
}

/* The request's cut of db, taken by its first series there. Each cut is allocated on its own: threads read theirs
 * after the lock is dropped while another volume's first series grows the array of pointers. */
static int cut_get(qcuts *cs, eng *db, qcut **out)
{
    int rc = 0;
    pthread_mutex_lock(&cs->lock);
    size_t i = 0;
    while (i < cs->n && cs->c[i]->db != db)
        i++;
    if (i == cs->n) {
        if (cs->n == cs->cap) {
            size_t cap = cs->cap ? 2 * cs->cap : 4;
            qcut **c = realloc(cs->c, cap * sizeof *c);
            if (!c)
                rc = TSDB_ERR_NOMEM;
            else {
                cs->c = c;
                cs->cap = cap;
            }
        }
        qcut *c = rc ? NULL : calloc(1, sizeof *c);
        if (rc == 0 && !c)
            rc = TSDB_ERR_NOMEM;
        if (rc == 0 && (rc = cut_take(db, c)) == 0)
            cs->c[cs->n++] = c;
        else
            free(c);
    }
    *out = rc ? NULL : cs->c[i];
    pthread_mutex_unlock(&cs->lock);
    return rc;
}

/* snapshot_take from a cut: the same memtable order (tails, flushing, active), each bounded by the cut's lsn */
static int snapshot_cut(eng *db, const qcut *c, uint32_t series, size_t w, int64_t start, int64_t end,
                        snapshot *s)
{
    int rc = 0;
    s->parts = malloc((c->np ? c->np : 1) * sizeof *s->parts);
    s->mem = calloc(c->ntails + c->nms + 1, sizeof *s->mem);
    if (!s->parts || !s->mem)
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < c->np; i++) {
        part *p = c->parts[i];
        if (p->max_ts >= start && p->min_ts < end) {
            part_ref(p);
            s->parts[s->np++] = p;
        }
    }
    bool locked = rc == 0 && (c->ntails || c->has_active);
    if (locked)
        pthread_mutex_lock(&db->buf_lock); /* tails and the active memtable may still grow */
    for (size_t i = 0; rc == 0 && i < c->ntails + c->nms; i++) {
        bool tail = i < c->ntails;
        const memtable *m = tail ? c->tails[i] : c->ms[i - c->ntails];
        mem_run r = {0};
        rc = mt_collect(m, series, w, start, end, tail ? c->tail_visible : c->visible, &r);
        if (r.n)
            s->mem[s->nmem++] = r;
        else
            mem_run_free(&r);
    }
    if (locked)
        pthread_mutex_unlock(&db->buf_lock);
    if (rc == 0)
        rc = snapshot_prepare(db, s, series, start, end);
    if (rc)
        snapshot_release(s);
    return rc;
}

/* Parts and memtables as of one instant: a flush publishes its parts and
 * retires its memtable under db->lock, so nothing is seen twice or missed. */
int snapshot_take(eng *db, uint32_t series, size_t w, int64_t start, int64_t end, snapshot *s)
{
    memset(s, 0, sizeof *s);
    if (cur_cuts) {
        qcut *c;
        int rc = cut_get(cur_cuts, db, &c);
        return rc ? rc : snapshot_cut(db, c, series, w, start, end, s);
    }
    memtable *ms[MAX_FLUSHING];
    size_t nms = 0;
    int rc = 0;
    mem_run active = {0};
    pthread_mutex_lock(&db->lock);
    /* With a WAL synced on every write, a row is visible once it is durable:
     * no reader sees a write that a power failure could still take back. That
     * includes the flushing memtables: a flush rotates the active one before
     * it writes and syncs the rest of its segment. */
    uint64_t visible = db->wal_enabled && db->opt.wal_sync_ms == 0 ? atomic_load(&db->wal_durable) : UINT64_MAX;
    s->parts = malloc((db->nparts ? db->nparts : 1) * sizeof *s->parts);
    if (s->parts) {
        for (size_t i = 0; i < db->nparts; i++) {
            part *p = db->parts[i];
            if (p->max_ts >= start && p->min_ts < end) {
                part_ref(p);
                s->parts[s->np++] = p;
            }
        }
    } else {
        rc = TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < db->nflushing; i++) {
        mt_ref(db->flushing[i]);
        ms[nms++] = db->flushing[i];
    }
    /* a follower's WAL tails: written to by the follower thread, so read under buf_lock */
    mem_run tails[MAX_TAILS];
    size_t ntails = 0;
    if (rc == 0 && (db->active || db->ntails)) {
        pthread_mutex_lock(&db->buf_lock);
        for (size_t i = 0; rc == 0 && i < db->ntails; i++) {
            rc = mt_collect(db->tails[i].m, series, w, start, end, UINT64_MAX, &tails[ntails]);
            if (tails[ntails].n)
                ntails++;
            else
                mem_run_free(&tails[ntails]);
        }
        if (rc == 0 && db->active)
            rc = mt_collect(db->active, series, w, start, end, visible, &active);
        pthread_mutex_unlock(&db->buf_lock);
    }
    pthread_mutex_unlock(&db->lock);

    s->mem = calloc(nms + ntails + 1, sizeof *s->mem);
    for (size_t i = 0; i < ntails; i++) { /* oldest first, like the writer's memtables */
        if (s->mem)
            s->mem[s->nmem++] = tails[i];
        else
            mem_run_free(&tails[i]);
    }
    if (!s->mem && rc == 0)
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; i < nms; i++) {
        /* flushing memtables are immutable: read them without locks */
        mem_run r = {0};
        if (rc == 0)
            rc = mt_collect(ms[i], series, w, start, end, visible, &r);
        if (r.n && s->mem)
            s->mem[s->nmem++] = r;
        else
            mem_run_free(&r);
        mt_unref(ms[i]);
    }
    if (active.n && s->mem)
        s->mem[s->nmem++] = active;
    else
        mem_run_free(&active);
    if (rc == 0)
        rc = snapshot_prepare(db, s, series, start, end);
    if (rc)
        snapshot_release(s);
    return rc;
}

/* Granules [*gi, *gj) of series s that may hold rows in [start, end). */
void part_find(const part *p, uint32_t s, int64_t start, int64_t end, uint32_t *gi, uint32_t *gj)
{
    const granule *g = p->g;
    uint32_t lo = 0, hi = p->ng;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (g[mid].series < s || (g[mid].series == s && g[mid].ts_max < start))
            lo = mid + 1;
        else
            hi = mid;
    }
    *gi = lo;
    hi = p->ng;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (g[mid].series < s || (g[mid].series == s && g[mid].ts_min < end))
            lo = mid + 1;
        else
            hi = mid;
    }
    *gj = lo;
}

/* Entry i's granules that may hold rows in [start, end), within its window. */
void snap_find(const snapshot *sn, size_t i, uint32_t series, int64_t start, int64_t end, uint32_t *gi,
                      uint32_t *gj)
{
    if (start <= sn->start && end >= sn->end) { /* the snapshot's window is the answer: no second search */
        *gi = sn->gi[i];
        *gj = sn->gj[i];
        return;
    }
    part_find(sn->parts[i], series, start, end, gi, gj);
    /* both ends clamped into the window: an empty answer stays inside it, so a caller stepping one granule
     * outward (TWA's neighbours) never reaches a granule the snapshot moved to mem */
    *gi = MIN(MAX(*gi, sn->gi[i]), sn->gj[i]);
    *gj = MAX(MIN(*gj, sn->gj[i]), sn->gi[i]);
}

typedef struct snap_build {
    part **parts;
    uint32_t *gi, *gj;
    size_t np, pcap;
    mem_run *mem;
    uint64_t *mprio;
    size_t nmem, mcap;
} snap_build;

static int sb_part(snap_build *b, part *p, uint32_t gi, uint32_t gj)
{
    if (b->np == b->pcap) {
        size_t cap = b->pcap ? b->pcap * 2 : 16;
        part **x = realloc(b->parts, cap * sizeof *x);
        if (x)
            b->parts = x;
        uint32_t *y = realloc(b->gi, cap * sizeof *y);
        if (y)
            b->gi = y;
        uint32_t *z = realloc(b->gj, cap * sizeof *z);
        if (z)
            b->gj = z;
        if (!x || !y || !z)
            return TSDB_ERR_NOMEM;
        b->pcap = cap;
    }
    part_ref(p);
    b->parts[b->np] = p;
    b->gi[b->np] = gi;
    b->gj[b->np] = gj;
    b->np++;
    return 0;
}

static int sb_mem(snap_build *b, mem_run r, uint64_t prio)
{
    if (b->nmem == b->mcap) {
        size_t cap = b->mcap ? b->mcap * 2 : 8;
        mem_run *x = realloc(b->mem, cap * sizeof *x);
        if (x)
            b->mem = x;
        uint64_t *y = realloc(b->mprio, cap * sizeof *y);
        if (y)
            b->mprio = y;
        if (!x || !y)
            return TSDB_ERR_NOMEM;
        b->mcap = cap;
    }
    b->mem[b->nmem] = r;
    b->mprio[b->nmem] = prio;
    b->nmem++;
    return 0;
}

/* Granules [k, e) of p decoded, cut to [start, end), without the rows h deletes. */
static int decode_visible(codec_ctx *cc, const part *p, uint32_t k, uint32_t e, int type, uint32_t series,
                          int64_t start, int64_t end, const tomb_set *h, mem_run *out)
{
    size_t w = type_width(type), total = 0;
    for (uint32_t x = k; x < e; x++)
        total += p->g[x].count;
    memset(out, 0, sizeof *out);
    out->ts = malloc(total * sizeof *out->ts + 1);
    out->v = malloc(total * w + 1);
    if (!out->ts || !out->v)
        return TSDB_ERR_NOMEM;
    for (uint32_t x = k; x < e; x++) {
        const granule *g = &p->g[x];
        int64_t *ts = out->ts + out->n;
        uint8_t *v = out->v + out->n * w;
        int rc = granule_read(cc, p, g, type, ts, v, p->verify);
        if (rc)
            return rc;
        size_t lo = lower_bound_i64(ts, g->count, start), hi = lower_bound_i64(ts, g->count, end);
        if (lo) {
            memmove(ts, ts + lo, (hi - lo) * sizeof *ts);
            memmove(v, v + lo * w, (hi - lo) * w);
        }
        out->n += tomb_set_filter(h, series, INT64_MIN, ts, v, w, hi - lo);
    }
    return 0;
}

/*
 * Each part entry gets its window of granules. When parts in the snapshot hold
 * tombstones, a part's granules that the tombstones of newer parts (of its
 * partition) touch are decoded and filtered into a run with the part's
 * priority; its other granules stay entries of their own, so their statistics
 * still answer aggregates. Without tombstones this is one part_find per part.
 */
static int snapshot_prepare(eng *db, snapshot *s, uint32_t series, int64_t start, int64_t end)
{
    s->start = start;
    s->end = end;
    s->gi = malloc((s->np + 1) * sizeof *s->gi);
    s->gj = malloc((s->np + 1) * sizeof *s->gj);
    s->mprio = malloc((s->nmem + 1) * sizeof *s->mprio);
    if (!s->gi || !s->gj || !s->mprio)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < s->nmem; i++)
        s->mprio[i] = UINT64_MAX - (s->nmem - i); /* memtables are newer than every part */
    bool tombs = false;
    for (size_t i = 0; i < s->np; i++) {
        part_find(s->parts[i], series, start, end, &s->gi[i], &s->gj[i]);
        tombs |= s->parts[i]->nt > 0;
    }
    if (!tombs)
        return 0;

    int type = eng_series_type(db, series);
    snap_build b = {0};
    codec_ctx *cc = NULL;
    int rc = 0;
    for (size_t i = 0; rc == 0 && i < s->np; i++) {
        part *p = s->parts[i];
        tomb_set h = {0};
        for (size_t j = 0; rc == 0 && s->gi[i] < s->gj[i] && j < s->np; j++) {
            const part *q = s->parts[j];
            if (!q->nt || q->partition != p->partition || q->min_block <= p->max_block)
                continue;
            for (uint32_t t = 0; rc == 0 && t < q->nt; t++)
                if (q->t[t].series == series && q->t[t].start < end && q->t[t].end > start)
                    rc = tomb_set_add(&h, &q->t[t], 1);
        }
        tomb_set_finish(&h);
        if (rc == 0 && h.n == 0) {
            rc = sb_part(&b, p, s->gi[i], s->gj[i]);
        } else {
            for (uint32_t k = s->gi[i], e; rc == 0 && k < s->gj[i]; k = e) {
                bool dirty = tomb_set_hits(&h, series, p->g[k].ts_min, p->g[k].ts_max);
                for (e = k + 1; e < s->gj[i] && tomb_set_hits(&h, series, p->g[e].ts_min, p->g[e].ts_max) == dirty;)
                    e++;
                if (!dirty) {
                    rc = sb_part(&b, p, k, e);
                    continue;
                }
                if (!cc && !(cc = cc_get(db))) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                mem_run r;
                rc = decode_visible(cc, p, k, e, type, series, start, end, &h, &r);
                if (rc == 0 && r.n)
                    rc = sb_mem(&b, r, p->max_block);
                if (rc || !r.n)
                    mem_run_free(&r);
            }
        }
        tomb_set_free(&h);
    }
    if (cc)
        cc_put(db, cc);
    for (size_t i = 0; rc == 0 && i < s->nmem; i++) {
        rc = sb_mem(&b, s->mem[i], s->mprio[i]);
        if (rc == 0)
            s->mem[i] = (mem_run){0}; /* moved */
    }
    if (rc) {
        db_release(b.parts, b.np);
        for (size_t i = 0; i < b.nmem; i++)
            if (b.mprio[i] < UINT64_MAX - s->nmem) /* decoded here; memtable runs stay with s */
                mem_run_free(&b.mem[i]);
        for (size_t i = 0, k = 0; i < b.nmem; i++) /* give moved memtable runs back */
            if (b.mprio[i] >= UINT64_MAX - s->nmem)
                s->mem[k++] = b.mem[i];
        free(b.gi);
        free(b.gj);
        free(b.mem);
        free(b.mprio);
        return rc;
    }
    db_release(s->parts, s->np);
    free(s->gi);
    free(s->gj);
    free(s->mem);
    free(s->mprio);
    s->parts = b.parts;
    s->gi = b.gi;
    s->gj = b.gj;
    s->np = b.np;
    s->mem = b.mem;
    s->mprio = b.mprio;
    s->nmem = b.nmem;
    return 0;
}

/* ---------------------------------------------------- deduplicating merge */

/* A time-ordered stream of unique timestamps from one source: a range of a
 * part's granules, or a memtable run. */

void src_init_part(src *c, const part *p, uint32_t gi, uint32_t gj, int type)
{
    memset(c, 0, sizeof *c);
    c->p = p;
    c->gi = gi;
    c->gj = gj;
    c->prio = p->max_block;
    c->type = type;
    c->w = type_width(type);
}

/* Memtable runs are newer than every part; later memtables are newer. */
void src_init_mem(src *c, const mem_run *r, uint64_t prio, int type)
{
    memset(c, 0, sizeof *c);
    c->prio = prio;
    c->ts = r->ts;
    c->v = r->v;
    c->n = r->n;
    c->loaded = r->n > 0;
    c->type = type;
    c->w = type_width(type);
}

void src_free(src *c)
{
    if (c->own) {
        free(c->ts);
        free(c->v);
    }
}

static int64_t src_first_ts(const src *c) { return c->p ? c->p->g[c->gi].ts_min : c->ts[0]; }
static int64_t src_last_ts(const src *c) { return c->p ? c->p->g[c->gj - 1].ts_max : c->ts[c->n - 1]; }

/* Decodes granules until one has rows in [start, end), or the source ends. */
static int src_fill(codec_ctx *cc, src *c, int64_t start, int64_t end)
{
    if (c->loaded || !c->p)
        return 0;
    if (!c->own && c->gi < c->gj) {
        /* as many rows as its largest granule, not the part's granule size: a query holds a source per part of
           its range at once, and parts of small batches (unmerged, or many series each) have granules of a few
           hundred rows. Sized by granule_rows, 4,500 such parts asked for 576 MB per query, and the heap's trim
           and the page faults after it were a third of a raw scan. A count above grows is refused by
           granule_read before it decodes. */
        uint32_t rows = 1;
        for (uint32_t k = c->gi; k < c->gj; k++)
            rows = MAX(rows, c->p->g[k].count);
        rows = MIN(rows, c->p->grows);
        c->ts = malloc(rows * sizeof *c->ts);
        c->v = malloc(rows * c->w);
        c->own = true;
        if (!c->ts || !c->v)
            return TSDB_ERR_NOMEM;
    }
    while (c->gi < c->gj) {
        const granule *g = &c->p->g[c->gi++];
        int rc = granule_read(cc, c->p, g, c->type, c->ts, c->v, c->p->verify);
        if (rc)
            return rc;
        c->pos = g->ts_min >= start ? 0 : lower_bound_i64(c->ts, g->count, start);
        c->n = g->ts_max < end ? g->count : lower_bound_i64(c->ts, g->count, end);
        if (c->pos < c->n) {
            c->loaded = true;
            return 0;
        }
    }
    return 0;
}

static int src_advance(codec_ctx *cc, src *c, size_t rows, int64_t start, int64_t end)
{
    c->pos += rows;
    if (c->pos < c->n)
        return 0;
    c->loaded = false;
    return src_fill(cc, c, start, end);
}


/* Emits the union of the sources in time order, one row per timestamp: the
 * row of the source with the highest priority. Every live source is kept
 * decoded, so the head timestamps are exact. */
int merge_stream(codec_ctx *cc, src *cur, size_t nc, int64_t start, int64_t end, slot_fn fn, void *ctx)
{
    int rc = 0;
    for (size_t i = 0; i < nc && rc == 0; i++)
        rc = src_fill(cc, &cur[i], start, end);
    while (rc == 0) {
        src *c = NULL;
        int64_t bound = INT64_MAX;
        bool has_bound = false;
        for (size_t i = 0; i < nc; i++) {
            src *x = &cur[i];
            if (!x->loaded)
                continue;
            int64_t t = x->ts[x->pos];
            if (!c || t < c->ts[c->pos] || (t == c->ts[c->pos] && x->prio > c->prio)) {
                if (c) {
                    int64_t ct = c->ts[c->pos];
                    if (!has_bound || ct < bound)
                        bound = ct;
                    has_bound = true;
                }
                c = x;
            } else if (!has_bound || t < bound) {
                bound = t;
                has_bound = true;
            }
        }
        if (!c)
            break;
        int64_t head = c->ts[c->pos];
        size_t e = c->n;
        if (has_bound)
            e = head < bound ? c->pos + lower_bound_i64(c->ts + c->pos, c->n - c->pos, bound) : c->pos + 1;
        if (fn(ctx, c->ts + c->pos, c->v + c->pos * c->w, e - c->pos))
            return TSDB_ERR_STOPPED;
        rc = src_advance(cc, c, e - c->pos, start, end);
        if (has_bound && head == bound) {
            /* the same timestamp in older sources: replaced, drop it */
            for (size_t i = 0; i < nc && rc == 0; i++)
                if (&cur[i] != c && cur[i].loaded && cur[i].ts[cur[i].pos] == head)
                    rc = src_advance(cc, &cur[i], 1, start, end);
        }
    }
    return rc;
}

/* --------------------------------------------------------------- buckets */

static const int64_t unit_ms[] = {0, 1000, 60000, 3600000};

int bucketer_init(bucketer *bk, int64_t start, const tsdb_group *g)
{
    memset(bk, 0, sizeof *bk);
    bk->start = start;
    if (!g) {
        bk->kind = BK_ONE;
        return 0;
    }
    if (g->unit < TSDB_SECOND || g->unit > TSDB_YEAR)
        return TSDB_ERR_INVALID;
    if (g->timezone && strcmp(g->timezone, "UTC") != 0)
        return TSDB_ERR_INVALID; /* UTC only: a client turns local periods into bounds */
    bk->count = g->count ? g->count : 1;
    if (g->unit <= TSDB_HOUR) {
        bk->kind = BK_FIXED;
        bk->interval = unit_ms[g->unit] * bk->count;
        return 0;
    }
    bk->kind = BK_CALENDAR;
    bk->unit = g->unit;
    return 0;
}

int bucketer_bounds(bucketer *bk, const int64_t *bounds, size_t n)
{
    memset(bk, 0, sizeof *bk);
    if (n == 0)
        return TSDB_ERR_INVALID;
    for (size_t i = 1; i < n; i++)
        if (bounds[i] <= bounds[i - 1])
            return TSDB_ERR_INVALID;
    bk->kind = BK_BOUNDS;
    bk->bounds = bounds;
    bk->nbounds = n;
    return 0;
}

static inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

static inline void civil_from_days(int64_t days, int64_t *y, int *m)
{
    int64_t z = days + 719468, era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097), yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
    *m = (int)(mp < 10 ? mp + 3 : mp - 9);
    *y = (int64_t)yoe + era * 400 + (*m <= 2);
}

/* index of the UTC calendar unit (days, ISO weeks, months, quarters, years since 1970) */
static int64_t cal_index(const bucketer *bk, int64_t ts)
{
    int64_t day = floor_div(ts, 86400000);
    switch (bk->unit) {
    case TSDB_DAY: return day;
    case TSDB_WEEK: return floor_div(day + 3, 7); /* 1970-01-01 was a Thursday */
    default: {
        int64_t y;
        int m;
        civil_from_days(day, &y, &m);
        int64_t month = (y - 1970) * 12 + (m - 1);
        return bk->unit == TSDB_MONTH ? month : bk->unit == TSDB_QUARTER ? floor_div(month, 3) : y - 1970;
    }
    }
}

/* the day (since 1970) on which calendar unit `idx` begins */
static int64_t cal_start_day(const bucketer *bk, int64_t idx)
{
    switch (bk->unit) {
    case TSDB_DAY: return idx;
    case TSDB_WEEK: return idx * 7 - 3;
    case TSDB_MONTH:
    case TSDB_QUARTER: {
        int64_t month = bk->unit == TSDB_MONTH ? idx : idx * 3;
        return days_from_civil(1970 + floor_div(month, 12), (unsigned)(month - floor_div(month, 12) * 12) + 1, 1);
    }
    default: return days_from_civil(1970 + idx, 1, 1);
    }
}

static inline int64_t bk_key(const bucketer *bk, int64_t ts)
{
    switch (bk->kind) {
    case BK_ONE: return 0;
    case BK_FIXED: return floor_div(ts, bk->interval);
    case BK_BOUNDS: { /* the last bound at or before ts */
        size_t i = lower_bound_i64(bk->bounds, bk->nbounds, sat_add(ts, 1));
        return (int64_t)i - 1;
    }
    default: return floor_div(cal_index(bk, ts), bk->count);
    }
}

/* The first instant (ms) of bucket `key`. */
static inline int64_t bk_start(const bucketer *bk, int64_t key)
{
    switch (bk->kind) {
    case BK_ONE: return bk->start;
    case BK_FIXED: return sat_mul(key, bk->interval);
    case BK_BOUNDS: return key < 0 ? INT64_MIN : (size_t)key < bk->nbounds ? bk->bounds[key] : INT64_MAX;
    default: return sat_mul(cal_start_day(bk, key * bk->count), 86400000);
    }
}

int64_t bucketer_key(const bucketer *bk, int64_t ts) { return bk_key(bk, ts); }
int64_t bucketer_start(const bucketer *bk, int64_t key) { return bk_start(bk, key); }

/* -------------------------------------------------------------- aggregate */

/* u[k..] += x * y, in 64-bit limbs (the carry runs off the top of a 256-bit number: never, for the callers) */
static void mul_add256(uint64_t u[4], int k, uint64_t x, uint64_t y)
{
    unsigned __int128 p = (unsigned __int128)x * y;
    unsigned __int128 c = 0;
    for (int i = k; i < 4; i++) {
        c += u[i];
        if (i == k)
            c += (uint64_t)p;
        else if (i == k + 1)
            c += (uint64_t)(p >> 64);
        u[i] = (uint64_t)c;
        c >>= 64;
    }
}

/* Integer variances from the exact sums: n^2 var_pop = n sum(x^2) - sum(x)^2,
 * a 256-bit integer, exact; converted once to long double (64-bit mantissa on
 * x86, so two roundings at most before the final one to double). Depends on
 * the multiset of values only: the same bits whatever the order. */
static void int_variance(const agg_state *a, const agg_dev *x, double *vp, double *vs)
{
    uint64_t n = a->count, A[4] = {0}, B[4] = {0}, D[4];
    mul_add256(A, 0, n, (uint64_t)x->isq);
    mul_add256(A, 1, n, (uint64_t)(x->isq >> 64));
    mul_add256(A, 2, n, x->isq_hi);
    unsigned __int128 s = a->isum < 0 ? -(unsigned __int128)a->isum : (unsigned __int128)a->isum;
    uint64_t lo = (uint64_t)s, hi = (uint64_t)(s >> 64);
    mul_add256(B, 0, lo, lo);
    mul_add256(B, 1, lo, hi);
    mul_add256(B, 1, lo, hi);
    mul_add256(B, 2, hi, hi);
    unsigned __int128 borrow = 0;
    for (int i = 0; i < 4; i++) {
        unsigned __int128 d = (unsigned __int128)A[i] - B[i] - borrow;
        D[i] = (uint64_t)d;
        borrow = (d >> 64) ? 1 : 0;
    }
    long double num = 0;
    for (int i = 3; i >= 0; i--)
        num = num * 0x1p64L + (long double)D[i];
    long double ln = (long double)n;
    *vp = (double)(num / (ln * ln));
    *vs = n > 1 ? (double)(num / (ln * (long double)(n - 1))) : nan_canonical();
}

void agg_spread(int type, const agg_state *a, const agg_dev *x, tsdb_spread_bucket *b)
{
    const double nan = nan_canonical();
    double vp, vs;
    b->range = b->var_pop = b->var_samp = b->stddev_pop = b->stddev_samp = nan;
    if (type == TSDB_STATE || a->count == 0)
        return;
    if (type_is_int(type)) {
        b->range = (double)((__int128)a->imax - a->imin);
        int_variance(a, x, &vp, &vs);
    } else {
        if (a->min <= a->max) /* else only NaN values */
            b->range = canon_nan(a->max - a->min);
        /* constant data: exactly 0, where the means of the runs combined may differ in the last bit */
        double m2 = a->min == a->max && isfinite(a->sum) ? 0 : x->m2;
        vp = m2 / (double)a->count;
        vs = a->count > 1 ? m2 / (double)(a->count - 1) : nan;
    }
    b->var_pop = canon_nan(vp);
    b->var_samp = canon_nan(vs);
    b->stddev_pop = canon_nan(__builtin_sqrt(vp));
    b->stddev_samp = canon_nan(__builtin_sqrt(vs));
}


typedef struct local_bucket {
    int64_t b;
    agg_state a;
} local_bucket;

typedef struct agg_target {
    int64_t start, end, b0;
    const bucketer *bk;
    agg_state *acc;
    agg_dev *accx; /* beside acc for the spread (tsdb_query_spread), else NULL */
    int type;
    size_t w;
} agg_target;



/* One run of rows into a: float32 without nulls goes straight to the SIMD
 * kernel, which matters when buckets hold a handful of rows each. A short run takes the short-run kernel (the
 * same bits, as values.c's fold_f32 picks it): the long kernel's setup and 32-lane combine cost more than the
 * fold itself at a few rows (tsdb-bench "agg buckets of 10", AVX-512: 10% faster). */
static inline void fold_run(const agg_target *t, const uint8_t *v, size_t n, bool may_be_null, agg_state *a,
                            agg_dev *x)
{
    if (t->type == TSDB_FLOAT32 && !may_be_null) {
        a->rows += n;
        if (n < simd_short)
            simd_small_f32((const float *)v, n, false, a, x);
        else
            simd_agg_f32((const float *)v, n, a, x);
    } else {
        agg_fold_hint(t->type, v, n, may_be_null, a, x);
    }
}

/* Folds time-sorted rows (all within [start, end)) into per-bucket states,
 * one entry per bucket touched (and its spread in outx, with t->accx); returns the number of entries. */
static size_t fold_rows(const agg_target *t, const int64_t *ts, const void *vv, size_t n, bool may_be_null,
                        local_bucket *out, agg_dev *outx)
{
    const uint8_t *v = vv;
    if (!n)
        return 0;
    if (t->bk->kind == BK_ONE) {
        out[0].b = t->b0;
        agg_init(&out[0].a, t->type);
        if (outx)
            agg_dev_init(&outx[0]);
        fold_run(t, v, n, may_be_null, &out[0].a, outx);
        return 1;
    }
    size_t nl = 0;
    for (size_t i = 0; i < n;) {
        /* one run of rows per bucket: the next bucket's start ends it */
        int64_t b = bk_key(t->bk, ts[i]);
        int64_t bend = bk_start(t->bk, b + 1);
        size_t e = bend == INT64_MAX ? n : i + 1 + lower_bound_i64(ts + i + 1, n - i - 1, bend);
        out[nl].b = b;
        agg_init(&out[nl].a, t->type);
        if (outx)
            agg_dev_init(&outx[nl]);
        fold_run(t, v + i * t->w, e - i, may_be_null, &out[nl].a, outx ? &outx[nl] : NULL);
        nl++;
        i = e;
    }
    return nl;
}

static inline void fold_into(const agg_target *t, const local_bucket *l, const agg_dev *lx, size_t nl)
{
    agg_state *acc = t->acc;
    if (t->accx) { /* the spread: its cost stays out of the loops below */
        for (size_t k = 0; k < nl; k++)
            agg_combine(&acc[l[k].b - t->b0], &t->accx[l[k].b - t->b0], &l[k].a, &lx[k], t->type);
        return;
    }
    /* the type decided once, not per bucket: buckets can be a handful of rows each */
    if (t->type == TSDB_FLOAT32 || t->type == TSDB_FLOAT64) {
        for (size_t k = 0; k < nl; k++) {
            agg_state *a = &acc[l[k].b - t->b0];
            const agg_state *x = &l[k].a;
            a->rows += x->rows;
            a->count += x->count;
            a->sum += x->sum;
            a->min = x->min < a->min ? x->min : a->min;
            a->max = x->max > a->max ? x->max : a->max;
        }
    } else {
        for (size_t k = 0; k < nl; k++)
            agg_combine(&acc[l[k].b - t->b0], NULL, &l[k].a, NULL, t->type);
    }
}

typedef struct fold_ctx {
    const agg_target *t;
    local_bucket *scratch;
    agg_dev *scratchx; /* with t->accx */
    size_t cap;
} fold_ctx;

static int fold_emit(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    fold_ctx *f = ctx;
    if (n > f->cap) {
        local_bucket *s = realloc(f->scratch, n * sizeof *s);
        if (!s)
            return 1;
        f->scratch = s;
        if (f->t->accx) {
            agg_dev *x = realloc(f->scratchx, n * sizeof *x);
            if (!x)
                return 1;
            f->scratchx = x;
        }
        f->cap = n;
    }
    fold_into(f->t, f->scratch, f->scratchx, fold_rows(f->t, ts, v, n, true, f->scratch, f->scratchx));
    return 0;
}

typedef struct gref {
    const part *p;
    const granule *g;
} gref;

typedef struct agg_job {
    eng *db;
    qstats *qs; /* the caller's: workers count into it too */
    const gref *work;
    size_t nwork;
    atomic_size_t next;
    agg_target t;
    /* striped by bucket range: workers decoding different time ranges rarely meet */
    pthread_mutex_t locks[AGG_STRIPES];
    /* Each granule's first and last bucket, the only ones other items may share, folded in work order once
     * the workers are done: a bucket's states combine in one order, however the threads ran (the spread and
     * float sums depend on it), and a window of the range (bucket_windows) combines as the whole range. */
    local_bucket *edge; /* 2 per work item, nedge of them set (a count, not a key sentinel: every int64 is a key) */
    agg_dev *edgex;
    uint8_t *nedge;
    atomic_int err;
} agg_job;

uint64_t query_rows_per_thread = QUERY_ROWS_PER_THREAD;

int query_threads_for(const eng *db, uint64_t rows, size_t items)
{
    uint64_t per = query_rows_per_thread ? query_rows_per_thread : 1;
    uint64_t t = rows / per;
    t = MIN(t, (uint64_t)items);
    t = MIN(t, (uint64_t)db->opt.query_threads);
    return t > 1 ? (int)t : 1;
}

static void agg_worker(void *arg, int idx)
{
    (void)idx;
    agg_job *j = arg;
    eng *db = j->db;
    qstats *mine = cur_qstats;
    cur_qstats = j->qs;
    codec_ctx *cc = cc_get(db);
    size_t cap = db->granule_rows;
    int64_t *ts = qscratch_get(0, cap * sizeof *ts);
    uint8_t *v = qscratch_get(1, cap * sizeof(uint64_t));
    local_bucket *local = qscratch_get(2, cap * sizeof *local); /* a granule touches at most count buckets */
    agg_dev *localx = j->t.accx ? qscratch_get(3, cap * sizeof *localx) : NULL;
    if (!cc || !ts || !v || !local || (j->t.accx && !localx)) {
        atomic_store(&j->err, TSDB_ERR_NOMEM);
        goto out;
    }
    for (;;) {
        size_t w = atomic_fetch_add(&j->next, 1);
        if (w >= j->nwork || atomic_load_explicit(&j->err, memory_order_relaxed))
            break;
        const gref *r = &j->work[w];
        int rc = granule_read(cc, r->p, r->g, j->t.type, ts, v, r->p->verify);
        if (rc) {
            atomic_store(&j->err, rc);
            break;
        }
        size_t n = r->g->count;
        size_t lo = lower_bound_i64(ts, n, j->t.start);
        size_t hi = lower_bound_i64(ts, n, j->t.end);
        size_t nl = fold_rows(&j->t, ts + lo, v + lo * j->t.w, hi > lo ? hi - lo : 0, r->g->nulls > 0, local,
                              localx);
        j->nedge[w] = nl > 1 ? 2 : (uint8_t)nl;
        if (nl) {
            j->edge[2 * w] = local[0];
            if (localx)
                j->edgex[2 * w] = localx[0];
            if (nl > 1) {
                j->edge[2 * w + 1] = local[nl - 1];
                if (localx)
                    j->edgex[2 * w + 1] = localx[nl - 1];
            }
            /* local buckets ascend: fold each run that falls in one stripe under its lock (the buckets between
             * the edges are this granule's alone: anything else with rows there would overlap it) */
            for (size_t a = 1; a + 1 < nl;) {
                size_t stripe = (size_t)((local[a].b - j->t.b0) >> AGG_STRIPE_SHIFT);
                size_t e = a + 1;
                while (e + 1 < nl && (size_t)((local[e].b - j->t.b0) >> AGG_STRIPE_SHIFT) == stripe)
                    e++;
                pthread_mutex_t *m = &j->locks[stripe % AGG_STRIPES];
                pthread_mutex_lock(m);
                fold_into(&j->t, local + a, localx ? localx + a : NULL, e - a);
                pthread_mutex_unlock(m);
                a = e;
            }
        }
        int lv = sched_leave();
        if (lv < 0)
            atomic_store(&j->err, lv);
        if (lv)
            break;
    }
out:
    if (cc)
        cc_put(db, cc);
    qscratch_put(ts);
    qscratch_put(v);
    qscratch_put(local);
    qscratch_put(localx);
    cur_qstats = mine;
}

/* One granule, or one memtable run, with its time range and source. */
typedef struct item {
    int64_t lo, hi;
    uint32_t src; /* index into the snapshot's sources: parts, then memtable runs */
    const granule *g;
} item;

static int cmp_item(const void *a, const void *b)
{
    const item *x = a, *y = b;
    return (x->lo > y->lo) - (x->lo < y->lo);
}

uint64_t mem_prio(const snapshot *sn, size_t i) { return sn->mprio[i]; }

/* Decoded runs take their part's max_block as priority, far below the memtables' UINT64_MAX - k; their rows are
 * already counted in rows_decoded. */
bool mem_from_memtable(const snapshot *sn, size_t i) { return sn->mprio[i] >= UINT64_MAX - sn->nmem; }

/* ------------------------------------------------- order, limit, early stop
 *
 * A bucketed query with a limit computes only the buckets it returns, plus
 * a few: one snapshot of the whole range, then windows whose edges are bucket
 * edges, from the start (ascending) or from the end (descending), each
 * computed on its own as if it were the query range, until `limit` non-empty
 * buckets are in. A bucket lies inside one window, so it is computed from the
 * same rows, the same granule statistics and in the same order as by one pass
 * over the whole range: the results are those of computing everything and
 * cutting. A window spans max(still needed, twice the previous) buckets
 * counted from the nearest data (w->bound skips empty time), so a sparse
 * range takes a few windows, and the last N of a long range touch about N
 * buckets' granules (plus those a window edge cuts, which are decoded). */
static void bk_swap(uint8_t *a, uint8_t *b, size_t row)
{
    uint8_t x[sizeof(tsdb_spread_bucket) > sizeof(tsdb_twa_bucket) ? sizeof(tsdb_spread_bucket)
                                                                    : sizeof(tsdb_twa_bucket)];
    memcpy(x, a, row);
    memcpy(a, b, row);
    memcpy(b, x, row);
}

int bucket_windows(const bk_windows *w, int64_t start, int64_t end, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (start >= end)
        return 0;
    if (w->limit == 0 || w->bk->kind == BK_ONE) { /* everything, then order and cut */
        int rc = w->run(w->ctx, start, end, out, nout);
        if (rc == 0 && w->desc)
            for (size_t i = 0, j = *nout ? *nout - 1 : 0; i < j; i++, j--)
                bk_swap((uint8_t *)*out + i * w->row, (uint8_t *)*out + j * w->row, w->row);
        if (rc == 0 && w->limit && *nout > w->limit)
            *nout = w->limit;
        return rc;
    }
    uint8_t *res = NULL;
    size_t got = 0, span = 0;
    int64_t kfirst = bk_key(w->bk, start), klast = bk_key(w->bk, end - 1);
    int64_t lo = start, hi = end;
    int rc = 0;
    while (rc == 0 && got < w->limit && lo < hi) {
        size_t need = w->limit - got;
        span = MAX(need, span > SIZE_MAX / 2 ? SIZE_MAX : span * 2);
        uint64_t n = MIN(span, (uint64_t)INT64_MAX);
        int64_t p; /* the nearest data: buckets before it are empty */
        if (!w->bound(w->ctx, lo, hi, w->desc, &p))
            break;
        int64_t kp = bk_key(w->bk, p), a = lo, b = hi;
        if (w->desc) {
            if ((uint64_t)kp - (uint64_t)kfirst >= n)
                a = bk_start(w->bk, kp - (int64_t)(n - 1));
        } else if ((uint64_t)klast - (uint64_t)kp >= n) {
            b = bk_start(w->bk, kp + (int64_t)n);
        }
        void *wr;
        size_t wn;
        rc = w->run(w->ctx, a, b, &wr, &wn);
        if (rc)
            break;
        if (wn && !res) { /* the first window with buckets: its array is the result so far, in the query's order */
            for (size_t i = 0, j = wn - 1; w->desc && i < j; i++, j--)
                bk_swap((uint8_t *)wr + i * w->row, (uint8_t *)wr + j * w->row, w->row);
            res = wr;
            got = wn;
            if (w->desc)
                hi = a;
            else
                lo = b;
            continue;
        }
        uint8_t *r2 = wn ? realloc(res, (got + wn) * w->row) : res;
        if (wn && !r2) {
            free(wr);
            rc = TSDB_ERR_NOMEM;
            break;
        }
        res = r2;
        for (size_t i = 0; i < wn; i++) /* a window's buckets are ascending */
            memcpy(res + (got + i) * w->row, (uint8_t *)wr + (w->desc ? wn - 1 - i : i) * w->row, w->row);
        got += wn;
        free(wr);
        if (w->desc)
            hi = a;
        else
            lo = b;
    }
    if (rc) {
        free(res);
        return rc;
    }
    *out = res ? res : malloc(w->row);
    if (!*out)
        return TSDB_ERR_NOMEM;
    *nout = MIN(got, w->limit);
    return 0;
}

/* Items that overlap in time form a cluster; one with items of several sources is merged (last write wins).
 * Clusters are found once for the whole range: a window takes each from it as it is, so every row is folded
 * the same way (from statistics, decoded, merged) and in the same order as without windows. */
typedef struct cluster {
    size_t a, b; /* items [a, b) */
    int64_t lo, hi;
    bool multi;
} cluster;

/* One bucketed query (aggregates, the spread) over one snapshot, window by window. */
typedef struct bq {
    eng *db;
    const snapshot *sn;
    uint32_t series;
    int type;
    const bucketer *bk;
    bool spread;
    item *items; /* every granule and memtable run of the series in the range, by time */
    size_t nitem;
    cluster *cl; /* ascending and disjoint in time */
    size_t ncl;
} bq;

static int bq_items(bq *q, int64_t start, int64_t end)
{
    const snapshot *sn = q->sn;
    size_t cap = 0;
    for (size_t i = 0; i < sn->np + sn->nmem; i++) {
        uint32_t gi = 0, gj;
        if (i < sn->np)
            snap_find(sn, i, q->series, start, end, &gi, &gj);
        else
            gj = sn->mem[i - sn->np].n > 0;
        for (uint32_t k = gi; k < gj; k++) {
            if (q->nitem == cap) {
                cap = cap ? cap * 2 : 256;
                item *it = realloc(q->items, cap * sizeof *it);
                if (!it)
                    return TSDB_ERR_NOMEM;
                q->items = it;
            }
            item *it = &q->items[q->nitem++];
            it->src = (uint32_t)i;
            if (i < sn->np) {
                it->g = &sn->parts[i]->g[k];
                it->lo = it->g->ts_min;
                it->hi = it->g->ts_max;
            } else {
                const mem_run *r = &sn->mem[i - sn->np];
                it->g = NULL;
                it->lo = r->ts[0];
                it->hi = r->ts[r->n - 1];
            }
        }
    }
    if (!q->nitem)
        return 0;
    qsort(q->items, q->nitem, sizeof *q->items, cmp_item);
    if (!(q->cl = malloc(q->nitem * sizeof *q->cl)))
        return TSDB_ERR_NOMEM;
    const item *items = q->items;
    for (size_t a = 0; a < q->nitem;) {
        size_t b = a + 1;
        int64_t hi = items[a].hi;
        bool multi = false;
        while (b < q->nitem && items[b].lo <= hi) {
            multi |= items[b].src != items[a].src;
            hi = MAX(hi, items[b].hi);
            b++;
        }
        q->cl[q->ncl++] = (cluster){a, b, items[a].lo, hi, multi};
        a = b;
    }
    return 0;
}

/* A memtable run's rows in [start, end): a view */
static mem_run run_cut(const mem_run *r, int64_t start, int64_t end, size_t w)
{
    size_t a = lower_bound_i64(r->ts, r->n, start), b = lower_bound_i64(r->ts, r->n, end);
    return (mem_run){r->ts + a, r->v + a * w, b - a};
}

/* The newest (oldest) row time in [lo, hi), from the granule index and the memtable runs; a granule that
 * reaches out of the range answers with the range's edge. False when there is none. */
static bool bq_bound(void *ctx, int64_t lo, int64_t hi, bool newest, int64_t *at)
{
    const bq *q = ctx;
    const snapshot *sn = q->sn;
    bool any = false;
    int64_t best = newest ? INT64_MIN : INT64_MAX;
    for (size_t i = 0; i < sn->np; i++) {
        uint32_t gi, gj;
        snap_find(sn, i, q->series, lo, hi, &gi, &gj);
        if (gi < gj) {
            any = true;
            best = newest ? MAX(best, MIN(sn->parts[i]->g[gj - 1].ts_max, hi - 1))
                          : MIN(best, MAX(sn->parts[i]->g[gi].ts_min, lo));
        }
    }
    for (size_t i = 0; i < sn->nmem; i++) {
        mem_run r = run_cut(&sn->mem[i], lo, hi, type_width(q->type));
        if (r.n) {
            any = true;
            best = newest ? MAX(best, r.ts[r.n - 1]) : MIN(best, r.ts[0]);
        }
    }
    *at = best;
    return any;
}

/* Bucket states of every dense array so far (once per array, not per row); tests check the work follows the rows */
size_t agg_dense_states;

/* The first cluster that ends at or after `start` */
static size_t bq_first_cluster(const bq *q, int64_t start)
{
    size_t a = 0, b = q->ncl;
    while (a < b) {
        size_t m = a + (b - a) / 2;
        if (q->cl[m].hi < start)
            a = m + 1;
        else
            b = m;
    }
    return a;
}

/* The clusters that reach into [start, end): [*c0, *c1) */
static void bq_clusters(const bq *q, int64_t start, int64_t end, size_t *c0, size_t *c1)
{
    size_t a = bq_first_cluster(q, start), b;
    for (b = a; b < q->ncl && q->cl[b].lo < end;)
        b++;
    *c0 = a;
    *c1 = b;
}

/* The buckets of [start, end) (bucket edges, or the query's ends) in one dense array of nb states from bucket b0
 * on, which holds every bucket with rows there; clusters [c0, c1), at most nin items. Ascending. out: tsdb_bucket,
 * or with spread tsdb_spread_bucket (which begins with one) */
static int bq_dense(const bq *q, int64_t start, int64_t end, size_t c0, size_t c1, size_t nin, int64_t b0,
                    size_t nb, void **out, size_t *nout)
{
    eng *db = q->db;
    const snapshot *sn = q->sn;
    int type = q->type;
    size_t wd = type_width(type);
    const bucketer *bk = q->bk;
    const item *items = q->items;
    int rc = 0;
    *out = NULL;
    *nout = 0;
    agg_state *acc = NULL;
    agg_dev *accx = NULL;
    gref *work = NULL;
    src *srcs = NULL;
    codec_ctx *cc = NULL;
    fold_ctx fc = {0};
    agg_job *j = NULL;
    agg_target t = {.start = start, .end = end, .b0 = b0, .bk = bk, .type = type, .w = wd};
    __atomic_fetch_add(&agg_dense_states, nb, __ATOMIC_RELAXED);
    acc = malloc(nb * sizeof *acc);
    accx = q->spread ? malloc(nb * sizeof *accx) : NULL;
    work = malloc(nin * sizeof *work);
    srcs = malloc((sn->np + sn->nmem) * sizeof *srcs);
    cc = cc_get(db);
    if (!acc || (q->spread && !accx) || !work || !srcs || !cc) {
        rc = TSDB_ERR_NOMEM;
        goto done;
    }
    for (size_t k = 0; k < nb; k++)
        agg_init(&acc[k], type);
    for (size_t k = 0; accx && k < nb; k++)
        agg_dev_init(&accx[k]);
    t.acc = acc;
    t.accx = accx;
    fc.t = &t;

    size_t nwork = 0;
    for (size_t ci = c0; ci < c1 && rc == 0; ci++) {
        const cluster *c = &q->cl[ci];
        if (!c->multi) {
            for (size_t k = c->a; k < c->b && rc == 0; k++) {
                const item *it = &items[k];
                if (it->hi < start || it->lo >= end)
                    continue;
                if (!it->g) {
                    mem_run r = run_cut(&sn->mem[it->src - sn->np], start, end, wd);
                    if (r.n && fold_emit(&fc, r.ts, r.v, r.n))
                        rc = TSDB_ERR_NOMEM;
                    continue;
                }
                const granule *g = it->g;
                bool inside = g->ts_min >= start && g->ts_max < end;
                int64_t bmin = bk_key(bk, g->ts_min);
                int64_t bmax = bk_key(bk, g->ts_max);
                bool one = bmin == bmax; /* keys only grow with time: one key, one bucket */
                if (inside && one) {
                    agg_state ga;
                    agg_dev gx;
                    QS_ADD(granules_from_stats, 1);
                    QS_ADD(rows_from_stats, g->count);
                    agg_from_granule(type, g, &ga, accx ? &gx : NULL);
                    agg_combine(&acc[bmin - t.b0], accx ? &accx[bmin - t.b0] : NULL, &ga, &gx, type);
                } else
                    work[nwork++] = (gref){sn->parts[it->src], g};
            }
            continue;
        }
        /* sources overlap here: merge them so replaced rows are not counted */
        size_t ns = 0;
        for (uint32_t s = 0; s < sn->np + sn->nmem; s++) {
            uint32_t gi = UINT32_MAX, gj = 0;
            bool any = false;
            for (size_t k = c->a; k < c->b; k++) {
                const item *it = &items[k];
                if (it->src != s || it->hi < start || it->lo >= end)
                    continue;
                any = true;
                if (s < sn->np) {
                    uint32_t idx = (uint32_t)(it->g - sn->parts[s]->g);
                    gi = MIN(gi, idx);
                    gj = MAX(gj, idx + 1);
                }
            }
            if (!any)
                continue;
            if (s < sn->np) {
                src_init_part(&srcs[ns++], sn->parts[s], gi, gj, type);
            } else {
                mem_run r = run_cut(&sn->mem[s - sn->np], start, end, wd);
                if (r.n)
                    src_init_mem(&srcs[ns++], &r, mem_prio(sn, s - sn->np), type);
            }
        }
        rc = merge_stream(cc, srcs, ns, start, end, fold_emit, &fc);
        if (rc == TSDB_ERR_STOPPED)
            rc = TSDB_ERR_NOMEM;
        for (size_t k = 0; k < ns; k++)
            src_free(&srcs[k]);
    }
    if (rc == 0 && nwork) {
        j = calloc(1, sizeof *j);
        local_bucket *edge = j ? malloc(2 * nwork * sizeof *edge) : NULL;
        agg_dev *edgex = j && accx ? malloc(2 * nwork * sizeof *edgex) : NULL;
        uint8_t *nedge = j ? calloc(nwork, 1) : NULL;
        if (!edge || !nedge || (accx && !edgex)) {
            free(edge);
            free(edgex);
            free(nedge);
            rc = TSDB_ERR_NOMEM;
            goto done;
        }
        *j = (agg_job){.db = db, .qs = cur_qstats, .work = work, .nwork = nwork, .t = t, .edge = edge,
                       .edgex = edgex, .nedge = nedge};
        atomic_init(&j->next, 0);
        atomic_init(&j->err, 0);
        for (int k = 0; k < AGG_STRIPES; k++)
            pthread_mutex_init(&j->locks[k], NULL);
        uint64_t rows = 0;
        for (size_t w = 0; w < nwork; w++)
            rows += work[w].g->count;
        sched_run(db->sched, query_threads_for(db, rows, nwork), agg_worker, j);
        for (int k = 0; k < AGG_STRIPES; k++)
            pthread_mutex_destroy(&j->locks[k]);
        rc = atomic_load(&j->err);
        for (size_t w = 0; rc == 0 && w < nwork; w++)
            for (size_t k = 2 * w; k < 2 * w + nedge[w]; k++)
                fold_into(&t, &edge[k], edgex ? &edgex[k] : NULL, 1);
        free(edge);
        free(edgex);
        free(nedge);
    }
    if (rc == 0) {
        size_t nonempty = 0;
        for (size_t k = 0; k < nb; k++)
            nonempty += acc[k].rows != 0;
        size_t row = q->spread ? sizeof(tsdb_spread_bucket) : sizeof(tsdb_bucket);
        uint8_t *res = malloc((nonempty ? nonempty : 1) * row);
        if (!res) {
            rc = TSDB_ERR_NOMEM;
        } else {
            size_t o = 0;
            for (size_t k = 0; k < nb; k++) {
                if (!acc[k].rows)
                    continue;
                tsdb_bucket *r = (tsdb_bucket *)(res + o * row);
                r->start = bk_start(bk, t.b0 + (int64_t)k);
                agg_to_bucket(type, &acc[k], r);
                if (q->spread)
                    agg_spread(type, &acc[k], &accx[k], (tsdb_spread_bucket *)r);
                o++;
            }
            *out = res;
            *nout = o;
        }
    }
done:
    if (cc)
        cc_put(db, cc);
    free(j);
    free(fc.scratch);
    free(fc.scratchx);
    free(acc);
    free(accx);
    free(work);
    free(srcs);
    return rc;
}

/* The most buckets one dense array holds (0: AGG_DENSE_BYTES worth); tests make it small to exercise chunks */
size_t agg_dense_buckets;
/* Empty buckets a chunk spans rather than ending and starting a new one at the next cluster: about what a chunk's
 * own setup costs, so a chunk's states stay near its buckets with rows (plus the time its granules span). */
#define AGG_CHUNK_GAP 4096

/* The buckets of [start, end) (bucket edges, or the query's ends), ascending: bq_window's contract.
 *
 * The working memory follows the buckets that have rows, never the span between the first and the last row: a
 * span up to the dense limit (some tens of MB of states) is one dense array, as fast as it gets; a longer one is
 * computed chunk by chunk. A chunk begins at the next row (empty time costs nothing) and takes whole clusters
 * while they fit in the dense limit and follow each other within AGG_CHUNK_GAP buckets, so its states follow the
 * time its granules span, not the gaps between them, and no granule is decoded twice; only a cluster longer than
 * the limit is cut, at a bucket edge, and its granules there are decoded once per chunk. A chunk's edges are
 * bucket edges, so it computes each of its buckets from the same rows, the same granule statistics and in the
 * same order as one pass would (bucket_windows relies on the same): the results are those of one dense array, bit
 * for bit. TSDB_ERR_TOO_MANY_BUCKETS still counts the span. */
static int bq_window(void *ctx, int64_t start, int64_t end, void **out, size_t *nout)
{
    const bq *q = ctx;
    const snapshot *sn = q->sn;
    size_t wd = type_width(q->type);
    const bucketer *bk = q->bk;
    const item *items = q->items;
    *out = NULL;
    *nout = 0;
    if (!q->ncl)
        return 0;
    size_t c0, c1;
    bq_clusters(q, start, end, &c0, &c1);
    size_t nin = 0;
    int64_t dmin = INT64_MAX, dmax = INT64_MIN;
    uint8_t *seen = calloc(sn->np + 1, 1);
    if (!seen)
        return TSDB_ERR_NOMEM;
    for (size_t k = c0 < c1 ? q->cl[c0].a : 0; c0 < c1 && k < q->cl[c1 - 1].b; k++) {
        const item *it = &items[k];
        if (it->hi < start || it->lo >= end)
            continue;
        nin++;
        dmin = MIN(dmin, MAX(it->lo, start));
        dmax = MAX(dmax, MIN(it->hi, end - 1));
        if (it->g) {
            QS_ADD(granules, 1);
            if (!seen[it->src])
                QS_ADD(parts, 1);
            seen[it->src] = 1;
        } else if (mem_from_memtable(sn, it->src - sn->np)) {
            QS_ADD(memtable_rows, run_cut(&sn->mem[it->src - sn->np], start, end, wd).n);
        }
    }
    free(seen);
    if (nin == 0)
        return 0;
    if (bk->kind == BK_ONE)
        return bq_dense(q, start, end, c0, c1, nin, 0, 1, out, nout);
    int64_t kfirst = bk_key(bk, dmin), klast = bk_key(bk, dmax);
    uint64_t span = (uint64_t)klast - (uint64_t)kfirst + 1;
    if (span > MAX_BUCKETS)
        return TSDB_ERR_TOO_MANY_BUCKETS;
    if (!qbudget_ok(span * (sizeof(agg_state) + (q->spread ? sizeof(agg_dev) + sizeof(tsdb_spread_bucket)
                                                            : sizeof(tsdb_bucket)))))
        return TSDB_ERR_DEFER; /* a multi-series task ahead of the head: run it at its turn */
    size_t dense = agg_dense_buckets ? agg_dense_buckets
                                     : AGG_DENSE_BYTES / (sizeof(agg_state) + (q->spread ? sizeof(agg_dev) : 0));
    if (span <= dense)
        return bq_dense(q, start, end, c0, c1, nin, kfirst, (size_t)span, out, nout);
    size_t row = q->spread ? sizeof(tsdb_spread_bucket) : sizeof(tsdb_bucket);
    uint8_t *res = NULL;
    size_t got = 0;
    int rc = 0;
    for (int64_t lo = start, p; rc == 0 && lo < end && bq_bound((void *)q, lo, end, false, &p);) {
        int64_t kp = bk_key(bk, p); /* the buckets before the next row are empty */
        if (kp > klast)
            break;
        size_t a = bq_first_cluster(q, lo), b;
        int64_t kmax = kp; /* the chunk's last bucket */
        for (size_t c = a; c < q->ncl && q->cl[c].lo < end; c++) {
            int64_t kc = bk_key(bk, MAX(q->cl[c].lo, lo)), ke = bk_key(bk, MIN(q->cl[c].hi, end - 1));
            /* a cluster that starts in a later bucket may begin the next chunk: after a gap, or if it does not fit */
            if (c > a && kc > kmax &&
                ((uint64_t)kc - (uint64_t)kmax > AGG_CHUNK_GAP || (uint64_t)ke - (uint64_t)kp >= dense))
                break;
            kmax = MAX(kmax, ke);
            if ((uint64_t)kmax - (uint64_t)kp >= dense) /* one cluster longer than a chunk: cut it */
                break;
        }
        size_t n = (size_t)MIN((uint64_t)dense, (uint64_t)kmax - (uint64_t)kp + 1);
        int64_t hi = (uint64_t)klast - (uint64_t)kp + 1 > n ? bk_start(bk, kp + (int64_t)n) : end;
        bq_clusters(q, lo, hi, &a, &b);
        void *wr;
        size_t wn;
        rc = bq_dense(q, lo, hi, a, b, nin, kp, n, &wr, &wn);
        if (rc == 0 && wn && !res) { /* the first chunk's array is the result so far */
            res = wr;
            got = wn;
            wr = NULL;
        } else if (rc == 0 && wn) {
            uint8_t *r2 = realloc(res, (got + wn) * row);
            if (r2) {
                memcpy(r2 + got * row, wr, wn * row);
                res = r2;
                got += wn;
            } else {
                rc = TSDB_ERR_NOMEM;
            }
        }
        free(wr);
        lo = hi;
    }
    if (rc) {
        free(res);
        return rc;
    }
    *out = res;
    *nout = got;
    return 0;
}

static int query_buckets(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, bool spread,
                         bool desc, size_t limit, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    int type = eng_series_type(db, series);
    start = MAX(start, eng_cutoff(db, series)); /* expired rows are gone at once, merged away or not */
    if (start >= end || type < 0)
        return 0;
    snapshot sn;
    int rc = snapshot_take(db, series, type_width(type), start, end, &sn);
    if (rc)
        return rc;
    bq q = {.db = db, .sn = &sn, .series = series, .type = type, .bk = bk, .spread = spread};
    rc = bq_items(&q, start, end);
    bk_windows w = {.bk = bk, .desc = desc, .limit = limit, .ctx = &q, .run = bq_window, .bound = bq_bound,
                    .row = spread ? sizeof(tsdb_spread_bucket) : sizeof(tsdb_bucket)};
    if (rc == 0)
        rc = bucket_windows(&w, start, end, out, nout);
    free(q.items);
    free(q.cl);
    snapshot_release(&sn);
    return rc;
}

int eng_query_group(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, bool desc,
                    size_t limit, tsdb_bucket **out, size_t *nout)
{
    return query_buckets(db, series, start, end, bk, false, desc, limit, (void **)out, nout);
}

int eng_query_spread(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, bool desc,
                     size_t limit, tsdb_spread_bucket **out, size_t *nout)
{
    return query_buckets(db, series, start, end, bk, true, desc, limit, (void **)out, nout);
}

/* -------------------------------------------------------------------- raw */

static int cmp_src_first(const void *a, const void *b)
{
    int64_t x = src_first_ts(a), y = src_first_ts(b);
    return (x > y) - (x < y);
}

/* Streams [start, end) of one series from a snapshot, in time order. */
static int raw_range(eng *db, codec_ctx *cc, const snapshot *sn, uint32_t series, int type, int64_t start,
                     int64_t end, slot_fn fn, void *ctx)
{
    size_t w = type_width(type);
    src *cur = calloc(sn->np + sn->nmem + 1, sizeof *cur);
    size_t nc = 0;
    int rc = cur ? 0 : TSDB_ERR_NOMEM;
    (void)db;
    for (size_t i = 0; rc == 0 && i < sn->np; i++) {
        uint32_t gi, gj;
        snap_find(sn, i, series, start, end, &gi, &gj);
        if (gi < gj) {
            src_init_part(&cur[nc++], sn->parts[i], gi, gj, type);
            QS_ADD(parts, 1);
            QS_ADD(granules, gj - gi);
        }
    }
    /* memtable runs were cut to the snapshot's range: cut them to this one */
    mem_run *clipped = calloc(sn->nmem + 1, sizeof *clipped);
    if (!clipped)
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < sn->nmem; i++) {
        const mem_run *r = &sn->mem[i];
        size_t lo = lower_bound_i64(r->ts, r->n, start), hi = lower_bound_i64(r->ts, r->n, end);
        if (lo >= hi)
            continue;
        clipped[i] = (mem_run){r->ts + lo, r->v + lo * w, hi - lo};
        if (mem_from_memtable(sn, i))
            QS_ADD(memtable_rows, hi - lo);
        src_init_mem(&cur[nc++], &clipped[i], mem_prio(sn, i), type);
    }
    if (rc || nc == 0)
        goto out;

    qsort(cur, nc, sizeof *cur, cmp_src_first);
    bool disjoint = true;
    for (size_t i = 1; i < nc; i++)
        if (src_last_ts(&cur[i - 1]) >= src_first_ts(&cur[i]))
            disjoint = false;
    if (disjoint) {
        /* no timestamp can appear twice: stream the sources one after another */
        for (size_t i = 0; i < nc && rc == 0; i++) {
            src *c = &cur[i];
            rc = src_fill(cc, c, start, end);
            while (rc == 0 && c->loaded) {
                if (fn(ctx, c->ts + c->pos, c->v + c->pos * c->w, c->n - c->pos)) {
                    rc = TSDB_ERR_STOPPED;
                    break;
                }
                rc = src_advance(cc, c, c->n - c->pos, start, end);
            }
            /* drained: its buffers go back now, for the next source's malloc to reuse (kept to the end, a query
               over thousands of small parts grew the heap by all of them, and the trim after it faulted them in
               again on the next query) */
            src_free(c);
            c->own = false;
        }
    } else {
        rc = merge_stream(cc, cur, nc, start, end, fn, ctx);
    }
out:
    for (size_t i = 0; i < nc; i++)
        src_free(&cur[i]);
    free(cur);
    free(clipped);
    return rc;
}

/* ascending with a limit: stop once enough rows went out */
typedef struct limit_ctx {
    slot_fn fn;
    void *ctx;
    size_t left;
    bool done;
} limit_ctx;

static int limit_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    limit_ctx *l = arg;
    size_t take = n < l->left ? n : l->left;
    if (take && l->fn(l->ctx, ts, v, take))
        return 1;
    l->left -= take;
    if (l->left == 0) {
        l->done = true;
        return 1;
    }
    return 0;
}

/* descending: a window of rows is collected in time order, then sent back to front */
typedef struct window_buf {
    int64_t *ts;
    uint8_t *v;
    size_t n, cap, w;
} window_buf;

static int window_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    window_buf *w = arg;
    if (w->n + n > w->cap) {
        size_t cap = MAX(w->cap * 2, w->n + n);
        int64_t *t2 = realloc(w->ts, cap * sizeof *t2);
        if (!t2)
            return 1;
        w->ts = t2;
        uint8_t *v2 = realloc(w->v, cap * w->w);
        if (!v2)
            return 1;
        w->v = v2;
        w->cap = cap;
    }
    memcpy(w->ts + w->n, ts, n * sizeof *ts);
    memcpy(w->v + w->n * w->w, v, n * w->w);
    w->n += n;
    return 0;
}

typedef struct span {
    int64_t lo;
    uint64_t rows;
} span;

static int cmp_span_desc(const void *a, const void *b)
{
    int64_t x = ((const span *)a)->lo, y = ((const span *)b)->lo;
    return (x < y) - (x > y);
}

size_t desc_window_rows = 65536; /* tests make it small to exercise many windows */

/* Newest first: walks the range backwards in windows of about
 * desc_window_rows rows, sized from the granule index, all from one snapshot. */
static int raw_desc(eng *db, codec_ctx *cc, const snapshot *sn, uint32_t series, int type, int64_t start,
                    int64_t end, size_t limit, slot_fn fn, void *ctx)
{
    size_t wd = type_width(type);
    size_t ns = 0, cap = 64;
    span *sp = malloc(cap * sizeof *sp);
    int rc = sp ? 0 : TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < sn->np + sn->nmem; i++) {
        uint32_t gi = 0, gj = 1;
        if (i < sn->np)
            snap_find(sn, i, series, start, end, &gi, &gj);
        for (uint32_t k = gi; k < gj; k++) {
            if (ns == cap) {
                span *s2 = realloc(sp, (cap *= 2) * sizeof *sp);
                if (!s2) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                sp = s2;
            }
            if (i < sn->np)
                sp[ns++] = (span){sn->parts[i]->g[k].ts_min, sn->parts[i]->g[k].count};
            else if (sn->mem[i - sn->np].n)
                sp[ns++] = (span){sn->mem[i - sn->np].ts[0], sn->mem[i - sn->np].n};
        }
    }
    if (rc == 0 && ns)
        qsort(sp, ns, sizeof *sp, cmp_span_desc);
    window_buf w = {.w = wd};
    int64_t *rt = NULL;
    uint8_t *rv = NULL;
    size_t rcap = 0, sent = 0, k = 0;
    int64_t hi = end;
    while (rc == 0 && hi > start && (limit == 0 || sent < limit)) {
        /* the window's lower edge: enough granules starting at or after it */
        uint64_t rows = 0;
        int64_t lo = start;
        while (k < ns) {
            if (sp[k].lo >= hi) {
                k++;
                continue;
            }
            rows += sp[k].rows;
            lo = MAX(sp[k].lo, start);
            k++;
            if (rows >= desc_window_rows)
                break;
        }
        if (k >= ns && rows < desc_window_rows)
            lo = start;
        w.n = 0;
        rc = raw_range(db, cc, sn, series, type, lo, hi, window_cb, &w);
        if (rc == TSDB_ERR_STOPPED)
            rc = TSDB_ERR_NOMEM; /* window_cb only stops when it cannot allocate */
        if (rc)
            break;
        if (w.n > rcap) {
            free(rt);
            free(rv);
            rt = malloc(w.n * sizeof *rt);
            rv = malloc(w.n * wd);
            rcap = rt && rv ? w.n : 0;
            if (!rcap) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
        }
        size_t m = w.n;
        if (limit && m > limit - sent)
            m = limit - sent;
        for (size_t i = 0; i < m; i++) {
            rt[i] = w.ts[w.n - 1 - i];
            memcpy(rv + i * wd, w.v + (w.n - 1 - i) * wd, wd);
        }
        if (m && fn(ctx, rt, rv, m)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        sent += m;
        if (lo <= start)
            break;
        hi = lo;
    }
    free(w.ts);
    free(w.v);
    free(rt);
    free(rv);
    free(sp);
    return rc;
}

int eng_query_raw_ex(eng *db, uint32_t series, int64_t start, int64_t end, bool descending, size_t limit,
                     slot_fn fn, void *ctx)
{
    int type = eng_series_type(db, series);
    start = MAX(start, eng_cutoff(db, series)); /* expired rows are gone at once, merged away or not */
    if (start >= end || type < 0)
        return 0;
    snapshot sn;
    int rc = snapshot_take(db, series, type_width(type), start, end, &sn);
    if (rc)
        return rc;
    codec_ctx *cc = cc_get(db);
    if (!cc) {
        snapshot_release(&sn);
        return TSDB_ERR_NOMEM;
    }
    if (descending) {
        rc = raw_desc(db, cc, &sn, series, type, start, end, limit, fn, ctx);
    } else if (limit) {
        limit_ctx l = {fn, ctx, limit, false};
        rc = raw_range(db, cc, &sn, series, type, start, end, limit_cb, &l);
        if (rc == TSDB_ERR_STOPPED && l.done)
            rc = 0;
    } else {
        rc = raw_range(db, cc, &sn, series, type, start, end, fn, ctx);
    }
    cc_put(db, cc);
    snapshot_release(&sn);
    return rc;
}

/* ------------------------------------------------------------------- lttb */

/* Largest-Triangle-Three-Buckets (Steinarsson 2013) over the known, finite
 * points of one series: point 0 and point n - 1 are kept; points 1 .. n - 2
 * fall into nb = threshold - 2 buckets, bucket b holding [lttb_start(b),
 * lttb_start(b + 1)) as the paper bounds them; from each bucket the point
 * with the largest triangle (previously chosen point, the point, the mean of
 * the next bucket) is kept.
 * The bucket after the last one, whose mean the last choice uses, is
 * [lttb_start(nb), n): the last point, or the last two where nb * every
 * rounds below n - 2.
 *
 * The bucket boundaries need n, so a first pass counts the points: from the
 * granule index where a granule lies inside the range, overlaps no other
 * source and its statistics show no NaN or infinity (its float sum is finite),
 * by streaming the rest. Choosing in bucket b needs the mean of bucket b + 1,
 * known only once that bucket is read. Two exact ways, both on one snapshot:
 *  - buffered: one pass that keeps the points of two buckets (16 bytes each);
 *  - two passes: the means of every bucket first, then the choice while
 *    streaming again, with O(threshold) memory.
 * Buffered is used while two buckets fit in lttb_buffer_bytes; it decodes the
 * range once instead of twice. Both add the coordinates in time order, so they
 * choose the same points. */
size_t lttb_buffer_bytes = 32u << 20;

typedef struct lttb_buf {
    int64_t *ts;
    uint64_t *v; /* slot bits, zero-extended */
    size_t n;
} lttb_buf;

typedef struct lttb_run {
    int type;
    size_t w;
    uint64_t n, nb; /* points, buckets (threshold - 2) */
    double every;   /* (n - 2) / nb, the paper's bucket width */
    bool buffered;
    int pass;       /* two passes: 1 sums the buckets, 2 chooses */
    uint64_t i;     /* index of the next point */
    uint64_t b;     /* bucket of the points being read; nb is the last point */
    uint64_t bend;  /* first point of bucket b + 1 */
    int64_t t0;     /* x is milliseconds since the first point */
    double ax, ay;  /* the point chosen last */
    double sx, sy;  /* sums of bucket b */
    uint64_t cnt;
    lttb_buf cur, nxt; /* buffered: bucket b - 1 (to choose from) and bucket b */
    double *mx, *my;   /* two passes: mean of bucket b at [b - 1], b = 1 .. nb */
    double best;       /* two passes: the choice so far in bucket b */
    int64_t bts;
    uint64_t bv;
    double bx, by;
    int64_t lts;       /* the last point read */
    uint64_t lv;
    int64_t ots[256];  /* chosen points not yet sent */
    uint64_t ov[256];
    uint8_t oslots[256 * 8];
    size_t on;
    slot_fn fn;
    void *ctx;
    int err;
} lttb_run;

/* A point's value as a double; false for nulls, NaN and infinities, which are
 * not points (they have no place on a chart and would poison the means). */
static inline bool lttb_value(int type, uint64_t x, double *y)
{
    switch (type) {
    case TSDB_FLOAT32: {
        float f;
        uint32_t u = (uint32_t)x;
        memcpy(&f, &u, 4);
        *y = f;
        return u != NULL_F32 && isfinite(f);
    }
    case TSDB_FLOAT64: {
        double d;
        memcpy(&d, &x, 8);
        *y = d;
        return x != NULL_F64 && isfinite(d);
    }
    case TSDB_INT64: *y = (double)(int64_t)x; return x != (uint64_t)NULL_I64;
    default: *y = (double)x; return x != NULL_BOOL;
    }
}

/* first point of bucket b (b <= nb): floor(b * every) + 1 in doubles, as in the
 * paper's code, ClickHouse and the TimescaleDB toolkit, so that the same
 * points come out. Exact integer division would differ where b * every rounds
 * below an integer (n = 17, threshold 13: bucket 11 starts at 15, not 16). */
static inline uint64_t lttb_start(const lttb_run *r, uint64_t b)
{
    return (uint64_t)floor((double)b * r->every) + 1;
}

static inline double lttb_x(const lttb_run *r, int64_t ts) { return (double)((uint64_t)ts - (uint64_t)r->t0); }

/* the triangle's area, computed as in the paper */
static inline double lttb_area(double ax, double ay, double cx, double cy, double x, double y)
{
    return fabs((ax - cx) * (y - ay) - (ax - x) * (cy - ay)) * 0.5;
}

static int lttb_flush_out(lttb_run *r)
{
    if (!r->on)
        return 0;
    for (size_t k = 0; k < r->on; k++)
        slot_put(r->oslots, r->w, k, r->ov[k]);
    size_t n = r->on;
    r->on = 0;
    if (r->fn(r->ctx, r->ots, r->oslots, n)) {
        r->err = TSDB_ERR_STOPPED;
        return 1;
    }
    return 0;
}

static int lttb_emit(lttb_run *r, int64_t ts, uint64_t v)
{
    r->ots[r->on] = ts;
    r->ov[r->on++] = v;
    return r->on == 256 ? lttb_flush_out(r) : 0;
}

/* buffered: bucket b is complete; choose from bucket b - 1 with its mean */
static int lttb_choose_buffered(lttb_run *r)
{
    if (r->b >= 1) {
        double cx = r->sx / (double)r->cnt, cy = r->sy / (double)r->cnt;
        size_t k = lttb_argmax(r->type, r->cur.ts, r->cur.v, r->cur.n, r->t0, r->ax, r->ay, cx, cy);
        lttb_value(r->type, r->cur.v[k], &r->ay);
        r->ax = lttb_x(r, r->cur.ts[k]);
        if (lttb_emit(r, r->cur.ts[k], r->cur.v[k]))
            return 1;
    }
    lttb_buf t = r->cur;
    r->cur = r->nxt;
    r->nxt = t;
    r->nxt.n = 0;
    return 0;
}

/* bucket b is complete */
static int lttb_finish_bucket(lttb_run *r)
{
    if (r->buffered) {
        if (lttb_choose_buffered(r))
            return 1;
    } else if (r->pass == 1) {
        if (r->b >= 1) {
            r->mx[r->b - 1] = r->sx / (double)r->cnt;
            r->my[r->b - 1] = r->sy / (double)r->cnt;
        }
    } else if (r->b < r->nb) {
        r->ax = r->bx;
        r->ay = r->by;
        if (lttb_emit(r, r->bts, r->bv))
            return 1;
    }
    r->sx = r->sy = 0;
    r->cnt = 0;
    r->best = -1;
    return 0;
}

static int lttb_point(lttb_run *r, int64_t ts, uint64_t v, double y)
{
    uint64_t i = r->i++;
    r->lts = ts;
    r->lv = v;
    if (i >= r->n) {
        r->err = TSDB_ERR_CORRUPT; /* the count disagrees with the rows */
        return 1;
    }
    if (i == 0) {
        r->t0 = ts;
        r->ax = 0;
        r->ay = y;
        r->b = 0;
        r->bend = lttb_start(r, 1);
        r->best = -1;
        return r->pass == 1 ? 0 : lttb_emit(r, ts, v);
    }
    if (i == r->bend) {
        if (lttb_finish_bucket(r))
            return 1;
        r->b++;
        r->bend = r->b < r->nb ? lttb_start(r, r->b + 1) : r->n;
    }
    double x = lttb_x(r, ts);
    if (r->buffered) {
        r->nxt.ts[r->nxt.n] = ts;
        r->nxt.v[r->nxt.n++] = v;
    } else if (r->pass == 2 && r->b < r->nb) {
        double a = lttb_area(r->ax, r->ay, r->mx[r->b], r->my[r->b], x, y);
        if (a > r->best || r->cnt == 0) { /* the first of equal areas wins */
            r->best = a > r->best ? a : r->best;
            r->bts = ts;
            r->bv = v;
            r->bx = x;
            r->by = y;
        }
    }
    r->sx += x;
    r->sy += y;
    r->cnt++;
    return 0;
}

/* raw rows to lttb_point, without nulls, NaN and infinities */
static int lttb_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    lttb_run *r = arg;
    for (size_t k = 0; k < n; k++) {
        uint64_t x = slot_get(v, r->w, k);
        double y;
        if (lttb_value(r->type, x, &y) && lttb_point(r, ts[k], x, y))
            return 1;
    }
    return 0;
}

/* n <= threshold: every point, in runs straight from the decode buffers */
static int lttb_all_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    lttb_run *r = arg;
    size_t k = 0;
    while (k < n) {
        double y;
        while (k < n && !lttb_value(r->type, slot_get(v, r->w, k), &y))
            k++;
        size_t a = k;
        while (k < n && lttb_value(r->type, slot_get(v, r->w, k), &y))
            k++;
        if (k > a) {
            r->i += k - a;
            if (r->fn(r->ctx, ts + a, (const uint8_t *)v + a * r->w, k - a)) {
                r->err = TSDB_ERR_STOPPED;
                return 1;
            }
        }
    }
    return 0;
}

static int lttb_count_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    lttb_run *r = arg;
    for (size_t k = 0; k < n; k++) {
        double y;
        r->n += lttb_value(r->type, slot_get(v, r->w, k), &y);
    }
    return 0;
}

/* The known, finite points in [start, end), as one read of the snapshot would
 * see them: granules alone in their time span count from the index. */
static int lttb_count(eng *db, codec_ctx *cc, const snapshot *sn, uint32_t series, lttb_run *r, int64_t start,
                      int64_t end)
{
    size_t cap = sn->nmem + 16, nitem = 0;
    item *items = malloc(cap * sizeof *items);
    int rc = items ? 0 : TSDB_ERR_NOMEM;
    for (uint32_t s = 0; rc == 0 && s < sn->np; s++) {
        uint32_t gi, gj;
        snap_find(sn, s, series, start, end, &gi, &gj); /* never granules a tombstone hides: those are in mem */
        for (uint32_t k = gi; k < gj; k++) {
            if (nitem == cap) {
                item *i2 = realloc(items, (cap *= 2) * sizeof *items);
                if (!i2) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                items = i2;
            }
            const granule *g = &sn->parts[s]->g[k];
            items[nitem++] = (item){g->ts_min, g->ts_max, s, g};
        }
    }
    for (size_t m = 0; rc == 0 && m < sn->nmem; m++) {
        const mem_run *mr = &sn->mem[m];
        size_t lo = lower_bound_i64(mr->ts, mr->n, start), hi = lower_bound_i64(mr->ts, mr->n, end);
        if (lo >= hi)
            continue;
        if (nitem == cap) {
            item *i2 = realloc(items, (cap *= 2) * sizeof *items);
            if (!i2) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            items = i2;
        }
        items[nitem++] = (item){mr->ts[lo], mr->ts[hi - 1], (uint32_t)(sn->np + m), NULL};
    }
    if (rc == 0)
        qsort(items, nitem, sizeof *items, cmp_item);
    for (size_t a = 0; a < nitem && rc == 0;) {
        size_t b = a + 1;
        int64_t hi = items[a].hi;
        bool multi = false;
        while (b < nitem && items[b].lo <= hi) {
            multi |= items[b].src != items[a].src;
            hi = MAX(hi, items[b].hi);
            b++;
        }
        for (size_t k = a; k < b && rc == 0; k++) {
            const granule *g = items[k].g;
            if (!multi && g && g->ts_min >= start && g->ts_max < end &&
                (type_is_int(r->type) || isfinite(g->fsum))) {
                r->n += g->count - g->nulls; /* a NaN or an infinity makes the sum NaN or infinite */
                continue;
            }
            /* a cluster of several sources goes through the deduplicating merge at once */
            int64_t lo = multi ? items[a].lo : items[k].lo, h = multi ? hi : items[k].hi;
            lo = MAX(lo, start);
            h = MIN(h, end - 1);
            if (lo <= h)
                rc = raw_range(db, cc, sn, series, r->type, lo, h + 1, lttb_count_cb, r);
            if (multi)
                break;
        }
        a = b;
    }
    free(items);
    return rc;
}

/* Newest first: the choice runs oldest first (each point depends on the one chosen before it), so the last `cap`
 * chosen points are kept in a ring, grown as they come (never more than were chosen), and sent backwards. */
typedef struct lttb_ring {
    size_t w, cap, alloc, total;
    int err;
    int64_t *ts;
    uint8_t *v;
} lttb_ring;

static int lttb_ring_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    lttb_ring *g = arg;
    for (size_t k = 0; k < n; k++, g->total++) {
        if (g->total < g->cap && g->total == g->alloc) {
            size_t a = MIN(g->cap, MAX(g->alloc * 2, (size_t)1024));
            int64_t *t2 = realloc(g->ts, a * sizeof *t2);
            if (t2)
                g->ts = t2;
            uint8_t *v2 = t2 ? realloc(g->v, a * g->w) : NULL;
            if (!v2) {
                g->err = TSDB_ERR_NOMEM;
                return 1;
            }
            g->v = v2;
            g->alloc = a;
        }
        size_t i = g->total % g->cap;
        g->ts[i] = ts[k];
        memcpy(g->v + i * g->w, (const uint8_t *)v + k * g->w, g->w);
    }
    return 0;
}

static void reverse_slots(int64_t *ts, uint8_t *v, size_t w, size_t n)
{
    for (size_t i = 0, j = n; i + 1 < j; i++, j--) {
        int64_t x = ts[i];
        ts[i] = ts[j - 1];
        ts[j - 1] = x;
        uint8_t tmp[8];
        memcpy(tmp, v + i * w, w);
        memcpy(v + i * w, v + (j - 1) * w, w);
        memcpy(v + (j - 1) * w, tmp, w);
    }
}

/* the ring newest first: [0, head) holds the newest, [head, kept) the older ones */
static int lttb_ring_send(lttb_ring *g, slot_fn fn, void *ctx)
{
    size_t kept = MIN(g->total, g->cap), head = g->total > g->cap ? g->total % g->cap : kept;
    reverse_slots(g->ts, g->v, g->w, head);
    reverse_slots(g->ts + head, g->v + head * g->w, g->w, kept - head);
    if (head && fn(ctx, g->ts, g->v, head))
        return TSDB_ERR_STOPPED;
    if (kept > head && fn(ctx, g->ts + head, g->v + head * g->w, kept - head))
        return TSDB_ERR_STOPPED;
    return 0;
}

int eng_query_lttb(eng *db, uint32_t series, int64_t start, int64_t end, size_t threshold, bool desc, size_t keep,
                   slot_fn fn, void *ctx)
{
    if (threshold < 3)
        return TSDB_ERR_INVALID;
    int type = eng_series_type(db, series);
    if (type == TSDB_STATE)
        return TSDB_ERR_INVALID;
    start = MAX(start, eng_cutoff(db, series)); /* expired rows are gone at once, merged away or not */
    if (start >= end || type < 0)
        return 0;
    snapshot sn;
    int rc = snapshot_take(db, series, type_width(type), start, end, &sn);
    if (rc)
        return rc;
    codec_ctx *cc = cc_get(db);
    if (!cc) {
        snapshot_release(&sn);
        return TSDB_ERR_NOMEM;
    }
    lttb_ring ring = {0};
    lttb_run *r = calloc(1, sizeof *r);
    if (!r) {
        rc = TSDB_ERR_NOMEM;
        goto out;
    }
    r->type = type;
    r->w = type_width(type);
    r->fn = fn;
    r->ctx = ctx;
    /* statistics describe one read of the range: the count and the means are extra passes */
    qstats *mine = cur_qstats;
    cur_qstats = NULL;
    rc = lttb_count(db, cc, &sn, series, r, start, end);
    cur_qstats = mine;
    if (rc || r->n == 0)
        goto out;
    if (r->n <= threshold) {
        /* every point: newest first is a backwards read, which stops early with the caller */
        rc = desc ? raw_desc(db, cc, &sn, series, type, start, end, 0, lttb_all_cb, r)
                  : raw_range(db, cc, &sn, series, type, start, end, lttb_all_cb, r);
    } else {
        if (desc) {
            ring = (lttb_ring){.w = r->w, .cap = keep ? MIN(keep, threshold) : threshold};
            r->fn = lttb_ring_cb;
            r->ctx = &ring;
        }
        r->nb = threshold - 2;
        r->every = (double)(r->n - 2) / (double)r->nb;
        /* the largest bucket: floor(e) + 1 points, one more for rounding; bucket nb (the
         * last mean) holds n - lttb_start(nb) <= 2 */
        uint64_t per = (r->n - 2) / r->nb + 2;
        r->buffered = per <= lttb_buffer_bytes / 32;
        if (r->buffered && !qbudget_ok(per * 32)) {
            rc = TSDB_ERR_DEFER; /* a multi-series task ahead of the head: run it at its turn */
            goto out;
        }
        if (r->buffered) {
            r->cur.ts = malloc(per * sizeof(int64_t));
            r->cur.v = malloc(per * sizeof(uint64_t));
            r->nxt.ts = malloc(per * sizeof(int64_t));
            r->nxt.v = malloc(per * sizeof(uint64_t));
            if (!r->cur.ts || !r->cur.v || !r->nxt.ts || !r->nxt.v) {
                rc = TSDB_ERR_NOMEM;
                goto out;
            }
        } else {
            r->mx = malloc(r->nb * sizeof *r->mx);
            r->my = malloc(r->nb * sizeof *r->my);
            if (!r->mx || !r->my) {
                rc = TSDB_ERR_NOMEM;
                goto out;
            }
            r->pass = 1;
            cur_qstats = NULL;
            rc = raw_range(db, cc, &sn, series, type, start, end, lttb_cb, r);
            cur_qstats = mine;
            if (rc == 0 && r->i != r->n)
                rc = TSDB_ERR_CORRUPT; /* no means for the last buckets: send nothing */
            if (rc == 0)
                lttb_finish_bucket(r); /* the mean of the last bucket */
            r->pass = 2;
            r->i = 0;
            r->sx = r->sy = 0;
            r->cnt = 0;
        }
        if (rc == 0)
            rc = raw_range(db, cc, &sn, series, type, start, end, lttb_cb, r);
        if (rc == 0 && r->i == r->n && !lttb_finish_bucket(r) && !lttb_emit(r, r->lts, r->lv))
            lttb_flush_out(r);
    }
    if (rc == TSDB_ERR_STOPPED || (rc == 0 && r->err))
        rc = r->err ? r->err : TSDB_ERR_STOPPED;
    if (rc == 0 && r->i != r->n)
        rc = TSDB_ERR_CORRUPT; /* the granule index promised more points than the parts hold */
    if (rc == TSDB_ERR_STOPPED && ring.err)
        rc = ring.err;
    if (rc == 0 && r->fn == lttb_ring_cb)
        rc = lttb_ring_send(&ring, fn, ctx);
out:
    free(ring.ts);
    free(ring.v);
    if (r) {
        free(r->cur.ts);
        free(r->cur.v);
        free(r->nxt.ts);
        free(r->nxt.v);
        free(r->mx);
        free(r->my);
        free(r);
    }
    cc_put(db, cc);
    snapshot_release(&sn);
    return rc;
}

void tsdb_free(void *p) { free(p); }

size_t tsdb_struct_size(int kind)
{
    switch (kind) {
    case TSDB_STRUCT_OPTIONS: return sizeof(tsdb_options);
    case TSDB_STRUCT_VALUES: return sizeof(tsdb_values);
    case TSDB_STRUCT_BUCKET: return sizeof(tsdb_bucket);
    case TSDB_STRUCT_POINTS: return sizeof(tsdb_points);
    case TSDB_STRUCT_GROUP: return sizeof(tsdb_group);
    case TSDB_STRUCT_QUERY_OPTS: return sizeof(tsdb_query_opts);
    case TSDB_STRUCT_QUERY_STATS: return sizeof(tsdb_query_stats);
    case TSDB_STRUCT_STATS: return sizeof(tsdb_stats);
    case TSDB_STRUCT_PART_INFO: return sizeof(tsdb_part_info);
    case TSDB_STRUCT_SPREAD_BUCKET: return sizeof(tsdb_spread_bucket);
    case TSDB_STRUCT_TWA_OPTS: return sizeof(tsdb_twa_opts);
    case TSDB_STRUCT_TWA_BUCKET: return sizeof(tsdb_twa_bucket);
    case TSDB_STRUCT_MULTI_QUERY: return sizeof(tsdb_multi_query);
    case TSDB_STRUCT_SERIES_RESULT: return sizeof(tsdb_series_result);
    case TSDB_STRUCT_MULTI_OPTS: return sizeof(tsdb_multi_opts);
    default: return 0;
    }
}

void tsdb_points_free(tsdb_points *p)
{
    free(p->ts);
    free(p->values);
    memset(p, 0, sizeof *p);
}
