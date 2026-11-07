/*
 * The latest table (docs/design-latest.md, step 2): per volume, for every series, the newest non-null row in the
 * parts (or a bound on it), so that "what is the newest value of these series?" is a few memory reads instead of a
 * search. latest.c is the slow path (latest_at) that defines the answer; this file keeps a table that must give
 * exactly the same one:
 *
 *  - the entries (16 bytes per series, in pages of 65,536) are read and written only under latest_rw, and every
 *    write section bumps lgen. Publishers (flush, large insert, replay, delete, replace) fold the part writer's
 *    per-series (R, N) lists into the entries in the same section that lists their parts (lt_begin, lt_end);
 *  - the memtables hold the newer rows; mt_add keeps top_nn and a few flags per series (internal.h, mt_series), so a
 *    lookup finds a memtable's newest non-null row in O(1) and falls back to the slow path where it cannot decide
 *    (a null that overwrote it, a row not synced yet, a possible null over the table's row);
 *  - an entry can be unknown (a bound): after a delete or a null over the newest row, a limit lengthened, or an open
 *    that could not read the value exactly. A lookup of one is answered by a search and its id goes to the warmer,
 *    the only thread that fills entries, which stores a batch only if no write section ran since its cut (lgen).
 *    Lookups never write the table;
 *  - the table is rebuilt at open from the granule index of every part (latest_build), by blocks of ids.
 */
#include "internal.h"

#include <stdlib.h>
#include <string.h>

enum { LE_EMPTY = 0, LE_KNOWN = 1, LE_UNKNOWN = 2 };
typedef struct lt_ent {
    int st;
    int64_t ts; /* a row's timestamp, or the bound */
    uint64_t x; /* the slot, with LE_KNOWN */
} lt_ent;

#define LK_VALUE_ 0
#define LK_NONE_ 2

static atomic_size_t lt_used; /* bytes of pages of every table of the process (the budget, tsdb_options.latest_cache) */

static inline uint64_t lt_enc(int64_t ts) { return (uint64_t)ts ^ (uint64_t)INT64_MAX; }
static inline int64_t lt_dec(uint64_t t) { return (int64_t)(t ^ (uint64_t)INT64_MAX); }

/* ---- the fold list the part writer makes ---- */

int lf_granule(lfold **lf, size_t *n, size_t *cap, uint32_t series, int type, const int64_t *ts, const void *v,
               size_t cnt, uint32_t nulls)
{
    if (!cnt)
        return 0;
    lfold *f;
    if (*n && (*lf)[*n - 1].series == series) {
        f = &(*lf)[*n - 1];
    } else {
        if (*n == *cap) {
            size_t c = *cap ? 2 * *cap : 64;
            lfold *x = realloc(*lf, c * sizeof *x);
            if (!x)
                return TSDB_ERR_NOMEM;
            *lf = x;
            *cap = c;
        }
        f = &(*lf)[(*n)++];
        memset(f, 0, sizeof *f);
        f->series = series;
    }
    size_t w = type_width(type);
    uint64_t null = type_null(type);
    if (nulls == 0) {
        f->flags |= LF_R;
        f->r_ts = ts[cnt - 1];
        f->r_slot = slot_get(v, w, cnt - 1);
        return 0;
    }
    size_t last_nn = cnt, first_null = cnt, last_null = cnt; /* cnt: none */
    for (size_t i = cnt; i-- > 0;) {
        bool isnull = slot_get(v, w, i) == null;
        if (isnull && last_null == cnt)
            last_null = i;
        if (!isnull && last_nn == cnt)
            last_nn = i;
        if (last_null != cnt && last_nn != cnt)
            break;
    }
    for (size_t i = 0; i < cnt; i++)
        if (slot_get(v, w, i) == null) {
            first_null = i;
            break;
        }
    if (last_nn != cnt) {
        f->flags |= LF_R;
        f->r_ts = ts[last_nn];
        f->r_slot = slot_get(v, w, last_nn);
    }
    if (first_null != cnt) { /* a null exists (the statistics said so) */
        if (!(f->flags & LF_N)) {
            f->flags |= LF_N;
            f->n_lo = ts[first_null];
        }
        f->n_hi = ts[last_null];
    }
    return 0;
}

/* ---- pages and entries (the caller holds latest_rw; pages are made only for writing) ---- */

/* Makes pages up to and including page pi, in id order, within the budget. False: capped (a page was refused). */
static bool lt_make_pages(eng *db, ltab *t, uint32_t pi)
{
    if (pi < t->npg)
        return true;
    if (t->capped || pi >= LT_MAXPAGES)
        return false;
    while (t->npg <= pi) {
        size_t cap = db->opt.latest_cache;
        size_t used = atomic_fetch_add(&lt_used, LT_PAGE_BYTES);
        lt_page *p = used + LT_PAGE_BYTES <= cap ? calloc(1, LT_PAGE_BYTES) : NULL;
        if (!p) {
            atomic_fetch_sub(&lt_used, LT_PAGE_BYTES);
            t->capped = true;
            return false;
        }
        t->pg[t->npg++] = p;
        t->bytes += LT_PAGE_BYTES;
    }
    return true;
}

/* The entry of id: false when the id has none (the budget capped the volume below it). A page not made yet is empty:
 * no listing before held a row of its ids (L5). */
static bool lt_load(const ltab *t, uint32_t id, lt_ent *e)
{
    uint32_t pi = id >> LT_PAGE_BITS, i = id & (LT_PAGE - 1);
    if (pi >= t->npg) {
        if (t->capped)
            return false;
        *e = (lt_ent){LE_EMPTY, INT64_MAX, 0};
        return true;
    }
    const lt_page *p = t->pg[pi];
    uint64_t w = p->e[2 * i];
    if ((p->unk[i >> 6] >> (i & 63)) & 1)
        *e = (lt_ent){LE_UNKNOWN, lt_dec(w), 0};
    else if (w == 0)
        *e = (lt_ent){LE_EMPTY, INT64_MAX, 0};
    else
        *e = (lt_ent){LE_KNOWN, lt_dec(w), p->e[2 * i + 1]};
    return true;
}

/* Stores the entry of id; its page must exist. */
static void lt_store(ltab *t, uint32_t id, const lt_ent *e)
{
    lt_page *p = t->pg[id >> LT_PAGE_BITS];
    uint32_t i = id & (LT_PAGE - 1);
    uint64_t bit = 1ull << (i & 63);
    switch (e->st) {
    case LE_EMPTY:
        p->e[2 * i] = p->e[2 * i + 1] = 0;
        p->unk[i >> 6] &= ~bit;
        break;
    case LE_KNOWN:
        p->e[2 * i] = lt_enc(e->ts);
        p->e[2 * i + 1] = e->x;
        p->unk[i >> 6] &= ~bit;
        break;
    default:
        p->e[2 * i] = lt_enc(e->ts);
        p->e[2 * i + 1] = 0;
        p->unk[i >> 6] |= bit;
    }
}

/* ---- the hint set ---- */

static void hints_add(lt_sec *h, uint32_t id)
{
    if (h->n == h->cap) {
        size_t c = h->cap ? 2 * h->cap : 64;
        uint32_t *x = realloc(h->id, c * sizeof *x);
        if (!x) {
            h->oom = true; /* the warmer rescans the bitmap instead */
            return;
        }
        h->id = x;
        h->cap = c;
    }
    h->id[h->n++] = id;
}

/* Pushes ids to the warmer. A full set sets `rescan` instead (the warmer walks the unknown bitmap). */
static void hints_push(eng *db, const uint32_t *id, size_t n, bool rescan)
{
    ltab *t = db->lt;
    if (!t || (!n && !rescan))
        return;
    pthread_mutex_lock(&t->hmu);
    for (size_t i = 0; i < n; i++) {
        if (t->nhint == LT_HINTS) {
            t->rescan = true;
            break;
        }
        t->hint[t->nhint++] = id[i];
    }
    if (rescan)
        t->rescan = true;
    pthread_cond_signal(&t->hcv);
    pthread_mutex_unlock(&t->hmu);
}

/* ---- folds (3, 3.4) ---- */

/* The entry of f->series after a publish that brought f (design-latest.md 3). A known entry below the drop floor
 * reads as unknown (ts) (L20). Stores only a change; ids made unknown go to h. */
static void fold_one(eng *db, ltab *t, const lfold *f, lt_sec *h)
{
    uint32_t id = f->series;
    lt_ent e;
    if (!lt_load(t, id, &e)) /* capped: no entry */
        return;
    bool floor = e.st == LE_KNOWN && e.ts < t->drop_floor;
    bool hasr = f->flags & LF_R;
    lt_ent n = e;
    switch (e.st) {
    case LE_EMPTY:
        if (hasr)
            n = (lt_ent){LE_KNOWN, f->r_ts, f->r_slot};
        break;
    case LE_KNOWN:
        if (hasr && f->r_ts >= e.ts)
            n = (lt_ent){LE_KNOWN, f->r_ts, f->r_slot};
        else if (!floor && (f->flags & LF_N) && e.ts >= f->n_lo && e.ts <= f->n_hi) {
            n = (lt_ent){LE_UNKNOWN, e.ts, 0}; /* the null may have overwritten the row (never known at an older one) */
            hints_add(h, id);
        }
        break;
    default:
        if (hasr && f->r_ts >= e.ts)
            n = (lt_ent){LE_KNOWN, f->r_ts, f->r_slot};
    }
    if (n.st == e.st && n.ts == e.ts && n.x == e.x)
        return;
    if (!lt_make_pages(db, t, id >> LT_PAGE_BITS)) /* capped (only an empty entry has no page yet) */
        return;
    lt_store(t, id, &n);
}

/* A tombstone of series s hiding [a, b) in the partition of the part being published: every listed row of s there
 * with a lower block is hidden. A known newest row inside it makes the entry unknown (a - 1): the delete covered
 * [start, end) with a at or above start, so every live non-null row is below a (3.4). */
static void fold_tomb(eng *db, ltab *t, const tomb *tb, lt_sec *h)
{
    (void)db;
    uint32_t id = tb->series;
    lt_ent e;
    if (!lt_load(t, id, &e) || e.st != LE_KNOWN || e.ts < t->drop_floor || e.ts < tb->start || e.ts >= tb->end)
        return;
    lt_ent n;
    if (tb->start == INT64_MIN) {
        n = (lt_ent){LE_EMPTY, INT64_MAX, 0};
    } else {
        n = (lt_ent){LE_UNKNOWN, tb->start - 1, 0};
        hints_add(h, id);
    }
    lt_store(t, id, &n);
}

void lt_begin(eng *db, lt_sec *sec)
{
    memset(sec, 0, sizeof *sec);
    ltab *t = db->lt;
    if (t)
        pthread_rwlock_wrlock(&t->rw);
}

/* Folds the pending parts (tombstones, then rows, part by part in block order) and frees their fold lists. Before
 * the parts are listed: once listed, a merge may free them. */
void lt_fold(eng *db, lt_sec *sec, part **ps, size_t n)
{
    ltab *t = db->lt;
    for (size_t i = 0; t && i < n; i++) {
        part *p = ps[i];
        for (uint32_t k = 0; k < p->nt; k++)
            fold_tomb(db, t, &p->t[k], sec);
        for (size_t k = 0; k < p->nlf; k++)
            fold_one(db, t, &p->lf[k], sec);
    }
    for (size_t i = 0; i < n; i++) { /* consumed, also when there is no table */
        free(ps[i]->lf);
        ps[i]->lf = NULL;
        ps[i]->nlf = 0;
    }
}

void lt_end(eng *db, lt_sec *sec)
{
    ltab *t = db->lt;
    if (!t)
        return;
    t->lgen++;
    pthread_rwlock_unlock(&t->rw);
    hints_push(db, sec->id, sec->n, sec->oom);
    free(sec->id);
    memset(sec, 0, sizeof *sec);
}

/* tsdb_set_retention lowered the cutoff of `series` (from oldc): a merge may already have dropped rows below oldc
 * that the new limit keeps, so a known entry below oldc no longer names a row that exists (3.5). In the write
 * section of the limit. */
void lt_retention_lowered(eng *db, lt_sec *sec, uint32_t series, int64_t oldc)
{
    ltab *t = db->lt;
    lt_ent e;
    if (!t || !lt_load(t, series, &e) || e.st != LE_KNOWN || e.ts >= oldc)
        return;
    lt_ent n = {LE_UNKNOWN, e.ts, 0};
    lt_store(t, series, &n);
    hints_add(sec, series);
}

/* tsdb_drop_before(ts) retired partitions: a known entry below ts reads as unknown from now on (L20), in the
 * section that retired them. The warmer walks them (3.8). */
void lt_drop_floor(eng *db, int64_t ts)
{
    ltab *t = db->lt;
    if (!t || ts <= t->drop_floor)
        return;
    t->drop_floor = ts;
    pthread_mutex_lock(&t->hmu);
    t->walk_floor = true;
    pthread_cond_signal(&t->hcv);
    pthread_mutex_unlock(&t->hmu);
}

/* ---- open, close ---- */

int latest_open(eng *db)
{
    db->lt = NULL;
    if (db->opt.latest_cache == 0 || db->opt.read_only || db->opt.follow)
        return 0;
    ltab *t = calloc(1, sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    t->pg = calloc(LT_MAXPAGES, sizeof *t->pg);
    t->hint = malloc(LT_HINTS * sizeof *t->hint);
    pthread_rwlockattr_t at;
    pthread_rwlockattr_init(&at);
    pthread_rwlockattr_setkind_np(&at, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&t->rw, &at);
    pthread_rwlockattr_destroy(&at);
    pthread_mutex_init(&t->pmu, NULL);
    pthread_cond_init(&t->pcv, NULL);
    pthread_mutex_init(&t->hmu, NULL);
    pthread_cond_init(&t->hcv, NULL);
    t->drop_floor = INT64_MIN;
    if (!t->pg || !t->hint) {
        free(t->pg);
        free(t->hint);
        free(t);
        return TSDB_ERR_NOMEM;
    }
    db->lt = t;
    return 0;
}

void latest_close(eng *db)
{
    ltab *t = db->lt;
    if (!t)
        return;
    latest_stop(db);
    db->lt = NULL;
    for (uint32_t i = 0; i < t->npg; i++)
        free(t->pg[i]);
    atomic_fetch_sub(&lt_used, t->bytes);
    free(t->pg);
    free(t->hint);
    pthread_rwlock_destroy(&t->rw);
    pthread_mutex_destroy(&t->pmu);
    pthread_cond_destroy(&t->pcv);
    pthread_mutex_destroy(&t->hmu);
    pthread_cond_destroy(&t->hcv);
    free(t);
}

/* ---- the build at open (3.9) ---- */

#define LB_BLOCK 32768u

typedef struct lb_job {
    eng *db;
    uint32_t nblocks, maxid;
    atomic_uint next;
    atomic_int rc;
} lb_job;

/* The value slot of the last row of granule g of p, from the index alone; false where it is not exact (L12). */
static bool slot_of_last(int type, double d, uint64_t *slot)
{
    switch (type) {
    case TSDB_FLOAT32: {
        float f = (float)d;
        if (f != f || (double)f != d)
            return false;
        uint32_t u;
        memcpy(&u, &f, 4);
        *slot = u;
        return true;
    }
    case TSDB_FLOAT64: {
        if (d != d)
            return false;
        memcpy(slot, &d, 8);
        return true;
    }
    case TSDB_INT64: {
        if (!(d > -9007199254740992.0 && d < 9007199254740992.0))
            return false;
        *slot = (uint64_t)(int64_t)d;
        return true;
    }
    case TSDB_BOOL:
        if (d != 0 && d != 1)
            return false;
        *slot = d != 0;
        return true;
    default:
        return false;
    }
}

/* One block of ids [b0, b1): every part's runs of those ids, in db->parts order (partition, block). The entries
 * hold the walk's state: a "noted" entry is (ts_max, part, granule) of the newest run so far, an unknown one a
 * bound; the final pass reads the noted granules' last value. */
static int build_block(eng *db, ltab *t, uint32_t b0, uint32_t b1)
{
    uint64_t noted[LB_BLOCK / 64];
    memset(noted, 0, sizeof noted);
    lt_page *pg = t->pg[b0 >> LT_PAGE_BITS]; /* LB_BLOCK divides LT_PAGE: one page per block */
    uint32_t base = b0 & (LT_PAGE - 1);
    for (size_t pi = 0; pi < db->nparts; pi++) {
        const part *p = db->parts[pi];
        const gix *ix = &p->ix;
        uint32_t lo = 0, hi = ix->nruns;
        while (lo < hi) { /* the first run with series >= b0 */
            uint32_t mid = lo + (hi - lo) / 2;
            if (ix->rser[mid] < b0)
                lo = mid + 1;
            else
                hi = mid;
        }
        /* the tombstones of p hide the rows of lower blocks of its partition (also where p has no rows of the series):
         * a noted row of an earlier part of this partition inside one becomes unknown at its own timestamp (not a - 1:
         * rows of another key visited earlier may lie between, 3.9 step 2). p's own rows fold after, below */
        if (p->nt) {
            uint32_t a = 0, b = p->nt;
            while (a < b) { /* the first tombstone with series >= b0 */
                uint32_t mid = a + (b - a) / 2;
                if (p->t[mid].series < b0)
                    a = mid + 1;
                else
                    b = mid;
            }
            for (; a < p->nt && p->t[a].series < b1; a++) {
                uint32_t k = p->t[a].series - b0, i = base + k;
                uint64_t bit = 1ull << (k & 63);
                if (!(noted[k >> 6] & bit))
                    continue;
                int64_t kts = lt_dec(pg->e[2 * i]);
                const part *np = db->parts[pg->e[2 * i + 1] >> 32];
                if (np->partition == p->partition && kts >= p->t[a].start && kts < p->t[a].end) {
                    noted[k >> 6] &= ~bit;
                    pg->unk[i >> 6] |= 1ull << (i & 63);
                    pg->e[2 * i + 1] = 0;
                }
            }
        }
        for (uint32_t r = lo; r < ix->nruns && ix->rser[r] < b1; r++) {
            uint32_t s = ix->rser[r], k = s - b0, i = base + k;
            uint32_t j = r + 1 < ix->nruns ? gix_rstart(ix, r + 1) : p->ng;
            uint64_t bit = 1ull << (k & 63);
            uint64_t *nw = &noted[k >> 6];
            uint64_t *uw = &pg->unk[i >> 6];
            uint64_t ubit = 1ull << (i & 63);
            if (j <= gix_rstart(ix, r)) /* no granules (cannot be: a run has at least one) */
                continue;
            int64_t tmax = gx_ts_max(p, j - 1);
            uint64_t w = pg->e[2 * i];
            bool take;
            if (*nw & bit)
                take = tmax >= lt_dec(w);
            else if (*uw & ubit)
                take = tmax >= lt_dec(w);
            else
                take = true; /* empty */
            if (take) {
                pg->e[2 * i] = lt_enc(tmax);
                pg->e[2 * i + 1] = ((uint64_t)pi << 32) | (j - 1);
                *nw |= bit;
                *uw &= ~ubit;
            }
        }
    }
    /* the noted entries: the last value of their granule decides */
    for (uint32_t k = 0; k < b1 - b0; k++) {
        if (!((noted[k >> 6] >> (k & 63)) & 1))
            continue;
        uint32_t i = base + k;
        int64_t ts = lt_dec(pg->e[2 * i]);
        const part *p = db->parts[pg->e[2 * i + 1] >> 32];
        uint32_t g = (uint32_t)pg->e[2 * i + 1];
        int type = eng_series_type(db, b0 + k);
        granule gb;
        gx_load(p, g, GW_TWEND | GW_STAT, &gb);
        uint32_t cnt = gx_count(p, g);
        bool unknown = true;
        int64_t bound = ts;
        uint64_t slot = 0;
        if (type == TSDB_STATE) { /* no time-weighted statistics: all-null granules are known from the counts */
            bound = gb.nulls == cnt && ts != INT64_MIN ? ts - 1 : ts;
        } else if (type < 0) {
            bound = ts;
        } else if (gb.tw_flags & GT_LAST_NULL) {
            bound = ts == INT64_MIN ? INT64_MIN : ts - 1;
            if (ts == INT64_MIN) { /* no non-null row can be at ts and every row seen is at or below it */
                pg->e[2 * i] = pg->e[2 * i + 1] = 0;
                continue;
            }
        } else if (slot_of_last(type, gb.tw_last, &slot)) {
            unknown = false;
        }
        if (unknown) {
            pg->e[2 * i] = lt_enc(bound);
            pg->e[2 * i + 1] = 0;
            pg->unk[i >> 6] |= 1ull << (i & 63);
        } else {
            pg->e[2 * i + 1] = slot;
        }
    }
    return 0;
}

static void *build_thread(void *arg)
{
    lb_job *j = arg;
    eng *db = j->db;
    uint32_t b;
    while ((b = atomic_fetch_add(&j->next, 1)) < j->nblocks) {
        if (atomic_load(&j->rc))
            break;
        uint32_t b0 = b * LB_BLOCK, b1 = b0 + LB_BLOCK;
        if (b1 > j->maxid + 1)
            b1 = j->maxid + 1;
        if ((b0 >> LT_PAGE_BITS) >= db->lt->npg) /* beyond the budget: nothing is built (the volume is capped) */
            continue;
        int rc = build_block(db, db->lt, b0, b1);
        if (rc)
            atomic_store(&j->rc, rc);
    }
    return NULL;
}

int latest_build(eng *db)
{
    ltab *t = db->lt;
    if (!t)
        return 0;
    uint32_t maxid = 0;
    bool any = false;
    for (size_t i = 0; i < db->nparts; i++) {
        const gix *ix = &db->parts[i]->ix;
        if (ix->nruns) {
            any = true;
            if (ix->rser[ix->nruns - 1] > maxid)
                maxid = ix->rser[ix->nruns - 1];
        }
    }
    if (!any)
        return 0;
    pthread_rwlock_wrlock(&t->rw);
    lt_make_pages(db, t, maxid >> LT_PAGE_BITS); /* in id order, before the walk: the first refusal caps the volume */
    pthread_rwlock_unlock(&t->rw);
    lb_job j = {.db = db, .maxid = maxid, .nblocks = maxid / LB_BLOCK + 1};
    atomic_init(&j.next, 0);
    atomic_init(&j.rc, 0);
    int nt = db->opt.query_threads > 0 ? db->opt.query_threads : 1;
    if ((uint32_t)nt > j.nblocks)
        nt = (int)j.nblocks;
    if (nt > 64)
        nt = 64;
    pthread_t th[64];
    int started = 0;
    for (int i = 1; i < nt; i++)
        if (tsdb_thread_create(&th[started], build_thread, &j) == 0)
            started++;
    build_thread(&j);
    for (int i = 0; i < started; i++)
        pthread_join(th[i], NULL);
    int rc = atomic_load(&j.rc);
    if (rc == 0) /* the unknown entries the build left: the warmer settles them */
        hints_push(db, NULL, 0, true);
    return rc;
}

/* ---- the lookup (2.3) ---- */

enum { MC_ABSENT = 0, MC_NONE, MC_CAND, MC_SEARCH };
typedef struct lmc { /* one memtable's view of one series */
    uint8_t st;
    bool has_null, sorted;
    int64_t t0, t1; /* the first and last row's timestamps (all rows, visible or not) */
    int64_t cts;
    uint64_t cslot;
} lmc;

static void lmc_read(const memtable *m, uint32_t id, uint64_t visible, lmc *o)
{
    memset(o, 0, sizeof *o);
    if (!m || id >= m->scap || m->s[id].n == 0)
        return; /* MC_ABSENT */
    const mt_series *s = &m->s[id];
    o->has_null = s->fl & MTF_HAS_NULL;
    o->sorted = s->sorted;
    o->t0 = s->ts[0];
    o->t1 = s->ts[s->n - 1];
    if (!(s->fl & MTF_HAS_NN)) {
        o->st = MC_NONE;
        return;
    }
    if ((s->fl & MTF_NN_STALE) || s->lsn[s->top_nn] > visible) { /* the newest visible non-null row needs a scan */
        o->st = MC_SEARCH;
        return;
    }
    o->st = MC_CAND;
    o->cts = s->ts[s->top_nn];
    o->cslot = s->v[s->top_nn];
}

/* whether memtable m may hold a null at timestamp T that overwrites an older row there */
static bool lmc_may_shadow(const lmc *o, int64_t T)
{
    if (o->st == MC_ABSENT || !o->has_null)
        return false;
    return !(o->sorted && (o->t0 > T || o->t1 < T));
}

enum { LH_MAX = 256, LH_SUB = 128 };

typedef struct lhold { /* what one hold read for one id */
    lt_ent e;
    bool has_entry;
    int64_t floor, cutoff;
    lmc a, f; /* the active and the flushing memtable */
} lhold;

/* Answers up to LH_MAX ids from the table and the memtables; ids it cannot decide are marked need[]. */
static void lookup_hold(eng *db, const uint32_t *local, size_t m, const int64_t *ends, int64_t end, bool *need,
                        latest_row *out, lt_sec *hints, bool *multi_flush)
{
    ltab *t = db->lt;
    lhold h[LH_MAX];
    memtable *act = NULL, *fl = NULL;
    uint64_t visible;
    *multi_flush = false;
    pthread_rwlock_rdlock(&t->rw);
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    visible = db->wal_enabled && db->opt.wal_sync_ms == 0 ? atomic_load(&db->wal_durable) : UINT64_MAX;
    if (db->active) {
        act = db->active;
        mt_ref(act);
    }
    if (db->nflushing == 1) {
        fl = db->flushing[0];
        mt_ref(fl);
    } else if (db->nflushing > 1) {
        *multi_flush = true;
    }
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < m; i++) {
        lhold *x = &h[i];
        x->has_entry = lt_load(t, local[i], &x->e);
        x->floor = t->drop_floor;
        x->cutoff = eng_cutoff(db, local[i]); /* inside the hold: a lengthened limit marks entries in one section */
        lmc_read(fl, local[i], visible, &x->f);
    }
    if (db->lt_hook)
        db->lt_hook(db->lt_hook_arg, LH_LOOKUP_AFTER_ENTRY);
    for (size_t a = 0; act && a < m; a += LH_SUB) {
        size_t b = a + LH_SUB < m ? a + LH_SUB : m;
        pthread_mutex_lock(&db->buf_lock);
        for (size_t i = a; i < b; i++)
            lmc_read(act, local[i], visible, &h[i].a);
        pthread_mutex_unlock(&db->buf_lock);
    }
    if (!act)
        for (size_t i = 0; i < m; i++)
            memset(&h[i].a, 0, sizeof h[i].a);
    pthread_rwlock_unlock(&t->rw);
    if (act)
        mt_unref(act);
    if (fl)
        mt_unref(fl);
    for (size_t i = 0; i < m; i++) {
        lhold *x = &h[i];
        int64_t lim = ends ? ends[i] : end;
        if (!x->has_entry || *multi_flush) {
            need[i] = true;
            continue;
        }
        if (x->a.st == MC_SEARCH || x->f.st == MC_SEARCH) {
            atomic_fetch_add(&t->st_nn, 1);
            need[i] = true;
            continue;
        }
        /* the table's candidate, or the bound of an unknown entry (a known one below the drop floor is one) */
        bool tc = false, unk = false;
        int64_t tts = 0, ubound = 0;
        if (x->e.st == LE_KNOWN && x->e.ts >= x->floor) {
            tc = true;
            tts = x->e.ts;
        } else if (x->e.st != LE_EMPTY) {
            unk = true;
            ubound = x->e.ts;
            if (ubound < x->cutoff) /* every non-null row in the parts has expired */
                unk = false;
        }
        int bsrc = 0; /* 1 active, 2 flushing, 3 table */
        int64_t bts = 0;
        uint64_t bslot = 0;
        if (x->a.st == MC_CAND) {
            bsrc = 1, bts = x->a.cts, bslot = x->a.cslot;
        }
        if (x->f.st == MC_CAND && (!bsrc || x->f.cts > bts)) {
            bsrc = 2, bts = x->f.cts, bslot = x->f.cslot;
        }
        if (tc && (!bsrc || tts > bts)) {
            bsrc = 3, bts = tts, bslot = x->e.x;
        }
        if (unk && (!bsrc || bts < ubound)) { /* no memtable row at or above the bound: the parts must be read */
            atomic_fetch_add(&t->st_floor, 1);
            hints_add(hints, local[i]);
            need[i] = true;
            continue;
        }
        if (bsrc >= 2 && lmc_may_shadow(&x->a, bts)) {
            need[i] = true;
            continue;
        }
        if (bsrc == 3 && lmc_may_shadow(&x->f, bts)) {
            need[i] = true;
            continue;
        }
        if (!bsrc) { /* nothing in the parts, nothing in the memtables: none */
            out[i].kind = LK_NONE_;
            continue;
        }
        if (bts >= lim) { /* the row lies at or above the bound: the answer is older, only a search finds it */
            need[i] = true;
            continue;
        }
        if (bts < x->cutoff) { /* expired, and so is every older row */
            out[i].kind = LK_NONE_;
            continue;
        }
        out[i].kind = LK_VALUE_;
        out[i].ts = bts;
        out[i].slot = bslot;
    }
}

int eng_latest(eng *db, const uint32_t *local, size_t n, const int64_t *ends, int64_t end, bool with_nulls,
               latest_row *out)
{
    ltab *t = db->lt;
    if (!t || with_nulls)
        return eng_latest_search(db, local, n, ends, end, with_nulls, out);
    bool *need = calloc(n ? n : 1, sizeof *need);
    if (!need)
        return TSDB_ERR_NOMEM;
    lt_sec hints = {0};
    for (size_t i = 0; i < n; i++) {
        out[i] = (latest_row){.kind = LK_NONE_};
        int ty = eng_series_type(db, local[i]);
        if (ty < 0) {
            out[i].status = TSDB_ERR_NOTFOUND;
            continue;
        }
        out[i].type = (uint8_t)ty;
    }
    /* the ids that name a series, in holds of at most LH_MAX */
    uint32_t ids[LH_MAX];
    size_t pos[LH_MAX];
    size_t m = 0, nsearch = 0, nanswered = 0;
    for (size_t i = 0; i <= n; i++) {
        if (i < n && out[i].status == 0) {
            ids[m] = local[i];
            pos[m++] = i;
        }
        if (m == LH_MAX || (i == n && m)) {
            bool nd[LH_MAX] = {0}, multi;
            latest_row tmp[LH_MAX];
            for (size_t k = 0; k < m; k++)
                tmp[k] = out[pos[k]];
            int64_t e2[LH_MAX];
            if (ends)
                for (size_t k = 0; k < m; k++)
                    e2[k] = ends[pos[k]];
            lookup_hold(db, ids, m, ends ? e2 : NULL, end, nd, tmp, &hints, &multi);
            for (size_t k = 0; k < m; k++) {
                out[pos[k]] = tmp[k];
                need[pos[k]] = nd[k];
                nsearch += nd[k];
            }
            nanswered += m;
            m = 0;
        }
    }
    atomic_fetch_add(&t->st_table, nanswered - nsearch);
    atomic_fetch_add(&t->st_search, nsearch);
    int rc = 0;
    if (nsearch) { /* the searches, on one cut taken after the holds, outside latest_rw */
        uint32_t *l2 = malloc(nsearch * sizeof *l2);
        int64_t *e2 = ends ? malloc(nsearch * sizeof *e2) : NULL;
        latest_row *o2 = malloc(nsearch * sizeof *o2);
        if (!l2 || !o2 || (ends && !e2))
            rc = TSDB_ERR_NOMEM;
        size_t k = 0;
        for (size_t i = 0; rc == 0 && i < n; i++)
            if (need[i]) {
                l2[k] = local[i];
                if (e2)
                    e2[k] = ends[i];
                k++;
            }
        if (rc == 0)
            rc = eng_latest_search(db, l2, nsearch, e2, end, false, o2);
        k = 0;
        for (size_t i = 0; rc == 0 && i < n; i++)
            if (need[i])
                out[i] = o2[k++];
        free(l2);
        free(e2);
        free(o2);
    }
    hints_push(db, hints.id, hints.n, hints.oom);
    free(hints.id);
    free(need);
    return rc;
}

/* ---- the warmer (3.8): not yet; the unknown entries are settled by the next publish of a newer row ---- */

void latest_start(eng *db) { (void)db; }

void latest_stop(eng *db) { (void)db; }
