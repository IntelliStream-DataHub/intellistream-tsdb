/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Hot lists and page-cache residency (docs/design-stats.md step 4, ST10, ST12).
 *
 * Three lists name the busiest series without a counter per series:
 *  - reads: a weighted space-saving summary (k = 256) per thread, fed by a sample of 1 query in 8 (drawn at random per
 *    query, so a round-robin client cannot alias with it); each sample weighs 8. The lists of the threads are merged
 *    by the mergeable-summaries rule.
 *  - writes: the same summary, fed by the sweeper with the rows of every flushed part (not sampled).
 *  - bytes per row: the k worst series by compressed bytes per row, over runs with at least one full granule in parts
 *    that merges wrote, one entry per series.
 * Reads and writes keep two windows of 60 s, the current and the one before; a list that nobody updated for two
 * windows says nothing (it is skipped by its epoch). Each list is guarded by its own mutex (a leaf): the owner takes
 * it once per sample, about one query in eight, so the cost on the query path is a few nanoseconds.
 *
 * Residency: parts are mapped, so the page cache is the cache. The sweeper samples up to 64 granules of every part
 * about every stats_resident_s with mincore, scales what it finds to the part's granules and keeps the result in the
 * part's record, per tenant, so a removed part takes its share out again. A series asks for its own residency
 * exactly, one mincore per (part, series).
 */
#include "stats_priv.h"

/* ------------------------------------------------------------------------------ space-saving */

static unsigned ss_hash(uint64_t id) { return (unsigned)((id * 0x9E3779B97F4A7C15ull) >> 55); } /* 9 bits */

void ss_clear(ss *s)
{
    s->n = 0;
    memset(s->bucket, 0, sizeof s->bucket);
}

static void ss_swap(ss *s, unsigned i, unsigned j)
{
    uint8_t a = s->h[i], b = s->h[j];
    s->h[i] = b;
    s->h[j] = a;
    s->hpos[b] = (uint8_t)i;
    s->hpos[a] = (uint8_t)j;
}

#define SS_EST(s, i) ((s)->e[(s)->h[i]].est)

static void ss_up(ss *s, unsigned i)
{
    while (i) {
        unsigned p = (i - 1) / 2;
        if (SS_EST(s, p) <= SS_EST(s, i))
            break;
        ss_swap(s, i, p);
        i = p;
    }
}

static void ss_down(ss *s, unsigned i)
{
    for (;;) {
        unsigned l = 2 * i + 1, r = l + 1, m = i;
        if (l < s->n && SS_EST(s, l) < SS_EST(s, m))
            m = l;
        if (r < s->n && SS_EST(s, r) < SS_EST(s, m))
            m = r;
        if (m == i)
            break;
        ss_swap(s, i, m);
        i = m;
    }
}

static int ss_slot(const ss *s, uint64_t id)
{
    for (unsigned c = s->bucket[ss_hash(id)]; c; c = s->nxt[c - 1])
        if (s->e[c - 1].id == id)
            return (int)(c - 1);
    return -1;
}

static void ss_link(ss *s, unsigned slot)
{
    unsigned b = ss_hash(s->e[slot].id);
    s->nxt[slot] = s->bucket[b];
    s->bucket[b] = (uint16_t)(slot + 1);
}

static void ss_unlink(ss *s, unsigned slot)
{
    uint16_t *pp = &s->bucket[ss_hash(s->e[slot].id)];
    while (*pp != slot + 1)
        pp = &s->nxt[*pp - 1];
    *pp = s->nxt[slot];
}

const ss_ent *ss_find(const ss *s, uint64_t id)
{
    int k = ss_slot(s, id);
    return k < 0 ? NULL : &s->e[k];
}

uint64_t ss_min(const ss *s) { return s->n == SS_K ? s->e[s->h[0]].est : 0; }

static void ss_insert(ss *s, uint64_t id, uint64_t est)
{
    unsigned slot = s->n++;
    s->e[slot] = (ss_ent){id, est, 0};
    s->h[slot] = (uint8_t)slot;
    s->hpos[slot] = (uint8_t)slot;
    ss_link(s, slot);
    ss_up(s, slot);
}

void ss_add(ss *s, uint64_t id, uint64_t w)
{
    int slot = ss_slot(s, id);
    if (slot >= 0) {
        s->e[slot].est += w;
        ss_down(s, s->hpos[slot]);
    } else if (s->n < SS_K) {
        ss_insert(s, id, w);
    } else { /* the minimum goes: its weight is what the newcomer may already have had */
        unsigned m = s->h[0];
        ss_unlink(s, m);
        uint64_t floor = s->e[m].est;
        s->e[m] = (ss_ent){id, floor + w, floor};
        ss_link(s, m);
        ss_down(s, 0);
    }
}

void ss_set(ss *s, uint64_t id, uint64_t value)
{
    int slot = ss_slot(s, id);
    if (slot >= 0) {
        s->e[slot].est = value;
        s->e[slot].err = 0;
        unsigned p = s->hpos[slot];
        ss_up(s, p);
        ss_down(s, s->hpos[slot]);
    } else if (s->n < SS_K) {
        ss_insert(s, id, value);
    } else if (value > s->e[s->h[0]].est) {
        unsigned m = s->h[0];
        ss_unlink(s, m);
        s->e[m] = (ss_ent){id, value, 0};
        ss_link(s, m);
        ss_down(s, 0);
    }
}

/* ---------------------------------------------------------------------------------- windows */

atomic_int_fast64_t stats_hot_clock_ms;

int64_t hot_now_ms(void)
{
    int64_t t = atomic_load_explicit(&stats_hot_clock_ms, memory_order_relaxed);
    if (t)
        return t;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

hotl *hotl_new(void)
{
    hotl *h = calloc(1, sizeof *h);
    if (h)
        pthread_mutex_init(&h->mu, NULL);
    return h;
}

void hotl_free(hotl *h)
{
    if (!h)
        return;
    pthread_mutex_destroy(&h->mu);
    free(h);
}

void hotl_add(hotl *h, int64_t epoch, uint64_t id, uint64_t w)
{
    pthread_mutex_lock(&h->mu);
    if (epoch != h->epoch) {
        if (epoch == h->epoch + 1) {
            h->cur ^= 1; /* the old current window is the previous one now; the old previous one is reused */
            ss_clear(&h->w[h->cur]);
        } else {
            ss_clear(&h->w[0]);
            ss_clear(&h->w[1]);
        }
        h->epoch = epoch;
    }
    ss_add(&h->w[h->cur], id, w);
    pthread_mutex_unlock(&h->mu);
}

/* ------------------------------------------------------------------------------- merging */

typedef struct hent {
    uint64_t id, est, err, m;
} hent;
struct hagg {
    hent *e;
    size_t n, cap;
    uint64_t min_sum; /* the minima of the full lists added: what an id missing from them may weigh */
    bool done;
};

hagg *hagg_new(void) { return calloc(1, sizeof(hagg)); }

void hagg_free(hagg *a)
{
    if (a)
        free(a->e);
    free(a);
}

static void hagg_push(hagg *a, uint64_t id, uint64_t est, uint64_t err, uint64_t m)
{
    if (a->n == a->cap) {
        size_t cap = a->cap ? a->cap * 2 : 512;
        hent *v = realloc(a->e, cap * sizeof *v);
        if (!v)
            return;
        a->e = v;
        a->cap = cap;
    }
    a->e[a->n++] = (hent){id, est, err, m};
}

void hagg_add_ss(hagg *a, const ss *s, uint64_t sid_base, bool with_min)
{
    uint64_t m = with_min ? ss_min(s) : 0;
    a->min_sum += m;
    for (uint32_t i = 0; i < s->n; i++)
        hagg_push(a, s->e[i].id | sid_base, s->e[i].est, s->e[i].err, m);
    a->done = false;
}

void hagg_add_hotl(hagg *a, hotl *h, uint64_t sid_base, int64_t now_epoch)
{
    pthread_mutex_lock(&h->mu);
    if (h->epoch == now_epoch || h->epoch == now_epoch - 1) {
        hagg_add_ss(a, &h->w[h->cur], sid_base, true);
        if (h->epoch == now_epoch) /* the window before it is one of the two still counted */
            hagg_add_ss(a, &h->w[h->cur ^ 1], sid_base, true);
    }
    pthread_mutex_unlock(&h->mu);
}

static int hent_id_cmp(const void *x, const void *y)
{
    const hent *a = x, *b = y;
    return (a->id > b->id) - (a->id < b->id);
}

static int hent_est_cmp(const void *x, const void *y)
{
    const hent *a = x, *b = y;
    if (a->est != b->est)
        return a->est > b->est ? -1 : 1;
    return (a->id > b->id) - (a->id < b->id);
}

/* one entry per id, sorted by id: the estimates summed, and what the lists that do not hold the id may add */
static void hagg_finish(hagg *a)
{
    if (a->done)
        return;
    if (a->n)
        qsort(a->e, a->n, sizeof *a->e, hent_id_cmp); /* e is NULL while empty: qsort and memcpy take no null */
    size_t k = 0;
    for (size_t i = 0; i < a->n;) {
        hent r = a->e[i];
        uint64_t present = 0;
        size_t j = i;
        r.est = r.err = 0;
        for (; j < a->n && a->e[j].id == a->e[i].id; j++) {
            r.est += a->e[j].est;
            r.err += a->e[j].err;
            present += a->e[j].m;
        }
        uint64_t absent = a->min_sum - present;
        r.est += absent;
        r.err += absent;
        a->e[k++] = r;
        i = j;
    }
    a->n = k;
    a->done = true;
}

int hagg_top(hagg *a, tsdb_hot *out, size_t k, size_t *n, uint64_t *threshold)
{
    hagg_finish(a);
    hent *c = malloc((a->n ? a->n : 1) * sizeof *c); /* a->e stays sorted by id for hagg_estimate */
    if (!c)
        return TSDB_ERR_NOMEM;
    if (a->n) {
        memcpy(c, a->e, a->n * sizeof *c);
        qsort(c, a->n, sizeof *c, hent_est_cmp);
    }
    size_t m = MIN(k, a->n);
    for (size_t i = 0; i < m; i++)
        out[i] = (tsdb_hot){c[i].id, c[i].est, c[i].err};
    free(c);
    *n = m;
    if (threshold)
        *threshold = a->min_sum;
    return 0;
}

uint64_t hagg_estimate(hagg *a, uint64_t sid, uint64_t *err)
{
    hagg_finish(a);
    size_t lo = 0, hi = a->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (a->e[mid].id < sid)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo < a->n && a->e[lo].id == sid) {
        *err = a->e[lo].err;
        return a->e[lo].est;
    }
    *err = 0;
    return 0; /* not in any list: it is below the threshold, and reported as 0 */
}

/* ------------------------------------------------------------------------- bytes per row */

void hot_bpr_run(struct estats *es, uint32_t id, uint64_t rows, uint64_t bytes, bool full)
{
    if (!full || !rows)
        return;
    pthread_mutex_lock(&es->mu);
    ss_set(&es->bpr, id, bytes * 1000 / rows);
    pthread_mutex_unlock(&es->mu);
}

void eng_hot_collect(eng *db, void *writes, void *bpr, uint64_t sid_base)
{
    struct estats *es = atomic_load_explicit(&db->stats, memory_order_acquire);
    if (!es)
        return;
    if (writes)
        hagg_add_hotl(writes, es->hw, sid_base, hot_now_ms() / HOT_PERIOD_MS);
    if (bpr) {
        pthread_mutex_lock(&es->mu);
        hagg_add_ss(bpr, &es->bpr, sid_base, false);
        pthread_mutex_unlock(&es->mu);
    }
}

/* -------------------------------------------------------------------------- residency */

size_t resident_bytes_range(const uint8_t *p, size_t len)
{
    static _Atomic size_t page_cache; /* several sweepers (one per engine) may fill it at once: same value */
    size_t page = atomic_load_explicit(&page_cache, memory_order_relaxed);
    if (!page) {
        long ps = sysconf(_SC_PAGESIZE);
        page = ps > 0 ? (size_t)ps : 4096;
        atomic_store_explicit(&page_cache, page, memory_order_relaxed);
    }
    if (!len)
        return 0;
    uintptr_t a = (uintptr_t)p, b = a + len, lo = a & ~(uintptr_t)(page - 1), hi = (b + page - 1) & ~(uintptr_t)(page - 1);
    size_t pages = (hi - lo) / page;
    unsigned char stack_vec[64];
    unsigned char *vec = pages <= sizeof stack_vec ? stack_vec : malloc(pages);
    if (!vec)
        return 0;
    size_t got = 0;
    if (tsdb_mincore((void *)lo, hi - lo, vec) == 0)
        for (size_t i = 0; i < pages; i++)
            if (vec[i] & 1) {
                uintptr_t from = MAX(lo + i * page, a), to = MIN(lo + (i + 1) * page, b);
                got += to - from;
            }
    if (vec != stack_vec)
        free(vec);
    return got;
}

void rec_free_res(prec *r)
{
    free(r->rt);
    free(r->rb);
    r->rt = NULL;
    r->rb = NULL;
    r->nres = 0;
}

void residency_forget(struct estats *es, prec *r)
{
    if (r->nres) {
        pthread_mutex_lock(&es->mu);
        for (unsigned i = 0; i < r->nres; i++)
            tenant_res_add(es, r->rt[i], r->rb[i], false);
        pthread_mutex_unlock(&es->mu);
    }
    rec_free_res(r);
}

#define RES_SAMPLES 64

static void sample_part(struct estats *es, const part *p, prec *rec)
{
    const gix *x = &p->ix;
    uint32_t n = x->n, m = MIN(n, (uint32_t)RES_SAMPLES);
    if (!m)
        return;
    uint16_t tn[RES_SAMPLES];
    uint64_t tb[RES_SAMPLES];
    unsigned nt = 0;
    for (uint32_t s = 0; s < m; s++) {
        uint32_t e = (uint32_t)(((uint64_t)(2 * s + 1) * n) / (2 * (uint64_t)m));
        uint64_t off = gix_get(x, e, GF_OFF), len = gix_get(x, e, GF_TSLEN) + gix_get(x, e, GF_VALLEN);
        if (off > p->map_len || len > p->map_len - off)
            continue; /* damaged index: no sample */
        uint32_t id = x->rser[gix_run_of(x, e)];
        uint32_t t = tenant_of(es, id);
        size_t got = resident_bytes_range(p->map + off, len);
        unsigned i = 0;
        while (i < nt && tn[i] != t)
            i++;
        if (i == nt) {
            tn[nt] = (uint16_t)t;
            tb[nt++] = 0;
        }
        tb[i] += got;
    }
    uint16_t *rt = malloc((nt ? nt : 1) * sizeof *rt);
    uint64_t *rb = malloc((nt ? nt : 1) * sizeof *rb);
    if (!rt || !rb) {
        free(rt);
        free(rb);
        return;
    }
    for (unsigned i = 0; i < nt; i++) { /* the sample stands for all n granules */
        rt[i] = tn[i];
        rb[i] = (uint64_t)((double)tb[i] * (double)n / (double)m);
    }
    pthread_mutex_lock(&es->mu);
    for (unsigned i = 0; i < rec->nres; i++)
        tenant_res_add(es, rec->rt[i], rec->rb[i], false);
    for (unsigned i = 0; i < nt; i++)
        tenant_res_add(es, rt[i], rb[i], true);
    pthread_mutex_unlock(&es->mu);
    free(rec->rt);
    free(rec->rb);
    rec->rt = rt;
    rec->rb = rb;
    rec->nres = (uint16_t)nt;
    rec->res_ns = stats_mono_ns();
}

/* A few parts a tick, so that every part is sampled about once a period; a part that is not due is skipped. */
void residency_step(struct estats *es)
{
    eng *db = es->db;
    if (stats_stopping(es))
        return;
    pthread_mutex_lock(&db->lock);
    size_t n = db->nparts, k = MAX((size_t)8, n / 600 + 1);
    if (k > n)
        k = n;
    part **ps = k ? malloc(k * sizeof *ps) : NULL;
    if (ps)
        for (size_t i = 0; i < k; i++) {
            part *p = db->parts[(es->res_cursor + i) % n];
            part_ref(p);
            ps[i] = p;
        }
    es->res_cursor = n ? (es->res_cursor + k) % n : 0;
    pthread_mutex_unlock(&db->lock);
    if (!ps)
        return;
    int64_t now = stats_mono_ns();
    for (size_t i = 0; i < k; i++) {
        pthread_mutex_lock(&es->mu);
        prec *rec = rec_find(es, ps[i]);
        bool due = rec && rec_matches(rec, ps[i]) && (rec->res_ns == 0 || now - rec->res_ns >= es->res_period_ns);
        pthread_mutex_unlock(&es->mu);
        if (due)
            sample_part(es, ps[i], rec);
        part_unref(ps[i]);
    }
    free(ps);
}
