/*
 * Tenants (docs/design-stats.md steps 3, ST4-ST8, ST11, section 4).
 *
 * A tenant is a configured name prefix; a series belongs to the tenant with the longest prefix its name starts with.
 * The numbers are process-wide and never reused (tenant_intern), so every volume of a handle, and every handle, agrees
 * on them and a removed prefix never shares a number with a later one. Number 0 is "(none)".
 *
 * Per volume the sweeper keeps
 *  - the tenant map: 2 bytes per local id, in pages of 65,536 ids; a page whose ids all belong to one tenant is that
 *    number in the page table and allocates nothing. It is filled in the background, in id order, before a part is
 *    walked (tenants_extend), so no lookup on the request or write path ever matches a name;
 *  - one vector per (partition): what each tenant has in it (rows, bytes by column, granules), sparse. A tenant's
 *    totals are the sum of these vectors and change only together with them, under es->mu, so they cannot drift or go
 *    below zero: a flushed part adds its split; a merge, a delete or any removal marks its partition stale, and the
 *    recount replaces the vector wholesale (the difference in rows is what merges removed); retention subtracts a
 *    partition whole, with no walk; a list change moves only the series whose tenant changed.
 *  - HyperLogLogs of the series that were flushed in the last two hours, and the hot-writes list.
 *
 * Everything here runs in the sweeper thread; es->mu (a leaf) guards only what other threads read: the tenant totals
 * and the HyperLogLogs. The scratch (sc, touch) holds the split of the part walked last.
 */
#include "stats_priv.h"

#include <math.h>

/* Tests only (test_tenants): makes the tenant side's large allocations fail, as under memory pressure. */
atomic_size_t stats_test_alloc_cap;

static bool alloc_refused(size_t n)
{
    size_t cap = atomic_load_explicit(&stats_test_alloc_cap, memory_order_relaxed);
    return cap && n > cap;
}

/* ------------------------------------------------------------------------ the process-wide numbers */

static pthread_mutex_t intern_mu = PTHREAD_MUTEX_INITIALIZER;
static char **intern_s;
static uint32_t intern_n, intern_cap;
static uint32_t *intern_ht; /* index + 1; open addressing */
static size_t intern_htn;

static uint64_t fnv(const char *s, size_t len, uint64_t h)
{
    for (size_t i = 0; i < len; i++)
        h = (h ^ (uint8_t)s[i]) * 0x100000001b3ull;
    return h;
}

/* The number of prefix s (len bytes), made when it is new; 0: all 65,535 are used. */
static uint32_t tenant_intern(const char *s, size_t len)
{
    uint32_t no = 0;
    pthread_mutex_lock(&intern_mu);
    if (intern_htn < 2 * (size_t)(intern_n + 1)) { /* grow: load factor under one half */
        size_t n = intern_htn ? intern_htn * 2 : 1024;
        uint32_t *t = calloc(n, sizeof *t);
        if (!t)
            goto out;
        for (uint32_t i = 0; i < intern_n; i++) {
            size_t k = fnv(intern_s[i], strlen(intern_s[i]), 0xcbf29ce484222325ull) & (n - 1);
            while (t[k])
                k = (k + 1) & (n - 1);
            t[k] = i + 1;
        }
        free(intern_ht);
        intern_ht = t;
        intern_htn = n;
    }
    size_t k = fnv(s, len, 0xcbf29ce484222325ull) & (intern_htn - 1);
    while (intern_ht[k]) {
        const char *q = intern_s[intern_ht[k] - 1];
        if (strlen(q) == len && memcmp(q, s, len) == 0) {
            no = intern_ht[k];
            goto out;
        }
        k = (k + 1) & (intern_htn - 1);
    }
    if (intern_n >= TENANT_MAX)
        goto out;
    if (intern_n == intern_cap) {
        uint32_t cap = intern_cap ? intern_cap * 2 : 64;
        char **v = realloc(intern_s, cap * sizeof *v);
        if (!v)
            goto out;
        intern_s = v;
        intern_cap = cap;
    }
    char *copy = strndup(s, len);
    if (!copy)
        goto out;
    intern_s[intern_n++] = copy;
    intern_ht[k] = intern_n;
    no = intern_n;
out:
    pthread_mutex_unlock(&intern_mu);
    return no;
}

/* ------------------------------------------------------------------------------------ the list */

static int str_cmp(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

int tlist_make(const char *const *prefixes, size_t n, tlist **out)
{
    *out = NULL;
    tlist *t = calloc(1, sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    atomic_init(&t->refs, 1);
    char **v = malloc((n ? n : 1) * sizeof *v);
    if (!v) {
        free(t);
        return TSDB_ERR_NOMEM;
    }
    size_t m = 0;
    for (size_t i = 0; i < n; i++) {
        if (!prefixes || !prefixes[i] || !prefixes[i][0])
            continue; /* an empty prefix would be every series: that is "(none)" already */
        if (!(v[m] = strdup(prefixes[i]))) {
            while (m)
                free(v[--m]);
            free(v);
            free(t);
            return TSDB_ERR_NOMEM;
        }
        m++;
    }
    qsort(v, m, sizeof *v, str_cmp);
    size_t k = 0;
    for (size_t i = 0; i < m; i++) { /* drop duplicates */
        if (k && strcmp(v[k - 1], v[i]) == 0)
            free(v[i]);
        else
            v[k++] = v[i];
    }
    t->n = (uint32_t)k;
    t->s = v;
    t->len = malloc((k ? k : 1) * sizeof *t->len);
    t->no = malloc((k ? k : 1) * sizeof *t->no);
    t->parent = malloc((k ? k : 1) * sizeof *t->parent);
    int32_t *stack = malloc((k ? k : 1) * sizeof *stack);
    if (!t->len || !t->no || !t->parent || !stack) {
        free(stack);
        tlist_unref(t);
        return TSDB_ERR_NOMEM;
    }
    size_t sp = 0;
    uint64_t h = 0xcbf29ce484222325ull;
    for (size_t i = 0; i < k; i++) {
        t->len[i] = (uint32_t)strlen(v[i]);
        t->no[i] = tenant_intern(v[i], t->len[i]);
        if (t->no[i] == 0) {
            free(stack);
            tlist_unref(t);
            return TSDB_ERR_INVALID; /* a process makes at most 65,535 different tenants */
        }
        t->maxno = MAX(t->maxno, t->no[i]);
        while (sp && !(t->len[stack[sp - 1]] < t->len[i] && memcmp(v[stack[sp - 1]], v[i], t->len[stack[sp - 1]]) == 0))
            sp--;
        t->parent[i] = sp ? stack[sp - 1] : -1;
        stack[sp++] = (int32_t)i;
        h = fnv(v[i], t->len[i], h);
        h = (h ^ 0xff) * 0x100000001b3ull; /* a separator no string byte makes unambiguous with the length */
    }
    free(stack);
    t->hash = h;
    *out = t;
    return 0;
}

tlist *tlist_ref(tlist *t)
{
    if (t)
        atomic_fetch_add_explicit(&t->refs, 1, memory_order_relaxed);
    return t;
}

void tlist_unref(tlist *t)
{
    if (!t || atomic_fetch_sub_explicit(&t->refs, 1, memory_order_acq_rel) != 1)
        return;
    for (uint32_t i = 0; i < t->n; i++)
        free(t->s[i]);
    free(t->s);
    free(t->len);
    free(t->no);
    free(t->parent);
    free(t);
}

/* The longest prefix of `name` in the list: the nearest entry at or before the name in sorted order, or the nearest
 * of its ancestors that the name starts with (a longer prefix of the name would sort between that entry and it). */
uint32_t tlist_match(const tlist *t, const char *name)
{
    uint32_t lo = 0, hi = t->n;
    while (lo < hi) { /* the first entry above the name */
        uint32_t mid = lo + (hi - lo) / 2;
        if (strcmp(name, t->s[mid]) < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    for (int32_t e = (int32_t)lo - 1; e >= 0; e = t->parent[e])
        if (strncmp(name, t->s[e], t->len[e]) == 0)
            return t->no[e];
    return 0;
}

int tlist_find(const tlist *t, const char *prefix, uint32_t *no)
{
    if (!prefix) {
        *no = 0;
        return 0;
    }
    uint32_t lo = 0, hi = t->n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int c = strcmp(prefix, t->s[mid]);
        if (c == 0) {
            *no = t->no[mid];
            return 0;
        }
        if (c < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return TSDB_ERR_NOTFOUND;
}

int eng_set_tenants(eng *db, const char *const *prefixes, size_t n)
{
    if (db->opt.stats < 0)
        return 0;
    tlist *nl;
    int rc = tlist_make(prefixes, n, &nl);
    if (rc)
        return rc;
    pthread_mutex_lock(&db->tl_mu);
    tlist *old = db->tl;
    db->tl = nl;
    pthread_mutex_unlock(&db->tl_mu);
    tlist_unref(old);
    struct estats *es = atomic_load_explicit(&db->stats, memory_order_acquire);
    if (es) {
        pthread_mutex_lock(&es->mu);
        pthread_cond_signal(&es->cv);
        pthread_mutex_unlock(&es->mu);
    }
    return 0;
}

/* ------------------------------------------------------------------------------ state of one volume */

void tenants_grow_to(struct estats *es, uint32_t maxno)
{
    size_t n = (size_t)maxno + 1;
    if (n <= es->ttn)
        return;
    pthread_mutex_lock(&es->mu);
    ttot *tt = realloc(es->tt, n * sizeof *tt);
    thll *hll = realloc(es->hll, n * sizeof *hll);
    if (tt)
        es->tt = tt;
    if (hll)
        es->hll = hll;
    if (tt && hll) {
        memset(es->tt + es->ttn, 0, (n - es->ttn) * sizeof *tt);
        memset(es->hll + es->ttn, 0, (n - es->ttn) * sizeof *hll);
        es->ttn = n;
    }
    pthread_mutex_unlock(&es->mu);
    if (es->ttn != n) {
        tenants_fail(es); /* out of memory: the numbers above the old size are not counted */
        return;
    }
    tent *sc = realloc(es->sc, n * sizeof *sc), *sc2 = realloc(es->sc2, n * sizeof *sc2);
    uint32_t *t1 = realloc(es->touch, n * sizeof *t1), *t2 = realloc(es->touch2, n * sizeof *t2);
    if (sc)
        es->sc = sc;
    if (sc2)
        es->sc2 = sc2;
    if (t1)
        es->touch = t1;
    if (t2)
        es->touch2 = t2;
    if (!(sc && sc2 && t1 && t2)) {
        tenants_fail(es); /* scratch the size of the tenant table: the walk cannot go on without it */
        return;
    }
    memset(es->sc + es->scn, 0, (n - es->scn) * sizeof *sc);
    memset(es->sc2 + es->scn, 0, (n - es->scn) * sizeof *sc2);
    es->scn = n;
}

void tenants_init(struct estats *es)
{
    eng *db = es->db;
    pthread_mutex_lock(&db->tl_mu);
    es->tl = tlist_ref(db->tl);
    es->applied = db->tl; /* NULL: no list was ever set (the empty one below stands in) */
    pthread_mutex_unlock(&db->tl_mu);
    es->tl_gen = 1;
    es->tbase = eng_series_count(db);
    if (!es->tl && tlist_make(NULL, 0, &es->tl) != 0) {
        es->tl = NULL;
        tenants_fail(es);
        return;
    }
    tenants_grow_to(es, es->tl->maxno);
}

static bool tdead(const struct estats *es) { return atomic_load_explicit(&es->tdead, memory_order_acquire); }

/* Out of memory on the tenant side. Statistics never fail the engine: the tenant numbers of this volume stop where
 * they are and read PARTIAL from now on (the sweeper frees the vectors at its next step; a reopen counts again).
 * Every entry point of this file returns at once while tdead is set, so nothing is added or subtracted half way
 * afterwards. */
void tenants_fail(struct estats *es)
{
    if (!atomic_exchange(&es->tdead, true))
        fprintf(stderr, "tsdb: statistics: out of memory: the tenant totals of this volume stop here (PARTIAL) "
                "until it is opened again\n");
}

/* The sweeper, once tdead is set: the partition vectors and the pending moves are not needed any more. */
static void tenants_release(struct estats *es)
{
    for (size_t b = 0; b < es->phtn; b++)
        for (pvec *pv = es->pht[b], *nx; pv; pv = nx) {
            nx = pv->next;
            free(pv->v);
            free(pv);
        }
    free(es->pht);
    es->pht = NULL;
    es->phtn = es->npv = 0;
    es->qhead = es->qtail = NULL;
    es->moving = 0;
    free(es->moves);
    es->moves = NULL;
    es->nmoves = 0;
    free(es->mids);
    free(es->mwid);
    free(es->mgr);
    es->mids = NULL;
    es->mwid = NULL;
    es->mgr = NULL;
    es->nmgr = 0;
    pthread_mutex_lock(&es->mu);
    es->oldest_stale_ns = 0;
    pthread_mutex_unlock(&es->mu);
}

void tenants_free(struct estats *es)
{
    for (size_t i = 0; i < es->ntp; i++)
        free(es->tp[i].v);
    free(es->tp);
    for (size_t b = 0; b < es->phtn; b++)
        for (pvec *pv = es->pht[b], *nx; pv; pv = nx) {
            nx = pv->next;
            free(pv->v);
            free(pv);
        }
    free(es->pht);
    free(es->moves);
    free(es->mids);
    free(es->mwid);
    free(es->mgr);
    for (size_t i = 0; i < es->ttn; i++) {
        free(es->hll[i].reg[0]);
        free(es->hll[i].reg[1]);
    }
    free(es->tt);
    free(es->hll);
    free(es->sc);
    free(es->sc2);
    free(es->touch);
    free(es->touch2);
    tlist_unref(es->tl);
}

uint32_t tenant_of(struct estats *es, uint32_t id)
{
    if (id >= es->tfront)
        return 0;
    const tpage *pg = &es->tp[id >> 16];
    return pg->v ? pg->v[id & 0xffff] : pg->one;
}

static void map_set(struct estats *es, uint32_t id, uint32_t tn)
{
    tpage *pg = &es->tp[id >> 16];
    uint32_t idx = id & 0xffff;
    if (!pg->v) {
        if (idx == 0 && id >= es->tfront) { /* the first id of a new page */
            pg->one = (uint16_t)tn;
            return;
        }
        if (pg->one == tn)
            return; /* a new id joins the page's one tenant, or a move that changes nothing */
        uint16_t *v = alloc_refused(65536 * sizeof *v) ? NULL : malloc(65536 * sizeof *v);
        if (!v) {
            tenants_fail(es); /* the map cannot be left half made: nothing is set */
            return;
        }
        uint32_t lo = id & ~0xffffu;
        uint32_t known = es->tfront > lo ? (uint32_t)MIN((uint64_t)65536, (uint64_t)es->tfront - lo) : 0; /* assigned */
        for (uint32_t i = 0; i < known; i++)
            v[i] = pg->one;
        memset(v + known, 0, (65536 - known) * sizeof *v);
        pg->v = v;
        atomic_fetch_add(&es->tmap_bytes, 65536 * sizeof *v);
    }
    pg->v[idx] = (uint16_t)tn;
}

void tenants_extend(struct estats *es, uint32_t nseries)
{
    if (nseries <= es->tfront || tdead(es))
        return;
    size_t need = ((size_t)nseries + 65535) >> 16;
    if (need > es->ntp) {
        tpage *tp = realloc(es->tp, need * sizeof *tp);
        if (!tp) {
            tenants_fail(es);
            return;
        }
        memset(tp + es->ntp, 0, (need - es->ntp) * sizeof *tp);
        atomic_fetch_add(&es->tmap_bytes, (need - es->ntp) * sizeof *tp);
        es->tp = tp;
        es->ntp = need;
    }
    uint16_t *buf = malloc(65536 * sizeof *buf);
    if (!buf) {
        tenants_fail(es);
        return;
    }
    while (es->tfront < nseries && !tdead(es)) {
        uint32_t lo = es->tfront, hi = (uint32_t)MIN((uint64_t)nseries, (uint64_t)lo + 65536);
        for (uint32_t id = lo; id < hi; id++) { /* the names first: the dictionary's lock never meets es->mu */
            const char *name = eng_series_name(es->db, id);
            buf[id - lo] = (uint16_t)(name ? tlist_match(es->tl, name) : 0);
        }
        pthread_mutex_lock(&es->mu);
        for (uint32_t id = lo; id < hi; id++) {
            uint32_t tn = buf[id - lo];
            map_set(es, id, tn);
            es->tfront = id + 1;
            if (tn < es->ttn) {
                es->tt[tn].series++;
                es->tt[tn].created += id >= es->tbase;
            }
        }
        pthread_mutex_unlock(&es->mu);
    }
    free(buf);
}

uint64_t tenants_map_bytes(struct estats *es) { return atomic_load(&es->tmap_bytes); }

/* ----------------------------------------------------------------------------------------- scratch */

void tenant_scratch_clear(struct estats *es)
{
    for (size_t i = 0; i < es->ntouch; i++)
        memset(&es->sc[es->touch[i]], 0, sizeof(tent));
    es->ntouch = 0;
}

void tenant_walk_run(struct estats *es, uint32_t id, uint64_t cnt, uint64_t nulls, uint64_t logical, uint64_t ts,
                     uint64_t val, uint64_t index, uint64_t granules)
{
    if (!granules || tdead(es))
        return;
    uint32_t t = tenant_of(es, id);
    if (t >= es->scn)
        t = 0;
    tent *e = &es->sc[t];
    if (!e->granules)
        es->touch[es->ntouch++] = t;
    e->rows += cnt;
    e->nulls += nulls;
    e->logical += logical;
    e->ts += ts;
    e->val += val;
    e->index += index;
    e->granules += granules;
}

static int u32_cmp(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* ------------------------------------------------------------------------------ partition vectors */

static void tent_add(tent *a, const tent *b, int sign)
{
    if (sign > 0) {
        a->rows += b->rows, a->nulls += b->nulls, a->logical += b->logical, a->ts += b->ts, a->val += b->val;
        a->index += b->index, a->granules += b->granules;
    } else {
        a->rows -= b->rows, a->nulls -= b->nulls, a->logical -= b->logical, a->ts -= b->ts, a->val -= b->val;
        a->index -= b->index, a->granules -= b->granules;
    }
}

static void tt_add(ttot *t, const tent *x, int sign)
{
    if (sign > 0) {
        t->rows += x->rows, t->nulls += x->nulls, t->logical += x->logical, t->ts += x->ts, t->val += x->val;
        t->index += x->index, t->granules += x->granules;
    } else {
        t->rows -= x->rows, t->nulls -= x->nulls, t->logical -= x->logical, t->ts -= x->ts, t->val -= x->val;
        t->index -= x->index, t->granules -= x->granules;
    }
}

static pvec *pv_lookup(struct estats *es, int64_t key)
{
    if (!es->phtn)
        return NULL;
    for (pvec *pv = es->pht[(uint64_t)key * 0x9E3779B97F4A7C15ull >> 40 & (es->phtn - 1)]; pv; pv = pv->next)
        if (pv->key == key)
            return pv;
    return NULL;
}

static pvec *pv_get(struct estats *es, int64_t key)
{
    pvec *pv = pv_lookup(es, key);
    if (pv)
        return pv;
    if (es->npv >= es->phtn) {
        size_t n = es->phtn ? es->phtn * 2 : 256;
        pvec **t = calloc(n, sizeof *t);
        if (!t)
            return NULL;
        for (size_t b = 0; b < es->phtn; b++)
            for (pvec *q = es->pht[b], *nx; q; q = nx) {
                nx = q->next;
                size_t k = (uint64_t)q->key * 0x9E3779B97F4A7C15ull >> 40 & (n - 1);
                q->next = t[k];
                t[k] = q;
            }
        free(es->pht);
        es->pht = t;
        es->phtn = n;
    }
    pv = calloc(1, sizeof *pv);
    if (!pv)
        return NULL;
    pv->key = key;
    pv->tgen = es->tl_gen;
    size_t k = (uint64_t)key * 0x9E3779B97F4A7C15ull >> 40 & (es->phtn - 1);
    pv->next = es->pht[k];
    es->pht[k] = pv;
    es->npv++;
    return pv;
}

static void q_remove(struct estats *es, pvec *pv)
{
    if (!pv->inq)
        return;
    pvec *prev = NULL;
    for (pvec *q = es->qhead; q; prev = q, q = q->qnext)
        if (q == pv) {
            if (prev)
                prev->qnext = q->qnext;
            else
                es->qhead = q->qnext;
            if (es->qtail == q)
                es->qtail = prev;
            break;
        }
    pv->inq = false;
    pv->qnext = NULL;
}

static void mark_stale(struct estats *es, pvec *pv)
{
    if (!pv->stale) {
        pv->stale = true;
        pv->stale_ns = stats_mono_ns();
    }
    if (!pv->inq) {
        pv->inq = true;
        pv->qnext = NULL;
        if (es->qtail)
            es->qtail->qnext = pv;
        else
            es->qhead = pv;
        es->qtail = pv;
    }
}

static void pv_free(struct estats *es, pvec *pv)
{
    q_remove(es, pv);
    if (pv->tgen != es->tl_gen && es->moving)
        es->moving--;
    size_t k = (uint64_t)pv->key * 0x9E3779B97F4A7C15ull >> 40 & (es->phtn - 1);
    for (pvec **pp = &es->pht[k]; *pp; pp = &(*pp)->next)
        if (*pp == pv) {
            *pp = pv->next;
            break;
        }
    es->npv--;
    free(pv->v);
    free(pv);
}

static tvec_e *pv_find(const pvec *pv, uint32_t t)
{
    uint32_t lo = 0, hi = pv->nv;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (pv->v[mid].t < t)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < pv->nv && pv->v[lo].t == t ? &pv->v[lo] : NULL;
}

/* The split in sc[touch[0..nt)] added to (sign +1) or taken from (-1) the vector and the tenant totals. Taking from a
 * tenant the vector does not have is skipped, never done: nothing is subtracted that was not added. */
static void pv_apply(struct estats *es, pvec *pv, const tent *sc, uint32_t *touch, size_t nt, int sign)
{
    if (tdead(es))
        return;
    qsort(touch, nt, sizeof *touch, u32_cmp);
    uint32_t *fresh = NULL;
    size_t nf = 0;
    uint32_t *done = malloc((nt ? nt : 1) * sizeof *done);
    size_t nd = 0;
    if (sign > 0)
        fresh = malloc((nt ? nt : 1) * sizeof *fresh);
    if (!done || (sign > 0 && !fresh)) { /* a partition that misses a tenant would drift: nothing is applied */
        free(done);
        free(fresh);
        tenants_fail(es);
        return;
    }
    for (size_t i = 0; i < nt; i++) {
        uint32_t t = touch[i];
        tvec_e *e = pv_find(pv, t);
        if (e) {
            tent_add(&e->v, &sc[t], sign);
            done[nd++] = t;
        } else if (sign > 0) {
            fresh[nf++] = t;
        }
    }
    if (nf) {
        if (pv->nv + nf > pv->cap) {
            uint32_t cap = MAX(pv->cap * 2, pv->nv + (uint32_t)nf);
            tvec_e *v = realloc(pv->v, cap * sizeof *v);
            if (!v) { /* a vector that misses a tenant would drift: the totals stop here */
                free(done);
                free(fresh);
                tenants_fail(es);
                return;
            }
            pv->v = v;
            pv->cap = cap;
        }
        long i = (long)pv->nv - 1, j = (long)nf - 1, k = (long)(pv->nv + nf) - 1;
        while (j >= 0) { /* both ascending: merged from the back */
            if (i >= 0 && pv->v[i].t > fresh[j]) {
                pv->v[k--] = pv->v[i--];
            } else {
                pv->v[k].t = fresh[j];
                pv->v[k].pad = 0;
                pv->v[k].v = sc[fresh[j]];
                k--;
                j--;
            }
        }
        pv->nv += (uint32_t)nf;
    }
    pthread_mutex_lock(&es->mu);
    for (size_t i = 0; i < nd; i++)
        tt_add(&es->tt[done[i]], &sc[done[i]], sign);
    for (size_t i = 0; i < nf; i++)
        tt_add(&es->tt[fresh[i]], &sc[fresh[i]], sign);
    pthread_mutex_unlock(&es->mu);
    free(done);
    free(fresh);
}

void pv_part_added(struct estats *es, const prec *r, bool fresh)
{
    if (tdead(es))
        return;
    pvec *pv = pv_get(es, r->partition);
    if (!pv) {
        tenants_fail(es);
        return; /* out of memory: the partition is not tracked, and its tenants would miss this part */
    }
    pv->nrec++;
    /* a merge output is not added: its partition is counted again. A vector in terms of an older list is not added
     * to either (the split is in terms of the new map): the recount puts it right. */
    if ((fresh && r->level > 0) || pv->tgen != es->tl_gen) {
        mark_stale(es, pv);
        return;
    }
    pv_apply(es, pv, es->sc, es->touch, es->ntouch, +1);
}

static void pv_drop_whole(struct estats *es, pvec *pv)
{
    pthread_mutex_lock(&es->mu);
    for (uint32_t i = 0; i < pv->nv; i++) {
        uint32_t t = pv->v[i].t;
        tt_add(&es->tt[t], &pv->v[i].v, -1);
        es->tt[t].retdrop_rows += pv->v[i].v.rows;
    }
    pthread_mutex_unlock(&es->mu);
    pv_free(es, pv);
}

void pv_part_removed(struct estats *es, const prec *r, bool dropped)
{
    if (tdead(es))
        return;
    pvec *pv = pv_lookup(es, r->partition);
    if (!pv)
        return;
    if (pv->nrec)
        pv->nrec--;
    if (dropped && pv->nrec == 0)
        pv_drop_whole(es, pv); /* retired by retention or drop_before and nothing of it left: no walk */
    else
        mark_stale(es, pv);
}

/* The vector replaced by the one in sc2[touch2[0..n)]: the totals move by the difference, and the rows a tenant lost
 * are what merges (and deletes, overwrites and retention inside the partition) removed. */
static void pv_replace(struct estats *es, pvec *pv, const tent *sc, uint32_t *touch, size_t n)
{
    if (tdead(es))
        return;
    qsort(touch, n, sizeof *touch, u32_cmp);
    tvec_e *nv = malloc((n ? n : 1) * sizeof *nv);
    if (!nv) { /* the vector stays as it was: the totals stop here */
        tenants_fail(es);
        return;
    }
    for (size_t i = 0; i < n; i++) {
        nv[i].t = touch[i];
        nv[i].pad = 0;
        nv[i].v = sc[touch[i]];
    }
    pthread_mutex_lock(&es->mu);
    size_t i = 0, j = 0;
    while (i < pv->nv || j < n) {
        if (j >= n || (i < pv->nv && pv->v[i].t < nv[j].t)) { /* a tenant with nothing left here */
            ttot *t = &es->tt[pv->v[i].t];
            tt_add(t, &pv->v[i].v, -1);
            t->removed_rows += pv->v[i].v.rows;
            i++;
        } else if (i >= pv->nv || nv[j].t < pv->v[i].t) {
            tt_add(&es->tt[nv[j].t], &nv[j].v, +1);
            j++;
        } else {
            ttot *t = &es->tt[nv[j].t];
            tt_add(t, &pv->v[i].v, -1);
            tt_add(t, &nv[j].v, +1);
            t->removed_rows += pv->v[i].v.rows > nv[j].v.rows ? pv->v[i].v.rows - nv[j].v.rows : 0;
            i++;
            j++;
        }
    }
    pthread_mutex_unlock(&es->mu);
    free(pv->v);
    pv->v = nv;
    pv->nv = pv->cap = (uint32_t)n;
}

/* ---------------------------------------------------------------------------------------- recounts */

static void pv_converted(struct estats *es, pvec *pv)
{
    if (pv->tgen != es->tl_gen) {
        pv->tgen = es->tl_gen;
        if (es->moving)
            es->moving--;
    }
}

static void recompute_pv(struct estats *es, pvec *pv)
{
    eng *db = es->db;
    int64_t key = pv->key;
    part **ps;
    size_t np;
    if (!partition_cut(es, key, &ps, &np))
        return; /* out of memory: it stays stale and is tried again */
    pv = pv_lookup(es, key); /* the events just handled may have retired it */
    if (!pv) {
        for (size_t i = 0; i < np; i++)
            part_unref(ps[i]);
        free(ps);
        return;
    }
    tent *acc = es->sc2;
    size_t nacc = 0;
    uint32_t *at = es->touch2;
    for (size_t i = 0; i < np && !stats_stopping(es); i++) {
        part *p = ps[i];
        pthread_mutex_lock(&es->mu);
        prec *rec = rec_find(es, p);
        bool known = rec && rec_matches(rec, p);
        pthread_mutex_unlock(&es->mu);
        if (rec && !known) { /* the address of a part that is gone */
            pthread_mutex_lock(&es->mu);
            prec *gone = rec_remove(es, p);
            pthread_mutex_unlock(&es->mu);
            if (gone)
                rec_discard(es, gone, false);
            pv = pv_lookup(es, key);
            if (!pv)
                break;
            rec = NULL;
        }
        throttle_wait(es);
        prec tmp;
        if (part_walk(es, p, &tmp, WALK_SPLIT) != 0)
            continue; /* its series are not in the dictionary yet (a follower ahead): its own add counts it */
        throttle_pay(es, p->index_len);
        for (size_t k = 0; k < es->ntouch; k++) {
            uint32_t t = es->touch[k];
            if (!acc[t].granules)
                at[nacc++] = t;
            tent_add(&acc[t], &es->sc[t], +1);
        }
        if (!known) { /* a listed part the events never delivered (a lost event, a put-off part): counted now */
            prec *nr = malloc(sizeof *nr);
            if (nr) {
                *nr = tmp;
                nr->mcounted = true;
                pthread_mutex_lock(&es->mu);
                nr->gen = es->gen;
                tot_apply(es, nr, true);
                size_t before = es->nrec;
                rec_insert(es, nr);
                if (es->nrec == before) {
                    tot_apply(es, nr, false);
                    free(nr);
                } else {
                    pv->nrec++;
                }
                pthread_mutex_unlock(&es->mu);
            }
        } else if (p->level > 0 && !rec->mcounted) { /* a merge output seen for the first time */
            rec->mcounted = true;
            pthread_mutex_lock(&es->mu);
            for (size_t k = 0; k < es->ntouch; k++)
                es->tt[es->touch[k]].merged_rows += es->sc[es->touch[k]].rows;
            pthread_mutex_unlock(&es->mu);
        }
    }
    if (!stats_stopping(es)) {
        pv_replace(es, pv, acc, at, nacc);
        pv->stale = false;
        pv->last_ns = stats_mono_ns();
        q_remove(es, pv);
        pv_converted(es, pv);
        if (!pv->nrec && !pv->nv)
            pv_free(es, pv);
    }
    for (size_t k = 0; k < nacc; k++)
        memset(&acc[at[k]], 0, sizeof(tent));
    for (size_t i = 0; i < np; i++)
        part_unref(ps[i]);
    free(ps);
    (void)db;
}

/* ------------------------------------------------------------------------------------ list changes */

static int move_cmp(const void *a, const void *b)
{
    const struct move *x = a, *y = b;
    if (x->from != y->from)
        return x->from < y->from ? -1 : 1;
    if (x->to != y->to)
        return x->to < y->to ? -1 : 1;
    return (x->id > y->id) - (x->id < y->id);
}

static void finish_moves(struct estats *es)
{
    pthread_mutex_lock(&es->mu);
    for (size_t i = 0; i < es->nmoves; i++) {
        if (es->moves[i].from < es->ttn)
            es->tt[es->moves[i].from].partial = false;
        if (es->moves[i].to < es->ttn)
            es->tt[es->moves[i].to].partial = false;
    }
    pthread_mutex_unlock(&es->mu);
    free(es->moves);
    es->moves = NULL;
    es->nmoves = 0;
    free(es->mids);
    free(es->mwid);
    free(es->mgr);
    es->mids = NULL;
    es->mwid = NULL;
    es->mgr = NULL;
    es->nmgr = 0;
}

/* After the sort: the groups, and the ids and widths of the moves as the groups slice them. On failure they stay
 * NULL, and move_pv counts the partitions again. The widths are looked up here, once per change. */
static void moves_prepare(struct estats *es)
{
    size_t ng = 0;
    for (size_t i = 0; i < es->nmoves; i++)
        if (!i || es->moves[i].from != es->moves[i - 1].from || es->moves[i].to != es->moves[i - 1].to)
            ng++;
    uint32_t *ids = malloc(es->nmoves * sizeof *ids);
    uint8_t *wid = malloc(es->nmoves);
    struct mgroup *gr = malloc(ng * sizeof *gr);
    if (!ids || !wid || !gr) {
        free(ids);
        free(wid);
        free(gr);
        return;
    }
    size_t k = 0;
    for (size_t i = 0; i < es->nmoves; i++) {
        const struct move *m = &es->moves[i];
        ids[i] = m->id;
        int type = eng_series_type(es->db, m->id);
        wid[i] = (uint8_t)(type >= 0 ? type_width(type) : 4);
        if (!i || m->from != es->moves[i - 1].from || m->to != es->moves[i - 1].to)
            gr[k++] = (struct mgroup){i, 0, m->from, m->to};
        gr[k - 1].n++;
    }
    es->mids = ids;
    es->mwid = wid;
    es->mgr = gr;
    es->nmgr = ng;
}

void tenants_apply_list(struct estats *es)
{
    eng *db = es->db;
    if (tdead(es))
        return;
    pthread_mutex_lock(&db->tl_mu);
    tlist *nt = db->tl;
    if (!nt || nt == es->tl) {
        pthread_mutex_unlock(&db->tl_mu);
        return;
    }
    tlist_ref(nt);
    pthread_mutex_unlock(&db->tl_mu);
    tlist *old = es->tl;
    uint32_t top = MAX(old->maxno, nt->maxno);
    bool *in_old = calloc(top + 1, 1), *in_new = calloc(top + 1, 1), *aff = calloc(top + 1, 1);
    if (!in_old || !in_new || !aff) {
        free(in_old);
        free(in_new);
        free(aff);
        tlist_unref(nt);
        return; /* tried again at the next tick */
    }
    for (uint32_t i = 0; i < old->n; i++)
        in_old[old->no[i]] = true;
    for (uint32_t i = 0; i < nt->n; i++)
        in_new[nt->no[i]] = true;
    bool changed = false;
    for (uint32_t i = 0; i < old->n; i++)
        if (!in_new[old->no[i]]) {
            aff[old->no[i]] = true; /* its series go to the tenant that now holds them */
            changed = true;
        }
    for (uint32_t i = 0; i < nt->n; i++)
        if (!in_old[nt->no[i]]) { /* a new prefix takes its series from the tenant that had them: its parent */
            aff[nt->parent[i] >= 0 ? nt->no[nt->parent[i]] : 0] = true;
            changed = true;
        }
    free(in_old);
    free(in_new);
    if (!changed) { /* the same prefixes again */
        pthread_mutex_lock(&es->mu);
        es->tl = nt;
        es->applied = nt;
        pthread_mutex_unlock(&es->mu);
        tlist_unref(old);
        free(aff);
        return;
    }
    tenants_grow_to(es, nt->maxno);
    if (es->moving) { /* a move is half done: the partitions that did not take it are counted again instead */
        for (size_t b = 0; b < es->phtn; b++)
            for (pvec *pv = es->pht[b]; pv; pv = pv->next)
                if (pv->tgen != es->tl_gen)
                    mark_stale(es, pv);
    }
    finish_moves(es); /* an unfinished earlier change: its tenants are no longer waiting for it */
    es->tl = nt;
    es->tl_gen++;
    /* the ids that change tenant: only those of the tenants a change touches are looked at, and a page of one tenant
     * that is not among them is skipped whole */
    struct move *mv = NULL;
    size_t nm = 0, cap = 0;
    int64_t *ds = calloc(top + 1, sizeof *ds); /* series count change per tenant */
    bool oom = !ds;
    for (size_t pg = 0; pg < es->ntp && !oom; pg++) {
        uint32_t lo = (uint32_t)(pg << 16), hi = (uint32_t)MIN((uint64_t)es->tfront, (uint64_t)lo + 65536);
        if (lo >= hi)
            break;
        if (!es->tp[pg].v && !aff[es->tp[pg].one])
            continue;
        for (uint32_t id = lo; id < hi; id++) {
            uint32_t t = tenant_of(es, id);
            if (!aff[t])
                continue;
            const char *name = eng_series_name(db, id);
            uint32_t n2 = name ? tlist_match(nt, name) : 0;
            if (n2 == t)
                continue;
            if (nm == cap) {
                cap = cap ? cap * 2 : 1024;
                struct move *v = alloc_refused(cap * sizeof *v) ? NULL : realloc(mv, cap * sizeof *v);
                if (!v) { /* the list of moved series cannot be kept: the totals stop here */
                    oom = true;
                    break;
                }
                mv = v;
            }
            mv[nm++] = (struct move){id, (uint16_t)t, (uint16_t)n2};
            map_set(es, id, n2);
            ds[t]--;
            ds[n2]++;
        }
    }
    free(aff);
    if (oom || tdead(es)) { /* a map page that could not be made counts too */
        free(mv);
        free(ds);
        tlist_unref(old);
        tenants_fail(es);
        return;
    }
    pthread_mutex_lock(&es->mu);
    es->applied = nt; /* the tenants a change touches are PARTIAL from here (below) until their moves are done */
    for (uint32_t t = 0; t <= top && t < es->ttn; t++)
        es->tt[t].series += (uint64_t)ds[t];
    for (size_t i = 0; i < nm; i++) {
        es->tt[mv[i].from].partial = true;
        es->tt[mv[i].to].partial = true;
    }
    pthread_mutex_unlock(&es->mu);
    free(ds);
    tlist_unref(old);
    if (nm) {
        qsort(mv, nm, sizeof *mv, move_cmp);
        es->moves = mv;
        es->nmoves = nm;
        es->moving = es->npv;
        es->mv_cursor = 0;
        moves_prepare(es);
    } else {
        free(mv);
        for (size_t b = 0; b < es->phtn; b++)
            for (pvec *pv = es->pht[b]; pv; pv = pv->next)
                pv->tgen = es->tl_gen;
        es->moving = 0;
    }
}

/* The entries of a part that belong to the sorted ids[0..g): their column sums into *x. The cheaper of a binary
 * search per id and one pass over the run table. Returns the index probes made. */
static unsigned ceil_lg(uint64_t n)
{
    unsigned lg = 1;
    while (((uint64_t)1 << lg) < n)
        lg++;
    return lg;
}

/* The ids[0..g) (sorted) that fall inside a part's series range: [*lo, *hi). */
static void ids_in_range(const gix *gx, const uint32_t *ids, size_t g, size_t *lo, size_t *hi)
{
    size_t a = 0, b = g;
    while (a < b) { /* the first id >= the part's first series */
        size_t m = a + (b - a) / 2;
        if (ids[m] < gx->rser[0])
            a = m + 1;
        else
            b = m;
    }
    size_t c = a, d = g;
    while (c < d) { /* the first id > its last */
        size_t m = c + (d - c) / 2;
        if (ids[m] <= gx->rser[gx->nruns - 1])
            c = m + 1;
        else
            d = m;
    }
    *lo = a;
    *hi = c;
}

enum { IDS_MERGE, IDS_PER_ID, IDS_PER_RUN };

/* How the moved ids (g of them, already cut to the part's range) are best found in a part's run table, and what it
 * costs in index probes: one merge pass over both, a search of the run table per id, or a search of the ids per run.
 * The last is what keeps a part of a few series cheap when millions of series moved. */
static uint64_t ids_plan(const gix *gx, size_t g, int *how)
{
    uint64_t lg = ceil_lg(gx->nruns), lgg = ceil_lg(g + 1);
    uint64_t m = (uint64_t)gx->nruns + g, pi = (uint64_t)g * lg, pr = (uint64_t)gx->nruns * lgg;
    *how = IDS_MERGE;
    uint64_t best = m;
    if (pi < best) {
        best = pi;
        *how = IDS_PER_ID;
    }
    if (pr < best) {
        best = pr;
        *how = IDS_PER_RUN;
    }
    return best;
}

/* What moving the ids out of this part costs in index bytes at the least (the probes), for the choice between moving
 * a partition and counting it again. */
static uint64_t part_ids_cost(const part *p, const uint32_t *ids, size_t g)
{
    const gix *gx = &p->ix;
    if (!gx->nruns || !g)
        return 8;
    size_t lo, hi;
    ids_in_range(gx, ids, g, &lo, &hi);
    if (hi <= lo)
        return 8;
    int how;
    return ids_plan(gx, hi - lo, &how) * 8;
}

static void entries_sum(const gix *gx, uint32_t a, uint32_t b, uint8_t wid, tent *x)
{
    uint64_t cnt = 0;
    for (uint32_t e = a; e < b; e++) {
        cnt += gix_get(gx, e, GF_COUNT);
        x->nulls += gix_get(gx, e, GF_NULLS);
        x->ts += gix_get(gx, e, GF_TSLEN);
        x->val += gix_get(gx, e, GF_VALLEN);
    }
    x->rows += cnt;
    x->logical += cnt * (8 + wid);
    x->index += (uint64_t)(b - a) * gx->stride + 8;
    x->granules += b - a;
}

/* Returns the index bytes read (the throttle's price). */
static uint64_t part_ids_sum(const part *p, const uint32_t *ids, size_t g, const uint8_t *wid, tent *x)
{
    const gix *gx = &p->ix;
    if (!gx->nruns || !g)
        return 8;
    size_t lo, hi;
    ids_in_range(gx, ids, g, &lo, &hi);
    if (hi <= lo)
        return 8;
    ids += lo;
    wid += lo;
    g = hi - lo;
    int how;
    uint64_t bytes = ids_plan(gx, g, &how) * 8;
    if (how == IDS_PER_RUN) {
        for (uint32_t k = 0; k < gx->nruns; k++) {
            size_t a = 0, b = g;
            while (a < b) {
                size_t m = a + (b - a) / 2;
                if (ids[m] < gx->rser[k])
                    a = m + 1;
                else
                    b = m;
            }
            if (a >= g || ids[a] != gx->rser[k])
                continue;
            uint32_t s = gix_rstart(gx, k), e = k + 1 < gx->nruns ? gix_rstart(gx, k + 1) : gx->n;
            if (e > s) {
                entries_sum(gx, s, e, wid[a], x);
                bytes += (uint64_t)(e - s) * gx->stride + 8;
            }
        }
        return bytes;
    }
    uint32_t rc = 0;
    for (size_t i = 0; i < g; i++) {
        uint32_t a, b;
        if (how == IDS_MERGE) {
            while (rc < gx->nruns && gx->rser[rc] < ids[i])
                rc++;
            a = rc < gx->nruns ? gix_rstart(gx, rc) : gx->n;
            b = rc < gx->nruns && gx->rser[rc] == ids[i] ? (rc + 1 < gx->nruns ? gix_rstart(gx, rc + 1) : gx->n) : a;
            if (b < a)
                b = a;
        } else {
            gix_run(gx, ids[i], &a, &b);
        }
        if (b <= a)
            continue;
        entries_sum(gx, a, b, wid[i], x);
        bytes += (uint64_t)(b - a) * gx->stride + 8;
    }
    return bytes;
}

/* One partition's share of the series that moved: found in its counted parts (the listed parts that have a record),
 * and moved from one tenant's entry to the other's. Only a partition whose vector is in terms of the previous list and
 * not stale: its vector is then exactly the sum of its records' splits. */
static void move_pv(struct estats *es, pvec *pv)
{
    int64_t key = pv->key;
    part **ps;
    size_t np;
    if (!partition_cut(es, key, &ps, &np))
        return;
    pv = pv_lookup(es, key);
    if (pv && !pv->stale && pv->tgen != es->tl_gen) {
        /* Moving finds the moved series in every part; counting again reads every part's index. Where the first
         * would cost more than the second (parts that hold a few of very many moved series), count again: a list
         * change then costs one walk of the index at most, as the design says. */
        bool *known_part = malloc(np ? np : 1);
        uint64_t mcost = 0, rcost = 0;
        for (size_t i = 0; known_part && i < np; i++) {
            pthread_mutex_lock(&es->mu);
            prec *rec = rec_find(es, ps[i]);
            known_part[i] = rec && rec_matches(rec, ps[i]);
            pthread_mutex_unlock(&es->mu);
            if (!known_part[i] || !es->mids)
                continue;
            rcost += ps[i]->index_len;
            for (size_t gk = 0; gk < es->nmgr && mcost <= rcost; gk++)
                mcost += part_ids_cost(ps[i], es->mids + es->mgr[gk].first, es->mgr[gk].n);
        }
        if (!known_part || !es->mids || mcost > rcost) {
            mark_stale(es, pv);
            pv = NULL;
        }
        for (size_t gk = 0; pv && gk < es->nmgr && !stats_stopping(es); gk++) {
            const struct mgroup *gr = &es->mgr[gk];
            const uint32_t *ids = es->mids + gr->first;
            const uint8_t *wid = es->mwid + gr->first;
            size_t g = gr->n;
            tent x;
            memset(&x, 0, sizeof x);
            for (size_t i = 0; i < np; i++) {
                if (!known_part[i])
                    continue; /* not in the vector (put off): nothing to move */
                throttle_wait(es);
                uint64_t bytes = part_ids_sum(ps[i], ids, g, wid, &x);
                throttle_pay(es, bytes);
            }
            uint32_t from = gr->from, to = gr->to;
            tvec_e *e = pv_find(pv, from);
            if (e && x.granules) {
                tent_add(&e->v, &x, -1);
                es->sc[to] = x; /* the destination goes through pv_apply: a new entry is merged in order */
                uint32_t one = to;
                pv_apply(es, pv, es->sc, &one, 1, +1);
                memset(&es->sc[to], 0, sizeof(tent));
                pthread_mutex_lock(&es->mu);
                tt_add(&es->tt[from], &x, -1);
                pthread_mutex_unlock(&es->mu);
            }
        }
        if (pv && !stats_stopping(es))
            pv_converted(es, pv);
        free(known_part);
    }
    for (size_t i = 0; i < np; i++)
        part_unref(ps[i]);
    free(ps);
}

/* One slice of the tenant side's work. The stale partitions that are due are counted again (due: a settle is waiting,
 * its vector is in terms of an older list, or stats_recompute_s has passed since its last count), and the partitions
 * that have not taken a list change move. True when there is more to do right now. Every partition whose vector is
 * not in terms of the current list is work at once: `moving` counts them. */
#define STEP_UNITS 16
bool tenants_step(struct estats *es, bool forced)
{
    if (tdead(es)) {
        tenants_release(es);
        return false;
    }
    int64_t now = stats_mono_ns();
    size_t units = 0, nq = 0;
    for (pvec *q = es->qhead; q; q = q->qnext)
        nq++;
    pvec **snap = nq ? malloc(nq * sizeof *snap) : NULL;
    nq = 0;
    for (pvec *q = es->qhead; snap && q; q = q->qnext)
        snap[nq++] = q;
    bool due_left = false;
    for (size_t i = 0; i < nq && !stats_stopping(es); i++) {
        pvec *pv = snap[i];
        if (!pv->inq)
            continue; /* counted or freed meanwhile */
        if (!(forced || pv->tgen != es->tl_gen || now - pv->last_ns >= es->recompute_ns))
            continue;
        if (units >= STEP_UNITS) {
            due_left = true;
            break;
        }
        recompute_pv(es, pv);
        units++;
    }
    free(snap);
    while (es->moving && es->nmoves && units < STEP_UNITS && !stats_stopping(es)) {
        if (es->mv_cursor >= es->phtn) {
            es->mv_cursor = 0; /* a full pass; the next call starts another */
            break;
        }
        pvec *found = NULL;
        for (pvec *pv = es->pht[es->mv_cursor]; pv && !found; pv = pv->next)
            if (!pv->stale && pv->tgen != es->tl_gen)
                found = pv;
        if (!found) {
            es->mv_cursor++;
            continue;
        }
        move_pv(es, found);
        units++;
    }
    if (!es->moving && es->nmoves)
        finish_moves(es);
    int64_t oldest = 0;
    for (pvec *q = es->qhead; q; q = q->qnext)
        if (q->stale && (!oldest || q->stale_ns < oldest))
            oldest = q->stale_ns;
    pthread_mutex_lock(&es->mu);
    es->oldest_stale_ns = oldest;
    pthread_mutex_unlock(&es->mu);
    if (stats_stopping(es))
        return false;
    return due_left || units >= STEP_UNITS || es->moving > 0 || (forced && es->qhead);
}

/* ------------------------------------------------------------------- ingest, HLL, hot writes */

static uint64_t mix64(uint64_t x)
{
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}

static void hll_add(thll *h, int64_t hour, uint64_t id)
{
    int s = (int)(hour & 1);
    if (h->win[s] != hour || !h->reg[s]) {
        if (!h->reg[s])
            h->reg[s] = calloc(HLL_M, 1);
        else
            memset(h->reg[s], 0, HLL_M);
        if (!h->reg[s])
            return;
        h->win[s] = hour;
    }
    uint64_t x = mix64(id);
    uint32_t idx = (uint32_t)(x >> 54);
    uint64_t w = (x << 10) | (1ull << 9);
    uint8_t rank = (uint8_t)(__builtin_clzll(w) + 1);
    if (rank > h->reg[s][idx])
        h->reg[s][idx] = rank;
}

static uint64_t hll_estimate(const thll *h, int64_t hour)
{
    int s = (int)(hour & 1);
    if (!h->reg[s] || h->win[s] != hour)
        return 0;
    double sum = 0;
    unsigned zeros = 0;
    for (int i = 0; i < HLL_M; i++) {
        sum += ldexp(1.0, -h->reg[s][i]);
        zeros += h->reg[s][i] == 0;
    }
    double m = HLL_M, alpha = 0.7213 / (1 + 1.079 / m);
    double e = alpha * m * m / sum;
    if (e <= 2.5 * m && zeros)
        e = m * log(m / zeros);
    return (uint64_t)(e + 0.5);
}

void tenants_ingest(struct estats *es, part *p, const prec *r)
{
    (void)r;
    int64_t hour = hot_now_ms() / 3600000;
    int64_t epoch = hot_now_ms() / HOT_PERIOD_MS;
    const gix *x = &p->ix;
    pthread_mutex_lock(&es->mu);
    for (size_t k = 0; k < es->ntouch; k++) {
        uint32_t t = es->touch[k];
        es->tt[t].ing_rows += es->sc[t].rows;
        es->tt[t].ing_bytes += es->sc[t].logical;
    }
    pthread_mutex_unlock(&es->mu);
    for (uint32_t k0 = 0; k0 < x->nruns && !stats_stopping(es); k0 += 4096) {
        uint32_t k1 = (uint32_t)MIN((uint64_t)x->nruns, (uint64_t)k0 + 4096);
        pthread_mutex_lock(&es->mu);
        for (uint32_t k = k0; k < k1; k++) {
            uint32_t t = tenant_of(es, x->rser[k]);
            if (t < es->ttn && es->runcnt[k])
                hll_add(&es->hll[t], hour, x->rser[k]);
        }
        pthread_mutex_unlock(&es->mu);
        for (uint32_t k = k0; k < k1; k++)
            if (es->runcnt[k])
                hotl_add(es->hw, epoch, x->rser[k], es->runcnt[k]);
    }
}

/* ---------------------------------------------------------------------------------- reading */

void tenant_res_add(struct estats *es, uint32_t t, uint64_t bytes, bool add)
{
    if (t >= es->ttn)
        return;
    if (add)
        es->tt[t].resident += bytes;
    else
        es->tt[t].resident -= bytes;
}

void tenants_fill(struct estats *es, uint32_t no, uint64_t v[17], uint32_t *flags, uint64_t *stale_s)
{
    memset(v, 0, 17 * sizeof v[0]);
    *flags = 0;
    *stale_s = 0;
    int64_t hour = hot_now_ms() / 3600000;
    /* The numbers lag a list the handle set while the engine has not applied it yet (its sweeper does at its next
     * tick): they are those of the old list. A failed tenant side (tenants_fail) is PARTIAL for good. The listed
     * parts not yet counted are checked here, from the count alone. */
    eng *db = es->db;
    pthread_mutex_lock(&db->tl_mu);
    const tlist *want = db->tl;
    pthread_mutex_unlock(&db->tl_mu);
    pthread_mutex_lock(&db->lock);
    uint64_t listed = db->nparts;
    pthread_mutex_unlock(&db->lock);
    if (tdead(es))
        *flags |= TSDB_TENANT_PARTIAL;
    pthread_mutex_lock(&es->mu);
    if (want != es->applied || listed != es->nrec)
        *flags |= TSDB_TENANT_PARTIAL;
    if (no < es->ttn) {
        const ttot *t = &es->tt[no];
        v[0] = t->series;
        v[1] = t->created;
        v[2] = hll_estimate(&es->hll[no], hour);
        v[3] = hll_estimate(&es->hll[no], hour - 1);
        v[4] = t->rows;
        v[5] = t->nulls;
        v[6] = t->logical;
        v[7] = t->ts;
        v[8] = t->val;
        v[9] = t->index;
        v[10] = t->granules;
        v[11] = t->ing_rows;
        v[12] = t->ing_bytes;
        v[13] = t->merged_rows;
        v[14] = t->removed_rows;
        v[15] = t->retdrop_rows;
        v[16] = t->resident;
        if (t->partial)
            *flags |= TSDB_TENANT_PARTIAL;
    }
    if (es->oldest_stale_ns) {
        int64_t age = stats_mono_ns() - es->oldest_stale_ns;
        *stale_s = age > 0 ? (uint64_t)age / 1000000000u : 0;
    }
    pthread_mutex_unlock(&es->mu);
}

int eng_tenant_fill(eng *db, uint32_t no, uint64_t v[17], uint32_t *flags, uint64_t *stale_s)
{
    if (db->opt.stats < 0)
        return TSDB_ERR_INVALID;
    stats_start(db, true); /* lazy (stats = 0): the first look starts it */
    struct estats *es = atomic_load_explicit(&db->stats, memory_order_acquire);
    if (!es)
        return TSDB_ERR_INVALID;
    tenants_fill(es, no, v, flags, stale_s);
    return 0;
}
