/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Statistics (docs/design-stats.md, steps 1 and 2).
 *
 * Two kinds of numbers, counted in two places.
 *
 * What queries did: one counter block per (thread, handle). The owner writes it with relaxed loads and stores, no
 * read-modify-write, so the hook at the end of a query costs a few nanoseconds; readers sum the blocks. A block is
 * found through a small thread-local array keyed by the handle's number, which is never reused, so two handles in one
 * thread stay apart. The blocks belong to a registry (qreg) that the handle and every thread's slot hold a reference
 * on: tsdb_close drops the handle's, a thread that exits later folds its block into the registry's retired totals and
 * drops its own, so a thread outliving the handle never touches freed memory. A failed allocation means that thread
 * does not count: a query never fails because of statistics.
 *
 * What is stored: the two functions that change a part list (db_parts_insert_locked, db_parts_remove_locked) push
 * one event each into a bounded ring, under the db->lock they already hold, and nothing else is added to the write
 * path (not the append, the memtable, the WAL, the part writer or the merge). A sweeper thread (nice 19, throttled
 * to stats_sweep_mb_s of index read, a hard cap) drains the ring and walks each added part's index once, while its
 * pages are in memory, into a record of column totals; a removed part's record is subtracted. An ADD holds a
 * reference on the part until it was walked (at most a tick); a REMOVE holds none, it names the part by address, and
 * a record is trusted only while its identity (address, partition, blocks, level, rows) matches. The volume totals
 * are always the sum of the records, so they cannot drift or go below zero. A full ring drops events and sets
 * `overflow`; so does a follower's list swap (`swapped`): the sweeper then reconciles, under db->lock discarding what
 * is queued and copying the list with references, and diffs it against its records. The first sweep of a lazily
 * started sweeper is such a reconcile too.
 *
 * Per-series numbers are kept nowhere: sscx_run walks the index runs of the requested ids in one cut of the
 * engine (the part list and the memtables, all with references taken in one db->lock hold) and reports what it
 * found, charging every index probe to the caller's budget.
 *
 * Lock order: es->mu is a leaf (never held across I/O, an allocation of a part, or another lock); the sweeper takes
 * db->lock and the dictionary's lock only with es->mu released. Statistics never write a file, never call eng_fail
 * and keep answering after the engine failed.
 */
#include "stats_priv.h"

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

_Static_assert(sizeof(tsdb_series_stat) == 176, "tsdb_series_stats is the 176-byte wire record");

/* ---------------------------------------------------------- query counters */

typedef struct qblock {
    _Alignas(128) atomic_uint_fast64_t c[QC_N];
    _Atomic(hotl *) hot; /* the thread's hot-reads list: made at its first sample, read by qreg_hot_collect */
} qblock;

struct qreg {
    uint64_t no; /* never reused in this process */
    atomic_int refs;
    pthread_mutex_t mu; /* a leaf */
    qblock **blocks;
    size_t n, cap;
    uint64_t retired[QC_N]; /* the blocks of threads that exited or dropped the handle */
    hotl *retired_hot;      /* their hot-reads lists, folded together */
};

static atomic_uint_fast64_t qreg_serial;

qreg *qreg_new(void)
{
    qreg *r = calloc(1, sizeof *r);
    if (!r)
        return NULL;
    r->no = atomic_fetch_add(&qreg_serial, 1) + 1;
    atomic_init(&r->refs, 1);
    pthread_mutex_init(&r->mu, NULL);
    return r;
}

static void qreg_unref(qreg *r)
{
    if (atomic_fetch_sub_explicit(&r->refs, 1, memory_order_acq_rel) != 1)
        return;
    free(r->blocks); /* every block was removed by its thread */
    hotl_free(r->retired_hot);
    pthread_mutex_destroy(&r->mu);
    free(r);
}

void qreg_close(qreg *r)
{
    if (r)
        qreg_unref(r);
}

typedef struct qslot {
    uint64_t no;
    qreg *r;
    qblock *b;
    uint64_t rng; /* xorshift state of the hot-reads sampling: this thread's own */
} qslot;
#define QSLOTS 8
static __thread qslot qtl[QSLOTS];
static __thread unsigned qtl_next;
static pthread_key_t qkey;
static pthread_once_t qonce = PTHREAD_ONCE_INIT;

static void qslot_drop(qslot *s)
{
    qreg *r = s->r;
    pthread_mutex_lock(&r->mu);
    for (size_t i = 0; i < r->n; i++)
        if (r->blocks[i] == s->b) {
            r->blocks[i] = r->blocks[--r->n];
            break;
        }
    for (int i = 0; i < QC_N; i++)
        r->retired[i] += atomic_load_explicit(&s->b->c[i], memory_order_relaxed);
    hotl *h = atomic_load_explicit(&s->b->hot, memory_order_acquire);
    if (h) { /* what this thread saw of the last two minutes goes into the registry's list */
        int64_t epoch = hot_now_ms() / HOT_PERIOD_MS;
        if (!r->retired_hot)
            r->retired_hot = hotl_new();
        pthread_mutex_lock(&h->mu);
        if (r->retired_hot && h->epoch >= epoch - 1)
            for (int w = 0; w < 2; w++)
                for (uint32_t i = 0; i < h->w[w].n; i++)
                    hotl_add(r->retired_hot, epoch, h->w[w].e[i].id, h->w[w].e[i].est);
        pthread_mutex_unlock(&h->mu);
    }
    pthread_mutex_unlock(&r->mu);
    hotl_free(h);
    free(s->b);
    qreg_unref(r);
    memset(s, 0, sizeof *s);
}

static void qthread_exit(void *arg)
{
    (void)arg;
    for (int i = 0; i < QSLOTS; i++)
        if (qtl[i].r)
            qslot_drop(&qtl[i]);
}

static void qkey_make(void) { pthread_key_create(&qkey, qthread_exit); }

static qslot *qslot_new(qreg *r)
{
    pthread_once(&qonce, qkey_make);
    qblock *b = aligned_alloc(128, sizeof *b);
    if (!b)
        return NULL;
    memset(b, 0, sizeof *b);
    pthread_mutex_lock(&r->mu);
    if (r->n == r->cap) {
        size_t cap = r->cap ? r->cap * 2 : 8;
        qblock **v = realloc(r->blocks, cap * sizeof *v);
        if (!v) {
            pthread_mutex_unlock(&r->mu);
            free(b);
            return NULL;
        }
        r->blocks = v;
        r->cap = cap;
    }
    r->blocks[r->n++] = b;
    pthread_mutex_unlock(&r->mu);
    atomic_fetch_add_explicit(&r->refs, 1, memory_order_relaxed);
    qslot *s = NULL;
    for (int i = 0; i < QSLOTS && !s; i++)
        if (!qtl[i].r)
            s = &qtl[i];
    if (!s) { /* a thread that uses more than QSLOTS handles: the oldest slot's counts go to its registry's total */
        s = &qtl[qtl_next++ % QSLOTS];
        qslot_drop(s);
    }
    *s = (qslot){.no = r->no, .r = r, .b = b,
                 .rng = (((uint64_t)(uintptr_t)s * 0x9E3779B97F4A7C15ull) ^ ((uint64_t)stats_mono_ns() << 7)) | 1};
    pthread_setspecific(qkey, qtl); /* any non-NULL value: the destructor runs when the thread exits */
    return s;
}

static qslot *qslot_get(qreg *r)
{
    for (int i = 0; i < QSLOTS; i++)
        if (qtl[i].no == r->no)
            return &qtl[i];
    return qslot_new(r);
}

/* One query in eight, drawn at random for each (so no client pattern, a round robin over eight series included, lines
 * up with it), goes into this thread's hot-reads list with weight 8. */
static void hot_sample(qslot *s, uint64_t sid)
{
    uint64_t x = s->rng;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    s->rng = x;
    if ((x >> 32) & 7)
        return;
    hotl *h = atomic_load_explicit(&s->b->hot, memory_order_relaxed);
    if (!h) {
        if (!(h = hotl_new()))
            return;
        atomic_store_explicit(&s->b->hot, h, memory_order_release);
    }
    hotl_add(h, hot_now_ms() / HOT_PERIOD_MS, sid, 8);
}

void qreg_add(qreg *r, const uint64_t v[QC_N], uint64_t sid)
{
    qslot *s = qslot_get(r);
    if (!s)
        return;
    for (int i = 0; i < QC_N; i++)
        atomic_store_explicit(&s->b->c[i], atomic_load_explicit(&s->b->c[i], memory_order_relaxed) + v[i],
                              memory_order_relaxed);
    if (sid)
        hot_sample(s, sid);
}

void qreg_hot_note(qreg *r, uint64_t sid)
{
    qslot *s = qslot_get(r);
    if (s)
        hot_sample(s, sid);
}

void qreg_hot_collect(qreg *r, hagg *a)
{
    int64_t epoch = hot_now_ms() / HOT_PERIOD_MS;
    pthread_mutex_lock(&r->mu);
    for (size_t k = 0; k < r->n; k++) {
        hotl *h = atomic_load_explicit(&r->blocks[k]->hot, memory_order_acquire);
        if (h)
            hagg_add_hotl(a, h, 0, epoch);
    }
    if (r->retired_hot)
        hagg_add_hotl(a, r->retired_hot, 0, epoch);
    pthread_mutex_unlock(&r->mu);
}

void qreg_totals(qreg *r, uint64_t out[QC_N])
{
    pthread_mutex_lock(&r->mu);
    for (int i = 0; i < QC_N; i++)
        out[i] = r->retired[i];
    for (size_t k = 0; k < r->n; k++)
        for (int i = 0; i < QC_N; i++)
            out[i] += atomic_load_explicit(&r->blocks[k]->c[i], memory_order_relaxed);
    pthread_mutex_unlock(&r->mu);
}

/* ------------------------------------------------------------- the sweeper */

atomic_uint stats_ring_cap = RING_N;
atomic_uint stats_pause;
atomic_uint_fast64_t stats_open_reconciles; /* tests: reconciles that ran before the open was done: none may */
#define TICK_MS 1000

int64_t stats_mono_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}
#define mono_ns stats_mono_ns

bool stats_stopping(struct estats *es) { return atomic_load_explicit(&es->stop, memory_order_relaxed); }

static size_t ptr_hash(const void *p) { return (size_t)(((uintptr_t)p >> 4) * 0x9E3779B97F4A7C15ull >> 20); }

bool rec_matches(const prec *r, const part *p)
{
    return r->key == p && r->partition == p->partition && r->min_block == p->min_block &&
           r->max_block == p->max_block && r->level == p->level && r->rows == p->rows;
}


/* es->mu held for the record table */
prec *rec_find(struct estats *es, const part *p)
{
    if (!es->htn)
        return NULL;
    for (prec *r = es->ht[ptr_hash(p) & (es->htn - 1)]; r; r = r->next)
        if (r->key == p)
            return r;
    return NULL;
}

void rec_insert(struct estats *es, prec *r)
{
    if (es->nrec >= es->htn) { /* load factor 1; if the table cannot grow, chains get longer, nothing breaks */
        size_t n = es->htn ? es->htn * 2 : 1024;
        prec **t = calloc(n, sizeof *t);
        if (t) {
            for (size_t b = 0; b < es->htn; b++)
                for (prec *q = es->ht[b], *nx; q; q = nx) {
                    nx = q->next;
                    size_t k = ptr_hash(q->key) & (n - 1);
                    q->next = t[k];
                    t[k] = q;
                }
            free(es->ht);
            es->ht = t;
            es->htn = n;
        } else if (!es->htn) {
            return;
        }
    }
    size_t k = ptr_hash(r->key) & (es->htn - 1);
    r->next = es->ht[k];
    es->ht[k] = r;
    es->nrec++;
}

void tot_apply(struct estats *es, const prec *r, bool add)
{
    vtot *t = &es->tot;
    if (add) {
        t->logical += r->logical;
        t->ts_bytes += r->ts_bytes;
        t->val_bytes += r->val_bytes;
        t->index_bytes += r->index_bytes;
        t->other_bytes += r->other_bytes;
    } else { /* what was added, so never below zero */
        t->logical -= r->logical;
        t->ts_bytes -= r->ts_bytes;
        t->val_bytes -= r->val_bytes;
        t->index_bytes -= r->index_bytes;
        t->other_bytes -= r->other_bytes;
    }
}

/* Takes the record of p out of the table and the totals; the caller frees it. */
prec *rec_remove(struct estats *es, const part *p)
{
    if (!es->htn)
        return NULL;
    for (prec **pp = &es->ht[ptr_hash(p) & (es->htn - 1)]; *pp; pp = &(*pp)->next)
        if ((*pp)->key == p) {
            prec *r = *pp;
            *pp = r->next;
            es->nrec--;
            tot_apply(es, r, false);
            return r;
        }
    return NULL;
}

/* Walks the index of p once: the column totals of its runs. -1: a series of p is not in the dictionary yet (a
 * follower ahead of its SERIES), so its width is unknown: the caller puts the part off. */
int part_walk(struct estats *es, const part *p, prec *r, unsigned flags)
{
    eng *db = es->db;
    memset(r, 0, sizeof *r);
    r->key = p;
    r->partition = p->partition;
    r->min_block = p->min_block;
    r->max_block = p->max_block;
    r->level = p->level;
    r->rows = p->rows;
    const gix *x = &p->ix;
    uint64_t rows = 0;
    if (x->nruns && x->rser[x->nruns - 1] >= eng_series_count(db))
        return -1;
    if (flags & WALK_SPLIT) {
        tenants_extend(es, x->nruns ? x->rser[x->nruns - 1] + 1 : 0); /* every id of the part has a tenant */
        tenant_scratch_clear(es);
    }
    if ((flags & WALK_RUNCNT) && x->nruns > es->runcnt_cap) {
        uint64_t *v = realloc(es->runcnt, (size_t)x->nruns * sizeof *v);
        if (!v)
            return -1; /* put off: tried again at the next tick */
        es->runcnt = v;
        es->runcnt_cap = x->nruns;
    }
    for (uint32_t k = 0; k < x->nruns; k++) {
        uint32_t a = gix_rstart(x, k), b = k + 1 < x->nruns ? gix_rstart(x, k + 1) : x->n;
        if (b < a)
            b = a;
        int type = eng_series_type(db, x->rser[k]);
        if (type < 0)
            return -1;
        uint64_t cnt = 0, ts = 0, val = 0, nulls = 0, full = 0;
        for (uint32_t i = a; i < b; i++) {
            uint64_t c = gix_get(x, i, GF_COUNT);
            cnt += c;
            full += c == p->grows;
            nulls += gix_get(x, i, GF_NULLS);
            ts += gix_get(x, i, GF_TSLEN);
            val += gix_get(x, i, GF_VALLEN);
        }
        rows += cnt;
        uint64_t logical = cnt * (8 + type_width(type));
        r->logical += logical;
        r->ts_bytes += ts;
        r->val_bytes += val;
        if (flags & WALK_RUNCNT)
            es->runcnt[k] = cnt;
        if (flags & WALK_SPLIT)
            tenant_walk_run(es, x->rser[k], cnt, nulls, logical, ts, val, (uint64_t)(b - a) * x->stride + 8, b - a);
        if ((flags & WALK_BPR) && p->level > 0 && full && cnt) /* merges' output: where compression shows */
            hot_bpr_run(es, x->rser[k], cnt, ts + val, true);
    }
    r->index_bytes = (uint64_t)x->n * x->stride + 8ull * x->nruns;
    uint64_t used = r->ts_bytes + r->val_bytes + r->index_bytes; /* the tombstones are in "other": a part's four columns
                                                                    * add up to its file */
    r->other_bytes = p->map_len > used ? p->map_len - used : 0;
    if (rows != p->rows) /* damage after the open: still counted, its bytes are on disk */
        fprintf(stderr, "tsdb: stats: part %s: its index holds %" PRIu64 " rows, its footer says %" PRIu64 "\n", p->name,
                rows, p->rows);
    return 0;
}

static void sleep_cv(struct estats *es, int64_t ns)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t t = (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec + ns;
    ts.tv_sec = t / 1000000000;
    ts.tv_nsec = t % 1000000000;
    pthread_cond_timedwait(&es->cv, &es->mu, &ts);
}

/* The throttle: a hard cap on the index bytes walked a second. A walk waits until its turn (at most 100 ms of
 * unused time is kept as a burst), then pays for itself. */
void throttle_wait(struct estats *es)
{
    if (es->mb_s <= 0)
        return;
    pthread_mutex_lock(&es->mu);
    for (;;) {
        int64_t now = mono_ns();
        if (atomic_load_explicit(&es->stop, memory_order_relaxed) || now >= es->ready_ns)
            break;
        sleep_cv(es, es->ready_ns - now);
    }
    pthread_mutex_unlock(&es->mu);
}

void throttle_pay(struct estats *es, uint64_t bytes)
{
    if (es->mb_s <= 0)
        return;
    pthread_mutex_lock(&es->mu);
    int64_t now = mono_ns(), base = es->ready_ns > now - 100000000 ? es->ready_ns : now - 100000000;
    es->ready_ns = base + (int64_t)(bytes * 1000 / (uint64_t)es->mb_s);
    pthread_mutex_unlock(&es->mu);
}

/* A record is gone (its part was removed, or the list moved on): what it added to the tenant side is taken back (the
 * partition is marked for a recount, or, when retention retired it and nothing of it is left, subtracted whole). */
void rec_discard(struct estats *es, prec *r, bool dropped)
{
    pv_part_removed(es, r, dropped);
    residency_forget(es, r);
    free(r);
}

/* Counts p unless its record is there. `fresh`: the writer listed it after the open (its own flush, merge or
 * insert): a level-0 one is ingest, a merge output makes its partition stale. false: put off. */
static bool add_part(struct estats *es, part *p, bool fresh)
{
    pthread_mutex_lock(&es->mu);
    prec *r = rec_find(es, p);
    bool known = r && rec_matches(r, p);
    pthread_mutex_unlock(&es->mu);
    if (known)
        return true;
    throttle_wait(es);
    prec *rec = malloc(sizeof *rec);
    if (!rec)
        return true; /* not counted: stats_parts_counted stays below stats_parts_listed */
    bool ing = fresh && p->level == 0;
    if (part_walk(es, p, rec, WALK_SPLIT | WALK_BPR | (ing ? WALK_RUNCNT : 0)) != 0) {
        free(rec);
        return false;
    }
    throttle_pay(es, p->index_len);
    pthread_mutex_lock(&es->mu);
    prec *stale = rec_remove(es, p); /* the address of a part that is gone, its REMOVE lost to a full ring */
    rec->gen = es->gen;
    rec->mcounted = !fresh || p->level == 0; /* a merge's rows are counted by the recount that first sees the part */
    tot_apply(es, rec, true);
    size_t before = es->nrec;
    rec_insert(es, rec);
    bool inserted = es->nrec != before;
    if (!inserted) { /* no table to put it in */
        tot_apply(es, rec, false);
        free(rec);
    }
    pthread_mutex_unlock(&es->mu);
    if (stale)
        rec_discard(es, stale, false);
    if (inserted) {
        pv_part_added(es, rec, fresh); /* the split is in the scratch the walk filled */
        if (ing)
            tenants_ingest(es, p, rec);
    }
    return true;
}

static bool ring_pop(struct estats *es, stat_ev *ev)
{
    uint64_t t = atomic_load_explicit(&es->tail, memory_order_relaxed);
    if (t == atomic_load_explicit(&es->head, memory_order_acquire))
        return false;
    *ev = es->ring[t % RING_N];
    atomic_store_explicit(&es->tail, t + 1, memory_order_release);
    return true;
}

static void pend_add(struct estats *es, part *p) /* takes the caller's reference */
{
    pthread_mutex_lock(&es->mu);
    if (es->npend == es->pend_cap) {
        size_t cap = es->pend_cap ? es->pend_cap * 2 : 16;
        part **v = realloc(es->pend, cap * sizeof *v);
        if (!v) {
            pthread_mutex_unlock(&es->mu);
            part_unref(p);
            return;
        }
        es->pend = v;
        es->pend_cap = cap;
    }
    es->pend[es->npend++] = p;
    pthread_mutex_unlock(&es->mu);
}

static void handle_remove(struct estats *es, part *p, bool dropped)
{
    part *gone = NULL;
    pthread_mutex_lock(&es->mu);
    prec *r = rec_remove(es, p);
    for (size_t i = 0; i < es->npend; i++)
        if (es->pend[i] == p) {
            gone = es->pend[i];
            es->pend[i] = es->pend[--es->npend];
            break;
        }
    pthread_mutex_unlock(&es->mu);
    if (r)
        rec_discard(es, r, dropped);
    if (gone)
        part_unref(gone);
}

/* Events in ring order. An ADD of a part that is removed again within the batch needs no walk, except a flush: its
 * rows are ingest, and the reference the event holds keeps the part readable. */
void process_events(struct estats *es, stat_ev *batch, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (batch[i].kind == STAT_EV_REMOVE) {
            handle_remove(es, batch[i].p, batch[i].dropped);
            continue;
        }
        bool cancelled = false;
        if (!(batch[i].fresh && batch[i].p->level == 0))
            for (size_t j = i + 1; j < n && !cancelled; j++)
                cancelled = batch[j].kind == STAT_EV_REMOVE && batch[j].p == batch[i].p;
        if (cancelled || add_part(es, batch[i].p, batch[i].fresh))
            part_unref(batch[i].p);
        else
            pend_add(es, batch[i].p);
        if (atomic_load_explicit(&es->stop, memory_order_relaxed)) { /* out of the ring: release them here */
            for (size_t j = i + 1; j < n; j++)
                if (batch[j].kind == STAT_EV_ADD)
                    part_unref(batch[j].p);
            return;
        }
    }
}

static void drain(struct estats *es)
{
    stat_ev batch[256];
    for (;;) {
        size_t n = 0;
        while (n < 256 && ring_pop(es, &batch[n]))
            n++;
        if (!n)
            return;
        process_events(es, batch, n);
        if (atomic_load_explicit(&es->stop, memory_order_relaxed))
            return;
    }
}

bool partition_cut(struct estats *es, int64_t key, part ***psp, size_t *npp)
{
    eng *db = es->db;
    stat_ev *batch = malloc(RING_N * sizeof *batch);
    if (!batch)
        return false;
    size_t nb = 0;
    pthread_mutex_lock(&db->lock);
    while (nb < RING_N && ring_pop(es, &batch[nb]))
        nb++;
    size_t lo = 0, hi = db->nparts; /* the list is ordered by partition first (part_cmp) */
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (db->parts[mid]->partition < key)
            lo = mid + 1;
        else
            hi = mid;
    }
    size_t end = lo;
    while (end < db->nparts && db->parts[end]->partition == key)
        end++;
    part **ps = malloc((end - lo ? end - lo : 1) * sizeof *ps);
    if (ps)
        for (size_t i = lo; i < end; i++) {
            part_ref(db->parts[i]);
            ps[i - lo] = db->parts[i];
        }
    pthread_mutex_unlock(&db->lock);
    if (!ps) { /* their changes are not in any list we have: the sweeper reconciles */
        for (size_t i = 0; i < nb; i++)
            if (batch[i].kind == STAT_EV_ADD)
                part_unref(batch[i].p);
        atomic_store(&es->overflow, true);
        free(batch);
        return false;
    }
    process_events(es, batch, nb);
    free(batch);
    *psp = ps;
    *npp = end - lo;
    return true;
}

/* The records against the list. Under db->lock the queued events are dropped (their changes are in the list copied
 * in the same hold), so what the ring holds afterwards is newer than the copy. */
static void reconcile(struct estats *es)
{
    eng *db = es->db;
    stat_ev ev;
    if (!atomic_load(&es->opened))
        atomic_fetch_add(&stats_open_reconciles, 1);
    pthread_mutex_lock(&db->lock);
    while (ring_pop(es, &ev))
        if (ev.kind == STAT_EV_ADD)
            part_unref(ev.p);
    atomic_store(&es->overflow, false);
    atomic_store(&es->swapped, false);
    size_t n = db->nparts;
    part **ps = malloc((n ? n : 1) * sizeof *ps);
    if (ps)
        for (size_t i = 0; i < n; i++) {
            ps[i] = db->parts[i];
            part_ref(ps[i]);
        }
    pthread_mutex_unlock(&db->lock);
    if (!ps) {
        atomic_store(&es->overflow, true); /* try again at the next pass */
        return;
    }
    part **todo = malloc((n ? n : 1) * sizeof *todo);
    size_t nt = 0;
    part **freed = NULL;
    size_t nfreed = 0;
    pthread_mutex_lock(&es->mu);
    freed = es->pend; /* the put-off parts are found again below */
    nfreed = es->npend;
    es->pend = NULL;
    es->npend = es->pend_cap = 0;
    uint32_t gen = ++es->gen;
    for (size_t i = 0; i < n; i++) {
        prec *r = rec_find(es, ps[i]);
        if (r && rec_matches(r, ps[i]))
            r->gen = gen;
        else if (todo)
            todo[nt++] = ps[i];
        else
            continue;
    }
    prec *dead = NULL; /* records of parts the list no longer has: their partitions are counted again */
    for (size_t b = 0; b < es->htn; b++)
        for (prec **pp = &es->ht[b]; *pp;) {
            prec *r = *pp;
            if (r->gen == gen) {
                pp = &r->next;
                continue;
            }
            *pp = r->next;
            es->nrec--;
            tot_apply(es, r, false);
            r->next = dead;
            dead = r;
        }
    pthread_mutex_unlock(&es->mu);
    while (dead) {
        prec *r = dead;
        dead = r->next;
        rec_discard(es, r, false);
    }
    for (size_t i = 0; i < nfreed; i++)
        part_unref(freed[i]);
    free(freed);
    for (size_t i = 0; i < nt; i++) /* not ingest: the previous list was counted by someone else or never seen */
        if (!atomic_load_explicit(&es->stop, memory_order_relaxed) && !add_part(es, todo[i], false)) {
            part_ref(todo[i]);
            pend_add(es, todo[i]);
        }
    if (!todo)
        atomic_store(&es->overflow, true);
    free(todo);
    for (size_t i = 0; i < n; i++)
        part_unref(ps[i]);
    free(ps);
}

static void retry_pending(struct estats *es)
{
    pthread_mutex_lock(&es->mu);
    size_t n = es->npend;
    part **v = es->pend;
    es->pend = NULL;
    es->npend = es->pend_cap = 0;
    pthread_mutex_unlock(&es->mu);
    for (size_t i = 0; i < n; i++) { /* still unlisted after a REMOVE meanwhile? handle_remove cannot see them here */
        bool listed = false;
        pthread_mutex_lock(&es->db->lock);
        for (size_t k = 0; k < es->db->nparts && !listed; k++)
            listed = es->db->parts[k] == v[i];
        pthread_mutex_unlock(&es->db->lock);
        if (!listed) {
            part_unref(v[i]);
        } else if (add_part(es, v[i], false)) {
            part_unref(v[i]);
        } else {
            pend_add(es, v[i]);
        }
    }
    free(v);
}

static void *sweeper_main(void *arg)
{
    struct estats *es = arg;
#ifdef __linux__
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only */
#endif
    while (!atomic_load(&es->stop)) {
        while (atomic_load_explicit(&stats_pause, memory_order_relaxed) && !atomic_load(&es->stop))
            usleep(1000); /* tests only */
        uint64_t req = atomic_load(&es->settle_req);
        bool opened = atomic_load(&es->opened);
        if (opened && (atomic_load(&es->overflow) || atomic_load(&es->swapped))) {
            if (atomic_load(&es->overflow))
                atomic_store(&es->ingest_approx, true); /* what the lost events added is not counted as ingest */
            reconcile(es);
        }
        tenants_apply_list(es);
        tenants_extend(es, eng_series_count(es->db)); /* the series that no part names yet have a tenant too */
        drain(es);
        bool more = false;
        if (opened) {
            retry_pending(es);
            more = tenants_step(es, atomic_load(&es->settle_done) < req);
            residency_step(es);
        }
        struct timespec ct;
        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ct) == 0)
            atomic_store(&es->cpu_ns, (uint64_t)ct.tv_sec * 1000000000u + (uint64_t)ct.tv_nsec);
        pthread_mutex_lock(&es->mu);
        if (atomic_load(&es->settle_done) < req && !more) {
            atomic_store(&es->settle_done, req);
            pthread_cond_broadcast(&es->done_cv);
        }
        /* before the end of the open an overflow stays set, and is no work yet */
        bool idle = !more && !(opened && (atomic_load(&es->overflow) || atomic_load(&es->swapped))) &&
                    atomic_load(&es->head) == atomic_load(&es->tail) && atomic_load(&es->settle_req) == req;
        if (idle && !atomic_load(&es->stop))
            sleep_cv(es, (int64_t)TICK_MS * 1000000);
        pthread_mutex_unlock(&es->mu);
    }
    return NULL;
}

void stats_push(eng *db, part *p, int kind)
{
    struct estats *es = atomic_load_explicit(&db->stats, memory_order_acquire);
    uint64_t h = atomic_load_explicit(&es->head, memory_order_relaxed);
    uint64_t t = atomic_load_explicit(&es->tail, memory_order_acquire);
    if (h - t >= atomic_load_explicit(&stats_ring_cap, memory_order_relaxed)) {
        atomic_store(&es->overflow, true);
        atomic_fetch_add_explicit(&es->overflows, 1, memory_order_relaxed);
        pthread_cond_signal(&es->cv);
        return;
    }
    if (kind == STAT_EV_ADD)
        part_ref(p);
    es->ring[h % RING_N] = (stat_ev){p, (uint8_t)kind, kind == STAT_EV_ADD && atomic_load_explicit(&es->opened, memory_order_relaxed),
                                     kind == STAT_EV_REMOVE && p->dropped}; /* db->lock is held: dropped is stable */
    atomic_store_explicit(&es->head, h + 1, memory_order_release);
    if (h + 1 - t == MAX(1u, atomic_load_explicit(&stats_ring_cap, memory_order_relaxed) / 2))
        pthread_cond_signal(&es->cv);
}

static void estats_free(struct estats *es)
{
    stat_ev ev;
    while (ring_pop(es, &ev))
        if (ev.kind == STAT_EV_ADD)
            part_unref(ev.p);
    for (size_t i = 0; i < es->npend; i++)
        part_unref(es->pend[i]);
    free(es->pend);
    for (size_t b = 0; b < es->htn; b++)
        for (prec *r = es->ht[b], *nx; r; r = nx) {
            nx = r->next;
            rec_free_res(r);
            free(r);
        }
    free(es->ht);
    tenants_free(es);
    hotl_free(es->hw);
    free(es->runcnt);
    pthread_mutex_destroy(&es->mu);
    pthread_cond_destroy(&es->cv);
    pthread_cond_destroy(&es->done_cv);
    free(es);
}

void stats_start(eng *db, bool reconcile_first)
{
    if (db->opt.stats < 0 || atomic_flag_test_and_set(&db->stats_starting))
        return;
    struct estats *es = calloc(1, sizeof *es);
    if (!es)
        return;
    es->db = db;
    es->mb_s = db->opt.stats_sweep_mb_s == 0 ? 256 : db->opt.stats_sweep_mb_s;
    es->recompute_ns = (int64_t)(db->opt.stats_recompute_s == 0 ? 30 : db->opt.stats_recompute_s) * 1000000000;
    es->res_period_ns = (int64_t)(db->opt.stats_resident_s == 0 ? 600 : db->opt.stats_resident_s) * 1000000000;
    es->hw = hotl_new();
    pthread_mutex_init(&es->mu, NULL);
    pthread_cond_init(&es->cv, NULL);
    pthread_cond_init(&es->done_cv, NULL);
    tenants_init(es);
    if (reconcile_first) { /* a start after the open (lazy, or a follower's) */
        atomic_store(&es->swapped, true);
        atomic_store(&es->opened, true);
    }
    /* published before the thread runs: a part listed from now on is an event, and the first reconcile copies
     * the list after that, so nothing falls between them */
    pthread_mutex_lock(&db->lock);
    atomic_store_explicit(&db->stats, es, memory_order_release);
    atomic_store_explicit(&db->stats_on, true, memory_order_release);
    pthread_mutex_unlock(&db->lock);
    if (tsdb_thread_create(&es->th, sweeper_main, es) != 0) {
        pthread_mutex_lock(&db->lock);
        atomic_store(&db->stats_on, false);
        atomic_store(&db->stats, NULL);
        pthread_mutex_unlock(&db->lock);
        estats_free(es);
        return;
    }
    es->th_started = true;
}

void stats_open_done(eng *db)
{
    struct estats *es = atomic_load(&db->stats);
    if (!es)
        return;
    atomic_store(&es->opened, true);
    pthread_mutex_lock(&es->mu);
    pthread_cond_broadcast(&es->cv); /* an overflow that waited for the open is work now */
    pthread_mutex_unlock(&es->mu);
}

void stats_stop(eng *db)
{
    struct estats *es = atomic_load(&db->stats);
    if (!es)
        return;
    pthread_mutex_lock(&db->lock);
    atomic_store(&db->stats_on, false); /* no more pushes */
    pthread_mutex_unlock(&db->lock);
    pthread_mutex_lock(&es->mu);
    atomic_store(&es->stop, true);
    pthread_cond_broadcast(&es->cv);
    pthread_mutex_unlock(&es->mu);
    if (es->th_started)
        pthread_join(es->th, NULL);
    atomic_store(&db->stats, NULL);
    estats_free(es);
}

void stats_list_swapped(eng *db)
{
    struct estats *es = atomic_load_explicit(&db->stats, memory_order_acquire);
    if (!es)
        return;
    atomic_store(&es->swapped, true);
    pthread_mutex_lock(&es->mu);
    pthread_cond_signal(&es->cv);
    pthread_mutex_unlock(&es->mu);
}

void stats_fill(eng *db, tsdb_volume_stats *vs)
{
    if (db->opt.stats < 0)
        return;
    stats_start(db, true); /* lazy (stats = 0): the first look starts it */
    struct estats *es = atomic_load_explicit(&db->stats, memory_order_acquire);
    if (!es)
        return;
    pthread_mutex_lock(&es->mu);
    vs->logical_bytes = es->tot.logical;
    vs->ts_bytes = es->tot.ts_bytes;
    vs->value_bytes = es->tot.val_bytes;
    vs->index_bytes = es->tot.index_bytes;
    vs->other_bytes = es->tot.other_bytes;
    vs->stats_parts_counted = es->nrec;
    pthread_mutex_unlock(&es->mu);
    vs->stats_parts_listed = vs->parts;
    vs->stats_ring_overflows = atomic_load(&es->overflows);
    vs->stats_cpu_ns = atomic_load(&es->cpu_ns);
    vs->stats_tmap_bytes = tenants_map_bytes(es);
}

int stats_settle(eng *db, int timeout_ms)
{
    if (db->opt.stats < 0)
        return 0;
    stats_start(db, true);
    struct estats *es;
    int64_t t0 = mono_ns();
    while (!(es = atomic_load_explicit(&db->stats, memory_order_acquire))) { /* another thread is starting it */
        if (mono_ns() - t0 > (int64_t)timeout_ms * 1000000)
            return TSDB_ERR_UNAVAILABLE;
        usleep(1000);
    }
    pthread_mutex_lock(&es->mu);
    uint64_t req = atomic_fetch_add(&es->settle_req, 1) + 1;
    pthread_cond_broadcast(&es->cv);
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t t = (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec + (int64_t)timeout_ms * 1000000;
    ts.tv_sec = t / 1000000000;
    ts.tv_nsec = t % 1000000000;
    int rc = 0;
    while (atomic_load(&es->settle_done) < req && rc == 0)
        rc = pthread_cond_timedwait(&es->done_cv, &es->mu, &ts);
    bool done = atomic_load(&es->settle_done) >= req;
    pthread_mutex_unlock(&es->mu);
    return done ? 0 : TSDB_ERR_UNAVAILABLE;
}

/* ------------------------------------------------------------ per series */

struct sscx {
    eng *db;
    part **ps;
    size_t np;
    memtable **ms; /* every memtable with a reference: flushing ones first (immutable), then the active and the tails */
    size_t nm, nflush;
};

sscx *sscx_open(eng *db)
{
    sscx *x = calloc(1, sizeof *x);
    if (!x)
        return NULL;
    x->db = db;
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock); /* one cut: the parts and the memtables that hold what they do not */
    x->ps = malloc((db->nparts ? db->nparts : 1) * sizeof *x->ps);
    x->ms = malloc((db->nflushing + db->ntails + 1) * sizeof *x->ms);
    if (!x->ps || !x->ms) {
        pthread_mutex_unlock(&db->buf_lock);
        pthread_mutex_unlock(&db->lock);
        free(x->ps);
        free(x->ms);
        free(x);
        return NULL;
    }
    for (size_t i = 0; i < db->nparts; i++) {
        part_ref(db->parts[i]);
        x->ps[x->np++] = db->parts[i];
    }
    for (size_t i = 0; i < db->nflushing; i++) {
        mt_ref(db->flushing[i]);
        x->ms[x->nm++] = db->flushing[i];
    }
    x->nflush = x->nm;
    if (db->active) {
        mt_ref(db->active);
        x->ms[x->nm++] = db->active;
    }
    for (size_t i = 0; i < db->ntails; i++) {
        mt_ref(db->tails[i].m);
        x->ms[x->nm++] = db->tails[i].m;
    }
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    return x;
}

void sscx_close(sscx *x)
{
    if (!x)
        return;
    for (size_t i = 0; i < x->np; i++)
        part_unref(x->ps[i]);
    for (size_t i = 0; i < x->nm; i++)
        mt_unref(x->ms[i]);
    free(x->ps);
    free(x->ms);
    free(x);
}

static unsigned log2_ceil(uint64_t n) { return n <= 1 ? 1 : 64 - (unsigned)__builtin_clzll(n - 1); }

typedef struct sacc {
    int64_t first, last, lastp;
    bool has, inpart;
} sacc;

/* One memtable's rows of the requested series. An unsorted series is scanned for its range (the write path keeps no
 * min and max), and every row scanned is charged to the caller's budget. A memtable that still grows (`live`) is read
 * under buf_lock, taken for one series at a time: the writers of the volume wait for one series' scan (at most the
 * rows of the memtable), never for a whole chunk. */
static void mem_account(const memtable *mt, const uint32_t *ids, size_t m, tsdb_series_stat *out, sacc *acc,
                        int64_t wall_ms, uint64_t *probes, eng *live)
{
    int64_t created = wall_ms - (eng_mono_ms() - mt->created_ms);
    for (size_t i = 0; i < m; i++) {
        uint32_t id = ids[i];
        if (out[i].status)
            continue;
        if (live)
            pthread_mutex_lock(&live->buf_lock);
        if (id >= mt->scap || !mt->s[id].n) {
            if (live)
                pthread_mutex_unlock(&live->buf_lock);
            continue;
        }
        const mt_series *s = &mt->s[id];
        int64_t lo = s->ts[0], hi = s->ts[s->n - 1];
        *probes += 1;
        if (!s->sorted) {
            *probes += s->n;
            for (uint32_t k = 1; k < s->n; k++) {
                lo = MIN(lo, s->ts[k]);
                hi = MAX(hi, s->ts[k]);
            }
        }
        uint32_t rows = s->n;
        if (live)
            pthread_mutex_unlock(&live->buf_lock);
        out[i].rows += rows;
        out[i].memtable_rows += rows;
        acc[i].first = acc[i].has ? MIN(acc[i].first, lo) : lo;
        acc[i].last = acc[i].has ? MAX(acc[i].last, hi) : hi;
        acc[i].has = true;
        out[i].last_write_ms = MAX(out[i].last_write_ms, created);
    }
}

void sscx_run(sscx *x, const uint32_t *ids, size_t m, tsdb_series_stat *out, uint64_t *probes, bool resident)
{
    eng *db = x->db;
    sacc acc[256];
    memset(out, 0, m * sizeof *out);
    memset(acc, 0, sizeof acc);
    for (size_t i = 0; i < m; i++) {
        int type = eng_series_type(db, ids[i]);
        if (type < 0) {
            out[i].status = TSDB_ERR_NOTFOUND;
            continue;
        }
        int cls = 0;
        eng_series_class(db, ids[i], &cls);
        out[i].cls = (uint8_t)cls;
        out[i].type = (uint8_t)type;
    }
    for (size_t k = 0; k < x->np; k++) {
        const part *p = x->ps[k];
        const gix *g = &p->ix;
        bool runs = g->nruns && g->rser[0] <= ids[m - 1] && g->rser[g->nruns - 1] >= ids[0];
        bool tombs = p->nt && p->t[0].series <= ids[m - 1] && p->t[p->nt - 1].series >= ids[0];
        *probes += 1;
        if (!runs && !tombs)
            continue;
        if (runs) {
            unsigned lg = log2_ceil(g->nruns);
            bool merge = (uint64_t)g->nruns + m < (uint64_t)m * lg; /* one pass over the run table is cheaper */
            *probes += merge ? g->nruns + m : (uint64_t)m * lg;
            uint32_t rc = 0;
            for (size_t i = 0; i < m; i++) {
                if (out[i].status)
                    continue;
                uint32_t a, b;
                if (merge) {
                    while (rc < g->nruns && g->rser[rc] < ids[i])
                        rc++;
                    a = rc < g->nruns ? gix_rstart(g, rc) : g->n;
                    b = rc < g->nruns && g->rser[rc] == ids[i] ? (rc + 1 < g->nruns ? gix_rstart(g, rc + 1) : g->n) : a;
                    if (b < a)
                        b = a;
                } else {
                    gix_run(g, ids[i], &a, &b);
                }
                if (b <= a)
                    continue;
                tsdb_series_stat *o = &out[i];
                uint64_t cnt = 0, nl = 0, tl = 0, vl = 0, full = 0;
                for (uint32_t e = a; e < b; e++) {
                    uint64_t c = gix_get(g, e, GF_COUNT);
                    cnt += c;
                    nl += gix_get(g, e, GF_NULLS);
                    tl += gix_get(g, e, GF_TSLEN);
                    vl += gix_get(g, e, GF_VALLEN);
                    full += c == p->grows;
                }
                *probes += b - a;
                o->rows += cnt;
                o->nulls += nl;
                o->logical_bytes += cnt * (8 + type_width(o->type));
                o->ts_bytes += tl;
                o->value_bytes += vl;
                o->index_bytes += (uint64_t)(b - a) * g->stride + 8;
                o->granules += b - a;
                o->full_granules += full;
                if (resident) { /* a series' granules are contiguous in a part: one mincore for the run */
                    uint64_t off0 = gix_get(g, a, GF_OFF);
                    uint64_t off1 = gix_get(g, b - 1, GF_OFF) + gix_get(g, b - 1, GF_TSLEN) + gix_get(g, b - 1, GF_VALLEN);
                    if (off1 > off0 && off1 <= p->map_len)
                        o->resident_bytes += resident_bytes_range(p->map + off0, off1 - off0);
                    *probes += 1 + (off1 > off0 ? (off1 - off0) / 4096 / 16 : 0); /* a syscall and its page vector */
                }
                o->parts++;
                if (!acc[i].inpart || acc[i].lastp != p->partition) /* parts are listed by partition */
                    o->partitions++;
                acc[i].inpart = true;
                acc[i].lastp = p->partition;
                int64_t lo, hi, lo2, hi2;
                gx_ts(p, a, &lo, &hi);
                gx_ts(p, b - 1, &lo2, &hi2);
                (void)hi;
                (void)lo2;
                acc[i].first = acc[i].has ? MIN(acc[i].first, lo) : lo;
                acc[i].last = acc[i].has ? MAX(acc[i].last, hi2) : hi2;
                acc[i].has = true;
                /* format 9 keeps the write time in the header (a merge carries its inputs' newest); a format-8 part has
                 * only the file's mtime, which a merge moves: say so */
                int64_t wms = p->written_ms;
                if (wms == 0) {
                    wms = (int64_t)p->written * 1000;
                    o->flags |= TSDB_SSTAT_WRITE_FROM_MTIME;
                }
                o->last_write_ms = MAX(o->last_write_ms, wms);
            }
        }
        if (tombs) {
            unsigned lg = log2_ceil(p->nt);
            for (size_t i = 0; i < m; i++) {
                if (out[i].status)
                    continue;
                uint32_t lo = 0, hi = p->nt; /* the first tombstone of the series: sorted by (series, start) */
                while (lo < hi) {
                    uint32_t mid = lo + (hi - lo) / 2;
                    if (p->t[mid].series < ids[i])
                        lo = mid + 1;
                    else
                        hi = mid;
                }
                *probes += lg;
                uint64_t cnt = 0;
                for (; lo < p->nt && p->t[lo].series == ids[i]; lo++)
                    cnt++;
                *probes += cnt;
                out[i].tomb_bytes += cnt * sizeof(tomb);
            }
        }
    }
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    int64_t wall_ms = (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
    for (size_t k = 0; k < x->nflush; k++) /* flushing memtables never change */
        mem_account(x->ms[k], ids, m, out, acc, wall_ms, probes, NULL);
    for (size_t k = x->nflush; k < x->nm; k++) /* the active memtable and the tails grow under buf_lock */
        mem_account(x->ms[k], ids, m, out, acc, wall_ms, probes, db);
    for (size_t i = 0; i < m; i++) {
        if (out[i].status)
            continue;
        int64_t cut = eng_cutoff(db, ids[i]); /* INT64_MIN: no limit */
        if (!acc[i].has || acc[i].last < cut) {
            out[i].flags |= TSDB_SSTAT_NO_RANGE;
            continue;
        }
        out[i].first_ts = MAX(acc[i].first, cut);
        out[i].last_ts = acc[i].last;
    }
}
