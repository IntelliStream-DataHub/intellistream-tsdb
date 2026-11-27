/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Reading an object back from its chunks, with decode (design-cluster-data.md §3.1-3.2, E2, E10, E11, E26).
 *
 * chunk_read_range works one stripe at a time over a chunk_source (files here, memory in tests and in fuzz_ec, the
 * network in R8). The healthy path reads the cells a byte range needs from their data chunks. A cell that is absent,
 * cannot be read or fails its sum is an erasure: the stripe is rebuilt from any k' good cells of its k' + m, data
 * cells first, then parity, and a data cell past the end of the object counts as a good cell of zeros. Fewer than k'
 * good cells is TSDB_ERR_CORRUPT naming the object, never wrong bytes.
 *
 * What chooses the good cells is the encode-time sum of each cell (E26): a 64-bit xxh64 per cell, from the object
 * record when the caller has it (which also catches a chunk of another encoding that is self-consistent), else from
 * the chunk's own table. A wrong survivor decodes to a plausible wrong answer, so nothing is ever tried and checked
 * after the fact by a short sum. The geometry is the object's own (the caller's record, or the headers), never today's
 * settings (E2).
 *
 * Two modes. The default reads every cell it uses whole and checks it, which is what a scrub, a repair or the second
 * try of a reader wants. CHUNK_RR_RAW reads only the bytes asked for and, when a cell fails to read, the same byte
 * range of the others (Reed-Solomon works column by column, so a 10 KB granule costs 10 KB from each of k' cells, not
 * k' MiB): it cannot see rot, so its caller must check what it gets (a granule's crc, E11) and ask again without RAW
 * when that fails.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef TSDB_EC
#include "ec/rs.h"
#endif

/* Bounded joinable I/O worker pool owned by chunk_backend (lazily initialized on first multi-chunk parallel read,
 * drained and pthread_join()'d in chunk_be_close()). Never spawns threads for single-chunk reads; falls back inline
 * when saturated, stopping, or forked. */
#define CR_POOL_MAX 8
#define CR_PAR_MIN_BYTES (32u << 10)

typedef struct cr_job {
    void (*fn)(void *);
    void *arg;
    struct cr_job *next;
} cr_job;

struct chunk_rpool {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    cr_job *head, *tail;
    int busy, queued, nthreads;
    bool stopping;
    pid_t pid;
    pthread_t threads[CR_POOL_MAX];
};

static void *cr_pool_worker_main(void *arg)
{
    chunk_rpool *p = arg;
    pthread_mutex_lock(&p->mu);
    for (;;) {
        while (!p->head && !p->stopping)
            pthread_cond_wait(&p->cv, &p->mu);
        if (p->head) {
            cr_job *j = p->head;
            p->head = j->next;
            if (!p->head)
                p->tail = NULL;
            p->queued--;
            p->busy++;
            pthread_mutex_unlock(&p->mu);
            j->fn(j->arg);
            pthread_mutex_lock(&p->mu);
            p->busy--;
            continue;
        }
        if (p->stopping)
            break;
    }
    pthread_mutex_unlock(&p->mu);
    return NULL;
}

static chunk_rpool *cr_pool_get(chunk_backend *be)
{
    if (!be)
        return NULL;
    chunk_rpool *p = atomic_load_explicit(&be->rpool, memory_order_acquire);
    if (p)
        return p;
    pthread_mutex_lock(&be->mu);
    p = atomic_load_explicit(&be->rpool, memory_order_relaxed);
    if (!p) {
        p = calloc(1, sizeof *p);
        if (p) {
            pthread_mutex_init(&p->mu, NULL);
            pthread_cond_init(&p->cv, NULL);
            p->pid = getpid();
            atomic_store_explicit(&be->rpool, p, memory_order_release);
        }
    }
    pthread_mutex_unlock(&be->mu);
    return p;
}

static int cr_pool_submit(chunk_backend *be, cr_job *j)
{
    chunk_rpool *p = cr_pool_get(be);
    if (!p || getpid() != p->pid)
        return -1;
    j->next = NULL;
    pthread_mutex_lock(&p->mu);
    if (p->stopping) {
        pthread_mutex_unlock(&p->mu);
        return -1;
    }
    int avail = p->nthreads - p->busy;
    if (p->queued >= avail && p->nthreads < CR_POOL_MAX) {
        pthread_attr_t at;
        pthread_attr_init(&at);
#ifdef __APPLE__
        pthread_attr_setstacksize(&at, TSDB_THREAD_STACK);
#endif
        pthread_t th;
        int e = pthread_create(&th, &at, cr_pool_worker_main, p);
        pthread_attr_destroy(&at);
        if (e == 0) {
            p->threads[p->nthreads++] = th;
            avail++;
        }
    }
    if (p->queued >= avail) {
        pthread_mutex_unlock(&p->mu);
        return -1;
    }
    if (p->tail)
        p->tail->next = j;
    else
        p->head = j;
    p->tail = j;
    p->queued++;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->mu);
    return 0;
}

void chunk_rpool_destroy(chunk_rpool *p)
{
    if (!p)
        return;
    if (getpid() == p->pid) {
        pthread_mutex_lock(&p->mu);
        p->stopping = true;
        pthread_cond_broadcast(&p->cv);
        int n = p->nthreads;
        pthread_mutex_unlock(&p->mu);
        for (int i = 0; i < n; i++)
            pthread_join(p->threads[i], NULL);
        pthread_cond_destroy(&p->cv);
        pthread_mutex_destroy(&p->mu);
    }
    free(p);
}

static int obj_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len);
static bool obj_healthy_pos_ready(const chunk_obj *o, unsigned pos);
static chunk_backend *obj_backend(const chunk_obj *o);

enum { PS_UNKNOWN = 0, PS_GOOD, PS_BAD };

/* Per-thread cache of cell scratch buffers for chunk_read_range / cr_pos_run: avoids multi-MiB mmap/munmap churn
 * across consecutive reads on the same thread. Bounded to CR_TLS_MAX_BYTES per thread, freed on thread exit via
 * pthread_key_t destructor (never registers atexit() inside libtsdb.so). */
#define CR_TLS_MAX_BYTES ((size_t)8 << 20)

typedef struct cr_tls_pool {
    uint8_t *slot[CHUNK_POS_MAX];
    size_t cap[CHUNK_POS_MAX];
    size_t total;
    bool in_use;
} cr_tls_pool;

static __thread cr_tls_pool *cr_tls;
static pthread_key_t cr_tls_key;
static bool cr_tls_key_ok;
static pthread_once_t cr_tls_once = PTHREAD_ONCE_INIT;

static void cr_tls_free(void *arg)
{
    cr_tls_pool *p = arg;
    if (!p)
        return;
    if (cr_tls == p)
        cr_tls = NULL;
    for (unsigned i = 0; i < CHUNK_POS_MAX; i++)
        free(p->slot[i]);
    free(p);
}

static void cr_tls_init(void) { cr_tls_key_ok = pthread_key_create(&cr_tls_key, cr_tls_free) == 0; }

static cr_tls_pool *cr_tls_acquire(void)
{
    cr_tls_pool *p = cr_tls;
    if (!p) {
        pthread_once(&cr_tls_once, cr_tls_init);
        if (!cr_tls_key_ok)
            return NULL;
        p = calloc(1, sizeof *p);
        if (!p || pthread_setspecific(cr_tls_key, p) != 0) {
            free(p);
            return NULL;
        }
        cr_tls = p;
    }
    if (p->in_use)
        return NULL;
    p->in_use = true;
    return p;
}

static uint8_t *cr_tls_slot(cr_tls_pool *p, unsigned pos, size_t need)
{
    if (!p || pos >= CHUNK_POS_MAX)
        return NULL;
    if (p->cap[pos] >= need)
        return p->slot[pos];
    size_t other = p->total - p->cap[pos];
    if (need > CR_TLS_MAX_BYTES - other)
        return NULL;
    size_t new_total = other + need;
    uint8_t *b = malloc(need);
    if (!b)
        return NULL;
    free(p->slot[pos]);
    p->slot[pos] = b;
    p->cap[pos] = need;
    p->total = new_total;
    return b;
}

typedef struct rr {
    const chunk_source *src;
    const chunk_geom *g;
    const uint64_t *rec;
    chunk_report *rep;
    bool raw;
    uint64_t base[CHUNK_POS_MAX];
    cr_tls_pool *tls;             /* borrowed per-thread scratch pool, NULL if nested/unavailable */
    uint8_t *slot[CHUNK_POS_MAX]; /* a buffer of one cell per position, made when first used, kept for the call */
    size_t slot_cap[CHUNK_POS_MAX];
    bool owned[CHUNK_POS_MAX];    /* true when slot[p] was heap-allocated outside tls and must be freed on exit */
    size_t cap;
#ifdef TSDB_EC
    iso_rs *rs;
    iso_rs_plan *plan;            /* cached RS decode plan for plan_mask across stripes; freed unconditionally on exit */
    uint32_t plan_mask;
    _Static_assert(CHUNK_POS_MAX <= 32, "plan_mask is uint32_t");
#endif
    uint8_t zero_[1];
} rr;

static void note(rr *r, unsigned pos, int st)
{
    r->rep->st[pos] = (uint8_t)st;
    r->rep->bad_cells++;
}

static inline bool pos_written(uint32_t positions, unsigned pos)
{
    return positions == 0 || (pos < 32 && (positions & (1u << pos)) != 0);
}

static uint8_t *rr_slot(rr *r, unsigned pos, size_t need)
{
    if (need == 0)
        need = 1;
    if (r->slot[pos] && r->slot_cap[pos] >= need)
        return r->slot[pos];
    if (r->owned[pos]) {
        free(r->slot[pos]);
        r->slot[pos] = NULL;
        r->slot_cap[pos] = 0;
        r->owned[pos] = false;
    }
    uint8_t *b = cr_tls_slot(r->tls, pos, need);
    if (b) {
        r->slot[pos] = b;
        r->slot_cap[pos] = r->tls->cap[pos];
        r->owned[pos] = false;
        return b;
    }
    b = malloc(need);
    if (!b)
        return NULL;
    r->slot[pos] = b;
    r->slot_cap[pos] = need;
    r->owned[pos] = true;
    return b;
}

/* Reads cell `idx` of chunk `pos` whole into dst (len bytes) and checks it: 0 or a CHUNK_ST_* status. */
static int fetch_checked(rr *r, unsigned pos, uint64_t idx, size_t len, uint8_t *dst)
{
    uint64_t want;
    if (r->rec) {
        want = r->rec[r->base[pos] + idx];
    } else {
        int st = 0;
        const uint64_t *tab = r->src->sums(r->src->ctx, pos, &st);
        if (!tab)
            return st ? st : CHUNK_ST_IOERR;
        want = tab[idx];
    }
    int st = r->src->read(r->src->ctx, pos, idx * r->g->cell, dst, len);
    if (st)
        return st;
    if (xxh64(dst, len, 0) != want && chunk_mutant != CHUNK_MUT_NOSUMS) /* planted: sums not checked */
        return CHUNK_ST_DAMAGED;
    return 0;
}

static void copy_out(const uint8_t *cell_bytes, uint64_t x, uint64_t y, uint8_t *dst)
{
    memcpy(dst, cell_bytes + x, (size_t)(y - x));
}

/* The stripe `s` has needed cells that failed (bad[i]): rebuilds them from the others into dst. */
static int degrade(rr *r, uint64_t s, uint64_t off, const bool *bad, const bool *have, const uint64_t *xs,
                   const uint64_t *ys, uint8_t *buf)
{
    const chunk_geom *g = r->g;
    if (g->m == 0) { /* single-copy (D5, k' = 1): no parity to decode from, never fabricate */
#ifdef TSDB_EC
        if (chunk_mutant == CHUNK_MUT_M0_PARITY) { /* planted: reader tries parity decode for m = 0 */
            if (!r->rs) {
                r->rs = chunk_rs_get(g->k, g->m);
                if (!r->rs)
                    return TSDB_ERR_NOMEM;
            }
        }
#endif
        int st = r->rep->st[0];
        bool is_lost = st == CHUNK_ST_LOST;
        bool away = st == CHUNK_ST_DOWN;
        if (!chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: stripe %llu single-copy chunk is unusable (%s): %s\n", r->src->name,
                    (unsigned long long)s,
                    st == CHUNK_ST_ABSENT ? "absent" : st == CHUNK_ST_DAMAGED ? "damaged"
                    : st == CHUNK_ST_LOST ? "store lost" : st == CHUNK_ST_DOWN ? "store down" : "unreadable",
                    is_lost ? "data lost" : away ? "unavailable while it is" : "not readable");
        return is_lost ? TSDB_ERR_IO : away ? TSDB_ERR_UNAVAILABLE : TSDB_ERR_CORRUPT;
    }
#ifndef TSDB_EC
    (void)off, (void)bad, (void)have, (void)xs, (void)ys, (void)buf;
    return TSDB_ERR_INVALID;
#else
    unsigned k = g->k, m = g->m;
    uint64_t ncells = chunk_ncells(g), Ls = chunk_stripe_len(g, s);
    uint64_t x0 = Ls, y0 = 0;
    for (unsigned i = 0; i < k; i++)
        if (bad[i]) {
            if (xs[i] < x0)
                x0 = xs[i];
            if (ys[i] > y0)
                y0 = ys[i];
        }
    if (!r->raw)
        x0 = 0, y0 = Ls; /* whole cells, each checked */
    size_t w = (size_t)(y0 - x0);
    uint8_t *shards[CHUNK_POS_MAX];
    bool present[CHUNK_POS_MAX], isbad[CHUNK_POS_MAX];
    unsigned ngood = 0;
    uint32_t pmask = 0;
    memset(shards, 0, sizeof shards);
    memset(isbad, 0, sizeof isbad);
    for (unsigned i = 0; i < k; i++)
        isbad[i] = bad[i];
    for (unsigned p = 0; p < k + m; p++)
        present[p] = false;
    int sts[CHUNK_POS_MAX] = {0};
    size_t need = r->raw ? w : r->cap;
    for (unsigned p = 0; p < k + m && ngood < k; p++) { /* data first, then parity */
        if (isbad[p])
            continue;
        if (!pos_written(r->src->positions, p)) {
            sts[p] = CHUNK_ST_UNWRITTEN;
            r->rep->st[p] = CHUNK_ST_UNWRITTEN;
            continue; /* E10 known erasure: never requested, never CORRUPT */
        }
        uint64_t c = p < k ? s * k + p : 0, cl = p < k ? chunk_cell_len(g, c) : Ls;
        uint8_t *b = rr_slot(r, p, need);
        if (!b)
            return TSDB_ERR_NOMEM;
        if (!r->raw && p < k && have[p]) { /* the direct pass read and checked it whole: only the padding is left */
            memset(b + cl, 0, w - (size_t)cl);
        } else if (p < k && c >= ncells) { /* beyond the object: zeros, no read */
            memset(b, 0, w);
            r->rep->st[p] = CHUNK_ST_VIRTUAL;
        } else if (r->raw) {
            if (eng_abandoned())
                return TSDB_ERR_UNAVAILABLE;
            size_t avail = x0 < cl ? (size_t)((y0 < cl ? y0 : cl) - x0) : 0;
            int st = avail ? r->src->read(r->src->ctx, p, s * g->cell + x0, b, avail) : 0;
            if (st) {
                sts[p] = st;
                note(r, p, st);
                continue;
            }
            memset(b + avail, 0, w - avail);
        } else {
            if (eng_abandoned())
                return TSDB_ERR_UNAVAILABLE;
            int st = fetch_checked(r, p, s, (size_t)cl, b);
            if (st) {
                sts[p] = st;
                note(r, p, st);
                continue;
            }
            memset(b + cl, 0, w - (size_t)cl);
        }
        shards[p] = b;
        present[p] = true;
        pmask |= 1u << p;
        ngood++;
    }
    if (ngood < k) {
        unsigned absent = 0, damaged = 0, io = 0, unwritten = 0;
        unsigned nbad = 0, down = 0, lost = 0;
        for (unsigned p = 0; p < k + m; p++) {
            int pst = sts[p] ? sts[p] : (isbad[p] ? r->rep->st[p] : 0);
            if (pst)
                nbad++;
            absent += pst == CHUNK_ST_ABSENT;
            damaged += pst == CHUNK_ST_DAMAGED;
            io += pst == CHUNK_ST_IOERR;
            unwritten += pst == CHUNK_ST_UNWRITTEN;
            /* a position whose store is merely away: the cell may be fine (E15: unavailable, never lost) */
            down += pst == CHUNK_ST_DOWN;
            /* a position whose store was declared permanently LOST (E15: > m lost -> TSDB_ERR_IO) */
            lost += pst == CHUNK_ST_LOST;
        }
        bool is_lost = lost > m;
        bool away = !is_lost && (ngood + down >= k);
        if (!chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: stripe %llu has %u good chunks of the %u it needs (%u of %u positions "
                        "unusable: %u absent, %u damaged, %u unreadable, %u on stores that are down, %u on lost "
                        "stores, %u unwritten): %s\n",
                    r->src->name, (unsigned long long)s, ngood, k, nbad, k + m, absent, damaged, io, down, lost,
                    unwritten,
                    is_lost ? "data lost" : away ? "unavailable while they are" : "not readable");
        return is_lost ? TSDB_ERR_IO : away ? TSDB_ERR_UNAVAILABLE : TSDB_ERR_CORRUPT;
    }
    /* the buffers the decode fills: every absent data position gets one of its own (data_only=true never writes parity) */
    for (unsigned p = 0; p < k; p++)
        if (!present[p]) {
            shards[p] = rr_slot(r, p, need);
            if (!shards[p])
                return TSDB_ERR_NOMEM;
        }
    if (!r->rs) {
        r->rs = chunk_rs_get(k, m);
        if (!r->rs)
            return TSDB_ERR_NOMEM;
    }
    if (!r->plan || r->plan_mask != pmask) {
        iso_rs_plan_free(r->plan);
        r->plan = iso_rs_plan_new(r->rs, present, true);
        if (!r->plan)
            return TSDB_ERR_NOMEM;
        r->plan_mask = pmask;
    }
    iso_rs_plan_apply(r->plan, shards, w);
    for (unsigned i = 0; i < k; i++) {
        if (!bad[i])
            continue;
        uint64_t c = s * k + i;
        if (!r->raw && r->rec && xxh64(shards[i], (size_t)chunk_cell_len(g, c), 0) != r->rec[r->base[i] + s] &&
            chunk_mutant != CHUNK_MUT_NOSUMS) {
            if (!chunk_log_quiet)
                fprintf(stderr, "tsdb: object %s: stripe %llu rebuilt cell %u does not match its encode-time sum: the "
                            "chunks are not one encoding\n", r->src->name, (unsigned long long)s, i);
            return TSDB_ERR_CORRUPT;
        }
        copy_out(shards[i], xs[i] - x0, ys[i] - x0, buf + (c * g->cell + xs[i] - off));
        r->rep->decoded_cells++;
    }
    return 0;
#endif
}

typedef struct cr_pos_task {
    cr_job job;
    pthread_mutex_t *mu;
    pthread_cond_t *cv;
    unsigned *left;
    const chunk_obj *o;
    const chunk_geom *g;
    const uint64_t *rec;
    uint64_t base;
    uint64_t off, len;
    uint64_t s0, s1, c0, c1;
    uint8_t *buf;
    unsigned pos;
    size_t cap;
    bool raw;
    const volatile int *abandon;
    bool failed;
} cr_pos_task;

static void cr_pos_run(cr_pos_task *t)
{
    const chunk_geom *g = t->g;
    unsigned k = g->k, pos = t->pos;
    cr_tls_pool *tls = NULL;
    uint8_t *slot = NULL;
    bool slot_owned = false;
    if (!t->raw) {
        size_t need = t->cap ? t->cap : 1;
        tls = cr_tls_acquire();
        slot = cr_tls_slot(tls, 0, need);
        if (!slot) {
            slot = malloc(need);
            slot_owned = true;
        }
        if (!slot) {
            if (tls)
                tls->in_use = false;
            t->failed = true;
            return;
        }
    }
    const uint64_t *sums = NULL;
    if (!t->raw && !t->rec) {
        sums = chunk_obj_pos_sums(t->o, pos);
        if (!sums) {
            if (slot_owned)
                free(slot);
            if (tls)
                tls->in_use = false;
            t->failed = true;
            return;
        }
    }
    bool failed = false;
    for (uint64_t s = t->s0; s <= t->s1 && !failed; s++) {
        uint64_t c = s * k + pos;
        if (c < t->c0 || c > t->c1)
            continue;
        if (t->abandon && __atomic_load_n(t->abandon, __ATOMIC_ACQUIRE)) {
            failed = true;
            break;
        }
        uint64_t cs = c * g->cell, cl = chunk_cell_len(g, c);
        uint64_t a = t->off > cs ? t->off : cs;
        uint64_t b = t->off + t->len < cs + cl ? t->off + t->len : cs + cl;
        uint64_t x = a - cs, y = b - cs;
        uint8_t *dst = t->buf + (a - t->off);
        if (t->raw) {
            if (obj_read((void *)t->o, pos, s * g->cell + x, dst, (size_t)(y - x)) != 0)
                failed = true;
        } else {
            uint64_t want = t->rec ? t->rec[t->base + s] : sums[s];
            if (obj_read((void *)t->o, pos, s * g->cell, slot, (size_t)cl) != 0) {
                failed = true;
            } else if (xxh64(slot, (size_t)cl, 0) != want && chunk_mutant != CHUNK_MUT_NOSUMS) {
                failed = true;
            } else {
                copy_out(slot, x, y, dst);
            }
        }
    }
    if (slot_owned)
        free(slot);
    if (tls)
        tls->in_use = false;
    t->failed = failed;
}

static void cr_pos_work(void *arg)
{
    cr_pos_task *t = arg;
    cr_pos_run(t);
    pthread_mutex_lock(t->mu);
    (*t->left)--;
    if (*t->left == 0)
        pthread_cond_signal(t->cv);
    pthread_mutex_unlock(t->mu);
}

/* Parallel healthy fast path for local chunk_obj multi-chunk reads: runs one task per touched data position across
 * [s0, s1] (pos 0 inline, others on the cached I/O pool). Returns true when every cell succeeded; returns false to
 * fall back to the exact stripe-by-stripe degraded path (zero change to degrade/report/error semantics). */
static bool try_read_parallel(const chunk_obj *o, const chunk_geom *g, const uint64_t *rec_sums, uint64_t off,
                              uint64_t len, uint8_t *buf, bool raw, uint64_t c0, uint64_t c1)
{
    unsigned k = g->k;
    uint64_t s0 = c0 / k, s1 = c1 / k;
    unsigned pos_list[CHUNK_K_MAX], npos = 0;
    for (unsigned i = 0; i < k; i++) {
        uint64_t first = s0 * k + i;
        if (first < c0)
            first += k;
        if (first <= c1) {
            if (!obj_healthy_pos_ready(o, i))
                return false; /* missing/damaged data chunk: let stripe-by-stripe degrade() handle it */
            pos_list[npos++] = i;
        }
    }
    if (npos < 2)
        return false;

    pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
    unsigned left = 0;
    cr_pos_task tasks[CHUNK_K_MAX];
    bool inline_run[CHUNK_K_MAX] = {false};
    size_t cap = g->cell < g->len ? g->cell : (size_t)g->len;

    for (unsigned idx = 0; idx < npos; idx++) {
        unsigned p = pos_list[idx];
        tasks[idx] = (cr_pos_task){
            .job = {.fn = cr_pos_work, .arg = &tasks[idx]},
            .mu = &mu,
            .cv = &cv,
            .left = &left,
            .o = o,
            .g = g,
            .rec = rec_sums,
            .base = chunk_sums_base(g, p),
            .off = off,
            .len = len,
            .s0 = s0,
            .s1 = s1,
            .c0 = c0,
            .c1 = c1,
            .buf = buf,
            .pos = p,
            .cap = cap,
            .raw = raw,
            .abandon = cur_abandon,
        };
    }

    inline_run[0] = true; /* calling thread handles first position inline */
    pthread_mutex_lock(&mu);
    for (unsigned idx = 1; idx < npos; idx++)
        left++;
    pthread_mutex_unlock(&mu);

    unsigned failed_submits = 0;
    chunk_backend *be = obj_backend(o);
    for (unsigned idx = 1; idx < npos; idx++) {
        if (cr_pool_submit(be, &tasks[idx].job) != 0) {
            inline_run[idx] = true;
            failed_submits++;
        }
    }
    if (failed_submits > 0) {
        pthread_mutex_lock(&mu);
        left -= failed_submits;
        pthread_mutex_unlock(&mu);
    }

    for (unsigned idx = 0; idx < npos; idx++)
        if (inline_run[idx])
            cr_pos_run(&tasks[idx]);

    pthread_mutex_lock(&mu);
    while (left > 0)
        pthread_cond_wait(&cv, &mu);
    pthread_mutex_unlock(&mu);
    pthread_cond_destroy(&cv);
    pthread_mutex_destroy(&mu);

    for (unsigned idx = 0; idx < npos; idx++)
        if (tasks[idx].failed)
            return false;
    return true;
}

int chunk_read_range(const chunk_source *src, const chunk_geom *g, const uint64_t *rec_sums, uint64_t off,
                     uint64_t len, void *vbuf, unsigned flags, chunk_report *rep)
{
    chunk_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (!chunk_geom_valid(g) || off > g->len || len > g->len - off)
        return TSDB_ERR_INVALID;
    if (len == 0)
        return 0;
    if (eng_abandoned())
        return TSDB_ERR_UNAVAILABLE;
    uint8_t *buf = vbuf;
    unsigned k = g->k;
    uint64_t c0 = off / g->cell, c1 = (off + len - 1) / g->cell;
    bool raw = (flags & CHUNK_RR_RAW) != 0;
    uint32_t data_mask = (1u << k) - 1u;
    bool all_data_written = src->positions == 0 || (src->positions & data_mask) == data_mask;
    if (src->read == obj_read && all_data_written && k > 1 && c1 > c0 && len >= CR_PAR_MIN_BYTES &&
        chunk_fault_read == 0) {
        if (try_read_parallel((const chunk_obj *)src->ctx, g, rec_sums, off, len, buf, raw, c0, c1))
            return 0;
        if (eng_abandoned())
            return TSDB_ERR_UNAVAILABLE;
    }
    rr r;
    memset(&r, 0, sizeof r);
    r.src = src;
    r.g = g;
    r.rec = rec_sums;
    r.rep = rep;
    r.raw = raw;
    r.cap = g->cell < g->len ? g->cell : (size_t)g->len;
    r.tls = cr_tls_acquire();
    for (unsigned p = 0; p < chunk_npos(g); p++)
        r.base[p] = chunk_sums_base(g, p);
    int rc = 0;
    for (uint64_t s = c0 / k; s <= c1 / k && rc == 0; s++) {
        bool bad[CHUNK_K_MAX] = {false}, have[CHUNK_K_MAX] = {false};
        uint64_t xs[CHUNK_K_MAX] = {0}, ys[CHUNK_K_MAX] = {0};
        bool any = false;
        for (unsigned i = 0; i < k; i++) {
            uint64_t c = s * k + i;
            if (c < c0 || c > c1)
                continue;
            uint64_t cs = c * g->cell, cl = chunk_cell_len(g, c);
            uint64_t a = off > cs ? off : cs, b = off + len < cs + cl ? off + len : cs + cl;
            xs[i] = a - cs;
            ys[i] = b - cs;
            if (!pos_written(src->positions, i)) {
                rep->st[i] = CHUNK_ST_UNWRITTEN;
                bad[i] = true;
                any = true;
                continue;
            }
            if (eng_abandoned()) {
                rc = TSDB_ERR_UNAVAILABLE;
                break;
            }
            uint8_t *dst = buf + (a - off);
            int st;
            if (r.raw) {
                st = src->read(src->ctx, i, s * g->cell + xs[i], dst, (size_t)(ys[i] - xs[i]));
            } else {
                uint8_t *slot = rr_slot(&r, i, r.cap);
                st = slot ? fetch_checked(&r, i, s, (size_t)cl, slot) : CHUNK_ST_IOERR;
                if (st == 0) {
                    copy_out(slot, xs[i], ys[i], dst);
                    have[i] = true; /* whole and checked: a source for the others' rebuild */
                }
            }
            if (st) {
                note(&r, i, st);
                bad[i] = true;
                any = true;
            }
        }
        if (rc == 0 && any)
            rc = degrade(&r, s, off, bad, have, xs, ys, buf);
    }
#ifdef TSDB_EC
    iso_rs_plan_free(r.plan);
#endif
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        if (r.owned[p])
            free(r.slot[p]);
    if (r.tls)
        r.tls->in_use = false;
    return rc;
}

/* ---------------------------------------------------------------- an object in a backend */
struct chunk_obj {
    chunk_backend *be;
    uint32_t volume;
    uint8_t vuuid[16], oid[16];
    chunk_geom g;
    uint32_t positions;               /* R9 (E10): written-positions bitmap (0 = all k'+m) */
    pthread_mutex_t mu;               /* serializes lazy open of deferred parity positions */
    atomic_bool tried[CHUNK_POS_MAX]; /* release-stored once rd[p]/st[p] are populated */
    chunk_rd *rd[CHUNK_POS_MAX];
    int rd_store[CHUNK_POS_MAX];      /* store index rd[p] was opened from (-1 if none) */
    int st[CHUNK_POS_MAX];
    atomic_int place_n;               /* inferred store count that placed this object (handles tier add) */
    const uint64_t *rec;
    char name[40];
};

static int st_of(int rc)
{
    return rc == TSDB_ERR_NOTFOUND    ? CHUNK_ST_ABSENT
           : rc == TSDB_ERR_CORRUPT   ? CHUNK_ST_DAMAGED
           : rc == TSDB_ERR_UNAVAILABLE ? CHUNK_ST_DOWN
                                      : CHUNK_ST_IOERR;
}

static bool obj_store_down(const chunk_backend *be, int st_idx)
{
    if (!be || st_idx < 0 || chunk_mutant == CHUNK_MUT_DOWNNOREAD)
        return false;
    int s = chunk_be_store_state(be, st_idx);
    if (s == CHUNK_STORE_UP || s == CHUNK_STORE_REPLACING)
        return false;
    return s == CHUNK_STORE_DOWN_STALLED || chunk_be_open_store(be, st_idx) == NULL;
}

static bool obj_healthy_pos_ready(const chunk_obj *o, unsigned pos)
{
    return pos < CHUNK_POS_MAX && atomic_load_explicit(&((chunk_obj *)o)->tried[pos], memory_order_acquire) &&
           o->rd[pos] != NULL && !chunk_be_store_is_lost(o->be, o->rd_store[pos]) &&
           !obj_store_down(o->be, o->rd_store[pos]);
}

static chunk_backend *obj_backend(const chunk_obj *o)
{
    return o ? o->be : NULL;
}

static int obj_open_pos(chunk_obj *o, unsigned p, bool *have, int *worst)
{
    chunk_ident id = {.volume = o->volume, .pos = (uint16_t)p};
    memcpy(id.vuuid, o->vuuid, 16);
    memcpy(id.oid, o->oid, 16);
    chunk_rd *rd = NULL;
    int st_idx = -1;
    int rc = chunk_be_rd_open_at(o->be, &id, &rd, &st_idx);
    if (rc == 0) {
        const chunk_geom *hg = &chunk_rd_hdr(rd)->g;
        if (!*have) { /* the geometry is the first valid chunk's: the object's own, recorded in every header */
            o->g = *hg;
            *have = true;
        }
        if (memcmp(hg, &o->g, sizeof *hg) != 0) { /* another encoding's chunk: not this object's */
            chunk_rd_close(rd);
            o->st[p] = CHUNK_ST_DAMAGED;
            return TSDB_ERR_CORRUPT;
        }
        o->rd[p] = rd;
        o->rd_store[p] = st_idx;
        o->st[p] = CHUNK_ST_OK;
        return 0;
    }
    o->st[p] = st_of(rc);
    if (worst && (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && *worst != TSDB_ERR_IO) ||
                  (rc == TSDB_ERR_NOMEM && *worst == TSDB_ERR_NOTFOUND)))
        *worst = rc;
    return rc;
}

static void obj_update_place_n(chunk_obj *o)
{
    int obs_st[CHUNK_POS_MAX];
    unsigned obs_pos[CHUNK_POS_MAX];
    size_t nobs = 0;
    unsigned np = chunk_npos(&o->g);
    for (unsigned i = 0; i < np && i < CHUNK_POS_MAX; i++) {
        if (o->rd[i] && o->rd_store[i] >= 0) {
            obs_st[nobs] = o->rd_store[i];
            obs_pos[nobs] = i;
            nobs++;
        }
    }
    if (nobs > 0) {
        int pn = chunk_be_infer_place_n(o->be, o->oid, np, obs_st, obs_pos, nobs, -1);
        atomic_store_explicit(&o->place_n, pn, memory_order_release);
    }
}

/* Lazily opens a deferred parity position (k .. k+m-1) on first use (degrade() or heal); lock-free once tried. */
static void obj_ensure_pos(chunk_obj *o, unsigned pos)
{
    if (pos >= CHUNK_POS_MAX || atomic_load_explicit(&o->tried[pos], memory_order_acquire))
        return;
    pthread_mutex_lock(&o->mu);
    if (!atomic_load_explicit(&o->tried[pos], memory_order_relaxed)) {
        if (!pos_written(o->positions, pos)) {
            o->st[pos] = CHUNK_ST_UNWRITTEN;
        } else if (pos < chunk_npos(&o->g)) {
            bool have = true;
            (void)obj_open_pos(o, pos, &have, NULL);
            if (o->rd[pos]) {
                obj_update_place_n(o);
            } else if (o->st[pos] == CHUNK_ST_ABSENT || o->st[pos] == CHUNK_ST_DOWN) {
                int pn = atomic_load_explicit(&o->place_n, memory_order_acquire);
                int sidx = chunk_be_place_for_obj(o->be, o->oid, pos, pn);
                if (chunk_be_store_is_lost(o->be, sidx))
                    o->st[pos] = CHUNK_ST_LOST;
                else if (chunk_be_store_state(o->be, sidx) != CHUNK_STORE_UP)
                    o->st[pos] = CHUNK_ST_DOWN;
                else
                    o->st[pos] = CHUNK_ST_ABSENT;
            }
        } else {
            o->st[pos] = CHUNK_ST_ABSENT;
        }
        atomic_store_explicit(&o->tried[pos], true, memory_order_release);
    }
    pthread_mutex_unlock(&o->mu);
}

static int obj_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    chunk_obj *o = ctx;
    if (pos >= CHUNK_POS_MAX)
        return CHUNK_ST_IOERR;
    obj_ensure_pos(o, pos);
    if (!o->rd[pos])
        return o->st[pos] ? o->st[pos] : CHUNK_ST_ABSENT;
    int st_idx = o->rd_store[pos];
    if (chunk_be_store_is_lost(o->be, st_idx))
        return CHUNK_ST_LOST;
    if (obj_store_down(o->be, st_idx))
        return CHUNK_ST_DOWN;
    bool real_io = false;
    int rrc = chunk_rd_read_ex(o->rd[pos], off, buf, len, &real_io);
    if (rrc != 0) {
        if (rrc == TSDB_ERR_IO && real_io && st_idx >= 0)
            chunk_be_note_ioerr(o->be, st_idx);
        return CHUNK_ST_IOERR;
    }
    return 0;
}

static const uint64_t *obj_sums(void *ctx, unsigned pos, int *status)
{
    chunk_obj *o = ctx;
    if (pos >= CHUNK_POS_MAX) {
        *status = CHUNK_ST_ABSENT;
        return NULL;
    }
    obj_ensure_pos(o, pos);
    if (!o->rd[pos]) {
        *status = o->st[pos] ? o->st[pos] : CHUNK_ST_ABSENT;
        return NULL;
    }
    if (chunk_be_store_is_lost(o->be, o->rd_store[pos])) {
        *status = CHUNK_ST_LOST;
        return NULL;
    }
    if (obj_store_down(o->be, o->rd_store[pos])) {
        *status = CHUNK_ST_DOWN;
        return NULL;
    }
    return chunk_rd_sums(o->rd[pos]);
}

int chunk_obj_open_pos(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                       const chunk_geom *expect, const uint64_t *rec_sums, uint32_t positions, chunk_obj **out)
{
    chunk_obj *o = calloc(1, sizeof *o);
    if (!o)
        return TSDB_ERR_NOMEM;
    for (unsigned i = 0; i < CHUNK_POS_MAX; i++)
        o->rd_store[i] = -1;
    o->be = be;
    atomic_init(&o->place_n, chunk_be_nstores(be));
    o->volume = volume;
    memcpy(o->vuuid, vuuid, 16);
    memcpy(o->oid, oid, 16);
    o->positions = positions;
    o->rec = rec_sums;
    pthread_mutex_init(&o->mu, NULL);
    chunk_oid_hex(oid, o->name);
    bool have = expect != NULL;
    if (have)
        o->g = *expect;
    bool alive = false;
    int worst = TSDB_ERR_NOTFOUND;
    /* Healthy path: open only data positions 0 .. k-1; parity positions k .. k+m-1 open lazily in obj_ensure_pos()
     * when degrade() or heal actually needs them. If geometry is unknown (!have), scan until the first valid header
     * discovers o->g (then finish up to k-1). If all data positions failed (!alive), scan parity too so an object
     * surviving only on parity chunks still opens. Positions cleared in `positions` are known erasures (never opened). */
    unsigned p = 0;
    for (; p < CHUNK_POS_MAX; p++) {
        if (have && alive && p >= o->g.k)
            break;
        if (have && p >= chunk_npos(&o->g))
            break;
        if (!pos_written(positions, p)) {
            o->st[p] = CHUNK_ST_UNWRITTEN;
            atomic_store_explicit(&o->tried[p], true, memory_order_relaxed);
            continue;
        }
        int rc = obj_open_pos(o, p, &have, &worst);
        atomic_store_explicit(&o->tried[p], true, memory_order_relaxed);
        if (rc == 0)
            alive = true;
    }
    bool away = false; /* a store that is not UP: what it holds may be what this object lacks */
    bool any_lost = false;
    for (int i = 0; i < chunk_be_nstores(be); i++) {
        if (chunk_be_store_is_lost(be, i))
            any_lost = true;
        else if (chunk_be_store_state(be, i) != CHUNK_STORE_UP)
            away = true;
    }
    if (!alive || !have) {
        bool obj_lost = false;
        bool obj_away = away;
        if (have) {
            unsigned npos = chunk_npos(&o->g);
            int bn = chunk_be_nstores(be);
            int min_n = npos >= 2 ? (int)npos : 1;
            if (min_n > bn)
                min_n = bn;
            /* Check active-store placement (when any slot is excluded) plus all valid store counts [min_n, bn] that
             * place every position on a non-UP store (so objects written before `tier add` or during `--shrink` still
             * classify LOST vs DOWN accurately even if no chunk opened). */
            bool any_cand = false, all_cand_lost = true, any_cand_away = false;
            bool has_excl = chunk_be_active_count(be) < bn;
            for (int cand = bn; cand >= (has_excl ? min_n - 1 : min_n); cand--) {
                bool use_active = has_excl && cand == min_n - 1;
                unsigned lost_pos = 0, down_pos = 0, up_pos = 0;
                for (unsigned i = 0; i < npos; i++) {
                    int sidx = use_active ? chunk_be_place(be, oid, i) : chunk_be_place_n(oid, i, cand);
                    if (chunk_be_store_is_lost(be, sidx))
                        lost_pos++;
                    else if (chunk_be_store_state(be, sidx) != CHUNK_STORE_UP)
                        down_pos++;
                    else
                        up_pos++;
                }
                if (up_pos > 0)
                    continue;
                any_cand = true;
                bool clost = false, caway = false;
                if (o->g.m == 0) {
                    if (o->st[0] != CHUNK_ST_DAMAGED) {
                        clost = lost_pos > 0;
                        caway = !clost && down_pos > 0;
                    }
                } else if (lost_pos > o->g.m) {
                    clost = true;
                } else {
                    caway = down_pos > 0;
                }
                if (!clost)
                    all_cand_lost = false;
                if (caway)
                    any_cand_away = true;
            }
            if (any_cand) {
                obj_lost = all_cand_lost;
                obj_away = !obj_lost && any_cand_away;
            } else {
                unsigned lost_pos = 0, down_pos = 0;
                for (unsigned i = 0; i < npos; i++) {
                    int sidx = chunk_be_place_n(oid, i, bn);
                    if (chunk_be_store_is_lost(be, sidx))
                        lost_pos++;
                    else if (chunk_be_store_state(be, sidx) != CHUNK_STORE_UP)
                        down_pos++;
                }
                if (o->g.m == 0) {
                    if (o->st[0] == CHUNK_ST_DAMAGED) {
                        obj_away = false;
                    } else if (lost_pos > 0) {
                        obj_lost = true;
                        obj_away = false;
                    } else {
                        obj_away = down_pos > 0;
                    }
                } else if (lost_pos > o->g.m) {
                    obj_lost = true;
                    obj_away = false;
                } else {
                    obj_away = down_pos > 0;
                }
            }
        }
        if (obj_lost && !chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: more than m positions are on lost stores: data lost\n", o->name);
        else if (obj_away && !chunk_log_quiet && worst != TSDB_ERR_NOTFOUND)
            fprintf(stderr, "tsdb: object %s: no chunk of it can be read while a store is down: unavailable\n", o->name);
        else if (worst != TSDB_ERR_NOTFOUND && !chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: no chunk of it can be read: not readable\n", o->name);
        chunk_obj_close(o);
        if (obj_lost)
            return TSDB_ERR_IO;
        if (obj_away)
            return TSDB_ERR_UNAVAILABLE;
        if (worst == TSDB_ERR_NOMEM)
            return TSDB_ERR_NOMEM;
        return worst == TSDB_ERR_NOTFOUND ? TSDB_ERR_NOTFOUND : TSDB_ERR_CORRUPT;
    }
    obj_update_place_n(o);
    int pn = atomic_load_explicit(&o->place_n, memory_order_acquire);
    if (away || any_lost)
        for (unsigned i = 0; i < p; i++)
            if (!o->rd[i] && (o->st[i] == CHUNK_ST_ABSENT || o->st[i] == CHUNK_ST_DOWN)) {
                int sidx = chunk_be_place_for_obj(be, oid, i, pn);
                if (chunk_be_store_is_lost(be, sidx))
                    o->st[i] = CHUNK_ST_LOST;
                else if (chunk_be_store_state(be, sidx) != CHUNK_STORE_UP)
                    o->st[i] = CHUNK_ST_DOWN; /* it may be there: it comes back with its store (or its heal) */
                else
                    o->st[i] = CHUNK_ST_ABSENT;
            }
    if (chunk_mutant == CHUNK_MUT_SETTINGS) { /* planted: the layout taken from today's settings, not from the object */
        chunk_geom t;
        if (chunk_geom_derive(&t, chunk_settings_k, chunk_settings_m, o->g.len) == 0)
            o->g = t;
    }
    *out = o;
    return 0;
}

int chunk_obj_open(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                   const chunk_geom *expect, const uint64_t *rec_sums, chunk_obj **out)
{
    return chunk_obj_open_pos(be, volume, vuuid, oid, expect, rec_sums, 0, out);
}

void chunk_obj_close(chunk_obj *o)
{
    if (!o)
        return;
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        chunk_rd_close(o->rd[p]);
    pthread_mutex_destroy(&o->mu);
    free(o);
}

const chunk_geom *chunk_obj_geom(const chunk_obj *o) { return &o->g; }

const uint64_t *chunk_obj_pos_sums(const chunk_obj *o, unsigned pos)
{
    if (pos >= CHUNK_POS_MAX)
        return NULL;
    obj_ensure_pos((chunk_obj *)o, pos);
    return o->rd[pos] ? chunk_rd_sums(o->rd[pos]) : NULL;
}

int chunk_obj_pos_status(const chunk_obj *o, unsigned pos)
{
    if (pos >= CHUNK_POS_MAX)
        return CHUNK_ST_ABSENT;
    obj_ensure_pos((chunk_obj *)o, pos);
    return o->rd[pos] ? CHUNK_ST_OK : o->st[pos];
}

uint64_t chunk_obj_epoch(const chunk_obj *o)
{
    uint64_t e = 0;
    unsigned n = chunk_npos(&o->g);
    for (unsigned p = 0; p < n; p++) {
        obj_ensure_pos((chunk_obj *)o, p);
        if (o->rd[p] && chunk_rd_hdr(o->rd[p])->epoch > e)
            e = chunk_rd_hdr(o->rd[p])->epoch;
    }
    return e;
}

int chunk_obj_place_n(const chunk_obj *o)
{
    return o ? atomic_load_explicit(&((chunk_obj *)o)->place_n, memory_order_acquire) : 0;
}

int chunk_obj_source(chunk_obj *o, chunk_source *src)
{
    memset(src, 0, sizeof *src);
    src->ctx = o;
    memcpy(src->name, o->name, sizeof o->name);
    src->read = obj_read;
    src->sums = obj_sums;
    src->positions = o->positions;
    return 0;
}

int chunk_obj_read(chunk_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep)
{
    chunk_source src;
    chunk_obj_source(o, &src);
    return chunk_read_range(&src, &o->g, o->rec, off, len, buf, flags, rep);
}
