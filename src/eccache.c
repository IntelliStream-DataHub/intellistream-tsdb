/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The granule cache and the open objects of a tier (src/eccache.h has the design).
 *
 * Two tables under one mutex (mu, a leaf): granule entries keyed by (oid, offset), each READY one on an LRU list with
 * its bytes counted against the budget, and open objects keyed by (volume, oid), on their own LRU with their chunk fds
 * counted against the fd budget. A LOADING entry or an OPENING object is in its table so that a second reader waits
 * for it (cv) instead of reading it again; it is pinned by its loader. Pinned entries and objects are never evicted;
 * what eviction takes out is freed or closed after mu is released. Nothing under mu does I/O.
 *
 * The backend: r7-tier hands its own over (eccache_set_backend); until then the cache opens one from the tier's
 * configuration at first use, under be_mu, which is held across that open (I/O) and taken only by readers with no
 * engine lock (the seam's callers hold none), never under mu.
 */
#include "eccache.h"
#include "chunk.h"
#include "ecpart.h"
#include "stats_priv.h"
#include "tier.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

int eccache_mutant;
void (*eccache_test_hook)(struct tier *t, int where, const uint8_t oid[16]);

enum { CE_LOADING = 0, CE_READY };

struct og;

typedef struct ce {
    struct ce *hnext;       /* hash chain (or the victims' list once out of the table) */
    struct ce *prev, *next; /* LRU, READY entries only */
    struct ce *gprev, *gnext; /* the entries of one object (its group), while listed */
    struct og *grp;
    uint8_t oid[16];
    uint64_t off, len;
    uint32_t crc;
    uint32_t pins;
    int state;
    bool listed; /* in the hash table */
    uint8_t *data;
} ce;

_Static_assert(sizeof(ce) + BLK_SLACK + 32 + 2 * sizeof(ce *) <= ECCACHE_ENTRY_OVERHEAD,
               "ECCACHE_ENTRY_OVERHEAD covers an entry's struct, slack, allocator headers and table slots");

enum { CO_OPENING = 0, CO_OPEN };

typedef struct co {
    struct co *hnext;
    struct co *prev, *next; /* LRU, OPEN objects only */
    uint32_t volume;
    uint8_t oid[16];
    chunk_obj *o;
    uint64_t *sums; /* the object's record sums: chunk_obj keeps a pointer to them, and the part may go first */
    uint32_t pins;
    unsigned fds; /* counted against the fd budget from the reservation (OPENING) on */
    int state;
    bool listed; /* in the table: found by readers; an object taken out of it is closed by its last reader */
} co;

/* The entries of one object (by oid), so that eccache_forget walks them and not the whole table. */
typedef struct og {
    struct og *hnext;
    uint8_t oid[16];
    ce *first;
} og;

typedef struct vu {
    uint32_t volume;
    uint8_t u[16];
} vu;

struct eccache_tier {
    tier *t;
    pthread_mutex_t mu; /* a leaf: never held across I/O, an open or close, a decode or another lock */
    pthread_cond_t cv;  /* a LOADING entry or an OPENING object is done */
    ce **eh;
    size_t ehn, nent;
    ce lru; /* sentinel: lru.next is the most recent */
    co **oh;
    size_t ohn, nobj;
    co olru;
    og **gh;
    size_t ghn, ngrp;
    uint64_t budget, bytes, fd_budget, fds;
    eccache_counters n; /* the counters (bytes, entries, objs_open, fds_open, budgets filled in on read) */
    vu *vus;
    size_t nvus;
    pthread_mutex_t be_mu; /* the fallback backend's open (I/O): see the file header */
    _Atomic(chunk_backend *) be;
    bool be_owned;
    int64_t be_fail_ms;
    int be_rc;
};

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static uint64_t ld64(const uint8_t *b)
{
    uint64_t v;
    memcpy(&v, b, 8);
    return v;
}

static size_t ce_hash(const struct eccache_tier *c, const uint8_t oid[16], uint64_t off)
{
    uint64_t h = off * 0x9e3779b97f4a7c15ull;
    if (eccache_mutant != ECC_MUT_KEYOFF)
        h ^= ld64(oid) ^ (ld64(oid + 8) * 0xc2b2ae3d27d4eb4full);
    h ^= h >> 29;
    return (size_t)(h & (c->ehn - 1));
}

static bool ce_is(const ce *x, const uint8_t oid[16], uint64_t off)
{
    return x->off == off && (eccache_mutant == ECC_MUT_KEYOFF || memcmp(x->oid, oid, 16) == 0);
}

static ce *ce_find(struct eccache_tier *c, const uint8_t oid[16], uint64_t off)
{
    for (ce *x = c->eh[ce_hash(c, oid, off)]; x; x = x->hnext)
        if (ce_is(x, oid, off))
            return x;
    return NULL;
}

/* A failed growth leaves the table as it is (longer chains, still right). */
static void ce_grow(struct eccache_tier *c)
{
    if (c->nent < 2 * c->ehn)
        return;
    size_t n = c->ehn * 2;
    ce **nh = calloc(n, sizeof *nh);
    if (!nh)
        return;
    ce **old = c->eh;
    size_t on = c->ehn;
    c->eh = nh;
    c->ehn = n;
    for (size_t i = 0; i < on; i++)
        for (ce *x = old[i], *nx; x; x = nx) {
            nx = x->hnext;
            size_t h = ce_hash(c, x->oid, x->off);
            x->hnext = nh[h];
            nh[h] = x;
        }
    free(old);
}

static size_t og_hash(const struct eccache_tier *c, const uint8_t oid[16])
{
    uint64_t h = ld64(oid) ^ ld64(oid + 8) * 0x9e3779b97f4a7c15ull;
    h ^= h >> 31;
    return (size_t)(h & (c->ghn - 1));
}

static og *og_find(struct eccache_tier *c, const uint8_t oid[16])
{
    for (og *g = c->gh[og_hash(c, oid)]; g; g = g->hnext)
        if (memcmp(g->oid, oid, 16) == 0)
            return g;
    return NULL;
}

static void og_grow(struct eccache_tier *c)
{
    if (c->ngrp < 2 * c->ghn)
        return;
    size_t n = c->ghn * 2;
    og **nh = calloc(n, sizeof *nh);
    if (!nh)
        return;
    og **old = c->gh;
    size_t on = c->ghn;
    c->gh = nh;
    c->ghn = n;
    for (size_t i = 0; i < on; i++)
        for (og *g = old[i], *nx; g; g = nx) {
            nx = g->hnext;
            size_t h = og_hash(c, g->oid);
            g->hnext = nh[h];
            nh[h] = g;
        }
    free(old);
}

/* Lists x (LOADING) in the table and in its object's group; *spare is a group allocated outside the leaf, taken when
 * the object has none yet (mu held). */
static void ce_insert(struct eccache_tier *c, ce *x, og **spare)
{
    size_t h = ce_hash(c, x->oid, x->off);
    x->hnext = c->eh[h];
    c->eh[h] = x;
    x->listed = true;
    c->nent++;
    og *g = og_find(c, x->oid);
    if (!g) {
        g = *spare;
        *spare = NULL;
        memcpy(g->oid, x->oid, 16);
        size_t gh = og_hash(c, g->oid);
        g->hnext = c->gh[gh];
        c->gh[gh] = g;
        c->ngrp++;
        og_grow(c);
    }
    x->grp = g;
    x->gprev = NULL;
    x->gnext = g->first;
    if (g->first)
        g->first->gprev = x;
    g->first = x;
    ce_grow(c);
}

static void ce_unhash(struct eccache_tier *c, ce *x)
{
    if (!x->listed)
        return;
    for (ce **pp = &c->eh[ce_hash(c, x->oid, x->off)]; *pp; pp = &(*pp)->hnext)
        if (*pp == x) {
            *pp = x->hnext;
            x->hnext = NULL;
            break;
        }
    x->listed = false;
    c->nent--;
    og *g = x->grp;
    if (x->gprev)
        x->gprev->gnext = x->gnext;
    else
        g->first = x->gnext;
    if (x->gnext)
        x->gnext->gprev = x->gprev;
    x->gprev = x->gnext = NULL;
    x->grp = NULL;
    if (!g->first) { /* the object's last entry: its group goes (a small free under the leaf, no I/O) */
        for (og **pp = &c->gh[og_hash(c, g->oid)]; *pp; pp = &(*pp)->hnext)
            if (*pp == g) {
                *pp = g->hnext;
                break;
            }
        c->ngrp--;
        free(g);
    }
}

static void lru_unlink(ce *x)
{
    x->prev->next = x->next;
    x->next->prev = x->prev;
    x->prev = x->next = NULL;
}

static void lru_front(struct eccache_tier *c, ce *x)
{
    if (x->prev)
        lru_unlink(x);
    x->next = c->lru.next;
    x->prev = &c->lru;
    c->lru.next->prev = x;
    c->lru.next = x;
}

/* Takes a READY entry out of the table and the LRU (mu held); it is freed by whoever lets go of it last. */
/* What an entry costs against the budget: its bytes, its slack, its struct, the allocator's headers and its share of
 * the tables (an entry of a slow series' granule is a few hundred bytes, so the overhead is not small beside it). */
static uint64_t ce_cost(const ce *x)
{
    return x->len + ECCACHE_ENTRY_OVERHEAD;
}

/* Takes x out of the table, its group, the LRU and the bytes (mu held). Only a READY entry is in the LRU and the
 * bytes; a LOADING one taken out goes on to its reader (and its waiters look again), never into the cache. */
static void ce_remove(struct eccache_tier *c, ce *x)
{
    ce_unhash(c, x);
    if (x->prev) {
        lru_unlink(x);
        c->bytes -= ce_cost(x);
    }
}

/* Evicts unpinned entries from the cold end until the bytes fit the budget; the victims go to *vict (mu held). */
static void ce_evict(struct eccache_tier *c, ce **vict)
{
    for (ce *x = c->lru.prev; c->bytes > c->budget && x != &c->lru;) {
        ce *pv = x->prev;
        if (x->pins == 0) {
            ce_remove(c, x);
            c->n.evictions++;
            x->hnext = *vict;
            *vict = x;
        }
        x = pv;
    }
}

static void ce_free_list(ce *x)
{
    for (ce *nx; x; x = nx) {
        nx = x->hnext;
        free(x->data);
        free(x);
    }
}

/* ------------------------------------------------------------ objects */

static size_t co_hash(const struct eccache_tier *c, uint32_t volume, const uint8_t oid[16])
{
    uint64_t h = (ld64(oid) ^ ld64(oid + 8) * 0x9e3779b97f4a7c15ull) + volume;
    h ^= h >> 31;
    return (size_t)(h & (c->ohn - 1));
}

static co *co_find(struct eccache_tier *c, uint32_t volume, const uint8_t oid[16])
{
    for (co *o = c->oh[co_hash(c, volume, oid)]; o; o = o->hnext)
        if (o->volume == volume && memcmp(o->oid, oid, 16) == 0)
            return o;
    return NULL;
}

static void co_grow(struct eccache_tier *c)
{
    if (c->nobj < 2 * c->ohn)
        return;
    size_t n = c->ohn * 2;
    co **nh = calloc(n, sizeof *nh);
    if (!nh)
        return;
    co **old = c->oh;
    size_t on = c->ohn;
    c->oh = nh;
    c->ohn = n;
    for (size_t i = 0; i < on; i++)
        for (co *o = old[i], *nx; o; o = nx) {
            nx = o->hnext;
            size_t h = co_hash(c, o->volume, o->oid);
            o->hnext = nh[h];
            nh[h] = o;
        }
    free(old);
}

static void co_lru_unlink(co *o)
{
    o->prev->next = o->next;
    o->next->prev = o->prev;
    o->prev = o->next = NULL;
}

static void co_lru_front(struct eccache_tier *c, co *o)
{
    if (o->prev)
        co_lru_unlink(o);
    o->next = c->olru.next;
    o->prev = &c->olru;
    c->olru.next->prev = o;
    c->olru.next = o;
}

/* Takes o out of the table and the LRU, and its fds out of the count (mu held); its reader, or the caller when none
 * holds it, closes it. */
static void co_remove(struct eccache_tier *c, co *o)
{
    if (!o->listed)
        return;
    for (co **pp = &c->oh[co_hash(c, o->volume, o->oid)]; *pp; pp = &(*pp)->hnext)
        if (*pp == o) {
            *pp = o->hnext;
            o->hnext = NULL;
            break;
        }
    o->listed = false;
    c->nobj--;
    if (o->prev)
        co_lru_unlink(o);
    c->fds -= o->fds;
    o->fds = 0;
}

/* Closes unpinned objects from the cold end until `need` more fds fit the budget (mu held); victims to *vict. */
static void co_evict(struct eccache_tier *c, uint64_t need, co **vict)
{
    for (co *o = c->olru.prev; c->fds + need > c->fd_budget && o != &c->olru;) {
        co *pv = o->prev;
        if (o->pins == 0) {
            co_remove(c, o);
            o->hnext = *vict;
            *vict = o;
        }
        o = pv;
    }
}

static void co_close_list(struct eccache_tier *c, co *o)
{
    uint64_t n = 0;
    for (co *nx; o; o = nx) {
        nx = o->hnext;
        if (o->o) {
            chunk_obj_close(o->o);
            n++;
        }
        free(o->sums);
        free(o);
    }
    if (n) {
        pthread_mutex_lock(&c->mu);
        c->n.obj_closes += n;
        pthread_mutex_unlock(&c->mu);
    }
}

/* The CHUNK_ERR_* codes are not the engine's: a store another owner holds or one that failed is an I/O error here. */
static int map_rc(int rc)
{
    if (rc == TSDB_ERR_NOTFOUND)
        return TSDB_ERR_CORRUPT; /* every store UP and no chunk of the object: it is lost (chunk_obj_open) */
    return rc <= -100 ? TSDB_ERR_IO : rc;
}

/* Gives the reader the object of e, open and pinned (obj_put lets go of it). */
static int obj_get(struct eccache_tier *c, const struct ecpart *e, co **out)
{
    const ec_record *r = &e->rec;
    chunk_geom gm;
    if (chunk_geom_make(&gm, r->k, r->m, r->cell_bytes, r->object_len) != 0 || r->nsums != chunk_sums_total(&gm))
        return TSDB_ERR_CORRUPT; /* r7-ecfile's open checks the record; never trusted further than this */
    unsigned need = chunk_npos(&gm);
    co *nw = NULL;
    co *vict = NULL;
    pthread_mutex_lock(&c->mu);
    for (;;) {
        co *o = co_find(c, e->volume, r->oid);
        if (o && o->state == CO_OPENING) {
            pthread_cond_wait(&c->cv, &c->mu);
            continue;
        }
        if (o) {
            o->pins++;
            co_lru_front(c, o);
            pthread_mutex_unlock(&c->mu);
            free(nw ? nw->sums : NULL);
            free(nw);
            *out = o;
            return 0;
        }
        if (nw)
            break;
        pthread_mutex_unlock(&c->mu); /* allocations outside the leaf */
        nw = calloc(1, sizeof *nw);
        uint64_t *sums = nw ? malloc(r->nsums * sizeof *sums) : NULL;
        if (!sums) {
            free(nw);
            return TSDB_ERR_NOMEM;
        }
        memcpy(sums, r->sums, r->nsums * sizeof *sums);
        nw->sums = sums;
        nw->volume = e->volume;
        memcpy(nw->oid, r->oid, 16);
        pthread_mutex_lock(&c->mu);
    }
    co_evict(c, need, &vict);
    nw->state = CO_OPENING;
    nw->pins = 1;
    nw->fds = need;
    c->fds += need;
    if (c->fds > c->n.fds_peak)
        c->n.fds_peak = c->fds;
    size_t h = co_hash(c, nw->volume, nw->oid);
    nw->hnext = c->oh[h];
    c->oh[h] = nw;
    nw->listed = true;
    c->nobj++;
    co_grow(c);
    pthread_mutex_unlock(&c->mu);
    co_close_list(c, vict);

    int rc = 0;
    chunk_backend *be = eccache_backend(c->t, &rc);
    if (be) {
        uint8_t u[16];
        eccache_vuuid(c->t, e->volume, u);
        rc = chunk_obj_open(be, e->volume, u, r->oid, &gm, nw->sums, &nw->o);
    }
    if (eccache_test_hook)
        eccache_test_hook(c->t, ECC_HOOK_OPENED, r->oid);
    pthread_mutex_lock(&c->mu);
    if (rc) {
        co_remove(c, nw);
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->mu);
        free(nw->sums);
        free(nw);
        return map_rc(rc);
    }
    nw->state = CO_OPEN;
    c->n.obj_opens++;
    if (nw->listed) /* else forgotten while it opened: its reader closes it */
        co_lru_front(c, nw);
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->mu);
    *out = nw;
    return 0;
}

/* Lets go of o. `stale`: the read just made saw it degraded (a cell that failed or was rebuilt, a position away, an
 * error), so it leaves the table and the next reader opens it again: a chunk that heal or replace wrote since is a new
 * file (chunkheal.c), and a position that was away at the open may be back; an open object keeps its fds on the old
 * inodes and its positions' state for its life. */
static void obj_put(struct eccache_tier *c, co *o, bool stale)
{
    co *vict = NULL;
    pthread_mutex_lock(&c->mu);
    o->pins--;
    if (stale && eccache_mutant != ECC_MUT_KEEPSTALE)
        co_remove(c, o);
    if (o->pins == 0 && !o->listed) {
        vict = o; /* out of the table (stale, forgotten, reopened): closed by its last reader */
        o->hnext = NULL;
    } else {
        co_evict(c, 0, &vict);
    }
    pthread_mutex_unlock(&c->mu);
    co_close_list(c, vict);
}

/* ------------------------------------------------------------ the read */

static long gran_index(const part *p, uint64_t off)
{
    uint32_t lo = 0, hi = p->ix.n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (gix_get(&p->ix, mid, GF_OFF) < off)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < p->ix.n && gix_get(&p->ix, lo, GF_OFF) == off ? (long)lo : -1;
}

static void corrupt_line(const part *p, const struct ecpart *e, const granule *g, const char *why)
{
    char hex[33];
    chunk_oid_hex(e->rec.oid, hex);
    fprintf(stderr, "tsdb: volume %" PRIu32 ", partition %" PRId64 ", part %s, granule %ld (offset %" PRIu64
                    ", %" PRIu64 " bytes): %s (object %s): TSDB_ERR_CORRUPT\n",
            e->volume, p->partition, p->name, gran_index(p, g->off), g->off, (uint64_t)g->ts_len + g->val_len, why,
            hex);
}

/* Reads the granule into x->data and checks it (E11); the reader holds x (LOADING, pinned). */
static int fill(struct eccache_tier *c, const part *p, const struct ecpart *e, const granule *g, ce *x)
{
    co *o = NULL;
    int rc = obj_get(c, e, &o);
    if (rc) {
        if (rc == TSDB_ERR_CORRUPT)
            corrupt_line(p, e, g, "the object cannot be opened");
        return rc;
    }
    chunk_report rep;
    memset(&rep, 0, sizeof rep);
    rc = chunk_obj_read(o->o, x->off, x->len, x->data, CHUNK_RR_RAW, &rep);
    atomic_fetch_add_explicit(&granule_checks, 1, memory_order_relaxed);
    bool ok = rc == 0 && (eccache_mutant == ECC_MUT_NOVERIFY || (uint32_t)xxh64(x->data, x->len, 0) == x->crc);
    bool retried = false;
    /* Only a mismatch is read again. A RAW read that fails (UNAVAILABLE, CORRUPT) met erasures: positions absent,
     * away or unreadable, the same ones a whole-cell read meets, which can only add the cells that fail their sums;
     * reading again would double the I/O of every miss while a store is away and answer the same. */
    if ((rc == 0 || eccache_mutant == ECC_MUT_RETRYERR) && !ok && rc != TSDB_ERR_NOMEM && rc != TSDB_ERR_INVALID &&
        eccache_mutant != ECC_MUT_NORETRY) {
        /* rot that RAW cannot see: whole cells against the record's sums */
        pthread_mutex_lock(&c->mu);
        c->n.retries++;
        pthread_mutex_unlock(&c->mu);
        retried = true;
        rc = chunk_obj_read(o->o, x->off, x->len, x->data, 0, &rep);
        ok = rc == 0 && (eccache_mutant == ECC_MUT_NOVERIFY || eccache_mutant == ECC_MUT_NORECHECK ||
                         (uint32_t)xxh64(x->data, x->len, 0) == x->crc);
    }
    obj_put(c, o, rc != 0 || retried || rep.bad_cells || rep.decoded_cells);
    bool mismatch = !ok && rc == 0;
    if (mismatch)
        rc = TSDB_ERR_CORRUPT;
    if (rc == TSDB_ERR_CORRUPT) {
        corrupt_line(p, e, g, mismatch ? "its bytes fail its checksum after a decode of whole cells"
                                       : "its object lost more chunks of a stripe than it has parity");
        pthread_mutex_lock(&c->mu);
        c->n.corrupt++;
        pthread_mutex_unlock(&c->mu);
    }
    return map_rc(rc);
}

static struct ecpart *part_ec(const part *p)
{
    return atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
}

int ec_granule_read(codec_ctx *cx, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                    bool verify)
{
    EC_CALLED(EC_CALL_GRANULE_READ);
    (void)verify; /* every .ec granule is checked once, as it enters the cache (E11) */
    struct ecpart *e = part_ec(p);
    if (e->test_mark)
        return granule_read_map(cx, p, g, type, ts, v, cap, verify);
    struct eccache_tier *c = e->tier ? e->tier->cache : NULL;
    if (!c)
        return TSDB_ERR_INVALID; /* an .ec part is opened only with a tier (r7-tier refuses the rest) */
    uint64_t len = (uint64_t)g->ts_len + g->val_len;
    if (len == 0 || g->off < p->hdr_len || g->off > p->index_off || len > p->index_off - g->off ||
        p->index_off > e->rec.object_len) {
        corrupt_line(p, e, g, "the index places it outside the object's granules");
        return TSDB_ERR_CORRUPT;
    }
    ce *nw = NULL, *x, *vict = NULL;
    og *spare = NULL;
    pthread_mutex_lock(&c->mu);
    for (;;) {
        x = ce_find(c, e->rec.oid, g->off);
        if (x && x->state == CE_LOADING) {
            c->n.waits++;
            pthread_cond_wait(&c->cv, &c->mu);
            continue;
        }
        if (x) {
            x->pins++;
            c->n.hits++;
            lru_front(c, x);
            break;
        }
        if (nw) {
            x = nw;
            nw = NULL;
            x->state = CE_LOADING;
            x->pins = 1;
            ce_insert(c, x, &spare);
            c->n.misses++;
            break;
        }
        pthread_mutex_unlock(&c->mu); /* allocations outside the leaf */
        nw = calloc(1, sizeof *nw);
        spare = nw ? calloc(1, sizeof *spare) : NULL;
        /* BLK_SLACK zeroed bytes past the granule: the decoders read up to that far beyond a block (in a mapped
         * part the footer covers it), and granule_decode runs on this buffer */
        if (!nw || !spare || !(nw->data = malloc(len + BLK_SLACK))) {
            free(nw);
            free(spare);
            return TSDB_ERR_NOMEM;
        }
        memset(nw->data + len, 0, BLK_SLACK);
        memcpy(nw->oid, e->rec.oid, 16);
        nw->off = g->off;
        nw->len = len;
        nw->crc = g->crc;
        pthread_mutex_lock(&c->mu);
    }
    pthread_mutex_unlock(&c->mu);
    free(spare); /* the object had a group, or another reader filled the entry meanwhile */
    if (nw) {
        free(nw->data);
        free(nw);
    }
    int rc = 0;
    if (x->state == CE_LOADING) {
        rc = fill(c, p, e, g, x);
        if (eccache_test_hook)
            eccache_test_hook(c->t, ECC_HOOK_FILLED, e->rec.oid);
        pthread_mutex_lock(&c->mu);
        if (rc) {
            ce_unhash(c, x);
        } else if (!x->listed) {
            x->state = CE_READY; /* forgotten while it was read: its reader decodes it and frees it */
        } else if (ce_cost(x) > c->budget) {
            /* larger than the whole budget: used by its reader and dropped, without emptying the cache for it */
            ce_unhash(c, x);
            x->state = CE_READY;
            c->n.uncached++;
        } else {
            x->state = CE_READY;
            c->bytes += ce_cost(x);
            lru_front(c, x);
            ce_evict(c, &vict);
        }
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->mu);
        ce_free_list(vict);
        vict = NULL;
    } else if (x->len != len || x->crc != g->crc) {
        /* the index says another granule is at this offset than the one cached: it changed in the mapping since */
        corrupt_line(p, e, g, "the index no longer matches the granule read before at this offset");
        rc = TSDB_ERR_CORRUPT;
    }
    if (rc == 0)
        rc = granule_decode(cx, p, g, x->data, type, ts, v, cap, false); /* from the pinned entry */
    pthread_mutex_lock(&c->mu);
    x->pins--;
    if (x->pins == 0 && !x->listed) {
        x->hnext = vict;
        vict = x;
    } else {
        ce_evict(c, &vict);
    }
    pthread_mutex_unlock(&c->mu);
    ce_free_list(vict);
    return rc;
}

/* ------------------------------------------------------------ statistics */

uint64_t eccache_part_bytes(const part *p)
{
    EC_CALLED(EC_CALL_PART_BYTES);
    struct ecpart *e = part_ec(p);
    return e && !e->test_mark && e->rec.object_len ? e->rec.object_len : p->map_len;
}

uint64_t eccache_resident(const part *p, uint64_t off, uint64_t len)
{
    EC_CALLED(EC_CALL_RESIDENT);
    struct ecpart *e = part_ec(p);
    if (!e || e->test_mark) {
        if (off > p->map_len || len > p->map_len - off)
            return 0;
        return resident_bytes_range(p->map + off, (size_t)len);
    }
    struct eccache_tier *c = e->tier ? e->tier->cache : NULL;
    if (!c || len == 0 || off + len < off)
        return 0;
    /* the granules whose offsets lie in the range (offsets ascend in a part's index; a damaged one counts less) */
    uint32_t n = p->ix.n, lo = 0, hi = n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (gix_get(&p->ix, mid, GF_OFF) < off)
            lo = mid + 1;
        else
            hi = mid;
    }
    uint64_t got = 0;
    uint32_t i = lo;
    while (i < n) {
        uint64_t offs[256];
        uint32_t k = 0;
        for (; i < n && k < 256; i++) {
            uint64_t o = gix_get(&p->ix, i, GF_OFF);
            if (o >= off + len)
                break;
            offs[k++] = o;
        }
        if (!k)
            break;
        pthread_mutex_lock(&c->mu); /* in batches: a whole part's walk never holds the leaf for long */
        for (uint32_t j = 0; j < k; j++) {
            ce *x = ce_find(c, e->rec.oid, offs[j]);
            if (x && x->state == CE_READY)
                got += MIN(x->len, off + len - offs[j]);
        }
        pthread_mutex_unlock(&c->mu);
        if (k < 256)
            break;
    }
    return got;
}

/* ------------------------------------------------------------ the tier's */

int eccache_tier_open(struct tier *t)
{
    EC_CALLED(EC_CALL_TIER_OPEN);
    struct eccache_tier *c = calloc(1, sizeof *c);
    if (!c)
        return TSDB_ERR_NOMEM;
    c->t = t;
    c->ehn = 1024;
    c->ohn = 64;
    c->ghn = 64;
    c->eh = calloc(c->ehn, sizeof *c->eh);
    c->oh = calloc(c->ohn, sizeof *c->oh);
    c->gh = calloc(c->ghn, sizeof *c->gh);
    if (!c->eh || !c->oh || !c->gh) {
        free(c->eh);
        free(c->oh);
        free(c->gh);
        free(c);
        return TSDB_ERR_NOMEM;
    }
    pthread_mutex_init(&c->mu, NULL);
    pthread_cond_init(&c->cv, NULL);
    pthread_mutex_init(&c->be_mu, NULL);
    c->lru.next = c->lru.prev = &c->lru;
    c->olru.next = c->olru.prev = &c->olru;
    if (t->cfg.cache_mb) {
        c->budget = t->cfg.cache_mb << 20;
    } else {
        long pages = sysconf(_SC_PHYS_PAGES), psz = sysconf(_SC_PAGESIZE);
        c->budget = pages > 0 && psz > 0 ? (uint64_t)pages * (uint64_t)psz / 10 : 256ull << 20;
    }
    struct rlimit rl;
    uint64_t nofile = getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY ? rl.rlim_cur : 4096;
    c->fd_budget = MIN(MAX(nofile / 4, 64), 4096); /* a quarter of the process's: tsdbd's connections need the rest */
    t->cache = c;
    return 0;
}

void eccache_tier_close(struct tier *t)
{
    EC_CALLED(EC_CALL_TIER_CLOSE);
    struct eccache_tier *c = t->cache;
    if (!c)
        return;
    /* the last reference: no reader is left */
    for (size_t i = 0; i < c->ehn; i++)
        ce_free_list(c->eh[i]);
    for (size_t i = 0; i < c->ghn; i++)
        for (og *g = c->gh[i], *nx; g; g = nx) {
            nx = g->hnext;
            free(g);
        }
    for (size_t i = 0; i < c->ohn; i++)
        for (co *o = c->oh[i], *nx; o; o = nx) {
            nx = o->hnext;
            if (o->o)
                chunk_obj_close(o->o);
            free(o->sums);
            free(o);
        }
    chunk_backend *be = atomic_load(&c->be);
    if (be && c->be_owned)
        chunk_be_close(be);
    free(c->eh);
    free(c->oh);
    free(c->gh);
    free(c->vus);
    pthread_mutex_destroy(&c->mu);
    pthread_cond_destroy(&c->cv);
    pthread_mutex_destroy(&c->be_mu);
    free(c);
    t->cache = NULL;
}

int eccache_set_backend(struct tier *t, struct chunk_backend *be)
{
    struct eccache_tier *c = t->cache;
    pthread_mutex_lock(&c->be_mu);
    chunk_backend *old = atomic_load(&c->be);
    /* The fallback is read without a reference (readers load c->be and open objects through it), so it is never
     * closed under them; and it holds every store's flock, so a backend opened beside it could not have been. */
    int rc = old && c->be_owned ? TSDB_ERR_LOCKED : 0;
    if (rc == 0)
        atomic_store(&c->be, be);
    pthread_mutex_unlock(&c->be_mu);
    return rc;
}

struct chunk_backend *eccache_backend(struct tier *t, int *rc)
{
    struct eccache_tier *c = t->cache;
    chunk_backend *be = atomic_load_explicit(&c->be, memory_order_acquire);
    if (be)
        return be;
    pthread_mutex_lock(&c->be_mu);
    be = atomic_load(&c->be);
    if (!be) {
        int64_t now = mono_ms();
        if (c->be_fail_ms && now - c->be_fail_ms < 1000) {
            *rc = c->be_rc;
        } else {
            chunk_be_opts o = {.min_up = t->cfg.ndirs - t->cfg.m};
            int r = chunk_be_open2(&be, (const char *const *)t->cfg.dirs, t->cfg.ndirs, NULL, &o);
            if (r) {
                be = NULL;
                c->be_rc = *rc = map_rc(r);
                c->be_fail_ms = now ? now : 1;
            } else {
                c->be_owned = true;
                c->be_fail_ms = 0;
                atomic_store_explicit(&c->be, be, memory_order_release);
            }
        }
    }
    pthread_mutex_unlock(&c->be_mu);
    return be;
}

void eccache_set_vuuid(struct tier *t, uint32_t volume, const uint8_t vuuid[16])
{
    struct eccache_tier *c = t->cache;
    pthread_mutex_lock(&c->mu);
    size_t i = 0;
    while (i < c->nvus && c->vus[i].volume != volume)
        i++;
    if (i == c->nvus) {
        vu *n = realloc(c->vus, (c->nvus + 1) * sizeof *n);
        if (!n) {
            pthread_mutex_unlock(&c->mu);
            return;
        }
        c->vus = n;
        c->nvus++;
        c->vus[i].volume = volume;
    }
    memcpy(c->vus[i].u, vuuid, 16);
    pthread_mutex_unlock(&c->mu);
}

void eccache_vuuid(struct tier *t, uint32_t volume, uint8_t out[16])
{
    struct eccache_tier *c = t->cache;
    memset(out, 0, 16);
    pthread_mutex_lock(&c->mu);
    for (size_t i = 0; i < c->nvus; i++)
        if (c->vus[i].volume == volume)
            memcpy(out, c->vus[i].u, 16);
    pthread_mutex_unlock(&c->mu);
}

/* Takes the object's entries out (each freed now, or by its reader: a fill in flight goes on to its reader and is
 * never cached) and the object out of the table, opening or open (closed now, or by its last reader). */
static void drop_object(struct eccache_tier *c, uint32_t volume, const uint8_t oid[16], bool entries)
{
    ce *vict = NULL;
    co *ov = NULL;
    pthread_mutex_lock(&c->mu);
    og *g = entries && eccache_mutant != ECC_MUT_FORGETNOOP ? og_find(c, oid) : NULL;
    while (g) {
        ce *x = g->first;
        bool last = x->gnext == NULL; /* the group is freed with its last entry */
        ce_remove(c, x);
        if (x->pins == 0) {
            x->hnext = vict;
            vict = x;
        }
        if (last)
            break;
    }
    co *o = eccache_mutant != ECC_MUT_FORGETNOOP ? co_find(c, volume, oid) : NULL;
    if (o) {
        co_remove(c, o);
        if (o->pins == 0) {
            o->hnext = NULL;
            ov = o;
        }
    }
    if (o || vict)
        pthread_cond_broadcast(&c->cv); /* waiters on a LOADING entry or an OPENING object taken out look again */
    pthread_mutex_unlock(&c->mu);
    ce_free_list(vict);
    co_close_list(c, ov);
}

void eccache_forget(struct tier *t, uint32_t volume, const uint8_t oid[16])
{
    struct eccache_tier *c = t ? t->cache : NULL;
    if (c)
        drop_object(c, volume, oid, true);
}

void eccache_reopen(struct tier *t, uint32_t volume, const uint8_t oid[16])
{
    struct eccache_tier *c = t ? t->cache : NULL;
    if (c)
        drop_object(c, volume, oid, false);
}

/* ------------------------------------------------------------ tests */

void eccache_get_counters(struct tier *t, eccache_counters *out)
{
    struct eccache_tier *c = t->cache;
    pthread_mutex_lock(&c->mu);
    *out = c->n;
    out->bytes = c->bytes;
    out->entries = c->nent;
    out->objs_open = c->nobj;
    out->fds_open = c->fds;
    out->budget = c->budget;
    out->fd_budget = c->fd_budget;
    pthread_mutex_unlock(&c->mu);
}

void eccache_test_budget(struct tier *t, uint64_t bytes)
{
    struct eccache_tier *c = t->cache;
    ce *vict = NULL;
    pthread_mutex_lock(&c->mu);
    c->budget = bytes;
    ce_evict(c, &vict);
    pthread_mutex_unlock(&c->mu);
    ce_free_list(vict);
}

void eccache_test_fd_budget(struct tier *t, uint64_t fds)
{
    struct eccache_tier *c = t->cache;
    co *vict = NULL;
    pthread_mutex_lock(&c->mu);
    c->fd_budget = fds;
    c->n.fds_peak = c->fds;
    co_evict(c, 0, &vict);
    pthread_mutex_unlock(&c->mu);
    co_close_list(c, vict);
}

void eccache_test_drop_entries(struct tier *t)
{
    struct eccache_tier *c = t->cache;
    ce *vict = NULL;
    pthread_mutex_lock(&c->mu);
    uint64_t b = c->budget;
    c->budget = 0;
    ce_evict(c, &vict);
    c->budget = b;
    pthread_mutex_unlock(&c->mu);
    ce_free_list(vict);
}

void eccache_test_drop(struct tier *t)
{
    struct eccache_tier *c = t->cache;
    ce *vict = NULL;
    co *ov = NULL;
    pthread_mutex_lock(&c->mu);
    uint64_t b = c->budget, f = c->fd_budget;
    c->budget = 0;
    c->fd_budget = 0;
    ce_evict(c, &vict);
    co_evict(c, 0, &ov);
    c->budget = b;
    c->fd_budget = f;
    pthread_mutex_unlock(&c->mu);
    ce_free_list(vict);
    co_close_list(c, ov);
}

static int attach_locked_check(eng *db, part *p)
{
    int rc = TSDB_ERR_NOTFOUND;
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i] == p) {
            rc = 0;
            break;
        }
    if (rc == 0 && p->merging)
        rc = TSDB_ERR_LOCKED;
    if (rc == 0 && p->ec)
        rc = TSDB_ERR_INVALID;
    return rc;
}

static int rec_copy(ec_record *dst, const ec_record *src)
{
    *dst = *src;
    dst->sums = malloc(MAX(src->nsums, 1) * sizeof *dst->sums);
    if (!dst->sums)
        return TSDB_ERR_NOMEM;
    memcpy(dst->sums, src->sums, src->nsums * sizeof *dst->sums);
    return 0;
}

int ec_test_attach_object(eng *db, part *p, const ec_record *rec)
{
    if (!db->tier || !db->tier->cache)
        return TSDB_ERR_INVALID;
    struct ecpart *e = calloc(1, sizeof *e);
    if (!e || rec_copy(&e->rec, rec) != 0) {
        free(e);
        return TSDB_ERR_NOMEM;
    }
    e->tier = db->tier;
    e->volume = db->tier_vol;
    pthread_mutex_lock(&db->lock);
    int rc = attach_locked_check(db, p);
    if (rc == 0)
        atomic_store_explicit(&p->ec, e, memory_order_release); /* readers run beside this store */
    pthread_mutex_unlock(&db->lock);
    if (rc)
        ecpart_free(e);
    return rc;
}

int ec_test_attach(eng *db, part *p, ec_record *rec_out)
{
    return ec_test_attach_from(db, p, NULL, rec_out);
}

int ec_test_attach_from(eng *db, part *p, const uint8_t *src, ec_record *rec_out)
{
    tier *t = db->tier;
    if (!t || !t->cache || p->ix.n == 0)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&db->lock);
    int rc = attach_locked_check(db, p);
    pthread_mutex_unlock(&db->lock);
    if (rc)
        return rc;
    chunk_backend *be = eccache_backend(t, &rc);
    if (!be)
        return rc;
    ec_record r;
    memset(&r, 0, sizeof r);
    chunk_geom g;
    if ((rc = chunk_geom_derive(&g, (unsigned)t->cfg.k, (unsigned)t->cfg.m, p->map_len)) != 0)
        return rc;
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    chunk_random(r.oid, sizeof r.oid);
    r.key = p->partition;
    r.min_block = p->min_block;
    r.max_block = p->max_block;
    r.level = p->level;
    r.k = g.k;
    r.m = g.m;
    r.cell_bytes = g.cell;
    r.object_len = p->map_len;
    if (!src)
        src = p->map;
    r.object_xxh64 = xxh64(src, p->map_len, 0);
    r.writer_epoch = 1;
    r.settle_ms = eng_now_ms(db);
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    if (!(r.sums = malloc(r.nsums * sizeof *r.sums)))
        return TSDB_ERR_NOMEM;
    uint8_t u[16];
    eccache_vuuid(t, db->tier_vol, u);
    rc = chunk_be_see_epoch(be, db->tier_vol, 1);
    if (rc == 0)
        rc = chunk_be_put_object(be, db->tier_vol, u, r.oid, &g, 1, src, ~0u, r.sums);
    if (rc == 0 && rec_out)
        rc = rec_copy(rec_out, &r); /* before the attach: a failure after it would report a part left attached */
    if (rc == 0) {
        rc = ec_test_attach_object(db, p, &r);
        if (rc && rec_out) {
            free(rec_out->sums);
            rec_out->sums = NULL;
        }
    }
    free(r.sums);
    return rc <= -100 ? TSDB_ERR_IO : rc; /* the chunk layer's own codes */
}
