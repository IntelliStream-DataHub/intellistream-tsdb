/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The granule cache and the open objects of a tier (src/eccache.h has the design).
 *
 * Granule entries are sharded into ECCACHE_SHARDS (64) cache-line aligned stripes keyed by hash(oid, offset). Each
 * stripe owns its own leaf mutex (mu), condvar (cv), hash table (eh), LRU list (lru), per-object group table (gh) and
 * counters: cache hits touch only one stripe with zero global atomics or locks. Open objects keyed by (volume, oid)
 * and prefetch walk/degraded state live under a separate leaf mutex (obj_mu), touched only on cache misses, forget and
 * reopen. No two cache mutexes are ever held at once, and nothing under any mutex does I/O.
 *
 * Prefetched windows in fill_ahead read into a single refcounted shared slab (ec_slab) so each cached granule entry
 * slices directly into slab->buf + at without per-granule allocations or memcpy, releasing the slab via tsdb_free_big
 * when its last slice evicts.
 *
 * The backend is the tier's (tier_open hands it over, eccache_set_backend), never one of the cache's own: a store flocks
 * its directory, so a second backend over the tier's stores would be BUSY at every store the tier holds (one backend
 * per handle, src/tier.h). The volumes' uuids likewise come from the tier's engines (tier_engine_attach).
 */
#include "eccache.h"
#include "chunk.h"
#include "coldops.h"
#include "compat.h"
#include "ecpart.h"
#include "stats_priv.h"
#include "tier.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

int eccache_mutant;
_Atomic int eccache_test_pf_fail;
void (*eccache_test_hook)(struct tier *t, int where, const uint8_t oid[16]);

#define ECCACHE_SHARD_BITS 6
#define ECCACHE_SHARDS (1u << ECCACHE_SHARD_BITS) /* 64 stripes */
_Static_assert(ECCACHE_SHARDS <= 64, "eccache_resident uses a 64-bit shard mask");

enum { CE_LOADING = 0, CE_READY };

struct og;

/* Refcounted shared buffer backing a prefetched window in fill_ahead. Trailing BLK_SLACK bytes at buf + total are
 * zeroed so every slice [at, at + len) has >= BLK_SLACK readable initialized bytes past its end. When a single hot
 * slice outlives all its siblings (refs == 1) in a slab larger than 4 * (len + BLK_SLACK), its next uncontended hit
 * copies it to a standalone buffer and drops the slab so 1 surviving slice cannot pin a multi-MiB slab indefinitely. */
typedef struct ec_slab {
    _Atomic uint32_t refs;
    size_t bytes; /* total allocation size (sizeof(ec_slab) + total + BLK_SLACK) */
    uint8_t buf[];
} ec_slab;

static void slab_unref(ec_slab *sl)
{
    if (sl && atomic_fetch_sub_explicit(&sl->refs, 1, memory_order_acq_rel) == 1)
        tsdb_free_big(sl, sl->bytes);
}

typedef struct ce {
    struct ce *hnext;         /* hash chain (or the victims' list once out of the table) */
    struct ce *prev, *next;   /* LRU, READY entries only */
    struct ce *gprev, *gnext; /* the entries of one object in this shard (its group), while listed */
    struct og *grp;
    uint8_t oid[16];
    uint64_t off, len;
    uint32_t crc;
    uint32_t pins;
    int state;
    _Atomic bool listed; /* in the shard's hash table */
    bool pf;             /* read ahead and not yet hit: its cost is in c->pf_bytes */
    uint8_t shard;       /* stripe index in c->shards[] */
    uint8_t *data;       /* owned buffer (slab == NULL) or direct slice into slab->buf */
    ec_slab *slab;       /* shared prefetch slab, or NULL for standalone entries */
} ce;

_Static_assert(sizeof(ce) + BLK_SLACK + 32 + 2 * sizeof(ce *) <= ECCACHE_ENTRY_OVERHEAD,
               "ECCACHE_ENTRY_OVERHEAD covers an entry's struct, slack, allocator headers and table slots");

enum { CO_OPENING = 0, CO_OPEN };

typedef struct co {
    struct co *hnext;
    struct co *prev, *next; /* LRU, OPEN objects only */
    uint32_t volume;
    uint8_t oid[16];
    cold_obj *o;
    const cold_ops *ops; /* the volume's ops that opened o: closing and reading need no context (src/coldops.h) */
    uint64_t *sums; /* the object's record sums: chunk_obj keeps a pointer to them, and the part may go first */
    uint32_t pins;
    unsigned fds; /* counted against the fd budget from the reservation (OPENING) on */
    int state;
    bool listed; /* in the table: found by readers; an object taken out of it is closed by its last reader */
} co;

/* The entries of one object (by oid) within one shard, so that eccache_forget walks them and not the whole table. */
typedef struct og {
    struct og *hnext;
    uint8_t oid[16];
    ce *first;
} og;

/* A sequential walk (prefetch): the next granule index of object oid a miss would continue it at, and its window. */
#define PF_STREAMS 16
#define PF_BAD_SLOTS 16
#define PF_BAD_TTL_NS (5ull * 1000000000ull) /* monotonic cooldown before probing a degraded object again */
#define PF_INIT 4
#define PF_CALL_BYTES (4ull << 20)
typedef struct pfstream {
    uint8_t oid[16];
    uint32_t next, n;
    uint64_t stamp; /* 0: free */
} pfstream;

/* Recently degraded objects: skip prefetch during their monotonic cooldown so damaged stripes do not issue extra
 * range reads, while transient peer restarts or out-of-band heals recover automatically once the cooldown elapses
 * (and immediately on eccache_reopen / eccache_forget). */
typedef struct pfbad {
    uint8_t oid[16];
    uint64_t stamp;    /* 0: free */
    uint64_t until_ns; /* CLOCK_MONOTONIC ns */
} pfbad;

typedef struct ec_shard {
    _Alignas(64) pthread_mutex_t mu; /* leaf lock for this stripe: never held across I/O, decode or another cache lock */
    pthread_cond_t cv;  /* a LOADING entry in this stripe is done */
    uint32_t waiters;   /* threads waiting on cv (protected by mu) */
    ce **eh;
    size_t ehn, nent;
    ce lru; /* sentinel: lru.next is the most recent */
    og **gh;
    size_t ghn, ngrp;
    _Atomic uint64_t bytes; /* READY bytes in this stripe (written under mu; relaxed reads skip empty shards) */
    _Atomic uint64_t cap;   /* per-shard budget target (budget / ECCACHE_SHARDS + remainder) */
    uint64_t hits, misses, waits, evictions, uncached;
    uint64_t prefetched, pf_used, pf_wasted, pf_dropped;
} ec_shard;

struct eccache_tier {
    tier *t;
    ec_shard *shards; /* ECCACHE_SHARDS 64-byte aligned stripes */

    /* Read-mostly configuration (own cache line: never dirtied on hits or misses) */
    _Alignas(64) _Atomic unsigned pf_cap; /* granules read ahead per read at most; 0: no prefetch */
    _Atomic(chunk_backend *) be;          /* the tier's: not owned, not closed here */

    /* Dedicated budget and occupancy line (read on unpin; isolated from miss-dirtied globals below) */
    _Alignas(64) _Atomic uint64_t budget;
    _Atomic uint64_t total_bytes;

    /* Prefetch, eviction-hand, forget and read-count coordination (never touched on warm cache hits) */
    _Alignas(64) _Atomic uint64_t pf_bytes;
    _Atomic uint64_t pf_peak;
    _Atomic uint32_t evict_hand;
    _Atomic uint32_t forget_active;
    _Atomic uint64_t forget_seq;
    _Atomic uint64_t nreads; /* obj_read calls (counted outside any lock) */

    /* Open objects and prefetch walk/degraded tracking (touched only on misses, forget, reopen) */
    _Alignas(64) pthread_mutex_t obj_mu; /* leaf lock: never held across I/O or alongside any shard mu */
    pthread_cond_t obj_cv;               /* an OPENING object is done */
    uint32_t obj_waiters;
    co **oh;
    size_t ohn, nobj;
    co olru;
    uint64_t fd_budget, fds;
    uint64_t obj_opens, obj_closes, fds_peak;
    uint64_t retries, corrupt;
    uint64_t pf_reads, pf_call_peak;
    pfstream pfs[PF_STREAMS];
    pfbad pf_bad[PF_BAD_SLOTS];
    uint64_t pf_stamp, pf_bad_stamp, pf_bad_ttl_ns;
    uint64_t pf_call; /* bytes of one window's range read at most (PF_CALL_BYTES; a test lowers it) */
};

static uint64_t ld64(const uint8_t *b)
{
    uint64_t v;
    memcpy(&v, b, 8);
    return v;
}

static uint64_t ce_hash64(const uint8_t oid[16], uint64_t off)
{
    uint64_t h = off * 0x9e3779b97f4a7c15ull;
    if (eccache_mutant != ECC_MUT_KEYOFF)
        h ^= ld64(oid) ^ (ld64(oid + 8) * 0xc2b2ae3d27d4eb4full);
    h ^= h >> 29;
    h *= 0x94d049bb133111ebull;
    h ^= h >> 31;
    return h;
}

static size_t ce_shard_idx(uint64_t h)
{
    return (size_t)(h & (ECCACHE_SHARDS - 1));
}

static size_t ce_bucket(const ec_shard *s, uint64_t h)
{
    return (size_t)((h >> ECCACHE_SHARD_BITS) & (s->ehn - 1));
}

static bool ce_is(const ce *x, const uint8_t oid[16], uint64_t off)
{
    return x->off == off && (eccache_mutant == ECC_MUT_KEYOFF || memcmp(x->oid, oid, 16) == 0);
}

static ce *ce_find(ec_shard *s, const uint8_t oid[16], uint64_t off, uint64_t h)
{
    for (ce *x = s->eh[ce_bucket(s, h)]; x; x = x->hnext)
        if (ce_is(x, oid, off))
            return x;
    return NULL;
}

/* A failed growth leaves the table as it is (longer chains, still right). */
static void ce_grow(ec_shard *s)
{
    if (s->nent < 2 * s->ehn)
        return;
    size_t n = s->ehn * 2;
    ce **nh = calloc(n, sizeof *nh);
    if (!nh)
        return;
    ce **old = s->eh;
    size_t on = s->ehn;
    s->eh = nh;
    s->ehn = n;
    for (size_t i = 0; i < on; i++)
        for (ce *x = old[i], *nx; x; x = nx) {
            nx = x->hnext;
            size_t b = ce_bucket(s, ce_hash64(x->oid, x->off));
            x->hnext = nh[b];
            nh[b] = x;
        }
    free(old);
}

static size_t og_hash(const ec_shard *s, const uint8_t oid[16])
{
    uint64_t h = ld64(oid) ^ ld64(oid + 8) * 0x9e3779b97f4a7c15ull;
    h ^= h >> 31;
    return (size_t)(h & (s->ghn - 1));
}

static og *og_find(ec_shard *s, const uint8_t oid[16])
{
    for (og *g = s->gh[og_hash(s, oid)]; g; g = g->hnext)
        if (memcmp(g->oid, oid, 16) == 0)
            return g;
    return NULL;
}

static void og_grow(ec_shard *s)
{
    if (s->ngrp < 2 * s->ghn)
        return;
    size_t n = s->ghn * 2;
    og **nh = calloc(n, sizeof *nh);
    if (!nh)
        return;
    og **old = s->gh;
    size_t on = s->ghn;
    s->gh = nh;
    s->ghn = n;
    for (size_t i = 0; i < on; i++)
        for (og *g = old[i], *nx; g; g = nx) {
            nx = g->hnext;
            size_t h = og_hash(s, g->oid);
            g->hnext = nh[h];
            nh[h] = g;
        }
    free(old);
}

/* Lists x (LOADING) in the shard's table and in its object's group; *spare is a group allocated outside the leaf,
 * taken when the object has none yet in this shard (s->mu held). */
static void ce_insert(ec_shard *s, ce *x, uint64_t h, og **spare)
{
    size_t b = ce_bucket(s, h);
    x->hnext = s->eh[b];
    s->eh[b] = x;
    atomic_store_explicit(&x->listed, true, memory_order_release);
    s->nent++;
    og *g = og_find(s, x->oid);
    if (!g) {
        g = *spare;
        *spare = NULL;
        memcpy(g->oid, x->oid, 16);
        size_t gh = og_hash(s, g->oid);
        g->hnext = s->gh[gh];
        s->gh[gh] = g;
        s->ngrp++;
        og_grow(s);
    }
    x->grp = g;
    x->gprev = NULL;
    x->gnext = g->first;
    if (g->first)
        g->first->gprev = x;
    g->first = x;
    ce_grow(s);
}

/* What an entry costs against the budget: its bytes, its slack, its struct, the allocator's headers and its share of
 * the tables (an entry of a slow series' granule is a few hundred bytes, so the overhead is not small beside it). */
static uint64_t ce_cost(const ce *x)
{
    return x->len + ECCACHE_ENTRY_OVERHEAD;
}

static void shard_bytes_add(struct eccache_tier *c, ec_shard *s, uint64_t delta)
{
    uint64_t sb = atomic_load_explicit(&s->bytes, memory_order_relaxed);
    atomic_store_explicit(&s->bytes, sb + delta, memory_order_relaxed);
    atomic_fetch_add_explicit(&c->total_bytes, delta, memory_order_acq_rel);
}

static void shard_bytes_sub(struct eccache_tier *c, ec_shard *s, uint64_t delta)
{
    uint64_t sb = atomic_load_explicit(&s->bytes, memory_order_relaxed);
    atomic_store_explicit(&s->bytes, sb - delta, memory_order_relaxed);
    atomic_fetch_sub_explicit(&c->total_bytes, delta, memory_order_acq_rel);
}

static void ce_unhash(struct eccache_tier *c, ec_shard *s, ce *x)
{
    if (!atomic_load_explicit(&x->listed, memory_order_relaxed))
        return;
    size_t b = ce_bucket(s, ce_hash64(x->oid, x->off));
    for (ce **pp = &s->eh[b]; *pp; pp = &(*pp)->hnext)
        if (*pp == x) {
            *pp = x->hnext;
            x->hnext = NULL;
            break;
        }
    atomic_store_explicit(&x->listed, false, memory_order_release);
    s->nent--;
    if (x->pf) { /* read ahead, never hit: wasted if it was cached, dropped by its reader if it was still loading */
        x->pf = false;
        atomic_fetch_sub_explicit(&c->pf_bytes, ce_cost(x), memory_order_acq_rel);
        if (x->state == CE_READY)
            s->pf_wasted++;
    }
    og *g = x->grp;
    if (x->gprev)
        x->gprev->gnext = x->gnext;
    else
        g->first = x->gnext;
    if (x->gnext)
        x->gnext->gprev = x->gprev;
    x->gprev = x->gnext = NULL;
    x->grp = NULL;
    if (!g->first) { /* the object's last entry in this shard: its group goes (a small free under the leaf, no I/O) */
        for (og **pp = &s->gh[og_hash(s, g->oid)]; *pp; pp = &(*pp)->hnext)
            if (*pp == g) {
                *pp = g->hnext;
                break;
            }
        s->ngrp--;
        free(g);
    }
}

static void lru_unlink(ce *x)
{
    x->prev->next = x->next;
    x->next->prev = x->prev;
    x->prev = x->next = NULL;
}

static void lru_front(ec_shard *s, ce *x)
{
    if (x->prev)
        lru_unlink(x);
    x->next = s->lru.next;
    x->prev = &s->lru;
    s->lru.next->prev = x;
    s->lru.next = x;
}

/* Takes x out of the shard's table, its group, the LRU and the bytes (s->mu held). Only a READY entry is in the LRU
 * and the bytes; a LOADING one taken out goes on to its reader (and its waiters look again), never into the cache. */
static void ce_remove(struct eccache_tier *c, ec_shard *s, ce *x)
{
    ce_unhash(c, s, x);
    if (x->prev) {
        lru_unlink(x);
        shard_bytes_sub(c, s, ce_cost(x));
    }
}

/* Evicts unpinned entries from shard s's cold end while global total_bytes > budget and s->bytes > limit (s->mu held).
 * `any`: true only for the prefetchevictpinned mutant during window eviction. */
static void ce_evict_shard(struct eccache_tier *c, ec_shard *s, uint64_t budget, uint64_t limit, ce **vict, bool any)
{
    for (ce *x = s->lru.prev; atomic_load_explicit(&s->bytes, memory_order_relaxed) > limit &&
                              atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget && x != &s->lru;) {
        ce *pv = x->prev;
        if (x->pins == 0 || any) {
            ce_remove(c, s, x);
            s->evictions++;
            x->hnext = *vict;
            *vict = x;
        }
        x = pv;
    }
}

/* Evicts unpinned entries from shard s's cold end down to `limit` regardless of total_bytes (budget shrink / drop). */
static void ce_evict_to_limit(struct eccache_tier *c, ec_shard *s, uint64_t limit, ce **vict)
{
    for (ce *x = s->lru.prev; atomic_load_explicit(&s->bytes, memory_order_relaxed) > limit && x != &s->lru;) {
        ce *pv = x->prev;
        if (x->pins == 0) {
            ce_remove(c, s, x);
            s->evictions++;
            x->hnext = *vict;
            *vict = x;
        }
        x = pv;
    }
}

/* Cross-shard clock sweep when total_bytes exceeds budget (no shard lock held). Pass 1 trims shards above their
 * per-shard cap; pass 2 absorbs slack from any non-empty shard when pinned entries hold another shard over cap or when
 * the total budget is small relative to ECCACHE_SHARDS. When `avoid_shard >= 0` (unpin under a small budget), that
 * shard is deferred until last so older entries on other shards evict before the just-used MRU entry. */
static void ce_evict_global(struct eccache_tier *c, ce **vict, bool window, int avoid_shard)
{
    uint64_t budget = atomic_load_explicit(&c->budget, memory_order_acquire);
    if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) <= budget)
        return;
    bool any = window && eccache_mutant == ECC_MUT_PFEVICTPINNED;
    uint32_t start = atomic_fetch_add_explicit(&c->evict_hand, 1, memory_order_relaxed);
    uint32_t visited = 0;
    for (uint32_t i = 0; i < ECCACHE_SHARDS; i++) {
        if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) <= budget)
            break;
        visited = i + 1;
        uint32_t si = (start + i) & (ECCACHE_SHARDS - 1);
        if ((int)si == avoid_shard)
            continue;
        ec_shard *s = &c->shards[si];
        uint64_t cap = atomic_load_explicit(&s->cap, memory_order_relaxed);
        if (atomic_load_explicit(&s->bytes, memory_order_relaxed) <= cap)
            continue;
        pthread_mutex_lock(&s->mu);
        ce_evict_shard(c, s, budget, atomic_load_explicit(&s->cap, memory_order_relaxed), vict, any);
        pthread_mutex_unlock(&s->mu);
    }
    if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget) {
        for (uint32_t i = 0; i < ECCACHE_SHARDS; i++) {
            if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) <= budget)
                break;
            uint32_t si = (start + i) & (ECCACHE_SHARDS - 1);
            if ((int)si == avoid_shard)
                continue;
            ec_shard *s = &c->shards[si];
            if (atomic_load_explicit(&s->bytes, memory_order_relaxed) == 0)
                continue;
            pthread_mutex_lock(&s->mu);
            ce_evict_shard(c, s, budget, 0, vict, any);
            pthread_mutex_unlock(&s->mu);
        }
    }
    if (avoid_shard >= 0 && atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget) {
        ec_shard *s = &c->shards[avoid_shard];
        if (atomic_load_explicit(&s->bytes, memory_order_relaxed) > 0) {
            pthread_mutex_lock(&s->mu);
            ce_evict_shard(c, s, budget, 0, vict, any);
            pthread_mutex_unlock(&s->mu);
        }
    }
    if (visited > 1)
        atomic_fetch_add_explicit(&c->evict_hand, visited - 1, memory_order_relaxed);
}

static void ce_free_list(ce *x)
{
    for (ce *nx; x; x = nx) {
        nx = x->hnext;
        if (x->slab)
            slab_unref(x->slab);
        else
            free(x->data);
        free(x);
    }
}

static void set_shard_caps(struct eccache_tier *c, uint64_t budget)
{
    uint64_t base = budget / ECCACHE_SHARDS;
    uint64_t rem = budget % ECCACHE_SHARDS;
    for (size_t i = 0; i < ECCACHE_SHARDS; i++)
        atomic_store_explicit(&c->shards[i].cap, base + (i < rem ? 1 : 0), memory_order_relaxed);
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

/* Takes o out of the table and the LRU, and its fds out of the count (obj_mu held); its reader, or the caller when none
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

/* Closes unpinned objects from the cold end until `need` more fds fit the budget (obj_mu held); victims to *vict. */
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

static void co_evict_all_unpinned(struct eccache_tier *c, co **vict)
{
    for (co *o = c->olru.prev; o != &c->olru;) {
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
            o->ops->obj_close(o->o);
            n++;
        }
        free(o->sums);
        free(o);
    }
    if (n) {
        pthread_mutex_lock(&c->obj_mu);
        c->obj_closes += n;
        pthread_mutex_unlock(&c->obj_mu);
    }
}

static void pf_mark_degraded_locked(struct eccache_tier *c, const uint8_t oid[16]);

/* The CHUNK_ERR_* codes are not the engine's: a store another owner holds or one that failed is an I/O error here. */
static int map_rc(int rc)
{
    if (rc == TSDB_ERR_NOTFOUND)
        return TSDB_ERR_CORRUPT; /* every store UP and no chunk of the object: it is lost (chunk_obj_open) */
    return rc <= -100 ? TSDB_ERR_IO : rc;
}

/* Gives the reader the object of e, open and pinned (obj_put lets go of it). */
static int obj_get(struct eccache_tier *c, const struct ecpart *e, const ce *x, co **out)
{
    const ec_record *r = &e->rec;
    chunk_geom gm;
    if (chunk_geom_make(&gm, r->k, r->m, r->cell_bytes, r->object_len) != 0 || r->nsums != chunk_sums_total(&gm))
        return TSDB_ERR_CORRUPT; /* r7-ecfile's open checks the record; never trusted further than this */
    unsigned need = chunk_npos(&gm);
    co *nw = NULL;
    co *vict = NULL;
    pthread_mutex_lock(&c->obj_mu);
    for (;;) {
        co *o = co_find(c, e->volume, r->oid);
        if (o && o->state == CO_OPENING) {
            c->obj_waiters++;
            pthread_cond_wait(&c->obj_cv, &c->obj_mu);
            c->obj_waiters--;
            continue;
        }
        if (o) {
            o->pins++;
            co_lru_front(c, o);
            pthread_mutex_unlock(&c->obj_mu);
            free(nw ? nw->sums : NULL);
            free(nw);
            *out = o;
            return 0;
        }
        if (nw)
            break;
        pthread_mutex_unlock(&c->obj_mu); /* allocations outside the leaf */
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
        pthread_mutex_lock(&c->obj_mu);
    }
    co_evict(c, need, &vict);
    nw->state = CO_OPENING;
    nw->pins = 1;
    nw->fds = need;
    c->fds += need;
    if (c->fds > c->fds_peak)
        c->fds_peak = c->fds;
    size_t h = co_hash(c, nw->volume, nw->oid);
    nw->hnext = c->oh[h];
    c->oh[h] = nw;
    nw->listed = true;
    c->nobj++;
    co_grow(c);
    pthread_mutex_unlock(&c->obj_mu);
    co_close_list(c, vict);

    /* the volume's ops of its latest attach: a retired engine's part reads the same volume's objects */
    int rc = TSDB_ERR_UNAVAILABLE;
    coldvol *cv = tier_cold_get(c->t, e->volume);
    if (cv) {
        rc = cv->ops->obj_open(cv->ctx, r->oid, &gm, nw->sums, &nw->o);
        nw->ops = cv->ops;
        tier_cold_put(cv);
    }
    if (eccache_test_hook)
        eccache_test_hook(c->t, ECC_HOOK_OPENED, r->oid);
    pthread_mutex_lock(&c->obj_mu);
    if (rc) {
        bool still_listed = nw->listed;
        co_remove(c, nw);
        if (still_listed && (!x || atomic_load_explicit(&x->listed, memory_order_acquire)) && rc != TSDB_ERR_NOMEM)
            pf_mark_degraded_locked(c, r->oid);
        if (c->obj_waiters)
            pthread_cond_broadcast(&c->obj_cv);
        pthread_mutex_unlock(&c->obj_mu);
        free(nw->sums);
        free(nw);
        return map_rc(rc);
    }
    nw->state = CO_OPEN;
    c->obj_opens++;
    if (nw->listed) /* else forgotten while it opened: its reader closes it */
        co_lru_front(c, nw);
    if (c->obj_waiters)
        pthread_cond_broadcast(&c->obj_cv);
    pthread_mutex_unlock(&c->obj_mu);
    *out = nw;
    return 0;
}

/* Lets go of o. `stale`: the read just made saw it degraded (a cell that failed or was rebuilt, a position away, an
 * error), so it leaves the table and the next reader opens it again: a chunk that heal or replace wrote since is a new
 * file (chunkheal.c), and a position that was away at the open may be back; an open object keeps its fds on the old
 * inodes and its positions' state for its life. `mark_degraded` records the object in pf_bad under the same lock
 * acquisition only if o (and x, when given) were still listed, so an in-flight read from before eccache_reopen or
 * eccache_forget cannot re-poison the cleared object. */
static void obj_put(struct eccache_tier *c, co *o, bool stale, bool mark_degraded, const ce *x, bool count_corrupt)
{
    co *vict = NULL;
    pthread_mutex_lock(&c->obj_mu);
    o->pins--;
    bool still_listed = o->listed;
    if (stale && eccache_mutant != ECC_MUT_KEEPSTALE)
        co_remove(c, o);
    if (mark_degraded && still_listed && (!x || atomic_load_explicit(&x->listed, memory_order_acquire)))
        pf_mark_degraded_locked(c, o->oid);
    if (count_corrupt)
        c->corrupt++;
    if (o->pins == 0 && !o->listed) {
        vict = o; /* out of the table (stale, forgotten, reopened): closed by its last reader */
        o->hnext = NULL;
    } else {
        co_evict(c, 0, &vict);
    }
    pthread_mutex_unlock(&c->obj_mu);
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

static long resolve_gran_index(const part *p, const granule *g, size_t gi)
{
    if (gi < p->ix.n && gix_get(&p->ix, (uint32_t)gi, GF_OFF) == g->off)
        return (long)gi;
    return gran_index(p, g->off);
}

static void corrupt_line(const part *p, const struct ecpart *e, const granule *g, size_t gi, const char *why)
{
    char hex[33];
    chunk_oid_hex(e->rec.oid, hex);
    fprintf(stderr, "tsdb: volume %" PRIu32 ", partition %" PRId64 ", part %s, granule %ld (offset %" PRIu64
                    ", %" PRIu64 " bytes): %s (object %s): TSDB_ERR_CORRUPT\n",
            e->volume, p->partition, p->name, resolve_gran_index(p, g, gi), g->off, (uint64_t)g->ts_len + g->val_len,
            why, hex);
}

static uint64_t pf_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Clears any tracked walk and degraded cooldown on `oid` (reopen, forget; obj_mu held). */
static void pf_reset_locked(struct eccache_tier *c, const uint8_t oid[16])
{
    for (int k = 0; k < PF_STREAMS; k++)
        if (c->pfs[k].stamp && memcmp(c->pfs[k].oid, oid, 16) == 0)
            memset(&c->pfs[k], 0, sizeof c->pfs[k]);
    for (int k = 0; k < PF_BAD_SLOTS; k++)
        if (c->pf_bad[k].stamp && memcmp(c->pf_bad[k].oid, oid, 16) == 0)
            memset(&c->pf_bad[k], 0, sizeof c->pf_bad[k]);
}

/* Marks `oid` degraded for a monotonic cooldown so further misses do not issue extra prefetch reads on a damaged
 * object/stripe, and drops any active walks on it so recovery probes cleanly from PF_INIT (obj_mu held). */
static void pf_mark_degraded_locked(struct eccache_tier *c, const uint8_t oid[16])
{
    for (int k = 0; k < PF_STREAMS; k++)
        if (c->pfs[k].stamp && memcmp(c->pfs[k].oid, oid, 16) == 0)
            memset(&c->pfs[k], 0, sizeof c->pfs[k]);
    pfbad *s = NULL, *old = &c->pf_bad[0];
    for (int k = 0; k < PF_BAD_SLOTS; k++) {
        pfbad *q = &c->pf_bad[k];
        if (q->stamp && memcmp(q->oid, oid, 16) == 0) {
            s = q;
            break;
        }
        if (q->stamp < old->stamp)
            old = q;
    }
    if (!s)
        s = old;
    memcpy(s->oid, oid, 16);
    s->stamp = ++c->pf_bad_stamp;
    s->until_ns = pf_now_ns() + c->pf_bad_ttl_ns;
}

/* True while `oid` is within its degraded cooldown. Does not refresh `stamp` on a hit (so healed entries expire by
 * both TTL and LRU); clears expired entries so prefetch recovers automatically (obj_mu held). */
static bool pf_is_degraded_locked(struct eccache_tier *c, const uint8_t oid[16])
{
    for (int k = 0; k < PF_BAD_SLOTS; k++) {
        pfbad *q = &c->pf_bad[k];
        if (q->stamp && memcmp(q->oid, oid, 16) == 0) {
            if (pf_now_ns() < q->until_ns)
                return true;
            memset(q, 0, sizeof *q);
            return false;
        }
    }
    return false;
}

/* Reads the granule into x->data and checks it (E11); the reader holds x (LOADING, pinned). When `skip_raw` is set,
 * fill_ahead already ran the RAW read and saw a checksum mismatch, so we go straight to whole-cell recovery. */
static int fill_inner(struct eccache_tier *c, const part *p, const struct ecpart *e, const granule *g, size_t gi,
                      ce *x, bool skip_raw)
{
    if (!x->data) {
        /* BLK_SLACK zeroed bytes past the granule: the decoders read up to that far beyond a block (in a mapped
         * part the footer covers it), and granule_decode runs on this buffer */
        x->data = malloc((size_t)x->len + BLK_SLACK);
        if (!x->data)
            return TSDB_ERR_NOMEM;
        memset(x->data + x->len, 0, BLK_SLACK);
    }
    co *o = NULL;
    int rc = obj_get(c, e, x, &o);
    if (rc) {
        if (rc == TSDB_ERR_CORRUPT)
            corrupt_line(p, e, g, gi, "the object cannot be opened");
        return rc;
    }
    chunk_report rep;
    memset(&rep, 0, sizeof rep);
    bool ok = false;
    if (!skip_raw) {
        rc = o->ops->obj_read(o->o, x->off, x->len, x->data, CHUNK_RR_RAW, &rep);
        atomic_fetch_add_explicit(&c->nreads, 1, memory_order_relaxed);
        granule_checks_add(1);
        ok = rc == 0 && (eccache_mutant == ECC_MUT_NOVERIFY || (uint32_t)xxh64(x->data, x->len, 0) == x->crc);
    }
    bool retried = false;
    /* Only a mismatch is read again. A RAW read that fails (UNAVAILABLE, CORRUPT) met erasures: positions absent,
     * away or unreadable, the same ones a whole-cell read meets, which can only add the cells that fail their sums;
     * reading again would double the I/O of every miss while a store is away and answer the same. */
    if ((rc == 0 || eccache_mutant == ECC_MUT_RETRYERR) && !ok && rc != TSDB_ERR_NOMEM && rc != TSDB_ERR_INVALID &&
        eccache_mutant != ECC_MUT_NORETRY) {
        /* rot that RAW cannot see: whole cells against the record's sums */
        pthread_mutex_lock(&c->obj_mu);
        c->retries++;
        pthread_mutex_unlock(&c->obj_mu);
        retried = true;
        rc = o->ops->obj_read(o->o, x->off, x->len, x->data, 0, &rep);
        atomic_fetch_add_explicit(&c->nreads, 1, memory_order_relaxed);
        ok = rc == 0 && (eccache_mutant == ECC_MUT_NOVERIFY || eccache_mutant == ECC_MUT_NORECHECK ||
                         (uint32_t)xxh64(x->data, x->len, 0) == x->crc);
    }
    bool degraded = rc != 0 || retried || rep.bad_cells || rep.decoded_cells || !ok;
    bool mismatch = !ok && rc == 0;
    if (mismatch)
        rc = TSDB_ERR_CORRUPT;
    obj_put(c, o, degraded, degraded || rc == TSDB_ERR_CORRUPT, x, rc == TSDB_ERR_CORRUPT);
    if (rc == TSDB_ERR_CORRUPT)
        corrupt_line(p, e, g, gi, mismatch ? "its bytes fail its checksum after a decode of whole cells"
                                           : "its object lost more chunks of a stripe than it has parity");
    return map_rc(rc);
}

static int fill(struct eccache_tier *c, const part *p, const struct ecpart *e, const granule *g, size_t gi, ce *x)
{
    return fill_inner(c, p, e, g, gi, x, false);
}

/* ------------------------------------------------------------ prefetch (R8 D19) */

typedef struct pf_cand {
    uint64_t off, len;
    uint32_t crc;
} pf_cand;

static bool pf_reserve(struct eccache_tier *c, uint64_t cost, uint64_t pfb)
{
    uint64_t cur = atomic_load_explicit(&c->pf_bytes, memory_order_relaxed);
    for (;;) {
        if (cur + cost > pfb)
            return false;
        if (atomic_compare_exchange_weak_explicit(&c->pf_bytes, &cur, cur + cost, memory_order_acq_rel,
                                                  memory_order_relaxed)) {
            uint64_t next = cur + cost;
            uint64_t pk = atomic_load_explicit(&c->pf_peak, memory_order_relaxed);
            while (next > pk && !atomic_compare_exchange_weak_explicit(&c->pf_peak, &pk, next, memory_order_relaxed,
                                                                       memory_order_relaxed))
                ;
            return true;
        }
    }
}

/* The window a miss of granule i (x, LOADING and pinned by this reader) reads ahead: the next granules of the same
 * series in the part, contiguous in the object, none cached or loading, listed LOADING and pinned here (pf set) with
 * their cost in pf_bytes. Returns how many (in win[]); 0 when the miss does not continue a walk. Budget and cache
 * presence are checked before allocating each ce, and no per-granule data buffer is allocated (fill_ahead backs the
 * whole window with one refcounted ec_slab). */
static size_t pf_plan(struct eccache_tier *c, const part *p, const struct ecpart *e, const granule *g, size_t gi,
                      ce *x, ce **win, size_t wcap)
{
    if (wcap == 0)
        return 0;
    long li = resolve_gran_index(p, g, gi);
    if (li < 0)
        return 0;
    uint32_t i = (uint32_t)li;
    bool cont = i > 0 && gx_series(p, i - 1) == gx_series(p, i);
    const uint8_t *oid = e->rec.oid;
    unsigned n = 0;
    pthread_mutex_lock(&c->obj_mu);
    unsigned cap = MIN(atomic_load_explicit(&c->pf_cap, memory_order_relaxed), (unsigned)wcap);
    if (cap && (pf_is_degraded_locked(c, oid) ||
                atomic_load_explicit(&c->forget_active, memory_order_seq_cst) != 0 ||
                !atomic_load_explicit(&x->listed, memory_order_acquire))) {
        pthread_mutex_unlock(&c->obj_mu);
        return 0;
    }
    pfstream *s = NULL, *old = &c->pfs[0];
    for (int k = 0; k < PF_STREAMS && cap; k++) {
        pfstream *q = &c->pfs[k];
        if (q->stamp && q->next == i && !memcmp(q->oid, oid, 16)) {
            s = q;
            break;
        }
        if (q->stamp < old->stamp)
            old = q;
    }
    if (s && cont)
        n = s->n && eccache_mutant != ECC_MUT_PFNODOUBLE ? MIN(2 * s->n, cap) : MIN(PF_INIT, cap);
    uint64_t callb = c->pf_call;
    if (cap) { /* this miss is where the walk stands now: the next one continues it at i + 1 */
        pfstream *dst = s ? s : old;
        memcpy(dst->oid, oid, 16);
        dst->next = i + 1;
        dst->n = n;
        dst->stamp = ++c->pf_stamp;
    }
    pthread_mutex_unlock(&c->obj_mu);
    if (n == 0)
        return 0;

    /* the candidates, from the index: same series, contiguous, inside the object's granules, under the call's bytes */
    pf_cand cands[ECCACHE_PF_CAP];
    uint32_t ser = gx_series(p, i);
    uint64_t end = g->off + (uint64_t)g->ts_len + g->val_len;
    size_t m = 0;
    for (uint32_t j = i + 1; j < p->ix.n && m < n; j++) {
        granule gj;
        gx_load(p, j, GW_LOC, &gj);
        uint64_t len = (uint64_t)gj.ts_len + gj.val_len;
        if (gx_series(p, j) != ser || gj.off != end || len == 0 || len > p->index_off - gj.off ||
            end + len - g->off > callb)
            break;
        cands[m++] = (pf_cand){.off = gj.off, .len = len, .crc = gj.crc};
        end += len;
    }
    if (m && eccache_test_hook)
        eccache_test_hook(c->t, ECC_HOOK_PFPLAN, oid);

    size_t k = 0;
    ce *y = NULL;
    og *spare = NULL; /* allocated lazily only if a destination shard has no group for oid yet */
    uint64_t fseq = atomic_load_explicit(&c->forget_seq, memory_order_seq_cst);
    if (atomic_load_explicit(&c->forget_active, memory_order_seq_cst) == 0 &&
        atomic_load_explicit(&x->listed, memory_order_acquire)) {
        for (; k < m; k++) {
            uint64_t cost = cands[k].len + ECCACHE_ENTRY_OVERHEAD;
            uint64_t pfb = atomic_load_explicit(&c->budget, memory_order_acquire) / 4;
            if (atomic_load_explicit(&c->pf_bytes, memory_order_relaxed) + cost > pfb)
                break;
            if (!y) {
                y = calloc(1, sizeof *y);
                if (!y)
                    break;
            }
            uint64_t h = ce_hash64(oid, cands[k].off);
            size_t si = ce_shard_idx(h);
            ec_shard *sh = &c->shards[si];
            bool inserted = false;
            for (;;) {
                pthread_mutex_lock(&sh->mu);
                if (atomic_load_explicit(&c->forget_active, memory_order_seq_cst) != 0 ||
                    atomic_load_explicit(&c->forget_seq, memory_order_seq_cst) != fseq ||
                    !atomic_load_explicit(&x->listed, memory_order_acquire) ||
                    (eccache_mutant != ECC_MUT_PFDUPKEY && ce_find(sh, oid, cands[k].off, h) != NULL)) {
                    pthread_mutex_unlock(&sh->mu);
                    break;
                }
                if (!og_find(sh, oid) && !spare) {
                    pthread_mutex_unlock(&sh->mu);
                    spare = calloc(1, sizeof *spare);
                    if (!spare)
                        break;
                    continue;
                }
                pfb = atomic_load_explicit(&c->budget, memory_order_acquire) / 4;
                if (!pf_reserve(c, cost, pfb)) {
                    pthread_mutex_unlock(&sh->mu);
                    break;
                }
                memcpy(y->oid, oid, 16);
                y->off = cands[k].off;
                y->len = cands[k].len;
                y->crc = cands[k].crc;
                y->shard = (uint8_t)si;
                y->state = CE_LOADING;
                y->pins = 1;
                y->pf = true;
                ce_insert(sh, y, h, &spare);
                pthread_mutex_unlock(&sh->mu);
                win[k] = y;
                y = NULL;
                inserted = true;
                break;
            }
            if (!inserted)
                break;
        }
    }
    free(y);
    free(spare);
    if (k > 0) {
        pthread_mutex_lock(&c->obj_mu);
        if (atomic_load_explicit(&c->forget_active, memory_order_seq_cst) == 0 &&
            atomic_load_explicit(&x->listed, memory_order_acquire)) {
            for (int q = 0; q < PF_STREAMS; q++) /* the walk continues after the window */
                if (c->pfs[q].stamp && c->pfs[q].next == i + 1 && !memcmp(c->pfs[q].oid, oid, 16)) {
                    c->pfs[q].next = i + 1 + (uint32_t)k;
                    break;
                }
        }
        pthread_mutex_unlock(&c->obj_mu);
    }
    return k;
}

/* Ends the window: each entry checked (ok[q]) turns READY and cached, the rest leave (dropped); pins let go of.
 * `bytes`: the range read's length, 0 when none was made. The window goes to the LRU's front from its far end, so
 * the granule a walk needs next is the warmest of it and a window that makes room for itself loses its far end first. */
static void pf_finish(struct eccache_tier *c, ce **win, const bool *ok, size_t k, uint64_t bytes)
{
    if (bytes) {
        pthread_mutex_lock(&c->obj_mu);
        c->pf_reads++;
        if (bytes > c->pf_call_peak)
            c->pf_call_peak = bytes;
        pthread_mutex_unlock(&c->obj_mu);
    }
    ce *vict = NULL;
    uint64_t budget = atomic_load_explicit(&c->budget, memory_order_acquire);
    for (size_t q = k; q-- > 0;) {
        ce *y = win[q];
        ec_shard *sh = &c->shards[y->shard];
        pthread_mutex_lock(&sh->mu);
        y->pins--;
        if (!atomic_load_explicit(&y->listed, memory_order_relaxed) && eccache_mutant != ECC_MUT_PFFORGETLIVE) {
            /* forgotten while it was read (its pf cost went with it) */
            y->hnext = vict;
            vict = y;
        } else if (!ok[q] || ce_cost(y) > budget) { /* (the budget may have shrunk since the plan) */
            sh->pf_dropped++;
            ce_unhash(c, sh, y); /* LOADING: not counted as wasted */
            y->hnext = vict;
            vict = y;
        } else {
            y->state = CE_READY;
            shard_bytes_add(c, sh, ce_cost(y));
            lru_front(sh, y);
            sh->prefetched++;
        }
        if (sh->waiters)
            pthread_cond_broadcast(&sh->cv);
        pthread_mutex_unlock(&sh->mu);
    }
    ce_evict_global(c, &vict, true, -1);
    ce_free_list(vict);
}

/* fill() with a window: one RAW range read of the demanded granule and the k after it into a shared refcounted slab.
 * The demanded granule's bytes, when they check, slice directly into slab->buf; when they do not, or when the range
 * read fails, it is filled alone as before (fill: RAW, then whole cells). Each granule ahead is checked in place and
 * slices directly into slab->buf + at with zero memcpy; a failed read checks none. */
static int fill_ahead(struct eccache_tier *c, const part *p, const struct ecpart *e, const granule *g, size_t gi,
                      ce *x, ce **win, size_t k)
{
    bool ok[ECCACHE_PF_CAP];
    memset(ok, 0, sizeof ok);
    uint64_t total = x->len;
    for (size_t q = 0; q < k; q++)
        total += win[q]->len;
    ec_slab *sl = NULL;
    if (total <= SIZE_MAX - sizeof(ec_slab) - BLK_SLACK) {
        size_t slab_bytes = sizeof(ec_slab) + (size_t)total + BLK_SLACK;
        sl = malloc(slab_bytes);
        if (sl) {
            atomic_init(&sl->refs, 1);
            sl->bytes = slab_bytes;
            memset(sl->buf + total, 0, BLK_SLACK);
        }
    }
    co *o = NULL;
    int rc = sl ? obj_get(c, e, x, &o) : TSDB_ERR_NOMEM;
    bool demand = false, pf_injected = false;
    if (rc == 0) {
        chunk_report rep;
        memset(&rep, 0, sizeof rep);
        if (atomic_load_explicit(&eccache_test_pf_fail, memory_order_relaxed) > 0 &&
            atomic_fetch_sub_explicit(&eccache_test_pf_fail, 1, memory_order_relaxed) > 0) {
            rc = TSDB_ERR_UNAVAILABLE; /* a test: the window's read fails as if a store went away under it */
            pf_injected = true;
        } else {
            rc = o->ops->obj_read(o->o, x->off, total, sl->buf, CHUNK_RR_RAW, &rep);
        }
        atomic_fetch_add_explicit(&c->nreads, 1, memory_order_relaxed);
        bool bad_crc = false;
        if (rc == 0) {
            granule_checks_add(1 + k);
            uint32_t add_refs = 0;
            if (eccache_mutant == ECC_MUT_NOVERIFY || (uint32_t)xxh64(sl->buf, x->len, 0) == x->crc) {
                x->data = sl->buf;
                x->slab = sl;
                add_refs++;
                demand = true;
            } else {
                bad_crc = true;
            }
            uint64_t at = x->len;
            for (size_t q = 0; q < k; q++) {
                ce *y = win[q];
                uint8_t *slice = sl->buf + at;
                at += y->len;
                ok[q] = eccache_mutant == ECC_MUT_PFNOCHECK || (uint32_t)xxh64(slice, y->len, 0) == y->crc;
                if (ok[q]) {
                    y->data = slice;
                    y->slab = sl;
                    add_refs++;
                } else {
                    bad_crc = true;
                }
            }
            if (add_refs)
                atomic_fetch_add_explicit(&sl->refs, add_refs, memory_order_relaxed);
        }
        bool stale = rc != 0 || rep.bad_cells || rep.decoded_cells || bad_crc;
        bool degraded = (!pf_injected && rc != 0) || rep.bad_cells || rep.decoded_cells || bad_crc;
        obj_put(c, o, stale, degraded, x, false);
    }
    if (eccache_test_hook)
        eccache_test_hook(c->t, ECC_HOOK_PFREAD, e->rec.oid);
    pf_finish(c, win, ok, k, o != NULL ? total : 0);
    slab_unref(sl);
    if (!demand && eccache_mutant == ECC_MUT_PFNOFALLBACK && rc)
        return map_rc(rc);
    return demand ? 0 : fill_inner(c, p, e, g, gi, x, rc == 0 && o != NULL);
}

static struct ecpart *part_ec(const part *p)
{
    return atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
}

int ec_granule_read_at(codec_ctx *cx, const part *p, const granule *g, size_t gi, int type, int64_t *ts, void *v,
                       size_t cap, bool verify)
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
        corrupt_line(p, e, g, gi, "the index places it outside the object's granules");
        return TSDB_ERR_CORRUPT;
    }
    uint64_t h = ce_hash64(e->rec.oid, g->off);
    size_t si = ce_shard_idx(h);
    ec_shard *sh = &c->shards[si];
    ce *nw = NULL, *x, *vict = NULL;
    og *spare = NULL;
    unsigned pfcap = 0;
    bool loader = false;
    bool try_detach = false;
    pthread_mutex_lock(&sh->mu);
    for (;;) {
        x = ce_find(sh, e->rec.oid, g->off, h);
        if (x && x->state == CE_LOADING) {
            sh->waits++;
            sh->waiters++;
            pthread_cond_wait(&sh->cv, &sh->mu);
            sh->waiters--;
            continue;
        }
        if (x) {
            x->pins++;
            sh->hits++;
            if (x->pf) { /* read ahead, and now used */
                x->pf = false;
                atomic_fetch_sub_explicit(&c->pf_bytes, ce_cost(x), memory_order_acq_rel);
                sh->pf_used++;
            }
            if (x->slab && atomic_load_explicit(&x->slab->refs, memory_order_relaxed) == 1 &&
                x->slab->bytes > 4 * (x->len + BLK_SLACK))
                try_detach = true;
            lru_front(sh, x);
            break;
        }
        bool need_og = og_find(sh, e->rec.oid) == NULL;
        if (nw && (!need_og || spare)) {
            x = nw;
            nw = NULL;
            x->state = CE_LOADING;
            x->pins = 1;
            ce_insert(sh, x, h, &spare);
            sh->misses++;
            pfcap = atomic_load_explicit(&c->pf_cap, memory_order_relaxed);
            loader = true;
            break;
        }
        pthread_mutex_unlock(&sh->mu); /* allocations outside the leaf */
        if (!nw) {
            nw = calloc(1, sizeof *nw);
            if (!nw) {
                free(spare);
                return TSDB_ERR_NOMEM;
            }
            memcpy(nw->oid, e->rec.oid, 16);
            nw->off = g->off;
            nw->len = len;
            nw->crc = g->crc;
            nw->shard = (uint8_t)si;
        }
        if (need_og && !spare) {
            spare = calloc(1, sizeof *spare);
            if (!spare) {
                free(nw);
                return TSDB_ERR_NOMEM;
            }
        }
        pthread_mutex_lock(&sh->mu);
    }
    pthread_mutex_unlock(&sh->mu);
    free(spare); /* the object had a group in this shard, or another reader filled the entry meanwhile */
    free(nw);
    int rc = 0;
    if (loader) {
        ce *win[ECCACHE_PF_CAP];
        size_t k = pf_plan(c, p, e, g, gi, x, win, MIN(pfcap, ECCACHE_PF_CAP));
        rc = k ? fill_ahead(c, p, e, g, gi, x, win, k) : fill(c, p, e, g, gi, x);
        if (eccache_test_hook)
            eccache_test_hook(c->t, ECC_HOOK_FILLED, e->rec.oid);
        uint64_t budget = atomic_load_explicit(&c->budget, memory_order_acquire);
        pthread_mutex_lock(&sh->mu);
        if (rc) {
            ce_unhash(c, sh, x);
        } else if (!atomic_load_explicit(&x->listed, memory_order_relaxed)) {
            x->state = CE_READY; /* forgotten while it was read: its reader decodes it and frees it */
        } else if (ce_cost(x) > budget) {
            /* larger than the whole budget: used by its reader and dropped, without emptying the cache for it */
            ce_unhash(c, sh, x);
            x->state = CE_READY;
            sh->uncached++;
        } else {
            x->state = CE_READY;
            shard_bytes_add(c, sh, ce_cost(x));
            lru_front(sh, x);
            ce_evict_shard(c, sh, budget, atomic_load_explicit(&sh->cap, memory_order_relaxed), &vict, false);
        }
        if (sh->waiters)
            pthread_cond_broadcast(&sh->cv);
        pthread_mutex_unlock(&sh->mu);
        if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget)
            ce_evict_global(c, &vict, false, -1);
        ce_free_list(vict);
        vict = NULL;
    } else if (x->len != len || x->crc != g->crc) {
        /* the index says another granule is at this offset than the one cached: it changed in the mapping since */
        corrupt_line(p, e, g, gi, "the index no longer matches the granule read before at this offset");
        rc = TSDB_ERR_CORRUPT;
    }
    if (rc == 0)
        rc = granule_decode(cx, p, g, x->data, type, ts, v, cap, false); /* from the pinned entry */
    uint8_t *detach_buf = NULL;
    if (rc == 0 && try_detach) {
        /* Sole surviving slice of a large shared slab: copy to a standalone buffer outside the lock and swap on unpin
         * when uncontended (x->pins == 1) so 1 hot granule cannot pin a multi-MiB slab indefinitely. */
        detach_buf = malloc((size_t)x->len + BLK_SLACK);
        if (detach_buf) {
            memcpy(detach_buf, x->data, (size_t)x->len);
            memset(detach_buf + x->len, 0, BLK_SLACK);
        }
    }
    uint64_t budget = atomic_load_explicit(&c->budget, memory_order_relaxed);
    ec_slab *old_slab = NULL;
    bool over = false;
    pthread_mutex_lock(&sh->mu);
    if (detach_buf && x->pins == 1 && atomic_load_explicit(&x->listed, memory_order_relaxed) && x->slab) {
        old_slab = x->slab;
        x->data = detach_buf;
        x->slab = NULL;
        detach_buf = NULL;
    }
    x->pins--;
    if (x->pins == 0 && !atomic_load_explicit(&x->listed, memory_order_relaxed)) {
        x->hnext = vict;
        vict = x;
    } else if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget) {
        uint64_t limit = MAX(atomic_load_explicit(&sh->cap, memory_order_relaxed), ce_cost(x));
        ce_evict_shard(c, sh, budget, limit, &vict, false);
        over = atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget;
    }
    pthread_mutex_unlock(&sh->mu);
    if (old_slab)
        slab_unref(old_slab);
    free(detach_buf);
    if (over)
        ce_evict_global(c, &vict, false, (int)si);
    if (vict)
        ce_free_list(vict);
    return rc;
}

int ec_granule_read(codec_ctx *cx, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                    bool verify)
{
    return ec_granule_read_at(cx, p, g, SIZE_MAX, type, ts, v, cap, verify);
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
        uint64_t offs[256], hs[256];
        uint8_t shs[256];
        uint64_t mask = 0;
        uint32_t k = 0;
        for (; i < n && k < 256; i++) {
            uint64_t o = gix_get(&p->ix, i, GF_OFF);
            if (o >= off + len)
                break;
            offs[k] = o;
            hs[k] = ce_hash64(e->rec.oid, o);
            shs[k] = (uint8_t)ce_shard_idx(hs[k]);
            mask |= 1ull << shs[k];
            k++;
        }
        if (!k)
            break;
        while (mask) {
            unsigned si = (unsigned)__builtin_ctzll(mask);
            mask &= mask - 1;
            ec_shard *sh = &c->shards[si];
            pthread_mutex_lock(&sh->mu); /* one shard at a time, in batches */
            for (uint32_t j = 0; j < k; j++) {
                if (shs[j] != si)
                    continue;
                ce *x = ce_find(sh, e->rec.oid, offs[j], hs[j]);
                if (x && x->state == CE_READY)
                    got += MIN(x->len, off + len - offs[j]);
            }
            pthread_mutex_unlock(&sh->mu);
        }
        if (k < 256)
            break;
    }
    return got;
}

/* ------------------------------------------------------------ the tier's */

int eccache_tier_open(struct tier *t)
{
    EC_CALLED(EC_CALL_TIER_OPEN);
    void *mem = NULL;
    if (posix_memalign(&mem, 64, sizeof(struct eccache_tier)) != 0 || !mem)
        return TSDB_ERR_NOMEM;
    struct eccache_tier *c = mem;
    memset(c, 0, sizeof *c);
    c->t = t;

    void *smem = NULL;
    if (posix_memalign(&smem, 64, ECCACHE_SHARDS * sizeof(ec_shard)) != 0 || !smem) {
        free(c);
        return TSDB_ERR_NOMEM;
    }
    c->shards = smem;
    memset(c->shards, 0, ECCACHE_SHARDS * sizeof(ec_shard));

    c->ohn = 64;
    c->oh = calloc(c->ohn, sizeof *c->oh);
    if (!c->oh) {
        free(c->shards);
        free(c);
        return TSDB_ERR_NOMEM;
    }
    size_t init_shards = 0;
    for (; init_shards < ECCACHE_SHARDS; init_shards++) {
        ec_shard *s = &c->shards[init_shards];
        s->ehn = 16;
        s->ghn = 8;
        s->eh = calloc(s->ehn, sizeof *s->eh);
        s->gh = calloc(s->ghn, sizeof *s->gh);
        if (!s->eh || !s->gh) {
            free(s->eh);
            free(s->gh);
            break;
        }
        pthread_mutex_init(&s->mu, NULL);
        pthread_cond_init(&s->cv, NULL);
        s->lru.next = s->lru.prev = &s->lru;
    }
    if (init_shards < ECCACHE_SHARDS) {
        for (size_t i = 0; i < init_shards; i++) {
            ec_shard *s = &c->shards[i];
            free(s->eh);
            free(s->gh);
            pthread_mutex_destroy(&s->mu);
            pthread_cond_destroy(&s->cv);
        }
        free(c->oh);
        free(c->shards);
        free(c);
        return TSDB_ERR_NOMEM;
    }

    pthread_mutex_init(&c->obj_mu, NULL);
    pthread_cond_init(&c->obj_cv, NULL);
    c->olru.next = c->olru.prev = &c->olru;

    uint64_t budget;
    if (t->cfg.cache_mb) {
        budget = t->cfg.cache_mb << 20;
    } else {
        long pages = sysconf(_SC_PHYS_PAGES), psz = sysconf(_SC_PAGESIZE);
        budget = pages > 0 && psz > 0 ? (uint64_t)pages * (uint64_t)psz / 10 : 256ull << 20;
    }
    atomic_store_explicit(&c->budget, budget, memory_order_relaxed);
    set_shard_caps(c, budget);

    struct rlimit rl;
    uint64_t nofile = getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY ? rl.rlim_cur : 4096;
    c->fd_budget = MIN(MAX(nofile / 4, 64), 4096); /* a quarter of the process's: tsdbd's connections need the rest */
    const char *pf_env = getenv("TSDB_EC_PREFETCH");
    bool pf_off = pf_env && (strcmp(pf_env, "0") == 0 || strcasecmp(pf_env, "off") == 0 ||
                             strcasecmp(pf_env, "false") == 0 || strcasecmp(pf_env, "no") == 0);
    atomic_store_explicit(&c->pf_cap, pf_off ? 0 : ECCACHE_PF_CAP, memory_order_relaxed);
    c->pf_call = PF_CALL_BYTES;
    c->pf_bad_ttl_ns = PF_BAD_TTL_NS;
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
    for (size_t si = 0; si < ECCACHE_SHARDS; si++) {
        ec_shard *s = &c->shards[si];
        for (size_t i = 0; i < s->ehn; i++)
            ce_free_list(s->eh[i]);
        for (size_t i = 0; i < s->ghn; i++)
            for (og *g = s->gh[i], *nx; g; g = nx) {
                nx = g->hnext;
                free(g);
            }
        free(s->eh);
        free(s->gh);
        pthread_mutex_destroy(&s->mu);
        pthread_cond_destroy(&s->cv);
    }
    free(c->shards);
    for (size_t i = 0; i < c->ohn; i++)
        for (co *o = c->oh[i], *nx; o; o = nx) {
            nx = o->hnext;
            if (o->o)
                o->ops->obj_close(o->o);
            free(o->sums);
            free(o);
        }
    free(c->oh);
    pthread_mutex_destroy(&c->obj_mu);
    pthread_cond_destroy(&c->obj_cv);
    free(c);
    t->cache = NULL;
}

void eccache_set_backend(struct tier *t, struct chunk_backend *be)
{
    atomic_store_explicit(&t->cache->be, be, memory_order_release);
}

struct chunk_backend *eccache_backend(struct tier *t, int *rc)
{
    chunk_backend *be = atomic_load_explicit(&t->cache->be, memory_order_acquire);
    if (!be)
        *rc = TSDB_ERR_UNAVAILABLE;
    return be;
}

void eccache_vuuid(struct tier *t, uint32_t volume, uint8_t out[16])
{
    memset(out, 0, 16);
    coldvol *cv = tier_cold_get(t, volume);
    if (cv)
        memcpy(out, cv->vuuid, 16);
    tier_cold_put(cv);
}

/* Takes the object out of the table (opening or open; closed now, or by its last reader) and then, when `entries` is
 * set, sweeps all shards to remove its granule entries (each freed now, or by its reader: a fill in flight goes on to
 * its reader and is never cached). Removing `co` under obj_mu BEFORE sweeping shards linearizes concurrent readers: any
 * reader that obtained the pre-forget `co` already inserted its `ce` into its shard before calling obj_get, so the
 * subsequent shard sweep is guaranteed to unhash it. */
static void drop_object(struct eccache_tier *c, uint32_t volume, const uint8_t oid[16], bool entries)
{
    ce *vict = NULL;
    co *ov = NULL;
    bool drop_ce = entries && eccache_mutant != ECC_MUT_FORGETNOOP;
    if (drop_ce) {
        atomic_fetch_add_explicit(&c->forget_active, 1, memory_order_seq_cst);
        atomic_fetch_add_explicit(&c->forget_seq, 1, memory_order_seq_cst);
    }
    pthread_mutex_lock(&c->obj_mu);
    co *o = eccache_mutant != ECC_MUT_FORGETNOOP ? co_find(c, volume, oid) : NULL;
    if (o) {
        co_remove(c, o);
        if (o->pins == 0) {
            o->hnext = NULL;
            ov = o;
        }
        if (c->obj_waiters)
            pthread_cond_broadcast(&c->obj_cv);
    }
    if (eccache_mutant != ECC_MUT_FORGETNOOP)
        pf_reset_locked(c, oid);
    pthread_mutex_unlock(&c->obj_mu);
    if (drop_ce) {
        for (size_t si = 0; si < ECCACHE_SHARDS; si++) {
            ec_shard *s = &c->shards[si];
            pthread_mutex_lock(&s->mu);
            og *g = og_find(s, oid);
            bool any = false;
            while (g) {
                ce *x = g->first;
                bool last = x->gnext == NULL; /* the group is freed with its last entry */
                ce_remove(c, s, x);
                any = true;
                if (x->pins == 0) {
                    x->hnext = vict;
                    vict = x;
                }
                if (last)
                    break;
            }
            if (any && s->waiters)
                pthread_cond_broadcast(&s->cv);
            pthread_mutex_unlock(&s->mu);
        }
        if (eccache_test_hook)
            eccache_test_hook(c->t, ECC_HOOK_FORGET_SWEPT, oid);
        atomic_fetch_sub_explicit(&c->forget_active, 1, memory_order_seq_cst);
    }
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
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&c->obj_mu);
    out->retries = c->retries;
    out->corrupt = c->corrupt;
    out->obj_opens = c->obj_opens;
    out->obj_closes = c->obj_closes;
    out->objs_open = c->nobj;
    out->fds_open = c->fds;
    out->fds_peak = c->fds_peak;
    out->fd_budget = c->fd_budget;
    out->pf_reads = c->pf_reads;
    out->pf_call_peak = c->pf_call_peak;
    pthread_mutex_unlock(&c->obj_mu);
    for (size_t si = 0; si < ECCACHE_SHARDS; si++) {
        ec_shard *s = &c->shards[si];
        pthread_mutex_lock(&s->mu);
        out->hits += s->hits;
        out->misses += s->misses;
        out->waits += s->waits;
        out->bytes += atomic_load_explicit(&s->bytes, memory_order_relaxed);
        out->entries += s->nent;
        out->evictions += s->evictions;
        out->uncached += s->uncached;
        out->prefetched += s->prefetched;
        out->pf_used += s->pf_used;
        out->pf_wasted += s->pf_wasted;
        out->pf_dropped += s->pf_dropped;
        pthread_mutex_unlock(&s->mu);
    }
    uint64_t b = atomic_load_explicit(&c->budget, memory_order_acquire);
    out->budget = b;
    out->pf_budget = b / 4;
    out->pf_bytes = atomic_load_explicit(&c->pf_bytes, memory_order_acquire);
    out->pf_peak = atomic_load_explicit(&c->pf_peak, memory_order_acquire);
    out->reads = atomic_load_explicit(&c->nreads, memory_order_relaxed);
}

static void pf_set_cap(struct tier *t, unsigned cap, bool reset_peak)
{
    struct eccache_tier *c = t ? t->cache : NULL;
    if (!c)
        return;
    pthread_mutex_lock(&c->obj_mu);
    atomic_store_explicit(&c->pf_cap, cap, memory_order_relaxed);
    memset(c->pfs, 0, sizeof c->pfs);
    if (reset_peak)
        atomic_store_explicit(&c->pf_peak, atomic_load_explicit(&c->pf_bytes, memory_order_acquire),
                              memory_order_relaxed);
    pthread_mutex_unlock(&c->obj_mu);
}

void eccache_set_prefetch(struct tier *t, bool on)
{
    pf_set_cap(t, on ? ECCACHE_PF_CAP : 0, false);
}

bool eccache_prefetch_enabled(const struct tier *t)
{
    struct eccache_tier *c = t ? t->cache : NULL;
    if (!c)
        return false;
    return atomic_load_explicit(&c->pf_cap, memory_order_relaxed) > 0;
}

void eccache_test_prefetch(struct tier *t, unsigned cap)
{
    pf_set_cap(t, cap, true);
}

void eccache_test_prefetch_call(struct tier *t, uint64_t bytes)
{
    struct eccache_tier *c = t->cache;
    pthread_mutex_lock(&c->obj_mu);
    c->pf_call = bytes ? bytes : PF_CALL_BYTES;
    c->pf_call_peak = 0;
    pthread_mutex_unlock(&c->obj_mu);
}

void eccache_test_pf_bad_ttl_ms(struct tier *t, uint64_t ms)
{
    struct eccache_tier *c = t ? t->cache : NULL;
    if (!c)
        return;
    pthread_mutex_lock(&c->obj_mu);
    c->pf_bad_ttl_ns = ms ? ms * 1000000ull : PF_BAD_TTL_NS;
    pthread_mutex_unlock(&c->obj_mu);
}

void *eccache_test_pin(struct tier *t, const uint8_t oid[16], uint64_t off)
{
    struct eccache_tier *c = t->cache;
    uint64_t h = ce_hash64(oid, off);
    ec_shard *sh = &c->shards[ce_shard_idx(h)];
    pthread_mutex_lock(&sh->mu);
    ce *x = ce_find(sh, oid, off, h);
    if (x && x->state == CE_READY)
        x->pins++;
    else
        x = NULL;
    pthread_mutex_unlock(&sh->mu);
    return x;
}

void eccache_test_unpin(struct tier *t, void *entry)
{
    struct eccache_tier *c = t->cache;
    ce *x = entry, *vict = NULL;
    int shard = (int)x->shard;
    ec_shard *sh = &c->shards[shard];
    uint64_t budget = atomic_load_explicit(&c->budget, memory_order_acquire);
    bool over = false;
    pthread_mutex_lock(&sh->mu);
    x->pins--;
    if (x->pins == 0 && !atomic_load_explicit(&x->listed, memory_order_relaxed)) {
        x->hnext = NULL;
        vict = x;
    } else if (atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget) {
        uint64_t limit = MAX(atomic_load_explicit(&sh->cap, memory_order_relaxed), ce_cost(x));
        ce_evict_shard(c, sh, budget, limit, &vict, false);
        over = atomic_load_explicit(&c->total_bytes, memory_order_relaxed) > budget;
    }
    pthread_mutex_unlock(&sh->mu);
    if (over)
        ce_evict_global(c, &vict, false, shard);
    ce_free_list(vict);
}

void eccache_test_budget(struct tier *t, uint64_t bytes)
{
    struct eccache_tier *c = t->cache;
    ce *vict = NULL;
    atomic_store_explicit(&c->budget, bytes, memory_order_release);
    set_shard_caps(c, bytes);
    for (size_t si = 0; si < ECCACHE_SHARDS; si++) {
        ec_shard *s = &c->shards[si];
        pthread_mutex_lock(&s->mu);
        ce_evict_to_limit(c, s, atomic_load_explicit(&s->cap, memory_order_relaxed), &vict);
        pthread_mutex_unlock(&s->mu);
    }
    ce_evict_global(c, &vict, false, -1);
    ce_free_list(vict);
}

void eccache_test_fd_budget(struct tier *t, uint64_t fds)
{
    struct eccache_tier *c = t->cache;
    co *vict = NULL;
    pthread_mutex_lock(&c->obj_mu);
    c->fd_budget = fds;
    c->fds_peak = c->fds;
    co_evict(c, 0, &vict);
    pthread_mutex_unlock(&c->obj_mu);
    co_close_list(c, vict);
}

void eccache_test_drop_entries(struct tier *t)
{
    struct eccache_tier *c = t->cache;
    ce *vict = NULL;
    for (size_t si = 0; si < ECCACHE_SHARDS; si++) {
        ec_shard *s = &c->shards[si];
        pthread_mutex_lock(&s->mu);
        ce_evict_to_limit(c, s, 0, &vict);
        pthread_mutex_unlock(&s->mu);
    }
    ce_free_list(vict);
}

void eccache_test_drop(struct tier *t)
{
    struct eccache_tier *c = t->cache;
    ce *vict = NULL;
    co *ov = NULL;
    for (size_t si = 0; si < ECCACHE_SHARDS; si++) {
        ec_shard *s = &c->shards[si];
        pthread_mutex_lock(&s->mu);
        ce_evict_to_limit(c, s, 0, &vict);
        pthread_mutex_unlock(&s->mu);
    }
    pthread_mutex_lock(&c->obj_mu);
    co_evict_all_unpinned(c, &ov);
    memset(c->pfs, 0, sizeof c->pfs);
    memset(c->pf_bad, 0, sizeof c->pf_bad);
    pthread_mutex_unlock(&c->obj_mu);
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

struct memsrc {
    const uint8_t *b;
};

static int memsrc_fill(void *ctx, uint64_t s, uint64_t off, uint8_t *buf, size_t len)
{
    (void)s;
    memcpy(buf, ((struct memsrc *)ctx)->b + off, len);
    return 0;
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
    coldvol *cv = tier_cold(db);
    if (!cv)
        return TSDB_ERR_UNAVAILABLE;
    ec_record r;
    memset(&r, 0, sizeof r);
    chunk_geom g;
    if ((rc = chunk_geom_derive(&g, (unsigned)cv->set.k, (unsigned)cv->set.m, p->map_len)) != 0)
        return rc;
    uint64_t epoch = coldvol_epoch(cv);
    if (epoch == 0)
        return TSDB_ERR_UNAVAILABLE;
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    if ((rc = chunk_random(r.oid, sizeof r.oid)) != 0)
        return rc;
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
    r.writer_epoch = epoch;
    r.settle_ms = eng_now_ms(db);
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    if (!(r.sums = malloc(r.nsums * sizeof *r.sums)))
        return TSDB_ERR_NOMEM;
    /* through the volume's ops (the engine's own uuid, META vuuid=, as a settle writes it): one pass from memory */
    struct memsrc ms = {src};
    chunk_put_src ps = {.ctx = &ms, .fill = memsrc_fill};
    rc = cv->ops->see_epoch(cv->ctx, epoch);
    if (rc == 0)
        rc = cv->ops->put_stream(cv->ctx, r.oid, &g, epoch, &ps, r.sums);
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
