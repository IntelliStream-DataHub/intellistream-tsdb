/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (src/ecgc.h; R7, design-cluster-data.md E14, 2.4, 15.7; the plan's D6).
 *
 * An .ec part's rows live in its object's chunks, which no file system removes for us: whoever takes the part out of
 * service must delete them, and must not do it while anything may still read them. Three rules:
 *
 * 1. A name stays until the chunks are gone. A retired .ec part keeps its name retired_<name>.ec (or retired_<name>
 *    for a part with an attached object) until every chunk of its object is deleted; only then is the name unlinked
 *    (with no directory fsync: a name that comes back is collected again). So a crash at any step leaves either the
 *    name (and the next open, which reads the object's id from it, deletes again: NOTFOUND counts as gone) or no
 *    chunk at all. part_unref leaves such a name alone (ecgc_owns_name).
 *
 * 2. Nothing is deleted while a part of the object is referenced. db_retire_locked takes a hold for the part
 *    (ecgc_retire_locked) in the tier, keyed by (volume, oid), which the part's last part_unref lets go of (no I/O
 *    there). The graveyard entry of the name (GY_EC; with obsolete_part_lifetime_s <= 0 due at once) is purged only
 *    once its deadline passed and no hold names its object: the hold is the tier's and not the engine's, so a reopened
 *    engine's purge also waits for a query that still holds the retired engine's part.
 *
 * 3. The chunk I/O runs on the tier's worker, never under an engine lock, and the worker never touches an engine:
 *    the purge (graveyard_purge, under db->lock) queues the object with a ticket, the worker deletes every position
 *    (the volume's cold_ops del, which skips a DOWN store, so its writable is asked before and after: a store not UP
 *    keeps the entry for a later round) and marks the ticket; the next purge of that engine sees the ticket and
 *    unlinks the name. An engine that closes meanwhile drops its tickets; its names stay for the next open.
 *
 * The sweep (D6) runs in the open after recovery and load_parts and before any thread of the engine starts: a settle
 * puts its chunks before any file names them, so a sweep later than that could delete a live settle's object. It
 * queues every object of the volume that no listed .ec part and no retired name names (the objects of abandoned
 * settles, of swaps refused or cut by a power loss, of parts attached in memory by tests), only with every store UP.
 * It lists file names only and reads one chunk header per object nothing names: an object is queued only when that
 * header names this volume's uuid, since another database on the same stores lists the same volume numbers (and
 * chunk_store_del refuses a chunk of another uuid as well).
 *
 * The queue is worked by a thread of the tier (nice 19, woken by every enqueue, and every 2 s while entries wait).
 * Lock order: db->lock > the tier gc mutex (a leaf: never held across I/O or another lock).
 */
#include "ecgc.h"
#include "chunk.h"
#include "coldops.h"
#include "eccache.h"
#include "ecpart.h"
#include "tier.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

int ecgc_mutant;
atomic_bool ecgc_test_manual;
atomic_bool ecgc_test_nosweep;
atomic_int ecgc_test_del_fail;
void (*ecgc_test_after_del)(struct tier *t, unsigned pos);

#define GC_RETIRED "retired_"

enum { TK_WAITING = 0, TK_QUEUED = 1, TK_DELETED = 2 };

typedef struct gc_ticket {
    atomic_int refs;
    atomic_int state;
} gc_ticket;

static void tk_put(gc_ticket *tk)
{
    if (tk && atomic_fetch_sub_explicit(&tk->refs, 1, memory_order_acq_rel) == 1)
        free(tk);
}

typedef struct gc_item {
    uint32_t volume;
    uint16_t npos;
    uint8_t vuuid[16], oid[16];
    gc_ticket *tk; /* NULL: nobody waits for it (an orphan, an unlisted object) */
} gc_item;

typedef struct gc_hold {
    const part *p;
    uint32_t volume;
    uint8_t oid[16];
} gc_hold;

/* sweepinflight's deferred listing (a mutant only) */
typedef struct gc_deferred {
    uint32_t volume;
    uint8_t vuuid[16];
    uint8_t (*named)[16];
    size_t nnamed;
} gc_deferred;

struct ecgc_tier {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    gc_item *q; /* the entries are q[qh .. nq): a pop moves qh, a push compacts only once qh is half of nq */
    size_t qh, nq, capq;
    gc_hold *holds;
    size_t nholds, capholds;
    gc_deferred *def;
    size_t ndef;
    pthread_t thread;
    bool has_thread, stop, kick;
    atomic_uint_fast64_t queued, deleted, retries, swept, sweeps, sweeps_skipped, unlinked;
};

/* A retired name of the engine (db->lock; the open before any thread). object false: a test-marked part. */
typedef struct gc_rec {
    char name[256];
    uint8_t oid[16];
    uint16_t npos;
    bool object;
    gc_ticket *tk;
} gc_rec;

struct ecgc_eng {
    gc_rec *r;
    size_t n, cap;
};

/* ------------------------------------------------------------ the tier's queue and holds */

static size_t qlen(const struct ecgc_tier *g) { return g->nq - g->qh; }

static int push_locked(struct ecgc_tier *g, const gc_item *it)
{
    if (g->nq == g->capq && g->qh && g->qh >= g->nq / 2) { /* at most half moves, after at least as many pops */
        memmove(g->q, g->q + g->qh, (g->nq - g->qh) * sizeof *g->q);
        g->nq -= g->qh;
        g->qh = 0;
    }
    if (g->nq == g->capq) {
        size_t cap = g->capq ? 2 * g->capq : 16;
        gc_item *q = realloc(g->q, cap * sizeof *q);
        if (!q)
            return TSDB_ERR_NOMEM;
        g->q = q;
        g->capq = cap;
    }
    g->q[g->nq++] = *it;
    return 0;
}

static int enqueue(tier *t, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16], unsigned npos,
                   gc_ticket *tk)
{
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g || npos == 0 || npos > 0xffff)
        return TSDB_ERR_INVALID;
    gc_item it = {.volume = volume, .npos = (uint16_t)npos, .tk = tk};
    memcpy(it.vuuid, vuuid, 16);
    memcpy(it.oid, oid, 16);
    if (tk)
        atomic_fetch_add(&tk->refs, 1);
    pthread_mutex_lock(&g->mu);
    int rc = push_locked(g, &it);
    if (rc == 0) {
        g->kick = true;
        pthread_cond_signal(&g->cv);
    }
    pthread_mutex_unlock(&g->mu);
    if (rc)
        tk_put(tk);
    else
        atomic_fetch_add(&g->queued, 1);
    return rc;
}

static bool held(tier *t, uint32_t volume, const uint8_t oid[16])
{
    struct ecgc_tier *g = t->gc;
    bool h = false;
    pthread_mutex_lock(&g->mu);
    for (size_t i = 0; i < g->nholds && !h; i++)
        h = g->holds[i].volume == volume && !memcmp(g->holds[i].oid, oid, 16);
    pthread_mutex_unlock(&g->mu);
    return h;
}

static int hold(tier *t, const part *p, uint32_t volume, const uint8_t oid[16])
{
    struct ecgc_tier *g = t->gc;
    int rc = 0;
    pthread_mutex_lock(&g->mu);
    if (g->nholds == g->capholds) {
        size_t cap = g->capholds ? 2 * g->capholds : 16;
        gc_hold *h = realloc(g->holds, cap * sizeof *h);
        if (h) {
            g->holds = h;
            g->capholds = cap;
        } else {
            rc = TSDB_ERR_NOMEM;
        }
    }
    if (rc == 0) {
        gc_hold *h = &g->holds[g->nholds++];
        h->p = p;
        h->volume = volume;
        memcpy(h->oid, oid, 16);
    }
    pthread_mutex_unlock(&g->mu);
    return rc;
}

static void release(tier *t, const part *p)
{
    struct ecgc_tier *g = t->gc;
    pthread_mutex_lock(&g->mu);
    for (size_t i = 0; i < g->nholds; i++)
        if (g->holds[i].p == p) {
            g->holds[i] = g->holds[--g->nholds];
            break;
        }
    pthread_mutex_unlock(&g->mu);
}

static bool delete_object_cv(tier *t, coldvol *cv, const gc_item *it);

/* every position of one object; true when all are gone with every store UP throughout. Through the volume's ops of its
 * latest attach (the tier keeps them until it closes, so an item queued by an engine that closed since finds them),
 * at the epoch of its provider (D4: 1 on a single server; a delete above a store's epoch would raise it). */
static bool delete_object(tier *t, const gc_item *it)
{
    coldvol *cv = tier_cold_get(t, it->volume);
    bool ok = cv && delete_object_cv(t, cv, it);
    tier_cold_put(cv);
    return ok;
}

static bool delete_object_cv(tier *t, coldvol *cv, const gc_item *it)
{
    uint64_t epoch = coldvol_epoch(cv);
    if (epoch == 0 || !cv->ops->writable(cv->ctx))
        return false;
    for (int f = atomic_load(&ecgc_test_del_fail); f > 0; f = atomic_load(&ecgc_test_del_fail))
        if (atomic_compare_exchange_weak(&ecgc_test_del_fail, &f, f - 1))
            return false;
    eccache_forget(t, it->volume, it->oid); /* no reader holds it (a part held would hold the object) */
    for (unsigned pos = 0; pos < it->npos; pos++) {
        int rc = cv->ops->del(cv->ctx, it->oid, pos, epoch, COLD_DEL_GC);
        if (rc && rc != TSDB_ERR_NOTFOUND)
            return false; /* a store that failed in service answers TSDB_ERR_IO for every delete after it */
        if (ecgc_test_after_del)
            ecgc_test_after_del(t, pos);
    }
    /* A backstop: chunk_be_del skips a store that holds no open store (DOWN), and counts the skip as NOTFOUND. No store
     * goes from open to DOWN in service today (a failed one stays open and refuses, a replaced one is swapped for the
     * new one), so nothing reaches this but a future change of the backend; the noafter mutant cannot be killed. */
    return ecgc_mutant == GC_MUT_NOAFTER || cv->ops->writable(cv->ctx);
}

static int sweep_list(tier *t, uint32_t volume, const uint8_t vuuid[16], uint8_t (*named)[16], size_t nnamed);

size_t ecgc_tier_work(tier *t)
{
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return 0;
    pthread_mutex_lock(&g->mu);
    gc_deferred *def = g->def;
    size_t ndef = g->ndef;
    g->def = NULL;
    g->ndef = 0;
    pthread_mutex_unlock(&g->mu);
    for (size_t i = 0; i < ndef; i++) {
        (void)sweep_list(t, def[i].volume, def[i].vuuid, def[i].named, def[i].nnamed);
        free(def[i].named);
    }
    free(def);
    pthread_mutex_lock(&g->mu);
    size_t round = qlen(g);
    pthread_mutex_unlock(&g->mu);

    /* one round: the entries there before it, each once; a kept one goes to the back */
    for (size_t k = 0; k < round; k++) {
        pthread_mutex_lock(&g->mu);
        if (qlen(g) == 0 || g->stop) {
            pthread_mutex_unlock(&g->mu);
            break;
        }
        gc_item it = g->q[g->qh++];
        if (g->qh == g->nq)
            g->qh = g->nq = 0;
        pthread_mutex_unlock(&g->mu);
        if (delete_object(t, &it)) {
            if (it.tk)
                atomic_store(&it.tk->state, TK_DELETED);
            tk_put(it.tk);
            atomic_fetch_add(&g->deleted, 1);
            continue;
        }
        atomic_fetch_add(&g->retries, 1);
        pthread_mutex_lock(&g->mu);
        int rc = push_locked(g, &it); /* cannot fail: the slot it left is free (a compaction makes room) */
        pthread_mutex_unlock(&g->mu);
        if (rc)
            tk_put(it.tk);
    }
    pthread_mutex_lock(&g->mu);
    size_t left = qlen(g);
    pthread_mutex_unlock(&g->mu);
    return left;
}

static void *gc_main(void *arg)
{
    tier *t = arg;
    struct ecgc_tier *g = t->gc;
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only, as D11's settle thread */
    pthread_mutex_lock(&g->mu);
    while (!g->stop) {
        if (!g->kick && ((qlen(g) == 0 && g->ndef == 0) || ecgc_mutant == GC_MUT_NORETRYTIMER)) {
            pthread_cond_wait(&g->cv, &g->mu);
            continue;
        }
        if (!g->kick) { /* entries kept from the last round (a transient error): again in 2 s */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 2;
            pthread_cond_timedwait(&g->cv, &g->mu, &ts);
            if (g->stop)
                break;
        }
        g->kick = false;
        pthread_mutex_unlock(&g->mu);
        ecgc_tier_work(t);
        pthread_mutex_lock(&g->mu);
    }
    pthread_mutex_unlock(&g->mu);
    return NULL;
}

int ecgc_tier_open(tier *t)
{
    EC_CALLED(EC_CALL_TIER_OPEN);
    struct ecgc_tier *g = calloc(1, sizeof *g);
    if (!g)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&g->mu, NULL);
    pthread_cond_init(&g->cv, NULL);
    t->gc = g;
    if (!atomic_load(&ecgc_test_manual)) {
        if (tsdb_thread_create(&g->thread, gc_main, t) != 0)
            return TSDB_ERR_NOMEM; /* tier_put runs ecgc_tier_close */
        g->has_thread = true;
    }
    return 0;
}

void ecgc_tier_close(tier *t)
{
    EC_CALLED(EC_CALL_TIER_CLOSE);
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return;
    pthread_mutex_lock(&g->mu);
    g->stop = true;
    pthread_cond_broadcast(&g->cv);
    pthread_mutex_unlock(&g->mu);
    if (g->has_thread)
        pthread_join(g->thread, NULL);
    /* what is left stays on the stores: named objects for the next open's purge, the rest for its sweep */
    for (size_t i = g->qh; i < g->nq; i++)
        tk_put(g->q[i].tk);
    free(g->q);
    free(g->holds);
    for (size_t i = 0; i < g->ndef; i++)
        free(g->def[i].named);
    free(g->def);
    pthread_mutex_destroy(&g->mu);
    pthread_cond_destroy(&g->cv);
    free(g);
    t->gc = NULL;
}

void ecgc_get_counters(tier *t, ecgc_counters *out)
{
    memset(out, 0, sizeof *out);
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return;
    out->queued = atomic_load(&g->queued);
    out->deleted = atomic_load(&g->deleted);
    out->retries = atomic_load(&g->retries);
    out->swept = atomic_load(&g->swept);
    out->sweeps = atomic_load(&g->sweeps);
    out->sweeps_skipped = atomic_load(&g->sweeps_skipped);
    out->unlinked = atomic_load(&g->unlinked);
    pthread_mutex_lock(&g->mu);
    out->pending = qlen(g);
    out->holds = g->nholds;
    pthread_mutex_unlock(&g->mu);
}

/* ------------------------------------------------------------ the engine's retired names */

int ecgc_eng_open(eng *db)
{
    EC_CALLED(EC_CALL_ENG_OPEN);
    struct ecgc_eng *e = calloc(1, sizeof *e);
    if (!e)
        return TSDB_ERR_NOMEM;
    db->ecstate->gc = e;
    return 0;
}

void ecgc_eng_close(eng *db)
{
    EC_CALLED(EC_CALL_ENG_CLOSE);
    struct ecgc_eng *e = db->ecstate ? db->ecstate->gc : NULL;
    if (!e)
        return;
    for (size_t i = 0; i < e->n; i++)
        tk_put(e->r[i].tk); /* a queued object is still deleted; its name stays for the next open */
    free(e->r);
    free(e);
    db->ecstate->gc = NULL;
}

static struct ecgc_eng *eng_gc(const eng *db) { return db->ecstate ? db->ecstate->gc : NULL; }

static gc_rec *rec_find(struct ecgc_eng *e, const char *name)
{
    for (size_t i = 0; e && i < e->n; i++)
        if (!strcmp(e->r[i].name, name))
            return &e->r[i];
    return NULL;
}

static void rec_remove(struct ecgc_eng *e, gc_rec *r)
{
    tk_put(r->tk);
    *r = e->r[--e->n];
}

static gc_rec *rec_add(struct ecgc_eng *e, const char *name, const uint8_t *oid, unsigned npos, bool object)
{
    if (e->n == e->cap) {
        size_t cap = e->cap ? 2 * e->cap : 16;
        gc_rec *r = realloc(e->r, cap * sizeof *r);
        if (!r)
            return NULL;
        e->r = r;
        e->cap = cap;
    }
    gc_ticket *tk = calloc(1, sizeof *tk);
    if (!tk)
        return NULL;
    atomic_init(&tk->refs, 1);
    atomic_init(&tk->state, TK_WAITING);
    gc_rec *r = &e->r[e->n++];
    memset(r, 0, sizeof *r);
    snprintf(r->name, sizeof r->name, "%s", name);
    if (oid)
        memcpy(r->oid, oid, 16);
    r->npos = (uint16_t)npos;
    r->object = object;
    r->tk = tk;
    return r;
}

bool ecgc_owns_name(const part *p)
{
    struct ecpart *e = atomic_load(&((part *)p)->ec);
    return e && !e->test_mark;
}

int ecgc_load_retired(eng *db, const char *name, bool read_only)
{
    EC_CALLED(EC_CALL_GC_LOAD);
    struct ecgc_eng *e = eng_gc(db);
    /* Read-only, or no tier: the file is left alone (no tier can delete its chunks; a writer's open with a tier
     * refuses a volume whose META says requires=tier, and tests of the .ec file run without one). */
    if (read_only || !db->tier || !e)
        return 0;
    part *p = NULL;
    int rc = part_open_ec(db, name, name + strlen(GC_RETIRED), &p);
    if (rc == TSDB_ERR_NOTFOUND)
        return 0; /* removed while load_parts listed the directory */
    if (rc) {
        fprintf(stderr, "tsdb: %s/%s: the record of a retired erasure-coded part cannot be read (%s): the open fails\n",
                db->dir, name, tsdb_strerror(rc));
        return rc;
    }
    struct ecpart *ep = atomic_load(&p->ec);
    uint8_t oid[16];
    memcpy(oid, ep->rec.oid, 16);
    unsigned npos = (unsigned)ep->rec.k + ep->rec.m;
    part_unref(p); /* never listed: no hold, nothing queued */
    int64_t life = db->opt.obsolete_part_lifetime_s > 0 ? (int64_t)db->opt.obsolete_part_lifetime_s * 1000 : 0;
    if (!rec_add(e, name, oid, npos, true) || graveyard_add(db, name, GY_EC, eng_mono_ms() + life) != 0)
        return TSDB_ERR_NOMEM;
    return 0;
}

void ecgc_retire_locked(eng *db, part *p)
{
    EC_CALLED(EC_CALL_GC_RETIRE);
    struct ecgc_eng *e = eng_gc(db);
    struct ecpart *ep = atomic_load(&p->ec);
    if (!e || !ep || !db->tier)
        return;
    char retired[sizeof p->name + sizeof GC_RETIRED];
    snprintf(retired, sizeof retired, GC_RETIRED "%s", p->name);
    bool object = !ep->test_mark;
    gc_rec *r = rec_add(e, retired, object ? ep->rec.oid : NULL, object ? (unsigned)ep->rec.k + ep->rec.m : 0,
                        object);
    /* out of memory: no record, and a GY_EC entry without one is kept (its name stays for the next open) */
    if (!r)
        return;
    if (object && hold(db->tier, p, db->tier_vol, ep->rec.oid) != 0) {
        rec_remove(e, r); /* no hold, no record: nothing deletes the object while p is read */
        return;
    }
    /* db_retire_locked adds the entry of a lifetime; with none it is due now, and the name is still ours to unlink
     * (ecgc_owns_name). Unrecorded (out of memory): the next open. */
    if (object && db->opt.obsolete_part_lifetime_s <= 0)
        (void)graveyard_add(db, retired, GY_EC, eng_mono_ms());
}

bool ecgc_purge_entry(eng *db, const graveyard_entry *ge, bool all, int64_t now)
{
    EC_CALLED(EC_CALL_GC_PURGE);
    struct ecgc_eng *e = eng_gc(db);
    gc_rec *r = rec_find(e, ge->name);
    if (!r && db->tier) /* out of memory at the retire: the name stays for the next open */
        return false;
    if (!r) { /* no tier (a test-marked part): a file and nothing else */
        if (!all && ge->deadline > now)
            return false;
        unlinkat(db->dirfd, ge->name, 0);
        return true;
    }
    if (!all && ge->deadline > now)
        return false;
    if (!r->object) { /* a test-marked part: a file and nothing else */
        unlinkat(db->dirfd, ge->name, 0);
        rec_remove(e, r);
        return true;
    }
    if (ecgc_mutant != GC_MUT_NOHOLD && held(db->tier, db->tier_vol, r->oid))
        return false;
    switch (atomic_load(&r->tk->state)) {
    case TK_WAITING:
        if (ecgc_mutant == GC_MUT_UNLINKFIRST) {
            unlinkat(db->dirfd, ge->name, 0);
            eng_sync_dir(db, "fsync of the directory after removing a retired erasure-coded part");
            (void)enqueue(db->tier, db->tier_vol, tier_vuuid(db), r->oid, r->npos, NULL);
            rec_remove(e, r);
            return true;
        }
        if (enqueue(db->tier, db->tier_vol, tier_vuuid(db), r->oid, r->npos, r->tk) == 0)
            atomic_store(&r->tk->state, TK_QUEUED);
        return false;
    case TK_QUEUED:
        return false;
    default: /* TK_DELETED: every chunk is gone, so the name may go */
        break;
    }
    /* No directory fsync (none per name under db->lock on the flusher): a name that a power loss brings back is
     * loaded again, its deletes find nothing (NOTFOUND counts as gone) and it goes, as GY_PART names go unsynced. */
    if (unlinkat(db->dirfd, ge->name, 0) == 0 || errno == ENOENT)
        atomic_fetch_add(&db->tier->gc->unlinked, 1);
    rec_remove(e, r);
    return true;
}

void ecgc_last_unref(part *p)
{
    EC_CALLED(EC_CALL_GC_LAST_UNREF);
    struct ecpart *e = p->ec;
    if (e && !e->test_mark && e->tier && e->tier->gc && ecgc_mutant != GC_MUT_UNREFLEAK)
        release(e->tier, p);
    ecpart_free(e);
    p->ec = NULL;
}

int ecgc_queue_object(eng *db, const uint8_t oid[16], unsigned npos)
{
    if (!db->tier)
        return TSDB_ERR_INVALID;
    return enqueue(db->tier, db->tier_vol, tier_vuuid(db), oid, npos, NULL);
}

void ecgc_unlisted_object(eng *db, const part *p)
{
    struct ecpart *e = atomic_load(&((part *)p)->ec);
    if (!e || e->test_mark || !db->tier || ecgc_mutant == GC_MUT_UNLISTEDNOOP)
        return;
    (void)ecgc_queue_object(db, e->rec.oid, (unsigned)e->rec.k + e->rec.m); /* unqueued: the next open's sweep */
}

/* ------------------------------------------------------------ the sweep (D6) */

/* A chunk that no listed or retired name names: its object is ours to delete only if a header says it is this volume's
 * (another database on the same stores lists the same volume number: rev F1). */
typedef struct sweep_ent {
    uint8_t oid[16];
    uint16_t pos;
    int32_t where; /* the ops' token for where the chunk is (chunk_listed.store): only cold_ops.owner reads it */
} sweep_ent;

typedef struct sweep_ctx {
    uint8_t (*named)[16];
    size_t nnamed;
    sweep_ent *e;
    size_t n, cap;
    bool nomem;
} sweep_ctx;

static int oid_cmp(const void *a, const void *b) { return memcmp(a, b, 16); }

static int ent_cmp(const void *a, const void *b)
{
    const sweep_ent *x = a, *y = b;
    int c = memcmp(x->oid, y->oid, 16);
    return c ? c : (x->pos > y->pos) - (x->pos < y->pos);
}

static int list_one(void *ctx, const chunk_listed *c)
{
    sweep_ctx *s = ctx;
    if (s->nnamed && bsearch(c->oid, s->named, s->nnamed, 16, oid_cmp))
        return 0;
    if (s->n == s->cap) {
        size_t cap = s->cap ? 2 * s->cap : 64;
        sweep_ent *e = realloc(s->e, cap * sizeof *e);
        if (!e) {
            s->nomem = true;
            return 1;
        }
        s->e = e;
        s->cap = cap;
    }
    sweep_ent *e = &s->e[s->n++];
    memcpy(e->oid, c->oid, 16);
    e->pos = c->pos;
    e->where = c->store;
    return 0;
}

/* 1: a header of the object names vuuid; 0: it names another volume; -1: no header of it could be read */
static int obj_owner(coldvol *cv, const uint8_t vuuid[16], const sweep_ent *e, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        chunk_listed c = {.pos = e[i].pos, .store = e[i].where};
        memcpy(c.oid, e[i].oid, 16);
        uint8_t u[16];
        if (cv->ops->owner(cv->ctx, &c, u) == 0)
            return !memcmp(u, vuuid, 16) || ecgc_mutant == GC_MUT_SWEEPFOREIGN;
    }
    return -1;
}

/* Lists the volume's chunks by name (no file opened), reads one header per object nothing names, and queues those
 * this volume wrote. TSDB_ERR_NOMEM, or 0 (a skip says why on stderr). */
static int sweep_cv(tier *t, coldvol *cv, uint32_t volume, const uint8_t vuuid[16], uint8_t (*named)[16],
                    size_t nnamed);

static int sweep_list(tier *t, uint32_t volume, const uint8_t vuuid[16], uint8_t (*named)[16], size_t nnamed)
{
    struct ecgc_tier *g = t->gc;
    atomic_fetch_add(&g->sweeps, 1);
    coldvol *cv = tier_cold_get(t, volume);
    int rc = sweep_cv(t, cv, volume, vuuid, named, nnamed);
    tier_cold_put(cv);
    return rc;
}

static int sweep_cv(tier *t, coldvol *cv, uint32_t volume, const uint8_t vuuid[16], uint8_t (*named)[16],
                    size_t nnamed)
{
    struct ecgc_tier *g = t->gc;
    if (!cv || !cv->ops->writable(cv->ctx)) {
        atomic_fetch_add(&g->sweeps_skipped, 1);
        fprintf(stderr, "tsdb: volume %u: the sweep of orphan chunks is skipped: a chunk store is not up\n", volume);
        return 0;
    }
    if (nnamed)
        qsort(named, nnamed, 16, oid_cmp);
    sweep_ctx s = {.named = named, .nnamed = nnamed};
    int rc = cv->ops->list_names(cv->ctx, list_one, &s);
    if (s.nomem) {
        free(s.e);
        return TSDB_ERR_NOMEM;
    }
    if (rc) { /* a listing cut short says nothing of what it did not reach: nothing is queued from it */
        atomic_fetch_add(&g->sweeps_skipped, 1);
        fprintf(stderr, "tsdb: volume %u: the sweep of orphan chunks is skipped: a chunk store cannot be listed (%s)\n",
                volume, tsdb_strerror(rc));
        free(s.e);
        return 0;
    }
    if (s.n)
        qsort(s.e, s.n, sizeof *s.e, ent_cmp);
    size_t orphans = 0, q = 0, foreign = 0, unread = 0;
    for (size_t i = 0, j; i < s.n; i = j) {
        for (j = i + 1; j < s.n && !memcmp(s.e[j].oid, s.e[i].oid, 16); j++)
            ;
        int own = obj_owner(cv, vuuid, s.e + i, j - i);
        if (own == 0) {
            foreign++;
            continue;
        }
        if (own < 0) {
            unread++;
            continue;
        }
        orphans++;
        if (enqueue(t, volume, vuuid, s.e[i].oid, s.e[j - 1].pos + 1u, NULL) == 0) /* sorted: the last is the highest */
            q++;
    }
    atomic_fetch_add(&g->swept, q);
    if (orphans)
        fprintf(stderr, "tsdb: volume %u: %zu orphan objects in the chunk stores, %zu queued for deletion\n", volume,
                orphans, q);
    if (foreign)
        fprintf(stderr,
                "tsdb: volume %u: the chunk stores hold %zu objects of another volume uuid (another database on the "
                "same stores?): left alone\n",
                volume, foreign);
    if (unread)
        fprintf(stderr, "tsdb: volume %u: %zu objects named by no part have no readable chunk header: left alone\n",
                volume, unread);
    free(s.e);
    return 0;
}

int ecgc_open_sweep(eng *db)
{
    EC_CALLED(EC_CALL_GC_SWEEP);
    tier *t = db->tier;
    struct ecgc_eng *e = eng_gc(db);
    if (!t || !t->gc || !e || atomic_load(&ecgc_test_nosweep))
        return 0;
    /* what the engine names: its listed .ec parts and its retired names (no thread of it runs yet) */
    pthread_mutex_lock(&db->lock);
    size_t n = 0;
    uint8_t(*named)[16] = malloc((db->nparts + e->n + 1) * 16);
    if (!named) {
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < db->nparts; i++) {
        struct ecpart *ep = atomic_load(&db->parts[i]->ec);
        if (ep && !ep->test_mark)
            memcpy(named[n++], ep->rec.oid, 16);
    }
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < e->n && ecgc_mutant != GC_MUT_SWEEPLIVE; i++)
        if (e->r[i].object)
            memcpy(named[n++], e->r[i].oid, 16);
    if (ecgc_mutant == GC_MUT_SWEEPINFLIGHT) { /* planted: the listing waits for the queue's first round */
        struct ecgc_tier *g = t->gc;
        pthread_mutex_lock(&g->mu);
        gc_deferred *d = realloc(g->def, (g->ndef + 1) * sizeof *d);
        if (d) {
            g->def = d;
            d = &g->def[g->ndef++];
            d->volume = db->tier_vol;
            memcpy(d->vuuid, tier_vuuid(db), 16);
            d->named = named;
            d->nnamed = n;
            named = NULL;
            g->kick = true;
            pthread_cond_signal(&g->cv);
        }
        pthread_mutex_unlock(&g->mu);
        free(named);
        return 0;
    }
    int rc = sweep_list(t, db->tier_vol, tier_vuuid(db), named, n);
    free(named);
    return rc;
}
