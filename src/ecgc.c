/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (src/ecgc.h; R7, R8 r8k-gc, design-cluster-data.md E14, 2.4, 15.7; D6, D13).
 *
 * Single-server tiers keep R7's three rules:
 * 1. A retired .ec part keeps its name retired_<name>.ec until every chunk of its object is deleted.
 * 2. Nothing is deleted while any part of the object is referenced. Holds are taken in part_open_ec (ecgc_hold) and
 *    dropped in ecgc_last_unref, keyed by part pointer in an O(1) hash paired with an O(1) (volume, oid) refcount hash
 *    in the tier, so a reopen's old engine still reading a part the new engine retires keeps its object alive (r7-gc F5).
 * 3. Chunk I/O runs on the tier's worker, never under an engine lock.
 *
 * Cluster EC volumes (db->ec_cluster): never delete directly through R7's queue or open sweep; registered cluster EC
 * engines are worked by the same tier GC thread through ecgcmk_work (src/ecgcmk.c), which commits MK_GC before any
 * CHUNK_DEL and MK_GC{done} after every position answers.
 *
 * Lock order: db->lock > the tier gc mutex (a leaf: never held across I/O or another lock).
 */
#include "ecgc.h"
#include "chunk.h"
#include "coldops.h"
#include "eccache.h"
#include "ecgcmk.h"
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
_Atomic int64_t ecgc_test_orphan_grace_ms = -1;
_Atomic int64_t ecgc_test_poll_ms = -1;
_Atomic int64_t ecgc_test_lifetime_s = -1;

#define GC_RETIRED "retired_"

int ecgc_mutant_parse(const char *name)
{
    static const struct {
        const char *name;
        int m;
    } tbl[] = {
        {"none", GC_MUT_NONE},
        {"gcunlinkfirst", GC_MUT_UNLINKFIRST},
        {"gcunrefleak", GC_MUT_UNREFLEAK},
        {"gcnohold", GC_MUT_NOHOLD},
        {"sweeplive", GC_MUT_SWEEPLIVE},
        {"sweepinflight", GC_MUT_SWEEPINFLIGHT},
        {"sweepforeign", GC_MUT_SWEEPFOREIGN},
        {"unlistednoop", GC_MUT_UNLISTEDNOOP},
        {"noretrytimer", GC_MUT_NORETRYTIMER},
        {"noafter", GC_MUT_NOAFTER},
        {"gcuncommitted", GC_MUT_UNCOMMITTED},
        {"gcnamedorphan", GC_MUT_NAMEDORPHAN},
        {"gcsuspendignored", GC_MUT_SUSPENDIGNORED},
        {"holdatretire", GC_MUT_HOLDATRET},
        {"gcreplicanoop", GC_MUT_REPLICANOOP},
        {"releasenomark", GC_MUT_RELEASE_NOMARK},
    };
    if (!name)
        return -1;
    for (size_t i = 0; i < sizeof tbl / sizeof *tbl; i++)
        if (strcmp(name, tbl[i].name) == 0)
            return tbl[i].m;
    return -1;
}

static pthread_once_t env_once = PTHREAD_ONCE_INIT;

static void env_init_fn(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *m = getenv("TSDB_ECGC_MUTANT");
    if (m && *m) {
        int v = ecgc_mutant_parse(m);
        if (v < 0) {
            fprintf(stderr, "tsdb: TSDB_ECGC_MUTANT=%s names no mutant: exiting with status 2\n", m);
            _exit(2);
        }
        if (ecgc_mutant == GC_MUT_NONE)
            ecgc_mutant = v;
        fprintf(stderr, "tsdb: ecgc MUTANT %s (tests only)\n", m);
    }
    const char *t = getenv("TSDB_ECGC_TEST");
    if (t && *t) {
        char buf[1024];
        snprintf(buf, sizeof buf, "%s", t);
        char *sp = NULL;
        for (char *tok = strtok_r(buf, ",", &sp); tok; tok = strtok_r(NULL, ",", &sp)) {
            if (strncmp(tok, "orphan_grace_ms=", 16) == 0)
                atomic_store(&ecgc_test_orphan_grace_ms, strtoll(tok + 16, NULL, 10));
            else if (strncmp(tok, "poll_ms=", 8) == 0)
                atomic_store(&ecgc_test_poll_ms, strtoll(tok + 8, NULL, 10));
            else if (strncmp(tok, "lifetime_s=", 11) == 0)
                atomic_store(&ecgc_test_lifetime_s, strtoll(tok + 11, NULL, 10));
            else
                ecgcmk_test_opt(tok);
        }
    }
#endif
}

void ecgc_env_init(void) { pthread_once(&env_once, env_init_fn); }

static int64_t gc_life_ms(const eng *db)
{
    ecgc_env_init();
    int64_t ov = atomic_load(&ecgc_test_lifetime_s);
    if (ov >= 0)
        return ov * 1000;
    return db->opt.obsolete_part_lifetime_s > 0 ? (int64_t)db->opt.obsolete_part_lifetime_s * 1000 : 0;
}

static int64_t gc_poll_ms(void)
{
    ecgc_env_init();
    int64_t ov = atomic_load(&ecgc_test_poll_ms);
    return ov > 0 ? ov : 2000;
}

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

/* O(1) hold tables under g->mu: ptab keyed by part pointer, otab keyed by (volume, oid) with refcount. */
typedef struct gc_phold {
    const part *p;
    uint32_t volume;
    uint8_t oid[16];
} gc_phold;

typedef struct gc_ohold {
    uint32_t volume;
    uint32_t count;
    uint8_t oid[16];
} gc_ohold;

/* sweepinflight's deferred listing (a mutant only) */
typedef struct gc_deferred {
    uint32_t volume;
    uint8_t vuuid[16];
    uint8_t (*named)[16];
    size_t nnamed;
} gc_deferred;

typedef struct gc_eng {
    eng *db;
    unsigned active;
    bool stopping;
} gc_eng;

struct ecgc_tier {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    gc_item *q; /* the entries are q[qh .. nq): a pop moves qh, a push compacts only once qh is half of nq */
    size_t qh, nq, capq;
    gc_phold *ptab;
    size_t nholds, pcap;
    gc_ohold *otab;
    size_t noholds, ocap;
    gc_deferred *def;
    size_t ndef;
    gc_eng *engs; /* registered cluster EC engines */
    size_t neng, capeng;
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
    int64_t deadline; /* monotonic ms: when cluster MK_GC may collect this retired object */
    gc_ticket *tk;
} gc_rec;

struct ecgc_eng {
    gc_rec *r;
    size_t n, cap;
};

/* ------------------------------------------------------------ O(1) holds hash (under g->mu) */

static size_t ptr_hash(const part *p, size_t cap)
{
    uint64_t h = (uint64_t)(uintptr_t)p;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return (size_t)h & (cap - 1);
}

static size_t oid_hash(uint32_t volume, const uint8_t oid[16], size_t cap)
{
    uint64_t a, b;
    memcpy(&a, oid, 8);
    memcpy(&b, oid + 8, 8);
    uint64_t h = a ^ (b * 0x9e3779b97f4a7c15ULL) ^ ((uint64_t)volume * 0xbf58476d1ce4e5b9ULL);
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    return (size_t)h & (cap - 1);
}

static int ptab_grow(struct ecgc_tier *g)
{
    if ((g->nholds + 1) * 2 <= g->pcap)
        return 0;
    size_t ncap = g->pcap ? g->pcap * 2 : 64;
    gc_phold *nt = calloc(ncap, sizeof *nt);
    if (!nt)
        return TSDB_ERR_NOMEM;
    size_t mask = ncap - 1;
    for (size_t i = 0; i < g->pcap; i++) {
        if (!g->ptab[i].p)
            continue;
        size_t j = ptr_hash(g->ptab[i].p, ncap);
        while (nt[j].p)
            j = (j + 1) & mask;
        nt[j] = g->ptab[i];
    }
    free(g->ptab);
    g->ptab = nt;
    g->pcap = ncap;
    return 0;
}

static int otab_grow(struct ecgc_tier *g)
{
    if ((g->noholds + 1) * 2 <= g->ocap)
        return 0;
    size_t ncap = g->ocap ? g->ocap * 2 : 64;
    gc_ohold *nt = calloc(ncap, sizeof *nt);
    if (!nt)
        return TSDB_ERR_NOMEM;
    size_t mask = ncap - 1;
    for (size_t i = 0; i < g->ocap; i++) {
        if (!g->otab[i].count)
            continue;
        size_t j = oid_hash(g->otab[i].volume, g->otab[i].oid, ncap);
        while (nt[j].count)
            j = (j + 1) & mask;
        nt[j] = g->otab[i];
    }
    free(g->otab);
    g->otab = nt;
    g->ocap = ncap;
    return 0;
}

static void otab_inc(struct ecgc_tier *g, uint32_t volume, const uint8_t oid[16])
{
    size_t mask = g->ocap - 1;
    size_t i = oid_hash(volume, oid, g->ocap);
    while (g->otab[i].count) {
        if (g->otab[i].volume == volume && memcmp(g->otab[i].oid, oid, 16) == 0) {
            g->otab[i].count++;
            return;
        }
        i = (i + 1) & mask;
    }
    g->otab[i].volume = volume;
    g->otab[i].count = 1;
    memcpy(g->otab[i].oid, oid, 16);
    g->noholds++;
}

static void otab_dec(struct ecgc_tier *g, uint32_t volume, const uint8_t oid[16])
{
    if (!g->ocap)
        return;
    size_t mask = g->ocap - 1;
    size_t i = oid_hash(volume, oid, g->ocap);
    while (g->otab[i].count) {
        if (g->otab[i].volume == volume && memcmp(g->otab[i].oid, oid, 16) == 0) {
            if (--g->otab[i].count > 0)
                return;
            g->noholds--;
            size_t j = (i + 1) & mask;
            while (g->otab[j].count) {
                size_t k = oid_hash(g->otab[j].volume, g->otab[j].oid, g->ocap);
                if (((j - k) & mask) >= ((j - i) & mask)) {
                    g->otab[i] = g->otab[j];
                    g->otab[j].count = 0;
                    i = j;
                }
                j = (j + 1) & mask;
            }
            return;
        }
        i = (i + 1) & mask;
    }
}

static bool held_locked(const struct ecgc_tier *g, uint32_t volume, const uint8_t oid[16])
{
    if (!g->ocap)
        return false;
    size_t mask = g->ocap - 1;
    size_t i = oid_hash(volume, oid, g->ocap);
    while (g->otab[i].count) {
        if (g->otab[i].volume == volume && memcmp(g->otab[i].oid, oid, 16) == 0)
            return true;
        i = (i + 1) & mask;
    }
    return false;
}

bool ecgc_held(tier *t, uint32_t volume, const uint8_t oid[16])
{
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return false;
    pthread_mutex_lock(&g->mu);
    bool h = held_locked(g, volume, oid);
    pthread_mutex_unlock(&g->mu);
    return h;
}

int ecgc_hold(tier *t, const part *p, uint32_t volume, const uint8_t oid[16])
{
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g || !p)
        return 0;
    pthread_mutex_lock(&g->mu);
    if (g->pcap) {
        size_t mask = g->pcap - 1;
        size_t i = ptr_hash(p, g->pcap);
        while (g->ptab[i].p) {
            if (g->ptab[i].p == p) {
                pthread_mutex_unlock(&g->mu);
                return 0; /* already held */
            }
            i = (i + 1) & mask;
        }
    }
    int rc = ptab_grow(g);
    if (rc == 0)
        rc = otab_grow(g);
    if (rc == 0) {
        size_t mask = g->pcap - 1;
        size_t i = ptr_hash(p, g->pcap);
        while (g->ptab[i].p)
            i = (i + 1) & mask;
        g->ptab[i].p = p;
        g->ptab[i].volume = volume;
        memcpy(g->ptab[i].oid, oid, 16);
        g->nholds++;
        otab_inc(g, volume, oid);
    }
    pthread_mutex_unlock(&g->mu);
    return rc;
}

static void release(tier *t, const part *p)
{
    struct ecgc_tier *g = t->gc;
    pthread_mutex_lock(&g->mu);
    if (g->pcap) {
        size_t mask = g->pcap - 1;
        size_t i = ptr_hash(p, g->pcap);
        while (g->ptab[i].p) {
            if (g->ptab[i].p == p) {
                uint32_t vol = g->ptab[i].volume;
                uint8_t oid[16];
                memcpy(oid, g->ptab[i].oid, 16);
                g->ptab[i].p = NULL;
                g->nholds--;
                size_t j = (i + 1) & mask;
                while (g->ptab[j].p) {
                    size_t k = ptr_hash(g->ptab[j].p, g->pcap);
                    if (((j - k) & mask) >= ((j - i) & mask)) {
                        g->ptab[i] = g->ptab[j];
                        g->ptab[j].p = NULL;
                        i = j;
                    }
                    j = (j + 1) & mask;
                }
                otab_dec(g, vol, oid);
                break;
            }
            i = (i + 1) & mask;
        }
    }
    pthread_mutex_unlock(&g->mu);
}

/* ------------------------------------------------------------ the tier's queue */

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

void ecgc_kick_tier(tier *t)
{
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return;
    pthread_mutex_lock(&g->mu);
    g->kick = true;
    pthread_cond_signal(&g->cv);
    pthread_mutex_unlock(&g->mu);
}

void ecgc_note_unlinked(tier *t, uint64_t n)
{
    if (t && t->gc && n)
        atomic_fetch_add(&t->gc->unlinked, n);
}

static bool delete_object_cv(tier *t, coldvol *cv, const gc_item *it);

/* every position of one object; true when all are gone with every store UP throughout. */
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

    /* one round of R7 single-server deletes: the entries there before it, each once; a kept one goes to the back */
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

    /* R8 cluster EC engines: MK_GC (retired objects, pending deletes, orphans) */
    pthread_mutex_lock(&g->mu);
    for (size_t i = 0; !g->stop && i < g->neng;) {
        if (g->engs[i].stopping) {
            i++;
            continue;
        }
        eng *db = g->engs[i].db;
        g->engs[i].active++;
        pthread_mutex_unlock(&g->mu);
        ecgcmk_work(db);
        pthread_mutex_lock(&g->mu);
        size_t idx = SIZE_MAX;
        for (size_t j = 0; j < g->neng; j++)
            if (g->engs[j].db == db) {
                idx = j;
                break;
            }
        if (idx != SIZE_MAX) {
            g->engs[idx].active--;
            if (g->engs[idx].active == 0 && g->engs[idx].stopping)
                pthread_cond_broadcast(&g->cv);
            i = idx + 1;
        } else {
            i = 0;
        }
    }
    size_t left = qlen(g);
    pthread_mutex_unlock(&g->mu);
    return left;
}

static void *gc_main(void *arg)
{
    tier *t = arg;
    struct ecgc_tier *g = t->gc;
    ecgc_env_init();
#ifdef __linux__
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only, as D11's settle thread */
#endif
    pthread_mutex_lock(&g->mu);
    while (!g->stop) {
        bool has_work = qlen(g) != 0 || g->ndef != 0;
        bool has_cluster = g->neng != 0;
        if (!g->kick && ((!has_work && !has_cluster) || (!has_cluster && ecgc_mutant == GC_MUT_NORETRYTIMER))) {
            pthread_cond_wait(&g->cv, &g->mu);
            continue;
        }
        if (!g->kick) {
            int64_t ms = has_cluster ? gc_poll_ms() : 2000;
            tsdb_cond_timedwait_ms(&g->cv, &g->mu, (uint64_t)ms);
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
    ecgc_env_init();
    struct ecgc_tier *g = calloc(1, sizeof *g);
    if (!g)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&g->mu, NULL);
    tsdb_cond_init_mono(&g->cv);
    t->gc = g;
    if (!atomic_load(&ecgc_test_manual)) {
        if (tsdb_thread_create(&g->thread, gc_main, t) != 0)
            return TSDB_ERR_NOMEM; /* tier_put runs ecgc_tier_close */
        g->has_thread = true;
    }
    return 0;
}

void ecgc_tier_quiesce(tier *t)
{
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return;
    pthread_mutex_lock(&g->mu);
    g->stop = true;
    pthread_cond_broadcast(&g->cv);
    pthread_mutex_unlock(&g->mu);
    if (g->has_thread) {
        pthread_join(g->thread, NULL);
        g->has_thread = false;
    }
}

void ecgc_tier_close(tier *t)
{
    EC_CALLED(EC_CALL_TIER_CLOSE);
    struct ecgc_tier *g = t ? t->gc : NULL;
    if (!g)
        return;
    ecgc_tier_quiesce(t);
    /* what is left stays on the stores: named objects for the next open's purge, the rest for its sweep */
    for (size_t i = g->qh; i < g->nq; i++)
        tk_put(g->q[i].tk);
    free(g->q);
    free(g->ptab);
    free(g->otab);
    free(g->engs);
    for (size_t i = 0; i < g->ndef; i++)
        free(g->def[i].named);
    free(g->def);
    pthread_mutex_destroy(&g->mu);
    pthread_cond_destroy(&g->cv);
    free(g);
    t->gc = NULL;
}

void ecgc_eng_ready(eng *db)
{
    if (!db || !db->ec_cluster || !db->tier || !db->tier->gc)
        return;
    struct ecgc_tier *g = db->tier->gc;
    pthread_mutex_lock(&g->mu);
    for (size_t i = 0; i < g->neng; i++)
        if (g->engs[i].db == db) {
            g->engs[i].stopping = false;
            g->kick = true;
            pthread_cond_signal(&g->cv);
            pthread_mutex_unlock(&g->mu);
            return;
        }
    if (g->neng == g->capeng) {
        size_t cap = g->capeng ? g->capeng * 2 : 8;
        gc_eng *e = realloc(g->engs, cap * sizeof *e);
        if (e) {
            g->engs = e;
            g->capeng = cap;
        }
    }
    if (g->neng < g->capeng) {
        g->engs[g->neng++] = (gc_eng){.db = db};
        g->kick = true;
        pthread_cond_signal(&g->cv);
    }
    pthread_mutex_unlock(&g->mu);
}

void ecgc_eng_stop(eng *db)
{
    if (!db || !db->tier || !db->tier->gc)
        return;
    ecgcmk_stop(db);
    struct ecgc_tier *g = db->tier->gc;
    pthread_mutex_lock(&g->mu);
    for (;;) {
        size_t idx = SIZE_MAX;
        for (size_t i = 0; i < g->neng; i++)
            if (g->engs[i].db == db) {
                idx = i;
                break;
            }
        if (idx == SIZE_MAX)
            break;
        g->engs[idx].stopping = true;
        if (g->engs[idx].active == 0) {
            g->engs[idx] = g->engs[--g->neng];
            break;
        }
        pthread_cond_wait(&g->cv, &g->mu);
    }
    pthread_mutex_unlock(&g->mu);
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
    ecgc_env_init();
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

static gc_rec *rec_add(struct ecgc_eng *e, const char *name, const uint8_t *oid, unsigned npos, bool object,
                       int64_t deadline)
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
    r->deadline = deadline;
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
    part_unref(p); /* drops the temporary hold taken by part_open_ec */
    int64_t deadline = eng_mono_ms() + gc_life_ms(db);
    if (!rec_add(e, name, oid, npos, true, deadline) || graveyard_add(db, name, GY_EC, deadline) != 0)
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
    int64_t now = eng_mono_ms();
    int64_t deadline = now + gc_life_ms(db);
    gc_rec *r = rec_add(e, retired, object ? ep->rec.oid : NULL, object ? (unsigned)ep->rec.k + ep->rec.m : 0, object,
                        deadline);
    /* out of memory: no record, and a GY_EC entry without one is kept (its name stays for the next open) */
    if (!r)
        return;
    /* Holds are taken in part_open_ec; holdatretire plants the old r7-gc F5 bug of taking them only here. */
    if (object && ecgc_mutant == GC_MUT_HOLDATRET && ecgc_hold(db->tier, p, db->tier_vol, ep->rec.oid) != 0) {
        rec_remove(e, r);
        return;
    }
    /* db_retire_locked adds the entry of a lifetime; with none it is due now, and the name is still ours to unlink
     * (ecgc_owns_name). Unrecorded (out of memory): the next open. */
    if (object && db->opt.obsolete_part_lifetime_s <= 0)
        (void)graveyard_add(db, retired, GY_EC, now);
    if (object && db->ec_cluster && deadline <= now)
        ecgc_kick_tier(db->tier);
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
    if (!all && !db->ec_cluster && ge->deadline > now)
        return false;
    if (!r->object) { /* a test-marked part: a file and nothing else */
        if (!all && ge->deadline > now)
            return false;
        unlinkat(db->dirfd, ge->name, 0);
        rec_remove(e, r);
        return true;
    }
    /* Cluster EC volume (E14, D13): never directly delete chunks; retired objects go only via committed MK_GC. */
    if (db->ec_cluster) {
        if (!db->rp && r->deadline <= now && (ecgc_mutant == GC_MUT_NOHOLD || !ecgc_held(db->tier, db->tier_vol, r->oid)))
            ecgc_kick_tier(db->tier);
        return false;
    }
    if (ecgc_mutant != GC_MUT_NOHOLD && ecgc_held(db->tier, db->tier_vol, r->oid))
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
    if (db->ec_cluster) {
        int rc = ecgcmk_queue_orphan(db, oid);
        if (rc == 0)
            ecgc_kick_tier(db->tier);
        return rc;
    }
    return enqueue(db->tier, db->tier_vol, tier_vuuid(db), oid, npos, NULL);
}

void ecgc_unlisted_object(eng *db, const part *p)
{
    struct ecpart *e = atomic_load(&((part *)p)->ec);
    if (!e || e->test_mark || !db->tier || ecgc_mutant == GC_MUT_UNLISTEDNOOP)
        return;
    (void)ecgc_queue_object(db, e->rec.oid, (unsigned)e->rec.k + e->rec.m); /* unqueued: the next open's sweep */
}

/* ------------------------------------------------------------ helpers for cluster MK_GC (src/ecgcmk.c) */

static bool oid_in_packed(const uint8_t oid[16], const uint8_t *oids, size_t noids)
{
    for (size_t i = 0; i < noids; i++)
        if (memcmp(oid, oids + i * 16, 16) == 0)
            return true;
    return false;
}

int ecgc_take_due_retired(eng *db, int64_t now, uint8_t (**oids_out)[16], size_t *n_out)
{
    *oids_out = NULL;
    *n_out = 0;
    struct ecgc_eng *e = eng_gc(db);
    if (!e || !db->tier)
        return 0;
    pthread_mutex_lock(&db->lock);
    size_t cap = e->n < MK_FILES_MAX ? e->n : MK_FILES_MAX;
    uint8_t (*oids)[16] = cap ? malloc(cap * 16) : NULL;
    if (cap && !oids) {
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_NOMEM;
    }
    size_t n = 0;
    for (size_t i = 0; i < e->n && n < MK_FILES_MAX; i++) {
        if (!e->r[i].object || e->r[i].deadline > now)
            continue;
        if (ecgc_mutant != GC_MUT_NOHOLD && ecgc_held(db->tier, db->tier_vol, e->r[i].oid))
            continue;
        bool dup = false;
        for (size_t j = 0; j < n && !dup; j++)
            dup = memcmp(oids[j], e->r[i].oid, 16) == 0;
        if (!dup)
            memcpy(oids[n++], e->r[i].oid, 16);
    }
    pthread_mutex_unlock(&db->lock);
    if (n == 0) {
        free(oids);
        oids = NULL;
    }
    *oids_out = oids;
    *n_out = n;
    return 0;
}

static int push_unlink(char (**arr)[256], size_t *n, size_t *cap, const char *name)
{
    for (size_t i = 0; i < *n; i++)
        if (strcmp((*arr)[i], name) == 0)
            return 0;
    if (*n == *cap) {
        size_t nc = *cap ? *cap * 2 : 16;
        char (*p)[256] = realloc(*arr, nc * sizeof *p);
        if (!p)
            return TSDB_ERR_NOMEM;
        *arr = p;
        *cap = nc;
    }
    snprintf((*arr)[(*n)++], 256, "%s", name);
    return 0;
}

/* Under db->lock: retires any listed .ec part whose oid is in oids[0..noids), collects all retired_ names for those
 * oids (both newly retired and already in ecgc_eng), and removes their ecgc_eng and GY_EC graveyard entries. */
int ecgc_collect_oids_locked(eng *db, const uint8_t *oids, size_t noids, char (**unlinks_out)[256],
                             size_t *nunlinks_out, bool *retired_listed_out)
{
    *unlinks_out = NULL;
    *nunlinks_out = 0;
    *retired_listed_out = false;
    char (*unlinks)[256] = NULL;
    size_t nu = 0, capu = 0;
    int rc = 0;

    /* 1. Any .ec of those oids still listed (a replica whose TTL is behind): retire it first so a promotion never
     *    lists an .ec whose chunks are going. */
    for (size_t i = 0; i < db->nparts && rc == 0;) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (!ep || ep->test_mark || !oid_in_packed(ep->rec.oid, oids, noids)) {
            i++;
            continue;
        }
        char retired[sizeof p->name + sizeof GC_RETIRED];
        snprintf(retired, sizeof retired, GC_RETIRED "%s", p->name);
        p->dropped = true;
        cls_dropped_note(db, p);
        if (renameat(db->dirfd, p->name, db->dirfd, retired) != 0 && errno != ENOENT) {
            eng_fail(db, "rename of an MK_GC .ec part to retired_", errno);
            rc = TSDB_ERR_IO;
            break;
        }
        p->retired = true;
        atomic_store(&p->obsolete, true);
        rc = push_unlink(&unlinks, &nu, &capu, retired);
        db_parts_remove_locked(db, p);
        *retired_listed_out = true;
    }

    /* 2. Collect and remove matching retired_ records from ecgc_eng and their GY_EC graveyard entries. */
    struct ecgc_eng *e = eng_gc(db);
    for (size_t i = 0; e && i < e->n && rc == 0;) {
        if (!e->r[i].object || !oid_in_packed(e->r[i].oid, oids, noids)) {
            i++;
            continue;
        }
        rc = push_unlink(&unlinks, &nu, &capu, e->r[i].name);
        if (rc == 0) {
            for (size_t g = 0; g < db->ngraveyard;) {
                if (db->graveyard[g].kind == GY_EC && strcmp(db->graveyard[g].name, e->r[i].name) == 0)
                    db->graveyard[g] = db->graveyard[--db->ngraveyard];
                else
                    g++;
            }
            rec_remove(e, &e->r[i]);
        }
    }
    if (rc) {
        free(unlinks);
        return rc;
    }
    *unlinks_out = unlinks;
    *nunlinks_out = nu;
    return 0;
}

int ecgc_list_retired_locked(eng *db, uint8_t (**oids_out)[16], size_t *n_out)
{
    *oids_out = NULL;
    *n_out = 0;
    struct ecgc_eng *e = eng_gc(db);
    if (!e || e->n == 0)
        return 0;
    uint8_t (*oids)[16] = malloc(e->n * 16);
    if (!oids)
        return TSDB_ERR_NOMEM;
    size_t n = 0;
    for (size_t i = 0; i < e->n; i++)
        if (e->r[i].object)
            memcpy(oids[n++], e->r[i].oid, 16);
    if (n == 0) {
        free(oids);
        oids = NULL;
    }
    *oids_out = oids;
    *n_out = n;
    return 0;
}

/* ------------------------------------------------------------ the sweep (D6, single server) */

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
    /* An ec cluster volume (PPOS: src/ppos.h): an object no file of this member names may be one a COLD marker names
     * that is not yet applied here. Its orphans are the leader's to delete by a committed MK_GC (E14), never this
     * member's by file name. */
    if (db->ppos || db->ec_cluster) {
        atomic_fetch_add(&t->gc->sweeps_skipped, 1);
        return 0;
    }
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
