/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tenants in tsdbd (docs/design-stats.md section 4 and 3.4; ST5, ST6, ST16).
 *
 * The list: sorted prefixes, one generation per applied change. Its sources are a file (--tenants, read again at
 * SIGHUP) or, without one, the `prefix:` selectors that users hold `admin` on (auth.c offers them after every
 * publish of USERS). A new set reaches the engine and the server's list only when it differs from the applied one
 * and has stayed the same for the settle time (a second the user stops editing, not a flap); the engine then moves
 * the series that changed tenant in the background (tsdb_set_tenants).
 *
 * The slots: every prefix that was ever in the list has a number, slot 0 is "(none)", and a number is never reused,
 * so a counter only grows (ST16). The counters are per thread (a block of pages of 64 slots, written by its owner with
 * relaxed stores and no read-modify-write, the way the engine's query counters are) and a reader sums them under a
 * lock the writers never take. A thread that ends folds its block into the retired one.
 */
#include "tenants.h"

#include "tsdb_protocol.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TN_MAX 65535u
#define TN_NAME_MAX 32767u /* the engine's longest series name: a longer prefix matches nothing */
#define PAGE_SLOTS 64u
#define NPAGES ((TN_MAX + 1) / PAGE_SLOTS)

/* ------------------------------------------------------------ the counters */

typedef struct tpage {
    _Atomic uint64_t c[PAGE_SLOTS][TN_NC];
} tpage;

typedef struct tblk {
    _Atomic(tpage *) pg[NPAGES];
    struct tblk *next;
} tblk;

static pthread_mutex_t blk_mu = PTHREAD_MUTEX_INITIALIZER; /* the registry of blocks; a writer never takes it */
static tblk *blks;                                         /* live threads' blocks */
static tblk retired;                                       /* the sums of the ended threads' */
static pthread_key_t blk_key;
static pthread_once_t blk_once = PTHREAD_ONCE_INIT;
static __thread tblk *my;

static void blk_end(void *arg)
{
    tblk *b = arg;
    pthread_mutex_lock(&blk_mu);
    for (tblk **pp = &blks; *pp; pp = &(*pp)->next)
        if (*pp == b) {
            *pp = b->next;
            break;
        }
    for (size_t i = 0; i < NPAGES; i++) {
        tpage *p = atomic_load_explicit(&b->pg[i], memory_order_relaxed);
        if (!p)
            continue;
        tpage *q = atomic_load_explicit(&retired.pg[i], memory_order_relaxed);
        if (!q && (q = calloc(1, sizeof *q)))
            atomic_store_explicit(&retired.pg[i], q, memory_order_release);
        for (size_t s = 0; q && s < PAGE_SLOTS; s++)
            for (int k = 0; k < TN_NC; k++)
                atomic_fetch_add_explicit(&q->c[s][k], atomic_load_explicit(&p->c[s][k], memory_order_relaxed),
                                          memory_order_relaxed);
        free(p);
    }
    pthread_mutex_unlock(&blk_mu);
    free(b);
    my = NULL;
}

static void blk_key_make(void) { pthread_key_create(&blk_key, blk_end); }

static tblk *blk_new(void)
{
    pthread_once(&blk_once, blk_key_make);
    tblk *b = calloc(1, sizeof *b);
    if (!b)
        return NULL;
    pthread_mutex_lock(&blk_mu);
    b->next = blks;
    blks = b;
    pthread_mutex_unlock(&blk_mu);
    pthread_setspecific(blk_key, b);
    my = b;
    return b;
}

void tn_count(uint16_t slot, int counter, uint64_t n)
{
    tblk *b = my ? my : blk_new();
    if (!b)
        return;
    _Atomic(tpage *) *pp = &b->pg[slot / PAGE_SLOTS];
    tpage *p = atomic_load_explicit(pp, memory_order_relaxed);
    if (!p) {
        if (!(p = calloc(1, sizeof *p)))
            return;
        atomic_store_explicit(pp, p, memory_order_release);
    }
    _Atomic uint64_t *x = &p->c[slot % PAGE_SLOTS][counter];
    atomic_store_explicit(x, atomic_load_explicit(x, memory_order_relaxed) + n, memory_order_relaxed);
}

void tn_read(uint16_t slot, uint64_t out[TN_NC])
{
    memset(out, 0, TN_NC * sizeof out[0]);
    pthread_mutex_lock(&blk_mu);
    tblk *all[2] = {&retired, blks};
    for (int w = 0; w < 2; w++)
        for (tblk *b = all[w]; b; b = w ? b->next : NULL) {
            tpage *p = atomic_load_explicit(&b->pg[slot / PAGE_SLOTS], memory_order_acquire);
            if (!p)
                continue;
            for (int k = 0; k < TN_NC; k++)
                out[k] += atomic_load_explicit(&p->c[slot % PAGE_SLOTS][k], memory_order_relaxed);
        }
    pthread_mutex_unlock(&blk_mu);
}

/* ------------------------------------------------------------ the list and the slots */

typedef struct tlst {
    char **s; /* sorted bytewise, no duplicates, none empty */
    size_t n, maxlen;
} tlst;

static pthread_mutex_t tn_mu = PTHREAD_MUTEX_INITIALIZER; /* cur, the slot table, pending, the settings */
static tlst cur;
static _Atomic uint32_t gen_now;
static char **slot_str; /* by slot; [0] is "" */
static size_t *slot_len;
static uint16_t *slot_ix; /* the slots 1.. sorted by their prefix */
static size_t nslots = 1, slot_cap;
static tlst pending, latest; /* latest: the newest set any source offered, whatever became of it (tick) */
static bool have_pending, have_latest, file_mode;
static struct timespec pending_since;
static int settle_s = 30;
static char *file_path;
static atomic_bool hup;
static tsdb *tdb;
static pthread_t tn_thread;
static bool tn_running;
static pthread_cond_t stop_cv = PTHREAD_COND_INITIALIZER;
static bool stop_flag;

static int str_cmp(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

void tn_free_list(char **v, size_t n)
{
    for (size_t i = 0; i < n; i++)
        free(v[i]);
    free(v);
}

/* Sorts and drops duplicates and empty prefixes in place; the new count. */
static size_t norm(char **v, size_t n)
{
    qsort(v, n, sizeof *v, str_cmp);
    size_t k = 0;
    for (size_t i = 0; i < n; i++) {
        if (!v[i][0] || (k && strcmp(v[k - 1], v[i]) == 0))
            free(v[i]);
        else
            v[k++] = v[i];
    }
    return k;
}

static void tl_free(tlst *t)
{
    tn_free_list(t->s, t->n);
    memset(t, 0, sizeof *t);
}

static bool tl_copy(tlst *t, char *const *v, size_t n)
{
    char **c = malloc((n ? n : 1) * sizeof *c);
    if (!c)
        return false;
    size_t m = 0;
    for (size_t i = 0; i < n; i++)
        if (!(c[m++] = strdup(v[i]))) {
            tn_free_list(c, m - 1);
            return false;
        }
    t->s = c;
    t->n = norm(c, m);
    t->maxlen = 0;
    for (size_t i = 0; i < t->n; i++)
        if (strlen(c[i]) > t->maxlen)
            t->maxlen = strlen(c[i]);
    return true;
}

static bool tl_equal(const tlst *a, const tlst *b)
{
    if (a->n != b->n)
        return false;
    for (size_t i = 0; i < a->n; i++)
        if (strcmp(a->s[i], b->s[i]))
            return false;
    return true;
}

/* The index of the prefix of exactly this text in t, or -1. */
static long tl_find(const tlst *t, const char *p, size_t len)
{
    size_t lo = 0, hi = t->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const char *s = t->s[mid];
        int c = strncmp(s, p, len);
        if (c == 0)
            c = s[len] ? 1 : 0; /* s is longer than p */
        if (c == 0)
            return (long)mid;
        if (c < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return -1;
}

static int slot_cmp_key(const char *p, size_t len, uint16_t slot)
{
    const char *s = slot_str[slot];
    int c = strncmp(p, s, len);
    if (c == 0)
        c = s[len] ? -1 : 0;
    return c;
}

/* tn_mu held. */
static int slot_find_locked(const char *p, size_t len)
{
    if (len == 0)
        return 0;
    size_t lo = 0, hi = nslots - 1;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int c = slot_cmp_key(p, len, slot_ix[mid]);
        if (c == 0)
            return slot_ix[mid];
        if (c > 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return -1;
}

/* A slot for each prefix of t that has none. tn_mu held. A list past 65,535 prefixes' worth of slots in one process
 * (they are never reused): the rest count under "(none)". */
static void slots_make(const tlst *t)
{
    for (size_t i = 0; i < t->n; i++) {
        size_t len = strlen(t->s[i]);
        if (slot_find_locked(t->s[i], len) >= 0 || nslots > TN_MAX)
            continue;
        if (nslots >= slot_cap) {
            size_t c = slot_cap ? slot_cap * 2 : 64;
            char **ns = realloc(slot_str, c * sizeof *ns);
            if (ns)
                slot_str = ns;
            size_t *nl = realloc(slot_len, c * sizeof *nl);
            if (nl)
                slot_len = nl;
            uint16_t *nx = realloc(slot_ix, c * sizeof *nx);
            if (nx)
                slot_ix = nx;
            if (!ns || !nl || !nx)
                return;
            slot_cap = c;
        }
        char *d = strdup(t->s[i]);
        if (!d)
            return;
        uint16_t id = (uint16_t)nslots++;
        slot_str[id] = d;
        slot_len[id] = len;
        size_t at = 0, hi = (size_t)id - 1; /* the sorted slots 1..id-1 */
        while (at < hi) {
            size_t mid = at + (hi - at) / 2;
            if (strcmp(slot_str[slot_ix[mid]], d) < 0)
                at = mid + 1;
            else
                hi = mid;
        }
        memmove(slot_ix + at + 1, slot_ix + at, ((size_t)id - 1 - at) * sizeof *slot_ix);
        slot_ix[at] = id;
    }
}

int tn_slot_find(const char *prefix, size_t len)
{
    pthread_mutex_lock(&tn_mu);
    int r = slot_find_locked(prefix, len);
    pthread_mutex_unlock(&tn_mu);
    return r;
}

/* The longest prefix of the list the name starts with: its index, or -1. tn_mu held. */
static long longest(const tlst *t, const char *name, size_t len)
{
    for (size_t l = len < t->maxlen ? len : t->maxlen; l > 0; l--) {
        long i = tl_find(t, name, l);
        if (i >= 0)
            return i;
    }
    return -1;
}

uint16_t tn_slot_name(const char *name, size_t len)
{
    pthread_mutex_lock(&tn_mu);
    long i = longest(&cur, name, len);
    int s = i < 0 ? 0 : slot_find_locked(cur.s[i], strlen(cur.s[i]));
    pthread_mutex_unlock(&tn_mu);
    return s < 0 ? 0 : (uint16_t)s;
}

static bool grants_under(const user *u, const char *p, size_t l)
{
    for (uint32_t i = 0; i < u->ng; i++) {
        const user_grant *g = &u->g[i];
        switch (g->kind) {
        case SEL_PREFIX:
        case SEL_NAME:
            if (g->len < l || memcmp(g->sel, p, l))
                return false;
            break;
        case SEL_GLOB:
            if (g->lit_len < l || memcmp(g->lit, p, l))
                return false;
            break;
        default: return false; /* all */
        }
    }
    return true;
}

uint16_t tn_slot_user(const user *u, uint32_t *gen)
{
    size_t nl = strlen(u->name);
    int s = 0;
    pthread_mutex_lock(&tn_mu);
    if (gen)
        *gen = atomic_load_explicit(&gen_now, memory_order_relaxed);
    if (!(u->flags & USER_SUPERUSER))
        for (size_t l = nl < cur.maxlen ? nl : cur.maxlen; l > 0; l--) {
            long i = tl_find(&cur, u->name, l);
            if (i >= 0 && grants_under(u, cur.s[i], l)) {
                s = slot_find_locked(cur.s[i], l);
                break;
            }
        }
    pthread_mutex_unlock(&tn_mu);
    return s < 0 ? 0 : (uint16_t)s;
}

uint32_t tn_gen(void) { return atomic_load_explicit(&gen_now, memory_order_relaxed); }

/* ------------------------------------------------------------ the sources */

int tn_read_file(const char *path, char ***out, size_t *n)
{
    *out = NULL;
    *n = 0;
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "tsdbd: tenants file %s: %s\n", path, strerror(errno));
        return TSDB_ERR_IO;
    }
    char **v = NULL;
    size_t m = 0, cap = 0;
    char *line = NULL;
    size_t lc = 0;
    ssize_t ll;
    int rc = 0;
    while ((ll = getline(&line, &lc, f)) >= 0) {
        char *b = line;
        while (ll > 0 && (b[ll - 1] == '\n' || b[ll - 1] == '\r' || b[ll - 1] == ' ' || b[ll - 1] == '\t'))
            b[--ll] = 0;
        while (*b == ' ' || *b == '\t')
            b++, ll--;
        if (!*b || *b == '#')
            continue;
        if ((size_t)ll > TN_NAME_MAX || strlen(b) != (size_t)ll) {
            fprintf(stderr, "tsdbd: tenants file %s: a prefix longer than %u bytes\n", path, TN_NAME_MAX);
            rc = TSDB_ERR_INVALID;
            break;
        }
        if (m == cap) {
            char **nv = realloc(v, (cap = cap ? cap * 2 : 16) * sizeof *v);
            if (!nv) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            v = nv;
        }
        if (!(v[m] = strdup(b))) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        m++;
    }
    free(line);
    int err = ferror(f);
    fclose(f);
    if (!rc && err) {
        fprintf(stderr, "tsdbd: tenants file %s: read error\n", path);
        rc = TSDB_ERR_IO;
    }
    if (!rc)
        m = m ? norm(v, m) : 0;
    if (!rc && m > TN_MAX) {
        fprintf(stderr, "tsdbd: tenants file %s: more than %u prefixes\n", path, TN_MAX);
        rc = TSDB_ERR_INVALID;
    }
    if (rc) {
        tn_free_list(v, m);
        return rc;
    }
    *out = v;
    *n = m;
    return 0;
}

int tn_from_users(const users *us, char ***out, size_t *n)
{
    *out = NULL;
    *n = 0;
    size_t cap = 16, m = 0;
    char **v = malloc(cap * sizeof *v);
    if (!v)
        return TSDB_ERR_NOMEM;
    for (uint32_t i = 0; i < us->n; i++) {
        const user *u = us->u[i];
        for (uint32_t j = 0; j < u->ng; j++) {
            const user_grant *g = &u->g[j];
            if (g->kind != SEL_PREFIX || !(g->rights & R_ADMIN) || g->len == 0)
                continue;
            if (m == cap) {
                char **nv = realloc(v, (cap *= 2) * sizeof *v);
                if (!nv) {
                    tn_free_list(v, m);
                    return TSDB_ERR_NOMEM;
                }
                v = nv;
            }
            if (!(v[m] = strdup(g->sel))) {
                tn_free_list(v, m);
                return TSDB_ERR_NOMEM;
            }
            m++;
        }
    }
    *out = v;
    *n = norm(v, m);
    return 0;
}

int tn_init(const char *dir, const char *file, bool auth_on, int settle, char ***out, size_t *n)
{
    char **v = NULL;
    size_t m = 0;
    int rc = 0;
    *out = NULL;
    *n = 0;
    if (file) {
        rc = tn_read_file(file, &v, &m);
        file_mode = true;
        if (rc == 0 && !(file_path = strdup(file)))
            rc = TSDB_ERR_NOMEM;
    } else if (auth_on) {
        users us;
        memset(&us, 0, sizeof us);
        rc = users_open(&us, dir, false);
        if (rc == 0) {
            rc = tn_from_users(&us, &v, &m);
            users_close(&us);
        }
        if (rc != 0 && rc != TSDB_ERR_NOMEM) { /* auth_init reports a damaged USERS in its own words */
            v = NULL, m = 0, rc = 0;
        }
    }
    if (rc)
        return rc;
    if (settle >= 0)
        settle_s = settle;
    pthread_mutex_lock(&tn_mu);
    tl_free(&cur);
    if (!tl_copy(&cur, v, m)) {
        pthread_mutex_unlock(&tn_mu);
        tn_free_list(v, m);
        return TSDB_ERR_NOMEM;
    }
    slots_make(&cur);
    atomic_store(&gen_now, 1);
    pthread_mutex_unlock(&tn_mu);
    *out = v;
    *n = m;
    return 0;
}

/* ------------------------------------------------------------ applying a new list */

static void now_mono(struct timespec *t) { clock_gettime(CLOCK_MONOTONIC, t); }

static double since(const struct timespec *t)
{
    struct timespec n;
    now_mono(&n);
    return (double)(n.tv_sec - t->tv_sec) + (double)(n.tv_nsec - t->tv_nsec) / 1e9;
}

/* A set from a source: nothing when it is the applied one, else the pending one, whose clock starts when the set
 * changes (a set that flaps is never applied). tn_mu held. */
static void offer_locked(char *const *v, size_t n)
{
    tlst t;
    if (have_latest)
        tl_free(&latest);
    have_latest = tl_copy(&latest, v, n);
    if (!tl_copy(&t, v, n))
        return; /* out of memory: the next offer tries again */
    if (tl_equal(&t, &cur)) {
        if (have_pending)
            tl_free(&pending);
        have_pending = false;
        tl_free(&t);
    } else if (!have_pending || !tl_equal(&t, &pending)) {
        if (have_pending)
            tl_free(&pending);
        pending = t;
        have_pending = true;
        now_mono(&pending_since);
    } else {
        tl_free(&t);
    }
}

void tn_offer(char *const *prefixes, size_t n)
{
    pthread_mutex_lock(&tn_mu);
    if (!file_mode)
        offer_locked(prefixes, n);
    pthread_mutex_unlock(&tn_mu);
}

void tn_hup(void) { atomic_store(&hup, true); }

void (*tn_test_after_set)(void); /* tests: runs between the engine taking a list and cur changing */

static void tick(void)
{
    if (atomic_exchange(&hup, false) && file_path) {
        char **v;
        size_t m;
        if (tn_read_file(file_path, &v, &m) == 0) {
            pthread_mutex_lock(&tn_mu);
            offer_locked(v, m);
            pthread_mutex_unlock(&tn_mu);
            tn_free_list(v, m);
        } else {
            fprintf(stderr, "tsdbd: tenants: the list in the server is unchanged (the file could not be read)\n");
        }
    }
    tlst take = {0};
    pthread_mutex_lock(&tn_mu);
    if (have_pending && since(&pending_since) >= settle_s) {
        take = pending;
        memset(&pending, 0, sizeof pending);
        have_pending = false;
    }
    pthread_mutex_unlock(&tn_mu);
    if (!take.s)
        return;
    int rc = tsdb_set_tenants(tdb, (const char *const *)take.s, take.n);
    if (tn_test_after_set)
        tn_test_after_set();
    if (rc) {
        fprintf(stderr, "tsdbd: tenants: the engine refused the list: %s\n", tsdb_strerror(rc));
        tl_free(&take);
        return;
    }
    pthread_mutex_lock(&tn_mu);
    tl_free(&cur);
    cur = take;
    slots_make(&cur);
    uint32_t g = atomic_fetch_add(&gen_now, 1) + 1;
    size_t nn = cur.n;
    /* An offer made while the engine took `take` was compared with the old cur: one that equalled it cleared
     * `pending`, though the source now says that. The newest offer is applied (after the settle time) if cur differs. */
    if (have_latest && !tl_equal(&latest, &cur)) {
        if (have_pending)
            tl_free(&pending);
        have_pending = tl_copy(&pending, latest.s, latest.n);
        if (have_pending)
            now_mono(&pending_since);
    }
    pthread_mutex_unlock(&tn_mu);
    fprintf(stderr, "tsdbd: tenants: %zu prefixes (generation %u)\n", nn, g);
}

static void *tn_main(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&tn_mu);
    while (!stop_flag) {
        struct timespec d;
        clock_gettime(CLOCK_REALTIME, &d);
        d.tv_nsec += 100000000;
        if (d.tv_nsec >= 1000000000)
            d.tv_sec++, d.tv_nsec -= 1000000000;
        pthread_cond_timedwait(&stop_cv, &tn_mu, &d);
        if (stop_flag)
            break;
        pthread_mutex_unlock(&tn_mu);
        tick();
        pthread_mutex_lock(&tn_mu);
    }
    pthread_mutex_unlock(&tn_mu);
    return NULL;
}

int tn_start(tsdb *db)
{
    tdb = db;
    stop_flag = false;
    if (pthread_create(&tn_thread, NULL, tn_main, NULL) != 0)
        return TSDB_ERR_NOMEM;
    tn_running = true;
    return 0;
}

void tn_stop(void)
{
    if (!tn_running)
        return;
    pthread_mutex_lock(&tn_mu);
    stop_flag = true;
    pthread_cond_signal(&stop_cv);
    pthread_mutex_unlock(&tn_mu);
    pthread_join(tn_thread, NULL);
    tn_running = false;
}
