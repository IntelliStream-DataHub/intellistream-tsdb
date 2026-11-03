/* The query scheduler: one per database (tsdb handle), shared by its volumes.
 *
 * Work comes as fork/join jobs: fn(arg, i) runs on the calling thread (i == 0) and on helpers that join it, each
 * claiming items from the job's own counter until none are left. The caller always works, so a job finishes
 * without help; helpers only make it faster. `query_threads - 1` helper threads serve every job of the database.
 *
 * Groups. Every outermost query (qs_begin in volumes.c) or multi-series call is a group, with a cap on the
 * threads working for it (`request_threads`, or the call's own). Jobs opened inside it, also by helpers working
 * for it (granule decode inside a series task), belong to it through the thread-local cur_group, so one cap
 * covers a request's series and its decoding together. A group links itself into the scheduler only when it
 * opens a job: a query that never fans out takes no lock here.
 *
 * Picking. An idle helper takes the group with the fewest active threads below its cap (ties: the oldest), and
 * within it the newest open job with room: decode for series already started before new series.
 *
 * Load. Outermost groups count themselves in `live` (an atomic, no lock). A job asks for at most its share
 * of the threads, (helpers + 1) / live, its caller included: the callers of the other requests occupy CPUs too,
 * so the shares add up to the threads and no more. Under as many concurrent requests as there are CPUs, or
 * somewhat fewer (a server's requests spend part of their time outside a query), every request runs on its own
 * thread, as fast as it can (waking helpers that are all busy costs locks, wake-ups and caches for nothing), and
 * a lone request gets every helper.
 *
 * Waking. The opener wakes at most WAKE_FANOUT idle helpers and each helper that joins wakes WAKE_FANOUT more
 * while the job has room: a tree, so the opener starts its own items after two wake-ups, not after one per
 * helper. The signals go out after the scheduler's mutex is released.
 *
 * Rebalancing. Workers call sched_leave() after each item: a relaxed load of the group's cancel flag and of a
 * generation counter, which bumps only when a group opens a job that idle helpers cannot staff. On a change a
 * helper whose group holds more than its share, while another group wants help, returns and picks again. The
 * caller of a job never leaves, so every job finishes. The cancel flag is the engine's cancel point.
 *
 * No deadlock: a thread waits only for the helpers inside its own job, which wait only for their own nested
 * jobs (a fork/join tree). The scheduler's mutex is a leaf: it is never held while fn runs or an engine lock is
 * taken, and fn must never wait for anything but the items of its own jobs. */
#include "internal.h"

#include <stdlib.h>

typedef struct helper {
    pthread_t th;
    pthread_cond_t cv;
    bool wake;
    struct helper *next_idle;
    struct sched *s;
} helper;

struct sched {
    pthread_mutex_t m;
    helper *h;
    int nh;
    helper *idle; /* a stack: the most recently idle helper is the warmest */
    int nidle;
    sgroup *groups; /* groups with open or running jobs */
    int ngroups;
    uint64_t seq;
    atomic_uint_fast64_t gen;
    atomic_int live; /* outermost groups begun and not ended: requests in flight */
    int request_threads;
    bool stop;
};

__thread sgroup *cur_group;
static __thread sjob *cur_job;
static __thread bool cur_helper; /* a helper of cur_job (not its caller) */
static __thread bool cur_left;   /* sched_leave told this helper to go */
static __thread uint64_t seen_gen;
static __thread bool is_helper_thread;

/* ------------------------------------------------------------ scratch */

#define SCRATCH_SLOTS 4
typedef struct scratch {
    void *p[SCRATCH_SLOTS];
    size_t cap[SCRATCH_SLOTS];
} scratch;
static pthread_key_t scratch_key;
static pthread_once_t scratch_once = PTHREAD_ONCE_INIT;

static void scratch_free(void *a)
{
    scratch *sc = a;
    for (int i = 0; i < SCRATCH_SLOTS; i++)
        free(sc->p[i]);
    free(sc);
}

static void scratch_init(void) { pthread_key_create(&scratch_key, scratch_free); }

void *qscratch_get(int slot, size_t bytes)
{
    if (!is_helper_thread)
        return malloc(bytes ? bytes : 1);
    pthread_once(&scratch_once, scratch_init);
    scratch *sc = pthread_getspecific(scratch_key);
    if (!sc) {
        sc = calloc(1, sizeof *sc);
        if (!sc || pthread_setspecific(scratch_key, sc) != 0) {
            free(sc);
            return NULL;
        }
    }
    if (sc->cap[slot] < bytes) {
        void *p = realloc(sc->p[slot], bytes);
        if (!p)
            return NULL;
        sc->p[slot] = p;
        sc->cap[slot] = bytes;
    }
    return sc->p[slot];
}

void qscratch_put(void *p)
{
    if (!is_helper_thread)
        free(p);
}

/* ------------------------------------------------------------ groups */

void sched_group_begin(sched *s, sgroup *g, int cap)
{
    memset(g, 0, sizeof *g);
    g->s = s;
    g->cap = cap > 0 ? cap : s ? s->request_threads : 1;
    g->active = 1; /* the thread that began it */
    atomic_init(&g->cancel, 0);
    if (s && !cur_group) {
        atomic_fetch_add_explicit(&s->live, 1, memory_order_relaxed);
        g->counted = true;
    }
    g->outer = cur_group;
    cur_group = g;
}

void sched_group_end(sgroup *g)
{
    /* every job of the group was closed, and a group is linked only while it has jobs */
    if (g->counted)
        atomic_fetch_sub_explicit(&g->s->live, 1, memory_order_relaxed);
    cur_group = g->outer;
}

void sched_cancel(sgroup *g) { atomic_store(&g->cancel, 1); }

static void group_link(sched *s, sgroup *g)
{
    if (g->linked)
        return;
    g->linked = true;
    g->seq = ++s->seq;
    g->prev = NULL;
    g->next = s->groups;
    if (s->groups)
        s->groups->prev = g;
    s->groups = g;
    s->ngroups++;
}

static void group_unlink(sched *s, sgroup *g)
{
    if (!g->linked)
        return;
    g->linked = false;
    if (g->prev)
        g->prev->next = g->next;
    else
        s->groups = g->next;
    if (g->next)
        g->next->prev = g->prev;
    s->ngroups--;
}

/* ------------------------------------------------------------ picking */

static bool job_room(const sgroup *g, const sjob *j) { return j->open && j->helpers < j->want - 1 && g->active < g->cap; }

static sjob *group_job(const sgroup *g)
{
    if (g->active >= g->cap)
        return NULL;
    for (sjob *j = g->jobs; j; j = j->next)
        if (job_room(g, j))
            return j;
    return NULL;
}

static sjob *pick(sched *s)
{
    sjob *best = NULL;
    const sgroup *bg = NULL;
    for (sgroup *g = s->groups; g; g = g->next) {
        sjob *j = group_job(g);
        if (j && (!bg || g->active < bg->active || (g->active == bg->active && g->seq < bg->seq))) {
            best = j;
            bg = g;
        }
    }
    return best;
}

/* a group's fair share of the helpers when n groups want them */
static int share(const sched *s, int n) { return n ? (s->nh + n - 1) / n : s->nh; }

#define WAKE_FANOUT 2
static int wake(sched *s, sgroup *g, sjob *j, helper **w, bool opener);
static void signal_all(helper **w, int n);

static void *helper_main(void *a)
{
    helper *h = a;
    sched *s = h->s;
    is_helper_thread = true;
    /* Named for top -H and for tests that count the helpers (test_sched's test_volumes). */
    tsdb_thread_name("tsdb-query");
    pthread_mutex_lock(&s->m);
    for (;;) {
        if (s->stop)
            break;
        sjob *j = pick(s);
        if (!j) {
            h->next_idle = s->idle;
            s->idle = h;
            s->nidle++;
            h->wake = false;
            while (!h->wake && !s->stop)
                pthread_cond_wait(&h->cv, &s->m);
            continue; /* the waker took it off the idle stack */
        }
        sgroup *g = j->g;
        j->helpers++;
        g->active++;
        int id = ++j->ids;
        uint64_t opens = j->opens;
        helper *w[WAKE_FANOUT];
        int nw = job_room(g, j) ? wake(s, g, j, w, false) : 0;
        pthread_mutex_unlock(&s->m);
        signal_all(w, nw);

        cur_group = g;
        cur_job = j;
        cur_helper = true;
        cur_left = false;
        seen_gen = atomic_load_explicit(&s->gen, memory_order_relaxed);
        j->fn(j->arg, id);
        cur_group = NULL;
        cur_job = NULL;
        cur_helper = false;

        pthread_mutex_lock(&s->m);
        if (!cur_left) {
            g->active--;
            if (j->opens == opens)
                j->open = false; /* nothing left to claim until the owner reopens it */
        }
        if (--j->helpers == 0 && j->closing)
            pthread_cond_signal(&j->done);
    }
    pthread_mutex_unlock(&s->m);
    return NULL;
}

/* Takes up to WAKE_FANOUT idle helpers the group's open job can use off the idle stack into w (lock held); the
 * caller signals them with signal_all once it has released the lock. */
static int wake(sched *s, sgroup *g, sjob *j, helper **w, bool opener)
{
    int room = MIN(j->want - 1 - j->helpers, g->cap - g->active);
    int avail = MIN(room, s->nidle);
    int k = MIN(avail, WAKE_FANOUT);
    for (int i = 0; i < k; i++) {
        helper *h = s->idle;
        s->idle = h->next_idle;
        s->nidle--;
        h->wake = true;
        w[i] = h;
    }
    /* busy helpers rebalance only if this group is below its share and idle ones cannot fill its room (asked once,
     * by the opener: the helpers it wakes do not ask again) */
    if (opener && room > avail && g->active - 1 + avail < share(s, s->ngroups))
        atomic_fetch_add_explicit(&s->gen, 1, memory_order_relaxed);
    return k;
}

/* h->wake was set under the lock and the helper waits for it in a loop, so a signal after the unlock is not lost;
 * helpers live as long as the scheduler */
static void signal_all(helper **w, int n)
{
    for (int i = 0; i < n; i++)
        pthread_cond_signal(&w[i]->cv);
}

/* a job's participants under the current load: its share of the threads among the requests in flight */
static int fair_want(sched *s, int want)
{
    int live = atomic_load_explicit(&s->live, memory_order_relaxed);
    if (live > 1)
        want = MIN(want, MAX(1, (s->nh + 1) / live));
    return want;
}

/* ------------------------------------------------------------ jobs */

void sched_open(sched *s, sjob *j, int want, void (*fn)(void *arg, int i), void *arg)
{
    memset(j, 0, sizeof *j);
    j->s = s;
    j->fn = fn;
    j->arg = arg;
    j->want = want;
    j->saved_job = cur_job;
    j->saved_helper = cur_helper;
    if (!cur_group) {
        sched_group_begin(s, &j->own, 0);
        j->implicit = true;
    }
    j->g = cur_group;
    cur_job = j;
    cur_helper = false;
    if (s && s->nh)
        j->want = want = fair_want(s, want);
    if (!s || s->nh == 0 || want <= 1 || j->g->cap <= 1)
        return; /* nothing to share: the caller works alone */
    pthread_cond_init(&j->done, NULL);
    j->shared = true;
    helper *w[WAKE_FANOUT];
    pthread_mutex_lock(&s->m);
    sgroup *g = j->g;
    group_link(s, g);
    j->next = g->jobs;
    g->jobs = j;
    j->open = true;
    int nw = wake(s, g, j, w, true);
    pthread_mutex_unlock(&s->m);
    signal_all(w, nw);
}

void sched_reopen(sjob *j)
{
    if (!j->shared)
        return;
    sched *s = j->s;
    helper *w[WAKE_FANOUT];
    pthread_mutex_lock(&s->m);
    j->opens++;
    j->open = true;
    int nw = wake(s, j->g, j, w, true);
    pthread_mutex_unlock(&s->m);
    signal_all(w, nw);
}

void sched_close(sjob *j)
{
    if (j->shared) {
        sched *s = j->s;
        sgroup *g = j->g;
        pthread_mutex_lock(&s->m);
        j->open = false;
        j->closing = true;
        while (j->helpers > 0)
            pthread_cond_wait(&j->done, &s->m);
        sjob **pp = &g->jobs;
        while (*pp != j)
            pp = &(*pp)->next;
        *pp = j->next;
        if (!g->jobs)
            group_unlink(s, g);
        pthread_mutex_unlock(&s->m);
        pthread_cond_destroy(&j->done);
    }
    cur_job = j->saved_job;
    cur_helper = j->saved_helper;
    if (j->implicit)
        sched_group_end(&j->own);
}

int sched_run(sched *s, int n, void (*fn)(void *arg, int i), void *arg)
{
    if (s && s->nh && n > 1)
        n = fair_want(s, n);
    if (!s || n <= 1 || s->nh == 0) {
        /* this thread is the job's caller even on a helper: if sched_leave let it go, the items would be lost */
        bool helper = cur_helper;
        cur_helper = false;
        fn(arg, 0);
        cur_helper = helper;
        return 1;
    }
    sjob j;
    sched_open(s, &j, n, fn, arg);
    fn(arg, 0);
    sched_close(&j);
    return j.ids + 1; /* read after the close: a helper may join until then (and find nothing) */
}

int sched_leave(void)
{
    sgroup *g = cur_group;
    if (!g)
        return 0;
    if (atomic_load_explicit(&g->cancel, memory_order_relaxed))
        return TSDB_ERR_STOPPED;
    if (!cur_helper)
        return 0;
    sched *s = g->s;
    uint64_t gen = atomic_load_explicit(&s->gen, memory_order_relaxed);
    if (gen == seen_gen)
        return 0;
    seen_gen = gen;
    int leave = 0;
    pthread_mutex_lock(&s->m);
    int competing = 0; /* groups that hold helpers or want them */
    bool other = false;
    for (sgroup *x = s->groups; x; x = x->next)
        if (x->active > 1 || group_job(x))
            competing++;
    int sh = share(s, competing);
    for (sgroup *x = s->groups; x && !other; x = x->next)
        if (x != g && group_job(x) && x->active - 1 < sh)
            other = true;
    if (other && g->active - 1 > sh) {
        g->active--; /* now, so the helpers checking next see it */
        cur_left = true;
        leave = 1;
    }
    pthread_mutex_unlock(&s->m);
    return leave;
}

/* ------------------------------------------------------------ life */

sched *sched_new(int query_threads, int request_threads)
{
    sched *s = calloc(1, sizeof *s);
    if (!s)
        return NULL;
    pthread_mutex_init(&s->m, NULL);
    atomic_init(&s->gen, 0);
    atomic_init(&s->live, 0);
    if (query_threads < 1)
        query_threads = 1;
    s->request_threads = request_threads > 0 ? MIN(request_threads, query_threads) : query_threads;
    int want = query_threads - 1;
    s->h = want ? calloc((size_t)want, sizeof *s->h) : NULL;
    for (int i = 0; s->h && i < want; i++) {
        helper *h = &s->h[s->nh];
        h->s = s;
        pthread_cond_init(&h->cv, NULL);
        if (tsdb_thread_create(&h->th, helper_main, h) != 0) {
            pthread_cond_destroy(&h->cv);
            break;
        }
        s->nh++;
    }
    return s;
}

int sched_helpers(const sched *s) { return s ? s->nh : 0; }

int sched_request_threads(const sched *s) { return s ? s->request_threads : 1; }

void sched_free(sched *s)
{
    if (!s)
        return;
    pthread_mutex_lock(&s->m);
    s->stop = true;
    for (int i = 0; i < s->nh; i++)
        pthread_cond_signal(&s->h[i].cv);
    pthread_mutex_unlock(&s->m);
    for (int i = 0; i < s->nh; i++) {
        pthread_join(s->h[i].th, NULL);
        pthread_cond_destroy(&s->h[i].cv);
    }
    free(s->h);
    pthread_mutex_destroy(&s->m);
    free(s);
}
