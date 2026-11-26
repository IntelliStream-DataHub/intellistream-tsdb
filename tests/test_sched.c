/* SPDX-License-Identifier: AGPL-3.0-only */
/* The query scheduler (src/sched.c): fork/join jobs on one set of helpers per database, groups with caps,
 * rebalancing between groups, nesting, cancel. `make tsan` runs it under ThreadSanitizer. */
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>
#include "watchdog.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "portable.h"

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            exit(1);                                                                 \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                     \
    do {                                                                                             \
        int rc_ = (expr);                                                                            \
        if (rc_ != 0) {                                                                              \
            fprintf(stderr, "%s:%d: %s = %d (%s)\n", __FILE__, __LINE__, #expr, rc_, tsdb_strerror(rc_)); \
            exit(1);                                                                                 \
        }                                                                                            \
    } while (0)

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

static void spin_us(int us)
{
    double end = now_s() + us * 1e-6;
    while (now_s() < end)
        ;
}

/* a job of n items, each spinning us microseconds; counts who did what */
typedef struct job {
    atomic_int next, done, inside, max_inside, helper_items, stopped, released;
    int n, us;
    int wait_inside; /* >0: a thread that enters waits (10 s at most) until this many are inside: "more than one
                      * thread worked" is then a fact of the run, not a race against the machine's load */
    double t0, first_helper; /* when a helper first finished an item */
    struct job *other;       /* with wait_inside: the job whose items left are counted when it is satisfied */
    int other_left;
    pthread_mutex_t m;
    int ids[64];
    int nids;
} job;

static void job_init(job *j, int n, int us)
{
    memset(j, 0, sizeof *j);
    j->n = n;
    j->us = us;
    j->t0 = now_s();
    pthread_mutex_init(&j->m, NULL);
}

static void job_fn(void *arg, int i)
{
    job *j = arg;
    int in = atomic_fetch_add(&j->inside, 1) + 1;
    for (int m = atomic_load(&j->max_inside); in > m && !atomic_compare_exchange_weak(&j->max_inside, &m, in);)
        ;
    if (j->wait_inside && !atomic_load(&j->released)) {
        double dl = now_s() + 10;
        while (atomic_load(&j->inside) < j->wait_inside && now_s() < dl)
            usleep(100);
        if (!atomic_exchange(&j->released, 1) && j->other)
            j->other_left = j->other->n - atomic_load(&j->other->next);
    }
    pthread_mutex_lock(&j->m);
    bool seen = false;
    for (int k = 0; k < j->nids; k++)
        seen |= j->ids[k] == i;
    if (!seen && j->nids < 64)
        j->ids[j->nids++] = i;
    pthread_mutex_unlock(&j->m);
    for (;;) {
        int k = atomic_fetch_add(&j->next, 1);
        if (k >= j->n)
            break;
        spin_us(j->us);
        atomic_fetch_add(&j->done, 1);
        if (i) {
            if (atomic_fetch_add(&j->helper_items, 1) == 0)
                j->first_helper = now_s();
        }
        int lv = sched_leave();
        if (lv < 0)
            atomic_store(&j->stopped, 1);
        if (lv)
            break;
    }
    atomic_fetch_sub(&j->inside, 1);
}

typedef struct run_arg {
    sched *s;
    int cap, n, us, wait;
    job *other; /* see job.other */
    job j;
    pthread_barrier_t *bar;
} run_arg;

static void *run_thread(void *a)
{
    run_arg *r = a;
    sgroup g;
    if (r->bar)
        pthread_barrier_wait(r->bar);
    sched_group_begin(r->s, &g, r->cap);
    job_init(&r->j, r->n, r->us);
    r->j.wait_inside = r->wait;
    r->j.other = r->other;
    sched_run(r->s, 32, job_fn, &r->j);
    sched_group_end(&g);
    return NULL;
}

/* Two requests at once both get more than one thread: the old pool ran the second alone. */
static void test_two_jobs(void)
{
    sched *s = sched_new(8, 0);
    CHECK(sched_helpers(s) == 7);
    pthread_barrier_t bar;
    pthread_barrier_init(&bar, NULL, 2);
    run_arg a[2] = {{.s = s, .n = 400, .us = 200, .bar = &bar, .wait = 2},
                    {.s = s, .n = 400, .us = 200, .bar = &bar, .wait = 2}};
    pthread_t th[2];
    for (int i = 0; i < 2; i++)
        pthread_create(&th[i], NULL, run_thread, &a[i]);
    for (int i = 0; i < 2; i++)
        pthread_join(th[i], NULL);
    for (int i = 0; i < 2; i++) {
        CHECK(atomic_load(&a[i].j.done) == 400);
        CHECK(atomic_load(&a[i].j.max_inside) > 1);
        CHECK(atomic_load(&a[i].j.max_inside) <= 8);
    }
    pthread_barrier_destroy(&bar);
    sched_free(s);
}

/* A group's cap bounds the threads working for it, the caller included. */
static void test_caps(void)
{
    sched *s = sched_new(16, 3);
    CHECK(sched_request_threads(s) == 3);
    run_arg a = {.s = s, .n = 300, .us = 100, .wait = 2};
    run_thread(&a);
    CHECK(atomic_load(&a.j.done) == 300);
    CHECK(atomic_load(&a.j.max_inside) >= 2 && atomic_load(&a.j.max_inside) <= 3);
    run_arg b = {.s = s, .cap = 5, .n = 300, .us = 100, .wait = 2}; /* the call's own cap */
    run_thread(&b);
    CHECK(atomic_load(&b.j.max_inside) >= 2 && atomic_load(&b.j.max_inside) <= 5);
    run_arg c = {.s = s, .cap = 1, .n = 50, .us = 10}; /* alone */
    run_thread(&c);
    CHECK(atomic_load(&c.j.max_inside) == 1 && c.j.nids == 1);
    sched_free(s);

    s = sched_new(1, 0); /* no helpers: everything on the caller */
    CHECK(sched_helpers(s) == 0);
    run_arg d = {.s = s, .n = 50, .us = 1};
    run_thread(&d);
    CHECK(atomic_load(&d.j.done) == 50 && d.j.nids == 1);
    sched_free(s);
}

/* As many requests in flight as there are threads, or 6 of 8 (a server's requests spend part of their time outside a
 * query): each runs on its own thread, waking no helper (they would only steal CPUs from the other requests). 4 of 8
 * share the threads, two each. One request alone gets every helper, through the wake tree. */
typedef struct load_arg {
    sched *s;
    pthread_barrier_t *in, *out;
    job j;
} load_arg;

static void *load_thread(void *a)
{
    load_arg *r = a;
    sgroup g;
    sched_group_begin(r->s, &g, 0);
    pthread_barrier_wait(r->in); /* every request is in flight before any job opens */
    job_init(&r->j, 64, 20);
    sched_run(r->s, 32, job_fn, &r->j);
    pthread_barrier_wait(r->out);
    sched_group_end(&g);
    return NULL;
}

static void load_requests(sched *s, int n, int share)
{
    pthread_barrier_t in, out;
    pthread_barrier_init(&in, NULL, (unsigned)n);
    pthread_barrier_init(&out, NULL, (unsigned)n);
    load_arg a[8];
    pthread_t th[8];
    for (int i = 0; i < n; i++) {
        a[i] = (load_arg){.s = s, .in = &in, .out = &out};
        pthread_create(&th[i], NULL, load_thread, &a[i]);
    }
    for (int i = 0; i < n; i++)
        pthread_join(th[i], NULL);
    for (int i = 0; i < n; i++) {
        CHECK(atomic_load(&a[i].j.done) == 64 && atomic_load(&a[i].j.max_inside) <= share);
        if (share == 1)
            CHECK(a[i].j.nids == 1);
    }
    pthread_barrier_destroy(&in);
    pthread_barrier_destroy(&out);
}

static void test_load(void)
{
    sched *s = sched_new(8, 0);
    load_requests(s, 8, 1);
    load_requests(s, 6, 1);
    load_requests(s, 4, 2);

    run_arg r = {.s = s, .n = 2000, .us = 100, .wait = 7}; /* the first item waits until the tree has staffed 7 */
    run_thread(&r);
    CHECK(atomic_load(&r.j.done) == 2000);
    CHECK(atomic_load(&r.j.max_inside) >= 7 && atomic_load(&r.j.max_inside) <= 8);
    sched_free(s);
}

/* A request that comes while another holds every helper gets one within about one item. */
static void test_newcomer(void)
{
    sched *s = sched_new(8, 0);
    run_arg big = {.s = s, .n = 4000, .us = 1000}; /* 4 s on one thread, 0.5 s on eight */
    pthread_t th;
    pthread_create(&th, NULL, run_thread, &big);
    while (atomic_load(&big.j.max_inside) < 8)
        spin_us(100);
    /* The newcomer's first item waits until three threads are inside it (about half of the helpers): they must come
     * while the big request still holds the pool, which the count of big's items left at that moment shows. The
     * old pool ran the second request alone and never got there (10 s, then the checks fail). No bound in ms: how
     * long that takes depends on the machine's load; what counts is that helpers left big's job for it. */
    run_arg small = {.s = s, .n = 200, .us = 1000, .wait = 3, .other = &big.j};
    double t0 = now_s();
    int big_left = -1;
    run_thread(&small);
    big_left = small.j.other_left;
    CHECK(atomic_load(&small.j.helper_items) > 0);
    fprintf(stderr, "  newcomer: first helper item after %.2f ms, %d of 200 items by helpers, %d threads, %d of 4000 items "
            "of the big request left when the third joined\n", (small.j.first_helper - t0) * 1e3,
            atomic_load(&small.j.helper_items), atomic_load(&small.j.max_inside), big_left);
    CHECK(atomic_load(&small.j.max_inside) >= 3);
    CHECK(big_left > 4000 / 10); /* helpers came from a pool that big still held, not from a pool big had finished */
    pthread_join(th, NULL);
    CHECK(atomic_load(&big.j.done) == 4000);
    sched_free(s);
}

/* 3-deep nesting in 64 groups at once: every leaf counted once. */
typedef struct nest {
    sched *s;
    int depth;
    atomic_int next;
    int n;
    atomic_long *leaves;
} nest;

static void nest_fn(void *arg, int i)
{
    (void)i;
    nest *x = arg;
    for (;;) {
        int k = atomic_fetch_add(&x->next, 1);
        if (k >= x->n)
            break;
        if (x->depth == 2) {
            atomic_fetch_add(x->leaves, 1);
        } else {
            nest y = {.s = x->s, .depth = x->depth + 1, .n = 4, .leaves = x->leaves};
            atomic_init(&y.next, 0);
            sched_run(x->s, 8, nest_fn, &y);
            CHECK(atomic_load(&y.next) >= 4);
        }
        if (sched_leave())
            break;
    }
}

typedef struct nest_arg {
    sched *s;
    atomic_long leaves;
} nest_arg;

static void *nest_thread(void *a)
{
    nest_arg *r = a;
    sgroup g;
    sched_group_begin(r->s, &g, 0);
    nest x = {.s = r->s, .depth = 0, .n = 8, .leaves = &r->leaves};
    atomic_init(&x.next, 0);
    sched_run(r->s, 8, nest_fn, &x);
    sched_group_end(&g);
    return NULL;
}

static void test_nesting(void)
{
    sched *s = sched_new(8, 4);
    for (int round = 0; round < 3; round++) {
        nest_arg a[64];
        pthread_t th[64];
        for (int i = 0; i < 64; i++) {
            a[i].s = s;
            atomic_init(&a[i].leaves, 0);
            pthread_create(&th[i], NULL, nest_thread, &a[i]);
        }
        for (int i = 0; i < 64; i++) {
            pthread_join(th[i], NULL);
            CHECK(atomic_load(&a[i].leaves) == 8 * 4 * 4);
        }
    }
    sched_free(s);
}

/* A helper that runs a job of its own inline (one thread asked, or its share under load is one) is that job's
 * caller and must finish it: when another request made the helper leave its outer job, the inner job's items
 * were dropped (a series aggregate in a multi call lost granules). */
typedef struct outer {
    sched *s;
    atomic_int next, inner_runs, short_runs;
    int n;
} outer;

static void outer_fn(void *arg, int i)
{
    (void)i;
    outer *o = arg;
    for (;;) {
        if (atomic_fetch_add(&o->next, 1) >= o->n)
            break;
        job in;
        job_init(&in, 20, 200);
        sched_run(o->s, 1, job_fn, &in); /* inline: this thread is its caller */
        atomic_fetch_add(&o->inner_runs, 1);
        if (atomic_load(&in.done) != in.n)
            atomic_fetch_add(&o->short_runs, 1);
        if (sched_leave())
            break;
    }
}

static void *outer_thread(void *a)
{
    outer *o = a;
    sgroup g;
    sched_group_begin(o->s, &g, 0);
    sched_run(o->s, 32, outer_fn, o);
    sched_group_end(&g);
    return NULL;
}

static void test_inline_inner(void)
{
    sched *s = sched_new(8, 0);
    outer o = {.s = s, .n = 400}; /* 400 items of 4 ms: 200 ms on eight threads */
    atomic_init(&o.next, 0);
    atomic_init(&o.inner_runs, 0);
    atomic_init(&o.short_runs, 0);
    pthread_t th;
    pthread_create(&th, NULL, outer_thread, &o);
    while (atomic_load(&o.inner_runs) < 16)
        spin_us(100);
    run_arg small = {.s = s, .n = 100, .us = 1000}; /* opens a job no idle helper can staff: helpers rebalance */
    run_thread(&small);
    pthread_join(th, NULL);
    fprintf(stderr, "  inline inner jobs: %d runs, %d cut short\n", atomic_load(&o.inner_runs),
            atomic_load(&o.short_runs));
    CHECK(atomic_load(&small.j.done) == 100);
    CHECK(atomic_load(&o.inner_runs) == 400);
    CHECK(atomic_load(&o.short_runs) == 0);
    sched_free(s);
}

/* 10^5 tiny jobs, from 4 threads at once, without a group (queries outside qs_begin: merges, tests). */
static void tiny_fn(void *arg, int i)
{
    (void)i;
    atomic_int *n = arg;
    atomic_fetch_add(n, 1);
}

typedef struct tiny_arg {
    sched *s;
    long jobs;
} tiny_arg;

static void *tiny_thread(void *a)
{
    tiny_arg *t = a;
    for (int k = 0; k < 25000; k++) {
        atomic_int n;
        atomic_init(&n, 0);
        int used = sched_run(t->s, 1 + k % 8, tiny_fn, &n);
        CHECK(used >= 1 && atomic_load(&n) == used);
        t->jobs++;
    }
    return NULL;
}

static void test_tiny(void)
{
    sched *s = sched_new(8, 0);
    tiny_arg a[4] = {{s, 0}, {s, 0}, {s, 0}, {s, 0}};
    pthread_t th[4];
    double t0 = now_s();
    for (int i = 0; i < 4; i++)
        pthread_create(&th[i], NULL, tiny_thread, &a[i]);
    for (int i = 0; i < 4; i++)
        pthread_join(th[i], NULL);
    fprintf(stderr, "  10^5 tiny jobs in %.0f ms\n", (now_s() - t0) * 1e3);
    CHECK(a[0].jobs + a[1].jobs + a[2].jobs + a[3].jobs == 100000);
    sched_free(s);
}

/* Cancel: every participant sees TSDB_ERR_STOPPED at its next item, and the job still closes. */
typedef struct cancel_arg {
    sched *s;
    sgroup *g;
    job j;
} cancel_arg;

static void cancel_fn(void *arg, int i)
{
    cancel_arg *c = arg;
    if (i == 0)
        sched_cancel(c->g); /* the caller cancels at once; helpers may have taken an item already */
    job_fn(&c->j, i);
}

static void test_cancel(void)
{
    CHECK(!sched_cancelled());
    sched *s = sched_new(4, 0);
    sgroup g;
    sched_group_begin(s, &g, 0);
    CHECK(!sched_cancelled());
    cancel_arg c = {.s = s, .g = &g};
    job_init(&c.j, 100000, 50);
    sched_run(s, 4, cancel_fn, &c);
    CHECK(sched_cancelled());
    sched_group_end(&g);
    CHECK(!sched_cancelled());
    CHECK(atomic_load(&c.j.stopped) == 1);
    CHECK(atomic_load(&c.j.done) < 100);
    sched_free(s);
}

/* reopen: a job whose items appear over time (the multi conductor's use) */
typedef struct feed {
    atomic_int avail, next, done;
    int n;
} feed;

static void feed_fn(void *arg, int i)
{
    (void)i;
    feed *f = arg;
    for (;;) {
        int k = atomic_load(&f->next);
        if (k >= atomic_load(&f->avail))
            return;
        if (!atomic_compare_exchange_weak(&f->next, &k, k + 1))
            continue;
        spin_us(20);
        atomic_fetch_add(&f->done, 1);
        if (sched_leave())
            return;
    }
}

static void test_reopen(void)
{
    sched *s = sched_new(4, 0);
    feed f;
    atomic_init(&f.avail, 0);
    atomic_init(&f.next, 0);
    atomic_init(&f.done, 0);
    sjob j;
    sched_open(s, &j, 4, feed_fn, &f);
    for (int k = 1; k <= 2000; k++) {
        atomic_store(&f.avail, k);
        sched_reopen(&j);
        if (k % 100 == 0)
            feed_fn(&f, 0); /* the owner works too */
    }
    while (atomic_load(&f.next) < 2000)
        feed_fn(&f, 0);
    sched_close(&j);
    CHECK(atomic_load(&f.done) == 2000);
    sched_free(s);
}

/* Threads of this process whose name starts with `name` (the engine names its threads tsdb-query, tsdb-flush, ...).
 * Counting every thread in /proc/self/task was flaky: pthread_join returns once the kernel clears the thread's tid,
 * before the task leaves /proc/self/task, and under load a joined thread stayed listed through a whole tsdb_open
 * (7 threads started and 3 for 8 and 3). So the test counts by name, waits for the threads of an earlier open to
 * leave, and waits for new ones to name themselves (a thread sets its name when it first runs). */
#ifdef __APPLE__ /* no /proc: the task's threads from mach, each name from its extended info */
#include <mach/mach.h>
static int count_threads(const char *name)
{
    thread_act_array_t list;
    mach_msg_type_number_t n = 0;
    if (task_threads(mach_task_self(), &list, &n) != KERN_SUCCESS)
        return 0;
    int found = 0;
    for (mach_msg_type_number_t i = 0; i < n; i++) {
        thread_extended_info_data_t info;
        mach_msg_type_number_t cnt = THREAD_EXTENDED_INFO_COUNT;
        if (thread_info(list[i], THREAD_EXTENDED_INFO, (thread_info_t)&info, &cnt) == KERN_SUCCESS)
            found += strncmp(info.pth_name, name, strlen(name)) == 0;
        mach_port_deallocate(mach_task_self(), list[i]);
    }
    vm_deallocate(mach_task_self(), (vm_address_t)list, n * sizeof *list);
    return found;
}
#else
static int count_threads(const char *name)
{
    DIR *d = opendir("/proc/self/task");
    int n = 0;
    for (struct dirent *e; d && (e = readdir(d));) {
        if (e->d_name[0] == '.')
            continue;
        char path[300], comm[32] = "";
        snprintf(path, sizeof path, "/proc/self/task/%s/comm", e->d_name);
        FILE *f = fopen(path, "r");
        if (!f)
            continue; /* gone since the readdir */
        if (fgets(comm, sizeof comm, f))
            n += strncmp(comm, name, strlen(name)) == 0;
        fclose(f);
    }
    if (d)
        closedir(d);
    return n;
}
#endif

/* Waits up to 10 s for the count of `name` threads to reach `want` from either side, and returns it. */
static int await_threads(const char *name, int want)
{
    int n = count_threads(name);
    for (int i = 0; n != want && i < 10000; i++) {
        usleep(1000);
        n = count_threads(name);
    }
    return n;
}

/* Volumes share the database's helpers: query_threads - 1 threads in all, not per volume. */
static void test_volumes(const char *base)
{
    char dir[512], v1[600], v2[600];
    snprintf(dir, sizeof dir, "%s/tsdb_test_sched.%d", base, (int)getpid());
    snprintf(v1, sizeof v1, "%s.v1", dir);
    snprintf(v2, sizeof v2, "%s.v2", dir);
    char cmd[2048];
    snprintf(cmd, sizeof cmd, "rm -rf %s %s %s", dir, v1, v2);
    CHECK(system(cmd) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.query_threads = 6;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t vi;
    OK(tsdb_volume_add(db, v1, &vi));
    OK(tsdb_volume_add(db, v2, &vi));
    OK(tsdb_close(db));
    CHECK(await_threads("tsdb-", 0) == 0); /* also the helpers of the tests before */
    o.query_threads = 1;                   /* no helpers; the flusher every volume has besides */
    OK(tsdb_open(dir, &o, &db));
    int flushers = await_threads("tsdb-flush", 3);
    int base_helpers = count_threads("tsdb-query");
    OK(tsdb_close(db));
    CHECK(await_threads("tsdb-", 0) == 0);
    o.query_threads = 6;
    OK(tsdb_open(dir, &o, &db));
    int helpers = await_threads("tsdb-query", 5);
    int during = await_threads("tsdb-", 8);
    OK(tsdb_close(db));
    fprintf(stderr, "  3 volumes, query_threads 6: %d helpers (%d threads), %d with query_threads 1 (%d flushers)\n",
            helpers, during, base_helpers, flushers);
    CHECK(flushers == 3 && base_helpers == 0);
    CHECK(helpers == 5 && during == 8);
    CHECK(system(cmd) == 0);
}

/* Calling threads reuse cached qscratch buffers (<= 2 MiB), free oversized buffers on put, handle nested
 * same-slot requests without aliasing, and release TLS buffers cleanly on thread exit (LSan). */
static void *scratch_thread(void *arg)
{
    int iters = *(int *)arg;
    void *first0 = NULL, *first2 = NULL;
    for (int i = 0; i < iters; i++) {
        void *a = qscratch_get(0, 64 * 1024);
        void *b = qscratch_get(1, 64 * 1024);
        void *c = qscratch_get(2, 448 * 1024);
        void *d = qscratch_get(3, 192 * 1024);
        CHECK(a && b && c && d);
        CHECK(a != b && a != c && a != d && b != c && b != d && c != d);
        if (i == 0) {
            first0 = a;
            first2 = c;
        } else {
            CHECK(a == first0);
            CHECK(c == first2);
        }
        /* defensive nested same-slot get must return a distinct buffer and not clobber the cached one */
        void *nested = qscratch_get(0, 4096);
        CHECK(nested && nested != a);
        memset(nested, 0x5a, 4096);
        qscratch_put(nested);
        memset(a, (int)(i & 0xff), 64 * 1024);
        memset(c, (int)((i + 1) & 0xff), 448 * 1024);
        qscratch_put(a);
        qscratch_put(b);
        qscratch_put(c);
        qscratch_put(d);
    }
    /* oversized (> 2 MiB) buffer is not cached; normal slot remains reusable afterwards */
    void *big = qscratch_get(2, (3u << 20));
    CHECK(big && big != first2);
    memset(big, 0x3c, 4096);
    qscratch_put(big);
    void *again = qscratch_get(2, 448 * 1024);
    CHECK(again == first2);
    qscratch_put(again);
    qscratch_put(NULL);
    return NULL;
}

static void test_qscratch(void)
{
    int iters = 16;
    /* exercise on main thread (atexit cleanup) and on secondary threads (pthread_key destructor cleanup) */
    scratch_thread(&iters);
    pthread_t th[8];
    for (int i = 0; i < 8; i++)
        CHECK(pthread_create(&th[i], NULL, scratch_thread, &iters) == 0);
    for (int i = 0; i < 8; i++)
        pthread_join(th[i], NULL);
}

int main(int argc, char **argv)
{
    th_watchdog("test_sched", 1200);
    const char *base = argc > 1 ? argv[1] : "/tmp";
    test_two_jobs();
    test_caps();
    test_load();
    test_newcomer();
    test_nesting();
    test_inline_inner();
    test_tiny();
    test_cancel();
    test_reopen();
    test_qscratch();
    test_volumes(base);
    printf("test_sched: ok\n");
    return 0;
}
