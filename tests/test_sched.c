/* The query scheduler (src/sched.c): fork/join jobs on one set of helpers per database, groups with caps,
 * rebalancing between groups, nesting, cancel. `make tsan` runs it under ThreadSanitizer. */
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

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
    atomic_int next, done, inside, max_inside, helper_items, stopped;
    int n, us;
    double t0, first_helper; /* when a helper first finished an item */
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
    int cap, n, us;
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
    run_arg a[2] = {{.s = s, .n = 400, .us = 200, .bar = &bar}, {.s = s, .n = 400, .us = 200, .bar = &bar}};
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
    run_arg a = {.s = s, .n = 300, .us = 100};
    run_thread(&a);
    CHECK(atomic_load(&a.j.done) == 300);
    CHECK(atomic_load(&a.j.max_inside) >= 2 && atomic_load(&a.j.max_inside) <= 3);
    run_arg b = {.s = s, .cap = 5, .n = 300, .us = 100}; /* the call's own cap */
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

/* A request that comes while another holds every helper gets one within about one item. */
static void test_newcomer(void)
{
    sched *s = sched_new(8, 0);
    run_arg big = {.s = s, .n = 4000, .us = 1000}; /* 4 s on one thread, 0.5 s on eight */
    pthread_t th;
    pthread_create(&th, NULL, run_thread, &big);
    while (atomic_load(&big.j.max_inside) < 8)
        spin_us(100);
    run_arg small = {.s = s, .n = 200, .us = 1000};
    double t0 = now_s();
    run_thread(&small);
    CHECK(atomic_load(&small.j.helper_items) > 0);
    double wait_ms = (small.j.first_helper - t0) * 1e3;
    fprintf(stderr, "  newcomer: first helper item after %.2f ms, %d of 200 items by helpers, %d threads\n",
            wait_ms, atomic_load(&small.j.helper_items), atomic_load(&small.j.max_inside));
    CHECK(wait_ms < 25);                             /* one item is 1 ms; slack for a loaded machine */
    CHECK(atomic_load(&small.j.max_inside) >= 3);    /* about half of the helpers */
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
    sched *s = sched_new(4, 0);
    sgroup g;
    sched_group_begin(s, &g, 0);
    cancel_arg c = {.s = s, .g = &g};
    job_init(&c.j, 100000, 50);
    sched_run(s, 4, cancel_fn, &c);
    sched_group_end(&g);
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

static int count_threads(void)
{
    DIR *d = opendir("/proc/self/task");
    int n = 0;
    for (struct dirent *e; d && (e = readdir(d));)
        n += e->d_name[0] != '.';
    if (d)
        closedir(d);
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
    int before = count_threads();
    o.query_threads = 1; /* the threads every volume has besides (WAL sync, ...) */
    OK(tsdb_open(dir, &o, &db));
    int base_threads = count_threads() - before;
    OK(tsdb_close(db));
    o.query_threads = 6;
    OK(tsdb_open(dir, &o, &db));
    int during = count_threads() - before;
    OK(tsdb_close(db));
    fprintf(stderr, "  3 volumes, query_threads 6: %d threads started, %d with query_threads 1\n", during, base_threads);
    CHECK(during - base_threads == 5);
    CHECK(system(cmd) == 0);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    test_two_jobs();
    test_caps();
    test_newcomer();
    test_nesting();
    test_tiny();
    test_cancel();
    test_reopen();
    test_volumes(base);
    printf("test_sched: ok\n");
    return 0;
}
