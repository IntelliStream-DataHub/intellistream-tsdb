/* A tiny fork/join thread pool for intra-query parallelism. */
#include "internal.h"

#include <stdlib.h>

struct pool {
    pthread_mutex_t m;
    pthread_cond_t start_cv, done_cv;
    pthread_mutex_t run_lock; /* one parallel section at a time */
    pthread_t *th;
    int n;
    uint64_t gen;
    int active; /* workers 1..active-1 take part in the current section */
    int remaining;
    bool stop;
    void (*fn)(void *arg, int i);
    void *arg;
};

typedef struct worker_arg {
    pool *p;
    int idx;
} worker_arg;

static void *worker(void *a)
{
    worker_arg wa = *(worker_arg *)a;
    free(a);
    pool *p = wa.p;
    uint64_t seen = 0;
    pthread_mutex_lock(&p->m);
    for (;;) {
        while (p->gen == seen && !p->stop)
            pthread_cond_wait(&p->start_cv, &p->m);
        if (p->stop)
            break;
        seen = p->gen;
        if (wa.idx < p->active) {
            pthread_mutex_unlock(&p->m);
            p->fn(p->arg, wa.idx);
            pthread_mutex_lock(&p->m);
            if (--p->remaining == 0)
                pthread_cond_signal(&p->done_cv);
        }
    }
    pthread_mutex_unlock(&p->m);
    return NULL;
}

pool *pool_new(int nthreads)
{
    if (nthreads <= 1)
        return NULL;
    pool *p = calloc(1, sizeof *p);
    if (!p)
        return NULL;
    pthread_mutex_init(&p->m, NULL);
    pthread_mutex_init(&p->run_lock, NULL);
    pthread_cond_init(&p->start_cv, NULL);
    pthread_cond_init(&p->done_cv, NULL);
    p->th = calloc((size_t)nthreads, sizeof *p->th);
    if (!p->th) {
        free(p);
        return NULL;
    }
    /* worker indices start at 1; the caller runs index 0 */
    for (int i = 1; i < nthreads; i++) {
        worker_arg *wa = malloc(sizeof *wa);
        if (!wa)
            break;
        wa->p = p;
        wa->idx = i;
        if (pthread_create(&p->th[p->n], NULL, worker, wa) != 0) {
            free(wa);
            break;
        }
        p->n++;
    }
    return p;
}

void pool_free(pool *p)
{
    if (!p)
        return;
    pthread_mutex_lock(&p->m);
    p->stop = true;
    pthread_cond_broadcast(&p->start_cv);
    pthread_mutex_unlock(&p->m);
    for (int i = 0; i < p->n; i++)
        pthread_join(p->th[i], NULL);
    free(p->th);
    free(p);
}

int pool_run(pool *p, int n, void (*fn)(void *arg, int i), void *arg)
{
    if (!p || n <= 1 || pthread_mutex_trylock(&p->run_lock) != 0) {
        fn(arg, 0);
        return 1;
    }
    if (n > p->n + 1)
        n = p->n + 1;
    pthread_mutex_lock(&p->m);
    p->fn = fn;
    p->arg = arg;
    p->active = n;
    p->remaining = n - 1;
    p->gen++;
    pthread_cond_broadcast(&p->start_cv);
    pthread_mutex_unlock(&p->m);

    fn(arg, 0);

    pthread_mutex_lock(&p->m);
    while (p->remaining > 0)
        pthread_cond_wait(&p->done_cv, &p->m);
    pthread_mutex_unlock(&p->m);
    pthread_mutex_unlock(&p->run_lock);
    return n;
}
