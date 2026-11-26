/* SPDX-License-Identifier: AGPL-3.0-only */
/* Forwarding one hop, the request half: see fwdreq.h. */
#include "fwdreq.h"

#include "fwd.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void p16(uint8_t *p, uint16_t v) { memcpy(p, &v, 2); }
static void p32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void p64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }
static uint16_t g16(const uint8_t *p) { uint16_t v; memcpy(&v, p, 2); return v; }
static uint32_t g32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static uint64_t g64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

void fwdr_env_encode(uint8_t *o, const struct fwdr_env *e)
{
    p32(o, FWDR_MAGIC);
    p32(o + 4, e->epoch);
    p16(o + 8, e->hops);
    p16(o + 10, e->flags);
    p32(o + 12, e->origin);
    p32(o + 16, e->uid);
    p32(o + 20, e->proto);
    p32(o + FWDR_AGE_AT, e->age_ms);
    p16(o + 28, e->vol);
    p16(o + 30, 0);
    p64(o + 32, e->users_applied);
    p32(o + FWDR_ENV, e->id);
    o[FWDR_ENV + 4] = e->op;
}

int fwdr_env_decode(const uint8_t *p, size_t n, struct fwdr_env *e)
{
    if (n < FWDR_ENV + FWDR_INNER || g32(p) != FWDR_MAGIC)
        return -1;
    *e = (struct fwdr_env){.epoch = g32(p + 4), .hops = g16(p + 8), .flags = g16(p + 10), .origin = g32(p + 12),
                           .uid = g32(p + 16), .proto = g32(p + 20), .age_ms = g32(p + FWDR_AGE_AT),
                           .vol = g16(p + 28), .users_applied = g64(p + 32), .id = g32(p + FWDR_ENV),
                           .op = p[FWDR_ENV + 4], .payload = p + FWDR_ENV + FWDR_INNER,
                           .len = n - FWDR_ENV - FWDR_INNER};
    return e->hops == 0 ? -1 : 0;
}

void fwdr_tail_encode(uint8_t o[FWDR_TAIL], const struct fwdr_tail *t)
{
    const tsdb_query_stats *s = &t->st;
    uint64_t v[10] = {s->rows,       s->rows_decoded, s->rows_from_stats, s->memtable_rows,       s->bytes,
                      s->bytes_read, s->parts,        s->granules,        s->granules_from_stats, s->result_rows};
    p32(o, FWDR_TMAGIC);
    p32(o + 4, t->flags);
    memcpy(o + 8, &s->elapsed_s, 8);
    memcpy(o + 16, v, sizeof v);
}

int fwdr_tail_decode(const uint8_t *p, size_t n, struct fwdr_tail *t)
{
    if (n < FWDR_TAIL)
        return -1;
    const uint8_t *o = p + n - FWDR_TAIL;
    if (g32(o) != FWDR_TMAGIC)
        return -1;
    uint64_t v[10];
    memcpy(v, o + 16, sizeof v);
    t->flags = g32(o + 4);
    memcpy(&t->st.elapsed_s, o + 8, 8);
    t->st.rows = v[0], t->st.rows_decoded = v[1], t->st.rows_from_stats = v[2], t->st.memtable_rows = v[3];
    t->st.bytes = v[4], t->st.bytes_read = v[5], t->st.parts = v[6], t->st.granules = v[7];
    t->st.granules_from_stats = v[8], t->st.result_rows = v[9];
    return 0;
}

/* ---- frames in flight */

struct piece {
    struct piece *next;
    bool last;
    size_t n;
    uint8_t p[];
};

struct fwait_sync {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool init;
    bool key_set;
};

static __thread struct fwait_sync fw_tls;
static pthread_key_t fw_tls_key;
static bool fw_tls_key_ok;
static pthread_once_t fw_tls_once = PTHREAD_ONCE_INIT;

static void fwait_sync_destroy(struct fwait_sync *s)
{
    if (s && s->init) {
        pthread_cond_destroy(&s->cv);
        pthread_mutex_destroy(&s->mu);
        s->init = false;
        s->key_set = false;
    }
}

static void fwait_sync_dtor(void *p) { fwait_sync_destroy(p); }

static void fwait_sync_atexit(void)
{
    if (fw_tls.key_set)
        pthread_setspecific(fw_tls_key, NULL);
    fwait_sync_destroy(&fw_tls);
}

static void fwait_sync_init_once(void)
{
    fw_tls_key_ok = pthread_key_create(&fw_tls_key, fwait_sync_dtor) == 0;
    atexit(fwait_sync_atexit);
}

static struct fwait_sync *fwait_sync_get(void)
{
    if (!fw_tls.init) {
        pthread_mutex_init(&fw_tls.mu, NULL);
        pthread_condattr_t ca;
        pthread_condattr_init(&ca);
        pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
        pthread_cond_init(&fw_tls.cv, &ca);
        pthread_condattr_destroy(&ca);
        fw_tls.init = true;
        pthread_once(&fw_tls_once, fwait_sync_init_once);
        if (fw_tls_key_ok && pthread_setspecific(fw_tls_key, &fw_tls) == 0)
            fw_tls.key_set = true;
    }
    return &fw_tls;
}

struct fwait {
    struct fwait *next; /* in its bucket */
    uint64_t frame_id;
    uint32_t peer;
    struct fwait_sync *sync;
    int sent;     /* -1: no outcome yet, else FWD_NEVER_WRITTEN / FWD_WRITTEN */
    int end;      /* 0, FWD_EV_LOST or FWD_EV_TIMEOUT */
    bool nomem;   /* a piece could not be kept: the reply is incomplete (INDET) */
    struct piece *head, *tail;
};

#define NB 256
struct fwdr_tab {
    pthread_mutex_t mu; /* the buckets; a leaf above a waiter's mu (taken under it, never the other way) */
    struct fwait *b[NB];
    atomic_ullong dropped;
    void (*hook)(void *arg, const struct fwd_ev *ev, int point); /* tests only (fwdr_tab_test_hook) */
    void *hook_arg;
};

struct fwdr_tab *fwdr_tab_new(void)
{
    struct fwdr_tab *t = calloc(1, sizeof *t);
    if (t)
        pthread_mutex_init(&t->mu, NULL);
    return t;
}

void fwdr_tab_free(struct fwdr_tab *t)
{
    if (!t)
        return;
    pthread_mutex_destroy(&t->mu);
    free(t);
}

uint64_t fwdr_tab_dropped(struct fwdr_tab *t) { return t ? atomic_load(&t->dropped) : 0; }

void fwdr_tab_test_hook(struct fwdr_tab *t, void (*fn)(void *arg, const struct fwd_ev *ev, int point), void *arg)
{
    t->hook_arg = arg;
    t->hook = fn;
}

static void tab_add(struct fwdr_tab *t, struct fwait *w)
{
    pthread_mutex_lock(&t->mu);
    w->next = t->b[w->frame_id % NB];
    t->b[w->frame_id % NB] = w;
    pthread_mutex_unlock(&t->mu);
}

/* After this no event reaches w and none is still inside it: fwdr_on_event finds w under t->mu and takes w->mu before
 * it lets t->mu go, so once w is out of the table only an event that found it earlier can still hold w->mu (writing
 * the pieces, broadcasting the cv). Taking w->mu once more waits for that one; w lives on fwdr_run's stack and is
 * destroyed right after. The caller must not hold w->mu. */
static void tab_del(struct fwdr_tab *t, struct fwait *w, bool mut_teardown)
{
    pthread_mutex_lock(&t->mu);
    for (struct fwait **pp = &t->b[w->frame_id % NB]; *pp; pp = &(*pp)->next)
        if (*pp == w) {
            *pp = w->next;
            break;
        }
    pthread_mutex_unlock(&t->mu);
    if (!mut_teardown) { /* planted (test_fwd teardown): the old claim that t->mu alone is enough */
        pthread_mutex_lock(&w->sync->mu);
        pthread_mutex_unlock(&w->sync->mu);
    }
}

void fwdr_on_event(struct fwdr_tab *t, const struct fwd_ev *ev)
{
    if (!t || (ev->type != FWD_EV_SENT && ev->type != FWD_EV_REPLY && ev->type != FWD_EV_CHUNK &&
               ev->type != FWD_EV_LOST && ev->type != FWD_EV_TIMEOUT))
        return;
    struct piece *pc = NULL;
    if (ev->type == FWD_EV_REPLY || ev->type == FWD_EV_CHUNK) {
        pc = malloc(sizeof *pc + ev->n);
        if (pc) {
            pc->next = NULL;
            pc->last = ev->type == FWD_EV_REPLY || ev->last;
            pc->n = ev->n;
            if (ev->n)
                memcpy(pc->p, ev->p, ev->n);
        }
    }
    pthread_mutex_lock(&t->mu);
    struct fwait *w = t->b[ev->frame_id % NB];
    while (w && !(w->frame_id == ev->frame_id && w->peer == ev->peer))
        w = w->next;
    if (!w) {
        pthread_mutex_unlock(&t->mu);
        atomic_fetch_add(&t->dropped, 1);
        free(pc);
        return;
    }
    if (t->hook)
        t->hook(t->hook_arg, ev, 0); /* found w, t->mu held */
    struct fwait_sync *sync = w->sync;
    pthread_mutex_lock(&sync->mu);
    pthread_mutex_unlock(&t->mu);
    if (t->hook)
        t->hook(t->hook_arg, ev, 1); /* w->sync->mu held, t->mu not: w may be leaving the table now */
    bool wake = true;
    switch (ev->type) {
    case FWD_EV_SENT:
        w->sent = ev->outcome;
        /* FWD_WRITTEN does not complete the request: avoid waking the waiting worker until reply/chunk/loss/timeout
         * (its timedwait is capped at 1 s so client_gone still fires promptly). */
        if (ev->outcome == FWD_WRITTEN)
            wake = false;
        break;
    case FWD_EV_LOST:
    case FWD_EV_TIMEOUT:
        w->end = ev->type;
        break;
    default:
        if (w->sent < 0)
            w->sent = FWD_WRITTEN; /* a reply proves it was written */
        if (!pc) {
            w->nomem = true;
            break;
        }
        if (w->tail)
            w->tail->next = pc;
        else
            w->head = pc;
        w->tail = pc;
        pc = NULL;
    }
    if (wake)
        pthread_cond_signal(&sync->cv);
    pthread_mutex_unlock(&sync->mu);
    free(pc);
}

static void w_free(struct fwait *w)
{
    for (struct piece *p = w->head, *n; p; p = n) {
        n = p->next;
        free(p);
    }
}

static void abs_at(struct timespec *ts, int64_t now_ns, int64_t until_ns)
{
    clock_gettime(CLOCK_MONOTONIC, ts);
    int64_t d = until_ns - now_ns;
    if (d < 0)
        d = 0;
    int64_t ns = ts->tv_nsec + d % 1000000000;
    ts->tv_sec += d / 1000000000 + ns / 1000000000;
    ts->tv_nsec = ns % 1000000000;
}

static int fwdr_send_req(const struct fwdr_ops *o, const struct fwdr_req *q, uint64_t dl)
{
    if (o->sendv)
        return o->sendv(o->arg, q->peer, q->kind, q->msg, q->n, q->payload, q->payload_len, q->frame_id, dl);
    return o->send(o->arg, q->peer, q->kind, q->msg, q->n, q->frame_id, dl);
}

int fwdr_run(const struct fwdr_ops *o, struct fwdr_tab *t, const struct fwdr_req *q, struct fwdr_tail *tail,
             uint64_t *hop_ns)
{
    memset(tail, 0, sizeof *tail);
    *hop_ns = 0;
    int64_t t0 = o->now_ns(o->arg), send_by = t0 + (int64_t)q->wait_ms * 1000000;
    struct fwait_sync *sync = fwait_sync_get();
    struct fwait w = {.frame_id = q->frame_id, .peer = q->peer, .sync = sync, .sent = -1};
    uint32_t credit_batch = q->credit_batch ? q->credit_batch : 1;
    uint32_t uncredited = 0;
    int64_t sent_at = 0;
    int res = FWDR_INDET;
    for (int attempts = 0;; attempts++) {
        if (o->stall)
            o->stall(o->arg);
        int64_t now = o->now_ns(o->arg), age = now - q->arrival_ns;
        if (!q->mut_noage && age > (int64_t)q->max_age_ms * 1000000) {
            res = FWDR_OLD;
            goto out;
        }
        if (o->client_gone(o->arg)) {
            res = FWDR_GONE;
            goto out;
        }
        uint32_t age_ms = age > 0 ? (uint32_t)(age / 1000000) : 0;
        memcpy(q->msg + FWDR_AGE_AT, &age_ms, 4);
        tab_add(t, &w);
        uint64_t dl = q->reply_ms ? (uint64_t)now + (uint64_t)q->reply_ms * 1000000 : 0;
        int rc = fwdr_send_req(o, q, dl);
        if (rc == FWD_OK) {
            sent_at = now;
            break;
        }
        tab_del(t, &w, q->mut_teardown);
        if (rc == FWD_E_TOOBIG) {
            res = FWDR_TOOBIG;
            goto out;
        }
        if (o->now_ns(o->arg) >= send_by) {
            res = FWDR_UNREACH;
            goto out;
        }
        o->waits(o->arg, 1);
        o->sleep_ms(o->arg, 10);
        o->waits(o->arg, -1);
    }
    o->waits(o->arg, 1);
    pthread_mutex_lock(&sync->mu);
    for (;;) {
        if (w.head) {
            struct piece *pc = w.head;
            if (!(w.head = pc->next))
                w.tail = NULL;
            pthread_mutex_unlock(&sync->mu);
            size_t n = pc->n;
            bool ok = true, last = pc->last;
            if (last) {
                if (fwdr_tail_decode(pc->p, pc->n, tail) == 0)
                    n -= FWDR_TAIL;
                else
                    tail->flags = FWDR_T_CLOSE; /* no tail: something is wrong with the reply, close */
                *hop_ns = (uint64_t)(o->now_ns(o->arg) - sent_at);
            }
            o->waits(o->arg, -1); /* the client's own back-pressure (OUT_CAP) counts as waiting in relay */
            ok = n == 0 || o->relay(o->arg, pc->p, n);
            o->waits(o->arg, 1);
            free(pc);
            if (!ok) { /* the client's connection is going: the owner's stream goes too */
                o->cancel(o->arg, q->peer, q->frame_id);
                pthread_mutex_lock(&sync->mu);
                res = FWDR_INDET;
                break;
            }
            if (last) {
                pthread_mutex_lock(&sync->mu);
                res = FWDR_DONE;
                break;
            }
            if (q->kind == PL_KIND_FWDQ && ++uncredited >= credit_batch) {
                o->credit(o->arg, q->peer, q->frame_id, uncredited);
                uncredited = 0;
            }
            pthread_mutex_lock(&sync->mu);
            continue;
        }
        if (w.sent == FWD_NEVER_WRITTEN) { /* the connection ended with it unwritten: definite */
            res = FWDR_UNREACH;
            break;
        }
        if (w.end || w.nomem) { /* written, no (whole) reply: the owner may have applied it */
            res = q->mut_unavonbreak ? FWDR_UNREACH : FWDR_INDET;
            if (q->mut_retry && w.end) { /* planted: send it once more (D30 forbids) */
                w.end = 0;
                w.sent = -1;
                pthread_mutex_unlock(&sync->mu);
                int64_t now = o->now_ns(o->arg);
                uint64_t dl = q->reply_ms ? (uint64_t)now + (uint64_t)q->reply_ms * 1000000 : 0;
                int rc = fwdr_send_req(o, q, dl);
                pthread_mutex_lock(&sync->mu);
                if (rc == FWD_OK)
                    continue;
                res = FWDR_UNREACH;
            }
            break;
        }
        int64_t now = o->now_ns(o->arg);
        if (w.sent < 0 && now >= send_by) {
            pthread_mutex_unlock(&sync->mu);
            int oc = o->cancel(o->arg, q->peer, q->frame_id);
            pthread_mutex_lock(&sync->mu);
            if (oc == FWD_NEVER_WRITTEN) {
                res = FWDR_UNREACH;
                break;
            }
            if (oc == FWD_WRITTEN) {
                res = FWDR_INDET;
                break;
            }
            send_by = INT64_MAX; /* not in flight: its outcome is on the way to w */
            continue;
        }
        struct timespec at;
        int64_t tick_until = now + 1000000000;
        int64_t wait_until = (w.sent < 0 && send_by < tick_until) ? send_by : tick_until;
        abs_at(&at, now, wait_until);
        if (pthread_cond_timedwait(&sync->cv, &sync->mu, &at) == ETIMEDOUT && !w.head && w.sent == FWD_WRITTEN &&
            !w.end && o->client_gone(o->arg)) { /* nobody waits for the answer any more (its client, or tsdbd's shutdown) */
            pthread_mutex_unlock(&sync->mu);
            o->cancel(o->arg, q->peer, q->frame_id);
            pthread_mutex_lock(&sync->mu);
            res = FWDR_INDET;
            break;
        }
    }
    pthread_mutex_unlock(&sync->mu);
    o->waits(o->arg, -1);
    tab_del(t, &w, q->mut_teardown);
out:
    w_free(&w);
    return res;
}
