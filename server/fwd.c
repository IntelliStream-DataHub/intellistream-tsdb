/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Forwarding links, the transport half (fwd.h has the design). Three tables under f->mu: the members and their
 * dialed links, the frames this node sent that are in flight (by frame id), and the query streams this node answers
 * (by connection and frame id, with their credit). The links' threads only look up, copy and enqueue; one
 * dispatcher thread delivers the inbox in order, one manager thread times out frames past their deadline.
 *
 * Order of events per frame: SENT is enqueued under f->mu, where the frame's state changes, so a TIMEOUT or LOST,
 * decided under the same lock, always follows it; a reply is read by the thread that wrote the frame, after its
 * SENT. Whoever takes a frame out of the table (reply, last chunk, LOST, TIMEOUT, NEVER_WRITTEN) is the only one to
 * tell of its end.
 *
 * The inbox never blocks a link's thread: an event always goes in, and a link whose event took its kind's inbox past
 * the bound is paused (plink_pause: it stops reading, TCP pushes back on the peer, keepalive goes on both ways) until
 * the dispatcher has drained the inbox to half the bound. The bound is soft by what the paused links had already read.
 *
 * Checks that are backstops (no test can reach them while the rest is right, so no test fails without them): a reply
 * from another connection's generation (on_frame; a connection's replies are read on its own thread, after its
 * frames' SENT), on_sent for another link (tokens are per instance), strm_find's gen (a stream goes with its link's
 * down, before the next connection's first frame is read).
 *
 * Lock order: f->mu > f->qmu; f->mu > a link's mutex and pl->mu (fwd_set_members dials under it; peerlink calls
 * back with neither held). plink_pause takes no lock (it may run under f->qmu).
 */
#include "fwd.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int fwd_mutant;

#define NB 1024 /* hash buckets of the frame and stream tables */
#define MS 1000000ull

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t le64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}

static unsigned hb(uint64_t id) { return (unsigned)((id * 0x9E3779B97F4A7C15ull) >> 54) & (NB - 1); }

/* ------------------------------------------------------------------ wire */

uint64_t fwd_id_check(uint64_t x)
{
    x ^= 0x6677645f69645f63ull; /* "fwd_id_c": the word of id 0 is not 0 */
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33;
    return x;
}

void fwd_wire_head(uint8_t out[FWD_HDR], uint64_t frame_id)
{
    put64(out, frame_id);
    put64(out + 8, fwd_id_check(frame_id));
}

int fwd_wire_decode(int kind, uint8_t type, const uint8_t *p, size_t n, struct fwd_wire *w)
{
    memset(w, 0, sizeof *w);
    if (n < FWD_HDR)
        return -1;
    w->frame_id = le64(p);
    if (fwd_mutant != FWD_MUT_NOIDCHECK && le64(p + 8) != fwd_id_check(w->frame_id))
        return FWD_E_IDCHECK;
    w->body = p + FWD_HDR;
    w->n = n - FWD_HDR;
    if (kind == PL_KIND_FWD)
        return type == FWD_T_REQ || type == FWD_T_REP ? 0 : -1;
    if (kind != PL_KIND_FWDQ)
        return -1;
    switch (type) {
    case FWDQ_T_REQ:
        return 0;
    case FWDQ_T_CHUNK:
        if (w->n < 1 || (p[FWD_HDR] & ~1u) || w->n - 1 > FWD_CHUNK_MAX)
            return -1;
        w->last = p[FWD_HDR] & 1;
        w->body = p + FWD_HDR + 1;
        w->n = n - FWD_HDR - 1;
        return 0;
    case FWDQ_T_CANCEL:
        if (n != FWD_HDR)
            return -1;
        w->body = NULL;
        return 0;
    case FWDQ_T_CREDIT:
        if (n != FWD_HDR + 4)
            return -1;
        w->chunks = (uint32_t)p[FWD_HDR] | (uint32_t)p[FWD_HDR + 1] << 8 | (uint32_t)p[FWD_HDR + 2] << 16 |
                    (uint32_t)p[FWD_HDR + 3] << 24;
        w->body = NULL;
        w->n = 0;
        return 0;
    default:
        return -1;
    }
}

size_t fwd_wire_credit(uint8_t out[FWD_CREDIT_LEN], uint64_t frame_id, uint32_t chunks)
{
    fwd_wire_head(out, frame_id);
    for (int i = 0; i < 4; i++)
        out[FWD_HDR + i] = (uint8_t)(chunks >> (8 * i));
    return FWD_CREDIT_LEN;
}

/* ------------------------------------------------------------------ state */

struct ent { /* a frame this node sent, in flight */
    struct ent *next;
    uint64_t frame_id, gen, deadline;
    uint64_t span; /* FWDQ: the caller's deadline span, armed again after each chunk while the owner holds credit */
    uint64_t owed; /* FWDQ: chunks the owner may still send (credit granted, not yet used) */
    uint32_t peer;
    int kind, written;
    struct plink *l;
};

struct strm { /* a query this node answers */
    struct strm *next;
    struct plink *l;
    uint64_t gen, frame_id, credit;
};

struct node {
    uint32_t id;
    char peer[128];
    struct plink *lk[2]; /* FWD, FWDQ */
};

struct evq {
    struct evq *next;
    struct fwd_ev ev;
    size_t cost; /* counted against the inbox of kind slot k (0: a control event, not counted) */
    int k;
    uint8_t data[];
};

struct fwd {
    struct fwd_cfg cfg;
    struct peerlink *pl;
    pthread_mutex_t mu;
    pthread_cond_t ccv; /* credit came, a stream went, or closing */
    pthread_cond_t mcv; /* the manager's: dl_min went down, or closing */
    struct ent *eh[NB];
    size_t nent;
    uint64_t dl_min; /* the earliest deadline of a written frame (0: none; may be early) */
    struct strm *sh[NB];
    struct node *nodes;
    uint32_t nn, ncap;

    pthread_mutex_t qmu;
    pthread_cond_t qcv, rcv; /* the dispatcher's; room in an inbox */
    struct evq *qh, *qt;
    size_t qbytes[2];
    struct plink **pz[2]; /* the links paused for the inbox of each kind slot */
    uint32_t npz[2], cpz[2];
    _Atomic int closing;
    pthread_t disp, mgr;
    int disp_ok, mgr_ok;
    struct fwd_stats st;
};

#define ST(f, x) __atomic_add_fetch(&(f)->st.x, 1, __ATOMIC_RELAXED)

static void cond_init_mono(pthread_cond_t *cv)
{
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(cv, &ca);
    pthread_condattr_destroy(&ca);
}

static struct timespec ts_at(uint64_t ns)
{
    return (struct timespec){(time_t)(ns / 1000000000ull), (long)(ns % 1000000000ull)};
}

static int kslot(int kind) { return kind == PL_KIND_FWDQ; }
static size_t inbox_cap(const struct fwd *f) { return f->cfg.inbox_bytes ? f->cfg.inbox_bytes : FWD_INBOX_BYTES; }
static uint64_t credit0(const struct fwd *f) { return f->cfg.credit ? f->cfg.credit : FWD_CREDIT_DEFAULT; }

/* f->mu held: a written frame's deadline is set or moved */
static void arm(struct fwd *f, uint64_t dl)
{
    if (dl && (!f->dl_min || dl < f->dl_min)) {
        f->dl_min = dl;
        pthread_cond_signal(&f->mcv);
    }
}
static int link_kind(struct plink *l) { return plink_kind(l) == PL_KIND_FWDQ ? PL_KIND_FWDQ : PL_KIND_FWD; }

/* ------------------------------------------------------------------ the inbox */

/* A control event (no payload): never waits, never counted. */
static void enq_ctl(struct fwd *f, const struct fwd_ev *ev)
{
    struct evq *e = calloc(1, sizeof *e);
    if (!e)
        abort();
    e->ev = *ev;
    pthread_mutex_lock(&f->qmu);
    if (f->qt)
        f->qt->next = e;
    else
        f->qh = e;
    f->qt = e;
    pthread_cond_signal(&f->qcv);
    pthread_mutex_unlock(&f->qmu);
}

/* f->qmu held: link l stops being read until its kind's inbox has drained (disp_main) */
static void pause_link(struct fwd *f, int k, struct plink *l)
{
    for (uint32_t i = 0; i < f->npz[k]; i++)
        if (f->pz[k][i] == l)
            return;
    if (f->npz[k] == f->cpz[k]) {
        uint32_t cap = f->cpz[k] ? f->cpz[k] * 2 : 8;
        struct plink **nz = realloc(f->pz[k], cap * sizeof *nz);
        if (!nz)
            abort();
        f->pz[k] = nz, f->cpz[k] = cap;
    }
    f->pz[k][f->npz[k]++] = l;
    plink_pause(l, 1);
}

/* An event with a body (copied), read on link l: always goes in (never waits: the link's thread must go on with its
 * keepalive), and past the inbox's bound pauses l. Dropped only when fwd_free has begun. */
static void enq_data(struct fwd *f, struct plink *l, const struct fwd_ev *ev, const uint8_t *p, size_t n)
{
    struct evq *e = malloc(sizeof *e + n);
    if (!e)
        abort();
    e->next = NULL;
    e->ev = *ev;
    if (n)
        memcpy(e->data, p, n);
    e->ev.p = e->data;
    e->ev.n = n;
    e->k = kslot(ev->kind);
    e->cost = n + sizeof *e;
    size_t cap = inbox_cap(f);
    pthread_mutex_lock(&f->qmu);
    if (fwd_mutant == FWD_MUT_INBOXWAIT) /* the planted bug: the link's thread waits for room, its keepalive stops */
        while (!atomic_load(&f->closing) && f->qbytes[e->k] && f->qbytes[e->k] + e->cost > cap)
            pthread_cond_wait(&f->rcv, &f->qmu);
    if (atomic_load(&f->closing) ||
        (fwd_mutant == FWD_MUT_INBOXDROP && f->qbytes[e->k] && f->qbytes[e->k] + e->cost > cap)) {
        pthread_mutex_unlock(&f->qmu);
        free(e);
        return;
    }
    f->qbytes[e->k] += e->cost;
    if (f->qt)
        f->qt->next = e;
    else
        f->qh = e;
    f->qt = e;
    if (f->qbytes[e->k] > cap && fwd_mutant != FWD_MUT_INBOXWAIT)
        pause_link(f, e->k, l);
    pthread_cond_signal(&f->qcv);
    pthread_mutex_unlock(&f->qmu);
}

static void *disp_main(void *arg)
{
    struct fwd *f = arg;
    pthread_mutex_lock(&f->qmu);
    for (;;) {
        while (!f->qh && !atomic_load(&f->closing))
            pthread_cond_wait(&f->qcv, &f->qmu);
        if (!f->qh || atomic_load(&f->closing))
            break; /* fwd_free has begun: nothing more is delivered */
        struct evq *e = f->qh;
        f->qh = e->next;
        if (!f->qh)
            f->qt = NULL;
        pthread_mutex_unlock(&f->qmu);
        if (f->cfg.on_event)
            f->cfg.on_event(f->cfg.arg, &e->ev);
        pthread_mutex_lock(&f->qmu);
        if (e->cost) {
            f->qbytes[e->k] -= e->cost;
            pthread_cond_broadcast(&f->rcv);
            if (f->npz[e->k] && f->qbytes[e->k] <= inbox_cap(f) / 2) { /* room again: the paused links read on */
                for (uint32_t i = 0; i < f->npz[e->k]; i++)
                    plink_pause(f->pz[e->k][i], 0);
                f->npz[e->k] = 0;
            }
        }
        free(e);
    }
    pthread_mutex_unlock(&f->qmu);
    return NULL;
}

/* ------------------------------------------------------------------ tables (f->mu held) */

static struct ent **ent_find(struct fwd *f, uint64_t id)
{
    struct ent **pp = &f->eh[hb(id)];
    while (*pp && (*pp)->frame_id != id)
        pp = &(*pp)->next;
    return pp;
}

static struct ent *ent_take(struct fwd *f, struct ent **pp)
{
    struct ent *e = *pp;
    *pp = e->next;
    f->nent--;
    return e;
}

static struct strm **strm_find(struct fwd *f, struct plink *l, uint64_t gen, uint64_t id)
{
    struct strm **pp = &f->sh[hb(id)];
    while (*pp && !((*pp)->frame_id == id && (*pp)->l == l && (*pp)->gen == gen))
        pp = &(*pp)->next;
    return pp;
}

static struct node *node_of(struct fwd *f, uint32_t id)
{
    for (uint32_t i = 0; i < f->nn; i++)
        if (f->nodes[i].id == id)
            return &f->nodes[i];
    return NULL;
}

static int is_dialed(struct fwd *f, struct plink *l)
{
    for (uint32_t i = 0; i < f->nn; i++)
        if (f->nodes[i].lk[0] == l || f->nodes[i].lk[1] == l)
            return 1;
    return 0;
}

/* ------------------------------------------------------------------ the links' callbacks */

static void on_link(void *arg, struct plink *l, int up)
{
    struct fwd *f = arg;
    uint64_t gen = plink_gen(l);
    uint32_t peer = plink_peer(l);
    pthread_mutex_lock(&f->mu);
    if (!up) {
        /* frames written on this connection and not answered: indeterminate; streams answered on it: gone */
        for (unsigned b = 0; b < NB; b++)
            for (struct ent **pp = &f->eh[b]; *pp;) {
                struct ent *e = *pp;
                if (e->l == l && e->gen == gen && e->written) {
                    ent_take(f, pp);
                    ST(f, lost);
                    enq_ctl(f, &(struct fwd_ev){.type = FWD_EV_LOST, .kind = e->kind, .peer = e->peer, .gen = gen,
                                                .frame_id = e->frame_id});
                    free(e);
                } else {
                    pp = &e->next;
                }
            }
        for (unsigned b = 0; b < NB; b++)
            for (struct strm **pp = &f->sh[b]; *pp;) {
                struct strm *s = *pp;
                if (s->l == l && s->gen == gen) {
                    *pp = s->next;
                    free(s);
                } else {
                    pp = &s->next;
                }
            }
        pthread_cond_broadcast(&f->ccv);
    }
    struct fwd_ev ev = {.type = FWD_EV_LINK, .kind = link_kind(l), .peer = peer, .gen = gen, .up = up,
                        .dialed = is_dialed(f, l)};
    enq_ctl(f, &ev);
    pthread_mutex_unlock(&f->mu);
}

static void on_sent(void *arg, struct plink *l, uint64_t token, uint64_t gen, int written)
{
    struct fwd *f = arg;
    pthread_mutex_lock(&f->mu);
    struct ent **pp = ent_find(f, token);
    struct ent *e = *pp;
    if (e && e->l == l && !e->written) {
        struct fwd_ev ev = {.type = FWD_EV_SENT, .kind = e->kind, .peer = e->peer, .gen = gen, .frame_id = token,
                            .outcome = written ? FWD_WRITTEN : FWD_NEVER_WRITTEN};
        if (written) {
            e->written = 1;
            e->gen = gen;
            if (e->kind == PL_KIND_FWDQ)
                e->owed = credit0(f);
            arm(f, e->deadline);
            ST(f, written);
        } else {
            free(ent_take(f, pp));
            ST(f, never_written);
        }
        enq_ctl(f, &ev);
    }
    pthread_mutex_unlock(&f->mu);
}

static void on_frame_in(struct fwd *f, struct plink *l, uint8_t type, const uint8_t *p, size_t n);

_Atomic uint64_t fwd_test_flip_reply;

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    struct fwd *f = arg;
    uint64_t x = atomic_load(&fwd_test_flip_reply);
    if (x && type != FWD_T_REQ && n >= FWD_HDR && atomic_compare_exchange_strong(&fwd_test_flip_reply, &x, 0)) {
        uint8_t *c = malloc(n); /* the test's flip, planted where a flipped bit of the wire would arrive */
        if (!c)
            abort();
        memcpy(c, p, n);
        for (int i = 0; i < 8; i++)
            c[i] ^= (uint8_t)(x >> (8 * i));
        on_frame_in(f, l, type, c, n);
        free(c);
        return;
    }
    on_frame_in(f, l, type, p, n);
}

static void on_frame_in(struct fwd *f, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    int kind = link_kind(l);
    struct fwd_wire w;
    int drc = fwd_wire_decode(kind, type, p, n, &w);
    if (drc == FWD_E_IDCHECK) { /* a flipped frame id (the link's frame check is off): no frame of it is trusted */
        ST(f, refused);
        plink_reset(l);
        return;
    }
    if (drc != 0) {
        ST(f, dropped); /* a peer of this version never sends it; nothing it could mean is lost by dropping it */
        return;
    }
    uint64_t gen = plink_gen(l);
    uint32_t peer = plink_peer(l);
    struct fwd_ev ev = {.kind = kind, .peer = peer, .gen = gen, .frame_id = w.frame_id};
    if (type == FWD_T_REQ) { /* == FWDQ_T_REQ */
        if (kind == PL_KIND_FWDQ) {
            struct strm *s = calloc(1, sizeof *s);
            if (!s)
                abort();
            s->l = l, s->gen = gen, s->frame_id = w.frame_id;
            s->credit = credit0(f);
            pthread_mutex_lock(&f->mu);
            struct strm **pp = strm_find(f, l, gen, w.frame_id);
            if (*pp) { /* the same frame again on one connection: one stream */
                (*pp)->credit = s->credit;
                free(s);
            } else {
                s->next = f->sh[hb(w.frame_id)];
                f->sh[hb(w.frame_id)] = s;
            }
            pthread_mutex_unlock(&f->mu);
        }
        ST(f, requests);
        ev.type = FWD_EV_REQUEST;
        ev.from = (struct fwd_from){l, gen, peer, kind};
        enq_data(f, l, &ev, w.body, w.n);
        return;
    }
    if (kind == PL_KIND_FWDQ && type == FWDQ_T_CANCEL) { /* the entry gave the query up: its stream goes */
        pthread_mutex_lock(&f->mu);
        struct strm **pp = strm_find(f, l, gen, w.frame_id);
        if (*pp) {
            struct strm *s = *pp;
            *pp = s->next;
            free(s);
            pthread_cond_broadcast(&f->ccv);
        }
        pthread_mutex_unlock(&f->mu);
        ST(f, cancels);
        return;
    }
    if (kind == PL_KIND_FWDQ && type == FWDQ_T_CREDIT) {
        pthread_mutex_lock(&f->mu);
        struct strm *s = *strm_find(f, l, gen, w.frame_id);
        if (s) {
            s->credit += w.chunks;
            pthread_cond_broadcast(&f->ccv);
        }
        pthread_mutex_unlock(&f->mu);
        ST(f, credits);
        return;
    }
    /* a reply (REP or CHUNK): only for a frame in flight that was written on this very connection */
    pthread_mutex_lock(&f->mu);
    struct ent **pp = ent_find(f, w.frame_id);
    struct ent *e = *pp;
    if (!e || e->l != l || e->gen != gen || !e->written || e->kind != kind) {
        pthread_mutex_unlock(&f->mu);
        ST(f, dropped);
        return;
    }
    int done = kind == PL_KIND_FWD || w.last;
    if (done) {
        free(ent_take(f, pp));
    } else {
        /* the stream is answering: while the owner holds credit it owes the next chunk within the span again; with
         * none (our reader is slow) nothing is owed and only fwd_cancel ends the frame */
        e->owed = e->owed ? e->owed - 1 : 0;
        e->deadline = e->span && e->owed ? now_ns() + e->span : 0;
        arm(f, e->deadline);
    }
    pthread_mutex_unlock(&f->mu);
    ev.type = kind == PL_KIND_FWD ? FWD_EV_REPLY : FWD_EV_CHUNK;
    ev.last = done;
    if (kind == PL_KIND_FWD)
        ST(f, replies);
    else
        ST(f, chunks);
    enq_data(f, l, &ev, w.body, w.n);
}

/* ------------------------------------------------------------------ the manager: deadlines */

static void *mgr_main(void *arg)
{
    struct fwd *f = arg;
    pthread_mutex_lock(&f->mu);
    while (!atomic_load(&f->closing)) {
        uint64_t now = now_ns();
        if (!f->dl_min) {
            pthread_cond_wait(&f->mcv, &f->mu);
            continue;
        }
        if (f->dl_min > now) {
            struct timespec ts = ts_at(f->dl_min);
            pthread_cond_timedwait(&f->mcv, &f->mu, &ts);
            continue;
        }
        {
            uint64_t min = 0;
            for (unsigned b = 0; b < NB; b++)
                for (struct ent **pp = &f->eh[b]; *pp;) {
                    struct ent *e = *pp;
                    if (e->written && e->deadline && e->deadline <= now) {
                        ent_take(f, pp);
                        ST(f, timeouts);
                        enq_ctl(f, &(struct fwd_ev){.type = FWD_EV_TIMEOUT, .kind = e->kind, .peer = e->peer,
                                                    .gen = e->gen, .frame_id = e->frame_id});
                        if (fwd_mutant == FWD_MUT_LATECLOSE)
                            plink_reset(e->l); /* the planted bug: a late reply ends the link */
                        free(e);
                        continue;
                    }
                    if (e->written && e->deadline && (!min || e->deadline < min))
                        min = e->deadline;
                    pp = &e->next;
                }
            f->dl_min = min;
        }
    }
    pthread_mutex_unlock(&f->mu);
    return NULL;
}

/* ------------------------------------------------------------------ API */

struct fwd *fwd_new(const struct fwd_cfg *cfg)
{
    struct fwd *f = calloc(1, sizeof *f);
    if (!f)
        return NULL;
    f->cfg = *cfg;
    f->cfg.secret = NULL; /* the peerlink keeps its copy */
    pthread_mutex_init(&f->mu, NULL);
    pthread_mutex_init(&f->qmu, NULL);
    cond_init_mono(&f->ccv);
    cond_init_mono(&f->mcv);
    pthread_cond_init(&f->qcv, NULL);
    pthread_cond_init(&f->rcv, NULL);
    struct pl_cfg pc = {.self = cfg->self, .secret = cfg->secret, .secret_len = cfg->secret_len, .arg = f,
                        .on_link = on_link, .on_frame = on_frame, .on_sent = on_sent,
                        .queue_bytes = cfg->queue_bytes, .ping_ms = cfg->ping_ms, .dead_ms = cfg->dead_ms,
                        .backoff_min_ms = cfg->backoff_min_ms, .backoff_max_ms = cfg->backoff_max_ms,
                        .hs_ms = cfg->hs_ms, .sock_buf = cfg->sock_buf};
    snprintf(pc.advertise, sizeof pc.advertise, "%s", cfg->advertise);
    if (pthread_create(&f->disp, NULL, disp_main, f) == 0)
        f->disp_ok = 1;
    if (pthread_create(&f->mgr, NULL, mgr_main, f) == 0)
        f->mgr_ok = 1;
    f->pl = f->disp_ok && f->mgr_ok ? peerlink_new(&pc) : NULL;
    if (!f->pl) {
        fwd_free(f);
        return NULL;
    }
    return f;
}

struct peerlink *fwd_peerlink(struct fwd *f) { return f->pl; }

void fwd_free(struct fwd *f)
{
    if (!f)
        return;
    pthread_mutex_lock(&f->mu);
    pthread_mutex_lock(&f->qmu);
    atomic_store(&f->closing, 1);
    pthread_cond_broadcast(&f->qcv);
    pthread_cond_broadcast(&f->rcv);
    pthread_mutex_unlock(&f->qmu);
    pthread_cond_broadcast(&f->ccv);
    pthread_cond_broadcast(&f->mcv);
    pthread_mutex_unlock(&f->mu);
    if (f->pl)
        peerlink_free(f->pl); /* its callbacks still find the tables; link threads waiting for room have left */
    if (f->mgr_ok)
        pthread_join(f->mgr, NULL);
    if (f->disp_ok)
        pthread_join(f->disp, NULL);
    for (struct evq *e = f->qh; e;) {
        struct evq *nx = e->next;
        free(e);
        e = nx;
    }
    for (unsigned b = 0; b < NB; b++) {
        for (struct ent *e = f->eh[b]; e;) {
            struct ent *nx = e->next;
            free(e);
            e = nx;
        }
        for (struct strm *s = f->sh[b]; s;) {
            struct strm *nx = s->next;
            free(s);
            s = nx;
        }
    }
    free(f->nodes);
    free(f->pz[0]);
    free(f->pz[1]);
    pthread_mutex_destroy(&f->mu);
    pthread_mutex_destroy(&f->qmu);
    pthread_cond_destroy(&f->ccv);
    pthread_cond_destroy(&f->mcv);
    pthread_cond_destroy(&f->qcv);
    pthread_cond_destroy(&f->rcv);
    free(f);
}

void fwd_set_members(struct fwd *f, const struct fwd_member *m, uint32_t n)
{
    pthread_mutex_lock(&f->mu);
    if (atomic_load(&f->closing)) {
        pthread_mutex_unlock(&f->mu);
        return;
    }
    uint8_t *seen = calloc((size_t)f->nn + n + 1, 1); /* each of the n may be a new node */
    if (!seen)
        abort();
    for (uint32_t i = 0; i < n; i++) {
        if (!m[i].id || m[i].id == f->cfg.self.id || !m[i].peer[0])
            continue;
        struct node *nd = node_of(f, m[i].id);
        if (nd) {
            seen[nd - f->nodes] = 1;
            for (int k = 0; k < 2; k++) /* a dial that failed (no thread, no pipe) is tried again */
                if (!nd->lk[k])
                    nd->lk[k] = peerlink_dial(f->pl, nd->id, m[i].peer, k ? PL_KIND_FWDQ : PL_KIND_FWD);
            if (strcmp(nd->peer, m[i].peer) != 0) { /* back on another peer port: dialed there */
                snprintf(nd->peer, sizeof nd->peer, "%s", m[i].peer);
                for (int k = 0; k < 2; k++)
                    if (nd->lk[k])
                        plink_set_addr(nd->lk[k], nd->peer);
            }
            continue;
        }
        if (f->nn == f->ncap) {
            uint32_t cap = f->ncap ? f->ncap * 2 : 8;
            struct node *nn = realloc(f->nodes, cap * sizeof *nn);
            if (!nn)
                abort();
            f->nodes = nn, f->ncap = cap;
        }
        nd = &f->nodes[f->nn];
        memset(nd, 0, sizeof *nd);
        nd->id = m[i].id;
        snprintf(nd->peer, sizeof nd->peer, "%s", m[i].peer);
        nd->lk[0] = peerlink_dial(f->pl, nd->id, nd->peer, PL_KIND_FWD);
        nd->lk[1] = peerlink_dial(f->pl, nd->id, nd->peer, PL_KIND_FWDQ);
        seen[f->nn] = 1;
        f->nn++;
    }
    /* members gone: their links stop (and stay valid: frames in flight on them end as LOST) */
    uint32_t w = 0;
    for (uint32_t i = 0; i < f->nn; i++) {
        if (seen[i]) {
            f->nodes[w++] = f->nodes[i];
            continue;
        }
        for (int k = 0; k < 2; k++)
            if (f->nodes[i].lk[k])
                plink_stop(f->nodes[i].lk[k]);
    }
    f->nn = w;
    free(seen);
    pthread_mutex_unlock(&f->mu);
}

int fwd_link_up(struct fwd *f, uint32_t peer, int kind)
{
    pthread_mutex_lock(&f->mu);
    struct node *nd = node_of(f, peer);
    struct plink *l = nd ? nd->lk[kslot(kind)] : NULL;
    pthread_mutex_unlock(&f->mu);
    return l && plink_up(l);
}

static int pl_err(int rc)
{
    switch (rc) {
    case PL_OK:
        return FWD_OK;
    case PL_E_FULL:
        return FWD_E_FULL;
    case PL_E_TOOBIG:
        return FWD_E_TOOBIG;
    case PL_E_CLOSED:
        return FWD_E_CLOSED;
    case PL_E_INVALID:
        return FWD_E_INVALID;
    default:
        return FWD_E_DOWN;
    }
}

int fwd_send(struct fwd *f, uint32_t peer, const struct fwd_msg *msg, uint64_t frame_id, uint64_t deadline_ns)
{
    if (!msg || (msg->kind != PL_KIND_FWD && msg->kind != PL_KIND_FWDQ) || (!msg->p && msg->n))
        return FWD_E_INVALID;
    if (msg->n > FWD_MSG_MAX)
        return FWD_E_TOOBIG;
    if (atomic_load(&f->closing))
        return FWD_E_CLOSED;
    struct ent *e = calloc(1, sizeof *e);
    if (!e)
        abort();
    e->frame_id = frame_id, e->peer = peer, e->kind = msg->kind, e->deadline = deadline_ns;
    if (deadline_ns && msg->kind == PL_KIND_FWDQ) {
        uint64_t now = now_ns();
        e->span = deadline_ns > now ? deadline_ns - now : 1;
    }
    pthread_mutex_lock(&f->mu);
    struct node *nd = node_of(f, peer);
    struct plink *l = nd ? nd->lk[kslot(msg->kind)] : NULL;
    if (!l || *ent_find(f, frame_id)) {
        pthread_mutex_unlock(&f->mu);
        free(e);
        return l ? FWD_E_INVALID : FWD_E_NOPEER;
    }
    e->l = l;
    e->next = f->eh[hb(frame_id)];
    f->eh[hb(frame_id)] = e;
    f->nent++;
    pthread_mutex_unlock(&f->mu);
    uint8_t h[FWD_HDR];
    fwd_wire_head(h, frame_id);
    int rc = plink_sendv(l, 0, msg->kind == PL_KIND_FWD ? FWD_T_REQ : FWDQ_T_REQ, h, sizeof h, msg->p, msg->n, 0, 1,
                         frame_id, deadline_ns);
    if (rc != PL_OK) { /* nothing was queued and on_sent never runs: the entry is ours to remove */
        pthread_mutex_lock(&f->mu);
        struct ent **pp = ent_find(f, frame_id);
        if (*pp == e)
            free(ent_take(f, pp));
        pthread_mutex_unlock(&f->mu);
        return pl_err(rc);
    }
    ST(f, sent);
    return FWD_OK;
}

int fwd_reply(struct fwd *f, const struct fwd_from *from, uint64_t frame_id, const void *p, size_t n)
{
    if (!from || !from->l || !from->gen || from->kind != PL_KIND_FWD || (!p && n))
        return FWD_E_INVALID;
    if (n > FWD_MSG_MAX)
        return FWD_E_TOOBIG;
    (void)f;
    uint8_t h[FWD_HDR];
    fwd_wire_head(h, frame_id);
    /* a reply waits a little for queue room: dropping it would make an answered request indeterminate */
    return pl_err(plink_sendv(from->l, from->gen, FWD_T_REP, h, sizeof h, p, n, 1000, 0, 0, 0));
}

void fwd_refuse(struct fwd *f, const struct fwd_from *from)
{
    if (!f || !from || !from->l || !from->gen)
        return;
    ST(f, refused);
    plink_reset_gen(from->l, from->gen);
}

int fwd_chunk(struct fwd *f, const struct fwd_from *from, uint64_t frame_id, const void *p, size_t n, int last,
              int timeout_ms)
{
    if (!from || !from->l || !from->gen || from->kind != PL_KIND_FWDQ || (!p && n))
        return FWD_E_INVALID;
    if (n > FWD_CHUNK_MAX)
        return FWD_E_TOOBIG;
    uint64_t dl = now_ns() + (uint64_t)(timeout_ms > 0 ? timeout_ms : 0) * MS;
    struct timespec ts = ts_at(dl);
    int took = 0;
    pthread_mutex_lock(&f->mu);
    for (;;) {
        if (atomic_load(&f->closing)) {
            pthread_mutex_unlock(&f->mu);
            return FWD_E_CLOSED;
        }
        struct strm **pp = strm_find(f, from->l, from->gen, frame_id);
        struct strm *s = *pp;
        if (!s) {
            pthread_mutex_unlock(&f->mu);
            return FWD_E_DOWN;
        }
        if (s->credit || fwd_mutant == FWD_MUT_NOCREDIT) {
            if (s->credit) {
                s->credit--;
                took = 1;
            }
            if (last && fwd_mutant == FWD_MUT_CHUNKFIRST) { /* the planted bug: the stream goes before the send */
                *pp = s->next;
                free(s);
            }
            break;
        }
        if (timeout_ms <= 0 || pthread_cond_timedwait(&f->ccv, &f->mu, &ts) == ETIMEDOUT) {
            /* the clock may have passed dl just as credit came: look once more */
            s = *strm_find(f, from->l, from->gen, frame_id);
            if (!s || !s->credit) {
                pthread_mutex_unlock(&f->mu);
                return s ? FWD_E_TIMEOUT : FWD_E_DOWN;
            }
        }
    }
    pthread_mutex_unlock(&f->mu);
    uint8_t h[FWD_HDR + 1];
    fwd_wire_head(h, frame_id);
    h[FWD_HDR] = last ? 1 : 0;
    uint64_t now = now_ns();
    int wait = dl > now ? (int)((dl - now) / MS) : 0;
    int rc = plink_sendv(from->l, from->gen, FWDQ_T_CHUNK, h, sizeof h, p, n, wait, 0, 0, 0);
    if (fwd_mutant == FWD_MUT_CHUNKFIRST)
        return pl_err(rc);
    /* accounted after the send: a chunk that did not go keeps its credit and, the last, its stream (a retry can) */
    pthread_mutex_lock(&f->mu);
    struct strm **pp = strm_find(f, from->l, from->gen, frame_id);
    if (*pp && rc == PL_OK && last) {
        struct strm *s = *pp;
        *pp = s->next;
        free(s);
    } else if (*pp && rc != PL_OK && took) {
        (*pp)->credit++;
        pthread_cond_broadcast(&f->ccv);
    }
    pthread_mutex_unlock(&f->mu);
    return pl_err(rc);
}

int fwd_credit(struct fwd *f, uint32_t peer, uint64_t frame_id, uint32_t chunks)
{
    pthread_mutex_lock(&f->mu);
    struct ent *e = *ent_find(f, frame_id);
    if (!e || e->peer != peer || e->kind != PL_KIND_FWDQ || !e->written) {
        pthread_mutex_unlock(&f->mu);
        return FWD_E_INVALID;
    }
    struct plink *l = e->l;
    uint64_t gen = e->gen;
    /* owed before the send: the owner may answer with a chunk before we come back */
    e->owed += chunks;
    if (e->span && !e->deadline && e->owed) {
        e->deadline = now_ns() + e->span;
        arm(f, e->deadline);
    }
    pthread_mutex_unlock(&f->mu);
    uint8_t b[FWD_CREDIT_LEN];
    fwd_wire_credit(b, frame_id, chunks);
    int rc = plink_sendv(l, gen, FWDQ_T_CREDIT, NULL, 0, b, sizeof b, 1000, 0, 0, 0);
    if (rc != PL_OK) { /* the owner never got it */
        pthread_mutex_lock(&f->mu);
        struct ent *e2 = *ent_find(f, frame_id);
        if (e2 == e) {
            e->owed = e->owed > chunks ? e->owed - chunks : 0;
            if (!e->owed)
                e->deadline = 0;
        }
        pthread_mutex_unlock(&f->mu);
    }
    return pl_err(rc);
}

int fwd_cancel(struct fwd *f, uint32_t peer, uint64_t frame_id)
{
    pthread_mutex_lock(&f->mu);
    struct ent **pp = ent_find(f, frame_id);
    struct ent *e = *pp;
    if (!e || e->peer != peer) {
        pthread_mutex_unlock(&f->mu);
        return FWD_E_INVALID;
    }
    ent_take(f, pp); /* from here no event of it is enqueued: every one looks it up under f->mu */
    pthread_mutex_unlock(&f->mu);
    int out = FWD_WRITTEN;
    if (!e->written && plink_unqueue(e->l, frame_id))
        out = FWD_NEVER_WRITTEN;
    if (out == FWD_WRITTEN && e->kind == PL_KIND_FWDQ) {
        /* best effort, no wait (a full queue: the owner's next fwd_chunk times out for credit); not yet reported
         * written, it is in the write buffer of the connection now up, and the CANCEL follows it there */
        uint8_t h[FWD_HDR];
        fwd_wire_head(h, frame_id);
        plink_sendv(e->l, e->written ? e->gen : 0, FWDQ_T_CANCEL, h, sizeof h, NULL, 0, 0, 0, 0, 0);
    }
    ST(f, canceled);
    free(e);
    return out;
}

void fwd_stats(struct fwd *f, struct fwd_stats *st)
{
    uint64_t *d = (uint64_t *)st, *s = (uint64_t *)&f->st;
    for (size_t i = 0; i < sizeof *st / sizeof(uint64_t); i++)
        d[i] = __atomic_load_n(&s[i], __ATOMIC_RELAXED);
}
