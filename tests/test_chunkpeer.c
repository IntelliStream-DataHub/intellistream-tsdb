/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cold ops of a cluster ec volume (server/chunkpeer.h) against six in-process chunk servers, one per stripe member,
 * through a fake transport (chunkpeer_local_call and _put, with members that are away or answer a put with another
 * sum): put, read whole, read around two members away, UNAVAILABLE (never CORRUPT) with three away, a put that a
 * member answers with another sum, fences, the refusal of deletes until MK_GC, the listing and owner, the epoch
 * provider and chunksrv's source for an unsure store (only from a caught-up copy, never the stores' own fence), and
 * readers on several threads while a put runs (TSan). Planted bugs: --mutant ownepoch|nosumcmp|staleraise|delbeforegc
 * must make it fail (the Makefile requires it).
 */
#ifndef TSDB_EC
#include <stdio.h>
int main(void)
{
    printf("test_chunkpeer: SKIPPED (this build has no erasure codec: it is x86-64 only)\n");
    return 0;
}
#else

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "chunkpeer.h"
#include "chunksrv.h"
#include "tsdb.h"

static int fails;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

#define NM 6
#define VOL 3
static const uint8_t CLUSTER[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
static const uint8_t VUUID[16] = {0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 7, 7, 7, 7, 7, 7, 7, 7};
static char S[PATH_MAX];

/* ---------------------------------------------------------------- the fake cluster */
struct fake {
    struct chunksrv *cs[NM + 1]; /* by member id 1..NM */
    atomic_uint down;            /* bit i: member i does not answer */
    atomic_uint liar;            /* bit i: member i answers a put with another sum */
    atomic_uint hang;            /* bit i: member i's link is up and nothing answers (until hang is cleared) */
    atomic_uint shortread;       /* bit i: member i answers a READ one byte short */
    atomic_uint ioerr;           /* bit i: member i answers every request with its store's IO error */
    atomic_uint garble;          /* bit i: member i's LIST pages say one entry fewer than they carry */
    atomic_uint flip_byte;       /* bit i: XOR 0xff into byte 0 of member i's READ replies (silent rot) */
    atomic_uint read_timeout;    /* bit i: after sleeping read_delay_ms[i], return TSDB_ERR_UNAVAILABLE on READ */
    atomic_int read_delay_ms[NM + 1];
    atomic_int put_delay_ms[NM + 1];
    atomic_uint read_delay_max[NM + 1];  /* 0: unlimited; >0: delay only the first max READs to node */
    atomic_uint read_delay_seen[NM + 1]; /* READs delayed so far on node */
    atomic_uint read_calls[NM + 1];      /* total CS_OP_READ calls received by node */
    atomic_int read_fail_stripe[NM + 1]; /* >=0: fail CS_OP_READ on this stripe index (cell_off / fail_cell) */
    atomic_int read_delay_stripe[NM + 1]; /* >=0: apply read_delay_ms only on this stripe index; <0: all stripes */
    atomic_uint fail_cell;               /* cell size used with read_fail_stripe / read_delay_stripe */
    void (*after_del_rel)(uint32_t node, const struct cs_req *r);
    pthread_cond_t hcv;
    struct chunkpeer_view view;
    pthread_mutex_t mu;
};
static struct fake F;

#ifdef TSDB_TEST_HOOKS
static _Atomic int64_t fake_clock_ms;
static int64_t f_now_ms(void) { return atomic_load(&fake_clock_ms); }
#endif

static uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static int f_call(void *arg, uint32_t node, const struct cs_req *r, int tmo, int32_t *rc, uint8_t **body, size_t *n)
{
    (void)arg, (void)tmo;
    *body = NULL;
    if (node == 0 || node > NM || (atomic_load(&F.down) >> node & 1))
        return TSDB_ERR_UNAVAILABLE;
    if (atomic_load(&F.hang) >> node & 1) { /* the deadline is the test's (CP.timeout_ms 600 s in test_hang) */
        pthread_mutex_lock(&F.mu);
        while (atomic_load(&F.hang) >> node & 1)
            pthread_cond_wait(&F.hcv, &F.mu);
        pthread_mutex_unlock(&F.mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    if (r->op == CS_OP_READ) {
        atomic_fetch_add(&F.read_calls[node], 1);
        int fs = atomic_load(&F.read_fail_stripe[node]);
        uint32_t fc = atomic_load(&F.fail_cell);
        if (fs >= 0 && fc > 0 && r->off / fc == (uint64_t)fs)
            return TSDB_ERR_UNAVAILABLE;
        int dms = atomic_load(&F.read_delay_ms[node]);
        int ds = atomic_load(&F.read_delay_stripe[node]);
        if (dms > 0 && (ds < 0 || (fc > 0 && r->off / fc == (uint64_t)ds))) {
            unsigned max = atomic_load(&F.read_delay_max[node]);
            bool do_delay = max == 0 || atomic_fetch_add(&F.read_delay_seen[node], 1) < max;
            if (do_delay) {
                struct timespec ts = {dms / 1000, (long)(dms % 1000) * 1000000L};
                nanosleep(&ts, NULL);
            }
        }
        if (atomic_load(&F.read_timeout) >> node & 1)
            return TSDB_ERR_UNAVAILABLE;
    }
    if (atomic_load(&F.ioerr) >> node & 1) {
        *rc = TSDB_ERR_IO;
        return 0;
    }
    int trc = chunkpeer_local_call(F.cs[node], 99, r, rc, body, n);
    if (trc == 0 && r->op == CS_OP_DEL_REL && F.after_del_rel)
        F.after_del_rel(node, r);
    if (trc == 0 && *rc == 0 && r->op == CS_OP_READ && *n > 0 && (atomic_load(&F.flip_byte) >> node & 1))
        (*body)[0] ^= 0xff;
    if (trc == 0 && *rc == 0 && r->op == CS_OP_READ && *n > 0 && (atomic_load(&F.shortread) >> node & 1))
        (*n)--;
    if (trc == 0 && *rc == 0 && r->op == CS_OP_LIST && *n >= 5 && (atomic_load(&F.garble) >> node & 1)) {
        uint32_t c = (uint32_t)(*body)[0] | (uint32_t)(*body)[1] << 8 | (uint32_t)(*body)[2] << 16 |
                     (uint32_t)(*body)[3] << 24;
        if (c > 0)
            c--; /* the page carries more than it says: as wrong as less */
        (*body)[0] = (uint8_t)c, (*body)[1] = (uint8_t)(c >> 8), (*body)[2] = (uint8_t)(c >> 16);
        (*body)[3] = (uint8_t)(c >> 24);
    }
    return trc;
}

static int f_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum, int tmo,
                 uint64_t *answered)
{
    (void)arg, (void)sum, (void)tmo;
    if (node == 0 || node > NM || (atomic_load(&F.down) >> node & 1))
        return TSDB_ERR_UNAVAILABLE;
    int pdms = atomic_load(&F.put_delay_ms[node]);
    if (pdms > 0) {
        struct timespec ts = {pdms / 1000, (long)(pdms % 1000) * 1000000L};
        nanosleep(&ts, NULL);
    }
    int rc = chunkpeer_local_put(F.cs[node], 99, m, fd, base, answered);
    if (rc == 0 && (atomic_load(&F.liar) >> node & 1))
        *answered ^= 1; /* a member of another version, or a bug there: what it stored is not what was sent */
    return rc;
}

static int f_view(void *arg, uint32_t volume, struct chunkpeer_view *v)
{
    (void)arg;
    if (volume != VOL)
        return TSDB_ERR_NOTFOUND;
    pthread_mutex_lock(&F.mu);
    *v = F.view;
    pthread_mutex_unlock(&F.mu);
    return 0;
}

static void set_view(int caught_up, uint64_t epoch)
{
    pthread_mutex_lock(&F.mu);
    F.view.caught_up = caught_up;
    F.view.epoch = epoch;
    pthread_mutex_unlock(&F.mu);
}

static struct chunkpeer CP;
static void *owner_ctx; /* the context lf asks the owner of a listed chunk */

static void fresh(const char *name, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%s", S, name);
    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", out);
    if (system(cmd) != 0 || mkdir(out, 0755) != 0) {
        fprintf(stderr, "cannot make %s\n", out);
        exit(1);
    }
}

static struct chunksrv *open_member(const char *dir, uint32_t node, bool init)
{
    char path[PATH_MAX + 32];
    snprintf(path, sizeof path, "%s/m%u", dir, node);
    if (init) {
        chunk_store_owner o = {.set = true, .node = node};
        memcpy(o.cluster, CLUSTER, 16);
        CHECK(chunk_store_init2(path, NULL, &o, NULL) == 0);
    }
    const char *pp[1] = {path};
    char err[256];
    struct chunksrv *cs = chunksrv_open(pp, 1, err, sizeof err);
    if (!cs)
        fprintf(stderr, "chunksrv_open %s: %s\n", path, err);
    CHECK(cs != NULL);
    return cs;
}

static void cluster_up(const char *name, char *dir, size_t cap)
{
    fresh(name, dir, cap);
    memset(&F.view, 0, sizeof F.view);
    for (uint32_t i = 1; i <= NM; i++)
        F.cs[i] = open_member(dir, i, true);
    atomic_store(&F.down, 0);
    atomic_store(&F.liar, 0);
    atomic_store(&F.shortread, 0);
    atomic_store(&F.ioerr, 0);
    atomic_store(&F.garble, 0);
    atomic_store(&F.flip_byte, 0);
    atomic_store(&F.read_timeout, 0);
    atomic_store(&F.fail_cell, 0);
    for (uint32_t i = 0; i <= NM; i++) {
        atomic_store(&F.read_delay_ms[i], 0);
        atomic_store(&F.put_delay_ms[i], 0);
        atomic_store(&F.read_delay_max[i], 0);
        atomic_store(&F.read_delay_seen[i], 0);
        atomic_store(&F.read_calls[i], 0);
        atomic_store(&F.read_fail_stripe[i], -1);
        atomic_store(&F.read_delay_stripe[i], -1);
    }
    chunkpeer_hedge_reset();
#ifdef TSDB_TEST_HOOKS
    chunkpeer_test_now_ms = NULL;
    chunkpeer_test_avoid_ms = 0;
#endif
    chunkpeer_test_list_max = CS_LIST_MAX;
    F.view = (struct chunkpeer_view){.caught_up = 1, .epoch = 5, .stripe_version = 1, .k = 4, .m = 2, .nstripe = NM};
    for (unsigned i = 0; i < NM; i++)
        F.view.stripe[i] = i + 1;
    memset(&CP, 0, sizeof CP);
    CP.net = (struct chunkpeer_net){.call = f_call, .put = f_put};
    CP.view = f_view;
    CP.timeout_ms = 5000;
    snprintf(CP.stage_dir, sizeof CP.stage_dir, "%s", dir);
}

static void cluster_down(void)
{
    chunkpeer_quiesce(); /* asks an open stopped waiting for still call the members */
    for (uint32_t i = 1; i <= NM; i++) {
        if (F.cs[i])
            chunksrv_close(F.cs[i]);
        F.cs[i] = NULL;
    }
}

/* ---------------------------------------------------------------- an object */
typedef struct obj {
    uint8_t oid[16];
    uint8_t *bytes;
    chunk_geom g;
    uint64_t *sums;
} obj;

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13, rng ^= rng >> 7, rng ^= rng << 17;
    return rng;
}

static void obj_make(obj *o, uint64_t len, uint8_t tag)
{
    memset(o, 0, sizeof *o);
    for (int i = 0; i < 16; i++)
        o->oid[i] = (uint8_t)(tag * 16 + i);
    o->bytes = malloc(len);
    for (uint64_t i = 0; i < len; i++)
        o->bytes[i] = (uint8_t)(i % 251 == 0 ? rnd() : i * 7 + tag);
    CHECK(chunk_geom_derive(&o->g, 4, 2, len) == 0);
    o->sums = calloc(chunk_sums_total(&o->g), sizeof *o->sums);
}

static void obj_free(obj *o)
{
    free(o->bytes);
    free(o->sums);
}

static int fill(void *ctx, uint64_t s, uint64_t off, uint8_t *buf, size_t len)
{
    (void)s;
    memcpy(buf, (const uint8_t *)ctx + off, len);
    return 0;
}

static int put(void *ctx, obj *o, uint64_t epoch)
{
    chunk_put_src src = {.ctx = o->bytes, .fill = fill};
    return coldops_peer.put_stream(ctx, o->oid, &o->g, epoch, &src, o->sums);
}

/* the whole object read back: 0 and equal, or the read's error (*rep its report) */
static int read_back(void *ctx, const obj *o, chunk_report *rep)
{
    cold_obj *h = NULL;
    int rc = coldops_peer.obj_open(ctx, o->oid, &o->g, o->sums, &h);
    if (rc)
        return rc;
    uint8_t *buf = malloc(o->g.len);
    memset(rep, 0, sizeof *rep);
    rc = coldops_peer.obj_read(h, 0, o->g.len, buf, 0, rep);
    if (rc == 0 && memcmp(buf, o->bytes, o->g.len) != 0)
        rc = -999;
    free(buf);
    coldops_peer.obj_close(h);
    return rc;
}

/* ---------------------------------------------------------------- cases */
static void *make_ctx(void)
{
    void *ctx = NULL;
    CHECK(chunkpeer_make(&CP, NULL, VOL, VUUID, &ctx) == 0);
    return ctx;
}

static void test_put_read(void)
{
    char dir[PATH_MAX];
    cluster_up("putread", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0 && coldops_peer.writable(ctx) && coldops_peer.epoch(ctx) == 5);
    CHECK(coldops_peer.all_up(ctx));
    CHECK(coldops_peer.stripe_version(ctx) == 1);
    obj o;
    obj_make(&o, 2u << 20, 1);
    CHECK(put(ctx, &o, 5) == 0);
    chunk_report rep;
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells == 0);
    /* geometry from the headers when the caller has none */
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, NULL, NULL, &h) == 0 && h);
    if (h) {
        const chunk_geom *g = coldops_peer.obj_geom(h);
        CHECK(g->k == o.g.k && g->m == o.g.m && g->cell == o.g.cell && g->len == o.g.len);
        uint8_t b[1000];
        CHECK(coldops_peer.obj_read(h, 123456, sizeof b, b, 0, &rep) == 0 && !memcmp(b, o.bytes + 123456, sizeof b));
        coldops_peer.obj_close(h);
    }
    /* two members away: decoded around; three: UNAVAILABLE, never CORRUPT (E15) */
    atomic_store(&F.down, 1u << 2 | 1u << 5);
    CHECK(!coldops_peer.all_up(ctx));
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells > 0 && rep.st[1] == CHUNK_ST_DOWN);
    atomic_store(&F.down, 1u << 1 | 1u << 2 | 1u << 5);
    int rc = read_back(ctx, &o, &rep);
    CHECK(rc == TSDB_ERR_UNAVAILABLE);
    atomic_store(&F.down, 0x7eu);
    CHECK(read_back(ctx, &o, &rep) == TSDB_ERR_UNAVAILABLE); /* every position away, with the record's geometry */
    uint8_t other[16] = {0xee};
    CHECK(coldops_peer.obj_open(ctx, other, NULL, NULL, &h) == TSDB_ERR_UNAVAILABLE);
    /* every member's store failing reads: UNAVAILABLE, never NOTFOUND (which eccache takes as lost, eckeep as
     * DIVERGED) */
    atomic_store(&F.down, 0);
    atomic_store(&F.ioerr, 0x7eu);
    CHECK(read_back(ctx, &o, &rep) == TSDB_ERR_UNAVAILABLE);
    CHECK(coldops_peer.obj_open(ctx, other, NULL, NULL, &h) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&F.ioerr, 0);
    /* a short READ answer is an error of that position, never the end: decoded around */
    atomic_store(&F.shortread, 1u << 2);
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells > 0 && rep.st[1] == CHUNK_ST_IOERR);
    atomic_store(&F.shortread, 0);
    /* a header of another geometry than the record's is another encoding of the object: no position is taken */
    {
        chunk_geom g2 = o.g;
        g2.len--;
        cold_obj *h2 = NULL;
        CHECK(coldops_peer.obj_open(ctx, o.oid, &g2, NULL, &h2) == TSDB_ERR_CORRUPT && !h2);
    }
    atomic_store(&F.down, 0);
    CHECK(coldops_peer.obj_open(ctx, other, NULL, NULL, &h) == TSDB_ERR_NOTFOUND);
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells == 0);
    /* a put while a member is away fails (no degraded settle) */
    obj q;
    obj_make(&q, 300000, 2);
    atomic_store(&F.down, 1u << 4);
    CHECK(put(ctx, &q, 5) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&F.down, 0);
    CHECK(put(ctx, &q, 5) == 0 && read_back(ctx, &q, &rep) == 0);
    /* an object of another m than the copy's stripe group is refused, though it has fewer positions */
    obj w;
    obj_make(&w, 300000, 4);
    CHECK(chunk_geom_derive(&w.g, 4, 1, 300000) == 0);
    CHECK(put(ctx, &w, 5) == TSDB_ERR_INVALID);
    obj_free(&w);
    obj_free(&q);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* A member that answers PUT_OK with another sum than the table sent: the put fails (E26). */
static void test_sum(void)
{
    char dir[PATH_MAX];
    cluster_up("sum", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 700000, 3);
    atomic_store(&F.liar, 1u << 3);
    CHECK(put(ctx, &o, 5) == TSDB_ERR_IO);
    atomic_store(&F.liar, 0);
    CHECK(put(ctx, &o, 5) == 0);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* Fences: a put below a member's fence is refused; deletes are refused until MK_GC whatever the epoch (D13). */
static void test_fence_del(void)
{
    char dir[PATH_MAX];
    cluster_up("fence", dir, sizeof dir);
    void *ctx = make_ctx();
    set_view(1, 7);
    CHECK(coldops_peer.see_epoch(ctx, 7) == 0 && coldops_peer.epoch(ctx) == 7);
    obj o;
    obj_make(&o, 500000, 4);
    CHECK(put(ctx, &o, 6) == CHUNK_ERR_FENCED);
    CHECK(put(ctx, &o, 7) == 0);
    for (unsigned pos = 0; pos < chunk_npos(&o.g); pos++)
        CHECK(coldops_peer.del(ctx, o.oid, pos, 7, COLD_DEL_GC) == TSDB_ERR_UNAVAILABLE);
    CHECK(coldops_peer.del(ctx, o.oid, 0, 7, COLD_DEL_SWEEP) == TSDB_ERR_UNAVAILABLE);
    chunk_report rep;
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells == 0); /* every position still there */
    /* the listing: every position once, and each one's header names the volume and writer epoch */
    struct lst {
        unsigned n;
        unsigned pos_seen;
    } l = {0};
    int lf(void *c, const chunk_listed *e);
    owner_ctx = ctx;
    CHECK(coldops_peer.list_names(ctx, lf, &l) == 0);
    CHECK(l.n == chunk_npos(&o.g) && l.pos_seen == (1u << chunk_npos(&o.g)) - 1);
    /* r8k-gc: nstripe, gc_suspended, and del_gc (requires mk_pos > 0 and valid epoch) */
    CHECK(coldops_peer.nstripe(ctx) == 6);
    CHECK(!coldops_peer.gc_suspended(ctx));
    pthread_mutex_lock(&F.mu);
    F.view.gc_suspended = 1;
    pthread_mutex_unlock(&F.mu);
    CHECK(coldops_peer.gc_suspended(ctx));
    pthread_mutex_lock(&F.mu);
    F.view.gc_suspended = 0;
    pthread_mutex_unlock(&F.mu);
    CHECK(coldops_peer.del_gc(ctx, o.oid, 0, 7, 0) == TSDB_ERR_INVALID);
    CHECK(coldops_peer.del_gc(ctx, o.oid, 0, 6, 42) == CHUNK_ERR_FENCED);
    for (unsigned pos = 0; pos < 6; pos++) {
        int drc = coldops_peer.del_gc(ctx, o.oid, pos, 7, 42);
        CHECK(drc == 0 || drc == TSDB_ERR_NOTFOUND);
    }
    CHECK(coldops_peer.del_gc(ctx, o.oid, 0, 7, 42) == TSDB_ERR_NOTFOUND);
    CHECK(coldops_peer.obj_open(ctx, o.oid, NULL, NULL, &(cold_obj *){NULL}) == TSDB_ERR_NOTFOUND);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

int lf(void *c, const chunk_listed *e)
{
    struct {
        unsigned n, pos_seen;
    } *l = c;
    l->n++;
    l->pos_seen |= 1u << e->pos;
    CHECK(e->epoch == 7 && memcmp(e->vuuid, VUUID, 16) == 0 && e->status == CHUNK_ST_OK);
    uint8_t v[16] = {0};
    int rc = coldops_peer.owner(owner_ctx, e, v);
    if (rc != 0 || memcmp(v, VUUID, 16))
        fprintf(stderr, "owner of position %u on member %d: %d\n", e->pos, e->store, rc);
    CHECK(rc == 0 && !memcmp(v, VUUID, 16));
    return 0;
}

/* D9: an unsure store (one EPOCH slot damaged) is raised only from a caught-up copy's epoch, never from a put's and
 * never to the store's own surviving fence; the tier's provider is the copy's epoch. */
static void test_unsure(void)
{
    char dir[PATH_MAX];
    cluster_up("unsure", dir, sizeof dir);
    void *ctx = make_ctx();
    set_view(1, 3);
    CHECK(coldops_peer.see_epoch(ctx, 3) == 0);
    set_view(1, 4);
    CHECK(coldops_peer.see_epoch(ctx, 4) == 0); /* both slots of every member's EPOCH written */
    coldops_peer.ctx_free(ctx);
    chunksrv_close(F.cs[1]);
    char ep[PATH_MAX + 64];
    snprintf(ep, sizeof ep, "%s/m1/s%u/EPOCH", dir, VOL);
    int fd = open(ep, O_RDWR);
    CHECK(fd >= 0);
    uint8_t junk[16];
    memset(junk, 0x5a, sizeof junk);
    CHECK(pwrite(fd, junk, sizeof junk, 4096) == (ssize_t)sizeof junk);
    close(fd);
    F.cs[1] = open_member(dir, 1, false);
    uint64_t own = chunk_be_epoch(chunksrv_backend(F.cs[1]), VOL);
    CHECK(own == 3 || own == 4);
    CP.local = F.cs[1];
    chunksrv_epoch_source(F.cs[1], chunkpeer_epoch_source, &CP);
    /* the copy holds 8 but is not caught up: nothing raises the store, and a put at its own old epoch is refused */
    set_view(0, 8);
    CHECK(chunkpeer_epoch(&CP, VOL) == 8);
    uint64_t e;
    CHECK(chunkpeer_epoch_source(&CP, VOL, &e) == 0);
    void *c3 = make_ctx(); /* an unsure member does not hold the epoch it was asked: no put (writable false) */
    CHECK(coldops_peer.see_epoch(c3, 8) == 0 && !coldops_peer.writable(c3));
    coldops_peer.ctx_free(c3);
    set_view(1, 0); /* a copy that holds no epoch for the volume is no source */
    CHECK(chunkpeer_epoch_source(&CP, VOL, &e) == 0);
    set_view(0, 8);
    ctx = make_ctx();
    /* an unsure member answers: all UP (eccold's stripe_ok would refuse every settle before its put otherwise) */
    CHECK(coldops_peer.all_up(ctx));
    obj o;
    obj_make(&o, 400000, 5);
    CHECK(put(ctx, &o, own) == CHUNK_ERR_EPOCH_UNSURE);
    CHECK(put(ctx, &o, 4) == CHUNK_ERR_EPOCH_UNSURE);
    /* caught up: raised to the copy's 8 at the next request; an older epoch is FENCED, 8 is taken */
    set_view(1, 8);
    CHECK(chunkpeer_epoch_source(&CP, VOL, &e) == 1 && e == 8);
    CHECK(put(ctx, &o, 4) == CHUNK_ERR_FENCED);
    CHECK(chunk_be_epoch(chunksrv_backend(F.cs[1]), VOL) == 8);
    CHECK(put(ctx, &o, 8) == 0);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    CP.local = NULL;
    cluster_down();
}

/* writable once every member holds the copy's epoch again: asked at most once a second, so waited for */
static bool wait_writable(void *ctx)
{
    for (int i = 0; i < 1000; i++) { /* 20 s: the asks are once a second */
        if (coldops_peer.writable(ctx))
            return true;
        struct timespec ts = {0, 20 * 1000000L};
        nanosleep(&ts, NULL);
    }
    return false;
}

/* see_epoch with a member away (a promotion after the old leader's node died): no put until every member holds the
 * epoch, and puts again once it is back (F1: a promoted leader must settle again without being reopened). With every
 * member away the attach still succeeds (a cluster's cold start opens its volumes before the chunk links are up). */
static void test_unfenced(void)
{
    char dir[PATH_MAX];
    cluster_up("unfenced", dir, sizeof dir);
    void *ctx = make_ctx();
    atomic_store(&F.down, 1u << 6);
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0 && !coldops_peer.writable(ctx));
    obj o;
    obj_make(&o, 100000, 6);
    CHECK(put(ctx, &o, 5) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&F.down, 0);
    CHECK(wait_writable(ctx));
    CHECK(put(ctx, &o, 5) == 0);
    atomic_store(&F.down, 0x7eu);
    void *c2 = make_ctx();
    CHECK(coldops_peer.see_epoch(c2, 5) == 0 && !coldops_peer.writable(c2));
    atomic_store(&F.down, 0);
    set_view(1, 6); /* a new epoch meanwhile: the members are asked at the copy's */
    CHECK(wait_writable(c2));
    CHECK(chunk_be_epoch(chunksrv_backend(F.cs[1]), VOL) == 6);
    obj_free(&o);
    coldops_peer.ctx_free(c2);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* A member that hangs (its link up, no answer) while the deadline is far: an open with k' positions answers at once,
 * decoding around it, never waiting for it (F5: one paused member made every cold read wait 30 s per position). */
static void test_hang(void)
{
    char dir[PATH_MAX];
    cluster_up("hang", dir, sizeof dir);
    CP.timeout_ms = 600000;
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 1u << 20, 7);
    CHECK(put(ctx, &o, 5) == 0);
    atomic_store(&F.hang, 1u << 2);
    chunk_report rep;
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells > 0 && rep.st[1] == CHUNK_ST_DOWN);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, NULL, NULL, &h) == 0 && h);
    coldops_peer.obj_close(h);
    pthread_mutex_lock(&F.mu);
    atomic_store(&F.hang, 0);
    pthread_cond_broadcast(&F.hcv);
    pthread_mutex_unlock(&F.mu);
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells == 0);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* list_names reads every page of every member, and a garbled page is an error, never the end */
static long listed;
static int count_fn(void *c, const chunk_listed *e)
{
    (void)c, (void)e;
    listed++;
    return 0;
}

static void test_list(void)
{
    char dir[PATH_MAX];
    cluster_up("list", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    long want = 0;
    for (uint8_t t = 1; t <= 7; t++) {
        obj o;
        obj_make(&o, 300000 + 1000 * t, (uint8_t)(t + 8));
        CHECK(put(ctx, &o, 5) == 0);
        want += chunk_npos(&o.g);
        obj_free(&o);
    }
    chunkpeer_test_list_max = 3;
    listed = 0;
    CHECK(coldops_peer.list_names(ctx, count_fn, NULL) == 0 && listed == want);
    atomic_store(&F.garble, 1u << 4);
    CHECK(coldops_peer.list_names(ctx, count_fn, NULL) != 0);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* Readers on several threads while a put runs and members come and go (TSan's case). */
static obj RO;
static void *RCTX;
static atomic_bool stop_readers;
static void *reader(void *arg)
{
    (void)arg;
    int n = 0;
    while (!atomic_load(&stop_readers) || n < 3) {
        chunk_report rep;
        int rc = read_back(RCTX, &RO, &rep);
        CHECK(rc == 0 || rc == TSDB_ERR_UNAVAILABLE);
        n++;
    }
    return NULL;
}

static void test_threads(void)
{
    char dir[PATH_MAX];
    cluster_up("threads", dir, sizeof dir);
    RCTX = make_ctx();
    CHECK(coldops_peer.see_epoch(RCTX, 5) == 0);
    obj_make(&RO, 1u << 20, 7);
    CHECK(put(RCTX, &RO, 5) == 0);
    atomic_store(&stop_readers, false);
    pthread_t th[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&th[i], NULL, reader, NULL);
    obj w;
    obj_make(&w, 900000, 8);
    CHECK(put(RCTX, &w, 5) == 0);
    atomic_store(&F.down, 1u << 3);
    atomic_store(&F.down, 0);
    atomic_store(&stop_readers, true);
    for (int i = 0; i < 4; i++)
        pthread_join(th[i], NULL);
    chunk_report rep;
    CHECK(read_back(RCTX, &w, &rep) == 0);
    obj_free(&w);
    obj_free(&RO);
    coldops_peer.ctx_free(RCTX);
    cluster_down();
}

static bool peer_stat(uint32_t node, struct chunkpeer_peer_stats *out)
{
    struct chunkpeer_peer_stats all[32];
    size_t n = chunkpeer_peer_stats_get(all, 32);
    for (size_t i = 0; i < n; i++)
        if (all[i].peer == node) {
            *out = all[i];
            return true;
        }
    memset(out, 0, sizeof *out);
    return false;
}

/* E31: sliding p99, hedge timer clamp(2*p99, [20, 500]) ms, first-answer-wins race (hedge wins and primary wins) with
 * late reply dropped cleanly. */
static void test_hedge_timer(void)
{
    char dir[PATH_MAX];
    cluster_up("hedgetimer", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 2u << 20, 20);
    CHECK(put(ctx, &o, 5) == 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h) == 0 && h);
    uint8_t buf[4096];
    chunk_report rep;
    /* 1. Fast server -> p99 < 10 ms -> hedge timer clamped to 20 ms floor. */
    for (int i = 0; i < 20; i++) {
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    }
    struct chunkpeer_peer_stats st;
    CHECK(peer_stat(1, &st) && st.hedge_ms == 20 && st.hedges == 0 && st.reads == 20);

    /* 2. Hedge wins while primary is slow (200 ms): returns quickly with exact bytes; late primary dropped cleanly. */
    atomic_store(&F.read_delay_ms[1], 200);
    uint64_t t0 = mono_ms();
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    uint64_t dt = mono_ms() - t0;
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    CHECK(dt < 140);
    CHECK(rep.decoded_cells >= 1);
    CHECK(peer_stat(1, &st) && st.hedges == 1 && st.decoded_cells >= 1);
    atomic_store(&F.read_delay_ms[1], 0);
    chunkpeer_quiesce(); /* late primary finished cleanly */

    /* 3. Primary wins AFTER hedge launched (primary 55 ms, parity node 5 220 ms -> hedge launches at 20 ms, primary
     *    wins at ~55 ms, late hedge dropped cleanly). */
    chunkpeer_hedge_reset();
    atomic_store(&F.read_delay_ms[1], 55);
    atomic_store(&F.read_delay_ms[5], 220);
    t0 = mono_ms();
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    dt = mono_ms() - t0;
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    CHECK(dt < 160 && rep.decoded_cells == 0);
    CHECK(peer_stat(1, &st) && st.hedges == 1);
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_store(&F.read_delay_ms[5], 0);
    chunkpeer_quiesce();

    /* 4. Dynamic 2 * p99 in (20, 500) ms, then ceiling clamp at 500 ms. */
    chunkpeer_hedge_reset();
    atomic_store(&F.read_delay_ms[1], 45);
    for (int i = 0; i < 4; i++) {
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    }
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &st) && st.p99_ms >= 35.0 && st.hedge_ms >= 70 && st.hedge_ms <= 200);

    atomic_store(&F.read_delay_ms[1], 320);
    for (int i = 0; i < 3; i++) {
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    }
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &st) && st.p99_ms >= 250.0 && st.hedge_ms == 500);

    coldops_peer.obj_close(h);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* E31: 5% token bucket per server keeps total chunk reads <= 1.05x baseline (kills mutant hedgeall). */
static void test_hedge_bucket(void)
{
    char dir[PATH_MAX];
    cluster_up("hedgebucket", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 2u << 20, 21);
    CHECK(put(ctx, &o, 5) == 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h) == 0 && h);
    uint8_t buf[4096];
    chunk_report rep;

    /* Baseline: 200 single-cell RAW reads spread across the 4 data positions (50 each). */
    const unsigned N = 200;
    for (unsigned i = 0; i < N; i++) {
        uint64_t off = (uint64_t)(i % o.g.k) * o.g.cell;
        CHECK(coldops_peer.obj_read(h, off, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes + off, sizeof buf) == 0);
    }
    unsigned base_reads = 0;
    for (uint32_t n = 1; n <= NM; n++) {
        base_reads += atomic_load(&F.read_calls[n]);
        atomic_store(&F.read_calls[n], 0);
    }
    CHECK(base_reads == N);

    /* Delay 6 reads on node 1 past the 500 ms ceiling (6 < 8 -> no read-avoid).
     * With 5% bucket (cap 1): only 1 hedge fires (+4 reads -> 204/200 = 1.02x <= 1.05x).
     * Without bucket (hedgeall): all 6 slow reads hedge (+24 reads -> 224/200 = 1.12x > 1.05x). */
    atomic_store(&F.read_delay_ms[1], 550);
    atomic_store(&F.read_delay_max[1], 6);
    atomic_store(&F.read_delay_seen[1], 0);
    for (unsigned i = 0; i < N; i++) {
        uint64_t off = (uint64_t)(i % o.g.k) * o.g.cell;
        CHECK(coldops_peer.obj_read(h, off, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes + off, sizeof buf) == 0);
    }
    chunkpeer_quiesce();
    unsigned slow_reads = 0;
    for (uint32_t n = 1; n <= NM; n++)
        slow_reads += atomic_load(&F.read_calls[n]);
    struct chunkpeer_peer_stats st;
    CHECK(peer_stat(1, &st));
    CHECK(slow_reads <= (base_reads * 105u) / 100u);
    CHECK(st.hedges <= 3 && st.read_avoid == 0);

    coldops_peer.obj_close(h);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* E31: 8 timeouts in 10 s enter read-avoid for 30 s (decoded around without waiting), then automatically recover
 * (kills mutant noavoid). */
static void test_hedge_avoid(void)
{
#ifndef TSDB_TEST_HOOKS
    printf("  test_hedge_avoid: SKIPPED (TSDB_TEST_HOOKS=0)\n");
    return;
#else
    char dir[PATH_MAX];
    cluster_up("hedgeavoid", dir, sizeof dir);
    atomic_store(&fake_clock_ms, 100000);
    chunkpeer_test_now_ms = f_now_ms;
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 2u << 20, 22);
    CHECK(put(ctx, &o, 5) == 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h) == 0 && h);
    uint8_t buf[4096];
    chunk_report rep;

    /* 7 timeouts in 10 s -> NOT yet in read-avoid. */
    atomic_store(&F.read_delay_ms[1], 35);
    atomic_store(&F.read_timeout, 1u << 1);
    for (int i = 0; i < 7; i++) {
        atomic_fetch_add(&fake_clock_ms, 100);
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    }
    chunkpeer_quiesce();
    struct chunkpeer_peer_stats st;
    CHECK(peer_stat(1, &st) && st.read_avoid == 0);

    /* Advance past the 10 s window: 1 more timeout does NOT trigger read-avoid (earlier 7 expired). */
    atomic_fetch_add(&fake_clock_ms, 11000);
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &st) && st.read_avoid == 0);

    /* 7 more timeouts within 10 s (total 8 in 10 s) -> enters read-avoid for 30 s. */
    for (int i = 0; i < 7; i++) {
        atomic_fetch_add(&fake_clock_ms, 100);
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
    }
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &st) && st.read_avoid == 1);

    /* While in read-avoid, even a 2 s delay on node 1 causes ZERO waits and ZERO reads to node 1. */
    atomic_store(&F.read_timeout, 0);
    atomic_store(&F.read_delay_ms[1], 2000);
    unsigned calls_before = atomic_load(&F.read_calls[1]);
    uint64_t t0 = mono_ms();
    for (int i = 0; i < 10; i++) {
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
        CHECK(memcmp(buf, o.bytes, sizeof buf) == 0);
        CHECK(rep.decoded_cells >= 1);
    }
    uint64_t dt = mono_ms() - t0;
    CHECK(dt < 500);
    CHECK(atomic_load(&F.read_calls[1]) == calls_before);
    CHECK(peer_stat(1, &st) && st.read_avoid == 1 && st.avoids >= 10 && st.decoded_cells >= 10);

    /* Still in read-avoid at +29 s. */
    atomic_fetch_add(&fake_clock_ms, 29000);
    CHECK(peer_stat(1, &st) && st.read_avoid == 1);

    /* At +30.001 s: automatically recovers and resumes direct reads to node 1. */
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_fetch_add(&fake_clock_ms, 1001);
    CHECK(peer_stat(1, &st) && st.read_avoid == 0);
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 0);
    CHECK(atomic_load(&F.read_calls[1]) == calls_before + 1);

    /* Short avoid duration (< 10 s window): timeouts arriving during read-avoid are ignored and expiry starts a
     * fresh window so a single timeout right after expiry cannot immediately re-trigger read-avoid. */
    chunkpeer_hedge_reset();
    chunkpeer_test_avoid_ms = 2000;
    atomic_store(&F.read_delay_ms[1], 35);
    atomic_store(&F.read_timeout, 1u << 1);
    for (int i = 0; i < 8; i++) {
        atomic_fetch_add(&fake_clock_ms, 50);
        CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    }
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &st) && st.read_avoid == 1);
    atomic_fetch_add(&fake_clock_ms, 2001); /* avoid expired, still < 10 s since earlier timeouts */
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &st) && st.read_avoid == 0);
    chunkpeer_test_avoid_ms = 0;
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_store(&F.read_timeout, 0);

    coldops_peer.obj_close(h);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
#endif
}

/* E31: p_obj_open() while a peer is in read-avoid skips CS_OP_SUMS (no 200 ms FAN_GRACE_MS stall), handles both
 * rec_sums != NULL and rec_sums == NULL, and resumes direct reads after read-avoid expires. */
static void test_hedge_open_avoid(void)
{
#ifndef TSDB_TEST_HOOKS
    printf("  test_hedge_open_avoid: SKIPPED (TSDB_TEST_HOOKS=0)\n");
    return;
#else
    char dir[PATH_MAX];
    cluster_up("hedgeopenav", dir, sizeof dir);
    atomic_store(&fake_clock_ms, 200000);
    chunkpeer_test_now_ms = f_now_ms;
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 2u << 20, 25);
    CHECK(put(ctx, &o, 5) == 0);
    cold_obj *h0 = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h0) == 0 && h0);
    uint8_t buf[4096];
    chunk_report rep;

    /* Drive node 1 into read-avoid. */
    atomic_store(&F.read_delay_ms[1], 35);
    atomic_store(&F.read_timeout, 1u << 1);
    for (int i = 0; i < 8; i++) {
        atomic_fetch_add(&fake_clock_ms, 100);
        CHECK(coldops_peer.obj_read(h0, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    }
    chunkpeer_quiesce();
    coldops_peer.obj_close(h0);
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_store(&F.read_timeout, 0);
    struct chunkpeer_peer_stats st;
    CHECK(peer_stat(1, &st) && st.read_avoid == 1);

    /* Hang node 1 completely: p_obj_open() MUST skip CS_OP_SUMS on node 1 (completing well below FAN_GRACE_MS=200ms). */
    atomic_store(&F.hang, 1u << 1);
    cold_obj *h_rec = NULL, *h_norec = NULL;
    uint64_t t0 = mono_ms();
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h_rec) == 0 && h_rec);
    uint64_t dt_rec = mono_ms() - t0;
    CHECK(dt_rec < 150);

    t0 = mono_ms();
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, NULL, &h_norec) == 0 && h_norec);
    uint64_t dt_norec = mono_ms() - t0;
    CHECK(dt_norec < 150);

    /* Both handles decode around node 1 without calling node 1 while avoided. */
    unsigned calls_before = atomic_load(&F.read_calls[1]);
    CHECK(coldops_peer.obj_read(h_rec, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 1);
    CHECK(coldops_peer.obj_read(h_norec, 0, sizeof buf, buf, 0, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 1);
    CHECK(atomic_load(&F.read_calls[1]) == calls_before);

    /* Release hang and expire read-avoid: h_rec (opened with rec_sums != NULL) resumes direct reads to node 1. */
    pthread_mutex_lock(&F.mu);
    atomic_store(&F.hang, 0);
    pthread_cond_broadcast(&F.hcv);
    pthread_mutex_unlock(&F.mu);
    atomic_fetch_add(&fake_clock_ms, 30001);
    CHECK(coldops_peer.obj_read(h_rec, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 0);
    CHECK(atomic_load(&F.read_calls[1]) == calls_before + 1);
    /* Non-RAW on h_rec also works directly (uses o->rec sums). */
    CHECK(coldops_peer.obj_read(h_rec, 0, sizeof buf, buf, 0, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 0);
    CHECK(atomic_load(&F.read_calls[1]) == calls_before + 2);

    coldops_peer.obj_close(h_rec);
    coldops_peer.obj_close(h_norec);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
#endif
}

/* Regression tests for degrade() isolation (exact decoded_cells attribution, zero nested hedges during degrade()),
 * failed-hedge short-circuit (no duplicate reads of surviving positions), and per-stripe bad_mask reset. */
static void test_hedge_degrade_and_stripes(void)
{
    char dir[PATH_MAX];
    cluster_up("hedgedegrade", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);

    /* 3-stripe object (cell = 64 KiB, k = 4, m = 2 -> 768 KiB total). */
    obj o;
    memset(&o, 0, sizeof o);
    for (int i = 0; i < 16; i++)
        o.oid[i] = (uint8_t)(26 * 16 + i);
    uint32_t cell = 64u << 10;
    uint64_t total_len = 3ull * 4ull * cell;
    o.bytes = malloc(total_len);
    for (uint64_t i = 0; i < total_len; i++)
        o.bytes[i] = (uint8_t)(i % 251 == 0 ? rnd() : i * 11 + 26);
    CHECK(chunk_geom_make(&o.g, 4, 2, cell, total_len) == 0);
    o.sums = calloc(chunk_sums_total(&o.g), sizeof *o.sums);
    CHECK(put(ctx, &o, 5) == 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h) == 0 && h);

    uint8_t buf[4096];
    chunk_report rep;
    struct chunkpeer_peer_stats s1, s2;

    /* 1. pos=0 (node 1) and pos=1 (node 2) both DOWN while reading ONLY pos=0:
     *    decoded_cells must be attributed solely to node 1 (+1), NOT node 2 (+0). */
    chunkpeer_hedge_reset();
    atomic_store(&F.down, (1u << 1) | (1u << 2));
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 1);
    CHECK(peer_stat(1, &s1) && s1.decoded_cells == 1);
    CHECK(!peer_stat(2, &s2) || s2.decoded_cells == 0);

    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, 0, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 1);
    CHECK(peer_stat(1, &s1) && s1.decoded_cells == 2);
    CHECK(!peer_stat(2, &s2) || s2.decoded_cells == 0);

    /* 2. pos=0 (node 1) DOWN, survivor pos=1 (node 2) SLOW (80 ms):
     *    degrade() reads pos=1 synchronously -> ZERO nested hedges on node 2 and exact attribution on node 1. */
    chunkpeer_hedge_reset();
    atomic_store(&F.down, 1u << 1);
    atomic_store(&F.read_delay_ms[2], 80);
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells == 1);
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &s1) && s1.decoded_cells == 1);
    CHECK(peer_stat(2, &s2) && s2.hedges == 0 && s2.decoded_cells == 0);
    atomic_store(&F.read_delay_ms[2], 0);
    atomic_store(&F.down, 0);

    /* 3. Failed hedge does NOT re-read surviving positions a second time in outer degrade(). */
    chunkpeer_hedge_reset();
    for (uint32_t n = 1; n <= NM; n++)
        atomic_store(&F.read_calls[n], 0);
    atomic_store(&F.read_delay_ms[1], 50);
    atomic_store(&F.read_timeout, (1u << 1) | (1u << 2) | (1u << 3));
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == TSDB_ERR_UNAVAILABLE);
    chunkpeer_quiesce();
    for (uint32_t n = 1; n <= NM; n++)
        CHECK(atomic_load(&F.read_calls[n]) <= 1);

    /* E15 with failed hedge: primary fails with IOERR (not DOWN) while 2 survivors are DAMAGED -> CORRUPT. */
    chunkpeer_hedge_reset();
    atomic_store(&F.read_timeout, 0);
    atomic_store(&F.ioerr, 1u << 1);
    atomic_store(&F.flip_byte, (1u << 2) | (1u << 3));
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, 0, &rep) == TSDB_ERR_CORRUPT);
    chunkpeer_quiesce();
    atomic_store(&F.ioerr, 0);
    atomic_store(&F.flip_byte, 0);
    atomic_store(&F.read_delay_ms[1], 0);

    /* 4. Multi-stripe bad_mask reset: node 2 fails on stripe 0, node 3 fails on stripe 1, node 1 is slow (220 ms)
     *    on stripe 2 -> bad_mask resets per stripe so stripe 2's hedge succeeds using healthy nodes 2..6. */
    chunkpeer_hedge_reset();
    atomic_store(&F.fail_cell, cell);
    atomic_store(&F.read_fail_stripe[2], 0);
    atomic_store(&F.read_fail_stripe[3], 1);
    atomic_store(&F.read_delay_ms[1], 220);
    atomic_store(&F.read_delay_stripe[1], 2);
    uint8_t *big = malloc(total_len);
    CHECK(big != NULL);
    CHECK(coldops_peer.obj_read(h, 0, total_len, big, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(big, o.bytes, total_len) == 0);
    chunkpeer_quiesce();
    CHECK(peer_stat(1, &s1) && s1.hedges == 1 && s1.decoded_cells == 1);
    free(big);
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_store(&F.read_delay_stripe[1], -1);
    atomic_store(&F.read_fail_stripe[2], -1);
    atomic_store(&F.read_fail_stripe[3], -1);
    atomic_store(&F.fail_cell, 0);

    coldops_peer.obj_close(h);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* E11 + E15: rot on a hedge survivor is repaired by whole-cell retry; >m rotted -> CORRUPT; hedge timeout with m
 * down or m damaged -> UNAVAILABLE, never CORRUPT (kills mutant hedgecorrupt). */
static void test_hedge_e15_e11(void)
{
    char dir[PATH_MAX];
    cluster_up("hedgee15", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 2u << 20, 23);
    CHECK(put(ctx, &o, 5) == 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open(ctx, o.oid, &o.g, o.sums, &h) == 0 && h);
    uint8_t buf[4096];
    chunk_report rep;

    /* E11: node 1 slow (hedged) + node 2 rotted -> RAW hedge produces bad bytes (caught by seam CRC), whole-cell
     * retry (flags=0) verifies encode-time sums, discards node 2 and decodes exact bytes from nodes 3..6. */
    atomic_store(&F.read_delay_ms[1], 150);
    atomic_store(&F.flip_byte, 1u << 2);
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) != 0); /* RAW cannot see rot: seam CRC triggers retry */
    chunkpeer_quiesce();
    chunkpeer_hedge_reset();
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, 0, &rep) == 0);
    CHECK(memcmp(buf, o.bytes, sizeof buf) == 0 && rep.decoded_cells >= 1);
    chunkpeer_quiesce();

    /* E15: m + 1 = 3 positions rotted -> CORRUPT. */
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_store(&F.flip_byte, (1u << 1) | (1u << 2) | (1u << 3));
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, 0, &rep) == TSDB_ERR_CORRUPT);
    atomic_store(&F.flip_byte, 0);

    /* E15: 2 positions down + 1 hedge timeout -> UNAVAILABLE, NEVER CORRUPT (kills hedgecorrupt). */
    chunkpeer_hedge_reset();
    atomic_store(&F.down, (1u << 2) | (1u << 3));
    atomic_store(&F.read_delay_ms[1], 45);
    atomic_store(&F.read_timeout, 1u << 1);
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, CHUNK_RR_RAW, &rep) == TSDB_ERR_UNAVAILABLE);
    chunkpeer_quiesce();

    /* E15: 2 positions damaged + 1 hedge timeout -> UNAVAILABLE (3 good + 1 down = 4 >= k), NEVER CORRUPT. */
    chunkpeer_hedge_reset();
    atomic_store(&F.down, 0);
    atomic_store(&F.flip_byte, (1u << 2) | (1u << 3));
    CHECK(coldops_peer.obj_read(h, 0, sizeof buf, buf, 0, &rep) == TSDB_ERR_UNAVAILABLE);
    chunkpeer_quiesce();

    atomic_store(&F.flip_byte, 0);
    atomic_store(&F.read_delay_ms[1], 0);
    atomic_store(&F.read_timeout, 0);
    coldops_peer.obj_close(h);
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* Concurrent hedged readers + read-avoid transitions + late replies dropped (TSan stress). */
static cold_obj *HOBJ;
static atomic_bool stop_hreaders;
static void *hreader(void *arg)
{
    int id = (int)(intptr_t)arg;
    uint8_t buf[4096];
    for (int i = 0; !atomic_load(&stop_hreaders) || i < 4; i++) {
        uint64_t off = (uint64_t)((i + id) % RO.g.k) * RO.g.cell;
        chunk_report rep;
        int rc = coldops_peer.obj_read(HOBJ, off, sizeof buf, buf, (i & 3) == 0 ? 0 : CHUNK_RR_RAW, &rep);
        CHECK(rc == 0);
        if (rc == 0)
            CHECK(memcmp(buf, RO.bytes + off, sizeof buf) == 0);
    }
    return NULL;
}

static void test_hedge_threads(void)
{
    char dir[PATH_MAX];
    cluster_up("hedgethreads", dir, sizeof dir);
#ifdef TSDB_TEST_HOOKS
    atomic_store(&fake_clock_ms, 500000);
    chunkpeer_test_now_ms = f_now_ms;
#endif
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj_make(&RO, 1u << 20, 24);
    CHECK(put(ctx, &RO, 5) == 0);
    CHECK(coldops_peer.obj_open(ctx, RO.oid, &RO.g, RO.sums, &HOBJ) == 0 && HOBJ);
    atomic_store(&stop_hreaders, false);
    pthread_t th[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&th[i], NULL, hreader, (void *)(intptr_t)i);
    atomic_store(&F.read_delay_ms[1], 45);
    for (int k = 0; k < 10; k++) {
        struct timespec ts = {0, 25 * 1000000L};
        nanosleep(&ts, NULL);
#ifdef TSDB_TEST_HOOKS
        atomic_fetch_add(&fake_clock_ms, 500);
#endif
    }
    atomic_store(&F.read_delay_ms[1], 0);
#ifdef TSDB_TEST_HOOKS
    atomic_fetch_add(&fake_clock_ms, 31000);
#endif
    struct timespec ts = {0, 50 * 1000000L};
    nanosleep(&ts, NULL);
    atomic_store(&stop_hreaders, true);
    for (int i = 0; i < 4; i++)
        pthread_join(th[i], NULL);
    coldops_peer.obj_close(HOBJ);
    HOBJ = NULL;
    obj_free(&RO);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* P-27: parallel p_put_stream uploads all k+m positions concurrently; pool-backed fan_run reuses threads. */
static void test_parallel_put(void)
{
    char dir[PATH_MAX];
    cluster_up("parput", dir, sizeof dir);
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);
    obj o;
    obj_make(&o, 128u << 10, 31);
    for (uint32_t i = 1; i <= NM; i++)
        atomic_store(&F.put_delay_ms[i], 120);
    uint64_t t0 = mono_ms();
    CHECK(put(ctx, &o, 5) == 0);
    uint64_t dt = mono_ms() - t0;
    /* Serial 6 x 120 ms = 720+ ms; parallel completes in ~120 ms (generous 500 ms cap for TSan/slow-disk headroom). */
    CHECK(dt < 500);
    for (uint32_t i = 1; i <= NM; i++)
        atomic_store(&F.put_delay_ms[i], 0);
    chunk_report rep;
    CHECK(read_back(ctx, &o, &rep) == 0 && rep.decoded_cells == 0);
    /* Repeated opens exercise pooled fan_run with both expect!=NULL and expect==NULL. */
    for (int i = 0; i < 20; i++) {
        cold_obj *h = NULL;
        CHECK(coldops_peer.obj_open(ctx, o.oid, (i & 1) ? &o.g : NULL, (i & 1) ? o.sums : NULL, &h) == 0 && h);
        coldops_peer.obj_close(h);
    }
    obj_free(&o);
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

/* R9 (E10): degraded put (k+m-1), stripe_up_mask, pre-marker UP check, known-erasure read, and note_missing hook. */
static atomic_uint deg_noted_cnt;
static atomic_uint deg_noted_pos;
static void deg_note_cb(uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    (void)volume;
    (void)oid;
    atomic_store(&deg_noted_pos, pos);
    atomic_fetch_add(&deg_noted_cnt, 1);
}

static void test_degraded_put_read(void)
{
    char dir[PATH_MAX];
    cluster_up("degput", dir, sizeof dir);
    CP.note_missing = deg_note_cb;
    void *ctx = make_ctx();
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);

    /* 1. Member 2 (pos 1) down -> stripe_up_mask reports 5 of 6 UP */
    atomic_store(&F.down, 1u << 2);
    uint32_t up = 0;
    unsigned npos = 0;
    CHECK(coldops_peer.stripe_up_mask(ctx, 0, 0, &up, &npos) == 0 && npos == 6 && up == (0x3fu & ~(1u << 1)));

    /* 2. put_stream_pos with 2 missing positions (0x3c) is refused */
    obj o;
    obj_make(&o, 512u << 10, 41);
    chunk_put_src psrc = {.ctx = o.bytes, .fill = fill};
    uint32_t written = 0;
    CHECK(coldops_peer.put_stream_pos(ctx, o.oid, &o.g, 5, &psrc, 0x3cu, &written, o.sums) == TSDB_ERR_UNAVAILABLE);

    /* 3. put_stream_pos with 5 reachable positions succeeds and writes only those 5 */
    CHECK(coldops_peer.put_stream_pos(ctx, o.oid, &o.g, 5, &psrc, up, &written, o.sums) == 0 && written == up);
    /* Pre-MK_COLD check: every written position is UP in epoch 5 */
    uint32_t up_e = 0;
    CHECK(coldops_peer.stripe_up_mask(ctx, 5, written, &up_e, &npos) == 0 && (up_e & written) == written);
    /* note_missing hook fires cleanly */
    atomic_store(&deg_noted_cnt, 0);
    coldops_peer.note_missing(ctx, o.oid, 1);
    CHECK(atomic_load(&deg_noted_cnt) == 1 && atomic_load(&deg_noted_pos) == 1);

    /* 4. Bring member 2 back UP (still holds NO chunk of o): reads with `written` bitmap NEVER request member 2 */
    atomic_store(&F.down, 0);
    atomic_store(&F.read_calls[2], 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open_pos(ctx, o.oid, &o.g, o.sums, written, &h) == 0 && h);
    uint8_t *got = malloc(o.g.len);
    chunk_report rep;
    CHECK(coldops_peer.obj_read(h, 0, o.g.len, got, 0, &rep) == 0);
    CHECK(memcmp(got, o.bytes, o.g.len) == 0 && rep.st[1] == CHUNK_ST_UNWRITTEN);
    CHECK(atomic_load(&F.read_calls[2]) == 0);
    coldops_peer.obj_close(h);

    /* 5. Member 3 (pos 2) down -> 4 written positions remain (k = 4) -> reads still succeed bit-exact */
    atomic_store(&F.down, 1u << 3);
    h = NULL;
    CHECK(coldops_peer.obj_open_pos(ctx, o.oid, &o.g, o.sums, written, &h) == 0 && h);
    memset(got, 0, o.g.len);
    CHECK(coldops_peer.obj_read(h, 0, o.g.len, got, 0, &rep) == 0);
    CHECK(memcmp(got, o.bytes, o.g.len) == 0 && atomic_load(&F.read_calls[2]) == 0);
    coldops_peer.obj_close(h);

    /* 6. Member 4 (pos 3) ALSO down -> 3 written positions remain (< k) -> UNAVAILABLE, never CORRUPT */
    atomic_store(&F.down, (1u << 3) | (1u << 4));
    h = NULL;
    CHECK(coldops_peer.obj_open_pos(ctx, o.oid, &o.g, o.sums, written, &h) == 0 && h);
    CHECK(coldops_peer.obj_read(h, 0, o.g.len, got, 0, &rep) == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&F.read_calls[2]) == 0);
    coldops_peer.obj_close(h);

    /* 7. All members back -> reads succeed again */
    atomic_store(&F.down, 0);
    h = NULL;
    CHECK(coldops_peer.obj_open_pos(ctx, o.oid, &o.g, o.sums, written, &h) == 0 && h);
    memset(got, 0, o.g.len);
    CHECK(coldops_peer.obj_read(h, 0, o.g.len, got, 0, &rep) == 0 && memcmp(got, o.bytes, o.g.len) == 0);
    coldops_peer.obj_close(h);

    free(got);
    obj_free(&o);
    CP.note_missing = NULL;
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

static atomic_int g_del_rel_calls;
static atomic_int g_rel_guard_rc;
static atomic_int g_mock_node_state;

static int mock_node_state(void *arg, uint32_t node)
{
    (void)arg, (void)node;
    return atomic_load(&g_mock_node_state);
}

static void move_back_after_first_del(uint32_t node, const struct cs_req *r)
{
    (void)node, (void)r;
    if (atomic_fetch_add(&g_del_rel_calls, 1) == 0) {
        pthread_mutex_lock(&F.mu);
        F.view.stripe[0] = 1;
        F.view.stripe_version = 3;
        F.view.stripe_set_ver[0] = 3;
        F.view.stripe_filling[0] = 1;
        F.view.stripe_prev[0] = 6;
        F.view.stripe_prev_up[0] = 1;
        pthread_mutex_unlock(&F.mu);
    }
}

static int mock_rel_guard(void *arg, uint32_t volume, unsigned pos, uint64_t stripe_ver)
{
    (void)arg, (void)volume, (void)pos, (void)stripe_ver;
    return atomic_load(&g_rel_guard_rc);
}

/* R9 (r9-whole, D10, D12): reads while filling (prev UP -> A, prev DOWN -> erasure unless B reported filled,
 * post-barrier >= V -> B; kills readfromfilling) and release_candidates / release_verify / release_pos,
 * including mid-release move-back to A and server-side CS_OP_DEL_REL guard. */
static void test_filling_reads_and_release(void)
{
    char dir[PATH_MAX];
    cluster_up("fillread", dir, sizeof dir);
    chunkpeer_filled_reset();
    void *ctx = NULL;
    CHECK(chunkpeer_make(&CP, NULL, VOL, VUUID, &ctx) == 0);
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0);

    /* Use k=3, m=2 (5 positions on nodes 1..5, with node 6 as spare B) so pos 0 can transition from node 1 (A)
     * to node 6 (B) within the 6-node test cluster. */
    pthread_mutex_lock(&F.mu);
    F.view.k = 3;
    F.view.m = 2;
    F.view.nstripe = 5;
    F.view.stripe_version = 1;
    for (unsigned i = 0; i < 5; i++)
        F.view.stripe[i] = i + 1;
    pthread_mutex_unlock(&F.mu);

    obj old_o, old_o2;
    obj_make(&old_o, 256u << 10, 71);
    CHECK(chunk_geom_derive(&old_o.g, 3, 2, old_o.g.len) == 0);
    free(old_o.sums);
    old_o.sums = calloc(chunk_sums_total(&old_o.g), sizeof *old_o.sums);
    chunk_put_src psrc = {.ctx = old_o.bytes, .fill = fill};
    CHECK(coldops_peer.put_stream(ctx, old_o.oid, &old_o.g, 5, &psrc, old_o.sums) == 0);

    obj_make(&old_o2, 256u << 10, 73);
    CHECK(chunk_geom_derive(&old_o2.g, 3, 2, old_o2.g.len) == 0);
    free(old_o2.sums);
    old_o2.sums = calloc(chunk_sums_total(&old_o2.g), sizeof *old_o2.sums);
    chunk_put_src psrc_o2 = {.ctx = old_o2.bytes, .fill = fill};
    CHECK(coldops_peer.put_stream(ctx, old_o2.oid, &old_o2.g, 5, &psrc_o2, old_o2.sums) == 0);

    /* Move pos 0 from node 1 (A) to node 6 (B) in state FILLING at stripe_version = 2 (barrier V = 2).
     * Node 6 does NOT hold old_o or old_o2's pos-0 chunks yet! */
    pthread_mutex_lock(&F.mu);
    F.view.stripe[0] = 6;
    F.view.stripe_version = 2;
    F.view.stripe_filling[0] = 1;
    F.view.stripe_prev[0] = 1;
    F.view.stripe_prev_up[0] = 1;
    F.view.stripe_set_ver[0] = 2;
    pthread_mutex_unlock(&F.mu);

    /* Put new_o at stripe_version = 2 while pos 0 is FILLING: new_o goes directly to node 6 (B)! */
    obj new_o;
    obj_make(&new_o, 256u << 10, 72);
    CHECK(chunk_geom_derive(&new_o.g, 3, 2, new_o.g.len) == 0);
    free(new_o.sums);
    new_o.sums = calloc(chunk_sums_total(&new_o.g), sizeof *new_o.sums);
    chunk_put_src psrc2 = {.ctx = new_o.bytes, .fill = fill};
    CHECK(coldops_peer.put_stream(ctx, new_o.oid, &new_o.g, 5, &psrc2, new_o.sums) == 0);

    uint8_t *got = malloc(old_o.g.len);
    chunk_report rep;

    /* 0. Filled-object tracking keyed by (node, volume, pos, set_ver, oid) and cleared per volume. */
    chunkpeer_note_filled(6, VOL, 0, 2, old_o.oid);
    CHECK(chunkpeer_is_filled(6, VOL, 0, 2, old_o.oid));
    CHECK(!chunkpeer_is_filled(6, VOL, 1, 2, old_o.oid));
    CHECK(!chunkpeer_is_filled(6, VOL, 0, 3, old_o.oid));
    chunkpeer_filled_clear_vol(VOL);
    CHECK(!chunkpeer_is_filled(6, VOL, 0, 2, old_o.oid));

    /* 1. Post-barrier object (stripe_version == 2 >= V): MUST read pos 0 directly from node 6 (B). */
    atomic_store(&F.read_calls[6], 0);
    cold_obj *h = NULL;
    CHECK(coldops_peer.obj_open_ver(ctx, new_o.oid, &new_o.g, new_o.sums, 0, 2, &h) == 0 && h);
    CHECK(coldops_peer.obj_read(h, 0, new_o.g.len, got, 0, &rep) == 0 && memcmp(got, new_o.bytes, new_o.g.len) == 0);
    CHECK(atomic_load(&F.read_calls[6]) > 0);
    coldops_peer.obj_close(h);

    /* 2. Pre-barrier object (stripe_version == 1 < V) with prev A (node 1) UP:
     *    rot 2 other positions (nodes 4 and 5) so reading pos 0 from A is REQUIRED (3 healthy = {1,2,3} == k=3).
     *    Honest reads from A (OK, 0 reads to B); readfromfilling asks B (ABSENT) -> CORRUPT! */
    atomic_store(&F.flip_byte, (1u << 4) | (1u << 5));
    atomic_store(&F.read_calls[6], 0);
    atomic_store(&F.read_calls[1], 0);
    h = NULL;
    CHECK(coldops_peer.obj_open_ver(ctx, old_o.oid, &old_o.g, old_o.sums, 0, 1, &h) == 0 && h);
    memset(got, 0, old_o.g.len);
    CHECK(coldops_peer.obj_read(h, 0, old_o.g.len, got, 0, &rep) == 0 && memcmp(got, old_o.bytes, old_o.g.len) == 0);
    CHECK(atomic_load(&F.read_calls[1]) > 0 && atomic_load(&F.read_calls[6]) == 0);
    coldops_peer.obj_close(h);

    /* 3. Pre-barrier object with prev A DOWN and B NOT reported filled:
     *    with nodes 4 and 5 rotted, only 2 healthy (nodes 2,3) + pos 0 erasure (DOWN) = 3 >= k=3 -> UNAVAILABLE!
     *    readfromfilling asks B (ABSENT) -> 2 good + 0 down < k=3 -> TSDB_ERR_CORRUPT! */
    pthread_mutex_lock(&F.mu);
    F.view.stripe_prev_up[0] = 0;
    pthread_mutex_unlock(&F.mu);
    atomic_store(&F.read_calls[6], 0);
    h = NULL;
    CHECK(coldops_peer.obj_open_ver(ctx, old_o.oid, &old_o.g, old_o.sums, 0, 1, &h) == 0 && h);
    CHECK(coldops_peer.obj_read(h, 0, old_o.g.len, got, 0, &rep) == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&F.read_calls[6]) == 0);
    coldops_peer.obj_close(h);
    atomic_store(&F.flip_byte, 0);

    /* 4. With prev A DOWN and healthy survivors, decode around unfilled pos 0 without ever asking B. */
    atomic_store(&F.read_calls[6], 0);
    h = NULL;
    CHECK(coldops_peer.obj_open_ver(ctx, old_o.oid, &old_o.g, old_o.sums, 0, 1, &h) == 0 && h);
    memset(got, 0, old_o.g.len);
    CHECK(coldops_peer.obj_read(h, 0, old_o.g.len, got, 0, &rep) == 0 && memcmp(got, old_o.bytes, old_o.g.len) == 0);
    CHECK(atomic_load(&F.read_calls[6]) == 0);
    coldops_peer.obj_close(h);

    /* 5. release_candidates / release_verify / release_pos (D12):
     *    While FILLING: release_candidates returns 0 and release_verify refuses (-1). */
    cold_release cands[8];
    CHECK(coldops_peer.release_candidates(ctx, cands, 8) == 0);
    cold_live_obj live[3];
    memcpy(live[0].oid, old_o.oid, 16);
    live[0].npos = 5;
    live[0].positions = 0;
    live[0].stripe_version = 1;
    memcpy(live[1].oid, old_o2.oid, 16);
    live[1].npos = 5;
    live[1].positions = 0;
    live[1].stripe_version = 1;
    memcpy(live[2].oid, new_o.oid, 16);
    live[2].npos = 5;
    live[2].positions = 0;
    live[2].stripe_version = 2;
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) < 0);

    /* Mark WHOLE while B (node 6) STILL lacks old_o & old_o2 -> release_verify MUST refuse (-1) via CHUNK_LIST! */
    pthread_mutex_lock(&F.mu);
    F.view.stripe_filling[0] = 0;
    F.view.stripe_prev_up[0] = 1;
    pthread_mutex_unlock(&F.mu);
    CHECK(coldops_peer.release_candidates(ctx, cands, 8) == 1 && cands[0].pos == 0 && cands[0].node == 1 &&
          cands[0].version == 2);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) < 0);

    /* Put both old_o and old_o2 onto B (node 6) at stripe={6,2,3,4,5}. */
    chunk_put_src psrc3 = {.ctx = old_o.bytes, .fill = fill};
    CHECK(coldops_peer.put_stream(ctx, old_o.oid, &old_o.g, 5, &psrc3, NULL) == 0);
    chunk_put_src psrc4 = {.ctx = old_o2.bytes, .fill = fill};
    CHECK(coldops_peer.put_stream(ctx, old_o2.oid, &old_o2.g, 5, &psrc4, NULL) == 0);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) == 1);

    /* 5a. Lagging copy (caught_up == 0) must refuse release_candidates, release_verify, and release_pos. */
    pthread_mutex_lock(&F.mu);
    F.view.caught_up = 0;
    pthread_mutex_unlock(&F.mu);
    CHECK(coldops_peer.release_candidates(ctx, cands, 8) == 0);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) < 0);
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == TSDB_ERR_UNAVAILABLE);
    pthread_mutex_lock(&F.mu);
    F.view.caught_up = 1;
    pthread_mutex_unlock(&F.mu);

    /* 5a2. Fail-closed CS_OP_DEL_REL when server has no rel_guard registered yet (startup window):
     *      must return TSDB_ERR_UNAVAILABLE and leave node 1's chunks untouched! */
    chunksrv_set_rel_guard(F.cs[1], NULL, NULL);
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == TSDB_ERR_UNAVAILABLE);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) == 1);

    /* 5b. Mid-release move-back to A (node 1): install permissive rel_guard; after the 1st CS_OP_DEL_REL,
     *     STRIPE_SET moves pos 0 back to node 1 at stripe_version=3 (FILLING). release_pos MUST stop
     *     immediately after 1 delete and leave chunk 2 intact! */
    atomic_store(&g_rel_guard_rc, 0);
    chunksrv_set_rel_guard(F.cs[1], mock_rel_guard, NULL);
    atomic_store(&g_del_rel_calls, 0);
    F.after_del_rel = move_back_after_first_del;
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == 0);
    F.after_del_rel = NULL;
    CHECK(atomic_load(&g_del_rel_calls) == 1);

    /* Restore version 2 (node 6 whole): node 1 MUST still hold its 2nd chunk (release_verify returns 1). */
    pthread_mutex_lock(&F.mu);
    F.view.stripe[0] = 6;
    F.view.stripe_version = 2;
    F.view.stripe_set_ver[0] = 2;
    F.view.stripe_filling[0] = 0;
    F.view.stripe_prev[0] = 1;
    F.view.stripe_prev_up[0] = 1;
    pthread_mutex_unlock(&F.mu);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) == 1);

    /* 5c. Server-side CS_OP_DEL_REL guard: node 1's own rel_guard refuses with TSDB_ERR_LOCKED (node 1 sees it now
     *     holds pos 0) -> release_pos refuses without deleting node 1's remaining chunk! */
    atomic_store(&g_rel_guard_rc, TSDB_ERR_LOCKED);
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == TSDB_ERR_UNAVAILABLE);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) == 1);

    /* 5d. Former holder DOWN skips without network call; DESTROYED retires with 0 without network call. */
    atomic_store(&g_rel_guard_rc, 0);
    CP.node_state = mock_node_state;
    atomic_store(&g_mock_node_state, CP_NODE_DOWN);
    atomic_store(&g_del_rel_calls, 0);
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&g_del_rel_calls) == 0);
    atomic_store(&g_mock_node_state, CP_NODE_DESTROYED);
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == 0);
    CHECK(atomic_load(&g_del_rel_calls) == 0);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) == 1); /* chunk still on disk */
    CP.node_state = NULL;

    /* 5e. Chain move A(1)->B(6) at V=2, then B(6)->C(6, whole) at V=3 (set_ver > ver, stripe[0] != 1):
     *     pending release for A at V=2 MUST STILL delete A's remaining chunk instead of leaking it! */
    pthread_mutex_lock(&F.mu);
    F.view.stripe_version = 3;
    F.view.stripe_set_ver[0] = 3;
    F.view.stripe_filling[0] = 0;
    F.view.stripe_prev[0] = 6;
    pthread_mutex_unlock(&F.mu);
    CHECK(coldops_peer.release_pos(ctx, 0, 1, 2, 5, 42) == 0);
    chunksrv_set_rel_guard(F.cs[1], NULL, NULL);
    CHECK(coldops_peer.release_verify(ctx, 0, 1, 2, live, 3) == 0);

    free(got);
    obj_free(&old_o);
    obj_free(&old_o2);
    obj_free(&new_o);
    chunkpeer_filled_reset();
    coldops_peer.ctx_free(ctx);
    cluster_down();
}

int main(int argc, char **argv)
{
    const char *base = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            if ((chunkpeer_mutant = chunkpeer_mutant_parse(argv[++i])) < 0) {
                fprintf(stderr, "unknown mutant %s\n", argv[i]);
                return 2;
            }
        } else {
            base = argv[i];
        }
    }
    pthread_mutex_init(&F.mu, NULL);
    pthread_cond_init(&F.hcv, NULL);
    snprintf(S, sizeof S, "%s/test_chunkpeer.%d", base, (int)getpid());
    if (mkdir(S, 0755) != 0) {
        fprintf(stderr, "cannot make %s: %s\n", S, strerror(errno));
        return 1;
    }
    test_put_read();
    test_parallel_put();
    test_degraded_put_read();
    test_filling_reads_and_release();
    test_sum();
    test_fence_del();
    test_unsure();
    test_unfenced();
    test_hang();
    test_list();
    test_threads();
    test_hedge_timer();
    test_hedge_bucket();
    test_hedge_avoid();
    test_hedge_open_avoid();
    test_hedge_degrade_and_stripes();
    test_hedge_e15_e11();
    test_hedge_threads();
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", S);
    if (system(cmd) != 0)
        fprintf(stderr, "could not remove %s\n", S);
    if (fails) {
        fprintf(stderr, "test_chunkpeer: %d FAILED%s\n", fails, chunkpeer_mutant ? " (a mutant)" : "");
        return 1;
    }
    printf("test_chunkpeer: OK\n");
    return 0;
}
#endif
