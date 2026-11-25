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
    struct chunkpeer_view view;
    pthread_mutex_t mu;
};
static struct fake F;

static int f_call(void *arg, uint32_t node, const struct cs_req *r, int tmo, int32_t *rc, uint8_t **body, size_t *n)
{
    (void)arg, (void)tmo;
    *body = NULL;
    if (node == 0 || node > NM || (atomic_load(&F.down) >> node & 1))
        return TSDB_ERR_UNAVAILABLE;
    return chunkpeer_local_call(F.cs[node], 99, r, rc, body, n);
}

static int f_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum, int tmo,
                 uint64_t *answered)
{
    (void)arg, (void)sum, (void)tmo;
    if (node == 0 || node > NM || (atomic_load(&F.down) >> node & 1))
        return TSDB_ERR_UNAVAILABLE;
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
    uint8_t other[16] = {0xee};
    CHECK(coldops_peer.obj_open(ctx, other, NULL, NULL, &h) == TSDB_ERR_UNAVAILABLE);
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
    /* the listing: every position once, and each one's header names the volume */
    struct lst {
        unsigned n;
        unsigned pos_seen;
    } l = {0};
    int lf(void *c, const chunk_listed *e);
    owner_ctx = ctx;
    CHECK(coldops_peer.list_names(ctx, lf, &l) == 0);
    CHECK(l.n == chunk_npos(&o.g) && l.pos_seen == (1u << chunk_npos(&o.g)) - 1);
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
    ctx = make_ctx();
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

/* see_epoch on a member that cannot take it (unsure, no caught-up copy): writable is false for the context's life */
static void test_unfenced(void)
{
    char dir[PATH_MAX];
    cluster_up("unfenced", dir, sizeof dir);
    void *ctx = make_ctx();
    atomic_store(&F.down, 1u << 6);
    CHECK(coldops_peer.see_epoch(ctx, 5) == 0 && !coldops_peer.writable(ctx));
    atomic_store(&F.down, 0);
    CHECK(!coldops_peer.writable(ctx));
    obj o;
    obj_make(&o, 100000, 6);
    CHECK(put(ctx, &o, 5) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&F.down, 0x7eu);
    void *c2 = make_ctx();
    CHECK(coldops_peer.see_epoch(c2, 5) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&F.down, 0);
    obj_free(&o);
    coldops_peer.ctx_free(c2);
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
    snprintf(S, sizeof S, "%s/test_chunkpeer.%d", base, (int)getpid());
    if (mkdir(S, 0755) != 0) {
        fprintf(stderr, "cannot make %s: %s\n", S, strerror(errno));
        return 1;
    }
    test_put_read();
    test_sum();
    test_fence_del();
    test_unsure();
    test_unfenced();
    test_threads();
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
