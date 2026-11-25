/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's chunk server (chunksrv.h) on the cluster, as fwd_cluster.c does it for the forwarding links: the stores of
 * `--chunk-dir` are opened at start (each one's flock: this process is their one owner; a store another process holds
 * refuses the start, a missing, damaged or failing one is DOWN and logged), checked against this member's cluster and
 * node (D11: a store whose META names another cluster or node refuses the start with a line naming both), and a thread
 * makes the peerlink once this server is a member, registers it for PL_KIND_CHUNK and PL_KIND_CHUNKB in the cluster's
 * acceptor table and keeps its peers to the metadata copy's members. Every member holds the links, stores or not: a
 * member without stores still calls the others.
 *
 * A server that is not a member yet at start (it waits for `tsdb cluster node add`) checks its stores when it learns
 * its node; a mismatch then cannot refuse the start, so the stores are closed (the flock released) and the member
 * serves none, with the same line.
 *
 * The number of stores up goes to the metadata group's report through cluster_set_chunk_stores (r8c-meta3) where that
 * call exists: it is referenced weakly, so this file links against a cluster.c without it.
 */
#include "chunksrv.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "cluster.h"
#include "peerlink.h"
#include "../client/tsdb_crypto.h"
#include "../src/chunk_priv.h"
#include "../src/internal.h"

#ifdef __ELF__
extern void cluster_set_chunk_stores(uint32_t n) __attribute__((weak));
#endif

static struct {
    pthread_t thr;
    int started;
    _Atomic int stop;
    struct chunksrv *cs; /* owned by the thread until it listens, then published in `pub` */
    _Atomic(struct chunksrv *) pub;
    uint64_t rate;
    int checked; /* the owner check ran */
    int last_up; /* stores up last reported (-1: never) */
} G;

static void report_up(struct chunksrv *cs)
{
    int up = 0;
    chunksrv_stores(cs, &up, false);
    if (up == G.last_up)
        return;
    if (G.last_up >= 0)
        fprintf(stderr, "tsdbd: chunk stores up: %d (was %d)\n", up, G.last_up);
    G.last_up = up;
#ifdef __ELF__
    if (cluster_set_chunk_stores)
        cluster_set_chunk_stores((uint32_t)up);
#endif
}

static void set_peers(struct chunksrv *cs, const struct cluster_copy *c, uint32_t self)
{
    uint32_t *keep = calloc(c->nmembers ? c->nmembers : 1, sizeof *keep);
    size_t nk = 0;
    for (uint32_t i = 0; i < c->nmembers; i++) {
        const struct cluster_copy_member *m = &c->members[i];
        if (m->id == self || m->state == META_OUT)
            continue;
        if (keep)
            keep[nk++] = m->id;
        if (m->peer[0])
            chunksrv_set_peer(cs, m->id, m->peer); /* a failure is tried again at the next build */
    }
    if (keep) /* members that left (or are gone from the copy): their links stop redialing */
        chunksrv_keep_peers(cs, keep, nk);
    free(keep);
}

/* The node became known after the start: a store of another owner is closed, the member serves none. */
static void check_late(const struct cluster_self *me)
{
    char err[512];
    if (chunksrv_check_owner(G.cs, me->uuid, me->id, err, sizeof err) == 0)
        return;
    fprintf(stderr, "tsdbd: %s; serving no chunk store\n", err);
    chunksrv_close(G.cs);
    G.cs = chunksrv_open(NULL, 0, err, sizeof err);
    if (!G.cs)
        fprintf(stderr, "tsdbd: chunk server: %s\n", err);
}

/* ---------------------------------------------------------------- test hook (tests/test_cluster.c `chunksrv`)
 * TSDB_CHUNKSRV_TEST="put:PEER:DIR" or "check:PEER:DIR" runs once this server's links are up, against member PEER
 * through two real tsdbd (DIR stages the put files). put: a chunk at epoch 3, every byte read back, PEER's fence of
 * the volume raised to 5, then a put at 3 and a delete at 4 must be FENCED and the chunk still whole. check (after
 * PEER was killed with SIGKILL and restarted): the fence is still 5, the chunk reads back whole, a put at 4 is FENCED
 * (both were durable before their answers). The result is one log line: "chunksrv test: pass MODE" or "... FAIL". */
enum { ST_VOL = 7, ST_POS = 1 };
static const uint8_t st_vuuid[16] = {0x5e, 0x1f, 0x7e, 0x57, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

struct st_chunk {
    struct cs_put_meta m;
    uint8_t *cells;
    uint64_t *sums;
    uint64_t ncells, len;
};

static int st_make(struct st_chunk *c, uint64_t epoch, unsigned tag)
{
    memset(c, 0, sizeof *c);
    c->m.volume = ST_VOL;
    c->m.epoch = epoch;
    c->m.pos = ST_POS;
    memcpy(c->m.vuuid, st_vuuid, 16);
    for (int i = 0; i < 16; i++)
        c->m.oid[i] = (uint8_t)(tag * 17 + (unsigned)i);
    if (chunk_geom_make(&c->m.g, 3, 2, 4096, 4096 * 7 + 123) != 0)
        return -1;
    c->ncells = chunk_pos_cells(&c->m.g, ST_POS);
    c->len = chunk_pos_len(&c->m.g, ST_POS);
    c->cells = malloc(c->len ? c->len : 1);
    c->sums = malloc((c->ncells ? c->ncells : 1) * 8);
    if (!c->cells || !c->sums)
        return -1;
    uint64_t r = tag * 2654435761u + 1;
    for (uint64_t i = 0; i < c->len; i++) {
        r ^= r << 13, r ^= r >> 7, r ^= r << 17;
        c->cells[i] = (uint8_t)r;
    }
    for (uint64_t i = 0, off = 0; i < c->ncells; i++) {
        uint64_t l = chunk_pos_cell_len(&c->m.g, ST_POS, i);
        c->sums[i] = xxh64(c->cells + off, l, 0);
        off += l;
    }
    return 0;
}

static int st_put(struct chunksrv *cs, uint32_t peer, const char *dir, const struct st_chunk *c)
{
    int fd = chunksrv_stage(dir, &c->m.g, c->m.pos, c->cells, c->sums);
    if (fd < 0)
        return fd;
    int rc = chunksrv_put_remote(cs, peer, &c->m, fd, 0, cs_sums_hash(c->sums, c->ncells), 20000);
    close(fd);
    return rc;
}

/* 0 with the answer's rc in *prc (and the fence in *fence for EPOCH, the bytes compared for READ), or the call's */
static int st_call(struct chunksrv *cs, uint32_t peer, uint8_t op, uint64_t epoch, const struct st_chunk *c,
                   int32_t *prc, uint64_t *fence, int ms)
{
    struct cs_req r = {.op = op, .volume = ST_VOL, .epoch = epoch, .pos = ST_POS};
    memcpy(r.vuuid, st_vuuid, 16);
    memcpy(r.oid, c->m.oid, 16);
    if (op == CS_OP_READ)
        r.off = 0, r.len = (uint32_t)c->len;
    uint8_t *body = NULL;
    size_t bl = 0;
    int rc = chunksrv_call(cs, peer, &r, ms, prc, &body, &bl);
    if (rc == 0 && *prc == 0 && op == CS_OP_EPOCH) {
        if (bl != 9)
            rc = TSDB_ERR_CORRUPT;
        else
            for (int i = 7; i >= 0; i--)
                *fence = *fence << 8 | body[i];
    }
    if (rc == 0 && *prc == 0 && op == CS_OP_READ && (bl != c->len || memcmp(body, c->cells, c->len) != 0))
        rc = TSDB_ERR_CORRUPT;
    free(body);
    return rc;
}

static void selftest(struct chunksrv *cs, const char *spec)
{
    char mode[16] = "", dir[512] = "";
    unsigned peer = 0;
    int n = 0;
    if (sscanf(spec, "%15[a-z]:%u:%n", mode, &peer, &n) != 2 || n == 0 || !spec[n]) {
        fprintf(stderr, "tsdbd: chunksrv test: FAIL bad spec %s\n", spec);
        return;
    }
    snprintf(dir, sizeof dir, "%s", spec + n);
    const int check = strcmp(mode, "check") == 0;
    struct st_chunk a = {0}, b = {0};
    const char *what = "make";
    int rc = -1;
    int32_t prc = -1;
    uint64_t fence = 0;
    if (st_make(&a, 3, 1) != 0 || st_make(&b, check ? 4 : 3, 2) != 0)
        goto out;
    what = "reach the peer"; /* its chunk server may come up after this one's */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    while ((rc = st_call(cs, peer, CS_OP_EPOCH, 0, &a, &prc, &fence, 1000)) != 0 && !atomic_load(&G.stop)) {
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (t1.tv_sec - t0.tv_sec > 60)
            break;
        nanosleep(&(struct timespec){0, 100 * 1000000L}, NULL);
    }
    if (rc != 0 || prc != 0)
        goto out;
    if (!check) {
        what = "put at epoch 3";
        if ((rc = st_put(cs, peer, dir, &a)) != 0)
            goto out;
        what = "read back";
        if ((rc = st_call(cs, peer, CS_OP_READ, 3, &a, &prc, &fence, 20000)) != 0 || prc != 0)
            goto out;
        what = "raise the fence to 5";
        fence = 0;
        if ((rc = st_call(cs, peer, CS_OP_EPOCH, 5, &a, &prc, &fence, 20000)) != 0 || prc != 0 || fence != 5)
            goto out;
        what = "a put at epoch 3 below the fence is not FENCED";
        if ((rc = st_put(cs, peer, dir, &b)) != CHUNK_ERR_FENCED)
            goto out;
        what = "a delete at epoch 4 below the fence is not FENCED";
        if ((rc = st_call(cs, peer, CS_OP_DEL, 4, &a, &prc, &fence, 20000)) != 0 || prc != CHUNK_ERR_FENCED)
            goto out;
    } else {
        what = "the fence after the restart is not 5";
        if (fence != 5)
            goto out;
        what = "a put at epoch 4 after the restart is not FENCED";
        if ((rc = st_put(cs, peer, dir, &b)) != CHUNK_ERR_FENCED)
            goto out;
    }
    what = "the chunk does not read back whole";
    if ((rc = st_call(cs, peer, CS_OP_READ, 5, &a, &prc, &fence, 20000)) != 0 || prc != 0)
        goto out;
    what = NULL;
out:
    if (what)
        fprintf(stderr, "tsdbd: chunksrv test: FAIL %s: %s (rc %d, answer %d, fence %" PRIu64 ")\n", mode, what, rc,
                (int)prc, fence);
    else
        fprintf(stderr, "tsdbd: chunksrv test: pass %s\n", mode);
    free(a.cells), free(a.sums), free(b.cells), free(b.sums);
}

static void *selftest_main(void *arg)
{
    selftest(arg, getenv("TSDB_CHUNKSRV_TEST"));
    return NULL;
}

static void *cc_main(void *arg)
{
    (void)arg;
    unsigned long seen = 0;
    int have = 0, tested = 0;
    pthread_t st_thr;
    uint32_t self = 0;
    while (!atomic_load(&G.stop)) {
        struct chunksrv *cs = atomic_load(&G.pub);
        if (!cs && G.cs) {
            struct cluster_self me;
            if (cluster_self(&me) == 0 && me.have_node) {
                if (!G.checked) {
                    check_late(&me);
                    G.checked = 1;
                }
                struct chunksrv_net_cfg nc = {.self_id = me.id, .secret = me.secret, .secret_len = me.secret_len,
                                              .rate = G.rate};
                memcpy(nc.uuid, me.uuid, 16);
                memcpy(nc.ident, me.ident, 16);
                snprintf(nc.advertise, sizeof nc.advertise, "%s", me.advertise);
                if (G.cs && chunksrv_listen(G.cs, &nc) == 0) {
                    cluster_set_peerlink_kind(PL_KIND_CHUNK, chunksrv_peerlink(G.cs));
                    cluster_set_peerlink_kind(PL_KIND_CHUNKB, chunksrv_peerlink(G.cs));
                    self = me.id;
                    cs = G.cs;
                    atomic_store(&G.pub, cs);
                    fprintf(stderr, "tsdbd: chunk server up as node %" PRIu32 ", %d stores\n", me.id,
                            chunksrv_stores(cs, NULL, false));
                } else if (G.cs) {
                    fprintf(stderr, "tsdbd: chunk server: cannot make its links (tried again)\n");
                }
                tsdb_wipe(&me, sizeof me);
                tsdb_wipe(&nc, sizeof nc);
            }
        }
        if (cs) {
            unsigned long b = cluster_copy_builds();
            if (!have || b != seen) {
                const struct cluster_copy *c = cluster_copy_get();
                if (c) {
                    set_peers(cs, c, self);
                    cluster_copy_put(c);
                    seen = b;
                    have = 1;
                    if (getenv("TSDB_CHUNKSRV_TEST") && !tested) /* on its own thread: peers keep moving */
                        tested = pthread_create(&st_thr, NULL, selftest_main, cs) == 0;
                }
            }
            report_up(cs);
        }
        nanosleep(&(struct timespec){0, 100 * 1000000L}, NULL);
    }
    if (tested)
        pthread_join(st_thr, NULL);
    return NULL;
}

int chunksrv_cluster_start(const char *const *paths, int n, uint64_t rate, char *err, size_t errlen)
{
    if (G.started)
        return 0;
    G.cs = chunksrv_open(paths, n, err, errlen);
    if (!G.cs)
        return -1;
    chunksrv_stores(G.cs, NULL, n > 0);
    struct cluster_self me;
    if (cluster_self(&me) == 0 && me.have_node) { /* D11: known now, so a foreign store refuses the start */
        int rc = chunksrv_check_owner(G.cs, me.uuid, me.id, err, errlen);
        tsdb_wipe(&me, sizeof me);
        if (rc != 0) {
            chunksrv_close(G.cs);
            G.cs = NULL;
            return -1;
        }
        G.checked = 1;
    }
    G.rate = rate;
    G.last_up = -1;
    atomic_store(&G.stop, 0);
    if (pthread_create(&G.thr, NULL, cc_main, NULL) != 0) {
        snprintf(err, errlen, "cannot start the chunk server's thread");
        chunksrv_close(G.cs);
        G.cs = NULL;
        return -1;
    }
    G.started = 1;
    return 0;
}

void chunksrv_cluster_stop(void)
{
    if (!G.started)
        return;
    atomic_store(&G.stop, 1);
    pthread_join(G.thr, NULL);
    G.started = 0;
    if (atomic_load(&G.pub)) {
        /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket over any more */
        cluster_set_peerlink_kind(PL_KIND_CHUNK, NULL);
        cluster_set_peerlink_kind(PL_KIND_CHUNKB, NULL);
    }
}

/* Callers that are not tsdbd's workers (the engines' tier threads through chunkpeer: a settle's puts, a digest's or a
 * query's reads, asks an open stopped waiting for) hold the instance across their call, and free waits for them: the
 * engines are closed only after it (tsdb_close), so without the hold a settle in flight at SIGTERM called into a
 * freed chunk server. Seq_cst on both sides: a holder that loaded the instance is counted before free looks. */
static atomic_int holders;
static atomic_int closes; /* chunksrv_cluster_closes: tsdbd's test hook holdput tells a close under a put */

struct chunksrv *chunksrv_cluster_hold(void)
{
    atomic_fetch_add(&holders, 1);
    struct chunksrv *cs = atomic_load(&G.pub);
    if (!cs)
        atomic_fetch_sub(&holders, 1);
    return cs;
}

void chunksrv_cluster_release(void) { atomic_fetch_sub(&holders, 1); }

void chunksrv_cluster_free(void)
{
    atomic_store(&G.pub, NULL);
    while (atomic_load(&holders) > 0) { /* a call in flight ends by its deadline at the latest */
        struct timespec ts = {0, 1000000L};
        nanosleep(&ts, NULL);
    }
    atomic_fetch_add(&closes, 1); /* before the close: a holder that sees no change still has a live instance */
    chunksrv_close(G.cs); /* tsdbd's workers are joined, and no holder is left; unlistens first */
    G.cs = NULL;
    G.checked = 0;
}

int chunksrv_cluster_closes(void) { return atomic_load(&closes); }

struct chunksrv *chunksrv_cluster(void) { return atomic_load(&G.pub); }
