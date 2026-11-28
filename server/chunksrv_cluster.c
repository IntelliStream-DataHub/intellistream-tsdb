/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's chunk server (chunksrv.h) on the cluster, as fwd_cluster.c does it for the forwarding links: the chunk stores
 * selected by server/disks.[ch] (`<disk>/chunks/`) are opened at start (each one's flock: this process is their one
 * owner; a store another process holds refuses the start, a missing, damaged or failing one is DOWN and logged), checked
 * against this member's cluster and node (`<disk>/DISK`), and a thread
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
#include "creport.h"
#include "disks.h"
#include "peerlink.h"
#include "../client/tsdb_crypto.h"
#include "../src/chunk_priv.h"
#include "../src/chunkmove.h"
#include "../src/chunkscrub.h"
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
    _Atomic uint32_t scrub_days;
    uint32_t scrub_mb_s;
    uint32_t scrub_out_count;
    uint32_t scrub_interval_ms;
    int scrub_days_ov;
    int scrub_mb_s_ov;
    int scrub_out_count_ov;
    uint32_t applied_scrub_days;
    uint32_t applied_scrub_mb_s;
    uint32_t applied_scrub_out_count;
} G;

/* ---- R12 chunk scrub: CHUNK_DAMAGED delivery, heartbeat section, Prometheus metrics ---- */

static int cc_on_damage(void *ctx, const chunk_scrub_damage *dmg)
{
    (void)ctx;
    if (!dmg || atomic_load(&G.stop))
        return TSDB_ERR_UNAVAILABLE;

    uint32_t leader = 0, self = 0;
    uint64_t epoch = 0;
    bool vol_unknown = false;
    const struct cluster_copy *c = cluster_copy_get();
    if (c) {
        self = c->self;
        const struct cluster_copy_vol *cv = cluster_copy_vol(c, dmg->volume);
        if (!cv || cv->redundancy != META_RED_EC) {
            vol_unknown = true;
        } else if (cv->leader != 0) {
            leader = cv->leader;
            epoch = cv->epoch;
        }
        cluster_copy_put(c);
    }
    if (vol_unknown)
        return 0; /* volume removed or not EC: drop report (E14 orphan rules clean up, D9) */
    if (leader == 0)
        return TSDB_ERR_UNAVAILABLE;

    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;

    struct cs_req req = {
        .op = CS_OP_DAMAGED,
        .volume = dmg->volume,
        .epoch = epoch,
        .pos = dmg->pos,
        .bad_cells = dmg->bad_cells,
    };
    memcpy(req.oid, dmg->oid, 16);

    int32_t prc = TSDB_ERR_UNAVAILABLE;
    uint8_t *body = NULL;
    size_t bl = 0;
    int rc = (self && leader == self) ? chunksrv_call_self(cs, self, &req, &prc, &body, &bl)
                                      : chunksrv_call(cs, leader, &req, 500, &prc, &body, &bl);
    free(body);
    chunksrv_cluster_release();
    return (rc == 0 && prc == 0) ? 0 : TSDB_ERR_UNAVAILABLE;
}

static void cc_on_out(void *ctx, int store_idx, uint64_t day_damaged)
{
    (void)ctx;
    disks_chunk_damaged_out(store_idx, day_damaged);
}

static void cc_on_store_fault(void *ctx, int store_idx, int err_code)
{
    (void)ctx;
    disk_probe_chunk_failed(store_idx, err_code);
}

static int cc_put_allow(void *ctx, int store_idx, int is_rebuild)
{
    (void)ctx;
    return disks_chunk_store_can_put(store_idx, is_rebuild);
}

static void cc_on_ioerr(void *ctx, int store_idx)
{
    (void)ctx;
    disk_probe_now_chunk(store_idx);
}

static void cscrub_put32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static void cscrub_put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static uint32_t cscrub_get32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t cscrub_get64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = (v << 8) | p[i];
    return v;
}

#define CSCRUB_SEC_VER 1u
#define CSCRUB_SEC_HDR 4u
#define CSCRUB_SEC_ENTRY 40u

struct cscrub_sec_row {
    uint32_t store_idx;
    uint32_t state;
    uint64_t bytes_total;
    uint64_t chunks_total;
    uint64_t damaged_total;
    uint32_t day_damaged;
    uint32_t age_s;
};

struct cscrub_sec_val {
    uint32_t n;
    struct cscrub_sec_row rows[];
};

static ssize_t cscrub_sec_encode(uint8_t *buf, size_t cap)
{
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return 0;
    chunk_backend *be = chunksrv_backend(cs);
    int ns = be ? chunk_be_nstores(be) : 0;
    if (ns <= 0) {
        chunksrv_cluster_release();
        return 0;
    }
    if (ns > CHUNKSRV_DIRS_MAX)
        ns = CHUNKSRV_DIRS_MAX;
    size_t need = CSCRUB_SEC_HDR + (size_t)ns * CSCRUB_SEC_ENTRY;
    if (need > cap) {
        chunksrv_cluster_release();
        return -1;
    }
    buf[0] = CSCRUB_SEC_VER;
    buf[1] = (uint8_t)ns;
    buf[2] = 0;
    buf[3] = 0;
    for (int i = 0; i < ns; i++) {
        chunk_scrub_stats st;
        if (chunksrv_scrub_stats(cs, i, &st) != 0)
            memset(&st, 0, sizeof st);
        int bstate = chunk_be_store_state(be, i);
        uint32_t ustate = bstate >= 0 ? (uint32_t)bstate : 0u;
        uint32_t dd = st.day_damaged > UINT32_MAX ? UINT32_MAX : (uint32_t)st.day_damaged;
        uint32_t age = st.age_s > UINT32_MAX ? UINT32_MAX : (uint32_t)st.age_s;
        uint8_t *e = buf + CSCRUB_SEC_HDR + (size_t)i * CSCRUB_SEC_ENTRY;
        cscrub_put32(e, (uint32_t)i);
        cscrub_put32(e + 4, ustate);
        cscrub_put64(e + 8, st.bytes_total);
        cscrub_put64(e + 16, st.chunks_total);
        cscrub_put64(e + 24, st.damaged_total);
        cscrub_put32(e + 32, dd);
        cscrub_put32(e + 36, age);
    }
    chunksrv_cluster_release();
    return (ssize_t)need;
}

static int cscrub_sec_decode(uint32_t member_id, const uint8_t *data, size_t len, void **out_val)
{
    (void)member_id;
    if (len < CSCRUB_SEC_HDR || data[0] != CSCRUB_SEC_VER || data[2] != 0 || data[3] != 0)
        return -1;
    uint32_t n = data[1];
    if (n > CHUNKSRV_DIRS_MAX || len != CSCRUB_SEC_HDR + (size_t)n * CSCRUB_SEC_ENTRY)
        return -1;
    struct cscrub_sec_val *v = calloc(1, sizeof *v + (size_t)n * sizeof v->rows[0]);
    if (!v)
        return -1;
    v->n = n;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *e = data + CSCRUB_SEC_HDR + (size_t)i * CSCRUB_SEC_ENTRY;
        v->rows[i].store_idx = cscrub_get32(e);
        v->rows[i].state = cscrub_get32(e + 4);
        v->rows[i].bytes_total = cscrub_get64(e + 8);
        v->rows[i].chunks_total = cscrub_get64(e + 16);
        v->rows[i].damaged_total = cscrub_get64(e + 24);
        v->rows[i].day_damaged = cscrub_get32(e + 32);
        v->rows[i].age_s = cscrub_get32(e + 36);
    }
    *out_val = v;
    return 0;
}

static void cscrub_sec_free(void *val) { free(val); }

static size_t cscrub_sec_format(uint32_t member_id, const void *val, char *buf, size_t cap)
{
    if (!val || !buf || cap == 0)
        return 0;
    const struct cscrub_sec_val *v = val;
    uint32_t sd = atomic_load_explicit(&G.scrub_days, memory_order_relaxed);
    uint64_t warn_s = (uint64_t)(sd ? sd : CHUNKSCRUB_DEFAULT_DAYS) * 2ull * 86400ull;
    size_t off = 0;
    for (uint32_t i = 0; i < v->n && off + 1 < cap; i++) {
        const struct cscrub_sec_row *r = &v->rows[i];
        int n = snprintf(buf + off, cap - off,
                         "chunk_scrub node=%" PRIu32 " store=%" PRIu32 " state=%s chunks=%" PRIu64 " bytes=%" PRIu64
                         " damaged=%" PRIu64 " day_damaged=%" PRIu32 " age_s=%" PRIu32 "%s\n",
                         member_id, r->store_idx, chunk_store_state_name((int)r->state), r->chunks_total,
                         r->bytes_total, r->damaged_total, r->day_damaged, r->age_s,
                         (uint64_t)r->age_s > warn_s ? " warn=stale" : "");
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    buf[off] = '\0';
    return off;
}

enum cscrub_metric_kind {
    CSM_BYTES = 0,
    CSM_CHUNKS,
    CSM_DAMAGED,
    CSM_AGE,
};

static void cscrub_collect_metric(void *ctx, struct creport_sink *sink)
{
    int kind = (int)(intptr_t)ctx;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return;
    chunk_backend *be = chunksrv_backend(cs);
    int ns = be ? chunk_be_nstores(be) : 0;
    if (ns > CHUNKSRV_DIRS_MAX)
        ns = CHUNKSRV_DIRS_MAX;
    for (int i = 0; i < ns; i++) {
        chunk_scrub_stats st;
        if (chunksrv_scrub_stats(cs, i, &st) != 0)
            continue;
        char sbuf[16];
        snprintf(sbuf, sizeof sbuf, "%d", i);
        struct creport_label lbl = {"store", sbuf};
        switch (kind) {
        case CSM_BYTES:
            creport_sink_put_u64(sink, &lbl, 1, st.bytes_total);
            break;
        case CSM_CHUNKS:
            creport_sink_put_u64(sink, &lbl, 1, st.chunks_total);
            break;
        case CSM_DAMAGED:
            creport_sink_put_u64(sink, &lbl, 1, st.damaged_total);
            break;
        case CSM_AGE:
            creport_sink_put(sink, &lbl, 1, (double)st.age_s);
            break;
        default:
            break;
        }
    }
    chunksrv_cluster_release();
}

static void cc_collect_peerlink_bytes(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return;
    struct peerlink *pl = chunksrv_peerlink(cs);
    if (pl) {
        struct pl_stats ps;
        peerlink_stats(pl, &ps);
        struct creport_label l_out = {"dir", "out"};
        struct creport_label l_in = {"dir", "in"};
        struct creport_label l_bulk = {"dir", "bulk"};
        creport_sink_put_u64(sink, &l_out, 1, ps.bytes_out);
        creport_sink_put_u64(sink, &l_in, 1, ps.bytes_in);
        creport_sink_put_u64(sink, &l_bulk, 1, ps.bulk_bytes);
    }
    chunksrv_cluster_release();
}

static void cc_collect_srv_bytes(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return;
    struct chunksrv_stats st;
    chunksrv_get_stats(cs, &st);
    struct creport_label l_put = {"op", "put"};
    struct creport_label l_read = {"op", "read"};
    creport_sink_put_u64(sink, &l_put, 1, st.put_bytes);
    creport_sink_put_u64(sink, &l_read, 1, st.read_bytes);
    chunksrv_cluster_release();
}

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
        /* R9 (D2): OUT is reversible (a returning member keeps its node id and reconnects), while DESTROYED is final;
         * only DESTROYED or removed members are retired via chunksrv_keep_peers. */
        if (m->id == self || m->state >= META_DESTROYED)
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

/* The node became known after the start: bind unbound DISK metadata (D11/D12); a foreign/locked disk stops serving. */
static void check_late(const struct cluster_self *me)
{
    char err[512];
    if (disks_bind_late(me->uuid, me->id, err, sizeof err) == 0)
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
#ifdef TSDB_TEST_HOOKS
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
#endif

static void *cc_main(void *arg)
{
    (void)arg;
    unsigned long seen = 0;
    int have = 0;
#ifdef TSDB_TEST_HOOKS
    int tested = 0;
    pthread_t st_thr;
#endif
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
                    cluster_set_peerlink_kind(PL_KIND_CHUNKR, chunksrv_peerlink(G.cs));
                    self = me.id;
                    cs = G.cs;
                    atomic_store(&G.pub, cs);
                    int nst = chunksrv_stores(cs, NULL, false);
                    fprintf(stderr, "tsdbd: chunk server up as node %" PRIu32 ", %d stores\n", me.id, nst);
                    if (nst > 0) {
                        uint32_t sd = atomic_load_explicit(&G.scrub_days, memory_order_relaxed), sm = G.scrub_mb_s,
                                 so = G.scrub_out_count;
                        const struct cluster_copy *ic = cluster_copy_get();
                        if (ic) {
                            if (!G.scrub_days_ov && ic->scrub_days > 0) {
                                sd = ic->scrub_days;
                                atomic_store_explicit(&G.scrub_days, sd, memory_order_relaxed);
                            }
                            if (!G.scrub_mb_s_ov && ic->scrub_mb_s > 0)
                                sm = ic->scrub_mb_s;
                            if (!G.scrub_out_count_ov && ic->scrub_out_count > 0)
                                so = ic->scrub_out_count;
                            cluster_copy_put(ic);
                        }
                        struct chunksrv_scrub_cfg scfg = {
                            .scrub_days = sd,
                            .scrub_mb_s = sm,
                            .scrub_out_count = so,
                            .pass_interval_ms = G.scrub_interval_ms,
                            .on_damage = cc_on_damage,
                            .on_out = cc_on_out,
                            .ctx = NULL,
                        };
                        if (chunksrv_scrub_start(cs, &scfg) != 0)
                            fprintf(stderr, "tsdbd: chunk scrub: could not start store scrubbers\n");
                        G.applied_scrub_days = sd;
                        G.applied_scrub_mb_s = sm;
                        G.applied_scrub_out_count = so;
                    }
                } else if (G.cs) {
                    fprintf(stderr, "tsdbd: chunk server: cannot make its links (tried again)\n");
                }
                tsdb_wipe(&me, sizeof me);
                tsdb_wipe(&nc, sizeof nc);
            }
        }
        if (cs) {
            unsigned long b = cluster_copy_builds();
            static uint64_t adopt_next_ns = 0;
            static uint64_t adopt_backoff_ns = 500ULL * 1000000ULL;
            if (!have || b != seen) {
                adopt_backoff_ns = 500ULL * 1000000ULL;
                adopt_next_ns = 0;
                const struct cluster_copy *c = cluster_copy_get();
                if (c) {
                    set_peers(cs, c, self);
                    disks_sync_copy(c);
                    uint32_t nd = (!G.scrub_days_ov && c->scrub_days > 0) ? c->scrub_days : G.applied_scrub_days;
                    uint32_t nm = (!G.scrub_mb_s_ov && c->scrub_mb_s > 0) ? c->scrub_mb_s : G.applied_scrub_mb_s;
                    uint32_t no =
                        (!G.scrub_out_count_ov && c->scrub_out_count > 0) ? c->scrub_out_count : G.applied_scrub_out_count;
                    if (nd != G.applied_scrub_days || nm != G.applied_scrub_mb_s || no != G.applied_scrub_out_count) {
                        if (!G.scrub_days_ov && nd > 0)
                            atomic_store_explicit(&G.scrub_days, nd, memory_order_relaxed);
                        chunksrv_scrub_update_cfg(cs, G.scrub_days_ov ? 0 : nd, G.scrub_mb_s_ov ? 0 : nm,
                                                  G.scrub_out_count_ov ? 0 : no);
                        G.applied_scrub_days = nd;
                        G.applied_scrub_mb_s = nm;
                        G.applied_scrub_out_count = no;
                    }
                    cluster_copy_put(c);
                    seen = b;
                    have = 1;
#ifdef TSDB_TEST_HOOKS
                    if (getenv("TSDB_CHUNKSRV_TEST") && !tested) /* on its own thread: peers keep moving */
                        tested = pthread_create(&st_thr, NULL, selftest_main, cs) == 0;
#endif
                }
            } else if (disks_adopt_info(NULL) != 0) {
                struct timespec ats;
                clock_gettime(CLOCK_MONOTONIC, &ats);
                uint64_t anow = (uint64_t)ats.tv_sec * 1000000000ULL + (uint64_t)ats.tv_nsec;
                if (anow >= adopt_next_ns) {
                    const struct cluster_copy *c = cluster_copy_get();
                    if (c) {
                        disks_adopt_retry(c);
                        cluster_copy_put(c);
                    }
                    adopt_next_ns = anow + adopt_backoff_ns;
                    if (adopt_backoff_ns < 5000ULL * 1000000ULL)
                        adopt_backoff_ns *= 2;
                }
            }
            report_up(cs);
            chunksrv_scrub_reconcile(cs);
        }
        nanosleep(&(struct timespec){0, 100 * 1000000L}, NULL);
    }
#ifdef TSDB_TEST_HOOKS
    if (tested)
        pthread_join(st_thr, NULL);
#endif
    return NULL;
}

int chunksrv_cluster_start_ex(const char *const *paths, const int *prefs, const bool *draining, int n, uint64_t rate,
                              char *err, size_t errlen)
{
    if (G.started)
        return 0;
#ifdef TSDB_TEST_HOOKS
    const char *lmu = getenv("TSDB_LMOVE_MUTANT");
    if (lmu && *lmu) {
        int m = chunkmove_mutant_parse(lmu);
        if (m < 0) {
            snprintf(err, errlen, "TSDB_LMOVE_MUTANT=%s is not a planted bug", lmu);
            return -1;
        }
        chunkmove_mutant = m;
        fprintf(stderr, "tsdbd: chunk move MUTANT %s (tests only)\n", lmu);
    }
#endif
    G.cs = chunksrv_open_ex(paths, prefs, draining, n, err, errlen);
    if (!G.cs)
        return -1;
    chunksrv_set_store_fault(G.cs, cc_on_store_fault, NULL);
    chunksrv_set_put_allow(G.cs, cc_put_allow, NULL);
    if (chunksrv_backend(G.cs))
        chunk_be_set_ioerr_cb(chunksrv_backend(G.cs), cc_on_ioerr, NULL);
    chunksrv_stores(G.cs, NULL, n > 0);
    struct cluster_self me;
    if (cluster_self(&me) == 0 && me.have_node) {
        /* If this server joined between disks_start() and now, bind unbound DISK metadata immediately. */
        int brc = disks_bind_late(me.uuid, me.id, err, errlen);
        tsdb_wipe(&me, sizeof me);
        if (brc != 0) {
            chunksrv_close(G.cs);
            G.cs = NULL;
            return -1;
        }
        G.checked = 1;
    }
    G.rate = rate;
    G.last_up = -1;
    atomic_store_explicit(&G.scrub_days, CHUNKSCRUB_DEFAULT_DAYS, memory_order_relaxed);
    G.scrub_mb_s = CHUNKSCRUB_DEFAULT_MB_S;
    G.scrub_out_count = CHUNKSCRUB_DEFAULT_OUT_COUNT;
    G.scrub_interval_ms = 0;
    G.scrub_days_ov = 0;
    G.scrub_mb_s_ov = 0;
    G.scrub_out_count_ov = 0;
    G.applied_scrub_days = 0;
    G.applied_scrub_mb_s = 0;
    G.applied_scrub_out_count = 0;
#ifdef TSDB_TEST_HOOKS
    const char *te = getenv("TSDB_CHUNKSCRUB_TEST");
    if (te) {
        const char *p;
        unsigned v = 0;
        if ((p = strstr(te, "days=")) != NULL && sscanf(p, "days=%u", &v) == 1 && v > 0) {
            atomic_store_explicit(&G.scrub_days, v, memory_order_relaxed);
            G.scrub_days_ov = 1;
        }
        if ((p = strstr(te, "mb_s=")) != NULL && sscanf(p, "mb_s=%u", &v) == 1 && v > 0) {
            G.scrub_mb_s = v;
            G.scrub_mb_s_ov = 1;
        }
        if ((p = strstr(te, "out_count=")) != NULL && sscanf(p, "out_count=%u", &v) == 1 && v > 0) {
            G.scrub_out_count = v;
            G.scrub_out_count_ov = 1;
        }
        if ((p = strstr(te, "interval_ms=")) != NULL && sscanf(p, "interval_ms=%u", &v) == 1 && v > 0)
            G.scrub_interval_ms = v;
    }
    const char *mu = getenv("TSDB_CHUNKSCRUB_MUTANT");
    if (mu && *mu) {
        int m = chunkscrub_mutant_parse(mu);
        if (m < 0) {
            snprintf(err, errlen, "TSDB_CHUNKSCRUB_MUTANT=%s is not a planted bug", mu);
            chunksrv_close(G.cs);
            G.cs = NULL;
            return -1;
        }
        chunkscrub_mutant = m;
        fprintf(stderr, "tsdbd: chunk scrub MUTANT %s (tests only)\n", mu);
    }
#endif
    (void)creport_section(CREPORT_SEC_CHUNKSCRUB, cscrub_sec_encode, cscrub_sec_decode, cscrub_sec_free,
                          cscrub_sec_format);
    (void)creport_metric_collector("tsdb_chunk_scrub_bytes_total", CREPORT_COUNTER,
                                   "Total chunk bytes verified by background scrub on this local store.",
                                   cscrub_collect_metric, (void *)(intptr_t)CSM_BYTES);
    (void)creport_metric_collector("tsdb_chunk_scrub_chunks_total", CREPORT_COUNTER,
                                   "Total chunks verified by background scrub on this local store.",
                                   cscrub_collect_metric, (void *)(intptr_t)CSM_CHUNKS);
    (void)creport_metric_collector("tsdb_chunk_scrub_damaged_total", CREPORT_COUNTER,
                                   "Total damaged chunks detected by background scrub on this local store.",
                                   cscrub_collect_metric, (void *)(intptr_t)CSM_DAMAGED);
    (void)creport_metric_collector("tsdb_chunk_scrub_age_seconds", CREPORT_GAUGE,
                                   "Seconds since the start of the last completed whole chunk scrub pass (or first pass).",
                                   cscrub_collect_metric, (void *)(intptr_t)CSM_AGE);
    (void)creport_metric_collector("tsdb_chunk_peerlink_bytes_total", CREPORT_COUNTER,
                                   "Bytes sent/received/bulk on this server's chunk peerlink.",
                                   cc_collect_peerlink_bytes, NULL);
    (void)creport_metric_collector("tsdb_chunk_srv_bytes_total", CREPORT_COUNTER,
                                   "Chunk bytes served over network puts and reads by this server.",
                                   cc_collect_srv_bytes, NULL);
    atomic_store(&G.stop, 0);
    if (pthread_create(&G.thr, NULL, cc_main, NULL) != 0) {
        snprintf(err, errlen, "cannot start the chunk server's thread");
        creport_section_unregister(CREPORT_SEC_CHUNKSCRUB);
        creport_metric_collector_del("tsdb_chunk_scrub_bytes_total");
        creport_metric_collector_del("tsdb_chunk_scrub_chunks_total");
        creport_metric_collector_del("tsdb_chunk_scrub_damaged_total");
        creport_metric_collector_del("tsdb_chunk_scrub_age_seconds");
        creport_metric_collector_del("tsdb_chunk_peerlink_bytes_total");
        creport_metric_collector_del("tsdb_chunk_srv_bytes_total");
        chunksrv_close(G.cs);
        G.cs = NULL;
        return -1;
    }
    G.started = 1;
    return 0;
}

int chunksrv_cluster_start(const char *const *paths, int n, uint64_t rate, char *err, size_t errlen)
{
    return chunksrv_cluster_start_ex(paths, NULL, NULL, n, rate, err, errlen);
}

void chunksrv_cluster_stop(void)
{
    if (!G.started)
        return;
    atomic_store(&G.stop, 1);
    pthread_join(G.thr, NULL);
    G.started = 0;
    if (G.cs)
        chunksrv_scrub_stop(G.cs);
    creport_section_unregister(CREPORT_SEC_CHUNKSCRUB);
    creport_metric_collector_del("tsdb_chunk_scrub_bytes_total");
    creport_metric_collector_del("tsdb_chunk_scrub_chunks_total");
    creport_metric_collector_del("tsdb_chunk_scrub_damaged_total");
    creport_metric_collector_del("tsdb_chunk_scrub_age_seconds");
    creport_metric_collector_del("tsdb_chunk_peerlink_bytes_total");
    creport_metric_collector_del("tsdb_chunk_srv_bytes_total");
    if (atomic_load(&G.pub)) {
        /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket over any more */
        cluster_set_peerlink_kind(PL_KIND_CHUNK, NULL);
        cluster_set_peerlink_kind(PL_KIND_CHUNKB, NULL);
        cluster_set_peerlink_kind(PL_KIND_CHUNKR, NULL);
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
    if (G.cs) {
        chunksrv_set_store_fault(G.cs, NULL, NULL);
        if (chunksrv_backend(G.cs))
            chunk_be_set_ioerr_cb(chunksrv_backend(G.cs), NULL, NULL);
    }
    chunksrv_close(G.cs); /* tsdbd's workers are joined, and no holder is left; unlistens first */
    G.cs = NULL;
    G.checked = 0;
}

int chunksrv_cluster_closes(void) { return atomic_load(&closes); }

struct chunksrv *chunksrv_cluster(void) { return atomic_load(&G.pub); }
