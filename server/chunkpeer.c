/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cold operations of a cluster ec volume over its stripe group (server/chunkpeer.h has the design). The context
 * holds the volume and its vuuid only: the stripe group is read from the copy at every put, open and listing (E9: a
 * position moves only by STRIPE_SET, and r8g checks at the marker that the version it put by is still the copy's). An
 * object handle copies what it reads through (the transport, the members, the sums) so that it never touches the
 * context after obj_open, as src/coldops.h requires.
 */
#define _GNU_SOURCE
#include "chunkpeer.h"

#include "../include/tsdb.h"
#include "../src/compat.h"
#include "../src/eccold.h"
#include "peerlink.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
#include <time.h>
#include <unistd.h>

int chunkpeer_mutant;
uint32_t chunkpeer_test_list_max = CS_LIST_MAX;
#ifdef TSDB_TEST_HOOKS
int64_t (*chunkpeer_test_now_ms)(void);
int chunkpeer_test_avoid_ms;
int chunkpeer_test_serial_put;
#endif

typedef struct cp_job {
    void (*fn)(void *);
    void *arg;
    struct cp_job *next;
} cp_job;

static int pool_submit(cp_job *j);

int chunkpeer_mutant_parse(const char *name)
{
    static const char *const names[] = {"none",        "ownepoch",   "nosumcmp",   "staleraise",  "delbeforegc",
                                        "sticky",      "unsuredown", "hedgeall",   "noavoid",     "hedgecorrupt"};
    for (size_t i = 0; i < sizeof names / sizeof *names; i++)
        if (!strcmp(name, names[i]))
            return (int)i;
    return -1;
}

static void put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}
static uint64_t get64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}
static uint32_t get32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint16_t get16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }

static const char *err_name(int rc)
{
    switch (rc) {
    case CHUNK_ERR_FENCED: return "fenced: below the member's epoch";
    case CHUNK_ERR_EPOCH_UNSURE: return "the member's store is unsure of its epoch";
    case CHUNK_ERR_FULL: return "the member's store is full";
    case CHUNK_ERR_STORE_ID: return "the member's store has another id";
    case CHUNK_ERR_BUSY: return "the member's store is busy";
    case TSDB_ERR_UNAVAILABLE: return "the member does not answer";
    default: return tsdb_strerror(rc);
    }
}

static int timeout_of(const struct chunkpeer *cp) { return cp->timeout_ms > 0 ? cp->timeout_ms : 30000; }

/* ---------------------------------------------------------------- the epoch (D9) */
static uint64_t own_fence(const struct chunkpeer *cp, uint32_t volume)
{
    chunk_backend *be = cp->local ? chunksrv_backend(cp->local) : NULL;
    return be ? chunk_be_epoch(be, volume) : 0;
}

uint64_t chunkpeer_epoch(void *arg, uint32_t volume)
{
    struct chunkpeer *cp = arg;
    if (chunkpeer_mutant == CP_MUT_OWNEPOCH)
        return own_fence(cp, volume); /* planted: what the stores (the writer) last saw */
    struct chunkpeer_view v;
    return cp->view(cp->view_arg, volume, &v) == 0 ? v.epoch : 0;
}

int chunkpeer_epoch_source(void *arg, uint32_t volume, uint64_t *epoch)
{
    struct chunkpeer *cp = arg;
    if (chunkpeer_mutant == CP_MUT_OWNEPOCH) {
        uint64_t e = own_fence(cp, volume); /* planted: an unsure store raised to its own surviving slot */
        if (e == 0)
            return 0;
        *epoch = e;
        return 1;
    }
    struct chunkpeer_view v;
    if (cp->view(cp->view_arg, volume, &v) != 0 || v.epoch == 0)
        return 0;
    if (!v.caught_up && chunkpeer_mutant != CP_MUT_STALERAISE)
        return 0; /* another leader may have committed a higher epoch that this copy has not applied */
    *epoch = v.epoch;
    return 1;
}

/* ---------------------------------------------------------------- the context */
typedef struct pctx {
    struct chunkpeer *cp;
    uint32_t volume;
    uint8_t vuuid[16];
    atomic_bool unfenced;  /* a member did not hold the epoch when last asked: no put or delete (writable false) */
    atomic_bool del_said;  /* the refusal of deletes was logged */
    _Atomic uint64_t seen; /* the highest fence a member answered */
    _Atomic int64_t asked_ms; /* when the members were last asked again while unfenced (one asker a second) */
} pctx;

static bool p_writable(void *ctx);

int chunkpeer_make(void *arg, struct tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx)
{
    (void)t;
    pctx *p = calloc(1, sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    p->cp = arg;
    p->volume = volume;
    memcpy(p->vuuid, vuuid, 16);
    *ctx = p;
    return 0;
}

static int view_of(pctx *p, struct chunkpeer_view *v)
{
    int rc = p->cp->view(p->cp->view_arg, p->volume, v);
    if (rc)
        return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_UNAVAILABLE : rc;
    if (v->nstripe == 0 || v->nstripe > CHUNK_POS_MAX)
        return TSDB_ERR_UNAVAILABLE;
    return 0;
}

static void see_fence(pctx *p, uint64_t e)
{
    uint64_t cur = atomic_load(&p->seen);
    while (e > cur && !atomic_compare_exchange_weak(&p->seen, &cur, e))
        ;
}

/* ---------------------------------------------------------------- put */
typedef struct stage {
    const chunk_geom *g;
    unsigned npos;
    int fd[CHUNK_POS_MAX];
} stage;

static int stage_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)sum;
    stage *s = ctx;
    if (pos >= s->npos)
        return TSDB_ERR_INVALID;
    if (s->fd[pos] < 0)
        return 0; /* E10: unwritten position of a degraded settle (sums still computed by chunk_encode_stream) */
    uint64_t off = 8 * chunk_pos_cells(s->g, pos) + idx * (uint64_t)s->g->cell;
    for (size_t done = 0; done < len;) {
        ssize_t w = pwrite(s->fd[pos], data + done, len - done, (off_t)(off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return errno == ENOSPC ? CHUNK_ERR_FULL : TSDB_ERR_IO;
        done += (size_t)w;
    }
    return 0;
}

static int stage_sums(int fd, const uint64_t *sums, uint64_t n)
{
    uint8_t buf[8 * 512];
    for (uint64_t i = 0; i < n;) {
        uint64_t c = n - i < 512 ? n - i : 512;
        for (uint64_t j = 0; j < c; j++)
            put64(buf + 8 * j, sums[i + j]);
        if (pwrite(fd, buf, 8 * c, (off_t)(8 * i)) != (ssize_t)(8 * c))
            return TSDB_ERR_IO;
        i += c;
    }
    return 0;
}

#define CP_MEMFD_MAX (16u << 20)

static int stage_open_fd(const char *stage_dir, bool use_memfd)
{
#if defined(__linux__) && defined(SYS_memfd_create)
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
    if (use_memfd) {
        int fd = (int)syscall(SYS_memfd_create, "tsdb-chunkstage", (unsigned)MFD_CLOEXEC);
        if (fd >= 0)
            return fd;
    }
#else
    (void)use_memfd;
#endif
    /* mkstemp and unlink, never O_TMPFILE: macOS has none, and NFS (tsdbd's root in production) refuses it */
    char path[PATH_MAX + 32];
    snprintf(path, sizeof path, "%s/chunkstage.XXXXXX", stage_dir);
    int fd = mkstemp(path);
    if (fd >= 0) {
        unlink(path);
        (void)fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    return fd;
}

typedef struct put_batch put_batch;
typedef struct put_pos_job {
    cp_job job;
    put_batch *b;
    unsigned i;
} put_pos_job;

struct put_batch {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    unsigned done;
    struct chunkpeer *cp;
    uint32_t volume;
    uint64_t epoch;
    const uint8_t *vuuid;
    const uint8_t *oid;
    const chunk_geom *g;
    const uint32_t *stripe;
    const int *fds;
    const uint64_t *sums;
    int rc[CHUNK_POS_MAX];
    uint64_t answered[CHUNK_POS_MAX];
    uint64_t sum_hash[CHUNK_POS_MAX];
    uint8_t meta[CHUNK_POS_MAX][CS_PUT_META_LEN];
    put_pos_job jobs[CHUNK_POS_MAX];
};

static void put_pos_run(put_batch *b, unsigned i)
{
    struct cs_put_meta m = {.volume = b->volume, .epoch = b->epoch, .pos = (uint16_t)i, .g = *b->g};
    memcpy(m.vuuid, b->vuuid, 16);
    memcpy(m.oid, b->oid, 16);
    uint64_t sum = cs_sums_hash(b->sums + chunk_sums_base(b->g, i), chunk_pos_cells(b->g, i)), answered = 0;
    cs_put_meta_encode(&m, b->meta[i]);
    b->sum_hash[i] = sum;
    b->rc[i] = b->cp->net.put(b->cp->net.arg, b->stripe[i], &m, b->fds[i], 0, sum, timeout_of(b->cp), &answered);
    b->answered[i] = answered;
}

static void put_pos_work(void *arg)
{
    put_pos_job *j = arg;
    put_batch *b = j->b;
    put_pos_run(b, j->i);
    pthread_mutex_lock(&b->mu);
    b->done++;
    pthread_cond_signal(&b->cv);
    pthread_mutex_unlock(&b->mu);
}

static int put_check_pos(pctx *p, const put_batch *b, unsigned i)
{
    int r = b->rc[i];
    /* the answer covers the identity the member took (volume, epoch, position, oid) as well as the sums */
    if (r == 0 && b->answered[i] != cs_put_answer_hash(b->meta[i], b->sum_hash[i]) &&
        chunkpeer_mutant != CP_MUT_NOSUMCMP) {
        fprintf(stderr, "tsdbd: volume %" PRIu32 ": member %" PRIu32 " answered the put of position %u with "
                        "another sum than the chunk's: the put fails\n", p->volume, b->stripe[i], i);
        r = TSDB_ERR_IO;
    }
    if (r)
        fprintf(stderr, "tsdbd: volume %" PRIu32 ": the put of position %u on member %" PRIu32 " failed (%s): "
                        "the settle fails\n", p->volume, i, b->stripe[i], err_name(r));
    else if (b->epoch)
        see_fence(p, b->epoch);
    return r;
}

static int p_put_stream_pos(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                            const chunk_put_src *src, uint32_t posmask, uint32_t *written_mask_out,
                            uint64_t *sums_out)
{
    pctx *p = ctx;
    struct chunkpeer *cp = p->cp;
    if (written_mask_out)
        *written_mask_out = 0;
    if (!p_writable(p))
        return TSDB_ERR_UNAVAILABLE;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    unsigned npos = chunk_npos(g);
    if (npos > v.nstripe || g->m != v.m || g->k > v.k) {
        fprintf(stderr, "tsdbd: volume %" PRIu32 ": an object of %u+%u positions does not fit the stripe group of the "
                        "metadata copy (%u+%u): the put is refused\n", p->volume, g->k, g->m, v.k, v.m);
        return TSDB_ERR_INVALID;
    }
    uint32_t full = npos < 32 ? ((1u << npos) - 1u) : ~0u;
    uint32_t target = posmask ? (posmask & full) : full;
    unsigned ntarget = (unsigned)__builtin_popcount(target);
    if (ntarget != npos) {
        unsigned max_skip = eccold_mutant == ECCOLD_MUT_DEGTWO ? 2u : 1u;
        if (g->m < 2 || ntarget < g->k || npos - ntarget > max_skip)
            return TSDB_ERR_UNAVAILABLE;
    }
    uint64_t *own = NULL;
    if (!sums_out && !(sums_out = own = malloc(chunk_sums_total(g) * sizeof *own)))
        return TSDB_ERR_NOMEM;
    uint64_t total_stage = 0;
    for (unsigned i = 0; i < npos; i++)
        if (target & (1u << i))
            total_stage += 8 * chunk_pos_cells(g, i) + chunk_pos_len(g, i);
    bool use_memfd = total_stage <= CP_MEMFD_MAX;
    stage s = {.g = g, .npos = npos};
    for (unsigned i = 0; i < npos; i++)
        s.fd[i] = -1;
    for (unsigned i = 0; i < npos && rc == 0; i++) {
        if (!(target & (1u << i)))
            continue;
        s.fd[i] = stage_open_fd(cp->stage_dir, use_memfd);
        if (s.fd[i] < 0) {
            fprintf(stderr, "tsdbd: volume %" PRIu32 ": a put cannot be staged in %s: %s\n", p->volume, cp->stage_dir,
                    strerror(errno));
            rc = TSDB_ERR_IO;
        }
    }
    if (rc == 0)
        rc = chunk_encode_stream(g, src, stage_cell, &s, sums_out);
    for (unsigned i = 0; i < npos && rc == 0; i++)
        if (target & (1u << i))
            rc = stage_sums(s.fd[i], sums_out + chunk_sums_base(g, i), chunk_pos_cells(g, i));
    /* Upload all targeted positions concurrently (k'+m for full, k'+m-1 for degraded E10); every targeted position
     * must commit before the settle goes on. */
    if (rc == 0 && ntarget > 0) {
        unsigned todo[CHUNK_POS_MAX], ntodo = 0;
        for (unsigned i = 0; i < npos; i++)
            if (target & (1u << i))
                todo[ntodo++] = i;
        put_batch b;
        memset(&b, 0, sizeof b);
        pthread_mutex_init(&b.mu, NULL);
        pthread_cond_init(&b.cv, NULL);
        b.cp = cp;
        b.volume = p->volume;
        b.epoch = epoch;
        b.vuuid = p->vuuid;
        b.oid = oid;
        b.g = g;
        b.stripe = v.stripe;
        b.fds = s.fd;
        b.sums = sums_out;
        unsigned start = 0;
#ifdef TSDB_TEST_HOOKS
        if (chunkpeer_test_serial_put) {
            put_pos_run(&b, todo[0]);
            rc = put_check_pos(p, &b, todo[0]);
            start = 1;
        }
#endif
        if (rc == 0 && start < ntodo) {
            unsigned submitted = 0;
            bool inline_run[CHUNK_POS_MAX] = {false};
            inline_run[start] = true;
            for (unsigned t = start + 1; t < ntodo; t++) {
                unsigned i = todo[t];
                b.jobs[i] = (put_pos_job){.job = {.fn = put_pos_work, .arg = &b.jobs[i]}, .b = &b, .i = i};
                if (pool_submit(&b.jobs[i].job) == 0)
                    submitted++;
                else
                    inline_run[t] = true;
            }
            for (unsigned t = start; t < ntodo; t++)
                if (inline_run[t])
                    put_pos_run(&b, todo[t]);
            if (submitted > 0) {
                pthread_mutex_lock(&b.mu);
                while (b.done < submitted)
                    pthread_cond_wait(&b.cv, &b.mu);
                pthread_mutex_unlock(&b.mu);
            }
            for (unsigned t = start; t < ntodo; t++) {
                int r = put_check_pos(p, &b, todo[t]);
                if (r && rc == 0)
                    rc = r;
            }
        }
        pthread_cond_destroy(&b.cv);
        pthread_mutex_destroy(&b.mu);
    }
    if (rc == 0 && written_mask_out)
        *written_mask_out = target;
    for (unsigned i = 0; i < npos; i++)
        if (s.fd[i] >= 0)
            close(s.fd[i]);
    free(own);
    return rc;
}

static int p_put_stream(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                        const chunk_put_src *src, uint64_t *sums_out)
{
    return p_put_stream_pos(ctx, oid, g, epoch, src, 0, NULL, sums_out);
}

/* ---------------------------------------------------------------- per-server latency, hedging & read-avoid (E31) */
#define CP_PEERS_MAX CHUNKPEER_PEERS_MAX
#define CP_LAT_WIN 128        /* sliding read latency window (samples per peer) */
#define CP_HEDGE_MIN_MS 20    /* E31: hedge timer floor 20 ms */
#define CP_HEDGE_MAX_MS 500   /* E31: hedge timer ceiling 500 ms */
#define CP_BUCKET_COST 20     /* 1 hedge costs 20 read credits -> 5% cap */
#define CP_BUCKET_CAP 20      /* max 1 hedge burst */
#define CP_BUCKET_INIT 19     /* read #1 brings tokens to 20 -> 5.0% exact over any 20*N reads */
#define CP_AVOID_THRESH 8     /* E31: 8 timeouts in 10 s -> read-avoid */
#define CP_AVOID_WIN_MS 10000 /* E31: 10 s timeout window */
#define CP_AVOID_DUR_MS 30000 /* E31: 30 s read-avoid duration */

typedef struct cp_peer {
    uint32_t node;
    uint32_t lat_us[CP_LAT_WIN];
    unsigned lat_n, lat_pos;
    uint32_t p99_us;
    uint32_t hedge_ms;
    int tokens;
    int64_t tmo_ms[CP_AVOID_THRESH];
    unsigned tmo_n, tmo_pos;
    int64_t avoid_until_ms;
    uint64_t reads, hedges, avoids, decoded_cells;
} cp_peer;

static pthread_mutex_t cp_mu = PTHREAD_MUTEX_INITIALIZER;
static cp_peer cp_peers[CP_PEERS_MAX];
static size_t cp_npeers;

static int64_t cp_now_ms(void)
{
#ifdef TSDB_TEST_HOOKS
    if (chunkpeer_test_now_ms)
        return chunkpeer_test_now_ms();
#endif
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static uint64_t cp_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static cp_peer *cp_peer_find_locked(uint32_t node)
{
    for (size_t i = 0; i < cp_npeers; i++)
        if (cp_peers[i].node == node)
            return &cp_peers[i];
    return NULL;
}

static cp_peer *cp_peer_get_locked(uint32_t node)
{
    cp_peer *p = cp_peer_find_locked(node);
    if (p)
        return p;
    if (cp_npeers >= CP_PEERS_MAX)
        return NULL;
    p = &cp_peers[cp_npeers++];
    memset(p, 0, sizeof *p);
    p->node = node;
    p->hedge_ms = CP_HEDGE_MIN_MS;
    p->tokens = CP_BUCKET_INIT;
    return p;
}

static void cp_peer_record_lat(uint32_t node, uint32_t us)
{
    pthread_mutex_lock(&cp_mu);
    cp_peer *p = cp_peer_get_locked(node);
    if (p) {
        p->lat_us[p->lat_pos] = us;
        p->lat_pos = (p->lat_pos + 1) % CP_LAT_WIN;
        if (p->lat_n < CP_LAT_WIN)
            p->lat_n++;
        /* p99 of up to 128 samples: index ceil(0.99 * n) - 1 is max1 for n < 100, max2 for 100..128 */
        uint32_t m1 = 0, m2 = 0;
        for (unsigned i = 0; i < p->lat_n; i++) {
            uint32_t v = p->lat_us[i];
            if (v >= m1) {
                m2 = m1;
                m1 = v;
            } else if (v > m2) {
                m2 = v;
            }
        }
        p->p99_us = p->lat_n >= 100 ? m2 : m1;
        uint64_t hms = ((uint64_t)p->p99_us * 2u + 999u) / 1000u;
        if (hms < CP_HEDGE_MIN_MS)
            hms = CP_HEDGE_MIN_MS;
        if (hms > CP_HEDGE_MAX_MS)
            hms = CP_HEDGE_MAX_MS;
        p->hedge_ms = (uint32_t)hms;
    }
    pthread_mutex_unlock(&cp_mu);
}

static uint32_t cp_peer_begin_read(uint32_t node, bool credit_bucket)
{
    uint32_t hms = CP_HEDGE_MIN_MS;
    pthread_mutex_lock(&cp_mu);
    cp_peer *p = cp_peer_get_locked(node);
    if (p) {
        p->reads++;
        if (credit_bucket && p->tokens < CP_BUCKET_CAP)
            p->tokens++;
        hms = p->hedge_ms;
    }
    pthread_mutex_unlock(&cp_mu);
    return hms;
}

static bool cp_peer_reserve_hedge(uint32_t node)
{
    bool ok = false;
    pthread_mutex_lock(&cp_mu);
    cp_peer *p = cp_peer_get_locked(node);
    if (p) {
        if (chunkpeer_mutant == CP_MUT_HEDGEALL) {
            ok = true;
        } else if (p->tokens >= CP_BUCKET_COST) {
            p->tokens -= CP_BUCKET_COST;
            ok = true;
        }
    }
    pthread_mutex_unlock(&cp_mu);
    return ok;
}

static void cp_peer_commit_hedge(uint32_t node)
{
    pthread_mutex_lock(&cp_mu);
    cp_peer *p = cp_peer_get_locked(node);
    if (p)
        p->hedges++;
    pthread_mutex_unlock(&cp_mu);
}

static void cp_peer_cancel_hedge(uint32_t node)
{
    if (chunkpeer_mutant == CP_MUT_HEDGEALL)
        return;
    pthread_mutex_lock(&cp_mu);
    cp_peer *p = cp_peer_find_locked(node);
    if (p) {
        p->tokens += CP_BUCKET_COST;
        if (p->tokens > CP_BUCKET_CAP)
            p->tokens = CP_BUCKET_CAP;
    }
    pthread_mutex_unlock(&cp_mu);
}

static void cp_peer_record_timeout(uint32_t node)
{
    pthread_mutex_lock(&cp_mu);
    int64_t now = cp_now_ms();
    cp_peer *p = cp_peer_get_locked(node);
    if (p) {
        if (p->avoid_until_ms > now) {
            pthread_mutex_unlock(&cp_mu);
            return;
        }
        if (p->avoid_until_ms > 0) {
            p->avoid_until_ms = 0;
            p->tmo_n = 0;
            p->tmo_pos = 0;
            fprintf(stderr, "tsdbd: chunk peer %" PRIu32 ": read-avoid expired: resuming direct reads\n", node);
        }
        p->tmo_ms[p->tmo_pos] = now;
        p->tmo_pos = (p->tmo_pos + 1) % CP_AVOID_THRESH;
        if (p->tmo_n < CP_AVOID_THRESH)
            p->tmo_n++;
        if (p->tmo_n == CP_AVOID_THRESH && chunkpeer_mutant != CP_MUT_NOAVOID) {
            int64_t oldest = p->tmo_ms[p->tmo_pos];
            if (now - oldest <= CP_AVOID_WIN_MS) {
                int64_t dur = CP_AVOID_DUR_MS;
#ifdef TSDB_TEST_HOOKS
                if (chunkpeer_test_avoid_ms > 0)
                    dur = chunkpeer_test_avoid_ms;
#endif
                p->avoid_until_ms = now + dur;
                p->tmo_n = 0;
                p->tmo_pos = 0;
                fprintf(stderr, "tsdbd: chunk peer %" PRIu32 ": %d read timeouts in %d s: read-avoid for %lld s\n",
                        node, CP_AVOID_THRESH, CP_AVOID_WIN_MS / 1000, (long long)(dur / 1000));
            }
        }
    }
    pthread_mutex_unlock(&cp_mu);
}

static bool cp_peer_is_avoid(uint32_t node, bool count_avoid)
{
    if (chunkpeer_mutant == CP_MUT_NOAVOID)
        return false;
    bool av = false;
    pthread_mutex_lock(&cp_mu);
    int64_t now = cp_now_ms();
    cp_peer *p = cp_peer_find_locked(node);
    if (p && p->avoid_until_ms > 0) {
        if (now < p->avoid_until_ms) {
            av = true;
            if (count_avoid)
                p->avoids++;
        } else {
            p->avoid_until_ms = 0;
            p->tmo_n = 0;
            p->tmo_pos = 0;
            fprintf(stderr, "tsdbd: chunk peer %" PRIu32 ": read-avoid expired: resuming direct reads\n", node);
        }
    }
    pthread_mutex_unlock(&cp_mu);
    return av;
}

static void cp_peer_add_decoded(uint32_t node, uint64_t n)
{
    if (n == 0)
        return;
    pthread_mutex_lock(&cp_mu);
    cp_peer *p = cp_peer_get_locked(node);
    if (p)
        p->decoded_cells += n;
    pthread_mutex_unlock(&cp_mu);
}

size_t chunkpeer_peer_stats_get(struct chunkpeer_peer_stats *out, size_t cap)
{
    size_t n = 0;
    pthread_mutex_lock(&cp_mu);
    int64_t now = cp_now_ms();
    for (size_t i = 0; i < cp_npeers && n < cap; i++) {
        const cp_peer *p = &cp_peers[i];
        out[n++] = (struct chunkpeer_peer_stats){
            .peer = p->node,
            .reads = p->reads,
            .hedges = p->hedges,
            .avoids = p->avoids,
            .decoded_cells = p->decoded_cells,
            .hedge_ms = p->hedge_ms,
            .p99_ms = (double)p->p99_us / 1000.0,
            .read_avoid = (chunkpeer_mutant != CP_MUT_NOAVOID && p->avoid_until_ms > now) ? 1 : 0,
        };
    }
    pthread_mutex_unlock(&cp_mu);
    return n;
}

void chunkpeer_hedge_reset(void)
{
    pthread_mutex_lock(&cp_mu);
    cp_npeers = 0;
    memset(cp_peers, 0, sizeof cp_peers);
    pthread_mutex_unlock(&cp_mu);
}

/* ---------------------------------------------------------------- read */
struct cold_obj {
    struct chunkpeer_net net;
    int timeout_ms;
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    chunk_geom g;
    unsigned npos;
    uint32_t positions;          /* R9 (E10): written-positions bitmap (0 = all k'+m) */
    uint32_t node[CHUNK_POS_MAX];
    int st[CHUNK_POS_MAX];       /* the open's status of each position */
    bool avoided[CHUNK_POS_MAX]; /* skipped at open because its peer was in read-avoid */
    uint64_t *sums[CHUNK_POS_MAX]; /* each position's own table (status OK) */
    uint64_t *rec;               /* the object record's sums, or NULL */
};

static void req_init(struct cs_req *r, uint8_t op, uint32_t volume, uint64_t epoch, const uint8_t vuuid[16],
                     const uint8_t oid[16], unsigned pos)
{
    memset(r, 0, sizeof *r);
    r->op = op;
    r->volume = volume;
    r->epoch = epoch;
    if (vuuid)
        memcpy(r->vuuid, vuuid, 16);
    if (oid)
        memcpy(r->oid, oid, 16);
    r->pos = (uint16_t)pos;
}

/* An answer's rc as a CHUNK_ST_* of a position: a member that did not answer is DOWN (it may well hold the chunk). */
static int st_of(int trc, int32_t rc)
{
    if (trc)
        return CHUNK_ST_DOWN;
    int st = chunksrv_st(rc);
    return st < 0 ? CHUNK_ST_IOERR : st;
}

/* Position pos's header and sums table: CHUNK_ST_OK with *g and *sums (malloc'ed) or a CHUNK_ST_*. With `expect`
 * the whole table is fetched and the header must name expect's geometry; without, one sum (the header alone). */
static int fetch_sums(const struct chunkpeer_net *net, int tmo, uint32_t node, uint32_t volume, uint64_t epoch,
                      const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos, const chunk_geom *expect,
                      chunk_geom *g, uint64_t **sums)
{
    *sums = NULL;
    uint64_t total = expect ? chunk_pos_cells(expect, pos) : 1, got = 0;
    uint64_t *t = expect ? malloc((total ? total : 1) * sizeof *t) : NULL;
    if (expect && !t)
        return CHUNK_ST_IOERR;
    int st = CHUNK_ST_OK;
    do {
        struct cs_req r;
        req_init(&r, CS_OP_SUMS, volume, epoch, vuuid, oid, pos);
        r.off = got;
        r.len = (uint32_t)(total - got < CS_SUMS_MAX ? total - got : CS_SUMS_MAX);
        int32_t rc = 0;
        uint8_t *b = NULL;
        size_t bn = 0;
        int trc = net->call(net->arg, node, &r, tmo, &rc, &b, &bn);
        if ((st = st_of(trc, rc)) != CHUNK_ST_OK) {
            free(b);
            break;
        }
        if (bn < 36) {
            st = CHUNK_ST_DAMAGED;
            free(b);
            break;
        }
        chunk_geom h;
        uint32_t n = get32(b + 32);
        if (chunk_geom_make(&h, get16(b + 8), get16(b + 10), get32(b + 12), get64(b + 16)) != 0 ||
            (expect && (h.k != expect->k || h.m != expect->m || h.cell != expect->cell || h.len != expect->len)) ||
            get64(b + 24) != chunk_pos_cells(&h, pos) || n != r.len || bn != 36 + 8 * (size_t)n) {
            st = CHUNK_ST_DAMAGED; /* another encoding of the object, or an answer that does not parse */
            free(b);
            break;
        }
        *g = h;
        for (uint32_t i = 0; t && i < n; i++)
            t[got + i] = get64(b + 36 + 8 * (size_t)i);
        got += n;
        free(b);
    } while (expect && got < total);
    if (st == CHUNK_ST_OK && expect)
        *sums = t;
    else
        free(t);
    return st;
}

/* ---------------------------------------------------------------- asking the positions at once */
/* Every position is asked on a thread of its own, so that one member that hangs (paused, a stuck disk: the link is up
 * and nothing answers) costs one deadline, not one per position, and an open that has its k' positions stops waiting
 * for it after FAN_GRACE_MS. A thread the caller stopped waiting for finishes on its own: what it shares with the
 * caller is the fan, reference-counted, and the transport, which outlives every context (tsdbd's holds the chunk
 * server while a call is in it). It never touches a context or an object handle. */
#define FAN_GRACE_MS 200

typedef struct ask {
    int st; /* SUMS: CHUNK_ST_*; EPOCH: the call's rc (0, or the error) */
    chunk_geom g;
    uint64_t *sums;
    uint64_t fence;
    bool unsure;
} ask;

typedef struct fan fan;
typedef struct fan_job {
    cp_job job;
    fan *f;
    unsigned i;
} fan_job;

struct fan {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int refs;
    unsigned n, done, ok;
    bool epoch_op; /* CS_OP_EPOCH at epoch, else CS_OP_SUMS (of the whole table with expect, else one sum) */
    struct chunkpeer_net net;
    int tmo;
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    bool has_expect;
    chunk_geom expect;
    uint32_t node[CHUNK_POS_MAX];
    unsigned pos[CHUNK_POS_MAX]; /* chunk position of each slot (default: i) */
    ask a[CHUNK_POS_MAX];
    bool fin[CHUNK_POS_MAX];
    fan_job job[CHUNK_POS_MAX];
};

static atomic_int fan_threads; /* asks and hedged reads still running, abandoned ones included (chunkpeer_quiesce) */

static void fan_one(fan *f, unsigned i, ask *out)
{
    memset(out, 0, sizeof *out);
    if (!f->epoch_op) {
        out->st = fetch_sums(&f->net, f->tmo, f->node[i], f->volume, f->epoch, f->vuuid, f->oid, f->pos[i],
                             f->has_expect ? &f->expect : NULL, &out->g, &out->sums);
        return;
    }
    struct cs_req r;
    req_init(&r, CS_OP_EPOCH, f->volume, f->epoch, NULL, NULL, 0);
    int32_t arc = 0;
    uint8_t *b = NULL;
    size_t bn = 0;
    int rc = f->net.call(f->net.arg, f->node[i], &r, f->tmo, &arc, &b, &bn);
    if (rc == 0 && arc != 0)
        rc = arc;
    if (rc == 0 && bn != 9)
        rc = TSDB_ERR_IO;
    if (rc == 0) {
        out->fence = get64(b);
        out->unsure = b[8] != 0;
    }
    free(b);
    out->st = rc;
}

static bool fan_ok(const fan *f, const ask *a) { return f->epoch_op ? a->st == 0 : a->st == CHUNK_ST_OK; }

static void fan_unref_locked(fan *f) /* unlocks f->mu; frees f at the last reference */
{
    bool last = --f->refs == 0;
    pthread_mutex_unlock(&f->mu);
    if (!last)
        return;
    for (unsigned i = 0; i < f->n; i++)
        free(f->a[i].sums);
    pthread_cond_destroy(&f->cv);
    pthread_mutex_destroy(&f->mu);
    free(f);
}

static void fan_record(fan *f, unsigned i, ask *a)
{
    pthread_mutex_lock(&f->mu);
    f->a[i] = *a;
    f->fin[i] = true;
    f->done++;
    f->ok += fan_ok(f, a);
    pthread_cond_broadcast(&f->cv);
}

static void fan_work(void *arg)
{
    fan_job *j = arg;
    fan *f = j->f;
    unsigned i = j->i;
    ask a;
    fan_one(f, i, &a);
    fan_record(f, i, &a);
    fan_unref_locked(f); /* may free f (and j embedded in f): never touch j after this */
}

/* Asks positions 0..n-1 of f and waits until all answered, or `need` (> 0) are OK and FAN_GRACE_MS more passed (0
 * grace with need 1: the first OK), or, with stop_on_fail, one failed. out[i] gets each answer (its sums move to the
 * caller), `unfinished` where none came in time. Consumes the caller's reference on f. */
static void fan_run(fan *f, unsigned need, int grace_ms, bool stop_on_fail, ask *out, int unfinished)
{
    pthread_mutex_init(&f->mu, NULL);
    tsdb_cond_init_mono(&f->cv);
    f->refs = 1 + (int)f->n;
    for (unsigned i = 0; i < f->n; i++) {
        f->job[i] = (fan_job){.job = {.fn = fan_work, .arg = &f->job[i]}, .f = f, .i = i};
        if (pool_submit(&f->job[i].job) != 0) { /* no thread: asked here, in turn */
            ask a;
            fan_one(f, i, &a);
            fan_record(f, i, &a);
            f->refs--;
            pthread_mutex_unlock(&f->mu);
        }
    }
    pthread_mutex_lock(&f->mu);
    bool armed = false;
    struct timespec dl;
    while (f->done < f->n && !(stop_on_fail && f->done > f->ok)) {
        if (need && f->ok >= need) {
            if (!armed) {
                tsdb_cond_deadline_ms(&dl, grace_ms);
                armed = true;
            }
            if (pthread_cond_timedwait(&f->cv, &f->mu, &dl) == ETIMEDOUT)
                break;
            continue;
        }
        pthread_cond_wait(&f->cv, &f->mu);
    }
    for (unsigned i = 0; i < f->n; i++) {
        if (f->fin[i]) {
            out[i] = f->a[i];
            f->a[i].sums = NULL;
        } else {
            memset(&out[i], 0, sizeof out[i]);
            out[i].st = unfinished;
        }
    }
    fan_unref_locked(f);
}

static fan *fan_new(const struct chunkpeer *cp, uint32_t volume, uint64_t epoch, const uint32_t *nodes, unsigned n)
{
    fan *f = calloc(1, sizeof *f);
    if (!f)
        return NULL;
    f->net = cp->net;
    f->tmo = timeout_of(cp);
    f->volume = volume;
    f->epoch = epoch;
    f->n = n;
    memcpy(f->node, nodes, n * sizeof *nodes);
    for (unsigned i = 0; i < n; i++)
        f->pos[i] = i;
    return f;
}

/* Cached pool of detached workers shared by p_put_stream, fan_run, and read_race primary/hedge tasks: avoids
 * pthread_create per healthy put/open/read while still spawning overflow threads when peers hang (never starves or
 * deadlocks on nested calls). */
#define CP_POOL_MAX 32
#define CP_POOL_IDLE_MS 5000

static pthread_mutex_t pool_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t pool_cv;
static pthread_once_t pool_once = PTHREAD_ONCE_INIT;
static cp_job *pool_head, *pool_tail;
static int pool_idle, pool_queued, pool_total;
static bool pool_draining;

static void pool_init_once(void) { tsdb_cond_init_mono(&pool_cv); }

static void *pool_oneshot_main(void *arg)
{
    cp_job *j = arg;
    void (*fn)(void *) = j->fn;
    void *a = j->arg;
    fn(a);
    atomic_fetch_sub(&fan_threads, 1);
    return NULL;
}

static void *pool_worker_main(void *arg)
{
    cp_job *j = arg;
    for (;;) {
        void (*fn)(void *) = j->fn;
        void *a = j->arg;
        fn(a); /* may free j (embedded in read_race): never touch j again */
        atomic_fetch_sub(&fan_threads, 1);
        pthread_mutex_lock(&pool_mu);
        if (pool_head) {
            j = pool_head;
            pool_head = j->next;
            if (!pool_head)
                pool_tail = NULL;
            pool_queued--;
            pthread_mutex_unlock(&pool_mu);
            continue;
        }
        if (pool_draining) {
            pool_total--;
            pthread_cond_broadcast(&pool_cv);
            pthread_mutex_unlock(&pool_mu);
            return NULL;
        }
        pool_idle++;
        struct timespec dl;
        tsdb_cond_deadline_ms(&dl, CP_POOL_IDLE_MS);
        while (!pool_head && !pool_draining) {
            if (pthread_cond_timedwait(&pool_cv, &pool_mu, &dl) == ETIMEDOUT)
                break;
        }
        pool_idle--;
        if (pool_head) {
            j = pool_head;
            pool_head = j->next;
            if (!pool_head)
                pool_tail = NULL;
            pool_queued--;
            pthread_mutex_unlock(&pool_mu);
            continue;
        }
        pool_total--;
        pthread_cond_broadcast(&pool_cv);
        pthread_mutex_unlock(&pool_mu);
        return NULL;
    }
}

static int pool_submit(cp_job *j)
{
    pthread_once(&pool_once, pool_init_once);
    j->next = NULL;
    atomic_fetch_add(&fan_threads, 1);
    pthread_mutex_lock(&pool_mu);
    if (!pool_draining && pool_idle > pool_queued) {
        if (pool_tail)
            pool_tail->next = j;
        else
            pool_head = j;
        pool_tail = j;
        pool_queued++;
        pthread_cond_signal(&pool_cv);
        pthread_mutex_unlock(&pool_mu);
        return 0;
    }
    bool pooled = !pool_draining && pool_total < CP_POOL_MAX;
    if (pooled)
        pool_total++;
    pthread_mutex_unlock(&pool_mu);

    pthread_attr_t at;
    pthread_t th;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
#ifdef __APPLE__
    pthread_attr_setstacksize(&at, TSDB_THREAD_STACK);
#endif
    int e = pthread_create(&th, &at, pooled ? pool_worker_main : pool_oneshot_main, j);
    pthread_attr_destroy(&at);
    if (e != 0) {
        if (pooled) {
            pthread_mutex_lock(&pool_mu);
            pool_total--;
            pthread_cond_broadcast(&pool_cv);
            pthread_mutex_unlock(&pool_mu);
        }
        atomic_fetch_sub(&fan_threads, 1);
        return -1;
    }
    return 0;
}

void chunkpeer_quiesce(void)
{
    pthread_once(&pool_once, pool_init_once);
    pthread_mutex_lock(&pool_mu);
    pool_draining = true;
    pthread_cond_broadcast(&pool_cv);
    while (pool_total > 0)
        pthread_cond_wait(&pool_cv, &pool_mu);
    pool_draining = false;
    pthread_mutex_unlock(&pool_mu);
    while (atomic_load(&fan_threads) > 0) {
        struct timespec ts = {0, 2 * 1000000L};
        nanosleep(&ts, NULL);
    }
}

/* Synchronous CS_OP_READ loop for one cell range: records read latency on success. */
static int direct_read_call(const struct chunkpeer_net *net, int tmo, uint32_t volume, uint64_t epoch,
                            const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos, uint32_t node, uint64_t off,
                            void *buf, size_t len, uint32_t *lat_us_out, bool *answered_out)
{
    uint64_t t0 = cp_now_us();
    bool answered = true;
    int st = CHUNK_ST_OK;
    for (size_t done = 0; done < len;) {
        struct cs_req r;
        req_init(&r, CS_OP_READ, volume, epoch, vuuid, oid, pos);
        r.off = off + done;
        r.len = (uint32_t)(len - done < CS_READ_MAX ? len - done : CS_READ_MAX);
        int32_t rc = 0;
        uint8_t *b = NULL;
        size_t bn = 0;
        int trc = net->call(net->arg, node, &r, tmo, &rc, &b, &bn);
        if (trc != 0)
            answered = false;
        st = st_of(trc, rc);
        if (st == CHUNK_ST_OK && bn != r.len)
            st = CHUNK_ST_IOERR; /* a short read is an error, never the end */
        if (st == CHUNK_ST_OK)
            memcpy((uint8_t *)buf + done, b, bn);
        free(b);
        if (st != CHUNK_ST_OK)
            break;
        done += r.len;
    }
    uint64_t dt = cp_now_us() - t0;
    uint32_t us = dt > UINT32_MAX ? UINT32_MAX : (uint32_t)dt;
    if (st == CHUNK_ST_OK && answered)
        cp_peer_record_lat(node, us);
    if (lat_us_out)
        *lat_us_out = us;
    if (answered_out)
        *answered_out = answered;
    return st;
}

typedef struct read_ctx {
    struct cold_obj *o;
    unsigned flags;
    bool allow_hedge;
    uint64_t c0, c1;           /* requested cell range [c0, c1] when allow_hedge is true */
    bool has_stripe;
    uint64_t cur_stripe;
    uint32_t seen_direct_mask; /* data positions already visited in the direct pass of cur_stripe */
    uint32_t skip_mask;        /* positions skipped for this entire read_ctx (set by hedge worker) */
    uint32_t bad_mask;         /* positions known bad in cur_stripe */
    int bad_st[CHUNK_POS_MAX]; /* failure status for positions in bad_mask */
    bool stripe_failed;        /* a failed hedge already proved cur_stripe has < k good positions */
    int stripe_fail_rc;        /* exact E15 error (UNAVAILABLE or CORRUPT) for cur_stripe */
    unsigned failed_direct[CHUNK_K_MAX];
    unsigned hedged_decoded;
    bool had_hedge_timeout;
    bool had_avoid;
} read_ctx;

static int src_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len);
static const uint64_t *src_sums(void *ctx, unsigned pos, int *status);

/* Reference-counted race between a primary cell read and a hedged decode from other positions (E31).
 * Self-contained: detached threads touch only this struct, net (process-lifetime), and cp_peers — never cold_obj. */
typedef struct read_race {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int refs;
    struct chunkpeer_net net;
    int timeout_ms;
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    chunk_geom g;
    unsigned npos;
    uint32_t positions;          /* R9 (E10): written-positions bitmap (0 = all k'+m) */
    uint32_t node[CHUNK_POS_MAX];
    int st_open[CHUNK_POS_MAX];
    bool avoided_open[CHUNK_POS_MAX];
    unsigned pos;
    uint64_t cell_off;
    uint64_t obj_off;
    size_t cell_len;
    unsigned flags;
    uint32_t skip_mask;
    /* Primary */
    cp_job pri_job;
    bool pri_done;
    int pri_st;
    uint8_t *pri_buf;
    /* Hedge */
    cp_job hedge_job;
    bool hedge_launched;
    bool hedge_done;
    int hedge_rc;
    uint8_t *hedge_buf;
    chunk_report hedge_rep;
    uint64_t *rec_copy;
    uint64_t *sums_copy[CHUNK_POS_MAX];
} read_race;

static void race_unref_locked(read_race *rr)
{
    bool last = --rr->refs == 0;
    pthread_mutex_unlock(&rr->mu);
    if (!last)
        return;
    free(rr->pri_buf);
    free(rr->hedge_buf);
    free(rr->rec_copy);
    for (unsigned i = 0; i < CHUNK_POS_MAX; i++)
        free(rr->sums_copy[i]);
    pthread_cond_destroy(&rr->cv);
    pthread_mutex_destroy(&rr->mu);
    free(rr);
}

static void race_primary_run(void *arg)
{
    read_race *rr = arg;
    int st = direct_read_call(&rr->net, rr->timeout_ms, rr->volume, rr->epoch, rr->vuuid, rr->oid, rr->pos,
                              rr->node[rr->pos], rr->cell_off, rr->pri_buf, rr->cell_len, NULL, NULL);
    pthread_mutex_lock(&rr->mu);
    rr->pri_st = st;
    rr->pri_done = true;
    pthread_cond_broadcast(&rr->cv);
    race_unref_locked(rr);
}

static void race_hedge_run(void *arg)
{
    read_race *rr = arg;
    struct cold_obj ho;
    memset(&ho, 0, sizeof ho);
    ho.net = rr->net;
    ho.timeout_ms = rr->timeout_ms;
    ho.volume = rr->volume;
    ho.epoch = rr->epoch;
    memcpy(ho.vuuid, rr->vuuid, 16);
    memcpy(ho.oid, rr->oid, 16);
    ho.g = rr->g;
    ho.npos = rr->npos;
    ho.positions = rr->positions;
    memcpy(ho.node, rr->node, sizeof ho.node);
    memcpy(ho.st, rr->st_open, sizeof ho.st);
    memcpy(ho.avoided, rr->avoided_open, sizeof ho.avoided);
    for (unsigned i = 0; i < CHUNK_POS_MAX; i++)
        ho.sums[i] = rr->sums_copy[i];
    ho.rec = rr->rec_copy;

    read_ctx hctx;
    memset(&hctx, 0, sizeof hctx);
    hctx.o = &ho;
    hctx.flags = rr->flags;
    hctx.allow_hedge = false;
    hctx.skip_mask = rr->skip_mask;

    chunk_source hsrc = {.ctx = &hctx, .read = src_read, .sums = src_sums, .positions = ho.positions};
    for (int i = 0; i < 16; i++)
        snprintf(hsrc.name + 2 * i, sizeof hsrc.name - 2 * (size_t)i, "%02x", rr->oid[i]);
    chunk_report rep;
    memset(&rep, 0, sizeof rep);
    int rc = chunk_read_range(&hsrc, &ho.g, ho.rec, rr->obj_off, rr->cell_len, rr->hedge_buf, rr->flags, &rep);

    pthread_mutex_lock(&rr->mu);
    rr->hedge_rc = rc;
    rr->hedge_rep = rep;
    rr->hedge_done = true;
    pthread_cond_broadcast(&rr->cv);
    race_unref_locked(rr);
}

/* Allocates optional sums copies for a non-RAW hedge (called before spawning race_hedge_run; primary never touches
 * hedge_* or *_copy fields). Returns 0 on success, -1 on OOM. */
static int race_prep_hedge(read_race *rr, const struct cold_obj *o, unsigned flags)
{
    rr->hedge_buf = malloc(rr->cell_len ? rr->cell_len : 1);
    if (!rr->hedge_buf)
        return -1;
    if (!(flags & CHUNK_RR_RAW)) {
        if (o->rec) {
            size_t n = (size_t)chunk_sums_total(&o->g);
            rr->rec_copy = malloc((n ? n : 1) * sizeof *rr->rec_copy);
            if (!rr->rec_copy)
                return -1;
            memcpy(rr->rec_copy, o->rec, n * sizeof *rr->rec_copy);
        } else {
            for (unsigned p = 0; p < o->npos; p++) {
                if (!o->sums[p])
                    continue;
                size_t n = (size_t)chunk_pos_cells(&o->g, p);
                rr->sums_copy[p] = malloc((n ? n : 1) * sizeof *rr->sums_copy[p]);
                if (!rr->sums_copy[p])
                    return -1;
                memcpy(rr->sums_copy[p], o->sums[p], n * sizeof *rr->sums_copy[p]);
            }
        }
    }
    return 0;
}

static int hedge_read_cell(read_ctx *rctx, unsigned pos, uint64_t off, void *buf, size_t len, bool is_direct)
{
    struct cold_obj *o = rctx->o;
    if (pos >= o->npos)
        return CHUNK_ST_ABSENT;
    if (rctx->skip_mask & (1u << pos))
        return CHUNK_ST_DOWN;
    uint32_t node = o->node[pos];
    /* Check read-avoid before o->st[pos] for positions skipped at open due to read-avoid, or normal OK positions. */
    if ((o->st[pos] == CHUNK_ST_OK || o->avoided[pos]) && cp_peer_is_avoid(node, true)) {
        rctx->had_avoid = true;
        return chunkpeer_mutant == CP_MUT_HEDGECORRUPT ? CHUNK_ST_DAMAGED : CHUNK_ST_DOWN;
    }
    if (o->st[pos] != CHUNK_ST_OK)
        return o->st[pos];

    uint64_t stripe = off / o->g.cell, in_cell = off % o->g.cell;
    uint64_t obj_off = (stripe * (uint64_t)o->g.k + (uint64_t)pos) * (uint64_t)o->g.cell + in_cell;
    bool can_race = is_direct && o->g.m > 0 && in_cell + len <= o->g.cell && obj_off < o->g.len &&
                    len <= o->g.len - obj_off;

    if (!can_race) {
        uint32_t hms = cp_peer_begin_read(node, false);
        uint32_t lat_us = 0;
        bool answered = false;
        int st = direct_read_call(&o->net, o->timeout_ms, o->volume, o->epoch, o->vuuid, o->oid, pos, node, off, buf,
                                  len, &lat_us, &answered);
        if (st == CHUNK_ST_DOWN && !answered && lat_us >= hms * 1000u) {
            cp_peer_record_timeout(node);
            rctx->had_hedge_timeout = true;
            if (chunkpeer_mutant == CP_MUT_HEDGECORRUPT)
                return CHUNK_ST_DAMAGED;
        }
        return st;
    }

    uint32_t hms = cp_peer_begin_read(node, true);
    read_race *rr = calloc(1, sizeof *rr);
    if (!rr || !(rr->pri_buf = malloc(len ? len : 1))) {
        free(rr);
        return direct_read_call(&o->net, o->timeout_ms, o->volume, o->epoch, o->vuuid, o->oid, pos, node, off, buf, len,
                                NULL, NULL);
    }
    pthread_mutex_init(&rr->mu, NULL);
    tsdb_cond_init_mono(&rr->cv);
    rr->refs = 2; /* caller + primary job */
    rr->net = o->net;
    rr->timeout_ms = o->timeout_ms;
    rr->volume = o->volume;
    rr->epoch = o->epoch;
    memcpy(rr->vuuid, o->vuuid, 16);
    memcpy(rr->oid, o->oid, 16);
    rr->g = o->g;
    rr->npos = o->npos;
    rr->positions = o->positions;
    memcpy(rr->node, o->node, sizeof rr->node);
    memcpy(rr->st_open, o->st, sizeof rr->st_open);
    memcpy(rr->avoided_open, o->avoided, sizeof rr->avoided_open);
    rr->pos = pos;
    rr->cell_off = off;
    rr->obj_off = obj_off;
    rr->cell_len = len;
    rr->flags = rctx->flags;
    rr->skip_mask = rctx->bad_mask | (1u << pos);
    rr->pri_job = (cp_job){.fn = race_primary_run, .arg = rr};

    if (pool_submit(&rr->pri_job) != 0) {
        pthread_mutex_lock(&rr->mu);
        rr->refs = 1;
        race_unref_locked(rr);
        return direct_read_call(&o->net, o->timeout_ms, o->volume, o->epoch, o->vuuid, o->oid, pos, node, off, buf, len,
                                NULL, NULL);
    }

    pthread_mutex_lock(&rr->mu);
    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, (int)hms);
    while (!rr->pri_done) {
        if (pthread_cond_timedwait(&rr->cv, &rr->mu, &dl) == ETIMEDOUT)
            break;
    }
    if (rr->pri_done) {
        int st = rr->pri_st;
        if (st == CHUNK_ST_OK)
            memcpy(buf, rr->pri_buf, len);
        race_unref_locked(rr);
        return st;
    }

    /* Hedge timer expired while primary is still in flight. */
    cp_peer_record_timeout(node);
    bool reserved = cp_peer_reserve_hedge(node);
    if (!reserved) {
        while (!rr->pri_done)
            pthread_cond_wait(&rr->cv, &rr->mu);
        int st = rr->pri_st;
        if (st == CHUNK_ST_OK) {
            memcpy(buf, rr->pri_buf, len);
        } else if (st == CHUNK_ST_DOWN) {
            rctx->had_hedge_timeout = true;
            if (chunkpeer_mutant == CP_MUT_HEDGECORRUPT)
                st = CHUNK_ST_DAMAGED;
        }
        race_unref_locked(rr);
        return st;
    }

    /* Launch hedge decode from other positions while keeping primary running: first good answer wins. */
    rctx->had_hedge_timeout = true;
    pthread_mutex_unlock(&rr->mu);
    int prep = race_prep_hedge(rr, o, rctx->flags);
    pthread_mutex_lock(&rr->mu);
    if (rr->pri_done && rr->pri_st == CHUNK_ST_OK) {
        cp_peer_cancel_hedge(node);
        memcpy(buf, rr->pri_buf, len);
        race_unref_locked(rr);
        return CHUNK_ST_OK;
    }
    if (prep == 0) {
        rr->refs++;
        rr->hedge_launched = true;
        rr->hedge_job = (cp_job){.fn = race_hedge_run, .arg = rr};
        if (pool_submit(&rr->hedge_job) == 0) {
            cp_peer_commit_hedge(node);
        } else {
            rr->refs--;
            rr->hedge_launched = false;
            cp_peer_cancel_hedge(node);
        }
    } else {
        cp_peer_cancel_hedge(node);
    }
    while (!(rr->pri_done && rr->pri_st == CHUNK_ST_OK) && !(rr->hedge_done && rr->hedge_rc == 0) &&
           !(rr->pri_done && (!rr->hedge_launched || rr->hedge_done))) {
        pthread_cond_wait(&rr->cv, &rr->mu);
    }
    int ret;
    if (rr->pri_done && rr->pri_st == CHUNK_ST_OK) {
        memcpy(buf, rr->pri_buf, len);
        ret = CHUNK_ST_OK;
    } else if (rr->hedge_done && rr->hedge_rc == 0) {
        memcpy(buf, rr->hedge_buf, len);
        rctx->hedged_decoded += rr->hedge_rep.decoded_cells ? rr->hedge_rep.decoded_cells : 1u;
        cp_peer_add_decoded(node, 1);
        ret = CHUNK_ST_OK;
    } else {
        int pri_st = rr->pri_st ? rr->pri_st : CHUNK_ST_DOWN;
        if (rr->hedge_launched && rr->hedge_done &&
            (rr->hedge_rc == TSDB_ERR_UNAVAILABLE || rr->hedge_rc == TSDB_ERR_CORRUPT)) {
            /* The hedge already probed every other position of this stripe and proved < k good (or corrupt encoding):
             * preserve the exact E15 result and avoid re-reading all survivors a second time in outer degrade(). */
            unsigned ngood = 0, down = 0;
            for (unsigned p = 0; p < o->npos; p++) {
                int pst;
                if (p == pos)
                    pst = pri_st;
                else if (rctx->bad_mask & (1u << p))
                    pst = rctx->bad_st[p];
                else
                    pst = rr->hedge_rep.st[p];
                if (pst == CHUNK_ST_OK || pst == CHUNK_ST_VIRTUAL)
                    ngood++;
                else if (pst == CHUNK_ST_DOWN)
                    down++;
            }
            int fail_rc = (ngood < o->g.k && ngood + down >= o->g.k) ? TSDB_ERR_UNAVAILABLE : TSDB_ERR_CORRUPT;
            if (fail_rc == TSDB_ERR_UNAVAILABLE && chunkpeer_mutant == CP_MUT_HEDGECORRUPT)
                fail_rc = TSDB_ERR_CORRUPT;
            rctx->stripe_failed = true;
            rctx->stripe_fail_rc = fail_rc;
        }
        ret = pri_st;
        if (ret == CHUNK_ST_DOWN && chunkpeer_mutant == CP_MUT_HEDGECORRUPT)
            ret = CHUNK_ST_DAMAGED;
    }
    race_unref_locked(rr);
    return ret;
}

static int src_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    read_ctx *rctx = ctx;
    struct cold_obj *o = rctx->o;
    uint64_t stripe = o->g.cell ? off / o->g.cell : 0;
    if (!rctx->has_stripe || stripe != rctx->cur_stripe) {
        rctx->has_stripe = true;
        rctx->cur_stripe = stripe;
        rctx->seen_direct_mask = 0;
        rctx->bad_mask = 0;
        memset(rctx->bad_st, 0, sizeof rctx->bad_st);
        rctx->stripe_failed = false;
        rctx->stripe_fail_rc = 0;
    }
    uint64_t c = stripe * (uint64_t)o->g.k + (uint64_t)pos;
    bool is_direct = rctx->allow_hedge && pos < o->g.k && c >= rctx->c0 && c <= rctx->c1 &&
                     !(rctx->seen_direct_mask & (1u << pos));
    if (is_direct)
        rctx->seen_direct_mask |= 1u << pos;

    if (rctx->stripe_failed) {
        int st = rctx->stripe_fail_rc == TSDB_ERR_UNAVAILABLE ? CHUNK_ST_DOWN : CHUNK_ST_DAMAGED;
        if (st == CHUNK_ST_DOWN && chunkpeer_mutant == CP_MUT_HEDGECORRUPT)
            st = CHUNK_ST_DAMAGED;
        if (pos < CHUNK_POS_MAX) {
            rctx->bad_mask |= 1u << pos;
            rctx->bad_st[pos] = st;
        }
        if (is_direct)
            rctx->failed_direct[pos]++;
        return st;
    }

    if (!is_direct && pos < CHUNK_POS_MAX && (rctx->bad_mask & (1u << pos)))
        return rctx->bad_st[pos];

    int st = hedge_read_cell(rctx, pos, off, buf, len, is_direct);
    if (st != CHUNK_ST_OK) {
        if (pos < CHUNK_POS_MAX) {
            rctx->bad_mask |= 1u << pos;
            rctx->bad_st[pos] = st;
        }
        if (is_direct)
            rctx->failed_direct[pos]++;
    }
    return st;
}

static const uint64_t *src_sums(void *ctx, unsigned pos, int *status)
{
    read_ctx *rctx = ctx;
    struct cold_obj *o = rctx->o;
    if (pos >= o->npos || o->st[pos] != CHUNK_ST_OK || !o->sums[pos]) {
        *status = pos < o->npos ? (o->st[pos] != CHUNK_ST_OK ? o->st[pos] : CHUNK_ST_DOWN) : CHUNK_ST_ABSENT;
        return NULL;
    }
    return o->sums[pos];
}

static void p_obj_close(cold_obj *o)
{
    if (!o)
        return;
    for (unsigned i = 0; i < CHUNK_POS_MAX; i++)
        free(o->sums[i]);
    free(o->rec);
    free(o);
}

/* An open with no position OK: NOTFOUND only when every position answered that it holds no such chunk; UNAVAILABLE
 * when one is away or failed to read (its chunk may be there: E15, never CORRUPT for that), CORRUPT when the rest
 * failed their checks (another encoding of the object, a header or table that does not parse). */
static int none_ok(const int *st, unsigned n)
{
    bool away = false, damaged = false;
    for (unsigned i = 0; i < n; i++) {
        away |= st[i] == CHUNK_ST_DOWN || st[i] == CHUNK_ST_IOERR;
        damaged |= st[i] == CHUNK_ST_DAMAGED;
    }
    return away ? TSDB_ERR_UNAVAILABLE : damaged ? TSDB_ERR_CORRUPT : TSDB_ERR_NOTFOUND;
}

static int p_obj_open_pos(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                          uint32_t positions, cold_obj **out)
{
    pctx *p = ctx;
    *out = NULL;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    struct cold_obj *o = calloc(1, sizeof *o);
    if (!o)
        return TSDB_ERR_NOMEM;
    o->net = p->cp->net;
    o->timeout_ms = timeout_of(p->cp);
    o->volume = p->volume;
    o->epoch = v.epoch;
    o->positions = positions;
    memcpy(o->vuuid, p->vuuid, 16);
    memcpy(o->oid, oid, 16);
    for (unsigned i = 0; i < v.nstripe; i++)
        o->node[i] = v.stripe[i];
    if (expect) {
        o->g = *expect;
    } else { /* the geometry from the first header found (positions are placed whatever k' is: E9) */
        uint32_t probe_nodes[CHUNK_POS_MAX];
        unsigned probe_pos[CHUNK_POS_MAX], nprobe = 0;
        for (unsigned i = 0; i < v.nstripe; i++) {
            if (positions != 0 && !(positions & (1u << i)))
                continue;
            probe_nodes[nprobe] = o->node[i];
            probe_pos[nprobe] = i;
            nprobe++;
        }
        if (nprobe == 0) {
            p_obj_close(o);
            return TSDB_ERR_CORRUPT;
        }
        fan *f = fan_new(p->cp, o->volume, o->epoch, probe_nodes, nprobe);
        if (!f) {
            p_obj_close(o);
            return TSDB_ERR_NOMEM;
        }
        for (unsigned j = 0; j < nprobe; j++)
            f->pos[j] = probe_pos[j];
        memcpy(f->vuuid, o->vuuid, 16);
        memcpy(f->oid, oid, 16);
        ask a[CHUNK_POS_MAX];
        fan_run(f, 1, 0, false, a, CHUNK_ST_DOWN);
        int st[CHUNK_POS_MAX];
        bool found = false;
        for (unsigned j = 0; j < nprobe; j++) {
            st[j] = a[j].st;
            if (a[j].st == CHUNK_ST_OK && !found) {
                o->g = a[j].g;
                found = true;
            }
        }
        if (!found) {
            p_obj_close(o);
            return none_ok(st, nprobe);
        }
    }
    o->npos = chunk_npos(&o->g);
    if (o->npos > v.nstripe) {
        p_obj_close(o);
        return TSDB_ERR_CORRUPT; /* an object record of more positions than the volume has members */
    }
    uint32_t active_nodes[CHUNK_POS_MAX];
    unsigned active_pos[CHUNK_POS_MAX];
    unsigned nactive = 0;
    for (unsigned i = 0; i < o->npos; i++) {
        if (positions != 0 && !(positions & (1u << i))) {
            o->st[i] = CHUNK_ST_UNWRITTEN; /* E10: known erasure, never asked */
            continue;
        }
        if (cp_peer_is_avoid(o->node[i], false)) {
            o->avoided[i] = true;
            o->st[i] = rec_sums ? CHUNK_ST_OK : CHUNK_ST_DOWN;
        } else {
            active_nodes[nactive] = o->node[i];
            active_pos[nactive] = i;
            nactive++;
        }
    }
    unsigned ok = 0;
    if (nactive > 0) {
        fan *f = fan_new(p->cp, o->volume, o->epoch, active_nodes, nactive);
        if (!f) {
            p_obj_close(o);
            return TSDB_ERR_NOMEM;
        }
        for (unsigned j = 0; j < nactive; j++)
            f->pos[j] = active_pos[j];
        memcpy(f->vuuid, o->vuuid, 16);
        memcpy(f->oid, oid, 16);
        f->has_expect = true;
        f->expect = o->g;
        ask a[CHUNK_POS_MAX];
        /* k' positions are enough to read every byte: one that does not answer by then is DOWN for this handle */
        fan_run(f, o->g.k, FAN_GRACE_MS, false, a, CHUNK_ST_DOWN);
        for (unsigned j = 0; j < nactive; j++) {
            unsigned i = active_pos[j];
            o->st[i] = a[j].st;
            o->sums[i] = a[j].sums;
            ok += o->st[i] == CHUNK_ST_OK;
        }
    }
    if (ok == 0) {
        int st_all[CHUNK_POS_MAX];
        unsigned nchk = 0;
        for (unsigned i = 0; i < o->npos; i++) {
            if (o->st[i] == CHUNK_ST_UNWRITTEN)
                continue;
            st_all[nchk++] = o->avoided[i] ? CHUNK_ST_DOWN : o->st[i];
        }
        int rc0 = nchk ? none_ok(st_all, nchk) : TSDB_ERR_CORRUPT;
        if (nactive < nchk && rc0 == TSDB_ERR_UNAVAILABLE && chunkpeer_mutant == CP_MUT_HEDGECORRUPT)
            rc0 = TSDB_ERR_CORRUPT;
        p_obj_close(o);
        return rc0;
    }
    if (rec_sums) {
        size_t n = chunk_sums_total(&o->g);
        if (!(o->rec = malloc(n * sizeof *o->rec))) {
            p_obj_close(o);
            return TSDB_ERR_NOMEM;
        }
        memcpy(o->rec, rec_sums, n * sizeof *o->rec);
    }
    *out = o;
    return 0;
}

static int p_obj_open(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                      cold_obj **out)
{
    return p_obj_open_pos(ctx, oid, expect, rec_sums, 0, out);
}

static int p_obj_read(cold_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep)
{
    chunk_report local_rep;
    if (!rep)
        rep = &local_rep;
    read_ctx rctx;
    memset(&rctx, 0, sizeof rctx);
    rctx.o = o;
    rctx.flags = flags;
    if (len > 0 && o->g.cell > 0) {
        rctx.allow_hedge = true;
        rctx.c0 = off / o->g.cell;
        rctx.c1 = (off + len - 1) / o->g.cell;
    }
    chunk_source src = {.ctx = &rctx, .read = src_read, .sums = src_sums, .positions = o->positions};
    for (int i = 0; i < 16; i++)
        snprintf(src.name + 2 * i, sizeof src.name - 2 * (size_t)i, "%02x", o->oid[i]);
    int rc = chunk_read_range(&src, &o->g, o->rec, off, len, buf, flags, rep);
    if (rc == 0) {
        unsigned attr = 0;
        if (rep->decoded_cells > 0) {
            for (unsigned p = 0; p < o->g.k; p++) {
                if (rctx.failed_direct[p] > 0) {
                    cp_peer_add_decoded(o->node[p], rctx.failed_direct[p]);
                    attr += rctx.failed_direct[p];
                }
            }
            if (rep->decoded_cells > attr) {
                unsigned rem = rep->decoded_cells - attr;
                unsigned unattr[CHUNK_K_MAX], nun = 0;
                for (unsigned p = 0; p < o->g.k; p++)
                    if (rep->st[p] != CHUNK_ST_OK && rctx.failed_direct[p] == 0)
                        unattr[nun++] = p;
                if (nun == 0) {
                    for (unsigned p = 0; p < o->g.k; p++)
                        if (rep->st[p] != CHUNK_ST_OK)
                            unattr[nun++] = p;
                }
                for (unsigned i = 0; i < nun && rem > 0; i++) {
                    unsigned share = rem / (nun - i);
                    if (share == 0)
                        share = 1;
                    cp_peer_add_decoded(o->node[unattr[i]], share);
                    rem -= share;
                }
            }
        }
        rep->decoded_cells += rctx.hedged_decoded;
    } else if (rc == TSDB_ERR_UNAVAILABLE && (rctx.had_hedge_timeout || rctx.had_avoid) &&
               chunkpeer_mutant == CP_MUT_HEDGECORRUPT) {
        rc = TSDB_ERR_CORRUPT;
    }
    return rc;
}

static const chunk_geom *p_obj_geom(const cold_obj *o) { return &o->g; }

/* ---------------------------------------------------------------- delete (D13: only by MK_GC, r8k) */
static int p_del(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, int reason)
{
    pctx *p = ctx;
    if (chunkpeer_mutant != CP_MUT_DELBEFOREGC) {
        if (!atomic_exchange(&p->del_said, true))
            fprintf(stderr, "tsdbd: volume %" PRIu32 ": direct chunk deletes on a cluster volume are refused "
                            "(chunks are deleted only by a committed MK_GC): %s refused\n", p->volume,
                    reason == COLD_DEL_SWEEP ? "open's sweep of orphans" : "direct delete");
        return TSDB_ERR_UNAVAILABLE;
    }
    struct chunkpeer_view v; /* planted: R7's delete, through to the member */
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    if (pos >= v.nstripe)
        return TSDB_ERR_NOTFOUND;
    struct cs_req r;
    req_init(&r, CS_OP_DEL, p->volume, epoch, p->vuuid, oid, pos);
    int32_t arc = 0;
    uint8_t *b = NULL;
    size_t bn = 0;
    rc = p->cp->net.call(p->cp->net.arg, v.stripe[pos], &r, timeout_of(p->cp), &arc, &b, &bn);
    free(b);
    return rc ? rc : arc;
}

static int p_del_gc(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, uint64_t mk_pos)
{
    pctx *p = ctx;
    if (mk_pos == 0 || epoch == 0)
        return TSDB_ERR_INVALID;
    if (!p_writable(p))
        return TSDB_ERR_UNAVAILABLE;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    if (pos >= v.nstripe)
        return TSDB_ERR_NOTFOUND;
    struct cs_req r;
    req_init(&r, CS_OP_DEL, p->volume, epoch, p->vuuid, oid, pos);
    int32_t arc = 0;
    uint8_t *b = NULL;
    size_t bn = 0;
    rc = p->cp->net.call(p->cp->net.arg, v.stripe[pos], &r, timeout_of(p->cp), &arc, &b, &bn);
    free(b);
    int res = rc ? rc : arc;
    if (res == 0)
        see_fence(p, epoch);
    return res;
}

/* ---------------------------------------------------------------- list */
static int list_entry(const uint8_t *e, chunk_listed *c, uint8_t vuuid[16])
{
    memset(c, 0, sizeof *c);
    memcpy(c->oid, e, 16);
    c->pos = get16(e + 16);
    c->size = get64(e + 18);
    c->epoch = get64(e + 26);
    memcpy(c->vuuid, e + 34, 16);
    if (vuuid)
        memcpy(vuuid, c->vuuid, 16);
    c->status = e[50];
    return e[50]; /* the entry's status */
}

/* One page of member `node`'s listing after the cursor: 0 with the body, or an error. */
static int list_page(pctx *p, uint32_t node, bool has_cursor, const uint8_t coid[16], unsigned cpos, uint32_t max,
                     uint8_t **body, size_t *bn, uint32_t *n, bool *more)
{
    struct cs_req r;
    req_init(&r, CS_OP_LIST, p->volume, 0, NULL, has_cursor ? coid : NULL, has_cursor ? cpos : 0);
    r.has_cursor = has_cursor;
    r.len = max;
    int32_t arc = 0;
    *body = NULL;
    int rc = p->cp->net.call(p->cp->net.arg, node, &r, timeout_of(p->cp), &arc, body, bn);
    if (rc == 0 && arc != 0)
        rc = arc;
    if (rc == 0 && (*bn < 5 || (*n = get32(*body)) > max || *bn != 5 + (size_t)*n * CS_LIST_ENTRY))
        rc = TSDB_ERR_IO; /* a listing cut short or garbled is an error, never the end */
    if (rc) {
        free(*body);
        *body = NULL;
        return rc;
    }
    *more = (*body)[4] != 0;
    return 0;
}

static int p_list_names(void *ctx, chunk_list_fn fn, void *fctx)
{
    pctx *p = ctx;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    for (unsigned i = 0; i < v.nstripe && rc == 0; i++) {
        bool has = false, more = true;
        uint8_t coid[16] = {0};
        unsigned cpos = 0;
        while (more && rc == 0) {
            uint8_t *b;
            size_t bn;
            uint32_t n = 0;
            if ((rc = list_page(p, v.stripe[i], has, coid, cpos, chunkpeer_test_list_max, &b, &bn, &n, &more)) != 0)
                break;
            for (uint32_t j = 0; j < n && rc == 0; j++) {
                chunk_listed c;
                list_entry(b + 5 + (size_t)j * CS_LIST_ENTRY, &c, NULL);
                c.store = (int)v.stripe[i]; /* our token: the member (owner asks it) */
                rc = fn(fctx, &c);
                memcpy(coid, c.oid, 16);
                cpos = c.pos;
                has = true;
            }
            free(b);
            if (n == 0)
                break;
        }
    }
    return rc;
}

/* The (oid, pos) just before c's in a member's listing order (oid bytes, then pos): a one-entry page after it is c. */
static bool cursor_before(const chunk_listed *c, uint8_t oid[16], unsigned *pos)
{
    memcpy(oid, c->oid, 16);
    if (c->pos > 0) {
        *pos = c->pos - 1u;
        return true;
    }
    int i = 15;
    while (i >= 0 && oid[i] == 0)
        oid[i--] = 0xff;
    if (i < 0)
        return false; /* the first name there is: no cursor */
    oid[i]--;
    *pos = CHUNK_POS_MAX - 1; /* the highest position a cursor may name (cs_req_decode) */
    return true;
}

static int p_owner(void *ctx, const chunk_listed *c, uint8_t vuuid[16])
{
    pctx *p = ctx;
    uint8_t coid[16];
    unsigned cpos = 0;
    bool has = cursor_before(c, coid, &cpos);
    uint8_t *b;
    size_t bn;
    uint32_t n = 0;
    bool more;
    int rc = list_page(p, (uint32_t)c->store, has, coid, cpos, 4, &b, &bn, &n, &more);
    if (rc)
        return rc;
    rc = TSDB_ERR_NOTFOUND;
    for (uint32_t j = 0; j < n; j++) { /* the copies of one (oid, pos) are listed together */
        chunk_listed e;
        int st = list_entry(b + 5 + (size_t)j * CS_LIST_ENTRY, &e, vuuid);
        if (memcmp(e.oid, c->oid, 16) || e.pos != c->pos)
            break;
        if (st == CHUNK_ST_OK) {
            rc = 0;
            break;
        }
        rc = TSDB_ERR_CORRUPT;
    }
    free(b);
    return rc;
}

/* ---------------------------------------------------------------- epoch and state */
/* EPOCH at e on every stripe member at once (e 0: only asks): how many hold e (a fence at or above it, a store that is
 * sure) in *held, how many answered at all in *answered (NULL: not wanted); *first_bad the first that did not hold it
 * (its index), its answer in *bad. */
static int ask_all(pctx *p, uint64_t e, bool stop_on_fail, unsigned *held, unsigned *answered, unsigned *nstripe,
                   int *first_bad, ask *bad)
{
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    fan *f = fan_new(p->cp, p->volume, e, v.stripe, v.nstripe);
    if (!f)
        return TSDB_ERR_NOMEM;
    f->epoch_op = true;
    ask a[CHUNK_POS_MAX];
    fan_run(f, 0, 0, stop_on_fail, a, TSDB_ERR_UNAVAILABLE);
    *held = 0;
    if (answered)
        *answered = 0;
    *nstripe = v.nstripe;
    *first_bad = -1;
    for (unsigned i = 0; i < v.nstripe; i++) {
        if (a[i].st == 0) {
            see_fence(p, a[i].fence);
            if (answered)
                (*answered)++;
        }
        if (a[i].st == 0 && a[i].fence >= e && !a[i].unsure) {
            (*held)++;
            continue;
        }
        if (*first_bad < 0) {
            *first_bad = (int)i;
            *bad = a[i];
            if (bad->st == 0)
                bad->st = bad->unsure ? CHUNK_ERR_EPOCH_UNSURE : CHUNK_ERR_FENCED;
        }
    }
    return 0;
}

/* At attach (a writer's open, a promotion, a replica's open). A member that is away or does not take e makes the
 * context unfenced: no put or delete until every member holds the copy's epoch (asked again by writable, at most once a
 * second). Never an error for members that are away: an attach that failed would refuse the volume until tsdbd
 * restarts, and a cluster's cold start opens its volumes before the chunk links are up. */
static int p_see_epoch(void *ctx, uint64_t e)
{
    pctx *p = ctx;
    unsigned held, n;
    int bad_i;
    ask bad;
    int rc = ask_all(p, e, false, &held, NULL, &n, &bad_i, &bad);
    if (rc)
        return rc;
    if (held == n) {
        atomic_store(&p->unfenced, false);
        return 0;
    }
    atomic_store(&p->unfenced, true);
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    atomic_store(&p->asked_ms, (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
    fprintf(stderr, "tsdbd: volume %" PRIu32 ": %u of %u stripe members do not hold epoch %" PRIu64 " (position %d: "
                    "%s): no put or delete of the volume until they do (asked again every second)\n",
            p->volume, n - held, n, e, bad_i, err_name(bad.st));
    return 0;
}

static uint64_t p_epoch(void *ctx) { return atomic_load(&((pctx *)ctx)->seen); }

static bool p_writable(void *ctx)
{
    pctx *p = ctx;
    if (!atomic_load(&p->unfenced))
        return true;
    if (chunkpeer_mutant == CP_MUT_STICKY)
        return false; /* planted: unfenced for the context's life (a promoted leader never settles again) */
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now = (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000, last = atomic_load(&p->asked_ms);
    if (now - last < 1000 || !atomic_compare_exchange_strong(&p->asked_ms, &last, now))
        return false; /* asked within the last second, or another thread asks now */
    struct chunkpeer_view v;
    if (view_of(p, &v) != 0 || v.epoch == 0)
        return false;
    unsigned held, n;
    int bad_i;
    ask bad;
    if (ask_all(p, v.epoch, true, &held, NULL, &n, &bad_i, &bad) != 0 || held != n)
        return false;
    atomic_store(&p->unfenced, false);
    fprintf(stderr, "tsdbd: volume %" PRIu32 ": every stripe member holds epoch %" PRIu64 ": puts and deletes go "
                    "on\n", p->volume, v.epoch);
    return true;
}

/* Every member answered: an unsure store is UP (as the directory backend's is) and refuses puts itself; counting it
 * down made eccold's stripe_ok refuse every settle before its put while one store was unsure (ecbasic's D9 case). */
static bool p_all_up(void *ctx)
{
    unsigned held, up, n;
    int bad_i;
    ask bad;
    if (chunkpeer_mutant == CP_MUT_UNSUREDOWN)
        return ask_all(ctx, 0, true, &held, &up, &n, &bad_i, &bad) == 0 && held == n; /* planted: unsure is down */
    return ask_all(ctx, 0, true, &held, &up, &n, &bad_i, &bad) == 0 && up == n;
}

static uint64_t p_stripe_version(void *ctx)
{
    struct chunkpeer_view v;
    return view_of(ctx, &v) == 0 ? v.stripe_version : 0;
}

static bool p_gc_suspended(void *ctx)
{
    struct chunkpeer_view v;
    return view_of(ctx, &v) != 0 || v.gc_suspended != 0;
}

static unsigned p_nstripe(void *ctx)
{
    struct chunkpeer_view v;
    return view_of(ctx, &v) == 0 ? v.nstripe : 0;
}

/* R9 (E10): probe stripe positions in `probe_mask` (0 = all nstripe) with bounded timeout; returns bitmap of UP
 * positions (and when require_epoch > 0, holding a durable non-unsure fence >= require_epoch). */
static int p_stripe_up_mask(void *ctx, uint64_t require_epoch, uint32_t probe_mask, uint32_t *up_mask_out,
                            unsigned *npos_out)
{
    pctx *p = ctx;
    *up_mask_out = 0;
    *npos_out = 0;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    *npos_out = v.nstripe;
    uint32_t full = v.nstripe < 32 ? ((1u << v.nstripe) - 1u) : ~0u;
    uint32_t want = probe_mask ? (probe_mask & full) : full;
    uint32_t nodes[CHUNK_POS_MAX];
    unsigned pidx[CHUNK_POS_MAX], n = 0;
    for (unsigned i = 0; i < v.nstripe; i++) {
        if (!(want & (1u << i)))
            continue;
        nodes[n] = v.stripe[i];
        pidx[n] = i;
        n++;
    }
    if (n == 0)
        return 0;
    fan *f = fan_new(p->cp, p->volume, require_epoch, nodes, n);
    if (!f)
        return TSDB_ERR_NOMEM;
    f->epoch_op = true;
    if (f->tmo > 1000)
        f->tmo = 1000; /* bounded probe so a SIGSTOPped member does not stall the settle thread or gate */
    ask a[CHUNK_POS_MAX];
    fan_run(f, 0, 0, false, a, TSDB_ERR_UNAVAILABLE);
    uint32_t up = 0;
    for (unsigned j = 0; j < n; j++) {
        if (a[j].st != 0)
            continue;
        see_fence(p, a[j].fence);
        if (require_epoch > 0) {
            if (a[j].fence >= require_epoch && !a[j].unsure)
                up |= 1u << pidx[j];
        } else if (chunkpeer_mutant == CP_MUT_UNSUREDOWN) {
            if (!a[j].unsure)
                up |= 1u << pidx[j];
        } else {
            up |= 1u << pidx[j];
        }
    }
    *up_mask_out = up;
    return 0;
}

static void p_note_missing(void *ctx, const uint8_t oid[16], unsigned pos)
{
    pctx *p = ctx;
    if (p->cp->note_missing)
        p->cp->note_missing(p->volume, oid, pos);
}

static void p_free(void *ctx) { free(ctx); }

const cold_ops coldops_peer = {
    .name = "peer",
    .put_stream = p_put_stream,
    .obj_open = p_obj_open,
    .obj_read = p_obj_read,
    .obj_geom = p_obj_geom,
    .obj_close = p_obj_close,
    .del = p_del,
    .list_names = p_list_names,
    .owner = p_owner,
    .see_epoch = p_see_epoch,
    .epoch = p_epoch,
    .writable = p_writable,
    .all_up = p_all_up,
    .ctx_free = p_free,
    .stripe_version = p_stripe_version,
    .del_gc = p_del_gc,
    .gc_suspended = p_gc_suspended,
    .nstripe = p_nstripe,
    .put_stream_pos = p_put_stream_pos,
    .obj_open_pos = p_obj_open_pos,
    .stripe_up_mask = p_stripe_up_mask,
    .note_missing = p_note_missing,
};

/* ---------------------------------------------------------------- in-process delivery */
typedef struct reply {
    int n;
    int32_t rc;
    uint8_t op;
    uint8_t *body;
    size_t len;
} reply;

static void take_reply(void *arg, const uint8_t *p, size_t n)
{
    reply *r = arg;
    struct cs_resp a;
    if (cs_resp_decode(p, n, &a) != 0 || r->n++ > 0)
        return;
    r->rc = a.rc;
    r->op = a.op;
    if (a.body_len && (r->body = malloc(a.body_len)) != NULL) {
        memcpy(r->body, a.body, a.body_len);
        r->len = a.body_len;
    }
}

int chunkpeer_local_call(struct chunksrv *cs, uint32_t from, const struct cs_req *r, int32_t *rc, uint8_t **body,
                         size_t *body_len)
{
    *body = NULL;
    *body_len = 0;
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    uint8_t buf[128];
    size_t n = cs_req_encode(r, buf, sizeof buf);
    reply a = {0};
    if (n == 0 || chunksrv_handle(cs, from, buf, n, take_reply, &a) != 0 || a.n != 1) {
        free(a.body);
        return TSDB_ERR_UNAVAILABLE;
    }
    *rc = a.rc;
    *body = a.body;
    *body_len = a.len;
    return 0;
}

int chunkpeer_local_put(struct chunksrv *cs, uint32_t from, const struct cs_put_meta *m, int fd, uint64_t base,
                        uint64_t *answered)
{
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    static _Atomic uint64_t file_ids = 1ull << 62; /* apart from any peerlink's */
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(m, meta);
    uint64_t total = cs_put_total(m);
    reply a = {0};
    struct cs_put *put = NULL;
    int rc = chunksrv_put_begin(cs, from, atomic_fetch_add(&file_ids, 1), meta, sizeof meta, total, take_reply, &a,
                                &put);
    if (rc == 0) {
        size_t cap = 1u << 20;
        uint8_t *buf = malloc(cap);
        int st = buf ? 0 : -1;
        for (uint64_t off = 0; off < total && st == 0 && rc == 0;) {
            size_t want = total - off < cap ? (size_t)(total - off) : cap;
            ssize_t got = pread(fd, buf, want, (off_t)(base + off));
            if (got < 0 && errno == EINTR)
                continue;
            if (got <= 0) {
                st = -1;
                break;
            }
            rc = chunksrv_put_data(put, off, buf, (size_t)got);
            off += (uint64_t)got;
        }
        free(buf);
        chunksrv_put_end(put, st == 0 && rc == 0 ? 0 : -1);
        if (st != 0 && rc == 0)
            rc = TSDB_ERR_IO;
    }
    if (a.n == 1 && rc == 0)
        rc = a.rc;
    else if (rc == 0)
        rc = TSDB_ERR_IO; /* committed without an answer: never taken as a success */
    if (rc == 0) {
        if (a.len != 8) {
            rc = TSDB_ERR_IO;
        } else {
            *answered = get64(a.body);
        }
    }
    free(a.body);
    return rc;
}

/* ---------------------------------------------------------------- R9 fill transport helpers (D11, PL_KIND_CHUNKR) */

int chunkpeer_objlist_page(struct chunksrv *cs, uint32_t self, uint32_t node, uint32_t volume, uint64_t max_version,
                           bool has_cursor, const uint8_t cursor_oid[16], uint32_t max_entries, int timeout_ms,
                           uint8_t vuuid_out[16], struct cs_objlist_item **items_out, uint32_t *n_out, bool *more_out)
{
    *items_out = NULL;
    *n_out = 0;
    *more_out = false;
    if (!cs || node == 0 || max_entries == 0 || max_entries > CS_OBJLIST_MAX)
        return TSDB_ERR_INVALID;
    struct cs_req r;
    req_init(&r, CS_OP_OBJLIST, volume, 0, NULL, has_cursor ? cursor_oid : NULL, 0);
    r.off = max_version;
    r.has_cursor = has_cursor;
    r.len = max_entries;
    int32_t arc = 0;
    uint8_t *body = NULL;
    size_t bl = 0;
    int rc = (self && node == self)
                 ? chunkpeer_local_call(cs, self, &r, &arc, &body, &bl)
                 : chunksrv_call_kind(cs, node, PL_KIND_CHUNK, &r, timeout_ms, &arc, &body, &bl);
    if (rc == 0 && arc != 0)
        rc = arc;
    if (rc == 0)
        rc = cs_objlist_resp_decode(body, bl, vuuid_out, items_out, n_out, more_out);
    free(body);
    return rc;
}

int chunkpeer_read_rebuild(struct chunksrv *cs, uint32_t self, uint32_t node, uint32_t volume, uint64_t epoch,
                           const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos, uint64_t off, void *buf,
                           size_t len, int timeout_ms)
{
    if (!cs || node == 0 || len == 0 || len > CS_READ_MAX)
        return CHUNK_ST_IOERR;
    struct cs_req r;
    req_init(&r, CS_OP_READ, volume, epoch, vuuid, oid, pos);
    r.off = off;
    r.len = (uint32_t)len;
    int32_t arc = 0;
    uint8_t *body = NULL;
    size_t bl = 0;
    int trc = (self && node == self)
                  ? chunkpeer_local_call(cs, self, &r, &arc, &body, &bl)
                  : chunksrv_call_kind(cs, node, PL_KIND_CHUNKR, &r, timeout_ms, &arc, &body, &bl);
    int st = st_of(trc, arc);
    if (st == CHUNK_ST_OK) {
        if (bl != len)
            st = CHUNK_ST_DAMAGED;
        else
            memcpy(buf, body, len);
    }
    free(body);
    return st;
}

int chunkpeer_notify_reopen(struct chunksrv *cs, uint32_t self, uint32_t node, uint32_t volume,
                            const uint8_t oid[16], int timeout_ms)
{
    if (!cs || node == 0 || !oid)
        return TSDB_ERR_INVALID;
    struct cs_req r;
    req_init(&r, CS_OP_REOPEN, volume, 0, NULL, oid, 0);
    int32_t arc = 0;
    uint8_t *body = NULL;
    size_t bl = 0;
    int rc = (self && node == self)
                 ? chunkpeer_local_call(cs, self, &r, &arc, &body, &bl)
                 : chunksrv_call_kind(cs, node, PL_KIND_CHUNK, &r, timeout_ms, &arc, &body, &bl);
    free(body);
    return rc ? rc : arc;
}
