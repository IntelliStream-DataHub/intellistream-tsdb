/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The data links (server/peerlink.c, docs/design-cluster.md C25): two endpoints in one process over 127.0.0.1, no
 * tsdbd. Usage: test_peerlink SCRATCHDIR [--quick] [--only NAME] [--mutant NAME] (the planted bugs of peerlink.h's PL_MUT_*, each of which must fail its test)
 * [--bench].
 *
 *   codec        frames encoded, split at random, decoded; a flipped bit, a repeated or a swapped frame ends the
 *                stream; the streaming xxh64 equals xxh64()
 *   log          log frames arrive in order and whole (random and compressible ones, zstd on the wire); an echo
 *   bulk         a file arrives whole with its xxh64; a metadata and a size are what the sender gave
 *   resume       the connection is cut twice in the middle of a file (an in-test proxy): the file completes from the
 *                receiver's offset, the last attempt sends less than the file
 *   secret       a peer with another secret never comes up; a peer that is not the node id asked for neither
 *   flip         a byte flipped on the wire ends the link and is never delivered (mutant notag delivers it)
 *   blackhole    a pair behind a blackholing proxy exchanges nothing, goes down, and reconnects once healed
 *   backpressure a receiver that stops reading: the sender's queue stops at its bound (mutant nobackpressure does
 *                not), a timed send waits and gives up
 *   latency      with 64 MB of a bulk file in flight to a slow receiver, small log frames keep their round trip
 *                (mutant sharedsocket puts the file on the log link's socket and the round trip is the file's)
 *
 * The proxy lives here: tests/jepsen/jepsen_cluster.c has r3f's, which is part of that program.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

#include "internal.h"
#include "peerlink.h"
#include "tsdb_crypto.h"

static int failures;
#define CHECK(cond)                                                                                                \
    do {                                                                                                           \
        if (!(cond)) {                                                                                             \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond);                                     \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)

static int quick;
static const char *scratch = "/tmp";

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
static uint64_t now_ms(void) { return now_ns() / 1000000ull; }

/* waits up to `ms` for a condition that other threads make true */
#define WAIT_FOR(cond, ms)                                                                                         \
    ({                                                                                                             \
        uint64_t t_end_ = now_ms() + (ms);                                                                         \
        int ok_ = 0;                                                                                               \
        while (!(ok_ = !!(cond)) && now_ms() < t_end_)                                                             \
            usleep(2000);                                                                                          \
        ok_;                                                                                                       \
    })

static uint64_t rng_next(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return *s = x;
}

static void fill_random(uint8_t *p, size_t n, uint64_t seed)
{
    uint64_t s = seed * 0x9E3779B97F4A7C15ull + 0x1234567ull;
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = rng_next(&s);
        memcpy(p + i, &v, n - i < 8 ? n - i : 8);
    }
}

static void fill_text(uint8_t *p, size_t n, uint64_t seed)
{
    static const char w[] = "temperature,pressure,flow,vibration;";
    for (size_t i = 0; i < n; i++)
        p[i] = (uint8_t)w[(i + seed) % (sizeof w - 1)];
}

static const uint8_t SECRET[32] = "r3d-links test secret 012345678";
static const uint8_t SECRET2[32] = "another cluster secret 98765432";

/* ---------------------------------------------------------------- codec */

struct collect {
    uint8_t type[4096];
    size_t len[4096];
    uint8_t *data[4096];
    int n;
};

static int collect_msg(void *a, const struct pl_msg *m)
{
    struct collect *c = a;
    if (c->n >= 4096)
        return -1;
    c->type[c->n] = m->type;
    c->len[c->n] = m->n;
    c->data[c->n] = malloc(m->n ? m->n : 1);
    memcpy(c->data[c->n], m->p, m->n);
    c->n++;
    return 0;
}

static void collect_free(struct collect *c)
{
    for (int i = 0; i < c->n; i++)
        free(c->data[i]);
    memset(c, 0, sizeof *c);
}

static int feed_split(struct pl_reader *r, const uint8_t *s, size_t n, struct collect *c, uint64_t *rs)
{
    size_t off = 0;
    int rc = 0;
    while (off < n && rc == 0) {
        size_t take = 1 + rng_next(rs) % 3000;
        if (take > n - off)
            take = n - off;
        rc = pl_reader_feed(r, s + off, take, collect_msg, c);
        off += take;
    }
    return rc;
}

static void test_codec(void)
{
    uint8_t key[32];
    fill_random(key, 32, 7);
    enum { NF = 60 };
    uint8_t *orig[NF];
    size_t olen[NF];
    uint8_t otype[NF];
    uint8_t *fr[NF];
    size_t flen[NF];
    uint64_t rs = 99, seq = 0;
    for (int i = 0; i < NF; i++) {
        static const size_t sz[] = {0, 1, 7, 80, 255, 256, 1000, 5000, 70000, 300000};
        olen[i] = sz[rng_next(&rs) % 10];
        otype[i] = (uint8_t)(1 + rng_next(&rs) % 0x6e);
        orig[i] = malloc(olen[i] + 1);
        if (rng_next(&rs) & 1)
            fill_random(orig[i], olen[i], (uint64_t)i + 1);
        else
            fill_text(orig[i], olen[i], (uint64_t)i);
        flen[i] = pl_frame_encode(key, &seq, i % 3 != 0, otype[i], orig[i], olen[i], &fr[i]);
        CHECK(flen[i] != 0);
    }
    size_t total = 0;
    for (int i = 0; i < NF; i++)
        total += flen[i];
    uint8_t *stream = malloc(total);
    size_t off = 0, at[NF];
    for (int i = 0; i < NF; i++) {
        at[i] = off;
        memcpy(stream + off, fr[i], flen[i]);
        off += flen[i];
    }

    /* the whole stream, split at random */
    for (int rep = 0; rep < 5; rep++) {
        struct pl_reader *r = pl_reader_new(key, 0);
        struct collect c = {0};
        CHECK(feed_split(r, stream, total, &c, &rs) == 0);
        CHECK(c.n == NF);
        for (int i = 0; i < c.n && i < NF; i++)
            CHECK(c.type[i] == otype[i] && c.len[i] == olen[i] && memcmp(c.data[i], orig[i], olen[i]) == 0);
        collect_free(&c);
        pl_reader_free(r);
    }
    /* a wrong key: nothing is delivered */
    {
        uint8_t k2[32];
        memcpy(k2, key, 32);
        k2[0] ^= 1;
        struct pl_reader *r = pl_reader_new(k2, 0);
        struct collect c = {0};
        CHECK(feed_split(r, stream, total, &c, &rs) < 0 && c.n == 0);
        collect_free(&c);
        pl_reader_free(r);
    }
    /* a flipped bit anywhere: the frames before it come through, it and what follows never do */
    for (int rep = 0; rep < 300; rep++) {
        uint8_t *bad = malloc(total);
        memcpy(bad, stream, total);
        size_t pos = rng_next(&rs) % total;
        bad[pos] ^= (uint8_t)(1u << (rng_next(&rs) % 8));
        int hit = 0;
        while (hit + 1 < NF && at[hit + 1] <= pos)
            hit++;
        struct pl_reader *r = pl_reader_new(key, 0);
        struct collect c = {0};
        feed_split(r, bad, total, &c, &rs);
        CHECK(c.n <= hit);
        for (int i = 0; i < c.n && i < NF; i++)
            CHECK(c.len[i] == olen[i] && memcmp(c.data[i], orig[i], olen[i]) == 0);
        collect_free(&c);
        pl_reader_free(r);
        free(bad);
    }
    /* a repeated frame and two swapped ones */
    for (int mode = 0; mode < 2; mode++) {
        int k = 5 + (int)(rng_next(&rs) % 40);
        size_t cut = at[k] + flen[k];
        uint8_t *bad = malloc(total + flen[k]);
        size_t w = 0;
        if (mode == 0) { /* frame k twice */
            memcpy(bad, stream, cut);
            memcpy(bad + cut, stream + at[k], flen[k]);
            memcpy(bad + cut + flen[k], stream + cut, total - cut);
            w = total + flen[k];
        } else { /* frames k and k+1 swapped */
            memcpy(bad, stream, at[k]);
            memcpy(bad + at[k], stream + at[k + 1], flen[k + 1]);
            memcpy(bad + at[k] + flen[k + 1], stream + at[k], flen[k]);
            size_t e = at[k + 1] + flen[k + 1];
            memcpy(bad + e, stream + e, total - e);
            w = total;
        }
        struct pl_reader *r = pl_reader_new(key, 0);
        struct collect c = {0};
        CHECK(feed_split(r, bad, w, &c, &rs) < 0);
        CHECK(c.n <= k + 1);
        collect_free(&c);
        pl_reader_free(r);
        free(bad);
    }
    /* a frame whose length field is above the cap ends the stream at once */
    {
        uint8_t hdr[8] = {0xff, 0xff, 0xff, 0x7f, 1, 0, 0, 0};
        struct pl_reader *r = pl_reader_new(key, 0);
        struct collect c = {0};
        CHECK(pl_reader_feed(r, hdr, sizeof hdr, collect_msg, &c) < 0);
        pl_reader_free(r);
    }
    /* the streaming xxh64 */
    for (int rep = 0; rep < 40; rep++) {
        size_t n = rng_next(&rs) % 200000;
        uint8_t *d = malloc(n + 1);
        fill_random(d, n, rng_next(&rs));
        struct pl_xxh h;
        pl_xxh_init(&h);
        for (size_t o = 0; o < n;) {
            size_t t = 1 + rng_next(&rs) % 5000;
            if (t > n - o)
                t = n - o;
            pl_xxh_update(&h, d + o, t);
            o += t;
        }
        CHECK(pl_xxh_final(&h) == xxh64(d, n, 0));
        free(d);
    }
    for (int i = 0; i < NF; i++) {
        free(orig[i]);
        free(fr[i]);
    }
    free(stream);
    /* a zstd frame that decodes to fewer bytes than its declared raw length is not a frame (the tail would be
     * uninitialised memory handed to the driver) */
    {
        uint8_t src[900], z[1200], body[1300], *own = NULL;
        memset(src, 'a', sizeof src);
        size_t zn = ZSTD_compress(z, sizeof z, src, sizeof src, 1);
        CHECK(!ZSTD_isError(zn));
        for (int declared = 899; declared <= 1000; declared += 101) { /* 899: smaller, 1000: larger than decoded */
            uint32_t raw = (uint32_t)declared;
            memcpy(body, &raw, 4);
            memcpy(body + 4, z, zn);
            struct pl_msg m;
            CHECK(pl_msg_decode(0x81, body, 4 + zn, &m, &own) != 0);
            free(own);
        }
        uint32_t raw = 900;
        memcpy(body, &raw, 4);
        struct pl_msg m;
        CHECK(pl_msg_decode(0x81, body, 4 + zn, &m, &own) == 0 && m.n == 900); /* the right size passes */
        free(own);
    }
}

/* ---------------------------------------------------------------- a node: a peerlink with recording callbacks */

#define MAXRTT 4096

static _Atomic int g_fail_data;
static int g_uuid_byte = 0xA0;
struct node {
    struct peerlink *pl;
    int port;
    uint32_t id;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    /* frames */
    _Atomic uint64_t frames_in, bad_frames, order_bad;
    uint64_t last_seq;
    int have_seq;
    int strict;       /* seq must be last + 1 */
    int echo;         /* a type 2 frame is answered with type 3 */
    int gate_closed;  /* on_frame blocks while set */
    /* rtt */
    uint64_t rtt[MAXRTT];
    _Atomic int nrtt;
    /* links */
    _Atomic int ups, downs;
    /* bulk in */
    char path[600];
    _Atomic int fd; /* the receiver's file: its link thread opens it, a test thread polls its size */
    int discard; /* bench: the bytes go nowhere and a resume starts over */
    unsigned delay_us;
    _Atomic uint64_t opens, last_resume, data_bytes, ends;
    _Atomic int last_end;
    _Atomic int meta_bad;
    char want_meta[64];
    uint64_t want_total, cur_file;
    int fake_pfd;          /* b_open answers with this resume prefix (another file's bytes) when >= 0... */
    uint64_t fake_resume;  /* ...of this many bytes */
    int link_hello;        /* on_link(up) sends a type 7 frame from inside the callback */
    _Atomic int first_type; /* the first driver frame type since the link last came up */
};

static void gen_payload(uint8_t type, uint8_t *p, size_t n, uint64_t seq)
{
    memcpy(p, &seq, 8);
    if (type == 1)
        fill_random(p + 8, n - 8, seq);
    else
        fill_text(p + 8, n - 8, seq);
}

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    struct node *nd = arg;
    if (!atomic_load(&nd->first_type))
        atomic_store(&nd->first_type, type);
    pthread_mutex_lock(&nd->mu);
    while (nd->gate_closed)
        pthread_cond_wait(&nd->cv, &nd->mu);
    pthread_mutex_unlock(&nd->mu);
    if (type == 1 || type == 4) {
        uint64_t seq;
        if (n < 8) {
            nd->bad_frames++;
            return;
        }
        memcpy(&seq, p, 8);
        uint8_t *want = malloc(n);
        gen_payload(type, want, n, seq);
        if (memcmp(want, p, n) != 0)
            nd->bad_frames++;
        free(want);
        if (nd->have_seq && (nd->strict ? seq != nd->last_seq + 1 : seq <= nd->last_seq))
            nd->order_bad++;
        nd->last_seq = seq;
        nd->have_seq = 1;
        nd->frames_in++;
    } else if (type == 2 && nd->echo) {
        plink_send(l, 3, p, n, 200);
    } else if (type == 3 && n == 8) {
        uint64_t t0;
        memcpy(&t0, p, 8);
        int i = atomic_load(&nd->nrtt);
        if (i < MAXRTT) {
            nd->rtt[i] = now_ns() - t0;
            atomic_store(&nd->nrtt, i + 1);
        }
    }
}

static void on_link(void *arg, struct plink *l, int up)
{
    struct node *nd = arg;
    if (up) {
        atomic_store(&nd->first_type, 0);
        nd->ups++;
        if (nd->link_hello)
            CHECK(plink_send(l, 7, "H", 1, 0) == PL_OK); /* from inside on_link: allowed, and first */
    } else {
        nd->downs++;
    }
}

static int b_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                  uint64_t *resume, int *prefix_fd)
{
    (void)l;
    struct node *nd = arg;
    if (nd->want_meta[0] && (meta_len != strlen(nd->want_meta) || memcmp(meta, nd->want_meta, meta_len) != 0 ||
                             file != 77 || total != nd->want_total))
        nd->meta_bad++;
    uint64_t n = atomic_fetch_add(&nd->opens, 1);
    if (nd->discard) {
        *resume = 0;
        *prefix_fd = -1;
        return 0;
    }
    if (nd->fake_pfd >= 0) { /* a resume prefix that is not the sender's bytes */
        if (nd->fd >= 0)
            close(nd->fd);
        nd->fd = open(nd->path, O_RDWR | O_CREAT | O_TRUNC, 0600);
        *resume = nd->fake_resume;
        *prefix_fd = nd->fake_pfd;
        return nd->fd < 0 ? -1 : 0;
    }
    if (n == 0 || nd->fd < 0 || file != nd->cur_file) {
        nd->cur_file = file;
        if (nd->fd >= 0)
            close(nd->fd);
        nd->fd = open(nd->path, O_RDWR | O_CREAT | O_TRUNC, 0600);
        if (nd->fd < 0)
            return -1;
        *resume = 0;
        *prefix_fd = -1;
    } else {
        struct stat st;
        if (fstat(nd->fd, &st) != 0)
            return -1;
        *resume = (uint64_t)st.st_size;
        *prefix_fd = nd->fd;
    }
    atomic_store(&nd->last_resume, *resume);
    return 0;
}

static int b_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    (void)l;
    (void)file;
    struct node *nd = arg;
    if (atomic_load(&g_fail_data)) return -1;
    if (nd->delay_us)
        usleep(nd->delay_us);
    if (!nd->discard && pwrite(nd->fd, p, n, (off_t)off) != (ssize_t)n)
        return -1;
    atomic_fetch_add(&nd->data_bytes, n);
    return 0;
}

static void b_end(void *arg, struct plink *l, uint64_t file, int status)
{
    (void)l;
    (void)file;
    struct node *nd = arg;
    atomic_store(&nd->last_end, status);
    atomic_fetch_add(&nd->ends, 1);
}

static struct node *node_new(uint32_t id, const uint8_t *secret, struct pl_cfg *over, int listen_it)
{
    struct node *nd = calloc(1, sizeof *nd);
    nd->id = id;
    nd->fd = -1;
    nd->fake_pfd = -1;
    nd->strict = 1;
    nd->echo = 1;
    pthread_mutex_init(&nd->mu, NULL);
    pthread_cond_init(&nd->cv, NULL);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    if (over)
        c = *over;
    c.self.id = id;
    memset(c.self.uuid, g_uuid_byte, 16); /* one cluster: one uuid */
    memset(c.self.ident, 0xB0 + (int)id, 16);
    c.secret = secret;
    c.secret_len = 32;
    c.arg = nd;
    c.on_link = on_link;
    c.on_frame = on_frame;
    c.bulk.open = b_open;
    c.bulk.data = b_data;
    c.bulk.end = b_end;
    if (!c.ping_ms)
        c.ping_ms = 100;
    if (!c.dead_ms)
        c.dead_ms = 1000;
    if (!c.backoff_min_ms) {
        c.backoff_min_ms = 20;
        c.backoff_max_ms = 200;
    }
    nd->pl = peerlink_new(&c);
    if (!nd->pl) {
        fprintf(stderr, "peerlink_new failed\n");
        exit(2);
    }
    if (listen_it) {
        nd->port = peerlink_listen(nd->pl, 0);
        if (nd->port <= 0) {
            fprintf(stderr, "peerlink_listen failed\n");
            exit(2);
        }
    }
    return nd;
}

static void node_free(struct node *nd)
{
    pthread_mutex_lock(&nd->mu);
    nd->gate_closed = 0;
    pthread_cond_broadcast(&nd->cv);
    pthread_mutex_unlock(&nd->mu);
    peerlink_free(nd->pl);
    if (nd->fd >= 0)
        close(nd->fd);
    free(nd);
}

static void set_gate(struct node *nd, int closed)
{
    pthread_mutex_lock(&nd->mu);
    nd->gate_closed = closed;
    pthread_cond_broadcast(&nd->cv);
    pthread_mutex_unlock(&nd->mu);
}

static struct plink *dial(struct node *from, uint32_t want, int port, int kind)
{
    char addr[64];
    snprintf(addr, sizeof addr, "127.0.0.1:%d", port);
    struct plink *l = peerlink_dial(from->pl, want, addr, kind);
    if (!l) {
        fprintf(stderr, "peerlink_dial failed\n");
        exit(2);
    }
    return l;
}

/* `count` frames seq from.., type 1 (random) or 4 (text), `len` bytes each */
static int send_frames(struct plink *l, uint8_t type, uint64_t from, int count, size_t len, int timeout_ms)
{
    uint8_t *b = malloc(len);
    int rc = 0;
    for (int i = 0; i < count && rc == 0; i++) {
        gen_payload(type, b, len, from + (uint64_t)i);
        rc = plink_send(l, type, b, len, timeout_ms);
    }
    free(b);
    return rc;
}

/* ---------------------------------------------------------------- a proxy: blackhole, flip, cut */

struct proxy;
struct ppair {
    struct proxy *px;
    int a, b; /* a: the dialer's side, b: the target's */
    pthread_t t[2];
    int dir;
};

struct proxy {
    int lfd, port, target;
    pthread_t thr;
    _Atomic int stop, blackhole, flip;
    pthread_mutex_t mu;
    struct ppair *pairs[256];
    int np;
    _Atomic uint64_t accepted;
};

static void *pump(void *arg)
{
    struct ppair *pp = arg;
    struct proxy *px = pp->px;
    int dir = pp->dir;
    int from = dir ? pp->b : pp->a, to = dir ? pp->a : pp->b;
    uint8_t buf[65536];
    for (;;) {
        while (atomic_load(&px->blackhole) && !atomic_load(&px->stop))
            usleep(1000);
        ssize_t n = read(from, buf, sizeof buf);
        if (n <= 0)
            break;
        while (atomic_load(&px->blackhole) && !atomic_load(&px->stop))
            usleep(1000);
        if (!dir && n >= 300 && atomic_exchange(&px->flip, 0))
            buf[200] ^= 0x10;
        for (ssize_t o = 0; o < n;) {
            ssize_t w = write(to, buf + o, (size_t)(n - o));
            if (w <= 0)
                goto out;
            o += w;
        }
    }
out:
    shutdown(from, SHUT_RDWR);
    shutdown(to, SHUT_RDWR);
    return NULL;
}

static void *accept_main(void *arg)
{
    struct proxy *px = arg;
    while (!atomic_load(&px->stop)) {
        struct pollfd pf = {.fd = px->lfd, .events = POLLIN};
        if (poll(&pf, 1, 50) <= 0)
            continue;
        int a = accept(px->lfd, NULL, NULL);
        if (a < 0)
            continue;
        int b = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)px->target)};
        inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
        if (b < 0 || connect(b, (struct sockaddr *)&sa, sizeof sa) != 0) {
            close(a);
            if (b >= 0)
                close(b);
            continue;
        }
        int one = 1;
        setsockopt(a, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        setsockopt(b, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        pthread_mutex_lock(&px->mu);
        if (px->np + 2 > 256) {
            pthread_mutex_unlock(&px->mu);
            close(a);
            close(b);
            continue;
        }
        for (int d = 0; d < 2; d++) {
            struct ppair *pp = calloc(1, sizeof *pp);
            pp->px = px;
            pp->a = a;
            pp->b = b;
            pp->dir = d;
            px->pairs[px->np++] = pp;
            pthread_create(&pp->t[0], NULL, pump, pp);
        }
        pthread_mutex_unlock(&px->mu);
        px->accepted++;
    }
    return NULL;
}

static struct proxy *proxy_new(int target)
{
    struct proxy *px = calloc(1, sizeof *px);
    pthread_mutex_init(&px->mu, NULL);
    px->target = target;
    px->lfd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(px->lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (bind(px->lfd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(px->lfd, 16) != 0) {
        perror("proxy listen");
        exit(2);
    }
    socklen_t sl = sizeof sa;
    getsockname(px->lfd, (struct sockaddr *)&sa, &sl);
    px->port = ntohs(sa.sin_port);
    pthread_create(&px->thr, NULL, accept_main, px);
    return px;
}

/* closes every connection through the proxy (the listener stays: a redial goes through) */
static void proxy_cut(struct proxy *px)
{
    pthread_mutex_lock(&px->mu);
    for (int i = 0; i < px->np; i++) {
        shutdown(px->pairs[i]->a, SHUT_RDWR);
        shutdown(px->pairs[i]->b, SHUT_RDWR);
    }
    pthread_mutex_unlock(&px->mu);
}

static void proxy_free(struct proxy *px)
{
    atomic_store(&px->stop, 1);
    pthread_join(px->thr, NULL);
    proxy_cut(px);
    for (int i = 0; i < px->np; i++)
        pthread_join(px->pairs[i]->t[0], NULL);
    for (int i = 0; i < px->np; i++) {
        /* both pumps of a pair share the two fds: close each once (the first of the two entries) */
        if (px->pairs[i]->dir == 0) {
            close(px->pairs[i]->a);
            close(px->pairs[i]->b);
        }
        free(px->pairs[i]);
    }
    close(px->lfd);
    free(px);
}

/* ---------------------------------------------------------------- files */

static uint64_t make_file(const char *path, size_t size, uint64_t seed)
{
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        perror(path);
        exit(2);
    }
    size_t blk = 1u << 20;
    uint8_t *b = malloc(blk);
    for (size_t o = 0; o < size; o += blk) {
        size_t n = size - o < blk ? size - o : blk;
        fill_random(b, n, seed + o / blk);
        if (pwrite(fd, b, n, (off_t)o) != (ssize_t)n) {
            perror("pwrite");
            exit(2);
        }
    }
    free(b);
    return (uint64_t)fd;
}

static int same_file(int fd_a, int fd_b, size_t size)
{
    size_t blk = 1u << 20;
    uint8_t *a = malloc(blk), *b = malloc(blk);
    int same = 1;
    for (size_t o = 0; o < size && same; o += blk) {
        size_t n = size - o < blk ? size - o : blk;
        if (pread(fd_a, a, n, (off_t)o) != (ssize_t)n || pread(fd_b, b, n, (off_t)o) != (ssize_t)n ||
            memcmp(a, b, n) != 0)
            same = 0;
    }
    free(a);
    free(b);
    return same;
}

struct jres {
    _Atomic int done;
    int status;
    uint64_t sent, file;
};

static void job_done(void *arg, uint64_t file, int status, uint64_t sent)
{
    struct jres *r = arg;
    r->status = status;
    r->sent = sent;
    r->file = file;
    atomic_store(&r->done, 1);
}

static int bulk_send_id(struct plink *l, uint64_t file, int fd, uint64_t length, const char *meta, struct jres *r)
{
    struct pl_job j;
    memset(&j, 0, sizeof j);
    j.file = file;
    j.fd = fd;
    j.base = 0;
    j.length = length;
    j.meta = meta;
    j.meta_len = strlen(meta);
    j.max_attempts = 20;
    j.done = job_done;
    j.arg = r;
    return plink_bulk_send(l, &j);
}

static int bulk_send(struct plink *l, int fd, uint64_t length, const char *meta, struct jres *r)
{
    return bulk_send_id(l, 77, fd, length, meta, r);
}

static void pathf(char *out, size_t cap, const char *name)
{
    snprintf(out, cap, "%s/peerlink-%d-%s", scratch, (int)getpid(), name);
}

/* ---------------------------------------------------------------- tests */

static void test_log(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l), 3000));
    CHECK(plink_peer(l) == 1 && plink_kind(l) == PL_KIND_LOG);
    CHECK(WAIT_FOR(peerlink_find(a->pl, 2, PL_KIND_LOG) != NULL, 3000));
    CHECK(WAIT_FOR(a->ups >= 1 && b->ups >= 1, 3000));
    int n = quick ? 1000 : 4000;
    uint64_t rs = 5;
    uint64_t seq = 0;
    for (int i = 0; i < n; i++) {
        size_t len = 8 + rng_next(&rs) % 3000;
        uint8_t type = (i & 1) ? 4 : 1;
        if (i % 97 == 0)
            len = 8 + 100000;
        /* one sequence across both kinds: a type 4 frame is a text one */
        uint8_t *buf = malloc(len);
        gen_payload(type, buf, len, seq++);
        CHECK(plink_send(l, type, buf, len, 5000) == PL_OK);
        free(buf);
    }
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)n, 10000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    /* an echo, from the acceptor's side too (its link is the one a frame arrived on) */
    uint64_t t0 = now_ns();
    CHECK(plink_send(l, 2, &t0, 8, 1000) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&b->nrtt) == 1, 3000));
    struct pl_stats sb, sa;
    peerlink_stats(b->pl, &sb);
    peerlink_stats(a->pl, &sa);
    CHECK(sb.frames_out >= (uint64_t)n && sa.frames_in >= (uint64_t)n);
    CHECK(sb.raw_out > sb.bytes_out); /* the text frames went through zstd */
    /* the acceptor sends to the dialer on its own accepted link */
    struct plink *al = peerlink_find(a->pl, 2, PL_KIND_LOG);
    CHECK(al != NULL);
    b->have_seq = 0;
    if (al) {
        CHECK(send_frames(al, 1, 0, 50, 500, 1000) == PL_OK);
        CHECK(WAIT_FOR(b->frames_in == 50, 3000));
        CHECK(b->bad_frames == 0 && b->order_bad == 0);
    }
    /* an id outside the driver's types is refused at the sender */
    CHECK(plink_send(l, 0x70, "x", 1, 0) == PL_E_INVALID);
    CHECK(plink_send(l, 0, "x", 1, 0) == PL_E_INVALID);
    CHECK(plink_send(l, 1, "x", (size_t)PL_FRAME_MAX + 1, 0) == PL_E_TOOBIG);
    node_free(b);
    node_free(a);
}

static void test_bulk(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "bulk-src");
    pathf(dst, sizeof dst, "bulk-dst");
    size_t size = (quick ? 6u : 20u) * 1000 * 1000 + 12345; /* not a multiple of a chunk */
    int sfd = (int)make_file(src, size, 11);
    /* no rate limit and a long ping interval: nothing but the socket paces the file. The link used to go back to
     * poll() with an empty write buffer and wait for its ping timer between two megabytes (found by --bench). */
    struct pl_cfg bc;
    memset(&bc, 0, sizeof bc);
    bc.bulk_rate = PL_RATE_UNLIMITED;
    bc.ping_ms = 5000;
    bc.dead_ms = 20000;
    struct node *a = node_new(1, SECRET, &bc, 1), *b = node_new(2, SECRET, &bc, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    strcpy(a->want_meta, "vol3/p17_0.part");
    a->want_total = size;
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    struct jres r = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send(l, sfd, size, a->want_meta, &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 30000));
    CHECK(now_ms() - t0 < 2000); /* an unthrottled file runs at the socket's pace, not the ping timer's */
    CHECK(r.status == PL_OK);
    CHECK(WAIT_FOR(a->ends == 1, 3000) && a->last_end == PL_OK);
    CHECK(a->meta_bad == 0 && a->opens == 1);
    CHECK(a->data_bytes == size);
    CHECK(same_file(sfd, a->fd, size));
    /* a second file on the same link; a refused one */
    struct jres r2 = {0};
    a->want_meta[0] = 0;
    CHECK(bulk_send_id(l, 78, sfd, 1000, "second", &r2) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r2.done), 10000) && r2.status == PL_OK);
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    CHECK(sb.bulk_files == 2);
    /* a length above the file is the sender's error, not the receiver's */
    struct jres r3 = {0};
    CHECK(bulk_send_id(l, 79, sfd, size + 4096, "short-file", &r3) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r3.done), 10000) && r3.status != PL_OK);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}

static void test_resume(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "res-src");
    pathf(dst, sizeof dst, "res-dst");
    size_t size = (quick ? 12u : 30u) * 1000 * 1000 + 777;
    int sfd = (int)make_file(src, size, 21);
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = 30ull << 20; /* the file takes about a second: time to cut it */
    struct node *b = node_new(2, SECRET, &c, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_BULK);
    struct jres r = {0};
    CHECK(bulk_send(l, sfd, size, "resume-me", &r) == PL_OK);
    int cuts = 0;
    double marks[2] = {0.25, 0.6};
    for (int k = 0; k < 2; k++) {
        if (!WAIT_FOR(atomic_load(&r.done) || (a->fd >= 0 && lseek(a->fd, 0, SEEK_END) >= (off_t)(marks[k] * (double)size)),
                      20000))
            break;
        if (atomic_load(&r.done))
            break;
        proxy_cut(px);
        cuts++;
        /* wait until the redial's file has started again before the next cut */
        uint64_t o = a->opens;
        WAIT_FOR(a->opens > o || atomic_load(&r.done), 10000);
    }
    CHECK(WAIT_FOR(atomic_load(&r.done), 60000));
    CHECK(r.status == PL_OK);
    CHECK(cuts == 2);
    CHECK(a->opens >= 3);
    CHECK(a->last_resume > 0);
    CHECK(r.sent < size); /* the last attempt sent less than the file */
    CHECK(a->last_end == PL_OK);
    CHECK(same_file(sfd, a->fd, size));
    struct stat st;
    CHECK(fstat(a->fd, &st) == 0 && (size_t)st.st_size == size);
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    CHECK(sb.connects >= 3);
    printf("  resume: %llu opens, last resume offset %llu of %zu, last attempt sent %llu\n",
           (unsigned long long)a->opens, (unsigned long long)a->last_resume, size, (unsigned long long)r.sent);
    node_free(b);
    node_free(a);
    proxy_free(px);
    close(sfd);
    unlink(src);
    unlink(dst);
}

static void test_secret(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *c = node_new(3, SECRET2, NULL, 0); /* another cluster */
    struct node *d = node_new(4, SECRET, NULL, 0);  /* the right secret and node id */
    struct node *e = node_new(5, SECRET, NULL, 0);  /* the right secret, but asks for another node id */
    struct plink *lc = dial(c, 1, a->port, PL_KIND_LOG);
    struct plink *lc2 = dial(c, 1, a->port, PL_KIND_BULK);
    struct plink *ld = dial(e, 9, a->port, PL_KIND_LOG);
    struct plink *ok = dial(d, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(ok), 3000));
    usleep(1200 * 1000);
    CHECK(!plink_up(lc) && !plink_up(lc2));
    CHECK(!plink_up(ld));
    struct pl_stats sa, sc, sd;
    peerlink_stats(a->pl, &sa);
    peerlink_stats(c->pl, &sc);
    peerlink_stats(d->pl, &sd);
    CHECK(sa.auth_failures + sc.auth_failures > 0);
    CHECK(c->ups == 0 && a->frames_in == 0);
    /* nothing was delivered from the wrong peer */
    CHECK(plink_send(lc, 1, "x", 1, 0) == PL_E_DOWN);
    CHECK(plink_up(ok)); /* and the good one is unharmed */
    node_free(e);
    node_free(d);
    node_free(c);
    node_free(a);
}

static void test_flip(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    a->strict = 0; /* a drop loses frames; the order inside a connection must still hold */
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    CHECK(send_frames(l, 1, 0, 20, 1500, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 20, 3000));
    int downs0 = a->downs, ups0 = b->ups;
    atomic_store(&px->flip, 1);
    /* 1500-byte random frames: byte 200 of the next read is inside a payload (a frame is 1537 bytes) */
    uint64_t sent_to = 20;
    for (int i = 0; i < 40; i++) {
        send_frames(l, 1, sent_to, 1, 1500, 100);
        sent_to++;
        usleep(3000);
    }
    CHECK(WAIT_FOR(a->downs > downs0, 3000)); /* the acceptor ended the link */
    CHECK(a->bad_frames == 0);                /* what came through is exactly what was sent */
    CHECK(a->order_bad == 0);
    /* the dialer redials and the stream goes on */
    CHECK(WAIT_FOR(b->ups > ups0 && plink_up(l), 5000));
    uint64_t before = a->frames_in;
    CHECK(send_frames(l, 1, 1000, 10, 1500, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in >= before + 10, 3000));
    CHECK(a->bad_frames == 0);
    node_free(b);
    node_free(a);
    proxy_free(px);
}

static void test_blackhole(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    CHECK(send_frames(l, 1, 0, 10, 300, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 10, 3000));
    a->strict = 0;
    int bdown0 = b->downs, bups0 = b->ups;
    atomic_store(&px->blackhole, 1);
    usleep(50 * 1000);
    uint64_t in0 = a->frames_in;
    /* frames sent into the hole: queued or refused, never delivered */
    for (int i = 0; i < 20; i++) {
        send_frames(l, 1, 100 + (uint64_t)i, 1, 300, 0);
        usleep(20 * 1000);
    }
    CHECK(WAIT_FOR(b->downs > bdown0, 4000)); /* dead_ms 1000: the dialer noticed the silence */
    CHECK(a->frames_in == in0);               /* the pair exchanged nothing meanwhile */
    atomic_store(&px->blackhole, 0);
    proxy_cut(px); /* what the hole held belongs to dead connections */
    CHECK(WAIT_FOR(b->ups > bups0 && plink_up(l), 6000));
    uint64_t before = a->frames_in;
    CHECK(WAIT_FOR(send_frames(l, 1, 1000, 10, 300, 100) == PL_OK, 3000));
    CHECK(WAIT_FOR(a->frames_in >= before + 10, 3000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    node_free(b);
    node_free(a);
    proxy_free(px);
}

static void test_backpressure(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.queue_bytes = 1u << 20;
    c.dead_ms = 30000;
    c.ping_ms = 5000;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    set_gate(a, 1); /* the receiver stops reading after the frame it holds */
    int accepted = 0, full = 0;
    size_t maxq = 0;
    const size_t len = 65536;
    for (int i = 0; i < 2000 && !full; i++) {
        uint8_t *buf = malloc(len);
        gen_payload(1, buf, len, (uint64_t)i);
        int rc = plink_send(l, 1, buf, len, 0);
        free(buf);
        if (rc == PL_OK)
            accepted++;
        else if (rc == PL_E_FULL)
            full = 1;
        else
            CHECK(rc == PL_OK || rc == PL_E_FULL);
        size_t q = plink_queued(l);
        if (q > maxq)
            maxq = q;
        if (accepted % 8 == 0)
            usleep(500); /* the I/O thread moves the queue into the socket meanwhile */
    }
    CHECK(full);
    CHECK(maxq <= (1u << 20) + len); /* the bound, a frame over at most */
    printf("  backpressure: %d frames of 64 KiB accepted before FULL, queue peak %zu bytes\n", accepted, maxq);
    /* a timed send waits for room (the socket may still take a little) and gives up with FULL */
    int rc = PL_OK;
    uint64_t waited = 0;
    for (int tries = 0; tries < 1000 && rc == PL_OK; tries++) {
        uint8_t *tb = malloc(len);
        gen_payload(1, tb, len, (uint64_t)accepted);
        uint64_t t0 = now_ms();
        rc = plink_send(l, 1, tb, len, 250);
        waited = now_ms() - t0;
        free(tb);
        if (rc == PL_OK)
            accepted++;
    }
    if (rc != PL_E_FULL)
        fprintf(stderr, "timed send: rc %d after %llu ms\n", rc, (unsigned long long)waited);
    CHECK(rc == PL_E_FULL);
    CHECK(waited >= 200 && waited < 2000);
    CHECK(plink_queued(l) <= (1u << 20) + len);
    set_gate(a, 0);
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)accepted, 15000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    CHECK(WAIT_FOR(plink_queued(l) == 0, 3000));
    /* room again: a send goes through */
    CHECK(send_frames(l, 1, (uint64_t)accepted, 1, 100, 1000) == PL_OK);
    node_free(b);
    node_free(a);
}

/* a backlog of frames larger than the write buffer, on a link whose ping timer is long: the link must go on at the
 * socket's pace and not wait for the timer between two buffers (found by --bench together with the bulk case) */
static void test_backlog(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.queue_bytes = 32u << 20;
    c.ping_ms = 5000;
    c.dead_ms = 20000;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    int n = 200;
    uint64_t t0 = now_ms();
    CHECK(send_frames(l, 1, 0, n, 65536, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)n, 15000));
    CHECK(now_ms() - t0 < 2000);
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    node_free(b);
    node_free(a);
}

static int cmp_u64(const void *x, const void *y)
{
    uint64_t a = *(const uint64_t *)x, b = *(const uint64_t *)y;
    return a < b ? -1 : a > b;
}

static void test_latency(void)
{
    char src[600];
    pathf(src, sizeof src, "lat-src");
    size_t size = (quick ? 32u : 64u) << 20;
    int sfd = (int)make_file(src, size, 31);
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = 200ull << 20;
    c.dead_ms = 20000;
    c.ping_ms = 5000;
    struct node *b = node_new(2, SECRET, &c, 0);
    a->discard = 1;
    a->delay_us = 5000; /* a receiver whose disk takes 5 ms a chunk */
    struct plink *ll = dial(b, 1, a->port, PL_KIND_LOG);
    struct plink *bl = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(ll) && a->ups >= 1, 3000));
    if (peerlink_mutant != PL_MUT_SHAREDSOCKET)
        CHECK(WAIT_FOR(plink_up(bl) && a->ups >= 2, 3000));
    struct jres r = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send(bl, sfd, size, "big", &r) == PL_OK);
    WAIT_FOR(a->data_bytes > (4u << 20), 5000); /* the file is flowing */
    int n = 0;
    while (!atomic_load(&r.done) && n < 200 && now_ms() < t0 + 40000) {
        uint64_t ts = now_ns();
        int want = atomic_load(&b->nrtt) + 1;
        if (plink_send(ll, 2, &ts, 8, 2000) != PL_OK)
            break;
        WAIT_FOR(atomic_load(&b->nrtt) >= want, 5000);
        n++;
        usleep(1000);
    }
    int during = atomic_load(&r.done) ? 0 : 1;
    CHECK(WAIT_FOR(atomic_load(&r.done), 60000));
    CHECK(r.status == PL_OK);
    int m = atomic_load(&b->nrtt);
    CHECK(m >= 20);
    if (m > 1) {
        uint64_t v[MAXRTT];
        memcpy(v, b->rtt, (size_t)m * 8);
        qsort(v, (size_t)m, 8, cmp_u64);
        uint64_t p50 = v[m / 2], p90 = v[m * 9 / 10], mx = v[m - 1];
        printf("  latency: %d round trips with a %zu MB file in flight (%s): p50 %.2f ms, p90 %.2f ms, max %.2f ms\n",
               m, size >> 20, during ? "still flowing" : "done", (double)p50 / 1e6, (double)p90 / 1e6,
               (double)mx / 1e6);
        CHECK(p50 < 10000000ull);
        CHECK(p90 < 40000000ull);
    }
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
}

/* ---------------------------------------------------------------- bench */

static double secs(uint64_t t0) { return (double)(now_ns() - t0) / 1e9; }

static void bench_hash(void)
{
    size_t n = 64u << 20;
    uint8_t *buf = malloc(n);
    fill_random(buf, n, 1);
    for (int rep = 0; rep < 3; rep++) {
        uint8_t out[32];
        uint64_t t = now_ns();
        tsdb_sha256_buf(buf, n, out);
        double s1 = secs(t);
        t = now_ns();
        volatile uint64_t h = xxh64(buf, n, 0);
        (void)h;
        double s2 = secs(t);
        /* a link's per-frame tag: HMAC over header + chunk, here over 256 KiB pieces */
        t = now_ns();
        for (size_t o = 0; o < n; o += 256u << 10)
            tsdb_hmac_sha256(SECRET, 32, buf + o, 256u << 10, out);
        double s3 = secs(t);
        printf("  bench hash rep %d: sha256 %.0f MB/s, hmac-sha256 over 256 KiB %.0f MB/s, xxh64 %.0f MB/s\n", rep + 1,
               (double)n / 1e6 / s1, (double)n / 1e6 / s3, (double)n / 1e6 / s2);
    }
    free(buf);
}

static void bench_bulk(uint32_t chunk, size_t size, int reps)
{
    char src[600];
    pathf(src, sizeof src, "bench-src");
    int sfd = (int)make_file(src, size, 41);
    double sum = 0;
    for (int rep = 0; rep < reps; rep++) {
        struct pl_cfg c;
        memset(&c, 0, sizeof c);
        c.bulk_rate = PL_RATE_UNLIMITED;
        c.bulk_chunk = chunk;
        c.dead_ms = 20000;
        c.ping_ms = 5000;
        struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
        a->discard = 1;
        struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
        WAIT_FOR(plink_up(l), 3000);
        struct jres r = {0};
        uint64_t t = now_ns();
        bulk_send(l, sfd, size, "b", &r);
        WAIT_FOR(atomic_load(&r.done), 120000);
        double s = secs(t);
        if (r.status != PL_OK)
            printf("  bench bulk FAILED status %d\n", r.status);
        sum += (double)size / 1e6 / s;
        printf("  bench bulk chunk %u KiB rep %d: %.0f MB/s (%.2f s, %zu MiB)\n", chunk >> 10, rep + 1,
               (double)size / 1e6 / s, s, size >> 20);
        node_free(b);
        node_free(a);
    }
    printf("  bench bulk chunk %u KiB mean: %.0f MB/s\n", chunk >> 10, sum / reps);
    close(sfd);
    unlink(src);
}

static void bench_log(int reps, size_t len)
{
    double sum = 0;
    for (int rep = 0; rep < reps; rep++) {
        struct pl_cfg c;
        memset(&c, 0, sizeof c);
        c.queue_bytes = 8u << 20;
        c.dead_ms = 20000;
        c.ping_ms = 5000;
        struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
        struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
        WAIT_FOR(plink_up(l) && a->ups >= 1, 3000);
        int n = 1000000;
        uint8_t *buf = malloc(len);
        uint64_t t = now_ns();
        for (int i = 0; i < n; i++) {
            gen_payload(4, buf, len, (uint64_t)i);
            if (plink_send(l, 4, buf, len, 5000) != PL_OK) {
                printf("  bench log send failed at %d\n", i);
                break;
            }
        }
        WAIT_FOR(a->frames_in == (uint64_t)n, 60000);
        double s = secs(t);
        struct pl_stats sb;
        peerlink_stats(b->pl, &sb);
        printf("  bench log rep %d: %.0f frames/s (%d frames of %zu bytes in %.2f s, %.1f MB/s of payload, wire %.1f%% "
               "of raw)\n",
               rep + 1, (double)a->frames_in / s, n, len, s, (double)a->frames_in * (double)len / 1e6 / s,
               100.0 * (double)sb.bytes_out / (double)(sb.raw_out ? sb.raw_out : 1));
        sum += (double)a->frames_in / s;
        free(buf);
        node_free(b);
        node_free(a);
    }
    printf("  bench log mean: %.0f frames/s\n", sum / reps);
}

static void bench(void)
{
    bench_hash();
    size_t size = 512u << 20;
    uint32_t chunks[] = {64u << 10, 256u << 10, 1u << 20, 4u << 20};
    for (int i = 0; i < 4; i++)
        bench_bulk(chunks[i], size, 3);
    bench_log(3, 80);
}

/* ---------------------------------------------------------------- main */

struct tcase {
    const char *name;
    void (*fn)(void);
};

/* a peer with the wrong secret but the same node id as a live dialer: the live accepted link must survive */
static void test_kick(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d = node_new(4, SECRET, NULL, 0);
    struct plink *ok = dial(d, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(ok) && a->ups >= 1, 3000));
    int adown0 = a->downs, dups0 = d->ups;
    struct node *c = node_new(4, SECRET2, NULL, 0);
    struct plink *lc = dial(c, 1, a->port, PL_KIND_LOG);
    usleep(3000 * 1000);
    printf("  kick: acceptor downs %d->%d, live dialer ups %d->%d, ok up %d, wrong-secret up %d\n", adown0,
           (int)a->downs, dups0, (int)d->ups, plink_up(ok), plink_up(lc));
    CHECK(a->downs == adown0 && d->ups == dups0);
    node_free(c);
    node_free(d);
    node_free(a);
}
/* the receiver's data callback fails: the header promises PL_E_IO to the sender */
static void test_dataerr(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "de-src");
    pathf(dst, sizeof dst, "de-dst");
    size_t size = 4u << 20;
    int sfd = (int)make_file(src, size, 5);
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *b = node_new(2, SECRET, NULL, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(l), 3000));
    atomic_store(&g_fail_data, 1);
    struct jres r = {0};
    CHECK(bulk_send(l, sfd, size, "x", &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 30000));
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    printf("  dataerr: status %d (PL_E_IO=%d), receiver opens %llu, sender connects %llu, dialer downs %d\n", r.status,
           PL_E_IO, (unsigned long long)a->opens, (unsigned long long)sb.connects, (int)b->downs);
    CHECK(r.status == PL_E_IO);
    atomic_store(&g_fail_data, 0);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}
struct opener {
    struct node *nd;
    unsigned ms;
};
static void *opener_main(void *arg)
{
    struct opener *o = arg;
    usleep(o->ms * 1000);
    set_gate(o->nd, 0);
    return NULL;
}

/* a driver callback slower than dead_ms (the receiver's disk stalls 2.5 s, dead_ms is 1 s) while the sender has more
 * to send: TCP stops the sender, which is held back and not silent, and the receiver does not count its own callback's
 * time against the peer: no link goes down, and every frame the sender was told PL_OK for arrives */
static void test_slowcb(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *b = node_new(2, SECRET, NULL, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    int ad = a->downs, bd = b->downs;
    set_gate(a, 1);
    struct opener o = {a, 2500};
    pthread_t t;
    pthread_create(&t, NULL, opener_main, &o);
    const int count = 400; /* 25 MB of random bytes: more than the queue and both socket buffers hold */
    CHECK(send_frames(l, 1, 0, count, 65536, 15000) == PL_OK);
    pthread_join(t, NULL);
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)count, 15000));
    usleep(1500 * 1000); /* a late drop would show now */
    printf("  slowcb: acceptor downs %d->%d, dialer downs %d->%d, frames_in %llu of %d\n", ad, (int)a->downs, bd,
           (int)b->downs, (unsigned long long)a->frames_in, count);
    CHECK(a->downs == ad && b->downs == bd && a->frames_in == (uint64_t)count && a->order_bad == 0);
    node_free(b);
    node_free(a);
}

/* ---------------------------------------------------------------- a raw peer: the acceptor's own checks */

static int raw_connect(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        fprintf(stderr, "raw connect: %s\n", strerror(errno));
        exit(2);
    }
    return fd;
}

/* the cluster HELLO as a client sends it: [u32 len][type 1][magic, version, id, uuid, ident, nonce, address, kind] */
static void raw_hello(int fd, uint32_t id, uint8_t uuid_byte, int kind)
{
    uint8_t b[128];
    size_t o = 4;
    b[o++] = 1;
    uint32_t magic = PL_CL_MAGIC;
    memcpy(b + o, &magic, 4);
    o += 4;
    b[o++] = PL_CL_VERSION;
    memcpy(b + o, &id, 4);
    o += 4;
    memset(b + o, uuid_byte, 16);
    o += 16;
    memset(b + o, 0xB0 + (int)id, 16);
    o += 16;
    fill_random(b + o, 32, 5);
    o += 32;
    b[o++] = 0; /* no address */
    b[o++] = 0;
    b[o++] = (uint8_t)kind;
    uint32_t len = (uint32_t)(o - 4);
    memcpy(b, &len, 4);
    CHECK(write(fd, b, o) == (ssize_t)o);
}

/* the next frame within ms: its type, or -1 when the peer closed or said nothing */
static int raw_read(int fd, int ms)
{
    uint8_t h[5];
    size_t got = 0;
    uint64_t end = now_ms() + (uint64_t)ms;
    while (got < 5) {
        struct pollfd pf = {fd, POLLIN, 0};
        int left = (int)(end - now_ms());
        if (left <= 0 || poll(&pf, 1, left) <= 0)
            return -1;
        ssize_t r = read(fd, h + got, 5 - got);
        if (r <= 0)
            return -1;
        got += (size_t)r;
    }
    uint32_t len;
    memcpy(&len, h, 4);
    for (uint32_t i = 1; i < len;) { /* the rest of the body */
        uint8_t tmp[256];
        struct pollfd pf = {fd, POLLIN, 0};
        int left = (int)(end - now_ms());
        if (left <= 0 || poll(&pf, 1, left) <= 0)
            return -1;
        ssize_t r = read(fd, tmp, len - i < sizeof tmp ? len - i : sizeof tmp);
        if (r <= 0)
            return -1;
        i += (uint32_t)r;
    }
    return h[4];
}

/* what the ACCEPTOR checks, played by a raw socket (the dialer's own checks would hide a missing one): a wrong AUTH
 * ends the connection and counts, a HELLO of another cluster is not even answered, a kind it does not know too */
static void test_rawauth(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    int fd = raw_connect(a->port);
    raw_hello(fd, 4, (uint8_t)g_uuid_byte, PL_KIND_LOG);
    CHECK(raw_read(fd, 3000) == 2); /* HELLO_ACK */
    uint8_t auth[5 + 32];
    uint32_t len = 33;
    memcpy(auth, &len, 4);
    auth[4] = 3;
    memset(auth + 5, 0x5a, 32); /* a MAC made without the secret */
    CHECK(write(fd, auth, sizeof auth) == (ssize_t)sizeof auth);
    CHECK(raw_read(fd, 3000) == -1); /* closed, nothing more said */
    usleep(200 * 1000);
    struct pl_stats sa;
    peerlink_stats(a->pl, &sa);
    CHECK(sa.auth_failures >= 1);
    CHECK(a->ups == 0 && peerlink_find(a->pl, 4, PL_KIND_LOG) == NULL); /* no link for a peer that failed AUTH */
    close(fd);
    /* another cluster's uuid: no ACK at all */
    fd = raw_connect(a->port);
    raw_hello(fd, 4, (uint8_t)(g_uuid_byte + 1), PL_KIND_LOG);
    CHECK(raw_read(fd, 3000) == -1);
    close(fd);
    CHECK(a->ups == 0);
    node_free(a);
}

/* a file whose receiver holds a prefix that is not the sender's bytes: the file's xxh64 does not match, and both ends
 * are told (PL_E_HASH): the per-file hash is what catches a resume from a damaged partial file */
static void test_endhash(void)
{
    char src[600], dst[600], bad[600];
    pathf(src, sizeof src, "eh-src");
    pathf(dst, sizeof dst, "eh-dst");
    pathf(bad, sizeof bad, "eh-bad");
    size_t size = 3u << 20;
    int sfd = (int)make_file(src, size, 21);
    int bfd = (int)make_file(bad, size, 22); /* other bytes */
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    a->fake_pfd = bfd;
    a->fake_resume = 1u << 20;
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    struct jres r = {0};
    CHECK(bulk_send(l, sfd, size, "x", &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 20000));
    CHECK(r.status == PL_E_HASH);
    CHECK(WAIT_FOR(a->ends == 1, 3000) && a->last_end == PL_E_HASH);
    /* a receiver that holds a longer prefix than the file is refused, not believed */
    a->fake_resume = size + 4096;
    struct jres r2 = {0};
    CHECK(bulk_send_id(l, 78, sfd, 1u << 19, "y", &r2) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r2.done), 20000));
    CHECK(r2.status == PL_E_REFUSED);
    node_free(b);
    node_free(a);
    close(sfd);
    close(bfd);
    unlink(src);
    unlink(dst);
    unlink(bad);
}

/* the rate holds: 4 MB at 8 MB/s takes about half a second (50 ms of burst), never the 20 ms of an open pipe */
static void test_throttle(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "th-src");
    pathf(dst, sizeof dst, "th-dst");
    size_t size = 4u * 1000 * 1000;
    int sfd = (int)make_file(src, size, 31);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = 8u * 1000 * 1000;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(l), 3000));
    struct jres r = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send(l, sfd, size, "x", &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 20000));
    uint64_t took = now_ms() - t0;
    printf("  throttle: %zu bytes at 8 MB/s took %llu ms\n", size, (unsigned long long)took);
    CHECK(r.status == PL_OK);
    CHECK(took >= 400 && took < 3000);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}

/* a peer that holds the secret and sends what no honest sender does: a CHUNK that is not at the next offset ends the
 * link, and nothing of it reaches the data callback */
static void test_badchunk(void)
{
    char dst[600];
    pathf(dst, sizeof dst, "bc-dst");
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    uint8_t begin[20 + 1] = {0}, chunk[16 + 10] = {0};
    uint64_t file = 5, total = 100, off = 50;
    memcpy(begin, &file, 8);
    memcpy(begin + 8, &total, 8);
    begin[16] = 1;
    begin[20] = 'm';
    memcpy(chunk, &file, 8);
    memcpy(chunk + 8, &off, 8); /* the receiver expects 0 */
    CHECK(plink_test_ctl(l, PLT_BEGIN, begin, sizeof begin) == PL_OK);
    CHECK(plink_test_ctl(l, PLT_CHUNK, chunk, sizeof chunk) == PL_OK);
    CHECK(WAIT_FOR(a->downs >= 1, 3000));
    CHECK(a->data_bytes == 0);
    node_free(b);
    node_free(a);
    unlink(dst);
}

/* a new connection of a peer whose old connection is still up (a restarted server, a redial the acceptor has not
 * noticed the end of) takes the accepted link over: its frames are delivered, the old one is closed */
static void test_replace(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d1 = node_new(4, SECRET, NULL, 0);
    struct plink *l1 = dial(d1, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l1) && a->ups >= 1, 3000));
    struct node *d2 = node_new(4, SECRET, NULL, 0);
    a->strict = 0;
    struct plink *l2 = dial(d2, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(a->ups >= 2, 3000));
    CHECK(WAIT_FOR(a->downs >= 1, 3000));
    CHECK(send_frames(l2, 1, 1000, 5, 200, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 5, 3000));
    node_free(d2);
    node_free(d1);
    node_free(a);
}

/* free with connections still in their handshake (a peer that says HELLO and nothing more): it returns at once, with
 * every thread joined (the sanitizers see a leak or a race) */
static void test_freehs(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.hs_ms = 20000;
    struct node *a = node_new(1, SECRET, &c, 1);
    int fds[10];
    for (int i = 0; i < 10; i++) {
        fds[i] = raw_connect(a->port);
        raw_hello(fds[i], 4 + (uint32_t)i, (uint8_t)g_uuid_byte, i % 2 ? PL_KIND_LOG : PL_KIND_BULK);
    }
    CHECK(raw_read(fds[0], 500) == 2); /* they are in the handshake, waiting for AUTH */
    uint64_t t0 = now_ms();
    node_free(a);
    printf("  freehs: free with 10 handshakes open took %llu ms\n", (unsigned long long)(now_ms() - t0));
    CHECK(now_ms() - t0 < 3000);
    for (int i = 0; i < 10; i++)
        close(fds[i]);
}

/* a frame is tied to the connection the driver meant: after a reconnect the old generation's send is PL_E_DOWN, and
 * what a driver sends from on_link(up) goes first, before another thread's frames (the link is open for them only
 * once on_link has returned) */
static void *spin_send(void *arg)
{
    struct plink *l = arg;
    for (int i = 0; i < 4000; i++) {
        plink_send(l, 8, "s", 1, 0);
        usleep(100);
    }
    return NULL;
}

static void test_gen(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    b->link_hello = 1;
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    uint64_t g0 = plink_gen(l);
    CHECK(g0 >= 1);
    CHECK(plink_send_gen(l, g0, 8, "x", 1, 0) == PL_OK);
    CHECK(plink_send_gen(l, g0 + 1, 8, "x", 1, 0) == PL_E_DOWN);
    CHECK(plink_send_gen(l, 0, 8, "x", 1, 0) == PL_E_INVALID);
    pthread_t t;
    pthread_create(&t, NULL, spin_send, l);
    for (int round = 0; round < 6; round++) {
        int ups = a->ups;
        proxy_cut(px);
        CHECK(WAIT_FOR(a->ups > ups, 5000));
        CHECK(WAIT_FOR(atomic_load(&a->first_type) != 0, 3000));
        CHECK(atomic_load(&a->first_type) == 7); /* the on_link frame, then the spinner's */
    }
    pthread_join(t, NULL);
    uint64_t g1 = plink_gen(l);
    CHECK(g1 > g0);
    CHECK(plink_send_gen(l, g0, 8, "x", 1, 0) == PL_E_DOWN); /* an older connection's frame is refused */
    proxy_free(px);
    node_free(b);
    node_free(a);
}

static int count_dir(const char *d)
{
    DIR *x = opendir(d);
    int n = 0;
    if (!x)
        return -1;
    while (readdir(x))
        n++;
    closedir(x);
    return n - 2;
}
/* the acceptor's callback is stuck (a driver blocked on a stalled volume) while the dialer goes on redialing: the
 * connections that pile up behind it cost no thread or descriptor beyond the one that waits for the link */
static void test_chain(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.hs_ms = 300; /* a dialer whose connection is not served gives up and redials quickly */
    struct node *a = node_new(1, SECRET, &c, 1);
    struct node *b = node_new(2, SECRET, &c, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    int t0 = count_dir("/proc/self/task"), f0 = count_dir("/proc/self/fd");
    set_gate(a, 1);
    CHECK(send_frames(l, 1, 0, 1, 300, 1000) == PL_OK);
    usleep(6000 * 1000);
    int t1 = count_dir("/proc/self/task"), f1 = count_dir("/proc/self/fd");
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    printf("  chain: threads %d -> %d, fds %d -> %d after 6 s of a stuck callback; dialer connects %llu\n", t0, t1, f0,
           f1, (unsigned long long)sb.connects);
    CHECK(t1 - t0 <= 3 && f1 - f0 <= 6);
    set_gate(a, 0);
    node_free(b);
    node_free(a);
}

int main(int argc, char **argv)
{
    const char *only = NULL;
    int do_bench = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = 1;
        else if (!strcmp(argv[i], "--bench"))
            do_bench = 1;
        else if (!strcmp(argv[i], "--only") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            if (!strcmp(m, "notag")) {
                peerlink_mutant = PL_MUT_NOTAG;
                only = "flip";
            } else if (!strcmp(m, "sharedsocket")) {
                peerlink_mutant = PL_MUT_SHAREDSOCKET;
                only = "latency";
            } else if (!strcmp(m, "nobackpressure")) {
                peerlink_mutant = PL_MUT_NOBACKPRESSURE;
                only = "backpressure";
            } else if (!strcmp(m, "accauth")) {
                peerlink_mutant = PL_MUT_ACCAUTH;
                only = "rawauth";
            } else if (!strcmp(m, "uuid")) {
                peerlink_mutant = PL_MUT_UUID;
                only = "rawauth";
            } else if (!strcmp(m, "endhash")) {
                peerlink_mutant = PL_MUT_ENDHASH;
                only = "endhash";
            } else if (!strcmp(m, "nothrottle")) {
                peerlink_mutant = PL_MUT_NOTHROTTLE;
                only = "throttle";
            } else if (!strcmp(m, "zstdsize")) {
                peerlink_mutant = PL_MUT_ZSTDSIZE;
                only = "codec";
            } else if (!strcmp(m, "chunkoff")) {
                peerlink_mutant = PL_MUT_CHUNKOFF;
                only = "badchunk";
            } else if (!strcmp(m, "resumebound")) {
                peerlink_mutant = PL_MUT_RESUMEBOUND;
                only = "endhash";
            } else if (!strcmp(m, "nokick")) {
                peerlink_mutant = PL_MUT_NOKICK;
                only = "replace";
            } else {
                fprintf(stderr, "unknown mutant %s\n", m);
                return 2;
            }
        } else if (argv[i][0] != '-')
            scratch = argv[i];
        else {
            fprintf(stderr, "usage: test_peerlink [DIR] [--quick] [--only NAME] [--mutant M] [--bench]\n");
            return 2;
        }
    }
    signal(SIGPIPE, SIG_IGN);
    if (do_bench) {
        bench();
        return 0;
    }
    static const struct tcase cases[] = {
        {"codec", test_codec},   {"log", test_log},         {"bulk", test_bulk},
        {"resume", test_resume}, {"secret", test_secret},   {"flip", test_flip},
        {"blackhole", test_blackhole}, {"backpressure", test_backpressure}, {"backlog", test_backlog}, {"latency", test_latency}, {"kick", test_kick}, {"dataerr", test_dataerr}, {"slowcb", test_slowcb}, {"chain", test_chain}, {"rawauth", test_rawauth}, {"endhash", test_endhash},
        {"throttle", test_throttle}, {"badchunk", test_badchunk}, {"replace", test_replace}, {"freehs", test_freehs},
        {"gen", test_gen},
    };
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        if (only && strcmp(only, cases[i].name) != 0)
            continue;
        int f0 = failures;
        uint64_t t = now_ns();
        cases[i].fn();
        printf("%-13s %s (%.1f s)\n", cases[i].name, failures == f0 ? "ok" : "FAILED", secs(t));
        fflush(stdout);
    }
    if (failures) {
        fprintf(stderr, "test_peerlink: %d failures\n", failures);
        return 1;
    }
    printf("test_peerlink: ok\n");
    return 0;
}
