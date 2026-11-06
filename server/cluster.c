/*
 * tsdbd's cluster thread (docs/design-cluster.md §2; step 1 of §14): the metadata group on the peer port.
 *
 * One thread with its own poll loop, like metrics.c, beside the request loop; it shares nothing with the engine in
 * step 1. It drives the Raft core copied from intellistream-objects (src/raft) over the metadata state machine
 * (meta.c), and serves three kinds of peers on one listening socket: other members (Raft messages, heartbeats, the
 * watch stream, JOIN), and `tsdb cluster` (the admin ops).
 *
 * Files, under <dir>/meta/:
 *   NODE      text: cluster=<uuid hex>, node=<id>, incarnation=<n>, seeds=<peer,...>. Written at INIT or JOIN, and
 *             again with incarnation + 1 at every start (tmp, fsync, rename, directory fsync): the incarnation tells
 *             a restarted server from one that only paused.
 *   raft/     the core's log and hard state (raft_log.c), opened only on a server that holds a core.
 *   snapshot  the state machine's image (meta_snapshot_encode) at an applied index, written every SNAP_EVERY
 *             entries and at every RESTORE (tmp, fsync, rename, directory fsync) before the core compacts its log.
 *
 * Roles. A server that is in the Raft configuration (a voter or a learner) holds a core: it boots one from its files
 * at start, or on the first Raft message an authenticated member sends it (the leader added it as a learner). Every
 * other member keeps the state in memory from a **watch stream**: WATCH{from} to a core server, which answers with
 * the committed entries from `from` (from a ring of the last RING applied) or, when those are gone, a snapshot of its
 * state first and the entries after it. An entry whose index does not follow is a broken stream: reconnect. A core
 * server that has known no leader for NOLEADER_PROBE_NS watches as well (into a second state); if what it sees is a
 * configuration without itself, it was removed while away, and it drops its core (its files stay) and goes on as a
 * watcher. So does a core whose own configuration has not held it for NOTMEMBER_DROP_NS.
 *
 * Durability: every action of the core is done inline, in its order (the hard state saved and synced, the log synced
 * through raft_log_sync_prepare / raft_log_sync_run, sends, applies). Metadata changes are rare (tens a minute), so
 * an fsync in the loop costs nothing that matters. A store fault, a hard state that cannot be saved, a sync that
 * fails, an entry this build cannot decode or a snapshot that cannot be installed is fail-stop: tsdbd exits with
 * status 1 and a line naming it (AGENTS.md, fail-stop).
 *
 * Heartbeats: every member sends HB{id, incarnation, applied} to every voter each heartbeat_ms (in memory, never an
 * entry); every voter records when it last heard each member. The Raft leader, once it has led for node_timeout,
 * proposes MEMBER_STATE down for an up member not heard for node_timeout, and up (with the new incarnation) for a down
 * member heard again or an up member that restarted; one proposal per member at a time. **Known gap (§6.1)**: this
 * thread is not the loop that serves the volumes, so a tsdbd whose request loop hangs still heartbeats; step 4 moves
 * the heartbeat there.
 *
 * Voters (§2.2), on the leader once a second while no configuration change is in flight: promote a caught-up learner;
 * else, while fewer voters than meta_voter_target have been heard within voter_replace_s, add an up non-voter as a
 * learner (another rack first); else remove a voter unheard for voter_replace_s while there are more voters than the
 * target; a learner unheard that long goes too. Never itself.
 *
 * Frames: [u32 len][u8 type][payload], len counting the type and the payload, at most FRAME_MAX. Little endian.
 * The dialer sends HELLO{magic, version, its node id (0: the CLI or a server in no cluster), cluster uuid (zero when
 * none), nonce_c[32], its peer address}; the acceptor answers HELLO_ACK{id, uuid, nonce_s[32],
 * HMAC(secret, "tsdb-cluster-s" | nonce_c | nonce_s)}, the dialer checks it and sends AUTH{HMAC(secret,
 * "tsdb-cluster-c" | nonce_c | nonce_s)}. Two known uuids that differ end the connection. Nothing else passes before
 * AUTH. The secret is the cluster's (--cluster-secret-file); it is never sent.
 */
#define _GNU_SOURCE
#include "cluster.h"

#include "meta.h"
#include "raft/raft.h"
#include "raft/raft_log.h"
#include "raft/raft_wire.h"
#include "tsdb_crypto.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define FRAME_MAX (16u << 20)
#define WB_MAX (64u << 20) /* queued to one peer; beyond it the link is dropped (Raft resends) */
#define CL_MAGIC 0x43445354u /* "TSDC" */
#define CL_VERSION 1
#define RING 4096
#define SNAP_EVERY 1000
#define MS 1000000ull
#define HANDSHAKE_NS (5000 * MS)
#define DIAL_GAP_NS (250 * MS)
#define JOIN_NS (10000 * MS)
#define NOLEADER_PROBE_NS (10000 * MS)
#define NOTMEMBER_DROP_NS (10000 * MS)
#define MAX_CONNS 4096
#define READ_ROUND (4u << 20) /* bytes read from one peer per loop round, for fairness */

enum {
    F_HELLO = 1,
    F_HELLO_ACK,
    F_AUTH,
    F_RAFT,
    F_HB,
    F_HB_ACK,
    F_WATCH,
    F_WATCH_SNAP,
    F_WATCH_ENTRY,
    F_ADMIN,
    F_ADMIN_REPLY,
    F_JOIN,
    F_JOIN_REPLY,
};

/* ------------------------------------------------------------------ bytes */

struct buf {
    uint8_t *p;
    size_t off, len, cap; /* data is p[off, len) */
};

static void oom(void)
{
    fputs("tsdbd: cluster: out of memory: stopping (fail-stop)\n", stderr);
    _exit(1);
}

static void buf_reserve(struct buf *b, size_t extra)
{
    if (b->off && b->off == b->len)
        b->off = b->len = 0;
    if (b->len + extra <= b->cap)
        return;
    if (b->off) {
        memmove(b->p, b->p + b->off, b->len - b->off);
        b->len -= b->off;
        b->off = 0;
        if (b->len + extra <= b->cap)
            return;
    }
    size_t cap = b->cap ? b->cap : 4096;
    while (cap < b->len + extra)
        cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p)
        oom();
    b->p = p;
    b->cap = cap;
}

static void buf_free(struct buf *b)
{
    free(b->p);
    *b = (struct buf){0};
}

static void put(struct buf *b, const void *d, size_t n)
{
    if (!n)
        return;
    buf_reserve(b, n);
    memcpy(b->p + b->len, d, n);
    b->len += n;
}

static void put8(struct buf *b, uint8_t v) { put(b, &v, 1); }

static void put32(struct buf *b, uint32_t v)
{
    uint8_t x[4] = {(uint8_t)v, (uint8_t)(v >> 8), (uint8_t)(v >> 16), (uint8_t)(v >> 24)};
    put(b, x, 4);
}

static void put64(struct buf *b, uint64_t v)
{
    put32(b, (uint32_t)v);
    put32(b, (uint32_t)(v >> 32));
}

static void putstr(struct buf *b, const char *s)
{
    size_t n = strlen(s);
    if (n > 0xffff)
        n = 0xffff;
    uint8_t x[2] = {(uint8_t)n, (uint8_t)(n >> 8)};
    put(b, x, 2);
    put(b, s, n);
}

static uint32_t le32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

struct rd {
    const uint8_t *p;
    size_t len, off;
    int bad;
};

static int need(struct rd *r, size_t n)
{
    if (r->bad || r->len - r->off < n) {
        r->bad = 1;
        return 0;
    }
    return 1;
}

static uint8_t get8(struct rd *r) { return need(r, 1) ? r->p[r->off++] : 0; }

static uint32_t get32(struct rd *r)
{
    if (!need(r, 4))
        return 0;
    uint32_t v = le32(r->p + r->off);
    r->off += 4;
    return v;
}

static uint64_t get64(struct rd *r)
{
    uint64_t lo = get32(r);
    return lo | (uint64_t)get32(r) << 32;
}

static void getn(struct rd *r, void *d, size_t n)
{
    if (need(r, n)) {
        memcpy(d, r->p + r->off, n);
        r->off += n;
    } else {
        memset(d, 0, n);
    }
}

static void getstr(struct rd *r, char *d, size_t cap)
{
    d[0] = 0;
    if (!need(r, 2))
        return;
    size_t n = (size_t)r->p[r->off] | (size_t)r->p[r->off + 1] << 8;
    r->off += 2;
    if (n >= cap || !need(r, n)) {
        r->bad = 1;
        return;
    }
    memcpy(d, r->p + r->off, n);
    d[n] = 0;
    r->off += n;
    if (memchr(d, 0, n))
        r->bad = 1; /* no NUL inside a name or an address */
}

static void frame_begin(struct buf *b, uint8_t type)
{
    b->off = b->len = 0;
    put32(b, 0);
    put8(b, type);
}

static void frame_end(struct buf *b)
{
    uint32_t n = (uint32_t)(b->len - 4);
    b->p[0] = (uint8_t)n;
    b->p[1] = (uint8_t)(n >> 8);
    b->p[2] = (uint8_t)(n >> 16);
    b->p[3] = (uint8_t)(n >> 24);
}

static void mac(const uint8_t *secret, size_t slen, const char *label, const uint8_t nc[32], const uint8_t ns[32],
                uint8_t out[32])
{
    uint8_t msg[14 + 64];
    memcpy(msg, label, 14);
    memcpy(msg + 14, nc, 32);
    memcpy(msg + 46, ns, 32);
    tsdb_hmac_sha256(secret, slen, msg, sizeof msg, out);
}

static int zero16(const uint8_t *u)
{
    for (int i = 0; i < 16; i++)
        if (u[i])
            return 0;
    return 1;
}

/* host:port or [v6]:port into a connected-in-progress (nonblock) or connected (block) socket; -1 with errno. */
static int dial(const char *addr, int nonblock)
{
    char host[META_ADDR_MAX], port[16];
    const char *colon = strrchr(addr, ':');
    if (!colon || !colon[1] || (size_t)(colon - addr) >= sizeof host || strlen(colon + 1) >= sizeof port) {
        errno = EINVAL;
        return -1;
    }
    size_t hl = (size_t)(colon - addr);
    memcpy(host, addr, hl);
    host[hl] = 0;
    if (hl >= 2 && host[0] == '[' && host[hl - 1] == ']') {
        memmove(host, host + 1, hl - 2);
        host[hl - 2] = 0;
    }
    snprintf(port, sizeof port, "%s", colon + 1);
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM, .ai_flags = AI_NUMERICSERV};
    struct addrinfo *ai = NULL;
    if (getaddrinfo(host, port, &hints, &ai) != 0 || !ai) {
        errno = EHOSTUNREACH;
        return -1;
    }
    int fd = socket(ai->ai_family, SOCK_STREAM | SOCK_CLOEXEC | (nonblock ? SOCK_NONBLOCK : 0), 0);
    if (fd < 0) {
        freeaddrinfo(ai);
        return -1;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0 && !(nonblock && errno == EINPROGRESS)) {
        int e = errno;
        freeaddrinfo(ai);
        close(fd);
        errno = e;
        return -1;
    }
    freeaddrinfo(ai);
    return fd;
}

/* ------------------------------------------------------------------ the shared parts (CLI and server) */

const char *cluster_strerror(int rc)
{
    switch (rc) {
    case 0: return "ok";
    case CLUSTER_ERR_NOTLEADER: return "not the Raft leader";
    case CLUSTER_ERR_EXISTS: return "the server is already in a cluster";
    case CLUSTER_ERR_UNREACHABLE: return "the new server did not answer";
    case CLUSTER_ERR_REFUSED: return "the new server belongs to another cluster or has another id";
    case CLUSTER_ERR_RETRY: return "the outcome is unknown (the leader changed); look at the status and retry";
    case CLUSTER_ERR_STATE: return "the server is in no cluster";
    default: return meta_strerror(rc);
    }
}

size_t cluster_settings_encode(const struct meta_settings *s, uint8_t *out)
{
    const uint32_t v[9] = {s->replication, s->min_isr,         s->node_timeout_ms,
                           s->heartbeat_ms, s->isr_lag_ms,     s->out_after_s,
                           s->voter_replace_s, s->voters,      s->volumes_per_server};
    for (int i = 0; i < 9; i++)
        for (int j = 0; j < 4; j++)
            out[i * 4 + j] = (uint8_t)(v[i] >> (8 * j));
    return 36;
}

static int settings_decode(struct rd *r, struct meta_settings *s)
{
    uint32_t v[9];
    for (int i = 0; i < 9; i++)
        v[i] = get32(r);
    *s = (struct meta_settings){v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8]};
    return r->bad ? -1 : 0;
}

int cluster_status_parse(const uint8_t *body, size_t len, struct cluster_status *st)
{
    struct rd r = {body, len, 0, 0};
    memset(st, 0, sizeof *st);
    st->self = get32(&r);
    st->mode = get8(&r);
    st->role = get8(&r);
    st->term = get64(&r);
    st->leader = get32(&r);
    st->commit = get64(&r);
    st->applied = get64(&r);
    st->nheard = get32(&r);
    if (r.bad || st->nheard > 1u << 20 || (size_t)st->nheard * 20 > len)
        return -1;
    st->heard = calloc(st->nheard ? st->nheard : 1, sizeof *st->heard);
    if (!st->heard)
        return -1;
    for (uint32_t i = 0; i < st->nheard; i++) {
        st->heard[i].id = get32(&r);
        st->heard[i].ago_ms = get64(&r);
        st->heard[i].incarnation = get64(&r);
    }
    uint32_t il = get32(&r);
    if (r.bad || r.len - r.off != il) {
        free(st->heard);
        st->heard = NULL;
        return -1;
    }
    st->image = body + r.off;
    st->image_len = il;
    return 0;
}

int cluster_read_secret(const char *path, uint8_t *out, size_t cap, size_t *len, char *err, size_t errn)
{
    FILE *f = fopen(path, "rb");
    if (!f) {
        snprintf(err, errn, "cannot open the secret file %s: %s", path, strerror(errno));
        return -1;
    }
    size_t n = fread(out, 1, cap, f);
    int bad = ferror(f), more = fgetc(f) != EOF;
    fclose(f);
    if (bad) {
        snprintf(err, errn, "cannot read the secret file %s", path);
        return -1;
    }
    if (more) {
        snprintf(err, errn, "the secret file %s is longer than %zu bytes", path, cap);
        return -1;
    }
    if (n && out[n - 1] == '\n')
        n--;
    if (n && out[n - 1] == '\r')
        n--;
    if (n < CLUSTER_SECRET_MIN) {
        snprintf(err, errn, "the secret in %s has %zu bytes; it needs %d at least", path, n, CLUSTER_SECRET_MIN);
        return -1;
    }
    *len = n;
    return 0;
}

/* ---- the CLI's side: one blocking request */

static int write_all(int fd, const uint8_t *p, size_t n, int64_t deadline_ms)
{
    while (n) {
        struct pollfd pf = {fd, POLLOUT, 0};
        int64_t left = deadline_ms - (int64_t)(iso_now_ns() / MS);
        if (left <= 0 || poll(&pf, 1, (int)left) <= 0)
            return -1;
        ssize_t w = write(fd, p, n);
        if (w < 0 && (errno == EINTR || errno == EAGAIN))
            continue;
        if (w <= 0)
            return -1;
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static int read_all(int fd, uint8_t *p, size_t n, int64_t deadline_ms)
{
    while (n) {
        struct pollfd pf = {fd, POLLIN, 0};
        int64_t left = deadline_ms - (int64_t)(iso_now_ns() / MS);
        if (left <= 0 || poll(&pf, 1, (int)left) <= 0)
            return -1;
        ssize_t got = read(fd, p, n);
        if (got < 0 && (errno == EINTR || errno == EAGAIN))
            continue;
        if (got <= 0)
            return -1;
        p += got;
        n -= (size_t)got;
    }
    return 0;
}

/* One frame: its type and payload (malloc'ed). 0 or -1. */
static int read_frame(int fd, int64_t deadline_ms, uint8_t *type, uint8_t **payload, size_t *len)
{
    uint8_t h[5];
    if (read_all(fd, h, 4, deadline_ms) != 0)
        return -1;
    uint32_t n = le32(h);
    if (n == 0 || n > FRAME_MAX || read_all(fd, h + 4, 1, deadline_ms) != 0)
        return -1;
    uint8_t *p = malloc(n);
    if (!p)
        return -1;
    if (read_all(fd, p, n - 1, deadline_ms) != 0) {
        free(p);
        return -1;
    }
    *type = h[4];
    *payload = p;
    *len = n - 1;
    return 0;
}

int cluster_admin(const char *addr, const uint8_t *secret, size_t secret_len, int op, const void *req,
                  size_t req_len, int timeout_ms, int *rc, uint8_t **reply, size_t *reply_len, char *err,
                  size_t errn)
{
    int64_t deadline = (int64_t)(iso_now_ns() / MS) + timeout_ms;
    int fd = dial(addr, 0);
    if (fd < 0) {
        snprintf(err, errn, "cannot connect to %s: %s", addr, strerror(errno));
        return -1;
    }
    struct buf b = {0};
    uint8_t nc[32], ns[32], m[32], want[32], *p = NULL;
    uint8_t type;
    size_t n;
    int ret = -1;
    tsdb_random(nc, sizeof nc);
    uint8_t zero[16] = {0};
    frame_begin(&b, F_HELLO);
    put32(&b, CL_MAGIC);
    put8(&b, CL_VERSION);
    put32(&b, 0);
    put(&b, zero, 16);
    put(&b, nc, 32);
    putstr(&b, "");
    frame_end(&b);
    if (write_all(fd, b.p, b.len, deadline) != 0 || read_frame(fd, deadline, &type, &p, &n) != 0) {
        snprintf(err, errn, "%s did not answer as a tsdbd cluster peer port", addr);
        goto out;
    }
    struct rd r = {p, n, 0, 0};
    get32(&r);
    getn(&r, zero, 16);
    getn(&r, ns, 32);
    getn(&r, m, 32);
    mac(secret, secret_len, "tsdb-cluster-s", nc, ns, want);
    if (type != F_HELLO_ACK || r.bad || !tsdb_ct_equal(m, want, 32)) {
        snprintf(err, errn, "%s is not a cluster peer port with this secret", addr);
        goto out;
    }
    free(p);
    p = NULL;
    mac(secret, secret_len, "tsdb-cluster-c", nc, ns, m);
    frame_begin(&b, F_AUTH);
    put(&b, m, 32);
    frame_end(&b);
    size_t auth_len = b.len;
    uint32_t reqid = 1;
    struct buf a = {0};
    frame_begin(&a, F_ADMIN);
    put8(&a, (uint8_t)op);
    put32(&a, reqid);
    put(&a, req, req_len);
    frame_end(&a);
    int wr = write_all(fd, b.p, auth_len, deadline) || write_all(fd, a.p, a.len, deadline);
    buf_free(&a);
    if (wr) {
        snprintf(err, errn, "%s closed the connection (a wrong secret?)", addr);
        goto out;
    }
    for (;;) {
        if (read_frame(fd, deadline, &type, &p, &n) != 0) {
            snprintf(err, errn, "no answer from %s within %d ms (a wrong secret closes the connection)", addr,
                     timeout_ms);
            goto out;
        }
        if (type == F_ADMIN_REPLY) {
            struct rd q = {p, n, 0, 0};
            uint32_t id = get32(&q);
            int32_t res = (int32_t)get32(&q);
            if (q.bad) {
                snprintf(err, errn, "a reply from %s does not decode", addr);
                goto out;
            }
            if (id == reqid) {
                *rc = res;
                *reply_len = n - q.off;
                *reply = malloc(*reply_len ? *reply_len : 1);
                if (!*reply)
                    goto out;
                memcpy(*reply, p + q.off, *reply_len);
                ret = 0;
                goto out;
            }
        }
        free(p); /* a heartbeat answer or the like: not ours */
        p = NULL;
    }
out:
    free(p);
    buf_free(&b);
    close(fd);
    return ret;
}

/* ------------------------------------------------------------------ the server: state */

enum { CS_CONNECTING, CS_HELLO_SENT, CS_WAIT_HELLO, CS_WAIT_AUTH, CS_READY };

struct conn {
    int fd, state, out, dead;
    uint64_t serial, opened_ns;
    uint32_t id;      /* the remote node id (0: the CLI, or a server in no cluster) */
    uint32_t want_id; /* dialed to reach this node id (0: dialed by address) */
    char peer[META_ADDR_MAX];
    uint8_t nc[32], ns[32];
    struct buf rb, wb, q; /* q: frames queued before AUTH */
    int watch_served;     /* it watches this server */
    int watch_ours;       /* this server's watch stream */
    int join;             /* a JOIN sent for a NODE_ADD, not yet answered */
    uint64_t join_cli;
    uint32_t join_reqid, join_id;
};

struct ring_e {
    uint64_t index, term;
    uint8_t type;
    uint32_t len;
    uint8_t *data;
};

struct heard {
    uint32_t id;
    uint64_t ns, inc, applied;
    char addr[META_ADDR_MAX], peer[META_ADDR_MAX]; /* as the member's heartbeat advertises them */
    uint64_t moved_ns; /* when the leader last proposed these addresses for the record */
};

struct addr_e {
    uint32_t id;
    char peer[META_ADDR_MAX];
    uint64_t last_dial;
};

/* an entry this server proposed: an admin op's reply, or a member's state */
struct pend {
    uint64_t conn; /* 0: no reply */
    uint32_t reqid, member;
    uint64_t index, term;
};

static struct {
    char dir[1024], metadir[1040], logdir[1060], nodefile[1060], snapfile[1060];
    char adv[META_ADDR_MAX], padv[META_ADDR_MAX], rack[META_RACK_MAX];
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t secret_len;
    int lfd, stop_pipe[2];
    pthread_t th;
    int running;
    atomic_int stop;

    int have_node;
    uint8_t uuid[16];
    uint32_t id;
    uint64_t incarnation;
    char seeds[1024];

    struct meta_state sm; /* the applied state: the core's, or the watch stream's */
    raft *r;
    raft_log *log;
    int role;
    uint64_t leader_since, leader_seen, notmember_since;
    uint8_t *snap; /* the image the core's log was last compacted to (sent as SNAPSHOT) */
    size_t snaplen;
    uint64_t snap_index;
    struct meta_state wsm; /* a core server's probe: the state a watch stream shows while it knows no leader */
    int wsm_on;

    struct ring_e ring[RING];
    uint64_t ring_first, ring_last; /* indexes held; 0 when none */

    struct conn **conns;
    size_t nconns, capconns;
    uint64_t next_serial;
    struct heard *heard;
    size_t nheard, capheard;
    struct addr_e *addrs;
    size_t naddrs, capaddrs;
    struct pend *pend;
    size_t npend, cappend;

    int init_pending;
    uint64_t init_conn;
    uint32_t init_reqid;
    struct meta_settings init_set;
    uint32_t reserved_id;

    uint64_t next_hb, next_manage, next_duty, watch_conn, watch_next_try;
    unsigned watch_rot;
    uint32_t hint_leader;
    uint64_t hint_term, hint_commit;

    struct buf fb; /* the frame being built; never held across a call that builds another */
    raft_entry ents[RAFT_WIRE_MAX_ENTRIES];
} C;

static void fail_stop(const char *fmt, ...) __attribute__((format(printf, 1, 2), noreturn));
static void fail_stop(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fputs("tsdbd: cluster: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputs(": stopping (fail-stop)\n", stderr);
    va_end(ap);
    _exit(1);
}

static void say(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void say(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fputs("tsdbd: cluster: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}

/* how long ago `t` was: times are taken at different moments of a round, so a later one may lie past `now` */
static uint64_t since(uint64_t now, uint64_t t) { return now > t ? now - t : 0; }

static const struct meta_settings *settings(void)
{
    static struct meta_settings def;
    if (C.sm.initialised)
        return &C.sm.set;
    meta_settings_default(&def);
    return &def;
}

/* ------------------------------------------------------------------ files */

static int fsync_dir(const char *d)
{
    int fd = open(d, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int rc = fsync(fd);
    close(fd);
    return rc;
}

/* tmp, fsync, rename, directory fsync */
static int write_durable(const char *path, const void *data, size_t len)
{
    char tmp[1100];
    snprintf(tmp, sizeof tmp, "%s.tmp", path);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;
    const uint8_t *p = data;
    size_t n = len;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            close(fd);
            unlink(tmp);
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    if (fsync(fd) != 0) {
        close(fd);
        unlink(tmp);
        return -1;
    }
    close(fd);
    if (rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    return fsync_dir(C.metadir);
}

static void hex16(const uint8_t *u, char *out)
{
    for (int i = 0; i < 16; i++)
        sprintf(out + 2 * i, "%02x", u[i]);
}

static int unhex16(const char *s, uint8_t *u)
{
    if (strlen(s) != 32)
        return -1;
    for (int i = 0; i < 16; i++) {
        unsigned v;
        if (sscanf(s + 2 * i, "%2x", &v) != 1)
            return -1;
        u[i] = (uint8_t)v;
    }
    return 0;
}

static int node_save(void)
{
    char u[33], text[2048];
    hex16(C.uuid, u);
    int n = snprintf(text, sizeof text, "cluster=%s\nnode=%" PRIu32 "\nincarnation=%" PRIu64 "\nseeds=%s\n", u, C.id,
                     C.incarnation, C.seeds);
    return write_durable(C.nodefile, text, (size_t)n);
}

/* 1: loaded, 0: none, -1: damaged (err says how) */
static int node_load(char *err, size_t errn)
{
    FILE *f = fopen(C.nodefile, "r");
    if (!f) {
        if (errno == ENOENT)
            return 0;
        snprintf(err, errn, "cannot open %s: %s", C.nodefile, strerror(errno));
        return -1;
    }
    char line[1200];
    int got = 0;
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\n")] = 0;
        if (!strncmp(line, "cluster=", 8) && unhex16(line + 8, C.uuid) == 0)
            got |= 1;
        else if (!strncmp(line, "node=", 5) && (C.id = (uint32_t)strtoul(line + 5, NULL, 10)) != 0)
            got |= 2;
        else if (!strncmp(line, "incarnation=", 12)) {
            C.incarnation = strtoull(line + 12, NULL, 10);
            got |= 4;
        } else if (!strncmp(line, "seeds=", 6))
            snprintf(C.seeds, sizeof C.seeds, "%.1000s", line + 6);
    }
    int bad = ferror(f);
    fclose(f);
    if (bad || got != 7) {
        snprintf(err, errn, "%s is damaged (it needs cluster=, node= and incarnation=)", C.nodefile);
        return -1;
    }
    return 1;
}

static int exists(const char *p)
{
    struct stat st;
    return stat(p, &st) == 0;
}

/* ------------------------------------------------------------------ connections */

static struct conn *conn_new(int fd, int out)
{
    if (C.nconns == C.capconns) {
        size_t cap = C.capconns ? C.capconns * 2 : 16;
        struct conn **p = realloc(C.conns, cap * sizeof *p);
        if (!p)
            oom();
        C.conns = p;
        C.capconns = cap;
    }
    struct conn *c = calloc(1, sizeof *c);
    if (!c)
        oom();
    c->fd = fd;
    c->out = out;
    c->serial = ++C.next_serial;
    c->opened_ns = iso_now_ns();
    c->state = out ? CS_CONNECTING : CS_WAIT_HELLO;
    C.conns[C.nconns++] = c;
    return c;
}

static struct conn *conn_by_serial(uint64_t s)
{
    if (!s)
        return NULL;
    for (size_t i = 0; i < C.nconns; i++)
        if (C.conns[i]->serial == s && !C.conns[i]->dead)
            return C.conns[i];
    return NULL;
}

/* the frame in C.fb to `c`: before AUTH into its queue unless `hs` (a handshake frame) */
static void conn_send_buf(struct conn *c, const struct buf *f, int hs)
{
    if (c->dead)
        return;
    struct buf *to = hs || c->state == CS_READY ? &c->wb : &c->q;
    if (to->len - to->off + f->len > WB_MAX) {
        say("the link to node %" PRIu32 " (%s) has %u MiB queued; dropping it", c->id, c->peer, WB_MAX >> 20);
        c->dead = 1;
        return;
    }
    put(to, f->p, f->len);
}

static void conn_send(struct conn *c, int hs) { conn_send_buf(c, &C.fb, hs); }

static void addr_learn(uint32_t id, const char *peer)
{
    if (!id || !peer[0])
        return;
    for (size_t i = 0; i < C.naddrs; i++)
        if (C.addrs[i].id == id) {
            snprintf(C.addrs[i].peer, sizeof C.addrs[i].peer, "%s", peer);
            return;
        }
    if (C.naddrs == C.capaddrs) {
        C.capaddrs = C.capaddrs ? C.capaddrs * 2 : 16;
        C.addrs = realloc(C.addrs, C.capaddrs * sizeof *C.addrs);
        if (!C.addrs)
            oom();
    }
    C.addrs[C.naddrs] = (struct addr_e){.id = id};
    snprintf(C.addrs[C.naddrs].peer, sizeof C.addrs[C.naddrs].peer, "%s", peer);
    C.naddrs++;
}

static struct addr_e *addr_entry(uint32_t id)
{
    for (size_t i = 0; i < C.naddrs; i++)
        if (C.addrs[i].id == id)
            return &C.addrs[i];
    return NULL;
}

static const char *peer_of(uint32_t id)
{
    const struct meta_member *m = meta_member(&C.sm, id);
    if (m && m->peer[0]) {
        addr_learn(id, m->peer); /* keeps the dial clock in one place */
        return m->peer;
    }
    /* a core that restarted applies nothing until a leader reaches it; its probe's watch stream knows the others */
    m = C.wsm_on ? meta_member(&C.wsm, id) : NULL;
    if (m && m->peer[0]) {
        addr_learn(id, m->peer);
        return m->peer;
    }
    struct addr_e *a = addr_entry(id);
    return a ? a->peer : NULL;
}

static struct conn *dial_conn(const char *peer, uint32_t want_id)
{
    if (C.nconns >= MAX_CONNS)
        return NULL;
    int fd = dial(peer, 1);
    if (fd < 0)
        return NULL;
    struct conn *c = conn_new(fd, 1);
    c->want_id = want_id;
    snprintf(c->peer, sizeof c->peer, "%s", peer);
    return c;
}

/* A link to node `id`: an authenticated one either way, one being dialed, or a new dial (at most every
 * DIAL_GAP_NS per node). NULL when there is none to be had now (the message is dropped; Raft resends). */
static struct conn *conn_to(uint32_t id)
{
    if (!id || id == C.id)
        return NULL;
    struct conn *pending = NULL;
    for (size_t i = 0; i < C.nconns; i++) {
        struct conn *c = C.conns[i];
        if (c->dead)
            continue;
        if (c->state == CS_READY && c->id == id)
            return c;
        if (c->out && c->want_id == id && !pending)
            pending = c;
    }
    if (pending)
        return pending;
    const char *peer = peer_of(id);
    if (!peer)
        return NULL;
    struct addr_e *a = addr_entry(id);
    uint64_t now = iso_now_ns();
    if (a && a->last_dial && since(now, a->last_dial) < DIAL_GAP_NS)
        return NULL;
    if (a)
        a->last_dial = now;
    return dial_conn(peer, id);
}

static void send_hello(struct conn *c)
{
    tsdb_random(c->nc, sizeof c->nc);
    uint8_t zero[16] = {0};
    frame_begin(&C.fb, F_HELLO);
    put32(&C.fb, CL_MAGIC);
    put8(&C.fb, CL_VERSION);
    put32(&C.fb, C.have_node ? C.id : 0);
    put(&C.fb, C.have_node ? C.uuid : zero, 16);
    put(&C.fb, c->nc, 32);
    putstr(&C.fb, C.padv);
    frame_end(&C.fb);
    conn_send(c, 1);
    c->state = CS_HELLO_SENT;
}

static void admin_reply(uint64_t conn, uint32_t reqid, int32_t rc, const void *body, size_t len)
{
    struct conn *c = conn_by_serial(conn);
    if (!c)
        return;
    struct buf b = {0};
    frame_begin(&b, F_ADMIN_REPLY);
    put32(&b, reqid);
    put32(&b, (uint32_t)rc);
    put(&b, body, len);
    frame_end(&b);
    conn_send_buf(c, &b, 0);
    buf_free(&b);
}

/* ------------------------------------------------------------------ heard, pending */

static struct heard *heard_get(uint32_t id)
{
    for (size_t i = 0; i < C.nheard; i++)
        if (C.heard[i].id == id)
            return &C.heard[i];
    return NULL;
}

/* addr, peer: what the member advertises now (NULL: not said here, keep what was heard) */
static void heard_set(uint32_t id, uint64_t now, uint64_t inc, uint64_t applied, const char *addr, const char *peer)
{
    struct heard *h = heard_get(id);
    if (!h) {
        if (C.nheard == C.capheard) {
            C.capheard = C.capheard ? C.capheard * 2 : 16;
            C.heard = realloc(C.heard, C.capheard * sizeof *C.heard);
            if (!C.heard)
                oom();
        }
        h = &C.heard[C.nheard++];
        *h = (struct heard){.id = id};
    }
    h->ns = now;
    h->inc = inc;
    h->applied = applied;
    if (addr && peer) {
        snprintf(h->addr, sizeof h->addr, "%s", addr);
        snprintf(h->peer, sizeof h->peer, "%s", peer);
    }
}

/* when this server last heard `id` (now for itself), 0 never */
static uint64_t heard_ns(uint32_t id, uint64_t now)
{
    if (id == C.id)
        return now;
    struct heard *h = heard_get(id);
    return h ? h->ns : 0;
}

static void pend_add(uint64_t conn, uint32_t reqid, uint32_t member, uint64_t index, uint64_t term)
{
    if (C.npend == C.cappend) {
        C.cappend = C.cappend ? C.cappend * 2 : 16;
        C.pend = realloc(C.pend, C.cappend * sizeof *C.pend);
        if (!C.pend)
            oom();
    }
    C.pend[C.npend++] = (struct pend){conn, reqid, member, index, term};
}

/* entry `index` (`term`) applied with the first operation's result rc0: answer whoever waited for it */
static void pend_applied(uint64_t index, uint64_t term, int rc0)
{
    for (size_t i = 0; i < C.npend;) {
        struct pend *p = &C.pend[i];
        if (p->index <= index) {
            int rc = p->index == index && p->term == term ? rc0 : CLUSTER_ERR_RETRY;
            if (p->conn)
                admin_reply(p->conn, p->reqid, rc, NULL, 0);
            C.pend[i] = C.pend[--C.npend];
        } else {
            i++;
        }
    }
}

static int pend_member(uint32_t member)
{
    for (size_t i = 0; i < C.npend; i++)
        if (C.pend[i].member == member)
            return 1;
    return 0;
}

/* ------------------------------------------------------------------ the watch ring and stream */

static void ring_reset(void)
{
    for (unsigned i = 0; i < RING; i++) {
        free(C.ring[i].data);
        C.ring[i].data = NULL;
    }
    C.ring_first = C.ring_last = 0;
}

static void ring_push(uint64_t index, uint64_t term, uint8_t type, const void *data, uint32_t len)
{
    if (C.ring_last && index != C.ring_last + 1)
        ring_reset();
    struct ring_e *e = &C.ring[index % RING];
    free(e->data);
    e->data = NULL;
    if (len) {
        e->data = malloc(len);
        if (!e->data)
            oom();
        memcpy(e->data, data, len);
    }
    *e = (struct ring_e){index, term, type, len, e->data};
    C.ring_last = index;
    if (!C.ring_first)
        C.ring_first = index;
    if (C.ring_last - C.ring_first >= RING)
        C.ring_first = C.ring_last - RING + 1;
}

static void send_entry(struct conn *c, uint64_t index, uint64_t term, uint8_t type, const void *data, uint32_t len)
{
    frame_begin(&C.fb, F_WATCH_ENTRY);
    put64(&C.fb, index);
    put64(&C.fb, term);
    put8(&C.fb, type);
    put(&C.fb, data, len);
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void send_snap(struct conn *c)
{
    uint8_t *img;
    size_t len;
    if (meta_snapshot_encode(&C.sm, &img, &len) != META_OK)
        oom();
    frame_begin(&C.fb, F_WATCH_SNAP);
    put(&C.fb, img, len);
    frame_end(&C.fb);
    free(img);
    conn_send(c, 0);
}

static void serve_watch(struct conn *c, uint64_t from)
{
    c->watch_served = 1;
    if (from <= 1) {
        /* a server with nothing: the image, which holds what no entry does (the first configuration) */
        send_snap(c);
        return;
    }
    if (from == C.sm.applied + 1)
        return;
    if (C.ring_first && C.ring_last == C.sm.applied && from >= C.ring_first && from <= C.ring_last) {
        for (uint64_t i = from; i <= C.ring_last && !c->dead; i++) {
            struct ring_e *e = &C.ring[i % RING];
            send_entry(c, e->index, e->term, e->type, e->data, e->len);
        }
        return;
    }
    send_snap(c);
}

/* the state was replaced (a RESTORE, a snapshot from the watch stream): everyone watching starts from it */
static void state_replaced(void)
{
    ring_reset();
    for (size_t i = 0; i < C.nconns; i++)
        if (C.conns[i]->watch_served && !C.conns[i]->dead)
            send_snap(C.conns[i]);
}

/* an entry to `m`; for the server's own state (`own`) also to the ring, the watchers and the waiting replies */
static void apply_entry(struct meta_state *m, int own, uint64_t index, uint64_t term, uint8_t type, const void *data,
                        uint32_t len)
{
    int rc0 = CLUSTER_ERR_RETRY;
    if (type == RAFT_ENTRY_NORMAL) {
        if (meta_apply(m, index, term, data, len, &rc0, 1) != META_OK)
            fail_stop("entry %" PRIu64 " (term %" PRIu64 ") does not decode: this build cannot apply it", index,
                      term);
    } else {
        meta_apply_other(m, index, term, type == RAFT_ENTRY_CONFIG ? data : NULL,
                         type == RAFT_ENTRY_CONFIG ? len : 0);
    }
    if (!own)
        return;
    ring_push(index, term, type, data, len);
    for (size_t i = 0; i < C.nconns; i++)
        if (C.conns[i]->watch_served && C.conns[i]->state == CS_READY && !C.conns[i]->dead)
            send_entry(C.conns[i], index, term, type, data, len);
    pend_applied(index, term, rc0);
}

/* ------------------------------------------------------------------ the core */

static void check_fault(void)
{
    char what[512];
    if (C.log && raft_log_fault(C.log, what, sizeof what))
        fail_stop("the Raft log: %s", what);
    if (C.r && raft_get_store_fault(C.r, what, sizeof what) != ISO_OK)
        fail_stop("the Raft log: %s", what);
}

static void sync_round(void)
{
    raft_log_sync s;
    iso_err e = raft_log_sync_prepare(C.log, &s);
    if (e == ISO_OK)
        e = raft_log_sync_run(&s);
    else
        for (unsigned i = 0; i < s.n; i++)
            close(s.fds[i]);
    if (e != ISO_OK || s.err)
        fail_stop("syncing the Raft log in %s: %s", C.logdir, strerror(s.err ? s.err : EIO));
    raft_log_durable(C.log, s.index, s.term);
    raft_persisted(C.r, s.index, s.term);
}

static int config_of(const struct meta_state *m, raft_config *c)
{
    c->n = 0;
    return m->config_len && raft_config_decode(m->config, m->config_len, c);
}

static void take_snapshot(void)
{
    raft_config c;
    if (!config_of(&C.sm, &c))
        return;
    uint8_t *img;
    size_t len;
    if (meta_snapshot_encode(&C.sm, &img, &len) != META_OK)
        oom();
    if (write_durable(C.snapfile, img, len) != 0) {
        say("cannot write %s: %s (the log is kept until a later snapshot is written)", C.snapfile,
            strerror(errno));
        free(img);
        C.snap_index = C.sm.applied; /* try again SNAP_EVERY entries later, not on every one */
        return;
    }
    free(C.snap);
    C.snap = img;
    C.snaplen = len;
    C.snap_index = C.sm.applied;
    raft_compact(C.r, C.sm.applied, C.sm.applied_term, &c);
}

static void send_raft(const raft_msg *m)
{
    const uint8_t *img = NULL;
    size_t il = 0;
    if (m->type == RAFT_MSG_SNAPSHOT) {
        if (!C.snap || C.snap_index != m->u.snapshot.index) {
            raft_snapshot_failed(C.r, m->to);
            return;
        }
        img = C.snap;
        il = C.snaplen;
    }
    struct conn *c = conn_to(m->to);
    if (!c) {
        if (m->type == RAFT_MSG_SNAPSHOT)
            raft_snapshot_failed(C.r, m->to);
        return;
    }
    size_t wl = raft_wire_size(m);
    frame_begin(&C.fb, F_RAFT);
    put32(&C.fb, (uint32_t)wl);
    buf_reserve(&C.fb, wl);
    if (raft_wire_encode(m, C.fb.p + C.fb.len, wl) != wl)
        fail_stop("cannot encode a Raft %s", raft_msg_type_name(m->type));
    C.fb.len += wl;
    put(&C.fb, img, il);
    if (C.fb.len - 4 > FRAME_MAX) {
        say("a Raft %s to node %" PRIu32 " is larger than a frame; not sent", raft_msg_type_name(m->type), m->to);
        if (m->type == RAFT_MSG_SNAPSHOT)
            raft_snapshot_failed(C.r, m->to);
        return;
    }
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void restore(const raft_action *a)
{
    struct meta_state t;
    meta_init(&t);
    if (meta_snapshot_decode(&t, a->u.snapshot.data, a->u.snapshot.len) != META_OK ||
        t.applied != a->u.snapshot.index)
        fail_stop("the snapshot image from node %" PRIu32 " at %" PRIu64 " does not verify", a->u.snapshot.from,
                  a->u.snapshot.index);
    if (write_durable(C.snapfile, a->u.snapshot.data, a->u.snapshot.len) != 0)
        fail_stop("cannot write %s: %s", C.snapfile, strerror(errno));
    meta_free(&C.sm);
    C.sm = t;
    free(C.snap);
    C.snap = malloc(a->u.snapshot.len ? a->u.snapshot.len : 1);
    if (!C.snap)
        oom();
    memcpy(C.snap, a->u.snapshot.data, a->u.snapshot.len);
    C.snaplen = a->u.snapshot.len;
    C.snap_index = a->u.snapshot.index;
    pend_applied(a->u.snapshot.index, 0, CLUSTER_ERR_RETRY);
    state_replaced();
}

static void pump(void)
{
    raft_action a;
    while (C.r && raft_next_action(C.r, &a)) {
        switch (a.type) {
        case RAFT_ACT_SAVE_HARD: {
            char err[2048];
            if (raft_log_hard_save(C.log, &a.u.hard, err, sizeof err) != ISO_OK)
                fail_stop("saving the Raft hard state: %s", err);
            raft_hard_saved(C.r, &a.u.hard);
            break;
        }
        case RAFT_ACT_SYNC: sync_round(); break;
        case RAFT_ACT_SEND: send_raft(&a.u.msg); break;
        case RAFT_ACT_APPLY: {
            raft_index last = 0;
            for (unsigned i = 0; i < a.u.apply.n; i++) {
                const raft_entry *e = &a.u.apply.entries[i];
                if (e->index != C.sm.applied + 1)
                    fail_stop("the core applies %" PRIu64 " after %" PRIu64, e->index, C.sm.applied);
                apply_entry(&C.sm, 1, e->index, e->term, e->type, e->data, e->len);
                last = e->index;
            }
            if (last)
                raft_applied(C.r, last);
            break;
        }
        case RAFT_ACT_RESTORE: restore(&a); break;
        case RAFT_ACT_STATE: {
            uint64_t now = iso_now_ns();
            if (a.u.state.role == RAFT_LEADER && C.role != RAFT_LEADER) {
                C.leader_since = now;
                C.reserved_id = 0;
                say("node %" PRIu32 " leads the metadata group (term %" PRIu64 ")", C.id, a.u.state.term);
            }
            C.role = a.u.state.role;
            if (a.u.state.leader)
                C.leader_seen = now;
            break;
        }
        default: break;
        }
        check_fault();
    }
    if (!C.r)
        return;
    check_fault();
    if (C.sm.applied >= C.snap_index + SNAP_EVERY)
        take_snapshot();
}

static void close_watches(void)
{
    for (size_t i = 0; i < C.nconns; i++)
        if (C.conns[i]->watch_served || C.conns[i]->watch_ours)
            C.conns[i]->dead = 1;
    C.watch_conn = 0;
}

/* The core from the files: the snapshot (or none), the log on it, the hard state. 0, or -1 with err. */
static int core_boot(char *err, size_t errn)
{
    meta_free(&C.sm);
    meta_init(&C.sm);
    free(C.snap);
    C.snap = NULL;
    C.snaplen = C.snap_index = 0;
    if (exists(C.snapfile)) {
        FILE *f = fopen(C.snapfile, "rb");
        if (!f) {
            snprintf(err, errn, "cannot open %s: %s", C.snapfile, strerror(errno));
            return -1;
        }
        fseek(f, 0, SEEK_END);
        long n = ftell(f);
        fseek(f, 0, SEEK_SET);
        uint8_t *img = n > 0 ? malloc((size_t)n) : NULL;
        int ok = img && fread(img, 1, (size_t)n, f) == (size_t)n && !ferror(f);
        fclose(f);
        if (!ok || meta_snapshot_decode(&C.sm, img, (size_t)n) != META_OK) {
            free(img);
            snprintf(err, errn, "%s is damaged (it does not verify): this server's metadata cannot be read", C.snapfile);
            return -1;
        }
        C.snap = img;
        C.snaplen = (size_t)n;
        C.snap_index = C.sm.applied;
    }
    raft_log_opts lo = {0};
    if (raft_log_open(C.logdir, &lo, C.sm.applied, C.sm.applied_term, &C.log, err, errn) != ISO_OK)
        return -1;
    raft_hard_state hs = {0};
    iso_err e = raft_log_hard_load(C.logdir, &hs, err, errn);
    if (e != ISO_OK && e != ISO_ERR_NOTFOUND) {
        raft_log_close(C.log);
        C.log = NULL;
        return -1;
    }
    if (e == ISO_ERR_NOTFOUND)
        hs = (raft_hard_state){0};
    raft_boot b = {.hard = hs, .applied = C.sm.applied, .applied_term = C.sm.applied_term};
    config_of(&C.sm, &b.config);
    raft_opts o;
    raft_opts_defaults(&o);
    tsdb_random(&o.seed, sizeof o.seed);
    C.r = raft_new(C.id, &o, &raft_log_file_ops, C.log, &b, err, errn);
    if (!C.r) {
        raft_log_close(C.log);
        C.log = NULL;
        return -1;
    }
    uint64_t now = iso_now_ns();
    raft_clock(C.r, now);
    C.role = RAFT_FOLLOWER;
    C.leader_seen = now;
    C.notmember_since = 0;
    ring_reset();
    close_watches();
    if (C.wsm_on) {
        meta_free(&C.wsm);
        C.wsm_on = 0;
    }
    return 0;
}

/* removed from the group while away: the core goes (its files stay), the state goes on from `next` (or as is) */
static void drop_core(struct meta_state *next, const char *why)
{
    say("node %" PRIu32 " is not in the metadata group's configuration (%s): it follows a watch stream now", C.id,
        why);
    raft_free(C.r);
    C.r = NULL;
    raft_log_close(C.log);
    C.log = NULL;
    C.role = RAFT_FOLLOWER;
    if (next) {
        meta_free(&C.sm);
        C.sm = *next;
        meta_init(next);
    }
    if (C.wsm_on) {
        meta_free(&C.wsm);
        C.wsm_on = 0;
    }
    for (size_t i = 0; i < C.npend; i++)
        if (C.pend[i].conn)
            admin_reply(C.pend[i].conn, C.pend[i].reqid, CLUSTER_ERR_RETRY, NULL, 0);
    C.npend = 0;
    close_watches();
    ring_reset();
}

/* Proposes `e` (and frees it); whoever waits for it (an admin reply on `conn`, a member's state) is registered
 * before the core runs: a single voter commits and applies inside the pump. */
static iso_err propose(struct meta_entry *e, uint64_t conn, uint32_t reqid, uint32_t member)
{
    meta_entry_finish(e);
    raft_index idx = 0;
    iso_err rc = raft_propose(C.r, e->buf, e->len, &idx);
    meta_entry_free(e);
    if (rc == ISO_OK)
        pend_add(conn, reqid, member, idx, raft_get_term(C.r));
    else if (conn)
        admin_reply(conn, reqid, CLUSTER_ERR_RETRY, NULL, 0);
    pump();
    return rc;
}

/* ------------------------------------------------------------------ frames from peers */

static void on_raft(struct conn *c, struct rd *r)
{
    if (!C.have_node || !c->id)
        return;
    uint32_t wl = get32(r);
    if (r->bad || wl > r->len - r->off) {
        c->dead = 1;
        return;
    }
    raft_msg m;
    if (!raft_wire_decode(r->p + r->off, wl, &m, C.ents, RAFT_WIRE_MAX_ENTRIES)) {
        say("a Raft message from node %" PRIu32 " does not decode; dropping the link", c->id);
        c->dead = 1;
        return;
    }
    if (m.to != C.id || m.from != c->id)
        return;
    if (m.type == RAFT_MSG_SNAPSHOT) {
        m.u.snapshot.data = r->p + r->off + wl;
        m.u.snapshot.len = r->len - r->off - wl;
    }
    if (!C.r) {
        char err[2048];
        if (core_boot(err, sizeof err) != 0)
            fail_stop("cannot start the Raft core: %s", err);
        say("node %" PRIu32 " holds a Raft core now (node %" PRIu32 " sent it a %s)", C.id, c->id,
            raft_msg_type_name(m.type));
    }
    raft_clock(C.r, iso_now_ns());
    raft_step(C.r, &m);
    pump();
}

static void on_hb(struct conn *c, struct rd *r)
{
    uint32_t node = get32(r);
    uint64_t inc = get64(r), applied = get64(r);
    char addr[META_ADDR_MAX], peer[META_ADDR_MAX];
    getstr(r, addr, sizeof addr);
    getstr(r, peer, sizeof peer);
    if (r->bad || !node || node != c->id) {
        c->dead = r->bad;
        return;
    }
    heard_set(node, iso_now_ns(), inc, applied, addr, peer);
    frame_begin(&C.fb, F_HB_ACK);
    put32(&C.fb, C.r ? raft_get_leader(C.r) : 0);
    put64(&C.fb, C.r ? raft_get_term(C.r) : 0);
    put64(&C.fb, C.r ? raft_get_commit(C.r) : 0);
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void on_hb_ack(struct rd *r)
{
    uint32_t leader = get32(r);
    uint64_t term = get64(r), commit = get64(r);
    if (r->bad || term < C.hint_term)
        return;
    C.hint_leader = leader;
    C.hint_term = term;
    C.hint_commit = commit;
}

/* after the probe state moved: removed while away? */
static void check_probe(void)
{
    raft_config cfg;
    if (C.r && C.wsm_on && C.wsm.applied >= C.sm.applied && config_of(&C.wsm, &cfg) && cfg.n &&
        !raft_config_find(&cfg, C.id))
        drop_core(&C.wsm, "a voter's state says so");
}

static void on_watch_snap(struct conn *c, struct rd *r)
{
    if (!c->watch_ours)
        return;
    struct meta_state *to = C.r ? &C.wsm : &C.sm;
    struct meta_state t;
    meta_init(&t);
    if (meta_snapshot_decode(&t, r->p + r->off, r->len - r->off) != META_OK || t.applied < to->applied) {
        meta_free(&t);
        c->dead = 1; /* a damaged image, or a state behind this one: another server */
        return;
    }
    meta_free(to);
    *to = t;
    if (to == &C.sm)
        state_replaced();
    else
        check_probe();
}

static void on_watch_entry(struct conn *c, struct rd *r)
{
    if (!c->watch_ours)
        return;
    uint64_t index = get64(r), term = get64(r);
    uint8_t type = get8(r);
    struct meta_state *to = C.r ? &C.wsm : &C.sm;
    if (r->bad || index != to->applied + 1 || type > RAFT_ENTRY_CONFIG) {
        c->dead = 1;
        return;
    }
    apply_entry(to, to == &C.sm, index, term, type, r->p + r->off, (uint32_t)(r->len - r->off));
    if (to == &C.wsm)
        check_probe();
}

static void put_status(struct buf *b)
{
    uint64_t now = iso_now_ns();
    put32(b, C.have_node ? C.id : 0);
    put8(b, C.r ? CLUSTER_MODE_CORE : C.have_node ? CLUSTER_MODE_WATCH : CLUSTER_MODE_NONE);
    put8(b, C.r ? (uint8_t)raft_get_role(C.r) : 0);
    put64(b, C.r ? raft_get_term(C.r) : C.hint_term);
    put32(b, C.r ? raft_get_leader(C.r) : C.hint_leader);
    put64(b, C.r ? raft_get_commit(C.r) : C.hint_commit);
    put64(b, C.sm.applied);
    uint32_t others = 0;
    for (size_t i = 0; i < C.nheard; i++)
        others += C.heard[i].id != C.id;
    put32(b, others + (C.have_node ? 1 : 0));
    if (C.have_node) {
        put32(b, C.id);
        put64(b, 0);
        put64(b, C.incarnation);
    }
    for (size_t i = 0; i < C.nheard; i++) {
        if (C.heard[i].id == C.id)
            continue;
        put32(b, C.heard[i].id);
        put64(b, (since(now, C.heard[i].ns)) / MS);
        put64(b, C.heard[i].inc);
    }
    if (C.sm.applied || C.sm.config_len) {
        uint8_t *img;
        size_t len;
        if (meta_snapshot_encode(&C.sm, &img, &len) != META_OK)
            oom();
        put32(b, (uint32_t)len);
        put(b, img, len);
        free(img);
    } else {
        put32(b, 0);
    }
}

static const char *leader_peer(void)
{
    uint32_t l = C.r ? raft_get_leader(C.r) : C.hint_leader;
    const struct meta_member *m = l ? meta_member(&C.sm, l) : NULL;
    return m ? m->peer : "";
}

static void admin_init(struct conn *c, uint32_t reqid, struct rd *r)
{
    struct meta_settings s;
    if (settings_decode(r, &s) != 0) {
        admin_reply(c->serial, reqid, META_ERR_FORMAT, NULL, 0);
        return;
    }
    if (C.have_node && (C.sm.initialised || !C.r || C.id != 1 || C.init_pending)) {
        admin_reply(c->serial, reqid, CLUSTER_ERR_EXISTS, NULL, 0);
        return;
    }
    if (!C.have_node) {
        /* the first server: id 1, a configuration of itself, written before the core starts on it */
        tsdb_random(C.uuid, sizeof C.uuid);
        C.id = 1;
        C.incarnation = 1;
        snprintf(C.seeds, sizeof C.seeds, "%s", C.padv);
        if (node_save() != 0)
            fail_stop("cannot write %s: %s", C.nodefile, strerror(errno));
        C.have_node = 1;
        meta_free(&C.sm);
        meta_init(&C.sm);
        raft_config cfg = {.n = 1, .m = {{.id = 1, .learner = false}}};
        uint8_t cb[RAFT_CONFIG_WIRE_MAX];
        size_t cl = raft_config_encode(&cfg, cb, sizeof cb);
        meta_apply_other(&C.sm, 0, 0, cb, cl);
        uint8_t *img;
        size_t len;
        if (meta_snapshot_encode(&C.sm, &img, &len) != META_OK)
            oom();
        if (write_durable(C.snapfile, img, len) != 0)
            fail_stop("cannot write %s: %s", C.snapfile, strerror(errno));
        free(img);
        char err[2048];
        if (core_boot(err, sizeof err) != 0)
            fail_stop("cannot start the Raft core: %s", err);
        raft_campaign(C.r);
        pump();
        say("node 1 starts a cluster");
    }
    /* INIT is proposed once this server leads (a single voter elects itself) */
    C.init_pending = 1;
    C.init_conn = c->serial;
    C.init_reqid = reqid;
    C.init_set = s;
}

static void admin_node_add(struct conn *c, uint32_t reqid, struct rd *r)
{
    char addr[META_ADDR_MAX];
    getstr(r, addr, sizeof addr);
    if (r->bad || !addr[0]) {
        admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0);
        return;
    }
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER || !C.sm.initialised) {
        const char *lp = leader_peer();
        admin_reply(c->serial, reqid, C.sm.initialised || C.have_node ? CLUSTER_ERR_NOTLEADER : CLUSTER_ERR_STATE,
                    lp, strlen(lp));
        return;
    }
    uint32_t id = C.sm.next_member > C.reserved_id ? C.sm.next_member : C.reserved_id;
    if (!id)
        id = 1;
    struct conn *d = dial_conn(addr, 0);
    if (!d) {
        admin_reply(c->serial, reqid, CLUSTER_ERR_UNREACHABLE, NULL, 0);
        return;
    }
    C.reserved_id = id + 1;
    d->join = 1;
    d->join_cli = c->serial;
    d->join_reqid = reqid;
    d->join_id = id;
    /* the seeds: the voters' peer addresses, where the new server finds a watch stream */
    char seeds[1024] = "";
    raft_config cfg;
    config_of(&C.sm, &cfg);
    for (unsigned i = 0; i < cfg.n; i++) {
        const struct meta_member *m = meta_member(&C.sm, cfg.m[i].id);
        if (m && !cfg.m[i].learner && strlen(seeds) + strlen(m->peer) + 2 < sizeof seeds) {
            if (seeds[0])
                strcat(seeds, ",");
            strcat(seeds, m->peer);
        }
    }
    frame_begin(&C.fb, F_JOIN);
    put(&C.fb, C.uuid, 16);
    put32(&C.fb, id);
    putstr(&C.fb, seeds);
    frame_end(&C.fb);
    conn_send(d, 0);
}

static void on_admin(struct conn *c, struct rd *r)
{
    uint8_t op = get8(r);
    uint32_t reqid = get32(r);
    if (r->bad) {
        c->dead = 1;
        return;
    }
    switch (op) {
    case CLUSTER_OP_INIT: admin_init(c, reqid, r); break;
    case CLUSTER_OP_NODE_ADD: admin_node_add(c, reqid, r); break;
    case CLUSTER_OP_STATUS: {
        struct buf b = {0};
        put_status(&b);
        admin_reply(c->serial, reqid, 0, b.p, b.len);
        buf_free(&b);
        break;
    }
    default: admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0); break;
    }
}

static void on_join(struct conn *c, struct rd *r)
{
    uint8_t uuid[16];
    getn(r, uuid, 16);
    uint32_t id = get32(r);
    char seeds[1024];
    getstr(r, seeds, sizeof seeds);
    int32_t rc = 0;
    if (r->bad || !id || zero16(uuid)) {
        c->dead = 1;
        return;
    }
    if (!C.have_node) {
        memcpy(C.uuid, uuid, 16);
        C.id = id;
        C.incarnation = 1;
        snprintf(C.seeds, sizeof C.seeds, "%s", seeds);
        if (node_save() != 0)
            fail_stop("cannot write %s: %s", C.nodefile, strerror(errno));
        C.have_node = 1;
        say("joined the cluster as node %" PRIu32, id);
    } else if (memcmp(uuid, C.uuid, 16) != 0 || id != C.id) {
        rc = CLUSTER_ERR_REFUSED;
    }
    frame_begin(&C.fb, F_JOIN_REPLY);
    put32(&C.fb, (uint32_t)rc);
    put64(&C.fb, C.incarnation);
    putstr(&C.fb, C.adv);
    putstr(&C.fb, C.padv);
    putstr(&C.fb, C.rack);
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void on_join_reply(struct conn *c, struct rd *r)
{
    if (!c->join)
        return;
    struct meta_member mb = {0};
    int32_t rc = (int32_t)get32(r);
    mb.incarnation = get64(r);
    getstr(r, mb.addr, sizeof mb.addr);
    getstr(r, mb.peer, sizeof mb.peer);
    getstr(r, mb.rack, sizeof mb.rack);
    c->join = 0;
    c->dead = 1; /* it knew no id when it answered the handshake; the members link again by id */
    if (r->bad)
        rc = CLUSTER_ERR_UNREACHABLE;
    if (rc) {
        admin_reply(c->join_cli, c->join_reqid, rc, NULL, 0);
        return;
    }
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER) {
        const char *lp = leader_peer();
        admin_reply(c->join_cli, c->join_reqid, CLUSTER_ERR_NOTLEADER, lp, strlen(lp));
        return;
    }
    mb.id = c->join_id;
    mb.state = META_UP;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_add(&e, &mb);
    heard_set(mb.id, iso_now_ns(), mb.incarnation, 0, NULL, NULL); /* it heartbeats once its watch stream has the member list */
    propose(&e, c->join_cli, c->join_reqid, 0);
}


static void handshake(struct conn *c, uint8_t type, struct rd *r)
{
    uint8_t uuid[16], m[32], want[32];
    if (c->state == CS_WAIT_HELLO && type == F_HELLO) {
        uint32_t magic = get32(r);
        uint8_t ver = get8(r);
        uint32_t id = get32(r);
        getn(r, uuid, 16);
        getn(r, c->nc, 32);
        getstr(r, c->peer, sizeof c->peer);
        if (r->bad || magic != CL_MAGIC || ver != CL_VERSION) {
            c->dead = 1;
            return;
        }
        if (C.have_node && !zero16(uuid) && memcmp(uuid, C.uuid, 16) != 0) {
            say("refused a peer of another cluster (node %" PRIu32 ", %s)", id, c->peer);
            c->dead = 1;
            return;
        }
        c->id = id;
        tsdb_random(c->ns, sizeof c->ns);
        mac(C.secret, C.secret_len, "tsdb-cluster-s", c->nc, c->ns, m);
        uint8_t zero[16] = {0};
        frame_begin(&C.fb, F_HELLO_ACK);
        put32(&C.fb, C.have_node ? C.id : 0);
        put(&C.fb, C.have_node ? C.uuid : zero, 16);
        put(&C.fb, c->ns, 32);
        put(&C.fb, m, 32);
        frame_end(&C.fb);
        conn_send(c, 1);
        c->state = CS_WAIT_AUTH;
    } else if (c->state == CS_HELLO_SENT && type == F_HELLO_ACK) {
        uint32_t id = get32(r);
        getn(r, uuid, 16);
        getn(r, c->ns, 32);
        getn(r, m, 32);
        mac(C.secret, C.secret_len, "tsdb-cluster-s", c->nc, c->ns, want);
        if (r->bad || !tsdb_ct_equal(m, want, 32)) {
            say("%s does not hold this cluster's secret; dropping the link", c->peer);
            c->dead = 1;
            return;
        }
        if ((C.have_node && !zero16(uuid) && memcmp(uuid, C.uuid, 16) != 0) || (c->want_id && id != c->want_id)) {
            say("%s is node %" PRIu32 " of %s cluster, not the one expected; dropping the link", c->peer, id,
                C.have_node && !zero16(uuid) && memcmp(uuid, C.uuid, 16) ? "another" : "this");
            c->dead = 1;
            return;
        }
        c->id = id;
        mac(C.secret, C.secret_len, "tsdb-cluster-c", c->nc, c->ns, m);
        frame_begin(&C.fb, F_AUTH);
        put(&C.fb, m, 32);
        frame_end(&C.fb);
        conn_send(c, 1);
        c->state = CS_READY;
        put(&c->wb, c->q.p ? c->q.p + c->q.off : NULL, c->q.len - c->q.off);
        buf_free(&c->q);
    } else if (c->state == CS_WAIT_AUTH && type == F_AUTH) {
        getn(r, m, 32);
        mac(C.secret, C.secret_len, "tsdb-cluster-c", c->nc, c->ns, want);
        if (r->bad || !tsdb_ct_equal(m, want, 32)) {
            say("a peer at %s does not hold this cluster's secret; dropping it", c->peer[0] ? c->peer : "?");
            c->dead = 1;
            return;
        }
        c->state = CS_READY;
        addr_learn(c->id, c->peer);
    } else {
        c->dead = 1;
    }
}

static void on_frame(struct conn *c, uint8_t type, const uint8_t *p, size_t n)
{
    struct rd r = {p, n, 0, 0};
    if (c->state != CS_READY) {
        handshake(c, type, &r);
        return;
    }
    switch (type) {
    case F_RAFT: on_raft(c, &r); break;
    case F_HB: on_hb(c, &r); break;
    case F_HB_ACK: on_hb_ack(&r); break;
    case F_WATCH: {
        uint64_t from = get64(&r);
        if (r.bad || !C.have_node)
            c->dead = 1;
        else
            serve_watch(c, from);
        break;
    }
    case F_WATCH_SNAP: on_watch_snap(c, &r); break;
    case F_WATCH_ENTRY: on_watch_entry(c, &r); break;
    case F_ADMIN: on_admin(c, &r); break;
    case F_JOIN: on_join(c, &r); break;
    case F_JOIN_REPLY: on_join_reply(c, &r); break;
    default: c->dead = 1; break;
    }
}

static void conn_read(struct conn *c)
{
    size_t got_round = 0;
    for (;;) {
        buf_reserve(&c->rb, 65536);
        ssize_t got = read(c->fd, c->rb.p + c->rb.len, c->rb.cap - c->rb.len);
        if (got < 0 && errno == EINTR)
            continue;
        if (got < 0 && errno == EAGAIN)
            break;
        if (got <= 0) {
            c->dead = 1;
            break;
        }
        c->rb.len += (size_t)got;
        got_round += (size_t)got;
        if (got_round >= READ_ROUND)
            break;
    }
    while (!c->dead && c->rb.len - c->rb.off >= 4) {
        uint32_t n = le32(c->rb.p + c->rb.off);
        if (n == 0 || n > FRAME_MAX) {
            c->dead = 1;
            break;
        }
        if (c->rb.len - c->rb.off - 4 < n)
            break;
        const uint8_t *f = c->rb.p + c->rb.off + 4;
        on_frame(c, f[0], f + 1, n - 1);
        c->rb.off += 4 + (size_t)n;
    }
}

static void conn_write(struct conn *c)
{
    while (c->wb.len > c->wb.off) {
        ssize_t w = write(c->fd, c->wb.p + c->wb.off, c->wb.len - c->wb.off);
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0 && errno == EAGAIN)
            return;
        if (w <= 0) {
            c->dead = 1;
            return;
        }
        c->wb.off += (size_t)w;
    }
    c->wb.off = c->wb.len = 0;
}

static void conn_free(struct conn *c)
{
    if (c->serial == C.watch_conn)
        C.watch_conn = 0;
    close(c->fd);
    buf_free(&c->rb);
    buf_free(&c->wb);
    buf_free(&c->q);
    free(c);
}

/* ------------------------------------------------------------------ periodic duties */

static void heartbeat(uint64_t now)
{
    if (!C.have_node)
        return;
    raft_config cfg;
    if (C.r)
        cfg = *raft_get_config(C.r);
    else if (!config_of(&C.sm, &cfg))
        return;
    heard_set(C.id, now, C.incarnation, C.sm.applied, C.adv, C.padv);
    frame_begin(&C.fb, F_HB);
    put32(&C.fb, C.id);
    put64(&C.fb, C.incarnation);
    put64(&C.fb, C.sm.applied);
    putstr(&C.fb, C.adv);
    putstr(&C.fb, C.padv); /* a restart may have moved it (another port, another host) */
    frame_end(&C.fb);
    for (unsigned i = 0; i < cfg.n; i++) {
        if (cfg.m[i].learner || cfg.m[i].id == C.id)
            continue;
        struct conn *c = conn_to(cfg.m[i].id); /* builds no frame */
        if (c)
            conn_send(c, 0);
    }
}

/* MEMBER_ADDR when `h` names other addresses than the record (h NULL: none), then MEMBER_STATE when state >= 0 */
static void propose_member(uint32_t id, int state, uint64_t inc, struct heard *h, uint64_t now)
{
    struct meta_entry e;
    meta_entry_init(&e);
    char addr[META_ADDR_MAX] = "", peer[META_ADDR_MAX] = "";
    if (h) {
        /* once per node timeout: a refusal (another live member holds that peer address) is not retried hot */
        h->moved_ns = now;
        snprintf(addr, sizeof addr, "%s", h->addr);
        snprintf(peer, sizeof peer, "%s", h->peer);
        meta_op_member_addr(&e, id, inc, addr, peer);
    }
    if (state >= 0)
        meta_op_member_state(&e, id, state, inc);
    if (propose(&e, 0, 0, id) == ISO_OK) {
        if (h)
            say("proposes node %" PRIu32 " at %s, peer %s (incarnation %" PRIu64 ")", id, addr, peer, inc);
        if (state >= 0)
            say("proposes node %" PRIu32 " %s (incarnation %" PRIu64 ")", id, meta_member_state_name(state), inc);
    }
}

static void member_states(uint64_t now)
{
    const struct meta_settings *s = settings();
    uint64_t to = (uint64_t)s->node_timeout_ms * MS;
    if (since(now, C.leader_since) < to)
        return; /* a new leader does not know when the others last heard from anyone */
    for (uint32_t i = 0; i < C.sm.nmembers && C.r && raft_get_role(C.r) == RAFT_LEADER; i++) {
        const struct meta_member *m = &C.sm.members[i];
        if (m->state == META_OUT || m->state == META_DRAINING || pend_member(m->id))
            continue;
        uint64_t last = heard_ns(m->id, now);
        struct heard *h = heard_get(m->id);
        uint64_t inc = m->id == C.id ? C.incarnation : h ? h->inc : 0;
        int fresh = last && since(now, last) < to;
        uint32_t id = m->id;
        uint64_t minc = m->incarnation;
        int st = m->state;
        /* a member that came back elsewhere (with --port 0, or on another host) is recorded there, or nobody
         * reaches it: only addresses its own heartbeat carried, of the run the record is about or a later one */
        struct heard *moved = fresh && h && inc >= minc && h->peer[0] && h->addr[0] &&
                                      (strcmp(h->peer, m->peer) != 0 || strcmp(h->addr, m->addr) != 0) &&
                                      (!h->moved_ns || since(now, h->moved_ns) >= to)
                                  ? h
                                  : NULL;
        if (st == META_UP && !fresh)
            propose_member(id, META_DOWN, minc, NULL, now);
        else if (fresh && inc >= minc && (st == META_DOWN || inc > minc))
            propose_member(id, META_UP, inc, moved, now);
        else if (moved)
            propose_member(id, -1, inc, moved, now);
    }
}

static int in_config(const raft_config *c, uint32_t id)
{
    return raft_config_find(c, id) != NULL;
}

static void manage_voters(uint64_t now)
{
    if (raft_get_config_index(C.r) > raft_get_applied(C.r))
        return; /* one change at a time */
    const struct meta_settings *s = settings();
    raft_config cfg = *raft_get_config(C.r);
    uint64_t to = (uint64_t)s->node_timeout_ms * MS, rep = (uint64_t)s->voter_replace_s * 1000 * MS;
    uint64_t lead = since(now, C.leader_since);
    uint32_t up = 0;
    for (uint32_t i = 0; i < C.sm.nmembers; i++)
        up += C.sm.members[i].state == META_UP;
    uint32_t target = meta_voter_target(s, up);
    raft_index commit = raft_get_commit(C.r);
    unsigned voters = 0, alive = 0, learners = 0;
    for (unsigned i = 0; i < cfg.n; i++) {
        uint32_t id = cfg.m[i].id;
        uint64_t last = heard_ns(id, now);
        int gone = last ? since(now, last) >= rep : lead >= rep;
        if (cfg.m[i].learner) {
            learners++;
            raft_progress p;
            if (last && since(now, last) < to && raft_get_progress(C.r, id, &p) && p.match >= commit) {
                raft_index idx;
                if (raft_change(C.r, RAFT_ADD_VOTER, id, &idx) == ISO_OK) {
                    say("promotes node %" PRIu32 " to a voter", id);
                    pump();
                    return;
                }
            } else if (gone && id != C.id) {
                raft_index idx;
                if (raft_change(C.r, RAFT_REMOVE, id, &idx) == ISO_OK) {
                    say("removes learner %" PRIu32 " (not heard for %u s)", id, s->voter_replace_s);
                    pump();
                    return;
                }
            }
            continue;
        }
        voters++;
        alive += !gone;
    }
    if (alive < target && !learners && cfg.n < RAFT_MEMBERS_MAX) {
        /* an up member that is not in the group, on a rack the voters do not use if there is one */
        const struct meta_member *best = NULL;
        int best_new_rack = 0;
        for (uint32_t i = 0; i < C.sm.nmembers; i++) {
            const struct meta_member *m = &C.sm.members[i];
            uint64_t last = heard_ns(m->id, now);
            if (m->state != META_UP || in_config(&cfg, m->id) || !last || since(now, last) >= to || !m->peer[0])
                continue;
            int new_rack = 1;
            for (unsigned j = 0; j < cfg.n; j++) {
                const struct meta_member *v = meta_member(&C.sm, cfg.m[j].id);
                if (v && m->rack[0] && !strcmp(v->rack, m->rack))
                    new_rack = 0;
            }
            if (!best || (new_rack && !best_new_rack)) {
                best = m;
                best_new_rack = new_rack;
            }
        }
        if (best) {
            raft_index idx;
            uint32_t id = best->id;
            if (raft_change(C.r, RAFT_ADD_LEARNER, id, &idx) == ISO_OK) {
                say("adds node %" PRIu32 " to the metadata group as a learner", id);
                pump();
            }
        }
        return;
    }
    if (voters > target) {
        for (unsigned i = 0; i < cfg.n; i++) {
            uint32_t id = cfg.m[i].id;
            uint64_t last = heard_ns(id, now);
            if (cfg.m[i].learner || id == C.id || !(last ? since(now, last) >= rep : lead >= rep))
                continue;
            raft_index idx;
            if (raft_change(C.r, RAFT_REMOVE, id, &idx) == ISO_OK) {
                say("removes voter %" PRIu32 " (not heard for %u s)", id, s->voter_replace_s);
                pump();
            }
            return;
        }
    }
}

static void leader_duties(uint64_t now)
{
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER)
        return;
    if (C.init_pending && !C.sm.initialised) {
        struct meta_member first = {.id = C.id, .state = META_UP, .incarnation = C.incarnation};
        snprintf(first.addr, sizeof first.addr, "%s", C.adv);
        snprintf(first.peer, sizeof first.peer, "%s", C.padv);
        snprintf(first.rack, sizeof first.rack, "%s", C.rack);
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_init(&e, C.uuid, &C.init_set, &first);
        C.init_pending = 0;
        propose(&e, C.init_conn, C.init_reqid, 0);
        return;
    }
    if (!C.sm.initialised)
        return;
    member_states(now);
    if (C.r && raft_get_role(C.r) == RAFT_LEADER && now >= C.next_manage) {
        C.next_manage = now + 1000 * MS;
        manage_voters(now);
    }
}

static void watch_duty(uint64_t now)
{
    if (!C.have_node)
        return;
    if (C.r) {
        raft_config cfg = *raft_get_config(C.r);
        if (cfg.n && !in_config(&cfg, C.id)) {
            if (!C.notmember_since)
                C.notmember_since = now;
            else if (since(now, C.notmember_since) >= NOTMEMBER_DROP_NS) {
                drop_core(NULL, "its own configuration no longer holds it");
                return;
            }
        } else {
            C.notmember_since = 0;
        }
    }
    int want = !C.r || (raft_get_leader(C.r) == 0 && since(now, C.leader_seen) >= NOLEADER_PROBE_NS);
    struct conn *w = conn_by_serial(C.watch_conn);
    if (!want) {
        if (w)
            w->dead = 1;
        C.watch_conn = 0;
        if (C.wsm_on) {
            meta_free(&C.wsm);
            C.wsm_on = 0;
        }
        return;
    }
    if (w || now < C.watch_next_try)
        return;
    C.watch_next_try = now + 1000 * MS;
    if (C.r && !C.wsm_on) {
        meta_init(&C.wsm);
        C.wsm_on = 1;
    }
    struct meta_state *st = C.r ? &C.wsm : &C.sm;
    /* where to watch: the servers in the group as this state knows them, else the seeds */
    const char *cand[RAFT_MEMBERS_MAX + 16];
    unsigned n = 0;
    raft_config cfg;
    if (config_of(&C.sm, &cfg))
        for (unsigned i = 0; i < cfg.n; i++) {
            const struct meta_member *m = meta_member(&C.sm, cfg.m[i].id);
            if (m && m->id != C.id && m->peer[0])
                cand[n++] = m->peer;
        }
    char seeds[sizeof C.seeds];
    snprintf(seeds, sizeof seeds, "%s", C.seeds);
    if (!n)
        for (char *sv = NULL, *t = strtok_r(seeds, ",", &sv); t && n < 16; t = strtok_r(NULL, ",", &sv))
            if (strcmp(t, C.padv) != 0)
                cand[n++] = t;
    if (!n)
        return;
    struct conn *c = dial_conn(cand[C.watch_rot++ % n], 0);
    if (!c)
        return;
    c->watch_ours = 1;
    C.watch_conn = c->serial;
    frame_begin(&C.fb, F_WATCH);
    put64(&C.fb, st->applied + 1);
    frame_end(&C.fb);
    conn_send(c, 0);
}

/* NODE's seeds follow the voters' peer addresses as the applied state names them. A server that restarts knows the
 * others only from NODE until a leader reaches it (a core applies nothing before that, a watcher has no stream yet):
 * the seeds a JOIN wrote (or none, on the server that ran INIT) go stale as the group changes, and a server that
 * finds none of them is cut off for good. Hints only: a failed write keeps the old file and is tried again later. */
static void seeds_refresh(uint64_t now)
{
    static uint64_t retry_ns;
    if (!C.have_node || !C.sm.initialised || (retry_ns && now < retry_ns))
        return;
    raft_config cfg;
    if (!config_of(&C.sm, &cfg))
        return;
    char seeds[sizeof C.seeds] = "";
    for (unsigned i = 0; i < cfg.n; i++) {
        const struct meta_member *m = meta_member(&C.sm, cfg.m[i].id);
        if (!m || cfg.m[i].learner || !m->peer[0] || strlen(seeds) + strlen(m->peer) + 2 >= sizeof seeds)
            continue;
        if (seeds[0])
            strcat(seeds, ",");
        strcat(seeds, m->peer);
    }
    if (!seeds[0] || !strcmp(seeds, C.seeds))
        return;
    char old[sizeof C.seeds];
    snprintf(old, sizeof old, "%s", C.seeds);
    snprintf(C.seeds, sizeof C.seeds, "%s", seeds);
    if (node_save() != 0) {
        say("cannot write %s (the seeds stay as they were; tried again in 10 s)", C.nodefile);
        snprintf(C.seeds, sizeof C.seeds, "%s", old);
        retry_ns = now + 10000 * MS;
        return;
    }
    retry_ns = 0;
}

/* ------------------------------------------------------------------ the loop */

static void *cluster_main(void *arg)
{
    (void)arg;
    struct pollfd *pf = NULL;
    size_t pfcap = 0;
    while (!atomic_load(&C.stop)) {
        uint64_t now = iso_now_ns();
        if (C.r) {
            raft_tick(C.r, now);
            pump();
            if (C.r && raft_log_floor_due(C.log, raft_get_commit(C.r), now))
                sync_round();
        }
        if (now >= C.next_hb) {
            uint32_t hb = settings()->heartbeat_ms;
            C.next_hb = now + (uint64_t)(hb < 10 ? 10 : hb) * MS;
            heartbeat(now);
        }
        if (now >= C.next_duty) {
            C.next_duty = now + 100 * MS;
            leader_duties(now);
            watch_duty(now);
            seeds_refresh(now);
            for (size_t i = 0; i < C.nconns; i++) {
                struct conn *c = C.conns[i];
                if ((c->state != CS_READY && since(now, c->opened_ns) > HANDSHAKE_NS) ||
                    (c->join && since(now, c->opened_ns) > JOIN_NS))
                    c->dead = 1;
            }
        }
        size_t n0 = C.nconns;
        if (pfcap < n0 + 2) {
            pfcap = (n0 + 2) * 2;
            pf = realloc(pf, pfcap * sizeof *pf);
            if (!pf)
                oom();
        }
        pf[0] = (struct pollfd){C.lfd, POLLIN, 0};
        pf[1] = (struct pollfd){C.stop_pipe[0], POLLIN, 0};
        for (size_t i = 0; i < n0; i++) {
            struct conn *c = C.conns[i];
            short ev = POLLIN;
            if (c->state == CS_CONNECTING || c->wb.len > c->wb.off)
                ev |= POLLOUT;
            pf[i + 2] = (struct pollfd){c->fd, ev, 0};
        }
        int timeout = 50;
        if (C.r) {
            uint64_t d = raft_next_deadline(C.r);
            if (d != UINT64_MAX)
                timeout = d <= now ? 0 : (d - now) / MS + 1 < 50 ? (int)((d - now) / MS + 1) : 50;
        }
        int rc = poll(pf, n0 + 2, timeout);
        if (rc < 0 && errno != EINTR)
            fail_stop("poll: %s", strerror(errno));
        if (rc > 0 && (pf[0].revents & POLLIN)) {
            for (int k = 0; k < 64 && C.nconns < MAX_CONNS; k++) {
                int fd = accept4(C.lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (fd < 0)
                    break;
                int one = 1;
                setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                conn_new(fd, 0);
            }
        }
        for (size_t i = 0; rc > 0 && i < n0; i++) {
            struct conn *c = C.conns[i];
            short re = pf[i + 2].revents;
            if (c->dead || !re)
                continue;
            if (c->state == CS_CONNECTING && (re & (POLLOUT | POLLERR | POLLHUP))) {
                int err = 0;
                socklen_t el = sizeof err;
                if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &el) != 0 || err) {
                    c->dead = 1;
                    continue;
                }
                send_hello(c);
            }
            if (re & (POLLIN | POLLHUP | POLLERR))
                conn_read(c);
        }
        /* everything queued this round goes out now; then the dead links go */
        for (size_t i = 0; i < C.nconns; i++) {
            struct conn *c = C.conns[i];
            if (!c->dead && c->state != CS_CONNECTING && c->wb.len > c->wb.off)
                conn_write(c);
        }
        for (size_t i = 0; i < C.nconns; i++) {
            struct conn *c = C.conns[i];
            if (c->dead && c->join) {
                c->join = 0;
                admin_reply(c->join_cli, c->join_reqid, CLUSTER_ERR_UNREACHABLE, NULL, 0);
            }
        }
        size_t k = 0;
        for (size_t i = 0; i < C.nconns; i++) {
            if (C.conns[i]->dead)
                conn_free(C.conns[i]);
            else
                C.conns[k++] = C.conns[i];
        }
        C.nconns = k;
    }
    free(pf);
    return NULL;
}

int cluster_start(const struct cluster_cfg *cfg)
{
    memset(&C, 0, sizeof C);
    if (cfg->secret_len < CLUSTER_SECRET_MIN || cfg->secret_len > CLUSTER_SECRET_MAX) {
        fprintf(stderr, "tsdbd: cluster: the secret must have %d to %d bytes\n", CLUSTER_SECRET_MIN,
                CLUSTER_SECRET_MAX);
        return -1;
    }
    if (!cfg->peer_advertise || !cfg->peer_advertise[0] || strlen(cfg->peer_advertise) >= META_ADDR_MAX ||
        strlen(cfg->advertise) >= META_ADDR_MAX || strlen(cfg->rack) >= META_RACK_MAX ||
        strlen(cfg->dir) >= sizeof C.dir) {
        fprintf(stderr, "tsdbd: cluster: an address, the rack or the directory is too long\n");
        return -1;
    }
    snprintf(C.dir, sizeof C.dir, "%s", cfg->dir);
    snprintf(C.adv, sizeof C.adv, "%s", cfg->advertise);
    snprintf(C.padv, sizeof C.padv, "%s", cfg->peer_advertise);
    snprintf(C.rack, sizeof C.rack, "%s", cfg->rack);
    memcpy(C.secret, cfg->secret, cfg->secret_len);
    C.secret_len = cfg->secret_len;
    snprintf(C.metadir, sizeof C.metadir, "%s/meta", C.dir);
    snprintf(C.logdir, sizeof C.logdir, "%s/raft", C.metadir);
    snprintf(C.nodefile, sizeof C.nodefile, "%s/NODE", C.metadir);
    snprintf(C.snapfile, sizeof C.snapfile, "%s/snapshot", C.metadir);
    meta_init(&C.sm);
    meta_init(&C.wsm);
    if (mkdir(C.metadir, 0755) == 0) {
        if (fsync_dir(C.dir) != 0) {
            fprintf(stderr, "tsdbd: cluster: cannot sync %s: %s\n", C.dir, strerror(errno));
            return -1;
        }
    } else if (errno != EEXIST) {
        fprintf(stderr, "tsdbd: cluster: cannot make %s: %s\n", C.metadir, strerror(errno));
        return -1;
    }
    iso_log_quiet = getenv("TSDB_CLUSTER_RAFT_LOG") == NULL; /* the core's INFO and WARN lines, on request */
    char err[2048];
    int got = node_load(err, sizeof err);
    if (got < 0) {
        fprintf(stderr, "tsdbd: cluster: %s\n", err);
        return -1;
    }
    if (got) {
        C.have_node = 1;
        C.incarnation++;
        if (node_save() != 0) {
            fprintf(stderr, "tsdbd: cluster: cannot write %s: %s\n", C.nodefile, strerror(errno));
            return -1;
        }
        if ((exists(C.snapfile) || exists(C.logdir)) && core_boot(err, sizeof err) != 0) {
            fprintf(stderr, "tsdbd: cluster: cannot start the Raft core: %s\n", err);
            return -1;
        }
    }
    C.lfd = cfg->peer_fd;
    if (pipe2(C.stop_pipe, O_CLOEXEC | O_NONBLOCK) != 0) {
        fprintf(stderr, "tsdbd: cluster: pipe: %s\n", strerror(errno));
        return -1;
    }
    int fl = fcntl(C.lfd, F_GETFL);
    fcntl(C.lfd, F_SETFL, fl | O_NONBLOCK);
    if (pthread_create(&C.th, NULL, cluster_main, NULL) != 0) {
        fprintf(stderr, "tsdbd: cluster: cannot start its thread\n");
        return -1;
    }
    C.running = 1;
    if (C.have_node)
        fprintf(stderr, "tsdbd: cluster: node %" PRIu32 ", incarnation %" PRIu64 ", %s\n", C.id, C.incarnation,
                C.r ? "in the metadata group" : "following a watch stream");
    else
        fprintf(stderr, "tsdbd: cluster: in no cluster yet (tsdb cluster init, or node add from a member)\n");
    return 0;
}

void cluster_stop(void)
{
    if (!C.running)
        return;
    atomic_store(&C.stop, 1);
    ssize_t w = write(C.stop_pipe[1], "x", 1);
    (void)w;
    pthread_join(C.th, NULL);
    C.running = 0;
    for (size_t i = 0; i < C.nconns; i++)
        conn_free(C.conns[i]);
    free(C.conns);
    if (C.r)
        raft_free(C.r);
    if (C.log)
        raft_log_close(C.log);
    meta_free(&C.sm);
    meta_free(&C.wsm);
    ring_reset();
    free(C.snap);
    free(C.heard);
    free(C.addrs);
    free(C.pend);
    buf_free(&C.fb);
    close(C.lfd);
    close(C.stop_pipe[0]);
    close(C.stop_pipe[1]);
}
