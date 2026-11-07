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
 * Frames: [u32 len][u8 type][payload], len counting the type and the payload, at most FRAME_MAX (HS_FRAME_MAX before
 * AUTH). Little endian. The dialer sends HELLO{magic, version, its node id (0: the CLI or a server in no cluster),
 * cluster uuid (zero when none), its identity (meta/IDENT; zero for the CLI), nonce_c[32], its peer address}; the
 * acceptor answers HELLO_ACK{id, uuid, identity, nonce_s[32], HMAC(secret, "tsdb-cluster-s" | transcript)}, the
 * transcript being HELLO's body and HELLO_ACK's fields before the MAC; the dialer checks it and sends
 * AUTH{HMAC(secret, "tsdb-cluster-c" | transcript | that MAC)}. Every claim of either side is under the MACs, so a
 * relay that alters one fails. From AUTH on, every frame carries a tag: HMAC(key, sequence | type | payload) after
 * its payload (len counts it), with a key per direction from the secret and the transcript ("tsdb-cluster-k>" from
 * the dialer, "tsdb-cluster-k<" to it) and a sequence number per direction from 0: a relay that passed the
 * handshake on unchanged can read the link but not write, reorder or replay into it. Two known uuids that differ end
 * the connection, and so does a server that claims a member id whose record names another identity (claim_ok).
 * The secret is the cluster's (--cluster-secret-file, a file only its owner may read; `tsdb cluster secret` makes
 * one); it is never sent.
 *
 * NODE_ADD (admin_node_add): PROBE to the new server (its identity), MEMBER_ADD committed with the next id and that
 * identity, then JOIN{uuid, id, identity}, on which the new server writes NODE. An id exists in the log before any
 * server holds it, so a leader that fails part way never lets the next one hand the same id to another server.
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
#ifdef __linux__
#include <sys/statfs.h>
#endif
#include <sys/statvfs.h>
#include <unistd.h>

#define FRAME_MAX (16u << 20)
#define WB_MAX (64u << 20) /* queued to one peer; beyond it the link is dropped (Raft resends) */
#define CL_MAGIC 0x43445354u /* "TSDC" */
#define CL_VERSION 2 /* 2: heartbeat sections, flags in PROBE_REPLY; peers of version 1 are refused (no mixed versions) */
#define RING 4096
#define SNAP_EVERY 1000
#define SYNC_STALL_NS (2000ull * 1000000) /* M11: a leader whose own sync is outstanding this long hands leadership on */
#define MS 1000000ull
#define HANDSHAKE_NS (5000 * MS)
#define DIAL_GAP_NS (250 * MS)
#define JOIN_NS (10000 * MS)
#define NOLEADER_PROBE_NS (10000 * MS)
#define NOTMEMBER_DROP_NS (10000 * MS)
#define MAX_CONNS 4096
#define TAG 32 /* HMAC-SHA256 under the link's key, on every frame after AUTH */
/* before AUTH only HELLO, HELLO_ACK and AUTH pass, all short: a longer frame ends the connection before it is
 * buffered (a peer without the secret must not make this server hold 16 MiB per connection) */
#define HS_FRAME_MAX 512
#define HS_TR_MAX (HS_FRAME_MAX + 128) /* the handshake's transcript: HELLO's body, HELLO_ACK's fields, its MAC */
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
    F_PROBE,
    F_PROBE_REPLY,
};

/* the sections of a heartbeat (after its address fields) */
enum { HBS_NODE = 1, HBS_VOLUMES = 2, HBS_DOMAINS = 3 };
#define FREE_MIN_DEFAULT (1ull << 30) /* a state directory below this is not voter_ok (M11) */

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

void cluster_hmac_parts(const uint8_t *key, size_t klen, int n, const void *const *part, const size_t *len,
                        uint8_t out[32])
{
    uint8_t k[64] = {0}, pad[64], ih[32];
    if (klen > 64)
        tsdb_sha256_buf(key, klen, k);
    else if (klen)
        memcpy(k, key, klen);
    tsdb_sha256 c;
    for (int i = 0; i < 64; i++)
        pad[i] = k[i] ^ 0x36;
    tsdb_sha256_init(&c);
    tsdb_sha256_update(&c, pad, 64);
    for (int i = 0; i < n; i++)
        tsdb_sha256_update(&c, part[i], len[i]);
    tsdb_sha256_final(&c, ih);
    for (int i = 0; i < 64; i++)
        pad[i] = k[i] ^ 0x5c;
    tsdb_sha256_init(&c);
    tsdb_sha256_update(&c, pad, 64);
    tsdb_sha256_update(&c, ih, 32);
    tsdb_sha256_final(&c, out);
    explicit_bzero(k, sizeof k);
    explicit_bzero(pad, sizeof pad);
    explicit_bzero(&c, sizeof c);
}

/* A handshake MAC or a link key: HMAC(secret, label | transcript). The transcript is the dialer's HELLO body, then
 * HELLO_ACK's fields before its MAC (and, for AUTH and the keys, that MAC): every claim either side made (ids, uuid,
 * identity, peer address, both nonces) is covered, so a relay that changes one of them is caught, and one that
 * changes nothing gets a link it cannot write into (the keys are the secret's, the frames are tagged). */
static void hs_mac(const uint8_t *secret, size_t slen, const char *label, const uint8_t *tr, size_t trlen,
                   uint8_t out[32])
{
    const void *part[2] = {label, tr};
    size_t len[2] = {strlen(label), trlen};
    cluster_hmac_parts(secret, slen, 2, part, len, out);
}

/* the tag of one frame after AUTH: its sequence number in that direction, its type and its payload */
static void frame_tag(const uint8_t key[32], uint64_t seq, const uint8_t *body, size_t n, uint8_t out[32])
{
    uint8_t s[8];
    for (int i = 0; i < 8; i++)
        s[i] = (uint8_t)(seq >> (8 * i));
    const void *part[2] = {s, body};
    size_t len[2] = {8, n};
    cluster_hmac_parts(key, 32, 2, part, len, out);
}

/* an untagged frame ([len][type][payload]) appended to `to` with its tag */
static void put_tagged(struct buf *to, const uint8_t key[32], uint64_t *seq, const uint8_t *frame, size_t flen)
{
    uint8_t t[TAG];
    frame_tag(key, (*seq)++, frame + 4, flen - 4, t);
    put32(to, (uint32_t)(flen - 4 + TAG));
    put(to, frame + 4, flen - 4);
    put(to, t, TAG);
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

size_t cluster_settings_encode(const struct meta_settings *s, int mode, uint8_t *out)
{
    const uint32_t v[13] = {s->replication,     s->min_isr,       s->node_timeout_ms, s->heartbeat_ms,
                            s->isr_lag_ms,      s->out_after_s,   s->voter_replace_s, s->voters,
                            s->volumes_per_server, s->failover,   s->read_barrier,    s->lease_s,
                            (uint32_t)mode};
    for (int i = 0; i < 13; i++)
        for (int j = 0; j < 4; j++)
            out[i * 4 + j] = (uint8_t)(v[i] >> (8 * j));
    return 52;
}

static int settings_decode(struct rd *r, struct meta_settings *s, int *mode)
{
    uint32_t v[13];
    for (int i = 0; i < 13; i++)
        v[i] = get32(r);
    *s = (struct meta_settings){.replication = v[0], .min_isr = v[1], .node_timeout_ms = v[2], .heartbeat_ms = v[3],
                                .isr_lag_ms = v[4], .out_after_s = v[5], .voter_replace_s = v[6], .voters = v[7],
                                .volumes_per_server = v[8], .failover = (uint8_t)v[9],
                                .read_barrier = (uint8_t)v[10], .lease_s = v[11]};
    *mode = (int)v[12];
    return r->bad || v[9] > 255 || v[10] > 255 ? -1 : 0;
}

void cluster_status_free(struct cluster_status *st)
{
    for (uint32_t i = 0; st->heard && i < st->nheard; i++) {
        free(st->heard[i].vols);
        free(st->heard[i].doms);
    }
    free(st->heard);
    st->heard = NULL;
}

int cluster_status_parse(const uint8_t *body, size_t len, struct cluster_status *st)
{
    struct rd r = {body, len, 0, 0};
    memset(st, 0, sizeof *st);
    st->self = get32(&r);
    st->mode = get8(&r);
    st->role = get8(&r);
    st->stale = get8(&r);
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
    for (uint32_t i = 0; i < st->nheard && !r.bad; i++) {
        struct cluster_heard *h = &st->heard[i];
        h->id = get32(&r);
        h->ago_ms = get64(&r);
        h->incarnation = get64(&r);
        h->flags = get8(&r);
        h->core_dropped = get8(&r);
        h->read_barrier = get8(&r);
        h->state_free = get64(&r);
        h->nvols = get32(&r);
        if (r.bad || h->nvols > CLUSTER_REPORT_VOLS)
            break;
        h->vols = calloc(h->nvols ? h->nvols : 1, sizeof *h->vols);
        if (!h->vols)
            break;
        for (uint32_t j = 0; j < h->nvols; j++) {
            struct cluster_vol_report *v = &h->vols[j].v;
            v->volume = get32(&r);
            v->role = get8(&r);
            v->health = get8(&r);
            v->epoch = get64(&r);
            getstr(&r, v->domain, sizeof v->domain);
            v->series_len = get64(&r);
            v->states_len = get64(&r);
            v->fenced_age_ms = get64(&r);
            v->wal_seg = get64(&r);
            v->wal_off = get64(&r);
            h->vols[j].local_fault_s = get64(&r);
        }
        h->ndoms = get32(&r);
        if (r.bad || h->ndoms > CLUSTER_REPORT_DOMS)
            break;
        h->doms = calloc(h->ndoms ? h->ndoms : 1, sizeof *h->doms);
        if (!h->doms)
            break;
        for (uint32_t j = 0; j < h->ndoms; j++) {
            getstr(&r, h->doms[j].name, sizeof h->doms[j].name);
            h->doms[j].state = get8(&r);
        }
    }
    uint32_t il = r.bad ? 0 : get32(&r);
    if (r.bad || r.len - r.off != il) {
        cluster_status_free(st);
        return -1;
    }
    st->image = body + r.off;
    st->image_len = il;
    return 0;
}

int cluster_read_secret(const char *path, uint8_t *out, size_t cap, size_t *len, char *err, size_t errn)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0) {
        snprintf(err, errn, "cannot open the secret file %s: %s", path, strerror(errno));
        if (fd >= 0)
            close(fd);
        return -1;
    }
    if (!S_ISREG(st.st_mode) || (st.st_mode & 077)) {
        snprintf(err, errn, "the secret file %s is %s: refused (chmod 600 %s; `tsdb cluster secret FILE` makes one)",
                 path, S_ISREG(st.st_mode) ? "open to its group or to others" : "not a regular file", path);
        close(fd);
        return -1;
    }
    FILE *f = fdopen(fd, "rb");
    if (!f) {
        snprintf(err, errn, "cannot open the secret file %s: %s", path, strerror(errno));
        close(fd);
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

int cluster_make_secret(const char *path, char *err, size_t errn)
{
    uint8_t raw[32];
    char text[2 * sizeof raw + 2];
    if (tsdb_random(raw, sizeof raw) != 0) {
        snprintf(err, errn, "no random bytes: %s", strerror(errno));
        return -1;
    }
    for (size_t i = 0; i < sizeof raw; i++)
        sprintf(text + 2 * i, "%02x", raw[i]);
    explicit_bzero(raw, sizeof raw);
    text[2 * sizeof raw] = '\n';
    size_t n = 2 * sizeof raw + 1;
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) {
        snprintf(err, errn, "cannot make %s: %s", path, strerror(errno));
        return -1;
    }
    int ok = fchmod(fd, 0600) == 0 && write(fd, text, n) == (ssize_t)n && fsync(fd) == 0;
    int e = errno;
    explicit_bzero(text, sizeof text);
    if (close(fd) != 0 && ok) {
        ok = 0;
        e = errno;
    }
    if (!ok) {
        unlink(path);
        snprintf(err, errn, "cannot write %s: %s", path, strerror(e));
        return -1;
    }
    char dir[4096];
    snprintf(dir, sizeof dir, "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash)
        *(slash == dir ? slash + 1 : slash) = 0;
    else
        snprintf(dir, sizeof dir, ".");
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0 || fsync(dfd) != 0) {
        snprintf(err, errn, "cannot sync the directory of %s: %s", path, strerror(errno));
        if (dfd >= 0)
            close(dfd);
        return -1;
    }
    close(dfd);
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
    uint8_t nc[32], m[32], want[32], kout[32], kin[32], tr[HS_TR_MAX], *p = NULL;
    uint64_t seq_in = 0, seq_out = 0;
    uint8_t type;
    size_t n, trlen;
    int ret = -1;
    tsdb_random(nc, sizeof nc);
    uint8_t zero[16] = {0};
    frame_begin(&b, F_HELLO);
    put32(&b, CL_MAGIC);
    put8(&b, CL_VERSION);
    put32(&b, 0);
    put(&b, zero, 16);
    put(&b, zero, 16); /* no identity: not a server */
    put(&b, nc, 32);
    putstr(&b, "");
    frame_end(&b);
    trlen = b.len - 5;
    memcpy(tr, b.p + 5, trlen);
    if (write_all(fd, b.p, b.len, deadline) != 0 || read_frame(fd, deadline, &type, &p, &n) != 0) {
        snprintf(err, errn, "%s did not answer as a tsdbd cluster peer port", addr);
        goto out;
    }
    /* HELLO_ACK: id, uuid, identity, nonce, then the MAC over the transcript so far */
    if (type != F_HELLO_ACK || n != 4 + 16 + 16 + 32 + 32) {
        snprintf(err, errn, "%s is not a cluster peer port with this secret", addr);
        goto out;
    }
    memcpy(tr + trlen, p, n - 32);
    trlen += n - 32;
    hs_mac(secret, secret_len, "tsdb-cluster-s", tr, trlen, want);
    if (!tsdb_ct_equal(p + n - 32, want, 32)) {
        snprintf(err, errn, "%s is not a cluster peer port with this secret", addr);
        goto out;
    }
    memcpy(tr + trlen, p + n - 32, 32);
    trlen += 32;
    free(p);
    p = NULL;
    hs_mac(secret, secret_len, "tsdb-cluster-c", tr, trlen, m);
    hs_mac(secret, secret_len, "tsdb-cluster-k>", tr, trlen, kout);
    hs_mac(secret, secret_len, "tsdb-cluster-k<", tr, trlen, kin);
    frame_begin(&b, F_AUTH);
    put(&b, m, 32);
    frame_end(&b);
    uint32_t reqid = 1;
    struct buf a = {0};
    frame_begin(&a, F_ADMIN);
    put8(&a, (uint8_t)op);
    put32(&a, reqid);
    put(&a, req, req_len);
    frame_end(&a);
    put_tagged(&b, kout, &seq_out, a.p, a.len);
    int wr = write_all(fd, b.p, b.len, deadline);
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
        uint8_t t[TAG];
        if (n < TAG) {
            snprintf(err, errn, "a frame from %s is too short", addr);
            goto out;
        }
        /* the tag covers the type and the payload: rebuild them in one piece */
        uint8_t *body = malloc(n + 1);
        if (!body)
            goto out;
        body[0] = type;
        memcpy(body + 1, p, n);
        frame_tag(kin, seq_in++, body, n + 1 - TAG, t);
        int good = tsdb_ct_equal(t, body + n + 1 - TAG, TAG);
        free(body);
        if (!good) {
            snprintf(err, errn, "a frame from %s does not authenticate", addr);
            goto out;
        }
        n -= TAG;
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
    explicit_bzero(kin, sizeof kin);
    explicit_bzero(kout, sizeof kout);
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
    uint8_t ident[16]; /* the remote server's identity, as its HELLO or HELLO_ACK claimed it */
    uint8_t nc[32];
    uint8_t tr[HS_TR_MAX]; /* the handshake's transcript (hs_mac) */
    size_t trlen;
    uint8_t kin[32], kout[32]; /* the link's keys, from AUTH on */
    uint64_t seq_in, seq_out;
    struct buf rb, wb, q; /* q: frames queued before AUTH (untagged; tagged as they move to wb) */
    int watch_served;     /* it watches this server */
    int watch_ours;       /* this server's watch stream */
    int join;             /* NODE_ADD: 1 a PROBE sent, 2 a JOIN sent, not yet answered */
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
    /* the heartbeat's sections (item 1) */
    int have_flags;
    uint8_t flags, core_dropped, read_barrier;
    uint64_t state_free;
    uint64_t bad_ns; /* since when its flags say it is not voter_ok or its core was dropped; 0: well */
    uint64_t flags_ns; /* when the leader last proposed these flags for the record */
    uint32_t nvols, ndoms;
    struct cluster_vol_status *vols;
    struct cluster_dom_report *doms;
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
    uint64_t join; /* a NODE_ADD's MEMBER_ADD: the link to the new server, which gets JOIN once it applied */
};


/* ------------------------------------------------------------------ the disk worker's jobs */
#define DW_Q 8
enum { DJ_SYNC = 1, DJ_SNAP };
struct djob {
    int kind;
    uint64_t gen;
    raft_log_sync sync; /* DJ_SYNC: the descriptors a prepare handed out */
    uint8_t *img;       /* DJ_SNAP: the image, handed back in the result */
    size_t len;
    uint64_t index, term;
    raft_config cfg;
    int fail_dirsync;
};
struct dres {
    int kind;
    uint64_t gen;
    int rc, err; /* DJ_SYNC: rc != 0 failed; DJ_SNAP: write_durable's result (-2: the directory sync failed) */
    uint64_t index, term;
    uint8_t *img;
    size_t len;
    raft_config cfg;
};
static struct {
    char dir[1024], metadir[1040], logdir[1060], nodefile[1060], snapfile[1060], identfile[1060], pendfile[1060];
    char copyfile[1060], retiredfile[1060];
    uint64_t pend_index; /* the install image kept in pendfile (keep_install), 0 for none */
    char adv[META_ADDR_MAX], padv[META_ADDR_MAX], rack[META_RACK_MAX], feed[META_FEED_MAX], bmc[META_BMC_MAX];
    int witness, rb_off;
    int retired;        /* the state directory holds `retired`: this identity's core was dropped (§5.6) */
    int core_dropped;   /* the core was dropped in this run, or a log fault ended the voter role (voter_ok 0) */
    int copy_stale;     /* the sm is the persisted copy of an earlier run: no watch stream has confirmed it yet */
    int copy_dirty;     /* the watch stream moved sm since the copy file was written */
    uint64_t copy_next; /* not before this time (ns) */
    uint64_t free_min;  /* bytes the state directory must have free for voter_ok */
    unsigned nk_vols, nk_doms; /* TSDB_CLUSTER_TEST report knobs: what the heartbeat's sections carry in tests */
    struct cluster_vol_report k_vols[4];
    struct cluster_dom_report k_doms[4];
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t secret_len;
    int lfd, stop_pipe[2];
    pthread_t th;
    int running;
    atomic_int stop;

    int have_node;
    uint8_t ident[16]; /* this server's own identity (meta/IDENT), made at its first start, never changed */
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
    int init_mode;
    uint32_t reserved_id;
    uint64_t pend_join; /* set around propose() by a NODE_ADD: the new server's link (struct pend) */

    uint64_t next_hb, next_manage, next_duty, watch_conn, watch_next_try;
    unsigned watch_rot;
    uint32_t hint_leader;
    uint64_t hint_term, hint_commit;
    uint64_t watch_behind_since; /* when the watch stream first stayed behind hint_commit; 0: it does not */

    /* TSDB_CLUSTER_TEST (tests only): snap_every=N, snap_retain=N, seg_bytes=N, fail_snap_dirsync=1,
     * hold_install_ms=N (installs dropped until N ms after the first arrived: a slow one) */
    uint64_t snap_every, snap_retain, seg_bytes;
    int fail_snap_dirsync;
    uint64_t hold_install_ns, hold_from;

    /* The disk worker (M11, item 2): the fdatasyncs of a log sync round and the durable write of a snapshot run on
     * their own thread, so a disk that stalls leaves the heartbeats, the watch streams and the Raft messages alone.
     * The core thread prepares a round (descriptors), the worker syncs, the core thread hears the result through
     * dw_pipe and tells the core. dw_gen: results of an older generation (the core was stopped or restored since)
     * are dropped. */
    pthread_t dw_th;
    int dw_on, dw_stop, dw_busy, dw_pipe[2];
    pthread_mutex_t dw_mu;
    pthread_cond_t dw_cv, dw_idle;
    struct djob dw_q[DW_Q];
    size_t dw_nq;
    struct dres dw_r[DW_Q];
    size_t dw_nr;
    uint64_t dw_gen;
    uint64_t sync_since; /* core thread: when the sync round now outstanding was handed to the worker; 0: none */
    int snap_out;        /* core thread: a snapshot write is outstanding */
    uint64_t stall_ns, xfer_for, xfer_next; /* a leader whose sync is outstanding this long hands leadership on */
    int stall_file;      /* TSDB_CLUSTER_TEST sync_stall_file=1: the worker holds a sync while <state-dir>/stall exists */
    char stallfile[1100];

    struct buf fb; /* the frame being built; never held across a call that builds another */
    raft_entry ents[RAFT_WIRE_MAX_ENTRIES];
} C;

static uint8_t my_flags(void);
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

/* tmp, fsync, rename, directory fsync. 0; -1 when nothing was renamed; -2 when the new name is in place and the
 * directory fsync failed: the caller must stop (a later fsync on another descriptor does not report that error) */
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
    return fsync_dir(C.metadir) == 0 ? 0 : -2;
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

/* meta/IDENT: this server's identity, made once. A server whose meta/ is gone is a new server (the member record of
 * the old one keeps refusing it, claim_ok). 0, or -1 with err. */
static int ident_load(char *err, size_t errn)
{
    FILE *f = fopen(C.identfile, "r");
    if (f) {
        char line[128] = "";
        int ok = fgets(line, sizeof line, f) != NULL && !strncmp(line, "ident=", 6);
        int bad = ferror(f);
        fclose(f);
        line[strcspn(line, "\n")] = 0;
        if (bad || !ok || unhex16(line + 6, C.ident) != 0 || zero16(C.ident)) {
            snprintf(err, errn, "%s is damaged (it needs ident=<32 hex digits>)", C.identfile);
            return -1;
        }
        return 0;
    }
    if (errno != ENOENT) {
        snprintf(err, errn, "cannot open %s: %s", C.identfile, strerror(errno));
        return -1;
    }
    do
        tsdb_random(C.ident, sizeof C.ident);
    while (zero16(C.ident));
    char u[33], text[64];
    hex16(C.ident, u);
    int n = snprintf(text, sizeof text, "ident=%s\n", u);
    if (write_durable(C.identfile, text, (size_t)n) != 0) {
        snprintf(err, errn, "cannot write %s: %s", C.identfile, strerror(errno));
        return -1;
    }
    return 0;
}

static void test_knobs(void)
{
    const char *t = getenv("TSDB_CLUSTER_TEST");
    C.snap_every = SNAP_EVERY;
    C.free_min = FREE_MIN_DEFAULT;
    C.stall_ns = SYNC_STALL_NS;
    for (const char *p = t; p && *p;) {
        unsigned long long v = 0;
        unsigned a = 0, b = 0, c = 0, ep = 0;
        char dn[META_DOMAIN_MAX] = "";
        if (sscanf(p, "free_min=%llu", &v) == 1)
            C.free_min = v;
        else if (sscanf(p, "vol=%u/%u/%u/%u/%63[^,]", &a, &b, &c, &ep, dn) >= 4 && C.nk_vols < 4) {
            /* volume/role/health/epoch/domain: a volume this server reports as owning */
            struct cluster_vol_report *v2 = &C.k_vols[C.nk_vols++];
            v2->volume = a;
            v2->role = (uint8_t)b;
            v2->health = (uint8_t)c;
            v2->epoch = ep;
            snprintf(v2->domain, sizeof v2->domain, "%s", dn);
        } else if (sscanf(p, "dom=%63[^/]/%u", dn, &a) == 2 && C.nk_doms < 4) {
            snprintf(C.k_doms[C.nk_doms].name, META_DOMAIN_MAX, "%s", dn);
            C.k_doms[C.nk_doms++].state = (uint8_t)a;
        } else if (sscanf(p, "snap_every=%llu", &v) == 1 && v)
            C.snap_every = v;
        else if (sscanf(p, "snap_retain=%llu", &v) == 1)
            C.snap_retain = v;
        else if (sscanf(p, "seg_bytes=%llu", &v) == 1)
            C.seg_bytes = v;
        else if (sscanf(p, "fail_snap_dirsync=%llu", &v) == 1)
            C.fail_snap_dirsync = v != 0;
        else if (sscanf(p, "sync_stall_file=%llu", &v) == 1)
            C.stall_file = v != 0;
        else if (sscanf(p, "stall_ms=%llu", &v) == 1 && v)
            C.stall_ns = v * 1000000ull;
        else if (sscanf(p, "hold_install_ms=%llu", &v) == 1)
            C.hold_install_ns = v * 1000000ull;
        p = strchr(p, ',');
        if (p)
            p++;
    }
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

/* the frame in C.fb to `c`: before AUTH into its queue unless `hs` (a handshake frame); after it tagged */
static void conn_send_buf(struct conn *c, const struct buf *f, int hs)
{
    if (c->dead)
        return;
    struct buf *to = hs || c->state == CS_READY ? &c->wb : &c->q;
    if (to->len - to->off + f->len + TAG > WB_MAX) {
        say("the link to node %" PRIu32 " (%s) has %u MiB queued; dropping it", c->id, c->peer, WB_MAX >> 20);
        c->dead = 1;
        return;
    }
    if (!hs && c->state == CS_READY)
        put_tagged(to, c->kout, &c->seq_out, f->p, f->len);
    else
        put(to, f->p, f->len);
}

/* AUTH passed: the link's keys from the transcript, and what was queued goes out tagged */
static void conn_ready(struct conn *c)
{
    const char *kd = c->out ? "tsdb-cluster-k>" : "tsdb-cluster-k<", *ku = c->out ? "tsdb-cluster-k<" : "tsdb-cluster-k>";
    hs_mac(C.secret, C.secret_len, kd, c->tr, c->trlen, c->kout);
    hs_mac(C.secret, C.secret_len, ku, c->tr, c->trlen, c->kin);
    c->state = CS_READY;
    for (size_t off = c->q.off; off + 4 <= c->q.len;) {
        size_t fl = 4 + (size_t)le32(c->q.p + off);
        put_tagged(&c->wb, c->kout, &c->seq_out, c->q.p + off, fl);
        off += fl;
    }
    buf_free(&c->q);
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
    put(&C.fb, C.ident, 16);
    put(&C.fb, c->nc, 32);
    putstr(&C.fb, C.padv);
    frame_end(&C.fb);
    c->trlen = C.fb.len - 5;
    memcpy(c->tr, C.fb.p + 5, c->trlen);
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

static cluster_report_fn g_reporter;
static void *g_reporter_arg;

void cluster_set_reporter(cluster_report_fn fn, void *arg)
{
    g_reporter = fn;
    g_reporter_arg = arg;
}

/* The sections of a heartbeat from r->off on, into h. Sets r->bad on a malformed one. An unknown type is skipped. */
static void heard_sections(struct heard *h, struct rd *r, uint64_t now)
{
    while (!r->bad && r->off < r->len) {
        uint32_t type = get8(r);
        type |= (uint32_t)get8(r) << 8;
        uint32_t len = get32(r);
        if (r->bad || !need(r, len)) {
            r->bad = 1;
            return;
        }
        struct rd s = {r->p + r->off, len, 0, 0};
        r->off += len;
        if (type == HBS_NODE) {
            uint8_t flags = get8(&s), dropped = get8(&s), rb = get8(&s);
            uint64_t fr = get64(&s);
            if (s.bad) {
                r->bad = 1;
                return;
            }
            h->flags = flags;
            h->core_dropped = dropped;
            h->read_barrier = rb;
            h->state_free = fr;
            h->have_flags = 1;
            int bad = !(flags & META_MF_VOTER_OK) || dropped;
            h->bad_ns = !bad ? 0 : h->bad_ns ? h->bad_ns : now;
        } else if (type == HBS_VOLUMES) {
            uint32_t n = get32(&s);
            if (s.bad || n > CLUSTER_REPORT_VOLS || (size_t)n * 62 > len) {
                r->bad = 1;
                return;
            }
            struct cluster_vol_status *nv = calloc(n ? n : 1, sizeof *nv);
            if (!nv)
                oom();
            for (uint32_t i = 0; i < n; i++) {
                struct cluster_vol_report *v = &nv[i].v;
                v->volume = get32(&s);
                v->role = get8(&s);
                v->health = get8(&s);
                v->epoch = get64(&s);
                getstr(&s, v->domain, sizeof v->domain);
                v->series_len = get64(&s);
                v->states_len = get64(&s);
                v->fenced_age_ms = get64(&s);
                v->wal_seg = get64(&s);
                v->wal_off = get64(&s);
                if (v->health == CL_HEALTH_OK)
                    continue;
                /* the timer of a (volume, domain) fault runs from the first report of it and breaks on a report
                 * without it (M6); the leader reads it as `local_fault_s` */
                nv[i].fault_ns = now;
                for (uint32_t j = 0; j < h->nvols; j++)
                    if (h->vols[j].v.volume == v->volume && h->vols[j].fault_ns &&
                        !strcmp(h->vols[j].v.domain, v->domain))
                        nv[i].fault_ns = h->vols[j].fault_ns;
            }
            if (s.bad) {
                free(nv);
                r->bad = 1;
                return;
            }
            free(h->vols);
            h->vols = nv;
            h->nvols = n;
        } else if (type == HBS_DOMAINS) {
            uint32_t n = get32(&s);
            if (s.bad || n > CLUSTER_REPORT_DOMS || (size_t)n * 3 > len) {
                r->bad = 1;
                return;
            }
            struct cluster_dom_report *nd = calloc(n ? n : 1, sizeof *nd);
            if (!nd)
                oom();
            for (uint32_t i = 0; i < n; i++) {
                getstr(&s, nd[i].name, sizeof nd[i].name);
                nd[i].state = get8(&s);
            }
            if (s.bad) {
                free(nd);
                r->bad = 1;
                return;
            }
            free(h->doms);
            h->doms = nd;
            h->ndoms = n;
        }
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
    C.pend[C.npend++] = (struct pend){conn, reqid, member, index, term, C.pend_join};
}

static void send_join(struct conn *d, uint32_t id);

/* entry `index` (`term`) applied with the first operation's result rc0: answer whoever waited for it */
static void pend_applied(uint64_t index, uint64_t term, int rc0)
{
    for (size_t i = 0; i < C.npend;) {
        struct pend *p = &C.pend[i];
        if (p->index <= index) {
            int rc = p->index == index && p->term == term ? rc0 : CLUSTER_ERR_RETRY;
            struct conn *d = p->join ? conn_by_serial(p->join) : NULL;
            if (d && rc == 0)
                send_join(d, p->member); /* committed: now the server may learn its id; it answers the CLI */
            else if (p->conn)
                admin_reply(p->conn, p->reqid, p->join && rc == 0 ? CLUSTER_ERR_UNREACHABLE : rc, NULL, 0);
            if (d && rc != 0)
                d->dead = 1;
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

/* A fault of the Raft log or the hard state (item 3, M11). In a shared-mode deployment the data does not live with the
 * log, so it ends only this node's voter role: the core stops, a `retired` mark is written (this identity never boots
 * a core from these files again), the heartbeat reports core_dropped, and the node goes on as a watcher with its
 * volumes. Replicated mode keeps the fail-stop (the log and the data share disks). Returns 1 when the core was
 * dropped (the caller must not touch C.r or C.log), else it does not return. */
static int log_fault(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

static void check_fault(void)
{
    char what[512];
    if (C.log && raft_log_fault(C.log, what, sizeof what))
        log_fault("the Raft log: %s", what);
    else if (C.r && raft_get_store_fault(C.r, what, sizeof what) != ISO_OK)
        log_fault("the Raft log: %s", what);
}

/* ------------------------------------------------------------------ the disk worker */

/* The worker takes jobs in order: a sync round's fdatasyncs (descriptors only, never the log) and a snapshot's durable
 * write. Everything that touches the log's own state (prepare, durable, compaction, the hard state) stays on the core
 * thread: the hard state's slot sequence is shared with the durable floor, which the Raft core's log does not guard. */
static void *dw_main(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&C.dw_mu);
    for (;;) {
        while (!C.dw_nq && !C.dw_stop)
            pthread_cond_wait(&C.dw_cv, &C.dw_mu);
        if (!C.dw_nq)
            break;
        struct djob j = C.dw_q[0];
        memmove(C.dw_q, C.dw_q + 1, (--C.dw_nq) * sizeof C.dw_q[0]);
        C.dw_busy = 1;
        pthread_mutex_unlock(&C.dw_mu);
        struct dres r = {.kind = j.kind, .gen = j.gen, .index = j.index, .term = j.term, .img = j.img, .len = j.len,
                         .cfg = j.cfg};
        if (j.kind == DJ_SYNC) {
            while (C.stall_file && access(C.stallfile, F_OK) == 0 && !C.dw_stop)
                usleep(10000); /* a test's stalled disk */
            iso_err e = raft_log_sync_run(&j.sync);
            r.index = j.sync.index;
            r.term = j.sync.term;
            r.rc = e != ISO_OK || j.sync.err;
            r.err = j.sync.err ? j.sync.err : EIO;
        } else {
            r.rc = write_durable(C.snapfile, j.img, j.len);
            r.err = errno;
            if (r.rc == 0 && j.fail_dirsync) {
                r.rc = -2;
                r.err = EIO;
            }
        }
        pthread_mutex_lock(&C.dw_mu);
        C.dw_busy = 0;
        if (C.dw_nr < DW_Q)
            C.dw_r[C.dw_nr++] = r;
        else
            free(r.img); /* cannot happen: the core thread has at most two jobs out */
        pthread_cond_broadcast(&C.dw_idle);
        ssize_t w = write(C.dw_pipe[1], "x", 1);
        (void)w;
    }
    pthread_mutex_unlock(&C.dw_mu);
    return NULL;
}

static int dw_start(void)
{
    if (C.dw_on)
        return 0;
    pthread_mutex_init(&C.dw_mu, NULL);
    pthread_cond_init(&C.dw_cv, NULL);
    pthread_cond_init(&C.dw_idle, NULL);
    if (pipe2(C.dw_pipe, O_CLOEXEC | O_NONBLOCK) != 0)
        return -1;
    C.dw_stop = 0;
    if (pthread_create(&C.dw_th, NULL, dw_main, NULL) != 0) {
        close(C.dw_pipe[0]);
        close(C.dw_pipe[1]);
        return -1;
    }
    C.dw_on = 1;
    return 0;
}

static void dw_submit(struct djob *j)
{
    j->gen = C.dw_gen;
    pthread_mutex_lock(&C.dw_mu);
    if (C.dw_nq == DW_Q)
        fail_stop("the disk worker's queue is full"); /* the core thread keeps at most two jobs out */
    C.dw_q[C.dw_nq++] = *j;
    pthread_cond_signal(&C.dw_cv);
    pthread_mutex_unlock(&C.dw_mu);
}

/* Waits until the worker has nothing queued or running (the core is about to go, or to rewrite the snapshot file:
 * what the worker holds must not land after that). Its results stay for dw_collect, which drops them by generation. */
static void dw_quiesce(void)
{
    if (!C.dw_on)
        return;
    pthread_mutex_lock(&C.dw_mu);
    while (C.dw_nq || C.dw_busy)
        pthread_cond_wait(&C.dw_idle, &C.dw_mu);
    pthread_mutex_unlock(&C.dw_mu);
}

/* The core was stopped or its log reset: what the worker was doing means nothing to the core that follows. */
static void dw_forget(void)
{
    dw_quiesce();
    C.dw_gen++;
    C.sync_since = 0;
    C.snap_out = 0;
}

static void dw_stop_thread(void)
{
    if (!C.dw_on)
        return;
    pthread_mutex_lock(&C.dw_mu);
    C.dw_stop = 1;
    pthread_cond_signal(&C.dw_cv);
    pthread_mutex_unlock(&C.dw_mu);
    pthread_join(C.dw_th, NULL);
    for (size_t i = 0; i < C.dw_nr; i++)
        free(C.dw_r[i].img);
    C.dw_nr = 0;
    close(C.dw_pipe[0]);
    close(C.dw_pipe[1]);
    C.dw_on = 0;
}

static void snap_done(struct dres *r);

/* The results the worker has for the core thread, applied in order. */
static void dw_collect(void)
{
    if (!C.dw_on)
        return;
    char junk[64];
    while (read(C.dw_pipe[0], junk, sizeof junk) > 0)
        ;
    for (;;) {
        struct dres r;
        pthread_mutex_lock(&C.dw_mu);
        if (!C.dw_nr) {
            pthread_mutex_unlock(&C.dw_mu);
            return;
        }
        r = C.dw_r[0];
        memmove(C.dw_r, C.dw_r + 1, (--C.dw_nr) * sizeof C.dw_r[0]);
        pthread_mutex_unlock(&C.dw_mu);
        if (r.gen != C.dw_gen || !C.r) {
            free(r.img);
            continue;
        }
        if (r.kind == DJ_SYNC) {
            C.sync_since = 0;
            if (r.rc) {
                log_fault("syncing the Raft log in %s: %s", C.logdir, strerror(r.err));
                continue;
            }
            raft_log_durable(C.log, r.index, r.term);
            raft_persisted(C.r, r.index, r.term);
        } else {
            snap_done(&r);
        }
    }
}

/* A sync round: the descriptors are taken here, the fdatasyncs run on the worker, and the core hears raft_persisted
 * from dw_collect. One round at a time: the core asks again for what was appended meanwhile. */
static void sync_round(void)
{
    if (C.sync_since)
        return;
    struct djob j = {.kind = DJ_SYNC};
    iso_err e = raft_log_sync_prepare(C.log, &j.sync);
    if (e != ISO_OK) {
        for (unsigned i = 0; i < j.sync.n; i++)
            close(j.sync.fds[i]);
        log_fault("syncing the Raft log in %s: %s", C.logdir, strerror(j.sync.err ? j.sync.err : EIO));
        return;
    }
    if (!j.sync.n && !j.sync.err) { /* nothing to sync: done at once */
        raft_log_durable(C.log, j.sync.index, j.sync.term);
        raft_persisted(C.r, j.sync.index, j.sync.term);
        return;
    }
    C.sync_since = iso_now_ns();
    dw_submit(&j);
}

static int config_of(const struct meta_state *m, raft_config *c)
{
    c->n = 0;
    return m->config_len && raft_config_decode(m->config, m->config_len, c);
}

static void take_snapshot(void)
{
    if (C.snap_out)
        return;
    struct djob j = {.kind = DJ_SNAP};
    if (!config_of(&C.sm, &j.cfg))
        return;
    if (meta_snapshot_encode(&C.sm, &j.img, &j.len) != META_OK)
        oom();
    j.index = C.sm.applied;
    j.term = C.sm.applied_term;
    j.fail_dirsync = C.fail_snap_dirsync;
    C.snap_out = 1;
    dw_submit(&j);
}

/* The worker wrote the image (or failed to): the log is compacted onto it only now, as it was when this ran inline. */
static void snap_done(struct dres *r)
{
    C.snap_out = 0;
    if (r->rc == -2) /* the new snapshot is named, maybe not durably: compacting on it could lose the log's base */
        fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.snapfile, strerror(r->err));
    if (r->rc != 0) {
        say("cannot write %s: %s (the log is kept until a later snapshot is written)", C.snapfile, strerror(r->err));
        free(r->img);
        C.snap_index = r->index; /* try again snap_every entries later, not on every one */
        return;
    }
    free(C.snap);
    C.snap = r->img;
    C.snaplen = r->len;
    C.snap_index = r->index;
    raft_compact(C.r, r->index, r->term, &r->cfg);
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
    if (C.fb.len - 4 + TAG > FRAME_MAX) {
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
    dw_forget(); /* a snapshot write still out would land over this image */
    struct meta_state t;
    meta_init(&t);
    if (meta_snapshot_decode(&t, a->u.snapshot.data, a->u.snapshot.len) != META_OK ||
        t.applied != a->u.snapshot.index)
        fail_stop("the snapshot image from node %" PRIu32 " at %" PRIu64 " does not verify", a->u.snapshot.from,
                  a->u.snapshot.index);
    if (write_durable(C.snapfile, a->u.snapshot.data, a->u.snapshot.len) != 0)
        fail_stop("cannot write %s: %s", C.snapfile, strerror(errno));
    if (C.pend_index) { /* the kept copy has done its work; one left behind is ignored at the next start */
        unlink(C.pendfile);
        C.pend_index = 0;
    }
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
            if (raft_log_hard_save(C.log, &a.u.hard, err, sizeof err) != ISO_OK) {
                log_fault("saving the Raft hard state: %s", err);
                return;
            }
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
    if (C.sm.applied >= C.snap_index + C.snap_every)
        take_snapshot();
}

/* M11: a leader whose own log sync has been outstanding for SYNC_STALL_NS cannot commit anything, and its disk may be
 * gone for hours. The heartbeats and the Raft messages still run (the worker has the sync), so it hands leadership to
 * the voter that holds most of its log (the core's transfer: TimeoutNow once that voter has everything) and steps
 * down on the higher term. Tried again every second while the same sync is out. */
static void sync_stall_check(uint64_t now)
{
    if (!C.sync_since || C.role != RAFT_LEADER || now - C.sync_since < C.stall_ns || now < C.xfer_next)
        return;
    C.xfer_next = now + 1000 * MS;
    const raft_config *cfg = raft_get_config(C.r);
    raft_id best = 0;
    raft_index best_match = 0;
    for (unsigned i = 0; cfg && i < cfg->n; i++) {
        raft_progress pr;
        if (cfg->m[i].learner || cfg->m[i].id == C.id || !raft_get_progress(C.r, cfg->m[i].id, &pr))
            continue;
        if (!best || pr.match > best_match) {
            best = cfg->m[i].id;
            best_match = pr.match;
        }
    }
    if (!best)
        return;
    if (C.xfer_for != C.sync_since) {
        C.xfer_for = C.sync_since;
        say("node %" PRIu32 ": its Raft log sync has been outstanding for %" PRIu64 " ms; handing leadership to node %u",
            C.id, (uint64_t)((now - C.sync_since) / MS), (unsigned)best);
    }
    raft_transfer(C.r, best);
}

static void close_watches(void)
{
    for (size_t i = 0; i < C.nconns; i++)
        if (C.conns[i]->watch_served || C.conns[i]->watch_ours)
            C.conns[i]->dead = 1;
    C.watch_conn = 0;
}

/* The log begins after the snapshot's index and no kept image fits (adopt_install takes it before the open; only
 * damage gets here, as keep_install's copy is durable before the core resets the log). Entries between the two are in neither, so this
 * server could never apply again. It fences its vote at what its log holds (the log forgot what it acknowledged;
 * the fence lifts once it holds that entry again or a snapshot covers it), drops the old snapshot and empties the
 * log, so the leader sends it an image again. Each step is durable before the next, and a crash part way leaves
 * the same condition, which the next start repairs the same way. 0, or -1 with err. */
static int reseed_self(raft_hard_state *hs, char *err, size_t errn)
{
    const raft_log_ops *ops = &raft_log_file_ops;
    raft_index first = ops->first(C.log), last = ops->last(C.log);
    raft_index fi = last;
    raft_term ft = ops->term_at(C.log, last);
    say("the Raft log in %s begins at %" PRIu64 ", after the snapshot's %" PRIu64 " (an install that did not finish):"
        " fencing the vote at %" PRIu64 " and emptying the log, for a new image from the leader",
        C.logdir, (uint64_t)(first - 1), C.sm.applied, (uint64_t)fi);
    if (hs->fence_index > fi) { /* a fence from an earlier try of this, on more than the log holds now */
        fi = hs->fence_index;
        ft = hs->fence_term;
    }
    if (raft_log_hard_fence(C.log, hs->term, fi, ft, hs, err, errn) != ISO_OK)
        return -1;
    if (exists(C.snapfile) && (unlink(C.snapfile) != 0 || fsync_dir(C.metadir) != 0)) {
        snprintf(err, errn, "cannot remove %s: %s", C.snapfile, strerror(errno));
        return -1;
    }
    if (raft_log_discard(C.log, err, errn) != ISO_OK)
        return -1;
    meta_free(&C.sm);
    meta_init(&C.sm);
    free(C.snap);
    C.snap = NULL;
    C.snaplen = C.snap_index = 0;
    return 0;
}

/* An install image, kept durably in meta/snapshot.install before the core sees the message. The core resets the log
 * to the image's index inside raft_step, before it hands out RESTORE, so a crash between that reset and restore()'s
 * write of meta/snapshot would leave a log based past the snapshot; with the image kept first, the next start takes it
 * (adopt_install) and nothing is lost. An image that does not verify goes to the core as it is (restore() stops on
 * it). 1 when the core may take the message, 0 when it must not (nothing kept; the leader sends it again). */
static int keep_install(const raft_msg *m)
{
    if (m->u.snapshot.index <= C.pend_index || m->u.snapshot.index <= C.sm.applied)
        return 1; /* kept already, or the core refuses it as old */
    struct meta_state t;
    meta_init(&t);
    int ok = meta_snapshot_decode(&t, m->u.snapshot.data, m->u.snapshot.len) == META_OK &&
             t.applied == m->u.snapshot.index && t.applied_term == m->u.snapshot.term;
    meta_free(&t);
    if (!ok)
        return 1;
    int rc = write_durable(C.pendfile, m->u.snapshot.data, m->u.snapshot.len);
    if (rc == -2)
        fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.pendfile, strerror(errno));
    if (rc != 0) {
        say("cannot write %s: %s (the install waits for the leader's next try)", C.pendfile, strerror(errno));
        return 0;
    }
    C.pend_index = m->u.snapshot.index;
    return 1;
}

/* A whole image file decoded into *m (img and n keep its bytes). 0, or -1 with err. */
static int load_image(const char *path, struct meta_state *m, uint8_t **img_out, size_t *n_out, char *err,
                      size_t errn)
{
    FILE *f = fopen(path, "rb");
    if (!f) {
        snprintf(err, errn, "cannot open %s: %s", path, strerror(errno));
        return -1;
    }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *img = n > 0 ? malloc((size_t)n) : NULL;
    int ok = img && fread(img, 1, (size_t)n, f) == (size_t)n && !ferror(f);
    fclose(f);
    if (!ok || meta_snapshot_decode(m, img, (size_t)n) != META_OK) {
        free(img);
        snprintf(err, errn, "%s is damaged (it does not verify): this server's metadata cannot be read", path);
        return -1;
    }
    *img_out = img;
    *n_out = (size_t)n;
    return 0;
}

/* The image keep_install kept, when it is ahead of the snapshot: it becomes the snapshot, before the log is opened.
 * An install is the core's reset of the log to the image's index followed by restore()'s write of the snapshot,
 * and a crash anywhere in between (or a reset the crash cut part way: a prefix of the old segments, or a new
 * segment whose header is not durable) must find the store the reset was made for. The image is a leader's, so it is
 * committed state (Raft section 7): every entry it covers is in it, whatever the log still holds. An image the core
 * refused, or whose reset the log already holds, is harmless: core_boot then finds (index, term) in the log and
 * keeps its suffix. An image that does not verify, or is not ahead, is left (core_boot unlinks it).
 * 1 taken, 0 none, -1 with err. */
static int adopt_install(char *err, size_t errn)
{
    if (!exists(C.pendfile))
        return 0;
    struct meta_state t;
    meta_init(&t);
    uint8_t *img = NULL;
    size_t n = 0;
    char why[256];
    if (load_image(C.pendfile, &t, &img, &n, why, sizeof why) != 0) {
        say("%s is dropped: %s", C.pendfile, why);
        meta_free(&t);
        return 0;
    }
    if (t.applied <= C.sm.applied) {
        meta_free(&t);
        free(img);
        return 0;
    }
    if (rename(C.pendfile, C.snapfile) != 0 || fsync_dir(C.metadir) != 0) {
        snprintf(err, errn, "cannot make %s the snapshot: %s", C.pendfile, strerror(errno));
        meta_free(&t);
        free(img);
        return -1;
    }
    say("taking the install image kept in %s (index %" PRIu64 ", term %" PRIu64 ") over the snapshot's %" PRIu64,
        C.pendfile, t.applied, (uint64_t)t.applied_term, C.sm.applied);
    meta_free(&C.sm);
    C.sm = t;
    free(C.snap);
    C.snap = img;
    C.snaplen = n;
    C.snap_index = C.sm.applied;
    return 1;
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
        if (load_image(C.snapfile, &C.sm, &C.snap, &C.snaplen, err, errn) != 0)
            return -1;
        C.snap_index = C.sm.applied;
    }
    C.pend_index = 0;
    /* Before the open: the log's own rules for a reset beside a store behind it (a lone segment named for anything
     * but base + 1 is no tear) name the store the reset was made for. */
    int adopted = adopt_install(err, errn);
    if (adopted < 0)
        return -1;
    raft_log_opts lo = {.segment_bytes = C.seg_bytes};
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
    raft_index first = raft_log_file_ops.first(C.log), last = raft_log_file_ops.last(C.log);
    raft_term at = C.sm.applied ? raft_log_file_ops.term_at(C.log, C.sm.applied) : 0;
    int bad = 0;
    if (first > 1 && first - 1 > C.sm.applied) {
        bad = reseed_self(&hs, err, errn) != 0; /* damage only: the image kept for the log's base is gone */
    } else if (last < C.sm.applied || (adopted && C.sm.applied && at != C.sm.applied_term)) {
        /* The log ends below the store (a reset stopped part way, or entries above the durable floor lost), or
         * holds another entry than the leader's image at its index: everything in it is at or below the index or
         * can never commit (Log Matching), and the store holds it. The snapshot is durable. */
        say("the Raft log in %s ends at %" PRIu64 ", the snapshot is at %" PRIu64 " (term %" PRIu64 "): starting the log"
            " on it", C.logdir, (uint64_t)last, C.sm.applied, (uint64_t)C.sm.applied_term);
        bad = raft_log_rebase(C.log, C.sm.applied, C.sm.applied_term, err, errn) != ISO_OK;
    } else if (C.sm.applied && at != C.sm.applied_term) {
        snprintf(err, errn, "the Raft log in %s holds term %" PRIu64 " at the snapshot's index %" PRIu64
                 ", the snapshot says %" PRIu64 ": damage", C.logdir, (uint64_t)at, C.sm.applied,
                 (uint64_t)C.sm.applied_term);
        bad = 1;
    }
    if (bad) {
        raft_log_close(C.log);
        C.log = NULL;
        return -1;
    }
    if (exists(C.pendfile))
        unlink(C.pendfile); /* an image the log never moved to, or taken above: no longer needed */
    raft_boot b = {.hard = hs, .applied = C.sm.applied, .applied_term = C.sm.applied_term};
    config_of(&C.sm, &b.config);
    raft_opts o;
    raft_opts_defaults(&o);
    if (C.snap_retain)
        o.snapshot_retain = C.snap_retain;
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

static void core_stop(struct meta_state *next);

/* removed from the group while away: the core goes (its files stay), the state goes on from `next` (or as is) */
static void drop_core(struct meta_state *next, const char *why)
{
    say("node %" PRIu32 " is not in the metadata group's configuration (%s): it follows a watch stream now", C.id,
        why);
    core_stop(next);
}

static int log_fault(const char *fmt, ...)
{
    char what[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(what, sizeof what, fmt, ap);
    va_end(ap);
    if (C.sm.mode != META_MODE_SHARED)
        fail_stop("%s", what);
    say("%s: in shared mode this ends the voter role only (core_dropped, retired); the volumes go on", what);
    core_stop(NULL);
    C.core_dropped = C.retired = 1;
    /* the mark is what keeps a restart from booting a core out of these files; if it cannot be written the node
     * cannot be sure of that, and stops (the disk is gone: the log was the only thing that relied on it) */
    if (write_durable(C.retiredfile, "retired\n", 8) != 0)
        fail_stop("cannot write %s: %s (after: %s)", C.retiredfile, strerror(errno), what);
    return 1;
}

static void core_stop(struct meta_state *next)
{
    dw_forget();
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
    if (m.type == RAFT_MSG_SNAPSHOT && C.hold_install_ns) {
        uint64_t now = iso_now_ns();
        if (!C.hold_from)
            C.hold_from = now;
        if (since(now, C.hold_from) < C.hold_install_ns)
            return; /* lost on the way, as far as the leader knows: it sends the image again */
    }
    if (m.type == RAFT_MSG_SNAPSHOT) {
        m.u.snapshot.data = r->p + r->off + wl;
        m.u.snapshot.len = r->len - r->off - wl;
        if (!keep_install(&m))
            return; /* not kept: the core must not reset the log for it; the leader sends it again */
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
    uint64_t now = iso_now_ns();
    heard_set(node, now, inc, applied, addr, peer);
    heard_sections(heard_get(node), r, now);
    if (r->bad) { /* a malformed section: the link is dropped, as for any broken frame */
        c->dead = 1;
        return;
    }
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
    if (term > C.hint_term || leader)
        C.hint_leader = leader;
    /* the highest commit any voter reported in this term: what the watch stream must reach (watch_duty) */
    if (term > C.hint_term || commit > C.hint_commit)
        C.hint_commit = commit;
    C.hint_term = term;
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
    int old_copy = to == &C.sm && C.copy_stale; /* a persisted copy of an earlier run yields to any live image */
    if (meta_snapshot_decode(&t, r->p + r->off, r->len - r->off) != META_OK || (t.applied < to->applied && !old_copy)) {
        meta_free(&t);
        c->dead = 1; /* a damaged image, or a state behind this one: another server */
        return;
    }
    meta_free(to);
    *to = t;
    if (to == &C.sm) {
        C.copy_stale = 0;
        C.copy_dirty = 1;
        state_replaced();
    } else {
        check_probe();
    }
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
    if (to == &C.wsm) {
        check_probe();
    } else {
        C.copy_stale = 0;
        C.copy_dirty = 1;
    }
}

/* Item 8: a member without a core keeps its copy of the metadata (the snapshot bytes, so one format) in the state
 * directory, written a moment after the watch stream moved it (tmp, fsync, rename, directory fsync), and serves from
 * it after a restart, marked stale until a stream confirms it. A failed write keeps the older copy and is tried
 * again: nothing in step 1 relies on it before a reservation does (S3). */
static void copy_flush(uint64_t now, int force)
{
    if (!C.copy_dirty || C.r || (!force && now < C.copy_next))
        return;
    C.copy_dirty = 0;
    if (!C.sm.initialised)
        return;
    uint8_t *img;
    size_t len;
    if (meta_snapshot_encode(&C.sm, &img, &len) != META_OK)
        oom();
    int rc = write_durable(C.copyfile, img, len);
    free(img);
    if (rc == -2)
        fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.copyfile, strerror(errno));
    if (rc != 0) {
        say("cannot write %s: %s (tried again in 1 s)", C.copyfile, strerror(errno));
        C.copy_dirty = 1;
        C.copy_next = now + 1000 * MS;
        return;
    }
    C.copy_next = now + 200 * MS;
}

/* what a member's heartbeat last reported (the sections), after its line of the status */
static void put_heard_extra(struct buf *b, const struct heard *h, uint64_t now)
{
    put8(b, h && h->have_flags ? h->flags : 0);
    put8(b, h ? h->core_dropped : 0);
    put8(b, h ? h->read_barrier : 0);
    put64(b, h && h->have_flags ? h->state_free : 0);
    put32(b, h ? h->nvols : 0);
    for (uint32_t i = 0; h && i < h->nvols; i++) {
        const struct cluster_vol_report *v = &h->vols[i].v;
        put32(b, v->volume);
        put8(b, v->role);
        put8(b, v->health);
        put64(b, v->epoch);
        putstr(b, v->domain);
        put64(b, v->series_len);
        put64(b, v->states_len);
        put64(b, v->fenced_age_ms);
        put64(b, v->wal_seg);
        put64(b, v->wal_off);
        put64(b, h->vols[i].fault_ns ? since(now, h->vols[i].fault_ns) / (1000 * MS) : 0);
    }
    put32(b, h ? h->ndoms : 0);
    for (uint32_t i = 0; h && i < h->ndoms; i++) {
        putstr(b, h->doms[i].name);
        put8(b, h->doms[i].state);
    }
}

static void put_status(struct buf *b)
{
    uint64_t now = iso_now_ns();
    put32(b, C.have_node ? C.id : 0);
    put8(b, C.r ? CLUSTER_MODE_CORE : C.have_node ? CLUSTER_MODE_WATCH : CLUSTER_MODE_NONE);
    put8(b, C.r ? (uint8_t)raft_get_role(C.r) : 0);
    put8(b, (uint8_t)C.copy_stale);
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
        put_heard_extra(b, heard_get(C.id), now);
    }
    for (size_t i = 0; i < C.nheard; i++) {
        if (C.heard[i].id == C.id)
            continue;
        put32(b, C.heard[i].id);
        put64(b, (since(now, C.heard[i].ns)) / MS);
        put64(b, C.heard[i].inc);
        put_heard_extra(b, &C.heard[i], now);
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
    int mode;
    if (settings_decode(r, &s, &mode) != 0) {
        admin_reply(c->serial, reqid, META_ERR_FORMAT, NULL, 0);
        return;
    }
    /* NODE is written first; a crash before the snapshot left NODE of id 1 and nothing else: an init to finish */
    int unfinished = C.have_node && C.id == 1 && !C.r && !C.sm.initialised && !exists(C.snapfile) &&
                     !exists(C.logdir);
    if (C.have_node && !unfinished && (C.sm.initialised || !C.r || C.id != 1 || C.init_pending)) {
        admin_reply(c->serial, reqid, CLUSTER_ERR_EXISTS, NULL, 0);
        return;
    }
    if (unfinished)
        say("finishing an init that stopped before its first snapshot");
    if (!C.have_node || unfinished) {
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
    C.init_mode = mode;
}

/* NODE_ADD (§2.2), on the leader. The new server's id comes from the log, never from this leader's memory: PROBE asks
 * the server for its identity; MEMBER_ADD commits the next id with that identity; only then does JOIN tell the
 * server its id, which it writes to NODE. A leader that loses its office part way leaves either nothing or a
 * committed member the server has not heard of yet; `node add` again finds that member by the identity and sends
 * the JOIN. So one id is never handed to two servers, and claim_ok refuses a server that claims another's. */
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
    struct conn *d = dial_conn(addr, 0);
    if (!d) {
        admin_reply(c->serial, reqid, CLUSTER_ERR_UNREACHABLE, NULL, 0);
        return;
    }
    d->join = 1;
    d->join_cli = c->serial;
    d->join_reqid = reqid;
    frame_begin(&C.fb, F_PROBE);
    put(&C.fb, C.uuid, 16);
    frame_end(&C.fb);
    conn_send(d, 0);
}

/* the leader's JOIN: the committed id, and the identity it was committed with */
static void send_join(struct conn *d, uint32_t id)
{
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
    d->join = 2;
    d->join_id = id;
    struct buf b = {0}; /* not C.fb: this runs inside an apply */
    frame_begin(&b, F_JOIN);
    put(&b, C.uuid, 16);
    put32(&b, id);
    put(&b, d->ident, 16);
    putstr(&b, seeds);
    frame_end(&b);
    conn_send_buf(d, &b, 0);
    buf_free(&b);
}

/* on the new server: who it is, and whether it is free to join this cluster */
static void on_probe(struct conn *c, struct rd *r)
{
    uint8_t uuid[16];
    getn(r, uuid, 16);
    if (r->bad || zero16(uuid)) {
        c->dead = 1;
        return;
    }
    int32_t rc = C.have_node && memcmp(uuid, C.uuid, 16) != 0 ? CLUSTER_ERR_REFUSED : 0;
    frame_begin(&C.fb, F_PROBE_REPLY);
    put32(&C.fb, (uint32_t)rc);
    put32(&C.fb, C.have_node ? C.id : 0);
    put64(&C.fb, C.have_node ? C.incarnation : 1);
    putstr(&C.fb, C.adv);
    putstr(&C.fb, C.padv);
    putstr(&C.fb, C.rack);
    putstr(&C.fb, C.feed);
    putstr(&C.fb, C.bmc);
    put8(&C.fb, my_flags());
    frame_end(&C.fb);
    conn_send(c, 0);
}

static const struct meta_member *member_by_ident(const uint8_t ident[16])
{
    for (uint32_t i = 0; i < C.sm.nmembers; i++)
        if (C.sm.members[i].state != META_OUT && !memcmp(C.sm.members[i].ident, ident, 16))
            return &C.sm.members[i];
    return NULL;
}

static void on_probe_reply(struct conn *c, struct rd *r)
{
    if (c->join != 1)
        return;
    struct meta_member mb = {0};
    int32_t rc = (int32_t)get32(r);
    uint32_t has = get32(r);
    mb.incarnation = get64(r);
    getstr(r, mb.addr, sizeof mb.addr);
    getstr(r, mb.peer, sizeof mb.peer);
    getstr(r, mb.rack, sizeof mb.rack);
    getstr(r, mb.power_feed, sizeof mb.power_feed);
    getstr(r, mb.bmc, sizeof mb.bmc);
    mb.flags = get8(r); /* what the new server says it is: a witness holds no volumes, a full disk no vote */
    c->join = 0;
    if (r->bad)
        rc = CLUSTER_ERR_UNREACHABLE;
    if (!rc && (!C.r || raft_get_role(C.r) != RAFT_LEADER)) {
        const char *lp = leader_peer();
        admin_reply(c->join_cli, c->join_reqid, CLUSTER_ERR_NOTLEADER, lp, strlen(lp));
        c->dead = 1;
        return;
    }
    const struct meta_member *known = rc ? NULL : member_by_ident(c->ident);
    if (!rc && known && (!has || has == known->id)) {
        send_join(c, known->id); /* committed before, its JOIN never arrived: the same id again */
        return;
    }
    if (!rc && has)
        rc = CLUSTER_ERR_REFUSED; /* it holds an id that no live member of this cluster has with its identity */
    if (rc) {
        admin_reply(c->join_cli, c->join_reqid, rc, NULL, 0);
        c->dead = 1;
        return;
    }
    mb.id = C.sm.next_member > C.reserved_id ? C.sm.next_member : C.reserved_id;
    if (!mb.id)
        mb.id = 1;
    C.reserved_id = mb.id + 1; /* two adds in flight: the second asks for the next id (the log decides either way) */
    mb.state = META_UP;
    memcpy(mb.ident, c->ident, 16);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_add(&e, &mb);
    heard_set(mb.id, iso_now_ns(), mb.incarnation, 0, NULL, NULL); /* not down before it heard its JOIN */
    C.pend_join = c->serial;
    if (propose(&e, c->join_cli, c->join_reqid, mb.id) != ISO_OK)
        c->dead = 1;
    C.pend_join = 0;
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
    uint8_t uuid[16], ident[16];
    getn(r, uuid, 16);
    uint32_t id = get32(r);
    getn(r, ident, 16);
    char seeds[1024];
    getstr(r, seeds, sizeof seeds);
    int32_t rc = 0;
    if (r->bad || !id || zero16(uuid)) {
        c->dead = 1;
        return;
    }
    if (memcmp(ident, C.ident, 16) != 0) {
        rc = CLUSTER_ERR_REFUSED; /* the id was committed for another server */
    } else if (!C.have_node) {
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
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void on_join_reply(struct conn *c, struct rd *r)
{
    if (c->join != 2)
        return;
    int32_t rc = (int32_t)get32(r);
    c->join = 0;
    c->dead = 1; /* it knew no id when it answered the handshake; the members link again by id */
    if (r->bad)
        rc = CLUSTER_ERR_UNREACHABLE;
    if (!rc)
        heard_set(c->join_id, iso_now_ns(), 1, 0, NULL, NULL);
    admin_reply(c->join_cli, c->join_reqid, rc, NULL, 0);
}

/* A server that claims member `id` with identity `ident` (HELLO, HELLO_ACK): refused when it is this server itself
 * (a handshake reflected back) or when the member record names another server. Two servers must never act as one
 * member: each would hold its own log and vote. A record this server does not know yet passes (the id was committed
 * with the identity before JOIN named it, so a server that knows the record checks it). */
static int claim_ok(uint32_t id, const uint8_t ident[16])
{
    if (!memcmp(ident, C.ident, 16))
        return 0;
    if (!id)
        return 1;
    if (zero16(ident))
        return 0;
    const struct meta_member *m = meta_member(&C.sm, id);
    if (!m && C.wsm_on)
        m = meta_member(&C.wsm, id);
    return !m || zero16(m->ident) || !memcmp(m->ident, ident, 16);
}

static void handshake(struct conn *c, uint8_t type, struct rd *r)
{
    uint8_t uuid[16], ident[16], m[32], want[32], ns[32];
    if (c->state == CS_WAIT_HELLO && type == F_HELLO) {
        memcpy(c->tr, r->p, r->len); /* r->len <= HS_FRAME_MAX (conn_read) */
        c->trlen = r->len;
        uint32_t magic = get32(r);
        uint8_t ver = get8(r);
        uint32_t id = get32(r);
        getn(r, uuid, 16);
        getn(r, ident, 16);
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
        if (!claim_ok(id, ident)) {
            say("refused a server at %s that claims node %" PRIu32 " but is not the server that member is", c->peer,
                id);
            c->dead = 1;
            return;
        }
        c->id = id;
        memcpy(c->ident, ident, 16);
        tsdb_random(ns, sizeof ns);
        uint8_t zero[16] = {0};
        frame_begin(&C.fb, F_HELLO_ACK);
        put32(&C.fb, C.have_node ? C.id : 0);
        put(&C.fb, C.have_node ? C.uuid : zero, 16);
        put(&C.fb, C.ident, 16);
        put(&C.fb, ns, 32);
        memcpy(c->tr + c->trlen, C.fb.p + 5, C.fb.len - 5);
        c->trlen += C.fb.len - 5;
        hs_mac(C.secret, C.secret_len, "tsdb-cluster-s", c->tr, c->trlen, m);
        put(&C.fb, m, 32);
        frame_end(&C.fb);
        memcpy(c->tr + c->trlen, m, 32);
        c->trlen += 32;
        conn_send(c, 1);
        c->state = CS_WAIT_AUTH;
    } else if (c->state == CS_HELLO_SENT && type == F_HELLO_ACK) {
        if (r->len != 4 + 16 + 16 + 32 + 32) {
            c->dead = 1;
            return;
        }
        memcpy(c->tr + c->trlen, r->p, r->len - 32);
        c->trlen += r->len - 32;
        uint32_t id = get32(r);
        getn(r, uuid, 16);
        getn(r, ident, 16);
        getn(r, ns, 32);
        getn(r, m, 32);
        hs_mac(C.secret, C.secret_len, "tsdb-cluster-s", c->tr, c->trlen, want);
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
        if (!claim_ok(id, ident)) {
            say("%s claims node %" PRIu32 " but is not the server that member is; dropping the link", c->peer, id);
            c->dead = 1;
            return;
        }
        c->id = id;
        memcpy(c->ident, ident, 16);
        memcpy(c->tr + c->trlen, m, 32);
        c->trlen += 32;
        hs_mac(C.secret, C.secret_len, "tsdb-cluster-c", c->tr, c->trlen, m);
        frame_begin(&C.fb, F_AUTH);
        put(&C.fb, m, 32);
        frame_end(&C.fb);
        conn_send(c, 1);
        conn_ready(c);
    } else if (c->state == CS_WAIT_AUTH && type == F_AUTH) {
        getn(r, m, 32);
        hs_mac(C.secret, C.secret_len, "tsdb-cluster-c", c->tr, c->trlen, want);
        if (r->bad || !tsdb_ct_equal(m, want, 32)) {
            say("a peer at %s does not hold this cluster's secret; dropping it", c->peer[0] ? c->peer : "?");
            c->dead = 1;
            return;
        }
        conn_ready(c);
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
    case F_PROBE: on_probe(c, &r); break;
    case F_PROBE_REPLY: on_probe_reply(c, &r); break;
    default: c->dead = 1; break;
    }
}

static void conn_read(struct conn *c)
{
    size_t got_round = 0;
    for (;;) {
        int ready = c->state == CS_READY;
        buf_reserve(&c->rb, ready ? 65536 : 2 * HS_FRAME_MAX);
        size_t room = c->rb.cap - c->rb.len;
        if (!ready && room > 2 * HS_FRAME_MAX)
            room = 2 * HS_FRAME_MAX;
        ssize_t got = read(c->fd, c->rb.p + c->rb.len, room);
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
        if (got_round >= READ_ROUND || c->state != CS_READY)
            break; /* before AUTH one read a round: the frames are checked before more is taken in */
    }
    while (!c->dead && c->rb.len - c->rb.off >= 4) {
        uint32_t n = le32(c->rb.p + c->rb.off);
        int ready = c->state == CS_READY;
        if (n == 0 || n > (ready ? FRAME_MAX : HS_FRAME_MAX) || (ready && n < 1 + TAG)) {
            c->dead = 1;
            break;
        }
        if (c->rb.len - c->rb.off - 4 < n)
            break;
        const uint8_t *f = c->rb.p + c->rb.off + 4;
        if (ready) {
            uint8_t t[TAG];
            frame_tag(c->kin, c->seq_in++, f, n - TAG, t);
            if (!tsdb_ct_equal(t, f + n - TAG, TAG)) {
                say("a frame from node %" PRIu32 " (%s) does not authenticate; dropping the link", c->id,
                    c->peer[0] ? c->peer : "?");
                c->dead = 1;
                break;
            }
            on_frame(c, f[0], f + 1, n - 1 - TAG);
        } else {
            on_frame(c, f[0], f + 1, n - 1);
        }
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

/* a section of a heartbeat: u16 type, u32 length, the body. A reader skips types it does not know (the peer link's
 * rule: a newer server may send more; never a rule of the metadata entries, where an unknown kind stops the server). */
static size_t sec_begin(struct buf *b, uint16_t type)
{
    put8(b, (uint8_t)type);
    put8(b, (uint8_t)(type >> 8));
    size_t at = b->len;
    put32(b, 0);
    return at;
}

static void sec_end(struct buf *b, size_t at)
{
    uint32_t n = (uint32_t)(b->len - at - 4);
    for (int i = 0; i < 4; i++)
        b->p[at + (size_t)i] = (uint8_t)(n >> (8 * i));
}

/* the free bytes of the state directory (UINT64_MAX when it cannot be told) */
static uint64_t state_free_bytes(void)
{
    struct statvfs sv;
    if (statvfs(C.metadir, &sv) != 0)
        return UINT64_MAX;
    return (uint64_t)sv.f_bavail * (uint64_t)sv.f_frsize;
}

/* this server's flags (item 6): a witness holds no volumes; a node whose state directory is nearly full, whose core
 * was dropped or that carries a `retired` mark may not be a voter */
static uint8_t my_flags(void)
{
    uint8_t f = 0;
    if (!C.retired && !C.core_dropped && state_free_bytes() >= C.free_min)
        f |= META_MF_VOTER_OK;
    if (!C.witness)
        f |= META_MF_VOLUMES_OK;
    return f;
}

static void put_sections(struct buf *b)
{
    size_t at = sec_begin(b, HBS_NODE);
    put8(b, my_flags());
    put8(b, (uint8_t)(C.core_dropped || C.retired));
    put8(b, (uint8_t)(!C.rb_off));
    put64(b, state_free_bytes());
    sec_end(b, at);
    static struct cluster_report rep;
    memset(&rep, 0, sizeof rep);
    if (g_reporter)
        g_reporter(g_reporter_arg, &rep);
    for (unsigned i = 0; i < C.nk_vols && rep.nvols < CLUSTER_REPORT_VOLS; i++)
        rep.vols[rep.nvols++] = C.k_vols[i];
    for (unsigned i = 0; i < C.nk_doms && rep.ndoms < CLUSTER_REPORT_DOMS; i++)
        rep.doms[rep.ndoms++] = C.k_doms[i];
    if (rep.nvols > CLUSTER_REPORT_VOLS || rep.ndoms > CLUSTER_REPORT_DOMS)
        rep.nvols = rep.ndoms = 0;
    at = sec_begin(b, HBS_VOLUMES);
    put32(b, rep.nvols);
    for (uint32_t i = 0; i < rep.nvols; i++) {
        const struct cluster_vol_report *v = &rep.vols[i];
        put32(b, v->volume);
        put8(b, v->role);
        put8(b, v->health);
        put64(b, v->epoch);
        putstr(b, v->domain);
        put64(b, v->series_len);
        put64(b, v->states_len);
        put64(b, v->fenced_age_ms);
        put64(b, v->wal_seg);
        put64(b, v->wal_off);
    }
    sec_end(b, at);
    at = sec_begin(b, HBS_DOMAINS);
    put32(b, rep.ndoms);
    for (uint32_t i = 0; i < rep.ndoms; i++) {
        putstr(b, rep.doms[i].name);
        put8(b, rep.doms[i].state);
    }
    sec_end(b, at);
}

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
    put_sections(&C.fb);
    {
        /* this server's own record of itself: the same sections, read back as a peer would */
        struct rd me = {C.fb.p + 4 + 1, C.fb.len - 4 - 1, 0, 0};
        (void)get32(&me), (void)get64(&me), (void)get64(&me);
        char skip[META_ADDR_MAX];
        getstr(&me, skip, sizeof skip);
        getstr(&me, skip, sizeof skip);
        heard_sections(heard_get(C.id), &me, now);
    }
    frame_end(&C.fb);
    /* A core booted by a leader's message holds an empty configuration until its first install arrives; it must
     * still be heard, or the leader marks it down and removes it as a learner before the image reaches it, and a
     * member never heard is never added again. So the leader the core knows gets the heartbeat too. */
    uint32_t lead = C.r ? (uint32_t)raft_get_leader(C.r) : 0;
    for (unsigned i = 0; i < cfg.n; i++) {
        if (cfg.m[i].id == lead)
            lead = 0;
        if (cfg.m[i].learner || cfg.m[i].id == C.id)
            continue;
        struct conn *c = conn_to(cfg.m[i].id); /* builds no frame */
        if (c)
            conn_send(c, 0);
    }
    struct conn *c = lead && lead != C.id ? conn_to(lead) : NULL;
    if (c)
        conn_send(c, 0);
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
        else if (fresh && h && h->have_flags && inc >= minc && h->flags != m->flags &&
                 (!h->flags_ns || since(now, h->flags_ns) >= to)) {
            /* the flags its heartbeat reports differ from the record (item 6): voter_ok lost to a full disk or a
             * dropped core, volumes_ok back after a repair; the record is what the placement reads */
            struct meta_entry e;
            meta_entry_init(&e);
            h->flags_ns = now;
            meta_op_member_flags(&e, id, inc, h->flags);
            if (propose(&e, 0, 0, id) == ISO_OK)
                say("proposes node %" PRIu32 " flags %u (voter_ok %d, volumes_ok %d)", id, (unsigned)h->flags,
                    !!(h->flags & META_MF_VOTER_OK), !!(h->flags & META_MF_VOLUMES_OK));
        }
    }
}

static int in_config(const raft_config *c, uint32_t id)
{
    return raft_config_find(c, id) != NULL;
}

/* A voter is replaced when it has not answered the leader's Raft traffic for voter_replace_s (the core's own progress:
 * a node whose core was dropped still heartbeats, so heartbeats prove nothing), or when its flags have said for that
 * long that it is not voter_ok or its core was dropped (item 6). Never the leader itself. */
static int voter_gone(uint32_t id, uint64_t now, uint64_t rep, uint64_t lead)
{
    if (id == C.id)
        return 0;
    raft_progress p;
    int gone;
    if (raft_get_progress(C.r, id, &p)) {
        gone = p.silence_ns >= rep;
    } else {
        uint64_t last = heard_ns(id, now);
        gone = last ? since(now, last) >= rep : lead >= rep;
    }
    const struct heard *h = heard_get(id);
    if (h && h->bad_ns && since(now, h->bad_ns) >= rep)
        gone = 1;
    return gone;
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
        int gone = voter_gone(id, now, rep, lead);
        if (cfg.m[i].learner) {
            learners++;
            raft_progress p;
            const struct meta_member *lm = meta_member(&C.sm, id);
            if (last && since(now, last) < to && lm && (lm->flags & META_MF_VOTER_OK) &&
                raft_get_progress(C.r, id, &p) && p.match >= commit) {
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
            const struct heard *mh = heard_get(m->id);
            if (m->state != META_UP || in_config(&cfg, m->id) || !last || since(now, last) >= to || !m->peer[0] ||
                !(m->flags & META_MF_VOTER_OK) || (mh && mh->have_flags && !(mh->flags & META_MF_VOTER_OK)) ||
                (mh && mh->core_dropped))
                continue; /* only a node whose state directory is local, writable and has room (item 6) */
            int new_rack = 1, new_feed = 1;
            for (unsigned j = 0; j < cfg.n; j++) {
                const struct meta_member *v = meta_member(&C.sm, cfg.m[j].id);
                if (v && m->rack[0] && !strcmp(v->rack, m->rack))
                    new_rack = 0;
                if (v && m->power_feed[0] && !strcmp(v->power_feed, m->power_feed))
                    new_feed = 0;
            }
            int score = new_rack * 2 + new_feed; /* distinct racks first, then distinct power feeds (item 10) */
            if (!best || score > best_new_rack) {
                best = m;
                best_new_rack = score;
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
            if (cfg.m[i].learner || id == C.id || !voter_gone(id, now, rep, lead))
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
        struct meta_member first = {.id = C.id, .state = META_UP, .incarnation = C.incarnation, .flags = my_flags()};
        snprintf(first.addr, sizeof first.addr, "%s", C.adv);
        snprintf(first.peer, sizeof first.peer, "%s", C.padv);
        snprintf(first.rack, sizeof first.rack, "%s", C.rack);
        snprintf(first.power_feed, sizeof first.power_feed, "%s", C.feed);
        snprintf(first.bmc, sizeof first.bmc, "%s", C.bmc);
        memcpy(first.ident, C.ident, 16);
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_init(&e, C.uuid, &C.init_set, &first, C.init_mode, META_LEVEL);
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
    if (w) {
        /* a voter that hangs, or is cut off from the group but not from this server, keeps its link open and sends
         * nothing new: once another voter's commit stays ahead of the stream for a node timeout, watch another */
        const struct meta_state *ws = C.r ? &C.wsm : &C.sm;
        if (C.hint_commit <= ws->applied) {
            C.watch_behind_since = 0;
        } else if (!C.watch_behind_since) {
            C.watch_behind_since = now;
        } else if (since(now, C.watch_behind_since) >= (uint64_t)settings()->node_timeout_ms * MS) {
            say("the watch stream from %s stays at %" PRIu64 " while a voter reports commit %" PRIu64
                "; watching another voter", w->peer, ws->applied, C.hint_commit);
            w->dead = 1;
            C.watch_conn = 0;
            C.watch_behind_since = 0;
            C.watch_next_try = 0;
        }
        return;
    }
    if (now < C.watch_next_try)
        return;
    C.watch_next_try = now + 1000 * MS;
    C.watch_behind_since = 0;
    if (C.r && !C.wsm_on) {
        meta_free(&C.wsm); /* the empty state cluster_start made, or a freed one */
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
        dw_collect();
        if (C.r) {
            raft_tick(C.r, now);
            pump();
            if (C.r && !C.sync_since && raft_log_floor_due(C.log, raft_get_commit(C.r), now))
                sync_round();
            if (C.r)
                sync_stall_check(now);
        }
        copy_flush(now, 0);
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
        if (pfcap < n0 + 3) {
            pfcap = (n0 + 3) * 2;
            pf = realloc(pf, pfcap * sizeof *pf);
            if (!pf)
                oom();
        }
        pf[0] = (struct pollfd){C.lfd, POLLIN, 0};
        pf[1] = (struct pollfd){C.stop_pipe[0], POLLIN, 0};
        pf[2] = (struct pollfd){C.dw_pipe[0], POLLIN, 0};
        for (size_t i = 0; i < n0; i++) {
            struct conn *c = C.conns[i];
            short ev = POLLIN;
            if (c->state == CS_CONNECTING || c->wb.len > c->wb.off)
                ev |= POLLOUT;
            pf[i + 3] = (struct pollfd){c->fd, ev, 0};
        }
        int timeout = 50;
        if (C.r) {
            uint64_t d = raft_next_deadline(C.r);
            if (d != UINT64_MAX)
                timeout = d <= now ? 0 : (d - now) / MS + 1 < 50 ? (int)((d - now) / MS + 1) : 50;
        }
        int rc = poll(pf, n0 + 3, timeout);
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
            short re = pf[i + 3].revents;
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
    copy_flush(iso_now_ns(), 1);
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
        strlen(cfg->power_feed) >= META_FEED_MAX || strlen(cfg->bmc) >= META_BMC_MAX || !cfg->state_dir ||
        !cfg->state_dir[0] || strlen(cfg->state_dir) >= sizeof C.metadir) {
        fprintf(stderr, "tsdbd: cluster: an address, the rack, the power feed, the BMC or the state directory is "
                        "missing or too long\n");
        return -1;
    }
    snprintf(C.adv, sizeof C.adv, "%s", cfg->advertise);
    snprintf(C.padv, sizeof C.padv, "%s", cfg->peer_advertise);
    snprintf(C.rack, sizeof C.rack, "%s", cfg->rack);
    snprintf(C.feed, sizeof C.feed, "%s", cfg->power_feed);
    snprintf(C.bmc, sizeof C.bmc, "%s", cfg->bmc);
    C.witness = cfg->witness != 0;
    C.rb_off = cfg->read_barrier_off != 0;
    memcpy(C.secret, cfg->secret, cfg->secret_len);
    C.secret_len = cfg->secret_len;
    snprintf(C.metadir, sizeof C.metadir, "%s", cfg->state_dir);
    snprintf(C.logdir, sizeof C.logdir, "%s/raft", C.metadir);
    snprintf(C.nodefile, sizeof C.nodefile, "%s/NODE", C.metadir);
    snprintf(C.snapfile, sizeof C.snapfile, "%s/snapshot", C.metadir);
    snprintf(C.identfile, sizeof C.identfile, "%s/IDENT", C.metadir);
    snprintf(C.pendfile, sizeof C.pendfile, "%s/snapshot.install", C.metadir);
    snprintf(C.copyfile, sizeof C.copyfile, "%s/copy", C.metadir);
    snprintf(C.retiredfile, sizeof C.retiredfile, "%s/retired", C.metadir);
    snprintf(C.stallfile, sizeof C.stallfile, "%s/stall", C.metadir);
    test_knobs();
    meta_init(&C.sm);
    meta_init(&C.wsm);
    if (mkdir(C.metadir, 0755) == 0) {
        /* the parent of the state directory is made durable: a new directory's name lives there */
        char parent[sizeof C.metadir + 4];
        snprintf(parent, sizeof parent, "%s/..", C.metadir);
        if (fsync_dir(parent) != 0) {
            fprintf(stderr, "tsdbd: cluster: cannot sync the parent of %s: %s\n", C.metadir, strerror(errno));
            return -1;
        }
    } else if (errno != EEXIST) {
        fprintf(stderr, "tsdbd: cluster: cannot make %s: %s\n", C.metadir, strerror(errno));
        return -1;
    }
#ifdef __linux__
    {
        /* M11: the Raft log lives on a local disk only. A pool rollback reverts a log on NFS together with the volumes,
         * and a hung sync on a hard mount freezes the leader for a whole outage. */
        struct statfs sf;
        if (statfs(C.metadir, &sf) == 0 && (sf.f_type == 0x6969 /* NFS */ || sf.f_type == 0xFF534D42 /* CIFS */)) {
            fprintf(stderr, "tsdbd: cluster: --state-dir %s is on a network file system: the Raft log must be on a "
                            "local disk (design-ha-modes.md M11)\n", C.metadir);
            return -1;
        }
    }
#endif
    iso_log_quiet = getenv("TSDB_CLUSTER_RAFT_LOG") == NULL; /* the core's INFO and WARN lines, on request */
    char err[2048];
    if (ident_load(err, sizeof err) != 0) {
        fprintf(stderr, "tsdbd: cluster: %s\n", err);
        return -1;
    }
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
        if (exists(C.retiredfile)) {
            /* a fault of the Raft log ended this identity's voter role (item 3): no core from these files, ever */
            C.retired = C.core_dropped = 1;
            fprintf(stderr, "tsdbd: cluster: %s exists: this node does not boot a Raft core (clear the state "
                            "directory and node add it as a new member to make it a voter again)\n",
                    C.retiredfile);
        } else if ((exists(C.snapfile) || exists(C.logdir)) && core_boot(err, sizeof err) != 0) {
            fprintf(stderr, "tsdbd: cluster: cannot start the Raft core: %s\n", err);
            return -1;
        }
        if (!C.r && exists(C.copyfile)) {
            /* the copy of the last run, served stale until a watch stream confirms it (item 8) */
            uint8_t *img = NULL;
            size_t n = 0;
            meta_free(&C.sm);
            meta_init(&C.sm);
            if (load_image(C.copyfile, &C.sm, &img, &n, err, sizeof err) != 0) {
                fprintf(stderr, "tsdbd: cluster: %s\n", err);
                return -1;
            }
            free(img);
            C.copy_stale = 1;
        }
    }
    C.lfd = cfg->peer_fd;
    if (pipe2(C.stop_pipe, O_CLOEXEC | O_NONBLOCK) != 0) {
        fprintf(stderr, "tsdbd: cluster: pipe: %s\n", strerror(errno));
        return -1;
    }
    int fl = fcntl(C.lfd, F_GETFL);
    fcntl(C.lfd, F_SETFL, fl | O_NONBLOCK);
    /* said before the thread starts: from then on C belongs to it (an INIT may already be changing it) */
    if (C.have_node)
        fprintf(stderr, "tsdbd: cluster: node %" PRIu32 ", incarnation %" PRIu64 ", %s\n", C.id, C.incarnation,
                C.r ? "in the metadata group" : "following a watch stream");
    else
        fprintf(stderr, "tsdbd: cluster: in no cluster yet (tsdb cluster init, or node add from a member)\n");
    if (dw_start() != 0) {
        fprintf(stderr, "tsdbd: cluster: cannot start the disk worker\n");
        return -1;
    }
    if (pthread_create(&C.th, NULL, cluster_main, NULL) != 0) {
        fprintf(stderr, "tsdbd: cluster: cannot start its thread\n");
        return -1;
    }
    C.running = 1;
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
    dw_stop_thread();
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
    for (size_t i = 0; i < C.nheard; i++) {
        free(C.heard[i].vols);
        free(C.heard[i].doms);
    }
    free(C.heard);
    free(C.addrs);
    free(C.pend);
    buf_free(&C.fb);
    close(C.lfd);
    close(C.stop_pipe[0]);
    close(C.stop_pipe[1]);
}
