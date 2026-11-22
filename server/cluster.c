/* SPDX-License-Identifier: AGPL-3.0-only */
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
 * server that has known no leader for NOLEADER_PROBE_NS (one booted from its files at start: until its own state
 * holds it) watches as well (into a second state); if what it sees is a configuration without itself, it was removed
 * while away, and it drops its core (its files stay) and goes on as a watcher. So does a core whose own configuration
 * has not held it for NOTMEMBER_DROP_NS.
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
 * member heard again or an up member that restarted; one proposal per member at a time.
 *
 * Liveness and leases (§6.1, §7.1). This thread is not the loop that serves the volumes, so the serving loop stamps a
 * monotonic atomic every round and every tick (cluster_loop_stamp); while the stamp is older than
 * CLUSTER_LOOP_STALE_MS a server with cfg.loop_gated sends no heartbeat at all (nor records itself, nor extends its
 * own lease), so a hung I/O loop makes the member go down although this thread runs. The stamp is the I/O loop's
 * only: workers that all hang inside requests (a stuck NFS call, a lock) while the loop runs keep the member up. A heartbeat carries the
 * report sections: HBS_NODE (address, rack, flags), HBS_VOLUMES (led volumes with their epochs, failed volumes: a
 * volume that r3d's fail-stop stopped is LOCAL_FAULT in the next beat), HBS_DOMAINS, HBS_LEASE {u64 send_ns, the
 * sender's iso_now_ns, CLOCK_BOOTTIME} and HBS_UNREACH {u32 n, n x {u32 node, u32 age_ms}} (peers this server shares a
 * replicated volume with and has not heard on any data link for a second; at most CLUSTER_REPORT_UNREACH) and HBS_ISR
 * {u32 n, n x {u32 vol, u64 epoch, u64 isr_version (the record the request is based on), u64 commit, u32 nisr, nisr x
 * {u32 node, u64 rid}}} (the ISR changes the volumes it leads ask for, at most CLUSTER_REPORT_ISR; a new one
 * makes the next heartbeat go at the next duty round). The Raft leader proposes each as a conditional ISR_CHANGE
 * (propose_isr: only from the record's leader, only while the record's epoch and ISR version are the request's basis,
 * again after a second). Each
 * section is [u16 type][u32 len][body]; an unknown type is skipped. HB_ACK is {u32 leader, u64 term, u64 commit, u64
 * echo (the HB's send_ns), u8 granted, u32 lease_ms, u32 drift_ppm}. The Raft leader grants only while the core's
 * own check-quorum lease holds (raft_read_lease: no other Raft leader can exist yet) on the clock read as the HB is
 * handled (the core answers against the last time it was given, which after a stall of this thread is from before
 * it), shortened to the cluster's drift_ppm where the core was booted with less (core_lease_until), and records when
 * it last granted to each member; a server extends its lease to echo + lease_ms * (1 - drift) on its own clock, only
 * from an ack whose sender is the Raft leader it names, in a term no older than any it has heard. promote_safe_ns(X) = max(leader_since, last grant to X) + lease_ms *
 * (1 + drift) is the earliest time a promotion away from X is safe; a new Raft leader declares nobody down before it
 * (and before max(node_timeout, lease) of its own leadership). While its lease has run out, a server answers every
 * request on a cluster volume it leads UNAVAILABLE (WHY UNREACHABLE, retry_after 200 ms) and repld admits no write.
 * The leases of the data links (repld's heartbeats, L1) are about those links only: they never extend this lease.
 * Promotion (design-cluster.md 6.2): FENCE {u32 vol, u64 fence} goes from the Raft leader to each ISR member of a
 * volume whose leader is down, reported it LOCAL_FAULT or resigned; the member's driver saves the fence in REPL
 * (fdatasync) before it answers VOLMSG {u8 kind (1 FENCE_REPLY, 2 RESIGN), u32 vol, u64 epoch, durable, last_epoch,
 * rid, start} to the Raft leader, which proposes a conditional VOLUME_LEADER once every fresh member answered or after
 * 500 ms with one answer (promo_step). These frames ride on version 6 (s2b's bump), not on a version of their own.
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
 * Data links (server/peerlink.h, design-cluster.md C25, section 5.1): HELLO ends with a u8 link kind (0 metadata, 1
 * log, 2 bulk; protocol version 4). A HELLO of kind 1 or 2 gets the same checks (uuid, member id, identity) and is
 * then handed, socket and HELLO body, to peerlink_adopt: from there a thread of the data link answers the handshake
 * (under its own labels "tsdb-link-...", so no key is shared with a metadata link) and runs the connection. This
 * thread never reads or writes a data link after the hand-off.
 *
 * NODE_ADD (admin_node_add): PROBE to the new server (its identity), MEMBER_ADD committed with the next id and that
 * identity, then JOIN{uuid, id, identity}, on which the new server writes NODE. An id exists in the log before any
 * server holds it, so a leader that fails part way never lets the next one hand the same id to another server.
 */
#define _GNU_SOURCE
#include "cluster.h"

#include "meta.h"
#include "peerlink.h"
#include "raft/raft.h"
#include "raft/raft_log.h"
#include "raft/raft_wire.h"
#include "tsdb_crypto.h"
#include "tsdb_protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
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
#define CL_MAGIC PL_CL_MAGIC /* "TSDC" */
#define CL_VERSION PL_CL_VERSION /* 2: heartbeat sections, flags in PROBE_REPLY; 3: the R3 settings in INIT, VOLUME_ADD; 4: the link
                      * kind in HELLO (data links, peerlink.h); 5: HBS_LEASE, HBS_UNREACH and
                      * the lease fields of HB_ACK; peers of another
                      * version are refused (no mixed versions) */
#define RING 4096
#define RING_BYTES (64ull << 20) /* the ring's entries hold at most this many bytes (a user's blob is up to 1 MiB, and every
                                  * grant puts it again): the oldest go first, a watcher behind them gets a snapshot */
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
    F_FENCE,  /* Raft leader -> member: u32 volume, u64 fence (promotion, 6.2) */
    F_VOLMSG, /* member -> Raft leader (or the fence's sender): cluster_vmsg */
};

/* the sections of a heartbeat (after its address fields) */
enum { HBS_NODE = 1, HBS_VOLUMES = 2, HBS_DOMAINS = 3, HBS_LEASE = 4, HBS_UNREACH = 5, HBS_ISR = 6 };
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
    case CLUSTER_ERR_FEW: return "fewer live members take volumes than the volume has replicas";
    default: return meta_strerror(rc);
    }
}

size_t cluster_settings_encode(const struct meta_settings *s, int mode, uint8_t *out)
{
    const uint32_t v[META_SETTINGS_WORDS + 1] = {
        s->replication,   s->min_isr,          s->node_timeout_ms,    s->heartbeat_ms,   s->isr_lag_ms,
        s->out_after_s,   s->voter_replace_s,  s->voters,             s->volumes_per_server, s->failover,
        s->read_barrier,  s->lease_s,          s->lease_ms,           s->drift_ppm,      s->unanswered_ms,
        s->commit_interval_ms, s->repl_retain_mb, s->repl_retain_s,   s->idle_seal_ms,   (uint32_t)mode};
    for (int i = 0; i <= META_SETTINGS_WORDS; i++)
        for (int j = 0; j < 4; j++)
            out[i * 4 + j] = (uint8_t)(v[i] >> (8 * j));
    return 4 * (META_SETTINGS_WORDS + 1);
}

static int settings_decode(struct rd *r, struct meta_settings *s, int *mode)
{
    uint32_t v[META_SETTINGS_WORDS + 1];
    for (int i = 0; i <= META_SETTINGS_WORDS; i++)
        v[i] = get32(r);
    *s = (struct meta_settings){.replication = v[0], .min_isr = v[1], .node_timeout_ms = v[2], .heartbeat_ms = v[3],
                                .isr_lag_ms = v[4], .out_after_s = v[5], .voter_replace_s = v[6], .voters = v[7],
                                .volumes_per_server = v[8], .failover = (uint8_t)v[9],
                                .read_barrier = (uint8_t)v[10], .lease_s = v[11], .lease_ms = v[12],
                                .drift_ppm = v[13], .unanswered_ms = v[14], .commit_interval_ms = v[15],
                                .repl_retain_mb = v[16], .repl_retain_s = v[17], .idle_seal_ms = v[18]};
    *mode = (int)v[META_SETTINGS_WORDS];
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
            v->rid = get64(&r);
            v->open_gen = get64(&r);
            v->open_nonce = get64(&r);
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
        h->nunreach = get32(&r);
        if (h->nunreach > CLUSTER_REPORT_UNREACH)
            r.bad = 1; /* the array holds that many: a longer list is a malformed reply, never read past */
        if (r.bad)
            break;
        for (uint32_t j = 0; j < h->nunreach; j++) {
            h->unreach[j].node = get32(&r);
            h->unreach[j].age_ms = get32(&r);
        }
        h->answered_ms = get64(&r);
        h->lease_left_ms = (int64_t)get64(&r);
    }
    st->users_caught_up = r.bad ? 0 : get8(&r);
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
    put8(&b, PL_KIND_META);
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
    int handoff;          /* a data link's HELLO passed the checks: conn_read hands the socket to peerlink */
    uint8_t kind;         /* and its kind (PL_KIND_*): the peerlink it goes to */
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
    uint32_t nunreach;
    struct cluster_unreach unreach[CLUSTER_REPORT_UNREACH];
    uint64_t send_ns;     /* HBS_LEASE of its last heartbeat: its clock when it sent it (0: none) */
    uint64_t answered_ns; /* the Raft leader: when it last granted it a lease (its own clock; 0: never) */
    uint32_t nisr;        /* HBS_ISR of its last heartbeat: the ISR changes the volumes it leads ask for */
    struct cluster_isr_req isr[CLUSTER_REPORT_ISR];
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
    uint32_t volume; /* a VOLUME_ADD: its number + 1, the reply's body once committed (0: none) */
    struct upend *user; /* a USER_PUT or USER_DEL (malloc'ed; NULL: none) */
};

/* A user op's pending reply (S2-1): once applied it waits in C.uwait until every member reported an applied index at
 * or above it, or wait_ms passed (design-ha-modes.md :916) */
struct upend {
    char name[META_USER_NAME_MAX];
    uint32_t uid; /* a USER_DEL's: the dropped user's, read before it was proposed */
    uint32_t wait_ms;
};
struct uwait {
    uint64_t conn;
    uint32_t reqid, uid;
    uint64_t index, deadline;
};


/* ------------------------------------------------------------------ the disk worker's jobs */
#define DW_Q 8
enum { DJ_SYNC = 1, DJ_SNAP, DJ_COPY };
struct djob {
    int kind;
    uint64_t gen;
    raft_log_sync sync; /* DJ_SYNC: the descriptors a prepare handed out */
    uint8_t *img;       /* DJ_SNAP: the image, handed back in the result */
    size_t len;
    uint64_t index, term, rgen; /* rgen: DJ_COPY, the restore generation of the image */
    raft_config cfg;
    int fail_dirsync;
};
struct dres {
    int kind;
    uint64_t gen;
    int rc, err; /* DJ_SYNC: rc != 0 failed; DJ_SNAP: write_durable's result (-2: the directory sync failed) */
    uint64_t index, term, rgen;
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
    int copy_out;       /* a DJ_COPY job is on the disk worker */
    uint64_t copy_applied, copy_gen; /* what the copy file holds (applied index, restore generation): never written lower */
    struct meta_state cpy; /* a voter's persisted copy that is ahead of what its core has applied since the restart */
    int cpy_on;
    int fail_copy_dirsync, fail_pend_dirsync;
    int img_shared; /* an image this run saw (an install) says shared mode: a learner's own state is empty until then */
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
    atomic_int claim_bad; /* cluster_set_claim_ok(0) */
    int loop_gated;       /* cfg.loop_gated */
    int mutant;           /* TSDB_CLUSTER_MUTANT (tests only): enum cl_mutant */

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
    int probe_at_start; /* booted from its files at start and no leader seen since: watch at once (watch_duty) */
    uint8_t *snap; /* the image the core's log was last compacted to (sent as SNAPSHOT) */
    size_t snaplen;
    uint64_t snap_index;
    struct meta_state wsm; /* a core server's probe: the state a watch stream shows while it knows no leader */
    int wsm_on;

    struct ring_e ring[RING];
    uint64_t ring_first, ring_last; /* indexes held; 0 when none */
    uint64_t ring_bytes, ring_max;  /* the bytes of the entries held; their bound (RING_BYTES, test knob ring_bytes) */
    int ring_said;                  /* the bound dropped an entry once in this run (logged once) */

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
    uint32_t pend_volume; /* set around propose() by a VOLUME_ADD: its number + 1 (struct pend) */
    struct upend *pend_user; /* set around propose() by a USER_PUT or USER_DEL (struct pend owns it after) */
    struct uwait *uwait;     /* applied user ops waiting for the members (users_duty) */
    size_t nuwait, capuwait;
    uint64_t users_floor;    /* a Raft leader's commit index heard in this run (cluster_users_caught_up); 0: none */
    uint64_t uw_n, uw_max;   /* the users' count and highest version last pushed to the users watch */
    uint64_t uw_at;          /* the applied index whose users the watch has taken (the barrier waits for it) */
    int uw_said;

    uint64_t next_hb, next_manage, next_duty, watch_conn, watch_next_try;
    unsigned watch_rot;
    uint32_t hint_leader;
    uint64_t hint_term, hint_commit;
    uint64_t core_e;     /* the core's election_min_ns and max_clock_drift_ppm, as booted (core_lease_until) */
    uint32_t core_drift;
    uint64_t watch_behind_since; /* when the watch stream first stayed behind hint_commit; 0: it does not */

    /* TSDB_CLUSTER_TEST (tests only): snap_every=N, snap_retain=N, seg_bytes=N, fail_snap_dirsync=1,
     * hold_install_ms=N (installs dropped until N ms after the first arrived: a slow one), ring_bytes=N (the watch
     * ring's byte bound, C.ring_max) */
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
    int sync_again;      /* core thread: the core asked for a sync while one was out; run another when it returns */
    uint64_t sync_since; /* core thread: when the sync round now outstanding was handed to the worker; 0: none */
    int snap_out;        /* core thread: a snapshot write is outstanding */
    uint64_t stall_ns, xfer_for, xfer_next; /* a leader whose sync is outstanding this long hands leadership on */
    int hold_opens;      /* TSDB_CLUSTER_TEST hold_opens_file=1: no REPLICA_OPEN proposed while <state-dir>/holdopens is */
    int hold_isr;        /* TSDB_CLUSTER_TEST hold_isr_file=1: no ISR_CHANGE proposed while <state-dir>/holdisr is */
    int isr_replay;      /* TSDB_CLUSTER_TEST isr_replay=1: every ISR request proposed once is proposed again, with its
                          * old basis, after the record moved past it (a late duplicate: the apply must refuse it) */
    char holdisrfile[1100];
    int drop_fence;      /* TSDB_CLUSTER_TEST drop_fence_file=1: while <state-dir>/dropfence is, a FENCE received is dropped
                          * and a volume led here is passed on as led here whatever epoch the record moves to (test_cluster
                          * oneway: an old leader that hears nothing of its deposition, but still heartbeats and gets
                          * leases) */
    char dropfencefile[1100];
    int hold_reply;      /* TSDB_CLUSTER_TEST hold_reply_file=1: while <state-dir>/holdreply is, a copy of each FENCE_REPLY sent
                          * is kept; while <state-dir>/replayreply is, the copies are sent again every round (test_cluster
                          * oneway2: a late answer to an earlier fence of the volume must not count for the promotion now) */
    char holdreplyfile[1100], replayreplyfile[1100];
    int stall_file;      /* TSDB_CLUSTER_TEST sync_stall_file=1: the worker holds a sync while <state-dir>/stall exists */
    char stallfile[1100];
    char holdopensfile[1100];
    int hold_raft;       /* TSDB_CLUSTER_TEST hold_raft_file=1: the Raft core is neither ticked nor given a message while
                          * <state-dir>/holdraft exists (heartbeats go on: test_cluster users widens a catch-up with it) */
    char holdraftfile[1100];
    int users_probe;     /* TSDB_CLUSTER_TEST users_probe=1: a users watch that writes <state-dir>/userswatch, and drops a
                          * user named probe-drop through cluster_user_propose from another thread (test_cluster users) */
    unsigned sync_delay_ms; /* TSDB_CLUSTER_TEST sync_delay_ms=N: the worker waits N ms before each sync round */
    unsigned copy_hold;  /* TSDB_CLUSTER_TEST copy_hold=N: cc_hold_main holds a snapshot across N more */
    unsigned copy_noleader; /* TSDB_CLUSTER_TEST copy_noleader=V (test_cluster route): the copy shows volume V with no
                             * leader (R3 never leaves a record without one), so routing answers NO_OWNER for it */
    atomic_uint copy_drive; /* proposals of the settings still wanted by cc_hold_main (the Raft leader makes them) */
    pthread_t copy_hold_th;
    int fail_file;       /* TSDB_CLUSTER_TEST fail_sync_file=1: a sync fails with EIO while <state-dir>/failsync exists */
    char failfile[1100];

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
    C.ring_max = RING_BYTES;
    C.free_min = FREE_MIN_DEFAULT;
    C.stall_ns = SYNC_STALL_NS;
    for (const char *p = t; p && *p;) {
        unsigned long long v = 0;
        unsigned a = 0, b = 0, c = 0, ep = 0;
        char dn[META_DOMAIN_MAX] = "";
        if (sscanf(p, "claim_ok=%llu", &v) == 1)
            atomic_store(&C.claim_bad, v == 0);
        else if (sscanf(p, "free_min=%llu", &v) == 1)
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
        else if (sscanf(p, "ring_bytes=%llu", &v) == 1 && v)
            C.ring_max = v;
        else if (sscanf(p, "snap_retain=%llu", &v) == 1)
            C.snap_retain = v;
        else if (sscanf(p, "seg_bytes=%llu", &v) == 1)
            C.seg_bytes = v;
        else if (sscanf(p, "fail_snap_dirsync=%llu", &v) == 1)
            C.fail_snap_dirsync = v != 0;
        else if (sscanf(p, "fail_copy_dirsync=%llu", &v) == 1)
            C.fail_copy_dirsync = v != 0;
        else if (sscanf(p, "fail_pend_dirsync=%llu", &v) == 1)
            C.fail_pend_dirsync = v != 0;
        else if (sscanf(p, "hold_opens_file=%llu", &v) == 1)
            C.hold_opens = v != 0;
        else if (sscanf(p, "hold_raft_file=%llu", &v) == 1)
            C.hold_raft = v != 0;
        else if (sscanf(p, "users_probe=%llu", &v) == 1)
            C.users_probe = v != 0;
        else if (sscanf(p, "hold_isr_file=%llu", &v) == 1)
            C.hold_isr = v != 0;
        else if (sscanf(p, "drop_fence_file=%llu", &v) == 1)
            C.drop_fence = v != 0;
        else if (sscanf(p, "hold_reply_file=%llu", &v) == 1)
            C.hold_reply = v != 0;
        else if (sscanf(p, "isr_replay=%llu", &v) == 1)
            C.isr_replay = v != 0;
        else if (sscanf(p, "sync_stall_file=%llu", &v) == 1)
            C.stall_file = v != 0;
        else if (sscanf(p, "sync_delay_ms=%llu", &v) == 1)
            C.sync_delay_ms = (unsigned)v;
        else if (sscanf(p, "fail_sync_file=%llu", &v) == 1)
            C.fail_file = v != 0;
        else if (sscanf(p, "stall_ms=%llu", &v) == 1 && v)
            C.stall_ns = v * 1000000ull;
        else if (sscanf(p, "hold_install_ms=%llu", &v) == 1)
            C.hold_install_ns = v * 1000000ull;
        else if (sscanf(p, "copy_hold=%llu", &v) == 1)
            C.copy_hold = (unsigned)v;
        else if (sscanf(p, "copy_noleader=%llu", &v) == 1)
            C.copy_noleader = (unsigned)v;
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
    put8(&C.fb, PL_KIND_META);
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

/* ---- liveness and leases (the header's "Liveness and leases") */
/* Mutants (TSDB_CLUSTER_MUTANT, tests only): `leaseanyvoter` lets any voter's HB_ACK extend the lease, `nodrift` takes
 * lease_ms without the drift margin both ways, `stampignored` heartbeats whatever the serving loop's stamp says,
 * `noquorum` grants without the core's lease, `nocoreclock` takes the core's answer on its own clock (the code before
 * the review), `nopromotewait` declares a member down after node_timeout without promote_safe_ns. The copy's
 * (cluster_copy_get, test_cluster topology/copyhold/links): `snapfreeheld` frees a snapshot one reference early,
 * `rolesswap` makes a replica outside the ISR STANDBY, `selfone` says node 1 for this server, `movedpeeraddr` gives the
 * leader's peer address for its client address, `kindall` hands a data link of any kind to whatever peerlink is set. */
enum cl_mutant {
    CLM_NONE = 0,
    CLM_LEASEANYVOTER = 1,
    CLM_NODRIFT = 2,
    CLM_STAMPIGNORED = 3,
    CLM_NOQUORUM = 4,
    CLM_NOCORECLOCK = 5,
    CLM_NOPROMOTEWAIT = 6,
    CLM_SHRINKNOCOND = 7, /* an ISR request is proposed on the record's current epoch and ISR version, not its own basis */
    CLM_SNAPFREEHELD = 8,
    CLM_ROLESSWAP = 9,
    CLM_SELFONE = 10,
    CLM_MOVEDPEERADDR = 11,
    CLM_KINDALL = 12,
    CLM_CCNOWAIT = 13,  /* cc_swap drops the old snapshot at once, readers taking a reference or not */
    CLM_CCONESLOT = 14, /* cc_swap waits until the current parity's counter is 0: one counter for every reader, which
                         * continuous readers never leave at 0 (the cluster thread starves) */
    CLM_OWNERSELF = 15, /* cluster_vol_owner says 1 (elsewhere) for a leader that is this server */
    CLM_DROPNOTCOMMITTED = 16, /* a user op is answered OK when proposed, before it commits */
    CLM_CAUGHTUPEARLY = 17,    /* cluster_users_caught_up says 1 from the start */
    CLM_FLOORNOAPPLIED = 18,   /* cluster_users_caught_up says 1 once a floor is known, before the copy applied it */
    CLM_LEASESTALE = 19,   /* a node that reports itself the writer of a volume another leads still gets a lease */
    CLM_UPWAITS = 20,      /* a new Raft leader proposes a restarted member up only after node_timeout */
    CLM_CUTSHRINKS = 21,  /* a leader cut from its replicas shrinks the ISR to itself instead of stepping aside */
    CLM_PROBELATE = 22,   /* a core restarted from its files watches only after NOLEADER_PROBE_NS without a leader */
    CLM_PROBEINIT = 23,   /* a watched state below the core's own log end (the INIT entry replayed) may drop the core */
    CLM_DEPOSEDSTUCK = 24, /* a record's leader deposed by a fence no promotion followed is never taken as resigned */
    CLM_FENCELEASE = 25,   /* an old leader whose fence answer a promotion waits for still gets leases (review R1) */
    CLM_OLDPASS = 26,      /* a promotion goes ahead without the old leader's fence answer or the end of its lease (R3) */
    CLM_MOVEGIVEUP = 27,   /* a move gives up at PROMO_MOVE_NS, and any promotion at PROMO_GIVEUP_NS, though a replica
                            * answered and only the old leader's lease is awaited */
    CLM_ANYEPOCH = 28,     /* a FENCE_REPLY of any epoch counts for the promotion of its volume (review R2) */
    CLM_NOCUTHOLD = 29,    /* a shrink below 2 goes ahead before the member it drops reported the cut (review R13) */
    CLM_FORCEUP = 30       /* force-leader goes ahead while an ISR member is up (review R6) */
};
static int cl_mutant(void)
{
    static atomic_int m = -1; /* parsed once; a race parses the same string twice */
    int v = atomic_load_explicit(&m, memory_order_relaxed);
    if (v < 0) {
        const char *e = getenv("TSDB_CLUSTER_MUTANT");
        v = !e                            ? CLM_NONE
            : !strcmp(e, "leaseanyvoter") ? CLM_LEASEANYVOTER
            : !strcmp(e, "nodrift")       ? CLM_NODRIFT
            : !strcmp(e, "stampignored")  ? CLM_STAMPIGNORED
            : !strcmp(e, "noquorum")      ? CLM_NOQUORUM
            : !strcmp(e, "nocoreclock")   ? CLM_NOCORECLOCK
            : !strcmp(e, "nopromotewait") ? CLM_NOPROMOTEWAIT
            : !strcmp(e, "shrinknocond")  ? CLM_SHRINKNOCOND
            : !strcmp(e, "snapfreeheld")  ? CLM_SNAPFREEHELD
            : !strcmp(e, "rolesswap")     ? CLM_ROLESSWAP
            : !strcmp(e, "selfone")       ? CLM_SELFONE
            : !strcmp(e, "ccnowait")      ? CLM_CCNOWAIT
            : !strcmp(e, "cconeslot")     ? CLM_CCONESLOT
            : !strcmp(e, "ownerself")     ? CLM_OWNERSELF
            : !strcmp(e, "movedpeeraddr") ? CLM_MOVEDPEERADDR
            : !strcmp(e, "kindall")       ? CLM_KINDALL
            : !strcmp(e, "dropnotcommitted") ? CLM_DROPNOTCOMMITTED
            : !strcmp(e, "caughtupearly") ? CLM_CAUGHTUPEARLY
            : !strcmp(e, "floornoapplied") ? CLM_FLOORNOAPPLIED
            : !strcmp(e, "leasestale")    ? CLM_LEASESTALE
            : !strcmp(e, "upwaits")       ? CLM_UPWAITS
            : !strcmp(e, "cutshrinks")    ? CLM_CUTSHRINKS
            : !strcmp(e, "probelate")     ? CLM_PROBELATE
            : !strcmp(e, "probeinit")     ? CLM_PROBEINIT
            : !strcmp(e, "deposedstuck")  ? CLM_DEPOSEDSTUCK
            : !strcmp(e, "fencelease")    ? CLM_FENCELEASE
            : !strcmp(e, "oldpass")       ? CLM_OLDPASS
            : !strcmp(e, "movegiveup")    ? CLM_MOVEGIVEUP
            : !strcmp(e, "anyepoch")      ? CLM_ANYEPOCH
            : !strcmp(e, "nocuthold")     ? CLM_NOCUTHOLD
            : !strcmp(e, "forceup")       ? CLM_FORCEUP
                                          : CLM_NONE;
        atomic_store_explicit(&m, v, memory_order_relaxed);
    }
    return v;
}

/* the cluster thread's clock (CLOCK_BOOTTIME: a suspend counts), for the stamp and the lease on any thread */
static uint64_t mono_ns(void) { return iso_now_ns(); }

static _Atomic uint64_t g_stamp_ns; /* the serving loop's last stamp (0: never) */
static _Atomic uint64_t g_lease_ns; /* this server's read lease runs until this (mono_ns; 0: none) */
static atomic_int g_beat_soon;      /* cluster_beat_soon: send the next heartbeat at the next duty round */

void cluster_beat_soon(void) { atomic_store_explicit(&g_beat_soon, 1, memory_order_relaxed); }

void cluster_loop_stamp(void) { atomic_store_explicit(&g_stamp_ns, mono_ns(), memory_order_relaxed); }

static _Atomic uint64_t g_lease_gen; /* + 1 whenever a lease begins after the last one had ended (lease_extend) */

int cluster_lease_valid(void) { return mono_ns() < atomic_load_explicit(&g_lease_ns, memory_order_acquire); }

uint64_t cluster_lease_gen(void) { return atomic_load_explicit(&g_lease_gen, memory_order_acquire); }

int cluster_lease_still(uint64_t gen)
{
    uint64_t until = atomic_load_explicit(&g_lease_ns, memory_order_acquire); /* sees the gen of the extension */
    return mono_ns() < until && atomic_load_explicit(&g_lease_gen, memory_order_acquire) == gen;
}

uint64_t cluster_lease_end(uint64_t send_ns, uint32_t lease_ms, uint32_t drift_ppm)
{
    uint64_t l = (uint64_t)lease_ms * MS;
    if (cl_mutant() != CLM_NODRIFT)
        l -= (uint64_t)lease_ms * drift_ppm; /* lease_ms * 1e6 ns * drift_ppm / 1e6 */
    return send_ns + l;
}

uint64_t cluster_lease_safe(uint64_t answer_ns, uint32_t lease_ms, uint32_t drift_ppm)
{
    uint64_t l = (uint64_t)lease_ms * MS;
    if (cl_mutant() != CLM_NODRIFT)
        l += (uint64_t)lease_ms * drift_ppm;
    return answer_ns + l;
}

/* The lease runs to `until` unless it already runs longer (several answers may cross; it never shrinks). */
static void lease_extend(uint64_t until)
{
    uint64_t cur = atomic_load_explicit(&g_lease_ns, memory_order_relaxed);
    while (until > cur) {
        if (cur <= mono_ns()) /* it had ended: a new lease, not the old one continued (a second bump is harmless) */
            atomic_fetch_add_explicit(&g_lease_gen, 1, memory_order_release);
        if (atomic_compare_exchange_weak_explicit(&g_lease_ns, &cur, until, memory_order_release,
                                                  memory_order_relaxed))
            break;
    }
}

/* E(1-d)/(1+d), exactly as raft_new computes the core's lease */
static uint64_t core_lease_ns(uint64_t e, uint64_t d) { return e - e * 2 * d / (1000000 + d); }

/* How long this server, as the Raft leader, holds the core's check-quorum lease (0: not at all). The core answers
 * against the last time it was given, which is from before any stall of this thread (a pause, a long poll), so it is
 * given `now` first and its end is compared with `now` here as well. A core booted before INIT's settings were applied
 * (a joined node: on_raft -> core_boot) took the default drift; its lease is shortened to the cluster's drift_ppm, as
 * if booted with it (the lease starts at the same round either way). */
static uint64_t core_lease_until(uint64_t now)
{
    raft_lease l;
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER)
        return 0;
    if (cl_mutant() == CLM_NOQUORUM)
        return UINT64_MAX;
    if (cl_mutant() == CLM_NOCORECLOCK)
        return raft_read_lease(C.r, &l) ? l.lease_until : 0;
    raft_clock(C.r, now);
    if (!raft_read_lease(C.r, &l))
        return 0;
    uint64_t until = l.lease_until, d = settings()->drift_ppm;
    if (until != UINT64_MAX && d > C.core_drift && d < 1000000) {
        uint64_t cut = core_lease_ns(C.core_e, C.core_drift) - core_lease_ns(C.core_e, d);
        until = until > cut ? until - cut : 0;
    }
    return now < until ? until : 0;
}

/* Does the serving loop run? (without the gate: always) */
static int loop_alive(uint64_t now)
{
    if (!C.loop_gated || cl_mutant() == CLM_STAMPIGNORED)
        return 1;
    uint64_t st = atomic_load_explicit(&g_stamp_ns, memory_order_relaxed);
    return st && since(now, st) <= (uint64_t)CLUSTER_LOOP_STALE_MS * MS;
}

void cluster_set_claim_ok(int ok)
{
    atomic_store(&C.claim_bad, !ok);
}

void cluster_set_reporter(cluster_report_fn fn, void *arg)
{
    g_reporter = fn;
    g_reporter_arg = arg;
}

static cluster_vol_watch_fn g_vwatch;
static void *g_vwatch_arg;
/* where data links go, one peerlink per kind (cluster_set_peerlink_kind; [PL_KIND_META] stays NULL) */
static _Atomic(struct peerlink *) g_pl_a[CLUSTER_PL_KINDS];

static struct peerlink *pl_of_kind(unsigned kind)
{
    if (cl_mutant() == CLM_KINDALL) /* the acceptor takes any kind: to the first peerlink there is */
        for (unsigned k = 1; k < CLUSTER_PL_KINDS; k++)
            if (atomic_load(&g_pl_a[k]))
                return atomic_load(&g_pl_a[k]);
    return kind != PL_KIND_META && kind < CLUSTER_PL_KINDS ? atomic_load(&g_pl_a[kind]) : NULL;
}

int cluster_set_peerlink_kind(int kind, struct peerlink *pl)
{
    if (kind <= PL_KIND_META || kind >= CLUSTER_PL_KINDS)
        return -1;
    atomic_store(&g_pl_a[kind], pl);
    return 0;
}

void cluster_set_peerlink(struct peerlink *pl)
{
    cluster_set_peerlink_kind(PL_KIND_LOG, pl);
    cluster_set_peerlink_kind(PL_KIND_BULK, pl);
}

/* ---- the metadata copy as one snapshot (cluster.h "the whole metadata copy")
 * One block per snapshot: the header, the volumes, the members. Readers take a reference without a lock: a reader
 * counts itself in cc_getting[q & 1], q being cc_phase (it checks that cc_phase did not move meanwhile, else it counts
 * itself again under the new parity), loads cc_cur, takes its reference and counts itself out. The cluster thread
 * never waits for readers (a wait for a counter that continuous readers never leave at 0 starved it, and any wait
 * lasts as long as a reader preempted inside its few instructions: test_cluster ccstress, mutant cconeslot). A
 * swapped-out snapshot goes on the retired list with the phase rp of its swap, and its published reference is dropped
 * later (cc_reclaim, every round of the cluster loop) once no reader can still be about to take one:
 * - a reader that loaded it registered at a phase q <= rp (its check of cc_phase came before its load, which came
 *   before the swap);
 * - cc_phase moves from p to p + 1 only while the counter of the parity it moves into, (p + 1) & 1, is 0, so a reader
 *   of phase q is out before the phase reaches q + 2: once the phase is rp + 1, a reader of phase rp - 1 is out, and
 *   once the counter of parity rp & 1 has been 0 after that move, so is every reader of phase rp;
 * so a retired snapshot is dropped when cc_phase >= rp + 2, or at rp + 1 with cc_getting[rp & 1] at 0. Only the
 * cluster thread (and cluster_stop after it, which does wait) swaps, reclaims and moves the phase. The checks of
 * cc_unref are always on: a count below zero or a put of a freed snapshot (poisoned by cc_unref) aborts with a line. */
#define CC_MAGIC 0x43435059u /* "YPCC" */
struct cc_snap {
    struct cluster_copy pub;
    atomic_long refs;       /* the cluster thread's while it is published, and one per reader */
    _Atomic uint32_t magic; /* CC_MAGIC until freed */
    size_t bytes;
    uint64_t sum; /* of the content, for the hold check (copy_hold) */
    struct cc_snap *next; /* on the retired list */
    unsigned long rp;     /* cc_phase when it was swapped out */
};
static _Atomic(struct cc_snap *) cc_cur;
static atomic_ulong cc_phase;
static atomic_long cc_getting[2];
static struct cc_snap *cc_retired; /* swapped out, the published reference still held; the swapping thread's */
static unsigned long cc_nretired, cc_nretired_max;
static atomic_ulong cc_builds; /* snapshots published since the start (the hold check counts them) */

static uint64_t cc_sum(const struct cc_snap *s)
{
    uint64_t h = 1469598103934665603ull;
    const uint8_t *p = (const uint8_t *)(s + 1);
    for (size_t i = 0; i < s->bytes - sizeof *s; i++)
        h = (h ^ p[i]) * 1099511628211ull;
    return h ^ s->pub.applied ^ (s->pub.applied_term << 1) ^ ((uint64_t)s->pub.nmembers << 32) ^ s->pub.nvolumes;
}

static void cc_unref(struct cc_snap *s)
{
    if (atomic_load(&s->magic) != CC_MAGIC) {
        say("copy: a snapshot was put after it was freed");
        abort();
    }
    long left = atomic_fetch_sub(&s->refs, 1) - 1;
    if (left < 0) {
        say("copy: a snapshot's reference count went below zero");
        abort();
    }
    if (left == 0 || (left == 1 && cl_mutant() == CLM_SNAPFREEHELD)) {
        memset(s, 0xdb, s->bytes); /* a reader that still held it sees the poison, not stale data */
        free(s);
    }
}

const struct cluster_copy *cluster_copy_get(void)
{
    unsigned long p;
    for (;;) {
        p = atomic_load(&cc_phase);
        atomic_fetch_add(&cc_getting[p & 1], 1);
        if (atomic_load(&cc_phase) == p)
            break;
        atomic_fetch_sub(&cc_getting[p & 1], 1); /* a swap flipped in between: count under the new parity */
    }
    struct cc_snap *s = atomic_load(&cc_cur);
    if (s)
        atomic_fetch_add(&s->refs, 1);
    atomic_fetch_sub(&cc_getting[p & 1], 1);
    return s ? &s->pub : NULL;
}

unsigned long cluster_copy_builds(void)
{
    return atomic_load_explicit(&cc_builds, memory_order_acquire);
}

void cluster_copy_put(const struct cluster_copy *c)
{
    if (c)
        cc_unref((struct cc_snap *)(void *)c); /* pub is the first member */
}

/* the swapping thread: drop the published reference of every retired snapshot no reader can still be taking one of,
 * and move the phase on while snapshots of the current one wait (never waits itself); 1 when none is left */
static int cc_reclaim(void)
{
    for (int pass = 0; pass < 2; pass++) {
        unsigned long p = atomic_load(&cc_phase);
        int drained = p > 0 && atomic_load(&cc_getting[(p - 1) & 1]) == 0, current = 0;
        for (struct cc_snap **pp = &cc_retired; *pp;) {
            struct cc_snap *x = *pp;
            if (x->rp + 2 <= p || (x->rp + 1 == p && drained)) {
                *pp = x->next;
                cc_nretired--;
                cc_unref(x);
            } else {
                current |= x->rp == p;
                pp = &x->next;
            }
        }
        if (!current || atomic_load(&cc_getting[(p + 1) & 1]) != 0)
            break;
        atomic_store(&cc_phase, p + 1); /* the readers of phase p - 1 are out: their parity can be p + 1's */
    }
    return cc_retired == NULL;
}

/* the cluster thread (and cluster_stop): s becomes the current snapshot (NULL: none); the old one is retired */
static void cc_swap(struct cc_snap *s)
{
    struct cc_snap *old = atomic_exchange(&cc_cur, s);
    if (!old)
        return;
    if (cl_mutant() == CLM_CCNOWAIT) {
        cc_unref(old);
        return;
    }
    if (cl_mutant() == CLM_CCONESLOT) { /* the old protocol: wait for the one counter every reader uses */
        while (atomic_load(&cc_getting[atomic_load(&cc_phase) & 1]))
            sched_yield();
        cc_unref(old);
        return;
    }
    old->rp = atomic_load(&cc_phase);
    old->next = cc_retired;
    cc_retired = old;
    if (++cc_nretired > cc_nretired_max)
        cc_nretired_max = cc_nretired;
    cc_reclaim();
}

/* cluster_stop and the stress test: none current, and every retired one dropped (waits for the readers in flight) */
static void cc_clear(void)
{
    cc_swap(NULL);
    while (!cc_reclaim())
        sched_yield();
}

/* After whatever moved the copy (an applied batch, an installed snapshot, a swap to the state file's copy, a
 * restore): a new snapshot of C.sm. The cluster thread only. */
static void copy_publish(void)
{
    if (cc_retired)
        cc_reclaim();
    if (!C.sm.initialised)
        return;
    uint32_t self = cl_mutant() == CLM_SELFONE ? 1 : C.have_node ? C.id : 0;
    struct cc_snap *cur = atomic_load(&cc_cur); /* ours: the published reference is the cluster thread's */
    if (cur && cur->pub.applied == C.sm.applied && cur->pub.applied_term == C.sm.applied_term &&
        cur->pub.restore_gen == C.sm.restore_gen && cur->pub.self == self)
        return;
    uint32_t nv = C.sm.nvolumes, nm = C.sm.nmembers;
    size_t bytes = sizeof(struct cc_snap) + (size_t)nv * sizeof(struct cluster_copy_vol) +
                   (size_t)nm * sizeof(struct cluster_copy_member);
    struct cc_snap *s = calloc(1, bytes);
    if (!s)
        oom();
    struct cluster_copy_vol *vols = (struct cluster_copy_vol *)(void *)(s + 1);
    struct cluster_copy_member *mem = (struct cluster_copy_member *)(void *)(vols + nv);
    for (uint32_t i = 0; i < nv; i++) {
        const struct meta_volume *v = &C.sm.volumes[i];
        struct cluster_copy_vol *o = &vols[i];
        o->number = v->number;
        snprintf(o->name, sizeof o->name, "%s", v->name);
        o->kind = v->kind;
        o->leader = C.copy_noleader && v->number == C.copy_noleader ? 0 : v->leader;
        o->epoch = v->epoch;
        o->nrep = v->nrep < META_REPLICAS_MAX ? v->nrep : META_REPLICAS_MAX;
        for (uint32_t k = 0; k < o->nrep; k++)
            o->rep[k] = (struct cluster_copy_rep){v->rep[k].node, v->rep[k].learner};
        o->nisr = v->nisr < META_REPLICAS_MAX ? v->nisr : META_REPLICAS_MAX;
        memcpy(o->isr, v->isr, o->nisr * sizeof o->isr[0]);
    }
    for (uint32_t i = 0; i < nm; i++) {
        const struct meta_member *m = &C.sm.members[i];
        mem[i].id = m->id;
        mem[i].state = m->state;
        snprintf(mem[i].addr, sizeof mem[i].addr, "%s", m->addr);
        snprintf(mem[i].peer, sizeof mem[i].peer, "%s", m->peer);
    }
    s->pub = (struct cluster_copy){.applied = C.sm.applied, .applied_term = C.sm.applied_term,
                                   .restore_gen = C.sm.restore_gen, .self = self, .nmembers = nm, .nvolumes = nv,
                                   .members = mem, .volumes = vols};
    s->bytes = bytes;
    atomic_init(&s->refs, 1);
    atomic_init(&s->magic, CC_MAGIC);
    if (C.copy_hold)
        s->sum = cc_sum(s); /* only the hold check reads it: not paid on every publish */
    cc_swap(s);
    atomic_fetch_add_explicit(&cc_builds, 1, memory_order_release);
}

const struct cluster_copy_member *cluster_copy_member(const struct cluster_copy *c, uint32_t id)
{
    for (uint32_t i = 0; c && i < c->nmembers; i++)
        if (c->members[i].id == id)
            return &c->members[i];
    return NULL;
}

const struct cluster_copy_vol *cluster_copy_vol(const struct cluster_copy *c, uint32_t number)
{
    uint32_t lo = 0, hi = c ? c->nvolumes : 0;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (c->volumes[mid].number < number)
            lo = mid + 1;
        else
            hi = mid;
    }
    return c && lo < c->nvolumes && c->volumes[lo].number == number ? &c->volumes[lo] : NULL;
}

const struct cluster_copy_vol *cluster_copy_vol_named(const struct cluster_copy *c, const char *name, size_t n)
{
    for (uint32_t i = 0; c && n && n < META_NAME_MAX && i < c->nvolumes; i++)
        if (memcmp(c->volumes[i].name, name, n) == 0 && c->volumes[i].name[n] == 0)
            return &c->volumes[i];
    return NULL;
}

int cluster_copy_role(const struct cluster_copy_vol *v, uint32_t node)
{
    if (!v || !node)
        return TSDB_ROLE_NONE;
    if (v->leader == node)
        return TSDB_ROLE_OWNER;
    for (uint32_t k = 0; k < v->nrep; k++) {
        if (v->rep[k].node != node)
            continue;
        for (uint32_t j = 0; j < v->nisr; j++)
            if (v->isr[j] == node)
                return TSDB_ROLE_STANDBY;
        return cl_mutant() == CLM_ROLESSWAP ? TSDB_ROLE_STANDBY : TSDB_ROLE_FOLLOWER;
    }
    return TSDB_ROLE_NONE;
}

int cluster_vol_owner(uint32_t vol, uint32_t *node, uint64_t *epoch, char *addr, size_t cap)
{
    const struct cluster_copy *c = cluster_copy_get();
    const struct cluster_copy_vol *v = cluster_copy_vol(c, vol);
    const struct cluster_copy_member *m = v && v->leader ? cluster_copy_member(c, v->leader) : NULL;
    int ok = m && m->state == META_UP ? (c->self && m->id == c->self && cl_mutant() != CLM_OWNERSELF ? 2 : 1) : 0;
    if (ok) {
        *node = m->id;
        *epoch = v->epoch;
        if (addr && cap)
            snprintf(addr, cap, "%s", cl_mutant() == CLM_MOVEDPEERADDR ? m->peer : m->addr);
    }
    cluster_copy_put(c);
    return ok;
}

/* TSDB_CLUSTER_TEST copy_hold=N (test_cluster copyhold): a reader that holds the first snapshot while N more are
 * published (the Raft leader proposes its settings again, one entry a duty round, while copy_drive is above 0), then
 * checks that what it held is unchanged and says so. */
static void *cc_hold_main(void *arg)
{
    unsigned want = (unsigned)(uintptr_t)arg;
    const struct cluster_copy *h = NULL;
    while (!atomic_load(&C.stop) && !(h = cluster_copy_get()))
        usleep(20000);
    if (!h)
        return NULL;
    const struct cc_snap *hs = (const struct cc_snap *)(const void *)h;
    uint64_t sum = hs->sum, applied = h->applied;
    unsigned long b0 = atomic_load(&cc_builds);
    say("copy hold: holds the snapshot of applied %" PRIu64, applied);
    atomic_store(&C.copy_drive, want + 5);
    while (!atomic_load(&C.stop) && atomic_load(&cc_builds) - b0 < want)
        usleep(5000);
    atomic_store(&C.copy_drive, 0);
    unsigned long seen = atomic_load(&cc_builds) - b0;
    if (seen < want)
        ; /* stopped first: no verdict */
    else if (atomic_load(&hs->magic) == CC_MAGIC && hs->sum == sum && cc_sum(hs) == sum && h->applied == applied)
        say("copy hold: intact after %lu batches", seen);
    else
        say("copy hold: FAILED: the held snapshot changed after %lu batches", seen);
    cluster_copy_put(h);
    return NULL;
}

/* Tests (test_cluster ccstress): the get/put/swap protocol under continuous readers, in-process with no cluster
 * running. `readers` threads loop get/put and check what they hold; this thread publishes `publishes` synthetic
 * snapshots through cc_swap. Readers stop at the deadline, so a starved swap returns then; it fails when the writer
 * did not get through (mutant cconeslot) or a reader saw a snapshot change or freed under it (ccnowait; ASan also
 * reports the use after free). */
struct ccst {
    atomic_int bad;
    atomic_ulong gets;
    _Atomic uint64_t deadline_ns;
};
#define CCST_VOLS 4
static uint64_t ccst_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
static void *ccst_reader(void *arg)
{
    struct ccst *t = arg;
    unsigned long n = 0;
    while (ccst_now() < atomic_load_explicit(&t->deadline_ns, memory_order_relaxed)) {
        const struct cluster_copy *c = cluster_copy_get();
        if (!c)
            continue;
        const struct cc_snap *s = (const struct cc_snap *)(const void *)c;
        int ok = atomic_load(&s->magic) == CC_MAGIC && c->nvolumes == CCST_VOLS;
        for (uint32_t i = 0; ok && i < CCST_VOLS; i++)
            ok = c->volumes[i].epoch == c->applied && c->volumes[i].number == i + 1;
        if (!ok || atomic_load(&s->magic) != CC_MAGIC) {
            atomic_store(&t->bad, 1); /* no put: a freed one would abort before the test says why */
            break;
        }
        cluster_copy_put(c);
        n++;
    }
    atomic_fetch_add(&t->gets, n);
    return NULL;
}

int cluster_copy_stress(unsigned readers, unsigned long publishes, unsigned ms, char *why, size_t wn)
{
    if (atomic_load(&cc_cur)) {
        snprintf(why, wn, "a copy is published: the cluster runs in this process");
        return -1;
    }
    uint64_t t0 = ccst_now();
    struct ccst t;
    atomic_init(&t.bad, 0);
    atomic_init(&t.gets, 0);
    atomic_init(&t.deadline_ns, ccst_now() + (uint64_t)ms * 1000000ull);
    pthread_t *th = calloc(readers ? readers : 1, sizeof *th);
    if (!th)
        oom();
    unsigned started = 0;
    while (started < readers && pthread_create(&th[started], NULL, ccst_reader, &t) == 0)
        started++;
    unsigned long done = 0;
    uint64_t worst = 0;
    while (done < publishes && ccst_now() < atomic_load(&t.deadline_ns)) {
        size_t bytes = sizeof(struct cc_snap) + CCST_VOLS * sizeof(struct cluster_copy_vol);
        struct cc_snap *s = calloc(1, bytes);
        if (!s)
            oom();
        struct cluster_copy_vol *vols = (struct cluster_copy_vol *)(void *)(s + 1);
        for (uint32_t i = 0; i < CCST_VOLS; i++)
            vols[i] = (struct cluster_copy_vol){.number = i + 1, .epoch = done + 1};
        s->pub = (struct cluster_copy){.applied = done + 1, .nvolumes = CCST_VOLS, .volumes = vols};
        s->bytes = bytes;
        atomic_init(&s->refs, 1);
        atomic_init(&s->magic, CC_MAGIC);
        uint64_t w0 = ccst_now();
        cc_swap(s);
        uint64_t w = ccst_now() - w0;
        worst = w > worst ? w : worst;
        done++;
    }
    atomic_store(&t.deadline_ns, 0); /* the readers stop */
    for (unsigned i = 0; i < started; i++)
        pthread_join(th[i], NULL);
    free(th);
    cc_clear();
    snprintf(why, wn, "%u readers, %lu of %lu publishes in %.0f ms (at most %u), %lu gets, the longest swap %.3f ms, "
             "at most %lu retired at once%s", started, done, publishes, (double)(ccst_now() - t0) / 1e6, ms,
             atomic_load(&t.gets), (double)worst / 1e6,
             cc_nretired_max, atomic_load(&t.bad) ? ": a reader saw its snapshot change or freed" : "");
    return started == readers && done == publishes && !atomic_load(&t.bad) ? 0 : -1;
}

static pthread_mutex_t self_mu = PTHREAD_MUTEX_INITIALIZER; /* C.id, C.uuid, C.have_node for cluster_self */

int cluster_self(struct cluster_self *out)
{
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&self_mu);
    out->have_node = C.have_node;
    out->id = C.have_node ? C.id : 0;
    memcpy(out->uuid, C.uuid, 16);
    pthread_mutex_unlock(&self_mu);
    memcpy(out->ident, C.ident, 16);
    memcpy(out->secret, C.secret, C.secret_len);
    out->secret_len = C.secret_len;
    snprintf(out->advertise, sizeof out->advertise, "%s", C.padv);
    return out->have_node ? 0 : -1;
}

static uint64_t g_vwatch_applied = UINT64_MAX; /* the metadata index last pushed (UINT64_MAX: none yet) */

void cluster_set_volume_watch(cluster_vol_watch_fn fn, void *arg)
{
    g_vwatch = fn;
    g_vwatch_arg = arg;
}

/* ---- users (S2-1). The watch's fn and arg change together under uw_mu, which the cluster thread holds across a call
 * (the callback must not block); uw_new asks for a push of what the copy has. */
static pthread_mutex_t uw_mu = PTHREAD_MUTEX_INITIALIZER;
static cluster_users_fn g_uwatch;
static void *g_uwatch_arg;
static atomic_int g_uw_new;
static atomic_int g_users_ok; /* cluster_users_caught_up */
/* where cluster_user_propose sends: the Raft leader's peer address as this server last knew it ("" none) */
static pthread_mutex_t ul_mu = PTHREAD_MUTEX_INITIALIZER;
static char g_ul_addr[META_ADDR_MAX];

void cluster_set_users_watch(cluster_users_fn fn, void *arg)
{
    pthread_mutex_lock(&uw_mu);
    g_uwatch = fn;
    g_uwatch_arg = arg;
    pthread_mutex_unlock(&uw_mu);
    atomic_store(&g_uw_new, 1);
}

int cluster_users_caught_up(void) { return atomic_load_explicit(&g_users_ok, memory_order_acquire); }

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
        if (type == HBS_LEASE) {
            uint64_t t = get64(&s);
            if (s.bad) {
                r->bad = 1;
                return;
            }
            h->send_ns = t;
        } else if (type == HBS_UNREACH) {
            uint32_t n = get32(&s);
            if (s.bad || n > CLUSTER_REPORT_UNREACH || (size_t)n * 8 > len) {
                r->bad = 1;
                return;
            }
            for (uint32_t i = 0; i < n; i++) {
                h->unreach[i].node = get32(&s);
                h->unreach[i].age_ms = get32(&s);
            }
            if (s.bad) {
                r->bad = 1;
                return;
            }
            h->nunreach = n;
        } else if (type == HBS_ISR) {
            uint32_t n = get32(&s);
            if (s.bad || n > CLUSTER_REPORT_ISR || (size_t)n * 32 > len) {
                r->bad = 1;
                return;
            }
            for (uint32_t i = 0; i < n && !s.bad; i++) {
                struct cluster_isr_req *q = &h->isr[i];
                q->volume = get32(&s);
                q->epoch = get64(&s);
                q->isr_version = get64(&s);
                q->commit = get64(&s);
                q->nisr = get32(&s);
                if (q->nisr > META_REPLICAS_MAX)
                    s.bad = 1;
                for (uint32_t k = 0; k < q->nisr && !s.bad; k++) {
                    q->node[k] = get32(&s);
                    q->rid[k] = get64(&s);
                }
            }
            if (s.bad) {
                h->nisr = 0;
                r->bad = 1;
                return;
            }
            h->nisr = n;
        } else if (type == HBS_NODE) {
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
                v->rid = get64(&s);
                v->open_gen = get64(&s);
                v->open_nonce = get64(&s);
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
    C.pend[C.npend++] = (struct pend){conn, reqid, member, index, term, C.pend_join, C.pend_volume, C.pend_user};
    C.pend_user = NULL;
}

static void send_join(struct conn *d, uint32_t id);

/* a user op applied here as entry `index` (C.sm is at it): its reply waits for the members (users_duty) */
static void uwait_add(const struct pend *p, uint64_t index)
{
    uint32_t uid = p->user->uid;
    const struct meta_user *u = meta_user(&C.sm, p->user->name);
    if (u)
        uid = u->uid; /* a put's: assigned at this apply when the user is new */
    if (C.nuwait == C.capuwait) {
        C.capuwait = C.capuwait ? C.capuwait * 2 : 8;
        C.uwait = realloc(C.uwait, C.capuwait * sizeof *C.uwait);
        if (!C.uwait)
            oom();
    }
    C.uwait[C.nuwait++] = (struct uwait){p->conn, p->reqid, uid, index,
                                         iso_now_ns() + (uint64_t)p->user->wait_ms * MS};
}

/* entry `index` (`term`) applied with the first operation's result rc0: answer whoever waited for it */
static void pend_applied(uint64_t index, uint64_t term, int rc0)
{
    for (size_t i = 0; i < C.npend;) {
        struct pend *p = &C.pend[i];
        if (p->index <= index) {
            int rc = p->index == index && p->term == term ? rc0 : CLUSTER_ERR_RETRY;
            struct conn *d = p->join ? conn_by_serial(p->join) : NULL;
            if (p->user) {
                if (p->conn && rc == 0)
                    uwait_add(p, index);
                else if (p->conn)
                    admin_reply(p->conn, p->reqid, rc, NULL, 0);
                free(p->user);
            } else if (d && rc == 0)
                send_join(d, p->member); /* committed: now the server may learn its id; it answers the CLI */
            else if (p->conn && p->volume && rc == 0) {
                uint8_t nb[4] = {(uint8_t)(p->volume - 1), (uint8_t)((p->volume - 1) >> 8),
                                 (uint8_t)((p->volume - 1) >> 16), (uint8_t)((p->volume - 1) >> 24)};
                admin_reply(p->conn, p->reqid, 0, nb, 4);
            } else if (p->conn)
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
    C.ring_bytes = 0;
}

static void ring_drop_first(void)
{
    struct ring_e *e = &C.ring[C.ring_first % RING];
    C.ring_bytes -= e->len;
    free(e->data);
    e->data = NULL;
    e->len = 0;
    C.ring_first++;
}

static void ring_push(uint64_t index, uint64_t term, uint8_t type, const void *data, uint32_t len)
{
    if (C.ring_last && index != C.ring_last + 1)
        ring_reset();
    if (C.ring_first && index - C.ring_first >= RING)
        ring_drop_first(); /* the slot `index` takes */
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
    C.ring_bytes += len;
    C.ring_last = index;
    if (!C.ring_first)
        C.ring_first = index;
    if (C.ring_bytes > C.ring_max && C.ring_first < C.ring_last && !C.ring_said) {
        C.ring_said = 1;
        say("the watch ring holds %" PRIu64 " bytes, over its %" PRIu64 ": the oldest entries go (said once)",
            C.ring_bytes, C.ring_max);
    }
    while (C.ring_bytes > C.ring_max && C.ring_first < C.ring_last)
        ring_drop_first();
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
static int is_shared(void);

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
        struct dres r = {.kind = j.kind, .gen = j.gen, .index = j.index, .term = j.term, .rgen = j.rgen,
                         .img = j.img, .len = j.len, .cfg = j.cfg};
        if (j.kind == DJ_SYNC) {
            while (C.stall_file && access(C.stallfile, F_OK) == 0 && !C.dw_stop)
                usleep(10000); /* a test's stalled disk */
            if (C.sync_delay_ms)
                usleep(C.sync_delay_ms * 1000u); /* a test's slow disk: appends land while a round is out */
            iso_err e = raft_log_sync_run(&j.sync);
            r.index = j.sync.index;
            r.term = j.sync.term;
            r.rc = e != ISO_OK || j.sync.err;
            r.err = j.sync.err ? j.sync.err : EIO;
            if (C.fail_file && access(C.failfile, F_OK) == 0) { /* a test's dying disk */
                r.rc = 1;
                r.err = EIO;
            }
        } else {
            r.rc = write_durable(j.kind == DJ_COPY ? C.copyfile : C.snapfile, j.img, j.len);
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
    C.sync_again = 0;
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
static void copy_done(struct dres *r);
static void sync_round(void);

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
        if (r.kind == DJ_COPY) { /* what the file holds is true whatever core came and went since */
            copy_done(&r);
            continue;
        }
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
            if (C.sync_again) { /* entries were appended while this round ran, and the core asked only once */
                C.sync_again = 0;
                sync_round();
            }
        } else {
            snap_done(&r);
        }
    }
}

/* A sync round: the descriptors are taken here, the fdatasyncs run on the worker, and the core hears raft_persisted
 * from dw_collect. One round at a time: the core asks again for what was appended meanwhile. */
static void sync_round(void)
{
    if (C.sync_since) {
        C.sync_again = 1;
        return;
    }
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
    if (r->rc == -2) { /* the new snapshot is named, maybe not durably: compacting on it could lose the log's base */
        if (!is_shared())
            fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.snapfile, strerror(r->err));
        free(r->img);
        log_fault("cannot sync %s after naming %s: %s", C.metadir, C.snapfile, strerror(r->err));
        return;
    }
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
    C.img_shared |= t.mode == META_MODE_SHARED;
    if (write_durable(C.snapfile, a->u.snapshot.data, a->u.snapshot.len) != 0) {
        if (!is_shared())
            fail_stop("cannot write %s: %s", C.snapfile, strerror(errno));
        meta_free(&t);
        log_fault("cannot write %s: %s", C.snapfile, strerror(errno));
        return;
    }
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
    C.copy_dirty = 1;
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
            if (last) {
                C.copy_dirty = 1;
                raft_applied(C.r, last);
            }
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
static void sync_stall_check(void)
{
    /* a fresh clock: the loop's `now` was read before pump() handed a sync to the worker, so sync_since can be
     * later than it, and the unsigned difference would read as hours (a stall at every round, leadership flapping) */
    uint64_t now = iso_now_ns();
    if (!C.sync_since || C.role != RAFT_LEADER || now < C.sync_since || now - C.sync_since < C.stall_ns ||
        now < C.xfer_next)
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
    C.img_shared |= ok && t.mode == META_MODE_SHARED;
    meta_free(&t);
    if (!ok)
        return 1;
    int rc = write_durable(C.pendfile, m->u.snapshot.data, m->u.snapshot.len);
    if (rc == 0 && C.fail_pend_dirsync)
        rc = -2;
    if (rc == -2) {
        if (!is_shared())
            fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.pendfile, strerror(errno));
        log_fault("cannot sync %s after naming %s: %s", C.metadir, C.pendfile, strerror(errno));
        return 0; /* the core is gone: nothing takes the message */
    }
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
    /* the core's own lease (check-quorum): the Raft leader grants read leases only while it holds (on_hb) */
    o.lease_reads = true;
    o.max_clock_drift_ppm = settings()->drift_ppm ? settings()->drift_ppm : 500;
    C.core_e = o.election_min_ns;
    C.core_drift = o.max_clock_drift_ppm;
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
static int cpy_ahead(void);

/* removed from the group while away: the core goes (its files stay), the state goes on from `next` (or as is) */
static void drop_core(struct meta_state *next, const char *why)
{
    say("node %" PRIu32 " is not in the metadata group's configuration (%s): it follows a watch stream now", C.id,
        why);
    core_stop(next);
}

static int is_shared(void)
{
    return C.sm.mode == META_MODE_SHARED || C.img_shared;
}

static int log_fault(const char *fmt, ...)
{
    char what[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(what, sizeof what, fmt, ap);
    va_end(ap);
    if (!is_shared())
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
    if (cpy_ahead()) { /* the core had applied less than the file holds: the state goes on from the copy, stale */
        meta_free(&C.sm);
        C.sm = C.cpy;
        C.cpy_on = 0;
        C.copy_stale = 1;
    } else if (C.cpy_on) {
        meta_free(&C.cpy);
        C.cpy_on = 0;
    }
    for (size_t i = 0; i < C.npend; i++) {
        if (C.pend[i].conn)
            admin_reply(C.pend[i].conn, C.pend[i].reqid, CLUSTER_ERR_RETRY, NULL, 0);
        free(C.pend[i].user);
    }
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

static int raft_held(void) { return C.hold_raft && access(C.holdraftfile, F_OK) == 0; }

static void on_raft(struct conn *c, struct rd *r)
{
    if (!C.have_node || !c->id || raft_held())
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
        if (C.retired)
            return; /* a log fault ended this identity's voter role: the leader still sends until it removes it */
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

static int promo_holds_lease(uint32_t node);

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
    struct heard *h = heard_get(node);
    h->send_ns = 0; /* this heartbeat's own, or none */
    heard_sections(h, r, now);
    if (r->bad) { /* a malformed section: the link is dropped, as for any broken frame */
        c->dead = 1;
        return;
    }
    /* the lease (§7.1): only the Raft leader grants, and only while its own lease from the core holds (check-quorum:
     * no other Raft leader can exist before it ends), so an answer is never given after a new Raft leader began its
     * wait of one lease */
    uint64_t until = h->send_ns && !promo_holds_lease(node) ? core_lease_until(now) : 0;
    int granted = until != 0;
    if (granted && until != UINT64_MAX && now >= until) /* only the mutants can: test_cluster lease looks for it */
        say("BUG: grants node %" PRIu32 " a lease %" PRIu64 " ms past the end of its own (term %" PRIu64 ")", node,
            (uint64_t)((now - until) / MS), raft_get_term(C.r));
    if (granted)
        h->answered_ns = now;
    const struct meta_settings *s = settings();
    frame_begin(&C.fb, F_HB_ACK);
    put32(&C.fb, C.r ? raft_get_leader(C.r) : 0);
    put64(&C.fb, C.r ? raft_get_term(C.r) : 0);
    put64(&C.fb, C.r ? raft_get_commit(C.r) : 0);
    put64(&C.fb, h->send_ns);
    put8(&C.fb, (uint8_t)granted);
    put32(&C.fb, s->lease_ms);
    put32(&C.fb, s->drift_ppm);
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void on_hb_ack(struct conn *c, struct rd *r)
{
    uint32_t leader = get32(r);
    uint64_t term = get64(r), commit = get64(r);
    uint64_t echo = get64(r);
    uint8_t granted = get8(r);
    uint32_t lease_ms = get32(r), drift = get32(r);
    if (r->bad)
        return;
    /* the lease: from the Raft leader itself, about a heartbeat this server sent (its own clock, echoed), and never
     * from a leader of a term older than one this server heard of (a deposed leader answering heartbeats it had
     * queued while it was stopped) */
    uint64_t now = iso_now_ns();
    uint64_t known = C.r && raft_get_term(C.r) > C.hint_term ? raft_get_term(C.r) : C.hint_term;
    if (term < known)
        return;
    int from_leader = granted && leader && leader == c->id;
    if ((from_leader || (cl_mutant() == CLM_LEASEANYVOTER && c->id)) && echo && echo <= now && drift < 1000000)
        lease_extend(cluster_lease_end(echo, lease_ms, drift));
    if (term > C.hint_term || leader)
        C.hint_leader = leader;
    /* the users' barrier: the commit index of the Raft leader itself, in a term no older than any heard of, and only
     * from an ack that grants the lease: the leader then holds the core's check-quorum lease, which it takes only once
     * an entry of its own term is committed (raft_read_lease), so no other leader exists and its commit index covers
     * every committed entry. A deposed leader in the same term that has not stepped down yet grants nothing. */
    if (!C.users_floor && from_leader && commit)
        C.users_floor = commit;
    /* the highest commit any voter reported in this term: what the watch stream must reach (watch_duty) */
    if (term > C.hint_term || commit > C.hint_commit)
        C.hint_commit = commit;
    C.hint_term = term;
}

/* after the probe state moved: removed while away? */
static void check_probe(void)
{
    raft_config cfg;
    /* Only a watched state at or past everything this core holds can say it was removed: a stream replayed from index
     * 1 passes the INIT entry, whose configuration holds the first node only, and a restarted core has applied nothing
     * (C.sm.applied 0) while its log reaches past the entry that added it. */
    uint64_t own = C.sm.applied;
    if (C.r && raft_get_last(C.r) > own && cl_mutant() != CLM_PROBEINIT)
        own = raft_get_last(C.r);
    if (C.r && C.wsm_on && C.wsm.applied >= own && config_of(&C.wsm, &cfg) && cfg.n &&
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
    /* An image below this state's index is a server that lags (or the copy of an earlier run, which went further
     * than that server did): never taken, not even over a persisted copy, or "never goes down" fails from this node's
     * view, and the copy file goes back with it. Only a `tsdb cluster restore` (a higher restore generation) takes
     * the state down. The link is dropped and another voter tried; the copy stays stale until one reaches it. */
    if (meta_snapshot_decode(&t, r->p + r->off, r->len - r->off) != META_OK ||
        (t.applied < to->applied && t.restore_gen <= to->restore_gen)) {
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
static int state_after(const struct meta_state *a, const struct meta_state *b)
{
    return a->restore_gen > b->restore_gen || (a->restore_gen == b->restore_gen && a->applied > b->applied);
}

/* a voter's copy file is ahead of what its core has applied since the restart */
static int cpy_ahead(void)
{
    return C.cpy_on && state_after(&C.cpy, &C.sm);
}

/* The state a status is shown from: a voter that restarted shows its persisted copy (marked stale) until its core has
 * applied that far again. */
static const struct meta_state *view_state(void)
{
    return cpy_ahead() ? &C.cpy : &C.sm;
}

/* Item 8, for every member (a voter writes its copy after it applied, a watcher after its stream moved it), on the
 * disk worker. The file never goes back: an image below the file's index under the same restore generation is not
 * written (a restarted voter applies from its snapshot up, behind the copy it loaded), and an equal one is not
 * written again. */
static void copy_flush(uint64_t now, int force)
{
    if (C.cpy_on && !cpy_ahead()) {
        meta_free(&C.cpy);
        C.cpy_on = 0;
    }
    if (!C.copy_dirty || (!force && (now < C.copy_next || C.copy_out)))
        return;
    if (!C.sm.initialised) {
        C.copy_dirty = 0;
        return;
    }
    if (C.sm.restore_gen == C.copy_gen && C.sm.applied == C.copy_applied) {
        C.copy_dirty = 0;
        return;
    }
    if (C.sm.restore_gen < C.copy_gen || (C.sm.restore_gen == C.copy_gen && C.sm.applied < C.copy_applied))
        return; /* behind the file: dirty stays set, written once the state is past it */
    uint8_t *img;
    size_t len;
    if (meta_snapshot_encode(&C.sm, &img, &len) != META_OK)
        oom();
    if (!force && C.dw_on) {
        struct djob j = {.kind = DJ_COPY, .img = img, .len = len, .index = C.sm.applied, .rgen = C.sm.restore_gen,
                         .fail_dirsync = C.fail_copy_dirsync};
        C.copy_dirty = 0;
        C.copy_out = 1;
        dw_submit(&j);
        return;
    }
    if (C.dw_on)
        dw_quiesce(); /* at the end: the worker's write is done before this one */
    C.copy_dirty = 0;
    int rc = write_durable(C.copyfile, img, len);
    if (rc == 0 && C.fail_copy_dirsync)
        rc = -2;
    free(img);
    if (rc == 0) {
        C.copy_applied = C.sm.applied;
        C.copy_gen = C.sm.restore_gen;
        return;
    }
    if (rc == -2 && !is_shared())
        fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.copyfile, strerror(errno));
    say("cannot %s %s: %s (the copy is not relied on)", rc == -2 ? "sync the directory of" : "write", C.copyfile,
        strerror(errno));
}

/* The worker's write of the copy is back. A failure keeps the older file and is tried again in a second. A failed
 * directory sync is fail-stop in replicated mode and a log line in shared mode: nothing relies on the copy in S1, and
 * a state-directory fault must not take the node's volumes down (§5.6). */
static void copy_done(struct dres *r)
{
    uint64_t now = iso_now_ns();
    C.copy_out = 0;
    free(r->img);
    if (r->rc == -2 && !is_shared())
        fail_stop("cannot sync %s after naming %s: %s", C.metadir, C.copyfile, strerror(r->err));
    if (r->rc != 0) {
        say("cannot %s %s: %s (tried again in 1 s; the copy is not relied on)",
            r->rc == -2 ? "sync the directory of" : "write", C.copyfile, strerror(r->err));
        C.copy_dirty = 1;
        C.copy_next = now + 1000 * MS;
        return;
    }
    C.copy_applied = r->index;
    C.copy_gen = r->rgen;
    C.copy_next = now + 200 * MS;
}

/* what a member's heartbeat last reported (the sections), after its line of the status */
static void put_heard_extra(struct buf *b, const struct heard *h, uint64_t now, int self)
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
        put64(b, v->rid);
        put64(b, v->open_gen);
        put64(b, v->open_nonce);
        put64(b, h->vols[i].fault_ns ? since(now, h->vols[i].fault_ns) / (1000 * MS) : 0);
    }
    put32(b, h ? h->ndoms : 0);
    for (uint32_t i = 0; h && i < h->ndoms; i++) {
        putstr(b, h->doms[i].name);
        put8(b, h->doms[i].state);
    }
    put32(b, h ? h->nunreach : 0);
    for (uint32_t i = 0; h && i < h->nunreach; i++) {
        put32(b, h->unreach[i].node);
        put32(b, h->unreach[i].age_ms);
    }
    put64(b, h && h->answered_ns ? since(now, h->answered_ns) / MS : UINT64_MAX);
    int64_t left = 0;
    if (self) {
        uint64_t m = mono_ns(), until = atomic_load(&g_lease_ns);
        left = until > m ? (int64_t)((until - m) / MS) : -(int64_t)((m - until) / MS) - 1;
    }
    put64(b, (uint64_t)left);
}

static void put_status(struct buf *b)
{
    uint64_t now = iso_now_ns();
    put32(b, C.have_node ? C.id : 0);
    put8(b, C.r ? CLUSTER_MODE_CORE : C.have_node ? CLUSTER_MODE_WATCH : CLUSTER_MODE_NONE);
    put8(b, C.r ? (uint8_t)raft_get_role(C.r) : 0);
    const struct meta_state *S = view_state();
    put8(b, (uint8_t)(C.copy_stale || S != &C.sm));
    put64(b, C.r ? raft_get_term(C.r) : C.hint_term);
    put32(b, C.r ? raft_get_leader(C.r) : C.hint_leader);
    put64(b, C.r ? raft_get_commit(C.r) : C.hint_commit);
    put64(b, S->applied);
    uint32_t others = 0;
    for (size_t i = 0; i < C.nheard; i++)
        others += C.heard[i].id != C.id;
    put32(b, others + (C.have_node ? 1 : 0));
    if (C.have_node) {
        put32(b, C.id);
        put64(b, 0);
        put64(b, C.incarnation);
        put_heard_extra(b, heard_get(C.id), now, 1);
    }
    for (size_t i = 0; i < C.nheard; i++) {
        if (C.heard[i].id == C.id)
            continue;
        put32(b, C.heard[i].id);
        put64(b, (since(now, C.heard[i].ns)) / MS);
        put64(b, C.heard[i].inc);
        put_heard_extra(b, &C.heard[i], now, 0);
    }
    put8(b, (uint8_t)cluster_users_caught_up());
    if (S->applied || S->config_len) {
        uint8_t *img;
        size_t len;
        if (meta_snapshot_encode(S, &img, &len) != META_OK)
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

size_t cluster_volume_add_encode(const char *name, uint32_t replicas, uint8_t *out)
{
    size_t n = strlen(name);
    out[0] = (uint8_t)n;
    out[1] = (uint8_t)(n >> 8);
    memcpy(out + 2, name, n);
    for (int j = 0; j < 4; j++)
        out[2 + n + j] = (uint8_t)(replicas >> (8 * j));
    return 2 + n + 4;
}

size_t cluster_volume_force_encode(uint32_t volume, uint32_t node, uint8_t *out)
{
    for (int j = 0; j < 4; j++) {
        out[j] = (uint8_t)(volume >> (8 * j));
        out[4 + j] = (uint8_t)(node >> (8 * j));
    }
    return 8;
}

/* A volume's name: what clients name it by (letters, digits, '_', '-', '.'; not "." or ".."). */
static int text_ok_cl(const char *s)
{
    if (!strcmp(s, ".") || !strcmp(s, ".."))
        return 0;
    for (; *s; s++)
        if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '_' ||
              *s == '-' || *s == '.'))
            return 0;
    return 1;
}

/* A member a new volume may be placed on: up, taking volumes, and heard within node_timeout (the leader: itself). */
static int placeable(const struct meta_member *mb, uint64_t now)
{
    if (mb->state != META_UP || !(mb->flags & META_MF_VOLUMES_OK))
        return 0;
    if (mb->id == C.id)
        return 1;
    const struct heard *h = heard_get(mb->id);
    return h && h->ns && now - h->ns < (uint64_t)C.sm.set.node_timeout_ms * MS;
}

/* §3.2 rules 1-2, a deterministic greedy pass: the preferred leader (nodes[0]) is the live member that leads the
 * fewest volumes (then holds the fewest, then the lowest id); each further replica the one that holds the fewest,
 * on a rack not used yet while one is left (racks count only when every candidate has one and there are at least
 * `n` of them). Returns 0 or -1 (fewer live members than n). */
static int place_volume(uint32_t n, uint64_t now, uint32_t *nodes)
{
    uint32_t cand[META_REPLICAS_MAX * 64], nc = 0, leads[META_REPLICAS_MAX * 64], holds[META_REPLICAS_MAX * 64];
    for (uint32_t i = 0; i < C.sm.nmembers && nc < sizeof cand / sizeof cand[0]; i++) {
        const struct meta_member *mb = &C.sm.members[i];
        if (!placeable(mb, now))
            continue;
        leads[nc] = holds[nc] = 0;
        for (uint32_t v = 0; v < C.sm.nvolumes; v++)
            for (uint32_t k = 0; k < C.sm.volumes[v].nrep; k++)
                if (C.sm.volumes[v].rep[k].node == mb->id) {
                    holds[nc]++;
                    leads[nc] += k == 0;
                }
        cand[nc++] = i;
    }
    if (nc < n)
        return -1;
    int racks = 1;
    uint32_t distinct = 0;
    for (uint32_t i = 0; i < nc; i++) {
        const char *ri = C.sm.members[cand[i]].rack;
        if (!ri[0])
            racks = 0;
        int seen = 0;
        for (uint32_t j = 0; j < i; j++)
            seen |= !strcmp(C.sm.members[cand[j]].rack, ri);
        distinct += !seen;
    }
    racks = racks && distinct >= n;
    uint8_t used[META_REPLICAS_MAX * 64] = {0};
    for (uint32_t k = 0; k < n; k++) {
        int best = -1;
        for (uint32_t i = 0; i < nc; i++) {
            if (used[i])
                continue;
            if (racks) {
                int clash = 0;
                for (uint32_t j = 0; j < k; j++)
                    clash |= !strcmp(C.sm.members[cand[i]].rack, meta_member(&C.sm, nodes[j])->rack);
                if (clash)
                    continue;
            }
            if (best < 0)
                best = (int)i;
            else if (k == 0 ? (leads[i] < leads[best] || (leads[i] == leads[best] && holds[i] < holds[best]))
                            : holds[i] < holds[best])
                best = (int)i;
        }
        if (best < 0)
            return -1;
        used[best] = 1;
        nodes[k] = C.sm.members[cand[best]].id;
    }
    return 0;
}

static void admin_volume_add(struct conn *c, uint32_t reqid, struct rd *r)
{
    struct meta_volume_add a = {0};
    getstr(r, a.name, sizeof a.name);
    uint32_t n = get32(r);
    if (r->bad || !a.name[0] || !text_ok_cl(a.name)) {
        admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0);
        return;
    }
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER || !C.sm.initialised) {
        const char *lp = leader_peer();
        admin_reply(c->serial, reqid, C.sm.initialised || C.have_node ? CLUSTER_ERR_NOTLEADER : CLUSTER_ERR_STATE,
                    lp, strlen(lp));
        return;
    }
    if (!n)
        n = C.sm.set.replication;
    if (C.sm.mode != META_MODE_REPLICATED || n < 1 || n > META_REPLICAS_MAX) {
        admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0);
        return;
    }
    for (uint32_t i = 0; i < C.sm.nvolumes; i++)
        if (!strcmp(C.sm.volumes[i].name, a.name)) {
            admin_reply(c->serial, reqid, META_ERR_EXISTS, NULL, 0);
            return;
        }
    /* the next free number; 0 is never a cluster volume's (a node's root keeps volume 0 for its USERS) */
    a.number = C.sm.next_volume ? C.sm.next_volume : 1;
    if (a.number > META_VOLUME_MAX) {
        admin_reply(c->serial, reqid, META_ERR_RANGE, NULL, 0);
        return;
    }
    if (place_volume(n, iso_now_ns(), a.nodes) != 0) {
        say("volume add %s: fewer than %" PRIu32 " live members take volumes", a.name, n);
        admin_reply(c->serial, reqid, CLUSTER_ERR_FEW, NULL, 0);
        return;
    }
    a.n = n;
    a.kind = META_VOL_REPLICATED;
    a.min_isr = C.sm.set.min_isr < n ? C.sm.set.min_isr : n;
    tsdb_random(a.uuid, sizeof a.uuid);
    for (uint32_t i = 0; i < n; i++) {
        int dup;
        do {
            tsdb_random(&a.rid[i], sizeof a.rid[i]);
            dup = !a.rid[i];
            for (uint32_t j = 0; j < i; j++)
                dup |= a.rid[j] == a.rid[i];
        } while (dup);
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_add(&e, &a);
    C.pend_volume = a.number + 1;
    propose(&e, c->serial, reqid, 0);
    C.pend_volume = 0;
}

/* ---- users (S2-1): USER_PUT, USER_DEL through the Raft leader, answered after commit with the members still below
 * the entry (users_duty); USER_GET from any copy */

static int uname_ok(const char *s)
{
    if (!s[0])
        return 0;
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || *s == 0x7f)
            return 0;
    return 1;
}

static void admin_user_change(struct conn *c, uint32_t reqid, struct rd *r, int del)
{
    struct upend *u = calloc(1, sizeof *u);
    if (!u)
        oom();
    getstr(r, u->name, sizeof u->name);
    uint64_t version = get64(r);
    u->wait_ms = get32(r);
    uint32_t blen = del ? 0 : get32(r);
    const uint8_t *blob = r->p + r->off;
    if (!r->bad && (blen > META_USER_BLOB_MAX || !need(r, blen)))
        r->bad = 1;
    if (!r->bad)
        r->off += blen;
    char actor[META_USER_NAME_MAX] = ""; /* the actor guard (meta_op_user_put_as): tsdbd's admin ops send it */
    uint64_t aver = 0;
    if (!r->bad && r->off < r->len) {
        getstr(r, actor, sizeof actor);
        aver = get64(r);
        if (!r->bad && (r->off != r->len || !uname_ok(actor)))
            r->bad = 1;
    }
    if (r->bad || !uname_ok(u->name) || (!del && blen < META_USER_UID_LEN) || u->wait_ms > 3600000) {
        free(u);
        admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0);
        return;
    }
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER || !C.sm.initialised) {
        free(u);
        const char *lp = leader_peer();
        admin_reply(c->serial, reqid, C.sm.initialised || C.have_node ? CLUSTER_ERR_NOTLEADER : CLUSTER_ERR_STATE,
                    lp, strlen(lp));
        return;
    }
    const struct meta_user *cur = meta_user(&C.sm, u->name);
    if (del && !cur) {
        free(u);
        admin_reply(c->serial, reqid, META_ERR_NOTFOUND, NULL, 0);
        return;
    }
    u->uid = cur ? cur->uid : 0;
    struct meta_entry e;
    meta_entry_init(&e);
    if (del)
        meta_op_user_del_as(&e, u->name, actor, aver);
    else
        meta_op_user_put_as(&e, u->name, version, blob, blen, actor, aver);
    int early = cl_mutant() == CLM_DROPNOTCOMMITTED;
    C.pend_user = u;
    iso_err rc = propose(&e, early ? 0 : c->serial, reqid, 0);
    if (C.pend_user) { /* not proposed: propose() answered RETRY */
        free(C.pend_user);
        C.pend_user = NULL;
    } else if (early && rc == ISO_OK) { /* the mutant: OK before the entry commits */
        uint8_t b[16] = {0};
        for (int j = 0; j < 4; j++)
            b[8 + j] = (uint8_t)(u->uid >> (8 * j));
        admin_reply(c->serial, reqid, 0, b, sizeof b);
    }
}

static void admin_user_get(struct conn *c, uint32_t reqid, struct rd *r)
{
    char name[META_USER_NAME_MAX];
    getstr(r, name, sizeof name);
    if (r->bad) {
        admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0);
        return;
    }
    const struct meta_state *S = view_state();
    struct buf b = {0};
    if (!name[0]) {
        put32(&b, S->nusers);
        for (uint32_t i = 0; i < S->nusers; i++) {
            putstr(&b, S->users[i].name);
            put32(&b, S->users[i].uid);
            put64(&b, S->users[i].version);
        }
    } else {
        const struct meta_user *u = meta_user(S, name);
        if (!u) {
            admin_reply(c->serial, reqid, META_ERR_NOTFOUND, NULL, 0);
            return;
        }
        put64(&b, u->version);
        put32(&b, u->uid);
        put(&b, u->blob, u->len);
    }
    admin_reply(c->serial, reqid, 0, b.p, b.len);
    buf_free(&b);
}

/* TSDB_CLUSTER_TEST users_probe=1 (tests only): the users watch and cluster_user_propose seen from outside. The watch
 * writes <state-dir>/userswatch ("applied A" and a line per user); a user named probe-drop is dropped once through
 * cluster_user_propose on a thread of its own (it must not run on the cluster thread), the result in userpropose. */
static void probe_write(const char *name, const char *text)
{
    char path[1200], tmp[1210];
    snprintf(path, sizeof path, "%s/%s", C.metadir, name);
    snprintf(tmp, sizeof tmp, "%s.tmp", path);
    FILE *f = fopen(tmp, "w");
    if (!f)
        return;
    fputs(text, f);
    if (fclose(f) == 0)
        rename(tmp, path);
}

static void *probe_drop(void *arg)
{
    (void)arg;
    uint64_t index = 0;
    uint32_t uid = 0, nlag = 0;
    struct cluster_laggard lag[CLUSTER_LAGGARDS_MAX];
    int rc = cluster_user_propose(CLUSTER_USER_DEL, "probe-drop", NULL, 0, 0, NULL, 0, 5000, &index, &uid, lag, &nlag);
    char t[128];
    snprintf(t, sizeof t, "rc %d index %" PRIu64 " uid %" PRIu32 "\n", rc, index, uid);
    probe_write("userpropose", t);
    return NULL;
}

static int users_probe_watch(void *arg, const struct cluster_user_rec *u, uint32_t n, uint64_t applied)
{
    (void)arg;
    static int dropping;
    struct buf b = {0};
    char line[300];
    snprintf(line, sizeof line, "applied %" PRIu64 "\n", applied);
    put(&b, line, strlen(line));
    for (uint32_t i = 0; i < n; i++) {
        snprintf(line, sizeof line, "user %s uid %" PRIu32 " version %" PRIu64 "\n", u[i].name, u[i].uid, u[i].version);
        put(&b, line, strlen(line));
        pthread_t t;
        if (!strcmp(u[i].name, "probe-drop") && !dropping && pthread_create(&t, NULL, probe_drop, NULL) == 0) {
            dropping = 1;
            pthread_detach(t);
        }
    }
    put(&b, "", 1);
    probe_write("userswatch", (const char *)b.p);
    buf_free(&b);
    return 0;
}

/* The users watch, given the copy's users when they changed. Called before every heartbeat, so the applied index a
 * heartbeat reports is one the watch has been given (a leader's "no member lags" then means every member's watch has
 * the change), and before the leader answers waiting user ops. */
static void users_push(void)
{
    uint64_t umax = 0;
    for (uint32_t i = 0; i < C.sm.nusers; i++)
        if (C.sm.users[i].version > umax)
            umax = C.sm.users[i].version;
    /* (count, highest version) changes with every change of the set: a put raises the highest version unless its
     * user is dropped again, which leaves the set as it was */
    int force = atomic_exchange(&g_uw_new, 0), taken = 1;
    if (force || C.sm.nusers != C.uw_n || umax != C.uw_max || (!C.uw_said && C.sm.nusers)) {
        pthread_mutex_lock(&uw_mu);
        if (g_uwatch && (C.sm.nusers || C.uw_said)) {
            struct cluster_user_rec *recs = calloc(C.sm.nusers ? C.sm.nusers : 1, sizeof *recs);
            if (!recs)
                oom();
            for (uint32_t i = 0; i < C.sm.nusers; i++) {
                const struct meta_user *u = &C.sm.users[i];
                recs[i] = (struct cluster_user_rec){u->name, u->uid, u->version, u->blob, u->len};
            }
            if (g_uwatch(g_uwatch_arg, recs, C.sm.nusers, C.sm.applied) != 0) {
                taken = 0;
                atomic_store(&g_uw_new, 1); /* not taken (out of memory): again on the next tick */
            }
            free(recs);
            C.uw_said = 1;
        }
        pthread_mutex_unlock(&uw_mu);
        C.uw_n = C.sm.nusers;
        C.uw_max = umax;
    }
    /* a watch set meanwhile (g_uw_new) has not been given these yet */
    if (taken && !atomic_load(&g_uw_new))
        C.uw_at = C.sm.applied;
}

/* the applied index member `id` last reported (its heartbeat; this server's own copy for itself), 0 never */
static uint64_t member_applied(uint32_t id)
{
    if (id == C.id)
        return C.sm.applied;
    struct heard *h = heard_get(id);
    return h ? h->applied : 0;
}

/* Every 100 ms on the cluster thread: the users' barrier, the waiting user replies, the users watch, and the leader's
 * address for cluster_user_propose. */
static void users_duty(uint64_t now)
{
    if (C.r && raft_get_role(C.r) == RAFT_LEADER) {
        raft_index commit = raft_get_commit(C.r);
        /* a leader's own commit index covers every earlier term only once an entry of its own term is committed */
        if (!C.users_floor && commit && raft_get_term_at(C.r, commit) == raft_get_term(C.r))
            C.users_floor = commit;
    }
    /* before the replies: a member this server counts as caught up has its watch given the change; and before the
     * barrier opens, so a login it lets in finds the users the copy has at the floor, not the ones from before */
    users_push();
    /* and only once the watch has taken the users at the floor: a push it failed (out of memory) keeps it closed */
    if (C.users_floor &&
        ((C.sm.applied >= C.users_floor && C.uw_at >= C.users_floor) || cl_mutant() == CLM_FLOORNOAPPLIED) &&
        !atomic_load(&g_users_ok))
        atomic_store_explicit(&g_users_ok, 1, memory_order_release);

    for (size_t i = 0; i < C.nuwait;) {
        struct uwait *w = &C.uwait[i];
        struct cluster_laggard lag[CLUSTER_LAGGARDS_MAX];
        uint32_t nl = 0;
        for (uint32_t k = 0; k < C.sm.nmembers && nl < CLUSTER_LAGGARDS_MAX; k++) {
            const struct meta_member *m = &C.sm.members[k];
            uint64_t a = m->state == META_OUT ? UINT64_MAX : member_applied(m->id);
            if (a < w->index)
                lag[nl++] = (struct cluster_laggard){m->id, a};
        }
        if (nl && now < w->deadline) {
            i++;
            continue;
        }
        struct buf b = {0};
        put64(&b, w->index);
        put32(&b, w->uid);
        put32(&b, nl);
        for (uint32_t k = 0; k < nl; k++) {
            put32(&b, lag[k].id);
            put64(&b, lag[k].applied);
        }
        admin_reply(w->conn, w->reqid, 0, b.p, b.len);
        buf_free(&b);
        *w = C.uwait[--C.nuwait];
    }


    const char *lp = C.r && raft_get_role(C.r) == RAFT_LEADER ? C.padv : leader_peer();
    pthread_mutex_lock(&ul_mu);
    snprintf(g_ul_addr, sizeof g_ul_addr, "%s", lp);
    pthread_mutex_unlock(&ul_mu);
}

int cluster_user_propose(int op, const char *name, const void *blob, uint32_t len, uint64_t version,
                         const char *actor, uint64_t actor_version, int wait_ms, uint64_t *index, uint32_t *uid,
                         struct cluster_laggard *lag, uint32_t *nlag)
{
    *index = 0;
    *uid = 0;
    *nlag = 0;
    size_t nl = strlen(name), al = actor ? strlen(actor) : 0;
    if ((op != CLUSTER_USER_PUT && op != CLUSTER_USER_DEL) || nl >= META_USER_NAME_MAX || al >= META_USER_NAME_MAX ||
        wait_ms < 0 || (op == CLUSTER_USER_PUT && (len < META_USER_UID_LEN || len > META_USER_BLOB_MAX)))
        return META_ERR_INVALID;
    if (op == CLUSTER_USER_DEL)
        len = 0;
    size_t qn = 2 + nl + 8 + 4 + (op == CLUSTER_USER_PUT ? 4 + (size_t)len : 0) + (al ? 2 + al + 8 : 0);
    uint8_t *q = malloc(qn), *p = q;
    if (!q)
        return CLUSTER_ERR_RETRY;
    *p++ = (uint8_t)nl;
    *p++ = (uint8_t)(nl >> 8);
    memcpy(p, name, nl);
    p += nl;
    for (int j = 0; j < 8; j++)
        *p++ = (uint8_t)(version >> (8 * j));
    for (int j = 0; j < 4; j++)
        *p++ = (uint8_t)((uint32_t)wait_ms >> (8 * j));
    if (op == CLUSTER_USER_PUT) {
        for (int j = 0; j < 4; j++)
            *p++ = (uint8_t)(len >> (8 * j));
        memcpy(p, blob, len);
        p += len;
    }
    if (al) {
        *p++ = (uint8_t)al;
        *p++ = (uint8_t)(al >> 8);
        memcpy(p, actor, al);
        p += al;
        for (int j = 0; j < 8; j++)
            *p++ = (uint8_t)(actor_version >> (8 * j));
    }
    char addr[META_ADDR_MAX];
    pthread_mutex_lock(&ul_mu);
    snprintf(addr, sizeof addr, "%s", g_ul_addr);
    pthread_mutex_unlock(&ul_mu);
    int ret = CLUSTER_ERR_NOTLEADER;
    for (int hop = 0; hop < 2 && addr[0]; hop++) {
        int rc;
        uint8_t *rep = NULL;
        size_t rn = 0;
        char err[256];
        if (cluster_admin(addr, C.secret, C.secret_len, op == CLUSTER_USER_PUT ? CLUSTER_OP_USER_PUT
                                                                               : CLUSTER_OP_USER_DEL,
                          q, qn, wait_ms + 15000, &rc, &rep, &rn, err, sizeof err) != 0) {
            ret = CLUSTER_ERR_RETRY; /* sent or not: the outcome is unknown */
            break;
        }
        if (rc == CLUSTER_ERR_NOTLEADER) { /* follow the leader it names, once */
            snprintf(addr, sizeof addr, "%.*s", (int)(rn < sizeof addr ? rn : sizeof addr - 1), (char *)rep);
            free(rep);
            ret = CLUSTER_ERR_NOTLEADER;
            continue;
        }
        ret = rc;
        if (rc == META_OK) {
            struct rd r = {rep, rn, 0, 0};
            *index = get64(&r);
            *uid = get32(&r);
            uint32_t n = get32(&r);
            for (uint32_t k = 0; k < n && !r.bad; k++) {
                uint32_t id = get32(&r);
                uint64_t a = get64(&r);
                if (k < CLUSTER_LAGGARDS_MAX)
                    lag[(*nlag)++] = (struct cluster_laggard){id, a};
            }
            if (r.bad)
                ret = CLUSTER_ERR_RETRY;
        }
        free(rep);
        break;
    }
    free(q);
    return ret;
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
        pthread_mutex_lock(&self_mu);
        tsdb_random(C.uuid, sizeof C.uuid);
        C.id = 1;
        pthread_mutex_unlock(&self_mu);
        C.incarnation = 1;
        snprintf(C.seeds, sizeof C.seeds, "%s", C.padv);
        if (node_save() != 0)
            fail_stop("cannot write %s: %s", C.nodefile, strerror(errno));
        pthread_mutex_lock(&self_mu);
        C.have_node = 1;
        pthread_mutex_unlock(&self_mu);
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

/* `tsdb cluster volume force-leader` (design-cluster.md 6.2): with every ISR member down, the group cannot promote
 * (NO_OWNER). The operator names a replica that is up; it joins the ISR (an ISR_CHANGE with the record's leader still
 * in it), so the promotion that is waiting fences it and, as the only one to answer, picks it. Unclean: writes the old
 * ISR acknowledged past the replica's log end are lost, and its own extra records are cut from the others when they
 * return. The promotion's log line compares the replica's durable end with the last shrink's commit point. */
/* The proposer's memory per volume (cluster thread): one entry per volume of C.sm, ascending by number as C.sm.volumes
 * is, so a lookup is a binary search; vs_sync follows the record's list whenever a volume is not found. Fixed tables of
 * 16 lost entries once more volumes than that failed over at once (review F4): a deposed leader's timer, overwritten
 * at random each round, could then never reach DEPOSED_HOLD_NS, and that volume stayed without a writer. Never hold a
 * pointer across a call that may look up a volume C.sm does not list (it reallocates). */
struct vstate {
    uint32_t vol;
    uint32_t promo;                     /* 1 + the index of its promotion in g_promo: a hint promo_find checks */
    uint8_t cut_held, dep_seen, cut_failed, forced, resigned;
    uint64_t cut_epoch, cut_version, cut_ns; /* cut_move: when a shrink of this basis was first held */
    uint32_t dep_leader;                /* leader_deposed: since when this leader of this epoch does not lead */
    uint64_t dep_epoch, dep_ns;
    uint64_t fail_epoch, fail_version;  /* a move given up for this basis: the shrink goes ahead */
    uint32_t forced_node;               /* force-leader, so that the promotion's log line can compare */
    uint64_t forced_epoch;
    uint32_t resign_from;               /* a RESIGN of the record's leader heard since the last duty round */
    uint64_t resign_epoch;
};
static struct vstate *g_vs;
static uint32_t g_nvs;

static void vs_sync(void)
{
    struct vstate *n = calloc(C.sm.nvolumes ? C.sm.nvolumes : 1, sizeof *n);
    if (!n)
        oom();
    for (uint32_t i = 0, j = 0; i < C.sm.nvolumes; i++) {
        uint32_t num = C.sm.volumes[i].number;
        while (j < g_nvs && g_vs[j].vol < num)
            j++;
        if (j < g_nvs && g_vs[j].vol == num)
            n[i] = g_vs[j];
        else
            n[i].vol = num;
    }
    free(g_vs);
    g_vs = n;
    g_nvs = C.sm.nvolumes;
}

/* NULL: C.sm has no such volume */
static struct vstate *vs_find(uint32_t vol)
{
    for (int pass = 0; pass < 2; pass++) {
        uint32_t lo = 0, hi = g_nvs;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            if (g_vs[mid].vol < vol)
                lo = mid + 1;
            else
                hi = mid;
        }
        if (lo < g_nvs && g_vs[lo].vol == vol)
            return &g_vs[lo];
        if (pass == 0)
            vs_sync();
    }
    return NULL;
}

static void admin_volume_force(struct conn *c, uint32_t reqid, struct rd *r)
{
    uint32_t number = get32(r), node = get32(r);
    char text[512];
    if (r->bad) {
        admin_reply(c->serial, reqid, META_ERR_INVALID, NULL, 0);
        return;
    }
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER || !C.sm.initialised) {
        const char *lp = leader_peer();
        admin_reply(c->serial, reqid, C.sm.initialised || C.have_node ? CLUSTER_ERR_NOTLEADER : CLUSTER_ERR_STATE,
                    lp, strlen(lp));
        return;
    }
    const struct meta_volume *v = meta_volume(&C.sm, number);
    if (!v) {
        admin_reply(c->serial, reqid, META_ERR_NOTFOUND, NULL, 0);
        return;
    }
    int x = -1;
    for (uint32_t i = 0; i < v->nrep; i++)
        if (v->rep[i].node == node)
            x = (int)i;
    const struct meta_member *nm = meta_member(&C.sm, node);
    if (v->kind != META_VOL_REPLICATED || x < 0 || !nm || nm->state != META_UP || meta_volume_in_isr(v, node)) {
        int n = snprintf(text, sizeof text, "node %" PRIu32 " is not an up replica of volume %" PRIu32
                         " outside its ISR", node, number);
        admin_reply(c->serial, reqid, META_ERR_INVALID, text, (size_t)n);
        return;
    }
    for (uint32_t i = 0; i < v->nisr; i++) {
        const struct meta_member *m = meta_member(&C.sm, v->isr[i]);
        if (m && m->state == META_UP && cl_mutant() != CLM_FORCEUP) {
            int n = snprintf(text, sizeof text, "ISR member %" PRIu32 " of volume %" PRIu32 " is up: the promotion "
                             "does not need an operator", v->isr[i], number);
            admin_reply(c->serial, reqid, CLUSTER_ERR_STATE, text, (size_t)n);
            return;
        }
    }
    uint32_t ids[META_REPLICAS_MAX];
    uint64_t rids[META_REPLICAS_MAX];
    uint32_t n = 0;
    for (uint32_t i = 0; i < v->nrep; i++)
        if (meta_volume_in_isr(v, v->rep[i].node) || (int)i == x) {
            ids[n] = v->rep[i].node;
            rids[n++] = v->rep[i].rid;
        }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_isr_change(&e, number, v->epoch, v->isr_version, ids, rids, n, 0);
    if (propose(&e, 0, 0, 0) != ISO_OK) {
        admin_reply(c->serial, reqid, CLUSTER_ERR_RETRY, NULL, 0);
        return;
    }
    struct vstate *vs = vs_find(number);
    if (vs)
        vs->forced = 1, vs->forced_node = node, vs->forced_epoch = v->epoch;
    int tl = snprintf(text, sizeof text, "volume %" PRIu32 ": node %" PRIu32 " joins the ISR of epoch %" PRIu64
                      " and is promoted (unclean). The last shrink's commit point is %" PRIu64 "; writes "
                      "acknowledged past node %" PRIu32 "'s log end are lost (the promotion's log line compares the two)",
                      number, node, v->epoch, v->isr_commit, node);
    say("%s", text);
    admin_reply(c->serial, reqid, 0, text, (size_t)tl);
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
    case CLUSTER_OP_VOLUME_ADD: admin_volume_add(c, reqid, r); break;
    case CLUSTER_OP_USER_PUT: admin_user_change(c, reqid, r, 0); break;
    case CLUSTER_OP_USER_DEL: admin_user_change(c, reqid, r, 1); break;
    case CLUSTER_OP_USER_GET: admin_user_get(c, reqid, r); break;
    case CLUSTER_OP_VOLUME_FORCE: admin_volume_force(c, reqid, r); break;
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
        pthread_mutex_lock(&self_mu);
        memcpy(C.uuid, uuid, 16);
        C.id = id;
        pthread_mutex_unlock(&self_mu);
        C.incarnation = 1;
        snprintf(C.seeds, sizeof C.seeds, "%s", seeds);
        if (node_save() != 0)
            fail_stop("cannot write %s: %s", C.nodefile, strerror(errno));
        pthread_mutex_lock(&self_mu);
        C.have_node = 1;
        pthread_mutex_unlock(&self_mu);
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

/* the metadata (or its watched copy) names `id` as the server with this identity */
static int member_ident_ok(uint32_t id, const uint8_t ident[16])
{
    const struct meta_member *m = meta_member(&C.sm, id);
    if (!m && C.wsm_on)
        m = meta_member(&C.wsm, id);
    return m && !zero16(m->ident) && !memcmp(m->ident, ident, 16);
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
        uint8_t kind = get8(r);
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
        if (kind != PL_KIND_META) {
            /* a data link (peerlink.h): only a member of this cluster, by its id and identity; the handshake goes on
             * on the link's own thread, and this thread never does data I/O */
            if (!pl_of_kind(kind) || !C.have_node || !id || zero16(uuid) || memcmp(uuid, C.uuid, 16) != 0 ||
                !member_ident_ok(id, ident)) {
                say("refused a data link from %s (node %" PRIu32 ", kind %u)", c->peer, id, (unsigned)kind);
                c->dead = 1;
                return;
            }
            c->handoff = 1;
            c->kind = kind;
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

static void on_fence(struct conn *c, struct rd *r);
static void on_volmsg(struct conn *c, struct rd *r);

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
    case F_HB_ACK: on_hb_ack(c, &r); break;
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
    case F_FENCE: on_fence(c, &r); break;
    case F_VOLMSG: on_volmsg(c, &r); break;
    default: c->dead = 1; break;
    }
}

/* the socket of a data link goes to peerlink with the HELLO it sent (c->tr); this connection ends either way */
static void conn_handoff(struct conn *c)
{
    struct pl_self self = {.id = C.id};
    memcpy(self.uuid, C.uuid, 16);
    memcpy(self.ident, C.ident, 16);
    struct peerlink *pl = pl_of_kind(c->kind);
    c->handoff = 0;
    if (pl && peerlink_adopt(pl, c->fd, c->tr, c->trlen, &self) == 0)
        c->fd = -1; /* the link's thread owns it now */
    c->dead = 1;
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
        if (c->handoff) {
            conn_handoff(c);
            break;
        }
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
    if (c->fd >= 0)
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
    if (!atomic_load(&C.claim_bad))
        f |= META_MF_CLAIM_OK;
    return f;
}

static void put_sections(struct buf *b, uint64_t now)
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
        put64(b, v->rid);
        put64(b, v->open_gen);
        put64(b, v->open_nonce);
    }
    sec_end(b, at);
    at = sec_begin(b, HBS_DOMAINS);
    put32(b, rep.ndoms);
    for (uint32_t i = 0; i < rep.ndoms; i++) {
        putstr(b, rep.doms[i].name);
        put8(b, rep.doms[i].state);
    }
    sec_end(b, at);
    at = sec_begin(b, HBS_LEASE);
    put64(b, now);
    sec_end(b, at);
    if (rep.nunreach > CLUSTER_REPORT_UNREACH)
        rep.nunreach = 0;
    at = sec_begin(b, HBS_UNREACH);
    put32(b, rep.nunreach);
    for (uint32_t i = 0; i < rep.nunreach; i++) {
        put32(b, rep.unreach[i].node);
        put32(b, rep.unreach[i].age_ms);
    }
    sec_end(b, at);
    if (rep.nisr > CLUSTER_REPORT_ISR)
        rep.nisr = 0;
    at = sec_begin(b, HBS_ISR);
    put32(b, rep.nisr);
    for (uint32_t i = 0; i < rep.nisr; i++) {
        const struct cluster_isr_req *q = &rep.isr[i];
        uint32_t n = q->nisr > META_REPLICAS_MAX ? 0 : q->nisr;
        put32(b, q->volume);
        put64(b, q->epoch);
        put64(b, q->isr_version);
        put64(b, q->commit);
        put32(b, n);
        for (uint32_t k = 0; k < n; k++) {
            put32(b, q->node[k]);
            put64(b, q->rid[k]);
        }
    }
    sec_end(b, at);
}

static void heartbeat(uint64_t now)
{
    if (!C.have_node)
        return;
    users_push();
    raft_config cfg;
    if (C.r)
        cfg = *raft_get_config(C.r);
    else if (!config_of(&C.sm, &cfg))
        return;
    if (!loop_alive(now))
        return; /* the serving loop hangs: this server is not heard, not even by itself, and its lease runs out */
    heard_set(C.id, now, C.incarnation, C.sm.applied, C.adv, C.padv);
    if (core_lease_until(now)) { /* the Raft leader's own lease */
        const struct meta_settings *s = settings();
        heard_get(C.id)->answered_ns = now;
        lease_extend(cluster_lease_end(now, s->lease_ms, s->drift_ppm));
    }
    frame_begin(&C.fb, F_HB);
    put32(&C.fb, C.id);
    put64(&C.fb, C.incarnation);
    put64(&C.fb, C.sm.applied);
    putstr(&C.fb, C.adv);
    putstr(&C.fb, C.padv); /* a restart may have moved it (another port, another host) */
    put_sections(&C.fb, now);
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

/* The earliest time (this server's clock) at which a promotion away from server `id` is safe: one lease with the drift
 * margin after the last answer that extended its lease, and never before one lease into this leadership (an earlier
 * Raft leader's answers are unknown here). For r3e-failover; member_states marks no server down before it. */
static uint64_t promote_safe_ns(uint32_t id)
{
    const struct meta_settings *s = settings();
    const struct heard *h = heard_get(id);
    uint64_t a = C.leader_since;
    if (h && h->answered_ns > a)
        a = h->answered_ns;
    return cluster_lease_safe(a, s->lease_ms, s->drift_ppm);
}

static void member_states(uint64_t now)
{
    const struct meta_settings *s = settings();
    uint64_t to = (uint64_t)s->node_timeout_ms * MS;
    uint64_t wait = cluster_lease_safe(0, s->lease_ms, s->drift_ppm);
    if (cl_mutant() == CLM_NOPROMOTEWAIT)
        wait = 0;
    /* A new leader does not know when the others last heard from anyone, nor the leases given before: it proposes
     * nothing but a member it hears itself coming back (DOWN, or a new incarnation: a restart, whose volumes open
     * only once the group holds that incarnation) until the wait is over. A restarted volume leader resigns only
     * then, and its restart often is what made this server the Raft leader (test_cluster resign). */
    int settled = since(now, C.leader_since) >= (wait > to ? wait : to);
    if (!settled && cl_mutant() == CLM_UPWAITS)
        return;
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
        if (!settled && !(fresh && inc >= minc && (st == META_DOWN || inc > minc)))
            continue;
        if (st == META_UP && !fresh && (cl_mutant() == CLM_NOPROMOTEWAIT || now >= promote_safe_ns(id)))
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

/* Pushes this server's replicated volume records to the volume manager whenever the metadata copy moved. */
static void vol_watch_duty(void)
{
    if (!g_vwatch || !C.sm.initialised || C.sm.applied == g_vwatch_applied)
        return;
    /* Only a copy that holds this run's own incarnation (the leader's MEMBER_STATE up for it, committed after this
     * process started) is known to hold every REPLICA_OPEN committed before the restart: the volume manager judges a
     * directory's generation against it (an older copy of v<N> must not pass on a copy that has not seen the open it
     * predates), so nothing is pushed before. Without a quorum nothing is: the volumes stay RECOVERING. */
    const struct meta_member *me = meta_member(&C.sm, C.id);
    if (!me || me->incarnation != C.incarnation)
        return;
    g_vwatch_applied = C.sm.applied;
    static struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS], prev[CLUSTER_REPORT_VOLS];
    static uint32_t said_over, nprev, nlast;
    uint32_t n = 0, over = 0;
    int hold = C.drop_fence && access(C.dropfencefile, F_OK) == 0, held = 0;
    if (hold) {
        memcpy(prev, recs, nlast * sizeof *recs);
        nprev = nlast;
    }
    for (uint32_t i = 0; i < C.sm.nvolumes; i++) {
        const struct meta_volume *v = &C.sm.volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        for (uint32_t k = 0; k < v->nrep; k++) {
            if (v->rep[k].node != C.id)
                continue;
            if (n == CLUSTER_REPORT_VOLS) {
                over++;
                break;
            }
            struct cluster_vol_rec *r = &recs[n++];
            memset(r, 0, sizeof *r);
            r->number = v->number;
            snprintf(r->name, sizeof r->name, "%s", v->name);
            memcpy(r->uuid, v->uuid, 16);
            memcpy(r->cluster, C.uuid, 16);
            r->leader = v->leader;
            r->epoch = v->epoch;
            const struct meta_member *lm = meta_member(&C.sm, v->leader);
            snprintf(r->leader_addr, sizeof r->leader_addr, "%s", lm ? lm->addr : "");
            r->rid = v->rep[k].rid;
            r->open_gen = v->rep[k].open_gen;
            r->open_nonce = v->rep[k].open_nonce;
            r->nrep = v->nrep;
            for (uint32_t j = 0; j < v->nrep && j < META_REPLICAS_MAX; j++) {
                const struct meta_member *mm = meta_member(&C.sm, v->rep[j].node);
                r->rep[j].node = v->rep[j].node;
                r->rep[j].rid = v->rep[j].rid;
                r->rep[j].learner = v->rep[j].learner;
                r->rep[j].unanswered = v->rep[j].unanswered;
                snprintf(r->rep[j].peer, sizeof r->rep[j].peer, "%s", mm ? mm->peer : "");
            }
            r->nisr = v->nisr;
            memcpy(r->isr, v->isr, sizeof r->isr);
            r->isr_version = v->isr_version;
            r->no_owner = 1;
            for (uint32_t j = 0; j < v->nisr; j++) {
                const struct meta_member *im = meta_member(&C.sm, v->isr[j]);
                if (im && im->state == META_UP)
                    r->no_owner = 0;
            }
            r->min_isr = v->min_isr;
            r->isr_lag_ms = C.sm.set.isr_lag_ms;
            r->unanswered_ms = C.sm.set.unanswered_ms;
            r->repl_retain_mb = C.sm.set.repl_retain_mb;
            r->repl_retain_s = C.sm.set.repl_retain_s;
            r->idle_seal_ms = C.sm.set.idle_seal_ms;
            for (uint32_t j = 0; hold && j < nprev; j++) /* drop_fence_file: a volume this server led stays so here */
                if (prev[j].number == r->number && prev[j].leader == C.id && r->epoch > prev[j].epoch) {
                    *r = prev[j];
                    held = 1;
                }
            break;
        }
    }
    if (held)
        g_vwatch_applied = 0; /* pushed again every round until the file goes */
    nlast = n;
    if (over > said_over) /* the heartbeat's report has room for this many: the rest are never made or opened here */
        say("holds replicas of %" PRIu32 " volumes more than the %d this server makes, opens and reports; those past"
            " volume %" PRIu32 " are not opened", over, CLUSTER_REPORT_VOLS, recs[n - 1].number);
    said_over = over > said_over ? over : said_over;
    g_vwatch(g_vwatch_arg, C.id, recs, n);
}

/* REPLICA_OPEN (§8.5): a replica raises its directory's generation at every open and reports it; the leader commits
 * it, and the replica serves only once its copy shows the generation. A proposal is retried once a second while the
 * report stays above the record (a lost leadership or a full pipeline drops it); the apply refuses a stale one. */
static uint64_t opens_ns;
static void propose_opens(uint64_t now)
{
    if (since(now, opens_ns) < 1000 * MS && opens_ns)
        return;
    opens_ns = now;
    if (C.hold_opens && access(C.holdopensfile, F_OK) == 0)
        return; /* tests: a replica that opened before the group recorded its generation is caught here */
    static struct cluster_report own;
    int have_own = 0;
    for (uint32_t i = 0; i < C.sm.nvolumes && C.r && raft_get_role(C.r) == RAFT_LEADER; i++) {
        const struct meta_volume *v = &C.sm.volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        for (uint32_t k = 0; k < v->nrep; k++) {
            const struct meta_replica *rp = &v->rep[k];
            const struct cluster_vol_report *vr = NULL;
            if (rp->node == C.id) {
                if (!have_own) {
                    memset(&own, 0, sizeof own);
                    if (g_reporter)
                        g_reporter(g_reporter_arg, &own);
                    have_own = 1;
                }
                for (uint32_t j = 0; j < own.nvols && !vr; j++)
                    if (own.vols[j].volume == v->number)
                        vr = &own.vols[j];
            } else {
                struct heard *h = heard_get(rp->node);
                for (uint32_t j = 0; h && j < h->nvols && !vr; j++)
                    if (h->vols[j].v.volume == v->number)
                        vr = &h->vols[j].v;
            }
            if (!vr || vr->rid != rp->rid || vr->open_gen <= rp->open_gen)
                continue;
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_replica_open(&e, v->number, rp->node, rp->rid, vr->open_gen, vr->open_nonce);
            if (propose(&e, 0, 0, 0) == ISO_OK)
                say("proposes volume %" PRIu32 " replica on node %" PRIu32 " open generation %" PRIu64, v->number,
                    rp->node, vr->open_gen);
        }
    }
}

/* ISR changes (design-cluster.md 6.1, 8.1 step 5): the leader of a replicated volume reports the change its core asks
 * for in its heartbeats (HBS_ISR) until its record moves; the Raft leader proposes ISR_CHANGE with the request's own
 * basis (epoch, ISR version), so the apply refuses one that is no longer current (META_ERR_STALE), and with the
 * leader's commit point. Only the record's leader is listened to. A request is proposed again after ISR_RETRY_NS while
 * the record has not moved (a lost leadership or a full pipeline drops a proposal). */
#define ISR_RETRY_NS (1000 * MS)
struct isr_sent {
    struct cluster_isr_req q;
    uint64_t ns;
    int replayed;
};
static struct isr_sent isr_sent[CLUSTER_REPORT_ISR];
static uint32_t nisr_sent;

static void propose_isr_one(const struct cluster_isr_req *q, uint64_t epoch, uint64_t version)
{
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_isr_change(&e, q->volume, epoch, version, q->node, q->rid, q->nisr, q->commit);
    if (propose(&e, 0, 0, 0) == ISO_OK)
        say("proposes volume %" PRIu32 " ISR of %" PRIu32 " members (epoch %" PRIu64 ", ISR version %" PRIu64
            ", commit point %" PRIu64 ")", q->volume, q->nisr, epoch, version, q->commit);
}

static int isr_same_members(const struct cluster_isr_req *a, const struct cluster_isr_req *b)
{
    if (a->nisr != b->nisr)
        return 0;
    for (uint32_t k = 0; k < a->nisr && k < META_REPLICAS_MAX; k++)
        if (a->node[k] != b->node[k] || a->rid[k] != b->rid[k])
            return 0;
    return 1;
}

static int cut_move(const struct meta_volume *v, const struct cluster_isr_req *q, uint64_t now);

static void propose_isr(uint64_t now)
{
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER)
        return;
    if (C.hold_isr && access(C.holdisrfile, F_OK) == 0)
        return; /* tests: a change the metadata group has not committed must hold the commit point back */
    static struct cluster_report own;
    int have_own = 0;
    for (uint32_t i = 0; i < C.sm.nvolumes; i++) {
        const struct meta_volume *v = &C.sm.volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        const struct cluster_isr_req *qs = NULL;
        uint32_t nq = 0;
        if (v->leader == C.id) {
            if (!have_own) {
                memset(&own, 0, sizeof own);
                if (g_reporter)
                    g_reporter(g_reporter_arg, &own);
                have_own = 1;
            }
            qs = own.isr;
            nq = own.nisr > CLUSTER_REPORT_ISR ? 0 : own.nisr;
        } else {
            struct heard *h = heard_get(v->leader);
            if (h) {
                qs = h->isr;
                nq = h->nisr;
            }
        }
        for (uint32_t j = 0; j < nq; j++) {
            const struct cluster_isr_req *q = &qs[j];
            if (q->volume != v->number || q->nisr == 0 || q->nisr > META_REPLICAS_MAX)
                continue;
            int nocond = cl_mutant() == CLM_SHRINKNOCOND;
            if (!nocond && (q->epoch != v->epoch || q->isr_version != v->isr_version))
                continue; /* the leader has not seen the record move yet; its next heartbeat drops it */
            if (cut_move(v, q, now))
                continue; /* the leader is the one cut off: it steps aside rather than shrink the others out */
            struct isr_sent *s = NULL;
            for (uint32_t k = 0; k < nisr_sent && !s; k++)
                if (isr_sent[k].q.volume == q->volume && isr_sent[k].q.epoch == q->epoch &&
                    isr_sent[k].q.isr_version == q->isr_version)
                    s = &isr_sent[k];
            /* the same members on the same basis: a resend only after ISR_RETRY_NS. The commit point is left out of
             * the comparison: during a grow under writes it moves at every heartbeat, and each move would otherwise
             * be a new proposal (refused as stale, but a Raft entry each; r3d-isr review L1) */
            if (s && isr_same_members(&s->q, q) && since(now, s->ns) < ISR_RETRY_NS)
                continue;
            if (!s) {
                if (nisr_sent == CLUSTER_REPORT_ISR)
                    nisr_sent = 0; /* a cache of what went out recently, nothing more: forgetting costs a resend */
                s = &isr_sent[nisr_sent++];
            }
            s->q = *q;
            s->ns = now;
            s->replayed = 0;
            propose_isr_one(q, nocond ? v->epoch : q->epoch, nocond ? v->isr_version : q->isr_version);
        }
    }
    /* tests (isr_replay=1): a late duplicate of a request whose record has moved on since */
    for (uint32_t k = 0; C.isr_replay && k < nisr_sent; k++) {
        struct isr_sent *s = &isr_sent[k];
        const struct meta_volume *v = NULL;
        for (uint32_t i = 0; i < C.sm.nvolumes && !v; i++)
            if (C.sm.volumes[i].number == s->q.volume)
                v = &C.sm.volumes[i];
        if (!v || s->replayed || v->isr_version < s->q.isr_version + 2)
            continue;
        s->replayed = 1;
        int nocond = cl_mutant() == CLM_SHRINKNOCOND;
        say("test: replays an ISR request of volume %" PRIu32 " on ISR version %" PRIu64, s->q.volume,
            s->q.isr_version);
        propose_isr_one(&s->q, nocond ? v->epoch : s->q.epoch, nocond ? v->isr_version : s->q.isr_version);
    }
}

/* ------------------------------------------------------------------ promotion (design-cluster.md 6.2, r3e-failover)
 *
 * The Raft leader keeps, in memory, one promotion per replicated volume whose leader is gone: the record leader DOWN
 * (UNHEARD: member_states marks it so only after promote_safe_ns, so its lease has run out), its heartbeat reporting
 * the volume LOCAL_FAULT (FENCED, D13: the driver stopped it and the volume answers UNAVAILABLE, never a read), or
 * its RESIGN of the record's epoch (RESIGNED: a restarted leader never resumes, `led` in REPL). It sends FENCE(v, e+1)
 * to every ISR member (resent every PROMO_RESEND_NS until it answers; this server's own through the handler), and
 * proposes VOLUME_LEADER on the basis (epoch, ISR version) once every member it hears has answered, or after
 * PROMO_WAIT_NS with one answer at least. A live old leader (RESIGNED) is not passed by until it answered (its core is
 * deposed by the fence) or its lease ran out (promote_safe_ns): until then on_hb grants it no lease. The promotion
 * goes once the record moves past its basis, or after PROMO_GIVEUP_NS (a new one starts on the record then). No answer
 * at all: NO_OWNER, said once (`tsdb cluster volume force-leader`).
 *
 * A move (PLANNED, design-cluster.md 6.1): the leader asks for an ISR below 2 while a member it would drop is heard
 * here and its heartbeat says it cannot reach the leader on the data links (a cut, not a slow disk). The Raft leader
 * then holds the shrink and promotes as above, the old leader's own answer not a candidate (its tail past the replicas'
 * was never committed), and the old leader not passed by until it answered or its lease ran out. A move that finds no
 * candidate within PROMO_MOVE_NS gives up for that basis, and the shrink goes ahead; one a candidate answered waits for
 * the old leader's answer or the end of its lease, past PROMO_GIVEUP_NS if need be (on_hb grants it none meanwhile:
 * dropped and started again, the promotion would let it renew its lease in between and wait for that one, test_cluster
 * oneway). A member reports a peer
 * unreachable only after a second (cvol.c UNREACH_MS), about when its leader asks to drop it, so a shrink below 2 that
 * would drop a member heard here waits up to CUT_HOLD_NS for that report: a slow disk's shrink to one copy costs that. */
#define PROMO_RESEND_NS (100 * MS)
#define PROMO_MOVE_NS (2000 * MS)
#define CUT_HOLD_NS (2000 * MS)
#define PROMO_WAIT_NS (500 * MS)
#define PROMO_GIVEUP_NS (5000 * MS)
#define VMSG_Q 256

struct promo {
    uint32_t vol, old;
    uint64_t epoch, version;
    uint8_t reason;
    uint32_t nfenced, fenced[META_REPLICAS_MAX];
    uint8_t replied[META_REPLICAS_MAX];
    struct meta_fence_reply reply[META_REPLICAS_MAX];
    uint64_t t0, sent_ns, proposed_ns;
    int said;
};

static pthread_mutex_t vmsg_mu = PTHREAD_MUTEX_INITIALIZER; /* the queue of cluster_vol_msg: any thread; a leaf */
static struct cluster_vmsg vmsg_q[VMSG_Q];
static unsigned vmsg_n;
static cluster_fence_fn g_fence_fn;
static void *g_fence_arg;
static struct promo *g_promo; /* the cluster thread's */
static size_t g_npromo, g_promo_cap;
static unsigned g_nresign; /* RESIGNs noted in the per-volume state since the last duty round */

void cluster_set_fence_handler(cluster_fence_fn fn, void *arg)
{
    g_fence_fn = fn;
    g_fence_arg = arg;
}

/* queued; the cluster thread sends it at the top of its next round (at most a poll timeout, 50 ms, later): the reply
 * of a fence is not on any write's path */
void cluster_vol_msg(const struct cluster_vmsg *m)
{
    pthread_mutex_lock(&vmsg_mu);
    if (vmsg_n < VMSG_Q)
        vmsg_q[vmsg_n++] = *m; /* full: dropped, the proposer resends its fence and the core its RESIGN */
    pthread_mutex_unlock(&vmsg_mu);
}

static struct promo *promo_find(uint32_t vol)
{
    struct vstate *vs = vs_find(vol);
    if (vs && vs->promo && vs->promo <= g_npromo && g_promo[vs->promo - 1].vol == vol)
        return &g_promo[vs->promo - 1];
    for (size_t i = 0; i < g_npromo; i++) /* the hint is set where a promotion starts or moves; this is a belt */
        if (g_promo[i].vol == vol) {
            if (vs)
                vs->promo = (uint32_t)i + 1;
            return &g_promo[i];
        }
    return NULL;
}

static void volmsg_in(uint32_t from, const struct cluster_vmsg *m)
{
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER)
        return;
    if (m->kind == CL_VMSG_RESIGN) {
        const struct meta_volume *v = meta_volume(&C.sm, m->volume);
        struct vstate *vs = v && v->leader == from && v->epoch == m->epoch ? vs_find(m->volume) : NULL;
        if (vs) { /* only the record's leader resigns its epoch; promote_duty checks again */
            vs->resigned = 1, vs->resign_from = from, vs->resign_epoch = m->epoch;
            g_nresign++;
        }
        return;
    }
    struct promo *p = promo_find(m->volume);
    /* only an answer to this promotion's fence: a late one to an earlier fence (the old leader's, from when it was
     * promoted) says nothing of whether it stopped at this epoch */
    if (!p || m->kind != CL_VMSG_FENCE_REPLY || (m->epoch != p->epoch + 1 && cl_mutant() != CLM_ANYEPOCH))
        return;
    for (uint32_t i = 0; i < p->nfenced; i++)
        if (p->fenced[i] == from && !p->replied[i]) {
            p->replied[i] = 1;
            p->reply[i] = (struct meta_fence_reply){from, m->rid, m->durable, m->last_epoch};
        }
}

static void vmsg_send(const struct cluster_vmsg *m, uint32_t to)
{
    if (to == C.id) {
        volmsg_in(C.id, m);
        return;
    }
    struct conn *c = conn_to(to);
    if (!c)
        return;
    frame_begin(&C.fb, F_VOLMSG);
    put8(&C.fb, m->kind);
    put32(&C.fb, m->volume);
    put64(&C.fb, m->epoch);
    put64(&C.fb, m->durable);
    put64(&C.fb, m->last_epoch);
    put64(&C.fb, m->rid);
    put64(&C.fb, m->start);
    frame_end(&C.fb);
    conn_send(c, 0);
}

static void vmsg_flush(void)
{
    struct cluster_vmsg q[VMSG_Q];
    static struct cluster_vmsg held[8]; /* tests (hold_reply_file=1), with `to` resolved */
    static unsigned nheld;
    pthread_mutex_lock(&vmsg_mu);
    unsigned n = vmsg_n;
    memcpy(q, vmsg_q, n * sizeof q[0]);
    vmsg_n = 0;
    pthread_mutex_unlock(&vmsg_mu);
    int hold = C.hold_reply && access(C.holdreplyfile, F_OK) == 0;
    for (unsigned i = 0; i < n; i++) {
        uint32_t to = q[i].to ? q[i].to : C.r ? raft_get_leader(C.r) : C.hint_leader;
        if (hold && q[i].kind == CL_VMSG_FENCE_REPLY && nheld < sizeof held / sizeof held[0]) {
            held[nheld] = q[i];
            held[nheld++].to = to;
            say("test: keeps a copy of its answer to fence %" PRIu64 " of volume %" PRIu32, q[i].epoch, q[i].volume);
        }
        vmsg_send(&q[i], to);
    }
    if (C.hold_reply && access(C.replayreplyfile, F_OK) == 0)
        for (unsigned i = 0; i < nheld; i++)
            vmsg_send(&held[i], held[i].to);
}

static void on_volmsg(struct conn *c, struct rd *r)
{
    struct cluster_vmsg m = {.kind = get8(r), .volume = get32(r)};
    m.epoch = get64(r);
    m.durable = get64(r);
    m.last_epoch = get64(r);
    m.rid = get64(r);
    m.start = get64(r);
    if (r->bad) {
        c->dead = 1;
        return;
    }
    volmsg_in(c->id, &m);
}

static void on_fence(struct conn *c, struct rd *r)
{
    uint32_t vol = get32(r);
    uint64_t fence = get64(r);
    if (r->bad) {
        c->dead = 1;
        return;
    }
    if (C.drop_fence && access(C.dropfencefile, F_OK) == 0)
        return;
    if (g_fence_fn)
        g_fence_fn(g_fence_arg, vol, fence, c->id);
}

/* on_hb: no lease for `node` while a promotion away from it waits for its answer, nor while it reports itself the
 * writer (OWNER) of a replicated volume whose record names another leader (stale reads, r3e-leases F5): its driver
 * reports OWNER only while its core leads (repld_leading), so a deposed writer that has not yet heard of its fence, or a
 * Raft leader elected after the promotion with no promotion state of its own, still gives it no lease */
static int promo_holds_lease(uint32_t node)
{
    const struct heard *h = heard_get(node);
    for (uint32_t i = 0; h && i < h->nvols; i++) {
        const struct cluster_vol_report *r = &h->vols[i].v;
        const struct meta_volume *v = r->role == CL_ROLE_OWNER ? meta_volume(&C.sm, r->volume) : NULL;
        if (v && v->kind == META_VOL_REPLICATED && v->leader != node && cl_mutant() != CLM_LEASESTALE)
            return 1;
    }
    for (size_t i = 0; i < g_npromo && cl_mutant() != CLM_FENCELEASE; i++) {
        const struct promo *p = &g_promo[i];
        if (p->old != node || p->reason == META_GR_FENCED)
            continue;
        for (uint32_t k = 0; k < p->nfenced; k++)
            if (p->fenced[k] == node && !p->replied[k])
                return 1;
    }
    return 0;
}

static void promo_start(const struct meta_volume *v, uint8_t reason, uint64_t now)
{
    if (v->epoch + 1 >= META_EPOCH_LIMIT) {
        static uint32_t said = UINT32_MAX;
        if (said != v->number)
            say("volume %" PRIu32 ": cannot promote past epoch %" PRIu64 " (the limit is %llu)", v->number, v->epoch,
                (unsigned long long)META_EPOCH_LIMIT);
        said = v->number;
        return;
    }
    if (g_npromo == g_promo_cap) {
        g_promo_cap = g_promo_cap ? 2 * g_promo_cap : 8;
        g_promo = realloc(g_promo, g_promo_cap * sizeof *g_promo);
        if (!g_promo)
            oom();
    }
    struct promo *p = &g_promo[g_npromo++];
    memset(p, 0, sizeof *p);
    struct vstate *vs = vs_find(v->number);
    if (vs)
        vs->promo = (uint32_t)g_npromo;
    p->vol = v->number;
    p->old = v->leader;
    p->epoch = v->epoch;
    p->version = v->isr_version;
    p->reason = reason;
    p->t0 = now;
    for (uint32_t i = 0; i < v->nisr && i < META_REPLICAS_MAX; i++)
        p->fenced[p->nfenced++] = v->isr[i];
    say("volume %" PRIu32 ": leader %" PRIu32 " at epoch %" PRIu64 " is gone (reason %u); fences %" PRIu32
        " ISR members for epoch %" PRIu64, v->number, v->leader, v->epoch, (unsigned)reason, p->nfenced, v->epoch + 1);
}

static int heard_fresh(uint32_t id, uint64_t now)
{
    if (id == C.id)
        return 1;
    uint64_t last = heard_ns(id, now);
    return last && since(now, last) < (uint64_t)settings()->node_timeout_ms * MS;
}

/* 1: proposed (or nothing to do any more) */
static int promo_step(struct promo *p, const struct meta_volume *v, uint64_t now)
{
    if (p->proposed_ns)
        return 0;
    if (!p->sent_ns || since(now, p->sent_ns) >= PROMO_RESEND_NS) {
        p->sent_ns = now;
        for (uint32_t i = 0; i < p->nfenced; i++) {
            if (p->replied[i])
                continue;
            if (p->fenced[i] == C.id) {
                if (g_fence_fn)
                    g_fence_fn(g_fence_arg, p->vol, p->epoch + 1, C.id);
                continue;
            }
            struct conn *c = conn_to(p->fenced[i]);
            if (!c)
                continue;
            frame_begin(&C.fb, F_FENCE);
            put32(&C.fb, p->vol);
            put64(&C.fb, p->epoch + 1);
            frame_end(&C.fb);
            conn_send(c, 0);
        }
    }
    uint32_t nrep = 0, waiting = 0, old_ok = p->reason == META_GR_FENCED;
    for (uint32_t i = 0; i < p->nfenced; i++) {
        if (p->replied[i])
            nrep++;
        else if (heard_fresh(p->fenced[i], now))
            waiting++;
        if (p->fenced[i] == p->old && p->replied[i])
            old_ok = 1;
    }
    const struct meta_member *om = meta_member(&C.sm, p->old);
    if (!old_ok && (!om || om->state != META_UP || now >= promote_safe_ns(p->old) || cl_mutant() == CLM_OLDPASS))
        old_ok = 1;
    if (!nrep) {
        if (!p->said && since(now, p->t0) >= PROMO_WAIT_NS && p->reason != META_GR_PLANNED) {
            p->said = 1;
            say("volume %" PRIu32 ": NO_OWNER: no ISR member answers the fence for epoch %" PRIu64
                " (tsdb cluster volume force-leader %" PRIu32 " NODE)", p->vol, p->epoch + 1, p->vol);
        }
        return 0;
    }
    if ((waiting && since(now, p->t0) < PROMO_WAIT_NS) || !old_ok)
        return 0;
    struct meta_grant g;
    memset(&g, 0, sizeof g);
    g.number = p->vol;
    g.epoch = p->epoch;
    g.new_epoch = p->epoch + 1;
    g.isr_version = p->version;
    g.reason = p->reason;
    g.nfenced = p->nfenced;
    memcpy(g.fenced, p->fenced, sizeof g.fenced);
    uint64_t best = 0;
    for (uint32_t i = 0; i < p->nfenced; i++)
        if (p->replied[i] && !((p->reason == META_GR_FENCED || p->reason == META_GR_PLANNED) &&
                               p->fenced[i] == p->old)) { /* not the failed or the cut-off one */
            g.reply[g.nreplies++] = p->reply[i];
            if (p->reply[i].durable > best)
                best = p->reply[i].durable;
        }
    int rc = meta_promote_pick(v, &g, &g.leader);
    if (rc != META_OK) {
        if (!p->said)
            say("volume %" PRIu32 ": no candidate among %" PRIu32 " fence answers (%d)", p->vol, g.nreplies, rc);
        p->said = 1;
        return 0;
    }
    for (uint32_t i = 0; i < g.nreplies; i++)
        if (g.reply[i].node == g.leader)
            best = g.reply[i].durable;
    /* the first position the new leader writes (its EPOCH marker) is past its durable end; recorded, not a condition */
    uint64_t last = v->nhist ? v->hist[v->nhist - 1].start : 0;
    g.start = best + 1 > last + 1 ? best + 1 : last + 1;
    p->proposed_ns = now;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_leader(&e, &g);
    if (propose(&e, 0, 0, 0) == ISO_OK)
        say("proposes volume %" PRIu32 " leader %" PRIu32 " at epoch %" PRIu64 " (%" PRIu32 " of %" PRIu32
            " fenced members answered, %" PRIu64 " ms)", p->vol, g.leader, g.new_epoch, g.nreplies, g.nfenced,
            (uint64_t)(since(now, p->t0) / MS));
    const struct vstate *fs = vs_find(p->vol);
    if (fs && fs->forced && fs->forced_node == g.leader && fs->forced_epoch == p->epoch)
        say("volume %" PRIu32 ": forced leader %" PRIu32 " ends at %" PRIu64 ", the last shrink's commit point is "
            "%" PRIu64 ": %s", p->vol, g.leader, best, v->isr_commit,
            best >= v->isr_commit ? "it holds every write acknowledged before that shrink"
                                  : "writes acknowledged before that shrink are lost");
    return 1;
}

/* a member other than the old leader answered the fence: the promotion has someone to name and may be waiting only for
 * the old leader's answer or the end of its lease */
static int promo_candidate(const struct promo *p)
{
    for (uint32_t i = 0; i < p->nfenced; i++)
        if (p->replied[i] && p->fenced[i] != p->old)
            return 1;
    return 0;
}

static int vol_reports_fault(uint32_t node, const struct meta_volume *v)
{
    const struct heard *h = heard_get(node);
    for (uint32_t j = 0; h && j < h->nvols; j++)
        if (h->vols[j].v.volume == v->number && h->vols[j].v.health == CL_HEALTH_LOCAL_FAULT)
            return 1;
    return 0;
}

/* 1: the shrink q is held for a move of the leadership (see the head of this section) */
static int cut_move(const struct meta_volume *v, const struct cluster_isr_req *q, uint64_t now)
{
    if (q->nisr >= 2 || v->nisr < 2 || !v->leader || cl_mutant() == CLM_CUTSHRINKS)
        return 0;
    if (promo_find(v->number))
        return 1;
    struct vstate *vs = vs_find(v->number);
    if (!vs)
        return 0;
    if (vs->cut_failed && vs->fail_epoch == v->epoch && vs->fail_version == v->isr_version)
        return 0;
    if (!vs->cut_held || vs->cut_epoch != v->epoch || vs->cut_version != v->isr_version)
        vs->cut_held = 1, vs->cut_epoch = v->epoch, vs->cut_version = v->isr_version, vs->cut_ns = now;
    uint64_t held_ns = vs->cut_ns;
    static struct cluster_report own;
    int cut = 0, alive = 0;
    for (uint32_t i = 0; i < v->nisr && !cut; i++) {
        uint32_t m = v->isr[i], kept = 0;
        for (uint32_t k = 0; k < q->nisr && k < META_REPLICAS_MAX; k++)
            kept |= q->node[k] == m;
        if (m == v->leader || kept || !heard_fresh(m, now))
            continue;
        alive = 1;
        const struct cluster_unreach *u = NULL;
        uint32_t nu = 0;
        if (m == C.id) {
            memset(&own, 0, sizeof own);
            if (g_reporter)
                g_reporter(g_reporter_arg, &own);
            u = own.unreach;
            nu = own.nunreach > CLUSTER_REPORT_UNREACH ? 0 : own.nunreach;
        } else {
            const struct heard *h = heard_get(m);
            if (h) {
                u = h->unreach;
                nu = h->nunreach;
            }
        }
        for (uint32_t k = 0; k < nu; k++)
            cut |= u[k].node == v->leader;
    }
    if (!cut) /* a member heard here but not (yet) saying it lost the leader: its report is at most a second late */
        return alive && since(now, held_ns) < CUT_HOLD_NS && cl_mutant() != CLM_NOCUTHOLD;
    promo_start(v, META_GR_PLANNED, now);
    return promo_find(v->number) != NULL;
}

/* The record's leader is up and heard, but its driver does not lead: it answered the fence of a promotion that was then
 * given up (a PLANNED move whose candidates never answered: cut_move's "no replica to move", or a promotion lost with
 * the Raft leader that ran it). Its core holds the higher fence and waits for a record past it that nobody proposes,
 * and the volume has no writer for good (jepsen-cluster meta,classes kill, 2026-11-19: ISR version 158 to the end).
 * Once that has lasted DEPOSED_HOLD_NS with no promotion under way it counts as a resignation: the group fences
 * again and may pick the old leader itself at a new epoch. A restarted leader's own RESIGN takes far less. A leader that
 * reports PROMOTING to the record's epoch is not deposed however long its promotion takes (shipping the log, the EPOCH
 * marker's commit, a reopen of seconds on a large volume or a loaded host: review F3, test_cluster promoteslow). *own is
 * this server's report, gathered at most once per call (the driver's state, cut_move's way). */
#define DEPOSED_HOLD_NS (3000 * MS)
static int leader_deposed(const struct meta_volume *v, uint64_t now, struct cluster_report *own, int *have_own)
{
    int role = -1;
    uint64_t pepoch = 0;
    if (v->leader == C.id) {
        if (!*have_own) {
            memset(own, 0, sizeof *own);
            if (g_reporter)
                g_reporter(g_reporter_arg, own);
            *have_own = 1;
        }
        for (uint32_t i = 0; i < own->nvols && i < CLUSTER_REPORT_VOLS; i++)
            if (own->vols[i].volume == v->number)
                role = own->vols[i].role, pepoch = own->vols[i].epoch;
    } else if (heard_fresh(v->leader, now)) {
        const struct heard *h = heard_get(v->leader);
        for (uint32_t i = 0; h && i < h->nvols; i++)
            if (h->vols[i].v.volume == v->number)
                role = h->vols[i].v.role, pepoch = h->vols[i].v.epoch;
    }
    struct vstate *vs = vs_find(v->number);
    if (!vs)
        return 0;
    if (role == CL_ROLE_PROMOTING && pepoch != v->epoch)
        role = CL_ROLE_NONE; /* promoting to an older epoch: the record moved past it */
    if (role != CL_ROLE_NONE || cl_mutant() == CLM_DEPOSEDSTUCK) {
        vs->dep_seen = 0;
        return 0;
    }
    if (vs->dep_seen && vs->dep_leader == v->leader && vs->dep_epoch == v->epoch)
        return since(now, vs->dep_ns) >= DEPOSED_HOLD_NS;
    vs->dep_seen = 1, vs->dep_leader = v->leader, vs->dep_epoch = v->epoch, vs->dep_ns = now;
    return 0;
}

static void promote_duty(uint64_t now)
{
    static struct cluster_report own;
    int have_own = 0;
    unsigned nres = g_nresign;
    g_nresign = 0;
    if (!C.r || raft_get_role(C.r) != RAFT_LEADER) {
        g_npromo = 0;
        for (uint32_t i = 0; nres && i < g_nvs; i++)
            g_vs[i].resigned = 0;
        return;
    }
    size_t k = 0;
    for (size_t i = 0; i < g_npromo; i++) { /* the record moved past the basis, or the proposal is lost */
        const struct promo *p = &g_promo[i];
        const struct meta_volume *v = meta_volume(&C.sm, p->vol);
        int moved = !v || v->epoch != p->epoch || v->isr_version != p->version;
        int cand = cl_mutant() != CLM_MOVEGIVEUP && promo_candidate(p);
        if (!moved && p->reason == META_GR_PLANNED && !p->proposed_ns && !cand && since(now, p->t0) >= PROMO_MOVE_NS) {
            say("volume %" PRIu32 ": no replica to move the leadership of epoch %" PRIu64 " to; the ISR shrinks",
                p->vol, p->epoch);
            struct vstate *vs = vs_find(p->vol);
            if (vs)
                vs->cut_failed = 1, vs->fail_epoch = p->epoch, vs->fail_version = p->version;
            continue;
        }
        if (!moved && (since(now, p->t0) < PROMO_GIVEUP_NS ||
                       (cand && !p->proposed_ns && now < promote_safe_ns(p->old) + PROMO_GIVEUP_NS))) {
            g_promo[k++] = *p;
            struct vstate *vs = vs_find(p->vol);
            if (vs)
                vs->promo = (uint32_t)k;
        }
    }
    g_npromo = k;
    for (uint32_t i = 0; i < C.sm.nvolumes; i++) {
        const struct meta_volume *v = &C.sm.volumes[i];
        if (v->kind != META_VOL_REPLICATED || !v->nhist || !v->leader)
            continue;
        struct promo *p = promo_find(v->number);
        if (!p) {
            const struct meta_member *m = meta_member(&C.sm, v->leader);
            uint8_t reason = 0;
            if (m && m->state == META_DOWN)
                reason = META_GR_UNHEARD;
            else if (vol_reports_fault(v->leader, v))
                reason = META_GR_FENCED;
            struct vstate *vs = nres ? vs_find(v->number) : NULL;
            if (vs && vs->resigned) {
                if (!reason && vs->resign_from == v->leader && vs->resign_epoch == v->epoch)
                    reason = META_GR_RESIGNED;
                vs->resigned = 0;
            }
            if (!reason && m && m->state == META_UP && leader_deposed(v, now, &own, &have_own)) {
                say("volume %" PRIu32 ": leader %" PRIu32 " of epoch %" PRIu64 " is up but has not led it for %llu ms "
                    "(deposed by a fence no promotion followed); taken as its resignation", v->number, v->leader,
                    v->epoch, (unsigned long long)(DEPOSED_HOLD_NS / MS));
                reason = META_GR_RESIGNED;
            }
            if (!reason)
                continue;
            promo_start(v, reason, now);
            p = promo_find(v->number);
            if (!p)
                continue;
        }
        promo_step(p, v, now);
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
    propose_opens(now);
    propose_isr(now);
    promote_duty(now);
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
    /* A core booted from its files at start knows the others only from its last applied state (often none: it applies
     * nothing before a leader reaches it) and NODE's seeds, and a leader dials it at the peer address the group holds,
     * which a restart elsewhere (--port 0, another host) changed: until the group records the new one nobody reaches
     * it and it reaches nobody, so its new incarnation (its volumes open only then, a volume leader's RESIGN with them)
     * would wait for NOLEADER_PROBE_NS. The watch stream names the others' addresses. It goes on, a leader seen or not,
     * until the core's own state holds this node: a leader's message is not enough (test_cluster reseed: a learner
     * whose log a damaged install emptied heard leader 1 once, stopped watching, and was removed as a learner while its
     * empty state named nobody to answer; never dropping its core, it sat as a follower of no configuration). */
    if (C.probe_at_start && C.r) {
        raft_config own;
        if (raft_get_leader(C.r) && config_of(&C.sm, &own) && in_config(&own, C.id))
            C.probe_at_start = 0;
    }
    int early = C.probe_at_start && cl_mutant() != CLM_PROBELATE;
    int want = !C.r || early || (raft_get_leader(C.r) == 0 && since(now, C.leader_seen) >= NOLEADER_PROBE_NS);
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
        vmsg_flush();
        if (C.r) {
            if (!raft_held())
                raft_tick(C.r, now);
            pump();
            if (C.r && !C.sync_since && raft_log_floor_due(C.log, raft_get_commit(C.r), now))
                sync_round();
            if (C.r)
                sync_stall_check();
        }
        copy_flush(now, 0);
        if (now >= C.next_hb || (now >= C.next_duty && atomic_exchange(&g_beat_soon, 0))) {
            uint32_t hb = settings()->heartbeat_ms;
            C.next_hb = now + (uint64_t)(hb < 10 ? 10 : hb) * MS;
            heartbeat(now);
        }
        if (now >= C.next_duty) {
            C.next_duty = now + 100 * MS;
            leader_duties(now);
            watch_duty(now);
            vol_watch_duty();
            users_duty(now);
            seeds_refresh(now);
            if (atomic_load(&C.copy_drive) && C.r && raft_get_role(C.r) == RAFT_LEADER && C.sm.initialised) {
                struct meta_entry e; /* copy_hold: an entry that changes nothing, one more applied batch */
                meta_entry_init(&e);
                meta_op_settings(&e, settings());
                if (propose(&e, 0, 0, 0) == ISO_OK)
                    atomic_fetch_sub(&C.copy_drive, 1);
            }
            for (size_t i = 0; i < C.nconns; i++) {
                struct conn *c = C.conns[i];
                if ((c->state != CS_READY && since(now, c->opened_ns) > HANDSHAKE_NS) ||
                    (c->join && since(now, c->opened_ns) > JOIN_NS))
                    c->dead = 1;
            }
        }
        copy_publish(); /* whatever moved the copy since the last round: an apply, an install, the watch stream */
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
    atomic_store(&g_users_ok, cl_mutant() == CLM_CAUGHTUPEARLY);
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
    C.loop_gated = cfg->loop_gated != 0;
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
    snprintf(C.holdopensfile, sizeof C.holdopensfile, "%s/holdopens", C.metadir);
    snprintf(C.holdraftfile, sizeof C.holdraftfile, "%s/holdraft", C.metadir);
    snprintf(C.holdisrfile, sizeof C.holdisrfile, "%s/holdisr", C.metadir);
    snprintf(C.dropfencefile, sizeof C.dropfencefile, "%s/dropfence", C.metadir);
    snprintf(C.holdreplyfile, sizeof C.holdreplyfile, "%s/holdreply", C.metadir);
    snprintf(C.replayreplyfile, sizeof C.replayreplyfile, "%s/replayreply", C.metadir);
    snprintf(C.failfile, sizeof C.failfile, "%s/failsync", C.metadir);
    test_knobs();
    if (C.users_probe)
        cluster_set_users_watch(users_probe_watch, NULL);
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
        } else if (exists(C.snapfile) || exists(C.logdir)) {
            if (core_boot(err, sizeof err) != 0) {
                fprintf(stderr, "tsdbd: cluster: cannot start the Raft core: %s\n", err);
                return -1;
            }
            C.probe_at_start = 1;
        }
        if (exists(C.copyfile)) {
            /* the copy of the last run, served stale until a watch stream (or, for a voter, its core) confirms it
             * (item 8) */
            uint8_t *img = NULL;
            size_t n = 0;
            struct meta_state t;
            meta_init(&t);
            if (load_image(C.copyfile, &t, &img, &n, err, sizeof err) != 0) {
                fprintf(stderr, "tsdbd: cluster: %s\n", err);
                return -1;
            }
            free(img);
            C.copy_applied = t.applied;
            C.copy_gen = t.restore_gen;
            if (!C.r) {
                meta_free(&C.sm);
                C.sm = t;
                C.copy_stale = 1;
            } else if (state_after(&t, &C.sm)) {
                C.cpy = t; /* a voter: its core has applied less since the restart than it had */
                C.cpy_on = 1;
            } else {
                meta_free(&t);
            }
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
    if (C.copy_hold && pthread_create(&C.copy_hold_th, NULL, cc_hold_main, (void *)(uintptr_t)C.copy_hold) != 0)
        C.copy_hold = 0;
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
    if (C.copy_hold)
        pthread_join(C.copy_hold_th, NULL);
    cc_clear(); /* readers that hold one keep it until they put it */
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
    if (C.cpy_on)
        meta_free(&C.cpy);
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
