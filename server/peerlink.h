/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Data links between servers (docs/design-cluster.md C25, section 5.1): per pair of servers a **log link** (small
 * frames, latency first, zstd-1) and a **bulk link** (part files in throttled chunks) of their own, each its own TCP
 * connection to the peer port, so a 100 MB part never delays a small insert. Transport only: the frames are typed
 * opaque payloads of the driver (r3d-stream); nothing here knows a replication message.
 *
 * Wire (server/cluster.c's header has the metadata side): the dialer sends the cluster HELLO with its link kind
 * (PL_KIND_LOG or PL_KIND_BULK; the metadata kind is 0) after the peer address and then the frame check types it
 * offers (u8 bit set: those at or above its level, plcheck.h), the acceptor answers HELLO_ACK with the type it chose
 * (the stronger of the two levels), the dialer AUTH, the MACs exactly as there but under their own labels
 * ("tsdb-link-..."), so a metadata link's MAC is never a data link's: the cluster secret is proven both ways once per
 * connection. From AUTH on every frame is [u32 len][u8 type][payload][check], len counting all after itself, the check
 * as long as its type says: none (the default) 0 bytes, header 8 (XXH3-64 of the length, the type and the payload's
 * first PL_CHECK_SPAN bytes), full 16 (XXH3-128 of type | payload), both seeded with the direction's salt (from the
 * handshake) ^ seq, seq counting from 0 per direction: under them a frame that was changed (in what they cover),
 * dropped, repeated or reordered, or comes from another connection, ends the link. Without a check the wire is trusted
 * to Ethernet's and TCP's checksums, the payloads carry their own (verified by the drivers), and every receive path
 * bounds and cross-checks the fields it acts on (docs/design-rdma.md §6, D1). Integrity only, never a MAC.
 *
 * Frame types: 1..0x6f the driver's (opaque, at most PL_FRAME_MAX); 0x70.. are the link's own (BEGIN, RESUME, CHUNK,
 * END, DONE, PING, PONG; peerlink.c lists them). Bit 0x80 on a frame of any kind but BULK says its payload is [u32 raw
 * length][zstd frame] (raw length at most PL_FRAME_MAX, and the decoded size must equal it).
 *
 * Threads. One I/O thread per link connection (never the cluster thread): the cluster thread's acceptor reads the
 * HELLO, checks the claims it can (uuid, member, identity), and hands the socket over (peerlink_adopt); the thread
 * answers the handshake. Callbacks run on that thread: a slow one stops the reads, TCP stops the peer, and the peer's
 * bounded queue answers PL_E_FULL: a slow peer never grows memory without bound. A peer that is being held back that
 * way (its bytes wait in our socket, the window shut) is not counted silent, so a callback of any length is fine while
 * the peer has more to send. When it has nothing queued it hears nothing back (a callback sends no PONG), and a
 * callback that blocks longer than dead_ms ends the link: the driver's callbacks must not block that long (a slow
 * disk gets a queue and a thread of the driver's own). The receiver never counts its own callbacks' time against
 * the peer. A host that vanishes while the sender is held back by its shut window is found only by the kernel's own
 * retransmission limit (minutes), since from outside that looks the same as a slow reader.
 *
 * Accepting. The handshake is answered first, before the connection touches anything (a peer that fails AUTH changes
 * nothing, whatever id it claims). Then it takes the (peer, kind) link over from the connection that runs it, which
 * leaves; a connection of the same peer that is still waiting for the link is closed by the newer one, so a stuck
 * callback costs one thread and one descriptor, never one per redial. No thread joins another.
 *
 * Shutdown order: stop what hands sockets over (cluster_set_peerlink(NULL) and the cluster thread stopped, or
 * cluster_stop) before peerlink_free; peerlink_free waits for the adopt threads it knows of and refuses new ones, but
 * a peerlink_adopt that starts after it returned would use freed memory. A callback must return for peerlink_free
 * to return.
 *
 * A dialed link (peerlink_dial) is persistent: it connects, and after any failure or a silent peer (PING, dead_ms)
 * reconnects with backoff, until peerlink_free. An accepted link is one connection; a new connection of the same
 * (peer, kind) replaces it. The queue of a link that went down is dropped (the driver's protocol resumes from its own
 * state, on_link tells it); bulk jobs stay and are retried from the receiver's offset.
 */
#ifndef TSDB_PEERLINK_H
#define TSDB_PEERLINK_H

#include <stddef.h>
#include <stdint.h>

#define PL_CL_MAGIC 0x43445354u /* "TSDC": the cluster HELLO's magic (cluster.c) */
#define PL_CL_VERSION 9         /* the cluster protocol version; 4 adds the link kind to HELLO, 5 the lease in
                                 * HB and HB_ACK (r3e-leases), 6 the replication messages' set version and replay
                                 * start (replwire RW_VERSION 2, r4-core): a peer of 5 would drop every frame unheard;
                                 * 7 R4's F_VOLCMD and VOLMSG READY (r4-iface): a peer of 6 drops the connection that
                                 * carries Raft at the first VOLCMD; 8 the data links' frame check (XXH3-128 in place
                                 * of HMAC-SHA256, its type negotiated in HELLO and HELLO_ACK; rd-mac), and the
                                 * capability byte after it in both (rd-seam); 9 the check an option of three levels
                                 * (none by default: no check bytes; rd-nocheck): an 8 peer is refused naming both */
/* The capability bits of a data link's HELLO and HELLO_ACK (one byte after the check types; bits a build does not know
 * are ignored, so a later build of the same cluster version adds one without a version bump). PL_CAP_RDMA: this build
 * decodes RDMA_OFFER (docs/design-rdma.md §5.1 step 1); rd-upgrade sets it, a build before it sends 0 and is never
 * offered an upgrade. */
#define PL_CAP_RDMA 0x01u
#define PL_CAPS 0u /* the bits this build sends */

/* LOG and BULK are the replication driver's (server/repld.c); FWD (forwarded requests and their replies) and FWDQ
 * (forwarded queries and their streamed replies under CREDIT) are server/fwd.c's, on a peerlink of its own, so a
 * forwarded insert never waits behind replication traffic (ha.md 9.1). The kinds need no new cluster version: a
 * server of this version that does not know a kind refuses its HELLO with a line (cluster.c's acceptor table,
 * peerlink_adopt), and the dialer redials with backoff. */
/* CHUNK (requests to a member's chunk stores and their answers) and CHUNKB (chunk puts as bulk files, under a rate cap
 * of their own) are server/chunksrv.c's, on a peerlink of its own as well (design-cluster-data.md §5.7, R8 D10); 5 is
 * R4's snapshot transfer. */
/* SNAP carries snapshots only (R4 plan D7: a COMMIT's file never waits behind a snapshot's), on the replication
 * driver's peerlink (repld.c hands its frames and files to server/rsnap.c). */
enum { PL_KIND_META = 0, PL_KIND_LOG = 1, PL_KIND_BULK = 2, PL_KIND_FWD = 3, PL_KIND_FWDQ = 4, PL_KIND_SNAP = 5,
       PL_KIND_CHUNK = 6, PL_KIND_CHUNKB = 7 };
#define PL_KIND_DATA_MAX PL_KIND_CHUNKB /* data kinds are 1 .. this */

enum {
    PL_OK = 0,
    PL_E_FULL = -1,    /* the queue is full (nothing was queued) */
    PL_E_DOWN = -2,    /* no connection now */
    PL_E_TOOBIG = -3,  /* payload above PL_FRAME_MAX */
    PL_E_INVALID = -4, /* a bad argument, or a type the driver may not use */
    PL_E_HASH = -5,    /* the file's xxh64 did not match at the receiver */
    PL_E_REFUSED = -6, /* the receiver refused the file */
    PL_E_IO = -7,      /* reading or writing the file failed */
    PL_E_CLOSED = -8,  /* the link or the peerlink is being closed */
    PL_E_GIVEUP = -9,  /* the job failed in max_attempts connections */
};

#define PL_USER_TYPE_MAX 0x6f
#define PL_FRAME_MAX (8u << 20)       /* a payload, raw */
#define PL_BULK_CHUNK (256u << 10)    /* default chunk of a file */
#define PL_META_MAX 4096              /* the opaque meta of a bulk file */
#define PL_QUEUE_BYTES (8u << 20)     /* default queue bound per link */
/* The bulk rate is a node-wide cap per kind: every bulk link of one kind shares a token bucket, and each kind has its
 * own (R4 D7: kind 5's snapshots never hold kind 2's COMMIT files). peerlink_set_bulk_rate sets the rate of every kind
 * without one of its own (peerlink_set_kind_rate). */
#define PL_BULK_RATE_DEFAULT (256ull << 20)
#define PL_RATE_UNLIMITED (~0ull)

struct peerlink;
struct plink;

struct pl_self {
    uint32_t id;
    uint8_t uuid[16];
    uint8_t ident[16];
};

/* The receiving side of bulk files, called on the link's thread. */
struct pl_bulk_in {
    /* A file begins (or begins again after a reconnect). Set *resume to the bytes the receiver already holds of it
     * from offset 0 (they are hashed by reading `*prefix_fd`, which must stay valid until end; -1 with *resume > 0
     * starts over), return 0, or a negative value to refuse it. */
    int (*open)(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                uint64_t *resume, int *prefix_fd);
    /* The bytes [off, off + n) of the file, in order. A negative return aborts the transfer (PL_E_IO to the sender, which stops;
     * bytes still in flight for the file are ignored). */
    int (*data)(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n);
    /* The file ended: status PL_OK (every byte and its xxh64 arrived) or why not (PL_E_HASH, a lost link: PL_E_DOWN). */
    void (*end)(void *arg, struct plink *l, uint64_t file, int status);
};

struct pl_cfg {
    struct pl_self self;
    const uint8_t *secret; /* the cluster secret (copied) */
    size_t secret_len;
    char advertise[128]; /* this server's peer address, for the HELLO ("" if none) */
    void *arg;
    /* a data link came up or went down (on its thread) */
    void (*on_link)(void *arg, struct plink *l, int up);
    /* a frame of the driver's (on the link's thread; the payload is valid during the call) */
    void (*on_frame)(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n);
    struct pl_bulk_in bulk;
    uint32_t queue_bytes; /* per link; 0: PL_QUEUE_BYTES */
    uint32_t bulk_chunk;  /* 0: PL_BULK_CHUNK */
    uint64_t bulk_rate;   /* bytes/s across the bulk links of one kind (each kind has its bucket, so the kinds together
                           * may move a multiple of it); 0: PL_BULK_RATE_DEFAULT */
    uint32_t ping_ms;     /* an idle link sends a PING this often; 0: 1000 */
    uint32_t dead_ms;     /* nothing heard for this long ends a link; 0: 5000 */
    uint32_t backoff_min_ms, backoff_max_ms; /* redial gap, doubled up to the max; 0: 50 and 2000 */
    uint32_t hs_ms;                          /* the handshake's deadline; 0: 5000 */
    /* A tracked frame of plink_sendv left the link: written 1 once the first of its bytes reached the socket (whether
     * the peer got it is then unknown), 0 when none ever did (the link went down or the peerlink closed with the
     * frame queued or still wholly in the write buffer, or its deadline passed in the queue): that is definite. Once
     * per tracked frame, on the link's thread, with the connection number it was queued on; must not block. */
    void (*on_sent)(void *arg, struct plink *l, uint64_t token, uint64_t gen, int written);
    int sock_buf; /* tests: SO_SNDBUF and SO_RCVBUF of every connection (0: the kernel's) */
    int check;    /* the frame check level this end asks for (PL_CHECK_* of plcheck.h; 0: PL_CHECK_NONE); a link runs
                   * at the stronger of its two ends' */
};

struct peerlink *peerlink_new(const struct pl_cfg *cfg);
/* The frame check level (PL_CHECK_*) of every peerlink whose cfg.check is 0, this process's (tsdbd's --peer-check);
 * set before any peerlink runs a handshake. 0: PL_CHECK_NONE. */
void peerlink_set_check_level(int level);
int peerlink_check_parse(const char *s); /* "none", "header", "full" -> the level; -1 for anything else */
const char *peerlink_check_name(int type); /* the level's word ("?" for none known, 0 included) */
/* Stops every link, joins their threads and frees everything (a pending bulk job is completed with PL_E_CLOSED). */
void peerlink_free(struct peerlink *pl);
void peerlink_set_bulk_rate(struct peerlink *pl, uint64_t bytes_per_s);
/* The rate of one data kind's bucket (bytes/s; 0: the peerlink's rate, PL_RATE_UNLIMITED: none); PL_E_INVALID for a
 * kind that is not a data kind */
int peerlink_set_kind_rate(struct peerlink *pl, int kind, uint64_t bytes_per_s);

/* A link to `addr` (host:port, the peer's peer port), persistent. want_id: the node id the peer must answer as (0:
 * any). The result lives until peerlink_free. */
struct plink *peerlink_dial(struct peerlink *pl, uint32_t want_id, const char *addr, int kind);
/* A dialed link's new address (the peer came back on another peer port): its next connection attempt dials it; a
 * connection that is up stays until it fails. -1 for an accepted link or an address that does not fit. */
int plink_set_addr(struct plink *l, const char *addr);

/* The cluster thread's hand-off: `fd` (nonblocking, accepted) whose HELLO frame body (after [len][type]) is `hello`.
 * `self` is what the HELLO_ACK says. 0: the link owns the fd; -1: the caller closes it. Never blocks. */
int peerlink_adopt(struct peerlink *pl, int fd, const uint8_t *hello, size_t hello_len, const struct pl_self *self);
/* The kind a HELLO body asks for (PL_KIND_*), or -1 when it is not a well formed HELLO. */
int peerlink_hello_kind(const uint8_t *hello, size_t len);

/* The accepted link of (peer, kind), or NULL. */
struct plink *peerlink_find(struct peerlink *pl, uint32_t peer, int kind);

/* A standalone listener (tests, tools): accepts on 127.0.0.1:port (0: any; the port is returned), reads each HELLO
 * itself and adopts. -1 on failure. The cluster's acceptor does this in tsdbd. */
int peerlink_listen(struct peerlink *pl, int port);

uint32_t plink_peer(const struct plink *l); /* the peer's node id, once connected */
int plink_kind(const struct plink *l);
int plink_check(const struct plink *l); /* the frame check type its connection runs (plcheck.h); 0 while down */
int plink_up(const struct plink *l);
uint64_t plink_gen(const struct plink *l); /* counts connections of this link (from 1); valid from on_link(up) on */
void plink_set_arg(struct plink *l, void *arg);
void *plink_arg(const struct plink *l);

/* Queues a frame. The payload is copied. timeout_ms 0: PL_E_FULL at once when the queue is full; else waits that
 * long for room. PL_E_DOWN while there is no connection. Safe from any thread. */
int plink_send(struct plink *l, uint8_t type, const void *p, size_t n, int timeout_ms);
/* The same, for connection number `gen` only (plink_gen, or the one on_link(up) ran for): PL_E_DOWN when the link is on
 * another connection now or none, so a frame of the driver's protocol state at gen G never goes out on G + 1. gen 0 is
 * PL_E_INVALID. Sends are PL_E_DOWN until on_link(up) has returned, except from the thread inside it (the link's own
 * thread), so the driver's resume logic in on_link goes first on a new connection. */
int plink_send_gen(struct plink *l, uint64_t gen, uint8_t type, const void *p, size_t n, int timeout_ms);
/* plink_send_gen (gen 0: whatever connection is up) of the payload hdr[0, hn) followed by p[0, n) (at most
 * PL_FRAME_MAX together), copied once. tracked: on PL_OK on_sent runs once for the frame, with `token`; deadline_ns
 * (CLOCK_MONOTONIC, 0: none): a frame still queued then leaves unwritten (on_sent 0). Any other return: nothing was
 * queued and on_sent never runs. */
int plink_sendv(struct plink *l, uint64_t gen, uint8_t type, const void *hdr, size_t hn, const void *p, size_t n,
                int timeout_ms, int tracked, uint64_t token, uint64_t deadline_ns);
/* Ends the link's current connection (a dialed link redials, an accepted one waits for the peer's next): what
 * was queued is dropped as when the peer goes. Tests call it, and chunksrv for a frame that does not parse (a late
 * reply is never a reason). */
void plink_reset(struct plink *l);
/* plink_reset of connection `gen` only (0: whatever is up); a later connection stays. */
void plink_reset_gen(struct plink *l, uint64_t gen);
/* A dialed link that is no longer wanted (its member left): its connection ends and it dials no more; the link
 * stays valid (sends are PL_E_DOWN) until peerlink_free. */
void plink_stop(struct plink *l);
/* Stops (1) or resumes (0) reading the link's socket, on every connection of the link until resumed: TCP pushes back
 * on the peer. The link's thread goes on writing and sends PINGs on its own clock, so the peer does not count it dead,
 * and it does not count the peer dead while it is not reading. Frames already read are still delivered after the
 * pause. A hangup or a socket error is still read. Any thread, also from inside on_frame. */
void plink_pause(struct plink *l, int paused);
/* Takes the tracked frame `token` out of the queue if no link thread has taken it yet: 1 (no byte of it will be
 * written, and its on_sent never runs), else 0 (on_sent runs or ran for it). */
int plink_unqueue(struct plink *l, uint64_t token);
/* bytes now queued (for the driver's own decisions) */
size_t plink_queued(struct plink *l);

struct pl_job {
    uint64_t file;           /* the driver's id of this file, unique per link while the job lives */
    int fd;                  /* read with pread; the caller keeps it open until done is called */
    uint64_t base, length;   /* the file is fd[base, base + length) */
    const void *meta;        /* opaque to the link (a name, a volume): at most PL_META_MAX, copied */
    size_t meta_len;
    int max_attempts;        /* connections the job may use; 0: 5 */
    void (*done)(void *arg, uint64_t file, int status, uint64_t sent);
    void *arg;
};
/* Queues a file for the link's receiver. Asynchronous; `done` runs once, on the link's thread. A call that returns
 * PL_E_FULL, PL_E_CLOSED or PL_E_INVALID did not queue the job and never calls `done`. One file at a time per link, in
 * order. A failure of the receiver's data callback is PL_E_IO to `done` at once (the job is not retried: the receiver's
 * disk is not a link fault); a lost link retries the job from the receiver's offset, max_attempts times. */
int plink_bulk_send(struct plink *l, const struct pl_job *job);

struct pl_stats {
    uint64_t frames_out, frames_in, bytes_out, bytes_in; /* wire bytes of the driver's frames, all links */
    uint64_t raw_out;                                    /* the same frames' payloads before zstd */
    uint64_t connects, drops, auth_failures;
    uint64_t bulk_files, bulk_bytes; /* files finished, payload bytes sent by this side */
    uint64_t handshakes;             /* accepted connections in their handshake now (a gauge, bounded) */
    uint64_t up_check[4];            /* connections up now per frame check type (index PL_CHECK_*; a gauge) */
};
void peerlink_stats(struct peerlink *pl, struct pl_stats *st);
/* The payload bytes the bulk links of one data kind took from their kind's bucket, chunk by chunk as they go to the
 * socket, in this process (every peerlink: in tsdbd each kind belongs to one, so this is that peerlink's). A count at
 * the bucket, not at the end of a file: test_cluster snaplarge weighs what kinds 2 and 5 moved in a window against
 * what one bucket at rebuild_mb_s lets through (D7). Kept outside the peerlink so a reader never races its free. */
uint64_t peerlink_kind_bytes(int kind);

/* ---------------------------------------------------------------- wire (exported for the tests and the fuzzer) */

enum {
    PLT_BEGIN = 0x70,  /* tx->rx: u64 file, u64 total, u32 meta_len, meta */
    PLT_RESUME = 0x71, /* rx->tx: u64 file, i32 status, u64 offset */
    PLT_CHUNK = 0x72,  /* tx->rx: u64 file, u64 offset, bytes */
    PLT_END = 0x73,    /* tx->rx: u64 file, u64 total, u64 xxh64 */
    PLT_DONE = 0x74,   /* rx->tx: u64 file, i32 status */
    PLT_PING = 0x75,
    PLT_PONG = 0x76,
};

struct pl_msg {
    uint8_t type; /* without the compression bit */
    const uint8_t *p;
    size_t n; /* the payload (decompressed) */
    uint64_t file, a, b, c;
    int32_t status;
    const uint8_t *data; /* BEGIN: meta; CHUNK: the bytes */
    size_t data_len;
};

/* Decodes one frame's body (type and payload, tag already checked): decompresses when bit 0x80 is set into *own (a
 * malloc'ed buffer the caller frees; NULL when not needed) and parses the link's own types. 0, or -1 when the body is
 * not well formed (a frame of another length than its type has, a bad zstd frame, a raw length that is not the
 * decoded size). Reads exactly n bytes of p. */
int pl_msg_decode(uint8_t type, const uint8_t *p, size_t n, struct pl_msg *m, uint8_t **own);
/* Encodes a driver frame: [u32 len][type][payload][check] into *out (malloc'ed); compresses when `compress` and it
 * pays. check (a PL_CHECK_ type of plcheck.h: the check's length follows it, 0 for none), salt and seq as a link's
 * direction; returns the length, or 0 on failure. Used by the tests. */
size_t pl_frame_encode(int check, uint64_t salt, uint64_t *seq, int compress, uint8_t type, const void *p, size_t n,
                       uint8_t **out);

/* A streaming splitter of the frames of one direction (what a link's reader does): feed bytes, each complete frame is
 * verified against its check of type `check` (none: no check bytes; unless skip_tag, for the fuzzer) and passed on. */
struct pl_reader;
struct pl_reader *pl_reader_new(int check, uint64_t salt, int skip_tag);
void pl_reader_free(struct pl_reader *r);
/* 0 while the stream is good; -1 once it is not (a bad length, tag or body, or fn returned negative): nothing more is
 * accepted. Frames are decoded through pl_msg_decode and handed to fn (m is valid during the call). */
int pl_reader_feed(struct pl_reader *r, const uint8_t *p, size_t n, int (*fn)(void *arg, const struct pl_msg *m),
                   void *arg);

/* A streaming xxh64 (the one-shot is xxh64() of src/internal.h; they agree). */
struct pl_xxh {
    uint64_t v[4], total;
    uint8_t buf[32];
    size_t nbuf;
};
void pl_xxh_init(struct pl_xxh *s);
void pl_xxh_update(struct pl_xxh *s, const void *p, size_t n);
uint64_t pl_xxh_final(const struct pl_xxh *s);

/* test hooks: a planted bug (tests/test_peerlink.c --mutant) */
enum {
    PL_MUT_NONE = 0,
    PL_MUT_NOTAG = 1, /* a frame whose check is wrong is accepted */
    PL_MUT_SHAREDSOCKET = 2,
    PL_MUT_NOBACKPRESSURE = 3,
    PL_MUT_ACCAUTH = 4,     /* the acceptor does not check AUTH */
    PL_MUT_UUID = 5,        /* the acceptor does not compare the cluster uuid */
    PL_MUT_ENDHASH = 6,     /* the file's xxh64 is not compared */
    PL_MUT_NOTHROTTLE = 7,  /* the token bucket lets everything through */
    PL_MUT_ZSTDSIZE = 8,    /* a zstd frame whose size is not the declared raw length is accepted */
    PL_MUT_CHUNKOFF = 9,    /* a CHUNK at another offset than the next is accepted */
    PL_MUT_RESUMEBOUND = 10, /* a receiver's resume offset beyond the file is accepted */
    PL_MUT_NOKICK = 11,     /* a new connection of the same peer does not take the accepted link over */
    PL_MUT_QUEUEDASUNSENT = 12, /* a frame only partly written when its link went down is reported unwritten */
    PL_MUT_KINDSHARED = 13,     /* a FWD link is the LOG link's socket to that peer */
    PL_MUT_WBWRITTEN = 14,      /* at a cut, every tracked frame in the write buffer is reported written */
    PL_MUT_PAUSEDEAD = 15,      /* a paused link keeps its dead_ms check and PINGs only when it hears nothing */
    PL_MUT_KINDBUCKET = 16,     /* every kind shares one bucket at the peerlink's rate (no per-kind rate) */
    PL_MUT_NOSEQ = 17,          /* the frame check leaves the sequence number out (a reorder or repeat passes) */
    PL_MUT_NOSALT = 18,         /* the frame check leaves the connection's salt out (an old connection's frame passes) */
    PL_MUT_CHECKTYPE = 19,      /* the acceptor takes a HELLO that offers no check type it runs at its level */
    PL_MUT_SAMESALT = 20,       /* both directions of a connection derive one salt (a reflected frame passes) */
    PL_MUT_NOTRANSCRIPT = 21,   /* the salts leave the transcript out: one per secret, an old connection's frame passes */
    PL_MUT_DIALANYTYPE = 22,    /* the dialer runs whatever check type HELLO_ACK names, below its level too */
    PL_MUT_NOTYPE = 23,         /* the frame check leaves the type byte out */
    PL_MUT_HALFCMP = 24,        /* the reader compares half of the check's bytes */
    PL_MUT_SEQ8 = 25,           /* the seed takes the low 8 bits of the sequence number (a frame 256 back passes) */
    PL_MUT_NOREADY = 26,        /* the link thread sleeps in poll although its transport said it holds what it wants */
    PL_MUT_CLOSEFIRST = 27,     /* the transport is closed before on_link(down) */
    PL_MUT_WNOSHORT = 28,       /* a short write counts the whole write buffer as written (WRITTEN for untaken frames) */
    PL_MUT_NHANDALL = 29,       /* a connection that never was in a handshake (the test attach) is taken off nhand */
    PL_MUT_DONESTATUS = 30,     /* a DONE or RESUME status that is no PL_* code is taken as it came */
};
/* Queues any frame type (a link's own ones too) on an open link, for the tests that play a peer that holds the secret
 * and misbehaves. */
int plink_test_ctl(struct plink *l, uint8_t type, const void *p, size_t n);
extern int peerlink_mutant;
/* Test hook (rsnap's TSDB_REPLD_TEST=snapcutdone): > 0 cuts the connection of a kind-5 link once, at the n-th file
 * that ends whole, after the receiver's end callback and before its DONE goes out (a DONE lost to a reconnect). */
extern int peerlink_test_cut_end;
/* Test hook (test_peerlink donestatus): nonzero replaces the status of the next DONE a receiver sends with this value,
 * once (what a flipped bit in an unchecked frame looks like to the sender) */
extern int32_t peerlink_test_done_status;
/* Test hook (test_peerlink salts): when set, called at the end of each connection's handshake with its salts, those of
 * the frames it sends and of those it reads (dialer: 1 on the dialing end) */
extern void (*peerlink_test_salts)(int dialer, uint64_t out, uint64_t in);
/* Test hook (test_peerlink `fake`, over tests/pl_fake.h's in-memory transport; server/pltransport.h): run one
 * connection of the accepted link (peer, kind) on `t` as if a handshake with that peer had passed, with transcript
 * `tr` (both ends give the same bytes and opposite `dialer`: their salts then match as a handshake's do; the check
 * type is this peerlink's level, cfg.check: both ends' peerlinks must have the same). It takes the link over as a new accepted connection does, on a thread of its
 * own, and owns `t` from the call on (closed when the connection ends, or at once on failure). 0, or -1. */
struct pl_transport;
int peerlink_test_attach(struct peerlink *pl, uint32_t peer, int kind, int dialer, const uint8_t *tr, size_t trlen,
                         struct pl_transport *t);

#endif
