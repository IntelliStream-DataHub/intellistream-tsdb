/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd: serves a tsdb directory over TCP (protocol in tsdb_protocol.h).
 *
 * One thread runs the event loop: one recv and at most one send in flight per
 * connection, and an eventfd through which workers report (a lock-free stack of
 * notes, see notify). The loop runs on
 * io_uring, or on epoll where io_uring is not allowed (RHEL and AlmaLinux 10
 * disable it by default); --io auto (the default) tries io_uring and falls
 * back to epoll when the kernel refuses it. Only how bytes move differs: see
 * "event loop" below. Connections are accepted by a thread of their own with a
 * plain accept(), not through the ring: a pending io_uring accept keeps the
 * listening socket alive until the ring is torn down, which for a killed
 * process happens some time after its death, and meanwhile a restarted server
 * cannot bind and clients connect to a listener nobody serves. Database calls may block (fsync, decoding),
 * so complete request frames go to a pool of worker threads.
 *
 * A connection's requests run in order, one at a time, which keeps "last
 * write wins" for clients that pipeline. Workers append replies to the
 * connection's output buffer; a worker streaming a large result waits while
 * that buffer is over OUT_CAP, so a slow client cannot make the server buffer
 * without bound. Who sends a connection's output is decided under its lock:
 * while the event loop has nothing of it in flight, the worker sends what it
 * appended itself, without blocking (out_send: one loop thread cannot do the
 * kernel's TCP transmit of every small reply); what such a send leaves, on a
 * full socket buffer or an error, goes to the event loop, which then sends
 * everything until it has drained the output (tx_loop), the worker meanwhile
 * only appending. The one QUERY_MULTI streaming at a time hands its batches to
 * the event loop, which sends them while the worker computes (streaming_conns).
 *
 *   tsdbd --dir /data/sensors [--listen 127.0.0.1] [--port 7070] [--port-file FILE] [--workers N]
 *         [--wal-sync-ms 0] [--no-fsync] [--read-only | --follow] [--io auto|uring|epoll]
 *         [--verify-checksums] [--pin-index] [--metrics-port N|auto [--metrics-bind ADDR]]
 *         [--cluster --cluster-secret-file F [--peer-port 7071|0] [--peer-listen ADDR] [--advertise HOST]
 *          [--rack NAME]]
 *
 * --cluster runs the cluster thread (cluster.c, docs/design-cluster.md): the metadata group on a peer port of its
 * own (--peer-port, default the client port + 1; bound to --peer-listen, default --listen), with its files in
 * DIR/meta/. --advertise is the host other servers and clients reach this one at (default --listen, which must then
 * not be a wildcard). Step 1 of the design: the engine and the client port are unchanged.
 *
 * --no-stats turns the statistics of what is stored off (no sweeper thread, no query counters, no column totals
 * in the metrics; docs/design-stats.md). On by default: the sweeper follows the open.
 *
 * --metrics-port serves Prometheus metrics (GET /metrics) on a port of its own, from a
 * thread of its own: see metrics.c. Off by default; binds to --listen unless --metrics-bind.
 *
 * --verify-checksums checks each granule's checksum before a query decodes it
 * (off by default: meant for storage without checksums of its own). Without it, a directory on ext2/3/4 or
 * XFS (macOS: APFS or HFS+), which keep no checksums of the data, gets a line at startup that says so (fs_warn).
 * --pin-index keeps each part's granule index and tombstones in memory as the open checked them (about 32 to 51
 * bytes per granule; the tsdb_pinned_index_bytes metric), so damage that reaches a part file later cannot change
 * which granules a query reads or the statistics it answers from. Off by default: then they are read in place.
 *
 * --port 0 binds a port the kernel chooses; --port-file FILE then holds "PORT PID" (written to a temporary file
 * and renamed once the listener is up; an old FILE is removed at start), and the banner names the real port.
 * Scripts and tests start servers this way and never compute a port, so two of them cannot collide.
 * --metrics-port auto does the same for the metrics port, which the file then names as a third number.
 * With --cluster the file names the peer port as a fourth ("PORT PID MPORT PEER_PORT", MPORT 0 without metrics).
 *
 * --follow serves queries from a directory another process writes (on this
 * host or another one, over NFS), following its parts and WAL as they change.
 *
 * Connections: up to --max-connections (default 10000) at once, each costing a
 * descriptor (the soft limit is raised to the hard one at start) and 16 KB
 * of receive buffer while idle. Requests run on --workers threads; requests
 * beyond that wait in their connection's queue.
 *
 * Signals: SIGTERM and SIGINT stop it cleanly (requests in progress finish,
 * the memtable is flushed, exit status 0); SIGHUP and SIGPIPE are ignored.
 * With --exit-with-parent it stops the same way once its parent process is gone: a test's server must not outlive
 * the test (killed tests left servers holding io_uring rings for hours). The tick compares getppid() with the
 * parent at start; not PR_SET_PDEATHSIG, which fires when the spawning thread ends, and Rust, Java and Python
 * tests start servers from threads that end while the test goes on. Harnesses pass --exit-with-parent=PID, their
 * own pid, and tsdbd refuses to start under any other parent: a launcher gone before the options are read would
 * otherwise leave a subreaper (systemd --user) as the parent to follow, which never exits.
 *
 * Fail-stop: when a storage error puts the database in its failed state
 * (tsdb_failed: a failed WAL write or fsync and the like, after which it is
 * unknown what reached the disk) writes return TSDB_ERR_IO, which clients
 * already know. The server then logs one line, stops accepting connections,
 * lets the replies in flight go out for a moment and exits with status 1:
 * only a reopen recovers, so a supervisor restarts it (or a standby takes over).
 * After SIGKILL a new server can start on the same port and directory at once.
 *
 * Memory: a request is held from its first byte until a worker has run it. Requests waiting to run (their payload
 * and REQ_OVERHEAD each, so empty frames count too), receive buffers grown past IN_KEEP (64 KB; a frame larger
 * than that gets a buffer of exactly its size when its length arrives) and packed inserts being unpacked all count
 * against --max-request-memory (default an eighth of the RAM, at least 1024 MB). A connection that would start a
 * large frame beyond it is not read from until memory frees (unless nothing else holds any, so one frame always
 * fits), and neither is one with requests waiting while the total is over it; a packed insert waits to unpack
 * while the total would exceed it, unless no other is unpacking. Outside it: up to IN_KEEP of receive buffer per
 * connection, and the replies waiting for a client that does not read them (two buffers per connection, the one
 * being sent and the one its worker fills, each up to OUT_CAP + OUT_PIECE: a larger reply goes in pieces, its
 * worker waiting between them; a connection whose output has not moved for --send-timeout seconds is closed, whether
 * a worker waits on it or the whole reply already fits; a client that keeps reading, however slowly, keeps it, and
 * time the server itself was stopped does not count). A connection with more than PIPE_CAP of requests (or MULTI_QUEUED
 * multi-series queries) waiting reads no more until they run, so a CANCEL behind them waits too, and one whose
 * frame has been started but not completed for --frame-timeout seconds
 * (default 60) while the server waits for its bytes is closed (slow senders); time the server itself held the
 * connection unread does not count. Replies never exceed a frame: a result too large for one is
 * TSDB_ERR_TOO_MANY_BUCKETS. A logged-in client can still hold the whole limit with large frames it sends slowly,
 * for --frame-timeout at a time, making others' large frames wait; before login a connection holds at most two
 * frames of TSDB_PRELOGIN_MAX bytes and is closed after --login-timeout (A5), so an anonymous client cannot.
 *
 * Login (docs/design-auth.md, server/auth.c): required unless --no-auth. Before login parse() takes only HELLO,
 * AUTH_START and AUTH_CONTINUE, at most two queued, and reads nothing after an AUTH_CONTINUE until the worker has
 * answered it; any other op is queued with eof set and answered TSDB_ERR_AUTH. Every request then passes
 * auth_check (the op table) in handle() before its case runs; ids that only the case knows (a row per series,
 * QUERY_MULTI) are checked there. There is no TLS: a login is not readable on the wire, the data is.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_protocol.h"
#include "tsdb_wire.h"
#include "../client/tsdb_pack.h"
#include "../client/tsdb_crypto.h"
#include "metrics.h"
#include "auth.h"
#include "cluster.h"

#ifndef TSDB_BUILD_VERSION
#define TSDB_BUILD_VERSION "dev"
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#if defined(__APPLE__) && !defined(TSDBD_NO_URING) /* macOS: kqueue behind epoll_compat.h, no io_uring */
#define TSDBD_NO_URING
#endif
#ifndef TSDBD_NO_URING /* make NO_URING=1: epoll only, no liburing needed at run time */
#include <liburing.h>
#endif
#include <zstd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#ifdef __APPLE__
#include "epoll_compat.h"
#else
#include <linux/sockios.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/timerfd.h>
#endif
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#ifdef __APPLE__ /* statfs, with the file system's name in f_fstypename (fs_warn) */
#include <sys/mount.h>
#else
#include <sys/vfs.h>
#endif
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include "../src/compat.h" /* macOS: accept4, SOCK_CLOEXEC, thread stacks; nothing on Linux */

#define RECV_MIN_FREE (16u << 10) /* per connection; grows for large frames */
#define BUF_KEEP (256u << 10)      /* buffers grown past this shrink back when idle */
#ifndef OUT_CAP /* tests/fuzz/fuzz_frames.c includes this file with its own */
#define OUT_CAP (8u << 20)
#endif
#define RAW_CHUNK_ROWS 16384
#define PIPE_CAP (4u << 20) /* bytes of requests waiting on one connection before it is read no further */
/* QUERY_MULTIs waiting on one connection before it is read no further: a CANCEL looks only at them, so what one
 * CANCEL costs the loop is bounded by this and the multis of one receive buffer, not by the queue's length. */
#define MULTI_QUEUED 16
/* the ops a CANCEL can name (and MULTI_QUEUED counts): the ones that answer in pieces */
static inline bool cancelable(uint8_t op) { return op == TSDB_OP_QUERY_MULTI || op == TSDB_OP_QUERY_LATEST; }
/* A receive buffer up to this size is not counted in the request memory; a frame larger than it gets a buffer of
 * its own size, which is (see the file header). */
#define IN_KEEP (64u << 10)

typedef struct buf {
    uint8_t *p;
    size_t len, cap;
} buf;

static int buf_reserve(buf *b, size_t extra)
{
    if (b->len + extra <= b->cap)
        return 0;
    size_t cap = b->cap ? b->cap : 4096;
    while (cap < b->len + extra)
        cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p)
        return -1;
    b->p = p;
    b->cap = cap;
    return 0;
}

/* Gives memory back after a large frame or reply, keeping idle connections cheap. */
static void buf_shrink(buf *b)
{
    if (b->cap > BUF_KEEP && b->len <= BUF_KEEP / 2) {
        uint8_t *p = realloc(b->p, BUF_KEEP);
        if (p) {
            b->p = p;
            b->cap = BUF_KEEP;
        }
    }
}

typedef enum { EV_RECV, EV_SEND, EV_WAKE, EV_TICK } ev_type;

struct conn;
typedef struct ev {
    ev_type type;
    struct conn *c;
} ev;

typedef struct req {
    struct req *next;
    struct conn *c;
    uint32_t id;
    uint8_t op;
    uint8_t *payload; /* malloc'd, so 16-byte aligned */
    size_t len;
    bool cancelled; /* a QUERY_MULTI a CANCEL named while it was queued (main thread) */
    struct req *mnext; /* the next QUERY_MULTI in the connection's queue (main thread) */
} req;

/* What a request costs beyond its payload: the struct and two allocations. Counted with the payload, so frames
 * without one (9 bytes on the wire) cannot pile up uncounted. */
#define REQ_OVERHEAD (sizeof(req) + 64)
#define REQ_COST(r) ((r)->len + REQ_OVERHEAD)
/* What a worker tells the main thread about a connection: output is waiting, or its request finished. */
typedef struct note {
    struct note *next;
    struct conn *c;
    bool done;
} note;

typedef struct conn {
    struct conn *prev, *next; /* all connections, for shutdown (main thread) */
    int fd;
    ev recv_ev, send_ev;
    /* main thread only */
    buf in;
    req *q_head, *q_tail;
    req *mq_head, *mq_tail; /* the QUERY_MULTIs in q, in order (through mnext), for CANCEL */
    size_t q_multi;         /* how many */
    bool busy;          /* a worker owns this connection's current request */
    bool recv_inflight, send_inflight, closed;
    bool eof;           /* the client sent everything it will: reply to it, then close */
    buf sending;
    size_t sent;
    int64_t tx_t0;      /* start of the progress window of the output (monotonic ms), see on_tick */
    int64_t tx_base;    /* what the client had consumed (tx_total - SIOCOUTQ) at tx_t0, -1: not read yet */
    bool tx_win;        /* a progress window is open: a send is in flight */
    _Atomic int64_t tx_total; /* bytes handed to the socket so far, by the loop or by a worker (out_send) */
    size_t in_charged;  /* of in.cap, counted in req_held (a buffer grown for a large frame) */
    size_t q_bytes;     /* REQ_COST of the requests in q */
    int64_t frame_t0;   /* when the partial frame in `in` was started (monotonic s), 0: none */
    bool parked;        /* not read from until memory frees (park list); park_frame: to grow for a frame */
    bool park_frame;
    bool held;          /* the server stopped reading it (parked or PIPE_CAP): its frame deadline restarts */
    struct conn *park_prev, *park_next;
    bool readable, writable, rdhup; /* epoll: what the socket can do, as far as we know */
    bool in_ready, dead;            /* epoll: in the ready list; freed while in it */
    struct conn *ready_next;
    uint32_t run_id;    /* the request a worker runs (while busy), for CANCEL */
    uint8_t run_op;
    /* shared with the worker */
    atomic_bool cancel_running; /* CANCEL named the running QUERY_MULTI */
    pthread_mutex_t lock;
    pthread_cond_t cv;
    buf out;
    atomic_bool closing;
    atomic_uint proto;  /* the version the HELLO settled on (TSDB_PROTOCOL_MIN until there is one): HA bodies from 13 */
    atomic_int dom_v;   /* the volume the request now running is inside (handle), -1: any (it names none), 0: none */
    _Atomic int64_t nonup_since; /* when this request first waited for output inside a domain that is not UP
                            (monotonic ms), 0: not yet; reset by handle() for every request (out_wait) */
    bool notified;      /* `out` has bytes the main thread was told of (n_out pushed): it sends them */
    bool tx_loop;       /* the main thread sends: a send in flight, bytes left in `sending`, or bytes a worker's
                           send left in `out` or handed it (out_put); the worker only appends until the main thread
                           drained it all */
    bool tx_worker;     /* the worker sends from the front of `out` itself (out_send): nobody else touches `out` */
    bool streaming;     /* the worker runs a QUERY_MULTI, whose batches go out while it computes (worker only) */
    note n_out, n_done; /* in note_top while queued for the main thread */
    atomic_bool out_queued; /* n_out is in note_top or not yet handled */
    tsdb_query_stats last_stats; /* of the previous query (LAST_STATS) */
    /* login: the worker sets these, the main thread reads them */
    atomic_bool authed;   /* logged in (or --no-auth): parse() takes every op */
    atomic_bool auth_end; /* a failed login or a dropped user: close once the requests received are answered */
    int64_t t_open;       /* accepted at (monotonic s), for --login-timeout (main thread) */
    bool auth_hold;       /* an AUTH_CONTINUE waits or runs: nothing more is read until it is answered (main) */
    auth_conn auth;       /* the worker running this connection's request (one at a time) */
} conn;

/* ------------------------------------------------------------ globals */

static tsdb *db;
#ifndef TSDBD_NO_URING
static struct io_uring ring;
#endif
static int wake_fd = -1;
/* How the loop is woken from other threads and from the signal handler: an eventfd (an 8-byte counter) on Linux,
 * the write end of a pipe on macOS (a byte each; a full pipe means a wakeup is already pending). */
#ifdef __APPLE__
static int wake_wr = -1;
static void wake_signal(void)
{
    char b = 1;
    if (write(wake_wr, &b, 1) != 1) {
        /* EAGAIN: full, so the loop will wake anyway */
    }
}
static void wake_drain(void)
{
    char b[256];
    while (read(wake_fd, b, sizeof b) == (ssize_t)sizeof b)
        ;
}
static int wake_open(void)
{
    int p[2];
    if (pipe(p) != 0)
        return -1;
    for (int i = 0; i < 2; i++) {
        fcntl(p[i], F_SETFD, FD_CLOEXEC);
        fcntl(p[i], F_SETFL, fcntl(p[i], F_GETFL) | O_NONBLOCK);
    }
    wake_wr = p[1];
    return p[0];
}
#else
static void wake_signal(void)
{
    uint64_t one = 1;
    if (write(wake_fd, &one, 8) != 8) {
        /* the counter cannot overflow in practice; the main thread will see the note */
    }
}
static void wake_drain(void)
{
    uint64_t v;
    if (read(wake_fd, &v, 8) < 0 && errno != EAGAIN)
        perror("tsdbd: eventfd");
}
static int wake_open(void) { return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC); }
#endif
static atomic_int stopping; /* lock-free, so the signal handler may store it; read by failwatch too */
#ifndef TSDBD_NO_URING
static ev wake_ev = {EV_WAKE, NULL}, tick_ev = {EV_TICK, NULL};
#endif
static conn *conns;
static size_t nconns, max_conns = 10000;

/* work queue: requests ready to run */
static pthread_mutex_t work_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_cv = PTHREAD_COND_INITIALIZER;
static req *work_head, *work_tail;
static bool work_stop;

/* A worker streaming to a client that does not read waits for output space.
 * Enough such clients would hold every worker and stop the server for
 * everyone else, so a worker about to wait while no other is free starts an
 * extra one (they leave again when the queue is empty), and a client that
 * reads nothing for send_timeout_s loses its connection. Under work_lock. */
#define MAX_EXTRA_WORKERS 256
static int base_workers, running_workers, waiting_workers;
static pthread_cond_t workers_gone_cv = PTHREAD_COND_INITIALIZER;
static int send_timeout_s = 60;
/* A client has to read this many bytes a second of what the server has for it (averaged over --send-timeout), or
 * its connection is closed: 16 KB/s lets a 100 MB reply take under two hours, and holds a worker no longer. */
#define SEND_MIN_RATE 16384
static void *worker_main(void *arg);

/* Bytes of requests held (see the file header): receive buffers grown for large frames, and payloads from
 * parsing until a worker has run them. Changed by the main thread and the workers. */
static size_t req_budget = (size_t)1024 << 20; /* at least; main raises it to an eighth of the RAM */
static atomic_size_t req_held;
static int frame_timeout_s = 60;
static pid_t start_ppid; /* --exit-with-parent: the parent at start; 0 without the flag */
static int login_timeout_s = 10; /* --login-timeout */

static size_t request_memory_now(void)
{
    return atomic_load_explicit(&req_held, memory_order_relaxed);
}

static void req_free(req *r)
{
    atomic_fetch_sub(&req_held, REQ_COST(r));
    free(r->payload);
    free(r);
}

/* Packed inserts decompress to up to a frame's worth of arrays from a few kilobytes: counted in req_held with the
 * requests (so the server also reads less meanwhile) and waiting while the total would exceed the limit. One always
 * may when no other is decompressing, so this never waits on itself: the peak is the limit plus one insert. */
static pthread_mutex_t inflate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t inflate_cv = PTHREAD_COND_INITIALIZER;
static size_t inflate_held;

static void inflate_acquire(size_t n)
{
    pthread_mutex_lock(&inflate_lock);
    while (inflate_held && atomic_load_explicit(&req_held, memory_order_relaxed) + n > req_budget)
        pthread_cond_wait(&inflate_cv, &inflate_lock);
    inflate_held += n;
    atomic_fetch_add_explicit(&req_held, n, memory_order_relaxed);
    pthread_mutex_unlock(&inflate_lock);
}

static void inflate_release(size_t n)
{
    pthread_mutex_lock(&inflate_lock);
    inflate_held -= n;
    atomic_fetch_sub_explicit(&req_held, n, memory_order_relaxed);
    pthread_cond_broadcast(&inflate_cv);
    pthread_mutex_unlock(&inflate_lock);
}

static void worker_waits(int delta)
{
    pthread_mutex_lock(&work_lock);
    waiting_workers += delta;
    if (delta < 0)
        pthread_cond_broadcast(&work_cv); /* an idle extra may leave now that this one is free */
    if (delta > 0 && running_workers == waiting_workers && running_workers < base_workers + MAX_EXTRA_WORKERS &&
        !work_stop) {
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
#ifdef __APPLE__
        pthread_attr_setstacksize(&at, TSDB_THREAD_STACK);
#endif
        pthread_t t;
        if (pthread_create(&t, &at, worker_main, (void *)1) == 0)
            running_workers++;
        pthread_attr_destroy(&at);
    }
    pthread_mutex_unlock(&work_lock);
}

/* A small write waiting for a large write's publish (tsdb_set_write_wait_hook) holds its worker as long as that
 * insert takes, and the large write's worker does not come free before then either: both count as waiting, so
 * connections writing into its keys do not take every worker from queries and other writes. */
static void write_on_wait(void *ctx, int delta)
{
    (void)ctx;
    worker_waits(delta);
}

/* The volume an UNAVAILABLE reply names (protocol 13): the volume that refused the request in hand, else the volume
 * of its series id, else TSDB_VOL_ANY. Set per request by handle() and dom_enter on the worker running it, read by
 * frame_put, so every path that answers TSDB_ERR_UNAVAILABLE (the admission, a quiesced volume under an engine
 * call) sends the same body. */
static __thread uint16_t unav_vol = TSDB_VOL_ANY;

#include "domains.h"
static void dom_waits(int delta) { worker_waits(delta); }

static uint32_t unav_jitter(uint32_t ms) /* +-20% */
{
    static __thread uint64_t x;
    if (!x)
        x = (uint64_t)(uintptr_t)&x ^ ((uint64_t)time(NULL) << 20) ^ 0x9e3779b97f4a7c15ull;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return (uint32_t)((uint64_t)ms * (800 + x % 401) / 1000);
}

/* The health of volume v from its storage domain, no I/O: what TOPOLOGY, STATUS and UNAVAILABLE say. A volume
 * outside every domain (and the root) is AVAILABLE here: its failure ends the process. */
static void vol_health(size_t v, uint8_t *health, uint8_t *reason, int64_t *since_ms, uint64_t *outage_id,
                       uint32_t *retry_ms, uint16_t *domain)
{
    *health = TSDB_HEALTH_AVAILABLE;
    *reason = TSDB_WHY_NONE;
    *since_ms = 0;
    *outage_id = 0;
    *retry_ms = 1000;
    *domain = 0xffff;
    int d = dom_of(v);
    if (d < 0 || !dom.health)
        return;
    *domain = (uint16_t)d;
    tsdb_domain_info di;
    if (tsdb_health_info(dom.health, d, &di) != 0)
        return;
    *since_ms = di.since_ms;
    *outage_id = di.outage_id;
    bool diverged = v < DOM_MAX_VOLS && atomic_load(&dom.vol[v].diverged);
    int64_t age = di.age_ms < 0 ? 0 : di.age_ms;
    uint32_t after = (uint32_t)(age / 10 < 1000 ? 1000 : age / 10 > 60000 ? 60000 : age / 10);
    if (diverged) {
        *health = TSDB_HEALTH_UNAVAILABLE;
        *reason = TSDB_WHY_DIVERGED;
        *retry_ms = 60000;
        return;
    }
    switch (di.state) {
    case TSDB_DOM_DOWN:
        *health = TSDB_HEALTH_UNAVAILABLE;
        *reason = TSDB_WHY_NFS_DOWN;
        *retry_ms = after;
        break;
    case TSDB_DOM_RECOVERING:
        *health = TSDB_HEALTH_UNAVAILABLE;
        *reason = TSDB_WHY_RECOVERING;
        *retry_ms = after;
        break;
    case TSDB_DOM_DEGRADED:
        *health = TSDB_HEALTH_DEGRADED;
        *reason = TSDB_WHY_BUSY;
        *retry_ms = 200;
        break;
    default:
        if (v < DOM_MAX_VOLS && atomic_load(&dom.vol[v].recovering)) {
            *health = TSDB_HEALTH_UNAVAILABLE;
            *reason = TSDB_WHY_RECOVERING;
            *retry_ms = after;
        }
        break;
    }
}

/* The one entry of an UNAVAILABLE reply: the named volume's health. A refusal whose volume is known to be fine
 * (the library quiesced it a moment ago) says RECOVERING, the reason a quiesced volume has. */
static tsdb_unavail unav_entry(uint16_t vol)
{
    tsdb_unavail e = {.vol = vol, .reason = TSDB_WHY_NONE};
    uint8_t health, reason;
    uint16_t dom_idx;
    uint32_t retry;
    if (vol == TSDB_VOL_ANY) { /* the first domain that is not UP, else nothing is known */
        size_t nvol = dom.db ? tsdb_volume_count(dom.db) : 0;
        for (size_t v = 1; v < DOM_MAX_VOLS && v < nvol; v++) {
            vol_health(v, &health, &reason, &e.since_ms, &e.outage_id, &retry, &dom_idx);
            if (health != TSDB_HEALTH_AVAILABLE)
                goto found;
        }
        e.retry_after_ms = unav_jitter(1000);
        return e;
    }
    vol_health(vol, &health, &reason, &e.since_ms, &e.outage_id, &retry, &dom_idx);
found:
    e.reason = health == TSDB_HEALTH_AVAILABLE ? (dom_idx == 0xffff ? TSDB_WHY_NONE : TSDB_WHY_RECOVERING) : reason;
    e.retry_after_ms = unav_jitter(retry);
    return e;
}

/* Tests (TSDBD_TEST_HOLD_LARGE_MS): every large insert, replace and delete sleeps that long after writing its parts,
 * before it publishes them, so a test can write into its keys while it is in flight. */
void tsdb_test_precommit(tsdb *t, void (*fn)(void *), void *arg); /* the library's test hook (src/internal.h) */
static int hold_large_ms;
static void hold_large(void *arg)
{
    (void)arg;
    struct timespec d = {hold_large_ms / 1000, (long)(hold_large_ms % 1000) * 1000000};
    nanosleep(&d, NULL);
}

/* events from workers to the main thread */
/* Workers' notes to the main thread, a lock-free stack (each connection has its two notes; see notify). */
static _Atomic(note *) note_top;
static pthread_mutex_t note_lock = PTHREAD_MUTEX_INITIALIZER; /* new_fds */

/* connections the accept thread took, for the main thread to set up */
static int *new_fds;
static size_t nnew, new_cap;

/* With many connections of short requests the workers met on a lock here for every reply, and the main thread spent
 * a quarter of its time waking the ones that waited for it; so the notes are a lock-free stack (Treiber), which the
 * main thread takes whole and reverses into the order of the pushes. Each connection has two notes, each pushed at
 * most once until the main thread took it: n_done once per request (the next is dispatched only after its note was
 * handled), n_out guarded by out_queued (the main thread takes all output when it handles it, so an output note
 * while one waits is not needed). A worker pushes n_out before n_done, so the main thread sees them in that order,
 * and a connection whose done note it handles has no other note behind it in the batch. */
static void notify(conn *c, bool done)
{
    if (!done && atomic_exchange(&c->out_queued, true))
        return;
    note *n = done ? &c->n_done : &c->n_out;
    note *top = atomic_load_explicit(&note_top, memory_order_relaxed);
    do
        n->next = top;
    while (!atomic_compare_exchange_weak_explicit(&note_top, &top, n, memory_order_release, memory_order_relaxed));
    /* Whoever pushes onto an empty stack writes the eventfd; the main thread reads the eventfd before it takes the
     * stack, so a note pushed onto one not yet taken is seen without a write (a syscall saved per reply while the
     * main thread is busy). */
    if (!top)
        wake_signal();
}

/* ------------------------------------------------------- reply writing */

static inline void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }

static int64_t mono_ms(void);
/* The clock of c->cv: monotonic where the platform lets a cv have it (a step of the wall clock must not stretch or
 * cut a wait); macOS has no pthread_condattr_setclock. */
#ifdef __APPLE__
#define CV_CLOCK CLOCK_REALTIME
#else
#define CV_CLOCK CLOCK_MONOTONIC
#endif
static void conn_cv_init(pthread_cond_t *cv)
{
#ifdef __APPLE__
    pthread_cond_init(cv, NULL);
#else
    pthread_condattr_t at;
    pthread_condattr_init(&at);
    pthread_condattr_setclock(&at, CV_CLOCK);
    pthread_cond_init(cv, &at);
    pthread_condattr_destroy(&at);
#endif
}
#ifndef OUT_WAIT /* tests/fuzz/fuzz_frames.c drains the output instead of waiting for a send */
#define OUT_WAIT out_wait
/* Waits, under c->lock, while c's output is over OUT_CAP. Outside a domain that is not UP it has no deadline of its
 * own: the cv is broadcast only when a whole `sending` buffer (up to OUT_CAP + OUT_PIECE) has gone out, so a deadline
 * per wait would cut a client that keeps reading, only slower than that buffer per --send-timeout. The tick decides
 * there (on_tick: output that is read slower than SEND_MIN_RATE over --send-timeout), and conn_close, like the
 * shutdown, sets `closing` and wakes this. While a worker waits here the main thread has a send in flight on the
 * connection, which the tick watches.
 * Inside a domain that is not UP the request holds the volume's recovery up (the engine's references), however
 * steadily its client reads: the deadline is then --send-timeout from the request's first wait there, over all its
 * waits (nonup_since, reset for each request), not from each wait: the waits end whenever a whole buffer has gone
 * out, which a client above 9 MB per --send-timeout would restart for ever. */
static void out_wait(conn *c)
{
    worker_waits(1);
    while (c->out.len > OUT_CAP && !atomic_load(&c->closing)) {
        bool nonup = dom.ndoms > 0 && dom_inside_nonup(atomic_load(&c->dom_v));
        if (!nonup) {
            /* the domain is up (or none is configured): the request holds no recovery up */
            atomic_store_explicit(&c->nonup_since, 0, memory_order_relaxed);
            if (dom.ndoms == 0) {
                pthread_cond_wait(&c->cv, &c->lock);
                continue;
            }
        } else {
            /* A request inside a volume whose domain is not UP: the volume's recovery waits for it to return (the
             * engine's references), however steadily its client reads (review m2). --send-timeout seconds from its
             * first wait here end it: the connection is closed, as when a message does not fit in memory (out_put).
             * Checked on entering as well as after a timed wait: a wait that a broadcast ends ends before the check. */
            int64_t now = mono_ms(), since = atomic_load_explicit(&c->nonup_since, memory_order_relaxed);
            if (since == 0)
                atomic_store_explicit(&c->nonup_since, now, memory_order_relaxed);
            else if (now - since >= send_timeout_s * 1000LL) {
                fprintf(stderr, "tsdbd: connection closed: its client reads too slowly for a volume in recovery\n");
                atomic_store(&c->closing, true);
                shutdown(c->fd, SHUT_RDWR);
                break;
            }
        }
        struct timespec at; /* the domain's state changes without this cv being signalled: look again within 1 s */
        clock_gettime(CV_CLOCK, &at);
        at.tv_sec += 1;
        pthread_cond_timedwait(&c->cv, &c->lock, &at);
    }
    worker_waits(-1);
}
#endif

/* Sends c's output from the worker's own thread, while the main thread has nothing of this connection's in flight
 * (tx_worker, taken under c->lock, keeps it from taking `out` meanwhile). One event loop thread cannot keep up with
 * many connections of small replies: the kernel's TCP transmit (on loopback the receiver's side too) was most of its
 * time, and the loop at full load capped the server at about 120,000 replies a second. Done here it spreads over the
 * workers, and a reply sent whole needs no wakeup of the main thread. What a non-blocking send leaves (a full socket
 * buffer, an error) goes to the main thread as before, which sends it or closes the connection. Returns whether the
 * main thread must be told of output. */
static bool out_send(conn *c)
{
    /* While tx_worker is set nobody else reads or changes `out`: the main thread leaves it alone (submit_send), and
     * only this worker appends to it. So the send, and moving what it left to the front (up to OUT_CAP + OUT_PIECE),
     * need no lock; the main thread, which takes the lock for every note it handles, never waits for them. */
    size_t done = 0, len = c->out.len;
    while (done < len) {
        ssize_t n = send(c->fd, c->out.p + done, len - done, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0)
            done += (size_t)n;
        else if (n < 0 && errno == EINTR)
            continue;
        else
            break;
    }
    if (done && done < len)
        memmove(c->out.p, c->out.p + done, len - done);
    c->out.len = len - done;
    atomic_fetch_add_explicit(&c->tx_total, (int64_t)done, memory_order_relaxed);
    pthread_mutex_lock(&c->lock);
    c->tx_worker = false;
    /* What the socket did not take now waits for it to drain: the main thread sends it, and everything appended
     * until it has drained the output, rather than this worker trying again (an EAGAIN and a move per piece). */
    if (c->out.len)
        c->tx_loop = true;
    bool wake = c->out.len && !c->notified;
    if (wake)
        c->notified = true;
    pthread_mutex_unlock(&c->lock);
    return wake;
}

/* Appends n pieces to c's output, waiting while the output is over OUT_CAP. A frame larger than OUT_PIECE goes in
 * pieces of that size, each after the output fell to OUT_CAP, so a connection's output buffer stays under OUT_CAP +
 * OUT_PIECE whatever its replies' size: a bucket reply of up to a frame (256 MB) is not copied whole into a buffer
 * rounded up to 512 MB while the engine's result is held too. Only the worker running the connection's request
 * appends to its output (a QUERY_MULTI's callbacks all run on it), so the pieces of a frame follow each other.
 * Returns false when the connection is going away. */
#define OUT_PIECE (1u << 20)

/* Connections whose worker runs a QUERY_MULTI (conn.streaming). Its results stream out in batches (ob_flush) while
 * it computes the next series; a batch the worker sends itself puts the send (on loopback the receiver's side of
 * TCP too) on that query's serial path, and one connection lost 28% of its windows/s that way (52b710e). So the
 * batches of the only streaming connection go to the event loop, which sends them meanwhile, as before 52b710e.
 * With two or more the workers send their own again: one loop thread sending for many streaming connections is
 * what capped the server (handing a batch over whenever the loop owned nobody's output, 32 connections lost 4%).
 * Only full batches go over (HANDOFF_MIN, OB_FLUSH): a smaller one (what is ready while the head is computed) costs
 * less to send than a wakeup of the loop. A connection counts for its whole tsdb_query_multi, also while its worker
 * waits for a client that does not read (OUT_WAIT): one stalled QUERY_MULTI makes another streaming one send its
 * own batches again, slower but correct, until the stalled one ends or times out (--send-timeout). */
static atomic_int streaming_conns;
#define HANDOFF_MIN (64u << 10)

static bool out_put(conn *c, const struct iovec *iov, int n)
{
    int i = 0;
    size_t at = 0; /* in iov[i] */
    for (;;) {
        while (i < n && at == iov[i].iov_len) {
            i++;
            at = 0;
        }
        if (i == n)
            return true;
        size_t total = 0;
        for (int k = i; k < n && total < OUT_PIECE; k++)
            total += iov[k].iov_len - (k == i ? at : 0);
        if (total > OUT_PIECE)
            total = OUT_PIECE;
        pthread_mutex_lock(&c->lock);
        if (c->out.len > OUT_CAP && !atomic_load(&c->closing))
            OUT_WAIT(c);
        bool ok = !atomic_load(&c->closing);
        if (ok && buf_reserve(&c->out, total) != 0) {
            /* a lost reply would leave the client waiting forever: drop the connection instead */
            atomic_store(&c->closing, true);
            shutdown(c->fd, SHUT_RDWR);
            ok = false;
        }
        bool wake = false, direct = false;
        if (ok) {
            for (size_t left = total; left;) {
                size_t take = iov[i].iov_len - at < left ? iov[i].iov_len - at : left;
                if (take)
                    memcpy(c->out.p + c->out.len, (const uint8_t *)iov[i].iov_base + at, take);
                c->out.len += take;
                left -= take;
                if ((at += take) == iov[i].iov_len) {
                    i++;
                    at = 0;
                }
            }
            direct = !c->tx_loop && !c->tx_worker && c->fd >= 0;
            if (direct && c->streaming && c->out.len >= HANDOFF_MIN &&
                atomic_load_explicit(&streaming_conns, memory_order_relaxed) == 1) {
                c->tx_loop = true; /* the loop owns `out` now, as after a send that left bytes in it */
                direct = false;
            }
            if (direct) {
                c->tx_worker = true;
            } else {
                wake = !c->notified;
                c->notified = true;
            }
        }
        pthread_mutex_unlock(&c->lock);
        if (direct)
            wake = out_send(c);
        if (wake)
            notify(c, false);
        if (!ok)
            return false;
    }
}

/* Small frames of one QUERY_MULTI are gathered here and reach c->out in batches of at least OB_FLUSH bytes: one
 * lock and one wakeup per batch rather than per series. Frames larger than OB_DIRECT go straight to the output. */
#define OB_FLUSH (64u << 10)
#define OB_DIRECT (64u << 10)
typedef struct obatch {
    conn *c;
    uint8_t *p;
    size_t len, cap;
} obatch;

static bool ob_flush(obatch *ob)
{
    if (!ob || !ob->len)
        return !ob || !atomic_load(&ob->c->closing);
    struct iovec v = {ob->p, ob->len};
    ob->len = 0;
    return out_put(ob->c, &v, 1);
}

/* One reply frame: status, the u32 index of a QUERY_MULTI series when ix is not NULL, then up to three pieces of
 * body. With ob, a small frame is gathered there. Returns false when the connection is going away. */
static bool frame_put(conn *c, obatch *ob, uint32_t id, int32_t status, const uint32_t *ix, const void *a,
                      size_t alen, const void *b, size_t blen, const void *d, size_t dlen)
{
    size_t pre = ix ? TSDB_MULTI_PREFIX : 0;
    uint8_t ub[2 + TSDB_UNAVAIL_ENTRY]; /* the body of UNAVAILABLE (protocol 13), after the op's own error shape */
    size_t ulen = 0;
    /* Only a connection that said version 13 in its HELLO reads the body (a 12 client takes a longer error reply
     * as corrupt), and in QUERY_MULTI only the END frame carries it: a per-series frame is the op's bare shape. */
    if (status == TSDB_ERR_UNAVAILABLE && atomic_load(&c->proto) >= 13 && (!ix || *ix == TSDB_MULTI_END)) {
        tsdb_unavail e = unav_entry(unav_vol);
        ulen = tsdb_wire_put_unavail(ub, sizeof ub, &e, 1);
    }
    size_t body = 4 + 1 + 4 + pre + alen + blen + dlen + ulen;
    if (body > TSDB_MAX_FRAME + pre) { /* callers check: a client could not read it, and the length would wrap */
        atomic_store(&c->closing, true);
        shutdown(c->fd, SHUT_RDWR);
        return false;
    }
    if (status)
        metrics_error(status);
    uint8_t hdr[17];
    put32(hdr, (uint32_t)body);
    put32(hdr + 4, id);
    hdr[8] = TSDB_OP_REPLY;
    memcpy(hdr + 9, &status, 4);
    if (ix)
        put32(hdr + 13, *ix);
    size_t hl = 13 + pre;
    if (ob && 4 + body <= OB_DIRECT) {
        if (ob->len + 4 + body > ob->cap) {
            size_t cap = ob->cap ? ob->cap : OB_FLUSH + OB_DIRECT;
            while (cap < ob->len + 4 + body)
                cap *= 2;
            uint8_t *np = realloc(ob->p, cap);
            if (!np) {
                atomic_store(&c->closing, true);
                shutdown(c->fd, SHUT_RDWR);
                return false;
            }
            ob->p = np;
            ob->cap = cap;
        }
        uint8_t *o = ob->p + ob->len;
        memcpy(o, hdr, hl);
        o += hl;
        if (alen)
            memcpy(o, a, alen);
        o += alen;
        if (blen)
            memcpy(o, b, blen);
        o += blen;
        if (dlen)
            memcpy(o, d, dlen);
        o += dlen;
        if (ulen)
            memcpy(o, ub, ulen);
        ob->len += 4 + body;
        return ob->len >= OB_FLUSH ? ob_flush(ob) : !atomic_load(&c->closing);
    }
    if (ob && !ob_flush(ob))
        return false;
    struct iovec v[5] = {{hdr, hl}, {(void *)a, alen}, {(void *)b, blen}, {(void *)d, dlen}, {ub, ulen}};
    return out_put(c, v, 5);
}

/* Appends a complete reply frame to c's output, waiting while the output is over OUT_CAP. Returns false when the
 * connection is going away. */
static bool reply(conn *c, uint32_t id, int32_t status, const void *a, size_t alen, const void *b, size_t blen,
                  const void *d, size_t dlen)
{
    return frame_put(c, NULL, id, status, NULL, a, alen, b, blen, d, dlen);
}

static void reply_status(conn *c, uint32_t id, int32_t status) { reply(c, id, status, NULL, 0, NULL, 0, NULL, 0); }

/* ------------------------------------------------------------- requests */

typedef struct raw_out {
    conn *c;
    uint32_t id;
    int64_t *ts;
    float *v;
    uint32_t n;
    bool gone;
} raw_out;

static bool raw_flush(raw_out *o, bool last, int32_t status)
{
    uint8_t h[5];
    h[0] = last;
    put32(h + 1, o->n);
    bool ok = reply(o->c, o->id, status, h, 5, o->ts, (size_t)o->n * 8, o->v, (size_t)o->n * 4);
    o->n = 0;
    return ok;
}

static int raw_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    raw_out *o = ctx;
    while (n) {
        size_t take = RAW_CHUNK_ROWS - o->n < n ? RAW_CHUNK_ROWS - o->n : n;
        memcpy(o->ts + o->n, ts, take * 8);
        memcpy(o->v + o->n, v, take * 4);
        o->n += (uint32_t)take;
        ts += take;
        v += take;
        n -= take;
        if (o->n == RAW_CHUNK_ROWS && !raw_flush(o, false, 0)) {
            o->gone = true;
            return 1; /* the client went away: stop the query */
        }
    }
    return 0;
}

/* QUERY_VALUES output: chunks of typed rows with a validity bitmap when they hold nulls. */
typedef struct values_out {
    conn *c;
    uint32_t id;
    uint8_t type, any_null;
    int64_t *ts;
    uint8_t *v; /* values, then room for the bitmap */
    uint32_t n;
    bool gone;
    obatch *ob;        /* QUERY_MULTI: frames gathered here, each with the series' index */
    const uint32_t *ix;
} values_out;

static bool values_flush(values_out *o, bool last, int32_t status)
{
    unsigned aw = tsdb_wire_width(o->type);
    size_t vb = (size_t)o->n * aw;
    uint8_t h[8] = {last, o->type, (uint8_t)(o->any_null ? TSDB_VALID : 0), 0};
    put32(h + 4, o->n);
    size_t valid = o->any_null ? ((size_t)o->n + 7) / 8 : 0;
    if (valid)
        memmove(o->v + vb, o->v + (size_t)RAW_CHUNK_ROWS * 8, valid); /* the bitmap right after the values */
    bool ok = frame_put(o->c, o->ob, o->id, status, o->ix, h, 8, o->ts, (size_t)o->n * 8, o->v, vb + valid);
    o->n = 0;
    o->any_null = 0;
    if (o->v) /* NULL for an error reply before any rows */
        memset(o->v + (size_t)RAW_CHUNK_ROWS * 8, 0, RAW_CHUNK_ROWS / 8);
    return ok;
}

static int values_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    values_out *o = ctx;
    unsigned aw = tsdb_wire_width(o->type);
    uint8_t *bits = o->v + (size_t)RAW_CHUNK_ROWS * 8;
    for (size_t i = 0; i < n; i++) {
        bool known = !col->valid || ((col->valid[i >> 3] >> (i & 7)) & 1);
        o->ts[o->n] = ts[i];
        memcpy(o->v + (size_t)o->n * aw, (const uint8_t *)col->values + i * aw, aw);
        bits[o->n >> 3] |= (uint8_t)(known << (o->n & 7));
        o->any_null |= !known;
        if (++o->n == RAW_CHUNK_ROWS && !values_flush(o, false, 0)) {
            o->gone = true;
            return 1; /* the client went away: stop the query */
        }
    }
    return 0;
}

/* A HELLO from a client below TSDB_PROTOCOL_MIN (version 11 widened every series id: no request of an older
 * client means what it says). One without a version asks for the server's and is answered. */
static bool hello_refused(const uint8_t *p, size_t len)
{
    uint32_t v;
    return len >= 4 && (memcpy(&v, p, 4), v < TSDB_PROTOCOL_MIN);
}

/* The u64 ids of a many-series insert: after `end` bytes of the payload, zero bytes to a multiple of 8, then n ids
 * (8-aligned in the malloc'd payload, so read in place). NULL: the frame is not exactly that long. */
static const tsdb_sid *frame_ids(const uint8_t *p, size_t len, size_t end, uint32_t n)
{
    size_t sofs = (end + 7) & ~(size_t)7;
    if (len != sofs + (size_t)n * 8)
        return NULL;
    for (size_t i = end; i < sofs; i++)
        if (p[i])
            return NULL;
    return (const tsdb_sid *)(const void *)(p + sofs);
}

/* A packed INSERT_VALUES: unzstd, unshape into plain arrays, insert. */
static int insert_packed(auth_conn *a, const uint8_t *p, size_t len, uint32_t n, tsdb_sid s)
{
    unsigned type = p[4];
    bool valid = p[5] & TSDB_VALID, many = s == TSDB_MANY_SERIES;
    uint32_t raw_len;
    if (len < TSDB_VALUES_HEADER + 4)
        return TSDB_ERR_INVALID;
    memcpy(&raw_len, p + TSDB_VALUES_HEADER, 4);
    const uint8_t *z = p + TSDB_VALUES_HEADER + 4;
    size_t zlen = len - TSDB_VALUES_HEADER - 4;
    unsigned w = tsdb_wire_width(type);
    /* The rows must fit a frame unpacked, as clients split them (INSERT_FRAME_ROWS); the packed form holds at
     * least a byte per timestamp and the values' bytes. Checked before anything is sized by n or raw_len: a
     * zstd frame of a few kilobytes can claim gigabytes. */
    size_t unpacked = TSDB_VALUES_HEADER + (size_t)n * (8 + w) + (valid ? ((size_t)n + 7) / 8 : 0);
    if (many)
        unpacked = ((unpacked + 7) & ~(size_t)7) + (size_t)n * 8;
    if (unpacked > TSDB_MAX_FRAME || raw_len < (size_t)n * (1 + w) ||
        raw_len > tsdb_pack_raw_bound(n, type, valid, many) || ZSTD_getFrameContentSize(z, zlen) != raw_len)
        return TSDB_ERR_INVALID;
    size_t held = raw_len + unpacked;
    inflate_acquire(held);
    uint8_t *raw = malloc(raw_len ? raw_len : 1);
    int64_t *ts = malloc((size_t)n * 8 + 8);
    uint8_t *vals = malloc((size_t)n * w + (size_t)n / 8 + 9); /* values, then the bitmap */
    tsdb_sid *series = many ? malloc((size_t)n * 8 + 8) : NULL;
    int rc = !raw || !ts || !vals || (many && !series) ? TSDB_ERR_NOMEM : 0;
    if (rc == 0) {
        size_t got = ZSTD_decompress(raw, raw_len, z, zlen);
        uint8_t *bits = vals + (size_t)n * w;
        if (ZSTD_isError(got) || got != raw_len ||
            tsdb_pack_unshape(raw, raw_len, n, type, valid, many, ts, vals, bits, series) != 0) {
            rc = TSDB_ERR_INVALID;
        } else if (many && (rc = auth_ids(a, series, n, R_INSERT)) != 0) {
            /* one row of a series the user may not insert into refuses the whole frame (A16) */
        } else {
            tsdb_values col = {(tsdb_type)type, vals, valid ? bits : NULL};
            uint16_t list[DOM_MAX_VOLS];
            size_t nl = 0;
            rc = many ? dom_enter_ids(series, n, list, &nl) : 0; /* a volume of a domain: handle() entered the one */
            if (rc == 0) {
                rc = many ? tsdb_insert_values(db, series, ts, &col, n) : tsdb_insert_series_values(db, s, ts, &col, n);
                dom_exit_all(list, nl);
            }
        }
    }
    free(raw);
    free(ts);
    free(vals);
    free(series);
    inflate_release(held);
    return rc;
}

/* fields: 0 (bb: tsdb_bucket) or TSDB_SPREAD (bb: tsdb_spread_bucket, version 8), which appends the spread
 * to every row */
/* The most bucket rows of `row` bytes that one reply frame carries (after its count). A reply with more buckets is
 * refused (TSDB_ERR_TOO_MANY_BUCKETS) rather than built. A request with a larger limit computes at most one bucket
 * more than a frame carries (the limit it runs with). One without a limit runs as it does embedded, so it is
 * refused at once when its span exceeds the engine's bucket limit, and its work and memory follow the buckets
 * with rows (src/query.c bq_window); running it with the frame as its limit would instead walk a sparse span
 * window by window that the engine refuses as a whole. */
static size_t frame_rows(size_t row) { return (TSDB_MAX_FRAME - 9 - 4) / row; }
static size_t frame_limit(uint64_t lim, size_t row)
{
    size_t cap = frame_rows(row);
    return lim > cap ? cap + 1 : (size_t)lim;
}

/* Bucket and TWA replies are the structs themselves: their layout is the wire's (little endian, no padding). */
_Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "the wire is little endian");
_Static_assert(sizeof(tsdb_bucket) == TSDB_BUCKET_ROW && offsetof(tsdb_bucket, count) == 8 &&
                   offsetof(tsdb_bucket, sum) == 16 && offsetof(tsdb_bucket, min) == 24 &&
                   offsetof(tsdb_bucket, max) == 32 && offsetof(tsdb_bucket, isum) == 40 &&
                   offsetof(tsdb_bucket, imin) == 48 && offsetof(tsdb_bucket, imax) == 56,
               "tsdb_bucket is a QUERY_BUCKETS row");
_Static_assert(sizeof(tsdb_spread_bucket) == TSDB_SPREAD_ROW && offsetof(tsdb_spread_bucket, range) == 64 &&
                   offsetof(tsdb_spread_bucket, var_pop) == 72 && offsetof(tsdb_spread_bucket, var_samp) == 80 &&
                   offsetof(tsdb_spread_bucket, stddev_pop) == 88 && offsetof(tsdb_spread_bucket, stddev_samp) == 96,
               "tsdb_spread_bucket is a TSDB_SPREAD row");
_Static_assert(sizeof(tsdb_twa_bucket) == TSDB_TWA_ROW && offsetof(tsdb_twa_bucket, twa) == 8 &&
                   offsetof(tsdb_twa_bucket, area) == 16 && offsetof(tsdb_twa_bucket, covered_ms) == 24 &&
                   offsetof(tsdb_twa_bucket, count) == 32,
               "tsdb_twa_bucket is a QUERY_TWA row");

/* nb rows of `row` bytes in one frame (with the index of a QUERY_MULTI series when ix is not NULL); too many for a
 * frame: TOO_MANY_BUCKETS. Returns false when the connection is going away. */
static bool reply_rows(conn *c, obatch *ob, uint32_t id, const uint32_t *ix, const void *rows, size_t nb, size_t row)
{
    if (nb > frame_rows(row))
        return frame_put(c, ob, id, TSDB_ERR_TOO_MANY_BUCKETS, ix, NULL, 0, NULL, 0, NULL, 0);
    uint32_t n = (uint32_t)nb;
    return frame_put(c, ob, id, 0, ix, &n, 4, rows, nb * row, NULL, 0);
}

static void reply_buckets(conn *c, uint32_t id, const void *bb, size_t nb, uint8_t fields)
{
    reply_rows(c, NULL, id, NULL, bb, nb, fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW);
}

/* A query request, parsed. The single handlers and QUERY_MULTI (whose template is the same payload) share the
 * parsers below, so both accept exactly the same requests. */
typedef struct qreq {
    tsdb_sid series;
    int64_t start, end, interval;
    tsdb_query_opts q;
    uint64_t threshold; /* LTTB */
    uint8_t unit, fields;
    uint32_t count;
    char *zone;       /* QUERY_BUCKETS; owned */
    int64_t *bounds;  /* QUERY_BOUNDS, QUERY_TWA; owned, aligned */
    uint32_t nbounds;
    tsdb_twa_opts twa;
} qreq;

static void qreq_free(qreq *r)
{
    free(r->zone);
    free(r->bounds);
}

static int copy_bounds(qreq *r, const uint8_t *p, uint32_t n)
{
    r->nbounds = n;
    if (!n)
        return 0;
    r->bounds = malloc((size_t)n * 8);
    if (!r->bounds)
        return TSDB_ERR_NOMEM;
    memcpy(r->bounds, p, (size_t)n * 8);
    return 0;
}

/* The largest LTTB threshold tsdbd computes (--lttb-max-threshold); a larger one is taken as this. Newest first
 * holds the chosen points until the last is chosen (8 bytes and the value's width each), so a threshold near the
 * rows of a series of billions would hold gigabytes for one request, outside --max-request-memory. A range of no
 * more rows than the cap still answers every point; one of more answers at most the cap. */
static uint64_t lttb_max_threshold = 16777216;

/* QUERY_VALUES and QUERY_LTTB (the latter: the threshold, then optional order and limit since version 9) */
static int parse_values(const uint8_t *p, size_t len, bool lttb, qreq *r)
{
    *r = (qreq){.q = {TSDB_ASC, 0}};
    if (lttb ? len != 32 && len != 41 : len != 24 && len != 33)
        return TSDB_ERR_INVALID;
    memcpy(&r->series, p, 8);
    memcpy(&r->start, p + 8, 8);
    memcpy(&r->end, p + 16, 8);
    if (lttb)
        memcpy(&r->threshold, p + 24, 8);
    if (len == 33 || len == 41) {
        uint64_t lim;
        size_t at = lttb ? 32 : 24;
        r->q.order = p[at] ? TSDB_DESC : TSDB_ASC;
        memcpy(&lim, p + at + 1, 8);
        r->q.limit = lim > SIZE_MAX ? SIZE_MAX : (size_t)lim;
    }
    if (lttb && r->threshold > lttb_max_threshold)
        r->threshold = lttb_max_threshold;
    return lttb && r->threshold > SIZE_MAX ? TSDB_ERR_INVALID : 0;
}

/* QUERY_BUCKETS: interval or UTC calendar groups, optional fields (version 8) after the zone */
static int parse_buckets(const uint8_t *p, size_t len, qreq *r)
{
    *r = (qreq){0};
    uint16_t zl;
    uint64_t lim;
    if (len < 48 || (memcpy(&zl, p + 46, 2), len != 48u + zl && len != 49u + zl) || memchr(p + 48, 0, zl) ||
        (len == 49u + zl && ((r->fields = p[48 + zl]) & ~TSDB_SPREAD)))
        return TSDB_ERR_INVALID;
    memcpy(&r->series, p, 8);
    memcpy(&r->start, p + 8, 8);
    memcpy(&r->end, p + 16, 8);
    memcpy(&r->interval, p + 24, 8);
    r->unit = p[32];
    memcpy(&r->count, p + 33, 4);
    memcpy(&lim, p + 38, 8);
    r->q = (tsdb_query_opts){p[37] ? TSDB_DESC : TSDB_ASC,
                             frame_limit(lim, r->fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW)};
    if (zl && !(r->zone = strndup((const char *)p + 48, zl)))
        return TSDB_ERR_NOMEM;
    return 0;
}

/* QUERY_BOUNDS: client bounds, optional fields after them */
static int parse_bounds(const uint8_t *p, size_t len, qreq *r)
{
    *r = (qreq){0};
    uint32_t n;
    uint64_t lim;
    if (len < 37 || (memcpy(&n, p + 33, 4), len != 37 + (size_t)n * 8 && len != 38 + (size_t)n * 8) || n == 0 ||
        (len == 38 + (size_t)n * 8 && ((r->fields = p[37 + (size_t)n * 8]) & ~TSDB_SPREAD)))
        return TSDB_ERR_INVALID;
    memcpy(&r->series, p, 8);
    memcpy(&r->start, p + 8, 8);
    memcpy(&r->end, p + 16, 8);
    memcpy(&lim, p + 25, 8);
    r->q = (tsdb_query_opts){p[24] ? TSDB_DESC : TSDB_ASC,
                             frame_limit(lim, r->fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW)};
    return copy_bounds(r, p + 37, n);
}

/* QUERY_TWA: the interval, calendar and bounds forms */
static int parse_twa(const uint8_t *p, size_t len, qreq *r)
{
    *r = (qreq){0};
    uint32_t n;
    int64_t gap;
    uint64_t lim;
    if (len < TSDB_TWA_HEADER || (memcpy(&n, p + 56, 4), len != TSDB_TWA_HEADER + (size_t)n * 8) || p[27])
        return TSDB_ERR_INVALID;
    memcpy(&r->series, p, 8);
    memcpy(&r->start, p + 8, 8);
    memcpy(&r->end, p + 16, 8);
    memcpy(&gap, p + 28, 8);
    memcpy(&lim, p + 36, 8);
    memcpy(&r->interval, p + 44, 8);
    memcpy(&r->count, p + 52, 4);
    r->twa = (tsdb_twa_opts){.method = (tsdb_twa_method)p[24], .max_gap_ms = gap,
                             .order = p[25] ? TSDB_DESC : TSDB_ASC, .limit = (size_t)lim};
    r->unit = p[26];
    if ((n && (r->unit || r->interval)) || (r->unit && r->interval))
        return TSDB_ERR_INVALID;
    return copy_bounds(r, p + TSDB_TWA_HEADER, n);
}

static int handle_twa(conn *c, uint32_t id, const uint8_t *p, size_t len)
{
    qreq r;
    tsdb_twa_bucket *b = NULL;
    size_t nb = 0;
    int rc = parse_twa(p, len, &r);
    if (rc == 0) {
        if (r.nbounds) {
            rc = tsdb_query_twa_bounds(db, r.series, r.start, r.end, r.bounds, r.nbounds, &r.twa, &b, &nb);
        } else if (r.unit) {
            tsdb_group g = {(tsdb_unit)r.unit, r.count, NULL};
            rc = tsdb_query_twa_group(db, r.series, r.start, r.end, &g, &r.twa, &b, &nb);
        } else {
            rc = tsdb_query_twa(db, r.series, r.start, r.end, r.interval, &r.twa, &b, &nb);
        }
    }
    qreq_free(&r);
    if (rc == 0)
        reply_rows(c, NULL, id, NULL, b, nb, TSDB_TWA_ROW);
    free(b);
    return rc;
}

/* ------------------------------------------------------------ QUERY_MULTI */

static uint64_t multi_max_rows = 268435456; /* --multi-max-rows */

typedef struct mctx {
    conn *c;
    uint32_t id;
    uint8_t op, fields;
    obatch ob;
    values_out vo; /* VALUES, LTTB: the chunks of the series at the head */
    size_t cur;    /* the index vo is for, or SIZE_MAX */
    uint32_t answered;
    bool gone;
    const uint8_t *denied; /* a bit per series the user may not read (asked as TSDB_SID_NONE); NULL: none */
} mctx;

static int multi_fn(void *ctx, const tsdb_series_result *r)
{
    mctx *m = ctx;
    if (m->gone)
        return 1;
    uint32_t ix = (uint32_t)r->index;
    tsdb_series_result dr;
    if (m->denied && (m->denied[ix >> 3] >> (ix & 7) & 1)) { /* its own status, the others answered (A17) */
        dr = *r;
        dr.status = TSDB_ERR_DENIED;
        dr.n = 0;
        r = &dr;
    }
    bool ok = true;
    if (m->op == TSDB_OP_QUERY_VALUES || m->op == TSDB_OP_QUERY_LTTB) {
        values_out *o = &m->vo;
        if (m->cur != r->index) { /* a new series: its frames carry its type, as a single request's do */
            tsdb_type t = TSDB_FLOAT32;
            tsdb_series_type(db, r->series, &t);
            o->type = (uint8_t)t;
            m->cur = r->index;
        }
        o->ix = &ix;
        if (r->n && !r->status && values_cb(o, r->ts, &r->values, r->n) != 0)
            ok = false;
        if (ok && r->last)
            ok = values_flush(o, true, r->status); /* the rest of the rows, or the error */
        o->ix = NULL;
    } else if (r->status) {
        ok = frame_put(m->c, &m->ob, m->id, r->status, &ix, NULL, 0, NULL, 0, NULL, 0);
    } else {
        size_t row = m->op == TSDB_OP_QUERY_TWA ? TSDB_TWA_ROW
                     : m->fields & TSDB_SPREAD  ? TSDB_SPREAD_ROW
                                                : TSDB_BUCKET_ROW;
        ok = reply_rows(m->c, &m->ob, m->id, &ix, r->buckets, r->n, row);
    }
    if (!ok) {
        m->gone = true;
        return 1;
    }
    if (r->last)
        m->answered++;
    return 0;
}

static int multi_cancelled(void *ctx)
{
    conn *c = ((mctx *)ctx)->c;
    return atomic_load_explicit(&c->cancel_running, memory_order_relaxed) || atomic_load(&c->closing);
}

static void multi_on_wait(void *ctx, int delta)
{
    mctx *m = ctx;
    if (delta > 0 && !ob_flush(&m->ob)) /* what is ready goes out while the head is computed */
        m->gone = true;
    worker_waits(delta);
}

/* Results held ahead of the head count against --max-request-memory; refused, the request goes on head-only. */
static int multi_budget_take(void *ctx, size_t n)
{
    (void)ctx;
    size_t cur = atomic_load_explicit(&req_held, memory_order_relaxed);
    do {
        if (cur + n > req_budget)
            return 1;
    } while (!atomic_compare_exchange_weak_explicit(&req_held, &cur, cur + n, memory_order_relaxed,
                                                    memory_order_relaxed));
    return 0;
}

static void multi_budget_give(void *ctx, size_t n)
{
    (void)ctx;
    atomic_fetch_sub_explicit(&req_held, n, memory_order_relaxed);
}

static void multi_on_deferred(void *ctx)
{
    (void)ctx;
    metrics_multi_deferred();
}

/* The multi query of a QUERY_MULTI request: the header's arrays (aligned in the payload, which malloc returned)
 * and the template, parsed by the single op's parser into qr. */
static int multi_parse(const uint8_t *p, size_t len, tsdb_multi_query *q, qreq *qr, const tsdb_sid **series,
                       uint32_t *np, uint8_t *op, uint16_t *threads, uint64_t *max_rows)
{
    *qr = (qreq){0};
    if (len < TSDB_MULTI_HEADER)
        return TSDB_ERR_INVALID;
    uint8_t flags = p[1];
    uint32_t n, tlen, zero;
    *op = p[0];
    memcpy(threads, p + 2, 2);
    memcpy(&n, p + 4, 4);
    memcpy(max_rows, p + 8, 8);
    memcpy(&tlen, p + 16, 4);
    memcpy(&zero, p + 20, 4);
    if (zero || n == 0 || n > TSDB_MULTI_MAX_SERIES || (flags & ~31u))
        return TSDB_ERR_INVALID;
    size_t at = TSDB_MULTI_HEADER;
    const int64_t *arr[3] = {NULL, NULL, NULL};
    for (int k = 0; k < 3; k++)
        if (flags & (1u << k)) {
            arr[k] = (const int64_t *)(const void *)(p + at);
            at += (size_t)n * 8;
        }
    size_t sat = at; /* 24 + 8k: the ids are 8-aligned in the malloc'd payload */
    at += (size_t)n * 8;
    if (len < at || len - at != tlen)
        return TSDB_ERR_INVALID;
    *series = (const tsdb_sid *)(const void *)(p + sat);
    *np = n;
    const uint8_t *t = p + at;
    bool per_iv = flags & TSDB_MULTI_PER_INTERVAL;
    unsigned trim = (flags & TSDB_MULTI_WIRE_TRIM_LO ? TSDB_MULTI_TRIM_LO : 0) |
                    (flags & TSDB_MULTI_WIRE_TRIM_HI ? TSDB_MULTI_TRIM_HI : 0);
    int rc;
    *q = (tsdb_multi_query){0};
    switch (*op) {
    case TSDB_OP_QUERY_VALUES:
    case TSDB_OP_QUERY_LTTB:
        rc = parse_values(t, tlen, *op == TSDB_OP_QUERY_LTTB, qr);
        if (rc == 0 && (per_iv || trim))
            rc = TSDB_ERR_INVALID;
        q->kind = *op == TSDB_OP_QUERY_LTTB ? TSDB_MULTI_LTTB : TSDB_MULTI_VALUES;
        q->opts = qr->q;
        q->threshold = (size_t)qr->threshold;
        break;
    case TSDB_OP_QUERY_BUCKETS:
        rc = parse_buckets(t, tlen, qr);
        /* the server knows UTC only: a zone other than UTC is the single request's INVALID */
        if (rc == 0 && ((qr->zone && strcmp(qr->zone, "UTC") != 0) || trim || (per_iv && (qr->unit || qr->interval))))
            rc = TSDB_ERR_INVALID;
        q->kind = qr->fields & TSDB_SPREAD ? TSDB_MULTI_SPREAD : TSDB_MULTI_BUCKETS;
        q->opts = qr->q;
        q->interval_ms = qr->interval;
        break;
    case TSDB_OP_QUERY_BOUNDS:
        rc = parse_bounds(t, tlen, qr);
        if (rc == 0 && per_iv)
            rc = TSDB_ERR_INVALID;
        q->kind = qr->fields & TSDB_SPREAD ? TSDB_MULTI_SPREAD : TSDB_MULTI_BUCKETS;
        q->opts = qr->q;
        break;
    case TSDB_OP_QUERY_TWA:
        rc = parse_twa(t, tlen, qr);
        if (rc == 0 && ((trim && !qr->nbounds) || (per_iv && (qr->nbounds || qr->unit || qr->interval))))
            rc = TSDB_ERR_INVALID;
        q->kind = TSDB_MULTI_TWA;
        q->twa = qr->twa;
        q->interval_ms = qr->interval;
        break;
    default: return TSDB_ERR_INVALID;
    }
    if (rc)
        return rc;
    /* the template names no series, and a field a per-series array replaces is 0 in it */
    if (qr->series != TSDB_MANY_SERIES || (arr[0] && qr->start) || (arr[1] && qr->end))
        return TSDB_ERR_INVALID;
    q->start = qr->start;
    q->end = qr->end;
    q->starts = arr[0];
    q->ends = arr[1];
    q->intervals = arr[2];
    q->bounds = qr->bounds;
    q->nbounds = qr->nbounds;
    q->trim = trim;
    return 0;
}

static void handle_multi(conn *c, req *r)
{
    static const uint32_t end_ix = TSDB_MULTI_END;
    mctx m = {.c = c, .id = r->id, .ob = {.c = c}, .cur = SIZE_MAX};
    tsdb_multi_query q;
    qreq qr;
    tsdb_group g = {0};
    const tsdb_sid *series = NULL;
    uint32_t n = 0;
    uint16_t threads = 0;
    uint64_t max_rows = 0;
    int rc = multi_parse(r->payload, r->len, &q, &qr, &series, &n, &m.op, &threads, &max_rows);
    tsdb_sid *own = NULL;
    uint8_t *denied = NULL;
    if (rc == 0 && !auth_all(&c->auth, R_READ)) {
        /* a series the user may not read is asked as an id that names none (status INVALID, no rows), which
         * multi_fn turns into DENIED: unknown and foreign ids answer alike (A16) */
        own = malloc((size_t)n * sizeof *own);
        denied = calloc(((size_t)n + 7) / 8, 1);
        if (!own || !denied)
            rc = TSDB_ERR_NOMEM;
        for (uint32_t i = 0; rc == 0 && i < n; i++) {
            bool no = auth_id(&c->auth, series[i], R_READ) != 0;
            own[i] = no ? TSDB_SID_NONE : series[i];
            denied[i >> 3] |= (uint8_t)(no << (i & 7));
        }
        series = own;
        m.denied = denied;
    }
    if (rc == 0 && (m.op == TSDB_OP_QUERY_VALUES || m.op == TSDB_OP_QUERY_LTTB)) {
        m.vo = (values_out){.c = c, .id = r->id, .ob = &m.ob};
        m.vo.ts = malloc(RAW_CHUNK_ROWS * 8);
        m.vo.v = calloc((size_t)RAW_CHUNK_ROWS * 8 + RAW_CHUNK_ROWS / 8, 1);
        if (!m.vo.ts || !m.vo.v)
            rc = TSDB_ERR_NOMEM;
    }
    if (rc == 0) {
        m.fields = qr.fields;
        if ((m.op == TSDB_OP_QUERY_BUCKETS || m.op == TSDB_OP_QUERY_TWA) && qr.unit) {
            g = (tsdb_group){(tsdb_unit)qr.unit, qr.count, NULL}; /* UTC */
            q.group = &g;
        }
        tsdb_multi_opts mo = {.threads = threads,
                              .max_rows = max_rows == 0 || max_rows > multi_max_rows ? multi_max_rows : max_rows,
                              .cancelled = multi_cancelled,
                              .on_wait = multi_on_wait,
                              .budget_take = multi_budget_take,
                              .budget_give = multi_budget_give,
                              .on_deferred = multi_on_deferred,
                              .hook_ctx = &m};
        /* the end frame and the last batch go out after it, by whoever owns the output then (out_put) */
        c->streaming = true;
        atomic_fetch_add(&streaming_conns, 1);
        rc = tsdb_query_multi(db, &q, series, n, &mo, multi_fn, &m);
        atomic_fetch_sub(&streaming_conns, 1);
        c->streaming = false;
    }
    qreq_free(&qr);
    free(m.vo.ts);
    free(m.vo.v);
    free(own);
    free(denied);
    metrics_multi_series(m.answered);
    if (!m.gone)
        frame_put(c, &m.ob, r->id, rc, &end_ix, &m.answered, 4, NULL, 0, NULL, 0);
    ob_flush(&m.ob);
    free(m.ob.p);
}

/* ------------------------------------------------------------ QUERY_LATEST */

/* The newest value of each of many series (protocol 14, docs/design-latest.md 6.2). The ids are answered in chunks
 * of TSDB_LATEST_FRAME_IDS, each a frame, so a million ids are never held as one reply; CANCEL is looked at between
 * chunks. A denied id is asked as TSDB_SID_NONE (NOTFOUND) and turned into DENIED, as QUERY_MULTI does (A16, A17). */
static void latest_put(uint8_t *o, const tsdb_latest_result *r)
{
    uint64_t v = 0;
    if (r->status == 0 && r->kind == TSDB_LATEST_VALUE) {
        switch (r->type) {
        case TSDB_FLOAT64: memcpy(&v, &r->v.f64, 8); break;
        case TSDB_INT64: v = (uint64_t)r->v.i64; break;
        case TSDB_BOOL: v = r->v.b; break;
        case TSDB_STATE: v = r->v.state; break;
        default: memcpy(&v, &r->v.f32, 4); break; /* float32: its bits in the low 4 bytes */
        }
    }
    int32_t st = r->status;
    memcpy(o, &st, 4);
    o[4] = r->status ? (uint8_t)TSDB_LATEST_NONE : r->kind;
    o[5] = r->status ? 0 : r->type;
    o[6] = o[7] = 0;
    int64_t ts = r->status || r->kind == TSDB_LATEST_NONE ? 0 : r->ts;
    memcpy(o + 8, &ts, 8);
    memcpy(o + 16, &v, 8);
}

/* One frame of the reply: the request's status, first, m, last, then the records. */
static bool latest_frame(conn *c, uint32_t id, int32_t status, uint32_t first, uint32_t m, bool last,
                         const uint8_t *rec)
{
    uint8_t h[TSDB_LATEST_PREFIX] = {0};
    memcpy(h, &first, 4);
    memcpy(h + 4, &m, 4);
    h[8] = last;
    return frame_put(c, NULL, id, status, NULL, h, sizeof h, rec, (size_t)m * TSDB_LATEST_REC, NULL, 0);
}

static void handle_latest(conn *c, req *r)
{
    const uint8_t *p = r->payload;
    int rc = 0;
    uint32_t n = 0;
    uint8_t flags = 0;
    uint16_t threads = 0;
    int64_t end = 0;
    const int64_t *ends = NULL;
    const tsdb_sid *ids = NULL;
    if (r->len < TSDB_LATEST_HEADER) {
        rc = TSDB_ERR_INVALID;
    } else {
        flags = p[0];
        memcpy(&threads, p + 2, 2);
        memcpy(&n, p + 4, 4);
        memcpy(&end, p + 8, 8);
        bool per = flags & TSDB_LATEST_PER_END;
        size_t want = TSDB_LATEST_HEADER + (size_t)n * 8 * (per ? 2 : 1);
        if (p[1] || n == 0 || n > TSDB_LATEST_MAX_IDS ||
            (flags & ~(unsigned)(TSDB_LATEST_WITH_NULLS | TSDB_LATEST_PER_END)) || (per && end != 0) ||
            r->len != want) {
            rc = TSDB_ERR_INVALID;
        } else {
            /* the arrays are 8-aligned in the malloc'd payload */
            if (per)
                ends = (const int64_t *)(const void *)(p + TSDB_LATEST_HEADER);
            ids = (const tsdb_sid *)(const void *)(p + TSDB_LATEST_HEADER + (per ? (size_t)n * 8 : 0));
        }
    }
    if (rc) { /* a bad header: one frame, m = 0, last = 1 */
        latest_frame(c, r->id, rc, 0, 0, true, NULL);
        return;
    }
    tsdb_sid *own = NULL;
    uint8_t *denied = NULL;
    if (!auth_all(&c->auth, R_READ)) {
        own = malloc((size_t)n * sizeof *own);
        denied = calloc(((size_t)n + 7) / 8, 1);
        if (!own || !denied)
            rc = TSDB_ERR_NOMEM;
        for (uint32_t i = 0; rc == 0 && i < n; i++) {
            bool no = auth_id(&c->auth, ids[i], R_READ) != 0;
            own[i] = no ? TSDB_SID_NONE : ids[i];
            denied[i >> 3] |= (uint8_t)(no << (i & 7));
        }
        ids = own;
    }
    tsdb_latest_result *res = rc ? NULL : malloc((size_t)TSDB_LATEST_FRAME_IDS * sizeof *res);
    uint8_t *rec = rc ? NULL : malloc((size_t)TSDB_LATEST_FRAME_IDS * TSDB_LATEST_REC);
    if (rc == 0 && (!res || !rec))
        rc = TSDB_ERR_NOMEM;
    uint32_t first = 0;
    bool gone = false;
    while (rc == 0 && first < n) {
        if (atomic_load_explicit(&c->cancel_running, memory_order_relaxed) || atomic_load(&c->closing)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        uint32_t m = n - first < TSDB_LATEST_FRAME_IDS ? n - first : TSDB_LATEST_FRAME_IDS;
        tsdb_latest_opts lo = {.end = end,
                               .ends = ends ? ends + first : NULL,
                               .with_nulls = (flags & TSDB_LATEST_WITH_NULLS) != 0,
                               .threads = threads};
        int lrc = tsdb_latest(db, ids + first, m, &lo, res);
        if (lrc != 0) {
            rc = lrc;
            break;
        }
        for (uint32_t i = 0; i < m; i++) {
            uint32_t ix = first + i;
            if (denied && (denied[ix >> 3] >> (ix & 7) & 1))
                res[i] = (tsdb_latest_result){.status = TSDB_ERR_DENIED, .kind = TSDB_LATEST_NONE};
            latest_put(rec + (size_t)i * TSDB_LATEST_REC, &res[i]);
        }
        first += m;
        if (!latest_frame(c, r->id, 0, first - m, m, first == n, rec)) {
            gone = true;
            break;
        }
    }
    if (rc && !gone) /* the request's status in a closing frame with no records (earlier frames may have gone out) */
        latest_frame(c, r->id, rc, first, 0, true, NULL);
    free(res);
    free(rec);
    free(own);
    free(denied);
}

/* An error in the shape the op's replies have, so a client reads it as it reads that op's errors. */
static void reply_error(conn *c, uint32_t id, uint8_t op, int32_t status)
{
    switch (auth_ops[op].shape) {
    case S_RAW: {
        raw_out o = {c, id, NULL, NULL, 0, false};
        raw_flush(&o, true, status);
        return;
    }
    case S_VALUES: {
        values_out o = {c, id, 0, 0, NULL, NULL, 0, false, NULL, NULL};
        values_flush(&o, true, status);
        return;
    }
    case S_MULTI: {
        static const uint32_t end_ix = TSDB_MULTI_END;
        uint32_t none = 0;
        frame_put(c, NULL, id, status, &end_ix, &none, 4, NULL, 0, NULL, 0);
        return;
    }
    case S_LATEST: latest_frame(c, id, status, 0, 0, true, NULL); return;
    default: reply_status(c, id, status); return;
    }
}

static void peer_name(int fd, char *out, size_t cap)
{
    struct sockaddr_storage sa;
    socklen_t sl = sizeof sa;
    char host[INET6_ADDRSTRLEN] = "?";
    int port = 0;
    if (fd >= 0 && getpeername(fd, (struct sockaddr *)&sa, &sl) == 0) {
        if (sa.ss_family == AF_INET) {
            inet_ntop(AF_INET, &((struct sockaddr_in *)&sa)->sin_addr, host, sizeof host);
            port = ntohs(((struct sockaddr_in *)&sa)->sin_port);
        } else if (sa.ss_family == AF_INET6) {
            inet_ntop(AF_INET6, &((struct sockaddr_in6 *)&sa)->sin6_addr, host, sizeof host);
            port = ntohs(((struct sockaddr_in6 *)&sa)->sin6_port);
        }
    }
    snprintf(out, cap, "%s:%d", host, port);
}

typedef struct list_out {
    conn *c;
    uint32_t id;
} list_out;

static int list_emit(void *ctx, bool last, uint32_t n, const uint8_t *e, size_t len)
{
    list_out *o = ctx;
    uint8_t h[5] = {last};
    put32(h + 1, n);
    return reply(o->c, o->id, 0, h, 5, e, len, NULL, 0) ? 0 : TSDB_ERR_STOPPED;
}

/* The volume a request names by one series id (a name, many ids or no series: TSDB_VOL_ANY), for UNAVAILABLE. */
static uint16_t req_volume(uint8_t op, const uint8_t *p, size_t len)
{
    const auth_op *e = &auth_ops[op];
    size_t at = e->where == W_ID0 ? 0 : e->where == W_ID8 ? 8 : (size_t)-1;
    tsdb_sid id;
    if (at == (size_t)-1 || len < at + 8)
        return TSDB_VOL_ANY;
    memcpy(&id, p + at, 8);
    if (id == TSDB_MANY_SERIES || id < ((tsdb_sid)1 << 32) || (id >> 48) != 0 || TSDB_SID_VOLUME(id) >= TSDB_VOL_ANY)
        return TSDB_VOL_ANY;
    return (uint16_t)TSDB_SID_VOLUME(id);
}

static bool g_reader_only; /* --follow or --read-only: every volume is a FOLLOWER in TOPOLOGY */

/* TOPOLOGY of a server without a cluster (protocol 13): itself as node 1, every volume it has, owned by it, with
 * the health of its storage domain. Its address is empty (the node you are connected to). The paths go to a
 * superuser only. gen is a hash of the rest of the reply, so it changes exactly when the reply does. */
static void handle_topology(req *r)
{
    conn *c = r->c;
    uint64_t since;
    if (tsdb_wire_get_topology_request(r->payload, r->len, &since) != 0) {
        reply_status(c, r->id, TSDB_ERR_INVALID);
        return;
    }
    uint32_t nv = tsdb_volume_count(db);
    bool paths = auth_conn_super(&c->auth);
    size_t cap = 12 + 7 + 4 + (size_t)nv * (30 + 2 + (paths ? 4096 : 0));
    uint8_t *buf = malloc(cap);
    if (!buf) {
        reply_status(c, r->id, TSDB_ERR_NOMEM);
        return;
    }
    size_t at = tsdb_wire_put_topo_head(buf, cap, 0, 1);
    tsdb_topo_node node = {1, 1, "", 0};
    at += tsdb_wire_put_topo_node(buf + at, cap - at, &node);
    at += tsdb_wire_put_u32(buf + at, cap - at, nv);
    for (uint32_t v = 0; v < nv && v < TSDB_VOL_ANY; v++) {
        tsdb_topo_vol tv = {.vol = (uint16_t)v, .role = g_reader_only ? TSDB_ROLE_FOLLOWER : TSDB_ROLE_OWNER,
                            .owner = 1, .server = 0xffff, .addr = "", .path = ""};
        uint32_t retry;
        vol_health(v, &tv.health, &tv.reason, &tv.since_ms, &(uint64_t){0}, &retry, &tv.domain);
        const char *path = paths ? tsdb_volume_path(db, v) : NULL;
        if (path && strlen(path) <= 4096) {
            tv.path = path;
            tv.path_len = (uint16_t)strlen(path);
        }
        size_t n = tsdb_wire_put_topo_vol(buf + at, cap - at, &tv);
        if (n == 0 || n > cap - at)
            break; /* cannot happen: cap holds every volume at its largest */
        at += n;
    }
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 8; i < at; i++)
        h = (h ^ buf[i]) * 1099511628211ull;
    tsdb_wire_wr64(buf, h);
    reply(c, r->id, 0, buf, at, NULL, 0, NULL, 0);
    free(buf);
}

static const char *why_name(unsigned why)
{
    static const char *const names[] = {"none", "nfs_down", "not_mounted", "stuck", "full", "busy", "root_down",
                                        "locked", "recovering", "diverged", "maintenance", "suspect", "promoting",
                                        "fenced", "mismatch", "no_owner", "unreachable", "reloading"};
    return why <= TSDB_WHY_LAST ? names[why] : "unknown";
}

/* STATUS (superuser): key=value lines, ha.md 15.4. */
static void handle_status(req *r)
{
    conn *c = r->c;
    if (r->len != 0) {
        reply_status(c, r->id, TSDB_ERR_INVALID);
        return;
    }
    uint32_t nv = tsdb_volume_count(db);
    size_t cap = 512 + (size_t)nv * 128 + (size_t)(dom.ndoms > 0 ? dom.ndoms : 0) * 256;
    char *t = malloc(cap + 4);
    if (!t) {
        reply_status(c, r->id, TSDB_ERR_NOMEM);
        return;
    }
    size_t at = 4;
#define ST_PUT(...) (at += (size_t)snprintf(t + at, cap - at, __VA_ARGS__))
    ST_PUT("node.id=1\nnode.role=%s\nprotocol=%d\nvolumes=%u\n", g_reader_only ? "follower" : "owner",
           TSDB_PROTOCOL_VERSION, (unsigned)nv);
    for (uint32_t v = 0; v < nv && at + 160 < cap; v++) {
        uint8_t health, reason;
        int64_t since;
        uint64_t outage;
        uint32_t retry;
        uint16_t d;
        vol_health(v, &health, &reason, &since, &outage, &retry, &d);
        ST_PUT("vol.%u.health=%s\nvol.%u.reason=%s\n", (unsigned)v,
               health == TSDB_HEALTH_AVAILABLE ? "available" : health == TSDB_HEALTH_DEGRADED ? "degraded" : "unavailable",
               (unsigned)v, why_name(reason));
    }
    for (int d = 0; dom.health && d < dom.ndoms && at + 256 < cap; d++) {
        tsdb_domain_info di;
        if (tsdb_health_info(dom.health, d, &di) != 0)
            continue;
        static const char *const st[] = {"up", "degraded", "down", "recovering"};
        ST_PUT("domain.%d.state=%s\ndomain.%d.since_ms=%lld\ndomain.%d.outage_id=%llu\ndomain.%d.probe_ms=%d\n", d,
               di.state >= 0 && di.state <= 3 ? st[di.state] : "?", d, (long long)di.since_ms, d,
               (unsigned long long)di.outage_id, d, di.write_ms);
    }
#undef ST_PUT
    if (at > cap)
        at = cap; /* snprintf truncated: cannot happen with the margins above */
    tsdb_wire_wr32((uint8_t *)t, (uint32_t)(at - 4));
    reply(c, r->id, 0, t, at, NULL, 0, NULL, 0);
    free(t);
}

static void handle_op(req *r);
static void handle(req *r)
{
    conn *c = r->c;
    const uint8_t *p = r->payload;
    size_t len = r->len;
    int rc = 0;
    auth_conn *a = &c->auth;
    uint8_t was = a->state;
    atomic_store_explicit(&c->nonup_since, 0, memory_order_relaxed); /* a new request: its time inside a domain
                                                                         * that is not UP starts again (out_wait) */
    if ((rc = auth_fresh(a)) != 0 || (rc = auth_check(a, r->op, p, len)) != 0) {
        if (rc == TSDB_ERR_AUTH) { /* not logged in, or the user was dropped: the connection closes */
            if (was == AS_NONE || was == AS_STARTED)
                metrics_login(METRICS_LOGIN_REFUSED);
            atomic_store(&c->auth_end, true);
        }
        reply_error(c, r->id, r->op, rc);
        return;
    }
    unav_vol = req_volume(r->op, p, len);
    int dv = dom_volume_of(r->op, p, len); /* a volume of a storage domain: at most DOM_CAP requests inside */
    if (dv >= 0) {
        if ((rc = dom_enter(dv)) != 0) {
            reply_error(c, r->id, r->op, rc);
            return;
        }
        atomic_store(&c->dom_v, dv);
        handle_op(r);
        atomic_store(&c->dom_v, 0);
        dom_exit(dv);
        return;
    }
    /* An insert of many series admits itself by the volumes its rows name (a packed one inside insert_packed, once
     * its rows are unpacked); everything else that names no volume counts as a waiting worker while a domain is not
     * UP (a stuck call in a hung volume must not take every worker). */
    if (dom.ndoms > 0 && (r->op == TSDB_OP_INSERT || r->op == TSDB_OP_INSERT_VALUES) && len >= TSDB_VALUES_HEADER) {
        uint32_t n;
        tsdb_sid s;
        memcpy(&n, p, 4);
        memcpy(&s, p + 8, 8);
        if (s == TSDB_MANY_SERIES) {
            if (r->op == TSDB_OP_INSERT_VALUES && (p[5] & TSDB_PACKED)) {
                handle_op(r);
                return;
            }
            size_t end = r->op == TSDB_OP_INSERT
                             ? 16 + (size_t)n * 12
                             : TSDB_VALUES_HEADER + (size_t)n * 8 + (size_t)n * tsdb_wire_width(p[4]) +
                                   ((p[5] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0);
            const tsdb_sid *ids = p[4] <= TSDB_STATE || r->op == TSDB_OP_INSERT ? frame_ids(p, len, end, n) : NULL;
            if (ids) {
                uint16_t list[DOM_MAX_VOLS];
                size_t nl;
                /* rights before admission: a user without them is DENIED whatever the volume's state, or the
                 * answer would tell which volumes exist and are degraded (review m3) */
                if ((rc = auth_ids(&c->auth, ids, n, R_INSERT)) != 0) {
                    reply_error(c, r->id, r->op, rc);
                    return;
                }
                if ((rc = dom_enter_ids(ids, n, list, &nl)) != 0) {
                    reply_error(c, r->id, r->op, rc);
                    return;
                }
                handle_op(r);
                dom_exit_all(list, nl);
                return;
            }
        }
    }
    if (dom.ndoms > 0) {
        dom_any_enter();
        atomic_store(&c->dom_v, -1);
        handle_op(r);
        atomic_store(&c->dom_v, 0);
        dom_any_exit();
        return;
    }
    handle_op(r);
}

static void handle_op(req *r)
{
    conn *c = r->c;
    const uint8_t *p = r->payload;
    size_t len = r->len;
    int rc = 0;
    auth_conn *a = &c->auth;
    switch (r->op) {
    case TSDB_OP_HELLO: {
        /* the version both sides speak: a client never sends what it does not know, so an older one hears
         * its own version back. One below TSDB_PROTOCOL_MIN puts u32 ids where u64 ones are read: refused, and
         * parse() stopped reading the connection, which closes once this reply is sent. */
        if (hello_refused(p, len)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        uint32_t v = TSDB_PROTOCOL_VERSION, theirs;
        if (len >= 4) {
            memcpy(&theirs, p, 4);
            if (theirs < v)
                v = theirs;
        }
        atomic_store(&c->proto, v);
        uint8_t h[64]; /* the version, then whether login is required and how (version 12) */
        memcpy(h, &v, 4);
        size_t hl = 4 + auth_hello(h + 4);
        reply(c, r->id, 0, h, hl, NULL, 0, NULL, 0);
        return;
    }
    case TSDB_OP_AUTH_START:
    case TSDB_OP_AUTH_CONTINUE: {
        uint8_t out[640];
        size_t ol = 0;
        char peer[INET6_ADDRSTRLEN + 8];
        peer_name(c->fd, peer, sizeof peer);
        rc = r->op == TSDB_OP_AUTH_START ? auth_scram_start(a, p, len, out, sizeof out, &ol, peer)
                                         : auth_scram_continue(a, p, len, out, sizeof out, &ol, peer);
        if (rc == 0) {
            if (a->state == AS_IN) {
                metrics_login(METRICS_LOGIN_OK);
                atomic_store(&c->authed, true); /* before the reply: parse() takes every op from now on */
            }
            reply(c, r->id, 0, out, ol, NULL, 0, NULL, 0);
            return;
        }
        if (a->state == AS_FAILED) { /* a failed login closes the connection (A5) */
            metrics_login(METRICS_LOGIN_FAILED);
            atomic_store(&c->auth_end, true);
        }
        break;
    }
    case TSDB_OP_WHOAMI:
    case TSDB_OP_USER_CREATE:
    case TSDB_OP_USER_DROP:
    case TSDB_OP_USER_ALTER:
    case TSDB_OP_USER_PASSWORD:
    case TSDB_OP_GRANT:
    case TSDB_OP_REVOKE: {
        uint8_t *out;
        size_t ol;
        if ((rc = auth_admin(a, r->op, p, len, &out, &ol)) == 0) {
            reply(c, r->id, 0, out, ol, NULL, 0, NULL, 0);
            free(out);
            return;
        }
        break;
    }
    case TSDB_OP_USER_LIST: {
        list_out o = {c, r->id};
        if (len != 0) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        rc = auth_user_list(a, list_emit, &o);
        if (rc == 0 || rc == TSDB_ERR_STOPPED) /* answered, or the client went away */
            return;
        break;
    }
    case TSDB_OP_SERIES: {
        if (len < 3) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        uint16_t n, pn = 0;
        memcpy(&n, p + 1, 2);
        if (len >= 3u + n + 2)
            memcpy(&pn, p + 3 + n, 2);
        if (n == 0 || (len != 3u + n && len != 3u + n + 2 + pn) || memchr(p + 3, 0, n) ||
            (len > 3u + n && memchr(p + 5 + n, 0, pn))) {
            rc = TSDB_ERR_INVALID; /* strndup would cut a name at a NUL and use another series */
            break;
        }
        char *name = strndup((const char *)p + 3, n);
        char *path = len > 3u + n ? strndup((const char *)p + 5 + n, pn) : NULL;
        tsdb_sid id = 0;
        if (!name || (len > 3u + n && !path))
            rc = TSDB_ERR_NOMEM;
        else if (!p[0])
            rc = tsdb_series_lookup(db, name, &id);
        else
            rc = tsdb_series_at(db, name, path && *path ? path : NULL, &id);
        free(name);
        free(path);
        if (rc == 0) {
            reply(c, r->id, 0, &id, 8, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_SERIES_NAME: {
        tsdb_sid id;
        if (len != 8) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&id, p, 8);
        const char *name = tsdb_series_name(db, id);
        if (!name) {
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        uint16_t n = (uint16_t)strlen(name);
        reply(c, r->id, 0, &n, 2, name, n, NULL, 0);
        return;
    }
    case TSDB_OP_INSERT: {
        uint32_t n, zero;
        tsdb_sid s;
        if (len < 16 || (memcpy(&zero, p + 4, 4), zero)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 8, 8);
        bool many = s == TSDB_MANY_SERIES;
        size_t end = 16 + (size_t)n * 12;
        const tsdb_sid *ids = many ? frame_ids(p, len, end, n) : NULL;
        if (many ? !ids : len != end) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        if (many && (rc = auth_ids(&c->auth, ids, n, R_INSERT)) != 0)
            break; /* all or nothing (A16) */
        const int64_t *ts = (const int64_t *)(const void *)(p + 16); /* the payload is malloc'd: 16-aligned */
        const float *v = (const float *)(const void *)(p + 16 + (size_t)n * 8);
        rc = many ? tsdb_insert(db, ids, ts, v, n) : tsdb_insert_series(db, s, ts, v, n);
        break;
    }
    case TSDB_OP_QUERY_RAW: {
        raw_out o = {c, r->id, NULL, NULL, 0, false};
        if (len != 24 && len != 33) {
            raw_flush(&o, true, TSDB_ERR_INVALID); /* raw replies always carry `last` and `n` */
            return;
        }
        tsdb_sid s;
        int64_t start, end;
        tsdb_query_opts q = {TSDB_ASC, 0};
        memcpy(&s, p, 8);
        memcpy(&start, p + 8, 8);
        memcpy(&end, p + 16, 8);
        if (len == 33) {
            uint64_t lim;
            q.order = p[24] ? TSDB_DESC : TSDB_ASC;
            memcpy(&lim, p + 25, 8);
            q.limit = (size_t)lim;
        }
        o.ts = malloc(RAW_CHUNK_ROWS * 8);
        o.v = malloc(RAW_CHUNK_ROWS * 4);
        if (!o.ts || !o.v) {
            free(o.ts);
            free(o.v);
            o.ts = NULL;
            o.v = NULL;
            raw_flush(&o, true, TSDB_ERR_NOMEM);
            return;
        }
        rc = tsdb_query_raw_ex(db, s, start, end, &q, raw_cb, &o);
        if (!o.gone)
            raw_flush(&o, true, rc); /* the rest of the rows, or the error */
        free(o.ts);
        free(o.v);
        return;
    }
    case TSDB_OP_SERIES_CREATE: {
        uint16_t n, pn;
        if (len < 5 || (memcpy(&n, p + 1, 2), len < 5u + n)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&pn, p + 3 + n, 2);
        if (n == 0 || len != 5u + n + pn || memchr(p + 3, 0, n) || memchr(p + 5 + n, 0, pn)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        char *name = strndup((const char *)p + 3, n);
        char *path = pn ? strndup((const char *)p + 5 + n, pn) : NULL;
        tsdb_sid id = 0;
        rc = !name || (pn && !path) ? TSDB_ERR_NOMEM : tsdb_series_create(db, name, (tsdb_type)p[0], path, &id);
        free(name);
        free(path);
        if (rc == 0) {
            reply(c, r->id, 0, &id, 8, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_SERIES_TYPE: {
        tsdb_sid id;
        tsdb_type t;
        if (len != 8) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&id, p, 8);
        if ((rc = tsdb_series_type(db, id, &t)) == 0) {
            uint8_t t8 = (uint8_t)t;
            reply(c, r->id, 0, &t8, 1, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_INSERT_VALUES: {
        uint32_t n;
        uint16_t zero;
        tsdb_sid s;
        /* reserved bytes and flags must be zero, so they can mean something later */
        if (len < TSDB_VALUES_HEADER || p[4] > TSDB_STATE || (p[5] & ~(TSDB_VALID | TSDB_PACKED)) ||
            (memcpy(&zero, p + 6, 2), zero)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 8, 8);
        bool many = s == TSDB_MANY_SERIES;
        if (p[5] & TSDB_PACKED) {
            rc = insert_packed(&c->auth, p, len, n, s);
            break;
        }
        size_t vb = (size_t)n * tsdb_wire_width(p[4]), valid = (p[5] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0;
        size_t end = TSDB_VALUES_HEADER + (size_t)n * 8 + vb + valid;
        const tsdb_sid *ids = many ? frame_ids(p, len, end, n) : NULL;
        if (many ? !ids : len != end) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        if (many && (rc = auth_ids(&c->auth, ids, n, R_INSERT)) != 0)
            break; /* all or nothing (A16) */
        const int64_t *ts = (const int64_t *)(const void *)(p + TSDB_VALUES_HEADER); /* the payload is 8-aligned */
        const uint8_t *vals = p + TSDB_VALUES_HEADER + (size_t)n * 8;
        tsdb_values col = {(tsdb_type)p[4], vals, valid ? vals + vb : NULL};
        rc = many ? tsdb_insert_values(db, ids, ts, &col, n) : tsdb_insert_series_values(db, s, ts, &col, n);
        break;
    }
    case TSDB_OP_QUERY_VALUES:
    case TSDB_OP_QUERY_LTTB: { /* the same chunks: LTTB's points are rows of the series */
        bool lttb = r->op == TSDB_OP_QUERY_LTTB;
        values_out o = {c, r->id, 0, 0, NULL, NULL, 0, false, NULL, NULL};
        qreq qr;
        if (parse_values(p, len, lttb, &qr) != 0) {
            values_flush(&o, true, TSDB_ERR_INVALID);
            return;
        }
        tsdb_sid s = qr.series;
        tsdb_type t = TSDB_FLOAT32;
        tsdb_series_type(db, s, &t); /* an unknown series: no rows */
        o.type = (uint8_t)t;
        o.ts = malloc(RAW_CHUNK_ROWS * 8);
        o.v = calloc((size_t)RAW_CHUNK_ROWS * 8 + RAW_CHUNK_ROWS / 8, 1);
        if (!o.ts || !o.v) {
            free(o.ts);
            free(o.v);
            o.ts = NULL;
            o.v = NULL;
            values_flush(&o, true, TSDB_ERR_NOMEM);
            return;
        }
        if (!lttb)
            rc = tsdb_query_values(db, s, qr.start, qr.end, &qr.q, values_cb, &o);
        else
            rc = tsdb_query_lttb_ex(db, s, qr.start, qr.end, (size_t)qr.threshold, &qr.q, values_cb, &o);
        if (!o.gone)
            values_flush(&o, true, rc); /* the rest of the rows, or the error */
        free(o.ts);
        free(o.v);
        return;
    }
    case TSDB_OP_STATE: {
        tsdb_sid s;
        uint32_t code;
        uint16_t n;
        if (len < 11 || (memcpy(&n, p + 9, 2), len != 11u + n) || n == 0 || memchr(p + 11, 0, n)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 8);
        char *name = strndup((const char *)p + 11, n);
        rc = !name ? TSDB_ERR_NOMEM : p[8] ? tsdb_state(db, s, name, &code) : tsdb_state_lookup(db, s, name, &code);
        free(name);
        if (rc == 0) {
            reply(c, r->id, 0, &code, 4, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_STATE_NAME: {
        tsdb_sid s;
        uint32_t code;
        if (len != 12) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 8);
        memcpy(&code, p + 8, 4);
        const char *name = tsdb_state_name(db, s, code);
        if (!name) {
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        uint16_t n = (uint16_t)strlen(name);
        reply(c, r->id, 0, &n, 2, name, n, NULL, 0);
        return;
    }
    case TSDB_OP_QUERY_BUCKETS: {
        qreq qr;
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if ((rc = parse_buckets(p, len, &qr)) == 0) {
            tsdb_group g = {(tsdb_unit)qr.unit, qr.count, qr.zone};
            if (qr.fields & TSDB_SPREAD) { /* the spread costs a second pass: only when asked for */
                tsdb_spread_bucket *sb = NULL;
                rc = qr.unit == 0 ? tsdb_query_spread(db, qr.series, qr.start, qr.end, qr.interval, &qr.q, &sb, &nb)
                                  : tsdb_query_spread_group(db, qr.series, qr.start, qr.end, &g, &qr.q, &sb, &nb);
                b = (tsdb_bucket *)sb;
            } else if (qr.unit == 0) {
                rc = tsdb_query_agg_ex(db, qr.series, qr.start, qr.end, qr.interval, &qr.q, &b, &nb);
            } else {
                rc = tsdb_query_group_ex(db, qr.series, qr.start, qr.end, &g, &qr.q, &b, &nb);
            }
        }
        if (rc == 0)
            reply_buckets(c, r->id, b, nb, qr.fields);
        qreq_free(&qr);
        free(b);
        if (rc == 0)
            return;
        break;
    }
    case TSDB_OP_LAST_STATS: {
        pthread_mutex_lock(&c->lock);
        tsdb_query_stats st = c->last_stats;
        pthread_mutex_unlock(&c->lock);
        uint64_t v[10] = {st.rows,  st.rows_decoded, st.rows_from_stats, st.memtable_rows,       st.bytes,
                          st.bytes_read, st.parts,  st.granules,        st.granules_from_stats, st.result_rows};
        reply(c, r->id, 0, &st.elapsed_s, 8, v, sizeof v, NULL, 0);
        return;
    }
    case TSDB_OP_QUERY_BOUNDS: {
        qreq qr;
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if ((rc = parse_bounds(p, len, &qr)) == 0) {
            if (qr.fields & TSDB_SPREAD) {
                tsdb_spread_bucket *sb = NULL;
                rc = tsdb_query_spread_bounds(db, qr.series, qr.start, qr.end, qr.bounds, qr.nbounds, &qr.q, &sb,
                                              &nb);
                b = (tsdb_bucket *)sb;
            } else {
                rc = tsdb_query_bounds(db, qr.series, qr.start, qr.end, qr.bounds, qr.nbounds, &qr.q, &b, &nb);
            }
        }
        if (rc == 0)
            reply_buckets(c, r->id, b, nb, qr.fields);
        qreq_free(&qr);
        free(b);
        if (rc == 0)
            return;
        break;
    }
    case TSDB_OP_QUERY_TWA:
        if ((rc = handle_twa(c, r->id, p, len)) == 0)
            return;
        break;
    case TSDB_OP_QUERY_MULTI: handle_multi(c, r); return;
    case TSDB_OP_QUERY_LATEST: handle_latest(c, r); return;
    case TSDB_OP_DELETE_RANGE: {
        tsdb_sid s;
        int64_t start, end;
        if (len != 24) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 8);
        memcpy(&start, p + 8, 8);
        memcpy(&end, p + 16, 8);
        rc = tsdb_delete_range(db, s, start, end);
        break;
    }
    case TSDB_OP_REPLACE_RANGE: {
        uint32_t n;
        uint16_t zero;
        tsdb_sid s;
        int64_t start, end;
        if (len < TSDB_REPLACE_HEADER || p[4] > TSDB_STATE || (p[5] & ~TSDB_VALID) || (memcpy(&zero, p + 6, 2), zero)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 8, 8);
        memcpy(&start, p + 16, 8);
        memcpy(&end, p + 24, 8);
        size_t vb = (size_t)n * tsdb_wire_width(p[4]), valid = (p[5] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0;
        if (len != TSDB_REPLACE_HEADER + (size_t)n * 8 + vb + valid) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        const int64_t *ts = (const int64_t *)(const void *)(p + TSDB_REPLACE_HEADER); /* the payload is 8-aligned */
        const uint8_t *vals = p + TSDB_REPLACE_HEADER + (size_t)n * 8;
        tsdb_values col = {(tsdb_type)p[4], vals, valid ? vals + vb : NULL};
        rc = tsdb_replace_range(db, s, start, end, ts, &col, n);
        break;
    }
    case TSDB_OP_SET_RETENTION: {
        tsdb_sid s;
        int64_t limit;
        if (len != 16) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 8);
        memcpy(&limit, p + 8, 8);
        rc = tsdb_set_retention(db, s, limit);
        break;
    }
    case TSDB_OP_GET_RETENTION: {
        tsdb_sid s;
        int64_t limit;
        if (len != 8) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 8);
        if ((rc = tsdb_get_retention(db, s, &limit)) == 0) {
            reply(c, r->id, 0, &limit, 8, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_COMMIT: rc = tsdb_commit(db); break;
    case TSDB_OP_FLUSH: rc = tsdb_flush(db); break;
    case TSDB_OP_OPTIMIZE: rc = tsdb_optimize(db); break;
    case TSDB_OP_STATS: {
        tsdb_stats st;
        tsdb_get_stats(db, &st);
        uint64_t v[8] = {st.parts, st.partitions, st.rows, st.bytes, st.merges, st.merged_rows,
                         st.passthrough_granules, st.memtable_rows};
        reply(c, r->id, 0, v, sizeof v, NULL, 0, NULL, 0);
        return;
    }
    case TSDB_OP_TOPOLOGY: handle_topology(r); return;
    case TSDB_OP_STATUS: handle_status(r); return;
    default: rc = TSDB_ERR_INVALID; break;
    }
    if (rc == 0 && (r->op == TSDB_OP_INSERT || r->op == TSDB_OP_INSERT_VALUES)) {
        uint32_t n;
        memcpy(&n, p, 4); /* the length was checked: it has the row count */
        metrics_rows_inserted(n);
    }
    reply_status(c, r->id, rc);
}

static void *worker_main(void *arg)
{
    bool extra = arg != NULL; /* started while every worker waited: leaves when idle and another is free */
    for (;;) {
        pthread_mutex_lock(&work_lock);
        while (!work_head && !work_stop && (!extra || running_workers - waiting_workers <= 1))
            pthread_cond_wait(&work_cv, &work_lock);
        if (!work_head) {
            if (--running_workers == 0)
                pthread_cond_broadcast(&workers_gone_cv);
            pthread_mutex_unlock(&work_lock);
            return NULL;
        }
        req *r = work_head;
        work_head = r->next;
        if (!work_head)
            work_tail = NULL;
        pthread_mutex_unlock(&work_lock);
        int64_t began = metrics_request_begin();
        handle(r); /* even for a closed connection: the client sent it, and replies are dropped */
        metrics_request_end(r->op, began);
        if (r->op == TSDB_OP_QUERY_RAW || r->op == TSDB_OP_QUERY_VALUES || r->op == TSDB_OP_QUERY_BUCKETS ||
            r->op == TSDB_OP_QUERY_BOUNDS || r->op == TSDB_OP_QUERY_LTTB || r->op == TSDB_OP_QUERY_TWA ||
            r->op == TSDB_OP_QUERY_MULTI) {
            tsdb_query_stats st;
            tsdb_last_query_stats(&st); /* this worker ran it */
            pthread_mutex_lock(&r->c->lock);
            r->c->last_stats = st;
            pthread_mutex_unlock(&r->c->lock);
        }
        conn *c = r->c;
        req_free(r);
        notify(c, true);
    }
}

/* ---------------------------------------------------------- event loop */

/*
 * Two backends move the bytes; everything else (framing, dispatch, replies,
 * closing) is shared. With io_uring a recv or send is submitted to the ring and
 * completes later. With epoll, for kernels or policies without io_uring (RHEL
 * and AlmaLinux 10 ship it disabled: kernel.io_uring_disabled=2), the same
 * operation is wanted until the socket is ready and then done with a
 * non-blocking call. Both end in on_recv and on_send, and recv_inflight and
 * send_inflight mean "a recv / send is pending" in both, so the order of
 * replies, back-pressure and closing cannot differ between them.
 *
 * epoll is edge-triggered: a socket is added once, with EPOLLIN, EPOLLOUT and
 * EPOLLRDHUP, and never modified, so a connection changing between reading
 * and writing costs no epoll_ctl, and a client that hangs up while its request
 * runs is reported once rather than on every wait (level-triggered, EPOLLHUP
 * cannot be masked). Events only mark a connection readable or writable. A
 * recv or send that returns EAGAIN clears the mark; so does a short read, since
 * data arriving later raises a new edge, unless the peer has already shut
 * down (EPOLLRDHUP: no edge will announce the end of stream). Connections with
 * a wanted operation on a ready socket wait in the ready list, worked through
 * in rounds of at most one recv and one send each, like one batch of
 * completions: a client that sends without pause cannot hold the loop.
 * Events carry the descriptor, not the connection: an event for a connection
 * freed earlier in the same batch finds nothing in fd_conn, or the connection
 * that has the number since, for which it is a spurious wakeup.
 */
typedef enum { IO_AUTO, IO_URING, IO_EPOLL } io_kind;
static io_kind io = IO_AUTO;
static int epfd = -1;
static conn **fd_conn; /* epoll: connections by descriptor */
static size_t fd_conn_cap;
static conn *ready_head, *ready_tail; /* epoll: connections that can make progress */
static size_t nready;
static conn *park_head, *park_tail; /* not read from until request memory frees */
static int tick_fd = -1;           /* a timerfd: once a second, the frame deadlines */
#ifdef __APPLE__ /* no timerfd: a kqueue holding a one second timer, which the main kqueue can watch for reading */
#define TICK_WHAT "kqueue timer"
static int tick_open(void)
{
    int kq = kqueue();
    struct kevent t;
    EV_SET(&t, 1, EVFILT_TIMER, EV_ADD | EV_ENABLE, 0, 1000 /* ms */, NULL);
    if (kq < 0 || kevent(kq, &t, 1, NULL, 0, NULL) < 0) {
        if (kq >= 0)
            close(kq);
        return -1;
    }
    fcntl(kq, F_SETFD, FD_CLOEXEC);
    return kq;
}
static void tick_drain(void)
{
    struct kevent ev[4];
    struct timespec zero = {0, 0};
    kevent(tick_fd, NULL, 0, ev, 4, &zero);
}
#else
#define TICK_WHAT "timerfd"
static int tick_open(void)
{
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    struct itimerspec second = {{1, 0}, {1, 0}};
    if (fd < 0 || timerfd_settime(fd, 0, &second, NULL) != 0) {
        if (fd >= 0)
            close(fd);
        return -1;
    }
    return fd;
}
static void tick_drain(void)
{
    uint64_t v;
    if (read(tick_fd, &v, 8) < 0 && errno != EAGAIN)
        perror("tsdbd: timerfd");
}
#endif

static int64_t mono_s(void)
{
    struct timespec t;
#ifdef CLOCK_MONOTONIC_COARSE /* whole seconds only: the coarse clock is enough, and cheaper */
    clock_gettime(CLOCK_MONOTONIC_COARSE, &t);
#else
    clock_gettime(CLOCK_MONOTONIC, &t);
#endif
    return (int64_t)t.tv_sec;
}

/* The send timeout counts in ms: whole seconds would close a connection anywhere from T to T + 2 s after its
 * output stopped (a send started late in one second, the tick early in another); this way T to T + 1 s. */
static int64_t mono_ms(void)
{
    struct timespec t;
#ifdef CLOCK_MONOTONIC_COARSE
    clock_gettime(CLOCK_MONOTONIC_COARSE, &t);
#else
    clock_gettime(CLOCK_MONOTONIC, &t);
#endif
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

/* The socket's unsent and unacknowledged bytes: SIOCOUTQ on Linux, SO_NWRITE on macOS. */
static int sock_outq(int fd, int *q)
{
#ifdef __APPLE__
    socklen_t len = sizeof *q;
    return getsockopt(fd, SOL_SOCKET, SO_NWRITE, q, &len);
#else
    return ioctl(fd, SIOCOUTQ, q);
#endif
}

/* The send-progress window of one connection (on_tick; tests/test_out_send.c): `acked` is what the client has consumed
 * so far (bytes handed to the socket minus SIOCOUTQ). The first reading is the baseline; a window that has seen
 * timeout_s x SEND_MIN_RATE bytes starts again; and a window that is timeout_s old without that has expired. The
 * count of bytes, not any change of the queue, is progress: a client taking a few bytes whenever its small window
 * opens changes the queue all the time and moves nothing worth a worker. */
static void send_window_note(int64_t *t0, int64_t *base, int64_t acked, int64_t now_ms, int timeout_s)
{
    if (*base < 0)
        *base = acked;
    else if (acked - *base >= (int64_t)timeout_s * SEND_MIN_RATE) {
        *t0 = now_ms;
        *base = acked;
    }
}

static bool send_window_expired(int64_t t0, int64_t now_ms, int timeout_s)
{
    return now_ms - t0 >= timeout_s * 1000LL;
}

/* What c's receive buffer counts in req_held: all of it once grown past IN_KEEP. */
static void in_account(conn *c)
{
    size_t now = c->in.cap > IN_KEEP ? c->in.cap : 0;
    if (now > c->in_charged)
        atomic_fetch_add(&req_held, now - c->in_charged);
    else if (now < c->in_charged)
        atomic_fetch_sub(&req_held, c->in_charged - now);
    c->in_charged = now;
}

/* Gives back a receive buffer grown for a large frame, unless one is being received into it. No recv may be in
 * flight. */
static void in_shrink(conn *c)
{
    uint32_t flen;
    if (c->in.cap <= IN_KEEP || c->in.len > IN_KEEP / 2 ||
        (c->in.len >= 4 && (memcpy(&flen, c->in.p, 4), 4 + (size_t)flen > IN_KEEP)))
        return;
    uint8_t *p = realloc(c->in.p, IN_KEEP);
    if (p) {
        c->in.p = p;
        c->in.cap = IN_KEEP;
    }
    in_account(c);
}

static void park(conn *c, bool for_frame)
{
    c->parked = true;
    c->held = true;
    c->park_frame = for_frame;
    c->park_next = NULL;
    c->park_prev = park_tail;
    if (park_tail)
        park_tail->park_next = c;
    else
        park_head = c;
    park_tail = c;
}

static void unpark(conn *c)
{
    if (!c->parked)
        return;
    c->parked = false;
    if (c->park_prev)
        c->park_prev->park_next = c->park_next;
    else
        park_head = c->park_next;
    if (c->park_next)
        c->park_next->park_prev = c->park_prev;
    else
        park_tail = c->park_prev;
    c->park_prev = c->park_next = NULL;
}

#ifndef TSDBD_NO_URING
static struct io_uring_sqe *sqe_get(void)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        io_uring_submit(&ring);
        sqe = io_uring_get_sqe(&ring);
    }
    return sqe;
}
#endif

/* epoll: queues c for the next round if a wanted operation can proceed. */
static void ep_want(conn *c)
{
    if (c->in_ready || !((c->recv_inflight && c->readable) || (c->send_inflight && c->writable)))
        return;
    c->in_ready = true;
    c->ready_next = NULL;
    if (ready_tail)
        ready_tail->ready_next = c;
    else
        ready_head = c;
    ready_tail = c;
    nready++;
}

static size_t pre_queued(const conn *c);

static void submit_recv(conn *c)
{
    if (c->recv_inflight || c->closed || c->eof || c->parked)
        return;
    if (c->q_bytes > PIPE_CAP || c->q_multi >= MULTI_QUEUED ||
        (!atomic_load(&c->authed) && (c->auth_hold || pre_queued(c) >= 2))) {
        c->held = true;
        return; /* on_wake resumes it as its requests run */
    }
    if (c->q_head && atomic_load_explicit(&req_held, memory_order_relaxed) > req_budget) {
        park(c, false); /* its requests waiting are memory enough: read again once memory frees */
        return;
    }
    in_shrink(c);
    size_t want = RECV_MIN_FREE;
    uint32_t flen;
    if (c->in.len >= 4 && (memcpy(&flen, c->in.p, 4), 4 + (size_t)flen > c->in.len)) {
        size_t rest = 4 + (size_t)flen - c->in.len;
        /* a large frame: the rest, into the buffer reserved for it; a small one: room for at least its rest,
         * which parse reserved, so the buffer never grows past IN_KEEP uncounted */
        want = 4 + (size_t)flen > IN_KEEP || rest < want ? rest : want;
    }
    if (buf_reserve(&c->in, want) != 0)
        return;
    in_account(c);
    if (c->held) { /* the wait was the server's: the client gets the whole deadline again */
        c->held = false;
        if (c->frame_t0)
            c->frame_t0 = mono_s();
    }
    c->recv_inflight = true;
    if (io == IO_EPOLL) {
        ep_want(c);
        return;
    }
#ifndef TSDBD_NO_URING
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_recv(sqe, c->fd, c->in.p + c->in.len, c->in.cap - c->in.len, 0);
    io_uring_sqe_set_data(sqe, &c->recv_ev);
#endif
}

static void submit_send(conn *c)
{
    if (c->send_inflight || c->closed)
        return;
    if (c->sent == c->sending.len) {
        /* take whatever the worker produced since the last send */
        pthread_mutex_lock(&c->lock);
        if (c->tx_worker) {
            /* the worker is sending `out` itself; it tells us of whatever it leaves */
            c->notified = false;
            c->tx_loop = false;
            pthread_mutex_unlock(&c->lock);
            c->sent = c->sending.len = 0;
            return;
        }
        buf t = c->sending;
        c->sending = c->out;
        c->out = t;
        c->out.len = 0;
        c->notified = false;
        c->tx_loop = c->sending.len > 0;
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->lock);
        c->sent = 0;
        if (c->sending.len == 0) {
            buf_shrink(&c->sending);
            return;
        }
    }
    c->send_inflight = true;
    if (!c->tx_win) { /* the window runs across sends: a send that took a few bytes is not progress by itself */
        c->tx_win = true;
        c->tx_t0 = mono_ms();
        c->tx_base = -1; /* the first tick's reading is the baseline, not progress */
    }
    if (io == IO_EPOLL) {
        ep_want(c);
        return;
    }
#ifndef TSDBD_NO_URING
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_send(sqe, c->fd, c->sending.p + c->sent, c->sending.len - c->sent, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, &c->send_ev);
#endif
}

static void dispatch(conn *c)
{
    if (c->busy || !c->q_head)
        return;
    req *r = c->q_head;
    c->q_head = r->next;
    if (!c->q_head)
        c->q_tail = NULL;
    r->next = NULL;
    c->q_bytes -= REQ_COST(r);
    if (cancelable(r->op)) { /* the queue's order, so it is the first of them */
        c->mq_head = r->mnext;
        if (!c->mq_head)
            c->mq_tail = NULL;
        r->mnext = NULL;
        c->q_multi--;
    }
    c->busy = true;
    c->run_id = r->id;
    c->run_op = r->op;
    atomic_store(&c->cancel_running, r->cancelled);
    pthread_mutex_lock(&work_lock);
    if (work_tail)
        work_tail->next = r;
    else
        work_head = r;
    work_tail = r;
    pthread_mutex_unlock(&work_lock);
    /* after the unlock: a worker woken while the lock is held blocks on it at once, and the unlock must wake it a
     * second time (with many short requests a quarter of the event loop's time) */
    pthread_cond_signal(&work_cv);
}

/* Closes the descriptor and frees c. A connection still in the epoll ready
 * list is only marked dead: ep_round frees it when it comes out. */
static void conn_free(conn *c)
{
    nconns--;
    metrics_conn_closed();
    if (c->prev)
        c->prev->next = c->next;
    else
        conns = c->next;
    if (c->next)
        c->next->prev = c->prev;
    while (c->q_head) {
        req *r = c->q_head;
        c->q_head = r->next;
        req_free(r);
    }
    c->q_tail = c->mq_head = c->mq_tail = NULL;
    c->q_multi = 0;
    unpark(c);
    free(c->in.p);
    c->in = (buf){0};
    in_account(c);
    if (io == IO_EPOLL && (size_t)c->fd < fd_conn_cap)
        fd_conn[c->fd] = NULL; /* before close: the number may come back for the next connection */
    close(c->fd);
    free(c->out.p);
    free(c->sending.p);
    auth_conn_free(&c->auth); /* no lock: the event loop never waits for auth_mu */
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    if (c->in_ready)
        c->dead = true;
    else
        free(c);
}

/* Starts closing; the memory goes once nothing refers to it any more. Requests
 * already received still run, one by one, with their replies dropped: a client
 * that pipelines inserts and closes without reading the acknowledgements should
 * not lose them depending on how far the server had got. */
static void conn_close(conn *c)
{
    if (!c->closed) {
        c->closed = true;
        pthread_mutex_lock(&c->lock);
        atomic_store(&c->closing, true);
        pthread_cond_broadcast(&c->cv); /* a worker waiting for output space gives up */
        pthread_mutex_unlock(&c->lock);
        shutdown(c->fd, SHUT_RDWR); /* completes the outstanding recv (io_uring) */
        if (io == IO_EPOLL)
            c->recv_inflight = c->send_inflight = false; /* only wanted: nothing in the kernel to complete */
    }
    dispatch(c);
    if (!c->busy && !c->q_head && !c->recv_inflight && !c->send_inflight)
        conn_free(c);
}

/* After the client's end of stream: close once every request has been answered. */
static void finish_if_drained(conn *c)
{
    if (c->eof && !c->closed && !c->busy && !c->q_head && !c->send_inflight)
        conn_close(c);
}

/* Splits received bytes into requests. Returns 0, -1 on a protocol error, or 1 when the frame begun needs a
 * buffer that the request memory cannot have yet (the connection is then parked, unread). */
/* CANCEL: the QUERY_MULTI with that id ends after the series in flight, whether a worker runs it or it waits in
 * the connection's queue. Main thread. */
static void cancel_multi(conn *c, uint32_t id)
{
    for (req *r = c->mq_head; r; r = r->mnext)
        if (r->id == id)
            r->cancelled = true;
    if (c->busy && cancelable(c->run_op) && c->run_id == id)
        atomic_store(&c->cancel_running, true);
}

/* Requests waiting on a connection that has not logged in: at most two (A5), so a short walk. */
static size_t pre_queued(const conn *c)
{
    size_t n = 0;
    for (const req *r = c->q_head; r && n < 2; r = r->next)
        n++;
    return n;
}

static int parse(conn *c)
{
    size_t off = 0;
    int rc = 0;
    bool consumed = false;
    while (c->in.len - off >= 4) {
        uint32_t flen;
        memcpy(&flen, c->in.p + off, 4);
        if (flen < 5 || flen > TSDB_MAX_FRAME)
            return -1;
        bool pre = !atomic_load(&c->authed); /* before login (A5) */
        if (pre && 4 + (size_t)flen > TSDB_PRELOGIN_MAX) {
            metrics_login(METRICS_LOGIN_REFUSED); /* closed before any of it is buffered */
            return -1;
        }
        if (pre && (c->auth_hold || pre_queued(c) >= 2))
            break; /* read on once the login has moved on (on_wake) */
        size_t need = 4 + (size_t)flen;
        if (c->in.len - off < need) {
            /* make room for the whole frame up front: exactly, for a large one, which request memory counts */
            if (off) {
                memmove(c->in.p, c->in.p + off, c->in.len - off);
                c->in.len -= off;
                off = 0;
            }
            if (need > c->in.cap) {
                if (need > IN_KEEP) {
                    size_t held = atomic_load_explicit(&req_held, memory_order_relaxed) - c->in_charged;
                    if (held && held + need > req_budget) {
                        rc = 1;
                        break;
                    }
                    uint8_t *p = realloc(c->in.p, need);
                    if (!p)
                        return -1;
                    c->in.p = p;
                    c->in.cap = need;
                } else if (buf_reserve(&c->in, need - c->in.len) != 0) {
                    return -1;
                }
                in_account(c);
            }
            break;
        }
        if (pre && c->in.p[off + 8] == TSDB_OP_CANCEL) {
            metrics_login(METRICS_LOGIN_REFUSED); /* nothing to cancel before login: a client that does not speak it */
            return -1;
        }
        if (c->in.p[off + 8] == TSDB_OP_CANCEL) { /* no reply, and it must not wait behind the query it stops */
            if (flen == 9) {
                uint32_t target;
                memcpy(&target, c->in.p + off + 9, 4);
                cancel_multi(c, target);
            } else {
                metrics_error(TSDB_ERR_INVALID); /* no reply to send it in, but a client's bug shows */
            }
            metrics_request_end(TSDB_OP_CANCEL, metrics_request_begin());
            off += need;
            consumed = true;
            continue;
        }
        req *r = calloc(1, sizeof *r);
        if (!r)
            return -1;
        r->c = c;
        memcpy(&r->id, c->in.p + off + 4, 4);
        r->op = c->in.p[off + 8];
        r->len = flen - 5;
        r->payload = malloc(r->len ? r->len : 1);
        if (!r->payload) {
            free(r);
            return -1;
        }
        memcpy(r->payload, c->in.p + off + 9, r->len);
        atomic_fetch_add_explicit(&req_held, REQ_COST(r), memory_order_relaxed);
        c->q_bytes += REQ_COST(r);
        if (c->q_tail)
            c->q_tail->next = r;
        else
            c->q_head = r;
        c->q_tail = r;
        if (cancelable(r->op)) {
            if (c->mq_tail)
                c->mq_tail->mnext = r;
            else
                c->mq_head = r;
            c->mq_tail = r;
            c->q_multi++;
        }
        off += need;
        consumed = true;
        if ((r->op == TSDB_OP_HELLO && hello_refused(r->payload, r->len)) ||
            (pre && r->op != TSDB_OP_HELLO && r->op != TSDB_OP_AUTH_START && r->op != TSDB_OP_AUTH_CONTINUE)) {
            /* an older client: nothing it sends after this can be read right. It hears INVALID, then the
             * connection closes as at its end of stream (finish_if_drained). An op before login likewise hears
             * TSDB_ERR_AUTH (the worker's auth_check), and nothing after it is read. */
            c->eof = true;
            off = c->in.len;
            break;
        }
        if (pre && r->op == TSDB_OP_AUTH_CONTINUE) {
            c->auth_hold = true; /* what follows is read as the login's outcome allows */
            break;
        }
    }
    if (off) {
        memmove(c->in.p, c->in.p + off, c->in.len - off);
        c->in.len -= off;
    }
    /* the deadline runs from the first byte of each frame */
    c->frame_t0 = c->in.len == 0 ? 0 : consumed || !c->frame_t0 ? mono_s() : c->frame_t0;
    return rc;
}

/* Before login, frames received past the two queued ones (A5) wait in c->in, and no more bytes may come to have
 * them parsed: they are parsed as a dispatch opens the window, so a third pipelined frame is answered (one past
 * the window: AUTH, then the close) instead of waiting for --login-timeout. Never while a recv may still write into
 * c->in. parse's result. */
static int pre_reparse(conn *c)
{
    if (atomic_load(&c->authed) || c->auth_hold || c->eof || c->closed || c->recv_inflight || c->in.len < 4 ||
        pre_queued(c) >= 2)
        return 0;
    int pr = parse(c);
    if (pr > 0)
        park(c, true);
    if (pr >= 0)
        dispatch(c);
    return pr;
}

/* A recv finished: res bytes arrived (0: end of stream), or -errno. */
static void on_recv(conn *c, ssize_t res)
{
    c->recv_inflight = false;
    if (res == 0 && !c->closed) { /* end of stream: what was received still gets its replies */
        if (getenv("TSDBD_DEBUG"))
            fprintf(stderr, "tsdbd: connection %d: recv end\n", c->fd);
        c->eof = true;
        finish_if_drained(c);
        return;
    }
    if (res < 0 || c->closed) {
        if (getenv("TSDBD_DEBUG") && !c->closed)
            fprintf(stderr, "tsdbd: connection %d: recv %s\n", c->fd, strerror((int)-res));
        conn_close(c);
        return;
    }
    c->in.len += (size_t)res;
    int pr = parse(c);
    if (pr < 0) {
        if (getenv("TSDBD_DEBUG"))
            fprintf(stderr, "tsdbd: connection %d: protocol error\n", c->fd);
        conn_close(c);
        return;
    }
    if (pr > 0)
        park(c, true);
    in_shrink(c); /* a large frame's buffer goes once the frame is parsed */
    dispatch(c);
    if (pre_reparse(c) < 0) {
        conn_close(c);
        return;
    }
    submit_recv(c);
}

/* Reads again from parked connections while request memory allows: each at most once a call (one that still
 * cannot have its frame's buffer goes back to the end). */
static void unpark_some(void)
{
    conn *last = park_tail;
    while (park_head && atomic_load_explicit(&req_held, memory_order_relaxed) < req_budget) {
        conn *c = park_head;
        bool frame = c->park_frame, end = c == last;
        unpark(c);
        int pr = frame ? parse(c) : 0;
        if (pr < 0) {
            conn_close(c);
        } else {
            if (pr > 0)
                park(c, true);
            dispatch(c);
            submit_recv(c);
        }
        if (end)
            break;
    }
}

/* Once a second: closes connections whose frame was started frame_timeout_s ago and is still incomplete while
 * the server waits for its bytes (not one parked or held back by its own pipeline), and those whose output has
 * not moved for send_timeout_s. Run after the events that came with it (ep_loop, uring_loop). */
static void on_tick(void)
{
    tick_drain();
    int64_t now = mono_s(), now_ms = mono_ms();
    static int64_t logged;
    static int64_t login_logged;
    static int64_t send_logged;
    /* Ticks come every second. A longer gap means the server itself did not run (SIGSTOP, a frozen cgroup such as
     * `podman pause`, a stalled host): that time is not the client's, so every deadline moves on by it, as time
     * the server held a connection unread does not count against its frame. Otherwise the first tick after a
     * stall longer than a timeout would close every connection in mid-reply or mid-frame, before the loop has
     * even seen what their clients did meanwhile. */
    static int64_t last_tick;
    int64_t stall = last_tick && now - last_tick > 2 ? now - last_tick - 1 : 0;
    last_tick = now;
    if (start_ppid && getppid() != start_ppid && !stopping) { /* reparented: the parent is gone */
        fprintf(stderr, "tsdbd: --exit-with-parent: parent %d exited, stopping\n", (int)start_ppid);
        stopping = 1; /* as SIGTERM: the loop ends, requests in progress finish, the memtable is flushed */
        wake_signal();
    }
    for (conn *c = conns, *next; c; c = next) {
        next = c->next;
        if (stall) {
            c->t_open += stall;
            c->tx_t0 += stall * 1000;
            if (c->frame_t0)
                c->frame_t0 += stall;
        }
        if (!c->closed && !atomic_load(&c->authed) && now - c->t_open >= login_timeout_s) {
            if (login_logged != now) {
                login_logged = now;
                fprintf(stderr, "tsdbd: connection closed: not logged in after %d s\n", login_timeout_s);
            }
            metrics_login(METRICS_LOGIN_TIMEOUT);
            conn_close(c);
            continue;
        }
        if (c->frame_t0 && c->recv_inflight && !c->closed && now - c->frame_t0 > frame_timeout_s) {
            if (logged != now) {
                logged = now;
                fprintf(stderr, "tsdbd: connection closed: a frame incomplete after %d s\n", frame_timeout_s);
            }
            conn_close(c);
            continue;
        }
        /* Output that is not being read: the one place that decides, for a worker waiting for output space
         * (out_wait, which this wakes through conn_close) and for a reply that fit whole in the two buffers alike
         * (which would otherwise hold up to 2 x (OUT_CAP + OUT_PIECE) for as long as the client keeps the socket
         * open). A send completes (io_uring) or the socket turns writable (epoll) only once about a third of its
         * send buffer is free, over a megabyte when the kernel has grown it, so a client reading slowly but
         * steadily moves no send for long: what the client has consumed is asked of the socket instead (bytes
         * handed to it minus SIOCOUTQ, its unsent and unacknowledged bytes). A window of --send-timeout seconds
         * must see at least SEND_MIN_RATE bytes a second consumed, else the connection is closed; a window that
         * has seen that starts again. Any change of the queue used to count, so a client reading a few bytes a
         * minute held a worker (and up to 18 MB of buffers) as long as it liked (review m1). */
        if (c->send_inflight && !c->closed) {
            int q;
            if (sock_outq(c->fd, &q) == 0) {
                int64_t acked = atomic_load_explicit(&c->tx_total, memory_order_relaxed) - q;
                send_window_note(&c->tx_t0, &c->tx_base, acked, now_ms, send_timeout_s);
            }
        } else
            c->tx_win = false;
        if (c->send_inflight && !c->closed && send_window_expired(c->tx_t0, now_ms, send_timeout_s)) {
            if (send_logged != now) {
                send_logged = now;
                fprintf(stderr, "tsdbd: connection closed: its output did not move for %d s\n", send_timeout_s);
            }
            conn_close(c);
        }
    }
    unpark_some();
}

/* A send finished: res bytes went out, or -errno. */
static void on_send(conn *c, ssize_t res)
{
    c->send_inflight = false;
    if (res < 0 || c->closed) {
        if (getenv("TSDBD_DEBUG") && res < 0)
            fprintf(stderr, "tsdbd: connection %d: send %s\n", c->fd, strerror((int)-res));
        conn_close(c);
        return;
    }
    c->sent += (size_t)res;
    atomic_fetch_add_explicit(&c->tx_total, (int64_t)res, memory_order_relaxed);
    submit_send(c);
    finish_if_drained(c);
}

/* A new connection closed for want of memory or of epoll watches: the client
 * only sees it closed, so say why here, at most once a second (a shortage hits
 * every connection of a burst). Event loop thread only. */
static void drop_new_conn(int fd, const char *what, int err)
{
    static time_t last;
    time_t t = time(NULL);
    if (t != last) {
        last = t;
        fprintf(stderr, "tsdbd: connection refused: %s: %s\n", what, strerror(err));
    }
    close(fd);
}

static void on_accept(int fd)
{
    if (nconns >= max_conns) {
        close(fd); /* over the limit: the client sees the connection closed */
        metrics_conn_rejected();
        return;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    conn *c = calloc(1, sizeof *c);
    if (!c) {
        drop_new_conn(fd, "connection", ENOMEM);
        return;
    }
    atomic_store(&c->proto, TSDB_PROTOCOL_MIN);
    if (io == IO_EPOLL) {
        if ((size_t)fd >= fd_conn_cap) {
            size_t cap = fd_conn_cap ? fd_conn_cap : 1024;
            while (cap <= (size_t)fd)
                cap *= 2;
            conn **t = realloc(fd_conn, cap * sizeof *t);
            if (!t) {
                free(c);
                drop_new_conn(fd, "descriptor table", ENOMEM);
                return;
            }
            memset(t + fd_conn_cap, 0, (cap - fd_conn_cap) * sizeof *t);
            fd_conn = t;
            fd_conn_cap = cap;
        }
        struct epoll_event e = {.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET, .data.fd = fd};
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &e) != 0) {
            int err = errno;
            free(c);
            drop_new_conn(fd, "epoll_ctl", err); /* ENOSPC: fs.epoll.max_user_watches */
            return;
        }
        fd_conn[fd] = c;
        c->readable = c->writable = true; /* until a call says otherwise */
    }
    c->fd = fd;
    c->n_out = (note){NULL, c, false};
    c->n_done = (note){NULL, c, true};
    c->recv_ev = (ev){EV_RECV, c};
    c->send_ev = (ev){EV_SEND, c};
    pthread_mutex_init(&c->lock, NULL);
    conn_cv_init(&c->cv);
    auth_conn_init(&c->auth);
    atomic_store(&c->authed, c->auth.state == AS_IN); /* --no-auth: logged in from the start */
    c->t_open = mono_s();
    c->next = conns;
    if (conns)
        conns->prev = c;
    conns = c;
    nconns++;
    metrics_conn_opened();
    submit_recv(c);
}

static void take_notes(void)
{
    note *top = atomic_exchange_explicit(&note_top, NULL, memory_order_acquire), *batch = NULL;
    while (top) { /* into the order of the pushes */
        note *next = top->next;
        top->next = batch;
        batch = top;
        top = next;
    }
    pthread_mutex_lock(&note_lock);
    int *fds = new_fds;
    size_t nfds = nnew;
    new_fds = NULL;
    nnew = new_cap = 0;
    pthread_mutex_unlock(&note_lock);
    for (size_t i = 0; i < nfds; i++)
        on_accept(fds[i]);
    free(fds);
    for (note *nt = batch, *next; nt; nt = next) {
        next = nt->next; /* nt may be pushed again once handled */
        conn *c = nt->c;
        bool done = nt->done;
        if (done)
            c->busy = false;
        else
            atomic_store(&c->out_queued, false);
        if (c->closed) {
            conn_close(c);
            continue;
        }
        if (done && atomic_load(&c->auth_end))
            c->eof = true; /* a failed login or a dropped user: answer what was received, then close */
        if (done && c->auth_hold && c->run_op == TSDB_OP_AUTH_CONTINUE) {
            c->auth_hold = false; /* the login's outcome is set: what the client sent behind it is read now */
            int pr = c->eof ? 0 : parse(c);
            if (pr < 0) {
                conn_close(c);
                continue;
            }
            if (pr > 0)
                park(c, true);
        }
        submit_send(c);
        if (done) {
            dispatch(c);
            if (pre_reparse(c) < 0) {
                conn_close(c);
                continue;
            }
            submit_recv(c); /* its pipeline may have room again */
        }
        finish_if_drained(c);
    }
    if (park_head)
        unpark_some();
}

static void on_wake(void)
{
    wake_drain();
    take_notes();
}

/* Only the push onto an empty stack writes the eventfd (notify), so a worker preempted between its push and that
 * write would hold up every note pushed after it. The main thread therefore also takes the notes after each round
 * of events when the stack is not empty (one load per round), so the notes that came during a round wait for no
 * such write. The eventfd is left alone here: a count it holds only makes a later on_wake find less to do. */
static void notes_pending(void)
{
    if (atomic_load_explicit(&note_top, memory_order_relaxed))
        take_notes();
}

#ifndef TSDBD_NO_URING
static void uring_loop(void)
{
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_poll_multishot(sqe, wake_fd, POLLIN);
    io_uring_sqe_set_data(sqe, &wake_ev);
    sqe = sqe_get();
    io_uring_prep_poll_multishot(sqe, tick_fd, POLLIN);
    io_uring_sqe_set_data(sqe, &tick_ev);
    while (!stopping) {
        io_uring_submit_and_wait(&ring, 1);
        struct io_uring_cqe *cqe;
        unsigned head, seen = 0;
        bool tick = false;
        io_uring_for_each_cqe(&ring, head, cqe)
        {
            seen++;
            ev *e = io_uring_cqe_get_data(cqe);
            switch (e->type) {
            case EV_WAKE:
                on_wake();
                if (!(cqe->flags & IORING_CQE_F_MORE)) {
                    struct io_uring_sqe *s2 = sqe_get();
                    io_uring_prep_poll_multishot(s2, wake_fd, POLLIN);
                    io_uring_sqe_set_data(s2, &wake_ev);
                }
                break;
            case EV_TICK:
                tick = true;
                if (!(cqe->flags & IORING_CQE_F_MORE)) {
                    struct io_uring_sqe *s2 = sqe_get();
                    io_uring_prep_poll_multishot(s2, tick_fd, POLLIN);
                    io_uring_sqe_set_data(s2, &tick_ev);
                }
                break;
            case EV_RECV: on_recv(e->c, cqe->res); break;
            case EV_SEND: on_send(e->c, cqe->res); break;
            }
        }
        io_uring_cq_advance(&ring, seen);
        if (tick) /* after the batch, as in ep_loop: sends and recvs completed with it count */
            on_tick();
        notes_pending();
    }
}
#endif

/* epoll: one round over the connections that were ready when it started.
 * in_ready stays set while one is handled, so it is not queued twice and a
 * close leaves the freeing to this loop. */
static void ep_round(void)
{
    for (size_t n = nready; n > 0 && ready_head; n--) {
        conn *c = ready_head;
        ready_head = c->ready_next;
        if (!ready_head)
            ready_tail = NULL;
        nready--;
        if (!c->dead && c->recv_inflight && c->readable) {
            size_t room = c->in.cap - c->in.len;
            ssize_t r = recv(c->fd, c->in.p + c->in.len, room, MSG_DONTWAIT);
            int err = errno;
            if (r < 0 && (err == EAGAIN || err == EWOULDBLOCK))
                c->readable = false;
            else if (r >= 0 || err != EINTR) {
                if (r == 0 || (r > 0 && (size_t)r < room && !c->rdhup))
                    c->readable = false;
                on_recv(c, r < 0 ? -err : r);
            }
        }
        if (!c->dead && c->send_inflight && c->writable) {
            ssize_t r = send(c->fd, c->sending.p + c->sent, c->sending.len - c->sent, MSG_NOSIGNAL | MSG_DONTWAIT);
            int err = errno;
            if (r < 0 && (err == EAGAIN || err == EWOULDBLOCK))
                c->writable = false;
            else if (r >= 0 || err != EINTR)
                on_send(c, r < 0 ? -err : r);
        }
        c->in_ready = false;
        if (c->dead)
            free(c);
        else
            ep_want(c);
    }
}

static void ep_loop(void)
{
    struct epoll_event evs[256];
    while (!stopping) {
        int n = epoll_wait(epfd, evs, 256, ready_head ? 0 : -1);
        bool tick = false;
        if (n < 0) {
            if (errno != EINTR) {
                perror("tsdbd: epoll_wait");
                stopping = 1;
            }
            continue;
        }
        for (int i = 0; i < n; i++) {
            int fd = evs[i].data.fd;
            if (fd == wake_fd) {
                on_wake();
                continue;
            }
            if (fd == tick_fd) {
                tick = true;
                continue;
            }
            conn *c = (size_t)fd < fd_conn_cap ? fd_conn[fd] : NULL;
            if (!c)
                continue;
            uint32_t m = evs[i].events;
            if (m & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
                c->rdhup = true; /* recv no longer blocks, and no further edge may come */
            if (m & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
                c->readable = true;
            if (m & (EPOLLOUT | EPOLLHUP | EPOLLERR))
                c->writable = true;
            ep_want(c);
        }
        ep_round();
        /* the deadlines after the round: what this epoll_wait reported (a socket drained, a frame's bytes) counts */
        if (tick)
            on_tick();
        notes_pending();
    }
}

static size_t workers_now(void)
{
    pthread_mutex_lock(&work_lock);
    size_t n = (size_t)running_workers;
    pthread_mutex_unlock(&work_lock);
    return n;
}

/* Accepts connections until the listening socket is shut down. */
static void *accept_main(void *arg)
{
    int lfd = *(int *)arg;
#ifdef __APPLE__ /* shutdown() does not wake an accept() there: poll, and look at `stopping` between the waits */
    fcntl(lfd, F_SETFL, fcntl(lfd, F_GETFL) | O_NONBLOCK);
#endif
    for (;;) {
#ifdef __APPLE__
        struct pollfd pf = {.fd = lfd, .events = POLLIN};
        if (stopping)
            return NULL;
        int pr = poll(&pf, 1, 100);
        if (pr < 0 && errno != EINTR)
            return NULL;
        if (pr <= 0)
            continue;
        if (pf.revents & (POLLHUP | POLLERR | POLLNVAL))
            return NULL;
#endif
#ifdef __APPLE__ /* MSG_DONTWAIT does not stop a send on a blocking socket there: the event loop would wait for a client
                  * that does not read. The sockets themselves must be non-blocking. */
        int fd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC | SOCK_NONBLOCK);
#else
        int fd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC);
#endif
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
#ifdef __APPLE__
            if (errno == EAGAIN)
                continue;
#endif
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
                struct timespec d = {0, 10000000}; /* out of descriptors: let some close */
                nanosleep(&d, NULL);
                continue;
            }
            return NULL; /* EINVAL: shut down */
        }
        pthread_mutex_lock(&note_lock);
        if (nnew == new_cap) {
            new_cap = new_cap ? new_cap * 2 : 64;
            new_fds = realloc(new_fds, new_cap * sizeof *new_fds);
            if (!new_fds)
                abort();
        }
        new_fds[nnew++] = fd;
        pthread_mutex_unlock(&note_lock);
        wake_signal();
    }
}

static atomic_bool db_failed;

/* Polls tsdb_failed: a failure can come from any worker or from a background
 * thread of the library, with no request to report it. */
static void *failwatch_main(void *arg)
{
    int lfd = *(int *)arg;
    char msg[512];
    while (!stopping) {
        if (dom_failwatch(msg, sizeof msg)) {
            fprintf(stderr, "tsdbd: storage failure, exiting: %s\n", msg);
            atomic_store(&db_failed, true);
            shutdown(lfd, SHUT_RDWR); /* new clients go elsewhere; ends the accept thread */
            struct timespec grace = {0, 200000000}; /* replies in flight (TSDB_ERR_IO for writes) go out */
            nanosleep(&grace, NULL);
            stopping = 1;
            wake_signal();
            return NULL;
        }
        struct timespec d = {0, 20000000};
        nanosleep(&d, NULL);
    }
    return NULL;
}

static void on_signal(int sig)
{
    (void)sig;
    stopping = 1;
    wake_signal();
}

/* Binds and listens; *port_p is updated to the port actually bound, which is the kernel's choice for port 0. */
static int listen_on(const char *addr, int *port_p)
{
    int port = *port_p;
    int fd = tsdb_socket_cloexec(AF_INET6, SOCK_STREAM, 0);
    struct sockaddr_in6 sa6 = {.sin6_family = AF_INET6, .sin6_port = htons((uint16_t)port)};
    struct sockaddr_in sa4 = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    struct sockaddr *sa;
    socklen_t salen;
    if (inet_pton(AF_INET, addr, &sa4.sin_addr) == 1) {
        close(fd);
        fd = tsdb_socket_cloexec(AF_INET, SOCK_STREAM, 0);
        sa = (struct sockaddr *)&sa4;
        salen = sizeof sa4;
    } else if (inet_pton(AF_INET6, addr, &sa6.sin6_addr) == 1) {
        sa = (struct sockaddr *)&sa6;
        salen = sizeof sa6;
    } else {
        fprintf(stderr, "tsdbd: bad listen address %s\n", addr);
        return -1;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (bind(fd, sa, salen) != 0 || listen(fd, 1024) != 0) {
        fprintf(stderr, "tsdbd: cannot listen on %s:%d: %s\n", addr, port, strerror(errno));
        close(fd);
        return -1;
    }
    struct sockaddr_storage bound;
    socklen_t bl = sizeof bound;
    if (getsockname(fd, (struct sockaddr *)&bound, &bl) == 0)
        *port_p = ntohs(bound.ss_family == AF_INET ? ((struct sockaddr_in *)&bound)->sin_port
                                                   : ((struct sockaddr_in6 *)&bound)->sin6_port);
    return fd;
}

/* "PORT PID [METRICS_PORT]\n", written to a temporary file and renamed once the listener is up, so a reader sees
 * all of it or none (a half-written pid would read as another server's, and the harness would refuse a good one). A stale file is removed at startup: whoever finds one now knows this server wrote it. */
static int write_port_file(const char *path, int port, int mport, int pport)
{
    /* A name nobody can guess, created new (mkostemp: O_EXCL, so never through a link planted there): the directory
     * may be shared. */
    char tmp[4096];
    if (snprintf(tmp, sizeof tmp, "%s.tmp.XXXXXX", path) >= (int)sizeof tmp)
        return -1;
#ifdef __APPLE__
    int fd = mkstemp(tmp);
    if (fd >= 0)
        fcntl(fd, F_SETFD, FD_CLOEXEC);
#else
    int fd = mkostemp(tmp, O_CLOEXEC);
#endif
    if (fd >= 0 && fchmod(fd, 0644) != 0) {
        close(fd);
        unlink(tmp);
        return -1;
    }
    FILE *f = fd < 0 ? NULL : fdopen(fd, "w");
    if (!f) {
        if (fd >= 0) {
            close(fd);
            unlink(tmp);
        }
        return -1;
    }
    int ok = fprintf(f, "%d %d", port, (int)getpid()) > 0;
    if (mport || pport)
        ok = (fprintf(f, " %d", mport) > 0 && ok);
    if (pport)
        ok = (fprintf(f, " %d", pport) > 0 && ok);
    ok = (fputc('\n', f) != EOF && ok);
    ok = (fflush(f) == 0 && ok);
    ok = (fclose(f) == 0 && ok);
    if (!ok || rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

/* A numeric flag: all of `s` a decimal integer within [lo, hi], or a message and exit status 2. */
static long long num_arg(const char *flag, const char *s, long long lo, long long hi)
{
    char *end;
    errno = 0;
    long long v = strtoll(s, &end, 10);
    if (!*s || *end || errno == ERANGE || v < lo || v > hi) {
        fprintf(stderr, "tsdbd: --%s %s: expected a whole number from %lld to %lld\n", flag, s, lo, hi);
        exit(2);
    }
    return v;
}

/* Without --verify-checksums a damaged or misdirected data block decodes to wrong values unless the storage checks
 * its data itself (ZFS, btrfs). ext2/3/4 and XFS do not, and statfs names them; NFS hides what is behind it, so it
 * gets no line (production: ZFS behind NFS). One line per volume on such a file system, at startup. */
static void fs_warn(tsdb *t, int verify)
{
    if (verify)
        return;
    for (uint32_t i = 0; i < tsdb_volume_count(t); i++) {
        const char *p = tsdb_volume_path(t, i);
        struct statfs sf;
        if (!p || statfs(p, &sf) != 0)
            continue;
#ifdef __APPLE__ /* f_type is only an index there; the name says it, and neither APFS nor HFS+ checksums data */
        const char *fs = !strcmp(sf.f_fstypename, "apfs") ? "APFS" : !strcmp(sf.f_fstypename, "hfs") ? "HFS+" : NULL;
#else
        const char *fs = sf.f_type == 0xEF53 ? "ext4" : sf.f_type == 0x58465342 ? "XFS" : NULL;
#endif
        if (fs)
            fprintf(stderr, "tsdbd: %s is on %s, which keeps no checksums of its data, and --verify-checksums is off: "
                            "a damaged or misdirected data block would be read as wrong values. Turn on "
                            "--verify-checksums, or use a file system that checks its data (ZFS)\n", p, fs);
    }
}

int main(int argc, char **argv)
{
    const char *dir = NULL, *addr = "127.0.0.1", *port_file = NULL;
    int port = TSDB_DEFAULT_PORT, nworkers = (int)sysconf(_SC_NPROCESSORS_ONLN);
    tsdb_options o;
    tsdb_options_init(&o);
    o.stats = 1;          /* statistics follow the open (docs/design-stats.md); --no-stats turns them off */
    o.create_volumes = 0; /* a client names a volume an operator added (tsdb-cli volume add), never a new one */
    static struct option opts[] = {{"dir", required_argument, 0, 'd'},        {"listen", required_argument, 0, 'l'},
                                   {"port", required_argument, 0, 'p'},       {"workers", required_argument, 0, 'w'},
                                   {"wal-sync-ms", required_argument, 0, 's'}, {"no-fsync", no_argument, 0, 'F'},
                                   {"read-only", no_argument, 0, 'r'},        {"follow", no_argument, 0, 'f'},
                                   {"refresh-ms", required_argument, 0, 'R'}, {"max-connections", required_argument, 0, 'm'},
                                   {"wal-max-batch-rows", required_argument, 0, 'B'},
                                   {"partition-ms", required_argument, 0, 'P'},
                                   {"partition-classes", required_argument, 0, 'K'},
                                   {"granule-rows", required_argument, 0, 'g'},
                                   {"merge-idle-s", required_argument, 0, 'G'},
                                   {"insert-zstd-level", required_argument, 0, 'Z'},
                                   {"flush-interval-ms", required_argument, 0, 'H'},
                                   {"send-timeout", required_argument, 0, 'T'},
                                   {"io", required_argument, 0, 'I'},
                                   {"verify-checksums", no_argument, 0, 'V'},
                                   {"pin-index", no_argument, 0, 'J'},
                                   {"metrics-port", required_argument, 0, 'M'},
                                   {"metrics-bind", required_argument, 0, 'b'},
                                   {"port-file", required_argument, 0, 'Q'},
                                   {"max-request-memory", required_argument, 0, 'X'},
                                   {"frame-timeout", required_argument, 0, 'D'},
                                   {"query-threads", required_argument, 0, 'q'},
                                   {"request-threads", required_argument, 0, 'L'},
                                   {"multi-max-rows", required_argument, 0, 'U'},
                                   {"lttb-max-threshold", required_argument, 0, 'W'},
                                   {"version", no_argument, 0, 'v'},
                                   {"no-auth", no_argument, 0, 'A'},
                                   {"login-timeout", required_argument, 0, 'O'},
                                   {"exit-with-parent", optional_argument, 0, 'E'},
                                   {"health-domain", required_argument, 0, 0x100},
                                   {"no-stats", no_argument, 0, 0x101},
                                   {"latest-cache", required_argument, 0, 0x107},
                                   {"cluster", no_argument, 0, 'c'},
                                   {"peer-port", required_argument, 0, 'y'},
                                   {"peer-listen", required_argument, 0, 'Y'},
                                   {"advertise", required_argument, 0, 'a'},
                                   {"rack", required_argument, 0, 'k'},
                                   {"cluster-secret-file", required_argument, 0, 'S'},
                                   {"state-dir", required_argument, 0, 0x102},
                                   {"power-feed", required_argument, 0, 0x103},
                                   {"bmc", required_argument, 0, 0x104},
                                   {"witness", no_argument, 0, 0x105},
                                   {"read-barrier", required_argument, 0, 0x106},
                                   {0, 0, 0, 0}};
    int ch, metrics_port = 0, metrics_on = 0, no_auth = 0, cluster_on = 0, peer_port = -1, cluster_opts = 0;
    int witness = 0, rb_off = 0;
    char *health_domains[DOM_MAX];
    int nhealth = 0;
    const char *peer_addr = NULL, *advertise = NULL, *rack = "", *secret_file = NULL;
    const char *state_dir = NULL, *power_feed = "", *bmc = "";
    const char *metrics_addr = NULL;
    long pages = sysconf(_SC_PHYS_PAGES), page = sysconf(_SC_PAGESIZE);
    if (pages > 0 && page > 0 && (size_t)pages / 8 * (size_t)page > req_budget)
        req_budget = (size_t)pages / 8 * (size_t)page; /* 1024 MB slowed 32 connections loading at full speed */
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'v': printf("tsdbd %s (build %s)\n", tsdb_version(), TSDB_BUILD_VERSION); return 0;
        case 'd': dir = optarg; break;
        case 0x100:
            if (nhealth >= DOM_MAX) {
                fprintf(stderr, "tsdbd: at most %d --health-domain\n", DOM_MAX);
                return 2;
            }
            health_domains[nhealth++] = optarg;
            break;
        case 'l': addr = optarg; break;
        case 'p': port = (int)num_arg("port", optarg, 0, 65535); break; /* 0: any free port, told by --port-file and the banner */
        case 'Q': port_file = optarg; break;
        case 'w': nworkers = (int)num_arg("workers", optarg, 1, 4096); break;
        case 's': o.wal_sync_ms = (int)num_arg("wal-sync-ms", optarg, -1, 3600000); break; /* -1: no WAL */
        case 'F': o.fsync = 0; break;
        case 'V': o.verify_checksums = 1; break;
        case 'J': o.pin_index = 1; break;
        case 'M': {
            if (!strcmp(optarg, "auto")) { /* a port the kernel chooses; the port file names it */
                metrics_on = 1;
                metrics_port = 0;
                break;
            }
            metrics_on = 1;
            metrics_port = (int)num_arg("metrics-port", optarg, 1, 65535);
            break;
        }
        case 'b': metrics_addr = optarg; break;
        case 'r': o.read_only = 1; break;
        case 'f': o.follow = 1; break;
        case 'R': o.refresh_ms = (int)num_arg("refresh-ms", optarg, 1, 3600000); break;
        case 'm': max_conns = (size_t)num_arg("max-connections", optarg, 1, 10000000); break;
        case 'B': o.wal_max_batch_rows = (size_t)num_arg("wal-max-batch-rows", optarg, 1, (long long)1 << 32); break;
        case 'P': /* only when the database is created */
            o.partition_ms = num_arg("partition-ms", optarg, 1, (long long)1 << 50);
            break;
        case 'K': /* only when the database is created: 0 keeps every series in base partitions */
            o.partition_classes = (int)num_arg("partition-classes", optarg, 0, 1);
            break;
        case 'g': /* only when the database is created */
            o.granule_rows = (uint32_t)num_arg("granule-rows", optarg, 1, 1 << 20);
            break;
        case 'G': o.merge_idle_s = (int)num_arg("merge-idle-s", optarg, -1, 86400 * 365); break;
        case 'Z': o.insert_zstd_level = (int)num_arg("insert-zstd-level", optarg, -1, 22); break;
        case 'H': o.flush_interval_ms = (int)num_arg("flush-interval-ms", optarg, 1, 86400000); break;
        case 'T': send_timeout_s = (int)num_arg("send-timeout", optarg, 1, 86400); break;
        case 'q': o.query_threads = (int)num_arg("query-threads", optarg, 1, 4096); break;
        case 'L': o.request_threads = (int)num_arg("request-threads", optarg, 1, 4096); break;
        case 'U': multi_max_rows = (uint64_t)num_arg("multi-max-rows", optarg, 1, LLONG_MAX); break;
        case 'W': lttb_max_threshold = (uint64_t)num_arg("lttb-max-threshold", optarg, 3, LLONG_MAX); break;
        case 'D': frame_timeout_s = (int)num_arg("frame-timeout", optarg, 1, 86400); break;
        case 'A': no_auth = 1; break;
        case 0x101: o.stats = -1; break;
        case 0x107: o.latest_cache = (size_t)num_arg("latest-cache", optarg, 0, LLONG_MAX); break;
        case 'c': cluster_on = 1; break;
        case 'y': peer_port = (int)num_arg("peer-port", optarg, 0, 65535); cluster_opts = 1; break;
        case 'Y': peer_addr = optarg; cluster_opts = 1; break;
        case 'a': advertise = optarg; cluster_opts = 1; break;
        case 'k': rack = optarg; cluster_opts = 1; break;
        case 'S': secret_file = optarg; cluster_opts = 1; break;
        case 0x102: state_dir = optarg; cluster_opts = 1; break;
        case 0x103: power_feed = optarg; cluster_opts = 1; break;
        case 0x104: bmc = optarg; cluster_opts = 1; break;
        case 0x105: witness = 1; cluster_opts = 1; break;
        case 0x106:
            if (!strcmp(optarg, "off"))
                rb_off = 1;
            else if (strcmp(optarg, "on"))
                goto usage;
            cluster_opts = 1;
            break;
        case 'E':
            start_ppid = getppid();
            if (start_ppid == 1) { /* the parent is gone already (or never was one to follow) */
                fprintf(stderr, "tsdbd: --exit-with-parent: the parent has already exited\n");
                return 2;
            }
            /* =PID: the harness's own pid. Without it a launcher that dies between fork and here (a shell that
             * exits, a test killed in that window) hands tsdbd to a subreaper (systemd --user), which getppid()
             * then names as the parent to follow, and the server outlives its test. */
            if (optarg && start_ppid != (pid_t)num_arg("exit-with-parent", optarg, 2, INT_MAX)) {
                fprintf(stderr, "tsdbd: --exit-with-parent=%s: the parent is %d, not %s: it has exited\n", optarg,
                        (int)start_ppid, optarg);
                return 2;
            }
            break;
        case 'O': login_timeout_s = (int)num_arg("login-timeout", optarg, 1, 86400); break;
        case 'X': req_budget = (size_t)num_arg("max-request-memory", optarg, 1, (long long)1 << 24) << 20; break;
        case 'I':
            if (strcmp(optarg, "auto") == 0)
                io = IO_AUTO;
            else if (strcmp(optarg, "uring") == 0)
                io = IO_URING;
            else if (strcmp(optarg, "epoll") == 0)
                io = IO_EPOLL;
            else
                goto usage;
            break;
        default: goto usage;
        }
    }
    if (cluster_on && (!secret_file || !state_dir || o.read_only || o.follow)) {
        fprintf(stderr, "tsdbd: --cluster needs --cluster-secret-file and --state-dir (a local disk, never --dir), "
                        "and a writer (not --read-only or --follow)\n");
        return 2;
    }
    if (witness && (!cluster_on || dir)) {
        fprintf(stderr, "tsdbd: --witness is a cluster member without volumes: it needs --cluster, and takes no "
                        "--dir\n");
        return 2;
    }
    if ((!dir && !witness) || nworkers < 1 || send_timeout_s < 1 || metrics_port < 0 || metrics_port > 65535 ||
        (metrics_addr && !metrics_on) || (cluster_opts && !cluster_on)) {
    usage:
        fprintf(stderr, "usage: tsdbd --version\n"
                        "       tsdbd --dir DIR [--listen ADDR] [--port %d|0] [--port-file FILE] [--workers N]\n"
                        "             [--wal-sync-ms MS] [--partition-ms MS] [--partition-classes 1|0]\n"
                        "             [--granule-rows 8192] (at creation)\n"
                        "             [--no-fsync] [--read-only | --follow [--refresh-ms 100]] [--max-connections 10000]\n"
                        "             [--send-timeout 60] [--io auto|uring|epoll] [--verify-checksums] [--pin-index]\n"
                        "             [--metrics-port N|auto [--metrics-bind ADDR]]\n"
                        "             [--max-request-memory MB] [--frame-timeout 60]\n"
                        "             [--query-threads N] [--request-threads N] [--multi-max-rows N]\n"
                        "             [--lttb-max-threshold 16777216]\n"
                        "             [--merge-idle-s 300|-1] [--flush-interval-ms 5000] [--insert-zstd-level 0|-1|N]\n"
                        "             [--no-auth] [--login-timeout 10] [--exit-with-parent[=PID]]\n"
                        "             [--health-domain PREFIX]... [--no-stats] [--latest-cache BYTES]\n"
                        "             [--cluster --cluster-secret-file F --state-dir DIR [--peer-port %d|0]\n"
                        "              [--peer-listen ADDR] [--advertise HOST] [--rack NAME] [--power-feed NAME]\n"
                        "              [--bmc ADDR] [--read-barrier on|off]]\n"
                        "       tsdbd --witness --cluster --cluster-secret-file F --state-dir DIR [--peer-port %d|0]\n"
                        "             [--peer-listen ADDR] --advertise HOST [--rack NAME] [--power-feed NAME]\n",
                TSDB_DEFAULT_PORT, TSDB_DEFAULT_PORT + 1, TSDB_DEFAULT_PORT + 1);
        return 2;
    }
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t secret_len = 0;
    if (cluster_on) {
        char err[512];
        if (cluster_read_secret(secret_file, secret, sizeof secret, &secret_len, err, sizeof err) != 0) {
            fprintf(stderr, "tsdbd: %s\n", err);
            return 1;
        }
        const char *host = advertise ? advertise : addr;
        if (!strcmp(host, "0.0.0.0") || !strcmp(host, "::")) {
            fprintf(stderr, "tsdbd: --cluster on a wildcard --listen needs --advertise HOST\n");
            return 2;
        }
    }
    if (witness) {
        /* A witness (design-ha-modes.md §7.1 item 7): the cluster thread and nothing else. No --dir, no engine, no
         * client port, volumes_ok 0 (never in placement); a voter candidate on any small machine with a local disk.
         * The port file names the peer port twice ("PEER PID 0 PEER"), so a harness that waits for the first number
         * finds a server that serves. */
        if (peer_port < 0)
            peer_port = TSDB_DEFAULT_PORT + 1;
        if (port_file)
            unlink(port_file);
        int wfd = listen_on(peer_addr ? peer_addr : addr, &peer_port);
        if (wfd < 0)
            return 1;
        signal(SIGINT, on_signal);
        signal(SIGTERM, on_signal);
        signal(SIGPIPE, SIG_IGN);
        signal(SIGHUP, SIG_IGN);
        const char *host = advertise ? advertise : addr;
        const char *lb = strchr(host, ':') ? "[" : "", *rb = strchr(host, ':') ? "]" : "";
        char cadv[256], padv[256];
        snprintf(cadv, sizeof cadv, "%s%s%s:0", lb, host, rb);
        snprintf(padv, sizeof padv, "%s%s%s:%d", lb, host, rb, peer_port);
        struct cluster_cfg wc = {.state_dir = state_dir, .peer_fd = wfd, .advertise = cadv, .peer_advertise = padv,
                                 .rack = rack, .power_feed = power_feed, .bmc = bmc, .witness = 1,
                                 .secret = secret, .secret_len = secret_len};
        if (cluster_start(&wc) != 0)
            return 1;
        tsdb_wipe(secret, sizeof secret);
        fprintf(stderr, "tsdbd: witness: cluster peer port on %s:%d (advertised as %s), no volumes\n",
                peer_addr ? peer_addr : addr, peer_port, padv);
        if (port_file && write_port_file(port_file, peer_port, 0, peer_port) != 0) {
            fprintf(stderr, "tsdbd: cannot write %s: %s\n", port_file, strerror(errno));
            return 1;
        }
        while (!atomic_load(&stopping)) {
            if (start_ppid && getppid() != start_ppid) {
                fprintf(stderr, "tsdbd: --exit-with-parent: parent %d exited, stopping\n", (int)start_ppid);
                break;
            }
            struct timespec d = {0, 50000000};
            nanosleep(&d, NULL);
        }
        fprintf(stderr, "tsdbd: shutting down\n");
        cluster_stop();
        return 0;
    }
    if (port_file)
        unlink(port_file);
    int rc = tsdb_open(dir, &o, &db);
    if (rc) {
        fprintf(stderr, "tsdbd: %s: %s\n", dir, tsdb_strerror(rc));
        return 1;
    }
    /* A13: login unless --no-auth, and then a superuser must exist; USERS is read before anyone connects */
    if (no_auth)
        fprintf(stderr, "tsdbd: --no-auth: no login, every client may read and write every series (user admin ops "
                        "are refused, A28): listen on a trusted interface only\n");
    g_reader_only = o.read_only || o.follow;
    if (auth_init(dir, !no_auth, !o.read_only && !o.follow, db) != 0) {
        tsdb_close(db);
        return 1;
    }
    tsdb_set_write_wait_hook(db, write_on_wait, NULL); /* before the workers start */
    if (dom_init(db, health_domains, nhealth, dom_waits) != 0) {
        tsdb_close(db);
        return 1;
    }
    if (getenv("TSDBD_TEST_HOLD_LARGE_MS") && (hold_large_ms = atoi(getenv("TSDBD_TEST_HOLD_LARGE_MS"))) > 0)
        tsdb_test_precommit(db, hold_large, NULL);
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max; /* one descriptor per connection */
#ifdef __APPLE__ /* the hard limit is "infinity", which setrlimit refuses: the kernel's own cap */
        if (rl.rlim_cur > OPEN_MAX)
            rl.rlim_cur = OPEN_MAX;
#endif
        setrlimit(RLIMIT_NOFILE, &rl);
    }
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY && max_conns + 64 > rl.rlim_cur) {
        max_conns = rl.rlim_cur > 64 ? rl.rlim_cur - 64 : 1;
        fprintf(stderr, "tsdbd: descriptor limit %llu: at most %zu connections\n", (unsigned long long)rl.rlim_cur,
                max_conns);
    }
    /* static: the accept and failwatch threads read it through a pointer, and a startup failure after they began
     * (the port file) returns from main while they may not have read it yet */
    static int lfd;
    if (cluster_on && peer_port < 0) /* by default the client port + 1; with --port 0 one the kernel chooses */
        peer_port = port == 0 || port == 65535 ? 0 : port + 1;
    lfd = listen_on(addr, &port);
    if (lfd < 0)
        return 1;
    int pfd = -1;
    if (cluster_on) {
        if ((pfd = listen_on(peer_addr ? peer_addr : addr, &peer_port)) < 0)
            return 1;
    }
    int mfd = -1;
    if (metrics_on)
        metrics_enable(); /* before any connection: the hooks count from the start */
    if (metrics_on && (mfd = listen_on(metrics_addr ? metrics_addr : addr, &metrics_port)) < 0)
        return 1;
    wake_fd = wake_open();
    if (wake_fd < 0) {
        fprintf(stderr, "tsdbd: wake-up descriptor: %s\n", strerror(errno));
        return 1;
    }
#ifdef TSDBD_NO_URING
    if (io == IO_URING) {
        fprintf(stderr, "tsdbd: io_uring: not in this build (NO_URING=1)\n");
        return 1;
    }
    io = IO_EPOLL;
#else
    if (io != IO_EPOLL) {
        struct io_uring_params params = {.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN};
        if ((rc = io_uring_queue_init_params(4096, &ring, &params)) < 0) {
            params = (struct io_uring_params){0}; /* older kernels */
            rc = io_uring_queue_init_params(4096, &ring, &params);
        }
        /* Under auto every error falls back, the reason logged: EPERM is policy
         * (kernel.io_uring_disabled, seccomp), ENOSYS a kernel without it, EINVAL
         * one too old for what we ask, EACCES a security module, ENOMEM a locked
         * memory limit; epoll serves the same clients in all of these. */
        if (rc < 0 && io == IO_URING) {
            fprintf(stderr, "tsdbd: io_uring: %s\n", strerror(-rc));
            return 1;
        }
        if (rc < 0)
            fprintf(stderr, "tsdbd: io_uring unavailable (%s%s), using epoll\n", strerror(-rc),
                    rc == -EPERM ? ": see sysctl kernel.io_uring_disabled" : "");
        io = rc < 0 ? IO_EPOLL : IO_URING;
    }
#endif
    tick_fd = tick_open();
    if (tick_fd < 0) {
        fprintf(stderr, "tsdbd: " TICK_WHAT ": %s\n", strerror(errno));
        return 1;
    }
    if (io == IO_EPOLL) {
        struct epoll_event e = {.events = EPOLLIN, .data.fd = wake_fd}, t = {.events = EPOLLIN, .data.fd = tick_fd};
        if ((epfd = epoll_create1(EPOLL_CLOEXEC)) < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, wake_fd, &e) != 0 ||
            epoll_ctl(epfd, EPOLL_CTL_ADD, tick_fd, &t) != 0) {
            fprintf(stderr, "tsdbd: epoll: %s\n", strerror(errno));
            return 1;
        }
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN); /* a client that went away is a send error, not a death */
    signal(SIGHUP, SIG_IGN);  /* nothing to reload; a closing terminal must not take the server down */
    pthread_t *workers = calloc((size_t)nworkers, sizeof *workers);
    base_workers = running_workers = nworkers;
    if (mfd >= 0) {
        metrics_cfg mc = {.db = db,
                          .listen_fd = mfd,
                          .io = io == IO_URING ? "io_uring" : "epoll",
                          .role = o.follow ? "follower" : o.read_only ? "read-only" : "writer",
                          .max_conns = max_conns,
                          .workers = workers_now,
                          .request_memory = request_memory_now,
                          .request_memory_limit = req_budget};
        int mrc = metrics_start(&mc);
        if (mrc != 0) { /* the operator asked for it: a monitoring gap must not be silent */
            fprintf(stderr, "tsdbd: metrics thread: %s\n", strerror(-mrc));
            return 1;
        }
        fprintf(stderr, "tsdbd: metrics on %s:%d\n", metrics_addr ? metrics_addr : addr, metrics_port);
    }
    pthread_t acceptor, failwatch;
    for (int i = 0; i < nworkers; i++) {
        if ((rc = tsdb_thread_create(&workers[i], worker_main, NULL)) != 0) {
            fprintf(stderr, "tsdbd: cannot start worker %d: %s\n", i + 1, strerror(rc));
            return 1;
        }
    }
    if ((rc = tsdb_thread_create(&acceptor, accept_main, &lfd)) != 0 ||
        (rc = tsdb_thread_create(&failwatch, failwatch_main, &lfd)) != 0) {
        fprintf(stderr, "tsdbd: cannot start a thread: %s\n", strerror(rc));
        return 1;
    }
    if ((rc = auth_start_reload()) != 0) {
        fprintf(stderr, "tsdbd: cannot start the USERS reload thread: %s\n", strerror(rc));
        return 1;
    }
    if (cluster_on) {
        const char *host = advertise ? advertise : addr;
        const char *lb = strchr(host, ':') ? "[" : "", *rb = strchr(host, ':') ? "]" : "";
        char cadv[256], padv[256];
        snprintf(cadv, sizeof cadv, "%s%s%s:%d", lb, host, rb, port);
        snprintf(padv, sizeof padv, "%s%s%s:%d", lb, host, rb, peer_port);
        struct cluster_cfg cc = {.state_dir = state_dir, .peer_fd = pfd, .advertise = cadv, .peer_advertise = padv,
                                 .rack = rack, .power_feed = power_feed, .bmc = bmc, .read_barrier_off = rb_off,
                                 .secret = secret, .secret_len = secret_len};
        if (cluster_start(&cc) != 0)
            return 1;
        tsdb_wipe(secret, sizeof secret);
        fprintf(stderr, "tsdbd: cluster peer port on %s:%d (advertised as %s)\n", peer_addr ? peer_addr : addr,
                peer_port, padv);
    }
    fs_warn(db, o.verify_checksums);
    /* io stays last: scripts read it from the end of this line (bench-tsdb.sh, test_stability) */
    fprintf(stderr, "tsdbd: serving %s%s on %s:%d, %d workers, up to %zu connections, %s, simd %s, io %s\n", dir,
            o.follow ? " (following its writer)" : o.read_only ? " (read only)" : "", addr, port, nworkers,
            max_conns, no_auth ? "no login (--no-auth)" : "login required", tsdb_simd_level(),
            io == IO_URING ? "io_uring" : "epoll");
    if (port_file && write_port_file(port_file, port, metrics_on ? metrics_port : 0,
                                      cluster_on ? peer_port : 0) != 0) { /* last: whoever reads it finds a server that serves */
        fprintf(stderr, "tsdbd: cannot write %s: %s\n", port_file, strerror(errno));
        return 1;
    }
#ifndef TSDBD_NO_URING
    if (io == IO_URING)
        uring_loop();
    else
#endif
        ep_loop();

    fprintf(stderr, "tsdbd: shutting down\n");
    shutdown(lfd, SHUT_RDWR); /* stops listening, and ends the accept thread's accept() */
    pthread_join(acceptor, NULL);
    pthread_join(failwatch, NULL);
    close(lfd);
    if (mfd >= 0)
        metrics_stop(); /* before tsdb_close: it reads the database */
    if (cluster_on)
        cluster_stop();
    pthread_mutex_lock(&work_lock);
    work_stop = true;
    pthread_cond_broadcast(&work_cv);
    pthread_mutex_unlock(&work_lock);
    for (conn *c = conns; c; c = c->next) { /* wake workers waiting for output space */
        pthread_mutex_lock(&c->lock);
        atomic_store(&c->closing, true);
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->lock);
    }
    for (int i = 0; i < nworkers; i++)
        pthread_join(workers[i], NULL);
    free(workers);
    pthread_mutex_lock(&work_lock); /* and the extra ones, which are detached */
    while (running_workers > 0)
        pthread_cond_wait(&workers_gone_cv, &work_lock);
    pthread_mutex_unlock(&work_lock);
#ifndef TSDBD_NO_URING
    if (io == IO_URING)
        io_uring_queue_exit(&ring);
    else
#endif
        close(epfd);
    while (conns)
        conn_free(conns);
    while (ready_head) { /* only dead ones are left */
        conn *c = ready_head;
        ready_head = c->ready_next;
        free(c);
    }
    free(fd_conn);
    auth_shutdown(); /* after the connections, whose states it frees */
    for (size_t i = 0; i < nnew; i++)
        close(new_fds[i]);
    free(new_fds);
    dom_shutdown();
    rc = tsdb_close(db);
    if (atomic_load(&db_failed))
        return 1; /* close failed too, by design: nothing more was written */
    if (rc)
        fprintf(stderr, "tsdbd: close: %s\n", tsdb_strerror(rc));
    return rc ? 1 : 0;
}
