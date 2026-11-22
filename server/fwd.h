/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Forwarding links, the transport half (ha.md 9.1 "Peer links" and "Streamed replies"; design-cluster.md C25): a
 * peerlink of its own on every node of a cluster, with a FWD link (forwarded requests and their replies) and a FWDQ
 * link (forwarded queries, whose replies stream in chunks under CREDIT) to every other member, so a forwarded insert
 * never waits behind a replication driver's 100 MB part, and a slow query reader stalls only its own query. What a
 * request means is the caller's (s2f); this file moves bytes and says, per frame, what is known of its fate.
 *
 * Outcomes. fwd_send queues one message; its fate comes back as events: SENT NEVER_WRITTEN (no byte of it reached
 * the socket: definite, the peer cannot act on it), SENT WRITTEN (some did: indeterminate until a reply), then REPLY
 * (or CHUNKs up to the last), or LOST (its connection ended before the reply), or TIMEOUT (its deadline passed
 * after it was written; a later reply is dropped). fwd_cancel ends a frame from the caller's side (and tells the
 * owner of a query to stop its stream). A link is closed only when it is dead (peerlink's keepalive and
 * dead_ms), never because a reply is late. Every frame carries a frame id the caller chose, unique among its frames
 * in flight; a reply names the frame and is taken only from the connection the frame was written on (another
 * connection's is dropped and counted).
 *
 * Threads. The links' threads never call the consumer: what they read is copied into an inbox and delivered in order
 * by one dispatcher thread (fwd_cfg.on_event, which must not block either: it hands off to the caller's queues). The
 * inbox is bounded per kind (shared by every link of the kind); a link whose message takes it past the bound is
 * paused (plink_pause: it reads no more, TCP pushes back on the peer, keepalive goes on both ways, so the link is not
 * counted dead) until the dispatcher has drained it to half; it never drops a message, and the bound is soft by what
 * the paused links had already read. One manager thread times out frames past their deadline.
 *
 * Wire (peerlink user frame types; every body begins with the u64 frame id, little-endian):
 *   FWD:  REQ  (1) frame_id, body          REP   (2) frame_id, body
 *   FWDQ: QREQ (1) frame_id, body          CHUNK (2) frame_id, u8 flags (1: last), at most FWD_CHUNK_MAX bytes
 *         CREDIT (3) frame_id, u32 chunks  (entry -> owner: that many more chunks may come)
 *         CANCEL (4) frame_id              (entry -> owner: the query is given up, its stream goes)
 * A query stream starts with fwd_cfg.credit chunks (default 4 of up to 1 MiB) granted; the owner sends a chunk only
 * within its credit (fwd_chunk waits), and the entry grants more as its client drains (fwd_credit). fwd_cfg.credit
 * must be the same on every node: the entry counts what the owner may still send from it.
 *
 * Shutdown: stop what hands sockets to fwd_peerlink() (the acceptor's table entries for PL_KIND_FWD and _FWDQ set
 * NULL and the cluster thread stopped), and every thread that calls into the instance (one waiting in fwd_chunk
 * included), before fwd_free (peerlink.h): fwd_free frees what such a call would touch.
 */
#ifndef TSDB_FWD_H
#define TSDB_FWD_H

#include <stddef.h>
#include <stdint.h>

#include "peerlink.h"

#define FWD_HDR 8                              /* the frame id before every body */
#define FWD_MSG_MAX (PL_FRAME_MAX - FWD_HDR)   /* a request or reply body: the whole frame fits one peerlink frame */
#define FWD_CHUNK_MAX (1u << 20)               /* a streamed reply's chunk */
#define FWD_CREDIT_DEFAULT 4                   /* chunks granted when a query stream starts */
#define FWD_INBOX_BYTES (64u << 20)            /* default inbox bound per kind */

enum { FWD_T_REQ = 1, FWD_T_REP = 2 };                            /* on PL_KIND_FWD */
enum { FWDQ_T_REQ = 1, FWDQ_T_CHUNK = 2, FWDQ_T_CREDIT = 3, FWDQ_T_CANCEL = 4 }; /* on PL_KIND_FWDQ */

enum { FWD_NEVER_WRITTEN = 0, FWD_WRITTEN = 1 };

enum {
    FWD_OK = 0,
    FWD_E_TOOBIG = -1,  /* above FWD_MSG_MAX (a chunk: FWD_CHUNK_MAX); the caller answers MOVED (s2f) */
    FWD_E_DOWN = -2,    /* no link to that peer up, or the stream is gone (its connection, or the entry's CANCEL) */
    FWD_E_FULL = -3,    /* the link's queue stayed full until the deadline */
    FWD_E_NOPEER = -4,  /* not a member (or this node) */
    FWD_E_INVALID = -5, /* a bad argument, or a frame id already in flight */
    FWD_E_CLOSED = -6,  /* the peerlink is closing (no call may race fwd_free itself: see Shutdown) */
    FWD_E_TIMEOUT = -7, /* fwd_chunk: no credit came within the timeout */
};

enum {
    FWD_EV_LINK = 1,    /* up/down of a link (dialed or accepted), with its connection number */
    FWD_EV_SENT = 2,    /* outcome: FWD_NEVER_WRITTEN (the frame is forgotten) or FWD_WRITTEN */
    FWD_EV_REPLY = 3,   /* the reply to a FWD request (the frame is done) */
    FWD_EV_CHUNK = 4,   /* a chunk of a FWDQ reply (last: the frame is done) */
    FWD_EV_LOST = 5,    /* written, then its connection ended before the reply: indeterminate */
    FWD_EV_TIMEOUT = 6, /* written, and its deadline passed with no reply: indeterminate (a late reply is dropped) */
    FWD_EV_REQUEST = 7, /* a peer's request (kind FWD) or query (FWDQ): answer through `from` */
};

/* where a request came from: the connection a reply must go back on */
struct fwd_from {
    struct plink *l;
    uint64_t gen;
    uint32_t peer;
    int kind;
};

struct fwd_ev {
    int type;
    int kind; /* PL_KIND_FWD or PL_KIND_FWDQ */
    uint32_t peer;
    uint64_t gen;
    uint64_t frame_id;
    int up;      /* LINK */
    int dialed;  /* LINK: this node dialed it */
    int outcome; /* SENT */
    int last;    /* CHUNK */
    struct fwd_from from; /* REQUEST */
    const uint8_t *p;     /* REPLY, CHUNK, REQUEST: the body (valid during the call) */
    size_t n;
};

struct fwd_cfg {
    struct pl_self self;
    const uint8_t *secret;
    size_t secret_len;
    char advertise[128];
    void *arg;
    void (*on_event)(void *arg, const struct fwd_ev *ev); /* NULL: events are dropped */
    uint32_t inbox_bytes; /* per kind; 0: FWD_INBOX_BYTES */
    uint32_t credit;      /* 0: FWD_CREDIT_DEFAULT */
    /* the peerlink's (0: its defaults) */
    uint32_t queue_bytes, ping_ms, dead_ms, backoff_min_ms, backoff_max_ms, hs_ms;
    int sock_buf; /* tests */
};

struct fwd;
struct fwd *fwd_new(const struct fwd_cfg *cfg);
void fwd_free(struct fwd *f);
/* the peerlink the acceptor hands FWD and FWDQ sockets to */
struct peerlink *fwd_peerlink(struct fwd *f);

struct fwd_member {
    uint32_t id;
    char peer[128]; /* its peer address, from the member list only (D35) */
};
/* The members to hold links to (this node's own id is skipped): a new one is dialed (FWD and FWDQ), one whose address
 * changed is redialed there, one no longer listed is stopped. Any thread. */
void fwd_set_members(struct fwd *f, const struct fwd_member *m, uint32_t n);
/* 1 when the dialed link of (peer, kind) is up */
int fwd_link_up(struct fwd *f, uint32_t peer, int kind);

struct fwd_msg {
    int kind; /* PL_KIND_FWD (a request; one REPLY) or PL_KIND_FWDQ (a query; CHUNKs) */
    const void *p;
    size_t n; /* at most FWD_MSG_MAX */
};
/* Queues `msg` for `peer` on this node's dialed link of msg->kind. FWD_OK: the outcome follows as events (SENT, then
 * REPLY/CHUNK, LOST or TIMEOUT). deadline_ns (CLOCK_MONOTONIC; 0: none): still in the link's queue by then it is
 * NEVER_WRITTEN (a frame already taken into the connection's write buffer cannot be taken back: it is reported when
 * its first byte goes, WRITTEN, or when the connection ends unwritten, NEVER_WRITTEN), written and unanswered by then
 * it is TIMEOUT. A FWDQ frame's span (deadline_ns - now) applies again after each chunk while the owner holds credit
 * (it owes the next chunk); while it holds none (this node's reader is slow) nothing ends the frame but its last
 * chunk, its connection, or fwd_cancel. A negative return is NEVER_WRITTEN at once (no event follows). Any thread;
 * never blocks (a full queue is FWD_E_FULL). */
int fwd_send(struct fwd *f, uint32_t peer, const struct fwd_msg *msg, uint64_t frame_id, uint64_t deadline_ns);
/* The reply to a FWD request, on the connection it came on (FWD_E_DOWN once that connection is gone). Any thread. */
int fwd_reply(struct fwd *f, const struct fwd_from *from, uint64_t frame_id, const void *p, size_t n);
/* One chunk of a FWDQ reply (at most FWD_CHUNK_MAX): waits up to timeout_ms for credit (FWD_E_TIMEOUT; 0: no
 * wait) and then for queue room (FWD_E_FULL). A chunk that was not queued keeps its credit and, the last, its stream:
 * the same chunk may be tried again. `last` ends the stream. FWD_E_DOWN once its connection is gone or the entry
 * canceled. One thread per stream; not on the dispatcher thread. */
int fwd_chunk(struct fwd *f, const struct fwd_from *from, uint64_t frame_id, const void *p, size_t n, int last,
              int timeout_ms);
/* The entry node grants `chunks` more chunks of the query frame_id it sent (its client drained). */
int fwd_credit(struct fwd *f, uint32_t peer, uint64_t frame_id, uint32_t chunks);
/* Gives up a frame in flight (the caller's client went or its deadline passed): no event of it is enqueued after
 * this returns (one already in the inbox may still be delivered: do not reuse the id at once), and a reply that comes
 * later is dropped. FWD_NEVER_WRITTEN: it was still queued and is taken out (definite: no byte of it goes); FWD_WRITTEN:
 * it may have reached the peer (indeterminate), and a query's owner is sent a CANCEL (best effort, no wait), so its
 * fwd_chunk says FWD_E_DOWN. FWD_E_INVALID: no such frame in flight to that peer. Any thread; never blocks. */
int fwd_cancel(struct fwd *f, uint32_t peer, uint64_t frame_id);

struct fwd_stats {
    uint64_t sent, never_written, written, replies, chunks, lost, timeouts;
    uint64_t dropped;          /* replies or chunks for no frame in flight on that connection (late, another gen) */
    uint64_t requests, credits;
    uint64_t canceled; /* fwd_cancel of a frame in flight */
    uint64_t cancels;  /* CANCELs received for the streams this node answers */
};
void fwd_stats(struct fwd *f, struct fwd_stats *st);

/* ---- wire (exported for the fuzzer) */
struct fwd_wire {
    uint64_t frame_id;
    uint32_t chunks; /* CREDIT */
    int last;        /* CHUNK */
    const uint8_t *body;
    size_t n;
};
/* One frame's payload of `kind` and `type`: 0, or -1 when it is not well formed (a short header, a CREDIT of another
 * length, a chunk above FWD_CHUNK_MAX or with unknown flags, an unknown type). Reads exactly n bytes of p. */
int fwd_wire_decode(int kind, uint8_t type, const uint8_t *p, size_t n, struct fwd_wire *w);
size_t fwd_wire_credit(uint8_t out[12], uint64_t frame_id, uint32_t chunks);

/* ---- tsdbd's instance (server/fwd_cluster.c): made once this server is a member (cluster_self), registered in the
 * acceptor's table for PL_KIND_FWD and _FWDQ, its members kept to the metadata copy's (every member not OUT, at the
 * peer address the copy holds). Start after cluster_start; stop after cluster_stop. fwd_cluster() is NULL until the
 * instance exists, and stays valid until fwd_cluster_free. on_event, when set before start, gets its events (else they
 * are logged: link up and down). fwd_cluster_stop ends the member poller and takes the instance out of the acceptor's
 * table (the links go on); fwd_cluster_free, once every thread that may call into the instance is joined (tsdbd's
 * workers), frees it. */
int fwd_cluster_start(void (*on_event)(void *arg, const struct fwd_ev *ev), void *arg);
void fwd_cluster_stop(void);
void fwd_cluster_free(void);
struct fwd *fwd_cluster(void);
/* the default handler of fwd_cluster_start: logs link up and down (downs once a second at most) */
void fwd_cluster_log_event(void *arg, const struct fwd_ev *ev);

/* test hooks: planted bugs (tests/test_peerlink.c --mutant) */
enum {
    FWD_MUT_NONE = 0,
    FWD_MUT_LATECLOSE = 1,  /* a frame's timeout resets its link */
    FWD_MUT_NOCREDIT = 2,   /* fwd_chunk does not wait for credit */
    FWD_MUT_INBOXWAIT = 3,  /* a link's thread waits for inbox room (its keepalive stops) */
    FWD_MUT_INBOXDROP = 4,  /* a message that does not fit the inbox is dropped */
    FWD_MUT_CHUNKFIRST = 5, /* fwd_chunk spends the credit, and the last its stream, before the send that may fail */
};
extern int fwd_mutant;

#endif
