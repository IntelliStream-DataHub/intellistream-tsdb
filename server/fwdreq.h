/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Forwarding one hop, the request half (S2-2; ha.md 9.1 "The entry node" and "Forwarded frames" without OWNERS and
 * sessions; design-cluster.md D29-D31, D-S3). server/fwd.h moves bytes and says what is known of a frame's fate; this
 * file says what a client's request means on the way: the FORWARD envelope, the tail of the owner's reply (its query
 * statistics), and the entry node's rules from the routing decision to the relayed reply. tsdbd.c runs the owner's
 * half (handle_in under the forwarded user, every gate shared) and gives fwdr_run its link and its client through
 * fwdr_ops, so tests/test_fwd.c drives the same rules with a fake link.
 *
 * The entry node, per request (one in flight per connection: tsdbd runs a connection's requests one at a time, D-S3):
 *   - the epoch is the copy's at the routing decision; the age counts from when the event loop read the frame;
 *   - before every send attempt: the stall hook (tests), then a frame older than fwd_max_age is never sent (OLD:
 *     answered UNAVAILABLE, definite), a client that has gone is never sent for (GONE: no answer, nobody reads it);
 *   - a send the link refuses (down, full) is tried again until fwd_wait_ms from the decision (UNREACH: UNAVAILABLE,
 *     definite); a frame still unwritten at that deadline is canceled: taken back (UNREACH) or not (INDET);
 *   - once written: the reply, relayed as it is (a query's chunks as they come, a credit granted per chunk the client
 *     took), or LOST / TIMEOUT (fwd_reply_ms) of the link: INDET, the client's connection is closed without an answer
 *     (D12: its outcome is unknown); a reply that comes later is dropped by fwd.c;
 *   - never a second send of a frame once fwd.c took it (D30: the client resends, through any node).
 *
 * The owner checks the envelope again (fwdr_env_decode; hops 1, its age with the time it waited there), and answers a
 * frame for a volume it does not lead at that epoch with MOVED, never forwarding it again.
 *
 * Wire (little-endian), the body of a FWD REQ or FWDQ QREQ:
 *   u32 magic "FWD1", u32 epoch, u16 hops (1), u16 flags (1: uid names a login), u32 origin node, u32 uid,
 *   u32 proto (the client's), u32 age_ms (at the send), u16 vol, u16 0, u64 users_applied,
 *   then the client's frame: u32 id, u8 op, payload (as received).
 * The reply: the client-wire reply frames as the owner made them (REP, or CHUNKs), and at the end of the last message
 * the tail: u32 magic "FWDT", u32 flags (1: stats, 2: close the client's connection), f64 elapsed_s, 10 x u64 stats
 * in LAST_STATS order.
 */
#ifndef TSDB_FWDREQ_H
#define TSDB_FWDREQ_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "tsdb.h"

#define FWDR_MAGIC 0x31445746u /* "FWD1" */
#define FWDR_TMAGIC 0x54445746u /* "FWDT" */
#define FWDR_ENV 40             /* the envelope before the client's frame */
#define FWDR_INNER 5            /* u32 id, u8 op */
#define FWDR_AGE_AT 24          /* offset of age_ms: stamped at each send attempt */
#define FWDR_TAIL 96

enum { FWDR_F_USER = 1 };
enum { FWDR_T_STATS = 1, FWDR_T_CLOSE = 2 };

struct fwdr_env {
    uint32_t epoch;
    uint16_t hops, flags;
    uint32_t origin, uid, proto, age_ms;
    uint16_t vol;
    uint64_t users_applied;
    uint32_t id; /* the client's frame */
    uint8_t op;
    const uint8_t *payload;
    size_t len;
};

/* The envelope and the client frame's id and op into out (FWDR_ENV + FWDR_INNER bytes); the payload follows. */
void fwdr_env_encode(uint8_t *out, const struct fwdr_env *e);
/* 0, or -1 when p (n bytes, the whole body) is not an envelope (short, another magic, hops 0). */
int fwdr_env_decode(const uint8_t *p, size_t n, struct fwdr_env *e);

struct fwdr_tail {
    uint32_t flags;
    tsdb_query_stats st;
};
void fwdr_tail_encode(uint8_t out[FWDR_TAIL], const struct fwdr_tail *t);
/* 0 and *t, or -1 when the last FWDR_TAIL bytes of p are no tail (n too short, another magic). */
int fwdr_tail_decode(const uint8_t *p, size_t n, struct fwdr_tail *t);

/* ---- the frames in flight (the entry node): events from fwd.c's dispatcher reach the request that waits for them */
struct fwdr_tab;
struct fwdr_tab *fwdr_tab_new(void);
void fwdr_tab_free(struct fwdr_tab *t);
struct fwd_ev;
/* SENT, REPLY, CHUNK, LOST, TIMEOUT of a frame some fwdr_run waits for; anything else, or a frame nobody waits for
 * (late, given up), is dropped and counted. Never blocks (the dispatcher's rule): copies the body. */
void fwdr_on_event(struct fwdr_tab *t, const struct fwd_ev *ev);
uint64_t fwdr_tab_dropped(struct fwdr_tab *t);

/* What fwdr_run needs of its world. The link calls are fwd.h's (send: fwd_send of one message). */
struct fwdr_ops {
    void *arg;
    int64_t (*now_ns)(void *arg); /* CLOCK_MONOTONIC */
    void (*sleep_ms)(void *arg, int ms);
    void (*waits)(void *arg, int delta); /* tsdbd: worker_waits, around every wait for the link */
    void (*stall)(void *arg);            /* tests: just before each send attempt; NULL none */
    bool (*client_gone)(void *arg);
    int (*send)(void *arg, uint32_t peer, int kind, const void *p, size_t n, uint64_t frame_id, uint64_t deadline_ns);
    int (*cancel)(void *arg, uint32_t peer, uint64_t frame_id);
    int (*credit)(void *arg, uint32_t peer, uint64_t frame_id, uint32_t chunks);
    /* reply bytes for the client, in order; false: its connection is going away */
    bool (*relay)(void *arg, const uint8_t *p, size_t n);
};

struct fwdr_req {
    uint32_t peer;
    int kind;           /* PL_KIND_FWD (a write: one reply) or PL_KIND_FWDQ (a read: chunks under credit) */
    uint8_t *msg;       /* envelope + frame; age_ms is stamped at FWDR_AGE_AT before each attempt */
    size_t n;
    uint64_t frame_id;
    int64_t arrival_ns; /* when the event loop read the frame */
    uint32_t max_age_ms, wait_ms, reply_ms; /* fwd_max_age, fwd_wait_ms, fwd_reply_ms (0: no reply deadline) */
    /* planted bugs (tests): age not checked, a lost frame sent again, a broken link answered as UNREACH */
    bool mut_noage, mut_retry, mut_unavonbreak;
};

enum {
    FWDR_DONE = 0,    /* the owner's reply was relayed (tail in *tail) */
    FWDR_OLD = 1,     /* older than max_age before the send: never sent (UNAVAILABLE, definite) */
    FWDR_GONE = 2,    /* the client went before the send: never sent, no answer */
    FWDR_UNREACH = 3, /* not written by wait_ms: never reached the owner (UNAVAILABLE UNREACHABLE, definite) */
    FWDR_TOOBIG = 4,  /* above the link's frame size (MOVED) */
    FWDR_INDET = 5,   /* written, then lost, timed out, or the client went mid-reply: close without an answer */
};
int fwdr_run(const struct fwdr_ops *o, struct fwdr_tab *t, const struct fwdr_req *q, struct fwdr_tail *tail,
             uint64_t *hop_ns);

#endif
