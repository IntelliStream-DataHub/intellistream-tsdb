/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The verbs transport under peerlink (docs/design-rdma.md §4, D5, D7-D9; track rd-verbs): a `struct pl_transport`
 * (server/pltransport.h) over one RC queue pair per link connection, SEND/RECV only, no remote key ever handed out.
 *
 * Layers. plrdma.c knows nothing of libibverbs: it calls the verbs it needs through `struct plr_ops`, a table of a
 * dozen operations in its own small types, so that the transport compiles and is tested everywhere (tests/plr_fake.h
 * is an in-memory fabric behind the same table, tests/test_plrdma.c runs peerlink over it), and so that the real table
 * (server/plrdma_dl.c, built only where rdma-core's headers are, TSDB_RDMA) loads libibverbs with dlopen at its first
 * use: tsdbd links no RDMA library and runs on hosts without rdma-core.
 *
 * The pool (D7): one per tsdbd and device, shared by its peerlinks and their threads: one protection domain, one
 * region of `pool_bytes` registered once at start (registration on the data path costs tens of µs and fails
 * unpredictably under a memlock limit, which the open checks first and names), cut into slots of one size. A
 * connection takes all its slots at creation and gives them back only after its queue pair was drained and destroyed
 * (below), so a connection that exists can always send and receive, and the pool's size bounds the links that upgrade.
 *
 * One slot size, per-connection receive rings (the choice §4 left to this track). A receive queue, or a shared one,
 * hands each incoming SEND the next posted buffer whatever its size: one queue pair cannot have receives of two sizes
 * that a sender chooses between (that needs XRC), so the 4 KiB / 64 KiB split of §4 is not possible with one QP per
 * connection, and every receive slot is the size of the largest message. Per-connection rings then cost what an SRQ
 * per peerlink would (16 slots of 32 KiB, half a MiB per direction, 56 links in 56 MiB at the default 64 MiB pool) and
 * make the credit rule exact: the credits a side holds are slots the peer posted for this queue pair alone.
 *
 * Messages. Every SEND carries an 8-byte header and up to slot - 8 bytes of the link's byte stream: frames are split
 * over slots and joined again in order (an RC queue pair delivers in order or fails). Header: type (DATA 1, CREDIT 2),
 * the control and data credits it returns, the payload length. The bytes of a slot are handed to the link thread as
 * it reads them; a data slot is posted again once its last byte was read, and the credit goes back to the peer in the
 * header of the next message.
 *
 * Credits (§4 "Flow control"). A SEND that finds no posted receive is an error here, never a stall: the tests run
 * with rnr_retry 0, and the fake fails the queue pair at once, as iWARP does. So a side sends only while it holds a
 * credit. Data credits: one per data slot of the peer's ring; every DATA message takes one; a reader that does not read
 * (plink_pause) posts nothing again and so returns none, which stops the sender as a closed TCP window does (held back:
 * slow, not silent). Control credits: K slots more in each ring, for CREDIT messages only, which carry no stream bytes
 * and are posted again the moment they arrive, also on a paused link: a side that owes a quarter of the peer's data
 * window sends a CREDIT (on a control credit); one that owes only control credits sends one only when it owes K/2, so
 * that an exchange of credits dies out instead of answering itself. Peerlink's PING and PONG are stream bytes like
 * any frame (the transport does not look into the stream): a paused side keeps sending its PINGs on data credits that
 * the reading side returns by CREDIT, and the reading side's PONGs wait in its write buffer as they would behind a shut
 * TCP window; the paused side never counts its peer dead (peerlink.h), and its peer hears its PINGs.
 *
 * Written (D5). Bytes that writev took were posted: WRITTEN from that moment, whatever completion comes back later. A
 * flushed completion is no statement about what the peer received and changes nothing.
 *
 * Completions in the link's poll. One completion channel and one CQ per connection; poll_fds drains the CQ, arms it,
 * drains it again (an entry that came between the two raises no event) and returns the channel's descriptor and the
 * anchor's, the TCP connection of the handshake, kept in the poll set so that its end is the end of the connection
 * (design-rdma.md §5.1 step 8). The anchor carries nothing once the link is on the queue pair: a byte on it is an error.
 *
 * Signalled sends. Every n-th send is signalled (and always the one that takes the last free send slot, so that a
 * completion always comes back to free them): a send slot goes back to the connection's free ring only at the next
 * signalled completion after it, never at its own post.
 *
 * Fencing and the drain (§4 "Fencing"). close moves the queue pair to the error state, posts a signalled marker on
 * each queue and waits until both markers' completions came back (every work request posted before them then has
 * completed, flushed or not: the ib_drain_qp pattern), destroys the queue pair and its CQ, and only then returns the
 * slots to the pool: nothing an old connection posted can land in memory a newer one uses. A drain that does not
 * finish within PLR_DRAIN_MS keeps the slots out of the pool for the life of the process (one line), never reused.
 *
 * Threads. A connection belongs to one link thread (pltransport.h); the pool's free list has its own mutex (a leaf).
 */
#ifndef TSDB_PLRDMA_H
#define TSDB_PLRDMA_H

#include <stddef.h>
#include <stdint.h>

#include "pltransport.h"

#define PLR_HDR 8          /* bytes of the header of every message */
#define PLR_EP_LEN 40      /* an encoded endpoint (plr_ep_encode) */
#define PLR_DRAIN_MS 5000  /* the most close waits for the drain's markers */

struct plr_dev; /* an open device and port with its protection domain (the ops' own) */
struct plr_qp;  /* one RC queue pair with its CQ and completion channel (the ops' own) */
struct plr_mr;  /* a registered region (the ops' own) */

enum { PLR_LL_IB = 1, PLR_LL_ETH = 2 }; /* a port's link layer */

/* what a device's port says of itself */
struct plr_port {
    uint16_t lid;
    uint8_t gid[16];
    uint8_t mtu; /* the active MTU as verbs number it: 1 = 256 ... 5 = 4096 */
    uint8_t link_layer;
    uint8_t port;
};

/* the peer's numbers for a queue pair's move to RTR and RTS */
struct plr_qp_attr {
    uint16_t lid;
    uint8_t gid[16];
    int use_gid; /* a global route (RoCE, another subnet); InfiniBand within one subnet addresses by LID */
    uint32_t qpn, rq_psn, sq_psn; /* the peer's queue pair; the first PSN of each direction */
    uint8_t mtu, sl, rnr_retry, retry_cnt, timeout;
};

enum { PLR_WC_SEND = 1, PLR_WC_RECV = 2 };
enum { PLR_WC_OK = 0, PLR_WC_FLUSH = 1, PLR_WC_ERR = 2 };
struct plr_wc {
    uint64_t wr_id;
    int op;     /* PLR_WC_SEND, PLR_WC_RECV */
    int status; /* PLR_WC_* */
    uint32_t byte_len;
    int vendor; /* the provider's status, for the log */
};

/* The verbs, as the transport uses them. Every call is made by one thread at a time per queue pair (the link
 * thread's), dev_open, reg and dereg by the pool's opener. -1 with errno, or NULL with errno, on failure. */
struct plr_ops {
    const char *name;
    /* dev NULL: the first active port whose link layer is InfiniBand (D10: RoCE and iWARP only when named) */
    int (*dev_open)(const char *dev, int port, struct plr_dev **d, struct plr_port *pi, char *err, size_t errlen);
    void (*dev_close)(struct plr_dev *d);
    struct plr_mr *(*reg)(struct plr_dev *d, void *p, size_t n, uint32_t *lkey);
    void (*dereg)(struct plr_mr *mr);
    /* a completion channel, a CQ of sq + rq + 2 entries, an RC queue pair in INIT with room for sq sends and rq
     * receives of one scatter entry each */
    struct plr_qp *(*qp_new)(struct plr_dev *d, int sq, int rq, uint32_t *qpn);
    int (*qp_rts)(struct plr_qp *q, const struct plr_qp_attr *a); /* INIT -> RTR -> RTS */
    int (*qp_err)(struct plr_qp *q);                               /* -> ERR: everything posted is flushed */
    void (*qp_free)(struct plr_qp *q);                             /* the queue pair, its CQ and channel */
    int (*fd)(struct plr_qp *q);                                   /* the completion channel's, non-blocking */
    int (*arm)(struct plr_qp *q);                                  /* an event for the next completion */
    int (*take_events)(struct plr_qp *q);                          /* events taken and acknowledged (0: none) */
    int (*poll)(struct plr_qp *q, struct plr_wc *wc, int n);       /* completions taken, 0 none */
    /* n 0 and p NULL: a marker with no scatter entry */
    int (*post_send)(struct plr_qp *q, uint64_t id, const void *p, uint32_t n, uint32_t lkey, int signaled);
    int (*post_recv)(struct plr_qp *q, uint64_t id, void *p, uint32_t n, uint32_t lkey);
};

struct plr_pool_cfg {
    const char *dev; /* "mlx4_0" or NULL (see dev_open) */
    int port;        /* 0: the device's first active port */
    size_t pool_bytes;
    uint32_t slot;        /* bytes of every slot (a multiple of 64, at least 1 KiB) */
    uint16_t data_slots;  /* receive slots of a connection for DATA: the peer's data credits */
    uint16_t ctl_slots;   /* receive slots of a connection for CREDIT: the peer's control credits (K) */
    uint16_t send_slots;  /* send slots of a connection */
    uint8_t sl, rnr_retry, retry_cnt, timeout; /* the queue pairs' (rnr_retry 0 in tests: no receive is an error) */
};
#define PLR_POOL_CFG_DEFAULT                                                                                           \
    {NULL, 0, (size_t)64 << 20, 32768, 12, 4, 16, 0, 3, 7, 14}

struct plr_pool;
/* Opens the device, checks RLIMIT_MEMLOCK against pool_bytes (a line naming both, ENOMEM), maps and registers the
 * region. NULL with a line in err on failure. */
struct plr_pool *plr_pool_open(const struct plr_ops *ops, const struct plr_pool_cfg *cfg, char *err, size_t errlen);
/* Every connection of the pool must be closed. */
void plr_pool_close(struct plr_pool *p);
struct plr_pool_stats {
    uint32_t slots, free, lost; /* lost: kept out for good after a drain that did not finish */
    uint32_t conns;
};
void plr_pool_stats(struct plr_pool *p, struct plr_pool_stats *st);
const struct plr_port *plr_pool_port(const struct plr_pool *p);
/* tests: called with each slot the pool takes back, before it is free again */
void plr_pool_test_on_free(struct plr_pool *p, void (*fn)(void *arg, const void *slot, size_t n), void *arg);

/* What one side tells the other to connect a queue pair (the payload of rd-upgrade's OFFER and ACCEPT). */
struct plr_ep {
    uint16_t lid;
    uint8_t gid[16];
    uint32_t qpn, psn;
    uint8_t mtu, link_layer, port;
    uint32_t slot;       /* its receive slots' size: the most one of my messages may carry */
    uint16_t data, ctl;  /* its receive slots: my credits */
};
void plr_ep_encode(const struct plr_ep *e, uint8_t out[PLR_EP_LEN]);
/* -1 (EPROTO) for numbers no peer of this code sends (a slot below 1 KiB, no credits, a QPN above 2^24) */
int plr_ep_decode(struct plr_ep *e, const uint8_t *in, size_t n);

struct plr_conn;
/* A queue pair in INIT with every receive posted, its slots taken from the pool. NULL with a line in err. */
struct plr_conn *plr_conn_new(struct plr_pool *p, char *err, size_t errlen);
void plr_conn_ep(const struct plr_conn *c, struct plr_ep *out);
/* RTR and RTS with the peer's numbers. -1 with a line. */
int plr_conn_start(struct plr_conn *c, const struct plr_ep *peer, char *err, size_t errlen);
/* The transport of a started connection; it takes over `anchor` (polled for its end, closed with it) and the
 * connection (closed by the transport's close). */
struct pl_transport *plr_conn_transport(struct plr_conn *c, struct pl_transport *anchor);
/* A connection that never became a transport: drained and freed (its slots back to the pool). */
void plr_conn_free(struct plr_conn *c);
/* tests and stats: what the connection did */
struct plr_conn_stats {
    uint64_t msgs_out, msgs_in, credit_out, credit_in, bytes_out, bytes_in;
    uint64_t stalls; /* writev found no data credit */
};
void plr_conn_stats(const struct pl_transport *t, struct plr_conn_stats *st);

/* tests/test_plrdma.c: planted bugs (docs/design-rdma.md §8) */
enum {
    PLR_MUT_NONE,
    PLR_MUT_WRITTENCQE, /* bytes reported taken only once their send completed (WRITTEN on completion) */
    PLR_MUT_NOCTL,      /* a CREDIT takes a data credit: no credits reserved for control */
    PLR_MUT_CTLSTORM,   /* a CREDIT sent for a single owed control credit: credits answer themselves */
    PLR_MUT_PAUSECTL,   /* a CREDIT's slot posted again only when the link reads: a pause withholds control credits */
    PLR_MUT_NODRAIN,    /* slots back to the pool before the drain's completions (and the queue pair destroyed) */
    PLR_MUT_UNSIGEARLY, /* an unsignalled send's slot reused at once */
    PLR_MUT_NOCREDIT,   /* DATA sent without a data credit */
    PLR_MUT_SPLITOFF,   /* a frame split over slots loses a byte at the slot's end */
};
extern int plrdma_mutant;

/* server/plrdma_dl.c: libibverbs through dlopen (NULL with a line where it is not built or not installed) */
const struct plr_ops *plrdma_verbs(char *err, size_t errlen);

#endif
