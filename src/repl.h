/* SPDX-License-Identifier: AGPL-3.0-only */
/* The replication core of a cluster volume (docs/design-cluster.md §4-§6 and §8.2, decision C23).
 *
 * Sans-IO, like the Raft core: no sockets, no files, no threads, no clock. One `struct repl` is one replica of one
 * volume. Events go in (a record to append, a finished sync, a message from a peer or from the metadata group, a
 * timer tick); actions come out of repl_next_action() in the order they must be carried out (write, sync, cut, save
 * the REPL state, send, apply, reply). The driver (tsdbd's peer loop in build step 4; tests/sim_repl.c here) carries
 * each action out before it feeds the next event. Record payloads (WAL bytes, marker contents) never pass through
 * the core: it names records by position, and the driver moves their bytes.
 *
 * Positions. Every record has a position `pos` (the feed number of design-cdc.md) and covers `n` consecutive
 * positions (a rows record one per row, a marker one). The log of a replica is records 1..written, contiguous. Each
 * record also carries the epoch in which its leader created it; the epoch history (epoch -> first position) is
 * derived from the log (segment names in tsdb), so a cut and a restart keep it right by construction.
 *
 * The rules (design-cluster.md):
 * - Durable first, applied at the commit point (§4.2): a record is written and synced as it arrives; the leader's
 *   commit point is the lowest durable end over the in-sync set (ISR, leader included); every replica applies
 *   (makes visible, flushes, lists parts) only up to the commit point it has heard, its own durable end, and the
 *   end of the prefix it has verified against the current leader. The commit floor in REPL is the applied end.
 * - While an ISR expansion is in flight, the leader counts the joining replica too ("maximal ISR"): otherwise the
 *   metadata group could record a member that lacks a position the leader committed without it (a gap in §8.1).
 * - Epochs (§6.2, §6.3): a replica that hears of a higher epoch saves it as its fence (SAVE, durable) before it acts
 *   on it, and refuses records of a lower epoch from then on. A leader that hears of a higher epoch steps down and
 *   answers its waiting writers REPL_MOVED (indeterminate). A replica never leads an epoch it already led before a
 *   restart (`led` in REPL): its sends of that epoch may have outlived its own unsynced log, so a second incarnation
 *   would write other records at the same (position, epoch). It resigns instead, and the metadata group promotes
 *   again with a new epoch.
 * - The divergent tail (§8.2, KIP-101 and KIP-279): a replica that follows a new leader sends its last epoch; the
 *   leader answers with the largest epoch of its own history not above it and that epoch's last position; the
 *   replica cuts after min(that, the end of the same epoch in its own log), and asks again until nothing is cut.
 *   Appends carry the epoch of the position before them, which a follower checks (a second line of defence).
 *   A cut below the applied end is never made: REPL_A_FAIL (damage or a bug, never silently).
 * - Replica instances: a fresh replica directory (new or replaced disk) gets a new `rid`. The ISR in the metadata
 *   names the rid of each member; a FENCE reply and every ACK carries it, and only a member whose rid matches may
 *   be promoted (a wiped disk answering a fence must not become leader: a gap in §6.2 / §8.5).
 *
 * The metadata group's side of the rules (conditional ISR changes, promotion from fence replies) is here too, as
 * pure functions on `struct repl_vmeta`, so that the state machine of build step 1 and the simulator share them.
 */
#ifndef TSDB_REPL_H
#define TSDB_REPL_H

#include <stdint.h>

#define REPL_MAX_NODES 8          /* replicas of one volume: node indexes 0 .. nnodes-1 */
#define REPL_META      (-1)       /* message address of the metadata group */

/* repl_write and repl_meta_* results; REPLY statuses */
#define REPL_OK          0
#define REPL_E_NOTLEADER (-1)     /* not the leader of the current epoch (or not yet / no longer) */
#define REPL_E_ISR       (-2)     /* fewer in-sync replicas than min_isr: refused before anything is written */
#define REPL_E_FAILED    (-3)     /* the core failed (REPL_A_FAIL was emitted) */
#define REPL_E_STALE     (-4)     /* metadata: the request names an epoch or ISR version that is no longer current */
#define REPL_E_NONE      (-5)     /* metadata: no fence reply from an in-sync replica, nobody to promote */
#define REPL_MOVED       (-6)     /* REPLY: the leader stepped down; the write may or may not be committed */

struct repl_rec {                 /* a record descriptor; its bytes are the driver's */
    uint64_t pos;                 /* first position */
    uint64_t epoch;               /* the epoch its leader wrote it in */
    uint32_t n;                   /* positions it covers, >= 1 */
};

struct repl_epoch {               /* one entry of the epoch history */
    uint64_t epoch;
    uint64_t start;               /* its first position (its EPOCH marker) */
};

/* The metadata group's record of a volume (§2.1), as far as replication needs it. */
struct repl_vmeta {
    uint64_t epoch;               /* raised by every promotion */
    uint64_t isr_version;         /* raised by every ISR change and promotion */
    int leader;
    uint32_t isr;                 /* bitmask of node indexes */
    uint64_t rid[REPL_MAX_NODES]; /* the replica instance of each ISR member */
    uint32_t unanswered;          /* ISR members the last promotion kept without a fence reply (the old leader
                                     among them): the new leader drops those that do not ACK within its
                                     unanswered_ticks, so a dead one does not hold the commit point for isr_lag */
};

enum repl_msg_type {
    REPL_M_APPEND = 1,            /* leader -> replica: records after prev_pos, and the commit point */
    REPL_M_ACK,                   /* replica -> leader: durable and written ends of the verified prefix */
    REPL_M_REFUSE,                /* replica -> sender of an older epoch: `epoch` (and `leader` if known) */
    REPL_M_QUERY,                 /* replica -> leader: my last epoch (last_epoch) and log end (written); end = a
                                     query number */
    REPL_M_ANSWER,                /* leader -> replica: last_epoch = its largest epoch <= the asked one, written = the
                                     last position of that epoch in its log; prev_epoch, prev_pos, end: the query's
                                     last_epoch, written and number, echoed */
    REPL_M_FENCE,                 /* metadata -> replica: refuse every epoch below `epoch` */
    REPL_M_FENCE_REPLY,           /* replica -> metadata: durable, last_epoch, rid */
    REPL_M_META,                  /* metadata -> replica: the volume's record (meta) */
    REPL_M_ISR_REQ,               /* leader -> metadata: meta = the requested record (epoch, isr_version: the basis) */
    REPL_M_RESIGN,                /* replica -> metadata: I cannot lead `epoch` (I led it before a restart) */
    REPL_M_SNAPSHOT,              /* leader -> replica: my log no longer has position `written` + 1 that you need
                                     (it went into parts, §8.3): install a snapshot from me (§8.1 step 3) */
};

struct repl_msg {
    int type;
    int from, to;                 /* node indexes, or REPL_META */
    uint64_t epoch;               /* the sender's epoch (the fence epoch for FENCE and FENCE_REPLY) */
    int leader;                   /* REFUSE: the leader of `epoch` if known, else -1 */
    /* APPEND */
    uint64_t prev_pos, prev_epoch, commit;
    uint32_t nrec;
    const struct repl_rec *rec;   /* nrec descriptors, contiguous from prev_pos + 1 */
    /* ACK, FENCE_REPLY, QUERY, ANSWER */
    uint64_t durable, written, rid, last_epoch, end;
    int resync;                   /* ACK: the leader should send from written + 1 */
    struct repl_vmeta meta;       /* META, ISR_REQ */
};

enum repl_action_type {
    REPL_A_WRITE = 1,             /* append rec to the log (src: where its bytes come from) */
    REPL_A_SYNC,                  /* make everything written so far durable, then call repl_synced() */
    REPL_A_TRUNCATE,              /* cut the log after position `pos` (a record end) and make the cut durable */
    REPL_A_SAVE,                  /* write the REPL state (fence, led, floor) durably before anything that follows */
    REPL_A_SEND,                  /* send msg; for an APPEND the driver attaches the records in prev_pos+1 .. upto */
    REPL_A_APPLY,                 /* apply every record up to `pos` (a record end) */
    REPL_A_REPLY,                 /* answer the writer of positions first .. pos with status */
    REPL_A_FAIL,                  /* the core refuses to go on (reason); treat as fail-stop */
    REPL_A_SNAPSHOT,              /* the leader (`pos`: its node index) no longer has the records this replica needs:
                                     copy its applied state P (parts) and reopen with log_start P + 1, log_end and
                                     floor P; until then the replica goes on as it is */
};

enum repl_src {
    REPL_SRC_CLIENT = 1,          /* the record of the repl_write() call that produced this action */
    REPL_SRC_EPOCH,               /* an EPOCH marker of rec.epoch */
    REPL_SRC_MSG,                 /* record `idx` of the APPEND being handled by repl_recv() */
};

struct repl_action {
    int type;
    struct repl_rec rec;          /* WRITE */
    int src;                      /* WRITE */
    uint32_t idx;                 /* WRITE with REPL_SRC_MSG */
    uint64_t pos;                 /* TRUNCATE, APPLY, REPLY (last position) */
    uint64_t first;               /* REPLY */
    int status;                   /* REPLY: REPL_OK or REPL_MOVED */
    uint64_t upto;                /* SEND of an APPEND */
    uint64_t fence, led, floor;   /* SAVE */
    const char *reason;           /* FAIL */
    struct repl_msg msg;          /* SEND */
};

struct repl_config {
    int self;                     /* this replica's node index */
    int nnodes;                   /* replicas of the volume */
    int min_isr;                  /* acknowledge only while the ISR has at least this many members (1, D1) */
    uint32_t isr_lag_ticks;       /* an ISR member that has not made durable what the leader had written this many
                                     ticks ago is removed (isr_lag_ms, §6.1) */
    uint32_t resend_ticks;        /* resend unanswered appends, queries, ISR requests */
    uint32_t heartbeat_ticks;     /* an empty APPEND with the commit point at least this often (§4.2: 5 ms) */
    uint32_t floor_ticks;         /* save the commit floor at most this often (§4.2: once a second) */
    uint32_t max_batch;           /* positions per APPEND */
    uint32_t window;              /* positions in flight per replica */
    uint32_t unanswered_ticks;    /* a member a promotion kept without a fence reply (meta.unanswered) is removed if
                                     it has not acknowledged this many ticks after the new leader took over; 0: only
                                     the lag rule (a few round trips is right: a live member answers by then) */
    /* The record of this replica's log that contains `pos` (1 <= pos <= written end): its first position and its
     * length. A read of the driver's own index (the WAL), never a write. Returns 0; 1 if the position is no longer
     * retained (below the log's start: its segment went into parts and was deleted, §8.3; the driver trims only
     * positions at or below the saved commit floor); -1 if there is no such record (damage: the core fails). */
    int (*rec_find)(void *ctx, uint64_t pos, uint64_t *start, uint32_t *n);
    void *ctx;
};

/* What the driver read at open: the REPL file and a scan of the durable log. A fresh replica: all zero but rid. */
struct repl_durable {
    uint64_t rid;                 /* replica instance, new for every fresh directory, never 0 */
    uint64_t fence;               /* the highest epoch this replica has saved */
    uint64_t led;                 /* the highest epoch it has led */
    uint64_t floor;               /* the commit floor: applied end */
    uint64_t log_start;           /* first position the log still has (0 or 1: all of it); everything below is
                                     applied (in parts): log_start - 1 <= floor. A replica opened from a snapshot at P
                                     has log_start P + 1, log_end P, floor P, and the epoch of P in hist */
    uint64_t log_end;             /* last position of the durable log (0: empty) */
    const struct repl_epoch *hist;
    uint32_t nhist;
};

struct repl;

struct repl *repl_open(const struct repl_config *cfg, const struct repl_durable *d);
void repl_free(struct repl *r);

/* Leader: append a record of n positions written by a client; *pos gets its first position. REPL_OK, or
 * REPL_E_NOTLEADER / REPL_E_ISR / REPL_E_FAILED with nothing written. The REPLY for it comes later. */
int repl_write(struct repl *r, uint32_t n, uint64_t *pos);
/* The last SYNC finished: every position up to `end` is durable. The core takes no more than the written end the
 * SYNC was issued for, lowered by every cut since: a sync issued before a cut does not cover records written again
 * after it (a driver that reports the end it asked for is right). */
void repl_synced(struct repl *r, uint64_t end);
void repl_recv(struct repl *r, const struct repl_msg *m);
void repl_tick(struct repl *r);
/* The next action, 1; or 0 if there is none. */
int repl_next_action(struct repl *r, struct repl_action *a);

/* Introspection, for drivers and checkers. */
int repl_is_leader(const struct repl *r);
uint64_t repl_epoch(const struct repl *r);
uint64_t repl_written(const struct repl *r);
uint64_t repl_durable_end(const struct repl *r);
uint64_t repl_applied(const struct repl *r);
uint64_t repl_commit(const struct repl *r);
int repl_failed(const struct repl *r);

/* The metadata group's rules. An ISR change applies only if it names the current epoch, ISR version and leader, and
 * keeps the leader (C11). A promotion applies only if the record still is the one the fences were sent for
 * (basis_epoch, basis_version; `fenced` the ISR they went to), and picks among the replies of ISR members whose rid
 * matches the one with the highest durable end (ties: the lowest node index); the new ISR is those replies and the
 * fenced members that did not answer, the old leader included (`unanswered`: each holds every committed position;
 * the new leader drops the dead ones), the epoch basis_epoch + 1. Without the version check, an ISR change that the
 * deposed leader got committed after the fences went out (dropping a fenced member, then committing without it)
 * would be undone by the promotion. */
struct repl_fence_reply {
    int node;
    uint64_t durable, last_epoch, rid;
};
int repl_meta_isr_change(struct repl_vmeta *m, const struct repl_vmeta *req);
int repl_meta_promote(struct repl_vmeta *m, uint64_t basis_epoch, uint64_t basis_version, uint32_t fenced,
                      const struct repl_fence_reply *r, int n);

#ifdef REPL_MUTANTS
/* tests/sim_repl.c's mutation runs: bit n plants bug n of repl.c (MUT(n)); 0: none. */
extern uint64_t repl_mutants;
#endif

#endif
