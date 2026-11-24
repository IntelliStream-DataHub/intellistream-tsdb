/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The replication driver's internals (server/repld.c), shared with the files that R4's tracks fill: rsnap.c (snapshot
 * requests, transfer and install), rset.c (replica set changes), rtransfer.c (planned leader transfer). Moved out of
 * repld.c unchanged: struct rvol and its parts, the global G, the inbox kinds and the few helpers those files call.
 * Everything here keeps repld.c's rules: rv->mu below buf_lock (jr_ops.admit takes it), never held across I/O by
 * anything but the paths repld.c documents; G.mu a leaf around the volume and link tables.
 */
#ifndef TSDB_REPLD_INT_H
#define TSDB_REPLD_INT_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "cluster.h"
#include "replwire.h"
#include "../src/internal.h"
#include "../src/repl.h"
#include "../src/replfile.h"

struct rin;
struct rsnap; /* server/rsnap.c (r4-xferlink, r4-xferinst): a snapshot asked for, sent or received */
struct rset;  /* server/rset.c (r4-setchg, r4-move): a replica set change in progress */
struct rtr;   /* server/rtransfer.c (r4-transfer): a planned leader transfer in progress */

#define TICK_MS 5
#define COMMIT_STALL_MS 5000 /* the writer says what holds its commit point once it has not moved for this long */
#define SPILL_BYTES (16u << 20) /* a bulk file above this is received into a file, not memory (r3d review M3) */
#define INBOX_BYTES (64u << 20) /* a volume's inbox of peer frames: past it the link waits, then drops (M3) */
#define INBOX_WAIT_MS 500       /* how long a link's thread waits for room (well below a link's dead_ms) */
#define IDLE_HEARTBEAT_MS 100 /* an idle volume's empty APPEND per peer (r3d review L1: was every tick) */
_Static_assert(META_ISR_LAG_MIN_MS >= 5 * IDLE_HEARTBEAT_MS, "a live idle member must be heard well within isr_lag_ms");
#define NOW_EVERY_MS 1000 /* a leader's NOW markers (design-cluster.md 5.4): its replicas' retention clock lags by at most this */
#define RESEND_MS 50
#define LOST_MS 500 /* a peer's acknowledged end standing still this long with the cursor ahead: what was sent is lost */
#define FETCH_LATE_MS 5000 /* fetchlate (tests) */
#define SYNC_PAUSE_MS 4000 /* syncpause (tests) */
#define READ_CAP (4u << 20)
#define SLOW_READ_MS 10  /* slowread (tests): longer than a tick */
#define SLOW_TICK_MS 20  /* slowtick (tests): a catch-up step that long, at every tick it runs */
#define LATE_TICK_MS 6   /* latetick (tests): every tick of a writer that much longer: the thread is never current */
#define TICK_BACKLOG_MS 1000 /* the core's clock catches up at most this much of wall time (vol_main) */
#define STALL_MS 250 /* a tick wait that returns this late: the process was stopped (SIGSTOP, a VM pause), not busy */
#define SLOW_ACK_MS 100  /* slowack (tests) */
#define SLOW_ACK_RING 512
#define MAX_NODES 64

enum { M_REPLICA = 0, M_PROMOTING, M_WRITER, M_DEAD };
/* the inbox's kinds: IN_MSG and IN_FETCH are peer frames (bounded by inbox_bytes), the rest local. IN_SNAP: a snapshot's
 * control frame or bulk event, posted by rsnap.c from the kind-5 link's callbacks and handled by rsnap_in on the
 * volume's thread; IN_CMD: a VOLCMD of the Raft leader (struct cluster_volcmd, cluster_set_volcmd_handler) */
enum { IN_MSG = 1, IN_FETCH, IN_META, IN_WAKE, IN_FENCE, IN_SNAP, IN_CMD };

struct item {
    struct item *next;
    int kind;
    uint32_t node;
    size_t n;
    uint8_t data[];
};

struct rpeer {
    uint64_t sent;     /* the last position whose records went out to it (a WAL record end) */
    uint64_t want;     /* the core wants it to have up to here */
    uint64_t sent_ms;  /* when `sent` last moved forward */
    uint64_t acked;    /* the `durable` of its last ACK in this run: a replica's durable end stops below a COMMIT
                        * marker whose file it has not staged, so the segment that holds the marker, and with it the
                        * link the file is fetched from, stays (its `written` passes the marker: replicate) */
    uint64_t gen;      /* the log link's connection the cursor was moved on */
    uint64_t wrote, wrote_ms; /* the `written` of its last ACK, and when that last changed */
    uint64_t ahead_ms; /* since when `sent` is past `wrote` (0: it is not): what was sent since has not arrived */
    int heard;
    uint64_t heard_ms; /* when a message of this volume last came from it (repld_unreachable) */
    int rderr;         /* the last error of a catch-up read for it, and when it was said (never silent, not a flood) */
    uint64_t rderr_ms;
};

struct rvol {
    uint32_t number;
    char dir[4200];
    pthread_mutex_t mu; /* the core, the index, the cursors, the inbox; below buf_lock (jr_ops.admit takes it) */
    pthread_cond_t cv;
    pthread_t th;
    int started, stop, mode, failed, set_changed;
    struct repl *core;
    struct replfile *rf;
    struct replfile_state st;
    struct cluster_vol_rec rec;
    int self, nnodes;
    uint64_t epoch; /* the core's, cached for the bytes path */
    struct rpeer peer[REPL_MAX_NODES];
    uint64_t open_ms; /* when its driver started: a peer never heard counts as unheard since then */
    uint64_t resumed_ms; /* when its thread came back from a stall (STALL_MS): peers count as unheard since then */
    uint64_t loop_ms;    /* when its thread last woke or looped (repld_unreachable) */
    struct item *in_head, *in_tail;
    size_t in_bytes;         /* of peer frames in the inbox (bounded: inbox_bytes) */
    pthread_cond_t in_space; /* a link's thread waits here for room */
    int in_waiters, in_full; /* in_full: the bound was hit and said, until the inbox empties */
    /* the index: starts of the WAL records from the log start, ascending; idx_end the last position indexed */
    uint64_t *idx;
    size_t nidx, cidx;
    uint64_t idx_end, log_start;
    uint64_t trace_ms;
    struct repl_epoch hist[REPLFILE_MAX_HIST];
    uint32_t nhist;
    /* the message being handled */
    struct rw_msg cur;
    struct repl_rec *recs;
    uint32_t *lens;
    uint32_t crec;
    uint64_t *coff; /* byte offset of each record of cur */
    int sync_wanted;
    uint64_t synced_rep; /* the writer: the last durable end handed to repl_synced */
    int rec_short;       /* isr_short_locked of `rec`, counted in G.nshort (rec_set) */
    int sync_said;       /* noclock's line, once */
    uint64_t sync_late;  /* synclate: a report op_synced held back for the next tick (0: none) */
    unsigned sync_n;     /* synclate: the reports op_synced has had */
    uint64_t cstall_at, cstall_ms; /* the writer: the commit point and since when it has not moved with writes above it */
    int cstall_said;
    uint64_t epoch_pos;  /* M_PROMOTING: the EPOCH marker's position */
    uint64_t fetch_ms;
    char fetch_name[RW_NAME_MAX + 1];
    uint64_t fetch_late_ms;                 /* fetchlate (tests): when fetch_late_name was found missing */
    uint64_t sync_pause_end;                /* syncpause (tests): a replica's syncs wait until then */
    uint64_t cu_next;                       /* a late tick runs a catch-up step only from then on (vol_main) */
    int sync_paused;                        /* syncpause: a SYNC waits for sync_pause_end */
    char fetch_late_name[RW_NAME_MAX + 1];
    struct jr_ops ops;
    /* the ISR change the core asked the metadata group for (REPL_M_ISR_REQ), reported in this server's heartbeats
     * (repld_isr_requests) while the record still is the request's basis */
    int isr_want;
    struct repl_vmeta isr_req;
    uint64_t resigned; /* the epoch a RESIGN was said for */
    uint64_t nofence_said; /* tests (nofencereply): the epoch whose withheld fence answer was said */
    uint32_t fence_from; /* the Raft leader whose FENCE the core is answering (its reply goes there) */
    /* min_isr after the append: a write admitted while the ISR was large enough, whose commit point is reached only
     * after a shrink below min_isr, is not acknowledged (the core's REPLY waits the same way, but the writer's waiters
     * go by APPLY): the commit point is held here and handed to the engine once the ISR is back at min_isr */
    uint64_t held_commit;
    int short_said;
    int snap_said; /* the SNAPSHOT notice was said */
    /* slowack (tests): the durable ends of its last ACKs and when they were sent; the one it reported last */
    uint64_t sa_ms[SLOW_ACK_RING], sa_dur[SLOW_ACK_RING], sa_last;
    unsigned sa_n;
    /* R4's state, each owned by its file (NULL: nothing in progress); under mu like the rest */
    struct rsnap *snap; /* rsnap.c */
    struct rset *set;   /* rset.c */
    struct rtr *tr;     /* rtransfer.c */
    int said_cmd_drop, said_cmd_tr; /* the stubs' lines, once per volume (said_cmd_tr: a bit per VOLCMD kind) */
};

enum {
    RM_NONE,
    RM_COMMITBYLEADER,
    RM_ACKBEFORESYNC,
    RM_FLOORNOTSAVED,
    RM_NODEPOSE,
    RM_NOQUIESCE,
    RM_NOFASTPATH,
    RM_FLOORNOTTOLD,
    RM_HBNOCLAMP,
    RM_UNSUREIGNORED,
    RM_ADMITNOCHECK,
    RM_NOCURSORUP,
    RM_COMMITPASTREMOVED,
    RM_NOMINISR,
    RM_ACKBELOWMINISR,
    RM_SHIPPERFILE,
    RM_NOCAP,
    RM_NOSEAL,
    RM_NOSPILL,
    RM_NOINBOXBOUND,
    RM_KEEPIGNORESPEERS,
    RM_NOLOGSTARTCHECK,
    RM_STOPNODEPOSE,
    RM_LATECLOCK,
    RM_SYNCBYREP,
    RM_INBOXWAITALL,
    RM_SHIPNOISR,
    RM_KEEPWRITTEN,
    RM_FASTRIDE,
    RM_NOJOINHOLD,
    RM_KEEPUNANSWERED, /* unanswered_ticks 0: a member a promotion kept unanswered goes only by the lag rule */
    RM_FENCENOSYNC,    /* a SAVE that raises the fence skipped: the FENCE_REPLY goes out, the fence not in REPL */
    RM_INBOXSTARVE,
    RM_OPENNOHOLD,
    RM_PROMOTENONE, /* a driver promoting reports role NONE: a slow promotion is taken for deposed (F3) */
    RM_TICKSTRETCH,
    RM_STEPCURRENTONLY,
    RM_STALLCATCHUP,
    RM_DEMOTESTALEIDX, /* the index rebuilt over what a deposed writer's engine pushed after demote cleared it */
    RM_DIVERGEISR,     /* a DIVERGED volume is not stopped: no LOCAL_FAULT, it stays in the ISR (r8i, D8) */
    RM_SNAPNOVERIFY,    /* rsnap.c: a received file is not compared with SNAPMAN's xxh64 */
    RM_SNAPSAMELINK,    /* rsnap.c: a snapshot's files go on kind 2, the COMMITs' link (D7) */
    RM_SNAPNOSPACECHECK, /* rsnap.c: the receiver takes a snapshot whatever its free space (D5) */
    RM_SNAPSRCPROMOTING, /* rsnap.c: a source serves a request while M_PROMOTING (D2) */
    RM_SNAPNODONEBITS,   /* rsnap.c: no per-file done bits: a file begun again after a lost DONE is counted twice */
    RM_SNAPSHAREDMAX,    /* rsnap.c: rebuild_max counts sources and receivers together */
    RM_SNAPMAKEDOWN,     /* rsnap.c: the source's link-down timer runs during the make */
    RM_SNAPNOWAITDONE,   /* rsnap.c: a source waits for the receiver's DONE 0 for up to 6 hours */
    RM_SNAPNOSTALE,      /* rsnap.c: a request of another set version or epoch is served */
    RM_SNAPNOSTALL,      /* rsnap.c: a receiver that hears nothing waits on */
    RM_SNAPNOSRCDOWN,    /* rsnap.c: a source whose receiver's link stays down is not released */
    RM_SNAPNOSRCMAX      /* rsnap.c: a source serves past rebuild_max */
};

struct repld_g {
    uint32_t isr_rot; /* (atomic) where repld_isr_requests starts its next report */
    int nshort; /* (atomic) the volumes whose record has an ISR below min_isr: 0, and repld_isr_short asks nothing more */
    pthread_mutex_t mu;
    tsdb *t;
    char dir[4096];
    struct peerlink *pl;
    uint32_t self;
    struct {
        uint32_t node;
        char addr[META_ADDR_MAX];
        struct plink *log, *bulk;
        struct plink *snap; /* kind 5 (PL_KIND_SNAP): dialed by rsnap.c when it needs one (NULL until then) */
    } nodes[MAX_NODES];
    uint32_t nn;
    struct rvol *vols[CLUSTER_REPORT_VOLS];
    uint32_t nvols;
    uint64_t fileno;
    int mutant; /* RM_*: tests only */
    uint64_t spill_bytes; /* a bulk file larger than this is received into a spill file (TSDB_REPLD_TEST=spill: 64 KiB) */
    int spill_said;
    size_t inbox_bytes; /* INBOX_BYTES (TSDB_REPLD_TEST=smallinbox: 256 KiB) */
    int cut_links;      /* TSDB_REPLD_TEST=cutlinks: a message of any volume from node N is dropped as it arrives while
                         * <dir>/cutfrom.N exists (test_cluster cut) */
    int stall_inbox;    /* TSDB_REPLD_TEST=stallinbox: no peer frame is handled while <dir>/stallinbox exists (a
                           stalled disk's stand-in), until repld_stop (test_cluster stallstop) */
    int drop_bulk; /* TSDB_REPLD_TEST=dropbulk (a fault, tests only): the first bulk file received is not staged */
    int promote_wait; /* TSDB_REPLD_TEST=promotewait (tests only): 1 s between the promotion's swap and M_WRITER */
    int promote_slow; /* TSDB_REPLD_TEST=promoteslow (tests only): 5 s before the promotion's swap, a slow reopen */
    int open_slow;    /* TSDB_REPLD_TEST=openslow (tests only): 3 s between the engine's open and the manager's MV_OPEN */
    int late_wal; /* TSDB_REPLD_TEST=latewal (tests only): repld_stop leaves its writers in place and waits 2.5 s after
                   * freeing the links, and a stopped driver admits records whatever its lease, so the engine's NOW
                   * markers reach the WAL hooks of a stopped driver (op_bytes says so once) */
    _Atomic int said_late; /* latewal's line, once */
    int late_bytes;   /* TSDB_REPLD_TEST=latebytes: the writer's first rows record written while <dir>/latebytes exists
                       * is held in op_bytes (written, not yet indexed or shipped) until a demote has begun (deposetail) */
    _Atomic int late_bytes_done;
    int sync_by_hook; /* TSDB_REPLD_TEST=syncbyhook: a writer's SYNC compares what op_synced reported, not the engine */
    int fetch_late;   /* TSDB_REPLD_TEST=fetchlate: a replica's first FETCH of a name waits FETCH_LATE_MS */
    int no_fetch;     /* TSDB_REPLD_TEST=nofetch: a replica sends no FETCH and says once per name that it withholds it */
    int snap_dial;    /* TSDB_REPLD_TEST=snapdial: the driver also dials every peer's kind 5 (PL_KIND_SNAP) and sends one
                       * frame when it comes up (test_cluster volcmd: the acceptor's row and rsnap_frame) */
    int sync_pause;   /* TSDB_REPLD_TEST=syncpause: a replica's SYNCs wait SYNC_PAUSE_MS after its core's open, so its
                       * ACKs carry the durable end the core opened with that long (reopenhold) */
    int sync_late;    /* TSDB_REPLD_TEST=synclate: every other report of op_synced reaches the core only at the next
                       * tick, so the report after it covers records written behind the SYNC in flight (writersync) */
    int no_clock; /* TSDB_REPLD_TEST=noclock (tests only): a leader writes no NOW markers and seals nothing idle, so
                   * only the clients' writes move its WAL (writersync), and says once when a SYNC finds the WAL
                   * durable past the core's durable end */
    int trace; /* TSDB_REPLD_TRACE (debugging): the core's and the cursors' state every 2 s */
    int stale_sent;   /* TSDB_REPLD_TEST=stalesent (tests only): once <dir>/stalesent exists, one peer's cursor goes to 0 */
    int skip_pos;     /* TSDB_REPLD_TEST=skippos (tests only): once <dir>/skippos exists, one admit takes a position more */
    int plant_diverge; /* TSDB_REPLD_TEST=plantdiverge (tests only): <dir>/diverge.<volume> makes that volume DIVERGED
                        * (ec_diverge, EC_DIV_TEST) at the driver's next tick, and is removed */
    int no_fence_reply; /* TSDB_REPLD_TEST=nofencereply (tests only): no FENCE_REPLY leaves while <dir>/nofencereply
                         * exists (the fence is saved; a member that dies before its answer is out) */
    int slow_read;    /* TSDB_REPLD_TEST=slowread: every catch-up read takes SLOW_READ_MS more (a slow disk, NFS) */
    int slow_tick;    /* TSDB_REPLD_TEST=slowtick: a writer's catch-up step takes SLOW_TICK_MS at every tick it runs,
                       * lagging peer or not (tickwall) */
    int late_tick;    /* TSDB_REPLD_TEST=latetick: every tick of a writer takes LATE_TICK_MS more, so its ticks are always
                       * late (a volume thread that cannot keep up: TSan, a full inbox) (rejoinlate) */
    int slow_ack;     /* TSDB_REPLD_TEST=slowack: a replica's ACK reports the durable end it had SLOW_ACK_MS ago (a far
                       * replica: what it reports trails the leader's end even while it keeps up) */
    struct rin *ins; /* bulk files being received */
};

/* The names below are short in the driver's files (repld.c and the R4 files that include this header) and prefixed
 * in the binary: tsdbd links cluster.c, chunksrv_cluster.c, fwd_cluster.c and crashpt.c, each with a static `say` or
 * `G` of its own, and an external one in another file must never bind to the driver's. */
#define G rd_G
#define say rd_say
#define vol_find rd_vol_find
#define post rd_post
#define save_repl rd_save_repl
#define rec_set rd_rec_set

extern struct repld_g G;

/* one line on stderr, "tsdbd: replication: ..." */
void say(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
/* the driver of volume `number`, or NULL (G.mu taken inside) */
struct rvol *vol_find(uint32_t number);
/* queues an item of kind IN_* for the volume's thread (any thread, rv->mu not held); a peer kind past the inbox's bound
 * waits, then is dropped (the protocol sends again) */
void post(struct rvol *rv, int kind, uint32_t node, const void *p, size_t n);
/* REPL saved with the core's fence, led and floor, the driver's log start and epoch history (rv->mu held; let go of
 * for the write) */
int save_repl(struct rvol *rv, uint64_t fence, uint64_t led, uint64_t floor);
/* rv->rec = *r with G.nshort kept (rv->mu held, or the volume not yet listed) */
void rec_set(struct rvol *rv, const struct cluster_vol_rec *r);

#endif
