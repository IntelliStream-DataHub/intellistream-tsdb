/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's cluster thread (docs/design-cluster.md §2, step 1 of §14): the metadata group's Raft core, the watch stream
 * of the servers that are not in the group, heartbeats, and the admin ops of `tsdb cluster`. See cluster.c.
 */
#ifndef TSDBD_CLUSTER_H
#define TSDBD_CLUSTER_H

#include <stddef.h>
#include <stdint.h>

#include "meta.h"

#define CLUSTER_SECRET_MIN 16
#define CLUSTER_SECRET_MAX 1024

struct cluster_cfg {
    const char *state_dir;      /* tsdbd's --state-dir: the cluster's files (NODE, IDENT, raft/, snapshot, copy,
                                 * retired). A local file system, never --dir (NFS in shared mode); made when
                                 * missing (its parent must exist) */
    int peer_fd;                /* bound and listening; cluster_start takes it over */
    const char *advertise;      /* the client address others reach this server at, host:port */
    const char *peer_advertise; /* the peer address, host:port */
    const char *rack;           /* "" when not given */
    const char *power_feed;     /* "" when not given (--power-feed) */
    const char *bmc;            /* "" when not given (--bmc) */
    int witness;                /* --witness: no volumes (volumes_ok 0), a voter candidate only */
    int read_barrier_off;       /* --read-barrier off: this node runs without it; its heartbeat says so */
    const uint8_t *secret;      /* the cluster secret (every member and the CLI hold the same bytes) */
    size_t secret_len;
    int loop_gated;             /* 1: heartbeats only while the serving loop stamps (cluster_loop_stamp); 0: the
                                 * witness, which serves nothing */
};

/* Reads <dir>/meta/NODE (a new incarnation, synced) and starts the thread. 0, or -1 with a line on stderr. */
int cluster_start(const struct cluster_cfg *cfg);
void cluster_stop(void);

/* ---- the admin ops (`tsdb cluster ...`), sent to a server's peer address */

enum cluster_admin_op {
    CLUSTER_OP_INIT = 1,     /* body: struct meta_settings (META_SETTINGS_WORDS u32) and the mode (u32); on a server in
                              * no cluster */
    CLUSTER_OP_NODE_ADD = 2, /* body: the new server's peer address; on the Raft leader */
    CLUSTER_OP_STATUS = 3,   /* no body; on any server */
    CLUSTER_OP_VOLUME_ADD = 4, /* body: u16 name length, the name, u32 replicas (0: the cluster's replication); on the
                                * Raft leader, which picks the number, uuid, servers and rids. Reply: u32 number */
    /* USER_PUT, USER_DEL: body u16 name length, the name, u64 version (DEL: 0), u32 wait_ms, u32 blob length, the
     * blob (DEL: none); on the Raft leader. Answered once applied there: the operation's result, and when it is
     * META_OK the body u64 index, u32 uid, u32 n, n x {u32 member, u64 applied} (the members below the index after
     * wait_ms, cluster_user_propose) */
    CLUSTER_OP_USER_PUT = 5,
    CLUSTER_OP_USER_DEL = 6,
    /* USER_GET: body u16 name length, the name; on any server, from its copy. Reply: u64 version, u32 uid, the blob
     * (META_ERR_NOTFOUND without a body). An empty name lists: u32 n, n x {u16 name length, the name, u32 uid, u64
     * version} */
    CLUSTER_OP_USER_GET = 7,
    CLUSTER_OP_VOLUME_FORCE = 8, /* body: u32 volume, u32 node; on the Raft leader. While no ISR member of the volume
                                  * is up (NO_OWNER), puts the replica on `node` into its ISR, so that the next
                                  * promotion fences it and picks it: an unclean election (design-cluster.md 6.2).
                                  * Reply: text for the operator (the proposal is made; the promotion follows) */
    /* USER_SYNC: body u32 wait_ms; on the Raft leader, while it holds its check-quorum lease (no other leader can
     * exist, and its commit index covers every committed entry). The read barrier of an admin op that changes
     * nothing (cluster_user_sync). Reply as USER_PUT's with uid 0: u64 index (the leader's commit index), u32 0,
     * u32 n, n x {u32 member, u64 applied} (the members below the index after wait_ms). CLUSTER_ERR_STATE: a leader
     * without the lease (a new one, or one cut off): try again. */
    CLUSTER_OP_USER_SYNC = 9,
    /* R4 (design-cluster-data.md §6.2; R4 plan D10, D13, D14), on the Raft leader; server/ctransfer.c, cmove.c and
     * cset.c. Reply: text for the operator. TRANSFER: body u32 volume, u32 node
     * (the new leader, an ISR member). MOVE: body u32 volume, u32 from, u32 to, u32 flags (CLUSTER_MOVE_LOG,
     * CLUSTER_MOVE_CANCEL). RESET: body u32 volume, u32 node (neither leader nor in the ISR). */
    CLUSTER_OP_VOLUME_TRANSFER = 10,
    CLUSTER_OP_VOLUME_MOVE = 11,
    CLUSTER_OP_REPLICA_RESET = 12,
    CLUSTER_OP_VOLUME_GC_RESUME = 13, /* R8: u32 volume; GC_RESUME of an erasure-coded volume (E14) */
    /* R8: VOLUME_ADD's body then u8 k, u8 m (cluster_volume_add_ec_encode): an erasure-coded volume. An op of its own,
     * so that a server that predates it refuses it (unknown op) instead of adding a replicas volume */
    CLUSTER_OP_VOLUME_ADD_EC = 14,
};
enum { CLUSTER_MOVE_LOG = 1, CLUSTER_MOVE_CANCEL = 2 }; /* CLUSTER_OP_VOLUME_MOVE's flags */

/* Results beside meta.h's META_ERR_* (an op's own result travels as that) */
enum cluster_err {
    CLUSTER_ERR_NOTLEADER = -100, /* the body is the leader's peer address ("" when none is known) */
    CLUSTER_ERR_EXISTS = -101,    /* INIT on a server that already is in a cluster */
    CLUSTER_ERR_UNREACHABLE = -102, /* NODE_ADD: the new server did not answer */
    CLUSTER_ERR_REFUSED = -103,   /* NODE_ADD: the new server belongs to another cluster or has another id */
    CLUSTER_ERR_RETRY = -104,     /* the entry's outcome is unknown here (a snapshot replaced it, a new leader) */
    CLUSTER_ERR_STATE = -105,     /* not now (the server is in no cluster) */
    CLUSTER_ERR_FEW = -106,       /* VOLUME_ADD: fewer live members that take volumes than replicas */
};
const char *cluster_strerror(int rc);

/* The STATUS reply: this server's view, then the metadata image (meta_snapshot_encode; empty when it has none). */
enum cluster_mode { CLUSTER_MODE_NONE = 0, CLUSTER_MODE_CORE = 1, CLUSTER_MODE_WATCH = 2 };
/* ---- what a server reports in its heartbeat besides its address: sections (docs/design-ha-modes.md §7.1 item 1) */

/* PROMOTING: named the record's leader, its driver not the writer yet (the EPOCH marker's commit, the reopen); the
 * report's epoch is then the epoch it promotes to */
enum cluster_vol_role { CL_ROLE_NONE = 0, CL_ROLE_OWNER = 1, CL_ROLE_RELEASING = 2, CL_ROLE_PROMOTING = 3 };
enum cluster_vol_health { CL_HEALTH_OK = 0, CL_HEALTH_LOCAL_FAULT = 1, CL_HEALTH_DOMAIN_FAULT = 2 };
enum cluster_dom_state { CL_DOM_OK = 0, CL_DOM_SLOW = 1, CL_DOM_FAULT = 2 };
#define CLUSTER_REPORT_VOLS 256
#define CLUSTER_REPORT_DOMS 64

struct cluster_vol_report { /* a volume this server owns */
    uint32_t volume;
    uint8_t role;   /* enum cluster_vol_role */
    uint8_t health; /* enum cluster_vol_health: why it is not well, when it is not */
    uint64_t epoch;
    char domain[META_DOMAIN_MAX]; /* the storage domain its files live on ("" when unknown) */
    uint64_t series_len, states_len;   /* the dictionaries' lengths */
    uint64_t fenced_age_ms;            /* age of the oldest fenced call still outstanding (the stuck-release rule) */
    uint64_t wal_seg, wal_off;         /* positions as the owner last made them durable */
    uint64_t rid, open_gen;            /* replicated: the directory's replica instance and the generation its last
                                        * open saved (§8.5); the leader proposes REPLICA_OPEN for a higher one */
    uint64_t open_nonce;               /* that open's random draw, recorded with the generation */
};
struct cluster_dom_report {
    char name[META_DOMAIN_MAX];
    uint8_t state; /* enum cluster_dom_state */
};
#define CLUSTER_REPORT_UNREACH 64
struct cluster_unreach { /* a server this one shares a volume with and has not heard on any data link for age_ms */
    uint32_t node;
    uint32_t age_ms;
};
#define CLUSTER_REPORT_ISR 32
/* A replicated volume led here asks the metadata group for an ISR change (design-cluster.md 6.1, 8.1 step 5; the core's
 * REPL_M_ISR_REQ): the new ISR as (node, rid) pairs, holding the leader, on the basis (epoch, isr_version) of the record
 * it saw, and the leader's commit point. The Raft leader proposes ISR_CHANGE (meta_op_isr_change) with exactly that
 * basis, so a request that is no longer current is refused at the apply. */
struct cluster_isr_req {
    uint32_t volume;
    uint64_t epoch, isr_version, commit;
    uint32_t nisr;
    uint32_t node[META_REPLICAS_MAX];
    uint64_t rid[META_REPLICAS_MAX];
};
struct cluster_report {
    uint32_t nvols, ndoms, nunreach, nisr;
    struct cluster_vol_report vols[CLUSTER_REPORT_VOLS];
    struct cluster_dom_report doms[CLUSTER_REPORT_DOMS];
    struct cluster_unreach unreach[CLUSTER_REPORT_UNREACH];
    struct cluster_isr_req isr[CLUSTER_REPORT_ISR];
};
/* A volume led here has a new request for the metadata group (any thread, an atomic store): the cluster thread sends
 * its next heartbeat within a duty round (100 ms) instead of after heartbeat_ms, so an ISR change waits that long. */
void cluster_beat_soon(void);
/* TSDB_CLUSTER_TEST vol_trace=1 (tests only): the path of <state-dir>/voltrace, else NULL. The volume leader's driver
 * appends its ISR requests there with the core's reason (repld.c), beside the applied records (cmove_trace), so a test
 * tells a move's shrink (leave) from one for lag (drop). */
const char *cluster_vol_trace_path(void);
/* Fills `out` (zeroed) from what this server runs; called from the cluster thread once per heartbeat, so it must not
 * block. Without one (set before cluster_start) the server reports no volumes and no domains. */
typedef void (*cluster_report_fn)(void *arg, struct cluster_report *out);
void cluster_set_reporter(cluster_report_fn fn, void *arg);
/* The volume manager's claim health (any thread): 0 while claims of volumes granted to this node fail for reasons of
 * its own (the state disk, the storage), 1 once they work again. It goes into the heartbeat's flags (claim_ok), and
 * the leader lifts a CLAIM_FAILED bar only on the 0 -> 1 change. */
void cluster_set_claim_ok(int ok);

/* ---- liveness and leases (design-cluster.md §6.1, §7.1; DECISION D8 of r3-scope/PLAN.md) */
/* The serving loop's stamp: tsdbd's request loop calls it every round and every tick (any thread, an atomic store of
 * the monotonic clock). With cfg.loop_gated the cluster thread sends no heartbeat while the stamp is older than
 * CLUSTER_LOOP_STALE_MS, so a server whose loop hangs is marked down although its cluster thread runs. */
#define CLUSTER_LOOP_STALE_MS 2500 /* the loop ticks once a second when idle */
void cluster_loop_stamp(void);
/* 1 while this server holds a read lease (a Raft leader's HB_ACK extended it into the future of the cluster thread's
 * clock, CLOCK_BOOTTIME); 0 outside a cluster, so callers ask only about cluster volumes. Any thread, no lock. A volume led here is answered only while it holds (§7.1). */
int cluster_lease_valid(void);
/* A request that was admitted under the lease checks again once its snapshot is taken (§7.1: an answer from a snapshot
 * taken after the lease ended, by a process paused in between, is a stale read): cluster_lease_gen() at admission,
 * cluster_lease_still(gen) later is 1 only while the lease holds and has not lapsed in between (a lease that ended
 * and was granted again bumps the gen). */
uint64_t cluster_lease_gen(void);
int cluster_lease_still(uint64_t gen);
/* The lease arithmetic (exported for the tests): a lease granted on a heartbeat sent at send_ns lasts until
 * send_ns + lease_ms * (1 - drift); a promotion away from a server is safe lease_ms * (1 + drift) after the Raft
 * leader's last answer to it (both on the clocks named, monotonic, ns). */
uint64_t cluster_lease_end(uint64_t send_ns, uint32_t lease_ms, uint32_t drift_ppm);
uint64_t cluster_lease_safe(uint64_t answer_ns, uint32_t lease_ms, uint32_t drift_ppm);

/* ---- data links (server/peerlink.h): the acceptor hands a data link's socket to this peerlink after the HELLO (a
 * member of this cluster, by id and identity); NULL (the default) refuses them. Any thread. Shutdown: cluster_set_peerlink(NULL),
 * then cluster_stop() (no hand-off is under way after it), and only then peerlink_free. */
struct peerlink;
void cluster_set_peerlink(struct peerlink *pl);
/* The acceptor's table, one peerlink per link kind (PL_KIND_*): a data link's HELLO of kind k goes to the peerlink
 * registered for k, and a kind without one (or at or above CLUSTER_PL_KINDS) is refused with a line.
 * cluster_set_peerlink(pl) is this for PL_KIND_LOG and PL_KIND_BULK. Any thread; each kind's pointer follows the
 * shutdown order above. -1 for PL_KIND_META (the cluster thread's own) or a kind out of range, else 0. */
#define CLUSTER_PL_KINDS 8
int cluster_set_peerlink_kind(int kind, struct peerlink *pl);

/* Promotion (design-cluster.md 6.2, r3e-failover). The Raft leader sends FENCE(v, fence) to the reachable ISR members of
 * a volume whose leader is gone; the cluster thread hands it to the handler (repld_fence), whose core saves the fence
 * before it answers. The answer, and a restarted leader's RESIGN, go back through cluster_vol_msg (any thread: queued
 * and sent by the cluster thread, to `to`, or the Raft leader when 0). */
/* READY (R4 plan D14): a volume leader's planned transfer drained; `epoch` its epoch, `durable` the end every write it
 * admitted lies below and the target acknowledged durable, `rid` the target node (server/ctransfer.c's ctr_ready). */
enum { CL_VMSG_FENCE_REPLY = 1, CL_VMSG_RESIGN = 2, CL_VMSG_READY = 3 };
struct cluster_vmsg {
    uint8_t kind;
    uint32_t volume;
    uint64_t epoch; /* FENCE_REPLY: the fence answered; RESIGN: the epoch resigned; READY: the leader's epoch */
    uint64_t durable, last_epoch, rid, start;
    uint32_t to;
};
void cluster_vol_msg(const struct cluster_vmsg *m);
typedef void (*cluster_fence_fn)(void *arg, uint32_t volume, uint64_t fence, uint32_t from);
void cluster_set_fence_handler(cluster_fence_fn fn, void *arg);
/* VOLCMD (R4): the Raft leader's command to a volume's member, frame F_VOLCMD {u8 kind, u32 volume, u64 epoch, u32 node,
 * u64 arg}: TRANSFER (to the volume leader: drain and report READY for a transfer to `node`), DROP_MEMBER (a move's
 * shrink of `node`), CANCEL (a transfer is off: admit again). Handed to the handler (repld_volcmd) by the cluster
 * thread; sent by server/ctransfer.c and cmove.c (cluster_int.h cl_volcmd_send). The frame carries no Raft term: `from`
 * (the connection's member, or this server for its own) is all a receiver knows of the sender, and a deposed Raft
 * leader's TRANSFER or CANCEL at the volume's current epoch looks like the new one's. The receiving track (r4-transfer,
 * r4-move) must check `from` against the Raft leader it knows, or carry the term or a transfer id in `arg`. */
enum { CL_VOLCMD_TRANSFER = 1, CL_VOLCMD_DROP_MEMBER = 2, CL_VOLCMD_CANCEL = 3 };
struct cluster_volcmd {
    uint8_t kind;
    uint32_t volume;
    uint64_t epoch; /* the volume's epoch the command is for */
    uint32_t node;
    uint64_t arg;
};
typedef void (*cluster_volcmd_fn)(void *arg, const struct cluster_volcmd *c, uint32_t from);
void cluster_set_volcmd_handler(cluster_volcmd_fn fn, void *arg);
/* What a peerlink needs to dial and answer as this server (any thread, after cluster_start). 0, or -1 while this
 * server is in no cluster yet (id 0). */
struct cluster_self {
    int have_node;
    uint32_t id;
    uint8_t uuid[16], ident[16];
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t secret_len;
    char advertise[128]; /* the peer address this server advertises */
};
int cluster_self(struct cluster_self *out);

/* ---- this server's replicated volumes as its metadata copy holds them (server/cvol.c) */
struct cluster_vol_rec {
    uint32_t number;
    char name[META_NAME_MAX];
    uint8_t uuid[16], cluster[16];
    uint32_t leader; /* the record's leader (rep[0] at VOLUME_ADD): the writer */
    uint64_t epoch;
    char leader_addr[META_ADDR_MAX]; /* its client address as the member list holds it ("" unknown): MOVED names it */
    uint64_t rid, open_gen; /* this server's replica: its instance and the last REPLICA_OPEN generation committed */
    uint64_t open_nonce;    /* and the nonce committed with it */
    /* replication (server/repld.c): the replicas in record order (the node index of the core, src/repl.h, is the
     * position here), with the peer address of each server as the member list holds it */
    uint32_t nrep;
    uint64_t set_version; /* the version of rep[]: an index into it means a node only together with it (meta.h, D8) */
    struct cluster_vol_rep {
        uint32_t node;
        uint64_t rid;
        uint8_t learner;
        uint8_t unanswered; /* kept in the ISR by the last promotion without a fence reply (meta.h; repl.h unanswered) */
        uint32_t replaces;  /* a learner: the member it moves in for (0: none); that one's REMOVE gives it its
                             * slot (D10) */
        char peer[META_ADDR_MAX];
    } rep[META_REPLICAS_MAX];
    uint32_t nisr;
    uint32_t isr[META_REPLICAS_MAX]; /* node ids */
    uint64_t isr_version;
    uint32_t min_isr, isr_lag_ms, unanswered_ms;
    uint8_t no_owner; /* no ISR member is up in the group's view: UNAVAILABLE(NO_OWNER), not MOVED to a dead leader */
    uint32_t repl_retain_mb, repl_retain_s, idle_seal_ms; /* the settings (meta.h) */
    uint32_t rebuild_mb_s, rebuild_max, transfer_wait_ms, leader_balance_s; /* R4's settings (meta.h, D16) */
};
/* Called on the cluster thread after the metadata copy changed, once the copy holds this run's incarnation (so it
 * holds every REPLICA_OPEN committed before this start), with every replicated volume that names this server (`self`)
 * as a replica, ascending by number (at most CLUSTER_REPORT_VOLS; past that a line is logged); it must copy what it
 * keeps and not block. */
typedef void (*cluster_vol_watch_fn)(void *arg, uint32_t self, const struct cluster_vol_rec *v, uint32_t n);
void cluster_set_volume_watch(cluster_vol_watch_fn fn, void *arg);

/* ---- the whole metadata copy as one snapshot (S2-1: TOPOLOGY, STATUS, MOVED; s2e's routing, r3e, R8 read it)
 * The cluster thread builds a new snapshot after every applied batch, snapshot install and copy swap (whatever moved
 * `applied`, its term or the restore generation) and publishes it by an atomic pointer swap. cluster_copy_get() (any
 * thread, no lock, never blocks) returns the current one with a reference, NULL before this server holds an
 * initialised copy or after cluster_stop; every get is paired with one cluster_copy_put(). A snapshot never changes
 * and lives until its last reference goes; holding one does not hold the cluster thread up (it is only memory). It is
 * as stale as any copy: leaders check epochs (design-cluster.md 2.4). A get and a put each write two shared counters
 * (cache lines every reader shares): a hot path (s2e's per-request routing) keeps a snapshot per thread and gets a new
 * one only when cluster_copy_builds() moved, so that per request it only reads a shared line.
 * Member states (to settle in S3/s2e, nothing proposes them yet): TOPOLOGY's alive and cluster_vol_owner take META_UP
 * only, so a DRAINING leader would show alive 0 and its volumes NO_OWNER; OUT members stay in the copy for good (a
 * new member may reuse an OUT member's peer address), so TOPOLOGY's nodes and STATUS's peers list them. */
struct cluster_copy_member {
    uint32_t id;
    uint8_t state;            /* enum meta_member_state */
    char addr[META_ADDR_MAX]; /* the client address, host:port */
    char peer[META_ADDR_MAX]; /* the peer address */
};
struct cluster_copy_vol {
    uint32_t number;
    char name[META_NAME_MAX]; /* what clients name it by (tsdbc_series_at's path) */
    uint8_t kind;             /* enum meta_vol_kind */
    uint32_t leader; /* the record's leader (writer, shared: owner); 0: none */
    uint64_t epoch;
    uint32_t nrep;
    struct cluster_copy_rep {
        uint32_t node;
        uint8_t learner;
    } rep[META_REPLICAS_MAX]; /* in record order */
    uint32_t nisr;
    uint32_t isr[META_REPLICAS_MAX]; /* node ids, ascending */
    /* R8: an erasure-coded volume's (redundancy META_RED_EC) k + m, its stripe group (position -> node) and version,
     * and whether its orphan collection is suspended (E14); a replicas volume has 0 in all of them */
    uint8_t redundancy, ec_k, ec_m, gc_suspended;
    uint32_t nstripe;
    uint32_t stripe[META_STRIPE_MAX];
    uint64_t stripe_version;
};
struct cluster_copy {
    uint64_t applied, applied_term, restore_gen;
    uint32_t self; /* this server's member id (0: none yet) */
    uint32_t nmembers, nvolumes;
    const struct cluster_copy_member *members; /* ascending by id */
    const struct cluster_copy_vol *volumes;    /* ascending by number */
};
const struct cluster_copy *cluster_copy_get(void);
void cluster_copy_put(const struct cluster_copy *c);
/* how many snapshots this process has published: unchanged means a snapshot held since is still the current one */
unsigned long cluster_copy_builds(void);
/* Tests only (test_cluster ccstress), with no cluster running in the process: `readers` threads loop get/put while
 * `publishes` snapshots are swapped in; 0 when all were published within `ms` and every reader saw what it held
 * intact, else -1. why: a line either way (counts, the longest swap). */
int cluster_copy_stress(unsigned readers, unsigned long publishes, unsigned ms, char *why, size_t wn);
const struct cluster_copy_member *cluster_copy_member(const struct cluster_copy *c, uint32_t id); /* NULL: none */
const struct cluster_copy_vol *cluster_copy_vol(const struct cluster_copy *c, uint32_t number);    /* NULL: none */
/* the volume named `name` (n bytes, no NUL needed), NULL: none (s2e routes series ops that name a volume by it) */
const struct cluster_copy_vol *cluster_copy_vol_named(const struct cluster_copy *c, const char *name, size_t n);
/* The role of member `node` in volume v as TOPOLOGY says it (TSDB_ROLE_*, D9: no new role before S3): the leader
 * OWNER, a replica in the ISR STANDBY, any other replica (a learner too) FOLLOWER, anyone else NONE. */
int cluster_copy_role(const struct cluster_copy_vol *v, uint32_t node);
/* Where requests for volume `vol` go, from the current snapshot: 1 when it has a leader whose member is UP and is
 * another server, 2 when that leader is this server (the snapshot's self), with its id, the epoch and its client
 * address in addr, cap bytes; 0 when there is none (no copy, no such volume, no leader, or the leader is not UP).
 * Never turn a 2 into MOVED: the client would follow it back here until its retry budget ran out. Any thread; never call it on the cluster thread's own path (vol_watch_duty and the like
 * read C.sm directly). */
int cluster_vol_owner(uint32_t vol, uint32_t *node, uint64_t *epoch, char *addr, size_t cap);

/* ---- users and grants in the metadata group (design-cluster.md C22, design-ha-modes.md :916; S2-1) */
struct cluster_user_rec {
    const char *name;    /* at most USER_NAME_MAX bytes */
    uint32_t uid;        /* assigned by the state machine, never reused */
    uint64_t version;    /* the index of the entry that last put the user: what a conditional put names */
    const uint8_t *blob; /* META_USER_UID_LEN bytes of uid (little-endian), then the user's compacted USERS records */
    uint32_t len;
};
/* The group stores any blob a holder of the cluster secret puts under a name (meta.c checks only the uid): a consumer
 * decodes it (user_blob_decode), refuses one that does not decode, and checks that the user it holds has the record's
 * name, before it lets anyone log in with it.
 * Called on the cluster thread with every user of the metadata copy, ascending by name, and the applied index the
 * list is at, after the copy changed its users (an entry or a snapshot) and once when a callback is set while the copy
 * has any. The records point into the copy and are valid during the call only: it must copy what it keeps and not
 * block. It returns 0, or non-zero when it could not take the list (out of memory): the list is pushed again on the
 * next tick (100 ms) until it is taken. NULL stops the calls. Any thread. */
typedef int (*cluster_users_fn)(void *arg, const struct cluster_user_rec *u, uint32_t n, uint64_t applied);
void cluster_set_users_watch(cluster_users_fn fn, void *arg);

/* The catch-up barrier (D-S2): 0 from cluster_start until this server has heard a Raft leader's commit index in this
 * run (an HB_ACK from the leader it names that grants the lease, so that leader holds the check-quorum lease; the leader
 * itself: its commit index once an entry of its own term is committed) and its copy has applied up to it; 1 from then on, also while it is cut off later (a member cut off
 * keeps the users it has until it hears the group, ha-modes :916). Logins wait for it, so a restarted server never
 * accepts a user whose drop committed while it was down. Any thread. */
int cluster_users_caught_up(void);
/* R8 (D9): 1 while copy `c` (cluster_copy_get) has applied the Raft leader's commit index heard last (from a heartbeat
 * ack that grants the lease, or on the leader its own) and the lease of that heartbeat still holds (counted from its
 * send time, at most 2 s): no other leader can have committed past what `c` shows. A chunk store that opened unsure is
 * raised to a volume's epoch taken from `c` only then. The caller reads the clock itself, so a stalled cluster thread
 * makes it 0, never a stale 1. 0 for NULL. Any thread. */
int cluster_copy_caught_up_at(const struct cluster_copy *c);
/* the same for the copy published now (a get and a put around cluster_copy_caught_up_at) */
int cluster_copy_caught_up(void);
/* the number of chunk stores UP on this server (chunksrv_cluster.c), sent in every heartbeat (HBS_STORES): the
 * proposer of an erasure-coded volume picks its stripe group among the members that report one or more */
void cluster_set_chunk_stores(uint32_t n);
/* The applied index through which the users watch has every change of the group's users (the index of the copy when
 * the watch was last given, or found to have, its users): a forwarded request's owner refuses it with UNAVAILABLE while
 * this is below the index the entry node's users were at (fwdreq.h), so a user added a moment ago is retried, never
 * refused AUTH. 0 until the first push. Any thread. */
uint64_t cluster_users_through(void);

enum { CLUSTER_USER_PUT = 1, CLUSTER_USER_DEL = 2 };
#define CLUSTER_LAGGARDS_MAX 64
struct cluster_laggard {
    uint32_t id;      /* a member (not OUT) */
    uint64_t applied; /* the applied index its last heartbeat reported; 0: not heard */
};
/* Proposes USER_PUT (`blob`, `len`: META_USER_UID_LEN bytes, which the state machine overwrites with the uid, then
 * the records; conditional on `version`, 0 to create) or USER_DEL (blob, len and version unused) through the Raft
 * leader (this server, or the leader it knows, over the admin link with the cluster secret). Returns once the entry
 * is committed and applied on the leader and every member has reported an applied index at or above it, or wait_ms
 * after that: *index is the entry's index, *uid the user's, lag[0 .. *nlag) the members still below it (at most
 * CLUSTER_LAGGARDS_MAX). Result: META_OK, a META_ERR_* of the operation (STALE, EXISTS, NOTFOUND, INVALID, RANGE), or
 * a CLUSTER_ERR_* (NOTLEADER when no leader is known, RETRY when the outcome is unknown: read the user again before
 * deciding). `actor` (NULL: the operator) and `actor_version`: the user whose rights allowed the change, as the
 * caller's copy holds them; the op is then STALE unless the actor still has that version (meta_op_user_put_as).
 * Blocks: never on the cluster thread. */
int cluster_user_propose(int op, const char *name, const void *blob, uint32_t len, uint64_t version,
                         const char *actor, uint64_t actor_version, int wait_ms, uint64_t *index, uint32_t *uid,
                         struct cluster_laggard *lag, uint32_t *nlag);
/* The read barrier of the users (A32): the Raft leader's commit index, taken while it holds its lease, in *index once
 * every member has reported an applied index at or above it, or wait_ms after that (lag, nlag: as
 * cluster_user_propose's). The caller then waits until cluster_users_through() reaches *index: its copy then holds
 * every change acknowledged before the call. Result: META_OK, CLUSTER_ERR_NOTLEADER (no leader known),
 * CLUSTER_ERR_STATE (a leader without its lease yet), CLUSTER_ERR_RETRY (no answer). Blocks: never on the cluster
 * thread. */
int cluster_user_sync(int wait_ms, uint64_t *index, struct cluster_laggard *lag, uint32_t *nlag);

struct cluster_vol_status {
    struct cluster_vol_report v;
    uint64_t local_fault_s; /* how long the owner has reported a fault of this volume's domain, without a break */
    uint64_t fault_ns;      /* inside the server: when that fault began, on its own clock; 0 none */
};
struct cluster_heard {
    uint32_t id;
    uint64_t ago_ms; /* UINT64_MAX: never */
    uint64_t incarnation;
    uint8_t flags;       /* enum meta_member_flags as the member reported them */
    uint8_t core_dropped;
    uint8_t read_barrier; /* 1: the node runs with the read barrier */
    uint64_t state_free; /* free bytes of its state directory */
    uint32_t nvols, ndoms;
    struct cluster_vol_status *vols;
    struct cluster_dom_report *doms;
    uint32_t nunreach; /* the servers its heartbeat says it cannot reach on the data links */
    struct cluster_unreach unreach[CLUSTER_REPORT_UNREACH];
    uint64_t answered_ms; /* on the Raft leader: since its last lease-granting HB_ACK to it (UINT64_MAX: none) */
    int64_t lease_left_ms; /* the server's own entry: how long its lease still runs (< 0: expired) */
};
struct cluster_status {
    uint32_t self;
    uint8_t mode, role; /* role: raft_role (0 follower, 1 pre-candidate, 2 candidate, 3 leader) */
    uint8_t stale;      /* 1: the image is this member's persisted copy of an earlier run, or the state it served
                         * before its core started, not yet confirmed by its core or a stream */
    uint64_t term, commit, applied;
    uint32_t leader;
    uint32_t nheard;
    struct cluster_heard *heard;
    uint8_t users_caught_up; /* cluster_users_caught_up() on that server */
    /* planned leader transfers (R4 D14, D15): enum cluster_tr_count; the Raft leader's side counts only on the one that
     * ran them (a transfer's state lives in memory), the volume leaders' side on every member */
    uint32_t ntr;
    uint64_t tr[16];
    char moves[8192]; /* the Raft leader's moves running, a line each with its step and age (cmove_status) */
    char links[1024]; /* the answering server's peer data links up, per kind and frame check (peerlink_links_status) */
    const uint8_t *image;
    size_t image_len;
};
enum cluster_tr_count {
    CL_TR_STARTED,   /* the Raft leader: transfers it sent TRANSFER for (an operator's and the balance's) */
    CL_TR_DONE,      /* the new leader was the target */
    CL_TR_TIMEDOUT,  /* no READY within transfer_wait_ms + 1 s: CANCEL */
    CL_TR_CANCELLED, /* CANCEL for another reason (a refused promotion, a lost epoch) */
    CL_TR_FALLBACK,  /* the targeted promotion was dropped after READY: a failover by another name */
    CL_TR_BALANCE,   /* of those started, by leader balance */
    CL_TR_DRAIN_TIMEDOUT, /* the volume leaders here: drains that gave up before READY */
    CL_TR_BLIND,     /* records admitted after READY's hard cap (10 x transfer_wait_ms); they may be cut */
    CL_TR_READY,     /* READYs sent */
    CL_TR_CUTS,      /* replica log cuts that removed records here (REPL_A_TRUNCATE below the log's end) */
    CL_TR_MV_STARTED,   /* the Raft leader: replica moves it proposed the REPLICA_ADD of (cmove.c) */
    CL_TR_MV_DONE,      /* moves it ended with the REMOVE of the replaced member */
    CL_TR_MV_CANCELLED, /* moves it ended with the REMOVE of the learner (`--cancel`) */
    CL_TR_N
};
/* The volume leaders' counters (CL_TR_DRAIN_TIMEDOUT and on) come from tsdbd's driver; without one they read 0. */
typedef void (*cluster_drv_counts_fn)(uint64_t out[CL_TR_N]);
void cluster_set_drv_counts(cluster_drv_counts_fn fn);
/* Parses a STATUS body; `image` points into it, `heard` is allocated (cluster_status_free). 0 or -1. */
int cluster_status_parse(const uint8_t *body, size_t len, struct cluster_status *st);
void cluster_status_free(struct cluster_status *st);

/* One admin request to `addr` (host:port, the server's peer address): connects, authenticates with the secret, sends
 * the op and waits up to timeout_ms for the reply. Returns 0 with the op's result in *rc and its body in *reply
 * (malloc'ed; the caller frees it), or -1 with a sentence in err. */
int cluster_admin(const char *addr, const uint8_t *secret, size_t secret_len, int op, const void *req,
                  size_t req_len, int timeout_ms, int *rc, uint8_t **reply, size_t *reply_len, char *err,
                  size_t errn);

/* The secret file: its bytes, without one trailing newline; CLUSTER_SECRET_MIN bytes at least. A regular file that
 * neither its group nor others may read or write (it is full control of the metadata), else refused. 0 or -1 with
 * err. */
int cluster_read_secret(const char *path, uint8_t *out, size_t cap, size_t *len, char *err, size_t errn);

/* `tsdb cluster secret FILE`: a new secret (32 random bytes, hex) in a new file of mode 0600, synced. 0 or -1 with
 * err; an existing file is never overwritten. */
int cluster_make_secret(const char *path, char *err, size_t errn);

/* HMAC-SHA256 over the concatenation of n parts (the handshake and the frame tags use it; exported for the tests). */
void cluster_hmac_parts(const uint8_t *key, size_t klen, int n, const void *const *part, const size_t *len,
                        uint8_t out[32]);

/* The VOLUME_ADD body (at most 2 + META_NAME_MAX + 4 bytes). */
size_t cluster_volume_add_encode(const char *name, uint32_t replicas, uint8_t *out);
/* the body of CLUSTER_OP_VOLUME_ADD_EC: the same with k + m of an erasure-coded volume (R8; replicas 0: m + 1) */
size_t cluster_volume_add_ec_encode(const char *name, uint32_t replicas, uint8_t k, uint8_t m, uint8_t *out);
size_t cluster_volume_force_encode(uint32_t volume, uint32_t node, uint8_t *out); /* 8 bytes */
/* The bodies of VOLUME_TRANSFER and REPLICA_RESET (8 bytes, as VOLUME_FORCE's) and VOLUME_MOVE (16 bytes); the
 * decoders take exactly that many bytes (0, else -1). */
size_t cluster_volume_move_encode(uint32_t volume, uint32_t from, uint32_t to, uint32_t flags, uint8_t *out);
int cluster_volume_move_decode(const uint8_t *p, size_t n, uint32_t *volume, uint32_t *from, uint32_t *to,
                               uint32_t *flags);
int cluster_volume_force_decode(const uint8_t *p, size_t n, uint32_t *volume, uint32_t *node);

/* The INIT body for `s` and the deployment's mode, an enum meta_mode (4 * (META_SETTINGS_WORDS + 1) bytes). */
size_t cluster_settings_encode(const struct meta_settings *s, int mode, uint8_t *out);

#endif
