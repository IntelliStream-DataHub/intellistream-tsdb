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
    CLUSTER_OP_VOLUME_FORCE = 5, /* body: u32 volume, u32 node; on the Raft leader. While no ISR member of the volume
                                  * is up (NO_OWNER), puts the replica on `node` into its ISR, so that the next
                                  * promotion fences it and picks it: an unclean election (design-cluster.md 6.2).
                                  * Reply: text for the operator (the proposal is made; the promotion follows) */
};

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

enum cluster_vol_role { CL_ROLE_NONE = 0, CL_ROLE_OWNER = 1, CL_ROLE_RELEASING = 2 };
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

/* Promotion (design-cluster.md 6.2, r3e-failover). The Raft leader sends FENCE(v, fence) to the reachable ISR members of
 * a volume whose leader is gone; the cluster thread hands it to the handler (repld_fence), whose core saves the fence
 * before it answers. The answer, and a restarted leader's RESIGN, go back through cluster_vol_msg (any thread: queued
 * and sent by the cluster thread, to `to`, or the Raft leader when 0). */
enum { CL_VMSG_FENCE_REPLY = 1, CL_VMSG_RESIGN = 2 };
struct cluster_vmsg {
    uint8_t kind;
    uint32_t volume;
    uint64_t epoch; /* FENCE_REPLY: the fence answered; RESIGN: the epoch resigned */
    uint64_t durable, last_epoch, rid, start;
    uint32_t to;
};
void cluster_vol_msg(const struct cluster_vmsg *m);
typedef void (*cluster_fence_fn)(void *arg, uint32_t volume, uint64_t fence, uint32_t from);
void cluster_set_fence_handler(cluster_fence_fn fn, void *arg);
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
    struct cluster_vol_rep {
        uint32_t node;
        uint64_t rid;
        uint8_t learner;
        char peer[META_ADDR_MAX];
    } rep[META_REPLICAS_MAX];
    uint32_t nisr;
    uint32_t isr[META_REPLICAS_MAX]; /* node ids */
    uint64_t isr_version;
    uint32_t min_isr, isr_lag_ms, unanswered_ms;
    uint8_t no_owner; /* no ISR member is up in the group's view: UNAVAILABLE(NO_OWNER), not MOVED to a dead leader */
    uint32_t repl_retain_mb, repl_retain_s, idle_seal_ms; /* the settings (meta.h) */
};
/* Called on the cluster thread after the metadata copy changed, once the copy holds this run's incarnation (so it
 * holds every REPLICA_OPEN committed before this start), with every replicated volume that names this server (`self`)
 * as a replica, ascending by number (at most CLUSTER_REPORT_VOLS; past that a line is logged); it must copy what it
 * keeps and not block. */
typedef void (*cluster_vol_watch_fn)(void *arg, uint32_t self, const struct cluster_vol_rec *v, uint32_t n);
void cluster_set_volume_watch(cluster_vol_watch_fn fn, void *arg);

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
    uint8_t stale;      /* 1: the image is this member's persisted copy of an earlier run, not yet confirmed */
    uint64_t term, commit, applied;
    uint32_t leader;
    uint32_t nheard;
    struct cluster_heard *heard;
    const uint8_t *image;
    size_t image_len;
};
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
size_t cluster_volume_force_encode(uint32_t volume, uint32_t node, uint8_t *out); /* 8 bytes */

/* The INIT body for `s` and the deployment's mode, an enum meta_mode (4 * (META_SETTINGS_WORDS + 1) bytes). */
size_t cluster_settings_encode(const struct meta_settings *s, int mode, uint8_t *out);

#endif
