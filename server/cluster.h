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
};

/* Reads <dir>/meta/NODE (a new incarnation, synced) and starts the thread. 0, or -1 with a line on stderr. */
int cluster_start(const struct cluster_cfg *cfg);
void cluster_stop(void);

/* ---- the admin ops (`tsdb cluster ...`), sent to a server's peer address */

enum cluster_admin_op {
    CLUSTER_OP_INIT = 1,     /* body: struct meta_settings (12 u32) and the mode (u32); on a server in no cluster */
    CLUSTER_OP_NODE_ADD = 2, /* body: the new server's peer address; on the Raft leader */
    CLUSTER_OP_STATUS = 3,   /* no body; on any server */
};

/* Results beside meta.h's META_ERR_* (an op's own result travels as that) */
enum cluster_err {
    CLUSTER_ERR_NOTLEADER = -100, /* the body is the leader's peer address ("" when none is known) */
    CLUSTER_ERR_EXISTS = -101,    /* INIT on a server that already is in a cluster */
    CLUSTER_ERR_UNREACHABLE = -102, /* NODE_ADD: the new server did not answer */
    CLUSTER_ERR_REFUSED = -103,   /* NODE_ADD: the new server belongs to another cluster or has another id */
    CLUSTER_ERR_RETRY = -104,     /* the entry's outcome is unknown here (a snapshot replaced it, a new leader) */
    CLUSTER_ERR_STATE = -105,     /* not now (the server is in no cluster) */
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
};
struct cluster_dom_report {
    char name[META_DOMAIN_MAX];
    uint8_t state; /* enum cluster_dom_state */
};
struct cluster_report {
    uint32_t nvols, ndoms;
    struct cluster_vol_report vols[CLUSTER_REPORT_VOLS];
    struct cluster_dom_report doms[CLUSTER_REPORT_DOMS];
};
/* Fills `out` (zeroed) from what this server runs; called from the cluster thread once per heartbeat, so it must not
 * block. Without one (set before cluster_start) the server reports no volumes and no domains. */
typedef void (*cluster_report_fn)(void *arg, struct cluster_report *out);
void cluster_set_reporter(cluster_report_fn fn, void *arg);

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

/* The INIT body for `s` and the deployment's mode, an enum meta_mode (52 bytes). */
size_t cluster_settings_encode(const struct meta_settings *s, int mode, uint8_t *out);

#endif
