/*
 * The cluster's metadata state machine (docs/design-cluster.md §2): what the Raft group agrees on. Sans-IO and
 * deterministic: an entry's bytes go in (meta_apply), the state changes the same way on every server, and a snapshot
 * (meta_snapshot_encode / meta_snapshot_decode) carries the whole state between servers and to disk.
 *
 * It holds the cluster (uuid, settings), the members, the volumes with their replicas, leader, epoch, in-sync set
 * (ISR) and epoch history, the 65,536 name slots and their earlier homes, and the users (design-auth.md's records,
 * opaque here). It never holds rows, positions, parts or series.
 *
 * An entry is a list of operations, each applied on its own and in order, with its own result: an operation that
 * does not hold (a stale epoch, a name in use) changes nothing and the ones after it still apply. That is what makes
 * one failover entry for many volumes safe without an all-or-nothing rule: every volume operation is conditional on
 * the volume's current epoch (and ISR version), so a proposer with a stale view changes nothing (§6.3, C11).
 *
 * Two kinds of volume (design-ha-modes.md §5.4), one deployment mode (INIT, never changed): a replicated volume has
 * replicas that each hold a copy and an in-sync set; a shared volume lives on storage every candidate sees, so its
 * "ISR" is always every candidate and no copy can fall behind. The rules the state enforces (and meta_check_step
 * checks from the outside, in the simulator), keyed on the volume's kind:
 *   - a replicated volume's epoch only rises, by one per new leader, and the new leader was in the ISR the old epoch
 *     had (C12); the promotion's leader and ISR are the result of repl_meta_promote over the fence replies the entry
 *     carries, the function the replication simulator uses;
 *   - a shared volume's epoch only rises too, but the owner holds epoch + 1 reserved: a grant to another node goes
 *     to epoch + 2 or higher, a node that claims its own reserved epoch records it with VOLUME_SELF, and every
 *     epoch stays below 2^24 - 1 with its reservation (ha.md §2.4). The reservation is implicit: always epoch + 1,
 *     held by the recorded leader. It needs no field, so it cannot drift from `leader`;
 *   - the ISR changes only by an operation that names the volume's current epoch and ISR version, always holds the
 *     leader, never holds a server that is not one of the volume's replicas, and names each member's replica
 *     instance (rid), which must be the one the record holds; a shared volume's ISR is not changed by ISR_CHANGE;
 *   - a learner becomes a full replica only by joining the ISR; an ISR member added had been a replica;
 *   - reservations of ids, watermarks, open generations, the restore generation and the highest epoch seen never go
 *     down; reservations and drops name the volume's epoch exactly, so every one of epoch E is committed before any
 *     grant above E (§5.4);
 *   - a name slot changes home only through a committed freeze of its old home (SLOT_FREEZE under the epoch the old
 *     home had then): SLOT_MOVE_PENDING, SLOT_FREEZE, SLOT_MOVE (or SLOT_MOVE_CANCEL);
 *   - member ids, volume numbers and volume names are never reused.
 * Positions an entry carries (grants, reservations) are reports: they are copied, never compared with anything, and
 * have no "never down" rule (a lower one after a pool rollback is exactly the signal for DIVERGED).
 *
 * **No clocks in apply.** "A grant after lease_s", "a new incarnation that has not claimed its volumes again within
 * 3 x lease_s" and every other decision about time are the proposer's (cluster.c). The state machine enforces epochs,
 * owners, kinds, blocks and attestations (the attestation's own time is written by the proposer and only recorded),
 * and this checker likewise judges nothing about time: a leader that grants too early is not caught here.
 *
 * An entry that does not decode is not a refusal but a fault: every server must apply the same bytes the same way,
 * so a build that does not understand an entry stops (META_ERR_FORMAT) rather than skip it, and an operation body is
 * consumed exactly. The format freezes at 1.0: after it, a new field is a new operation kind, never trailing bytes on
 * an old one, and `enum meta_op_kind` is never renumbered. INIT and the snapshot carry a `level` (META_LEVEL) so that
 * a later rolling upgrade can hold new operation kinds back until every member reports it supports them; until that
 * exists, servers of mixed versions are not supported and a cluster is re-initialised, not upgraded.
 * (Heartbeat sections in cluster.c are peer-link wire, not replicated state: unknown sections may be skipped there.)
 */
#ifndef TSDB_META_H
#define TSDB_META_H

#include <stddef.h>
#include <stdint.h>

#define META_LEVEL 1 /* the operation level this build applies (INIT and the snapshot carry the cluster's) */
#define META_ADDR_MAX 128
#define META_RACK_MAX 64
#define META_NAME_MAX 64
#define META_FEED_MAX 64  /* a power feed's name */
#define META_BMC_MAX 128  /* a BMC address */
#define META_PATH_MAX 256 /* a shared volume's mount path and export */
#define META_FSTYPE_MAX 16
#define META_GROUP_MAX 64 /* a server group (the takeover cap) */
#define META_DOMAIN_MAX 64
#define META_REPLICAS_MAX 7 /* replication 4 plus learners during a move */
#define META_SLOTS 65536
#define META_NONE 0xffffffffu /* no volume (a slot without a home) */
#define META_VOLUME_MAX 65534 /* series64: the volume field of an id is volume + 1 below 65,536 */
#define META_USER_BLOB_MAX 4096
#define META_OPS_MAX 1024 /* operations in one entry */
/* A shared volume's epoch, and the epoch reserved after it, stay below this (ha.md §2.4: E is a u24 in block
 * numbers, and an epoch at or above 2^24 - 1 is refused everywhere). */
#define META_EPOCH_LIMIT ((1ull << 24) - 1)

enum meta_err {
    META_OK = 0,
    META_ERR_FORMAT = -1,   /* the bytes do not decode: a fault, never a refusal */
    META_ERR_EXISTS = -2,   /* the id, number or name is taken */
    META_ERR_NOTFOUND = -3, /* no such member, volume or user */
    META_ERR_STALE = -4,    /* the epoch, ISR version, generation or reservation named is not current */
    META_ERR_INVALID = -5,  /* the change would break a rule above */
    META_ERR_STATE = -6,    /* not now: the cluster is (not) initialised */
    META_ERR_RANGE = -7,    /* a shared volume's epoch space (below 2^24 - 1, reservation included) is used up */
    META_ERR_ATTEST = -8,   /* the deployment's failover mode needs an attestation this grant does not carry */
    META_ERR_BLOCKED = -9,  /* the grant goes to a node a CLAIM_FAILED barred, while another candidate is not */
};
const char *meta_strerror(int rc);

/* The deployment's mode, set once by INIT; a volume's kind must equal it. */
enum meta_mode { META_MODE_NONE = 0, META_MODE_REPLICATED = 1, META_MODE_SHARED = 2 };
enum meta_vol_kind { META_VOL_REPLICATED = 1, META_VOL_SHARED = 2 };
/* SETTINGS.failover: how a shared volume moves off a node that is no longer heard (design-ha-modes.md M22). */
enum meta_failover { META_FAILOVER_LEASE = 0, META_FAILOVER_ATTESTED = 1, META_FAILOVER_BMC = 2 };

enum meta_member_state { META_UP = 0, META_DOWN = 1, META_DRAINING = 2, META_OUT = 3 };
const char *meta_member_state_name(int s);
/* A member's flags (MEMBER_ADD, then MEMBER_FLAGS from heartbeats): may it be a voter, may it hold volumes, can it
 * claim a volume it was granted. A witness has volumes_ok = 0; a node whose disk filled has voter_ok = 0; a node whose
 * claims failed (CLAIM_FAILED's reasons: the state disk, the storage) reports claim_ok = 0 until it is well again. */
enum meta_member_flags { META_MF_VOTER_OK = 1, META_MF_VOLUMES_OK = 2, META_MF_CLAIM_OK = 4, META_MF_ALL = 7 };

struct meta_settings {
    uint32_t replication;        /* replicas per volume (3) */
    uint32_t min_isr;            /* default per volume (1, D1) */
    uint32_t node_timeout_ms;    /* a server is down after this long without a heartbeat (3000) */
    uint32_t heartbeat_ms;       /* heartbeat period (500) */
    uint32_t isr_lag_ms;         /* a replica this far behind leaves the ISR (1000) */
    uint32_t out_after_s;        /* a server down this long is out (1800) */
    uint32_t voter_replace_s;    /* a voter down this long is replaced (600) */
    uint32_t voters;             /* the Raft group's size at most (5) */
    uint32_t volumes_per_server; /* led per server at init (16) */
    uint8_t failover;            /* enum meta_failover (lease) */
    uint8_t read_barrier;        /* 1: owners answer reads only behind a fenced write (M24; a node may override) */
    uint32_t lease_s;            /* the shared volumes' lease (10); the proposer's clock, never apply's */
};
void meta_settings_default(struct meta_settings *s);

struct meta_member {
    uint32_t id;
    uint8_t state; /* enum meta_member_state */
    uint64_t incarnation;
    char addr[META_ADDR_MAX]; /* the client address, host:port */
    char peer[META_ADDR_MAX]; /* the peer address, host:port */
    char rack[META_RACK_MAX];
    char power_feed[META_FEED_MAX]; /* the feed beside the rack ("" when not given) */
    char bmc[META_BMC_MAX];         /* the BMC address ("" when not given) */
    uint8_t flags;                  /* enum meta_member_flags */
    uint8_t ident[16]; /* the server's own random identity (meta/IDENT): one id is one server; zero: not known */
};

struct meta_replica {
    uint32_t node;
    uint8_t learner;
    uint8_t unanswered;    /* the last promotion kept it in the ISR without a fence reply (§7.1 item 16) */
    uint8_t claim_blocked; /* shared: a CLAIM_FAILED barred it from grants until its flags say it is well again */
    uint64_t rid;          /* the replica instance (replicated: new for every fresh directory; shared: 0) */
    uint64_t open_gen;     /* the last REPLICA_OPEN generation */
};

struct meta_epoch {
    uint64_t epoch, start; /* the epoch and the first position its leader wrote (§6.2, C14) */
};

/* What the owner reported at its last durable points: reports, never conditions (head comment). */
struct meta_positions {
    uint64_t wal_seg, wal_off;       /* the WAL at the last wal_durable */
    uint64_t ledger_gen, ledger_off; /* the ledger's checkpoint generation and the offset after its last record */
    uint64_t series_len, states_len, classes_len, retention_len;
};

struct meta_storage { /* a shared volume's storage */
    char path[META_PATH_MAX];     /* the mount path every candidate uses */
    char export[META_PATH_MAX];   /* server:/export */
    char fstype[META_FSTYPE_MAX]; /* "nfs4", ... */
    char group[META_GROUP_MAX];   /* the server group (the takeover cap) */
};

struct meta_volume {
    uint32_t number;
    char name[META_NAME_MAX];
    uint8_t uuid[16];
    uint8_t kind; /* enum meta_vol_kind: equals the deployment's mode */
    struct meta_storage st; /* shared volumes; empty for replicated ones */
    uint32_t min_isr;
    uint32_t nrep;
    struct meta_replica rep[META_REPLICAS_MAX]; /* rep[0] is the preferred leader; the candidates of a shared volume */
    uint32_t leader; /* shared: the owner; epoch + 1 is reserved for it */
    uint64_t epoch;
    uint32_t nisr;
    uint32_t isr[META_REPLICAS_MAX]; /* node ids, ascending; shared: every candidate */
    uint64_t isr_version;
    uint64_t isr_commit;    /* a shrink's commit point, when the leader gave one (item 22); 0: none */
    uint64_t highest_seen;  /* shared: the highest epoch a CLAIM_REFUSED reported; never goes down */
    uint32_t series_reserved, states_reserved; /* shared: ids below these are the owner's to assign (M12) */
    uint64_t wm_gen;        /* shared: the drop watermark's generation */
    int64_t dropped_before; /* shared: the drop watermark, ms */
    struct meta_positions pos;
    uint32_t nhist; /* replicated volumes only: a shared volume's `start` means nothing and is not kept */
    struct meta_epoch *hist;
};

struct meta_slot_move {
    uint32_t slot, home;     /* the home the slot had before a move */
    uint64_t from_index;     /* the metadata index from which `home` was the slot's home (§5.7) */
};

/* A move in flight (SLOT_MOVE_PENDING .. SLOT_MOVE or SLOT_MOVE_CANCEL), sparse: a slot without one has no record. */
struct meta_slot_pending {
    uint32_t slot, from, to;
    uint64_t freeze_epoch; /* the epoch `from`'s owner committed its freeze under; 0: not frozen yet */
};

/* Runs of slots whose home has been that since one metadata index (sparse: a handful of runs). */
struct meta_since_run {
    uint32_t first, count;
    uint64_t index;
};

/* A storage domain's state in the metadata (§5.4): the others live in heartbeats. */
enum meta_domain_state { META_DOM_NORMAL = 0, META_DOM_MAINTENANCE = 1, META_DOM_ROLLED_BACK = 2 };
struct meta_domain {
    char name[META_DOMAIN_MAX];
    uint8_t state;
};

struct meta_user {
    char name[META_NAME_MAX];
    uint32_t len;
    uint8_t *blob;
};

/* The Raft configuration as the state machine last applied it (a CONFIG entry or a snapshot), in the core's wire
 * form (raft_config_encode), so meta.c need not know Raft. */
#define META_CONFIG_MAX 128

struct meta_state {
    uint64_t applied, applied_term;
    uint32_t config_len;
    uint8_t config[META_CONFIG_MAX];
    int initialised;
    uint8_t uuid[16];
    uint8_t mode;       /* enum meta_mode, set once by INIT */
    uint32_t level;     /* the operation level the cluster runs at (INIT) */
    uint64_t restore_gen; /* raised by `tsdb cluster restore`; never goes down */
    struct meta_settings set;
    uint32_t next_member; /* ids are never reused */
    uint32_t nmembers, members_cap;
    struct meta_member *members; /* ascending by id */
    uint32_t next_volume;
    uint32_t nvolumes, volumes_cap;
    struct meta_volume *volumes; /* ascending by number */
    uint32_t ndomains, domains_cap;
    struct meta_domain *domains; /* ascending by name */
    uint32_t *slot_home;         /* META_SLOTS entries */
    uint32_t nsince, since_cap;
    struct meta_since_run *since; /* ascending by first; every slot with a home is in one run */
    uint32_t nmoves, moves_cap;
    struct meta_slot_move *moves;
    uint32_t npend, pend_cap;
    struct meta_slot_pending *pend; /* ascending by slot */
    uint32_t nusers, users_cap;
    struct meta_user *users; /* ascending by name */
};

void meta_init(struct meta_state *m);
void meta_free(struct meta_state *m);

const struct meta_member *meta_member(const struct meta_state *m, uint32_t id);
const struct meta_volume *meta_volume(const struct meta_state *m, uint32_t number);
const struct meta_domain *meta_domain(const struct meta_state *m, const char *name);
const struct meta_replica *meta_replica(const struct meta_volume *v, uint32_t node);
const struct meta_slot_pending *meta_slot_pending(const struct meta_state *m, uint32_t slot);
/* The index from which slot `s`'s current home has been its home (0 when it has none). */
uint64_t meta_slot_since(const struct meta_state *m, uint32_t s);
int meta_volume_in_isr(const struct meta_volume *v, uint32_t node);
/* A shared volume's reserved epoch: held by the recorded leader. */
static inline uint64_t meta_reserved_epoch(const struct meta_volume *v) { return v->epoch + 1; }

/* ---- operations: a builder appends them to an entry */

/* Never renumbered (the format freezes at 1.0); new kinds go at the end. */
enum meta_op_kind {
    META_OP_INIT = 1,         /* uuid, settings, the first member, the mode, the level */
    META_OP_MEMBER_ADD,       /* a new server: id, addresses, rack, power feed, BMC, flags, incarnation */
    META_OP_MEMBER_STATE,     /* up, down, draining, out (with the incarnation the leader saw) */
    META_OP_SETTINGS,         /* the cluster's settings, whole */
    META_OP_VOLUME_ADD,       /* number, name, uuid, kind, storage, min ISR, replicas (with rids): epoch 1, leader rep[0] */
    META_OP_VOLUME_LEADER,    /* a grant or promotion, see struct meta_grant */
    META_OP_ISR_CHANGE,       /* if epoch == e and isr_version == v: a new ISR (holding the leader) of (node, rid) pairs */
    META_OP_REPLICA_ADD,      /* if epoch == e: a learner (replicated) or a candidate (shared) on node N, instance rid */
    META_OP_REPLICA_REMOVE,   /* if epoch == e: node N, neither the leader nor (replicated) in the ISR */
    META_OP_SLOT_HOME,        /* slots [first, first + count) without a home get home volume V */
    META_OP_USER_PUT,         /* a user's record, by name */
    META_OP_USER_DEL,         /* a user, by name */
    META_OP_MEMBER_ADDR,      /* a server's addresses as its run `incarnation` advertises them (a restart moved it) */
    META_OP_MEMBER_FLAGS,     /* a member's flags, as its run `incarnation` reports them; claim_ok 0 -> 1 clears its blocks */
    META_OP_VOLUME_SELF,      /* shared: if leader == W and epoch + 1 == E: W's own claim of its reserved epoch E */
    META_OP_VOLUME_RESERVE,   /* shared: if epoch == E: ids reserved (absolute, never lower), the positions reported */
    META_OP_VOLUME_DROP,      /* shared: if epoch == E: the drop watermark (generation and ms, never lower) */
    META_OP_CLAIM_REFUSED,    /* shared: if epoch == E: the highest epoch the claimant saw on storage */
    META_OP_CLAIM_FAILED,     /* shared: if epoch == E: the claimant (the leader) cannot claim for its own reasons */
    META_OP_VOLUME_RELOCATE,  /* shared: if epoch == E: the export the volume's storage now has (the operator's) */
    META_OP_DOMAIN_STATE,     /* a storage domain: normal, maintenance, rolled back */
    META_OP_SLOT_MOVE_PENDING, /* slot s moves from home A to B: A's owner is to freeze it (§5.7) */
    META_OP_SLOT_FREEZE,      /* if A's epoch == E_A and the move is pending: A froze slot s under E_A */
    META_OP_SLOT_MOVE,        /* a frozen slot's home becomes B (the old one joins its chain) */
    META_OP_SLOT_MOVE_CANCEL, /* a pending move is dropped (frozen or not: a frozen slot stays frozen on A's side
                                 until A's owner learns of this, which is the cluster layer's) */
    META_OP_REPLICA_OPEN,     /* replicated: replica (node, rid) opened generation gen, above the recorded one */
    META_OP_RESTORE,          /* the restore generation (`tsdb cluster restore`), above the recorded one */
    META_OP_NAME_GROUP,       /* reserved for S4's prefix groups (design-ha-modes.md M23): refused until built */
    META_OP_LAST = META_OP_NAME_GROUP,
};

/* Why a volume got a new leader (grant reasons of §5.5, in its order; 7 is the replicated mode's RESIGN). */
enum meta_grant_reason {
    META_GR_NONE = 0,
    META_GR_UNHEARD = 1,   /* the owner was not heard for lease_s */
    META_GR_DOMAIN = 2,    /* the owner's domain is down and another candidate's is up */
    META_GR_PLANNED = 3,   /* an operator or the balancer moved it */
    META_GR_FENCED = 4,    /* the owner reported FENCED, or came back new and has not claimed again */
    META_GR_REFUSED = 5,   /* a claimant reported CLAIM_REFUSED */
    META_GR_CLAIMANT = 6,  /* the granted claimant did not come up, or reported CLAIM_FAILED */
    META_GR_RESIGNED = 7,  /* the leader resigned (REPL_M_RESIGN) */
    META_GR_LAST = META_GR_RESIGNED,
};

/* What a grant of a shared volume away from a node that is not heard needs under failover = attested or bmc. */
enum meta_attest_kind { META_ATT_NONE = 0, META_ATT_OPERATOR = 1, META_ATT_BMC = 2 };
struct meta_attest {
    uint8_t kind;               /* enum meta_attest_kind */
    char who[META_NAME_MAX];    /* the operator, or the BMC's address */
    uint64_t ms;                /* the proposer's clock at the attestation (recorded, never judged) */
    uint64_t incarnation;       /* bmc: the incarnation of the old owner that was powered off */
};

struct meta_fence_reply { /* a replica's answer to a fence (design-cluster.md §6.2) */
    uint32_t node;
    uint64_t rid, durable, last_epoch;
};

/* VOLUME_LEADER. `epoch` is the volume's epoch the proposer saw (and, for a replicated volume, `isr_version` the
 * basis the fences went out for); a stale one changes nothing.
 *   replicated: new_epoch == epoch + 1; `leader` must be what repl_meta_promote picks from the replies of the
 *     `fenced` set, the new ISR and `unanswered` are its result, `start` is above the history's last start.
 *   shared: the ISR stays every candidate; a grant to another node needs new_epoch >= epoch + 2 and above
 *     highest_seen, a grant to the leader itself above max(epoch, highest_seen); new_epoch + 1 stays below
 *     META_EPOCH_LIMIT (else META_ERR_RANGE); `leader` is a candidate no CLAIM_FAILED bars (unless all are barred);
 *     with failover != lease, every grant that moves the volume to another node needs an attestation (operator, or
 *     for bmc a power-off of the owner's incarnation or later) whatever its reason, except PLANNED (the owner's
 *     release) and REFUSED (the claimant never held the lock); RESIGNED is refused (META_ERR_INVALID: the replicated
 *     mode's reason). */
struct meta_grant {
    uint32_t number;
    uint64_t epoch, new_epoch, isr_version;
    uint8_t reason; /* enum meta_grant_reason */
    uint32_t leader;
    uint64_t start;
    struct meta_positions pos;
    struct meta_attest att;
    uint32_t nfenced;
    uint32_t fenced[META_REPLICAS_MAX];
    uint32_t nreplies;
    struct meta_fence_reply reply[META_REPLICAS_MAX];
};

/* VOLUME_ADD's content. For a shared volume: st filled, rid all 0; for a replicated one st empty, rid nonzero. */
struct meta_volume_add {
    uint32_t number;
    char name[META_NAME_MAX];
    uint8_t uuid[16];
    uint8_t kind;
    struct meta_storage st;
    uint32_t min_isr;
    uint32_t n;
    uint32_t nodes[META_REPLICAS_MAX];
    uint64_t rid[META_REPLICAS_MAX];
};

struct meta_entry {
    uint8_t *buf;
    size_t len, cap;
    uint32_t nops;
};

void meta_entry_init(struct meta_entry *e);
void meta_entry_free(struct meta_entry *e);
/* The entry's bytes are e->buf[0, e->len) once meta_entry_finish has written the count. */
void meta_entry_finish(struct meta_entry *e);

void meta_op_init(struct meta_entry *e, const uint8_t uuid[16], const struct meta_settings *s,
                  const struct meta_member *first, int mode, uint32_t level);
void meta_op_member_add(struct meta_entry *e, const struct meta_member *mb);
void meta_op_member_state(struct meta_entry *e, uint32_t id, int state, uint64_t incarnation);
/* Refused as STALE for an incarnation below the member's, EXISTS for a peer address another live member has. */
void meta_op_member_addr(struct meta_entry *e, uint32_t id, uint64_t incarnation, const char *addr,
                         const char *peer);
/* STALE for an incarnation below the member's. claim_ok 0 -> 1 (the record's flag before, this one after) clears the
 * member's CLAIM_FAILED blocks; no other change of flags does. */
void meta_op_member_flags(struct meta_entry *e, uint32_t id, uint64_t incarnation, uint8_t flags);
void meta_op_settings(struct meta_entry *e, const struct meta_settings *s);
void meta_op_volume_add(struct meta_entry *e, const struct meta_volume_add *a);
void meta_op_volume_leader(struct meta_entry *e, const struct meta_grant *g);
/* `rid` parallel to `isr`; `commit` the leader's commit point (0: none; kept when given). */
void meta_op_isr_change(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t isr_version,
                        const uint32_t *isr, const uint64_t *rid, uint32_t nisr, uint64_t commit);
void meta_op_replica_add(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node, uint64_t rid);
void meta_op_replica_remove(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node);
void meta_op_slot_home(struct meta_entry *e, uint32_t first, uint32_t count, uint32_t volume);
void meta_op_user_put(struct meta_entry *e, const char *name, const void *blob, uint32_t len);
void meta_op_user_del(struct meta_entry *e, const char *name);
void meta_op_volume_self(struct meta_entry *e, uint32_t number, uint32_t node, uint64_t new_epoch);
void meta_op_volume_reserve(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t series_reserved,
                            uint32_t states_reserved, const struct meta_positions *pos);
void meta_op_volume_drop(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t wm_gen,
                         int64_t dropped_before);
void meta_op_claim_refused(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t highest_seen);
void meta_op_claim_failed(struct meta_entry *e, uint32_t number, uint64_t epoch, uint8_t reason);
void meta_op_volume_relocate(struct meta_entry *e, uint32_t number, uint64_t epoch, const char *export_path);
void meta_op_domain_state(struct meta_entry *e, const char *name, int state);
void meta_op_slot_move_pending(struct meta_entry *e, uint32_t slot, uint32_t from, uint32_t to);
void meta_op_slot_freeze(struct meta_entry *e, uint32_t slot, uint32_t from, uint64_t from_epoch);
void meta_op_slot_move(struct meta_entry *e, uint32_t slot, uint32_t to);
void meta_op_slot_move_cancel(struct meta_entry *e, uint32_t slot);
void meta_op_replica_open(struct meta_entry *e, uint32_t number, uint32_t node, uint64_t rid, uint64_t gen);
void meta_op_restore(struct meta_entry *e, uint64_t gen);
void meta_op_name_group(struct meta_entry *e); /* reserved: an empty body; applies as INVALID */

/* Apply a committed entry at `index` (`term`). Each operation's result goes to res[i] for i < nres (pass NULL, 0 to
 * ignore them). Returns META_OK, or META_ERR_FORMAT for bytes that do not decode, which leaves the state as it was:
 * the caller stops (see the head comment). */
int meta_apply(struct meta_state *m, uint64_t index, uint64_t term, const void *data, size_t len, int *res,
               uint32_t nres);
/* A Raft entry that is not a command (the no-op, a configuration: `config` in raft_config's wire form) still moves
 * the applied index; a configuration is kept for snapshots. */
void meta_apply_other(struct meta_state *m, uint64_t index, uint64_t term, const void *config, size_t config_len);

/* The whole state as bytes (checksummed): what a snapshot file, a Raft SNAPSHOT, a member's persisted copy in
 * --state-dir and the watch stream carry (one format). The caller frees *out. */
int meta_snapshot_encode(const struct meta_state *m, uint8_t **out, size_t *len);
/* Replaces `m` with the image; META_ERR_FORMAT (and `m` unchanged) when it does not verify, is not version 2, or has
 * a level above META_LEVEL. */
int meta_snapshot_decode(struct meta_state *m, const void *buf, size_t len);
/* A 64-bit hash of everything the state holds but the applied index and term: equal states, equal hashes. */
uint64_t meta_hash(const struct meta_state *m);

/* The Raft group's size for `servers` servers that are up and not out: 5 from 5 servers on (at most s->voters),
 * 3 from 3, else 1 (a group of 2 would need both). */
uint32_t meta_voter_target(const struct meta_settings *s, uint32_t servers);

/* The checker of the simulator and the tests: does `next` follow from `prev` by the rules in the head comment?
 * `prev` is the state at a lower applied index on the same history. Returns 0, or -1 with a sentence in `why`. */
int meta_check_step(const struct meta_state *prev, const struct meta_state *next, char *why, size_t n);

/* A deep copy (the simulator keeps the states of a history). */
void meta_copy(struct meta_state *dst, const struct meta_state *src);

#endif
