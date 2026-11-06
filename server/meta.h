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
 * The rules the state enforces (and meta_check_step checks from the outside, in the simulator):
 *   - a volume's epoch only rises, by one per new leader, and the new leader was in the ISR the old epoch had (C12);
 *   - the ISR changes only by an operation that names the volume's current epoch and ISR version, always holds the
 *     leader, and never holds a server that is not one of the volume's replicas;
 *   - a learner becomes a full replica only by joining the ISR;
 *   - member ids, volume numbers and volume names are never reused.
 *
 * An entry that does not decode is not a refusal but a fault: every server must apply the same bytes the same way,
 * so a build that does not understand an entry stops (META_ERR_FORMAT) rather than skip it.
 */
#ifndef TSDB_META_H
#define TSDB_META_H

#include <stddef.h>
#include <stdint.h>

#define META_ADDR_MAX 128
#define META_RACK_MAX 64
#define META_NAME_MAX 64
#define META_REPLICAS_MAX 7 /* replication 4 plus learners during a move */
#define META_SLOTS 65536
#define META_NONE 0xffffffffu /* no volume (a slot without a home) */
#define META_VOLUME_MAX 65534 /* series64: the volume field of an id is volume + 1 below 65,536 */
#define META_USER_BLOB_MAX 4096
#define META_OPS_MAX 1024 /* operations in one entry */

enum meta_err {
    META_OK = 0,
    META_ERR_FORMAT = -1,   /* the bytes do not decode: a fault, never a refusal */
    META_ERR_EXISTS = -2,   /* the id, number or name is taken */
    META_ERR_NOTFOUND = -3, /* no such member, volume or user */
    META_ERR_STALE = -4,    /* the epoch or ISR version named is not the current one */
    META_ERR_INVALID = -5,  /* the change would break a rule above */
    META_ERR_STATE = -6,    /* not now: the cluster is (not) initialised */
};
const char *meta_strerror(int rc);

enum meta_member_state { META_UP = 0, META_DOWN = 1, META_DRAINING = 2, META_OUT = 3 };
const char *meta_member_state_name(int s);

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
};
void meta_settings_default(struct meta_settings *s);

struct meta_member {
    uint32_t id;
    uint8_t state; /* enum meta_member_state */
    uint64_t incarnation;
    char addr[META_ADDR_MAX]; /* the client address, host:port */
    char peer[META_ADDR_MAX]; /* the peer address, host:port */
    char rack[META_RACK_MAX];
};

struct meta_replica {
    uint32_t node;
    uint8_t learner;
};

struct meta_epoch {
    uint64_t epoch, start; /* the epoch and the first position its leader wrote (§6.2, C14) */
};

struct meta_volume {
    uint32_t number;
    char name[META_NAME_MAX];
    uint8_t uuid[16];
    uint32_t min_isr;
    uint32_t nrep;
    struct meta_replica rep[META_REPLICAS_MAX]; /* rep[0] is the preferred leader */
    uint32_t leader;
    uint64_t epoch;
    uint32_t nisr;
    uint32_t isr[META_REPLICAS_MAX]; /* node ids, ascending */
    uint64_t isr_version;
    uint32_t nhist;
    struct meta_epoch *hist;
};

struct meta_slot_move {
    uint32_t slot, home; /* the home the slot had before a move */
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
    struct meta_settings set;
    uint32_t next_member; /* ids are never reused */
    uint32_t nmembers, members_cap;
    struct meta_member *members; /* ascending by id */
    uint32_t next_volume;
    uint32_t nvolumes, volumes_cap;
    struct meta_volume *volumes; /* ascending by number */
    uint32_t *slot_home;         /* META_SLOTS entries */
    uint32_t nmoves, moves_cap;
    struct meta_slot_move *moves;
    uint32_t nusers, users_cap;
    struct meta_user *users; /* ascending by name */
};

void meta_init(struct meta_state *m);
void meta_free(struct meta_state *m);

const struct meta_member *meta_member(const struct meta_state *m, uint32_t id);
const struct meta_volume *meta_volume(const struct meta_state *m, uint32_t number);
int meta_volume_in_isr(const struct meta_volume *v, uint32_t node);

/* ---- operations: a builder appends them to an entry */

enum meta_op_kind {
    META_OP_INIT = 1,         /* uuid, settings, the first member */
    META_OP_MEMBER_ADD,       /* a new server: id, addresses, rack, incarnation */
    META_OP_MEMBER_STATE,     /* up, down, draining, out (with the incarnation the leader saw) */
    META_OP_SETTINGS,         /* the cluster's settings, whole */
    META_OP_VOLUME_ADD,       /* number, name, uuid, min ISR, replicas: epoch 1, leader rep[0], ISR = the replicas */
    META_OP_VOLUME_LEADER,    /* if epoch == e: leader L (in the ISR), epoch e + 1, ISR (a subset holding L), start */
    META_OP_ISR_CHANGE,       /* if epoch == e and isr_version == v: a new ISR (holding the leader) */
    META_OP_REPLICA_ADD,      /* if epoch == e: a learner on node N */
    META_OP_REPLICA_REMOVE,   /* if epoch == e: node N, neither the leader nor in the ISR */
    META_OP_SLOT_HOME,        /* slots [first, first + count) get home volume V */
    META_OP_USER_PUT,         /* a user's record, by name */
    META_OP_USER_DEL,         /* a user, by name */
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
                  const struct meta_member *first);
void meta_op_member_add(struct meta_entry *e, const struct meta_member *mb);
void meta_op_member_state(struct meta_entry *e, uint32_t id, int state, uint64_t incarnation);
void meta_op_settings(struct meta_entry *e, const struct meta_settings *s);
void meta_op_volume_add(struct meta_entry *e, uint32_t number, const char *name, const uint8_t uuid[16],
                        uint32_t min_isr, const uint32_t *nodes, uint32_t n);
void meta_op_volume_leader(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t leader,
                           const uint32_t *isr, uint32_t nisr, uint64_t start);
void meta_op_isr_change(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t isr_version,
                        const uint32_t *isr, uint32_t nisr);
void meta_op_replica_add(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node);
void meta_op_replica_remove(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node);
void meta_op_slot_home(struct meta_entry *e, uint32_t first, uint32_t count, uint32_t volume);
void meta_op_user_put(struct meta_entry *e, const char *name, const void *blob, uint32_t len);
void meta_op_user_del(struct meta_entry *e, const char *name);

/* Apply a committed entry at `index` (`term`). Each operation's result goes to res[i] for i < nres (pass NULL, 0 to
 * ignore them). Returns META_OK, or META_ERR_FORMAT for bytes that do not decode, which leaves the state as it was:
 * the caller stops (see the head comment). */
int meta_apply(struct meta_state *m, uint64_t index, uint64_t term, const void *data, size_t len, int *res,
               uint32_t nres);
/* A Raft entry that is not a command (the no-op, a configuration: `config` in raft_config's wire form) still moves
 * the applied index; a configuration is kept for snapshots. */
void meta_apply_other(struct meta_state *m, uint64_t index, uint64_t term, const void *config, size_t config_len);

/* The whole state as bytes (checksummed): what a snapshot file, a Raft SNAPSHOT and the watch stream carry. The
 * caller frees *out. */
int meta_snapshot_encode(const struct meta_state *m, uint8_t **out, size_t *len);
/* Replaces `m` with the image; META_ERR_FORMAT (and `m` unchanged) when it does not verify. */
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
