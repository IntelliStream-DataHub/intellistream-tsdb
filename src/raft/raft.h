/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/raft (src/raft/UPSTREAM names the commit) by tools/sync-raft.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* Raft, the sans-IO core (plan section 6).
 *
 * This file is the whole surface. Nothing behind it does I/O, reads a clock or knows what a socket
 * is: events go in through the functions below, and what the node should now do comes back out
 * of raft_next_action() in an order the caller can follow without thinking -- hard state before
 * any message of the new term, a sync before the acknowledgement that depends on it, one apply
 * batch at a time. The same core runs under the daemon and under the simulator in raft_sim.h,
 * which is the point: every election, every truncation and every snapshot handoff the daemon will
 * ever perform has been through thousands of seeded runs with the network and the disk misbehaving
 * in ways a test cluster cannot be made to reproduce on demand.
 *
 * Ownership: the caller owns the log store (a vtable, so the simulator's in-memory log and the
 * daemon's segmented file are interchangeable), the hard-state file, the state machine and the
 * transport. The core owns the protocol state and decides, and it owns the lifetime of what it
 * hands out: the entries an action names -- the bytes the log store read, and the array that
 * carries them -- stay as they are until the caller has drained the action queue, whatever the
 * caller asks of the core meanwhile (plan 5a-i-g, #201). It never allocates on the message path
 * once its buffers have grown to the size of the largest drain.
 */
#ifndef ISO_RAFT_RAFT_H
#define ISO_RAFT_RAFT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "common/util.h"

typedef uint64_t raft_index;
typedef uint64_t raft_term;
typedef uint32_t raft_id; /* the node number from the cluster map; 0 means nobody */

#define RAFT_MEMBERS_MAX 16

/* ------------------------------------------------------------------ membership */

typedef struct {
	raft_id id;
	bool learner;
} raft_member;

typedef struct {
	uint32_t n;
	raft_member m[RAFT_MEMBERS_MAX];
} raft_config;

/* Wire form of a configuration: what a CONFIG entry carries and what a snapshot image records.
 * Fixed layout so a state machine can store it without knowing anything else about Raft. */
#define RAFT_CONFIG_WIRE_MAX (4 + RAFT_MEMBERS_MAX * 5)
size_t raft_config_encode(const raft_config *c, void *buf, size_t cap);
bool raft_config_decode(const void *buf, size_t len, raft_config *out);
const raft_member *raft_config_find(const raft_config *c, raft_id id);
unsigned raft_config_voters(const raft_config *c);

/* ------------------------------------------------------------------ log */

typedef enum {
	RAFT_ENTRY_NORMAL = 0, /* a command for the state machine */
	RAFT_ENTRY_NOOP = 1,   /* the entry a new leader commits to learn its commit index */
	RAFT_ENTRY_CONFIG = 2, /* an encoded raft_config */
} raft_entry_type;

typedef struct {
	raft_index index;
	raft_term term;
	uint8_t type;
	uint32_t len;
	const void *data;
} raft_entry;

/* The log store. Indices are 1-based and contiguous from first() to last(); first()-1 is the base
 * (the snapshot the log sits on) and term_at(first()-1) is that snapshot's term. An empty
 * store at boot has first() == 1, last() == 0, term_at(0) == 0.
 *
 * append() copies the entries into memory and returns; the caller makes them durable in its own
 * time and reports it with raft_persisted(). get() fills `out` with entries [lo, hi] (inclusive)
 * stopping early once `max_bytes` of payload has been gathered (always at least one entry).
 *
 * **The data pointers get() hands out stay valid, and their bytes unchanged, until release()** --
 * across every append, truncation, reset and compaction, and every later get(). The core queues
 * actions that name those bytes (an APPEND for a follower, an APPLY for the state machine), and a
 * state machine may propose from inside an APPLY, which appends while they are still queued; a
 * snapshot taken inside an apply compacts under the next batch. release() says that nothing the
 * core handed out is named any more, and the store may reuse the memory; the core calls it when
 * its action queue is empty (plan 5a-i-g, #201).
 *
 * **A truncation that fails ends the append that asked for it** (#114, plan 28c): the core does
 * not append after it and answers nothing, since what the log holds is then not what the core
 * would say it holds; the leader sends again. **A reset that fails installs nothing** (#308):
 * no RESTORE, no answer. A store whose failures cannot be retried -- the daemon's
 * raft_log, whose failed sync may have taken a write error -- says so to its owner, which halts
 * (raft_log_fault). */
typedef struct raft_log_ops {
	raft_index (*first)(void *log);
	raft_index (*last)(void *log);
	raft_term (*term_at)(void *log, raft_index index); /* 0 when the index is not held */
	unsigned (*get)(void *log, raft_index lo, raft_index hi, size_t max_bytes, raft_entry *out,
			unsigned max_n);
	iso_err (*append)(void *log, const raft_entry *e, unsigned n);
	iso_err (*truncate)(void *log, raft_index from);      /* drop [from, last] */
	iso_err (*reset)(void *log, raft_index index, raft_term term); /* drop all; new base */
	iso_err (*compact)(void *log, raft_index upto);       /* drop [first, upto]; base = upto */
	void (*release)(void *log); /* every pointer get() has handed out is dead */
} raft_log_ops;

/* ------------------------------------------------------------------ messages */

typedef enum {
	RAFT_MSG_PREVOTE = 1,
	RAFT_MSG_PREVOTE_RESP,
	RAFT_MSG_VOTE,
	RAFT_MSG_VOTE_RESP,
	RAFT_MSG_APPEND, /* also the heartbeat: an append with no entries */
	RAFT_MSG_APPEND_RESP,
	RAFT_MSG_SNAPSHOT,
	RAFT_MSG_TIMEOUT_NOW,
	RAFT_MSG__COUNT
} raft_msg_type;

const char *raft_msg_type_name(unsigned type);

typedef struct {
	uint8_t type;
	raft_id from;
	raft_id to;
	raft_term term; /* for PREVOTE and PREVOTE_RESP: the term the candidate would start */
	union {
		struct {
			raft_index last_index;
			raft_term last_term;
			bool transfer; /* leadership transfer: stickiness does not apply */
		} vote;
		struct {
			bool granted;
		} vote_resp;
		struct {
			raft_index prev_index;
			raft_term prev_term;
			raft_index commit;
			uint32_t seq; /* echoed in the response; confirms leadership for reads */
			const raft_entry *entries;
			unsigned n;
		} append;
		struct {
			bool ok;
			raft_index index; /* ok: durably matched through here; else the rejected prev */
			raft_index hint_index; /* rejection: where the leader should probe next */
			raft_term hint_term;   /* rejection: term at the conflict, 0 if the log was short */
			uint32_t seq;
		} append_resp;
		struct {
			raft_index index;
			raft_term term;
			raft_config config;
			/* Opaque to the core. The simulator carries the image here; the daemon streams
			 * it beside the message and leaves this empty. */
			const void *data;
			size_t len;
		} snapshot;
	} u;
} raft_msg;

/* ------------------------------------------------------------------ core */

typedef enum {
	RAFT_FOLLOWER = 0,
	RAFT_PRECANDIDATE,
	RAFT_CANDIDATE,
	RAFT_LEADER,
} raft_role;

const char *raft_role_name(raft_role r);

typedef struct {
	raft_term term;
	raft_id vote;
	/* **The vote fence** (plan 5a-i-g, L's demotion's repair): while this node's log does not
	 * hold entry `fence_index` with term `fence_term` -- and its base, from a snapshot, is
	 * below `fence_index` -- it answers no PREVOTE or VOTE and never campaigns, whatever its
	 * configuration says. A reseed writes it before it empties a replica's log, with the
	 * leader's commit and that entry's term: a log that has been emptied has forgotten what it
	 * acknowledged, and a candidate on a configuration from before the replica's demotion would
	 * still count its vote. The core only carries it, in every SAVE_HARD. It lifts by itself
	 * once the log holds that entry, which is committed and is never truncated. 0 = none. */
	raft_index fence_index;
	raft_term fence_term;
} raft_hard_state;

typedef struct {
	uint64_t seed; /* election timeouts are drawn from this; the simulator sets it per node */

	uint64_t heartbeat_ns;    /* 0 selects 150 ms */
	uint64_t election_min_ns; /* 0 selects 1000 ms; the timeout is drawn from [min, 2*min) */

	unsigned max_inflight;        /* appends in flight per follower; 0 selects 16 */
	unsigned max_append_entries;  /* 0 selects 256 */
	size_t max_append_bytes;      /* 0 selects 1 MiB */
	unsigned max_apply_entries;   /* 0 selects 256 */
	size_t max_apply_bytes;       /* 0 selects 1 MiB */
	uint64_t batch_ns;            /* group commit window for proposals; 0 sends at once */
	uint64_t snapshot_resend_ns;  /* 0 selects 5 s */
	raft_index snapshot_retain;   /* entries kept below a snapshot; 0 selects 1000 */
	size_t max_uncommitted_bytes; /* proposals refused with BUSY beyond this; 0 selects 64 MiB */
	unsigned max_reads;           /* reads waiting at once; 0 selects 1024 */
	/* The shortest wait before a read round that nobody has answered is sent again to the
	 * members that have not, 0 selecting 2 ms (plan 28r). The wait actually used for a member is
	 * four times what its answers have taken, never less than this and never more than half a
	 * heartbeat, and doubles with each resend of the round. */
	uint64_t read_resend_min_ns;
	bool no_read_resend; /* default off: a round waits for the heartbeat, as it did before 28r */

	bool prevote;      /* default on; the simulator turns it off to show why it is on */
	bool check_quorum; /* default on */
	bool lease_reads;  /* default off */
	/* The largest rate error any node's clock may have against true time, in parts per million:
	 * every clock in the group is assumed to run between 1 - d and 1 + d.
	 *
	 * It is read only when `lease_reads` is on, and then it is **required** -- there is no
	 * default and `raft_new` refuses without one. A lease is held for a span of the leader's
	 * clock and honoured for a span of each follower's, so what makes the first end before the
	 * second is a margin taken out of the lease, and the size of that margin is this number.
	 * Getting it wrong is a stale read, and no value the core could pick would be a statement
	 * about the operator's hardware. `CLOCK_MONOTONIC` is slewed by the time daemon, so this is
	 * that daemon's slew ceiling and not a crystal's tolerance: 500 ppm under ntpd, 83333 ppm
	 * under chrony's default `maxslewrate`, less only where it has been capped. Plan 20f-i. */
	uint32_t max_clock_drift_ppm;
} raft_opts;

void raft_opts_defaults(raft_opts *o);

/* Everything a node needs to resume: hard state from its file, the applied index, term and
 * configuration from the state machine (the log store knows its own base). A brand-new voter
 * bootstrapping a cluster passes a configuration listing only itself; a node that is being added
 * passes an empty one and waits to be told. */
typedef struct {
	raft_hard_state hard;
	raft_index applied;
	raft_term applied_term;
	raft_config config;
} raft_boot;

typedef struct raft raft;

raft *raft_new(raft_id self, const raft_opts *opts, const raft_log_ops *ops, void *log,
	       const raft_boot *boot, char *err, size_t errsize);
void raft_free(raft *r);

/* Has a follower's acknowledged round reached `seq`, counting the wrap?
 *
 * A ReadIndex round is named by a 32-bit `seq` that goes out in an APPEND and comes back in its
 * response, and a leader confirms a read once a quorum has acknowledged the round it was put in.
 * The counter restarts with each leadership term, and on a busy leader it advances once per read
 * round trip -- tens of thousands a second on a LAN -- so a long-lived leader can run it past
 * 2^32. A plain `>=` there is not a wrong answer by a little: the acknowledged value only ever
 * rises, so once the counter has wrapped below it, *every* round reads as already acknowledged
 * for the next four billion, and reads confirm with no round at all. That is the stale read
 * ReadIndex exists to prevent.
 *
 * So the comparison is made in the sequence space, as TCP compares its own: correct while the
 * distance between the two is under 2^31, which it is by a wide margin -- a leader has one round
 * outstanding. The alternative, a 64-bit field, would be a change to the wire. */
static inline bool raft_seq_reached(uint32_t acked, uint32_t seq)
{
	return (int32_t)(acked - seq) >= 0;
}

/* Was `a` sent in a later round than `b`, in the same sequence space? 0 is no round at all -- a
 * follower answers a snapshot with it -- and is later than nothing (#205). */
static inline bool raft_seq_newer(uint32_t a, uint32_t b)
{
	return a != 0 && (int32_t)(a - b) > 0;
}

/* ---- inputs */

/* Tell the core what time it is, and nothing else: no deadline is evaluated and no action is
 * queued, so it can be called immediately before another input with nothing to drain between
 * them. The clock only ever moves forward.
 *
 * A driver should call this before every input, because the core computes its deadlines from this
 * clock and answers questions against it: a read batched at `now + read_batch_ns` on a clock that
 * only a periodic timer moves cannot come due before that timer, whatever else arrives in the
 * meantime, and a lease held until `lease_until` reads as valid for a whole tick after it has
 * expired (plan section 20f). */
void raft_clock(raft *r, uint64_t now_ns);

/* The clock, plus the deadlines that have come due: heartbeats, the election timeout, the quorum
 * check, a lapsed leadership transfer. */
void raft_tick(raft *r, uint64_t now_ns);

/* When the core next has something to do, on the same clock `raft_clock` sets. A driver that only
 * ticks periodically answers everything here no sooner than its own period; one that also arms a
 * timer here answers as soon as the core asked to be woken. UINT64_MAX when nothing is pending. */
uint64_t raft_next_deadline(const raft *r);

/* A message from a peer. Entries and snapshot data are borrowed for the call. */
void raft_step(raft *r, const raft_msg *m);

/* The caller's log is durable through `index`, whose entry has `term`. */
void raft_persisted(raft *r, raft_index index, raft_term term);
void raft_hard_saved(raft *r, const raft_hard_state *hs);
void raft_applied(raft *r, raft_index index);

/* The state machine is durable through `index` (with `term`, under `config`): the core may
 * compact the log below it (keeping `snapshot_retain` entries) and will describe this image to
 * followers that have fallen behind. A compaction that fails is the log's to say: the core goes on
 * either way, since what it dropped is below the snapshot, and the daemon's log faults and its
 * owner halts (raft_log_fault; #114, plan 28c). */
void raft_compact(raft *r, raft_index index, raft_term term, const raft_config *config);

/* The same, with an explicit number of entries to keep below the snapshot instead of
 * `snapshot_retain`.
 *
 * **The retain is a preference and it has to be able to yield.** Entries are kept below a snapshot
 * so a slightly-behind follower can be caught up by replication rather than by a whole image; that
 * is worth doing while there is room for them, and it is not worth doing when there is not. A log
 * in a fixed-size region reaches that point (plan section 23, 94-c): the caller there compacts
 * because it is nearly out of segment slots, and retaining a tenth of a ten-thousand-entry
 * interval would free nothing at all. */
void raft_compact_keeping(raft *r, raft_index index, raft_term term, const raft_config *config,
			  raft_index keep);

/* The transport gave up streaming a snapshot to `to`; the follower goes back to being probed. */
void raft_snapshot_failed(raft *r, raft_id to);

/* Leader-only inputs. NOTLEADER when this node is not the leader (or, for reads and changes, has
 * not yet committed in its term); BUSY when a transfer is in progress, a configuration change is
 * still uncommitted, or too much is uncommitted; INVAL for an impossible change. */
iso_err raft_propose(raft *r, const void *data, size_t len, raft_index *index_out);
iso_err raft_read(raft *r, uint64_t ctx);
/* A read confirmed by a quorum round whatever the lease says (plan 28s): what a pool that does not
 * rest on the clock asks for. A lone voter's round of one needs no clock and is taken as always. */
iso_err raft_read_index(raft *r, uint64_t ctx);

/* What a read may be answered on with no round at all (plan 17i-5b): this node leads, its term's
 * first entry has committed, no transfer is under way, and either the lease holds (`lease_reads`,
 * the clock `raft_clock` last gave short of `lease_until`) or the voter set is exactly this node,
 * whose reads are a round of one. `raft_read` confirms a read on exactly this, so the two cannot
 * drift. Read-only: nothing is queued and nothing changes.
 *
 * `out` is filled when it answers true: the term, `term_start` (the index of this leader's own
 * first entry of the term, below which every entry an earlier leader could have acknowledged
 * lies), and `lease_until` on the clock the driver gives the core -- UINT64_MAX for a lone voter,
 * whose answer does not run out. A driver that hands this to another thread lets that thread read
 * the clock itself against `lease_until`; nobody tells it when the lease has run out. */
typedef struct {
	raft_term term;
	raft_index term_start;
	uint64_t lease_until;
} raft_lease;

bool raft_read_lease(const raft *r, raft_lease *out);

typedef enum {
	RAFT_ADD_LEARNER,
	RAFT_ADD_VOTER, /* also promotes a learner */
	RAFT_REMOVE,
	/* A voter becomes a learner, for a member whose log is about to be emptied (a reseed, plan
	 * 5a-i-g, L's demotion): the leader restarts its progress as a new member's -- match 0, probed
	 * from the end of the log -- and counts only its answers to what it sends from now on, so
	 * neither a `match` the member no longer holds nor an answer already on its way can make it a
	 * voter again or count it toward a commit. EXISTS for a learner, NOTFOUND for a non-member,
	 * INVAL for the leader itself. */
	RAFT_DEMOTE,
} raft_change_kind;

iso_err raft_change(raft *r, raft_change_kind kind, raft_id id, raft_index *index_out);
iso_err raft_transfer(raft *r, raft_id to);

/* Start an election now, the way a TIMEOUT_NOW would: no pre-vote, and the request carries the
 * transfer flag so leader stickiness does not stand in the way. An up-to-date node wins with it;
 * an operator uses it through `isoctl mds transfer`. A non-voter ignores it. */
void raft_campaign(raft *r);

/* **The clock guard's gate** (plan 28y, section 4): while `allowed` is false this node never
 * stands -- the election timer, a TIMEOUT_NOW and raft_campaign() all pass it by -- but it still
 * votes, replicates and applies. Its owner sets it whenever the verdict changes; a new core is
 * allowed. Not hard state: a restarted node is judged again before it would stand. */
void raft_set_campaign(raft *r, bool allowed);

/* A leader gives its leadership up within its term, as a failed quorum check makes it do: its
 * pending reads fail and the others elect once their timers run out. For a leader the clock guard
 * refuses when no transfer can be made, or one lapsed. Nothing on a node that does not lead. */
void raft_step_down(raft *r);

/* ---- outputs */

typedef enum {
	RAFT_ACT_SAVE_HARD = 1, /* write u.hard durably, then raft_hard_saved() */
	RAFT_ACT_SYNC,          /* make the log durable through u.sync.index, then raft_persisted() */
	RAFT_ACT_SEND,          /* u.msg; entries borrowed until the queue is drained */
	RAFT_ACT_APPLY,         /* u.apply.entries, borrowed the same; then raft_applied() */
	RAFT_ACT_RESTORE,       /* install the snapshot image described by u.snapshot, synchronously */
	RAFT_ACT_READ_READY,    /* u.read.ctx may be served from the state machine now */
	RAFT_ACT_READ_FAILED,   /* leadership was lost; the client retries elsewhere */
	RAFT_ACT_STATE,         /* role, term or leader changed */
} raft_action_type;

typedef struct {
	raft_action_type type;
	union {
		raft_hard_state hard;
		struct {
			raft_index index;
			raft_term term;
		} sync;
		raft_msg msg;
		struct {
			const raft_entry *entries;
			unsigned n;
		} apply;
		struct {
			raft_id from;
			raft_index index;
			raft_term term;
			raft_config config;
			const void *data;
			size_t len;
		} snapshot;
		struct {
			uint64_t ctx;
		} read;
		struct {
			raft_role role;
			raft_term term;
			raft_id leader;
		} state;
	} u;
} raft_action;

/* Pops the next action. Call until it returns false after every input.
 *
 * **Entry pointers inside an action are good until this returns false**, and the entries do not
 * change until then, whatever inputs the caller makes in between (plan 5a-i-g, #201). So a caller
 * may make an input while it is consuming an action -- a state machine that proposes from inside
 * an APPLY, a snapshot taken after raft_applied() -- and the actions queued before and after it
 * keep their entries. What it must not do is drain from inside that consumption: the queue would
 * empty, and the entries of the action being consumed would be released under it. A drain asked
 * for from inside one is the outer drain's to finish. */
bool raft_next_action(raft *r, raft_action *out);

/* ---- observation */

raft_role raft_get_role(const raft *r);
raft_term raft_get_term(const raft *r);
raft_id raft_get_leader(const raft *r);
raft_index raft_get_commit(const raft *r);
raft_index raft_get_applied(const raft *r);
raft_index raft_get_last(const raft *r);
const raft_config *raft_get_config(const raft *r);
/* The index of the entry that carries raft_get_config(): 0 when it came from a snapshot or the
 * state machine, which is committed. The configuration is committed once this is at or below
 * raft_get_commit(). */
raft_index raft_get_config_index(const raft *r);
/* The term of the entry at `index` in this node's log -- the snapshot's term at its base -- and 0
 * when the log does not hold it. What a leader tells a reseed its fence is (L's demotion's
 * repair). */
raft_term raft_get_term_at(const raft *r, raft_index index);
/* The vote fence's index while it holds (raft_hard_state), 0 once it has lifted or where there is
 * none: while it is not 0 this node votes for nobody and never campaigns. */
raft_index raft_get_vote_fence(const raft *r);
bool raft_is_voter(const raft *r); /* according to the configuration this node currently holds */
/* What the log refused that the core cannot go on without (#439, minor 68): ISO_OK, or the error
 * of the append of a new leader's no-op, and then `what` (when given) gets one sentence without a
 * full stop. Kept from the first and never cleared; the node has stepped down and never
 * campaigns again. The log's own faults are raft_log_fault's; an owner asks after both and halts
 * its replica on either. */
iso_err raft_get_store_fault(const raft *r, char *what, size_t n);
/* How long since this node last heard from a leader (0 for the leader itself). A learner never
 * campaigns and so never forgets its leader on its own; this is how it, and anyone watching it,
 * can tell the leader is gone. */
uint64_t raft_get_leader_silence(const raft *r);

/* Per-follower view as the leader sees it: what the promotion policy and `isoctl mds ls` read. */
typedef struct {
	raft_id id;
	bool learner;
	raft_index match;
	raft_index next;
	bool snapshotting;
	uint64_t last_heard_ns; /* by the leader's clock; 0 = never */
	/* How long since the member last answered this leader, or -- never having answered -- since
	 * this leader began tracking it: its election, or the member's addition since. 0 for the
	 * leader itself. On the core's own clock (plan 5a-i-g, G). */
	uint64_t silence_ns;
	/* How long this member's answers to a round take, by the leader's clock: a smoothed figure,
	 * and the shortest one seen (0 = none yet), and how many rounds were answered after
	 * `RAFT_READ_SLOW_NS` (plan 28r). Rounds that were sent again to it are not sampled. */
	uint64_t ack_ns;
	uint64_t ack_min_ns;
	uint64_t acks_slow;
} raft_progress;

/* A round answered later than this is counted slow (`reads_slow`, `acks_slow`). A LAN round
 * trip is a tenth of a millisecond; ten is a hundred of them and still far below the heartbeat. */
#define RAFT_READ_SLOW_NS 10000000ull

bool raft_get_progress(const raft *r, raft_id id, raft_progress *out);

typedef struct {
	uint64_t elections_started;
	uint64_t elections_won;
	uint64_t prevotes_refused;  /* by this node, out of stickiness or log age */
	uint64_t step_downs;        /* check-quorum or a higher term */
	uint64_t appends_sent;
	uint64_t appends_rejected;  /* received rejections */
	uint64_t snapshots_sent;
	uint64_t snapshots_installed;
	uint64_t proposals;
	uint64_t reads;
	uint64_t reads_lease;
	/* ReadIndex rounds a leader sent for waiting reads (a heartbeat that happened to carry
	 * them is not one). `reads / read_rounds` is the batching ratio: one at low load, where
	 * a read starts its own round and waits for nothing, and rising with arrival rate, since
	 * a read that comes in while a round is out rides the next one (plan 20f, 92-c-ii). A
	 * ratio stuck at one under heavy load says reads are not sharing rounds. */
	uint64_t read_rounds;
	uint64_t entries_applied;
	uint64_t messages_dropped;  /* held back and then abandoned because the term moved on */
	/* Replication rounds a leader skipped because its term or vote was not yet on disk. A few
	 * at every election are ordinary -- the save is one fsync away. A number that keeps rising
	 * on a leader whose followers are not catching up says the durable write it is waiting for
	 * never came back, which is otherwise indistinguishable from a network that is not
	 * delivering. */
	uint64_t replicate_blocked;
	/* Times a leader gave up on what it had in flight to a follower and sent it again from the
	 * follower's `match` (#205): an append proven lost -- by an answer to something sent after it
	 * that does not cover it -- or a window left empty with `next` past what the follower has
	 * confirmed. A follower's crash is one; a store that keeps refusing an append, one a round.
	 * In a healthy group this stays near zero, and one that climbs with `appends_sent` while the
	 * commit crawls is the leader resending what the followers already hold. */
	uint64_t resends;
	/* **A read that waited** (plan 28r). `reads_slow` counts reads that took `RAFT_READ_SLOW_NS`
	 * or more from arriving to a quorum confirming them (reads on a lease, which wait for no
	 * quorum, are not counted), and `read_wait_max_ns` is the longest such wait of any quorum read
	 * (a lost round's read counts from its own arrival, not from the round that finally answered
	 * it). Against `reads` they say whether reads are waiting for the network (a read per hundred
	 * is the stall this section was written for) or not at all. */
	uint64_t reads_slow;
	uint64_t read_wait_max_ns;
	/* Heartbeats sent again for a round nobody had answered, to the members that had not. The
	 * rounds those belong to are `read_rounds_resent`; of those, `read_resend_won` completed on
	 * an answer that arrived later than the shortest reply the member has ever made to a resend
	 * could have (so the resend, or the loss it recovered, is what it waited for) and
	 * `read_resend_late` on one that could not have been (the first answer was merely late). */
	uint64_t read_resends;
	uint64_t read_rounds_resent;
	uint64_t read_resend_won;
	uint64_t read_resend_late;
	/* Answers this node, as a follower, held back until its log was durable (ack_append): what
	 * makes a leader's round look slow without any frame being lost. */
	uint64_t acks_deferred;
	/* Answers a debug build's seam dropped on purpose (raft_test_drop_first_read_acks). */
	uint64_t test_acks_dropped;
} raft_stats;

void raft_get_stats(const raft *r, raft_stats *out);

#ifdef ISO_DEBUG
/* Move the leader's round counter on by `n` rounds, as that many read rounds would, without
 * sending any: what a test needs to reach the counter's wrap (2^31 rounds is about a day of a
 * leader serving 25,000 read rounds a second). Debug builds only. */
void raft_test_add_rounds(raft *r, uint32_t n);
/* A network that loses, for one round in `every_n` -- a heartbeat's or a read's -- the first
 * answer of every member: what a burst that overflows a receive ring does to a leader's reads
 * (plan 28r).
 * The answers to anything sent afterwards -- a resend, the next round -- are heard. 0 = off.
 * Debug builds only. */
void raft_test_drop_first_read_acks(raft *r, unsigned every_n);
#endif

#endif /* ISO_RAFT_RAFT_H */
