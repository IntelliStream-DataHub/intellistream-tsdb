/*
 * Copyright 2026 IntelliStream AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 *
 * Copied from intellistream-objects src/raft (src/raft/UPSTREAM names the commit) by tools/sync-raft.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* Raft core: the protocol, and nothing else (see raft.h and plan section 6). */
#include "raft/raft.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/rng.h"
#include "common/util.h"

#define INFLIGHT_RING 64
#define HELD_MAX 64
#define FENCE_WINDOW (1u << 30) /* how far behind the round counter a member's fence may lag */
#define ENTS_BLOCK 1024         /* entries in the first block of queued entry arrays */

/* ------------------------------------------------------------------ configuration */

size_t raft_config_encode(const raft_config *c, void *buf, size_t cap)
{
	size_t need = 4 + (size_t)c->n * 5;
	if (c->n > RAFT_MEMBERS_MAX || cap < need)
		return 0;
	uint8_t *p = buf;
	p[0] = 1;
	p[1] = (uint8_t)c->n;
	p[2] = p[3] = 0;
	p += 4;
	for (uint32_t i = 0; i < c->n; i++, p += 5) {
		iso_put_u32le(p, c->m[i].id);
		p[4] = c->m[i].learner ? 1 : 0;
	}
	return need;
}

bool raft_config_decode(const void *buf, size_t len, raft_config *out)
{
	const uint8_t *p = buf;
	if (len < 4 || p[0] != 1 || p[1] > RAFT_MEMBERS_MAX || len != 4 + (size_t)p[1] * 5)
		return false;
	memset(out, 0, sizeof *out);
	out->n = p[1];
	p += 4;
	for (uint32_t i = 0; i < out->n; i++, p += 5) {
		out->m[i].id = iso_get_u32le(p);
		out->m[i].learner = (p[4] & 1) != 0;
		if (out->m[i].id == 0 || (p[4] & ~1u))
			return false;
		for (uint32_t j = 0; j < i; j++)
			if (out->m[j].id == out->m[i].id)
				return false;
	}
	return true;
}

const raft_member *raft_config_find(const raft_config *c, raft_id id)
{
	for (uint32_t i = 0; i < c->n; i++)
		if (c->m[i].id == id)
			return &c->m[i];
	return NULL;
}

unsigned raft_config_voters(const raft_config *c)
{
	unsigned n = 0;
	for (uint32_t i = 0; i < c->n; i++)
		n += !c->m[i].learner;
	return n;
}

const char *raft_msg_type_name(unsigned type)
{
	static const char *const names[RAFT_MSG__COUNT] = {
		"?", "PREVOTE", "PREVOTE_RESP", "VOTE", "VOTE_RESP", "APPEND", "APPEND_RESP",
		"SNAPSHOT", "TIMEOUT_NOW",
	};
	return type < RAFT_MSG__COUNT ? names[type] : "?";
}

const char *raft_role_name(raft_role r)
{
	switch (r) {
	case RAFT_FOLLOWER: return "follower";
	case RAFT_PRECANDIDATE: return "precandidate";
	case RAFT_CANDIDATE: return "candidate";
	case RAFT_LEADER: return "leader";
	}
	return "?";
}

void raft_opts_defaults(raft_opts *o)
{
	memset(o, 0, sizeof *o);
	o->heartbeat_ns = 150ull * 1000000;
	o->election_min_ns = 1000ull * 1000000;
	o->max_inflight = 16;
	o->max_append_entries = 256;
	o->max_append_bytes = 1u << 20;
	o->max_apply_entries = 256;
	o->max_apply_bytes = 1u << 20;
	o->batch_ns = 0;
	o->snapshot_resend_ns = 5000ull * 1000000;
	o->snapshot_retain = 1000;
	o->max_uncommitted_bytes = 64u << 20;
	o->max_reads = 1024;
	o->read_resend_min_ns = 2ull * 1000000;
	o->prevote = true;
	o->check_quorum = true;
	o->lease_reads = false;
}

/* ------------------------------------------------------------------ state */

typedef enum { PROG_PROBE, PROG_REPLICATE, PROG_SNAPSHOT } prog_state;

typedef struct {
	raft_id id;
	bool learner;
	prog_state state;
	raft_index match;
	raft_index next;
	bool paused; /* PROBE: one append out, waiting for the verdict */
	raft_index inflight[INFLIGHT_RING]; /* REPLICATE: last index of each append out */
	uint32_t inflight_seq[INFLIGHT_RING]; /* ... and the round it went out in (#205) */
	unsigned in_head, in_n;
	raft_index pending_snapshot;
	uint64_t snapshot_sent_at;
	uint64_t last_heard;
	/* When this leader began tracking the member: its own election, for every member it found
	 * in the configuration, or the member's addition since. A member not heard from since is
	 * silent from here (plan 5a-i-g, G), not from the start of the process. */
	uint64_t tracked_since;
	bool active; /* answered since the last check-quorum window began */
	uint32_t acked_seq;
	bool sent_this_round;
	/* A voter made a learner (RAFT_DEMOTE) is fenced until it answers a round sent after the
	 * change: `fence_seq` is that round's `seq`, and an answer to anything older is dropped. What
	 * such an answer says is about a log the member is about to lose (plan 5a-i-g, L's
	 * demotion). The first answer that passes lifts the fence. */
	bool fenced;
	uint32_t fence_seq;
	/* How long this member takes to answer a round (plan 28r): `ack_ns` is smoothed over the
	 * rounds that were sent to it once, `ack_min_ns` is the shortest of any answer. */
	uint64_t ack_ns, ack_min_ns, acks_slow;
	uint32_t ack_noted; /* the newest round whose answer was timed */
	/* A read round nobody answered is sent again to a member that has not: `resend_at` is when
	 * (0 = not armed), `resend_n` how many times it has been this round, `resend_round` the
	 * round it was last resent in, `resend_first_at` when the first of those went out and
	 * `resend_last_at` the latest. */
	uint64_t resend_at, resend_first_at, resend_last_at;
	unsigned resend_n;
	uint32_t resend_round;
	bool test_dropped; /* the seam has taken this member's first answer to the round */
} progress;

typedef struct {
	uint64_t ctx;
	raft_index index;
	uint32_t seq; /* the round that has to come back; 0 = not sent yet */
	bool confirmed;
	uint64_t at; /* when it arrived: what `reads_slow` and `read_wait_max_ns` count from */
} read_req;

typedef struct {
	raft_action a;
	const raft_entry *ents; /* in a block of `raft.ents`; see ents_room */
	unsigned ent_n;
} act;

/* The entry arrays actions carry. A block never moves once handed out -- a new one is taken when
 * the newest has no room -- so an array an action names stays where it is until the queue has
 * been drained, whatever the driver asks of the core meanwhile (plan 5a-i-g, #201). It used to be
 * one array grown with realloc, which moved it under an APPLY whose consumer was proposing. */
typedef struct ents_block {
	struct ents_block *older;
	size_t used, cap;
	raft_entry e[];
} ents_block;

struct raft {
	raft_id self;
	raft_opts o;
	const raft_log_ops *ops;
	void *log;
	iso_rng rng;

	/* hard state */
	raft_term term;
	raft_id vote;
	raft_index fence_index; /* the vote fence (raft_hard_state): carried, never changed here */
	raft_term fence_term;
	bool campaign_refused; /* the clock guard's gate (raft_set_campaign): not hard state */
	bool hard_dirty;    /* changed since the last SAVE_HARD was emitted */
	bool hard_inflight; /* SAVE_HARD emitted, raft_hard_saved not yet seen */

	/* volatile */
	raft_role role;
	raft_id leader;
	raft_index commit;
	raft_index applied;
	raft_index handed; /* last index handed out in an APPLY */
	bool apply_inflight;
	raft_index persisted;      /* log durable through here */
	raft_index sync_requested; /* last index a SYNC was emitted for */
	/* A SYNC is out and has not been answered by raft_persisted() yet. While that is true no
	 * second one is emitted: the one in flight will be followed by another covering whatever
	 * arrived meanwhile, and one fsync for four entries is four times less waiting than four
	 * fsyncs for one entry each (plan section 19d, M14-c). Nothing is applied any earlier for
	 * it -- maybe_commit still gates on `persisted` -- so what this removes is redundant
	 * fsyncs and not durability. */
	bool sync_inflight;

	raft_index snap_index;
	raft_term snap_term;
	raft_config snap_config;

	raft_config config; /* the newest configuration in the log */
	raft_index config_index;
	raft_config applied_config; /* what the state machine holds: the fallback after a truncation */
	struct {
		bool on;
		raft_index index;
		raft_config config;
	} handed_config; /* a CONFIG entry in the outstanding apply batch */

	/* time */
	uint64_t now;
	bool ticked;
	uint64_t election_deadline;
	uint64_t heartbeat_deadline;
	bool heartbeat_due;
	uint64_t batch_deadline;
	uint64_t quorum_check_at;
	uint64_t transfer_deadline;
	uint64_t last_leader_contact;
	uint64_t round_sent_ns;
	bool round_reads;  /* the round in flight carries a read */
	bool round_done;   /* ... and a quorum has answered it */
	bool round_resent; /* ... and it was sent again to someone */
	uint64_t round_overdue_at; /* when a round no read waited on is late enough to replace (28r) */
	unsigned test_drop_every; /* raft_test_drop_first_read_acks */
	unsigned test_rounds;
	uint32_t test_drop_seq;
	uint64_t lease_until;
	uint64_t lease_ns;        /* election_min_ns scaled by the drift bound; see raft_new */
	uint64_t vote_quiet_until; /* lease reads: this node refuses to vote until here; see sticky */

	/* candidate */
	uint8_t votes[RAFT_MEMBERS_MAX]; /* 0 unknown, 1 granted, 2 refused; indexed like config */

	/* leader */
	raft_index term_start; /* index of this term's no-op */
	/* The log refused this term's no-op (#439, minor 68): the error, and the entry's index and
	 * term. Never cleared; the node stops leading and never campaigns again, and its owner halts
	 * it (raft_get_store_fault). */
	iso_err store_err;
	raft_index store_err_index;
	raft_term store_err_term;
	progress prog[RAFT_MEMBERS_MAX];
	unsigned n_prog;
	uint32_t seq;
	raft_id transfer_to;
	read_req *reads;
	unsigned n_reads;
	size_t uncommitted_bytes;

	/* follower */
	struct {
		bool on;
		raft_id to;
		raft_index index;
		uint32_t seq;
	} ack; /* the acknowledgement waiting for the log to become durable */

	/* outputs */
	raft_msg held[HELD_MAX]; /* waiting for the hard state to be saved */
	unsigned n_held;
	act *acts;
	size_t acts_n, acts_head, acts_cap;
	ents_block *ents; /* the newest first */
	bool state_dirty;

	raft_stats st;
};

static void settle(raft *r);

/* ---- small helpers */

static raft_index log_first(raft *r) { return r->ops->first(r->log); }
static raft_index log_last(raft *r) { return r->ops->last(r->log); }
static raft_term term_at(raft *r, raft_index i) { return i == 0 ? 0 : r->ops->term_at(r->log, i); }
static raft_term last_term(raft *r) { return term_at(r, log_last(r)); }

static bool is_voter_in(const raft_config *c, raft_id id)
{
	const raft_member *m = raft_config_find(c, id);
	return m && !m->learner;
}

/* **The vote fence holds** (raft_hard_state; plan 5a-i-g, L's demotion's repair): a reseed emptied
 * this node's log, and it has not yet been given back the entry the leader named then -- the
 * group's commit as of the change that made it a learner. Until it has, what it would vote with is
 * a log that has forgotten what it acknowledged, and nothing but the fence stops a candidate on a
 * configuration from before that change from counting it: two such replicas complete a majority of
 * five.
 *
 * By index *and* term. A discard that a crash cut short leaves a prefix of the old log, and when
 * the old log held a deposed leader's tail that prefix can reach past the index with another term's
 * entries. A base at or past the index means a snapshot that already covers it. */
static bool vote_fenced(raft *r)
{
	if (r->fence_index == 0)
		return false;
	raft_index first = log_first(r);
	if (first > 0 && first - 1 >= r->fence_index)
		return false;
	return term_at(r, r->fence_index) != r->fence_term;
}

static unsigned quorum(const raft *r) { return raft_config_voters(&r->config) / 2 + 1; }

static uint64_t draw_timeout(raft *r)
{
	return r->o.election_min_ns + iso_rng_below(&r->rng, r->o.election_min_ns);
}

static void reset_election_timer(raft *r)
{
	r->election_deadline = r->now + draw_timeout(r);
}

static void set_hard(raft *r, raft_term term, raft_id vote)
{
	if (r->term == term && r->vote == vote)
		return;
	r->term = term;
	r->vote = vote;
	r->hard_dirty = true;
	r->state_dirty = true;
}

/* ---- action queue */

static act *push_act(raft *r, raft_action_type type)
{
	if (r->acts_n == r->acts_cap) {
		size_t cap = r->acts_cap ? r->acts_cap * 2 : 64;
		r->acts = iso_xrealloc(r->acts, cap * sizeof *r->acts);
		r->acts_cap = cap;
	}
	act *a = &r->acts[r->acts_n++];
	memset(a, 0, sizeof *a);
	a->a.type = type;
	return a;
}

/* Room for `n` entries, where nothing already handed out lives; `ents_take` keeps what was used. */
static raft_entry *ents_room(raft *r, unsigned n)
{
	ents_block *b = r->ents;
	if (!b || b->cap - b->used < n) {
		size_t cap = n > ENTS_BLOCK ? n : ENTS_BLOCK;
		ents_block *nb = iso_xmalloc(sizeof *nb + cap * sizeof nb->e[0]);
		nb->older = b;
		nb->used = 0;
		nb->cap = cap;
		r->ents = b = nb;
	}
	return &b->e[b->used];
}

static void ents_take(raft *r, unsigned n) { r->ents->used += n; }

/* The queue is empty, so no action names an array any more. One block is kept, as large as all of
 * them together were, so that a drain the size of this one takes nothing new. */
static void ents_release(raft *r)
{
	ents_block *b = r->ents;
	if (!b)
		return;
	if (!b->older) {
		b->used = 0;
		return;
	}
	size_t cap = 0;
	while (b) {
		ents_block *older = b->older;
		cap += b->cap;
		free(b);
		b = older;
	}
	b = iso_xmalloc(sizeof *b + cap * sizeof b->e[0]);
	b->older = NULL;
	b->used = 0;
	b->cap = cap;
	r->ents = b;
}

static bool hard_pending(const raft *r) { return r->hard_dirty || r->hard_inflight; }

/* A message goes out now unless it would carry a term or vote that is not yet on disk. */
static void send(raft *r, const raft_msg *m)
{
	if (hard_pending(r)) {
		if (r->n_held == HELD_MAX) {
			r->st.messages_dropped++;
			return;
		}
		r->held[r->n_held++] = *m;
		return;
	}
	act *a = push_act(r, RAFT_ACT_SEND);
	a->a.u.msg = *m;
}

static void send_with_entries(raft *r, const raft_msg *m, const raft_entry *ents, unsigned n)
{
	/* Leaders never have hard state pending: the term was saved before the campaign. */
	act *a = push_act(r, RAFT_ACT_SEND);
	a->a.u.msg = *m;
	a->a.u.msg.u.append.entries = NULL;
	a->a.u.msg.u.append.n = n;
	a->ents = ents;
	a->ent_n = n;
}

static void fail_reads(raft *r)
{
	for (unsigned i = 0; i < r->n_reads; i++) {
		act *a = push_act(r, RAFT_ACT_READ_FAILED);
		a->a.u.read.ctx = r->reads[i].ctx;
	}
	r->n_reads = 0;
}

/* ---- configuration bookkeeping */

static progress *prog_of(raft *r, raft_id id)
{
	for (unsigned i = 0; i < r->n_prog; i++)
		if (r->prog[i].id == id)
			return &r->prog[i];
	return NULL;
}

/* A member this leader knows nothing about yet: probed from the end of the log, and silent from
 * now rather than from the start of the process. */
static void progress_start(raft *r, progress *p, raft_id id)
{
	memset(p, 0, sizeof *p);
	p->id = id;
	p->state = PROG_PROBE;
	p->next = log_last(r) + 1;
	p->tracked_since = r->now;
	if (id == r->self)
		p->match = r->persisted;
}

/* Rebuild the leader's progress table from the configuration, keeping what is known about
 * members that stay.
 *
 * **Except about a voter that has just become a learner** (RAFT_DEMOTE, plan 5a-i-g, L's
 * demotion). It is demoted because its log is about to be emptied, and what the leader knows of it
 * -- its `match` above all -- is about to be untrue. Kept, that `match` would pass the promotion's
 * "has it caught up" at the next housekeeping, and the member would be a voter again, counted at a
 * `match` it no longer holds, before its log had even gone. So it starts again as a new member,
 * and is fenced: answers to what the leader sent it before the change are dropped, and the first
 * answer that counts is one to the round `raft_change` starts with it. This core's analogue of
 * etcd giving a re-added member a new ID. */
static void rebuild_progress(raft *r)
{
	progress old[RAFT_MEMBERS_MAX];
	unsigned n_old = r->n_prog;
	memcpy(old, r->prog, n_old * sizeof old[0]);
	r->n_prog = 0;
	for (uint32_t i = 0; i < r->config.n; i++) {
		progress *p = &r->prog[r->n_prog++];
		const raft_member *m = &r->config.m[i];
		unsigned j;
		for (j = 0; j < n_old; j++)
			if (old[j].id == m->id)
				break;
		if (j < n_old && !(m->learner && !old[j].learner && m->id != r->self)) {
			*p = old[j];
		} else {
			progress_start(r, p, m->id);
			if (j < n_old) {
				/* The round the change starts: see raft_change. 0 means "no round yet"
				 * to a queued read, so the counter skips it, and so does the fence. */
				p->fenced = true;
				p->fence_seq = r->seq + 1 ? r->seq + 1 : 1;
			}
		}
		p->learner = m->learner;
	}
}

static void adopt_config(raft *r, const raft_config *c, raft_index index)
{
	r->config = *c;
	r->config_index = index;
	if (r->role == RAFT_LEADER)
		rebuild_progress(r);
}

/* The newest CONFIG entry in the log wins; below the applied index the state machine's copy is
 * the same thing, so only entries above it can change the answer. */
static void reload_config(raft *r, const raft_config *fallback)
{
	raft_index lo = ISO_MAX(log_first(r), r->applied + 1), hi = log_last(r);
	raft_config found;
	raft_index found_at = 0;
	while (lo <= hi) {
		/* Scratch: read and done with before anything is queued, so never taken. */
		raft_entry *es = ents_room(r, 256);
		unsigned n = r->ops->get(r->log, lo, hi, 1u << 20, es, 256);
		if (n == 0)
			break;
		for (unsigned i = 0; i < n; i++) {
			const raft_entry *e = &es[i];
			if (e->type == RAFT_ENTRY_CONFIG && raft_config_decode(e->data, e->len, &found))
				found_at = e->index;
		}
		lo = es[n - 1].index + 1;
	}
	if (found_at)
		adopt_config(r, &found, found_at);
	else
		adopt_config(r, fallback, 0);
}

/* ---- role changes */

static void become_follower(raft *r, raft_term term, raft_id leader)
{
	bool was_leader = r->role == RAFT_LEADER;
	if (term > r->term)
		set_hard(r, term, 0);
	if (r->role != RAFT_FOLLOWER || r->leader != leader)
		r->state_dirty = true;
	r->role = RAFT_FOLLOWER;
	r->leader = leader;
	r->transfer_to = 0;
	r->heartbeat_due = false;
	r->batch_deadline = 0;
	if (was_leader) {
		fail_reads(r);
		r->n_prog = 0;
		r->uncommitted_bytes = 0;
	}
	reset_election_timer(r);
}

static void become_leader(raft *r)
{
	r->role = RAFT_LEADER;
	r->leader = r->self;
	r->state_dirty = true;
	r->last_leader_contact = r->now;
	r->n_prog = 0;
	rebuild_progress(r);
	r->transfer_to = 0;
	r->n_reads = 0;
	r->uncommitted_bytes = 0;
	/* The round counter restarts with the progress it is compared against: `become_leader`
	 * zeroed `n_prog` above, so `rebuild_progress` built every entry fresh and every
	 * `acked_seq` is 0. Sharing an origin per term is what keeps the wrap four billion
	 * rounds away from wherever this term starts rather than from the process's. */
	r->seq = 0;
	r->heartbeat_deadline = r->now + r->o.heartbeat_ns;
	r->heartbeat_due = false;
	r->quorum_check_at = r->now + r->o.election_min_ns;
	r->lease_until = 0;
	r->batch_deadline = 0;
	r->round_overdue_at = 0;
	for (unsigned i = 0; i < r->n_prog; i++)
		r->prog[i].active = false;

	raft_entry e = { .index = log_last(r) + 1, .term = r->term, .type = RAFT_ENTRY_NOOP };
	r->term_start = e.index;
	iso_err rc = r->ops->append(r->log, &e, 1);
	if (rc != ISO_OK) {
		/* **A leader whose log refuses its term's no-op does not lead** (#439, minor 68). Until
		 * that entry commits it may serve no read, and nothing else would ever append it: the
		 * node held the term with heartbeats, kept its followers from electing anyone else,
		 * and answered no read until a write happened to fill the gap -- on a store that had
		 * just failed one. A refusal the log took back cleanly does not fault the log
		 * (raft_log_fault), so the core keeps it: it steps down, never campaigns again, and
		 * its owner halts the replica on it, the #117 rule for a store fault. */
		r->store_err = rc;
		r->store_err_index = e.index;
		r->store_err_term = r->term;
		become_follower(r, r->term, 0);
		return;
	}
	/* Counted once the term has its no-op: a leadership refused at once is no election won. */
	r->st.elections_won++;
}

static bool can_campaign(raft *r)
{
	return r->config.n > 0 && is_voter_in(&r->config, r->self) && !vote_fenced(r) &&
	       !r->campaign_refused && r->store_err == ISO_OK;
}

static void send_vote_requests(raft *r, bool prevote, bool transfer)
{
	raft_msg m = { .type = prevote ? RAFT_MSG_PREVOTE : RAFT_MSG_VOTE,
		       .from = r->self,
		       .term = prevote ? r->term + 1 : r->term };
	m.u.vote.last_index = log_last(r);
	m.u.vote.last_term = last_term(r);
	m.u.vote.transfer = transfer;
	for (uint32_t i = 0; i < r->config.n; i++) {
		if (r->config.m[i].learner || r->config.m[i].id == r->self)
			continue;
		m.to = r->config.m[i].id;
		send(r, &m);
	}
}

static void tally_self(raft *r)
{
	memset(r->votes, 0, sizeof r->votes);
	for (uint32_t i = 0; i < r->config.n; i++)
		if (r->config.m[i].id == r->self)
			r->votes[i] = 1;
}

static void become_candidate(raft *r, bool transfer)
{
	set_hard(r, r->term + 1, r->self);
	r->role = RAFT_CANDIDATE;
	r->leader = 0;
	r->state_dirty = true;
	r->st.elections_started++;
	tally_self(r);
	reset_election_timer(r);
	if (quorum(r) == 1) {
		become_leader(r);
		return;
	}
	send_vote_requests(r, false, transfer);
}

static void become_precandidate(raft *r)
{
	if (quorum(r) == 1) {
		become_candidate(r, false);
		return;
	}
	r->role = RAFT_PRECANDIDATE;
	r->leader = 0;
	r->state_dirty = true;
	tally_self(r);
	reset_election_timer(r);
	send_vote_requests(r, true, false);
}

static void campaign(raft *r)
{
	if (r->o.prevote)
		become_precandidate(r);
	else
		become_candidate(r, false);
}

/* ---- log helpers */

static bool up_to_date(raft *r, raft_index index, raft_term term)
{
	raft_term mine = last_term(r);
	return term > mine || (term == mine && index >= log_last(r));
}

/* False when the log's truncation failed (#114, plan 28c). What the core knows of the log is
 * lowered all the same -- a lower `persisted` claims less, never more -- and the caller then stops:
 * the log holds what it held, or part of it, and nothing appended after it would sit where the core
 * thinks. */
static bool truncate_log(raft *r, raft_index from)
{
	iso_err rc = r->ops->truncate(r->log, from);
	if (r->persisted >= from)
		r->persisted = from - 1;
	if (r->sync_requested >= from) {
		r->sync_requested = from - 1;
		/* And the sync that was out for the entries just thrown away will never be
		 * answered for an index this log still has. Leaving the flag set would mean no
		 * SYNC is ever emitted again: healthy nodes, a group that never persists. */
		r->sync_inflight = false;
	}
	if (r->ack.on && r->ack.index >= from)
		r->ack.index = from - 1;
	if (r->config_index >= from)
		reload_config(r, &r->applied_config);
	return rc == ISO_OK;
}

/* ---- commit and apply */

static void hand_out_apply(raft *r)
{
	if (r->apply_inflight || r->handed >= r->commit)
		return;
	raft_index lo = r->handed + 1, hi = r->commit;
	if (hi > log_last(r))
		hi = log_last(r); /* a restarted node whose volatile tail is being re-sent */
	if (lo > hi)
		return;
	unsigned max_n = r->o.max_apply_entries;
	raft_entry *es = ents_room(r, max_n);
	unsigned n = r->ops->get(r->log, lo, hi, r->o.max_apply_bytes, es, max_n);
	if (n == 0)
		return;
	ents_take(r, n);
	act *a = push_act(r, RAFT_ACT_APPLY);
	a->ents = es;
	a->ent_n = n;
	r->handed = es[n - 1].index;
	r->apply_inflight = true;
	for (unsigned i = 0; i < n; i++) {
		const raft_entry *e = &es[i];
		raft_config c;
		if (e->type == RAFT_ENTRY_CONFIG && raft_config_decode(e->data, e->len, &c)) {
			r->handed_config.on = true;
			r->handed_config.index = e->index;
			r->handed_config.config = c;
		}
	}
	r->st.entries_applied += n;
	if (r->role == RAFT_LEADER) {
		for (unsigned i = 0; i < n; i++) {
			size_t len = es[i].len;
			r->uncommitted_bytes -= ISO_MIN(len, r->uncommitted_bytes);
		}
	}
}

static int cmp_index_desc(const void *a, const void *b)
{
	raft_index x = *(const raft_index *)a, y = *(const raft_index *)b;
	return x > y ? -1 : x < y;
}

static void check_reads(raft *r);

static void maybe_commit(raft *r)
{
	if (r->role != RAFT_LEADER)
		return;
	raft_index matches[RAFT_MEMBERS_MAX];
	unsigned n = 0;
	for (unsigned i = 0; i < r->n_prog; i++) {
		progress *p = &r->prog[i];
		if (p->learner)
			continue;
		matches[n++] = p->id == r->self ? r->persisted : p->match;
	}
	unsigned q = quorum(r);
	if (n < q)
		return;
	qsort(matches, n, sizeof matches[0], cmp_index_desc);
	raft_index cand = matches[q - 1];
	if (cand > r->commit && term_at(r, cand) == r->term) {
		r->commit = cand;
		check_reads(r);
	}
}

/* ---- reads */

static bool quorum_acked(const raft *r, uint32_t seq)
{
	unsigned n = 0;
	for (unsigned i = 0; i < r->n_prog; i++) {
		const progress *p = &r->prog[i];
		if (p->learner)
			continue;
		if (p->id == r->self || raft_seq_reached(p->acked_seq, seq))
			n++;
	}
	return n >= quorum(r);
}

/* The queue is in the order the reads came, and the first one whose round has not come back holds
 * every read behind it (the loop in check_reads stops there). That is right for reads that wait for
 * the same kind of proof; it is wrong for a read the lease already confirmed, queued behind a
 * `raft_read_index` read (a pool with `read_lease = off`, plan 28s) whose acks are lost: the lease
 * read has everything it needs, and must not wait 150 ms for a round that is none of its business.
 * Each read is linearized at its own instant (28s section 1), so releasing it ahead of an earlier
 * one that is still waiting reorders nothing a client can see: the two overlap. This takes out, in
 * their order, the confirmed reads past the first read that is not yet confirmed; the loop below
 * then does what it always did with the rest. */
static void release_confirmed_behind_a_round(raft *r)
{
	bool held = false;
	unsigned keep = 0;
	for (unsigned i = 0; i < r->n_reads; i++) {
		read_req *q = &r->reads[i];
		if (held && q->confirmed && r->applied >= q->index) {
			act *a = push_act(r, RAFT_ACT_READ_READY);
			a->a.u.read.ctx = q->ctx;
			continue;
		}
		if (!q->confirmed && (q->seq == 0 || !quorum_acked(r, q->seq)))
			held = true;
		r->reads[keep++] = *q;
	}
	r->n_reads = keep;
}

static void check_reads(raft *r)
{
	release_confirmed_behind_a_round(r);
	unsigned done = 0;
	for (unsigned i = 0; i < r->n_reads; i++) {
		read_req *q = &r->reads[i];
		if (!q->confirmed) {
			if (q->seq == 0 || !quorum_acked(r, q->seq))
				break;
			q->confirmed = true;
			uint64_t took = r->now > q->at ? r->now - q->at : 0;
			if (took >= RAFT_READ_SLOW_NS)
				r->st.reads_slow++;
			if (took > r->st.read_wait_max_ns)
				r->st.read_wait_max_ns = took;
		}
		if (r->applied < q->index)
			break;
		act *a = push_act(r, RAFT_ACT_READ_READY);
		a->a.u.read.ctx = q->ctx;
		done++;
	}
	if (done) {
		memmove(r->reads, r->reads + done, (r->n_reads - done) * sizeof r->reads[0]);
		r->n_reads -= done;
	}
}

/* A lease is the promise that no other leader can be elected before it expires, which rests on
 * followers refusing to vote until their election timeout has run. A leadership transfer voids
 * that premise on purpose -- it asks a follower to campaign now and the others to vote at once
 * (see sticky()) -- so from the moment one starts this leader has no lease, and must not grow one
 * back from the acks that keep arriving while the hand-over is in flight. Reads fall back to the
 * quorum round, which a deposed leader cannot win.
 *
 * The lease runs from when the round was *sent*, not from when the acks came back, and it lasts
 * `lease_ns` rather than a whole election timeout: this leader measures the lease on its own clock
 * and the followers measure their refusal on theirs, and `lease_ns` is what makes the first end
 * before the second does whatever the drift between them (raft_new, plan section 20f-i). */
static void update_lease(raft *r)
{
	if (r->transfer_to)
		return;
	if (r->seq && quorum_acked(r, r->seq))
		r->lease_until = r->round_sent_ns + r->lease_ns;
}

/* ---- leader: replication */

static void inflight_push(progress *p, raft_index last, uint32_t seq)
{
	unsigned at = (p->in_head + p->in_n) % INFLIGHT_RING;
	p->inflight[at] = last;
	p->inflight_seq[at] = seq;
	p->in_n++;
}

static void inflight_retire(progress *p, raft_index upto)
{
	while (p->in_n && p->inflight[p->in_head] <= upto) {
		p->in_head = (p->in_head + 1) % INFLIGHT_RING;
		p->in_n--;
	}
}

static void send_heartbeat(raft *r, progress *p)
{
	raft_msg m = { .type = RAFT_MSG_APPEND, .from = r->self, .to = p->id, .term = r->term };
	m.u.append.prev_index = p->match;
	m.u.append.prev_term = term_at(r, p->match);
	m.u.append.commit = ISO_MIN(r->commit, p->match);
	m.u.append.seq = r->seq;
	send(r, &m);
	p->sent_this_round = true;
}

static void send_snapshot(raft *r, progress *p)
{
	raft_msg m = { .type = RAFT_MSG_SNAPSHOT, .from = r->self, .to = p->id, .term = r->term };
	m.u.snapshot.index = r->snap_index;
	m.u.snapshot.term = r->snap_term;
	m.u.snapshot.config = r->snap_config;
	send(r, &m);
	p->state = PROG_SNAPSHOT;
	p->pending_snapshot = r->snap_index;
	p->snapshot_sent_at = r->now;
	p->in_n = 0;
	p->paused = false;
	p->sent_this_round = true;
	r->st.snapshots_sent++;
}

/* One append from p->next. Returns false when the follower needs a snapshot instead. */
static bool send_append(raft *r, progress *p)
{
	raft_index first = log_first(r), last = log_last(r);
	if (p->next < first) {
		send_snapshot(r, p);
		return false;
	}
	raft_msg m = { .type = RAFT_MSG_APPEND, .from = r->self, .to = p->id, .term = r->term };
	m.u.append.prev_index = p->next - 1;
	m.u.append.prev_term = term_at(r, p->next - 1);
	m.u.append.seq = r->seq;
	unsigned n = 0;
	raft_entry *es = NULL;
	if (p->next <= last) {
		es = ents_room(r, r->o.max_append_entries);
		n = r->ops->get(r->log, p->next, last, r->o.max_append_bytes, es,
				r->o.max_append_entries);
		ents_take(r, n);
	}
	raft_index sent_last = n ? es[n - 1].index : p->next - 1;
	m.u.append.commit = ISO_MIN(r->commit, sent_last);
	send_with_entries(r, &m, es, n);
	p->next = sent_last + 1;
	p->sent_this_round = true;
	r->st.appends_sent++;
	if (p->state == PROG_PROBE)
		p->paused = true;
	else
		inflight_push(p, sent_last, r->seq);
	return true;
}

/* **A fence has to keep inside the counter's window** (L's demotion's repair). The comparison is
 * made in the sequence space, which is right while the two are less than 2^31 apart, and a demoted
 * member can stay silent for longer than that -- a halted learner whose reseed is run the next day,
 * on a leader serving 25,000 read rounds a second. Its first answer would then read as older than
 * its fence, and so would every answer after it: a member the leader never hears again. So a fence
 * is kept at most 2^30 rounds behind the counter. What it drops is still every answer to a round
 * sent before the change, which was more than 2^30 rounds ago. A fence on the round the change is
 * about to start is ahead of the counter, and stays where it is. */
static void keep_fences_in_window(raft *r)
{
	for (unsigned i = 0; i < r->n_prog; i++) {
		progress *p = &r->prog[i];
		if (p->fenced && raft_seq_reached(r->seq, p->fence_seq) &&
		    r->seq - p->fence_seq > FENCE_WINDOW)
			p->fence_seq = r->seq - FENCE_WINDOW;
	}
}

/* ---- a read round nobody has answered (plan 28r)
 *
 * A read is confirmed once a quorum has answered a round sent after it arrived, and a round is
 * answered or it is not: when the answers do not come (a burst of traffic overflowed a receive
 * ring, a segment was lost on a connection with nothing behind it) the next thing that moves is the
 * heartbeat, 150 ms on, and every read in the group waits for it together -- the next round starts
 * only when this one is answered.
 *
 * So a round that carries a read is sent again, to the voters that have not answered it, after a
 * wait of four times what their answers have been taking (at least `read_resend_min_ns`, at most
 * half a heartbeat), then after twice that, and so on: one frame per member per wait, and only
 * while a read depends on it.
 *
 * **What the resend carries is the round's own number, and the round's clock does not move.** An
 * answer to it is an answer to the round: the member took a message from this leader, in this
 * term, after the round was first sent, and so after every read the round holds had arrived -- the
 * only thing a ReadIndex round has to show, and the same whether the answer is to the first
 * frame, to the resend, or to the first frame arriving late. `round_sent_ns` stays where the
 * round began, which is what keeps a lease honest (update_lease counts from it): a lease that
 * started at the resend would outlast what the member promised when it heard the first frame.
 * Nor is the round's number advanced, so the cohort is unchanged and a read that arrives meanwhile
 * still waits for the next round, as before. Election timing is not touched: a follower that
 * hears a leader more often than it must is no more likely to campaign. */

static bool read_waiting(const raft *r)
{
	for (unsigned i = 0; i < r->n_reads; i++)
		if (!r->reads[i].confirmed)
			return true;
	return false;
}

/* A member a resend is worth sending to: a voter that has been heard from lately. One silent for
 * two heartbeats is down as far as this knows, and the heartbeat keeps trying it. Not a learner (it
 * is not counted for a read), not a fenced member (the heartbeat lifts its fence, and a resend is
 * not worth the draws it would add to the churn of a rebuild) and not one being sent a snapshot
 * (it answers nothing else until the snapshot is applied). */
static bool resend_member(const raft *r, const progress *p)
{
	if (p->id == r->self || p->learner || p->fenced || p->state == PROG_SNAPSHOT)
		return false;
	uint64_t since = p->last_heard ? p->last_heard : p->tracked_since;
	return r->now <= since || r->now - since <= 2 * r->o.heartbeat_ns;
}

static uint64_t resend_after(const raft *r, const progress *p, unsigned n)
{
	uint64_t base = 4 * p->ack_ns;
	if (base < r->o.read_resend_min_ns)
		base = r->o.read_resend_min_ns;
	if (base > r->o.heartbeat_ns / 2)
		base = r->o.heartbeat_ns / 2;
	/* No cap at the heartbeat: the heartbeat starts a round of its own, which arms its own. */
	return base << ISO_MIN(n, 6u);
}

/* A round has just been sent: arm the resends of the members that might need one. Whether a read
 * waits on it is not asked here: `resend_round` runs at the end of the same `replicate` and lets
 * go of every resend when no read waits, so a round nobody waits on arms nothing that outlives
 * the call. */
static void arm_resends(raft *r)
{
	bool wait = !r->o.no_read_resend;
	r->round_overdue_at = 0;
	for (unsigned i = 0; i < r->n_prog; i++) {
		progress *p = &r->prog[i];
		p->resend_n = 0;
		p->resend_at = wait && resend_member(r, p) ? r->now + resend_after(r, p, 0) : 0;
		/* The first of them is when a round that carries no read is late: see
		 * `idle_round_overdue`. */
		if (p->resend_at && (!r->round_overdue_at || p->resend_at < r->round_overdue_at))
			r->round_overdue_at = p->resend_at;
	}
}

/* A round sent with no read waiting (a heartbeat) whose answers are late, and a read that arrived
 * behind it. The read cannot ride that round -- an answer to it proves nothing about a read that
 * arrived after it was sent, so resending it would not help -- and it would otherwise wait for the
 * answers or for the heartbeat 150 ms on. So a round of its own, with a new number, goes out as
 * soon as the first member is overdue by the same measure a resend uses; it carries the read, and
 * from then on a lost answer is the resend's business. One such round at most per read wait: the
 * round it starts carries a read, so this is false for it. */
static bool idle_round_overdue(const raft *r)
{
	return r->round_overdue_at && !r->round_reads && r->now >= r->round_overdue_at;
}

static void resend_round(raft *r)
{
	bool armed = false;
	for (unsigned i = 0; i < r->n_prog; i++)
		armed |= r->prog[i].resend_at != 0;
	if (!armed)
		return;
	bool done = !read_waiting(r) || quorum_acked(r, r->seq);
	for (unsigned i = 0; i < r->n_prog; i++) {
		progress *p = &r->prog[i];
		if (!p->resend_at || (!done && r->now < p->resend_at))
			continue;
		if (done || !resend_member(r, p)) { /* an answer cleared `resend_at` (note_answer) */
			p->resend_at = 0;
			continue;
		}
		send_heartbeat(r, p);
		r->st.read_resends++;
		if (!r->round_resent) {
			r->round_resent = true;
			r->st.read_rounds_resent++;
		}
		if (p->resend_round != r->seq)
			p->resend_first_at = r->now;
		p->resend_round = r->seq;
		p->resend_last_at = r->now;
		p->resend_n++;
		p->resend_at = r->now + resend_after(r, p, p->resend_n);
	}
}

/* A member has answered round `seq`. What is learned from it: how long the member takes (a round
 * sent to it again is timed from its latest resend, see below), and, when this answer is the one
 * that completes the round, whether a resend won it. A read's own wait is told in `check_reads`,
 * from its arrival. */
static void note_answer(raft *r, progress *p, uint32_t seq)
{
	if (seq != r->seq || p->ack_noted == seq)
		return;
	p->ack_noted = seq;
	p->resend_at = 0;
	uint64_t took = r->now > r->round_sent_ns ? r->now - r->round_sent_ns : 0;
	if (took >= RAFT_READ_SLOW_NS)
		p->acks_slow++;
	/* A round that was sent again to this member is answered by either frame, so the time since
	 * the round was sent is not the member's pace. The time since the latest resend is the
	 * pace when the answer is to the resend (the case the resend exists for: the first frame
	 * was lost) and falls short of it when the answer is to the first frame (a member slower
	 * than the resend wait). It is the sample either way: a shortfall lengthens nothing and
	 * is made good in the next round, which waits 4x the sample and so is not resent, and a
	 * sample of the whole time instead -- the wait plus the pace -- would grow the estimate
	 * by a third at every lost round, a ratchet that ends at the heartbeat. `ack_min_ns`,
	 * the fastest answer ever seen, which tells a resend that won from one that was late, is
	 * taught by unsent rounds only. */
	bool resent = p->resend_round == seq;
	if (resent)
		took = r->now > p->resend_last_at ? r->now - p->resend_last_at : 0;
	else if (!p->ack_min_ns || took < p->ack_min_ns)
		p->ack_min_ns = took;
	if (!p->ack_ns) {
		p->ack_ns = ISO_MIN(took, r->o.heartbeat_ns / 8);
	} else {
		/* A stall must not teach the member's pace: one sample moves it at most 4x. */
		uint64_t sample = ISO_MIN(took, 4 * p->ack_ns);
		p->ack_ns = p->ack_ns + sample / 8 - p->ack_ns / 8;
	}
	if (r->round_done || !r->round_reads || !quorum_acked(r, seq))
		return;
	r->round_done = true;
	if (r->round_resent) {
		if (p->resend_round == seq && r->now - p->resend_first_at >= p->ack_min_ns)
			r->st.read_resend_won++;
		else
			r->st.read_resend_late++;
	}
}

static void replicate(raft *r)
{
	if (hard_pending(r)) {
		r->st.replicate_blocked++;
		return; /* a single-voter leader with learners: its term is not on disk yet */
	}
	bool round = false;
	if (r->heartbeat_due) {
		round = true;
	} else if (r->n_reads && (quorum_acked(r, r->seq) || idle_round_overdue(r))) {
		/* At most one round in flight, and the cohort of a round is every read queued
		 * when it was sent: a read that arrives while one is out keeps `seq == 0` and
		 * rides the next, which starts the moment the acks for this one land. The same
		 * shape M14-c gave the log sync and 92-b the chunk sync (plan 20f). */
		for (unsigned i = 0; i < r->n_reads; i++)
			if (r->reads[i].seq == 0) {
				round = true;
				break;
			}
	}
	if (round) {
		if (!r->heartbeat_due)
			r->st.read_rounds++; /* a round this leader would not have sent otherwise */
		if (++r->seq == 0)
			r->seq = 1; /* 0 means "no round yet" on a queued read */
		keep_fences_in_window(r);
		r->round_sent_ns = r->now;
		r->heartbeat_deadline = r->now + r->o.heartbeat_ns;
		r->heartbeat_due = false;
		for (unsigned i = 0; i < r->n_reads; i++)
			if (r->reads[i].seq == 0)
				r->reads[i].seq = r->seq;
		r->round_reads = read_waiting(r);
		r->round_done = r->round_resent = false;
		arm_resends(r);
#ifdef ISO_DEBUG
		r->test_drop_seq = 0;
		if (r->test_drop_every && ++r->test_rounds % r->test_drop_every == 0) {
			r->test_drop_seq = r->seq;
			for (unsigned i = 0; i < r->n_prog; i++)
				r->prog[i].test_dropped = false;
		}
#endif
	}
	bool window_open = r->batch_deadline == 0 || r->now >= r->batch_deadline;
	raft_index last = log_last(r);
	for (unsigned i = 0; i < r->n_prog; i++) {
		progress *p = &r->prog[i];
		if (p->id == r->self)
			continue;
		p->sent_this_round = false;
		switch (p->state) {
		case PROG_SNAPSHOT:
			if (r->now - p->snapshot_sent_at >= r->o.snapshot_resend_ns)
				send_snapshot(r, p);
			break;
		case PROG_PROBE:
			if (!p->paused && (window_open || p->next > last || round))
				send_append(r, p);
			break;
		case PROG_REPLICATE:
			/* Nothing in flight, and `next` past what the follower has confirmed: every
			 * append that moved `next` was lost.
			 *
			 * `next` is advanced when an append is *sent*, not when it is acknowledged, so
			 * a lost one leaves it ahead of anything the follower holds. Only a rejection
			 * lowers it again -- and the heartbeat that follows names `match` as its
			 * previous entry, which the follower does hold, so it is accepted rather than
			 * rejected and confirms nothing. With `next` past the end of the log the loop
			 * below has nothing in range to send, so the leader falls silent: both nodes
			 * healthy, both counters still, for as long as nobody proposes anything. A busy
			 * log hides it entirely, since the next proposal lands at `next` and *is*
			 * rejected; the metadata group of an idle cluster does not, and every VM run
			 * wedged with its third node at match 0 and the group one voter short.
			 *
			 * With the pipeline empty there is nothing to be optimistic about, so resume
			 * from what was acknowledged. What leaves it empty with `next` ahead is a probe
			 * that was out when an answer moved the follower to replicating: the probe is
			 * not in the ring. A window with appends in it is repaired where an answer
			 * proves one lost, in `on_append_resp` (#205). */
			if (p->in_n == 0 && p->next > p->match + 1) {
				p->next = p->match + 1;
				r->st.resends++;
			}
			while (p->in_n < r->o.max_inflight && p->next <= last) {
				bool full = last - p->next + 1 >= r->o.max_append_entries;
				if (!window_open && !full)
					break;
				if (!send_append(r, p))
					break;
			}
			break;
		}
		if (round && !p->sent_this_round)
			send_heartbeat(r, p);
	}
	if (window_open)
		r->batch_deadline = 0;
	resend_round(r);
	/* A group of one confirms its own round: nobody will answer to trigger the check. */
	if (round)
		check_reads(r);
}

static void check_quorum(raft *r)
{
	unsigned n = 0;
	for (unsigned i = 0; i < r->n_prog; i++) {
		progress *p = &r->prog[i];
		if (p->learner)
			continue;
		if (p->id == r->self || p->active)
			n++;
		p->active = false;
	}
	r->quorum_check_at = r->now + r->o.election_min_ns;
	if (n < quorum(r)) {
		r->st.step_downs++;
		become_follower(r, r->term, 0);
	}
}

static void maybe_transfer(raft *r, progress *p)
{
	if (r->transfer_to != p->id || p->match != log_last(r))
		return;
	raft_msg m = { .type = RAFT_MSG_TIMEOUT_NOW, .from = r->self, .to = p->id, .term = r->term };
	send(r, &m);
}

/* ---- follower: acknowledgements */

static void ack_append(raft *r, raft_id to, raft_index matched, uint32_t seq)
{
	if (r->persisted >= matched) {
		raft_msg m = { .type = RAFT_MSG_APPEND_RESP, .from = r->self, .to = to, .term = r->term };
		m.u.append_resp.ok = true;
		m.u.append_resp.index = matched;
		m.u.append_resp.seq = seq;
		send(r, &m);
		return;
	}
	/* Answer once the entries are on disk; the newest request supersedes older ones. */
	r->st.acks_deferred++;
	r->ack.on = true;
	r->ack.to = to;
	r->ack.index = matched;
	r->ack.seq = seq;
}

static void reject_append(raft *r, const raft_msg *in)
{
	raft_msg m = { .type = RAFT_MSG_APPEND_RESP, .from = r->self, .to = in->from, .term = r->term };
	raft_index prev = in->u.append.prev_index, last = log_last(r);
	m.u.append_resp.ok = false;
	m.u.append_resp.index = prev;
	m.u.append_resp.seq = in->u.append.seq;
	if (prev > last) {
		m.u.append_resp.hint_index = last + 1;
		m.u.append_resp.hint_term = 0;
	} else {
		raft_term t = term_at(r, prev);
		raft_index i = prev;
		while (i > log_first(r) && term_at(r, i - 1) == t)
			i--;
		m.u.append_resp.hint_index = i;
		m.u.append_resp.hint_term = t;
	}
	send(r, &m);
}

/* ---- message handlers */

static void on_prevote(raft *r, const raft_msg *m)
{
	/* A grant echoes the term the candidate asked about, so it can match the answer to its
	 * campaign; a refusal carries this node's real term, which is how a candidate that has
	 * fallen behind learns it (a refusal from a peer at the same term changes nothing). */
	bool ok = m->term > r->term && up_to_date(r, m->u.vote.last_index, m->u.vote.last_term);
	raft_msg resp = { .type = RAFT_MSG_PREVOTE_RESP, .from = r->self, .to = m->from,
			  .term = ok ? m->term : r->term };
	resp.u.vote_resp.granted = ok;
	if (!ok)
		r->st.prevotes_refused++;
	send(r, &resp);
}

static void on_vote(raft *r, const raft_msg *m)
{
	raft_msg resp = { .type = RAFT_MSG_VOTE_RESP, .from = r->self, .to = m->from, .term = r->term };
	bool ok = (r->vote == 0 || r->vote == m->from) &&
		  up_to_date(r, m->u.vote.last_index, m->u.vote.last_term);
	if (ok) {
		set_hard(r, r->term, m->from);
		reset_election_timer(r);
	}
	resp.u.vote_resp.granted = ok;
	send(r, &resp);
}

static void tally(raft *r, raft_id from, bool granted, unsigned *g, unsigned *rj)
{
	*g = *rj = 0;
	for (uint32_t i = 0; i < r->config.n; i++) {
		if (r->config.m[i].learner)
			continue;
		if (r->config.m[i].id == from && r->votes[i] == 0)
			r->votes[i] = granted ? 1 : 2;
		*g += r->votes[i] == 1;
		*rj += r->votes[i] == 2;
	}
}

static void on_prevote_resp(raft *r, const raft_msg *m)
{
	if (r->role != RAFT_PRECANDIDATE)
		return;
	if (!m->u.vote_resp.granted) {
		if (m->term > r->term) {
			become_follower(r, m->term, 0);
			return;
		}
		if (m->term < r->term)
			return; /* an answer to an older campaign */
	} else if (m->term != r->term + 1) {
		return;
	}
	unsigned g, rj;
	tally(r, m->from, m->u.vote_resp.granted, &g, &rj);
	if (g >= quorum(r))
		become_candidate(r, false);
	else if (rj >= quorum(r))
		become_follower(r, r->term, 0);
}

static void on_vote_resp(raft *r, const raft_msg *m)
{
	if (r->role != RAFT_CANDIDATE || m->term != r->term)
		return;
	unsigned g, rj;
	tally(r, m->from, m->u.vote_resp.granted, &g, &rj);
	if (g >= quorum(r))
		become_leader(r);
	else if (rj >= quorum(r))
		become_follower(r, r->term, 0);
}

static void on_append(raft *r, const raft_msg *m)
{
	if (r->role != RAFT_FOLLOWER || r->leader != m->from) {
		become_follower(r, r->term, m->from);
		r->ack.on = false; /* owed to whoever led before, not to this leader */
	}
	r->last_leader_contact = r->now;
	reset_election_timer(r);

	raft_index prev = m->u.append.prev_index, last = log_last(r);
	const raft_entry *es = m->u.append.entries;
	unsigned n = m->u.append.n, i = 0;
	/* Entries must continue from prev without gaps and with terms that never go back;
	 * anything else is a broken peer, and the answer to that is no answer. */
	for (unsigned k = 0; k < n; k++)
		if (es[k].index != prev + 1 + k || es[k].term > m->term ||
		    (k && es[k].term < es[k - 1].term))
			return;
	if (prev > last || (prev > r->commit && term_at(r, prev) != m->u.append.prev_term)) {
		reject_append(r, m);
		return;
	}
	raft_index matched = prev;
	for (; i < n; i++) {
		raft_index idx = es[i].index;
		if (idx <= last && term_at(r, idx) == es[i].term) {
			matched = idx;
			continue;
		}
		if (idx <= r->commit) {
			/* Cannot happen with a correct leader: a committed entry is being replaced. */
			matched = idx;
			continue;
		}
		/* A truncation that failed ends this append unanswered (#114): the leader sends
		 * again, and the daemon's owner halts on the log's fault first. */
		if (idx <= last && !truncate_log(r, idx))
			return;
		break;
	}
	if (i < n) {
		if (r->ops->append(r->log, es + i, n - i) != ISO_OK) {
			/* The store refused (out of space, an I/O error): acknowledge only what is
			 * there and let the leader retry; the caller sees the store's error. */
			ack_append(r, m->from, matched, m->u.append.seq);
			return;
		}
		matched = es[n - 1].index;
		for (unsigned j = i; j < n; j++) {
			if (es[j].type != RAFT_ENTRY_CONFIG)
				continue;
			raft_config c;
			if (raft_config_decode(es[j].data, es[j].len, &c))
				adopt_config(r, &c, es[j].index);
		}
	}
	raft_index c = ISO_MIN(m->u.append.commit, matched);
	if (c > r->commit)
		r->commit = c;
	/* Everything this log holds from the leader's own term came from this leader and sits
	 * on a prefix it verified, so all of it matches; saying so lets a heartbeat answer
	 * repair a lost acknowledgement instead of repeating what the leader already knew. */
	raft_index last_now = log_last(r);
	if (last_now > matched && term_at(r, last_now) == m->term)
		matched = last_now;
	ack_append(r, m->from, matched, m->u.append.seq);
}

static void on_append_resp(raft *r, const raft_msg *m)
{
	if (r->role != RAFT_LEADER)
		return;
	progress *p = prog_of(r, m->from);
	if (!p)
		return;
	if (p->fenced) {
		/* An answer about a log the member had before it was demoted (rebuild_progress). */
		if (!raft_seq_reached(m->u.append_resp.seq, p->fence_seq))
			return;
		p->fenced = false;
	}
#ifdef ISO_DEBUG
	if (r->test_drop_seq && m->u.append_resp.seq == r->test_drop_seq && !p->test_dropped) {
		p->test_dropped = true; /* lost on the way: the round's other frames are heard */
		r->st.test_acks_dropped++;
		return;
	}
#endif
	p->last_heard = r->now;
	p->active = true;
	if (!raft_seq_reached(p->acked_seq, m->u.append_resp.seq))
		p->acked_seq = m->u.append_resp.seq;
	note_answer(r, p, m->u.append_resp.seq);
	if (m->u.append_resp.ok) {
		raft_index idx = m->u.append_resp.index;
		if (idx > p->match) {
			p->match = idx;
			if (p->next <= idx)
				p->next = idx + 1;
		}
		switch (p->state) {
		case PROG_PROBE:
			p->paused = false;
			if (idx >= p->match) {
				p->state = PROG_REPLICATE;
				p->in_n = 0;
			}
			break;
		case PROG_REPLICATE:
			inflight_retire(p, idx);
			/* **An append is lost when the answer to something sent after it does not
			 * cover it** (#205). The oldest append still out went in the round
			 * `inflight_seq` names, and this answer carries the round of the newest request
			 * the follower had taken when it answered. A later round means the follower
			 * took that request after the append -- one stream per peer keeps them in the
			 * order they were sent -- and it answers with everything it holds, so an
			 * answer that stops short of the append says the follower never held it: the
			 * process died holding it (#85), or its store refused it. Everything after it
			 * is behind a gap, so the window goes and replication resumes from `match`.
			 *
			 * **Only then.** This used to fire on any answer that brought nothing new while
			 * `next` was past the end of the log -- and a duplicate answer is not a lost
			 * append. A follower answers a heartbeat, and an append it already holds, at
			 * once with what it has already said; arriving while newer entries are out, that
			 * answer dropped the window and sent them again, and the follower, holding them
			 * too by then, answered the copy at once with nothing new either. Each copy
			 * bought the next. With a writer never idle the chains never ended and every
			 * heartbeat started another, until the leader sent 40,000 appends a second to
			 * commit a few hundred entries. An answer from the round the oldest append went
			 * out in, or an older one, proves nothing and changes nothing: the window is
			 * kept, and its own answers retire it. The repair for a full window went with
			 * this one, since the answer it waited for is the one this reads. */
			if (p->in_n && raft_seq_newer(m->u.append_resp.seq, p->inflight_seq[p->in_head])) {
				p->in_n = 0;
				p->next = p->match + 1;
				r->st.resends++;
			}
			break;
		case PROG_SNAPSHOT:
			if (idx >= p->pending_snapshot) {
				p->state = PROG_REPLICATE;
				p->in_n = 0;
				p->next = p->match + 1;
			}
			break;
		}
		maybe_commit(r);
		update_lease(r);
		check_reads(r);
		maybe_transfer(r, p);
		return;
	}
	r->st.appends_rejected++;
	raft_index rejected = m->u.append_resp.index;
	/* A rejection below match is stale. One at exactly match is not: a heartbeat names
	 * match as its previous entry with whatever term the leader still has for it, and once
	 * that entry is compacted away the term is unknown and the follower says so -- which is
	 * the moment it needs a snapshot. */
	if (p->state == PROG_SNAPSHOT || rejected < p->match)
		return;
	/* A rejection at match answers a heartbeat, not the probe that is out, and it is kept: the
	 * probe's own answer may have been lost, and the heartbeat that names match with no term
	 * (compacted away) is then the only thing that can still tell the leader a snapshot is
	 * due. Dropped, the probe would never be resent and the follower would stay behind for
	 * ever. A repeated one is harmless: it lands on the same `next`. */
	if (p->state == PROG_PROBE && rejected != p->next - 1 && rejected != p->match)
		return; /* not the probe that is out; a duplicate or a reordered answer */
	raft_index next;
	if (!m->u.append_resp.hint_term && m->u.append_resp.hint_index <= p->match) {
		/* **The follower says its log is shorter than `match`, and it is the authority on
		 * its own log.**
		 *
		 * A rejection with no hint term is the one a follower sends when the entry named
		 * was past the end of its log: `hint_index` is then its last index plus one, which
		 * is a statement of fact and not a guess. When that lands at or below `match`, the
		 * leader's `match` has just stopped being true -- a replaced disk, or a store
		 * restored from further back -- and every path below would clamp `next` up to
		 * `match + 1` again, so `next` could never fall below the log's first index and the
		 * snapshot that is the only cure would never be sent. The follower rejects, the
		 * leader re-sends the same entry, forever: measured at 396 rejections and 0
		 * snapshots, with a metadata node stuck at applied=0 beside a group at 214.
		 *
		 * Lowering `match` is safe. It is a lower bound on what a follower has confirmed,
		 * the leader's commit index never moves backwards, and the entries it counted are
		 * committed on a majority already. What it costs is re-sending what that follower
		 * no longer has, which is the point. */
		p->match = m->u.append_resp.hint_index - 1;
		next = m->u.append_resp.hint_index;
	} else if (p->state == PROG_REPLICATE) {
		/* The pipeline ran past what the follower holds; what it does hold is known. */
		next = p->match + 1;
	} else {
		next = ISO_MIN(rejected, m->u.append_resp.hint_index);
		if (m->u.append_resp.hint_term) {
			/* Skip the whole run of the follower's conflicting term in one step. */
			raft_index i = ISO_MIN(next, log_last(r));
			while (i > log_first(r) && term_at(r, i) > m->u.append_resp.hint_term)
				i--;
			next = ISO_MIN(next, i + 1);
		}
		if (next <= p->match)
			next = p->match + 1;
	}
	p->next = next;
	p->state = PROG_PROBE;
	p->paused = false;
	p->in_n = 0;
	update_lease(r);
	check_reads(r);
}

static void on_snapshot(raft *r, const raft_msg *m)
{
	if (r->role != RAFT_FOLLOWER || r->leader != m->from)
		become_follower(r, r->term, m->from);
	r->last_leader_contact = r->now;
	reset_election_timer(r);
	const raft_config *c = &m->u.snapshot.config;
	raft_index idx = m->u.snapshot.index;
	if (c->n > RAFT_MEMBERS_MAX || idx == 0 || m->u.snapshot.term == 0 ||
	    m->u.snapshot.term > m->term)
		return; /* not a snapshot any leader could have taken */
	if (idx <= r->commit) {
		ack_append(r, m->from, ISO_MIN(idx, log_last(r)), 0);
		return;
	}
	if (idx <= log_last(r) && term_at(r, idx) == m->u.snapshot.term) {
		/* The log already holds the snapshot's last entry, so it holds everything the
		 * image would bring and possibly more that the leader has not heard about yet
		 * (a stale rejection can send it back further than we are). Keep the suffix:
		 * the leader may already be counting on it. */
		r->commit = idx;
		raft_index matched = idx, last_now = log_last(r);
		if (last_now > matched && term_at(r, last_now) == m->term)
			matched = last_now;
		ack_append(r, m->from, matched, 0);
		return;
	}
	/* **A reset that fails installs nothing and answers nothing** (#308, plan 28c), as a
	 * failed truncation ends its append: the log may hold part of what it held, or nothing, and an
	 * image restored over it, acknowledged to the leader, would be a replica whose log says
	 * otherwise. What the core knows to be synced is lowered to what the log may still hold -- a
	 * lower `persisted` claims less, never more -- and the leader sends the image again; the daemon's
	 * log has faulted, and its owner halts first. The reset used to be done after the RESTORE was
	 * queued and its result ignored: the install went ahead and was acknowledged over a log with
	 * no segment. */
	if (r->ops->reset(r->log, idx, m->u.snapshot.term) != ISO_OK) {
		raft_index last = log_last(r);
		if (r->persisted > last)
			r->persisted = last;
		if (r->sync_requested > last) {
			r->sync_requested = last;
			r->sync_inflight = false;
		}
		return;
	}
	act *a = push_act(r, RAFT_ACT_RESTORE);
	a->a.u.snapshot.from = m->from;
	a->a.u.snapshot.index = idx;
	a->a.u.snapshot.term = m->u.snapshot.term;
	a->a.u.snapshot.config = *c;
	a->a.u.snapshot.data = m->u.snapshot.data;
	a->a.u.snapshot.len = m->u.snapshot.len;
	r->snap_index = idx;
	r->snap_term = m->u.snapshot.term;
	r->snap_config = *c;
	r->persisted = r->sync_requested = idx;
	r->sync_inflight = false;
	r->commit = r->applied = r->handed = idx;
	r->apply_inflight = false;
	r->handed_config.on = false;
	r->applied_config = *c;
	r->ack.on = false;
	adopt_config(r, c, idx);
	r->st.snapshots_installed++;
	ack_append(r, m->from, idx, 0);
}

static void on_timeout_now(raft *r, const raft_msg *m)
{
	(void)m;
	if (!can_campaign(r) || r->role == RAFT_LEADER)
		return;
	become_candidate(r, true);
}

/* Whether a vote request should be ignored because we are happy with the leader we have. */
static bool sticky(const raft *r, const raft_msg *m)
{
	if (m->u.vote.transfer || !r->o.check_quorum)
		return false;
	if (r->role == RAFT_LEADER)
		return true; /* until check-quorum says otherwise, this node has the quorum */
	/* **A restart forgets a promise this node already made.** A leader's lease rests on every
	 * voter that acknowledged its round refusing to elect anyone else for an election timeout
	 * after it -- and `leader` and `last_leader_contact` are in memory, so a node that crashes
	 * a moment after acknowledging comes back with neither and grants the first vote it is
	 * asked for. The lease is still running on the leader, which is a stale read. So a node
	 * that has just started keeps quiet for one election timeout, which is exactly as long as
	 * the promise it might have made could still be outstanding (plan section 20f-i). Only
	 * with lease reads on: without them nothing rests on the promise, and the wait would cost
	 * a cold-started cluster an election round for nothing. */
	if (r->o.lease_reads && r->now < r->vote_quiet_until)
		return true;
	return r->leader != 0 && r->now - r->last_leader_contact < r->o.election_min_ns;
}

void raft_step(raft *r, const raft_msg *m)
{
	bool is_vote_req = m->type == RAFT_MSG_PREVOTE || m->type == RAFT_MSG_VOTE;
	bool prevote = m->type == RAFT_MSG_PREVOTE || m->type == RAFT_MSG_PREVOTE_RESP;

	if (is_vote_req) {
		/* **A learner votes** (etcd's rule, raft thesis section 4.1: a server answers an RPC
		 * without looking at its own configuration; who counts a vote is the candidate's
		 * quorum test alone, `tally`, over the voters of the candidate's configuration).
		 * A learner that has just been promoted, but has not yet received the entry that
		 * promotes it, still sees itself as a learner, while the leader that committed the
		 * change counts it: with one of the other voters down, the survivor can win only with
		 * this node's vote, and refusing it stalled the group for ever. The vote is safe
		 * because the node's log is not amnesiac -- what it acknowledged as a voter it still
		 * holds (a replica that was reseeded is the other case, and it is the fence's: a
		 * fenced log never votes) -- and because term and vote are made durable before the
		 * reply like any other. A vote nobody counts is harmless, and `sticky` below applies
		 * to a learner as to a voter: a promoted learner acknowledges its leader's appends,
		 * the leader's lease counts it, and refusing while the leader is heard keeps lease
		 * reads safe. */
		if (vote_fenced(r)) {
			settle(r);
			return; /* a fenced log never votes */
		}
		if (sticky(r, m)) {
			r->st.prevotes_refused++;
			settle(r);
			return;
		}
	}
	if (m->term > r->term && !prevote) {
		bool from_leader = m->type == RAFT_MSG_APPEND || m->type == RAFT_MSG_SNAPSHOT;
		if (r->role == RAFT_LEADER)
			r->st.step_downs++;
		become_follower(r, m->term, from_leader ? m->from : 0);
	} else if (m->term < r->term) {
		/* Tell a stale sender what the term is; responses are simply old news. */
		raft_msg resp = { .from = r->self, .to = m->from, .term = r->term };
		switch (m->type) {
		case RAFT_MSG_APPEND:
		case RAFT_MSG_SNAPSHOT:
			resp.type = RAFT_MSG_APPEND_RESP;
			resp.u.append_resp.ok = false;
			resp.u.append_resp.index = 0;
			send(r, &resp);
			break;
		case RAFT_MSG_VOTE:
			resp.type = RAFT_MSG_VOTE_RESP;
			send(r, &resp);
			break;
		case RAFT_MSG_PREVOTE:
			resp.type = RAFT_MSG_PREVOTE_RESP;
			send(r, &resp);
			break;
		default:
			break;
		}
		settle(r);
		return;
	}

	switch (m->type) {
	case RAFT_MSG_PREVOTE: on_prevote(r, m); break;
	case RAFT_MSG_PREVOTE_RESP: on_prevote_resp(r, m); break;
	case RAFT_MSG_VOTE: on_vote(r, m); break;
	case RAFT_MSG_VOTE_RESP: on_vote_resp(r, m); break;
	case RAFT_MSG_APPEND: on_append(r, m); break;
	case RAFT_MSG_APPEND_RESP: on_append_resp(r, m); break;
	case RAFT_MSG_SNAPSHOT: on_snapshot(r, m); break;
	case RAFT_MSG_TIMEOUT_NOW: on_timeout_now(r, m); break;
	default: break;
	}
	settle(r);
}

/* ------------------------------------------------------------------ inputs */

void raft_clock(raft *r, uint64_t now_ns)
{
	if (!r->ticked) {
		r->ticked = true;
		r->now = now_ns;
		/* The first reading is the only "when did this process start" the core has: it takes
		 * no clock at construction, on purpose, so that nothing reads one behind the
		 * caller's back. See sticky() for what the quiet window is for. */
		r->vote_quiet_until = now_ns + r->o.election_min_ns;
		reset_election_timer(r);
		return;
	}
	if (now_ns > r->now)
		r->now = now_ns;
}

void raft_tick(raft *r, uint64_t now_ns)
{
	raft_clock(r, now_ns);
	uint64_t now = r->now;

	if (r->role == RAFT_LEADER) {
		if (now >= r->heartbeat_deadline)
			r->heartbeat_due = true;
		if (r->o.check_quorum && now >= r->quorum_check_at)
			check_quorum(r);
		if (r->transfer_to && now >= r->transfer_deadline)
			r->transfer_to = 0;
	} else if (now >= r->election_deadline) {
		if (can_campaign(r))
			campaign(r);
		else
			reset_election_timer(r);
	}
	settle(r);
}

uint64_t raft_next_deadline(const raft *r)
{
	uint64_t d = UINT64_MAX;
	if (r->role == RAFT_LEADER) {
		d = ISO_MIN(d, r->heartbeat_deadline);
		if (r->round_overdue_at && !r->round_reads && !quorum_acked(r, r->seq)) {
			for (unsigned i = 0; i < r->n_reads; i++)
				if (r->reads[i].seq == 0) {
					d = ISO_MIN(d, r->round_overdue_at);
					break;
				}
		}
		if (r->o.check_quorum)
			d = ISO_MIN(d, r->quorum_check_at);
		if (r->batch_deadline)
			d = ISO_MIN(d, r->batch_deadline);
		if (r->transfer_to)
			d = ISO_MIN(d, r->transfer_deadline);
		for (unsigned i = 0; i < r->n_prog; i++) {
			if (r->prog[i].state == PROG_SNAPSHOT)
				d = ISO_MIN(d, r->prog[i].snapshot_sent_at + r->o.snapshot_resend_ns);
			if (r->prog[i].resend_at)
				d = ISO_MIN(d, r->prog[i].resend_at);
		}
	} else if (r->ticked) {
		d = r->election_deadline;
	} else {
		d = 0;
	}
	return d;
}

void raft_persisted(raft *r, raft_index index, raft_term term)
{
	/* Whatever it says, the sync it answers is over, and `settle()` below emits the next one
	 * for anything that arrived while it ran. Cleared before the index is examined, because a
	 * report this log has moved past is still a report that the fsync finished. */
	r->sync_inflight = false;
	if (index > r->persisted && index <= log_last(r) && term_at(r, index) == term) {
		r->persisted = index;
		if (r->role == RAFT_LEADER) {
			progress *p = prog_of(r, r->self);
			if (p)
				p->match = index;
			maybe_commit(r);
		}
		if (r->ack.on && r->persisted >= r->ack.index) {
			r->ack.on = false;
			ack_append(r, r->ack.to, r->ack.index, r->ack.seq);
		}
	}
	settle(r);
}

void raft_hard_saved(raft *r, const raft_hard_state *hs)
{
	(void)hs;
	r->hard_inflight = false;
	settle(r);
}

void raft_applied(raft *r, raft_index index)
{
	if (index > r->applied)
		r->applied = index;
	if (index >= r->handed)
		r->apply_inflight = false;
	if (r->handed_config.on && index >= r->handed_config.index) {
		r->applied_config = r->handed_config.config;
		r->handed_config.on = false;
	}
	if (r->role == RAFT_LEADER) {
		check_reads(r);
		/* A leader that has just applied its own removal hands over. */
		if (r->config_index && r->config_index <= r->applied &&
		    !raft_config_find(&r->config, r->self)) {
			r->st.step_downs++;
			become_follower(r, r->term, 0);
		}
	}
	settle(r);
}

void raft_compact(raft *r, raft_index index, raft_term term, const raft_config *config)
{
	raft_compact_keeping(r, index, term, config, r->o.snapshot_retain);
}

void raft_compact_keeping(raft *r, raft_index index, raft_term term, const raft_config *config,
			  raft_index keep)
{
	if (index < r->snap_index)
		return;
	r->snap_index = index;
	r->snap_term = term;
	r->snap_config = *config;
	if (index > keep) {
		raft_index upto = ISO_MIN(index - keep, r->persisted);
		if (upto >= log_first(r))
			r->ops->compact(r->log, upto);
	}
	settle(r);
}

void raft_snapshot_failed(raft *r, raft_id to)
{
	progress *p = r->role == RAFT_LEADER ? prog_of(r, to) : NULL;
	if (p && p->state == PROG_SNAPSHOT) {
		p->state = PROG_PROBE;
		p->next = p->match + 1;
		p->paused = false;
	}
	settle(r);
}

static bool term_committed(const raft *r) { return r->commit >= r->term_start; }

iso_err raft_propose(raft *r, const void *data, size_t len, raft_index *index_out)
{
	if (r->role != RAFT_LEADER)
		return ISO_ERR_NOTLEADER;
	if (r->transfer_to || r->uncommitted_bytes + len > r->o.max_uncommitted_bytes)
		return ISO_ERR_BUSY;
	raft_entry e = { .index = log_last(r) + 1, .term = r->term, .type = RAFT_ENTRY_NORMAL,
			 .len = (uint32_t)len, .data = data };
	iso_err rc = r->ops->append(r->log, &e, 1);
	if (rc != ISO_OK)
		return rc;
	r->uncommitted_bytes += len;
	r->st.proposals++;
	if (r->o.batch_ns && !r->batch_deadline)
		r->batch_deadline = r->now + r->o.batch_ns;
	if (index_out)
		*index_out = e.index;
	settle(r);
	return ISO_OK;
}

/* A voter set of exactly this node: whatever it has committed is the group's, and no other node can
 * be elected while it stays that way -- a configuration that adds a second voter is adopted the
 * moment it is appended, here, before any message that carries it is sent. */
static bool lone_voter(const raft *r)
{
	return raft_config_voters(&r->config) == 1 && is_voter_in(&r->config, r->self);
}

bool raft_read_lease(const raft *r, raft_lease *out)
{
	if (r->role != RAFT_LEADER || !term_committed(r) || r->transfer_to)
		return false;
	uint64_t until;
	if (lone_voter(r))
		until = UINT64_MAX;
	else if (r->o.lease_reads && r->now < r->lease_until)
		until = r->lease_until;
	else
		return false;
	if (out) {
		out->term = r->term;
		out->term_start = r->term_start;
		out->lease_until = until;
	}
	return true;
}

static iso_err read_queue(raft *r, uint64_t ctx, bool use_lease)
{
	if (r->role != RAFT_LEADER || !term_committed(r))
		return ISO_ERR_NOTLEADER;
	if (r->n_reads >= r->o.max_reads)
		return ISO_ERR_BUSY;
	r->st.reads++;
	read_req *q = &r->reads[r->n_reads++];
	q->ctx = ctx;
	q->index = r->commit;
	q->seq = 0;
	q->confirmed = false;
	q->at = r->now;
	/* Confirmed now on exactly what a client loop answers on (plan 17i-5b): the lease, or a lone
	 * voter's round of one, which needs no round to be sent to be confirmed. */
	raft_lease l;
	/* `use_lease` false (raft_read_index) skips a lease that rests on the clock and keeps a lone
	 * voter's, which rests on none: the round it would send is a round of one. */
	if (raft_read_lease(r, &l) && (use_lease || l.lease_until == UINT64_MAX)) {
		q->confirmed = true;
		q->seq = UINT32_MAX;
		if (l.lease_until != UINT64_MAX)
			r->st.reads_lease++;
	}
	check_reads(r);
	settle(r);
	return ISO_OK;
}

iso_err raft_read(raft *r, uint64_t ctx)
{
	return read_queue(r, ctx, true);
}

iso_err raft_read_index(raft *r, uint64_t ctx)
{
	return read_queue(r, ctx, false);
}

iso_err raft_change(raft *r, raft_change_kind kind, raft_id id, raft_index *index_out)
{
	if (r->role != RAFT_LEADER || !term_committed(r))
		return ISO_ERR_NOTLEADER;
	if (r->transfer_to || r->config_index > r->commit)
		return ISO_ERR_BUSY;
	if (id == 0)
		return ISO_ERR_INVAL;
	raft_config c = r->config;
	raft_member *m = (raft_member *)raft_config_find(&c, id);
	switch (kind) {
	case RAFT_ADD_LEARNER:
		if (m)
			return ISO_ERR_EXISTS;
		if (c.n == RAFT_MEMBERS_MAX)
			return ISO_ERR_FULL;
		c.m[c.n++] = (raft_member){ .id = id, .learner = true };
		break;
	case RAFT_ADD_VOTER:
		if (m && !m->learner)
			return ISO_ERR_EXISTS;
		if (m) {
			m->learner = false;
		} else {
			if (c.n == RAFT_MEMBERS_MAX)
				return ISO_ERR_FULL;
			c.m[c.n++] = (raft_member){ .id = id, .learner = false };
		}
		break;
	case RAFT_REMOVE:
		if (!m)
			return ISO_ERR_NOTFOUND;
		*m = c.m[--c.n];
		if (raft_config_voters(&c) == 0)
			return ISO_ERR_INVAL;
		break;
	case RAFT_DEMOTE:
		if (!m)
			return ISO_ERR_NOTFOUND;
		if (m->learner)
			return ISO_ERR_EXISTS;
		if (id == r->self)
			return ISO_ERR_INVAL; /* a leader that counted nobody's vote, its own included */
		m->learner = true;
		break;
	default:
		return ISO_ERR_INVAL;
	}
	uint8_t buf[RAFT_CONFIG_WIRE_MAX];
	size_t len = raft_config_encode(&c, buf, sizeof buf);
	raft_entry e = { .index = log_last(r) + 1, .term = r->term, .type = RAFT_ENTRY_CONFIG,
			 .len = (uint32_t)len, .data = buf };
	iso_err rc = r->ops->append(r->log, &e, 1);
	if (rc != ISO_OK)
		return rc;
	adopt_config(r, &c, e.index);
	/* A demotion starts a round of its own, so that the first append the demoted member is sent
	 * already carries the `seq` its fence waits for (rebuild_progress); otherwise the answer to
	 * that probe would be dropped and the member would sit paused until the next heartbeat. */
	if (kind == RAFT_DEMOTE)
		r->heartbeat_due = true;
	if (index_out)
		*index_out = e.index;
	settle(r);
	return ISO_OK;
}

iso_err raft_transfer(raft *r, raft_id to)
{
	if (r->role != RAFT_LEADER)
		return ISO_ERR_NOTLEADER;
	if (to == r->self || !is_voter_in(&r->config, to))
		return ISO_ERR_INVAL;
	if (r->transfer_to)
		return ISO_ERR_BUSY;
	r->transfer_to = to;
	r->transfer_deadline = r->now + r->o.election_min_ns;
	r->lease_until = 0; /* the hand-over invites a new leader now; see update_lease */
	progress *p = prog_of(r, to);
	if (p)
		maybe_transfer(r, p);
	settle(r);
	return ISO_OK;
}

void raft_campaign(raft *r)
{
	if (r->role != RAFT_LEADER && can_campaign(r))
		become_candidate(r, true);
	settle(r);
}

void raft_set_campaign(raft *r, bool allowed)
{
	r->campaign_refused = !allowed;
}

void raft_step_down(raft *r)
{
	if (r->role == RAFT_LEADER) {
		r->st.step_downs++;
		become_follower(r, r->term, 0);
	}
	settle(r);
}

/* ------------------------------------------------------------------ outputs */

static void flush_held(raft *r)
{
	for (unsigned i = 0; i < r->n_held; i++) {
		raft_msg *m = &r->held[i];
		bool prevote = m->type == RAFT_MSG_PREVOTE || m->type == RAFT_MSG_PREVOTE_RESP;
		if (!prevote && m->term != r->term) {
			r->st.messages_dropped++;
			continue;
		}
		act *a = push_act(r, RAFT_ACT_SEND);
		a->a.u.msg = *m;
	}
	r->n_held = 0;
}

static void settle(raft *r)
{
	if (r->hard_dirty && !r->hard_inflight) {
		act *a = push_act(r, RAFT_ACT_SAVE_HARD);
		a->a.u.hard.term = r->term;
		a->a.u.hard.vote = r->vote;
		a->a.u.hard.fence_index = r->fence_index;
		a->a.u.hard.fence_term = r->fence_term;
		r->hard_dirty = false;
		r->hard_inflight = true;
	}
	raft_index last = log_last(r);
	if (last > r->sync_requested && !r->sync_inflight) {
		act *a = push_act(r, RAFT_ACT_SYNC);
		a->a.u.sync.index = last;
		a->a.u.sync.term = term_at(r, last);
		r->sync_requested = last;
		r->sync_inflight = true;
	}
	if (r->role == RAFT_LEADER) {
		replicate(r);
		maybe_commit(r);
	}
	hand_out_apply(r);
	if (!hard_pending(r) && r->n_held)
		flush_held(r);
	if (r->state_dirty) {
		act *a = push_act(r, RAFT_ACT_STATE);
		a->a.u.state.role = r->role;
		a->a.u.state.term = r->term;
		a->a.u.state.leader = r->leader;
		r->state_dirty = false;
	}
}

bool raft_next_action(raft *r, raft_action *out)
{
	if (r->acts_head == r->acts_n) {
		/* **The one moment nothing names an entry** (plan 5a-i-g, #201): every action queued
		 * has been handed over, and the caller, asking again, is done with the last. Only
		 * now may the log's bytes and the arrays that carry them be reused -- not at the
		 * next append, which a state machine makes from inside an APPLY while the actions
		 * queued before it still wait to be sent. */
		r->acts_head = r->acts_n = 0;
		ents_release(r);
		r->ops->release(r->log);
		return false;
	}
	act *a = &r->acts[r->acts_head++];
	*out = a->a;
	if (a->a.type == RAFT_ACT_SEND && a->a.u.msg.type == RAFT_MSG_APPEND)
		out->u.msg.u.append.entries = a->ent_n ? a->ents : NULL;
	else if (a->a.type == RAFT_ACT_APPLY) {
		out->u.apply.entries = a->ents;
		out->u.apply.n = a->ent_n;
	}
	return true;
}

/* ------------------------------------------------------------------ lifecycle */

raft *raft_new(raft_id self, const raft_opts *opts, const raft_log_ops *ops, void *log,
	       const raft_boot *boot, char *err, size_t errsize)
{
	if (self == 0 || !ops || !ops->first || !ops->last || !ops->term_at || !ops->get ||
	    !ops->append || !ops->truncate || !ops->reset || !ops->compact || !ops->release) {
		iso_errf(err, errsize, ISO_ERR_INVAL, "raft: node id and a complete log store are required");
		return NULL;
	}
	raft *r = iso_xcalloc(1, sizeof *r);
	r->self = self;
	r->ops = ops;
	r->log = log;
	if (opts)
		r->o = *opts;
	else
		raft_opts_defaults(&r->o);
	raft_opts d;
	raft_opts_defaults(&d);
#define DEF(f) if (!r->o.f) r->o.f = d.f
	DEF(heartbeat_ns); DEF(election_min_ns); DEF(max_inflight); DEF(max_append_entries);
	DEF(max_append_bytes); DEF(max_apply_entries); DEF(max_apply_bytes);
	DEF(snapshot_resend_ns); DEF(snapshot_retain); DEF(max_uncommitted_bytes); DEF(max_reads);
	DEF(read_resend_min_ns);
#undef DEF
	if (r->o.max_inflight > INFLIGHT_RING)
		r->o.max_inflight = INFLIGHT_RING;
	if (r->o.heartbeat_ns >= r->o.election_min_ns) {
		iso_errf(err, errsize, ISO_ERR_INVAL,
			 "raft: heartbeat interval must be shorter than the election timeout");
		free(r);
		return NULL;
	}
	/* A lease read is only safe while followers refuse to elect anyone else before their timeout
	 * has run (sticky votes); without check-quorum that refusal is off and the lease proves
	 * nothing. Refuse the combination rather than serve stale reads quietly. */
	if (r->o.lease_reads && !r->o.check_quorum) {
		iso_errf(err, errsize, ISO_ERR_INVAL, "raft: lease reads require check-quorum");
		free(r);
		return NULL;
	}
	/* **And a lease is worth exactly what the clocks under it are worth.** This leader holds it
	 * for `lease_ns` of its own clock while the followers refuse to vote for `election_min_ns`
	 * of theirs; if the leader's runs slow and a follower's runs fast, the first outlasts the
	 * second and there is a window with two leaders in it, one of them still answering reads.
	 * Scaling the lease by (1-d)/(1+d) closes it for any pair of clocks inside the bound --
	 * the proof is in plan section 20f-i.
	 *
	 * There is deliberately no default for `d`. The core's clock is CLOCK_MONOTONIC, which
	 * Linux *slews*, so the number is not a crystal's 50 ppm but whatever the host's time
	 * daemon is allowed to correct at -- 500 ppm under ntpd, and 83333 ppm under chrony's
	 * default maxslewrate. That is a claim about someone else's machines, and this code is not
	 * in a position to make it. */
	if (r->o.lease_reads && (r->o.max_clock_drift_ppm == 0 ||
				 r->o.max_clock_drift_ppm >= 1000000)) {
		iso_errf(err, errsize, ISO_ERR_INVAL,
			 "raft: lease reads need max_clock_drift_ppm in 1..999999 -- the worst rate "
			 "error any node's clock may have against true time. CLOCK_MONOTONIC is "
			 "slewed by the time daemon, so this is its slew ceiling: 500 for ntpd, "
			 "83333 for chrony's default maxslewrate, less only if you have capped it");
		free(r);
		return NULL;
	}
	/* E(1-d)/(1+d), written as E - 2dE/(1e6+d) so the product stays inside 64 bits for any
	 * election timeout up to a couple of hours rather than only a few seconds. With lease reads
	 * off the drift is nobody's business and the lease is never read. */
	r->lease_ns = r->o.election_min_ns -
		      r->o.election_min_ns * 2 * r->o.max_clock_drift_ppm /
			      (1000000 + r->o.max_clock_drift_ppm);
	if (boot->config.n > RAFT_MEMBERS_MAX) {
		iso_errf(err, errsize, ISO_ERR_INVAL, "raft: configuration lists too many members");
		free(r);
		return NULL;
	}
	iso_rng_seed(&r->rng, r->o.seed ^ ((uint64_t)self << 32));
	r->term = boot->hard.term;
	r->vote = boot->hard.vote;
	r->fence_index = boot->hard.fence_index;
	r->fence_term = boot->hard.fence_term;
	r->role = RAFT_FOLLOWER;
	raft_index last = log_last(r);
	r->persisted = r->sync_requested = last;
	r->sync_inflight = false;
	r->applied = r->commit = r->handed = boot->applied;
	r->snap_index = boot->applied;
	r->snap_term = boot->applied_term;
	r->snap_config = boot->config;
	r->applied_config = boot->config;
	r->reads = iso_xcalloc(r->o.max_reads, sizeof *r->reads);
	reload_config(r, &boot->config);
	r->state_dirty = true;
	return r;
}

void raft_free(raft *r)
{
	if (!r)
		return;
	free(r->reads);
	free(r->acts);
	ents_block *b = r->ents;
	while (b) {
		ents_block *older = b->older;
		free(b);
		b = older;
	}
	free(r);
}

/* ------------------------------------------------------------------ observation */

raft_role raft_get_role(const raft *r) { return r->role; }
raft_term raft_get_term(const raft *r) { return r->term; }
raft_id raft_get_leader(const raft *r) { return r->leader; }
raft_index raft_get_commit(const raft *r) { return r->commit; }
raft_index raft_get_applied(const raft *r) { return r->applied; }
raft_index raft_get_last(const raft *r) { return r->ops->last(r->log); }
const raft_config *raft_get_config(const raft *r) { return &r->config; }
raft_index raft_get_config_index(const raft *r) { return r->config_index; }
raft_term raft_get_term_at(const raft *r, raft_index index) { return term_at((raft *)r, index); }
iso_err raft_get_store_fault(const raft *r, char *what, size_t n)
{
	if (r->store_err != ISO_OK && what && n)
		snprintf(what, n,
			 "the raft log refused the no-op that opens term %llu at index %llu"
			 " (%s), so this replica cannot lead",
			 (unsigned long long)r->store_err_term,
			 (unsigned long long)r->store_err_index, iso_strerr(r->store_err));
	return r->store_err;
}

raft_index raft_get_vote_fence(const raft *r)
{
	return vote_fenced((raft *)r) ? r->fence_index : 0;
}

#ifdef ISO_DEBUG
void raft_test_drop_first_read_acks(raft *r, unsigned every_n)
{
	r->test_drop_every = every_n;
	r->test_rounds = 0;
	r->test_drop_seq = 0;
}

void raft_test_add_rounds(raft *r, uint32_t n)
{
	/* In steps well inside the window, keeping the fences as the rounds themselves would. */
	while (n) {
		uint32_t step = ISO_MIN(n, FENCE_WINDOW / 2);
		r->seq += step;
		if (r->seq == 0)
			r->seq = 1;
		keep_fences_in_window(r);
		n -= step;
	}
}
#endif

bool raft_is_voter(const raft *r) { return is_voter_in(&r->config, r->self); }

uint64_t raft_get_leader_silence(const raft *r)
{
	if (r->role == RAFT_LEADER)
		return 0;
	if (r->last_leader_contact == 0 || r->now < r->last_leader_contact)
		return r->now; /* never heard one: since the clock started */
	return r->now - r->last_leader_contact;
}

bool raft_get_progress(const raft *r, raft_id id, raft_progress *out)
{
	if (r->role != RAFT_LEADER)
		return false;
	for (unsigned i = 0; i < r->n_prog; i++) {
		const progress *p = &r->prog[i];
		if (p->id != id)
			continue;
		out->id = p->id;
		out->learner = p->learner;
		out->match = p->id == r->self ? r->persisted : p->match;
		out->next = p->next;
		out->snapshotting = p->state == PROG_SNAPSHOT;
		out->last_heard_ns = p->id == r->self ? r->now : p->last_heard;
		/* On the core's own clock, both ends: a second clock read by the caller would
		 * subtract two clocks read at different moments, which once reported a silence of
		 * 585 years. Clamped at 0, as the leader's own silence is, although the core's
		 * clock never goes back: it costs a compare, and an underflow here is a member
		 * called silent for ever. */
		uint64_t since = p->id == r->self ? r->now : p->last_heard ? p->last_heard
									   : p->tracked_since;
		out->silence_ns = r->now > since ? r->now - since : 0;
		out->ack_ns = p->ack_ns;
		out->ack_min_ns = p->ack_min_ns;
		out->acks_slow = p->acks_slow;
		return true;
	}
	return false;
}

void raft_get_stats(const raft *r, raft_stats *out) { *out = r->st; }
