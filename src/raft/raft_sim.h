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
/* The Raft simulator (plan section 6): real cores, a fake world.
 *
 * Every node is a genuine `raft` instance from raft.h. What is simulated is everything the core
 * was designed not to know about: an in-memory log store whose durability is explicit (a sync
 * completes after a delay, a crash forgets everything after the durable prefix and rolls the
 * hard state back to the last save), a virtual clock that drifts per node, and a network that
 * delays, loses, duplicates, reorders and partitions. Invariants are checked after every event,
 * and the first violation stops the run with a message that names the seed.
 *
 * Two ways to drive it: raft_sim_run() with non-zero fault and workload rates is chaos, and the
 * scripted functions (crash, partition, propose, transfer...) let a test stage one exact story.
 */
#ifndef ISO_RAFT_RAFT_SIM_H
#define ISO_RAFT_RAFT_SIM_H

#include "raft/raft.h"

typedef struct raft_sim raft_sim;

typedef struct {
	uint64_t seed;
	unsigned voters;   /* nodes that start in the configuration as voters (1..) */
	unsigned learners; /* nodes that start in it as learners */
	unsigned spare;    /* nodes that exist but are not members until churn adds them */

	/* the network */
	uint64_t delay_min_ns, delay_max_ns; /* one hop */
	double drop;                          /* per message */
	double dup;                           /* per message */
	unsigned lose_first_answers; /* raft_sim_lose_first_answers from the start; 0 = never */
	bool reorder; /* default off: the daemon talks over one TCP stream per peer, which keeps
		       * order; on, every message draws its own delay */

	/* the disks */
	uint64_t sync_min_ns, sync_max_ns; /* fsync latency, log and hard state alike */

	/* faults, as events per virtual second (0 = never) */
	double partition_rate;
	uint64_t partition_ns; /* how long a split lasts */
	double crash_rate;
	uint64_t restart_min_ns, restart_max_ns;
	double clock_drift; /* each node's clock runs at 1 +- this */

	/* workload, as events per virtual second */
	double propose_rate;
	double read_rate;
	double churn_rate;    /* membership changes */
	double transfer_rate; /* leadership transfers */
	/* A voter halts and is re-seeded (plan 5a-i-g, L's demotion and its repair): the leader is
	 * asked to make it a learner, and once that has committed its log and state machine are
	 * emptied behind a vote fence, as `isoctl mds reseed` does. */
	double reseed_rate;
	unsigned payload_max; /* proposals carry 8 bytes plus up to this much padding */
	/* On a leader, the chance that an applied entry's state machine proposes one to three more
	 * from inside the APPLY, as often of one length as not -- what the metadata service does at
	 * `verify start` (plan 5a-i-g, #201). 0 draws nothing, so a profile without it replays. */
	double apply_propose;

	raft_index snapshot_every; /* applied entries between snapshots; 0 = never */

	/* The simulator's own bookkeeping, held to the long way round. `pend_cap` is how many
	 * proposals it tracks for latency (0 = 65536; a small one makes a short run evict, which
	 * the full size reaches only after a minute), and `cross_check` has it find each tracked
	 * proposal and count each configuration entry above the commit index a second way, by
	 * walking everything, and fail on a difference. Linear per step, so for tests only. */
	unsigned pend_cap;
	unsigned pend_buckets; /* hash buckets for them (0 = 65536; a few make the chains long, as
				* the full ring's are, so that a walk past a chain's head is run) */
	bool cross_check;

	raft_opts core; /* per-node core options; the seed is derived per node */
} raft_sim_params;

void raft_sim_params_defaults(raft_sim_params *p);

typedef struct {
	uint64_t proposals;       /* accepted by a leader */
	uint64_t proposals_refused;
	uint64_t proposed_inside;  /* of `proposals`, made from inside an APPLY (apply_propose) */
	uint64_t committed;       /* proposals seen applied on the leader that took them (the
				   * most recent 65536 outstanding are tracked) */
	uint64_t reads;
	uint64_t reads_ok;
	uint64_t reads_failed;
	uint64_t messages_sent;
	uint64_t messages_dropped; /* by loss, partition or a dead receiver */
	uint64_t crashes;
	uint64_t partitions;
	uint64_t leader_changes;
	uint64_t changes;         /* membership changes accepted */
	uint64_t reseeds;         /* replicas emptied behind a vote fence */
	uint64_t transfers;
	uint64_t snapshots_installed;
	raft_term max_term;
	raft_index max_applied;
	uint64_t commit_latency_max_ns;
	uint64_t commit_latency_sum_ns;
	uint64_t checks;          /* invariant sweeps */
} raft_sim_stats;

raft_sim *raft_sim_new(const raft_sim_params *p);
void raft_sim_free(raft_sim *s);

/* Advance virtual time by `ns`. Returns false at the first invariant violation. */
bool raft_sim_run(raft_sim *s, uint64_t ns);

/* Advance until `cond` holds (checked after every event) or `max_ns` has passed. Returns true
 * when the condition was met; false on timeout or on a violation (see raft_sim_failure). */
bool raft_sim_run_until(raft_sim *s, uint64_t max_ns, bool (*cond)(const raft_sim *, void *),
			void *user);

const char *raft_sim_failure(const raft_sim *s); /* "" while all is well */
uint64_t raft_sim_now(const raft_sim *s);
unsigned raft_sim_nodes(const raft_sim *s); /* ids are 1..n */

/* ---- observation */
bool raft_sim_up(const raft_sim *s, raft_id id);
raft *raft_sim_core(raft_sim *s, raft_id id); /* NULL while the node is down */
raft_id raft_sim_leader(const raft_sim *s);   /* highest-term live leader, 0 if none */
raft_index raft_sim_applied(const raft_sim *s, raft_id id); /* durable, survives crashes */
uint64_t raft_sim_state_hash(const raft_sim *s, raft_id id); /* of everything applied */
const raft_config *raft_sim_config(const raft_sim *s, raft_id id); /* as applied */
raft_index raft_sim_committed_max(const raft_sim *s);
void raft_sim_get_stats(const raft_sim *s, raft_sim_stats *out);

/* ---- scripted faults */
void raft_sim_crash(raft_sim *s, raft_id id);
void raft_sim_restart(raft_sim *s, raft_id id);
/* The node loses everything it had on disk -- log, hard state and state machine -- and keeps only
 * its identity, which is what a replaced disk looks like to the rest of the group. It comes back
 * with `raft_sim_restart`. The group must notice and feed it a snapshot; the leader's `match` for
 * it is a belief that has just stopped being true, and a leader that keeps believing it will
 * replicate to that node forever without ever sending anything it can use. */
void raft_sim_wipe(raft_sim *s, raft_id id);
/* The listed nodes can talk to each other and to nobody else. */
void raft_sim_partition(raft_sim *s, const raft_id *isolated, unsigned n);
void raft_sim_heal(raft_sim *s);
void raft_sim_set_drop(raft_sim *s, double drop);
/* A network that loses, for one round in `every_n` (those whose number is a multiple of it), the
 * first answer of every member to the leader: what a burst that overflows a receive ring does to a
 * round (plan 28r). The second answer to the same round -- to a frame sent again -- is delivered,
 * as is everything for any other round. 0 = off. */
void raft_sim_lose_first_answers(raft_sim *s, unsigned every_n);
/* The node is suspended until `raft_sim_resume`: it takes no input -- no tick, no message (dropped,
 * as a suspended host's peers give up on it), no disk completion (held until it resumes) and no
 * request (refused with ISO_ERR_CLOSED). With `clock_stops` its clock stands still meanwhile and
 * runs on afterwards from where it stood, behind the others by the span, which is CLOCK_MONOTONIC
 * across a suspend; without, it ran on, which is CLOCK_BOOTTIME (plan 28y, #429). Resuming reads
 * the node's clock at once. A crash ends a suspend. */
void raft_sim_suspend(raft_sim *s, raft_id id, bool clock_stops);
void raft_sim_resume(raft_sim *s, raft_id id);
/* Set one node's clock rate against virtual time, replacing the draw `clock_drift` made for it.
 * 1.0 is a true clock, 0.5 a clock that runs at half speed.
 *
 * **Call it before the run starts.** A node's local time is its rate times the virtual clock, so
 * changing the rate later is a step, and `raft_clock` refuses to go backwards -- lowering a rate
 * mid-run would freeze that node's clock until real time caught up rather than slow it down. The
 * chaos path draws every rate at construction for the same reason. */
void raft_sim_set_clock_rate(raft_sim *s, raft_id id, double rate);
/* Print elections, crashes, partitions, votes, snapshot installs and configuration changes to
 * `file` (a FILE *), for reading one seed's story. NULL turns it off. */
void raft_sim_set_trace(raft_sim *s, void *file);
void raft_sim_set_rates(raft_sim *s, double propose_rate, double read_rate);

/* ---- scripted workload; `at` is the node asked, which may refuse */
iso_err raft_sim_propose(raft_sim *s, raft_id at, raft_index *index_out);
iso_err raft_sim_read(raft_sim *s, raft_id at);
/* The same read, confirmed by a round whatever the lease says (`raft_read_index`). */
iso_err raft_sim_read_index(raft_sim *s, raft_id at);
iso_err raft_sim_change(raft_sim *s, raft_id at, raft_change_kind kind, raft_id id);
iso_err raft_sim_transfer(raft_sim *s, raft_id at, raft_id to);

/* One chaos run from fresh: create, run for `duration_ns`, report. */
bool raft_sim_chaos(const raft_sim_params *p, uint64_t duration_ns, raft_sim_stats *stats,
		    char *failure, size_t failure_size);

#endif /* ISO_RAFT_RAFT_SIM_H */
