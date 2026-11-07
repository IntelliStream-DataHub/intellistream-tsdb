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
/* Raft simulator: see raft_sim.h. */
#include "raft/raft_sim.h"

#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/rng.h"

#define NODES_MAX RAFT_MEMBERS_MAX
#define TICK_NS 100000ull     /* every node is ticked this often (virtual) */
#define SWEEP_NS 1000000ull   /* pairwise log checks this often */
#define PEND_BUCKETS 65536 /* a power of two */
#define PEND_MAX 65536 /* proposals tracked for latency; a deeper backlog is not counted */
#define READS_MAX 4096

/* ------------------------------------------------------------------ hashing */

static uint64_t mix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ull;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebull;
	x ^= x >> 31;
	return x;
}

static uint64_t chain_next(uint64_t prev, raft_index index, raft_term term, uint8_t type,
			   const void *data, size_t len)
{
	uint64_t h = 0xcbf29ce484222325ull;
	const uint8_t *p = data;
	for (size_t i = 0; i < len; i++)
		h = (h ^ p[i]) * 0x100000001b3ull;
	return mix64(prev ^ mix64(index * 0x9e3779b97f4a7c15ull ^ term * 0xd1b54a32d192ed03ull ^
				  h ^ type));
}

/* ------------------------------------------------------------------ in-memory log store */

typedef struct {
	raft_index index;
	raft_term term;
	uint8_t type;
	uint32_t len;
	uint8_t *data;
	uint64_t chain;
} mementry;

/* What `ml_get` hands out: copies, as the file log's read arena holds them (plan 5a-i-g, #201). */
typedef struct memblock {
	struct memblock *older;
	size_t used, cap;
	uint8_t data[];
} memblock;

#define MEMBLOCK 65536
#define POISON 0xdb

typedef struct {
	raft_index base_index;
	raft_term base_term;
	uint64_t base_chain;
	mementry *e;
	size_t n, cap;
	raft_index durable; /* index the disk holds through */
	memblock *arena;    /* the newest first */
	/* The indices of the CONFIG entries the log holds, ascending. The invariant "one
	 * configuration change in flight" asks how many lie above the commit index after every
	 * step; counting them off this list costs the number of changes, not the length of the
	 * uncommitted tail (a leader cut off from its followers at three thousand proposals a second
	 * has twenty thousand of those, and walking them after every step was most of a run). */
	raft_index *cfg;
	size_t ncfg, capcfg;
} memlog;

static mementry *ml_at(memlog *l, raft_index i)
{
	if (i <= l->base_index || i > l->base_index + l->n)
		return NULL;
	return &l->e[i - l->base_index - 1];
}

static uint64_t ml_chain_at(memlog *l, raft_index i)
{
	if (i == l->base_index)
		return l->base_chain;
	mementry *e = ml_at(l, i);
	return e ? e->chain : 0;
}

static raft_index ml_first(void *log) { return ((memlog *)log)->base_index + 1; }
static raft_index ml_last(void *log)
{
	memlog *l = log;
	return l->base_index + l->n;
}

static raft_term ml_term_at(void *log, raft_index i)
{
	memlog *l = log;
	if (i == l->base_index)
		return l->base_term;
	mementry *e = ml_at(l, i);
	return e ? e->term : 0;
}

/* A copy of `len` bytes in the arena, where nothing handed out since the last release lives. */
static const void *ml_copy(memlog *l, const void *data, size_t len)
{
	if (!len)
		return NULL;
	memblock *b = l->arena;
	if (!b || b->cap - b->used < len) {
		size_t cap = len > MEMBLOCK ? len : MEMBLOCK;
		memblock *nb = iso_xmalloc(sizeof *nb + cap);
		nb->older = b;
		nb->used = 0;
		nb->cap = cap;
		l->arena = b = nb;
	}
	void *p = b->data + b->used;
	memcpy(p, data, len);
	b->used += len;
	return p;
}

/* **The entries the core is handed are copies, good until `ml_release`** -- the file log's own
 * contract (raft_log.c, the read arena), so that the fuzz holds the core to it: a copy the core
 * still names after it released them reads as poison, or as whatever a later read put there, and
 * the chain of the state machine that applies it, or of the follower it was shipped to, is not
 * the committed one. Handing out the log's own bytes, as this did, kept every pointer good until
 * the entry itself was dropped, which the daemon's log never promised. */
static unsigned ml_get(void *log, raft_index lo, raft_index hi, size_t max_bytes, raft_entry *out,
		       unsigned max_n)
{
	memlog *l = log;
	unsigned n = 0;
	size_t bytes = 0;
	for (raft_index i = lo; i <= hi && n < max_n; i++) {
		mementry *e = ml_at(l, i);
		if (!e)
			break;
		if (n && bytes + e->len > max_bytes)
			break;
		out[n++] = (raft_entry){ .index = e->index, .term = e->term, .type = e->type,
					 .len = e->len, .data = ml_copy(l, e->data, e->len) };
		bytes += e->len;
	}
	return n;
}

/* Poisoned before it is reused, so that a pointer kept past it reads as nothing an entry holds.
 * The oldest block is kept and rewound, as the file log keeps its first. */
static void ml_release(void *log)
{
	memlog *l = log;
	memblock *b = l->arena;
	while (b) {
		memblock *older = b->older;
		memset(b->data, POISON, b->used);
		if (older) {
			free(b);
		} else {
			b->used = 0;
			l->arena = b;
		}
		b = older;
	}
}

static void ml_free_arena(memlog *l)
{
	memblock *b = l->arena;
	while (b) {
		memblock *older = b->older;
		free(b);
		b = older;
	}
	l->arena = NULL;
}

static void ml_drop_from(memlog *l, size_t keep)
{
	for (size_t i = keep; i < l->n; i++)
		free(l->e[i].data);
	l->n = keep;
	while (l->ncfg && l->cfg[l->ncfg - 1] > l->base_index + keep)
		l->ncfg--;
}

static iso_err ml_append(void *log, const raft_entry *es, unsigned n)
{
	memlog *l = log;
	for (unsigned i = 0; i < n; i++) {
		if (es[i].index != ml_last(l) + 1)
			return ISO_ERR_INVAL;
		if (l->n == l->cap) {
			l->cap = l->cap ? l->cap * 2 : 64;
			l->e = iso_xrealloc(l->e, l->cap * sizeof *l->e);
		}
		mementry *e = &l->e[l->n++];
		e->index = es[i].index;
		e->term = es[i].term;
		e->type = es[i].type;
		e->len = es[i].len;
		e->data = es[i].len ? iso_xmemdup(es[i].data, es[i].len) : NULL;
		e->chain = chain_next(ml_chain_at(l, e->index - 1), e->index, e->term, e->type,
				      e->data, e->len);
		if (e->type == RAFT_ENTRY_CONFIG) {
			if (l->ncfg == l->capcfg) {
				l->capcfg = l->capcfg ? l->capcfg * 2 : 8;
				l->cfg = iso_xrealloc(l->cfg, l->capcfg * sizeof *l->cfg);
			}
			l->cfg[l->ncfg++] = e->index;
		}
	}
	return ISO_OK;
}

static iso_err ml_truncate(void *log, raft_index from)
{
	memlog *l = log;
	if (from <= l->base_index)
		return ISO_ERR_INVAL;
	if (from <= ml_last(l))
		ml_drop_from(l, from - l->base_index - 1);
	if (l->durable >= from)
		l->durable = from - 1;
	return ISO_OK;
}

static iso_err ml_reset(void *log, raft_index index, raft_term term)
{
	memlog *l = log;
	ml_drop_from(l, 0);
	l->base_index = index;
	l->base_term = term;
	l->base_chain = 0; /* the image's chain is filled in when it is installed */
	l->durable = index;
	return ISO_OK;
}

static iso_err ml_compact(void *log, raft_index upto)
{
	memlog *l = log;
	if (upto <= l->base_index)
		return ISO_OK;
	if (upto > ml_last(l))
		return ISO_ERR_INVAL;
	size_t drop = upto - l->base_index;
	raft_term t = l->e[drop - 1].term;
	uint64_t chain = l->e[drop - 1].chain;
	for (size_t i = 0; i < drop; i++)
		free(l->e[i].data);
	memmove(l->e, l->e + drop, (l->n - drop) * sizeof *l->e);
	l->n -= drop;
	size_t gone = 0;
	while (gone < l->ncfg && l->cfg[gone] <= upto)
		gone++;
	if (gone) { /* (memmove from a NULL list is undefined, empty or not) */
		memmove(l->cfg, l->cfg + gone, (l->ncfg - gone) * sizeof *l->cfg);
		l->ncfg -= gone;
	}
	l->base_index = upto;
	l->base_term = t;
	l->base_chain = chain;
	if (l->durable < upto)
		l->durable = upto;
	return ISO_OK;
}

static const raft_log_ops ml_ops = {
	.first = ml_first, .last = ml_last, .term_at = ml_term_at, .get = ml_get,
	.append = ml_append, .truncate = ml_truncate, .reset = ml_reset, .compact = ml_compact,
	.release = ml_release,
};

/* ------------------------------------------------------------------ world */

typedef struct {
	raft_index index;
	raft_term term;
	uint64_t chain;
	raft_config config;
} simimage;

typedef struct {
	raft_msg m;
	raft_entry *ents;
	uint8_t *blob;
} simmsg;

typedef enum {
	EV_TICK,
	EV_MSG,
	EV_SYNC,
	EV_HARD,
	EV_RESTART,
	EV_HEAL,
	EV_PROPOSE,
	EV_READ,
	EV_CRASH,
	EV_PARTITION,
	EV_CHURN,
	EV_TRANSFER,
	EV_RESEED,      /* a voter halts and is re-seeded (plan 5a-i-g, L's demotion's repair) */
	EV_RESEED_STEP, /* the reseed of `node` asks again, or goes ahead */
} evkind;

typedef struct {
	uint64_t time;
	uint64_t seq;
	evkind kind;
	raft_id node;
	unsigned incarnation;
	union {
		simmsg *msg;
		struct {
			raft_index index;
			raft_term term;
		} sync;
		raft_hard_state hs;
	} u;
} event;

typedef struct {
	raft_id id;
	bool up;
	unsigned incarnation;
	raft *r;
	memlog log;
	raft_hard_state hard_durable;
	raft_config initial_config;
	double clock_rate;
	unsigned group;
	bool draining; /* a drain asked for from inside one is the outer one's (group.c's guard) */

	/* A suspend (raft_sim_suspend): no input while `suspended`; with `clock_stops` its clock
	 * stands at `suspended_at`, and `stopped_ns` is the virtual time every finished stop took off
	 * it (plan 28y, #429). */
	bool suspended;
	bool clock_stops;
	uint64_t suspended_at;
	uint64_t stopped_ns;

	/* the state machine, durable by construction (isokv commits every apply batch) */
	raft_index applied;
	raft_term applied_term;
	uint64_t chain;
	raft_config config;
	raft_index last_snapshot;

	/* A reseed under way: the node halted, and stays down -- nothing restarts it -- until the
	 * leader has committed the change that makes it a learner, or the reseed gives up. */
	bool reseeding;
	uint64_t reseed_until;

	/* bookkeeping for the invariants */
	raft_term seen_term;
	raft_index seen_commit;
	raft_term leader_term;
	raft_index leader_last;
	uint64_t leader_last_chain;

	/* The progress check (raft_sim_params.progress_ns): when the node's stretch of being
	 * eligible began (0 = not in one), the leader's commit then, and what the stretch is
	 * keyed on, so that any change of it starts a new one. */
	uint64_t prog_start;
	raft_index prog_target;
	raft_id prog_leader;
	raft_term prog_lterm;
	unsigned prog_linc, prog_xinc, prog_group;
} simnode;

typedef struct {
	raft_index index;
	raft_term term;
	raft_id leader;
	uint64_t issued;
	uint32_t bnext; /* the next entry in this one's bucket, slot + 1; 0 ends the chain */
} pending;

typedef struct {
	uint64_t ctx;
	raft_index need;
	raft_id at;
} pending_read;

struct raft_sim {
	raft_sim_params p;
	iso_rng rng;
	uint64_t now;
	uint64_t evseq;
	unsigned n;
	simnode node[NODES_MAX + 1];

	event *heap;
	size_t hn, hcap;

	/* the committed prefix as the world knows it */
	raft_term *cterm;
	uint64_t *cchain;
	size_t ccap;
	raft_index cmax;
	raft_index commit_high; /* highest commit index any node ever reported */

	raft_id *term_leader;
	size_t tlcap;

	/* Whom each node granted its vote in each term, `[term * (NODES_MAX + 1) + node]`, 0 for
	 * nobody: kept across crashes, which is what makes a second grant in one term visible, and
	 * what a leader's election is counted from (a voter's live hard state may have moved on). */
	raft_id *grant;
	size_t gcap; /* terms the array has room for */

	uint64_t next_value;
	pending pend[PEND_MAX];
	unsigned pend_n, pend_head, pend_cap;
	uint32_t pend_mask; /* buckets in use less one: a power of two, at most PEND_BUCKETS */
	uint32_t pend_bucket[PEND_BUCKETS]; /* by (index, leader): the newest entry's slot + 1 */
	pending_read reads[READS_MAX];
	unsigned n_reads;
	uint64_t next_ctx;

	uint64_t last_delivery[NODES_MAX + 1][NODES_MAX + 1]; /* per pair, to keep order */
	bool cut[NODES_MAX + 1][NODES_MAX + 1]; /* raft_sim_cut_link: nothing from [a] reaches [b] */
	unsigned lose_every;                   /* raft_sim_lose_first_answers */
	uint32_t lose_last[NODES_MAX + 1];      /* the round whose first answer each member lost */
	raft_id last_leader;
	uint64_t next_sweep;
	raft_sim_stats st;
	bool failed;
	char failure[256];
	FILE *trace;
};

#define TRACE(s, ...)                                                          \
	do {                                                                   \
		if ((s)->trace) {                                              \
			fprintf((s)->trace, "%9.4f ", (double)(s)->now / 1e9);  \
			fprintf((s)->trace, __VA_ARGS__);                      \
			fputc('\n', (s)->trace);                               \
		}                                                              \
	} while (0)

void raft_sim_params_defaults(raft_sim_params *p)
{
	memset(p, 0, sizeof *p);
	p->seed = 1;
	p->voters = 3;
	p->delay_min_ns = 1000000;  /* 1 ms */
	p->delay_max_ns = 10000000; /* 10 ms */
	p->sync_min_ns = 100000;    /* 100 us */
	p->sync_max_ns = 2000000;   /* 2 ms */
	p->partition_ns = 3000ull * 1000000;
	p->restart_min_ns = 100ull * 1000000;
	p->restart_max_ns = 5000ull * 1000000;
	p->payload_max = 64;
	raft_opts_defaults(&p->core);
	p->core.snapshot_retain = 20;
	p->core.snapshot_resend_ns = 500ull * 1000000;
}

/* ---- failure */

static void ISO_PRINTF(2, 3) fail(raft_sim *s, const char *fmt, ...)
{
	if (s->failed)
		return;
	s->failed = true;
	int n = snprintf(s->failure, sizeof s->failure, "seed %llu t=%.3fs: ",
			 (unsigned long long)s->p.seed, (double)s->now / 1e9);
	if (n < 0 || (size_t)n >= sizeof s->failure)
		return;
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(s->failure + n, sizeof s->failure - (size_t)n, fmt, ap);
	va_end(ap);
}

/* ---- event heap */

static void heap_swap(event *a, event *b)
{
	event t = *a;
	*a = *b;
	*b = t;
}

static bool ev_before(const event *a, const event *b)
{
	return a->time < b->time || (a->time == b->time && a->seq < b->seq);
}

static void schedule(raft_sim *s, event ev)
{
	ev.seq = s->evseq++;
	if (s->hn == s->hcap) {
		s->hcap = s->hcap ? s->hcap * 2 : 256;
		s->heap = iso_xrealloc(s->heap, s->hcap * sizeof *s->heap);
	}
	size_t i = s->hn++;
	s->heap[i] = ev;
	while (i > 0) {
		size_t parent = (i - 1) / 2;
		if (!ev_before(&s->heap[i], &s->heap[parent]))
			break;
		heap_swap(&s->heap[i], &s->heap[parent]);
		i = parent;
	}
}

static event heap_pop(raft_sim *s)
{
	event top = s->heap[0];
	s->heap[0] = s->heap[--s->hn];
	size_t i = 0;
	for (;;) {
		size_t l = 2 * i + 1, rr = l + 1, m = i;
		if (l < s->hn && ev_before(&s->heap[l], &s->heap[m]))
			m = l;
		if (rr < s->hn && ev_before(&s->heap[rr], &s->heap[m]))
			m = rr;
		if (m == i)
			break;
		heap_swap(&s->heap[i], &s->heap[m]);
		i = m;
	}
	return top;
}

static void free_msg(simmsg *m)
{
	if (!m)
		return;
	free(m->ents);
	free(m->blob);
	free(m);
}

static uint64_t rnd_range(raft_sim *s, uint64_t lo, uint64_t hi)
{
	return hi > lo ? lo + iso_rng_below(&s->rng, hi - lo + 1) : lo;
}

/* Exponential inter-arrival time for a rate per second, in ns. */
static uint64_t expo(raft_sim *s, double rate)
{
	double u = iso_rng_double(&s->rng);
	if (u < 1e-12)
		u = 1e-12;
	double secs = -log(u) / rate;
	if (secs > 3600.0)
		secs = 3600.0;
	return (uint64_t)(secs * 1e9);
}

static void arm(raft_sim *s, evkind kind, double rate)
{
	if (rate > 0)
		schedule(s, (event){ .time = s->now + expo(s, rate), .kind = kind });
}

/* ---- committed prefix */

static void committed_reserve(raft_sim *s, raft_index i)
{
	if (i < s->ccap)
		return;
	size_t cap = s->ccap ? s->ccap : 1024;
	while (cap <= i)
		cap *= 2;
	s->cterm = iso_xrealloc(s->cterm, cap * sizeof *s->cterm);
	s->cchain = iso_xrealloc(s->cchain, cap * sizeof *s->cchain);
	memset(s->cterm + s->ccap, 0, (cap - s->ccap) * sizeof *s->cterm);
	memset(s->cchain + s->ccap, 0, (cap - s->ccap) * sizeof *s->cchain);
	s->ccap = cap;
}

/* Record that `index` is committed with this term and prefix hash, or fail if the world already
 * knows a different entry there: state-machine safety. */
static void note_committed(raft_sim *s, raft_id who, raft_index index, raft_term term,
			   uint64_t chain)
{
	committed_reserve(s, index);
	if (s->cterm[index]) {
		if (s->cterm[index] != term || s->cchain[index] != chain)
			fail(s, "node %u applied index %llu term %llu but the committed entry there "
				"has term %llu (state machine safety)",
			     who, (unsigned long long)index, (unsigned long long)term,
			     (unsigned long long)s->cterm[index]);
		return;
	}
	s->cterm[index] = term;
	s->cchain[index] = chain;
	if (index > s->cmax)
		s->cmax = index;
}

/* ---- nodes */

/* A node's own clock: its rate times the virtual time it has seen run. A suspend that stops the
 * clock (CLOCK_MONOTONIC's) takes its span off for good; one that does not (CLOCK_BOOTTIME's) takes
 * nothing, so the first reading after it sees the whole span. */
static double local_time(const simnode *nd, uint64_t now)
{
	uint64_t ran = (nd->suspended && nd->clock_stops ? nd->suspended_at : now) - nd->stopped_ns;
	return (double)ran * nd->clock_rate;
}

static void node_resume(raft_sim *s, simnode *nd);

/* A re-seeded node's vote fence (L's demotion's repair), as the simulator reads it from what the
 * node holds and not from the core: a core that ignored its fence is caught voting or campaigning
 * (check_send, on_state). */
static bool sim_fenced(simnode *nd)
{
	const raft_hard_state *h = &nd->hard_durable;
	if (!h->fence_index || nd->log.base_index >= h->fence_index)
		return false;
	return ml_term_at(&nd->log, h->fence_index) != h->fence_term;
}

static void node_boot(raft_sim *s, simnode *nd)
{
	raft_boot b = { .hard = nd->hard_durable, .applied = nd->applied,
			.applied_term = nd->applied_term, .config = nd->config };
	raft_opts o = s->p.core;
	o.seed = s->p.seed * 1000003ull + nd->id * 7919ull + nd->incarnation;
	char err[128];
	nd->r = raft_new(nd->id, &o, &ml_ops, &nd->log, &b, err, sizeof err);
	if (!nd->r) {
		fail(s, "node %u: raft_new: %s", nd->id, err);
		return;
	}
	nd->up = true;
	nd->seen_term = nd->hard_durable.term;
	nd->seen_commit = nd->applied;
	nd->leader_term = 0;
	raft_tick(nd->r, (uint64_t)local_time(nd, s->now));
}

static void check_node(raft_sim *s, simnode *nd);
static void drain(raft_sim *s, simnode *nd);

static void node_crash(raft_sim *s, simnode *nd)
{
	if (!nd->up)
		return;
	if (nd->suspended) { /* a crash ends a suspend; the restart reads the clock afresh */
		if (nd->clock_stops)
			nd->stopped_ns += s->now - nd->suspended_at;
		nd->suspended = false;
	}
	raft_free(nd->r);
	nd->r = NULL;
	nd->up = false;
	nd->incarnation++;
	/* Everything after the durable prefix was in memory. */
	if (ml_last(&nd->log) > nd->log.durable)
		ml_drop_from(&nd->log, nd->log.durable - nd->log.base_index);
	s->st.crashes++;
	TRACE(s, "n%u crash: durable last=%llu term=%llu applied=%llu", nd->id,
	      (unsigned long long)ml_last(&nd->log), (unsigned long long)nd->hard_durable.term,
	      (unsigned long long)nd->applied);
}

/* ---- the state machine */

/* The proposals in flight are found by (index, leader), newest first in a bucket, so that the
 * oldest match -- the one the first-match scan this replaced took -- is the last one met. The scan
 * was linear in the tracked proposals on every apply, which at three thousand proposals a second
 * is most of a run. */
static unsigned pend_hash(const raft_sim *s, raft_index index, raft_id leader)
{
	return (unsigned)(mix64(index * 0x9e3779b97f4a7c15ull + leader) & s->pend_mask);
}

static void pend_unlink(raft_sim *s, unsigned slot)
{
	uint32_t *at = &s->pend_bucket[pend_hash(s, s->pend[slot].index, s->pend[slot].leader)];
	while (*at != slot + 1)
		at = &s->pend[*at - 1].bnext;
	*at = s->pend[slot].bnext;
}

static void apply_entry(raft_sim *s, simnode *nd, const raft_entry *e)
{
	if (e->index != nd->applied + 1) {
		fail(s, "node %u applied index %llu after %llu (gap)", nd->id,
		     (unsigned long long)e->index, (unsigned long long)nd->applied);
		return;
	}
	uint64_t chain = chain_next(nd->chain, e->index, e->term, e->type, e->data, e->len);
	uint64_t logchain = ml_chain_at(&nd->log, e->index);
	if (logchain && logchain != chain)
		fail(s, "node %u applied an entry at %llu that differs from its own log", nd->id,
		     (unsigned long long)e->index);
	note_committed(s, nd->id, e->index, e->term, chain);
	nd->chain = chain;
	nd->applied = e->index;
	nd->applied_term = e->term;
	if (e->type == RAFT_ENTRY_CONFIG) {
		raft_config c;
		if (raft_config_decode(e->data, e->len, &c)) {
			nd->config = c;
			TRACE(s, "n%u applied CONFIG at %llu (%u members, %u voters)", nd->id,
			      (unsigned long long)e->index, c.n, raft_config_voters(&c));
		} else
			fail(s, "node %u applied an undecodable CONFIG entry at %llu", nd->id,
			     (unsigned long long)e->index);
	}
	if (nd->applied > s->st.max_applied)
		s->st.max_applied = nd->applied;

	/* Was this one of ours, and did the leader that took it just apply it? */
	pending *p = NULL;
	for (uint32_t at = s->pend_bucket[pend_hash(s, e->index, nd->id)]; at; at = s->pend[at - 1].bnext)
		if (s->pend[at - 1].index == e->index && s->pend[at - 1].leader == nd->id)
			p = &s->pend[at - 1]; /* the oldest is the last */
	if (s->p.cross_check) {
		pending *ref = NULL;
		for (unsigned i = 0; i < s->pend_n && !ref; i++) {
			pending *q = &s->pend[(s->pend_head + i) % s->pend_cap];
			if (q->index == e->index && q->leader == nd->id)
				ref = q;
		}
		if (ref != p)
			fail(s, "node %u applied index %llu: the tracked proposal is %s the oldest one "
				"that matches", nd->id, (unsigned long long)e->index,
			     p ? "not" : "missing, though there is");
	}
	if (p && p->term == e->term) {
		uint64_t lat = s->now - p->issued;
		s->st.committed++;
		s->st.commit_latency_sum_ns += lat;
		if (lat > s->st.commit_latency_max_ns)
			s->st.commit_latency_max_ns = lat;
	}
}

static void install_image(raft_sim *s, simnode *nd, const raft_action *a)
{
	if (a->u.snapshot.len != sizeof(simimage) || !a->u.snapshot.data) {
		fail(s, "node %u: RESTORE without an image", nd->id);
		return;
	}
	simimage img;
	memcpy(&img, a->u.snapshot.data, sizeof img);
	if (img.index != a->u.snapshot.index || img.term != a->u.snapshot.term) {
		fail(s, "node %u: RESTORE describes %llu/%llu but the image is %llu/%llu", nd->id,
		     (unsigned long long)a->u.snapshot.index, (unsigned long long)a->u.snapshot.term,
		     (unsigned long long)img.index, (unsigned long long)img.term);
		return;
	}
	if (img.index < s->ccap && s->cterm[img.index] &&
	    (s->cterm[img.index] != img.term || s->cchain[img.index] != img.chain))
		fail(s, "node %u restored an image at %llu that contradicts the committed prefix",
		     nd->id, (unsigned long long)img.index);
	if (img.index <= nd->applied)
		fail(s, "node %u restored an image at %llu below its applied %llu", nd->id,
		     (unsigned long long)img.index, (unsigned long long)nd->applied);
	if (nd->log.base_index != img.index)
		fail(s, "node %u: log base %llu after a restore to %llu", nd->id,
		     (unsigned long long)nd->log.base_index, (unsigned long long)img.index);
	nd->log.base_chain = img.chain;
	nd->applied = img.index;
	nd->applied_term = img.term;
	nd->chain = img.chain;
	nd->config = img.config;
	nd->last_snapshot = img.index;
	note_committed(s, nd->id, img.index, img.term, img.chain);
	s->st.snapshots_installed++;
	TRACE(s, "n%u installed snapshot %llu/%llu from n%u", nd->id, (unsigned long long)img.index,
	      (unsigned long long)img.term, a->u.snapshot.from);
	if (nd->applied > s->st.max_applied)
		s->st.max_applied = nd->applied;
}

/* ---- the network */

static simmsg *copy_msg(raft_sim *s, simnode *from, const raft_msg *m)
{
	simmsg *c = iso_xcalloc(1, sizeof *c);
	c->m = *m;
	if (m->type == RAFT_MSG_APPEND && m->u.append.n) {
		unsigned n = m->u.append.n;
		size_t bytes = 0;
		for (unsigned i = 0; i < n; i++)
			bytes += m->u.append.entries[i].len;
		c->ents = iso_xcalloc(n, sizeof *c->ents);
		c->blob = iso_xmalloc(bytes ? bytes : 1);
		size_t off = 0;
		for (unsigned i = 0; i < n; i++) {
			const raft_entry *e = &m->u.append.entries[i];
			c->ents[i] = *e;
			if (e->len)
				memcpy(c->blob + off, e->data, e->len);
			c->ents[i].data = c->blob + off;
			off += e->len;
		}
		c->m.u.append.entries = c->ents;
	} else if (m->type == RAFT_MSG_SNAPSHOT) {
		/* The transport ships the sender's state as it stands, which may be newer than
		 * what the core described; the message is corrected to match the image. */
		simimage img = { .index = from->applied, .term = from->applied_term,
				 .chain = from->chain, .config = from->config };
		if (img.index < m->u.snapshot.index)
			fail(s, "node %u offered a snapshot at %llu above its applied %llu", from->id,
			     (unsigned long long)m->u.snapshot.index, (unsigned long long)img.index);
		c->blob = iso_xmemdup(&img, sizeof img);
		c->m.u.snapshot.index = img.index;
		c->m.u.snapshot.term = img.term;
		c->m.u.snapshot.config = img.config;
		c->m.u.snapshot.data = c->blob;
		c->m.u.snapshot.len = sizeof img;
	}
	return c;
}

static raft_id *grant_slot(raft_sim *s, raft_term term, raft_id node)
{
	if (term >= s->gcap) {
		size_t cap = s->gcap ? s->gcap : 256;
		while (cap <= term)
			cap *= 2;
		size_t row = NODES_MAX + 1;
		s->grant = iso_xrealloc(s->grant, cap * row * sizeof *s->grant);
		memset(s->grant + s->gcap * row, 0, (cap - s->gcap) * row * sizeof *s->grant);
		s->gcap = cap;
	}
	return &s->grant[term * (NODES_MAX + 1) + node];
}

static void check_send(raft_sim *s, simnode *nd, const raft_msg *m)
{
	bool prevote = m->type == RAFT_MSG_PREVOTE || m->type == RAFT_MSG_PREVOTE_RESP;
	const raft_hard_state *h = &nd->hard_durable;
	if (!prevote && h->term < m->term)
		fail(s, "node %u sent %s for term %llu with durable term %llu (persisted before send)",
		     nd->id, raft_msg_type_name(m->type), (unsigned long long)m->term,
		     (unsigned long long)h->term);
	if (m->type == RAFT_MSG_VOTE && (h->vote != nd->id || h->term != m->term))
		fail(s, "node %u asked for votes in term %llu before saving its own", nd->id,
		     (unsigned long long)m->term);
	if (m->type == RAFT_MSG_VOTE)
		TRACE(s, "n%u asks n%u for a vote in term %llu (last %llu/%llu%s)", nd->id, m->to,
		      (unsigned long long)m->term, (unsigned long long)m->u.vote.last_index,
		      (unsigned long long)m->u.vote.last_term, m->u.vote.transfer ? ", transfer" : "");
	if (m->type == RAFT_MSG_VOTE_RESP)
		TRACE(s, "n%u %s n%u in term %llu (own last %llu/%llu, commit %llu)", nd->id,
		      m->u.vote_resp.granted ? "votes for" : "refuses", m->to,
		      (unsigned long long)m->term, (unsigned long long)ml_last(&nd->log),
		      (unsigned long long)ml_term_at(&nd->log, ml_last(&nd->log)),
		      (unsigned long long)raft_get_commit(nd->r));
	if (m->type == RAFT_MSG_VOTE_RESP && m->u.vote_resp.granted) {
		if (h->vote != m->to || h->term != m->term)
			fail(s, "node %u granted a vote to %u in term %llu before saving it", nd->id,
			     m->to, (unsigned long long)m->term);
		/* A learner votes as a voter does (raft.c, raft_step); who counts it is the
		 * candidate's configuration, and on_state checks that a leader's election was won by
		 * voters of its own. Here: at most one candidate a term, across crashes, and a count
		 * of the votes a node that read itself as a learner gave, so that a run shows the
		 * path was taken. */
		raft_id *g = grant_slot(s, m->term, nd->id);
		if (*g && *g != m->to)
			fail(s, "node %u granted n%u and n%u in term %llu", nd->id, *g, m->to,
			     (unsigned long long)m->term);
		*g = m->to;
		const raft_member *me = raft_config_find(raft_get_config(nd->r), nd->id);
		if (me && me->learner)
			s->st.learner_votes++;
	}
	if ((m->type == RAFT_MSG_VOTE_RESP || m->type == RAFT_MSG_PREVOTE_RESP) &&
	    m->u.vote_resp.granted && sim_fenced(nd))
		fail(s, "node %u granted a %s to %u while its log does not hold %llu/%llu, its "
			"vote fence",
		     nd->id, raft_msg_type_name(m->type), m->to,
		     (unsigned long long)nd->hard_durable.fence_index,
		     (unsigned long long)nd->hard_durable.fence_term);
	if (m->type == RAFT_MSG_APPEND_RESP)
		TRACE(s, "n%u acks n%u %s index %llu (durable %llu, last %llu)", nd->id, m->to,
		      m->u.append_resp.ok ? "ok" : "reject",
		      (unsigned long long)m->u.append_resp.index, (unsigned long long)nd->log.durable,
		      (unsigned long long)ml_last(&nd->log));
	if (m->type == RAFT_MSG_SNAPSHOT)
		TRACE(s, "n%u sends snapshot %llu to n%u", nd->id,
		      (unsigned long long)m->u.snapshot.index, m->to);
	if (m->type == RAFT_MSG_APPEND_RESP && m->u.append_resp.ok &&
	    nd->log.durable < m->u.append_resp.index)
		fail(s, "node %u acknowledged index %llu with only %llu durable", nd->id,
		     (unsigned long long)m->u.append_resp.index,
		     (unsigned long long)nd->log.durable);
}

static void send_msg(raft_sim *s, simnode *from, const raft_msg *m)
{
	s->st.messages_sent++;
	check_send(s, from, m);
	if (m->to == 0 || m->to > s->n || m->to == from->id) {
		fail(s, "node %u sent %s to %u", from->id, raft_msg_type_name(m->type), m->to);
		return;
	}
	simnode *to = &s->node[m->to];
	if (s->lose_every && m->type == RAFT_MSG_APPEND_RESP && m->u.append_resp.seq &&
	    m->u.append_resp.seq % s->lose_every == 0 &&
	    m->u.append_resp.seq != s->lose_last[from->id]) {
		s->lose_last[from->id] = m->u.append_resp.seq;
		s->st.messages_dropped++;
		return;
	}
	if (!to->up || to->group != from->group || s->cut[from->id][m->to] ||
	    iso_rng_chance(&s->rng, s->p.drop)) {
		s->st.messages_dropped++;
		return;
	}
	unsigned copies = 1 + (iso_rng_chance(&s->rng, s->p.dup) ? 1 : 0);
	for (unsigned i = 0; i < copies; i++) {
		uint64_t t = s->now + rnd_range(s, s->p.delay_min_ns, s->p.delay_max_ns);
		uint64_t *last = &s->last_delivery[from->id][m->to];
		if (!s->p.reorder && t < *last)
			t = *last;
		*last = t;
		event ev = { .time = t, .kind = EV_MSG, .node = m->to };
		ev.u.msg = copy_msg(s, from, m);
		schedule(s, ev);
	}
}

/* ---- actions */

static void on_state(raft_sim *s, simnode *nd, const raft_action *a)
{
	raft_term term = a->u.state.term;
	TRACE(s, "n%u is %s in term %llu (leader n%u, last %llu, commit %llu)", nd->id,
	      raft_role_name(a->u.state.role), (unsigned long long)term, a->u.state.leader,
	      (unsigned long long)ml_last(&nd->log), (unsigned long long)raft_get_commit(nd->r));
	if (term > s->st.max_term)
		s->st.max_term = term;
	const raft_member *me = raft_config_find(raft_get_config(nd->r), nd->id);
	if (a->u.state.role != RAFT_FOLLOWER && me && me->learner)
		fail(s, "node %u became %s while a learner", nd->id, raft_role_name(a->u.state.role));
	if (a->u.state.role != RAFT_FOLLOWER && sim_fenced(nd))
		fail(s, "node %u became %s while its vote fence holds", nd->id,
		     raft_role_name(a->u.state.role));
	if (a->u.state.role != RAFT_LEADER)
		return;
	if (term >= s->tlcap) {
		size_t cap = s->tlcap ? s->tlcap : 256;
		while (cap <= term)
			cap *= 2;
		s->term_leader = iso_xrealloc(s->term_leader, cap * sizeof *s->term_leader);
		memset(s->term_leader + s->tlcap, 0, (cap - s->tlcap) * sizeof *s->term_leader);
		s->tlcap = cap;
	}
	if (s->term_leader[term] && s->term_leader[term] != nd->id)
		fail(s, "term %llu has two leaders, %u and %u (election safety)",
		     (unsigned long long)term, s->term_leader[term], nd->id);
	if (s->term_leader[term] == nd->id)
		return; /* the same leadership reported again (leader changed) */
	s->term_leader[term] = nd->id;
	/* **A leader was elected by a quorum of the voters of its own configuration**: itself and
	 * those that granted it this term. The configuration read now is the one the tally used
	 * (the no-op that opens a term changes none). This is the design rule that "a learner never
	 * votes" stood in for. */
	{
		const raft_config *c = raft_get_config(nd->r);
		unsigned voters = 0, votes = 0;
		for (uint32_t i = 0; i < c->n; i++) {
			if (c->m[i].learner)
				continue;
			voters++;
			votes += c->m[i].id == nd->id || *grant_slot(s, term, c->m[i].id) == nd->id;
		}
		if (votes < voters / 2 + 1)
			fail(s, "n%u led term %llu with %u of %u votes from voters of its own "
				"configuration (needs %u)",
			     nd->id, (unsigned long long)term, votes, voters, voters / 2 + 1);
	}
	if (nd->id != s->last_leader) {
		s->st.leader_changes++;
		s->last_leader = nd->id;
	}
	/* Leader completeness: the committed prefix is in the new leader's log. */
	if (s->cmax) {
		raft_index last = ml_last(&nd->log);
		if (last < s->cmax)
			fail(s, "node %u leads term %llu with last index %llu but %llu is committed "
				"(leader completeness)",
			     nd->id, (unsigned long long)term, (unsigned long long)last,
			     (unsigned long long)s->cmax);
		else if (s->cmax >= nd->log.base_index &&
			 ml_chain_at(&nd->log, s->cmax) != s->cchain[s->cmax])
			fail(s, "node %u leads term %llu with a log that differs from the committed "
				"prefix at %llu (leader completeness)",
			     nd->id, (unsigned long long)term, (unsigned long long)s->cmax);
	}
	nd->leader_term = term;
	nd->leader_last = ml_last(&nd->log);
	nd->leader_last_chain = ml_chain_at(&nd->log, nd->leader_last);
}

static void on_read_ready(raft_sim *s, simnode *nd, uint64_t ctx)
{
	for (unsigned i = 0; i < s->n_reads; i++) {
		pending_read *q = &s->reads[i];
		if (q->ctx != ctx)
			continue;
		if (q->at != nd->id)
			fail(s, "node %u answered read %llu that was asked of %u", nd->id,
			     (unsigned long long)ctx, q->at);
		else if (nd->applied < q->need)
			fail(s, "node %u served read %llu at applied %llu but %llu was committed when "
				"it was asked (stale read)",
			     nd->id, (unsigned long long)ctx, (unsigned long long)nd->applied,
			     (unsigned long long)q->need);
		s->st.reads_ok++;
		s->reads[i] = s->reads[--s->n_reads];
		return;
	}
}

static void on_read_failed(raft_sim *s, uint64_t ctx)
{
	for (unsigned i = 0; i < s->n_reads; i++) {
		if (s->reads[i].ctx != ctx)
			continue;
		s->st.reads_failed++;
		s->reads[i] = s->reads[--s->n_reads];
		return;
	}
}

static iso_err propose_on(raft_sim *s, simnode *nd, raft_id at, size_t pad,
			  raft_index *index_out);

/* A leader's state machine proposing from inside its apply, as the metadata service's does --
 * `verify start` a row a device (plan 5a-i-g, #201): one to three entries, as often of one length
 * as not, the drain each asks for being the outer one's. */
static void propose_inside(raft_sim *s, simnode *nd)
{
	if (s->p.apply_propose <= 0 || raft_get_role(nd->r) != RAFT_LEADER ||
	    !iso_rng_chance(&s->rng, s->p.apply_propose))
		return;
	unsigned k = 1 + (unsigned)iso_rng_below(&s->rng, 3);
	bool same = iso_rng_chance(&s->rng, 0.5);
	size_t pad = s->p.payload_max ? iso_rng_below(&s->rng, ISO_MIN(s->p.payload_max, 256) + 1) : 0;
	for (unsigned i = 0; i < k && nd->up && !s->failed; i++) {
		if (!same && s->p.payload_max)
			pad = iso_rng_below(&s->rng, ISO_MIN(s->p.payload_max, 256) + 1);
		if (propose_on(s, nd, nd->id, pad, NULL) == ISO_OK)
			s->st.proposed_inside++;
	}
}

static void drain(raft_sim *s, simnode *nd)
{
	if (nd->draining)
		return;
	nd->draining = true;
	raft_action a;
	while (nd->up && !s->failed && raft_next_action(nd->r, &a)) {
		switch (a.type) {
		case RAFT_ACT_SAVE_HARD: {
			event ev = { .time = s->now + rnd_range(s, s->p.sync_min_ns, s->p.sync_max_ns),
				     .kind = EV_HARD, .node = nd->id, .incarnation = nd->incarnation };
			ev.u.hs = a.u.hard;
			schedule(s, ev);
			break;
		}
		case RAFT_ACT_SYNC: {
			event ev = { .time = s->now + rnd_range(s, s->p.sync_min_ns, s->p.sync_max_ns),
				     .kind = EV_SYNC, .node = nd->id, .incarnation = nd->incarnation };
			ev.u.sync.index = a.u.sync.index;
			ev.u.sync.term = a.u.sync.term;
			schedule(s, ev);
			break;
		}
		case RAFT_ACT_SEND:
			send_msg(s, nd, &a.u.msg);
			break;
		case RAFT_ACT_APPLY: {
			raft_index last = 0;
			for (unsigned i = 0; i < a.u.apply.n && !s->failed; i++) {
				apply_entry(s, nd, &a.u.apply.entries[i]);
				last = a.u.apply.entries[i].index;
				if (!s->failed)
					propose_inside(s, nd);
			}
			if (s->failed)
				break;
			raft_applied(nd->r, last);
			if (s->p.snapshot_every && nd->applied - nd->last_snapshot >= s->p.snapshot_every) {
				nd->last_snapshot = nd->applied;
				raft_compact(nd->r, nd->applied, nd->applied_term, &nd->config);
			}
			break;
		}
		case RAFT_ACT_RESTORE:
			install_image(s, nd, &a);
			break;
		case RAFT_ACT_READ_READY:
			on_read_ready(s, nd, a.u.read.ctx);
			break;
		case RAFT_ACT_READ_FAILED:
			on_read_failed(s, a.u.read.ctx);
			break;
		case RAFT_ACT_STATE:
			on_state(s, nd, &a);
			break;
		}
	}
	nd->draining = false;
	if (nd->up && !s->failed)
		check_node(s, nd);
}

/* ---- invariants */

static void check_node(raft_sim *s, simnode *nd)
{
	raft *r = nd->r;
	raft_term term = raft_get_term(r);
	raft_index commit = raft_get_commit(r), applied = raft_get_applied(r);
	if (term < nd->seen_term)
		fail(s, "node %u: term went from %llu to %llu", nd->id,
		     (unsigned long long)nd->seen_term, (unsigned long long)term);
	if (commit < nd->seen_commit)
		fail(s, "node %u: commit went from %llu to %llu", nd->id,
		     (unsigned long long)nd->seen_commit, (unsigned long long)commit);
	if (applied > commit)
		fail(s, "node %u: applied %llu above commit %llu", nd->id,
		     (unsigned long long)applied, (unsigned long long)commit);
	if (applied != nd->applied)
		fail(s, "node %u: core applied %llu, state machine at %llu", nd->id,
		     (unsigned long long)applied, (unsigned long long)nd->applied);
	if (commit / 100 != nd->seen_commit / 100)
		TRACE(s, "n%u commit %llu (last %llu, term %llu)", nd->id, (unsigned long long)commit,
		      (unsigned long long)ml_last(&nd->log), (unsigned long long)term);
	nd->seen_term = term;
	nd->seen_commit = commit;
	if (commit > s->commit_high)
		s->commit_high = commit;

	if (s->p.cross_check) {
		/* The list of configuration entries is the log's CONFIG entries, ascending, and no
		 * others: one left behind by a truncation, or lost by a compaction, is found here
		 * and not only on a leader that happens to count it. */
		size_t k = 0;
		for (raft_index i = nd->log.base_index + 1; i <= nd->log.base_index + nd->log.n; i++) {
			mementry *e = ml_at(&nd->log, i);
			if (e && e->type == RAFT_ENTRY_CONFIG &&
			    (k >= nd->log.ncfg || nd->log.cfg[k++] != i)) {
				fail(s, "node %u: the log's CONFIG entry at %llu is not the next in its "
					"list", nd->id, (unsigned long long)i);
				k = nd->log.ncfg + 1;
				break;
			}
		}
		if (k != nd->log.ncfg)
			fail(s, "node %u: the log's list names %zu configuration entries, the log "
				"holds %zu", nd->id, nd->log.ncfg, k);
	}

	if (raft_get_role(r) == RAFT_LEADER) {
		/* Leader append-only: what it had when it last looked is still there. */
		raft_index last = ml_last(&nd->log);
		if (nd->leader_term == term && nd->leader_last) {
			if (last < nd->leader_last)
				fail(s, "node %u (leader, term %llu) shortened its log from %llu to %llu",
				     nd->id, (unsigned long long)term,
				     (unsigned long long)nd->leader_last, (unsigned long long)last);
			else if (nd->leader_last >= nd->log.base_index &&
				 ml_chain_at(&nd->log, nd->leader_last) != nd->leader_last_chain)
				fail(s, "node %u (leader, term %llu) rewrote its log at %llu", nd->id,
				     (unsigned long long)term, (unsigned long long)nd->leader_last);
		}
		nd->leader_term = term;
		nd->leader_last = last;
		nd->leader_last_chain = ml_chain_at(&nd->log, last);
		/* One configuration change in flight at a time. */
		unsigned pending_cfg = 0;
		for (size_t i = nd->log.ncfg; i > 0 && nd->log.cfg[i - 1] > commit; i--)
			pending_cfg++;
		if (s->p.cross_check) {
			unsigned walked = 0;
			for (raft_index i = commit + 1; i <= last; i++) {
				mementry *e = ml_at(&nd->log, i);
				if (e && e->type == RAFT_ENTRY_CONFIG)
					walked++;
			}
			if (walked != pending_cfg)
				fail(s, "node %u (leader): %u configuration entries above the commit "
					"index by the log's list, %u by walking it", nd->id, pending_cfg,
				     walked);
		}
		if (pending_cfg > 1)
			fail(s, "node %u (leader) has %u uncommitted configuration entries", nd->id,
			     pending_cfg);
	}
}

/* Log matching, pairwise: where two logs agree on a term they agree on everything before. */
static void sweep(raft_sim *s)
{
	s->st.checks++;
	for (unsigned a = 1; a <= s->n; a++) {
		if (!s->node[a].up)
			continue;
		memlog *la = &s->node[a].log;
		for (unsigned b = a + 1; b <= s->n; b++) {
			if (!s->node[b].up)
				continue;
			memlog *lb = &s->node[b].log;
			raft_index i = ISO_MIN(ml_last(la), ml_last(lb));
			raft_index lo = ISO_MAX(la->base_index, lb->base_index);
			unsigned steps = 0;
			while (i > lo && steps++ < 64 && ml_term_at(la, i) != ml_term_at(lb, i))
				i--;
			if (i < lo || i == 0 || ml_term_at(la, i) != ml_term_at(lb, i))
				continue;
			if (ml_chain_at(la, i) != ml_chain_at(lb, i)) {
				fail(s, "nodes %u and %u both hold index %llu in term %llu with different "
					"histories (log matching)",
				     a, b, (unsigned long long)i, (unsigned long long)ml_term_at(la, i));
				return;
			}
		}
	}
}

/* ---- faults and workload */

static raft_id pick_leader(raft_sim *s)
{
	raft_id best = 0;
	raft_term best_term = 0;
	for (unsigned i = 1; i <= s->n; i++) {
		simnode *nd = &s->node[i];
		if (nd->up && raft_get_role(nd->r) == RAFT_LEADER &&
		    (best == 0 || raft_get_term(nd->r) > best_term)) {
			best = i;
			best_term = raft_get_term(nd->r);
		}
	}
	return best;
}

/* The progress check. Every safety invariant above holds of a group that does nothing, so a
 * follower the leader never catches up -- a probe whose rejection is taken for stale, a stalled
 * snapshot -- passes the sweep for as long as it lasts (a 7 s stall of one node under
 * `batched-lease`, seed 218817, passed every raft_sim test). What the core owes a follower that
 * is up, reachable and a member is that it keeps pace: a stretch of `progress_ns` in which
 * nothing touches the node, the leader or the configuration ends with the node at what the leader
 * had committed when the stretch began. Anything that changes the setting starts a new stretch, so
 * a crash, a partition, a leader change or a pending configuration entry excuses what follows
 * and never a stall inside one. Lost messages do not excuse it: a core that needs a reliable
 * network to catch a follower up is the bug. */
static void progress_check(raft_sim *s)
{
	if (!s->p.progress_ns)
		return;
	raft_id lid = pick_leader(s);
	simnode *ld = lid ? &s->node[lid] : NULL;
	bool steady = ld && !ld->suspended && !ld->reseeding;
	if (steady && ld->log.ncfg > 0 && ld->log.cfg[ld->log.ncfg - 1] > raft_get_commit(ld->r))
		steady = false; /* a change in flight: the leader may already have stopped feeding a leaver */
	raft_index lcommit = steady ? raft_get_commit(ld->r) : 0;
	for (unsigned i = 1; i <= s->n && !s->failed; i++) {
		simnode *x = &s->node[i];
		bool eligible = steady && x != ld && x->up && !x->suspended && !x->reseeding &&
				x->group == ld->group && raft_config_find(&ld->config, x->id);
		if (!eligible) {
			x->prog_start = 0;
			continue;
		}
		raft_term lterm = raft_get_term(ld->r);
		if (x->prog_start && (x->prog_leader != lid || x->prog_lterm != lterm ||
				      x->prog_linc != ld->incarnation || x->prog_xinc != x->incarnation ||
				      x->prog_group != x->group))
			x->prog_start = 0;
		if (x->prog_start && x->applied >= x->prog_target)
			x->prog_start = 0; /* kept pace: the next stretch asks for the new commit */
		if (x->prog_start && s->now - x->prog_start >= s->p.progress_ns) {
			fail(s, "node %u has not kept pace with leader n%u (term %llu) for %.2f s of an "
				"undisturbed group: applied %llu, the leader's commit was %llu when the "
				"stretch began and is %llu now", x->id, lid, (unsigned long long)lterm,
			     (double)(s->now - x->prog_start) / 1e9, (unsigned long long)x->applied,
			     (unsigned long long)x->prog_target, (unsigned long long)lcommit);
			return;
		}
		if (!x->prog_start) {
			x->prog_start = s->now;
			x->prog_target = lcommit;
			x->prog_leader = lid;
			x->prog_lterm = lterm;
			x->prog_linc = ld->incarnation;
			x->prog_xinc = x->incarnation;
			x->prog_group = x->group;
		}
	}
}

/* Any node that believes it leads, for a workload that should also hit the stale ones. */
static raft_id pick_believer(raft_sim *s)
{
	raft_id ids[NODES_MAX];
	unsigned n = 0;
	for (unsigned i = 1; i <= s->n; i++)
		if (s->node[i].up && !s->node[i].suspended && raft_get_role(s->node[i].r) == RAFT_LEADER)
			ids[n++] = i;
	return n ? ids[iso_rng_below(&s->rng, n)] : 0;
}

/* ---- the reseed (plan 5a-i-g, L's demotion and its repair)
 *
 * What `isoctl mds reseed` does to a halted replica, in the simulator's terms: the node is down
 * (its replica halted) while the leader is asked, every 50 ms for up to 5 s, to make it a learner;
 * once a committed configuration has it as one, its log and state machine are emptied -- the fresh
 * store holds no configuration -- and its hard state is written as the node writes it: the leader's
 * term when that is higher, with the vote cleared, and a vote fence on the leader's commit and that
 * entry's term. Then it restarts. A reseed that cannot get its change committed gives up, and the
 * node restarts over the log it had. The churn promotes learners at random, caught up or not, which
 * is harsher than the housekeeping. */

static void reseed_wipe(raft_sim *s, simnode *nd, raft_term term, raft_index fence_index,
			raft_term fence_term)
{
	if (nd->up)
		node_crash(s, nd);
	ml_reset(&nd->log, 0, 0);
	if (term > nd->hard_durable.term) {
		nd->hard_durable.term = term;
		nd->hard_durable.vote = 0;
	}
	nd->hard_durable.fence_index = fence_index;
	nd->hard_durable.fence_term = fence_term;
	nd->applied = 0;
	nd->applied_term = 0;
	nd->chain = 0;
	nd->last_snapshot = 0;
	memset(&nd->config, 0, sizeof nd->config);
	s->st.reseeds++;
	TRACE(s, "n%u re-seeded: log emptied, term %llu, votes after %llu/%llu", nd->id,
	      (unsigned long long)nd->hard_durable.term, (unsigned long long)fence_index,
	      (unsigned long long)fence_term);
}

static void random_reseed(raft_sim *s)
{
	raft_id leader = pick_leader(s);
	if (!leader)
		return;
	const raft_config *c = raft_get_config(s->node[leader].r);
	raft_id cands[NODES_MAX];
	unsigned n = 0;
	for (uint32_t i = 0; i < c->n; i++)
		if (!c->m[i].learner && c->m[i].id != leader && !s->node[c->m[i].id].reseeding)
			cands[n++] = c->m[i].id;
	if (!n)
		return;
	simnode *v = &s->node[cands[iso_rng_below(&s->rng, n)]];
	if (v->up)
		node_crash(s, v);
	v->reseeding = true;
	v->reseed_until = s->now + 5 * 1000000000ull;
	TRACE(s, "n%u halts, and is re-seeded: n%u is asked to make it a learner", v->id, leader);
	(void)raft_sim_change(s, leader, RAFT_DEMOTE, v->id);
	schedule(s, (event){ .time = s->now + 50000000ull, .kind = EV_RESEED_STEP, .node = v->id });
}

static void reseed_step(raft_sim *s, simnode *nd)
{
	if (!nd->reseeding)
		return;
	raft_id l = pick_leader(s);
	if (l) {
		raft *lr = s->node[l].r;
		const raft_member *m = raft_config_find(raft_get_config(lr), nd->id);
		if (!m) {
			/* Removed meanwhile: the reseed is refused, and nothing was emptied. */
			nd->reseeding = false;
			raft_sim_restart(s, nd->id);
			return;
		}
		if (m->learner && raft_get_config_index(lr) <= raft_get_commit(lr)) {
			raft_index fi = raft_get_commit(lr);
			reseed_wipe(s, nd, raft_get_term(lr), fi, raft_get_term_at(lr, fi));
			nd->reseeding = false;
			raft_sim_restart(s, nd->id);
			return;
		}
		if (!m->learner)
			(void)raft_sim_change(s, l, RAFT_DEMOTE, nd->id);
	}
	if (s->now >= nd->reseed_until) {
		TRACE(s, "n%u's reseed gives up; it restarts over its own log", nd->id);
		nd->reseeding = false;
		raft_sim_restart(s, nd->id);
		return;
	}
	schedule(s,
		 (event){ .time = s->now + 50000000ull, .kind = EV_RESEED_STEP, .node = nd->id });
}

static void random_partition(raft_sim *s)
{
	unsigned k = 1 + (unsigned)iso_rng_below(&s->rng, s->n - 1);
	raft_id ids[NODES_MAX];
	for (unsigned i = 0; i < s->n; i++)
		ids[i] = i + 1;
	iso_rng_shuffle(&s->rng, ids, s->n, sizeof ids[0]);
	raft_sim_partition(s, ids, k);
	schedule(s, (event){ .time = s->now + s->p.partition_ns, .kind = EV_HEAL });
}

static void random_churn(raft_sim *s)
{
	raft_id leader = pick_leader(s);
	if (!leader)
		return;
	const raft_config *c = raft_get_config(s->node[leader].r);
	raft_id spare[NODES_MAX], learners[NODES_MAX], members[NODES_MAX];
	unsigned ns = 0, nl = 0, nm = 0;
	for (unsigned i = 1; i <= s->n; i++) {
		const raft_member *m = raft_config_find(c, i);
		if (!m)
			spare[ns++] = i;
		else {
			members[nm++] = i;
			if (m->learner)
				learners[nl++] = i;
		}
	}
	unsigned choice = (unsigned)iso_rng_below(&s->rng, 3);
	if (choice == 0 && ns)
		raft_sim_change(s, leader, RAFT_ADD_LEARNER, spare[iso_rng_below(&s->rng, ns)]);
	else if (choice == 1 && nl)
		raft_sim_change(s, leader, RAFT_ADD_VOTER, learners[iso_rng_below(&s->rng, nl)]);
	else if (nm > 1)
		raft_sim_change(s, leader, RAFT_REMOVE, members[iso_rng_below(&s->rng, nm)]);
}

static void random_transfer(raft_sim *s)
{
	raft_id leader = pick_leader(s);
	if (!leader)
		return;
	const raft_config *c = raft_get_config(s->node[leader].r);
	raft_id cands[NODES_MAX];
	unsigned n = 0;
	for (uint32_t i = 0; i < c->n; i++)
		if (!c->m[i].learner && c->m[i].id != leader)
			cands[n++] = c->m[i].id;
	if (n)
		raft_sim_transfer(s, leader, cands[iso_rng_below(&s->rng, n)]);
}

/* A disk completion for a suspended node: the disk finished, the node hears of it when it resumes. */
static bool held_while_suspended(raft_sim *s, simnode *nd, const event *ev)
{
	if (!nd->suspended || nd->incarnation != ev->incarnation)
		return false;
	event later = *ev;
	later.time = s->now + TICK_NS;
	schedule(s, later);
	return true;
}
static void handle(raft_sim *s, event *ev)
{
	simnode *nd = &s->node[ev->node]; /* slot 0 exists and is never a node; global events use 0 */
	switch (ev->kind) {
	case EV_TICK:
		for (unsigned i = 1; i <= s->n && !s->failed; i++) {
			simnode *x = &s->node[i];
			if (!x->up || x->suspended)
				continue;
			raft_tick(x->r, (uint64_t)local_time(x, s->now));
			drain(s, x);
		}
		if (s->now >= s->next_sweep) {
			sweep(s);
			progress_check(s);
			s->next_sweep = s->now + SWEEP_NS;
		}
		schedule(s, (event){ .time = s->now + TICK_NS, .kind = EV_TICK });
		break;
	case EV_MSG: {
		simmsg *m = ev->u.msg;
		simnode *from = &s->node[m->m.from];
		if (nd->up && !nd->suspended && from->group == nd->group) {
			raft_step(nd->r, &m->m);
			drain(s, nd);
		} else {
			s->st.messages_dropped++;
		}
		free_msg(m);
		break;
	}
	case EV_SYNC:
		if (held_while_suspended(s, nd, ev))
			break;
		if (nd->up && nd->incarnation == ev->incarnation) {
			/* The disk holds whatever the log contained up to the requested index
			 * at the moment the write finished. */
			raft_index idx = ISO_MIN(ev->u.sync.index, ml_last(&nd->log));
			if (idx > nd->log.durable)
				nd->log.durable = idx;
			raft_persisted(nd->r, idx, ml_term_at(&nd->log, idx));
			drain(s, nd);
		}
		break;
	case EV_HARD:
		if (held_while_suspended(s, nd, ev))
			break;
		if (nd->up && nd->incarnation == ev->incarnation) {
			nd->hard_durable = ev->u.hs;
			raft_hard_saved(nd->r, &ev->u.hs);
			drain(s, nd);
		}
		break;
	case EV_RESTART:
		if (!nd->up && !nd->reseeding)
			raft_sim_restart(s, nd->id);
		break;
	case EV_HEAL:
		raft_sim_heal(s);
		break;
	case EV_PROPOSE: {
		raft_id at = pick_believer(s);
		if (at)
			raft_sim_propose(s, at, NULL);
		else
			s->st.proposals_refused++;
		arm(s, EV_PROPOSE, s->p.propose_rate);
		break;
	}
	case EV_READ: {
		raft_id at = pick_believer(s);
		/* Every fourth read asks for a round whatever the lease says (raft_read_index, plan
		 * 28s: a pool that does not rest on the clock), so the checker holds lease reads and
		 * ReadIndex reads on one leader to one history. Chosen by the read's own count and not by a
		 * draw, so the generator's stream picks the same events; what moves is what a round
		 * sends, so a seed's message counts are not those of a build before 28s. */
		if (at)
			(s->next_ctx % 4 == 3 ? raft_sim_read_index : raft_sim_read)(s, at);
		arm(s, EV_READ, s->p.read_rate);
		break;
	}
	case EV_CRASH: {
		raft_id victim = 1 + (raft_id)iso_rng_below(&s->rng, s->n);
		if (s->node[victim].up) {
			raft_sim_crash(s, victim);
			schedule(s, (event){ .time = s->now + rnd_range(s, s->p.restart_min_ns,
									  s->p.restart_max_ns),
					     .kind = EV_RESTART, .node = victim });
		}
		arm(s, EV_CRASH, s->p.crash_rate);
		break;
	}
	case EV_PARTITION:
		random_partition(s);
		arm(s, EV_PARTITION, s->p.partition_rate);
		break;
	case EV_CHURN:
		random_churn(s);
		arm(s, EV_CHURN, s->p.churn_rate);
		break;
	case EV_TRANSFER:
		random_transfer(s);
		arm(s, EV_TRANSFER, s->p.transfer_rate);
		break;
	case EV_RESEED:
		random_reseed(s);
		arm(s, EV_RESEED, s->p.reseed_rate);
		break;
	case EV_RESEED_STEP:
		reseed_step(s, nd);
		break;
	}
}

/* ------------------------------------------------------------------ public */

raft_sim *raft_sim_new(const raft_sim_params *p)
{
	raft_sim *s = iso_xcalloc(1, sizeof *s);
	s->p = *p;
	s->lose_every = p->lose_first_answers;
	s->n = p->voters + p->learners + p->spare;
	s->pend_cap = p->pend_cap && p->pend_cap < PEND_MAX ? p->pend_cap : PEND_MAX;
	/* A power of two (the next one down from what was asked), so that a mask picks the bucket. */
	unsigned buckets = PEND_BUCKETS;
	while (p->pend_buckets && buckets > p->pend_buckets)
		buckets >>= 1;
	s->pend_mask = buckets - 1;
	if (s->n == 0 || s->n > NODES_MAX || p->voters == 0) {
		free(s);
		return NULL;
	}
	iso_rng_seed(&s->rng, p->seed);
	raft_config initial = { 0 };
	for (unsigned i = 1; i <= p->voters + p->learners; i++)
		initial.m[initial.n++] = (raft_member){ .id = i, .learner = i > p->voters };
	for (unsigned i = 1; i <= s->n; i++) {
		simnode *nd = &s->node[i];
		nd->id = i;
		nd->log.base_index = 0;
		nd->log.base_chain = 0;
		nd->initial_config = i <= p->voters + p->learners ? initial : (raft_config){ 0 };
		nd->config = nd->initial_config;
		double drift = p->clock_drift ? (iso_rng_double(&s->rng) * 2 - 1) * p->clock_drift : 0;
		nd->clock_rate = 1.0 + drift;
		node_boot(s, nd);
	}
	schedule(s, (event){ .time = TICK_NS, .kind = EV_TICK });
	arm(s, EV_PROPOSE, p->propose_rate);
	arm(s, EV_READ, p->read_rate);
	arm(s, EV_CRASH, p->crash_rate);
	arm(s, EV_PARTITION, p->partition_rate);
	arm(s, EV_CHURN, p->churn_rate);
	arm(s, EV_TRANSFER, p->transfer_rate);
	arm(s, EV_RESEED, p->reseed_rate);
	return s;
}

void raft_sim_free(raft_sim *s)
{
	if (!s)
		return;
	for (unsigned i = 1; i <= s->n; i++) {
		raft_free(s->node[i].r);
		ml_drop_from(&s->node[i].log, 0);
		free(s->node[i].log.e);
		ml_free_arena(&s->node[i].log);
		free(s->node[i].log.cfg);
	}
	for (size_t i = 0; i < s->hn; i++)
		if (s->heap[i].kind == EV_MSG)
			free_msg(s->heap[i].u.msg);
	free(s->heap);
	free(s->cterm);
	free(s->cchain);
	free(s->term_leader);
	free(s->grant);
	free(s);
}

static bool step_one(raft_sim *s, uint64_t limit)
{
	if (s->hn == 0 || s->heap[0].time > limit)
		return false;
	event ev = heap_pop(s);
	if (ev.time > s->now)
		s->now = ev.time;
	handle(s, &ev);
	return true;
}

bool raft_sim_run(raft_sim *s, uint64_t ns)
{
	uint64_t end = s->now + ns;
	while (!s->failed && step_one(s, end))
		;
	if (!s->failed && s->now < end)
		s->now = end;
	return !s->failed;
}

bool raft_sim_run_until(raft_sim *s, uint64_t max_ns, bool (*cond)(const raft_sim *, void *),
			void *user)
{
	uint64_t end = s->now + max_ns;
	while (!s->failed) {
		if (cond(s, user))
			return true;
		if (!step_one(s, end))
			break;
	}
	return !s->failed && cond(s, user);
}

const char *raft_sim_failure(const raft_sim *s) { return s->failure; }
uint64_t raft_sim_now(const raft_sim *s) { return s->now; }
unsigned raft_sim_nodes(const raft_sim *s) { return s->n; }
bool raft_sim_up(const raft_sim *s, raft_id id) { return id >= 1 && id <= s->n && s->node[id].up; }
raft *raft_sim_core(raft_sim *s, raft_id id) { return raft_sim_up(s, id) ? s->node[id].r : NULL; }
raft_id raft_sim_leader(const raft_sim *s) { return pick_leader((raft_sim *)s); }
raft_index raft_sim_applied(const raft_sim *s, raft_id id) { return s->node[id].applied; }
uint64_t raft_sim_state_hash(const raft_sim *s, raft_id id) { return s->node[id].chain; }
const raft_config *raft_sim_config(const raft_sim *s, raft_id id) { return &s->node[id].config; }
raft_index raft_sim_committed_max(const raft_sim *s) { return s->cmax; }
void raft_sim_get_stats(const raft_sim *s, raft_sim_stats *out) { *out = s->st; }

void raft_sim_crash(raft_sim *s, raft_id id)
{
	if (raft_sim_up(s, id))
		node_crash(s, &s->node[id]);
}

void raft_sim_restart(raft_sim *s, raft_id id)
{
	if (id < 1 || id > s->n || s->node[id].up)
		return;
	node_boot(s, &s->node[id]);
	TRACE(s, "n%u restart: last=%llu term=%llu applied=%llu", id,
	      (unsigned long long)ml_last(&s->node[id].log),
	      (unsigned long long)s->node[id].hard_durable.term,
	      (unsigned long long)s->node[id].applied);
	if (!s->failed)
		drain(s, &s->node[id]);
}

void raft_sim_wipe(raft_sim *s, raft_id id)
{
	if (id < 1 || id > s->n)
		return;
	simnode *nd = &s->node[id];
	if (nd->up)
		node_crash(s, nd);

	ml_reset(&nd->log, 0, 0);
	nd->hard_durable = (raft_hard_state){ 0 };
	nd->applied = 0;
	nd->applied_term = 0;
	nd->chain = 0;
	nd->last_snapshot = 0;
	memset(&nd->config, 0, sizeof nd->config);
	s->st.crashes++;
	TRACE(s, "n%u wiped: nothing on disk, identity kept", id);
}

void raft_sim_partition(raft_sim *s, const raft_id *isolated, unsigned n)
{
	for (unsigned i = 1; i <= s->n; i++)
		s->node[i].group = 0;
	for (unsigned i = 0; i < n; i++)
		if (isolated[i] >= 1 && isolated[i] <= s->n)
			s->node[isolated[i]].group = 1;
	s->st.partitions++;
	if (s->trace) {
		fprintf(s->trace, "%9.4f partition:", (double)s->now / 1e9);
		for (unsigned i = 1; i <= s->n; i++)
			fprintf(s->trace, " n%u%s", i, s->node[i].group ? "*" : "");
		fputc('\n', s->trace);
	}
}

void raft_sim_heal(raft_sim *s)
{
	for (unsigned i = 1; i <= s->n; i++)
		s->node[i].group = 0;
	TRACE(s, "heal");
}

void raft_sim_set_trace(raft_sim *s, void *file) { s->trace = file; }

void raft_sim_set_drop(raft_sim *s, double drop) { s->p.drop = drop; }

void raft_sim_cut_link(raft_sim *s, raft_id from, raft_id to, bool cut)
{
	if (from >= 1 && from <= s->n && to >= 1 && to <= s->n)
		s->cut[from][to] = cut;
}

void raft_sim_lose_first_answers(raft_sim *s, unsigned every_n)
{
	s->lose_every = every_n;
	memset(s->lose_last, 0, sizeof s->lose_last);
}

void raft_sim_suspend(raft_sim *s, raft_id id, bool clock_stops)
{
	if (!raft_sim_up(s, id) || s->node[id].suspended)
		return;
	simnode *nd = &s->node[id];
	nd->suspended = true;
	nd->clock_stops = clock_stops;
	nd->suspended_at = s->now;
	TRACE(s, "n%u suspended (its clock %s)", id, clock_stops ? "stops" : "runs on");
}

void raft_sim_resume(raft_sim *s, raft_id id)
{
	if (raft_sim_up(s, id) && s->node[id].suspended)
		node_resume(s, &s->node[id]);
}

/* The first thing a resumed node does is read its clock, as the group's loop does on its first turn
 * after a suspend; a request asked of it next is judged on that reading. */
static void node_resume(raft_sim *s, simnode *nd)
{
	if (nd->clock_stops)
		nd->stopped_ns += s->now - nd->suspended_at;
	nd->suspended = false;
	TRACE(s, "n%u resumed", nd->id);
	raft_tick(nd->r, (uint64_t)local_time(nd, s->now));
	drain(s, nd);
}

void raft_sim_set_clock_rate(raft_sim *s, raft_id id, double rate)
{
	if (id >= 1 && id <= s->n && rate > 0)
		s->node[id].clock_rate = rate;
}

void raft_sim_set_rates(raft_sim *s, double propose_rate, double read_rate)
{
	bool arm_p = s->p.propose_rate == 0 && propose_rate > 0;
	bool arm_r = s->p.read_rate == 0 && read_rate > 0;
	s->p.propose_rate = propose_rate;
	s->p.read_rate = read_rate;
	if (arm_p)
		arm(s, EV_PROPOSE, propose_rate);
	if (arm_r)
		arm(s, EV_READ, read_rate);
}

iso_err raft_sim_propose(raft_sim *s, raft_id at, raft_index *index_out)
{
	if (!raft_sim_up(s, at) || s->node[at].suspended)
		return ISO_ERR_CLOSED;
	simnode *nd = &s->node[at];
	size_t pad = s->p.payload_max ? iso_rng_below(&s->rng, ISO_MIN(s->p.payload_max, 256) + 1) : 0;
	return propose_on(s, nd, at, pad, index_out);
}

/* One proposal of 8 bytes and `pad` more at node `at`, and the drain after it -- a no-op from
 * inside one, whose outer drain sends it. */
static iso_err propose_on(raft_sim *s, simnode *nd, raft_id at, size_t pad, raft_index *index_out)
{
	uint8_t buf[8 + 256];
	uint64_t value = ++s->next_value;
	iso_put_u64le(buf, value);
	for (size_t i = 0; i < pad; i++)
		buf[8 + i] = (uint8_t)(value + i);
	raft_index index = 0;
	iso_err rc = raft_propose(nd->r, buf, 8 + pad, &index);
	if (rc != ISO_OK) {
		s->st.proposals_refused++;
		drain(s, nd);
		return rc;
	}
	s->st.proposals++;
	unsigned slot = (s->pend_head + s->pend_n) % s->pend_cap;
	if (s->pend_n == s->pend_cap) {
		pend_unlink(s, slot); /* the oldest, which this one takes the place of */
		s->pend_head = (s->pend_head + 1) % s->pend_cap;
	} else
		s->pend_n++;
	unsigned h = pend_hash(s, index, at);
	s->pend[slot] = (pending){ .index = index, .term = raft_get_term(nd->r), .leader = at,
				   .issued = s->now, .bnext = s->pend_bucket[h] };
	s->pend_bucket[h] = slot + 1;
	drain(s, nd);
	if (index_out)
		*index_out = index;
	return ISO_OK;
}

static iso_err sim_read(raft_sim *s, raft_id at, bool round)
{
	if (!raft_sim_up(s, at) || s->node[at].suspended)
		return ISO_ERR_CLOSED;
	simnode *nd = &s->node[at];
	uint64_t ctx = ++s->next_ctx;
	if (s->n_reads < READS_MAX)
		s->reads[s->n_reads++] = (pending_read){ .ctx = ctx, .need = s->commit_high, .at = at };
	iso_err rc = round ? raft_read_index(nd->r, ctx) : raft_read(nd->r, ctx);
	if (rc != ISO_OK)
		on_read_failed(s, ctx);
	else
		s->st.reads++;
	drain(s, nd);
	return rc;
}

iso_err raft_sim_read(raft_sim *s, raft_id at)
{
	return sim_read(s, at, false);
}

iso_err raft_sim_read_index(raft_sim *s, raft_id at)
{
	return sim_read(s, at, true);
}

iso_err raft_sim_change(raft_sim *s, raft_id at, raft_change_kind kind, raft_id id)
{
	if (!raft_sim_up(s, at))
		return ISO_ERR_CLOSED;
	iso_err rc = raft_change(s->node[at].r, kind, id, NULL);
	if (rc == ISO_OK) {
		s->st.changes++;
		const raft_config *c = raft_get_config(s->node[at].r);
		if (s->trace) {
			fprintf(s->trace, "%9.4f n%u appends CONFIG (%s n%u) at %llu:",
				(double)s->now / 1e9, at,
				kind == RAFT_ADD_LEARNER ? "add learner" :
				kind == RAFT_ADD_VOTER ? "add voter" :
				kind == RAFT_DEMOTE ? "demote" : "remove", id,
				(unsigned long long)raft_get_last(s->node[at].r));
			for (unsigned i = 0; i < c->n; i++)
				fprintf(s->trace, " n%u%s", c->m[i].id, c->m[i].learner ? "L" : "");
			fputc('\n', s->trace);
		}
	}
	drain(s, &s->node[at]);
	return rc;
}

iso_err raft_sim_transfer(raft_sim *s, raft_id at, raft_id to)
{
	if (!raft_sim_up(s, at))
		return ISO_ERR_CLOSED;
	iso_err rc = raft_transfer(s->node[at].r, to);
	if (rc == ISO_OK)
		s->st.transfers++;
	drain(s, &s->node[at]);
	return rc;
}

bool raft_sim_chaos(const raft_sim_params *p, uint64_t duration_ns, raft_sim_stats *stats,
		    char *failure, size_t failure_size)
{
	raft_sim_params q = *p;
	if (!q.progress_ns)
		q.progress_ns = 4 * q.core.election_min_ns;
	raft_sim *s = raft_sim_new(&q);
	if (!s) {
		iso_strlcpy(failure, "bad parameters", failure_size);
		return false;
	}
	bool ok = raft_sim_run(s, duration_ns);
	if (stats)
		raft_sim_get_stats(s, stats);
	iso_strlcpy(failure, s->failure, failure_size);
	raft_sim_free(s);
	return ok;
}
