/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure placement, timeout and balancer planner (docs/design-cluster-data.md §5, §6.2, §10.2; R9 plan D7).
 *
 * Pure module: NO I/O, NO clock reads, NO locks, NO globals mutated. Every input comes in via `struct place_in`
 * (the applied metadata state, per-member liveness and reports the Raft leader holds, per-position fill reports,
 * the proposer's clock `now_ms`, and settings); every decision goes out via `struct place_out` (proposals and
 * operator-facing log lines).
 *
 * Three entry points share this input/output contract and are called once a second on the Raft leader in order:
 *   1. pout_plan     (server/pout.c, r9-out):    DOWN/OUT timers, opening hold, fail-stop fast path, operator holds,
 *                                                 mass-OUT guard, and per-replica LOCAL_FAULT escalation (D3-D6).
 *   2. place_plan    (server/place.c, r9-plan, r9-return): re-placement after OUT / fault / drain / replace, rack
 *                                                 rule (E9, D20), endangered-first ordering (E13), stripe whole
 *                                                 commit, MK_GC release requests, and return-during-rebuild (D8-D13).
 *   3. balance_plan  (server/balance.c, r9-balance): §3.2 rules 1-4 over log replicas, stripe positions and leaders,
 *                                                 bounded by 5% concurrent and 25% total per plan (D19).
 *
 * tests/sim_place.c drives the exact same functions deterministically with simulated clocks and faults (D7).
 * Until their owning tracks land, pout_plan, place_plan and balance_plan are compiled, linked no-ops returning 0
 * proposals and 0 log lines.
 */
#ifndef TSDB_PLACE_H
#define TSDB_PLACE_H

#include <stddef.h>
#include <stdint.h>

#include "meta.h"

/* Maximum store ids carried per member liveness snapshot (R9 D13/D15; R13 extends per disk). */
#define PLACE_STORES_MAX 32
/* Maximum faulted volumes reported per member snapshot (D6). */
#define PLACE_FAULTS_MAX 64
/* Maximum length of one planner log line. */
#define PLACE_LOG_MAX 256

/* Per-volume LOCAL_FAULT duration reported by a member (D6). */
struct place_vol_fault {
    uint32_t volume;
    uint64_t fault_s; /* continuous seconds reporting CL_HEALTH_LOCAL_FAULT */
};

/* Per-member liveness and heartbeat report as the Raft leader sees it. */
struct place_member {
    uint32_t id;
    uint64_t heard_age_ms; /* ms since last heartbeat (UINT64_MAX: never heard) */
    uint8_t opening;       /* HBS_NODE: 1 while running initial volume opens (D5) */
    uint8_t failstop;      /* HBS_NODE: 1 if fail-stop last-gasp beat was received (D5) */
    uint8_t fill_pct;      /* HBS_NODE: 0..100 data-dir fill percentage (0xff: unknown; D17) */
    uint32_t chunk_stores; /* HBS_STORES: number of chunk stores currently UP */
    uint32_t nstore_ids;
    uint64_t store_ids[PLACE_STORES_MAX]; /* store identities for return-vs-lost check (D13, D15) */
    uint32_t nfaults;
    struct place_vol_fault faults[PLACE_FAULTS_MAX];
};

/* Per-position fill report received via HBS_FILL (D10, D11). */
struct place_fill {
    uint32_t node;           /* reporting member id */
    uint32_t volume;         /* volume number */
    uint32_t pos;            /* stripe position 0 .. k+m-1 */
    uint64_t stripe_version; /* stripe_version this fill is for */
    uint64_t done;           /* objects filled so far */
    uint64_t total;          /* objects to fill */
    uint8_t whole;           /* 1 once every object below stripe_version is held and verified */
};

/* R9 planner settings (D23). Filled from meta_settings once r9-meta adds the fields; defaults until then. */
struct place_settings {
    uint32_t rebuild_after_s;     /* 1800 (renames out_after_s) */
    uint32_t opening_hold_s;      /* 3600 */
    uint32_t failstop_out_s;      /* 60 */
    uint32_t out_guard_pct;       /* 25 */
    uint32_t out_guard_hold_s;    /* 7200 */
    uint32_t repl_retain_down_gb; /* 64 */
    uint32_t young_high_pct;      /* 90 */
    uint32_t balance_mb_s;        /* 0 (= rebuild_mb_s) */
    uint32_t rebuild_concurrency; /* 4 */
};

void place_settings_default(struct place_settings *s);

/* Planted bugs for sim_place and mutant suites (place_in.mutant; 0 = honest). */
enum place_mutant {
    PLACE_MUT_NONE = 0,
    PLACE_MUT_ORDER_FIFO = 1,
    PLACE_MUT_RACK_IGNORED = 2,
    PLACE_MUT_TWO_PER_SERVER = 3,
    PLACE_MUT_MOVE_SHRINKS = 4,
    PLACE_MUT_WHOLE_NO_BARRIER = 5,
    PLACE_MUT_DELETE_UNMARKED = 6,
    PLACE_MUT_GUARD_UNBOUNDED = 7,
    PLACE_MUT_NO_GUARD = 8,
    PLACE_MUT_HOLD_IGNORED = 9,
    PLACE_MUT_OPENING_UNBOUNDED = 10,
    PLACE_MUT_FAILSTOP_SLOW = 11,
    PLACE_MUT_BALANCE_UNBOUNDED = 12,
};

/* Active `node replace OLD NEW` intent (r9-ops records; place_plan sends each of OLD's positions to NEW). */
struct place_replace {
    uint32_t old_node;
    uint32_t new_node;
};

/* Optional per-volume byte size from heartbeats for E13 tie-breaking (0/absent: tie-break by volume number). */
struct place_vol_size {
    uint32_t volume;
    uint64_t bytes;
};

/* Complete read-only input to pout_plan / place_plan / balance_plan (D7). */
struct place_in {
    const struct meta_state *sm; /* applied metadata state (never mutated) */
    const struct place_member *members;
    uint32_t nmembers;
    const struct place_fill *fills;
    uint32_t nfills;
    uint64_t now_ms;   /* proposer's wall-clock time in ms (D3) */
    uint64_t mono_ms;  /* proposer's monotonic time in ms (for relative ages) */
    uint32_t self_id;  /* this Raft leader's member id */
    struct place_settings set;
    int mutant;        /* enum place_mutant (0 in production) */
    const struct place_replace *replaces; /* optional active node-replace intents (NULL/0: none) */
    uint32_t nreplaces;
    const struct place_vol_size *vol_sizes; /* optional per-volume bytes (NULL/0: none) */
    uint32_t nvol_sizes;
};

/* Kinds of proposals a planner may emit. */
enum place_prop_kind {
    PLACE_PROP_MEMBER_STATE = 1,    /* MEMBER_STATE(node, state, incarnation, down_reason) */
    PLACE_PROP_REPLICA_ADD = 2,     /* REPLICA_ADD(volume, epoch, node, replaces) — log move */
    PLACE_PROP_STRIPE_SET = 3,      /* STRIPE_SET(volume, pos, node, prev_node, stripe_version, filling) */
    PLACE_PROP_STRIPE_WHOLE = 4,    /* STRIPE_WHOLE(volume, pos, stripe_version) */
    PLACE_PROP_GC_RELEASE = 5,      /* MK_GC release request(volume, pos, node, stripe_version) */
    PLACE_PROP_MOVE_CANCEL = 6,     /* cancel an in-flight log move(volume, from_node, to_node) */
    PLACE_PROP_VOLUME_TRANSFER = 7, /* planned leader transfer(volume, node) for balancer */
    PLACE_PROP_REPLICA_REMOVE = 8,  /* REPLICA_REMOVE(volume, epoch, node) — mutant moveshrinks only */
};

struct place_prop {
    enum place_prop_kind kind;
    uint32_t volume;         /* volume number (0 for MEMBER_STATE) */
    uint32_t node;           /* target member id */
    uint32_t other_node;     /* replaces (REPLICA_ADD), prev_node (STRIPE_SET), from_node (MOVE_CANCEL) */
    uint32_t pos;            /* stripe position (STRIPE_SET, STRIPE_WHOLE, GC_RELEASE) */
    uint64_t epoch;          /* expected volume epoch (REPLICA_ADD) */
    uint64_t version;        /* expected stripe_version (STRIPE_SET, STRIPE_WHOLE, GC_RELEASE) or incarnation */
    uint8_t state;           /* MEMBER_STATE: enum meta_member_state */
    uint8_t reason;          /* MEMBER_STATE down reason (timeout/failstop/fault) or filling flag */
    int margin;              /* priority margin for ordering/pacing (lower = more endangered) */
};

struct place_log {
    char msg[PLACE_LOG_MAX];
};

/* Output buffer accumulated across pout_plan -> place_plan -> balance_plan. */
struct place_out {
    struct place_prop *props;
    uint32_t nprops, cap_props;
    struct place_log *logs;
    uint32_t nlogs, cap_logs;
};

void place_out_init(struct place_out *out);
void place_out_free(struct place_out *out);
int place_out_push(struct place_out *out, const struct place_prop *p);
int place_out_log(struct place_out *out, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

/* Entry points called in order once per second on the Raft leader (and by sim_place). */
void pout_plan(const struct place_in *in, struct place_out *out);
void place_plan(const struct place_in *in, struct place_out *out);
void balance_plan(const struct place_in *in, struct place_out *out);

/* Computed mass-OUT guard summary (D4): shared by pout_plan and `tsdb cluster status` so they never drift. */
struct pout_guard {
    int active;        /* 1 if pct or rack guard threshold is currently crossed */
    int pct_triggered; /* 1 if > out_guard_pct % of non-OUT/non-DESTROYED members are DOWN */
    int rack_triggered;/* 1 if every non-OUT/non-DESTROYED member of a multi-server rack is DOWN */
    uint32_t ndown;    /* DOWN members with down_since_ms > 0 */
    uint32_t non_out;  /* non-OUT/non-DESTROYED members */
    uint64_t start_ms; /* down_since_ms of the member that crossed the guard */
    uint64_t due_ms;   /* start_ms + out_guard_hold_s * 1000 (saturated) */
    int holding;       /* 1 if active && now_ms < due_ms */
};
void pout_guard_eval(const struct meta_state *sm, uint32_t out_guard_pct, uint32_t out_guard_hold_s, uint64_t now_ms,
                     struct pout_guard *out);

/* Initial placement for VOLUME_ADD / VOLUME_ADD_EC (pure; moved from cluster.c by r9-plan; §3.2 rules 1-4, E9, D20).
 * Returns 0 on success, -1 if fewer than n (or ns) eligible members qualify. */
int place_volume(const struct place_in *in, uint32_t n, uint32_t *nodes);
int place_stripe(const struct place_in *in, uint32_t ns, uint32_t *stripe, uint32_t *qualify);

/* Returns 1 if losing any single rack leaves volume `v` survivable in both log and EC tiers (E9, D20), else 0. */
int place_rack_survivable(const struct meta_state *sm, const struct meta_volume *v);

/* Parse a mutant name (NULL or "" -> PLACE_MUT_NONE; unknown -> -1). */
int place_mutant_parse(const char *name);
/* Read TSDB_PLACE_MUTANT once (tests only, outside pure planners; 0 in production builds). */
int place_mutant_env(void);

#endif
