/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit and mutant tests for server/pout.c (r9-out; R9 plan D2-D6, E12).
 *
 * Drives the pure `pout_plan(in, out)` function directly with synthetic metadata states and liveness snapshots,
 * verifying:
 *   - normal rebuild_after_s timer (not before deadline, OUT at deadline, never self/UP/DRAINING/OUT/DESTROYED)
 *   - fail-stop fast path (UP -> DOWN failstop, DOWN timeout -> failstop escalation, OUT at failstop_out_s)
 *   - operator holds (hold_until_ms > now_ms holds OUT, expires at hold_until_ms, short hold still waits rebuild_after)
 *   - opening holds (active opening extends by opening_hold_s, bounded never unbounded, stale beat ends hold)
 *   - percentage mass-OUT guard (> out_guard_pct of non-OUT/non-DESTROYED members DOWN, crossing member's down_since,
 *     exact 25% boundary not guarded, OUT/DESTROYED excluded from denominator, guard bound passed log line once)
 *   - single-rack mass-OUT guard (full rack DOWN with >= 2 configured racks, bounded by out_guard_hold_s)
 *   - safety exclusions (last ISR member never OUT while merely DOWN under a guard; mid-move target never OUT)
 *   - per-replica LOCAL_FAULT escalation (UP member reporting fault >= failstop_out_s emits PLACE_PROP_REPLICA_ADD
 *     with replaces = faulted node, one per volume, skipped while already moving or member DOWN/stale)
 *   - determinism / purity and NULL/uninitialized safety
 *
 * Run with `--mutant <name>` for each of the 5 planted mutants (noguard, guardunbounded, holdignored,
 * openingunbounded, failstopslow); every mutant must fail the suite.
 */
#include "meta.h"
#include "place.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_mutant = PLACE_MUT_NONE;

static void die(const char *fmt, ...) __attribute__((noreturn, format(printf, 1, 2)));
static void die(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "test_pout FAIL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    exit(1);
}

static int parse_mutant(const char *name)
{
    if (!name || !*name)
        return PLACE_MUT_NONE;
    if (!strcmp(name, "noguard"))
        return PLACE_MUT_NO_GUARD;
    if (!strcmp(name, "guardunbounded"))
        return PLACE_MUT_GUARD_UNBOUNDED;
    if (!strcmp(name, "holdignored"))
        return PLACE_MUT_HOLD_IGNORED;
    if (!strcmp(name, "openingunbounded"))
        return PLACE_MUT_OPENING_UNBOUNDED;
    if (!strcmp(name, "failstopslow"))
        return PLACE_MUT_FAILSTOP_SLOW;
    die("unknown mutant \"%s\"", name);
}

/* Build a clean initialised replicated cluster of `n` UP members (ids 1..n, incarnation 1) with optional racks. */
static void make_cluster(struct meta_state *sm, uint32_t n, const char *const *racks, const struct place_settings *ps)
{
    meta_init(sm);
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 3000;
    if (ps) {
        s.rebuild_after_s = ps->rebuild_after_s;
        s.opening_hold_s = ps->opening_hold_s;
        s.failstop_out_s = ps->failstop_out_s;
        s.out_guard_pct = ps->out_guard_pct;
        s.out_guard_hold_s = ps->out_guard_hold_s;
    }
    uint8_t uuid[16] = {1, 2, 3, 4};
    struct meta_member first = {.id = 1, .state = META_UP, .incarnation = 1, .flags = META_MF_ALL};
    snprintf(first.addr, sizeof first.addr, "127.0.0.1:5001");
    snprintf(first.peer, sizeof first.peer, "127.0.0.1:6001");
    if (racks && racks[0])
        snprintf(first.rack, sizeof first.rack, "%s", racks[0]);
    first.ident[0] = 1;

    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_init(&e, uuid, &s, &first, META_MODE_REPLICATED, META_LEVEL);
    for (uint32_t id = 2; id <= n; id++) {
        struct meta_member mb = {.id = id, .state = META_UP, .incarnation = 1, .flags = META_MF_ALL};
        snprintf(mb.addr, sizeof mb.addr, "127.0.0.1:%" PRIu32, 5000 + id);
        snprintf(mb.peer, sizeof mb.peer, "127.0.0.1:%" PRIu32, 6000 + id);
        if (racks && racks[id - 1])
            snprintf(mb.rack, sizeof mb.rack, "%s", racks[id - 1]);
        mb.ident[0] = (uint8_t)id;
        meta_op_member_add(&e, &mb);
    }
    meta_entry_finish(&e);
    int res[32];
    if (meta_apply(sm, 1, 1, e.buf, e.len, res, 32) != META_OK)
        die("meta_apply init failed");
    for (uint32_t i = 0; i < e.nops; i++)
        if (res[i] != META_OK)
            die("init op %" PRIu32 " failed: %s", i, meta_strerror(res[i]));
    meta_entry_free(&e);
}

static void apply_one(struct meta_state *sm, struct meta_entry *e, uint64_t *idx)
{
    meta_entry_finish(e);
    int res[16];
    if (meta_apply(sm, ++(*idx), 1, e->buf, e->len, res, 16) != META_OK)
        die("meta_apply failed at idx %" PRIu64, *idx);
    for (uint32_t i = 0; i < e->nops; i++)
        if (res[i] != META_OK)
            die("op %" PRIu32 " at idx %" PRIu64 " failed: %s", i, *idx, meta_strerror(res[i]));
    meta_entry_free(e);
}

static void mark_down(struct meta_state *sm, uint64_t *idx, uint32_t id, uint8_t reason, uint64_t since_ms)
{
    const struct meta_member *m = meta_member(sm, id);
    if (!m)
        die("mark_down: no member %" PRIu32, id);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_state(&e, id, META_DOWN, m->incarnation, reason, since_ms, since_ms);
    apply_one(sm, &e, idx);
}

static void mark_state(struct meta_state *sm, uint64_t *idx, uint32_t id, int state)
{
    const struct meta_member *m = meta_member(sm, id);
    if (!m)
        die("mark_state: no member %" PRIu32, id);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_state(&e, id, state, m->incarnation, META_DOWN_NONE, 0, 1000);
    apply_one(sm, &e, idx);
}

static void set_hold(struct meta_state *sm, uint64_t *idx, uint32_t id, uint64_t until_ms, uint64_t clock_ms)
{
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_node_hold(&e, id, until_ms, clock_ms);
    apply_one(sm, &e, idx);
}

static void add_vol(struct meta_state *sm, uint64_t *idx, uint32_t num, const uint32_t *nodes, uint32_t n,
                    uint32_t min_isr)
{
    struct meta_volume_add a;
    memset(&a, 0, sizeof a);
    a.number = num;
    snprintf(a.name, sizeof a.name, "v%" PRIu32, num);
    a.uuid[0] = (uint8_t)num;
    a.kind = META_VOL_REPLICATED;
    a.min_isr = min_isr;
    a.n = n;
    for (uint32_t i = 0; i < n; i++) {
        a.nodes[i] = nodes[i];
        a.rid[i] = 100ull * num + i + 1;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_add(&e, &a);
    apply_one(sm, &e, idx);
}

static void set_isr(struct meta_state *sm, uint64_t *idx, uint32_t vol, const uint32_t *isr, uint32_t nisr)
{
    const struct meta_volume *v = meta_volume(sm, vol);
    if (!v)
        die("set_isr: no volume %" PRIu32, vol);
    uint64_t rids[META_REPLICAS_MAX];
    for (uint32_t i = 0; i < nisr; i++) {
        const struct meta_replica *r = meta_replica(v, isr[i]);
        if (!r)
            die("set_isr: node %" PRIu32 " not in vol %" PRIu32, isr[i], vol);
        rids[i] = r->rid;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_isr_change(&e, vol, v->epoch, v->isr_version, isr, rids, nisr, 0);
    apply_one(sm, &e, idx);
}

static void run_pout(const struct meta_state *sm, const struct place_member *pm, uint32_t npm, uint64_t now_ms,
                     const struct place_settings *set, struct place_out *out)
{
    struct place_in in = {
        .sm = sm,
        .members = pm,
        .nmembers = npm,
        .now_ms = now_ms,
        .mono_ms = now_ms,
        .self_id = 1,
        .set = *set,
        .mutant = g_mutant,
    };
    place_out_init(out);
    pout_plan(&in, out);
}

static const struct place_prop *find_member_prop(const struct place_out *out, uint32_t node, uint8_t state)
{
    for (uint32_t i = 0; i < out->nprops; i++) {
        const struct place_prop *p = &out->props[i];
        if (p->kind == PLACE_PROP_MEMBER_STATE && p->node == node && p->state == state)
            return p;
    }
    return NULL;
}

static int has_log_substr(const struct place_out *out, const char *needle)
{
    int count = 0;
    for (uint32_t i = 0; i < out->nlogs; i++)
        if (strstr(out->logs[i].msg, needle))
            count++;
    return count;
}

/* 1. Normal DOWN -> OUT timer at rebuild_after_s, never before, never self/UP/DRAINING/OUT/DESTROYED. */
static void test_basic_timer(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 6;
    set.out_guard_pct = 25;

    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    mark_state(&sm, &idx, 4, META_DRAINING);

    struct place_out out;
    run_pout(&sm, NULL, 0, 15999, &set, &out);
    if (out.nprops != 0)
        die("basic_timer: proposed %" PRIu32 " ops 1 ms before deadline", out.nprops);
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (out.nprops != 1)
        die("basic_timer: expected 1 prop at deadline, got %" PRIu32, out.nprops);
    const struct place_prop *p = find_member_prop(&out, 5, META_OUT);
    if (!p || p->version != 1 || p->reason != META_DOWN_NONE)
        die("basic_timer: unexpected OUT prop for node 5");
    if (out.nlogs != 0)
        die("basic_timer: unexpected log lines when unguarded");
    place_out_free(&out);

    /* Once OUT or DESTROYED, never proposed OUT again. */
    mark_state(&sm, &idx, 5, META_OUT);
    run_pout(&sm, NULL, 0, 100000, &set, &out);
    if (out.nprops != 0)
        die("basic_timer: proposed on already-OUT member");
    place_out_free(&out);

    mark_state(&sm, &idx, 5, META_DESTROYED);
    run_pout(&sm, NULL, 0, 200000, &set, &out);
    if (out.nprops != 0)
        die("basic_timer: proposed on DESTROYED member");
    place_out_free(&out);

    /* Never OUT self_id even if sm says self_id is DOWN. */
    mark_down(&sm, &idx, 1, META_DOWN_TIMEOUT, 10000);
    run_pout(&sm, NULL, 0, 100000, &set, &out);
    if (find_member_prop(&out, 1, META_OUT))
        die("basic_timer: proposed OUT for self_id");
    place_out_free(&out);

    meta_free(&sm);
    printf("  basic_timer ok\n");
}

/* 2. Fail-stop fast path (D5): beat -> DOWN(failstop) -> OUT at failstop_out_s. Catches `failstopslow`. */
static void test_failstop_fastpath(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 1800;
    set.failstop_out_s = 60;
    set.out_guard_pct = 25;

    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;

    /* UP member reporting failstop -> proposes META_DOWN with META_DOWN_FAILSTOP. */
    struct place_member pm[1] = {{.id = 5, .heard_age_ms = 100, .failstop = 1}};
    struct place_out out;
    run_pout(&sm, pm, 1, 10000, &set, &out);
    const struct place_prop *p = find_member_prop(&out, 5, META_DOWN);
    if (!p || p->reason != META_DOWN_FAILSTOP || p->version != 1)
        die("failstop: UP member with failstop beat did not propose META_DOWN(failstop)");
    place_out_free(&out);

    /* DOWN(timeout) member with failstop beat -> escalates reason to META_DOWN_FAILSTOP. */
    mark_down(&sm, &idx, 4, META_DOWN_TIMEOUT, 10000);
    struct place_member pm4[1] = {{.id = 4, .heard_age_ms = 500, .failstop = 1}};
    run_pout(&sm, pm4, 1, 20000, &set, &out);
    p = find_member_prop(&out, 4, META_DOWN);
    if (!p || p->reason != META_DOWN_FAILSTOP)
        die("failstop: DOWN(timeout) member with failstop beat did not escalate to failstop");
    place_out_free(&out);

    /* Reset node 4 to UP and test node 5 DOWN(failstop): OUT at 60 s, not at 59.999 s. */
    mark_state(&sm, &idx, 4, META_UP);
    mark_down(&sm, &idx, 5, META_DOWN_FAILSTOP, 10000);
    run_pout(&sm, NULL, 0, 69999, &set, &out);
    if (out.nprops != 0)
        die("failstop: proposed OUT before failstop_out_s (got %" PRIu32 ")", out.nprops);
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 70000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("failstop: did not propose OUT at failstop_out_s (70000 ms)");
    place_out_free(&out);

    meta_free(&sm);
    printf("  failstop_fastpath ok\n");
}

/* 3. Operator holds (D3, D4, E12). Catches `holdignored`. */
static void test_operator_hold(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 6;
    set.out_guard_pct = 25;

    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    set_hold(&sm, &idx, 5, 30000, 10000);

    struct place_out out;
    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (out.nprops != 0)
        die("operator_hold: proposed OUT while held (at 16000 ms, hold until 30000 ms)");
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 29999, &set, &out);
    if (out.nprops != 0)
        die("operator_hold: proposed OUT 1 ms before hold expiry");
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 30000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("operator_hold: did not propose OUT once hold expired");
    place_out_free(&out);

    /* Short hold expiring before rebuild_after_s must still wait until rebuild_after_s. */
    set_hold(&sm, &idx, 5, 12000, 10000);
    run_pout(&sm, NULL, 0, 13000, &set, &out);
    if (out.nprops != 0)
        die("operator_hold: proposed OUT before rebuild_after_s after short hold expired");
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("operator_hold: did not propose OUT at rebuild_after_s after short hold");
    place_out_free(&out);

    meta_free(&sm);
    printf("  operator_hold ok\n");
}

/* 4. Opening hold (D4, D5, E12): extends by at most opening_hold_s, never unbounded. Catches `openingunbounded`. */
static void test_opening_hold(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 6;
    set.opening_hold_s = 10;
    set.out_guard_pct = 25;

    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);

    struct place_member pm[1] = {{.id = 5, .heard_age_ms = 200, .opening = 1}};
    struct place_out out;

    /* At rebuild_after_s (16000 ms), actively opening holds OUT. */
    run_pout(&sm, pm, 1, 16000, &set, &out);
    if (out.nprops != 0)
        die("opening_hold: proposed OUT at rebuild_after_s while opening");
    place_out_free(&out);

    /* 1 ms before opening_hold_s bound (10000 + 6000 + 10000 - 1 = 25999 ms): still held. */
    run_pout(&sm, pm, 1, 25999, &set, &out);
    if (out.nprops != 0)
        die("opening_hold: proposed OUT 1 ms before opening_hold_s bound");
    place_out_free(&out);

    /* At 26000 ms (6 + 10 = 16 s): bound expired, OUT must proceed even while opening == 1. */
    run_pout(&sm, pm, 1, 26000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("opening_hold: did not propose OUT once opening_hold_s bound passed");
    place_out_free(&out);

    /* If heartbeat went stale (heard_age_ms >= node_timeout_ms = 3000), opening no longer holds OUT. */
    pm[0].heard_age_ms = 3000;
    run_pout(&sm, pm, 1, 16000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("opening_hold: stale heartbeat with opening=1 wrongly held OUT");
    place_out_free(&out);

    meta_free(&sm);
    printf("  opening_hold ok\n");
}

/* 5. Mass-OUT percentage guard (D4): > out_guard_pct of non-OUT/non-DESTROYED members DOWN, bounded by
 *    out_guard_hold_s from the crossing member's down_since_ms. Catches `noguard` and `guardunbounded`. */
static void test_pct_guard(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 6;
    set.out_guard_pct = 25;
    set.out_guard_hold_s = 15;

    /* Exact boundary: 1 of 4 DOWN = 25% (NOT > 25%) -> NOT guarded. */
    {
        struct meta_state sm4;
        make_cluster(&sm4, 4, NULL, &set);
        uint64_t idx4 = 1;
        mark_down(&sm4, &idx4, 4, META_DOWN_TIMEOUT, 10000);
        struct place_out out;
        run_pout(&sm4, NULL, 0, 16000, &set, &out);
        if (!find_member_prop(&out, 4, META_OUT))
            die("pct_guard: 1 of 4 DOWN (25%%) was wrongly guarded");
        if (has_log_substr(&out, "guard bound passed") != 0)
            die("pct_guard: 1 of 4 DOWN logged guard bound passed");
        place_out_free(&out);
        meta_free(&sm4);
    }

    /* 2 of 5 DOWN = 40% > 25%: node 4 down at 10000, node 5 crosses at 12000 -> guard_due = 27000. */
    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;
    mark_down(&sm, &idx, 4, META_DOWN_TIMEOUT, 10000);
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 12000);

    struct place_out out;
    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (out.nprops != 0)
        die("pct_guard: proposed OUT while guarded at 16000 ms (got %" PRIu32 ")", out.nprops);
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 26999, &set, &out);
    if (out.nprops != 0 || out.nlogs != 0)
        die("pct_guard: acted 1 ms before guard bound (26999 ms)");
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 27000, &set, &out);
    if (!find_member_prop(&out, 4, META_OUT) || !find_member_prop(&out, 5, META_OUT))
        die("pct_guard: did not propose OUT for nodes 4 and 5 once guard bound passed");
    if (has_log_substr(&out, "guard bound passed") != 1)
        die("pct_guard: expected exactly 1 'guard bound passed' log, got %d",
            has_log_substr(&out, "guard bound passed"));
    place_out_free(&out);

    /* OUT member is excluded from non_out: once node 4 is OUT, node 5 is 1/4 = 25% -> unguarded. */
    mark_state(&sm, &idx, 4, META_OUT);
    run_pout(&sm, NULL, 0, 18000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("pct_guard: with node 4 OUT, 1/4 DOWN (25%%) should not be guarded");
    if (has_log_substr(&out, "guard bound passed") != 0)
        die("pct_guard: unguarded OUT should not log guard bound passed");
    place_out_free(&out);

    /* Short guard bound (< rebuild_after_s): no log or OUT before rebuild_after_s. */
    meta_free(&sm);
    set.out_guard_hold_s = 3;
    make_cluster(&sm, 5, NULL, &set);
    idx = 1;
    mark_down(&sm, &idx, 4, META_DOWN_TIMEOUT, 10000);
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    run_pout(&sm, NULL, 0, 14000, &set, &out);
    if (out.nprops != 0 || out.nlogs != 0)
        die("pct_guard: emitted OUT/log before rebuild_after_s when guard_hold < rebuild_after");
    place_out_free(&out);
    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (!find_member_prop(&out, 4, META_OUT) || !find_member_prop(&out, 5, META_OUT) ||
        has_log_substr(&out, "guard bound passed") != 1)
        die("pct_guard: expected OUT + 1 guard log at rebuild_after_s after short guard bound");
    place_out_free(&out);

    meta_free(&sm);
    printf("  pct_guard ok\n");
}

/* 6. Single-rack mass-OUT guard (D4): every non-OUT member of one rack DOWN (>= 2 configured racks). */
static void test_rack_guard(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 6;
    set.out_guard_pct = 50; /* 2/6 = 33% will NOT trigger pct guard; only rack guard triggers */
    set.out_guard_hold_s = 15;

    static const char *const racks[6] = {"rack-a", "rack-a", "rack-b", "rack-b", "rack-c", "rack-c"};
    struct meta_state sm;
    make_cluster(&sm, 6, racks, &set);
    uint64_t idx = 1;
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    mark_down(&sm, &idx, 6, META_DOWN_TIMEOUT, 11000); /* crosses whole-rack line at 11000 */

    struct place_out out;
    run_pout(&sm, NULL, 0, 17000, &set, &out);
    if (out.nprops != 0)
        die("rack_guard: proposed OUT while entire rack-c is DOWN (17000 ms)");
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 25999, &set, &out);
    if (out.nprops != 0)
        die("rack_guard: proposed OUT 1 ms before rack guard bound");
    place_out_free(&out);

    run_pout(&sm, NULL, 0, 26000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT) || !find_member_prop(&out, 6, META_OUT))
        die("rack_guard: did not propose OUT for rack-c once guard bound passed");
    if (has_log_substr(&out, "guard bound passed") != 1)
        die("rack_guard: expected 1 'guard bound passed' log, got %d", has_log_substr(&out, "guard bound passed"));
    place_out_free(&out);
    meta_free(&sm);

    /* Only 1 rack configured: rack guard does NOT engage. */
    static const char *const one_rack[6] = {"rack-a", "rack-a", "rack-a", "rack-a", "rack-a", "rack-a"};
    make_cluster(&sm, 6, one_rack, &set);
    idx = 1;
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("rack_guard: single-rack cluster wrongly triggered rack guard");
    place_out_free(&out);
    meta_free(&sm);

    /* Single-member racks (one server per rack, 1 of 5 DOWN = 20% <= 25%): rack guard must NOT engage on 1 node. */
    static const char *const solo_racks[5] = {"r1", "r2", "r3", "r4", "r5"};
    set.out_guard_pct = 25;
    make_cluster(&sm, 5, solo_racks, &set);
    idx = 1;
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    run_pout(&sm, NULL, 0, 16000, &set, &out);
    if (!find_member_prop(&out, 5, META_OUT))
        die("rack_guard: single-member rack wrongly triggered rack guard on 1-node failure");
    place_out_free(&out);
    meta_free(&sm);
    printf("  rack_guard ok\n");
}

/* 7. Safety exclusions: never OUT last ISR member while merely DOWN under a guard; never OUT mid-move target. */
static void test_safety_exclusions(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 6;
    set.out_guard_pct = 25;
    set.out_guard_hold_s = 15;

    /* Last ISR member under a guard (after guard bound passes): spared if merely DOWN. */
    {
        struct meta_state sm;
        make_cluster(&sm, 5, NULL, &set);
        uint64_t idx = 1;
        uint32_t reps[3] = {4, 2, 3};
        add_vol(&sm, &idx, 1, reps, 3, 1);
        uint32_t lone_isr[1] = {4};
        set_isr(&sm, &idx, 1, lone_isr, 1);
        /* Also a second volume where node 5 has an UP co-ISR member (node 1): node 5 CAN go OUT. */
        uint32_t reps2[3] = {1, 2, 5};
        add_vol(&sm, &idx, 2, reps2, 3, 1);

        mark_down(&sm, &idx, 4, META_DOWN_TIMEOUT, 10000);
        mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);

        struct place_out out;
        run_pout(&sm, NULL, 0, 25000, &set, &out);
        if (find_member_prop(&out, 4, META_OUT))
            die("safety: proposed OUT for last ISR member (node 4) while merely DOWN under a guard");
        if (!find_member_prop(&out, 5, META_OUT))
            die("safety: did not propose OUT for node 5 (has UP co-ISR member) after guard bound passed");
        if (has_log_substr(&out, "guard bound passed") != 1)
            die("safety: expected 1 'guard bound passed' log when node 5 went OUT");
        place_out_free(&out);
        meta_free(&sm);
    }

    /* Mid-move target (learner with replaces != 0): never OUT; move source CAN go OUT. */
    {
        struct meta_state sm;
        make_cluster(&sm, 5, NULL, &set);
        uint64_t idx = 1;
        uint32_t reps[3] = {1, 2, 3};
        add_vol(&sm, &idx, 1, reps, 3, 1);
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_replica_add(&e, 1, 1, 5, 999, 3); /* node 5 moving in to replace node 3 */
        apply_one(&sm, &e, &idx);

        /* Node 5 (target) DOWN past rebuild_after_s: must NOT go OUT. */
        mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
        struct place_out out;
        run_pout(&sm, NULL, 0, 20000, &set, &out);
        if (find_member_prop(&out, 5, META_OUT))
            die("safety: proposed OUT for mid-move target (node 5)");
        place_out_free(&out);

        /* Reset node 5 UP, take node 3 (source) DOWN past rebuild_after_s: CAN go OUT. */
        mark_state(&sm, &idx, 5, META_UP);
        mark_down(&sm, &idx, 3, META_DOWN_TIMEOUT, 10000);
        run_pout(&sm, NULL, 0, 20000, &set, &out);
        if (!find_member_prop(&out, 3, META_OUT))
            die("safety: move source (node 3) should still be allowed to go OUT");
        place_out_free(&out);
        meta_free(&sm);
    }

    printf("  safety_exclusions ok\n");
}

/* 8. Per-replica LOCAL_FAULT escalation (D6). Also catches `failstopslow`. */
static void test_replica_fault(void)
{
    struct place_settings set;
    place_settings_default(&set);
    set.rebuild_after_s = 60;
    set.failstop_out_s = 2;
    set.out_guard_pct = 25;

    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;
    uint32_t reps[3] = {1, 2, 3};
    add_vol(&sm, &idx, 1, reps, 3, 1);

    /* Fault duration 1 s (< failstop_out_s 2 s): no proposal. */
    struct place_member pm[2] = {
        {.id = 3, .heard_age_ms = 100, .nfaults = 1, .faults = {{.volume = 1, .fault_s = 1}}},
        {.id = 2, .heard_age_ms = 100, .nfaults = 0},
    };
    struct place_out out;
    run_pout(&sm, pm, 2, 10000, &set, &out);
    if (out.nprops != 0)
        die("replica_fault: proposed replace before failstop_out_s");
    place_out_free(&out);

    /* Fault duration 2 s (>= failstop_out_s): emits PLACE_PROP_REPLICA_ADD with replaces = 3. */
    pm[0].faults[0].fault_s = 2;
    run_pout(&sm, pm, 2, 11000, &set, &out);
    if (out.nprops != 1)
        die("replica_fault: expected 1 REPLICA_ADD prop, got %" PRIu32, out.nprops);
    const struct place_prop *p = &out.props[0];
    if (p->kind != PLACE_PROP_REPLICA_ADD || p->volume != 1 || p->node != 0 || p->other_node != 3 || p->epoch != 1 ||
        p->reason != META_DOWN_FAULT || p->margin != 2)
        die("replica_fault: unexpected prop (kind=%d vol=%" PRIu32 " node=%" PRIu32 " other=%" PRIu32 " ep=%" PRIu64
            " reason=%u margin=%d)",
            (int)p->kind, p->volume, p->node, p->other_node, p->epoch, p->reason, p->margin);
    place_out_free(&out);

    /* Two replicas of volume 1 both faulted: only ONE replace emitted for volume 1 (R4 D10). */
    pm[1].nfaults = 1;
    pm[1].faults[0] = (struct place_vol_fault){.volume = 1, .fault_s = 5};
    run_pout(&sm, pm, 2, 12000, &set, &out);
    if (out.nprops != 1)
        die("replica_fault: expected 1 move per volume when 2 replicas faulted, got %" PRIu32, out.nprops);
    place_out_free(&out);

    /* Stale heartbeat: no replace emitted. */
    pm[1].nfaults = 0;
    pm[0].heard_age_ms = 3000;
    run_pout(&sm, pm, 2, 12000, &set, &out);
    if (out.nprops != 0)
        die("replica_fault: emitted replace for stale heartbeat");
    place_out_free(&out);

    /* Already has an active move on volume 1: no second replace emitted. */
    pm[0].heard_age_ms = 100;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_replica_add(&e, 1, 1, 4, 777, 3);
    apply_one(&sm, &e, &idx);
    run_pout(&sm, pm, 2, 12000, &set, &out);
    if (out.nprops != 0)
        die("replica_fault: emitted replace while move already active on volume 1");
    place_out_free(&out);

    /* Non-replica member or DOWN member reporting fault: ignored. */
    uint32_t reps2[3] = {1, 2, 4};
    add_vol(&sm, &idx, 2, reps2, 3, 1);
    pm[0].faults[0] = (struct place_vol_fault){.volume = 2, .fault_s = 10}; /* node 3 not in vol 2 */
    run_pout(&sm, pm, 2, 12000, &set, &out);
    if (out.nprops != 0)
        die("replica_fault: emitted replace for non-replica node");
    place_out_free(&out);

    mark_down(&sm, &idx, 2, META_DOWN_TIMEOUT, 10000);
    pm[1].nfaults = 1;
    pm[1].faults[0] = (struct place_vol_fault){.volume = 2, .fault_s = 10};
    run_pout(&sm, pm, 2, 12000, &set, &out);
    if (out.nprops != 0)
        die("replica_fault: emitted replace for DOWN member");
    place_out_free(&out);

    meta_free(&sm);
    printf("  replica_fault ok\n");
}

/* 9. Purity / determinism & defensive handling of NULL / uninitialised inputs. */
static void test_purity_and_null(void)
{
    struct place_out out1, out2;
    place_out_init(&out1);
    pout_plan(NULL, &out1);
    pout_plan(NULL, NULL);
    struct meta_state empty;
    meta_init(&empty);
    struct place_settings set;
    place_settings_default(&set);
    struct place_in in = {.sm = &empty, .now_ms = 50000, .set = set, .self_id = 1};
    pout_plan(&in, &out1);
    if (out1.nprops != 0 || out1.nlogs != 0)
        die("purity: uninitialised meta_state emitted output");
    place_out_free(&out1);
    meta_free(&empty);

    /* Two identical calls must yield identical outputs. */
    set.rebuild_after_s = 6;
    set.out_guard_pct = 25;
    set.out_guard_hold_s = 15;
    struct meta_state sm;
    make_cluster(&sm, 5, NULL, &set);
    uint64_t idx = 1;
    mark_down(&sm, &idx, 4, META_DOWN_TIMEOUT, 10000);
    mark_down(&sm, &idx, 5, META_DOWN_TIMEOUT, 10000);
    run_pout(&sm, NULL, 0, 25000, &set, &out1);
    run_pout(&sm, NULL, 0, 25000, &set, &out2);
    if (out1.nprops != out2.nprops || out1.nlogs != out2.nlogs)
        die("purity: repeated pout_plan calls differed in count");
    for (uint32_t i = 0; i < out1.nprops; i++) {
        const struct place_prop *a = &out1.props[i], *b = &out2.props[i];
        if (a->kind != b->kind || a->volume != b->volume || a->node != b->node || a->other_node != b->other_node ||
            a->pos != b->pos || a->epoch != b->epoch || a->version != b->version || a->state != b->state ||
            a->reason != b->reason || a->margin != b->margin)
            die("purity: repeated pout_plan calls differed in prop %" PRIu32, i);
    }
    for (uint32_t i = 0; i < out1.nlogs; i++)
        if (strcmp(out1.logs[i].msg, out2.logs[i].msg) != 0)
            die("purity: repeated pout_plan calls differed in log %" PRIu32, i);
    place_out_free(&out1);
    place_out_free(&out2);
    meta_free(&sm);
    printf("  purity_and_null ok\n");
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            g_mutant = parse_mutant(argv[++i]);
        else
            die("unknown argument \"%s\"", argv[i]);
    }
    printf("test_pout (mutant=%d):\n", g_mutant);
    test_basic_timer();
    test_failstop_fastpath();
    test_operator_hold();
    test_opening_hold();
    test_pct_guard();
    test_rack_guard();
    test_safety_exclusions();
    test_replica_fault();
    test_purity_and_null();
    printf("test_pout: OK\n");
    return 0;
}
