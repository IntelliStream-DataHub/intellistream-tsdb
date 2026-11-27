/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit tests for the pure R9 placement planner (server/place.c, server/place.h; track r9-plan).
 *
 * Exercises place_volume, place_stripe, place_rack_survivable, and place_plan over hand-built metadata states and
 * applies emitted proposals through real meta_apply(). Passing `--mutant NAME` runs the suite with one of r9-plan's
 * five planted bugs (orderfifo, rackignored, twoperserver, moveshrinks, wholenobarrier); each must fail at least one
 * assertion here.
 */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "meta.h"
#include "place.h"

static int fails;
#define CHECK(c, ...)                                                                                                  \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);                                                      \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fputc('\n', stderr);                                                                                       \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

static int g_mut = PLACE_MUT_NONE;
static uint64_t g_idx = 1;

static int apply_entry(struct meta_state *sm, struct meta_entry *e, int *res, uint32_t nres)
{
    meta_entry_finish(e);
    int rc = meta_apply(sm, ++g_idx, 1, e->buf, e->len, res, nres);
    meta_entry_free(e);
    return rc;
}

static void init_cluster_state(struct meta_state *sm, uint32_t nmembers, const char *const *racks,
                               struct place_member *pm, uint32_t stores_per_node)
{
    meta_init(sm);
    struct meta_settings s;
    meta_settings_default(&s);
    uint8_t uuid[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    struct meta_member first = {
        .id = 1,
        .state = META_UP,
        .incarnation = 1,
        .flags = META_MF_ALL,
    };
    snprintf(first.addr, sizeof first.addr, "127.0.0.1:10001");
    snprintf(first.peer, sizeof first.peer, "127.0.0.1:20001");
    if (racks && racks[0])
        snprintf(first.rack, sizeof first.rack, "%s", racks[0]);
    first.ident[0] = 1;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_init(&e, uuid, &s, &first, META_MODE_REPLICATED, META_LEVEL);
    int res[4] = {0};
    int rc = apply_entry(sm, &e, res, 4);
    CHECK(rc == META_OK && res[0] == META_OK, "init cluster: rc %d res %d", rc, res[0]);

    for (uint32_t i = 2; i <= nmembers; i++) {
        struct meta_member mb = {
            .id = i,
            .state = META_UP,
            .incarnation = 1,
            .flags = META_MF_ALL,
        };
        snprintf(mb.addr, sizeof mb.addr, "127.0.0.1:%" PRIu32, 10000 + i);
        snprintf(mb.peer, sizeof mb.peer, "127.0.0.1:%" PRIu32, 20000 + i);
        if (racks && racks[i - 1])
            snprintf(mb.rack, sizeof mb.rack, "%s", racks[i - 1]);
        mb.ident[0] = (uint8_t)i;
        meta_entry_init(&e);
        meta_op_member_add(&e, &mb);
        rc = apply_entry(sm, &e, res, 4);
        CHECK(rc == META_OK && res[0] == META_OK, "member add %" PRIu32 ": rc %d res %d", i, rc, res[0]);
    }

    if (pm) {
        for (uint32_t i = 0; i < nmembers; i++) {
            memset(&pm[i], 0, sizeof pm[i]);
            pm[i].id = i + 1;
            pm[i].heard_age_ms = 50;
            pm[i].fill_pct = 20;
            pm[i].chunk_stores = stores_per_node;
        }
    }
}

static void add_vol(struct meta_state *sm, uint32_t num, const char *name, const uint32_t *reps, uint32_t nrep,
                    uint8_t ec_k, uint8_t ec_m, const uint32_t *stripe)
{
    struct meta_volume_add a = {0};
    a.number = num;
    snprintf(a.name, sizeof a.name, "%s", name);
    a.uuid[0] = (uint8_t)num;
    a.uuid[15] = 0xab;
    a.kind = META_VOL_REPLICATED;
    a.n = nrep;
    for (uint32_t i = 0; i < nrep; i++) {
        a.nodes[i] = reps[i];
        a.rid[i] = ((uint64_t)num << 32) | (i + 1);
    }
    if (ec_k && ec_m) {
        a.redundancy = META_RED_EC;
        a.ec_k = ec_k;
        a.ec_m = ec_m;
        a.nstripe = (uint32_t)ec_k + ec_m;
        memcpy(a.stripe, stripe, a.nstripe * sizeof(uint32_t));
    }
    a.min_isr = meta_volume_min_isr(a.redundancy, a.ec_m, a.n, sm->set.min_isr);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_volume_add(&e, &a);
    int res[4] = {0};
    int rc = apply_entry(sm, &e, res, 4);
    CHECK(rc == META_OK && res[0] == META_OK, "vol add %s (#%" PRIu32 "): rc %d res %d", name, num, rc, res[0]);
}

static void set_member_state(struct meta_state *sm, uint32_t id, int state)
{
    const struct meta_member *mb = meta_member(sm, id);
    CHECK(mb != NULL, "set_member_state: missing node %" PRIu32, id);
    if (!mb)
        return;
    struct meta_entry e;
    meta_entry_init(&e);
    if (state == META_DOWN)
        meta_op_member_state(&e, id, state, mb->incarnation, META_DOWN_TIMEOUT, 1000, 1000);
    else
        meta_op_member_state(&e, id, state, mb->incarnation, META_DOWN_NONE, 0, 0);
    int res[4] = {0};
    int rc = apply_entry(sm, &e, res, 4);
    CHECK(rc == META_OK && res[0] == META_OK, "member %" PRIu32 " -> state %d: rc %d res %d", id, state, rc, res[0]);
}

static void set_isr(struct meta_state *sm, uint32_t vol, const uint32_t *isr, uint32_t nisr)
{
    const struct meta_volume *v = meta_volume(sm, vol);
    CHECK(v != NULL, "set_isr: missing vol %" PRIu32, vol);
    if (!v)
        return;
    uint64_t rids[META_REPLICAS_MAX] = {0};
    for (uint32_t i = 0; i < nisr; i++) {
        const struct meta_replica *r = meta_replica(v, isr[i]);
        CHECK(r != NULL, "set_isr: node %" PRIu32 " not in vol %" PRIu32, isr[i], vol);
        rids[i] = r ? r->rid : 0;
    }
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_isr_change(&e, vol, v->epoch, v->isr_version, isr, rids, nisr, 0);
    int res[4] = {0};
    int rc = apply_entry(sm, &e, res, 4);
    CHECK(rc == META_OK && res[0] == META_OK, "isr_change vol %" PRIu32 ": rc %d res %d", vol, rc, res[0]);
}

static struct place_in make_in(const struct meta_state *sm, const struct place_member *pm, uint32_t nm)
{
    struct place_in in = {
        .sm = sm,
        .members = pm,
        .nmembers = nm,
        .now_ms = 100000,
        .mono_ms = 100000,
        .self_id = 1,
        .mutant = g_mut,
    };
    place_settings_default(&in.set);
    return in;
}

static int has_dup_u32(const uint32_t *a, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++)
        for (uint32_t j = i + 1; j < n; j++)
            if (a[i] == a[j])
                return 1;
    return 0;
}

static uint32_t max_per_rack(const struct meta_state *sm, const uint32_t *nodes, uint32_t n)
{
    uint32_t mx = 0;
    for (uint32_t i = 0; i < n; i++) {
        const struct meta_member *mi = meta_member(sm, nodes[i]);
        if (!mi)
            continue;
        uint32_t cnt = 0;
        for (uint32_t j = 0; j < n; j++) {
            const struct meta_member *mj = meta_member(sm, nodes[j]);
            if (mj && !strcmp(mi->rack, mj->rack))
                cnt++;
        }
        if (cnt > mx)
            mx = cnt;
    }
    return mx;
}

/* 1. Initial placement (place_volume & place_stripe) and rack survivability (E9, D20). */
static void test_initial_placement_and_racks(void)
{
    static const char *const racks7[7] = {"r1", "r1", "r1", "r2", "r2", "r3", "r3"};
    struct meta_state sm;
    struct place_member pm[7];
    init_cluster_state(&sm, 7, racks7, pm, 2);

    struct place_in in = make_in(&sm, pm, 7);
    uint32_t log3[3] = {0}, stripe6[6] = {0}, qual = 0;
    CHECK(place_volume(&in, 3, log3) == 0, "place_volume 3 on 7 racked nodes failed");
    CHECK(!has_dup_u32(log3, 3), "place_volume placed duplicate server: %u,%u,%u", log3[0], log3[1], log3[2]);
    CHECK(max_per_rack(&sm, log3, 3) == 1, "place_volume did not use 3 distinct racks: max %u",
          max_per_rack(&sm, log3, 3));

    CHECK(place_stripe(&in, 6, stripe6, &qual) == 0 && qual == 7, "place_stripe 6 on 7 failed (qual %u)", qual);
    CHECK(!has_dup_u32(stripe6, 6), "place_stripe placed duplicate server");
    CHECK(max_per_rack(&sm, stripe6, 6) == 2, "place_stripe 4+2 on 3 racks: max per rack %u, want 2 (<= m)",
          max_per_rack(&sm, stripe6, 6));

    /* Add the volume and verify rack loss survivability = yes. */
    add_vol(&sm, 1, "ec1", log3, 3, 4, 2, stripe6);
    CHECK(place_rack_survivable(&sm, meta_volume(&sm, 1)) == 1, "ec1 (2/2/2 on 3 racks, 1/1/1 log) should be survivable");

    /* A volume with 3 stripe positions on r1 (exceeds m=2) is NOT rack-loss survivable. */
    uint32_t bad_stripe[6] = {1, 2, 3, 4, 5, 6};
    add_vol(&sm, 2, "ec_bad_rack", log3, 3, 4, 2, bad_stripe);
    CHECK(place_rack_survivable(&sm, meta_volume(&sm, 2)) == 0, "ec_bad_rack (3 on r1 > m=2) must not be survivable");

    /* An EC volume with 2 log replicas on r1 (min_isr=2 -> losing r1 leaves 1 < 2) is NOT survivable. */
    uint32_t bad_log[3] = {1, 2, 4};
    add_vol(&sm, 3, "ec_bad_log", bad_log, 3, 4, 2, stripe6);
    CHECK(place_rack_survivable(&sm, meta_volume(&sm, 3)) == 0, "ec_bad_log (2 log on r1, min_isr=2) must not survive");

    /* Plain 3-replica volume (min_isr=1): 3 racks -> yes; 2 racks -> yes; 1 rack -> no. */
    uint32_t one_rack[3] = {1, 2, 3};
    add_vol(&sm, 4, "rep_ok", log3, 3, 0, 0, NULL);
    add_vol(&sm, 5, "rep_two", bad_log, 3, 0, 0, NULL);
    add_vol(&sm, 6, "rep_one", one_rack, 3, 0, 0, NULL);
    CHECK(place_rack_survivable(&sm, meta_volume(&sm, 4)) == 1, "rep_ok on 3 racks should survive");
    CHECK(place_rack_survivable(&sm, meta_volume(&sm, 5)) == 1, "rep_two (min_isr=1) on 2 racks should survive");
    CHECK(place_rack_survivable(&sm, meta_volume(&sm, 6)) == 0, "rep_one on 1 rack must not survive");

    /* Storage-capable filter: dropping chunk_stores on two nodes leaves only 5 < 6 -> place_stripe fails. */
    pm[5].chunk_stores = 0;
    pm[6].chunk_stores = 0;
    CHECK(place_stripe(&in, 6, stripe6, &qual) == -1 && qual == 5,
          "place_stripe with 5 stores must fail with qual=5 (got %u)", qual);
    CHECK(place_volume(&in, 3, log3) == 0, "place_volume should still succeed without chunk stores");

    meta_free(&sm);

    /* Unracked cluster: place_volume deterministic 1,2,3 (test_cluster volumes unchanged), rack_survivable = 0. */
    struct meta_state sm2;
    struct place_member pm2[3];
    init_cluster_state(&sm2, 3, NULL, pm2, 0);
    struct place_in in2 = make_in(&sm2, pm2, 3);
    uint32_t r3[3] = {0};
    CHECK(place_volume(&in2, 3, r3) == 0, "place_volume on 3 unracked nodes failed");
    CHECK(!has_dup_u32(r3, 3) && r3[0] == 1 && r3[1] == 2 && r3[2] == 3,
          "unracked 3-node place_volume: got %u,%u,%u want 1,2,3", r3[0], r3[1], r3[2]);
    add_vol(&sm2, 1, "unracked", r3, 3, 0, 0, NULL);
    CHECK(place_rack_survivable(&sm2, meta_volume(&sm2, 1)) == 0, "unracked volume must report rack survivable = no");
    meta_free(&sm2);
}

/* 2. 7-node 4+2 cluster across 3 racks: OUT one -> both log and stripe re-placed cleanly, no move shrink. */
static void test_out_one_ec42(void)
{
    static const char *const racks7[7] = {"r1", "r1", "r1", "r2", "r2", "r3", "r3"};
    struct meta_state sm;
    struct place_member pm[7];
    init_cluster_state(&sm, 7, racks7, pm, 2);
    uint32_t reps[3] = {1, 4, 6};
    uint32_t stripe[6] = {1, 2, 4, 5, 6, 7};
    add_vol(&sm, 1, "ec42", reps, 3, 4, 2, stripe);

    set_member_state(&sm, 1, META_OUT);
    pm[0].heard_age_ms = UINT64_MAX;

    struct place_in in = make_in(&sm, pm, 7);
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);

    int saw_add = 0, saw_rem = 0, saw_ss = 0;
    struct place_prop padd = {0}, pss = {0};
    for (uint32_t i = 0; i < out.nprops; i++) {
        const struct place_prop *p = &out.props[i];
        if (p->kind == PLACE_PROP_REPLICA_REMOVE)
            saw_rem++;
        else if (p->kind == PLACE_PROP_REPLICA_ADD) {
            saw_add++;
            padd = *p;
        } else if (p->kind == PLACE_PROP_STRIPE_SET) {
            saw_ss++;
            pss = *p;
        }
    }
    CHECK(saw_rem == 0, "log move must never emit REPLICA_REMOVE before learner joins (saw %d)", saw_rem);
    CHECK(saw_add == 1, "want 1 REPLICA_ADD for OUT log replica, got %d", saw_add);
    if (saw_add == 1) {
        CHECK(padd.volume == 1 && padd.other_node == 1 && (padd.node == 2 || padd.node == 3),
              "log target should be on r1 (2 or 3) replacing 1, got node %u replaces %u", padd.node, padd.other_node);
        CHECK(padd.node != 4 && padd.node != 6, "log target %u already holds a replica of vol 1", padd.node);
    }
    CHECK(saw_ss == 1, "want 1 STRIPE_SET for OUT stripe position, got %d", saw_ss);
    if (saw_ss == 1) {
        CHECK(pss.volume == 1 && pss.pos == 0 && pss.node == 3 && pss.other_node == 1 &&
                  pss.reason == META_STRIPE_FILLING && pss.version == 1,
              "stripe set: vol %u pos %u node %u prev %u reason %u ver %" PRIu64, pss.volume, pss.pos, pss.node,
              pss.other_node, pss.reason, pss.version);
        /* Apply through real meta_apply and check resulting state. */
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_stripe_set(&e, pss.volume, pss.pos, pss.node, pss.reason, pss.other_node, pss.version);
        int res[2] = {0};
        CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "apply STRIPE_SET: res %d", res[0]);
        const struct meta_volume *v = meta_volume(&sm, 1);
        CHECK(v->stripe[0] == 3 && v->stripe_pos[0].state == META_STRIPE_FILLING && v->stripe_pos[0].prev == 1 &&
                  v->stripe_version == 2,
              "after STRIPE_SET: holder %u state %u prev %u ver %" PRIu64, v->stripe[0], v->stripe_pos[0].state,
              v->stripe_pos[0].prev, v->stripe_version);
    }
    place_out_free(&out);
    meta_free(&sm);
}

/* 3. Rack cap on re-placement (8 nodes, 3 racks): catches rackignored in place_plan. */
static void test_rack_cap_replacement(void)
{
    static const char *const racks8[8] = {"r1", "r1", "r1", "r2", "r2", "r2", "r3", "r3"};
    struct meta_state sm;
    struct place_member pm[8];
    init_cluster_state(&sm, 8, racks8, pm, 2);
    /* Vol 1: stripe {1,2 (r1), 4,5 (r2), 7,8 (r3)}. Vol 2 gives node 6 (r2) higher holds than node 3 (r1). */
    uint32_t reps1[3] = {1, 4, 7};
    uint32_t stripe1[6] = {1, 2, 4, 5, 7, 8};
    uint32_t reps2[3] = {2, 5, 8};
    uint32_t stripe2[6] = {1, 2, 5, 6, 7, 8};
    add_vol(&sm, 1, "v1", reps1, 3, 4, 2, stripe1);
    add_vol(&sm, 2, "v2", reps2, 3, 4, 2, stripe2);

    /* Node 4 (r2) goes OUT. Node 3 (r1) has 0 stripe holds and lower id than node 6 (r2, 1 hold),
     * but placing vol 1's pos 2 on node 3 would put 3 positions on r1 (> m=2). Must pick node 6 (r2)! */
    set_member_state(&sm, 4, META_OUT);
    pm[3].heard_age_ms = UINT64_MAX;

    struct place_in in = make_in(&sm, pm, 8);
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);

    int found_ss = 0, found_add = 0;
    for (uint32_t i = 0; i < out.nprops; i++) {
        const struct place_prop *p = &out.props[i];
        if (p->kind == PLACE_PROP_STRIPE_SET && p->volume == 1) {
            found_ss++;
            CHECK(p->node == 6 && p->pos == 2 && p->other_node == 4,
                  "rack cap re-placement: picked node %u (rack %s), want node 6 (r2)", p->node,
                  meta_member(&sm, p->node)->rack);
        }
        if (p->kind == PLACE_PROP_REPLICA_ADD && p->volume == 1) {
            found_add++;
            CHECK(p->node == 5 || p->node == 6,
                  "log re-placement must stay on r2 (node 5 or 6), got node %u (rack %s)", p->node,
                  meta_member(&sm, p->node)->rack);
        }
    }
    CHECK(found_ss == 1 && found_add == 1, "want 1 stripe + 1 log prop for v1, got %d, %d", found_ss, found_add);
    place_out_free(&out);
    meta_free(&sm);
}

/* 4. OUT two on 7-node 4+2 (only 1 spare -> no duplicate node!), and OUT one per rack on 9-node 4+2. */
static void test_out_two_and_per_rack(void)
{
    /* 7-node 4+2, OUT two (nodes 1 and 4): only node 3 is spare. */
    static const char *const racks7[7] = {"r1", "r1", "r1", "r2", "r2", "r3", "r3"};
    struct meta_state sm;
    struct place_member pm[7];
    init_cluster_state(&sm, 7, racks7, pm, 2);
    uint32_t reps[3] = {2, 5, 6};
    uint32_t stripe[6] = {1, 2, 4, 5, 6, 7};
    add_vol(&sm, 1, "v7", reps, 3, 4, 2, stripe);
    set_member_state(&sm, 1, META_OUT);
    set_member_state(&sm, 4, META_OUT);
    pm[0].heard_age_ms = pm[3].heard_age_ms = UINT64_MAX;

    struct place_in in = make_in(&sm, pm, 7);
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET && out.props[0].node == 3,
          "tick 1 OUT-2 on 7: want 1 STRIPE_SET to node 3, got %u props (first node %u)", out.nprops,
          out.nprops ? out.props[0].node : 0);
    if (out.nprops >= 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET) {
        struct meta_entry e;
        meta_entry_init(&e);
        meta_op_stripe_set(&e, out.props[0].volume, out.props[0].pos, out.props[0].node, out.props[0].reason,
                           out.props[0].other_node, out.props[0].version);
        int res[2] = {0};
        CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "apply first OUT-2 STRIPE_SET: %d", res[0]);
    }
    place_out_free(&out);

    /* Tick 2: all 5 UP nodes now hold a position of v7 -> MUST emit 0 STRIPE_SET (never place 2 on one server!). */
    place_out_init(&out);
    place_plan(&in, &out);
    int ss2 = 0;
    for (uint32_t i = 0; i < out.nprops; i++)
        if (out.props[i].kind == PLACE_PROP_STRIPE_SET)
            ss2++;
    CHECK(ss2 == 0, "tick 2 OUT-2 on 7: no spare node left, must emit 0 STRIPE_SET (got %d)", ss2);
    place_out_free(&out);
    meta_free(&sm);

    /* 9-node 3-rack 4+2: OUT one per rack (1, 4, 7) -> 3 ticks move them onto 3, 6, 9 preserving 2/2/2 rack balance. */
    static const char *const racks9[9] = {"r1", "r1", "r1", "r2", "r2", "r2", "r3", "r3", "r3"};
    struct place_member pm9[9];
    init_cluster_state(&sm, 9, racks9, pm9, 2);
    uint32_t reps9[3] = {2, 5, 8};
    uint32_t stripe9[6] = {1, 2, 4, 5, 7, 8};
    add_vol(&sm, 1, "v9", reps9, 3, 4, 2, stripe9);
    set_member_state(&sm, 1, META_OUT);
    set_member_state(&sm, 4, META_OUT);
    set_member_state(&sm, 7, META_OUT);
    pm9[0].heard_age_ms = pm9[3].heard_age_ms = pm9[6].heard_age_ms = UINT64_MAX;

    in = make_in(&sm, pm9, 9);
    for (int tick = 0; tick < 3; tick++) {
        place_out_init(&out);
        place_plan(&in, &out);
        CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET,
              "9-node per-rack tick %d: want 1 STRIPE_SET, got %u", tick, out.nprops);
        if (out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET) {
            struct meta_entry e;
            meta_entry_init(&e);
            meta_op_stripe_set(&e, out.props[0].volume, out.props[0].pos, out.props[0].node, out.props[0].reason,
                               out.props[0].other_node, out.props[0].version);
            int res[2] = {0};
            CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "9-node tick %d apply: %d", tick,
                  res[0]);
        }
        place_out_free(&out);
    }
    const struct meta_volume *v9 = meta_volume(&sm, 1);
    CHECK(!has_dup_u32(v9->stripe, 6), "9-node final stripe has duplicates");
    CHECK(max_per_rack(&sm, v9->stripe, 6) == 2, "9-node final stripe max per rack %u, want 2",
          max_per_rack(&sm, v9->stripe, 6));
    CHECK(place_rack_survivable(&sm, v9) == 1, "9-node final stripe should remain rack-loss survivable");
    meta_free(&sm);
}

/* 5. E13 endangered-first ordering, byte tie-break, and margin-0 preemption (catches orderfifo). */
static void test_order_and_preemption(void)
{
    struct meta_state sm;
    struct place_member pm[8];
    init_cluster_state(&sm, 8, NULL, pm, 2);
    /* Vol 1: margin 1 (node 1 OUT, 5 UP). Vol 2: margin 0 (node 1 OUT, node 2 DOWN -> 4 UP).
     * Vol 3: margin 1, larger bytes than Vol 4 (also margin 1). */
    uint32_t reps[3] = {5, 6, 7};
    uint32_t s1[6] = {1, 3, 4, 5, 6, 7}; /* node 2 NOT in s1 -> margin 1 */
    uint32_t s2[6] = {1, 2, 4, 5, 6, 7}; /* node 1 OUT + node 2 DOWN -> margin 0! */
    uint32_t s3[6] = {1, 3, 4, 5, 6, 7}; /* margin 1, bytes = 5000 */
    uint32_t s4[6] = {1, 3, 4, 5, 6, 7}; /* margin 1, bytes = 1000 */
    add_vol(&sm, 1, "v1", reps, 3, 4, 2, s1);
    add_vol(&sm, 2, "v2", reps, 3, 4, 2, s2);
    add_vol(&sm, 3, "v3", reps, 3, 4, 2, s3);
    add_vol(&sm, 4, "v4", reps, 3, 4, 2, s4);

    set_member_state(&sm, 1, META_OUT);
    set_member_state(&sm, 2, META_DOWN);
    pm[0].heard_age_ms = pm[1].heard_age_ms = UINT64_MAX;

    struct place_vol_size sizes[4] = {{1, 2000}, {2, 100}, {3, 5000}, {4, 1000}};
    struct place_in in = make_in(&sm, pm, 8);
    in.vol_sizes = sizes;
    in.nvol_sizes = 4;

    /* With rebuild_concurrency = 1, only node 8 is spare -> ONLY vol 2 (margin 0) must be scheduled! */
    in.set.rebuild_concurrency = 1;
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET && out.props[0].volume == 2 &&
              out.props[0].margin == 0,
          "concurrency=1 must pick margin-0 vol 2 first (got n=%u vol=%u margin=%d)", out.nprops,
          out.nprops ? out.props[0].volume : 0, out.nprops ? out.props[0].margin : 99);
    place_out_free(&out);

    /* With rebuild_concurrency = 4, order must be: vol 2 (margin 0), vol 3 (m=1, 5000B), vol 1 (m=1, 2000B), vol 4 (m=1, 1000B). */
    in.set.rebuild_concurrency = 4;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 4, "concurrency=4 want 4 STRIPE_SETs, got %u", out.nprops);
    if (out.nprops == 4) {
        CHECK(out.props[0].volume == 2 && out.props[1].volume == 3 && out.props[2].volume == 1 &&
                  out.props[3].volume == 4,
              "E13 order wrong: got vols %u,%u,%u,%u want 2,3,1,4", out.props[0].volume, out.props[1].volume,
              out.props[2].volume, out.props[3].volume);
    }
    place_out_free(&out);

    /* Preemption: vol 1 is already FILLING on node 8 (margin 1), rebuild_concurrency = 1:
     * vol 2 (margin 0) MUST still be scheduled onto node 8 (preempting margin-1 cap), while vol 3 (margin 1) waits! */
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_stripe_set(&e, 1, 0, 8, META_STRIPE_FILLING, 1, 1);
    int res[2] = {0};
    CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "seed filling on v1: %d", res[0]);

    in.set.rebuild_concurrency = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].volume == 2 && out.props[0].node == 8,
          "margin-0 preemption: want only vol 2 -> node 8, got n=%u vol=%u node=%u", out.nprops,
          out.nprops ? out.props[0].volume : 0, out.nprops ? out.props[0].node : 0);
    place_out_free(&out);
    meta_free(&sm);
}

/* 6. STRIPE_WHOLE version barrier (D10; catches wholenobarrier). */
static void test_stripe_whole_barrier(void)
{
    struct meta_state sm;
    struct place_member pm[7];
    init_cluster_state(&sm, 7, NULL, pm, 2);
    uint32_t reps[3] = {2, 3, 4};
    uint32_t stripe[6] = {1, 2, 3, 4, 5, 6};
    add_vol(&sm, 1, "ecw", reps, 3, 4, 2, stripe);
    /* Move pos 0 to node 7 (FILLING, stripe_version becomes 2). */
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_stripe_set(&e, 1, 0, 7, META_STRIPE_FILLING, 1, 1);
    int res[2] = {0};
    CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "seed filling: %d", res[0]);

    struct place_in in = make_in(&sm, pm, 7);
    struct place_out out;

    /* Case A: fill in progress (whole == 0) at current version -> MUST emit 0 STRIPE_WHOLE. */
    struct place_fill f_prog = {.node = 7, .volume = 1, .pos = 0, .stripe_version = 2, .done = 5, .total = 10, .whole = 0};
    in.fills = &f_prog;
    in.nfills = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "whole=0 must not emit STRIPE_WHOLE (got %u props)", out.nprops);
    place_out_free(&out);

    /* Case B: whole == 1 at STALE stripe_version (1 != 2) -> MUST emit 0 STRIPE_WHOLE. */
    struct place_fill f_stale = {.node = 7, .volume = 1, .pos = 0, .stripe_version = 1, .done = 10, .total = 10, .whole = 1};
    in.fills = &f_stale;
    in.nfills = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "stale version whole=1 must not emit STRIPE_WHOLE (got %u props)", out.nprops);
    place_out_free(&out);

    /* Case C: whole == 1 at CURRENT stripe_version (2) -> MUST emit STRIPE_WHOLE and apply cleanly! */
    struct place_fill f_ok = {.node = 7, .volume = 1, .pos = 0, .stripe_version = 2, .done = 10, .total = 10, .whole = 1};
    in.fills = &f_ok;
    in.nfills = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_WHOLE && out.props[0].volume == 1 &&
              out.props[0].pos == 0 && out.props[0].version == 2,
          "current version whole=1 must emit STRIPE_WHOLE (got %u props)", out.nprops);
    if (out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_WHOLE) {
        meta_entry_init(&e);
        meta_op_stripe_whole(&e, out.props[0].volume, out.props[0].pos, out.props[0].version);
        CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "apply STRIPE_WHOLE: %d", res[0]);
        CHECK(meta_volume(&sm, 1)->stripe_pos[0].state == META_STRIPE_WHOLE, "pos 0 not WHOLE after apply");
    }
    place_out_free(&out);
    meta_free(&sm);
}

/* 7. DRAINING, one-move-per-vol cap, rebuild_max per server, fault-replace (D6), and node replace OLD NEW (D22). */
static void test_drain_caps_fault_replace(void)
{
    struct meta_state sm;
    struct place_member pm[6];
    init_cluster_state(&sm, 6, NULL, pm, 2);
    /* Set rebuild_max = 1 in settings so we can verify per-server send & receive caps tightly. */
    struct meta_settings s = sm.set;
    s.rebuild_max = 1;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_settings(&e, &s);
    int res[2] = {0};
    CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "set rebuild_max=1: %d", res[0]);

    /* 3 volumes all led by node 1, all holding node 3. */
    uint32_t r1[3] = {1, 2, 3};
    uint32_t r2[3] = {1, 2, 3};
    uint32_t r3[3] = {1, 2, 3};
    add_vol(&sm, 1, "m1", r1, 3, 0, 0, NULL);
    add_vol(&sm, 2, "m2", r2, 3, 0, 0, NULL);
    add_vol(&sm, 3, "m3", r3, 3, 0, 0, NULL);

    /* Shrink vol 2's ISR to {1} (young margin = 1 - 1 = 0); vol 1 and vol 3 stay at ISR 3 (young margin = 2). */
    uint32_t isr1[1] = {1};
    set_isr(&sm, 2, isr1, 1);

    /* Mark node 3 DRAINING -> sender node 1 has rebuild_max = 1, so ONLY vol 2 (margin 0) should start! */
    set_member_state(&sm, 3, META_DRAINING);

    struct place_in in = make_in(&sm, pm, 6);
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);
    int nadd = 0;
    for (uint32_t i = 0; i < out.nprops; i++) {
        CHECK(out.props[i].kind != PLACE_PROP_REPLICA_REMOVE, "DRAINING move must never emit REPLICA_REMOVE");
        if (out.props[i].kind == PLACE_PROP_REPLICA_ADD) {
            nadd++;
            CHECK(out.props[i].volume == 2 && out.props[i].other_node == 3 && out.props[i].node >= 4,
                  "rebuild_max=1 sender cap must pick margin-0 vol 2 first: got vol %u %u->%u", out.props[i].volume,
                  out.props[i].other_node, out.props[i].node);
        }
    }
    CHECK(nadd == 1, "want 1 move under rebuild_max=1 sender cap, got %d", nadd);
    place_out_free(&out);

    /* One-move-per-volume: add a learner to vol 2 -> vol 2 must NOT get a second move! */
    meta_entry_init(&e);
    meta_op_replica_add(&e, 2, 1, 4, 0x9999, 3);
    CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "add learner to vol 2: %d", res[0]);
    /* Sender node 1 now has 1 in-flight move (margin 0) -> vol 1 and vol 3 (margin 2) must wait! */
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "sender at rebuild_max=1 must block margin-2 vol 1/3 and vol 2 learner blocks 2nd move (got %u)",
          out.nprops);
    place_out_free(&out);
    meta_free(&sm);

    /* Fault-replace (D6): both direct in->members[].faults and unresolved pout PLACE_PROP_REPLICA_ADD(node=0). */
    init_cluster_state(&sm, 5, NULL, pm, 2);
    uint32_t rf[3] = {1, 2, 3};
    add_vol(&sm, 1, "vf", rf, 3, 0, 0, NULL);
    /* Short fault (< failstop_out_s=60): no move. */
    pm[2].nfaults = 1;
    pm[2].faults[0] = (struct place_vol_fault){.volume = 1, .fault_s = 30};
    in = make_in(&sm, pm, 5);
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "fault_s=30 < 60 must not move (got %u)", out.nprops);
    place_out_free(&out);

    /* Persistent fault (>= 60): moves node 3 to 4 or 5. */
    pm[2].faults[0].fault_s = 60;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_REPLICA_ADD && out.props[0].other_node == 3 &&
              (out.props[0].node == 4 || out.props[0].node == 5),
          "fault_s=60 must replace node 3 -> 4/5 (got n=%u kind=%d %u->%u)", out.nprops,
          out.nprops ? (int)out.props[0].kind : -1, out.nprops ? out.props[0].other_node : 0,
          out.nprops ? out.props[0].node : 0);
    place_out_free(&out);

    /* Unresolved pout fault request (node=0, other_node=2) in out -> resolved to target != 0! */
    pm[2].nfaults = 0;
    place_out_init(&out);
    struct place_prop pout_req = {.kind = PLACE_PROP_REPLICA_ADD, .volume = 1, .node = 0, .other_node = 2};
    place_out_push(&out, &pout_req);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_REPLICA_ADD && out.props[0].other_node == 2 &&
              (out.props[0].node == 4 || out.props[0].node == 5),
          "pout unresolved fault request must be resolved to target 4/5 (got n=%u node=%u)", out.nprops,
          out.nprops ? out.props[0].node : 0);
    place_out_free(&out);
    meta_free(&sm);

    /* node replace OLD NEW (D22): OLD's log and stripe positions go specifically to NEW (node 8), not lower-id node 7. */
    struct place_member pm8[8];
    init_cluster_state(&sm, 8, NULL, pm8, 2);
    uint32_t rrep[3] = {1, 2, 3};
    uint32_t srep[6] = {1, 2, 3, 4, 5, 6};
    add_vol(&sm, 1, "vr", rrep, 3, 4, 2, srep);
    struct place_replace rep_intent = {.old_node = 1, .new_node = 8};
    in = make_in(&sm, pm8, 8);
    in.replaces = &rep_intent;
    in.nreplaces = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    int r_log = 0, r_str = 0;
    for (uint32_t i = 0; i < out.nprops; i++) {
        if (out.props[i].kind == PLACE_PROP_REPLICA_ADD) {
            r_log++;
            CHECK(out.props[i].other_node == 1 && out.props[i].node == 8,
                  "node replace log: want 1->8, got %u->%u", out.props[i].other_node, out.props[i].node);
        } else if (out.props[i].kind == PLACE_PROP_STRIPE_SET) {
            r_str++;
            CHECK(out.props[i].other_node == 1 && out.props[i].node == 8 && out.props[i].pos == 0,
                  "node replace stripe: want pos 0 1->8, got pos %u %u->%u", out.props[i].pos,
                  out.props[i].other_node, out.props[i].node);
        }
    }
    CHECK(r_log == 1 && r_str == 1, "node replace want 1 log + 1 stripe prop to NEW=8, got %d, %d", r_log, r_str);
    place_out_free(&out);
    meta_free(&sm);
}

/* 8. Reachable sender gate: skip log moves when no ISR member is reachable; allow live OUT sole-ISR leader. */
static void test_sender_reachability(void)
{
    struct meta_state sm;
    struct place_member pm[5];
    init_cluster_state(&sm, 5, NULL, pm, 2);
    uint32_t rdead[3] = {1, 2, 3};
    add_vol(&sm, 1, "vdead", rdead, 3, 0, 0, NULL);
    set_member_state(&sm, 1, META_OUT);
    set_member_state(&sm, 2, META_DOWN);
    set_member_state(&sm, 3, META_DOWN);
    pm[0].heard_age_ms = pm[1].heard_age_ms = pm[2].heard_age_ms = UINT64_MAX;
    struct place_in in = make_in(&sm, pm, 5);
    in.self_id = 4; /* Raft leader is node 4, outside the dead ISR */
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "no reachable ISR sender must emit 0 log moves (got %u)", out.nprops);
    place_out_free(&out);

    /* Live OUT sole ISR member (replicas=1 marked OUT while still heard): log_move_sender uses the live OUT leader
     * so cmove can replicate to a new node and perform a planned transfer with zero outage. */
    uint32_t rsolo[1] = {4};
    add_vol(&sm, 2, "vsolo", rsolo, 1, 0, 0, NULL);
    set_member_state(&sm, 4, META_OUT);
    pm[3].heard_age_ms = 50; /* node 4 is still alive and heard */
    in.self_id = 5;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_REPLICA_ADD && out.props[0].volume == 2 &&
              out.props[0].other_node == 4 && out.props[0].node == 5,
          "live OUT sole-ISR leader must still move to node 5 (got n=%u vol=%u %u->%u)", out.nprops,
          out.nprops ? out.props[0].volume : 0, out.nprops ? out.props[0].other_node : 0,
          out.nprops ? out.props[0].node : 0);
    place_out_free(&out);
    meta_free(&sm);
}

int main(int argc, char **argv)
{
    if (argc == 3 && strcmp(argv[1], "--mutant") == 0) {
        g_mut = place_mutant_parse(argv[2]);
        if (g_mut <= 0) {
            fprintf(stderr, "test_place: unknown mutant %s\n", argv[2]);
            return 2;
        }
        printf("test_place: mutant %s\n", argv[2]);
    } else if (argc != 1) {
        fprintf(stderr, "usage: test_place [--mutant NAME]\n");
        return 2;
    }

    CHECK(place_mutant_parse(NULL) == PLACE_MUT_NONE && place_mutant_parse("") == PLACE_MUT_NONE &&
              place_mutant_parse("nosuch") == -1,
          "place_mutant_parse edge cases");

    test_initial_placement_and_racks();
    test_out_one_ec42();
    test_rack_cap_replacement();
    test_out_two_and_per_rack();
    test_order_and_preemption();
    test_stripe_whole_barrier();
    test_drain_caps_fault_replace();
    test_sender_reachability();

    if (fails) {
        fprintf(stderr, "test_place: %d checks failed\n", fails);
        return 1;
    }
    printf("test_place: ok\n");
    return 0;
}
