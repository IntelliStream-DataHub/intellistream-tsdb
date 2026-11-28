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
    else {
        uint64_t inc = (mb->state == META_OUT && state == META_UP) ? mb->incarnation + 1 : mb->incarnation;
        meta_op_member_state(&e, id, state, inc, META_DOWN_NONE, 0, 0);
    }
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
    meta_op_settings(&e, &s, sm.settings_version);
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

/* 9. R13 (r13-place; E49, E50, E51, E53, §15.3, §15.5): placement by capability, role, disk class and fill marks.
 * Catches planted mutants: loghdd, mixstripe, placefull. */
static struct meta_disk mk_disk(uint8_t tag, uint8_t cls, uint8_t uses, uint64_t size_bytes)
{
    struct meta_disk d = {0};
    d.store_id[0] = 0xd0;
    d.store_id[1] = tag;
    d.store_id[15] = (uint8_t)(tag ^ 0x5a);
    d.disk_class = cls;
    d.uses = uses;
    d.state = META_STORE_UP;
    d.size_bytes = size_bytes;
    d.sync_us = cls == META_DISK_HDD ? 5000 : 100;
    return d;
}

static void add_disk(struct meta_state *sm, uint32_t node, const struct meta_disk *d)
{
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_store_add(&e, node, d);
    int res[2] = {0};
    int rc = apply_entry(sm, &e, res, 2);
    CHECK(rc == META_OK && res[0] == META_OK, "store_add node %u: rc %d res %d", node, rc, res[0]);
}

static void set_role(struct meta_state *sm, uint32_t node, uint8_t role)
{
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_member_role(&e, node, role);
    int res[2] = {0};
    int rc = apply_entry(sm, &e, res, 2);
    CHECK(rc == META_OK && res[0] == META_OK, "member_role node %u -> %u: rc %d res %d", node, role, rc, res[0]);
}

static void test_r13_capability_class_and_fill(void)
{
    /* (a) 5 nodes: nodes 1,2 HDD-only + role=storage; nodes 3,4,5 NVMe(LOG)+HDD(CHUNK).
     *     replicas=3 MUST land only on {3,4,5} (catches loghdd). */
    struct meta_state sm;
    struct place_member pm[6];
    init_cluster_state(&sm, 5, NULL, pm, 1);

    uint64_t sz = 1000ull * 1024 * 1024 * 1024; /* 1000 GiB */
    struct meta_disk h1 = mk_disk(1, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk h2 = mk_disk(2, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk n3 = mk_disk(3, META_DISK_NVME, META_DISK_USE_LOG, sz);
    struct meta_disk h3 = mk_disk(13, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk n4 = mk_disk(4, META_DISK_NVME, META_DISK_USE_LOG, sz);
    struct meta_disk h4 = mk_disk(14, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk n5 = mk_disk(5, META_DISK_NVME, META_DISK_USE_LOG, sz);
    struct meta_disk h5 = mk_disk(15, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    add_disk(&sm, 1, &h1);
    add_disk(&sm, 2, &h2);
    set_role(&sm, 1, META_ROLE_STORAGE);
    set_role(&sm, 2, META_ROLE_STORAGE);
    add_disk(&sm, 3, &n3);
    add_disk(&sm, 3, &h3);
    add_disk(&sm, 4, &n4);
    add_disk(&sm, 4, &h4);
    add_disk(&sm, 5, &n5);
    add_disk(&sm, 5, &h5);

    struct place_in in = make_in(&sm, pm, 5);
    uint32_t reps[3] = {0};
    CHECK(place_volume(&in, 3, reps) == 0, "place_volume(3) on 3 compute + 2 storage failed");
    for (int i = 0; i < 3; i++)
        CHECK(reps[i] >= 3 && reps[i] <= 5, "log replica landed on storage-only node %u (want 3..5)", reps[i]);

    /* (b) Young use tie-breaking & young_move_pct (80%) avoidance (catches placefull). */
    struct place_disk_usage du3[2] = {
        {.disk_class = META_DISK_NVME, .uses = META_DISK_USE_LOG, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .young_bytes = sz * 85 / 100}, /* 85% >= 80%: full! */
        {.disk_class = META_DISK_HDD, .uses = META_DISK_USE_CHUNK, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .chunk_bytes = sz * 10 / 100},
    };
    memcpy(du3[0].store_id, n3.store_id, 16);
    memcpy(du3[1].store_id, h3.store_id, 16);
    struct place_disk_usage du4[2] = {
        {.disk_class = META_DISK_NVME, .uses = META_DISK_USE_LOG, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .young_bytes = sz * 50 / 100}, /* 50% < 80% */
        {.disk_class = META_DISK_HDD, .uses = META_DISK_USE_CHUNK, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .chunk_bytes = sz * 10 / 100},
    };
    memcpy(du4[0].store_id, n4.store_id, 16);
    memcpy(du4[1].store_id, h4.store_id, 16);
    struct place_disk_usage du5[2] = {
        {.disk_class = META_DISK_NVME, .uses = META_DISK_USE_LOG, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .young_bytes = sz * 20 / 100}, /* 20% < 80% (lowest!) */
        {.disk_class = META_DISK_HDD, .uses = META_DISK_USE_CHUNK, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .chunk_bytes = sz * 10 / 100},
    };
    memcpy(du5[0].store_id, n5.store_id, 16);
    memcpy(du5[1].store_id, h5.store_id, 16);
    pm[2].disks = du3;
    pm[2].ndisks = 2;
    pm[3].disks = du4;
    pm[3].ndisks = 2;
    pm[4].disks = du5;
    pm[4].ndisks = 2;

    /* With node 3 at 85% >= 80%, replicas=3 cannot fit (only nodes 4 and 5 eligible), replicas=2 picks {5, 4} (lowest young use first!). */
    CHECK(place_volume(&in, 3, reps) == -1, "place_volume(3) must fail when 1 of 3 compute nodes is at 85%% >= 80%%");
    uint32_t reps2[2] = {0};
    CHECK(place_volume(&in, 2, reps2) == 0, "place_volume(2) must succeed on nodes 4 and 5");
    CHECK(reps2[0] == 5 && reps2[1] == 4,
          "place_volume(2) must avoid 85%% node 3 and prefer lower young-use node 5 before 4 (got %u,%u)", reps2[0],
          reps2[1]);

    /* Adding a second NVMe below 80% on node 3 makes node 3 eligible again (not ALL fast disks >= 80%). */
    struct meta_disk n3b = mk_disk(33, META_DISK_NVME, META_DISK_USE_LOG, sz);
    add_disk(&sm, 3, &n3b);
    struct place_disk_usage du3_two[3] = {du3[0], du3[1],
        {.disk_class = META_DISK_NVME, .uses = META_DISK_USE_LOG, .state = META_STORE_UP, .have_usage = 1,
         .size_bytes = sz, .young_bytes = sz * 15 / 100}};
    memcpy(du3_two[2].store_id, n3b.store_id, 16);
    pm[2].disks = du3_two;
    pm[2].ndisks = 3;
    CHECK(place_volume(&in, 3, reps) == 0, "node 3 with a second NVMe at 15%% must be eligible again");

    /* (c) log_on_hdd = 1: fast nodes still preferred when enough exist; HDD-only used only as fallback. */
    struct meta_settings st = sm.set;
    st.log_on_hdd = 1;
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_settings(&e, &st, sm.settings_version);
    int res[2] = {0};
    CHECK(apply_entry(&sm, &e, res, 2) == META_OK && res[0] == META_OK, "enable log_on_hdd");
    set_role(&sm, 1, META_ROLE_BOTH); /* node 1 HDD-only now has META_CAP_COMPUTE via log_on_hdd */
    /* Even with node 1 having lower id and 0 holds, place_volume(3) MUST still pick fast nodes {3,4,5}. */
    CHECK(place_volume(&in, 3, reps) == 0, "place_volume(3) with log_on_hdd=1 failed");
    for (int i = 0; i < 3; i++)
        CHECK(reps[i] >= 3 && reps[i] <= 5, "log_on_hdd=1 picked HDD node %u while 3 fast nodes exist", reps[i]);
    /* Requesting 4 replicas exceeds the 3 fast nodes -> falls back to include node 1 (not storage-only node 2). */
    uint32_t reps4[4] = {0};
    CHECK(place_volume(&in, 4, reps4) == 0, "place_volume(4) with log_on_hdd=1 fallback failed");
    int saw1 = 0, saw2 = 0;
    for (int i = 0; i < 4; i++) {
        saw1 += (reps4[i] == 1);
        saw2 += (reps4[i] == 2);
    }
    CHECK(saw1 == 1 && saw2 == 0, "log_on_hdd=1 fallback: want node 1 (role=both) and not node 2 (role=storage)");
    meta_free(&sm);

    /* (d) Stripe class preference (catches mixstripe), mixed-class fallback + helper, and chunk fill mark (catches placefull). */
    init_cluster_state(&sm, 5, NULL, pm, 1);
    struct meta_disk hd1 = mk_disk(21, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk hd2 = mk_disk(22, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk hd3 = mk_disk(23, META_DISK_HDD, META_DISK_USE_CHUNK, sz);
    struct meta_disk nv4 = mk_disk(24, META_DISK_NVME, META_DISK_USE_ALL, sz);
    struct meta_disk nv5 = mk_disk(25, META_DISK_NVME, META_DISK_USE_ALL, sz);
    add_disk(&sm, 1, &hd1);
    add_disk(&sm, 2, &hd2);
    add_disk(&sm, 3, &hd3);
    add_disk(&sm, 4, &nv4);
    add_disk(&sm, 5, &nv5);
    /* Give nodes 1,2,3 (HDD) 1 existing stripe hold each so NVMe nodes 4,5 have FEWER holds (0 < 1). */
    uint32_t init_rep[2] = {4, 5};
    uint32_t init_str[3] = {1, 2, 3};
    add_vol(&sm, 1, "ec_hdd1", init_rep, 2, 2, 1, init_str);
    CHECK(place_stripe_mixed_class(&sm, meta_volume(&sm, 1)) == 0, "single-class HDD stripe must report mixed=0");

    in = make_in(&sm, pm, 5);
    uint32_t str3[3] = {0}, qual = 0;
    CHECK(place_stripe(&in, 3, str3, &qual) == 0 && qual == 5, "place_stripe(3) failed (qual %u)", qual);
    for (int i = 0; i < 3; i++)
        CHECK(place_member_chunk_class(meta_member(&sm, str3[i])) == META_DISK_HDD,
              "place_stripe(3) mixed classes (picked node %u class %s) despite 3 HDD members", str3[i],
              meta_disk_class_name(place_member_chunk_class(meta_member(&sm, str3[i]))));

    /* Asking for 4 stripe positions (3+1): neither HDD (3) nor NVMe (2) has 4 -> mixed fallback allowed, flagged mixed=1. */
    uint32_t str4[4] = {0};
    CHECK(place_stripe(&in, 4, str4, &qual) == 0, "place_stripe(4) mixed fallback failed");
    add_vol(&sm, 2, "ec_mixed", init_rep, 2, 3, 1, str4);
    CHECK(place_stripe_mixed_class(&sm, meta_volume(&sm, 2)) == 1, "mixed HDD+NVMe stripe must report mixed=1");

    /* Chunk store fill mark (§5.6): 5 equal storage nodes -> mark = 95% * (4/5) = 76%.
     * Filling node 1's HDD to 75% (< 76%) keeps it eligible; 77% (>= 76%) excludes it! */
    struct place_disk_usage ch1 = {.disk_class = META_DISK_HDD, .uses = META_DISK_USE_CHUNK, .state = META_STORE_UP,
                                   .have_usage = 1, .size_bytes = sz, .chunk_bytes = sz * 75 / 100};
    memcpy(ch1.store_id, hd1.store_id, 16);
    pm[0].disks = &ch1;
    pm[0].ndisks = 1;
    uint32_t str5[5] = {0};
    CHECK(place_stripe(&in, 5, str5, &qual) == 0 && qual == 5,
          "place_stripe(5) must accept node 1 at 75%% < 76%% mark (qual=%u)", qual);
    ch1.chunk_bytes = sz * 77 / 100;
    CHECK(place_stripe(&in, 5, str5, &qual) == -1 && qual == 4,
          "place_stripe(5) must reject full chunk node 1 at 77%% >= 76%% mark (qual=%u, want 4)", qual);
    CHECK(place_stripe(&in, 4, str4, &qual) == 0, "place_stripe(4) must succeed on remaining 4 nodes");
    for (int i = 0; i < 4; i++)
        CHECK(str4[i] != 1, "place_stripe(4) placed on full chunk node 1");

    /* Shared disk (LOG|CHUNK) young mark uses (size - chunk_bytes): node 4 has 50% chunks + 42% young -> 42/50 = 84% >= 80%! */
    struct place_disk_usage sh4 = {.disk_class = META_DISK_NVME, .uses = META_DISK_USE_ALL, .state = META_STORE_UP,
                                   .have_usage = 1, .size_bytes = sz, .young_bytes = sz * 42 / 100,
                                   .chunk_bytes = sz * 50 / 100};
    memcpy(sh4.store_id, nv4.store_id, 16);
    pm[3].disks = &sh4;
    pm[3].ndisks = 1;
    CHECK(place_member_log_ok(&in, meta_member(&sm, 4), 0, NULL) == 0,
          "shared disk with 50%% chunks and 42%% young (84%% of non-chunk room >= 80%%) must be rejected for log");
    meta_free(&sm);
}

/* 10. Returning server (r9-return; E28, §5.4, D13): take back FILLING stripe positions with intact stores, leave WHOLE
 * positions alone (mutant `takebackwhole`), cancel unjoined log moves when returning replica can catch up from retained
 * segments, ignore operator moves / already-joined learners / changed store ids. */
static void test_r9_return(void)
{
    struct meta_state sm;
    struct place_member pm[8];
    init_cluster_state(&sm, 7, NULL, pm, 2);
    uint64_t sz = 1000ull << 30;
    struct meta_disk dks[7];
    struct place_disk_usage du[7];
    for (uint32_t i = 0; i < 7; i++) {
        dks[i] = mk_disk((uint8_t)(40 + i), META_DISK_NVME, META_DISK_USE_ALL, sz);
        add_disk(&sm, i + 1, &dks[i]);
        du[i] = (struct place_disk_usage){.disk_class = META_DISK_NVME, .uses = META_DISK_USE_ALL,
                                          .state = META_STORE_UP, .have_usage = 1, .size_bytes = sz};
        memcpy(du[i].store_id, dks[i].store_id, 16);
        pm[i].disks = &du[i];
        pm[i].ndisks = 1;
    }

    uint32_t reps[3] = {1, 2, 3};
    uint32_t stripe[6] = {1, 2, 3, 4, 5, 6};
    add_vol(&sm, 1, "ec_ret", reps, 3, 4, 2, stripe);

    /* (a) Node 6 goes OUT and is replaced at pos 5 by spare node 7 (FILLING). */
    set_member_state(&sm, 6, META_DOWN);
    set_member_state(&sm, 6, META_OUT);
    const struct meta_volume *v = meta_volume(&sm, 1);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_stripe_set(&e, 1, 5, 7, META_STRIPE_FILLING, 6, v->stripe_version);
    int res[4] = {0};
    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "replace node 6 with 7");
    v = meta_volume(&sm, 1);
    uint64_t set_ver = v->stripe_pos[5].set_ver;

    /* Node 6 returns UP with its store_id intact -> place_return emits STRIPE_SET(pos=5, node=6, prev=7, is_return=1). */
    set_member_state(&sm, 6, META_UP);
    struct place_return_cand cand_ec = {.volume = 1, .pos = 5, .prev_node = 6, .version = set_ver, .is_log = 0};
    struct place_in in = make_in(&sm, pm, 7);
    in.return_cands = &cand_ec;
    in.nreturn_cands = 1;
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET && out.props[0].node == 6 &&
              out.props[0].other_node == 7 && out.props[0].pos == 5 && out.props[0].reason == META_STRIPE_FILLING &&
              out.props[0].is_return == 1,
          "return during fill: expected 1 return STRIPE_SET to node 6 (got %u props)", out.nprops);
    place_out_free(&out);

    /* (b) Changed store_id on node 6 -> NOT intact -> must NOT take back! */
    du[5].store_id[0] ^= 0xff;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "return with changed store_id must emit 0 props (got %u)", out.nprops);
    place_out_free(&out);
    du[5].store_id[0] ^= 0xff;

    /* (c) Operator posmove (not in return_cands) -> must NOT take back (also prevents ping-pong). */
    in.return_cands = NULL;
    in.nreturn_cands = 0;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "non-OUT move (empty return_cands) must emit 0 props (got %u)", out.nprops);
    place_out_free(&out);

    /* (c2) Uncommitted STRIPE_WHOLE window: if B already reported whole == 1 in fills (even at an older version),
     *      place_return MUST NOT propose a take-back. */
    v = meta_volume(&sm, 1);
    in.return_cands = &cand_ec;
    in.nreturn_cands = 1;
    struct place_fill wf = {.node = 7, .volume = 1, .pos = 5, .stripe_version = v->stripe_version - 1, .whole = 1};
    in.fills = &wf;
    in.nfills = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "return while B already reported whole must emit 0 props (got %u)", out.nprops);
    place_out_free(&out);
    in.fills = NULL;
    in.nfills = 0;

    /* (d) Node 7 finishes filling -> STRIPE_WHOLE commits. Returning node 6 must NOT take back a WHOLE position!
     *     (Kills mutant `takebackwhole`.) */
    v = meta_volume(&sm, 1);
    meta_entry_init(&e);
    meta_op_stripe_whole(&e, 1, 5, v->stripe_version);
    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "commit STRIPE_WHOLE on node 7");
    in.return_cands = &cand_ec;
    in.nreturn_cands = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "return after WHOLE must emit 0 props (got %u, mutant=%d)", out.nprops, g_mut);
    place_out_free(&out);

    /* (e) Log replica move cancellation: node 3 went OUT, learner 4 replaces 3, node 3 returns UP before 4 joins ISR. */
    set_member_state(&sm, 3, META_DOWN);
    set_member_state(&sm, 3, META_OUT);
    uint32_t isr12[2] = {1, 2};
    set_isr(&sm, 1, isr12, 2);
    v = meta_volume(&sm, 1);
    meta_entry_init(&e);
    meta_op_replica_add(&e, 1, v->epoch, 4, 7777, 3);
    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "add learner 4 replacing 3");
    set_member_state(&sm, 3, META_UP);

    v = meta_volume(&sm, 1);
    struct place_return_cand cand_log = {.volume = 1, .pos = 0, .prev_node = 3, .version = 7777, .is_log = 1};
    struct place_vol_log vlogs[2] = {
        {.node = v->leader, .volume = 1, .epoch = v->epoch, .log_start = 100, .log_end = 500, .is_leader = 1, .healthy = 1},
        {.node = 3, .volume = 1, .rid = meta_replica(v, 3)->rid, .epoch = v->epoch, .log_start = 1, .log_end = 250, .is_leader = 0, .healthy = 1},
    };
    in = make_in(&sm, pm, 7);
    in.return_cands = &cand_log;
    in.nreturn_cands = 1;
    in.vol_logs = vlogs;
    in.nvol_logs = 2;

    /* Retained segments cover node 3 (250 >= 100) -> cancel move! */
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 1 && out.props[0].kind == PLACE_PROP_MOVE_CANCEL && out.props[0].volume == 1 &&
              out.props[0].node == 4 && out.props[0].other_node == 3 && out.props[0].is_return == 1,
          "log return with retained segments: expected MOVE_CANCEL of learner 4 (got %u props)", out.nprops);
    place_out_free(&out);

    /* Trimmed segments (node 3 log_end=50 < leader log_start=100) -> cannot catch up -> do NOT cancel! */
    vlogs[1].log_end = 50;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return with trimmed segments must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);

    /* Additional log-cancel safety guards: volume fault on A, leader epoch mismatch, unhealthy leader/A, rid mismatch. */
    vlogs[1].log_end = 250;
    pm[2].nfaults = 1;
    pm[2].faults[0] = (struct place_vol_fault){.volume = 1, .fault_s = 1};
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return with volume fault on A must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);
    pm[2].nfaults = 0;

    vlogs[0].epoch = v->epoch + 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return with leader epoch mismatch must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);
    vlogs[0].epoch = v->epoch;

    vlogs[0].healthy = 0;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return with unhealthy leader must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);
    vlogs[0].healthy = 1;

    vlogs[1].healthy = 0;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return with unhealthy A must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);
    vlogs[1].healthy = 1;

    vlogs[1].rid = meta_replica(v, 3)->rid + 1;
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return with rid mismatch must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);
    vlogs[1].rid = meta_replica(v, 3)->rid;

    /* Learner 4 already joined ISR -> do NOT cancel even if node 3 has log_end >= log_start! */
    uint32_t isr124[3] = {1, 2, 4};
    set_isr(&sm, 1, isr124, 3);
    place_out_init(&out);
    place_plan(&in, &out);
    CHECK(out.nprops == 0, "log return after learner joined ISR must NOT cancel (got %u props)", out.nprops);
    place_out_free(&out);

    meta_free(&sm);

    /* (f) Rack survivability guard: taking A back must NOT turn a rack-survivable stripe into non-rack-survivable. */
    static const char *const racks7[7] = {"r1", "r1", "r2", "r2", "r3", "r1", "r3"};
    init_cluster_state(&sm, 7, racks7, pm, 2);
    /* 4+2 on 3 racks (r1, r2, r3): current stripe {1(r1), 2(r1), 3(r2), 4(r2), 5(r3), 7(r3)} has 2 per rack (<= m=2 ->
     * rack-survivable). Node 6 is in r1: replacing 7(r3) with 6(r1) would put 3 positions in r1 (> m=2 -> NOT rack-survivable!). */
    uint32_t reps_r[3] = {1, 3, 5};
    uint32_t str_r[6] = {1, 2, 3, 4, 5, 6};
    add_vol(&sm, 1, "ec_rackret", reps_r, 3, 4, 2, str_r);
    set_member_state(&sm, 6, META_DOWN);
    set_member_state(&sm, 6, META_OUT);
    v = meta_volume(&sm, 1);
    meta_entry_init(&e);
    meta_op_stripe_set(&e, 1, 5, 7, META_STRIPE_FILLING, 6, v->stripe_version);
    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "replace rack node 6 with 7");
    set_member_state(&sm, 6, META_UP);
    v = meta_volume(&sm, 1);
    struct place_return_cand cand_rk = {
        .volume = 1, .pos = 5, .prev_node = 6, .version = v->stripe_pos[5].set_ver, .is_log = 0};
    in = make_in(&sm, pm, 7);
    in.return_cands = &cand_rk;
    in.nreturn_cands = 1;
    place_out_init(&out);
    place_plan(&in, &out);
    if (g_mut != PLACE_MUT_RACK_IGNORED)
        CHECK(out.nprops == 0, "take-back breaking rack survivability must emit 0 props (got %u)", out.nprops);
    place_out_free(&out);
    meta_free(&sm);
}

/* 11. Pure greedy balancer (r9-balance; D19, §3.2, §6.2, E9, E51):
 *   (a) 5-node cluster with 18 replicas=3 volumes gains 6th node -> moves bounded by 5% concurrent,
 *       add-before-drop only (never REPLICA_REMOVE), converges to exact 9 per node & balanced leaders;
 *   (b) 25% automatic cap stops and logs (catches mutant `balanceunbounded`), `balance_apply = 1` overrides;
 *       `balance = 0` disables automatic moves while `balance_apply = 1` still runs;
 *   (c) E9 rack survivability never broken when adding a new server to an existing rack;
 *   (d) EC stripe balance: same-class preservation (E51), chunk-bytes-per-capacity balancing, fill-mark avoidance;
 *   (e) Never runs while any rebuild / drain / OUT / DOWN / fault / place_plan proposal is active. */
static void test_balance(void)
{
    struct meta_state sm;
    struct place_member pm[10];

    /* (a) 5 nodes -> add 6th node with 18 replicas=3 volumes (54 positions: 5% = 2 concurrent, 25% = 13 cap). */
    init_cluster_state(&sm, 5, NULL, pm, 1);
    for (uint32_t v = 1; v <= 18; v++) {
        struct place_in pin = make_in(&sm, pm, 5);
        uint32_t reps[3] = {0};
        CHECK(place_volume(&pin, 3, reps) == 0, "bal(a): place_volume %u", v);
        char vname[32];
        snprintf(vname, sizeof vname, "bv%u", v);
        add_vol(&sm, v, vname, reps, 3, 0, 0, NULL);
    }
    /* Add 6th UP node. */
    struct meta_entry e;
    meta_entry_init(&e);
    struct meta_member m6 = {.id = 6, .state = META_UP, .incarnation = 1, .flags = META_MF_ALL};
    snprintf(m6.addr, sizeof m6.addr, "198.51.100.6:9000");
    snprintf(m6.peer, sizeof m6.peer, "198.51.100.6:9001");
    meta_op_member_add(&e, &m6);
    int res[4] = {0};
    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(a): add node 6");
    memset(&pm[5], 0, sizeof pm[5]);
    pm[5].id = 6;
    pm[5].heard_age_ms = 50;
    pm[5].fill_pct = 20;
    pm[5].chunk_stores = 1;

    struct place_in in = make_in(&sm, pm, 6);
    struct balance_summary sum;
    balance_plan_summary(&in, &sum);
    CHECK(sum.total_pos == 54 && sum.max_plan == 13 && sum.max_conc == 2 && sum.log_moves == 9 && !sum.over_cap &&
              !sum.rebuild_busy,
          "bal(a): summary mismatch (tot=%u maxp=%u maxc=%u log=%u over=%u busy=%u)", sum.total_pos, sum.max_plan,
          sum.max_conc, sum.log_moves, sum.over_cap, sum.rebuild_busy);

    /* Drive balance_plan rounds to convergence, checking 5% concurrent cap and add-before-drop on every step. */
    uint32_t total_log_emitted = 0;
    for (int step = 0; step < 30; step++) {
        struct place_out out;
        place_out_init(&out);
        balance_plan(&in, &out);
        if (out.nprops == 0) {
            place_out_free(&out);
            break;
        }
        uint32_t step_data = 0;
        for (uint32_t i = 0; i < out.nprops; i++) {
            const struct place_prop *p = &out.props[i];
            CHECK(p->kind != PLACE_PROP_REPLICA_REMOVE, "bal(a): must never emit REPLICA_REMOVE");
            CHECK(p->is_balance == 1, "bal(a): prop must set is_balance=1");
            if (p->kind == PLACE_PROP_REPLICA_ADD) {
                step_data++;
                total_log_emitted++;
                const struct meta_volume *v = meta_volume(&sm, p->volume);
                CHECK(v != NULL && p->other_node != 0 && meta_replica(v, p->other_node) != NULL &&
                          meta_replica(v, p->node) == NULL,
                      "bal(a): invalid REPLICA_ADD vol=%u %u->%u", p->volume, p->other_node, p->node);
                /* Simulate completed add-before-drop move: add learner -> join ISR -> transfer if leader -> remove old. */
                meta_entry_init(&e);
                meta_op_replica_add(&e, v->number, v->epoch, p->node, 9000ull + (uint64_t)step * 10 + i, p->other_node);
                CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(a): apply REPLICA_ADD");
                v = meta_volume(&sm, p->volume);
                uint32_t new_isr[META_REPLICAS_MAX], ni = 0;
                for (uint32_t k = 0; k < v->nrep; k++)
                    if (v->rep[k].node != p->other_node)
                        new_isr[ni++] = v->rep[k].node;
                if (v->leader == p->other_node) {
                    /* Transfer leader to target before shrinking old out of ISR. */
                    uint32_t all_isr[META_REPLICAS_MAX], na = 0;
                    for (uint32_t k = 0; k < v->nrep; k++)
                        all_isr[na++] = v->rep[k].node;
                    set_isr(&sm, v->number, all_isr, na);
                    v = meta_volume(&sm, p->volume);
                    struct meta_grant gr = {.number = v->number, .epoch = v->epoch, .new_epoch = v->epoch + 1,
                                            .isr_version = v->isr_version, .reason = META_GR_PLANNED, .leader = p->node,
                                            .start = v->epoch + 1, .target = p->node, .min_end = 10,
                                            .nfenced = v->nisr, .nreplies = v->nisr};
                    for (uint32_t k = 0; k < v->nisr; k++) {
                        gr.fenced[k] = v->isr[k];
                        gr.reply[k] = (struct meta_fence_reply){.node = v->isr[k],
                                                                .rid = meta_replica(v, v->isr[k])->rid,
                                                                .durable = 10, .last_epoch = v->epoch};
                    }
                    meta_entry_init(&e);
                    meta_op_volume_leader(&e, &gr);
                    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(a): transfer leader");
                }
                set_isr(&sm, p->volume, new_isr, ni);
                v = meta_volume(&sm, p->volume);
                meta_entry_init(&e);
                meta_op_replica_remove(&e, v->number, v->epoch, p->other_node);
                CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(a): apply REPLICA_REMOVE");
            } else if (p->kind == PLACE_PROP_VOLUME_TRANSFER) {
                const struct meta_volume *v = meta_volume(&sm, p->volume);
                CHECK(v != NULL && meta_volume_in_isr(v, p->node) && v->leader != p->node,
                      "bal(a): invalid VOLUME_TRANSFER vol=%u -> %u", p->volume, p->node);
                struct meta_grant gr = {.number = v->number, .epoch = v->epoch, .new_epoch = v->epoch + 1,
                                        .isr_version = v->isr_version, .reason = META_GR_PLANNED, .leader = p->node,
                                        .start = v->epoch + 1, .target = p->node, .min_end = 10,
                                        .nfenced = v->nisr, .nreplies = v->nisr};
                for (uint32_t k = 0; k < v->nisr; k++) {
                    gr.fenced[k] = v->isr[k];
                    gr.reply[k] = (struct meta_fence_reply){.node = v->isr[k],
                                                            .rid = meta_replica(v, v->isr[k])->rid,
                                                            .durable = 10, .last_epoch = v->epoch};
                }
                meta_entry_init(&e);
                meta_op_volume_leader(&e, &gr);
                CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(a): apply leader transfer");
            }
        }
        CHECK(step_data <= sum.max_conc, "bal(a): step emitted %u moves > 5%% cap %u", step_data, sum.max_conc);
        place_out_free(&out);
    }
    CHECK(total_log_emitted == 9, "bal(a): expected 9 log moves to balance 5->6 (got %u)", total_log_emitted);
    uint32_t hcnt[7] = {0}, lcnt[7] = {0};
    for (uint32_t vi = 0; vi < sm.nvolumes; vi++) {
        const struct meta_volume *v = &sm.volumes[vi];
        lcnt[v->leader]++;
        for (uint32_t k = 0; k < v->nrep; k++)
            hcnt[v->rep[k].node]++;
    }
    for (uint32_t id = 1; id <= 6; id++) {
        CHECK(hcnt[id] == 9, "bal(a): node %u holds %u replicas (want 9)", id, hcnt[id]);
        CHECK(lcnt[id] >= 2 && lcnt[id] <= 4, "bal(a): node %u leads %u volumes (want 2..4)", id, lcnt[id]);
    }
    meta_free(&sm);

    /* (b) 25% automatic cap stops and logs (catches `balanceunbounded`), `--apply` overrides, `balance=off` stops auto. */
    init_cluster_state(&sm, 3, NULL, pm, 1);
    for (uint32_t v = 1; v <= 12; v++) {
        uint32_t reps[3] = {1, 2, 3};
        char vname[32];
        snprintf(vname, sizeof vname, "capv%u", v);
        add_vol(&sm, v, vname, reps, 3, 0, 0, NULL);
    }
    for (uint32_t nid = 4; nid <= 5; nid++) {
        meta_entry_init(&e);
        struct meta_member mn = {.id = nid, .state = META_UP, .incarnation = 1, .flags = META_MF_ALL};
        snprintf(mn.addr, sizeof mn.addr, "10.0.0.%u:9000", nid);
        snprintf(mn.peer, sizeof mn.peer, "10.0.0.%u:9001", nid);
        meta_op_member_add(&e, &mn);
        CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(b): add node %u", nid);
        memset(&pm[nid - 1], 0, sizeof pm[nid - 1]);
        pm[nid - 1].id = nid;
        pm[nid - 1].heard_age_ms = 50;
        pm[nid - 1].fill_pct = 20;
        pm[nid - 1].chunk_stores = 1;
    }
    in = make_in(&sm, pm, 5);
    balance_plan_summary(&in, &sum);
    CHECK(sum.total_pos == 36 && sum.max_plan == 9 && sum.log_moves == 14 && sum.over_cap == 1,
          "bal(b): expected 14/36 over 25%% cap (got %u/%u cap=%u over=%u)", sum.log_moves, sum.total_pos, sum.max_plan,
          sum.over_cap);

    struct place_out out;
    place_out_init(&out);
    balance_plan(&in, &out);
    CHECK(out.nprops == 0, "bal(b): automatic plan >25%% MUST emit 0 props (got %u, mutant=%d)", out.nprops, g_mut);
    CHECK(out.nlogs >= 1 && strstr(out.logs[0].msg, "25% automatic cap") != NULL,
          "bal(b): automatic plan >25%% must log 25%% cap message");
    place_out_free(&out);

    /* Operator --apply overrides 25% cap while still respecting 5% concurrent bound (36/20 = 1). */
    in.balance_apply = 1;
    place_out_init(&out);
    balance_plan(&in, &out);
    uint32_t apply_data = 0;
    for (uint32_t i = 0; i < out.nprops; i++)
        if (out.props[i].kind == PLACE_PROP_REPLICA_ADD || out.props[i].kind == PLACE_PROP_STRIPE_SET)
            apply_data++;
    CHECK(apply_data == 1 && out.props[0].kind == PLACE_PROP_REPLICA_ADD && out.props[0].node >= 4,
          "bal(b): balance_apply=1 must emit 1 data move (5%% cap) onto node 4/5 (got %u data / %u total)",
          apply_data, out.nprops);
    place_out_free(&out);
    in.balance_apply = 0;

    /* balance=off disables automatic moves even when <= 25%, while balance_apply=1 still works. */
    struct meta_settings bs = sm.set;
    bs.balance = 0;
    meta_entry_init(&e);
    meta_op_settings(&e, &bs, sm.settings_version);
    CHECK(apply_entry(&sm, &e, res, 4) == META_OK && res[0] == META_OK, "bal(b): set balance=off");
    in.balance_apply = 0;
    place_out_init(&out);
    balance_plan(&in, &out);
    CHECK(out.nprops == 0 && out.nlogs == 0, "bal(b): balance=off must emit 0 props and 0 logs");
    place_out_free(&out);
    in.balance_apply = 1;
    place_out_init(&out);
    balance_plan(&in, &out);
    apply_data = 0;
    for (uint32_t i = 0; i < out.nprops; i++)
        if (out.props[i].kind == PLACE_PROP_REPLICA_ADD || out.props[i].kind == PLACE_PROP_STRIPE_SET)
            apply_data++;
    CHECK(apply_data == 1, "bal(b): balance_apply=1 must run even when balance=off (got %u data)", apply_data);
    place_out_free(&out);
    meta_free(&sm);

    /* (c) E9 rack survivability preserved: adding node 7 in rack r1 only replaces r1 replicas (never r2 or r3). */
    static const char *const bracks[7] = {"r1", "r2", "r3", "r1", "r2", "r3", "r1"};
    init_cluster_state(&sm, 7, bracks, pm, 1);
    /* Place 9 volumes across nodes 1..6 (3 per rack: {1,2,3} and {4,5,6}), node 7 in r1 has 0 replicas. */
    for (uint32_t v = 1; v <= 9; v++) {
        uint32_t reps[3] = {(v % 2) ? 1u : 4u, (v % 2) ? 2u : 5u, (v % 2) ? 3u : 6u};
        char vname[32];
        snprintf(vname, sizeof vname, "rkv%u", v);
        add_vol(&sm, v, vname, reps, 3, 0, 0, NULL);
    }
    in = make_in(&sm, pm, 7);
    place_out_init(&out);
    balance_plan(&in, &out);
    if (g_mut != PLACE_MUT_RACK_IGNORED) {
        CHECK(out.nprops >= 1, "bal(c): expected balance move onto node 7 (r1)");
        for (uint32_t i = 0; i < out.nprops; i++) {
            if (out.props[i].kind != PLACE_PROP_REPLICA_ADD)
                continue;
            const struct meta_member *src = meta_member(&sm, out.props[i].other_node);
            const struct meta_member *dst = meta_member(&sm, out.props[i].node);
            CHECK(src && dst && strcmp(src->rack, dst->rack) == 0,
                  "bal(c): replacing %s with %s would break E9 rack survivability", src ? src->rack : "?",
                  dst ? dst->rack : "?");
        }
    }
    place_out_free(&out);
    meta_free(&sm);

    /* (d) EC stripe balance: same-class preservation (E51), byte-utilization balancing, and fill-mark avoidance. */
    init_cluster_state(&sm, 8, NULL, pm, 1);
    uint64_t sz = 1000ull << 30;
    struct meta_disk dks[8];
    struct place_disk_usage du[8];
    for (uint32_t i = 0; i < 8; i++) {
        /* Nodes 1..6 and 8 are HDD; node 7 is NVMe (different class!). */
        uint8_t cls = (i == 6) ? META_DISK_NVME : META_DISK_HDD;
        uint8_t uses = (i == 6) ? META_DISK_USE_ALL : META_DISK_USE_CHUNK;
        dks[i] = mk_disk((uint8_t)(60 + i), cls, uses, sz);
        add_disk(&sm, i + 1, &dks[i]);
        if (i != 6) {
            struct meta_disk nv = mk_disk((uint8_t)(80 + i), META_DISK_NVME, META_DISK_USE_LOG, sz);
            add_disk(&sm, i + 1, &nv);
        }
        du[i] = (struct place_disk_usage){.disk_class = cls, .uses = uses, .state = META_STORE_UP, .have_usage = 1,
                                          .size_bytes = sz, .chunk_bytes = 0};
        memcpy(du[i].store_id, dks[i].store_id, 16);
        pm[i].disks = &du[i];
        pm[i].ndisks = 1;
    }
    /* Create 4 EC 4+2 volumes across HDD nodes 1..6 (balanced logs: 2 per node 1..6; each HDD holds 4 stripe pos). */
    for (uint32_t v = 1; v <= 4; v++) {
        uint32_t lrep[3] = {(v % 2) ? 1u : 4u, (v % 2) ? 2u : 5u, (v % 2) ? 3u : 6u};
        uint32_t stp[6] = {1, 2, 3, 4, 5, 6};
        char vname[32];
        snprintf(vname, sizeof vname, "ecb%u", v);
        add_vol(&sm, v, vname, lrep, 3, 4, 2, stp);
    }
    in = make_in(&sm, pm, 8);
    in.balance_apply = 1;
    place_out_init(&out);
    balance_plan(&in, &out);
    if (g_mut != PLACE_MUT_MIX_STRIPE) {
        int saw_stripe_to_8 = 0;
        for (uint32_t i = 0; i < out.nprops; i++) {
            if (out.props[i].kind == PLACE_PROP_STRIPE_SET) {
                CHECK(out.props[i].node == 8, "bal(d): HDD stripe must move to HDD node 8, not NVMe node %u",
                      out.props[i].node);
                saw_stripe_to_8 = 1;
            }
        }
        /* Logs on nodes 1..6 are balanced (2 each); only nodes 7/8 have 0 log replicas so a log move may take the
         * single 5% slot first. Verify stripe-only behavior directly below once nodes 7/8 are storage-only. */
        (void)saw_stripe_to_8;
    }
    place_out_free(&out);

    /* Pure byte-utilization imbalance on balanced counts: node 1 at 70% chunks, node 8 at 5% chunks (and fill mark). */
    du[0].chunk_bytes = sz * 70 / 100;
    for (uint32_t i = 1; i < 6; i++)
        du[i].chunk_bytes = sz * 30 / 100;
    du[7].chunk_bytes = sz * 5 / 100;
    /* Make node 8 storage-only so no log move competes for the 5% slot. */
    set_role(&sm, 8, META_ROLE_STORAGE);
    set_role(&sm, 7, META_ROLE_STORAGE);
    in = make_in(&sm, pm, 8);
    in.balance_apply = 1;
    place_out_init(&out);
    balance_plan(&in, &out);
    if (g_mut != PLACE_MUT_MIX_STRIPE) {
        CHECK(out.nprops >= 1 && out.props[0].kind == PLACE_PROP_STRIPE_SET && out.props[0].other_node == 1 &&
                  out.props[0].node == 8,
              "bal(d): expected byte-driven STRIPE_SET from 70%% node 1 to 5%% HDD node 8 (got %u props, kind=%d %u->%u)",
              out.nprops, out.nprops ? (int)out.props[0].kind : -1, out.nprops ? out.props[0].other_node : 0,
              out.nprops ? out.props[0].node : 0);
    }
    place_out_free(&out);

    /* Fill mark check: pushing node 8 to 90% (> fill mark) must exclude it from stripe moves. */
    du[7].chunk_bytes = sz * 90 / 100;
    place_out_init(&out);
    balance_plan(&in, &out);
    if (g_mut != PLACE_MUT_PLACE_FULL && g_mut != PLACE_MUT_MIX_STRIPE) {
        for (uint32_t i = 0; i < out.nprops; i++)
            CHECK(!(out.props[i].kind == PLACE_PROP_STRIPE_SET && out.props[i].node == 8),
                  "bal(d): must not move stripe position onto 90%% full node 8");
    }
    place_out_free(&out);
    du[7].chunk_bytes = sz * 5 / 100;

    /* (e) Never runs while any rebuild / drain / OUT / DOWN / fault / place_plan proposal is active. */
    set_member_state(&sm, 2, META_DRAINING);
    place_out_init(&out);
    balance_plan(&in, &out);
    CHECK(out.nprops == 0, "bal(e): must emit 0 props while a member is DRAINING");
    place_out_free(&out);
    set_member_state(&sm, 2, META_UP);

    set_member_state(&sm, 2, META_DOWN);
    place_out_init(&out);
    balance_plan(&in, &out);
    CHECK(out.nprops == 0, "bal(e): must emit 0 props while a member holding data is DOWN");
    place_out_free(&out);
    set_member_state(&sm, 2, META_UP);

    pm[0].nfaults = 1;
    pm[0].faults[0] = (struct place_vol_fault){.volume = 1, .fault_s = 1};
    place_out_init(&out);
    balance_plan(&in, &out);
    CHECK(out.nprops == 0, "bal(e): must emit 0 props while a member reports a replica fault");
    place_out_free(&out);
    pm[0].nfaults = 0;

    place_out_init(&out);
    struct place_prop prev_p = {.kind = PLACE_PROP_STRIPE_WHOLE, .volume = 1, .pos = 0, .node = 1, .version = 1};
    place_out_push(&out, &prev_p);
    balance_plan(&in, &out);
    CHECK(out.nprops == 1, "bal(e): must emit 0 new props when place_plan emitted proposals this tick");
    place_out_free(&out);

    /* (f) Regression test (r13-sim seed 550): concurrent balance emission must check chunk fill mark against REAL
     *     (non-virtual) bytes so an incoming move onto dst does not ride a concurrent outgoing move off dst before
     *     STRIPE_WHOLE + MK_GC actually frees space on dst. */
    {
        struct meta_state sm2;
        struct place_member pm2[9];
        init_cluster_state(&sm2, 9, NULL, pm2, 1);
        struct meta_disk dks2[9];
        struct place_disk_usage du2[9];
        for (uint32_t i = 0; i < 9; i++) {
            dks2[i] = mk_disk((uint8_t)(100 + i), META_DISK_HDD, META_DISK_USE_CHUNK, sz);
            add_disk(&sm2, i + 1, &dks2[i]);
            struct meta_disk nv = mk_disk((uint8_t)(120 + i), META_DISK_NVME, META_DISK_USE_LOG, sz);
            add_disk(&sm2, i + 1, &nv);
            du2[i] = (struct place_disk_usage){.disk_class = META_DISK_HDD,
                                               .uses = META_DISK_USE_CHUNK,
                                               .state = META_STORE_UP,
                                               .have_usage = 1,
                                               .size_bytes = sz,
                                               .chunk_bytes = sz * 41 / 100};
            memcpy(du2[i].store_id, dks2[i].store_id, 16);
            pm2[i].disks = &du2[i];
            pm2[i].ndisks = 1;
            set_role(&sm2, i + 1, (i < 6) ? META_ROLE_BOTH : META_ROLE_STORAGE);
        }
        /* 6 EC 4+2 volumes -> total_pos = 54, max_conc = 2. Balanced logs on nodes 1..6 (3 each).
         * Vols 1-2 stripe = {1,2,3,4,5,9} (node 9 already holds vols 1-2, so 1 cannot move to 9);
         * Vols 3-6 stripe = {2,3,4,5,6,8} (node 8 holds 4 pos at 44%, est_b = 11%).
         * Node 1 holds 2 pos at 94% (est_b = 47%); node 9 is at 10%; mark_ppm = 95% * 8/9 = 84.4%.
         * Move 1 (8->9) virtually drops node 8 from 44% to 33%, making virtual 1->8 look 33% + 47% = 80% < 84.4%,
         * while REAL concurrent 44% + 47% = 91% >= 84.4%. Step 10 MUST NOT emit 1->8 concurrently with 8->9! */
        for (uint32_t v = 1; v <= 6; v++) {
            uint32_t lrep[3] = {(v % 2) ? 1u : 4u, (v % 2) ? 2u : 5u, (v % 2) ? 3u : 6u};
            uint32_t stp[6];
            if (v <= 2) {
                uint32_t s1[6] = {1, 2, 3, 4, 5, 9};
                memcpy(stp, s1, sizeof stp);
            } else {
                uint32_t s2[6] = {2, 3, 4, 5, 6, 8};
                memcpy(stp, s2, sizeof stp);
            }
            char vname[32];
            snprintf(vname, sizeof vname, "reg%u", v);
            add_vol(&sm2, v, vname, lrep, 3, 4, 2, stp);
        }
        du2[0].chunk_bytes = sz * 94 / 100; /* node 1: 94% across 2 pos -> est_b = 47% */
        du2[6].chunk_bytes = sz * 80 / 100; /* node 7: 80% (80+47=127 >= 84.4) */
        du2[7].chunk_bytes = sz * 44 / 100; /* node 8: 44% across 4 pos -> est_b = 11% */
        du2[8].chunk_bytes = sz * 10 / 100; /* node 9: 10% across 2 pos */
        struct place_in in2 = make_in(&sm2, pm2, 9);
        in2.balance_apply = 1;
        place_out_init(&out);
        balance_plan(&in2, &out);
        if (g_mut != PLACE_MUT_PLACE_FULL && g_mut != PLACE_MUT_MIX_STRIPE) {
            int saw_8_to_9 = 0;
            for (uint32_t i = 0; i < out.nprops; i++) {
                if (out.props[i].kind == PLACE_PROP_STRIPE_SET && out.props[i].other_node == 8 &&
                    out.props[i].node == 9)
                    saw_8_to_9 = 1;
                CHECK(!(out.props[i].kind == PLACE_PROP_STRIPE_SET && out.props[i].other_node == 1 &&
                        out.props[i].node == 8),
                      "bal(f): emitted concurrent 1->8 move projecting node 8 to 91%% >= 84.4%% fill mark before "
                      "outgoing 8->9 freed space");
            }
            CHECK(saw_8_to_9, "bal(f): expected 8->9 move to be emitted so concurrent fill-mark path is exercised");
        }
        place_out_free(&out);
        meta_free(&sm2);
    }

    meta_free(&sm);
}

/* R10 (r10-driver): converting-volume handling in place_rack_survivable and place_plan. */
static void test_r10_converting(void)
{
    struct meta_state sm;
    struct place_member pm[8];
    const char *racks[8] = {"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"};
    init_cluster_state(&sm, 8, racks, pm, 1);

    /* EC 4+3 converting to 4+2 in REENCODE: nstripe=7, pos 6 is retired (retired_ver=2). */
    uint32_t lrep[4] = {1, 2, 3, 4};
    uint32_t stp[7] = {1, 2, 3, 4, 5, 6, 7};
    add_vol(&sm, 1, "conv1", lrep, 4, 4, 3, stp);
    struct meta_volume *v = &sm.volumes[0];
    v->ec_k = 4;
    v->ec_m = 2;
    v->stripe_version = 2;
    v->stripe_pos[6].retired_ver = 2;
    v->conv.phase = META_CONV_REENCODE;
    v->conv.orig_redundancy = META_RED_EC;
    v->conv.orig_k = 4;
    v->conv.orig_m = 3;
    v->conv.orig_replicas = 4;
    v->conv.target_replicas = 3;
    v->conv.new_stripe_version = 2;

    /* All 7 stripe positions on distinct racks: both 4+2 and 4+3 survive any single rack loss. */
    CHECK(place_rack_survivable(&sm, v) == 1, "conv 4+3->4+2 on 7 racks should be rack survivable");

    /* Mark node 7 DRAINING (retired pos 6) and node 1 OUT (log replica + active pos 0): place_plan must NOT move log
     * replicas during active conversion and must NOT drain retired pos 6, while still healing active pos 0. */
    set_member_state(&sm, 7, META_DRAINING);
    set_member_state(&sm, 1, META_OUT);
    struct place_in in = make_in(&sm, pm, 8);
    struct place_out out;
    place_out_init(&out);
    place_plan(&in, &out);
    for (uint32_t i = 0; i < out.nprops; i++) {
        CHECK(out.props[i].kind != PLACE_PROP_REPLICA_ADD,
              "conv: place_plan must not emit REPLICA_ADD during active conversion");
        CHECK(!(out.props[i].kind == PLACE_PROP_STRIPE_SET && out.props[i].pos == 6),
              "conv: place_plan must not drain retired stripe pos 6");
    }
    /* Active pos 0 on node 1 (non-retired) SHOULD still be re-placed onto node 8 during REENCODE. */
    if (g_mut == PLACE_MUT_NONE) {
        int saw_pos0 = 0;
        for (uint32_t i = 0; i < out.nprops; i++)
            if (out.props[i].kind == PLACE_PROP_STRIPE_SET && out.props[i].pos == 0)
                saw_pos0 = 1;
        CHECK(saw_pos0, "conv REENCODE: expected active pos 0 on OUT node 1 to be re-placed");
    }
    place_out_free(&out);

    /* During ec -> replicas REENCODE (redundancy == REPLICAS, nstripe > 0, all positions retired), an OUT stripe
     * holder MUST still be rebuilt so old objects remain decodable for MK_UNSETTLE. */
    set_member_state(&sm, 7, META_OUT);
    set_member_state(&sm, 1, META_UP);
    sm.volumes[0].redundancy = META_RED_REPLICAS;
    sm.volumes[0].ec_k = 0;
    sm.volumes[0].ec_m = 0;
    for (uint32_t p = 0; p < 7; p++)
        sm.volumes[0].stripe_pos[p].retired_ver = 2;
    place_out_init(&out);
    place_plan(&in, &out);
    if (g_mut == PLACE_MUT_NONE) {
        int saw_pos6 = 0;
        for (uint32_t i = 0; i < out.nprops; i++)
            if (out.props[i].kind == PLACE_PROP_STRIPE_SET && out.props[i].pos == 6)
                saw_pos6 = 1;
        CHECK(saw_pos6, "conv ec->replicas REENCODE: expected OUT retired pos 6 to still be rebuilt");
    }
    place_out_free(&out);

    /* During DROP phase, place_plan skips all stripe fills on the converting volume. */
    sm.volumes[0].conv.phase = META_CONV_DROP;
    place_out_init(&out);
    place_plan(&in, &out);
    for (uint32_t i = 0; i < out.nprops; i++)
        CHECK(out.props[i].kind != PLACE_PROP_STRIPE_SET,
              "conv DROP: place_plan must not emit STRIPE_SET during DROP");
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
    test_r13_capability_class_and_fill();
    test_r9_return();
    test_balance();
    test_r10_converting();

    if (fails) {
        fprintf(stderr, "test_place: %d checks failed\n", fails);
        return 1;
    }
    printf("test_place: ok\n");
    return 0;
}
