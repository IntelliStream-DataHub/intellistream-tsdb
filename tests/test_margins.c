/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The margins of a cluster volume (server/margins.c; design-cluster-data.md E15, E24) on hand-made metadata copies,
 * and jepsen's judgement of a failed volume (tests/jepsen/outage_judge.h). No server: test_cluster margins runs them
 * end to end. `--mutant NAME` plants one of margins.h's bugs (or jepsenlost: the judgement forgets LOST); each must
 * fail a check here (Makefile, test-meta).
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int judge_mut;
#define OUTAGE_JUDGE_MUT judge_mut
#include "jepsen/outage_judge.h"
#include "margins.h"

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

static struct cluster_copy_member members[8];
static struct cluster_copy cp = {.self = 1, .nmembers = 8, .members = members};

static void members_up(void)
{
    for (uint32_t i = 0; i < 8; i++)
        members[i] = (struct cluster_copy_member){.id = i + 1, .state = META_UP};
}

static struct cluster_copy_vol ec42(void) /* ec=4+2 on nodes 1..6, replicas 1, 2, 3 all in sync */
{
    struct cluster_copy_vol v = {.number = 7, .redundancy = META_RED_EC, .ec_k = 4, .ec_m = 2, .nstripe = 6, .nrep = 3,
                                 .nisr = 3, .stripe_version = 1};
    for (uint32_t i = 0; i < 6; i++)
        v.stripe[i] = i + 1;
    for (uint32_t i = 0; i < 3; i++)
        v.rep[i].node = v.isr[i] = i + 1;
    return v;
}

int main(int argc, char **argv)
{
    int mut = MARGIN_MUT_NONE;
    if (argc == 3 && strcmp(argv[1], "--mutant") == 0) {
        if (strcmp(argv[2], "jepsenlost") == 0)
            judge_mut = 1;
        else if ((mut = margin_mutant_parse(argv[2])) <= 0 || mut == MARGIN_MUT_LOSTNOWAIT ||
                 mut == MARGIN_MUT_YOUNGCORRUPT) { /* tsdbd's own: tests/margin_mutants.sh */
            fprintf(stderr, "test_margins: no mutant %s\n", argv[2]);
            return 2;
        }
        printf("test_margins: mutant %s\n", argv[2]);
    }
    struct margins m;
    members_up();
    struct cluster_copy_vol v = ec42();

    /* a whole volume: two more losses on either tier */
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.young == 2 && m.settled == 2 && m.tier == MARGIN_TIER_OK && m.present == 6,
          "whole 4+2: young %d settled %d tier %s present %u", m.young, m.settled, margin_tier_name(m.tier), m.present);

    /* a stripe member down: settled margin 1 (k positions plus one); the young tier keeps its ISR */
    members[4].state = META_DOWN;
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.young == 2 && m.settled == 1 && m.tier == MARGIN_TIER_OK && m.away == 1,
          "node 5 down: young %d settled %d tier %s", m.young, m.settled, margin_tier_name(m.tier));
    /* DRAINING still serves its chunks; OUT and a member the copy does not know are away */
    members[4].state = META_DRAINING;
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.settled == 2, "node 5 draining: settled %d, not 2", m.settled);
    members[4].state = META_OUT;
    v.stripe[5] = 42;
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.settled == 0 && m.tier == MARGIN_TIER_OK, "node 5 out, position 5 on an unknown node: settled %d tier %s",
          m.settled, margin_tier_name(m.tier));
    v.stripe[5] = 6;

    /* a replica leaves the ISR: the young margin drops, whatever the replica count says */
    v.nisr = 2;
    v.isr[1] = 3;
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.young == 1, "an ISR of 2 of 3 replicas: young %d, not 1", m.young);
    v.nisr = 0;
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.young == -1, "an empty ISR: young %d, not -1", m.young);
    v = ec42();

    /* more than m away: unavailable (retryable), never lost while nobody declared the holders destroyed */
    members_up();
    members[0].state = members[1].state = members[2].state = META_DOWN;
    margins_of(&cp, &v, NULL, 0, false, mut, &m);
    CHECK(m.settled == -1 && m.tier == MARGIN_TIER_UNAVAILABLE, "three of 4+2 down: settled %d tier %s", m.settled,
          margin_tier_name(m.tier));
    /* the same with two of them declared destroyed: still unavailable (m destroyed is not lost) */
    uint32_t two[] = {1, 2};
    margins_of(&cp, &v, two, 2, false, mut, &m);
    CHECK(m.tier == MARGIN_TIER_UNAVAILABLE && m.destroyed == 2, "two destroyed, one down: tier %s destroyed %u",
          margin_tier_name(m.tier), m.destroyed);
    /* three destroyed (more than m): lost, even with their servers answering (a destroyed position never counts) */
    members_up();
    uint32_t three[] = {6, 1, 3};
    margins_of(&cp, &v, three, 3, false, mut, &m);
    CHECK(m.tier == MARGIN_TIER_LOST && m.destroyed == 3 && m.settled == -1, "three destroyed: tier %s destroyed %u "
          "settled %d", margin_tier_name(m.tier), m.destroyed, m.settled);
    /* unavailable wins over corrupt: short of k positions, a read is retried whatever damage was seen before */
    members[0].state = members[1].state = members[2].state = META_DOWN;
    margins_of(&cp, &v, NULL, 0, true, mut, &m);
    CHECK(m.tier == MARGIN_TIER_UNAVAILABLE, "three down and corrupt: %s, not unavailable", margin_tier_name(m.tier));
    members_up();
    /* lost wins over corrupt; corrupt is the state of a volume with every position there */
    margins_of(&cp, &v, three, 3, true, mut, &m);
    CHECK(m.tier == MARGIN_TIER_LOST, "lost and corrupt: %s", margin_tier_name(m.tier));
    margins_of(&cp, &v, NULL, 0, true, mut, &m);
    CHECK(m.tier == MARGIN_TIER_CORRUPT && m.settled == 2, "corrupt: %s", margin_tier_name(m.tier));
    /* a destroyed node outside the stripe group changes nothing */
    uint32_t other[] = {7, 8, 9};
    margins_of(&cp, &v, other, 3, false, mut, &m);
    CHECK(m.tier == MARGIN_TIER_OK && m.settled == 2, "destroyed outside the group: %s %d", margin_tier_name(m.tier),
          m.settled);

    /* a replicas volume: a young margin only */
    struct cluster_copy_vol r = {.number = 3, .nrep = 3, .nisr = 2, .isr = {1, 2}, .rep = {{1, 0}, {2, 0}, {3, 0}}};
    margins_of(&cp, &r, three, 3, true, mut, &m);
    CHECK(m.young == 1 && m.tier == MARGIN_TIER_NONE && m.settled == 0, "replicas volume: young %d tier %s", m.young,
          margin_tier_name(m.tier));

    /* a shared volume (NFS failover): every candidate is in its ISR, but they share one storage and hold no copies */
    struct cluster_copy_vol sh = {.number = 4, .kind = META_VOL_SHARED, .nrep = 3, .nisr = 3, .isr = {1, 2, 3},
                                  .rep = {{1, 0}, {2, 0}, {3, 0}}};
    margins_of(&cp, &sh, NULL, 0, false, mut, &m);
    CHECK(m.young_na && m.young == 0 && m.tier == MARGIN_TIER_NONE, "shared volume: young_na %d young %d tier %s",
          m.young_na, m.young, margin_tier_name(m.tier));
    r.kind = META_VOL_REPLICATED;
    margins_of(&cp, &r, NULL, 0, false, mut, &m);
    CHECK(!m.young_na, "a replicated volume has a young margin");

    /* 6+2 with one away: margin 1 */
    struct cluster_copy_vol w = {.number = 9, .redundancy = META_RED_EC, .ec_k = 6, .ec_m = 2, .nstripe = 8, .nrep = 3,
                                 .nisr = 3};
    for (uint32_t i = 0; i < 8; i++)
        w.stripe[i] = 8 - i;
    members[7].state = META_DOWN;
    margins_of(&cp, &w, NULL, 0, false, mut, &m);
    CHECK(m.settled == 1 && m.present == 7, "6+2 with node 8 down: settled %d present %u", m.settled, m.present);
    members_up();

    CHECK(strcmp(margin_tier_name(MARGIN_TIER_LOST), "lost") == 0 && strcmp(margin_tier_name(99), "?") == 0,
          "tier names");
    CHECK(margin_mutant_parse(NULL) == 0 && margin_mutant_parse("") == 0 && margin_mutant_parse("nosuch") == -1,
          "mutant names");

    /* jepsen: DIVERGED and LOST are failed for good, an outage still being handled is not */
    CHECK(outage_entry_failed(TSDB_HEALTH_UNAVAILABLE, TSDB_WHY_DIVERGED), "DIVERGED not failed");
    CHECK(outage_entry_failed(TSDB_HEALTH_UNAVAILABLE, TSDB_WHY_LOST), "LOST not failed");
    CHECK(outage_entry_failed(TSDB_HEALTH_DEGRADED, TSDB_WHY_LOST), "LOST of a degraded volume not failed");
    CHECK(!outage_entry_failed(TSDB_HEALTH_UNAVAILABLE, TSDB_WHY_NFS_DOWN), "NFS_DOWN failed");
    CHECK(!outage_entry_failed(TSDB_HEALTH_UNAVAILABLE, TSDB_WHY_RECOVERING), "RECOVERING failed");
    CHECK(!outage_entry_failed(TSDB_HEALTH_AVAILABLE, TSDB_WHY_NONE), "an available volume failed");

    if (fails) {
        fprintf(stderr, "test_margins: %d checks failed\n", fails);
        return 1;
    }
    printf("test_margins: ok\n");
    return 0;
}
