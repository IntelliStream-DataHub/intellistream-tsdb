/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cluster's metadata (docs/design-cluster.md §2, §13.1): the state machine of server/meta.c on its own, the copied
 * Raft core under its own simulator (src/raft/raft_sim.c), the core's log on real files (src/raft/raft_log.c), and the
 * metadata state machine under real Raft cores in a simulated world.
 *
 *   test_meta DIR [--seeds N] [--seconds S] [--first SEED]   (make test: a few hundred seeds; make meta-soak: many)
 *
 * The metadata simulator: 3 or 5 real cores (raft.c), each with an in-memory log whose durability is explicit (a sync
 * completes after a delay; a crash keeps only what was synced and the last saved hard state), a state machine from
 * meta.c and snapshots of it. Messages go through raft_wire's encoding over a network that delays, drops, duplicates
 * and partitions; clocks drift. The leader of the moment proposes metadata changes made from its own applied view,
 * which is often stale: leader changes, ISR changes, members, replicas, slots, users, several in one entry. After
 * every apply it checks that
 *   - every server that applies index i reaches the same state (hash) as the first one that did;
 *   - each step obeys meta_check_step: epochs rise one at a time, a new leader was in the old ISR, the ISR changes
 *     only with a new version and always holds the leader;
 *   - (volume, epoch) names one leader in the whole run;
 *   - an operation applied only if what it named held in the state it met (check_named: the epoch of VOLUME_LEADER,
 *     the epoch and ISR version of ISR_CHANGE, the incarnation of MEMBER_STATE).
 * A failure prints the seed; `--first SEED --seeds 1` replays it.
 */
#include "../server/meta.h"
#include "../src/repl.h"
#include "raft/raft.h"
#include "raft/raft_log.h"
#include "raft/raft_sim.h"
#include "raft/raft_wire.h"

#include <errno.h>
#include "watchdog.h"
#include <stdarg.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "scratch.h"

static int failures;
#define CHECK(c)                                                                                                     \
    do {                                                                                                             \
        if (!(c)) {                                                                                                  \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                             \
            failures++;                                                                                              \
        }                                                                                                            \
    } while (0)

/* ================================================================ the state machine alone */

static int apply1(struct meta_state *m, struct meta_entry *e, int *res, uint32_t nres)
{
    meta_entry_finish(e);
    int rc = meta_apply(m, m->applied + 1, 1, e->buf, e->len, res, nres);
    meta_entry_free(e);
    meta_entry_init(e);
    return rc;
}

static void member(struct meta_member *mb, uint32_t id)
{
    memset(mb, 0, sizeof *mb);
    mb->id = id;
    snprintf(mb->addr, sizeof mb->addr, "10.0.0.%u:5555", id);
    snprintf(mb->peer, sizeof mb->peer, "10.0.0.%u:5556", id);
    snprintf(mb->rack, sizeof mb->rack, "r%u", id % 3);
    snprintf(mb->power_feed, sizeof mb->power_feed, "f%u", id % 2);
    mb->flags = META_MF_ALL;
}

/* The replica instance of replica i of replicated volume `number` in these tests */
static uint64_t rid_of(uint32_t number, uint32_t i) { return 1000 + number * 10 + i; }

static void vol_add(struct meta_entry *e, int kind, uint32_t number, const char *name, uint32_t min_isr,
                    const uint32_t *nodes, uint32_t n)
{
    struct meta_volume_add a;
    memset(&a, 0, sizeof a);
    a.number = number;
    snprintf(a.name, sizeof a.name, "%s", name);
    a.uuid[0] = (uint8_t)(number + 1);
    a.kind = (uint8_t)kind;
    a.min_isr = min_isr;
    a.n = n;
    for (uint32_t i = 0; i < n; i++) {
        a.nodes[i] = nodes[i];
        a.rid[i] = kind == META_VOL_SHARED ? 0 : rid_of(number, i);
    }
    if (kind == META_VOL_SHARED) {
        snprintf(a.st.path, sizeof a.st.path, "/mnt/%s", name);
        snprintf(a.st.export, sizeof a.st.export, "nfs%u:/pool/%s", number % 2, name);
        snprintf(a.st.fstype, sizeof a.st.fstype, "nfs4");
        snprintf(a.st.group, sizeof a.st.group, "g%u", number % 2);
    }
    meta_op_volume_add(e, &a);
}

/* An ISR change to `isr` with the replica instances the record holds */
static void isr_chg(struct meta_entry *e, const struct meta_volume *v, uint64_t epoch, uint64_t ver,
                    const uint32_t *isr, uint32_t n, uint64_t commit)
{
    uint64_t rid[META_REPLICAS_MAX];
    for (uint32_t i = 0; i < n; i++) {
        const struct meta_replica *r = meta_replica(v, isr[i]);
        rid[i] = r ? r->rid : 0;
    }
    meta_op_isr_change(e, v->number, epoch, ver, isr, rid, n, commit);
}

/* A promotion of replicated volume v: the replicas in `fenced` were fenced, those in `answer` answered (with durable
 * ends 10 + their node id, so the highest node id wins), `leader` is the proposer's pick. */
static struct meta_grant promo_grant(const struct meta_volume *v, uint64_t epoch, uint32_t leader,
                                     const uint32_t *fenced, uint32_t nf, const uint32_t *answer, uint32_t na,
                                     uint64_t start, uint64_t skip)
{
    struct meta_grant g;
    memset(&g, 0, sizeof g);
    g.number = v->number;
    g.epoch = epoch;
    g.new_epoch = epoch + 1 + skip; /* skip: a promotion that jumps an epoch */
    g.isr_version = v->isr_version;
    g.reason = META_GR_UNHEARD;
    g.leader = leader;
    g.start = start;
    g.nfenced = nf;
    memcpy(g.fenced, fenced, nf * sizeof *fenced);
    g.nreplies = na;
    for (uint32_t i = 0; i < na; i++) {
        const struct meta_replica *r = meta_replica(v, answer[i]);
        g.reply[i] = (struct meta_fence_reply){answer[i], r ? r->rid : 0, 10 + answer[i], epoch};
    }
    return g;
}

static void promo_skip(struct meta_entry *e, const struct meta_volume *v, uint64_t epoch, uint32_t leader,
                       const uint32_t *fenced, uint32_t nf, const uint32_t *answer, uint32_t na, uint64_t start,
                       uint64_t skip)
{
    struct meta_grant g = promo_grant(v, epoch, leader, fenced, nf, answer, na, start, skip);
    meta_op_volume_leader(e, &g);
}

static void promo(struct meta_entry *e, const struct meta_volume *v, uint64_t epoch, uint32_t leader,
                  const uint32_t *fenced, uint32_t nf, const uint32_t *answer, uint32_t na, uint64_t start)
{
    promo_skip(e, v, epoch, leader, fenced, nf, answer, na, start, 0);
}

/* A grant of a shared volume */
static struct meta_grant sgrant(const struct meta_volume *v, uint32_t leader, uint64_t new_epoch, int reason)
{
    struct meta_grant g;
    memset(&g, 0, sizeof g);
    g.number = v->number;
    g.epoch = v->epoch;
    g.new_epoch = new_epoch;
    g.isr_version = v->isr_version;
    g.reason = (uint8_t)reason;
    g.leader = leader;
    return g;
}

static void init_cluster(struct meta_state *m, struct meta_entry *e, int mode, int nmembers)
{
    struct meta_member mb;
    struct meta_settings s;
    uint8_t uuid[16] = {1, 2, 3};
    int res[16];
    meta_init(m);
    meta_settings_default(&s);
    member(&mb, 1);
    meta_op_init(e, uuid, &s, &mb, mode, META_LEVEL);
    for (int id = 2; id <= nmembers; id++) {
        member(&mb, (uint32_t)id);
        meta_op_member_add(e, &mb);
    }
    CHECK(apply1(m, e, res, 16) == META_OK && res[0] == META_OK);
}

static void test_state_machine(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    struct meta_member mb;
    struct meta_settings s;
    uint8_t uuid[16] = {1, 2, 3};
    int res[16];
    meta_init(&m);
    meta_entry_init(&e);
    meta_settings_default(&s);

    /* nothing before INIT */
    member(&mb, 2);
    meta_op_member_add(&e, &mb);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && m.nmembers == 0);

    member(&mb, 1);
    meta_op_init(&e, uuid, &s, &mb, META_MODE_REPLICATED, META_LEVEL);
    meta_op_init(&e, uuid, &s, &mb, META_MODE_REPLICATED, META_LEVEL); /* a second INIT: refused, the first stands */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STATE);
    CHECK(m.initialised && m.nmembers == 1 && m.next_member == 2 && m.applied == 2 && m.mode == META_MODE_REPLICATED &&
          m.level == META_LEVEL);

    for (uint32_t id = 2; id <= 5; id++) {
        member(&mb, id);
        meta_op_member_add(&e, &mb);
    }
    member(&mb, 3); /* taken */
    meta_op_member_add(&e, &mb);
    member(&mb, 9);
    strcpy(mb.peer, "10.0.0.1:5556"); /* a live member's peer address */
    meta_op_member_add(&e, &mb);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[3] == META_OK && res[4] == META_ERR_EXISTS &&
          res[5] == META_ERR_EXISTS && m.nmembers == 5);
    CHECK(!strcmp(meta_member(&m, 3)->power_feed, "f1") && meta_member(&m, 3)->flags == META_MF_ALL);

    /* a volume: epoch 1, its first replica leads, the ISR is all of them; the other kind is refused (M7) */
    uint32_t nodes[3] = {2, 4, 1};
    vol_add(&e, META_VOL_REPLICATED, 0, "v0", 1, nodes, 3);
    vol_add(&e, META_VOL_REPLICATED, 1, "v0", 1, nodes, 3); /* name taken */
    uint32_t bad[3] = {2, 2, 1};
    vol_add(&e, META_VOL_REPLICATED, 2, "v2", 1, bad, 3); /* one server twice */
    vol_add(&e, META_VOL_SHARED, 3, "v3", 1, nodes, 3);   /* a shared volume in a replicated cluster */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_EXISTS &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID);
    const struct meta_volume *v = meta_volume(&m, 0);
    CHECK(v && v->kind == META_VOL_REPLICATED && v->epoch == 1 && v->leader == 2 && v->nisr == 3 &&
          v->isr[0] == 1 && v->isr[2] == 4 && v->nhist == 1 && v->rep[1].rid == rid_of(0, 1));

    /* the ISR shrinks at the current epoch and version only; a stale one changes nothing; a member's instance must
     * be the record's; a shared-only op on a replicated volume is refused */
    meta_copy(&prev, &m);
    uint32_t isr2[2] = {2, 4};
    isr_chg(&e, meta_volume(&m, 0), 1, 1, isr2, 2, 77);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->isr_commit == 77);
    isr_chg(&e, meta_volume(&m, 0), 1, 1, isr2, 2, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    uint32_t noleader[1] = {4};
    isr_chg(&e, meta_volume(&m, 0), 1, 2, noleader, 1, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    {
        uint64_t wrong[2] = {rid_of(0, 0), 4242}; /* node 2's instance is rid_of(0, 0); node 4's is not 4242 */
        meta_op_isr_change(&e, 0, 1, 2, isr2, wrong, 2, 0);
        meta_op_volume_self(&e, 0, 2, 2);
        meta_op_volume_reserve(&e, 0, 1, 64, 64, &(struct meta_positions){0});
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
              res[2] == META_ERR_INVALID);
    }
    v = meta_volume(&m, 0);
    CHECK(v->nisr == 2 && v->isr_version == 2);
    char why[256];
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* a new leader from the fence replies (repl_meta_promote's rule): replica 1 left the ISR, so a reply from it
     * does not count, and a pick other than the rule's is refused */
    meta_copy(&prev, &m);
    uint32_t f24[2] = {2, 4}, a1[1] = {1}, a24[2] = {2, 4}, a4[1] = {4};
    promo(&e, meta_volume(&m, 0), 1, 1, f24, 2, a1, 1, 100); /* answers only from outside the ISR */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    promo(&e, meta_volume(&m, 0), 1, 2, f24, 2, a24, 2, 100); /* node 4 answered with the longer log: the rule picks 4, not 2 */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    promo(&e, meta_volume(&m, 0), 1, 4, f24, 2, a4, 1, 100); /* 2 did not answer: it stays in the ISR, marked unanswered (item 16) */
    promo(&e, meta_volume(&m, 0), 1, 4, f24, 2, a4, 1, 101); /* two ops on one volume in an entry: the second is refused */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID);
    v = meta_volume(&m, 0);
    CHECK(v->epoch == 2 && v->leader == 4 && v->nisr == 2 && v->nhist == 2 && v->hist[1].start == 100 &&
          meta_replica(v, 2)->unanswered && !meta_replica(v, 4)->unanswered && v->isr_version == 3);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    promo(&e, meta_volume(&m, 0), 1, 4, f24, 2, a4, 1, 200); /* the deposed epoch's proposer */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    promo(&e, meta_volume(&m, 0), 2, 4, f24, 2, a4, 1, 100); /* a start that is not above the last one: refused */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    promo_skip(&e, meta_volume(&m, 0), 2, 4, f24, 2, a4, 1, 300, 1); /* epoch 4 from 2: a replicated epoch rises by one */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && meta_volume(&m, 0)->epoch == 2);
    {
        /* the basis is the epoch AND the ISR version (design-cluster.md 6.2): fences sent before an ISR change answer
         * for a set that no longer stands, so a grant at the right epoch with an older ISR version is STALE (MUT(14),
         * mutant versionless, drops the version check) */
        struct meta_volume old = *meta_volume(&m, 0);
        old.isr_version--;
        promo(&e, &old, 2, 4, f24, 2, a4, 1, 300);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE && meta_volume(&m, 0)->epoch == 2);
    }
    /* D10: a replicated epoch stays below 2^22 (replica_epoch_begin refuses more): a promotion to 2^22 - 1 commits, one
     * to 2^22 is RANGE (on a copy whose epoch was set near the bound; MUT(9) drops the check) */
    {
        struct meta_state near;
        meta_copy(&near, &m);
        struct meta_volume *nv = &near.volumes[0];
        nv->epoch = META_EPOCH_LIMIT - 2;
        nv->hist[nv->nhist - 1].epoch = nv->epoch;
        promo(&e, meta_volume(&near, 0), META_EPOCH_LIMIT - 2, 4, f24, 2, a4, 1, 400);
        CHECK(apply1(&near, &e, res, 8) == META_OK && res[0] == META_OK &&
              meta_volume(&near, 0)->epoch == META_EPOCH_LIMIT - 1);
        promo(&e, meta_volume(&near, 0), META_EPOCH_LIMIT - 1, 4, f24, 2, a4, 1, 500);
        CHECK(apply1(&near, &e, res, 8) == META_OK && res[0] == META_ERR_RANGE &&
              meta_volume(&near, 0)->epoch == META_EPOCH_LIMIT - 1);
        CHECK(META_EPOCH_LIMIT == 1ull << 22);
        meta_free(&near);
    }

    /* the checker catches a leader from outside the ISR, an epoch that goes back, a skipped epoch */
    struct meta_state bent;
    meta_copy(&bent, &m);
    bent.volumes[0].leader = 1; /* not in epoch 1's ISR {2, 4} */
    bent.volumes[0].isr[0] = 1;
    CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
    meta_free(&bent);
    CHECK(meta_check_step(&m, &prev, why, sizeof why) != 0); /* epoch 2 -> 1 */
    meta_copy(&bent, &m);
    bent.volumes[0].epoch = 3;
    bent.volumes[0].hist[bent.volumes[0].nhist - 1].epoch = 3;
    CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0); /* a replicated volume's epoch skipping one */
    meta_free(&bent);
    meta_free(&prev);

    /* replicas: a learner joins (with its instance), becomes full by joining the ISR; one leaves only once out of the
     * ISR; an ISR member added had been a replica with the same instance; REPLICA_OPEN generations only rise */
    v = meta_volume(&m, 0);
    meta_op_replica_add(&e, 0, 2, 5, 0, 0); /* a replicated replica needs an instance */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_op_replica_add(&e, 0, 2, 5, 5555, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->nrep == 4 && v->rep[3].node == 5 && v->rep[3].learner && v->rep[3].rid == 5555);
    meta_op_replica_open(&e, 0, 5, 5555, 1, 77);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_replica(meta_volume(&m, 0), 5)->open_nonce == 77);
    meta_op_replica_open(&e, 0, 5, 5555, 1, 78); /* not above the recorded one: a restored copy, its own nonce */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_replica_open(&e, 0, 5, 9999, 5, 79); /* another instance of the directory */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_replica_open(&e, 0, 5, 5555, 4, 80);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_replica(meta_volume(&m, 0), 5)->open_gen == 4 &&
          meta_replica(meta_volume(&m, 0), 5)->open_nonce == 80);
    v = meta_volume(&m, 0);
    uint32_t isr45[2] = {4, 5};
    isr_chg(&e, meta_volume(&m, 0), 2, v->isr_version, isr45, 2, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(!v->rep[3].learner && !meta_replica(v, 2)->unanswered);
    meta_op_replica_remove(&e, 0, 2, 5);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_op_replica_remove(&e, 0, 2, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->nrep == 3);

    /* member state follows incarnations; out is final */
    meta_op_member_state(&e, 3, META_DOWN, 7);
    meta_op_member_state(&e, 3, META_UP, 6);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE);
    meta_op_member_state(&e, 3, META_OUT, 7);
    meta_op_member_state(&e, 3, META_UP, 8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STATE);

    /* member flags follow incarnations; a node without volumes_ok is no candidate (a witness) */
    meta_op_member_flags(&e, 3, 9, 0); /* out */
    meta_op_member_flags(&e, 77, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_ERR_NOTFOUND);
    meta_op_member_flags(&e, 5, 0, 0x80); /* an unknown flag bit: damage, a fault */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT && meta_member(&m, 5)->flags == META_MF_ALL);
    meta_op_member_state(&e, 5, META_UP, 3);
    meta_op_member_flags(&e, 5, 2, META_MF_ALL); /* the flags of an earlier run */
    meta_op_member_flags(&e, 5, 3, META_MF_VOTER_OK);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE && res[2] == META_OK);
    CHECK(meta_member(&m, 5)->flags == META_MF_VOTER_OK);
    {
        uint32_t w5[3] = {2, 4, 5};
        vol_add(&e, META_VOL_REPLICATED, 7, "w", 1, w5, 3); /* node 5 has no volumes_ok */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    }
    meta_op_member_flags(&e, 5, 3, META_MF_ALL);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);

    /* a member's addresses move with its current or a later run (the leader pairs them with MEMBER_STATE up);
     * never those of an earlier run, nor onto another live member's peer address */
    uint64_t inc2 = meta_member(&m, 2)->incarnation;
    meta_op_member_addr(&e, 2, inc2 + 1, "10.0.1.2:5555", "10.0.1.2:5556");
    meta_op_member_state(&e, 2, META_UP, inc2 + 1);
    meta_op_member_addr(&e, 2, inc2, "10.0.0.2:5555", "10.0.0.2:5556"); /* the earlier run's */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_ERR_STALE);
    CHECK(!strcmp(meta_member(&m, 2)->peer, "10.0.1.2:5556") && !strcmp(meta_member(&m, 2)->addr, "10.0.1.2:5555"));
    meta_op_member_addr(&e, 2, inc2 + 1, "x:1", "10.0.0.4:5556"); /* member 4's */
    meta_op_member_addr(&e, 2, inc2 + 1, "x:1", "10.0.0.3:5556"); /* member 3's, but 3 is out */
    meta_op_member_addr(&e, 3, 99, "x:1", "10.0.0.9:5556");       /* out is final */
    meta_op_member_addr(&e, 77, 1, "x:1", "10.0.0.9:5556");
    meta_op_member_addr(&e, 2, inc2 + 1, "", "10.0.0.9:5556");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS && res[1] == META_OK &&
          res[2] == META_ERR_STATE && res[3] == META_ERR_NOTFOUND && res[4] == META_ERR_INVALID);
    CHECK(!strcmp(meta_member(&m, 2)->peer, "10.0.0.3:5556"));

    /* slots: a home only for slots without one; a move is pending, frozen under the old home's epoch, then made */
    uint32_t n5[3] = {2, 4, 5};
    vol_add(&e, META_VOL_REPLICATED, 1, "v1", 2, n5, 3);
    meta_op_slot_home(&e, 0, META_SLOTS, 0);
    meta_op_slot_home(&e, 100, 50, 1); /* those slots have a home: the freeze protocol moves them */
    meta_op_slot_home(&e, 0, 1, 77);   /* no such volume */
    meta_op_user_put(&e, "bob", 0, "\0\0\0\0abc", 7);
    meta_op_user_put(&e, "alice", 0, "\0\0\0\0x", 5);
    meta_op_user_del(&e, "carol");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_NOTFOUND && res[4] == META_OK && res[5] == META_OK &&
          res[6] == META_ERR_NOTFOUND);
    CHECK(m.slot_home[99] == 0 && m.slot_home[100] == 0 && m.nmoves == 0 && m.nsince == 1 &&
          meta_slot_since(&m, 5) == m.applied && meta_slot_since(&m, 100) == m.applied);
    CHECK(m.nusers == 2 && !strcmp(m.users[0].name, "alice"));
    {
        uint64_t since0 = meta_slot_since(&m, 100);
        CHECK(since0 > 0);
        meta_copy(&prev, &m);
        meta_op_slot_move_pending(&e, 100, 0, 1);
        meta_op_slot_move_pending(&e, 101, 1, 0); /* slot 101's home is 0, not 1 */
        meta_op_slot_move_pending(&e, 102, 0, 0);
        meta_op_slot_move(&e, 100, 1); /* not frozen (and in the same entry as its pending) */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE &&
              res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
        meta_op_slot_move_pending(&e, 100, 0, 1); /* already pending */
        meta_op_slot_move(&e, 100, 1);            /* not frozen */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS && res[1] == META_ERR_INVALID);
        uint64_t e0 = meta_volume(&m, 0)->epoch;
        meta_op_slot_freeze(&e, 100, 0, e0 + 1); /* an epoch volume 0 does not have: a stale owner's freeze */
        meta_op_slot_freeze(&e, 100, 1, e0);     /* not the move's old home */
        meta_op_slot_freeze(&e, 101, 0, e0);     /* no pending move */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE && res[1] == META_ERR_INVALID &&
              res[2] == META_ERR_NOTFOUND);
        meta_op_slot_freeze(&e, 100, 0, e0);
        meta_op_slot_move(&e, 100, 1); /* the freeze is in this very entry: not committed before it */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID);
        const struct meta_slot_pending *pend = meta_slot_pending(&m, 100);
        CHECK(pend && pend->freeze_epoch == e0); /* a planted bug can leave no pending move: fail, do not crash */
        meta_op_slot_freeze(&e, 100, 0, e0);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS);
        meta_copy(&prev, &m);
        meta_op_slot_move(&e, 100, 2); /* not the pending target */
        meta_op_slot_move(&e, 101, 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_NOTFOUND);
        meta_op_slot_move(&e, 100, 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(m.slot_home[100] == 1 && m.nmoves == 1 && m.moves[0].slot == 100 && m.moves[0].home == 0 &&
              m.moves[0].from_index == since0 && m.npend == 0 && meta_slot_since(&m, 100) == m.applied &&
              meta_slot_since(&m, 99) == since0 && meta_slot_since(&m, 101) == since0);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        /* the checker catches a move that skipped the freeze */
        struct meta_state cheat, unfrozen;
        meta_copy(&unfrozen, &prev);
        unfrozen.pend[0].freeze_epoch = 0; /* the freeze never committed */
        meta_copy(&cheat, &prev);
        cheat.slot_home[100] = 1;
        cheat.npend = 0;
        CHECK(meta_check_step(&unfrozen, &cheat, why, sizeof why) != 0);
        unfrozen.npend = 0; /* no pending move at all */
        CHECK(meta_check_step(&unfrozen, &cheat, why, sizeof why) != 0);
        meta_free(&cheat);
        meta_free(&unfrozen);
        meta_free(&prev);
        /* a move that is cancelled leaves the home and the chain as they were */
        meta_op_slot_move_pending(&e, 200, 0, 1);
        meta_op_slot_move_cancel(&e, 200);
        meta_op_slot_move_cancel(&e, 201);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID &&
              res[2] == META_ERR_NOTFOUND && m.npend == 1);
        meta_op_slot_move_cancel(&e, 200);
        meta_op_slot_move_cancel(&e, 200);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID && m.npend == 0 &&
              m.slot_home[200] == 0 && m.nmoves == 1);
        meta_op_slot_move_cancel(&e, 200);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_NOTFOUND);
    }

    /* domains and the restore generation */
    meta_op_domain_state(&e, "poolA", 9); /* an unknown state: damage */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT && m.ndomains == 0);
    meta_op_domain_state(&e, "poolA", META_DOM_MAINTENANCE);
    meta_op_domain_state(&e, "", META_DOM_NORMAL);
    meta_op_domain_state(&e, "poolB", META_DOM_ROLLED_BACK);
    meta_op_domain_state(&e, "poolA", META_DOM_NORMAL);
    meta_op_restore(&e, 3);
    meta_op_restore(&e, 3);
    meta_op_name_group(&e);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID && res[2] == META_OK &&
          res[3] == META_OK && res[4] == META_OK && res[5] == META_ERR_STALE && res[6] == META_ERR_INVALID);
    CHECK(m.ndomains == 2 && meta_domain(&m, "poolA")->state == META_DOM_NORMAL &&
          meta_domain(&m, "poolB")->state == META_DOM_ROLLED_BACK && m.restore_gen == 3);

    /* snapshots: the same state back, the same hash; damage refused and the state kept */
    uint8_t *img;
    size_t len;
    meta_snapshot_encode(&m, &img, &len);
    struct meta_state back;
    meta_init(&back);
    CHECK(meta_snapshot_decode(&back, img, len) == META_OK);
    CHECK(meta_hash(&back) == meta_hash(&m) && back.applied == m.applied && back.nvolumes == 2 &&
          back.restore_gen == 3 && back.ndomains == 2 && back.nmoves == 1 && back.mode == META_MODE_REPLICATED);
    uint64_t h = meta_hash(&back);
    for (size_t i = 0; i < len; i += 7) {
        img[i] ^= 0x10;
        CHECK(meta_snapshot_decode(&back, img, len) == META_ERR_FORMAT);
        img[i] ^= 0x10;
    }
    CHECK(meta_snapshot_decode(&back, img, len - 1) == META_ERR_FORMAT && meta_hash(&back) == h);
    /* a version 1 image, or a level this build does not apply, is refused even with a valid checksum */
    {
        uint8_t *bad1 = malloc(len);
        memcpy(bad1, img, len);
        uint32_t one = 1, crc;
        memcpy(bad1 + 4, &one, 4);
        crc = iso_crc32c(0, bad1, len - 4);
        memcpy(bad1 + len - 4, &crc, 4);
        CHECK(meta_snapshot_decode(&back, bad1, len) == META_ERR_FORMAT && meta_hash(&back) == h);
        free(bad1);
        struct meta_state hi;
        meta_copy(&hi, &m);
        hi.level = META_LEVEL + 1;
        uint8_t *him;
        size_t hlen;
        meta_snapshot_encode(&hi, &him, &hlen);
        CHECK(meta_snapshot_decode(&back, him, hlen) == META_ERR_FORMAT && meta_hash(&back) == h);
        free(him);
        meta_free(&hi);
    }
    free(img);
    meta_free(&back);

    /* an entry that does not decode changes nothing and says so; so does an operation this build lacks */
    meta_op_user_del(&e, "alice");
    meta_entry_finish(&e);
    uint64_t before = meta_hash(&m), applied = m.applied;
    for (size_t cut = 0; cut < e.len; cut++)
        CHECK(meta_apply(&m, applied + 1, 1, e.buf, cut, NULL, 0) == META_ERR_FORMAT);
    e.buf[8] = 99;
    CHECK(meta_apply(&m, applied + 1, 1, e.buf, e.len, NULL, 0) == META_ERR_FORMAT);
    e.buf[8] = META_OP_LAST + 1;
    CHECK(meta_apply(&m, applied + 1, 1, e.buf, e.len, NULL, 0) == META_ERR_FORMAT);
    e.buf[8] = META_OP_USER_DEL; /* a body with a byte too many: consumed exactly */
    {
        uint8_t *longer = malloc(e.len + 1);
        memcpy(longer, e.buf, e.len);
        longer[e.len] = 0;
        uint32_t bl;
        memcpy(&bl, longer + 9, 4);
        bl++;
        memcpy(longer + 9, &bl, 4);
        CHECK(meta_apply(&m, applied + 1, 1, longer, e.len + 1, NULL, 0) == META_ERR_FORMAT);
        free(longer);
    }
    CHECK(meta_hash(&m) == before && m.applied == applied);
    meta_entry_free(&e);

    /* the group's size */
    CHECK(meta_voter_target(&s, 1) == 1 && meta_voter_target(&s, 2) == 1 && meta_voter_target(&s, 3) == 3 &&
          meta_voter_target(&s, 4) == 3 && meta_voter_target(&s, 5) == 5 && meta_voter_target(&s, 8) == 5);
    s.voters = 3;
    CHECK(meta_voter_target(&s, 8) == 3);
    s.voters = 4;
    CHECK(meta_voter_target(&s, 8) == 3);
    meta_free(&m);
}

/* Shared volumes (design-ha-modes.md §5.4): reserved epochs, VOLUME_SELF, reservations, claims, attestations */
/* R4 (the plan's D8-D16): the replica set's version, moves (REPLICA_ADD with `replaces`, ended by the REMOVE of the
 * replaced member, which puts the learner into its slot), REPLICA_RESET, a planned grant's target, R4's settings and
 * the snapshot version 5. Each step's state is also judged by meta_check_step. */
static void test_replica_set(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[16];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 7);
    uint32_t n0[3] = {2, 4, 1}, n1[3] = {1, 2, 3}, n2[3] = {1, 2, 3};
    vol_add(&e, META_VOL_REPLICATED, 0, "v0", 1, n0, 3);
    vol_add(&e, META_VOL_REPLICATED, 1, "v1", 1, n1, 3);
    vol_add(&e, META_VOL_REPLICATED, 2, "v2", 1, n2, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK);
    CHECK(meta_volume(&m, 0)->set_version == 1 && meta_volume(&m, 2)->set_version == 1);

    /* an ISR change is not a set change (mutant setverraiseisr) */
    meta_copy(&prev, &m);
    uint32_t only2[1] = {2};
    isr_chg(&e, meta_volume(&m, 0), 1, 1, only2, 1, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->set_version == 1);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* REPLICA_RESET: refused for a stale epoch, the leader, an ISR member, a non-member, rid 0 and an instance the
     * volume has (the member's own included); else a learner of the new instance with open generation 0 */
    meta_op_replica_open(&e, 0, 4, rid_of(0, 1), 3, 77);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_replica(meta_volume(&m, 0), 4)->open_gen == 3);
    meta_copy(&prev, &m);
    meta_op_replica_reset(&e, 0, 2, 4, 5000);          /* stale epoch */
    meta_op_replica_reset(&e, 0, 1, 2, 5000);          /* the leader (mutant resetleader) */
    meta_op_replica_reset(&e, 1, 1, 2, 5000);          /* an ISR member of v1 (mutant resetinisr) */
    meta_op_replica_reset(&e, 0, 1, 5, 5000);          /* not a member */
    meta_op_replica_reset(&e, 0, 1, 4, 0);             /* no instance */
    meta_op_replica_reset(&e, 0, 1, 4, rid_of(0, 2));  /* another member's instance */
    meta_op_replica_reset(&e, 0, 1, 4, rid_of(0, 1));  /* its own: not a new one */
    meta_op_replica_reset(&e, 9, 1, 4, 5000);          /* no such volume */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_NOTFOUND && res[4] == META_ERR_INVALID &&
          res[5] == META_ERR_INVALID && res[6] == META_ERR_INVALID && res[7] == META_ERR_NOTFOUND);
    CHECK(meta_hash(&prev) == meta_hash(&m));
    meta_op_replica_reset(&e, 0, 1, 4, 5000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    const struct meta_volume *v = meta_volume(&m, 0);
    const struct meta_replica *r4 = meta_replica(v, 4);
    CHECK(v->set_version == 2 && v->rep[1].node == 4 && r4->rid == 5000 && r4->learner && !r4->open_gen &&
          !r4->open_nonce && !r4->unanswered && meta_volume(&m, 1)->set_version == 1);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    {
        /* the checker: a reset member that is no learner, or keeps an open generation */
        struct meta_state bent;
        meta_copy(&bent, &m);
        bent.volumes[0].rep[1].learner = 0;
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_copy(&bent, &m);
        bent.volumes[0].rep[1].open_gen = 3;
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
    }
    meta_free(&prev);

    /* a move of rep[1] (node 4 of v0): one per volume; the REMOVE of the replaced member ends it, the learner in its
     * slot; a second move while one is pending is EXISTS, an unknown member NOTFOUND */
    meta_copy(&prev, &m);
    meta_op_replica_add(&e, 0, 1, 5, 6000, 4);
    meta_op_replica_add(&e, 1, 1, 6, 6002, 7); /* node 7 is no replica of v1 */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_NOTFOUND);
    meta_op_replica_add(&e, 0, 1, 6, 6001, 1); /* a second move (an entry changes a volume once: an entry of its own) */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS);
    v = meta_volume(&m, 0);
    CHECK(v->set_version == 3 && v->nrep == 4 && v->rep[3].node == 5 && v->rep[3].replaces == 4 && v->rep[3].learner);
    CHECK(meta_volume(&m, 1)->set_version == 1);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    meta_copy(&prev, &m);
    meta_op_replica_remove(&e, 0, 1, 4);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->set_version == 4 && v->nrep == 3 && v->rep[0].node == 2 && v->rep[1].node == 5 && v->rep[2].node == 1 &&
          !v->rep[1].replaces && v->rep[1].rid == 6000); /* mutants moveslotlast, replacesstays */
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    {
        /* the checker: B left at its own slot, or still naming the member it replaced */
        struct meta_state bent;
        meta_copy(&bent, &m);
        struct meta_volume *bv = &bent.volumes[0];
        struct meta_replica t = bv->rep[1];
        bv->rep[1] = bv->rep[2];
        bv->rep[2] = t;
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_copy(&bent, &m);
        bent.volumes[0].rep[1].replaces = 4;
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_copy(&bent, &m); /* the set changed without its version */
        bent.volumes[0].set_version = prev.volumes[0].set_version;
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
    }
    meta_free(&prev);

    /* a move of the last member (node 1 of v0), and its cancel first: the REMOVE of the learner drops the move */
    meta_op_replica_add(&e, 0, 1, 6, 6003, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_remove(&e, 0, 1, 6);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->set_version == 6 && v->nrep == 3 && v->rep[2].node == 1);
    meta_op_replica_add(&e, 0, 1, 6, 6004, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_add(&e, 0, 1, 7, 6005, 0); /* a plain add beside a move: not a second move */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    {
        /* the checker: two moves at once, or a move of a member that is gone */
        struct meta_state bent;
        meta_copy(&bent, &m);
        bent.volumes[0].rep[4].replaces = 2;
        CHECK(meta_check_step(&m, &bent, why, sizeof why) != 0);
        bent.volumes[0].rep[4].replaces = 3;
        CHECK(meta_check_step(&m, &bent, why, sizeof why) != 0);
        meta_free(&bent);
    }
    meta_copy(&prev, &m);
    meta_op_replica_remove(&e, 0, 1, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->set_version == 9 && v->nrep == 4 && v->rep[0].node == 2 && v->rep[1].node == 5 && v->rep[2].node == 6 &&
          v->rep[3].node == 7 && !v->rep[2].replaces);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* a move of rep[0], the preferred leader (node 1 of v2): its leadership goes first (a promotion to 3, then an ISR
     * without 1); the learner B ends in slot 0 (D10) */
    meta_op_replica_add(&e, 2, 1, 4, 7000, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_remove(&e, 2, 1, 1); /* still the leader: refused */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    uint32_t f123[3] = {1, 2, 3}, a3[1] = {3}, i34[2] = {3, 4};
    promo(&e, meta_volume(&m, 2), 1, 3, f123, 3, a3, 1, 100);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 2)->leader == 3);
    v = meta_volume(&m, 2);
    isr_chg(&e, v, 2, v->isr_version, i34, 2, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && !meta_replica(meta_volume(&m, 2), 4)->learner);
    meta_copy(&prev, &m);
    meta_op_replica_remove(&e, 2, 2, 4); /* B is in the ISR now: never removed while in it */
    meta_op_replica_remove(&e, 2, 2, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_OK);
    v = meta_volume(&m, 2);
    CHECK(v->nrep == 3 && v->rep[0].node == 4 && v->rep[0].rid == 7000 && !v->rep[0].replaces && v->rep[1].node == 2 &&
          v->rep[2].node == 3 && v->set_version == 3);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* a planned grant's target (D14): PLANNED only, an ISR member, its end only with it; stored with the epoch */
    {
        v = meta_volume(&m, 2); /* epoch 2, leader 3, ISR {3, 4} */
        uint32_t f34[2] = {3, 4}, a34[2] = {3, 4};
        struct meta_grant g = promo_grant(v, 2, 4, f34, 2, a34, 2, 200, 0);
        g.reason = META_GR_PLANNED;
        /* not in the ISR (mutant targetnotisr, equivalent since r4-transfer: promote_rule refuses it too) */
        g.target = 2;
        g.min_end = 14;
        meta_op_volume_leader(&e, &g);
        g.target = 4;
        g.reason = META_GR_UNHEARD; /* a target only for a planned transfer */
        meta_op_volume_leader(&e, &g);
        g.target = 0; /* an end without a target */
        g.reason = META_GR_PLANNED;
        meta_op_volume_leader(&e, &g);
        g.target = 3; /* the leader it moves away from (mutant targetleader) */
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
              res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && meta_volume(&m, 2)->epoch == 2);
        /* the grant leads to its target or not at all: the old leader answered with the higher end (a transfer
         * passes every reply), a grant naming it is refused (mutant targetignored), as is a target short of min_end */
        g.target = 4;
        g.reply[0].durable = 20; /* node 3's */
        g.leader = 3;
        meta_op_volume_leader(&e, &g);
        g.leader = 4;
        g.min_end = 15;
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
              meta_volume(&m, 2)->epoch == 2);
        g.min_end = 14;
        meta_copy(&prev, &m);
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        v = meta_volume(&m, 2);
        CHECK(v->epoch == 3 && v->leader == 4 && v->grant_target == 4 && v->grant_min_end == 14 && v->set_version == 3);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        struct meta_state bent;
        meta_copy(&bent, &m);
        bent.volumes[2].grant_target = 2; /* the checker: a target that was not in the ISR */
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_copy(&bent, &m);
        bent.volumes[2].grant_target = 3; /* in the ISR, but the leader of epoch 2 */
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_copy(&bent, &m);
        bent.volumes[2].leader = 3; /* the checker: epoch 3 is a transfer to 4, but 3 leads it */
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0 && strstr(why, "but 3 leads it"));
        meta_free(&bent);
        meta_copy(&bent, &m); /* the target or its end changed within an epoch */
        bent.volumes[2].grant_target = 3;
        CHECK(meta_check_step(&m, &bent, why, sizeof why) != 0);
        bent.volumes[2].grant_target = 4;
        bent.volumes[2].grant_min_end = 15;
        CHECK(meta_check_step(&m, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_free(&prev);
    }

    /* settings (D16): defaults and bounds */
    {
        struct meta_settings st;
        meta_settings_default(&st);
        CHECK(st.rebuild_mb_s == 200 && st.rebuild_max == 2 && st.transfer_wait_ms == 2000 && st.leader_balance_s == 0);
        CHECK(m.set.rebuild_mb_s == 200 && m.set.leader_balance_s == 0);
        struct { int field; uint32_t val; int ok; } b[] = {
            {0, 0, 0}, {0, 1, 1}, {1, 0, 0}, {1, 1, 1}, {1, 16, 1}, {1, 17, 0}, {2, 99, 0}, {2, 100, 1},
            {2, 60000, 1}, {2, 60001, 0}, {3, 0, 1}, {3, 1, 1}, {3, 86400, 1}, {3, 86401, 0},
        };
        for (size_t i = 0; i < sizeof b / sizeof b[0]; i++) {
            meta_settings_default(&st);
            uint32_t *f[4] = {&st.rebuild_mb_s, &st.rebuild_max, &st.transfer_wait_ms, &st.leader_balance_s};
            *f[b[i].field] = b[i].val;
            meta_op_settings(&e, &st);
            CHECK(apply1(&m, &e, res, 8) == META_OK && (res[0] == META_OK) == b[i].ok);
            const uint32_t *got[4] = {&m.set.rebuild_mb_s, &m.set.rebuild_max, &m.set.transfer_wait_ms,
                                      &m.set.leader_balance_s};
            CHECK(!b[i].ok || *got[b[i].field] == b[i].val);
        }
        meta_settings_default(&st);
        st.leader_balance_s = 30;
        st.rebuild_max = 5;
        meta_op_settings(&e, &st);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && m.set.leader_balance_s == 30 &&
              m.set.rebuild_max == 5);
    }

    /* the snapshot, version 5: every new field back; an image of version 4 is refused, and its version is named */
    {
        meta_op_replica_add(&e, 2, 3, 1, 7100, 2); /* a pending move in the image */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        uint8_t *img;
        size_t len;
        meta_snapshot_encode(&m, &img, &len);
        CHECK(meta_snapshot_version(img, len) == META_SNAP_VERSION && META_SNAP_VERSION == 5);
        struct meta_state back;
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&m));
        const struct meta_volume *bv = meta_volume(&back, 2);
        CHECK(bv && bv->set_version == 4 && bv->rep[3].replaces == 2 && bv->grant_target == 4 &&
              bv->grant_min_end == 14 && back.set.leader_balance_s == 30 && back.set.rebuild_max == 5 &&
              back.set.transfer_wait_ms == 2000 && back.set.rebuild_mb_s == 200);
        uint32_t four = 4, crc;
        memcpy(img + 4, &four, 4);
        crc = iso_crc32c(0, img, len - 4);
        memcpy(img + len - 4, &crc, 4);
        uint64_t h = meta_hash(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_ERR_FORMAT && meta_hash(&back) == h);
        CHECK(meta_snapshot_version(img, len) == 4);
        img[10] ^= 1; /* damage: no version at all */
        CHECK(meta_snapshot_version(img, len) == 0);
        meta_free(&back);
        free(img);
    }

    /* an entry of a build of version 4 or older (magic "TME1": INIT, REPLICA_ADD and VOLUME_LEADER bodies grew) is
     * refused, and named as older, so that a log read before any image still names both versions */
    {
        meta_op_replica_reset(&e, 2, 3, 3, 7200);
        meta_entry_finish(&e);
        CHECK(e.len >= 8 && !meta_entry_older(e.buf, e.len) && memcmp(e.buf, "TME2", 4) == 0);
        uint64_t h = meta_hash(&m);
        memcpy(e.buf, "TME1", 4);
        CHECK(meta_entry_older(e.buf, e.len) && !meta_entry_older(e.buf, 3));
        CHECK(meta_apply(&m, m.applied + 1, 1, e.buf, e.len, res, 8) == META_ERR_FORMAT && meta_hash(&m) == h);
        meta_entry_free(&e);
        meta_entry_init(&e);
    }

    /* a shared volume has no moves and no resets */
    {
        struct meta_state sm;
        init_cluster(&sm, &e, META_MODE_SHARED, 4);
        uint32_t ns[3] = {1, 2, 3};
        vol_add(&e, META_VOL_SHARED, 0, "s0", 1, ns, 3);
        CHECK(apply1(&sm, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_replica_add(&e, 0, 1, 4, 0, 2);
        meta_op_replica_reset(&e, 0, 1, 2, 5000);
        meta_op_replica_add(&e, 0, 1, 4, 0, 0); /* a plain candidate: the set's version moves */
        CHECK(apply1(&sm, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
              res[2] == META_OK && meta_volume(&sm, 0)->set_version == 2);
        /* nor a planned grant's target: the same grant is refused with one and taken without */
        const struct meta_volume *sv = meta_volume(&sm, 0);
        uint32_t to = sv->leader == 3 ? 2 : 3;
        struct meta_grant g = sgrant(sv, to, sv->epoch + 2, META_GR_PLANNED);
        g.target = to;
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&sm, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
        g.target = 0;
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&sm, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&sm, 0)->leader == to &&
              !meta_volume(&sm, 0)->grant_target);
        meta_free(&sm);
    }
    meta_entry_free(&e);
    meta_free(&m);
}

static void test_shared(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[16];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_SHARED, 4);
    uint32_t nodes[3] = {1, 2, 3};
    vol_add(&e, META_VOL_SHARED, 0, "s0", 1, nodes, 3);
    vol_add(&e, META_VOL_REPLICATED, 1, "r1", 1, nodes, 3); /* the other kind: refused */
    uint32_t w[2] = {1, 99};
    vol_add(&e, META_VOL_SHARED, 2, "s2", 1, w, 2); /* no such candidate */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_INVALID);
    const struct meta_volume *v = meta_volume(&m, 0);
    CHECK(v->kind == META_VOL_SHARED && v->epoch == 1 && v->leader == 1 && v->nisr == 3 && v->nhist == 0 &&
          !strcmp(v->st.path, "/mnt/s0") && !strcmp(v->st.export, "nfs0:/pool/s0") && !strcmp(v->st.fstype, "nfs4") &&
          meta_reserved_epoch(v) == 2);
    CHECK(meta_check_step(&m, &m, why, sizeof why) == 0);

    /* ISR_CHANGE is refused; VOLUME_RESERVE names the epoch exactly, ids never go down, positions are copied */
    uint32_t isr1[1] = {1};
    isr_chg(&e, meta_volume(&m, 0), 1, 1, isr1, 1, 0);
    struct meta_positions pos = {.wal_seg = 4, .wal_off = 9000, .ledger_gen = 2, .ledger_off = 77, .series_len = 5};
    meta_op_volume_reserve(&e, 0, 1, 64, 32, &pos);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->series_reserved == 64 && v->states_reserved == 32 && v->pos.wal_off == 9000 && v->pos.series_len == 5);
    meta_copy(&prev, &m);
    meta_op_volume_reserve(&e, 0, 1, 63, 32, &pos); /* lower: a late block */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_volume_reserve(&e, 0, 2, 128, 32, &pos); /* the epoch it does not have yet */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    pos.wal_off = 1; /* positions are reports: a lower one is copied, not refused (a pool rolled back, F7) */
    meta_op_volume_reserve(&e, 0, 1, 64, 32, &pos);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->pos.wal_off == 1);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* VOLUME_SELF(W, E + 1): conditional on the owner and the epoch; then E + 2 is W's reserved epoch */
    meta_op_volume_self(&e, 0, 2, 2); /* not the owner */
    meta_op_volume_self(&e, 0, 1, 3); /* not epoch + 1 */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE && res[1] == META_ERR_STALE);
    meta_copy(&prev, &m);
    meta_op_volume_self(&e, 0, 1, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->epoch == 2 && v->leader == 1 && v->nhist == 0 && meta_reserved_epoch(v) == 3);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_op_volume_reserve(&e, 0, 1, 128, 32, &pos); /* the old epoch: the owner proposes again at E + 1 */
    meta_op_volume_drop(&e, 0, 1, 1, 5000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE && res[1] == META_ERR_STALE);
    meta_op_volume_drop(&e, 0, 2, 1, 5000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_drop(&e, 0, 2, 0, 5000); /* the watermark's generation and time never go down */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_volume_drop(&e, 0, 2, 1, 4000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    v = meta_volume(&m, 0);
    CHECK(v->wm_gen == 1 && v->dropped_before == 5000);
    meta_free(&prev);

    /* a grant to another node goes to e + 2 at least (e + 1 is the owner's) and above the highest epoch seen */
    v = meta_volume(&m, 0);
    struct meta_grant g = sgrant(meta_volume(&m, 0), 2, v->epoch + 1, META_GR_UNHEARD);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    v = meta_volume(&m, 0); /* an apply replaces the state: the pointer from before it is freed */
    g = sgrant(meta_volume(&m, 0), 2, v->epoch + 2, META_GR_UNHEARD);
    g.pos = (struct meta_positions){.wal_seg = 7};
    meta_copy(&prev, &m);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->epoch == 4 && v->leader == 2 && v->pos.wal_seg == 7 && v->isr_version == 1 && v->nisr == 3);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    /* the checker catches a new owner at the epoch reserved for the old one */
    struct meta_state bent;
    meta_copy(&bent, &prev);
    bent.volumes[0].leader = 2;
    bent.volumes[0].epoch = prev.volumes[0].epoch + 1;
    CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
    meta_free(&bent);
    meta_free(&prev);

    /* CLAIM_REFUSED(E, highest seen): the next grant goes above it, to the same node or another */
    meta_op_claim_refused(&e, 0, 3, 9); /* not the epoch the claimant holds */
    meta_op_claim_refused(&e, 0, 4, 9);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    CHECK(meta_volume(&m, 0)->highest_seen == 9);
    meta_op_claim_refused(&e, 0, 4, 5); /* never goes down */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->highest_seen == 9);
    v = meta_volume(&m, 0);
    g = sgrant(meta_volume(&m, 0), 2, 9, META_GR_REFUSED); /* the same node, at or below highest seen */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    g = sgrant(meta_volume(&m, 0), 2, 10, META_GR_REFUSED);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->epoch == 10 && v->leader == 2);
    g = sgrant(meta_volume(&m, 0), 3, 11, META_GR_PLANNED); /* another node at e + 1: no */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    g = sgrant(meta_volume(&m, 0), 3, 12, META_GR_PLANNED);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->leader == 3);

    /* CLAIM_FAILED bars the claimant from grants until another candidate is not barred or its flags clear it */
    v = meta_volume(&m, 0);
    meta_op_claim_failed(&e, 0, 11, 1); /* stale epoch */
    meta_op_claim_failed(&e, 0, v->epoch, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE && res[1] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(meta_replica(v, 3)->claim_blocked && !meta_replica(v, 1)->claim_blocked);
    g = sgrant(meta_volume(&m, 0), 3, v->epoch + 1, META_GR_CLAIMANT); /* back to the barred node while others are fine: refused */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_BLOCKED);
    v = meta_volume(&m, 0);
    g = sgrant(meta_volume(&m, 0), 1, v->epoch + 2, META_GR_CLAIMANT);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    /* the barred bit stays across the leader change, in the replicated state, not in the leader's memory */
    {
        uint8_t *img;
        size_t len;
        struct meta_state back;
        meta_snapshot_encode(&m, &img, &len);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_replica(meta_volume(&back, 0), 3)->claim_blocked);
        meta_free(&back);
        free(img);
    }
    v = meta_volume(&m, 0);
    /* flags that change nothing about claims do not lift the bar (ha-s1 review 6: any flags used to), nor does a
     * voter_ok loss; a claim_ok 0 -> 1 does */
    meta_op_member_flags(&e, 3, 0, META_MF_ALL);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_replica(meta_volume(&m, 0), 3)->claim_blocked);
    meta_op_member_flags(&e, 3, 0, META_MF_VOLUMES_OK | META_MF_CLAIM_OK); /* its state disk filled */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_replica(meta_volume(&m, 0), 3)->claim_blocked);
    meta_op_member_flags(&e, 3, 0, META_MF_VOLUMES_OK); /* its claims fail */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_replica(meta_volume(&m, 0), 3)->claim_blocked);
    meta_op_member_flags(&e, 3, 0, META_MF_ALL); /* node 3 is well again */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(!meta_replica(meta_volume(&m, 0), 3)->claim_blocked);
    /* when every candidate is barred a grant goes to one of them anyway */
    for (uint32_t n = 1; n <= 3; n++) {
        v = meta_volume(&m, 0);
        struct meta_grant gg = sgrant(meta_volume(&m, 0), n, v->epoch + 2, META_GR_CLAIMANT);
        if (n != v->leader) {
            meta_op_volume_leader(&e, &gg);
            CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        }
        v = meta_volume(&m, 0);
        meta_op_claim_failed(&e, 0, v->epoch, 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    }
    v = meta_volume(&m, 0);
    CHECK(meta_replica(v, 1)->claim_blocked && meta_replica(v, 2)->claim_blocked && meta_replica(v, 3)->claim_blocked);
    {
        uint32_t other = v->leader == 1 ? 2 : 1;
        g = sgrant(meta_volume(&m, 0), other, v->epoch + 2, META_GR_CLAIMANT);
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->leader == other);
    }

    /* the epoch bound (D10: 2^22, what the engine takes): a grant whose reservation would reach 2^22 - 1 is refused
     * with its own code; VOLUME_SELF may reach 2^22 - 2 and no further */
    {
        uint32_t n3[3] = {1, 2, 3};
        vol_add(&e, META_VOL_SHARED, 1, "s1", 1, n3, 3); /* its own volume: the bound is far from volume 0's epochs */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    }
    v = meta_volume(&m, 1);
    uint32_t someone = v->leader == 1 ? 2 : 1;
    g = sgrant(v, someone, META_EPOCH_LIMIT - 1, META_GR_PLANNED); /* its reservation would be 2^22 - 1 */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_RANGE);
    v = meta_volume(&m, 1);
    g = sgrant(v, someone, UINT64_MAX, META_GR_PLANNED); /* and no wrap-around */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_RANGE);
    v = meta_volume(&m, 1);
    g = sgrant(v, someone, META_EPOCH_LIMIT - 2, META_GR_PLANNED);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 1);
    CHECK(v->epoch == META_EPOCH_LIMIT - 2 && meta_reserved_epoch(v) == META_EPOCH_LIMIT - 1);
    meta_op_volume_self(&e, 1, v->leader, v->epoch + 1); /* 2^22 - 2: the last legal epoch */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 1);
    CHECK(v->epoch == META_EPOCH_LIMIT - 1);
    meta_op_volume_self(&e, 1, v->leader, v->epoch + 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_RANGE);
    CHECK(!strcmp(meta_strerror(META_ERR_RANGE), "the volume's epoch space is used up"));

    /* VOLUME_RELOCATE rewrites the export at the epoch only; candidates move with REPLICA_ADD / REMOVE, in the ISR */
    v = meta_volume(&m, 0);
    meta_op_volume_relocate(&e, 0, v->epoch - 1, "nfs9:/pool/s0");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    v = meta_volume(&m, 0);
    meta_op_volume_relocate(&e, 0, v->epoch, "nfs9:/pool/s0");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK &&
          !strcmp(meta_volume(&m, 0)->st.export, "nfs9:/pool/s0") && !strcmp(meta_volume(&m, 0)->st.path, "/mnt/s0"));
    v = meta_volume(&m, 0);
    meta_op_replica_add(&e, 0, v->epoch, 4, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->nrep == 4 && v->nisr == 4 && !v->rep[3].learner);
    meta_op_replica_remove(&e, 0, v->epoch, v->leader); /* never the owner */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    v = meta_volume(&m, 0);
    meta_op_replica_remove(&e, 0, v->epoch, 4);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->nisr == 3);
    meta_copy(&prev, &m);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* failover = attested: a grant away from a node that is not heard needs an attestation; bmc a power-off */
    struct meta_settings s;
    meta_settings_default(&s);
    /* settings below what the replication driver can act on are refused (r3d-isr review M5) */
    s.isr_lag_ms = META_ISR_LAG_MIN_MS - 1;
    meta_op_settings(&e, &s);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    s.isr_lag_ms = META_ISR_LAG_MIN_MS;
    s.idle_seal_ms = META_IDLE_SEAL_MIN_MS - 1;
    meta_op_settings(&e, &s);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_settings_default(&s);
    s.failover = META_FAILOVER_ATTESTED;
    meta_op_settings(&e, &s);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    someone = v->leader == 1 ? 2 : 1;
    g = sgrant(meta_volume(&m, 0), someone, v->epoch + 2, META_GR_UNHEARD);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_ATTEST);
    g.reason = META_GR_FENCED;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_ATTEST);
    /* the reason is the proposer's label: every other move to another node needs it too (ha-s1 review 3) */
    for (int rsn = META_GR_UNHEARD; rsn <= META_GR_LAST; rsn++) {
        if (rsn == META_GR_PLANNED || rsn == META_GR_REFUSED)
            continue;
        g.reason = (uint8_t)rsn;
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&m, &e, res, 8) == META_OK &&
              res[0] == (rsn == META_GR_RESIGNED ? META_ERR_INVALID : META_ERR_ATTEST)); /* shared: no resigning */
    }
    g.reason = META_GR_UNHEARD; /* kind NONE with a name and a time is no attestation */
    snprintf(g.att.who, sizeof g.att.who, "olav");
    g.att.ms = 1234;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_ATTEST);
    g.reason = META_GR_RESIGNED; /* not even with a full attestation */
    g.att.kind = META_ATT_OPERATOR;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    memset(&g.att, 0, sizeof g.att);
    g.reason = META_GR_PLANNED; /* a planned move of a live owner needs none */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    someone = v->leader == 1 ? 2 : 1;
    g = sgrant(meta_volume(&m, 0), someone, v->epoch + 2, META_GR_REFUSED); /* a claimant refused: it never held the lock */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    someone = v->leader == 1 ? 2 : 1;
    g = sgrant(meta_volume(&m, 0), someone, v->epoch + 2, META_GR_UNHEARD);
    g.att.kind = META_ATT_OPERATOR;
    snprintf(g.att.who, sizeof g.att.who, "olav");
    g.att.ms = 1234;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    s.failover = META_FAILOVER_BMC;
    meta_op_settings(&e, &s);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    someone = v->leader == 1 ? 2 : 1;
    g = sgrant(meta_volume(&m, 0), someone, v->epoch + 2, META_GR_UNHEARD);
    g.att.kind = META_ATT_OPERATOR; /* an operator's word is not a power-off */
    snprintf(g.att.who, sizeof g.att.who, "olav");
    g.att.ms = 1234;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_ATTEST);
    g.att.kind = META_ATT_BMC;
    g.att.incarnation = 0; /* a member's incarnation is 0 here: the power-off covers it */
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_member_state(&e, 1, META_UP, 5);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    if (v->leader != 1) {
        g = sgrant(meta_volume(&m, 0), 1, v->epoch + 2, META_GR_PLANNED);
        meta_op_volume_leader(&e, &g);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        v = meta_volume(&m, 0);
    }
    someone = 2;
    g = sgrant(meta_volume(&m, 0), someone, v->epoch + 2, META_GR_FENCED); /* the owner (1) came back as incarnation 5: a power-off of
                                                           * an older one proves nothing */
    g.att.kind = META_ATT_BMC;
    snprintf(g.att.who, sizeof g.att.who, "bmc1");
    g.att.ms = 99;
    g.att.incarnation = 4;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_ATTEST);
    g.att.incarnation = 5;
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);

    /* the owner is granted its own volume at an epoch it holds already: refused (it would not raise the epoch) */
    v = meta_volume(&m, 0);
    g = sgrant(v, v->leader, v->epoch, META_GR_PLANNED);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    /* never to a member that is out */
    v = meta_volume(&m, 0);
    uint32_t gone = v->rep[0].node == v->leader ? v->rep[1].node : v->rep[0].node;
    meta_op_member_state(&e, gone, META_OUT, 9);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    g = sgrant(v, gone, v->epoch + 2, META_GR_PLANNED);
    meta_op_volume_leader(&e, &g);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);

    /* the checker alone (the simulator has no unit test of its own for these): a grant to a member that is out, and a
     * grant at or below an epoch a claimant reported, are failures of the step */
    {
        struct meta_state pv, nx;
        meta_copy(&pv, &m);
        meta_copy(&nx, &m);
        struct meta_volume *nv = (struct meta_volume *)meta_volume(&nx, 0);
        nv->leader = gone;
        nv->epoch += 2;
        CHECK(meta_check_step(&pv, &nx, why, sizeof why) == -1);
        meta_free(&nx);
        meta_copy(&nx, &m);
        nv = (struct meta_volume *)meta_volume(&nx, 0);
        ((struct meta_volume *)meta_volume(&pv, 0))->highest_seen = nv->epoch + 5;
        ((struct meta_volume *)meta_volume(&nx, 0))->highest_seen = nv->epoch + 5;
        nv->epoch += 2; /* the owner's own grant, below what was seen */
        CHECK(meta_check_step(&pv, &nx, why, sizeof why) == -1);
        meta_free(&nx);
        meta_free(&pv);
    }

    /* a snapshot of all this round-trips, byte for byte in hash */
    {
        uint8_t *img;
        size_t len;
        struct meta_state back;
        meta_snapshot_encode(&m, &img, &len);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&m));
        meta_free(&back);
        free(img);
    }
    meta_entry_free(&e);
    meta_free(&m);
}

/* ================================================================ the copied core under its own simulator */

static uint32_t blob_uid(const struct meta_user *u)
{
    uint32_t v;
    memcpy(&v, u->blob, 4);
    return v;
}

/* users (S2-1): puts conditional on the version, uids from the state machine, never reused, the snapshot */
static void test_users(void)
{
    struct meta_state m, prev, back;
    struct meta_entry e;
    int res[8];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 3);

    /* two creates whose proposers both picked uid 7 from their copies: the state machine assigns 1 and 2 */
    uint8_t b7[8] = {7, 0, 0, 0, 'v', 'e', 'r', 'f'};
    meta_copy(&prev, &m);
    meta_op_user_put(&e, "alice", 0, b7, 8);
    meta_op_user_put(&e, "bob", 0, b7, 8);
    meta_op_user_put(&e, "carol", 0, b7, 3);  /* shorter than the uid */
    meta_op_user_put(&e, "", 0, b7, 8);       /* no name */
    meta_op_user_put(&e, "dave", 5, b7, 8);   /* an update of a user that does not exist */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && res[4] == META_ERR_NOTFOUND);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    const struct meta_user *a = meta_user(&m, "alice"), *b = meta_user(&m, "bob");
    CHECK(a && b && a->uid == 1 && b->uid == 2 && m.max_uid == 2 && blob_uid(a) == 1 && blob_uid(b) == 2);
    CHECK(a->version == m.applied && b->version == m.applied && a->len == 8 && !memcmp(a->blob + 4, "verf", 4));
    CHECK(!meta_user(&m, "carol") && !meta_user(&m, "dave") && m.nusers == 2);
    uint64_t va = a->version;

    /* a name of 128 bytes (USERS allows them); 129 does not fit */
    char longname[130];
    memset(longname, 'n', 128);
    longname[128] = 0;
    meta_op_user_put(&e, longname, 0, b7, 8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_user(&m, longname) && meta_user(&m, longname)->uid == 3);

    /* an update keeps the uid; one with an old version, or a create of a taken name, is refused */
    meta_copy(&prev, &m);
    meta_op_user_put(&e, "alice", va, "\0\0\0\0passwd2", 11);
    meta_op_user_put(&e, "alice", va, "\0\0\0\0revoked", 11); /* the same version: stale after the first */
    meta_op_user_put(&e, "bob", 0, b7, 8);
    meta_op_user_put(&e, "bob", va + 1000, b7, 8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE &&
          res[2] == META_ERR_EXISTS && res[3] == META_ERR_STALE);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    a = meta_user(&m, "alice");
    CHECK(a->uid == 1 && blob_uid(a) == 1 && a->version == m.applied && a->version > va && a->len == 11 &&
          !memcmp(a->blob + 4, "passwd2", 7));

    /* a drop; the name made again gets a new uid, never the old one, and max_uid stays */
    meta_op_user_del(&e, "bob");
    meta_op_user_del(&e, "bob");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_NOTFOUND);
    CHECK(!meta_user(&m, "bob") && m.max_uid == 3);
    meta_op_user_put(&e, longname, meta_user(&m, longname)->version, b7, 8);
    meta_op_user_del(&e, longname);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && m.max_uid == 3);
    meta_copy(&prev, &m);
    meta_op_user_put(&e, "bob", 0, b7, 8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    CHECK(meta_user(&m, "bob")->uid == 4 && m.max_uid == 4 && blob_uid(meta_user(&m, "bob")) == 4);
    /* the checker: a uid given out again, max_uid down, a version that moved without a put */
    {
        struct meta_state bent;
        meta_copy(&bent, &m);
        bent.max_uid = 2;
        CHECK(meta_check_step(&m, &bent, why, sizeof why) != 0);
        meta_free(&bent);
        meta_copy(&bent, &m);
        struct meta_user *u = (struct meta_user *)meta_user(&bent, "bob");
        u->uid = 2;
        CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0); /* also: the blob says 4 */
        meta_free(&bent);
    }
    meta_free(&prev);

    /* a 1 MiB blob and max_uid through a snapshot; one byte more does not decode */
    uint8_t *big = malloc(META_USER_BLOB_MAX + 1);
    for (uint32_t i = 0; i < META_USER_BLOB_MAX + 1; i++)
        big[i] = (uint8_t)(i * 31 + 7);
    meta_op_user_put(&e, "zed", 0, big, META_USER_BLOB_MAX);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_user_put(&e, "zz", 0, big, META_USER_BLOB_MAX + 1);
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
    meta_op_user_del(&e, "alice"); /* max_uid (5) stays above every live uid */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    uint8_t *img;
    size_t len;
    CHECK(meta_snapshot_encode(&m, &img, &len) == 0);
    meta_init(&back);
    CHECK(meta_snapshot_decode(&back, img, len) == META_OK);
    CHECK(back.max_uid == 5 && back.nusers == m.nusers && meta_hash(&back) == meta_hash(&m));
    const struct meta_user *z = meta_user(&back, "zed");
    CHECK(z && z->uid == 5 && z->len == META_USER_BLOB_MAX && blob_uid(z) == 5 && !memcmp(z->blob + 4, big + 4, z->len - 4));
    CHECK(z->version == meta_user(&m, "zed")->version);
    meta_free(&back);
    free(img);
    /* a copy shares blobs; freeing either leaves the other whole */
    meta_copy(&back, &m);
    meta_free(&m);
    z = meta_user(&back, "zed");
    CHECK(z && z->len == META_USER_BLOB_MAX && !memcmp(z->blob + 4, big + 4, 64));
    meta_op_user_del(&e, "zed");
    CHECK(apply1(&back, &e, res, 8) == META_OK && res[0] == META_OK && !meta_user(&back, "zed") && back.max_uid == 5);
    meta_op_user_put(&e, "zed", 0, b7, 8);
    CHECK(apply1(&back, &e, res, 8) == META_OK && meta_user(&back, "zed")->uid == 6);
    /* the actor guard (tsdbd's admin ops, checked on a copy that may lag): a change applies only while its actor has
     * the version it was checked at, so a demotion or drop of the actor committed first wins (mutant noactorguard) */
    {
        uint64_t zv = meta_user(&back, "zed")->version;
        meta_op_user_put_as(&e, "carl", 0, b7, 8, "zed", zv);        /* zed as checked: applies */
        meta_op_user_put(&e, "zed", zv, "\0\0\0\0demoted", 11);      /* zed demoted */
        meta_op_user_put_as(&e, "dora", 0, b7, 8, "zed", zv);        /* checked before the demotion */
        meta_op_user_del_as(&e, "carl", "zed", zv);
        meta_op_user_put_as(&e, "erin", 0, b7, 8, "nobody", zv);     /* an actor dropped */
        meta_op_user_del_as(&e, "nosuch", "zed", zv);
        CHECK(apply1(&back, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK &&
              res[2] == META_ERR_STALE && res[3] == META_ERR_STALE && res[4] == META_ERR_STALE &&
              res[5] == META_ERR_NOTFOUND);
        CHECK(meta_user(&back, "carl") && !meta_user(&back, "dora") && !meta_user(&back, "erin"));
        uint64_t z2 = meta_user(&back, "zed")->version;
        meta_op_user_del_as(&e, "carl", "zed", z2);
        CHECK(apply1(&back, &e, res, 8) == META_OK && res[0] == META_OK && !meta_user(&back, "carl"));
    }
    free(big);
    meta_free(&back);
    meta_entry_free(&e);
}

/* An image of a state with user `name`, patched at `at` bytes past the name (u32 uid, u64 version, u32 len, then the
 * blob, its uid first) with `val` (also into the blob's uid when `blob_too`), its crc fixed: what the decode says */
static int users_patch(const uint8_t *img, size_t len, const char *name, size_t at, const void *val, size_t vl,
                       int blob_too)
{
    uint8_t *p = malloc(len), *hit = NULL;
    memcpy(p, img, len);
    size_t nl = strlen(name);
    for (size_t i = 2; i + nl + 20 <= len && !hit; i++)
        if (p[i - 2] == (uint8_t)nl && p[i - 1] == 0 && !memcmp(p + i, name, nl))
            hit = p + i + nl;
    CHECK(hit != NULL);
    if (hit) {
        memcpy(hit + at, val, vl);
        if (blob_too)
            memcpy(hit + 16, val, 4);
    }
    uint32_t crc = iso_crc32c(0, p, len - 4);
    memcpy(p + len - 4, &crc, 4);
    struct meta_state t;
    meta_init(&t);
    int rc = meta_snapshot_decode(&t, p, len);
    meta_free(&t);
    free(p);
    return rc;
}

/* The users' limits: the snapshot decoder's rules (each broken alone in an image), uids used up, the bytes budget */
static void test_users_limits(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[8];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 3);
    uint8_t b[8] = {0, 0, 0, 0, 'v', 'e', 'r', 'f'};
    meta_op_user_put(&e, "uuA", 0, b, 8);
    meta_op_user_put(&e, "uuB", 0, b, 8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && m.max_uid == 2);
    uint8_t *img;
    size_t len;
    CHECK(meta_snapshot_encode(&m, &img, &len) == 0);
    uint32_t one = 1, two = 2, three = 3;
    uint64_t ahead = m.applied + 1;
    CHECK(users_patch(img, len, "uuB", 0, &two, 4, 1) == META_OK);           /* the patch alone: it decodes */
    CHECK(users_patch(img, len, "uuB", 16, &one, 4, 0) == META_ERR_FORMAT);  /* the blob says uid 1, the record 2 */
    CHECK(users_patch(img, len, "uuB", 0, &one, 4, 1) == META_ERR_FORMAT);   /* uid 1 twice */
    CHECK(users_patch(img, len, "uuB", 4, &ahead, 8, 0) == META_ERR_FORMAT); /* a version past applied */
    CHECK(users_patch(img, len, "uuB", 0, &three, 4, 1) == META_ERR_FORMAT); /* a uid above max_uid */
    free(img);

    /* uids used up: the last one is given out, then RANGE; an update of a user still works */
    m.max_uid = UINT32_MAX - 1;
    meta_op_user_put(&e, "uuC", 0, b, 8);
    meta_op_user_put(&e, "uuD", 0, b, 8);
    meta_op_user_put(&e, "uuA", meta_user(&m, "uuA")->version, b, 8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_RANGE && res[2] == META_OK);
    CHECK(meta_user(&m, "uuC") && meta_user(&m, "uuC")->uid == UINT32_MAX && !meta_user(&m, "uuD"));
    meta_free(&m);

    /* the bytes budget: users of 1 MiB until RANGE; the image stays well inside a peer frame (16 MiB) */
    init_cluster(&m, &e, META_MODE_REPLICATED, 3);
    uint8_t *big = calloc(1, META_USER_BLOB_MAX);
    unsigned made = 0;
    int last = 0;
    for (unsigned i = 0; i < 16 && !last; i++) {
        char nm[16];
        snprintf(nm, sizeof nm, "big%02u", i);
        meta_copy(&prev, &m);
        meta_op_user_put(&e, nm, 0, big, META_USER_BLOB_MAX);
        CHECK(apply1(&m, &e, res, 8) == META_OK);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
        if (res[0] == META_OK)
            made++;
        else
            last = res[0];
    }
    CHECK(last == META_ERR_RANGE && made == META_USERS_BYTES_MAX / (META_USER_BLOB_MAX + 6));
    CHECK(meta_users_bytes(&m) <= META_USERS_BYTES_MAX);
    /* a put that shrinks a user is no new bytes; then a smaller new user fits where a big one did not */
    meta_op_user_put(&e, "big00", meta_user(&m, "big00")->version, b, 8);
    meta_op_user_put(&e, "small", 0, big, META_USER_BLOB_MAX / 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    CHECK(meta_users_bytes(&m) <= META_USERS_BYTES_MAX);
    CHECK(meta_snapshot_encode(&m, &img, &len) == 0 && len < (12u << 20));
    free(img);
    free(big);
    meta_free(&m);
    meta_entry_free(&e);
}

static void test_raft_sim(unsigned seeds, uint64_t first, double seconds)
{
    unsigned bad = 0;
    for (unsigned i = 0; i < seeds; i++) {
        raft_sim_params p;
        raft_sim_params_defaults(&p);
        p.seed = first + i;
        p.voters = i % 2 ? 5 : 3;
        p.learners = 1;
        p.spare = 1;
        p.drop = 0.02;
        p.dup = 0.01;
        p.partition_rate = 0.2;
        p.crash_rate = 0.2;
        p.clock_drift = 0.0005;
        p.propose_rate = 200;
        p.read_rate = 50;
        p.churn_rate = 0.1;
        p.transfer_rate = 0.1;
        p.snapshot_every = 100;
        p.cross_check = true;
        raft_sim_stats st;
        char why[512];
        if (!raft_sim_chaos(&p, (uint64_t)(seconds * 1e9), &st, why, sizeof why)) {
            fprintf(stderr, "FAIL raft_sim seed %llu: %s\n", (unsigned long long)p.seed, why);
            failures++;
            if (++bad > 3)
                break;
        } else if (st.committed == 0) {
            fprintf(stderr, "FAIL raft_sim seed %llu: nothing committed\n", (unsigned long long)p.seed);
            failures++;
        }
    }
}

/* ================================================================ the core's log on files */

static void test_raft_log(const char *base)
{
    char dir[4096];
    snprintf(dir, sizeof dir, "%s/log", base); /* base is this run's own directory (scratch_make) */
    char err[512];
    raft_log *l;
    raft_log_opts o = {0};
    o.segment_bytes = RAFT_LOG_SEGMENT_MIN;
    CHECK(raft_log_open(dir, &o, 0, 0, &l, err, sizeof err) == ISO_OK);
    raft_entry e[200];
    char payload[200][32];
    for (unsigned i = 0; i < 200; i++) {
        snprintf(payload[i], sizeof payload[i], "entry %u", i + 1);
        e[i] = (raft_entry){.index = i + 1, .term = 1 + i / 50, .type = RAFT_ENTRY_NORMAL,
                            .len = (uint32_t)strlen(payload[i]), .data = payload[i]};
    }
    CHECK(raft_log_file_ops.append(l, e, 200) == ISO_OK);
    raft_log_sync s;
    CHECK(raft_log_sync_prepare(l, &s) == ISO_OK);
    CHECK(raft_log_sync_run(&s) == ISO_OK);
    raft_log_durable(l, s.index, s.term);
    raft_hard_state hs = {.term = 4, .vote = 2};
    CHECK(raft_log_hard_save(l, &hs, err, sizeof err) == ISO_OK);
    raft_log_close(l);

    raft_hard_state back;
    CHECK(raft_log_hard_load(dir, &back, err, sizeof err) == ISO_OK && back.term == 4 && back.vote == 2);
    CHECK(raft_log_open(dir, &o, 0, 0, &l, err, sizeof err) == ISO_OK);
    CHECK(raft_log_file_ops.first(l) == 1 && raft_log_file_ops.last(l) == 200 && raft_log_file_ops.term_at(l, 151) == 4);
    raft_entry got[4];
    unsigned n = raft_log_file_ops.get(l, 120, 123, 1 << 20, got, 4);
    CHECK(n == 4 && got[0].len == strlen("entry 120") && !memcmp(got[0].data, "entry 120", got[0].len));
    raft_log_file_ops.release(l);
    CHECK(raft_log_file_ops.truncate(l, 150) == ISO_OK && raft_log_file_ops.last(l) == 149);
    raft_log_close(l);
    /* the files are left in the scratch directory, as the other tests leave theirs */
}

/* The log's damage branches (open: a tear is cut, rot is refused, the durable floor tells them apart; the hard-state ring).
 * The layouts are those in the header comment of src/raft/raft_log.c, which this test does not edit. */
#define DMG_N 40
static char dmg_payload[DMG_N + 1][32];
static uint64_t dmg_off[DMG_N + 2]; /* byte offset of record i in the segment file; [DMG_N + 1] is its end */

/* A log of DMG_N entries in `dir`, synced; with `floor` the durable floor is made to name DMG_N. Returns nothing:
 * the files are the result. */
static void dmg_make(const char *dir, bool floor)
{
    char err[512];
    raft_log *l;
    raft_log_opts o = {0};
    o.segment_bytes = RAFT_LOG_SEGMENT_MIN;
    o.floor_every_ns = 1;
    CHECK(raft_log_open(dir, &o, 0, 0, &l, err, sizeof err) == ISO_OK);
    raft_entry e[DMG_N];
    dmg_off[1] = RAFT_LOG_SEGMENT_HEAD;
    for (unsigned i = 0; i < DMG_N; i++) {
        snprintf(dmg_payload[i + 1], sizeof dmg_payload[i + 1], "entry %u", i + 1);
        e[i] = (raft_entry){.index = i + 1, .term = 1, .type = RAFT_ENTRY_NORMAL,
                            .len = (uint32_t)strlen(dmg_payload[i + 1]), .data = dmg_payload[i + 1]};
        dmg_off[i + 2] = dmg_off[i + 1] + RAFT_LOG_RECORD_HEAD + e[i].len;
    }
    CHECK(raft_log_file_ops.append(l, e, DMG_N) == ISO_OK);
    raft_hard_state hs = {.term = 1, .vote = 1};
    CHECK(raft_log_hard_save(l, &hs, err, sizeof err) == ISO_OK);
    raft_log_sync s;
    CHECK(raft_log_sync_prepare(l, &s) == ISO_OK);
    CHECK(raft_log_sync_run(&s) == ISO_OK);
    raft_log_durable(l, s.index, s.term);
    if (floor) {
        CHECK(raft_log_floor_due(l, DMG_N, 1000000000ull));
        CHECK(raft_log_sync_prepare(l, &s) == ISO_OK);
        CHECK(raft_log_sync_run(&s) == ISO_OK);
        raft_log_durable(l, s.index, s.term);
        raft_term ft;
        CHECK(raft_log_floor(l, &ft) == DMG_N);
    }
    raft_log_close(l);
}

static void dmg_path(char *out, size_t n, const char *dir)
{
    snprintf(out, n, "%s/log-%020d", dir, 1);
}

static off_t dmg_size(const char *dir)
{
    char p[4200];
    struct stat st;
    dmg_path(p, sizeof p, dir);
    CHECK(stat(p, &st) == 0);
    return st.st_size;
}

static void dmg_flip(const char *dir, uint64_t at)
{
    char p[4200];
    dmg_path(p, sizeof p, dir);
    FILE *f = fopen(p, "r+b");
    CHECK(f != NULL);
    uint8_t b = 0;
    CHECK(f && fseek(f, (long)at, SEEK_SET) == 0 && fread(&b, 1, 1, f) == 1);
    b ^= 0x5a;
    CHECK(f && fseek(f, (long)at, SEEK_SET) == 0 && fwrite(&b, 1, 1, f) == 1);
    if (f)
        fclose(f);
}

static void dmg_open_ok(const char *dir, raft_index want_last, uint64_t want_torn, const char *what)
{
    char err[512];
    raft_log *l;
    raft_log_opts o = {0};
    o.segment_bytes = RAFT_LOG_SEGMENT_MIN;
    iso_err rc = raft_log_open(dir, &o, 0, 0, &l, err, sizeof err);
    if (rc != ISO_OK) {
        fprintf(stderr, "%s: open failed (%d): %s\n", what, (int)rc, err);
        failures++;
        return;
    }
    raft_log_stats st;
    raft_log_stat(l, &st);
    if (raft_log_file_ops.last(l) != want_last || st.torn_tail_bytes != want_torn) {
        fprintf(stderr, "%s: last %" PRIu64 " (want %" PRIu64 "), torn bytes %" PRIu64 " (want %" PRIu64 ")\n", what,
                (uint64_t)raft_log_file_ops.last(l), (uint64_t)want_last, st.torn_tail_bytes, want_torn);
        failures++;
    }
    /* what is left reads back whole, and the log takes the next entry */
    raft_entry got[1];
    for (raft_index i = 1; i <= want_last && i <= DMG_N; i++) {
        unsigned n = raft_log_file_ops.get(l, i, i + 1, 1 << 20, got, 1);
        CHECK(n == 1 && got[0].len == strlen(dmg_payload[i]) && !memcmp(got[0].data, dmg_payload[i], got[0].len));
        raft_log_file_ops.release(l);
    }
    raft_entry e = {.index = want_last + 1, .term = 1, .type = RAFT_ENTRY_NORMAL, .len = 3, .data = "new"};
    CHECK(raft_log_file_ops.append(l, &e, 1) == ISO_OK && raft_log_file_ops.last(l) == want_last + 1);
    raft_log_close(l);
}

/* The open refuses: ISO_ERR_CORRUPT, a message that says where, and nothing cut (twice: a restart refuses again) */
static void dmg_open_refused(const char *dir, const char *word, const char *what)
{
    for (int round = 0; round < 2; round++) {
        char err[1024] = "";
        raft_log *l = NULL;
        raft_log_opts o = {0};
        o.segment_bytes = RAFT_LOG_SEGMENT_MIN;
        off_t before = dmg_size(dir);
        iso_err rc = raft_log_open(dir, &o, 0, 0, &l, err, sizeof err);
        if (rc != ISO_ERR_CORRUPT || !strstr(err, word)) {
            fprintf(stderr, "%s (open %d): rc %d, message '%s'; wanted ISO_ERR_CORRUPT naming '%s'\n", what, round,
                    (int)rc, err, word);
            failures++;
            if (rc == ISO_OK)
                raft_log_close(l);
        }
        CHECK(dmg_size(dir) == before); /* a refusal cuts nothing */
    }
}

static void test_raft_log_damage(const char *base)
{
    char dir[4096], seg[4200];
    /* 1. a record cut in two: the tail of the last segment is where a crash leaves one: cut, no more */
    snprintf(dir, sizeof dir, "%s/torn", base);
    dmg_make(dir, false);
    dmg_path(seg, sizeof seg, dir);
    uint64_t cut = dmg_off[DMG_N] + RAFT_LOG_RECORD_HEAD + 3; /* inside the last record's payload */
    CHECK(truncate(seg, (off_t)cut) == 0);
    dmg_open_ok(dir, DMG_N - 1, cut - dmg_off[DMG_N], "torn last record");
    /* 2. zeros after the last record (a file extended and never written) */
    snprintf(dir, sizeof dir, "%s/zeros", base);
    dmg_make(dir, false);
    dmg_path(seg, sizeof seg, dir);
    CHECK(truncate(seg, (off_t)dmg_off[DMG_N + 1] + 100) == 0);
    dmg_open_ok(dir, DMG_N, 100, "zeros after the last record");
    /* 3. no floor yet: a flipped byte in a record in the middle is cut with everything after it (the floor is what
     * tells acknowledged rot from a tear; before it there is none) */
    snprintf(dir, sizeof dir, "%s/nofloor", base);
    dmg_make(dir, false);
    dmg_flip(dir, dmg_off[21] + RAFT_LOG_RECORD_HEAD + 2);
    dmg_open_ok(dir, 20, dmg_off[DMG_N + 1] - dmg_off[21], "rot above no floor");
    /* 4. the floor names the end: the same flip is rot in what was acknowledged, refused, nothing cut */
    snprintf(dir, sizeof dir, "%s/floor", base);
    dmg_make(dir, true);
    dmg_flip(dir, dmg_off[21] + RAFT_LOG_RECORD_HEAD + 2);
    dmg_open_refused(dir, "21", "rot at or below the durable floor");
    /* ... and so is a record cut in two at or below it */
    snprintf(dir, sizeof dir, "%s/floortorn", base);
    dmg_make(dir, true);
    dmg_path(seg, sizeof seg, dir);
    CHECK(truncate(seg, (off_t)(dmg_off[DMG_N] + RAFT_LOG_RECORD_HEAD + 3)) == 0);
    dmg_open_refused(dir, "40", "a torn record at the durable floor");
    /* 5. the header of the only segment damaged: refused whatever else holds */
    snprintf(dir, sizeof dir, "%s/header", base);
    dmg_make(dir, false);
    dmg_flip(dir, 20);
    dmg_open_refused(dir, "log-", "a damaged segment header");
}

/* The hard state's two slots (4096 bytes each; 0 magic[8], 8 crc, 12 seq, 20 term, 28 vote): a slot a save tore is
 * forgiven beside a valid one, a damaged one whose magic is whole is not, a zeroed one beside a later sequence is not. */
static void test_hard_state_damage(const char *base)
{
    char dir[4096], hard[4200], err[512];
    raft_hard_state hs, back;
    raft_log *l;
    raft_log_opts o = {0};
    o.segment_bytes = RAFT_LOG_SEGMENT_MIN;
    for (int variant = 0; variant < 4; variant++) {
        snprintf(dir, sizeof dir, "%s/hard%d", base, variant);
        CHECK(raft_log_open(dir, &o, 0, 0, &l, err, sizeof err) == ISO_OK);
        for (unsigned t = 1; t <= 3; t++) { /* three saves: sequences 1 to 3, in both slots */
            hs = (raft_hard_state){.term = 10 + t, .vote = t};
            CHECK(raft_log_hard_save(l, &hs, err, sizeof err) == ISO_OK);
        }
        raft_log_close(l);
        snprintf(hard, sizeof hard, "%s/hard", dir);
        CHECK(raft_log_hard_load(dir, &back, err, sizeof err) == ISO_OK && back.term == 13 && back.vote == 3);
        FILE *f = fopen(hard, "r+b");
        CHECK(f != NULL);
        if (!f)
            return;
        uint8_t slot[2][8192 / 2];
        CHECK(fread(slot[0], 1, 4096, f) == 4096 && fread(slot[1], 1, 4096, f) == 4096);
        uint64_t seq[2];
        memcpy(&seq[0], slot[0] + 12, 8);
        memcpy(&seq[1], slot[1] + 12, 8);
        int newer = seq[1] > seq[0], older = !newer;
        CHECK(seq[newer] == 3 && seq[older] == 2);
        long at = 0;
        const char *label = "";
        switch (variant) {
        case 0: /* the older slot's magic broken: a torn save beside a valid slot, forgiven */
            at = older * 4096 + 0;
            label = "torn older slot";
            break;
        case 1: /* the newest slot's magic broken: forgiven, the older state is what is read */
            at = newer * 4096 + 0;
            label = "torn newest slot";
            break;
        case 2: /* a slot whose magic is whole and whose checksum is not: damage, refused */
            at = newer * 4096 + 20;
            label = "damaged newest slot";
            break;
        default: /* the older slot zeroed beside sequence 3: refused (only the first save leaves a blank) */
            at = -1;
            label = "zeroed older slot";
        }
        if (at >= 0) {
            CHECK(fseek(f, at, SEEK_SET) == 0);
            uint8_t b = slot[at / 4096][at % 4096] ^ 0x5a;
            CHECK(fwrite(&b, 1, 1, f) == 1);
        } else {
            uint8_t zeros[4096] = {0};
            CHECK(fseek(f, older * 4096, SEEK_SET) == 0 && fwrite(zeros, 1, 4096, f) == 4096);
        }
        fclose(f);
        iso_err rc = raft_log_hard_load(dir, &back, err, sizeof err);
        if (variant <= 1) {
            raft_term want = variant == 0 ? 13 : 12;
            if (rc != ISO_OK || back.term != want) {
                fprintf(stderr, "%s: load rc %d term %" PRIu64 " (want ok, term %" PRIu64 "): %s\n", label, (int)rc,
                        (uint64_t)back.term, (uint64_t)want, err);
                failures++;
            }
        } else if (rc != ISO_ERR_CORRUPT || !strstr(err, "hard state")) {
            fprintf(stderr, "%s: load rc %d, message '%s'; wanted ISO_ERR_CORRUPT naming the hard state\n", label,
                    (int)rc, err);
            failures++;
        }
        /* the log's own open reads the ring too and agrees */
        err[0] = 0;
        l = NULL;
        rc = raft_log_open(dir, &o, 0, 0, &l, err, sizeof err);
        if ((variant <= 1) != (rc == ISO_OK)) {
            fprintf(stderr, "%s: open rc %d: %s\n", label, (int)rc, err);
            failures++;
        }
        if (rc == ISO_OK)
            raft_log_close(l);
    }
}

/* ================================================================ the metadata simulator */

#define SIM_NODES_MAX 5
#define SIM_VOLS 6

struct mentry {
    raft_term term;
    uint8_t type;
    uint32_t len;
    uint8_t *data;
};

struct mlog {
    raft_index base;
    raft_term base_term;
    struct mentry *e;
    size_t n, cap;
    raft_index durable;
    uint8_t **grave; /* payloads dropped while the core may still name them; freed at release() */
    size_t ngrave, gravecap;
};

static void bury(struct mlog *l, uint8_t *p)
{
    if (l->ngrave == l->gravecap) {
        l->gravecap = l->gravecap ? 2 * l->gravecap : 64;
        l->grave = realloc(l->grave, l->gravecap * sizeof *l->grave);
    }
    l->grave[l->ngrave++] = p;
}

static raft_index ml_first(void *p) { return ((struct mlog *)p)->base + 1; }
static raft_index ml_last(void *p) { struct mlog *l = p; return l->base + l->n; }

static raft_term ml_term_at(void *p, raft_index i)
{
    struct mlog *l = p;
    if (i == l->base)
        return l->base_term;
    if (i < l->base || i > l->base + l->n)
        return 0;
    return l->e[i - l->base - 1].term;
}

static unsigned ml_get(void *p, raft_index lo, raft_index hi, size_t max_bytes, raft_entry *out, unsigned max_n)
{
    struct mlog *l = p;
    unsigned n = 0;
    size_t bytes = 0;
    for (raft_index i = lo; i <= hi && i > l->base && i <= l->base + l->n && n < max_n; i++) {
        const struct mentry *m = &l->e[i - l->base - 1];
        if (n && bytes + m->len > max_bytes)
            break;
        out[n++] = (raft_entry){.index = i, .term = m->term, .type = m->type, .len = m->len, .data = m->data};
        bytes += m->len;
    }
    return n;
}

static iso_err ml_append(void *p, const raft_entry *e, unsigned n)
{
    struct mlog *l = p;
    for (unsigned k = 0; k < n; k++) {
        if (e[k].index != l->base + l->n + 1)
            return ISO_ERR_INVAL;
        if (l->n == l->cap) {
            l->cap = l->cap ? 2 * l->cap : 64;
            l->e = realloc(l->e, l->cap * sizeof *l->e);
        }
        uint8_t *d = malloc(e[k].len + 1);
        if (e[k].len) /* an empty entry's data may be null */
            memcpy(d, e[k].data, e[k].len);
        l->e[l->n++] = (struct mentry){e[k].term, e[k].type, e[k].len, d};
    }
    return ISO_OK;
}

static iso_err ml_truncate(void *p, raft_index from)
{
    struct mlog *l = p;
    while (l->base + l->n >= from && l->n)
        bury(l, l->e[--l->n].data);
    if (l->durable > l->base + l->n)
        l->durable = l->base + l->n;
    return ISO_OK;
}

static iso_err ml_reset(void *p, raft_index index, raft_term term)
{
    struct mlog *l = p;
    while (l->n)
        bury(l, l->e[--l->n].data);
    l->base = index;
    l->base_term = term;
    l->durable = index;
    return ISO_OK;
}

static iso_err ml_compact(void *p, raft_index upto)
{
    struct mlog *l = p;
    if (upto <= l->base)
        return ISO_OK;
    if (upto > l->base + l->n)
        upto = l->base + l->n;
    size_t k = upto - l->base;
    l->base_term = l->e[k - 1].term;
    for (size_t i = 0; i < k; i++)
        bury(l, l->e[i].data);
    memmove(l->e, l->e + k, (l->n - k) * sizeof *l->e);
    l->n -= k;
    l->base = upto;
    return ISO_OK;
}

static void ml_release(void *p)
{
    struct mlog *l = p;
    for (size_t i = 0; i < l->ngrave; i++)
        free(l->grave[i]);
    l->ngrave = 0;
}

/* a deep copy of the durable part of `src` (what a crash leaves) in dst, with its first `keep` entries */
static void ml_clone_durable(struct mlog *dst, const struct mlog *src, size_t keep)
{
    for (size_t i = 0; i < dst->n; i++)
        free(dst->e[i].data);
    free(dst->e);
    /* payloads dropped earlier stay in dst's grave (the core may still name them until release()): resetting the
     * struct would leak them and the grave itself (LeakSanitizer, has1-asan2) */
    *dst = (struct mlog){.base = src->base, .base_term = src->base_term,
                         .grave = dst->grave, .ngrave = dst->ngrave, .gravecap = dst->gravecap};
    size_t n = src->durable > src->base ? src->durable - src->base : 0;
    if (n > src->n)
        n = src->n;
    if (keep < n)
        n = keep;
    dst->e = n ? calloc(n, sizeof *dst->e) : NULL;
    dst->cap = n;
    for (size_t i = 0; i < n; i++) {
        dst->e[i] = src->e[i];
        dst->e[i].data = malloc(src->e[i].len + 1);
        if (src->e[i].len)
            memcpy(dst->e[i].data, src->e[i].data, src->e[i].len);
    }
    dst->n = n;
    dst->durable = dst->base + n;
}

static void ml_free(struct mlog *l)
{
    ml_release(l);
    for (size_t k = 0; k < l->n; k++)
        free(l->e[k].data);
    free(l->e);
    free(l->grave);
    *l = (struct mlog){0};
}

static const raft_log_ops mlog_ops = {ml_first, ml_last, ml_term_at, ml_get, ml_append,
                                     ml_truncate, ml_reset, ml_compact, ml_release};

struct snode {
    unsigned id;
    int up;
    unsigned incarnation;
    raft *r;
    struct mlog log;
    raft_hard_state hard; /* durable */
    struct meta_state sm;
    uint8_t *snap; /* durable image of sm at snap_index */
    size_t snaplen;
    raft_index snap_index;
    double rate;
    uint64_t restart_at;
    int group; /* partitions: nodes talk within one group */
    unsigned reseeds;
    int blank; /* reseeded and no image since: boots with an empty configuration, as cluster.c's core_boot does */
    uint8_t *pend; /* durable: an install image kept before the core sees it (cluster.c's keep_install) */
    size_t pendlen;
    raft_index pend_index;
    raft_term pend_term;
    struct mlog prelog; /* the log as it was when an install message arrived: what a reset cut short leaves a prefix of */
    raft_index ack_floor; /* what this server held durably and a cut install must not make it forget (I1) */
};

enum { EV_MSG, EV_SYNC, EV_HARD };

struct sevent {
    uint64_t t;
    int kind;
    unsigned node, inc;
    raft_index index;
    raft_term term;
    raft_hard_state hs;
    uint8_t *bytes; /* EV_MSG: the wire message, and a snapshot image after it */
    size_t len, wire_len;
    unsigned from;
};

struct vleader {
    uint64_t epoch;
    uint32_t leader;
};

struct msim {
    uint64_t seed, now;
    iso_rng rng;
    unsigned n;
    struct snode nd[SIM_NODES_MAX + 1];
    struct sevent *ev;
    size_t nev, evcap;
    uint64_t *ref_hash; /* index -> the state's hash there, as the first server to apply it saw it */
    size_t ref_cap;
    struct vleader *vl; /* (volume, epoch) -> leader, every pair seen */
    size_t nvl, vlcap;
    char fail[1024];
    uint64_t applied_max, proposals, ops_ok, leader_changes, crashes, partitions, installs, cut_installs, reseeds;
    uint64_t changes; /* membership changes the leaders started */
    uint64_t resets;  /* REPLICA_RESET instances drawn */
    uint64_t adopts;  /* installs cut short and finished from the kept image */
    uint64_t partial_resets;
    uint64_t rebases; /* logs started again on the snapshot at a boot (a reset cut short left them below it) */
    uint64_t quiet; /* no faults and no workload from here on */
    int mode;       /* the deployment's mode: replicated or shared, by seed */
    raft_config boot;
    int heal_pending;
    uint64_t heal_at;
    struct udrop { char name[8]; uint64_t index; } *drops; /* every committed drop of a user, as first applied */
    size_t ndrops, dropcap;
    char (*uid_name)[8]; /* uid -> the one user it names, ever */
    uint32_t uid_cap;
};

static void sim_fail(struct msim *s, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void sim_fail(struct msim *s, const char *fmt, ...)
{
    if (s->fail[0])
        return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(s->fail, sizeof s->fail, fmt, ap);
    va_end(ap);
}

static uint64_t rnd(struct msim *s, uint64_t lo, uint64_t hi)
{
    return lo + iso_rng_below(&s->rng, hi - lo + 1);
}

static void push(struct msim *s, struct sevent ev)
{
    if (s->nev == s->evcap) {
        s->evcap = s->evcap ? 2 * s->evcap : 256;
        s->ev = realloc(s->ev, s->evcap * sizeof *s->ev);
    }
    s->ev[s->nev++] = ev;
}

static uint64_t local_now(struct msim *s, struct snode *nd)
{
    return (uint64_t)((double)s->now * nd->rate);
}

/* Users: a committed drop is never undone in any copy (a copy past the drop holds the name only as a user made again
 * after it, so with a later version), and a uid names one user, ever. */
static void check_users(struct msim *s, struct snode *nd, const struct meta_state *before)
{
    const struct meta_state *m = &nd->sm;
    if (before)
        for (uint32_t i = 0; i < before->nusers; i++) {
            const struct meta_user *o = &before->users[i], *u = meta_user(m, o->name);
            if (u && u->uid == o->uid)
                continue;
            size_t k;
            for (k = 0; k < s->ndrops; k++)
                if (s->drops[k].index == m->applied && !strcmp(s->drops[k].name, o->name))
                    break;
            if (k == s->ndrops) {
                if (s->ndrops == s->dropcap) {
                    s->dropcap = s->dropcap ? 2 * s->dropcap : 64;
                    s->drops = realloc(s->drops, s->dropcap * sizeof *s->drops);
                }
                snprintf(s->drops[s->ndrops].name, sizeof s->drops[0].name, "%.7s", o->name);
                s->drops[s->ndrops++].index = m->applied;
            }
        }
    for (size_t k = 0; k < s->ndrops; k++) {
        const struct meta_user *u = meta_user(m, s->drops[k].name);
        if (s->drops[k].index <= m->applied && u && u->version < s->drops[k].index) /* = : made again in it */
            sim_fail(s, "node %u at %" PRIu64 ": user %s, dropped at %" PRIu64 ", is back (version %" PRIu64 ")",
                     nd->id, m->applied, s->drops[k].name, s->drops[k].index, u->version);
    }
    for (uint32_t i = 0; i < m->nusers; i++) {
        uint32_t uid = m->users[i].uid;
        if (uid >= (1u << 20)) {
            sim_fail(s, "node %u: user %s has uid %u, more than were ever made", nd->id, m->users[i].name, uid);
            continue;
        }
        if (uid >= s->uid_cap) {
            uint32_t c = s->uid_cap ? s->uid_cap : 64;
            while (c <= uid)
                c *= 2;
            s->uid_name = realloc(s->uid_name, c * sizeof *s->uid_name);
            memset(s->uid_name + s->uid_cap, 0, (c - s->uid_cap) * sizeof *s->uid_name);
            s->uid_cap = c;
        }
        if (!s->uid_name[uid][0])
            snprintf(s->uid_name[uid], sizeof s->uid_name[uid], "%.7s", m->users[i].name);
        else if (strcmp(s->uid_name[uid], m->users[i].name))
            sim_fail(s, "node %u: uid %u names %s and %s", nd->id, uid, s->uid_name[uid], m->users[i].name);
    }
}

static void record(struct msim *s, struct snode *nd, const struct meta_state *before)
{
    uint64_t i = nd->sm.applied;
    if (i >= s->ref_cap) {
        size_t c = s->ref_cap ? s->ref_cap : 1024;
        while (c <= i)
            c *= 2;
        s->ref_hash = realloc(s->ref_hash, c * sizeof *s->ref_hash);
        memset(s->ref_hash + s->ref_cap, 0, (c - s->ref_cap) * sizeof *s->ref_hash);
        s->ref_cap = c;
    }
    uint64_t h = meta_hash(&nd->sm) | 1;
    if (s->ref_hash[i] && s->ref_hash[i] != h)
        sim_fail(s, "node %u: state at index %" PRIu64 " differs from the first server's", nd->id, i);
    s->ref_hash[i] = h;
    if (i > s->applied_max)
        s->applied_max = i;
    char why[256];
    if (before && meta_check_step(before, &nd->sm, why, sizeof why))
        sim_fail(s, "node %u, index %" PRIu64 ": %s", nd->id, i, why);
    check_users(s, nd, before);
    for (uint32_t v = 0; v < nd->sm.nvolumes; v++) {
        const struct meta_volume *mv = &nd->sm.volumes[v];
        size_t k;
        for (k = 0; k < s->nvl; k++)
            if (s->vl[k].epoch == ((uint64_t)mv->number << 40 | mv->epoch))
                break;
        if (k == s->nvl) {
            if (s->nvl == s->vlcap) {
                s->vlcap = s->vlcap ? 2 * s->vlcap : 64;
                s->vl = realloc(s->vl, s->vlcap * sizeof *s->vl);
            }
            s->vl[s->nvl++] = (struct vleader){(uint64_t)mv->number << 40 | mv->epoch, mv->leader};
        } else if (s->vl[k].leader != mv->leader) {
            sim_fail(s, "volume %u epoch %" PRIu64 " has two leaders, %u and %u", mv->number, mv->epoch,
                     s->vl[k].leader, mv->leader);
        }
    }
}

static uint32_t rd32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static uint64_t rd64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

/* One applied op, from the state it met (pre) to the one it left (at): the replica set's version rises by one with
 * each REPLICA_ADD, REPLICA_REMOVE and REPLICA_RESET of its volume and with nothing else; a RESET leaves exactly
 * what it names (a learner of the new instance, open generation 0, in its slot); and the checker holds for the op on
 * its own. An entry changes a volume once (vol_get), so this repeats per op what the checker sees per entry: it
 * stays right should that rule ever go, and names the op that broke a rule. */
static void check_one_op(struct msim *s, struct snode *nd, uint64_t index, uint32_t k, uint8_t kind, const uint8_t *b,
                         uint32_t n, const struct meta_state *pre, const struct meta_state *at)
{
    int setop = kind == META_OP_REPLICA_ADD || kind == META_OP_REPLICA_REMOVE || kind == META_OP_REPLICA_RESET;
    uint32_t number = n >= 4 ? rd32(b) : UINT32_MAX;
    for (uint32_t i = 0; i < pre->nvolumes; i++) {
        const struct meta_volume *a = &pre->volumes[i], *v = meta_volume(at, a->number);
        uint64_t want = a->set_version + (setop && a->number == number);
        if (v && v->set_version != want)
            sim_fail(s, "node %u, index %" PRIu64 ": op %u (kind %u) left volume %u's set version at %" PRIu64
                     ", not %" PRIu64, nd->id, index, k, kind, a->number, v->set_version, want);
    }
    if (kind == META_OP_REPLICA_RESET && n >= 24) {
        const struct meta_volume *a = meta_volume(pre, number), *v = meta_volume(at, number);
        uint32_t node = rd32(b + 12);
        int ia = -1, iv = -1;
        for (uint32_t i = 0; a && i < a->nrep; i++)
            ia = a->rep[i].node == node ? (int)i : ia;
        for (uint32_t i = 0; v && i < v->nrep; i++)
            iv = v->rep[i].node == node ? (int)i : iv;
        if (ia < 0 || ia != iv || v->rep[iv].rid != rd64(b + 16) || !v->rep[iv].learner || v->rep[iv].open_gen ||
            v->rep[iv].open_nonce || v->rep[iv].unanswered || meta_volume_in_isr(a, node) || node == a->leader)
            sim_fail(s, "node %u, index %" PRIu64 ": REPLICA_RESET of node %u on volume %u was applied wrongly", nd->id,
                     index, node, number);
    }
    char why[256];
    if ((setop || kind == META_OP_VOLUME_LEADER || kind == META_OP_ISR_CHANGE) &&
        meta_check_step(pre, at, why, sizeof why))
        sim_fail(s, "node %u, index %" PRIu64 ", op %u (kind %u) on its own: %s", nd->id, index, k, kind, why);
}

/* The conditions each operation names, against the state it met: an applied VOLUME_LEADER named the volume's epoch,
 * an applied ISR_CHANGE its epoch and ISR version, an applied MEMBER_STATE an incarnation no older than the member's
 * (of a member not out). The state each op met is rebuilt by applying, one op at a time on a copy of the state before
 * the entry, the ops the real apply took (res[k] == META_OK): what meta_check_step cannot see (it compares states,
 * not what an op asked for). */
static void check_named(struct msim *s, struct snode *nd, const struct meta_state *before, const raft_entry *e,
                        const int *res, unsigned nres)
{
    const uint8_t *d = e->data;
    if (e->len < 8)
        return;
    uint32_t nops = rd32(d + 4);
    size_t off = 8;
    struct meta_state at;
    meta_copy(&at, before);
    for (uint32_t k = 0; k < nops && k < nres && off + 5 <= e->len; k++) {
        uint8_t kind = d[off];
        uint32_t n = rd32(d + off + 1);
        const uint8_t *b = d + off + 5;
        if (off + 5 + n > e->len)
            break;
        int ok = res[k] == META_OK;
        if ((kind == META_OP_VOLUME_LEADER || kind == META_OP_ISR_CHANGE) && n >= 12 && ok) {
            const struct meta_volume *v = meta_volume(&at, rd32(b));
            uint64_t epoch = rd64(b + 4);
            if (!v || v->epoch != epoch)
                sim_fail(s, "node %u, index %" PRIu64 ": op %u (%s) named epoch %" PRIu64 " of volume %u at epoch %"
                         PRIu64 " and was applied", nd->id, e->index, k,
                         kind == META_OP_VOLUME_LEADER ? "VOLUME_LEADER" : "ISR_CHANGE", epoch, rd32(b),
                         v ? v->epoch : 0);
            else if (kind == META_OP_ISR_CHANGE && n >= 20 && rd64(b + 12) != v->isr_version)
                sim_fail(s, "node %u, index %" PRIu64 ": ISR_CHANGE named ISR version %" PRIu64 " of volume %u at %"
                         PRIu64 " and was applied", nd->id, e->index, rd64(b + 12), rd32(b), v->isr_version);
        }
        if ((kind == META_OP_VOLUME_RESERVE || kind == META_OP_VOLUME_DROP || kind == META_OP_CLAIM_REFUSED ||
             kind == META_OP_CLAIM_FAILED || kind == META_OP_VOLUME_RELOCATE) && n >= 12 && ok) {
            const struct meta_volume *v = meta_volume(&at, rd32(b));
            if (!v || v->epoch != rd64(b + 4)) /* a reservation or a drop of epoch E comes before any grant above E */
                sim_fail(s, "node %u, index %" PRIu64 ": op %u (kind %u) named epoch %" PRIu64 " of volume %u at epoch %"
                         PRIu64 " and was applied", nd->id, e->index, k, kind, rd64(b + 4), rd32(b), v ? v->epoch : 0);
        }
        if (kind == META_OP_VOLUME_SELF && n >= 16 && ok) {
            const struct meta_volume *v = meta_volume(&at, rd32(b));
            if (!v || v->leader != rd32(b + 4) || v->epoch + 1 != rd64(b + 8))
                sim_fail(s, "node %u, index %" PRIu64 ": VOLUME_SELF of node %u to epoch %" PRIu64 " on volume %u (owner %u,"
                         " epoch %" PRIu64 ") was applied", nd->id, e->index, rd32(b + 4), rd64(b + 8), rd32(b),
                         v ? v->leader : 0, v ? v->epoch : 0);
        }
        if (kind == META_OP_SLOT_FREEZE && n >= 16 && ok) {
            const struct meta_volume *v = meta_volume(&at, rd32(b + 4));
            if (!v || v->epoch != rd64(b + 8))
                sim_fail(s, "node %u, index %" PRIu64 ": SLOT_FREEZE of slot %u named epoch %" PRIu64 " of volume %u at %"
                         PRIu64 " and was applied", nd->id, e->index, rd32(b), rd64(b + 8), rd32(b + 4),
                         v ? v->epoch : 0);
        }
        if (kind == META_OP_ISR_CHANGE && n >= 24 && ok) {
            const struct meta_volume *v = meta_volume(&at, rd32(b));
            uint32_t ni = rd32(b + 20);
            for (uint32_t i = 0; v && i < ni && ni <= META_REPLICAS_MAX && 24 + (size_t)ni * 12 <= n; i++) {
                const struct meta_replica *r = meta_replica(v, rd32(b + 24 + i * 4));
                size_t ro = 24 + (size_t)ni * 4 + (size_t)i * 8;
                if (ro + 8 <= n && (!r || r->rid != rd64(b + ro)))
                    sim_fail(s, "node %u, index %" PRIu64 ": ISR_CHANGE of volume %u listed node %u with instance %" PRIu64
                             " and was applied", nd->id, e->index, rd32(b), rd32(b + 24 + i * 4), rd64(b + ro));
            }
        }
        if (kind == META_OP_MEMBER_STATE && n >= 13 && ok) {
            const struct meta_member *mb = meta_member(&at, rd32(b));
            uint64_t inc = rd64(b + 5);
            if (!mb || mb->state == META_OUT || inc < mb->incarnation)
                sim_fail(s, "node %u, index %" PRIu64 ": MEMBER_STATE of member %u named incarnation %" PRIu64
                         " (the member's: %" PRIu64 ", state %d) and was applied", nd->id, e->index, rd32(b), inc,
                         mb ? mb->incarnation : 0, mb ? mb->state : -1);
        }
        if (ok) { /* the state the next op meets */
            uint8_t one[8 + 5 + 4096];
            if (5 + (size_t)n <= sizeof one - 8) {
                uint32_t n1 = 1;
                memcpy(one, d, 4);
                memcpy(one + 4, &n1, 4);
                memcpy(one + 8, d + off, 5 + (size_t)n);
                int r1 = META_ERR_INVALID;
                struct meta_state pre;
                meta_copy(&pre, &at);
                meta_apply(&at, e->index, e->term, one, 13 + (size_t)n, &r1, 1);
                check_one_op(s, nd, e->index, k, kind, b, n, &pre, &at);
                meta_free(&pre);
            }
        }
        off += 5 + (size_t)n;
    }
    meta_free(&at);
}

static void apply_entry(struct msim *s, struct snode *nd, const raft_entry *e)
{
    if (e->index != nd->sm.applied + 1) {
        sim_fail(s, "node %u applied %" PRIu64 " after %" PRIu64, nd->id, e->index, nd->sm.applied);
        return;
    }
    if (e->type == RAFT_ENTRY_NORMAL) {
        struct meta_state before;
        meta_copy(&before, &nd->sm);
        int res[16];
        for (unsigned i = 0; i < 16; i++)
            res[i] = META_ERR_INVALID; /* entries hold up to 4 operations: the rest stay "not applied" */
        if (meta_apply(&nd->sm, e->index, e->term, e->data, e->len, res, 16) != META_OK)
            sim_fail(s, "node %u: entry %" PRIu64 " does not decode", nd->id, e->index);
        else
            for (unsigned i = 0; i < 16; i++)
                s->ops_ok += res[i] == META_OK;
        check_named(s, nd, &before, e, res, 16);
        record(s, nd, &before);
        meta_free(&before);
    } else {
        meta_apply_other(&nd->sm, e->index, e->term, e->type == RAFT_ENTRY_CONFIG ? e->data : NULL,
                         e->type == RAFT_ENTRY_CONFIG ? e->len : 0);
        record(s, nd, NULL);
    }
}

static void take_snapshot(struct snode *nd)
{
    free(nd->snap);
    meta_snapshot_encode(&nd->sm, &nd->snap, &nd->snaplen);
    nd->snap_index = nd->sm.applied;
    raft_config c;
    if (!raft_config_decode(nd->sm.config, nd->sm.config_len, &c))
        abort();
    raft_compact(nd->r, nd->sm.applied, nd->sm.applied_term, &c);
}

static void send_msg(struct msim *s, struct snode *from, const raft_msg *m)
{
    struct snode *to = m->to >= 1 && m->to <= s->n ? &s->nd[m->to] : NULL;
    /* Loss is a fault like the others: it stops at `quiet`, or the 12 s tail (one install, two resends) fails on three
     * lost messages in a row (soak seed 4475: the ack of an install and both resends dropped, 0.0008 % a send, hit
     * once in 5000 seeds). The draw is still made, so a seed replays the same until the tail. */
    if (!to || to->group != from->group)
        return;
    int lost = iso_rng_chance(&s->rng, 0.02) && s->now < s->quiet;
    if (lost)
        return;
    size_t wl = raft_wire_size(m), il = 0;
    const uint8_t *img = NULL;
    if (m->type == RAFT_MSG_SNAPSHOT) {
        if (from->snap_index != m->u.snapshot.index) {
            sim_fail(s, "node %u sends a snapshot at %" PRIu64 " but holds one at %" PRIu64, from->id,
                     m->u.snapshot.index, from->snap_index);
            return;
        }
        img = from->snap;
        il = from->snaplen;
    }
    uint8_t *b = malloc(wl + il + 1);
    if (raft_wire_encode(m, b, wl) != wl) {
        sim_fail(s, "raft_wire_encode failed");
        free(b);
        return;
    }
    if (il)
        memcpy(b + wl, img, il);
    int copies = iso_rng_chance(&s->rng, 0.01) ? 2 : 1;
    for (int c = 0; c < copies; c++) {
        uint8_t *bc = c ? malloc(wl + il + 1) : b;
        if (c)
            memcpy(bc, b, wl + il);
        /* one TCP stream per pair keeps order: a delay that only grows per pair would be exact; a small spread
         * with rare reordering is what the core must survive anyway */
        push(s, (struct sevent){.t = s->now + rnd(s, 100000, 2000000), .kind = EV_MSG, .node = to->id,
                                .inc = to->incarnation, .bytes = bc, .len = wl + il, .wire_len = wl,
                                .from = from->id});
    }
}

static void crash(struct msim *s, struct snode *nd);

static void drain(struct msim *s, struct snode *nd)
{
    raft_action a;
    while (nd->up && raft_next_action(nd->r, &a)) {
        switch (a.type) {
        case RAFT_ACT_SAVE_HARD:
            push(s, (struct sevent){.t = s->now + rnd(s, 50000, 1000000), .kind = EV_HARD, .node = nd->id,
                                    .inc = nd->incarnation, .hs = a.u.hard});
            break;
        case RAFT_ACT_SYNC:
            push(s, (struct sevent){.t = s->now + rnd(s, 50000, 1000000), .kind = EV_SYNC, .node = nd->id,
                                    .inc = nd->incarnation, .index = a.u.sync.index, .term = a.u.sync.term});
            break;
        case RAFT_ACT_SEND: send_msg(s, nd, &a.u.msg); break;
        case RAFT_ACT_APPLY: {
            raft_index last = 0;
            for (unsigned i = 0; i < a.u.apply.n; i++) {
                apply_entry(s, nd, &a.u.apply.entries[i]);
                last = a.u.apply.entries[i].index;
            }
            raft_applied(nd->r, last);
            /* Only over a durable log, as cluster.c does (its syncs run inline, before it snapshots): a follower
             * applies what the leader committed before its own copy is synced, and an image kept past the synced
             * log boots a core whose commit is above its last entry, which then skips the entry it lacks and never
             * appends again (soak seed 4305: stuck at 5200 with a log ending at 5199). */
            if (nd->sm.applied - nd->snap_index >= 50 && nd->sm.applied <= nd->log.durable)
                take_snapshot(nd);
            break;
        }
        case RAFT_ACT_RESTORE: {
            if (s->now < s->quiet && rnd(s, 0, 3) == 0) {
                /* the crash cluster.c's restore can meet: the core has reset the log to the image's index, the image
                 * is not kept yet (the boot finds a log based past its snapshot: reseed) */
                s->cut_installs++;
                crash(s, nd);
                /* The reset is a floor lowered, then segments deleted newest first, then the new one made, a
                 * crash at any step of it: the reset in full (the log on the image's index), a prefix of the old
                 * log (cut anywhere, below the snapshot too), or no segment at all (the log opens on the snapshot).
                 * What the server held durably it must not forget (I1). */
                nd->ack_floor = nd->prelog.durable < nd->pend_index ? nd->prelog.durable : nd->pend_index;
                unsigned how = (unsigned)rnd(s, 0, 3);
                if (how == 1) {
                    size_t n = nd->prelog.durable > nd->prelog.base ? nd->prelog.durable - nd->prelog.base : 0;
                    ml_clone_durable(&nd->log, &nd->prelog, (size_t)rnd(s, 0, (uint64_t)n));
                    s->partial_resets++;
                } else if (how == 2) {
                    struct meta_state t;
                    meta_init(&t);
                    if (nd->snap && meta_snapshot_decode(&t, nd->snap, nd->snaplen) != META_OK)
                        abort();
                    ml_reset(&nd->log, t.applied, t.applied_term);
                    ml_release(&nd->log);
                    meta_free(&t);
                    s->partial_resets++;
                }
                break;
            }
            struct meta_state t;
            meta_init(&t);
            if (meta_snapshot_decode(&t, a.u.snapshot.data, a.u.snapshot.len) != META_OK ||
                t.applied != a.u.snapshot.index) {
                sim_fail(s, "node %u: the snapshot image for %" PRIu64 " does not decode", nd->id,
                         a.u.snapshot.index);
                meta_free(&t);
                break;
            }
            meta_free(&nd->sm);
            nd->sm = t;
            free(nd->snap);
            nd->snap = malloc(a.u.snapshot.len);
            memcpy(nd->snap, a.u.snapshot.data, a.u.snapshot.len);
            nd->snaplen = a.u.snapshot.len;
            nd->snap_index = a.u.snapshot.index;
            nd->blank = 0;
            free(nd->pend); /* restore() removes the kept copy */
            nd->pend = NULL;
            nd->pend_index = 0;
            s->installs++;
            record(s, nd, NULL);
            break;
        }
        case RAFT_ACT_STATE:
            if (a.u.state.role == RAFT_LEADER)
                s->leader_changes++;
            break;
        default: break;
        }
    }
}

static void boot(struct msim *s, struct snode *nd)
{
    meta_free(&nd->sm);
    meta_init(&nd->sm);
    if (nd->snap) {
        if (meta_snapshot_decode(&nd->sm, nd->snap, nd->snaplen) != META_OK)
            abort();
    }
    int adopted = 0;
    if (nd->pend && nd->pend_index > nd->sm.applied) {
        /* cluster.c's adopt_install, before the open: a kept image ahead of the snapshot is the store the reset was
         * made for (a leader's image is committed state), whatever the log a cut reset left holds */
        free(nd->snap);
        nd->snap = nd->pend;
        nd->snaplen = nd->pendlen;
        nd->snap_index = nd->pend_index;
        nd->pend = NULL;
        meta_free(&nd->sm);
        meta_init(&nd->sm);
        if (meta_snapshot_decode(&nd->sm, nd->snap, nd->snaplen) != META_OK)
            abort();
        adopted = 1;
        s->adopts++;
    }
    free(nd->pend); /* cluster.c drops a kept image the log never moved to */
    nd->pend = NULL;
    nd->pend_index = 0;
    if (nd->log.base > nd->sm.applied) {
        /* keep_install's copy is durable before the reset, so only damage, which this simulator does not make, could
         * get here; cluster.c would then reseed (below): fence the vote at the log's last entry, drop the snapshot
         * and the log, and wait for a new image. A fenced voter counts as missing until then. */
        sim_fail(s, "node %u: its log begins after %" PRIu64 ", past its snapshot's %" PRIu64 ", with no image kept",
                 nd->id, (uint64_t)nd->log.base, nd->sm.applied);
        raft_index li = nd->log.base + nd->log.n;
        raft_term lt = ml_term_at(&nd->log, li);
        if (nd->hard.fence_index < li) {
            nd->hard.fence_index = li;
            nd->hard.fence_term = lt;
        }
        ml_reset(&nd->log, 0, 0);
        ml_release(&nd->log);
        free(nd->snap);
        nd->snap = NULL;
        nd->snaplen = nd->snap_index = 0;
        meta_free(&nd->sm);
        meta_init(&nd->sm);
        s->reseeds++;
        nd->reseeds++;
        nd->blank = 1;
    }
    if (nd->log.base <= nd->sm.applied &&
        (ml_last(&nd->log) < nd->sm.applied ||
         (adopted && nd->sm.applied && ml_term_at(&nd->log, nd->sm.applied) != nd->sm.applied_term))) {
        /* cluster.c's rebase: the log ends below the store, or holds another entry than the leader's image at its
         * index (it can never commit): the log starts again on the snapshot */
        ml_reset(&nd->log, nd->sm.applied, nd->sm.applied_term);
        ml_release(&nd->log);
        s->rebases++;
    }
    if (nd->log.base > nd->sm.applied || nd->sm.applied > ml_last(&nd->log))
        sim_fail(s, "node %u: the log (%" PRIu64 "..%" PRIu64 ") does not cover the store at %" PRIu64 " (I2)",
                 nd->id, (uint64_t)nd->log.base, (uint64_t)ml_last(&nd->log), nd->sm.applied);
    if (nd->ack_floor) {
        raft_index have = ml_last(&nd->log) > nd->sm.applied ? ml_last(&nd->log) : nd->sm.applied;
        if (have < nd->ack_floor && nd->hard.fence_index < nd->ack_floor)
            sim_fail(s, "node %u held %" PRIu64 " durably and boots with %" PRIu64 " and a fence at %" PRIu64
                     ": a forgetful voter (I1)", nd->id, (uint64_t)nd->ack_floor, (uint64_t)have,
                     (uint64_t)nd->hard.fence_index);
        nd->ack_floor = 0;
    }
    if (!nd->snap && !nd->blank) {
        uint8_t c[META_CONFIG_MAX];
        size_t cl = raft_config_encode(&s->boot, c, sizeof c);
        meta_apply_other(&nd->sm, 0, 0, c, cl);
    }
    raft_boot b = {.hard = nd->hard, .applied = nd->sm.applied, .applied_term = nd->sm.applied_term};
    if (nd->sm.config_len && !raft_config_decode(nd->sm.config, nd->sm.config_len, &b.config))
        abort(); /* a blank server boots with no configuration until an image arrives */
    raft_opts o;
    raft_opts_defaults(&o);
    o.seed = s->seed * 31 + nd->id * 7 + nd->incarnation;
    o.heartbeat_ns = 50000000;
    o.election_min_ns = 300000000;
    char err[256];
    nd->r = raft_new(nd->id, &o, &mlog_ops, &nd->log, &b, err, sizeof err);
    if (!nd->r) {
        sim_fail(s, "node %u: raft_new: %s", nd->id, err);
        return;
    }
    nd->up = 1;
    raft_clock(nd->r, local_now(s, nd));
}

static void crash(struct msim *s, struct snode *nd)
{
    raft_free(nd->r);
    nd->r = NULL;
    nd->up = 0;
    nd->incarnation++;
    s->crashes++;
    /* the disk keeps the synced prefix (but never below the snapshot's base) and the last saved hard state */
    ml_release(&nd->log);
    if (nd->log.durable < nd->log.base + nd->log.n)
        ml_truncate(&nd->log, nd->log.durable + 1);
    ml_release(&nd->log);
    nd->restart_at = s->now + rnd(s, 100000000, 2000000000);
}

static int has_id_local(const uint32_t *ids, uint32_t n, uint32_t id)
{
    for (uint32_t i = 0; i < n; i++)
        if (ids[i] == id)
            return 1;
    return 0;
}

/* What the leader of the moment proposes: changes made from its own applied view, often stale or wrong on purpose. */
static void propose(struct msim *s, struct snode *nd)
{
    const struct meta_state *m = &nd->sm;
    struct meta_entry e;
    meta_entry_init(&e);
    unsigned nops = (unsigned)rnd(s, 1, 4);
    for (unsigned k = 0; k < nops; k++) {
        if (!m->initialised) {
            struct meta_settings st;
            meta_settings_default(&st);
            st.failover = (uint8_t)rnd(s, 0, 2);
            struct meta_member mb;
            member(&mb, 1);
            uint8_t uuid[16] = {9};
            meta_op_init(&e, uuid, &st, &mb, s->mode, META_LEVEL);
            continue;
        }
        int shared = m->mode == META_MODE_SHARED;
        unsigned what = (unsigned)rnd(s, 0, 15);
        const struct meta_volume *v = m->nvolumes ? &m->volumes[rnd(s, 0, m->nvolumes - 1)] : NULL;
        int stale = rnd(s, 0, 9) == 0; /* the epoch a stale proposer would name */
        if (what == 0 && m->nmembers < 8) {
            struct meta_member mb;
            member(&mb, m->next_member + (uint32_t)rnd(s, 0, 1));
            if (rnd(s, 0, 5) == 0)
                mb.flags = META_MF_VOTER_OK; /* a witness: no volumes */
            meta_op_member_add(&e, &mb);
        } else if (what == 1 && m->nvolumes < SIM_VOLS && m->nmembers >= 3) {
            uint32_t nodes[3], have = 0;
            while (have < 3) {
                uint32_t id = m->members[rnd(s, 0, m->nmembers - 1)].id;
                if (!has_id_local(nodes, have, id))
                    nodes[have++] = id;
            }
            char name[16];
            snprintf(name, sizeof name, "v%u", m->next_volume);
            vol_add(&e, rnd(s, 0, 15) == 0 ? 3 - m->mode : m->mode, m->next_volume, name, 1, nodes, 3);
        } else if (what == 2 && v) {
            uint64_t epoch = v->epoch - (stale && v->epoch > 1);
            if (!shared) {
                /* a promotion: fence the ISR, some answer; the pick is the rule's (the highest node id among the
                 * answers of the right instance) or, sometimes, wild */
                uint32_t fenced[META_REPLICAS_MAX], nf = 0, ans[META_REPLICAS_MAX], na = 0;
                for (uint32_t i = 0; i < v->nisr; i++) {
                    fenced[nf++] = v->isr[i];
                    if (rnd(s, 0, 2))
                        ans[na++] = v->isr[i];
                }
                if (rnd(s, 0, 7) == 0 && v->nrep > nf) /* a fence that went to a non-member too */
                    fenced[nf++] = v->rep[v->nrep - 1].node;
                uint32_t leader = 0;
                for (uint32_t i = 0; i < na; i++)
                    if (ans[i] > leader)
                        leader = ans[i];
                if (!na || rnd(s, 0, 7) == 0)
                    leader = v->rep[rnd(s, 0, v->nrep - 1)].node;
                struct meta_volume tmp = *v;
                if (rnd(s, 0, 9) == 0 && tmp.nrep)
                    tmp.rep[0].rid ^= 0x55; /* a reply of another instance */
                tmp.isr_version = v->isr_version - (rnd(s, 0, 11) == 0);
                struct meta_grant g = promo_grant(&tmp, epoch, leader, fenced, nf, ans, na,
                                                  v->hist[v->nhist - 1].start + rnd(s, 0, 20), rnd(s, 0, 15) == 0);
                if (rnd(s, 0, 3) == 0) { /* a planned transfer: its target the pick, or any replica; its end */
                    g.reason = rnd(s, 0, 9) ? META_GR_PLANNED : META_GR_UNHEARD;
                    g.target = rnd(s, 0, 2) ? leader : v->rep[rnd(s, 0, v->nrep - 1)].node;
                    g.min_end = rnd(s, 0, 50);
                }
                meta_op_volume_leader(&e, &g);
            } else {
                uint32_t to = rnd(s, 0, 3) ? v->rep[rnd(s, 0, v->nrep - 1)].node : v->leader;
                uint64_t ne = v->epoch + (to == v->leader ? 1 : 2) + rnd(s, 0, 3) - (rnd(s, 0, 9) == 0);
                struct meta_grant g = sgrant(v, to, ne, (int)rnd(s, 1, META_GR_LAST));
                g.epoch = epoch;
                g.isr_version = v->isr_version - (rnd(s, 0, 15) == 0);
                g.pos.wal_off = rnd(s, 0, 1000);
                if (rnd(s, 0, 1)) {
                    g.att.kind = (uint8_t)rnd(s, 1, 2);
                    snprintf(g.att.who, sizeof g.att.who, "op");
                    g.att.ms = rnd(s, 0, 5);
                    g.att.incarnation = rnd(s, 0, 3);
                }
                meta_op_volume_leader(&e, &g);
            }
        } else if (what == 3 && v && !shared) {
            /* an ISR change keeps the leader; may add back any replica; sometimes with another instance */
            uint32_t isr[META_REPLICAS_MAX], ni = 0;
            int wild = rnd(s, 0, 7) == 0;
            isr[ni++] = wild ? v->rep[rnd(s, 0, v->nrep - 1)].node : v->leader;
            for (uint32_t i = 0; i < v->nrep; i++)
                if (v->rep[i].node != isr[0] && rnd(s, 0, 1))
                    isr[ni++] = v->rep[i].node;
            uint64_t rid[META_REPLICAS_MAX];
            for (uint32_t i = 0; i < ni; i++)
                rid[i] = meta_replica(v, isr[i])->rid ^ (rnd(s, 0, 15) == 0 ? 1 : 0);
            meta_op_isr_change(&e, v->number, v->epoch - (stale && v->epoch > 1),
                               v->isr_version - (rnd(s, 0, 9) == 0), isr, rid, ni, rnd(s, 0, 3) ? 0 : rnd(s, 1, 99));
        } else if (what == 4 && v) {
            uint32_t node = m->members[rnd(s, 0, m->nmembers - 1)].id;
            uint64_t epoch = v->epoch - (stale && v->epoch > 1);
            unsigned how = (unsigned)rnd(s, 0, 7);
            int mover = -1; /* a pending move's learner */
            for (uint32_t i = 0; i < v->nrep; i++)
                if (v->rep[i].replaces)
                    mover = (int)i;
            if (how <= 2) { /* an add; sometimes a move (R4 D10), of a replica or of any member */
                uint32_t replaces = 0;
                if (rnd(s, 0, 1))
                    replaces = rnd(s, 0, 5) ? v->rep[rnd(s, 0, v->nrep - 1)].node : node;
                meta_op_replica_add(&e, v->number, epoch, node,
                                    shared ? (rnd(s, 0, 15) == 0) : 7000 + rnd(s, 0, 4000), replaces);
            } else if (how <= 5) { /* a remove; with a move pending often its end (the replaced) or its cancel */
                if (mover >= 0 && rnd(s, 0, 1))
                    node = rnd(s, 0, 2) ? v->rep[mover].replaces : v->rep[mover].node;
                meta_op_replica_remove(&e, v->number, epoch, node);
            } else { /* a reset (D13): of a replica, sometimes of any member or to an instance the volume has */
                if (rnd(s, 0, 4))
                    node = v->rep[rnd(s, 0, v->nrep - 1)].node;
                /* fresh instances are even numbers above 2^40: never one a REPLICA_OPEN of this entry names (it names
                 * the view's instance, or that one with its low bit flipped) */
                uint64_t rid = rnd(s, 0, 9) ? (1ull << 40) + 2 * ++s->resets : v->rep[rnd(s, 0, v->nrep - 1)].rid;
                meta_op_replica_reset(&e, v->number, epoch, node, rid);
            }
        } else if (what == 5 && v && !shared) {
            const struct meta_replica *r = &v->rep[rnd(s, 0, v->nrep - 1)];
            meta_op_replica_open(&e, v->number, r->node, r->rid ^ (rnd(s, 0, 11) == 0), r->open_gen + rnd(s, 0, 2),
                                 rnd(s, 0, 1000));
        } else if (what == 6) {
            uint32_t id = m->members[rnd(s, 0, m->nmembers - 1)].id;
            uint64_t inc = meta_member(m, id)->incarnation + rnd(s, 0, 1);
            if (rnd(s, 0, 3) == 0) { /* moved: to a fresh address, or onto another member's (refused) */
                char peer[32];
                snprintf(peer, sizeof peer, "10.0.%u.%u:5556", (unsigned)rnd(s, 0, 1), (unsigned)rnd(s, 1, 9));
                meta_op_member_addr(&e, id, inc, "10.9.9.9:5555", peer);
            }
            if (rnd(s, 0, 2) == 0)
                meta_op_member_flags(&e, id, inc, (uint8_t)rnd(s, 0, 3));
            else
                meta_op_member_state(&e, id, (int)rnd(s, 0, 2), inc);
        } else if (what == 7 && m->nvolumes) {
            uint32_t first = (uint32_t)rnd(s, 0, META_SLOTS - 1);
            meta_op_slot_home(&e, first, (uint32_t)rnd(s, 1, first + 64 <= META_SLOTS ? 64 : META_SLOTS - first),
                              m->volumes[rnd(s, 0, m->nvolumes - 1)].number);
        } else if (what == 8 && m->nvolumes) {
            /* the two-phase move of a slot: pending, A's owner freezes (its epoch, sometimes a stale one), the
             * leader moves it or cancels */
            uint32_t slot = m->npend && rnd(s, 0, 1) ? m->pend[rnd(s, 0, m->npend - 1)].slot
                                                     : (uint32_t)rnd(s, 0, META_SLOTS - 1);
            const struct meta_slot_pending *p = meta_slot_pending(m, slot);
            if (!p) {
                uint32_t from = m->slot_home[slot];
                uint32_t to = m->volumes[rnd(s, 0, m->nvolumes - 1)].number;
                if (from == META_NONE) /* give it a home first */
                    meta_op_slot_home(&e, slot, 1, to);
                else
                    meta_op_slot_move_pending(&e, slot, from, to);
            } else {
                const struct meta_volume *fv = meta_volume(m, p->from);
                unsigned c = (unsigned)rnd(s, 0, 5);
                if (c <= 2 || !p->freeze_epoch)
                    meta_op_slot_freeze(&e, slot, p->from, (fv ? fv->epoch : 1) - (rnd(s, 0, 5) == 0 && fv && fv->epoch > 1));
                else if (c <= 4)
                    meta_op_slot_move(&e, slot, p->to);
                else
                    meta_op_slot_move_cancel(&e, slot);
            }
        } else if (what == 9 && v && shared) {
            uint64_t epoch = v->epoch - (stale && v->epoch > 1);
            switch (rnd(s, 0, 5)) {
            case 0:
                meta_op_volume_self(&e, v->number, rnd(s, 0, 7) ? v->leader : v->rep[rnd(s, 0, v->nrep - 1)].node,
                                    v->epoch + 1 + (rnd(s, 0, 11) == 0));
                break;
            case 1: {
                struct meta_positions pos = {.wal_seg = rnd(s, 0, 9), .wal_off = rnd(s, 0, 999)};
                meta_op_volume_reserve(&e, v->number, epoch, v->series_reserved + (uint32_t)rnd(s, 0, 64) -
                                                                 (rnd(s, 0, 9) == 0 && v->series_reserved ? 1 : 0),
                                       v->states_reserved + (uint32_t)rnd(s, 0, 8), &pos);
                break;
            }
            case 2:
                meta_op_volume_drop(&e, v->number, epoch, v->wm_gen + rnd(s, 0, 1), v->dropped_before + (int64_t)rnd(s, 0, 50) -
                                    (int64_t)(rnd(s, 0, 9) == 0));
                break;
            case 3:
                meta_op_claim_refused(&e, v->number, epoch, v->epoch + rnd(s, 0, 4));
                break;
            case 4:
                meta_op_claim_failed(&e, v->number, epoch, 1);
                break;
            default:
                meta_op_volume_relocate(&e, v->number, epoch, "nfs7:/pool/moved");
            }
        } else if (what == 10) {
            char name[16];
            snprintf(name, sizeof name, "pool%u", (unsigned)rnd(s, 0, 2));
            if (rnd(s, 0, 3))
                meta_op_domain_state(&e, name, (int)rnd(s, 0, 2));
            else
                meta_op_restore(&e, m->restore_gen + rnd(s, 0, 1));
        } else if (what == 11 && rnd(s, 0, 5) == 0) {
            meta_op_name_group(&e);
        } else {
            char name[16];
            snprintf(name, sizeof name, "u%u", (unsigned)rnd(s, 0, 5));
            if (rnd(s, 0, 2)) {
                /* the version this proposer's copy has (often stale by the time it applies), sometimes a wrong one;
                 * the blob's uid is what a proposer would pick from its copy (the state machine must not take it) */
                const struct meta_user *u = meta_user(m, name);
                uint64_t ver = u ? u->version : 0;
                if (!rnd(s, 0, 5))
                    ver = rnd(s, 0, 1) ? 0 : ver + 1;
                uint8_t blob[13] = "\0\0\0\0verifier";
                uint32_t uid = m->max_uid + (uint32_t)rnd(s, 0, 1);
                memcpy(blob, &uid, 4);
                meta_op_user_put(&e, name, ver, blob, (uint32_t)rnd(s, 3, 12));
            } else {
                meta_op_user_del(&e, name);
            }
        }
    }
    meta_entry_finish(&e);
    if (raft_propose(nd->r, e.buf, e.len, NULL) == ISO_OK)
        s->proposals++;
    meta_entry_free(&e);
}

/* Membership changes at the leader of the moment, as cluster.c's voter management makes them: promote a learner that
 * has caught up, add a server that is not a member as a learner, or (before the quiet tail only) remove or demote a
 * voter other than itself, down to 3 voters of 5 or 2 of 3. In the quiet tail everyone comes back as a voter, so the
 * final check covers every server. The core refuses a change while another is pending; that is part of the test. */
static void membership(struct msim *s, struct snode *ld, int quiet)
{
    const raft_config *c = raft_get_config(ld->r);
    raft_id learner = 0, absent = 0, voter = 0;
    unsigned voters = raft_config_voters(c), nv = 0, nl = 0, na = 0;
    for (unsigned i = 1; i <= s->n; i++) {
        const raft_member *m = raft_config_find(c, i);
        raft_progress pr;
        if (!m) {
            if (rnd(s, 0, na++) == 0)
                absent = i;
        } else if (m->learner) {
            if (raft_get_progress(ld->r, i, &pr) && pr.match >= raft_get_commit(ld->r) && rnd(s, 0, nl++) == 0)
                learner = i;
        } else if (i != ld->id && rnd(s, 0, nv++) == 0) {
            voter = i;
        }
    }
    raft_change_kind kind;
    raft_id id;
    if (learner)
        kind = RAFT_ADD_VOTER, id = learner;
    else if (absent && (quiet || rnd(s, 0, 1)))
        kind = RAFT_ADD_LEARNER, id = absent;
    else if (!quiet && voter && voters > (s->n == 5 ? 3u : 2u))
        kind = rnd(s, 0, 1) ? RAFT_REMOVE : RAFT_DEMOTE, id = voter;
    else
        return;
    if (raft_change(ld->r, kind, id, NULL) == ISO_OK)
        s->changes++;
}

static int msim_run(uint64_t seed, double seconds, int verbose)
{
    struct msim *s = calloc(1, sizeof *s);
    s->seed = seed;
    s->mode = (seed >> 1) & 1 ? META_MODE_SHARED : META_MODE_REPLICATED;
    iso_rng_seed(&s->rng, seed);
    s->n = seed % 3 == 0 ? 3 : 5;
    s->boot.n = s->n;
    for (unsigned i = 1; i <= s->n; i++) {
        struct snode *nd = &s->nd[i];
        nd->id = i;
        nd->rate = 1.0 + ((double)rnd(s, 0, 2000) - 1000.0) * 1e-6; /* +-1000 ppm */
        meta_init(&nd->sm);
        s->boot.m[i - 1] = (raft_member){.id = i, .learner = false};
    }
    for (unsigned i = 1; i <= s->n; i++)
        boot(s, &s->nd[i]);
    /* the faults and the workload stop at `quiet`; every server must then catch up (a reseed included) */
    /* 12 s: a restart (up to 2 s) and the leader's snapshot resend (5 s) fit */
    uint64_t quiet = (uint64_t)(seconds * 1e9), end = quiet + 12000000000ull, step = 1000000, next_tick = 0;
    s->quiet = quiet;
    while (s->now < end && !s->fail[0]) {
        s->now += step;
        /* due events, in time order among those due */
        for (;;) {
            size_t best = SIZE_MAX;
            for (size_t i = 0; i < s->nev; i++)
                if (s->ev[i].t <= s->now && (best == SIZE_MAX || s->ev[i].t < s->ev[best].t))
                    best = i;
            if (best == SIZE_MAX)
                break;
            struct sevent ev = s->ev[best];
            s->ev[best] = s->ev[--s->nev];
            struct snode *nd = &s->nd[ev.node];
            if (!nd->up || nd->incarnation != ev.inc) {
                free(ev.bytes);
                continue;
            }
            raft_clock(nd->r, local_now(s, nd));
            if (ev.kind == EV_MSG) {
                raft_msg m;
                raft_entry ents[RAFT_WIRE_MAX_ENTRIES];
                if (s->nd[ev.from].group != nd->group) {
                    free(ev.bytes);
                    continue;
                }
                if (!raft_wire_decode(ev.bytes, ev.wire_len, &m, ents, RAFT_WIRE_MAX_ENTRIES)) {
                    sim_fail(s, "a message from %u does not decode", ev.from);
                    free(ev.bytes);
                    break;
                }
                if (m.type == RAFT_MSG_SNAPSHOT) {
                    m.u.snapshot.data = ev.bytes + ev.wire_len;
                    m.u.snapshot.len = ev.len - ev.wire_len;
                    ml_clone_durable(&nd->prelog, &nd->log, SIZE_MAX);
                    if (m.u.snapshot.index > nd->pend_index && m.u.snapshot.index > nd->sm.applied) {
                        free(nd->pend); /* keep_install: durable before the core resets the log */
                        nd->pend = malloc(m.u.snapshot.len + 1);
                        memcpy(nd->pend, m.u.snapshot.data, m.u.snapshot.len);
                        nd->pendlen = m.u.snapshot.len;
                        nd->pend_index = m.u.snapshot.index;
                        nd->pend_term = m.u.snapshot.term;
                    }
                }
                raft_step(nd->r, &m);
            } else if (ev.kind == EV_SYNC) {
                raft_index idx = ev.index < ml_last(&nd->log) ? ev.index : ml_last(&nd->log);
                if (idx > nd->log.durable)
                    nd->log.durable = idx;
                raft_persisted(nd->r, idx, ml_term_at(&nd->log, idx));
            } else {
                nd->hard = ev.hs;
                raft_hard_saved(nd->r, &ev.hs);
            }
            drain(s, nd);
            free(ev.bytes);
        }
        if (s->now >= next_tick) {
            next_tick = s->now + 10000000;
            for (unsigned i = 1; i <= s->n; i++) {
                struct snode *nd = &s->nd[i];
                if (!nd->up && nd->restart_at <= s->now)
                    boot(s, nd);
                if (!nd->up)
                    continue;
                raft_tick(nd->r, local_now(s, nd));
                drain(s, nd);
            }
            /* faults: crashes keep a majority up most of the time, partitions heal */
            if (s->now < quiet && rnd(s, 0, 999) < 3) {
                struct snode *nd = &s->nd[rnd(s, 1, s->n)];
                if (nd->up)
                    crash(s, nd);
            }
            /* membership: now and then before the quiet tail, every 100 ms in it */
            if (s->now >= quiet ? s->now % 100000000 < 10000000 : rnd(s, 0, 999) < 4)
                for (unsigned i = 1; i <= s->n; i++) {
                    struct snode *nd = &s->nd[i];
                    if (nd->up && raft_get_role(nd->r) == RAFT_LEADER) {
                        raft_clock(nd->r, local_now(s, nd));
                        membership(s, nd, s->now >= quiet);
                        drain(s, nd);
                    }
                }
            if (s->now < quiet && !s->heal_pending && rnd(s, 0, 999) < 3) {
                for (unsigned i = 1; i <= s->n; i++)
                    s->nd[i].group = (int)rnd(s, 0, 1);
                s->heal_pending = 1;
                s->heal_at = s->now + rnd(s, 200000000, 3000000000ull);
                s->partitions++;
            } else if (s->heal_pending && (s->now >= s->heal_at || s->now >= quiet)) {
                for (unsigned i = 1; i <= s->n; i++)
                    s->nd[i].group = 0;
                s->heal_pending = 0;
            }
        }
        /* the workload: a proposal at whoever leads, about 200 a second */
        if (s->now < quiet && rnd(s, 0, 4) == 0)
            for (unsigned i = 1; i <= s->n; i++) {
                struct snode *nd = &s->nd[i];
                if (nd->up && raft_get_role(nd->r) == RAFT_LEADER) {
                    raft_clock(nd->r, local_now(s, nd));
                    propose(s, nd);
                    drain(s, nd);
                }
            }
    }
    int ok = !s->fail[0];
    for (unsigned i = 1; ok && i <= s->n; i++) {
        const struct snode *nd = &s->nd[i];
        if (!nd->up || nd->sm.applied != s->applied_max) {
            raft_progress pr = {0};
            for (unsigned j = 1; j <= s->n; j++)
                if (s->nd[j].up && raft_get_role(s->nd[j].r) == RAFT_LEADER)
                    raft_get_progress(s->nd[j].r, nd->id, &pr);
            snprintf(s->fail, sizeof s->fail,
                     "node %u ended %s at %" PRIu64 " of %" PRIu64 " after %.0f quiet seconds (log %" PRIu64 "..%" PRIu64
                     ", commit %" PRIu64 ", leader %u, %u reseeds; the leader's progress: match %" PRIu64
                     " next %" PRIu64 " snapshotting %d)",
                     nd->id, nd->up ? "up" : "down", nd->sm.applied, s->applied_max, (end - quiet) / 1e9,
                     (uint64_t)nd->log.base, (uint64_t)(nd->log.base + nd->log.n),
                     nd->up ? (uint64_t)raft_get_commit(nd->r) : 0, nd->up ? (unsigned)raft_get_leader(nd->r) : 0,
                     nd->reseeds, (uint64_t)pr.match, (uint64_t)pr.next, (int)pr.snapshotting);
            ok = 0;
        }
    }
    /* the quiet tail brings every server back as a voter: a change that never completes would show here */
    for (unsigned i = 1; ok && i <= s->n; i++)
        if (raft_get_role(s->nd[i].r) == RAFT_LEADER && raft_config_voters(raft_get_config(s->nd[i].r)) != s->n &&
            raft_get_config(s->nd[i].r)->n >= 1) {
            snprintf(s->fail, sizeof s->fail, "leader %u ended with %u voters of %u servers", i,
                     raft_config_voters(raft_get_config(s->nd[i].r)), s->n);
            ok = 0;
        }
    if (ok && s->applied_max < 100)
        snprintf(s->fail, sizeof s->fail, "only %" PRIu64 " entries applied", s->applied_max), ok = 0;
    for (unsigned i = 1; !ok && i <= s->n; i++) { /* every server's role, term and configuration, for the diagnosis */
        const struct snode *nd = &s->nd[i];
        size_t at = strlen(s->fail);
        if (!nd->up) {
            snprintf(s->fail + at, sizeof s->fail - at, "; %u down", i);
            continue;
        }
        const raft_config *c = raft_get_config(nd->r);
        at += (size_t)snprintf(s->fail + at, sizeof s->fail - at, "; %u %s term %" PRIu64 " config", i,
                               (const char *[]){"follower", "precandidate", "candidate",
                                                "leader"}[raft_get_role(nd->r) & 3],
                               (uint64_t)raft_get_term(nd->r));
        for (uint32_t k = 0; k < c->n && at < sizeof s->fail; k++)
            at += (size_t)snprintf(s->fail + at, sizeof s->fail - at, " %u%s", c->m[k].id, c->m[k].learner ? "L" : "");
    }
    if (!ok)
        fprintf(stderr, "FAIL metadata simulator seed %" PRIu64 ": %s\n", seed, s->fail);
    else if (verbose)
        printf("seed %" PRIu64 ": %u servers, %" PRIu64 " applied, %" PRIu64 " proposals, %" PRIu64
               " operations took effect, %" PRIu64 " leaders, %" PRIu64 " crashes, %" PRIu64 " partitions, %" PRIu64
               " snapshot installs (%" PRIu64 " cut short, %" PRIu64 " finished from the kept image, %" PRIu64
               " of them with a prefix of the old log or none; %" PRIu64 " logs started again on the snapshot), %" PRIu64
               " membership changes\n",
               seed, s->n, s->applied_max, s->proposals, s->ops_ok, s->leader_changes, s->crashes, s->partitions,
               s->installs, s->cut_installs, s->adopts, s->partial_resets, s->rebases, s->changes);
    for (unsigned i = 1; i <= s->n; i++) {
        struct snode *nd = &s->nd[i];
        if (nd->r)
            raft_free(nd->r);
        ml_release(&nd->log);
        for (size_t k = 0; k < nd->log.n; k++)
            free(nd->log.e[k].data);
        free(nd->log.e);
        free(nd->log.grave);
        meta_free(&nd->sm);
        free(nd->snap);
        free(nd->pend);
        ml_free(&nd->prelog);
    }
    for (size_t i = 0; i < s->nev; i++)
        free(s->ev[i].bytes);
    free(s->ev);
    free(s->ref_hash);
    free(s->vl);
    free(s->drops);
    free(s->uid_name);
    free(s);
    return ok;
}

int main(int argc, char **argv)
{
    const char *dir = argc > 1 && argv[1][0] != '-' ? argv[1] : "/tmp";
    unsigned seeds = 40;
    uint64_t first = 1;
    double seconds = 20;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--seeds") && i + 1 < argc)
            seeds = (unsigned)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--first") && i + 1 < argc)
            first = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--seconds") && i + 1 < argc)
            seconds = strtod(argv[++i], NULL);
    }
    /* The deadline covers the whole run, so it grows with the seeds: a seed of 30 virtual seconds takes about 3.4 s
     * (0.11 s per virtual second, 2026-11-23, i5-13400), and make meta-soak's 5000 seeds about 4.7 hours. One real
     * second per virtual second, never under 1200 s; a fixed 1200 s failed every soak above about 350 seeds. */
    double budget = (double)seeds * (seconds > 0 ? seconds : 0);
    th_watchdog("test_meta", budget > 1e7 ? 10000000L : budget > 1200 ? (long)budget : 1200);
    iso_log_quiet = true;
    int mutant = 0, sim_only = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--sim-only")) /* the simulator's checker alone, to see it catch a planted bug */
            sim_only = 1;
    }
    if (sim_only && !mutant) {
        fprintf(stderr, "test_meta: --sim-only is for --mutant runs\n");
        return 2;
    }
#ifdef META_MUTANTS
    extern uint64_t meta_mutants;
    meta_mutants = mutant ? 1ull << mutant : 0;
#else
    if (mutant) {
        fprintf(stderr, "test_meta: --mutant needs the build with -DMETA_MUTANTS (make test_meta_mut)\n");
        return 2;
    }
#endif
    if (!sim_only) {
        test_state_machine();
        test_replica_set();
        test_shared();
        test_users();
        test_users_limits();
    }
    if (!mutant) {
        const char *b = scratch_make(dir, "test_meta_log");
        test_raft_log(b);
        test_raft_log_damage(b);
        test_hard_state_damage(b);
        test_raft_sim(seeds, first, seconds);
    }
    unsigned bad = 0;
    for (unsigned i = 0; i < seeds && bad < 3; i++)
        bad += !msim_run(first + i, seconds, seeds <= 4);
    failures += (int)bad;
    scratch_done(failures);
    if (failures) {
        fprintf(stderr, "test_meta: %d failures%s\n", failures, mutant ? " (the planted bug was caught)" : "");
        return 1;
    }
    printf("test_meta: ok (%u seeds of %.0f virtual seconds, from seed %llu)\n", seeds, seconds,
           (unsigned long long)first);
    return 0;
}
