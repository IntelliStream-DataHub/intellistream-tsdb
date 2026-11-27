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

    /* member state follows incarnations; OUT -> UP needs a new incarnation; DESTROYED is final */
    meta_op_member_state(&e, 3, META_DOWN, 7, META_DOWN_TIMEOUT, 1000, 1000);
    meta_op_member_state(&e, 3, META_UP, 6, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE);
    CHECK(meta_member(&m, 3)->down_since_ms == 1000 && meta_member(&m, 3)->down_reason == META_DOWN_TIMEOUT);
    meta_op_member_state(&e, 3, META_OUT, 7, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 3, META_UP, 7, META_DOWN_NONE, 0, 0); /* same inc: stale */
    meta_op_member_state(&e, 3, META_UP, 8, META_DOWN_NONE, 0, 0); /* new inc: OUT -> UP allowed (D2) */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE && res[2] == META_OK);
    CHECK(meta_member(&m, 3)->state == META_UP && meta_member(&m, 3)->incarnation == 8 &&
          meta_member(&m, 3)->down_since_ms == 0);
    meta_op_member_state(&e, 3, META_OUT, 8, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_member(&m, 3)->state == META_OUT);

    /* member flags follow incarnations; a node without volumes_ok is no candidate (a witness) */
    meta_op_member_flags(&e, 3, 9, 0); /* out */
    meta_op_member_flags(&e, 77, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_ERR_NOTFOUND);
    meta_op_member_flags(&e, 5, 0, 0x80); /* an unknown flag bit: damage, a fault */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT && meta_member(&m, 5)->flags == META_MF_ALL);
    meta_op_member_state(&e, 5, META_UP, 3, META_DOWN_NONE, 0, 0);
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
     * never those of an earlier run, nor onto another non-DESTROYED member's peer address (OUT keeps it reserved) */
    uint64_t inc2 = meta_member(&m, 2)->incarnation;
    meta_op_member_addr(&e, 2, inc2 + 1, "10.0.1.2:5555", "10.0.1.2:5556");
    meta_op_member_state(&e, 2, META_UP, inc2 + 1, META_DOWN_NONE, 0, 0);
    meta_op_member_addr(&e, 2, inc2, "10.0.0.2:5555", "10.0.0.2:5556"); /* the earlier run's */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_ERR_STALE);
    CHECK(!strcmp(meta_member(&m, 2)->peer, "10.0.1.2:5556") && !strcmp(meta_member(&m, 2)->addr, "10.0.1.2:5555"));
    char peer3[META_ADDR_MAX];
    snprintf(peer3, sizeof peer3, "%s", meta_member(&m, 3)->peer);
    meta_op_member_addr(&e, 2, inc2 + 1, "x:1", "10.0.0.4:5556"); /* member 4's */
    meta_op_member_addr(&e, 2, inc2 + 1, "x:1", peer3); /* member 3 is OUT: keeps its peer reserved (D2) */
    meta_op_member_addr(&e, 3, 8, "x:1", "198.51.100.9:5556");    /* OUT needs a new incarnation */
    meta_op_member_addr(&e, 77, 1, "x:1", "198.51.100.9:5556");
    meta_op_member_addr(&e, 2, inc2 + 1, "", "198.51.100.9:5556");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS && res[1] == META_ERR_EXISTS &&
          res[2] == META_ERR_STALE && res[3] == META_ERR_NOTFOUND && res[4] == META_ERR_INVALID);
    /* once member 3 is DESTROYED, its peer address is freed and MEMBER_ADDR on 3 is refused */
    meta_op_member_state(&e, 3, META_DESTROYED, 8, META_DOWN_NONE, 0, 0);
    meta_op_member_addr(&e, 2, inc2 + 1, "x:1", peer3);
    meta_op_member_addr(&e, 3, 99, "x:1", "198.51.100.9:5556");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_ERR_STATE);
    CHECK(!strcmp(meta_member(&m, 2)->peer, peer3));

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
        CHECK(cheat.slot_home == prev.slot_home && unfrozen.slot_home == prev.slot_home);
        meta_slot_home_mut(&cheat)[100] = 1;
        CHECK(cheat.slot_home != prev.slot_home && prev.slot_home[100] == 0 && unfrozen.slot_home[100] == 0);
        cheat.npend = 0;
        CHECK(meta_check_step(&unfrozen, &cheat, why, sizeof why) != 0);
        unfrozen.npend = 0; /* no pending move at all */
        CHECK(meta_check_step(&unfrozen, &cheat, why, sizeof why) != 0);
        meta_free(&cheat);
        meta_free(&unfrozen);
        meta_free(&prev);
        /* COW slot_home: zero-init state allocates a META_NONE table safely */
        struct meta_state zero = {0};
        uint32_t *zsh = meta_slot_home_mut(&zero);
        CHECK(zsh != NULL && zsh[0] == META_NONE && zsh[META_SLOTS - 1] == META_NONE);
        zsh[42] = 7;
        CHECK(zero.slot_home[42] == 7);
        meta_free(&zero);
        /* COW slot_home: non-slot apply keeps the shared buffer, and a malformed entry after a slot write rolls back */
        meta_copy(&prev, &m);
        CHECK(prev.slot_home == m.slot_home);
        meta_op_member_flags(&e, 2, meta_member(&m, 2)->incarnation, META_MF_ALL);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(prev.slot_home == m.slot_home);
        meta_free(&prev);
        /* rollback on META_ERR_FORMAT after mutating slot_home in scratch */
        uint32_t h100_before = m.slot_home[100];
        meta_op_slot_move_pending(&e, 100, h100_before, h100_before ^ 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_slot_freeze(&e, 100, h100_before, meta_volume(&m, h100_before)->epoch);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_slot_move(&e, 100, h100_before ^ 1);
        meta_op_domain_state(&e, "bad", 9); /* triggers META_ERR_FORMAT after scratch cloned & mutated slot_home */
        CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT && m.slot_home[100] == h100_before);
        meta_op_slot_move_cancel(&e, 100);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && m.slot_home[100] == h100_before);
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
        CHECK(meta_snapshot_version(img, len) == META_SNAP_VERSION && META_SNAP_VERSION == 10);
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
        CHECK(e.len >= 8 && !meta_entry_older(e.buf, e.len) && memcmp(e.buf, "TME7", 4) == 0);
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

/* ================================================================ erasure-coded volumes (R8, snapshot version 6) */

/* VOLUME_ADD of an EC volume: replicas `nodes` (the log group), k + m and the stripe group the proposer picked */
/* a VOLUME_ADD of redundancy `red` with these k, m and group (a replicas body with EC fields: refused) */
static void red_add(struct meta_entry *e, uint32_t number, const char *name, uint32_t min_isr, const uint32_t *nodes,
                    uint32_t n, int kind, uint8_t red, uint8_t k, uint8_t mm, const uint32_t *stripe, uint32_t ns)
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
        snprintf(a.st.export, sizeof a.st.export, "nfs0:/pool/%s", name);
        snprintf(a.st.fstype, sizeof a.st.fstype, "nfs4");
        snprintf(a.st.group, sizeof a.st.group, "g0");
    }
    a.redundancy = red;
    a.ec_k = k;
    a.ec_m = mm;
    a.nstripe = ns;
    memcpy(a.stripe, stripe, ns * sizeof *stripe);
    meta_op_volume_add(e, &a);
}

static void ec_add(struct meta_entry *e, uint32_t number, const char *name, uint32_t min_isr, const uint32_t *nodes,
                   uint32_t n, int kind, uint8_t k, uint8_t mm, const uint32_t *stripe, uint32_t ns)
{
    red_add(e, number, name, min_isr, nodes, n, kind, META_RED_EC, k, mm, stripe, ns);
}

static void test_ec(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[16];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 8);
    meta_op_member_state(&e, 8, META_OUT, 1, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    uint32_t log3[3] = {1, 2, 3}, g6[6] = {2, 3, 4, 5, 6, 7};

    CHECK(meta_volume_min_isr(META_RED_EC, 2, 3, 1) == 2 && meta_volume_min_isr(META_RED_EC, 4, 1, 1) == 1 &&
          meta_volume_min_isr(META_RED_REPLICAS, 0, 3, 2) == 2 && meta_volume_min_isr(META_RED_REPLICAS, 0, 1, 3) == 1);

    /* refused at apply: a group that repeats a node, names an unknown or an out member, k + m positions not given,
     * k or m out of range, a shared volume, min_isr below m (mutant minisrone gives every EC volume min_isr 1); and a
     * replicas body that carries k, m and a group (a proposer that encodes an EC add as replicas) */
    uint32_t dup[6] = {2, 3, 4, 5, 6, 2}, unk[6] = {2, 3, 4, 5, 6, 9}, out[6] = {2, 3, 4, 5, 6, 8};
    uint32_t g20[20], g5[5] = {2, 3, 4, 5, 6};
    for (uint32_t i = 0; i < 20; i++)
        g20[i] = i + 1;
    ec_add(&e, 0, "e0", 2, log3, 3, META_VOL_REPLICATED, 4, 2, dup, 6);
    ec_add(&e, 1, "e1", 2, log3, 3, META_VOL_REPLICATED, 4, 2, unk, 6);
    ec_add(&e, 2, "e2", 2, log3, 3, META_VOL_REPLICATED, 4, 2, out, 6);
    ec_add(&e, 3, "e3", 2, log3, 3, META_VOL_REPLICATED, 4, 2, g5, 5);
    ec_add(&e, 4, "e4", 2, log3, 3, META_VOL_REPLICATED, 0, 2, g20, 2);
    ec_add(&e, 5, "e5", 2, log3, 3, META_VOL_REPLICATED, 17, 3, g20, 20);
    ec_add(&e, 6, "e6", 2, log3, 3, META_VOL_SHARED, 4, 2, g6, 6);
    ec_add(&e, 7, "e7", 1, log3, 3, META_VOL_REPLICATED, 4, 2, g6, 6);
    red_add(&e, 8, "e8", 2, log3, 3, META_VOL_REPLICATED, META_RED_REPLICAS, 4, 2, g6, 6);
    CHECK(apply1(&m, &e, res, 16) == META_OK);
    for (int i = 0; i < 9; i++)
        CHECK(res[i] == META_ERR_INVALID);
    CHECK(m.nvolumes == 0);
    if (m.nvolumes) { /* what follows assumes an empty state: stop at this failure, not at a crash further on */
        meta_entry_free(&e);
        meta_free(&m);
        return;
    }

    /* taken: the group is the body's (mutant stripeatapply takes this copy's first members), version 1, min_isr as
     * given; a 1-replica EC volume (TSDB_CLUSTER_WRITES) has min_isr 1 */
    meta_copy(&prev, &m);
    uint32_t one[1] = {4};
    ec_add(&e, 0, "e0", 2, log3, 3, META_VOL_REPLICATED, 4, 2, g6, 6);
    ec_add(&e, 1, "e1", 1, one, 1, META_VOL_REPLICATED, 2, 1, g6 + 3, 3);
    vol_add(&e, META_VOL_REPLICATED, 2, "r2", 1, log3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    const struct meta_volume *v0 = meta_volume(&m, 0), *v1 = meta_volume(&m, 1), *r2 = meta_volume(&m, 2);
    CHECK(v0 && v0->redundancy == META_RED_EC && v0->ec_k == 4 && v0->ec_m == 2 && v0->nstripe == 6 &&
          memcmp(v0->stripe, g6, sizeof g6) == 0 && v0->stripe_version == 1 && !v0->gc_suspended && !v0->gc_gen &&
          v0->min_isr == 2);
    CHECK(v1 && v1->nstripe == 3 && v1->stripe[0] == 5 && v1->stripe[2] == 7 && v1->min_isr == 1);
    CHECK(r2 && r2->redundancy == META_RED_REPLICAS && !r2->nstripe && !r2->stripe_version);
    /* a REPLICA_REMOVE that would leave fewer replicas than min_isr is refused: r3 (min_isr 3) and r4 (min_isr 2), each
     * with its ISR shrunk to the leader, so that node 3 is neither the leader nor in the ISR; r4 lets it go */
    {
        uint32_t lead1[1] = {1};
        vol_add(&e, META_VOL_REPLICATED, 3, "r3", 3, log3, 3);
        vol_add(&e, META_VOL_REPLICATED, 4, "r4", 2, log3, 3);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
        for (uint32_t n = 3; n <= 4; n++) {
            const struct meta_volume *rv = meta_volume(&m, n);
            CHECK(rv && rv->leader == 1);
            if (!rv)
                continue;
            isr_chg(&e, rv, rv->epoch, rv->isr_version, lead1, 1, 0);
            CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, n)->nisr == 1);
        }
        meta_op_replica_remove(&e, 3, meta_volume(&m, 3)->epoch, 3);
        meta_op_replica_remove(&e, 4, meta_volume(&m, 4)->epoch, 3);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_OK &&
              meta_volume(&m, 3)->nrep == 3 && meta_volume(&m, 4)->nrep == 2);
    }

    /* STRIPE_SET: stale version, a position past the group, a node already in it, an out or unknown node, a
     * replicas volume: refused; else one position moves and the version goes up by one (mutant stripeuncond takes a
     * stale one) */
    meta_op_stripe_set(&e, 0, 1, 1, META_STRIPE_WHOLE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_stripe_set(&e, 0, 1, 1, META_STRIPE_WHOLE, 0, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_stripe_set(&e, 0, 6, 1, META_STRIPE_WHOLE, 0, 1);
    meta_op_stripe_set(&e, 0, 1, 4, META_STRIPE_WHOLE, 0, 1);
    meta_op_stripe_set(&e, 0, 1, 8, META_STRIPE_WHOLE, 0, 1);
    meta_op_stripe_set(&e, 0, 1, 9, META_STRIPE_WHOLE, 0, 1);
    meta_op_stripe_set(&e, 2, 0, 4, META_STRIPE_WHOLE, 0, 0);
    meta_op_stripe_set(&e, 9, 0, 4, META_STRIPE_WHOLE, 0, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && res[4] == META_ERR_INVALID &&
          res[5] == META_ERR_NOTFOUND);
    CHECK(meta_volume(&m, 0)->stripe_version == 1 && memcmp(meta_volume(&m, 0)->stripe, g6, sizeof g6) == 0);
    meta_copy(&prev, &m);
    meta_op_stripe_set(&e, 0, 1, 1, META_STRIPE_WHOLE, 0, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v0 = meta_volume(&m, 0);
    CHECK(v0->stripe[1] == 1 && v0->stripe[0] == 2 && v0->stripe[2] == 4 && v0->stripe_version == 2 &&
          v0->stripe_pos[1].state == META_STRIPE_WHOLE && v0->stripe_pos[1].set_ver == 2);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    meta_op_stripe_set(&e, 0, 1, 1, META_STRIPE_WHOLE, 0, 2); /* the same node again: the version still moves */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->stripe_version == 3);

    /* the checker: a position moved without a bump, two positions in one bump, the version back, a repeated node */
    {
        struct meta_state bad;
        meta_copy(&bad, &m);
        struct meta_volume *bv = (struct meta_volume *)meta_volume(&bad, 0);
        bv->stripe[3] = 1;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        bv->stripe[3] = 6;
        bv->stripe[4] = 3;
        bv->stripe[5] = 8;
        bv->stripe_version++;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        bv->stripe[4] = 6;
        bv->stripe[5] = 7;
        bv->stripe_version -= 2;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        bv->stripe_version += 1;
        bv->gc_suspended = 0;
        bv->min_isr = 4; /* more than its replicas */
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
    }

    /* GC_SUSPEND and GC_RESUME: INVALID on a replicas volume, NOTFOUND on none; a suspend is idempotent and shares
     * an entry with a touch of the volume, a resume clears it and raises gc_gen */
    meta_op_gc_suspend(&e, 2);
    meta_op_gc_resume(&e, 2);
    meta_op_gc_suspend(&e, 9);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_NOTFOUND);
    meta_copy(&prev, &m);
    meta_op_gc_suspend(&e, 0);
    meta_op_gc_suspend(&e, 0);
    meta_op_stripe_set(&e, 0, 1, 1, META_STRIPE_WHOLE, 0, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK &&
          meta_volume(&m, 0)->gc_suspended && meta_volume(&m, 0)->stripe_version == 4);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    {
        struct meta_state bad;
        meta_copy(&bad, &m);
        ((struct meta_volume *)meta_volume(&bad, 0))->gc_suspended = 0; /* cleared by anything but a resume */
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
    }
    meta_copy(&prev, &m);
    meta_op_gc_resume(&e, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && !meta_volume(&m, 0)->gc_suspended &&
          meta_volume(&m, 0)->gc_gen == 1);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* force-leader: GC_SUSPEND with the ISR_CHANGE that adds the node, in one entry (mutant forcenosuspend); a
     * replicas volume gets the ISR_CHANGE alone */
    uint32_t lead[1] = {1};
    isr_chg(&e, meta_volume(&m, 0), meta_volume(&m, 0)->epoch, meta_volume(&m, 0)->isr_version, lead, 1, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->nisr == 1);
    isr_chg(&e, meta_volume(&m, 2), meta_volume(&m, 2)->epoch, meta_volume(&m, 2)->isr_version, lead, 1, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_force(&e, meta_volume(&m, 0), 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    CHECK(meta_volume(&m, 0)->gc_suspended && meta_volume(&m, 0)->nisr == 2 &&
          meta_volume_in_isr(meta_volume(&m, 0), 3));
    meta_op_volume_force(&e, meta_volume(&m, 2), 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 2)->nisr == 2 &&
          !meta_volume(&m, 2)->gc_suspended);
    meta_op_gc_resume(&e, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->gc_gen == 2);

    /* RESTORE suspends every EC volume (mutant gcresumeany: a promotion clears it) */
    meta_copy(&prev, &m);
    meta_op_restore(&e, m.restore_gen + 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->gc_suspended &&
          meta_volume(&m, 1)->gc_suspended && !meta_volume(&m, 2)->gc_suspended);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    {
        const struct meta_volume *v = meta_volume(&m, 0);
        uint32_t fenced[2] = {2, 3}, answer[1] = {3};
        promo(&e, v, v->epoch, 3, fenced, 2, answer, 1, 20);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->leader == 3);
        CHECK(meta_volume(&m, 0)->gc_suspended);
    }

    /* the snapshot: every EC field back */
    {
        uint8_t *img;
        size_t len;
        meta_snapshot_encode(&m, &img, &len);
        struct meta_state back;
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&m));
        const struct meta_volume *a = meta_volume(&m, 0), *b = meta_volume(&back, 0);
        CHECK(b && b->redundancy == META_RED_EC && b->ec_k == 4 && b->ec_m == 2 && b->nstripe == 6 &&
              memcmp(a->stripe, b->stripe, sizeof a->stripe) == 0 && b->stripe_version == a->stripe_version &&
              b->gc_suspended == 1 && b->gc_gen == 2 && meta_volume(&back, 1)->gc_suspended &&
              meta_volume(&back, 2)->redundancy == META_RED_REPLICAS);
        meta_free(&back);
        free(img);
    }

    /* an entry of a build of snapshot version 5 ("TME2") or 6 ("TME3") is named as older and refused */
    {
        meta_op_gc_resume(&e, 0);
        meta_entry_finish(&e);
        uint64_t h = meta_hash(&m);
        memcpy(e.buf, "TME2", 4);
        CHECK(meta_entry_older(e.buf, e.len));
        CHECK(meta_apply(&m, m.applied + 1, 1, e.buf, e.len, res, 8) == META_ERR_FORMAT && meta_hash(&m) == h);
        memcpy(e.buf, "TME3", 4);
        CHECK(meta_entry_older(e.buf, e.len));
        CHECK(meta_apply(&m, m.applied + 1, 1, e.buf, e.len, res, 8) == META_ERR_FORMAT && meta_hash(&m) == h);
        memcpy(e.buf, "TME4", 4);
        CHECK(meta_entry_older(e.buf, e.len));
        CHECK(meta_apply(&m, m.applied + 1, 1, e.buf, e.len, res, 8) == META_ERR_FORMAT && meta_hash(&m) == h);
        memcpy(e.buf, "TME5", 4);
        CHECK(meta_entry_older(e.buf, e.len));
        CHECK(meta_apply(&m, m.applied + 1, 1, e.buf, e.len, res, 8) == META_ERR_FORMAT && meta_hash(&m) == h);
        memcpy(e.buf, "TME6", 4);
        CHECK(meta_entry_older(e.buf, e.len));
        CHECK(meta_apply(&m, m.applied + 1, 1, e.buf, e.len, res, 8) == META_ERR_FORMAT && meta_hash(&m) == h);
        meta_entry_free(&e);
        meta_entry_init(&e);
    }
    meta_entry_free(&e);
    meta_free(&m);
}

/* ================================================================ R9 metadata (snapshot version 7) */

struct test_live_ctx {
    uint32_t dead_mask;
};

static int test_live_fn(const void *ctx, uint32_t node)
{
    const struct test_live_ctx *c = ctx;
    return !(c->dead_mask & (1u << node));
}

static void test_r9_meta(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[16];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 8);

    /* 1. Member DOWN timers & reasons: every reason accepted, invalid/future refused, cleared on UP/DRAINING/OUT */
    CHECK(!strcmp(meta_member_state_name(META_DESTROYED), "destroyed") && !strcmp(meta_member_state_name(5), "?"));
    CHECK(!strcmp(meta_down_reason_name(META_DOWN_TIMEOUT), "timeout") &&
          !strcmp(meta_down_reason_name(META_DOWN_FAILSTOP), "failstop") &&
          !strcmp(meta_down_reason_name(META_DOWN_FAULT), "fault") &&
          !strcmp(meta_down_reason_name(META_DOWN_DRAIN), "drain") && !strcmp(meta_down_reason_name(9), "?"));
    CHECK(!strcmp(meta_stripe_state_name(META_STRIPE_WHOLE), "whole") &&
          !strcmp(meta_stripe_state_name(META_STRIPE_FILLING), "filling") && !strcmp(meta_stripe_state_name(2), "?"));

    meta_op_member_state(&e, 2, META_DOWN, 1, META_DOWN_NONE, 1000, 1000);       /* reason none */
    meta_op_member_state(&e, 2, META_DOWN, 1, META_DOWN_TIMEOUT, 0, 1000);       /* since 0 */
    meta_op_member_state(&e, 2, META_DOWN, 1, META_DOWN_TIMEOUT, 2001, 2000);    /* future of op clock */
    meta_op_member_state(&e, 2, META_UP, 1, META_DOWN_TIMEOUT, 0, 0);            /* reason on UP */
    meta_op_member_state(&e, 2, META_UP, 1, META_DOWN_NONE, 1000, 1000);         /* since on UP */
    meta_op_member_state(&e, 2, META_DESTROYED, 1, META_DOWN_NONE, 0, 0);        /* DESTROYED from UP */
    CHECK(apply1(&m, &e, res, 16) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && res[4] == META_ERR_INVALID &&
          res[5] == META_ERR_STATE);
    meta_op_member_state(&e, 2, META_DOWN, 1, 9, 1000, 1000); /* unknown reason: format error */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
    meta_op_member_state(&e, 2, 9, 1, META_DOWN_NONE, 0, 0); /* unknown state: format error */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);

    meta_copy(&prev, &m);
    meta_op_member_state(&e, 2, META_DOWN, 1, META_DOWN_FAILSTOP, 5000, 5500);
    meta_op_member_state(&e, 3, META_DOWN, 1, META_DOWN_FAULT, 6000, 6000);
    meta_op_member_state(&e, 4, META_DOWN, 1, META_DOWN_DRAIN, 7000, 7100);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK);
    CHECK(meta_member(&m, 2)->down_since_ms == 5000 && meta_member(&m, 2)->down_reason == META_DOWN_FAILSTOP &&
          meta_member(&m, 2)->down_clock_ms == 5500); /* catches MUT(59) downsinceapply */
    CHECK(meta_member(&m, 3)->down_since_ms == 6000 && meta_member(&m, 3)->down_reason == META_DOWN_FAULT);
    CHECK(meta_member(&m, 4)->down_since_ms == 7000 && meta_member(&m, 4)->down_reason == META_DOWN_DRAIN);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    /* repeated DOWN at the same incarnation keeps earlier down_since_ms while allowing reason escalation */
    meta_copy(&prev, &m);
    meta_op_member_state(&e, 3, META_DOWN, 1, META_DOWN_FAILSTOP, 9000, 9000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_member(&m, 3)->down_since_ms == 6000 && meta_member(&m, 3)->down_reason == META_DOWN_FAILSTOP);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    /* an earlier down_since_ms at the same incarnation is kept (min); a newer incarnation resets it */
    meta_op_member_state(&e, 3, META_DOWN, 1, META_DOWN_FAILSTOP, 4500, 9000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_member(&m, 3)->down_since_ms == 4500);
    meta_op_member_state(&e, 3, META_DOWN, 2, META_DOWN_TIMEOUT, 9500, 9500);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_member(&m, 3)->down_since_ms == 9500 &&
          meta_member(&m, 3)->down_reason == META_DOWN_TIMEOUT);
    /* DESTROYED from DOWN or DRAINING is refused */
    meta_op_member_state(&e, 2, META_DESTROYED, 1, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 4, META_DRAINING, 1, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 4, META_DESTROYED, 1, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_OK &&
          res[2] == META_ERR_STATE);
    CHECK(meta_member(&m, 4)->state == META_DRAINING && !meta_member(&m, 4)->down_since_ms &&
          meta_member(&m, 4)->down_reason == META_DOWN_NONE);

    /* 2. NODE_HOLD: bounded by META_HOLD_MAX_MS (86,400,000), refused on OUT/DESTROYED, until==0 releases */
    meta_copy(&prev, &m);
    uint64_t clk = 1000000000ull;
    meta_op_node_hold(&e, 99, clk + 1000, clk);                     /* unknown member */
    meta_op_node_hold(&e, 2, clk - 1, clk);                         /* until < op_clock */
    meta_op_node_hold(&e, 2, 1000, 0);                              /* op_clock 0 */
    meta_op_node_hold(&e, 2, clk + META_HOLD_MAX_MS + 1, clk);      /* past 24 h (MUT(56) holdunbounded) */
    meta_op_node_hold(&e, 2, clk + META_HOLD_MAX_MS, clk);          /* exact 24 h: allowed */
    meta_op_node_hold(&e, 4, clk + 60000, clk);                     /* hold on DRAINING: allowed */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_NOTFOUND && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && res[4] == META_OK && res[5] == META_OK);
    CHECK(meta_member(&m, 2)->hold_until_ms == clk + META_HOLD_MAX_MS && meta_member(&m, 2)->hold_clock_ms == clk);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    /* hold survives DOWN -> UP; release (until == 0) clears it */
    meta_op_member_state(&e, 2, META_UP, 2, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK &&
          meta_member(&m, 2)->hold_until_ms == clk + META_HOLD_MAX_MS);
    meta_op_node_hold(&e, 2, 0, clk + 500);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && !meta_member(&m, 2)->hold_until_ms &&
          !meta_member(&m, 2)->hold_clock_ms);

    /* 3. OUT reversible (new incarnation), OUT preserves addr & ident, DESTROYED frees them and is final */
    {
        struct meta_member mb;
        member(&mb, 9);
        memset(mb.ident, 0x99, 16);
        meta_op_member_add(&e, &mb);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    }
    meta_op_node_hold(&e, 9, clk + 10000, clk);
    meta_op_member_state(&e, 9, META_OUT, 1, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    CHECK(meta_member(&m, 9)->state == META_OUT && !meta_member(&m, 9)->hold_until_ms); /* OUT clears hold */
    /* hold / DOWN / DRAINING / OUT refused while OUT; duplicate peer & ident refused while OUT */
    meta_op_node_hold(&e, 9, clk + 1000, clk);
    meta_op_node_hold(&e, 9, 0, clk);
    meta_op_member_state(&e, 9, META_DOWN, 2, META_DOWN_TIMEOUT, 1000, 1000);
    meta_op_member_state(&e, 9, META_DRAINING, 2, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 9, META_OUT, 2, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 9, META_UP, 1, META_DOWN_NONE, 0, 0); /* same incarnation: stale */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_ERR_STATE &&
          res[2] == META_ERR_STATE && res[3] == META_ERR_STATE && res[4] == META_ERR_STATE &&
          res[5] == META_ERR_STALE);
    {
        struct meta_member dup;
        member(&dup, 10);
        strcpy(dup.peer, meta_member(&m, 9)->peer);
        meta_op_member_add(&e, &dup); /* peer reserved by OUT member 9 */
        member(&dup, 10);
        memset(dup.ident, 0x99, 16);
        meta_op_member_add(&e, &dup); /* ident reserved by OUT member 9 */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS && res[1] == META_ERR_EXISTS);
    }
    /* OUT -> UP with new incarnation succeeds (catches MUT(55) outfinal); MEMBER_ADDR paired before UP also works */
    meta_copy(&prev, &m);
    meta_op_member_addr(&e, 9, 2, "198.51.100.9:5555", "198.51.100.9:5556");
    meta_op_member_state(&e, 9, META_UP, 2, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    CHECK(meta_member(&m, 9)->state == META_UP && meta_member(&m, 9)->incarnation == 2 &&
          !strcmp(meta_member(&m, 9)->peer, "198.51.100.9:5556"));
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    /* OUT -> DESTROYED is final (catches MUT(58) destroyedback) and frees peer & ident */
    meta_op_member_state(&e, 9, META_OUT, 2, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 9, META_DESTROYED, 1, META_DOWN_NONE, 0, 0); /* stale inc */
    meta_op_member_state(&e, 9, META_DESTROYED, 2, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STALE && res[2] == META_OK);
    CHECK(meta_member(&m, 9)->state == META_DESTROYED);
    meta_op_member_state(&e, 9, META_UP, 3, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 9, META_OUT, 3, META_DOWN_NONE, 0, 0);
    meta_op_node_hold(&e, 9, clk + 1000, clk);
    meta_op_member_flags(&e, 9, 3, META_MF_ALL);
    meta_op_member_addr(&e, 9, 3, "203.0.113.9:5555", "203.0.113.9:5556");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_ERR_STATE &&
          res[2] == META_ERR_STATE && res[3] == META_ERR_STATE && res[4] == META_ERR_STATE);
    {
        struct meta_member reuse;
        member(&reuse, 10);
        strcpy(reuse.peer, "198.51.100.9:5556");
        memset(reuse.ident, 0x99, 16);
        meta_op_member_add(&e, &reuse); /* freed by DESTROYED member 9 */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_member(&m, 10) != NULL);
    }

    /* 4. Stripe position fill states, STRIPE_WHOLE, and meta_vol_margins */
    meta_op_member_state(&e, 3, META_UP, 2, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 4, META_UP, 2, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    uint32_t log3[3] = {1, 2, 3}, g6[6] = {1, 2, 3, 4, 5, 6};
    ec_add(&e, 0, "ec0", 2, log3, 3, META_VOL_REPLICATED, 4, 2, g6, 6);
    vol_add(&e, META_VOL_REPLICATED, 1, "rep1", 1, log3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    const struct meta_volume *v = meta_volume(&m, 0);
    for (uint32_t p = 0; p < 6; p++)
        CHECK(v->stripe_pos[p].state == META_STRIPE_WHOLE && v->stripe_pos[p].prev == 0 &&
              v->stripe_pos[p].set_ver == 1);
    int yg = 99, sd = 99;
    meta_vol_margins(&m, meta_volume(&m, 1), NULL, &yg, &sd);
    CHECK(yg == 2 && sd == 0); /* replicas: nisr(3) - min_isr(1) = 2, settled 0 */
    meta_vol_margins(&m, v, NULL, &yg, &sd);
    CHECK(yg == 1 && sd == 2); /* EC 4+2: nisr(3) - min_isr(2) = 1, 6 whole UP - 4 = 2 */

    /* STRIPE_SET filling + refusals */
    meta_op_stripe_set(&e, 0, 5, 7, 2, 6, 1); /* invalid state: format error */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
    meta_op_stripe_set(&e, 0, 5, 7, META_STRIPE_WHOLE, 6, 1);    /* prev on WHOLE */
    meta_op_stripe_set(&e, 0, 5, 7, META_STRIPE_FILLING, 7, 1);  /* prev == node */
    meta_op_stripe_set(&e, 0, 5, 7, META_STRIPE_FILLING, 99, 1); /* unknown prev */
    meta_op_stripe_set(&e, 0, 5, 9, META_STRIPE_FILLING, 6, 1);  /* DESTROYED node */
    meta_op_stripe_whole(&e, 0, 5, 1);                           /* already WHOLE */
    meta_op_stripe_whole(&e, 1, 0, 0);                           /* replicas volume */
    meta_op_stripe_whole(&e, 99, 0, 1);                          /* unknown volume */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && res[4] == META_ERR_INVALID &&
          res[5] == META_ERR_INVALID && res[6] == META_ERR_NOTFOUND);

    /* node 6 goes OUT, pos 5 reassigned to 7 as FILLING with prev 6 */
    meta_op_member_state(&e, 6, META_OUT, 1, META_DOWN_NONE, 0, 0);
    meta_op_stripe_set(&e, 0, 5, 7, META_STRIPE_FILLING, 6, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->stripe[5] == 7 && v->stripe_version == 2 && v->stripe_pos[5].state == META_STRIPE_FILLING &&
          v->stripe_pos[5].prev == 6 && v->stripe_pos[5].set_ver == 2);
    /* filling counts as missing for objects below set_ver, even though node 7 is UP */
    meta_vol_margins(&m, v, NULL, &yg, &sd);
    CHECK(yg == 1 && sd == 1);
    /* DRAINING still counts present; DOWN counts missing; liveness overlay marks UP node missing */
    meta_op_member_state(&e, 4, META_DRAINING, 2, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 5, META_DOWN, 1, META_DOWN_TIMEOUT, 8000, 8000);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
    meta_vol_margins(&m, meta_volume(&m, 0), NULL, &yg, &sd);
    CHECK(sd == 0); /* 1,2,3,4(draining) present; 5(DOWN) & 7(FILLING) missing -> 4 - 4 = 0 */
    {
        struct test_live_ctx lc = {.dead_mask = (1u << 1)}; /* node 1 UP in meta, unlive in overlay */
        struct meta_liveness lv = {.live = test_live_fn, .ctx = &lc};
        meta_vol_margins(&m, meta_volume(&m, 0), &lv, NULL, &sd);
        CHECK(sd == -1);
    }
    meta_op_member_state(&e, 5, META_UP, 2, META_DOWN_NONE, 0, 0);
    meta_op_member_state(&e, 4, META_UP, 2, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);

    /* STRIPE_SET cannot bypass STRIPE_WHOLE (FILLING -> WHOLE refused); STRIPE_WHOLE checks exact stripe_version */
    meta_op_stripe_set(&e, 0, 5, 7, META_STRIPE_WHOLE, 0, 2);
    meta_op_stripe_whole(&e, 0, 5, 1); /* stale version (MUT(57) wholeanyversion) */
    meta_op_stripe_whole(&e, 0, 5, 3); /* future version */
    meta_op_stripe_whole(&e, 0, 6, 2); /* pos out of range */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_STALE &&
          res[2] == META_ERR_STALE && res[3] == META_ERR_INVALID);
    meta_copy(&prev, &m);
    meta_op_stripe_whole(&e, 0, 5, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->stripe_pos[5].state == META_STRIPE_WHOLE && v->stripe_version == 2 && v->stripe_whole_gen == 1 &&
          v->stripe_whole_ver == 2 && v->stripe_pos[5].prev == 6 && v->stripe_pos[5].set_ver == 2);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    meta_vol_margins(&m, v, NULL, &yg, &sd);
    CHECK(sd == 2); /* whole again -> full margin restored */
    /* shared volume: young == 0, settled == 0 (no replicated young copies, matching margins.c young_na) */
    {
        struct meta_volume sv = *meta_volume(&m, 1);
        sv.kind = META_VOL_SHARED;
        yg = 99;
        sd = 99;
        meta_vol_margins(&m, &sv, NULL, &yg, &sd);
        CHECK(yg == 0 && sd == 0);
    }

    /* 5. D23 settings defaults, bounds, and SNAP_VERSION 7 refusing version 6 by name */
    {
        struct meta_settings st;
        meta_settings_default(&st);
        CHECK(st.rebuild_after_s == 1800 && st.opening_hold_s == 3600 && st.failstop_out_s == 60 &&
              st.out_guard_pct == 25 && st.out_guard_hold_s == 7200 && st.repl_retain_down_gb == 64 &&
              st.young_high_pct == 90 && st.balance_mb_s == 0 && st.rebuild_concurrency == 4);
        struct { int f; uint32_t v; int ok; } cases[] = {
            {0, 0, 0},     {0, 1, 1},     {1, 0, 1},     {1, 86400, 1}, {1, 86401, 0}, {2, 0, 0},     {2, 1, 1},
            {3, 0, 0},     {3, 1, 1},     {3, 100, 1},   {3, 101, 0},   {4, 0, 0},     {4, 1, 1},     {4, 86400, 1},
            {4, 86401, 0}, {5, 0, 1},     {5, 128, 1},   {6, 9, 0},     {6, 10, 1},    {6, 99, 1},    {6, 100, 0},
            {7, 0, 1},     {7, 500, 1},   {8, 0, 0},     {8, 1, 1},     {8, 64, 1},    {8, 65, 0},
        };
        for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
            meta_settings_default(&st);
            if (cases[i].f == 6 && cases[i].v <= st.young_move_pct) {
                st.young_warn_pct = 8;
                st.young_move_pct = 9;
            }
            uint32_t *ptrs[9] = {&st.rebuild_after_s,     &st.opening_hold_s, &st.failstop_out_s,
                                 &st.out_guard_pct,       &st.out_guard_hold_s, &st.repl_retain_down_gb,
                                 &st.young_high_pct,      &st.balance_mb_s,   &st.rebuild_concurrency};
            *ptrs[cases[i].f] = cases[i].v;
            meta_op_settings(&e, &st);
            CHECK(apply1(&m, &e, res, 8) == META_OK && (res[0] == META_OK) == cases[i].ok);
        }
        /* set non-defaults, plus active DOWN & hold & FILLING, and verify snapshot v7 round-trip + v6 refusal */
        meta_settings_default(&st);
        st.rebuild_after_s = 30;
        st.opening_hold_s = 120;
        st.failstop_out_s = 5;
        st.out_guard_pct = 30;
        st.out_guard_hold_s = 900;
        st.repl_retain_down_gb = 32;
        st.young_high_pct = 85;
        st.balance_mb_s = 100;
        st.rebuild_concurrency = 8;
        meta_op_settings(&e, &st);
        meta_op_member_state(&e, 2, META_DOWN, 2, META_DOWN_FAILSTOP, 9000, 9100);
        meta_op_node_hold(&e, 2, clk + 3600000, clk);
        meta_op_node_replace(&e, 2, 10);
        meta_op_stripe_set(&e, 0, 4, 8, META_STRIPE_FILLING, 5, 2);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK &&
              res[3] == META_OK && res[4] == META_OK);
        uint8_t *img;
        size_t len;
        meta_snapshot_encode(&m, &img, &len);
        CHECK(meta_snapshot_version(img, len) == 10);
        struct meta_state back;
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&m));
        CHECK(back.set.rebuild_after_s == 30 && back.set.opening_hold_s == 120 && back.set.failstop_out_s == 5 &&
              back.set.out_guard_pct == 30 && back.set.out_guard_hold_s == 900 && back.set.repl_retain_down_gb == 32 &&
              back.set.young_high_pct == 85 && back.set.balance_mb_s == 100 && back.set.rebuild_concurrency == 8);
        CHECK(meta_member(&back, 2)->down_since_ms == 9000 && meta_member(&back, 2)->down_reason == META_DOWN_FAILSTOP &&
              meta_member(&back, 2)->down_clock_ms == 9100 && meta_member(&back, 2)->hold_until_ms == clk + 3600000 &&
              meta_member(&back, 2)->hold_clock_ms == clk && meta_member(&back, 2)->replaced_by == 10 &&
              meta_member(&back, 9)->state == META_DESTROYED);
        const struct meta_volume *bv = meta_volume(&back, 0);
        CHECK(bv->stripe[4] == 8 && bv->stripe_pos[4].state == META_STRIPE_FILLING && bv->stripe_pos[4].prev == 5 &&
              bv->stripe_pos[4].set_ver == 3 && bv->stripe_whole_gen == 1 && bv->stripe_whole_ver == 2);
        /* snapshot version 7 and 6 refused and named */
        uint32_t seven = 7, six = 6, crc;
        memcpy(img + 4, &seven, 4);
        crc = iso_crc32c(0, img, len - 4);
        memcpy(img + len - 4, &crc, 4);
        CHECK(meta_snapshot_version(img, len) == 7 && meta_snapshot_decode(&back, img, len) == META_ERR_FORMAT);
        memcpy(img + 4, &six, 4);
        crc = iso_crc32c(0, img, len - 4);
        memcpy(img + len - 4, &crc, 4);
        CHECK(meta_snapshot_version(img, len) == 6 && meta_snapshot_decode(&back, img, len) == META_ERR_FORMAT);
        meta_free(&back);
        free(img);
    }

    /* 5b. NODE_REPLACE: refusals, volume overlap, clearing, OUT + replace, DESTROYED clears replaced_by */
    {
        meta_op_node_replace(&e, 99, 10);  /* unknown old */
        meta_op_node_replace(&e, 2, 99);   /* unknown new */
        meta_op_node_replace(&e, 2, 2);    /* self */
        meta_op_node_replace(&e, 9, 10);   /* old is DESTROYED */
        meta_op_node_replace(&e, 2, 6);    /* new is OUT */
        meta_op_node_replace(&e, 2, 9);    /* new is DESTROYED */
        meta_op_node_replace(&e, 2, 1);    /* new holds volume 0 alongside old (overlap) */
        meta_op_node_replace(&e, 6, 10);   /* old is OUT, new is UP: allowed */
        meta_op_node_replace(&e, 2, 0);    /* clear replaced_by on member 2 */
        CHECK(apply1(&m, &e, res, 16) == META_OK && res[0] == META_ERR_NOTFOUND && res[1] == META_ERR_NOTFOUND &&
              res[2] == META_ERR_INVALID && res[3] == META_ERR_STATE && res[4] == META_ERR_STATE &&
              res[5] == META_ERR_STATE && res[6] == META_ERR_INVALID && res[7] == META_OK && res[8] == META_OK);
        CHECK(meta_member(&m, 6)->replaced_by == 10 && meta_member(&m, 2)->replaced_by == 0);
        /* clear replaced_by on OUT member 6; test DESTROYED clearing replaced_by on fresh member 11 */
        meta_op_node_replace(&e, 6, 0);
        struct meta_member mb11;
        member(&mb11, 11);
        meta_op_member_add(&e, &mb11);
        meta_op_member_state(&e, 11, META_OUT, 1, META_DOWN_NONE, 0, 0);
        meta_op_node_replace(&e, 11, 10);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK &&
              res[3] == META_OK && meta_member(&m, 6)->replaced_by == 0 && meta_member(&m, 11)->replaced_by == 10);
        meta_copy(&prev, &m);
        meta_op_member_state(&e, 11, META_DESTROYED, 1, META_DOWN_NONE, 0, 0);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_member(&m, 11)->state == META_DESTROYED &&
              meta_member(&m, 11)->replaced_by == 0);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
    }

    /* 6. meta_check_step catches every new invariant violation directly */
    {
        struct meta_state bad;
        /* hold > META_HOLD_MAX_MS */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 2))->hold_until_ms = clk + META_HOLD_MAX_MS + 1;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        /* DESTROYED left */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 9))->state = META_UP;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        /* OUT -> UP without new incarnation */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 6))->state = META_UP;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        /* down_since set on UP */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 1))->down_since_ms = 1234;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        /* down_since in future of op clock */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 2))->down_since_ms = 9200;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        /* stripe pos goes whole without STRIPE_WHOLE */
        meta_copy(&bad, &m);
        ((struct meta_volume *)meta_volume(&bad, 0))->stripe_pos[4].state = META_STRIPE_WHOLE;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        /* stripe pos goes whole at wrong version */
        ((struct meta_volume *)meta_volume(&bad, 0))->stripe_whole_gen++;
        ((struct meta_volume *)meta_volume(&bad, 0))->stripe_whole_ver = 1;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        /* replaced_by invariants: self, unknown, or on DESTROYED */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 1))->replaced_by = 1;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        ((struct meta_member *)meta_member(&bad, 1))->replaced_by = 999;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        ((struct meta_member *)meta_member(&bad, 1))->replaced_by = 0;
        ((struct meta_member *)meta_member(&bad, 9))->replaced_by = 1;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
    }

    meta_entry_free(&e);
    meta_free(&m);
}

/* ================================================================ R10 metadata (snapshot version 9) */

static void test_r10_meta(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[16];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 12);

    /* Phase names */
    CHECK(!strcmp(meta_conv_phase_name(META_CONV_NONE), "none") &&
          !strcmp(meta_conv_phase_name(META_CONV_GROW), "grow") &&
          !strcmp(meta_conv_phase_name(META_CONV_RECORD), "record") &&
          !strcmp(meta_conv_phase_name(META_CONV_REENCODE), "reencode") &&
          !strcmp(meta_conv_phase_name(META_CONV_DROP), "drop") &&
          !strcmp(meta_conv_phase_name(META_CONV_SHRINK), "shrink") && !strcmp(meta_conv_phase_name(9), "?"));

    uint32_t log3[3] = {1, 2, 3}, g6[6] = {1, 2, 3, 4, 5, 6};
    vol_add(&e, META_VOL_REPLICATED, 0, "r0", 1, log3, 3);
    ec_add(&e, 1, "e1", 2, log3, 3, META_VOL_REPLICATED, 4, 2, g6, 6);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);

    /* 1. VOLUME_REDUNDANCY refusals: unknown vol, stale epoch, invalid targets, active move/learner/filling */
    struct meta_redundancy_target t_ec42 = {.redundancy = META_RED_EC, .ec_k = 4, .ec_m = 2, .replicas = 3};
    struct meta_redundancy_target t_ec43 = {.redundancy = META_RED_EC, .ec_k = 4, .ec_m = 3, .replicas = 0};
    struct meta_redundancy_target t_ec21 = {.redundancy = META_RED_EC, .ec_k = 2, .ec_m = 1, .replicas = 2};
    struct meta_redundancy_target t_rep3 = {.redundancy = META_RED_REPLICAS, .ec_k = 0, .ec_m = 0, .replicas = 3};
    struct meta_redundancy_target t_bad1 = {.redundancy = 9, .ec_k = 4, .ec_m = 2, .replicas = 3};
    struct meta_redundancy_target t_bad2 = {.redundancy = META_RED_EC, .ec_k = 0, .ec_m = 2, .replicas = 3};
    struct meta_redundancy_target t_bad3 = {.redundancy = META_RED_EC, .ec_k = 4, .ec_m = 5, .replicas = 6};
    struct meta_redundancy_target t_bad4 = {.redundancy = META_RED_EC, .ec_k = 4, .ec_m = 2, .replicas = 5};
    struct meta_redundancy_target t_bad5 = {.redundancy = META_RED_REPLICAS, .ec_k = 2, .ec_m = 0, .replicas = 3};
    struct meta_redundancy_target t_bad6 = {.redundancy = META_RED_REPLICAS, .ec_k = 0, .ec_m = 0, .replicas = 9};

    meta_op_volume_redundancy(&e, 99, &t_ec42, 1); /* unknown volume */
    meta_op_volume_redundancy(&e, 0, &t_ec42, 9);  /* stale epoch */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_NOTFOUND && res[1] == META_ERR_STALE);

    const struct meta_redundancy_target *bads[] = {&t_bad1, &t_bad2, &t_bad3, &t_bad4, &t_bad5, &t_bad6, &t_rep3};
    for (size_t i = 0; i < sizeof bads / sizeof bads[0]; i++) {
        meta_op_volume_redundancy(&e, 0, bads[i], 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    }
    meta_op_volume_redundancy(&e, 1, &t_ec42, 1); /* already ec=4+2 */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);

    /* Refused on shared mode */
    {
        struct meta_state sm;
        init_cluster(&sm, &e, META_MODE_SHARED, 4);
        vol_add(&e, META_VOL_SHARED, 0, "s0", 1, log3, 3);
        CHECK(apply1(&sm, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_volume_redundancy(&e, 0, &t_ec42, 1);
        CHECK(apply1(&sm, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
        meta_free(&sm);
    }

    /* Refused while a move (`replaces`), plain learner, or filling stripe pos runs */
    meta_op_replica_add(&e, 0, 1, 4, 8001, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_redundancy(&e, 0, &t_ec42, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_replica_remove(&e, 0, 1, 4);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);

    meta_op_replica_add(&e, 0, 1, 4, 8002, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_redundancy(&e, 0, &t_ec42, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_replica_remove(&e, 0, 1, 4);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);

    meta_op_stripe_set(&e, 1, 5, 7, META_STRIPE_FILLING, 6, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_redundancy(&e, 1, &t_ec43, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_stripe_whole(&e, 1, 5, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);

    /* 2. Full conversion replicas=3 -> ec=4+2 on volume 0, plus convskip & cancel refusals */
    meta_copy(&prev, &m);
    meta_op_volume_redundancy(&e, 0, &t_ec42, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    const struct meta_volume *v0 = meta_volume(&m, 0);
    CHECK(v0->conv.phase == META_CONV_GROW && v0->conv.target_redundancy == META_RED_EC && v0->conv.target_k == 4 &&
          v0->conv.target_m == 2 && v0->conv.target_replicas == 3 && v0->conv.new_stripe_version == 0);

    /* Second conversion and concurrent move refused while converting */
    meta_op_volume_redundancy(&e, 0, &t_ec43, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS);
    meta_op_replica_add(&e, 0, 1, 4, 8003, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);

    /* Stale from_phase and skipped phases refused (catches MUT(63) convskip) */
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_REENCODE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_NONE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);

    /* GROW -> RECORD -> REENCODE (sets redundancy=EC, min_isr=2 per E24, stripe[0..5], new_stripe_version=1) */
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    uint32_t dup6[6] = {1, 2, 3, 4, 5, 1};
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, dup6, 6);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, g6, 6);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    v0 = meta_volume(&m, 0);
    CHECK(v0->redundancy == META_RED_EC && v0->ec_k == 4 && v0->ec_m == 2 && v0->min_isr == 2 && v0->nstripe == 6 &&
          v0->stripe_version == 1 && v0->conv.phase == META_CONV_REENCODE && v0->conv.new_stripe_version == 1);

    /* REENCODE -> DROP -> SHRINK -> NONE */
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_cancel(&e, 0); /* refused in DROP */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_conv_step(&e, 0, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_cancel(&e, 0); /* refused in SHRINK */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    CHECK(meta_volume(&m, 0)->conv.phase == META_CONV_NONE);

    /* 3. Grow ec=4+2 -> ec=4+3 on volume 0 (log group 3 -> 4, min_isr 2 -> 3, nstripe 6 -> 7) */
    meta_op_volume_redundancy(&e, 0, &t_ec43, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_replica_add(&e, 0, 1, 4, 8010, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    uint32_t log4[4] = {1, 2, 3, 4};
    v0 = meta_volume(&m, 0);
    isr_chg(&e, v0, v0->epoch, v0->isr_version, log4, 4, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    uint32_t bad_prefix[7] = {9, 2, 3, 4, 5, 6, 7};
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, bad_prefix, 7);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    uint32_t g7[7] = {1, 2, 3, 4, 5, 6, 7};
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, g7, 7);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    v0 = meta_volume(&m, 0);
    CHECK(v0->ec_k == 4 && v0->ec_m == 3 && v0->min_isr == 3 && v0->nstripe == 7 && v0->stripe_version == 2 &&
          v0->conv.new_stripe_version == 2);
    /* Grow 4+2 -> 4+3 has no retired positions, so REENCODE -> DROP MUST still require a current-epoch live report
     * proving 0 live objects at sv < 2 (otherwise settled margin would jump from 2 to 3 while 4+2 objects exist;
     * catches MUT(66) reencskip). */
    {
        int yg_g = 0, sd_g = 0;
        meta_vol_margins(&m, v0, NULL, &yg_g, &sd_g);
        CHECK(sd_g == 2); /* capped at lower (4+2) margin until REENCODE finishes */
    }
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    struct meta_live_sv lv_g1[1] = {{1, 4}};
    meta_op_volume_live_objs(&e, 0, 1, 2, 0, lv_g1, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    struct meta_live_sv lv_g2[1] = {{2, 7}};
    meta_op_volume_live_objs(&e, 0, 1, 2, 0, lv_g2, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    meta_op_volume_conv_step(&e, 0, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->conv.phase == META_CONV_NONE);
    {
        int yg_g = 0, sd_g = 0;
        meta_vol_margins(&m, meta_volume(&m, 0), NULL, &yg_g, &sd_g);
        CHECK(sd_g == 3);
    }

    /* 4. Shrink ec=4+3 (7 pos, 4 log) -> ec=2+1 (3 pos, 2 log): STRIPE_SET live-object refusal (D28, MUT(62)) */
    meta_op_volume_redundancy(&e, 0, &t_ec21, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    v0 = meta_volume(&m, 0);
    CHECK(v0->ec_k == 2 && v0->ec_m == 1 && v0->min_isr == 1 && v0->nstripe == 7 && v0->stripe_version == 3 &&
          v0->conv.new_stripe_version == 3 && v0->stripe_pos[2].retired_ver == 0 &&
          v0->stripe_pos[3].retired_ver == 3 && v0->stripe_pos[6].retired_ver == 3);
    int yg = 0, sd = 0;
    meta_vol_margins(&m, v0, NULL, &yg, &sd);
    CHECK(yg == 3 && sd == 1);
    /* Log replica shrink before SHRINK is refused (§6.4 step 5) */
    isr_chg(&e, v0, v0->epoch, v0->isr_version, log3, 3, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_remove(&e, 0, 1, 4);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    /* STRIPE_SET drop refused in REENCODE; REENCODE -> DROP refused until 0 live objects at sv < 3 */
    meta_op_stripe_drop(&e, 0, 3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    /* VOLUME_LIVE_OBJS stale epoch/sv and invalid vector refused */
    struct meta_live_sv lv_old[2] = {{2, 5}, {3, 10}};
    struct meta_live_sv lv_bad[2] = {{3, 1}, {2, 1}};
    meta_op_volume_live_objs(&e, 0, 99, 3, 0, lv_old, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_volume_live_objs(&e, 0, 1, 2, 0, lv_old, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    meta_op_volume_live_objs(&e, 0, 1, 3, 0, lv_bad, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    /* Report 0 live objects at sv < 3 to leave REENCODE -> DROP */
    struct meta_live_sv lv_new[1] = {{3, 15}};
    meta_op_volume_live_objs(&e, 0, 1, 3, 0, lv_new, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    /* DROP -> SHRINK refused while retired positions remain */
    meta_op_volume_conv_step(&e, 0, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    /* Report live objects at sv=2 (count 5 > 0) and sv=3 (count 10): drop MUST be refused (catches MUT(62)) */
    meta_op_volume_live_objs(&e, 0, 1, 3, 0, lv_old, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_stripe_drop(&e, 0, 3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && meta_volume(&m, 0)->nstripe == 7);
    /* Overflow flag refuses drop even with empty vector */
    meta_op_volume_live_objs(&e, 0, 1, 3, 1, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_stripe_drop(&e, 0, 3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    /* Report 0 live objects at sv < 3 (only new objects at sv == 3): snapshot v9 round-trip + v8 refusal */
    meta_op_volume_live_objs(&e, 0, 1, 3, 0, lv_new, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    {
        uint8_t *img;
        size_t len;
        meta_snapshot_encode(&m, &img, &len);
        CHECK(meta_snapshot_version(img, len) == 10);
        struct meta_state back;
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&m));
        const struct meta_volume *bv = meta_volume(&back, 0);
        CHECK(bv->conv.phase == META_CONV_DROP && bv->conv.new_stripe_version == 3 &&
              bv->stripe_pos[3].retired_ver == 3 && bv->live_epoch == 1 && bv->live_sv_at == 3 && bv->nlive_sv == 1 &&
              bv->live_sv[0].stripe_version == 3 && bv->live_sv[0].count == 15);
        uint32_t eight = 8, crc;
        memcpy(img + 4, &eight, 4);
        crc = iso_crc32c(0, img, len - 4);
        memcpy(img + len - 4, &crc, 4);
        CHECK(meta_snapshot_version(img, len) == 8 && meta_snapshot_decode(&back, img, len) == META_ERR_FORMAT);
        meta_free(&back);
        free(img);
    }
    /* Promote volume 0 to epoch 2: previous epoch's live report is no longer under current epoch -> drop refused! */
    v0 = meta_volume(&m, 0);
    {
        uint32_t f3[3] = {1, 2, 3};
        promo(&e, v0, v0->epoch, 3, f3, 3, f3, 3, 10);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->epoch == 2);
    }
    meta_op_stripe_drop(&e, 0, 3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && meta_volume(&m, 0)->nstripe == 7);
    /* New leader under epoch 2 reports only sv=3 live -> pos=2 < active refused; pos=3 succeeds! */
    meta_op_volume_live_objs(&e, 0, 2, 3, 0, lv_new, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_stripe_drop(&e, 0, 2, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    meta_copy(&prev, &m);
    meta_op_stripe_drop(&e, 0, 3, 3);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    CHECK(meta_volume(&m, 0)->nstripe == 3 && meta_volume(&m, 0)->stripe_version == 4);
    /* DROP -> SHRINK -> remove excess replicas 4 and 2 -> SHRINK -> NONE */
    meta_op_volume_conv_step(&e, 0, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_replica_remove(&e, 0, 2, 4);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v0 = meta_volume(&m, 0);
    uint32_t log2[2] = {1, 3};
    isr_chg(&e, v0, v0->epoch, v0->isr_version, log2, 2, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_remove(&e, 0, 2, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    uint32_t lead3[1] = {3};
    v0 = meta_volume(&m, 0);
    isr_chg(&e, v0, v0->epoch, v0->isr_version, lead3, 1, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_remove(&e, 0, 2, 1); /* cannot shrink below target_replicas (2) */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_conv_step(&e, 0, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->conv.phase == META_CONV_NONE);

    /* 5. ec=2+1 -> replicas=3: GROW 2 -> 3, RECORD sets REPLICAS + min_isr, DROP empties nstripe to 0 */
    meta_op_volume_redundancy(&e, 0, &t_rep3, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_add(&e, 0, 2, 2, 8020, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v0 = meta_volume(&m, 0);
    isr_chg(&e, v0, v0->epoch, v0->isr_version, log3, 3, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_copy(&prev, &m);
    meta_op_volume_conv_step(&e, 0, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    v0 = meta_volume(&m, 0);
    CHECK(v0->redundancy == META_RED_REPLICAS && v0->ec_k == 0 && v0->ec_m == 0 && v0->min_isr == 1 &&
          v0->nstripe == 3 && v0->stripe_version == 5 && v0->stripe_pos[0].retired_ver == 5);
    struct meta_live_sv lv4[1] = {{4, 1}};
    meta_op_volume_live_objs(&e, 0, 2, 5, 0, lv4, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_live_objs(&e, 0, 2, 5, 0, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_live_objs(&e, 0, 2, 5, 0, lv4, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_stripe_drop(&e, 0, 0, 5);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_live_objs(&e, 0, 2, 5, 0, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_copy(&prev, &m);
    meta_op_stripe_drop(&e, 0, 0, 5);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    CHECK(meta_volume(&m, 0)->nstripe == 0 && meta_volume(&m, 0)->stripe_version == 6);
    meta_op_volume_conv_step(&e, 0, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 0, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 0)->conv.phase == META_CONV_NONE);

    /* 6. --cancel in GROW, RECORD, and REENCODE (on volume 1: ec=4+2 -> ec=4+3) */
    meta_op_volume_conv_cancel(&e, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_redundancy(&e, 1, &t_ec43, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_copy(&prev, &m);
    meta_op_volume_conv_cancel(&e, 1); /* cancel in GROW */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 1)->conv.phase == META_CONV_NONE &&
          meta_volume(&m, 1)->conv_cancel_gen == 1);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    meta_op_volume_redundancy(&e, 1, &t_ec43, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_replica_add(&e, 1, 1, 4, 8030, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    const struct meta_volume *v1 = meta_volume(&m, 1);
    isr_chg(&e, v1, v1->epoch, v1->isr_version, log4, 4, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_volume_conv_step(&e, 1, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    uint32_t g7b[7] = {1, 2, 3, 4, 5, 7, 8}; /* v1 had pos 5 moved to 7 earlier */
    meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, g7b, 7);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_volume(&m, 1)->ec_m == 3 && meta_volume(&m, 1)->min_isr == 3 && meta_volume(&m, 1)->nstripe == 7);
    meta_copy(&prev, &m);
    meta_op_volume_conv_cancel(&e, 1); /* cancel in REENCODE */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    v1 = meta_volume(&m, 1);
    CHECK(v1->conv.phase == META_CONV_NONE && v1->redundancy == META_RED_EC && v1->ec_k == 4 && v1->ec_m == 2 &&
          v1->min_isr == 2 && v1->nstripe == 7 && v1->stripe_pos[6].retired_ver == v1->stripe_version);
    uint64_t v1_ep = v1->epoch, v1_sv = v1->stripe_version;
    meta_op_stripe_drop(&e, 1, 6, v1_sv);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
    meta_op_volume_live_objs(&e, 1, v1_ep, v1_sv, 0, NULL, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_stripe_drop(&e, 1, 6, v1_sv);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&m, 1)->nstripe == 6);

    /* 7. Direct meta_check_step invariant tests */
    {
        struct meta_state bad;
        meta_op_volume_redundancy(&e, 1, &t_ec21, 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_copy(&bad, &m);
        ((struct meta_volume *)meta_volume(&bad, 1))->conv.phase = META_CONV_DROP;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        ((struct meta_volume *)meta_volume(&bad, 1))->conv.phase = META_CONV_NONE;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
        meta_copy(&bad, &m);
        struct meta_volume *bv = (struct meta_volume *)meta_volume(&bad, 1);
        bv->nstripe = 5;
        bv->stripe_version++;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);
    }

    /* 8. Regression: RESTORE and RECORD (replicas -> ec) in the same entry must pass meta_check_step */
    {
        vol_add(&e, META_VOL_REPLICATED, 2, "r2", 1, log3, 3);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_volume_redundancy(&e, 2, &t_ec42, 1);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_volume_conv_step(&e, 2, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_copy(&prev, &m);
        meta_op_restore(&e, m.restore_gen + 1);
        meta_op_volume_conv_step(&e, 2, META_CONV_RECORD, META_CONV_REENCODE, g6, 6);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
    }

    /* 9. orig_min_isr preservation:
     *    (a) replicas=3 with operator min_isr=2 -> ec 2+1, RECORD sets min_isr=1, cancel in REENCODE restores min_isr=2;
     *    (b) ec 4+2 (nrep=3, min_isr=3) -> replicas=3, RECORD sets min_isr=2, cancel in REENCODE restores min_isr=3;
     *    (c) replicas=3 (min_isr=2) -> replicas=4 preserves min_isr=2 at RECORD (never silently raised to 3). */
    {
        struct meta_state mr, pr;
        init_cluster(&mr, &e, META_MODE_REPLICATED, 7);
        uint32_t r3[3] = {1, 2, 3}, g6[6] = {1, 2, 3, 4, 5, 6}, g3[3] = {1, 2, 3};
        vol_add(&e, META_VOL_REPLICATED, 1, "r_min2", 2, r3, 3);
        ec_add(&e, 2, "ec_min3", 3, r3, 3, META_VOL_REPLICATED, 4, 2, g6, 6);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
        CHECK(meta_volume(&mr, 1)->min_isr == 2 && meta_volume(&mr, 2)->min_isr == 3);

        /* (a) replicas(3, min_isr=2) -> ec 2+1 (min_isr becomes 1 at RECORD) -> cancel in REENCODE restores min_isr=2 */
        struct meta_redundancy_target t21 = {.redundancy = META_RED_EC, .ec_k = 2, .ec_m = 1, .replicas = 2};
        meta_op_volume_redundancy(&e, 1, &t21, meta_volume(&mr, 1)->epoch);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 1)->conv.orig_min_isr == 2);
        meta_op_volume_conv_step(&e, 1, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_copy(&pr, &mr);
        meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, g3, 3);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 1)->min_isr == 1 && meta_check_step(&pr, &mr, why, sizeof why) == 0);
        meta_free(&pr);
        meta_copy(&pr, &mr);
        meta_op_volume_conv_cancel(&e, 1);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 1)->redundancy == META_RED_REPLICAS && meta_volume(&mr, 1)->min_isr == 2 &&
              meta_volume(&mr, 1)->mixed_redundancy == META_RED_EC && meta_volume(&mr, 1)->mixed_k == 2 &&
              meta_volume(&mr, 1)->mixed_m == 1);
        CHECK(meta_check_step(&pr, &mr, why, sizeof why) == 0);
        meta_free(&pr);
        /* While mixed_redundancy is active (leftover 2+1 EC objects), settled margin reflects 2+1 (not 0),
         * STRIPE_SET drop is refused, and a new conversion is refused until VOLUME_LIVE_OBJS proves 0 live. */
        {
            int yg = 0, sd = 0;
            meta_vol_margins(&mr, meta_volume(&mr, 1), NULL, &yg, &sd);
            CHECK(sd == 1);
            struct meta_redundancy_target tr4_try = {.redundancy = META_RED_REPLICAS, .replicas = 4};
            meta_op_volume_redundancy(&e, 1, &tr4_try, meta_volume(&mr, 1)->epoch);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
            meta_op_stripe_drop(&e, 1, 0, meta_volume(&mr, 1)->stripe_version);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
            meta_copy(&pr, &mr);
            meta_op_volume_live_objs(&e, 1, meta_volume(&mr, 1)->epoch, meta_volume(&mr, 1)->stripe_version, 0, NULL,
                                     0);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
            CHECK(meta_volume(&mr, 1)->mixed_redundancy == META_RED_REPLICAS);
            CHECK(meta_check_step(&pr, &mr, why, sizeof why) == 0);
            meta_free(&pr);
            meta_vol_margins(&mr, meta_volume(&mr, 1), NULL, &yg, &sd);
            CHECK(sd == 0);
            /* Regression: [STRIPE_SET drop -> nstripe 0, RESTORE] in one entry must pass meta_check_step */
            meta_copy(&pr, &mr);
            meta_op_stripe_drop(&e, 1, 0, meta_volume(&mr, 1)->stripe_version);
            meta_op_restore(&e, mr.restore_gen + 1);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
            CHECK(meta_volume(&mr, 1)->nstripe == 0 && meta_volume(&mr, 1)->gc_suspended == 0);
            CHECK(meta_check_step(&pr, &mr, why, sizeof why) == 0);
            meta_free(&pr);
        }

        /* (b) ec 4+2 (nrep=3, min_isr=3) -> replicas=3 (RECORD sets min_isr=2) -> cancel in REENCODE restores min_isr=3 */
        struct meta_redundancy_target tr3 = {.redundancy = META_RED_REPLICAS, .replicas = 3};
        meta_op_volume_redundancy(&e, 2, &tr3, meta_volume(&mr, 2)->epoch);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 2)->conv.orig_min_isr == 3);
        meta_op_volume_conv_step(&e, 2, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_copy(&pr, &mr);
        meta_op_volume_conv_step(&e, 2, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 2)->redundancy == META_RED_REPLICAS && meta_volume(&mr, 2)->min_isr == mr.set.min_isr);
        CHECK(meta_check_step(&pr, &mr, why, sizeof why) == 0);
        meta_free(&pr);

        /* 11. meta_op_volume_force must emit GC_SUSPEND while ec -> replicas REENCODE still has nstripe > 0 */
        {
            const struct meta_volume *v2 = meta_volume(&mr, 2);
            uint32_t i2[2] = {1, 3};
            isr_chg(&e, v2, 1, v2->epoch, i2, 2, 0);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
            meta_copy(&pr, &mr);
            meta_op_volume_force(&e, meta_volume(&mr, 2), 2);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
            CHECK(meta_volume(&mr, 2)->gc_suspended == 1 && meta_volume(&mr, 2)->nstripe == 6 &&
                  meta_volume(&mr, 2)->nisr == 3);
            CHECK(meta_check_step(&pr, &mr, why, sizeof why) == 0);
            meta_free(&pr);
        }

        meta_copy(&pr, &mr);
        meta_op_volume_conv_cancel(&e, 2);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 2)->redundancy == META_RED_EC && meta_volume(&mr, 2)->min_isr == 3);
        CHECK(meta_check_step(&pr, &mr, why, sizeof why) == 0);
        meta_free(&pr);

        /* (c) replicas=3 (min_isr=2) -> replicas=4 preserves min_isr=2 at RECORD (not raised to 3) */
        struct meta_redundancy_target tr4 = {.redundancy = META_RED_REPLICAS, .replicas = 4};
        meta_op_volume_redundancy(&e, 1, &tr4, meta_volume(&mr, 1)->epoch);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_replica_add(&e, 1, meta_volume(&mr, 1)->epoch, 4, 8010, 0);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        {
            const struct meta_volume *vr = meta_volume(&mr, 1);
            uint32_t i4[4] = {1, 2, 3, 4};
            isr_chg(&e, vr, 1, vr->epoch, i4, 4, 0);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        }
        meta_op_volume_conv_step(&e, 1, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_copy(&pr, &mr);
        meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
        CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mr, 1)->min_isr == 2 && meta_check_step(&pr, &mr, why, sizeof why) == 0);
        meta_free(&pr);

        /* 10. One-touch-per-volume rule prevents multi-op entries on the same volume:
         *     [VOLUME_LIVE_OBJS, STRIPE_SET drop] and [CONV_STEP(RECORD->REENCODE), CONV_CANCEL] */
        {
            const struct meta_volume *ve = meta_volume(&mr, 2);
            struct meta_live_sv lv = {.stripe_version = ve->stripe_version, .count = 0};
            meta_op_volume_live_objs(&e, 2, ve->epoch, ve->stripe_version, 0, &lv, 1);
            meta_op_stripe_drop(&e, 2, 5, ve->stripe_version);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID);
            CHECK(meta_volume(&mr, 2)->nstripe == 6);

            /* Second op on same volume in one entry (RECORD->REENCODE + CONV_CANCEL) is rejected */
            meta_op_volume_conv_cancel(&e, 1);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
            meta_op_volume_redundancy(&e, 1, &t21, meta_volume(&mr, 1)->epoch);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
            meta_op_volume_conv_step(&e, 1, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK);
            meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, g3, 3);
            meta_op_volume_conv_cancel(&e, 1);
            CHECK(apply1(&mr, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID);
            CHECK(meta_volume(&mr, 1)->conv.phase == META_CONV_REENCODE);
        }
        meta_free(&mr);
    }

    /* 12. Snapshot v9 decoder negative tests: every corrupt conv / live_sv field fails with META_ERR_FORMAT */
    {
        struct meta_state ms, back;
        init_cluster(&ms, &e, META_MODE_REPLICATED, 7);
        uint32_t r2[2] = {1, 2}, g6[6] = {1, 2, 3, 4, 5, 6};
        ec_add(&e, 1, "ec_snap", 2, r2, 2, META_VOL_REPLICATED, 4, 2, g6, 6);
        CHECK(apply1(&ms, &e, res, 8) == META_OK && res[0] == META_OK);
        /* bump stripe_version to 2 so we can record 2 sorted live_sv entries */
        meta_op_stripe_set(&e, 1, 5, 7, META_STRIPE_FILLING, 6, 1);
        CHECK(apply1(&ms, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_stripe_whole(&e, 1, 5, 2);
        CHECK(apply1(&ms, &e, res, 8) == META_OK && res[0] == META_OK);
        struct meta_live_sv lv2[2] = {{1, 5}, {2, 3}};
        meta_op_volume_live_objs(&e, 1, meta_volume(&ms, 1)->epoch, meta_volume(&ms, 1)->stripe_version, 0, lv2, 2);
        CHECK(apply1(&ms, &e, res, 8) == META_OK && res[0] == META_OK);

        /* Snapshot with conv.phase == NONE and 2 live_sv entries */
        uint8_t *img = NULL;
        size_t len = 0;
        CHECK(meta_snapshot_encode(&ms, &img, &len) == META_OK);
        /* Locate conv_off relative to trailer (44B) + conv/mixed/live fixed tail (75B) + 2*16B live_sv,
         * and assert surrounding known fields so any layout shift fails loudly here. */
        size_t conv_off = len - 44 - (75 + 2 * 16);
        uint64_t f_sv = 0, f_wgen = 0, f_wver = 0, f_gcgen = 0, f_lep = 0, f_lsv = 0;
        uint32_t f_nlv = 0;
        memcpy(&f_sv, img + conv_off - 33, 8);
        memcpy(&f_wgen, img + conv_off - 25, 8);
        memcpy(&f_wver, img + conv_off - 17, 8);
        memcpy(&f_gcgen, img + conv_off - 8, 8);
        memcpy(&f_lep, img + conv_off + 54, 8);
        memcpy(&f_lsv, img + conv_off + 62, 8);
        memcpy(&f_nlv, img + conv_off + 71, 4);
        CHECK(f_sv == 2 && f_wgen == 1 && f_wver == 2 && f_gcgen == 0 && img[conv_off] == META_CONV_NONE &&
              img[conv_off + 35] == META_RED_REPLICAS && f_lep == 1 && f_lsv == 2 && f_nlv == 2);

        /* helper: patch a copy, recompute CRC, assert META_ERR_FORMAT */
        uint8_t *bad = malloc(len);
        /* (a) invalid conv.phase > META_CONV_LAST */
        memcpy(bad, img, len);
        bad[conv_off + 0] = 9;
        uint32_t crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (b) conv.phase == NONE with nonzero target_k */
        memcpy(bad, img, len);
        bad[conv_off + 2] = 4;
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (c) nlive_sv > META_LIVE_SV_MAX */
        memcpy(bad, img, len);
        bad[conv_off + 71] = META_LIVE_SV_MAX + 1;
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (d) unsorted live_sv: {2, 5}, {1, 3} */
        memcpy(bad, img, len);
        uint64_t sv2 = 2, sv1 = 1;
        memcpy(bad + conv_off + 75, &sv2, 8);
        memcpy(bad + conv_off + 75 + 16, &sv1, 8);
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (e) live_epoch == 0 while nlive_sv == 2 */
        memcpy(bad, img, len);
        uint64_t z64 = 0;
        memcpy(bad + conv_off + 54, &z64, 8);
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (f) live_epoch > v->epoch */
        memcpy(bad, img, len);
        uint64_t hi_ep = meta_volume(&ms, 1)->epoch + 1;
        memcpy(bad + conv_off + 54, &hi_ep, 8);
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (f2) mixed_redundancy == 0 with nonzero mixed_k, and mixed_redundancy == EC with mixed_sv_hi >= sv */
        memcpy(bad, img, len);
        bad[conv_off + 36] = 2;
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        memcpy(bad, img, len);
        bad[conv_off + 35] = META_RED_EC;
        bad[conv_off + 36] = 2;
        bad[conv_off + 37] = 1;
        memcpy(bad + conv_off + 38, &sv1, 8);
        memcpy(bad + conv_off + 46, &sv2, 8); /* mixed_sv_hi == v->stripe_version (must be <) */
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);
        free(bad);
        free(img);

        /* Active conversion in GROW: test nonzero new_stripe_version and orig_min_isr == 0 */
        struct meta_redundancy_target tr3 = {.redundancy = META_RED_REPLICAS, .replicas = 3};
        meta_op_volume_redundancy(&e, 1, &tr3, meta_volume(&ms, 1)->epoch);
        CHECK(apply1(&ms, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_snapshot_encode(&ms, &img, &len) == META_OK);
        conv_off = len - 44 - (75 + 2 * 16);
        CHECK(img[conv_off] == META_CONV_GROW);
        bad = malloc(len);

        /* (g) conv.phase == GROW with nonzero new_stripe_version */
        memcpy(bad, img, len);
        uint64_t bad_nsv = 1;
        memcpy(bad + conv_off + 8, &bad_nsv, 8);
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        /* (h) active conv with orig_min_isr == 0 */
        memcpy(bad, img, len);
        uint32_t z32 = 0;
        memcpy(bad + conv_off + 23, &z32, 4);
        crc = iso_crc32c(0, bad, len - 4);
        memcpy(bad + len - 4, &crc, 4);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
        meta_free(&back);

        free(bad);
        free(img);
        meta_free(&ms);
    }

    /* 13. Cancel shrink 4+3 -> 2+1 in REENCODE preserves 2+1 margin until VOLUME_LIVE_OBJS proves 0 live objects;
     *     STRIPE_SET on a retired position preserves retired_ver (MUT(64) retireclear);
     *     VOLUME_LIVE_OBJS with stale epoch is refused (MUT(65) livestale);
     *     RECORD refuses FILLING positions, OUT reused members, and nisr < new_min_isr. */
    {
        struct meta_state mc, pr, back;
        init_cluster(&mc, &e, META_MODE_REPLICATED, 9);
        uint32_t r4[4] = {1, 2, 3, 4}, g7[7] = {1, 2, 3, 4, 5, 6, 7};
        ec_add(&e, 1, "ec43", 3, r4, 4, META_VOL_REPLICATED, 4, 3, g7, 7);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);

        /* Bump epoch to 2 so epoch 1 is stale (catches MUT(65) livestale in unit tests) */
        {
            const struct meta_volume *v = meta_volume(&mc, 1);
            uint32_t f4[4] = {1, 2, 3, 4};
            struct meta_grant g = promo_grant(v, 1, 4, f4, 4, f4, 4, 10, 0);
            meta_op_volume_leader(&e, &g);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK && meta_volume(&mc, 1)->epoch == 2);
            meta_op_volume_live_objs(&e, 1, 1, meta_volume(&mc, 1)->stripe_version, 0, NULL, 0);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
            CHECK(meta_volume(&mc, 1)->live_epoch == 0);
        }

        /* Start shrink 4+3 -> 2+1; test RECORD refusals first (FILLING pos, OUT member, nisr < new_min_isr) */
        struct meta_redundancy_target t21 = {.redundancy = META_RED_EC, .ec_k = 2, .ec_m = 1, .replicas = 2};
        meta_op_volume_redundancy(&e, 1, &t21, meta_volume(&mc, 1)->epoch);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_volume_conv_step(&e, 1, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);

        /* (a) FILLING stripe pos during RECORD -> META_ERR_STATE */
        meta_op_stripe_set(&e, 1, 6, 8, META_STRIPE_FILLING, 7, meta_volume(&mc, 1)->stripe_version);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
        meta_op_stripe_whole(&e, 1, 6, meta_volume(&mc, 1)->stripe_version);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);

        /* (b) OUT member in reused position 0..2 -> META_ERR_INVALID (on a copy so MUT(55) outfinal stays clean) */
        {
            struct meta_state mc_out;
            meta_copy(&mc_out, &mc);
            meta_op_member_state(&e, 3, META_OUT, 1, META_DOWN_NONE, 0, 0);
            CHECK(apply1(&mc_out, &e, res, 8) == META_OK && res[0] == META_OK);
            meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
            CHECK(apply1(&mc_out, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
            meta_free(&mc_out);
        }

        /* Now RECORD succeeds -> REENCODE */
        meta_copy(&pr, &mc);
        meta_op_volume_conv_step(&e, 1, META_CONV_RECORD, META_CONV_REENCODE, NULL, 0);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_check_step(&pr, &mc, why, sizeof why) == 0);
        meta_free(&pr);
        /* Verify STRIPE_SET replacement on retired position 6 preserves retired_ver (catches MUT(64) retireclear) */
        {
            const struct meta_volume *v = meta_volume(&mc, 1);
            uint64_t rver = v->stripe_pos[6].retired_ver;
            CHECK(rver == v->stripe_version);
            meta_copy(&pr, &mc);
            meta_op_stripe_set(&e, 1, 6, 9, META_STRIPE_FILLING, 8, v->stripe_version);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
            CHECK(meta_volume(&mc, 1)->stripe_pos[6].retired_ver == rver);
            CHECK(meta_check_step(&pr, &mc, why, sizeof why) == 0);
            meta_free(&pr);
            meta_op_stripe_whole(&e, 1, 6, meta_volume(&mc, 1)->stripe_version);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        }

        /* Cancel in REENCODE: geometry reverts to 4+3, but leftover 2+1 objects exist in [lo..hi] so settled
         * margin must stay capped at 2+1's margin (1, NOT 3!) until VOLUME_LIVE_OBJS proves 0 live in [lo..hi]. */
        meta_copy(&pr, &mc);
        meta_op_volume_conv_cancel(&e, 1);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_check_step(&pr, &mc, why, sizeof why) == 0);
        meta_free(&pr);
        const struct meta_volume *vc = meta_volume(&mc, 1);
        CHECK(vc->redundancy == META_RED_EC && vc->ec_k == 4 && vc->ec_m == 3 && vc->min_isr == 3 &&
              vc->mixed_redundancy == META_RED_EC && vc->mixed_k == 2 && vc->mixed_m == 1 &&
              vc->mixed_sv_lo >= 1 && vc->mixed_sv_lo <= vc->mixed_sv_hi && vc->mixed_sv_hi < vc->stripe_version);
        int yg = 0, sd = 0;
        meta_vol_margins(&mc, vc, NULL, &yg, &sd);
        CHECK(sd == 1); /* MUST NOT exceed 2+1 margin (1), even though active geometry is 4+3 (margin 3) */

        /* Snapshot round-trip preserves mixed_* */
        uint8_t *img = NULL;
        size_t len = 0;
        CHECK(meta_snapshot_encode(&mc, &img, &len) == META_OK);
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&mc));
        meta_vol_margins(&back, meta_volume(&back, 1), NULL, &yg, &sd);
        CHECK(sd == 1);
        meta_free(&back);
        free(img);

        /* Report live object at mixed_sv_lo -> mixed stays, sd remains 1 */
        struct meta_live_sv lv1 = {.stripe_version = vc->mixed_sv_lo, .count = 2};
        meta_copy(&pr, &mc);
        meta_op_volume_live_objs(&e, 1, vc->epoch, vc->stripe_version, 0, &lv1, 1);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mc, 1)->mixed_redundancy == META_RED_EC);
        CHECK(meta_check_step(&pr, &mc, why, sizeof why) == 0);
        meta_free(&pr);
        meta_vol_margins(&mc, meta_volume(&mc, 1), NULL, &yg, &sd);
        CHECK(sd == 1);

        /* Report 0 live objects in [mixed_sv_lo..mixed_sv_hi] -> mixed clears, sd returns to 3 */
        vc = meta_volume(&mc, 1);
        struct meta_live_sv lv_cur = {.stripe_version = vc->stripe_version, .count = 9};
        meta_copy(&pr, &mc);
        meta_op_volume_live_objs(&e, 1, vc->epoch, vc->stripe_version, 0, &lv_cur, 1);
        CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_volume(&mc, 1)->mixed_redundancy == META_RED_REPLICAS);
        CHECK(meta_check_step(&pr, &mc, why, sizeof why) == 0);
        meta_free(&pr);
        meta_vol_margins(&mc, meta_volume(&mc, 1), NULL, &yg, &sd);
        CHECK(sd == 3);

        /* Direct checker tests: mutated retired_ver, mutated live_epoch, and illegal mixed clear are rejected */
        {
            struct meta_state bent;
            meta_copy(&bent, &mc);
            ((struct meta_volume *)meta_volume(&bent, 1))->stripe_pos[0].retired_ver = 1;
            CHECK(meta_check_step(&mc, &bent, why, sizeof why) != 0);
            meta_free(&bent);

            meta_copy(&bent, &mc);
            ((struct meta_volume *)meta_volume(&bent, 1))->live_epoch = 1;
            CHECK(meta_check_step(&mc, &bent, why, sizeof why) != 0);
            meta_free(&bent);
        }

        /* Also test RECORD nisr < new_min_isr refusal on replicas -> ec 4+3 */
        {
            uint32_t r4b[4] = {1, 2, 3, 4};
            vol_add(&e, META_VOL_REPLICATED, 2, "r_isr", 1, r4b, 4);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
            struct meta_redundancy_target t43 = {.redundancy = META_RED_EC, .ec_k = 4, .ec_m = 3, .replicas = 4};
            meta_op_volume_redundancy(&e, 2, &t43, 1);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
            meta_op_volume_conv_step(&e, 2, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
            CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
            /* Shrink ISR to 2 (below new ec_m = 3) while in RECORD -> RECORD -> REENCODE must refuse */
            const struct meta_volume *v2 = meta_volume(&mc, 2);
            CHECK(v2 != NULL);
            if (v2) {
                uint32_t i2[2] = {1, 2};
                isr_chg(&e, v2, 1, v2->epoch, i2, 2, 0);
                CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_OK);
                meta_op_volume_conv_step(&e, 2, META_CONV_RECORD, META_CONV_REENCODE, g7, 7);
                CHECK(apply1(&mc, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);
            }
        }

        meta_free(&mc);
    }

    meta_entry_free(&e);
    meta_free(&m);
}

/* ================================================================ R13 metadata (snapshot version 10) */

static void make_store_id(uint8_t out[16], uint32_t tag)
{
    memset(out, 0, 16);
    out[0] = 0x53;
    out[1] = 0x54;
    out[12] = (uint8_t)(tag >> 24);
    out[13] = (uint8_t)(tag >> 16);
    out[14] = (uint8_t)(tag >> 8);
    out[15] = (uint8_t)tag;
}

static struct meta_disk make_disk(uint32_t tag, uint8_t cls, uint8_t uses)
{
    struct meta_disk d;
    memset(&d, 0, sizeof d);
    make_store_id(d.store_id, tag);
    d.disk_class = cls;
    d.uses = uses;
    d.state = META_STORE_UP;
    d.size_bytes = 1024ull * 1024ull * 1024ull * 1024ull; /* 1 TiB */
    d.sync_us = cls == META_DISK_NVME ? 120 : cls == META_DISK_SSD ? 450 : 6500;
    return d;
}

static void test_r13_meta(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    int res[16];
    char why[256];
    meta_entry_init(&e);
    init_cluster(&m, &e, META_MODE_REPLICATED, 8);

    /* 1. Name helpers */
    CHECK(!strcmp(meta_disk_class_name(META_DISK_NONE), "none") &&
          !strcmp(meta_disk_class_name(META_DISK_NVME), "nvme") &&
          !strcmp(meta_disk_class_name(META_DISK_SSD), "ssd") &&
          !strcmp(meta_disk_class_name(META_DISK_HDD), "hdd") && !strcmp(meta_disk_class_name(9), "?"));
    CHECK(!strcmp(meta_store_state_name(META_STORE_UP), "up") &&
          !strcmp(meta_store_state_name(META_STORE_DOWN), "down") &&
          !strcmp(meta_store_state_name(META_STORE_OUT), "out") &&
          !strcmp(meta_store_state_name(META_STORE_DESTROYED), "destroyed") && !strcmp(meta_store_state_name(9), "?"));
    CHECK(!strcmp(meta_role_name(META_ROLE_BOTH), "both") &&
          !strcmp(meta_role_name(META_ROLE_COMPUTE), "compute") &&
          !strcmp(meta_role_name(META_ROLE_STORAGE), "storage") && !strcmp(meta_role_name(9), "?"));
    CHECK(!strcmp(meta_cap_name(META_CAP_NONE), "none") && !strcmp(meta_cap_name(META_CAP_COMPUTE), "compute") &&
          !strcmp(meta_cap_name(META_CAP_STORAGE), "storage") && !strcmp(meta_cap_name(META_CAP_BOTH), "both") &&
          !strcmp(meta_cap_name(9), "?"));

    /* 2. R12/R13 settings defaults and bounds */
    {
        struct meta_settings st;
        meta_settings_default(&st);
        CHECK(st.scrub_days == 14 && st.scrub_mb_s == 50 && st.scrub_out_count == 8 && st.log_on_hdd == 0 &&
              st.young_warn_pct == 75 && st.young_move_pct == 80 && st.disk_stall_s == 10);
        struct { int f; uint32_t v; int ok; } cases[] = {
            {0, 0, 0},    {0, 1, 1},    {0, 365, 1},  {0, 366, 0},
            {1, 0, 0},    {1, 1, 1},    {1, 500, 1},
            {2, 0, 0},    {2, 1, 1},    {2, 100, 1},
            {3, 0, 1},    {3, 1, 1},    {3, 2, 0},
            {4, 0, 0},    {4, 1, 1},    {4, 80, 1},   {4, 81, 0},
            {5, 0, 0},    {5, 74, 0},   {5, 75, 1},   {5, 89, 1},   {5, 90, 0},
            {6, 0, 0},    {6, 1, 1},    {6, 3600, 1}, {6, 3601, 0},
        };
        for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
            meta_settings_default(&st);
            if (cases[i].f == 0) st.scrub_days = cases[i].v;
            else if (cases[i].f == 1) st.scrub_mb_s = cases[i].v;
            else if (cases[i].f == 2) st.scrub_out_count = cases[i].v;
            else if (cases[i].f == 3) st.log_on_hdd = (uint8_t)cases[i].v;
            else if (cases[i].f == 4) st.young_warn_pct = cases[i].v;
            else if (cases[i].f == 5) st.young_move_pct = cases[i].v;
            else st.disk_stall_s = cases[i].v;
            meta_op_settings(&e, &st);
            CHECK(apply1(&m, &e, res, 8) == META_OK && (res[0] == META_OK) == cases[i].ok);
        }
        /* Cross-field ordering: warn <= move < high (E53) */
        meta_settings_default(&st);
        st.young_warn_pct = 99;
        st.young_move_pct = 95;
        st.young_high_pct = 50;
        meta_op_settings(&e, &st);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
        meta_settings_default(&st);
        st.young_warn_pct = 80;
        st.young_move_pct = 80;
        st.young_high_pct = 81;
        meta_op_settings(&e, &st);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        meta_settings_default(&st);
        meta_op_settings(&e, &st);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    }

    /* 3. STORE_ADD: valid disks, capability computation, global uniqueness (catches MUT(67) storeaddtwice), refusals */
    struct meta_disk d_nvme1 = make_disk(101, META_DISK_NVME, META_DISK_USE_LOG);
    struct meta_disk d_hdd1 = make_disk(102, META_DISK_HDD, META_DISK_USE_CHUNK);
    struct meta_disk d_ssd2 = make_disk(201, META_DISK_SSD, META_DISK_USE_ALL);
    struct meta_disk d_hdd3 = make_disk(301, META_DISK_HDD, META_DISK_USE_CHUNK);
    struct meta_disk d_nvme4 = make_disk(401, META_DISK_NVME, META_DISK_USE_ALL);
    struct meta_disk d_nvme5 = make_disk(501, META_DISK_NVME, META_DISK_USE_LOG); /* compute-only (no chunk use) */

    /* Invalid STORE_ADD bodies and refusals */
    {
        struct meta_disk bad_zero = d_nvme1;
        memset(bad_zero.store_id, 0, 16);
        struct meta_disk bad_cls = make_disk(901, META_DISK_NONE, META_DISK_USE_LOG);
        struct meta_disk bad_uses = make_disk(902, META_DISK_NVME, 0);
        struct meta_disk bad_sz = make_disk(903, META_DISK_NVME, META_DISK_USE_LOG);
        bad_sz.size_bytes = 0;
        struct meta_disk bad_st = make_disk(904, META_DISK_NVME, META_DISK_USE_LOG);
        bad_st.state = META_STORE_DOWN;
        meta_op_store_add(&e, 1, &bad_zero);
        meta_op_store_add(&e, 1, &bad_cls);
        meta_op_store_add(&e, 1, &bad_uses);
        meta_op_store_add(&e, 1, &bad_sz);
        meta_op_store_add(&e, 1, &bad_st);
        meta_op_store_add(&e, 99, &d_nvme1); /* unknown member */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
              res[2] == META_ERR_INVALID && res[3] == META_ERR_INVALID && res[4] == META_ERR_INVALID &&
              res[5] == META_ERR_NOTFOUND);

        /* Format errors: unknown state / unknown uses bit */
        struct meta_disk fmt_st = make_disk(905, META_DISK_NVME, META_DISK_USE_LOG);
        fmt_st.state = 9;
        meta_op_store_add(&e, 1, &fmt_st);
        CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
        struct meta_disk fmt_u = make_disk(906, META_DISK_NVME, 0x80);
        meta_op_store_add(&e, 1, &fmt_u);
        CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
    }

    /* Register disks on members 1..5 */
    meta_copy(&prev, &m);
    meta_op_store_add(&e, 1, &d_nvme1);
    meta_op_store_add(&e, 1, &d_hdd1);
    meta_op_store_add(&e, 2, &d_ssd2);
    meta_op_store_add(&e, 3, &d_hdd3);
    meta_op_store_add(&e, 4, &d_nvme4);
    meta_op_store_add(&e, 5, &d_nvme5);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK &&
          res[3] == META_OK && res[4] == META_OK && res[5] == META_OK);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    CHECK(meta_member(&m, 1)->ndisks == 2 && meta_member(&m, 1)->capability == META_CAP_BOTH &&
          meta_member_has_fast_disk(meta_member(&m, 1)));
    CHECK(meta_member(&m, 2)->ndisks == 1 && meta_member(&m, 2)->capability == META_CAP_BOTH);
    CHECK(meta_member(&m, 3)->ndisks == 1 && meta_member(&m, 3)->capability == META_CAP_STORAGE &&
          !meta_member_has_fast_disk(meta_member(&m, 3)));
    CHECK(meta_member(&m, 4)->ndisks == 1 && meta_member(&m, 4)->capability == META_CAP_BOTH);
    CHECK(meta_member(&m, 5)->ndisks == 1 && meta_member(&m, 5)->capability == META_CAP_COMPUTE);
    CHECK(meta_member_disk(meta_member(&m, 1), d_hdd1.store_id) != NULL &&
          meta_member_disk(meta_member(&m, 1), d_hdd3.store_id) == NULL);

    /* Duplicate store_id refused on SAME member and on DIFFERENT member (catches MUT(67) storeaddtwice) */
    meta_op_store_add(&e, 1, &d_nvme1); /* same member */
    meta_op_store_add(&e, 2, &d_nvme1); /* moved disk on another member: not a new disk */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_EXISTS && res[1] == META_ERR_EXISTS);
    CHECK(meta_member(&m, 1)->ndisks == 2 && meta_member(&m, 2)->ndisks == 1);

    /* OUT member cannot register new disks */
    meta_op_member_state(&e, 8, META_OUT, 1, META_DOWN_NONE, 0, 0);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    struct meta_disk d_out8 = make_disk(801, META_DISK_NVME, META_DISK_USE_ALL);
    meta_op_store_add(&e, 8, &d_out8);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE);

    /* META_DISKS_MAX cap (64 disks per member -> 65th returns META_ERR_RANGE) */
    {
        struct meta_state ml;
        init_cluster(&ml, &e, META_MODE_REPLICATED, 2);
        for (uint32_t k = 0; k < META_DISKS_MAX; k++) {
            struct meta_disk dk = make_disk(5000 + k, META_DISK_HDD, META_DISK_USE_CHUNK);
            meta_op_store_add(&e, 1, &dk);
            CHECK(apply1(&ml, &e, res, 8) == META_OK && res[0] == META_OK);
        }
        struct meta_disk dk65 = make_disk(5000 + META_DISKS_MAX, META_DISK_HDD, META_DISK_USE_CHUNK);
        meta_op_store_add(&e, 1, &dk65);
        CHECK(apply1(&ml, &e, res, 8) == META_OK && res[0] == META_ERR_RANGE &&
              meta_member(&ml, 1)->ndisks == META_DISKS_MAX);
        meta_free(&ml);
    }

    /* 4. MEMBER_ROLE: valid narrowing, refusals wider than disks (catches MUT(69) rolewiden) */
    meta_copy(&prev, &m);
    meta_op_member_role(&e, 3, META_ROLE_COMPUTE); /* member 3 is HDD-only (log_on_hdd=0): refused */
    meta_op_member_role(&e, 5, META_ROLE_STORAGE); /* member 5 is log-only NVMe: refused */
    meta_op_member_role(&e, 8, META_ROLE_STORAGE); /* member 8 is OUT: refused */
    meta_op_member_role(&e, 99, META_ROLE_COMPUTE); /* unknown member */
    meta_op_member_role(&e, 3, META_ROLE_STORAGE); /* allowed */
    meta_op_member_role(&e, 5, META_ROLE_COMPUTE); /* allowed */
    meta_op_member_role(&e, 6, META_ROLE_STORAGE); /* member 6 has 0 disks: allowed */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_ERR_INVALID &&
          res[2] == META_ERR_STATE && res[3] == META_ERR_NOTFOUND && res[4] == META_OK && res[5] == META_OK &&
          res[6] == META_OK);
    CHECK(meta_member(&m, 3)->role == META_ROLE_STORAGE && meta_member(&m, 5)->role == META_ROLE_COMPUTE &&
          meta_member(&m, 6)->role == META_ROLE_STORAGE);
    CHECK(meta_member_effective_cap(meta_member(&m, 3)) == META_CAP_STORAGE &&
          meta_member_effective_cap(meta_member(&m, 5)) == META_CAP_COMPUTE);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);
    /* Reset member 3 and 6 back to BOTH ("whatever the disks allow") */
    meta_op_member_role(&e, 3, META_ROLE_BOTH);
    meta_op_member_role(&e, 6, META_ROLE_BOTH);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK &&
          meta_member(&m, 3)->role == META_ROLE_BOTH);
    meta_op_member_role(&e, 1, 9); /* invalid enum: FORMAT */
    CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
    /* MEMBER_ADD carrying disks + unsupported role is refused by insert_member */
    {
        struct meta_member bad_mb;
        member(&bad_mb, m.next_member);
        bad_mb.role = META_ROLE_COMPUTE;
        bad_mb.ndisks = 1;
        bad_mb.disks[0] = make_disk(9901, META_DISK_HDD, META_DISK_USE_CHUNK);
        meta_op_member_add(&e, &bad_mb);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    }

    /* 5. Log replica placement & log_on_hdd (catches MUT(68) logonhdd) */
    {
        uint32_t bad_hdd[3] = {1, 2, 3}; /* node 3 is HDD-only with log_on_hdd=0 -> refused */
        uint32_t good_fast[3] = {1, 2, 4}; /* all have UP fast disks */
        uint32_t compat_nodisk[3] = {1, 6, 7}; /* nodes 6, 7 have 0 disks -> backwards compatible */
        vol_add(&e, META_VOL_REPLICATED, 0, "v_hdd_no", 1, bad_hdd, 3);
        vol_add(&e, META_VOL_REPLICATED, 0, "v_fast", 1, good_fast, 3);
        vol_add(&e, META_VOL_REPLICATED, 1, "v_compat", 1, compat_nodisk, 3);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_OK &&
              res[2] == META_OK);
        /* REPLICA_ADD on HDD-only node 3 also refused when log_on_hdd=0 */
        meta_op_replica_add(&e, 0, 1, 3, 9101, 0);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
        /* EC stripe position on compute-only node 5 (no chunk use) refused */
        uint32_t bad_ec[3] = {1, 2, 5}, good_ec[3] = {1, 2, 3};
        ec_add(&e, 2, "ec_nochunk", 1, good_fast, 3, META_VOL_REPLICATED, 2, 1, bad_ec, 3);
        ec_add(&e, 2, "ec_ok", 1, good_fast, 3, META_VOL_REPLICATED, 2, 1, good_ec, 3);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID && res[1] == META_OK);

        /* Enable log_on_hdd=1 via SETTINGS: node 3 immediately gains META_CAP_COMPUTE, accepts MEMBER_ROLE compute
         * and log replicas! */
        struct meta_settings st = m.set;
        st.log_on_hdd = 1;
        meta_copy(&prev, &m);
        meta_op_settings(&e, &st);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
        CHECK(meta_member(&m, 3)->capability == META_CAP_BOTH);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);

        meta_copy(&prev, &m);
        meta_op_member_role(&e, 3, META_ROLE_COMPUTE);
        meta_op_replica_add(&e, 0, 1, 3, 9102, 0);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);

        /* Turn log_on_hdd back off: node 3 drops META_CAP_COMPUTE (existing replica stays, new ones refused) */
        st.log_on_hdd = 0;
        meta_copy(&prev, &m);
        meta_op_settings(&e, &st);
        meta_op_member_role(&e, 3, META_ROLE_BOTH);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);
        CHECK(meta_member(&m, 3)->capability == META_CAP_STORAGE);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
        meta_op_replica_add(&e, 1, 1, 3, 9103, 0);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    }

    /* 6. STORE_CLASS: class changes update capability; invalid class / unknown store / DESTROYED refused */
    {
        meta_copy(&prev, &m);
        meta_op_store_class(&e, 3, d_hdd3.store_id, META_DISK_SSD); /* HDD -> SSD: gains META_CAP_COMPUTE */
        meta_op_store_class(&e, 3, d_hdd3.store_id, META_DISK_NONE); /* invalid */
        meta_op_store_class(&e, 3, d_nvme1.store_id, META_DISK_SSD); /* not on member 3 */
        meta_op_store_class(&e, 99, d_hdd3.store_id, META_DISK_SSD); /* unknown member */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID &&
              res[2] == META_ERR_NOTFOUND && res[3] == META_ERR_NOTFOUND);
        CHECK(meta_member(&m, 3)->disks[0].disk_class == META_DISK_SSD &&
              meta_member(&m, 3)->capability == META_CAP_BOTH);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
        /* Change back to HDD: drops META_CAP_COMPUTE */
        meta_op_store_class(&e, 3, d_hdd3.store_id, META_DISK_HDD);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK &&
              meta_member(&m, 3)->capability == META_CAP_STORAGE);
    }

    /* 7. STORE_STATE: OUT only from DOWN, DESTROYED is final (catches MUT(70) storedestroyedback), cap updates */
    {
        /* UP -> OUT and UP -> DESTROYED refused */
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_OUT);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DESTROYED);
        meta_op_store_state(&e, 1, d_hdd3.store_id, META_STORE_DOWN); /* wrong node */
        meta_op_store_state(&e, 99, d_nvme1.store_id, META_STORE_DOWN); /* unknown node */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_ERR_STATE &&
              res[2] == META_ERR_NOTFOUND && res[3] == META_ERR_NOTFOUND);
        /* UP -> DOWN drops META_CAP_COMPUTE on member 1 (hdd1 still UP -> META_CAP_STORAGE); DOWN -> DESTROYED refused */
        meta_copy(&prev, &m);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DOWN);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DESTROYED);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STATE);
        CHECK(meta_member(&m, 1)->disks[0].state == META_STORE_DOWN &&
              meta_member(&m, 1)->capability == META_CAP_STORAGE);
        CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
        meta_free(&prev);
        /* DOWN -> OUT -> (OUT -> OUT and OUT -> DOWN refused) -> UP -> DOWN -> OUT -> DESTROYED */
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_OUT);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_OUT);  /* OUT -> OUT refused */
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DOWN); /* OUT -> DOWN refused */
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_UP);   /* OUT -> UP restores compute cap */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STATE &&
              res[2] == META_ERR_STATE && res[3] == META_OK);
        CHECK(meta_member(&m, 1)->capability == META_CAP_BOTH);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DOWN);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_OUT);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DESTROYED);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK);
        CHECK(meta_member(&m, 1)->disks[0].state == META_STORE_DESTROYED &&
              meta_member(&m, 1)->capability == META_CAP_STORAGE);
        /* DESTROYED is final: -> UP/DOWN/OUT/DESTROYED and STORE_CLASS and re-adding its store_id are all refused */
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_UP);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DOWN);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_OUT);
        meta_op_store_state(&e, 1, d_nvme1.store_id, META_STORE_DESTROYED);
        meta_op_store_class(&e, 1, d_nvme1.store_id, META_DISK_SSD);
        meta_op_store_add(&e, 4, &d_nvme1); /* destroyed store_id still known globally */
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && res[1] == META_ERR_STATE &&
              res[2] == META_ERR_STATE && res[3] == META_ERR_STATE && res[4] == META_ERR_STATE &&
              res[5] == META_ERR_EXISTS);
        CHECK(meta_member(&m, 1)->disks[0].state == META_STORE_DESTROYED);
        /* Unknown state enum -> FORMAT */
        meta_op_store_state(&e, 2, d_ssd2.store_id, 9);
        CHECK(apply1(&m, &e, res, 8) == META_ERR_FORMAT);
    }

    /* 8. Snapshot v10 round-trip, v9 refusal by name, and negative decode tests */
    {
        struct meta_settings st = m.set;
        st.scrub_days = 7;
        st.scrub_mb_s = 75;
        st.scrub_out_count = 12;
        st.log_on_hdd = 0;
        st.young_warn_pct = 70;
        st.young_move_pct = 78;
        st.disk_stall_s = 15;
        meta_op_settings(&e, &st);
        CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);

        uint8_t *img = NULL;
        size_t len = 0;
        CHECK(meta_snapshot_encode(&m, &img, &len) == META_OK);
        CHECK(meta_snapshot_version(img, len) == 10);
        struct meta_state back;
        meta_init(&back);
        CHECK(meta_snapshot_decode(&back, img, len) == META_OK && meta_hash(&back) == meta_hash(&m));
        CHECK(back.set.scrub_days == 7 && back.set.scrub_mb_s == 75 && back.set.scrub_out_count == 12 &&
              back.set.log_on_hdd == 0 && back.set.young_warn_pct == 70 && back.set.young_move_pct == 78 &&
              back.set.disk_stall_s == 15);
        const struct meta_member *bm1 = meta_member(&back, 1), *bm5 = meta_member(&back, 5);
        CHECK(bm1 && bm5 && bm1->ndisks == 2 && bm1->disks[0].state == META_STORE_DESTROYED &&
              bm1->disks[1].disk_class == META_DISK_HDD && bm1->capability == META_CAP_STORAGE &&
              bm5->role == META_ROLE_COMPUTE);
        /* Refuse snapshot version 9 by name */
        uint8_t *v9 = malloc(len);
        memcpy(v9, img, len);
        uint32_t nine = 9;
        memcpy(v9 + 4, &nine, 4);
        uint32_t crc = iso_crc32c(0, v9, len - 4);
        memcpy(v9 + len - 4, &crc, 4);
        CHECK(meta_snapshot_version(v9, len) == 9 && meta_snapshot_decode(&back, v9, len) == META_ERR_FORMAT);
        free(v9);

        /* Negative decode tests on member disk/role/capability fields */
        size_t sid_off = 0;
        for (size_t k = 0; k + 16 <= len; k++)
            if (memcmp(img + k, d_nvme1.store_id, 16) == 0) {
                sid_off = k;
                break;
            }
        CHECK(sid_off >= 6);
        if (sid_off >= 6) {
            /* Layout before disk[0].store_id: role(1), capability(1), ndisks(4) -> sid_off - 6 */
            uint8_t *bad = malloc(len);
            /* (a) invalid role */
            memcpy(bad, img, len);
            bad[sid_off - 6] = 9;
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            /* (b) mismatched capability */
            memcpy(bad, img, len);
            bad[sid_off - 5] = META_CAP_BOTH;
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            /* (c) ndisks > META_DISKS_MAX */
            memcpy(bad, img, len);
            uint32_t over = META_DISKS_MAX + 1;
            memcpy(bad + sid_off - 4, &over, 4);
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            /* (d) zero store_id */
            memcpy(bad, img, len);
            memset(bad + sid_off, 0, 16);
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            /* (e) duplicate store_id (overwrite disk[1].store_id with disk[0].store_id; disk record is 35 bytes) */
            memcpy(bad, img, len);
            memcpy(bad + sid_off + 35, d_nvme1.store_id, 16);
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            /* (f) invalid disk_class */
            memcpy(bad, img, len);
            bad[sid_off + 16] = 0;
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            /* (g) zero size_bytes */
            memcpy(bad, img, len);
            memset(bad + sid_off + 19, 0, 8);
            crc = iso_crc32c(0, bad, len - 4);
            memcpy(bad + len - 4, &crc, 4);
            CHECK(meta_snapshot_decode(&back, bad, len) == META_ERR_FORMAT);
            free(bad);
        }
        meta_free(&back);
        free(img);
    }

    /* 9. Direct meta_check_step negative tests */
    {
        struct meta_state bad;
        /* (a) DESTROYED store returning to UP */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 1))->disks[0].state = META_STORE_UP;
        ((struct meta_member *)meta_member(&bad, 1))->capability = META_CAP_BOTH;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);

        /* (b) Duplicate store_id across members */
        meta_copy(&bad, &m);
        struct meta_member *m4 = (struct meta_member *)meta_member(&bad, 4);
        if (m4 && m4->ndisks < META_DISKS_MAX) {
            m4->disks[m4->ndisks++] = d_hdd3;
            CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        }
        meta_free(&bad);

        /* (c) Log replica placed on HDD-only member 3 with log_on_hdd=0 */
        meta_copy(&bad, &m);
        struct meta_volume *v1 = (struct meta_volume *)meta_volume(&bad, 1);
        if (v1 && v1->nrep < META_REPLICAS_MAX) {
            v1->rep[v1->nrep++] = (struct meta_replica){.node = 3, .learner = 1, .rid = 99999};
            v1->set_version++;
            CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
            /* Allowed when log_on_hdd=1 and capability reflects it */
            bad.set.log_on_hdd = 1;
            ((struct meta_member *)meta_member(&bad, 3))->capability = META_CAP_BOTH;
            ((struct meta_member *)meta_member(&bad, 1))->capability = META_CAP_BOTH;
            CHECK(meta_check_step(&m, &bad, why, sizeof why) == 0);
        }
        meta_free(&bad);

        /* (d) Role widened to COMPUTE on HDD-only member 3 */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 3))->role = META_ROLE_COMPUTE;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);

        /* (e) Stale capability mismatch */
        meta_copy(&bad, &m);
        ((struct meta_member *)meta_member(&bad, 3))->capability = META_CAP_BOTH;
        CHECK(meta_check_step(&m, &bad, why, sizeof why) != 0);
        meta_free(&bad);

        /* (f) Log replica placed on a --role storage member (even with NVMe disks or 0 disks; R13 r13-place) */
        {
            struct meta_state mrole;
            meta_copy(&mrole, &m);
            meta_op_member_role(&e, 4, META_ROLE_STORAGE); /* node 4 has NVMe(LOG|CHUNK), narrowed to storage */
            meta_op_member_role(&e, 6, META_ROLE_STORAGE); /* node 6 has 0 disks, narrowed to storage */
            CHECK(apply1(&mrole, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK);

            /* Planted REPLICA_ADD on storage-role node 4 (has NVMe, but role forbids compute) */
            meta_copy(&bad, &mrole);
            struct meta_volume *vb = (struct meta_volume *)meta_volume(&bad, 1);
            CHECK(vb != NULL && vb->nrep < META_REPLICAS_MAX);
            if (vb && vb->nrep < META_REPLICAS_MAX) {
                vb->rep[vb->nrep++] = (struct meta_replica){.node = 4, .learner = 1, .rid = 99991};
                vb->set_version++;
                CHECK(meta_check_step(&mrole, &bad, why, sizeof why) != 0 && strstr(why, "without compute capability"));
            }
            meta_free(&bad);

            /* Planted REPLICA_ADD on storage-role node 6 (ndisks == 0, but role forbids compute) */
            meta_copy(&bad, &mrole);
            vb = (struct meta_volume *)meta_volume(&bad, 0);
            CHECK(vb != NULL && vb->nrep < META_REPLICAS_MAX);
            if (vb && vb->nrep < META_REPLICAS_MAX) {
                vb->rep[vb->nrep++] = (struct meta_replica){.node = 6, .learner = 1, .rid = 99992};
                vb->set_version++;
                CHECK(meta_check_step(&mrole, &bad, why, sizeof why) != 0 && strstr(why, "without compute capability"));
            }
            meta_free(&bad);

            /* Planted duplicate log replica on the same node (distinct rids -> violates one replica per node) */
            meta_copy(&bad, &mrole);
            vb = (struct meta_volume *)meta_volume(&bad, 1);
            CHECK(vb != NULL && vb->nrep < META_REPLICAS_MAX);
            if (vb && vb->nrep < META_REPLICAS_MAX) {
                uint32_t dup_node = vb->rep[0].node;
                vb->rep[vb->nrep++] = (struct meta_replica){.node = dup_node, .learner = 0, .rid = 99993};
                vb->set_version++;
                CHECK(meta_check_step(&mrole, &bad, why, sizeof why) != 0);
                /* Also bypass one_set_op (e.g. multi-op entry raising set_version by 2): caught by duplicate-node check */
                vb->set_version++;
                CHECK(meta_check_step(&mrole, &bad, why, sizeof why) != 0 && strstr(why, "holds two replicas"));
            }
            meta_free(&bad);

            /* Planted EC stripe position on compute-only member 5 (log-only NVMe) */
            meta_copy(&bad, &mrole);
            struct meta_volume *vec = (struct meta_volume *)meta_volume(&bad, 2);
            CHECK(vec != NULL && vec->redundancy == META_RED_EC && vec->nstripe >= 3);
            if (vec && vec->redundancy == META_RED_EC && vec->nstripe >= 3) {
                vec->stripe[2] = 5;
                vec->stripe_version++;
                vec->stripe_pos[2].set_ver = vec->stripe_version;
                CHECK(meta_check_step(&mrole, &bad, why, sizeof why) != 0 && strstr(why, "without storage capability"));
            }
            meta_free(&bad);

            meta_free(&mrole);
        }
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
    meta_op_member_state(&e, 1, META_UP, 5, META_DOWN_NONE, 0, 0);
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
    meta_op_member_state(&e, gone, META_OUT, 9, META_DOWN_NONE, 0, 0);
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
    uint64_t disk_seq; /* STORE_ADD store_id sequence */
    uint64_t outs;    /* committed transitions into META_OUT */
    uint64_t out_back_tries; /* valid OUT -> UP (new inc) proposals sent */
    uint64_t out_backs; /* committed transitions META_OUT -> META_UP (catches outfinal) */
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
    if (kind == META_OP_VOLUME_ADD) { /* an EC volume's stripe group is the body's, never apply's own pick */
        const struct meta_volume *v = meta_volume(at, number);
        if (v && v->redundancy == META_RED_EC) {
            size_t ns = v->nstripe, st = n - 4 * ns;
            int same = n >= 4 * ns + 7 && rd32(b + st - 4) == ns && b[st - 7] == META_RED_EC;
            for (size_t i = 0; same && i < ns; i++)
                same = rd32(b + st + 4 * i) == v->stripe[i];
            if (!same)
                sim_fail(s, "node %u, index %" PRIu64 ": EC volume %u's stripe group is not the one VOLUME_ADD named",
                         nd->id, index, number);
        }
    }
    if (kind == META_OP_STRIPE_SET && n >= 25) {
        const struct meta_volume *a = meta_volume(pre, number), *v = meta_volume(at, number);
        uint32_t pos = rd32(b + 4), node = rd32(b + 8), prev = rd32(b + 13);
        uint8_t st = b[12];
        uint64_t sv = rd64(b + 17);
        if (node == 0) {
            if (!a || !v || a->stripe_version != sv || v->stripe_version != a->stripe_version + 1 ||
                pos >= a->nstripe || v->nstripe != pos || a->live_epoch != a->epoch || a->live_overflow)
                sim_fail(s, "node %u, index %" PRIu64 ": STRIPE_SET drop on volume %u applied wrongly", nd->id, index,
                         number);
            for (uint32_t p = pos; a && p < a->nstripe; p++) {
                uint64_t rv = a->stripe_pos[p].retired_ver;
                if (!rv || a->live_sv_at < rv)
                    sim_fail(s, "node %u, index %" PRIu64 ": STRIPE_SET dropped pos %u without retired_ver/live_sv_at",
                             nd->id, index, p);
                for (uint32_t i = 0; i < a->nlive_sv; i++)
                    if (a->live_sv[i].count > 0 && a->live_sv[i].stripe_version < rv)
                        sim_fail(s, "node %u, index %" PRIu64 ": STRIPE_SET dropped pos %u with live objects", nd->id,
                                 index, p);
            }
        } else if (!a || !v || a->stripe_version != sv || v->stripe_version != a->stripe_version + 1 ||
                   pos >= v->nstripe || v->stripe[pos] != node || v->stripe_pos[pos].state != st ||
                   v->stripe_pos[pos].set_ver != v->stripe_version ||
                   (st == META_STRIPE_FILLING && v->stripe_pos[pos].prev != prev)) {
            sim_fail(s, "node %u, index %" PRIu64 ": STRIPE_SET of volume %u named stripe version %" PRIu64
                     " (the volume's: %" PRIu64 ") and was applied wrongly", nd->id, index, number, sv,
                     a ? a->stripe_version : 0);
        }
    }
    if (kind == META_OP_STRIPE_WHOLE && n >= 16) {
        const struct meta_volume *a = meta_volume(pre, number), *v = meta_volume(at, number);
        uint32_t pos = rd32(b + 4);
        uint64_t ver = rd64(b + 8);
        if (!a || !v || a->stripe_version != ver || v->stripe_version != a->stripe_version || pos >= v->nstripe ||
            a->stripe_pos[pos].state != META_STRIPE_FILLING || v->stripe_pos[pos].state != META_STRIPE_WHOLE ||
            v->stripe_whole_gen != a->stripe_whole_gen + 1)
            sim_fail(s, "node %u, index %" PRIu64 ": STRIPE_WHOLE of volume %u pos %u named version %" PRIu64
                     " (the volume's: %" PRIu64 ") and was applied wrongly", nd->id, index, number, pos, ver,
                     a ? a->stripe_version : 0);
    }
    if (kind == META_OP_GC_SUSPEND && !(meta_volume(at, number) && meta_volume(at, number)->gc_suspended))
        sim_fail(s, "node %u, index %" PRIu64 ": GC_SUSPEND of volume %u left orphan collection on", nd->id, index,
                 number);
    char why[256];
    if ((setop || kind == META_OP_VOLUME_LEADER || kind == META_OP_ISR_CHANGE || kind == META_OP_VOLUME_ADD ||
         kind == META_OP_STRIPE_SET || kind == META_OP_STRIPE_WHOLE || kind == META_OP_GC_SUSPEND ||
         kind == META_OP_GC_RESUME || kind == META_OP_RESTORE || kind == META_OP_MEMBER_STATE ||
         kind == META_OP_NODE_HOLD || kind == META_OP_NODE_REPLACE || kind == META_OP_VOLUME_REDUNDANCY ||
         kind == META_OP_VOLUME_CONV_STEP || kind == META_OP_VOLUME_CONV_CANCEL || kind == META_OP_VOLUME_LIVE_OBJS ||
         kind == META_OP_STORE_ADD || kind == META_OP_STORE_CLASS || kind == META_OP_STORE_STATE ||
         kind == META_OP_MEMBER_ROLE || kind == META_OP_SETTINGS) &&
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
        if (kind == META_OP_MEMBER_STATE && n >= 30 && ok) {
            const struct meta_member *mb = meta_member(&at, rd32(b));
            uint8_t st = b[4];
            uint64_t inc = rd64(b + 5);
            int bad = !mb || mb->state == META_DESTROYED || inc < mb->incarnation ||
                      (st == META_DESTROYED && mb->state != META_OUT) ||
                      (mb->state == META_OUT && st != META_UP && st != META_DESTROYED) ||
                      (mb->state == META_OUT && st == META_UP && inc <= mb->incarnation);
            if (bad)
                sim_fail(s, "node %u, index %" PRIu64 ": MEMBER_STATE of member %u (%d -> %u, inc %" PRIu64
                         " vs %" PRIu64 ") was applied", nd->id, e->index, rd32(b), mb ? mb->state : -1, st, inc,
                         mb ? mb->incarnation : 0);
            if (mb && mb->state != META_OUT && st == META_OUT)
                s->outs++;
            if (mb && mb->state == META_OUT && st == META_UP)
                s->out_backs++;
        }
        if (kind == META_OP_NODE_HOLD && n >= 20 && ok) {
            const struct meta_member *mb = meta_member(&at, rd32(b));
            uint64_t until_ms = rd64(b + 4), clk = rd64(b + 12);
            if (!mb || mb->state >= META_OUT ||
                (until_ms && (!clk || until_ms < clk || until_ms - clk > META_HOLD_MAX_MS)))
                sim_fail(s, "node %u, index %" PRIu64 ": NODE_HOLD of member %u (until %" PRIu64 ", clock %" PRIu64
                         ", state %d) was applied", nd->id, e->index, rd32(b), until_ms, clk, mb ? mb->state : -1);
        }
        if (kind == META_OP_NODE_REPLACE && n >= 8 && ok) {
            uint32_t old_id = rd32(b), new_id = rd32(b + 4);
            const struct meta_member *mb = meta_member(&at, old_id);
            const struct meta_member *nt = new_id ? meta_member(&at, new_id) : NULL;
            int overlap = 0;
            for (uint32_t vi = 0; new_id && vi < at.nvolumes && !overlap; vi++) {
                const struct meta_volume *vv = &at.volumes[vi];
                int oh = meta_replica(vv, old_id) != NULL, nh = meta_replica(vv, new_id) != NULL;
                if (vv->redundancy == META_RED_EC)
                    for (uint32_t p = 0; p < vv->nstripe && p < META_STRIPE_MAX; p++) {
                        oh |= vv->stripe[p] == old_id;
                        nh |= vv->stripe[p] == new_id;
                    }
                overlap = oh && nh;
            }
            if (!mb || mb->state == META_DESTROYED ||
                (new_id && (new_id == old_id || !nt || nt->state >= META_OUT || overlap)))
                sim_fail(s, "node %u, index %" PRIu64 ": NODE_REPLACE of %u -> %u (state %d, overlap %d) was applied",
                         nd->id, e->index, old_id, new_id, mb ? mb->state : -1, overlap);
        }
        if (kind == META_OP_STORE_ADD && n >= 39 && ok) {
            uint32_t nid = rd32(b);
            const struct meta_member *mb = meta_member(&at, nid);
            int dup = 0;
            for (uint32_t mi = 0; mi < at.nmembers && !dup; mi++)
                if (meta_member_disk(&at.members[mi], b + 4) != NULL)
                    dup = 1;
            if (!mb || mb->state >= META_OUT || mb->ndisks >= META_DISKS_MAX || dup)
                sim_fail(s, "node %u, index %" PRIu64 ": STORE_ADD on member %u (dup %d) was applied illegally", nd->id,
                         e->index, nid, dup);
        }
        if (kind == META_OP_STORE_STATE && n >= 21 && ok) {
            uint32_t nid = rd32(b);
            uint8_t nst = b[20];
            const struct meta_member *mb = meta_member(&at, nid);
            const struct meta_disk *d = mb ? meta_member_disk(mb, b + 4) : NULL;
            if (!mb || !d || mb->state == META_DESTROYED || d->state == META_STORE_DESTROYED ||
                (nst == META_STORE_OUT && d->state != META_STORE_DOWN) ||
                (nst == META_STORE_DESTROYED && d->state != META_STORE_OUT) ||
                (d->state == META_STORE_OUT && nst != META_STORE_UP && nst != META_STORE_DESTROYED))
                sim_fail(s, "node %u, index %" PRIu64 ": STORE_STATE on member %u (%d -> %u) was applied illegally",
                         nd->id, e->index, nid, d ? d->state : -1, nst);
        }
        if (kind == META_OP_MEMBER_ROLE && n >= 5 && ok) {
            uint32_t nid = rd32(b);
            uint8_t nrole = b[4];
            const struct meta_member *mb = meta_member(&at, nid);
            if (!mb || mb->state >= META_OUT ||
                (mb->ndisks > 0 && ((nrole == META_ROLE_COMPUTE && !(mb->capability & META_CAP_COMPUTE)) ||
                                    (nrole == META_ROLE_STORAGE && !(mb->capability & META_CAP_STORAGE)))))
                sim_fail(s, "node %u, index %" PRIu64 ": MEMBER_ROLE on member %u to %u (cap %u) was applied illegally",
                         nd->id, e->index, nid, nrole, mb ? mb->capability : 0);
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
            /* Prefer compute-capable live members so volumes form readily (still pick wild 1/8 of the time) */
            uint32_t comp[16], ncomp = 0;
            for (uint32_t j = 0; j < m->nmembers && ncomp < 16; j++) {
                const struct meta_member *mj = &m->members[j];
                if (mj->state < META_OUT && (mj->flags & META_MF_VOLUMES_OK) &&
                    (mj->ndisks == 0 || (meta_member_effective_cap(mj) & META_CAP_COMPUTE)))
                    comp[ncomp++] = mj->id;
            }
            int use_comp = ncomp >= 3 && rnd(s, 0, 7) != 0;
            while (have < 3) {
                uint32_t id = use_comp ? comp[rnd(s, 0, ncomp - 1)] : m->members[rnd(s, 0, m->nmembers - 1)].id;
                if (!has_id_local(nodes, have, id))
                    nodes[have++] = id;
            }
            char name[16];
            snprintf(name, sizeof name, "v%u", m->next_volume);
            if (rnd(s, 0, 2) == 0 && m->nmembers >= 3) {
                /* erasure coded (R8): the proposer's pick of k + m distinct members, sometimes a bad one (a node
                 * twice, an unknown node, too few positions), min_isr as cluster.c gives it, sometimes one below m */
                uint32_t stor[16], nstor = 0;
                for (uint32_t j = 0; j < m->nmembers && nstor < 16; j++) {
                    const struct meta_member *mj = &m->members[j];
                    if (mj->state < META_OUT && (mj->ndisks == 0 || (meta_member_effective_cap(mj) & META_CAP_STORAGE)))
                        stor[nstor++] = mj->id;
                }
                int use_stor = nstor >= 3 && rnd(s, 0, 7) != 0;
                uint32_t pool_n = use_stor ? nstor : m->nmembers;
                uint8_t mm = (uint8_t)rnd(s, 1, 2), k = (uint8_t)rnd(s, 1, 4);
                if (k + mm > pool_n)
                    k = (uint8_t)(pool_n - mm);
                uint32_t stripe[META_STRIPE_MAX], ns = 0;
                while (ns < (uint32_t)k + mm) {
                    uint32_t id = use_stor ? stor[rnd(s, 0, nstor - 1)] : m->members[rnd(s, 0, m->nmembers - 1)].id;
                    if (!has_id_local(stripe, ns, id))
                        stripe[ns++] = id;
                }
                unsigned bad = (unsigned)rnd(s, 0, 23);
                if (bad == 0 && ns > 1)
                    stripe[ns - 1] = stripe[0];
                else if (bad == 1)
                    stripe[0] = m->next_member + 5;
                else if (bad == 2)
                    ns--;
                uint32_t min_isr = meta_volume_min_isr(META_RED_EC, mm, 3, 1) - (bad == 3 && mm > 1);
                ec_add(&e, m->next_volume, name, min_isr, nodes, 3, rnd(s, 0, 15) == 0 ? 3 - m->mode : m->mode, k, mm,
                       stripe, ns);
            } else {
                vol_add(&e, rnd(s, 0, 15) == 0 ? 3 - m->mode : m->mode, m->next_volume, name, 1, nodes, 3);
            }
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
            if (rnd(s, 0, 7)) { /* usually pick a non-OUT member so replica moves exercise freely */
                uint32_t live[16], nl = 0;
                for (uint32_t j = 0; j < m->nmembers && nl < 16; j++)
                    if (m->members[j].state < META_OUT)
                        live[nl++] = m->members[j].id;
                if (nl)
                    node = live[rnd(s, 0, nl - 1)];
            }
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
            const struct meta_member *mb = &m->members[rnd(s, 0, m->nmembers - 1)];
            uint32_t id = mb->id;
            uint64_t inc = mb->incarnation + rnd(s, 0, 1);
            uint64_t clk = 1000000ull + s->now / 1000000ull;
            if (rnd(s, 0, 4) == 0) { /* moved: to a fresh address, or onto another member's (refused) */
                char peer[32];
                snprintf(peer, sizeof peer, "10.0.%u.%u:5556", (unsigned)rnd(s, 0, 1), (unsigned)rnd(s, 1, 9));
                meta_op_member_addr(&e, id, inc, "10.9.9.9:5555", peer);
            }
            unsigned act = (unsigned)rnd(s, 0, 7);
            if (mb->state == META_OUT) {
                /* OUT: mostly return to UP with new inc (exercises OUT -> UP back path), sometimes DESTROYED */
                uint32_t ndest = 0;
                for (uint32_t j = 0; j < m->nmembers; j++)
                    ndest += m->members[j].state == META_DESTROYED;
                if ((act == 5 || (act == 4 && mb->replaced_by)) && m->nmembers >= 6 && ndest < 2)
                    meta_op_member_state(&e, id, META_DESTROYED, mb->incarnation, META_DOWN_NONE, 0, 0);
                else if (act <= 4) {
                    uint64_t bump = rnd(s, 0, 5) ? 1 : 0;
                    if (bump)
                        s->out_back_tries++;
                    meta_op_member_state(&e, id, META_UP, mb->incarnation + bump, META_DOWN_NONE, 0, 0);
                } else
                    meta_op_node_hold(&e, id, clk + 60000, clk); /* refused on OUT */
            } else if (mb->state == META_DESTROYED) {
                /* DESTROYED: try leaving it (must be refused; catches destroyedback) */
                meta_op_member_state(&e, id, META_UP, mb->incarnation + 1, META_DOWN_NONE, 0, 0);
            } else if (act == 0) {
                meta_op_member_flags(&e, id, inc, (uint8_t)rnd(s, 0, 3));
            } else if (act <= 2) {
                /* NODE_HOLD: valid hold, release (0), or over-bound (catches holdunbounded) */
                uint64_t until = rnd(s, 0, 4) == 0 ? 0
                                 : rnd(s, 0, 2) == 0 ? clk + META_HOLD_MAX_MS + rnd(s, 1, 3600000)
                                                     : clk + rnd(s, 1000, META_HOLD_MAX_MS);
                meta_op_node_hold(&e, id, until, clk);
            } else if (act == 3) {
                /* propose OUT when enough live members remain for volume adds */
                uint32_t alive = 0;
                for (uint32_t j = 0; j < m->nmembers; j++)
                    alive += m->members[j].state < META_OUT;
                if (alive > 3 || s->outs == 0)
                    meta_op_member_state(&e, id, META_OUT, inc, META_DOWN_NONE, 0, 0);
                else
                    meta_op_member_state(&e, id, META_DOWN, inc, (uint8_t)rnd(s, 1, META_DOWN_LAST),
                                         clk - rnd(s, 0, 1000), clk);
            } else if (act == 4) {
                /* DOWN with random reason; occasionally a future since_ms (must be refused) */
                uint64_t since_ms = rnd(s, 0, 9) ? clk - rnd(s, 0, 5000) : clk + 1000;
                meta_op_member_state(&e, id, META_DOWN, inc, (uint8_t)rnd(s, 1, META_DOWN_LAST), since_ms, clk);
            } else if (act == 5 && m->nmembers >= 2) {
                uint32_t target = rnd(s, 0, 2) == 0 ? 0 : m->members[rnd(s, 0, m->nmembers - 1)].id;
                meta_op_node_replace(&e, id, target);
            } else {
                int st = rnd(s, 0, 1) ? META_UP : META_DRAINING;
                meta_op_member_state(&e, id, st, inc, META_DOWN_NONE, 0, 0);
            }
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
        } else if (what == 12 && v && (v->redundancy == META_RED_EC || v->nstripe > 0)) {
            unsigned how = (unsigned)rnd(s, 0, 7);
            if (how <= 2 && v->nstripe > 0) { /* STRIPE_SET: mostly filling with prev, sometimes whole; sometimes drop */
                uint32_t active = v->redundancy == META_RED_EC ? (uint32_t)v->ec_k + v->ec_m : 0;
                if (v->nstripe > active && rnd(s, 0, 1) == 0) {
                    meta_op_stripe_drop(&e, v->number, active, v->stripe_version - (rnd(s, 0, 7) == 0));
                } else {
                    /* Bias toward retired positions when any exist so MUT(64) retireclear triggers quickly */
                    uint32_t pos = (v->nstripe > active && rnd(s, 0, 1) == 0)
                                       ? (uint32_t)rnd(s, active, v->nstripe - 1)
                                       : (uint32_t)rnd(s, 0, v->nstripe);
                    uint32_t node = m->members[rnd(s, 0, m->nmembers - 1)].id;
                    uint8_t st = rnd(s, 0, 3) ? META_STRIPE_FILLING : META_STRIPE_WHOLE;
                    uint32_t prev = (st == META_STRIPE_FILLING && pos < v->nstripe && rnd(s, 0, 2)) ? v->stripe[pos] : 0;
                    meta_op_stripe_set(&e, v->number, pos, node, st, prev, v->stripe_version - (rnd(s, 0, 7) == 0));
                }
            } else if (how <= 4 && v->nstripe > 0) { /* STRIPE_WHOLE */
                uint32_t pos = (uint32_t)rnd(s, 0, v->nstripe - 1);
                for (uint32_t j = 0; j < v->nstripe; j++)
                    if (v->stripe_pos[j].state == META_STRIPE_FILLING) {
                        pos = j;
                        break;
                    }
                uint64_t ver = v->stripe_version - (rnd(s, 0, 3) == 0 && v->stripe_version > 1);
                meta_op_stripe_whole(&e, v->number, pos, ver);
            } else if (how == 5 && v->redundancy == META_RED_EC) {
                meta_op_volume_force(&e, v, v->rep[rnd(s, 0, v->nrep - 1)].node);
            } else if (how == 6 && v->redundancy == META_RED_EC) {
                meta_op_gc_resume(&e, v->number);
            } else if (v->redundancy == META_RED_EC) {
                meta_op_gc_suspend(&e, v->number);
            }
        } else if (what == 13 && v && !shared) {
            /* R10: redundancy conversions, phase steps (including illegal skips to catch convskip),
             * live-object reports, and STRIPE_SET drops (including with live/unknown objects to catch stripelive). */
            if (v->conv.phase == META_CONV_NONE) {
                /* If leftover mixed geometry from a cancelled REENCODE exists, report 0 live objects so it clears;
                 * also occasionally send a stale-epoch VOLUME_LIVE_OBJS to catch MUT(65) livestale in --sim-only. */
                if (v->mixed_redundancy == META_RED_EC) {
                    uint64_t ep = (v->epoch > 1 && rnd(s, 0, 3) == 0) ? v->epoch - 1 : v->epoch;
                    meta_op_volume_live_objs(&e, v->number, ep, v->stripe_version, 0, NULL, 0);
                    continue;
                }
                if (v->epoch > 1 && (v->redundancy == META_RED_EC || v->nstripe > 0) && rnd(s, 0, 5) == 0) {
                    meta_op_volume_live_objs(&e, v->number, v->epoch - 1, v->stripe_version, 0, NULL, 0);
                    continue;
                }
                /* Start a conversion occasionally so volumes also stay free for replica moves (44/46). */
                if (rnd(s, 0, 3) != 0)
                    continue;
                struct meta_redundancy_target t = {0};
                if (v->redundancy == META_RED_EC && rnd(s, 0, 2) == 0) {
                    t.redundancy = META_RED_REPLICAS;
                    t.replicas = 3;
                } else {
                    uint32_t alive = 0;
                    for (uint32_t j = 0; j < m->nmembers; j++)
                        alive += m->members[j].state < META_OUT;
                    t.redundancy = META_RED_EC;
                    t.ec_m = (uint8_t)rnd(s, 1, 2);
                    uint32_t kmax = alive > (uint32_t)t.ec_m + 1 ? alive - t.ec_m : 1;
                    if (kmax > 4)
                        kmax = 4;
                    t.ec_k = (uint8_t)rnd(s, 1, kmax);
                    t.replicas = (uint32_t)t.ec_m + 1;
                }
                meta_op_volume_redundancy(&e, v->number, &t, v->epoch - (stale && v->epoch > 1));
            } else {
                unsigned act = (unsigned)rnd(s, 0, 7);
                if (act == 0) {
                    /* Try a skipped phase (catches MUT(63) convskip in --sim-only!) */
                    uint8_t bad_to = v->conv.phase <= META_CONV_RECORD ? META_CONV_DROP : META_CONV_NONE;
                    meta_op_volume_conv_step(&e, v->number, v->conv.phase, bad_to, NULL, 0);
                } else if (act == 1) {
                    meta_op_volume_conv_cancel(&e, v->number);
                } else if (v->conv.phase == META_CONV_GROW) {
                    if (v->nrep < v->conv.target_replicas) {
                        for (uint32_t j = 0; j < m->nmembers; j++) {
                            const struct meta_member *mj = &m->members[j];
                            uint32_t nid = mj->id;
                            if (mj->state < META_OUT && (mj->flags & META_MF_VOLUMES_OK) &&
                                (mj->ndisks == 0 || (meta_member_effective_cap(mj) & META_CAP_COMPUTE)) &&
                                !meta_replica(v, nid)) {
                                meta_op_replica_add(&e, v->number, v->epoch, nid, 9000 + ++s->resets, 0);
                                break;
                            }
                        }
                    } else if (v->nisr < v->conv.target_replicas) {
                        uint32_t ids[META_REPLICAS_MAX];
                        uint64_t rids[META_REPLICAS_MAX];
                        for (uint32_t j = 0; j < v->nrep; j++) {
                            ids[j] = v->rep[j].node;
                            rids[j] = v->rep[j].rid;
                        }
                        meta_op_isr_change(&e, v->number, v->epoch, v->isr_version, ids, rids, v->nrep, 0);
                    } else {
                        meta_op_volume_conv_step(&e, v->number, META_CONV_GROW, META_CONV_RECORD, NULL, 0);
                    }
                } else if (v->conv.phase == META_CONV_RECORD) {
                    uint32_t stripe[META_STRIPE_MAX], ns = 0;
                    if (v->conv.target_redundancy == META_RED_EC) {
                        uint32_t need = (uint32_t)v->conv.target_k + v->conv.target_m;
                        if (need > v->nstripe) {
                            for (uint32_t j = 0; j < v->nstripe; j++)
                                stripe[ns++] = v->stripe[j];
                            for (uint32_t j = 0; j < m->nmembers && ns < need; j++) {
                                const struct meta_member *mj = &m->members[j];
                                uint32_t nid = mj->id;
                                if (mj->state < META_OUT &&
                                    (mj->ndisks == 0 || (meta_member_effective_cap(mj) & META_CAP_STORAGE)) &&
                                    !has_id_local(stripe, ns, nid))
                                    stripe[ns++] = nid;
                            }
                        }
                    }
                    meta_op_volume_conv_step(&e, v->number, META_CONV_RECORD, META_CONV_REENCODE, ns ? stripe : NULL,
                                             ns);
                } else if (v->conv.phase == META_CONV_REENCODE) {
                    if (v->conv.orig_redundancy == META_RED_EC && act >= 4) {
                        if (act == 4 && v->conv.new_stripe_version > 1) {
                            struct meta_live_sv lv[1] = {{v->conv.new_stripe_version - 1, 2}};
                            meta_op_volume_live_objs(&e, v->number, v->epoch, v->stripe_version, 0, lv, 1);
                        } else {
                            meta_op_volume_live_objs(&e, v->number, v->epoch, v->stripe_version, 0, NULL, 0);
                        }
                    } else {
                        /* Also tries REENCODE -> DROP before live_objs clears old EC objects (catches MUT(66) reencskip) */
                        meta_op_volume_conv_step(&e, v->number, META_CONV_REENCODE, META_CONV_DROP, NULL, 0);
                    }
                } else if (v->conv.phase == META_CONV_DROP) {
                    uint32_t active = v->redundancy == META_RED_EC ? (uint32_t)v->ec_k + v->ec_m : 0;
                    if (v->nstripe > active) {
                        if (act <= 3) {
                            /* Try STRIPE_SET drop (both before and after live_objs: catches MUT(62) stripelive!) */
                            meta_op_stripe_drop(&e, v->number, active, v->stripe_version);
                        } else if (act == 4 && v->stripe_version > 1) {
                            /* Report live objects at an older stripe_version (> 0): next drop must be refused */
                            struct meta_live_sv lv[1] = {{v->stripe_version - 1, 3}};
                            meta_op_volume_live_objs(&e, v->number, v->epoch, v->stripe_version, 0, lv, 1);
                        } else {
                            /* Report 0 live objects at old versions so drop can legally proceed */
                            meta_op_volume_live_objs(&e, v->number, v->epoch, v->stripe_version, 0, NULL, 0);
                        }
                    } else {
                        meta_op_volume_conv_step(&e, v->number, META_CONV_DROP, META_CONV_SHRINK, NULL, 0);
                    }
                } else if (v->conv.phase == META_CONV_SHRINK) {
                    if (v->nrep > v->conv.target_replicas) {
                        uint32_t drop = 0;
                        for (uint32_t j = 0; j < v->nrep; j++)
                            if (v->rep[j].node != v->leader && !meta_volume_in_isr(v, v->rep[j].node)) {
                                drop = v->rep[j].node;
                                break;
                            }
                        if (drop) {
                            meta_op_replica_remove(&e, v->number, v->epoch, drop);
                        } else {
                            uint32_t ids[1] = {v->leader};
                            uint64_t rids[1] = {meta_replica(v, v->leader)->rid};
                            meta_op_isr_change(&e, v->number, v->epoch, v->isr_version, ids, rids, 1, 0);
                        }
                    } else {
                        meta_op_volume_conv_step(&e, v->number, META_CONV_SHRINK, META_CONV_NONE, NULL, 0);
                    }
                }
            }
        } else if (what == 11 && rnd(s, 0, 5) == 0) {
            meta_op_name_group(&e);
        } else if (what == 14 && m->nmembers > 0) {
            /* R13: STORE_ADD (including duplicate store_id across members -> storeaddtwice), STORE_CLASS, STORE_STATE
             * (including UP->OUT and DESTROYED->UP -> storedestroyedback), MEMBER_ROLE (including wider than UP disks
             * -> rolewiden); member 4 gets HDD-only (logonhdd) and member 5 gets log-only NVMe (storage role refusal),
             * while the other members keep disks[0] UP with BOTH capability so volumes/moves/stripes stay healthy. */
            const struct meta_member *mb = &m->members[rnd(s, 0, m->nmembers - 1)];
            unsigned act = (unsigned)rnd(s, 0, 7);
            if (mb->ndisks == 0 || (mb->ndisks == 1 && mb->id != 4 && mb->id != 5 && act <= 3) ||
                (act <= 1 && mb->id != 4 && mb->ndisks < 4)) {
                struct meta_disk d;
                /* Occasionally reuse an existing store_id from another member (catches MUT(67) storeaddtwice) */
                int try_dup = 0;
                if (mb->ndisks > 0 && rnd(s, 0, 2) == 0) {
                    for (uint32_t j = 0; j < m->nmembers && !try_dup; j++)
                        if (m->members[j].ndisks > 0) {
                            d = m->members[j].disks[rnd(s, 0, m->members[j].ndisks - 1)];
                            d.state = META_STORE_UP;
                            try_dup = 1;
                        }
                }
                if (!try_dup) {
                    uint8_t cls, uses;
                    if (mb->id == 4) {
                        cls = META_DISK_HDD;
                        uses = META_DISK_USE_CHUNK;
                    } else if (mb->id == 5) {
                        cls = META_DISK_NVME;
                        uses = META_DISK_USE_LOG; /* log-only: exercises STORAGE role refusal */
                    } else {
                        cls = rnd(s, 0, 1) ? META_DISK_NVME : META_DISK_SSD;
                        uses = META_DISK_USE_ALL;
                    }
                    d = make_disk((uint32_t)(10000 + ++s->disk_seq), cls, uses);
                }
                meta_op_store_add(&e, mb->id, &d);
            } else if (mb->id == 4 && act <= 5 && !shared && m->nvolumes > 0) {
                /* Try placing a log replica on HDD-only member 4 (catches MUT(68) logonhdd in --sim-only) */
                const struct meta_volume *tv = NULL;
                for (uint32_t j = 0; j < m->nvolumes; j++)
                    if (!meta_replica(&m->volumes[j], mb->id)) {
                        tv = &m->volumes[j];
                        break;
                    }
                if (tv)
                    meta_op_replica_add(&e, tv->number, tv->epoch, mb->id, 9000 + ++s->resets, 0);
                else
                    meta_op_member_role(&e, mb->id, META_ROLE_COMPUTE);
            } else if (act == 2 && mb->ndisks > 1) {
                const struct meta_disk *d = &mb->disks[rnd(s, 1, mb->ndisks - 1)];
                uint8_t cls = mb->id == 4 ? META_DISK_HDD : (uint8_t)rnd(s, META_DISK_NVME, META_DISK_CLASS_LAST);
                meta_op_store_class(&e, mb->id, d->store_id, cls);
            } else if (act <= 5 && mb->ndisks > 1) {
                /* Exercise state machine on disks[1..] while keeping disks[0] UP on normal members */
                const struct meta_disk *d = &mb->disks[rnd(s, 1, mb->ndisks - 1)];
                uint8_t nst;
                if (d->state == META_STORE_DESTROYED)
                    nst = META_STORE_UP; /* must be refused: catches MUT(70) storedestroyedback */
                else if (d->state == META_STORE_UP)
                    nst = rnd(s, 0, 3) == 0 ? META_STORE_OUT : META_STORE_DOWN;
                else if (d->state == META_STORE_DOWN)
                    nst = rnd(s, 0, 1) ? META_STORE_OUT : META_STORE_UP;
                else /* OUT */
                    nst = rnd(s, 0, 1) ? META_STORE_DESTROYED : META_STORE_UP;
                meta_op_store_state(&e, mb->id, d->store_id, nst);
            } else {
                /* Member 4 tries COMPUTE (refused), member 5 tries STORAGE (refused), others stay BOTH */
                uint8_t nrole = mb->id == 4 ? META_ROLE_COMPUTE
                                : mb->id == 5 ? META_ROLE_STORAGE
                                              : META_ROLE_BOTH;
                meta_op_member_role(&e, mb->id, nrole);
            }
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
    if (ok && s->out_back_tries >= 3 && s->out_backs == 0)
        snprintf(s->fail, sizeof s->fail, "%" PRIu64 " OUT -> UP proposals tried (%" PRIu64 " OUT), none applied",
                 s->out_back_tries, s->outs), ok = 0;
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
    const char *env_tmp = getenv("TMPDIR");
    const char *dir = (env_tmp && *env_tmp) ? env_tmp : "/tmp";
    unsigned seeds = 40;
    uint64_t first = 1;
    double seconds = 20;
    int mutant = 0, sim_only = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--seeds") && i + 1 < argc)
            seeds = (unsigned)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--first") && i + 1 < argc)
            first = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--seconds") && i + 1 < argc)
            seconds = strtod(argv[++i], NULL);
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *mn = argv[++i];
            if (!strcmp(mn, "stripelive"))
                mutant = 62;
            else if (!strcmp(mn, "convskip"))
                mutant = 63;
            else if (!strcmp(mn, "retireclear"))
                mutant = 64;
            else if (!strcmp(mn, "livestale"))
                mutant = 65;
            else if (!strcmp(mn, "reencskip"))
                mutant = 66;
            else if (!strcmp(mn, "storeaddtwice"))
                mutant = 67;
            else if (!strcmp(mn, "logonhdd"))
                mutant = 68;
            else if (!strcmp(mn, "rolewiden"))
                mutant = 69;
            else if (!strcmp(mn, "storedestroyedback"))
                mutant = 70;
            else
                mutant = atoi(mn);
        } else if (!strcmp(argv[i], "--sim-only")) /* the simulator's checker alone, to see it catch a planted bug */
            sim_only = 1;
        else if (argv[i][0] != '-' && *argv[i])
            dir = argv[i];
    }
    /* The deadline covers the whole run, so it grows with the seeds: a seed of 30 virtual seconds takes about 3.4 s
     * (0.11 s per virtual second, 2026-11-23, i5-13400), and make meta-soak's 5000 seeds about 4.7 hours. One real
     * second per virtual second, never under 1200 s; a fixed 1200 s failed every soak above about 350 seeds. */
    double budget = (double)seeds * (seconds > 0 ? seconds : 0);
    th_watchdog("test_meta", budget > 1e7 ? 10000000L : budget > 1200 ? (long)budget : 1200);
    iso_log_quiet = true;
    if (sim_only && !mutant) {
        fprintf(stderr, "test_meta: --sim-only is for --mutant runs\n");
        return 2;
    }
#ifdef META_MUTANTS
    extern int meta_mutant;
    meta_mutant = mutant;
#else
    if (mutant) {
        fprintf(stderr, "test_meta: --mutant needs the build with -DMETA_MUTANTS (make test_meta_mut)\n");
        return 2;
    }
#endif
    if (!sim_only) {
        test_state_machine();
        test_replica_set();
        test_ec();
        test_r9_meta();
        test_r10_meta();
        test_r13_meta();
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
