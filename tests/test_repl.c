/* SPDX-License-Identifier: AGPL-3.0-only */
/* Scenarios of the replication core (src/repl.c) that the simulator (tests/sim_repl.c) reaches rarely or never, each
 * driven by hand, event by event. Built twice: build/test_repl, and build/test_repl_mut (-DREPL_MUTANTS), where
 * `--mutant N` plants bug N and the scenario that guards it must fail (tests/sim_repl.sh quick checks that it does).
 *
 *   trim        a leader whose log no longer has a lagging replica's next record tells it to install a snapshot;
 *               before, one ACK fail-stopped the leader (and every next one). MUT(36) is the old behaviour.
 *   cut_applied a cut to the applied end does not look up a record the log no longer retains (MUT(36)).
 *   sync_cut    a SYNC issued before a cut, reported after the replica wrote the cut positions again from a new
 *               leader, does not count those rewritten records as durable (MUT(35)).
 *   promote     a promotion keeps the old leader, which did not answer the fence, in the ISR (MUT(34): it dropped
 *               it, and a loss of the promoted member's disk then stranded the volume: sim seed 76).
 *   unanswered  the new leader drops a kept member that does not ACK within unanswered_ticks, long before isr_lag
 *               (MUT(37)); the simulator cannot see this: it costs time, not safety.
 *   open_damage a log reopened with an epoch history or a floor that cannot be right fails at once (the branches of
 *               repl_open that nothing reached).
 *   follower_damage a follower fails, never repairs, when the leader's log differs from its own below the verified end
 *               (MUT(30)), or a cut would pass its applied end or a position its log no longer retains.
 *   below_start a follower filled by a snapshot (log start and epoch history above 1) skips the records below its log
 *               start that a new leader's catch-up sends from position 1, and still fails on another epoch at a
 *               position it holds (MUT(59): judged them by the history and stopped).
 *   idle_shrink a member that stops acknowledging leaves the ISR after isr_lag although nothing is written (MUT(40):
 *               only a member behind the log end isr_lag ago went, so an idle volume kept a dead member, and min_isr
 *               never refused).
 *   round       a cut can fall inside a record: a replica whose last epoch the leader lacks cuts at the end of the
 *               leader's epoch before it, inside an older epoch's record that only the replica has.
 *   replay_floor the leader's replay floor: the lowest log start of itself and every member, unknown until each has
 *               ACKed in the current epoch (MUT(39): members not heard from were left out).
 *   steady_join a learner that keeps up with writes that never pause, a few ticks behind, joins the ISR: the commit
 *               point is held for it until it has it (MUT(42): no hold; it joined only in a pause of the writes, and
 *               behind a lone leader never). When asked for, it has every position up to the commit point.
 *   join_giveup a learner that stops short under the hold: the commit point waits at most join_hold_ticks, then
 *               moves on, and is not held again for 4 x isr_lag; a learner more than half isr_lag behind is not held for.
 *   reconfig    a replica set change (D8): a member's match moves with it across an index shift (MUT(43): state kept
 *               by index), a removed member no longer counts (MUT(45)); the leader refuses its own removal, an ISR
 *               member's and a bad map; a message of another set version is dropped and counted (MUT(44)).
 *   leave       a move's shrink (D11): asked for once, only while the ISR without the member keeps min_isr (MUT(49)),
 *               and the leaving member is never asked for again (MUT(48)); a cancel lets it rejoin.
 *   target      a planned promotion picks its target or nobody (MUT(50): the lowest index won a tie), and no reply is
 *               left `unanswered`.
 *   replay      the replay start (D17) in ACKs, an empty log's "nothing to replay", and the floor with a snapshot
 *               notice outstanding (MUT(46): the leader's own replay start only); a reordered ACK of an empty log
 *               is not taken once a start was heard (MUT(58)), a lower start after an install is (replay_floor).
 *   snapshot    a fresh learner whose first position is not retained gets the notice; a snapshot is wanted only above
 *               the log end (MUT(47)); an install at P with log start S <= P + 1 opens, and its ACK stops the notice.
 *   peer_match  what a planned transfer's drain (D14) reads of its target: the durable end acknowledged as the instance
 *               the record names, the leader's own durable end for itself, 0 for an index outside the set and for a
 *               member whose ACKs come from another instance (a wiped disk copied again must not pass for the drained
 *               target: its match was earned by a log the promotion may not find); a stale ACK of the old instance
 *               must not flip it back (stale_rid).
 *   stale_replay a stale ACK with a higher nonzero replay start sent before a snapshot install lowered it (and before
 *               a crash + install even if its unsynced written end exceeded the install) is dropped (MUT(65)).
 *   stale_rid   a stale ACK carrying an ISR member's initial rid (before or after its ISR drop) must not overwrite
 *               p->rid back, raise match or replay, or revive it (MUT(66)), while delayed/duplicated ACKs of an
 *               earlier learner rid never permanently silence a live instance.
 *   stale_epoch a leader ignores an ACK carrying an older epoch even when its durable end is within r->written (MUT(8)).
 */
#include "repl.h"

#include <inttypes.h>
#include "watchdog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAXREC 4096

struct drv {
    struct repl_rec log[MAXREC];
    uint32_t n;
    uint64_t start;            /* first retained position */
    struct repl_action act[256];
    int nact;
};

static int failures;

#define CHECK(c, ...)                                                                                                 \
    do {                                                                                                              \
        if (!(c)) {                                                                                                   \
            fprintf(stderr, "test_repl: %s:%d: ", __func__, __LINE__);                                                \
            fprintf(stderr, __VA_ARGS__);                                                                             \
            fprintf(stderr, "\n");                                                                                    \
            failures++;                                                                                               \
            return;                                                                                                   \
        }                                                                                                             \
    } while (0)

static int rec_find(void *ctx, uint64_t pos, uint64_t *s, uint32_t *n)
{
    const struct drv *d = ctx;
    if (pos < d->start)
        return 1;
    for (uint32_t i = 0; i < d->n; i++)
        if (d->log[i].pos <= pos && pos < d->log[i].pos + d->log[i].n) {
            *s = d->log[i].pos;
            *n = d->log[i].n;
            return 0;
        }
    return -1;
}

/* Carry out every action: WRITE and TRUNCATE on the fake log, the rest kept in d->act for the scenario to look at. */
static void drain(struct repl *r, struct drv *d, const struct repl_msg *cur)
{
    struct repl_action a;
    d->nact = 0;
    while (repl_next_action(r, &a)) {
        if (a.type == REPL_A_WRITE) {
            struct repl_rec rec = a.rec;
            if (a.src == REPL_SRC_MSG && cur)
                rec = cur->rec[a.idx];
            d->log[d->n++] = rec;
        } else if (a.type == REPL_A_TRUNCATE) {
            while (d->n && d->log[d->n - 1].pos > a.pos)
                d->n--;
        }
        if (d->nact < 256)
            d->act[d->nact++] = a;
    }
}

static const struct repl_action *find_act(const struct drv *d, int type, int msg)
{
    for (int i = 0; i < d->nact; i++)
        if (d->act[i].type == type && (msg == 0 || d->act[i].msg.type == msg))
            return &d->act[i];
    return NULL;
}

static struct repl_config config(int self, struct drv *d)
{
    return (struct repl_config){.self = self, .nnodes = 3, .min_isr = 1, .isr_lag_ticks = 1000, .resend_ticks = 10,
                                .heartbeat_ticks = 3, .floor_ticks = 20, .unanswered_ticks = 20, .max_batch = 16,
                                .window = 512, .rec_find = rec_find, .ctx = d};
}

/* Records of `n` positions each from position `from` to `to`, in `epoch`. */
static void fill(struct drv *d, uint64_t from, uint64_t to, uint64_t epoch, uint32_t n)
{
    for (uint64_t p = from; p <= to; p += n)
        d->log[d->n++] = (struct repl_rec){.pos = p, .epoch = epoch, .n = n};
}

static struct repl_msg meta_msg(int to, uint64_t epoch, int leader, uint32_t isr, uint32_t unanswered)
{
    struct repl_msg m = {.type = REPL_M_META, .from = REPL_META, .to = to, .epoch = epoch, .leader = -1};
    m.meta = (struct repl_vmeta){.epoch = epoch, .isr_version = 7, .leader = leader, .isr = isr,
                                 .rid = {11, 22, 33}, .unanswered = unanswered};
    return m;
}

/* An ACK beyond what the replica can have verified (durable above written, written above the leader's log: a flipped
 * bit, the peer links carry no frame check by default) is dropped: the commit point stays at the replica's real
 * durable end (MUT(64) took it: the commit point jumped to the leader's end). */
static void ack_bound(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 1000, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 400, .log_start = 1, .log_end = 1000, .hist = h,
                              .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "open");
    struct repl_msg m = meta_msg(0, 2, 0, 0x3, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    struct repl_msg k = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 2, .leader = -1, .durable = 500,
                         .written = 500, .rid = 22, .resync = 1};
    repl_recv(r, &k);
    drain(r, &d, NULL);
    uint64_t c1 = repl_commit(r);
    CHECK(c1 <= 500, "the commit point %" PRIu64 " passed the replica's durable end 500", c1);
    struct repl_msg bad[2] = {k, k};
    bad[0].durable = 1ull << 40; /* durable above written */
    bad[0].resync = 0;
    bad[1].durable = bad[1].written = 1ull << 40; /* written above the leader's log */
    bad[1].resync = 0;
    for (int i = 0; i < 2; i++) {
        repl_recv(r, &bad[i]);
        drain(r, &d, NULL);
        CHECK(!repl_failed(r) && repl_is_leader(r), "the leader failed on a bad ACK (%d)", i);
        CHECK(repl_commit(r) == c1, "a bad ACK (%d) moved the commit point from %" PRIu64 " to %" PRIu64, i, c1,
              (uint64_t)repl_commit(r));
    }
    /* control: an honest ACK of the whole log moves it */
    k.durable = k.written = repl_written(r);
    k.resync = 0;
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_commit(r) > c1, "an honest ACK of %" PRIu64 " left the commit point at %" PRIu64, (uint64_t)k.durable,
          (uint64_t)repl_commit(r));
    repl_free(r);
}

static void trim(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 1000, 1, 1);
    d.start = 900;
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 1000, .log_start = 900, .log_end = 1000, .hist = h,
                              .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "open");
    struct repl_msg m = meta_msg(0, 2, 0, 0x1, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    /* A replica back after its segments were deleted, outside the ISR, acknowledges its end 10. */
    struct repl_msg k = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 2, .leader = -1, .durable = 10,
                         .written = 10, .rid = 22, .resync = 1};
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(!repl_failed(r) && repl_is_leader(r), "the leader failed on an ACK below its log start");
    const struct repl_action *s = find_act(&d, REPL_A_SEND, REPL_M_SNAPSHOT);
    CHECK(s && s->msg.to == 1 && s->msg.epoch == 2, "no SNAPSHOT notice to the lagging replica");
    CHECK(!find_act(&d, REPL_A_SEND, REPL_M_APPEND), "an APPEND of records the log no longer has");
    /* It repeats the notice no faster than resend_ticks. */
    int notices = 0;
    for (int t = 0; t < 25; t++) {
        repl_tick(r);
        drain(r, &d, NULL);
        notices += find_act(&d, REPL_A_SEND, REPL_M_SNAPSHOT) != NULL;
    }
    CHECK(notices >= 2 && notices <= 3, "%d notices in 25 ticks", notices);
    /* The replica installs the snapshot, P = 1000, and acknowledges it: it gets appends again. */
    repl_write(r, 1, &(uint64_t){0});
    drain(r, &d, NULL);
    k.durable = k.written = 1000;
    repl_recv(r, &k);
    drain(r, &d, NULL);
    s = find_act(&d, REPL_A_SEND, REPL_M_APPEND);
    CHECK(s && s->msg.prev_pos == 1000 && s->upto == repl_written(r), "no append after the snapshot");
    repl_free(r);

    /* The follower's side: a notice from its leader asks the driver for a snapshot from it. */
    static struct drv f;
    memset(&f, 0, sizeof f);
    fill(&f, 1, 10, 1, 1);
    c = config(1, &f);
    du = (struct repl_durable){.rid = 22, .fence = 1, .floor = 10, .log_end = 10, .hist = h, .nhist = 1};
    r = repl_open(&c, &du);
    m = meta_msg(1, 2, 0, 0x1, 0);
    repl_recv(r, &m);
    drain(r, &f, NULL);
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 2, .leader = 0, .last_epoch = 1,
                          .written = 1000, .prev_epoch = 1, .prev_pos = 10, .end = 1};
    repl_recv(r, &an);
    drain(r, &f, NULL);
    /* A notice about a position it already has (the leader went back after a lost ACK; sim seed 75881 stalled on
     * it): it tells the leader where it is, and asks for no snapshot. */
    struct repl_msg sn = {.type = REPL_M_SNAPSHOT, .from = 0, .to = 1, .epoch = 2, .leader = -1, .written = 9};
    repl_recv(r, &sn);
    drain(r, &f, NULL);
    s = find_act(&f, REPL_A_SEND, REPL_M_ACK);
    CHECK(s && s->msg.written == 10 && s->msg.resync && !find_act(&f, REPL_A_SNAPSHOT, 0),
          "a stale notice asked for a snapshot instead of an ACK of 10");
    sn.written = 10;
    repl_recv(r, &sn);
    drain(r, &f, NULL);
    s = find_act(&f, REPL_A_SNAPSHOT, 0);
    CHECK(s && s->pos == 0, "the follower did not ask for a snapshot from its leader");
    repl_free(r);

    /* Reopened on the snapshot: an empty log that starts after P. */
    memset(&f, 0, sizeof f);
    f.start = 1001;
    struct repl_epoch h2[1] = {{.epoch = 1, .start = 1}}; /* the epoch of P; the leader's marker of 2 is at 1001 */
    du = (struct repl_durable){.rid = 22, .fence = 2, .floor = 1000, .log_start = 1001, .log_end = 1000,
                               .hist = h2, .nhist = 1};
    r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r) && repl_applied(r) == 1000, "open on a snapshot");
    repl_free(r);
    du.floor = 999;
    r = repl_open(&c, &du);
    CHECK(r && repl_failed(r), "a log start above the floor is accepted");
    repl_free(r);
}

/* A replica reopened with a COMMIT marker at 7 whose file it has not staged (repl_durable.hold): its durable end starts
 * at 6, not at its log end, and its ACK after the leader verified its log says so. It said 10: the leader kept its
 * segments, and the link the file is fetched from, only up to what the ACK claimed, and dropped the marker's
 * (jepsen-cluster kill, 2026-11-18; test_cluster reopenhold). */
static void open_hold(void)
{
    static struct drv f;
    memset(&f, 0, sizeof f);
    fill(&f, 1, 10, 1, 1);
    struct repl_config c = config(1, &f);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_durable du = {.rid = 22, .fence = 1, .floor = 5, .log_end = 10, .hold = 7, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r) && repl_durable_end(r) == 6 && repl_written(r) == 10,
          "a replica opened with a hold at 7 starts durable to %llu", r ? (unsigned long long)repl_durable_end(r) : 0);
    struct repl_msg m = meta_msg(1, 2, 0, 0x1, 0);
    repl_recv(r, &m);
    drain(r, &f, NULL);
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 2, .leader = 0, .last_epoch = 1,
                          .written = 1000, .prev_epoch = 1, .prev_pos = 10, .end = 1};
    repl_recv(r, &an);
    drain(r, &f, NULL);
    const struct repl_action *s = find_act(&f, REPL_A_SEND, REPL_M_ACK);
    CHECK(s && s->msg.written == 10 && s->msg.durable == 6, "the ACK after the verification says durable %llu",
          s ? (unsigned long long)s->msg.durable : 0);
    repl_free(r);
}

static void cut_applied(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 1000, 1, 1);
    fill(&d, 1001, 1010, 2, 1);
    d.start = 1001; /* everything applied went into parts */
    struct repl_epoch h[2] = {{.epoch = 1, .start = 1}, {.epoch = 2, .start = 1001}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 2, .floor = 1000, .log_start = 1001, .log_end = 1010, .hist = h,
                              .nhist = 2};
    struct repl *r = repl_open(&c, &du);
    struct repl_msg m = meta_msg(1, 3, 0, 0x3, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    /* The new leader never had epoch 2: its epoch 1 ends at 1000, the replica's applied end. */
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 3, .leader = 0, .last_epoch = 1,
                          .written = 1000, .prev_epoch = 2, .prev_pos = 1010, .end = 1};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(!repl_failed(r), "a cut at the applied end failed (it looked up a record the log no longer has)");
    const struct repl_action *t = find_act(&d, REPL_A_TRUNCATE, 0);
    CHECK(t && t->pos == 1000 && repl_written(r) == 1000, "no cut to 1000");
    repl_free(r);
}

/* A replica reopened on a log that cannot be right is failed at once (damage is never silent): an epoch history
 * that is not increasing, starts at 0 or past the log, a floor above the log's end. The sound log opens first, so
 * each refusal is about the one value changed. */
static void open_damage(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    fill(&d, 101, 200, 2, 1);
    struct repl_config c = config(1, &d);
    struct repl_epoch good[2] = {{.epoch = 1, .start = 1}, {.epoch = 2, .start = 101}};
    struct repl_durable du = {.rid = 22, .fence = 2, .floor = 150, .log_start = 1, .log_end = 200, .hist = good,
                              .nhist = 2};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "a sound log is refused");
    repl_free(r);
    struct {
        const char *what;
        struct repl_epoch h[2];
        uint64_t floor;
    } bad[] = {
        {"epochs not increasing", {{.epoch = 2, .start = 1}, {.epoch = 2, .start = 101}}, 150},
        {"starts not increasing", {{.epoch = 1, .start = 101}, {.epoch = 2, .start = 101}}, 150},
        {"a start of 0", {{.epoch = 1, .start = 0}, {.epoch = 2, .start = 101}}, 150},
        {"a start past the log's end", {{.epoch = 1, .start = 1}, {.epoch = 2, .start = 201}}, 150},
        {"the floor above the log's end", {{.epoch = 1, .start = 1}, {.epoch = 2, .start = 101}}, 201},
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        struct repl_durable x = du;
        x.hist = bad[i].h;
        x.floor = bad[i].floor;
        r = repl_open(&c, &x);
        CHECK(r && repl_failed(r), "%s: the log is accepted", bad[i].what);
        repl_free(r);
    }
}

/* A replica with log 1..`end` (epoch 1; 1..`applied` applied, `start` the first retained position), told that
 * node 0 leads epoch 3. With `answered` the leader's answer says the logs agree, so the replica follows. */
static struct repl *follower(struct drv *d, uint64_t end, uint64_t applied, uint64_t start, int answered)
{
    memset(d, 0, sizeof *d);
    fill(d, 1, end, 1, 1);
    d->start = start;
    static struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    static struct repl_config c;
    c = config(1, d);
    struct repl_durable du = {.rid = 22, .fence = 1, .floor = applied, .log_start = start, .log_end = end, .hist = h,
                              .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    if (!r)
        return NULL;
    struct repl_msg m = meta_msg(1, 3, 0, 0x3, 0);
    repl_recv(r, &m);
    drain(r, d, NULL);
    if (answered) {
        struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 3, .leader = 0, .last_epoch = 1,
                              .written = end, .prev_epoch = 1, .prev_pos = end, .end = 1};
        repl_recv(r, &an);
        drain(r, d, NULL);
    }
    return r;
}

/* The follower fails (never repairs, never skips) when the leader's log differs from its own below the verified end,
 * and when a cut would pass its applied end or a position its log no longer retains. */
static void follower_damage(void)
{
    static struct drv d;
    struct repl *r = follower(&d, 10, 5, 1, 1);
    CHECK(r && !repl_failed(r), "the follower did not start");
    /* Control: an append that continues the log is taken. */
    struct repl_rec ok[1] = {{.pos = 11, .epoch = 3, .n = 1}};
    struct repl_msg ap = {.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .prev_pos = 10, .prev_epoch = 1,
                          .nrec = 1, .rec = ok, .commit = 5};
    repl_recv(r, &ap);
    drain(r, &d, NULL);
    CHECK(!repl_failed(r) && repl_written(r) == 11, "an append that continues the log was refused (written %" PRIu64
          ")", (uint64_t)repl_written(r));
    /* The leader says position 5 is in epoch 2 while ours has epoch 1 there: another epoch at a position both logs
     * hold is damage, never a cut (MUT(30) lets it pass). */
    ap = (struct repl_msg){.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .prev_pos = 5, .prev_epoch = 2,
                           .commit = 5};
    repl_recv(r, &ap);
    drain(r, &d, NULL);
    CHECK(repl_failed(r), "an append whose previous record is of another epoch than ours there was accepted");
    CHECK(find_act(&d, REPL_A_FAIL, 0), "no FAIL action");
    repl_free(r);

    /* A record at a position we hold, of another epoch. */
    r = follower(&d, 10, 5, 1, 1);
    CHECK(r && !repl_failed(r), "the follower did not start (2)");
    struct repl_rec other[1] = {{.pos = 7, .epoch = 2, .n = 1}};
    ap = (struct repl_msg){.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .prev_pos = 0, .prev_epoch = 0,
                           .nrec = 1, .rec = other, .commit = 5};
    repl_recv(r, &ap);
    drain(r, &d, NULL);
    CHECK(repl_failed(r), "a record of another epoch at a position we hold was accepted");
    repl_free(r);

    /* A leader whose epoch 1 ends below our applied end: the cut would drop applied records. */
    r = follower(&d, 1000, 1000, 1001, 0);
    CHECK(r && !repl_failed(r), "the follower did not start (3)");
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 3, .leader = 0, .last_epoch = 1,
                          .written = 900, .prev_epoch = 1, .prev_pos = 1000, .end = 1};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(repl_failed(r), "a cut below the applied end was not refused");
    repl_free(r);

    /* A cut above the applied end, to a position the log no longer retains (it was trimmed under the replica). */
    r = follower(&d, 1010, 1000, 1001, 0);
    CHECK(r && !repl_failed(r), "the follower did not start (4)");
    d.start = 1006;
    an.written = 1005;
    an.prev_pos = 1010;
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(repl_failed(r), "a cut to a position the log does not retain was not refused");
    repl_free(r);
}

/* A follower filled by a snapshot: its log starts at 1001 and its epoch history at 900 (epoch 2), what an install
 * keeps. A new leader's driver sends a member it has not heard from yet its log from the start (repld catch_up): the
 * records below 1001 are not in this log, are committed, and are skipped, also when the history has no epoch for them;
 * the ones it holds are judged as before (MUT(59) judged them all: the member stopped, jepsen-cluster churn). */
static void below_start(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1001, 1010, 2, 1);
    d.start = 1001;
    struct repl_epoch h[1] = {{.epoch = 2, .start = 900}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 2, .floor = 1010, .log_start = 1001, .log_end = 1010, .hist = h,
                              .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "the follower did not open");
    struct repl_msg m = meta_msg(1, 3, 0, 0x3, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 3, .leader = 0, .last_epoch = 2,
                          .written = 1010, .prev_epoch = 2, .prev_pos = 1010, .end = 1};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(!repl_failed(r), "the follower failed on the answer");
    /* the leader's log from 1: epoch 1 up to 899, epoch 2 from 900; then its own epoch 3 record at 1011 */
    struct repl_rec rec[16];
    uint32_t n = 0;
    for (uint64_t p = 1; p <= 4; p++)
        rec[n++] = (struct repl_rec){.pos = p, .epoch = 1, .n = 1};
    struct repl_msg ap = {.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .prev_pos = 0, .prev_epoch = 0,
                          .nrec = n, .rec = rec, .commit = 1010};
    repl_recv(r, &ap);
    drain(r, &d, NULL);
    CHECK(!repl_failed(r), "records below the log start were judged by an epoch history that does not reach them");
    n = 0;
    for (uint64_t p = 998; p <= 1011; p++)
        rec[n++] = (struct repl_rec){.pos = p, .epoch = p == 1011 ? 3 : 2, .n = 1};
    ap = (struct repl_msg){.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .prev_pos = 850, .prev_epoch = 1,
                           .nrec = n, .rec = rec, .commit = 1010};
    repl_recv(r, &ap);
    drain(r, &d, NULL);
    CHECK(!repl_failed(r) && repl_written(r) == 1011,
          "an append from below the log start, on through the log to a new record, was refused (written %" PRIu64 ")",
          (uint64_t)repl_written(r));
    repl_free(r);

    /* Controls: at a position the log holds, another epoch is damage all the same, the log start (1001) included.
     * Each differs from the log at 1001 alone: a record there (MUT(32): skipped as below the start), then a prev_pos
     * there (MUT(33): not judged), the records around it agreeing. */
    for (int k = 0; k < 2; k++) {
        memset(&d, 0, sizeof d);
        fill(&d, 1001, 1010, 2, 1);
        d.start = 1001;
        r = repl_open(&c, &du);
        CHECK(r && !repl_failed(r), "the follower did not open (control %d)", k);
        repl_recv(r, &m);
        drain(r, &d, NULL);
        repl_recv(r, &an);
        drain(r, &d, NULL);
        n = 0;
        for (uint64_t p = k ? 1002 : 999; p <= 1003; p++)
            rec[n++] = (struct repl_rec){.pos = p, .epoch = p <= 1001 ? 1 : 2, .n = 1};
        ap = (struct repl_msg){.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .prev_pos = k ? 1001 : 998,
                               .prev_epoch = 1, .nrec = n, .rec = rec, .commit = 1010};
        repl_recv(r, &ap);
        drain(r, &d, NULL);
        CHECK(repl_failed(r), "%s of another epoch at the log start was accepted", k ? "a prev_pos" : "a record");
        repl_free(r);
    }
}

static void sync_cut(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 40, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 1, .floor = 40, .log_end = 40, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    /* Follow leader 0 of epoch 2 (its marker at 41) and take 41..46. */
    struct repl_msg m = meta_msg(1, 2, 0, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 2, .leader = 0, .last_epoch = 1,
                          .written = 40, .prev_epoch = 1, .prev_pos = 40, .end = 1};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    struct repl_rec recs[6];
    for (int i = 0; i < 6; i++)
        recs[i] = (struct repl_rec){.pos = 41 + i, .epoch = 2, .n = 1};
    struct repl_msg ap = {.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 2, .leader = 0, .prev_pos = 40,
                          .prev_epoch = 1, .commit = 40, .nrec = 6, .rec = recs};
    repl_recv(r, &ap);
    drain(r, &d, &ap);
    CHECK(repl_written(r) == 46 && find_act(&d, REPL_A_SYNC, 0), "no sync of 41..46");
    /* Before that sync finishes, leader 2 of epoch 3 (which had 41..44 of epoch 2) takes over: cut after 44. */
    m = meta_msg(1, 3, 2, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    an = (struct repl_msg){.type = REPL_M_ANSWER, .from = 2, .to = 1, .epoch = 3, .leader = 2, .last_epoch = 2,
                           .written = 44, .prev_epoch = 2, .prev_pos = 46, .end = 2};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(repl_written(r) == 44, "no cut to 44 (written %" PRIu64 ")", repl_written(r));
    an.prev_pos = 44;
    an.end = 3;
    repl_recv(r, &an);
    drain(r, &d, NULL);
    for (int i = 0; i < 6; i++)
        recs[i] = (struct repl_rec){.pos = 45 + i, .epoch = 3, .n = 1};
    ap = (struct repl_msg){.type = REPL_M_APPEND, .from = 2, .to = 1, .epoch = 3, .leader = 2, .prev_pos = 44,
                           .prev_epoch = 2, .commit = 40, .nrec = 6, .rec = recs};
    repl_recv(r, &ap);
    drain(r, &d, &ap);
    CHECK(repl_written(r) == 50, "no write of 45..50 from the new leader");
    /* The old sync finishes; the driver reports the end it was issued for. 45 and 46 are new records: unsynced. */
    repl_synced(r, 46);
    drain(r, &d, NULL);
    CHECK(repl_durable_end(r) <= 44, "durable end %" PRIu64 " counts records written after the sync was issued",
          repl_durable_end(r));
    const struct repl_action *k = find_act(&d, REPL_A_SEND, REPL_M_ACK);
    CHECK(!k || k->msg.durable <= 44, "ACK of durable %" PRIu64, k->msg.durable);
    CHECK(find_act(&d, REPL_A_SYNC, 0), "no new sync for 45..50");
    repl_free(r);
}

static void promote(void)
{
    struct repl_vmeta m = {.epoch = 6, .isr_version = 9, .leader = 0, .isr = 0x3, .rid = {11, 22}};
    /* Node 0 (the old leader) is paused through the failover; node 1 answers. */
    struct repl_fence_reply rep[1] = {{.node = 1, .durable = 500, .last_epoch = 6, .rid = 22}};
    int rc = repl_meta_promote(&m, 6, 9, 0x3, rep, 1);
    CHECK(rc == REPL_OK && m.leader == 1 && m.epoch == 7, "promotion");
    CHECK(m.isr == 0x3 && m.rid[0] == 11, "the old leader, which holds every committed position, left the ISR "
          "(isr %#x)", m.isr);
    CHECK(m.unanswered == 0x1, "unanswered %#x", m.unanswered);
    /* An ISR change keeps `unanswered` within the ISR. */
    struct repl_vmeta req = m;
    req.isr = 0x2;
    CHECK(repl_meta_isr_change(&m, &req) == REPL_OK && m.unanswered == 0, "unanswered after a shrink");
    /* A member that answered as another instance (a wiped disk) is out, answered or not. */
    m = (struct repl_vmeta){.epoch = 6, .isr_version = 9, .leader = 0, .isr = 0x7, .rid = {11, 22, 33}};
    struct repl_fence_reply rep2[2] = {{.node = 1, .durable = 500, .rid = 22}, {.node = 2, .durable = 0, .rid = 99}};
    rc = repl_meta_promote(&m, 6, 9, 0x7, rep2, 2);
    CHECK(rc == REPL_OK && m.isr == 0x3 && m.rid[2] == 0 && m.unanswered == 0x1, "isr %#x unanswered %#x", m.isr,
          m.unanswered);
}

static void unanswered(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    /* Promoted in epoch 2 with ISR {0, 1, 2}; node 0, the old leader, did not answer the fence. */
    struct repl_msg m = meta_msg(1, 2, 1, 0x7, 0x1);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    struct repl_msg k = {.type = REPL_M_ACK, .from = 2, .to = 1, .epoch = 2, .leader = -1, .durable = 101,
                         .written = 101, .rid = 33, .resync = 1};
    const struct repl_action *q = NULL;
    int t;
    for (t = 1; t <= 100 && !q; t++) {
        repl_recv(r, &k); /* node 2 keeps up; node 0 is silent */
        drain(r, &d, NULL);
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    }
    CHECK(q && q->msg.meta.isr == 0x6, "the silent old leader was not dropped within 100 ticks");
    CHECK(t <= (int)c.unanswered_ticks + 2, "dropped only after %d ticks (unanswered_ticks %u)", t, c.unanswered_ticks);
    repl_free(r);
}

static void idle_shrink(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    c.isr_lag_ticks = 50;
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    struct repl_msg m = meta_msg(0, 2, 0, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    /* both members ACK the EPOCH marker (position 101); then node 1 goes silent and nothing more is written */
    struct repl_msg k1 = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 2, .leader = -1, .durable = 101,
                          .written = 101, .rid = 22, .resync = 1};
    struct repl_msg k2 = {.type = REPL_M_ACK, .from = 2, .to = 0, .epoch = 2, .leader = -1, .durable = 101,
                          .written = 101, .rid = 33, .resync = 1};
    repl_recv(r, &k1);
    repl_recv(r, &k2);
    drain(r, &d, NULL);
    const struct repl_action *q = NULL;
    int t;
    for (t = 1; t <= 3 * (int)c.isr_lag_ticks && !q; t++) {
        repl_recv(r, &k2); /* node 2 answers every heartbeat */
        drain(r, &d, NULL);
        if ((q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ)) != NULL)
            break;
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    }
    CHECK(q && q->msg.meta.isr == 0x5, "a silent member of an idle volume stayed in the ISR for %d ticks (isr_lag %u)",
          t, c.isr_lag_ticks);
    CHECK(!q || t <= (int)c.isr_lag_ticks + 3, "dropped only after %d ticks (isr_lag %u)", t, c.isr_lag_ticks);
    repl_free(r);
}

/* The leader of epoch 2 (node 0, ISR {0, 2}, learner 1), synced, both others heard at the EPOCH marker (101) */
static struct repl *join_leader(struct drv *d, struct repl_config *c)
{
    memset(d, 0, sizeof *d);
    fill(d, 1, 100, 1, 1);
    static struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    *c = config(0, d);
    c->isr_lag_ticks = 50;
    c->join_hold_ticks = 50;
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(c, &du);
    if (!r)
        return NULL;
    struct repl_msg m = meta_msg(0, 2, 0, 0x5, 0);
    repl_recv(r, &m);
    drain(r, d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, d, NULL);
    for (int from = 1; from <= 2; from++) {
        struct repl_msg k = {.type = REPL_M_ACK, .from = from, .to = 0, .epoch = 2, .leader = -1, .durable = 101,
                             .written = 101, .rid = from == 1 ? 22 : 33, .resync = 1};
        repl_recv(r, &k);
        drain(r, d, NULL);
    }
    return r;
}

/* One tick of writes that never pause: a record written and synced, member 2 at the leader's end, the learner at
 * `learner` (its durable and written end; 0: silent). Returns the ISR request this tick made, if any. */
static const struct repl_action *join_step_at(struct repl *r, struct drv *d, uint64_t learner, int li, int mi,
                                             uint64_t setv)
{
    uint64_t pos;
    repl_write(r, 1, &pos);
    drain(r, d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, d, NULL);
    struct repl_msg k2 = {.type = REPL_M_ACK, .from = mi, .to = 0, .epoch = 2, .leader = -1,
                          .durable = repl_written(r), .written = repl_written(r), .rid = 33, .set_version = setv};
    repl_recv(r, &k2);
    drain(r, d, NULL);
    if (learner) {
        struct repl_msg k1 = {.type = REPL_M_ACK, .from = li, .to = 0, .epoch = 2, .leader = -1, .durable = learner,
                              .written = learner, .rid = 22, .set_version = setv};
        repl_recv(r, &k1);
        drain(r, d, NULL);
        const struct repl_action *q = find_act(d, REPL_A_SEND, REPL_M_ISR_REQ);
        if (q)
            return q;
    }
    repl_tick(r);
    drain(r, d, NULL);
    return find_act(d, REPL_A_SEND, REPL_M_ISR_REQ);
}

static const struct repl_action *join_step(struct repl *r, struct drv *d, uint64_t learner)
{
    return join_step_at(r, d, learner, 1, 2, 0);
}

static void steady_join(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c);
    CHECK(r && repl_is_leader(r), "not leader");
    uint64_t wend[1024] = {0};
    const struct repl_action *q = NULL;
    int t, stalled = 0;
    uint64_t last_commit = 0;
    for (t = 0; t < 600 && !q; t++) {
        uint64_t learner = t >= 3 ? wend[t - 3] : 101; /* three ticks behind the leader, always */
        q = join_step(r, &d, learner);
        wend[t] = repl_written(r);
        if (q) {
            CHECK(q->msg.meta.isr == 0x7 && q->msg.meta.rid[1] == 22, "asked for ISR 0x%x", q->msg.meta.isr);
            CHECK(repl_commit(r) <= learner, "the learner was asked for at %" PRIu64 " below the commit point %" PRIu64,
                  learner, repl_commit(r));
        }
        stalled = repl_commit(r) == last_commit ? stalled + 1 : 0;
        last_commit = repl_commit(r);
        CHECK(stalled <= (int)c.join_hold_ticks, "the commit point stood still for %d ticks", stalled);
    }
    CHECK(q, "a learner three ticks behind writes that never pause did not join in %d ticks (isr_lag %u)", t,
          c.isr_lag_ticks);
    CHECK(t <= 2 * (int)c.isr_lag_ticks, "joined only after %d ticks (isr_lag %u)", t, c.isr_lag_ticks);
    repl_free(r);
}

static void join_giveup(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c);
    CHECK(r && repl_is_leader(r), "not leader");
    uint64_t wend[1024] = {0};
    int t, stalled = 0, held = 0, after = -1;
    uint64_t last_commit = 0;
    /* far behind (more than half isr_lag): never held for */
    for (t = 0; t < 150; t++) {
        uint64_t learner = t >= 40 ? wend[t - 40] : 101;
        CHECK(!join_step(r, &d, learner), "a learner 40 ticks behind was asked for");
        wend[t] = repl_written(r);
        CHECK(t < 2 || repl_commit(r) == repl_written(r), "the commit point was held for a learner 40 ticks behind");
    }
    /* always one short of the commit point: held for at most join_hold_ticks, then not again for 4 x isr_lag */
    int gaveup = 0;
    for (; t < 1000; t++) {
        uint64_t learner = repl_commit(r) - 1;
        CHECK(!join_step(r, &d, learner), "a learner short of the commit point was asked for");
        wend[t] = repl_written(r);
        if (repl_commit(r) == last_commit) {
            stalled++;
            held = 1;
        } else {
            if (held && stalled)
                after = t, gaveup++;
            stalled = 0;
        }
        last_commit = repl_commit(r);
        CHECK(stalled <= (int)c.join_hold_ticks + 1, "the commit point stood still for %d ticks (join_hold %u)", stalled,
              c.join_hold_ticks);
        CHECK(after < 0 || t == after || repl_commit(r) == repl_written(r) || t - after >= 4 * (int)c.isr_lag_ticks - 2,
              "held again %d ticks after giving up", t - after);
    }
    CHECK(held && gaveup >= 2, "the commit point was held %d times for a learner close behind", gaveup);
    repl_free(r);
}

/* An idle volume (r3d review L1): with nothing to send, a peer gets an empty APPEND only every idle_heartbeat_ticks,
 * but a commit point it has not been sent goes out at the next tick */
static void idle_heartbeat(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    c.heartbeat_ticks = 1;
    c.idle_heartbeat_ticks = 20;
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    struct repl_msg m = meta_msg(0, 2, 0, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    struct repl_msg k1 = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 2, .leader = -1, .durable = 101,
                          .written = 101, .rid = 22, .resync = 1};
    struct repl_msg k2 = {.type = REPL_M_ACK, .from = 2, .to = 0, .epoch = 2, .leader = -1, .durable = 101,
                          .written = 101, .rid = 33, .resync = 1};
    repl_recv(r, &k1);
    repl_recv(r, &k2);
    drain(r, &d, NULL);
    CHECK(repl_commit(r) == 101, "commit %" PRIu64 ", not 101", repl_commit(r));
    repl_tick(r);
    drain(r, &d, NULL);
    int news = 0;
    for (int i = 0; i < d.nact; i++)
        news += d.act[i].type == REPL_A_SEND && d.act[i].msg.type == REPL_M_APPEND && d.act[i].msg.commit == 101;
    CHECK(news == 2, "the new commit point went to %d peers at the next tick, not 2", news);
    int sent = 0;
    for (int t = 0; t < 200; t++) {
        repl_recv(r, &k1);
        repl_recv(r, &k2);
        repl_tick(r);
        drain(r, &d, NULL);
        for (int i = 0; i < d.nact; i++)
            sent += d.act[i].type == REPL_A_SEND && d.act[i].msg.type == REPL_M_APPEND;
    }
    CHECK(sent >= 18 && sent <= 22, "%d empty APPENDs to 2 peers in 200 idle ticks, not about 20", sent);
    repl_free(r);
}

static void round_cut(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 4); /* records 1-4, 5-8, ..., 97-100 of epoch 1, from a leader of epoch 1 */
    fill(&d, 101, 101, 3, 1);
    struct repl_epoch h[2] = {{.epoch = 1, .start = 1}, {.epoch = 3, .start = 101}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 3, .floor = 0, .log_end = 101, .hist = h, .nhist = 2};
    struct repl *r = repl_open(&c, &du);
    struct repl_msg m = meta_msg(1, 4, 0, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    /* The leader of epoch 4 has epochs 1 (to 50) and 2 (51..58), not 3: its largest epoch <= 3 is 2, ending at 58. */
    struct repl_msg an = {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 4, .leader = 0, .last_epoch = 2,
                          .written = 58, .prev_epoch = 3, .prev_pos = 101, .end = 1};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    const struct repl_action *t = find_act(&d, REPL_A_TRUNCATE, 0);
    CHECK(!repl_failed(r) && t && t->pos == 56, "the cut inside record 57-60 went to %" PRIu64, t ? t->pos : 0);
    /* Asked again with last epoch 1: the leader's epoch 1 ends at 50, inside 49-52: cut to 48, then it follows. */
    an = (struct repl_msg){.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 4, .leader = 0, .last_epoch = 1,
                           .written = 50, .prev_epoch = 1, .prev_pos = 56, .end = 2};
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(repl_written(r) == 48, "written %" PRIu64, repl_written(r));
    an.written = 48;
    an.prev_pos = 48;
    an.end = 3;
    repl_recv(r, &an);
    drain(r, &d, NULL);
    CHECK(find_act(&d, REPL_A_SEND, REPL_M_ACK) && repl_written(r) == 48, "does not follow at 48");
    repl_free(r);
}

/* The replay floor (design-cluster.md 5.4): the lowest log start of every member heard from in this epoch and the
 * leader's own; unknown (0) while a member has not ACKed in this epoch (MUT(39) took the members heard from alone), on
 * a node that is not leading, and again after a new epoch. A partition-class switch puts its T past every row at or
 * above the floor, so a floor too high lets a replica replay rows under a key the switch has moved. */
static void replay_floor(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(repl_replay_floor(r) == 0, "a node that is not leading has floor %" PRIu64, repl_replay_floor(r));
    struct repl_msg m = meta_msg(1, 2, 1, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    repl_log_start(r, 50);
    CHECK(repl_replay_floor(r) == 0, "floor %" PRIu64 " before any member ACKed", repl_replay_floor(r));
    struct repl_msg k2 = {.type = REPL_M_ACK, .from = 2, .to = 1, .epoch = 2, .leader = -1, .durable = 101,
                          .written = 101, .rid = 33, .resync = 1, .start = 30, .replay = 30};
    repl_recv(r, &k2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 0, "floor %" PRIu64 " while node 0 has not ACKed in this epoch",
          repl_replay_floor(r));
    struct repl_msg k0 = k2;
    k0.from = 0;
    k0.rid = 11;
    k0.start = k0.replay = 70;
    repl_recv(r, &k0);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 30, "floor %" PRIu64 ", not node 2's start 30", repl_replay_floor(r));
    /* node 2's ACK from before it held anything (an empty log: replay 0), reordered behind the one above: node 2 still
     * replays from 30 (MUT(58): the floor rose to the leader's start 50) */
    struct repl_msg stale = {.type = REPL_M_ACK, .from = 2, .to = 1, .epoch = 2, .leader = -1, .rid = 33, .start = 1};
    repl_recv(r, &stale);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 30, "floor %" PRIu64 " after a stale ACK of node 2, which replays from 30",
          repl_replay_floor(r));
    /* node 2 installed a snapshot under the same rid: its segments start at 20, below the 30 it said before, and the
     * floor follows it down (a rule that only raised a rid's replay start kept 30: sim_repl seed 46150) */
    struct repl_msg inst = k2;
    inst.start = inst.replay = 20;
    repl_recv(r, &inst);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 20, "floor %" PRIu64 " after node 2 installed segments from 20",
          repl_replay_floor(r));
    k2.start = k2.replay = 60; /* node 2 removed segments: the leader's own start is now the lowest */
    repl_recv(r, &k2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 50, "floor %" PRIu64 ", not the leader's start 50", repl_replay_floor(r));
    repl_log_start(r, 40); /* never lowered */
    CHECK(repl_replay_floor(r) == 50, "the leader's start went back: floor %" PRIu64, repl_replay_floor(r));
    /* A later epoch with this node leading again: what was heard in epoch 2 says nothing about epoch 3. */
    m = meta_msg(1, 3, 1, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader in epoch 3");
    CHECK(repl_replay_floor(r) == 0, "floor %" PRIu64 " in a new epoch before any ACK", repl_replay_floor(r));
    k0.epoch = k2.epoch = 3;
    repl_recv(r, &k0);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 0, "floor %" PRIu64 " in epoch 3 with node 2 unheard", repl_replay_floor(r));
    repl_recv(r, &k2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 50, "floor %" PRIu64 " in epoch 3", repl_replay_floor(r));
    repl_free(r);
}


/* An ACK from `from` in epoch 2 (durable = written = end), rid, replay start. */
static struct repl_msg ack_msg(int from, int to, uint64_t end, uint64_t rid, uint64_t replay, uint64_t setv)
{
    return (struct repl_msg){.type = REPL_M_ACK, .from = from, .to = to, .epoch = 2, .leader = -1, .durable = end,
                             .written = end, .rid = rid, .start = 1, .replay = replay, .set_version = setv};
}

static void peer_match(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c); /* node 0 leads epoch 2, ISR {0, 2} (rid 33); node 1 a learner; all at 101 */
    CHECK(r && repl_is_leader(r), "no leader");
    CHECK(repl_peer_match(r, 0) == repl_durable_end(r) && repl_peer_match(r, 0) == 101, "own index: %" PRIu64,
          repl_peer_match(r, 0));
    CHECK(repl_peer_match(r, 2) == 101, "ISR member 2: %" PRIu64, repl_peer_match(r, 2));
    CHECK(repl_peer_match(r, -1) == 0 && repl_peer_match(r, 3) == 0 && repl_peer_match(r, REPL_MAX_NODES - 1) == 0,
          "an index outside the set");
    /* node 2's disk is lost and its log copied again: a new instance acknowledges the whole log */
    struct repl_msg k = ack_msg(2, 0, 101, 99, 1, 0);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 0, "member 2 as another instance than the record's: %" PRIu64,
          repl_peer_match(r, 2));
    /* a stale ACK of the old instance 33 must not flip p->rid back */
    k = ack_msg(2, 0, 101, 33, 1, 0);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 0, "a stale ACK of old rid 33 restored peer match: %" PRIu64,
          repl_peer_match(r, 2));
    /* once the metadata record adopts the new instance 99, peer_match reads 101 */
    struct repl_msg m2 = meta_msg(0, 2, 0, 0x5, 0);
    m2.meta.isr_version = 8;
    m2.meta.rid[2] = 99;
    repl_recv(r, &m2);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 101, "member 2 as the record's new instance: %" PRIu64, repl_peer_match(r, 2));
    /* a follower answers 0 for everyone */
    struct repl_msg m = meta_msg(0, 3, 1, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    CHECK(!repl_is_leader(r) && repl_peer_match(r, 0) == 0 && repl_peer_match(r, 2) == 0, "a follower");
    repl_free(r);
}

static void reconfig(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c); /* node 0 leads epoch 2, ISR {0, 2}; node 1 a learner; all at 101 */
    CHECK(r && repl_is_leader(r), "no leader");
    /* written 110, durable 101 (the sync not back), node 2 has 108: the commit point waits for the leader */
    for (int i = 0; i < 9; i++) {
        uint64_t pos;
        repl_write(r, 1, &pos);
        if (i == 8) { /* its SENDs still queued: they name members by the old indexes */
            int same[3] = {0, 1, 2};
            CHECK(repl_reconfig(r, 3, 0, same) == REPL_E_PENDING, "a reconfig with actions still queued");
        }
        drain(r, &d, NULL);
    }
    struct repl_msg k = ack_msg(2, 0, 108, 33, 1, 0);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_commit(r) == 101, "commit %" PRIu64, repl_commit(r));
    int bad[3] = {0, 2, 2};
    CHECK(repl_reconfig(r, 3, 0, bad) == REPL_E_INVALID, "a map naming one member twice");
    int noself[2] = {1, 2};
    CHECK(repl_reconfig(r, 2, 0, noself) == REPL_E_INVALID, "the leader's own removal");
    int noisr[2] = {0, 1};
    CHECK(repl_reconfig(r, 2, 0, noisr) == REPL_E_INVALID, "removing ISR member 2");
    /* REMOVE node 1 (the learner): node 2 becomes index 1 */
    int map[2] = {0, 2};
    CHECK(repl_reconfig(r, 2, 0, map) == REPL_OK, "reconfig");
    repl_set_version(r, 1);
    struct repl_msg m = meta_msg(0, 2, 0, 0x3, 0);
    m.set_version = 1;
    m.meta.rid[1] = 33;
    m.meta.rid[2] = 0;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    for (int i = 0; i < 2; i++) { /* the sync in flight covers 102; the next one the rest */
        repl_synced(r, repl_written(r));
        drain(r, &d, NULL);
    }
    CHECK(repl_commit(r) == 108, "commit %" PRIu64 " after the shift: node 2's match did not move to index 1",
          repl_commit(r));
    /* an ACK of the old set version (node 2 under its old index) is dropped and counted */
    uint64_t dropped = repl_dropped(r);
    k = ack_msg(2, 0, 110, 33, 1, 0);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_dropped(r) == dropped + 1 && repl_commit(r) == 108, "an old-version ACK was taken (commit %" PRIu64
          ")", repl_commit(r));
    k = ack_msg(1, 0, 110, 33, 1, 1);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_commit(r) == 110, "commit %" PRIu64 " with node 2 at its new index", repl_commit(r));
    /* ADD a learner at index 2: it never ACKed, so the floor is unknown, and the removed node is not counted */
    int add[3] = {0, 1, -1};
    CHECK(repl_reconfig(r, 3, 0, add) == REPL_OK, "add");
    repl_set_version(r, 2);
    CHECK(repl_replay_floor(r) == 0, "floor %" PRIu64 " with a learner unheard", repl_replay_floor(r));
    k = ack_msg(2, 0, 0, 44, 0, 2);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    k = ack_msg(1, 0, 110, 33, 1, 2);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 1, "floor %" PRIu64, repl_replay_floor(r));
    uint64_t pos;
    CHECK(repl_write(r, 1, &pos) == REPL_OK, "write");
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    k = ack_msg(1, 0, repl_written(r), 33, 1, 2);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_commit(r) == repl_written(r), "commit %" PRIu64 " held by a removed member (written %" PRIu64 ")",
          repl_commit(r), repl_written(r));
    repl_free(r);
}

static void leave(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c); /* ISR {0, 2}, node 1 a caught-up learner */
    CHECK(r, "no leader");
    CHECK(repl_leave(r, 0, 1) == REPL_E_INVALID, "the leader leaving");
    /* the move: node 1 (B) joins, node 2 (A) leaves; the ISR had 2 members before B joined */
    CHECK(repl_leave(r, 2, 2) == REPL_OK && repl_leaving(r) == 2, "leave");
    const struct repl_action *q = NULL;
    for (int t = 0; t < 40 && !q; t++) {
        struct repl_msg k = ack_msg(2, 0, 101, 33, 1, 0);
        repl_recv(r, &k);
        drain(r, &d, NULL);
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
        CHECK(!q || (q->msg.meta.isr & 0x4), "asked for ISR %#x, below min_isr 2 without node 2", q->msg.meta.isr);
    }
    CHECK(q && q->msg.meta.isr == 0x7 && repl_isr_why(r) == REPL_WHY_JOIN, "B was not asked for");
    struct repl_msg m = meta_msg(0, 2, 0, 0x7, 0);
    m.meta.isr_version = 8;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    int asks = 0;
    for (int t = 0; t < 5; t++) { /* less than resend_ticks: asked once */
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
        if (q) {
            asks++;
            CHECK(q->msg.meta.isr == 0x3 && repl_isr_why(r) == REPL_WHY_LEAVE, "asked for %#x", q->msg.meta.isr);
        }
    }
    CHECK(asks == 1, "the shrink asked %d times", asks);
    m.meta.isr_version = 9;
    m.meta.isr = 0x3;
    m.meta.rid[2] = 0;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    for (int t = 0; t < 60; t++) { /* node 2 keeps up: it is never asked for again */
        struct repl_msg k = ack_msg(2, 0, repl_written(r), 33, 1, 0);
        repl_recv(r, &k);
        k = ack_msg(1, 0, repl_written(r), 22, 1, 0);
        repl_recv(r, &k);
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
        CHECK(!q, "the leaving member asked for again (ISR %#x)", q->msg.meta.isr);
    }
    CHECK(repl_leave(r, -1, 0) == REPL_OK && repl_leaving(r) == -1, "cancel");
    for (int t = 0; t < 5 && !q; t++) {
        struct repl_msg k = ack_msg(2, 0, repl_written(r), 33, 1, 0);
        repl_recv(r, &k);
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    }
    CHECK(q && q->msg.meta.isr == 0x7, "after a cancel node 2 does not rejoin");
    repl_free(r);
}

static void target(void)
{
    struct repl_vmeta m0 = {.epoch = 6, .isr_version = 9, .leader = 0, .isr = 0x7, .rid = {11, 22, 33}};
    struct repl_fence_reply rep[3] = {{.node = 0, .durable = 400, .rid = 11}, {.node = 1, .durable = 500, .rid = 22},
                                      {.node = 2, .durable = 500, .rid = 33}};
    struct repl_vmeta m = m0;
    CHECK(repl_meta_promote_to(&m, 6, 9, 0x7, rep, 3, 2, 500) == REPL_OK && m.leader == 2 && m.epoch == 7,
          "the target with the same durable end as a lower index: leader %d", m.leader);
    CHECK(m.isr == 0x7 && m.unanswered == 0, "isr %#x unanswered %#x: every member answered", m.isr, m.unanswered);
    m = m0;
    CHECK(repl_meta_promote_to(&m, 6, 9, 0x7, rep, 3, -1, 0) == REPL_OK && m.leader == 1, "no target: leader %d",
          m.leader);
    m = m0;
    CHECK(repl_meta_promote_to(&m, 6, 9, 0x7, rep, 3, 2, 501) == REPL_E_NONE && m.epoch == 6 && m.leader == 0,
          "a target below the drained end was promoted");
    rep[2].rid = 99;
    m = m0;
    CHECK(repl_meta_promote_to(&m, 6, 9, 0x7, rep, 3, 2, 0) == REPL_E_NONE, "a target of another instance");
    m = m0;
    CHECK(repl_meta_promote_to(&m, 6, 9, 0x7, rep, 2, 2, 0) == REPL_E_NONE, "a target that did not answer");
}

static void replay(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    /* a follower's ACK: replay start never below the log start, never lowered; an empty log reports 0 */
    struct repl *f = follower(&d, 100, 100, 1, 1);
    CHECK(f, "no follower");
    repl_log_start(f, 20);
    repl_replay_start(f, 10);
    struct repl_msg ap = {.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 3, .leader = 0,
                          .prev_pos = repl_written(f), .prev_epoch = 1, .commit = 0};
    repl_recv(f, &ap);
    drain(f, &d, &ap);
    const struct repl_action *k = find_act(&d, REPL_A_SEND, REPL_M_ACK);
    CHECK(k && k->msg.replay == 20 && k->msg.start == 20, "ACK replay %" PRIu64, k ? k->msg.replay : 0);
    repl_replay_start(f, 60);
    repl_replay_start(f, 30);
    repl_recv(f, &ap);
    drain(f, &d, &ap);
    k = find_act(&d, REPL_A_SEND, REPL_M_ACK);
    CHECK(k && k->msg.replay == 60, "ACK replay %" PRIu64 " (lowered?)", k ? k->msg.replay : 0);
    repl_free(f);
    /* the leader's floor: its own replay start, each member's, an empty log by what was sent to it */
    struct repl_config c;
    struct repl *r = join_leader(&d, &c);
    CHECK(r, "no leader");
    d.start = 40;
    repl_log_start(r, 40);
    repl_replay_start(r, 90);
    struct repl_msg a1 = ack_msg(1, 0, 101, 22, 70, 0), a2 = ack_msg(2, 0, 101, 33, 80, 0);
    repl_recv(r, &a1);
    repl_recv(r, &a2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 70, "floor %" PRIu64 ", not node 1's replay start 70", repl_replay_floor(r));
    a1.replay = 95;
    repl_recv(r, &a1);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 80, "floor %" PRIu64 ", not node 2's replay start 80", repl_replay_floor(r));
    a2.replay = 99;
    repl_recv(r, &a2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 90, "floor %" PRIu64 ", not the leader's own 90", repl_replay_floor(r));
    /* node 1 lost its disk: empty, sent from 40 (the leader's log start) on */
    a1 = ack_msg(1, 0, 0, 23, 0, 0);
    a1.resync = 1;
    repl_recv(r, &a1);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 40, "floor %" PRIu64 " with an empty member: the leader sends it from 40",
          repl_replay_floor(r));
    repl_free(r);
}

static void snapshot(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c);
    CHECK(r, "no leader");
    d.start = 50; /* positions below 50 went into parts */
    repl_log_start(r, 50);
    repl_replay_start(r, 60);
    /* a fresh learner (node 1 reset: empty log, a new rid) */
    struct repl_msg k = ack_msg(1, 0, 0, 23, 0, 0);
    k.resync = 1;
    repl_recv(r, &k);
    drain(r, &d, NULL);
    const struct repl_action *s = find_act(&d, REPL_A_SEND, REPL_M_SNAPSHOT);
    CHECK(s && s->msg.written == 0 && s->msg.to == 1, "no snapshot notice for an empty learner");
    k = ack_msg(1, 0, 0, 23, 0, 0);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) <= 50, "floor %" PRIu64 " above what a snapshot may bring", repl_replay_floor(r));
    /* the learner's side: asked, a snapshot wanted only above its log end */
    static struct drv fd;
    memset(&fd, 0, sizeof fd);
    struct repl *f = follower(&fd, 10, 10, 1, 1);
    CHECK(f, "no follower");
    struct repl_msg n = {.type = REPL_M_SNAPSHOT, .from = 0, .to = 1, .epoch = 3, .leader = -1, .written = 10};
    repl_recv(f, &n);
    drain(f, &fd, NULL);
    CHECK(find_act(&fd, REPL_A_SNAPSHOT, 0), "no snapshot asked for");
    CHECK(!repl_snapshot_wanted(f, 10) && !repl_snapshot_wanted(f, 5), "a snapshot at or below the log end wanted");
    CHECK(repl_snapshot_wanted(f, 101), "the snapshot at 101 not wanted");
    repl_free(f);
    /* installed: P = 101 (the leader's commit point), segments from S = 60 */
    memset(&fd, 0, sizeof fd);
    fill(&fd, 60, 101, 2, 1);
    fd.start = 60;
    struct repl_epoch h[1] = {{.epoch = 2, .start = 60}};
    struct repl_config fc = config(1, &fd);
    struct repl_durable du = {.rid = 23, .fence = 2, .floor = 101, .log_start = 60, .log_end = 101, .hist = h,
                              .nhist = 1};
    f = repl_open(&fc, &du);
    CHECK(f && !repl_failed(f) && repl_applied(f) == 101, "the install's open (log start 60, end and floor 101)");
    repl_free(f);
    k = ack_msg(1, 0, 101, 23, 60, 0);
    k.resync = 1;
    repl_recv(r, &k);
    drain(r, &d, NULL);
    for (int t = 0; t < 30; t++) {
        repl_tick(r);
        drain(r, &d, NULL);
        CHECK(!find_act(&d, REPL_A_SEND, REPL_M_SNAPSHOT), "a notice after the ACK showed 101");
    }
    k = ack_msg(1, 0, repl_written(r), 23, 60, 0);
    repl_recv(r, &k);
    k = ack_msg(2, 0, repl_written(r), 33, 70, 0);
    repl_recv(r, &k);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 60, "floor %" PRIu64 " after the install", repl_replay_floor(r));
    repl_free(r);
}

/* A leader whose own shrink of node 1 was applied, and then node 1's REMOVE, before it adopted the shrunk record: it
 * takes the change but moves no commit point and asks nothing until it has adopted a record of the new set version
 * (it may be deposed, and the next epoch fenced node 1: counting without it would commit alone). An older copy of the
 * record is dropped and counted (the driver's contract). */
static void hold(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    c.isr_lag_ticks = 50;
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    struct repl_msg m = meta_msg(0, 2, 0, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    struct repl_msg k2 = ack_msg(2, 0, 101, 33, 1, 0);
    k2.resync = 1;
    const struct repl_action *q = NULL;
    for (int t = 0; t < 3 * (int)c.isr_lag_ticks && !q; t++) { /* node 1 silent: dropped */
        repl_recv(r, &k2);
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    }
    CHECK(q && q->msg.meta.isr == 0x5, "no shrink of node 1");
    uint64_t c0 = repl_commit(r);
    int map[2] = {0, 2}; /* REMOVE node 1: node 2 becomes index 1 */
    CHECK(repl_reconfig(r, 2, 0, map) == REPL_OK, "the reconfig with the shrink pending");
    repl_set_version(r, 1);
    for (int t = 0; t < 3 * (int)c.resend_ticks; t++) {
        uint64_t pos;
        CHECK(repl_write(r, 1, &pos) == REPL_OK, "write");
        drain(r, &d, NULL);
        repl_synced(r, repl_written(r));
        drain(r, &d, NULL);
        struct repl_msg k = ack_msg(1, 0, repl_written(r), 33, 1, 1);
        repl_recv(r, &k);
        repl_tick(r);
        drain(r, &d, NULL);
        CHECK(repl_commit(r) == c0, "commit %" PRIu64 " before a record of the new set version", repl_commit(r));
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
        CHECK(!q, "asked for ISR %#x before a record of the new set version", q->msg.meta.isr);
    }
    /* an older copy (set version 0; its isr 0x3 named nodes 0 and 1) is dropped, counted, and holds nothing up */
    uint64_t dm = repl_dropped_meta(r);
    m = meta_msg(0, 2, 0, 0x3, 0);
    m.meta.isr_version = 8;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    CHECK(repl_dropped_meta(r) == dm + 1, "a record of another set version was not dropped");
    CHECK(repl_commit(r) == c0, "commit %" PRIu64 " after a record of the old set version", repl_commit(r));
    /* the record of the new set version, its ISR version not above the one held: adopted, and the commit moves */
    m = meta_msg(0, 2, 0, 0x3, 0);
    m.set_version = 1;
    m.meta.rid[1] = 33;
    m.meta.rid[2] = 0;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    CHECK(repl_commit(r) == repl_written(r), "commit %" PRIu64 " after the record of the new set version (written %"
          PRIu64 ")", repl_commit(r), repl_written(r));
    repl_free(r);
}

/* A set change that moves an unanswered member (meta.unanswered: kept by the promotion without a fence reply) to
 * another index: the grace rule drops it there, within unanswered_ticks, not the member now at its old index. */
static void unans_remap(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(1, &d);
    struct repl_durable du = {.rid = 22, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    struct repl_msg m = meta_msg(1, 2, 1, 0x7, 0x1); /* node 0, the old leader, did not answer the fence */
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    int swap[3] = {2, 1, 0}; /* nodes 0 and 2 change places */
    CHECK(repl_reconfig(r, 3, 1, swap) == REPL_OK, "reconfig");
    repl_set_version(r, 1);
    struct repl_msg k = {.type = REPL_M_ACK, .from = 0, .to = 1, .epoch = 2, .leader = -1, .durable = 101,
                         .written = 101, .rid = 33, .resync = 1, .set_version = 1};
    const struct repl_action *q = NULL;
    int t;
    for (t = 1; t <= 100 && !q; t++) {
        repl_recv(r, &k); /* node 2, now index 0, keeps up; node 0, now index 2, is silent */
        drain(r, &d, NULL);
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    }
    CHECK(q && q->msg.meta.isr == 0x3, "the silent old leader (index 2) was not dropped within 100 ticks");
    CHECK(t <= (int)c.unanswered_ticks + 2, "dropped only after %d ticks (unanswered_ticks %u)", t, c.unanswered_ticks);
    repl_free(r);
}

/* A set change during a join that moves the learner to another index: the commit point is held for it there. */
static void join_remap(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c);
    CHECK(r && repl_is_leader(r), "not leader");
    uint64_t wend[1024] = {0}, learner = 101;
    char desc[2048];
    int t, held = 0;
    for (t = 0; t < 600 && !held; t++) {
        learner = t >= 3 ? wend[t - 3] : 101; /* three ticks behind the leader */
        CHECK(!join_step(r, &d, learner), "the learner was asked for before its join began");
        wend[t] = repl_written(r);
        repl_describe(r, desc, sizeof desc);
        held = strstr(desc, "held for learner 1 ") != NULL;
    }
    CHECK(held, "the commit point was never held for the learner");
    int swap[3] = {0, 2, 1}; /* the learner moves to index 2, member 2 to index 1 */
    CHECK(repl_reconfig(r, 3, 0, swap) == REPL_OK, "reconfig");
    repl_set_version(r, 1);
    repl_describe(r, desc, sizeof desc);
    CHECK(strstr(desc, "held for learner 2 "), "the join did not move with the learner: %s", desc);
    uint64_t c0 = repl_commit(r); /* the hold began this tick, above the learner: it keeps the commit point there */
    join_step_at(r, &d, learner, 2, 1, 1); /* the learner stays where it was, member 2 follows the leader */
    CHECK(repl_commit(r) == c0, "commit %" PRIu64 " moved past %" PRIu64 " with the joining learner at %" PRIu64
          " (written %" PRIu64 ")", repl_commit(r), c0, learner, repl_written(r));
    repl_free(r);
}

/* A pending ISR request that adds a member the set change removes was never applied: it goes, and the leader asks
 * again in the new set (here: to drop a member gone silent) without waiting for a later record. */
static void busy_drop(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c); /* ISR {0, 2}, learner 1 caught up at 101 */
    CHECK(r && repl_is_leader(r), "not leader");
    const struct repl_action *q = NULL;
    for (int t = 0; t < 100 && !q; t++)
        q = join_step(r, &d, repl_written(r));
    CHECK(q && q->msg.meta.isr == 0x7, "the learner was not asked for");
    int map[2] = {0, 2}; /* REMOVE the learner before the add was applied */
    CHECK(repl_reconfig(r, 2, 0, map) == REPL_OK, "reconfig");
    repl_set_version(r, 1);
    struct repl_msg m = meta_msg(0, 2, 0, 0x3, 0);
    m.set_version = 1;
    m.meta.rid[1] = 33;
    m.meta.rid[2] = 0;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    q = NULL;
    int t;
    for (t = 0; t < 3 * (int)c.isr_lag_ticks && !q; t++) { /* node 2 (index 1) goes silent */
        uint64_t pos;
        repl_write(r, 1, &pos);
        drain(r, &d, NULL);
        repl_synced(r, repl_written(r));
        repl_tick(r);
        drain(r, &d, NULL);
        q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
        if (q && q->msg.meta.isr != 0x1)
            q = NULL;
    }
    CHECK(q, "a silent member was not dropped in %d ticks: the removed learner's add still pending", t);
    repl_free(r);
}

/* A stale ACK with a higher nonzero replay start sent before a snapshot install lowered it (m->durable < p->match)
 * must not overwrite p->replay, p->acked, or p->next (MUT(65)). Also covers a pre-install ACK whose unsynced written
 * end exceeded the install cut after a crash. */
static void stale_replay(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 200, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 200, .log_start = 1, .log_end = 200, .hist = h,
                              .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "open");
    struct repl_msg m = meta_msg(0, 2, 0, 0x5, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    d.start = 80;
    repl_log_start(r, 80);
    repl_replay_start(r, 80);
    /* node 1 (learner) and node 2 (ISR) start with replay 65 (and node 2 also had an unsynced ACK at written 180) */
    struct repl_msg k1 = ack_msg(1, 0, 150, 22, 80, 0);
    struct repl_msg k2_pre = ack_msg(2, 0, 70, 33, 65, 0);
    struct repl_msg k2_unsynced = {.type = REPL_M_ACK, .from = 2, .to = 0, .epoch = 2, .leader = -1, .durable = 50,
                                   .written = 180, .rid = 33, .start = 70, .replay = 70};
    repl_recv(r, &k1);
    repl_recv(r, &k2_pre);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 65, "floor %" PRIu64 ", not 65", repl_replay_floor(r));
    /* node 2 installs a snapshot at P = 150 with segments from 40 (lowering its replay start from 65 to 40) */
    struct repl_msg k2_post = ack_msg(2, 0, 150, 33, 40, 0);
    k2_post.start = 40;
    k2_post.resync = 1;
    repl_recv(r, &k2_post);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 40, "floor %" PRIu64 " after install to 40", repl_replay_floor(r));
    /* delayed pre-install ACKs arrive after the post-install ACK: must be ignored */
    k2_pre.resync = 1;
    repl_recv(r, &k2_pre);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 40, "floor %" PRIu64 " after stale pre-install ACK (replay 65)",
          repl_replay_floor(r));
    repl_recv(r, &k2_unsynced);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 40, "floor %" PRIu64 " after stale pre-install unsynced ACK (replay 70)",
          repl_replay_floor(r));
    /* p->next was not rewound to 71 (which is below r->start 80 and would trigger a bogus SNAPSHOT) */
    uint64_t pos;
    repl_write(r, 1, &pos);
    drain(r, &d, NULL);
    CHECK(!find_act(&d, REPL_A_SEND, REPL_M_SNAPSHOT), "a bogus SNAPSHOT after stale pre-install resync ACK");
    const struct repl_action *ap = NULL;
    for (int i = 0; i < d.nact; i++)
        if (d.act[i].type == REPL_A_SEND && d.act[i].msg.type == REPL_M_APPEND && d.act[i].msg.to == 2) {
            ap = &d.act[i];
            break;
        }
    CHECK(ap && ap->msg.prev_pos == 166, "node 2 append prev_pos %" PRIu64 ", not 166", ap ? ap->msg.prev_pos : 0);
    repl_free(r);
}

/* A stale ACK carrying an old rid must not flip p->rid back, revive old match, or raise p->replay (MUT(66)). */
static void stale_rid(void)
{
    static struct drv d;
    struct repl_config c;
    struct repl *r = join_leader(&d, &c); /* node 0 leads epoch 2, ISR {0, 2} (rid 33), learner 1 (rid 22), all at 101 */
    CHECK(r && repl_is_leader(r), "not leader");
    repl_log_start(r, 50);
    repl_replay_start(r, 50);
    /* 1. ISR member 2 (init_rid 33) wipes to rid 99; stale ACK of rid 33 arrives while 33 is still in meta.isr */
    struct repl_msg wipe2 = ack_msg(2, 0, 0, 99, 0, 0);
    wipe2.resync = 1;
    repl_recv(r, &wipe2);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 0, "wiped node 2 peer_match %" PRIu64, repl_peer_match(r, 2));
    struct repl_msg stale2 = ack_msg(2, 0, 101, 33, 80, 0);
    stale2.start = 80;
    repl_recv(r, &stale2);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 0, "stale ACK of rid 33 restored peer_match to %" PRIu64, repl_peer_match(r, 2));
    uint64_t pos;
    repl_write(r, 1, &pos);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_commit(r) == 101, "stale ACK of rid 33 let commit advance to %" PRIu64, repl_commit(r));
    CHECK(repl_replay_floor(r) <= 50, "stale ACK of rid 33 raised replay floor to %" PRIu64, repl_replay_floor(r));
    /* 2. Even after node 2 is dropped from meta.isr (meta.rid[2] becomes 0), stale init_rid 33 is still refused */
    struct repl_msg drop2 = meta_msg(0, 2, 0, 0x1, 0);
    drop2.meta.isr_version = 8;
    drop2.meta.rid[2] = 0;
    repl_recv(r, &drop2);
    drain(r, &d, NULL);
    stale2.durable = stale2.written = 102;
    stale2.replay = stale2.start = 90;
    repl_recv(r, &stale2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) <= 50, "stale rid 33 raised replay floor after ISR drop to %" PRIu64,
          repl_replay_floor(r));
    repl_tick(r);
    drain(r, &d, NULL);
    const struct repl_action *q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    CHECK(!q, "stale rid 33 triggered ISR rejoin request (rid %" PRIu64 ")", q ? q->msg.meta.rid[2] : 0);
    repl_free(r);

    /* 3. Mid-epoch ISR join deadlock regression: learner 2 (init_rid == 0) wipes 22 -> 23, joins the ISR via
     * adopt_meta(), then receives a delayed ACK(22). Live ACK(23) must continue to be accepted both before and after
     * an ISR eviction, never permanently silencing node 2. */
    r = join_leader(&d, &c);
    CHECK(r && repl_is_leader(r), "not leader (2)");
    int map1[2] = {0, 2};
    CHECK(repl_reconfig(r, 2, 0, map1) == REPL_OK, "reconfig drop");
    repl_set_version(r, 1);
    int map2[3] = {0, 1, -1};
    CHECK(repl_reconfig(r, 3, 0, map2) == REPL_OK, "reconfig add");
    repl_set_version(r, 2);
    struct repl_msg m = meta_msg(0, 2, 0, 0x3, 0);
    m.set_version = 2;
    m.meta.isr_version = 8;
    m.meta.rid[1] = 33;
    m.meta.rid[2] = 0;
    repl_recv(r, &m);
    drain(r, &d, NULL);
    struct repl_msg r23 = ack_msg(2, 0, 101, 23, 20, 2);
    struct repl_msg r22 = ack_msg(2, 0, 101, 22, 20, 2);
    repl_recv(r, &r23);
    repl_recv(r, &r23);
    drain(r, &d, NULL);
    /* Promote learner 2 (rid 23) into the ISR mid-epoch */
    struct repl_msg join2 = m;
    join2.meta.isr = 0x7;
    join2.meta.isr_version = 9;
    join2.meta.rid[2] = 23;
    repl_recv(r, &join2);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 101, "joined node 2 peer_match %" PRIu64, repl_peer_match(r, 2));
    /* Single delayed ACK of unseen earlier rid 22 arrives; even if evicted, live rid 23 must recover and rejoin */
    repl_recv(r, &r22);
    drain(r, &d, NULL);
    repl_tick(r);
    drain(r, &d, NULL);
    q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    CHECK(q && q->msg.meta.isr == 0x3, "expected ISR drop of node 2 after delayed rid 22");
    struct repl_msg evict2 = join2;
    evict2.meta.isr = 0x3;
    evict2.meta.isr_version = 10;
    evict2.meta.rid[2] = 0;
    repl_recv(r, &evict2);
    drain(r, &d, NULL);
    repl_recv(r, &r23);
    drain(r, &d, NULL);
    repl_tick(r);
    drain(r, &d, NULL);
    q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    CHECK(q && q->msg.meta.isr == 0x7 && q->msg.meta.rid[2] == 23,
          "live rid 23 was permanently silenced after mid-epoch ISR join + delayed rid 22");
    repl_free(r);

    /* 4. Duplicated/delayed old-rid deadlock regression on a learner: learner 2 (init_rid == 0) wipes 22 -> 23,
     * sends live ACKs, then receives TWO delayed/duplicated ACK(22) packets back-to-back. Live ACK(23) must still
     * recover and trigger ISR rejoin. */
    r = join_leader(&d, &c);
    CHECK(r && repl_is_leader(r), "not leader (3)");
    CHECK(repl_reconfig(r, 2, 0, map1) == REPL_OK, "reconfig drop (2)");
    repl_set_version(r, 1);
    CHECK(repl_reconfig(r, 3, 0, map2) == REPL_OK, "reconfig add (2)");
    repl_set_version(r, 2);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_recv(r, &r23);
    repl_recv(r, &r23);
    repl_recv(r, &r22); /* two delayed/duplicated ACKs of unseen rid 22 back-to-back */
    repl_recv(r, &r22);
    repl_recv(r, &r23); /* live rid 23 keeps sending and must recover */
    repl_recv(r, &r23);
    drain(r, &d, NULL);
    repl_tick(r);
    drain(r, &d, NULL);
    q = find_act(&d, REPL_A_SEND, REPL_M_ISR_REQ);
    CHECK(q && q->msg.meta.rid[2] == 23, "live learner rid 23 was deadlocked by two delayed/duplicated stale ACKs");
    repl_free(r);
}

/* A leader ignores an ACK from an older epoch even when its durable end fits within r->written (MUT(8)). */
static void stale_epoch(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 100, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "open");
    struct repl_msg m = meta_msg(0, 2, 0, 0x5, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");
    struct repl_msg old = ack_msg(2, 0, 101, 33, 1, 0);
    old.epoch = 1;
    repl_recv(r, &old);
    drain(r, &d, NULL);
    CHECK(repl_peer_match(r, 2) == 0, "epoch-1 ACK set peer_match to %" PRIu64, repl_peer_match(r, 2));
    CHECK(repl_commit(r) == 100, "epoch-1 ACK moved commit to %" PRIu64, repl_commit(r));
    CHECK(repl_replay_floor(r) == 0, "epoch-1 ACK made node 2 heard (floor %" PRIu64 ")", repl_replay_floor(r));
    repl_free(r);
}

/* repl_debug must never underflow cap - pos or return a length >= cap when truncated (SEC-03). */
static void debug_trunc(void)
{
    static struct drv d;
    memset(&d, 0, sizeof d);
    fill(&d, 1, 100, 1, 1);
    struct repl_epoch h[1] = {{.epoch = 1, .start = 1}};
    struct repl_config c = config(0, &d);
    struct repl_durable du = {.rid = 11, .fence = 1, .floor = 100, .log_start = 1, .log_end = 100, .hist = h, .nhist = 1};
    struct repl *r = repl_open(&c, &du);
    CHECK(r && !repl_failed(r), "open");
    struct repl_msg m = meta_msg(0, 2, 0, 0x7, 0);
    repl_recv(r, &m);
    drain(r, &d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, &d, NULL);
    CHECK(repl_is_leader(r), "not leader");

    static const unsigned long caps[] = {0, 1, 2, 16, 64, 128, 180, 256, 1024};
    for (size_t i = 0; i < sizeof caps / sizeof caps[0]; i++) {
        unsigned long cap = caps[i];
        char raw[1040];
        memset(raw, 0x5a, sizeof raw);
        char *buf = raw + 8;
        unsigned long n = repl_debug(r, buf, cap);
        for (int k = 0; k < 8; k++)
            CHECK((unsigned char)raw[k] == 0x5a, "cap %lu: underflow before buffer", cap);
        for (size_t k = 8 + cap; k < sizeof raw; k++)
            CHECK((unsigned char)raw[k] == 0x5a, "cap %lu: overflow past buffer at %zu", cap, k - 8);
        if (cap == 0) {
            CHECK(n == 0, "cap 0 returned %lu", n);
        } else {
            CHECK(n < cap, "cap %lu returned %lu >= cap", cap, n);
            CHECK(buf[n] == '\0' && strlen(buf) == n, "cap %lu: returned %lu, strlen %zu", cap, n, strlen(buf));
        }
    }
    repl_free(r);
}

int main(int argc, char **argv)
{
    th_watchdog("test_repl", 1200);
    uint64_t mutant = 0, mutant2 = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            unsigned long b = strtoul(argv[++i], NULL, 10);
            if (b == 0 || b > 127)
                return 2;
            if (b < 64)
                mutant |= 1ull << b;
            else
                mutant2 |= 1ull << (b - 64);
        } else {
            fprintf(stderr, "usage: test_repl [--mutant N]\n");
            return 2;
        }
    }
#ifdef REPL_MUTANTS
    repl_mutants = mutant;
    repl_mutants2 = mutant2;
#else
    if (mutant || mutant2) {
        fprintf(stderr, "test_repl: --mutant needs the build with -DREPL_MUTANTS (test_repl_mut)\n");
        return 2;
    }
#endif
    trim();
    ack_bound();
    open_damage();
    follower_damage();
    below_start();
    open_hold();
    cut_applied();
    sync_cut();
    promote();
    unanswered();
    idle_shrink();
    idle_heartbeat();
    round_cut();
    replay_floor();
    steady_join();
    join_giveup();
    reconfig();
    peer_match();
    hold();
    unans_remap();
    join_remap();
    busy_drop();
    leave();
    target();
    replay();
    snapshot();
    stale_replay();
    stale_rid();
    stale_epoch();
    debug_trunc();
    if (failures) {
        fprintf(stderr, "test_repl: %d scenario(s) failed\n", failures);
        return 1;
    }
    printf("test_repl: ok\n");
    return 0;
}
