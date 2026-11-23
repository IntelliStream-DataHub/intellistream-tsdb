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
static const struct repl_action *join_step(struct repl *r, struct drv *d, uint64_t learner)
{
    uint64_t pos;
    repl_write(r, 1, &pos);
    drain(r, d, NULL);
    repl_synced(r, repl_written(r));
    drain(r, d, NULL);
    struct repl_msg k2 = {.type = REPL_M_ACK, .from = 2, .to = 0, .epoch = 2, .leader = -1,
                          .durable = repl_written(r), .written = repl_written(r), .rid = 33};
    repl_recv(r, &k2);
    drain(r, d, NULL);
    if (learner) {
        struct repl_msg k1 = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 2, .leader = -1, .durable = learner,
                              .written = learner, .rid = 22};
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
                          .written = 101, .rid = 33, .resync = 1, .start = 30};
    repl_recv(r, &k2);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 0, "floor %" PRIu64 " while node 0 has not ACKed in this epoch",
          repl_replay_floor(r));
    struct repl_msg k0 = k2;
    k0.from = 0;
    k0.rid = 11;
    k0.start = 70;
    repl_recv(r, &k0);
    drain(r, &d, NULL);
    CHECK(repl_replay_floor(r) == 30, "floor %" PRIu64 ", not node 2's start 30", repl_replay_floor(r));
    k2.start = 60; /* node 2 removed segments: the leader's own start is now the lowest */
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

int main(int argc, char **argv)
{
    th_watchdog("test_repl", 1200);
    uint64_t mutant = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            unsigned long b = strtoul(argv[++i], NULL, 10);
            if (b == 0 || b > 63)
                return 2;
            mutant |= 1ull << b;
        } else {
            fprintf(stderr, "usage: test_repl [--mutant N]\n");
            return 2;
        }
    }
#ifdef REPL_MUTANTS
    repl_mutants = mutant;
#else
    if (mutant) {
        fprintf(stderr, "test_repl: --mutant needs the build with -DREPL_MUTANTS (test_repl_mut)\n");
        return 2;
    }
#endif
    trim();
    open_damage();
    follower_damage();
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
    if (failures) {
        fprintf(stderr, "test_repl: %d scenario(s) failed\n", failures);
        return 1;
    }
    printf("test_repl: ok\n");
    return 0;
}
