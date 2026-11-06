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
 *   round       a cut can fall inside a record: a replica whose last epoch the leader lacks cuts at the end of the
 *               leader's epoch before it, inside an older epoch's record that only the replica has.
 */
#include "repl.h"

#include <inttypes.h>
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

int main(int argc, char **argv)
{
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
    cut_applied();
    sync_cut();
    promote();
    unanswered();
    round_cut();
    if (failures) {
        fprintf(stderr, "test_repl: %d scenario(s) failed\n", failures);
        return 1;
    }
    printf("test_repl: ok\n");
    return 0;
}
