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
#include "raft/raft.h"
#include "raft/raft_log.h"
#include "raft/raft_sim.h"
#include "raft/raft_wire.h"

#include <errno.h>
#include <stdarg.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

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
}

static void test_state_machine(void)
{
    struct meta_state m, prev;
    struct meta_entry e;
    struct meta_member mb;
    struct meta_settings s;
    uint8_t uuid[16] = {1, 2, 3};
    int res[8];
    meta_init(&m);
    meta_entry_init(&e);
    meta_settings_default(&s);

    /* nothing before INIT */
    member(&mb, 2);
    meta_op_member_add(&e, &mb);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STATE && m.nmembers == 0);

    member(&mb, 1);
    meta_op_init(&e, uuid, &s, &mb);
    meta_op_init(&e, uuid, &s, &mb); /* a second INIT in the same entry is refused, the first stands */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_STATE);
    CHECK(m.initialised && m.nmembers == 1 && m.next_member == 2 && m.applied == 2);

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

    /* a volume: epoch 1, its first replica leads, the ISR is all of them */
    uint32_t nodes[3] = {2, 4, 1};
    meta_op_volume_add(&e, 0, "v0", uuid, 1, nodes, 3);
    meta_op_volume_add(&e, 1, "v0", uuid, 1, nodes, 3); /* name taken */
    uint32_t bad[3] = {2, 2, 1};
    meta_op_volume_add(&e, 2, "v2", uuid, 1, bad, 3); /* one server twice */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_EXISTS &&
          res[2] == META_ERR_INVALID);
    const struct meta_volume *v = meta_volume(&m, 0);
    CHECK(v && v->epoch == 1 && v->leader == 2 && v->nisr == 3 && v->isr[0] == 1 && v->isr[2] == 4);

    /* the ISR shrinks at the current epoch and version only; a stale one changes nothing */
    meta_copy(&prev, &m);
    uint32_t isr2[2] = {2, 4};
    meta_op_isr_change(&e, 0, 1, 1, isr2, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    meta_op_isr_change(&e, 0, 1, 1, isr2, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);
    uint32_t noleader[1] = {4};
    meta_op_isr_change(&e, 0, 1, 2, noleader, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    v = meta_volume(&m, 0);
    CHECK(v->nisr == 2 && v->isr_version == 2);
    char why[256];
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_free(&prev);

    /* a new leader only from the ISR (1 left it), at the current epoch; two ops on one volume in an entry: no */
    meta_copy(&prev, &m);
    uint32_t isr1[1] = {1};
    meta_op_volume_leader(&e, 0, 1, 1, isr1, 1, 100);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_INVALID);
    uint32_t isr4[1] = {4};
    meta_op_volume_leader(&e, 0, 1, 4, isr4, 1, 100);
    meta_op_isr_change(&e, 0, 2, 3, isr4, 1);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_ERR_INVALID);
    v = meta_volume(&m, 0);
    CHECK(v->epoch == 2 && v->leader == 4 && v->nisr == 1 && v->nhist == 2 && v->hist[1].start == 100);
    CHECK(meta_check_step(&prev, &m, why, sizeof why) == 0);
    meta_op_volume_leader(&e, 0, 1, 4, isr4, 1, 0); /* the deposed epoch's proposer */
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_ERR_STALE);

    /* the checker catches a leader from outside the ISR and an epoch that goes back */
    struct meta_state bent;
    meta_copy(&bent, &m);
    bent.volumes[0].leader = 1; /* not in epoch 1's ISR {2, 4} */
    bent.volumes[0].isr[0] = 1;
    CHECK(meta_check_step(&prev, &bent, why, sizeof why) != 0);
    meta_free(&bent);
    CHECK(meta_check_step(&m, &prev, why, sizeof why) != 0); /* epoch 2 -> 1 */
    meta_free(&bent);
    meta_free(&prev);

    /* replicas: a learner joins, becomes full by joining the ISR; one leaves only once out of the ISR */
    v = meta_volume(&m, 0);
    meta_op_replica_add(&e, 0, 2, 5);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(v->nrep == 4 && v->rep[3].node == 5 && v->rep[3].learner);
    uint32_t isr45[2] = {4, 5};
    meta_op_isr_change(&e, 0, 2, v->isr_version, isr45, 2);
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK);
    v = meta_volume(&m, 0);
    CHECK(!v->rep[3].learner);
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

    /* slots and their earlier homes; users */
    uint32_t n5[3] = {2, 4, 5};
    meta_op_volume_add(&e, 1, "v1", uuid, 2, n5, 3);
    meta_op_slot_home(&e, 0, META_SLOTS, 0);
    meta_op_slot_home(&e, 100, 50, 1);
    meta_op_slot_home(&e, 0, 1, 77); /* no such volume */
    meta_op_user_put(&e, "bob", "abc", 3);
    meta_op_user_put(&e, "alice", "x", 1);
    meta_op_user_del(&e, "carol");
    CHECK(apply1(&m, &e, res, 8) == META_OK && res[0] == META_OK && res[1] == META_OK && res[2] == META_OK &&
          res[3] == META_ERR_NOTFOUND && res[4] == META_OK && res[6] == META_ERR_NOTFOUND);
    CHECK(m.slot_home[99] == 0 && m.slot_home[100] == 1 && m.slot_home[149] == 1 && m.nmoves == 50 &&
          m.moves[0].slot == 100 && m.moves[0].home == 0);
    CHECK(m.nusers == 2 && !strcmp(m.users[0].name, "alice"));

    /* snapshots: the same state back, the same hash; damage refused and the state kept */
    uint8_t *img;
    size_t len;
    meta_snapshot_encode(&m, &img, &len);
    struct meta_state back;
    meta_init(&back);
    CHECK(meta_snapshot_decode(&back, img, len) == META_OK);
    CHECK(meta_hash(&back) == meta_hash(&m) && back.applied == m.applied && back.nvolumes == 2);
    uint64_t h = meta_hash(&back);
    for (size_t i = 0; i < len; i += 7) {
        img[i] ^= 0x10;
        CHECK(meta_snapshot_decode(&back, img, len) == META_ERR_FORMAT);
        img[i] ^= 0x10;
    }
    CHECK(meta_snapshot_decode(&back, img, len - 1) == META_ERR_FORMAT && meta_hash(&back) == h);
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

/* ================================================================ the copied core under its own simulator */

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
    snprintf(dir, sizeof dir, "%s/test_meta_log.%d", base, (int)getpid());
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
    *dst = (struct mlog){.base = src->base, .base_term = src->base_term};
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
    uint64_t adopts;  /* installs cut short and finished from the kept image */
    uint64_t partial_resets;
    uint64_t rebases; /* logs started again on the snapshot at a boot (a reset cut short left them below it) */
    uint64_t quiet; /* no faults and no workload from here on */
    raft_config boot;
    int heal_pending;
    uint64_t heal_at;
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
                meta_apply(&at, e->index, e->term, one, 13 + (size_t)n, &r1, 1);
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
    if (!to || to->group != from->group || iso_rng_chance(&s->rng, 0.02))
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
            struct meta_member mb;
            member(&mb, 1);
            uint8_t uuid[16] = {9};
            meta_op_init(&e, uuid, &st, &mb);
            continue;
        }
        unsigned what = (unsigned)rnd(s, 0, 9);
        if (what == 0 && m->nmembers < 8) {
            struct meta_member mb;
            member(&mb, m->next_member + (uint32_t)rnd(s, 0, 1));
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
            uint8_t uuid[16] = {(uint8_t)m->next_volume};
            meta_op_volume_add(&e, m->next_volume, name, uuid, 1, nodes, 3);
        } else if ((what == 2 || what == 3 || what == 4) && m->nvolumes) {
            const struct meta_volume *v = &m->volumes[rnd(s, 0, m->nvolumes - 1)];
            uint64_t epoch = v->epoch - (rnd(s, 0, 9) == 0 && v->epoch > 1);
            uint32_t pool[META_REPLICAS_MAX] = {0}, np = 0, isr[META_REPLICAS_MAX], ni = 0;
            int wild = rnd(s, 0, 7) == 0; /* sometimes from all replicas: the state machine must refuse */
            for (uint32_t i = 0; i < (wild ? v->nrep : v->nisr); i++)
                pool[np++] = wild ? v->rep[i].node : v->isr[i];
            uint32_t leader = pool[rnd(s, 0, np - 1)];
            isr[ni++] = leader;
            for (uint32_t i = 0; i < np; i++)
                if (pool[i] != leader && rnd(s, 0, 1))
                    isr[ni++] = pool[i];
            if (what == 2) {
                meta_op_volume_leader(&e, v->number, epoch, leader, isr, ni, rnd(s, 0, 1000));
            } else {
                /* an ISR change keeps the leader; may add back any replica */
                ni = 0;
                isr[ni++] = wild ? leader : v->leader;
                for (uint32_t i = 0; i < v->nrep; i++)
                    if (v->rep[i].node != isr[0] && rnd(s, 0, 1))
                        isr[ni++] = v->rep[i].node;
                meta_op_isr_change(&e, v->number, epoch, v->isr_version - (rnd(s, 0, 9) == 0), isr, ni);
            }
        } else if (what == 5 && m->nvolumes) {
            const struct meta_volume *v = &m->volumes[rnd(s, 0, m->nvolumes - 1)];
            uint32_t node = m->members[rnd(s, 0, m->nmembers - 1)].id;
            if (rnd(s, 0, 1))
                meta_op_replica_add(&e, v->number, v->epoch, node);
            else
                meta_op_replica_remove(&e, v->number, v->epoch, node);
        } else if (what == 6) {
            uint32_t id = m->members[rnd(s, 0, m->nmembers - 1)].id;
            uint64_t inc = meta_member(m, id)->incarnation + rnd(s, 0, 1);
            if (rnd(s, 0, 3) == 0) { /* moved: to a fresh address, or onto another member's (refused) */
                char peer[32];
                snprintf(peer, sizeof peer, "10.0.%u.%u:5556", (unsigned)rnd(s, 0, 1), (unsigned)rnd(s, 1, 9));
                meta_op_member_addr(&e, id, inc, "10.9.9.9:5555", peer);
            }
            meta_op_member_state(&e, id, (int)rnd(s, 0, 2), inc);
        } else if (what == 7 && m->nvolumes) {
            uint32_t first = (uint32_t)rnd(s, 0, META_SLOTS - 1);
            meta_op_slot_home(&e, first, (uint32_t)rnd(s, 1, first + 64 <= META_SLOTS ? 64 : META_SLOTS - first),
                              m->volumes[rnd(s, 0, m->nvolumes - 1)].number);
        } else {
            char name[16];
            snprintf(name, sizeof name, "u%u", (unsigned)rnd(s, 0, 5));
            if (rnd(s, 0, 2))
                meta_op_user_put(&e, name, "verifier", (uint32_t)rnd(s, 0, 8));
            else
                meta_op_user_del(&e, name);
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
    iso_log_quiet = true;
    test_state_machine();
    test_raft_log(dir);
    test_raft_sim(seeds, first, seconds);
    unsigned bad = 0;
    for (unsigned i = 0; i < seeds && bad < 3; i++)
        bad += !msim_run(first + i, seconds, seeds <= 4);
    failures += (int)bad;
    if (failures) {
        fprintf(stderr, "test_meta: %d failures\n", failures);
        return 1;
    }
    printf("test_meta: ok (%u seeds of %.0f virtual seconds, from seed %llu)\n", seeds, seconds,
           (unsigned long long)first);
    return 0;
}
