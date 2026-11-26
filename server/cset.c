/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes, the Raft leader's side (R4 plan D12, D13; cluster_int.h; design-cluster.md 8.5).
 *
 * cset_admin is CLUSTER_OP_REPLICA_RESET (`tsdb cluster replica reset ADDR VOLUME NODE`): on the Raft leader it proposes
 * REPLICA_RESET(v, epoch, node, a fresh random rid) for a replica whose directory was lost. The member must be neither
 * the volume's leader nor in its ISR: those copies are the ones a promotion may need, and a reset makes the member a
 * learner that is rebuilt from the leader's log or a snapshot. meta.c refuses the same (D13); this side answers with the
 * reason in words, before anything is proposed. The admin link already proved the cluster's secret (cluster.c's
 * handshake), as for every admin op.
 *
 * The answer is "proposed": the change applies through the log, and the reset server learns it from the watch
 * (server/cvolset_mgr.c: the old directory set aside as v<N>.lost-<oldrid>, a fresh one made for the new rid).
 *
 * cset_setop_file is a test hook (TSDB_CLUSTER_TEST setop_file=1): REPLICA_ADD and REPLICA_REMOVE have no operator verb
 * before r4-move, so test_cluster's set* scenarios write them, one per line ("add V NODE [REPLACES]", "remove V NODE",
 * "reset V NODE"), into <state-dir>/setop; the Raft leader proposes each line once, appends what it did to setop.out
 * ("OP V NODE: proposed|refused RID REASON") and removes the file. Whether the entry applied is the record's to say.
 */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../client/tsdb_crypto.h"
#include "cluster_int.h"

/* TSDB_CLUSTER_MUTANT (test_cluster resetlost's mutants): resetinisr skips the leader and ISR check here (meta.c still
 * refuses: the answer then lacks the reason), resetsamerid proposes the member's old rid (meta.c refuses it as not new:
 * the reset never applies) */
static int cset_mutant(const char *name)
{
#ifdef TSDB_TEST_HOOKS
    const char *m = getenv("TSDB_CLUSTER_MUTANT");
    return m && !strcmp(m, name);
#else
    (void)name;
    return 0;
#endif
}

/* a rid no member of v holds (nor 0) */
static uint64_t fresh_rid(const struct meta_volume *v)
{
    for (;;) {
        uint64_t rid = 0;
        tsdb_random(&rid, sizeof rid);
        int dup = !rid;
        for (uint32_t j = 0; v && j < v->nrep; j++)
            dup |= v->rep[j].rid == rid;
        if (!dup)
            return rid;
    }
}

/* Checks a reset of `node` in volume `number` and proposes it: 0 with *rid set, else a META_ERR_* / CLUSTER_ERR_* code
 * and the reason in t */
static int reset_propose(uint32_t number, uint32_t node, uint64_t *rid, char *t, size_t tn)
{
    const struct meta_volume *v = meta_volume(cl_meta(), number);
    if (!v) {
        snprintf(t, tn, "volume %" PRIu32 " does not exist", number);
        return META_ERR_NOTFOUND;
    }
    const struct meta_replica *r = meta_replica(v, node);
    if (v->kind != META_VOL_REPLICATED || !r) {
        snprintf(t, tn, "node %" PRIu32 " is not a replica of replicated volume %" PRIu32, node, number);
        return META_ERR_INVALID;
    }
    if (!cset_mutant("resetinisr")) {
        if (node == v->leader) {
            snprintf(t, tn, "node %" PRIu32 " leads volume %" PRIu32 " (epoch %" PRIu64 "): a reset is refused for the "
                     "leader and every ISR member (transfer the leadership or let the ISR shrink first)",
                     node, number, v->epoch);
            return META_ERR_INVALID;
        }
        if (meta_volume_in_isr(v, node)) {
            snprintf(t, tn, "node %" PRIu32 " is in the ISR of volume %" PRIu32 ": a reset is refused for the leader "
                     "and every ISR member (its copy is one a promotion may need)", node, number);
            return META_ERR_INVALID;
        }
    }
    *rid = cset_mutant("resetsamerid") ? r->rid : fresh_rid(v);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_replica_reset(&e, number, v->epoch, node, *rid);
    if (cl_propose(&e) != 0) { /* frees e */
        snprintf(t, tn, "not proposed (no quorum now): try again");
        return CLUSTER_ERR_RETRY;
    }
    return 0;
}

void cset_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t v, node;
    char t[320];
    if (cluster_volume_force_decode(body, n, &v, &node) != 0) {
        int tl = snprintf(t, sizeof t, "replica reset: a body of %zu bytes", n);
        cl_admin_reply(conn, reqid, META_ERR_INVALID, t, (size_t)tl);
        return;
    }
    uint64_t rid = 0;
    int rc = reset_propose(v, node, &rid, t, sizeof t);
    if (rc == 0) {
        snprintf(t, sizeof t, "volume %" PRIu32 ": reset of node %" PRIu32 " proposed (new instance %016" PRIx64
                 "); its old directory is set aside as v%" PRIu32 ".lost-<old rid> and a fresh one fills", v, node,
                 rid, v);
        cl_say("%s", t);
    }
    cl_admin_reply(conn, reqid, rc, t, strlen(t));
}

void cset_setop_file(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return;
    char out[1200];
    snprintf(out, sizeof out, "%s.out", path);
    FILE *o = fopen(out, "a");
    char line[256];
    while (fgets(line, sizeof line, f)) {
        char op[16];
        unsigned vol = 0, node = 0, repl = 0;
        int k = sscanf(line, "%15s %u %u %u", op, &vol, &node, &repl);
        if (k < 3)
            continue;
        char t[320] = "";
        int rc = 0;
        uint64_t rid = 0;
        const struct meta_volume *v = meta_volume(cl_meta(), vol);
        if (!strcmp(op, "reset")) {
            rc = reset_propose(vol, node, &rid, t, sizeof t);
        } else if (!v) {
            rc = META_ERR_NOTFOUND;
            snprintf(t, sizeof t, "no volume %u", vol);
        } else {
            struct meta_entry e;
            meta_entry_init(&e);
            if (!strcmp(op, "add")) {
                rid = fresh_rid(v);
                meta_op_replica_add(&e, vol, v->epoch, node, rid, k >= 4 ? repl : 0);
            } else {
                meta_op_replica_remove(&e, vol, v->epoch, node);
            }
            rc = cl_propose(&e);
        }
        if (o)
            fprintf(o, "%s %u %u: %s %016" PRIx64 " %s\n", op, vol, node, rc ? "refused" : "proposed", rid, t);
        cl_say("setop %s %u %u: %s %016" PRIx64 " %s", op, vol, node, rc ? "refused" : "proposed", rid, t);
    }
    fclose(f);
    if (o) {
        fflush(o);
        fsync(fileno(o));
        fclose(o);
    }
    unlink(path);
}
