/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cold operations of one cluster ec volume over its stripe group (R8 r8f; design-cluster-data.md E9-E11, E14, E15,
 * §5.7; the R8 plan's D9, D10, D13, D22): `coldops_peer`, the struct cold_ops (src/coldops.h) a cluster volume with
 * redundancy EC attaches with. Position p of an object lives on stripe member stripe[p] of the volume's stripe group as
 * the metadata copy holds it at the put (k' <= k data positions first, then the m parity positions: a small object's
 * parity sits on data members, which E9 allows: positions are distinct members whatever k' is). Everything goes to the
 * members through the chunk server's links (server/chunksrv.h, kinds CHUNK and CHUNKB), which chunksrv_cluster.c keeps
 * to every member of the copy (a superset of every stripe group this node leads or replicates, added and dropped with
 * the copy as fwd_cluster.c keeps FWD links), and this member's own position through its own chunk server, in process.
 *
 *   put_stream  encodes the object once (chunk_encode_stream), stages each position as an unlinked temporary file
 *               (sums table, then cells: the CHUNKB file), and puts it on its member; each PUT_OK's sum must be the
 *               sum of the table sent (E26). A FENCED, a member that is away or does not answer, or any refusal fails
 *               the put: no degraded settle (E10 is R9's). Positions put before a failure are orphans (E14).
 *   obj_open    asks every position for its sums table (CS_OP_SUMS) and header; obj_read is chunk_read_range over a
 *               chunk_source that reads with CS_OP_READ, so a missing or damaged position is decoded around, and a
 *               member that is away is CHUNK_ST_DOWN: UNAVAILABLE, never CORRUPT, when too many are away (E15).
 *   del         refused (TSDB_ERR_UNAVAILABLE, one line per volume) until r8k builds MK_GC (D13): a chunk of a cluster
 *               volume goes only by a committed MK_GC, never by R7's queue or sweep, which could delete the objects of
 *               an MK_COLD that is not committed yet (E14). ecgc keeps the item.
 *   list_names  CS_OP_LIST of every stripe member; `owner` reads the header's vuuid from a one-entry LIST.
 *   see_epoch   CS_OP_EPOCH at e on every stripe member: a member that does not then hold e (away, or an unsure
 *               store, which only the copy raises) makes `writable` false until every member holds the copy's epoch:
 *               writable asks them again, at most once a second. Members that are away never fail the attach.
 * Every position of an open, and every member of see_epoch, writable and all_up, is asked at once (one thread each):
 * a member that hangs costs one deadline, and an open stops waiting for it once k' positions answered.
 *
 * The epoch (D9): a cluster volume's chunk epoch is its metadata epoch. chunkpeer_epoch is the tier's provider (the
 * volume's epoch as the copy holds it); chunkpeer_epoch_source is chunksrv's hook for an unsure store, and answers only
 * while the copy is caught up (cluster_copy_caught_up_at): never from a request, never from the writer's own fence.
 *
 * The transport and the view of the copy are function pointers, so tests/test_chunkpeer.c runs the ops against
 * in-process chunk servers (chunkpeer_local_*) with no peerlink and no cluster.
 */
#ifndef TSDB_CHUNKPEER_H
#define TSDB_CHUNKPEER_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../src/coldops.h"
#include "chunksrv.h"

/* How requests reach a member (the real one: chunkpeer_cluster.c's, over chunksrv's links). call: chunksrv_call's
 * contract. put: chunksrv_put_remote's, with *answered the body the member answered PUT_OK with (rc 0):
 * cs_put_answer_hash of the meta it took and of its sums table's hash. */
struct chunkpeer_net {
    int (*call)(void *arg, uint32_t node, const struct cs_req *r, int timeout_ms, int32_t *rc, uint8_t **body,
                size_t *body_len);
    int (*put)(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum,
               int timeout_ms, uint64_t *answered);
    void *arg;
};

/* An ec volume as one metadata copy holds it. */
struct chunkpeer_view {
    int caught_up; /* cluster_copy_caught_up_at of that copy */
    uint64_t epoch, stripe_version;
    unsigned k, m, nstripe;
    uint8_t gc_suspended; /* R8 (E14): orphan collection suspended until GC_RESUME */
    uint32_t stripe[CHUNK_POS_MAX]; /* position -> member */
};
/* 0 with *v, TSDB_ERR_NOTFOUND when there is no copy or it does not hold `volume` as an ec volume. Any thread. */
typedef int (*chunkpeer_view_fn)(void *arg, uint32_t volume, struct chunkpeer_view *v);

/* One per process (tsdbd's, or a test's): the attach's make_arg and epoch_arg. Outlives every tier using it. */
struct chunkpeer {
    struct chunkpeer_net net;
    chunkpeer_view_fn view;
    void *view_arg;
    struct chunksrv *local; /* this member's chunk server (the ownepoch mutant reads its stores' own fence) */
    char stage_dir[PATH_MAX]; /* where puts are staged (unlinked temporaries) */
    int timeout_ms;           /* per request or put; 0: 30,000 */
};

extern const cold_ops coldops_peer;
/* tier_attach.make with arg a struct chunkpeer * */
int chunkpeer_make(void *arg, struct tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx);
/* tier_epoch_fn (arg a struct chunkpeer *): the volume's epoch in the copy, 0 when it holds none */
uint64_t chunkpeer_epoch(void *arg, uint32_t volume);
/* chunksrv_epoch_fn (arg a struct chunkpeer *): 1 with *epoch only while the copy is caught up and holds the volume */
int chunkpeer_epoch_source(void *arg, uint32_t volume, uint64_t *epoch);
/* Waits until no ask of this process runs any more, those an open stopped waiting for included (tests, before they
 * close the chunk servers their transport calls). */
void chunkpeer_quiesce(void);

/* A request or a put handed to an in-process chunk server as if member `from` had sent it (this member's own
 * position; the tests' transport): the same contracts as chunkpeer_net's call and put. */
int chunkpeer_local_call(struct chunksrv *cs, uint32_t from, const struct cs_req *r, int32_t *rc, uint8_t **body,
                         size_t *body_len);
int chunkpeer_local_put(struct chunksrv *cs, uint32_t from, const struct cs_put_meta *m, int fd, uint64_t base,
                        uint64_t *answered);

/* tsdbd's instance (chunkpeer_cluster.c): over chunksrv_cluster() and the metadata copy; installs chunksrv's epoch
 * source and tier.c's cluster attach hook. Before tsdb_open. */
int chunkpeer_cluster_start(const char *stage_dir);
void chunkpeer_cluster_stop(void); /* the epoch source's installer, if it still waits; after chunksrv_cluster_stop */
struct chunkpeer *chunkpeer_cluster(void);
/* cvol.c, before a cluster volume's engine opens (writer or replica): an ec volume in the copy gets requires=tier,ec in
 * its META (synced), so that it attaches the peer cold ops; 0 for any other volume, else the line and an error. */
int chunkpeer_cluster_prepare(uint32_t volume, const char *vdir);

/* Planted bugs (tests/test_chunkpeer.c and test_cluster ecbasic --mutant; TSDB_CHUNKPEER_MUTANT in tsdbd). */
enum {
    CP_MUT_NONE = 0,
    CP_MUT_OWNEPOCH = 1,    /* ownepoch: the epoch provider and source answer the stores' own fence (the writer's) */
    CP_MUT_NOSUMCMP = 2,    /* nosumcmp: a PUT_OK's sum is not compared with the table sent */
    CP_MUT_STALERAISE = 3,  /* staleraise: the source answers from a copy that is not caught up */
    CP_MUT_DELBEFOREGC = 4, /* delbeforegc: deletes go through on a cluster volume before MK_GC exists */
    CP_MUT_STICKY = 5,      /* sticky: a context unfenced at attach stays so for its life (never asked again) */
    CP_MUT_UNSUREDOWN = 6,  /* unsuredown: all_up counts a member whose store is unsure as down */
};
extern int chunkpeer_mutant;
extern uint32_t chunkpeer_test_list_max; /* entries per page of list_names (test_chunkpeer: pagination) */
int chunkpeer_mutant_parse(const char *name); /* CP_MUT_* or -1 */

#endif
