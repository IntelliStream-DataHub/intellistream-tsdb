/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The chunk server (docs/design-cluster-data.md §5.7, E14, E26, E27; R8 D9-D11): this member's chunk stores
 * (`--chunk-dir`, a chunk_backend of them) served to the other members over a peerlink of its own, as the forwarding
 * links have theirs (fwd_cluster.c), with two kinds:
 *
 *   PL_KIND_CHUNK   requests and their answers (frames CS_T_REQ, CS_T_RESP below);
 *   PL_KIND_CHUNKB  puts, as bulk files under this peerlink's own rate cap (`--chunk-rate-mb-s`): a settle never
 *                   competes with replication's BULK cap, and replication never with it.
 *
 * A put is a bulk file whose meta (struct cs_put_meta) names the chunk (volume, vuuid, oid, position, geometry) and the
 * writer's epoch, and whose bytes are the position's sums table (8 bytes a cell, little endian) followed by its cells.
 * The sums come first so that the receiver checks each cell against its sum as it arrives (E26) and never stages an
 * unchecked chunk; they are not in the meta because the meta is bounded (PL_META_MAX) and a position may have more
 * cells than fit. The receiver raises the volume's fence to the put's epoch, durably, or refuses below it (FENCED),
 * writes the chunk into the store chunk_be_place names (tmp, fsync, rename, directory fsync) and only then answers
 * CS_OP_PUT on the CHUNK link with the chunk's sum: xxh64 of the sums table, which the sender compares with its own.
 * A put that was not answered may or may not be there (whole, never torn); putting it again is harmless.
 *
 * Every request carries the requester's epoch of the volume and raises the fence first (E14). An "unsure" store (one
 * EPOCH slot damaged, chunkstore.c) is never raised from a request: only from the metadata copy's epoch of that volume
 * while the copy is caught up (D9), through the hook chunksrv_epoch_source; until then puts and deletes of the volume
 * are refused (CHUNK_ERR_EPOCH_UNSURE), reads and lists go on.
 *
 * Every length on the wire is bounded before anything is allocated, and a frame that does not parse ends the link
 * (plink_reset): a member of this version never sends one, so it is damage or a bug, and nothing is guessed.
 *
 * A store whose write or sync fails is failed in place (chunk_store_failed: it acknowledges nothing more) and reads as
 * DOWN in chunksrv_stores; tsdbd goes on (E57's per-disk rule).
 *
 * The server half (chunksrv_handle, chunksrv_put_*) takes its frames from any transport, so tests/test_chunksrv.c
 * drives it with a fake link; chunksrv_listen puts it on a peerlink, and the client half (chunksrv_call,
 * chunksrv_put_remote) uses the same peerlink to reach other members.
 */
#ifndef TSDB_CHUNKSRV_H
#define TSDB_CHUNKSRV_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../src/chunk.h"

struct peerlink;
struct plink;

/* ---------------------------------------------------------------- wire (exported for the tests and the fuzzer) */
enum { CS_T_REQ = 1, CS_T_RESP = 2 }; /* driver frame types on a CHUNK link */
enum { CS_OP_READ = 1, CS_OP_SUMS = 2, CS_OP_LIST = 3, CS_OP_DEL = 4, CS_OP_EPOCH = 5, CS_OP_PUT = 6 /* answers only */ };

#define CS_READ_MAX (4u << 20)   /* bytes of one CS_OP_READ */
#define CS_SUMS_MAX 65536u       /* sums of one CS_OP_SUMS */
#define CS_LIST_MAX 4096u        /* entries of one CS_OP_LIST */
#define CS_LIST_ENTRY 52u        /* oid 16, pos 2, size 8, epoch 8, vuuid 16, status 1, store 1 */
#define CS_PUT_MAGIC 0x54505343u /* "CSPT" */
#define CS_PUT_META_LEN 66u /* magic 4, volume 4, epoch 8, vuuid 16, oid 16, pos 2, k 2, m 2, cell 4, len 8 */

/* A request: [u64 rid][u8 op][u32 volume][u64 epoch] and the op's body:
 *   READ   vuuid 16, oid 16, u16 pos, u64 off, u32 len (1 .. CS_READ_MAX)
 *   SUMS   vuuid 16, oid 16, u16 pos, u64 first, u32 count (1 .. CS_SUMS_MAX)
 *   LIST   u8 has_cursor, oid 16, u16 pos (the last entry seen), u32 max (1 .. CS_LIST_MAX)
 *   DEL    vuuid 16, oid 16, u16 pos
 *   EPOCH  nothing */
struct cs_req {
    uint64_t rid;
    uint8_t op;
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    uint16_t pos;
    uint64_t off; /* READ: offset in the cell area; SUMS: first cell */
    uint32_t len; /* READ: bytes; SUMS: count; LIST: max */
    bool has_cursor;
};
/* An answer: [u64 rid][u8 op][i32 rc] and, when rc is 0, the op's body:
 *   READ   the bytes
 *   SUMS   u64 epoch, u16 k, u16 m, u32 cell, u64 len (the chunk's header), u64 ncells, u32 n, n x u64
 *   LIST   u32 n, u8 more, n entries of CS_LIST_ENTRY bytes
 *   DEL    nothing
 *   EPOCH  u64 the fence now, u8 unsure
 *   PUT    u64 the chunk's sum (rid is the bulk file's id)
 * rc is 0, a TSDB_ERR_* or a CHUNK_ERR_* code; chunksrv_st names it as CHUNK_ST_* for a read. */
struct cs_resp {
    uint64_t rid;
    uint8_t op;
    int32_t rc;
    const uint8_t *body;
    size_t body_len;
};
struct cs_put_meta {
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    uint16_t pos;
    chunk_geom g;
};

size_t cs_req_encode(const struct cs_req *r, uint8_t *out, size_t cap); /* bytes written, 0 if cap is too small */
int cs_req_decode(const uint8_t *p, size_t n, struct cs_req *r);         /* 0, or -1 for a malformed frame */
int cs_resp_decode(const uint8_t *p, size_t n, struct cs_resp *r);       /* 0 or -1 (the body is checked per op) */
void cs_put_meta_encode(const struct cs_put_meta *m, uint8_t out[CS_PUT_META_LEN]);
int cs_put_meta_decode(const uint8_t *p, size_t n, struct cs_put_meta *m); /* 0, or -1 (a geometry that cannot be too) */
uint64_t cs_put_total(const struct cs_put_meta *m);                       /* the file's bytes: sums, then cells */
uint64_t cs_sums_hash(const uint64_t *sums, uint64_t n);                  /* xxh64 of the table, little endian */
int chunksrv_st(int rc); /* CHUNK_ST_* of an answer's rc: OK, ABSENT, DAMAGED, IOERR, DOWN (-1: none of those) */

/* ---------------------------------------------------------------- the server */
struct chunksrv;

/* Opens the stores at paths[0..n) (each its own chunk_store_open: the flock makes this process their one owner). A
 * store that is missing, damaged or failing is DOWN, logged and not fatal; another owner holding one (BUSY) or a path
 * named twice fails the open. NULL with a line in err. */
struct chunksrv *chunksrv_open(const char *const *paths, int n, char *err, size_t errlen);
void chunksrv_close(struct chunksrv *cs); /* after chunksrv_unlisten */
/* D11: every store that is open must name this cluster and node in its META; one that names another one, or none,
 * is refused with a line naming both (a disk moved between members or clusters): -1. */
int chunksrv_check_owner(struct chunksrv *cs, const uint8_t cluster[16], uint32_t node, char *err, size_t errlen);
/* Stores by state: up (open and not failed) and n. Logs each store's state when `log`. */
int chunksrv_stores(struct chunksrv *cs, int *up, bool log);
chunk_backend *chunksrv_backend(struct chunksrv *cs);

/* D9: the authoritative epoch of a volume, for an unsure store: 1 with *epoch set while the metadata copy is caught
 * up (cluster_copy_caught_up) and holds the volume, else 0. r8f sets it; without one an unsure store stays refused. */
typedef int (*chunksrv_epoch_fn)(void *arg, uint32_t volume, uint64_t *epoch);
void chunksrv_epoch_source(struct chunksrv *cs, chunksrv_epoch_fn fn, void *arg);

/* One answer frame (a CS_T_RESP payload) to the requester. */
typedef void (*cs_reply_fn)(void *arg, const uint8_t *p, size_t n);
/* A CHUNK request frame from member `peer`: answered through `reply` before it returns. -1: malformed (no answer; the
 * link that brought it is ended). Blocks on the stores: call it from a worker, never from a link's thread. */
int chunksrv_handle(struct chunksrv *cs, uint32_t peer, const uint8_t *p, size_t n, cs_reply_fn reply, void *arg);

/* A put arriving (the CHUNKB bulk callbacks call these). begin: the file's meta and length; a refusal is answered
 * through `reply` (CS_OP_PUT with the reason) and returns it (< 0). data: the bytes in order; < 0 ends the put (also
 * answered). end: status 0 commits it and answers (blocks on a sync: from a worker), anything else drops it. */
struct cs_put;
int chunksrv_put_begin(struct chunksrv *cs, uint32_t peer, uint64_t file, const void *meta, size_t meta_len,
                       uint64_t total, cs_reply_fn reply, void *arg, struct cs_put **out);
int chunksrv_put_data(struct cs_put *p, uint64_t off, const void *d, size_t n);
void chunksrv_put_end(struct cs_put *p, int status);

struct chunksrv_stats {
    uint64_t requests, malformed, puts_ok, puts_refused, put_bytes, reads, read_bytes, fenced, unsure_refused,
        source_raises;
};
void chunksrv_get_stats(struct chunksrv *cs, struct chunksrv_stats *st);

/* ---------------------------------------------------------------- on a peerlink */
struct chunksrv_net_cfg {
    uint32_t self_id;
    uint8_t uuid[16], ident[16];
    const uint8_t *secret;
    size_t secret_len;
    char advertise[128];
    uint64_t rate;  /* bytes/s of this peerlink's puts, all links (0: PL_BULK_RATE_DEFAULT) */
    int workers;    /* 0: 4 */
    uint32_t dead_ms, ping_ms; /* 0: the peerlink's defaults */
};
/* Makes the peerlink (kinds CHUNK and CHUNKB) and its workers on cs. A member with no store opens cs with no paths and
 * still calls others (every request it gets is answered TSDB_ERR_UNAVAILABLE). */
int chunksrv_listen(struct chunksrv *cs, const struct chunksrv_net_cfg *cfg);
struct peerlink *chunksrv_peerlink(struct chunksrv *cs);
/* Stops the workers and the links (no call of the client half may be running); chunksrv_close does it too. */
void chunksrv_unlisten(struct chunksrv *cs);
/* Where member `id` is (its peer address); a changed address is redialed. */
int chunksrv_set_peer(struct chunksrv *cs, uint32_t id, const char *addr);
/* Every peer not in ids (members that left) is retired: its links end and dial no more (plink_stop), calls to it are
 * TSDB_ERR_UNAVAILABLE, and chunksrv_set_peer does not bring it back. */
void chunksrv_keep_peers(struct chunksrv *cs, const uint32_t *ids, size_t n);
void chunksrv_set_rate(struct chunksrv *cs, uint64_t bytes_per_s);

/* The client half. A request to member `peer`: 0 with *rc and the answer's body (malloc'ed, *body may be NULL when
 * empty), or TSDB_ERR_UNAVAILABLE (no link, or no answer within timeout_ms). */
int chunksrv_call(struct chunksrv *cs, uint32_t peer, const struct cs_req *r, int timeout_ms, int32_t *rc,
                  uint8_t **body, size_t *body_len);
/* Puts a chunk on member `peer`: fd[base, base + cs_put_total) holds its sums table and cells (the staged put; the
 * fd is dup'ed, the caller may close it at return). 0 once the peer answered PUT_OK with the same sum; else its
 * refusal (CHUNK_ERR_FENCED, CORRUPT for a cell that failed its sum, ...), TSDB_ERR_IO for a sum that differs, or
 * TSDB_ERR_UNAVAILABLE (no answer: the chunk may or may not be there). */
int chunksrv_put_remote(struct chunksrv *cs, uint32_t peer, const struct cs_put_meta *m, int fd, uint64_t base,
                        uint64_t sum, int timeout_ms);
/* Stages a put in an unlinked temporary file under dir: sums then cells. The fd, or a negative TSDB_ERR_*. */
int chunksrv_stage(const char *dir, const chunk_geom *g, unsigned pos, const void *cells, const uint64_t *sums);

/* The erasure-coded tier's settings (design-cluster-data.md E5, E10, E26, §5.8): tsdbd parses its --ec-* options into
 * g_ec_cfg (server/tsdbd.c), which the tier's attach takes (R8). */
struct chunksrv_ec_cfg {
    uint32_t settle_grace_s;    /* E5: a key's end this far in the past before it settles (3,600) */
    uint32_t settle_keep_s;     /* E26: a settle's retired parts are kept this long (86,400) */
    uint32_t settle_force_s;    /* E5: a settle forces a flush of its rows after this long (3,600) */
    uint32_t settle_wait_max_s; /* E10: a settle waits this long for a whole stripe group, then goes degraded (7,200) */
    uint64_t cache_bytes;       /* the decoded-granule cache; 0: a tenth of the RAM */
};
extern struct chunksrv_ec_cfg g_ec_cfg;

#define CHUNKSRV_DIRS_MAX 64 /* --chunk-dir, one per disk (E27) */
/* ---------------------------------------------------------------- tsdbd's instance (chunksrv_cluster.c) */
/* Opens the stores (a store held by another process refuses the start; one that is missing, damaged or failing is
 * DOWN), checks their owner when this server's node is known (-1 with the line in err: tsdbd refuses to start) and
 * starts the thread that puts the server on the cluster's peer port once it is a member. After cluster_start. */
int chunksrv_cluster_start(const char *const *paths, int n, uint64_t rate, char *err, size_t errlen);
void chunksrv_cluster_stop(void);         /* after cluster_stop: no socket is handed to the links any more */
void chunksrv_cluster_free(void);         /* once tsdbd's workers are joined */
struct chunksrv *chunksrv_cluster(void);  /* NULL until it serves */
/* chunksrv_cluster() for a caller that is not one of tsdbd's workers (the engines' threads): held until
 * chunksrv_cluster_release, which chunksrv_cluster_free waits for; NULL (nothing held) until it serves and once freed */
struct chunksrv *chunksrv_cluster_hold(void);
void chunksrv_cluster_release(void);
int chunksrv_cluster_closes(void); /* how many times chunksrv_cluster_free closed it (a test hook's check) */

/* ---------------------------------------------------------------- test hooks (tests/test_chunksrv.c --mutant) */
enum {
    CS_MUT_NONE = 0,
    CS_MUT_NOFENCE = 1,    /* chunknofence: a put or delete is made at the store's own epoch, not the request's */
    CS_MUT_SELFRAISE = 2,  /* chunkselfraise: an unsure store is raised from a request's epoch */
    CS_MUT_PUTOKEARLY = 3, /* putokearly: PUT_OK goes out before the chunk is synced and named */
    CS_MUT_NOCELLCHECK = 4, /* nocellcheck: cells are taken without checking them against their sums */
    CS_MUT_STOREFOREIGN = 5, /* storeforeign: a store's cluster and node are not checked */
};
extern int chunksrv_mutant;
extern int chunksrv_test_put_delay_ms; /* tests: the first cell of every put waits this long (a slow disk) */
int chunksrv_mutant_parse(const char *name); /* CS_MUT_* or -1 */

#endif
