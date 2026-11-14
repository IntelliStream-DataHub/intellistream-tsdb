/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Objects, chunk files and chunk stores (docs/design-cluster-data.md step R6; §2.2, §3.1-3.2, §5.5, §5.7, §8).
 * Internal: R7 (the engine's .ec parts) and R8 (tsdbd's chunk server) are its users; nothing here is public API and
 * nothing in the engine calls it yet.
 *
 * An object is a part file's bytes, length L, cut into cells of `cell` bytes and dealt round-robin over k' data chunks
 * (k' = min(k, number of cells): E7); stripe s holds cells s*k' .. s*k'+k'-1 and m parity cells, computed over the
 * cells padded with zeros to the stripe's longest one (its first), and stored at that length: a one-cell object is
 * m + 1 copies by construction, and only a short last cell is shorter than the cell size. Positions are the object's
 * own: 0 .. k'-1 data, k' .. k'+m-1 parity. How an object's positions map to a volume's stripe group (k' below the
 * volume's k) is the placement's (R8).
 *
 * Layers:
 *   chunk_geom_*     the geometry, recorded in the object and in every chunk header, never taken from settings (E2).
 *   chunk file       header (96 bytes: object id, position, geometry, the writer's epoch), cells, an xxh64 per cell
 *                    (the encode-time sums, E26), a footer hashing header and sums. Written tmp + fsync + rename +
 *                    directory fsync. A chunk that does not parse, whose size is not what its header implies, whose
 *                    identity is not the one asked for or whose cell fails its sum is an erasure, never decoder input.
 *   chunk_store      one directory per disk: META (store id), per volume an EPOCH (two checksummed slots, as FNO) and
 *                    `s<volume>/o<oid>.<pos>.chunk`. A put or delete below the volume's epoch is refused (E14); a write
 *                    or sync that fails after a name may have become visible, or in EPOCH, is fail-stop for the store.
 *   chunk_backend    N stores of one machine (the backend R7 settles on and the tests use): put, get, list, delete.
 *   chunk_encode     an object into its chunks, stripe by stripe; chunk_read_range: any byte range of an object from
 *                    any k' good chunks, bit exact, over chunk_source (files here, memory in tests, the network in R8).
 */
#ifndef TSDB_CHUNK_H
#define TSDB_CHUNK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CHUNK_K_MAX 16
#define CHUNK_M_MAX 4
#define CHUNK_POS_MAX (CHUNK_K_MAX + CHUNK_M_MAX)
#define CHUNK_CELL_MIN (64u << 10) /* E7's clamp, for derived cells; a recorded cell may be any size from 1 */
#define CHUNK_CELL_MAX (1u << 20)
#define CHUNK_CELL_LIMIT (64u << 20)
#define CHUNK_OBJ_MAX (1ull << 40) /* a sanity bound on object_len */
#define CHUNK_NCELLS_MAX (1ull << 26) /* data cells of an object: a sums table is 8 bytes a cell, and a damaged header
                                       * must not make a reader allocate gigabytes (it also keeps the footer's 32-bit
                                       * cell count honest) */
#define CHUNK_HDR_BYTES 96
#define CHUNK_FTR_BYTES 16
#define CHUNK_VERSION 1u
#define CHUNK_STORE_VERSION 1

/* Errors: the TSDB_ERR_* codes where one fits (NOTFOUND: no such chunk or store; CORRUPT: damaged, or more than m
 * chunks unusable; IO; INVALID: a bad argument or an object that cannot be). Beside them: */
#define CHUNK_ERR_FENCED (-101)   /* a put or delete below the volume's epoch, or a delete below the chunk's own (E14) */
#define CHUNK_ERR_STORE_ID (-102) /* the store's META names another store than the one the caller was told (E27) */
#define CHUNK_ERR_FULL (-103)     /* ENOSPC while writing a chunk (the call failed; nothing was acknowledged) */
#define CHUNK_ERR_BUSY (-104)     /* another opener holds the store directory (two owners would let EPOCH go back) */
#define CHUNK_ERR_EPOCH_UNSURE (-105) /* one EPOCH slot of the volume was damaged at open: the fence may be lower than it
                                       * was, so puts and deletes of the volume are refused until chunk_store_raise
                                       * (or chunk_be_see_epoch) is given the authoritative epoch; reads go on */

/* What a chunk (or a cell of one) is, as a reader found it. */
enum { CHUNK_ST_OK = 0, CHUNK_ST_ABSENT, CHUNK_ST_DAMAGED, CHUNK_ST_IOERR, CHUNK_ST_VIRTUAL /* zero padding */,
       CHUNK_ST_DOWN /* the chunk's store is not UP: it may well be there, and it comes back with the store (E15) */ };

/* ---------------------------------------------------------------- geometry */
typedef struct chunk_geom {
    uint16_t k;    /* data chunks (k'), 1 .. CHUNK_K_MAX, at most the number of cells */
    uint16_t m;    /* parity chunks, 1 .. CHUNK_M_MAX */
    uint32_t cell; /* cell bytes */
    uint64_t len;  /* object bytes, at least 1 */
} chunk_geom;

/* E7: the cell size of an object of `len` bytes on a volume of k data chunks: clamp(len / (8k), 64 KiB, 1 MiB),
 * rounded up to 4 KiB (before the clamp); k' = min(k, cells). TSDB_ERR_INVALID for k or m out of range or len 0. */
int chunk_geom_derive(chunk_geom *g, unsigned k, unsigned m, uint64_t len);
/* The geometry an object record or a header names (k' already derived), checked: 0 or TSDB_ERR_INVALID. */
int chunk_geom_make(chunk_geom *g, unsigned k, unsigned m, uint32_t cell, uint64_t len);
bool chunk_geom_valid(const chunk_geom *g);
uint64_t chunk_ncells(const chunk_geom *g);                         /* data cells of the object */
uint64_t chunk_nstripes(const chunk_geom *g);
unsigned chunk_npos(const chunk_geom *g);                           /* k' + m */
uint64_t chunk_cell_len(const chunk_geom *g, uint64_t c);           /* bytes of data cell c (the last may be short) */
uint64_t chunk_stripe_len(const chunk_geom *g, uint64_t s);         /* shard length of stripe s = its parity cell's */
uint64_t chunk_pos_cells(const chunk_geom *g, unsigned pos);        /* cells chunk `pos` holds */
uint64_t chunk_pos_len(const chunk_geom *g, unsigned pos);          /* bytes of those cells */
uint64_t chunk_file_size(const chunk_geom *g, unsigned pos);        /* header + cells + sums + footer */
uint64_t chunk_sums_total(const chunk_geom *g);                     /* cells of all positions: the object record's sums */
uint64_t chunk_sums_base(const chunk_geom *g, unsigned pos);        /* index of pos's first cell in that array */

/* ---------------------------------------------------------------- identity */
typedef struct chunk_ident {
    uint32_t volume; /* the volume number: the directory s<volume> */
    uint8_t vuuid[16];
    uint8_t oid[16];
    uint16_t pos;
} chunk_ident;

void chunk_oid_hex(const uint8_t oid[16], char out[33]);
void chunk_random(void *buf, size_t n); /* store ids, object ids: /dev/urandom (never a timestamp or a counter) */

/* ---------------------------------------------------------------- chunk file format */
typedef struct chunk_hdr {
    uint32_t flags; /* 0 in version 1 */
    uint8_t vuuid[16], oid[16];
    uint64_t epoch; /* the writer's epoch */
    chunk_geom g;
    uint16_t pos;
    uint64_t chunk_len; /* bytes of cells */
} chunk_hdr;

void chunk_hdr_encode(const chunk_hdr *h, uint8_t out[CHUNK_HDR_BYTES]);
/* 0, or TSDB_ERR_CORRUPT: magic, version, flags, reserved bytes, geometry, pos or chunk_len not as version 1 says. */
int chunk_hdr_decode(const uint8_t in[CHUNK_HDR_BYTES], chunk_hdr *h);

/* A whole chunk file in memory: 0 when it is valid in every part (header, size, sums table, footer, and with
 * `check_cells` each cell against its sum), else TSDB_ERR_CORRUPT. *sums points into `buf`. This is what fuzz_chunk
 * feeds, and what the file reader does piece by piece. */
int chunk_image_parse(const uint8_t *buf, size_t n, bool check_cells, chunk_hdr *h, const uint8_t **cells,
                      const uint8_t **sums_le);
/* The bytes of a chunk file from its parts (cells: h->chunk_len bytes; sums: one per cell). Caller frees. */
uint8_t *chunk_image_build(const chunk_hdr *h, const uint8_t *cells, const uint64_t *sums, size_t *n);

/* ---------------------------------------------------------------- stores */
typedef struct chunk_store chunk_store;
typedef struct chunk_wr chunk_wr;

/* Makes the store: the directory (one level may be created), its META with a store id (id NULL: random). Refuses a
 * directory that already has a META (TSDB_ERR_INVALID). Synced with its parent. */
int chunk_store_init(const char *path, const uint8_t *id, uint8_t id_out[16]);
/* Opens it. expect_id (16 bytes) names the store the caller believes this directory to be: another id is
 * CHUNK_ERR_STORE_ID (a disk that came back as another one: a lost disk, E27); no META is TSDB_ERR_NOTFOUND; a
 * damaged one TSDB_ERR_CORRUPT. Volumes found are loaded with their EPOCH, leftover tmp_* files removed. The directory
 * is locked (flock, held until close): a second open of it, in this process or another, is CHUNK_ERR_BUSY, because two
 * owners each hold their own in-memory epoch and one of them would write it back lower. A volume with exactly one
 * damaged EPOCH slot (a torn store and rot look alike) opens with the older slot's epoch, logs it, and refuses puts and
 * deletes (CHUNK_ERR_EPOCH_UNSURE) until chunk_store_raise gives it the epoch the metadata group holds. A readdir that
 * fails is an error, never the end of the directory. */
int chunk_store_open(const char *path, const uint8_t *expect_id, chunk_store **out);
void chunk_store_close(chunk_store *st);
const uint8_t *chunk_store_id(const chunk_store *st);
bool chunk_store_failed(const chunk_store *st); /* a failed write or sync: it acknowledges nothing more (reads go on) */
/* The volume's epoch in this store (0 if it has none). */
uint64_t chunk_store_epoch(chunk_store *st, uint32_t volume);
/* Raises the volume's EPOCH to at least e, durably, before returning (E14: before acting on a higher one). This is
 * the authoritative call (the epoch comes from the metadata group): it also rewrites the damaged slot of an unsure
 * volume (e >= the epoch read) and clears that state. */
int chunk_store_raise(chunk_store *st, uint32_t volume, uint64_t e);

/* One put, piece by piece. The fence is checked at open (epoch >= the store's: else CHUNK_ERR_FENCED, nothing made)
 * and again under the epoch lock at the rename; a higher epoch raises EPOCH first. cell(): `len` must be the length
 * the geometry gives the next cell; with sum_given the cell must hash to `sum` (a chunk server checks what it
 * receives, E26). commit() makes the chunk durable and visible (file fsync, rename, directory fsync) or fails with
 * nothing visible; a failed directory fsync is fail-stop. abort() drops the temporary file. */
int chunk_wr_open(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t epoch, chunk_wr **out);
int chunk_wr_cell(chunk_wr *w, const void *data, size_t len, bool sum_given, uint64_t sum);
int chunk_wr_commit(chunk_wr *w);
void chunk_wr_abort(chunk_wr *w);
/* A whole chunk from memory: `cells` is chunk_pos_len bytes; sums NULL computes them, else they are checked. */
int chunk_store_put(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t epoch, const void *cells,
                    const uint64_t *sums);
/* Deletes the chunk (a missing one is TSDB_ERR_NOTFOUND). Refused below the volume's epoch, and below the epoch the
 * chunk's header records (E14); the unlink is followed by a directory fsync (fail-stop). */
int chunk_store_del(chunk_store *st, const chunk_ident *id, uint64_t epoch);

/* A chunk open for reading. open() checks everything but the cells (header, size, sums table, footer, identity):
 * TSDB_ERR_NOTFOUND (absent), TSDB_ERR_CORRUPT (damaged), TSDB_ERR_IO (a read failed; never taken for the end). */
typedef struct chunk_rd chunk_rd;
int chunk_rd_open(chunk_store *st, const chunk_ident *id, chunk_rd **out);
void chunk_rd_close(chunk_rd *r);
const chunk_hdr *chunk_rd_hdr(const chunk_rd *r);
const uint64_t *chunk_rd_sums(const chunk_rd *r); /* chunk_pos_cells entries */
/* Bytes of the cell area at `off`, exactly `len` of them or an error (a short read is TSDB_ERR_IO). */
int chunk_rd_read(chunk_rd *r, uint64_t off, void *buf, size_t len);
/* Every cell against its sum: 0 and *bad = cells that fail (TSDB_ERR_IO if a read failed). */
int chunk_rd_scrub(chunk_rd *r, uint64_t *bad);

/* A chunk listed: what its file name and header say (verify: the footer and sums table too). */
typedef struct chunk_listed {
    uint8_t oid[16];
    uint16_t pos;
    uint64_t size;
    uint64_t epoch;
    int status; /* CHUNK_ST_OK, DAMAGED (header or, with verify, footer or sums bad), IOERR */
    int store;  /* chunk_be_list: the index of the store it is in */
} chunk_listed;
typedef int (*chunk_list_fn)(void *ctx, const chunk_listed *c); /* non-zero stops the scan, and is returned */
int chunk_store_list(chunk_store *st, uint32_t volume, bool verify, chunk_list_fn fn, void *ctx);

/* ---------------------------------------------------------------- the directory backend */
typedef struct chunk_backend chunk_backend;

/* What a store of a backend is (chunk_be_store_state). Only UP and REPLACING hold an open store; the DOWN ones are
 * read around by decoding (§5.5, E55) and never written to. A DOWN store keeps its index: placement is by index. */
enum {
    CHUNK_STORE_UP = 0,
    CHUNK_STORE_DOWN_MISSING,  /* no path was given, the directory is not there, or it has no META (a lost disk) */
    CHUNK_STORE_DOWN_OTHER_ID, /* its META names another store than the one expected: a disk that came back as another
                                * one (E27). Never written to: its chunks and its EPOCH are another store's */
    CHUNK_STORE_DOWN_DAMAGED,  /* META or EPOCH damaged (both slots) beyond what its own checks repair: the store's id or
                                * fence cannot be trusted, so nothing is rewritten in place: replace it */
    CHUNK_STORE_DOWN_IOERR,    /* the open failed with an I/O error, or the store failed in service (chunk_store_failed) */
    CHUNK_STORE_REPLACING,     /* open, fenced and being filled by heal (chunk_be_replace, chunk_be_attach): a chunk it
                                * lacks is read around, as if it were DOWN, until chunk_be_settle finished clean */
};
const char *chunk_store_state_name(int state);

/* How a backend opens. min_up is the number of stores that must be UP: the caller derives it from its geometry, at
 * least k' of every object it holds (so each stays readable; with m = 0 every store), and for a cluster or a tier
 * that is the stores minus m, not less. A store that cannot be used becomes DOWN with its reason (one line on stderr
 * naming the index, the path and the reason); fewer than min_up UP is TSDB_ERR_UNAVAILABLE naming the down ones.
 * Never DOWN, always a failed open: CHUNK_ERR_BUSY (another owner: two owners would write the fence back lower),
 * and the same directory or store id twice (TSDB_ERR_INVALID). min_up 0: any number of stores, even none, may be up. */
typedef struct chunk_be_opts {
    int min_up;
} chunk_be_opts;
/* The stores at paths[0..n); ids (n x 16 bytes) or NULL as chunk_store_open's expect_id. The fence of a volume is the
 * maximum over the stores that are UP at open: a DOWN store may have seen a higher epoch, so the caller gives the
 * authoritative one (chunk_be_see_epoch) after the open, and chunk_be_attach / replace raise a returning store to it.
 * A path that is NULL is a store the caller knows is down (DOWN_MISSING): puts to it are TSDB_ERR_UNAVAILABLE; reads,
 * deletes, listings and epoch raises skip it. */
int chunk_be_open2(chunk_backend **out, const char *const *paths, int n, const uint8_t *ids, const chunk_be_opts *o);
/* The strict open: every named path must open (a disk that came back as another one, or is damaged, fails it and is
 * never dropped quietly); only a NULL path is down. */
int chunk_be_open(chunk_backend **out, const char *const *paths, int n, const uint8_t *ids);
void chunk_be_close(chunk_backend *be);
int chunk_be_nstores(const chunk_backend *be);
chunk_store *chunk_be_store(chunk_backend *be, int i); /* NULL while DOWN */
int chunk_be_store_state(const chunk_backend *be, int i); /* CHUNK_STORE_*; -1 for an index out of range */
uint64_t chunk_be_epoch(chunk_backend *be, uint32_t volume);
/* A request showed epoch e: the fence is at least e, durable in every store before it returns. If a store fails to
 * store it, the first such error is returned even though the others hold it (the fence is in memory and in the stores
 * that succeeded; the caller retries or reopens: a failed store acknowledges nothing, so it is skipped from then on).
 * Authoritative like chunk_store_raise: it clears an unsure volume. */
int chunk_be_see_epoch(chunk_backend *be, uint32_t volume, uint64_t e);
/* The store a chunk is put in when the caller names none: (hash of oid + pos) mod n. */
int chunk_be_place(const chunk_backend *be, const uint8_t oid[16], unsigned pos);
/* store < 0: chunk_be_place. A put whose store is DOWN is TSDB_ERR_UNAVAILABLE, nothing written: a degraded backend
 * refuses what it cannot place, and E10's degraded settle (k' + m - 1 positions, never fewer) is the cluster's rule
 * (R7/R9), not made here: the caller that wants it passes a posmask of the positions it can place to
 * chunk_be_put_object, and heals the rest when the store is back. A REPLACING store takes puts (its fence is already
 * up), so a long fill does not stop the writers. */
int chunk_be_put(chunk_backend *be, int store, const chunk_ident *id, const chunk_geom *g, uint64_t epoch,
                 const void *cells, const uint64_t *sums);
int chunk_be_wr_open(chunk_backend *be, int store, const chunk_ident *id, const chunk_geom *g, uint64_t epoch,
                     chunk_wr **out);
/* Opens the chunk wherever it is: the first store with a valid one, else the worst answer of the others. */
int chunk_be_rd_open(chunk_backend *be, const chunk_ident *id, chunk_rd **out);
/* In every store that is open and has it. A DOWN store keeps its copy (an orphan once it comes back: its object has
 * fewer than k' positions left, which heal never rebuilds from; E14 removes it, never this layer). */
int chunk_be_del(chunk_backend *be, const chunk_ident *id, uint64_t epoch);
int chunk_be_list(chunk_backend *be, uint32_t volume, bool verify, chunk_list_fn fn, void *ctx);

/* ---------------------------------------------------------------- heal, attach, replace (§5.5, §5.9, E55) */
/* A chunk that is missing or damaged in a store is rebuilt, never edited: the position's cells are decoded from k'
 * good positions of the object, every rebuilt cell is checked against the encode-time sums of the chunks that
 * survive (the object is re-encoded stripe by stripe and each surviving chunk's own sums table must agree with it; a
 * set of chunks that are not one encoding is refused and nothing is written), written as a new chunk file (tmp,
 * fsync, rename over the old name, directory fsync: after a power loss the old file or the whole new one), at the
 * epoch the object's chunks were written at (the header says so: a delete below it stays refused), then read back
 * and verified. Which positions a store holds is the placement's: (chunk_be_place + pos) mod n, by index. */
typedef struct chunk_heal_opts {
    uint64_t epoch; /* the caller's epoch: below the volume's fence the heal is refused (CHUNK_ERR_FENCED) like any put,
                     * above it the fence is raised first. The rebuilt chunks keep their own writer epoch. */
    bool scrub;     /* read every cell of the target's chunks and check its sum (else header, size, sums table, footer) */
    /* Optional: whether the caller's records still have the object. A heal never resurrects what the caller deleted
     * (checked again just before each chunk is named). Without it every object with k' good positions is healed. */
    int (*live)(void *ctx, const uint8_t oid[16]);
    void *live_ctx;
} chunk_heal_opts;

typedef struct chunk_heal_report {
    uint64_t objects;          /* objects with a position on the store that were looked at */
    uint64_t chunks_ok;        /* positions found whole (not touched) */
    uint64_t rebuilt_missing;  /* chunks made that were absent */
    uint64_t rebuilt_damaged;  /* chunks replaced that did not parse, or (scrub) had a cell failing its sum */
    uint64_t unrecoverable;    /* objects that need a rebuild and have fewer than k' good positions even counting the
                                * positions on DOWN stores: nothing is written for them, what is there is kept. This also
                                * counts the remains of a deleted object (E14's orphans: never rebuilt) */
    uint64_t unavailable;      /* objects that need a rebuild and could have it once a DOWN store (one other than the
                                * target, and not a REPLACING one: what those lack is what heals make) is back: E15's
                                * unavailable, not lost */
    uint64_t not_live;         /* objects the caller's `live` does not have: skipped */
    uint64_t failed;           /* chunks or objects that failed (I/O, the chunks disagree, the read-back differed): the
                                * old file is still there, a second heal retries */
    uint64_t bytes;            /* chunk bytes written */
} chunk_heal_report;

/* Heals one volume of the store at index `store` (UP or REPLACING). The objects are those that the listings of the OTHER
 * stores name (a chunk found only here is an orphan: never a reason to rebuild), geometry and epoch from their
 * headers. 0 when it ran through, whatever the report says; a negative code when it could not start (the store is
 * DOWN or failed, the epoch is fenced or unsure) or a listing failed. Meant for a quiet volume: a put in flight may be
 * counted unrecoverable or rebuilt twice with the same bytes. A deletion in flight must be the caller's to order
 * (`live`). Never fabricated, never resurrected: an object with fewer than k' good positions is not rebuilt (the remains
 * of an object deleted while a store was away are orphans, E14's), but for k' = 1 a single copy decodes, so the layer
 * cannot tell an orphan from a survivor: a caller that deletes objects passes `live`.
 * The listing is taken once, from the stores that are open: an object that only a DOWN store lists is not seen (it is
 * neither unavailable nor unrecoverable here; chunk_be_settle therefore does not promote while another store is DOWN),
 * and a degraded put (a posmask that skipped this store) that lands after the listing is not seen either: whoever
 * puts degraded heals the store again after it is UP. */
int chunk_be_heal(chunk_backend *be, uint32_t volume, int store, const chunk_heal_opts *o, chunk_heal_report *rep);
/* The chunk `id` (volume, vuuid, oid, pos) in the store its position is placed on, which a read or a scrub found
 * damaged: always checks its cells. 0 with rep->chunks_ok = 1 if it is whole, else as heal. */
int chunk_be_repair(chunk_backend *be, const chunk_ident *id, const chunk_heal_opts *o, chunk_heal_report *rep);
/* Heals every volume the backend knows on the store; a REPLACING store becomes UP only if nothing was unavailable
 * or failed and no other store is DOWN (objects that only that store lists were never seen; a REPLACING one does not
 * hold it back, so two stores replaced in turn settle: call settle on the first again once the second is UP;
 * objects past repair, `unrecoverable`, do not hold it back: a full set of other stores could not give
 * them either). opts->epoch 0 means each volume's current fence (the backend acting on its own account). 0 when the
 * store is UP at the end; TSDB_ERR_UNAVAILABLE when it is still REPLACING (filled as far as the other stores allow). */
int chunk_be_settle(chunk_backend *be, int store, const chunk_heal_opts *o, chunk_heal_report *rep);
/* A DOWN store (DOWN_* or failed) comes back at `path`, which must be the same store (id: 16 bytes, or NULL for the
 * caller's own check; a store with another id is refused, CHUNK_ERR_STORE_ID). Every volume's fence is raised in it
 * before anything else, it becomes REPLACING and is settled: a chunk written while it was away is read around until
 * the heal has made it. A store that failed in service (chunk_store_failed) is replaced or the backend reopened;
 * attach refuses it (TSDB_ERR_INVALID: its descriptors cannot be trusted). */
int chunk_be_attach(chunk_backend *be, int index, const char *path, const uint8_t *id, const chunk_heal_opts *o,
                    chunk_heal_report *rep);
/* The store at `index` is lost for good. A new, empty store is made at new_path (chunk_store_init: a directory that
 * has a META is refused), opened, given every volume's fence (durable, before a chunk is written to it), put in the
 * place (REPLACING: reads treat what it lacks as DOWN) and settled. new_id_out gets its id: the caller records it
 * (the TIERS file, later the metadata group); the index is unchanged, so the new store takes exactly the old one's
 * positions. Refused (TSDB_ERR_INVALID) for a store that is UP. 0 when it is UP at the end; TSDB_ERR_UNAVAILABLE when
 * it was filled as far as the other stores allow and stays REPLACING (call chunk_be_settle again later); the id is
 * given in both cases. The new store holds every fence in its chunk writes from the first byte; its EPOCH file may be
 * one fence behind for the moment between the raise and the install (a chunk_be_see_epoch in between reaches the open
 * stores only), which nothing relies on: wr_open raises it before a chunk is written. A replace that died halfway leaves new_path behind: replace again at another path. */
int chunk_be_replace(chunk_backend *be, int index, const char *new_path, uint8_t new_id_out[16], const chunk_heal_opts *o,
                     chunk_heal_report *rep);

/* ---------------------------------------------------------------- encode */
/* Called for every cell of every position, stripe by stripe: `idx` is the cell's index within chunk `pos`, `sum` its
 * xxh64 (the encode-time sum of E26). Non-zero aborts the encode and is returned. */
typedef int (*chunk_cell_fn)(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum);
/* Encodes obj[0 .. g->len): one stripe's cells at a time in memory ((k'+m) cells). sums_out (chunk_sums_total
 * entries, or NULL) gets the object record's sums. TSDB_ERR_INVALID without an erasure codec in this build. */
int chunk_encode(const chunk_geom *g, const uint8_t *obj, chunk_cell_fn fn, void *ctx, uint64_t *sums_out);
/* Encodes and puts every position whose bit is set in posmask (all: ~0u) through the backend, position p in store
 * (chunk_be_place + p) mod n (with fewer stores than k' + m positions share a store, and losing it loses more than m
 * of them: only tests do that; R7 passes at least k' + m). Each chunk is durable when this returns 0, and a failure
 * aborts the unfinished ones (those already committed stay: chunks are immutable and an orphan is E14's business). */
int chunk_be_put_object(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                        const chunk_geom *g, uint64_t epoch, const uint8_t *obj, unsigned posmask, uint64_t *sums_out);

/* ---------------------------------------------------------------- read, with decode */
/* Where an object's chunks are read from: files (chunk_obj below), memory in tests and fuzzing, the peers in R8. */
typedef struct chunk_source {
    void *ctx;
    char name[40]; /* the object, for messages (its oid in hex) */
    /* `len` bytes of the cell area of chunk `pos` at `off`: 0, or CHUNK_ST_ABSENT, DAMAGED or IOERR. */
    int (*read)(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len);
    /* The chunk's own sums table (chunk_pos_cells entries), valid as long as the source is; NULL with *status set to
     * CHUNK_ST_ABSENT, DAMAGED or IOERR when the chunk has none to give. */
    const uint64_t *(*sums)(void *ctx, unsigned pos, int *status);
} chunk_source;

typedef struct chunk_report {
    uint8_t st[CHUNK_POS_MAX]; /* the last status seen at each position */
    unsigned bad_cells;        /* cells that failed their sums or could not be read */
    unsigned decoded_cells;    /* cells rebuilt from others */
} chunk_report;

enum {
    CHUNK_RR_RAW = 1, /* read only the bytes asked for, from the same byte range of the others when one is bad: the
                       * caller checks them (a granule's crc, E11) and asks again without this flag when they fail */
};

/* Reads obj[off .. off+len) into buf. Without CHUNK_RR_RAW every cell used is read whole and must match its sum (the
 * object record's rec_sums when given, which also catches a chunk of another encoding, else the chunk's own table):
 * a cell that is absent, unreadable or fails is an erasure and its stripe is rebuilt from any k' good cells, data
 * first. 0, or TSDB_ERR_INVALID (a range outside the object), or when a stripe has fewer than k' good cells
 * (a line on stderr names the object; the bytes are never wrong, only absent): TSDB_ERR_UNAVAILABLE if the source
 * reported CHUNK_ST_DOWN for enough of the missing positions that the stripe would be readable with them (the store is
 * merely away: E15 never calls that lost), else TSDB_ERR_CORRUPT. */
int chunk_read_range(const chunk_source *src, const chunk_geom *g, const uint64_t *rec_sums, uint64_t off,
                     uint64_t len, void *buf, unsigned flags, chunk_report *rep);

/* An object in a backend: geometry from the chunks' headers (or `expect`, which each header must match), one open
 * chunk per position found. A position that is not found and is placed (chunk_be_place) on a store that is not UP
 * reads as CHUNK_ST_DOWN, not absent. Nothing found at all is TSDB_ERR_NOTFOUND while every store is UP, and
 * TSDB_ERR_UNAVAILABLE while one is not (the object may be wholly on the store that is away). */
typedef struct chunk_obj chunk_obj;
int chunk_obj_open(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                   const chunk_geom *expect, const uint64_t *rec_sums, chunk_obj **out);
void chunk_obj_close(chunk_obj *o);
const chunk_geom *chunk_obj_geom(const chunk_obj *o);
int chunk_obj_read(chunk_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep);
int chunk_obj_source(chunk_obj *o, chunk_source *src); /* for chunk_read_range over the same chunks */

/* ---------------------------------------------------------------- tests */
/* Planted bugs: a test sets chunk_mutant and must then fail (tests/test_chunk.c --mutant NAME; the Makefile requires it). */
enum { CHUNK_MUT_NONE = 0, CHUNK_MUT_NOSUMS, CHUNK_MUT_NOFENCE, CHUNK_MUT_EOF, CHUNK_MUT_SETTINGS, CHUNK_MUT_NOSYNC,
       CHUNK_MUT_EPOCH_NOSYNC,
       /* the heal tests (tests/test_chunk_heal.c) */
       CHUNK_MUT_HEAL_NOVERIFY,    /* a rebuilt chunk is neither cross-checked nor read back */
       CHUNK_MUT_HEAL_EPOCH0,      /* rebuilt chunks are written at epoch 0, not the object's */
       CHUNK_MUT_REPLACE_LATE,     /* the new store's EPOCH is raised after its first chunk, not before */
       CHUNK_MUT_OPEN_BUSY_DOWN,   /* a store another owner holds is taken as DOWN */
       CHUNK_MUT_OPEN_ID_UP,       /* a store with another id is taken as UP */
       CHUNK_MUT_HEAL_KMINUS1,     /* a rebuild from k' - 1 good positions is attempted */
       CHUNK_MUT_HEAL_RESURRECT,   /* the caller's `live` is never asked */
       CHUNK_MUT_HEAL_COUNT_TARGET, /* the target's own absent positions count as a store that is away */
       CHUNK_MUT_SETTLE_UP_ALWAYS  /* a REPLACING store goes UP whatever the heal said */ };
extern int chunk_mutant;
/* Non-zero: chunk_read_range and chunk_obj_open do not log (a fuzz harness provokes the refusal by the million). */
extern int chunk_log_quiet;
/* Fault injection for tests: the nth following pread of the chunk files / the nth readdir fails with EIO (0: off). */
extern int chunk_fault_read, chunk_fault_readdir;
/* The nth chunk a heal has just rebuilt gets one byte of its first cell flipped before the read-back (0: off). */
extern int chunk_fault_heal_flip;
/* "Today's settings", used only by CHUNK_MUT_SETTINGS (a reader that takes k and m from them, not from the object). */
extern unsigned chunk_settings_k, chunk_settings_m;

#endif
