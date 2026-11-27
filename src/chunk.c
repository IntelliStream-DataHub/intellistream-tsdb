/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The geometry of an object, the chunk file format and the encoder (docs/design-cluster-data.md §2.2, E7, E26; the
 * API is src/chunk.h).
 *
 * The geometry is a few integers, recorded in the object record and in every chunk header, and everything else is
 * computed from them here, in one place: which cell is in which chunk, how long each is, where a chunk's sums are.
 * Nothing reads "today's" k and m after an object was written (E2): a reader that did would take a 4+2 object for an
 * 8+2 one the day the setting changes.
 *
 * A chunk file is the header, the cells, one xxh64 per cell, and a footer that hashes the header and the sums. The
 * parser here is a pure function of the bytes (chunk_image_parse); the file reader (chunkstore.c) does the same piece
 * by piece, so that opening a chunk costs two small reads and not the file. Nothing is accepted that
 * chunk_image_build would not write byte for byte: reserved fields are zero, the lengths are what the geometry gives,
 * and the file is exactly as long as they say (fuzz_chunk checks that round trip).
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef __APPLE__
#include <sys/random.h>
#endif
#include <unistd.h>

#ifdef TSDB_EC
#include "ec/rs.h"
#endif

int chunk_mutant;
int chunk_fault_read, chunk_fault_readdir, chunk_fault_write, chunk_fault_heal_flip;
int chunk_log_quiet;
unsigned chunk_settings_k, chunk_settings_m;

/* ---------------------------------------------------------------- geometry */
bool chunk_geom_valid(const chunk_geom *g)
{
    if (g->k < 1 || g->k > CHUNK_K_MAX || g->m > CHUNK_M_MAX || (g->m == 0 && g->k != 1) || g->cell < 1 ||
        g->cell > CHUNK_CELL_LIMIT || g->len < 1 || g->len > CHUNK_OBJ_MAX)
        return false;
    uint64_t cells = (g->len + g->cell - 1) / g->cell;
    return g->k <= cells && cells <= CHUNK_NCELLS_MAX;
}

int chunk_geom_make(chunk_geom *g, unsigned k, unsigned m, uint32_t cell, uint64_t len)
{
    if (k > 0xffff || m > 0xffff)
        return TSDB_ERR_INVALID;
    chunk_geom t = {.k = (uint16_t)k, .m = (uint16_t)m, .cell = cell, .len = len};
    if (!chunk_geom_valid(&t))
        return TSDB_ERR_INVALID;
    *g = t;
    return 0;
}

int chunk_geom_derive(chunk_geom *g, unsigned k, unsigned m, uint64_t len)
{
    if (k < 1 || k > CHUNK_K_MAX || m > CHUNK_M_MAX || (m == 0 && k != 1) || len < 1 || len > CHUNK_OBJ_MAX)
        return TSDB_ERR_INVALID;
    uint64_t c = (len / (8ull * k) + 4095) & ~4095ull;
    if (c < CHUNK_CELL_MIN)
        c = CHUNK_CELL_MIN;
    if (c > CHUNK_CELL_MAX)
        c = CHUNK_CELL_MAX;
    uint64_t cells = (len + c - 1) / c;
    return chunk_geom_make(g, cells < k ? (unsigned)cells : k, m, (uint32_t)c, len);
}

uint64_t chunk_ncells(const chunk_geom *g) { return (g->len + g->cell - 1) / g->cell; }
uint64_t chunk_nstripes(const chunk_geom *g) { return (chunk_ncells(g) + g->k - 1) / g->k; }
unsigned chunk_npos(const chunk_geom *g) { return (unsigned)g->k + g->m; }

uint64_t chunk_cell_len(const chunk_geom *g, uint64_t c)
{
    uint64_t at = c * g->cell;
    if (at >= g->len)
        return 0;
    return g->len - at < g->cell ? g->len - at : g->cell;
}

uint64_t chunk_stripe_len(const chunk_geom *g, uint64_t s) { return chunk_cell_len(g, s * g->k); }

uint64_t chunk_pos_cells(const chunk_geom *g, unsigned pos)
{
    if (pos >= chunk_npos(g))
        return 0;
    if (pos >= g->k)
        return chunk_nstripes(g);
    uint64_t n = chunk_ncells(g);
    return n > pos ? (n - pos + g->k - 1) / g->k : 0;
}

uint64_t chunk_pos_len(const chunk_geom *g, unsigned pos)
{
    uint64_t n = chunk_pos_cells(g, pos);
    if (n == 0)
        return 0;
    if (pos >= g->k) { /* every stripe's parity is as long as its first data cell: all full but the last stripe's */
        uint64_t s = chunk_nstripes(g);
        return (s - 1) * g->cell + chunk_stripe_len(g, s - 1);
    }
    uint64_t last = chunk_ncells(g) - 1, short_by = g->cell - chunk_cell_len(g, last);
    return n * g->cell - (last % g->k == pos ? short_by : 0);
}

uint64_t chunk_file_size(const chunk_geom *g, unsigned pos)
{
    return CHUNK_HDR_BYTES + chunk_pos_len(g, pos) + 8 * chunk_pos_cells(g, pos) + CHUNK_FTR_BYTES;
}

uint64_t chunk_sums_total(const chunk_geom *g) { return chunk_ncells(g) + (uint64_t)g->m * chunk_nstripes(g); }

uint64_t chunk_sums_base(const chunk_geom *g, unsigned pos)
{
    uint64_t b = 0;
    for (unsigned p = 0; p < pos && p < chunk_npos(g); p++)
        b += chunk_pos_cells(g, p);
    return b;
}

/* The length of cell `i` of chunk `pos`. */
static uint64_t pos_cell_len(const chunk_geom *g, unsigned pos, uint64_t i)
{
    return pos >= g->k ? chunk_stripe_len(g, i) : chunk_cell_len(g, i * g->k + pos);
}

/* ---------------------------------------------------------------- identity */
void chunk_oid_hex(const uint8_t oid[16], char out[33])
{
    static const char d[] = "0123456789abcdef";
    for (int i = 0; i < 16; i++) {
        out[2 * i] = d[oid[i] >> 4];
        out[2 * i + 1] = d[oid[i] & 15];
    }
    out[32] = 0;
}

int chunk_random(void *buf, size_t n)
{
    uint8_t *p = buf;
    size_t left = n;
    while (left > 0) {
        ssize_t r = getrandom(p, left, 0);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            if (errno == ENOSYS)
                break;
            return TSDB_ERR_IO;
        }
        if (r == 0)
            return TSDB_ERR_IO;
        p += r;
        left -= (size_t)r;
    }
    if (left == 0)
        return 0;
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    while (left > 0) {
        ssize_t r = read(fd, p, left);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            close(fd);
            return TSDB_ERR_IO;
        }
        p += r;
        left -= (size_t)r;
    }
    close(fd);
    return 0;
}

/* ---------------------------------------------------------------- the file format */
static void put16(uint8_t *p, uint64_t v) { p[0] = (uint8_t)v, p[1] = (uint8_t)(v >> 8); }
static void put32(uint8_t *p, uint64_t v) { put16(p, v), put16(p + 2, v >> 16); }
static void put64(uint8_t *p, uint64_t v) { put32(p, v), put32(p + 4, v >> 32); }
static uint64_t get16(const uint8_t *p) { return (uint64_t)p[0] | (uint64_t)p[1] << 8; }
static uint64_t get32(const uint8_t *p) { return get16(p) | get16(p + 2) << 16; }
static uint64_t get64(const uint8_t *p) { return get32(p) | get32(p + 4) << 32; }

static const uint8_t HDR_MAGIC[8] = {'T', 'S', 'D', 'B', 'C', 'H', 'K', '1'};
static const uint8_t FTR_MAGIC[4] = {'C', 'H', 'K', 'E'};

void chunk_hdr_encode(const chunk_hdr *h, uint8_t out[CHUNK_HDR_BYTES])
{
    memset(out, 0, CHUNK_HDR_BYTES);
    memcpy(out, HDR_MAGIC, 8);
    put32(out + 8, CHUNK_VERSION);
    put32(out + 12, h->flags);
    memcpy(out + 16, h->vuuid, 16);
    memcpy(out + 32, h->oid, 16);
    put64(out + 48, h->epoch);
    put16(out + 64, h->g.k);
    put16(out + 66, h->g.m);
    put16(out + 68, h->pos);
    put32(out + 72, h->g.cell);
    put64(out + 80, h->g.len);
    put64(out + 88, h->chunk_len);
}

int chunk_hdr_decode(const uint8_t in[CHUNK_HDR_BYTES], chunk_hdr *h)
{
    if (memcmp(in, HDR_MAGIC, 8) != 0 || get32(in + 8) != CHUNK_VERSION || get32(in + 12) != 0 ||
        get64(in + 56) != 0 || get16(in + 70) != 0 || get32(in + 76) != 0)
        return TSDB_ERR_CORRUPT;
    chunk_hdr t;
    memset(&t, 0, sizeof t);
    memcpy(t.vuuid, in + 16, 16);
    memcpy(t.oid, in + 32, 16);
    t.epoch = get64(in + 48);
    t.g.k = (uint16_t)get16(in + 64);
    t.g.m = (uint16_t)get16(in + 66);
    t.pos = (uint16_t)get16(in + 68);
    t.g.cell = (uint32_t)get32(in + 72);
    t.g.len = get64(in + 80);
    t.chunk_len = get64(in + 88);
    if (!chunk_geom_valid(&t.g) || t.pos >= chunk_npos(&t.g) || t.chunk_len != chunk_pos_len(&t.g, t.pos))
        return TSDB_ERR_CORRUPT;
    *h = t;
    return 0;
}

/* The footer's hash: xxh64 (seed 0) over the header and the sums table as they lie in the file (little endian),
 * streamed directly without allocating or copying. */
static const uint64_t FTR_P1 = 11400714785074694791ULL;
static const uint64_t FTR_P2 = 14029467366897019727ULL;
static const uint64_t FTR_P3 = 1609587929392839161ULL;
static const uint64_t FTR_P4 = 9650029242287828579ULL;

static inline uint64_t ftr_rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static inline uint64_t ftr_round(uint64_t acc, uint64_t in) { return ftr_rotl64(acc + in * FTR_P2, 31) * FTR_P1; }
static inline uint64_t ftr_merge(uint64_t acc, uint64_t v) { return (acc ^ ftr_round(0, v)) * FTR_P1 + FTR_P4; }

static uint64_t ftr_hash(const uint8_t *hdr, const uint8_t *table, uint64_t ncells)
{
    _Static_assert(CHUNK_HDR_BYTES >= 32 && CHUNK_HDR_BYTES % 32 == 0,
                   "CHUNK_HDR_BYTES must be a positive multiple of 32");
    uint64_t v1 = FTR_P1 + FTR_P2, v2 = FTR_P2, v3 = 0, v4 = (uint64_t)0 - FTR_P1;
    for (size_t off = 0; off < CHUNK_HDR_BYTES; off += 32) {
        v1 = ftr_round(v1, get64(hdr + off));
        v2 = ftr_round(v2, get64(hdr + off + 8));
        v3 = ftr_round(v3, get64(hdr + off + 16));
        v4 = ftr_round(v4, get64(hdr + off + 24));
    }
    uint64_t i = 0;
    for (; i + 4 <= ncells; i += 4) {
        const uint8_t *p = table + 8 * i;
        v1 = ftr_round(v1, get64(p));
        v2 = ftr_round(v2, get64(p + 8));
        v3 = ftr_round(v3, get64(p + 16));
        v4 = ftr_round(v4, get64(p + 24));
    }
    uint64_t h = ftr_rotl64(v1, 1) + ftr_rotl64(v2, 7) + ftr_rotl64(v3, 12) + ftr_rotl64(v4, 18);
    h = ftr_merge(h, v1);
    h = ftr_merge(h, v2);
    h = ftr_merge(h, v3);
    h = ftr_merge(h, v4);
    h += (uint64_t)CHUNK_HDR_BYTES + 8 * ncells;
    for (; i < ncells; i++)
        h = ftr_rotl64(h ^ ftr_round(0, get64(table + 8 * i)), 27) * FTR_P1 + FTR_P4;
    h ^= h >> 33;
    h *= FTR_P2;
    h ^= h >> 29;
    h *= FTR_P3;
    h ^= h >> 32;
    return h;
}

/* Checks the footer (16 bytes) against header bytes and sums table; 0 or TSDB_ERR_CORRUPT. */
int chunk_ftr_check(const uint8_t *hdr, const uint8_t *table, uint64_t ncells, const uint8_t ftr[CHUNK_FTR_BYTES])
{
    if (memcmp(ftr + 12, FTR_MAGIC, 4) != 0 || get32(ftr + 8) != ncells)
        return TSDB_ERR_CORRUPT;
    return ftr_hash(hdr, table, ncells) == get64(ftr) ? 0 : TSDB_ERR_CORRUPT;
}

int chunk_image_parse(const uint8_t *buf, size_t n, bool check_cells, chunk_hdr *h, const uint8_t **cells,
                      const uint8_t **sums_le)
{
    if (n < CHUNK_HDR_BYTES + CHUNK_FTR_BYTES)
        return TSDB_ERR_CORRUPT;
    chunk_hdr t;
    if (chunk_hdr_decode(buf, &t) != 0)
        return TSDB_ERR_CORRUPT;
    uint64_t nc = chunk_pos_cells(&t.g, t.pos);
    if (n != chunk_file_size(&t.g, t.pos))
        return TSDB_ERR_CORRUPT;
    const uint8_t *table = buf + CHUNK_HDR_BYTES + t.chunk_len;
    if (chunk_ftr_check(buf, table, nc, buf + n - CHUNK_FTR_BYTES) != 0)
        return TSDB_ERR_CORRUPT;
    if (check_cells) {
        uint64_t off = 0;
        for (uint64_t i = 0; i < nc; i++) {
            uint64_t cl = pos_cell_len(&t.g, t.pos, i);
            if (xxh64(buf + CHUNK_HDR_BYTES + off, cl, 0) != get64(table + 8 * i))
                return TSDB_ERR_CORRUPT;
            off += cl;
        }
    }
    *h = t;
    if (cells)
        *cells = buf + CHUNK_HDR_BYTES;
    if (sums_le)
        *sums_le = table;
    return 0;
}

uint8_t *chunk_image_build(const chunk_hdr *h, const uint8_t *cells, const uint64_t *sums, size_t *n)
{
    uint64_t nc = chunk_pos_cells(&h->g, h->pos);
    size_t total = CHUNK_HDR_BYTES + h->chunk_len + 8 * nc + CHUNK_FTR_BYTES;
    uint8_t *b = malloc(total);
    if (!b)
        return NULL;
    chunk_hdr_encode(h, b);
    memcpy(b + CHUNK_HDR_BYTES, cells, h->chunk_len);
    uint8_t *table = b + CHUNK_HDR_BYTES + h->chunk_len;
    for (uint64_t i = 0; i < nc; i++)
        put64(table + 8 * i, sums[i]);
    uint8_t *f = b + total - CHUNK_FTR_BYTES;
    put64(f, ftr_hash(b, table, nc));
    put32(f + 8, nc);
    memcpy(f + 12, FTR_MAGIC, 4);
    *n = total;
    return b;
}

/* little-endian helpers for chunkstore.c */
uint64_t chunk_le64(const uint8_t *p) { return get64(p); }
void chunk_put_le64(uint8_t *p, uint64_t v) { put64(p, v); }
void chunk_ftr_make(uint8_t ftr[CHUNK_FTR_BYTES], const uint8_t *hdr, const uint8_t *table, uint64_t ncells)
{
    put64(ftr, ftr_hash(hdr, table, ncells));
    put32(ftr + 8, ncells);
    memcpy(ftr + 12, FTR_MAGIC, 4);
}
uint64_t chunk_pos_cell_len(const chunk_geom *g, unsigned pos, uint64_t i) { return pos_cell_len(g, pos, i); }

/* ---------------------------------------------------------------- encode */
/* One codec per (k, m): immutable once built, shared by every thread. */
#ifdef TSDB_EC
static iso_rs *rs_cache[CHUNK_K_MAX + 1][CHUNK_M_MAX + 1];

void *chunk_rs_get(unsigned k, unsigned m)
{
    /* The codec's kernels are chosen by src/cpu.c's constructor (tsdb_ec_select). Naming a symbol of cpu.o makes the
     * linker take it from libtsdb.a for a program that uses nothing else of it, so that constructor runs. */
    (void)cpu_level_max();
    iso_rs **slot = &rs_cache[k][m];
    iso_rs *r = __atomic_load_n(slot, __ATOMIC_ACQUIRE);
    if (!r) {
        iso_rs *n = iso_rs_new(k, m);
        iso_rs *exp = NULL;
        if (!n)
            return NULL;
        if (__atomic_compare_exchange_n(slot, &exp, n, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            r = n;
        else {
            iso_rs_free(n);
            r = exp;
        }
    }
    return r;
}
#else
void *chunk_rs_get(unsigned k, unsigned m)
{
    (void)k, (void)m;
    return NULL;
}
#endif

#ifdef TSDB_EC
/* The data shards of stripe s from `cells` (the bytes of its first data cell): a full-length cell is read in place,
 * a short or missing one is copied into mem and padded with zeros (never stored). */
static void data_shards(const chunk_geom *g, uint64_t s, const uint8_t *cells, uint8_t *mem, size_t cap,
                        uint8_t **shards)
{
    uint64_t ncells = chunk_ncells(g);
    size_t ls = (size_t)chunk_stripe_len(g, s);
    for (unsigned i = 0; i < g->k; i++) {
        uint64_t c = s * g->k + i, cl = chunk_cell_len(g, c);
        if (c < ncells && cl == ls) {
            shards[i] = (uint8_t *)(uintptr_t)(cells + (size_t)i * g->cell);
            continue;
        }
        shards[i] = mem + (size_t)i * cap;
        if (cl)
            memcpy(shards[i], cells + (size_t)i * g->cell, (size_t)cl);
        memset(shards[i] + cl, 0, ls - (size_t)cl);
    }
}

/* Stripe s: `cells` points at the bytes of its first data cell (cell s * k); mem is (k + m) * cap bytes of scratch
 * (unused when m = 0). The parity is computed from `pcells`: `cells`, except in CHUNK_MUT_STREAM_REREAD (a second read
 * of the source). */
static int encode_stripe(const chunk_geom *g, iso_rs *rs, uint64_t s, const uint8_t *cells, const uint8_t *pcells,
                         uint8_t *mem, size_t cap, chunk_cell_fn fn, void *ctx, uint64_t *sums_out,
                         const uint64_t *bases)
{
    unsigned k = g->k, m = g->m;
    if (m == 0) { /* single-copy (D5, k' = 1): one cell, encode-time sum, no RS codec call */
        size_t len = (size_t)chunk_cell_len(g, s);
        uint64_t sum = xxh64(cells, len, 0);
        if (sums_out)
            sums_out[s] = sum;
        return fn ? fn(ctx, 0, s, cells, len, sum) : 0;
    }
    size_t ls = (size_t)chunk_stripe_len(g, s);
    uint8_t *shards[CHUNK_POS_MAX];
    data_shards(g, s, pcells, mem, cap, shards);
    for (unsigned j = 0; j < m; j++)
        shards[k + j] = mem + (size_t)(k + j) * cap;
    iso_rs_encode(rs, shards, ls);
    if (pcells != cells) /* planted: the data cells are what the first read gave, the parity what the second gave */
        data_shards(g, s, cells, mem, cap, shards);
    int rc = 0;
    for (unsigned p = 0; p < k + m && rc == 0; p++) {
        size_t len = p < k ? (size_t)chunk_cell_len(g, s * k + p) : ls;
        if (len == 0)
            continue; /* a data cell past the end of the object */
        uint64_t sum = xxh64(shards[p], len, 0);
        if (sums_out)
            sums_out[bases[p] + s] = sum;
        if (fn)
            rc = fn(ctx, p, s, shards[p], len, sum);
    }
    return rc;
}
#endif

int chunk_encode(const chunk_geom *g, const uint8_t *obj, chunk_cell_fn fn, void *ctx, uint64_t *sums_out)
{
#ifndef TSDB_EC
    (void)g, (void)obj, (void)fn, (void)ctx, (void)sums_out;
    return TSDB_ERR_INVALID; /* this build has no erasure codec (it is x86-64 only) */
#else
    if (!chunk_geom_valid(g))
        return TSDB_ERR_INVALID;
    unsigned k = g->k, m = g->m;
    iso_rs *rs = m ? chunk_rs_get(k, m) : NULL;
    if (m && !rs)
        return TSDB_ERR_NOMEM;
    uint64_t ns = chunk_nstripes(g), bases[CHUNK_POS_MAX];
    for (unsigned p = 0; p < k + m; p++)
        bases[p] = chunk_sums_base(g, p);
    size_t cap = g->cell > g->len ? (size_t)g->len : g->cell; /* the longest cell */
    uint8_t *mem = m ? malloc((size_t)(k + m) * cap) : NULL;
    if (m && !mem)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (uint64_t s = 0; s < ns && rc == 0; s++)
        rc = encode_stripe(g, rs, s, obj + s * k * g->cell, obj + s * k * g->cell, mem, cap, fn, ctx, sums_out, bases);
    free(mem);
    return rc;
#endif
}

int chunk_encode_stripe(const chunk_geom *g, uint64_t s, const uint8_t *stripe, chunk_cell_fn fn, void *ctx)
{
#ifndef TSDB_EC
    (void)g, (void)s, (void)stripe, (void)fn, (void)ctx;
    return TSDB_ERR_INVALID;
#else
    if (!chunk_geom_valid(g) || s >= chunk_nstripes(g))
        return TSDB_ERR_INVALID;
    iso_rs *rs = g->m ? chunk_rs_get(g->k, g->m) : NULL;
    if (g->m && !rs)
        return TSDB_ERR_NOMEM;
    size_t cap = g->cell > g->len ? (size_t)g->len : g->cell;
    uint8_t *mem = g->m ? malloc((size_t)(g->k + g->m) * cap) : NULL;
    if (g->m && !mem)
        return TSDB_ERR_NOMEM;
    int rc = encode_stripe(g, rs, s, stripe, stripe, mem, cap, fn, ctx, NULL, NULL);
    free(mem);
    return rc;
#endif
}

int chunk_encode_stream(const chunk_geom *g, const chunk_put_src *src, chunk_cell_fn fn, void *ctx, uint64_t *sums_out)
{
#ifndef TSDB_EC
    (void)g, (void)src, (void)fn, (void)ctx, (void)sums_out;
    return TSDB_ERR_INVALID;
#else
    if (!chunk_geom_valid(g) || !src || !src->fill)
        return TSDB_ERR_INVALID;
    unsigned k = g->k, m = g->m;
    iso_rs *rs = m ? chunk_rs_get(k, m) : NULL;
    if (m && !rs)
        return TSDB_ERR_NOMEM;
    uint64_t ns = chunk_nstripes(g), bases[CHUNK_POS_MAX], sbytes = (uint64_t)k * g->cell;
    for (unsigned p = 0; p < k + m; p++)
        bases[p] = chunk_sums_base(g, p);
    if (sbytes > g->len)
        sbytes = g->len; /* the most bytes one stripe holds */
    size_t cap = g->cell > g->len ? (size_t)g->len : g->cell;
    bool reread = chunk_mutant == CHUNK_MUT_STREAM_REREAD;
    uint8_t *mem = m ? malloc((size_t)(k + m) * cap) : NULL, *buf = malloc((size_t)sbytes);
    uint8_t *buf2 = reread ? malloc((size_t)sbytes) : NULL;
    int rc = (!m || mem) && buf && (!reread || buf2) ? 0 : TSDB_ERR_NOMEM;
    for (uint64_t s = 0; s < ns && rc == 0; s++) {
        /* Everything below reads `buf` only: the source is asked for each byte once, so what fill() checked in it is
         * exactly what is hashed and encoded (E26: two reads of a mapping can see two different pages). */
        uint64_t off = s * k * g->cell, n = g->len - off < sbytes ? g->len - off : sbytes;
        rc = src->fill(src->ctx, s, off, buf, (size_t)n);
        if (rc > 0)
            rc = TSDB_ERR_INVALID; /* a caller that tests for a negative code must not take an abort for success */
        if (rc == 0 && reread)
            rc = src->fill(src->ctx, s, off, buf2, (size_t)n); /* planted: the source read again for the parity */
        if (rc == 0)
            rc = encode_stripe(g, rs, s, buf, reread ? buf2 : buf, mem, cap, fn, ctx, sums_out, bases);
    }
    free(buf2);
    free(buf);
    free(mem);
    return rc;
#endif
}
