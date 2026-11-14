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
#include <unistd.h>

#ifdef TSDB_EC
#include "ec/rs.h"
#endif

int chunk_mutant;
int chunk_fault_read, chunk_fault_readdir;
int chunk_log_quiet;
unsigned chunk_settings_k, chunk_settings_m;

/* ---------------------------------------------------------------- geometry */
bool chunk_geom_valid(const chunk_geom *g)
{
    if (g->k < 1 || g->k > CHUNK_K_MAX || g->m < 1 || g->m > CHUNK_M_MAX || g->cell < 1 || g->cell > CHUNK_CELL_LIMIT ||
        g->len < 1 || g->len > CHUNK_OBJ_MAX)
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
    if (k < 1 || k > CHUNK_K_MAX || m < 1 || m > CHUNK_M_MAX || len < 1 || len > CHUNK_OBJ_MAX)
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

void chunk_random(void *buf, size_t n)
{
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    size_t got = 0;
    while (fd >= 0 && got < n) {
        ssize_t r = read(fd, (uint8_t *)buf + got, n - got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    if (fd >= 0)
        close(fd);
    if (got < n) { /* an id that repeats is worse than none */
        fprintf(stderr, "tsdb: cannot read /dev/urandom: aborting\n");
        abort();
    }
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

/* The footer's hash: the header and the sums table as they lie in the file (little endian). */
static uint64_t ftr_hash(const uint8_t *hdr, const uint8_t *table, uint64_t ncells)
{
    uint8_t *b = malloc(CHUNK_HDR_BYTES + 8 * ncells);
    if (!b)
        return 0; /* the caller's comparison fails: an allocation failure reads as damage, never as a pass */
    memcpy(b, hdr, CHUNK_HDR_BYTES);
    memcpy(b + CHUNK_HDR_BYTES, table, 8 * ncells);
    uint64_t h = xxh64(b, CHUNK_HDR_BYTES + 8 * ncells, 0);
    free(b);
    return h;
}

/* Checks the footer (16 bytes) against header bytes and sums table; 0 or TSDB_ERR_CORRUPT. */
int chunk_ftr_check(const uint8_t *hdr, const uint8_t *table, uint64_t ncells, const uint8_t ftr[CHUNK_FTR_BYTES])
{
    if (memcmp(ftr + 12, FTR_MAGIC, 4) != 0 || get32(ftr + 8) != ncells)
        return TSDB_ERR_CORRUPT;
    uint64_t want = ftr_hash(hdr, table, ncells);
    return want != 0 && want == get64(ftr) ? 0 : TSDB_ERR_CORRUPT;
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

int chunk_encode(const chunk_geom *g, const uint8_t *obj, chunk_cell_fn fn, void *ctx, uint64_t *sums_out)
{
#ifndef TSDB_EC
    (void)g, (void)obj, (void)fn, (void)ctx, (void)sums_out;
    return TSDB_ERR_INVALID; /* this build has no erasure codec (it is x86-64 only) */
#else
    if (!chunk_geom_valid(g))
        return TSDB_ERR_INVALID;
    unsigned k = g->k, m = g->m;
    iso_rs *rs = chunk_rs_get(k, m);
    if (!rs)
        return TSDB_ERR_NOMEM;
    uint64_t ncells = chunk_ncells(g), ns = chunk_nstripes(g), bases[CHUNK_POS_MAX];
    for (unsigned p = 0; p < k + m; p++)
        bases[p] = chunk_sums_base(g, p);
    size_t cap = g->cell > g->len ? (size_t)g->len : g->cell; /* the longest cell */
    uint8_t *mem = malloc((size_t)(k + m) * cap);
    if (!mem)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (uint64_t s = 0; s < ns && rc == 0; s++) {
        size_t ls = (size_t)chunk_stripe_len(g, s);
        uint8_t *shards[CHUNK_POS_MAX];
        for (unsigned i = 0; i < k; i++) {
            uint64_t c = s * k + i, cl = chunk_cell_len(g, c);
            if (c < ncells && cl == ls) { /* a full-length cell is read in place */
                shards[i] = (uint8_t *)(uintptr_t)(obj + c * g->cell);
                continue;
            }
            shards[i] = mem + (size_t)i * cap;
            if (cl)
                memcpy(shards[i], obj + c * g->cell, (size_t)cl);
            memset(shards[i] + cl, 0, ls - (size_t)cl); /* the padding of a short or missing cell: zeros, never stored */
        }
        for (unsigned j = 0; j < m; j++)
            shards[k + j] = mem + (size_t)(k + j) * cap;
        iso_rs_encode(rs, shards, ls);
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
    }
    free(mem);
    return rc;
#endif
}
