/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Objects, chunk files, chunk stores and the directory backend (src/chunk*.c, docs/design-cluster-data.md step R6).
 *
 *  - geometry: E7's cell size and the derived k', every chunk's length, the sums layout, invalid geometries;
 *  - the layout against an oracle written here (the data chunks, dealt round-robin, read back without any decode);
 *  - the chunk file: build and parse round trip; every single-byte change, every truncation and every extension of a
 *    chunk image is refused (a torn or damaged chunk is an erasure, never decoder input);
 *  - decode in memory, EXHAUSTIVE: for 4+2, 8+2, 8+4, 16+4, 3 copies (1+2), derived small k and other shapes, every
 *    set of up to m bad positions (absent, unreadable, a flipped byte, truncated), range reads at every boundary of
 *    cells and stripes, with and without the object record's sums, whole cells and CHUNK_RR_RAW; m + 1 bad is an error
 *    (never wrong bytes);
 *  - the same on files through the directory backend (every pattern for 4+2 and 8+2, sampled above): absent, damaged
 *    in the cells, the header or the footer, truncated, empty, zeros, another position's file, another object's file,
 *    a chunk of another encoding (caught by the object record);
 *  - geometry from the object, never from settings; a short read is never the end of a file;
 *  - stores: META and the store id, EPOCH (damage, torn slots, no EPOCH beside chunks), the fence (put and delete
 *    below the epoch, a delete below the chunk's own epoch, the maximum over the stores of a backend), tmp_* files
 *    removed at open, list with and without verify;
 *  - power loss (crashfs) at every step of a chunk write, an EPOCH store and a delete, with unsynced data torn at
 *    random: every chunk present is whole, every acknowledged one is there, EPOCH never goes back; and an I/O error
 *    (crashfs eio) at every step: the store acknowledges nothing after it fails.
 *
 *   test_chunk [DIR]                  DIR is the scratch directory of the other tests
 *   test_chunk --quick                coarser power-loss sweeps (the mutant runs use it)
 *   test_chunk --mutant NAME          plants nosums | nofence | eof | settings | nosync | epochnosync: must fail
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#ifndef TSDB_EC /* Apple silicon and other machines that are not x86-64: the Makefile builds no erasure codec there */
#include <stdio.h>
int main(void)
{
    printf("test_chunk: SKIPPED (this build has no erasure codec: it is x86-64 only)\n");
    return 0;
}
#else
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
static FILE *ERR; /* the real stderr: the library's refusals (which the loops below provoke by the thousand) go to
                   * /dev/null while quiet() is on, a failed check never does */
static int saved_err = -1;
static void quiet(bool on)
{
    if (on) {
        saved_err = dup(2);
        int nul = open("/dev/null", O_WRONLY);
        if (nul >= 0)
            dup2(nul, 2), close(nul);
    } else if (saved_err >= 0) {
        dup2(saved_err, 2);
        close(saved_err);
        saved_err = -1;
    }
}
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(ERR ? ERR : stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            if (++failures > 60)                                                     \
                exit(1);                                                             \
        }                                                                            \
    } while (0)

static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];
static bool quick;

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng ^= rng >> 12, rng ^= rng << 25, rng ^= rng >> 27;
    return rng * 0x2545f4914f6cdd1dull;
}
static void fill(uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        p[i] = (uint8_t)(rnd() >> 33);
}

static void sh(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void sh(const char *fmt, ...)
{
    char cmd[PATH_MAX * 3];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    if (system(cmd) != 0)
        fprintf(stderr, "(command failed: %s)\n", cmd);
}

static void fresh(const char *tag, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%s", S, tag);
    sh("rm -rf '%s'; mkdir -p '%s'", out, out);
}

/* ---------------------------------------------------------------- geometry */
static void test_geometry(void)
{
    chunk_geom g;
    /* E7: clamp(len / (8k), 64 KiB, 1 MiB), rounded to 4 KiB; k' = min(k, cells) */
    CHECK(chunk_geom_derive(&g, 16, 4, 17u << 20) == 0 && g.cell == 139264 && g.k == 16 && g.m == 4);
    CHECK(chunk_geom_derive(&g, 8, 2, 4ull << 30) == 0 && g.cell == (1u << 20) && g.k == 8);
    CHECK(chunk_geom_derive(&g, 8, 2, 3u << 20) == 0 && g.cell == CHUNK_CELL_MIN && g.k == 8);
    CHECK(chunk_geom_derive(&g, 8, 2, 10000) == 0 && g.cell == CHUNK_CELL_MIN && g.k == 1); /* one cell: m + 1 copies */
    CHECK(chunk_pos_len(&g, 0) == 10000 && chunk_pos_len(&g, 1) == 10000 && chunk_pos_len(&g, 2) == 10000);
    CHECK(chunk_geom_derive(&g, 8, 2, CHUNK_CELL_MIN * 3 + 5) == 0 && g.k == 4 && chunk_ncells(&g) == 4);
    CHECK(chunk_geom_derive(&g, 4, 2, 1) == 0 && g.k == 1 && chunk_pos_len(&g, 1) == 1);
    CHECK(chunk_geom_derive(&g, 0, 2, 100) == TSDB_ERR_INVALID && chunk_geom_derive(&g, 17, 2, 100) == TSDB_ERR_INVALID);
    CHECK(chunk_geom_derive(&g, 4, 0, 100) == TSDB_ERR_INVALID && chunk_geom_derive(&g, 4, 5, 100) == TSDB_ERR_INVALID);
    CHECK(chunk_geom_derive(&g, 4, 2, 0) == TSDB_ERR_INVALID);
    CHECK(chunk_geom_make(&g, 4, 2, 64, 100) == TSDB_ERR_INVALID); /* 2 cells cannot feed 4 data chunks */
    CHECK(chunk_geom_make(&g, 2, 2, 64, 100) == 0 && chunk_geom_make(&g, 2, 2, 0, 100) == TSDB_ERR_INVALID);
    /* every chunk's length, the sums layout and the sizes add up, for many shapes */
    int shapes = 0;
    for (unsigned k = 1; k <= 16; k++)
        for (unsigned m = 1; m <= 4; m++)
            for (int t = 0; t < 12; t++) {
                uint32_t cell = 1 + (uint32_t)(rnd() % 100);
                uint64_t len = (uint64_t)k * cell + rnd() % ((uint64_t)cell * k * 3 + 1);
                if (chunk_geom_make(&g, k, m, cell, len) != 0)
                    continue;
                shapes++;
                uint64_t data = 0, cells = 0, sums = 0;
                for (unsigned p = 0; p < k + m; p++) {
                    CHECK(chunk_pos_cells(&g, p) >= 1);
                    CHECK(chunk_sums_base(&g, p) == sums);
                    sums += chunk_pos_cells(&g, p);
                    if (p < k)
                        data += chunk_pos_len(&g, p), cells += chunk_pos_cells(&g, p);
                    uint64_t sum_len = 0;
                    for (uint64_t i = 0; i < chunk_pos_cells(&g, p); i++)
                        sum_len += chunk_pos_cell_len(&g, p, i);
                    CHECK(sum_len == chunk_pos_len(&g, p));
                    CHECK(chunk_file_size(&g, p) == CHUNK_HDR_BYTES + chunk_pos_len(&g, p) + 8 * chunk_pos_cells(&g, p) + 16);
                }
                CHECK(data == len && cells == chunk_ncells(&g) && sums == chunk_sums_total(&g));
            }
    CHECK(shapes > 500);
    printf("  geometry: E7 cell sizes, derived k', lengths of every chunk (%d shapes), invalid geometries refused\n",
           shapes);
}

/* ---------------------------------------------------------------- an object in memory */
typedef struct msrc {
    chunk_geom g;
    uint8_t *cells[CHUNK_POS_MAX];
    size_t clen[CHUNK_POS_MAX];
    uint64_t *sums[CHUNK_POS_MAX];
    int fail[CHUNK_POS_MAX]; /* 0, or the status every read of that position returns */
    uint64_t *rec;           /* the object record: sums of every cell of every position */
} msrc;

static int ms_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    msrc *m = ctx;
    if (m->fail[pos])
        return m->fail[pos];
    if (off + len > m->clen[pos])
        return CHUNK_ST_IOERR; /* a short read is an error, never the end */
    memcpy(buf, m->cells[pos] + off, len);
    return 0;
}

static const uint64_t *ms_sums(void *ctx, unsigned pos, int *status)
{
    msrc *m = ctx;
    if (m->fail[pos]) {
        *status = m->fail[pos];
        return NULL;
    }
    return m->sums[pos];
}

static int ms_collect(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    msrc *m = ctx;
    memcpy(m->cells[pos] + idx * m->g.cell, data, len);
    m->sums[pos][idx] = sum;
    return 0;
}

static void ms_build(msrc *m, const chunk_geom *g, const uint8_t *obj)
{
    memset(m, 0, sizeof *m);
    m->g = *g;
    for (unsigned p = 0; p < chunk_npos(g); p++) {
        m->clen[p] = (size_t)chunk_pos_len(g, p);
        m->cells[p] = calloc(m->clen[p] + 1, 1);
        m->sums[p] = calloc(chunk_pos_cells(g, p) + 1, 8);
    }
    m->rec = calloc(chunk_sums_total(g) + 1, 8);
    CHECK(chunk_encode(g, obj, ms_collect, m, m->rec) == 0);
}

static void ms_free(msrc *m)
{
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        free(m->cells[p]), free(m->sums[p]);
    free(m->rec);
}

static chunk_source ms_source(msrc *m)
{
    chunk_source s;
    memset(&s, 0, sizeof s);
    s.ctx = m;
    snprintf(s.name, sizeof s.name, "mem");
    s.read = ms_read;
    s.sums = ms_sums;
    return s;
}

/* The layout against an oracle: the data chunks alone, dealt round-robin, are the object. */
static void check_layout(const msrc *m, const uint8_t *obj)
{
    const chunk_geom *g = &m->g;
    for (uint64_t c = 0; c < chunk_ncells(g); c++) {
        uint64_t cl = chunk_cell_len(g, c);
        CHECK(memcmp(m->cells[c % g->k] + (c / g->k) * g->cell, obj + c * g->cell, (size_t)cl) == 0);
    }
    /* with k' = 1 every parity chunk is as long as the data chunk (m + 1 copies by construction; the first parity row of
       the generator is all ones, so that one is byte for byte the data) */
    if (g->k == 1) {
        for (unsigned j = 0; j < g->m; j++)
            CHECK(m->clen[1 + j] == m->clen[0]);
        CHECK(memcmp(m->cells[1], m->cells[0], m->clen[0]) == 0);
    }
}

enum { DK_ABSENT, DK_IOERR, DK_FLIP, DK_TRUNC, DK_DAMAGED, DK_KINDS };

static int popcount(unsigned x) { return __builtin_popcount(x); }

/* The ranges a reader may ask: the whole object, the ends, and the neighbourhood of boundaries of cells and stripes. */
typedef struct range {
    uint64_t off, len;
} range;

static size_t make_ranges(const chunk_geom *g, range *out, size_t cap)
{
    size_t n = 0;
    uint64_t L = g->len, cell = g->cell, stripe = cell * g->k;
#define ADD(o, l)                                    \
    do {                                             \
        uint64_t o_ = (o), l_ = (l);                 \
        if (n < cap && o_ < L && l_ > 0 && o_ + l_ <= L) \
            out[n].off = o_, out[n++].len = l_;      \
    } while (0)
    ADD(0, L);
    ADD(0, 1);
    ADD(L - 1, 1);
    ADD(0, L > 1 ? L - 1 : 1);
    ADD(1, L > 1 ? L - 1 : 1);
    uint64_t bounds[] = {cell, stripe, stripe + cell, L > cell ? L - cell : 1, (chunk_nstripes(g) - 1) * stripe};
    for (size_t i = 0; i < sizeof bounds / sizeof *bounds; i++) {
        uint64_t b = bounds[i];
        ADD(b ? b - 1 : 0, 2);
        ADD(b, cell);
        ADD(b + 1, cell > 1 ? cell - 1 : 1);
        ADD(b > cell ? b - cell : 0, 2 * cell);
        ADD(b > 3 ? b - 3 : 0, stripe + 7);
    }
    for (int i = 0; i < 6; i++) {
        uint64_t o = rnd() % L;
        ADD(o, 1 + rnd() % (L - o));
    }
#undef ADD
    return n;
}

static void damage_mem(msrc *m, unsigned pos, int kind, uint8_t **saved, size_t *saved_len)
{
    /* the caller restores through saved; flips and truncations are real changes of the bytes the reader gets */
    *saved = malloc(m->clen[pos] + 1);
    memcpy(*saved, m->cells[pos], m->clen[pos]);
    *saved_len = m->clen[pos];
    switch (kind) {
    case DK_ABSENT:
        m->fail[pos] = CHUNK_ST_ABSENT;
        break;
    case DK_IOERR:
        m->fail[pos] = CHUNK_ST_IOERR;
        break;
    case DK_DAMAGED:
        m->fail[pos] = CHUNK_ST_DAMAGED;
        break;
    case DK_FLIP: {
        uint64_t nc = chunk_pos_cells(&m->g, pos);
        bool any = false;
        for (uint64_t i = 0; i < nc; i++)
            if (rnd() & 1 || (i == nc - 1 && !any)) { /* some cells of the chunk, at least one */
                uint64_t cl = chunk_pos_cell_len(&m->g, pos, i);
                m->cells[pos][i * m->g.cell + rnd() % cl] ^= (uint8_t)(1 + rnd() % 255);
                any = true;
            }
        break;
    }
    case DK_TRUNC:
        m->clen[pos] = (size_t)(rnd() % (m->clen[pos] ? m->clen[pos] : 1)); /* a read past it is an error */
        break;
    }
}

static void restore_mem(msrc *m, unsigned pos, uint8_t *saved, size_t len)
{
    memcpy(m->cells[pos], saved, len);
    m->clen[pos] = len;
    m->fail[pos] = 0;
    free(saved);
}

/* Every set of up to m bad positions, every kind: reads equal the original; m + 1 bad is an error. */
static void mem_exhaustive(unsigned k, unsigned m, uint32_t cell, uint64_t len, const char *label)
{
    chunk_geom g;
    CHECK(chunk_geom_make(&g, k, m, cell, len) == 0);
    uint8_t *obj = malloc(len);
    fill(obj, len);
    msrc ms;
    ms_build(&ms, &g, obj);
    check_layout(&ms, obj);
    chunk_source src = ms_source(&ms);
    unsigned np = chunk_npos(&g);
    range rg[64];
    size_t nr = make_ranges(&g, rg, 64);
    uint8_t *buf = malloc(len + 1);
    uint64_t reads = 0, patterns = 0, refused = 0;
    for (unsigned mask = 0; mask < (1u << np); mask++) {
        int pc = popcount(mask);
        if (pc > (int)m + 1)
            continue;
        if (pc == (int)m + 1 && (rnd() % 9) != 0 && np > 8) /* above m: a sample for the large shapes */
            continue;
        patterns++;
        uint8_t *saved[CHUNK_POS_MAX] = {0};
        size_t slen[CHUNK_POS_MAX];
        bool no_flip = true, all_hard = true; /* hard: the whole chunk is unusable (flips and cuts may spare a stripe) */
        for (unsigned p = 0; p < np; p++)
            if (mask >> p & 1) {
                int kind = (int)(rnd() % DK_KINDS);
                if (kind == DK_FLIP)
                    no_flip = false;
                if (kind == DK_FLIP || kind == DK_TRUNC)
                    all_hard = false;
                damage_mem(&ms, p, kind, &saved[p], &slen[p]);
            }
        for (int use_rec = 0; use_rec < 2; use_rec++) {
            const uint64_t *rec = use_rec ? ms.rec : NULL;
            chunk_report rep;
            /* the whole object: with up to m bad positions it is exact, with m + 1 it is refused */
            memset(buf, 0xA5, len);
            int rc = chunk_read_range(&src, &g, rec, 0, len, buf, 0, &rep);
            reads++;
            if (pc <= (int)m) {
                CHECK(rc == 0 && memcmp(buf, obj, len) == 0);
            } else if (rc == 0) {
                CHECK(memcmp(buf, obj, len) == 0); /* never wrong bytes */
                CHECK(!all_hard);                  /* the first stripe is full: m + 1 lost chunks cannot be read */
            } else {
                CHECK(rc == TSDB_ERR_CORRUPT);
                refused++;
            }
            for (size_t i = 1; i < nr; i++) {
                memset(buf, 0xA5, rg[i].len);
                rc = chunk_read_range(&src, &g, rec, rg[i].off, rg[i].len, buf, 0, &rep);
                reads++;
                if (rc == 0)
                    CHECK(memcmp(buf, obj + rg[i].off, (size_t)rg[i].len) == 0);
                else
                    CHECK(pc > (int)m && rc == TSDB_ERR_CORRUPT);
                if (pc <= (int)m && no_flip) { /* RAW: the same bytes from the same byte range of the others */
                    memset(buf, 0xA5, rg[i].len);
                    rc = chunk_read_range(&src, &g, NULL, rg[i].off, rg[i].len, buf, CHUNK_RR_RAW, &rep);
                    reads++;
                    CHECK(rc == 0 && memcmp(buf, obj + rg[i].off, (size_t)rg[i].len) == 0);
                }
            }
        }
        for (unsigned p = 0; p < np; p++)
            if (saved[p])
                restore_mem(&ms, p, saved[p], slen[p]);
    }
    CHECK(refused > 0);
    free(buf);
    free(obj);
    ms_free(&ms);
    printf("  %-34s %6" PRIu64 " patterns, %8" PRIu64 " reads: exact up to m bad, an error above\n", label, patterns,
           reads);
}

static void test_memory(void)
{
    /* the shapes of the design (E1): 4+2, 8+2, 8+4, 16+4, 3 copies as 1+2, and what small objects derive */
    static const struct {
        unsigned k, m;
        const char *label;
    } shapes[] = {{4, 2, "4+2"}, {8, 2, "8+2"}, {8, 4, "8+4"}, {16, 4, "16+4"}, {1, 2, "3 copies (1+2)"},
                  {2, 1, "2+1"}, {3, 1, "3+1"}, {5, 2, "5+2"}, {1, 1, "mirror (1+1)"}, {16, 1, "16+1"}};
    for (size_t i = 0; i < sizeof shapes / sizeof *shapes; i++) {
        unsigned k = shapes[i].k, m = shapes[i].m;
        uint32_t cell = 32;
        /* two full stripes, then a stripe of k/2 + 1 cells whose last cell is short */
        uint64_t len = (uint64_t)cell * (2 * k + k / 2 + 1) - 11;
        mem_exhaustive(k, m, cell, len, shapes[i].label);
    }
    /* small objects: k' below k; cells fewer than the data chunks the volume has */
    mem_exhaustive(3, 2, 16, 16 * 3 + 5, "derived k' = 3 of 8+2");
    mem_exhaustive(2, 4, 8, 8 * 5, "k' = 2 of 8+4, whole cells");
    mem_exhaustive(1, 4, 100, 37, "one short cell, 1+4");
    mem_exhaustive(7, 3, 1, 7 * 4 + 3, "cells of one byte");
}

/* ---------------------------------------------------------------- the chunk file image */
static void test_image(void)
{
    chunk_geom g;
    CHECK(chunk_geom_make(&g, 3, 2, 40, 40 * 7 + 9) == 0);
    uint8_t *obj = malloc(g.len);
    fill(obj, g.len);
    msrc ms;
    ms_build(&ms, &g, obj);
    int images = 0;
    for (unsigned pos = 0; pos < chunk_npos(&g); pos++) {
        chunk_hdr h;
        memset(&h, 0, sizeof h);
        memset(h.vuuid, 0x11, 16);
        memset(h.oid, 0x22, 16);
        h.epoch = 77;
        h.g = g;
        h.pos = (uint16_t)pos;
        h.chunk_len = chunk_pos_len(&g, pos);
        size_t n;
        uint8_t *img = chunk_image_build(&h, ms.cells[pos], ms.sums[pos], &n);
        CHECK(img && n == chunk_file_size(&g, pos));
        chunk_hdr back;
        const uint8_t *cells, *sums;
        CHECK(chunk_image_parse(img, n, true, &back, &cells, &sums) == 0);
        CHECK(back.epoch == 77 && back.pos == pos && memcmp(&back.g, &g, sizeof g) == 0 && back.chunk_len == h.chunk_len);
        CHECK(memcmp(cells, ms.cells[pos], ms.clen[pos]) == 0);
        /* every single-byte change anywhere is refused (the cell area by its sums, which the check reads) */
        for (size_t i = 0; i < n; i++) {
            img[i] ^= 0x10;
            CHECK(chunk_image_parse(img, n, true, &back, &cells, &sums) != 0);
            img[i] ^= 0x10;
            images++;
        }
        /* every truncation and an extension */
        for (size_t t = 0; t < n; t++)
            CHECK(chunk_image_parse(img, t, true, &back, &cells, &sums) != 0);
        uint8_t *longer = realloc(img, n + 1);
        longer[n] = 0;
        CHECK(chunk_image_parse(longer, n + 1, true, &back, &cells, &sums) != 0);
        CHECK(chunk_image_parse(longer, n, true, &back, &cells, &sums) == 0);
        free(longer);
    }
    ms_free(&ms);
    free(obj);
    printf("  chunk image: round trip, %d single-byte changes, every truncation and an extension refused\n", images);
}

/* ---------------------------------------------------------------- files, stores, backend */
typedef struct fx { /* a backend of n stores with one object in it */
    char dir[PATH_MAX + 32];
    char paths[6][PATH_MAX + 64];
    const char *pp[6];
    int n;
    chunk_backend *be;
    uint32_t vol;
    uint8_t vuuid[16], oid[16];
    chunk_geom g;
    uint8_t *obj;
    uint64_t *rec;
    int base;
} fx;

static void fx_path(const fx *f, unsigned pos, char *out, size_t cap)
{
    char hex[33];
    chunk_oid_hex(f->oid, hex);
    int st = (f->base + (int)pos) % f->n;
    snprintf(out, cap, "%s/s%u/o%s.%u.chunk", f->paths[st], f->vol, hex, pos);
}

static void fx_make(fx *f, const char *tag, int nstores, unsigned k, unsigned m, uint32_t cell, uint64_t len,
                    uint64_t epoch)
{
    memset(f, 0, sizeof *f);
    fresh(tag, f->dir, sizeof f->dir);
    f->n = nstores;
    for (int i = 0; i < nstores; i++) {
        snprintf(f->paths[i], sizeof f->paths[i], "%s/st%d", f->dir, i);
        f->pp[i] = f->paths[i];
        CHECK(chunk_store_init(f->paths[i], NULL, NULL) == 0);
    }
    CHECK(chunk_be_open(&f->be, f->pp, nstores, NULL) == 0);
    f->vol = 3;
    chunk_random(f->vuuid, 16);
    chunk_random(f->oid, 16);
    if (cell)
        CHECK(chunk_geom_make(&f->g, k, m, cell, len) == 0);
    else
        CHECK(chunk_geom_derive(&f->g, k, m, len) == 0);
    f->obj = malloc(len);
    fill(f->obj, len);
    f->rec = calloc(chunk_sums_total(&f->g) + 1, 8);
    f->base = chunk_be_place(f->be, f->oid, 0);
    CHECK(chunk_be_put_object(f->be, f->vol, f->vuuid, f->oid, &f->g, epoch, f->obj, ~0u, f->rec) == 0);
}

static void fx_free(fx *f)
{
    chunk_be_close(f->be);
    free(f->obj);
    free(f->rec);
}

static uint8_t *slurp(const char *path, size_t *n)
{
    int fd = open(path, O_RDONLY);
    struct stat sb;
    if (fd < 0 || fstat(fd, &sb) != 0) {
        *n = 0;
        return NULL;
    }
    uint8_t *b = malloc((size_t)sb.st_size + 1);
    size_t got = 0;
    while (got < (size_t)sb.st_size) {
        ssize_t r = read(fd, b + got, (size_t)sb.st_size - got);
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    close(fd);
    *n = got;
    return b;
}

static void spit(const char *path, const uint8_t *b, size_t n)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);
    if (fd >= 0) {
        CHECK(write(fd, b, n) == (ssize_t)n);
        close(fd);
    }
}

enum { FK_ABSENT, FK_FLIP_CELL, FK_FLIP_HDR, FK_FLIP_TAIL, FK_TRUNC, FK_EMPTY, FK_ZEROS, FK_SWAP, FK_OTHER_OBJ, FK_KINDS };

/* Damages the file of `pos`; the original bytes go to *saved so the caller can put them back. */
static void damage_file(fx *f, unsigned pos, int kind, uint8_t **saved, size_t *sn)
{
    char path[PATH_MAX + 128];
    fx_path(f, pos, path, sizeof path);
    *saved = slurp(path, sn);
    size_t n = *sn;
    uint8_t *b = malloc(n + 1);
    memcpy(b, *saved, n);
    uint64_t cells = chunk_pos_len(&f->g, pos);
    switch (kind) {
    case FK_ABSENT:
        unlink(path);
        free(b);
        return;
    case FK_FLIP_CELL:
        b[CHUNK_HDR_BYTES + rnd() % cells] ^= (uint8_t)(1 + rnd() % 255);
        break;
    case FK_FLIP_HDR:
        b[rnd() % CHUNK_HDR_BYTES] ^= (uint8_t)(1 + rnd() % 255);
        break;
    case FK_FLIP_TAIL:
        b[CHUNK_HDR_BYTES + cells + rnd() % (n - CHUNK_HDR_BYTES - cells)] ^= (uint8_t)(1 + rnd() % 255);
        break;
    case FK_TRUNC:
        n = (size_t)(rnd() % n);
        break;
    case FK_EMPTY:
        n = 0;
        break;
    case FK_ZEROS:
        memset(b, 0, n);
        break;
    case FK_SWAP: { /* another position's file under this name (misdirected, or a bad rename) */
        char other[PATH_MAX + 128];
        fx_path(f, (pos + 1) % chunk_npos(&f->g), other, sizeof other);
        size_t on;
        uint8_t *ob = slurp(other, &on);
        free(b);
        b = ob;
        n = on;
        break;
    }
    case FK_OTHER_OBJ: { /* the same position of an object with another oid: valid in itself, not this one's */
        chunk_hdr h;
        const uint8_t *cl, *sl;
        CHECK(chunk_image_parse(*saved, n, false, &h, &cl, &sl) == 0);
        uint64_t nc = chunk_pos_cells(&h.g, h.pos), *sums = malloc((nc + 1) * 8);
        for (uint64_t i = 0; i < nc; i++)
            sums[i] = chunk_le64(sl + 8 * i);
        h.oid[3] ^= 1;
        free(b);
        b = chunk_image_build(&h, cl, sums, &n);
        free(sums);
        break;
    }
    }
    spit(path, b, n);
    free(b);
}

static void restore_file(fx *f, unsigned pos, uint8_t *saved, size_t n)
{
    char path[PATH_MAX + 128];
    fx_path(f, pos, path, sizeof path);
    spit(path, saved, n);
    free(saved);
}

static void file_exhaustive(const char *label, unsigned k, unsigned m, uint32_t cell, uint64_t len, int nstores,
                            unsigned sample)
{
    fx f;
    fx_make(&f, "fx-ex", nstores, k, m, cell, len, 1);
    unsigned np = chunk_npos(&f.g);
    range rg[64];
    size_t nr = make_ranges(&f.g, rg, 64);
    uint8_t *buf = malloc(len + 1);
    uint64_t patterns = 0, reads = 0;
    for (unsigned mask = 0; mask < (1u << np); mask++) {
        int pc = popcount(mask);
        if (pc > (int)m + 1 || (sample && pc > 0 && rnd() % sample != 0))
            continue;
        if (pc == (int)m + 1 && (rnd() % 4) != 0)
            continue;
        patterns++;
        uint8_t *saved[CHUNK_POS_MAX] = {0};
        size_t sn[CHUNK_POS_MAX];
        for (unsigned p = 0; p < np; p++)
            if (mask >> p & 1)
                damage_file(&f, p, (int)(rnd() % FK_KINDS), &saved[p], &sn[p]);
        chunk_obj *o = NULL;
        int rc = chunk_obj_open(f.be, f.vol, f.vuuid, f.oid, NULL, f.rec, &o);
        if (pc <= (int)m) {
            CHECK(rc == 0);
        }
        if (rc == 0) {
            CHECK(memcmp(chunk_obj_geom(o), &f.g, sizeof f.g) == 0); /* from the headers: the object's own */
            for (size_t i = 0; i < nr; i++) {
                memset(buf, 0xA5, rg[i].len);
                chunk_report rep;
                int r = chunk_obj_read(o, rg[i].off, rg[i].len, buf, 0, &rep);
                reads++;
                if (r == 0)
                    CHECK(memcmp(buf, f.obj + rg[i].off, (size_t)rg[i].len) == 0);
                else
                    CHECK(pc > (int)m && r == TSDB_ERR_CORRUPT);
                if (pc <= (int)m)
                    CHECK(r == 0);
            }
            chunk_obj_close(o);
        } else {
            CHECK(pc > (int)m && (rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_NOTFOUND));
        }
        for (unsigned p = 0; p < np; p++)
            if (saved[p])
                restore_file(&f, p, saved[p], sn[p]);
    }
    free(buf);
    fx_free(&f);
    printf("  files, %-22s %6" PRIu64 " patterns, %6" PRIu64 " reads: absent, flipped, truncated, empty, zeros, "
           "swapped\n", label, patterns, reads);
}

static void test_files(void)
{
    file_exhaustive("4+2", 4, 2, 48, 48 * 9 + 5, 3, 0);
    file_exhaustive("8+2", 8, 2, 40, 40 * 19 + 7, 4, 0);
    file_exhaustive("8+4 (sampled)", 8, 4, 40, 40 * 19 + 7, 4, 5);
    file_exhaustive("16+4 (sampled)", 16, 4, 24, 24 * 37 + 5, 3, 120);
    file_exhaustive("3 copies (1+2)", 1, 2, 64, 64 * 5 + 3, 3, 0);
}

/* A large object with derived geometry (cells of E7), reads at random, the full set of failures. */
static void test_derived(void)
{
    static const struct {
        unsigned k, m;
        uint64_t len;
    } t[] = {{16, 4, 3u << 20}, {8, 2, (5u << 20) + 12345}, {4, 2, 1000000}, {8, 4, 70000}, {8, 2, 10000}};
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        fx f;
        fx_make(&f, "fx-derived", 4, t[i].k, t[i].m, 0, t[i].len, 2);
        unsigned np = chunk_npos(&f.g);
        uint8_t *buf = malloc(t[i].len);
        for (int round = 0; round < (quick ? 3 : 8); round++) {
            uint8_t *saved[CHUNK_POS_MAX] = {0};
            size_t sn[CHUNK_POS_MAX];
            unsigned nbad = round % (t[i].m + 1), picked = 0;
            while (picked < nbad) {
                unsigned p = (unsigned)(rnd() % np);
                if (saved[p])
                    continue;
                damage_file(&f, p, round & 1 ? FK_FLIP_CELL : FK_ABSENT, &saved[p], &sn[p]);
                picked++;
            }
            chunk_obj *o = NULL;
            CHECK(chunk_obj_open(f.be, f.vol, f.vuuid, f.oid, NULL, NULL, &o) == 0);
            for (int q = 0; q < 12; q++) {
                uint64_t off = rnd() % t[i].len, len = 1 + rnd() % (t[i].len - off);
                if (q == 0)
                    off = 0, len = t[i].len;
                chunk_report rep;
                CHECK(chunk_obj_read(o, off, len, buf, 0, &rep) == 0 && memcmp(buf, f.obj + off, (size_t)len) == 0);
                if (nbad > 0 && q == 0)
                    CHECK(rep.decoded_cells > 0 || rep.bad_cells == 0);
            }
            chunk_obj_close(o);
            for (unsigned p = 0; p < np; p++)
                if (saved[p])
                    restore_file(&f, p, saved[p], sn[p]);
        }
        free(buf);
        fx_free(&f);
    }
    printf("  derived geometry (E7 cells, k' below k) at 4 KiB to 5 MiB: random ranges with 0 to m chunks lost\n");
}

/* A chunk of another encoding of the same object id: self-consistent, caught only by the object record. */
static void test_other_encoding(void)
{
    fx f;
    fx_make(&f, "fx-enc", 3, 4, 2, 32, 32 * 10 + 3, 1);
    char path[PATH_MAX + 128];
    fx_path(&f, 1, path, sizeof path);
    size_t n1;
    uint8_t *first = slurp(path, &n1);
    /* the second encoding: same oid, other bytes, a higher epoch */
    uint8_t *obj2 = malloc(f.g.len);
    fill(obj2, f.g.len);
    uint64_t *rec2 = calloc(chunk_sums_total(&f.g) + 1, 8);
    CHECK(chunk_be_put_object(f.be, f.vol, f.vuuid, f.oid, &f.g, 2, obj2, ~0u, rec2) == 0);
    spit(path, first, n1); /* position 1 goes back to the first encoding's chunk */
    free(first);
    chunk_obj *o = NULL;
    CHECK(chunk_obj_open(f.be, f.vol, f.vuuid, f.oid, NULL, rec2, &o) == 0);
    uint8_t *buf = malloc(f.g.len);
    chunk_report rep;
    CHECK(chunk_obj_read(o, 0, f.g.len, buf, 0, &rep) == 0 && memcmp(buf, obj2, f.g.len) == 0);
    CHECK(rep.decoded_cells > 0);
    chunk_obj_close(o);
    free(buf);
    free(obj2);
    free(rec2);
    fx_free(&f);
    printf("  a chunk of another encoding of the object is an erasure when the object record's sums are given\n");
}

/* The geometry is the object's: a reader whose settings differ still reads it. */
static void test_geometry_from_object(void)
{
    chunk_settings_k = 4;
    chunk_settings_m = 2; /* "today's settings": the object below was written under 8+2 */
    fx f;
    fx_make(&f, "fx-geom", 5, 8, 2, 40, 40 * 21 + 5, 1);
    chunk_obj *o = NULL;
    CHECK(chunk_obj_open(f.be, f.vol, f.vuuid, f.oid, NULL, NULL, &o) == 0);
    uint8_t *buf = malloc(f.g.len);
    CHECK(chunk_obj_read(o, 0, f.g.len, buf, 0, NULL) == 0 && memcmp(buf, f.obj, f.g.len) == 0);
    CHECK(chunk_obj_geom(o)->k == 8 && chunk_obj_geom(o)->m == 2 && chunk_obj_geom(o)->cell == 40);
    chunk_obj_close(o);
    free(buf);
    fx_free(&f);
    printf("  the layout is the object's own (headers), not today's settings\n");
}

/* A short read is an error, never the end of the file: the chunk shrinks after it was opened. */
static void test_short_read(void)
{
    fx f;
    fx_make(&f, "fx-short", 3, 4, 2, 64, 64 * 12 + 9, 1);
    chunk_obj *o = NULL;
    CHECK(chunk_obj_open(f.be, f.vol, f.vuuid, f.oid, NULL, NULL, &o) == 0);
    char path[PATH_MAX + 128];
    fx_path(&f, 0, path, sizeof path);
    CHECK(truncate(path, CHUNK_HDR_BYTES + 10) == 0); /* under the open handle: reads at 10 on find the end */
    uint8_t *buf = malloc(f.g.len);
    memset(buf, 0xA5, f.g.len);
    chunk_report rep;
    /* raw: nothing checks the bytes, so only the error saves them; verified: the sums do as well */
    CHECK(chunk_obj_read(o, 0, f.g.len, buf, CHUNK_RR_RAW, &rep) == 0 && memcmp(buf, f.obj, f.g.len) == 0);
    CHECK(rep.st[0] == CHUNK_ST_IOERR && rep.decoded_cells > 0);
    memset(buf, 0xA5, f.g.len);
    CHECK(chunk_obj_read(o, 0, f.g.len, buf, 0, &rep) == 0 && memcmp(buf, f.obj, f.g.len) == 0);
    chunk_obj_close(o);
    free(buf);
    fx_free(&f);
    printf("  a chunk that shrinks under an open handle: the short read is an error, the bytes come from the others\n");
}

/* Range arguments, and the writer's own checks. */
static void test_args_and_writer(void)
{
    fx f;
    fx_make(&f, "fx-args", 2, 3, 1, 50, 50 * 7 + 1, 1);
    chunk_obj *o = NULL;
    CHECK(chunk_obj_open(f.be, f.vol, f.vuuid, f.oid, NULL, f.rec, &o) == 0);
    uint8_t buf[1024];
    CHECK(chunk_obj_read(o, 0, 0, buf, 0, NULL) == 0 && chunk_obj_read(o, f.g.len, 0, buf, 0, NULL) == 0);
    CHECK(chunk_obj_read(o, f.g.len, 1, buf, 0, NULL) == TSDB_ERR_INVALID);
    CHECK(chunk_obj_read(o, 1, f.g.len, buf, 0, NULL) == TSDB_ERR_INVALID);
    CHECK(chunk_obj_read(o, UINT64_MAX, 2, buf, 0, NULL) == TSDB_ERR_INVALID);
    chunk_obj_close(o);
    /* a writer: wrong cell length, a given sum that is wrong, missing cells; nothing becomes visible */
    chunk_ident id = {.volume = f.vol, .pos = 0};
    memcpy(id.vuuid, f.vuuid, 16);
    chunk_random(id.oid, 16);
    chunk_store *st = chunk_be_store(f.be, 0);
    chunk_wr *w;
    CHECK(chunk_wr_open(st, &id, &f.g, 1, &w) == 0);
    uint8_t cell[64] = {1};
    CHECK(chunk_wr_cell(w, cell, 49, false, 0) == TSDB_ERR_INVALID);
    chunk_wr_abort(w);
    CHECK(chunk_wr_open(st, &id, &f.g, 1, &w) == 0);
    CHECK(chunk_wr_cell(w, cell, 50, true, xxh64(cell, 50, 0) ^ 1) == TSDB_ERR_CORRUPT); /* what a server checks (E26) */
    CHECK(chunk_wr_commit(w) != 0);
    chunk_rd *rd = NULL;
    CHECK(chunk_rd_open(st, &id, &rd) == TSDB_ERR_NOTFOUND);
    CHECK(chunk_wr_open(st, &id, &f.g, 1, &w) == 0);
    CHECK(chunk_wr_cell(w, cell, 50, true, xxh64(cell, 50, 0)) == 0);
    CHECK(chunk_wr_commit(w) == TSDB_ERR_INVALID); /* only one cell of three */
    CHECK(chunk_rd_open(st, &id, &rd) == TSDB_ERR_NOTFOUND);
    /* a put with the sums of the encode, and its scrub */
    uint64_t sums[16];
    uint8_t chunk0[400];
    fill(chunk0, sizeof chunk0);
    uint64_t pl = chunk_pos_len(&f.g, 0), off = 0;
    for (uint64_t i = 0; i < chunk_pos_cells(&f.g, 0); i++) {
        uint64_t cl = chunk_pos_cell_len(&f.g, 0, i);
        sums[i] = xxh64(chunk0 + off, cl, 0);
        off += cl;
    }
    CHECK(pl <= sizeof chunk0 && chunk_store_put(st, &id, &f.g, 1, chunk0, sums) == 0);
    CHECK(chunk_rd_open(st, &id, &rd) == 0);
    uint64_t bad = 99;
    CHECK(chunk_rd_scrub(rd, &bad) == 0 && bad == 0);
    uint8_t back[400];
    CHECK(chunk_rd_read(rd, 0, back, pl) == 0 && memcmp(back, chunk0, pl) == 0);
    CHECK(chunk_rd_read(rd, 1, back, pl) == TSDB_ERR_INVALID); /* past the cells */
    CHECK(chunk_rd_hdr(rd)->epoch == 1 && chunk_rd_hdr(rd)->pos == 0);
    chunk_rd_close(rd);
    sums[0] ^= 1;
    CHECK(chunk_store_put(st, &id, &f.g, 1, chunk0, sums) == TSDB_ERR_CORRUPT); /* the bytes are not the sum the sender said */
    /* a put over an existing chunk replaces it whole (a repair) */
    CHECK(chunk_store_put(st, &id, &f.g, 1, chunk0, NULL) == 0);
    fx_free(&f);
    printf("  the writer refuses a wrong cell length, a wrong given sum and an incomplete chunk, leaving nothing "
           "visible\n");
}

/* ---------------------------------------------------------------- stores, EPOCH, fences */
static int count_listed(void *ctx, const chunk_listed *c)
{
    int *n = ctx;
    if (c->status == CHUNK_ST_OK)
        (*n)++;
    else
        (*n) += 1000;
    return 0;
}

static void epoch_page_flip(const char *store, unsigned vol, int slot)
{
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/s%u/EPOCH", store, vol);
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0);
    uint8_t b = 0;
    CHECK(pread(fd, &b, 1, slot * 4096 + 9) == 1);
    b ^= 0x40;
    CHECK(pwrite(fd, &b, 1, slot * 4096 + 9) == 1);
    close(fd);
}

static void test_stores(void)
{
    char dir[PATH_MAX + 32], sp[PATH_MAX + 64];
    fresh("stores", dir, sizeof dir);
    snprintf(sp, sizeof sp, "%s/a", dir);
    uint8_t id[16], other[16];
    chunk_store *st;
    CHECK(chunk_store_open(sp, NULL, &st) == TSDB_ERR_NOTFOUND);
    CHECK(chunk_store_init(sp, NULL, id) == 0);
    CHECK(chunk_store_init(sp, NULL, other) == TSDB_ERR_INVALID); /* its id is not replaced */
    CHECK(chunk_store_open(sp, id, &st) == 0 && memcmp(chunk_store_id(st), id, 16) == 0);
    chunk_store_close(st);
    memcpy(other, id, 16);
    other[7] ^= 1;
    fprintf(stderr, "  (a refusal is logged next: another disk than the one told)\n");
    CHECK(chunk_store_open(sp, other, &st) == CHUNK_ERR_STORE_ID); /* a disk that came back as another (E27) */
    /* META damaged */
    char meta[PATH_MAX + 80];
    snprintf(meta, sizeof meta, "%s/META", sp);
    size_t n;
    uint8_t *m = slurp(meta, &n);
    quiet(true);
    for (size_t i = 0; i < n; i++) {
        m[i] ^= 0x01;
        spit(meta, m, n);
        CHECK(chunk_store_open(sp, NULL, &st) == TSDB_ERR_CORRUPT);
        m[i] ^= 0x01;
    }
    quiet(false);
    spit(meta, m, n);
    CHECK(chunk_store_open(sp, id, &st) == 0);
    chunk_store_close(st);
    free(m);

    /* EPOCH: raised durably, never lowered, torn slots */
    CHECK(chunk_store_open(sp, id, &st) == 0);
    CHECK(chunk_store_epoch(st, 5) == 0);
    CHECK(chunk_store_raise(st, 5, 4) == 0 && chunk_store_epoch(st, 5) == 4);
    CHECK(chunk_store_raise(st, 5, 2) == 0 && chunk_store_epoch(st, 5) == 4); /* never back */
    CHECK(chunk_store_raise(st, 5, 9) == 0 && chunk_store_epoch(st, 5) == 9);
    chunk_store_close(st);
    CHECK(chunk_store_open(sp, id, &st) == 0 && chunk_store_epoch(st, 5) == 9);
    chunk_store_close(st);
    for (int slot = 0; slot < 2; slot++) { /* one slot torn: the other holds the value before, or this one */
        epoch_page_flip(sp, 5, slot);
        CHECK(chunk_store_open(sp, id, &st) == 0);
        uint64_t e = chunk_store_epoch(st, 5);
        /* a torn newest slot is a torn store or rot, and they look alike: the other slot (the value before) is used.
           The backend's fence is the maximum over its stores, which covers it where there are several. */
        CHECK(e == (slot == 0 ? 4u : 12u));
        CHECK(chunk_store_raise(st, 5, 12 + (uint64_t)slot) == 0); /* the torn slot is the older generation: rewritten */
        chunk_store_close(st);
        CHECK(chunk_store_open(sp, id, &st) == 0 && chunk_store_epoch(st, 5) == 12 + (uint64_t)slot);
        chunk_store_close(st);
    }
    epoch_page_flip(sp, 5, 0);
    epoch_page_flip(sp, 5, 1);
    fprintf(stderr, "  (a refusal is logged next: both EPOCH slots bad)\n");
    CHECK(chunk_store_open(sp, id, &st) == TSDB_ERR_CORRUPT); /* the fence is lost: the open fails, it is not guessed */

    /* no EPOCH beside chunks: the open fails; an empty volume directory is fine */
    snprintf(sp, sizeof sp, "%s/b", dir);
    CHECK(chunk_store_init(sp, id, NULL) == 0 && chunk_store_open(sp, id, &st) == 0);
    chunk_geom g;
    CHECK(chunk_geom_make(&g, 2, 1, 16, 40) == 0);
    chunk_ident ci = {.volume = 7, .pos = 0};
    memset(ci.vuuid, 5, 16);
    memset(ci.oid, 6, 16);
    uint8_t cells[64];
    fill(cells, sizeof cells);
    CHECK(chunk_store_put(st, &ci, &g, 3, cells, NULL) == 0);
    chunk_store_close(st);
    char ep[PATH_MAX + 80];
    snprintf(ep, sizeof ep, "%s/s7/EPOCH", sp);
    char sv[PATH_MAX + 80];
    snprintf(sv, sizeof sv, "%s/s7/EPOCH.saved", sp);
    CHECK(rename(ep, sv) == 0);
    fprintf(stderr, "  (a refusal is logged next: chunks but no EPOCH)\n");
    CHECK(chunk_store_open(sp, id, &st) == TSDB_ERR_CORRUPT);
    CHECK(rename(sv, ep) == 0);

    /* tmp_* files a dead writer left are removed at open; list sees only chunks; damage shows in list */
    char tmp[PATH_MAX + 96];
    snprintf(tmp, sizeof tmp, "%s/s7/tmp_o00.0.0", sp);
    spit(tmp, cells, 10);
    CHECK(chunk_store_open(sp, id, &st) == 0 && access(tmp, F_OK) != 0);
    int listed = 0;
    CHECK(chunk_store_list(st, 7, true, count_listed, &listed) == 0 && listed == 1);
    CHECK(chunk_store_list(st, 99, true, count_listed, &listed) == 0 && listed == 1); /* no such volume: nothing */
    char cf[PATH_MAX + 160], hex[33];
    chunk_oid_hex(ci.oid, hex);
    snprintf(cf, sizeof cf, "%s/s7/o%s.0.chunk", sp, hex);
    size_t cn;
    uint8_t *cb = slurp(cf, &cn);
    cb[CHUNK_HDR_BYTES] ^= 1;
    spit(cf, cb, cn);
    listed = 0;
    CHECK(chunk_store_list(st, 7, false, count_listed, &listed) == 0 && listed == 1); /* a header-only listing */
    listed = 0;
    CHECK(chunk_store_list(st, 7, true, count_listed, &listed) == 0 && listed == 1); /* cells are the scrub's */
    chunk_rd *rd = NULL;
    uint64_t bad = 0;
    CHECK(chunk_rd_open(st, &ci, &rd) == 0 && chunk_rd_scrub(rd, &bad) == 0 && bad == 1);
    chunk_rd_close(rd);
    cb[CHUNK_HDR_BYTES] ^= 1;
    cb[5] ^= 1;
    spit(cf, cb, cn);
    listed = 0;
    CHECK(chunk_store_list(st, 7, false, count_listed, &listed) == 0 && listed == 1000);
    chunk_store_close(st);
    free(cb);
    printf("  stores: META and the store id, EPOCH (never back, torn slot, both bad, none beside chunks), tmp_* removed, "
           "list\n");
}

static void test_fences(void)
{
    fx f;
    fx_make(&f, "fx-fence", 3, 4, 2, 32, 32 * 11 + 1, 5);
    chunk_ident id = {.volume = f.vol, .pos = 2};
    memcpy(id.vuuid, f.vuuid, 16);
    memcpy(id.oid, f.oid, 16);
    CHECK(chunk_be_epoch(f.be, f.vol) == 5);
    /* below the epoch: refused, nothing made */
    chunk_ident id2 = id;
    chunk_random(id2.oid, 16);
    uint8_t cells[400];
    fill(cells, sizeof cells);
    CHECK(chunk_be_put(f.be, -1, &id2, &f.g, 4, cells, NULL) == CHUNK_ERR_FENCED);
    chunk_rd *rd = NULL;
    CHECK(chunk_be_rd_open(f.be, &id2, &rd) == TSDB_ERR_NOTFOUND);
    uint8_t *obj2 = malloc(f.g.len);
    fill(obj2, f.g.len);
    CHECK(chunk_be_put_object(f.be, f.vol, f.vuuid, id2.oid, &f.g, 4, obj2, ~0u, NULL) == CHUNK_ERR_FENCED);
    CHECK(chunk_be_del(f.be, &id, 4) == CHUNK_ERR_FENCED);
    chunk_rd *r0 = NULL;
    CHECK(chunk_be_rd_open(f.be, &id, &r0) == 0); /* still there; reads are never fenced */
    chunk_rd_close(r0);
    /* at the epoch: allowed; above: raises it in every store, durably */
    CHECK(chunk_be_put_object(f.be, f.vol, f.vuuid, id2.oid, &f.g, 5, obj2, ~0u, NULL) == 0);
    CHECK(chunk_be_put_object(f.be, f.vol, f.vuuid, id2.oid, &f.g, 8, obj2, ~0u, NULL) == 0);
    CHECK(chunk_be_epoch(f.be, f.vol) == 8);
    for (int i = 0; i < 3; i++)
        CHECK(chunk_store_epoch(chunk_be_store(f.be, i), f.vol) == 8);
    /* a delete below the chunk's own writer epoch is refused even where the store's epoch is lower (E14): the chunk
       of epoch 8 is copied into a store that has only seen 6 */
    char src[PATH_MAX + 128], dst[PATH_MAX + 128], hex[33];
    chunk_oid_hex(id2.oid, hex);
    int st_of_pos0 = (chunk_be_place(f.be, id2.oid, 0)) % 3;
    snprintf(src, sizeof src, "%s/s%u/o%s.0.chunk", f.paths[st_of_pos0], f.vol, hex);
    chunk_store *fresh_st;
    char fp[PATH_MAX + 64];
    snprintf(fp, sizeof fp, "%s/fresh", f.dir);
    CHECK(chunk_store_init(fp, NULL, NULL) == 0 && chunk_store_open(fp, NULL, &fresh_st) == 0);
    CHECK(chunk_store_raise(fresh_st, f.vol, 6) == 0);
    snprintf(dst, sizeof dst, "%s/s%u/o%s.0.chunk", fp, f.vol, hex);
    size_t n;
    uint8_t *b = slurp(src, &n);
    spit(dst, b, n);
    free(b);
    chunk_ident id0 = id2;
    id0.pos = 0;
    CHECK(chunk_store_del(fresh_st, &id0, 7) == CHUNK_ERR_FENCED); /* 7 >= the store's 6, below the chunk's 8 */
    CHECK(chunk_store_epoch(fresh_st, f.vol) == 7);                /* the delete showed the store an epoch: kept */
    CHECK(chunk_store_del(fresh_st, &id0, 8) == 0);
    CHECK(chunk_store_del(fresh_st, &id0, 8) == TSDB_ERR_NOTFOUND);
    chunk_store_close(fresh_st);
    /* a delete at the epoch removes the chunk from every store that has it */
    CHECK(chunk_be_del(f.be, &id, 8) == 0);
    CHECK(chunk_be_rd_open(f.be, &id, &rd) == TSDB_ERR_NOTFOUND);
    CHECK(chunk_be_del(f.be, &id, 8) == TSDB_ERR_NOTFOUND);
    /* the fence survives a reopen, and is the maximum over the stores */
    chunk_be_close(f.be);
    f.be = NULL;
    chunk_store *s1;
    CHECK(chunk_store_open(f.paths[1], NULL, &s1) == 0 && chunk_store_raise(s1, f.vol, 11) == 0);
    chunk_store_close(s1);
    CHECK(chunk_be_open(&f.be, f.pp, 3, NULL) == 0);
    CHECK(chunk_be_epoch(f.be, f.vol) == 11);
    CHECK(chunk_be_put(f.be, 0, &id2, &f.g, 9, cells, NULL) == CHUNK_ERR_FENCED); /* store 0 only knows 8 */
    CHECK(chunk_be_see_epoch(f.be, f.vol, 12) == 0);
    CHECK(chunk_store_epoch(chunk_be_store(f.be, 0), f.vol) == 12);
    /* ids: the backend refuses a store that is not the one named */
    uint8_t ids[3 * 16];
    for (int i = 0; i < 3; i++)
        memcpy(ids + 16 * i, chunk_store_id(chunk_be_store(f.be, i)), 16);
    chunk_backend *be2;
    CHECK(chunk_be_open(&be2, f.pp, 3, ids) == 0);
    chunk_be_close(be2);
    ids[16 + 3] ^= 1;
    fprintf(stderr, "  (a refusal is logged next: a store that is not the one named)\n");
    CHECK(chunk_be_open(&be2, f.pp, 3, ids) == CHUNK_ERR_STORE_ID);
    free(obj2);
    fx_free(&f);
    printf("  fences: a put or delete below the epoch refused, above it raises every store, a delete below the chunk's "
           "epoch refused, the maximum over the stores, store ids\n");
}

/* ---------------------------------------------------------------- power loss and I/O errors (crashfs) */
static void ack(const char *file, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void ack(const char *file, const char *fmt, ...)
{
    FILE *f = fopen(file, "a");
    va_list ap;
    va_start(ap, fmt);
    if (f) {
        vfprintf(f, fmt, ap);
        fputc('\n', f);
        fclose(f);
    }
    va_end(ap);
}

typedef struct wl { /* the workload's objects */
    chunk_geom g;
    uint8_t vuuid[16];
    uint8_t oid[2][16];
    msrc ms[2];
    uint8_t *obj[2];
} wl;

static void wl_make(wl *w)
{
    memset(w, 0, sizeof *w);
    CHECK(chunk_geom_make(&w->g, 4, 2, 48, 48 * 9 + 5) == 0);
    memset(w->vuuid, 0x33, 16);
    for (int o = 0; o < 2; o++) {
        memset(w->oid[o], 0x40 + o, 16);
        w->obj[o] = malloc(w->g.len);
        for (size_t i = 0; i < w->g.len; i++)
            w->obj[o][i] = (uint8_t)(i * 7 + o * 31 + (i >> 5));
        ms_build(&w->ms[o], &w->g, w->obj[o]);
    }
}

/* The workload: epoch 3, object 0 at epoch 3, epoch 5, object 1 at epoch 5, delete object 0 at epoch 5. Every
 * acknowledged step goes to the ack file, which is outside the shim's root. With a single store (eio) the child also
 * checks that nothing succeeds once the store reports it failed. */
static int child_workload(const char *dir, const char *ackf, int nstores)
{
    char p[2][PATH_MAX + 96];
    const char *pp[2] = {p[0], p[1]};
    for (int i = 0; i < nstores; i++)
        snprintf(p[i], sizeof p[i], "%s/st%d", dir, i);
    chunk_backend *be;
    if (chunk_be_open(&be, pp, nstores, NULL) != 0)
        return 2;
    wl w;
    wl_make(&w);
    int bad = 0;
    bool failed_seen = false;
#define STEP(call, ...)                                                                       \
    do {                                                                                      \
        int rc_ = (call);                                                                     \
        bool was_ = nstores == 1 && chunk_store_failed(chunk_be_store(be, 0));                \
        if (rc_ == 0) {                                                                       \
            if (was_)                                                                         \
                bad = 3; /* a store that failed acknowledged something */                      \
            ack(ackf, __VA_ARGS__);                                                           \
        } else if (nstores == 1 && chunk_store_failed(chunk_be_store(be, 0)))                \
            failed_seen = true;                                                               \
    } while (0)
    STEP(chunk_be_see_epoch(be, 1, 3), "E 3");
    for (unsigned pos = 0; pos < chunk_npos(&w.g); pos++) {
        chunk_ident id = {.volume = 1, .pos = (uint16_t)pos};
        memcpy(id.vuuid, w.vuuid, 16);
        memcpy(id.oid, w.oid[0], 16);
        STEP(chunk_be_put(be, -1, &id, &w.g, 3, w.ms[0].cells[pos], w.ms[0].sums[pos]), "C 0 %u", pos);
    }
    STEP(chunk_be_see_epoch(be, 1, 5), "E 5");
    for (unsigned pos = 0; pos < chunk_npos(&w.g); pos++) {
        chunk_ident id = {.volume = 1, .pos = (uint16_t)pos};
        memcpy(id.vuuid, w.vuuid, 16);
        memcpy(id.oid, w.oid[1], 16);
        STEP(chunk_be_put(be, -1, &id, &w.g, 5, w.ms[1].cells[pos], NULL), "C 1 %u", pos);
    }
    for (unsigned pos = 0; pos < chunk_npos(&w.g); pos++) {
        chunk_ident id = {.volume = 1, .pos = (uint16_t)pos};
        memcpy(id.vuuid, w.vuuid, 16);
        memcpy(id.oid, w.oid[0], 16);
        ack(ackf, "A 0 %u", pos); /* attempted: a delete that failed may still have removed the name */
        STEP(chunk_be_del(be, &id, 5), "D 0 %u", pos);
    }
    (void)failed_seen;
    chunk_be_close(be);
    return bad;
}

static int run_child(const char *dir, const char *ackf, const char *mode, int warmup, int seed, int nstores)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16], ns[8];
        /* the child ends in _exit or a power cut: its workload buffers and the shim's kept pages are not freed, which a
         * leak check at exit would report as a failure (exit 1) */
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        snprintf(ns, sizeof ns, "%d", nstores);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        if (strcmp(mode, "eio") == 0)
            setenv("CRASHFS_MODE", "eio", 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_chunk", "--child", dir, ackf, ns, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* After a power loss or an I/O error: every chunk present is whole, every acknowledged one is there and no
 * acknowledged delete came back, and EPOCH is at least every acknowledged epoch. */
static void check_after(const char *dir, const char *ackf, int nstores, int k, const char *what)
{
    char p[2][PATH_MAX + 96];
    const char *pp[2] = {p[0], p[1]};
    for (int i = 0; i < nstores; i++)
        snprintf(p[i], sizeof p[i], "%s/st%d", dir, i);
    chunk_backend *be;
    int rc = chunk_be_open(&be, pp, nstores, NULL);
    if (rc != 0) {
        fprintf(stderr, "  %s step %d: the stores do not open (%d)\n", what, k, rc);
        CHECK(rc == 0);
        return;
    }
    wl w;
    wl_make(&w);
    uint64_t max_e = 0;
    bool acked_c[2][CHUNK_POS_MAX] = {{false}}, acked_d[CHUNK_POS_MAX] = {false}, tried_d[CHUNK_POS_MAX] = {false};
    FILE *f = fopen(ackf, "r");
    char kind;
    unsigned a, b;
    char line[64];
    while (f && fgets(line, sizeof line, f)) {
        if (sscanf(line, "%c %u %u", &kind, &a, &b) == 3 && kind == 'C')
            acked_c[a][b] = true;
        else if (sscanf(line, "%c %u %u", &kind, &a, &b) == 3 && kind == 'D')
            acked_d[b] = true;
        else if (sscanf(line, "%c %u %u", &kind, &a, &b) == 3 && kind == 'A')
            tried_d[b] = true;
        else if (sscanf(line, "%c %u", &kind, &a) == 2 && kind == 'E' && a > max_e)
            max_e = a;
    }
    if (f)
        fclose(f);
    if (chunk_be_epoch(be, 1) < max_e) {
        fprintf(stderr, "  %s step %d: EPOCH %" PRIu64 " after an acknowledged %" PRIu64 "\n", what, k,
                chunk_be_epoch(be, 1), max_e);
        CHECK(false);
    }
    for (int o = 0; o < 2; o++)
        for (unsigned pos = 0; pos < chunk_npos(&w.g); pos++) {
            chunk_ident id = {.volume = 1, .pos = (uint16_t)pos};
            memcpy(id.vuuid, w.vuuid, 16);
            memcpy(id.oid, w.oid[o], 16);
            chunk_rd *rd = NULL;
            int r = chunk_be_rd_open(be, &id, &rd);
            if (r == 0) {
                uint64_t bad = 1;
                int sr = chunk_rd_scrub(rd, &bad);
                if (sr != 0 || bad != 0) {
                    fprintf(stderr, "  %s step %d: chunk %d.%u is present but damaged\n", what, k, o, pos);
                    CHECK(false);
                }
                chunk_rd_close(rd);
            } else if (r != TSDB_ERR_NOTFOUND) {
                fprintf(stderr, "  %s step %d: chunk %d.%u is present but torn (%d): a chunk is whole or absent\n", what,
                        k, o, pos, r);
                CHECK(false);
            }
            bool present = r == 0;
            if (acked_c[o][pos] && !(o == 0 && tried_d[pos]) && !present) {
                fprintf(stderr, "  %s step %d: acknowledged chunk %d.%u is gone\n", what, k, o, pos);
                CHECK(false);
            }
            if (o == 0 && acked_d[pos] && present) {
                fprintf(stderr, "  %s step %d: acknowledged delete of chunk %d.%u came back\n", what, k, o, pos);
                CHECK(false);
            }
        }
    /* nothing that is listed is anything but whole */
    int listed = 0;
    CHECK(chunk_be_list(be, 1, true, count_listed, &listed) == 0 && listed < 1000);
    chunk_be_close(be);
    for (int o = 0; o < 2; o++)
        ms_free(&w.ms[o]), free(w.obj[o]);
}

#define WORKLOAD_ACKS (1 + 6 + 1 + 6 + 6 + 6) /* the attempts of the deletes are lines too */
static int count_lines(const char *file)
{
    FILE *f = fopen(file, "r");
    int n = 0, c;
    while (f && (c = fgetc(f)) != EOF)
        n += c == '\n';
    if (f)
        fclose(f);
    return n;
}

static void sweep(const char *mode, int nstores, int stride)
{
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32];
    snprintf(ackf, sizeof ackf, "%s/%s.ack", S, mode);
    int crashes = 0, finished = 0, steps = 0;
    for (int k = 0; k < 4000 && !finished; k += stride) {
        fresh(mode, dir, sizeof dir);
        unlink(ackf);
        for (int i = 0; i < nstores; i++) {
            char sp[PATH_MAX + 64];
            snprintf(sp, sizeof sp, "%s/st%d", dir, i);
            CHECK(chunk_store_init(sp, NULL, NULL) == 0);
        }
        int code = run_child(dir, ackf, mode, k, k + 1, nstores);
        bool power = strcmp(mode, "power") == 0;
        if (!(code == 0 || (power && code == 99)))
            fprintf(ERR, "  %s step %d: the child exited with %d\n", mode, k, code);
        CHECK(code == 0 || (power && code == 99));
        if (code != 0 && !(power && code == 99))
            break;
        crashes += code == 99;
        steps++;
        int before = failures;
        check_after(dir, ackf, nstores, k, mode);
        if (failures != before)
            break; /* the directory stays for a look */
        /* a power run ends with the first that finishes; an eio run when every step of the workload was acknowledged
           (any fault makes some call fail, so none was injected: the fault point is past the workload) */
        finished = power ? code == 0 : count_lines(ackf) == WORKLOAD_ACKS;
    }
    CHECK(finished);
    if (strcmp(mode, "power") == 0)
        CHECK(crashes > 20);
    printf("  %s: %d fault points in puts, EPOCH stores and deletes on %d store%s, each followed by the checks\n", mode,
           steps, nstores, nstores == 1 ? "" : "s");
}

static void test_crashfs(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    sweep("power", 2, quick ? 4 : 1);
    sweep("eio", 1, quick ? 3 : 1);
}

/* ---------------------------------------------------------------- main */
static void set_mutant(const char *m)
{
    if (!strcmp(m, "nosums"))
        chunk_mutant = CHUNK_MUT_NOSUMS;
    else if (!strcmp(m, "nofence"))
        chunk_mutant = CHUNK_MUT_NOFENCE;
    else if (!strcmp(m, "eof"))
        chunk_mutant = CHUNK_MUT_EOF;
    else if (!strcmp(m, "settings"))
        chunk_mutant = CHUNK_MUT_SETTINGS;
    else if (!strcmp(m, "nosync"))
        chunk_mutant = CHUNK_MUT_NOSYNC;
    else if (!strcmp(m, "epochnosync"))
        chunk_mutant = CHUNK_MUT_EPOCH_NOSYNC;
    else {
        fprintf(stderr, "test_chunk: no mutant %s\n", m);
        exit(2);
    }
}

int main(int argc, char **argv)
{
    if (argc >= 5 && strcmp(argv[1], "--child") == 0) {
        if (chunk_mutant == 0 && getenv("TSDB_CHUNK_MUTANT"))
            set_mutant(getenv("TSDB_CHUNK_MUTANT"));
        return child_workload(argv[2], argv[3], atoi(argv[4]));
    }
    const char *mutant = NULL, *base = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = true;
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else
            base = argv[i];
    }
    if (getenv("TSDB_CHUNK_QUICK"))
        quick = true;
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/tsdb-chunk-XXXXXX", base);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    if (mutant) {
        set_mutant(mutant);
        setenv("TSDB_CHUNK_MUTANT", mutant, 1); /* the power-loss children are other processes */
    }
    printf("test_chunk%s%s\n", quick ? " (quick)" : "", mutant ? " (mutant)" : "");
    ERR = fdopen(dup(2), "w");
    setvbuf(ERR, NULL, _IONBF, 0);
    test_geometry();
    test_image();
    quiet(true);
    test_memory();
    test_files();
    test_derived();
    test_other_encoding();
    test_geometry_from_object();
    test_short_read();
    quiet(false);
    test_args_and_writer();
    test_stores();
    test_fences();
    test_crashfs();
    if (failures) {
        fprintf(stderr, "test_chunk: %d check(s) failed (scratch %s)\n", failures, S);
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_chunk: ok\n");
    return 0;
}
#endif
