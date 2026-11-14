/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The decode path of objects (src/chunkread.c over the codec of src/ec) on arbitrary inputs: an object cut into cells
 * and encoded with a geometry the input names, chunks damaged as the input says (absent, unreadable, flipped, cut),
 * and byte ranges the input chooses. Whatever the input:
 *
 *  - up to m unusable chunks: every range reads back byte for byte what was encoded (with the object record's sums and
 *    without, whole cells and, for the chunks that are only absent, the raw mode too);
 *  - more than m: an error or the right bytes (a stripe may still have enough cells), never other bytes;
 *  - a range outside the object is refused and nothing is read outside the buffers (the sanitizer's to find);
 *  - the codec on shards that are not a codeword at all (the input's bytes as the shards, any pattern of present
 *    ones): decode must not crash or write outside the shards, and what it rebuilds, with the first k present shards,
 *    is the codeword that those shards fix (re-encoding the rebuilt data reproduces every one of them).
 *
 * Input: byte 0 selects the mode; the rest are parameters and data (see below).
 */
#include "fuzz.h"
#include "internal.h"
#include "chunk.h"

#if !defined(__x86_64__) /* the codec is x86-64 code (the Makefile builds no src/ec elsewhere): nothing to fuzz */
int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    (void)data, (void)size;
    return 0;
}
void fuzz_seeds(const char *dir) { (void)dir; }
#else
#include "ec/rs.h"

typedef struct msrc {
    chunk_geom g;
    uint8_t *cells[CHUNK_POS_MAX];
    size_t clen[CHUNK_POS_MAX];
    uint64_t *sums[CHUNK_POS_MAX];
    int fail[CHUNK_POS_MAX];
} msrc;

static int ms_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    msrc *m = ctx;
    if (m->fail[pos])
        return m->fail[pos];
    if (off + len > m->clen[pos])
        return CHUNK_ST_IOERR;
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

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    chunk_log_quiet = 1;
    (void)cpu_level_max(); /* the codec's kernels are chosen by src/cpu.c's constructor: have it linked */
    return 0;
}

typedef struct rd {
    const uint8_t *p;
    size_t n, at;
} rd;
static unsigned take(rd *r) { return r->at < r->n ? r->p[r->at++] : 0; }

static void mode_object(rd *r)
{
    unsigned m = 1 + take(r) % 4, k = 1 + take(r) % 16, cell = 1 + take(r) % 40;
    uint64_t cells = 1 + take(r) % 40, len = (cells - 1) * cell + 1 + take(r) % cell;
    chunk_geom g;
    if (chunk_geom_make(&g, k, m, cell, len) != 0) /* more data chunks than cells: the derived k' (E7) */
        if (chunk_geom_make(&g, cells < k ? (unsigned)cells : k, m, cell, len) != 0)
            return;
    uint8_t *obj = malloc(g.len);
    for (uint64_t i = 0; i < g.len; i++)
        obj[i] = r->at < r->n ? r->p[r->at++] : (uint8_t)(i * 29 + (i >> 4));
    msrc ms;
    memset(&ms, 0, sizeof ms);
    ms.g = g;
    unsigned np = chunk_npos(&g);
    for (unsigned p = 0; p < np; p++) {
        ms.clen[p] = (size_t)chunk_pos_len(&g, p);
        ms.cells[p] = calloc(ms.clen[p] + 1, 1);
        ms.sums[p] = calloc(chunk_pos_cells(&g, p) + 1, 8);
    }
    uint64_t *rec = calloc(chunk_sums_total(&g) + 1, 8);
    if (chunk_encode(&g, obj, ms_collect, &ms, rec) != 0)
        abort();
    /* damage: per position a kind from the input */
    unsigned nbad = 0, nflip = 0;
    uint8_t *orig[CHUNK_POS_MAX] = {0};
    for (unsigned p = 0; p < np; p++) {
        unsigned kind = take(r) % 6;
        if (kind == 0 || kind > 4)
            continue;
        nbad++;
        orig[p] = malloc(ms.clen[p] + 1);
        memcpy(orig[p], ms.cells[p], ms.clen[p]);
        switch (kind) {
        case 1: ms.fail[p] = CHUNK_ST_ABSENT; break;
        case 2: ms.fail[p] = CHUNK_ST_IOERR; break;
        case 3:
            ms.cells[p][take(r) * 256 % (ms.clen[p] ? ms.clen[p] : 1)] ^= (uint8_t)(1 + take(r) % 255);
            nflip++;
            break;
        case 4: ms.clen[p] = take(r) % (ms.clen[p] + 1); break;
        }
    }
    chunk_source src;
    memset(&src, 0, sizeof src);
    src.ctx = &ms;
    snprintf(src.name, sizeof src.name, "fuzz");
    src.read = ms_read;
    src.sums = ms_sums;
    uint8_t *buf = malloc(g.len + 8);
    for (int q = 0; q < 6; q++) {
        uint64_t off = (take(r) | take(r) << 8) % (g.len + 2), len = (take(r) | take(r) << 8) % (g.len + 2);
        if (q == 0)
            off = 0, len = g.len;
        for (int use_rec = 0; use_rec < 2; use_rec++) {
            memset(buf, 0xA5, g.len + 8);
            chunk_report rep;
            int rc = chunk_read_range(&src, &g, use_rec ? rec : NULL, off, len, buf, 0, &rep);
            if (off > g.len || len > g.len - off) {
                if (rc != TSDB_ERR_INVALID)
                    abort();
                continue;
            }
            if (rc == 0) {
                if (memcmp(buf, obj + off, (size_t)len) != 0)
                    abort(); /* wrong bytes: the one thing it must never give */
            } else if (nbad <= m) {
                abort(); /* up to m unusable chunks is always readable */
            }
            if (nflip == 0 && nbad <= m && use_rec == 0) { /* raw reads: the chunks are absent or cut, never rotten */
                memset(buf, 0xA5, g.len + 8);
                if (chunk_read_range(&src, &g, NULL, off, len, buf, CHUNK_RR_RAW, &rep) != 0 ||
                    memcmp(buf, obj + off, (size_t)len) != 0)
                    abort();
            }
            for (int i = 0; i < 8; i++)
                if (buf[g.len + (unsigned)i] != 0xA5)
                    abort(); /* a write past the object's length */
        }
    }
    free(buf);
    for (unsigned p = 0; p < np; p++) {
        free(ms.cells[p]);
        free(ms.sums[p]);
        free(orig[p]);
    }
    free(rec);
    free(obj);
}

/* The codec on shards that are not a codeword: any bytes, any pattern. */
static void mode_codec(rd *r)
{
    unsigned k = 1 + take(r) % 16, m = 1 + take(r) % 4, n = k + m;
    size_t len = 1 + take(r) % 70;
    iso_rs *rs = iso_rs_new(k, m);
    if (!rs)
        abort();
    uint8_t *mem = calloc(n, len + 16), *shards[CHUNK_POS_MAX];
    bool present[CHUNK_POS_MAX];
    unsigned have = 0;
    for (unsigned i = 0; i < n; i++) {
        shards[i] = mem + i * (len + 16) + 8; /* canaries around each */
        present[i] = take(r) % 3 != 0;
        have += present[i];
        for (size_t j = 0; j < len; j++)
            shards[i][j] = r->at < r->n ? r->p[r->at++] : 0;
    }
    for (unsigned i = 0; i < n; i++) {
        memset(shards[i] - 8, 0x5C, 8);
        memset(shards[i] + len, 0x5C, 8);
    }
    unsigned srcs[CHUNK_K_MAX], ns = 0;
    for (unsigned i = 0; i < n && ns < k; i++)
        if (present[i])
            srcs[ns++] = i;
    uint8_t *before = malloc(n * len);
    for (unsigned i = 0; i < n; i++)
        memcpy(before + i * len, shards[i], len);
    iso_err e = iso_rs_decode(rs, shards, present, len);
    if ((have >= k) != (e == ISO_OK))
        abort();
    if (e == ISO_OK) {
        for (unsigned i = 0; i < n; i++) {
            for (int j = 0; j < 8; j++)
                if (shards[i][-1 - j] != 0x5C || shards[i][len + (unsigned)j] != 0x5C)
                    abort();
            if (present[i] && memcmp(before + i * len, shards[i], len) != 0)
                abort(); /* a present shard is never rewritten */
        }
        /* the rebuilt data and the first k present shards are one codeword */
        uint8_t *tmp = calloc(n, len), *tp[CHUNK_POS_MAX];
        for (unsigned i = 0; i < n; i++) {
            tp[i] = tmp + i * len;
            if (i < k)
                memcpy(tp[i], shards[i], len);
        }
        iso_rs_encode(rs, tp, len);
        for (unsigned s = 0; s < ns; s++)
            if (memcmp(tp[srcs[s]], before + srcs[s] * len, len) != 0)
                abort();
        free(tmp);
    }
    free(before);
    free(mem);
    iso_rs_free(rs);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    uint8_t *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    memcpy(p, data, size);
    rd r = {p, size, 0};
    if (take(&r) & 1)
        mode_codec(&r);
    else
        mode_object(&r);
    free(p);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    uint8_t b[256];
    for (int s = 0; s < 8; s++) {
        for (int i = 0; i < 256; i++)
            b[i] = (uint8_t)(i * (3 + s) + s * 17 + (i >> 3));
        b[0] = 0; /* object mode */
        b[1] = (uint8_t)(s % 4);     /* m - 1 */
        b[2] = (uint8_t)(s * 2 + 1); /* k - 1 */
        char nm[40];
        snprintf(nm, sizeof nm, "seed-object-%d", s);
        fuzz_put(dir, nm, b, sizeof b);
    }
    for (int s = 0; s < 4; s++) {
        for (int i = 0; i < 256; i++)
            b[i] = (uint8_t)(i * (5 + s) + s);
        b[0] = 1; /* codec mode */
        b[1] = (uint8_t)(s * 5);
        b[2] = (uint8_t)s;
        char nm[40];
        snprintf(nm, sizeof nm, "seed-codec-%d", s);
        fuzz_put(dir, nm, b, sizeof b);
    }
}
#endif
