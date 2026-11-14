/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A chunk file (src/chunk.c, src/chunkstore.c) from any bytes: a header, cells, a table of sums and a footer that
 * damage, a torn write or a bad disk may have left in any state. Two readers must agree and neither may read outside
 * what it was given:
 *
 *  - chunk_image_parse, a pure function of the bytes, and the file reader (a store, chunk_rd_open, the scrub, the
 *    listing), which does the same piece by piece from a file. Where the image parses, the file opens, the scrub finds
 *    no bad cell exactly when the check of the cells passes, and the listing says OK; where it does not, the file
 *    reader refuses it (never a chunk with an error taken for the end, never a different verdict);
 *  - what is accepted is the one encoding: chunk_image_build from the parsed header, cells and sums gives the very same
 *    bytes (a reserved field, a longer file or a shorter one is damage, not a second spelling);
 *  - a header that decodes names a valid geometry and a chunk length that geometry gives.
 *
 * Input: byte 0 (bit 0: "mend"), then the chunk file's bytes. With mend set and a header that decodes, the footer is
 * recomputed over the header and the sums table before the file is used, so that a mutation of the cells or of a sum
 * reaches the check of the cells instead of stopping at the footer's 64-bit hash (which a mutator never matches). The
 * harness puts the bytes in a store's volume directory under the object id and position the header names (so that an
 * identity check has something to pass), reads them back every way, and removes them.
 */
#include "fuzz.h"
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <dirent.h>

static chunk_store *st;
static char vdir[1200];

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    const char *d = fuzz_dir("chunk");
    char sp[1100];
    snprintf(sp, sizeof sp, "%s/store", d);
    if (chunk_store_init(sp, NULL, NULL) != 0 || chunk_store_open(sp, NULL, &st) != 0 || chunk_store_raise(st, 1, 1) != 0) {
        fprintf(stderr, "fuzz_chunk: cannot make the store\n");
        exit(1);
    }
    snprintf(vdir, sizeof vdir, "%s/s1", sp);
    return 0;
}

static int listed_status;
static int on_list(void *ctx, const chunk_listed *c)
{
    (void)ctx;
    listed_status = c->status;
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size == 0)
        return 0;
    bool mend = data[0] & 1;
    data++, size--;
    /* an exact-size copy: a read past the end is the sanitizer's to find */
    uint8_t *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    memcpy(p, data, size);
    chunk_hdr hh;
    if (mend && size >= CHUNK_HDR_BYTES + CHUNK_FTR_BYTES && chunk_hdr_decode(p, &hh) == 0 &&
        size == chunk_file_size(&hh.g, hh.pos))
        chunk_ftr_make(p + size - CHUNK_FTR_BYTES, p, p + CHUNK_HDR_BYTES + hh.chunk_len, chunk_pos_cells(&hh.g, hh.pos));

    chunk_hdr h;
    const uint8_t *cells, *sums;
    int whole = chunk_image_parse(p, size, true, &h, &cells, &sums);
    chunk_hdr h2;
    int shape = chunk_image_parse(p, size, false, &h2, &cells, &sums);
    if (whole == 0 && shape != 0)
        abort(); /* the check of the cells only adds a reason to refuse */
    if (shape == 0) {
        if (!chunk_geom_valid(&h2.g) || h2.pos >= chunk_npos(&h2.g) || h2.chunk_len != chunk_pos_len(&h2.g, h2.pos) ||
            size != chunk_file_size(&h2.g, h2.pos))
            abort();
        uint64_t nc = chunk_pos_cells(&h2.g, h2.pos), *tab = malloc((nc + 1) * 8);
        for (uint64_t i = 0; i < nc; i++) {
            uint64_t v = 0;
            for (int b = 7; b >= 0; b--)
                v = v << 8 | sums[8 * i + (uint64_t)b];
            tab[i] = v;
        }
        size_t n2;
        uint8_t *again = chunk_image_build(&h2, cells, tab, &n2);
        free(tab);
        if (!again || n2 != size || memcmp(again, p, size) != 0)
            abort(); /* one encoding */
        free(again);
    }

    /* the file reader, under the name the header (as far as it goes) gives it */
    uint8_t oid[16] = {0}, vuuid[16] = {0};
    unsigned pos = 0;
    if (size >= CHUNK_HDR_BYTES) {
        memcpy(vuuid, p + 16, 16);
        memcpy(oid, p + 32, 16);
        pos = (unsigned)(p[68] | p[69] << 8);
        if (pos >= CHUNK_POS_MAX)
            pos = 0;
    }
    char hex[33], path[1300];
    chunk_oid_hex(oid, hex);
    snprintf(path, sizeof path, "%s/o%s.%u.chunk", vdir, hex, pos);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0 || (size && write(fd, p, size) != (ssize_t)size))
        abort();
    close(fd);

    chunk_ident id = {.volume = 1, .pos = (uint16_t)pos};
    memcpy(id.vuuid, vuuid, 16);
    memcpy(id.oid, oid, 16);
    chunk_rd *rd;
    int rc = chunk_rd_open(st, &id, &rd);
    if (shape == 0 && h2.pos == pos && rc != 0)
        abort(); /* the image is whole but the reader refused it */
    if (rc == 0) {
        if (shape != 0)
            abort(); /* the reader took what the parser refused */
        uint64_t bad = 0;
        if (chunk_rd_scrub(rd, &bad) != 0 || (bad == 0) != (whole == 0))
            abort();
        const chunk_hdr *rh = chunk_rd_hdr(rd);
        if (memcmp(&rh->g, &h2.g, sizeof h2.g) != 0 || rh->epoch != h2.epoch || rh->pos != h2.pos)
            abort();
        uint8_t *buf = malloc(h2.chunk_len + 1);
        if (chunk_rd_read(rd, 0, buf, (size_t)h2.chunk_len) != 0 || memcmp(buf, cells, (size_t)h2.chunk_len) != 0)
            abort();
        free(buf);
        chunk_rd_close(rd);
    }
    listed_status = -1;
    if (chunk_store_list(st, 1, true, on_list, NULL) != 0)
        abort();
    if (listed_status != -1 && (listed_status == CHUNK_ST_OK) != (rc == 0))
        abort();
    unlink(path);
    free(p);
    return 0;
}

/* the seed files carry the mend byte in front */
static void put_seed(const char *dir, const char *name, int mend, const uint8_t *img, size_t n)
{
    uint8_t *b = malloc(n + 1);
    b[0] = (uint8_t)mend;
    memcpy(b + 1, img, n);
    fuzz_put(dir, name, b, n + 1);
    free(b);
}

static void seed_for(const char *dir, const char *name, unsigned k, unsigned m, uint32_t cell, uint64_t len, unsigned pos)
{
    chunk_geom g;
    if (chunk_geom_make(&g, k, m, cell, len) != 0)
        abort();
    uint8_t *obj = malloc(len);
    for (uint64_t i = 0; i < len; i++)
        obj[i] = (uint8_t)(i * 31 + (i >> 7));
    chunk_hdr h;
    memset(&h, 0, sizeof h);
    memset(h.vuuid, 0x11, 16);
    memset(h.oid, 0x22, 16);
    h.epoch = 7;
    h.g = g;
    h.pos = (uint16_t)pos;
    h.chunk_len = chunk_pos_len(&g, pos);
    uint64_t nc = chunk_pos_cells(&g, pos), *sums = calloc(nc + 1, 8);
    uint8_t *cl = calloc(h.chunk_len + 1, 1);
    /* the data chunk's cells straight from the object, for a data position; parity positions get other bytes */
    if (pos < k) {
        uint64_t off = 0;
        for (uint64_t i = 0; i < nc; i++) {
            uint64_t c = i * k + pos, n = chunk_cell_len(&g, c);
            memcpy(cl + off, obj + c * cell, n);
            sums[i] = xxh64(cl + off, n, 0);
            off += n;
        }
    } else {
        for (uint64_t i = 0; i < h.chunk_len; i++)
            cl[i] = (uint8_t)(i * 13 + 5);
        uint64_t off = 0;
        for (uint64_t i = 0; i < nc; i++) {
            uint64_t n = chunk_stripe_len(&g, i);
            sums[i] = xxh64(cl + off, n, 0);
            off += n;
        }
    }
    size_t n;
    uint8_t *img = chunk_image_build(&h, cl, sums, &n);
    put_seed(dir, name, 0, img, n);
    char nm[160];
    snprintf(nm, sizeof nm, "%s-flip-cell", name);
    img[CHUNK_HDR_BYTES] ^= 0x20;
    put_seed(dir, nm, 0, img, n);
    snprintf(nm, sizeof nm, "%s-flip-cell-mended", name); /* the footer made right: the cell check is what refuses it */
    put_seed(dir, nm, 1, img, n);
    img[CHUNK_HDR_BYTES] ^= 0x20;
    snprintf(nm, sizeof nm, "%s-flip-header", name);
    img[40] ^= 1;
    put_seed(dir, nm, 0, img, n);
    img[40] ^= 1;
    snprintf(nm, sizeof nm, "%s-truncated", name);
    put_seed(dir, nm, 0, img, n - 5);
    free(img);
    free(cl);
    free(sums);
    free(obj);
}

void fuzz_seeds(const char *dir)
{
    seed_for(dir, "seed-4+2-data", 4, 2, 48, 48 * 9 + 5, 1);
    seed_for(dir, "seed-4+2-parity", 4, 2, 48, 48 * 9 + 5, 5);
    seed_for(dir, "seed-8+2-last", 8, 2, 40, 40 * 19 + 7, 2);
    seed_for(dir, "seed-copy-1+2", 1, 2, 64, 64 * 3 + 3, 2);
    seed_for(dir, "seed-one-cell", 1, 1, 100, 37, 0);
    seed_for(dir, "seed-16+4", 16, 4, 16, 16 * 33 + 1, 18);
    fuzz_put(dir, "seed-empty", "", 0);
    uint8_t z[CHUNK_HDR_BYTES + CHUNK_FTR_BYTES] = {0};
    put_seed(dir, "seed-zeros", 0, z, sizeof z);
}
