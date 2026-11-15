/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The REPL file of a replica directory (src/replfile.c) on arbitrary bytes: replfile_parse must not read past the
 * length it is given, and whatever it accepts must encode back to the very same bytes (one encoding per state) and
 * parse to the same values. A second pass makes the input pass the checksum (magic, length and hash fixed up) so that
 * the structure checks behind it are reached; the file view (two slots) picks the newer valid one like a load does.
 *
 * Input: one slot, or a whole file (two pages).
 */
#include "fuzz.h"
#include "internal.h"
#include "replfile.h"

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}

static void roundtrip(const uint8_t *p, size_t n)
{
    struct replfile_state st;
    uint64_t gen;
    if (!replfile_parse(p, n, &st, &gen))
        return;
    uint8_t page[REPLFILE_PAGE];
    uint32_t len = 128 + st.nhist * 16 + 8;
    if (replfile_encode(page, &st, gen) != 0 || len > n || memcmp(page, p, len) != 0)
        abort();
    for (size_t i = len; i < REPLFILE_PAGE; i++)
        if (page[i] != 0)
            abort();
    struct replfile_state again;
    uint64_t g2;
    if (!replfile_parse(page, sizeof page, &again, &g2) || g2 != gen || again.nhist != st.nhist ||
        memcmp(again.volume_uuid, st.volume_uuid, 16) != 0 || again.rid != st.rid || again.open_nonce != st.open_nonce || again.fence != st.fence ||
        (st.nhist && memcmp(again.hist, st.hist, st.nhist * sizeof *st.hist) != 0))
        abort();
    struct repl_durable d;
    replfile_to_durable(&st, 1000, &d);
    /* entries that start beyond the log's end are left out (the epoch was saved, the power went before its marker) */
    uint32_t keep = st.nhist;
    while (keep && st.hist[keep - 1].start > 1000)
        keep--;
    if (d.nhist != keep || d.rid == 0 || d.hist != st.hist)
        abort();
    replfile_state_free(&again);
    replfile_state_free(&st);
}

/* The file view: the two pages of whatever length, as a load reads them. Whatever it picks is a state the slot parse
 * takes, and a pick of two slots is one of them. */
static void pick_file(const uint8_t *p, size_t n)
{
    struct replfile_state st;
    uint64_t gen;
    int slot = -1;
    bool torn = false;
    int rc = replfile_pick(p, n, &st, &gen, &slot, &torn);
    if (rc != 0) {
        if (rc != TSDB_ERR_CORRUPT)
            abort();
        return;
    }
    uint8_t page[REPLFILE_PAGE];
    if ((slot != 0 && slot != 1) || replfile_encode(page, &st, gen) != 0)
        abort();
    replfile_state_free(&st);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    /* an exact-size copy: a read past the end is the sanitizer's to find */
    uint8_t *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    memcpy(p, data, size);
    pick_file(p, size > 2 * REPLFILE_PAGE ? 2 * REPLFILE_PAGE : size);
    roundtrip(p, size > REPLFILE_PAGE ? REPLFILE_PAGE : size);
    if (size > REPLFILE_PAGE)
        roundtrip(p + REPLFILE_PAGE, size - REPLFILE_PAGE > REPLFILE_PAGE ? REPLFILE_PAGE : size - REPLFILE_PAGE);
    /* the same with a good magic and a checksum made to fit the length the input claims */
    for (size_t off = 0; off < 2 && off * REPLFILE_PAGE < size; off++) {
        uint8_t *q = p + off * REPLFILE_PAGE;
        size_t n = size - off * REPLFILE_PAGE;
        if (n > REPLFILE_PAGE)
            n = REPLFILE_PAGE;
        if (n < 136)
            continue;
        uint64_t magic = 0x32504552425f5344ull;
        memcpy(q, &magic, 8);
        uint32_t nh;
        memcpy(&nh, q + 20, 4);
        if (nh > REPLFILE_MAX_HIST || 128 + nh * 16 + 8 > n)
            continue;
        uint32_t len = 128 + nh * 16 + 8;
        memcpy(q + 16, &len, 4);
        memset(q + 60, 0, 4);
        uint64_t h = xxh64(q, len - 8, 0);
        memcpy(q + len - 8, &h, 8);
        roundtrip(q, n);
    }
    pick_file(p, size > 2 * REPLFILE_PAGE ? 2 * REPLFILE_PAGE : size);
    free(p);
    return 0;
}

static void seed(const char *dir, const char *name, int nhist, int slots)
{
    struct replfile_state st;
    memset(&st, 0, sizeof st);
    for (int i = 0; i < 16; i++)
        st.volume_uuid[i] = (uint8_t)(1 + i), st.cluster_uuid[i] = (uint8_t)(0x80 + i);
    st.node = 3, st.incarnation = 12, st.rid = 0xfeedfacecafebeefull, st.open_gen = 9, st.open_nonce = 0x0123456789abcdefull, st.fence = 40, st.led = 38;
    st.floor = 500, st.log_start = 101;
    st.nhist = (uint32_t)nhist;
    st.hist = calloc((size_t)nhist + 1, sizeof *st.hist);
    for (int i = 0; i < nhist; i++)
        st.hist[i] = (struct repl_epoch){.epoch = 2 * (uint64_t)i + 1, .start = 10 * (uint64_t)i + 1};
    uint8_t file[2 * REPLFILE_PAGE];
    memset(file, 0, sizeof file);
    if (replfile_encode(file, &st, 5) != 0 || replfile_encode(file + REPLFILE_PAGE, &st, 4) != 0) {
        fprintf(stderr, "fuzz: cannot encode seed %s\n", name);
        exit(1);
    }
    fuzz_put(dir, name, file, slots == 2 ? sizeof file : REPLFILE_PAGE);
    free(st.hist);
}

void fuzz_seeds(const char *dir)
{
    seed(dir, "seed-slot-empty-history", 0, 1);
    seed(dir, "seed-slot-history", 6, 1);
    seed(dir, "seed-file", 12, 2);
    seed(dir, "seed-file-full", REPLFILE_MAX_HIST, 2);
}
