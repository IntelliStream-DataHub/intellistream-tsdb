/*
 * Part files: part_open (header, index, footer, tombstones and their checksum)
 * and then every granule through granule_read, with and without checksum
 * verification and with value types that do not match. Inputs replace one
 * part of the template, so its name stays valid. The checksum over header,
 * index, tombstones and footer is no authentication, so a crafted part carries
 * a valid one: the harness re-seals every input whose footer places the index
 * inside the file, or a mutated granule entry or tombstone would only ever be
 * refused at the hash.
 *
 * Inputs that begin with "GIX!" are a packed granule index alone (gindex.c): a u32 entry count, then the index,
 * parsed from a buffer of its exact length plus the 8 bytes a reader may load past it; when gix_parse accepts it,
 * every entry is read through every accessor. The seeds hold the template parts' indexes this way too, so the
 * fuzzer mutates descriptors, runs and entries directly instead of through a part's other bytes.
 */
#include "template.h"

static const char *D;
static char name[512];
static codec_ctx *cc;
static int64_t *ts;
static void *vals;
static snap tpl;

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("part");
    template_build(D, true, false, NULL);
    snap_take(&tpl, D);
    const snap_file *f = tpl_find(&tpl, "", ".part");
    if (!f) {
        fprintf(stderr, "fuzz: the template has no part\n");
        exit(1);
    }
    snprintf(name, sizeof name, "%s", f->rel);
    cc = codec_new();
    ts = malloc(MAX_GRANULE_ROWS * sizeof *ts);
    vals = malloc(MAX_GRANULE_ROWS * 8);
    atexit(fuzz_rm_at_exit);
    return 0;
}

/* part_hash (part.c) over b, when the footer's bounds allow it; part_open checks them again */
static void reseal(uint8_t *b, size_t size)
{
    /* at x86-64-v4 GCC copies this 64-byte struct with aligned zmm moves; ASan's fake stack aligns its frames
     * to 32 bytes only, so without the alignment the harness itself faults (FUZZ_ARCH=-march=x86-64-v4) */
    _Alignas(64) part_footer f;
    if (size < sizeof(part_header) + sizeof f)
        return;
    memcpy(&f, b + size - sizeof f, sizeof f);
    if (f.index_off < sizeof(part_header) || f.index_off > (uint64_t)size - sizeof f ||
        f.index_len > (uint64_t)size - sizeof f - f.index_off)
        return;
    uint64_t index_end = f.index_off + f.index_len;
    uint64_t x = xxh64(b, sizeof(part_header), 0);
    x = xxh64(b + f.index_off, (size_t)f.index_len, x);
    size_t tlen = (size_t)((uint64_t)size - sizeof f - index_end);
    if (tlen)
        x = xxh64(b + index_end, tlen, x);
    f.index_hash = xxh64(&f, offsetof(part_footer, index_hash), x);
    memcpy(b + size - sizeof f, &f, sizeof f);
}

#define GIX_TAG "GIX!"

static void index_only(const uint8_t *data, size_t size)
{
    uint32_t n;
    memcpy(&n, data + 4, 4);
    n %= 1u << 16;
    size_t len = size - 8;
    uint8_t *b = malloc(len + 8);
    memcpy(b, data + 8, len);
    memset(b + len, 0xa5, 8);
    part p;
    memset(&p, 0, sizeof p);
    if (gix_parse(b, len, n, &p.ix) == 0) {
        p.ng = n;
        uint64_t sink = 0;
        for (uint32_t i = 0; i < n; i++) {
            granule g;
            memset(&g, 0, sizeof g);
            gx_load(&p, i, GW_ALL, &g);
            int64_t lo, hi;
            gx_ts(&p, i, &lo, &hi);
            uint32_t a, e;
            gix_run(&p.ix, gx_series(&p, i), &a, &e);
            if (a > i || i >= e || e > n)
                abort(); /* the run of an entry's own series holds it */
            sink += g.count + (uint64_t)lo + (uint64_t)hi + gx_count(&p, i);
        }
        __asm__ volatile("" : : "r"(sink));
    }
    free(b);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size >= 8 && memcmp(data, GIX_TAG, 4) == 0) {
        index_only(data, size);
        return 0;
    }
    uint8_t *b = malloc(size ? size : 1);
    memcpy(b, data, size);
    reseal(b, size);
    fuzz_put(D, name, b, size);
    free(b);
    int dfd = open(D, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    part *p;
    /* Odd sizes open with verify_checksums, which reads the index from a checked heap copy (reseal made the
     * checksums match); even sizes read it in place from the mapping. */
    if (dfd >= 0 && part_open(dfd, name, (size & 1) != 0, &p) == 0) {
        for (uint32_t i = 0; i < p->ng && i < 512; i++) {
            granule g;
            memset(&g, 0, sizeof g);
            gx_load(p, i, GW_ALL, &g);
            granule_read(cc, p, &g, (int)((g.series + i / 2) % 5), ts, vals, MAX_GRANULE_ROWS, i & 1);
        }
        for (uint32_t i = 0; i < p->nt; i++)
            (void)p->t[i].series;
        part_unref(p);
    }
    if (dfd >= 0)
        close(dfd);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    for (size_t i = 0; i < tpl.n; i++)
        if (!tpl.f[i].dir && strstr(tpl.f[i].rel, ".part")) {
            char seed[600];
            snprintf(seed, sizeof seed, "seed-%s", tpl.f[i].rel);
            fuzz_put(dir, seed, tpl.f[i].d, tpl.f[i].n);
            /* its index alone */
            _Alignas(64) part_footer f;
            const uint8_t *d = tpl.f[i].d;
            size_t n = tpl.f[i].n;
            if (n < sizeof f)
                continue;
            memcpy(&f, d + n - sizeof f, sizeof f);
            if (f.index_off > n || f.index_len > n - f.index_off)
                continue;
            uint8_t *b = malloc(8 + (size_t)f.index_len);
            memcpy(b, GIX_TAG, 4);
            memcpy(b + 4, &f.ngranules, 4);
            memcpy(b + 8, d + f.index_off, (size_t)f.index_len);
            snprintf(seed, sizeof seed, "seed-index-%s", tpl.f[i].rel);
            fuzz_put(dir, seed, b, 8 + (size_t)f.index_len);
            free(b);
        }
}
