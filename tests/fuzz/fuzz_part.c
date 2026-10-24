/*
 * Part files: part_open (header, index, footer, tombstones and their checksum)
 * and then every granule through granule_read, with and without checksum
 * verification and with value types that do not match. Inputs replace one
 * part of the template, so its name stays valid. The checksum over header,
 * index, tombstones and footer is no authentication, so a crafted part carries
 * a valid one: the harness re-seals every input whose footer places the index
 * inside the file, or a mutated granule entry or tombstone would only ever be
 * refused at the hash.
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
    part_footer f;
    if (size < sizeof(part_header) + sizeof f)
        return;
    memcpy(&f, b + size - sizeof f, sizeof f);
    if (f.index_off < sizeof(part_header) || f.index_off > (uint64_t)size - sizeof f ||
        (uint64_t)f.ngranules * sizeof(granule) > (uint64_t)size - sizeof f - f.index_off)
        return;
    uint64_t index_end = f.index_off + (uint64_t)f.ngranules * sizeof(granule);
    uint64_t x = xxh64(b, sizeof(part_header), 0);
    x = xxh64(b + f.index_off, (size_t)f.ngranules * sizeof(granule), x);
    size_t tlen = (size_t)((uint64_t)size - sizeof f - index_end);
    if (tlen)
        x = xxh64(b + index_end, tlen, x);
    f.index_hash = xxh64(&f, offsetof(part_footer, index_hash), x);
    memcpy(b + size - sizeof f, &f, sizeof f);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    uint8_t *b = malloc(size ? size : 1);
    memcpy(b, data, size);
    reseal(b, size);
    fuzz_put(D, name, b, size);
    free(b);
    int dfd = open(D, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    part *p;
    if (dfd >= 0 && part_open(dfd, name, &p) == 0) {
        for (uint32_t i = 0; i < p->ng && i < 512; i++)
            granule_read(cc, p, &p->g[i], (int)((p->g[i].series + i / 2) % 5), ts, vals, i & 1);
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
        }
}
