/*
 * VOLUMES and CATALOG (volumes.c): the template with a second volume, one of
 * the two files replaced by the input, then a read-only open, every series
 * named and looked up, a query on each, and a close. Byte 0: bit 0 picks
 * CATALOG, else VOLUMES; with bit 1 a VOLUMES input is a list of paths, each
 * taken below the template's root and written with a valid checksum (they are
 * no authentication, and a path with the scratch directory's pid in it would
 * not survive into the corpus); without it the bytes are the file. Paths in
 * VOLUMES are absolute, so the template is restored in the same place before
 * each input.
 */
#include "template.h"

static const char *D;
static snap tpl;

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("volumes");
    template_build(D, true, true, NULL);
    snap_take(&tpl, D);
    if (!tpl_find(&tpl, "VOLUMES", "") || !tpl_find(&tpl, "CATALOG", "")) {
        fprintf(stderr, "fuzz: the template has no VOLUMES or CATALOG\n");
        exit(1);
    }
    atexit(fuzz_rm_at_exit);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size < 1)
        return 0;
    const char *file = data[0] & 1 ? "CATALOG" : "VOLUMES";
    snap_put(&tpl, D, file);
    if ((data[0] & 3) == 2) {
        size_t n = 0, line = 0; /* each line grows by at most 11 bytes and the root */
        char *b = malloc(size * (12 + strlen(D)) + 64), path[8192];
        for (size_t i = 1; i < size;) {
            const uint8_t *e = memchr(data + i, '\n', size - i);
            size_t l = e ? (size_t)(e - (data + i)) : size - i;
            int pl = snprintf(path, sizeof path, "%s/%.*s", D, (int)(l < 4096 ? l : 4096), (const char *)data + i);
            n += (size_t)sprintf(b + n, "%08x %s\n", (uint32_t)xxh64(path, (size_t)pl, ++line), path);
            i += l + 1;
        }
        fuzz_put(D, file, b, n);
        free(b);
    } else {
        fuzz_put(D, file, data + 1, size - 1);
    }
    tsdb_options o;
    template_opts(&o);
    o.read_only = 1;
    tsdb *db;
    if (tsdb_open(D, &o, &db) == 0) {
        uint32_t n = tsdb_series_count(db);
        for (uint32_t id = 0; id < n && id < 64; id++) {
            const char *name = tsdb_series_name(db, id);
            uint32_t got;
            if (name)
                tsdb_series_lookup(db, name, &got);
            tsdb_bucket *b = NULL;
            size_t nb;
            if (tsdb_query_agg(db, id, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0)
                free(b);
        }
        tsdb_close(db);
    }
    return 0;
}

void fuzz_seeds(const char *dir)
{
    fuzz_put(dir, "seed-VOLUMES-paths", "\002vol2\n", 6);
    const char *names[2] = {"VOLUMES", "CATALOG"};
    for (int i = 1; i < 2; i++) {
        const snap_file *f = tpl_find(&tpl, names[i], "");
        uint8_t *b = malloc(f->n + 1);
        b[0] = (uint8_t)i;
        memcpy(b + 1, f->d, f->n);
        char seed[64];
        snprintf(seed, sizeof seed, "seed-%s", names[i]);
        fuzz_put(dir, seed, b, f->n + 1);
        free(b);
    }
}
