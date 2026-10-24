/*
 * SERIES and STATES (the same record format): series_open on any bytes, then
 * lookups and a reload. A torn tail is cut; anything else bad fails the open.
 * Record checks are no authentication, so the harness re-seals each record
 * (rec_check in series.c) unless bit 7 of its zero byte is set: that bit is
 * cleared and the record left as it is, so damage before a valid record is
 * reached as well as records of odd types and lengths.
 */
#include "template.h"

static const char *D;

static void reseal(uint8_t *b, size_t size)
{
    for (size_t q = 0; q + 8 <= size;) {
        size_t len = (size_t)b[q + 4] | ((size_t)b[q + 5] << 8);
        bool keep = b[q + 7] & 0x80;
        b[q + 7] &= 0x7f;
        size_t rec = (8 + len + 7) & ~(size_t)7;
        if (len == 0 || rec > size - q)
            return;
        if (!keep)
            store32(b + q, (uint32_t)xxh64(b + q + 4, rec - 4, q));
        q += rec;
    }
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("dict");
    atexit(fuzz_rm_at_exit);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    uint8_t *b = malloc(size ? size : 1);
    memcpy(b, data, size);
    reseal(b, size);
    fuzz_put(D, "SERIES", b, size);
    free(b);
    int dfd = open(D, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    series_dict d;
    if (dfd >= 0 && series_open(&d, dfd, "SERIES", true, false) == 0) {
        for (uint32_t id = 0; id < 64; id++)
            if (series_name(&d, id))
                (void)series_type(&d, id);
        uint32_t id;
        series_get(&d, "a", false, TSDB_FLOAT32, &id);
        series_reload(&d, dfd);
        series_close(&d);
    }
    if (dfd >= 0)
        close(dfd);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    const char *t = fuzz_dir("dict-seed");
    snap s;
    template_build(t, false, false, NULL);
    snap_take(&s, t);
    const snap_file *f = tpl_find(&s, "SERIES", "");
    if (f)
        fuzz_put(dir, "seed-SERIES", f->d, f->n);
}
