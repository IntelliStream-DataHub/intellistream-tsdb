/* CLASSES (src/classes.c): a template database with parts, its CLASSES replaced by the input, then tsdb_open (the
 * load, the open's checks of every part against the history, cls_check_parts) and queries of every series. Records
 * are re-sealed (rec_check) unless bit 31 of their zero word is set (cleared, left as it is). */
#include "template.h"
#define CLS_FILE "CLASSES"
#define CLS_REC 24

static const char *D;
static snap tpl;

static void reseal(uint8_t *b, size_t size)
{
    for (size_t q = 0; q + CLS_REC <= size; q += CLS_REC) {
        uint32_t z = load32(b + q + 20);
        bool keep = z >> 31;
        store32(b + q + 20, z & 0x7fffffffu);
        if (!keep) {
            store32(b + q + 4, load32(b + q + 4) & 63); /* a huge id only makes a 4 GB table (ASan: slow) */
            store32(b + q, (uint32_t)xxh64(b + q + 4, CLS_REC - 4, q));
        }
    }
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("classes");
    template_build(D, true, false, NULL);
    snap_take(&tpl, D);
    atexit(fuzz_rm_at_exit);
    return 0;
}

static int cb(void *ctx, const int64_t *ts, const float *v, size_t n) { (void)ctx, (void)ts, (void)v, (void)n; return 0; }

/* tsdb_open of D, then queries of every series (and, writable, one insert). Returns whether the open succeeded. */
static bool run_open(bool read_only)
{
    tsdb_options o;
    template_opts(&o);
    o.read_only = read_only;
    tsdb *db;
    if (tsdb_open(D, &o, &db) != 0)
        return false;
    static const char *names[] = {"a", "b", "c", "d"};
    for (int i = 0; i < 4; i++) {
        tsdb_sid id;
        if (tsdb_series_lookup(db, names[i], &id) != 0)
            continue;
        int cls;
        (void)tsdb_series_class(db, id, &cls);
        if (i == 0)
            (void)tsdb_query_raw(db, id, INT64_MIN, INT64_MAX, cb, NULL);
        tsdb_bucket *bk = NULL;
        size_t nb = 0;
        if (tsdb_query_agg(db, id, INT64_MIN, INT64_MAX, 3600000, &bk, &nb) == 0)
            free(bk);
        if (!read_only && i == 0) {
            int64_t t = 1704067200000 + 400 * 600000LL;
            float v = 1;
            (void)tsdb_insert_series(db, id, &t, &v, 1);
        }
    }
    tsdb_close(db);
    return true;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    snap_put(&tpl, D, CLS_FILE);
    uint8_t *b = malloc(size ? size : 1);
    memcpy(b, data, size);
    reseal(b, size);
    fuzz_put(D, CLS_FILE, b, size);
    free(b);
    /* A reader waits up to 1 s (100 x 10 ms, src/db.c) on a CLASSES that fails the parts check, which made one
     * input in 8 cost 1 s: it opens read-only only after the writable open of the same bytes succeeded (the check
     * passed, nothing waits). The writable run changed the files (an insert), the reader sees that state. */
    if (run_open(false) && size % 8 == 1)
        (void)run_open(true);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    const snap_file *f = tpl_find(&tpl, CLS_FILE, "");
    if (f)
        fuzz_put(dir, "seed-classes", f->d, f->n);
    uint8_t r[3 * CLS_REC];
    memset(r, 0, sizeof r);
    for (int k = 0; k < 3; k++) {
        store32(r + k * CLS_REC + 4, (uint32_t)(k % 2));
        int64_t from = k == 0 ? INT64_MIN : 1704067200000 + k * 86400000LL;
        memcpy(r + k * CLS_REC + 8, &from, 8);
        store32(r + k * CLS_REC + 16, (uint32_t)(k + 1) % 4);
    }
    fuzz_put(dir, "seed-classes-switch", r, sizeof r);
}
