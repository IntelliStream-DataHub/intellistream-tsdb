/*
 * WAL replay at a writer's open (memtable.c): one segment of any bytes in a
 * database whose SERIES knows four series, then a clean close (which flushes
 * what was replayed). A torn tail is cut; a bad record before a valid one
 * fails the open, loudly. The record checks are no authentication, so the
 * harness re-seals each record (header check and rows hash, as
 * wal_write_record) unless bit 31 of its row count is set: that bit is
 * cleared and the record left as it is, so damage before a valid record is
 * reached as well as crafted rows.
 */
#include "template.h"

static const char *D;
static snap tpl;
static char wal[512];
static uint64_t seq;

static void reseal(uint8_t *b, size_t size)
{
    for (size_t q = 0; q + WAL_HDR <= size;) {
        uint32_t n = load32(b + q);
        bool keep = n >> 31;
        n &= 0x7fffffffu;
        store32(b + q, n);
        if (n == 0 || n > (size - q - WAL_HDR) / WAL_ROW)
            return;
        if (!keep) {
            uint64_t hash = xxh64(b + q + WAL_HDR, (size_t)n * WAL_ROW, 0);
            uint8_t t[12];
            store32(t, n);
            memcpy(t + 4, &hash, 8);
            store32(b + q + 4, (uint32_t)xxh64(t, sizeof t, seq * 0x9e3779b97f4a7c15ull ^ q)); /* wal_head_check */
            memcpy(b + q + 8, &hash, 8);
        }
        q += WAL_HDR + (size_t)n * WAL_ROW;
    }
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("wal");
    template_build(D, false, false, &tpl);
    const snap_file *f = tpl_find(&tpl, "wal_", ".log");
    if (!f) {
        fprintf(stderr, "fuzz: the template has no WAL segment\n");
        exit(1);
    }
    snprintf(wal, sizeof wal, "%s", f->rel);
    if (sscanf(strrchr(wal, '/') ? strrchr(wal, '/') + 1 : wal, "wal_%" SCNx64 ".log", &seq) != 1) {
        fprintf(stderr, "fuzz: cannot parse %s\n", wal);
        exit(1);
    }
    atexit(fuzz_rm_at_exit);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    snap_put(&tpl, D, wal);
    uint8_t *b = malloc(size ? size : 1);
    memcpy(b, data, size);
    reseal(b, size);
    fuzz_put(D, wal, b, size);
    free(b);
    tsdb_options o;
    template_opts(&o);
    eng *db;
    if (eng_open(D, &o, &db) == 0)
        eng_close(db);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    const snap_file *f = tpl_find(&tpl, "wal_", ".log");
    fuzz_put(dir, "seed-wal", f->d, f->n);
}
