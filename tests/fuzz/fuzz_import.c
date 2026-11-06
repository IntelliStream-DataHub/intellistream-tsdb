/* The export stream reader (tools/dump.c): im_pass over any bytes, in the verifying pass (no database) and, for
 * inputs with bit 0 of the last byte set, applying straight away (a pipe, --no-verify) into a scratch database with
 * --merge or --replace. CRC-32C is no authentication: the harness re-seals the header and every record (unless bit 1
 * of the last byte is set), so mutations reach the record parsers. The seeds are real exports of a small
 * database, each with the four control bytes 0, 1, 5 and 9. */
#include "fuzz.h"
#include "tsdb.h"
#include "../../tools/dump.c"

static tsdb *DB;
static ZSTD_DCtx *ZD;
static int applied_ok; /* applying runs that read their stream to its end */

/* FUZZ_IMPORT_EXPECT_END=1 (set by `make test` for the replay): the corpus must hold an apply seed that reaches the
 * end of its stream. Seeds whose series lived in a volume the harness database lacks all stopped at that series, and
 * the end-of-stream checks went unfuzzed for a whole run before anyone saw it. */
static void expect_end(void)
{
    if (applied_ok == 0) {
        fprintf(stderr, "fuzz-import: no applying input ran to the end of its stream\n");
        _exit(1);
    }
}

static void reseal(uint8_t *b, size_t size)
{
    if (size < 16 || memcmp(b, DUMP_MAGIC, 8) != 0)
        return;
    uint32_t len = get_u32(b + 12);
    if (len > size - 16 || size - 16 - len < 4)
        return;
    uint32_t c = crc32c(crc32c(0, b, 16), b + 16, len);
    memcpy(b + 16 + len, &c, 4);
    size_t q = 16 + (size_t)len + 4;
    for (uint32_t seq = 0; q + 16 <= size; seq++) {
        uint32_t pl = get_u32(b + q + 4);
        if (pl > size - q - 16)
            return;
        c = crc32c(crc32c(0, b + q, 12), b + q + 16, pl);
        memcpy(b + q + 12, &c, 4);
        q += 16 + pl;
    }
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    const char *d = fuzz_dir("import");
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.wal_sync_ms = -1;
    o.create_volumes = 0;
    o.merge_threads = 0;
    char p[8192];
    snprintf(p, sizeof p, "%s/db", d);
    if (tsdb_open(p, &o, &DB) != 0)
        exit(1);
    ZD = ZSTD_createDCtx();
    if (getenv("FUZZ_IMPORT_EXPECT_END"))
        atexit(expect_end); /* before the removal: handlers run last to first, and this one leaves with _exit */
    atexit(fuzz_rm_at_exit);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!DB)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size < 17)
        return 0;
    uint8_t ctl = data[size - 1];
    size--;
    uint8_t *b = malloc(size);
    memcpy(b, data, size);
    if (!(ctl & 2))
        reseal(b, size);
    FILE *in = fmemopen(b, size, "rb");
    importer im = {.existing = (ctl & 4) ? EXIST_REPLACE : EXIST_MERGE, .batch_rows = (ctl & 8) ? 3 : 1u << 16};
    im.in = in;
    im.zd = ZD;
    im.now_ms = 1700000000000;
    size_t cap = im.batch_rows + DUMP_CHUNK_MAX;
    im.ts = malloc(cap * 8);
    im.val = malloc(cap * 8);
    im.sid = malloc(cap * sizeof *im.sid);
    im.valid = malloc(cap / 8 + 1);
    im.maps = calloc(1, sizeof *im.maps);
    im.roots = calloc(1, sizeof *im.roots);
    if (ctl & 1) {
        im.db = DB;
        im.apply = true;
        snprintf(im.root, sizeof im.root, "%s", tsdb_volume_path(DB, 0));
    }
    if (im_pass(&im, false) == 0 && (ctl & 1))
        applied_ok++;
    fclose(in);
    free(im.ts), free(im.val), free(im.sid), free(im.valid), free(im.codes), free(im.names);
    free(im.rec.p), free(im.raw.p), free(im.name), free(im.path), free(im.src_version);
    for (int i = 0; i < im.nvols_ok; i++)
        free(im.vols_ok[i]);
    free(im.vols_ok);
    free(im.maps), free(im.roots);
    free(b);
    return 0;
}

/* a small database for the seeds: four value types with nulls, a range delete; series b in the volume vol, if any */
static void seed_db(const char *root, const char *vol)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.granule_rows = 64;
    tsdb *db;
    if (vol)
        mkdir(vol, 0755);
    if (tsdb_open(root, &o, &db) != 0) {
        fprintf(stderr, "fuzz: cannot create the seed database in %s\n", root);
        exit(1);
    }
    tsdb_sid id[4];
    tsdb_series_create(db, "a", TSDB_FLOAT32, NULL, &id[0]);
    tsdb_series_create(db, "b", TSDB_FLOAT64, vol, &id[1]);
    tsdb_series_create(db, "c", TSDB_INT64, NULL, &id[2]);
    tsdb_series_create(db, "d", TSDB_BOOL, NULL, &id[3]);
    enum { N = 300 };
    int64_t ts[N], iv[N];
    float f[N];
    double d[N];
    uint32_t bv[N];
    uint8_t valid[N];
    for (int i = 0; i < N; i++) {
        ts[i] = 1704067200000 + (int64_t)i * 600000;
        f[i] = 20.0f + (float)(i % 37) * 0.1f;
        d[i] = 1000.0 + i * 0.25;
        iv[i] = 5000 + (i * 7919) % 101;
        bv[i] = (uint32_t)(i / 40 & 1);
        valid[i] = i % 17 != 3;
    }
    tsdb_values v[4] = {{TSDB_FLOAT32, f, NULL}, {TSDB_FLOAT64, d, valid}, {TSDB_INT64, iv, NULL},
                        {TSDB_BOOL, bv, valid}};
    for (int s = 0; s < 4; s++)
        tsdb_insert_series_values(db, id[s], ts, &v[s], N);
    tsdb_delete_range(db, id[0], ts[50], ts[70]);
    tsdb_close(db);
}

/* exports of that database at two zstd levels, each with the control bytes: verify only (0), apply (1), apply
 * with --replace (5), apply in 3-row batches (9). Two databases: one whose series all live in its directory, so an
 * applied stream reaches its end (the trailer, the last batch), and one with series b in a second volume, which the
 * harness database does not have: its apply seeds (names with -vol) end in the refusal of that volume. */
void fuzz_seeds(const char *dir)
{
    const char *t = fuzz_dir("import-seed");
    for (int two = 0; two <= 1; two++) {
        char db[8192], vol[8192], out[8192];
        snprintf(db, sizeof db, "%s/db%d", t, two);
        snprintf(vol, sizeof vol, "%s/vol2-%d", t, two); /* beside the database: a volume may not lie in it */
        seed_db(db, two ? vol : NULL);
        for (int lvl = 0; lvl <= 1; lvl++) {
            snprintf(out, sizeof out, "%s/export%d-%d", t, two, lvl);
            char level[8];
            snprintf(level, sizeof level, "%d", lvl);
            char *args[] = {"-o", out, "--zstd-level", level};
            if (dump_export_main(db, 4, args) != 0) {
                fprintf(stderr, "fuzz: the seed export failed\n");
                exit(1);
            }
            size_t n;
            uint8_t *f = fuzz_get(t, strrchr(out, '/') + 1, &n);
            if (!f || n > 1u << 20)
                exit(1);
            static const uint8_t ctl[] = {0, 1, 5, 9};
            for (size_t k = 0; k < sizeof ctl; k++) {
                if (two && !ctl[k])
                    continue; /* the verifying pass reads both alike */
                uint8_t *s = malloc(n + 1);
                if (!s)
                    exit(1);
                memcpy(s, f, n);
                s[n] = ctl[k];
                char name[64];
                snprintf(name, sizeof name, "seed%s-z%d-c%u", two ? "-vol" : "", lvl, ctl[k]);
                fuzz_put(dir, name, s, n + 1);
                free(s);
            }
            free(f);
        }
    }
}
