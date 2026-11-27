/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * WAL marker payloads (src/marker.c) on arbitrary bytes: mk_decode must not read past the length it is given, and
 * whatever it accepts must encode back to the very same bytes (one encoding per payload) and decode to the same
 * values. The COMMIT file list and COLD's objects and files are walked entry by entry, as a consumer would.
 *
 * Input: the payload, as the record's len gives it (type, flags, span, body).
 */
#include "fuzz.h"
#include "internal.h"

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    /* an exact-size copy: a read past the end is the sanitizer's to find */
    uint8_t *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    memcpy(p, data, size);
    mk m;
    if (mk_decode(p, size, &m) == 0) {
        if (m.type == MK_COMMIT) {
            size_t pos = 0, n = 0;
            mk_file f;
            while (mk_file_next(&m.u.commit, &pos, &f)) {
                n++;
                if (f.name_len == 0 || f.name_len > MK_NAME_MAX || memchr(f.name, 0, f.name_len))
                    abort();
            }
            if (n != m.u.commit.nfiles || pos != m.u.commit.files_len)
                abort();
        }
        if (m.type == MK_COLD) {
            const mk_cold *c = &m.u.cold;
            size_t pos = 0, n = 0;
            mk_cold_obj o;
            while (mk_cold_obj_next(c, &pos, &o)) {
                n++;
                if (o.rec_len == 0 || o.rec_len > MK_REC_MAX || o.rec < p || o.rec + o.rec_len > p + size)
                    abort();
            }
            if (n != c->nobjs || pos != c->objs_len || n > MK_FILES_MAX)
                abort();
            pos = 0, n = 0;
            mk_file f;
            while (mk_cold_file_next(c, &pos, &f)) {
                n++;
                if (f.name_len == 0 || f.name_len > MK_NAME_MAX || (f.kind != MK_FILE_EC && f.kind != MK_FILE_Y))
                    abort();
            }
            if (n != c->nfiles || pos != c->files_len || c->nretired > MK_FILES_MAX ||
                c->retired + (size_t)c->nretired * MK_OID != p + size)
                abort();
        }
        if (m.type == MK_GC) {
            if (m.u.gc.reason == MK_GC_RELEASE || m.u.gc.reason == MK_GC_RELEASE_DONE) {
                if (m.u.gc.noids != 0 || m.u.gc.rel_pos >= MK_REL_POS_MAX || m.u.gc.rel_node == 0 ||
                    m.u.gc.rel_ver == 0)
                    abort();
            } else if (m.u.gc.noids == 0 || m.u.gc.noids > MK_FILES_MAX) {
                abort();
            }
        }
        if ((m.type == MK_DIGEST && m.u.digest.noids > MK_FILES_MAX) ||
            (m.type == MK_UNSETTLE && (m.u.unsettle.noids == 0 || m.u.unsettle.noids > MK_FILES_MAX)))
            abort();
        uint8_t *q;
        size_t qn;
        if (mk_encode(&m, &q, &qn) != 0 || qn != size || memcmp(q, p, size) != 0)
            abort();
        free(q);
    }
    free(p);
    return 0;
}

static void seed(const char *dir, const char *name, const mk *m)
{
    uint8_t *p;
    size_t n;
    if (mk_encode(m, &p, &n) != 0) {
        fprintf(stderr, "fuzz: cannot encode seed %s\n", name);
        exit(1);
    }
    fuzz_put(dir, name, p, n);
    free(p);
}

void fuzz_seeds(const char *dir)
{
    mk m;
    memset(&m, 0, sizeof m);
    m.type = MK_DROP;
    m.u.drop = (mk_drop){.wm_gen = 3, .dropped_before = 1700000000000, .block = 41, .partition = MK_NO_PARTITION};
    seed(dir, "seed-drop", &m);
    m.u.drop.partition = 19700;
    seed(dir, "seed-drop-partition", &m);

    memset(&m, 0, sizeof m);
    m.type = MK_RETENTION;
    m.u.retention = (mk_retention){.series = 7, .limit_ms = 86400000, .record_off = 96};
    seed(dir, "seed-retention", &m);

    memset(&m, 0, sizeof m);
    m.type = MK_EPOCH;
    m.u.epoch = (mk_epoch){.epoch = 5, .owner = "node-a", .owner_len = 6};
    seed(dir, "seed-epoch", &m);
    m.u.epoch.owner_len = 0;
    seed(dir, "seed-epoch-noowner", &m);

    memset(&m, 0, sizeof m);
    m.type = MK_CLASS;
    m.u.cls = (mk_class){.series = 4, .cls = 2, .at = 1700000000000, .record_off = 48};
    seed(dir, "seed-class", &m);
    m.u.cls.at = INT64_MIN;
    seed(dir, "seed-class-fresh", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_SERIES;
    m.u.series = (mk_series){.id = 9, .vtype = TSDB_FLOAT64, .name = "plant/boiler 3/temp", .name_len = 19};
    seed(dir, "seed-series", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_STATE;
    m.u.state = (mk_state){.id = 2, .name = "RUNNING", .name_len = 7};
    seed(dir, "seed-state", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_NOW;
    m.u.now.now_ms = 1700000000123;
    seed(dir, "seed-now", &m);

    uint8_t files[256];
    mk_file f1 = {"p0_5_5_0.part", 13, 4096, 0x1122334455667788ull, 0x99aabbccddeeff00ull, 0};
    mk_file f2 = {"p1_6_6_0.part", 13, 8192, 3, 4, 0};
    size_t fl = mk_file_put(files, &f1);
    fl += mk_file_put(files + fl, &f2);
    memset(&m, 0, sizeof m);
    m.type = MK_COMMIT;
    m.u.commit = (mk_commit){.op = MK_OP_INSERT, .keys_form = MK_KEYS_RANGE, .nfiles = 2, .flush_through = 9,
                             .rows = 100000, .events = 2, .reserve = 0, .series = 3, .lo = 10, .hi = 20,
                             .files = files, .files_len = fl};
    seed(dir, "seed-commit-range", &m);
    m.u.commit.op = MK_OP_DELETE;
    m.u.commit.keys_form = MK_KEYS_NONE;
    m.u.commit.nfiles = 0;
    m.u.commit.files_len = 0;
    m.u.commit.events = 0;
    seed(dir, "seed-commit-delete", &m);

    uint8_t *table = calloc(MK_BUCKETS, 16);
    for (uint32_t i = 0; i < MK_BUCKETS; i++)
        mk_table_put(table, i, i % 7 ? 1 : 0, i % 7 ? 0 : 5); /* mostly "no keys" (lo > hi) */
    m.u.commit.keys_form = MK_KEYS_TABLE;
    m.u.commit.table = table;
    m.u.commit.op = MK_OP_REPLACE;
    m.u.commit.nfiles = 2;
    m.u.commit.files = files;
    m.u.commit.files_len = fl;
    m.u.commit.events = 1;
    seed(dir, "seed-commit-table", &m);
    free(table);

    /* R8: COLD (two objects, an .ec and a Y file, one retired oid, F set), GC of each reason, DIGEST */
    uint8_t objs[256], rec[37], oid[MK_OID], oids[3 * MK_OID];
    for (size_t i = 0; i < sizeof rec; i++)
        rec[i] = (uint8_t)(i * 7 + 1);
    for (int i = 0; i < MK_OID; i++)
        oid[i] = (uint8_t)(i + 1);
    for (size_t i = 0; i < sizeof oids; i++)
        oids[i] = (uint8_t)(i * 3 + 2);
    size_t ol = mk_cold_obj_put(objs, oid, rec, sizeof rec);
    oid[0] = 99;
    ol += mk_cold_obj_put(objs + ol, oid, rec, 4);
    mk_file e1 = {"p5_1_4_1.ec", 11, 4000, 7, 8, MK_FILE_EC};
    mk_file y1 = {"p5_5_5_0.part", 13, 900, 9, 10, MK_FILE_Y};
    fl = mk_file_put(files, &e1);
    fl += mk_file_put(files + fl, &y1);
    memset(&m, 0, sizeof m);
    m.type = MK_COLD;
    m.u.cold = (mk_cold){.z = 5000, .flush_through = 12, .stripe_version = 3, .key = 4 * 19700 + 1, .nobjs = 2,
                         .nfiles = 2, .nretired = 0, .objs = objs, .objs_len = ol, .files = files, .files_len = fl,
                         .retired = oids};
    seed(dir, "seed-cold", &m); /* retired oids are R10's: a decode refuses them (the corpus keeps one such input) */
    m.u.cold.nfiles = 0, m.u.cold.files_len = 0;
    seed(dir, "seed-cold-objs", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_GC;
    m.u.gc = (mk_gc){.reason = MK_GC_RETIRED, .noids = 3, .oids = oids};
    seed(dir, "seed-gc-retired", &m);
    m.u.gc.reason = MK_GC_ORPHAN, m.u.gc.noids = 1;
    seed(dir, "seed-gc-orphan", &m);
    m.u.gc.reason = MK_GC_DONE, m.u.gc.noids = 2;
    seed(dir, "seed-gc-done", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_GC;
    m.u.gc = (mk_gc){.reason = MK_GC_RELEASE, .noids = 0, .rel_vol = 2, .rel_pos = 1, .rel_node = 5, .rel_ver = 3};
    seed(dir, "seed-gc-release", &m);
    m.u.gc.reason = MK_GC_RELEASE_DONE;
    seed(dir, "seed-gc-release-done", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_DIGEST;
    m.u.digest = (mk_digest){.key = 4 * 19700 + 1, .z = 5000, .rows = 86400, .series = 3, .xxh64 = 0x1234567890abcdefull,
                             .now_ms = 1700000000000, .noids = 2, .oids = oids};
    seed(dir, "seed-digest", &m);
    memset(&m, 0, sizeof m);
    m.type = MK_UNSETTLE;
    m.u.unsettle = (mk_unsettle){.key = 4 * 19700 + 1, .noids = 2, .oids = oids};
    seed(dir, "seed-unsettle", &m);

    memset(&m, 0, sizeof m); /* a reserved type and an unknown one: the body is theirs */
    m.type = MK_FEED_SLOT;
    m.span = 1;
    static const uint8_t body[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    m.body = body, m.body_len = sizeof body;
    seed(dir, "seed-reserved", &m);
    m.type = 60000;
    m.span = 3;
    seed(dir, "seed-unknown", &m);
}
