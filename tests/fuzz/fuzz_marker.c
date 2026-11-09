/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * WAL marker payloads (src/marker.c) on arbitrary bytes: mk_decode must not read past the length it is given, and
 * whatever it accepts must encode back to the very same bytes (one encoding per payload) and decode to the same
 * values. The COMMIT file list is walked entry by entry, as a consumer would.
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

    uint8_t files[256];
    mk_file f1 = {"p0_5_5_0.part", 13, 4096, 0x1122334455667788ull, 0x99aabbccddeeff00ull};
    mk_file f2 = {"p1_6_6_0.part", 13, 8192, 3, 4};
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
