/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * WAL replay at a writer's open (memtable.c): one segment of any bytes in a
 * database whose SERIES knows four series, then a clean close (which flushes
 * what was replayed). A torn tail is cut; a bad record before a valid one
 * fails the open, loudly. The record checks are no authentication, so the
 * harness re-seals each record (header check and rows hash, as
 * wal_seal) unless bit 31 of its len word is set: that bit is
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
        uint32_t w = load32(b + q);
        bool keep = w >> 31;
        w &= 0x7fffffffu;
        store32(b + q, w);
        uint32_t kind = w >> 28, n = w & 0x0fffffffu;
        size_t body = kind == WAL_KIND_ROWS ? (size_t)n * WAL_ROW : n;
        if (kind > WAL_KIND_MARKER || n == 0 || body > size - q - WAL_HDR)
            return;
        if (!keep) { /* wal_seal: the hash covers fno, commit_ms and the payload; the check binds segment and offset */
            uint64_t hash = xxh64(b + q + 16, 16 + body, 0);
            uint8_t t[12];
            store32(t, w);
            memcpy(t + 4, &hash, 8);
            store32(b + q + 4, (uint32_t)xxh64(t, sizeof t, seq * 0x9e3779b97f4a7c15ull ^ q)); /* wal_head_check */
            memcpy(b + q + 8, &hash, 8);
        }
        q += WAL_HDR + body;
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
    /* the same segment with a marker (a DROP, which a replay skips and numbers) after the rows */
    uint64_t end = 1;
    wal_rec r;
    size_t pos = 0, rl;
    while (pos < f->n && (rl = wal_record(f->d + pos, f->n - pos, seq, pos, &r)) != 0) {
        end = r.fno + r.span;
        pos += rl;
    }
    mk m;
    memset(&m, 0, sizeof m);
    m.type = MK_DROP;
    m.u.drop = (mk_drop){.wm_gen = 1, .dropped_before = 0, .block = 7, .partition = MK_NO_PARTITION};
    uint8_t *pl;
    size_t pn;
    if (mk_encode(&m, &pl, &pn) != 0)
        exit(1);
    uint8_t *buf = calloc(1, pos + WAL_HDR + pn);
    memcpy(buf, f->d, pos);
    store32(buf + pos, (uint32_t)pn | (uint32_t)WAL_KIND_MARKER << 28);
    memcpy(buf + pos + 16, &end, 8);
    memcpy(buf + pos + WAL_HDR, pl, pn);
    reseal(buf, pos + WAL_HDR + pn);
    fuzz_put(dir, "seed-wal-marker", buf, pos + WAL_HDR + pn);
    free(buf);
    free(pl);
}
