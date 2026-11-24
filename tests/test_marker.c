/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R8's marker bodies and the checks around them (src/marker.c, docs/design-cluster-data.md 2.3, E14, E17, E26):
 *  - COLD, GC and DIGEST round-trip, flush_through (F) included, and their lists walk back entry by entry;
 *  - every strict prefix and every extension by 4 bytes of an encoding is refused, and so is a file of the wrong kind
 *    (a COMMIT's with a kind, a COLD's without one) and a record of 0 or more than MK_REC_MAX bytes;
 *  - the bounds: a count past MK_FILES_MAX is refused for each list by the decoder and the encoder (the mutant
 *    mkcoldbounds, which leaves them out, must fail here);
 *  - record_valid: ppos or stripe_version != 0 refused on a single server's tier and accepted for a cluster ec volume,
 *    positions != 0 refused always (mutant ecpposloose);
 *  - every reserved byte of COLD (its fixed part, an object entry, a file entry) and of COMMIT's file entries is refused
 *    when set, and COLD's retired oids (R10's) at any count (mutant mkretired);
 *  - E17's admission table: GC and DIGEST need requires=ec, COLD ec or tier, 14 and 15 are admitted nowhere; the writer
 *    of a volume refuses what it does not admit (mutant mkwriteany), and an open refuses a segment that holds one,
 *    written while META had the word (mutant mkadmitall); with the word it replays;
 *  - requires=cluster,ec written through META's rewrite (meta_require) makes ec_cluster at the open, either word alone not.
 *  - requires=tier alone (a single server's tier) admits COLD at the writer and the replay, GC still refused.
 *
 *   ./build/test_marker [scratch dir]                                  must pass
 *   ./build/test_marker --mutant mkcoldbounds|mkadmitall|ecpposloose|mkwriteany|mkretired [scratch dir]    must fail
 */
#define _GNU_SOURCE
#include "internal.h"
#include "chunk.h"
#include "ecpart.h"
#include "tier.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

static void oid_of(uint8_t oid[MK_OID], uint32_t i)
{
    for (int j = 0; j < MK_OID; j++)
        oid[j] = (uint8_t)(i * 31 + (uint32_t)j * 7 + 1);
}

/* every strict prefix and the encoding with 4 more bytes fail; the encoding itself decodes */
static void truncations(const uint8_t *p, size_t n, const char *what)
{
    mk m;
    int bad = 0;
    for (size_t k = 0; k < n; k++)
        bad += mk_decode(p, k, &m) == 0;
    uint8_t *q = calloc(1, n + 4);
    memcpy(q, p, n);
    bad += mk_decode(q, n + 4, &m) == 0;
    free(q);
    if (bad)
        fprintf(stderr, "%s: %d truncated or extended encodings decode\n", what, bad);
    CHECK(bad == 0);
}

/* ---------------------------------------------------------------- COLD */
static void test_cold(void)
{
    /* three objects of record lengths 1, 5 and 4096 (padding on two), two .ec files and a Y file; two retired oids are
     * refused (R10's list: written empty in R8, and a decode refuses one) */
    uint32_t rl[3] = {1, 5, 4096};
    size_t olen = 0;
    for (int i = 0; i < 3; i++)
        olen += mk_cold_obj_size(rl[i]);
    uint8_t *objs = malloc(olen), *rec = malloc(4096);
    for (int i = 0; i < 4096; i++)
        rec[i] = (uint8_t)(i * 13 + 5);
    size_t at = 0;
    for (uint32_t i = 0; i < 3; i++) {
        uint8_t oid[MK_OID];
        oid_of(oid, i);
        at += mk_cold_obj_put(objs + at, oid, rec, rl[i]);
    }
    CHECK(at == olen);
    mk_file fs[3] = {{"p5_1_4_1.ec", 11, 999, 1, 2, MK_FILE_EC},
                     {"p5_5_9_1.ec", 11, 1999, 3, 4, MK_FILE_EC},
                     {"p5_10_10_0.part", 15, 77777, 5, 6, MK_FILE_Y}};
    uint8_t files[512];
    size_t fl = 0;
    for (int i = 0; i < 3; i++)
        fl += mk_file_put(files + fl, &fs[i]);
    uint8_t retired[2 * MK_OID];
    oid_of(retired, 100);
    oid_of(retired + MK_OID, 101);

    mk m, d;
    memset(&m, 0, sizeof m);
    m.type = MK_COLD;
    m.u.cold = (mk_cold){.z = 123456789, .flush_through = 41, .stripe_version = 7, .key = 4 * 19700 + 1,
                         .nobjs = 3, .nfiles = 3, .nretired = 2, .objs = objs, .objs_len = olen, .files = files,
                         .files_len = fl, .retired = retired};
    uint8_t *p;
    size_t n;
    int erc = mk_encode(&m, &p, &n); /* the encoder's own decode refuses the list */
    CHECK(erc == TSDB_ERR_INVALID);
    if (erc == 0)
        free(p);
    m.u.cold.nretired = 0, m.u.cold.retired = NULL;
    CHECK(mk_encode(&m, &p, &n) == 0);
    CHECK(mk_decode(p, n, &d) == 0);
    CHECK(d.type == MK_COLD && d.span == 1 && strcmp(mk_type_name(MK_COLD), "COLD") == 0);
    const mk_cold *c = &d.u.cold;
    CHECK(c->z == 123456789 && c->flush_through == 41 && c->stripe_version == 7 && c->key == 4 * 19700 + 1);
    CHECK(c->nobjs == 3 && c->nfiles == 3 && c->nretired == 0 && c->objs_len == olen && c->files_len == fl);
    size_t pos = 0;
    mk_cold_obj o;
    uint32_t no = 0;
    while (mk_cold_obj_next(c, &pos, &o)) {
        uint8_t oid[MK_OID];
        oid_of(oid, no);
        CHECK(no < 3 && o.rec_len == rl[no] && memcmp(o.oid, oid, MK_OID) == 0 && memcmp(o.rec, rec, o.rec_len) == 0);
        no++;
    }
    CHECK(no == 3 && pos == c->objs_len);
    pos = 0;
    mk_file f;
    uint32_t nf = 0;
    while (mk_cold_file_next(c, &pos, &f)) {
        CHECK(nf < 3 && f.name_len == fs[nf].name_len && memcmp(f.name, fs[nf].name, f.name_len) == 0 &&
              f.size == fs[nf].size && f.xxh64 == fs[nf].xxh64 && f.header_check == fs[nf].header_check &&
              f.kind == fs[nf].kind);
        nf++;
    }
    CHECK(nf == 3 && pos == c->files_len);
    truncations(p, n, "COLD");

    /* the same payload with two retired oids behind it and the count set: refused (with mkretired it decodes) */
    uint8_t *q = malloc(n + sizeof retired);
    memcpy(q, p, n);
    memcpy(q + n, retired, sizeof retired);
    uint32_t two = 2;
    memcpy(q + MK_HDR + 40, &two, 4);
    CHECK(mk_decode(q, n + sizeof retired, &d) == TSDB_ERR_CORRUPT);
    free(q);
    /* every reserved byte the encoder writes as zero: the fixed part's u32 at 44, an object entry's u32 after rec_len,
     * a file entry's byte after the kind; one encoding per payload */
    size_t resv[3] = {MK_HDR + 44, MK_HDR + 48 + MK_OID + 4, MK_HDR + 48 + olen + 3};
    for (int k = 0; k < 3; k++) {
        CHECK(p[resv[k]] == 0);
        p[resv[k]] = 1;
        int rc = mk_decode(p, n, &d);
        if (rc != TSDB_ERR_CORRUPT)
            fprintf(stderr, "COLD with reserved byte %zu set decodes (%d)\n", resv[k], rc);
        CHECK(rc == TSDB_ERR_CORRUPT);
        p[resv[k]] = 0;
    }
    CHECK(mk_decode(p, n, &d) == 0);
    {
        /* COMMIT's file entries share the layout: their pad bytes (2 and 3: no kind there) are refused too */
        mk cm, cd;
        uint8_t cf[64], *cp;
        size_t cn;
        mk_file f0 = fs[2];
        f0.kind = 0; /* a COMMIT's file has none */
        memset(&cm, 0, sizeof cm);
        cm.type = MK_COMMIT;
        cm.u.commit = (mk_commit){.op = MK_OP_INSERT, .keys_form = MK_KEYS_NONE, .events = 1, .nfiles = 1,
                                  .files = cf, .files_len = mk_file_put(cf, &f0)};
        CHECK(mk_encode(&cm, &cp, &cn) == 0);
        if (cp) {
            CHECK(mk_decode(cp, cn, &cd) == 0);
            for (size_t off = MK_HDR + 40 + 2; off <= MK_HDR + 40 + 3; off++) {
                CHECK(cp[off] == 0);
                cp[off] = 1;
                CHECK(mk_decode(cp, cn, &cd) == TSDB_ERR_CORRUPT);
                cp[off] = 0;
            }
            free(cp);
        }
    }

    /* the kind byte: a COLD file of kind 0 or 3 is refused, and a COMMIT's file with a kind */
    size_t kind_off = MK_HDR + 48 + olen + mk_file_size(&fs[0]) + mk_file_size(&fs[1]) + 2; /* the Y file's */
    CHECK(p[kind_off] == MK_FILE_Y);
    for (int k = 0; k < 4; k += 3) {
        p[kind_off] = (uint8_t)k;
        CHECK(mk_decode(p, n, &d) == TSDB_ERR_CORRUPT);
    }
    p[kind_off] = MK_FILE_Y;
    CHECK(mk_decode(p, n, &d) == 0);
    free(p);
    mk cm;
    memset(&cm, 0, sizeof cm);
    cm.type = MK_COMMIT;
    cm.u.commit = (mk_commit){.op = MK_OP_INSERT, .keys_form = MK_KEYS_NONE, .nfiles = 1, .files = files,
                              .files_len = mk_file_size(&fs[0])};
    CHECK(mk_encode(&cm, &p, &n) == TSDB_ERR_INVALID); /* an .ec kind in a COMMIT */
    fs[0].kind = 0;
    mk_file_put(files, &fs[0]);
    CHECK(mk_encode(&cm, &p, &n) == 0);
    free(p);
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID); /* and a COLD file without one */

    /* empty lists, and the record lengths at their ends */
    memset(&m.u.cold, 0, sizeof m.u.cold);
    m.u.cold.z = 9;
    CHECK(mk_encode(&m, &p, &n) == 0 && n == MK_HDR + 48);
    free(p);
    uint8_t *big = malloc(mk_cold_obj_size(MK_REC_MAX + 4)), *brec = calloc(1, MK_REC_MAX + 4);
    uint8_t oid[MK_OID] = {0};
    m.u.cold.nobjs = 1;
    m.u.cold.objs = big;
    m.u.cold.objs_len = mk_cold_obj_put(big, oid, brec, MK_REC_MAX);
    CHECK(mk_encode(&m, &p, &n) == 0);
    free(p);
    m.u.cold.objs_len = mk_cold_obj_put(big, oid, brec, MK_REC_MAX + 4);
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    m.u.cold.objs_len = mk_cold_obj_put(big, oid, brec, 0);
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    free(big), free(brec), free(objs), free(rec);
    printf("  COLD: round trip with F, lists, truncation, kinds, record lengths\n");
}

/* ---------------------------------------------------------------- GC and DIGEST */
static void test_gc_digest(void)
{
    uint8_t oids[5 * MK_OID];
    for (uint32_t i = 0; i < 5; i++)
        oid_of(oids + i * MK_OID, i + 50);
    mk m, d;
    uint8_t *p;
    size_t n;
    for (uint32_t reason = MK_GC_RETIRED; reason <= MK_GC_DONE; reason++) {
        memset(&m, 0, sizeof m);
        m.type = MK_GC;
        m.u.gc = (mk_gc){.reason = reason, .noids = 5, .oids = oids};
        CHECK(mk_encode(&m, &p, &n) == 0 && n == MK_HDR + 8 + 5 * MK_OID);
        CHECK(mk_decode(p, n, &d) == 0 && d.type == MK_GC && d.span == 1 && d.u.gc.reason == reason &&
              d.u.gc.noids == 5 && memcmp(mk_oid_at(d.u.gc.oids, 4), oids + 4 * MK_OID, MK_OID) == 0);
        truncations(p, n, "GC");
        free(p);
    }
    m.u.gc.reason = 0;
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    m.u.gc.reason = MK_GC_DONE + 1;
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    m.u.gc.reason = MK_GC_ORPHAN, m.u.gc.noids = 0;
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID); /* a GC names something */

    memset(&m, 0, sizeof m);
    m.type = MK_DIGEST;
    m.u.digest = (mk_digest){.key = -8, .z = 77, .rows = 1ull << 40, .series = 10000, .xxh64 = 0xfeedfacecafef00dull,
                             .now_ms = -1700000000123, .noids = 5, .oids = oids};
    m.flags = MKD_TOMBS;
    CHECK(mk_encode(&m, &p, &n) == 0 && n == MK_HDR + 56 + 5 * MK_OID);
    CHECK(mk_decode(p, n, &d) == 0 && d.type == MK_DIGEST && d.span == 1);
    const mk_digest *g = &d.u.digest;
    CHECK(g->key == -8 && g->z == 77 && g->rows == 1ull << 40 && g->series == 10000 &&
          g->xxh64 == 0xfeedfacecafef00dull && g->now_ms == -1700000000123 && d.flags == MKD_TOMBS && g->noids == 5 &&
          memcmp(g->oids, oids, 5 * MK_OID) == 0);
    truncations(p, n, "DIGEST");
    p[MK_HDR + 52] = 1; /* the reserved word */
    CHECK(mk_decode(p, n, &d) == TSDB_ERR_CORRUPT);
    free(p);
    CHECK(MK_TYPE_LAST == 13 && strcmp(mk_type_name(MK_GC), "GC") == 0 &&
          strcmp(mk_type_name(MK_DIGEST), "DIGEST") == 0 && mk_type_name(14) == NULL);
    printf("  GC and DIGEST: round trips, reasons, truncation\n");
}

/* ---------------------------------------------------------------- bounds */
/* decodes a hand-made payload of `type`: its fixed part, then `rest` (the entries a count in the fixed part names) */
static int decode_body(uint16_t type, const uint8_t *fixed, size_t flen, const uint8_t *rest, size_t rlen)
{
    size_t n = MK_HDR + flen + rlen;
    uint8_t *p = calloc(1, n);
    uint32_t span = 1;
    memcpy(p, &type, 2);
    memcpy(p + 4, &span, 4);
    memcpy(p + MK_HDR, fixed, flen);
    if (rlen)
        memcpy(p + MK_HDR + flen, rest, rlen);
    mk m;
    int rc = mk_decode(p, n, &m);
    free(p);
    return rc;
}

static void test_bounds(void)
{
    const uint32_t over = MK_FILES_MAX + 1;
    /* COLD: over-many objects (each 1 byte of record), files, retired oids, one list at a time */
    size_t osz = mk_cold_obj_size(1);
    uint8_t *objs = calloc(over, osz), one = 0xab, oid[MK_OID] = {1};
    for (uint32_t i = 0; i < over; i++)
        mk_cold_obj_put(objs + (size_t)i * osz, oid, &one, 1);
    mk_file f = {"p1_1_1_0.part", 13, 1, 2, 3, MK_FILE_Y};
    size_t fsz = mk_file_size(&f);
    uint8_t *files = calloc(over, fsz);
    for (uint32_t i = 0; i < over; i++)
        mk_file_put(files + (size_t)i * fsz, &f);
    uint8_t *ids = calloc(over, MK_OID);
    uint8_t fixed[56];
    for (uint32_t cnt = MK_FILES_MAX; cnt <= over; cnt++) {
        int want = cnt == over ? TSDB_ERR_CORRUPT : 0;
        for (int list = 0; list < 3; list++) {
            memset(fixed, 0, sizeof fixed);
            memcpy(fixed + 32 + 4 * list, &cnt, 4);
            const uint8_t *rest = list == 0 ? objs : list == 1 ? files : ids;
            size_t rlen = (size_t)cnt * (list == 0 ? osz : list == 1 ? fsz : MK_OID);
            int rc = decode_body(MK_COLD, fixed, 48, rest, rlen);
            int w = list == 2 ? TSDB_ERR_CORRUPT : want; /* a retired oid is refused at any count (R10's) */
            if (rc != w)
                fprintf(stderr, "COLD list %d with %u entries: decode gives %d, not %d\n", list, cnt, rc, w);
            CHECK(rc == w);
        }
        /* GC and DIGEST */
        uint32_t g[2] = {MK_GC_RETIRED, cnt};
        CHECK(decode_body(MK_GC, (const uint8_t *)g, 8, ids, (size_t)cnt * MK_OID) == want);
        memset(fixed, 0, sizeof fixed);
        memcpy(fixed + 48, &cnt, 4);
        CHECK(decode_body(MK_DIGEST, fixed, 56, ids, (size_t)cnt * MK_OID) == want);
    }
    /* the encoder refuses a count past the bound as well */
    mk m;
    uint8_t *p;
    size_t n;
    memset(&m, 0, sizeof m);
    m.type = MK_COLD;
    m.u.cold = (mk_cold){.nretired = over, .retired = ids};
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    memset(&m, 0, sizeof m);
    m.type = MK_GC;
    m.u.gc = (mk_gc){.reason = MK_GC_DONE, .noids = over, .oids = ids};
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    memset(&m, 0, sizeof m);
    m.type = MK_DIGEST;
    m.u.digest = (mk_digest){.noids = over, .oids = ids};
    CHECK(mk_encode(&m, &p, &n) == TSDB_ERR_INVALID);
    /* a huge count with nothing behind it: refused without walking past the end */
    uint32_t huge = UINT32_MAX;
    for (int list = 0; list < 3; list++) {
        memset(fixed, 0, sizeof fixed);
        memcpy(fixed + 32 + 4 * list, &huge, 4);
        CHECK(decode_body(MK_COLD, fixed, 48, NULL, 0) == TSDB_ERR_CORRUPT);
    }
    free(objs), free(files), free(ids);
    printf("  bounds: %u entries per list decode, %u are refused (COLD's three lists, GC, DIGEST)\n", MK_FILES_MAX,
           over);
}

/* ---------------------------------------------------------------- record_valid */
static void test_record_valid(void)
{
    ec_record r, d;
    memset(&r, 0, sizeof r);
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, 2, 1, 3000) == 0);
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    oid_of(r.oid, 3);
    r.key = 77, r.min_block = 3, r.max_block = 9, r.level = 2;
    r.k = g.k, r.m = g.m, r.cell_bytes = g.cell, r.object_len = 3000;
    snprintf(r.source, sizeof r.source, "p77_3_9_2.part");
    r.nsums = chunk_sums_total(&g);
    r.sums = calloc(r.nsums, 8);
    size_t n = ec_record_size(&r);
    uint8_t *b = malloc(n);
    CHECK(ec_record_encode(&r, b, n) == 0 && ec_record_decode(b, n, &d) == 0); /* R7's record, every volume */
    ec_record_free(&d);
    CHECK(ec_record_encode_as(&r, b, n, true) == 0 && ec_record_decode_as(b, n, &d, true) == 0);
    ec_record_free(&d);
    for (int field = 0; field < 2; field++) {
        r.ppos = field == 0 ? 4242 : 0;
        r.stripe_version = field == 1 ? 3 : 0;
        CHECK(ec_record_encode(&r, b, n) == TSDB_ERR_INVALID); /* a single server's tier: refused */
        CHECK(ec_record_encode_as(&r, b, n, false) == TSDB_ERR_INVALID);
        CHECK(ec_record_encode_as(&r, b, n, true) == 0); /* a cluster ec volume: taken */
        CHECK(ec_record_decode(b, n, &d) == TSDB_ERR_CORRUPT && d.sums == NULL);
        CHECK(ec_record_decode_as(b, n, &d, false) == TSDB_ERR_CORRUPT);
        CHECK(ec_record_decode_as(b, n, &d, true) == 0 && d.ppos == r.ppos && d.stripe_version == r.stripe_version);
        ec_record_free(&d);
    }
    r.ppos = 1, r.stripe_version = 1, r.positions = 1; /* E10 is R9's: refused everywhere */
    CHECK(ec_record_encode_as(&r, b, n, true) == TSDB_ERR_INVALID && ec_record_encode(&r, b, n) == TSDB_ERR_INVALID);
    r.positions = 0;
    CHECK(ec_record_encode_as(&r, b, n, true) == 0);
    uint32_t pos = 1;
    memcpy(b + 60, &pos, 4); /* EC_REC_POSITIONS */
    CHECK(ec_record_decode_as(b, n, &d, true) == TSDB_ERR_CORRUPT);
    free(b), free(r.sums);
    printf("  record_valid: ppos and stripe_version only in a cluster ec volume, positions nowhere\n");
}

/* ---------------------------------------------------------------- the open */
static int open_eng(const char *dir, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    return eng_open(dir, &o, db);
}

static void marker_of(mk *m, uint16_t type, uint8_t oid[MK_OID])
{
    memset(m, 0, sizeof *m);
    m->type = type;
    if (type == MK_GC)
        m->u.gc = (mk_gc){.reason = MK_GC_RETIRED, .noids = 1, .oids = oid};
    else if (type == MK_DIGEST)
        m->u.digest = (mk_digest){.key = 1, .z = 1, .noids = 1, .oids = oid};
    else
        m->u.cold = (mk_cold){.z = 1, .key = 1}; /* COLD with empty lists */
}

/* a child opens the volume, appends one marker of `type` and ends without a close: the segment stays for the next open */
static void leave_marker(const char *dir, uint16_t type)
{
    pid_t pid = fork();
    if (pid == 0) {
        eng *db;
        if (open_eng(dir, &db) != 0)
            _exit(2);
        uint8_t oid[MK_OID] = {9};
        mk m;
        marker_of(&m, type, oid);
        _exit(wal_marker_mk(db, &m, NULL) == 0 ? 0 : 3);
    }
    int st = 0;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
}

/* META's requires= line set to `words` ("" drops it): what an operator's edit or a lost word leaves */
static void set_requires(const char *dir, const char *words)
{
    char path[PATH_MAX + 8], buf[4096];
    snprintf(path, sizeof path, "%s/META", dir);
    FILE *f = fopen(path, "r");
    CHECK(f != NULL);
    if (!f)
        return;
    size_t n = 0;
    char line[512];
    while (fgets(line, sizeof line, f))
        if (strncmp(line, "requires=", 9) != 0 && n + strlen(line) < sizeof buf)
            n += (size_t)snprintf(buf + n, sizeof buf - n, "%s", line);
    fclose(f);
    if (words[0])
        n += (size_t)snprintf(buf + n, sizeof buf - n, "requires=%s\n", words);
    f = fopen(path, "w");
    CHECK(f != NULL && fwrite(buf, 1, n, f) == n);
    if (f)
        fclose(f);
}

static void fresh_dir(const char *scratch, const char *what, char *dir, size_t cap)
{
    char cmd[PATH_MAX + 16];
    snprintf(dir, cap, "%s/test_marker_%d_%s", scratch, (int)getpid(), what);
    snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    if (system(cmd) != 0 || mkdir(dir, 0755) != 0)
        failures++;
}

static void rm_dir(const char *dir)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    if (system(cmd) != 0)
        failures++;
}

/* E17's table: which volume admits which type, at the writer and at the replay */
static void test_admitted(void)
{
    for (uint32_t t = 1; t <= 70000; t = t < 40 ? t + 1 : t * 2) {
        uint16_t ty = (uint16_t)t;
        bool later = ty >= MK_EC_LATER_LO && ty <= MK_EC_LATER_HI;
        bool ec_only = ty == MK_GC || ty == MK_DIGEST;
        for (int ec = 0; ec < 2; ec++)
            for (int tier = 0; tier < 2; tier++) {
                bool want = later ? false : ty == MK_COLD ? (ec || tier) : ec_only ? ec : true;
                if (mk_type_admitted(ty, ec, tier) != want)
                    fprintf(stderr, "type %u, ec %d, tier %d: admitted %d\n", t, ec, tier, !want);
                CHECK(mk_type_admitted(ty, ec, tier) == want);
            }
    }
    printf("  admitted: GC, DIGEST with ec; COLD with ec or tier; 14-15 nowhere; the rest everywhere\n");
}

static void test_open(const char *scratch)
{
    static const uint16_t types[3] = {MK_GC, MK_DIGEST, MK_COLD};
    for (int t = 0; t < 3; t++) {
        uint16_t type = types[t];
        char dir[PATH_MAX];
        fresh_dir(scratch, mk_type_name(type), dir, sizeof dir);
        eng *db;
        CHECK(open_eng(dir, &db) == 0); /* makes META */
        if (db) {
            /* the writer of a volume without the word refuses it: nothing in the log, the next open is clean */
            uint8_t oid[MK_OID] = {9};
            mk m;
            marker_of(&m, type, oid);
            uint64_t fno = 0;
            int rc = wal_marker_mk(db, &m, &fno);
            if (rc != TSDB_ERR_INVALID)
                fprintf(stderr, "a volume without requires=ec logged a %s marker (%d)\n", mk_type_name(type), rc);
            CHECK(rc == TSDB_ERR_INVALID);
            CHECK(wal_marker(db, type, 0, 1, NULL, 0, &fno) == TSDB_ERR_INVALID); /* the raw writer as well */
            eng_close(db);
        }
        CHECK(open_eng(dir, &db) == 0);
        if (db)
            eng_close(db);
        /* a segment written while META had the word, then META without it: the open refuses, never skips */
        set_requires(dir, "ec");
        leave_marker(dir, type);
        set_requires(dir, "");
        int rc = open_eng(dir, &db);
        if (rc == 0) {
            fprintf(stderr, "a volume without requires=ec opened with a %s marker in its WAL\n", mk_type_name(type));
            eng_close(db);
        }
        CHECK(rc == TSDB_ERR_CORRUPT);
        set_requires(dir, "ec"); /* with the word, the same segment replays (the marker is skipped as before) */
        rc = open_eng(dir, &db);
        CHECK(rc == 0);
        if (rc == 0) {
            CHECK(db->ec_req && !db->ec_cluster && !db->tier_req);
            eng_close(db);
        }
        rm_dir(dir);
    }
    /* 14 and 15 (R10's): a segment that holds one is refused even with the word; the writer never logs one */
    for (uint16_t type = MK_EC_LATER_LO; type <= MK_EC_LATER_HI; type++) {
        char dir[PATH_MAX], what[16];
        snprintf(what, sizeof what, "later%u", (unsigned)type);
        fresh_dir(scratch, what, dir, sizeof dir);
        eng *db;
        CHECK(open_eng(dir, &db) == 0);
        if (db)
            eng_close(db);
        set_requires(dir, "ec");
        static const uint8_t body[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        CHECK(open_eng(dir, &db) == 0);
        if (db) {
            uint64_t fno = 0;
            int rc = wal_marker(db, type, 0, 1, body, sizeof body, &fno);
            if (rc != TSDB_ERR_INVALID)
                fprintf(stderr, "an ec volume logged a marker of type %u (%d)\n", (unsigned)type, rc);
            CHECK(rc == TSDB_ERR_INVALID);
            eng_close(db);
        }
        pid_t pid = fork(); /* a later release's segment: planted through the writer's test hook */
        if (pid == 0) {
            if (open_eng(dir, &db) != 0)
                _exit(2);
            mk_test_mutant = MK_MUT_WRITE_ANY;
            uint64_t fno = 0;
            _exit(wal_marker(db, type, 0, 1, body, sizeof body, &fno) == 0 ? 0 : 3);
        }
        int st = 0;
        CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
        int rc = open_eng(dir, &db);
        if (rc == 0) {
            fprintf(stderr, "an ec volume opened with a marker of type %u in its WAL\n", (unsigned)type);
            eng_close(db);
        }
        CHECK(rc == TSDB_ERR_CORRUPT);
        rm_dir(dir);
    }
    printf("  open: GC, DIGEST, COLD refused by the writer and the replay of a volume without the word\n");
}

/* requires=cluster,ec (through META's rewrite, as r8f's attach): ec_cluster at the open; cluster or ec alone does not */
static void test_cluster_ec(const char *scratch)
{
    static const struct { unsigned add; bool ec, cluster; } c[] = {{META_REQ_CLUSTER | META_REQ_EC, true, true},
                                                                   {META_REQ_CLUSTER, false, true},
                                                                   {META_REQ_EC, true, false}};
    for (size_t k = 0; k < sizeof c / sizeof c[0]; k++) {
        char dir[PATH_MAX], what[16];
        snprintf(what, sizeof what, "cl%zu", k);
        fresh_dir(scratch, what, dir, sizeof dir);
        eng *db;
        CHECK(open_eng(dir, &db) == 0);
        if (db)
            eng_close(db);
        int fd = open(dir, O_RDONLY | O_DIRECTORY);
        CHECK(fd >= 0 && meta_require(fd, true, c[k].add) == 0);
        if (fd >= 0)
            close(fd);
        meta_cluster_open_allow(true);
        int rc = open_eng(dir, &db);
        meta_cluster_open_allow(false);
        CHECK(rc == 0);
        if (rc == 0) {
            if (db->ec_req != c[k].ec || db->cluster_req != c[k].cluster || db->ec_cluster != (c[k].ec && c[k].cluster))
                fprintf(stderr, "requires %#x: ec_req %d cluster_req %d ec_cluster %d\n", c[k].add, db->ec_req,
                        db->cluster_req, db->ec_cluster);
            CHECK(db->ec_req == c[k].ec && db->cluster_req == c[k].cluster &&
                  db->ec_cluster == (c[k].ec && c[k].cluster));
            eng_close(db);
        }
        rm_dir(dir);
    }
    printf("  requires=cluster,ec: an erasure-coded cluster volume; either word alone is not\n");
}

#ifdef TSDB_EC
/* E55: COLD is admitted on a single server's tier volume (requires=tier, no ec): its writer logs one and the next open
 * replays it; GC and DIGEST stay refused there. tier_req must come from META at the open (mutant: never set). */
static int open_tiered(const char *dir, tier **t, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    *t = NULL;
    *db = NULL;
    int rc = tier_open(dir, &o, t);
    if (rc)
        return rc;
    tier_want(*t, 0);
    rc = eng_open(dir, &o, db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(*t);
        *t = NULL;
        *db = NULL;
    }
    return rc;
}

static void close_tiered(tier *t, eng *db)
{
    if (db)
        eng_close(db);
    if (t)
        tier_put(t);
}

static void test_cold_tier(const char *scratch)
{
    char dir[PATH_MAX], stores[PATH_MAX + 16], spec[4 * PATH_MAX];
    fresh_dir(scratch, "coldtier", dir, sizeof dir);
    snprintf(stores, sizeof stores, "%s.st", dir);
    char cmd[2 * PATH_MAX + 64];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && mkdir -p '%s/st0' '%s/st1' '%s/st2'", stores, stores, stores, stores);
    CHECK(system(cmd) == 0);
    snprintf(spec, sizeof spec, "k=2,m=1,dirs=%s/st0:%s/st1:%s/st2", stores, stores, stores);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    tier *t;
    eng *db;
    int rc = open_tiered(dir, &t, &db); /* writes requires=tier */
    CHECK(rc == 0);
    if (rc == 0) {
        if (!db->tier_req || db->ec_req)
            fprintf(stderr, "a tier volume opened with tier_req %d ec_req %d\n", db->tier_req, db->ec_req);
        CHECK(db->tier_req && !db->ec_req);
        uint8_t oid[MK_OID] = {9};
        mk m;
        marker_of(&m, MK_GC, oid);
        uint64_t fno = 0;
        CHECK(wal_marker_mk(db, &m, &fno) == TSDB_ERR_INVALID); /* GC needs ec */
        close_tiered(t, db);
    }
    pid_t pid = fork(); /* a COLD marker left in the WAL (no clean close) */
    if (pid == 0) {
        if (open_tiered(dir, &t, &db) != 0)
            _exit(2);
        uint8_t oid[MK_OID] = {9};
        mk m;
        marker_of(&m, MK_COLD, oid);
        _exit(wal_marker_mk(db, &m, NULL) == 0 ? 0 : 3);
    }
    int st = 0;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st));
    if (WIFEXITED(st) && WEXITSTATUS(st) != 0)
        fprintf(stderr, "a tier volume's writer refused a COLD marker (exit %d)\n", WEXITSTATUS(st));
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    rc = open_tiered(dir, &t, &db);
    if (rc != 0)
        fprintf(stderr, "a tier volume with a COLD marker in its WAL failed to open (%d)\n", rc);
    CHECK(rc == 0);
    if (rc == 0)
        close_tiered(t, db);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    rm_dir(dir);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", stores);
    CHECK(system(cmd) == 0);
    printf("  cold on a tier: requires=tier admits COLD (writer and replay), GC still refused\n");
}
#endif

int main(int argc, char **argv)
{
    int a = 1;
    if (a + 1 < argc && strcmp(argv[a], "--mutant") == 0) {
        const char *mu = argv[a + 1];
        mk_test_mutant = strcmp(mu, "mkcoldbounds") == 0  ? MK_MUT_COLD_BOUNDS
                         : strcmp(mu, "mkadmitall") == 0  ? MK_MUT_ADMIT_ALL
                         : strcmp(mu, "ecpposloose") == 0 ? MK_MUT_PPOS_LOOSE
                         : strcmp(mu, "mkwriteany") == 0  ? MK_MUT_WRITE_ANY
                         : strcmp(mu, "mkretired") == 0   ? MK_MUT_RETIRED
                                                          : -1;
        if (mk_test_mutant < 0) {
            fprintf(stderr, "unknown mutant %s\n", mu);
            return 2;
        }
        a += 2;
    }
    const char *scratch = a < argc ? argv[a] : "/tmp";
    printf("test_marker%s%s\n", mk_test_mutant ? ": mutant " : "", mk_test_mutant ? argv[2] : "");
    test_cold();
    test_gc_digest();
    test_bounds();
    test_record_valid();
    test_admitted();
    test_open(scratch);
    test_cluster_ec(scratch);
#ifdef TSDB_EC
    test_cold_tier(scratch);
#endif
    if (failures) {
        fprintf(stderr, "test_marker: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_marker: ok\n");
    return 0;
}
