/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled (.ec) part files (src/ecpart.h): part_open_ec on any bytes, and the object record's decode alone.
 *
 * An input is a whole .ec file, written under one fixed name (the template's first part, as .ec). Its hashes are no
 * authentication, so a crafted file carries valid ones: the harness re-seals every input whose trailer places the
 * record inside the file (the source's footer hash, the record's hash in the trailer, the trailer's own hash), and
 * writes the name's identity (key, block range, level, source name) into the record, or every mutation would only
 * ever be refused at a hash or at the name. Odd sizes open with pin_index (the index read from a checked copy),
 * even ones in place. An accepted file must pass ec_index_intact (nothing changed it since the open), and every
 * granule entry is read through gx_load and gx_ts, whose run must hold it.
 *
 * Inputs that begin with "REC!" are a record alone: ec_record_decode on the rest, and a record it accepts must
 * encode back to the same bytes (every field is stored where the decode read it).
 *
 * The open reads only the engine's dirfd, pin_index and tier: a zeroed eng with those set stands in for one.
 */
#include "template.h"

#include "chunk.h"
#include "ecpart.h"

static const char *D;
static char name[512];
static char source[96];
static int64_t key;
static uint64_t min_block, max_block;
static uint32_t level;
static snap tpl;
static eng E;

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("ecpart");
    template_build(D, true, false, NULL);
    snap_take(&tpl, D);
    const snap_file *f = tpl_find(&tpl, "", ".part");
    if (!f || strlen(f->rel) >= sizeof source ||
        sscanf(f->rel, "p%" SCNd64 "_%" SCNu64 "_%" SCNu64 "_%u.part", &key, &min_block, &max_block, &level) != 4) {
        fprintf(stderr, "fuzz: the template has no part\n");
        exit(1);
    }
    snprintf(source, sizeof source, "%s", f->rel);
    snprintf(name, sizeof name, "%.*s.ec", (int)(strlen(f->rel) - 5), f->rel);
    E.dirfd = open(D, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (E.dirfd < 0) {
        fprintf(stderr, "fuzz: cannot open %s\n", D);
        exit(1);
    }
    atexit(fuzz_rm_at_exit);
    return 0;
}

static uint64_t le64(const uint8_t *b)
{
    uint64_t v;
    memcpy(&v, b, 8);
    return v;
}

/* the hashes and the name's identity, where the trailer's and the footer's bounds allow; part_open_ec checks them all
 * again */
static void reseal(uint8_t *b, size_t size)
{
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer), TL = EC_TRAILER_BYTES;
    if (size < HL + FL + EC_REC_FIXED + TL)
        return;
    uint8_t *tr = b + size - TL;
    uint64_t rlen = le64(tr + EC_TR_REC_LEN);
    if (rlen < EC_REC_FIXED || rlen > size - TL - HL - FL)
        return;
    uint64_t roff = size - TL - rlen;
    memcpy(tr + EC_TR_REC_OFF, &roff, 8);
    uint8_t *rec = b + roff;
    memcpy(rec + EC_REC_KEY, &key, 8);
    memcpy(rec + EC_REC_MIN_BLOCK, &min_block, 8);
    memcpy(rec + EC_REC_MAX_BLOCK, &max_block, 8);
    memcpy(rec + EC_REC_LEVEL, &level, 4);
    memcpy(rec + EC_REC_SOURCE, source, sizeof source);
    /* the source's footer hash over header, index, tombstones and the footer before its hash (part.c part_hash) */
    _Alignas(64) part_footer f; /* see fuzz_part.c: ASan's fake stack aligns to 32 only */
    memcpy(&f, b + roff - FL, FL);
    if (f.index_len <= roff - FL - HL) {
        size_t ilen = (size_t)f.index_len, tlen = (size_t)(roff - FL - HL - ilen);
        uint64_t x = xxh64(b, HL, 0);
        x = xxh64(b + HL, ilen, x);
        if (tlen)
            x = xxh64(b + HL + ilen, tlen, x);
        f.index_hash = xxh64(&f, offsetof(part_footer, index_hash), x);
        memcpy(b + roff - FL, &f, FL);
    }
    uint64_t h = xxh64(rec, (size_t)rlen, 0);
    memcpy(tr + EC_TR_REC_HASH, &h, 8);
    h = xxh64(tr, EC_TR_HASH, 0);
    memcpy(tr + EC_TR_HASH, &h, 8);
}

#define REC_TAG "REC!"

static void record_only(const uint8_t *data, size_t size)
{
    ec_record r;
    if (ec_record_decode(data, size, &r) != 0) {
        if (r.sums || r.nsums)
            abort(); /* a refused record owns nothing */
        return;
    }
    size_t n = ec_record_size(&r);
    uint8_t *b = malloc(n ? n : 1);
    if (n != size || ec_record_encode(&r, b, n) != 0 || memcmp(b, data, n) != 0)
        abort(); /* what the decode accepts, the encode writes back bit for bit */
    free(b);
    ec_record_free(&r);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size >= 4 && memcmp(data, REC_TAG, 4) == 0) {
        record_only(data + 4, size - 4);
        return 0;
    }
    uint8_t *b = malloc(size ? size : 1);
    memcpy(b, data, size);
    reseal(b, size);
    fuzz_put(D, name, b, size);
    free(b);
    E.opt.pin_index = (size & 1) != 0;
    part *p;
    int rc = part_open_ec(&E, name, name, &p);
    if (getenv("FUZZ_ECPART_TRACE")) /* whether the seeds open: a seed the open refuses only explores refusals */
        fprintf(stderr, "fuzz-ecpart: %zu bytes: %d\n", size, rc);
    if (rc == 0) {
        if (!p->ec || !p->verify || !ec_index_intact(p))
            abort(); /* an .ec part is checked whenever read (E11), and the file is as the open found it */
        uint64_t sink = 0;
        for (uint32_t i = 0; i < p->ng; i++) {
            granule g;
            memset(&g, 0, sizeof g);
            gx_load(p, i, GW_ALL, &g);
            int64_t lo, hi;
            gx_ts(p, i, &lo, &hi);
            uint32_t a, e;
            gix_run(&p->ix, gx_series(p, i), &a, &e);
            if (a > i || i >= e || e > p->ng)
                abort(); /* the run of an entry's own series holds it */
            sink += g.count + (uint64_t)lo + (uint64_t)hi + gx_count(p, i);
        }
        for (uint32_t i = 0; i < p->nt; i++)
            sink += p->t[i].series + (uint64_t)p->t[i].start;
        __asm__ volatile("" : : "r"(sink));
        part_unref(p);
    }
    return 0;
}

/* The .ec image of a part's bytes, as ec_index_write lays it out: header, index and tombstones, footer, record,
 * trailer (reseal names this harness's part in it). */
static uint8_t *ec_image(const uint8_t *d, size_t n, size_t *out)
{
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer);
    _Alignas(64) part_footer f;
    if (n < HL + FL)
        return NULL;
    memcpy(&f, d + n - FL, FL);
    if (f.index_off < HL || f.index_off > n - FL)
        return NULL;
    size_t body = n - FL - (size_t)f.index_off; /* index and tombstones */
    chunk_geom g;
    if (chunk_geom_derive(&g, 2, 1, n) != 0)
        return NULL;
    ec_record r;
    memset(&r, 0, sizeof r);
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    r.k = (uint16_t)g.k;
    r.m = (uint16_t)g.m;
    r.cell_bytes = g.cell;
    r.object_len = n;
    r.object_xxh64 = xxh64(d, n, 0);
    r.writer_epoch = 1;
    snprintf(r.source, sizeof r.source, "%s", source);
    r.nsums = chunk_sums_total(&g);
    r.sums = calloc(r.nsums ? r.nsums : 1, 8);
    size_t rlen = ec_record_size(&r), len = HL + body + FL + rlen + EC_TRAILER_BYTES;
    uint8_t *b = calloc(1, len);
    if (!r.sums || !b || ec_record_encode(&r, b + HL + body + FL, rlen) != 0) {
        free(b);
        ec_record_free(&r);
        return NULL;
    }
    ec_record_free(&r);
    memcpy(b, d, HL);
    memcpy(b + HL, d + f.index_off, body + FL);
    uint8_t *tr = b + len - EC_TRAILER_BYTES;
    uint32_t v = EC_TRAILER_VERSION;
    memcpy(tr + EC_TR_MAGIC, EC_TRAILER_MAGIC, 8);
    memcpy(tr + EC_TR_VERSION, &v, 4);
    v = EC_RECORD_VERSION;
    memcpy(tr + EC_TR_REC_VERSION, &v, 4);
    uint64_t q = rlen;
    memcpy(tr + EC_TR_REC_LEN, &q, 8);
    reseal(b, len);
    *out = len;
    return b;
}

void fuzz_seeds(const char *dir)
{
    for (size_t i = 0; i < tpl.n; i++)
        if (!tpl.f[i].dir && strstr(tpl.f[i].rel, ".part")) {
            size_t n;
            uint8_t *b = ec_image(tpl.f[i].d, tpl.f[i].n, &n);
            if (!b)
                continue;
            char seed[600];
            snprintf(seed, sizeof seed, "seed-%s", tpl.f[i].rel);
            fuzz_put(dir, seed, b, n);
            /* its record alone */
            uint64_t rlen = le64(b + n - EC_TRAILER_BYTES + EC_TR_REC_LEN);
            uint8_t *r = malloc(4 + (size_t)rlen);
            memcpy(r, REC_TAG, 4);
            memcpy(r + 4, b + n - EC_TRAILER_BYTES - rlen, (size_t)rlen);
            snprintf(seed, sizeof seed, "seed-record-%s", tpl.f[i].rel);
            fuzz_put(dir, seed, r, 4 + (size_t)rlen);
            free(r);
            free(b);
        }
}
