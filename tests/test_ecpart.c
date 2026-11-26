/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The .ec index file (src/ecpart.c, R7 r7-ecfile): its record codec, its writer and its open, against the source part
 * it was written from.
 *
 *  - the record: encode/decode round trip, another version refused, every truncation and every bad field refused;
 *  - round trip: parts with one granule, many granules, tombstones and rows (a replace): each written as an .ec file,
 *    swapped in for its source while the engine is closed, and the reopened engine's .ec part compared with the source
 *    opened by part_open: every gx_* field (gx_load GW_ALL), tombstones, rows, time range, wal_seq, fno range, index
 *    geometry and hash; the index check (part_index_intact) holds, and fails on damage after the open; again with
 *    pin_index (the index parsed from the checked copy; damage to the mapping after the open changes nothing);
 *  - damage to every region of the file (header, index, tombstones, footer, record, sums, trailer, length) fails the
 *    open with TSDB_ERR_CORRUPT;
 *  - sealed: the checks behind the hashes, on images whose hashes are made right again (identity, footer against the
 *    index, index_off against object_len, log numbers, tombstone order, R8 fields, the granules' places);
 *  - the writer refuses a record of another part, and a source whose mapped index changed (unless pinned);
 *  - listing: an .ec and a .part of one name both present fails the open (a reader's lists one of them); an .ec file
 *    in a volume whose META lacks requires=tier fails the open; a record whose class check failed fails the open;
 *  - a WAL segment replayed at reopen does not replay into a partition held only by an .ec part;
 *  - fno_next counts the .ec part's numbers (FNO lowered by the test: the slot is taken first otherwise).
 *
 * The tests run without a tier: ec_test_assume_tier stands in for META's requires=tier (r7-tier's), and no granule of
 * an .ec part is read (r7-seam's), so the latest table and statistics are off.
 *
 *   ./build/test_ecpart [scratch dir]
 *   ./build/test_ecpart --mutant ecnohash|ecnowalseq|ecskipname|ecnofno|ecnolayout [scratch dir]     must fail
 */
#define _GNU_SOURCE
#include "internal.h"
#include "chunk.h"
#include "ecpart.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

#define T0 1700000000000LL
static char S[1024]; /* the scratch directory: short enough that every path built from it fits PATH_MAX */

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void fresh(const char *name, char *dir, size_t n)
{
    snprintf(dir, n, "%s/test_ecpart_%d_%s", S, (int)getpid(), name);
    sh("rm -rf '%s' '%s.ref'", dir, dir);
    mkdir(dir, 0755);
    char ref[PATH_MAX + 8];
    snprintf(ref, sizeof ref, "%s.ref", dir);
    mkdir(ref, 0755);
}

static uint32_t grows_opt = 0;
static int pin_opt, ro_opt; /* pin_index, read_only */
static int open_eng(const char *dir, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.pin_index = pin_opt;
    o.read_only = ro_opt;
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.stats = -1;        /* the sweeper and sampler would read only the index, but nothing here needs them */
    o.latest_cache = 0;  /* the warmer would decode granules: an .ec part's are r7-seam's to read */
    if (grows_opt)
        o.granule_rows = grows_opt;
    return eng_open(dir, &o, db);
}

static uint64_t fno_of(eng *db)
{
    pthread_mutex_lock(&db->buf_lock);
    uint64_t v = db->fno_next;
    pthread_mutex_unlock(&db->buf_lock);
    return v;
}

static void rows(eng *db, uint32_t series, int64_t t0, size_t n, float base)
{
    int64_t ts[256];
    float v[256];
    for (size_t i = 0; i < n; i += 256) {
        size_t k = n - i < 256 ? n - i : 256;
        for (size_t j = 0; j < k; j++)
            ts[j] = t0 + (int64_t)(i + j) * 1000, v[j] = base + (float)(i + j);
        CHECK(eng_insert_series(db, TSDB_FLOAT32, series, ts, v, k) == 0);
    }
}

/* ---------------------------------------------------------------- records */
static void make_record(const part *p, uint32_t cls, ec_record *r)
{
    memset(r, 0, sizeof *r);
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, 2, 1, p->map_len) == 0);
    r->version = EC_RECORD_VERSION;
    r->class_check = cls;
    for (int i = 0; i < 16; i++)
        r->oid[i] = (uint8_t)(i * 17 + 3);
    r->key = p->partition;
    r->min_block = p->min_block;
    r->max_block = p->max_block;
    r->level = p->level;
    r->k = g.k;
    r->m = g.m;
    r->cell_bytes = g.cell;
    r->object_len = p->map_len;
    r->object_xxh64 = xxh64(p->map, p->map_len, 0);
    r->writer_epoch = 1;
    r->settle_ms = 1234567;
    snprintf(r->source, sizeof r->source, "%s", p->name);
    r->nsums = chunk_sums_total(&g);
    r->sums = malloc(r->nsums * 8);
    for (uint64_t i = 0; i < r->nsums; i++)
        r->sums[i] = i * 0x9e3779b97f4a7c15ull + 1;
}

static bool records_equal(const ec_record *a, const ec_record *b)
{
    return a->version == b->version && a->class_check == b->class_check && !memcmp(a->oid, b->oid, 16) &&
           a->key == b->key && a->min_block == b->min_block && a->max_block == b->max_block && a->level == b->level &&
           a->k == b->k && a->m == b->m && a->cell_bytes == b->cell_bytes && a->positions == b->positions &&
           a->object_len == b->object_len && a->object_xxh64 == b->object_xxh64 &&
           a->writer_epoch == b->writer_epoch && a->settle_ms == b->settle_ms && a->ppos == b->ppos &&
           a->stripe_version == b->stripe_version && !memcmp(a->source, b->source, 96) && a->nsums == b->nsums &&
           !memcmp(a->sums, b->sums, a->nsums * 8);
}

static void test_record(void)
{
    part fake;
    memset(&fake, 0, sizeof fake);
    uint8_t obj[3000];
    memset(obj, 7, sizeof obj);
    fake.map = obj;
    fake.map_len = sizeof obj;
    fake.partition = -5;
    fake.min_block = 3;
    fake.max_block = 9;
    fake.level = 2;
    snprintf(fake.name, sizeof fake.name, "p-5_3_9_2.part");
    ec_record r, d;
    make_record(&fake, EC_CLS_PASSED, &r);
    size_t n = ec_record_size(&r);
    uint8_t *b = malloc(n + 8);
    CHECK(ec_record_encode(&r, b, n - 1) == TSDB_ERR_INVALID);
    CHECK(ec_record_encode(&r, b, n) == 0);
    CHECK(ec_record_decode(b, n, &d) == 0 && records_equal(&r, &d));
    ec_record_free(&d);
    for (size_t len = 0; len < n; len++) { /* every truncation */
        CHECK(ec_record_decode(b, len, &d) != 0 && d.sums == NULL);
        ec_record_free(&d);
    }
    CHECK(ec_record_decode(b, n + 8, &d) == TSDB_ERR_CORRUPT); /* a sum too many */
    ec_record_free(&d);
    /* every field out of range, one at a time */
    struct { size_t off, width; uint64_t val; const char *what; } bad[] = {
        {EC_REC_VERSION, 4, 2, "another version"},
        {EC_REC_CLASS, 4, 3, "an unknown class result"},
        {EC_REC_K, 2, 0, "k 0"},
        {EC_REC_K, 2, CHUNK_K_MAX + 1, "k too large"},
        {EC_REC_M, 2, 0, "m 0"},
        {EC_REC_M, 2, CHUNK_M_MAX + 1, "m too large"},
        {EC_REC_CELL, 4, 0, "cell 0"},
        {EC_REC_OBJ_LEN, 8, 0, "length 0"},
        {EC_REC_OBJ_LEN, 8, 1u << 30, "a length the sums do not fit"},
        {EC_REC_NSUMS, 8, 1, "too few sums"},
        {EC_REC_NSUMS, 8, UINT64_MAX, "a huge count"},
        {EC_REC_MIN_BLOCK, 8, 10, "blocks out of order"},
        {EC_REC_SOURCE, 1, 0, "an empty source"},
        {EC_REC_SOURCE + 95, 1, 'x', "a source without its NUL"},
        {EC_REC_SOURCE + 60, 1, 'x', "bytes after the NUL"},
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        uint8_t *c = malloc(n);
        memcpy(c, b, n);
        for (size_t j = 0; j < bad[i].width; j++)
            c[bad[i].off + j] = (uint8_t)(bad[i].val >> (8 * j));
        int rc = ec_record_decode(c, n, &d);
        if (rc != TSDB_ERR_CORRUPT)
            fprintf(stderr, "  record with %s: %d\n", bad[i].what, rc);
        CHECK(rc == TSDB_ERR_CORRUPT && d.sums == NULL);
        ec_record_free(&d);
        free(c);
    }
    ec_record e = r;
    e.version = 2;
    CHECK(ec_record_encode(&e, b, n) == TSDB_ERR_INVALID);
    e = r;
    e.nsums--;
    CHECK(ec_record_encode(&e, b, n) == TSDB_ERR_INVALID);
    e = r;
    e.class_check = 7;
    CHECK(ec_record_encode(&e, b, n) == TSDB_ERR_INVALID);
    ec_record_free(&r);
    free(b);
    printf("  record: round trip, every truncation and every bad field refused\n");
}

/* ---------------------------------------------------------------- settle by hand */
typedef struct settled {
    char name[96];   /* the .ec part's name */
    char source[96]; /* the .part's, kept in DIR.ref */
} settled;

/* Writes an .ec file for every listed part with granules, closes the engine, then swaps each in for its source (the
 * source goes to DIR.ref, where the test opens it with part_open): what r7-swap's intent will do, by hand. */
static size_t settle_all(eng *db, const char *dir, uint32_t cls, settled *out, size_t cap)
{
    char tmp[64][96];
    size_t n = 0;
    pthread_mutex_lock(&db->lock);
    size_t np = db->nparts;
    part *ps[64];
    for (size_t i = 0; i < np && i < 64; i++) {
        ps[i] = db->parts[i];
        part_ref(ps[i]);
    }
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < np && i < 64; i++) {
        part *p = ps[i];
        if (p->ng > 0 && n < cap) {
            ec_record r;
            make_record(p, cls, &r);
            int rc = ec_index_write(db, p, &r, tmp[n]);
            CHECK(rc == 0 && ec_tmp_name(tmp[n]));
            if (rc == 0) {
                snprintf(out[n].source, sizeof out[n].source, "%s", p->name);
                snprintf(out[n].name, sizeof out[n].name, "%.*s.ec", (int)(strlen(p->name) - 5), p->name);
                n++;
            }
            ec_record_free(&r);
        }
        part_unref(p);
    }
    CHECK(eng_close(db) == 0);
    for (size_t i = 0; i < n; i++) {
        char a[PATH_MAX + 200], b[PATH_MAX + 200];
        snprintf(a, sizeof a, "%s/%.*s", dir, (int)sizeof tmp[i], tmp[i]);
        snprintf(b, sizeof b, "%s/%s", dir, out[i].name);
        CHECK(rename(a, b) == 0);
        snprintf(a, sizeof a, "%s/%s", dir, out[i].source);
        snprintf(b, sizeof b, "%s.ref/%s", dir, out[i].source);
        CHECK(rename(a, b) == 0);
    }
    return n;
}

static part *listed(eng *db, const char *name)
{
    part *p = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (strcmp(db->parts[i]->name, name) == 0)
            p = db->parts[i];
    pthread_mutex_unlock(&db->lock);
    return p;
}

/* the .ec part against its source, field by field */
static void compare(const char *dir, const part *e, const char *source)
{
    char ref[PATH_MAX + 8];
    snprintf(ref, sizeof ref, "%s.ref", dir);
    int rfd = open(ref, O_RDONLY | O_DIRECTORY);
    part *s = NULL;
    CHECK(rfd >= 0 && part_open(rfd, source, false, &s) == 0 && s);
    if (!s) {
        close(rfd);
        return;
    }
    CHECK(e->ec && !e->ec->test_mark && e->verify);
    /* pin_index: the index is parsed from the copy the open checked, never from the mapping */
    if (pin_opt)
        CHECK(e->icopy && e->ix.e >= e->icopy && e->ix.e < e->icopy + e->pinned);
    else
        CHECK(!e->icopy);
    CHECK(e->partition == s->partition && e->min_block == s->min_block && e->max_block == s->max_block &&
          e->level == s->level);
    CHECK(e->grows == s->grows && e->wal_seq == s->wal_seq && e->hdr_len == s->hdr_len);
    CHECK(e->fno_lo == s->fno_lo && e->fno_hi == s->fno_hi && e->written_ms == s->written_ms);
    CHECK(e->ng == s->ng && e->nt == s->nt && e->rows == s->rows && e->min_ts == s->min_ts && e->max_ts == s->max_ts);
    CHECK(e->index_off == s->index_off && e->index_len == s->index_len && e->index_hash == s->index_hash);
    CHECK(e->ix.nruns == s->ix.nruns);
    CHECK(e->nt == 0 || memcmp(e->t, s->t, e->nt * sizeof(tomb)) == 0);
    for (uint32_t i = 0; i < e->ng && i < s->ng; i++) {
        granule a, b;
        memset(&a, 0, sizeof a);
        memset(&b, 0, sizeof b);
        gx_load(e, i, GW_ALL, &a);
        gx_load(s, i, GW_ALL, &b);
        if (memcmp(&a, &b, sizeof a) != 0) {
            fprintf(stderr, "  %s: granule %u differs from its source's\n", e->name, i);
            failures++;
            break;
        }
        CHECK(gx_count(e, i) == gx_count(s, i) && gx_ts_min(e, i) == gx_ts_min(s, i) &&
              gx_ts_max(e, i) == gx_ts_max(s, i) && gx_series(e, i) == gx_series(s, i));
    }
    CHECK(e->ec->rec.object_len == s->map_len && e->ec->rec.object_xxh64 == xxh64(s->map, s->map_len, 0));
    part_unref(s);
    close(rfd);
}

/* Merges the two newest parts of the newest partition (a replace's tombstone part and its rows) the way
 * optimize_where does. An older part of the partition lies below them, so the output keeps the tombstones: the
 * one way to get a part with both granules and tombstones (a replace writes its tombstones in a part of their own). */
static void merge_newest(eng *db, size_t k)
{
    pthread_mutex_lock(&db->lock);
    size_t np = db->nparts;
    bool ok = k >= 2 && k <= 4 && np >= k + 1;
    for (size_t i = 2; ok && i <= k + 1; i++)
        ok = db->parts[np - i]->partition == db->parts[np - 1]->partition;
    CHECK(ok);
    if (!ok) {
        pthread_mutex_unlock(&db->lock);
        return;
    }
    part *sel[4];
    for (size_t i = 0; i < k; i++) {
        sel[i] = db->parts[np - k + i];
        sel[i]->merging = true;
        sel[i]->merge_len_gen = atomic_load(&db->ret_len_gen);
        part_ref(sel[i]);
    }
    pthread_mutex_unlock(&db->lock);
    part *out = NULL;
    int rc = merge_parts(db, sel, k, &out);
    CHECK(rc == 0 && out != NULL && out->nt > 0 && out->ng > 0);
    pthread_mutex_lock(&db->lock);
    merge_commit_locked(db, sel, k, out, rc);
    pthread_mutex_unlock(&db->lock);
    if (out)
        CHECK(manifest_publish(db) == 0);
    for (size_t i = 0; i < k; i++)
        part_unref(sel[i]);
}

static void merge_newest_two(eng *db) { merge_newest(db, 2); }

/* pin_index: damage to the mapped index after the open changes no answer and no index check (the copy is used) */
static void pinned_flip(const char *dir, part *p)
{
    granule *before = calloc(p->ng, sizeof *before);
    for (uint32_t i = 0; i < p->ng; i++)
        gx_load(p, i, GW_ALL, &before[i]);
    char path[PATH_MAX + 200];
    snprintf(path, sizeof path, "%s/%s", dir, p->name);
    int fd = open(path, O_RDWR);
    off_t at = (off_t)(p->hdr_len + p->index_len / 2);
    uint8_t x = 0, y;
    CHECK(fd >= 0 && pread(fd, &x, 1, at) == 1);
    y = x ^ 0x5a;
    CHECK(pwrite(fd, &y, 1, at) == 1);
    for (uint32_t i = 0; i < p->ng; i++) {
        granule g;
        memset(&g, 0, sizeof g);
        gx_load(p, i, GW_ALL, &g);
        CHECK(memcmp(&g, &before[i], sizeof g) == 0);
    }
    CHECK(part_index_intact(p));
    CHECK(pwrite(fd, &x, 1, at) == 1);
    close(fd);
    free(before);
}

static void test_roundtrip(const char *what, uint32_t grows, int kind)
{
    char dir[PATH_MAX];
    fresh(what, dir, sizeof dir);
    grows_opt = grows;
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = 0, b = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0 && eng_series(db, "b", TSDB_FLOAT32, &b) == 0);
    if (kind == 0) { /* one granule */
        rows(db, a, T0, 100, 0);
    } else if (kind == 1) { /* many granules, two series */
        rows(db, a, T0, 5000, 0);
        rows(db, b, T0, 3000, 100);
    } else { /* rows, then a replace: its part holds tombstones and rows */
        rows(db, a, T0, 2000, 0);
        CHECK(eng_flush(db) == 0);
        int64_t ts[50];
        float v[50];
        for (int i = 0; i < 50; i++)
            ts[i] = T0 + 100000 + i * 2000, v[i] = -1.0f - (float)i;
        CHECK(eng_replace_range(db, TSDB_FLOAT32, a, T0 + 100000, T0 + 300000, ts, v, 50) == 0);
        merge_newest_two(db);
    }
    CHECK(eng_flush(db) == 0);
    settled st[64];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 64);
    CHECK(n >= 1);
    db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    bool tombs = false;
    for (size_t i = 0; i < n; i++) {
        part *p = listed(db, st[i].name);
        CHECK(p != NULL);
        if (!p)
            continue;
        tombs |= p->nt > 0;
        compare(dir, p, st[i].source);
        CHECK(part_index_intact(p) && part_checked(db, p, "test"));
        if (pin_opt)
            pinned_flip(dir, p);
    }
    if (kind == 2)
        CHECK(tombs);
    CHECK(eng_close(db) == 0);
    grows_opt = 0;
    printf("  round trip (%s): %zu .ec parts equal their sources in every index field\n", what, n);
}

/* ---------------------------------------------------------------- damage */
static uint8_t *slurp(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    *len = (size_t)ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *b = malloc(*len + 16);
    if (fread(b, 1, *len, f) != *len) {
        free(b);
        b = NULL;
    }
    fclose(f);
    return b;
}

static void spit(const char *path, const uint8_t *b, size_t len)
{
    FILE *f = fopen(path, "wb");
    CHECK(f && fwrite(b, 1, len, f) == len);
    if (f)
        fclose(f);
}

static void test_damage(void)
{
    char dir[PATH_MAX];
    fresh("damage", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 3000, 0);
    CHECK(eng_flush(db) == 0);
    int64_t ts[10];
    float v[10];
    for (int i = 0; i < 10; i++)
        ts[i] = T0 + 5000 + i * 1000, v[i] = 9;
    CHECK(eng_replace_range(db, TSDB_FLOAT32, a, T0 + 5000, T0 + 20000, ts, v, 10) == 0);
    merge_newest_two(db);
    CHECK(eng_flush(db) == 0);
    settled st[8];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 8);
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    /* the part with tombstones, so that every region exists */
    const part *p = NULL;
    for (size_t i = 0; i < n; i++) {
        const part *q = listed(db, st[i].name);
        if (q && q->nt)
            p = q;
    }
    CHECK(p != NULL);
    if (!p) {
        eng_close(db);
        return;
    }
    char path[PATH_MAX + 200], bad[PATH_MAX + 200];
    snprintf(path, sizeof path, "%s/%s", dir, p->name);
    snprintf(bad, sizeof bad, "%s/damaged.bin", dir);
    size_t len = 0;
    uint8_t *orig = slurp(path, &len);
    CHECK(orig != NULL);
    if (!orig) {
        eng_close(db);
        return;
    }
    size_t hl = sizeof(part_header), il = (size_t)p->index_len, tl = p->nt * sizeof(tomb) + 8, fl = 64;
    size_t R = (size_t)p->ec->rec_off, rl = (size_t)p->ec->rec_len;
    CHECK(R == hl + il + tl + fl && R + rl + EC_TRAILER_BYTES == len);
    struct { size_t off; const char *what; } at[] = {
        {0, "the header's magic"},
        {16, "the header's wal_seq"},
        {24, "the header's fno_lo"},
        {hl + 2, "the index's head"},
        {hl + il / 2, "the middle of the index"},
        {hl + il - 1, "the index's last byte"},
        {hl + il + 4, "a tombstone's padding"},
        {hl + il + 10, "a tombstone's start"},
        {hl + il + tl - 3, "the tombstones' magic"},
        {R - fl + 0, "the footer's index_off"},
        {R - fl + 16, "the footer's rows"},
        {R - fl + 24, "the footer's min_ts"},
        {R - fl + 40, "the footer's ngranules"},
        {R - fl + 48, "the footer's hash"},
        {R - fl + 57, "the footer's magic"},
        {R + EC_REC_CLASS, "the record's class"},
        {R + EC_REC_OID + 3, "the record's oid"},
        {R + EC_REC_KEY, "the record's key"},
        {R + EC_REC_MAX_BLOCK, "the record's block range"},
        {R + EC_REC_LEVEL, "the record's level"},
        {R + EC_REC_OBJ_HASH + 2, "the object's hash"},
        {R + EC_REC_EPOCH, "the writer epoch"},
        {R + EC_REC_SETTLE_MS, "the settle time"},
        {R + EC_REC_SOURCE + 3, "the source's name"},
        {R + EC_REC_SUMS + 5, "a cell sum"},
        {R + rl - 1, "the last cell sum"},
        {R + rl + EC_TR_MAGIC, "the trailer's magic"},
        {R + rl + EC_TR_VERSION, "the trailer's version"},
        {R + rl + EC_TR_REC_OFF, "the record's offset"},
        {R + rl + EC_TR_REC_LEN, "the record's length"},
        {R + rl + EC_TR_REC_HASH, "the record's hash"},
        {R + rl + EC_TR_RESERVED + 1, "the trailer's reserved bytes"},
        {R + rl + EC_TR_HASH + 7, "the trailer's hash"},
    };
    part *q = NULL;
    spit(bad, orig, len);
    CHECK(part_open_ec(db, "damaged.bin", p->name, &q) == 0 && q); /* the control: a copy opens */
    if (q) {
        CHECK(part_index_intact(q));
        /* damage after the open: the index check sees it (MAP_SHARED), in the index and in the record */
        int fd = open(bad, O_WRONLY);
        uint8_t x = orig[hl + il / 2] ^ 0x10, y = orig[R + EC_REC_SUMS + 1] ^ 1;
        CHECK(fd >= 0 && pwrite(fd, &x, 1, (off_t)(hl + il / 2)) == 1);
        CHECK(!part_index_intact(q));
        CHECK(pwrite(fd, orig + hl + il / 2, 1, (off_t)(hl + il / 2)) == 1 && part_index_intact(q));
        CHECK(pwrite(fd, &y, 1, (off_t)(R + EC_REC_SUMS + 1)) == 1);
        CHECK(!part_index_intact(q));
        close(fd);
        part_unref(q);
    }
    for (size_t i = 0; i < sizeof at / sizeof *at; i++) {
        uint8_t *c = malloc(len);
        memcpy(c, orig, len);
        c[at[i].off] ^= 0x41;
        spit(bad, c, len);
        q = NULL;
        int rc = part_open_ec(db, "damaged.bin", p->name, &q);
        if (rc != TSDB_ERR_CORRUPT)
            fprintf(stderr, "  damage to %s (offset %zu): %d\n", at[i].what, at[i].off, rc);
        CHECK(rc == TSDB_ERR_CORRUPT && !q);
        if (q)
            part_unref(q);
        free(c);
    }
    /* shorter and longer */
    for (size_t cut = 1; cut <= 72; cut += 7) {
        spit(bad, orig, len - cut);
        q = NULL;
        CHECK(part_open_ec(db, "damaged.bin", p->name, &q) == TSDB_ERR_CORRUPT && !q);
    }
    uint8_t *c = calloc(1, len + 8);
    memcpy(c, orig, len);
    spit(bad, c, len + 8);
    CHECK(part_open_ec(db, "damaged.bin", p->name, &q) == TSDB_ERR_CORRUPT);
    /* the record of another name */
    spit(bad, orig, len);
    char other[96];
    snprintf(other, sizeof other, "p%lld_%llu_%llu_%u.ec", (long long)p->partition, (unsigned long long)p->min_block,
             (unsigned long long)p->max_block, p->level + 1);
    CHECK(part_open_ec(db, "damaged.bin", other, &q) == TSDB_ERR_CORRUPT);
    free(c);
    free(orig);
    unlink(bad);
    CHECK(eng_close(db) == 0);
    printf("  damage: %zu regions, truncations and an extension each fail the open; damage after the open fails "
           "the index check\n", sizeof at / sizeof *at);
}

/* ---------------------------------------------------------------- the checks behind the hashes */
/* The hashes of an edited .ec image, made right again (the footer's, the record's, the trailer's): what a settle that
 * mixed something up, or damage the hashes cannot see, would leave. part_open_ec must still refuse it. */
static void reseal(uint8_t *b, size_t len)
{
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer), TL = EC_TRAILER_BYTES;
    uint8_t *tr = b + len - TL;
    uint64_t rlen;
    memcpy(&rlen, tr + EC_TR_REC_LEN, 8);
    size_t roff = len - TL - (size_t)rlen;
    part_footer f;
    memcpy(&f, b + roff - FL, FL);
    size_t il = (size_t)f.index_len, tl = roff - FL - HL - il;
    uint64_t x = xxh64(b, HL, 0);
    x = xxh64(b + HL, il, x);
    if (tl)
        x = xxh64(b + HL + il, tl, x);
    f.index_hash = xxh64(&f, offsetof(part_footer, index_hash), x);
    memcpy(b + roff - FL, &f, FL);
    uint64_t h = xxh64(b + roff, (size_t)rlen, 0);
    memcpy(tr + EC_TR_REC_HASH, &h, 8);
    h = xxh64(tr, EC_TR_HASH, 0);
    memcpy(tr + EC_TR_HASH, &h, 8);
}

/* An .ec image of p (listed, opened from `orig`) with the packed index of g[0..n) in place of its own: the object's
 * length follows the index's, and the record's geometry and sums follow the length, so only what the index says
 * differs. Sealed. */
static uint8_t *rebuild(const uint8_t *orig, const part *p, const granule *g, uint32_t n, size_t *out_len)
{
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer), TL = EC_TRAILER_BYTES;
    size_t R = (size_t)p->ec->rec_off, il = (size_t)p->index_len, tl = R - FL - HL - il;
    uint8_t *ix = NULL;
    size_t il2 = 0;
    if (gix_encode(g, n, &ix, &il2) != 0)
        return NULL;
    part_footer f;
    memcpy(&f, orig + R - FL, FL);
    f.index_len = il2;
    f.ngranules = n;
    ec_record r;
    if (ec_record_decode(orig + R, (size_t)p->ec->rec_len, &r) != 0) {
        free(ix);
        return NULL;
    }
    chunk_geom geo;
    r.object_len = f.index_off + il2 + tl + FL;
    CHECK(chunk_geom_derive(&geo, 2, 1, r.object_len) == 0);
    r.k = geo.k, r.m = geo.m, r.cell_bytes = geo.cell;
    free(r.sums);
    r.nsums = chunk_sums_total(&geo);
    r.sums = calloc(r.nsums, 8);
    size_t rl = ec_record_size(&r), len = HL + il2 + tl + FL + rl + TL;
    uint8_t *b = calloc(1, len);
    memcpy(b, orig, HL);
    memcpy(b + HL, ix, il2);
    memcpy(b + HL + il2, orig + HL + il, tl);
    memcpy(b + HL + il2 + tl, &f, FL);
    size_t R2 = HL + il2 + tl + FL;
    CHECK(ec_record_encode(&r, b + R2, rl) == 0);
    uint8_t *t = b + R2 + rl;
    memcpy(t + EC_TR_MAGIC, EC_TRAILER_MAGIC, 8);
    uint32_t v = EC_TRAILER_VERSION, rv = r.version;
    uint64_t o = R2, l = rl;
    memcpy(t + EC_TR_VERSION, &v, 4);
    memcpy(t + EC_TR_REC_VERSION, &rv, 4);
    memcpy(t + EC_TR_REC_OFF, &o, 8);
    memcpy(t + EC_TR_REC_LEN, &l, 8);
    reseal(b, len);
    ec_record_free(&r);
    free(ix);
    *out_len = len;
    return b;
}

static size_t tmp_files(const char *dir)
{
    size_t n = 0;
    DIR *d = opendir(dir);
    struct dirent *de;
    while (d && (de = readdir(d)))
        n += strncmp(de->d_name, "tmp_", 4) == 0;
    if (d)
        closedir(d);
    return n;
}

static void set64(uint8_t *at, uint64_t v) { memcpy(at, &v, 8); }

/* Opens the image b as p's name: the return code */
static int open_image(eng *db, const char *bad, const part *p, const uint8_t *b, size_t len)
{
    spit(bad, b, len);
    part *q = NULL;
    int rc = part_open_ec(db, "damaged.bin", p->name, &q);
    CHECK((rc == 0) == (q != NULL));
    if (q)
        part_unref(q);
    return rc;
}

/* Every check of part_open_ec that a hash cannot stand in for, each on an image whose hashes are right: the record of
 * another key, block range or source; the footer's rows and time range off by one; index_off against object_len; the
 * header's log numbers; tombstones out of order; the R8 fields; granules the index places on top of each other or past
 * the object's data (a fuzz timeout once: offsets of stride 0 made any granule count look valid). */
static void test_sealed(void)
{
    char dir[PATH_MAX];
    fresh("sealed", dir, sizeof dir);
    grows_opt = 256;
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = 0, b = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0 && eng_series(db, "b", TSDB_FLOAT32, &b) == 0);
    rows(db, a, T0, 3000, 0);
    rows(db, b, T0, 3000, 50);
    CHECK(eng_flush(db) == 0);
    /* a delete of b, then a replace of a, merged: one part with granules and two tombstones, a's before b's */
    CHECK(eng_delete_range(db, b, T0 + 10000, T0 + 20000) == 0);
    int64_t ts[600];
    float v[600];
    for (int i = 0; i < 600; i++)
        ts[i] = T0 + 100000 + i * 1000, v[i] = -1.0f - (float)i;
    CHECK(eng_replace_range(db, TSDB_FLOAT32, a, T0 + 100000, T0 + 800000, ts, v, 600) == 0);
    merge_newest(db, 3);
    CHECK(eng_flush(db) == 0);
    settled st[8];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 8);
    db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    grows_opt = 0;
    if (!db)
        return;
    const part *p = NULL;
    for (size_t i = 0; i < n; i++) {
        const part *q = listed(db, st[i].name);
        if (q && q->nt == 2 && q->ng >= 2)
            p = q;
    }
    CHECK(p != NULL);
    char path[PATH_MAX + 200], bad[PATH_MAX + 200];
    size_t len = 0;
    uint8_t *orig = NULL;
    if (p) {
        snprintf(path, sizeof path, "%s/%s", dir, p->name);
        orig = slurp(path, &len);
    }
    CHECK(orig != NULL);
    if (!orig) {
        eng_close(db);
        return;
    }
    snprintf(bad, sizeof bad, "%s/damaged.bin", dir);
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer);
    size_t R = (size_t)p->ec->rec_off, il = (size_t)p->index_len, F = R - FL;
    uint8_t *c = malloc(len);
    int bad_cases = 0;
#define EDIT(what, stmt)                                                                                               \
    do {                                                                                                               \
        memcpy(c, orig, len);                                                                                          \
        stmt;                                                                                                          \
        reseal(c, len);                                                                                                \
        int rc_ = open_image(db, bad, p, c, len);                                                                      \
        if (rc_ != TSDB_ERR_CORRUPT)                                                                                   \
            fprintf(stderr, "  sealed %s: %d\n", what, rc_);                                                         \
        CHECK(rc_ == TSDB_ERR_CORRUPT);                                                                                \
        bad_cases++;                                                                                                   \
    } while (0)
#define SET64(off, val) set64(c + (off), (uint64_t)(val))
#define GET64(off) ({ uint64_t v_; memcpy(&v_, orig + (off), 8); v_; })
    memcpy(c, orig, len);
    reseal(c, len);
    CHECK(memcmp(c, orig, len) == 0); /* the reseal of an untouched image changes nothing */
    CHECK(open_image(db, bad, p, c, len) == 0);
    EDIT("record key", SET64(R + EC_REC_KEY, GET64(R + EC_REC_KEY) + 1));
    EDIT("record min_block", SET64(R + EC_REC_MIN_BLOCK, GET64(R + EC_REC_MIN_BLOCK) + 1));
    EDIT("record max_block", SET64(R + EC_REC_MAX_BLOCK, GET64(R + EC_REC_MAX_BLOCK) + 1));
    EDIT("record source", c[R + EC_REC_SOURCE] = 'q');
    EDIT("record positions", c[R + EC_REC_POSITIONS] = 1);
    EDIT("record ppos", c[R + EC_REC_PPOS] = 1);
    EDIT("record stripe_version", c[R + EC_REC_STRIPE_VERSION] = 1);
    EDIT("footer rows", SET64(F + offsetof(part_footer, rows), GET64(F + offsetof(part_footer, rows)) + 1));
    EDIT("footer min_ts", SET64(F + offsetof(part_footer, min_ts), GET64(F + offsetof(part_footer, min_ts)) - 1));
    EDIT("footer max_ts", SET64(F + offsetof(part_footer, max_ts), GET64(F + offsetof(part_footer, max_ts)) + 1));
    EDIT("footer index_off", SET64(F + offsetof(part_footer, index_off), GET64(F + offsetof(part_footer, index_off)) + 8));
    EDIT("header fno_lo > fno_hi", (SET64(offsetof(part_header, fno_lo), 7), SET64(offsetof(part_header, fno_hi), 5)));
    EDIT("header fno_lo 0 only", (SET64(offsetof(part_header, fno_lo), 0), SET64(offsetof(part_header, fno_hi), 5)));
    EDIT("header fno_hi at the bound", (SET64(offsetof(part_header, fno_lo), 1),
                                        SET64(offsetof(part_header, fno_hi), FNO_BOUND_MAX)));
    tomb t2[2];
    memcpy(t2, orig + HL + il, sizeof t2);
    CHECK(t2[0].series < t2[1].series);
    EDIT("tombstones out of order", (memcpy(c + HL + il, &t2[1], sizeof(tomb)),
                                     memcpy(c + HL + il + sizeof(tomb), &t2[0], sizeof(tomb))));
    /* b's: the time range of the part ends with a's tombstone and starts with b's, so this one changes neither */
    EDIT("an empty tombstone", SET64(HL + il + sizeof(tomb) + offsetof(tomb, end), t2[1].start));
    /* the index against the object's data */
    granule *g = calloc(p->ng, sizeof *g);
    for (uint32_t i = 0; i < p->ng; i++) {
        gx_load(p, i, GW_ALL, &g[i]);
        g[i].series = gx_series(p, i);
    }
    size_t blen = 0;
    uint8_t *img = rebuild(orig, p, g, p->ng, &blen);
    CHECK(img && open_image(db, bad, p, img, blen) == 0); /* the control: the same index, rebuilt */
    free(img);
    struct { const char *what; uint32_t i; int field; uint64_t val; } lay[] = {
        {"two granules at one offset", 1, 0, g[0].off},
        {"a granule inside the one before", 1, 0, g[0].off + 1},
        {"a granule before the data", 0, 0, HL - 8},
        {"a granule past the object's data", p->ng - 1, 1, (uint64_t)1 << 20},
        {"a granule of no bytes", p->ng - 1, 2, 0},
    };
    for (size_t k = 0; k < sizeof lay / sizeof *lay; k++) {
        granule s = g[lay[k].i];
        if (lay[k].field == 0)
            g[lay[k].i].off = lay[k].val;
        else if (lay[k].field == 1)
            g[lay[k].i].ts_len = (uint32_t)lay[k].val;
        else
            g[lay[k].i].ts_len = g[lay[k].i].val_len = 0;
        img = rebuild(orig, p, g, p->ng, &blen);
        CHECK(img != NULL);
        int rc = img ? open_image(db, bad, p, img, blen) : 0;
        if (rc != TSDB_ERR_CORRUPT)
            fprintf(stderr, "  sealed %s: %d\n", lay[k].what, rc);
        CHECK(rc == TSDB_ERR_CORRUPT);
        bad_cases++;
        free(img);
        g[lay[k].i] = s;
    }
#undef EDIT
#undef SET64
#undef GET64
    free(g);
    free(c);
    free(orig);
    unlink(bad);
    CHECK(eng_close(db) == 0);
    printf("  sealed: %d images with right hashes and a wrong identity, footer, header, tombstone, R8 field or "
           "granule layout each fail the open\n", bad_cases);
}

/* ec_index_write refuses a record of another part, and a source whose mapped index changed since its open (with
 * pin_index the copy the open checked is written, and the change does not matter) */
static void test_writer(int pin)
{
    char dir[PATH_MAX];
    fresh(pin ? "writer_pinned" : "writer", dir, sizeof dir);
    pin_opt = pin;
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    pin_opt = 0;
    if (!db)
        return;
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 500, 0);
    CHECK(eng_flush(db) == 0);
    pthread_mutex_lock(&db->lock);
    part *src = db->nparts == 1 ? db->parts[0] : NULL;
    if (src)
        part_ref(src);
    pthread_mutex_unlock(&db->lock);
    CHECK(src && (pin ? src->icopy != NULL : src->icopy == NULL));
    if (!src) {
        eng_close(db);
        return;
    }
    char tmp[96], path[PATH_MAX + 200];
    ec_record r;
    make_record(src, EC_CLS_PASSED, &r);
    CHECK(ec_index_write(db, src, &r, tmp) == 0 && tmp_files(dir) == 1); /* the control */
    snprintf(path, sizeof path, "%s/%s", dir, tmp);
    unlink(path);
    struct { const char *what; int field; } w[] = {{"key", 0}, {"max_block", 1}, {"level", 2}, {"object_len", 3},
                                                   {"source", 4}};
    for (size_t k = 0; k < sizeof w / sizeof *w; k++) {
        ec_record x;
        make_record(src, EC_CLS_PASSED, &x);
        if (w[k].field == 0) {
            x.key++;
        } else if (w[k].field == 1) {
            x.max_block++;
        } else if (w[k].field == 2) {
            x.level++;
        } else if (w[k].field == 3) { /* one byte short, the record's own geometry and sums kept consistent */
            chunk_geom geo;
            x.object_len--;
            CHECK(chunk_geom_make(&geo, x.k, x.m, x.cell_bytes, x.object_len) == 0);
            x.nsums = chunk_sums_total(&geo);
        } else {
            snprintf(x.source, sizeof x.source, "p%lld_%llu_%llu_%u.part", (long long)src->partition,
                     (unsigned long long)src->min_block, (unsigned long long)src->max_block, src->level + 1);
            memset(x.source + strlen(x.source), 0, sizeof x.source - strlen(x.source));
        }
        int rc = ec_index_write(db, src, &x, tmp);
        if (rc != TSDB_ERR_INVALID)
            fprintf(stderr, "  writer: a record of another %s: %d\n", w[k].what, rc);
        CHECK(rc == TSDB_ERR_INVALID && tmp[0] == '\0' && tmp_files(dir) == 0);
        ec_record_free(&x);
    }
    /* a byte of the source's index changes in the file (and so in its mapping) after the open */
    snprintf(path, sizeof path, "%s/%s", dir, src->name);
    int fd = open(path, O_RDWR);
    off_t at = (off_t)(src->index_off + src->index_len / 2);
    uint8_t x0 = 0, x1;
    CHECK(fd >= 0 && pread(fd, &x0, 1, at) == 1);
    x1 = x0 ^ 0x22;
    CHECK(pwrite(fd, &x1, 1, at) == 1);
    if (pin) {
        CHECK(ec_index_write(db, src, &r, tmp) == 0 && tmp_files(dir) == 1);
        snprintf(path, sizeof path, "%s/%s", dir, tmp);
        unlink(path);
    } else {
        fprintf(stderr, "  (a refusal is logged next: a source whose index changed)\n");
        CHECK(ec_index_write(db, src, &r, tmp) == TSDB_ERR_CORRUPT && tmp[0] == '\0' && tmp_files(dir) == 0);
    }
    CHECK(pwrite(fd, &x0, 1, at) == 1);
    close(fd);
    ec_record_free(&r);
    part_unref(src);
    CHECK(eng_close(db) == 0);
    printf("  writer%s: a record of another key, block range, level, length or source is refused, and %s\n",
           pin ? " (pin_index)" : "", pin ? "the checked copy is written after the mapping changed"
                                         : "a source whose mapped index changed is not written");
}

/* R8: a cluster ec volume (requires=cluster,ec, as r8f's attach writes it) writes and opens a record with ppos and
 * stripe_version set, through the engine's own flag (db.c -> ec_index_write, part_open_ec); the same bytes are not this
 * volume's once the flag is off (a single server's tier keeps both 0). */
static void test_cluster_record(void)
{
    char dir[PATH_MAX];
    fresh("cluster_ec", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (db)
        CHECK(eng_close(db) == 0);
    int fd = open(dir, O_RDONLY | O_DIRECTORY);
    CHECK(fd >= 0 && meta_require(fd, true, META_REQ_CLUSTER | META_REQ_EC) == 0);
    if (fd >= 0)
        close(fd);
    { /* a cluster volume's META names its metadata uuid (cvolfs writes volume_uuid=): a tier takes the
       * volume's vuuid from it (r8f) */
        char mp[PATH_MAX + 16];
        FILE *mf = snprintf(mp, sizeof mp, "%s/META", dir) < (int)sizeof mp ? fopen(mp, "a") : NULL;
        if (!mf || fputs("volume_uuid=5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a\n", mf) < 0 || fclose(mf) != 0)
            exit(2);
    }
    meta_cluster_open_allow(true);
    db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    meta_cluster_open_allow(false);
    if (!db)
        return;
    CHECK(db->ec_cluster);
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 500, 0);
    CHECK(eng_flush(db) == 0);
    pthread_mutex_lock(&db->lock);
    part *src = db->nparts == 1 ? db->parts[0] : NULL;
    if (src)
        part_ref(src);
    pthread_mutex_unlock(&db->lock);
    CHECK(src != NULL);
    if (src) {
        char tmp[96], ecname[160];
        snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(strlen(src->name) - 5), src->name);
        ec_record r;
        make_record(src, EC_CLS_PASSED, &r);
        r.ppos = 4242, r.stripe_version = 3;
        int rc = ec_index_write(db, src, &r, tmp);
        if (rc != 0)
            fprintf(stderr, "  cluster ec: a record with ppos was not written (%d)\n", rc);
        CHECK(rc == 0);
        if (rc == 0) {
            part *q = NULL;
            rc = part_open_ec(db, tmp, ecname, &q);
            if (rc != 0)
                fprintf(stderr, "  cluster ec: a record with ppos did not open (%d)\n", rc);
            CHECK(rc == 0 && q && q->ec);
            if (q)
                part_unref(q);
            db->ec_cluster = false; /* as a volume without the word: the same file is not its own */
            q = NULL;
            CHECK(part_open_ec(db, tmp, ecname, &q) == TSDB_ERR_CORRUPT && !q);
            if (q)
                part_unref(q);
            char t2[96];
            CHECK(ec_index_write(db, src, &r, t2) == TSDB_ERR_INVALID && t2[0] == '\0');
            db->ec_cluster = true;
            char path[PATH_MAX + 200];
            snprintf(path, sizeof path, "%s/%s", dir, tmp);
            unlink(path);
        }
        ec_record_free(&r);
        part_unref(src);
    }
    CHECK(eng_close(db) == 0);
    printf("  cluster ec: ppos and stripe_version written and opened under requires=cluster,ec, refused without\n");
}

/* ---------------------------------------------------------------- listing */
static void test_listing(void)
{
    char dir[PATH_MAX];
    fresh("listing", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 500, 0);
    CHECK(eng_flush(db) == 0);
    settled st[4];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 4);
    CHECK(n == 1);
    /* both names: the open fails, naming both */
    sh("cp '%s.ref/%s' '%s/'", dir, st[0].source, dir);
    fprintf(stderr, "  (two refusals are logged next: a .part beside its .ec)\n");
    db = NULL;
    CHECK(open_eng(dir, &db) == TSDB_ERR_CORRUPT && !db);
    /* a reader's open has recovered no intent, and may see a swap between its two renames: it lists one of the two
     * (they hold the same rows), never fails and never neither. With a MANIFEST it lists what that names (here the
     * .part: the swap ran with the writer closed); without one it lists the directory (load_parts) */
    for (int man = 1; man >= 0; man--) {
        if (!man)
            sh("mv '%s/MANIFEST' '%s/MANIFEST.keep'", dir, dir);
        ro_opt = 1;
        CHECK(open_eng(dir, &db) == 0 && db);
        ro_opt = 0;
        if (db) {
            CHECK((listed(db, st[0].name) != NULL) + (listed(db, st[0].source) != NULL) == 1);
            CHECK(eng_close(db) == 0);
            db = NULL;
        }
        if (!man)
            sh("mv '%s/MANIFEST.keep' '%s/MANIFEST'", dir, dir);
    }
    sh("rm -f '%s/%s'", dir, st[0].source);
    /* no requires=tier */
    atomic_store(&ec_test_assume_tier, false);
    fprintf(stderr, "  (two refusals are logged next: an .ec file without requires=tier)\n");
    CHECK(open_eng(dir, &db) == TSDB_ERR_CORRUPT && !db);
    atomic_store(&ec_test_assume_tier, true);
    CHECK(open_eng(dir, &db) == 0 && db);
    if (db) {
        CHECK(listed(db, st[0].name) != NULL);
        /* META's requires= read for real (the engine refuses "tier" before load_parts on this branch, so the load is
         * called alone): named among others, absent, and META unreadable (an I/O error, not damage) */
        atomic_store(&ec_test_assume_tier, false);
        char meta[PATH_MAX + 16], keep[PATH_MAX + 16];
        snprintf(meta, sizeof meta, "%s/META", dir);
        snprintf(keep, sizeof keep, "%s/META.keep", dir);
        sh("cp '%s' '%s' && printf 'requires=feed,tier\n' >> '%s'", meta, keep, meta);
        part *q = NULL;
        CHECK(ecpart_load(db, st[0].name, false, &q) == 0 && q);
        if (q)
            part_unref(q);
        sh("cp '%s' '%s' && printf 'requires=tiers,feed\n' >> '%s'", keep, meta, meta);
        fprintf(stderr, "  (two refusals are logged next: no word tier in requires=, and META unreadable)\n");
        q = NULL;
        CHECK(ecpart_load(db, st[0].name, false, &q) == TSDB_ERR_CORRUPT && !q);
        sh("rm -f '%s' && mkdir '%s'", meta, meta);
        CHECK(ecpart_load(db, st[0].name, false, &q) == TSDB_ERR_IO && !q);
        sh("rmdir '%s' && mv '%s' '%s'", meta, keep, meta);
        atomic_store(&ec_test_assume_tier, true);
        CHECK(eng_close(db) == 0);
    }

    /* a record whose class check failed */
    char dir2[PATH_MAX];
    fresh("listing_cls", dir2, sizeof dir2);
    db = NULL;
    CHECK(open_eng(dir2, &db) == 0);
    if (!db)
        return;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 500, 0);
    CHECK(eng_flush(db) == 0);
    CHECK(settle_all(db, dir2, EC_CLS_FAILED, st, 4) == 1);
    db = NULL;
    fprintf(stderr, "  (a refusal is logged next: a failed class check)\n");
    CHECK(open_eng(dir2, &db) == TSDB_ERR_CORRUPT && !db);

    /* a record whose settle ran no class check, with partition classes on: its rows cannot be placed */
    char dir3[PATH_MAX];
    fresh("listing_nocls", dir3, sizeof dir3);
    db = NULL;
    CHECK(open_eng(dir3, &db) == 0);
    if (!db)
        return;
    CHECK(db->cls_on);
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 500, 0);
    CHECK(eng_flush(db) == 0);
    CHECK(settle_all(db, dir3, EC_CLS_NONE, st, 4) == 1);
    db = NULL;
    fprintf(stderr, "  (a refusal is logged next: no class check)\n");
    CHECK(open_eng(dir3, &db) == TSDB_ERR_CORRUPT && !db);
    printf("  listing: an .ec part is listed; beside its .part, without requires=tier or with a failed class check "
           "(or none, with classes on) the open fails; a reader beside a swap lists one of the two\n");
}

/* ---------------------------------------------------------------- the WAL and FNO */
static void test_replay(void)
{
    char dir[PATH_MAX], seg[64];
    fresh("replay", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 300, 0); /* synced at return (wal_sync_ms 0) */
    pthread_mutex_lock(&db->buf_lock);
    uint64_t seq = db->wal_seq;
    pthread_mutex_unlock(&db->buf_lock);
    wal_name(seg, sizeof seg, seq);
    CHECK(sh("cp '%s/%s' '%s.ref/%s'", dir, seg, dir, seg) == 0); /* what a crash before its unlink would leave */
    CHECK(eng_flush(db) == 0);
    settled st[4];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 4);
    CHECK(n == 1);
    CHECK(sh("cp '%s.ref/%s' '%s/%s'", dir, seg, dir, seg) == 0);
    db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    part *p = listed(db, st[0].name);
    CHECK(p && p->wal_seq >= seq);
    pthread_mutex_lock(&db->buf_lock);
    size_t replayed = db->active ? db->active->rows : 0;
    pthread_mutex_unlock(&db->buf_lock);
    if (replayed)
        fprintf(stderr, "  %zu rows of segment %s were replayed into a partition its .ec part holds\n", replayed, seg);
    CHECK(replayed == 0);
    CHECK(eng_close(db) == 0);
    printf("  replay: a segment whose rows an .ec part holds is not replayed\n");
}

static void test_fno(void)
{
    char dir[PATH_MAX];
    fresh("fno", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    rows(db, a, T0, 400, 0);
    CHECK(eng_flush(db) == 0);
    settled st[4];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 4);
    CHECK(n == 1);
    /* FNO lowered (both slots valid, both 1): the parts decide the next number */
    uint8_t pages[8192];
    fno_slot_page(pages, 1, 1);
    fno_slot_page(pages + 4096, 1, 2);
    char fno[PATH_MAX + 8];
    snprintf(fno, sizeof fno, "%s/" FNO_FILE, dir);
    int fd = open(fno, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, pages, sizeof pages, 0) == (ssize_t)sizeof pages);
    close(fd);
    db = NULL;
    CHECK(open_eng(dir, &db) == 0);
    if (!db)
        return;
    part *p = listed(db, st[0].name);
    CHECK(p != NULL);
    char ref[PATH_MAX + 8];
    snprintf(ref, sizeof ref, "%s.ref", dir);
    int rfd = open(ref, O_RDONLY | O_DIRECTORY);
    part *s = NULL;
    CHECK(rfd >= 0 && part_open(rfd, st[0].source, false, &s) == 0 && s && s->fno_hi > 0);
    if (s) {
        uint64_t next = fno_of(db);
        if (next <= s->fno_hi)
            fprintf(stderr, "  fno_next %llu after a lowered FNO: the .ec part holds numbers up to %llu\n",
                    (unsigned long long)next, (unsigned long long)s->fno_hi);
        CHECK(next > s->fno_hi);
        part_unref(s);
    }
    close(rfd);
    CHECK(eng_close(db) == 0);
    printf("  fno: the next number counts an .ec part's range\n");
}

void tsdb_test_before_parts(void (*fn)(void *), void *arg);

static void append_requires_tier_cb(void *arg)
{
    const char *meta = arg;
    FILE *f = fopen(meta, "a");
    if (f) {
        fputs("requires=tier\n", f);
        fclose(f);
    }
}

/* P-37 regression test: opening a directory with many .ec parts exercises load_parts's meta_tier_cache batching
 * (positive cache hit == 3 across all parts, reset to 0 post-open) while preserving exact refusal semantics when
 * META is missing or lacks requires=tier. */
static void test_many_and_refusals(void)
{
    char dir[PATH_MAX];
    fresh("many64", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, &db) == 0 && db);
    if (!db)
        return;
    uint32_t a = 0;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0);
    const size_t NPARTS = 64;
    for (size_t i = 0; i < NPARTS; i++) {
        rows(db, a, T0 + (int64_t)i * 86400000LL, 20, (float)i);
        CHECK(eng_flush(db) == 0);
    }
    settled st[72];
    size_t n = settle_all(db, dir, EC_CLS_PASSED, st, 72);
    CHECK(n == NPARTS);
    db = NULL;

    /* 1. Reopen with ec_test_assume_tier=false and requires=tier injected just before load_parts() (so
     * tier_engine_attach leaves db->tier_req=false and load_parts exercises meta_tier_cache 1 -> 3 across all 64
     * .ec parts, then resets meta_tier_cache to 0). */
    char meta[PATH_MAX + 16], keep[PATH_MAX + 16];
    snprintf(meta, sizeof meta, "%s/META", dir);
    snprintf(keep, sizeof keep, "%s/META.keep", dir);
    sh("cp '%s' '%s'", meta, keep);
    atomic_store(&ec_test_assume_tier, false);
    tsdb_test_before_parts(append_requires_tier_cb, meta);
    CHECK(open_eng(dir, &db) == 0 && db);
    tsdb_test_before_parts(NULL, NULL);
    if (db) {
        CHECK(!db->tier_req);
        CHECK(db->meta_tier_cache == 0);
        CHECK(db->nparts == NPARTS);
        for (size_t i = 0; i < NPARTS; i++)
            CHECK(listed(db, st[i].name) != NULL);
        /* Directly verify meta_tier_cache == 3 serves positive hits without re-reading META, and 0 reads fresh. */
        part *p = NULL;
        db->meta_tier_cache = 1;
        CHECK(ecpart_load(db, st[0].name, true, &p) == 0 && p);
        CHECK(db->meta_tier_cache == 3);
        if (p)
            part_unref(p);
        sh("cp '%s' '%s'", keep, meta);
        p = NULL;
        CHECK(ecpart_load(db, st[1].name, true, &p) == 0 && p);
        if (p)
            part_unref(p);
        db->meta_tier_cache = 0;
        p = NULL;
        CHECK(ecpart_load(db, st[1].name, true, &p) == TSDB_ERR_CORRUPT && !p);
        CHECK(eng_close(db) == 0);
        db = NULL;
    } else {
        sh("cp '%s' '%s'", keep, meta);
    }

    /* 2. Refusal without requires=tier across all 64 .ec parts */
    CHECK(open_eng(dir, &db) == TSDB_ERR_CORRUPT && !db);

    /* 3. Refusal when META is missing */
    sh("rm -f '%s'", meta);
    CHECK(open_eng(dir, &db) != 0 && !db);

    /* Restore META and assume_tier for any subsequent tests */
    sh("mv '%s' '%s'", keep, meta);
    atomic_store(&ec_test_assume_tier, true);
    printf("  many_and_refusals: 64 .ec parts hit meta_tier_cache==3 and reset to 0; missing/untiered META refused\n");
}

int main(int argc, char **argv)
{
    int ai = 1;
    if (ai + 1 < argc && strcmp(argv[ai], "--mutant") == 0) {
        const char *m = argv[ai + 1];
        ecpart_mutant = !strcmp(m, "ecnohash")     ? EC_MUT_NOHASH
                        : !strcmp(m, "ecnowalseq") ? EC_MUT_NOWALSEQ
                        : !strcmp(m, "ecskipname") ? EC_MUT_SKIPNAME
                        : !strcmp(m, "ecnofno")    ? EC_MUT_NOFNO
                        : !strcmp(m, "ecnolayout") ? EC_MUT_NOLAYOUT
                                                   : -1;
        if (ecpart_mutant < 0) {
            fprintf(stderr, "unknown mutant %s\n", m);
            return 2;
        }
        ai += 2;
    }
    snprintf(S, sizeof S, "%s", ai < argc ? argv[ai] : "/tmp");
#ifndef TSDB_EC
    printf("test_ecpart: skipped (built without TSDB_EC: an .ec part needs the erasure codec)\n");
    return 0;
#endif
    atomic_store(&ec_test_assume_tier, true);
    printf("test_ecpart:\n");
    test_record();
    test_roundtrip("one", 0, 0);
    test_roundtrip("many", 256, 1);
    test_roundtrip("tombs", 256, 2);
    pin_opt = 1; /* the same with pin_index: the open and the writer use the checked copy */
    test_roundtrip("one-pinned", 0, 0);
    test_roundtrip("many-pinned", 256, 1);
    test_roundtrip("tombs-pinned", 256, 2);
    pin_opt = 0;
    test_damage();
    test_sealed();
    test_writer(0);
    test_writer(1);
    test_cluster_record();
    test_listing();
    test_many_and_refusals();
    test_replay();
    test_fno();
    if (failures) {
        fprintf(stderr, "test_ecpart: %d failures\n", failures);
        return 1;
    }
    sh("rm -rf '%s'/test_ecpart_%d_*", S, (int)getpid());
    printf("test_ecpart: ok\n");
    return 0;
}
