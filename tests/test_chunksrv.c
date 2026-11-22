/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The chunk server (server/chunksrv.h) through a fake link (chunksrv_handle and chunksrv_put_* called directly, the
 * answers captured), and through two real peerlinks in one process (`net`):
 *  - wire: every request and answer round-trips; bounds and malformed frames are refused (-1: the link is ended);
 *  - a put, then every byte read back in random ranges, the sums and the listing;
 *  - a cell whose sum is wrong is refused and leaves nothing;
 *  - fences: a put and a delete below the volume's epoch are refused (FENCED), a request raises it durably;
 *  - an unsure store (one EPOCH slot damaged) refuses puts and deletes until the epoch source, and only a caught-up
 *    one, raises it; a request's own epoch never does;
 *  - store binding: a store of another cluster or node, or of none, is refused with a line naming both;
 *  - power loss (crashfs) at every step of a put: once PUT_OK went out the chunk is there and whole, before it the
 *    chunk is absent or whole;
 *  - net: puts and reads between two chunk servers over real links, several threads at once.
 * Mutants (--mutant NAME; the Makefile requires each to fail it): chunknofence, chunkselfraise, putokearly,
 * nocellcheck, storeforeign.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#ifndef TSDB_EC
#include <stdio.h>
int main(void)
{
    printf("test_chunksrv: SKIPPED (this build has no erasure codec: it is x86-64 only)\n");
    return 0;
}
#else

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "chunksrv.h"
#include "peerlink.h"

static int fails;
static bool quick;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];
static const uint8_t CLUSTER[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
static const uint8_t VUUID[16] = {0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 9, 9, 9, 9, 9, 9, 9, 9};

#define CHECK(c)                                                                                                      \
    do {                                                                                                              \
        if (!(c)) {                                                                                                   \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                              \
            fails++;                                                                                                  \
        }                                                                                                             \
    } while (0)

/* Per thread: test_net's threads draw rids too (chunksrv_call assigns its own, so equal seeds are harmless). */
static _Thread_local uint64_t rng = 88172645463325252ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13, rng ^= rng >> 7, rng ^= rng << 17;
    return rng;
}

static void fresh(const char *name, char *out, size_t cap)
{
    static int seq;
    if ((size_t)snprintf(out, cap, "%s/%s-%d", S, name, seq++) >= cap)
        abort();
    mkdir(out, 0755);
}

/* a store at dir/st<i> of CLUSTER, node `node` (owner false: made without one) */
static void mkstore(const char *dir, int i, uint32_t node, bool owner, char *path, size_t cap)
{
    snprintf(path, cap, "%s/st%d", dir, i);
    chunk_store_owner o = {.set = owner, .node = node};
    memcpy(o.cluster, CLUSTER, 16);
    CHECK(chunk_store_init2(path, NULL, owner ? &o : NULL, NULL) == 0);
}

/* ---------------------------------------------------------------- a chunk to put */
typedef struct cdata {
    struct cs_put_meta m;
    uint8_t *cells;
    uint64_t *sums;
    uint64_t ncells, len;
} cdata;

static void cdata_make(cdata *c, uint64_t epoch, unsigned pos, uint32_t cell, uint64_t objlen, uint64_t seed)
{
    memset(c, 0, sizeof *c);
    c->m.volume = 1;
    c->m.epoch = epoch;
    c->m.pos = (uint16_t)pos;
    memcpy(c->m.vuuid, VUUID, 16);
    for (int i = 0; i < 16; i++)
        c->m.oid[i] = (uint8_t)(seed * 31 + (uint64_t)i);
    CHECK(chunk_geom_make(&c->m.g, 3, 2, cell, objlen) == 0);
    c->ncells = chunk_pos_cells(&c->m.g, pos);
    c->len = chunk_pos_len(&c->m.g, pos);
    c->cells = malloc(c->len ? c->len : 1);
    c->sums = malloc((c->ncells ? c->ncells : 1) * 8);
    uint64_t r = seed * 2654435761u + 1;
    for (uint64_t i = 0; i < c->len; i++) {
        r ^= r << 13, r ^= r >> 7, r ^= r << 17;
        c->cells[i] = (uint8_t)r;
    }
    uint64_t off = 0;
    for (uint64_t i = 0; i < c->ncells; i++) {
        uint64_t l = chunk_pos_cell_len(&c->m.g, pos, i);
        c->sums[i] = xxh64(c->cells + off, l, 0);
        off += l;
    }
}

static void cdata_free(cdata *c)
{
    free(c->cells);
    free(c->sums);
}

/* the bytes of the put file: sums, then cells */
static uint8_t *cdata_file(const cdata *c, size_t *n)
{
    *n = 8 * c->ncells + c->len;
    uint8_t *b = malloc(*n);
    for (uint64_t i = 0; i < c->ncells; i++)
        for (int j = 0; j < 8; j++)
            b[8 * i + (uint64_t)j] = (uint8_t)(c->sums[i] >> (8 * j));
    memcpy(b + 8 * c->ncells, c->cells, c->len);
    return b;
}

/* ---------------------------------------------------------------- the fake link */
typedef struct cap {
    int n;
    int32_t rc;
    uint8_t op;
    uint64_t rid;
    uint8_t *body;
    size_t body_len;
    const char *ackf; /* the crashfs child: PUT_OK appended here */
} cap;

static void cap_reply(void *arg, const uint8_t *p, size_t n)
{
    cap *c = arg;
    struct cs_resp r;
    CHECK(cs_resp_decode(p, n, &r) == 0);
    c->n++;
    c->rc = r.rc, c->op = r.op, c->rid = r.rid;
    free(c->body);
    c->body = malloc(r.body_len + 1);
    memcpy(c->body, r.body, r.body_len);
    c->body_len = r.body_len;
    if (c->ackf && r.op == CS_OP_PUT && r.rc == 0) {
        FILE *f = fopen(c->ackf, "a");
        if (f) {
            fputs("OK\n", f);
            fclose(f);
        }
    }
}

static uint64_t le64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}

/* a whole put through the fake link, the bytes in random pieces: the answer's rc (and *sum_ok) */
static int fake_put(struct chunksrv *cs, const cdata *c, uint64_t file, bool *sum_ok, const char *ackf)
{
    size_t n;
    uint8_t *f = cdata_file(c, &n);
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(&c->m, meta);
    cap cp = {.ackf = ackf};
    struct cs_put *p = NULL;
    int rc = chunksrv_put_begin(cs, 2, file, meta, sizeof meta, n, cap_reply, &cp, &p);
    if (rc == 0) {
        size_t off = 0;
        while (off < n && rc == 0) {
            size_t k = 1 + rnd() % 70000;
            if (k > n - off)
                k = n - off;
            rc = chunksrv_put_data(p, off, f + off, k);
            off += k;
        }
        chunksrv_put_end(p, 0);
        rc = cp.n == 1 ? cp.rc : -999;
    } else {
        CHECK(cp.n == 1 && cp.rc == rc && cp.op == CS_OP_PUT && cp.rid == file);
    }
    if (sum_ok)
        *sum_ok = rc == 0 && cp.body_len == 8 && le64(cp.body) == cs_sums_hash(c->sums, c->ncells);
    free(cp.body);
    free(f);
    return rc;
}

static int fake_req(struct chunksrv *cs, struct cs_req *r, cap *cp)
{
    uint8_t buf[128];
    size_t n = cs_req_encode(r, buf, sizeof buf);
    CHECK(n > 0);
    cp->n = 0;
    int rc = chunksrv_handle(cs, 2, buf, n, cap_reply, cp);
    CHECK(rc == 0 && cp->n == 1 && cp->rid == r->rid && cp->op == r->op);
    return cp->rc;
}

static void req_of(struct cs_req *r, uint8_t op, const cdata *c, uint64_t epoch)
{
    memset(r, 0, sizeof *r);
    r->rid = rnd();
    r->op = op;
    r->volume = c->m.volume;
    r->epoch = epoch;
    memcpy(r->vuuid, c->m.vuuid, 16);
    memcpy(r->oid, c->m.oid, 16);
    r->pos = c->m.pos;
}

static struct chunksrv *open1(const char *path)
{
    char err[512] = "";
    const char *pp[1] = {path};
    struct chunksrv *cs = chunksrv_open(pp, 1, err, sizeof err);
    if (!cs)
        fprintf(stderr, "chunksrv_open: %s\n", err);
    CHECK(cs != NULL);
    return cs;
}

/* ---------------------------------------------------------------- wire */
static void test_wire(void)
{
    uint8_t buf[128];
    for (uint8_t op = CS_OP_READ; op <= CS_OP_EPOCH; op++) {
        struct cs_req r = {.rid = 77, .op = op, .volume = 3, .epoch = 9, .pos = 4, .off = 100, .len = 10,
                           .has_cursor = op == CS_OP_LIST};
        memset(r.vuuid, 5, 16);
        memset(r.oid, 6, 16);
        size_t n = cs_req_encode(&r, buf, sizeof buf);
        CHECK(n > 0);
        struct cs_req d;
        CHECK(cs_req_decode(buf, n, &d) == 0 && d.rid == 77 && d.op == op && d.volume == 3 && d.epoch == 9);
        for (size_t k = 0; k < n; k++) /* every shorter frame, and one longer */
            CHECK(cs_req_decode(buf, k, &d) == -1);
        buf[n] = 0;
        CHECK(cs_req_decode(buf, n + 1, &d) == -1);
    }
    struct cs_req r = {.op = CS_OP_READ, .len = CS_READ_MAX + 1}, d;
    size_t n = cs_req_encode(&r, buf, sizeof buf);
    CHECK(cs_req_decode(buf, n, &d) == -1); /* a read longer than the bound */
    r.len = 0;
    n = cs_req_encode(&r, buf, sizeof buf);
    CHECK(cs_req_decode(buf, n, &d) == -1);
    r = (struct cs_req){.op = CS_OP_SUMS, .len = CS_SUMS_MAX + 1};
    n = cs_req_encode(&r, buf, sizeof buf);
    CHECK(cs_req_decode(buf, n, &d) == -1);
    r = (struct cs_req){.op = CS_OP_LIST, .len = CS_LIST_MAX + 1};
    n = cs_req_encode(&r, buf, sizeof buf);
    CHECK(cs_req_decode(buf, n, &d) == -1);
    r = (struct cs_req){.op = CS_OP_DEL, .pos = CHUNK_POS_MAX};
    n = cs_req_encode(&r, buf, sizeof buf);
    CHECK(cs_req_decode(buf, n, &d) == -1);
    buf[8] = 99; /* an unknown op */
    CHECK(cs_req_decode(buf, n, &d) == -1);
    /* answers: a body of the wrong length for its op */
    uint8_t a[64] = {0};
    a[8] = CS_OP_PUT;
    struct cs_resp rr;
    CHECK(cs_resp_decode(a, 13 + 8, &rr) == 0 && cs_resp_decode(a, 13 + 7, &rr) == -1);
    a[8] = CS_OP_EPOCH;
    CHECK(cs_resp_decode(a, 13 + 9, &rr) == 0 && cs_resp_decode(a, 13 + 10, &rr) == -1);
    a[9] = 0xff, a[10] = 0xff, a[11] = 0xff, a[12] = 0xff; /* rc -1 with a body */
    CHECK(cs_resp_decode(a, 13 + 9, &rr) == -1 && cs_resp_decode(a, 13, &rr) == 0);
    a[12] = 0x7f; /* a positive rc */
    CHECK(cs_resp_decode(a, 13, &rr) == -1);
    /* put meta */
    cdata c;
    cdata_make(&c, 5, 4, 4096, 100000, 1);
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(&c.m, meta);
    struct cs_put_meta m;
    CHECK(cs_put_meta_decode(meta, sizeof meta, &m) == 0 && m.pos == 4 && m.epoch == 5 && m.g.len == 100000);
    CHECK(cs_put_meta_decode(meta, sizeof meta - 1, &m) == -1);
    meta[48] = 5; /* position 5 of 3+2 */
    CHECK(cs_put_meta_decode(meta, sizeof meta, &m) == -1);
    cdata_free(&c);
    CHECK(chunksrv_st(0) == CHUNK_ST_OK && chunksrv_st(TSDB_ERR_NOTFOUND) == CHUNK_ST_ABSENT &&
          chunksrv_st(TSDB_ERR_CORRUPT) == CHUNK_ST_DAMAGED && chunksrv_st(TSDB_ERR_IO) == CHUNK_ST_IOERR &&
          chunksrv_st(TSDB_ERR_UNAVAILABLE) == CHUNK_ST_DOWN);
}

/* ---------------------------------------------------------------- put, read, list */
static void test_put_read(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16];
    fresh("putread", dir, sizeof dir);
    mkstore(dir, 0, 7, true, st, sizeof st);
    struct chunksrv *cs = open1(st);
    if (!cs)
        return;
    const uint32_t cells[] = {1, 4096, 65536, 100000};
    for (int t = 0; t < 4; t++) {
        cdata c;
        cdata_make(&c, 3, (unsigned)t % 5, cells[t], 3 * cells[t] * 4 + 777, 10 + (uint64_t)t);
        bool ok = false;
        CHECK(fake_put(cs, &c, 100 + (uint64_t)t, &ok, NULL) == 0 && ok);
        cap cp = {0};
        struct cs_req r;
        /* every byte, in random ranges */
        uint8_t *back = calloc(1, c.len + 1);
        for (uint64_t off = 0; off < c.len;) {
            uint64_t k = 1 + rnd() % 300000;
            if (k > c.len - off)
                k = c.len - off;
            req_of(&r, CS_OP_READ, &c, 3);
            r.off = off, r.len = (uint32_t)k;
            CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len == k);
            if (cp.body_len == k)
                memcpy(back + off, cp.body, k);
            off += k;
        }
        CHECK(memcmp(back, c.cells, c.len) == 0);
        free(back);
        req_of(&r, CS_OP_READ, &c, 3); /* past the end */
        r.off = c.len, r.len = 1;
        CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_INVALID);
        req_of(&r, CS_OP_SUMS, &c, 3);
        r.off = 0, r.len = CS_SUMS_MAX;
        CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len == 36 + 8 * c.ncells);
        if (cp.body_len == 36 + 8 * c.ncells) {
            CHECK(le64(cp.body + 24) == c.ncells && le64(cp.body) == 3);
            for (uint64_t i = 0; i < c.ncells; i++)
                CHECK(le64(cp.body + 36 + 8 * i) == c.sums[i]);
        }
        cdata o = c;
        o.m.oid[0] ^= 0xff; /* another object: absent */
        req_of(&r, CS_OP_READ, &o, 3);
        r.off = 0, r.len = 1;
        CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND && chunksrv_st(cp.rc) == CHUNK_ST_ABSENT);
        free(cp.body);
        cdata_free(&c);
    }
    /* the listing, two entries a page, with a cursor */
    cap cp = {0};
    struct cs_req r = {.rid = 1, .op = CS_OP_LIST, .volume = 1, .epoch = 3, .len = 2};
    int seen = 0;
    for (int page = 0; page < 10; page++) {
        CHECK(fake_req(cs, &r, &cp) == 0);
        if (cp.rc != 0 || cp.body_len < 5)
            break;
        uint32_t k = (uint32_t)(cp.body[0] | cp.body[1] << 8);
        seen += (int)k;
        if (k) {
            const uint8_t *e = cp.body + 5 + CS_LIST_ENTRY * (k - 1);
            memcpy(r.oid, e, 16);
            r.pos = (uint16_t)(e[16] | e[17] << 8);
            r.has_cursor = true;
            CHECK(e[50] == CHUNK_ST_OK && memcmp(e + 34, VUUID, 16) == 0);
        }
        if (!cp.body[4])
            break;
    }
    CHECK(seen == 4);
    free(cp.body);
    /* malformed frames are refused without an answer */
    uint8_t junk[40];
    for (size_t n = 0; n < sizeof junk; n++) {
        memset(junk, 0xee, sizeof junk);
        cap c2 = {0};
        CHECK(chunksrv_handle(cs, 2, junk, n, cap_reply, &c2) == -1 && c2.n == 0);
    }
    struct chunksrv_stats sst;
    chunksrv_get_stats(cs, &sst);
    CHECK(sst.puts_ok == 4 && sst.malformed == sizeof junk);
    chunksrv_close(cs);
}

/* a cell that fails its sum, a short file and a file of the wrong length leave nothing */
static void test_bad_cell(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16];
    fresh("badcell", dir, sizeof dir);
    mkstore(dir, 0, 7, true, st, sizeof st);
    struct chunksrv *cs = open1(st);
    if (!cs)
        return;
    cdata c;
    cdata_make(&c, 3, 1, 4096, 4096 * 3 * 6 + 5, 3);
    c.sums[2] ^= 1; /* the third cell's sum is wrong */
    int rc = fake_put(cs, &c, 1, NULL, NULL);
    CHECK(rc == TSDB_ERR_CORRUPT);
    cap cp = {0};
    struct cs_req r;
    req_of(&r, CS_OP_READ, &c, 3);
    r.len = 1;
    CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND);
    c.sums[2] ^= 1;
    c.cells[c.len - 1] ^= 0x40; /* the last cell's byte */
    CHECK(fake_put(cs, &c, 2, NULL, NULL) == TSDB_ERR_CORRUPT);
    CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND);
    c.cells[c.len - 1] ^= 0x40;
    /* a total that is not the geometry's */
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(&c.m, meta);
    struct cs_put *p = NULL;
    cap c2 = {0};
    CHECK(chunksrv_put_begin(cs, 2, 3, meta, sizeof meta, cs_put_total(&c.m) + 1, cap_reply, &c2, &p) ==
          TSDB_ERR_INVALID && c2.n == 1 && c2.rc == TSDB_ERR_INVALID);
    /* a file that ends early: nothing is named */
    CHECK(chunksrv_put_begin(cs, 2, 4, meta, sizeof meta, cs_put_total(&c.m), cap_reply, &c2, &p) == 0);
    size_t n;
    uint8_t *f = cdata_file(&c, &n);
    CHECK(chunksrv_put_data(p, 0, f, n / 2) == 0);
    chunksrv_put_end(p, 0);
    CHECK(c2.rc == TSDB_ERR_INVALID);
    CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND);
    /* and one whose link was lost: dropped */
    CHECK(chunksrv_put_begin(cs, 2, 5, meta, sizeof meta, cs_put_total(&c.m), cap_reply, &c2, &p) == 0);
    CHECK(chunksrv_put_data(p, 0, f, n) == 0);
    chunksrv_put_end(p, PL_E_DOWN);
    CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND);
    /* the whole one goes */
    CHECK(fake_put(cs, &c, 6, NULL, NULL) == 0);
    CHECK(fake_req(cs, &r, &cp) == 0);
    free(f);
    free(cp.body);
    free(c2.body);
    cdata_free(&c);
    chunksrv_close(cs);
}

/* ---------------------------------------------------------------- fences */
static void test_fences(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16];
    fresh("fences", dir, sizeof dir);
    mkstore(dir, 0, 7, true, st, sizeof st);
    struct chunksrv *cs = open1(st);
    if (!cs)
        return;
    cdata c;
    cdata_make(&c, 4, 0, 4096, 50000, 4);
    CHECK(fake_put(cs, &c, 1, NULL, NULL) == 0);
    cap cp = {0};
    struct cs_req r;
    req_of(&r, CS_OP_EPOCH, &c, 10); /* a request raises the fence first */
    CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len == 9 && le64(cp.body) == 10 && cp.body[8] == 0);
    cdata d;
    cdata_make(&d, 5, 1, 4096, 50000, 5); /* a writer of epoch 5, partitioned away while 10 came */
    CHECK(fake_put(cs, &d, 2, NULL, NULL) == CHUNK_ERR_FENCED);
    req_of(&r, CS_OP_READ, &d, 10);
    r.len = 1;
    CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND);
    req_of(&r, CS_OP_DEL, &c, 5);
    CHECK(fake_req(cs, &r, &cp) == CHUNK_ERR_FENCED);
    req_of(&r, CS_OP_READ, &c, 0); /* a read is never fenced, and still there */
    r.len = 1;
    CHECK(fake_req(cs, &r, &cp) == 0);
    /* durable: a reopen keeps the fence */
    chunksrv_close(cs);
    cs = open1(st);
    if (!cs)
        return;
    CHECK(chunk_be_epoch(chunksrv_backend(cs), 1) == 10);
    CHECK(fake_put(cs, &d, 3, NULL, NULL) == CHUNK_ERR_FENCED);
    req_of(&r, CS_OP_DEL, &c, 9);
    CHECK(fake_req(cs, &r, &cp) == CHUNK_ERR_FENCED);
    req_of(&r, CS_OP_DEL, &c, 11); /* a delete at a higher epoch raises and goes */
    CHECK(fake_req(cs, &r, &cp) == 0);
    CHECK(chunk_be_epoch(chunksrv_backend(cs), 1) == 11);
    req_of(&r, CS_OP_READ, &c, 11);
    r.len = 1;
    CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_NOTFOUND);
    d.m.epoch = 11;
    CHECK(fake_put(cs, &d, 4, NULL, NULL) == 0);
    free(cp.body);
    cdata_free(&c);
    cdata_free(&d);
    chunksrv_close(cs);
}

/* ---------------------------------------------------------------- unsure stores (D9) */
struct esrc {
    int caught_up;
    uint64_t epoch;
    int calls;
};

static int src_fn(void *arg, uint32_t volume, uint64_t *e)
{
    struct esrc *s = arg;
    s->calls++;
    if (!s->caught_up || volume != 1)
        return 0;
    *e = s->epoch;
    return 1;
}

static void test_unsure(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16];
    fresh("unsure", dir, sizeof dir);
    mkstore(dir, 0, 7, true, st, sizeof st);
    struct chunksrv *cs = open1(st);
    if (!cs)
        return;
    cap cp = {0};
    struct cs_req r = {.op = CS_OP_EPOCH, .volume = 1, .epoch = 3};
    CHECK(fake_req(cs, &r, &cp) == 0);
    r.epoch = 4; /* both slots written */
    CHECK(fake_req(cs, &r, &cp) == 0);
    chunksrv_close(cs);
    /* damage one slot (the store's EPOCH: two slots on pages of their own) */
    char ep[PATH_MAX + 64];
    snprintf(ep, sizeof ep, "%s/s1/EPOCH", st);
    int fd = open(ep, O_RDWR);
    CHECK(fd >= 0);
    uint8_t junk[16];
    memset(junk, 0x5a, sizeof junk);
    CHECK(pwrite(fd, junk, sizeof junk, 4096) == (ssize_t)sizeof junk);
    close(fd);
    cs = open1(st);
    if (!cs)
        return;
    r.epoch = 9;
    CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len == 9 && cp.body[8] == 1); /* reads and epoch queries go on */
    cdata c;
    cdata_make(&c, 9, 0, 4096, 30000, 6);
    CHECK(fake_put(cs, &c, 1, NULL, NULL) == CHUNK_ERR_EPOCH_UNSURE); /* a request's epoch never raises it */
    struct cs_req dr;
    req_of(&dr, CS_OP_DEL, &c, 9);
    CHECK(fake_req(cs, &dr, &cp) == CHUNK_ERR_EPOCH_UNSURE);
    struct esrc s = {.caught_up = 0, .epoch = 8};
    chunksrv_epoch_source(cs, src_fn, &s);
    CHECK(fake_put(cs, &c, 2, NULL, NULL) == CHUNK_ERR_EPOCH_UNSURE); /* a stale metadata copy does not either */
    CHECK(s.calls > 0);
    s.caught_up = 1; /* the caught-up copy says 8 */
    CHECK(fake_put(cs, &c, 3, NULL, NULL) == 0);
    CHECK(chunk_be_epoch(chunksrv_backend(cs), 1) == 9);
    cdata d;
    cdata_make(&d, 7, 1, 4096, 30000, 7);
    CHECK(fake_put(cs, &d, 4, NULL, NULL) == CHUNK_ERR_FENCED);
    r.epoch = 0;
    CHECK(fake_req(cs, &r, &cp) == 0 && cp.body[8] == 0);
    struct chunksrv_stats sst;
    chunksrv_get_stats(cs, &sst);
    CHECK(sst.source_raises == 1 && sst.unsure_refused >= 3);
    free(cp.body);
    cdata_free(&c);
    cdata_free(&d);
    chunksrv_close(cs);
}

/* ---------------------------------------------------------------- store binding (D11) */
static void test_owner(void)
{
    char dir[PATH_MAX], a[PATH_MAX + 16], b[PATH_MAX + 16], none[PATH_MAX + 16], err[512];
    fresh("owner", dir, sizeof dir);
    mkstore(dir, 0, 7, true, a, sizeof a);
    mkstore(dir, 1, 8, true, b, sizeof b);
    mkstore(dir, 2, 0, false, none, sizeof none);
    struct chunksrv *cs = open1(a);
    if (!cs)
        return;
    CHECK(chunksrv_check_owner(cs, CLUSTER, 7, err, sizeof err) == 0);
    err[0] = 0;
    CHECK(chunksrv_check_owner(cs, CLUSTER, 8, err, sizeof err) == -1); /* another node's disk */
    CHECK(strstr(err, "node 7") && strstr(err, "node 8") && strstr(err, a));
    uint8_t other[16];
    memcpy(other, CLUSTER, 16);
    other[0] ^= 0xff;
    err[0] = 0;
    CHECK(chunksrv_check_owner(cs, other, 7, err, sizeof err) == -1); /* another cluster's */
    CHECK(strstr(err, "0102030405") && strstr(err, "fe02030405"));
    chunksrv_close(cs);
    cs = open1(none);
    if (cs) {
        err[0] = 0;
        CHECK(chunksrv_check_owner(cs, CLUSTER, 7, err, sizeof err) == -1 && strstr(err, "names no cluster"));
        chunksrv_close(cs);
    }
    /* META with an owner is as strict as without: a changed node number fails the open (its sum) */
    char meta[PATH_MAX + 32];
    snprintf(meta, sizeof meta, "%s/META", b);
    FILE *f = fopen(meta, "r+");
    char text[256] = "";
    size_t n = f ? fread(text, 1, sizeof text - 1, f) : 0;
    char *at = strstr(text, "node=8");
    CHECK(at != NULL);
    if (f && at) {
        at[5] = '9';
        rewind(f);
        fwrite(text, 1, n, f);
    }
    if (f)
        fclose(f);
    chunk_store *s = NULL;
    CHECK(chunk_store_open(b, NULL, &s) == TSDB_ERR_CORRUPT);
    /* two owners of one store: refused */
    cs = open1(a);
    const char *pp[1] = {a};
    CHECK(chunksrv_open(pp, 1, err, sizeof err) == NULL && strstr(err, "open elsewhere"));
    chunksrv_close(cs);
    /* a missing store is DOWN, not fatal */
    char gone[PATH_MAX + 16];
    snprintf(gone, sizeof gone, "%s/nothere", dir);
    const char *p2[2] = {a, gone};
    cs = chunksrv_open(p2, 2, err, sizeof err);
    CHECK(cs != NULL);
    if (cs) {
        int up = -1;
        CHECK(chunksrv_stores(cs, &up, false) == 2 && up == 1);
        CHECK(chunksrv_check_owner(cs, CLUSTER, 7, err, sizeof err) == 0);
        chunksrv_close(cs);
    }
}

/* ---------------------------------------------------------------- power loss (crashfs) */
static int child_put(const char *dir, const char *ackf)
{
    char st[PATH_MAX + 16];
    snprintf(st, sizeof st, "%s/st0", dir);
    const char *pp[1] = {st};
    char err[256];
    struct chunksrv *cs = chunksrv_open(pp, 1, err, sizeof err);
    if (!cs)
        return 3;
    cdata c;
    cdata_make(&c, 2, 1, 4096, 4096 * 3 * 3 + 100, 42);
    int rc = fake_put(cs, &c, 1, NULL, ackf);
    cdata_free(&c);
    chunksrv_close(cs);
    return rc == 0 ? 0 : 4;
}

static int run_child(const char *dir, const char *ackf, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_chunksrv", "--child", dir, ackf, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void test_crashfs(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  crashfs: SKIPPED (no %s)\n", SHIM);
        return;
    }
    cdata c;
    cdata_make(&c, 2, 1, 4096, 4096 * 3 * 3 + 100, 42);
    int crashes = 0, acked_crashes = 0, acked_runs = 0, extra = 0;
    bool finished = false;
    for (int k = 0, last = -1; k < 400 && extra < (quick ? 8 : 24); k += finished ? 0 : 1) {
        char dir[PATH_MAX], st[PATH_MAX + 16], ackf[PATH_MAX + 32];
        fresh("crash", dir, sizeof dir);
        mkstore(dir, 0, 7, true, st, sizeof st);
        snprintf(ackf, sizeof ackf, "%s.ack", dir);
        unlink(ackf);
        int code = run_child(dir, ackf, finished ? last : k, k + 1 + extra * 7);
        if (finished)
            extra++;
        CHECK(code == 0 || code == 99);
        if (code != 0 && code != 99)
            break;
        crashes += code == 99;
        struct stat sb;
        bool acked = stat(ackf, &sb) == 0 && sb.st_size > 0;
        acked_crashes += acked && code == 99;
        acked_runs += acked;
        chunk_store *s = NULL;
        int rc = chunk_store_open(st, NULL, &s);
        CHECK(rc == 0);
        if (rc == 0) {
            chunk_ident id = {.volume = 1, .pos = c.m.pos};
            memcpy(id.vuuid, c.m.vuuid, 16);
            memcpy(id.oid, c.m.oid, 16);
            chunk_rd *rd = NULL;
            rc = chunk_rd_open(s, &id, &rd);
            if (acked && rc != 0)
                fprintf(stderr, "FAIL crashfs step %d: PUT_OK went out, and the chunk is %s after the power loss\n", k,
                        rc == TSDB_ERR_NOTFOUND ? "gone" : "damaged");
            CHECK(rc == 0 || (!acked && rc == TSDB_ERR_NOTFOUND));
            if (rc == 0) {
                uint64_t bad = 1;
                CHECK(chunk_rd_scrub(rd, &bad) == 0 && bad == 0);
                uint8_t *b = malloc(c.len);
                CHECK(chunk_rd_read(rd, 0, b, c.len) == 0 && memcmp(b, c.cells, c.len) == 0);
                free(b);
                chunk_rd_close(rd);
            }
            chunk_store_close(s);
        }
        if (code == 0 && !finished) {
            finished = true;
            last = k;
        }
    }
    printf("  crashfs: %d power cuts in a put (%d after PUT_OK), %d runs acknowledged then lost power at exit or before\n",
           crashes, acked_crashes, acked_runs);
    CHECK(finished && crashes > 3 && acked_runs > 3);
    cdata_free(&c);
}

/* ---------------------------------------------------------------- two servers over real links */
struct net_arg {
    struct chunksrv *a;
    uint32_t peer;
    int t, n;
    const char *dir;
    int bad;
};

static void *net_thread(void *arg)
{
    struct net_arg *na = arg;
    for (int i = 0; i < na->n; i++) {
        cdata c;
        cdata_make(&c, 6, (unsigned)(i % 5), 4096, 4096 * 3 * (2 + (uint64_t)i % 4) + 11,
                   1000 + (uint64_t)na->t * 100 + (uint64_t)i);
        int fd = chunksrv_stage(na->dir, &c.m.g, c.m.pos, c.cells, c.sums);
        int rc = fd >= 0 ? chunksrv_put_remote(na->a, na->peer, &c.m, fd, 0, cs_sums_hash(c.sums, c.ncells), 20000)
                         : fd;
        if (fd >= 0)
            close(fd);
        if (rc != 0) {
            fprintf(stderr, "FAIL net put: %d\n", rc);
            na->bad++;
        }
        struct cs_req r;
        req_of(&r, CS_OP_READ, &c, 6);
        r.off = 0, r.len = (uint32_t)c.len;
        int32_t prc = -1;
        uint8_t *body = NULL;
        size_t bl = 0;
        rc = chunksrv_call(na->a, na->peer, &r, 20000, &prc, &body, &bl);
        if (rc != 0 || prc != 0 || bl != c.len || memcmp(body, c.cells, c.len) != 0) {
            fprintf(stderr, "FAIL net read back: %d %d %zu\n", rc, prc, bl);
            na->bad++;
        }
        free(body);
        cdata_free(&c);
    }
    return NULL;
}

static void test_net(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16], err[256];
    fresh("net", dir, sizeof dir);
    mkstore(dir, 0, 2, true, st, sizeof st);
    struct chunksrv *b = open1(st);
    struct chunksrv *a = chunksrv_open(NULL, 0, err, sizeof err);
    CHECK(a && b);
    if (!a || !b)
        return;
    static const uint8_t secret[32] = {7};
    struct chunksrv_net_cfg ca = {.self_id = 1, .secret = secret, .secret_len = sizeof secret, .rate = 0};
    memcpy(ca.uuid, CLUSTER, 16);
    struct chunksrv_net_cfg cb = ca;
    cb.self_id = 2;
    ca.ident[0] = 1, cb.ident[0] = 2;
    CHECK(chunksrv_listen(a, &ca) == 0 && chunksrv_listen(b, &cb) == 0);
    int pa = peerlink_listen(chunksrv_peerlink(a), 0), pb = peerlink_listen(chunksrv_peerlink(b), 0);
    CHECK(pa > 0 && pb > 0);
    char addr[64];
    snprintf(addr, sizeof addr, "127.0.0.1:%d", pb);
    CHECK(chunksrv_set_peer(a, 2, addr) == 0);
    snprintf(addr, sizeof addr, "127.0.0.1:%d", pa);
    CHECK(chunksrv_set_peer(b, 1, addr) == 0);
    enum { NT = 4 };
    struct net_arg na[NT];
    pthread_t th[NT];
    for (int t = 0; t < NT; t++) {
        na[t] = (struct net_arg){.a = a, .peer = 2, .t = t, .n = quick ? 3 : 8, .dir = dir};
        pthread_create(&th[t], NULL, net_thread, &na[t]);
    }
    for (int t = 0; t < NT; t++) {
        pthread_join(th[t], NULL);
        CHECK(na[t].bad == 0);
    }
    /* a fenced put over the link: refused with the reason, nothing there */
    struct cs_req r = {.op = CS_OP_EPOCH, .volume = 1, .epoch = 20};
    int32_t prc = -1;
    uint8_t *body = NULL;
    size_t bl = 0;
    CHECK(chunksrv_call(a, 2, &r, 10000, &prc, &body, &bl) == 0 && prc == 0 && bl == 9);
    free(body);
    cdata c;
    cdata_make(&c, 19, 0, 4096, 40000, 77);
    int fd = chunksrv_stage(dir, &c.m.g, c.m.pos, c.cells, c.sums);
    CHECK(chunksrv_put_remote(a, 2, &c.m, fd, 0, cs_sums_hash(c.sums, c.ncells), 10000) == CHUNK_ERR_FENCED);
    /* a sum the answer does not match: the sender notices */
    c.m.epoch = 20;
    CHECK(chunksrv_put_remote(a, 2, &c.m, fd, 0, cs_sums_hash(c.sums, c.ncells) ^ 1, 10000) == TSDB_ERR_IO);
    close(fd);
    /* a member that is not there: unavailable, quickly */
    CHECK(chunksrv_call(a, 9, &r, 100, &prc, &body, &bl) == TSDB_ERR_UNAVAILABLE);
    cdata_free(&c);
    struct chunksrv_stats sst;
    chunksrv_get_stats(b, &sst);
    CHECK(sst.puts_ok == (uint64_t)NT * (uint64_t)na[0].n + 1 && sst.fenced == 1);
    chunksrv_close(a);
    chunksrv_close(b);
}

int main(int argc, char **argv)
{
    const char *mutant = NULL, *base = "/tmp";
    if (getenv("TSDB_CHUNKSRV_MUTANT"))
        chunksrv_mutant = chunksrv_mutant_parse(getenv("TSDB_CHUNKSRV_MUTANT"));
    if (argc >= 4 && strcmp(argv[1], "--child") == 0)
        return child_put(argv[2], argv[3]);
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = true;
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else
            base = argv[i];
    }
    if (mutant) {
        chunksrv_mutant = chunksrv_mutant_parse(mutant);
        if (chunksrv_mutant <= 0) {
            fprintf(stderr, "test_chunksrv: no mutant %s\n", mutant);
            return 2;
        }
        setenv("TSDB_CHUNKSRV_MUTANT", mutant, 1); /* the power-loss children are other processes */
    }
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/tsdb-chunksrv-XXXXXX", base);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    printf("test_chunksrv%s%s%s\n", quick ? " (quick)" : "", mutant ? " mutant " : "", mutant ? mutant : "");
    test_wire();
    test_put_read();
    test_bad_cell();
    test_fences();
    test_unsure();
    test_owner();
    test_crashfs();
    test_net();
    if (fails == 0) {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", S);
        if (system(cmd) != 0)
            fprintf(stderr, "test_chunksrv: could not remove %s\n", S);
    }
    printf("test_chunksrv: %s (%d failures)\n", fails ? "FAILED" : "ok", fails);
    return fails ? 1 : 0;
}
#endif
