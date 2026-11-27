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
 *  - EIO (crashfs eio) at every step of a run of puts: the server carries on; a store the error failed (E57) is
 *    reported DOWN and refuses puts, one it did not fail stays UP and takes the next put whole;
 *  - net: puts and reads between two chunk servers over real links, several threads at once.
 * Mutants (--mutant NAME; the Makefile requires each to fail it): chunknofence, chunkselfraise, putokearly,
 * nocellcheck, storeforeign, nometacheck, sumsonly, noreqcheck (test_wire and net_flips), noopmatch (net_flips), norespcheck (test_wire and net_flips). The eio case's teeth were checked by hand (chunk_be_store_state reporting a failed
 * store UP fails it with exit 20); that check is R6's, in src/chunkstore.c.
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
static bool net_only;          /* --net: test_net alone */
static const char *check_name; /* --check LEVEL */
static int check_level;        /* its level (0: the default, none) */
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
        *sum_ok = rc == 0 && cp.body_len == 8 &&
                  le64(cp.body) == cs_put_answer_hash(meta, cs_sums_hash(c->sums, c->ncells));
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
        /* every bit of a request is covered by its check: with no frame check on the link (the default), a flipped
         * epoch would otherwise raise a store's fence durably */
        int taken = 0;
        for (size_t k = 0; k < 8 * n; k++) {
            buf[k / 8] ^= (uint8_t)(1u << k % 8);
            taken += cs_req_decode(buf, n, &d) == 0;
            buf[k / 8] ^= (uint8_t)(1u << k % 8);
        }
        CHECK(taken == 0);
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
    /* answers (rid 8, op 1, rc 4, check 8, body): a body of the wrong length for its op; each sealed as sent */
    uint8_t a[64] = {0};
    struct cs_resp rr;
#define RESP_OK(len) (cs_resp_seal(a, (len)), cs_resp_decode(a, (len), &rr))
    a[8] = CS_OP_PUT;
    CHECK(RESP_OK(21 + 8) == 0 && RESP_OK(21 + 7) == -1);
    a[8] = CS_OP_EPOCH;
    CHECK(RESP_OK(21 + 9) == 0 && RESP_OK(21 + 10) == -1);
    a[9] = 0xff, a[10] = 0xff, a[11] = 0xff, a[12] = 0xff; /* rc -1 with a body */
    CHECK(RESP_OK(21 + 9) == -1 && RESP_OK(21) == 0);
    a[12] = 0x7f; /* a positive rc */
    CHECK(RESP_OK(21) == -1);
    /* the check (review F4): a DEL's error flipped to 0 after sealing is refused, and so is a flipped EPOCH fence; a
     * READ's covers its length but not its cells (the reader checks those against the object's sums) */
    memset(a, 0, sizeof a);
    a[8] = CS_OP_DEL, a[9] = 0xf5, a[10] = 0xff, a[11] = 0xff, a[12] = 0xff;
    cs_resp_seal(a, 21);
    memset(a + 9, 0, 4);
    CHECK(cs_resp_decode(a, 21, &rr) == -1);
    memset(a, 0, sizeof a);
    a[8] = CS_OP_EPOCH, a[21] = 9;
    cs_resp_seal(a, 21 + 9);
    a[21 + 5] ^= 0x01;
    CHECK(cs_resp_decode(a, 21 + 9, &rr) == -1);
    memset(a, 0, sizeof a);
    a[8] = CS_OP_READ, a[21] = 1;
    cs_resp_seal(a, 21 + 4);
    a[22] ^= 0x01; /* a cell byte: the reader's sums are its check */
    CHECK(cs_resp_decode(a, 21 + 4, &rr) == 0);
    CHECK(cs_resp_decode(a, 21 + 3, &rr) == -1); /* a length changed */
#undef RESP_OK
    /* put meta */
    cdata c;
    cdata_make(&c, 5, 4, 4096, 100000, 1);
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(&c.m, meta);
    struct cs_put_meta m;
    CHECK(cs_put_meta_decode(meta, sizeof meta, &m) == 0 && m.pos == 4 && m.epoch == 5 && m.g.len == 100000);
    CHECK(cs_put_meta_decode(meta, sizeof meta - 1, &m) == -1);
    int taken = 0; /* every bit of the meta is covered by its check (the epoch is raised durably from it) */
    for (size_t k = 0; k < 8 * sizeof meta; k++) {
        meta[k / 8] ^= (uint8_t)(1u << k % 8);
        taken += cs_put_meta_decode(meta, sizeof meta, &m) == 0;
        meta[k / 8] ^= (uint8_t)(1u << k % 8);
    }
    CHECK(taken == 0);
    /* PUT_OK's hash covers the meta: another identity with the same sums answers another hash */
    uint64_t h0 = cs_put_answer_hash(meta, 42);
    meta[32] ^= 1;
    CHECK(cs_put_answer_hash(meta, 42) != h0 && cs_put_answer_hash(meta, 43) != cs_put_answer_hash(meta, 42));
    c.m.pos = 5; /* position 5 of 3+2, with a check that holds */
    cs_put_meta_encode(&c.m, meta);
    CHECK(cs_put_meta_decode(meta, sizeof meta, &m) == -1);
    cdata_free(&c);
    CHECK(chunksrv_st(0) == CHUNK_ST_OK && chunksrv_st(TSDB_ERR_NOTFOUND) == CHUNK_ST_ABSENT &&
          chunksrv_st(TSDB_ERR_CORRUPT) == CHUNK_ST_DAMAGED && chunksrv_st(TSDB_ERR_IO) == CHUNK_ST_IOERR &&
          chunksrv_st(TSDB_ERR_UNAVAILABLE) == CHUNK_ST_DOWN);
    /* a SUMS answer is taken only when its count and its length agree: a reader by count must not overread */
    uint8_t rb[21 + 36 + 16];
    memset(rb, 0, sizeof rb);
    rb[8] = CS_OP_SUMS;
    struct cs_resp rsp;
    for (uint32_t cnt = 0; cnt <= 3; cnt++) {
        rb[21 + 32] = (uint8_t)cnt;
        cs_resp_seal(rb, sizeof rb); /* a check that holds: only the count decides */
        CHECK(cs_resp_decode(rb, sizeof rb, &rsp) == (cnt == 2 ? 0 : -1));
    }
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


/* ---------------------------------------------------------------- review regressions */
/* LIST with one (oid, pos) in two stores: every page size sees every copy once, with its header (F1, F8) */
static void test_list_copies(void)
{
    char dir[PATH_MAX], s0[PATH_MAX + 16], s1[PATH_MAX + 16], err[256];
    fresh("listcp", dir, sizeof dir);
    mkstore(dir, 0, 7, true, s0, sizeof s0);
    mkstore(dir, 1, 7, true, s1, sizeof s1);
    const char *pp[2] = {s0, s1};
    struct chunksrv *cs = chunksrv_open(pp, 2, err, sizeof err);
    CHECK(cs != NULL);
    if (!cs)
        return;
    enum { NC = 4 };
    cdata c[NC];
    int where[NC][2] = {{1, 1}, {1, 0}, {0, 1}, {1, 1}}; /* chunk i in store 0, in store 1 */
    int total = 0;
    for (int i = 0; i < NC; i++) {
        cdata_make(&c[i], 3, (unsigned)i % 5, 4096, 30000, 50 + (uint64_t)i);
        chunk_ident id = {.volume = c[i].m.volume, .pos = c[i].m.pos};
        memcpy(id.vuuid, c[i].m.vuuid, 16);
        memcpy(id.oid, c[i].m.oid, 16);
        for (int st = 0; st < 2; st++)
            if (where[i][st]) {
                CHECK(chunk_be_put(chunksrv_backend(cs), st, &id, &c[i].m.g, 3, c[i].cells, c[i].sums) == 0);
                total++;
            }
    }
    for (uint32_t pg = 1; pg <= 7; pg++) {
        struct cs_req r = {.rid = 1, .op = CS_OP_LIST, .volume = c[0].m.volume, .epoch = 3, .len = pg};
        cap cp = {0};
        int seen[NC][2] = {{0}};
        int got = 0, pages = 0;
        for (; pages < 40; pages++) {
            CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len >= 5);
            if (cp.rc != 0 || cp.body_len < 5)
                break;
            uint32_t k = (uint32_t)(cp.body[0] | cp.body[1] << 8);
            CHECK(k <= pg || k <= 2); /* above the page only for the two copies of one key */
            for (uint32_t j = 0; j < k; j++) {
                const uint8_t *e = cp.body + 5 + CS_LIST_ENTRY * j;
                for (int i = 0; i < NC; i++)
                    if (!memcmp(e, c[i].m.oid, 16) && (e[16] | e[17] << 8) == c[i].m.pos && e[51] < 2) {
                        seen[i][e[51]]++;
                        CHECK(e[50] == CHUNK_ST_OK && le64(e + 26) == 3 && le64(e + 18) > 0 &&
                              !memcmp(e + 34, c[i].m.vuuid, 16));
                    }
                got++;
            }
            if (k) {
                const uint8_t *e = cp.body + 5 + CS_LIST_ENTRY * (k - 1);
                memcpy(r.oid, e, 16);
                r.pos = (uint16_t)(e[16] | e[17] << 8);
                r.has_cursor = true;
            }
            if (!cp.body[4])
                break;
        }
        CHECK(got == total);
        for (int i = 0; i < NC; i++)
            for (int st = 0; st < 2; st++)
                if (seen[i][st] != where[i][st]) {
                    fprintf(stderr, "FAIL list page %u: chunk %d store %d seen %d times\n", pg, i, st, seen[i][st]);
                    fails++;
                }
        free(cp.body);
    }
    for (int i = 0; i < NC; i++)
        cdata_free(&c[i]);
    chunksrv_close(cs);
}

/* An unsure store of two: a request at 9 raises the sure one; the caught-up source says 8; the unsure one is raised to
 * 9, not 8: a writer at 8 is FENCED on both stores (F4). Without the raise of the sure store too, 8 passes. */
static int src_eight(void *arg, uint32_t volume, uint64_t *e)
{
    (void)arg, (void)volume;
    *e = 8;
    return 1;
}

static void test_unsure_two(void)
{
    char dir[PATH_MAX], s0[PATH_MAX + 16], s1[PATH_MAX + 16], err[256];
    fresh("unsure2", dir, sizeof dir);
    mkstore(dir, 0, 7, true, s0, sizeof s0);
    mkstore(dir, 1, 7, true, s1, sizeof s1);
    const char *pp[2] = {s0, s1};
    struct chunksrv *cs = chunksrv_open(pp, 2, err, sizeof err);
    CHECK(cs != NULL);
    if (!cs)
        return;
    cap cp = {0};
    struct cs_req r = {.op = CS_OP_EPOCH, .volume = 1, .epoch = 3};
    CHECK(fake_req(cs, &r, &cp) == 0);
    r.epoch = 4;
    CHECK(fake_req(cs, &r, &cp) == 0);
    chunksrv_close(cs);
    char ep[PATH_MAX + 64];
    snprintf(ep, sizeof ep, "%s/s1/EPOCH", s0);
    int fd = open(ep, O_RDWR);
    CHECK(fd >= 0);
    uint8_t junk[16];
    memset(junk, 0x5a, sizeof junk);
    CHECK(fd >= 0 && pwrite(fd, junk, sizeof junk, 4096) == (ssize_t)sizeof junk);
    if (fd >= 0)
        close(fd);
    cs = chunksrv_open(pp, 2, err, sizeof err);
    CHECK(cs != NULL);
    if (!cs)
        return;
    cdata c;
    cdata_make(&c, 9, 0, 4096, 30000, 6);
    struct cs_req rr;
    req_of(&rr, CS_OP_READ, &c, 9);
    rr.len = 1;
    fake_req(cs, &rr, &cp); /* nothing there; the epoch is seen all the same */
    chunk_store *sure = chunk_be_store(chunksrv_backend(cs), 1);
    CHECK(sure && chunk_store_epoch(sure, 1) == 9);
    chunksrv_epoch_source(cs, src_eight, NULL);
    int fenced = 0;
    for (unsigned seed = 20; seed < 28; seed++) {
        cdata d;
        cdata_make(&d, 8, 0, 4096, 30000, seed);
        fenced += fake_put(cs, &d, seed, NULL, NULL) == CHUNK_ERR_FENCED;
        cdata_free(&d);
    }
    CHECK(fenced == 8);
    r.epoch = 0;
    CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len == 9 && le64(cp.body) == 9 && cp.body[8] == 0);
    free(cp.body);
    cdata_free(&c);
    chunksrv_close(cs);
}

/* Bytes out of order on a put, and a SUMS beyond the chunk's cells: refused (INVALID), nothing read past a table */
static void test_bounds(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16];
    fresh("bounds", dir, sizeof dir);
    mkstore(dir, 0, 7, true, st, sizeof st);
    struct chunksrv *cs = open1(st);
    if (!cs)
        return;
    cdata c;
    cdata_make(&c, 3, 1, 4096, 30000, 9);
    size_t n;
    uint8_t *f = cdata_file(&c, &n);
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(&c.m, meta);
    cap cp = {0};
    struct cs_put *p = NULL;
    CHECK(chunksrv_put_begin(cs, 2, 5, meta, sizeof meta, n, cap_reply, &cp, &p) == 0);
    if (p) {
        CHECK(chunksrv_put_data(p, 8, f + 8, 8) == TSDB_ERR_INVALID); /* a gap */
        chunksrv_put_end(p, 0);
        CHECK(cp.n == 1 && cp.rc == TSDB_ERR_INVALID);
    }
    p = NULL;
    cp.n = 0;
    CHECK(chunksrv_put_begin(cs, 2, 6, meta, sizeof meta, n, cap_reply, &cp, &p) == 0);
    if (p) {
        CHECK(chunksrv_put_data(p, 0, f, 16) == 0);
        CHECK(chunksrv_put_data(p, 8, f + 8, 16) == TSDB_ERR_INVALID); /* an overlap */
        chunksrv_put_end(p, 0);
        CHECK(cp.n == 1 && cp.rc == TSDB_ERR_INVALID);
    }
    CHECK(fake_put(cs, &c, 7, NULL, NULL) == 0);
    struct cs_req r;
    req_of(&r, CS_OP_SUMS, &c, 3);
    r.len = 1;
    r.off = c.ncells; /* the end: no sums, but asked properly */
    CHECK(fake_req(cs, &r, &cp) == 0 && cp.body_len == 36);
    for (uint64_t off = c.ncells + 1; off < c.ncells + 4; off++) {
        r.off = off;
        CHECK(fake_req(cs, &r, &cp) == TSDB_ERR_INVALID && cp.body_len == 0);
    }
    free(cp.body);
    free(f);
    cdata_free(&c);
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

/* ---------------------------------------------------------------- EIO (crashfs eio): a failed store is DOWN */
/* Puts until one fails (the shim injects one EIO), then: the report, one more put and a read of it. Exit 0: no put
 * failed (the fault lay past the workload); 10: the store failed, is reported DOWN and refuses the put; 11: only the
 * call failed, the store is UP and takes the put whole; 20, 21: the report and the store disagree; 30: a check. */
static int child_eio(const char *dir)
{
    char st[PATH_MAX + 16];
    snprintf(st, sizeof st, "%s/st0", dir);
    const char *pp[1] = {st};
    char err[256];
    struct chunksrv *cs = chunksrv_open(pp, 1, err, sizeof err);
    if (!cs)
        return 3;
    int code = 0;
    bool failed = false;
    for (int i = 0; i < 3 && !failed; i++) {
        cdata c;
        cdata_make(&c, 2, 1, 4096, 4096 * 3 * 3 + 100, 50 + (uint64_t)i);
        failed = fake_put(cs, &c, 1 + (uint64_t)i, NULL, NULL) != 0;
        cdata_free(&c);
    }
    if (failed) {
        int up = -1;
        chunksrv_stores(cs, &up, false);
        cdata c;
        cdata_make(&c, 2, 1, 4096, 4096 * 3 * 3 + 100, 60);
        bool ok = fake_put(cs, &c, 9, NULL, NULL) == 0;
        if (ok) {
            cap cp = {0};
            struct cs_req r;
            req_of(&r, CS_OP_READ, &c, 2);
            r.off = 0, r.len = (uint32_t)c.len;
            ok = fake_req(cs, &r, &cp) == 0 && cp.body_len == c.len && memcmp(cp.body, c.cells, c.len) == 0;
            free(cp.body);
        }
        cdata_free(&c);
        code = up == 0 ? (ok ? 21 : 10) : (ok ? 11 : 20);
    }
    chunksrv_close(cs);
    return fails ? 30 : code;
}

static void test_eio(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  eio: SKIPPED (no %s)\n", SHIM);
        return;
    }
    int n[32] = {0}, k = 0;
    for (; k < 400; k++) {
        char dir[PATH_MAX], st[PATH_MAX + 16];
        fresh("eio", dir, sizeof dir);
        mkstore(dir, 0, 7, true, st, sizeof st);
        int code = -1;
        pid_t pid = fork();
        if (pid == 0) {
            char asan[512], w[16];
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                     getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            snprintf(w, sizeof w, "%d", k);
            setenv("LD_PRELOAD", SHIM, 1);
            setenv("CRASHFS_ROOT", dir, 1);
            setenv("CRASHFS_MODE", "eio", 1);
            setenv("CRASHFS_PROB", "1", 1);
            setenv("CRASHFS_WARMUP", w, 1);
            setenv("CRASHFS_SEED", w, 1);
            int null = open("/dev/null", O_WRONLY);
            if (null >= 0) {
                dup2(null, 1);
                dup2(null, 2);
            }
            execl(SELF, "test_chunksrv", "--child-eio", dir, (char *)NULL);
            _exit(127);
        }
        int ws = -1;
        if (pid > 0 && waitpid(pid, &ws, 0) == pid && WIFEXITED(ws))
            code = WEXITSTATUS(ws);
        if (code != 0 && code != 10 && code != 11) {
            fprintf(stderr, "FAIL eio step %d: child exit %d (%s)\n", k, code,
                    code == 20   ? "a failed store reported UP"
                    : code == 21 ? "a store reported DOWN took a put"
                                 : "the child failed");
            fails++;
            break;
        }
        n[code]++;
        if (code == 0)
            break;
    }
    printf("  eio: %d steps: the store failed and was reported DOWN %d times, the call alone failed %d times\n", k + 1,
           n[10], n[11]);
    CHECK(n[0] == 1 && n[10] > 0);
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

/* b's fence of volume 1, by an EPOCH call at epoch 0 (which raises nothing) */
static uint64_t net_epoch(struct chunksrv *a)
{
    struct cs_req r = {.op = CS_OP_EPOCH, .volume = 1, .epoch = 0};
    int32_t prc = -1;
    uint8_t *body = NULL;
    size_t bl = 0;
    uint64_t e = UINT64_MAX;
    for (int i = 0; i < 100 && e == UINT64_MAX; i++) /* the link may be coming back after a reset */
        if (chunksrv_call(a, 2, &r, 10000, &prc, &body, &bl) == 0 && prc == 0 && bl == 9)
            e = le64(body);
    free(body);
    return e;
}

static int net_link_reset(struct chunksrv *cs, uint64_t g0, struct plink *l)
{
    for (int i = 0; i < 500 && plink_gen(l) == g0; i++)
        nanosleep(&(struct timespec){0, 10 * 1000000L}, NULL);
    (void)cs;
    return plink_gen(l) != g0;
}

/* Flips planted on the receive side, as the links carry no frame check by default (--peer-check none): a put meta's
 * epoch (refused, the fence not raised), its oid with a check that holds (never PUT_OK), a request's epoch (refused,
 * the link ended, the fence not raised), an answer that reaches a call of another op (refused, the link ended). */
static void net_flips(struct chunksrv *a, struct chunksrv *b, const char *dir)
{
    uint64_t e0 = net_epoch(a);
    CHECK(e0 == 20);
    cdata c;
    cdata_make(&c, 21, 0, 4096, 40000, 78);
    c.m.epoch = 20;
    int fd = chunksrv_stage(dir, &c.m.g, c.m.pos, c.cells, c.sums);
    uint64_t sh = cs_sums_hash(c.sums, c.ncells);
    chunksrv_test_flip(CS_FLIP_META, 8 + 5, 0x01, false); /* the epoch's bit 40 */
    int rc = chunksrv_put_remote(a, 2, &c.m, fd, 0, sh, 10000);
    uint64_t e1 = net_epoch(a);
    if (rc == 0 || e1 != 20)
        fprintf(stderr, "FAIL a put meta with its epoch flipped: put %d, the fence went from 20 to %" PRIu64 "\n", rc, e1);
    CHECK(rc != 0 && e1 == 20);
    chunksrv_test_flip(CS_FLIP_META, 32, 0x01, true); /* the oid, with a check that holds */
    rc = chunksrv_put_remote(a, 2, &c.m, fd, 0, sh, 10000);
    if (rc != TSDB_ERR_IO)
        fprintf(stderr, "FAIL a put whose oid changed on the way: %d, not IO (PUT_OK taken for another chunk)\n", rc);
    CHECK(rc == TSDB_ERR_IO);
    CHECK(chunksrv_put_remote(a, 2, &c.m, fd, 0, sh, 10000) == 0); /* the honest put after them */
    close(fd);
    cdata_free(&c);

    struct plink *lb = peerlink_find(chunksrv_peerlink(b), 1, PL_KIND_CHUNK);
    CHECK(lb != NULL);
    if (lb) /* the link runs at the level both ends were given (--check; none by default) */
        CHECK(plink_check(lb) == (check_level ? check_level : peerlink_check_parse("none")));
    if (lb) {
        uint64_t g0 = plink_gen(lb);
        struct chunksrv_stats st0, st1;
        chunksrv_get_stats(b, &st0);
        chunksrv_test_flip(CS_FLIP_REQ, 13 + 5, 0x01, false); /* a request's epoch, bit 40 */
        struct cs_req r = {.op = CS_OP_EPOCH, .volume = 1, .epoch = 0};
        int32_t prc = -1;
        uint8_t *body = NULL;
        size_t bl = 0;
        rc = chunksrv_call(a, 2, &r, 2000, &prc, &body, &bl);
        free(body);
        body = NULL;
        CHECK(rc == TSDB_ERR_UNAVAILABLE && net_link_reset(b, g0, lb));
        chunksrv_get_stats(b, &st1);
        CHECK(st1.malformed == st0.malformed + 1);
        e1 = net_epoch(a);
        if (e1 != 20)
            fprintf(stderr, "FAIL a request with its epoch flipped raised the fence from 20 to %" PRIu64 "\n", e1);
        CHECK(e1 == 20);
    }
    struct plink *la = peerlink_find(chunksrv_peerlink(a), 2, PL_KIND_CHUNK);
    CHECK(la != NULL);
    if (la) {
        /* a fenced DEL answers an error with no body, which decodes as any op's: the op is
         * changed to EPOCH's on its way back, so only the match with the pending call refuses it */
        struct cs_req r = {.op = CS_OP_DEL, .volume = 1, .epoch = 19, .pos = 0}; /* fenced: an rc, no body */
        memset(r.vuuid, 0x33, 16);
        memset(r.oid, 0x44, 16);
        int32_t prc = 0;
        uint8_t *body = NULL;
        size_t bl = 0;
        CHECK(chunksrv_call(a, 2, &r, 10000, &prc, &body, &bl) == 0 && prc != 0 && bl == 0); /* the control */
        free(body);
        body = NULL;
        struct chunksrv_stats st0, st1;
        chunksrv_get_stats(a, &st0);
        chunksrv_test_flip(CS_FLIP_RESP, 8, CS_OP_DEL ^ CS_OP_EPOCH, true); /* the check made again: the op match */
        prc = 0;
        rc = chunksrv_call(a, 2, &r, 2000, &prc, &body, &bl);
        free(body);
        if (rc != TSDB_ERR_UNAVAILABLE)
            fprintf(stderr, "FAIL an answer of op EPOCH taken by a DEL call: %d (rc %d)\n", rc, prc);
        CHECK(rc == TSDB_ERR_UNAVAILABLE); /* the link the answer came on is ended (the line it says) */
        chunksrv_get_stats(a, &st1);
        CHECK(st1.answer_mismatch == st0.answer_mismatch + 1);
        CHECK(net_epoch(a) == 20); /* and serves again */
        /* the DEL's error code flipped on its way back with the answer's check as sent (review F4): malformed, the
         * link ends; unchecked it would reach the caller as another answer */
        chunksrv_get_stats(a, &st0);
        chunksrv_test_flip(CS_FLIP_RESP, 9, 0x01, false);
        prc = 0;
        rc = chunksrv_call(a, 2, &r, 2000, &prc, &body, &bl);
        free(body);
        body = NULL;
        if (rc != TSDB_ERR_UNAVAILABLE)
            fprintf(stderr, "FAIL a DEL answer with a flipped rc taken: %d (rc %d)\n", rc, prc);
        CHECK(rc == TSDB_ERR_UNAVAILABLE);
        chunksrv_get_stats(a, &st1);
        CHECK(st1.malformed == st0.malformed + 1);
        CHECK(net_epoch(a) == 20);
    }
    chunksrv_test_flip(CS_FLIP_NONE, 0, 0, false);
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
    enum { NT = 16 };
    struct net_arg na[NT];
    pthread_t th[NT];
    for (int t = 0; t < NT; t++) {
        na[t] = (struct net_arg){.a = a, .peer = 2, .t = t, .n = quick ? 2 : 6, .dir = dir};
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
    /* a malformed CHUNK frame ends the connection it came on (TASK 4): the link comes back as a new one */
    struct plink *l = peerlink_find(chunksrv_peerlink(a), 2, PL_KIND_CHUNK);
    CHECK(l != NULL);
    if (l) {
        uint64_t g0 = plink_gen(l);
        static const uint8_t junk[5] = {1, 2, 3, 4, 5};
        CHECK(plink_send(l, CS_T_REQ, junk, sizeof junk, 1000) == PL_OK);
        for (int i = 0; i < 500 && plink_gen(l) == g0; i++)
            nanosleep(&(struct timespec){0, 10 * 1000000L}, NULL);
        CHECK(plink_gen(l) != g0);
        chunksrv_get_stats(b, &sst);
        CHECK(sst.malformed == 1);
        r.epoch = 0; /* and serves again */
        CHECK(chunksrv_call(a, 2, &r, 10000, &prc, &body, &bl) == 0 && prc == 0);
        free(body);
        body = NULL;
    }
    net_flips(a, b, dir);
    /* a member that left: its links stop, calls to it are unavailable at once, and it is not dialed again */
    chunksrv_keep_peers(a, NULL, 0);
    CHECK(chunksrv_call(a, 2, &r, 5000, &prc, &body, &bl) == TSDB_ERR_UNAVAILABLE);
    CHECK(chunksrv_set_peer(a, 2, "127.0.0.1:1") == -1);
    chunksrv_close(a);
    chunksrv_close(b);
}

/* A disk slower than the links' dead time: the put is answered once the disk is done, on the same CHUNKB connection
 * (the store I/O runs on the peer's put thread; peerlink also keeps a link whose file is in flight, so this holds with
 * the I/O on the link's thread too: it is the behaviour, not the move, that this checks). */
static void test_slow_disk(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16], err[256];
    fresh("slow", dir, sizeof dir);
    mkstore(dir, 0, 2, true, st, sizeof st);
    struct chunksrv *b = open1(st);
    struct chunksrv *a = chunksrv_open(NULL, 0, err, sizeof err);
    CHECK(a && b);
    if (!a || !b)
        return;
    static const uint8_t secret[32] = {9};
    struct chunksrv_net_cfg ca = {.self_id = 1, .secret = secret, .secret_len = sizeof secret, .dead_ms = 400,
                                  .ping_ms = 100};
    memcpy(ca.uuid, CLUSTER, 16);
    struct chunksrv_net_cfg cb = ca;
    cb.self_id = 2;
    ca.ident[0] = 1, cb.ident[0] = 2;
    CHECK(chunksrv_listen(a, &ca) == 0 && chunksrv_listen(b, &cb) == 0);
    int pa = peerlink_listen(chunksrv_peerlink(a), 0), pb = peerlink_listen(chunksrv_peerlink(b), 0);
    char addr[64];
    snprintf(addr, sizeof addr, "127.0.0.1:%d", pb);
    CHECK(chunksrv_set_peer(a, 2, addr) == 0);
    snprintf(addr, sizeof addr, "127.0.0.1:%d", pa);
    CHECK(chunksrv_set_peer(b, 1, addr) == 0);
    cdata c;
    cdata_make(&c, 4, 0, 4096, 40000, 99);
    int fd = chunksrv_stage(dir, &c.m.g, c.m.pos, c.cells, c.sums);
    CHECK(fd >= 0);
    struct cs_req er = {.op = CS_OP_EPOCH, .volume = c.m.volume, .epoch = 1};
    int32_t erc = -1;
    uint8_t *eb = NULL;
    size_t ebl = 0;
    CHECK(chunksrv_call(a, 2, &er, 10000, &erc, &eb, &ebl) == 0 && erc == 0); /* both links up */
    free(eb);
    struct plink *lb = peerlink_find(chunksrv_peerlink(a), 2, PL_KIND_CHUNKB);
    for (int i = 0; i < 500 && !(lb && plink_up(lb)); i++) {
        nanosleep(&(struct timespec){0, 10 * 1000000L}, NULL);
        lb = peerlink_find(chunksrv_peerlink(a), 2, PL_KIND_CHUNKB);
    }
    CHECK(lb && plink_up(lb));
    uint64_t g0 = lb ? plink_gen(lb) : 0;
    chunksrv_test_put_delay_ms = 2000;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = fd >= 0 ? chunksrv_put_remote(a, 2, &c.m, fd, 0, cs_sums_hash(c.sums, c.ncells), 8000) : fd;
    chunksrv_test_put_delay_ms = 0;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    long ms = (long)((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000);
    printf("  slow disk: a put that waited 2000 ms on its first cell answered after %ld ms\n", ms);
    CHECK(ms >= 2000); /* the hook ran */
    if (rc != 0)
        fprintf(stderr, "FAIL slow disk put: %d\n", rc);
    CHECK(rc == 0);
    if (lb && plink_gen(lb) != g0)
        fprintf(stderr, "FAIL slow disk: the CHUNKB link was ended during the put (connection %llu, then %llu)\n",
                (unsigned long long)g0, (unsigned long long)plink_gen(lb));
    CHECK(!lb || plink_gen(lb) == g0);
    if (fd >= 0)
        close(fd);
    struct cs_req r;
    req_of(&r, CS_OP_READ, &c, 4);
    r.off = 0, r.len = (uint32_t)c.len;
    int32_t prc = -1;
    uint8_t *body = NULL;
    size_t bl = 0;
    CHECK(chunksrv_call(a, 2, &r, 10000, &prc, &body, &bl) == 0 && prc == 0 && bl == c.len &&
          memcmp(body, c.cells, c.len) == 0);
    free(body);
    cdata_free(&c);
    chunksrv_close(a);
    chunksrv_close(b);
}

/* ---------------------------------------------------------------- R9 OBJLIST, REOPEN, and PL_KIND_CHUNKR */
struct mock_objlist_ctx {
    struct cs_objlist_item items[5];
    uint32_t n;
    int force_rc;
    uint32_t reopen_calls;
    uint32_t reopen_vol;
    uint8_t reopen_oid[16];
    pthread_mutex_t mu;
    uint32_t pace_calls;
    uint32_t pace_vol;
    size_t pace_bytes;
};

static int mock_objlist_fn(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                           const uint8_t cursor_oid[16], uint32_t max_entries, size_t max_bytes,
                           uint8_t vuuid_out[16], struct cs_objlist_item **items_out, uint32_t *n_out,
                           bool *more_out)
{
    struct mock_objlist_ctx *ctx = arg;
    (void)volume;
    if (ctx->force_rc != 0)
        return ctx->force_rc;
    memcpy(vuuid_out, VUUID, 16);
    uint32_t idxs[5], nmatch = 0;
    for (uint32_t i = 0; i < ctx->n; i++) {
        if (max_version != 0 && ctx->items[i].stripe_version >= max_version)
            continue;
        if (has_cursor && memcmp(ctx->items[i].oid, cursor_oid, 16) <= 0)
            continue;
        idxs[nmatch++] = i;
    }
    size_t bytes = CS_OBJLIST_HDR;
    uint32_t take = 0;
    for (uint32_t i = 0; i < nmatch && take < max_entries; i++) {
        size_t need = CS_OBJLIST_ENTRY_HDR + (size_t)(ctx->items[idxs[i]].nsums * 8);
        if (take > 0 && bytes + need > max_bytes)
            break;
        bytes += need;
        take++;
    }
    *more_out = take < nmatch;
    struct cs_objlist_item *out = take ? calloc(take, sizeof *out) : NULL;
    for (uint32_t i = 0; i < take; i++) {
        const struct cs_objlist_item *src = &ctx->items[idxs[i]];
        out[i] = *src;
        out[i].sums = malloc((size_t)src->nsums * sizeof(uint64_t));
        memcpy(out[i].sums, src->sums, (size_t)src->nsums * sizeof(uint64_t));
    }
    *items_out = out;
    *n_out = take;
    return 0;
}

static void mock_reopen_fn(void *arg, uint32_t volume, const uint8_t oid[16])
{
    struct mock_objlist_ctx *ctx = arg;
    pthread_mutex_lock(&ctx->mu);
    ctx->reopen_calls++;
    ctx->reopen_vol = volume;
    memcpy(ctx->reopen_oid, oid, 16);
    pthread_mutex_unlock(&ctx->mu);
}

static void mock_read_pace_fn(void *arg, uint32_t volume, size_t bytes)
{
    struct mock_objlist_ctx *ctx = arg;
    pthread_mutex_lock(&ctx->mu);
    ctx->pace_calls++;
    ctx->pace_vol = volume;
    ctx->pace_bytes += bytes;
    pthread_mutex_unlock(&ctx->mu);
}

static void test_objlist_reopen_chunkr(void)
{
    char dir[PATH_MAX], st[PATH_MAX + 16], err[256];
    fresh("objlist", dir, sizeof dir);
    mkstore(dir, 0, 2, true, st, sizeof st);
    struct chunksrv *b = open1(st);
    struct chunksrv *a = chunksrv_open(NULL, 0, err, sizeof err);
    CHECK(a && b);
    if (!a || !b)
        return;

    /* Without objlist handler, CS_OP_OBJLIST returns TSDB_ERR_UNAVAILABLE. */
    struct cs_req r = {.rid = 11, .op = CS_OP_OBJLIST, .volume = 1, .off = 5, .len = 2};
    cap cp = {0};
    CHECK(fake_req(b, &r, &cp) == TSDB_ERR_UNAVAILABLE);

    struct mock_objlist_ctx ctx;
    memset(&ctx, 0, sizeof ctx);
    pthread_mutex_init(&ctx.mu, NULL);
    ctx.n = 5;
    for (uint32_t i = 0; i < 5; i++) {
        memset(ctx.items[i].oid, 0, 16);
        ctx.items[i].oid[15] = (uint8_t)(i + 1);
        CHECK(chunk_geom_make(&ctx.items[i].g, 3, 2, 4096, 4096 * 3 * 2 + 17) == 0);
        ctx.items[i].positions = 0x1fu;
        ctx.items[i].writer_epoch = 3 + i;
        ctx.items[i].stripe_version = i < 4 ? 1 : 5; /* item 4 is at version 5, filtered out by max_version=5 */
        ctx.items[i].nsums = chunk_sums_total(&ctx.items[i].g);
        ctx.items[i].sums = malloc((size_t)ctx.items[i].nsums * sizeof(uint64_t));
        for (uint64_t s = 0; s < ctx.items[i].nsums; s++)
            ctx.items[i].sums[s] = 0x10000000ull * (i + 1) + s;
    }
    chunksrv_set_objlist(b, mock_objlist_fn, &ctx);
    chunksrv_set_reopen(b, mock_reopen_fn, &ctx);
    chunksrv_set_read_pacer(b, mock_read_pace_fn, &ctx);

    /* Paginated OBJLIST with max_version=5 (r.off) and page size 2: expects 2 + 2 = 4 items (item 4 filtered out). */
    uint32_t got_total = 0;
    bool has_cur = false;
    uint8_t cur[16] = {0};
    for (int pg = 0; pg < 5; pg++) {
        struct cs_req lr = {.rid = 20 + (uint64_t)pg, .op = CS_OP_OBJLIST, .volume = 1, .off = 5, .len = 2,
                            .has_cursor = has_cur};
        memcpy(lr.oid, cur, 16);
        CHECK(fake_req(b, &lr, &cp) == 0);
        uint8_t vu[16] = {0};
        struct cs_objlist_item *dec = NULL;
        uint32_t ndec = 0;
        bool more = false;
        CHECK(cs_objlist_resp_decode(cp.body, cp.body_len, vu, &dec, &ndec, &more) == 0);
        CHECK(memcmp(vu, VUUID, 16) == 0);
        CHECK(ndec == 2);
        for (uint32_t j = 0; j < ndec; j++) {
            uint32_t idx = got_total + j;
            CHECK(memcmp(dec[j].oid, ctx.items[idx].oid, 16) == 0);
            CHECK(dec[j].writer_epoch == ctx.items[idx].writer_epoch);
            CHECK(dec[j].stripe_version == ctx.items[idx].stripe_version);
            CHECK(dec[j].positions == ctx.items[idx].positions);
            CHECK(dec[j].nsums == ctx.items[idx].nsums);
            CHECK(memcmp(dec[j].sums, ctx.items[idx].sums, (size_t)dec[j].nsums * sizeof(uint64_t)) == 0);
        }
        got_total += ndec;
        if (ndec > 0) {
            memcpy(cur, dec[ndec - 1].oid, 16);
            has_cur = true;
        }
        cs_objlist_items_free(dec, ndec);
        if (!more)
            break;
    }
    CHECK(got_total == 4);

    /* Error propagation from objlist callback (e.g. OOM). */
    ctx.force_rc = TSDB_ERR_NOMEM;
    CHECK(fake_req(b, &r, &cp) == TSDB_ERR_NOMEM);
    ctx.force_rc = 0;

    /* CS_OP_REOPEN invokes reopen callback with exact volume & oid. */
    struct cs_req rr = {.rid = 30, .op = CS_OP_REOPEN, .volume = 7};
    memcpy(rr.oid, ctx.items[2].oid, 16);
    CHECK(fake_req(b, &rr, &cp) == 0);
    CHECK(ctx.reopen_calls == 1 && ctx.reopen_vol == 7 && memcmp(ctx.reopen_oid, ctx.items[2].oid, 16) == 0);
    free(cp.body);

    /* Real peerlink: PL_KIND_CHUNKR routes through dedicated rebuild pool + invokes read_pacer with (volume, len),
     * while foreground PL_KIND_CHUNK does NOT invoke read_pacer. */
    static const uint8_t secret[32] = {5};
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

    cdata c;
    cdata_make(&c, 4, 0, 4096, 4096 * 3 * 2 + 19, 123);
    int fd = chunksrv_stage(dir, &c.m.g, c.m.pos, c.cells, c.sums);
    CHECK(fd >= 0);
    CHECK(chunksrv_put_remote(a, 2, &c.m, fd, 0, cs_sums_hash(c.sums, c.ncells), 10000) == 0);
    close(fd);

    /* Foreground PL_KIND_CHUNK read: succeeds, read_pacer NOT called. */
    struct cs_req rd;
    req_of(&rd, CS_OP_READ, &c, 4);
    rd.off = 0;
    rd.len = (uint32_t)c.len;
    int32_t prc = -1;
    uint8_t *body = NULL;
    size_t bl = 0;
    CHECK(chunksrv_call(a, 2, &rd, 10000, &prc, &body, &bl) == 0 && prc == 0 && bl == c.len &&
          memcmp(body, c.cells, c.len) == 0);
    free(body);
    body = NULL;
    pthread_mutex_lock(&ctx.mu);
    CHECK(ctx.pace_calls == 0);
    pthread_mutex_unlock(&ctx.mu);

    /* Rebuild PL_KIND_CHUNKR read: succeeds, read_pacer called with (volume=1, bytes=c.len). */
    prc = -1;
    CHECK(chunksrv_call_kind(a, 2, PL_KIND_CHUNKR, &rd, 10000, &prc, &body, &bl) == 0 && prc == 0 && bl == c.len &&
          memcmp(body, c.cells, c.len) == 0);
    free(body);
    body = NULL;
    pthread_mutex_lock(&ctx.mu);
    CHECK(ctx.pace_calls == 1 && ctx.pace_vol == c.m.volume && ctx.pace_bytes == c.len);
    pthread_mutex_unlock(&ctx.mu);

    /* Remote CS_OP_REOPEN over real peerlink. */
    prc = -1;
    CHECK(chunksrv_call(a, 2, &rr, 10000, &prc, &body, &bl) == 0 && prc == 0);
    free(body);
    pthread_mutex_lock(&ctx.mu);
    CHECK(ctx.reopen_calls == 2);
    pthread_mutex_unlock(&ctx.mu);

    cdata_free(&c);
    for (uint32_t i = 0; i < 5; i++)
        free(ctx.items[i].sums);
    chunksrv_close(a);
    chunksrv_close(b);
    pthread_mutex_destroy(&ctx.mu);
}

int main(int argc, char **argv)
{
    const char *mutant = NULL, *base = "/tmp";
    if (getenv("TSDB_CHUNKSRV_MUTANT"))
        chunksrv_mutant = chunksrv_mutant_parse(getenv("TSDB_CHUNKSRV_MUTANT"));
    if (argc >= 4 && strcmp(argv[1], "--child") == 0)
        return child_put(argv[2], argv[3]);
    if (argc >= 3 && strcmp(argv[1], "--child-eio") == 0)
        return child_eio(argv[2]);
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = true;
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else if (!strcmp(argv[i], "--net"))
            net_only = true;
        else if (!strcmp(argv[i], "--check") && i + 1 < argc) {
            /* the links' frame check level (plcheck.h): every peerlink of the process takes it (chunksrv's pl_cfg leaves
             * its own at 0); the net cases must hold at every level */
            int lv = peerlink_check_parse(argv[++i]);
            if (lv < 0) {
                fprintf(stderr, "test_chunksrv: no check level %s\n", argv[i]);
                return 2;
            }
            peerlink_set_check_level(lv);
            check_level = lv;
            check_name = argv[i];
        } else
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
    printf("test_chunksrv%s%s%s%s%s\n", quick ? " (quick)" : "", mutant ? " mutant " : "", mutant ? mutant : "",
           check_name ? " check " : "", check_name ? check_name : "");
    if (net_only) { /* --net: the cases on real peer links only (the Makefile runs them at each check level) */
        test_net();
        goto out;
    }
    test_wire();
    test_put_read();
    test_bad_cell();
    test_fences();
    test_unsure();
    test_unsure_two();
    test_list_copies();
    test_bounds();
    test_owner();
    test_crashfs();
    test_eio();
    test_net();
    test_objlist_reopen_chunkr();
    test_slow_disk();
out:
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
