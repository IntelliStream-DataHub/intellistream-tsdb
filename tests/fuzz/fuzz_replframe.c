/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The data links' frame decoders (server/peerlink.c) on arbitrary bytes: the stream splitter with its length and
 * type checks, the zstd frames of a compressed log frame (a raw length that is not the decoded size, a bomb), the
 * link's own frame bodies (BEGIN, RESUME, CHUNK, END, DONE), the HELLO that carries the link kind, and the streaming
 * xxh64 against the one-shot, and the replication driver's payloads (server/replwire.c).
 *
 * Input: byte 0 picks the mode (mod 10), the rest is the data:
 *   0  a byte stream of frames, checks not verified (byte 1 mod 3 picks the level: none, header, full, which sets
 *      the check's length), fed in pieces of byte 1 / 3 + 1 bytes: any bytes, no crash, nothing read outside a frame
 *   1  one frame body: byte 1 is the type, the rest the payload
 *   2  a round trip at the level byte 1 / 111 picks (none, header, full): the data is encoded as a (compressed) frame
 *      of type 1 + byte 1 mod 111 and must come back exactly, its check verified; then each of its last 64 bytes
 *      flipped: under full the frame never arrives, under header it arrives only with its type, length and payload
 *      head intact (a check byte flipped never), under none whatever arrives, nothing crashes
 *   3  a HELLO body
 *   4  the data hashed whole and in two pieces (cut at byte 1 mod len) against xxh64()
 *   5  a replication message (RW_T_MSG); one that decodes must encode back to the same bytes, and with any bit of
 *      what its driver check covers flipped (byte 1 picks it) must not decode; bit 0 of byte 1 seals the data with
 *      its right check first, so the decoder behind the check is reached too
 *   6  a FETCH body, then the same as a bulk meta; each that decodes must encode back to the same bytes, and must not
 *      decode with a bit flipped; bits 0 and 1 of byte 1 seal it as a FETCH or a bulk meta first
 *   7  a replication message decoded with room for byte 1 records only (sealed when the check's place is found)
 *   8  a forwarding payload (server/fwd.h): bit 0 of byte 1 FWDQ, bits 1-2 the type
 *   9  a chunk server payload (server/chunksrv.h), byte 1 mod 3: a request, an answer, a put's meta; a request and a
 *      meta that decode must encode back to the same bytes
 */
#include "fuzz.h"
#include "internal.h"
#include "peerlink.h"
#include "plcheck.h"
#include "replwire.h"
#include "fwd.h"
#include "chunksrv.h"

static const uint64_t SALT = 0x0102030405060708ull; /* a direction's salt (plcheck.h) */

struct cnt {
    size_t frames;
    const uint8_t *lo, *hi; /* the input: a payload pointer must lie inside it or inside a decode buffer */
};

struct grab {
    struct pl_msg *m;
    uint8_t *copy;
};

static int grab_msg(void *a, const struct pl_msg *m)
{
    struct grab *b = a;
    *b->m = *m;
    b->copy = malloc(m->n ? m->n : 1);
    if (!b->copy)
        abort();
    memcpy(b->copy, m->p, m->n);
    return 0;
}

static int on_msg(void *arg, const struct pl_msg *m)
{
    struct cnt *c = arg;
    c->frames++;
    if (m->type >= 1 && m->type <= PL_USER_TYPE_MAX) {
        if (m->n > PL_FRAME_MAX)
            abort();
        /* touch every byte: an overread is the sanitizer's to find */
        uint8_t x = 0;
        for (size_t i = 0; i < m->n; i++)
            x ^= m->p[i];
        (void)x;
    } else if (m->type == PLT_CHUNK) {
        if (m->data_len == 0 || m->data_len > PL_FRAME_MAX || m->data != m->p + 16)
            abort();
    } else if (m->type == PLT_BEGIN) {
        if (m->data_len > PL_META_MAX || m->data_len != m->b)
            abort();
    }
    return 0;
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 2)
        return 0;
    int mode = data[0] % 10;
    uint8_t arg = data[1];
    size_t n = size - 2;
    /* an exact-size copy: a read past the end is the sanitizer's to find */
    uint8_t *p = malloc(n ? n : 1);
    if (!p)
        return 0;
    memcpy(p, data + 2, n);
    struct cnt c = {0, p, p + n};
    if (mode == 0) {
        /* bit 7 of arg: verify the level's check (review F6: the header check hashes the decompressed head of any
         * frame, so its zstd decode runs on unchecked bytes and must be safe there); else skip it and reach the
         * decoders behind it */
        struct pl_reader *r = pl_reader_new(PL_CHECK_NONE + (arg & 0x7f) % 3, SALT, !(arg & 0x80));
        size_t step = (size_t)(arg & 0x7f) / 3 + 1;
        for (size_t off = 0; off < n; off += step)
            if (pl_reader_feed(r, p + off, n - off < step ? n - off : step, on_msg, &c) != 0)
                break;
        pl_reader_free(r);
    } else if (mode == 1) {
        struct pl_msg m;
        uint8_t *own = NULL;
        if (pl_msg_decode(arg, p, n, &m, &own) == 0)
            on_msg(&c, &m);
        free(own);
    } else if (mode == 2) {
        uint8_t type = (uint8_t)(1 + arg % PL_USER_TYPE_MAX);
        int ck = PL_CHECK_NONE + arg / PL_USER_TYPE_MAX; /* 0..255 / 111: none, header, full */
        size_t cl = pl_check_len(ck);
        uint64_t seq = 0;
        uint8_t *fr;
        size_t fl = pl_frame_encode(ck, SALT, &seq, 1, type, p, n, &fr);
        if (!fl)
            abort();
        struct pl_reader *r = pl_reader_new(ck, SALT, 0);
        struct pl_msg got = {0};
        struct grab box = {&got, NULL};
        if (pl_reader_feed(r, fr, fl, grab_msg, &box) != 0 || !box.copy || got.type != type || got.n != n ||
            (n && memcmp(box.copy, p, n)))
            abort();
        free(box.copy);
        /* a flipped bit in its last 64 bytes: what the level's check covers ends the stream */
        for (size_t i = 0; i < fl && i < 64; i++) {
            uint8_t *bad = malloc(fl);
            memcpy(bad, fr, fl);
            bad[fl - 1 - i] ^= 1;
            struct pl_reader *r2 = pl_reader_new(ck, SALT, 0);
            struct pl_msg g2 = {0};
            struct grab b2 = {&g2, NULL};
            int ok = pl_reader_feed(r2, bad, fl, grab_msg, &b2) == 0 && b2.copy;
            size_t h = n < PL_CHECK_SPAN ? n : PL_CHECK_SPAN;
            if (ok && (ck == PL_CHECK_FULL || (ck == PL_CHECK_HEADER && i < cl)))
                abort();
            if (ok && ck == PL_CHECK_HEADER && (g2.type != type || g2.n != n || (h && memcmp(b2.copy, p, h))))
                abort();
            free(b2.copy);
            pl_reader_free(r2);
            free(bad);
        }
        pl_reader_free(r);
        free(fr);
    } else if (mode == 5 || mode == 7) {
        uint32_t cap = mode == 7 ? arg : 4096;
        struct repl_rec *recs = malloc((cap ? cap : 1) * sizeof *recs);
        uint32_t *lens = malloc((cap ? cap : 1) * sizeof *lens);
        struct rw_msg m;
        size_t at = rw_msg_check_at(p, n);
        if (at && (mode == 7 || (arg & 1))) { /* sealed: the decoder behind the check */
            uint64_t ck = rw_check(RW_K_MSG, p, at);
            memcpy(p + at, &ck, 8);
        }
        if (rw_decode_msg(p, n, &m, recs, lens, cap) == 0) {
            if (!at || m.m.nrec > cap || (m.nbytes && (m.bytes < p || m.bytes + m.nbytes != p + n)))
                abort();
            uint8_t *back = malloc(n ? n : 1);
            size_t bl = rw_encode_msg(back, n, m.volume, &m.m, m.lens, m.seq, m.off, m.bytes, m.nbytes);
            if (bl != n || memcmp(back, p, n) != 0)
                abort();
            free(back);
            /* one bit of what the check covers (the check included) flipped: never a message */
            size_t bit = ((size_t)arg * 2654435761u) % ((at + 8) * 8);
            p[bit / 8] ^= (uint8_t)(1u << (bit % 8));
            if (rw_decode_msg(p, n, &m, recs, lens, cap) == 0)
                abort();
        }
        free(recs);
        free(lens);
    } else if (mode == 6) {
        uint32_t vol;
        uint64_t size;
        char name[RW_NAME_MAX + 1];
        uint8_t back[RW_NAME_MAX + 32];
        uint64_t fpos;
        if (n > 8 && (arg & 3)) { /* sealed as a FETCH (bit 0) or a bulk meta (bit 1) */
            uint64_t ck = rw_check(arg & 1 ? RW_K_FETCH : RW_K_META, p, n - 8);
            memcpy(p + n - 8, &ck, 8);
        }
        size_t bit = n ? ((size_t)arg * 2654435761u) % (n * 8) : 0;
        if (rw_decode_fetch(p, n, &vol, &fpos, name, sizeof name) == 0) {
            size_t bl = rw_encode_fetch(back, sizeof back, vol, fpos, name);
            if (bl != n || memcmp(back, p, n) != 0)
                abort();
            p[bit / 8] ^= (uint8_t)(1u << (bit % 8));
            if (rw_decode_fetch(p, n, &vol, &fpos, name, sizeof name) == 0)
                abort();
            p[bit / 8] ^= (uint8_t)(1u << (bit % 8));
        }
        if (rw_decode_bulk_meta(p, n, &vol, &size, name, sizeof name) == 0) {
            size_t bl = rw_encode_bulk_meta(back, sizeof back, vol, size, name);
            if (bl != n || memcmp(back, p, n) != 0 || n > PL_META_MAX)
                abort();
            p[bit / 8] ^= (uint8_t)(1u << (bit % 8));
            if (rw_decode_bulk_meta(p, n, &vol, &size, name, sizeof name) == 0)
                abort();
        }
    } else if (mode == 8) {
        /* the forwarding links' payloads (fwd.h): FWD or FWDQ by bit 0 of arg, the type by the next three */
        int kind = arg & 1 ? PL_KIND_FWDQ : PL_KIND_FWD;
        uint8_t type = (uint8_t)((arg >> 1) & 7);
        struct fwd_wire w;
        int drc = fwd_wire_decode(kind, type, p, n, &w);
        if (drc == FWD_E_IDCHECK) { /* only a header whose check word is not its id's */
            uint8_t hd[FWD_HDR];
            if (n < FWD_HDR)
                abort();
            fwd_wire_head(hd, w.frame_id);
            if (!memcmp(hd, p, FWD_HDR))
                abort();
        }
        if (drc == 0) {
            uint8_t hd[FWD_HDR];
            if (n < FWD_HDR)
                abort();
            fwd_wire_head(hd, w.frame_id); /* accepted: the id is the first 8 bytes and the word matches it */
            if (memcmp(hd, p, FWD_HDR) != 0)
                abort();
            if (w.n && (w.body < p || w.body + w.n != p + n))
                abort();
            uint8_t x = 0;
            for (size_t i = 0; i < w.n; i++)
                x ^= w.body[i];
            (void)x;
            if (kind == PL_KIND_FWDQ && type == FWDQ_T_CHUNK && w.n > FWD_CHUNK_MAX)
                abort();
            if (kind == PL_KIND_FWDQ && type == FWDQ_T_CANCEL && (n != FWD_HDR || w.n))
                abort();
            if (kind == PL_KIND_FWDQ && type == FWDQ_T_CREDIT) {
                uint8_t back[FWD_CREDIT_LEN];
                if (fwd_wire_credit(back, w.frame_id, w.chunks) != n || memcmp(back, p, n) != 0)
                    abort();
            }
        }
    } else if (mode == 9) {
        int w = arg % 3;
        if (w == 0) {
            struct cs_req r;
            if (cs_req_decode(p, n, &r) == 0) {
                if (r.op < CS_OP_READ || r.op > CS_OP_DEL_REL || r.op == CS_OP_PUT || r.pos >= CHUNK_POS_MAX)
                    abort();
                uint32_t max = r.op == CS_OP_READ      ? CS_READ_MAX
                               : r.op == CS_OP_SUMS    ? CS_SUMS_MAX
                               : r.op == CS_OP_OBJLIST ? CS_OBJLIST_MAX
                                                       : CS_LIST_MAX;
                if ((r.op == CS_OP_READ || r.op == CS_OP_SUMS || r.op == CS_OP_LIST || r.op == CS_OP_OBJLIST) &&
                    (r.len == 0 || r.len > max))
                    abort();
                uint8_t back[128];
                if (cs_req_encode(&r, back, sizeof back) != n || memcmp(back, p, n) != 0)
                    abort();
            }
        } else if (w == 1) {
            struct cs_resp r;
            if (cs_resp_decode(p, n, &r) == 0) {
                if (r.rc > 0 || r.op < CS_OP_READ || r.op > CS_OP_DEL_REL)
                    abort();
                if (r.body_len && (r.body < p || r.body + r.body_len != p + n))
                    abort();
                if (r.op == CS_OP_READ && r.body_len > CS_READ_MAX)
                    abort();
                uint8_t x = 0;
                for (size_t i = 0; i < r.body_len; i++)
                    x ^= r.body[i];
                (void)x;
                (void)chunksrv_st(r.rc);
            }
        } else {
            struct cs_put_meta m;
            if (cs_put_meta_decode(p, n, &m) == 0) {
                uint8_t back[CS_PUT_META_LEN];
                cs_put_meta_encode(&m, back);
                if (n != CS_PUT_META_LEN || memcmp(back, p, n) != 0 || m.pos >= chunk_npos(&m.g))
                    abort();
                (void)cs_put_total(&m);
            }
        }
    } else if (mode == 3) {
        int k = peerlink_hello_kind(p, n);
        if (k < -1 || k > 255)
            abort();
    } else {
        struct pl_xxh h;
        pl_xxh_init(&h);
        size_t cut = n ? (size_t)arg % (n + 1) : 0;
        pl_xxh_update(&h, p, cut);
        pl_xxh_update(&h, p + cut, n - cut);
        if (pl_xxh_final(&h) != xxh64(p, n, 0))
            abort();
    }
    free(p);
    return 0;
}

static void put_frame(const char *dir, const char *name, int mode, uint8_t arg, const void *body, size_t n)
{
    if (n > (1u << 24))
        abort();
    uint8_t *b = malloc(n + 2);
    if (!b)
        abort();
    b[0] = (uint8_t)mode;
    b[1] = arg;
    if (n)
        memcpy(b + 2, body, n);
    fuzz_put(dir, name, b, n + 2);
    free(b);
}

static void le(uint8_t *p, uint64_t v, int bytes)
{
    for (int i = 0; i < bytes; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

void fuzz_seeds(const char *dir)
{
    uint8_t b[256] = {0};
    /* BEGIN: file 7, total 100, meta "m" */
    le(b, 7, 8), le(b + 8, 100, 8), le(b + 16, 1, 4), b[20] = 'm';
    put_frame(dir, "seed-begin", 1, PLT_BEGIN, b, 21);
    memset(b, 0, sizeof b);
    le(b, 7, 8), le(b + 8, 0, 4), le(b + 12, 64, 8);
    put_frame(dir, "seed-resume", 1, PLT_RESUME, b, 20);
    le(b, 7, 8), le(b + 8, 64, 8), memset(b + 16, 0xab, 32);
    put_frame(dir, "seed-chunk", 1, PLT_CHUNK, b, 48);
    le(b, 7, 8), le(b + 8, 100, 8), le(b + 16, 0x1234, 8);
    put_frame(dir, "seed-end", 1, PLT_END, b, 24);
    le(b, 7, 8), le(b + 8, 0, 4);
    put_frame(dir, "seed-done", 1, PLT_DONE, b, 12);
    put_frame(dir, "seed-ping", 1, PLT_PING, "", 0);
    /* a compressed user frame: raw length 300, then a zstd frame (the round trip makes real ones) */
    uint8_t z[64] = {44, 1, 0, 0, 0x28, 0xb5, 0x2f, 0xfd};
    put_frame(dir, "seed-zstd-bad", 1, 0x80 | 5, z, sizeof z);
    /* a stream of two frames at level none (byte 1 = 6: none, pieces of 3 bytes): len 1, type PING; then a user
     * frame; and the same at full (byte 1 = 8: 16 check bytes, not verified, pieces of 3) */
    uint8_t s[200] = {0};
    le(s, 1, 4), s[4] = PLT_PING;
    le(s + 5, 6, 4), s[9] = 9, memset(s + 10, 0x55, 5);
    put_frame(dir, "seed-stream", 0, 6, s, 5 + 4 + 6);
    memset(s, 0, sizeof s);
    le(s, 1 + 16, 4), s[4] = PLT_PING;
    le(s + 21, 6 + 16, 4), s[25] = 9, memset(s + 26, 0x55, 5);
    put_frame(dir, "seed-stream-full", 0, 8, s, 21 + 4 + 6 + 16);
    uint8_t text[300];
    for (size_t i = 0; i < sizeof text; i++)
        text[i] = (uint8_t)("tsdb log frame "[i % 15]);
    put_frame(dir, "seed-roundtrip-text", 2, 4, text, sizeof text);
    put_frame(dir, "seed-roundtrip-small", 2, 1, "x", 1);
    put_frame(dir, "seed-roundtrip-text-header", 2, 111 + 4, text, sizeof text);
    put_frame(dir, "seed-roundtrip-text-full", 2, 222 + 4, text, sizeof text);
    { /* a verified stream at header (byte 1 = 0x80 | 4: verify, header, pieces of 2): two frames whose checks hold,
       * the first compressed, so the fuzzer starts from frames that reach the check of a decompressed head */
        uint64_t seq = 0;
        uint8_t *f1, *f2;
        size_t l1 = pl_frame_encode(PL_CHECK_HEADER, SALT, &seq, 1, 4, text, sizeof text, &f1);
        size_t l2 = pl_frame_encode(PL_CHECK_HEADER, SALT, &seq, 1, 5, "x", 1, &f2);
        uint8_t *st = l1 && l2 ? malloc(l1 + l2) : NULL;
        if (!st)
            abort();
        memcpy(st, f1, l1), memcpy(st + l1, f2, l2);
        put_frame(dir, "seed-stream-header-verified", 0, 0x80 | 4, st, l1 + l2);
        free(st), free(f1), free(f2);
    }
    uint8_t h[80] = {0};
    le(h, PL_CL_MAGIC, 4), h[4] = PL_CL_VERSION, le(h + 5, 2, 4), le(h + 73, 0, 2), h[75] = PL_KIND_LOG;
    h[76] = PL_CHECKS_KNOWN, h[77] = PL_CAPS;
    put_frame(dir, "seed-hello", 3, 0, h, 78);
    h[75] = PL_KIND_FWDQ;
    put_frame(dir, "seed-hello-fwdq", 3, 0, h, 78);
    /* the forwarding payloads (mode 8; arg: bit 0 FWDQ, bits 1-2 the type) */
    uint8_t fw[48] = {0};
    fwd_wire_credit(fw, 0x1122334455667788ull, 4);
    put_frame(dir, "seed-fwd-credit", 8, 1 | FWDQ_T_CREDIT << 1, fw, FWD_CREDIT_LEN);
    fwd_wire_head(fw, 21), fw[FWD_HDR] = 1, memset(fw + FWD_HDR + 1, 0x5a, 16);
    put_frame(dir, "seed-fwd-chunk", 8, 1 | FWDQ_T_CHUNK << 1, fw, FWD_HDR + 17);
    fwd_wire_head(fw, 22), memcpy(fw + FWD_HDR, "select", 6);
    put_frame(dir, "seed-fwd-qreq", 8, 1 | FWDQ_T_REQ << 1, fw, FWD_HDR + 6);
    fwd_wire_head(fw, 23), memcpy(fw + FWD_HDR, "insert", 6);
    put_frame(dir, "seed-fwd-req", 8, FWD_T_REQ << 1, fw, FWD_HDR + 6);
    put_frame(dir, "seed-fwd-rep", 8, FWD_T_REP << 1, fw, FWD_HDR + 6);
    fw[0] ^= 1; /* 22: its check word is 23's */
    put_frame(dir, "seed-fwd-rep-idflip", 8, FWD_T_REP << 1, fw, FWD_HDR + 6);
    fwd_wire_head(fw, 24);
    put_frame(dir, "seed-fwd-cancel", 8, 1 | FWDQ_T_CANCEL << 1, fw, FWD_HDR);
    put_frame(dir, "seed-xxh", 4, 3, text, sizeof text);
    /* replication messages: an ACK, and an APPEND of two records (5 and 1 positions) of 40 and 32 bytes */
    static uint8_t mb[RW_HEAD + RW_CHECK + RW_APPEND_HEAD + 2 * RW_DESC + 72];
    struct repl_msg am = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 3, .leader = -1, .durable = 90,
                          .written = 95, .rid = 77, .start = 1, .meta.leader = -1, .set_version = 5,
                          .replay = 40};
    size_t ml = rw_encode_msg(mb, sizeof mb, 4, &am, NULL, 0, 0, NULL, 0);
    if (!ml)
        abort();
    put_frame(dir, "seed-rw-ack", 5, 0, mb, ml);
    struct repl_rec rr[2] = {{.pos = 11, .epoch = 3, .n = 5}, {.pos = 16, .epoch = 3, .n = 1}};
    uint32_t rl[2] = {40, 32};
    struct repl_msg ap = {.type = REPL_M_APPEND, .from = 0, .to = 2, .epoch = 3, .leader = 0, .prev_pos = 10,
                          .prev_epoch = 3, .commit = 9, .nrec = 2, .rec = rr, .meta.leader = -1};
    ml = rw_encode_msg(mb, sizeof mb, 4, &ap, rl, 1ull << 40, 4096, text, 72);
    if (!ml)
        abort();
    put_frame(dir, "seed-rw-append", 5, 0, mb, ml);
    put_frame(dir, "seed-rw-append-cap1", 7, 1, mb, ml);
    put_frame(dir, "seed-rw-append-sealed", 5, 0x81, mb, ml);
    mb[RW_HEAD + RW_APPEND_HEAD + RW_DESC + 8] ^= 1; /* descriptor 1's epoch: the check refuses it */
    put_frame(dir, "seed-rw-append-desc1flip", 5, 0x80, mb, ml);
    uint8_t fb[64];
    size_t fl2 = rw_encode_fetch(fb, sizeof fb, 4, 4242, "p19000_1_2_0.part");
    if (!fl2)
        abort();
    put_frame(dir, "seed-rw-fetch", 6, 0, fb, fl2);
    put_frame(dir, "seed-rw-fetch-sealed", 6, 0x41, fb, fl2);
    fl2 = rw_encode_bulk_meta(fb, sizeof fb, 4, 123456, "p19000_1_2_0.part");
    if (!fl2)
        abort();
    put_frame(dir, "seed-rw-bulkmeta", 6, 0, fb, fl2);
    /* the chunk server's payloads (mode 9) */
    uint8_t cb[128];
    struct cs_req cr = {.rid = 5, .op = CS_OP_READ, .volume = 3, .epoch = 7, .pos = 2, .off = 4096, .len = 100};
    memset(cr.vuuid, 0x11, 16), memset(cr.oid, 0x22, 16);
    size_t cl = cs_req_encode(&cr, cb, sizeof cb);
    put_frame(dir, "seed-cs-read", 9, 0, cb, cl);
    cr.op = CS_OP_LIST, cr.has_cursor = 1, cr.len = 64;
    cl = cs_req_encode(&cr, cb, sizeof cb);
    put_frame(dir, "seed-cs-list", 9, 0, cb, cl);
    cr.op = CS_OP_EPOCH;
    cl = cs_req_encode(&cr, cb, sizeof cb);
    put_frame(dir, "seed-cs-epoch", 9, 0, cb, cl);
    cr.op = CS_OP_DAMAGED, cr.pos = 4, cr.bad_cells = 0x15ull;
    cl = cs_req_encode(&cr, cb, sizeof cb);
    put_frame(dir, "seed-cs-damaged", 9, 0, cb, cl);
    memset(cb, 0, sizeof cb); /* an answer: rid 7, DAMAGED, rc 0, empty body */
    le(cb, 7, 8), cb[8] = CS_OP_DAMAGED;
    cs_resp_seal(cb, 21);
    put_frame(dir, "seed-cs-resp-damaged", 9, 1, cb, 21);
    memset(cb, 0, sizeof cb); /* an answer: rid 5, EPOCH, rc 0, fence 9, sure */
    le(cb, 5, 8), cb[8] = CS_OP_EPOCH, le(cb + 21, 9, 8);
    cs_resp_seal(cb, 21 + 9);
    put_frame(dir, "seed-cs-resp-epoch", 9, 1, cb, 21 + 9);
    memset(cb, 0, sizeof cb); /* SUMS: header of 36 bytes, 2 sums */
    le(cb, 6, 8), cb[8] = CS_OP_SUMS, le(cb + 21 + 32, 2, 4);
    cs_resp_seal(cb, 21 + 36 + 16);
    put_frame(dir, "seed-cs-resp-sums", 9, 1, cb, 21 + 36 + 16);
    struct cs_put_meta pm = {.volume = 3, .epoch = 7, .pos = 1};
    memset(pm.vuuid, 0x11, 16), memset(pm.oid, 0x22, 16);
    if (chunk_geom_make(&pm.g, 4, 2, 4096, 100000) != 0)
        abort();
    cs_put_meta_encode(&pm, cb);
    put_frame(dir, "seed-cs-putmeta", 9, 2, cb, CS_PUT_META_LEN);
}
