/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The data links' frame decoders (server/peerlink.c) on arbitrary bytes: the stream splitter with its length and
 * type checks, the zstd frames of a compressed log frame (a raw length that is not the decoded size, a bomb), the
 * link's own frame bodies (BEGIN, RESUME, CHUNK, END, DONE), the HELLO that carries the link kind, and the streaming
 * xxh64 against the one-shot. r3d-stream extends this harness with the replication message decoder.
 *
 * Input: byte 0 picks the mode (mod 5), the rest is the data:
 *   0  a byte stream of frames, tags not checked, fed in pieces whose sizes come from byte 1
 *   1  one frame body: byte 1 is the type, the rest the payload
 *   2  a round trip: the data is encoded as a (compressed) frame of type byte 1 and must come back exactly, with
 *      its tag checked
 *   3  a HELLO body
 *   4  the data hashed whole and in two pieces (cut at byte 1 mod len) against xxh64()
 */
#include "fuzz.h"
#include "internal.h"
#include "peerlink.h"

static const uint8_t KEY[32] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

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
    int mode = data[0] % 5;
    uint8_t arg = data[1];
    size_t n = size - 2;
    /* an exact-size copy: a read past the end is the sanitizer's to find */
    uint8_t *p = malloc(n ? n : 1);
    if (!p)
        return 0;
    memcpy(p, data + 2, n);
    struct cnt c = {0, p, p + n};
    if (mode == 0) {
        struct pl_reader *r = pl_reader_new(KEY, 1);
        size_t step = (size_t)arg + 1;
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
        uint64_t seq = 0;
        uint8_t *fr;
        size_t fl = pl_frame_encode(KEY, &seq, 1, type, p, n, &fr);
        if (!fl)
            abort();
        struct pl_reader *r = pl_reader_new(KEY, 0);
        struct pl_msg got = {0};
        struct grab box = {&got, NULL};
        if (pl_reader_feed(r, fr, fl, grab_msg, &box) != 0 || !box.copy || got.type != type || got.n != n ||
            (n && memcmp(box.copy, p, n)))
            abort();
        free(box.copy);
        /* a flipped bit anywhere must end the stream */
        for (size_t i = 0; i < fl && i < 64; i++) {
            uint8_t *bad = malloc(fl);
            memcpy(bad, fr, fl);
            bad[fl - 1 - i] ^= 1;
            struct pl_reader *r2 = pl_reader_new(KEY, 0);
            struct cnt c2 = {0, NULL, NULL};
            if (pl_reader_feed(r2, bad, fl, on_msg, &c2) == 0 && c2.frames)
                abort();
            pl_reader_free(r2);
            free(bad);
        }
        pl_reader_free(r);
        free(fr);
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
    uint8_t *b = malloc(n + 2);
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
    /* a stream of two frames, no tags checked: len 33 (type + 32 tag bytes), type PING; then a user frame */
    uint8_t s[200] = {0};
    le(s, 33, 4), s[4] = PLT_PING;
    le(s + 37, 38, 4), s[41] = 9, memset(s + 42, 0x55, 5);
    put_frame(dir, "seed-stream", 0, 7, s, 37 + 4 + 38);
    uint8_t text[300];
    for (size_t i = 0; i < sizeof text; i++)
        text[i] = (uint8_t)("tsdb log frame "[i % 15]);
    put_frame(dir, "seed-roundtrip-text", 2, 4, text, sizeof text);
    put_frame(dir, "seed-roundtrip-small", 2, 1, "x", 1);
    uint8_t h[80] = {0};
    le(h, PL_CL_MAGIC, 4), h[4] = PL_CL_VERSION, le(h + 5, 2, 4), le(h + 73, 0, 2), h[75] = PL_KIND_LOG;
    put_frame(dir, "seed-hello", 3, 0, h, 76);
    put_frame(dir, "seed-xxh", 4, 3, text, sizeof text);
}
