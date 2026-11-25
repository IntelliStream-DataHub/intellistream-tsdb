/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The data links (server/peerlink.c, docs/design-cluster.md C25): two endpoints in one process over 127.0.0.1, no
 * tsdbd. Usage: test_peerlink SCRATCHDIR [--quick] [--only NAME[,NAME...]] [--mutant NAME] (the planted bugs of peerlink.h's PL_MUT_*, each of which must fail its test)
 * [--check none|header|full] (every node's frame check level, plcheck.h; default none) [--bench].
 *
 *   codec        frames encoded, split at random, decoded at each check level; under full a flipped bit, under header
 *                one in the length, type or the payload's head, and under both a repeated or a swapped frame end the
 *                stream; under none nothing crashes; the streaming xxh64 equals xxh64()
 *   log          log frames arrive in order and whole (random and compressible ones, zstd on the wire); an echo
 *   bulk         a file arrives whole with its xxh64; a metadata and a size are what the sender gave
 *   resume       the connection is cut twice in the middle of a file (an in-test proxy): the file completes from the
 *                receiver's offset, the last attempt sends less than the file
 *   secret       a peer with another secret never comes up; a peer that is not the node id asked for neither
 *   flip         under a check (full, or the run's header), a byte flipped on the wire ends the link and is never
 *                delivered (mutant notag delivers it)
 *   levels       every pair of check levels: the link runs at the stronger, both ends say so, frames go through
 *   blackhole    a pair behind a blackholing proxy exchanges nothing, goes down, and reconnects once healed
 *   backpressure a receiver that stops reading: the sender's queue stops at its bound (mutant nobackpressure does
 *                not), a timed send waits and gives up
 *   latency      with 64 MB of a bulk file in flight to a slow receiver, small log frames keep their round trip
 *                (mutant sharedsocket puts the file on the log link's socket and the round trip is the file's)
 *   fake         the link over tests/pl_fake.h's in-memory transport (server/pltransport.h): frames, held back and
 *                paused past dead_ms, a flipped byte, a bulk file cut mid-stream and resumed, a cut
 *                (every fake case: no down after the transport's close, no attach counted in a handshake;
 *                mutants closefirst nhandall)
 *   fakeready    the `ready` path: an edge-mode fake (one 4 KiB slot per read, drained before the poll, two fds)
 *                moves 2.4 MB both ways in well under its timers' turns (mutant noready)
 *   fakeend      an end reported to a paused link; WRITTEN / NEVER_WRITTEN after short writes into a 4 KiB ring
 *                with the receiver held (mutant wnoshort)
 *
 * The proxy lives here: tests/jepsen/jepsen_cluster.c has r3f's, which is part of that program.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

#include "internal.h"
#include "peerlink.h"
#include "plcheck.h"
#include "fwd.h"
#include "tsdb_crypto.h"
#include "pl_fake.h"

static int failures;
#define CHECK(cond)                                                                                                \
    do {                                                                                                           \
        if (!(cond)) {                                                                                             \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond);                                     \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)

static int quick;
static const char *scratch = "/tmp";

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
static uint64_t now_ms(void) { return now_ns() / 1000000ull; }

/* waits up to `ms` for a condition that other threads make true */
#define WAIT_FOR(cond, ms)                                                                                         \
    ({                                                                                                             \
        uint64_t t_end_ = now_ms() + (ms);                                                                         \
        int ok_ = 0;                                                                                               \
        while (!(ok_ = !!(cond)) && now_ms() < t_end_)                                                             \
            usleep(2000);                                                                                          \
        ok_;                                                                                                       \
    })

static uint64_t rng_next(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return *s = x;
}

static void fill_random(uint8_t *p, size_t n, uint64_t seed)
{
    uint64_t s = seed * 0x9E3779B97F4A7C15ull + 0x1234567ull;
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = rng_next(&s);
        memcpy(p + i, &v, n - i < 8 ? n - i : 8);
    }
}

static void fill_text(uint8_t *p, size_t n, uint64_t seed)
{
    static const char w[] = "temperature,pressure,flow,vibration;";
    for (size_t i = 0; i < n; i++)
        p[i] = (uint8_t)w[(i + seed) % (sizeof w - 1)];
}

static const uint8_t SECRET[32] = "r3d-links test secret 012345678";
static const uint8_t SECRET2[32] = "another cluster secret 98765432";

/* ---------------------------------------------------------------- codec */

struct collect {
    uint8_t type[4096];
    size_t len[4096];
    uint8_t *data[4096];
    int n;
};

static int collect_msg(void *a, const struct pl_msg *m)
{
    struct collect *c = a;
    if (c->n >= 4096)
        return -1;
    c->type[c->n] = m->type;
    c->len[c->n] = m->n;
    c->data[c->n] = malloc(m->n ? m->n : 1);
    memcpy(c->data[c->n], m->p, m->n);
    c->n++;
    return 0;
}

static void collect_free(struct collect *c)
{
    for (int i = 0; i < c->n; i++)
        free(c->data[i]);
    memset(c, 0, sizeof *c);
}

static int feed_split(struct pl_reader *r, const uint8_t *s, size_t n, struct collect *c, uint64_t *rs)
{
    size_t off = 0;
    int rc = 0;
    while (off < n && rc == 0) {
        size_t take = 1 + rng_next(rs) % 3000;
        if (take > n - off)
            take = n - off;
        rc = pl_reader_feed(r, s + off, take, collect_msg, c);
        off += take;
    }
    return rc;
}

/* the frame check against xxHash's own sanity vectors (its tests/sanity_test_vectors.h, XXH128 with seed PRIME64_1 over
 * its generated buffer), at lengths that reach each of XXH3-128's code paths (0, 1-3, 4-8, 9-16, 17-128, 129-240, and
 * the long loop, a block and more): the copy in server/xxhash, compiled at this build's level, is the upstream's hash */
static void test_check_kat(void)
{
    static const struct {
        uint32_t len;
        uint64_t lo, hi;
    } v[] = {
        {0, 0xA986DFC5D7605BFEull, 0x00FEAA732A3CE25Eull},    {3, 0x634B8990B4976373ull, 0x1C7ECF6A308CF00Eull},
        {8, 0x7B29471DC729B5FFull, 0xF50CEC145BCD5C5Aull},    {16, 0x0346D13A7A5498C7ull, 0x6FFCB80CD33085C8ull},
        {100, 0xD5E0C2A715F11657ull, 0x984CEC52A9A9A561ull},  {200, 0x2236D1B483E8D9EBull, 0xCF0349DD7CC2B545ull},
        {240, 0x604E98DB085C1864ull, 0x29D2133D6EA58C5Bull},  {241, 0xDDA9B0A161D4829Aull, 0xEC64AFAE6A137582ull},
        {1024, 0xEF368A8A2EBABAEFull, 0x17600EFE2B493A18ull}, {2048, 0x66F81670669ABABCull, 0x23CC3A2E75EBAAEAull},
        {2367, 0xD2DB3415B942B42Aull, 0xCCB7A94CCA1A6496ull}, {4096, 0x2A3BBB20A5439DCDull, 0x8FBC8FD4D526D1BDull},
    };
    static uint8_t buf[4096];
    uint64_t gen = 2654435761u; /* PRIME32_1, the generator of xxHash's sanity buffer */
    for (size_t i = 0; i < sizeof buf; i++) {
        buf[i] = (uint8_t)(gen >> 56);
        gen *= 11400714785074694797ull; /* PRIME64_1 */
    }
    for (size_t i = 0; i < sizeof v / sizeof v[0]; i++) {
        uint8_t out[16];
        pl_check(0x9E3779B185EBCA8Dull, buf, v[i].len, out);
        uint64_t lo = 0, hi = 0;
        for (int b = 7; b >= 0; b--) {
            lo = lo << 8 | out[b];
            hi = hi << 8 | out[8 + b];
        }
        if (lo != v[i].lo || hi != v[i].hi)
            fprintf(stderr, "XXH3-128 of %u bytes: %016" PRIx64 "%016" PRIx64 ", want %016" PRIx64 "%016" PRIx64 "\n",
                    v[i].len, hi, lo, v[i].hi, v[i].lo);
        CHECK(lo == v[i].lo && hi == v[i].hi);
    }
}

/* the codec at one frame check level (plcheck.h): what a check covers ends the stream when flipped, what it does
 * not cover arrives as it was sent (never a crash, never bytes outside the frame) */
static void test_codec_at(int ck)
{
    size_t cl = pl_check_len(ck);
    const uint64_t salt = 0x5eed5a17c0ffee01ull;
    enum { NF = 60 };
    uint8_t *orig[NF];
    size_t olen[NF];
    uint8_t otype[NF];
    uint8_t *fr[NF];
    size_t flen[NF];
    uint64_t rs = 99, seq = 0;
    for (int i = 0; i < NF; i++) {
        static const size_t sz[] = {0, 1, 7, 80, 255, 256, 1000, 5000, 70000, 300000};
        olen[i] = sz[rng_next(&rs) % 10];
        otype[i] = (uint8_t)(1 + rng_next(&rs) % 0x6e);
        orig[i] = malloc(olen[i] + 1);
        if (rng_next(&rs) & 1)
            fill_random(orig[i], olen[i], (uint64_t)i + 1);
        else
            fill_text(orig[i], olen[i], (uint64_t)i);
        flen[i] = pl_frame_encode(ck, salt, &seq, i % 3 != 0, otype[i], orig[i], olen[i], &fr[i]);
        CHECK(flen[i] != 0);
    }
    size_t total = 0;
    for (int i = 0; i < NF; i++)
        total += flen[i];
    uint8_t *stream = malloc(total);
    size_t off = 0, at[NF];
    for (int i = 0; i < NF; i++) {
        at[i] = off;
        memcpy(stream + off, fr[i], flen[i]);
        off += flen[i];
    }

    /* the whole stream, split at random */
    for (int rep = 0; rep < 5; rep++) {
        struct pl_reader *r = pl_reader_new(ck, salt, 0);
        struct collect c = {0};
        CHECK(feed_split(r, stream, total, &c, &rs) == 0);
        CHECK(c.n == NF);
        for (int i = 0; i < c.n && i < NF; i++)
            CHECK(c.type[i] == otype[i] && c.len[i] == olen[i] && memcmp(c.data[i], orig[i], olen[i]) == 0);
        collect_free(&c);
        pl_reader_free(r);
    }
    /* another connection's salt (the frames of an earlier connection replayed into a new one): nothing is delivered */
    if (ck != PL_CHECK_NONE) {
        struct pl_reader *r = pl_reader_new(ck, salt ^ 1, 0);
        struct collect c = {0};
        CHECK(feed_split(r, stream, total, &c, &rs) < 0 && c.n == 0);
        collect_free(&c);
        pl_reader_free(r);
    }
    /* a flipped bit anywhere: the frames before it come through; under full it and what follows never do, under
     * header the flipped frame may arrive only with its type, length and first PL_CHECK_SPAN bytes intact (a flip in
     * the rest of its payload), under none it may arrive changed (the wire is trusted) but nothing crashes */
    for (int rep = 0; rep < 300; rep++) {
        uint8_t *bad = malloc(total);
        memcpy(bad, stream, total);
        size_t pos = rng_next(&rs) % total;
        bad[pos] ^= (uint8_t)(1u << (rng_next(&rs) % 8));
        int hit = 0;
        while (hit + 1 < NF && at[hit + 1] <= pos)
            hit++;
        struct pl_reader *r = pl_reader_new(ck, salt, 0);
        struct collect c = {0};
        feed_split(r, bad, total, &c, &rs);
        if (ck == PL_CHECK_FULL)
            CHECK(c.n <= hit);
        for (int i = 0; i < c.n && i < NF; i++) {
            if (i < hit || ck == PL_CHECK_FULL || (ck == PL_CHECK_HEADER && i > hit))
                CHECK(c.type[i] == otype[i] && c.len[i] == olen[i] && memcmp(c.data[i], orig[i], olen[i]) == 0);
            else if (ck == PL_CHECK_HEADER && i == hit) {
                size_t h = olen[i] < PL_CHECK_SPAN ? olen[i] : PL_CHECK_SPAN;
                CHECK(c.type[i] == otype[i] && c.len[i] == olen[i] && memcmp(c.data[i], orig[i], h) == 0);
            }
        }
        collect_free(&c);
        pl_reader_free(r);
        free(bad);
    }
    /* a repeated frame and two swapped ones */
    for (int mode = 0; mode < 2 && ck != PL_CHECK_NONE; mode++) {
        int k = 5 + (int)(rng_next(&rs) % 40);
        size_t cut = at[k] + flen[k];
        uint8_t *bad = malloc(total + flen[k]);
        size_t w = 0;
        if (mode == 0) { /* frame k twice */
            memcpy(bad, stream, cut);
            memcpy(bad + cut, stream + at[k], flen[k]);
            memcpy(bad + cut + flen[k], stream + cut, total - cut);
            w = total + flen[k];
        } else { /* frames k and k+1 swapped */
            memcpy(bad, stream, at[k]);
            memcpy(bad + at[k], stream + at[k + 1], flen[k + 1]);
            memcpy(bad + at[k] + flen[k + 1], stream + at[k], flen[k]);
            size_t e = at[k + 1] + flen[k + 1];
            memcpy(bad + e, stream + e, total - e);
            w = total;
        }
        struct pl_reader *r = pl_reader_new(ck, salt, 0);
        struct collect c = {0};
        CHECK(feed_split(r, bad, w, &c, &rs) < 0);
        CHECK(c.n <= k + 1);
        collect_free(&c);
        pl_reader_free(r);
        free(bad);
    }
    /* every bit of the length, the type and the check bytes of the middle frame of three, flipped one at a time
     * (the random flips above seldom land there), and under header every bit of an uncompressed payload's first
     * PL_CHECK_SPAN bytes: the first frame comes through, the flipped one never. Small and large, compressed and not:
     * a type left out of the check would deliver the payload under another type, a compare of part of the check would
     * take a flip in the rest. Under header a flip just past the span arrives (the check covers a header, not the
     * bytes), under full it does not */
    for (int v = 0; v < 4 && ck != PL_CHECK_NONE; v++) {
        static const size_t vlen[] = {0, 80, 5000, 70000};
        uint8_t *pay = malloc(vlen[v] + 1), *f3[3];
        size_t n3[3];
        uint64_t sq = 0;
        fill_text(pay, vlen[v], (uint64_t)v);
        for (int i = 0; i < 3; i++) {
            n3[i] = pl_frame_encode(ck, salt, &sq, v & 1, (uint8_t)(0x21 + i), pay, vlen[v], &f3[i]);
            CHECK(n3[i] != 0);
        }
        size_t w = n3[0] + n3[1] + n3[2];
        uint8_t *s3 = malloc(w);
        memcpy(s3, f3[0], n3[0]);
        memcpy(s3 + n3[0], f3[1], n3[1]);
        memcpy(s3 + n3[0] + n3[1], f3[2], n3[2]);
        size_t pos[4 + 1 + PL_CHECK_LEN_MAX + PL_CHECK_SPAN];
        int np = 0;
        for (int i = 0; i < 5; i++) /* length, type */
            pos[np++] = n3[0] + (size_t)i;
        for (size_t i = 0; i < cl; i++)
            pos[np++] = n3[0] + n3[1] - cl + i;
        int plain = n3[1] == 4 + 1 + vlen[v] + cl; /* not compressed: the payload is on the wire as it was given */
        if (plain)
            for (size_t i = 0; i < vlen[v] && i < PL_CHECK_SPAN; i++)
                pos[np++] = n3[0] + 5 + i;
        int bad = 0;
        for (int i = 0; i < np; i++)
            for (int bit = 0; bit < 8; bit++) {
                s3[pos[i]] ^= (uint8_t)(1u << bit);
                struct pl_reader *r = pl_reader_new(ck, salt, 0);
                struct collect c = {0};
                pl_reader_feed(r, s3, w, collect_msg, &c);
                if (c.n != 1 || c.type[0] != 0x21 || c.len[0] != vlen[v])
                    bad++;
                collect_free(&c);
                pl_reader_free(r);
                s3[pos[i]] ^= (uint8_t)(1u << bit);
            }
        if (bad)
            fprintf(stderr, "  codec %s: %zu bytes%s: %d of %d single-bit flips of length, type, head or check were "
                    "taken\n", pl_check_name(ck), vlen[v], plain ? "" : " compressed", bad, np * 8);
        CHECK(bad == 0);
        if (plain && vlen[v] > PL_CHECK_SPAN) { /* the first byte past the span */
            s3[n3[0] + 5 + PL_CHECK_SPAN] ^= 1;
            struct pl_reader *r = pl_reader_new(ck, salt, 0);
            struct collect c = {0};
            int rc = pl_reader_feed(r, s3, w, collect_msg, &c);
            CHECK(ck == PL_CHECK_HEADER ? rc == 0 && c.n == 3 && c.data[1][PL_CHECK_SPAN] != pay[PL_CHECK_SPAN]
                                        : rc < 0 && c.n == 1);
            collect_free(&c);
            pl_reader_free(r);
            s3[n3[0] + 5 + PL_CHECK_SPAN] ^= 1;
        }
        for (int i = 0; i < 3; i++)
            free(f3[i]);
        free(s3);
        free(pay);
    }
    /* a frame replayed from further back: one encoded as frame d of its direction, read where frame 0 is due. Each
     * distance must be refused, the whole 64-bit number counts (a seed of its low 8 or 32 bits takes 256 or 2^32) */
    if (ck != PL_CHECK_NONE) {
        static const uint64_t dist[] = {0, 1, 255, 256, 65536, 1ull << 32, 1ull << 63};
        uint8_t pay[100];
        fill_random(pay, sizeof pay, 7);
        for (size_t i = 0; i < sizeof dist / sizeof dist[0]; i++) {
            uint64_t sq = dist[i];
            uint8_t *fr1;
            size_t n1 = pl_frame_encode(ck, salt, &sq, 0, 0x22, pay, sizeof pay, &fr1);
            CHECK(n1 != 0);
            struct pl_reader *r = pl_reader_new(ck, salt, 0);
            struct collect c = {0};
            int rc = pl_reader_feed(r, fr1, n1, collect_msg, &c);
            if ((dist[i] == 0) != (rc == 0 && c.n == 1))
                fprintf(stderr, "  codec: a frame of number %" PRIu64 " read as frame 0: rc %d, %d delivered\n",
                        dist[i], rc, c.n);
            CHECK(dist[i] == 0 ? rc == 0 && c.n == 1 : rc < 0 && c.n == 0);
            collect_free(&c);
            pl_reader_free(r);
            free(fr1);
        }
    }
    /* a frame whose length field is above the cap ends the stream at once */
    {
        uint8_t hdr[8] = {0xff, 0xff, 0xff, 0x7f, 1, 0, 0, 0};
        struct pl_reader *r = pl_reader_new(ck, salt, 0);
        struct collect c = {0};
        CHECK(pl_reader_feed(r, hdr, sizeof hdr, collect_msg, &c) < 0);
        pl_reader_free(r);
    }
    /* the streaming xxh64 */
    for (int rep = 0; rep < 40; rep++) {
        size_t n = rng_next(&rs) % 200000;
        uint8_t *d = malloc(n + 1);
        fill_random(d, n, rng_next(&rs));
        struct pl_xxh h;
        pl_xxh_init(&h);
        for (size_t o = 0; o < n;) {
            size_t t = 1 + rng_next(&rs) % 5000;
            if (t > n - o)
                t = n - o;
            pl_xxh_update(&h, d + o, t);
            o += t;
        }
        CHECK(pl_xxh_final(&h) == xxh64(d, n, 0));
        free(d);
    }
    for (int i = 0; i < NF; i++) {
        free(orig[i]);
        free(fr[i]);
    }
    free(stream);
    /* a zstd frame that decodes to fewer bytes than its declared raw length is not a frame (the tail would be
     * uninitialised memory handed to the driver) */
    {
        uint8_t src[900], z[1200], body[1300], *own = NULL;
        memset(src, 'a', sizeof src);
        size_t zn = ZSTD_compress(z, sizeof z, src, sizeof src, 1);
        CHECK(!ZSTD_isError(zn));
        for (int declared = 899; declared <= 1000; declared += 101) { /* 899: smaller, 1000: larger than decoded */
            uint32_t raw = (uint32_t)declared;
            memcpy(body, &raw, 4);
            memcpy(body + 4, z, zn);
            struct pl_msg m;
            CHECK(pl_msg_decode(0x81, body, 4 + zn, &m, &own) != 0);
            free(own);
        }
        uint32_t raw = 900;
        memcpy(body, &raw, 4);
        struct pl_msg m;
        CHECK(pl_msg_decode(0x81, body, 4 + zn, &m, &own) == 0 && m.n == 900); /* the right size passes */
        free(own);
    }
}

static void test_codec(void)
{
    test_check_kat();
    for (int ck = PL_CHECK_NONE; ck <= PL_CHECK_FULL; ck++)
        test_codec_at(ck);
    /* the levels and their HELLO offers: each end offers what is at or above its level */
    CHECK(pl_check_offer(PL_CHECK_NONE) == PL_CHECKS_KNOWN && pl_check_offer(PL_CHECK_FULL) == 1u << PL_CHECK_FULL);
    CHECK(pl_check_len(PL_CHECK_NONE) == 0 && pl_check_len(PL_CHECK_HEADER) == 8 && pl_check_len(PL_CHECK_FULL) == 16);
}

/* ---------------------------------------------------------------- a node: a peerlink with recording callbacks */

#define MAXRTT 4096

static _Atomic int g_fail_data;
static int g_uuid_byte = 0xA0;
struct node {
    struct peerlink *pl;
    int port;
    uint32_t id;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    /* frames */
    _Atomic uint64_t frames_in, bad_frames, order_bad;
    uint64_t last_seq;
    int have_seq;
    int strict;       /* seq must be last + 1 */
    int echo;         /* a type 2 frame is answered with type 3 */
    int gate_closed;  /* on_frame blocks while set */
    int gate_kind;    /* ...on links of this kind only (0: every link) */
    _Atomic int t6;   /* type 6 frames seen */
    _Atomic int sent_out[64]; /* on_sent by token: 0 none yet, 1 unwritten, 2 written; +10 per extra call */
    _Atomic uint64_t sent_gen[64];
    /* rtt */
    uint64_t rtt[MAXRTT];
    _Atomic int nrtt;
    /* links */
    _Atomic int ups, downs;
    struct plf_pair *_Atomic fp; /* a fake transport's pair this node's end is on (fend), checked at each down */
    int fend;
    _Atomic int close_early; /* downs seen after the transport of the connection was closed */
    /* bulk in */
    char path[600];
    _Atomic int fd; /* the receiver's file: its link thread opens it, a test thread polls its size */
    int discard; /* bench: the bytes go nowhere and a resume starts over */
    unsigned delay_us;
    _Atomic uint64_t opens, last_resume, data_bytes, ends;
    _Atomic int last_end;
    _Atomic int meta_bad;
    char want_meta[64];
    uint64_t want_total, cur_file;
    int fake_pfd;          /* b_open answers with this resume prefix (another file's bytes) when >= 0... */
    uint64_t fake_resume;  /* ...of this many bytes */
    int link_hello;        /* on_link(up) sends a type 7 frame from inside the callback */
    _Atomic int first_type; /* the first driver frame type since the link last came up */
};

static void gen_payload(uint8_t type, uint8_t *p, size_t n, uint64_t seq)
{
    memcpy(p, &seq, 8);
    if (type == 1)
        fill_random(p + 8, n - 8, seq);
    else
        fill_text(p + 8, n - 8, seq);
}

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    struct node *nd = arg;
    if (!atomic_load(&nd->first_type))
        atomic_store(&nd->first_type, type);
    pthread_mutex_lock(&nd->mu);
    while (nd->gate_closed && (!nd->gate_kind || plink_kind(l) == nd->gate_kind))
        pthread_cond_wait(&nd->cv, &nd->mu);
    pthread_mutex_unlock(&nd->mu);
    if (type == 6)
        atomic_fetch_add(&nd->t6, 1);
    if (type == 1 || type == 4) {
        uint64_t seq;
        if (n < 8) {
            nd->bad_frames++;
            return;
        }
        memcpy(&seq, p, 8);
        uint8_t *want = malloc(n);
        gen_payload(type, want, n, seq);
        if (memcmp(want, p, n) != 0)
            nd->bad_frames++;
        free(want);
        pthread_mutex_lock(&nd->mu); /* links of two kinds deliver on two threads (kinds) */
        if (nd->have_seq && (nd->strict ? seq != nd->last_seq + 1 : seq <= nd->last_seq))
            nd->order_bad++;
        nd->last_seq = seq;
        nd->have_seq = 1;
        pthread_mutex_unlock(&nd->mu);
        nd->frames_in++;
    } else if (type == 2 && nd->echo) {
        plink_send(l, 3, p, n, 200);
    } else if (type == 3 && n == 8) {
        uint64_t t0;
        memcpy(&t0, p, 8);
        int i = atomic_load(&nd->nrtt);
        if (i < MAXRTT) {
            nd->rtt[i] = now_ns() - t0;
            atomic_store(&nd->nrtt, i + 1);
        }
    }
}

static void on_link(void *arg, struct plink *l, int up)
{
    struct node *nd = arg;
    if (up) {
        atomic_store(&nd->first_type, 0);
        nd->ups++;
        if (nd->link_hello)
            CHECK(plink_send(l, 7, "H", 1, 0) == PL_OK); /* from inside on_link: allowed, and first */
    } else {
        struct plf_pair *fp = atomic_load(&nd->fp);
        if (fp && plf_closed(fp, nd->fend))
            nd->close_early++; /* pltransport.h: close is last, after on_link(down) */
        nd->downs++;
    }
}

static int b_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                  uint64_t *resume, int *prefix_fd)
{
    (void)l;
    struct node *nd = arg;
    if (nd->want_meta[0] && (meta_len != strlen(nd->want_meta) || memcmp(meta, nd->want_meta, meta_len) != 0 ||
                             file != 77 || total != nd->want_total))
        nd->meta_bad++;
    uint64_t n = atomic_fetch_add(&nd->opens, 1);
    if (nd->discard) {
        *resume = 0;
        *prefix_fd = -1;
        return 0;
    }
    if (nd->fake_pfd >= 0) { /* a resume prefix that is not the sender's bytes */
        if (nd->fd >= 0)
            close(nd->fd);
        nd->fd = open(nd->path, O_RDWR | O_CREAT | O_TRUNC, 0600);
        *resume = nd->fake_resume;
        *prefix_fd = nd->fake_pfd;
        return nd->fd < 0 ? -1 : 0;
    }
    if (n == 0 || nd->fd < 0 || file != nd->cur_file) {
        nd->cur_file = file;
        if (nd->fd >= 0)
            close(nd->fd);
        nd->fd = open(nd->path, O_RDWR | O_CREAT | O_TRUNC, 0600);
        if (nd->fd < 0)
            return -1;
        *resume = 0;
        *prefix_fd = -1;
    } else {
        struct stat st;
        if (fstat(nd->fd, &st) != 0)
            return -1;
        *resume = (uint64_t)st.st_size;
        *prefix_fd = nd->fd;
    }
    atomic_store(&nd->last_resume, *resume);
    return 0;
}

static int b_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    (void)l;
    (void)file;
    struct node *nd = arg;
    if (atomic_load(&g_fail_data)) return -1;
    if (nd->delay_us)
        usleep(nd->delay_us);
    if (!nd->discard && pwrite(nd->fd, p, n, (off_t)off) != (ssize_t)n)
        return -1;
    atomic_fetch_add(&nd->data_bytes, n);
    return 0;
}

static void b_end(void *arg, struct plink *l, uint64_t file, int status)
{
    (void)l;
    (void)file;
    struct node *nd = arg;
    atomic_store(&nd->last_end, status);
    atomic_fetch_add(&nd->ends, 1);
}

static void rec_sent(void *arg, struct plink *l, uint64_t token, uint64_t gen, int written)
{
    (void)l;
    struct node *nd = arg;
    if (token >= 64)
        return;
    int was = atomic_load(&nd->sent_out[token]);
    atomic_store(&nd->sent_out[token], was ? was + 10 : 1 + !!written);
    atomic_store(&nd->sent_gen[token], gen);
}

static int g_check; /* --check: every node's frame check level (plcheck.h; 0: the default, none) */

static struct node *node_new(uint32_t id, const uint8_t *secret, struct pl_cfg *over, int listen_it)
{
    struct node *nd = calloc(1, sizeof *nd);
    nd->id = id;
    nd->fd = -1;
    nd->fake_pfd = -1;
    nd->strict = 1;
    nd->echo = 1;
    pthread_mutex_init(&nd->mu, NULL);
    pthread_cond_init(&nd->cv, NULL);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    if (over)
        c = *over;
    if (!c.check)
        c.check = g_check;
    c.self.id = id;
    memset(c.self.uuid, g_uuid_byte, 16); /* one cluster: one uuid */
    memset(c.self.ident, 0xB0 + (int)id, 16);
    c.secret = secret;
    c.secret_len = 32;
    c.arg = nd;
    c.on_link = on_link;
    c.on_frame = on_frame;
    c.on_sent = rec_sent;
    c.bulk.open = b_open;
    c.bulk.data = b_data;
    c.bulk.end = b_end;
    if (!c.ping_ms)
        c.ping_ms = 100;
    if (!c.dead_ms)
        c.dead_ms = 1000;
    if (!c.backoff_min_ms) {
        c.backoff_min_ms = 20;
        c.backoff_max_ms = 200;
    }
    nd->pl = peerlink_new(&c);
    if (!nd->pl) {
        fprintf(stderr, "peerlink_new failed\n");
        exit(2);
    }
    if (listen_it) {
        nd->port = peerlink_listen(nd->pl, 0);
        if (nd->port <= 0) {
            fprintf(stderr, "peerlink_listen failed\n");
            exit(2);
        }
    }
    return nd;
}

static void node_free(struct node *nd)
{
    pthread_mutex_lock(&nd->mu);
    nd->gate_closed = 0;
    pthread_cond_broadcast(&nd->cv);
    pthread_mutex_unlock(&nd->mu);
    peerlink_free(nd->pl);
    if (nd->fd >= 0)
        close(nd->fd);
    free(nd);
}

static void set_gate(struct node *nd, int closed)
{
    pthread_mutex_lock(&nd->mu);
    nd->gate_closed = closed;
    pthread_cond_broadcast(&nd->cv);
    pthread_mutex_unlock(&nd->mu);
}

static struct plink *dial(struct node *from, uint32_t want, int port, int kind)
{
    char addr[64];
    snprintf(addr, sizeof addr, "127.0.0.1:%d", port);
    struct plink *l = peerlink_dial(from->pl, want, addr, kind);
    if (!l) {
        fprintf(stderr, "peerlink_dial failed\n");
        exit(2);
    }
    return l;
}

/* `count` frames seq from.., type 1 (random) or 4 (text), `len` bytes each */
static int send_frames(struct plink *l, uint8_t type, uint64_t from, int count, size_t len, int timeout_ms)
{
    uint8_t *b = malloc(len);
    int rc = 0;
    for (int i = 0; i < count && rc == 0; i++) {
        gen_payload(type, b, len, from + (uint64_t)i);
        rc = plink_send(l, type, b, len, timeout_ms);
    }
    free(b);
    return rc;
}

/* ---------------------------------------------------------------- a proxy: blackhole, flip, cut */

struct proxy;
struct ppair {
    struct proxy *px;
    int a, b; /* a: the dialer's side, b: the target's */
    pthread_t t[2];
    int dir;
};

struct proxy {
    int lfd, port, target;
    pthread_t thr;
    _Atomic int stop, blackhole, flip;
    pthread_mutex_t mu;
    struct ppair *pairs[256];
    int np;
    _Atomic uint64_t accepted;
    _Atomic uint64_t bytes[2]; /* passed through, dialer to listener [0] and back [1] */
};

static void *pump(void *arg)
{
    struct ppair *pp = arg;
    struct proxy *px = pp->px;
    int dir = pp->dir;
    int from = dir ? pp->b : pp->a, to = dir ? pp->a : pp->b;
    uint8_t buf[65536];
    for (;;) {
        while (atomic_load(&px->blackhole) && !atomic_load(&px->stop))
            usleep(1000);
        ssize_t n = read(from, buf, sizeof buf);
        if (n <= 0)
            break;
        while (atomic_load(&px->blackhole) && !atomic_load(&px->stop))
            usleep(1000);
        if (!dir && n >= 300 && atomic_exchange(&px->flip, 0))
            buf[200] ^= 0x10;
        for (ssize_t o = 0; o < n;) {
            ssize_t w = write(to, buf + o, (size_t)(n - o));
            if (w <= 0)
                goto out;
            o += w;
        }
        atomic_fetch_add(&px->bytes[dir], (uint64_t)n);
    }
out:
    shutdown(from, SHUT_RDWR);
    shutdown(to, SHUT_RDWR);
    return NULL;
}

static void *accept_main(void *arg)
{
    struct proxy *px = arg;
    while (!atomic_load(&px->stop)) {
        struct pollfd pf = {.fd = px->lfd, .events = POLLIN};
        if (poll(&pf, 1, 50) <= 0)
            continue;
        int a = accept(px->lfd, NULL, NULL);
        if (a < 0)
            continue;
        int b = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)px->target)};
        inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
        if (b < 0 || connect(b, (struct sockaddr *)&sa, sizeof sa) != 0) {
            close(a);
            if (b >= 0)
                close(b);
            continue;
        }
        int one = 1;
        setsockopt(a, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        setsockopt(b, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        pthread_mutex_lock(&px->mu);
        if (px->np + 2 > 256) {
            pthread_mutex_unlock(&px->mu);
            close(a);
            close(b);
            continue;
        }
        for (int d = 0; d < 2; d++) {
            struct ppair *pp = calloc(1, sizeof *pp);
            pp->px = px;
            pp->a = a;
            pp->b = b;
            pp->dir = d;
            px->pairs[px->np++] = pp;
            pthread_create(&pp->t[0], NULL, pump, pp);
        }
        pthread_mutex_unlock(&px->mu);
        px->accepted++;
    }
    return NULL;
}

static struct proxy *proxy_new(int target)
{
    struct proxy *px = calloc(1, sizeof *px);
    pthread_mutex_init(&px->mu, NULL);
    px->target = target;
    px->lfd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(px->lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (bind(px->lfd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(px->lfd, 16) != 0) {
        perror("proxy listen");
        exit(2);
    }
    socklen_t sl = sizeof sa;
    getsockname(px->lfd, (struct sockaddr *)&sa, &sl);
    px->port = ntohs(sa.sin_port);
    pthread_create(&px->thr, NULL, accept_main, px);
    return px;
}

/* closes every connection through the proxy (the listener stays: a redial goes through) */
static void proxy_cut(struct proxy *px)
{
    pthread_mutex_lock(&px->mu);
    for (int i = 0; i < px->np; i++) {
        shutdown(px->pairs[i]->a, SHUT_RDWR);
        shutdown(px->pairs[i]->b, SHUT_RDWR);
    }
    pthread_mutex_unlock(&px->mu);
}

static void proxy_free(struct proxy *px)
{
    atomic_store(&px->stop, 1);
    pthread_join(px->thr, NULL);
    proxy_cut(px);
    for (int i = 0; i < px->np; i++)
        pthread_join(px->pairs[i]->t[0], NULL);
    for (int i = 0; i < px->np; i++) {
        /* both pumps of a pair share the two fds: close each once (the first of the two entries) */
        if (px->pairs[i]->dir == 0) {
            close(px->pairs[i]->a);
            close(px->pairs[i]->b);
        }
        free(px->pairs[i]);
    }
    close(px->lfd);
    free(px);
}

/* ---------------------------------------------------------------- files */

static uint64_t make_file(const char *path, size_t size, uint64_t seed)
{
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        perror(path);
        exit(2);
    }
    size_t blk = 1u << 20;
    uint8_t *b = malloc(blk);
    for (size_t o = 0; o < size; o += blk) {
        size_t n = size - o < blk ? size - o : blk;
        fill_random(b, n, seed + o / blk);
        if (pwrite(fd, b, n, (off_t)o) != (ssize_t)n) {
            perror("pwrite");
            exit(2);
        }
    }
    free(b);
    return (uint64_t)fd;
}

static int same_file(int fd_a, int fd_b, size_t size)
{
    size_t blk = 1u << 20;
    uint8_t *a = malloc(blk), *b = malloc(blk);
    int same = 1;
    for (size_t o = 0; o < size && same; o += blk) {
        size_t n = size - o < blk ? size - o : blk;
        if (pread(fd_a, a, n, (off_t)o) != (ssize_t)n || pread(fd_b, b, n, (off_t)o) != (ssize_t)n ||
            memcmp(a, b, n) != 0)
            same = 0;
    }
    free(a);
    free(b);
    return same;
}

struct jres {
    _Atomic int done;
    int status;
    uint64_t sent, file;
};

static void job_done(void *arg, uint64_t file, int status, uint64_t sent)
{
    struct jres *r = arg;
    r->status = status;
    r->sent = sent;
    r->file = file;
    atomic_store(&r->done, 1);
}

static int bulk_send_id(struct plink *l, uint64_t file, int fd, uint64_t length, const char *meta, struct jres *r)
{
    struct pl_job j;
    memset(&j, 0, sizeof j);
    j.file = file;
    j.fd = fd;
    j.base = 0;
    j.length = length;
    j.meta = meta;
    j.meta_len = strlen(meta);
    j.max_attempts = 20;
    j.done = job_done;
    j.arg = r;
    return plink_bulk_send(l, &j);
}

static int bulk_send(struct plink *l, int fd, uint64_t length, const char *meta, struct jres *r)
{
    return bulk_send_id(l, 77, fd, length, meta, r);
}

static void pathf(char *out, size_t cap, const char *name)
{
    snprintf(out, cap, "%s/peerlink-%d-%s", scratch, (int)getpid(), name);
}

/* ---------------------------------------------------------------- tests */

static void test_log(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l), 3000));
    CHECK(plink_peer(l) == 1 && plink_kind(l) == PL_KIND_LOG);
    CHECK(WAIT_FOR(peerlink_find(a->pl, 2, PL_KIND_LOG) != NULL, 3000));
    CHECK(WAIT_FOR(a->ups >= 1 && b->ups >= 1, 3000));
    int n = quick ? 1000 : 4000;
    uint64_t rs = 5;
    uint64_t seq = 0;
    for (int i = 0; i < n; i++) {
        size_t len = 8 + rng_next(&rs) % 3000;
        uint8_t type = (i & 1) ? 4 : 1;
        if (i % 97 == 0)
            len = 8 + 100000;
        /* one sequence across both kinds: a type 4 frame is a text one */
        uint8_t *buf = malloc(len);
        gen_payload(type, buf, len, seq++);
        CHECK(plink_send(l, type, buf, len, 5000) == PL_OK);
        free(buf);
    }
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)n, 10000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    /* an echo, from the acceptor's side too (its link is the one a frame arrived on) */
    uint64_t t0 = now_ns();
    CHECK(plink_send(l, 2, &t0, 8, 1000) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&b->nrtt) == 1, 3000));
    struct pl_stats sb, sa;
    peerlink_stats(b->pl, &sb);
    peerlink_stats(a->pl, &sa);
    CHECK(sb.frames_out >= (uint64_t)n && sa.frames_in >= (uint64_t)n);
    CHECK(sb.raw_out > sb.bytes_out); /* the text frames went through zstd */
    /* the acceptor sends to the dialer on its own accepted link */
    struct plink *al = peerlink_find(a->pl, 2, PL_KIND_LOG);
    CHECK(al != NULL);
    b->have_seq = 0;
    if (al) {
        CHECK(send_frames(al, 1, 0, 50, 500, 1000) == PL_OK);
        CHECK(WAIT_FOR(b->frames_in == 50, 3000));
        CHECK(b->bad_frames == 0 && b->order_bad == 0);
    }
    /* an id outside the driver's types is refused at the sender */
    CHECK(plink_send(l, 0x70, "x", 1, 0) == PL_E_INVALID);
    CHECK(plink_send(l, 0, "x", 1, 0) == PL_E_INVALID);
    CHECK(plink_send(l, 1, "x", (size_t)PL_FRAME_MAX + 1, 0) == PL_E_TOOBIG);
    node_free(b);
    node_free(a);
}

static void test_bulk(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "bulk-src");
    pathf(dst, sizeof dst, "bulk-dst");
    size_t size = (quick ? 6u : 20u) * 1000 * 1000 + 12345; /* not a multiple of a chunk */
    int sfd = (int)make_file(src, size, 11);
    /* no rate limit and a long ping interval: nothing but the socket paces the file. The link used to go back to
     * poll() with an empty write buffer and wait for its ping timer between two megabytes (found by --bench). */
    struct pl_cfg bc;
    memset(&bc, 0, sizeof bc);
    bc.bulk_rate = PL_RATE_UNLIMITED;
    bc.ping_ms = 5000;
    bc.dead_ms = 20000;
    struct node *a = node_new(1, SECRET, &bc, 1), *b = node_new(2, SECRET, &bc, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    strcpy(a->want_meta, "vol3/p17_0.part");
    a->want_total = size;
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    struct jres r = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send(l, sfd, size, a->want_meta, &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 30000));
    CHECK(now_ms() - t0 < 2000); /* an unthrottled file runs at the socket's pace, not the ping timer's */
    CHECK(r.status == PL_OK);
    CHECK(WAIT_FOR(a->ends == 1, 3000) && a->last_end == PL_OK);
    CHECK(a->meta_bad == 0 && a->opens == 1);
    CHECK(a->data_bytes == size);
    CHECK(same_file(sfd, a->fd, size));
    /* a second file on the same link; a refused one */
    struct jres r2 = {0};
    a->want_meta[0] = 0;
    CHECK(bulk_send_id(l, 78, sfd, 1000, "second", &r2) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r2.done), 10000) && r2.status == PL_OK);
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    CHECK(sb.bulk_files == 2);
    /* a length above the file is the sender's error, not the receiver's */
    struct jres r3 = {0};
    CHECK(bulk_send_id(l, 79, sfd, size + 4096, "short-file", &r3) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r3.done), 10000) && r3.status != PL_OK);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}

static void test_resume(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "res-src");
    pathf(dst, sizeof dst, "res-dst");
    size_t size = (quick ? 12u : 30u) * 1000 * 1000 + 777;
    int sfd = (int)make_file(src, size, 21);
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = 30ull << 20; /* the file takes about a second: time to cut it */
    struct node *b = node_new(2, SECRET, &c, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_BULK);
    struct jres r = {0};
    CHECK(bulk_send(l, sfd, size, "resume-me", &r) == PL_OK);
    int cuts = 0;
    double marks[2] = {0.25, 0.6};
    for (int k = 0; k < 2; k++) {
        if (!WAIT_FOR(atomic_load(&r.done) || (a->fd >= 0 && lseek(a->fd, 0, SEEK_END) >= (off_t)(marks[k] * (double)size)),
                      20000))
            break;
        if (atomic_load(&r.done))
            break;
        proxy_cut(px);
        cuts++;
        /* wait until the redial's file has started again before the next cut */
        uint64_t o = a->opens;
        WAIT_FOR(a->opens > o || atomic_load(&r.done), 10000);
    }
    CHECK(WAIT_FOR(atomic_load(&r.done), 60000));
    CHECK(r.status == PL_OK);
    CHECK(cuts == 2);
    CHECK(a->opens >= 3);
    CHECK(a->last_resume > 0);
    CHECK(r.sent < size); /* the last attempt sent less than the file */
    CHECK(a->last_end == PL_OK);
    CHECK(same_file(sfd, a->fd, size));
    struct stat st;
    CHECK(fstat(a->fd, &st) == 0 && (size_t)st.st_size == size);
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    CHECK(sb.connects >= 3);
    printf("  resume: %llu opens, last resume offset %llu of %zu, last attempt sent %llu\n",
           (unsigned long long)a->opens, (unsigned long long)a->last_resume, size, (unsigned long long)r.sent);
    node_free(b);
    node_free(a);
    proxy_free(px);
    close(sfd);
    unlink(src);
    unlink(dst);
}

static void test_secret(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *c = node_new(3, SECRET2, NULL, 0); /* another cluster */
    struct node *d = node_new(4, SECRET, NULL, 0);  /* the right secret and node id */
    struct node *e = node_new(5, SECRET, NULL, 0);  /* the right secret, but asks for another node id */
    struct plink *lc = dial(c, 1, a->port, PL_KIND_LOG);
    struct plink *lc2 = dial(c, 1, a->port, PL_KIND_BULK);
    struct plink *ld = dial(e, 9, a->port, PL_KIND_LOG);
    struct plink *ok = dial(d, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(ok), 3000));
    usleep(1200 * 1000);
    CHECK(!plink_up(lc) && !plink_up(lc2));
    CHECK(!plink_up(ld));
    struct pl_stats sa, sc, sd;
    peerlink_stats(a->pl, &sa);
    peerlink_stats(c->pl, &sc);
    peerlink_stats(d->pl, &sd);
    CHECK(sa.auth_failures + sc.auth_failures > 0);
    CHECK(c->ups == 0 && a->frames_in == 0);
    /* nothing was delivered from the wrong peer */
    CHECK(plink_send(lc, 1, "x", 1, 0) == PL_E_DOWN);
    CHECK(plink_up(ok)); /* and the good one is unharmed */
    node_free(e);
    node_free(d);
    node_free(c);
    node_free(a);
}

/* under a check (full, or header with frames short enough that it covers every byte; the run's --check when it names
 * one, else full: under none a flipped payload byte is delivered, the wire is trusted) */
static void test_flip(void)
{
    struct pl_cfg o = {.check = g_check > PL_CHECK_NONE ? g_check : PL_CHECK_FULL};
    size_t fl = o.check == PL_CHECK_HEADER ? 200 : 1500; /* header: 4 + 1 + 200 + 8 bytes, all covered */
    struct node *a = node_new(1, SECRET, &o, 1), *b = node_new(2, SECRET, &o, 0);
    a->strict = 0; /* a drop loses frames; the order inside a connection must still hold */
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    CHECK(plink_check(l) == o.check);
    CHECK(send_frames(l, 1, 0, 20, fl, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 20, 3000));
    int downs0 = a->downs, ups0 = b->ups;
    atomic_store(&px->flip, 1);
    /* random frames: byte 200 of the next read is inside a frame's checked bytes (1500: its payload) */
    uint64_t sent_to = 20;
    int per = o.check == PL_CHECK_HEADER ? 4 : 1; /* the proxy flips in reads of 300 bytes or more */
    for (int i = 0; i < 40; i++) {
        send_frames(l, 1, sent_to, per, fl, 100);
        sent_to += (uint64_t)per;
        usleep(3000);
    }
    CHECK(WAIT_FOR(a->downs > downs0, 3000)); /* the acceptor ended the link */
    CHECK(a->bad_frames == 0);                /* what came through is exactly what was sent */
    CHECK(a->order_bad == 0);
    /* the dialer redials and the stream goes on */
    CHECK(WAIT_FOR(b->ups > ups0 && plink_up(l), 5000));
    uint64_t before = a->frames_in;
    CHECK(send_frames(l, 1, 1000, 10, 1500, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in >= before + 10, 3000));
    CHECK(a->bad_frames == 0);
    node_free(b);
    node_free(a);
    proxy_free(px);
}

static void test_blackhole(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    CHECK(send_frames(l, 1, 0, 10, 300, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 10, 3000));
    a->strict = 0;
    int bdown0 = b->downs, bups0 = b->ups;
    atomic_store(&px->blackhole, 1);
    usleep(50 * 1000);
    uint64_t in0 = a->frames_in;
    /* frames sent into the hole: queued or refused, never delivered */
    for (int i = 0; i < 20; i++) {
        send_frames(l, 1, 100 + (uint64_t)i, 1, 300, 0);
        usleep(20 * 1000);
    }
    CHECK(WAIT_FOR(b->downs > bdown0, 4000)); /* dead_ms 1000: the dialer noticed the silence */
    CHECK(a->frames_in == in0);               /* the pair exchanged nothing meanwhile */
    atomic_store(&px->blackhole, 0);
    proxy_cut(px); /* what the hole held belongs to dead connections */
    CHECK(WAIT_FOR(b->ups > bups0 && plink_up(l), 6000));
    uint64_t before = a->frames_in;
    CHECK(WAIT_FOR(send_frames(l, 1, 1000, 10, 300, 100) == PL_OK, 3000));
    CHECK(WAIT_FOR(a->frames_in >= before + 10, 3000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    node_free(b);
    node_free(a);
    proxy_free(px);
}

static void test_backpressure(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.queue_bytes = 1u << 20;
    c.dead_ms = 30000;
    c.ping_ms = 5000;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    set_gate(a, 1); /* the receiver stops reading after the frame it holds */
    int accepted = 0, full = 0;
    size_t maxq = 0;
    const size_t len = 65536;
    for (int i = 0; i < 2000 && !full; i++) {
        uint8_t *buf = malloc(len);
        gen_payload(1, buf, len, (uint64_t)i);
        int rc = plink_send(l, 1, buf, len, 0);
        free(buf);
        if (rc == PL_OK)
            accepted++;
        else if (rc == PL_E_FULL)
            full = 1;
        else
            CHECK(rc == PL_OK || rc == PL_E_FULL);
        size_t q = plink_queued(l);
        if (q > maxq)
            maxq = q;
        if (accepted % 8 == 0)
            usleep(500); /* the I/O thread moves the queue into the socket meanwhile */
    }
    CHECK(full);
    CHECK(maxq <= (1u << 20) + len); /* the bound, a frame over at most */
    printf("  backpressure: %d frames of 64 KiB accepted before FULL, queue peak %zu bytes\n", accepted, maxq);
    /* a timed send waits for room (the socket may still take a little) and gives up with FULL */
    int rc = PL_OK;
    uint64_t waited = 0;
    for (int tries = 0; tries < 1000 && rc == PL_OK; tries++) {
        uint8_t *tb = malloc(len);
        gen_payload(1, tb, len, (uint64_t)accepted);
        uint64_t t0 = now_ms();
        rc = plink_send(l, 1, tb, len, 250);
        waited = now_ms() - t0;
        free(tb);
        if (rc == PL_OK)
            accepted++;
    }
    if (rc != PL_E_FULL)
        fprintf(stderr, "timed send: rc %d after %llu ms\n", rc, (unsigned long long)waited);
    CHECK(rc == PL_E_FULL);
    CHECK(waited >= 200 && waited < 2000);
    CHECK(plink_queued(l) <= (1u << 20) + len);
    set_gate(a, 0);
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)accepted, 15000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    CHECK(WAIT_FOR(plink_queued(l) == 0, 3000));
    /* room again: a send goes through */
    CHECK(send_frames(l, 1, (uint64_t)accepted, 1, 100, 1000) == PL_OK);
    node_free(b);
    node_free(a);
}

/* a backlog of frames larger than the write buffer, on a link whose ping timer is long: the link must go on at the
 * socket's pace and not wait for the timer between two buffers (found by --bench together with the bulk case) */
static void test_backlog(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.queue_bytes = 32u << 20;
    c.ping_ms = 5000;
    c.dead_ms = 20000;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    int n = 200;
    uint64_t t0 = now_ms();
    CHECK(send_frames(l, 1, 0, n, 65536, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)n, 15000));
    CHECK(now_ms() - t0 < 2000);
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    node_free(b);
    node_free(a);
}

static int cmp_u64(const void *x, const void *y)
{
    uint64_t a = *(const uint64_t *)x, b = *(const uint64_t *)y;
    return a < b ? -1 : a > b;
}

static void test_latency(void)
{
    char src[600];
    pathf(src, sizeof src, "lat-src");
    size_t size = (quick ? 32u : 64u) << 20;
    int sfd = (int)make_file(src, size, 31);
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = 200ull << 20;
    c.dead_ms = 20000;
    c.ping_ms = 5000;
    struct node *b = node_new(2, SECRET, &c, 0);
    a->discard = 1;
    a->delay_us = 5000; /* a receiver whose disk takes 5 ms a chunk */
    struct plink *ll = dial(b, 1, a->port, PL_KIND_LOG);
    struct plink *bl = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(ll) && a->ups >= 1, 3000));
    if (peerlink_mutant != PL_MUT_SHAREDSOCKET)
        CHECK(WAIT_FOR(plink_up(bl) && a->ups >= 2, 3000));
    struct jres r = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send(bl, sfd, size, "big", &r) == PL_OK);
    WAIT_FOR(a->data_bytes > (4u << 20), 5000); /* the file is flowing */
    int n = 0;
    while (!atomic_load(&r.done) && n < 200 && now_ms() < t0 + 40000) {
        uint64_t ts = now_ns();
        int want = atomic_load(&b->nrtt) + 1;
        if (plink_send(ll, 2, &ts, 8, 2000) != PL_OK)
            break;
        WAIT_FOR(atomic_load(&b->nrtt) >= want, 5000);
        n++;
        usleep(1000);
    }
    int during = atomic_load(&r.done) ? 0 : 1;
    CHECK(WAIT_FOR(atomic_load(&r.done), 60000));
    CHECK(r.status == PL_OK);
    int m = atomic_load(&b->nrtt);
    CHECK(m >= 20);
    if (m > 1) {
        uint64_t v[MAXRTT];
        memcpy(v, b->rtt, (size_t)m * 8);
        qsort(v, (size_t)m, 8, cmp_u64);
        uint64_t p50 = v[m / 2], p90 = v[m * 9 / 10], mx = v[m - 1];
        printf("  latency: %d round trips with a %zu MB file in flight (%s): p50 %.2f ms, p90 %.2f ms, max %.2f ms\n",
               m, size >> 20, during ? "still flowing" : "done", (double)p50 / 1e6, (double)p90 / 1e6,
               (double)mx / 1e6);
        CHECK(p50 < 10000000ull);
        CHECK(p90 < 40000000ull);
    }
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
}

/* ---------------------------------------------------------------- bench */

static double secs(uint64_t t0) { return (double)(now_ns() - t0) / 1e9; }

static void bench_hash(void)
{
    size_t n = 64u << 20;
    uint8_t *buf = malloc(n);
    fill_random(buf, n, 1);
    for (int rep = 0; rep < 3; rep++) {
        uint8_t out[32];
        uint64_t t = now_ns();
        tsdb_sha256_buf(buf, n, out);
        double s1 = secs(t);
        t = now_ns();
        volatile uint64_t h = xxh64(buf, n, 0);
        (void)h;
        double s2 = secs(t);
        /* a link's per-frame tag: HMAC over header + chunk, here over 256 KiB pieces */
        t = now_ns();
        for (size_t o = 0; o < n; o += 256u << 10)
            tsdb_hmac_sha256(SECRET, 32, buf + o, 256u << 10, out);
        double s3 = secs(t);
        printf("  bench hash rep %d: sha256 %.0f MB/s, hmac-sha256 over 256 KiB %.0f MB/s, xxh64 %.0f MB/s\n", rep + 1,
               (double)n / 1e6 / s1, (double)n / 1e6 / s3, (double)n / 1e6 / s2);
    }
    free(buf);
}

static void bench_bulk(uint32_t chunk, size_t size, int reps)
{
    char src[600];
    pathf(src, sizeof src, "bench-src");
    int sfd = (int)make_file(src, size, 41);
    double sum = 0;
    for (int rep = 0; rep < reps; rep++) {
        struct pl_cfg c;
        memset(&c, 0, sizeof c);
        c.bulk_rate = PL_RATE_UNLIMITED;
        c.bulk_chunk = chunk;
        c.dead_ms = 20000;
        c.ping_ms = 5000;
        struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
        a->discard = 1;
        struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
        WAIT_FOR(plink_up(l), 3000);
        struct jres r = {0};
        uint64_t t = now_ns();
        bulk_send(l, sfd, size, "b", &r);
        WAIT_FOR(atomic_load(&r.done), 120000);
        double s = secs(t);
        if (r.status != PL_OK)
            printf("  bench bulk FAILED status %d\n", r.status);
        sum += (double)size / 1e6 / s;
        printf("  bench bulk chunk %u KiB rep %d: %.0f MB/s (%.2f s, %zu MiB)\n", chunk >> 10, rep + 1,
               (double)size / 1e6 / s, s, size >> 20);
        node_free(b);
        node_free(a);
    }
    printf("  bench bulk chunk %u KiB mean: %.0f MB/s\n", chunk >> 10, sum / reps);
    close(sfd);
    unlink(src);
}

static void bench_log(int reps, size_t len)
{
    double sum = 0;
    for (int rep = 0; rep < reps; rep++) {
        struct pl_cfg c;
        memset(&c, 0, sizeof c);
        c.queue_bytes = 8u << 20;
        c.dead_ms = 20000;
        c.ping_ms = 5000;
        struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
        struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
        WAIT_FOR(plink_up(l) && a->ups >= 1, 3000);
        int n = 1000000;
        uint8_t *buf = malloc(len);
        uint64_t t = now_ns();
        for (int i = 0; i < n; i++) {
            gen_payload(4, buf, len, (uint64_t)i);
            if (plink_send(l, 4, buf, len, 5000) != PL_OK) {
                printf("  bench log send failed at %d\n", i);
                break;
            }
        }
        WAIT_FOR(a->frames_in == (uint64_t)n, 60000);
        double s = secs(t);
        struct pl_stats sb;
        peerlink_stats(b->pl, &sb);
        printf("  bench log rep %d: %.0f frames/s (%d frames of %zu bytes in %.2f s, %.1f MB/s of payload, wire %.1f%% "
               "of raw)\n",
               rep + 1, (double)a->frames_in / s, n, len, s, (double)a->frames_in * (double)len / 1e6 / s,
               100.0 * (double)sb.bytes_out / (double)(sb.raw_out ? sb.raw_out : 1));
        sum += (double)a->frames_in / s;
        free(buf);
        node_free(b);
        node_free(a);
    }
    printf("  bench log mean: %.0f frames/s\n", sum / reps);
}

static void bench(void)
{
    bench_hash();
    size_t size = 512u << 20;
    uint32_t chunks[] = {64u << 10, 256u << 10, 1u << 20, 4u << 20};
    for (int i = 0; i < 4; i++)
        bench_bulk(chunks[i], size, 3);
    bench_log(3, 80);
}

/* ---------------------------------------------------------------- main */

struct tcase {
    const char *name;
    void (*fn)(void);
};

/* a peer with the wrong secret but the same node id as a live dialer: the live accepted link must survive */
static void test_kick(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d = node_new(4, SECRET, NULL, 0);
    struct plink *ok = dial(d, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(ok) && a->ups >= 1, 3000));
    int adown0 = a->downs, dups0 = d->ups;
    struct node *c = node_new(4, SECRET2, NULL, 0);
    struct plink *lc = dial(c, 1, a->port, PL_KIND_LOG);
    usleep(3000 * 1000);
    printf("  kick: acceptor downs %d->%d, live dialer ups %d->%d, ok up %d, wrong-secret up %d\n", adown0,
           (int)a->downs, dups0, (int)d->ups, plink_up(ok), plink_up(lc));
    CHECK(a->downs == adown0 && d->ups == dups0);
    node_free(c);
    node_free(d);
    node_free(a);
}
/* the receiver's data callback fails: the header promises PL_E_IO to the sender */
static void test_dataerr(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "de-src");
    pathf(dst, sizeof dst, "de-dst");
    size_t size = 4u << 20;
    int sfd = (int)make_file(src, size, 5);
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *b = node_new(2, SECRET, NULL, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(l), 3000));
    atomic_store(&g_fail_data, 1);
    struct jres r = {0};
    CHECK(bulk_send(l, sfd, size, "x", &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 30000));
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    printf("  dataerr: status %d (PL_E_IO=%d), receiver opens %llu, sender connects %llu, dialer downs %d\n", r.status,
           PL_E_IO, (unsigned long long)a->opens, (unsigned long long)sb.connects, (int)b->downs);
    CHECK(r.status == PL_E_IO);
    atomic_store(&g_fail_data, 0);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}
struct opener {
    struct node *nd;
    unsigned ms;
};
static void *opener_main(void *arg)
{
    struct opener *o = arg;
    usleep(o->ms * 1000);
    set_gate(o->nd, 0);
    return NULL;
}

/* a driver callback slower than dead_ms (the receiver's disk stalls 2.5 s, dead_ms is 1 s) while the sender has more
 * to send: TCP stops the sender, which is held back and not silent, and the receiver does not count its own callback's
 * time against the peer: no link goes down, and every frame the sender was told PL_OK for arrives */
static void test_slowcb(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *b = node_new(2, SECRET, NULL, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    int ad = a->downs, bd = b->downs;
    set_gate(a, 1);
    struct opener o = {a, 2500};
    pthread_t t;
    pthread_create(&t, NULL, opener_main, &o);
    const int count = 400; /* 25 MB of random bytes: more than the queue and both socket buffers hold */
    CHECK(send_frames(l, 1, 0, count, 65536, 15000) == PL_OK);
    pthread_join(t, NULL);
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)count, 15000));
    usleep(1500 * 1000); /* a late drop would show now */
    printf("  slowcb: acceptor downs %d->%d, dialer downs %d->%d, frames_in %llu of %d\n", ad, (int)a->downs, bd,
           (int)b->downs, (unsigned long long)a->frames_in, count);
    CHECK(a->downs == ad && b->downs == bd && a->frames_in == (uint64_t)count && a->order_bad == 0);
    node_free(b);
    node_free(a);
}

/* ---------------------------------------------------------------- a raw peer: the acceptor's own checks */

static int raw_connect(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        fprintf(stderr, "raw connect: %s\n", strerror(errno));
        exit(2);
    }
    return fd;
}

/* the cluster HELLO as a data link's dialer sends it: [u32 len][type 1][magic, version, id, uuid, ident, nonce,
 * address, kind, check types, capabilities (all bits set)], of any version and check types */
static void raw_hello_as(int fd, uint32_t id, uint8_t uuid_byte, int kind, int version, unsigned checks)
{
    uint8_t b[128];
    size_t o = 4;
    b[o++] = 1;
    uint32_t magic = PL_CL_MAGIC;
    memcpy(b + o, &magic, 4);
    o += 4;
    b[o++] = (uint8_t)version;
    memcpy(b + o, &id, 4);
    o += 4;
    memset(b + o, uuid_byte, 16);
    o += 16;
    memset(b + o, 0xB0 + (int)id, 16);
    o += 16;
    fill_random(b + o, 32, 5);
    o += 32;
    b[o++] = 0; /* no address */
    b[o++] = 0;
    b[o++] = (uint8_t)kind;
    b[o++] = (uint8_t)checks;
    b[o++] = 0xFF; /* every capability bit: those this build does not know are ignored */
    uint32_t len = (uint32_t)(o - 4);
    memcpy(b, &len, 4);
    CHECK(write(fd, b, o) == (ssize_t)o);
}

static void raw_hello(int fd, uint32_t id, uint8_t uuid_byte, int kind)
{
    raw_hello_as(fd, id, uuid_byte, kind, PL_CL_VERSION, PL_CHECKS_KNOWN);
}

/* the next frame within ms: its type, or -1 when the peer closed or said nothing */
static int raw_read(int fd, int ms)
{
    uint8_t h[5];
    size_t got = 0;
    uint64_t end = now_ms() + (uint64_t)ms;
    while (got < 5) {
        struct pollfd pf = {fd, POLLIN, 0};
        int left = (int)(end - now_ms());
        if (left <= 0 || poll(&pf, 1, left) <= 0)
            return -1;
        ssize_t r = read(fd, h + got, 5 - got);
        if (r <= 0)
            return -1;
        got += (size_t)r;
    }
    uint32_t len;
    memcpy(&len, h, 4);
    for (uint32_t i = 1; i < len;) { /* the rest of the body */
        uint8_t tmp[256];
        struct pollfd pf = {fd, POLLIN, 0};
        int left = (int)(end - now_ms());
        if (left <= 0 || poll(&pf, 1, left) <= 0)
            return -1;
        ssize_t r = read(fd, tmp, len - i < sizeof tmp ? len - i : sizeof tmp);
        if (r <= 0)
            return -1;
        i += (uint32_t)r;
    }
    return h[4];
}

/* what the ACCEPTOR checks, played by a raw socket (the dialer's own checks would hide a missing one): a wrong AUTH
 * ends the connection and counts, a HELLO of another cluster is not even answered, a kind it does not know too */
static void test_rawauth(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    int fd = raw_connect(a->port);
    raw_hello(fd, 4, (uint8_t)g_uuid_byte, PL_KIND_LOG);
    CHECK(raw_read(fd, 3000) == 2); /* HELLO_ACK */
    uint8_t auth[5 + 32];
    uint32_t len = 33;
    memcpy(auth, &len, 4);
    auth[4] = 3;
    memset(auth + 5, 0x5a, 32); /* a MAC made without the secret */
    CHECK(write(fd, auth, sizeof auth) == (ssize_t)sizeof auth);
    CHECK(raw_read(fd, 3000) == -1); /* closed, nothing more said */
    usleep(200 * 1000);
    struct pl_stats sa;
    peerlink_stats(a->pl, &sa);
    CHECK(sa.auth_failures >= 1);
    CHECK(a->ups == 0 && peerlink_find(a->pl, 4, PL_KIND_LOG) == NULL); /* no link for a peer that failed AUTH */
    close(fd);
    /* another cluster's uuid: no ACK at all */
    fd = raw_connect(a->port);
    raw_hello(fd, 4, (uint8_t)(g_uuid_byte + 1), PL_KIND_LOG);
    CHECK(raw_read(fd, 3000) == -1);
    close(fd);
    /* a HELLO that offers only frame check types this server does not run (here bit 7): no ACK, no common check */
    fd = raw_connect(a->port);
    raw_hello_as(fd, 4, (uint8_t)g_uuid_byte, PL_KIND_LOG, PL_CL_VERSION, 0x80);
    CHECK(raw_read(fd, 3000) == -1);
    close(fd);
    /* a peer of the previous cluster version: refused by the version byte alone, with a line naming both (stderr
     * caught in a file for the while: the line is written before the connection is closed) */
    char ep[4096 + 32];
    snprintf(ep, sizeof ep, "%s/rawauth.err", scratch);
    fflush(stderr);
    int save = dup(2), ef = open(ep, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    CHECK(save >= 0 && ef >= 0 && dup2(ef, 2) == 2);
    fd = raw_connect(a->port);
    raw_hello_as(fd, 4, (uint8_t)g_uuid_byte, PL_KIND_LOG, PL_CL_VERSION - 1, PL_CHECKS_KNOWN);
    CHECK(raw_read(fd, 3000) == -1);
    close(fd);
    fflush(stderr);
    dup2(save, 2);
    close(save);
    char line[512] = {0}, want[128];
    CHECK(pread(ef, line, sizeof line - 1, 0) > 0);
    close(ef);
    unlink(ep);
    snprintf(want, sizeof want, "cluster version %d (this server speaks %d)", PL_CL_VERSION - 1, PL_CL_VERSION);
    if (!strstr(line, want))
        fprintf(stderr, "  rawauth: wanted \"%s\" on stderr, got \"%s\"\n", want, line);
    CHECK(strstr(line, want) != NULL);
    CHECK(a->ups == 0);
    node_free(a);
}

/* the salts each handshake derived (peerlink_test_salts), both ends of every connection */
static pthread_mutex_t salts_mu = PTHREAD_MUTEX_INITIALIZER;
static struct {
    int dialer;
    uint64_t out, in;
} salts[16];
static int nsalts;

static void salts_rec(int dialer, uint64_t out, uint64_t in)
{
    pthread_mutex_lock(&salts_mu);
    if (nsalts < 16)
        salts[nsalts].dialer = dialer, salts[nsalts].out = out, salts[nsalts].in = in, nsalts++;
    pthread_mutex_unlock(&salts_mu);
}

static int salts_count(void)
{
    pthread_mutex_lock(&salts_mu);
    int n = nsalts;
    pthread_mutex_unlock(&salts_mu);
    return n;
}

/* 1 when a frame encoded under salt so passes a reader under salt si */
static int salt_reads(uint64_t so, uint64_t si)
{
    uint8_t pay[64], *fr;
    uint64_t sq = 0;
    fill_random(pay, sizeof pay, so);
    size_t n = pl_frame_encode(PL_CHECK_FULL, so, &sq, 0, 0x22, pay, sizeof pay, &fr);
    struct pl_reader *r = pl_reader_new(PL_CHECK_FULL, si, 0);
    struct collect c = {0};
    int ok = n && pl_reader_feed(r, fr, n, collect_msg, &c) == 0 && c.n == 1;
    collect_free(&c);
    pl_reader_free(r);
    free(fr);
    return ok;
}

/* what the handshake derives (plcheck.h): each connection and each direction of it has a salt of its own, so a frame
 * reflected back to its sender, or one of an earlier connection replayed into a later one, fails its check (the codec
 * test shows that a reader under another salt refuses; this shows the handshake hands out other salts). Two connections
 * of one pair of nodes (two kinds), each seen from both ends. */
static void test_salts(void)
{
    nsalts = 0;
    peerlink_test_salts = salts_rec;
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d = node_new(2, SECRET, NULL, 0);
    struct plink *f = dial(d, 1, a->port, PL_KIND_FWD), *q = dial(d, 1, a->port, PL_KIND_FWDQ);
    CHECK(WAIT_FOR(plink_up(f) && plink_up(q), 3000));
    CHECK(WAIT_FOR(salts_count() >= 4, 3000));
    node_free(d);
    node_free(a);
    peerlink_test_salts = NULL;
    int n = nsalts, dials = 0, paired = 0, distinct = 1, twoway = 1;
    for (int i = 0; i < n; i++) {
        if (salts[i].out == salts[i].in || salt_reads(salts[i].out, salts[i].in))
            twoway = 0;
        if (!salts[i].dialer)
            continue;
        dials++;
        for (int j = 0; j < n; j++) {
            if (!salts[j].dialer && salts[j].in == salts[i].out && salts[j].out == salts[i].in &&
                salt_reads(salts[i].out, salts[j].in))
                paired++;
            if (j != i && salts[j].dialer &&
                (salts[j].out == salts[i].out || salts[j].in == salts[i].in || salt_reads(salts[i].out, salts[j].in)))
                distinct = 0;
        }
    }
    printf("  salts: %d handshake ends, %d dialers, %d paired; directions differ %d, connections differ %d\n", n, dials,
           paired, twoway, distinct);
    CHECK(n >= 4 && dials >= 2 && paired == dials);
    CHECK(twoway);   /* a frame reflected to its sender fails */
    CHECK(distinct); /* a frame of one connection fails in another */
}

/* raw: one frame with its body, its type or -1 */
static int raw_frame(int fd, int ms, uint8_t *body, size_t cap, size_t *n)
{
    uint8_t h[5];
    size_t got = 0, want = 5;
    uint64_t end = now_ms() + (uint64_t)ms;
    uint32_t len = 0;
    while (got < want) {
        struct pollfd pf = {fd, POLLIN, 0};
        int left = (int)(end - now_ms());
        if (left <= 0 || poll(&pf, 1, left) <= 0)
            return -1;
        ssize_t r = got < 5 ? read(fd, h + got, 5 - got) : read(fd, body + got - 5, want - got);
        if (r <= 0)
            return -1;
        got += (size_t)r;
        if (got == 5) {
            memcpy(&len, h, 4);
            if (len < 1 || len - 1 > cap)
                return -1;
            want = 4 + (size_t)len;
        }
    }
    *n = len - 1;
    return h[4];
}

/* the frame check levels (plcheck.h): a link runs at the stronger of its two ends' levels, both ends say so
 * (plink_check, the stats' gauge), and frames go through at every pair of levels, a file on the bulk link too */
static void test_levels(void)
{
    for (int x = PL_CHECK_NONE; x <= PL_CHECK_FULL; x++)
        for (int y = PL_CHECK_NONE; y <= PL_CHECK_FULL; y++) {
            struct pl_cfg ox = {.check = x}, oy = {.check = y};
            struct node *a = node_new(1, SECRET, &ox, 1), *b = node_new(2, SECRET, &oy, 0);
            struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
            struct plink *al = NULL;
            CHECK(WAIT_FOR(plink_up(l) && (al = peerlink_find(a->pl, 2, PL_KIND_LOG)) != NULL && plink_up(al), 3000));
            int want = x > y ? x : y;
            if (al && (plink_check(l) != want || plink_check(al) != want))
                fprintf(stderr, "  levels: %s acceptor, %s dialer: link at %s and %s, want %s\n", pl_check_name(x),
                        pl_check_name(y), pl_check_name(plink_check(al)), pl_check_name(plink_check(l)),
                        pl_check_name(want));
            CHECK(plink_check(l) == want && al && plink_check(al) == want);
            struct pl_stats sa, sb;
            peerlink_stats(a->pl, &sa);
            peerlink_stats(b->pl, &sb);
            CHECK(sa.up_check[want] == 1 && sb.up_check[want] == 1);
            CHECK(send_frames(l, 1, 0, 30, 700, 1000) == PL_OK);
            CHECK(WAIT_FOR(a->frames_in == 30, 3000));
            CHECK(a->bad_frames == 0 && a->order_bad == 0);
            node_free(b);
            CHECK(WAIT_FOR(!plink_up(al), 3000));
            peerlink_stats(a->pl, &sa);
            CHECK(sa.up_check[want] == 0);
            node_free(a);
        }
}

/* what the DIALER checks of a HELLO_ACK, played by a raw acceptor that holds the secret (the acceptor's own choice would
 * hide a missing check): the ACK naming the type offered gets AUTH, one naming a type it did not offer (7, which a later
 * build might choose) ends the handshake before AUTH, and so does one naming a type below the dialer's level (none to
 * a dialer of level header: its frames would go unchecked) */
static void test_dialtype(void)
{
    struct pl_cfg o = {.check = PL_CHECK_HEADER};
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t sl = sizeof sa;
    CHECK(lfd >= 0 && bind(lfd, (struct sockaddr *)&sa, sizeof sa) == 0 && listen(lfd, 4) == 0 &&
          getsockname(lfd, (struct sockaddr *)&sa, &sl) == 0);
    struct node *d = node_new(2, SECRET, &o, 0);
    dial(d, 1, ntohs(sa.sin_port), PL_KIND_LOG);
    static const int choices[] = {PL_CHECK_HEADER, 7, PL_CHECK_NONE, PL_CHECK_FULL};
    for (int round = 0; round < 4; round++) {
        int choice = choices[round];
        struct pollfd pf = {lfd, POLLIN, 0};
        CHECK(poll(&pf, 1, 3000) == 1);
        int fd = accept(lfd, NULL, NULL);
        CHECK(fd >= 0);
        uint8_t hello[256], m[512], body[256];
        size_t hn = 0, o = 0, bn;
        CHECK(raw_frame(fd, 3000, hello, sizeof hello, &hn) == 1);
        memcpy(m, "tsdb-link-s", 11);
        o = 11;
        memcpy(m + o, hello, hn);
        o += hn;
        uint8_t *ack = m + o;
        uint32_t id = 1;
        memcpy(ack, &id, 4);
        memset(ack + 4, g_uuid_byte, 16);
        memset(ack + 20, 0xB1, 16);
        fill_random(ack + 36, 32, (uint64_t)round + 1);
        ack[68] = (uint8_t)choice;
        ack[69] = 0xFF; /* every capability bit: unknown ones are ignored */
        o += 70;
        uint8_t fr[5 + 70 + 32];
        uint32_t len = 1 + 70 + 32;
        memcpy(fr, &len, 4);
        fr[4] = 2; /* HELLO_ACK */
        memcpy(fr + 5, ack, 70);
        tsdb_hmac_sha256(SECRET, 32, m, o, fr + 5 + 70);
        CHECK(write(fd, fr, sizeof fr) == (ssize_t)sizeof fr);
        int t = raw_frame(fd, 3000, body, sizeof body, &bn);
        printf("  dialtype: an ACK choosing check type %d gets %d (3: AUTH, -1: closed)\n", choice, t);
        CHECK(choice == PL_CHECK_HEADER || choice == PL_CHECK_FULL ? t == 3 && bn == 32 : t == -1);
        close(fd);
    }
    node_free(d);
    close(lfd);
}

/* a file whose receiver holds a prefix that is not the sender's bytes: the file's xxh64 does not match, and both ends
 * are told (PL_E_HASH): the per-file hash is what catches a resume from a damaged partial file */
static void test_endhash(void)
{
    char src[600], dst[600], bad[600];
    pathf(src, sizeof src, "eh-src");
    pathf(dst, sizeof dst, "eh-dst");
    pathf(bad, sizeof bad, "eh-bad");
    size_t size = 3u << 20;
    int sfd = (int)make_file(src, size, 21);
    int bfd = (int)make_file(bad, size, 22); /* other bytes */
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    a->fake_pfd = bfd;
    a->fake_resume = 1u << 20;
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    struct jres r = {0};
    CHECK(bulk_send(l, sfd, size, "x", &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 20000));
    CHECK(r.status == PL_E_HASH);
    CHECK(WAIT_FOR(a->ends == 1, 3000) && a->last_end == PL_E_HASH);
    /* a receiver that holds a longer prefix than the file is refused, not believed */
    a->fake_resume = size + 4096;
    struct jres r2 = {0};
    CHECK(bulk_send_id(l, 78, sfd, 1u << 19, "y", &r2) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r2.done), 20000));
    CHECK(r2.status == PL_E_REFUSED);
    node_free(b);
    node_free(a);
    close(sfd);
    close(bfd);
    unlink(src);
    unlink(dst);
    unlink(bad);
}

/* the rate holds: 4 MB at 8 MB/s takes about half a second (50 ms of burst), never the 20 ms of an open pipe */
static void test_throttle(void)
{
    char src[600], dst[600];
    pathf(src, sizeof src, "th-src");
    pathf(dst, sizeof dst, "th-dst");
    size_t size = 4u * 1000 * 1000;
    int sfd = (int)make_file(src, size, 31);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = 8u * 1000 * 1000;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(l), 3000));
    struct jres r = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send(l, sfd, size, "x", &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 20000));
    uint64_t took = now_ms() - t0;
    printf("  throttle: %zu bytes at 8 MB/s took %llu ms\n", size, (unsigned long long)took);
    CHECK(r.status == PL_OK);
    CHECK(took >= 400 && took < 3000);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}

/* each data kind has its own bucket (R4 D7): kind 5 (a snapshot) at 4 MB/s, kind 2 (a COMMIT's file) unlimited. A file
 * on kind 2 sent while kind 5 is busy for a second arrives at once, and kind 5 keeps its rate (mutant kindbucket: one
 * bucket at the peerlink's rate, kind 5 is not held to its own) */
static void test_kindrate(void)
{
    char src[600], src2[600];
    pathf(src, sizeof src, "kr-src");
    pathf(src2, sizeof src2, "kr-src2");
    size_t size = 4u * 1000 * 1000, size2 = 1u * 1000 * 1000;
    int sfd = (int)make_file(src, size, 41), sfd2 = (int)make_file(src2, size2, 43);
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.bulk_rate = PL_RATE_UNLIMITED;
    struct node *a = node_new(1, SECRET, &c, 1), *b = node_new(2, SECRET, &c, 0);
    a->discard = 1;
    CHECK(peerlink_set_kind_rate(b->pl, PL_KIND_SNAP, 4u * 1000 * 1000) == PL_OK);
    CHECK(peerlink_set_kind_rate(b->pl, PL_KIND_META, 1) == PL_E_INVALID);
    CHECK(peerlink_set_kind_rate(b->pl, PL_KIND_DATA_MAX + 1, 1) == PL_E_INVALID);
    struct plink *ls = dial(b, 1, a->port, PL_KIND_SNAP), *lb = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(ls) && plink_up(lb), 3000));
    struct jres rs = {0}, rb = {0};
    uint64_t t0 = now_ms();
    CHECK(bulk_send_id(ls, 1, sfd, size, "s", &rs) == PL_OK);
    usleep(100 * 1000);
    uint64_t t1 = now_ms();
    CHECK(bulk_send_id(lb, 2, sfd2, size2, "b", &rb) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&rb.done), 20000));
    uint64_t tookb = now_ms() - t1;
    int s_busy = !atomic_load(&rs.done); /* kind 2's file did not wait for kind 5's */
    CHECK(WAIT_FOR(atomic_load(&rs.done), 20000));
    uint64_t tooks = now_ms() - t0;
    printf("  kindrate: kind 5 %zu bytes at 4 MB/s took %llu ms; kind 2 %zu bytes beside it %llu ms\n", size,
           (unsigned long long)tooks, size2, (unsigned long long)tookb);
    CHECK(rs.status == PL_OK && rb.status == PL_OK);
    CHECK(tooks >= 800 && tooks < 5000);
    CHECK(tookb < 500 && s_busy);
    node_free(b);
    node_free(a);
    close(sfd);
    close(sfd2);
    unlink(src);
    unlink(src2);
}

/* a peer that holds the secret and sends what no honest sender does: a CHUNK that is not at the next offset ends the
 * link, and nothing of it reaches the data callback */
static void test_badchunk(void)
{
    char dst[600];
    pathf(dst, sizeof dst, "bc-dst");
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plink *l = dial(b, 1, a->port, PL_KIND_BULK);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    uint8_t begin[20 + 1] = {0}, chunk[16 + 10] = {0};
    uint64_t file = 5, total = 100, off = 50;
    memcpy(begin, &file, 8);
    memcpy(begin + 8, &total, 8);
    begin[16] = 1;
    begin[20] = 'm';
    memcpy(chunk, &file, 8);
    memcpy(chunk + 8, &off, 8); /* the receiver expects 0 */
    CHECK(plink_test_ctl(l, PLT_BEGIN, begin, sizeof begin) == PL_OK);
    CHECK(plink_test_ctl(l, PLT_CHUNK, chunk, sizeof chunk) == PL_OK);
    CHECK(WAIT_FOR(a->downs >= 1, 3000));
    CHECK(a->data_bytes == 0);
    node_free(b);
    node_free(a);
    unlink(dst);
}

/* a new connection of a peer whose old connection is still up (a restarted server, a redial the acceptor has not
 * noticed the end of) takes the accepted link over: its frames are delivered, the old one is closed */
static void test_replace(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d1 = node_new(4, SECRET, NULL, 0);
    struct plink *l1 = dial(d1, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l1) && a->ups >= 1, 3000));
    struct node *d2 = node_new(4, SECRET, NULL, 0);
    a->strict = 0;
    struct plink *l2 = dial(d2, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(a->ups >= 2, 3000));
    CHECK(WAIT_FOR(a->downs >= 1, 3000));
    CHECK(send_frames(l2, 1, 1000, 5, 200, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 5, 3000));
    node_free(d2);
    node_free(d1);
    node_free(a);
}

/* free with connections still in their handshake (a peer that says HELLO and nothing more): it returns at once, with
 * every thread joined (the sanitizers see a leak or a race) */
static void test_freehs(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.hs_ms = 20000;
    struct node *a = node_new(1, SECRET, &c, 1);
    int fds[10];
    for (int i = 0; i < 10; i++) {
        fds[i] = raw_connect(a->port);
        raw_hello(fds[i], 4 + (uint32_t)i, (uint8_t)g_uuid_byte, i % 2 ? PL_KIND_LOG : PL_KIND_BULK);
    }
    CHECK(raw_read(fds[0], 500) == 2); /* they are in the handshake, waiting for AUTH */
    uint64_t t0 = now_ms();
    node_free(a);
    printf("  freehs: free with 10 handshakes open took %llu ms\n", (unsigned long long)(now_ms() - t0));
    CHECK(now_ms() - t0 < 3000);
    for (int i = 0; i < 10; i++)
        close(fds[i]);
}

/* a frame is tied to the connection the driver meant: after a reconnect the old generation's send is PL_E_DOWN, and
 * what a driver sends from on_link(up) goes first, before another thread's frames (the link is open for them only
 * once on_link has returned) */
static void *spin_send(void *arg)
{
    struct plink *l = arg;
    for (int i = 0; i < 4000; i++) {
        plink_send(l, 8, "s", 1, 0);
        usleep(100);
    }
    return NULL;
}

static void test_gen(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1), *b = node_new(2, SECRET, NULL, 0);
    b->link_hello = 1;
    struct proxy *px = proxy_new(a->port);
    struct plink *l = dial(b, 1, px->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    uint64_t g0 = plink_gen(l);
    CHECK(g0 >= 1);
    CHECK(plink_send_gen(l, g0, 8, "x", 1, 0) == PL_OK);
    CHECK(plink_send_gen(l, g0 + 1, 8, "x", 1, 0) == PL_E_DOWN);
    CHECK(plink_send_gen(l, 0, 8, "x", 1, 0) == PL_E_INVALID);
    pthread_t t;
    pthread_create(&t, NULL, spin_send, l);
    for (int round = 0; round < 6; round++) {
        int ups = a->ups;
        proxy_cut(px);
        CHECK(WAIT_FOR(a->ups > ups, 5000));
        CHECK(WAIT_FOR(atomic_load(&a->first_type) != 0, 3000));
        CHECK(atomic_load(&a->first_type) == 7); /* the on_link frame, then the spinner's */
    }
    pthread_join(t, NULL);
    uint64_t g1 = plink_gen(l);
    CHECK(g1 > g0);
    CHECK(plink_send_gen(l, g0, 8, "x", 1, 0) == PL_E_DOWN); /* an older connection's frame is refused */
    proxy_free(px);
    node_free(b);
    node_free(a);
}

static int count_dir(const char *d)
{
    DIR *x = opendir(d);
    int n = 0;
    if (!x)
        return -1;
    while (readdir(x))
        n++;
    closedir(x);
    return n - 2;
}
/* the acceptor's callback is stuck (a driver blocked on a stalled volume) while the dialer goes on redialing: the
 * connections that pile up behind it cost no thread or descriptor beyond the one that waits for the link */
static void test_chain(void)
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.hs_ms = 300; /* a dialer whose connection is not served gives up and redials quickly */
    struct node *a = node_new(1, SECRET, &c, 1);
    struct node *b = node_new(2, SECRET, &c, 0);
    struct plink *l = dial(b, 1, a->port, PL_KIND_LOG);
    CHECK(WAIT_FOR(plink_up(l) && a->ups >= 1, 3000));
    int t0 = count_dir("/proc/self/task"), f0 = count_dir("/proc/self/fd");
    set_gate(a, 1);
    CHECK(send_frames(l, 1, 0, 1, 300, 1000) == PL_OK);
    usleep(6000 * 1000);
    int t1 = count_dir("/proc/self/task"), f1 = count_dir("/proc/self/fd");
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    printf("  chain: threads %d -> %d, fds %d -> %d after 6 s of a stuck callback; dialer connects %llu\n", t0, t1, f0,
           f1, (unsigned long long)sb.connects);
    CHECK(t1 - t0 <= 3 && f1 - f0 <= 6);
    set_gate(a, 0);
    node_free(b);
    node_free(a);
}

/* ---------------------------------------------------------------- forwarding kinds (server/fwd.h) */

/* FWD and FWDQ links come up and carry frames; a kind this build does not know is refused at the HELLO, and cannot
 * be dialed */
static void test_kinds(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d = node_new(2, SECRET, NULL, 0);
    a->strict = 0;
    struct plink *f = dial(d, 1, a->port, PL_KIND_FWD), *q = dial(d, 1, a->port, PL_KIND_FWDQ);
    CHECK(WAIT_FOR(plink_up(f) && plink_up(q), 3000));
    CHECK(plink_kind(f) == PL_KIND_FWD && plink_kind(q) == PL_KIND_FWDQ);
    CHECK(send_frames(f, 4, 1, 10, 3000, 1000) == PL_OK && send_frames(q, 4, 11, 10, 3000, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 20, 3000));
    CHECK(a->bad_frames == 0);
    CHECK(WAIT_FOR(peerlink_find(a->pl, 2, PL_KIND_FWD) && peerlink_find(a->pl, 2, PL_KIND_FWDQ), 1000));
    struct pl_stats st;
    peerlink_stats(d->pl, &st);
    CHECK(st.bytes_out < st.raw_out); /* text frames: FWD and FWDQ are compressed, like LOG */
    CHECK(peerlink_dial(d->pl, 1, "127.0.0.1:1", PL_KIND_DATA_MAX + 1) == NULL);
    CHECK(peerlink_dial(d->pl, 1, "127.0.0.1:1", PL_KIND_META) == NULL);
    int fd = raw_connect(a->port);
    raw_hello(fd, 2, (uint8_t)g_uuid_byte, PL_KIND_DATA_MAX + 1);
    int t = raw_read(fd, 1000);
    printf("  kinds: an unknown kind's HELLO gets %d (-1: closed)\n", t);
    CHECK(t == -1);
    close(fd);
    node_free(d);
    node_free(a);
}

/* ---------------------------------------------------------------- chunk kinds (server/chunksrv.h) */

/* CHUNK and CHUNKB links come up and carry frames and files: CHUNK frames are compressed like FWD's, a CHUNKB file goes
 * as it is, like every bulk file (its cells are compressed parts already: zstd would only spend CPU); a frame above
 * PL_FRAME_MAX is refused on CHUNK; a kind above this build's (a server that does not know it) is refused at its
 * HELLO. */
static void test_chunk(void)
{
    struct pl_cfg bc;
    memset(&bc, 0, sizeof bc);
    bc.bulk_rate = PL_RATE_UNLIMITED;
    struct node *a = node_new(1, SECRET, &bc, 1);
    struct node *d = node_new(2, SECRET, &bc, 0);
    a->strict = 0;
    struct proxy *px = proxy_new(a->port); /* counts the CHUNKB link's bytes on the wire */
    struct plink *c = dial(d, 1, a->port, PL_KIND_CHUNK), *b = dial(d, 1, px->port, PL_KIND_CHUNKB);
    CHECK(WAIT_FOR(plink_up(c) && plink_up(b), 3000));
    CHECK(plink_kind(c) == PL_KIND_CHUNK && plink_kind(b) == PL_KIND_CHUNKB);
    CHECK(send_frames(c, 4, 1, 10, 3000, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->frames_in == 10, 3000));
    CHECK(a->bad_frames == 0);
    CHECK(WAIT_FOR(peerlink_find(a->pl, 2, PL_KIND_CHUNK) && peerlink_find(a->pl, 2, PL_KIND_CHUNKB), 1000));
    struct pl_stats s0;
    peerlink_stats(d->pl, &s0);
    CHECK(s0.bytes_out < s0.raw_out); /* text frames on CHUNK: compressed */
    /* a file of zeros on CHUNKB: compressed it would be a few KB on the wire */
    char src[600], dst[600];
    pathf(src, sizeof src, "chunkb-src");
    pathf(dst, sizeof dst, "chunkb-dst");
    size_t size = 4u << 20;
    int sfd = open(src, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(sfd >= 0 && ftruncate(sfd, (off_t)size) == 0);
    snprintf(a->path, sizeof a->path, "%s", dst);
    strcpy(a->want_meta, "chunk");
    a->want_total = size;
    struct jres r = {0};
    uint64_t w0 = atomic_load(&px->bytes[0]);
    CHECK(bulk_send(b, sfd, size, a->want_meta, &r) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&r.done), 10000) && r.status == PL_OK);
    CHECK(WAIT_FOR(a->ends == 1, 3000) && a->last_end == PL_OK);
    CHECK(a->meta_bad == 0 && same_file(sfd, a->fd, size));
    uint64_t w = atomic_load(&px->bytes[0]) - w0;
    printf("  chunk: a CHUNKB file of %zu zero bytes took %" PRIu64 " bytes on the wire\n", size, w);
    CHECK(w >= size);
    uint8_t *big = calloc(1, PL_FRAME_MAX + 1);
    CHECK(plink_send(c, 5, big, PL_FRAME_MAX + 1, 0) == PL_E_TOOBIG);
    free(big);
    CHECK(PL_KIND_CHUNK > PL_KIND_FWDQ && PL_KIND_CHUNKB <= PL_KIND_DATA_MAX);
    CHECK(peerlink_dial(d->pl, 1, "127.0.0.1:1", PL_KIND_DATA_MAX + 1) == NULL);
    int fd = raw_connect(a->port);
    raw_hello(fd, 2, (uint8_t)g_uuid_byte, PL_KIND_DATA_MAX + 1);
    CHECK(raw_read(fd, 1000) == -1);
    close(fd);
    node_free(d);
    node_free(a);
    proxy_free(px);
    close(sfd);
    unlink(src);
    unlink(dst);
}

/* a payload above PL_FRAME_MAX is refused, header included; one at the bound goes */
static void test_toobig(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d = node_new(2, SECRET, NULL, 0);
    struct plink *f = dial(d, 1, a->port, PL_KIND_FWD);
    CHECK(WAIT_FOR(plink_up(f), 3000));
    uint8_t *b = malloc(PL_FRAME_MAX + 1);
    fill_random(b, PL_FRAME_MAX + 1, 3);
    uint8_t h[8] = {0};
    CHECK(plink_send(f, 5, b, PL_FRAME_MAX + 1, 0) == PL_E_TOOBIG);
    CHECK(plink_sendv(f, 0, 5, h, 8, b, PL_FRAME_MAX - 7, 0, 1, 9, 0) == PL_E_TOOBIG);
    CHECK(atomic_load(&d->sent_out[9]) == 0); /* refused: never called back */
    CHECK(plink_sendv(f, 0, 6, h, 8, b, PL_FRAME_MAX - 8, 1000, 1, 8, 0) == PL_OK);
    CHECK(WAIT_FOR(a->t6 == 1 && atomic_load(&d->sent_out[8]) == 2, 5000));
    free(b);
    node_free(d);
    node_free(a);
}

static int sent_all(struct node *d, int from, int to)
{
    for (int i = from; i <= to; i++)
        if (!atomic_load(&d->sent_out[i]))
            return 0;
    return 1;
}

/* A frame is WRITTEN once a byte of it reached the socket, NEVER_WRITTEN when none did: frames 0 and 1 fill a
 * receiver that stopped reading (0 delivered, 1 partly written), 2 and 3 wait in the queue; a deadline takes one out
 * of the queue while the link is up; a cut ends the rest. Small socket buffers make "partly" certain. */
static void test_written(void)
{
    struct pl_cfg over = {.sock_buf = 32 << 10, .dead_ms = 4000, .queue_bytes = 64u << 20};
    struct node *a = node_new(1, SECRET, &over, 1);
    struct node *d = node_new(2, SECRET, &over, 0);
    a->strict = 0;
    set_gate(a, 1);
    struct plink *f = dial(d, 1, a->port, PL_KIND_FWD);
    CHECK(WAIT_FOR(plink_up(f), 3000));
    uint64_t g = plink_gen(f);
    size_t n = PL_FRAME_MAX;
    uint8_t *b = malloc(n);
    for (uint64_t i = 0; i < 4; i++) {
        gen_payload(1, b, n, 100 + i);
        CHECK(plink_sendv(f, 0, 1, NULL, 0, b, n, 2000, 1, i, 0) == PL_OK);
    }
    /* 0 and 1 written (1 only in part: the receiver sits in frame 0's callback) */
    int ok = WAIT_FOR(atomic_load(&d->sent_out[0]) == 2 && atomic_load(&d->sent_out[1]) == 2, 5000);
    printf("  written: frames 0, 1: %d %d (2: written)\n", atomic_load(&d->sent_out[0]), atomic_load(&d->sent_out[1]));
    CHECK(ok);
    usleep(300 * 1000);
    CHECK(atomic_load(&d->sent_out[2]) == 0 && atomic_load(&d->sent_out[3]) == 0);
    /* a deadline in the queue: NEVER_WRITTEN, the link stays */
    gen_payload(1, b, 64, 200);
    uint64_t t0 = now_ns();
    CHECK(plink_sendv(f, 0, 1, NULL, 0, b, 64, 0, 1, 4, now_ns() + 200 * 1000000ull) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&d->sent_out[4]) == 1, 2000));
    printf("  written: a queued frame's deadline of 200 ms ended it after %.0f ms\n", (double)(now_ns() - t0) / 1e6);
    CHECK(plink_up(f) && plink_gen(f) == g);
    plink_reset(f);
    CHECK(WAIT_FOR(sent_all(d, 0, 4), 3000));
    printf("  written: after the cut: %d %d %d %d %d\n", atomic_load(&d->sent_out[0]), atomic_load(&d->sent_out[1]),
           atomic_load(&d->sent_out[2]), atomic_load(&d->sent_out[3]), atomic_load(&d->sent_out[4]));
    CHECK(atomic_load(&d->sent_out[0]) == 2 && atomic_load(&d->sent_out[1]) == 2);
    CHECK(atomic_load(&d->sent_out[2]) == 1 && atomic_load(&d->sent_out[3]) == 1);
    for (int i = 0; i < 5; i++)
        CHECK(atomic_load(&d->sent_gen[i]) == g);
    set_gate(a, 0);
    /* the receiver got frame 0 and at most frame 1, never one that was reported unwritten */
    usleep(300 * 1000);
    CHECK(a->frames_in <= 2 && a->bad_frames == 0);
    CHECK(WAIT_FOR(plink_up(f) && plink_gen(f) > g, 3000)); /* the dialed link comes back */
    usleep(200 * 1000);
    for (int i = 0; i < 5; i++)
        CHECK(atomic_load(&d->sent_out[i]) < 10); /* once each */
    free(b);
    node_free(d);
    node_free(a);
}

/* A LOG link whose receiver stopped reading does not delay a FWD frame to the same peer: their own sockets */
static void test_fwdstall(void)
{
    struct node *a = node_new(1, SECRET, NULL, 1);
    struct node *d = node_new(2, SECRET, NULL, 0);
    a->strict = 0;
    a->gate_kind = PL_KIND_LOG;
    set_gate(a, 1);
    struct plink *lg = dial(d, 1, a->port, PL_KIND_LOG);
    struct plink *f = dial(d, 1, a->port, PL_KIND_FWD);
    CHECK(WAIT_FOR(plink_up(lg) && plink_up(f), 3000));
    CHECK(send_frames(lg, 1, 1, 6, 1 << 20, 1000) == PL_OK); /* the receiver sits in the first one's callback */
    usleep(200 * 1000);
    uint64_t t0 = now_ns();
    CHECK(plink_send(f, 6, "fwd", 3, 1000) == PL_OK);
    int got = WAIT_FOR(a->t6 == 1, 1000);
    printf("  fwdstall: a FWD frame behind a stalled LOG link: %s after %.1f ms\n", got ? "delivered" : "NOT delivered",
           (double)(now_ns() - t0) / 1e6);
    CHECK(got);
    set_gate(a, 0);
    node_free(d);
    node_free(a);
}

/* two fwd instances: requests and replies, outcomes, deadlines, credit */
#define FEV_MAX 512
struct frec {
    pthread_mutex_t mu;
    struct fwd_ev ev[FEV_MAX];
    uint8_t body0[FEV_MAX]; /* the first byte of each body */
    int n;
    struct fwd *self;
    int autoreply;          /* a FWD request with frame id below 10 (or below autoreply, when above 1) is echoed (its
                             * first 64 bytes) */
    int block_type;         /* the first event of this type after block_ms is set sleeps that long (a slow consumer) */
    _Atomic int block_ms;
};

static void frec_ev(void *arg, const struct fwd_ev *ev)
{
    struct frec *r = arg;
    if (r->block_type && ev->type == r->block_type) {
        int ms = atomic_exchange(&r->block_ms, 0);
        if (ms)
            usleep((useconds_t)ms * 1000);
    }
    if (ev->type == FWD_EV_REQUEST && ev->kind == PL_KIND_FWD && r->autoreply &&
        ev->frame_id < (r->autoreply > 1 ? (uint64_t)r->autoreply : 10))
        CHECK(fwd_reply(r->self, &ev->from, ev->frame_id, ev->p, ev->n < 64 ? ev->n : 64) == FWD_OK);
    pthread_mutex_lock(&r->mu);
    if (r->n < FEV_MAX) {
        r->ev[r->n] = *ev;
        r->ev[r->n].p = NULL;
        r->body0[r->n] = ev->n ? ev->p[0] : 0;
        r->n++;
    }
    pthread_mutex_unlock(&r->mu);
}

/* the index of the k-th event of `type` for frame_id (any frame: UINT64_MAX), or -1 */
static int frec_find(struct frec *r, int type, uint64_t id, int k, struct fwd_ev *out)
{
    int hit = -1;
    pthread_mutex_lock(&r->mu);
    for (int i = 0; i < r->n; i++)
        if (r->ev[i].type == type && (id == UINT64_MAX || r->ev[i].frame_id == id) && k-- == 0) {
            hit = i;
            if (out)
                *out = r->ev[i];
            break;
        }
    pthread_mutex_unlock(&r->mu);
    return hit;
}

static int frec_count(struct frec *r, int type, uint64_t id)
{
    int c = 0;
    pthread_mutex_lock(&r->mu);
    for (int i = 0; i < r->n; i++)
        c += r->ev[i].type == type && (id == UINT64_MAX || r->ev[i].frame_id == id);
    pthread_mutex_unlock(&r->mu);
    return c;
}

static struct fwd *fwd_node_with(uint32_t id, struct frec *r, int *port, const struct fwd_cfg *over)
{
    struct fwd_cfg c = {.secret = SECRET, .secret_len = 32, .arg = r, .on_event = frec_ev, .ping_ms = 100,
                        .dead_ms = 2000, .backoff_min_ms = 20, .backoff_max_ms = 200};
    if (over) {
        c.dead_ms = over->dead_ms ? over->dead_ms : c.dead_ms;
        c.inbox_bytes = over->inbox_bytes;
        c.credit = over->credit;
        c.queue_bytes = over->queue_bytes;
        c.sock_buf = over->sock_buf;
    }
    c.self.id = id;
    memset(c.self.uuid, g_uuid_byte, 16);
    memset(c.self.ident, 0xB0 + (int)id, 16);
    pthread_mutex_init(&r->mu, NULL);
    struct fwd *f = fwd_new(&c);
    if (!f || (*port = peerlink_listen(fwd_peerlink(f), 0)) <= 0) {
        fprintf(stderr, "fwd_new failed\n");
        exit(2);
    }
    r->self = f;
    return f;
}

static struct fwd *fwd_node(uint32_t id, struct frec *r, int *port) { return fwd_node_with(id, r, port, NULL); }

static uint64_t in_ms(int ms) { return now_ns() + (uint64_t)ms * 1000000ull; }

static void test_fwd(void)
{
    static struct frec ra, rb;
    memset(&ra, 0, sizeof ra);
    memset(&rb, 0, sizeof rb);
    rb.autoreply = 1;
    int pa, pb;
    struct fwd *A = fwd_node(1, &ra, &pa), *B = fwd_node(2, &rb, &pb);
    struct fwd_member ma[2] = {{1, ""}, {2, ""}}, mb[2];
    snprintf(ma[0].peer, sizeof ma[0].peer, "127.0.0.1:%d", pa);
    snprintf(ma[1].peer, sizeof ma[1].peer, "127.0.0.1:%d", pb);
    memcpy(mb, ma, sizeof ma);
    fwd_set_members(A, ma, 2); /* itself is skipped */
    fwd_set_members(B, mb, 2);
    CHECK(WAIT_FOR(fwd_link_up(A, 2, PL_KIND_FWD) && fwd_link_up(A, 2, PL_KIND_FWDQ) && fwd_link_up(B, 1, PL_KIND_FWD) &&
                       fwd_link_up(B, 1, PL_KIND_FWDQ),
                   3000));
    CHECK(!fwd_link_up(A, 1, PL_KIND_FWD));

    /* a request and its reply */
    struct fwd_msg m = {PL_KIND_FWD, "hello", 5};
    CHECK(fwd_send(A, 2, &m, 1, in_ms(2000)) == FWD_OK);
    struct fwd_ev s1 = {0}, r1 = {0};
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_REPLY, 1, 0, &r1) >= 0, 2000));
    CHECK(frec_find(&ra, FWD_EV_SENT, 1, 0, &s1) >= 0 && s1.outcome == FWD_WRITTEN);
    CHECK(frec_find(&ra, FWD_EV_SENT, 1, 0, NULL) < frec_find(&ra, FWD_EV_REPLY, 1, 0, NULL));
    CHECK(r1.n == 5 && r1.gen == s1.gen);
    CHECK(fwd_send(A, 2, &m, 1, 0) == FWD_OK); /* done: its id is free again */
    CHECK(WAIT_FOR(frec_count(&ra, FWD_EV_REPLY, 1) == 2, 2000));
    CHECK(fwd_send(A, 9, &m, 2, 0) == FWD_E_NOPEER && fwd_send(A, 1, &m, 2, 0) == FWD_E_NOPEER);

    /* the size bound: FWD_MSG_MAX + 1 is refused, FWD_MSG_MAX goes */
    uint8_t *big = malloc(FWD_MSG_MAX + 1);
    fill_random(big, FWD_MSG_MAX + 1, 7);
    struct fwd_msg mbig = {PL_KIND_FWD, big, FWD_MSG_MAX + 1};
    CHECK(fwd_send(A, 2, &mbig, 3, 0) == FWD_E_TOOBIG);
    mbig.n = FWD_MSG_MAX;
    CHECK(fwd_send(A, 2, &mbig, 3, in_ms(5000)) == FWD_OK);
    struct fwd_ev rq = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 3, 0, &rq) >= 0, 5000) && rq.n == FWD_MSG_MAX);
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_REPLY, 3, 0, NULL) >= 0, 5000));
    free(big);

    /* a late reply: the frame times out at its deadline, the reply is dropped, and the link stays */
    struct fwd_stats st0, st1;
    fwd_stats(A, &st0);
    CHECK(fwd_send(A, 2, &m, 10, in_ms(200)) == FWD_OK);
    CHECK(fwd_send(A, 2, &m, 10, 0) == FWD_E_INVALID); /* in flight */
    struct fwd_ev held = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 10, 0, &held) >= 0, 2000));
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_TIMEOUT, 10, 0, NULL) >= 0, 2000));
    usleep(200 * 1000);
    int rr = fwd_reply(B, &held.from, 10, "late", 4);
    CHECK(rr == FWD_OK);
    CHECK(WAIT_FOR((fwd_stats(A, &st1), st1.dropped > st0.dropped), 2000));
    CHECK(frec_count(&ra, FWD_EV_REPLY, 10) == 0);
    CHECK(fwd_send(A, 2, &m, 4, in_ms(2000)) == FWD_OK);
    struct fwd_ev r4 = {0};
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_REPLY, 4, 0, &r4) >= 0, 2000));
    printf("  fwd: a late reply: connection %" PRIu64 " before, %" PRIu64 " after (link downs at A: %d)\n", s1.gen,
           r4.gen, frec_count(&ra, FWD_EV_LINK, UINT64_MAX) - 4);
    CHECK(r4.gen == s1.gen);
    for (int k = 0; frec_find(&ra, FWD_EV_LINK, UINT64_MAX, k, &rq) >= 0; k++)
        CHECK(rq.up);

    /* credit: the owner sends 4 chunks, then waits for CREDIT; a grant lets the rest through */
    struct fwd_msg mq = {PL_KIND_FWDQ, "select", 6};
    CHECK(fwd_send(A, 2, &mq, 20, in_ms(5000)) == FWD_OK);
    struct fwd_ev qreq = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 20, 0, &qreq) >= 0, 2000) && qreq.kind == PL_KIND_FWDQ);
    uint8_t *chunk = malloc(FWD_CHUNK_MAX + 1);
    fill_random(chunk, FWD_CHUNK_MAX + 1, 9);
    CHECK(fwd_chunk(B, &qreq.from, 20, chunk, FWD_CHUNK_MAX + 1, 0, 0) == FWD_E_TOOBIG);
    int ok4 = 1;
    for (int i = 0; i < 4; i++) {
        chunk[0] = (uint8_t)i;
        ok4 &= fwd_chunk(B, &qreq.from, 20, chunk, FWD_CHUNK_MAX, 0, 0) == FWD_OK;
    }
    CHECK(ok4);
    chunk[0] = 4;
    int c5 = fwd_chunk(B, &qreq.from, 20, chunk, FWD_CHUNK_MAX, 0, 300);
    usleep(300 * 1000);
    int got = frec_count(&ra, FWD_EV_CHUNK, 20);
    printf("  fwd: credit 4: the 5th chunk without a grant: %d (%d: no credit), chunks at the entry %d\n", c5,
           FWD_E_TIMEOUT, got);
    CHECK(c5 == FWD_E_TIMEOUT && got == 4);
    CHECK(fwd_credit(A, 2, 20, 1) == FWD_OK);
    CHECK(fwd_chunk(B, &qreq.from, 20, chunk, FWD_CHUNK_MAX, 1, 2000) == FWD_OK);
    CHECK(WAIT_FOR(frec_count(&ra, FWD_EV_CHUNK, 20) == 5, 2000));
    struct fwd_ev last = {0};
    CHECK(frec_find(&ra, FWD_EV_CHUNK, 20, 4, &last) >= 0 && last.last);
    for (int i = 0; i < 5; i++) {
        struct fwd_ev c;
        int at = frec_find(&ra, FWD_EV_CHUNK, 20, i, &c);
        CHECK(at >= 0 && ra.body0[at] == i); /* in order */
    }
    CHECK(fwd_credit(A, 2, 20, 1) == FWD_E_INVALID);              /* done at the entry */
    CHECK(fwd_chunk(B, &qreq.from, 20, chunk, 1, 1, 0) == FWD_E_DOWN); /* and at the owner */
    free(chunk);

    /* a connection that ends: the frame written on it is LOST; the owner's reply on the old connection is refused */
    CHECK(fwd_send(A, 2, &m, 30, 0) == FWD_OK);
    struct fwd_ev r30 = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 30, 0, &r30) >= 0, 2000));
    plink_reset(r30.from.l);
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_LOST, 30, 0, NULL) >= 0, 3000));
    CHECK(WAIT_FOR(fwd_link_up(A, 2, PL_KIND_FWD), 3000));
    CHECK(fwd_reply(B, &r30.from, 30, "x", 1) == FWD_E_DOWN);
    CHECK(fwd_send(A, 2, &m, 5, in_ms(2000)) == FWD_OK);
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_REPLY, 5, 0, NULL) >= 0, 3000));

    /* a member that leaves: its links stop */
    fwd_set_members(A, ma, 1);
    CHECK(!fwd_link_up(A, 2, PL_KIND_FWD));
    struct plink *bl = peerlink_find(fwd_peerlink(B), 1, PL_KIND_FWD);
    CHECK(bl && WAIT_FOR(!plink_up(bl), 3000)); /* the stopped link's connection ended */
    CHECK(fwd_send(A, 2, &m, 6, 0) == FWD_E_NOPEER);
    fwd_free(A);
    fwd_free(B);
}

/* The member list grows by several at once while the node table has room (nn < ncap), and past its capacity: every
 * member gets its two links (ASan: the per-call table of seen nodes once had room for one new member only, which
 * test_cluster ids hit when two nodes joined between two polls) */
static int frec_downs(struct frec *r)
{
    int downs = 0;
    struct fwd_ev e;
    for (int k = 0; frec_find(r, FWD_EV_LINK, UINT64_MAX, k, &e) >= 0; k++)
        downs += !e.up;
    return downs;
}

static void fwd_pair(struct fwd **A, struct frec *ra, struct fwd **B, struct frec *rb, const struct fwd_cfg *over)
{
    int pa, pb;
    *A = fwd_node_with(1, ra, &pa, over);
    *B = fwd_node_with(2, rb, &pb, over);
    struct fwd_member ma[2] = {{1, ""}, {2, ""}};
    snprintf(ma[0].peer, sizeof ma[0].peer, "127.0.0.1:%d", pa);
    snprintf(ma[1].peer, sizeof ma[1].peer, "127.0.0.1:%d", pb);
    fwd_set_members(*A, ma, 2);
    fwd_set_members(*B, ma, 2);
    CHECK(WAIT_FOR(fwd_link_up(*A, 2, PL_KIND_FWD) && fwd_link_up(*A, 2, PL_KIND_FWDQ) && fwd_link_up(*B, 1, PL_KIND_FWD) &&
                       fwd_link_up(*B, 1, PL_KIND_FWDQ),
                   3000));
}

/* A consumer that is slow for 3 s behind a small inbox (dead_ms 1 s): the links stay up (the full inbox pauses the
 * link, keepalive goes on both ways; the requests compress to little, so the sender is not held back by a shut window,
 * which alone would keep it from counting the peer dead), every request and reply arrives (none dropped); then, of the
 * frames behind a second pause, one still queued is canceled NEVER_WRITTEN, one in the write buffer WRITTEN (and its
 * late reply dropped). */
static void test_fwdslow(void)
{
    static struct frec ra, rb;
    memset(&ra, 0, sizeof ra);
    memset(&rb, 0, sizeof rb);
    rb.autoreply = 1000;
    rb.block_type = FWD_EV_REQUEST;
    struct fwd_cfg over = {.dead_ms = 1000, .inbox_bytes = 256 << 10, .sock_buf = 64 << 10};
    struct fwd *A, *B;
    fwd_pair(&A, &ra, &B, &rb, &over);
    size_t n = 64 << 10, big = 4u << 20;
    uint8_t *b = malloc(big);
    atomic_store(&rb.block_ms, 3000);
    for (uint64_t i = 0; i < 16; i++) {
        memset(b, 'a' + (int)i, n);
        struct fwd_msg m = {PL_KIND_FWD, b, n};
        CHECK(fwd_send(A, 2, &m, 100 + i, 0) == FWD_OK);
    }
    int ok = WAIT_FOR(frec_count(&ra, FWD_EV_REPLY, UINT64_MAX) == 16, 8000);
    printf("  fwdslow: consumer slow 3 s, inbox 256 KiB, dead_ms 1 s: %d replies, %d LOST, link downs %d/%d\n",
           frec_count(&ra, FWD_EV_REPLY, UINT64_MAX), frec_count(&ra, FWD_EV_LOST, UINT64_MAX), frec_downs(&ra),
           frec_downs(&rb));
    CHECK(ok && frec_count(&ra, FWD_EV_LOST, UINT64_MAX) == 0 && frec_downs(&ra) == 0 && frec_downs(&rb) == 0);
    for (uint64_t i = 0; i < 16; i++)
        CHECK(frec_count(&ra, FWD_EV_REPLY, 100 + i) == 1);

    atomic_store(&rb.block_ms, 2000);
    for (uint64_t i = 0; i < 8; i++) {
        memset(b, 'A' + (int)i, n);
        struct fwd_msg m = {PL_KIND_FWD, b, n};
        CHECK(fwd_send(A, 2, &m, 120 + i, 0) == FWD_OK);
    }
    fill_random(b, big, 200);
    struct fwd_msg mb = {PL_KIND_FWD, b, big}, ml = {PL_KIND_FWD, "late", 4};
    CHECK(fwd_send(A, 2, &mb, 200, 0) == FWD_OK); /* behind the pause: taken into the write buffer */
    usleep(300 * 1000);
    CHECK(fwd_send(A, 2, &ml, 201, 0) == FWD_OK); /* behind that: still queued */
    usleep(200 * 1000);
    int c201 = fwd_cancel(A, 2, 201), c200 = fwd_cancel(A, 2, 200);
    CHECK(fwd_cancel(A, 2, 201) == FWD_E_INVALID);
    ok = WAIT_FOR(frec_count(&ra, FWD_EV_REPLY, UINT64_MAX) == 24 && frec_count(&rb, FWD_EV_REQUEST, 200) == 1, 8000);
    usleep(300 * 1000);
    struct fwd_stats st;
    fwd_stats(A, &st);
    printf("  fwdslow: behind a second pause: cancel queued %d, in the buffer %d; %d replies, B got %d requests\n",
           c201, c200, frec_count(&ra, FWD_EV_REPLY, UINT64_MAX), frec_count(&rb, FWD_EV_REQUEST, UINT64_MAX));
    CHECK(ok && frec_count(&ra, FWD_EV_LOST, UINT64_MAX) == 0 && frec_downs(&ra) == 0 && frec_downs(&rb) == 0);
    CHECK(c201 == FWD_NEVER_WRITTEN && c200 == FWD_WRITTEN);
    CHECK(frec_count(&rb, FWD_EV_REQUEST, 201) == 0 && frec_count(&rb, FWD_EV_REQUEST, UINT64_MAX) == 25);
    CHECK(frec_count(&ra, FWD_EV_REPLY, 200) == 0 && st.dropped >= 1 && st.canceled == 2);
    free(b);
    fwd_free(A);
    fwd_free(B);
}

/* FWDQ streams: the span of a frame's deadline applies again after a chunk while the owner holds credit; a cancel
 * ends the owner's stream; a chunk that found no queue room keeps its credit and, the last, its stream */
static void test_fwdstream(void)
{
    static struct frec ra, rb;
    memset(&ra, 0, sizeof ra);
    memset(&rb, 0, sizeof rb);
    ra.block_type = FWD_EV_CHUNK;
    struct fwd_cfg over = {.dead_ms = 1000, .inbox_bytes = 64 << 10, .sock_buf = 64 << 10, .queue_bytes = 64 << 10,
                           .credit = 16};
    struct fwd *A, *B;
    fwd_pair(&A, &ra, &B, &rb, &over);
    struct fwd_msg mq = {PL_KIND_FWDQ, "q", 1};

    /* one chunk, then nothing: TIMEOUT one span later, and the id is free again */
    CHECK(fwd_send(A, 2, &mq, 50, in_ms(500)) == FWD_OK);
    struct fwd_ev q = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 50, 0, &q) >= 0, 2000));
    CHECK(fwd_chunk(B, &q.from, 50, "x", 1, 0, 0) == FWD_OK);
    uint64_t t0 = now_ns();
    int to = WAIT_FOR(frec_count(&ra, FWD_EV_TIMEOUT, 50) == 1, 2500);
    printf("  fwdstream: a stream silent after one chunk: %s after %.0f ms (span 500 ms)\n", to ? "TIMEOUT" : "nothing",
           (double)(now_ns() - t0) / 1e6);
    CHECK(to && frec_count(&ra, FWD_EV_CHUNK, 50) == 1);
    CHECK(fwd_send(A, 2, &mq, 50, 0) == FWD_OK && fwd_cancel(A, 2, 50) >= 0);

    /* a cancel: the owner's stream goes */
    CHECK(fwd_send(A, 2, &mq, 51, 0) == FWD_OK);
    struct fwd_ev q2 = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 51, 0, &q2) >= 0, 2000));
    CHECK(fwd_chunk(B, &q2.from, 51, "y", 1, 0, 0) == FWD_OK);
    CHECK(WAIT_FOR(frec_count(&ra, FWD_EV_CHUNK, 51) == 1, 2000));
    CHECK(fwd_cancel(A, 2, 51) == FWD_WRITTEN);
    CHECK(WAIT_FOR(fwd_chunk(B, &q2.from, 51, "z", 1, 0, 0) == FWD_E_DOWN, 2000));
    struct fwd_stats sb;
    fwd_stats(B, &sb);
    CHECK(sb.cancels >= 1);

    /* the entry's reader stops for 2 s (its link paused): the owner's queue fills, and what found no room is kept */
    CHECK(fwd_send(A, 2, &mq, 60, 0) == FWD_OK);
    struct fwd_ev q3 = {0};
    CHECK(WAIT_FOR(frec_find(&rb, FWD_EV_REQUEST, 60, 0, &q3) >= 0, 2000));
    atomic_store(&ra.block_ms, 2000);
    size_t cn = 256 << 10;
    uint8_t *b = malloc(cn);
    int sent = 0, rc = FWD_OK;
    while (sent < 15) {
        fill_random(b, cn, 600 + (uint64_t)sent);
        if ((rc = fwd_chunk(B, &q3.from, 60, b, cn, 0, 500)) != FWD_OK) /* a long wait: FULL only behind A */
            break;
        sent++;
    }
    int rl = rc == FWD_E_FULL ? fwd_chunk(B, &q3.from, 60, b, cn, 1, 0) : rc; /* the last does not fit either */
    int again = fwd_chunk(B, &q3.from, 60, b, cn, 0, 5000); /* the same chunk once the entry reads again */
    int more = 0, end = FWD_OK, fulls = 0;
    for (int tries = 0; tries < 2000; tries++) { /* small chunks, no wait: FULL (kept, tried again) or TIMEOUT */
        end = fwd_chunk(B, &q3.from, 60, "m", 1, 0, 0);
        if (end == FWD_OK)
            more++;
        else if (end == FWD_E_FULL)
            fulls++, usleep(2000);
        else
            break;
    }
    printf("  fwdstream: queue FULL after %d chunks (last: %d), retry %d, then %d more within the credit of 16 (%d "
           "FULL on the way), then %d\n",
           sent, rl, again, more, fulls, end);
    CHECK(rc == FWD_E_FULL && rl == FWD_E_FULL && again == FWD_OK && end == FWD_E_TIMEOUT);
    CHECK(sent + 1 + more == 16);
    CHECK(fwd_credit(A, 2, 60, 1) == FWD_OK);
    CHECK(fwd_chunk(B, &q3.from, 60, "end", 3, 1, 2000) == FWD_OK);
    struct fwd_ev last = {0};
    CHECK(WAIT_FOR(frec_find(&ra, FWD_EV_CHUNK, 60, sent + 1 + more, &last) >= 0, 3000) && last.last);
    CHECK(fwd_chunk(B, &q3.from, 60, "after", 5, 0, 0) == FWD_E_DOWN); /* the last one ended it */
    CHECK(frec_count(&ra, FWD_EV_LOST, UINT64_MAX) == 0 && frec_downs(&ra) == 0 && frec_downs(&rb) == 0);
    free(b);
    fwd_free(A);
    fwd_free(B);
}

/* Small frames taken into the write buffer behind a receiver that stopped reading, none of whose bytes went: at the
 * cut they are NEVER_WRITTEN, like the queued ones (test_written covers those and the partly written) */
static void test_wbcut(void)
{
    struct pl_cfg over = {.sock_buf = 32 << 10, .dead_ms = 4000, .queue_bytes = 64u << 20};
    struct node *a = node_new(1, SECRET, &over, 1);
    struct node *d = node_new(2, SECRET, &over, 0);
    a->strict = 0;
    set_gate(a, 1);
    struct plink *f = dial(d, 1, a->port, PL_KIND_FWD);
    CHECK(WAIT_FOR(plink_up(f), 3000));
    size_t n = 32 << 10;
    uint8_t *b = malloc(n);
    for (uint64_t i = 0; i < 48; i++) {
        gen_payload(1, b, n, 300 + i);
        CHECK(plink_sendv(f, 0, 1, NULL, 0, b, n, 2000, 1, i, 0) == PL_OK);
    }
    usleep(500 * 1000);
    int queued = (int)(plink_queued(f) / (n + 48));
    plink_reset(f);
    CHECK(WAIT_FOR(sent_all(d, 0, 47), 3000));
    int w = 0, u = 0;
    for (int i = 0; i < 48; i++) {
        w += atomic_load(&d->sent_out[i]) == 2;
        u += atomic_load(&d->sent_out[i]) == 1;
    }
    printf("  wbcut: 48 frames of 32 KiB, %d still queued at the cut: %d written, %d unwritten\n", queued, w, u);
    CHECK(w >= 1 && w + u == 48 && u > queued);
    set_gate(a, 0);
    free(b);
    node_free(d);
    node_free(a);
}

/* A member whose dial failed (no pipe for its link: the fd limit) is dialed again by the next fwd_set_members */
static void test_fwdredial(void)
{
    static struct frec ra, rb;
    memset(&ra, 0, sizeof ra);
    memset(&rb, 0, sizeof rb);
    int pa, pb;
    struct fwd *A = fwd_node(1, &ra, &pa), *B = fwd_node(2, &rb, &pb);
    struct fwd_member ma[2] = {{1, ""}, {2, ""}};
    snprintf(ma[0].peer, sizeof ma[0].peer, "127.0.0.1:%d", pa);
    snprintf(ma[1].peer, sizeof ma[1].peer, "127.0.0.1:%d", pb);
    fwd_set_members(B, ma, 2);
    struct rlimit rl0, rl;
    CHECK(getrlimit(RLIMIT_NOFILE, &rl0) == 0);
    int lo = open("/dev/null", O_RDONLY); /* the lowest free descriptor: with the limit there, no pipe can be made */
    CHECK(lo >= 0);
    close(lo);
    rl = rl0;
    rl.rlim_cur = (rlim_t)lo;
    CHECK(setrlimit(RLIMIT_NOFILE, &rl) == 0);
    fwd_set_members(A, ma, 2);
    CHECK(setrlimit(RLIMIT_NOFILE, &rl0) == 0);
    struct fwd_msg m = {PL_KIND_FWD, "x", 1};
    int before = fwd_send(A, 2, &m, 1, 0);
    fwd_set_members(A, ma, 2);
    int up = WAIT_FOR(fwd_link_up(A, 2, PL_KIND_FWD) && fwd_link_up(A, 2, PL_KIND_FWDQ), 3000);
    printf("  fwdredial: send while the dial had failed: %d (-4: no peer); after the next member list: %s\n", before,
           up ? "both links up" : "NOT up");
    CHECK(before == FWD_E_NOPEER && up);
    fwd_free(A);
    fwd_free(B);
}

static void test_fwdgrow(void)
{
    static struct frec r;
    memset(&r, 0, sizeof r);
    int port;
    struct fwd *A = fwd_node(1, &r, &port);
    struct fwd_member m[12];
    for (int i = 0; i < 12; i++) {
        m[i].id = (uint32_t)i + 1;
        snprintf(m[i].peer, sizeof m[i].peer, "127.0.0.1:%d", i ? 1 : port); /* nobody listens on port 1 */
    }
    static const uint32_t steps[] = {2, 5, 7, 12, 3, 11};
    struct fwd_msg q = {PL_KIND_FWDQ, "q", 1};
    uint64_t fid = 1;
    for (size_t s = 0; s < sizeof steps / sizeof steps[0]; s++) {
        fwd_set_members(A, m, steps[s]);
        for (uint32_t i = 1; i < 12; i++) { /* a member has a link (queued or down), anyone else none */
            int rc = fwd_send(A, m[i].id, &q, fid++, in_ms(100));
            CHECK(i < steps[s] ? rc != FWD_E_NOPEER : rc == FWD_E_NOPEER);
        }
    }
    fwd_free(A);
}

/* ---------------------------------------------------------------- another transport (server/pltransport.h) */

/* the two ends of (a, b, kind) on a new pair of tests/pl_fake.h (edge mode with reads of `slot` bytes, 0: level), as
 * if a handshake with transcript `tr` had passed; a is end 0, b end 1 */
static struct plf_pair *fake_attach_slot(struct node *a, struct node *b, int kind, const char *tr, size_t cap,
                                         size_t slot)
{
    struct pl_transport *ta, *tb;
    struct plf_pair *p = slot ? plf_new_edge(cap, slot, &ta, &tb) : plf_new(cap, &ta, &tb);
    a->fend = 0;
    b->fend = 1;
    atomic_store(&a->fp, p);
    atomic_store(&b->fp, p);
    CHECK(peerlink_test_attach(a->pl, b->id, kind, 0, (const uint8_t *)tr, strlen(tr), ta) == 0);
    CHECK(peerlink_test_attach(b->pl, a->id, kind, 1, (const uint8_t *)tr, strlen(tr), tb) == 0);
    return p;
}

static struct plf_pair *fake_attach(struct node *a, struct node *b, int kind, const char *tr, size_t cap)
{
    return fake_attach_slot(a, b, kind, tr, cap, 0);
}

/* the test lets go of its reference to the pair once both ends are down: the nodes stop looking at it */
static void fake_put(struct node *a, struct node *b, struct plf_pair *p)
{
    atomic_store(&a->fp, NULL);
    atomic_store(&b->fp, NULL);
    plf_put(p);
}

/* what every fake case ends with: no down after its close, and no connection counted in a handshake (an attach never
 * is: mutant nhandall takes it off all the same) */
static void fake_end_checks(struct node *a, struct node *b)
{
    struct pl_stats sa, sb;
    peerlink_stats(a->pl, &sa);
    peerlink_stats(b->pl, &sb);
    printf("  fake: downs after close %d %d, in handshake %" PRId64 " %" PRId64 "\n", (int)a->close_early,
           (int)b->close_early, (int64_t)sa.handshakes, (int64_t)sb.handshakes);
    CHECK(a->close_early == 0 && b->close_early == 0);
    CHECK(sa.handshakes == 0 && sb.handshakes == 0);
}

/* The link thread on a transport that is not TCP: the in-memory pair of tests/pl_fake.h with a 64 KiB ring, so a
 * large frame is many short writes and the thread polls the transport for room. Frames both ways, zstd, an echo; a
 * receiver held in its callback (the sender is held back: slow, not silent); a paused link that only PINGs; a byte
 * flipped in transit (the frame check ends the link, nothing wrong reaches the driver); a bulk file cut mid-stream
 * that resumes on the next connection from the receiver's offset; a cut now. */
static void test_fake(void)
{
    struct node *a = node_new(1, SECRET, NULL, 0), *b = node_new(2, SECRET, NULL, 0);
    struct plf_pair *p = fake_attach(a, b, PL_KIND_LOG, "fake transcript 1", 65536);
    struct plink *l = NULL, *al = NULL;
    CHECK(WAIT_FOR((l = peerlink_find(b->pl, 1, PL_KIND_LOG)) != NULL && plink_up(l), 3000));
    CHECK(WAIT_FOR((al = peerlink_find(a->pl, 2, PL_KIND_LOG)) != NULL && plink_up(al), 3000));
    if (!l || !al) {
        fake_put(a, b, p);
        node_free(b);
        node_free(a);
        return;
    }
    CHECK(plink_peer(l) == 1 && plink_peer(al) == 2);
    int n = quick ? 500 : 2000;
    uint64_t rs = 9;
    for (int i = 0; i < n; i++) {
        size_t len = 8 + rng_next(&rs) % 3000;
        if (i % 97 == 0)
            len = 8 + 300000; /* several rings of it */
        uint8_t type = (i & 1) ? 4 : 1;
        uint8_t *buf = malloc(len);
        gen_payload(type, buf, len, (uint64_t)i);
        CHECK(plink_send(l, type, buf, len, 5000) == PL_OK);
        free(buf);
    }
    CHECK(WAIT_FOR(a->frames_in == (uint64_t)n, 10000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);
    struct pl_stats sb;
    peerlink_stats(b->pl, &sb);
    CHECK(sb.raw_out > sb.bytes_out); /* the text frames went through zstd */
    CHECK(plf_total(p, 1) >= sb.bytes_out); /* the frames' bytes went through the transport (and its PINGs) */
    uint64_t t0 = now_ns();
    CHECK(plink_send(l, 2, &t0, 8, 1000) == PL_OK);
    CHECK(WAIT_FOR(atomic_load(&b->nrtt) == 1, 3000));
    b->have_seq = 0;
    CHECK(send_frames(al, 1, 0, 50, 500, 1000) == PL_OK);
    CHECK(WAIT_FOR(b->frames_in == 50, 3000));
    CHECK(b->bad_frames == 0 && b->order_bad == 0);

    /* held back for longer than dead_ms (1 s): a receiver in its callback, then a paused one; neither link ends */
    a->have_seq = 0;
    set_gate(a, 1);
    uint64_t f0 = a->frames_in;
    CHECK(send_frames(l, 1, 0, 32, 65536, 2000) == PL_OK);
    usleep(1500 * 1000);
    CHECK(plink_up(l) && plink_up(al) && a->downs == 0 && b->downs == 0);
    set_gate(a, 0);
    CHECK(WAIT_FOR(a->frames_in == f0 + 32, 5000));
    plink_pause(al, 1);
    CHECK(send_frames(l, 1, 32, 32, 65536, 2000) == PL_OK);
    usleep(1500 * 1000);
    CHECK(plink_up(l) && plink_up(al) && a->downs == 0 && b->downs == 0);
    CHECK(a->frames_in <= f0 + 32 + 5); /* at most the frames of the read before the pause (256 KiB) */
    plink_pause(al, 0);
    CHECK(WAIT_FOR(a->frames_in == f0 + 64, 5000));
    CHECK(a->bad_frames == 0 && a->order_bad == 0);

    /* a byte flipped in transit: the receiver's check ends the connection, both ends go down. Under none a payload
     * byte would arrive flipped (the wire is trusted): the flip lands in the next frame's length field, whose bound
     * ends it; under header in a 200-byte frame's covered head, under full in a payload */
    int ckl = g_check ? g_check : PL_CHECK_NONE;
    size_t fl = ckl == PL_CHECK_HEADER ? 200 : 1000;
    CHECK(plink_check(l) == ckl && plink_check(al) == ckl);
    plf_set_flip(p, 1, plf_total(p, 1) + (ckl == PL_CHECK_NONE ? 4 : ckl == PL_CHECK_HEADER ? 140 : 700));
    uint64_t f1 = a->frames_in;
    CHECK(send_frames(l, 1, 64, 10, fl, 1000) == PL_OK);
    CHECK(WAIT_FOR(a->downs == 1 && b->downs == 1, 5000));
    CHECK(a->frames_in < f1 + 10 && a->bad_frames == 0 && a->order_bad == 0);
    CHECK(!plink_up(l) && !plink_up(al));
    fake_put(a, b, p);

    /* a bulk file cut after its first megabyte: the job stays, and the next connection resumes it */
    char src[600], dst[600];
    pathf(src, sizeof src, "fake-src");
    pathf(dst, sizeof dst, "fake-dst");
    size_t size = 4u * 1000 * 1000 + 321;
    int sfd = (int)make_file(src, size, 21);
    snprintf(a->path, sizeof a->path, "%s", dst);
    struct plf_pair *p2 = fake_attach(a, b, PL_KIND_BULK, "fake transcript 2", 65536);
    plf_set_cut(p2, 1, 1u << 20);
    struct plink *bl = NULL;
    CHECK(WAIT_FOR((bl = peerlink_find(b->pl, 1, PL_KIND_BULK)) != NULL && plink_up(bl), 3000));
    struct jres r = {0};
    if (bl)
        CHECK(bulk_send(bl, sfd, size, "fake-file", &r) == PL_OK);
    CHECK(WAIT_FOR(a->downs == 2 && b->downs == 2, 5000));
    CHECK(!atomic_load(&r.done));
    CHECK(a->ends == 1 && a->last_end == PL_E_DOWN);
    fake_put(a, b, p2);
    struct plf_pair *p3 = fake_attach(a, b, PL_KIND_BULK, "fake transcript 3", 65536);
    CHECK(WAIT_FOR(atomic_load(&r.done), 10000));
    CHECK(r.status == PL_OK);
    CHECK(WAIT_FOR(a->ends == 2, 3000) && a->last_end == PL_OK);
    CHECK(a->last_resume > 0 && a->last_resume < size); /* resumed, not begun again */
    CHECK(same_file(sfd, a->fd, size));
    /* the stream ends now: both ends go down */
    plf_cut(p3);
    CHECK(WAIT_FOR(a->downs == 3 && b->downs == 3, 5000));
    fake_put(a, b, p3);
    fake_end_checks(a, b);
    node_free(b);
    node_free(a);
    close(sfd);
    unlink(src);
    unlink(dst);
}

/* The `ready` path (pltransport.h): a transport that has drained its news before the poll (tests/pl_fake.h's edge
 * mode: reads of one 4 KiB slot, a wake only when a ring goes from empty to not empty or from full to not full, both
 * pipes drained in poll_fds, two descriptors to poll) announces the bytes and the room it already holds through
 * `*ready` alone. A link thread that sleeps in poll anyway reads one slot per turn of its timers (mutant noready): the
 * frames below, about 2 MB both ways, then take tens of seconds instead of a fraction of one. */
static void test_fakeready(void)
{
    struct node *a = node_new(1, SECRET, NULL, 0), *b = node_new(2, SECRET, NULL, 0);
    struct plf_pair *p = fake_attach_slot(a, b, PL_KIND_LOG, "fake transcript r", 65536, 4096);
    struct plink *l = NULL, *al = NULL;
    CHECK(WAIT_FOR((l = peerlink_find(b->pl, 1, PL_KIND_LOG)) != NULL && plink_up(l), 3000));
    CHECK(WAIT_FOR((al = peerlink_find(a->pl, 2, PL_KIND_LOG)) != NULL && plink_up(al), 3000));
    if (!l || !al) {
        fake_put(a, b, p);
        node_free(b);
        node_free(a);
        return;
    }
    uint64_t t0 = now_ns();
    CHECK(send_frames(l, 1, 0, 200, 4000, 5000) == PL_OK);
    CHECK(send_frames(al, 1, 0, 200, 4000, 5000) == PL_OK);
    for (int i = 0; i < 4; i++) {
        uint8_t *buf = malloc(300000);
        gen_payload(1, buf, 300000, 1000 + (uint64_t)i);
        CHECK(plink_send(l, 1, buf, 300000, 5000) == PL_OK);
        free(buf);
    }
    int ok = WAIT_FOR(a->frames_in == 204 && b->frames_in == 200, 5000);
    double ms = (double)(now_ns() - t0) / 1e6;
    printf("  fakeready: %d + %d frames (2.4 MB) through 4 KiB slots in %.0f ms\n", (int)a->frames_in,
           (int)b->frames_in, ms);
    CHECK(ok && ms < 3000);
    CHECK(a->bad_frames == 0 && b->bad_frames == 0);
    plf_cut(p);
    CHECK(WAIT_FOR(a->downs == 1 && b->downs == 1, 5000));
    fake_put(a, b, p);
    fake_end_checks(a, b);
    node_free(b);
    node_free(a);
}

/* What a connection's end means over a transport. (1) An end is reported to a paused link too (pltransport.h): with
 * PINGs 5 s apart, only the report ends it within a second. (2) WRITTEN and NEVER_WRITTEN after short writes: a 4 KiB
 * ring, the receiver held in its first frame's callback, six tracked frames of 5000 bytes; the transport takes frame 0,
 * frame 1 and at most part of frame 2 (the reader's 4 KiB beyond frame 0 at most, and a full ring), so after a cut
 * frames 0 and 1 are WRITTEN and 3-5 NEVER_WRITTEN (mutant wnoshort counts a short write as the whole buffer). */
static void test_fakeend(void)
{
    struct pl_cfg over = {.ping_ms = 5000, .dead_ms = 20000};
    struct node *a = node_new(1, SECRET, &over, 0), *b = node_new(2, SECRET, &over, 0);
    struct plf_pair *p = fake_attach(a, b, PL_KIND_LOG, "fake transcript e1", 65536);
    struct plink *l = NULL, *al = NULL;
    CHECK(WAIT_FOR((l = peerlink_find(b->pl, 1, PL_KIND_LOG)) != NULL && plink_up(l), 3000));
    CHECK(WAIT_FOR((al = peerlink_find(a->pl, 2, PL_KIND_LOG)) != NULL && plink_up(al), 3000));
    plink_pause(al, 1);
    usleep(100 * 1000);
    uint64_t t0 = now_ns();
    plf_cut(p);
    int ok = WAIT_FOR(a->downs == 1 && b->downs == 1, 1000);
    printf("  fakeend: a paused link ended %s after %.0f ms\n", ok ? "down" : "NOT down", (double)(now_ns() - t0) / 1e6);
    CHECK(ok);
    fake_put(a, b, p);

    a->strict = 0;
    set_gate(a, 1);
    p = fake_attach(a, b, PL_KIND_FWD, "fake transcript e2", 4096);
    struct plink *f = NULL;
    CHECK(WAIT_FOR((f = peerlink_find(b->pl, 1, PL_KIND_FWD)) != NULL && plink_up(f), 3000));
    if (f) {
        uint8_t *buf = malloc(5000);
        for (uint64_t i = 0; i < 6; i++) {
            gen_payload(1, buf, 5000, 300 + i);
            CHECK(plink_sendv(f, 0, 1, NULL, 0, buf, 5000, 2000, 1, 10 + i, 0) == PL_OK);
        }
        free(buf);
        /* the receiver sits in frame 0's callback (on_frame counts after it), the ring full behind it: nothing more
         * is taken */
        CHECK(WAIT_FOR(plf_total(p, 1) >= 5021 + 4096, 3000));
        uint64_t tot = 0;
        for (int i = 0; i < 100; i++) {
            usleep(10 * 1000);
            uint64_t t = plf_total(p, 1);
            if (t == tot && i > 10)
                break;
            tot = t;
        }
        plf_cut(p);
        CHECK(WAIT_FOR(sent_all(b, 10, 15), 3000));
        printf("  fakeend: %" PRIu64 " bytes taken; frames 0-5: %d %d %d %d %d %d (2: written, 1: never)\n", tot,
               atomic_load(&b->sent_out[10]), atomic_load(&b->sent_out[11]), atomic_load(&b->sent_out[12]),
               atomic_load(&b->sent_out[13]), atomic_load(&b->sent_out[14]), atomic_load(&b->sent_out[15]));
        CHECK(atomic_load(&b->sent_out[10]) == 2 && atomic_load(&b->sent_out[11]) == 2);
        CHECK(atomic_load(&b->sent_out[12]) == 1 || atomic_load(&b->sent_out[12]) == 2);
        for (int i = 13; i < 16; i++)
            CHECK(atomic_load(&b->sent_out[i]) == 1);
    }
    CHECK(WAIT_FOR(b->downs == 2, 3000));
    set_gate(a, 0); /* the receiver leaves its callback and finds the end */
    CHECK(WAIT_FOR(a->downs == 2, 3000));
    CHECK(a->frames_in <= 3 && a->bad_frames == 0); /* never a frame reported NEVER_WRITTEN */
    fake_put(a, b, p);
    fake_end_checks(a, b);
    node_free(b);
    node_free(a);
}

/* --only NAME[,NAME...] */
static int only_has(const char *only, const char *name)
{
    size_t n = strlen(name);
    for (const char *p = only; p; p = strchr(p, ',') ? strchr(p, ',') + 1 : NULL)
        if (!strncmp(p, name, n) && (p[n] == ',' || p[n] == 0))
            return 1;
    return 0;
}

int main(int argc, char **argv)
{
    const char *only = NULL;
    int do_bench = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = 1;
        else if (!strcmp(argv[i], "--bench"))
            do_bench = 1;
        else if (!strcmp(argv[i], "--only") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--check") && i + 1 < argc) {
            if ((g_check = pl_check_parse(argv[++i])) < 0) {
                fprintf(stderr, "test_peerlink: --check none|header|full\n");
                return 2;
            }
        }
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            if (!strcmp(m, "notag")) { /* (flip runs under a check whatever --check says) */
                peerlink_mutant = PL_MUT_NOTAG;
                only = "flip";
            } else if (!strcmp(m, "noseq")) {
                peerlink_mutant = PL_MUT_NOSEQ;
                only = "codec";
            } else if (!strcmp(m, "nosalt")) {
                peerlink_mutant = PL_MUT_NOSALT;
                only = "codec";
            } else if (!strcmp(m, "checktype")) {
                peerlink_mutant = PL_MUT_CHECKTYPE;
                only = "rawauth";
            } else if (!strcmp(m, "samesalt")) {
                peerlink_mutant = PL_MUT_SAMESALT;
                only = "salts";
            } else if (!strcmp(m, "notranscript")) {
                peerlink_mutant = PL_MUT_NOTRANSCRIPT;
                only = "salts";
            } else if (!strcmp(m, "dialanytype")) {
                peerlink_mutant = PL_MUT_DIALANYTYPE;
                only = "dialtype";
            } else if (!strcmp(m, "notype")) {
                peerlink_mutant = PL_MUT_NOTYPE;
                only = "codec";
            } else if (!strcmp(m, "halfcmp")) {
                peerlink_mutant = PL_MUT_HALFCMP;
                only = "codec";
            } else if (!strcmp(m, "seq8")) {
                peerlink_mutant = PL_MUT_SEQ8;
                only = "codec";
            } else if (!strcmp(m, "sharedsocket")) {
                peerlink_mutant = PL_MUT_SHAREDSOCKET;
                only = "latency";
            } else if (!strcmp(m, "nobackpressure")) {
                peerlink_mutant = PL_MUT_NOBACKPRESSURE;
                only = "backpressure";
            } else if (!strcmp(m, "accauth")) {
                peerlink_mutant = PL_MUT_ACCAUTH;
                only = "rawauth";
            } else if (!strcmp(m, "uuid")) {
                peerlink_mutant = PL_MUT_UUID;
                only = "rawauth";
            } else if (!strcmp(m, "endhash")) {
                peerlink_mutant = PL_MUT_ENDHASH;
                only = "endhash";
            } else if (!strcmp(m, "nothrottle")) {
                peerlink_mutant = PL_MUT_NOTHROTTLE;
                only = "throttle";
            } else if (!strcmp(m, "zstdsize")) {
                peerlink_mutant = PL_MUT_ZSTDSIZE;
                only = "codec";
            } else if (!strcmp(m, "chunkoff")) {
                peerlink_mutant = PL_MUT_CHUNKOFF;
                only = "badchunk";
            } else if (!strcmp(m, "resumebound")) {
                peerlink_mutant = PL_MUT_RESUMEBOUND;
                only = "endhash";
            } else if (!strcmp(m, "nokick")) {
                peerlink_mutant = PL_MUT_NOKICK;
                only = "replace";
            } else if (!strcmp(m, "queuedasunsent")) {
                peerlink_mutant = PL_MUT_QUEUEDASUNSENT;
                only = "written";
            } else if (!strcmp(m, "kindshared")) {
                peerlink_mutant = PL_MUT_KINDSHARED;
                only = "fwdstall";
            } else if (!strcmp(m, "lateclose")) {
                fwd_mutant = FWD_MUT_LATECLOSE;
                only = "fwd";
            } else if (!strcmp(m, "nocredit")) {
                fwd_mutant = FWD_MUT_NOCREDIT;
                only = "fwd";
            } else if (!strcmp(m, "inboxwait")) {
                fwd_mutant = FWD_MUT_INBOXWAIT;
                only = "fwdslow";
            } else if (!strcmp(m, "inboxdrop")) {
                fwd_mutant = FWD_MUT_INBOXDROP;
                only = "fwdslow";
            } else if (!strcmp(m, "chunkfirst")) {
                fwd_mutant = FWD_MUT_CHUNKFIRST;
                only = "fwdstream";
            } else if (!strcmp(m, "wbwritten")) {
                peerlink_mutant = PL_MUT_WBWRITTEN;
                only = "wbcut";
            } else if (!strcmp(m, "pausedead")) {
                peerlink_mutant = PL_MUT_PAUSEDEAD;
                only = "fwdslow";
            } else if (!strcmp(m, "noready")) {
                peerlink_mutant = PL_MUT_NOREADY;
                only = "fakeready";
            } else if (!strcmp(m, "closefirst")) {
                peerlink_mutant = PL_MUT_CLOSEFIRST;
                only = "fake";
            } else if (!strcmp(m, "wnoshort")) {
                peerlink_mutant = PL_MUT_WNOSHORT;
                only = "fakeend";
            } else if (!strcmp(m, "nhandall")) {
                peerlink_mutant = PL_MUT_NHANDALL;
                only = "fake";
            } else if (!strcmp(m, "kindbucket")) {
                peerlink_mutant = PL_MUT_KINDBUCKET;
                only = "kindrate";
            } else {
                fprintf(stderr, "unknown mutant %s\n", m);
                return 2;
            }
        } else if (argv[i][0] != '-')
            scratch = argv[i];
        else {
            fprintf(stderr, "usage: test_peerlink [DIR] [--quick] [--only NAME] [--mutant M] [--bench]\n");
            return 2;
        }
    }
    signal(SIGPIPE, SIG_IGN);
    if (do_bench) {
        bench();
        return 0;
    }
    static const struct tcase cases[] = {
        {"codec", test_codec},   {"log", test_log},         {"bulk", test_bulk},
        {"resume", test_resume}, {"secret", test_secret},   {"flip", test_flip}, {"levels", test_levels},
        {"blackhole", test_blackhole}, {"backpressure", test_backpressure}, {"backlog", test_backlog}, {"latency", test_latency}, {"kick", test_kick}, {"dataerr", test_dataerr}, {"slowcb", test_slowcb}, {"chain", test_chain}, {"rawauth", test_rawauth}, {"salts", test_salts}, {"dialtype", test_dialtype}, {"endhash", test_endhash},
        {"throttle", test_throttle}, {"kindrate", test_kindrate}, {"badchunk", test_badchunk}, {"replace", test_replace}, {"freehs", test_freehs},
        {"gen", test_gen}, {"kinds", test_kinds}, {"chunk", test_chunk}, {"toobig", test_toobig}, {"written", test_written},
        {"fwdstall", test_fwdstall}, {"fwd", test_fwd}, {"fwdgrow", test_fwdgrow}, {"fwdredial", test_fwdredial},
        {"fwdslow", test_fwdslow}, {"fwdstream", test_fwdstream}, {"wbcut", test_wbcut}, {"fake", test_fake},
        {"fakeready", test_fakeready}, {"fakeend", test_fakeend},
    };
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        if (only && !only_has(only, cases[i].name))
            continue;
        int f0 = failures;
        uint64_t t = now_ns();
        cases[i].fn();
        printf("%-13s %s (%.1f s)\n", cases[i].name, failures == f0 ? "ok" : "FAILED", secs(t));
        fflush(stdout);
    }
    if (failures) {
        fprintf(stderr, "test_peerlink: %d failures\n", failures);
        return 1;
    }
    printf("test_peerlink: ok\n");
    return 0;
}
