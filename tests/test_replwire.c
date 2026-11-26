/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The replication driver's own check (server/replwire.h, RW_VERSION 3; rd-drvcheck): every driver message carries an
 * XXH3-64 over the fields the driver acts on, whatever check level the data link runs at, so a flipped bit there is
 * refused (RW_E_CHECK) before any field is read, and a flipped bit in a record's bytes is not its business (the
 * record's own checksum and its descriptor are, in repld.c's msg_refused).
 *
 *   ack      review F3: an ACK whose durable is raised to every value inside (durable, written], which passes every
 *            structural bound, is refused; so is every single-bit flip of the message
 *   append   every bit of an APPEND's head, APPEND head (seq, off) and each of its descriptors, the last ones included
 *            (review F2: `header` covers descriptor 0 only), is covered; a flip in a record's bytes still decodes
 *   fetch    a FETCH and a bulk meta, every bit
 *   version  a message of RW_VERSION 2 (no check) is not a message
 *
 * ./test_replwire [--mutant checknocmp|checkshort]: the planted bugs of replwire.c (the Makefile requires each to
 * fail). checklate, the driver's, is test_cluster drvcheck's.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "replwire.h"

static int fails;
#define CHECK(c, ...)                                                                                                  \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            printf("FAIL %s:%d: ", __FILE__, __LINE__);                                                                \
            printf(__VA_ARGS__);                                                                                       \
            printf("\n");                                                                                              \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

static struct repl_rec recs[16];
static uint32_t lens[16];

static int dec(const uint8_t *p, size_t n, struct rw_msg *m)
{
    return rw_decode_msg(p, n, m, recs, lens, 16);
}

/* verify that a check mismatch returns RW_E_CHECK before reading any field into *out, recs[] or lens[] */
static void check_untouched_on_e_check(const uint8_t *p, size_t n, const char *what)
{
    struct rw_msg m, z = {0};
    struct repl_rec srec[16];
    uint32_t slen[16];
    memset(&m, 0xA5, sizeof m);
    memset(recs, 0x5A, sizeof recs);
    memset(srec, 0x5A, sizeof srec);
    memset(lens, 0x3C, sizeof lens);
    memset(slen, 0x3C, sizeof slen);
    int rc = dec(p, n, &m);
    CHECK(rc == RW_E_CHECK, "%s: expected RW_E_CHECK, got %d", what, rc);
    CHECK(memcmp(&m, &z, sizeof m) == 0, "%s: *out was modified before RW_E_CHECK", what);
    CHECK(memcmp(recs, srec, sizeof recs) == 0, "%s: recs[] was modified before RW_E_CHECK", what);
    CHECK(memcmp(lens, slen, sizeof lens) == 0, "%s: lens[] was modified before RW_E_CHECK", what);
}

/* every single-bit flip of p[lo, hi) must be refused; returns how many were not */
static int flips_refused(uint8_t *p, size_t n, size_t lo, size_t hi, const char *what)
{
    int passed = 0;
    for (size_t i = lo; i < hi; i++)
        for (int b = 0; b < 8; b++) {
            struct rw_msg m;
            p[i] ^= (uint8_t)(1u << b);
            int rc = dec(p, n, &m);
            p[i] ^= (uint8_t)(1u << b);
            if (rc == 0) {
                if (!passed)
                    printf("FAIL: a flip of bit %d of byte %zu (%s) decoded\n", b, i, what);
                passed++;
            }
        }
    return passed;
}

static void t_ack(void)
{
    struct repl_msg am = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 3, .leader = -1, .durable = 90,
                          .written = 95, .rid = 77, .start = 1, .meta.leader = -1, .set_version = 5, .replay = 40};
    uint8_t b[RW_HEAD + RW_CHECK];
    size_t n = rw_encode_msg(b, sizeof b, 4, &am, NULL, 0, 0, NULL, 0);
    CHECK(n == RW_HEAD + RW_CHECK && n == rw_msg_size(0, 0), "an ACK encodes to %zu bytes", n);
    struct rw_msg m;
    CHECK(dec(b, n, &m) == 0 && m.m.durable == 90 && m.m.written == 95 && m.volume == 4, "the ACK decodes");
    /* F3: durable raised inside (durable, written]: every bound holds, only the check can tell; *out stays zeroed */
    for (uint64_t d = 91; d <= 95; d++) {
        uint8_t c[sizeof b];
        memcpy(c, b, n);
        memcpy(c + 52, &d, 8); /* durable: the 5th u64 from byte 20 (replwire.h) */
        check_untouched_on_e_check(c, n, "ACK durable in (durable, written]");
    }
    CHECK(flips_refused(b, n, 0, n, "ACK") == 0, "a flipped ACK decoded");
    /* empty APPEND (heartbeat, nrec == 0) and non-ACK control messages must also be covered bit for bit */
    struct repl_msg others[] = {
        {.type = REPL_M_APPEND, .from = 0, .to = 1, .epoch = 4, .leader = 0, .prev_pos = 10, .prev_epoch = 3,
         .commit = 9, .nrec = 0, .meta.leader = -1},
        {.type = REPL_M_REFUSE, .from = 1, .to = 0, .epoch = 5, .leader = -1, .meta.leader = -1},
        {.type = REPL_M_QUERY, .from = 1, .to = 0, .epoch = 4, .leader = -1, .last_epoch = 3, .written = 10,
         .rid = 77, .end = 1, .meta.leader = -1},
        {.type = REPL_M_ANSWER, .from = 0, .to = 1, .epoch = 4, .leader = 0, .last_epoch = 3, .end = 10,
         .meta.leader = -1},
    };
    const char *onames[] = {"empty APPEND", "REFUSE", "QUERY", "ANSWER"};
    for (size_t k = 0; k < sizeof others / sizeof *others; k++) {
        uint8_t ob[RW_HEAD + RW_CHECK];
        size_t on = rw_encode_msg(ob, sizeof ob, 4, &others[k], NULL, 0, 0, NULL, 0);
        CHECK(on == RW_HEAD + RW_CHECK, "%s encodes to %zu bytes", onames[k], on);
        CHECK(dec(ob, on, &m) == 0 && m.m.type == others[k].type, "%s decodes", onames[k]);
        CHECK(flips_refused(ob, on, 0, on, onames[k]) == 0, "a flipped %s decoded", onames[k]);
    }
    uint8_t c[sizeof b + 1];
    memcpy(c, b, n);
    c[n] = 0;
    CHECK(dec(c, n + 1, &m) != 0, "an ACK with a byte after its check decoded");
    CHECK(dec(b, n - 1, &m) != 0, "an ACK without its check's last byte decoded");
    CHECK(rw_encode_msg(b, n - 1, 4, &am, NULL, 0, 0, NULL, 0) == 0, "an ACK encoded into a buffer one byte short");
}

static void t_append(void)
{
    struct repl_rec rr[3] = {{.pos = 11, .epoch = 3, .n = 5}, {.pos = 16, .epoch = 3, .n = 1},
                             {.pos = 17, .epoch = 4, .n = 2}};
    uint32_t rl[3] = {40, 32, 24};
    uint8_t text[96];
    for (int i = 0; i < 96; i++)
        text[i] = (uint8_t)(i * 7 + 1);
    struct repl_msg ap = {.type = REPL_M_APPEND, .from = 0, .to = 2, .epoch = 4, .leader = 0, .prev_pos = 10,
                          .prev_epoch = 3, .commit = 9, .nrec = 3, .rec = rr, .meta.leader = -1};
    uint8_t b[RW_HEAD + RW_CHECK + RW_APPEND_HEAD + 3 * RW_DESC + 96];
    size_t n = rw_encode_msg(b, sizeof b, 4, &ap, rl, 1ull << 40, 4096, text, 96);
    CHECK(n == sizeof b && n == rw_msg_size(3, 96), "an APPEND of 3 records encodes to %zu bytes", n);
    size_t at = rw_msg_check_at(b, n);
    CHECK(at == RW_HEAD + RW_APPEND_HEAD + 3 * RW_DESC, "the check of the APPEND lies at %zu", at);
    struct rw_msg m;
    CHECK(dec(b, n, &m) == 0 && m.m.nrec == 3 && m.off == 4096 && m.nbytes == 96 && !memcmp(m.bytes, text, 96) &&
              recs[2].epoch == 4 && lens[2] == 24,
          "the APPEND decodes");
    /* verify *out, recs[], lens[] are untouched on RW_E_CHECK for flipped head, off, and descriptor 2 */
    const size_t flip_offs[] = {44, RW_HEAD + 8, RW_HEAD + RW_APPEND_HEAD + 2 * RW_DESC + 8};
    const char *flip_names[] = {"APPEND commit", "APPEND off", "APPEND descriptor 2"};
    for (size_t k = 0; k < sizeof flip_offs / sizeof *flip_offs; k++) {
        b[flip_offs[k]] ^= 1;
        check_untouched_on_e_check(b, n, flip_names[k]);
        b[flip_offs[k]] ^= 1;
    }
    CHECK(flips_refused(b, n, 0, RW_HEAD, "head") == 0, "a flipped APPEND head field decoded");
    CHECK(flips_refused(b, n, RW_HEAD, RW_HEAD + RW_APPEND_HEAD, "seq, off") == 0, "a flipped seq or off decoded");
    CHECK(flips_refused(b, n, RW_HEAD + RW_APPEND_HEAD, RW_HEAD + RW_APPEND_HEAD + RW_DESC, "descriptor 0") == 0,
          "a flipped descriptor 0 decoded");
    /* review F2: beyond descriptor 0, which `header` does not reach */
    CHECK(flips_refused(b, n, RW_HEAD + RW_APPEND_HEAD + RW_DESC, at, "descriptors 1-2") == 0,
          "a flipped descriptor beyond descriptor 0 decoded");
    CHECK(flips_refused(b, n, at, at + RW_CHECK, "the check") == 0, "a flipped check decoded");
    /* the records' bytes are not the check's: a flip there decodes (wal_record and msg_refused judge it) */
    int dropped = 0;
    for (size_t i = at + RW_CHECK; i < n; i += 13) {
        b[i] ^= 0x10;
        dropped += dec(b, n, &m) != 0;
        b[i] ^= 0x10;
    }
    CHECK(dropped == 0, "%d flips of record bytes were refused by the driver check", dropped);
    /* rw_encode_append_hdr() + rw_append_hdr_set_to() must match rw_encode_msg() byte for byte */
    uint8_t hdr[RW_HEAD + RW_APPEND_HEAD + 3 * RW_DESC + RW_CHECK];
    size_t hlen = rw_encode_append_hdr(hdr, sizeof hdr, 4, &ap, rl, 1ull << 40, 4096, 96);
    CHECK(hlen == at + RW_CHECK, "rw_encode_append_hdr returned %zu, expected %zu", hlen, at + RW_CHECK);
    CHECK(memcmp(hdr, b, hlen) == 0, "rw_encode_append_hdr bytes differ from rw_encode_msg");
    rw_append_hdr_set_to(hdr, hlen, 1);
    struct repl_msg ap1 = ap;
    ap1.to = 1;
    uint8_t b1[sizeof b];
    size_t n1 = rw_encode_msg(b1, sizeof b1, 4, &ap1, rl, 1ull << 40, 4096, text, 96);
    CHECK(n1 == sizeof b1 && memcmp(hdr, b1, hlen) == 0, "rw_append_hdr_set_to bytes differ from rw_encode_msg");
    uint8_t scat[sizeof b];
    memcpy(scat, hdr, hlen);
    memcpy(scat + hlen, text, 96);
    CHECK(dec(scat, sizeof scat, &m) == 0 && m.m.to == 1 && m.m.nrec == 3 && !memcmp(m.bytes, text, 96),
          "scattered append header after rw_append_hdr_set_to failed to decode");
    CHECK(rw_encode_append_hdr(hdr, hlen - 1, 4, &ap, rl, 1ull << 40, 4096, 96) == 0,
          "rw_encode_append_hdr accepted short cap");
    struct repl_msg empty_ap = ap;
    empty_ap.nrec = 0;
    empty_ap.rec = NULL;
    CHECK(rw_encode_append_hdr(hdr, sizeof hdr, 4, &empty_ap, NULL, 0, 0, 0) == 0,
          "rw_encode_append_hdr accepted empty APPEND");
    struct repl_msg not_ap = {.type = REPL_M_ACK, .from = 0, .to = 1, .leader = -1, .meta.leader = -1};
    CHECK(rw_encode_append_hdr(hdr, sizeof hdr, 4, &not_ap, NULL, 0, 0, 0) == 0,
          "rw_encode_append_hdr accepted non-APPEND message");
}

static void t_fetch(void)
{
    uint8_t b[RW_NAME_MAX + 32];
    char name[RW_NAME_MAX + 1];
    uint32_t vol;
    uint64_t x;
    size_t n = rw_encode_fetch(b, sizeof b, 4, 4242, "p19000_1_2_0.part");
    CHECK(n == 1 + 4 + 8 + 2 + 17 + RW_CHECK, "a FETCH encodes to %zu bytes", n);
    CHECK(rw_decode_fetch(b, n, &vol, &x, name, sizeof name) == 0 && vol == 4 && x == 4242, "the FETCH decodes");
    /* all 3 message kinds must use distinct seeds on the same buffer */
    uint64_t km = rw_check(RW_K_MSG, b, n), kf = rw_check(RW_K_FETCH, b, n), kb = rw_check(RW_K_META, b, n);
    CHECK(km != kf && km != kb && kf != kb, "rw_check did not separate RW_K_MSG, RW_K_FETCH and RW_K_META");
    /* flipped FETCH leaves output parameters untouched on RW_E_CHECK */
    b[5] ^= 1;
    vol = 0xDEADBEEFu;
    x = 0xCAFEBABE12345678ull;
    strcpy(name, "SENTINEL");
    CHECK(rw_decode_fetch(b, n, &vol, &x, name, sizeof name) == RW_E_CHECK && vol == 0xDEADBEEFu &&
              x == 0xCAFEBABE12345678ull && !strcmp(name, "SENTINEL"),
          "flipped FETCH touched outputs before RW_E_CHECK");
    b[5] ^= 1;
    int passed = 0;
    for (size_t i = 0; i < n; i++)
        for (int k = 0; k < 8; k++) {
            b[i] ^= (uint8_t)(1u << k);
            passed += rw_decode_fetch(b, n, &vol, &x, name, sizeof name) == 0;
            b[i] ^= (uint8_t)(1u << k);
        }
    CHECK(passed == 0, "%d flipped FETCHes decoded", passed);
    n = rw_encode_bulk_meta(b, sizeof b, 4, 123456, "p19000_1_2_0.part");
    CHECK(rw_decode_bulk_meta(b, n, &vol, &x, name, sizeof name) == 0 && x == 123456, "the bulk meta decodes");
    CHECK(rw_decode_fetch(b, n, &vol, &x, name, sizeof name) == RW_E_CHECK, "a bulk meta passed as a FETCH");
    b[5] ^= 1;
    vol = 0xDEADBEEFu;
    x = 0xCAFEBABE12345678ull;
    strcpy(name, "SENTINEL");
    CHECK(rw_decode_bulk_meta(b, n, &vol, &x, name, sizeof name) == RW_E_CHECK && vol == 0xDEADBEEFu &&
              x == 0xCAFEBABE12345678ull && !strcmp(name, "SENTINEL"),
          "flipped bulk meta touched outputs before RW_E_CHECK");
    b[5] ^= 1;
    passed = 0;
    for (size_t i = 0; i < n; i++)
        for (int k = 0; k < 8; k++) {
            b[i] ^= (uint8_t)(1u << k);
            passed += rw_decode_bulk_meta(b, n, &vol, &x, name, sizeof name) == 0;
            b[i] ^= (uint8_t)(1u << k);
        }
    CHECK(passed == 0, "%d flipped bulk metas decoded", passed);
}

static void t_version(void)
{
    struct repl_msg am = {.type = REPL_M_ACK, .from = 1, .to = 0, .epoch = 3, .leader = -1, .durable = 90,
                          .written = 95, .meta.leader = -1};
    uint8_t b[RW_HEAD + RW_CHECK];
    size_t n = rw_encode_msg(b, sizeof b, 4, &am, NULL, 0, 0, NULL, 0);
    struct rw_msg m;
    b[0] = 2;
    CHECK(dec(b, n, &m) == -1 && dec(b, RW_HEAD, &m) == -1, "a message of RW_VERSION 2 decoded");
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *mu = argv[++i];
            rw_mutant = !strcmp(mu, "checknocmp") ? RW_MUT_NOCMP : !strcmp(mu, "checkshort") ? RW_MUT_SHORT : -1;
            if (rw_mutant < 0) {
                fprintf(stderr, "unknown mutant %s\n", mu);
                return 2;
            }
            printf("MUTANT %s\n", mu);
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "usage: %s [SCRATCH] [--mutant checknocmp|checkshort]\n", argv[0]);
            return 2;
        }
    }
    t_ack();
    t_append();
    t_fetch();
    t_version();
    if (fails) {
        printf("test_replwire: %d FAILED\n", fails);
        return 1;
    }
    printf("test_replwire: ok (F3's ACK, every bit of an APPEND's head and descriptors, FETCH, bulk meta)\n");
    return 0;
}
