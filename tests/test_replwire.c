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
    /* F3: durable raised inside (durable, written]: every bound holds, only the check can tell */
    for (uint64_t d = 91; d <= 95; d++) {
        uint8_t c[sizeof b];
        memcpy(c, b, n);
        memcpy(c + 52, &d, 8); /* durable: the 5th u64 from byte 20 (replwire.h) */
        int rc = dec(c, n, &m);
        CHECK(rc == RW_E_CHECK, "an ACK with durable %llu (true 90, written 95) gave %d, not RW_E_CHECK",
              (unsigned long long)d, rc);
    }
    CHECK(flips_refused(b, n, 0, n, "ACK") == 0, "a flipped ACK decoded");
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
