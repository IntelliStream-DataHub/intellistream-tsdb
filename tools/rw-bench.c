/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * rw-bench: what the replication driver's messages cost one core to encode and decode (server/replwire.c), and what
 * the driver check of RW_VERSION 3 (rd-drvcheck) adds: ns per message for an ACK and for APPENDs of one 4 KiB record
 * and of sixteen 4 KiB records (64 KiB), and the MB/s of record bytes one core turns through encode + decode at
 * those sizes, which bounds a LOG link's throughput from the driver's side. Built against the replwire.c of the tree
 * (make build/rw-bench) or of another (cc -I<old>/server ... <old>/server/replwire.c) so the two can be compared.
 * Also the check alone over the covered bytes: XXH3-64 (the driver's) and CRC32C (SSE4.2), the candidates.
 *
 *   rw-bench [--secs S]      one line per case: name, ns per message, MB/s of record bytes
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#if defined(__x86_64__) && defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

#include "plcheck.h"
#include "replwire.h"

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static volatile uint64_t sink;

static void one(const char *name, uint32_t nrec, uint32_t reclen, double secs)
{
    struct repl_rec rr[16];
    uint32_t lens[16];
    size_t nbytes = (size_t)nrec * reclen;
    uint8_t *text = malloc(nbytes ? nbytes : 1);
    for (size_t i = 0; i < nbytes; i++)
        text[i] = (uint8_t)(i * 31 + 7);
    for (uint32_t i = 0; i < nrec; i++)
        rr[i] = (struct repl_rec){.pos = 11 + i, .epoch = 3, .n = 1}, lens[i] = reclen;
    struct repl_msg m = {.type = nrec ? REPL_M_APPEND : REPL_M_ACK, .from = nrec ? 0 : 1, .to = nrec ? 1 : 0,
                         .epoch = 3, .leader = nrec ? 0 : -1, .prev_pos = 10, .prev_epoch = 3, .commit = 9,
                         .durable = 90, .written = 95, .nrec = nrec, .rec = rr, .meta.leader = -1};
    size_t cap = rw_msg_size(nrec, nbytes);
    uint8_t *b = malloc(cap);
    struct repl_rec dr[16];
    uint32_t dl[16];
    struct rw_msg out;
    uint64_t n = 0;
    double t0 = now_s(), t;
    do {
        for (int k = 0; k < 1000; k++) {
            size_t l = rw_encode_msg(b, cap, 4, &m, lens, 1, 4096, text, nbytes);
            if (!l || rw_decode_msg(b, l, &out, dr, dl, 16) != 0) {
                fprintf(stderr, "%s: encode %zu or decode failed\n", name, l);
                exit(1);
            }
            sink += out.m.durable;
        }
        n += 1000;
    } while ((t = now_s() - t0) < secs);
    printf("%-22s %8.1f ns/msg  %9.0f MB/s of records\n", name, t * 1e9 / (double)n,
           nbytes ? (double)nbytes * (double)n / t / 1e6 : 0.0);
    free(b);
    free(text);
}

static void hash(const char *name, size_t len, int crc, double secs)
{
    uint8_t buf[1024];
    for (size_t i = 0; i < sizeof buf; i++)
        buf[i] = (uint8_t)(i * 13 + 5);
    uint64_t n = 0, acc = 0;
    double t0 = now_s(), t;
    do {
        for (int k = 0; k < 10000; k++) {
            buf[0] = (uint8_t)k;
            if (!crc) {
                acc += XXH3_64bits_withSeed(buf, len, 17);
            } else {
#if defined(__x86_64__) && defined(__SSE4_2__)
                uint64_t c = ~0ull;
                size_t i = 0;
                for (; i + 8 <= len; i += 8) {
                    uint64_t v;
                    memcpy(&v, buf + i, 8);
                    c = _mm_crc32_u64(c, v);
                }
                for (; i < len; i++)
                    c = _mm_crc32_u8((uint32_t)c, buf[i]);
                acc += c;
#else
                acc += len;
#endif
            }
        }
        n += 10000;
    } while ((t = now_s() - t0) < secs);
    sink += acc;
    printf("%-22s %8.1f ns/check over %zu bytes\n", name, t * 1e9 / (double)n, len);
}

int main(int argc, char **argv)
{
    double secs = argc > 2 && !strcmp(argv[1], "--secs") ? atof(argv[2]) : 1.0;
    printf("RW_VERSION %d\n", RW_VERSION);
    one("ack", 0, 0, secs);
    one("append 1 x 4 KiB", 1, 4096, secs);
    one("append 16 x 4 KiB", 16, 4096, secs);
    one("append 1 x 64 KiB", 1, 65536, secs);
    for (int crc = 0; crc < 2; crc++) {
        hash(crc ? "crc32c ack head" : "xxh3-64 ack head", 212, crc, secs);
        hash(crc ? "crc32c append 1 desc" : "xxh3-64 append 1 desc", 212 + 16 + 24, crc, secs);
        hash(crc ? "crc32c append 16 desc" : "xxh3-64 append 16 desc", 212 + 16 + 16 * 24, crc, secs);
    }
    return 0;
}
