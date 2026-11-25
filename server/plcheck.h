/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The data links' frame check (docs/design-rdma.md §6 and D1; peerlink.h has the wire). Integrity, not
 * authentication, and an option (`--peer-check none|header|full`, key `cluster.peer.check`; the owner's decision of
 * 2026-11-25): the wire is trusted to Ethernet's FCS and TCP's checksum (InfiniBand's ICRC on RDMA), as NFS over
 * TCP/RDMA trusts it, and every payload that matters carries a checksum of its own that its receiver verifies (WAL
 * records, bulk and snapshot files, chunk cells). What a flipped bit in a header or control field could do is limited
 * by bounds and consistency checks on the receive side, in every mode. The handshake still proves the cluster secret
 * both ways with HMAC-SHA256 (once per connection).
 *
 * The levels are check types (HELLO offers a bit set, HELLO_ACK names the one chosen; both inside the handshake's
 * MACs). Each end offers the types at or above its own level and the acceptor picks the lowest of those at or above
 * its own, so a link runs at the STRONGER of the two ends' levels: turning it on at one node protects all its links.
 * 0 is never a type (check_choose's "no common type").
 *
 *  - PL_CHECK_NONE (1, the default): no check bytes on the wire.
 *  - PL_CHECK_HEADER (2): XXH3-64 (8 bytes) over the frame's length, type, compression header and the first
 *    PL_CHECK_SPAN bytes of its payload as the driver gave it (decompressed): the drivers' heads (a replication
 *    message's 212 bytes, a forward envelope, a chunk put's meta, a bulk frame's fields) without the bulk of the
 *    bytes, so it costs a fixed amount per frame (at most 265 bytes hashed, 23-31 ns). Of an APPEND it covers the
 *    message head, the APPEND head and descriptor 0 only (212 + 16 + 24 = 252 bytes): the later descriptors were
 *    guarded by the receive side's bounds alone; since rd-drvcheck the driver check (replwire.h, RW_VERSION 3) covers
 *    every descriptor at every level. XXH3-64 rather than CRC32C: already vendored and portable
 *    C, one hash for both checks; the two are within 15-25 ns per frame of each other either way (CRC32C faster on
 *    an i5-13400, slower on a Xeon E5; design-rdma.md §6 has the numbers), so the choice is not about speed.
 *  - PL_CHECK_FULL (3): XXH3-128 (16 bytes) over the type, header and payload as they are on the wire (rd-mac's
 *    check, the fastest measured; it reads every byte again, so on RDMA it gives up zero copy's CPU saving).
 *
 * Both checks are seeded with `salt ^ seq`: seq is the frame's number in its direction (from 0 per connection), salt
 * a 64-bit value per connection and direction that both ends derive from the handshake's transcript (HMAC under the
 * secret, labels "tsdb-link-x>" and "tsdb-link-x<"). The seq makes a dropped, repeated or reordered frame fail its
 * check; the salt makes a frame of an earlier connection fail in a new one. Under PL_CHECK_NONE nothing reads them.
 *
 * Inline, with every xxHash symbol static (XXH_INLINE_ALL): nothing is exported from libtsdb or tsdbd, and the
 * header compiles at whatever level its includer does (SSE2 in `make dist`, which is all the x86-64-v2 level has;
 * tools/check-isa.sh sees no wider instruction). AVX-512 is capped to AVX2: its accumulator is a 64-byte aligned
 * local, which ASan's fake stack (32-byte frames) would fault on, and the servers it runs on have no AVX-512.
 */
#ifndef TSDB_PLCHECK_H
#define TSDB_PLCHECK_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#if defined(__AVX512F__) && !defined(XXH_VECTOR)
#define XXH_VECTOR 2 /* XXH_AVX2 */
#endif
#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

enum { PL_CHECK_NONE = 1, PL_CHECK_HEADER = 2, PL_CHECK_FULL = 3 };
#define PL_CHECK_LEN_MAX 16 /* the longest check on the wire (PL_CHECK_FULL) */
#define PL_CHECK_SPAN 256   /* PL_CHECK_HEADER: the payload bytes it covers, from the start */
#define PL_CHECKS_KNOWN ((1u << PL_CHECK_NONE) | (1u << PL_CHECK_HEADER) | (1u << PL_CHECK_FULL))

/* the check's length on the wire for a type (0 for none or an unknown type) */
static inline size_t pl_check_len(int type)
{
    return type == PL_CHECK_FULL ? 16 : type == PL_CHECK_HEADER ? 8 : 0;
}

/* the HELLO bit set an end of level `level` offers: every type it runs at or above its level */
static inline unsigned pl_check_offer(int level)
{
    return level < 1 || level > 7 ? 0 : PL_CHECKS_KNOWN & ~((1u << level) - 1);
}

static inline const char *pl_check_name(int type)
{
    return type == PL_CHECK_NONE ? "none" : type == PL_CHECK_HEADER ? "header" : type == PL_CHECK_FULL ? "full" : "?";
}

/* "none", "header" or "full" -> the type; -1 for anything else */
static inline int pl_check_parse(const char *s)
{
    return !strcmp(s, "none") ? PL_CHECK_NONE : !strcmp(s, "header") ? PL_CHECK_HEADER
           : !strcmp(s, "full") ? PL_CHECK_FULL : -1;
}

/* PL_CHECK_FULL: the check of one frame's bytes (type, header, payload: contiguous on the wire) under its seed */
static inline void pl_check(uint64_t seed, const void *p, size_t n, uint8_t out[16])
{
    XXH128_hash_t h = XXH3_128bits_withSeed(p, n, seed);
    for (int i = 0; i < 8; i++) {
        out[i] = (uint8_t)(h.low64 >> (8 * i));
        out[8 + i] = (uint8_t)(h.high64 >> (8 * i));
    }
}

/* PL_CHECK_HEADER: the check of a frame of wire length `len` and type byte `type`, `h` its compression header (hn 4
 * bytes, or 0 when not compressed), whose payload as the driver gave it is a[0..an) then b[0..bn) (two pieces: the
 * sender has a control frame's fields apart from its bytes); only the first PL_CHECK_SPAN of those are covered. */
static inline void pl_check_header(uint64_t seed, uint32_t len, uint8_t type, const uint8_t *h, size_t hn,
                                   const uint8_t *a, size_t an, const uint8_t *b, size_t bn, uint8_t out[8])
{
    uint8_t buf[4 + 1 + 4 + PL_CHECK_SPAN];
    size_t k = 0;
    for (int i = 0; i < 4; i++)
        buf[k++] = (uint8_t)(len >> (8 * i));
    buf[k++] = type;
    if (hn > 4)
        hn = 4;
    if (hn)
        memcpy(buf + k, h, hn);
    k += hn;
    size_t na = an < PL_CHECK_SPAN ? an : PL_CHECK_SPAN;
    if (na)
        memcpy(buf + k, a, na);
    k += na;
    size_t nb = PL_CHECK_SPAN - na < bn ? PL_CHECK_SPAN - na : bn;
    if (nb)
        memcpy(buf + k, b, nb);
    k += nb;
    uint64_t v = XXH3_64bits_withSeed(buf, k, seed);
    for (int i = 0; i < 8; i++)
        out[i] = (uint8_t)(v >> (8 * i));
}

#endif
