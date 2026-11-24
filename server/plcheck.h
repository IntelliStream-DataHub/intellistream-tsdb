/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The data links' frame check (docs/design-rdma.md §6 and D1; peerlink.h has the wire). Integrity, not
 * authentication: the owner's decision of 2026-11-24 is that the cluster runs on its own servers and its own network,
 * so a frame needs to be protected against corruption, loss, repetition and reordering, not against forgery. The
 * handshake still proves the cluster secret both ways with HMAC-SHA256 (once per connection), so a server of another
 * cluster or a wrong address cannot open a link; what it no longer gives is a key over every frame.
 *
 * Check type 1 (PL_CHECK_XXH3): XXH3-128 (xxHash 0.8.3, server/xxhash, unchanged; BSD-2-Clause) of the frame's type,
 * header and payload, as they are on the wire and contiguous there, seeded with `salt ^ seq`: seq is the frame's
 * number in its direction (from 0 per connection), salt a 64-bit value per connection and direction that both ends
 * derive from the handshake's transcript (HMAC under the secret, labels "tsdb-link-x>" and "tsdb-link-x<"). The seq
 * makes a dropped, repeated or reordered frame fail its check; the salt makes a frame of an earlier connection fail
 * in a new one (frames count from 0 again on every connection). 16 bytes on the wire.
 *
 * The type is negotiated (HELLO offers a bit set, HELLO_ACK names the one chosen; both inside the handshake's MACs),
 * so a keyed MAC can be added later as another type (design-rdma.md §6: AES-128-GMAC is the fastest measured) without
 * changing the frame layout beyond the check's length.
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

#if defined(__AVX512F__) && !defined(XXH_VECTOR)
#define XXH_VECTOR 2 /* XXH_AVX2 */
#endif
#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

enum { PL_CHECK_XXH3 = 1 };
#define PL_CHECK_LEN 16                                /* every type's length on the wire, so far */
#define PL_CHECKS_KNOWN (1u << PL_CHECK_XXH3)          /* the types this build can run, as HELLO's bit set */

/* the check of one frame's bytes (type, header, payload: contiguous on the wire) under its seed */
static inline void pl_check(int type, uint64_t seed, const void *p, size_t n, uint8_t out[PL_CHECK_LEN])
{
    (void)type; /* one type so far */
    XXH128_hash_t h = XXH3_128bits_withSeed(p, n, seed);
    for (int i = 0; i < 8; i++) {
        out[i] = (uint8_t)(h.low64 >> (8 * i));
        out[8 + i] = (uint8_t)(h.high64 >> (8 * i));
    }
}

#endif
