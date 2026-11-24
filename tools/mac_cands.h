// SPDX-License-Identifier: AGPL-3.0-only
/* The frame MAC candidates of docs/design-rdma.md §6, for link-bench (`link-bench mac` and `run --mac NAME`): each
 * tags one frame from a per-direction key, its sequence number, its type and its payload, as peerlink's frame_tag.
 *
 *   hmac        HMAC-SHA256, 32-byte tag, tsdb_crypto.c's portable SHA-256 (peerlink up to cluster version 7)
 *   hmac-shani  the same bytes with the SHA extensions (sha_ni)
 *   gmac        AES-128-GMAC (AES-NI + PCLMULQDQ), the sequence number as the 96-bit nonce, 16-byte tag
 *   poly1305    Poly1305 under a one-time key from ChaCha20 (RFC 8439 2.6, the sequence number as the nonce), 16 bytes
 *   blake3      keyed BLAKE3 (tools/blake3, its own SIMD dispatch), the first 16 bytes of the output
 *
 * A candidate the CPU cannot run says so at mc_init (-1). Bench code: not linked into tsdbd. */
#ifndef TSDB_MAC_CANDS_H
#define TSDB_MAC_CANDS_H

#include <stddef.h>
#include <stdint.h>

enum { MC_NONE, MC_HMAC, MC_HMAC_SHANI, MC_GMAC, MC_POLY1305, MC_BLAKE3, MC_N };

struct mc {
    int kind;
    size_t taglen;
    _Alignas(64) uint8_t st[512]; /* the candidate's key schedule */
};

const char *mc_name(int kind);
int mc_kind(const char *name); /* -1 when unknown */
/* the key schedule of `kind` from a 32-byte link key; -1 when this CPU lacks the candidate's instructions */
int mc_init(struct mc *m, int kind, const uint8_t key[32]);
/* the tag of one frame: seq, type, then the payload as n1 + n2 bytes (header and body), written to out[taglen] */
void mc_tag(const struct mc *m, uint64_t seq, uint8_t type, const void *p1, size_t n1, const void *p2, size_t n2,
            uint8_t *out);
/* known answers of every candidate this CPU runs; 0 when all match, else prints the first mismatch */
int mc_selftest(void);

#endif
