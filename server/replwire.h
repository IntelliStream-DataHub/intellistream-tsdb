/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The replication driver's payloads on the data links (server/peerlink.h carries them as opaque frames of the driver's
 * types; server/repld.c sends and receives them; docs/design-cluster.md 5.1-5.2, C25).
 *
 * Log link, frame type RW_T_MSG: one `struct repl_msg` of the core (src/repl.h) of one volume, little endian:
 *   u8 version (RW_VERSION), u8 msg type (REPL_M_*), u16 flags (bit 0: resync), u32 volume,
 *   i32 from, to, leader, u64 epoch, prev_pos, prev_epoch, commit, durable, written, rid, last_epoch, end, start,
 *   the vmeta (u64 epoch, isr_version, i32 leader, u32 isr, unanswered, u64 rid[REPL_MAX_NODES]),
 *   u32 nrec; an APPEND then has u64 seq, u64 off (where the first record lies in the leader's WAL segment `seq`),
 *   nrec descriptors (u64 pos, u64 epoch, u32 n, u32 len: the record's bytes) and the records' bytes, back to back.
 * The records of an APPEND are whole WAL records of one segment, byte for byte what the leader's segment holds (the
 * replica writes them at the same place: replica_append checks them there). The descriptors must be contiguous from
 * prev_pos + 1, every n and len at least 1, positions below 2^62, and the lengths must add up to the bytes exactly.
 * Any other type has nrec 0 and nothing after it.
 *
 * Log link, frame type RW_T_FETCH (replica -> leader): u8 version, u32 volume, u64 pos (the position of the COMMIT
 * marker), u16 len, the name that marker gives a file the replica could not stage (replica_missing); the leader sends
 * it over the bulk link (eng_leader_file_open serves a name only for the marker that named it)
 * or, when its segment went, a REPL_M_SNAPSHOT.
 *
 * Bulk link, the opaque meta of a file (pl_job.meta): u8 version, u32 volume, u64 size, u16 len, the marker's name.
 *
 * The decoders read exactly the bytes they are given, never trust a length, and are fuzzed (tests/fuzz/fuzz_replframe.c).
 */
#ifndef TSDBD_REPLWIRE_H
#define TSDBD_REPLWIRE_H

#include <stddef.h>
#include <stdint.h>

#include "../src/repl.h"

#define RW_VERSION 1
enum { RW_T_MSG = 1, RW_T_FETCH = 2 };
#define RW_HEAD 196         /* the fixed part of an RW_T_MSG */
#define RW_DESC 24          /* one record descriptor */
#define RW_APPEND_HEAD 16   /* seq, off */
#define RW_NAME_MAX 255

struct rw_msg {
    uint32_t volume;
    struct repl_msg m;     /* m.rec = recs */
    uint64_t seq, off;     /* APPEND */
    struct repl_rec *recs; /* decode: the caller's array of `cap` */
    uint32_t *lens;        /* decode: the caller's array of `cap` */
    const uint8_t *bytes;  /* the records' bytes (inside the decoded buffer) */
    size_t nbytes;
};

/* The size of an encoded message with nrec records of nbytes bytes. */
static inline size_t rw_msg_size(uint32_t nrec, size_t nbytes)
{
    return RW_HEAD + (nrec ? RW_APPEND_HEAD + (size_t)nrec * RW_DESC + nbytes : 0);
}
/* Encodes m (its m.rec / nrec for an APPEND, with lens[] and the bytes) into out (cap bytes). Returns the length, 0 if
 * it does not fit or the message breaks the rules above (the encoder checks what the decoder checks). */
size_t rw_encode_msg(uint8_t *out, size_t cap, uint32_t volume, const struct repl_msg *m, const uint32_t *lens,
                     uint64_t seq, uint64_t off, const void *bytes, size_t nbytes);
/* Decodes p[0, n). recs and lens are arrays of cap entries (an APPEND with more records is refused). 0, or -1 when the
 * bytes are not a well formed message. Pointers in *out point into p. */
int rw_decode_msg(const uint8_t *p, size_t n, struct rw_msg *out, struct repl_rec *recs, uint32_t *lens, uint32_t cap);

size_t rw_encode_fetch(uint8_t *out, size_t cap, uint32_t volume, uint64_t pos, const char *name);
/* name gets a NUL-terminated copy (cap >= RW_NAME_MAX + 1). 0 or -1. */
int rw_decode_fetch(const uint8_t *p, size_t n, uint32_t *volume, uint64_t *pos, char *name, size_t cap);

size_t rw_encode_bulk_meta(uint8_t *out, size_t cap, uint32_t volume, uint64_t size, const char *name);
int rw_decode_bulk_meta(const uint8_t *p, size_t n, uint32_t *volume, uint64_t *size, char *name, size_t cap);

#endif
