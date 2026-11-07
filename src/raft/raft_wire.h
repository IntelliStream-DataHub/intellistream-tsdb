/*
 * Copyright 2026 IntelliStream AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 *
 * Copied from intellistream-objects src/raft (src/raft/UPSTREAM names the commit) by tools/sync-raft.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* Raft messages on the wire (plan section 6): one frame per message, little-endian, entries
 * carried inline. Every decoder bounds-checks against what the frame actually holds; a message
 * comes from a peer, and a peer can be wrong.
 *
 * Header, 32 bytes: u8 type, u8 flags, u16 fence flags, u32 from, u32 to, u32 restore generation,
 * u64 term, u64 a. The fence fields (bytes 2-3 and 12-15) are not part of the message the core
 * sees: the transport stamps them after encoding and reads them before stepping (raft_net.c, plan
 * 28h 3b). An older build writes zeros there and never reads them, which is why they are where they
 * are -- byte 1 is checked by the decoder, bytes 2-3 and 12-15 never were.
 * Body by type:
 *   PREVOTE, VOTE            a = last_index; u64 last_term; flags bit 0 = transfer
 *   PREVOTE_RESP, VOTE_RESP  flags bit 0 = granted
 *   APPEND                   a = prev_index; u64 prev_term, u64 commit, u32 seq, u32 n, then n
 *                            entries of {u64 index, u64 term, u8 type, u8 pad[3], u32 len, bytes}
 *   APPEND_RESP              a = index; u64 hint_index, u64 hint_term, u32 seq, u32 zero;
 *                            flags bit 0 = ok
 *   SNAPSHOT                 a = index; u64 term of the image, u16 length, the configuration in
 *                            its own wire form; the image travels beside the message
 *   TIMEOUT_NOW              nothing more
 */
#ifndef ISO_RAFT_RAFT_WIRE_H
#define ISO_RAFT_RAFT_WIRE_H

#include "proto/frame.h"
#include "raft/raft.h"

#define RAFT_WIRE_HDR 32
#define RAFT_WIRE_APPEND_HDR 24 /* an APPEND's prev_term, commit, seq and n, before its entries */
#define RAFT_WIRE_ENTRY_HDR 24
#define RAFT_WIRE_MAX_ENTRIES 4096

/* What an APPEND of one entry adds to the entry: the message's head, the APPEND's and the entry's
 * own. An entry of `len` bytes reaches a follower in a message of `len` plus this. */
#define RAFT_WIRE_APPEND_OVERHEAD (RAFT_WIRE_HDR + RAFT_WIRE_APPEND_HDR + RAFT_WIRE_ENTRY_HDR)

/* **The largest entry replication can carry** (plan section 23, #161). An entry travels inside an
 * APPEND, an APPEND is one frame, and a frame's payload is at most ISO_FRAME_MAX_PAYLOAD -- so an
 * entry longer than a frame less the message's own head, the APPEND's and the entry's is one that
 * no follower can ever be sent: `iso_raft_net_send` drops the message and counts it. What a node's
 * transport admits can be less (its buffers bound it), never more, which is why this is the bound
 * a log's segment is sized by: a segment that holds this holds anything a leader can replicate. */
#define RAFT_WIRE_ENTRY_MAX (ISO_FRAME_MAX_PAYLOAD - RAFT_WIRE_APPEND_OVERHEAD)

/* **The largest entry one APPEND of a transport carries** (plan section 23, #163), for a transport
 * whose messages are at most `message_max` bytes -- `iso_xport_max_payload`, which a node's buffers
 * make about half the frame's bound. A leader that proposes a longer entry has proposed one no
 * follower can be sent: the transport refuses the message and only counts it, the core resends it
 * at every heartbeat, nothing after it commits, and every proposal is BUSY once the uncommitted
 * budget is full. Never more than RAFT_WIRE_ENTRY_MAX, because the frame bounds every transport; 0
 * when a message that small carries no entry at all. */
static inline size_t raft_wire_entry_max(size_t message_max)
{
	if (message_max > ISO_FRAME_MAX_PAYLOAD)
		message_max = ISO_FRAME_MAX_PAYLOAD;
	return message_max > RAFT_WIRE_APPEND_OVERHEAD ? message_max - RAFT_WIRE_APPEND_OVERHEAD : 0;
}

/* **The restore fence in a raft frame's header** (plan 28h, 4086, step 3b). `gen` is the sender's
 * restore generation (0: its metadata was never restored from a backup) and `empty` says the sender
 * holds nothing -- an empty log over a store at index 0 -- so it takes and answers any generation.
 * Stamped into an encoded message of at least RAFT_WIRE_HDR bytes; read from a frame of any length,
 * where one shorter than the header reads as an older build's (0, false). */
#define RAFT_WIRE_FENCE_EMPTY 0x0001u /* bit 0 of the u16 at byte 2 */
void raft_wire_set_fence(void *buf, uint32_t gen, bool empty);
void raft_wire_get_fence(const void *buf, size_t len, uint32_t *gen, bool *empty);

/* Bytes needed to encode `m`; 0 when it cannot be encoded. */
size_t raft_wire_size(const raft_msg *m);

/* Encodes into `buf`; returns the length written, 0 when `cap` is too small. */
size_t raft_wire_encode(const raft_msg *m, void *buf, size_t cap);

/* Decodes `buf` into `m`. Entries go into `entries` (room for `max_entries`) and their payloads
 * point into `buf`; nothing is copied, so `buf` must outlive the use of `m`. */
bool raft_wire_decode(const void *buf, size_t len, raft_msg *m, raft_entry *entries,
		      unsigned max_entries);

#endif /* ISO_RAFT_RAFT_WIRE_H */
