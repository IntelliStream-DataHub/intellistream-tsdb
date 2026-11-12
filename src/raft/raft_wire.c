/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/raft (src/raft/UPSTREAM names the commit) by tools/sync-raft.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* Raft wire codec: see raft_wire.h. */
#include "raft/raft_wire.h"

#include <string.h>

static size_t body_size(const raft_msg *m)
{
	switch (m->type) {
	case RAFT_MSG_PREVOTE:
	case RAFT_MSG_VOTE:
		return 8;
	case RAFT_MSG_PREVOTE_RESP:
	case RAFT_MSG_VOTE_RESP:
	case RAFT_MSG_TIMEOUT_NOW:
		return 0;
	case RAFT_MSG_APPEND: {
		if (m->u.append.n > RAFT_WIRE_MAX_ENTRIES)
			return SIZE_MAX;
		size_t n = RAFT_WIRE_APPEND_HDR;
		for (unsigned i = 0; i < m->u.append.n; i++)
			n += RAFT_WIRE_ENTRY_HDR + m->u.append.entries[i].len;
		return n;
	}
	case RAFT_MSG_APPEND_RESP:
		return 24;
	case RAFT_MSG_SNAPSHOT:
		if (m->u.snapshot.config.n > RAFT_MEMBERS_MAX)
			return SIZE_MAX;
		return 8 + 2 + 4 + (size_t)m->u.snapshot.config.n * 5;
	default:
		return SIZE_MAX;
	}
}

size_t raft_wire_size(const raft_msg *m)
{
	size_t b = body_size(m);
	return b == SIZE_MAX ? 0 : RAFT_WIRE_HDR + b;
}

/*
 * raft_wire_set_fence
 *		Stamp the sender's restore generation and its "empty" bit into an encoded message.
 *
 * `buf` holds a message raft_wire_encode wrote, so at least RAFT_WIRE_HDR bytes. Bits of the u16
 * at byte 2 other than RAFT_WIRE_FENCE_EMPTY stay zero: they are a later build's to define.
 */
void raft_wire_set_fence(void *buf, uint32_t gen, bool empty)
{
	uint8_t *p = buf;
	iso_put_u16le(p + 2, empty ? RAFT_WIRE_FENCE_EMPTY : 0);
	iso_put_u32le(p + 12, gen);
}

/*
 * raft_wire_get_fence
 *		Read the restore fence a frame carries: what raft_wire_set_fence wrote, or (0, false).
 *
 * Unknown bits at byte 2 are ignored rather than refused, so that a later build may define them
 * without this one dropping its frames. A frame shorter than a header (which raft_wire_decode
 * refuses anyway) reads as an older build's.
 */
void raft_wire_get_fence(const void *buf, size_t len, uint32_t *gen, bool *empty)
{
	const uint8_t *p = buf;
	if (len < RAFT_WIRE_HDR) {
		*gen = 0;
		*empty = false;
		return;
	}
	*empty = (iso_get_u16le(p + 2) & RAFT_WIRE_FENCE_EMPTY) != 0;
	*gen = iso_get_u32le(p + 12);
}

size_t raft_wire_encode(const raft_msg *m, void *buf, size_t cap)
{
	size_t need = raft_wire_size(m);
	if (!need || cap < need || m->from == 0 || m->to == 0)
		return 0;
	uint8_t *p = buf;
	memset(p, 0, RAFT_WIRE_HDR);
	p[0] = m->type;
	iso_put_u32le(p + 4, m->from);
	iso_put_u32le(p + 8, m->to);
	iso_put_u64le(p + 16, m->term);
	uint8_t *q = p + RAFT_WIRE_HDR;

	switch (m->type) {
	case RAFT_MSG_PREVOTE:
	case RAFT_MSG_VOTE:
		p[1] = m->u.vote.transfer ? 1 : 0;
		iso_put_u64le(p + 24, m->u.vote.last_index);
		iso_put_u64le(q, m->u.vote.last_term);
		break;
	case RAFT_MSG_PREVOTE_RESP:
	case RAFT_MSG_VOTE_RESP:
		p[1] = m->u.vote_resp.granted ? 1 : 0;
		break;
	case RAFT_MSG_APPEND: {
		iso_put_u64le(p + 24, m->u.append.prev_index);
		iso_put_u64le(q, m->u.append.prev_term);
		iso_put_u64le(q + 8, m->u.append.commit);
		iso_put_u32le(q + 16, m->u.append.seq);
		iso_put_u32le(q + 20, m->u.append.n);
		q += RAFT_WIRE_APPEND_HDR;
		for (unsigned i = 0; i < m->u.append.n; i++) {
			const raft_entry *e = &m->u.append.entries[i];
			iso_put_u64le(q, e->index);
			iso_put_u64le(q + 8, e->term);
			q[16] = e->type;
			q[17] = q[18] = q[19] = 0;
			iso_put_u32le(q + 20, e->len);
			if (e->len)
				memcpy(q + 24, e->data, e->len);
			q += RAFT_WIRE_ENTRY_HDR + e->len;
		}
		break;
	}
	case RAFT_MSG_APPEND_RESP:
		p[1] = m->u.append_resp.ok ? 1 : 0;
		iso_put_u64le(p + 24, m->u.append_resp.index);
		iso_put_u64le(q, m->u.append_resp.hint_index);
		iso_put_u64le(q + 8, m->u.append_resp.hint_term);
		iso_put_u32le(q + 16, m->u.append_resp.seq);
		iso_put_u32le(q + 20, 0);
		break;
	case RAFT_MSG_SNAPSHOT: {
		iso_put_u64le(p + 24, m->u.snapshot.index);
		iso_put_u64le(q, m->u.snapshot.term);
		size_t cl = raft_config_encode(&m->u.snapshot.config, q + 10, need - RAFT_WIRE_HDR - 10);
		if (!cl)
			return 0;
		iso_put_u16le(q + 8, (uint16_t)cl);
		break;
	}
	default:
		break;
	}
	return need;
}

bool raft_wire_decode(const void *buf, size_t len, raft_msg *m, raft_entry *entries,
		      unsigned max_entries)
{
	const uint8_t *p = buf;
	if (len < RAFT_WIRE_HDR)
		return false;
	memset(m, 0, sizeof *m);
	m->type = p[0];
	uint8_t flags = p[1];
	m->from = iso_get_u32le(p + 4);
	m->to = iso_get_u32le(p + 8);
	m->term = iso_get_u64le(p + 16);
	uint64_t a = iso_get_u64le(p + 24);
	const uint8_t *q = p + RAFT_WIRE_HDR;
	size_t body = len - RAFT_WIRE_HDR;
	if (m->from == 0 || m->to == 0 || (flags & ~1u))
		return false;

	switch (m->type) {
	case RAFT_MSG_PREVOTE:
	case RAFT_MSG_VOTE:
		if (body != 8)
			return false;
		m->u.vote.last_index = a;
		m->u.vote.last_term = iso_get_u64le(q);
		m->u.vote.transfer = flags & 1;
		return true;
	case RAFT_MSG_PREVOTE_RESP:
	case RAFT_MSG_VOTE_RESP:
		if (body != 0)
			return false;
		m->u.vote_resp.granted = flags & 1;
		return true;
	case RAFT_MSG_TIMEOUT_NOW:
		return body == 0;
	case RAFT_MSG_APPEND: {
		if (body < RAFT_WIRE_APPEND_HDR || flags)
			return false;
		m->u.append.prev_index = a;
		m->u.append.prev_term = iso_get_u64le(q);
		m->u.append.commit = iso_get_u64le(q + 8);
		m->u.append.seq = iso_get_u32le(q + 16);
		uint32_t n = iso_get_u32le(q + 20);
		if (n > RAFT_WIRE_MAX_ENTRIES || n > max_entries)
			return false;
		q += RAFT_WIRE_APPEND_HDR;
		body -= RAFT_WIRE_APPEND_HDR;
		for (uint32_t i = 0; i < n; i++) {
			if (body < RAFT_WIRE_ENTRY_HDR)
				return false;
			raft_entry *e = &entries[i];
			e->index = iso_get_u64le(q);
			e->term = iso_get_u64le(q + 8);
			e->type = q[16];
			e->len = iso_get_u32le(q + 20);
			if (q[17] || q[18] || q[19] || e->type > RAFT_ENTRY_CONFIG ||
			    e->len > body - RAFT_WIRE_ENTRY_HDR)
				return false;
			e->data = q + 24;
			q += RAFT_WIRE_ENTRY_HDR + e->len;
			body -= RAFT_WIRE_ENTRY_HDR + e->len;
		}
		if (body != 0)
			return false;
		m->u.append.entries = entries;
		m->u.append.n = n;
		return true;
	}
	case RAFT_MSG_APPEND_RESP:
		if (body != 24 || iso_get_u32le(q + 20) != 0)
			return false;
		m->u.append_resp.ok = flags & 1;
		m->u.append_resp.index = a;
		m->u.append_resp.hint_index = iso_get_u64le(q);
		m->u.append_resp.hint_term = iso_get_u64le(q + 8);
		m->u.append_resp.seq = iso_get_u32le(q + 16);
		return true;
	case RAFT_MSG_SNAPSHOT: {
		if (body < 10 || flags)
			return false;
		m->u.snapshot.index = a;
		m->u.snapshot.term = iso_get_u64le(q);
		size_t cl = iso_get_u16le(q + 8);
		if (cl != body - 10 || !raft_config_decode(q + 10, cl, &m->u.snapshot.config))
			return false;
		return true;
	}
	default:
		return false;
	}
}
