/* SPDX-License-Identifier: Apache-2.0 */
/*
 * The coding of the HA bodies of protocol 13 (layouts: tsdb_protocol.h), written once: tsdbd, the C client, the
 * test servers and the fuzz harness include this header; the Rust, Python and Java clients have the same
 * functions in their own language, and tests/test_wire.c checks that all of them agree with these bytes.
 *
 * Every decoder takes a byte range it does not trust: it checks every length against the range, never reads
 * outside it, keeps no pointer past its argument (strings are pointers into it), and answers TSDB_ERR_INVALID for
 * anything malformed, trailing bytes the layout does not allow included. Every encoder is snprintf-like: it returns
 * the size the body needs and writes only when that fits in cap, so a caller can size a buffer first. An encoder
 * that is given something no decoder accepts (an address over TSDB_ADDR_MAX, too many entries) returns 0.
 * The host is little endian, as everywhere in tsdb (AGENTS.md: x86-64 and arm64 Linux, macOS).
 */
#ifndef TSDB_WIRE_H
#define TSDB_WIRE_H

#include "tsdb.h"
#include "tsdb_protocol.h"

#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_unavail {
    uint16_t vol;  /* TSDB_VOL_ANY: no one volume */
    uint8_t reason; /* TSDB_WHY_* */
    uint8_t flags;
    uint32_t epoch;
    int64_t since_ms;
    uint32_t retry_after_ms;
    uint64_t outage_id;
} tsdb_unavail;

typedef struct tsdb_moved {
    uint16_t vol;
    uint32_t epoch;
    uint8_t flags; /* TSDB_MOVED_HANDOFF */
    uint32_t node_id;
    const char *addr; /* not NUL terminated: addr_len bytes */
    uint16_t addr_len;
} tsdb_moved;

typedef struct tsdb_partial_vol {
    uint16_t vol;
    int16_t status; /* the volume's own status: TSDB_ERR_UNAVAILABLE, _MOVED, _DROPPED, or 0 */
} tsdb_partial_vol;

/* ------------------------------------------------------------ small readers and writers */

static inline uint16_t tsdb_wire_rd16(const uint8_t *p)
{
    uint16_t v;
    memcpy(&v, p, 2);
    return v;
}
static inline uint32_t tsdb_wire_rd32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}
static inline uint64_t tsdb_wire_rd64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}
static inline void tsdb_wire_wr16(uint8_t *p, uint16_t v) { memcpy(p, &v, 2); }
static inline void tsdb_wire_wr32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static inline void tsdb_wire_wr64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }

/* ------------------------------------------------------------ UNAVAILABLE */

static inline size_t tsdb_wire_put_unavail(uint8_t *out, size_t cap, const tsdb_unavail *e, unsigned n)
{
    if (n > TSDB_UNAVAIL_MAX)
        return 0;
    size_t need = 2 + (size_t)n * TSDB_UNAVAIL_ENTRY;
    if (need > cap)
        return need;
    tsdb_wire_wr16(out, (uint16_t)n);
    uint8_t *q = out + 2;
    for (unsigned i = 0; i < n; i++, q += TSDB_UNAVAIL_ENTRY) {
        tsdb_wire_wr16(q, e[i].vol);
        q[2] = e[i].reason;
        q[3] = e[i].flags;
        tsdb_wire_wr32(q + 4, e[i].epoch);
        tsdb_wire_wr64(q + 8, (uint64_t)e[i].since_ms);
        tsdb_wire_wr32(q + 16, e[i].retry_after_ms);
        tsdb_wire_wr64(q + 20, e[i].outage_id);
    }
    return need;
}

/* Reads u16 n and n entries from the start of p. Stores the first `cap` of them in e (cap may be 0), sets *n to how
 * many the body holds and *used to the bytes it took. 0, or TSDB_ERR_INVALID (truncated, or n over TSDB_UNAVAIL_MAX).
 * e, n and used may be NULL. */
static inline int tsdb_wire_get_unavail(const uint8_t *p, size_t len, tsdb_unavail *e, unsigned cap, unsigned *n,
                                        size_t *used)
{
    if (len < 2)
        return TSDB_ERR_INVALID;
    unsigned cnt = tsdb_wire_rd16(p);
    if (cnt > TSDB_UNAVAIL_MAX || len - 2 < (size_t)cnt * TSDB_UNAVAIL_ENTRY)
        return TSDB_ERR_INVALID;
    const uint8_t *q = p + 2;
    for (unsigned i = 0; i < cnt && e && i < cap; i++, q += TSDB_UNAVAIL_ENTRY) {
        e[i].vol = tsdb_wire_rd16(q);
        e[i].reason = q[2];
        e[i].flags = q[3];
        e[i].epoch = tsdb_wire_rd32(q + 4);
        e[i].since_ms = (int64_t)tsdb_wire_rd64(q + 8);
        e[i].retry_after_ms = tsdb_wire_rd32(q + 16);
        e[i].outage_id = tsdb_wire_rd64(q + 20);
    }
    if (n)
        *n = cnt;
    if (used)
        *used = 2 + (size_t)cnt * TSDB_UNAVAIL_ENTRY;
    return 0;
}

/* The optional trailing u64 resend_from of an UNAVAILABLE or MOVED body: rest is what follows the entries.
 * 1 and *out set when present, 0 when there are no bytes, TSDB_ERR_INVALID for any other length. */
static inline int tsdb_wire_get_resend(const uint8_t *rest, size_t len, uint64_t *out)
{
    if (len == 0)
        return 0;
    if (len != 8)
        return TSDB_ERR_INVALID;
    if (out)
        *out = tsdb_wire_rd64(rest);
    return 1;
}

/* The longest retry_after_ms of the entries, clamped to [TSDB_RETRY_MIN_MS, TSDB_RETRY_MAX_MS]; the minimum when
 * there are none. What a client sleeps (with its own jitter) before it sends the request again. */
static inline uint32_t tsdb_wire_retry_after(const tsdb_unavail *e, unsigned n)
{
    uint32_t best = 0;
    for (unsigned i = 0; i < n; i++)
        if (e[i].retry_after_ms > best)
            best = e[i].retry_after_ms;
    return best < TSDB_RETRY_MIN_MS ? TSDB_RETRY_MIN_MS : best > TSDB_RETRY_MAX_MS ? TSDB_RETRY_MAX_MS : best;
}

/* ------------------------------------------------------------ MOVED */

static inline size_t tsdb_wire_put_moved(uint8_t *out, size_t cap, const tsdb_moved *m, unsigned n)
{
    if (n > TSDB_UNAVAIL_MAX)
        return 0;
    size_t need = 2;
    for (unsigned i = 0; i < n; i++) {
        if (m[i].addr_len > TSDB_ADDR_MAX)
            return 0;
        need += 13 + m[i].addr_len;
    }
    if (need > cap)
        return need;
    tsdb_wire_wr16(out, (uint16_t)n);
    uint8_t *q = out + 2;
    for (unsigned i = 0; i < n; i++) {
        tsdb_wire_wr16(q, m[i].vol);
        tsdb_wire_wr32(q + 2, m[i].epoch);
        q[6] = m[i].flags;
        tsdb_wire_wr32(q + 7, m[i].node_id);
        tsdb_wire_wr16(q + 11, m[i].addr_len);
        if (m[i].addr_len)
            memcpy(q + 13, m[i].addr, m[i].addr_len);
        q += 13 + m[i].addr_len;
    }
    return need;
}

static inline int tsdb_wire_get_moved(const uint8_t *p, size_t len, tsdb_moved *m, unsigned cap, unsigned *n,
                                      size_t *used)
{
    if (len < 2)
        return TSDB_ERR_INVALID;
    unsigned cnt = tsdb_wire_rd16(p);
    if (cnt > TSDB_UNAVAIL_MAX)
        return TSDB_ERR_INVALID;
    size_t at = 2;
    for (unsigned i = 0; i < cnt; i++) {
        if (len - at < 13)
            return TSDB_ERR_INVALID;
        const uint8_t *q = p + at;
        unsigned al = tsdb_wire_rd16(q + 11);
        if (al > TSDB_ADDR_MAX || len - at - 13 < al)
            return TSDB_ERR_INVALID;
        if (m && i < cap) {
            m[i].vol = tsdb_wire_rd16(q);
            m[i].epoch = tsdb_wire_rd32(q + 2);
            m[i].flags = q[6];
            m[i].node_id = tsdb_wire_rd32(q + 7);
            m[i].addr = (const char *)q + 13;
            m[i].addr_len = (uint16_t)al;
        }
        at += 13 + al;
    }
    if (n)
        *n = cnt;
    if (used)
        *used = at;
    return 0;
}

/* ------------------------------------------------------------ PARTIAL */

/* v: nv volume statuses; not_applied: the bitmap of nrows bits, (nrows + 7) / 8 bytes (NULL: all clear); e: the
 * UNAVAILABLE entries of the refused volumes. */
static inline size_t tsdb_wire_put_partial(uint8_t *out, size_t cap, const tsdb_partial_vol *v, unsigned nv,
                                           uint32_t nrows, const uint8_t *not_applied, const tsdb_unavail *e,
                                           unsigned ne)
{
    if (nv > TSDB_UNAVAIL_MAX || ne > TSDB_UNAVAIL_MAX || nrows > TSDB_MAX_FRAME)
        return 0;
    size_t bits = ((size_t)nrows + 7) / 8;
    size_t head = 2 + (size_t)nv * 4 + 4 + bits;
    size_t need = head + 2 + (size_t)ne * TSDB_UNAVAIL_ENTRY;
    if (need > cap)
        return need;
    tsdb_wire_wr16(out, (uint16_t)nv);
    uint8_t *q = out + 2;
    for (unsigned i = 0; i < nv; i++, q += 4) {
        tsdb_wire_wr16(q, v[i].vol);
        tsdb_wire_wr16(q + 2, (uint16_t)v[i].status);
    }
    tsdb_wire_wr32(q, nrows);
    q += 4;
    if (not_applied)
        memcpy(q, not_applied, bits);
    else
        memset(q, 0, bits);
    tsdb_wire_put_unavail(out + head, 2 + (size_t)ne * TSDB_UNAVAIL_ENTRY, e, ne);
    return need;
}

/* Stores the first capv statuses and cape entries; *bits points into p at the bitmap. *nv, *ne: how many the body
 * holds. Nothing may follow the entries (a PARTIAL carries no resend_from: each row resends under its own seq). */
static inline int tsdb_wire_get_partial(const uint8_t *p, size_t len, tsdb_partial_vol *v, unsigned capv,
                                        unsigned *nv, uint32_t *nrows, const uint8_t **bits, tsdb_unavail *e,
                                        unsigned cape, unsigned *ne)
{
    if (len < 2)
        return TSDB_ERR_INVALID;
    unsigned cnt = tsdb_wire_rd16(p);
    if (cnt > TSDB_UNAVAIL_MAX || len - 2 < (size_t)cnt * 4 + 4)
        return TSDB_ERR_INVALID;
    for (unsigned i = 0; v && i < cnt && i < capv; i++) {
        v[i].vol = tsdb_wire_rd16(p + 2 + (size_t)i * 4);
        v[i].status = (int16_t)tsdb_wire_rd16(p + 4 + (size_t)i * 4);
    }
    size_t at = 2 + (size_t)cnt * 4;
    uint32_t rows = tsdb_wire_rd32(p + at);
    at += 4;
    size_t nb = ((size_t)rows + 7) / 8;
    if (rows > TSDB_MAX_FRAME || len - at < nb)
        return TSDB_ERR_INVALID;
    if (bits)
        *bits = p + at;
    at += nb;
    size_t used;
    unsigned ecnt;
    if (tsdb_wire_get_unavail(p + at, len - at, e, cape, &ecnt, &used) != 0 || used != len - at)
        return TSDB_ERR_INVALID;
    if (nv)
        *nv = cnt;
    if (nrows)
        *nrows = rows;
    if (ne)
        *ne = ecnt;
    return 0;
}

/* ------------------------------------------------------------ DROPPED */

static inline size_t tsdb_wire_put_dropped(uint8_t *out, size_t cap, uint16_t vol, int64_t dropped_before)
{
    if (cap >= 10) {
        tsdb_wire_wr16(out, vol);
        tsdb_wire_wr64(out + 2, (uint64_t)dropped_before);
    }
    return 10;
}

static inline int tsdb_wire_get_dropped(const uint8_t *p, size_t len, uint16_t *vol, int64_t *dropped_before)
{
    if (len != 10)
        return TSDB_ERR_INVALID;
    if (vol)
        *vol = tsdb_wire_rd16(p);
    if (dropped_before)
        *dropped_before = (int64_t)tsdb_wire_rd64(p + 2);
    return 0;
}

/* ------------------------------------------------------------ TOPOLOGY */

typedef struct tsdb_topo_node {
    uint32_t node_id;
    uint8_t alive;
    const char *addr;
    uint16_t addr_len;
} tsdb_topo_node;

typedef struct tsdb_topo_vol {
    uint16_t vol;
    uint8_t role, health;
    uint32_t epoch;
    uint8_t reason;
    uint16_t domain;
    int64_t since_ms;
    uint32_t owner;
    uint16_t server;
    const char *addr;
    uint16_t addr_len;
    const char *path;
    uint16_t path_len;
} tsdb_topo_vol;

/* The request: an empty payload is since_gen 0. */
static inline int tsdb_wire_get_topology_request(const uint8_t *p, size_t len, uint64_t *since_gen)
{
    if (len != 0 && len != 8)
        return TSDB_ERR_INVALID;
    if (since_gen)
        *since_gen = len ? tsdb_wire_rd64(p) : 0;
    return 0;
}

/* Appends to a growing reply: a writer for the sender, and an iterator for the reader. The encoder is split in
 * three calls so a server can stream nodes and volumes out of its tables without building arrays. Each returns its
 * bytes (written when they fit in cap), as the others do. */
static inline size_t tsdb_wire_put_topo_head(uint8_t *out, size_t cap, uint64_t gen, uint32_t nn)
{
    if (cap >= 12) {
        tsdb_wire_wr64(out, gen);
        tsdb_wire_wr32(out + 8, nn);
    }
    return 12;
}

static inline size_t tsdb_wire_put_topo_node(uint8_t *out, size_t cap, const tsdb_topo_node *n)
{
    if (n->addr_len > TSDB_ADDR_MAX)
        return 0;
    size_t need = 7 + n->addr_len;
    if (need <= cap) {
        tsdb_wire_wr32(out, n->node_id);
        out[4] = n->alive;
        tsdb_wire_wr16(out + 5, n->addr_len);
        if (n->addr_len)
            memcpy(out + 7, n->addr, n->addr_len);
    }
    return need;
}

static inline size_t tsdb_wire_put_topo_vol(uint8_t *out, size_t cap, const tsdb_topo_vol *v)
{
    if (v->addr_len > TSDB_ADDR_MAX || v->path_len > 4096)
        return 0;
    size_t need = 28 + (size_t)v->addr_len + 2 + v->path_len;
    if (need <= cap) {
        tsdb_wire_wr16(out, v->vol);
        out[2] = v->role;
        out[3] = v->health;
        tsdb_wire_wr32(out + 4, v->epoch);
        out[8] = v->reason;
        out[9] = 0;
        tsdb_wire_wr16(out + 10, v->domain);
        tsdb_wire_wr64(out + 12, (uint64_t)v->since_ms);
        tsdb_wire_wr32(out + 20, v->owner);
        tsdb_wire_wr16(out + 24, v->server);
        tsdb_wire_wr16(out + 26, v->addr_len);
        uint8_t *q = out + 28;
        if (v->addr_len)
            memcpy(q, v->addr, v->addr_len);
        q += v->addr_len;
        tsdb_wire_wr16(q, v->path_len);
        if (v->path_len)
            memcpy(q + 2, v->path, v->path_len);
    }
    return need;
}

/* The u32 count of volumes sits between the nodes and the volumes. */
static inline size_t tsdb_wire_put_u32(uint8_t *out, size_t cap, uint32_t v)
{
    if (cap >= 4)
        tsdb_wire_wr32(out, v);
    return 4;
}

typedef struct tsdb_topo_it {
    const uint8_t *p;
    size_t len, at;
    uint64_t gen;
    uint32_t nn, nv; /* nodes and volumes the body says it holds (nv is known once the nodes are read) */
    uint32_t node_i, vol_i;
} tsdb_topo_it;

static inline int tsdb_wire_topo_open(tsdb_topo_it *it, const uint8_t *p, size_t len)
{
    if (len < 12)
        return TSDB_ERR_INVALID;
    it->p = p;
    it->len = len;
    it->gen = tsdb_wire_rd64(p);
    it->nn = tsdb_wire_rd32(p + 8);
    if (it->nn > TSDB_TOPOLOGY_MAX_NODES)
        return TSDB_ERR_INVALID;
    it->at = 12;
    it->nv = 0;
    it->node_i = it->vol_i = 0;
    return 0;
}

/* The next node: 1, or 0 when the nodes are done (the volume count is read then), or TSDB_ERR_INVALID. */
static inline int tsdb_wire_topo_node(tsdb_topo_it *it, tsdb_topo_node *n)
{
    if (it->node_i == it->nn) {
        if (it->len - it->at < 4)
            return TSDB_ERR_INVALID;
        it->nv = tsdb_wire_rd32(it->p + it->at);
        if (it->nv > TSDB_TOPOLOGY_MAX_VOLS)
            return TSDB_ERR_INVALID;
        it->at += 4;
        it->node_i++; /* once */
        return 0;
    }
    if (it->node_i > it->nn)
        return 0;
    if (it->len - it->at < 7)
        return TSDB_ERR_INVALID;
    const uint8_t *q = it->p + it->at;
    unsigned al = tsdb_wire_rd16(q + 5);
    if (al > TSDB_ADDR_MAX || it->len - it->at - 7 < al)
        return TSDB_ERR_INVALID;
    n->node_id = tsdb_wire_rd32(q);
    n->alive = q[4];
    n->addr = (const char *)q + 7;
    n->addr_len = (uint16_t)al;
    it->at += 7 + al;
    it->node_i++;
    return 1;
}

/* The next volume: 1, 0 when done (and nothing follows: a body with trailing bytes is INVALID), or TSDB_ERR_INVALID.
 * Call only after tsdb_wire_topo_node returned 0. */
static inline int tsdb_wire_topo_vol(tsdb_topo_it *it, tsdb_topo_vol *v)
{
    if (it->vol_i == it->nv)
        return it->at == it->len ? 0 : TSDB_ERR_INVALID;
    if (it->len - it->at < 28)
        return TSDB_ERR_INVALID;
    const uint8_t *q = it->p + it->at;
    unsigned al = tsdb_wire_rd16(q + 26);
    size_t rest = it->len - it->at - 28;
    if (al > TSDB_ADDR_MAX || rest < (size_t)al + 2)
        return TSDB_ERR_INVALID;
    unsigned pl = tsdb_wire_rd16(q + 28 + al);
    if (pl > 4096 || rest - al - 2 < pl)
        return TSDB_ERR_INVALID;
    v->vol = tsdb_wire_rd16(q);
    v->role = q[2];
    v->health = q[3];
    v->epoch = tsdb_wire_rd32(q + 4);
    v->reason = q[8];
    v->domain = tsdb_wire_rd16(q + 10);
    v->since_ms = (int64_t)tsdb_wire_rd64(q + 12);
    v->owner = tsdb_wire_rd32(q + 20);
    v->server = tsdb_wire_rd16(q + 24);
    v->addr = (const char *)q + 28;
    v->addr_len = (uint16_t)al;
    v->path = (const char *)q + 30 + al;
    v->path_len = (uint16_t)pl;
    it->at += 30 + (size_t)al + pl;
    it->vol_i++;
    return 1;
}

/* ------------------------------------------------------------ an error reply's body, whatever the op */

/* Where the body starts in the payload of an error reply (the bytes after the i32 status) for an op: the op's own
 * empty reply shape comes first (see tsdb_protocol.h, version 13). -1 for an op whose error reply is not
 * known to carry one. */
static inline int tsdb_wire_error_shape(unsigned op)
{
    switch (op) {
    case TSDB_OP_QUERY_RAW: return 5;
    case TSDB_OP_QUERY_VALUES:
    case TSDB_OP_QUERY_LTTB: return 8;
    case TSDB_OP_QUERY_MULTI: return 8; /* u32 index, u32 answered of the END frame */
    case TSDB_OP_QUERY_LATEST: return TSDB_LATEST_PREFIX; /* u32 first, u32 m, u8 last, 3 zero bytes */
    default: return 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif
