/* SPDX-License-Identifier: AGPL-3.0-only */
/* The replication driver's payloads on the data links: see replwire.h. */
#include <string.h>

#include "plcheck.h" /* xxHash, inline and static */
#include "replwire.h"

#define POS_LIMIT ((uint64_t)1 << 62) /* FNO_LIMIT: no position at or above it */

int rw_mutant;
__thread int rw_nocmp_local;

/* XXH3-64 rather than CRC32C: vendored, portable to every level `make dist` builds and to arm64, and within a few ns of
 * CRC32C over these sizes (design-rdma.md 6). The seed tells the kinds apart, so the bytes of one never pass as
 * another's. */
static const uint64_t rw_seed[3] = {0x3152574443484b4dull /* MSG */, 0x3252574443484b46ull /* FETCH */,
                                    0x3352574443484b42ull /* META */};

uint64_t rw_check(int kind, const void *p, size_t n)
{
    return XXH3_64bits_withSeed(p, n, rw_seed[kind < 0 || kind > 2 ? 0 : kind]);
}

/* what the check of a message covers: everything before it, or only RW_HEAD under the planted bug */
static size_t covered(size_t at)
{
    return rw_mutant == RW_MUT_SHORT && at > RW_HEAD ? RW_HEAD : at;
}

static uint64_t le64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

size_t rw_msg_check_at(const uint8_t *p, size_t n)
{
    if (n < RW_HEAD + RW_CHECK)
        return 0;
    uint32_t nrec;
    memcpy(&nrec, p + RW_HEAD - 4, 4);
    if (!nrec)
        return RW_HEAD;
    if (p[1] != REPL_M_APPEND || (n - RW_HEAD - RW_CHECK) / RW_DESC < nrec ||
        n - RW_HEAD - RW_CHECK < RW_APPEND_HEAD + (size_t)nrec * RW_DESC)
        return 0;
    return RW_HEAD + RW_APPEND_HEAD + (size_t)nrec * RW_DESC;
}

/* the check at p + at over p[0, at): 0, RW_E_CHECK */
static int check_ok(int kind, const uint8_t *p, size_t at)
{
    if (rw_mutant == RW_MUT_NOCMP || rw_nocmp_local)
        return 0;
    return rw_check(kind, p, kind == RW_K_MSG ? covered(at) : at) == le64(p + at) ? 0 : RW_E_CHECK;
}

struct wr {
    uint8_t *p;
    size_t n, cap;
};
static void w8(struct wr *w, uint8_t v)
{
    if (w->n + 1 <= w->cap)
        w->p[w->n] = v;
    w->n += 1;
}
static void w16(struct wr *w, uint16_t v)
{
    if (w->n + 2 <= w->cap)
        memcpy(w->p + w->n, &v, 2);
    w->n += 2;
}
static void w32(struct wr *w, uint32_t v)
{
    if (w->n + 4 <= w->cap)
        memcpy(w->p + w->n, &v, 4);
    w->n += 4;
}
static void w64(struct wr *w, uint64_t v)
{
    if (w->n + 8 <= w->cap)
        memcpy(w->p + w->n, &v, 8);
    w->n += 8;
}
static void wbytes(struct wr *w, const void *b, size_t len)
{
    if (len && w->n + len <= w->cap)
        memcpy(w->p + w->n, b, len);
    w->n += len;
}

struct rd {
    const uint8_t *p;
    size_t n, at;
    int bad;
};
static const uint8_t *take(struct rd *r, size_t len)
{
    if (r->bad || r->n - r->at < len) {
        r->bad = 1;
        return NULL;
    }
    const uint8_t *q = r->p + r->at;
    r->at += len;
    return q;
}
static uint8_t r8(struct rd *r)
{
    const uint8_t *q = take(r, 1);
    return q ? q[0] : 0;
}
static uint16_t r16(struct rd *r)
{
    uint16_t v = 0;
    const uint8_t *q = take(r, 2);
    if (q)
        memcpy(&v, q, 2);
    return v;
}
static uint32_t r32(struct rd *r)
{
    uint32_t v = 0;
    const uint8_t *q = take(r, 4);
    if (q)
        memcpy(&v, q, 4);
    return v;
}
static uint64_t r64(struct rd *r)
{
    uint64_t v = 0;
    const uint8_t *q = take(r, 8);
    if (q)
        memcpy(&v, q, 8);
    return v;
}

static int node_ok(int x) { return x >= -1 && x < REPL_MAX_NODES; }

/* the rules both sides check: see the header */
static int recs_ok(const struct repl_msg *m, const uint32_t *lens, size_t nbytes)
{
    if (m->type != REPL_M_APPEND)
        return m->nrec == 0 && nbytes == 0;
    uint64_t next = m->prev_pos + 1, sum = 0;
    if (m->prev_pos >= POS_LIMIT)
        return 0;
    for (uint32_t i = 0; i < m->nrec; i++) {
        const struct repl_rec *r = &m->rec[i];
        if (r->pos != next || r->n == 0 || lens[i] == 0 || r->pos >= POS_LIMIT || POS_LIMIT - r->pos < r->n)
            return 0;
        next = r->pos + r->n;
        sum += lens[i];
    }
    return sum == nbytes;
}

size_t rw_encode_msg(uint8_t *out, size_t cap, uint32_t volume, const struct repl_msg *m, const uint32_t *lens,
                     uint64_t seq, uint64_t off, const void *bytes, size_t nbytes)
{
    if (m->type < REPL_M_APPEND || m->type > REPL_M_SNAPSHOT || !node_ok(m->from) || !node_ok(m->to) ||
        !node_ok(m->leader) || !node_ok(m->meta.leader) || (m->nrec && !m->rec) || !recs_ok(m, lens, nbytes))
        return 0;
    struct wr w = {.p = out, .cap = cap};
    w8(&w, RW_VERSION);
    w8(&w, (uint8_t)m->type);
    w16(&w, m->resync ? 1 : 0);
    w32(&w, volume);
    w32(&w, (uint32_t)m->from);
    w32(&w, (uint32_t)m->to);
    w32(&w, (uint32_t)m->leader);
    const uint64_t f[12] = {m->epoch, m->prev_pos, m->prev_epoch, m->commit, m->durable,     m->written,
                            m->rid,   m->last_epoch, m->end,   m->start,  m->set_version, m->replay};
    for (int i = 0; i < 12; i++)
        w64(&w, f[i]);
    w64(&w, m->meta.epoch);
    w64(&w, m->meta.isr_version);
    w32(&w, (uint32_t)m->meta.leader);
    w32(&w, m->meta.isr);
    w32(&w, m->meta.unanswered);
    for (int i = 0; i < REPL_MAX_NODES; i++)
        w64(&w, m->meta.rid[i]);
    w32(&w, m->nrec);
    if (m->type == REPL_M_APPEND && m->nrec) {
        w64(&w, seq);
        w64(&w, off);
        for (uint32_t i = 0; i < m->nrec; i++) {
            w64(&w, m->rec[i].pos);
            w64(&w, m->rec[i].epoch);
            w32(&w, m->rec[i].n);
            w32(&w, lens[i]);
        }
    }
    if (w.n + RW_CHECK > cap)
        return 0;
    w64(&w, rw_check(RW_K_MSG, out, covered(w.n)));
    if (m->type == REPL_M_APPEND && m->nrec)
        wbytes(&w, bytes, nbytes);
    return w.n <= cap ? w.n : 0;
}

int rw_decode_msg(const uint8_t *p, size_t n, struct rw_msg *out, struct repl_rec *recs, uint32_t *lens, uint32_t cap)
{
    struct rd r = {.p = p, .n = n};
    memset(out, 0, sizeof *out);
    struct repl_msg *m = &out->m;
    if (n < 1 || p[0] != RW_VERSION)
        return -1;
    /* the check first: nothing of the message is read into *out before it holds (type and nrec only find it) */
    size_t at = rw_msg_check_at(p, n);
    if (!at)
        return -1;
    int ck = check_ok(RW_K_MSG, p, at);
    if (ck)
        return ck;
    r8(&r);
    m->type = r8(&r);
    uint16_t flags = r16(&r);
    out->volume = r32(&r);
    m->from = (int32_t)r32(&r);
    m->to = (int32_t)r32(&r);
    m->leader = (int32_t)r32(&r);
    uint64_t *f[12] = {&m->epoch, &m->prev_pos, &m->prev_epoch, &m->commit, &m->durable,     &m->written,
                       &m->rid,   &m->last_epoch, &m->end,   &m->start,  &m->set_version, &m->replay};
    for (int i = 0; i < 12; i++)
        *f[i] = r64(&r);
    m->meta.epoch = r64(&r);
    m->meta.isr_version = r64(&r);
    m->meta.leader = (int32_t)r32(&r);
    m->meta.isr = r32(&r);
    m->meta.unanswered = r32(&r);
    for (int i = 0; i < REPL_MAX_NODES; i++)
        m->meta.rid[i] = r64(&r);
    m->nrec = r32(&r);
    if (r.bad || (flags & ~1u) || m->type < REPL_M_APPEND || m->type > REPL_M_SNAPSHOT || !node_ok(m->from) ||
        !node_ok(m->to) || !node_ok(m->leader) || !node_ok(m->meta.leader))
        return -1;
    m->resync = flags & 1;
    if (m->nrec) {
        if (m->type != REPL_M_APPEND || m->nrec > cap || !recs || !lens)
            return -1;
        out->seq = r64(&r);
        out->off = r64(&r);
        for (uint32_t i = 0; i < m->nrec && !r.bad; i++) {
            recs[i].pos = r64(&r);
            recs[i].epoch = r64(&r);
            recs[i].n = r32(&r);
            lens[i] = r32(&r);
        }
        take(&r, RW_CHECK);
        if (r.bad)
            return -1;
        m->rec = recs;
        out->recs = recs;
        out->lens = lens;
        out->bytes = p + r.at;
        out->nbytes = n - r.at;
    } else if (take(&r, RW_CHECK), r.bad || r.at != n) {
        return -1;
    }
    return recs_ok(m, lens, out->nbytes) ? 0 : -1;
}

static int name_ok(const char *s, size_t len)
{
    if (len == 0 || len > RW_NAME_MAX)
        return 0;
    for (size_t i = 0; i < len; i++)
        if (s[i] == '/' || s[i] == 0 || (unsigned char)s[i] < 0x20)
            return 0;
    return !(len <= 2 && s[0] == '.' && (len == 1 || s[1] == '.'));
}

size_t rw_encode_fetch(uint8_t *out, size_t cap, uint32_t volume, uint64_t pos, const char *name)
{
    size_t len = strlen(name);
    if (!name_ok(name, len))
        return 0;
    struct wr w = {.p = out, .cap = cap};
    w8(&w, RW_VERSION);
    w32(&w, volume);
    w64(&w, pos);
    w16(&w, (uint16_t)len);
    wbytes(&w, name, len);
    if (w.n + RW_CHECK > cap)
        return 0;
    w64(&w, rw_check(RW_K_FETCH, out, w.n));
    return w.n <= cap ? w.n : 0;
}

static int rname(struct rd *r, char *name, size_t cap)
{
    uint16_t len = r16(r);
    const uint8_t *q = take(r, len);
    if (!q || len + 1u > cap || !name_ok((const char *)q, len))
        return -1;
    memcpy(name, q, len);
    name[len] = 0;
    return 0;
}

int rw_decode_fetch(const uint8_t *p, size_t n, uint32_t *volume, uint64_t *pos, char *name, size_t cap)
{
    if (n < 1 + RW_CHECK || p[0] != RW_VERSION)
        return -1;
    int ck = check_ok(RW_K_FETCH, p, n - RW_CHECK); /* before any field is read */
    if (ck)
        return ck;
    struct rd r = {.p = p, .n = n - RW_CHECK};
    r8(&r);
    *volume = r32(&r);
    *pos = r64(&r);
    if (rname(&r, name, cap) != 0 || r.bad || r.at != r.n)
        return -1;
    return 0;
}

size_t rw_encode_bulk_meta(uint8_t *out, size_t cap, uint32_t volume, uint64_t size, const char *name)
{
    size_t len = strlen(name);
    if (!name_ok(name, len))
        return 0;
    struct wr w = {.p = out, .cap = cap};
    w8(&w, RW_VERSION);
    w32(&w, volume);
    w64(&w, size);
    w16(&w, (uint16_t)len);
    wbytes(&w, name, len);
    if (w.n + RW_CHECK > cap)
        return 0;
    w64(&w, rw_check(RW_K_META, out, w.n));
    return w.n <= cap ? w.n : 0;
}

int rw_decode_bulk_meta(const uint8_t *p, size_t n, uint32_t *volume, uint64_t *size, char *name, size_t cap)
{
    if (n < 1 + RW_CHECK || p[0] != RW_VERSION)
        return -1;
    int ck = check_ok(RW_K_META, p, n - RW_CHECK); /* before any field is read */
    if (ck)
        return ck;
    struct rd r = {.p = p, .n = n - RW_CHECK};
    r8(&r);
    *volume = r32(&r);
    *size = r64(&r);
    if (rname(&r, name, cap) != 0 || r.bad || r.at != r.n)
        return -1;
    return 0;
}
