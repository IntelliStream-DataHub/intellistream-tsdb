/*
 * WAL marker payloads: one encoder and one decoder (marker.h has the layouts' contract). Layout of a payload,
 * little endian, the body a multiple of 4 bytes:
 *
 *   all     u16 type, u16 flags, u32 span, body
 *   COMMIT  u8 op, u8 keys_form, u16 nfiles, u32 0, u64 flush_through, u64 rows, u64 events, u64 reserve,
 *           keys (none: nothing; range: u32 series, u32 0, i64 lo, i64 hi; table: 4096 x (i64 lo, i64 hi)),
 *           files: nfiles x (u16 name_len, u16 0, u64 size, u64 xxh64, u64 header_check, name, zeros to 4 bytes)
 *   DROP    u64 wm_gen, i64 dropped_before, u64 block, i64 partition (INT64_MIN: none)
 *   RETENTION u32 series, u32 0, i64 limit_ms, u64 record_off
 *   EPOCH   u64 epoch, u32 owner_len, owner, zeros to 4 bytes
 *
 * The decoder refuses what the encoder never writes (a non-zero reserved field or padding, a span other than the
 * type's, trailing bytes), so a payload has exactly one encoding and the fuzz harness compares both directions.
 */
#include <stdlib.h>
#include <string.h>

#include "internal.h"

_Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "marker payloads are little endian and written by memcpy");

static const char *const type_names[] = {NULL,    "COMMIT", "DROP", "RETENTION", "EPOCH", "FEED_SLOT",
                                         "CLASS", "SERIES", "STATE", "SLOT",    "NOW",   "COLD"};

const char *mk_type_name(uint16_t type)
{
    return type >= 1 && type <= MK_TYPE_LAST ? type_names[type] : NULL;
}

bool mk_type_known(uint16_t type)
{
    return type >= 1 && type <= MK_TYPE_LAST;
}

uint32_t mk_span(const mk *m)
{
    if (m->type == MK_COMMIT && m->u.commit.events > 0)
        return m->u.commit.events > UINT32_MAX ? 0 : (uint32_t)m->u.commit.events;
    return 1;
}

static size_t pad4(size_t n)
{
    return (n + 3) & ~(size_t)3;
}

static uint64_t get64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

static void put64(uint8_t *p, uint64_t v)
{
    memcpy(p, &v, 8);
}

static uint16_t get16(const uint8_t *p)
{
    uint16_t v;
    memcpy(&v, p, 2);
    return v;
}

static void put16(uint8_t *p, uint16_t v)
{
    memcpy(p, &v, 2);
}

/* the bytes from p[from] to p[to] are all zero */
static bool zeros(const uint8_t *p, size_t from, size_t to)
{
    for (size_t i = from; i < to; i++)
        if (p[i])
            return false;
    return true;
}

static bool name_ok(const char *s, size_t n)
{
    if (n == 0 || n > MK_NAME_MAX)
        return false;
    for (size_t i = 0; i < n; i++)
        if (s[i] == '/' || s[i] == 0)
            return false;
    return true;
}

#define FILE_FIXED 28 /* u16 len, u16 0, three u64 */
#define COMMIT_FIXED 40

size_t mk_file_size(const mk_file *f)
{
    return FILE_FIXED + pad4(f->name_len);
}

size_t mk_file_put(uint8_t *buf, const mk_file *f)
{
    size_t n = mk_file_size(f);
    memset(buf, 0, n);
    put16(buf, (uint16_t)f->name_len);
    put64(buf + 4, f->size);
    put64(buf + 12, f->xxh64);
    put64(buf + 20, f->header_check);
    memcpy(buf + FILE_FIXED, f->name, f->name_len);
    return n;
}

/* Reads and checks the entry at p[0..avail); gives its size, 0 when it is not one. */
static size_t file_parse(const uint8_t *p, size_t avail, mk_file *f)
{
    if (avail < FILE_FIXED)
        return 0;
    uint32_t nl = get16(p);
    if (nl == 0 || nl > MK_NAME_MAX || get16(p + 2) != 0)
        return 0;
    size_t sz = FILE_FIXED + pad4(nl);
    if (sz > avail || !zeros(p, FILE_FIXED + nl, sz) || !name_ok((const char *)p + FILE_FIXED, nl))
        return 0;
    if (f) {
        f->name = (const char *)p + FILE_FIXED;
        f->name_len = nl;
        f->size = get64(p + 4);
        f->xxh64 = get64(p + 12);
        f->header_check = get64(p + 20);
    }
    return sz;
}

bool mk_file_next(const mk_commit *c, size_t *pos, mk_file *f)
{
    if (*pos >= c->files_len)
        return false;
    size_t sz = file_parse(c->files + *pos, c->files_len - *pos, f);
    if (sz == 0)
        return false;
    *pos += sz;
    return true;
}

void mk_table_get(const uint8_t *table, uint32_t bucket, int64_t *lo, int64_t *hi)
{
    memcpy(lo, table + (size_t)bucket * 16, 8);
    memcpy(hi, table + (size_t)bucket * 16 + 8, 8);
}

void mk_table_put(uint8_t *table, uint32_t bucket, int64_t lo, int64_t hi)
{
    memcpy(table + (size_t)bucket * 16, &lo, 8);
    memcpy(table + (size_t)bucket * 16 + 8, &hi, 8);
}

static int decode_commit(const uint8_t *b, size_t n, mk_commit *c)
{
    if (n < COMMIT_FIXED)
        return TSDB_ERR_CORRUPT;
    c->op = b[0];
    c->keys_form = b[1];
    c->nfiles = get16(b + 2);
    if (c->op < MK_OP_INSERT || c->op > MK_OP_DELETE || c->keys_form > MK_KEYS_TABLE || c->nfiles > MK_FILES_MAX ||
        !zeros(b, 4, 8))
        return TSDB_ERR_CORRUPT;
    c->flush_through = get64(b + 8);
    c->rows = get64(b + 16);
    c->events = get64(b + 24);
    c->reserve = get64(b + 32);
    size_t pos = COMMIT_FIXED;
    c->series = 0, c->lo = c->hi = 0, c->table = NULL;
    if (c->keys_form == MK_KEYS_RANGE) {
        if (n - pos < 24 || !zeros(b, pos + 4, pos + 8))
            return TSDB_ERR_CORRUPT;
        memcpy(&c->series, b + pos, 4);
        c->lo = (int64_t)get64(b + pos + 8);
        c->hi = (int64_t)get64(b + pos + 16);
        pos += 24;
    } else if (c->keys_form == MK_KEYS_TABLE) {
        if (n - pos < (size_t)MK_BUCKETS * 16)
            return TSDB_ERR_CORRUPT;
        c->table = b + pos;
        pos += (size_t)MK_BUCKETS * 16;
    }
    c->files = b + pos;
    c->files_len = n - pos;
    size_t at = 0;
    for (uint32_t i = 0; i < c->nfiles; i++) {
        size_t sz = file_parse(c->files + at, c->files_len - at, NULL);
        if (sz == 0)
            return TSDB_ERR_CORRUPT;
        at += sz;
    }
    return at == c->files_len ? 0 : TSDB_ERR_CORRUPT; /* no bytes after the last file */
}

int mk_decode(const uint8_t *p, size_t n, mk *m)
{
    memset(m, 0, sizeof *m);
    if (n < MK_HDR || n % 4 != 0)
        return TSDB_ERR_CORRUPT;
    m->type = get16(p);
    m->flags = get16(p + 2);
    memcpy(&m->span, p + 4, 4);
    m->body = p + MK_HDR;
    m->body_len = n - MK_HDR;
    const uint8_t *b = m->body;
    size_t bn = m->body_len;
    int rc = 0;
    switch (m->type) {
    case MK_COMMIT:
        rc = decode_commit(b, bn, &m->u.commit);
        break;
    case MK_DROP:
        if (bn != 32)
            return TSDB_ERR_CORRUPT;
        m->u.drop.wm_gen = get64(b);
        m->u.drop.dropped_before = (int64_t)get64(b + 8);
        m->u.drop.block = get64(b + 16);
        m->u.drop.partition = (int64_t)get64(b + 24);
        break;
    case MK_RETENTION:
        if (bn != 24 || !zeros(b, 4, 8))
            return TSDB_ERR_CORRUPT;
        memcpy(&m->u.retention.series, b, 4);
        m->u.retention.limit_ms = (int64_t)get64(b + 8);
        m->u.retention.record_off = get64(b + 16);
        break;
    case MK_EPOCH: {
        if (bn < 12)
            return TSDB_ERR_CORRUPT;
        uint32_t ol;
        memcpy(&ol, b + 8, 4);
        if (ol > MK_NAME_MAX || bn != 12 + pad4(ol) || !zeros(b, 12 + ol, bn))
            return TSDB_ERR_CORRUPT;
        m->u.epoch.epoch = get64(b);
        m->u.epoch.owner = (const char *)b + 12;
        m->u.epoch.owner_len = ol;
        break;
    }
    default:
        break; /* reserved and unknown types: the body is theirs */
    }
    if (rc != 0)
        return rc;
    uint32_t want = mk_span(m);
    if (m->span == 0 || (mk_type_known(m->type) && m->type <= MK_EPOCH && m->span != want))
        return TSDB_ERR_CORRUPT;
    return 0;
}

int mk_encode(const mk *m, uint8_t **out, size_t *len)
{
    *out = NULL;
    size_t bn;
    switch (m->type) {
    case MK_COMMIT: {
        const mk_commit *c = &m->u.commit;
        if (c->keys_form > MK_KEYS_TABLE || (c->keys_form == MK_KEYS_TABLE && !c->table) ||
            (c->files_len && !c->files))
            return TSDB_ERR_INVALID;
        bn = COMMIT_FIXED + (c->keys_form == MK_KEYS_RANGE ? 24 : c->keys_form == MK_KEYS_TABLE ? (size_t)MK_BUCKETS * 16 : 0) +
             c->files_len;
        break;
    }
    case MK_DROP:
        bn = 32;
        break;
    case MK_RETENTION:
        bn = 24;
        break;
    case MK_EPOCH:
        if (m->u.epoch.owner_len > MK_NAME_MAX || (m->u.epoch.owner_len && !m->u.epoch.owner))
            return TSDB_ERR_INVALID;
        bn = 12 + pad4(m->u.epoch.owner_len);
        break;
    default:
        bn = m->body_len;
        if (bn % 4 != 0 || (bn && !m->body))
            return TSDB_ERR_INVALID;
        break;
    }
    if (bn > ((size_t)1 << 27) - MK_HDR)
        return TSDB_ERR_INVALID;
    uint8_t *p = calloc(1, MK_HDR + bn);
    if (!p)
        return TSDB_ERR_NOMEM;
    uint32_t span = mk_type_known(m->type) && m->type <= MK_EPOCH ? mk_span(m) : m->span; /* the defined types' span is a function of them */
    put16(p, m->type);
    put16(p + 2, m->flags);
    memcpy(p + 4, &span, 4);
    uint8_t *b = p + MK_HDR;
    switch (m->type) {
    case MK_COMMIT: {
        const mk_commit *c = &m->u.commit;
        b[0] = c->op;
        b[1] = c->keys_form;
        put16(b + 2, (uint16_t)c->nfiles);
        put64(b + 8, c->flush_through);
        put64(b + 16, c->rows);
        put64(b + 24, c->events);
        put64(b + 32, c->reserve);
        size_t pos = COMMIT_FIXED;
        if (c->keys_form == MK_KEYS_RANGE) {
            memcpy(b + pos, &c->series, 4);
            put64(b + pos + 8, (uint64_t)c->lo);
            put64(b + pos + 16, (uint64_t)c->hi);
            pos += 24;
        } else if (c->keys_form == MK_KEYS_TABLE) {
            memcpy(b + pos, c->table, (size_t)MK_BUCKETS * 16);
            pos += (size_t)MK_BUCKETS * 16;
        }
        if (c->files_len)
            memcpy(b + pos, c->files, c->files_len);
        break;
    }
    case MK_DROP:
        put64(b, m->u.drop.wm_gen);
        put64(b + 8, (uint64_t)m->u.drop.dropped_before);
        put64(b + 16, m->u.drop.block);
        put64(b + 24, (uint64_t)m->u.drop.partition);
        break;
    case MK_RETENTION:
        memcpy(b, &m->u.retention.series, 4);
        put64(b + 8, (uint64_t)m->u.retention.limit_ms);
        put64(b + 16, m->u.retention.record_off);
        break;
    case MK_EPOCH:
        put64(b, m->u.epoch.epoch);
        memcpy(b + 8, &m->u.epoch.owner_len, 4);
        if (m->u.epoch.owner_len)
            memcpy(b + 12, m->u.epoch.owner, m->u.epoch.owner_len);
        break;
    default:
        if (bn)
            memcpy(b, m->body, bn);
        break;
    }
    mk back; /* what the decoder refuses, the encoder does not write */
    if (mk_decode(p, MK_HDR + bn, &back) != 0) {
        free(p);
        return TSDB_ERR_INVALID;
    }
    *out = p;
    *len = MK_HDR + bn;
    return 0;
}
