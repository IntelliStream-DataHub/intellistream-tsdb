/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled (.ec) parts: the record codec, the writer and the open (src/ecpart.h has the layout).
 *
 * The open trusts nothing it has not checked: the trailer hashes itself and the record, the source's footer hash covers
 * its header, index, tombstones and footer (as part_open_as checks a .part), and the record must agree with the name
 * (key, block range, level, source name) and with the footer (the object's length places the index exactly where the
 * footer says), and the footer with the index (its rows and time range). An .ec part's granules are always checked
 * when read (E11: verify), and its index is judged at open without decoding a granule (the class check of the settle,
 * recorded in the record, stands: design-cluster-data.md §2.4). Damage is never silent: every failed check is
 * TSDB_ERR_CORRUPT, and load_parts fails the open on it.
 *
 * The hash and the tombstone checks repeat part.c's (part_hash, trailer_parse, static there, in a file another track
 * owns); the round trip in tests/test_ecpart.c (every source opened both ways, every gx_* field compared) keeps the two
 * copies equal.
 *
 * eng_test_mark_ec is r7-iface's test hook and stays.
 */
#include "ecpart.h"
#include "chunk.h"
#include "eccold.h"
#include "ecgc.h"
#include "tier.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

atomic_uint_fast64_t ec_calls[EC_CALL_N];
int ecpart_mutant;
atomic_bool ec_test_assume_tier;

/* part.c's (static there): the magics of a part's header and footer and of its tombstone trailer */
#define EC_PART_MAGIC "TSDBPART"
#define EC_PART_END_MAGIC "TSDBEND1"
#define EC_TOMB_MAGIC 0x424d4f54u

/* ---------------------------------------------------------------- little-endian fields */
static void put16(uint8_t *b, uint16_t v) { b[0] = (uint8_t)v, b[1] = (uint8_t)(v >> 8); }
static void put32(uint8_t *b, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}
static void put64(uint8_t *b, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}
static uint16_t get16(const uint8_t *b) { return (uint16_t)(b[0] | b[1] << 8); }
static uint32_t get32(const uint8_t *b)
{
    uint32_t v = 0;
    for (int i = 3; i >= 0; i--)
        v = v << 8 | b[i];
    return v;
}
static uint64_t get64(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | b[i];
    return v;
}

/* ---------------------------------------------------------------- the record */
size_t ec_record_size(const ec_record *r) { return EC_REC_FIXED + 8 * (size_t)r->nsums; }

/* The source name: NUL-terminated inside its 96 bytes, not empty, and zero after the NUL (one encoding per record). */
static bool source_ok(const char *s)
{
    const char *z = memchr(s, '\0', 96);
    if (!z || z == s)
        return false;
    for (const char *c = z; c < s + 96; c++)
        if (*c)
            return false;
    return true;
}

/* What a record must hold whichever way it came: its version and class result, its geometry (E2: recorded, never
 * derived from settings), the sums of exactly that geometry, a block range in order, a source name. The marker's log
 * position (`ppos`) and the stripe list's version are R8's and set only in an erasure-coded cluster volume
 * (`cluster_ec`: META says requires=cluster and ec); a single server's tier keeps them 0, so a record that sets one
 * there is not this volume's. A degraded settle's mask of written positions (E10, R9) is admitted on cluster EC
 * volumes only: 0 (all k'+m), all k'+m bits set, or (when m >= 2 so margin m - 1 >= 1) k'+m-1 bits set; single-server
 * tiers keep refusing non-zero positions. */
static bool positions_valid(const ec_record *r, bool cluster_ec)
{
    if (r->positions == 0)
        return true;
    if (!cluster_ec)
        return false;
    unsigned npos = (unsigned)r->k + (unsigned)r->m;
    if (npos == 0 || npos > CHUNK_POS_MAX)
        return false;
    uint32_t full = (1u << npos) - 1u;
    if ((r->positions & ~full) != 0)
        return false;
    unsigned bits = (unsigned)__builtin_popcount(r->positions);
    if (bits == npos)
        return true;
    if (r->m >= 2 && bits == npos - 1)
        return true;
    if (eccold_mutant == ECCOLD_MUT_DEGTWO && r->m >= 2 && bits == npos - 2)
        return true; /* planted degtwo: allows settling with 2 positions down */
    return false;
}

static bool record_valid(const ec_record *r, bool cluster_ec)
{
    chunk_geom g;
    bool r8 = cluster_ec || mk_test_mutant == MK_MUT_PPOS_LOOSE;
    return r->version == EC_RECORD_VERSION && r->class_check <= EC_CLS_FAILED && r->min_block <= r->max_block &&
           (r8 || (r->ppos == 0 && r->stripe_version == 0)) &&
           chunk_geom_make(&g, r->k, r->m, r->cell_bytes, r->object_len) == 0 && r->nsums == chunk_sums_total(&g) &&
           positions_valid(r, cluster_ec) && source_ok(r->source);
}

int ec_record_encode(const ec_record *r, uint8_t *buf, size_t cap)
{
    return ec_record_encode_as(r, buf, cap, false);
}

int ec_record_decode(const uint8_t *buf, size_t len, ec_record *r)
{
    return ec_record_decode_as(buf, len, r, false);
}

int ec_record_encode_as(const ec_record *r, uint8_t *buf, size_t cap, bool cluster_ec)
{
    if (!record_valid(r, cluster_ec) || (r->nsums && !r->sums) || r->nsums > (SIZE_MAX - EC_REC_FIXED) / 8 ||
        cap < ec_record_size(r))
        return TSDB_ERR_INVALID;
    memset(buf, 0, EC_REC_FIXED);
    put32(buf + EC_REC_VERSION, r->version);
    put32(buf + EC_REC_CLASS, r->class_check);
    memcpy(buf + EC_REC_OID, r->oid, 16);
    put64(buf + EC_REC_KEY, (uint64_t)r->key);
    put64(buf + EC_REC_MIN_BLOCK, r->min_block);
    put64(buf + EC_REC_MAX_BLOCK, r->max_block);
    put32(buf + EC_REC_LEVEL, r->level);
    put16(buf + EC_REC_K, r->k);
    put16(buf + EC_REC_M, r->m);
    put32(buf + EC_REC_CELL, r->cell_bytes);
    put32(buf + EC_REC_POSITIONS, r->positions);
    put64(buf + EC_REC_OBJ_LEN, r->object_len);
    put64(buf + EC_REC_OBJ_HASH, r->object_xxh64);
    put64(buf + EC_REC_EPOCH, r->writer_epoch);
    put64(buf + EC_REC_SETTLE_MS, (uint64_t)r->settle_ms);
    put64(buf + EC_REC_PPOS, r->ppos);
    put64(buf + EC_REC_STRIPE_VERSION, r->stripe_version);
    memcpy(buf + EC_REC_SOURCE, r->source, 96);
    put64(buf + EC_REC_NSUMS, r->nsums);
    for (uint64_t i = 0; i < r->nsums; i++)
        put64(buf + EC_REC_SUMS + 8 * i, r->sums[i]);
    return 0;
}

int ec_record_decode_as(const uint8_t *buf, size_t len, ec_record *r, bool cluster_ec)
{
    memset(r, 0, sizeof *r);
    if (len < EC_REC_FIXED)
        return TSDB_ERR_CORRUPT;
    r->version = get32(buf + EC_REC_VERSION);
    if (r->version != EC_RECORD_VERSION)
        return TSDB_ERR_CORRUPT; /* a record this build does not know: never read as one it does */
    r->class_check = get32(buf + EC_REC_CLASS);
    memcpy(r->oid, buf + EC_REC_OID, 16);
    r->key = (int64_t)get64(buf + EC_REC_KEY);
    r->min_block = get64(buf + EC_REC_MIN_BLOCK);
    r->max_block = get64(buf + EC_REC_MAX_BLOCK);
    r->level = get32(buf + EC_REC_LEVEL);
    r->k = get16(buf + EC_REC_K);
    r->m = get16(buf + EC_REC_M);
    r->cell_bytes = get32(buf + EC_REC_CELL);
    r->positions = get32(buf + EC_REC_POSITIONS);
    r->object_len = get64(buf + EC_REC_OBJ_LEN);
    r->object_xxh64 = get64(buf + EC_REC_OBJ_HASH);
    r->writer_epoch = get64(buf + EC_REC_EPOCH);
    r->settle_ms = (int64_t)get64(buf + EC_REC_SETTLE_MS);
    r->ppos = get64(buf + EC_REC_PPOS);
    r->stripe_version = get64(buf + EC_REC_STRIPE_VERSION);
    memcpy(r->source, buf + EC_REC_SOURCE, 96);
    r->nsums = get64(buf + EC_REC_NSUMS);
    /* the sums fill the rest exactly; checked before anything is allocated, so the allocation is bounded by len */
    if ((len - EC_REC_FIXED) % 8 != 0 || r->nsums != (len - EC_REC_FIXED) / 8 || !record_valid(r, cluster_ec)) {
        r->nsums = 0;
        return TSDB_ERR_CORRUPT;
    }
    if (!(r->sums = malloc((size_t)r->nsums * 8))) {
        r->nsums = 0;
        return TSDB_ERR_NOMEM;
    }
    for (uint64_t i = 0; i < r->nsums; i++)
        r->sums[i] = get64(buf + EC_REC_SUMS + 8 * i);
    return 0;
}

void ec_record_free(ec_record *r)
{
    free(r->sums);
    r->sums = NULL;
    r->nsums = 0;
}

/* ---------------------------------------------------------------- the source's bytes */
/* part.c's part_hash over the bytes as they lie: header, index, tombstones (when any), footer up to its hash. */
static uint64_t src_hash(const uint8_t *h, size_t hlen, const uint8_t *ib, size_t ilen, size_t tlen, const uint8_t *f)
{
    uint64_t x = xxh64(h, hlen, 0);
    x = xxh64(ib, ilen, x);
    if (tlen)
        x = xxh64(ib + ilen, tlen, x);
    return xxh64(f, offsetof(part_footer, index_hash), x);
}

/* part.c's trailer_parse: tombstones read in place (8-byte aligned, which the caller arranges), each a range, in order */
static bool tombs_parse(const uint8_t *buf, size_t tlen, const tomb **t, uint32_t *n)
{
    *t = NULL;
    *n = 0;
    if (tlen == 0)
        return true;
    uint32_t cnt, magic;
    if (tlen < 8 || (uintptr_t)buf % _Alignof(tomb) != 0)
        return false;
    memcpy(&cnt, buf + tlen - 8, 4);
    memcpy(&magic, buf + tlen - 4, 4);
    if (magic != EC_TOMB_MAGIC || cnt == 0 || (uint64_t)cnt * sizeof(tomb) + 8 != tlen)
        return false;
    const tomb *x = (const tomb *)buf;
    for (uint32_t i = 0; i < cnt; i++) {
        if (x[i].start >= x[i].end)
            return false;
        if (i && (x[i].series < x[i - 1].series || (x[i].series == x[i - 1].series && x[i].start < x[i - 1].end)))
            return false;
    }
    *t = x;
    *n = cnt;
    return true;
}

/* "p<K>_<A>_<B>_<L>.ec" -> "p<K>_<A>_<B>_<L>.part", zero padded (false when it does not fit 96 bytes) */
static bool source_name(const char *name, char out[96])
{
    size_t n = strlen(name);
    if (n < 3 || strcmp(name + n - 3, ".ec") != 0 || n - 3 + 6 > 96)
        return false;
    memset(out, 0, 96);
    memcpy(out, name, n - 3);
    memcpy(out + n - 3, ".part", 5);
    return true;
}

/* ---------------------------------------------------------------- the writer */
int ec_index_write(eng *db, const part *src, const ec_record *r, char tmp[96])
{
    tmp[0] = '\0';
    /* a .part of the current format (FORMAT_VERSION) with granules (v8 parts and parts without granules are never
     * settled: src/merge.c), and a record of exactly this part: a settle's mix-up must not become a file that names
     * another */
    if (atomic_load(&((part *)src)->ec) || src->hdr_len != sizeof(part_header) || src->ng == 0 || !record_valid(r, db->ec_cluster) ||
        (r->nsums && !r->sums) || r->key != src->partition || r->min_block != src->min_block ||
        r->max_block != src->max_block || r->level != src->level || r->object_len != src->map_len)
        return TSDB_ERR_INVALID;
    char want[96];
    memset(want, 0, sizeof want);
    snprintf(want, sizeof want, "%s", src->name);
    if (memcmp(want, r->source, 96) != 0)
        return TSDB_ERR_INVALID;
    size_t hl = src->hdr_len, il = (size_t)src->index_len, fl = sizeof(part_footer);
    size_t tl = src->map_len - fl - (size_t)src->index_off - il;
    size_t R = hl + il + tl + fl, n = ec_record_size(r), total = R + n + EC_TRAILER_BYTES;
    uint8_t *b = calloc(1, total);
    if (!b)
        return TSDB_ERR_NOMEM;
    memcpy(b, src->map, hl);
    memcpy(b + hl, src->icopy ? src->icopy : src->map + src->index_off, il + tl); /* the bytes the open checked */
    memcpy(b + hl + il + tl, src->map + src->map_len - fl, fl);
    int rc = TSDB_ERR_CORRUPT;
    /* damage since the source's open would be written under a fresh trailer: the source's own hash decides */
    if (src_hash(b, hl, b + hl, il, tl, b + hl + il + tl) != src->index_hash) {
        fprintf(stderr, "tsdb: %s/%s: its index no longer matches the checksum its open checked: not settled\n",
                db->dir, src->name);
        goto out;
    }
    if ((rc = ec_record_encode_as(r, b + R, n, db->ec_cluster)) != 0)
        goto out;
    uint8_t *t = b + R + n;
    memcpy(t + EC_TR_MAGIC, EC_TRAILER_MAGIC, 8);
    put32(t + EC_TR_VERSION, EC_TRAILER_VERSION);
    put32(t + EC_TR_REC_VERSION, r->version);
    put64(t + EC_TR_REC_OFF, R);
    put64(t + EC_TR_REC_LEN, n);
    put64(t + EC_TR_REC_HASH, xxh64(b + R, n, 0));
    put64(t + EC_TR_HASH, xxh64(t, EC_TR_HASH, 0));

    char name[96];
    snprintf(name, sizeof name, "tmp_%d_%u.ec", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));
    int fd = openat(db->dirfd, name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) {
        rc = TSDB_ERR_IO;
        goto out;
    }
    rc = write_full(fd, b, total);
    if (rc == 0 && fsync(fd) != 0) /* nothing relies on it yet: a failure fails this call only (the swap renames it) */
        rc = TSDB_ERR_IO;
    if (close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    if (rc)
        unlinkat(db->dirfd, name, 0);
    else
        memcpy(tmp, name, sizeof name);
out:
    free(b);
    return rc;
}

/* ---------------------------------------------------------------- the open */
int part_open_ec(eng *db, const char *file, const char *name, part **out)
{
    int rc = part_open_ec_at(db->dirfd, file, name, db->ec_cluster, db->opt.pin_index, out);
    if (rc == 0) {
        struct ecpart *e = atomic_load(&(*out)->ec);
        e->tier = db->tier;
        e->volume = db->tier_vol;
        /* r7-gc F5 / r8k-gc: hold the object from open (not only from retire), so a reopen's old engine still reading
         * a part the new engine retires keeps its object alive. Planted bug holdatretire keeps the old behaviour. */
        if (db->tier && !e->test_mark && ecgc_mutant != GC_MUT_HOLDATRET) {
            rc = ecgc_hold(db->tier, *out, db->tier_vol, e->rec.oid);
            if (rc != 0) {
                part_unref(*out);
                *out = NULL;
            }
        }
    }
    return rc;
}

int part_open_ec_at(int dirfd, const char *file, const char *name, bool cluster_ec, bool pin, part **out)
{
    *out = NULL;
    part *p = calloc(1, sizeof *p);
    struct ecpart *e = calloc(1, sizeof *e);
    if (!p || !e) {
        free(p);
        free(e);
        return TSDB_ERR_NOMEM;
    }
    int consumed = 0;
    char srcname[96];
    if (strlen(name) >= sizeof p->name || !ec_file_name(name) || !source_name(name, srcname) ||
        sscanf(name, "p%" SCNd64 "_%" SCNu64 "_%" SCNu64 "_%u.ec%n", &p->partition, &p->min_block, &p->max_block,
               &p->level, &consumed) != 4 ||
        name[consumed] != '\0' || p->min_block > p->max_block) {
        free(p);
        free(e);
        return TSDB_ERR_INVALID;
    }
    snprintf(p->name, sizeof p->name, "%s", name);
    p->dirfd = dirfd;

    int fd = openat(dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        int err = errno;
        free(p);
        free(e);
        return err == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        free(p);
        free(e);
        return TSDB_ERR_IO;
    }
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer);
    size_t size = (size_t)st.st_size;
    if (st.st_size < 0 || size < HL + FL + EC_REC_FIXED + EC_TRAILER_BYTES) {
        close(fd);
        free(p);
        free(e);
        return TSDB_ERR_CORRUPT;
    }
    void *map = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        free(p);
        free(e);
        return TSDB_ERR_IO;
    }
    p->map = map;
    p->map_len = size;
    p->created = st.st_mtime;
    p->written = st.st_mtime;
    bool hashes = ecpart_mutant != EC_MUT_NOHASH;
    int rc = TSDB_ERR_CORRUPT;

    /* the trailer: its own hash, then where the record is and the record's hash */
    uint8_t tr[EC_TRAILER_BYTES];
    memcpy(tr, p->map + size - EC_TRAILER_BYTES, sizeof tr);
    static const uint8_t zero16[16];
    if (memcmp(tr + EC_TR_MAGIC, EC_TRAILER_MAGIC, 8) != 0 || get32(tr + EC_TR_VERSION) != EC_TRAILER_VERSION ||
        get32(tr + EC_TR_REC_VERSION) != EC_RECORD_VERSION || memcmp(tr + EC_TR_RESERVED, zero16, 16) != 0 ||
        (hashes && xxh64(tr, EC_TR_HASH, 0) != get64(tr + EC_TR_HASH)))
        goto fail;
    uint64_t roff = get64(tr + EC_TR_REC_OFF), rlen = get64(tr + EC_TR_REC_LEN);
    /* checked without wrapping: the record ends where the trailer begins, and the source's bytes come before it */
    if (rlen > size - EC_TRAILER_BYTES || roff != size - EC_TRAILER_BYTES - rlen || roff < HL + FL || roff % 8 != 0)
        goto fail;
    e->rec_off = roff;
    e->rec_len = rlen;
    e->rec_hash = xxh64(p->map + roff, (size_t)rlen, 0);
    if (hashes && e->rec_hash != get64(tr + EC_TR_REC_HASH))
        goto fail;
    if ((rc = ec_record_decode_as(p->map + roff, (size_t)rlen, &e->rec, cluster_ec)) != 0)
        goto fail;
    rc = TSDB_ERR_CORRUPT;
    const ec_record *r = &e->rec;
    /* the record is of this name: key, block range, level, source */
    if (r->key != p->partition || r->min_block != p->min_block || r->max_block != p->max_block ||
        r->level != p->level || memcmp(r->source, srcname, 96) != 0)
        goto fail;

    /* the source's header and footer, as part_open_as reads a format-9 part (an .ec part is never a v8 one) */
    part_header h;
    part_footer f;
    memcpy(&h, p->map, HL);
    memcpy(&f, p->map + roff - FL, FL);
    if (memcmp(h.magic, EC_PART_MAGIC, 8) != 0 || memcmp(f.magic, EC_PART_END_MAGIC, 8) != 0 ||
        h.version != FORMAT_VERSION || f.version != h.version || h.granule_rows == 0 ||
        h.granule_rows > MAX_GRANULE_ROWS)
        goto fail;
    if (h.fno_lo > h.fno_hi || (h.fno_lo == 0) != (h.fno_hi == 0) || h.fno_hi >= FNO_BOUND_MAX)
        goto fail;
    p->hdr_len = (uint32_t)HL;
    p->grows = h.granule_rows;
    p->wal_seq = ecpart_mutant == EC_MUT_NOWALSEQ ? 0 : h.wal_seq; /* a WAL replay skips the partition (AGENTS.md) */
    if (ecpart_mutant != EC_MUT_NOFNO) { /* fno_next counts 1 + the largest fno_hi of every part */
        p->fno_lo = h.fno_lo;
        p->fno_hi = h.fno_hi;
    }
    p->written_ms = h.written_ms;
    /* here the index lies at HL and the tombstones after it; the footer places them in the object, whose length the
     * record names: both must say the same */
    if (f.index_len % 8 != 0 || f.index_off % 8 != 0 || f.index_len > roff - FL - HL || f.index_off < HL)
        goto fail;
    size_t ilen = (size_t)f.index_len, tlen = (size_t)(roff - FL - HL - f.index_len);
    if (r->object_len < (uint64_t)ilen + tlen + FL || r->object_len - ilen - tlen - FL != f.index_off)
        goto fail;
    /* Every granule's blocks lie between the header and the index, at least a byte each, so the count is bounded by
     * the object's data bytes. That bound is the object's, not this file's: a packed index of stride 0 (every field
     * constant or linear) is a few bytes for any count, and a record's 8-byte sum may vouch for a cell of up to
     * CHUNK_CELL_LIMIT. What keeps a crafted or damaged count from holding the open (a fuzz timeout: a sealed file of
     * 536 bytes claimed 10^8 granules, then one of 656 bytes 2^30) is the walk below, which checks each granule's
     * place first and stops at the first one out of place: constant offsets fail at the second. A count the walk
     * accepts describes an object of at least that many bytes, whose open costs what its granules cost. */
    if (f.ngranules > f.index_off - HL)
        goto fail;
    const uint8_t *ib = p->map + HL;
    if (pin) { /* as part_open_as: the bytes checked are the bytes used */
        rc = TSDB_ERR_NOMEM;
        if (!(p->icopy = malloc(ilen + tlen + 8)))
            goto fail;
        memcpy(p->icopy, ib, ilen + tlen);
        memset(p->icopy + ilen + tlen, 0, 8);
        ib = p->icopy;
        p->pinned = ilen + tlen + 8;
        rc = TSDB_ERR_CORRUPT;
    }
    if ((hashes && src_hash(p->map, HL, ib, ilen, tlen, p->map + roff - FL) != f.index_hash) || f.min_ts > f.max_ts ||
        !tombs_parse(ib + ilen, tlen, &p->t, &p->nt) || gix_parse(ib, ilen, f.ngranules, &p->ix) != 0)
        goto fail;
    p->ng = f.ngranules;
    p->index_off = f.index_off; /* the object's: granule offsets (GF_OFF) are offsets into it */
    p->index_len = f.index_len;
    p->index_hash = f.index_hash;
    p->rows = f.rows;
    p->min_ts = f.min_ts;
    p->max_ts = f.max_ts;
    /* the index against the object: the writer appends each granule's blocks at the end of the data in index order
     * (pw_emit, pw_copy_granule), so granule i starts at or after the end of granule i - 1, holds a byte or more and
     * ends before the index. Nothing else checks this here (the blocks are not in this file), and a reader would take
     * the offsets as they are. Then the footer against the index: its rows and its time range (the rows' and the
     * tombstones', as pw_finish_mode writes them) */
    if (p->ng || p->nt) {
        uint64_t rows = 0, end = HL;
        int64_t lo = INT64_MAX, hi = INT64_MIN;
        for (uint32_t i = 0; i < p->ng; i++) {
            if (ecpart_mutant != EC_MUT_NOLAYOUT) {
                granule g;
                gx_load(p, i, GW_LOC, &g);
                uint64_t len = (uint64_t)g.ts_len + g.val_len;
                if (g.off < end || g.off > f.index_off || len == 0 || len > f.index_off - g.off)
                    goto fail;
                end = g.off + len;
            }
            rows += gx_count(p, i);
            lo = MIN(lo, gx_ts_min(p, i));
            hi = MAX(hi, gx_ts_max(p, i));
        }
        for (uint32_t i = 0; i < p->nt; i++) {
            lo = MIN(lo, p->t[i].start);
            hi = MAX(hi, p->t[i].end - 1);
        }
        if (rows != p->rows || lo != p->min_ts || hi != p->max_ts)
            goto fail;
    }
    p->verify = true; /* E11: an .ec part's granules are always checked */
    atomic_init(&p->ec, e); /* no tier: part_open_ec sets it, part_open_ec_at's part releases nothing in ecgc */
    atomic_init(&p->refs, 1);
    atomic_init(&p->obsolete, false);
    *out = p;
    return 0;
fail:
    munmap((void *)p->map, p->map_len);
    free(p->icopy);
    free(p);
    ec_record_free(&e->rec);
    free(e);
    return rc;
}

/* Whether META's requires= line names "tier" (r7-tier writes it before the first .ec file can exist): 1 or 0, or
 * TSDB_ERR_IO (named) when META cannot be read. Uses db->tier_req (already set by tier_engine_attach before
 * load_parts) and a per-load_parts cache (db->meta_tier_cache) to avoid re-opening META for every .ec file at open. */
static int meta_says_tier(eng *db)
{
    if (db->tier_req || atomic_load(&ec_test_assume_tier))
        return 1;
    if (db->meta_tier_cache == 2)
        return 0;
    if (db->meta_tier_cache == 3)
        return 1;
    int fd = openat(db->dirfd, "META", O_RDONLY | O_CLOEXEC);
    if (fd < 0 && errno == ENOENT) {
        if (db->meta_tier_cache == 1)
            db->meta_tier_cache = 2;
        return 0;
    }
    char buf[65536];
    size_t n = 0;
    ssize_t k = fd < 0 ? -1 : 0;
    while (fd >= 0 && n < sizeof buf - 1 && (k = read(fd, buf + n, sizeof buf - 1 - n)) > 0)
        n += (size_t)k;
    int err = errno;
    if (fd >= 0)
        close(fd);
    if (k < 0) {
        fprintf(stderr, "tsdb: %s/META: %s: the open fails\n", db->dir, strerror(err));
        return TSDB_ERR_IO;
    }
    buf[n] = '\0';
    unsigned feats = 0;
    meta_requires_parse(buf, &feats, NULL, 0); /* the one parser of requires= (an unknown word failed the open before) */
    int has = (feats & META_REQ_TIER) != 0;
    if (db->meta_tier_cache == 1)
        db->meta_tier_cache = has ? 3 : 2;
    return has;
}

int ecpart_load(eng *db, const char *name, bool read_only, part **out)
{
    EC_CALLED(EC_CALL_LOAD);
    *out = NULL;
    if (ecpart_mutant == EC_MUT_SKIPNAME)
        return 0; /* planted: the file is left alone, and its rows vanish */
    int tier = meta_says_tier(db);
    if (tier < 0)
        return tier;
    if (!tier) {
        fprintf(stderr, "tsdb: %s/%s is an erasure-coded part, but the volume's META does not say requires=tier "
                        "(which is written before the first one): the open fails\n", db->dir, name);
        return TSDB_ERR_CORRUPT;
    }
    /* A settle's swap leaves one of the two (intent v4, recovered before the parts are listed): both, with no intent
     * naming them, is damage or a bug, and which of them holds the rows cannot be told. Only a writer's open has
     * recovered the intents (db.c). A reader follows MANIFEST where there is one (ecpart_follower_open, r7-tier's);
     * without one it lists the directory here, and beside a writer it sees the swap between its two renames, or a crash's
     * intent that the next writer finishes: both files hold the same rows (E7) under the same name, so a reader opens
     * the .ec as well and the cover rule lists one of them, as it does for a merge's output and its sources. Skipping
     * the .ec here instead would lose the rows when the .part is renamed after this check and before the listing
     * reaches it. */
    char src[96];
    if (!source_name(name, src))
        return TSDB_ERR_INVALID;
    if (!read_only && faccessat(db->dirfd, src, F_OK, AT_SYMLINK_NOFOLLOW) == 0) {
        fprintf(stderr, "tsdb: %s/%s and %s/%s both exist and no intent names them (a settle leaves one of the two): "
                        "the open fails\n", db->dir, src, db->dir, name);
        return TSDB_ERR_CORRUPT;
    } else if (!read_only && errno != ENOENT) {
        fprintf(stderr, "tsdb: %s/%s: %s: the open fails\n", db->dir, src, strerror(errno));
        return TSDB_ERR_IO;
    }
    return part_open_ec(db, name, name, out);
}

int ecpart_follower_open(eng *db, const char *name, part **out)
{
    EC_CALLED(EC_CALL_FOLLOWER_OPEN);
    *out = NULL;
    return tier_follower_ec(db, name); /* r7-tier: the follower stops with one line (D7) */
}

bool ec_index_intact(const part *p)
{
    EC_CALLED(EC_CALL_INDEX_INTACT);
    const struct ecpart *e = atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
    if (!e || e->test_mark)
        return part_map_intact(p); /* a marked part's map is a whole part */
    /* the record: its decoded copy is what is used, but damage to it is damage the next open refuses */
    if (xxh64(p->map + e->rec_off, (size_t)e->rec_len, 0) != e->rec_hash)
        return false;
    if (p->icopy)
        return true; /* the copy is what was checked */
    /* the geometry of the open, never the footer's now: damaged lengths must not move the read */
    size_t hl = p->hdr_len, il = (size_t)p->index_len, fl = sizeof(part_footer);
    size_t tl = (size_t)e->rec_off - fl - hl - il;
    return src_hash(p->map, hl, p->map + hl, il, tl, p->map + e->rec_off - fl) == p->index_hash;
}

int ec_cls_record(eng *db, const part *p)
{
    EC_CALLED(EC_CALL_CLS_RECORD);
    const struct ecpart *e = atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
    if (!e || e->test_mark)
        return 0; /* a marked part's granules are decoded like any other's */
    /* its granules are never decoded at open: the settle's check stands, and one that failed (or, with partition
     * classes on, never ran) leaves rows the open cannot place */
    if (e->rec.class_check == EC_CLS_FAILED || (e->rec.class_check != EC_CLS_PASSED && db->cls_on)) {
        fprintf(stderr, "tsdb: %s/%s: its record says the class check of its settle %s: the open fails\n", db->dir,
                p->name, e->rec.class_check == EC_CLS_FAILED ? "failed" : "did not run");
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

void ecpart_free(struct ecpart *e)
{
    if (!e)
        return;
    free(e->rec.sums);
    free(e);
}

int eng_test_mark_ec(eng *db, part *p, unsigned flags)
{
    int rc = TSDB_ERR_NOTFOUND;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i] == p) {
            rc = 0;
            break;
        }
    if (rc == 0 && p->merging)
        rc = TSDB_ERR_LOCKED; /* the merge would meet an .ec source (pw_copy_granule) and fail */
    if (rc == 0 && (flags & EC_MARK_EC) && !p->ec) {
        struct ecpart *e = calloc(1, sizeof *e);
        if (!e) {
            rc = TSDB_ERR_NOMEM;
        } else {
            e->test_mark = true;
            e->tier = db->tier;
            e->volume = db->tier_vol;
            atomic_store_explicit(&p->ec, e, memory_order_release); /* readers run beside this store */
        }
    }
    if (rc == 0)
        p->settling = (flags & EC_MARK_SETTLING) != 0;
    pthread_mutex_unlock(&db->lock);
    return rc;
}
