/*
 * Part files: immutable, sorted by (series, ts), written once and mmap'd.
 *
 *   header (24 bytes) | ts block, value block, ... per granule | pad to 8 |
 *   granule index (bit-packed, gindex.c; index_len bytes, a multiple of 8) | [tombstones] | footer (64 bytes)
 *
 * The tombstone trailer (24 bytes per range tombstone, then u32 count and u32
 * TOMB_MAGIC) is there only in parts that hold deletes, so every other part is
 * byte for byte what it was before deletes existed; a binary that predates
 * them rejects a part with a trailer at open (its size check), it never reads
 * one as if nothing were deleted. A part may hold tombstones and no granules.
 *
 * A granule holds rows of one series; its value block is coded for the
 * series' type and its index entry carries the statistics of its known
 * values. The header also records the newest WAL segment whose rows the part
 * holds. One hash in the footer covers the header, the index and the footer,
 * so a damaged wal_seq, row count or time range is caught at open like a
 * damaged index; each granule's blocks have a checksum of their own. The index
 * is read where it lies in the mapping, never copied (gindex.c).
 * A part is written to tmp_*.part, fsync'd, and renamed to its final name
 * p<partition>_<min_block>_<max_block>_<level>.part, which makes it visible.
 */
#include "internal.h"

#include <dirent.h>
#include <stddef.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define PART_MAGIC "TSDBPART"
#define PART_END_MAGIC "TSDBEND1"
#define PW_FLUSH_BYTES (4u << 20)
#define TOMB_MAGIC 0x424d4f54u /* "TOMB" */

static int write_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len) {
        ssize_t r = write(fd, p, len);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return TSDB_ERR_IO;
        }
        p += r;
        len -= (size_t)r;
    }
    return 0;
}

/* ---------------------------------------------------------------- reader */

/* Covers everything open trusts besides the granule blocks: chained over three pieces. */
/* The trailer is hashed only when present: parts without tombstones keep their hash. */
static uint64_t part_hash(const part_header *h, size_t hlen, const void *index, size_t len, const void *trailer,
                          size_t tlen, const part_footer *f)
{
    uint64_t x = xxh64(h, hlen, 0); /* a part carried over from format 8 hashes its 24-byte header, as it was written */
    x = xxh64(index, len, x);
    if (tlen)
        x = xxh64(trailer, tlen, x);
    return xxh64(f, offsetof(part_footer, index_hash), x);
}

/* Tombstones of the trailer [buf, buf + tlen): checked like the index, since they hide rows. */
static bool trailer_parse(const uint8_t *buf, size_t tlen, const tomb **t, uint32_t *n)
{
    *t = NULL;
    *n = 0;
    if (tlen == 0)
        return true;
    uint32_t cnt, magic;
    if (tlen < 8 || (uintptr_t)buf % _Alignof(tomb) != 0) /* read in place as tombs; part_open checks it too */
        return false;
    memcpy(&cnt, buf + tlen - 8, 4);
    memcpy(&magic, buf + tlen - 4, 4);
    if (magic != TOMB_MAGIC || cnt == 0 || (uint64_t)cnt * sizeof(tomb) + 8 != tlen)
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

int part_open(int dirfd, const char *name, bool pin, part **out)
{
    return part_open_as(dirfd, name, name, pin, false, out);
}

/* Whether the index, tombstones, header and footer in the mapping still hash to what the open checked. A part read
 * in place (without pin_index) can be damaged after the open; a merge copies granule statistics from the index
 * verbatim and would write them under a fresh hash, so it checks first (about 0.3% of the part's bytes). */
bool part_index_intact(const part *p)
{
    if (p->icopy)
        return true; /* the copy is what was checked */
    part_header h;
    part_footer f;
    memset(&h, 0, sizeof h);
    memcpy(&h, p->map, p->hdr_len);
    memcpy(&f, p->map + p->map_len - sizeof f, sizeof f);
    /* the geometry of the open, not the footer's now: damaged lengths must not move the read */
    size_t ilen = (size_t)p->index_len, tlen = p->map_len - sizeof f - (size_t)p->index_off - ilen;
    const uint8_t *ib = p->map + p->index_off;
    return part_hash(&h, p->hdr_len, ib, ilen, ib + ilen, tlen, &f) == p->index_hash;
}

/* Before a decision that is made for good from a part's index (a merge, which writes what it read under a fresh
 * checksum; a retention drop or a class record, which take its series ids): damage since the open must not decide
 * it. A damaged part is marked once, logged once and counted in st_broken_parts; it is then left out of merges and
 * drops (which would otherwise retry it every second, starving the rest), and stays for the next open to refuse. */
bool part_checked(eng *db, part *p, const char *what)
{
    if (atomic_load(&p->damaged))
        return false;
    if (part_index_intact(p))
        return true;
    if (!atomic_exchange(&p->damaged, true)) {
        fprintf(stderr, "tsdb: %s: the index of part %s no longer matches the checksum its open checked (damaged "
                        "since): the part is left as it is, out of merges and retention drops; the next open will "
                        "refuse it (restore it from a backup or snapshot)\n", what, p->name);
        atomic_fetch_add(&db->st_broken_parts, 1);
    }
    return false;
}

/* Opens the file `file` as the part called `name` (a follower may find a part
 * under its retired_ name). pin: read the index and tombstones from a checked copy (pin_index). v8_ok: the volume's
 * META says parts_v8=1 (`tsdb DIR upgrade` made it): a part with a format-8 header (24 bytes, version 8 in header
 * and footer alike) is then one of its old parts. Anywhere else it is damage: a v8 part in a database created at 9
 * cannot be a valid old part. */
int part_open_as(int dirfd, const char *file, const char *name, bool pin, bool v8_ok, part **out)
{
    *out = NULL;
    part *p = calloc(1, sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    int consumed = 0;
    if (strlen(name) >= sizeof p->name ||
        sscanf(name, "p%" SCNd64 "_%" SCNu64 "_%" SCNu64 "_%u.part%n", &p->partition, &p->min_block,
               &p->max_block, &p->level, &consumed) != 4 ||
        name[consumed] != '\0') {
        free(p);
        return TSDB_ERR_INVALID;
    }
    snprintf(p->name, sizeof p->name, "%s", name);
    p->dirfd = dirfd;

    int fd = openat(dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        int err = errno;
        free(p);
        return err == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        free(p);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size;
    if (size < PART_HDR_V8 + sizeof(part_footer)) {
        close(fd);
        free(p);
        return TSDB_ERR_CORRUPT;
    }
    void *map = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        free(p);
        return TSDB_ERR_IO;
    }
    p->map = map;
    p->map_len = size;
    p->created = st.st_mtime;
    p->written = st.st_mtime;

    part_header h;
    part_footer f;
    memset(&h, 0, sizeof h);
    memcpy(&h, p->map, PART_HDR_V8); /* the version is in these bytes in both formats */
    memcpy(&f, p->map + size - sizeof f, sizeof f);
    int rc = TSDB_ERR_CORRUPT;
    if (memcmp(h.magic, PART_MAGIC, 8) != 0 || memcmp(f.magic, PART_END_MAGIC, 8) != 0)
        goto fail;
    bool v8 = h.version == PREV_FORMAT_VERSION && v8_ok;
    if ((h.version != FORMAT_VERSION && !v8) || f.version != h.version || h.granule_rows == 0 ||
        h.granule_rows > MAX_GRANULE_ROWS)
        goto fail;
    p->hdr_len = v8 ? PART_HDR_V8 : (uint32_t)sizeof h;
    if (!v8) {
        if (size < sizeof(part_header) + sizeof(part_footer))
            goto fail;
        memcpy(&h, p->map, sizeof h);
        p->fno_lo = h.fno_lo;
        p->fno_hi = h.fno_hi;
        p->written_ms = h.written_ms;
        if (h.fno_lo > h.fno_hi || (h.fno_lo == 0) != (h.fno_hi == 0))
            goto fail;
    }
    p->grows = h.granule_rows;
    p->wal_seq = h.wal_seq;
    /* checked without wrapping: the hash is no authentication, and a crafted index_off near 2^64 would otherwise
       make part_hash read before the mapping. The writer pads the index to a multiple of 8, so the tombstone
       trailer, read in place as tomb structs, starts 8-byte aligned; a length that is not is refused here,
       before trailer_parse touches it (gix_parse refuses it too, but runs later). */
    if (f.index_off % 8 != 0 || f.index_len % 8 != 0 || f.index_off < p->hdr_len || f.index_off > (uint64_t)size - sizeof f ||
        f.index_len > (uint64_t)size - sizeof f - f.index_off)
        goto fail;
    size_t ilen = (size_t)f.index_len, tlen = (size_t)((uint64_t)size - sizeof f - f.index_off - f.index_len);
    const uint8_t *ib = p->map + f.index_off;
    if (pin) {
        /* The index and the tombstones are checked here, once, and then trusted by every query. In the mapping,
           damage that reaches the file later (a misdirected write, a page misread after it was evicted) changes
           them under the readers unchecked: a granule skipped, a statistic wrong (never a read outside the mapping:
           the gix_* readers clamp). pin_index reads them from a copy instead, so the bytes checked are the bytes
           used, at about 32 to 51 bytes of memory per granule. 8 zero bytes follow it, as the footer follows them
           in the file: gix_get loads 8 bytes at a time. */
        rc = TSDB_ERR_NOMEM;
        if (!(p->icopy = malloc(ilen + tlen + 8)))
            goto fail;
        memcpy(p->icopy, ib, ilen + tlen);
        memset(p->icopy + ilen + tlen, 0, 8);
        ib = p->icopy;
        p->pinned = ilen + tlen + 8;
        rc = TSDB_ERR_CORRUPT;
    }
    if (part_hash(&h, p->hdr_len, ib, ilen, ib + ilen, tlen, &f) != f.index_hash || f.min_ts > f.max_ts ||
        !trailer_parse(ib + ilen, tlen, &p->t, &p->nt) || gix_parse(ib, ilen, f.ngranules, &p->ix) != 0)
        goto fail;
    p->ng = f.ngranules;
    p->index_off = f.index_off;
    p->index_len = f.index_len;
    p->index_hash = f.index_hash;
    p->rows = f.rows;
    p->min_ts = f.min_ts;
    p->max_ts = f.max_ts;
    atomic_init(&p->refs, 1);
    atomic_init(&p->obsolete, false);
    *out = p;
    return 0;
fail:
    munmap((void *)p->map, p->map_len);
    free(p->icopy);
    free(p);
    return rc;
}

void part_ref(part *p) { atomic_fetch_add_explicit(&p->refs, 1, memory_order_relaxed); }

void part_unref(part *p)
{
    if (!p || atomic_fetch_sub_explicit(&p->refs, 1, memory_order_acq_rel) != 1)
        return;
    munmap((void *)p->map, p->map_len);
    free(p->icopy);
    if (atomic_load(&p->obsolete))
        unlinkat(p->dirfd, p->name, 0);
    free(p);
}

atomic_uint_fast64_t granule_checks;
atomic_uint hour_cut_min = HOUR_CUT_MIN;
void (*_Atomic index_reload_hook)(const part *p, uint32_t i, int at);

int granule_read_at(codec_ctx *c, const part *p, uint32_t i, int type, int64_t *ts, void *v, size_t cap, bool verify,
                    granule *g)
{
    gx_load(p, i, GW_LOC, g);
    return granule_read(c, p, g, type, ts, v, cap, verify);
}

int granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                 bool verify)
{
    uint64_t len = (uint64_t)g->ts_len + g->val_len;
    if (g->off < p->hdr_len || g->off > p->index_off || len > p->index_off - g->off || g->count == 0 ||
        g->count > p->grows || g->count > cap)
        return TSDB_ERR_CORRUPT;
    const uint8_t *blk = p->map + g->off;
    if (verify) {
        atomic_fetch_add_explicit(&granule_checks, 1, memory_order_relaxed);
        if ((uint32_t)xxh64(blk, len, 0) != g->crc)
            return TSDB_ERR_CORRUPT;
    }
    QS_ADD(rows_decoded, g->count);
    QS_ADD(bytes_read, len);
    int rc = block_decode_ts(c, blk, g->ts_len, g->count, ts);
    if (rc == 0 && v)
        rc = block_decode_vals(c, type, blk + g->ts_len, g->val_len, g->count, v);
    return rc;
}

/* ---------------------------------------------------------------- writer */

__thread uint64_t cur_fno_lo, cur_fno_hi;

static void pw_header(const part_writer *w, part_header *h)
{
    memset(h, 0, sizeof *h);
    memcpy(h->magic, PART_MAGIC, 8);
    h->version = FORMAT_VERSION;
    h->granule_rows = w->grows;
    h->wal_seq = w->wal_seq;
    h->fno_lo = w->fno_lo;
    h->fno_hi = w->fno_hi;
    h->written_ms = w->written_ms;
}

int pw_begin(part_writer *w, struct eng *db, int64_t partition, uint64_t wal_seq)
{
    memset(w, 0, sizeof *w);
    w->db = db;
    w->fd = -1;
    w->partition = partition;
    w->wal_seq = wal_seq;
    w->fno_lo = cur_fno_lo;
    w->fno_hi = cur_fno_hi;
    w->written_ms = eng_wall_ms();
    w->grows = db->granule_rows;
    int64_t cut = db->partition_ms * GRANULE_CUT; /* cls_len is partition_ms for every class without classes */
    w->cut_ms = db->cls_len[partition_class(db, partition)] > cut ? cut : 0;
    w->hc_min = atomic_load_explicit(&hour_cut_min, memory_order_relaxed);
    w->hk_end = INT64_MIN; /* no row given yet */
    /* inserts and flushes (CODEC_FAST) may skip zstd: a merge re-encodes every part no merge wrote (merge.c) */
    w->zstd_level = db->opt.insert_zstd_level < 0 ? db->opt.zstd_level : db->opt.insert_zstd_level;
    w->value_codec = db->opt.value_codec;
    w->min_ts = INT64_MAX;
    w->max_ts = INT64_MIN;
    w->cap = PW_FLUSH_BYTES + 2 * block_bound(w->grows) + sizeof(part_header) + 8;
    w->buf = malloc(w->cap);
    w->pend_ts = malloc(w->grows * sizeof(int64_t));
    w->pend_v = malloc(w->grows * sizeof(uint64_t)); /* the widest slot */
    w->cc = cc_get(db);
    if (!w->buf || !w->pend_ts || !w->pend_v || !w->cc) {
        pw_abort(w);
        return TSDB_ERR_NOMEM;
    }
    snprintf(w->tmp_name, sizeof w->tmp_name, "tmp_%d_%u.part", (int)getpid(),
             atomic_fetch_add(&db->tmp_counter, 1));
    w->fd = openat(db->dirfd, w->tmp_name, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (w->fd < 0) {
        pw_abort(w);
        return TSDB_ERR_IO;
    }
    part_header h;
    pw_header(w, &h);
    memcpy(w->buf, &h, sizeof h);
    w->len = sizeof h;
    return 0;
}

static int pw_flush(part_writer *w)
{
    int rc = write_all(w->fd, w->buf, w->len);
    w->off += w->len;
    w->len = 0;
    return rc;
}

/* A part's rows are sorted by (series, ts), one row per (series, ts): queries binary-search them and merges trust
 * it. Rows out of that order come from a bug upstream (a placement that read two class histories wrote a series
 * run going back in time, once); they fail the call and are never written. */
static int pw_disorder(part_writer *w, uint32_t series, int64_t a, int64_t b)
{
    fprintf(stderr, "tsdb: writing a part of partition %" PRId64 ": rows of series %u out of order (%" PRId64
                    " then %" PRId64 "): refused\n", w->partition, series, a, b);
    return TSDB_ERR_CORRUPT;
}

static int pw_push_index(part_writer *w, const granule *g)
{
    if (g->ts_min > g->ts_max)
        return pw_disorder(w, g->series, g->ts_min, g->ts_max);
    if (w->ng) {
        const granule *p = &w->idx[w->ng - 1];
        if (g->series < p->series || (g->series == p->series && g->ts_min <= p->ts_max))
            return pw_disorder(w, g->series, p->ts_max, g->ts_min);
    }
    if (w->ng == w->idx_cap) {
        size_t cap = w->idx_cap ? w->idx_cap * 2 : 64;
        granule *idx = realloc(w->idx, cap * sizeof *idx);
        if (!idx)
            return TSDB_ERR_NOMEM;
        w->idx = idx;
        w->idx_cap = cap;
    }
    w->idx[w->ng++] = *g;
    w->rows += g->count;
    if (g->ts_min < w->min_ts)
        w->min_ts = g->ts_min;
    if (g->ts_max > w->max_ts)
        w->max_ts = g->ts_max;
    return w->len >= PW_FLUSH_BYTES ? pw_flush(w) : 0;
}

static int pw_emit(part_writer *w, uint32_t series, int type, const int64_t *ts, const void *v, size_t n)
{
    for (size_t i = 1; i < n; i++)
        if (UNLIKELY(ts[i] <= ts[i - 1]))
            return pw_disorder(w, series, ts[i - 1], ts[i]);
    granule g;
    memset(&g, 0, sizeof g);
    uint8_t *start = w->buf + w->len;
    w->cc->effort = w->effort; /* the context comes from a pool shared with other writers */
    size_t tl = block_encode_ts(w->cc, w->zstd_level, ts, n, start);
    size_t vl = tl ? block_encode_vals(w->cc, w->zstd_level, w->value_codec, type, v, n, start + tl) : 0;
    if (!vl)
        return TSDB_ERR_NOMEM;
    granule_stats(type, v, n, &g);
    granule_twa(type, ts, v, n, &g);
    g.series = series;
    g.count = (uint32_t)n;
    g.ts_min = ts[0];
    g.ts_max = ts[n - 1];
    g.off = w->off + w->len;
    g.ts_len = (uint32_t)tl;
    g.val_len = (uint32_t)vl;
    g.crc = (uint32_t)xxh64(start, tl + vl, 0);
    w->len += tl + vl;
    return pw_push_index(w, &g);
}

static int pw_emit_pending(part_writer *w)
{
    if (!w->pend_n)
        return 0;
    int rc = pw_emit(w, w->pend_series, w->pend_type, w->pend_ts, w->pend_v, w->pend_n);
    w->pend_n = 0;
    return rc;
}

/* The start of hour q (q x HOUR_CUT_MS), clamped to the int64 range. */
static int64_t hc_edge(int64_t q)
{
    return q > INT64_MAX / HOUR_CUT_MS ? INT64_MAX : q < INT64_MIN / HOUR_CUT_MS ? INT64_MIN : q * HOUR_CUT_MS;
}

/* The writer was given cnt rows of series in hour h, the hour of the last one. */
static void hk_set(part_writer *w, uint32_t series, int64_t h, uint64_t cnt)
{
    w->hk_series = series;
    w->hk_hour = h;
    w->hk_end = hc_edge(h + 1);
    w->hk_cnt = cnt;
}

/* Counts ts[0..n) (rows of `series`, after every row given before) into the rows given in the hour of the last. */
static void hk_push(part_writer *w, uint32_t series, const int64_t *ts, size_t n)
{
    if (!w->hc_min || !n)
        return;
    if (w->hk_series == series && ts[n - 1] < w->hk_end) { /* the rows before lie in that hour, and so do all of ts */
        w->hk_cnt += n;
        return;
    }
    int64_t h = floor_div(ts[n - 1], HOUR_CUT_MS);
    hk_set(w, series, h, n - lower_bound_i64(ts, n, hc_edge(h)));
}

/* Whether the writer cuts `series` at the hour edge q x HOUR_CUT_MS when ts[0..m) (all before it) come next: at
 * least hc_min rows of the series given or coming lie in the hour before it. */
static bool hk_cut(const part_writer *w, uint32_t series, int64_t q, const int64_t *ts, size_t m)
{
    uint64_t c = m - lower_bound_i64(ts, m, hc_edge(q - 1));
    if (w->hk_series == series && w->hk_hour == q - 1) /* given rows lie in that hour (then all of ts[0..m) do) */
        c += w->hk_cnt;
    return c >= w->hc_min;
}

/* Rows of g in [a, b), as far as its count, time window and longest gap between consecutive rows (gap; 0: unknown)
 * tell: at least *lo, at most *hi. Rows at most gap apart put at least floor(w / gap) in a window of w ms inside the
 * granule's span, and at least ceil(d / gap) in d ms of it outside the window. */
static void hc_rows_in(const granule *g, uint64_t gap, int64_t a, int64_t b, uint64_t *lo, uint64_t *hi)
{
    if (b <= g->ts_min || a > g->ts_max) {
        *lo = *hi = 0;
        return;
    }
    if (a <= g->ts_min && b > g->ts_max) {
        *lo = *hi = g->count;
        return;
    }
    /* the window cuts the span: ts_min < ts_max */
    uint64_t a1 = a > g->ts_min ? (uint64_t)a : (uint64_t)g->ts_min;
    uint64_t b1 = b > g->ts_max ? (uint64_t)g->ts_max + 1 : (uint64_t)b;
    uint64_t wlen = b1 - a1;
    uint64_t pre = a > g->ts_min ? (uint64_t)a - (uint64_t)g->ts_min : 0;   /* time of the span before a */
    uint64_t post = b <= g->ts_max ? (uint64_t)g->ts_max - (uint64_t)b + 1 : 0; /* and from b on */
    uint64_t out, in;
    if (gap) {
        out = pre / gap + (pre % gap != 0) + post / gap + (post % gap != 0);
        in = wlen / gap;
    } else { /* only the first and the last row are known */
        out = (pre > 0) + (post > 0);
        in = (pre == 0) + (post == 0);
    }
    uint64_t h = g->count > out ? g->count - out : 0;
    *hi = h < wlen ? h : wlen; /* stamps are distinct integers */
    *lo = in < *hi ? in : *hi;
}

static uint64_t hc_gap(const part *src, uint32_t i, uint32_t count)
{
    uint64_t gap = count > 1 ? gix_get(&src->ix, i, GF_GAP) : 0; /* 0 for state series, which keep no TWA */
    return gap >= UINT32_MAX ? 0 : gap;                              /* saturated: not a bound */
}

int pw_copy_fits(part_writer *w, const part *src, uint32_t i, const granule *g, bool want_end, int64_t *end)
{
    *end = INT64_MAX;
    w->hk_dsrc = NULL;
    if (!w->hc_min)
        return 1;
    uint64_t gap = hc_gap(src, i, g->count), lo, hi, c = g->count;
    int64_t h0 = floor_div(g->ts_min, HOUR_CUT_MS), h1 = floor_div(g->ts_max, HOUR_CUT_MS);
    uint64_t hk0 = w->hk_series == g->series && w->hk_hour == h0 ? w->hk_cnt : 0; /* given rows in g's first hour */
    bool need = false;                                                            /* the bounds do not decide */
    if (h1 > h0) { /* edges inside at (h0 + 1) x 1 h .. h1 x 1 h */
        hc_rows_in(g, gap, hc_edge(h0), hc_edge(h0 + 1), &lo, &hi);
        if (lo + hk0 >= w->hc_min)
            return 0;
        need = hi + hk0 >= w->hc_min;
        if (!need && h1 > h0 + 1) {
            /* a later hour inside the span (span > 1 h) holds at most count - ceil((span - 1 h + 1) / gap) rows */
            uint64_t d = (uint64_t)g->ts_max - (uint64_t)g->ts_min - HOUR_CUT_MS + 1;
            uint64_t o = gap ? d / gap + (d % gap != 0) : 1, most = c > o ? c - o : 0;
            if (most >= w->hc_min && (uint64_t)(h1 - h0) > 64)
                need = true; /* decoding is cheaper than bounding many hours */
            for (int64_t h = h0 + 1; most >= w->hc_min && h < h1 && !need; h++) {
                hc_rows_in(g, gap, hc_edge(h), hc_edge(h + 1), &lo, &hi);
                if (lo >= w->hc_min)
                    return 0;
                need = hi >= w->hc_min;
            }
        }
    }
    if (want_end && !need && h1 < INT64_MAX / HOUR_CUT_MS) {
        uint64_t hk1 = h1 == h0 ? hk0 : 0;
        hc_rows_in(g, gap, hc_edge(h1), hc_edge(h1 + 1), &lo, &hi);
        if (lo + hk1 >= w->hc_min)
            *end = (h1 + 1) * HOUR_CUT_MS;
        else
            need = hi + hk1 >= w->hc_min;
    }
    if (!need)
        return 1;
    /* the stamps decide: decoded (timestamps only), and kept for pw_copy_granule's count of the hour */
    if (!w->hk_buf && !(w->hk_buf = malloc(MAX_GRANULE_ROWS * sizeof *w->hk_buf)))
        return TSDB_ERR_NOMEM;
    granule gl;
    memset(&gl, 0, sizeof gl);
    IX_HOOK(src, i, IX_FITS);
    gx_load(src, i, GW_TIME | GW_LOC, &gl);
    /* the count bounds the loop below: another than the caller's (the index changed in the mapping) is damage */
    if (gl.count != c)
        return TSDB_ERR_CORRUPT;
    int rc = granule_read(w->cc, src, &gl, 0, w->hk_buf, NULL, MAX_GRANULE_ROWS, true);
    if (rc)
        return rc;
    const int64_t *ts = w->hk_buf;
    *end = INT64_MAX;
    for (size_t j = 0; j < c;) {
        int64_t h = floor_div(ts[j], HOUR_CUT_MS);
        if (h >= INT64_MAX / HOUR_CUT_MS)
            break; /* no edge after this hour */
        size_t k = j + lower_bound_i64(ts + j, c - j, (h + 1) * HOUR_CUT_MS);
        uint64_t n = k - j + (j == 0 ? hk0 : 0); /* rows given and of g in hour h */
        if (k < c && n >= w->hc_min)
            return 0; /* the writer cuts at the edge inside */
        if (k == c && want_end && n >= w->hc_min)
            *end = (h + 1) * HOUR_CUT_MS;
        j = k;
    }
    w->hk_dsrc = src;
    w->hk_di = i;
    w->hk_dn = gl.count;
    return 1;
}

bool pw_hcut_before(const part_writer *w, int64_t t)
{
    if (!w->hc_min || !w->pend_n)
        return false;
    int64_t h = floor_div(w->pend_ts[w->pend_n - 1], HOUR_CUT_MS); /* hk_hour: pending rows were given last */
    return h < floor_div(t, HOUR_CUT_MS) && h < INT64_MAX / HOUR_CUT_MS && w->hk_series == w->pend_series &&
           w->hk_hour == h && w->hk_cnt >= w->hc_min;
}

/* Rows that may join the current granule out of ts[0..lim): up to the first hour edge after the pending rows at
 * which the writer cuts, unless the granule fills up first. */
static size_t hour_lim(const part_writer *w, uint32_t series, const int64_t *ts, size_t lim)
{
    if (w->hk_series == series && ts[lim - 1] < w->hk_end)
        return lim; /* all in the hour of the last row given: no edge (one-row calls of a merge take this) */
    int64_t from = w->pend_n ? w->pend_ts[w->pend_n - 1] : ts[0];
    size_t room = w->grows - w->pend_n;
    int64_t q = floor_div(from, HOUR_CUT_MS) + 1; /* the next edge is q x HOUR_CUT_MS, if representable */
    size_t m = 0;
    while (q <= INT64_MAX / HOUR_CUT_MS) {
        size_t e = m + lower_bound_i64(ts + m, lim - m, q * HOUR_CUT_MS); /* rows before the edge */
        if (e >= lim || e >= room)
            break; /* no row past it here, or the granule is full before it */
        if (hk_cut(w, series, q, ts, e))
            return e;
        m = e; /* ts[e] is past the edge: the next one that can cut is the one after it */
        q = floor_div(ts[e], HOUR_CUT_MS) + 1;
    }
    return lim;
}

/* Rows must be in (series, ts) order across calls. Granules never span series, nor a multiple of cut_ms, nor an
 * hour edge where the writer cuts the series (hc_min). */
int pw_add(part_writer *w, uint32_t series, int type, const int64_t *ts, const void *v, size_t n)
{
    int rc;
    size_t wd = type_width(type);
    const uint8_t *vb = v;
    if (w->pend_n && w->pend_series != series && (rc = pw_emit_pending(w)) != 0)
        return rc;
    while (n) {
        size_t lim = n; /* rows that may join the current granule */
        if (w->cut_ms) {
            int64_t first = w->pend_n ? w->pend_ts[0] : ts[0];
            int64_t q = floor_div(first, w->cut_ms) + 1; /* the next edge is q x cut_ms, if representable */
            lim = q > INT64_MAX / w->cut_ms ? n : lower_bound_i64(ts, n, q * w->cut_ms);
        }
        if (w->hc_min && lim)
            lim = hour_lim(w, series, ts, lim);
        if (lim == 0) { /* only with pending rows: they end at the edge */
            if ((rc = pw_emit_pending(w)) != 0)
                return rc;
            continue;
        }
        if (!w->pend_n && lim >= w->grows) {
            if ((rc = pw_emit(w, series, type, ts, vb, w->grows)) != 0)
                return rc;
            hk_push(w, series, ts, w->grows);
            ts += w->grows;
            vb += w->grows * wd;
            n -= w->grows;
            continue;
        }
        size_t take = MIN(lim, w->grows - w->pend_n);
        memcpy(w->pend_ts + w->pend_n, ts, take * sizeof *ts);
        memcpy(w->pend_v + w->pend_n * wd, vb, take * wd);
        w->pend_series = series;
        w->pend_type = type;
        w->pend_n += take;
        hk_push(w, series, ts, take);
        ts += take;
        vb += take * wd;
        n -= take;
        if ((w->pend_n == w->grows || (take == lim && n)) && (rc = pw_emit_pending(w)) != 0)
            return rc;
    }
    return 0;
}

/* Copies an already encoded granule verbatim. Any pending rows are emitted
 * first, so callers only do this when the pending rows belong to an earlier series. */
int pw_copy_granule(part_writer *w, const part *src, uint32_t i)
{
    int rc = pw_emit_pending(w);
    if (rc)
        return rc;
    granule gb;
    memset(&gb, 0, sizeof gb);
    IX_HOOK(src, i, IX_COPY);
    gx_load(src, i, GW_ALL, &gb);
    const granule *g = &gb;
    size_t len = (size_t)g->ts_len + g->val_len;
    if (g->off < src->hdr_len || g->off > src->index_off || len > src->index_off - g->off)
        return TSDB_ERR_CORRUPT;
    bool decoded = w->hc_min && w->hk_dsrc == src && w->hk_di == i; /* pw_copy_fits decoded its stamps */
    if (decoded && g->count != w->hk_dn)
        return TSDB_ERR_CORRUPT; /* not the count of the stamps in hk_buf: the index changed in the mapping */
    if (w->len + len > w->cap && (rc = pw_flush(w)) != 0)
        return rc;
    granule ng = *g;
    ng.off = w->off + w->len;
    if (len > w->cap) {
        /* cannot happen with matching granule sizes, but stay safe */
        if ((rc = write_all(w->fd, src->map + g->off, len)) != 0)
            return rc;
        w->off += len;
    } else {
        memcpy(w->buf + w->len, src->map + g->off, len);
        w->len += len;
    }
    if (w->hc_min && g->count) {
        if (decoded) {
            hk_push(w, g->series, w->hk_buf, g->count);
        } else { /* the rows of its last hour, or at least as many as its window and longest gap give */
            int64_t h = floor_div(g->ts_max, HOUR_CUT_MS);
            uint64_t in = g->count, hi;
            if (floor_div(g->ts_min, HOUR_CUT_MS) != h)
                hc_rows_in(g, hc_gap(src, i, g->count), hc_edge(h), hc_edge(h + 1), &in, &hi);
            hk_set(w, g->series, h, w->hk_series == g->series && w->hk_hour == h ? w->hk_cnt + in : in);
        }
    }
    w->hk_dsrc = NULL;
    return pw_push_index(w, &ng);
}

int pw_set_tombs(part_writer *w, const tomb *t, size_t n)
{
    free(w->tombs);
    w->tombs = NULL;
    w->nt = 0;
    if (n == 0)
        return 0;
    if (n > UINT32_MAX || !(w->tombs = malloc(n * sizeof *t)))
        return TSDB_ERR_NOMEM;
    memcpy(w->tombs, t, n * sizeof *t);
    w->nt = n;
    return 0;
}

void pw_abort(part_writer *w)
{
    if (w->fd >= 0) {
        close(w->fd);
        unlinkat(w->db->dirfd, w->tmp_name, 0);
    }
    if (w->cc)
        cc_put(w->db, w->cc);
    free(w->buf);
    free(w->idx);
    free(w->pend_ts);
    free(w->pend_v);
    free(w->tombs);
    free(w->hk_buf);
    memset(w, 0, sizeof *w);
    w->fd = -1;
}

static int pw_finish_mode(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, bool pending,
                          part **out)
{
    *out = NULL;
    eng *db = w->db;
    int rc = pw_emit_pending(w);
    if (rc)
        goto fail;
    if (w->ng == 0 && w->nt == 0 && !w->keep_empty) {
        pw_abort(w);
        return 0;
    }
    while ((w->off + w->len) % 8)
        w->buf[w->len++] = 0;
    part_footer f;
    memset(&f, 0, sizeof f);
    f.index_off = w->off + w->len;
    f.rows = w->rows;
    f.min_ts = w->min_ts;
    f.max_ts = w->max_ts;
    for (size_t i = 0; i < w->nt; i++) { /* the part's time range covers what it deletes: snapshots find it */
        f.min_ts = MIN(f.min_ts, w->tombs[i].start);
        f.max_ts = MAX(f.max_ts, w->tombs[i].end - 1);
    }
    if (w->ng == 0 && w->nt == 0)
        f.min_ts = f.max_ts = w->empty_ts;
    f.ngranules = (uint32_t)w->ng;
    f.version = FORMAT_VERSION;
    uint8_t *index = NULL;
    size_t ilen = 0;
    if ((rc = gix_encode(w->idx, w->ng, &index, &ilen)) != 0)
        goto fail;
    f.index_len = ilen;
    part_header h;
    pw_header(w, &h);
    size_t tlen = w->nt ? w->nt * sizeof(tomb) + 8 : 0;
    uint8_t *trailer = tlen ? malloc(tlen) : NULL;
    if (tlen && !trailer) {
        free(index);
        rc = TSDB_ERR_NOMEM;
        goto fail;
    }
    if (tlen) {
        uint32_t cnt = (uint32_t)w->nt, magic = TOMB_MAGIC;
        memcpy(trailer, w->tombs, w->nt * sizeof(tomb));
        memcpy(trailer + tlen - 8, &cnt, 4);
        memcpy(trailer + tlen - 4, &magic, 4);
    }
    f.index_hash = part_hash(&h, sizeof h, index, ilen, trailer, tlen, &f);
    memcpy(f.magic, PART_END_MAGIC, 8);
    /* the range and the write time may be set after pw_begin wrote the header (a merge knows its sources only then):
       the header on disk is the one hashed */
    rc = pw_flush(w);
    if (rc == 0 && pwrite(w->fd, &h, sizeof h, 0) != (ssize_t)sizeof h)
        rc = TSDB_ERR_IO;
    if (rc != 0 || (rc = write_all(w->fd, index, ilen)) != 0 ||
        (tlen && (rc = write_all(w->fd, trailer, tlen)) != 0) || (rc = write_all(w->fd, &f, sizeof f)) != 0) {
        free(trailer);
        free(index);
        goto fail;
    }
    free(trailer);
    free(index);
    /* a deferred sync starts the writeback now: the caller syncs all its parts after the last one is written,
     * by then mostly one journal commit for all of them instead of one each */
    if (db->opt.fsync && pending && w->defer_fd) {
        if (sync_file_range(w->fd, 0, 0, SYNC_FILE_RANGE_WRITE) != 0 && errno != ESPIPE && errno != EINVAL) {
            rc = TSDB_ERR_IO;
            goto fail;
        }
    } else if (db->opt.fsync && tsdb_fdatasync(w->fd) != 0) {
        rc = TSDB_ERR_IO;
        goto fail;
    }
    char name[96];
    snprintf(name, sizeof name, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_%u.part", w->partition, min_block,
             max_block, level);
    if (pending) {
        if (db->opt.fsync && w->defer_fd)
            *w->defer_fd = w->fd; /* the caller syncs and closes it */
        else
            close(w->fd);
        w->fd = -1; /* the tmp file stays */
        rc = part_open_as(db->dirfd, w->tmp_name, name, db->opt.pin_index, db->parts_v8, out);
        if (rc == 0)
            (*out)->verify = db->opt.verify_checksums;
        if (rc == 0)
            snprintf((*out)->pending, sizeof (*out)->pending, "%s", w->tmp_name);
        else
            unlinkat(db->dirfd, w->tmp_name, 0);
        pw_abort(w);
        return rc;
    }
    /* a merge that finishes after the engine failed: a failed engine names no new files */
    if (eng_check(db) || renameat(db->dirfd, w->tmp_name, db->dirfd, name) != 0) {
        rc = TSDB_ERR_IO;
        goto fail;
    }
    close(w->fd);
    w->fd = -1;
    if (eng_sync_dir(db, "fsync of the directory after naming a merged part") != 0) {
        /* the caller (a merge) would retire the sources next */
        unlinkat(db->dirfd, name, 0);
        pw_abort(w);
        return TSDB_ERR_IO;
    }
    rc = part_open_as(db->dirfd, name, name, db->opt.pin_index, db->parts_v8, out);
    if (rc == 0)
        (*out)->verify = db->opt.verify_checksums;
    if (rc != 0)
        unlinkat(db->dirfd, name, 0);
    pw_abort(w);
    return rc;
fail:
    pw_abort(w);
    return rc;
}

int pw_finish(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out)
{
    return pw_finish_mode(w, min_block, max_block, level, false, out);
}

int pw_finish_pending(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out)
{
    return pw_finish_mode(w, min_block, max_block, level, true, out);
}

/* ----------------------------------------------------- atomic commits */
/*
 * One insert can make several parts (one per partition). They are written
 * and fsync'd under tmp names; then an intent file listing the renames is
 * made durable, which is the commit point; then the renames happen and the
 * intent is removed. tmp files without an intent are deleted at open. So an
 * insert is all or nothing.
 *
 * An intent reads "tsdb-intent 3 <n>", n lines "<tmp> <final>", and
 * "end <xxh64 of everything before it>". At open, one whose count or
 * checksum does not match was torn while it was written: no rename can have
 * followed it (renames start only after the intent and the directory are
 * synced), so the insert is rolled back whole. A complete intent is rolled
 * forward only when every one of its parts is still there, under its tmp or
 * its final name: after a failed directory fsync the intent's name can be
 * durable while some tmp names are not, and renaming the ones that survived
 * would publish part of the insert. If a part is missing and none was
 * renamed yet, the insert is rolled back; if some were renamed, the insert
 * was published and the missing ones have since been merged or dropped.
 * Failing I/O (reading the intent, a rename) fails the open.
 * Not covered: an intent damaged on disk after its renames ran (a synced file
 * gone bad) also reads as torn, and the parts it renamed are rolled back
 * although the insert may have been acknowledged.
 */

#define INTENT_PREFIX "txn_"

void parts_discard(eng *db, part **ps, size_t n)
{
    /* after a failed commit, files stay as they are: the next open sorts them out */
    bool keep = eng_check(db) != 0;
    for (size_t i = 0; i < n; i++) {
        if (keep)
            ;
        else if (ps[i]->pending[0])
            unlinkat(db->dirfd, ps[i]->pending, 0);
        else
            atomic_store(&ps[i]->obsolete, true);
        part_unref(ps[i]);
    }
}

int parts_commit(eng *db, part **ps, size_t n)
{
    if (n == 0)
        return 0;
    if (eng_check(db))
        return TSDB_ERR_IO;
    /* block numbers in commit order: the caller publishes before anyone else commits */
    for (size_t i = 0; i < n; i++) {
        part *p = ps[i];
        p->min_block = p->max_block = atomic_fetch_add(&db->next_block, 1);
        p->level = 0;
        snprintf(p->name, sizeof p->name, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_0.part", p->partition, p->min_block,
                 p->max_block);
    }
    char intent[64] = "";
    if (n > 1) {
        size_t cap = 32 + n * 200, len = 0;
        char *buf = malloc(cap);
        if (!buf)
            return TSDB_ERR_NOMEM;
        len += (size_t)snprintf(buf + len, cap - len, "tsdb-intent 3 %zu\n", n);
        for (size_t i = 0; i < n; i++)
            len += (size_t)snprintf(buf + len, cap - len, "%s %s\n", ps[i]->pending, ps[i]->name);
        len += (size_t)snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
        snprintf(intent, sizeof intent, INTENT_PREFIX "%d_%u.intent", (int)getpid(),
                 atomic_fetch_add(&db->tmp_counter, 1));
        int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        int rc = fd < 0 || write_all(fd, buf, len) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
        if (fd >= 0)
            close(fd);
        free(buf);
        if (rc) { /* nothing is committed: the intent's name was not synced, and is gone now */
            unlinkat(db->dirfd, intent, 0);
            return rc;
        }
        if (eng_sync_dir(db, "fsync of the directory after writing an insert's intent") != 0)
            return TSDB_ERR_IO; /* the intent's name is the commit; committed or not: the next open decides */
    }
    for (size_t i = 0; i < n; i++) {
        if (renameat(db->dirfd, ps[i]->pending, db->dirfd, ps[i]->name) != 0) {
            eng_fail(db, "rename of a new part", errno);
            return TSDB_ERR_IO;
        }
        ps[i]->pending[0] = '\0';
    }
    if (eng_sync_dir(db, "fsync of the directory after publishing parts") != 0)
        return TSDB_ERR_IO; /* not published; the next open lists the parts that made it */
    if (intent[0])
        unlinkat(db->dirfd, intent, 0);
    return 0;
}

/* An intent renames a part writer's tmp file to a part name, nothing else: the hash is no authentication, and a
 * crafted intent must not rename over MANIFEST or SERIES, or pull a file in from another directory. */
static bool intent_names_valid(const char *tmp, const char *fin) { return part_tmp_name(tmp) && part_file_name(fin); }

/* The names the engine writes, matched exactly. A volume may be a directory that held the user's files before
 * (tsdb_volume_add_dir), so a cleanup at open touches only these, never a user's tmp_notes.txt or txn_list. */
static const char *skip_digits(const char *s)
{
    const char *d = s;
    while (*d >= '0' && *d <= '9')
        d++;
    return d > s && d - s <= 20 ? d : NULL;
}

/* PREFIX <digits>_<digits> SUFFIX, as written with "%d_%u" from getpid() and a counter */
static bool two_numbers_name(const char *name, const char *prefix, const char *suffix)
{
    size_t lp = strlen(prefix);
    if (strncmp(name, prefix, lp) != 0)
        return false;
    const char *s = skip_digits(name + lp);
    if (!s || *s != '_' || !(s = skip_digits(s + 1)))
        return false;
    return strcmp(s, suffix) == 0;
}

bool part_tmp_name(const char *name) { return two_numbers_name(name, "tmp_", ".part"); }

/* p<partition>_<min_block>_<max_block>_<level>.part; the partition may be negative (before 1970) */
bool part_file_name(const char *name)
{
    if (name[0] != 'p')
        return false;
    const char *s = name + 1 + (name[1] == '-');
    for (int i = 0; i < 4; i++) {
        if (!(s = skip_digits(s)) || *s != (i < 3 ? '_' : '.'))
            return false;
        s++;
    }
    return strcmp(s, "part") == 0;
}

bool intent_file_name(const char *name) { return two_numbers_name(name, INTENT_PREFIX, ".intent"); }

/* Parses an intent into its n renames; false when it is torn or damaged. */
static bool intent_parse(char *buf, size_t len, char (**tmp)[96], char (**fin)[128], size_t *n)
{
    *tmp = NULL;
    *fin = NULL;
    *n = 0;
    if (len < 4 || buf[len - 1] != '\n' || memchr(buf, '\0', len))
        return false;
    buf[len - 1] = '\0';
    char *end = strrchr(buf, '\n'); /* the line before "end <hash>" */
    uint64_t want;
    char extra;
    if (!end || sscanf(end + 1, "end %16" SCNx64 "%c", &want, &extra) != 1 || strlen(end + 1) != 20 ||
        xxh64(buf, (size_t)(end + 1 - buf), 0) != want)
        return false;
    *end = '\0';
    char *line = buf, *next = strchr(line, '\n');
    size_t count;
    if (!next)
        return false;
    *next = '\0';
    if (sscanf(line, "tsdb-intent 3 %zu%c", &count, &extra) != 1 || count < 2 || count > 1u << 20)
        return false;
    *tmp = malloc(count * sizeof **tmp);
    *fin = malloc(count * sizeof **fin);
    if (!*tmp || !*fin)
        return false;
    for (line = next + 1; *n < count; line = next + 1) {
        next = strchr(line, '\n');
        if (next)
            *next = '\0';
        if (sscanf(line, "%95s %127s%c", (*tmp)[*n], (*fin)[*n], &extra) != 2 || !intent_names_valid((*tmp)[*n], (*fin)[*n]))
            return false;
        ++*n;
        if (!next)
            break;
    }
    return *n == count && !next;
}

/* At open: finish committed inserts that a crash interrupted, roll back the others. */
int intents_recover(eng *db)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    struct dirent *e;
    bool any = false;
    int rc = 0;
    /* readdir returning NULL with errno set is a read error, never the end: an intent skipped would leave a
     * half-committed insert (ha.md 15.5, "damage is never silent") */
    for (errno = 0; rc == 0 && (e = readdir(d)); errno = 0) {
        if (!intent_file_name(e->d_name))
            continue; /* only the names parts_commit writes: a user's txn_* file in a volume is not read, nor deleted */
        int fd = openat(db->dirfd, e->d_name, O_RDONLY | O_CLOEXEC);
        struct stat st;
        char *buf = NULL;
        if (fd < 0 || fstat(fd, &st) != 0 || !(buf = malloc((size_t)st.st_size + 1)) ||
            pread(fd, buf, (size_t)st.st_size, 0) != st.st_size) {
            fprintf(stderr, "tsdb: cannot read the insert intent %s: %s\n", e->d_name,
                    buf || fd < 0 ? strerror(errno ? errno : EIO) : "out of memory");
            rc = buf || fd < 0 ? TSDB_ERR_IO : TSDB_ERR_NOMEM;
            free(buf);
            if (fd >= 0)
                close(fd);
            break;
        }
        close(fd);
        char (*tmp)[96], (*fin)[128];
        size_t n;
        if (!intent_parse(buf, (size_t)st.st_size, &tmp, &fin, &n)) {
            fprintf(stderr, "tsdb: insert intent %s is torn: the insert is rolled back\n", e->d_name);
        } else {
            size_t missing = 0, renamed = 0;
            for (size_t i = 0; i < n; i++) {
                if (faccessat(db->dirfd, tmp[i], F_OK, 0) == 0)
                    continue;
                if (faccessat(db->dirfd, fin[i], F_OK, 0) == 0)
                    renamed++;
                else
                    missing++;
            }
            if (missing && !renamed) {
                fprintf(stderr, "tsdb: %zu of the %zu parts of insert intent %s are gone: the insert is rolled back\n",
                        missing, n, e->d_name);
            } else {
                for (size_t i = 0; i < n && rc == 0; i++)
                    if (faccessat(db->dirfd, tmp[i], F_OK, 0) == 0 && renameat(db->dirfd, tmp[i], db->dirfd, fin[i]) != 0) {
                        fprintf(stderr, "tsdb: rolling insert intent %s forward: rename of %s: %s\n", e->d_name,
                                tmp[i], strerror(errno));
                        rc = TSDB_ERR_IO;
                    }
            }
        }
        free(tmp);
        free(fin);
        free(buf);
        if (rc == 0) {
            /* the tmp files of a rolled-back insert go with the other tmp files, after this */
            if (unlinkat(db->dirfd, e->d_name, 0) != 0 && errno != ENOENT)
                rc = TSDB_ERR_IO;
            any = true;
        }
    }
    if (rc == 0 && errno != 0) {
        fprintf(stderr, "tsdb: reading the directory failed: %s: the open fails\n", strerror(errno));
        rc = TSDB_ERR_IO;
    }
    closedir(d);
    if (rc == 0 && any && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)
        return TSDB_ERR_IO; /* at open: the open fails */
    return rc;
}
