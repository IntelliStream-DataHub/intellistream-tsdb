/*
 * Memtable and write-ahead log for small inserts.
 *
 * Small writes (tsdb_append, and tsdb_insert below wal_max_batch_rows) go
 * into the active memtable, which queries read directly, and into the WAL.
 * The memtable becomes parts when it is full or old enough.
 *
 * WAL: one segment file wal_<seq>.log per memtable, holding records of
 * [u32 rows][u32 header check][u64 hash][rows x (u32 series, i64 ts, u64 value slot)],
 * one per group commit. The header check binds a record to its segment and
 * offset, so zeros, garbage and stale bytes never pass for a record. Replay
 * keeps the records up to the first bad one. That one is a torn tail, cut,
 * unless a valid record follows it: then it is damage to rows that may have
 * been acknowledged, and the open fails (TSDB_ERR_CORRUPT, with a log line)
 * and keeps the segment, rather than drop them silently. Removing the segment
 * from the directory accepts the loss. A read error during replay fails the
 * open too: it never passes for the end of the segment.
 * Durability is group commit: one writer at a time writes everything
 * appended so far and fdatasyncs it, and wakes every waiter whose rows that
 * covered. Waiters sleep on a condition variable rather than queueing on a
 * mutex, so a covered waiter returns at once instead of waiting its turn. Each part records the WAL sequence of the data it holds, so on
 * restart a segment is replayed only into partitions that do not already
 * contain its data, even when a crash interrupted its flush half way.
 *
 * A failed WAL write or fdatasync is final (eng_fail, "fsyncgate"): the
 * failing writer marks the engine failed before it hands on the writer role,
 * so no waiter is acknowledged for rows that went through the failed call,
 * wal_durable never moves again and no byte follows a possibly torn record,
 * which is therefore the last one replay sees.
 *
 * Memtables are never modified after they stop being active, and readers
 * never modify them, so queries can read a memtable while it is flushed.
 * They hold every value as an 8-byte slot (4-byte types zero-extended), so
 * series of all types share one; a flush writes each type's series apart.
 *
 * Lock order: flush_lock > (the wal_writing role) > db->lock > buf_lock;
 * wal_sync_lock only guards the wal_writing flag.
 */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef O_DIRECT /* macOS: no direct reads; a fresh open is what there is */
#define O_DIRECT 0
#endif

static int64_t mono_ms(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

/* ------------------------------------------------------------- memtable */

memtable *mt_new(uint64_t wal_seq)
{
    memtable *m = calloc(1, sizeof *m);
    if (!m)
        return NULL;
    atomic_init(&m->refs, 1);
    m->wal_seq = wal_seq;
    m->min_ts = INT64_MAX;
    m->max_ts = INT64_MIN;
    m->created_ms = mono_ms();
    return m;
}

void mt_ref(memtable *m) { atomic_fetch_add(&m->refs, 1); }

void mt_unref(memtable *m)
{
    if (!m || atomic_fetch_sub(&m->refs, 1) != 1)
        return;
    for (uint32_t i = 0; i < m->nids; i++) {
        mt_series *s = &m->s[m->ids[i]];
        free(s->ts);
        free(s->v);
        free(s->lsn);
    }
    free(m->s);
    free(m->ids);
    free(m);
}

/* Room for one more row of series id. */
static int mt_reserve(memtable *m, uint32_t id)
{
    if (id >= m->scap) {
        uint32_t cap = MAX(id + 1, m->scap * 2);
        cap = MAX(cap, 64u);
        mt_series *s = realloc(m->s, cap * sizeof *s);
        if (!s)
            return TSDB_ERR_NOMEM;
        memset(s + m->scap, 0, (cap - m->scap) * sizeof *s);
        m->s = s;
        m->scap = cap;
    }
    mt_series *s = &m->s[id];
    if (s->n < s->cap)
        return 0;
    if (s->cap == 0 && m->nids == m->idcap) {
        uint32_t c = m->idcap ? m->idcap * 2 : 64;
        uint32_t *ids = realloc(m->ids, c * sizeof *ids);
        if (!ids)
            return TSDB_ERR_NOMEM;
        m->ids = ids;
        m->idcap = c;
    }
    uint32_t cap = s->cap ? s->cap * 2 : 64;
    int64_t *t = realloc(s->ts, cap * sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    s->ts = t;
    uint64_t *v = realloc(s->v, cap * sizeof *v);
    if (!v)
        return TSDB_ERR_NOMEM;
    s->v = v;
    uint64_t *l = realloc(s->lsn, cap * sizeof *l);
    if (!l)
        return TSDB_ERR_NOMEM;
    s->lsn = l;
    if (s->cap == 0) {
        m->ids[m->nids++] = id;
        s->sorted = true;
    }
    s->cap = cap;
    return 0;
}

/* Adds rows to a memtable the caller may modify (the active one, under buf_lock). */
int mt_add(memtable *m, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n,
           uint64_t lsn0)
{
    for (size_t i = 0; i < n; i++) {
        uint32_t id = series ? series[i] : cs;
        int rc = mt_reserve(m, id);
        if (rc)
            return rc;
        mt_series *s = &m->s[id];
        if (s->n && ts[i] < s->ts[s->n - 1])
            s->sorted = false;
        s->ts[s->n] = ts[i];
        s->v[s->n] = slot_get(v, w, i);
        s->lsn[s->n] = lsn0 ? lsn0 + i : 0;
        s->n++;
        m->rows++;
        if (ts[i] < m->min_ts)
            m->min_ts = ts[i];
        if (ts[i] > m->max_ts)
            m->max_ts = ts[i];
    }
    return 0;
}

/* The rows just added took the numbers first .. first + n - 1 (the memtable's position range: rows only). */
void mt_note_fno(memtable *m, uint64_t first, size_t n)
{
    if (n == 0)
        return;
    m->fno_lo = m->fno_lo ? MIN(m->fno_lo, first) : first;
    m->fno_hi = MAX(m->fno_hi, first + n - 1);
}

typedef struct tv {
    int64_t t;
    uint64_t v;
    uint32_t i; /* insertion order: equal timestamps keep it, so the last write stays last */
} tv;

static int cmp_tv(const void *a, const void *b)
{
    const tv *x = a, *y = b;
    if (x->t != y->t)
        return (x->t > y->t) - (x->t < y->t);
    return (x->i > y->i) - (x->i < y->i);
}

/* Stable sort of parallel arrays by time (series that arrived out of order). */
static int sort_by_time(int64_t *t, uint64_t *v, size_t n)
{
    tv *a = malloc(n * sizeof *a);
    if (!a)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++)
        a[i] = (tv){t[i], v[i], (uint32_t)i};
    qsort(a, n, sizeof *a, cmp_tv);
    for (size_t i = 0; i < n; i++) {
        t[i] = a[i].t;
        v[i] = a[i].v;
    }
    free(a);
    return 0;
}

/* Keeps the last of each run of equal timestamps (input sorted, stable). */
static size_t keep_last(int64_t *t, uint64_t *v, size_t n)
{
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        if (o && t[o - 1] == t[i])
            o--;
        t[o] = t[i];
        v[o] = v[i];
        o++;
    }
    return o;
}

/* The slots of out narrowed to width w, in place. */
static void narrow(mem_run *out, size_t w)
{
    if (w == 8)
        return;
    const uint64_t *src = (const uint64_t *)(void *)out->v;
    for (size_t i = 0; i < out->n; i++) {
        uint32_t x = (uint32_t)src[i];
        memcpy(out->v + 4 * i, &x, 4); /* behind the reads: 4 * i <= 8 * i */
    }
}

/* Copies the rows of series id in [start, end): sorted by time, last write
 * per timestamp. Reads only; the caller holds buf_lock when m is active. */
int mt_collect(const memtable *m, uint32_t id, size_t w, int64_t start, int64_t end, uint64_t visible, mem_run *out)
{
    memset(out, 0, sizeof *out);
    if (id >= m->scap || m->s[id].n == 0)
        return 0;
    const mt_series *s = &m->s[id];
    /* rows are appended in WAL order: the visible ones are a prefix */
    size_t vn = s->n;
    if (visible != UINT64_MAX) {
        size_t lo = 0, hi = s->n;
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            if (s->lsn[mid] <= visible)
                lo = mid + 1;
            else
                hi = mid;
        }
        vn = lo;
        if (!vn)
            return 0;
    }
    size_t lo = 0, hi = vn;
    if (s->sorted) {
        lo = lower_bound_i64(s->ts, vn, start);
        hi = lower_bound_i64(s->ts, vn, end);
        if (lo >= hi)
            return 0;
    }
    out->ts = malloc((hi - lo) * sizeof *out->ts);
    out->v = malloc((hi - lo) * sizeof(uint64_t));
    if (!out->ts || !out->v) {
        mem_run_free(out);
        return TSDB_ERR_NOMEM;
    }
    uint64_t *ov = (uint64_t *)(void *)out->v;
    if (s->sorted) {
        out->n = hi - lo;
        memcpy(out->ts, s->ts + lo, out->n * sizeof *out->ts);
        memcpy(ov, s->v + lo, out->n * sizeof *ov);
        out->n = keep_last(out->ts, ov, out->n);
        narrow(out, w);
        return 0;
    }
    for (size_t i = 0; i < vn; i++) {
        if (s->ts[i] >= start && s->ts[i] < end) {
            out->ts[out->n] = s->ts[i];
            ov[out->n] = s->v[i];
            out->n++;
        }
    }
    int rc = sort_by_time(out->ts, ov, out->n);
    out->n = keep_last(out->ts, ov, out->n);
    narrow(out, w);
    return rc;
}

void mem_run_free(mem_run *r)
{
    free(r->ts);
    free(r->v);
    memset(r, 0, sizeof *r);
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* Writes an immutable memtable as parts, returned unpublished in `out`: one
 * batch per value type, all committed together by the caller. */
static int mt_write(eng *db, const memtable *m, part_list *out)
{
    if (m->rows == 0)
        return 0;
    int rc0 = cls_classify_memtable(db, m); /* fresh series get their class before their rows are placed */
    if (rc0)
        return rc0;
    int ucls = cls_uniform(db, m->ids, 0, m->nids);
    uint32_t *s = malloc(m->rows * sizeof *s);
    int64_t *t = malloc(m->rows * sizeof *t);
    uint64_t *v = malloc(m->rows * sizeof *v);
    uint32_t *ids = malloc((m->nids ? m->nids : 1) * sizeof *ids);
    int rc = (!s || !t || !v || !ids) ? TSDB_ERR_NOMEM : 0;
    if (rc == 0) {
        memcpy(ids, m->ids, m->nids * sizeof *ids);
        qsort(ids, m->nids, sizeof *ids, cmp_u32);
    }
    for (int type = 0; type < NTYPES && rc == 0; type++) {
        size_t k = 0, w = type_width(type);
        for (uint32_t i = 0; i < m->nids && rc == 0; i++) {
            const mt_series *ms = &m->s[ids[i]];
            if (series_type(&db->series, ids[i]) != type)
                continue;
            for (uint32_t j = 0; j < ms->n; j++)
                s[k + j] = ids[i];
            memcpy(t + k, ms->ts, ms->n * sizeof *t);
            memcpy(v + k, ms->v, ms->n * sizeof *v);
            if (!ms->sorted)
                rc = sort_by_time(t + k, v + k, ms->n);
            k += ms->n;
        }
        if (rc || !k)
            continue;
        if (w == 4) /* the slots to this type's width, in place */
            for (size_t i = 0; i < k; i++) {
                uint32_t x = (uint32_t)v[i];
                memcpy((uint8_t *)v + 4 * i, &x, 4);
            }
        /* in (series, ts) order; insert_rows re-sorts only when partitions interleave */
        cur_fno_lo = m->fno_lo, cur_fno_hi = m->fno_hi; /* the parts' position range (pw_begin) */
        rc = insert_rows(db, type, s, 0, t, v, k, m->wal_seq, out, false, ucls, NULL, NULL);
        cur_fno_lo = cur_fno_hi = 0;
    }
    free(s);
    free(t);
    free(v);
    free(ids);
    return rc;
}

/* ------------------------------------------------------------------ WAL */

static void wal_name(char *buf, size_t len, uint64_t seq) { snprintf(buf, len, "wal_%016" PRIx64 ".log", seq); }

static int wal_open_segment(eng *db, uint64_t seq, int *fd)
{
    char name[64];
    wal_name(name, sizeof name, seq);
    *fd = openat(db->dirfd, name, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (*fd < 0)
        return TSDB_ERR_IO;
    /* the segment must exist after a crash before rows in it are acknowledged */
    if (eng_sync_dir(db, "fsync of the directory after creating a WAL segment") != 0) {
        close(*fd);
        *fd = -1;
        return TSDB_ERR_IO;
    }
    return 0;
}

/* errno tells why on failure */
static int write_full(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len) {
        ssize_t r = write(fd, p, len);
        if (r == 0)
            errno = ENOSPC;
        if (r <= 0) {
            if (errno == EINTR)
                continue;
            return TSDB_ERR_IO;
        }
        p += r;
        len -= (size_t)r;
    }
    return 0;
}

static uint32_t wal_head_check(uint32_t word, uint64_t hash, uint64_t seq, uint64_t off)
{
    uint8_t t[12];
    store32(t, word);
    memcpy(t + 4, &hash, 8);
    return (uint32_t)xxh64(t, sizeof t, seq * 0x9e3779b97f4a7c15ull ^ off);
}

/* The size of the record whose header (len and kind in place) is at h. */
static size_t wal_rec_size(const uint8_t *h)
{
    uint32_t w = load32(h), n = w & 0x0fffffffu;
    return WAL_HDR + (size_t)n * ((w >> 28) == WAL_KIND_ROWS ? WAL_ROW : 1);
}

/* Seals the records in buf[0, len), which will be written at segment offset off: the group commit's time, the hash
 * over fno, commit_ms and the payload, and the header check. Their len, kind and fno are set where they were
 * appended (wal_buffer, wal_marker). Gives the offset and hash of the last one. */
static void wal_seal(uint8_t *buf, size_t len, uint64_t seq, uint64_t off, int64_t commit_ms, size_t *last_pos,
                     uint64_t *last_hash)
{
    for (size_t pos = 0; pos < len;) {
        uint8_t *h = buf + pos;
        size_t sz = wal_rec_size(h);
        memcpy(h + 24, &commit_ms, 8);
        uint64_t hash = xxh64(h + 16, sz - 16, 0);
        store32(h + 4, wal_head_check(load32(h), hash, seq, off + pos));
        memcpy(h + 8, &hash, 8);
        *last_pos = pos;
        *last_hash = hash;
        pos += sz;
    }
}

/* One rows record of n rows (the rows at buf + WAL_HDR) numbered from fno, written at once (the relog's). */
static int wal_write_record(int fd, uint64_t seq, uint64_t off, uint8_t *buf, size_t nrows, uint64_t fno)
{
    memset(buf, 0, WAL_HDR);
    store32(buf, (uint32_t)nrows | (uint32_t)WAL_KIND_ROWS << 28);
    memcpy(buf + 16, &fno, 8);
    size_t last;
    uint64_t hash;
    wal_seal(buf, WAL_HDR + nrows * WAL_ROW, seq, off, eng_wall_ms(), &last, &hash);
    return write_full(fd, buf, WAL_HDR + nrows * WAL_ROW);
}

size_t wal_record(const uint8_t *p, size_t avail, uint64_t seq, uint64_t off, wal_rec *r)
{
    if (avail < WAL_HDR)
        return 0;
    uint32_t w = load32(p), kind = w >> 28, n = w & 0x0fffffffu;
    uint64_t hash, fno;
    int64_t commit_ms;
    memcpy(&hash, p + 8, 8);
    memcpy(&fno, p + 16, 8);
    memcpy(&commit_ms, p + 24, 8);
    size_t body;
    uint64_t span;
    uint16_t mtype = 0, mflags = 0;
    if (kind == WAL_KIND_ROWS) {
        if (n == 0 || n > (avail - WAL_HDR) / WAL_ROW)
            return 0;
        body = (size_t)n * WAL_ROW;
        span = n;
    } else if (kind == WAL_KIND_MARKER) {
        if (n < WAL_MARKER_HDR || n % 4 != 0 || n > avail - WAL_HDR)
            return 0;
        body = n;
        memcpy(&mtype, p + WAL_HDR, 2);
        memcpy(&mflags, p + WAL_HDR + 2, 2);
        span = load32(p + WAL_HDR + 4);
        if (span == 0)
            return 0;
    } else {
        return 0;
    }
    if (wal_head_check(w, hash, seq, off) != load32(p + 4) || xxh64(p + 16, 16 + body, 0) != hash || fno == 0 ||
        fno >= (uint64_t)1 << 62)
        return 0;
    r->kind = kind;
    r->n = n;
    r->fno = fno;
    r->span = span;
    r->commit_ms = commit_ms;
    r->payload = p + WAL_HDR;
    r->mtype = mtype;
    r->mflags = mflags;
    return WAL_HDR + body;
}

bool wal_valid_after(const uint8_t *p, size_t len, size_t from, uint64_t seq, uint64_t base)
{
    wal_rec r;
    /* records are 4-byte aligned (32 + 20 n bytes, or 32 + a multiple of 4); the header check rejects
     * nearly every other offset without hashing rows */
    for (size_t q = (from + 4) & ~(size_t)3; q + WAL_HDR <= len; q += 4)
        if (wal_record(p + q, len - q, seq, base + q, &r))
            return true;
    return false;
}

void wal_row(const uint8_t *r, uint32_t *id, int64_t *ts, uint64_t *slot)
{
    memcpy(id, r, 4);
    memcpy(ts, r + 4, 8);
    memcpy(slot, r + 12, 8);
}

/* Appends rows to the pending WAL buffer and numbers them (*fno0 gets the first); caller holds buf_lock. Rows of
 * successive batches join one record while no marker comes between them (their numbers stay contiguous). Fails
 * without side effects. */
static int wal_buffer(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w,
                      size_t n, uint64_t *fno0)
{
    bool extend = db->wal_rec_open && (uint64_t)(load32(db->wal_buf + db->wal_rec_at) & 0x0fffffffu) + n < (1u << 28);
    size_t need = db->wal_len + (extend ? 0 : WAL_HDR) + n * WAL_ROW;
    if (need > db->wal_cap) {
        size_t cap = MAX(need, db->wal_cap * 2);
        cap = MAX(cap, (size_t)65536);
        uint8_t *b = realloc(db->wal_buf, cap);
        if (!b)
            return TSDB_ERR_NOMEM;
        db->wal_buf = b;
        db->wal_cap = cap;
    }
    if (!extend) {
        uint8_t *h = db->wal_buf + db->wal_len;
        memset(h, 0, WAL_HDR);
        store32(h, (uint32_t)WAL_KIND_ROWS << 28);
        memcpy(h + 16, &db->fno_next, 8);
        db->wal_rec_at = db->wal_len;
        db->wal_rec_open = true;
        db->wal_len += WAL_HDR;
    }
    uint8_t *h = db->wal_buf + db->wal_rec_at;
    store32(h, load32(h) + (uint32_t)n);
    *fno0 = db->fno_next;
    db->fno_next += n;
    uint8_t *p = db->wal_buf + db->wal_len;
    for (size_t i = 0; i < n; i++, p += WAL_ROW) {
        uint32_t id = series ? series[i] : cs;
        uint64_t x = slot_get(v, w, i);
        memcpy(p, &id, 4);
        memcpy(p + 4, &ts[i], 8);
        memcpy(p + 12, &x, 8);
    }
    db->wal_len = need;
    return 0;
}

/* A marker record: appended to the same buffer as the rows, in order, and made durable before it returns (markers
 * are rare). body_len is a multiple of 4 (the record is 4-byte aligned). */
int wal_marker(eng *db, uint16_t type, uint16_t flags, uint32_t span, const void *body, size_t body_len,
               uint64_t *fno)
{
    if (!db->wal_enabled || span == 0 || body_len % 4 != 0 || body_len > ((size_t)1 << 27))
        return TSDB_ERR_INVALID;
    size_t plen = WAL_MARKER_HDR + body_len;
    pthread_mutex_lock(&db->buf_lock);
    int rc = eng_check(db);
    if (rc == 0 && db->wal_len + WAL_HDR + plen > db->wal_cap) {
        size_t cap = MAX(db->wal_len + WAL_HDR + plen, db->wal_cap * 2);
        cap = MAX(cap, (size_t)65536);
        uint8_t *b = realloc(db->wal_buf, cap);
        if (!b)
            rc = TSDB_ERR_NOMEM;
        else
            db->wal_buf = b, db->wal_cap = cap;
    }
    uint64_t lsn = 0;
    if (rc == 0) {
        uint8_t *h = db->wal_buf + db->wal_len;
        memset(h, 0, WAL_HDR);
        store32(h, (uint32_t)plen | (uint32_t)WAL_KIND_MARKER << 28);
        memcpy(h + 16, &db->fno_next, 8);
        memcpy(h + WAL_HDR, &type, 2);
        memcpy(h + WAL_HDR + 2, &flags, 2);
        store32(h + WAL_HDR + 4, span);
        if (body_len)
            memcpy(h + WAL_HDR + WAL_MARKER_HDR, body, body_len);
        if (fno)
            *fno = db->fno_next;
        db->fno_next += span;
        db->wal_len += WAL_HDR + plen;
        db->wal_rec_open = false;
        lsn = ++db->wal_appended;
    }
    pthread_mutex_unlock(&db->buf_lock);
    return rc ? rc : wal_commit(db, lsn);
}

/* Takes the single WAL writer role; waits while someone else holds it. */
static void wal_writer_enter(eng *db)
{
    pthread_mutex_lock(&db->wal_sync_lock);
    while (db->wal_writing)
        pthread_cond_wait(&db->wal_cv, &db->wal_sync_lock);
    db->wal_writing = true;
    pthread_mutex_unlock(&db->wal_sync_lock);
}

static void wal_writer_leave(eng *db)
{
    pthread_mutex_lock(&db->wal_sync_lock);
    db->wal_writing = false;
    pthread_cond_broadcast(&db->wal_cv);
    pthread_mutex_unlock(&db->wal_sync_lock);
}

/* Writes a record of the rows in buf (WAL_HDR spare bytes first) to fd and
 * syncs it. On failure the engine is failed (eng_fail) before it returns:
 * the caller holds the writer role and has not woken anyone yet. */
static int wal_write_sync(eng *db, int fd, uint64_t seq, uint64_t *off, uint8_t *buf, size_t len)
{
    if (len == 0)
        return 0;
    size_t last_pos = 0;
    uint64_t last_hash = 0;
    wal_seal(buf, len, seq, *off, eng_wall_ms(), &last_pos, &last_hash);
    if (write_full(fd, buf, len) != 0) {
        eng_fail(db, "WAL write", errno);
        return TSDB_ERR_IO;
    }
    if (db->opt.fsync) {
        if (tsdb_fdatasync(fd) != 0) {
            eng_fail(db, "WAL fdatasync", errno);
            return TSDB_ERR_IO;
        }
        atomic_fetch_add_explicit(&db->st_wal_syncs, 1, memory_order_relaxed);
    }
    atomic_fetch_add_explicit(&db->st_wal_bytes, len, memory_order_relaxed);
    /* the last record of the write: what the revalidation reads again */
    db->wal_last_seq = seq, db->wal_last_off = *off + last_pos, db->wal_last_len = len - last_pos;
    db->wal_last_hash = last_hash;
    *off += len;
    return 0;
}

/* Makes every row appended up to `target` durable: group commit. Returns 0
 * only if they are; TSDB_ERR_IO once the engine has failed, including to
 * waiters whose rows were in a batch whose write or sync failed. */
int wal_commit(eng *db, uint64_t target)
{
    if (!db->wal_enabled || atomic_load(&db->wal_durable) >= target)
        return 0;
    int rc = 0;
    pthread_mutex_lock(&db->wal_sync_lock);
    while (rc == 0 && atomic_load(&db->wal_durable) < target) {
        /* set before the failing writer gave up the role, so seen here first */
        if ((rc = eng_check(db)) != 0)
            break;
        if (db->wal_writing) {
            /* another writer is syncing; it, or the next one, will cover us */
            pthread_cond_wait(&db->wal_cv, &db->wal_sync_lock);
            continue;
        }
        db->wal_writing = true;
        pthread_mutex_unlock(&db->wal_sync_lock);

        /* take everything appended so far; appenders continue into the spare buffer */
        pthread_mutex_lock(&db->buf_lock);
        uint8_t *buf = db->wal_buf;
        size_t cap = db->wal_cap, len = db->wal_len;
        uint64_t upto = db->wal_appended;
        int fd = db->wal_fd;
        uint64_t seq = db->wal_seq;
        db->wal_buf = db->wal_spare;
        db->wal_cap = db->wal_spare_cap;
        db->wal_len = 0;
        db->wal_rec_open = false;
        pthread_mutex_unlock(&db->buf_lock);

        rc = wal_write_sync(db, fd, seq, &db->wal_off, buf, len);
        db->wal_spare = buf;
        db->wal_spare_cap = cap;
        if (rc == 0)
            atomic_store(&db->wal_durable, upto);

        pthread_mutex_lock(&db->wal_sync_lock);
        db->wal_writing = false;
        pthread_cond_broadcast(&db->wal_cv);
    }
    pthread_mutex_unlock(&db->wal_sync_lock);
    return rc;
}

/* ---------------------------------------------------------------- writes */

/* The keys of a large insert (internal.h, lw_fence), in one pass over the rows. Rows of a batch come in any order
 * of series (time-major from collectors, series-major from loads): a block of LW_BLOCK rows of one series is one
 * update; in a mixed block each of LW_LANES rows in turn updates a table of its own, without branches, so rows of
 * one series close together never wait for each other's store. A batch of one series keeps one range and no table.
 * 10 million rows of 1000 series (Ryzen 9 7950X): 8 ms time-major or in random order, 2 ms series-major; a branch
 * per run of one series took 22 ms time-major, beside about 240 ms for the insert itself. */
#define LW_LANES 4
#define LW_BLOCK 64
int lw_fence_rows(lw_fence *f, const uint32_t *series, uint32_t cs, const int64_t *ts, size_t n)
{
    memset(f, 0, sizeof *f);
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    if (!series) {
        for (size_t i = 0; i < n; i++) {
            lo = ts[i] < lo ? ts[i] : lo;
            hi = ts[i] > hi ? ts[i] : hi;
        }
        f->series = cs, f->lo = lo, f->hi = hi;
        return 0;
    }
    /* per lane and bucket: lo, hi; the thread's grow-only buffer (a malloc of 256 KB is an mmap and 64 page faults
     * per insert); an insert nested in another fence's window allocates its own */
    const size_t bytes = LW_LANES * 2 * LW_BUCKETS * sizeof(int64_t);
    int64_t *r = lw_table_take(bytes);
    if (!r && (r = malloc(bytes)) != NULL)
        f->r_heap = true;
    if (!r)
        return TSDB_ERR_NOMEM;
    for (size_t b = 0; b < LW_LANES * LW_BUCKETS; b++)
        r[2 * b] = INT64_MAX, r[2 * b + 1] = INT64_MIN;
    uint32_t s0 = n ? series[0] : 0, diff = 0;
    size_t i = 0;
    for (; i + LW_BLOCK <= n; i += LW_BLOCK) {
        uint32_t s = series[i], d = 0;
        for (size_t k = 0; k < LW_BLOCK; k++)
            d |= series[i + k] ^ s;
        diff |= d | (s ^ s0);
        if (d == 0) { /* a block of one series: one update */
            int64_t blo = ts[i], bhi = ts[i], *q = r + 2 * (s % LW_BUCKETS);
            for (size_t k = 1; k < LW_BLOCK; k++) {
                blo = ts[i + k] < blo ? ts[i + k] : blo;
                bhi = ts[i + k] > bhi ? ts[i + k] : bhi;
            }
            q[0] = MIN(q[0], blo);
            q[1] = MAX(q[1], bhi);
            continue;
        }
        for (size_t k = 0; k < LW_BLOCK; k += LW_LANES)
            for (size_t l = 0; l < LW_LANES; l++) {
                uint32_t sl = series[i + k + l];
                int64_t t = ts[i + k + l], *q = r + 2 * (l * LW_BUCKETS + sl % LW_BUCKETS);
                q[0] = t < q[0] ? t : q[0];
                q[1] = t > q[1] ? t : q[1];
            }
    }
    for (; i < n; i++) {
        int64_t t = ts[i], *q = r + 2 * (series[i] % LW_BUCKETS);
        diff |= series[i] ^ s0;
        q[0] = t < q[0] ? t : q[0];
        q[1] = t > q[1] ? t : q[1];
    }
    for (size_t b = 0; b < LW_BUCKETS; b++) {
        for (size_t l = 1; l < LW_LANES; l++) {
            const int64_t *q = r + 2 * (l * LW_BUCKETS + b);
            r[2 * b] = MIN(r[2 * b], q[0]);
            r[2 * b + 1] = MAX(r[2 * b + 1], q[1]);
        }
        lo = MIN(lo, r[2 * b]);
        hi = MAX(hi, r[2 * b + 1]);
    }
    f->lo = lo, f->hi = hi, f->series = s0;
    if (diff == 0) { /* one series */
        if (f->r_heap)
            free(r);
        else
            lw_table_put();
        f->r_heap = false;
        return 0;
    }
    f->nb = LW_BUCKETS;
    f->r = r;
    return 0;
}

void lw_fence_range(lw_fence *f, uint32_t series, int64_t start, int64_t end)
{
    memset(f, 0, sizeof *f);
    f->series = series;
    f->lo = start;
    f->hi = end - 1; /* start < end */
}

bool lw_enter(eng *db, lw_fence *f)
{
    pthread_mutex_lock(&db->lock); /* nflushing changes under db->lock, the active memtable under both */
    pthread_cond_init(&f->cv, NULL);
    pthread_mutex_lock(&db->buf_lock);
    f->next = db->lw_head;
    db->lw_head = f;
    bool pending = db->active->rows > 0 || db->nflushing > 0;
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    return pending;
}

void lw_leave(eng *db, lw_fence *f)
{
    pthread_mutex_lock(&db->buf_lock);
    for (lw_fence **p = &db->lw_head; *p; p = &(*p)->next)
        if (*p == f) {
            *p = f->next;
            break;
        }
    pthread_cond_broadcast(&f->cv); /* only the writes this fence held; they look at the others again */
    bool held = f->held;
    pthread_mutex_unlock(&db->buf_lock);
    if (held) /* this thread no longer holds up anyone; to the hook that got the +1 (with cls_rw read-held) */
        f->hold_fn(f->hold_ctx, -1);
    /* waits until the woken waiters are out of the cv (glibc), so the caller may free f */
    pthread_cond_destroy(&f->cv);
    if (f->r_heap)
        free(f->r);
    else if (f->r)
        lw_table_put(); /* no one reads it any more: unlinked under buf_lock */
    f->r = NULL;
    f->r_heap = false;
}

/* the first registered fence a row of the batch falls inside, else NULL; buf_lock held */
static lw_fence *lw_overlaps(const eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, size_t n)
{
    for (lw_fence *f = db->lw_head; f; f = f->next) {
        const int64_t lo = f->lo, hi = f->hi, *r = f->r;
        if (f->nb == 0) {
            for (size_t i = 0; i < n; i++)
                if (ts[i] >= lo && ts[i] <= hi && (series ? series[i] : cs) == f->series)
                    return f;
            continue;
        }
        for (size_t i = 0; i < n; i++) {
            if (ts[i] < lo || ts[i] > hi)
                continue;
            const int64_t *q = r + 2 * ((series ? series[i] : cs) & (LW_BUCKETS - 1));
            if (ts[i] >= q[0] && ts[i] <= q[1])
                return f;
        }
    }
    return NULL;
}

__thread const volatile int *cur_abandon;

int mem_insert(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n)
{
    pthread_mutex_lock(&db->buf_lock);
    /* after the large write's publish this batch is the newer one, and wins as a memtable row (lw_fence) */
    bool announced = false;
    void (*hook)(void *, int) = NULL; /* the hook this wait counts with, from its first +1 to its -1 */
    void *hook_ctx = NULL;
    for (lw_fence *f; db->lw_head && (f = lw_overlaps(db, series, cs, ts, n)) != NULL;) {
        if (!announced)
            hook = db->wait_fn, hook_ctx = db->wait_ctx;
        if (hook && (!announced || !f->held)) {
            /* This thread is held, and so, for the server, is the large write's: it frees its thread only after
             * its publish, however long the write takes. Counted once per large write, given back by its
             * lw_leave to the same hook. Not under buf_lock: tsdbd may start a thread. */
            int d = !announced + !f->held;
            if (!f->held)
                f->held = true, f->hold_fn = hook, f->hold_ctx = hook_ctx;
            announced = true;
            pthread_mutex_unlock(&db->buf_lock);
            for (int k = 0; k < d; k++)
                hook(hook_ctx, 1);
            pthread_mutex_lock(&db->buf_lock);
            continue; /* f may have left meanwhile */
        }
        pthread_cond_wait(&f->cv, &db->buf_lock);
        db->lw_wakeups++;
    }
    /* A failure while this write waited (the large write's own publish, say) must refuse it: in async WAL mode
     * nothing after this point checks, and the write would be acknowledged by a failed engine. */
    int rc = eng_check(db);
    if (rc == 0) {
        rc = db->bg_error;
        db->bg_error = 0;
    }
    /* before the WAL append, never after: an abandoned batch in the WAL would replay after a crash, and a refused
     * write would become visible. After the fence wait, so a request parked behind a large write sees the flag. */
    if (rc == 0)
        rc = eng_abandoned();
    /* wal_appended counts rows the WAL holds: a refusal before the append adds none, or a flush would wait for
     * rows that never come (wal_durable never reaches them) */
    bool counted = rc == 0;
    uint64_t fno0 = 0;
    if (rc == 0 && db->wal_enabled) {
        rc = wal_buffer(db, series, cs, ts, v, w, n, &fno0); /* first: it can fail without side effects */
        counted = rc == 0;
    }
    if (rc == 0)
        rc = mt_add(db->active, series, cs, ts, v, w, n, db->wal_appended + 1);
    if (rc == 0 && fno0)
        mt_note_fno(db->active, fno0, n);
    if (counted)
        db->wal_appended += n;
    uint64_t lsn = db->wal_appended;
    size_t rows = db->active->rows;
    if (rows >= db->opt.buffer_rows)
        pthread_cond_signal(&db->flush_cv);
    pthread_mutex_unlock(&db->buf_lock);
    if (announced)
        hook(hook_ctx, -1);
    if (rc == 0 && db->wal_enabled && db->opt.wal_sync_ms == 0)
        rc = wal_commit(db, lsn);
    /* backpressure when the flusher falls behind */
    if (rc == 0 && rows >= 2 * db->opt.buffer_rows)
        rc = mt_flush(db);
    return rc;
}

/* Unlinks the WAL segments up to seq, now in parts, and moves wal_removed on over those that are gone. One whose
 * unlink fails (an I/O error, say) stays, and so does every later one, to be tried again at the next flush:
 * wal_removed must not pass a segment that a reopen would replay (its empty parts and the switches' bound in
 * classes.c rely on it). Called with flush_lock held. */
static void wal_unlink_upto(eng *db, uint64_t seq)
{
    if (db->wal_enabled) {
        for (uint64_t s = db->wal_unlink_next ? db->wal_unlink_next : seq; s <= seq; s++) {
            char name[64];
            wal_name(name, sizeof name, s);
            if (unlinkat(db->dirfd, name, 0) != 0 && errno != ENOENT) {
                if (db->wal_unlink_next != s)
                    fprintf(stderr, "tsdb: removing %s failed (%s): it stays until a later flush removes it\n", name,
                            strerror(errno));
                db->wal_unlink_next = s;
                return;
            }
        }
        db->wal_unlink_next = 0;
    }
    pthread_mutex_lock(&db->lock);
    db->wal_removed = MAX(db->wal_removed, seq);
    pthread_mutex_unlock(&db->lock);
}

/* Writes every memtable waiting to be flushed, oldest first; one whose write
 * failed stays in the list to be retried. Called with flush_lock held. */
static int flush_waiting(eng *db)
{
    int rc = 0;
    for (size_t i = 0; i < db->nflushing && rc == 0;) {
        memtable *m = db->flushing[i]; /* only flush_lock holders change the list's members */
        part_list pl = {0};
        if ((rc = eng_check(db)) != 0) /* e.g. the MANIFEST rewrite in mt_flush failed */
            break;
        cls_freeze(db); /* one history from placing its rows until they are listed (classes.c) */
        rc = mt_write(db, m, &pl);
        struct cls_wtail *wt = rc == 0 && db->cls_on ? cls_wal_tail_new(m) : NULL;
        if (rc == 0) {
            pthread_mutex_lock(&db->publish_lock); /* numbered, named and listed in one step */
            rc = parts_commit(db, pl.p, pl.n);
            if (rc)
                pthread_mutex_unlock(&db->publish_lock);
        }
        if (rc) {
            cls_thaw(db);
            free(wt);
            parts_discard(db, pl.p, pl.n);
            free(pl.p);
            break;
        }
        pthread_mutex_lock(&db->lock);
        for (size_t k = 0; k < pl.n; k++)
            db_parts_insert_locked(db, pl.p[k]);
        memmove(db->flushing + i, db->flushing + i + 1, (db->nflushing - i - 1) * sizeof *db->flushing);
        db->nflushing--;
        db->wal_flushed = MAX(db->wal_flushed, m->wal_seq); /* flushes go in segment order */
        if (db->cls_on)
            cls_wal_tail_locked(db, wt, m->wal_seq, m->max_ts);
        pthread_cond_signal(&db->merge_cv);
        pthread_mutex_unlock(&db->lock);
        pthread_mutex_unlock(&db->publish_lock);
        cls_thaw(db);
        int mrc = manifest_publish(db); /* fails the engine on error */
        free(pl.p);
        if (mrc == 0) /* the numbers of the segment stay used after it is gone (fno.c) */
            mrc = fno_require(db, m->fno_end);
        if (mrc == 0)
            wal_unlink_upto(db, m->wal_seq); /* else MANIFEST may still name it: a follower would tail it */
        if (mrc == 0)
            mrc = cls_flush_guard(db, m); /* a series of a long class that got fast moves on now */
        mt_unref(m);
        rc = mrc;
    }
    return rc;
}

/* Turns the active memtable into parts. Rows stay visible throughout: the
 * memtable is moved to the flushing list and only leaves it together with
 * the publication of its parts. */
int mt_flush(eng *db)
{
    if (db->opt.read_only)
        return 0;
    pthread_mutex_lock(&db->flush_lock);
    /* a memtable whose write failed earlier holds older rows than the active
     * one: it is written first, and the active one after it all the same, or
     * a large insert that relies on this flush would be shadowed by it */
    int rc = flush_waiting(db);
    wal_writer_enter(db); /* no commit may write to the segment being retired */
    pthread_mutex_lock(&db->buf_lock);
    bool empty = db->active->rows == 0;
    pthread_mutex_unlock(&db->buf_lock);
    if (rc == 0)
        rc = eng_check(db); /* rows of a failed engine may not be durable: none become parts */
    if (rc == 0 && !empty && db->nflushing == 0) {
        uint64_t seq = db->wal_seq + 1;
        int newfd = -1;
        memtable *nm = mt_new(seq);
        if (!nm)
            rc = TSDB_ERR_NOMEM;
        else if (db->wal_enabled)
            rc = wal_open_segment(db, seq, &newfd);
        if (rc) {
            mt_unref(nm);
        } else {
            pthread_mutex_lock(&db->lock);
            pthread_mutex_lock(&db->buf_lock);
            memtable *m = db->active;
            db->active = nm;
            uint8_t *buf = db->wal_buf;
            size_t len = db->wal_len;
            uint64_t upto = db->wal_appended;
            int oldfd = db->wal_fd;
            uint64_t oldseq = db->wal_seq, oldoff = db->wal_off;
            db->wal_off = 0;
            db->wal_buf = NULL;
            db->wal_cap = db->wal_len = 0;
            db->wal_rec_open = false;
            m->fno_end = db->fno_next; /* the end of the old segment: FNO holds it before the segment goes */
            db->wal_fd = newfd;
            db->wal_seq = seq;
            db->flushing[db->nflushing++] = m;
            pthread_mutex_unlock(&db->buf_lock);
            pthread_mutex_unlock(&db->lock);
            if (db->wal_enabled) {
                /* the rest of the old segment; a part will hold these rows, but
                 * until then the segment is what makes them durable */
                rc = wal_write_sync(db, oldfd, oldseq, &oldoff, buf, len);
                close(oldfd);
                if (rc == 0)
                    atomic_store(&db->wal_durable, upto);
            }
            free(buf);
            manifest_publish(db); /* followers start tailing the new segment */
        }
    }
    wal_writer_leave(db);

    if (rc == 0)
        rc = flush_waiting(db);
    pthread_mutex_unlock(&db->flush_lock);
    return rc;
}

/* ------------------------------------------------------- background work */

void *flusher_main(void *arg)
{
    eng *db = arg;
    tsdb_thread_name("tsdb-flush");
    pthread_mutex_lock(&db->buf_lock);
    while (!db->flusher_stop) {
        int64_t interval = db->opt.flush_interval_ms > 0 ? db->opt.flush_interval_ms : 1000;
        int64_t due = db->active->rows ? db->active->created_ms + interval : mono_ms() + interval;
        if (db->opt.wal_sync_ms > 0)
            due = MIN(due, db->last_wal_sync_ms + db->opt.wal_sync_ms);
        int64_t wait = MAX(due - mono_ms(), (int64_t)1);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += wait / 1000;
        ts.tv_nsec += (wait % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&db->flush_cv, &db->buf_lock, &ts);
        if (db->flusher_stop)
            break;
        int64_t now = mono_ms();
        bool flush = db->active->rows &&
                     (db->active->rows >= db->opt.buffer_rows || now - db->active->created_ms >= interval);
        bool sync = db->opt.wal_sync_ms > 0 && now - db->last_wal_sync_ms >= db->opt.wal_sync_ms;
        uint64_t target = db->wal_appended;
        pthread_mutex_unlock(&db->buf_lock);
        int rc = eng_check(db); /* a failed engine writes nothing more */
        if (rc == 0 && sync) {
            rc = wal_commit(db, target);
            db->last_wal_sync_ms = now;
        }
        if (rc == 0 && flush)
            rc = mt_flush(db);
        if (rc == 0)
            graveyard_purge(db, false);
        pthread_mutex_lock(&db->buf_lock);
        if (rc && !db->bg_error)
            db->bg_error = rc;
    }
    pthread_mutex_unlock(&db->buf_lock);
    return NULL;
}

/* ----------------------------------------------------------------- open */

typedef struct seg {
    uint64_t seq;
    uint64_t fno_first, fno_end; /* the first number of its first valid record (0: none) and where the numbers end */
    size_t end; /* a writer's open: where the valid records end, as wal_check_segment found */
    size_t len; /* and the segment's size */
    char name[256];
} seg;

static int cmp_seg(const void *a, const void *b)
{
    uint64_t x = ((const seg *)a)->seq, y = ((const seg *)b)->seq;
    return (x > y) - (x < y);
}

/* Highest WAL sequence already stored in a partition's parts. Caller holds no locks; open is single threaded. */
static uint64_t partition_wal_seq(eng *db, int64_t partition)
{
    uint64_t best = 0;
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == partition)
            best = MAX(best, db->parts[i]->wal_seq);
    return best;
}

static int read_file(int dirfd, const char *name, uint8_t **out, size_t *len)
{
    int fd = openat(dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    *len = (size_t)st.st_size;
    *out = malloc(*len ? *len : 1);
    size_t got = 0;
    int rc = *out ? 0 : TSDB_ERR_NOMEM;
    while (rc == 0 && got < *len) {
        ssize_t r = pread(fd, *out + got, *len - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            /* an error, or a file that shrank under us: never taken for its end */
            fprintf(stderr, "tsdb: reading %s at offset %zu: %s\n", name, got, r < 0 ? strerror(errno) : "short read");
            rc = TSDB_ERR_IO;
        } else {
            got += (size_t)r;
        }
    }
    close(fd);
    if (rc) {
        free(*out);
        *out = NULL;
    }
    return rc;
}

/* A writer's open checks every segment before it replays any: the replay writes parts and unlinks each segment
 * (with its torn tail), so a refusal found in a later segment would come after the earlier ones were changed. The
 * checks are the replay's own: records out of order, a row of a series SERIES lacks, a tail that reads otherwise
 * the second time. s->end is where the valid records end. */
static int wal_check_segment(eng *db, seg *s)
{
    uint8_t *buf;
    size_t len, pos = 0, rec;
    wal_rec wr;
    int rc = read_file(db->dirfd, s->name, &buf, &len);
    if (rc)
        return rc;
    uint32_t count = eng_series_count(db);
    s->fno_first = s->fno_end = 0;
    while (rc == 0 && (rec = wal_record(buf + pos, len - pos, s->seq, pos, &wr)) != 0) {
        const uint8_t *r = wr.payload;
        size_t n = wr.kind == WAL_KIND_ROWS ? wr.n : 0; /* a marker holds no rows */
        if (wr.fno < s->fno_end) { /* numbers never go back: a valid record that does is damage, not a torn tail */
            fprintf(stderr, "tsdb: WAL segment %s: the record at offset %zu has number %" PRIu64 ", below %" PRIu64
                            ", where the one before it ends: the log is damaged\n", s->name, pos, wr.fno, s->fno_end);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (!s->fno_first)
            s->fno_first = wr.fno;
        s->fno_end = wr.fno + wr.span;
        for (size_t i = 0; i < n; i++, r += WAL_ROW) {
            uint32_t id;
            int64_t ts;
            uint64_t slot;
            wal_row(r, &id, &ts, &slot);
            if (id >= count) {
                fprintf(stderr,
                        "tsdb: WAL segment %s names series %" PRIu32 " in the record at offset %zu, but SERIES has "
                        "only %" PRIu32 ": SERIES is damaged. Nothing was changed: restore SERIES from a backup or "
                        "snapshot, or move the segment out of the directory to accept the loss of its rows; tsdb "
                        "export cannot read the database either until then\n",
                        s->name, id, pos, count);
                rc = TSDB_ERR_CORRUPT;
                break;
            }
        }
        pos += rec;
    }
    if (rc == 0 && pos < len && wal_valid_after(buf, len, pos, s->seq, 0)) {
        fprintf(stderr,
                "tsdb: WAL segment %s is damaged at offset %zu, before valid records: the open fails and the "
                "segment is kept (move it out of the directory to accept the loss of its rows)\n",
                s->name, pos);
        rc = TSDB_ERR_CORRUPT;
    } else if (rc == 0 && pos < len) {
        /* a torn append: never acknowledged, unless the last record was damaged at rest, which looks the same */
        rc = tail_confirm(db->dirfd, s->name, buf + pos, pos, len - pos);
    }
    s->end = pos;
    s->len = len;
    free(buf);
    return rc;
}

/* A torn tail is a torn append only in the newest segment. Every write to a segment holds the WAL writer role and is
 * followed by its fdatasync before the role is given up (wal_write_sync, from wal_commit in either wal_sync_ms mode
 * and from mt_flush); the rotation (mt_flush) holds the role from before the new segment takes rows until the rest of
 * the old one is written and synced; and after a failed write or sync the engine fails, so no later segment gets a
 * record. So once segment k+1 holds a record, all of k was durable: a power loss cannot leave k torn, and a torn end
 * of k with records after it is damage at rest to acknowledged rows, unless parts hold k's rows already (flushes go in
 * segment order and give their parts the segment's number; large inserts give 0). With fsync = 0 nothing above is
 * synced, and a power loss looks like this damage, as it does elsewhere: the open fails. */
static int wal_check_order(const seg *segs, size_t nseg, uint64_t parts_seq)
{
    for (size_t k = 0; k + 1 < nseg; k++) {
        if (segs[k].end == segs[k].len || parts_seq >= segs[k].seq)
            continue;
        for (size_t j = k + 1; j < nseg; j++)
            if (segs[j].end > 0) {
                fprintf(stderr,
                        "tsdb: WAL segment %s ends in %zu bytes that are not a record at offset %zu, but %s after it "
                        "holds records: a power loss cannot do that (each segment is synced before the next takes "
                        "rows), so acknowledged rows are damaged. Nothing was changed: restore the segment from a "
                        "backup or snapshot, or move it out of the directory to accept the loss of its rows\n",
                        segs[k].name, segs[k].len - segs[k].end, segs[k].end, segs[j].name);
                return TSDB_ERR_CORRUPT;
            }
    }
    return 0;
}

/* Exactly what wal_name writes (16 lowercase hex digits): a user's wal_x.log in a volume directory is neither
 * replayed (and cut at a "torn tail") nor deleted. */
bool wal_file_name(const char *name)
{
    return strlen(name) == 24 && strncmp(name, "wal_", 4) == 0 && strspn(name + 4, "0123456789abcdef") == 16 &&
           strcmp(name + 20, ".log") == 0;
}

/* Replays WAL segments left by a previous run, then opens a fresh segment. A writer checks every segment first and
 * then calls before_replay (the open's cuts of torn tails), so nothing is written or cut until all checks passed. */
/* A writer's open: the next number is the largest of FNO's valid slot, the end of every valid record of every segment
 * and 1 + the largest part range (design-cdc.md 2.1). Numbers also go up from segment to segment. */
static int wal_open_numbers(eng *db, const seg *segs, size_t nseg)
{
    uint64_t next = 1, prev_end = 0;
    for (size_t k = 0; k < nseg; k++) {
        if (segs[k].fno_first && segs[k].fno_first < prev_end) {
            fprintf(stderr, "tsdb: WAL segment %s starts at number %" PRIu64 ", below %" PRIu64
                            ", where the segment before it ends: the log is damaged\n",
                    segs[k].name, segs[k].fno_first, prev_end);
            return TSDB_ERR_CORRUPT;
        }
        if (segs[k].fno_end)
            prev_end = segs[k].fno_end;
        next = MAX(next, segs[k].fno_end);
    }
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->fno_hi)
            next = MAX(next, db->parts[i]->fno_hi + 1);
    uint64_t fno, gen;
    int slot, rc;
    bool torn;
    if ((rc = fno_load(db, &fno, &gen, &slot, &torn)) == TSDB_ERR_NOTFOUND) {
        fprintf(stderr, "tsdb: FNO is missing: the numbers of the log cannot be bounded, the open fails (a database "
                        "of this format has one in every volume; restore it from a backup)\n");
        return TSDB_ERR_CORRUPT;
    }
    if (rc)
        return rc;
    if (torn)
        fprintf(stderr, "tsdb: FNO: one slot is torn (a store the power loss cut): the other is used\n");
    db->fno_slot = slot;
    db->fno_gen = gen;
    atomic_store(&db->fno_durable, fno);
    db->fno_next = MAX(next, fno);
    return 0;
}

int wal_open(eng *db, int (*before_replay)(eng *))
{
    bool ro = db->opt.read_only;
    seg *segs = NULL;
    size_t nseg = 0, cap = 0;
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d); /* the dup shares its offset with dirfd, which an earlier scan moved */
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) { /* NULL with errno set is a read error, not the end */
        uint64_t seq;
        int consumed = 0;
        if (!wal_file_name(e->d_name) || sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &seq, &consumed) != 1 ||
            e->d_name[consumed])
            continue;
        if (nseg == cap) {
            cap = cap ? cap * 2 : 16;
            seg *s = realloc(segs, cap * sizeof *s);
            if (!s) {
                closedir(d);
                free(segs);
                return TSDB_ERR_NOMEM;
            }
            segs = s;
        }
        segs[nseg].seq = seq;
        snprintf(segs[nseg].name, sizeof segs[nseg].name, "%s", e->d_name);
        nseg++;
    }
    if (errno != 0) {
        fprintf(stderr, "tsdb: reading the directory failed: %s: the open fails\n", strerror(errno));
        closedir(d);
        free(segs);
        return TSDB_ERR_IO;
    }
    closedir(d);
    if (nseg)
        qsort(segs, nseg, sizeof *segs, cmp_seg);

    uint64_t max_seq = 0;
    for (size_t i = 0; i < db->nparts; i++)
        max_seq = MAX(max_seq, db->parts[i]->wal_seq);
    if (nseg)
        max_seq = MAX(max_seq, segs[nseg - 1].seq);
    db->wal_seq = max_seq + 1;
    db->active = mt_new(db->wal_seq);
    if (!db->active) {
        free(segs);
        return TSDB_ERR_NOMEM;
    }

    int rc = 0;
    uint64_t parts_seq = 0;
    for (size_t i = 0; i < db->nparts; i++)
        parts_seq = MAX(parts_seq, db->parts[i]->wal_seq);
    for (size_t k = 0; !ro && k < nseg && rc == 0; k++)
        rc = wal_check_segment(db, &segs[k]);
    if (rc == 0 && !ro)
        rc = wal_check_order(segs, nseg, parts_seq);
    if (rc == 0 && !ro)
        rc = wal_open_numbers(db, segs, nseg);
    if (rc == 0 && before_replay)
        rc = before_replay(db);
    for (size_t k = 0; k < nseg && rc == 0; k++) {
        uint8_t *buf;
        size_t len;
        if ((rc = read_file(db->dirfd, segs[k].name, &buf, &len)) != 0)
            break;
        /* the segment's rows go into a memtable of their own (the active one
         * for a reader), which a writer turns into parts in one commit */
        memtable *m = ro ? db->active : mt_new(segs[k].seq);
        if (!m) {
            free(buf);
            rc = TSDB_ERR_NOMEM;
            break;
        }
        size_t pos = 0, rec;
        wal_rec wr;
        int64_t cached_p = 0;
        uint64_t cached_seq = 0;
        bool have_cache = false, reloaded = false;
        while (rc == 0 && (rec = wal_record(buf + pos, len - pos, segs[k].seq, pos, &wr)) != 0) {
            const uint8_t *r = wr.payload;
            size_t n = wr.kind == WAL_KIND_ROWS ? wr.n : 0; /* a marker is skipped; its numbers are counted */
            for (size_t i = 0; i < n && rc == 0; i++, r += WAL_ROW) {
                uint32_t id;
                int64_t ts;
                uint64_t slot;
                wal_row(r, &id, &ts, &slot);
                if (id >= eng_series_count(db) && ro && !reloaded) {
                    /* a reader read SERIES before this segment: a writer may have created the series since */
                    rc = series_reload(&db->series, db->dirfd);
                    reloaded = true;
                }
                if (rc == 0 && id >= eng_series_count(db)) {
                    /* A series' record is synced before its id is handed out, so a WAL row of an id SERIES lacks
                     * means SERIES lost records: cut as a torn tail at this open (damage to the last records
                     * looks the same) or overwritten. Its rows are not dropped as those of a series never made. */
                    fprintf(stderr,
                            "tsdb: WAL segment %s names series %" PRIu32 " in the record at offset %zu, but SERIES "
                            "has only %" PRIu32 ": SERIES is damaged (restore it from a backup or snapshot)\n",
                            segs[k].name, id, pos, eng_series_count(db));
                    rc = TSDB_ERR_CORRUPT;
                    break;
                }
                if (rc)
                    break;
                int64_t p = partition_of(db, id, ts);
                if (!have_cache || p != cached_p) {
                    cached_p = p;
                    cached_seq = partition_wal_seq(db, p);
                    have_cache = true;
                }
                if (cached_seq >= segs[k].seq)
                    continue; /* already in a part */
                rc = mt_add(m, NULL, id, &ts, &slot, 8, 1, 0);
                if (rc == 0)
                    mt_note_fno(m, wr.fno + i, 1);
            }
            if (rc)
                break;
            pos += rec;
        }
        if (rc == 0 && pos < len && wal_valid_after(buf, len, pos, segs[k].seq, 0)) {
            fprintf(stderr,
                    "tsdb: WAL segment %s is damaged at offset %zu, before valid records: the open fails and the "
                    "segment is kept (move it out of the directory to accept the loss of its rows)\n",
                    segs[k].name, pos);
            rc = TSDB_ERR_CORRUPT;
        } else if (rc == 0 && !ro && pos != segs[k].end) {
            fprintf(stderr, "tsdb: %s: two reads at the open end its valid records at offsets %zu and %zu: the open "
                            "fails\n", segs[k].name, segs[k].end, pos);
            rc = TSDB_ERR_IO;
        } else if (rc == 0 && pos < len && !ro) {
            /* the torn tail wal_check_segment confirmed: it goes with the segment, unlinked below */
            fprintf(stderr, "tsdb: %s: cutting a torn tail of %zu bytes at offset %zu\n", segs[k].name, len - pos,
                    pos);
        }
        free(buf);
        if (rc == 0 && !ro) {
            part_list pl = {0};
            rc = mt_write(db, m, &pl);
            if (rc == 0)
                rc = parts_publish(db, pl.p, pl.n);
            if (rc)
                parts_discard(db, pl.p, pl.n);
            free(pl.p);
        }
        if (!ro)
            mt_unref(m);
        /* the numbers of the segment stay used once it is gone, markers included: FNO holds them first */
        if (rc == 0 && !ro && segs[k].fno_end)
            rc = fno_require(db, segs[k].fno_end);
        if (rc == 0 && !ro)
            unlinkat(db->dirfd, segs[k].name, 0);
    }
    free(segs);
    if (rc == 0 && !ro && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO; /* at open: the open fails */
    if (rc == 0 && db->wal_enabled)
        rc = wal_open_segment(db, db->wal_seq, &db->wal_fd);
    return rc;
}

/* Called by close after the final flush. */
void wal_close(eng *db)
{
    if (db->wal_fd >= 0) {
        close(db->wal_fd);
        db->wal_fd = -1;
        if (db->active && db->active->rows == 0 && db->nflushing == 0 && !eng_check(db) && !db->retiring) {
            /* a clean close holds the exact next number; an empty segment carries none, so FNO must before it goes */
            if (fno_require(db, db->fno_next) == 0) {
                char name[64];
                wal_name(name, sizeof name, db->wal_seq);
                unlinkat(db->dirfd, name, 0);
            }
        }
    }
    if (db->wal_enabled && !eng_check(db) && !db->retiring)
        fno_require(db, db->fno_next); /* a failure fails the engine; the segments stay and the open finds the end */
    for (size_t i = 0; i < db->nflushing; i++)
        mt_unref(db->flushing[i]);
    db->nflushing = 0;
    mt_unref(db->active);
    db->active = NULL;
    free(db->wal_buf);
    free(db->wal_spare);
    db->wal_buf = db->wal_spare = NULL;
}

/* ------------------------------------------- recovery of a halted engine */

/* Reads [off, off + len) of a file through O_DIRECT where the file system has it (the client's page cache and the
 * server's cache then cannot answer for what is on disk), else through a fresh open. 0, 1 if the file is shorter,
 * <0 on an error (nothing about the file is known then). */
static int read_fresh(int dirfd, const char *name, uint64_t off, size_t len, uint8_t *out)
{
    const size_t A = 4096;
    int fd = openat(dirfd, name, O_RDONLY | O_DIRECT | O_CLOEXEC);
    bool direct = fd >= 0;
    if (fd < 0 && errno == EINVAL)
        fd = openat(dirfd, name, O_RDONLY | O_CLOEXEC); /* tmpfs: the cache is all there is */
    if (fd < 0)
        return errno == ENOENT ? 2 : TSDB_ERR_IO;
    uint64_t from = off & ~(uint64_t)(A - 1);
    size_t span = (size_t)((off + len - from + A - 1) & ~(uint64_t)(A - 1));
    void *buf = NULL;
    int rc = 0;
    if (posix_memalign(&buf, A, span) != 0) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    size_t got = 0;
    while (got < span) {
        ssize_t r = pread(fd, (uint8_t *)buf + got, span - got, (off_t)(from + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0 && errno == EINVAL && direct) { /* O_DIRECT refused at the read: the same through the cache */
            close(fd);
            fd = openat(dirfd, name, O_RDONLY | O_CLOEXEC);
            direct = false;
            if (fd < 0) {
                rc = TSDB_ERR_IO;
                goto out;
            }
            got = 0;
            continue;
        }
        if (r < 0) {
            rc = TSDB_ERR_IO;
            goto out;
        }
        if (r == 0)
            break;
        got += (size_t)r;
    }
    if (from + got < off + len)
        rc = 1;
    else
        memcpy(out, (uint8_t *)buf + (off - from), len);
out:
    free(buf);
    if (fd >= 0)
        close(fd);
    return rc;
}

/* The last record that was durable (wal_last_*) must be on the storage as the engine wrote it: same bytes, same
 * place. A server that lost its newest state (a rolled-back pool, an export restored from a snapshot) has a shorter
 * segment or other bytes there, and every row acknowledged after that point is gone from the log. The engine is
 * halted: nothing writes. A segment that is gone is fine only if the engine itself removed it. */
int wal_verify_tail(eng *db, const char *dir)
{
    if (!db->wal_enabled || db->wal_last_len == 0)
        return 0;
    int dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return TSDB_ERR_IO;
    char name[64];
    wal_name(name, sizeof name, db->wal_last_seq);
    size_t len = (size_t)db->wal_last_len;
    uint8_t *rec = malloc(len);
    int rc = rec ? read_fresh(dirfd, name, db->wal_last_off, len, rec) : TSDB_ERR_NOMEM;
    close(dirfd);
    if (rc == 2) /* ENOENT */
        rc = db->wal_last_seq <= atomic_load(&db->wal_removed) ? 0 : 1;
    else if (rc == 0) {
        uint64_t hash;
        wal_rec wr;
        memcpy(&hash, rec + 8, 8);
        bool ok = len > WAL_HDR && hash == db->wal_last_hash &&
                  wal_record(rec, len, db->wal_last_seq, db->wal_last_off, &wr) == len;
        rc = ok ? 0 : 1;
    }
    free(rec);
    if (rc == 1)
        fprintf(stderr, "tsdb: %s: the WAL record at offset %llu of segment %llu is not what was written: "
                        "the storage lost acknowledged data\n",
                dir, (unsigned long long)db->wal_last_off, (unsigned long long)db->wal_last_seq);
    return rc;
}

/* Writes the rows the halted engine holds in memory (its flushing memtables, oldest first, then the active one;
 * rows past what was durable only where the WAL is not synced per write: there they were acknowledged, in sync mode they were not) into a new
 * segment after the last one, on new descriptors, and syncs it and the directory. The open that follows replays it
 * after every older segment, so a row it holds wins over an older copy and nothing the memory knew is missing.
 * The failed descriptors are never used: what they report proves nothing (ha.md 15.5 step 4).
 * A row at timestamp INT64_MAX is not carried (a memtable range is half open). */
int wal_relog(eng *db, const char *dir, uint64_t *rows_out)
{
    *rows_out = 0;
    if (!db->wal_enabled)
        return 0;
    uint64_t visible = db->opt.wal_sync_ms == 0 ? atomic_load(&db->wal_durable) : UINT64_MAX;
    int dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return TSDB_ERR_IO;
    char name[64];
    uint64_t seq = db->wal_seq + 1;
    wal_name(name, sizeof name, seq);
    /* O_TRUNC, not O_EXCL: an attempt whose open failed afterwards left this segment, and the retry writes the same
     * rows again (the old engine's memory has not changed); nothing else can own this name, the lock is ours */
    int fd = openat(dirfd, name, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        close(dirfd);
        return TSDB_ERR_IO;
    }
    const size_t CH = 4096; /* rows per record */
    uint8_t *buf = malloc(WAL_HDR + CH * WAL_ROW);
    uint64_t fno = db->fno_next; /* the rows get numbers of their own: the numbers they had are not kept per row */
    int rc = buf ? 0 : TSDB_ERR_NOMEM;
    uint64_t off = 0, total = 0;
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    for (size_t k = 0; rc == 0 && k <= db->nflushing; k++) {
        memtable *m = k < db->nflushing ? db->flushing[k] : db->active;
        if (!m || m->rows == 0)
            continue;
        int64_t end = m->max_ts == INT64_MAX ? INT64_MAX : m->max_ts + 1;
        for (uint32_t i = 0; rc == 0 && i < m->nids; i++) {
            mem_run r;
            if ((rc = mt_collect(m, m->ids[i], 8, m->min_ts, end, visible, &r)) != 0)
                break;
            for (size_t a = 0; rc == 0 && a < r.n;) {
                size_t n = r.n - a < CH ? r.n - a : CH;
                uint8_t *p = buf + WAL_HDR;
                for (size_t j = 0; j < n; j++, p += WAL_ROW) {
                    uint32_t id = m->ids[i];
                    uint64_t x = slot_get(r.v, 8, a + j);
                    memcpy(p, &id, 4);
                    memcpy(p + 4, &r.ts[a + j], 8);
                    memcpy(p + 12, &x, 8);
                }
                if ((rc = wal_write_record(fd, seq, off, buf, n, fno)) == 0) {
                    off += WAL_HDR + n * WAL_ROW;
                    fno += n;
                    total += n;
                }
                a += n;
            }
            mem_run_free(&r);
        }
    }
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    free(buf);
    if (rc == 0 && fsync(fd) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && fsync(dirfd) != 0)
        rc = TSDB_ERR_IO;
    close(fd);
    close(dirfd);
    if (rc == 0)
        *rows_out = total;
    return rc;
}
