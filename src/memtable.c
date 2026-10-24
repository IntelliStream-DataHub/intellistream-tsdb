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
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>


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
        rc = insert_rows(db, type, s, 0, t, v, k, m->wal_seq, out);
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

static uint32_t wal_head_check(uint32_t nrows, uint64_t hash, uint64_t seq, uint64_t off)
{
    uint8_t t[12];
    store32(t, nrows);
    memcpy(t + 4, &hash, 8);
    return (uint32_t)xxh64(t, sizeof t, seq * 0x9e3779b97f4a7c15ull ^ off);
}

/* buf holds WAL_HDR spare bytes in front of the rows for the record header. */
static int wal_write_record(int fd, uint64_t seq, uint64_t off, uint8_t *buf, size_t nrows)
{
    uint64_t hash = xxh64(buf + WAL_HDR, nrows * WAL_ROW, 0);
    store32(buf, (uint32_t)nrows);
    store32(buf + 4, wal_head_check((uint32_t)nrows, hash, seq, off));
    memcpy(buf + 8, &hash, 8);
    return write_full(fd, buf, WAL_HDR + nrows * WAL_ROW);
}

size_t wal_record(const uint8_t *p, size_t avail, uint64_t seq, uint64_t off, size_t *nrows)
{
    if (avail < WAL_HDR)
        return 0;
    uint32_t n = load32(p);
    uint64_t hash;
    memcpy(&hash, p + 8, 8);
    if (n == 0 || n > (avail - WAL_HDR) / WAL_ROW || wal_head_check(n, hash, seq, off) != load32(p + 4) ||
        xxh64(p + WAL_HDR, (size_t)n * WAL_ROW, 0) != hash)
        return 0;
    *nrows = n;
    return WAL_HDR + (size_t)n * WAL_ROW;
}

bool wal_valid_after(const uint8_t *p, size_t len, size_t from, uint64_t seq, uint64_t base)
{
    size_t n;
    /* records are 4-byte aligned (16 + 20 n bytes); the header check rejects
     * nearly every other offset without hashing rows */
    for (size_t q = (from + 4) & ~(size_t)3; q + WAL_HDR <= len; q += 4)
        if (wal_record(p + q, len - q, seq, base + q, &n))
            return true;
    return false;
}

void wal_row(const uint8_t *r, uint32_t *id, int64_t *ts, uint64_t *slot)
{
    memcpy(id, r, 4);
    memcpy(ts, r + 4, 8);
    memcpy(slot, r + 12, 8);
}

/* Appends rows to the pending WAL buffer; caller holds buf_lock. */
static int wal_buffer(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w,
                      size_t n)
{
    if (db->wal_len == 0)
        db->wal_len = WAL_HDR; /* room for the record header */
    size_t need = db->wal_len + n * WAL_ROW;
    if (need > db->wal_cap) {
        size_t cap = MAX(need, db->wal_cap * 2);
        cap = MAX(cap, (size_t)65536);
        uint8_t *b = realloc(db->wal_buf, cap);
        if (!b)
            return TSDB_ERR_NOMEM;
        db->wal_buf = b;
        db->wal_cap = cap;
    }
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
    if (len <= WAL_HDR)
        return 0;
    if (wal_write_record(fd, seq, *off, buf, (len - WAL_HDR) / WAL_ROW) != 0) {
        eng_fail(db, "WAL write", errno);
        return TSDB_ERR_IO;
    }
    if (db->opt.fsync && fdatasync(fd) != 0) {
        eng_fail(db, "WAL fdatasync", errno);
        return TSDB_ERR_IO;
    }
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

int mem_insert(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n)
{
    pthread_mutex_lock(&db->buf_lock);
    int rc = db->bg_error;
    db->bg_error = 0;
    if (rc == 0 && db->wal_enabled)
        rc = wal_buffer(db, series, cs, ts, v, w, n); /* first: it can fail without side effects */
    if (rc == 0)
        rc = mt_add(db->active, series, cs, ts, v, w, n, db->wal_appended + 1);
    db->wal_appended += n;
    uint64_t lsn = db->wal_appended;
    size_t rows = db->active->rows;
    if (rows >= db->opt.buffer_rows)
        pthread_cond_signal(&db->flush_cv);
    pthread_mutex_unlock(&db->buf_lock);
    if (rc == 0 && db->wal_enabled && db->opt.wal_sync_ms == 0)
        rc = wal_commit(db, lsn);
    /* backpressure when the flusher falls behind */
    if (rc == 0 && rows >= 2 * db->opt.buffer_rows)
        rc = mt_flush(db);
    return rc;
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
        rc = mt_write(db, m, &pl);
        if (rc == 0) {
            pthread_mutex_lock(&db->publish_lock); /* numbered, named and listed in one step */
            rc = parts_commit(db, pl.p, pl.n);
            if (rc)
                pthread_mutex_unlock(&db->publish_lock);
        }
        if (rc) {
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
        pthread_cond_signal(&db->merge_cv);
        pthread_mutex_unlock(&db->lock);
        pthread_mutex_unlock(&db->publish_lock);
        int mrc = manifest_publish(db); /* fails the engine on error */
        free(pl.p);
        if (db->wal_enabled && mrc == 0) { /* else MANIFEST may still name it: a follower would tail it */
            char name[64];
            wal_name(name, sizeof name, m->wal_seq);
            unlinkat(db->dirfd, name, 0);
        }
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

/* Replays WAL segments left by a previous run, then opens a fresh segment. */
int wal_open(eng *db)
{
    bool ro = db->opt.read_only;
    seg *segs = NULL;
    size_t nseg = 0, cap = 0;
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d); /* the dup shares its offset with dirfd, which an earlier scan moved */
    struct dirent *e;
    while ((e = readdir(d))) {
        uint64_t seq;
        int consumed = 0;
        if (sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &seq, &consumed) != 1 || e->d_name[consumed])
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
        size_t pos = 0, n, rec;
        int64_t cached_p = 0;
        uint64_t cached_seq = 0;
        bool have_cache = false;
        while (rc == 0 && (rec = wal_record(buf + pos, len - pos, segs[k].seq, pos, &n)) != 0) {
            const uint8_t *r = buf + pos + WAL_HDR;
            for (size_t i = 0; i < n && rc == 0; i++, r += WAL_ROW) {
                uint32_t id;
                int64_t ts;
                uint64_t slot;
                wal_row(r, &id, &ts, &slot);
                int64_t p = partition_of(db, ts);
                if (!have_cache || p != cached_p) {
                    cached_p = p;
                    cached_seq = partition_wal_seq(db, p);
                    have_cache = true;
                }
                if (cached_seq >= segs[k].seq || id >= eng_series_count(db))
                    continue; /* already in a part (or a series that was never created) */
                rc = mt_add(m, NULL, id, &ts, &slot, 8, 1, 0);
            }
            pos += rec;
        }
        if (rc == 0 && pos < len && wal_valid_after(buf, len, pos, segs[k].seq, 0)) {
            fprintf(stderr,
                    "tsdb: WAL segment %s is damaged at offset %zu, before valid records: the open fails and the "
                    "segment is kept (move it out of the directory to accept the loss of its rows)\n",
                    segs[k].name, pos);
            rc = TSDB_ERR_CORRUPT;
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
        if (rc == 0 && !ro)
            unlinkat(db->dirfd, segs[k].name, 0);
    }
    free(segs);
    if (rc == 0 && !ro && db->opt.fsync && fsync(db->dirfd) != 0)
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
        if (db->active && db->active->rows == 0 && db->nflushing == 0 && !eng_check(db)) {
            char name[64];
            wal_name(name, sizeof name, db->wal_seq);
            unlinkat(db->dirfd, name, 0);
        }
    }
    for (size_t i = 0; i < db->nflushing; i++)
        mt_unref(db->flushing[i]);
    db->nflushing = 0;
    mt_unref(db->active);
    db->active = NULL;
    free(db->wal_buf);
    free(db->wal_spare);
    db->wal_buf = db->wal_spare = NULL;
}
