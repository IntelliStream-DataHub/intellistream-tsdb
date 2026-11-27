/* SPDX-License-Identifier: AGPL-3.0-only */
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
#include "ppos.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
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

#ifndef __APPLE__
/* The series array of a memtable that sees a million series is 40 MB, and mt_reserve_to grows it under buf_lock, which
 * every append and lookup takes. realloc and the memset of the new tail touched every page there (about 9 ms, twice,
 * right after a flush began a new memtable and the first rows with high ids arrived); a mapping is zero already,
 * mremap moves page tables instead of copying, and the pages are touched one by one as series are added. */
#define MT_S_MAP_MIN ((size_t)1 << 20)
#define MT_PAGE ((size_t)4096)
static size_t mt_s_bytes(uint32_t cap) { return ((size_t)cap * sizeof(mt_series) + MT_PAGE - 1) & ~(MT_PAGE - 1); }
#endif

static void mt_s_free(memtable *m)
{
#ifndef __APPLE__
    if (m->s_map) {
        tsdb_zap_slices(m->s, mt_s_bytes(m->scap)); /* the unmap then has nothing left to do under the mmap lock */
        munmap(m->s, mt_s_bytes(m->scap));
        return;
    }
#endif
    free(m->s);
}

/* Grows m->s to cap entries, the new ones zero. Caller holds buf_lock when m is active. */
static int mt_s_grow(memtable *m, uint32_t cap)
{
#ifndef __APPLE__
    if ((size_t)cap * sizeof(mt_series) >= MT_S_MAP_MIN) {
        void *p;
        if (m->s_map) {
            p = mremap(m->s, mt_s_bytes(m->scap), mt_s_bytes(cap), MREMAP_MAYMOVE);
        } else {
            p = mmap(NULL, mt_s_bytes(cap), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (p != MAP_FAILED) {
                if (m->scap)
                    memcpy(p, m->s, (size_t)m->scap * sizeof *m->s);
                free(m->s);
            }
        }
        if (p == MAP_FAILED)
            return TSDB_ERR_NOMEM;
        m->s = p;
        m->s_map = true;
        m->scap = cap;
        return 0;
    }
#endif
    mt_series *s = realloc(m->s, cap * sizeof *s);
    if (!s)
        return TSDB_ERR_NOMEM;
    memset(s + m->scap, 0, (cap - m->scap) * sizeof *s);
    m->s = s;
    m->scap = cap;
    return 0;
}

#define MT_ROW_BYTES (sizeof(int64_t) + sizeof(uint64_t) + sizeof(uint64_t)) /* 24 bytes: ts + v + lsn */
#define MT_BIG_PAD 64u /* 1 cache-line stagger between ts, v, lsn in mt_big to avoid 4 KiB L1D set aliasing */
#define MT_CHUNK_MIN ((size_t)64 << 10)  /* 64 KiB: small memtables stay small */
#define MT_CHUNK_MAX ((size_t)4 << 20)   /* 4 MiB: 1M series x 4 rows (~96 MiB) = ~26 chunks */

/* Allocates one packed row block (ts[cap], v[cap], lsn[cap]) from the memtable's arena (for cap <= 128) or as a
 * standalone mt_big block. Does not zero fresh chunks: under buf_lock only the 32-byte header and the requested block
 * are touched, keeping page faults incremental. */
static void *mt_blk_alloc(memtable *m, size_t cap, uint8_t lg)
{
    if (lg <= MT_ARENA_MAX_LG) {
        unsigned cls = lg - MT_INIT_LG;
        if (m->free_blk[cls]) {
            void *p = m->free_blk[cls];
            memcpy(&m->free_blk[cls], p, sizeof(void *));
            return p;
        }
        size_t bytes = cap * MT_ROW_BYTES;
        mt_chunk *c = m->chunks;
        if (!c || c->cap - c->used < bytes) {
            size_t total = m->chunk_next ? m->chunk_next : MT_CHUNK_MIN;
            while (total < sizeof(mt_chunk) + bytes)
                total = total < MT_CHUNK_MAX / 4 ? total * 4 : MAX(MT_CHUNK_MAX, sizeof(mt_chunk) + bytes);
            c = malloc(total);
            if (!c)
                return NULL;
            c->next = m->chunks;
            c->cap = total - sizeof *c;
            c->used = 0;
            m->chunks = c;
            m->chunk_next = total < MT_CHUNK_MAX ? MIN(total * 4, MT_CHUNK_MAX) : MT_CHUNK_MAX;
        }
        void *p = c->data + c->used;
        c->used += bytes;
        return p;
    }
    if (cap > (SIZE_MAX - sizeof(mt_big) - 2 * MT_BIG_PAD) / MT_ROW_BYTES)
        return NULL;
    size_t total = sizeof(mt_big) + cap * MT_ROW_BYTES + 2 * MT_BIG_PAD;
    mt_big *b = malloc(total);
    if (!b)
        return NULL;
    b->prev = NULL;
    b->next = m->big_head;
    b->bytes = total;
    if (m->big_head)
        m->big_head->prev = b;
    m->big_head = b;
    return b->data;
}

static void mt_blk_free(memtable *m, void *blk, uint8_t lg)
{
    if (lg <= MT_ARENA_MAX_LG) {
        unsigned cls = lg - MT_INIT_LG;
        memcpy(blk, &m->free_blk[cls], sizeof(void *));
        m->free_blk[cls] = blk;
        return;
    }
    mt_big *b = (mt_big *)((uint8_t *)blk - offsetof(mt_big, data));
    if (b->prev)
        b->prev->next = b->next;
    else
        m->big_head = b->next;
    if (b->next)
        b->next->prev = b->prev;
    tsdb_free_big(b, b->bytes);
}

void (*mt_test_unref_hook)(const memtable *m);

void mt_unref(memtable *m)
{
    if (!m || atomic_fetch_sub(&m->refs, 1) != 1)
        return;
    void (*hook)(const memtable *) = __atomic_load_n(&mt_test_unref_hook, __ATOMIC_ACQUIRE);
    if (hook)
        hook(m);
    for (mt_big *b = m->big_head, *nx; b; b = nx) {
        nx = b->next;
        tsdb_free_big(b, b->bytes);
    }
    for (mt_chunk *c = m->chunks, *nx; c; c = nx) {
        nx = c->next;
        tsdb_free_big(c, sizeof *c + c->cap);
    }
    if (m->ppos)
        ppos_mt_free(m);
    mt_s_free(m);
    tsdb_free_big(m->ids, (size_t)m->idcap * sizeof *m->ids);
    tsdb_free_big(m->fresh_ids, (size_t)m->fresh_cap * sizeof *m->fresh_ids);
    free(m);
}

/* test hooks (internal.h eng_ppos_rows, parts_commit and pw_finish) */
void (*ppos_test_note)(struct eng *db, memtable *m, int site, const int64_t *ts, size_t n, uint64_t fno,
                       const uint32_t *ix);
void (*ppos_test_record)(struct eng *db, const char *first, size_t n, uint64_t pos);
int (*mt_test_reserve_fail)(uint32_t id); /* test hook: nonzero fails the growth of series id (tests/test_failstop.c) */
/* test hook: how often the slow part of the room check ran (tests/test_failstop.c: a row inside the capacity must not
 * get here; counted with a relaxed atomic, on the slow path only) */
uint64_t mt_test_reserve_calls;

static inline size_t mt_cap(const mt_series *s) { return (size_t)(((uint64_t)1 << s->lgcap) >> 1); }

/* Room for `extra` more rows of series id, after the pend rows an insert has counted already (one capacity check;
 * the arrays at least double when they grow). The slow part of mt_room. */
static int mt_reserve_to(memtable *m, uint32_t id, size_t extra)
{
    __atomic_fetch_add(&mt_test_reserve_calls, 1, __ATOMIC_RELAXED);
    if (id >= m->scap) {
        uint32_t cap = MAX(id + 1, m->scap * 2);
        cap = MAX(cap, 64u);
        int rc = mt_s_grow(m, cap);
        if (rc)
            return rc;
    }
    mt_series *s = &m->s[id];
    size_t need = (size_t)s->n + s->pend + extra;
    if (need <= mt_cap(s))
        return 0;
    if (mt_test_reserve_fail && mt_test_reserve_fail(id))
        return TSDB_ERR_NOMEM;
    if (need > UINT32_MAX / 2)
        return TSDB_ERR_NOMEM;
    if (s->lgcap == 0 && m->nids == m->idcap) {
        uint32_t c = m->idcap ? m->idcap * 2 : 64;
        uint32_t *ids = realloc(m->ids, c * sizeof *ids);
        if (!ids)
            return TSDB_ERR_NOMEM;
        m->ids = ids;
        m->idcap = c;
    }
    size_t cap = s->lgcap ? mt_cap(s) * 2 : MT_INIT_CAP;
    while (cap < need)
        cap *= 2;
    uint8_t new_lg = (uint8_t)(__builtin_ctzll(cap) + 1);
    void *blk = mt_blk_alloc(m, cap, new_lg);
    if (!blk)
        return TSDB_ERR_NOMEM;
    size_t pad = new_lg > MT_ARENA_MAX_LG ? MT_BIG_PAD : 0;
    int64_t *t = (int64_t *)blk;
    uint64_t *v = (uint64_t *)((uint8_t *)(t + cap) + pad);
    uint64_t *l = (uint64_t *)((uint8_t *)(v + cap) + pad);
    if (s->lgcap != 0) {
        if (s->n) {
            memcpy(t, s->ts, (size_t)s->n * sizeof *t);
            memcpy(v, s->v, (size_t)s->n * sizeof *v);
            memcpy(l, s->lsn, (size_t)s->n * sizeof *l);
        }
        mt_blk_free(m, s->ts, s->lgcap);
    } else {
        m->ids[m->nids++] = id;
        s->sorted = true;
    }
    s->ts = t;
    s->v = v;
    s->lsn = l;
    s->lgcap = new_lg;
    return 0;
}

/* test hook (tsdb_test_mem_cap): the capacity of series `local` in the active memtable, 0 for none; caller holds buf_lock */
size_t mt_test_cap(const memtable *m, uint32_t local) { return local < m->scap ? mt_cap(&m->s[local]) : 0; }

/* Room for `extra` more rows of series id: the capacity check is inline, the growth is not. */
static inline int mt_room(memtable *m, uint32_t id, size_t extra)
{
    if (__builtin_expect(id < m->scap, 1)) {
        const mt_series *s = &m->s[id];
        if (__builtin_expect((size_t)s->n + s->pend + extra <= mt_cap(s), 1))
            return 0;
    }
    return mt_reserve_to(m, id, extra);
}

/* Room for all rows of one insert in m (the active memtable, under buf_lock), taken before the insert's WAL append:
 * the mt_add after that append cannot fail then. A failure between the two would leave a refused write in the WAL,
 * which a crash replays (and part of it in the memtable at once).
 * One series (series == NULL): one capacity check for all rows. Rows of several series: each row is counted into
 * its series' pend as it is checked, so a series that comes more than once is checked for all its rows, in any order
 * of the ids; a row whose series has the room costs a compare and an increment. mt_add (on the lines it touches
 * anyway) clears the counts, as does mt_unreserve when no mt_add follows.
 * On failure the counts are cleared and the series this call registered are unregistered and freed again (the growth
 * of one already listed stays: harmless). A series a successful reservation registered stays listed with no rows if
 * the WAL append then fails: the readers of a memtable treat a series without rows like one with a few. */
static void mt_unreserve(memtable *m, const uint32_t *series, size_t n)
{
    for (size_t k = 0; k < n; k++)
        m->s[series[k]].pend = 0;
}

int mt_reserve_rows(memtable *m, const uint32_t *series, uint32_t cs, size_t n)
{
    uint32_t nids0 = m->nids;
    int rc = 0;
    if (!series) {
        rc = mt_room(m, cs, n);
    } else {
        size_t k = 0;
        for (; k < n; k++) {
            if ((rc = mt_room(m, series[k], 1)) != 0)
                break;
            m->s[series[k]].pend++;
        }
        if (rc) {
            mt_unreserve(m, series, k);
            for (uint32_t i = nids0; i < m->nids; i++) {
                mt_series *s = &m->s[m->ids[i]];
                if (s->lgcap)
                    mt_blk_free(m, s->ts, s->lgcap);
                s->ts = NULL, s->v = NULL, s->lsn = NULL;
                s->lgcap = 0;
            }
            m->nids = nids0;
        }
    }
    return rc;
}

/* Adds rows to a memtable the caller may modify (the active one, under buf_lock). After mt_reserve_rows for the same
 * rows it cannot fail (and clears the counts); the replays at open reserve as they go. */
int mt_add(memtable *m, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n,
           uint64_t lsn0, int type)
{
    const uint64_t null = type >= 0 ? type_null(type) : 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t id = series ? series[i] : cs;
        if (__builtin_expect(id >= m->scap || m->s[id].n >= mt_cap(&m->s[id]), 0)) { /* not after a reservation */
            int rc = mt_reserve_to(m, id, 1);
            if (rc)
                return rc;
        }
        mt_series *s = &m->s[id];
        s->pend = 0; /* the rows this insert counted are being added */
        if (s->n && ts[i] < s->ts[s->n - 1])
            s->sorted = false;
        uint64_t slot = slot_get(v, w, i);
        if (type >= 0) { /* the latest table's memtable layer (design-latest.md 2.2), on the line just touched */
            s->type = (uint8_t)type;
            if (slot != null) { /* a newer (or equal-time) non-null row is the series' newest; an older one is not */
                if (!(s->fl & MTF_HAS_NN) || ts[i] >= s->ts[s->top_nn]) {
                    s->top_nn = s->n;
                    s->fl = (uint8_t)((s->fl | MTF_HAS_NN) & ~MTF_NN_STALE);
                }
            } else {
                s->fl |= MTF_HAS_NULL;
                if ((s->fl & MTF_HAS_NN) && ts[i] == s->ts[s->top_nn])
                    s->fl |= MTF_NN_STALE; /* last write at the newest non-null timestamp is a null now */
            }
        }
        s->ts[s->n] = ts[i];
        s->v[s->n] = slot;
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

/* Copies the raw rows of series id in [start, end) with lsn <= visible into out
 * (values remain 8-byte slots, possibly unsorted/undeduplicated when !*sorted_out).
 * Fast enough to run under buf_lock; call mt_finish() unlocked to sort, dedup, and narrow. */
int mt_copy_raw(const memtable *m, uint32_t id, int64_t start, int64_t end, uint64_t visible, mem_run *out,
                bool *sorted_out)
{
    memset(out, 0, sizeof *out);
    *sorted_out = true;
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
    if (s->sorted) {
        size_t lo = lower_bound_i64(s->ts, vn, start);
        size_t hi = lower_bound_i64(s->ts, vn, end);
        if (lo >= hi)
            return 0;
        size_t cnt = hi - lo;
        out->ts = malloc(cnt * sizeof *out->ts);
        out->v = malloc(cnt * sizeof(uint64_t));
        if (!out->ts || !out->v) {
            mem_run_free(out);
            return TSDB_ERR_NOMEM;
        }
        out->n = cnt;
        memcpy(out->ts, s->ts + lo, cnt * sizeof *out->ts);
        memcpy(out->v, s->v + lo, cnt * sizeof(uint64_t));
        *sorted_out = true;
        return 0;
    }
    size_t cnt = 0;
    for (size_t i = 0; i < vn; i++)
        cnt += (size_t)(s->ts[i] >= start && s->ts[i] < end);
    if (!cnt)
        return 0;
    out->ts = malloc(cnt * sizeof *out->ts);
    out->v = malloc(cnt * sizeof(uint64_t));
    if (!out->ts || !out->v) {
        mem_run_free(out);
        return TSDB_ERR_NOMEM;
    }
    uint64_t *ov = (uint64_t *)(void *)out->v;
    for (size_t i = 0; i < vn; i++) {
        if (s->ts[i] >= start && s->ts[i] < end) {
            out->ts[out->n] = s->ts[i];
            ov[out->n] = s->v[i];
            out->n++;
        }
    }
    *sorted_out = false;
    return 0;
}

/* Unlocked post-processing for a run copied by mt_copy_raw: sort, keep last per ts, narrow to w. */
int mt_finish(mem_run *out, size_t w, bool sorted)
{
    if (!out->n)
        return 0;
    uint64_t *ov = (uint64_t *)(void *)out->v;
    int rc = sorted ? 0 : sort_by_time(out->ts, ov, out->n);
    if (rc) {
        mem_run_free(out);
        return rc;
    }
    out->n = keep_last(out->ts, ov, out->n);
    narrow(out, w);
    return 0;
}

/* Copies the rows of series id in [start, end): sorted by time, last write
 * per timestamp. Reads only; the caller holds buf_lock when m is active. */
int mt_collect(const memtable *m, uint32_t id, size_t w, int64_t start, int64_t end, uint64_t visible, mem_run *out)
{
    bool sorted;
    int rc = mt_copy_raw(m, id, start, end, visible, out, &sorted);
    return rc ? rc : mt_finish(out, w, sorted);
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
    uint32_t tcnt[NTYPES] = {0};
    size_t trows[NTYPES] = {0};
    uint32_t nactive = 0;
    for (uint32_t i = 0; i < m->nids; i++) {
        uint32_t id = m->ids[i];
        const mt_series *ms = &m->s[id];
        if (ms->n == 0)
            continue;
        int ty = ms->fl ? (int)ms->type : series_type(&db->series, id);
        if ((unsigned)ty < NTYPES) {
            tcnt[ty]++;
            trows[ty] += ms->n;
            nactive++;
        }
    }
    int ntypes_present = 0, only_type = -1;
    size_t max_rows = 0;
    for (int ty = 0; ty < NTYPES; ty++) {
        if (tcnt[ty]) {
            ntypes_present++;
            only_type = ty;
            if (trows[ty] > max_rows)
                max_rows = trows[ty];
        }
    }
    if (max_rows == 0)
        return 0;
    /* Reuse grow-only flush scratch on db (guarded by flush_lock, or single-threaded during wal_open;
     * freed in wal_close via tsdb_free_big) so steady flushes avoid repeated multi-MB page faults and munmaps. */
    if (max_rows > db->fl_rows_cap) {
        tsdb_free_big(db->fl_s, db->fl_rows_cap * sizeof *db->fl_s);
        tsdb_free_big(db->fl_t, db->fl_rows_cap * sizeof *db->fl_t);
        tsdb_free_big(db->fl_v, db->fl_rows_cap * sizeof *db->fl_v);
        db->fl_s = NULL;
        db->fl_t = NULL;
        db->fl_v = NULL;
        db->fl_rows_cap = 0;
        uint32_t *ns = malloc(max_rows * sizeof *ns);
        int64_t *nt = ns ? malloc(max_rows * sizeof *nt) : NULL;
        uint64_t *nv = nt ? malloc(max_rows * sizeof *nv) : NULL;
        if (!nv) {
            tsdb_free_big(ns, max_rows * sizeof *ns);
            tsdb_free_big(nt, max_rows * sizeof *nt);
            return TSDB_ERR_NOMEM;
        }
        db->fl_s = ns;
        db->fl_t = nt;
        db->fl_v = nv;
        db->fl_rows_cap = max_rows;
    }
    size_t need_ids = nactive ? nactive : 1;
    if (need_ids > db->fl_ids_cap) {
        tsdb_free_big(db->fl_ids, db->fl_ids_cap * sizeof *db->fl_ids);
        db->fl_ids = malloc(need_ids * sizeof *db->fl_ids);
        db->fl_ids_cap = db->fl_ids ? need_ids : 0;
        if (!db->fl_ids)
            return TSDB_ERR_NOMEM;
    }
    uint32_t *s = db->fl_s;
    int64_t *t = db->fl_t;
    uint64_t *v = db->fl_v;
    uint32_t *ids = db->fl_ids;
    int rc = 0;
    uint32_t toff[NTYPES] = {0};
    if (rc == 0) {
        if (ntypes_present == 1) {
            toff[only_type] = 0;
            if (nactive == m->nids) {
                memcpy(ids, m->ids, nactive * sizeof *ids);
            } else {
                uint32_t p = 0;
                for (uint32_t i = 0; i < m->nids; i++) {
                    uint32_t id = m->ids[i];
                    const mt_series *ms = &m->s[id];
                    if (ms->n == 0)
                        continue;
                    int ty = ms->fl ? (int)ms->type : series_type(&db->series, id);
                    if (ty == only_type)
                        ids[p++] = id;
                }
            }
            qsort(ids, tcnt[only_type], sizeof *ids, cmp_u32);
        } else {
            uint32_t tpos[NTYPES];
            uint32_t off = 0;
            for (int ty = 0; ty < NTYPES; ty++) {
                toff[ty] = off;
                tpos[ty] = off;
                off += tcnt[ty];
            }
            for (uint32_t i = 0; i < m->nids; i++) {
                uint32_t id = m->ids[i];
                const mt_series *ms = &m->s[id];
                if (ms->n == 0)
                    continue;
                int ty = ms->fl ? (int)ms->type : series_type(&db->series, id);
                if ((unsigned)ty < NTYPES)
                    ids[tpos[ty]++] = id;
            }
            for (int ty = 0; ty < NTYPES; ty++)
                if (tcnt[ty] > 1)
                    qsort(ids + toff[ty], tcnt[ty], sizeof *ids, cmp_u32);
        }
    }
    for (int type = 0; type < NTYPES && rc == 0; type++) {
        if (!tcnt[type])
            continue;
        size_t k = 0, w = type_width(type);
        const uint32_t *tids = ids + toff[type];
        uint32_t nt = tcnt[type];
        for (uint32_t i = 0; i < nt && rc == 0; i++) {
            uint32_t id = tids[i];
            const mt_series *ms = &m->s[id];
            for (uint32_t j = 0; j < ms->n; j++)
                s[k + j] = id;
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
    return rc;
}

/* ------------------------------------------------------------------ WAL */

void wal_name(char *buf, size_t len, uint64_t seq) { snprintf(buf, len, "wal_%016" PRIx64 ".log", seq); }

void wsegs_add(eng *db, uint64_t seq, uint64_t first, uint64_t last)
{
    if (!first)
        return;
    if (last < first)
        last = first;
    pthread_mutex_lock(&db->wsegs_mu);
    if (db->nwsegs > 0 && db->wsegs[db->nwsegs - 1].seq == seq) {
        if (first < db->wsegs[db->nwsegs - 1].first)
            db->wsegs[db->nwsegs - 1].first = first;
        if (last > db->wsegs[db->nwsegs - 1].last)
            db->wsegs[db->nwsegs - 1].last = last;
        pthread_mutex_unlock(&db->wsegs_mu);
        return;
    }
    size_t lo = 0, hi = db->nwsegs;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (db->wsegs[mid].seq < seq)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo < db->nwsegs && db->wsegs[lo].seq == seq) {
        if (first < db->wsegs[lo].first)
            db->wsegs[lo].first = first;
        if (last > db->wsegs[lo].last)
            db->wsegs[lo].last = last;
        pthread_mutex_unlock(&db->wsegs_mu);
        return;
    }
    if (db->nwsegs == db->capwsegs) {
        size_t nc = db->capwsegs ? db->capwsegs * 2 : 16;
        wseg *nw = realloc(db->wsegs, nc * sizeof *nw);
        if (!nw) {
            db->wsegs_dirty = true;
            pthread_mutex_unlock(&db->wsegs_mu);
            return;
        }
        db->wsegs = nw;
        db->capwsegs = nc;
    }
    if (lo < db->nwsegs)
        memmove(db->wsegs + lo + 1, db->wsegs + lo, (db->nwsegs - lo) * sizeof *db->wsegs);
    db->wsegs[lo] = (wseg){.seq = seq, .first = first, .last = last};
    db->nwsegs++;
    pthread_mutex_unlock(&db->wsegs_mu);
}

void wsegs_remove(eng *db, uint64_t seq)
{
    pthread_mutex_lock(&db->wsegs_mu);
    size_t lo = 0, hi = db->nwsegs;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (db->wsegs[mid].seq < seq)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo < db->nwsegs && db->wsegs[lo].seq == seq) {
        if (lo + 1 < db->nwsegs)
            memmove(db->wsegs + lo, db->wsegs + lo + 1, (db->nwsegs - lo - 1) * sizeof *db->wsegs);
        db->nwsegs--;
    }
    pthread_mutex_unlock(&db->wsegs_mu);
}

int wal_open_segment(eng *db, uint64_t seq, int *fd)
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
int write_full(int fd, const void *buf, size_t len)
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

/* Seals the one record at rec again after a test changed its bytes (its fno, say) or moved it to segment seq, offset
 * off: the hash and the header check come out as the writer's (tests/test_fno.c builds records a bad disk or a bug
 * could leave, which still pass every check of a record). */
void wal_reseal(uint8_t *rec, uint64_t seq, uint64_t off)
{
    size_t last;
    uint64_t hash;
    int64_t commit_ms;
    memcpy(&commit_ms, rec + 24, 8);
    wal_seal(rec, wal_rec_size(rec), seq, off, commit_ms, &last, &hash);
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

bool wal_col_header_valid(const uint8_t *payload, size_t payload_len, uint32_t *nrows_out)
{
    if (payload_len < WAL_COL_HDR || (payload_len & 3u) != 0)
        return false;
    uint32_t nrows = load32(payload);
    uint8_t flags = payload[4], pad = payload[5];
    uint16_t rsv;
    memcpy(&rsv, payload + 6, 2);
    uint32_t cs = load32(payload + 8), uncomp_len = load32(payload + 12);
    if (nrows == 0 || nrows >= (1u << 28) || (flags & ~WAL_COL_FLAGS_MASK) || pad > 3 || rsv != 0)
        return false;
    if (!(flags & WAL_COL_SINGLE) && cs != 0)
        return false;
    uint64_t id_bytes = (flags & WAL_COL_SINGLE) ? 0 : (uint64_t)nrows * 4;
    uint64_t ts_bytes = (flags & WAL_COL_TS32) ? 8 + (uint64_t)(nrows - 1) * 4 : (uint64_t)nrows * 8;
    uint64_t val_bytes = (uint64_t)nrows * ((flags & WAL_COL_VAL4) ? 4 : 8);
    uint64_t expect = id_bytes + ts_bytes + val_bytes;
    if (expect > WAL_COL_MAX_UNCOMP || (uint64_t)uncomp_len != expect)
        return false;
    if ((size_t)WAL_COL_HDR + (size_t)pad >= payload_len)
        return false;
    size_t comp_len = payload_len - WAL_COL_HDR - (size_t)pad;
    if (!(flags & WAL_COL_ZSTD) && comp_len != (size_t)uncomp_len)
        return false;
    for (uint8_t i = 0; i < pad; i++)
        if (payload[payload_len - 1 - i] != 0)
            return false;
    if (nrows_out)
        *nrows_out = nrows;
    return true;
}

void wal_col_rows_free(wal_col_rows *r)
{
    free(r->heap);
    memset(r, 0, sizeof *r);
}

int wal_col_decode(const uint8_t *payload, size_t payload_len, void *dctx, wal_col_rows *out)
{
    memset(out, 0, sizeof *out);
    uint32_t nrows = 0;
    if (!wal_col_header_valid(payload, payload_len, &nrows))
        return TSDB_ERR_CORRUPT;
    uint8_t flags = payload[4], pad = payload[5];
    uint32_t cs = load32(payload + 8), uncomp_len = load32(payload + 12);
    size_t comp_len = payload_len - WAL_COL_HDR - (size_t)pad;
    bool single = (flags & WAL_COL_SINGLE) != 0;
    bool val4 = (flags & WAL_COL_VAL4) != 0;
    bool ts32 = (flags & WAL_COL_TS32) != 0;
    bool zstd = (flags & WAL_COL_ZSTD) != 0;
    size_t n = nrows;
    size_t id_sz = single ? 0 : n * 4;
    size_t ts_sz = n * 8;
    size_t w = val4 ? 4 : 8;
    size_t val_sz = n * w;
    /* Single 8-byte aligned backing allocation: [ts (8*n)] [vals (w*n)] [ids (4*n)] [raw (uncomp_len if zstd)] */
    size_t off_vals = ts_sz;
    size_t off_ids = (off_vals + val_sz + 7) & ~(size_t)7;
    size_t off_raw = (off_ids + id_sz + 7) & ~(size_t)7;
    size_t total = off_raw + (zstd ? (size_t)uncomp_len : 0);
    uint8_t *heap = malloc(total ? total : 1);
    if (!heap)
        return TSDB_ERR_NOMEM;
    const uint8_t *raw = payload + WAL_COL_HDR;
    if (zstd) {
        uint8_t *dst = heap + off_raw;
        size_t r = dctx ? ZSTD_decompressDCtx((ZSTD_DCtx *)dctx, dst, uncomp_len, raw, comp_len)
                        : ZSTD_decompress(dst, uncomp_len, raw, comp_len);
        if (ZSTD_isError(r) || r != (size_t)uncomp_len) {
            free(heap);
            return TSDB_ERR_CORRUPT;
        }
        raw = dst;
    }
    const uint8_t *p = raw;
    uint32_t *ids = NULL;
    if (!single) {
        ids = (uint32_t *)(void *)(heap + off_ids);
        uint32_t cur;
        memcpy(&cur, p, 4);
        ids[0] = cur;
        for (size_t i = 1; i < n; i++) {
            uint32_t d;
            memcpy(&d, p + i * 4, 4);
            cur += d;
            ids[i] = cur;
        }
        p += n * 4;
    }
    int64_t *ts = (int64_t *)(void *)heap;
    uint64_t tcur;
    memcpy(&tcur, p, 8);
    p += 8;
    ts[0] = (int64_t)tcur;
    if (ts32) {
        for (size_t i = 1; i < n; i++) {
            int32_t d;
            memcpy(&d, p + (i - 1) * 4, 4);
            tcur += (uint64_t)(int64_t)d;
            ts[i] = (int64_t)tcur;
        }
        p += (n - 1) * 4;
    } else {
        for (size_t i = 1; i < n; i++) {
            uint64_t d;
            memcpy(&d, p + (i - 1) * 8, 8);
            tcur += d;
            ts[i] = (int64_t)tcur;
        }
        p += (n - 1) * 8;
    }
    void *vals = heap + off_vals;
    memcpy(vals, p, val_sz);
    out->nrows = nrows;
    out->cs = cs;
    out->ids = ids;
    out->ts = ts;
    out->vals = vals;
    out->w = w;
    out->heap = heap;
    return 0;
}

/* range: a number at or above FNO_LIMIT is not a record. The check of a writer's open passes false and judges the
 * range itself: a record whose checks pass but whose number is out of range is damage, not a torn tail. */
static size_t wal_record_x(const uint8_t *p, size_t avail, uint64_t seq, uint64_t off, wal_rec *r, bool range)
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
    } else if (kind == WAL_KIND_COL) {
        uint32_t nrows = 0;
        if (n > avail - WAL_HDR || !wal_col_header_valid(p + WAL_HDR, n, &nrows))
            return 0;
        body = n;
        span = nrows;
    } else {
        return 0;
    }
    if (wal_head_check(w, hash, seq, off) != load32(p + 4) || xxh64(p + 16, 16 + body, 0) != hash || fno == 0 ||
        (range && fno >= FNO_LIMIT))
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

size_t wal_record(const uint8_t *p, size_t avail, uint64_t seq, uint64_t off, wal_rec *r)
{
    return wal_record_x(p, avail, seq, off, r, true);
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

/* Closes the open rows record: its row count goes into its header now, once, instead of on every append (the header
 * is a cache line far from the tail the appenders write, and it would travel between cores with every call). Called
 * with buf_lock held at every place that hands wal_buf on or appends something that is not a row of this record. */
atomic_uint wal_rec_max_rows = 1u << 28; /* a record's row count has 28 bits; tests lower it to split records */
atomic_uint jr_rec_max_rows = 1u << 18; /* internal.h: a leader's record fits one peer frame */

static void wal_rec_close(eng *db)
{
    if (db->wal_rec_open) {
        uint8_t *h = db->wal_buf + db->wal_rec_at;
        store32(h, ((uint32_t)WAL_KIND_ROWS << 28) | db->wal_rec_rows);
        db->wal_rec_open = false;
    }
}

/* A leader's admission of a record before it is buffered (jr_ops.admit; buf_lock held): a deposed leader refuses
 * every write, definitely not applied (the request may go to the new leader). */
__thread uint16_t eng_admit_marker;

static int leader_admit(eng *db, uint64_t fno, uint32_t span)
{
    const struct jr_ops *jr = eng_jr(db);
    if (!jr)
        return 0;
    if (atomic_load(&db->lead_deposed))
        return TSDB_ERR_UNAVAILABLE;
    return jr->admit ? jr->admit(jr->ctx, db, fno, span) : 0;
}

/* Reusable encoder scratch for building WAL_KIND_COL records outside buf_lock. */
static struct wal_enc_slot *wal_enc_acquire(eng *db)
{
    for (size_t i = 0; i < WAL_ENC_SLOTS; i++) {
        if (!atomic_load_explicit(&db->wal_enc[i].busy, memory_order_relaxed) &&
            !atomic_exchange_explicit(&db->wal_enc[i].busy, true, memory_order_acquire))
            return &db->wal_enc[i];
    }
    return NULL;
}

static void wal_enc_release(struct wal_enc_slot *s)
{
    if (s)
        atomic_store_explicit(&s->busy, false, memory_order_release);
}

/* Encodes (series, cs, ts, v, w, n) into a ready-to-append WAL_KIND_COL record (with 32-byte header, fno=0
 * placeholder) OUTSIDE buf_lock. Returns total record length (> 0) and sets *rec_out, or 0 to fall back to
 * WAL_KIND_ROWS (also sets *owned_out = true if caller must free *rec_out). */
static size_t wal_col_encode(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w,
                             size_t n, struct wal_enc_slot **slot_out, uint8_t **rec_out, bool *owned_out)
{
    *slot_out = NULL;
    *rec_out = NULL;
    *owned_out = false;
    if (n < WAL_COL_MIN_ROWS || n >= (1u << 28) || (w != 4 && w != 8))
        return 0;
    bool single = series == NULL;
    bool val4 = w == 4;
    uint64_t max_uncomp = (single ? 0 : (uint64_t)n * 4) + (uint64_t)n * 8 + (uint64_t)n * w;
    if (max_uncomp > WAL_COL_MAX_UNCOMP)
        return 0;

    struct wal_enc_slot *slot = wal_enc_acquire(db);
    ZSTD_CCtx *cctx = NULL;
    bool own_cctx = false;
    uint8_t *stage = NULL, *rec = NULL;
    size_t zbound = ZSTD_compressBound((size_t)max_uncomp);
    size_t rec_need = WAL_HDR + WAL_COL_HDR + MAX((size_t)max_uncomp, zbound) + 4;
    if (slot) {
        if (!slot->cctx)
            slot->cctx = ZSTD_createCCtx();
        cctx = (ZSTD_CCtx *)slot->cctx;
        if (slot->stage_cap < (size_t)max_uncomp) {
            uint8_t *nb = realloc(slot->stage, (size_t)max_uncomp);
            if (!nb) {
                wal_enc_release(slot);
                return 0;
            }
            slot->stage = nb;
            slot->stage_cap = (size_t)max_uncomp;
        }
        if (slot->rec_cap < rec_need) {
            uint8_t *nb = realloc(slot->rec, rec_need);
            if (!nb) {
                wal_enc_release(slot);
                return 0;
            }
            slot->rec = nb;
            slot->rec_cap = rec_need;
        }
        stage = slot->stage;
        rec = slot->rec;
    } else {
        cctx = ZSTD_createCCtx();
        own_cctx = true;
        stage = malloc((size_t)max_uncomp);
        rec = malloc(rec_need);
        if (!stage || !rec) {
            free(stage);
            free(rec);
            if (cctx)
                ZSTD_freeCCtx(cctx);
            return 0;
        }
    }

    uint8_t *p = stage;
    if (!single) {
        uint32_t prev = series[0];
        memcpy(p, &prev, 4);
        for (size_t i = 1; i < n; i++) {
            uint32_t cur = series[i], d = cur - prev;
            memcpy(p + i * 4, &d, 4);
            prev = cur;
        }
        p += n * 4;
    }
    /* Timestamp deltas: check whether every consecutive delta fits in int32_t. */
    uint64_t bad32 = 0;
    for (size_t i = 1; i < n; i++) {
        int64_t d = (int64_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1]);
        bad32 |= (uint64_t)(d ^ (int64_t)(int32_t)d);
    }
    bool ts32 = bad32 == 0;
    memcpy(p, &ts[0], 8);
    p += 8;
    if (ts32) {
        for (size_t i = 1; i < n; i++) {
            int32_t d = (int32_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1]);
            memcpy(p + (i - 1) * 4, &d, 4);
        }
        p += (n - 1) * 4;
    } else {
        for (size_t i = 1; i < n; i++) {
            int64_t d = (int64_t)((uint64_t)ts[i] - (uint64_t)ts[i - 1]);
            memcpy(p + (i - 1) * 8, &d, 8);
        }
        p += (n - 1) * 8;
    }
    memcpy(p, v, n * w);
    p += n * w;
    size_t uncomp_len = (size_t)(p - stage);

    uint8_t *body_dst = rec + WAL_HDR + WAL_COL_HDR;
    size_t comp_len = uncomp_len;
    bool use_zstd = false;
    if (cctx) {
        size_t zlen = ZSTD_compressCCtx(cctx, body_dst, zbound, stage, uncomp_len, -1);
        if (!ZSTD_isError(zlen) && zlen > 0 && zlen + 16 < uncomp_len) {
            comp_len = zlen;
            use_zstd = true;
        }
    }
    if (!use_zstd)
        memcpy(body_dst, stage, uncomp_len);
    uint8_t pad = (uint8_t)((4u - ((WAL_COL_HDR + comp_len) & 3u)) & 3u);
    for (uint8_t i = 0; i < pad; i++)
        body_dst[comp_len + i] = 0;
    size_t payload_len = WAL_COL_HDR + comp_len + pad;
    if (payload_len >= (1u << 28)) {
        if (slot) {
            wal_enc_release(slot);
        } else {
            free(stage);
            free(rec);
            if (own_cctx && cctx)
                ZSTD_freeCCtx(cctx);
        }
        return 0;
    }
    memset(rec, 0, WAL_HDR);
    store32(rec, (uint32_t)payload_len | ((uint32_t)WAL_KIND_COL << 28));
    uint8_t *ch = rec + WAL_HDR;
    store32(ch, (uint32_t)n);
    ch[4] = (uint8_t)((single ? WAL_COL_SINGLE : 0u) | (val4 ? WAL_COL_VAL4 : 0u) | (ts32 ? WAL_COL_TS32 : 0u) |
                      (use_zstd ? WAL_COL_ZSTD : 0u));
    ch[5] = pad;
    ch[6] = ch[7] = 0;
    store32(ch + 8, single ? cs : 0u);
    store32(ch + 12, (uint32_t)uncomp_len);

    if (!slot) {
        free(stage);
        if (own_cctx && cctx)
            ZSTD_freeCCtx(cctx);
        *owned_out = true;
    } else {
        *slot_out = slot;
    }
    *rec_out = rec;
    return WAL_HDR + payload_len;
}

/* Appends a pre-encoded WAL_KIND_COL record to the pending WAL buffer and stamps its fno; caller holds buf_lock. */
static int wal_buffer_pre(eng *db, const uint8_t *rec, size_t rec_len, size_t n, uint64_t *fno0)
{
    if (db->fno_next >= FNO_BOUND_MAX - n) {
        eng_fail(db, "WAL numbers", EOVERFLOW);
        return TSDB_ERR_IO;
    }
    size_t need = db->wal_len + rec_len;
    if (need > db->wal_cap) {
        size_t cap = MAX(need, db->wal_cap * 2);
        cap = MAX(cap, (size_t)65536);
        uint8_t *b = realloc(db->wal_buf, cap);
        if (!b)
            return TSDB_ERR_NOMEM;
        db->wal_buf = b;
        db->wal_cap = cap;
    }
    int arc = eng_test_mutant == MUT_LEADER_ADMIT_AFTER ? 0 : leader_admit(db, db->fno_next, (uint32_t)n);
    if (arc)
        return arc;
    wal_rec_close(db);
    uint8_t *h = db->wal_buf + db->wal_len;
    memcpy(h, rec, rec_len);
    memcpy(h + 16, &db->fno_next, 8);
    *fno0 = db->fno_next;
    db->fno_next += n;
    db->fno_rows_hi = db->fno_next - 1;
    db->wal_len += rec_len;
    if (eng_test_mutant == MUT_LEADER_ADMIT_AFTER) /* planted: the refusal comes with the batch already buffered */
        return leader_admit(db, *fno0, (uint32_t)n);
    return 0;
}

/* Appends rows to the pending WAL buffer and numbers them (*fno0 gets the first); caller holds buf_lock. Rows of
 * successive batches join one record while no marker comes between them (their numbers stay contiguous). Fails
 * without side effects. */
static int wal_buffer(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w,
                      size_t n, uint64_t *fno0)
{
    if (db->fno_next >= FNO_BOUND_MAX - n) { /* never number a record past what the next open accepts */
        eng_fail(db, "WAL numbers", EOVERFLOW);
        return TSDB_ERR_IO;
    }
    uint32_t rmax = atomic_load_explicit(&wal_rec_max_rows, memory_order_relaxed);
    if (eng_jr(db))
        rmax = MIN(rmax, atomic_load_explicit(&jr_rec_max_rows, memory_order_relaxed));
    bool extend = db->wal_rec_open && (uint64_t)db->wal_rec_rows + n < rmax;
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
    /* a leader admits the batch last: after it, nothing may fail (its driver has handed out the positions) */
    int arc = eng_test_mutant == MUT_LEADER_ADMIT_AFTER ? 0 : leader_admit(db, db->fno_next, (uint32_t)n);
    if (arc)
        return arc;
    if (!extend) {
        wal_rec_close(db); /* a record that reached its cap ends here: its count must be in its header */
        uint8_t *h = db->wal_buf + db->wal_len;
        memset(h, 0, WAL_HDR);
        store32(h, (uint32_t)WAL_KIND_ROWS << 28);
        memcpy(h + 16, &db->fno_next, 8);
        db->wal_rec_at = db->wal_len;
        db->wal_rec_open = true;
        db->wal_rec_rows = 0;
        db->wal_len += WAL_HDR;
    }
    db->wal_rec_rows += (uint32_t)n;
    *fno0 = db->fno_next;
    db->fno_next += n;
    db->fno_rows_hi = db->fno_next - 1;
    uint8_t *p = db->wal_buf + db->wal_len;
    for (size_t i = 0; i < n; i++, p += WAL_ROW) {
        uint32_t id = series ? series[i] : cs;
        uint64_t x = slot_get(v, w, i);
        memcpy(p, &id, 4);
        memcpy(p + 4, &ts[i], 8);
        memcpy(p + 12, &x, 8);
    }
    db->wal_len = need;
    if (eng_test_mutant == MUT_LEADER_ADMIT_AFTER) /* planted: the refusal comes with the batch already buffered */
        return leader_admit(db, *fno0, (uint32_t)n);
    return 0;
}

/* A marker record: appended to the same buffer as the rows, in order, and made durable before it returns (markers
 * are rare). body_len is a multiple of 4 (the record is 4-byte aligned). */
typedef struct wal_mrec {
    uint16_t type, flags;
    uint32_t span;
    const void *body;
    size_t body_len;
} wal_mrec;

/* Appends the marker records r[0..n) to the WAL buffer, consecutive, as one unit: buf_lock held. One admission covers
 * all of them (their first number and total span), after everything that can fail, so either every record is buffered
 * or none is: a leader never has the first markers of a batch in its log without the rest (a batch's dictionary
 * records then go to their file all or none). *fno gets the first record's number, *lsn_out the last position (the
 * commit target, wal_commit). */
static int wal_markers_locked(eng *db, const wal_mrec *r, size_t n, uint64_t *fno, uint64_t *lsn_out)
{
    uint64_t span = 0;
    size_t bytes = 0;
    for (size_t i = 0; i < n; i++) {
        if (r[i].span == 0 || r[i].body_len % 4 != 0 || r[i].body_len > ((size_t)1 << 27))
            return TSDB_ERR_INVALID;
        /* E17: a marker the volume's own next open would refuse (GC without requires=ec, ...) is never logged; a
         * write that tried fails here, not the restart */
        if (!mk_type_admitted(r[i].type, db->ec_req, db->tier_req) && mk_test_mutant != MK_MUT_WRITE_ANY)
            return TSDB_ERR_INVALID;
        span += r[i].span;
        bytes += WAL_HDR + WAL_MARKER_HDR + r[i].body_len;
    }
    if (!db->wal_enabled || n == 0 || span > UINT32_MAX || bytes > ((size_t)1 << 30))
        return TSDB_ERR_INVALID;
    int rc = eng_check(db);
    if (rc == 0 && db->fno_next >= FNO_BOUND_MAX - span) {
        eng_fail(db, "WAL numbers", EOVERFLOW);
        rc = TSDB_ERR_IO;
    }
    if (rc == 0 && db->wal_len + bytes > db->wal_cap) {
        size_t cap = MAX(db->wal_len + bytes, db->wal_cap * 2);
        cap = MAX(cap, (size_t)65536);
        uint8_t *b = realloc(db->wal_buf, cap);
        if (!b)
            rc = TSDB_ERR_NOMEM;
        else
            db->wal_buf = b, db->wal_cap = cap;
    }
    uint64_t lsn = 0;
    if (rc == 0) {
        eng_admit_marker = r[0].type;
        rc = leader_admit(db, db->fno_next, (uint32_t)span); /* last: nothing below fails */
        eng_admit_marker = 0;
    }
    if (rc == 0) {
        wal_rec_close(db); /* the marker follows the rows record: it takes no more rows */
        if (fno)
            *fno = db->fno_next;
        for (size_t i = 0; i < n; i++) {
            size_t plen = WAL_MARKER_HDR + r[i].body_len;
            uint8_t *h = db->wal_buf + db->wal_len;
            memset(h, 0, WAL_HDR);
            store32(h, (uint32_t)plen | (uint32_t)WAL_KIND_MARKER << 28);
            memcpy(h + 16, &db->fno_next, 8);
            memcpy(h + WAL_HDR, &r[i].type, 2);
            memcpy(h + WAL_HDR + 2, &r[i].flags, 2);
            store32(h + WAL_HDR + 4, r[i].span);
            if (r[i].body_len)
                memcpy(h + WAL_HDR + WAL_MARKER_HDR, r[i].body, r[i].body_len);
            db->fno_next += r[i].span;
            db->wal_len += WAL_HDR + plen;
        }
        lsn = db->fno_next - 1; /* its last position (eng_leader_commit) */
        atomic_store_explicit(&db->wal_appended, lsn, memory_order_release);
    }
    *lsn_out = lsn;
    return rc;
}

/* Appends one marker record to the WAL buffer: buf_lock held. *lsn is its commit target (wal_commit). */
static int wal_marker_locked(eng *db, uint16_t type, uint16_t flags, uint32_t span, const void *body, size_t body_len,
                             uint64_t *fno, uint64_t *lsn_out)
{
    wal_mrec r = {.type = type, .flags = flags, .span = span, .body = body, .body_len = body_len};
    return wal_markers_locked(db, &r, 1, fno, lsn_out);
}

int wal_marker(eng *db, uint16_t type, uint16_t flags, uint32_t span, const void *body, size_t body_len,
               uint64_t *fno)
{
    if (!db->wal_enabled || span == 0 || body_len % 4 != 0 || body_len > ((size_t)1 << 27))
        return TSDB_ERR_INVALID;
    uint64_t lsn = 0;
    pthread_mutex_lock(&db->buf_lock);
    int rc = wal_marker_locked(db, type, flags, span, body, body_len, fno, &lsn);
    pthread_mutex_unlock(&db->buf_lock);
    return rc ? rc : wal_commit(db, lsn);
}

int wal_marker_mk(eng *db, const mk *m, uint64_t *fno)
{
    uint64_t last = 0;
    return wal_markers_mk(db, m, 1, fno, &last);
}

int wal_markers_mk(eng *db, const mk *m, size_t n, uint64_t *fno, uint64_t *last)
{
    return wal_markers_mk_at(db, m, n, fno, last, NULL, NULL);
}

/* `at`, when given, runs under buf_lock once the numbers are assigned, before the records can reach the file (and so
 * before anyone can read them by position): a leader notes there what a reader of the marker may ask for. It takes
 * only leaf locks (lead_mu) and cannot fail. */
int wal_markers_mk_at(eng *db, const mk *m, size_t n, uint64_t *fno, uint64_t *last, void (*at)(eng *, uint64_t, void *),
                      void *arg)
{
    if (n == 0)
        return TSDB_ERR_INVALID;
    uint8_t **enc = calloc(n, sizeof *enc);
    wal_mrec *r = calloc(n, sizeof *r);
    int rc = enc && r ? 0 : TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n && rc == 0; i++) {
        size_t len;
        rc = mk_encode(&m[i], &enc[i], &len);
        if (rc == 0) {
            r[i] = (wal_mrec){.type = m[i].type, .flags = m[i].flags, .body = enc[i] + MK_HDR, .body_len = len - MK_HDR};
            memcpy(&r[i].span, enc[i] + 4, 4);
        }
    }
    uint64_t lsn = 0;
    if (rc == 0) {
        pthread_mutex_lock(&db->buf_lock);
        rc = wal_markers_locked(db, r, n, fno, &lsn);
        if (rc == 0 && at)
            at(db, *fno, arg);
        pthread_mutex_unlock(&db->buf_lock);
    }
    for (size_t i = 0; enc && i < n; i++)
        free(enc[i]);
    free(enc);
    free(r);
    if (rc)
        return rc; /* nothing buffered */
    *last = lsn;
    return wal_commit(db, lsn); /* one sync for the batch */
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

_Atomic eng_wal_sync_fn eng_wal_sync_obs;

void eng_set_wal_sync_observer(void (*fn)(int64_t ns))
{
    atomic_store_explicit(&eng_wal_sync_obs, fn, memory_order_release);
}

/* Writes a record of the rows in buf (WAL_HDR spare bytes first) to fd and
 * syncs it. On failure the engine is failed (eng_fail) before it returns:
 * the caller holds the writer role and has not woken anyone yet. */
void (*wal_test_sync_hook)(eng *db);

/* `last`: the last position in buf (a leader's bytes callback gets it; the first is in the first record's header). */
static int wal_write_sync(eng *db, int fd, uint64_t seq, uint64_t *off, uint8_t *buf, size_t len, uint64_t last)
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
    /* a leader hands the bytes to its sender before it syncs (design-cluster.md 5.1 step 2): the replicas write and
     * sync them while it does, and the commit point needs only both */
    const struct jr_ops *jr = eng_jr(db);
    bool bytes = jr && jr->bytes;
    uint64_t first = 0;
    if (len >= WAL_HDR) {
        memcpy(&first, buf + 16, 8);
        wsegs_add(db, seq, first, last);
    }
    if (bytes && eng_test_mutant != MUT_LEADER_BYTES_AFTER_SYNC)
        jr->bytes(jr->ctx, db, seq, *off, buf, len, first, last);
    void (*sh)(eng *) = __atomic_load_n(&wal_test_sync_hook, __ATOMIC_RELAXED);
    if (sh)
        sh(db);
    if (db->opt.fsync) {
        eng_wal_sync_fn obs = atomic_load_explicit(&eng_wal_sync_obs, memory_order_acquire);
        int64_t t0 = obs ? eng_mono_ns() : 0;
        if (tsdb_fdatasync(fd) != 0) {
            eng_fail(db, "WAL fdatasync", errno);
            return TSDB_ERR_IO;
        }
        if (obs)
            obs(eng_mono_ns() - t0);
        atomic_fetch_add_explicit(&db->st_wal_syncs, 1, memory_order_relaxed);
    }
    if (bytes && eng_test_mutant == MUT_LEADER_BYTES_AFTER_SYNC) /* planted: shipped only after the sync */
        jr->bytes(jr->ctx, db, seq, *off, buf, len, first, last);
    atomic_fetch_add_explicit(&db->st_wal_bytes, len, memory_order_relaxed);
    /* the last record of the write: what the revalidation reads again */
    db->wal_last_seq = seq, db->wal_last_off = *off + last_pos, db->wal_last_len = len - last_pos;
    db->wal_last_hash = last_hash;
    *off += len;
    return 0;
}

/* The WAL writer role made the log durable up to `upto`: readers and waiters see it, and a leader's driver is told
 * (jr_ops.synced), in position order because the role is held by one thread at a time. */
static void wal_durable_set(eng *db, uint64_t upto)
{
    atomic_store(&db->wal_durable, upto);
    const struct jr_ops *jr = eng_jr(db);
    if (jr && jr->synced && eng_test_mutant != MUT_LEADER_NO_SYNCED)
        jr->synced(jr->ctx, db, upto);
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
        uint64_t upto = atomic_load_explicit(&db->wal_appended, memory_order_relaxed);
        int fd = db->wal_fd;
        uint64_t seq = db->wal_seq;
        wal_rec_close(db);
        db->wal_buf = db->wal_spare;
        db->wal_cap = db->wal_spare_cap;
        db->wal_len = 0;
        pthread_mutex_unlock(&db->buf_lock);

        rc = wal_write_sync(db, fd, seq, &db->wal_off, buf, len, upto);
        db->wal_spare = buf;
        db->wal_spare_cap = cap;
        if (rc == 0)
            wal_durable_set(db, upto);

        pthread_mutex_lock(&db->wal_sync_lock);
        db->wal_writing = false;
        pthread_cond_broadcast(&db->wal_cv);
    }
    pthread_mutex_unlock(&db->wal_sync_lock);
    return rc;
}

/* ------------------------------------------------------- a leader's commit point (internal.h, eng_leader_attach) */

/* set by mem_insert around its backpressure flush: the flush gate's wait holds a (server) writer's thread */
static __thread bool gate_hook;

int eng_leader_attach(eng *db, const struct jr_ops *ops)
{
    if (!db->wal_enabled || db->opt.wal_sync_ms != 0 || db->rp || db->opt.read_only)
        return TSDB_ERR_INVALID;
    if (!db->replica_req) { /* the same rule as a replica's open (db.c): only a directory made for replication */
        fprintf(stderr, "tsdb: %s: META does not say requires=replica: not attached as a replicated leader\n",
                db->dir ? db->dir : "?");
        return TSDB_ERR_INVALID;
    }
    int rc = db->lrs ? 0 : leader_rs_open(db); /* RSTATE before the first marker */
    if (rc)
        return rc;
    /* an earlier leadership's links, or a promoted replica's stages: no marker here needs them; keep_ files are served */
    leader_links_sweep(db, eng_test_mutant != MUT_LEADER_NO_SWEEP);
    pthread_mutex_lock(&db->lock); /* rows of positions below the log end at the attach are in no wtail entry */
    pthread_mutex_lock(&db->buf_lock);
    db->wtail_from = MAX(db->wtail_from, db->fno_next);
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    __atomic_store_n(&db->jr, ops, __ATOMIC_RELEASE); /* the flusher, warmer and merges are running already */
    return 0;
}

void eng_leader_replay_floor(eng *db, uint64_t floor) { atomic_store(&db->lead_floor, floor); }

void eng_leader_commit(eng *db, uint64_t pos)
{
    pthread_mutex_lock(&db->lead_mu);
    if (pos > atomic_load(&db->lead_commit)) /* never back: the core's commit point does not go back either */
        atomic_store(&db->lead_commit, pos);
    pthread_cond_broadcast(&db->lead_cv);
    pthread_mutex_unlock(&db->lead_mu);
}

uint64_t eng_wal_durable_fno(eng *db) { return atomic_load(&db->wal_durable); }

void eng_leader_depose(eng *db)
{
    /* no buf_lock: the driver may depose from admit (under buf_lock) or under its core mutex (below buf_lock) */
    atomic_store(&db->lead_deposed, true);
    pthread_mutex_lock(&db->lead_mu);
    pthread_cond_broadcast(&db->lead_cv);
    pthread_mutex_unlock(&db->lead_mu);
}

int eng_leader_wait(eng *db, uint64_t pos, bool hook)
{
    if (atomic_load(&db->lead_commit) >= pos)
        return 0;
    void (*fn)(void *, int) = NULL;
    void *ctx = NULL;
    if (hook) {
        pthread_mutex_lock(&db->buf_lock);
        fn = db->wait_fn, ctx = db->wait_ctx;
        pthread_mutex_unlock(&db->buf_lock);
        if (fn)
            fn(ctx, 1);
    }
    int rc = 0;
    pthread_mutex_lock(&db->lead_mu);
    while (atomic_load(&db->lead_commit) < pos) {
        /* eng_fail and eng_leader_depose broadcast under lead_mu after their store: no wake-up is lost */
        if (atomic_load(&db->lead_deposed) || eng_check(db)) {
            rc = TSDB_ERR_INDETERMINATE;
            break;
        }
        pthread_cond_wait(&db->lead_cv, &db->lead_mu);
    }
    pthread_mutex_unlock(&db->lead_mu);
    if (fn)
        fn(ctx, -1);
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

void jr_serial_lock(eng *db)
{
    pthread_mutex_lock(&db->jr_serial.mu);
    uint64_t t = db->jr_serial.next++;
    if (eng_test_mutant == MUT_JR_BARGE) /* a plain mutex: whoever finds it free takes it, a waiter just woken or not */
        while (db->jr_serial.held)
            pthread_cond_wait(&db->jr_serial.cv, &db->jr_serial.mu);
    else
        while (db->jr_serial.serving != t)
            pthread_cond_wait(&db->jr_serial.cv, &db->jr_serial.mu);
    db->jr_serial.held = true;
    pthread_mutex_unlock(&db->jr_serial.mu);
}

void jr_serial_unlock(eng *db)
{
    pthread_mutex_lock(&db->jr_serial.mu);
    if (!db->jr_serial.held) {
        /* serving past next would make every later turn wait forever: every large write, drop and settle of the
         * volume, with no line. A plain mutex took this unlock quietly (undefined, harmless in glibc) */
        db->jr_serial.unheld++;
        pthread_mutex_unlock(&db->jr_serial.mu);
        fprintf(stderr, "tsdb: %s: jr_serial let go of with no turn taken (a bug): ignored\n", db->dir);
        return;
    }
    db->jr_serial.held = false;
    db->jr_serial.serving++;
    pthread_cond_broadcast(&db->jr_serial.cv); /* few wait (large writes, a drop, a settle): the next ticket goes */
    pthread_mutex_unlock(&db->jr_serial.mu);
}

bool lw_enter(eng *db, lw_fence *f)
{
    /* a leader of a replicated volume: marker order is block order (design-cluster.md 5.2). db->jr is set by
     * eng_leader_attach after the engine's threads run, so lw_leave goes by this decision, not by db->jr again */
    f->serial = eng_jr(db) != NULL;
    if (f->serial)
        jr_serial_lock(db);
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
    if (f->serial)
        jr_serial_unlock(db);
    f->serial = false;
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

/* Records newly-registered series in m that are still undecided for partition classes (buf_lock held).
 * On OOM growing fresh_ids, sets fresh_overflow so cls_classify_active safely falls back to scanning m->ids. */
static void mt_note_fresh(eng *db, memtable *m, uint32_t nids0)
{
    if (m->fresh_overflow)
        return;
    for (uint32_t i = nids0; i < m->nids; i++) {
        uint32_t id = m->ids[i];
        if (cls_byte(db, id) & CLS_DECIDED)
            continue;
        if (m->nfresh == m->fresh_cap) {
            if (m->fresh_cap > UINT32_MAX / 2) {
                m->fresh_overflow = true;
                return;
            }
            uint32_t c = m->fresh_cap ? m->fresh_cap * 2 : 32;
            uint32_t *p = realloc(m->fresh_ids, (size_t)c * sizeof *p);
            if (!p) {
                m->fresh_overflow = true;
                return;
            }
            m->fresh_ids = p;
            m->fresh_cap = c;
        }
        m->fresh_ids[m->nfresh++] = id;
    }
}

int mem_insert(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n,
               int type)
{
    /* Backpressure when the flusher falls behind: before the append, never after. A flush that fails (no memory,
     * no space for its tmp part) then refuses a write that is not in the WAL and not in the memtable, a definite
     * failure; after the append the same error would be the result of a write that is visible and replays. While
     * flushes keep failing every write is refused here, so the memtable stops at the size it had. No lock is held,
     * as for the flush this replaces; the flush takes flush_lock and the WAL writer role, which this writer does
     * not hold, having appended nothing. */
    if (db->rp) /* a replica's rows come from the leader's log, as records (replica.c) */
        return TSDB_ERR_READONLY;
    bool want_col = false;
    if (db->wal_enabled && n >= WAL_COL_MIN_ROWS) {
        uint32_t rmax = atomic_load_explicit(&wal_rec_max_rows, memory_order_relaxed);
        if (eng_jr(db))
            rmax = MIN(rmax, atomic_load_explicit(&jr_rec_max_rows, memory_order_relaxed));
        want_col = n < rmax;
    }
    struct wal_enc_slot *eslot = NULL;
    uint8_t *pre_rec = NULL;
    bool pre_owned = false;
    size_t pre_len = 0;
    pthread_mutex_lock(&db->buf_lock);
    if (db->active->rows >= 2 * db->opt.buffer_rows) {
        pthread_mutex_unlock(&db->buf_lock);
        int frc = eng_abandoned(); /* a parked request starts no flush on a stuck volume */
        if (frc == 0) {
            gate_hook = true; /* this thread may be a server's: its wait in the flush gate is reported */
            frc = leader_unappended(mt_flush(db)); /* a deposed leader's flush: this write was not appended */
            gate_hook = false;
        }
        if (frc)
            return frc;
        if (want_col)
            pre_len = wal_col_encode(db, series, cs, ts, v, w, n, &eslot, &pre_rec, &pre_owned);
        pthread_mutex_lock(&db->buf_lock);
    } else if (want_col) {
        /* Encode large WAL batches into a compressed columnar WAL_KIND_COL record OUTSIDE buf_lock. */
        pthread_mutex_unlock(&db->buf_lock);
        pre_len = wal_col_encode(db, series, cs, ts, v, w, n, &eslot, &pre_rec, &pre_owned);
        pthread_mutex_lock(&db->buf_lock);
    }
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
    if (rc == 0 && db->bg_error) { /* a store only when there is something to clear: it is read on every call */
        rc = db->bg_error;
        db->bg_error = 0;
    }
    /* before the WAL append, never after: an abandoned batch in the WAL would replay after a crash, and a refused
     * write would become visible. After the fence wait, so a request parked behind a large write sees the flag. */
    if (rc == 0)
        rc = eng_abandoned();
    /* wal_appended is the last position the WAL holds: a refusal before the append adds none, or a flush would wait
     * for rows that never come (wal_durable never reaches them) */
    uint32_t nids0 = db->active->nids;
    if (rc == 0)
        rc = mt_reserve_rows(db->active, series, cs, n); /* before the WAL: after it, nothing may fail */
    if (rc == 0 && db->cls_on && db->active->nids > nids0)
        mt_note_fresh(db, db->active, nids0);
    bool counted = rc == 0;
    uint64_t fno0 = 0;
    if (rc == 0 && db->wal_enabled) {
        rc = pre_len ? wal_buffer_pre(db, pre_rec, pre_len, n, &fno0)
                     : wal_buffer(db, series, cs, ts, v, w, n, &fno0); /* can fail without side effects */
        counted = rc == 0;
        if (rc && series)
            mt_unreserve(db->active, series, n); /* no mt_add clears the counts */
    }
    uint64_t app = atomic_load_explicit(&db->wal_appended, memory_order_relaxed);
    if (rc == 0) {
        const uint64_t f0 = db->wal_enabled ? fno0 : app + 1;
        rc = mt_add(db->active, series, cs, ts, v, w, n, f0, type);
        if (rc == 0)
            eng_ppos_rows(db, db->active, PPOS_SITE_INSERT, ts, n, f0, NULL);
        if (rc && db->wal_enabled)
            eng_fail(db, "adding a write the WAL holds to the memtable", ENOMEM); /* cannot happen: room reserved */
    }
    /* the memtable's position range: its first number now (a read, a store only for its first rows), its last one
     * when it is sealed (mt_flush: db->fno_rows_hi), not a store of both on the memtable's line with every call */
    if (rc == 0 && fno0 && !db->active->fno_lo)
        db->active->fno_lo = fno0;
    uint64_t lsn = app;
    if (counted) { /* the last position: with the WAL on, the batch's last fno (the leader's commit point is one too) */
        lsn = db->wal_enabled ? fno0 + n - 1 : app + n;
        atomic_store_explicit(&db->wal_appended, lsn, memory_order_release);
    }
    size_t rows = db->active->rows;
    if (rows >= db->opt.buffer_rows)
        pthread_cond_signal(&db->flush_cv);
    pthread_mutex_unlock(&db->buf_lock);
    if (eslot)
        wal_enc_release(eslot);
    else if (pre_owned)
        free(pre_rec);
    if (announced)
        hook(hook_ctx, -1);
    if (rc == 0 && db->wal_enabled && db->opt.wal_sync_ms == 0) {
        rc = wal_commit(db, lsn); /* a failure is eng_fail: indeterminate, and the server exits */
        /* a leader's batch went to the replicas before its sync (jr_ops.bytes): a new leader may commit it, so the
         * client gets no answer (D12), never an error it could take for a refusal */
        if (rc != 0 && eng_jr(db) && counted)
            rc = TSDB_ERR_INDETERMINATE;
    }
    /* A leader acknowledges once the commit point passed the batch (C7): a failover keeps what it acknowledged. The
     * wait holds a server thread for a replica's round trip and fdatasync (about one fdatasync longer than a write on
     * a plain engine), so it is reported through the write wait hook like a fence wait: tsdbd starts another worker
     * rather than run out of them while writes wait for replicas. Deposed or failed: TSDB_ERR_INDETERMINATE (the batch
     * is in the log, and a replica may have it). */
    if (rc == 0 && eng_jr(db) && counted)
        rc = eng_leader_wait(db, lsn, true);
    return rc; /* an applied write is a success: the flush that backpressure wants comes before the next write */
}

int (*eng_test_retire_fail)(const char *name); /* test hook: errno to fail the unlink or rename of a segment, or 0 */

/* Moves segment s out of the engine's way: unlinked, or with the feed on renamed to fwal_<s> (the feed keeps it;
 * the engine looks only for wal_ names, so for it the segment is gone either way). A rename never replaces: a
 * name taken is a reused number or files put there by hand, and fails loudly like any failed unlink. ENOENT is the
 * caller's (already gone). */
static int wal_retire_name(eng *db, uint64_t s, bool feed, const fseg *info)
{
    char name[64], fname[64];
    wal_name(name, sizeof name, s);
    if (eng_test_retire_fail && (errno = eng_test_retire_fail(name)) != 0)
        return -1; /* planted failure (tests/test_feedstore.c) */
    if (!feed)
        return unlinkat(db->dirfd, name, 0);
    fwal_name(fname, sizeof fname, s);
    if (tsdb_rename_noreplace(db->dirfd, name, fname) != 0) {
        if (errno == EEXIST)
            fprintf(stderr, "tsdb: %s: %s exists already: a number of the log was used twice, or a file was put "
                            "there by hand; %s stays\n", db->dir, fname, name);
        return -1;
    }
    feed_index_add(db, s, info);
    return 0;
}

/* wal_removed moves on to seq, but never past a segment still on disk that an open would replay (wal_unlink_next, the
 * first one whose removal failed) or that is kept (db->lk, oldest first). flush_lock held (or the open). */
static void wal_removed_raise(eng *db, uint64_t seq)
{
    if (db->wal_unlink_next)
        seq = MIN(seq, db->wal_unlink_next - 1);
    if (db->nlk)
        seq = MIN(seq, db->lk[0].seq - 1);
    pthread_mutex_lock(&db->lock);
    db->wal_removed = MAX(db->wal_removed, seq);
    pthread_mutex_unlock(&db->lock);
}

/* Unlinks (with the feed on: renames to fwal_) the WAL segments up to seq, now in parts, and moves wal_removed on over
 * those that are gone. One whose unlink fails (an I/O error, say) stays, and so does every later one, to be tried
 * again at the next flush: wal_removed must not pass a segment that a reopen would replay (its empty parts and the
 * switches' bound in classes.c rely on it). Called with flush_lock held. */
static void wal_unlink_upto(eng *db, uint64_t seq, const fseg *info)
{
    if (db->wal_enabled) {
        bool feed = atomic_load(&db->feed_on); /* changed only under flush_lock */
        for (uint64_t s = db->wal_unlink_next ? db->wal_unlink_next : seq; s <= seq; s++) {
            const struct jr_ops *jr = eng_jr(db);
            if (jr && jr->seg_done) /* a leader's driver reads what it still has to send of it */
                jr->seg_done(jr->ctx, db, s);
            if (wal_retire_name(db, s, feed, s == seq ? info : NULL) != 0 && errno != ENOENT) {
                char name[64];
                wal_name(name, sizeof name, s);
                if (db->wal_unlink_next != s)
                    fprintf(stderr, "tsdb: removing %s failed (%s): it stays until a later flush removes it\n", name,
                            strerror(errno));
                db->wal_unlink_next = s;
                return;
            }
            wsegs_remove(db, s);
        }
        db->wal_unlink_next = 0;
    }
    wal_removed_raise(db, seq);
    if (atomic_load(&db->feed_on))
        feed_cleaner_wake(db);
}

/* Kept segments whose last position is at or below `bound` go, oldest first, FNO holding their end first. Their numbers
 * are not consecutive (a promotion jumps to epoch << 40), so each goes by its own name and never through
 * wal_unlink_next, which retries a run of consecutive ones: one whose removal fails stays at the head and is tried
 * again at the next call, and so is every later one. A run that the open or a flush failed to remove (wal_unlink_next)
 * below the head goes first. flush_lock held. */
static void lk_unlink_upto(eng *db, uint64_t bound)
{
    size_t n = 0;
    bool feed = atomic_load(&db->feed_on); /* changed only under flush_lock */
    while (!db->lk_stuck && n < db->nlk && db->lk[n].fno_end - 1 <= bound) {
        uint64_t s = db->lk[n].seq;
        if (db->wal_unlink_next && db->wal_unlink_next < s) {
            wal_unlink_upto(db, s - 1, NULL); /* in parts (the open replayed them, or a flush wrote them) */
            if (db->wal_unlink_next != 0)
                break;
        }
        if (fno_require(db, db->lk[n].fno_end) != 0)
            break; /* the engine failed */
        const struct jr_ops *jr = eng_jr(db);
        if (jr && jr->seg_done)
            jr->seg_done(jr->ctx, db, s);
        if (wal_retire_name(db, s, feed, NULL) != 0 && errno != ENOENT) {
            if (db->lk_failed != s) {
                char name[64];
                wal_name(name, sizeof name, s);
                fprintf(stderr, "tsdb: removing %s failed (%s): it stays until a later flush removes it\n", name,
                        strerror(errno));
            }
            db->lk_failed = s;
            break;
        }
        wsegs_remove(db, s);
        n++;
    }
    if (n == 0)
        return;
    uint64_t last = db->lk[n - 1].seq;
    leader_links_drop(db, db->lk[n - 1].fno_end); /* the files their COMMIT markers named go with them */
    leader_rs_prune(db, db->lk[n - 1].fno_end);   /* and the DROPs no retained record lies behind */
    memmove(db->lk, db->lk + n, (db->nlk - n) * sizeof *db->lk);
    db->nlk -= n;
    wal_removed_raise(db, last);
    if (feed)
        feed_cleaner_wake(db);
}

static uint64_t lk_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* The last position of the newest kept segment the caps let go (eng_leader_keep_cap), 0 none: the caps count from the
 * newest segment back, so what goes is always a prefix of lk[]. */
static uint64_t lk_cap_bound(eng *db)
{
    uint64_t maxb = atomic_load(&db->lead_cap_bytes), maxms = atomic_load(&db->lead_cap_ms);
    if ((!maxb && !maxms) || db->nlk == 0)
        return 0;
    uint64_t now = lk_now_ms(), sum = 0;
    for (size_t i = db->nlk; i-- > 0;) {
        sum += db->lk[i].bytes;
        if ((maxb && sum > maxb) || (maxms && now > db->lk[i].ms && now - db->lk[i].ms > maxms))
            return db->lk[i].fno_end - 1;
    }
    return 0;
}

/* A leader's kept segments go once their last position is at or below min(keep bound, commit point); the caps raise
 * the keep bound up to the commit floor the driver saved, never past it (a segment above it may hold records a new
 * leader rolls back to, design-cluster.md section 4 decision 13) */
static void leader_unlink_due(eng *db)
{
    uint64_t bound = atomic_load(&db->lead_keep), c = atomic_load(&db->lead_commit);
    uint64_t cap = lk_cap_bound(db);
    if (cap > bound)
        bound = MIN(cap, MAX(bound, atomic_load(&db->lead_saved)));
    bound = MIN(bound, c);
    if (eng_test_mutant == MUT_LEADER_KEEP_IGNORED) /* planted: a flushed segment goes at once */
        bound = UINT64_MAX - 1;
    lk_unlink_upto(db, bound);
}

static int lk_push(eng *db, uint64_t seq, uint64_t fno_end)
{
    if (db->nlk == db->caplk) {
        size_t c = db->caplk ? 2 * db->caplk : 16;
        void *p = realloc(db->lk, c * sizeof *db->lk);
        if (!p)
            return TSDB_ERR_NOMEM;
        db->lk = p;
        db->caplk = c;
    }
    char name[64];
    struct stat st;
    wal_name(name, sizeof name, seq);
    uint64_t bytes = fstatat(db->dirfd, name, &st, 0) == 0 ? (uint64_t)st.st_size : 0; /* gone: nothing to count */
    db->lk[db->nlk++] = (struct lkseg){seq, fno_end, bytes, lk_now_ms()};
    return 0;
}

static void leader_flushed(eng *db, uint64_t seq, uint64_t fno_end)
{
    /* RSTATE names it as holding nothing outside the parts before it can outlive a drop of its rows: a writer's open
     * neither replays nor removes it (fails the engine on error) */
    leader_rs_kept(db, seq, fno_end);
    if (lk_push(db, seq, fno_end) != 0) {
        db->lk_stuck = true; /* the segment stays; a later one must not go before it */
        return;
    }
    leader_unlink_due(db);
}

/* Never waits for flush_lock: a flush holds it while it waits for the commit point (flush_waiting, eng_leader_wait),
 * and the driver's thread that calls this is the one that moves that point. When a flush holds it, the bound is
 * stored and that flush's leader_flushed, or the next call, removes what is due. */
void eng_leader_keep(eng *db, uint64_t pos)
{
    eng_leader_keep_floor(db, pos, pos);
}

void eng_leader_keep_cap(eng *db, uint64_t max_bytes, uint64_t max_ms)
{
    atomic_store(&db->lead_cap_bytes, max_bytes);
    atomic_store(&db->lead_cap_ms, max_ms);
}

int eng_leader_keep_excess(eng *db, uint64_t normal_bytes, uint64_t normal_ms, uint64_t *excess_out)
{
    if (!normal_bytes && !normal_ms) {
        *excess_out = 0;
        return 0;
    }
    if (pthread_mutex_trylock(&db->flush_lock) != 0)
        return 1;
    uint64_t ex = 0;
    if (db->nlk > 0) {
        uint64_t now = lk_now_ms(), sum = 0;
        uint64_t saved = atomic_load(&db->lead_saved), commit = atomic_load(&db->lead_commit);
        uint64_t lim = MIN(saved, commit);
        for (size_t i = db->nlk; i-- > 0;) {
            sum += db->lk[i].bytes;
            if ((normal_bytes && sum > normal_bytes) ||
                (normal_ms && now > db->lk[i].ms && now - db->lk[i].ms > normal_ms)) {
                for (size_t j = 0; j <= i; j++)
                    if (db->lk[j].fno_end - 1 <= lim)
                        ex += db->lk[j].bytes;
                break;
            }
        }
    }
    pthread_mutex_unlock(&db->flush_lock);
    *excess_out = ex;
    return 0;
}

void eng_leader_clock(eng *db, uint64_t now_every_ms, uint64_t idle_seal_ms)
{
    uint64_t was = atomic_exchange(&db->lead_now_ms, now_every_ms);
    atomic_store(&db->lead_seal_ms, idle_seal_ms);
    if (was != now_every_ms) { /* the flusher's wait was computed without it */
        pthread_mutex_lock(&db->buf_lock);
        pthread_cond_signal(&db->flush_cv);
        pthread_mutex_unlock(&db->buf_lock);
    }
}

/* A NOW marker (eng_leader_clock), not waited for: a replica only reads the clock from it. Refused (not leading, below
 * min_isr) it is not in the log; appended with a failed sync, the WAL's rules failed the engine. */
int leader_now_marker(eng *db)
{
    if (!eng_jr(db) || db->rp || !atomic_load(&db->lead_now_ms))
        return 0;
    mk m = {.type = MK_NOW, .span = 1};
    m.u.now = (mk_now){.now_ms = eng_now_ms(db)};
    uint64_t fno = 0, last = 0;
    return wal_markers_mk(db, &m, 1, &fno, &last);
}

void eng_leader_keep_floor(eng *db, uint64_t pos, uint64_t floor)
{
    atomic_store(&db->lead_saved, MAX(pos, floor));
    atomic_store(&db->lead_keep, pos);
    leader_replay_floor(db); /* lrq_mu, a leaf: never waits for flush_lock */
    if (pthread_mutex_trylock(&db->flush_lock) != 0)
        return;
    leader_unlink_due(db);
    pthread_mutex_unlock(&db->flush_lock);
}

/* Writes every memtable waiting to be flushed, oldest first; one whose write
 * failed stays in the list to be retried. Called with flush_lock held.
 * When `retired` is provided, detached memtables are collected into it so the
 * caller can call mt_unref() after releasing flush_lock. */
static int flush_waiting_collect(eng *db, memtable **retired, size_t *nretired, size_t rcap)
{
    int rc = 0;
    for (size_t i = 0; i < db->nflushing && rc == 0;) {
        memtable *m = db->flushing[i]; /* only flush_lock holders change the list's members */
        part_list pl = {0};
        if ((rc = eng_check(db)) != 0) /* e.g. the MANIFEST rewrite in mt_flush failed */
            break;
        /* A leader writes rows into parts only once they are committed (C7): no part has fno_hi above the commit point,
         * and a deposed leader has nothing uncommitted outside its log, which the new leader's history cuts. Deposed:
         * TSDB_ERR_INDETERMINATE, and m stays in flushing (the retry rule). Holding flush_lock is safe: the commit
         * point needs no engine lock (eng_leader_wait). The write wait hook is told only for a small write's
         * backpressure flush (gate_hook): the flusher is not a server thread, and a large write's thread may be
         * counted already by a small write waiting on its fence (lw_fence held). */
        if (eng_jr(db) && db->wal_enabled && eng_test_mutant != MUT_LEADER_FLUSH_UNCOMMITTED &&
            (rc = eng_leader_wait(db, m->fno_end - 1, gate_hook && eng_test_mutant != MUT_LEADER_GATE_NOHOOK)) != 0)
            break;
        cls_freeze(db); /* one history from placing its rows until they are listed (classes.c) */
        if (db->ppos) /* the parts' positions from m's map (src/ppos.h), through parts_commit's ppos_record */
            ppos_flush_begin(db, m);
        rc = mt_write(db, m, &pl);
        struct cls_wtail *wt = rc == 0 && db->cls_on ? cls_wal_tail_new(m) : NULL;
        if (rc == 0) {
            pthread_mutex_lock(&db->publish_lock); /* numbered, named and listed in one step */
            db_pub_begin(db, pl.p, pl.n);
            rc = parts_commit(db, pl.p, pl.n);
            if (rc) {
                db_pub_end(db);
                pthread_mutex_unlock(&db->publish_lock);
            } else {
                jr_commit(db, JR_FLUSHED, &pl);
            }
        }
        if (db->ppos)
            ppos_flush_end(db, m);
        if (rc) {
            cls_thaw(db);
            free(wt);
            parts_discard(db, pl.p, pl.n);
            free(pl.p);
            break;
        }
        lt_sec lsec; /* the latest table folds the flush in the section that lists it and drops the memtable */
        lt_begin_flush(db, &lsec, pl.p, pl.n);
        lt_fold(db, &lsec, pl.p, pl.n);
        pthread_mutex_lock(&db->lock);
        for (size_t k = 0; k < pl.n; k++)
            db_parts_insert_locked(db, pl.p[k]);
        db_pub_end_locked(db);
        memmove(db->flushing + i, db->flushing + i + 1, (db->nflushing - i - 1) * sizeof *db->flushing);
        db->nflushing--;
        db->wal_flushed = MAX(db->wal_flushed, m->wal_seq); /* flushes go in segment order */
        struct cls_wtail *wdead = NULL;
        if (db->cls_on)
            cls_wal_tail_locked(db, wt, m->wal_seq, m->fno_end, m->max_ts, &wdead);
        pthread_cond_signal(&db->merge_cv);
        pthread_mutex_unlock(&db->lock);
        lt_end(db, &lsec);
        pthread_mutex_unlock(&db->publish_lock);
        cls_thaw(db);
        if (db->cls_on) /* past the cap, the oldest entries fold, built outside db->lock (classes.c) */
            cls_wal_tail_fold(db, wdead);
        int mrc = manifest_publish(db); /* fails the engine on error */
        free(pl.p);
        if (mrc == 0) /* the numbers of the segment stay used after it is gone (fno.c) */
            mrc = fno_require(db, m->fno_end);
        if (mrc == 0 && db->rp) /* a replica unlinks at or below its saved commit floor only (replica.c) */
            replica_flushed(db, m->wal_seq, m->fno_end);
        else if (mrc == 0 && eng_jr(db)) /* a leader keeps it for the replicas (eng_leader_keep) */
            leader_flushed(db, m->wal_seq, m->fno_end);
        else if (mrc == 0) {
            /* kept segments a writer's open found (RSTATE's `kept`), with no leader to keep them for: they go first,
             * all below the flushed one (wal_removed passes none still on disk) */
            if (db->nlk)
                lk_unlink_upto(db, UINT64_MAX - 1);
            wal_unlink_upto(db, m->wal_seq, NULL); /* else MANIFEST may still name it: a follower would tail it */
        }
        if (mrc == 0)
            mrc = cls_flush_guard(db, m); /* a series of a long class that got fast moves on now */
        if (retired && *nretired < rcap)
            retired[(*nretired)++] = m;
        else
            mt_unref(m);
        rc = mrc;
    }
    return rc;
}

int flush_waiting(eng *db) { return flush_waiting_collect(db, NULL, NULL, 0); }

void (*mt_test_before_seal)(eng *db);
void (*mt_test_after_wal_enter)(eng *db);

/* Turns the active memtable into parts. Rows stay visible throughout: the
 * memtable is moved to the flushing list and only leaves it together with
 * the publication of its parts. */
/* test hook (tests/test_failstop.c): 0 flushes as usual, INT_MIN returns 0 without flushing (a flusher that falls
 * behind), any other value is the flush's error, as a failed allocation or a tmp part that cannot be written gives */
int (*mt_test_flush_fail)(void);

int mt_flush(eng *db) { return mt_flush_ex(db, NULL, NULL); }

/* mt_flush; with `marker`, that marker record is also appended and made durable, as the first record of the segment
 * the flush starts (the rows before it are in the sealed segments the flush writes; nothing a writer appends after
 * it is flushed here): a replica that applies it has sealed and flushes exactly the memtables the leader flushed
 * (the DROP marker, design-cluster.md 5.2). With an empty active memtable there is no new segment: the marker
 * goes into the current one, and the replica checks that its memtable holds no rows then. *fno: its first number. */
static int mt_flush_do(eng *db, const mk *marker, uint64_t *fno, bool held);
int mt_flush_ex(eng *db, const mk *marker, uint64_t *fno) { return mt_flush_do(db, marker, fno, false); }
int mt_flush_held(eng *db, const mk *marker, uint64_t *fno) { return mt_flush_do(db, marker, fno, true); }

static int mt_flush_do(eng *db, const mk *marker, uint64_t *fno, bool held)
{
    if (db->opt.read_only)
        return 0;
    if (db->rp) /* the replica's rows live in the memtable of their leader segment; sealed ones flush (replica.c) */
        return replica_flush(db);
    int (*hook)(void) = __atomic_load_n(&mt_test_flush_fail, __ATOMIC_RELAXED); /* the tests set it while threads run */
    if (hook) {
        int hr = hook();
        if (hr)
            return hr == INT_MIN ? 0 : hr;
    }
    uint8_t *mbody = NULL; /* the marker's encoded payload */
    size_t mlen = 0;
    uint64_t mlsn = 0;
    if (marker) {
        int mrc = mk_encode(marker, &mbody, &mlen);
        if (mrc)
            return mrc;
    }
    memtable *retired[MAX_FLUSHING * 2 + 1];
    size_t nretired = 0;
    if (!held)
        pthread_mutex_lock(&db->flush_lock);
    /* a memtable whose write failed earlier holds older rows than the active
     * one: it is written first, and the active one after it all the same, or
     * a large insert that relies on this flush would be shadowed by it */
    int rc = flush_waiting_collect(db, retired, &nretired, MAX_FLUSHING * 2);
    if (rc == 0)
        rc = cls_classify_active(db); /* a leader's fresh series: their CLASS markers go before the seal */
    void (*seal_hook)(eng *) = __atomic_load_n(&mt_test_before_seal, __ATOMIC_ACQUIRE);
    if (rc == 0 && seal_hook)
        seal_hook(db);
    /* Pre-allocate the next memtable and open/dir-sync the next WAL segment before taking the
     * WAL writer role: flush_lock already prevents db->wal_seq and db->nflushing from changing,
     * so concurrent wal_commit() callers are not stalled on openat() / fsync(dirfd). */
    uint64_t seq = db->wal_seq + 1;
    memtable *nm = NULL;
    int newfd = -1;
    if (rc == 0 && eng_check(db) == 0 && db->nflushing == 0) {
        pthread_mutex_lock(&db->buf_lock);
        bool pre_empty = db->active->rows == 0;
        pthread_mutex_unlock(&db->buf_lock);
        if (!pre_empty) {
            nm = mt_new(seq);
            if (!nm)
                rc = TSDB_ERR_NOMEM;
            else if (db->wal_enabled) {
                rc = wal_open_segment(db, seq, &newfd);
                if (rc) {
                    mt_unref(nm);
                    nm = NULL;
                }
            }
        }
    }
    bool rotated = false;
    if (rc == 0) {
        wal_writer_enter(db); /* no commit may write to the segment being retired */
        void (*after_enter)(eng *) = __atomic_load_n(&mt_test_after_wal_enter, __ATOMIC_ACQUIRE);
        if (UNLIKELY(after_enter != NULL))
            after_enter(db);
        pthread_mutex_lock(&db->buf_lock);
        bool empty = db->active->rows == 0;
        if (marker && empty && eng_check(db) == 0) { /* in the same hold as the check: no row slips in before it */
            uint32_t span;
            memcpy(&span, mbody + 4, 4);
            rc = wal_marker_locked(db, marker->type, marker->flags, span, mbody + MK_HDR, mlen - MK_HDR, fno, &mlsn);
            marker = NULL; /* done */
        }
        pthread_mutex_unlock(&db->buf_lock);
        if (rc == 0)
            rc = eng_check(db); /* rows of a failed engine may not be durable: none become parts */
        if (rc == 0 && !empty && db->nflushing == 0) {
            /* rare case: active was empty at pre-peek and a row arrived before wal_writer_enter */
            if (!nm) {
                nm = mt_new(seq);
                if (!nm)
                    rc = TSDB_ERR_NOMEM;
                else if (db->wal_enabled)
                    rc = wal_open_segment(db, seq, &newfd);
            }
            if (rc == 0) {
                pthread_mutex_lock(&db->lock);
                pthread_mutex_lock(&db->buf_lock);
                memtable *m = db->active;
                db->active = nm;
                nm = NULL;
                if (m->fno_lo) /* it took rows records, and the last of them is the last the WAL took (all under buf_lock) */
                    m->fno_hi = MAX(m->fno_hi, db->fno_rows_hi);
                if (eng_test_mutant != MUT_FLUSH_NOCLOSE) /* planted: the tail keeps a header with no count (test_fno) */
                    wal_rec_close(db);
                uint8_t *buf = db->wal_buf;
                size_t cap = db->wal_cap, len = db->wal_len;
                uint64_t upto = atomic_load_explicit(&db->wal_appended, memory_order_relaxed);
                int oldfd = db->wal_fd;
                uint64_t oldseq = db->wal_seq, oldoff = db->wal_off;
                db->wal_off = 0;
                /* recycle wal_spare as wal_commit does: avoid discarding and re-faulting wal_buf under buf_lock */
                db->wal_buf = db->wal_spare;
                db->wal_cap = db->wal_spare_cap;
                db->wal_len = 0;
                db->wal_spare = NULL;
                db->wal_spare_cap = 0;
                m->fno_end = db->fno_next; /* the end of the old segment: FNO holds it before the segment goes */
                db->wal_fd = newfd;
                newfd = -1;
                db->wal_seq = seq;
                db->flushing[db->nflushing++] = m;
                int mrc = 0; /* the marker's own result: the old segment's write below must not hide a refusal (admit) */
                if (marker) { /* the first record of the new segment */
                    uint32_t span;
                    memcpy(&span, mbody + 4, 4);
                    mrc = wal_marker_locked(db, marker->type, marker->flags, span, mbody + MK_HDR, mlen - MK_HDR, fno,
                                            &mlsn);
                    marker = NULL;
                }
                pthread_mutex_unlock(&db->buf_lock);
                pthread_mutex_unlock(&db->lock);
                if (db->wal_enabled) {
                    /* the rest of the old segment; a part will hold these rows, but
                     * until then the segment is what makes them durable */
                    rc = wal_write_sync(db, oldfd, oldseq, &oldoff, buf, len, upto);
                    close(oldfd);
                    if (rc == 0)
                        wal_durable_set(db, upto);
                }
                db->wal_spare = buf;
                db->wal_spare_cap = cap;
                rotated = true;
                /* a marker refused here (a leader's admission: min_isr, not leading, deposed) was not appended: the call
                 * fails, the sealed memtable stays in flushing for the next flush (a DROP never retires without its
                 * marker) */
                if (rc == 0 && eng_test_mutant != MUT_LEADER_MARKER_RC)
                    rc = mrc;
            }
        }
        if (marker && rc == 0)
            rc = TSDB_ERR_IO; /* no marker was appended (a memtable still waiting to flush): the caller must not go on */
        wal_writer_leave(db);
    }
    if (nm)
        retired[nretired++] = nm;
    if (newfd >= 0) {
        close(newfd);
        if (!eng_check(db)) {
            char name[64];
            wal_name(name, sizeof name, seq);
            (void)unlinkat(db->dirfd, name, 0);
        }
    }
    if (rotated)
        /* Publish MANIFEST outside wal_writing (still serialized under flush_lock, preserving
         * flush_lock > manifest_lock > db->lock > buf_lock). A concurrent sync-mode append into
         * the new segment may return before MANIFEST advertises wal_active = seq; explicit
         * tsdb_flush() still publishes before returning, and followers poll asynchronously
         * (docs/ha.md §7). */
        manifest_publish(db);

    if (rc == 0)
        rc = flush_waiting_collect(db, retired, &nretired, MAX_FLUSHING * 2);
    if (!held)
        pthread_mutex_unlock(&db->flush_lock);
    for (size_t i = 0; i < nretired; i++)
        mt_unref(retired[i]);
    free(mbody);
    if (mlsn && rc == 0)
        rc = wal_commit(db, mlsn); /* the marker is durable before the caller goes on */
    else if (mlsn)
        wal_commit(db, mlsn);
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
        if (db->opt.wal_sync_ms > 0) {
            int64_t sync_due = db->last_wal_sync_ms + db->opt.wal_sync_ms; /* the clock starts at the open (db.c) */
            due = MIN(due, sync_due);
            if (atomic_load_explicit(&db->st_sync_due0, memory_order_relaxed) == 0)
                atomic_store_explicit(&db->st_sync_due0, sync_due, memory_order_relaxed);
        }
        /* a leader's clock duties (eng_leader_clock): the next NOW marker, the idle seal of the active memtable */
        uint64_t now_every = atomic_load(&db->lead_now_ms), seal_ms = atomic_load(&db->lead_seal_ms);
        bool lead = eng_jr(db) && !db->rp;
        if (lead && (db->active->rows != db->idle_rows || db->active->wal_seq != db->idle_seq)) {
            db->idle_rows = db->active->rows; /* seen changed: idle from now on (at most a wake late) */
            db->idle_seq = db->active->wal_seq;
            db->idle_since = mono_ms();
        }
        if (lead && now_every)
            due = MIN(due, db->lead_now_next);
        if (lead && seal_ms && db->active->rows)
            due = MIN(due, db->idle_since + (int64_t)seal_ms);
        int64_t wait = MAX(due - mono_ms(), (int64_t)1);
        tsdb_cond_timedwait_ms(&db->flush_cv, &db->buf_lock, wait);
        if (db->flusher_stop)
            break;
        int64_t now = mono_ms();
        bool flush = db->active->rows &&
                     (db->active->rows >= db->opt.buffer_rows || now - db->active->created_ms >= interval);
        bool sync = db->opt.wal_sync_ms > 0 && now - db->last_wal_sync_ms >= db->opt.wal_sync_ms;
        uint64_t target = atomic_load_explicit(&db->wal_appended, memory_order_relaxed);
        lead = eng_jr(db) && !db->rp;
        now_every = atomic_load(&db->lead_now_ms);
        seal_ms = atomic_load(&db->lead_seal_ms);
        bool idle = lead && seal_ms && db->active->rows && db->active->rows == db->idle_rows &&
                    db->active->wal_seq == db->idle_seq && now - db->idle_since >= (int64_t)seal_ms;
        bool now_mark = lead && now_every && (now >= db->lead_now_next || idle);
        if (now_mark)
            db->lead_now_next = now + (int64_t)now_every;
        flush = flush || idle;
        pthread_mutex_unlock(&db->buf_lock);
        if (db->rp && !flush) { /* a replica's memtable of a finished leader segment waits to be written; nflushing is
                                   db->lock's, which the flusher may not take under buf_lock */
            pthread_mutex_lock(&db->lock);
            flush = db->nflushing > 0;
            pthread_mutex_unlock(&db->lock);
        }
        int rc = eng_check(db); /* a failed engine writes nothing more */
        if (rc == 0 && sync) {
            rc = wal_commit(db, target);
            uint64_t k = atomic_load_explicit(&db->st_periodic_syncs, memory_order_relaxed);
            if (k < 2)
                atomic_store_explicit(&db->st_sync_at[k], now, memory_order_relaxed);
            atomic_fetch_add_explicit(&db->st_periodic_syncs, 1, memory_order_release);
            db->last_wal_sync_ms = now;
        }
        if (rc == 0 && flush)
            rc = mt_flush(db);
        if (rc == 0 && now_mark) /* after an idle seal's flush: the first record of the new segment */
            leader_now_marker(db); /* refused or failed: no marker (a failed sync failed the engine) */
        if (rc == 0)
            graveyard_purge(db, false);
        pthread_mutex_lock(&db->buf_lock);
        if (rc && !db->bg_error && rc != TSDB_ERR_INDETERMINATE) /* a deposed leader refuses writes on its own */
            db->bg_error = rc;
        atomic_fetch_add_explicit(&db->st_flusher_rounds, 1, memory_order_release);
    }
    pthread_mutex_unlock(&db->buf_lock);
    return NULL;
}

/* ----------------------------------------------------------------- open */


int cmp_seg(const void *a, const void *b)
{
    uint64_t x = ((const wal_seg *)a)->seq, y = ((const wal_seg *)b)->seq;
    return (x > y) - (x < y);
}

/* Highest WAL sequence already stored in a partition's parts. Caller holds no locks; open is single threaded. */
uint64_t partition_wal_seq(eng *db, int64_t partition)
{
    uint64_t best = 0;
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == partition)
            best = MAX(best, db->parts[i]->wal_seq);
    return best;
}

int read_file(int dirfd, const char *name, uint8_t **out, size_t *len)
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
int wal_check_segment(eng *db, wal_seg *s)
{
    uint8_t *buf;
    size_t len, pos = 0, rec;
    wal_rec wr;
    int rc = read_file(db->dirfd, s->name, &buf, &len);
    if (rc)
        return rc;
    uint32_t count = eng_series_count(db);
    s->fno_first = s->fno_end = 0;
    while (rc == 0 && (rec = wal_record_x(buf + pos, len - pos, s->seq, pos, &wr, false)) != 0) {
        const uint8_t *r = wr.payload;
        if (wr.fno >= FNO_BOUND_MAX || wr.span >= FNO_BOUND_MAX - wr.fno) { /* numbers no writer could have given */
            fprintf(stderr, "tsdb: WAL segment %s: the record at offset %zu has number %" PRIu64 " (span %" PRIu64
                            "), beyond the limit of the log numbers: the log is damaged\n",
                    s->name, pos, wr.fno, wr.span);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (wr.fno < s->fno_end) { /* numbers never go back: a valid record that does is damage, not a torn tail */
            fprintf(stderr, "tsdb: WAL segment %s: the record at offset %zu has number %" PRIu64 ", below %" PRIu64
                            ", where the one before it ends: the log is damaged\n", s->name, pos, wr.fno, s->fno_end);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (!s->fno_first)
            s->fno_first = wr.fno;
        s->fno_end = wr.fno + wr.span;
        if (wr.kind == WAL_KIND_COL) {
            wal_col_rows cr = {0};
            rc = wal_col_decode(wr.payload, wr.n, NULL, &cr);
            if (rc != 0) {
                if (rc == TSDB_ERR_CORRUPT)
                    fprintf(stderr,
                            "tsdb: WAL segment %s: columnar record at offset %zu fails decode: the log is damaged\n",
                            s->name, pos);
                break;
            }
            if (!db->rp) {
                size_t ni = cr.ids ? cr.nrows : 1;
                for (size_t i = 0; i < ni; i++) {
                    uint32_t id = cr.ids ? cr.ids[i] : cr.cs;
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
            }
            wal_col_rows_free(&cr);
            if (rc)
                break;
        } else if (wr.kind == WAL_KIND_ROWS) {
            size_t n = wr.n;
            for (size_t i = 0; i < n; i++, r += WAL_ROW) {
                uint32_t id;
                int64_t ts;
                uint64_t slot;
                wal_row(r, &id, &ts, &slot);
                /* a replica's log runs ahead of its dictionaries: a SERIES marker above its commit floor creates the id */
                if (id >= count && !db->rp) {
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
int wal_check_order(const wal_seg *segs, size_t nseg, uint64_t parts_seq)
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
int wal_open_numbers(eng *db, const wal_seg *segs, size_t nseg)
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
    rc = fno_load(db, &fno, &gen, &slot, &torn);
    if (rc == TSDB_ERR_NOTFOUND && nseg == 0 && db->nparts == 0 && eng_series_count(db) == 0 &&
        faccessat(db->dirfd, "MANIFEST", F_OK, AT_SYMLINK_NOFOLLOW) != 0 &&
        faccessat(db->dirfd, "ROOT_ID", F_OK, AT_SYMLINK_NOFOLLOW) == 0) {
        /* A volume that never held a record: an add by a release that did not make FNO (0.9.0's, on an upgraded
         * database) left only META and ROOT_ID. Nothing here ever had a number, so the bound is 1: the one case where
         * a missing FNO is not lost data (MANIFEST is written at the end of the first open that gets here). */
        if ((rc = fno_create(db->dirfd, 1, db->opt.fsync)) == 0)
            rc = fno_load(db, &fno, &gen, &slot, &torn);
        if (rc == 0)
            fprintf(stderr, "tsdb: FNO was missing in a volume that holds no series, part or segment: made (numbers "
                            "start at 1)\n");
    }
    if (rc == TSDB_ERR_NOTFOUND) {
        fprintf(stderr, "tsdb: FNO is missing: the numbers of the log cannot be bounded, the open fails (a database "
                        "of this format has one in every volume; restore it from a backup)\n");
        return TSDB_ERR_CORRUPT;
    }
    if (rc)
        return rc;
    if (fno >= FNO_BOUND_MAX) {
        fprintf(stderr, "tsdb: FNO holds %" PRIu64 ", beyond the limit of the log numbers: the file is damaged, the open "
                        "fails (restore FNO from a backup)\n", fno);
        return TSDB_ERR_CORRUPT;
    }
    db->fno_slot = slot;
    db->fno_gen = gen;
    atomic_store(&db->fno_durable, fno);
    db->fno_next = MAX(next, fno);
    db->fno_torn_open = torn;
    if (torn) {
        /* A tear (a store the power loss cut) and damage at rest (rot, a misdirected write) look alike, and damage
         * may have hit the newest slot, whose segments are unlinked already: the number it held is lost, and the
         * other slot is older by any number of stores. So the numbers jump ahead (a gap, never a repeat). */
        db->fno_next += FNO_BELT;
        fprintf(stderr, "tsdb: FNO: one slot is torn or damaged: the other is used and the log numbers skip ahead by "
                        "%" PRIu64 " (the newer value may be lost)\n", (uint64_t)FNO_BELT);
    }
    if (eng_test_mutant == MUT_FNO_REUSE) { /* planted: numbers restart from what parts hold (tests/test_fno.c) */
        db->fno_next = 1;
        for (size_t i = 0; i < db->nparts; i++)
            if (db->parts[i]->fno_hi)
                db->fno_next = MAX(db->fno_next, db->parts[i]->fno_hi + 1);
    } else if (eng_test_mutant == MUT_FNO_MANIFEST_ONLY) { /* planted: MANIFEST's number is the bound, not FNO's */
        uint64_t m = 0;
        int mfd = openat(db->dirfd, "MANIFEST", O_RDONLY | O_CLOEXEC);
        FILE *f = mfd >= 0 ? fdopen(mfd, "r") : NULL;
        char line[128];
        while (f && fgets(line, sizeof line, f))
            if (sscanf(line, "fno_next %" SCNu64, &m) == 1)
                break;
        if (f)
            fclose(f);
        db->fno_next = MAX(m, 1);
        for (size_t i = 0; i < db->nparts; i++)
            if (db->parts[i]->fno_hi)
                db->fno_next = MAX(db->fno_next, db->parts[i]->fno_hi + 1);
        for (size_t k = 0; k < nseg; k++)
            db->fno_next = MAX(db->fno_next, segs[k].fno_end);
    }
    return 0;
}

/* A replayed segment about to be retained: cut at its last valid record when torn, then fdatasynced. */
static int wal_seal_replayed(eng *db, const char *name, size_t valid, size_t len)
{
    int fd = openat(db->dirfd, name, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "tsdb: %s: cannot open %s to seal it: %s\n", db->dir, name, strerror(errno));
        return TSDB_ERR_IO;
    }
    int rc = 0;
    if (valid < len && ftruncate(fd, (off_t)valid) != 0) {
        fprintf(stderr, "tsdb: %s: cutting the torn tail of %s failed: %s\n", db->dir, name, strerror(errno));
        rc = TSDB_ERR_IO;
    }
    if (rc == 0 && db->opt.fsync && eng_test_mutant != MUT_FEED_NOSEAL && /* planted: the rename without the sync */
        tsdb_fdatasync(fd) != 0) {
        fprintf(stderr, "tsdb: %s: fdatasync of %s failed: %s\n", db->dir, name, strerror(errno));
        rc = TSDB_ERR_IO;
    }
    close(fd);
    return rc;
}

int wal_open(eng *db, int (*before_replay)(eng *))
{
    bool ro = db->opt.read_only;
    wal_seg *segs = NULL;
    size_t nseg = 0, cap = 0;
    uint64_t max_fwal = 0;
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d); /* the dup shares its offset with dirfd, which an earlier scan moved */
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) { /* NULL with errno set is a read error, not the end */
        uint64_t seq;
        int consumed = 0;
        if (fwal_file_name(e->d_name) && sscanf(e->d_name, "fwal_%" SCNx64 ".log", &seq) == 1)
            max_fwal = MAX(max_fwal, seq); /* retained for the feed: a new segment must never take its number */
        if (!wal_file_name(e->d_name) || sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &seq, &consumed) != 1 ||
            e->d_name[consumed])
            continue;
        if (nseg == cap) {
            cap = cap ? cap * 2 : 16;
            wal_seg *s = realloc(segs, cap * sizeof *s);
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

    uint64_t max_seq = max_fwal;
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
    bool stuck = false;
    db->wal_open_removed = db->wal_seq - 1;
    for (size_t i = 0; i < db->nparts; i++)
        parts_seq = MAX(parts_seq, db->parts[i]->wal_seq);
    for (size_t k = 0; !ro && k < nseg && rc == 0; k++)
        rc = wal_check_segment(db, &segs[k]);
    if (rc == 0 && !ro)
        rc = wal_check_order(segs, nseg, parts_seq);
    if (rc == 0 && !ro)
        rc = wal_open_numbers(db, segs, nseg);
    if (rc == 0 && !ro)
        rc = feed_open_numbers(db); /* enable or renew FEED before the first FNO store (and so before the replay) */
    if (rc == 0 && before_replay)
        rc = before_replay(db);
    uint64_t kept = 0; /* segments up to it hold nothing outside the parts (a leader's or a promoted replica's) */
    if (rc == 0 && nseg)
        rc = rs_kept_seq(db, &kept);
    for (size_t k = 0; k < nseg && rc == 0; k++) {
        uint8_t *buf;
        size_t len;
        if (segs[k].seq <= kept) {
            /* not replayed: a drop after their flush took rows out of parts, which a replay would bring back; kept
             * for the leader's replicas (eng_leader_keep), removed by a writer that is not one at its next flush */
            if (!ro && (rc = lk_push(db, segs[k].seq, segs[k].fno_end ? segs[k].fno_end : 1)) == 0) {
                db->wal_open_removed = MIN(db->wal_open_removed, db->lk[0].seq - 1); /* wal_removed passes none */
                if (segs[k].fno_first)
                    wsegs_add(db, segs[k].seq, segs[k].fno_first,
                              segs[k].fno_end ? segs[k].fno_end - 1 : segs[k].fno_first);
            }
            continue;
        }
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
            wal_col_rows cr = {0};
            bool is_col = wr.kind == WAL_KIND_COL;
            if (is_col) {
                rc = wal_col_decode(wr.payload, wr.n, NULL, &cr);
                if (rc == TSDB_ERR_CORRUPT) /* a writer's open said so in wal_check_segment; a read-only one did not */
                    fprintf(stderr, "tsdb: WAL segment %s: the columnar record at offset %zu does not decode: the log "
                                    "is damaged\n", segs[k].name, pos);
                if (rc != 0)
                    break;
            }
            size_t n = wr.kind == WAL_KIND_ROWS ? wr.n : is_col ? cr.nrows : 0; /* a marker is skipped; its numbers are counted */
            if (eng_test_mutant == MUT_MARKER_ROWS && wr.kind == WAL_KIND_MARKER)
                n = wr.n / WAL_ROW; /* planted: a marker's payload replayed as rows (tests/test_fno.c) */
            if (wr.kind == WAL_KIND_MARKER && !mk_type_admitted(wr.mtype, db->ec_req, db->tier_req)) {
                /* E17: GC and DIGEST are an erasure-coded volume's (COLD a tier's too); without the word in META they
                 * are as unknown to this volume as to an older binary, which refuses the word: never skipped quietly.
                 * 14 and 15 are R10's, behind the same word, and this binary cannot apply them anywhere */
                const char *tn = mk_type_name(wr.mtype);
                fprintf(stderr, "tsdb: %s: WAL segment %s holds a marker of type %u (%s) at offset %zu, which this "
                                "volume does not admit (META lacks requires=ec or tier, or a later release wrote it): "
                                "the open refuses (META lost the word, or the segment is another volume's)\n",
                        db->dir, segs[k].name, (unsigned)wr.mtype, tn ? tn : "a later release's", pos);
                rc = TSDB_ERR_CORRUPT;
                break;
            }
            for (size_t i = 0; i < n && rc == 0; i++) {
                uint32_t id;
                int64_t ts;
                uint64_t slot;
                if (is_col) {
                    id = cr.ids ? cr.ids[i] : cr.cs;
                    ts = cr.ts[i];
                    slot = slot_get(cr.vals, cr.w, i);
                } else {
                    wal_row(r, &id, &ts, &slot);
                    r += WAL_ROW;
                }
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
                rc = mt_add(m, NULL, id, &ts, &slot, 8, 1, 0, -1);
                if (rc == 0) {
                    mt_note_fno(m, wr.fno + i, 1);
                    eng_ppos_rows(db, m, PPOS_SITE_REPLAY, &ts, 1, wr.fno + i, NULL);
                }
            }
            wal_col_rows_free(&cr);
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
            if (db->ppos)
                ppos_flush_begin(db, m);
            rc = mt_write(db, m, &pl);
            if (rc == 0)
                rc = parts_publish(db, pl.p, pl.n);
            if (db->ppos)
                ppos_flush_end(db, m);
            if (rc)
                parts_discard(db, pl.p, pl.n);
            free(pl.p);
        }
        if (!ro)
            mt_unref(m);
        if (rc == 0 && !ro) {
            /* With the feed on a segment with records is kept, as fwal_: cut at its last valid record and synced first
             * (a torn tail or page-cache bytes of a killed process must not become a "sealed" segment that a power
             * loss cuts); one without a record is unlinked. */
            bool keep = atomic_load(&db->feed_on) && pos > 0;
            if (keep) /* also after a rename that failed: the next flush renames this one too, as it is */
                rc = wal_seal_replayed(db, segs[k].name, pos, len);
            /* the numbers of the segment stay used once it is gone, markers included: FNO holds them first */
            if (rc == 0 && segs[k].fno_end)
                rc = fno_require(db, segs[k].fno_end);
            /* A segment that cannot be unlinked or renamed stays, and so does every later one (the retained set stays
             * contiguous): wal_removed must not pass it, or an empty merge output could go and the next open replay
             * the segment, deleted rows with it. The next flush tries again (wal_unlink_upto). */
            if (rc == 0 && !stuck && wal_retire_name(db, segs[k].seq, keep, NULL) != 0 && errno != ENOENT) {
                fprintf(stderr, "tsdb: %s: removing %s at the open failed (%s): it stays until a later flush removes "
                                "it\n", db->dir, segs[k].name, strerror(errno));
                stuck = true;
                db->wal_unlink_next = segs[k].seq;
                db->wal_open_removed = MIN(db->wal_open_removed, segs[k].seq - 1);
            }
            if (rc == 0 && stuck && segs[k].fno_first)
                wsegs_add(db, segs[k].seq, segs[k].fno_first,
                          segs[k].fno_end ? segs[k].fno_end - 1 : segs[k].fno_first);
        }
    }
    free(segs);
    if (rc == 0 && !ro && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO; /* at open: the open fails */
    if (rc == 0 && db->wal_enabled)
        rc = wal_open_segment(db, db->wal_seq, &db->wal_fd);
    return rc;
}

/* Called by close after the final flush. */
/* A deposed leader (eng_close deposes every leader): its close leaves the log as it is */
static bool lead_closing(eng *db)
{
    return eng_jr(db) && atomic_load(&db->lead_deposed) && eng_test_mutant != MUT_LEADER_CLOSE_FLUSH;
}

void wal_close(eng *db)
{
    if (db->wal_fd >= 0) {
        close(db->wal_fd);
        db->wal_fd = -1;
        /* a replica's log is the leader's: no empty segment is dropped, no exact next number stored (a cut can still
         * lower the log end after a reopen) */
        if (!db->rp && db->active && db->active->rows == 0 && db->nflushing == 0 && !eng_check(db) && !db->retiring &&
            !lead_closing(db)) {
            /* a clean close holds the exact next number; an empty segment carries none, so FNO must before it goes */
            if (fno_require(db, db->fno_next) == 0) {
                char name[64];
                wal_name(name, sizeof name, db->wal_seq);
                /* With the feed on a segment that holds any record, a marker alone included (it adds no rows), is
                 * kept as fwal_; only one nothing was ever written to goes. With the feed off: as it was. */
                if (atomic_load(&db->feed_on) && db->wal_off != 0) /* bytes in the file: a pending buffer is not written */
                    wal_retire_name(db, db->wal_seq, true, NULL);
                else
                    unlinkat(db->dirfd, name, 0);
                wsegs_remove(db, db->wal_seq);
            }
        }
    }
    /* not a deposed leader's: its log end may be above the commit point, which a replica_cut takes back (FNO above the
     * log end fails a replica's open) */
    if (db->wal_enabled && !eng_check(db) && !db->retiring && !lead_closing(db))
        fno_require(db, db->fno_next); /* a failure fails the engine; the segments stay and the open finds the end */
    for (size_t i = 0; i < db->nflushing; i++)
        mt_unref(db->flushing[i]);
    db->nflushing = 0;
    mt_unref(db->active);
    db->active = NULL;
    replica_free(db);
    tsdb_free_big(db->wal_buf, db->wal_cap);
    tsdb_free_big(db->wal_spare, db->wal_spare_cap);
    db->wal_buf = db->wal_spare = NULL;
    db->wal_cap = db->wal_spare_cap = 0;
    for (size_t i = 0; i < WAL_ENC_SLOTS; i++) {
        if (db->wal_enc[i].cctx)
            ZSTD_freeCCtx((ZSTD_CCtx *)db->wal_enc[i].cctx);
        tsdb_free_big(db->wal_enc[i].stage, db->wal_enc[i].stage_cap);
        tsdb_free_big(db->wal_enc[i].rec, db->wal_enc[i].rec_cap);
        db->wal_enc[i].cctx = NULL;
        db->wal_enc[i].stage = db->wal_enc[i].rec = NULL;
        db->wal_enc[i].stage_cap = db->wal_enc[i].rec_cap = 0;
    }
    tsdb_free_big(db->fl_s, db->fl_rows_cap * sizeof *db->fl_s);
    tsdb_free_big(db->fl_t, db->fl_rows_cap * sizeof *db->fl_t);
    tsdb_free_big(db->fl_v, db->fl_rows_cap * sizeof *db->fl_v);
    tsdb_free_big(db->fl_ids, db->fl_ids_cap * sizeof *db->fl_ids);
    db->fl_s = db->fl_ids = NULL;
    db->fl_t = NULL;
    db->fl_v = NULL;
    db->fl_rows_cap = db->fl_ids_cap = 0;
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
    /* a leader's rows past the commit point were shipped: a relog that drops them (unacknowledged in sync mode) would
     * leave positions the replicas hold and the leader does not; a leader that loses its storage is deposed instead */
    if (eng_jr(db))
        return TSDB_ERR_INVALID;
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
