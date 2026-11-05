/*
 * Partition classes: a partition length per series, from its rate.
 *
 * A part holds one partition, and every series pays per partition at least one granule (a 160-byte index entry
 * and two framed blocks); a series that writes 1440 rows a day pays that for 1440 rows, in blocks that compress
 * worse than 8192-row ones. So each series has a class c, and its rows go to partitions of L[c] = partition_ms x
 * {1, 7, 28, 364} (a day, a week, four weeks, 52 weeks by default), chosen so that a partition holds at least
 * eight granules of it. A class-c partition holds only series of class c: fast series keep exactly their day
 * partitions, and a slow series gets long blocks and few granules wherever it lives.
 *
 * Keys. A partition's key is one int64, 4 x (its first base partition) + c, so keys of all classes sort by time
 * and every rule keyed by partition compares keys for equality as before (merges, tombstones, WAL replay by
 * wal_seq, retention). With partition_classes = 0 the multiplier is 1 and every key is a base partition number.
 *
 * Choice. A fresh series (created since the open, in no flush or large insert yet) whose first flush or large
 * insert brings at least 64 rows gets its class from their rate, from the beginning of time: a backfill or an
 * import lands in its final partitions at once. Otherwise a series starts in class 0, and the idle merge of a
 * partition (one part left) measures each series in it; one whose rate lies outside its class's band widened
 * 2x either way (no flapping around a threshold) moves. A series of a long class that gets much faster is caught
 * at the next flush. A class is never longer than an eighth of the series' retention.
 *
 * Moves never rewrite rows: a switch (series, T, class) applies to timestamps >= T, T being the next base
 * partition boundary after the newest row the series has anywhere (parts, memtables, and flushed WAL segments
 * that may still be replayed). So WAL replay maps every row to the partition its flush used (a deleted row merged
 * away cannot come back in another partition), and rows of the series do not straddle T. A T more than a year
 * ahead of now (a wrong timestamp far in the future) is not written: the series keeps its class until that row is
 * gone, rather than for good.
 *
 * One (series, time) never has rows in two partitions. That is not just tidiness: queries rank a part's rows by
 * its max_block, which orders rows correctly only between parts whose block ranges do not interleave, and merges
 * keep that within a partition only. A merge in one partition can span a block of another (a tombstone, say), so
 * the older of two copies of a row in two partitions could outrank the newer one. So every placement (a large
 * insert, a replace, a flush) reads ONE frozen history: it holds cls_rw for reading from its first class lookup
 * until its parts are listed, and a switch takes cls_rw for writing. A switch thus sees every placed row in a
 * listed part or a memtable when it computes T, and no placement sees two histories (rereading the live one while
 * grouping rows once wrote a part whose series run went back in time). Switches are rare (a flush's guard, an idle
 * merge), placements never wait for a flush or a merge while they hold it, and the read lock costs nothing
 * measurable beside writing a part. A switch holds flush_lock, so it waits only CLS_SWITCH_WAIT_MS for the
 * placements in flight (a large insert can take seconds) and is asked again later rather than stall the flushes. As backstops, the part writer refuses rows out of (series, ts) order, and the
 * open checks that every part's rows lie where the history puts them (cls_check_parts).
 *
 * CLASSES is append-only like RETENTION: 24-byte records [u32 check][u32 series][i64 from][u32 class][u32 0];
 * a series' records come in increasing `from` (an equal one replaces the last). A record is written and synced
 * (and the directory when the file is new) before any part or flush relies on it, under class_lock; a failed
 * write or sync fails the engine (eng_fail), as the WAL's rows map through it. A torn tail is cut; a bad record
 * before a valid one fails the open (TSDB_ERR_CORRUPT).
 *
 * In memory: one byte per series in a lock-free, grow-only table (the class at its newest switch, whether it
 * has a history, whether it was decided); the few series with a switch after the beginning of time keep their
 * history in a hash map under cls_mu. A batch whose series all have one class and no history (series_check
 * sees it) is partitioned like before, with that class's length.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CLS_FILE "CLASSES"
#define CLS_REC 24
#define CLS_MIN_ROWS 64 /* rows a rate is measured from */

static const int64_t cls_mult[PART_CLASSES] = {1, 7, 28, 364};

void cls_setup(eng *db, int classes)
{
    int64_t pms = db->partition_ms;
    db->cls_on = classes && pms >= PART_CLASSES && pms <= INT64_MAX / (PART_CLASSES * 364);
    db->cls_kmul = db->cls_on ? PART_CLASSES : 1;
    for (int c = 0; c < PART_CLASSES; c++) {
        db->cls_len[c] = db->cls_on ? pms * cls_mult[c] : pms;
        db->cls_km[c] = db->cls_on ? PART_CLASSES * cls_mult[c] : 1;
    }
}

int partition_class(const eng *db, int64_t key)
{
    return (int)(key - floor_div(key, db->cls_kmul) * db->cls_kmul);
}

void partition_range(const eng *db, int64_t key, int64_t *lo, int64_t *hi)
{
    int c = partition_class(db, key);
    *lo = sat_mul(floor_div(key, db->cls_kmul), db->partition_ms);
    *hi = sat_add(*lo, db->cls_len[c]);
}

/* ------------------------------------------------------------ in memory */

static cls_hist *hfind(eng *db, uint32_t s)
{
    if (!db->hcap)
        return NULL;
    for (size_t h = (s * 0x9E3779B1u) & (db->hcap - 1);; h = (h + 1) & (db->hcap - 1)) {
        if (!db->hmap[h].used)
            return NULL;
        if (db->hmap[h].series == s)
            return &db->hmap[h];
    }
}

static cls_hist *hget(eng *db, uint32_t s)
{
    cls_hist *e = hfind(db, s);
    if (e)
        return e;
    if ((db->hn + 1) * 2 > db->hcap) {
        size_t cap = db->hcap ? db->hcap * 2 : 64;
        cls_hist *m = calloc(cap, sizeof *m);
        if (!m)
            return NULL;
        for (size_t i = 0; i < db->hcap; i++) {
            if (!db->hmap[i].used)
                continue;
            size_t h = (db->hmap[i].series * 0x9E3779B1u) & (cap - 1);
            while (m[h].used)
                h = (h + 1) & (cap - 1);
            m[h] = db->hmap[i];
        }
        free(db->hmap);
        db->hmap = m;
        db->hcap = cap;
    }
    size_t h = (s * 0x9E3779B1u) & (db->hcap - 1);
    while (db->hmap[h].used)
        h = (h + 1) & (db->hcap - 1);
    db->hmap[h] = (cls_hist){.series = s, .used = true};
    db->hn++;
    return &db->hmap[h];
}

/* Caller holds cls_mu. */
static int set_byte_locked(eng *db, uint32_t s, uint8_t b)
{
    cls_table *t = atomic_load_explicit(&db->cls_tab, memory_order_relaxed);
    if (!t || s >= t->cap) {
        uint64_t cap = t ? t->cap : 1024;
        while (cap <= s)
            cap *= 2;
        cls_table *x = calloc(1, sizeof *x + cap);
        if (!x)
            return TSDB_ERR_NOMEM;
        x->cap = cap;
        x->older = t;
        for (uint64_t i = 0; t && i < t->cap; i++)
            atomic_init(&x->b[i], atomic_load_explicit(&t->b[i], memory_order_relaxed));
        atomic_store_explicit(&db->cls_tab, x, memory_order_release);
        t = x;
    }
    atomic_store_explicit(&t->b[s], b, memory_order_relaxed);
    return 0;
}

/* Applies a switch record in memory. Caller holds cls_mu. */
static int apply_locked(eng *db, uint32_t s, int64_t from, int c)
{
    uint8_t b = cls_byte(db, s);
    if (from == INT64_MIN && !(b & CLS_HIST))
        return set_byte_locked(db, s, (uint8_t)(CLS_DECIDED | c));
    cls_hist *h = hget(db, s);
    if (!h)
        return TSDB_ERR_NOMEM;
    if (h->n == 0 || h->n + 1 >= h->cap) {
        uint32_t cap = h->cap ? h->cap * 2 : 4;
        int64_t *f = realloc(h->from, cap * sizeof *f);
        if (f)
            h->from = f;
        uint8_t *k = f ? realloc(h->cls, cap) : NULL;
        if (!k)
            return TSDB_ERR_NOMEM;
        h->cls = k;
        h->cap = cap;
    }
    if (h->n == 0) { /* what held until now: the class from the beginning of time */
        h->from[0] = INT64_MIN;
        h->cls[0] = b & CLS_MASK;
        h->n = 1;
    }
    if (from < h->from[h->n - 1])
        return TSDB_ERR_CORRUPT;
    if (from == h->from[h->n - 1]) {
        h->cls[h->n - 1] = (uint8_t)c;
    } else {
        h->from[h->n] = from;
        h->cls[h->n] = (uint8_t)c;
        h->n++;
    }
    return set_byte_locked(db, s, (uint8_t)(CLS_DECIDED | (h->n > 1 ? CLS_HIST : 0) | c));
}

int cls_at(eng *db, uint32_t s, int64_t ts, int64_t *lo, int64_t *hi)
{
    uint8_t b = cls_byte(db, s);
    *lo = INT64_MIN;
    *hi = INT64_MAX;
    if (LIKELY(!(b & CLS_HIST)))
        return b & CLS_MASK;
    int c = 0;
    pthread_mutex_lock(&db->cls_mu);
    cls_hist *h = hfind(db, s);
    if (h) {
        uint32_t a = 0, z = h->n; /* a = records with from <= ts */
        while (a < z) {
            uint32_t mid = a + (z - a) / 2;
            if (h->from[mid] <= ts)
                a = mid + 1;
            else
                z = mid;
        }
        if (a > 0) {
            c = h->cls[a - 1];
            *lo = h->from[a - 1];
        }
        if (a < h->n)
            *hi = h->from[a];
    }
    pthread_mutex_unlock(&db->cls_mu);
    return c;
}

int64_t partition_of(eng *db, uint32_t series, int64_t ts)
{
    int64_t lo, hi;
    int c = db->cls_on ? cls_at(db, series, ts, &lo, &hi) : 0;
    return floor_div(ts, db->cls_len[c]) * db->cls_km[c] + c;
}

int cls_uniform(eng *db, const uint32_t *ids, uint32_t cs, size_t n)
{
    if (!db->cls_on)
        return 0;
    uint8_t b0 = cls_byte(db, ids ? (n ? ids[0] : 0) : cs);
    for (size_t i = 1; ids && i < n; i++)
        if (cls_byte(db, ids[i]) != b0)
            return -1;
    return (b0 & (CLS_HIST | CLS_DECIDED)) == CLS_DECIDED ? (int)(b0 & CLS_MASK) : -1;
}

int eng_series_class(eng *db, uint32_t series, int *cls)
{
    if (series >= eng_series_count(db))
        return TSDB_ERR_NOTFOUND;
    *cls = cls_byte(db, series) & CLS_MASK;
    return 0;
}

/* ----------------------------------------------------------------- file */

static uint32_t rec_check(const uint8_t *rec, uint64_t off) { return (uint32_t)xxh64(rec + 4, CLS_REC - 4, off); }

static bool rec_ok(const uint8_t *rec, uint64_t off)
{
    return load32(rec) == rec_check(rec, off) && load32(rec + 16) < PART_CLASSES && load32(rec + 20) == 0;
}

/* Applies the records from cls_off on (all of them at open; a reader's refresh, those appended since). confirm: a
 * writer's open, which cuts a torn tail, reads it again first (tail_confirm). */
static int cls_load(eng *db, bool confirm)
{
    int fd = openat(db->dirfd, CLS_FILE, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size, base = (size_t)db->cls_off;
    if (size <= base) {
        close(fd);
        return 0;
    }
    uint8_t *buf = malloc(size - base);
    if (!buf) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    size_t got = 0;
    int rc = 0;
    while (got < size - base) {
        ssize_t r = pread(fd, buf + got, size - base - got, (off_t)(base + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            fprintf(stderr, "tsdb: reading %s at offset %zu: %s\n", CLS_FILE, base + got,
                    r < 0 ? strerror(errno) : "short read");
            rc = TSDB_ERR_IO;
            break;
        }
        got += (size_t)r;
    }
    close(fd);
    size_t pos = 0, len = size - base;
    uint32_t nser = eng_series_count(db);
    bool later = false; /* a reader stopped at a record of a series it has not read yet */
    pthread_mutex_lock(&db->cls_mu);
    for (; rc == 0 && pos + CLS_REC <= len && rec_ok(buf + pos, base + pos); pos += CLS_REC) {
        uint32_t s = load32(buf + pos + 4);
        db->cls_ids = MAX(db->cls_ids, (uint64_t)s + 1); /* the writer's open checks these */
        /* The table grows to the id: one SERIES lacks (a crafted record may name 2^32 - 1) is never applied. The
         * writer's open fails on it (open_check_records), a reader reads it again at its next refresh: the writer
         * may have made the series after the reader read SERIES. */
        if (s >= nser) {
            if (confirm)
                continue;
            later = true;
            break;
        }
        int64_t from;
        memcpy(&from, buf + pos + 8, 8);
        rc = apply_locked(db, s, from, (int)load32(buf + pos + 16));
        if (rc == TSDB_ERR_CORRUPT)
            fprintf(stderr, "tsdb: %s: a record at offset %zu goes back in time\n", CLS_FILE, base + pos);
    }
    pthread_mutex_unlock(&db->cls_mu);
    for (size_t q = pos + CLS_REC; rc == 0 && !later && q + CLS_REC <= len; q += CLS_REC)
        if (rec_ok(buf + q, base + q)) {
            fprintf(stderr, "tsdb: %s is damaged at offset %zu, before valid records\n", CLS_FILE, base + pos);
            rc = TSDB_ERR_CORRUPT;
        }
    if (rc == 0 && confirm && pos < len)
        rc = tail_confirm(db->dirfd, CLS_FILE, buf + pos, base + pos, len - pos);
    if (rc == 0)
        db->cls_off = base + pos; /* a torn tail is overwritten by the next append */
    free(buf);
    return rc;
}

int cls_open(eng *db, bool read_only)
{
    pthread_mutex_init(&db->class_lock, NULL);
    pthread_mutex_init(&db->cls_mu, NULL);
    pthread_rwlockattr_t ra; /* a waiting switch holds new placements back: it is not starved by a stream of them */
    pthread_rwlockattr_init(&ra);
    pthread_rwlockattr_setkind_np(&ra, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&db->cls_rw, &ra);
    pthread_rwlockattr_destroy(&ra);
    db->cls_fd = -1;
    db->cls_dropped_end = INT64_MIN;
    /* every series that exists now was decided (by a record, or class 0 without one) */
    uint32_t n = eng_series_count(db);
    pthread_mutex_lock(&db->cls_mu);
    int rc = n ? set_byte_locked(db, n - 1, CLS_DECIDED) : 0;
    cls_table *t = atomic_load(&db->cls_tab);
    for (uint32_t i = 0; rc == 0 && i < n; i++)
        atomic_store_explicit(&t->b[i], CLS_DECIDED, memory_order_relaxed);
    pthread_mutex_unlock(&db->cls_mu);
    if (rc == 0)
        rc = cls_load(db, !read_only);
    return rc;
}

/* A torn tail is cut as SERIES's is: nothing relied on it, since a record is synced before it is used. Called by a
 * writer's open once its checks passed (as series_cut_tail), so a refused open leaves the file as it found it. */
int cls_cut_tail(eng *db)
{
    struct stat st;
    if (fstatat(db->dirfd, CLS_FILE, &st, 0) != 0 || (uint64_t)st.st_size <= db->cls_off)
        return 0;
    fprintf(stderr, "tsdb: %s: cutting a torn tail of %llu bytes at offset %llu\n", CLS_FILE,
            (unsigned long long)st.st_size - (unsigned long long)db->cls_off, (unsigned long long)db->cls_off);
    int fd = openat(db->dirfd, CLS_FILE, O_WRONLY | O_CLOEXEC);
    int rc = fd < 0 || ftruncate(fd, (off_t)db->cls_off) != 0 ? TSDB_ERR_IO : 0;
    if (fd >= 0)
        close(fd);
    return rc;
}

int cls_reload(eng *db)
{
    pthread_mutex_lock(&db->class_lock);
    uint32_t n = eng_series_count(db);
    int rc = 0;
    pthread_mutex_lock(&db->cls_mu);
    for (uint32_t i = 0; rc == 0 && i < n; i++)
        if (!(cls_byte(db, i) & CLS_DECIDED))
            rc = set_byte_locked(db, i, cls_byte(db, i) | CLS_DECIDED);
    pthread_mutex_unlock(&db->cls_mu);
    if (rc == 0)
        rc = cls_load(db, false);
    pthread_mutex_unlock(&db->class_lock);
    return rc;
}

void cls_close(eng *db)
{
    if (db->cls_fd >= 0)
        close(db->cls_fd);
    db->cls_fd = -1;
    for (cls_table *t = atomic_load(&db->cls_tab), *o; t; t = o) {
        o = t->older;
        free(t);
    }
    atomic_store(&db->cls_tab, NULL);
    for (size_t i = 0; i < db->hcap; i++) {
        free(db->hmap[i].from);
        free(db->hmap[i].cls);
    }
    free(db->hmap);
    db->hmap = NULL;
    db->hcap = db->hn = 0;
    for (struct cls_wtail *e = db->wtail, *x; e; e = x) {
        x = e->next;
        free(e);
    }
    db->wtail = NULL;
    db->nwtail = 0;
    pthread_mutex_destroy(&db->class_lock);
    pthread_mutex_destroy(&db->cls_mu);
    pthread_rwlock_destroy(&db->cls_rw);
}

typedef struct cls_rec {
    uint32_t series;
    int64_t from;
    int cls;
} cls_rec;

/* Appends and syncs records, then applies them in memory. Caller holds class_lock. Fail-stop: the WAL's rows
 * are mapped through these records, by flushes now and by a replay later. */
static int append_locked(eng *db, const cls_rec *r, size_t n)
{
    if (n == 0)
        return 0;
    int rc = eng_check(db);
    if (rc)
        return rc;
    if (db->cls_broken)
        return TSDB_ERR_IO;
    if (db->cls_fd < 0) {
        bool existed = faccessat(db->dirfd, CLS_FILE, F_OK, 0) == 0;
        db->cls_fd = openat(db->dirfd, CLS_FILE, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (db->cls_fd < 0) {
            int err = errno;
            db->cls_broken = true;
            eng_fail(db, "open of CLASSES", err);
            return TSDB_ERR_IO;
        }
        if (!existed && db->opt.fsync && (rc = eng_sync_dir(db, "fsync of the directory after creating CLASSES")))
            return rc;
    }
    uint8_t *buf = malloc(n * CLS_REC);
    if (!buf)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++) {
        uint8_t *rec = buf + i * CLS_REC;
        memset(rec, 0, CLS_REC);
        store32(rec + 4, r[i].series);
        memcpy(rec + 8, &r[i].from, 8);
        store32(rec + 16, (uint32_t)r[i].cls);
        store32(rec, rec_check(rec, db->cls_off + i * CLS_REC));
    }
    size_t len = n * CLS_REC, done = 0;
    int err = 0;
    while (done < len) {
        ssize_t w = pwrite(db->cls_fd, buf + done, len - done, (off_t)(db->cls_off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            err = w < 0 ? errno : ENOSPC;
            break;
        }
        done += (size_t)w;
    }
    free(buf);
    if (!err && db->opt.fsync && fdatasync(db->cls_fd) != 0)
        err = errno;
    if (err) {
        db->cls_broken = true; /* the file may end in a torn record: append no more */
        eng_fail(db, "write or fsync of CLASSES", err);
        return TSDB_ERR_IO;
    }
    db->cls_off += len;
    pthread_mutex_lock(&db->cls_mu);
    for (size_t i = 0; rc == 0 && i < n; i++)
        rc = apply_locked(db, r[i].series, r[i].from, r[i].cls);
    pthread_mutex_unlock(&db->cls_mu);
    if (rc) /* in the file but not in memory: flushes would map rows unlike a replay */
        eng_fail(db, "applying a CLASSES record", ENOMEM);
    atomic_fetch_add(&db->st_class_switches, n);
    return rc;
}

/* ----------------------------------------------------------- the choice */

static double target_rows(eng *db) { return 8.0 * db->granule_rows; }

/* The longest class allowed by the series' retention: a partition at most an eighth of it. */
static int class_cap(eng *db, uint32_t s)
{
    int64_t ret = eng_retention(db, s);
    int cap = PART_CLASSES - 1;
    while (ret > 0 && cap > 0 && db->cls_len[cap] > ret / 8)
        cap--;
    return cap;
}

/* rate in rows per ms */
static int class_choose(eng *db, double rate, int cap)
{
    for (int c = 0; c < cap; c++)
        if (rate * (double)db->cls_len[c] >= target_rows(db))
            return c;
    return cap;
}

/* Whether the rate lies outside class c's band, widened 2x on either side. */
static bool out_of_band(eng *db, int c, double rate)
{
    double t = target_rows(db);
    if (c < PART_CLASSES - 1 && rate * (double)db->cls_len[c] < t / 2)
        return true; /* too slow for c */
    if (c > 0 && rate * (double)db->cls_len[c - 1] >= 2 * t)
        return true; /* too fast */
    return false;
}

static double rate_of(size_t n, int64_t tmin, int64_t tmax)
{
    if (n < CLS_MIN_ROWS)
        return -1;
    double span = (double)tmax - (double)tmin;
    return span > 0 ? (double)(n - 1) / span : 1e30;
}

typedef struct fstat_ {
    uint32_t s;
    size_t n;
    int64_t tmin, tmax;
} fstat_;

/* Decides the fresh series among st[0..n): a record from the beginning of time when they get a longer class,
 * in memory only for class 0. */
static int decide_fresh(eng *db, const fstat_ *st, size_t n)
{
    cls_rec *r = malloc((n ? n : 1) * sizeof *r);
    if (!r)
        return TSDB_ERR_NOMEM;
    size_t k = 0;
    int rc = 0;
    pthread_mutex_lock(&db->class_lock);
    for (size_t i = 0; i < n; i++) {
        if (cls_byte(db, st[i].s) & CLS_DECIDED)
            continue; /* a flush or another insert was first */
        double rate = rate_of(st[i].n, st[i].tmin, st[i].tmax);
        int c = rate < 0 ? 0 : class_choose(db, rate, class_cap(db, st[i].s));
        if (c)
            r[k++] = (cls_rec){st[i].s, INT64_MIN, c};
        else {
            pthread_mutex_lock(&db->cls_mu);
            rc = set_byte_locked(db, st[i].s, CLS_DECIDED);
            pthread_mutex_unlock(&db->cls_mu);
            if (rc)
                break;
        }
    }
    if (rc == 0)
        rc = append_locked(db, r, k);
    pthread_mutex_unlock(&db->class_lock);
    free(r);
    return rc;
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

int cls_classify_insert(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, size_t n)
{
    if (!db->cls_on || n == 0)
        return 0;
    if (!series) {
        fstat_ st = {cs, n, INT64_MAX, INT64_MIN};
        for (size_t i = 0; i < n; i++) {
            st.tmin = MIN(st.tmin, ts[i]);
            st.tmax = MAX(st.tmax, ts[i]);
        }
        return (cls_byte(db, cs) & CLS_DECIDED) ? 0 : decide_fresh(db, &st, 1);
    }
    /* Series ids of a batch are usually dense: their statistics then go into a table indexed by id, in one pass
     * (rows arrive time-major as often as not, so collecting the fresh ids row by row and sorting them cost seconds
     * on a 100 M-row batch). */
    uint32_t smin = UINT32_MAX, smax = 0;
    for (size_t i = 0; i < n; i++) {
        smin = series[i] < smin ? series[i] : smin;
        smax = series[i] > smax ? series[i] : smax;
    }
    size_t span = (size_t)smax - smin + 1;
    if (span <= MAX(n, (size_t)65536)) {
        fstat_ *t = malloc(span * sizeof *t);
        uint8_t *fresh = calloc(span, 1); /* 0: not seen yet, 1: fresh, 2: decided */
        if (!t || !fresh) {
            free(t);
            free(fresh);
            return TSDB_ERR_NOMEM;
        }
        for (size_t i = 0; i < n; i++) {
            size_t k = series[i] - smin;
            if (UNLIKELY(!fresh[k])) {
                fresh[k] = (cls_byte(db, series[i]) & CLS_DECIDED) ? 2 : 1;
                t[k] = (fstat_){series[i], 0, INT64_MAX, INT64_MIN};
            }
            if (fresh[k] == 2)
                continue;
            t[k].n++;
            t[k].tmin = MIN(t[k].tmin, ts[i]);
            t[k].tmax = MAX(t[k].tmax, ts[i]);
        }
        size_t u = 0;
        for (size_t k = 0; k < span; k++)
            if (fresh[k] == 1)
                t[u++] = t[k];
        free(fresh);
        int rc = u ? decide_fresh(db, t, u) : 0;
        free(t);
        return rc;
    }
    /* sparse ids: the fresh series, sorted, then their rows' statistics */
    size_t nf = 0, cap = 16;
    uint32_t *f = malloc(cap * sizeof *f);
    for (size_t i = 0; f && i < n; i++) {
        if ((i && series[i] == series[i - 1]) || (cls_byte(db, series[i]) & CLS_DECIDED))
            continue;
        if (nf == cap) {
            uint32_t *x = realloc(f, (cap *= 2) * sizeof *x);
            if (!x) {
                free(f);
                f = NULL;
                break;
            }
            f = x;
        }
        f[nf++] = series[i];
    }
    if (!f)
        return TSDB_ERR_NOMEM;
    qsort(f, nf, sizeof *f, cmp_u32);
    size_t u = 0;
    for (size_t i = 0; i < nf; i++)
        if (!u || f[u - 1] != f[i])
            f[u++] = f[i];
    fstat_ *st = malloc((u ? u : 1) * sizeof *st);
    if (!st) {
        free(f);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < u; i++)
        st[i] = (fstat_){f[i], 0, INT64_MAX, INT64_MIN};
    size_t last = SIZE_MAX;
    for (size_t i = 0; i < n && u; i++) {
        size_t k = last;
        if (k == SIZE_MAX || f[k] != series[i]) {
            size_t a = 0, z = u;
            while (a < z) {
                size_t mid = a + (z - a) / 2;
                if (f[mid] < series[i])
                    a = mid + 1;
                else
                    z = mid;
            }
            if (a == u || f[a] != series[i])
                continue;
            k = last = a;
        }
        st[k].n++;
        st[k].tmin = MIN(st[k].tmin, ts[i]);
        st[k].tmax = MAX(st[k].tmax, ts[i]);
    }
    int rc = decide_fresh(db, st, u);
    free(st);
    free(f);
    return rc;
}

static void run_stats(const mt_series *ms, int64_t *tmin, int64_t *tmax)
{
    *tmin = INT64_MAX;
    *tmax = INT64_MIN;
    if (ms->sorted && ms->n) {
        *tmin = ms->ts[0];
        *tmax = ms->ts[ms->n - 1];
        return;
    }
    for (uint32_t j = 0; j < ms->n; j++) {
        *tmin = MIN(*tmin, ms->ts[j]);
        *tmax = MAX(*tmax, ms->ts[j]);
    }
}

int cls_classify_memtable(eng *db, const memtable *m)
{
    if (!db->cls_on || m->nids == 0)
        return 0;
    fstat_ *st = NULL;
    size_t k = 0;
    for (uint32_t i = 0; i < m->nids; i++) {
        uint32_t id = m->ids[i];
        if (cls_byte(db, id) & CLS_DECIDED)
            continue;
        if (!st && !(st = malloc(m->nids * sizeof *st)))
            return TSDB_ERR_NOMEM;
        const mt_series *ms = &m->s[id];
        st[k] = (fstat_){id, ms->n, 0, 0};
        run_stats(ms, &st[k].tmin, &st[k].tmax);
        k++;
    }
    int rc = k ? decide_fresh(db, st, k) : 0;
    free(st);
    return rc;
}

/* ---------------------------------------------------------- the switches */

/* Newest timestamp of series s in part p, or INT64_MIN. */
static int64_t part_newest(const part *p, uint32_t s)
{
    uint32_t a, z;
    gix_run(&p->ix, s, &a, &z);
    return a < z ? gx_ts_max(p, z - 1) : INT64_MIN;
}

static int64_t mt_newest(const memtable *m, uint32_t s)
{
    if (s >= m->scap || m->s[s].n == 0)
        return INT64_MIN;
    int64_t a, b;
    run_stats(&m->s[s], &a, &b);
    return b;
}

struct cls_wtail *cls_wal_tail_new(const memtable *m)
{
    struct cls_wtail *e = malloc(sizeof *e + (size_t)m->nids * (sizeof(int64_t) + sizeof(uint32_t)));
    if (!e)
        return NULL;
    e->next = NULL;
    e->n = m->nids;
    e->sorted = false;
    e->max = (int64_t *)(e + 1);
    e->ids = (uint32_t *)(e->max + e->n);
    for (size_t i = 0; i < e->n; i++) {
        e->ids[i] = m->ids[i];
        e->max[i] = mt_newest(m, m->ids[i]);
    }
    return e;
}

void cls_wal_tail_locked(eng *db, struct cls_wtail *e, uint64_t seq, int64_t max_ts)
{
    uint64_t synced = atomic_load(&db->wal_removed_synced);
    for (struct cls_wtail **pp = &db->wtail; *pp;) { /* drop what can no longer be replayed */
        struct cls_wtail *x = *pp;
        if (x->seq <= synced) {
            *pp = x->next;
            free(x);
            db->nwtail--;
        } else {
            pp = &x->next;
        }
    }
    if (db->wtail_all_seq <= synced)
        db->wtail_all_seq = 0, db->wtail_all_max = INT64_MIN;
    if (seq <= synced) {
        free(e);
        return;
    }
    if (!e) { /* ENOMEM: keep the bound, for every series (coarse, but never dropped) */
        db->wtail_all_seq = MAX(db->wtail_all_seq, seq);
        db->wtail_all_max = MAX(db->wtail_all_max, max_ts);
        return;
    }
    e->seq = seq;
    e->next = db->wtail;
    db->wtail = e;
    db->nwtail++;
}

static int cmp_tail(const void *a, const void *b)
{
    const uint64_t *x = a, *y = b;
    return (*x > *y) - (*x < *y);
}

/* Newest timestamp of series s in the flushed memtables whose segments may still be replayed. db->lock held. */
static int64_t wal_newest_locked(eng *db, uint32_t s)
{
    int64_t newest = db->wtail_all_seq > atomic_load(&db->wal_removed_synced) ? db->wtail_all_max : INT64_MIN;
    for (struct cls_wtail *e = db->wtail; e; e = e->next) {
        if (e->seq <= atomic_load(&db->wal_removed_synced))
            continue;
        if (!e->sorted) { /* pack (id, max) pairs to sort them together, then unpack */
            uint64_t *pk = malloc((e->n ? e->n : 1) * sizeof *pk);
            if (!pk) {
                for (size_t i = 0; i < e->n; i++) /* ENOMEM: a linear look */
                    if (e->ids[i] == s)
                        newest = MAX(newest, e->max[i]);
                continue;
            }
            for (size_t i = 0; i < e->n; i++)
                pk[i] = (uint64_t)e->ids[i] << 32 | i;
            qsort(pk, e->n, sizeof *pk, cmp_tail);
            int64_t *mx = malloc((e->n ? e->n : 1) * sizeof *mx);
            if (!mx) {
                free(pk);
                for (size_t i = 0; i < e->n; i++)
                    if (e->ids[i] == s)
                        newest = MAX(newest, e->max[i]);
                continue;
            }
            for (size_t i = 0; i < e->n; i++)
                mx[i] = e->max[(uint32_t)pk[i]], e->ids[i] = (uint32_t)(pk[i] >> 32);
            memcpy(e->max, mx, e->n * sizeof *mx);
            free(mx);
            free(pk);
            e->sorted = true;
        }
        size_t a = 0, z = e->n;
        while (a < z) {
            size_t mid = a + (z - a) / 2;
            if (e->ids[mid] < s)
                a = mid + 1;
            else
                z = mid;
        }
        if (a < e->n && e->ids[a] == s)
            newest = MAX(newest, e->max[a]);
    }
    return newest;
}

void cls_freeze(eng *db)
{
    if (db->cls_on)
        pthread_rwlock_rdlock(&db->cls_rw);
}

void cls_thaw(eng *db)
{
    if (db->cls_on)
        pthread_rwlock_unlock(&db->cls_rw);
}

#define CLS_LATER 1          /* switch_locked: placements in flight held the history; ask again later */
#define CLS_SWITCH_WAIT_MS 5 /* how long a switch waits for them, holding flush_lock and new placements back */
#define CLS_RETRY_US 20000   /* between two tries of a switch that must happen (an observation, the test hook) */

/* cls_rw for writing, or CLS_LATER after CLS_SWITCH_WAIT_MS. The caller holds flush_lock, and the lock prefers
 * writers: waiting out a large insert (23.7 s for 1 G rows) would stop every flush, large insert and replace that
 * long. A switch only moves future rows to better partitions, so it can wait instead. */
static int switch_wrlock(eng *db)
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    t.tv_nsec += CLS_SWITCH_WAIT_MS * 1000000L;
    if (t.tv_nsec >= 1000000000L)
        t.tv_sec++, t.tv_nsec -= 1000000000L;
    return pthread_rwlock_timedwrlock(&db->cls_rw, &t) == 0 ? 0 : CLS_LATER;
}

typedef struct sw {
    uint32_t s;
    int cls;
    const int64_t *at; /* test hook: this T instead of the computed one */
} sw;

#define CLS_FAR_MS (366 * 86400000LL) /* a T further ahead of now than this is not written (file comment) */

/* Switches series to new classes at T (see the file comment). Caller holds flush_lock: no flush maps rows
 * meanwhile, so every WAL row of the series that is in a part is behind T; cls_rw for writing: no placement is
 * between looking up classes and listing its parts. */
static int switch_locked(eng *db, const sw *w, size_t n)
{
    if (!n)
        return 0;
    cls_rec *r = malloc(n * sizeof *r);
    if (!r)
        return TSDB_ERR_NOMEM;
    if (switch_wrlock(db)) {
        free(r);
        return CLS_LATER;
    }
    pthread_mutex_lock(&db->class_lock);
    size_t k = 0;
    int64_t far = sat_add(eng_now_ms(db), CLS_FAR_MS);
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++) {
        uint32_t s = w[i].s;
        uint8_t b = cls_byte(db, s);
        if ((int)(b & CLS_MASK) == w[i].cls && (b & CLS_DECIDED))
            continue;
        int64_t newest = wal_newest_locked(db, s);
        for (size_t j = 0; j < db->nparts; j++)
            newest = MAX(newest, part_newest(db->parts[j], s));
        for (size_t j = 0; j < db->nflushing; j++)
            newest = MAX(newest, mt_newest(db->flushing[j], s));
        pthread_mutex_lock(&db->buf_lock);
        newest = MAX(newest, mt_newest(db->active, s));
        pthread_mutex_unlock(&db->buf_lock);
        int64_t T = newest == INT64_MIN ? INT64_MIN : sat_mul(floor_div(newest, db->partition_ms) + 1, db->partition_ms);
        /* rows of dropped partitions are no longer listed, but their removal may not be durable yet: a power loss
         * brings them back, and they must still lie before T */
        T = MAX(T, db->cls_dropped_end);
        if (T == INT64_MAX)
            continue; /* rows at the end of time: nothing can come after them */
        if (b & CLS_HIST) {
            pthread_mutex_lock(&db->cls_mu);
            cls_hist *h = hfind(db, s);
            if (h && h->n)
                T = MAX(T, h->from[h->n - 1]);
            pthread_mutex_unlock(&db->cls_mu);
        }
        if (!w[i].at && T != INT64_MIN && T - db->partition_ms > far) {
            static _Atomic int64_t logged; /* once a minute: the guard asks again at every flush */
            int64_t now = eng_now_ms(db), l = atomic_load(&logged);
            if (now - l >= 60000 && atomic_compare_exchange_strong(&logged, &l, now))
                fprintf(stderr, "tsdb: series %u: a row at %" PRId64 " ms, far in the future, keeps it in partition class %d "
                            "until the row is gone\n", s, newest, (int)(b & CLS_MASK));
            continue;
        }
        r[k++] = (cls_rec){s, w[i].at && *w[i].at > T ? *w[i].at : T, w[i].cls};
    }
    pthread_mutex_unlock(&db->lock);
    int rc = append_locked(db, r, k);
    if (rc == 0 && k) /* after the records were applied, under cls_rw: a placement that reads it later sees them */
        atomic_fetch_add_explicit(&db->cls_gen, k, memory_order_release);
    pthread_mutex_unlock(&db->class_lock);
    pthread_rwlock_unlock(&db->cls_rw);
    free(r);
    return rc;
}

/* A switch that must happen: tried until placements let it in, without holding flush_lock between tries. */
static int switch_retry(eng *db, const sw *w, size_t n)
{
    for (;;) {
        pthread_mutex_lock(&db->flush_lock);
        int rc = switch_locked(db, w, n);
        pthread_mutex_unlock(&db->flush_lock);
        if (rc != CLS_LATER)
            return rc;
        if (atomic_load(&db->stop))
            return 0; /* closing: the series is measured again after the next open */
        usleep(CLS_RETRY_US);
    }
}

uint64_t cls_gen_now(eng *db) { return atomic_load_explicit(&db->cls_gen, memory_order_acquire); }

int eng_class_switch(eng *db, uint32_t series, int cls, const int64_t *at)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    if (!db->cls_on || cls < 0 || cls >= PART_CLASSES || series >= eng_series_count(db))
        return TSDB_ERR_INVALID;
    sw w = {series, cls, at};
    return switch_retry(db, &w, 1);
}

int cls_flush_guard(eng *db, const memtable *m)
{
    if (!db->cls_on)
        return 0;
    sw *w = NULL;
    size_t k = 0;
    for (uint32_t i = 0; i < m->nids; i++) {
        uint32_t id = m->ids[i];
        int c = cls_byte(db, id) & CLS_MASK;
        const mt_series *ms = &m->s[id];
        if (c == 0 || ms->n < CLS_MIN_ROWS)
            continue;
        int64_t tmin, tmax;
        run_stats(ms, &tmin, &tmax);
        double rate = rate_of(ms->n, tmin, tmax);
        if (rate * (double)db->cls_len[c] <= 4 * target_rows(db) || !out_of_band(db, c, rate))
            continue; /* within the band too: no flapping between flushes and idle merges */
        int to = class_choose(db, rate, class_cap(db, id));
        if (to >= c)
            continue;
        if (!w && !(w = malloc(m->nids * sizeof *w)))
            return TSDB_ERR_NOMEM;
        w[k++] = (sw){id, to, NULL};
    }
    int rc = switch_locked(db, w, k);
    free(w);
    return rc == CLS_LATER ? 0 : rc; /* placements in flight: the guard asks again at the next flush */
}

/* Where the series' newest class begins (INT64_MIN when it never switched). */
static int64_t newest_from(eng *db, uint32_t s, uint8_t b)
{
    if (!(b & CLS_HIST))
        return INT64_MIN;
    pthread_mutex_lock(&db->cls_mu);
    cls_hist *h = hfind(db, s);
    int64_t from = h && h->n ? h->from[h->n - 1] : INT64_MIN;
    pthread_mutex_unlock(&db->cls_mu);
    return from;
}

int cls_observe(eng *db, part *p)
{
    if (!db->cls_on || db->opt.read_only || p->ng == 0)
        return 0;
    /* the checks below catch a run table that damage made impossible, not one that names another valid series in
     * order: only the checksum does, and a class record is for good */
    if (!part_checked(db, p, "measuring series rates"))
        return TSDB_ERR_CORRUPT;
    int64_t lo, hi;
    partition_range(db, p->partition, &lo, &hi);
    double len = (double)hi - (double)lo;
    sw *w = NULL;
    size_t k = 0;
    uint32_t nser = eng_series_count(db);
    for (uint32_t r = 0; r < p->ix.nruns; r++) {
        uint32_t s = p->ix.rser[r], i = gix_rstart(&p->ix, r);
        uint32_t j = r + 1 < p->ix.nruns ? gix_rstart(&p->ix, r + 1) : p->ng;
        /* A run table damaged after the open (see gix_rstart; read in the mapping without pin_index) must not
         * become a durable record: a series SERIES lacks (which a series made later would inherit), or runs out of
         * order, which name some other series. */
        if (j <= i || s >= nser || (r > 0 && s <= p->ix.rser[r - 1]))
            continue;
        size_t n = 0;
        for (uint32_t x = i; x < j; x++)
            n += gx_count(p, x);
        int64_t tmin = gx_ts_min(p, i), tmax = gx_ts_max(p, j - 1);
        double rate = (double)tmax - (double)tmin >= len / 2 ? (double)n / len : rate_of(n, tmin, tmax);
        if (rate < 0)
            continue; /* no opinion */
        uint8_t b = cls_byte(db, s);
        int c = b & CLS_MASK, cap = class_cap(db, s);
        if (c != partition_class(db, p->partition) || tmax < newest_from(db, s, b))
            continue; /* rows from before its newest switch: they show the rate it changed from */
        if (!out_of_band(db, c, rate) && c <= cap)
            continue;
        int to = class_choose(db, rate, cap);
        if (to == c)
            continue;
        if (!w && !(w = malloc(p->ng * sizeof *w)))
            return TSDB_ERR_NOMEM;
        w[k++] = (sw){s, to, NULL};
    }
    int rc = 0;
    if (k)
        rc = switch_retry(db, w, k);
    free(w);
    return rc;
}

/* ------------------------------------------------------- the open check */

static int check_fail(const part *p, uint32_t s, const char *why, int64_t a, int64_t b)
{
    fprintf(stderr, "tsdb: part %s: series %u: %s (%" PRId64 " .. %" PRId64 ")\n", p->name, s, why, a, b);
    return TSDB_ERR_CORRUPT;
}

/* Whether the history gives series s class pc at every row of granule g. A granule whose rows lie in one range of
 * the history is judged from its bounds; one that spans a switch is decoded, since a series can come back to a
 * class: its rows before and after a stay in another class share a partition, with none in between. */
static int check_granule(eng *db, codec_ctx *cc, int64_t *ts, const part *p, const granule *g, int pc)
{
    uint32_t s = g->series;
    int64_t flo, fhi;
    int c = cls_at(db, s, g->ts_min, &flo, &fhi);
    if (c == pc && (g->ts_max < fhi || fhi == INT64_MAX))
        return 0;
    if (c != pc || cls_at(db, s, g->ts_max, &flo, &fhi) != pc)
        return check_fail(p, s, "rows in a partition of another class than CLASSES gives them", g->ts_min, g->ts_max);
    int rc = granule_read(cc, p, g, 0, ts, NULL, p->grows, db->opt.verify_checksums); /* ts holds grows */
    if (rc) {
        fprintf(stderr, "tsdb: part %s: series %u: a granule does not decode (%d)\n", p->name, s, rc);
        return rc == TSDB_ERR_NOMEM ? rc : TSDB_ERR_CORRUPT;
    }
    fhi = INT64_MIN;
    for (uint32_t r = 0; r < g->count; r++) {
        if (ts[r] < g->ts_min || ts[r] > g->ts_max)
            return check_fail(p, s, "a row outside its granule's bounds", g->ts_min, ts[r]);
        if (r && ts[r] <= ts[r - 1]) /* the skip below relies on ascending rows: damage must not hide one */
            return check_fail(p, s, "rows out of time order", ts[r - 1], ts[r]);
        if (ts[r] < fhi || fhi == INT64_MAX)
            continue;
        if ((c = cls_at(db, s, ts[r], &flo, &fhi)) != pc)
            return check_fail(p, s, "rows in a partition of another class than CLASSES gives them", ts[r], ts[r]);
    }
    return 0;
}

int cls_check_parts(eng *db)
{
    codec_ctx *cc = NULL;
    int64_t *ts = NULL;
    size_t tscap = 0;
    int rc = 0;
    for (size_t pi = 0; pi < db->nparts && rc == 0; pi++) {
        const part *p = db->parts[pi];
        int pc = partition_class(db, p->partition);
        int64_t lo, hi;
        partition_range(db, p->partition, &lo, &hi);
        /* the runs are in series order (gix_parse checks it) */
        for (uint32_t r = 0; r < p->ix.nruns && rc == 0; r++) {
            uint32_t s = p->ix.rser[r], i = gix_rstart(&p->ix, r);
            uint32_t j = r + 1 < p->ix.nruns ? gix_rstart(&p->ix, r + 1) : p->ng;
            for (uint32_t x = i; x < j; x++)
                if (gx_ts_min(p, x) > gx_ts_max(p, x) || (x > i && gx_ts_min(p, x) <= gx_ts_max(p, x - 1))) {
                    rc = check_fail(p, s, "rows out of time order", gx_ts_min(p, x), gx_ts_max(p, x));
                    break;
                }
            if (rc)
                break;
            int64_t tmin = gx_ts_min(p, i), tmax = gx_ts_max(p, j - 1);
            if (tmin < lo || (hi != INT64_MAX && tmax >= hi)) {
                rc = check_fail(p, s, "rows outside the partition", tmin, tmax);
                break;
            }
            if (!db->cls_on || !(cls_byte(db, s) & CLS_HIST)) { /* one class at every time */
                if ((db->cls_on ? (int)(cls_byte(db, s) & CLS_MASK) : 0) != pc)
                    rc = check_fail(p, s, "rows in a partition of another class than CLASSES gives them", tmin, tmax);
                continue;
            }
            if (!cc && !(cc = codec_new()))
                rc = TSDB_ERR_NOMEM;
            if (rc == 0 && tscap < p->grows) {
                free(ts);
                tscap = p->grows;
                if (!(ts = malloc(tscap * sizeof *ts)))
                    rc = TSDB_ERR_NOMEM, tscap = 0;
            }
            for (uint32_t g = i; g < j && rc == 0; g++) { /* granule_read refuses a count above p->grows */
                granule gb;
                gx_load(p, g, GW_LOC | GW_TIME, &gb);
                gb.series = s;
                rc = check_granule(db, cc, ts, p, &gb, pc);
            }
        }
    }
    if (cc)
        codec_free(cc);
    free(ts);
    return rc;
}
