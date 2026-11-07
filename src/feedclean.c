/*
 * The cleaner: removes retained WAL segments (fwal_<seq>.log) that the feed no longer owes anyone.
 * docs/design-cdc.md 2.6 (F1 builds it without slots).
 *
 * One thread per engine (the design says per handle; a handle's volumes are few and operator-added, and what the
 * design asks for is only that it is not a merge thread), woken by every flush and once a second. It does its
 * I/O with no engine lock held and never takes flush_lock; feed_lock guards only the index and the floor.
 *
 * What goes, oldest first, never the newest segment:
 *   - a segment entirely at or below the floor (left by a crash after the floor was made durable but before the
 *     unlinks, or renamed after a live enable: it lies before the feed's history), whatever its age or size;
 *   - one whose last record's commit_ms is older than retention_s;
 *   - the oldest while the retained segments together exceed max_bytes;
 *   - under the free-space rule (the volume's file system below min_free_pct free) the oldest, ignoring age and size,
 *     until the space is back.
 * The floor is the last number of the last record of the newest segment removed in a pass (and the low 32 bits of that
 * record's hash): it is made durable in FEEDSLOTS (scratch half, fdatasync, then the anchor) BEFORE any unlink, so a
 * consumer that resumes below it is told it fell behind and never reads a gap. A failed FEEDSLOTS write is fail-stop
 * (fslots_set_floor); a failed unlink is logged and retried (the floor already excludes the segment), and the
 * directory fsync after the unlinks is eng_sync_dir's, after feed_lock is released.
 *
 * A segment's last record is read lazily and once: header to header with pread (32 bytes each), then the last record
 * whole and checked. Segments the flush renamed carry it already (feed_index_add with info).
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
#include <sys/statvfs.h>
#include <unistd.h>

#define CLEAN_BATCH 16 /* segments per floor write under the free-space rule */

static int (*free_hook)(void *, const char *, uint32_t *);
static void *free_hook_ctx;

void feed_test_free_hook(int (*fn)(void *ctx, const char *dir, uint32_t *pct), void *ctx)
{
    free_hook_ctx = ctx;
    free_hook = fn;
}

/* ------------------------------------------------------------------ index */

static size_t index_find_locked(const eng *db, uint64_t seq, bool *found)
{
    size_t lo = 0, hi = db->nfidx;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (db->fidx[mid].seq < seq)
            lo = mid + 1;
        else
            hi = mid;
    }
    *found = lo < db->nfidx && db->fidx[lo].seq == seq;
    return lo;
}

void feed_index_add(eng *db, uint64_t seq, const fseg *info)
{
    pthread_mutex_lock(&db->feed_lock);
    bool found;
    size_t at = index_find_locked(db, seq, &found);
    if (!found) {
        if (db->nfidx == db->capfidx) {
            size_t cap = db->capfidx ? db->capfidx * 2 : 32;
            fseg *n = realloc(db->fidx, cap * sizeof *n);
            if (!n) { /* the next scan of the directory finds it */
                pthread_mutex_unlock(&db->feed_lock);
                return;
            }
            db->fidx = n;
            db->capfidx = cap;
        }
        memmove(db->fidx + at + 1, db->fidx + at, (db->nfidx - at) * sizeof *db->fidx);
        db->nfidx++;
        memset(&db->fidx[at], 0, sizeof db->fidx[at]);
        db->fidx[at].seq = seq;
        db->fidx[at].size = UINT64_MAX; /* not yet known */
    }
    if (info && info->known) {
        db->fidx[at] = *info;
        db->fidx[at].seq = seq;
    }
    pthread_mutex_unlock(&db->feed_lock);
}

void feed_index_free(eng *db)
{
    pthread_mutex_lock(&db->feed_lock);
    free(db->fidx);
    db->fidx = NULL;
    db->nfidx = db->capfidx = 0;
    pthread_mutex_unlock(&db->feed_lock);
}

/* Every fwal_ name of the directory into the index (the cleaner's start: the segments found at open). */
static int index_scan(eng *db)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) {
        uint64_t seq;
        if (fwal_file_name(e->d_name) && sscanf(e->d_name, "fwal_%" SCNx64 ".log", &seq) == 1)
            feed_index_add(db, seq, NULL);
    }
    int rc = errno ? TSDB_ERR_IO : 0;
    closedir(d);
    return rc;
}

/* --------------------------------------------------------- reading a segment */

/* Fills first, last, last_ms, last_hash and size of the segment. 0, or an error (logged): a damaged retained segment
 * is the feed's problem, never skipped; the cleaner then stops its pass. */
static int seg_fill(eng *db, fseg *s)
{
    char name[64];
    fwal_name(name, sizeof name, s->seq);
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    off_t size = lseek(fd, 0, SEEK_END);
    if (size < 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    uint8_t h[WAL_HDR];
    uint64_t off = 0, last_off = 0;
    bool any = false;
    int rc = 0;
    uint64_t first = 0;
    while ((uint64_t)size - off >= WAL_HDR) {
        ssize_t r = pread(fd, h, sizeof h, (off_t)off);
        if (r != (ssize_t)sizeof h) {
            rc = TSDB_ERR_IO;
            break;
        }
        uint32_t w = load32(h), kind = w >> 28, n = w & 0x0fffffffu;
        size_t body = kind == WAL_KIND_ROWS ? (size_t)n * WAL_ROW : kind == WAL_KIND_MARKER ? n : (size_t)-1;
        if (body == (size_t)-1 || (uint64_t)size - off < WAL_HDR + body)
            break; /* not a record: the end of the valid ones */
        if (!any)
            memcpy(&first, h + 16, 8);
        any = true;
        last_off = off;
        off += WAL_HDR + body;
    }
    uint8_t *rec = NULL;
    if (rc == 0 && any) {
        size_t len = (size_t)(off - last_off);
        rec = malloc(len);
        if (!rec)
            rc = TSDB_ERR_NOMEM;
        else if (pread(fd, rec, len, (off_t)last_off) != (ssize_t)len)
            rc = TSDB_ERR_IO;
        wal_rec wr;
        if (rc == 0 && wal_record(rec, len, s->seq, last_off, &wr) != len) {
            fprintf(stderr, "tsdb: %s: the last record of %s is damaged: it stays, and the cleaner waits (the feed "
                            "ends its stream with an error there)\n", db->dir, name);
            rc = TSDB_ERR_CORRUPT;
        }
        if (rc == 0) {
            s->first = first;
            s->last = wr.fno + wr.span - 1;
            s->last_ms = wr.commit_ms;
            memcpy(&s->last_hash, rec + 8, 8);
        }
    }
    free(rec);
    close(fd);
    if (rc == 0 && !any) {
        fprintf(stderr, "tsdb: %s: %s holds no record: it stays\n", db->dir, name);
        rc = TSDB_ERR_CORRUPT;
    }
    if (rc == 0) {
        s->size = (uint64_t)size;
        s->known = true;
    }
    return rc;
}

/* ------------------------------------------------------------------ passes */

static uint32_t free_pct(eng *db)
{
    uint32_t pct = 100;
    if (free_hook) {
        if (free_hook(free_hook_ctx, db->dir, &pct) == 0)
            return pct;
    }
    struct statvfs sv;
    if (fstatvfs(db->dirfd, &sv) == 0 && sv.f_blocks)
        pct = (uint32_t)((uint64_t)sv.f_bavail * 100 / sv.f_blocks);
    return pct;
}

/* One pass. Returns 0, or an error that ends the thread (the engine failed). */
static int clean_pass(eng *db)
{
    int rc = eng_check(db);
    if (rc)
        return rc;
    pthread_mutex_lock(&db->feed_lock);
    feed_cfg cfg = db->feed;
    uint64_t floor = db->feed_floor_event;
    size_t n = db->nfidx;
    fseg *snap = n ? malloc(n * sizeof *snap) : NULL;
    if (n && !snap) {
        pthread_mutex_unlock(&db->feed_lock);
        return 0;
    }
    if (n)
        memcpy(snap, db->fidx, n * sizeof *snap);
    pthread_mutex_unlock(&db->feed_lock);
    if (n < 2) { /* never the newest */
        free(snap);
        return 0;
    }
    /* sizes are needed for the bound: one fstat each, once; the rest of a segment is read only when the rules need it */
    uint64_t total = 0;
    for (size_t i = 0; i < n; i++) {
        if (snap[i].size == UINT64_MAX) {
            char name[64];
            struct stat sb;
            fwal_name(name, sizeof name, snap[i].seq);
            if (fstatat(db->dirfd, name, &sb, 0) != 0) {
                pthread_mutex_lock(&db->feed_lock); /* gone (by hand): forgotten */
                bool found;
                size_t at = index_find_locked(db, snap[i].seq, &found);
                if (found) {
                    memmove(db->fidx + at, db->fidx + at + 1, (db->nfidx - at - 1) * sizeof *db->fidx);
                    db->nfidx--;
                }
                pthread_mutex_unlock(&db->feed_lock);
                snap[i].size = 0;
                snap[i].seq = 0;
            } else {
                snap[i].size = (uint64_t)sb.st_size;
            }
        }
        total += snap[i].size;
    }
    int64_t now = eng_wall_ms();
    int64_t ret_ms = (int64_t)cfg.retention_s * 1000;
    bool pressure = free_pct(db) < cfg.min_free_pct;
    size_t k = 0; /* segments [0, k) go */
    uint64_t bytes = total;
    bool stop = false;
    for (; k + 1 < n && !stop; k++) {
        fseg *s = &snap[k];
        if (s->seq == 0)
            continue; /* vanished */
        bool why = false;
        if (pressure && k < CLEAN_BATCH) {
            why = true; /* free space beats age and size */
        } else {
            if (!s->known && seg_fill(db, s) != 0) {
                stop = true;
                break;
            }
            if (s->last <= floor || s->last_ms < now - ret_ms || bytes > cfg.max_bytes)
                why = true;
        }
        if (!why)
            break;
        bytes -= s->size;
    }
    /* the floor needs the last record of the newest segment removed in this pass, not only of the oldest */
    size_t last = k;
    while (last > 0 && snap[last - 1].seq == 0)
        last--;
    if (last == 0) {
        free(snap);
        return 0;
    }
    if (!snap[last - 1].known && seg_fill(db, &snap[last - 1]) != 0) {
        free(snap);
        return 0;
    }
    rc = fslots_set_floor(db, snap[last - 1].last, (uint32_t)snap[last - 1].last_hash);
    if (rc) {
        free(snap);
        return rc;
    }
    size_t done = 0, gone = 0; /* the first `done` of [0, k) were handled; `gone` of them unlinked here */
    uint64_t gone_bytes = 0;
    for (; done < k; done++) {
        if (snap[done].seq == 0)
            continue;
        char name[64];
        fwal_name(name, sizeof name, snap[done].seq);
        if (unlinkat(db->dirfd, name, 0) != 0 && errno != ENOENT) {
            fprintf(stderr, "tsdb: %s: removing %s failed (%s): retried at the next pass\n", db->dir, name,
                    strerror(errno));
            break; /* the floor already excludes it; later ones wait too, so the set stays contiguous */
        }
        gone++;
        gone_bytes += snap[done].size;
    }
    /* forget what is gone, then make the unlinks durable with no feed_lock held */
    pthread_mutex_lock(&db->feed_lock);
    for (size_t i = 0; i < done; i++) {
        bool found;
        if (snap[i].seq == 0)
            continue;
        size_t at = index_find_locked(db, snap[i].seq, &found);
        if (found) {
            memmove(db->fidx + at, db->fidx + at + 1, (db->nfidx - at - 1) * sizeof *db->fidx);
            db->nfidx--;
        }
    }
    db->feed_removed_segs += gone;
    db->feed_removed_bytes += gone_bytes;
    pthread_mutex_unlock(&db->feed_lock);
    free(snap);
    if (gone)
        rc = eng_sync_dir(db, "fsync of the directory after removing retained WAL segments");
    return rc;
}

static void *cleaner_main(void *arg)
{
    eng *db = arg;
    tsdb_thread_name("tsdb-feedgc");
    pthread_mutex_lock(&db->feed_lock);
    while (!db->cleaner_stop) {
        pthread_mutex_unlock(&db->feed_lock);
        int rc = clean_pass(db);
        pthread_mutex_lock(&db->feed_lock);
        if (rc != 0 && eng_check(db) != 0)
            break; /* a failed engine removes nothing more */
        if (db->cleaner_stop)
            break;
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&db->feed_cv, &db->feed_lock, &ts);
    }
    pthread_mutex_unlock(&db->feed_lock);
    return NULL;
}

int feed_cleaner_start(eng *db)
{
    if (db->has_cleaner || db->opt.read_only)
        return 0;
    int rc = index_scan(db);
    if (rc)
        return rc;
    db->cleaner_stop = false;
    if (tsdb_thread_create(&db->cleaner, cleaner_main, db) != 0)
        return TSDB_ERR_NOMEM;
    db->has_cleaner = true;
    return 0;
}

void feed_cleaner_stop(eng *db)
{
    if (!db->has_cleaner)
        return;
    pthread_mutex_lock(&db->feed_lock);
    db->cleaner_stop = true;
    pthread_cond_broadcast(&db->feed_cv);
    pthread_mutex_unlock(&db->feed_lock);
    pthread_join(db->cleaner, NULL);
    db->has_cleaner = false;
    db->cleaner_stop = false;
}

void feed_cleaner_wake(eng *db)
{
    if (!db->has_cleaner)
        return;
    pthread_mutex_lock(&db->feed_lock);
    pthread_cond_broadcast(&db->feed_cv);
    pthread_mutex_unlock(&db->feed_lock);
}
