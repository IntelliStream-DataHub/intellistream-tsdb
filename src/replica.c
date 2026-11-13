/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The engine as a replica of a cluster volume (docs/design-cluster.md 4, 5, 8.2; header: replica.h).
 *
 * The log is the leader's WAL, byte for byte: the records arrive with the segment and offset they have at the leader
 * (their checks bind them to both), are written to the same segment name at the same offset and made durable, and are
 * applied only up to the commit point the driver gives. Applying is one path for the live apply and for the replay at
 * open: it reads the records from the segment files (the page cache has what was just written) from a cursor
 * (apply_seq, apply_off) and puts their rows into the memtable of their segment. A memtable is one leader segment's:
 * when the cursor passes the end of a segment the memtable is sealed (moved to db->flushing) and a new one starts, so a
 * part's wal_seq and the rule "a segment is replayed only into partitions without a part from it" keep their meaning.
 * Only sealed memtables flush (every record of their segment is applied: parts never hold uncommitted rows), and a
 * segment is unlinked only at or below the commit floor the driver has saved durably (8.3).
 *
 * Nothing here assigns an fno. db->fno_next is the log end + 1; db->wal_fd, wal_seq and wal_off are the segment being
 * written, so a promotion (a later step) turns the same fields into a writer's.
 *
 * Locks: rp->mu (log, cursor, memtable switching; held across the apply) > flush_lock > db->lock > buf_lock;
 * segs_mu and unl_mu are leaves (the flusher takes flush_lock and then needs the segment list and the unlink list, so
 * those must not sit under rp->mu).
 *
 * Not in this step: rebuild and catch-up (R4), the network (R3). Marker records are carried in the log and counted,
 * and applied by apply_marker (later steps add the types).
 */
#include "replica.h"
#include "marker.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct rseg {
    uint64_t seq;
    uint64_t first; /* the first number of its first record, 0: no record */
    uint64_t len;   /* its valid bytes */
} rseg;

struct replica {
    pthread_mutex_t mu;
    pthread_mutex_t segs_mu;
    pthread_mutex_t unl_mu;
    rseg *segs; /* the wal_ segments present, ascending by seq (segs_mu) */
    size_t nsegs, capsegs;
    /* the apply cursor: the next record to apply is at apply_off of segment apply_seq (0: before the first segment) */
    uint64_t apply_seq, apply_off;
    uint64_t apply_next; /* the next position to apply */
    uint64_t act_seq;    /* the segment the active memtable holds (0: none yet) */
    uint64_t sealed_seq; /* the highest segment whose memtable was sealed: records of it or below are refused */
    uint64_t durable_next; /* positions below this are durable */
    uint64_t cut_gen, last_cut_next;
    _Atomic uint64_t floor_saved;
    _Atomic int64_t now_ms; /* the replicated retention clock: the newest commit_ms / NOW applied, never the wall clock */
    struct {
        uint64_t seq, fno_end;
    } *unl; /* flushed segments not yet unlinked, ascending (unl_mu) */
    size_t nunl, capunl;
    int rfd; /* the segment the cursor reads (-1: none) */
    uint64_t rfd_seq;
    uint8_t *rbuf;
    size_t rcap;
    /* the sender's cursor (replica_read): where the last read ended */
    uint64_t rd_seq, rd_off, rd_next;
};

#define RP_CHUNK ((size_t)1 << 20)

static int replica_apply_locked(eng *db, struct replica *rp, uint64_t commit);

void replica_now_advance(eng *db, int64_t ms)
{
    struct replica *rp = db->rp;
    int64_t cur = atomic_load_explicit(&rp->now_ms, memory_order_relaxed);
    while (ms > cur && !atomic_compare_exchange_weak(&rp->now_ms, &cur, ms)) {
    }
}

int64_t replica_now_ms(eng *db)
{
    if (eng_test_mutant == MUT_REPLICA_WALL_CLOCK) /* planted: the replica reads its own clock for retention */
        return eng_wall_ms();
    return atomic_load_explicit(&db->rp->now_ms, memory_order_relaxed);
}

static size_t rec_size(const uint8_t *h)
{
    uint32_t w = load32(h), n = w & 0x0fffffffu;
    return WAL_HDR + (size_t)n * ((w >> 28) == WAL_KIND_ROWS ? WAL_ROW : 1);
}

/* ------------------------------------------------------------ segment list */

static int segs_push(struct replica *rp, uint64_t seq, uint64_t first, uint64_t len)
{
    pthread_mutex_lock(&rp->segs_mu);
    if (rp->nsegs == rp->capsegs) {
        size_t c = rp->capsegs ? rp->capsegs * 2 : 16;
        rseg *s = realloc(rp->segs, c * sizeof *s);
        if (!s) {
            pthread_mutex_unlock(&rp->segs_mu);
            return TSDB_ERR_NOMEM;
        }
        rp->segs = s;
        rp->capsegs = c;
    }
    rp->segs[rp->nsegs++] = (rseg){seq, first, len};
    pthread_mutex_unlock(&rp->segs_mu);
    return 0;
}

/* the segment after `seq` (any seq, present or not): its entry in *out */
static bool segs_after(struct replica *rp, uint64_t seq, rseg *out)
{
    bool found = false;
    pthread_mutex_lock(&rp->segs_mu);
    for (size_t i = 0; i < rp->nsegs; i++)
        if (rp->segs[i].seq > seq) {
            *out = rp->segs[i];
            found = true;
            break;
        }
    pthread_mutex_unlock(&rp->segs_mu);
    return found;
}

static bool segs_find(struct replica *rp, uint64_t seq, rseg *out)
{
    bool found = false;
    pthread_mutex_lock(&rp->segs_mu);
    for (size_t i = 0; i < rp->nsegs; i++)
        if (rp->segs[i].seq == seq) {
            *out = rp->segs[i];
            found = true;
            break;
        }
    pthread_mutex_unlock(&rp->segs_mu);
    return found;
}

static void rfd_close(struct replica *rp)
{
    if (rp->rfd >= 0)
        close(rp->rfd);
    rp->rfd = -1;
    rp->rfd_seq = 0;
}

/* ------------------------------------------------------------------- open */

int replica_open_log(eng *db, uint64_t floor, int (*cut_tails)(eng *))
{
    if (db->feed_req || atomic_load(&db->feed_on)) {
        /* a retained segment (fwal_) is kept for the feed instead of unlinked: the replica unlinks flushed segments and
         * ignores fwal_ names in its numbering, which would break that invariant silently. Not designed yet. */
        fprintf(stderr, "tsdb: %s: a volume with a change feed cannot be opened as a replica yet\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    struct replica *rp = calloc(1, sizeof *rp);
    if (!rp)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&rp->mu, NULL);
    pthread_mutex_init(&rp->segs_mu, NULL);
    pthread_mutex_init(&rp->unl_mu, NULL);
    rp->rfd = -1;
    db->rp = rp; /* from here wal_check_segment lets rows name series that SERIES does not hold yet */

    wal_seg *segs = NULL;
    size_t nseg = 0, cap = 0;
    int rc = 0;
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) {
        uint64_t seq;
        int consumed = 0;
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

    uint64_t parts_seq = 0;
    for (size_t i = 0; i < db->nparts; i++)
        parts_seq = MAX(parts_seq, db->parts[i]->wal_seq);
    for (size_t k = 0; k < nseg && rc == 0; k++)
        rc = wal_check_segment(db, &segs[k]);
    if (rc == 0)
        rc = wal_check_order(segs, nseg, parts_seq);
    if (rc == 0)
        rc = wal_open_numbers(db, segs, nseg);
    if (rc == 0 && db->fno_torn_open) {
        /* A writer skips ahead by a belt here. A replica's numbers are the leader's: it cannot invent a bound, and a
         * wrong log end would make it ack positions it does not hold. It is rebuilt (8.1). */
        fprintf(stderr, "tsdb: %s: FNO has a damaged slot: a replica cannot tell its log end, rebuild it\n", db->dir);
        rc = TSDB_ERR_CORRUPT;
    }
    /* The log of a replica is contiguous (replica_append enforces it): a segment that does not start where the one before
     * it ends means a file went (damage, or a file system that reordered the unlinks of a cut), and the replica would
     * acknowledge positions it does not hold. FNO is only a bound: it never exceeds what the log and parts hold. */
    if (rc == 0) {
        uint64_t prev_end = 0, last_end = 0;
        for (size_t k = 0; k < nseg && rc == 0; k++) {
            if (!segs[k].fno_first)
                continue;
            if (prev_end && segs[k].fno_first != prev_end && eng_test_mutant != MUT_REPLICA_NO_GAP_CHECK) {
                fprintf(stderr, "tsdb: %s: segment %s starts at position %" PRIu64 ", but the segment before it ends at "
                                "%" PRIu64 ": a segment is missing, the replica's log is damaged\n",
                        db->dir, segs[k].name, segs[k].fno_first, prev_end - 1);
                rc = TSDB_ERR_CORRUPT;
            }
            prev_end = last_end = segs[k].fno_end;
        }
        if (rc == 0 && last_end && atomic_load(&db->fno_durable) > last_end) {
            fprintf(stderr, "tsdb: %s: FNO holds %" PRIu64 ", beyond the end of the log (%" PRIu64 "): the replica's log "
                            "lost records, rebuild it\n", db->dir, (uint64_t)atomic_load(&db->fno_durable), last_end - 1);
            rc = TSDB_ERR_CORRUPT;
        }
    }
    if (rc == 0 && cut_tails)
        rc = cut_tails(db);
    /* every torn tail goes now (the checks passed): a segment's file is its valid records and nothing else */
    bool cutany = false;
    for (size_t k = 0; k < nseg && rc == 0; k++) {
        if (segs[k].end >= segs[k].len || eng_test_mutant == MUT_REPLICA_NO_TORN_CUT)
            continue;
        int fd = openat(db->dirfd, segs[k].name, O_RDWR | O_CLOEXEC);
        if (fd < 0 || ftruncate(fd, (off_t)segs[k].end) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0))
            rc = TSDB_ERR_IO;
        if (fd >= 0)
            close(fd);
        cutany = true;
    }
    /* What is read back is the page cache's: after a process kill it holds bytes no power loss would keep. Only what
     * is synced now is durable (the driver acknowledges positions by durable_next): every segment goes to disk before
     * the open claims its positions, and the directory with it. A segment synced already costs a no-op. */
    for (size_t k = 0; k < nseg && rc == 0 && db->opt.fsync && eng_test_mutant != MUT_REPLICA_NO_OPEN_SYNC; k++) {
        if (segs[k].end == 0)
            continue;
        int fd = openat(db->dirfd, segs[k].name, O_RDWR | O_CLOEXEC);
        if (fd < 0 || tsdb_fdatasync(fd) != 0)
            rc = TSDB_ERR_IO;
        else
            atomic_fetch_add_explicit(&db->st_wal_syncs, 1, memory_order_relaxed);
        if (fd >= 0)
            close(fd);
        cutany = true;
    }
    if (rc == 0 && cutany && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO;
    for (size_t k = 0; k < nseg && rc == 0; k++)
        rc = segs_push(rp, segs[k].seq, segs[k].fno_first, segs[k].end);
    if (rc) {
        free(segs);
        return rc;
    }

    db->wal_seq = nseg ? segs[nseg - 1].seq : parts_seq;
    if (nseg) {
        db->wal_fd = openat(db->dirfd, segs[nseg - 1].name, O_WRONLY | O_APPEND | O_CLOEXEC);
        if (db->wal_fd < 0)
            rc = TSDB_ERR_IO;
        db->wal_off = segs[nseg - 1].end;
    }
    db->wal_open_removed = db->wal_flushed = nseg ? segs[0].seq - 1 : parts_seq;
    rp->durable_next = db->fno_next; /* synced above (fsync on): the files' bytes are durable now */
    rp->sealed_seq = parts_seq;
    rp->apply_next = db->fno_next;
    for (size_t k = 0; k < nseg; k++)
        if (segs[k].fno_first) {
            rp->apply_next = segs[k].fno_first;
            break;
        }
    free(segs);
    if (rc)
        return rc;
    db->active = mt_new(0);
    if (!db->active)
        return TSDB_ERR_NOMEM;
    atomic_store(&rp->floor_saved, floor);
    if (floor >= db->fno_next && floor > 0 && eng_test_mutant != MUT_REPLICA_NO_FLOOR_CHECK) {
        fprintf(stderr, "tsdb: %s: the commit floor %" PRIu64 " is beyond the log end %" PRIu64 ": the replica's log lost "
                        "records the floor says were applied\n", db->dir, floor, db->fno_next - 1);
        return TSDB_ERR_CORRUPT;
    }
    pthread_mutex_lock(&rp->mu);
    rc = replica_apply_locked(db, rp, floor);
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

void replica_free(eng *db)
{
    struct replica *rp = db->rp;
    if (!rp)
        return;
    rfd_close(rp);
    free(rp->rbuf);
    free(rp->segs);
    free(rp->unl);
    pthread_mutex_destroy(&rp->mu);
    pthread_mutex_destroy(&rp->segs_mu);
    pthread_mutex_destroy(&rp->unl_mu);
    free(rp);
    db->rp = NULL;
}

/* ---------------------------------------------------------------- memtables */

/* The active memtable must hold segment `seq`: the first record of a new segment seals the old one (all of its records
 * are applied: the cursor passed its end) and starts the next. */
static int switch_memtable(eng *db, struct replica *rp, uint64_t seq)
{
    if (rp->act_seq == seq)
        return 0;
    if (rp->act_seq == 0) { /* the placeholder of the open takes the first segment */
        pthread_mutex_lock(&db->buf_lock);
        db->active->wal_seq = seq;
        pthread_mutex_unlock(&db->buf_lock);
        rp->act_seq = seq;
        return 0;
    }
    if (db->nflushing >= MAX_FLUSHING) { /* only this thread (under rp->mu) adds to the list */
        int rc = replica_flush(db);
        if (rc)
            return rc;
        if (db->nflushing >= MAX_FLUSHING)
            return TSDB_ERR_IO;
    }
    memtable *nm = mt_new(seq);
    if (!nm)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    memtable *m = db->active;
    db->active = nm;
    m->fno_end = rp->apply_next; /* the end of the old segment: FNO holds it before the segment goes */
    db->flushing[db->nflushing++] = m;
    pthread_cond_signal(&db->flush_cv);
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    rp->sealed_seq = MAX(rp->sealed_seq, rp->act_seq);
    rp->act_seq = seq;
    return 0;
}

int replica_flush(eng *db)
{
    pthread_mutex_lock(&db->flush_lock);
    int rc = flush_waiting(db);
    pthread_mutex_unlock(&db->flush_lock);
    return rc;
}

/* Unlinks the flushed segments whose last position is at or below the saved floor (flush_lock held). */
static void unlink_due(eng *db)
{
    struct replica *rp = db->rp;
    uint64_t floor = atomic_load(&rp->floor_saved);
    if (eng_test_mutant == MUT_REPLICA_NO_FLOOR) /* planted: a segment goes before the floor is saved */
        floor = UINT64_MAX - 1;
    uint64_t seqs[64];
    for (;;) {
        size_t n = 0;
        pthread_mutex_lock(&rp->unl_mu);
        while (n < rp->nunl && n < 64 && rp->unl[n].fno_end - 1 <= floor)
            seqs[n] = rp->unl[n].seq, n++;
        if (n > 0) { /* rp->unl is NULL while the list was never used: memmove(NULL, ...) is undefined */
            memmove(rp->unl, rp->unl + n, (rp->nunl - n) * sizeof *rp->unl);
            rp->nunl -= n;
        }
        pthread_mutex_unlock(&rp->unl_mu);
        if (n == 0)
            return;
        size_t done = 0;
        for (; done < n; done++) {
            char name[64];
            wal_name(name, sizeof name, seqs[done]);
            if (unlinkat(db->dirfd, name, 0) != 0 && errno != ENOENT) {
                fprintf(stderr, "tsdb: removing %s failed (%s): it stays until a later flush removes it\n", name,
                        strerror(errno));
                break;
            }
        }
        if (done < n) { /* put the rest back, in front */
            pthread_mutex_lock(&rp->unl_mu);
            if (rp->nunl + (n - done) > rp->capunl) {
                size_t c = (rp->nunl + n) * 2;
                void *p = realloc(rp->unl, c * sizeof *rp->unl);
                if (!p) {
                    pthread_mutex_unlock(&rp->unl_mu);
                    return; /* the segments stay on disk and are found again at the next open */
                }
                rp->unl = p;
                rp->capunl = c;
            }
            memmove(rp->unl + (n - done), rp->unl, rp->nunl * sizeof *rp->unl);
            for (size_t i = done; i < n; i++) {
                rp->unl[i - done].seq = seqs[i];
                rp->unl[i - done].fno_end = floor + 1; /* due: below the floor already */
            }
            rp->nunl += n - done;
            pthread_mutex_unlock(&rp->unl_mu);
        }
        if (done == 0)
            return;
        pthread_mutex_lock(&rp->segs_mu); /* forget the unlinked ones */
        size_t keep = 0;
        for (size_t i = 0; i < rp->nsegs; i++) {
            bool gone = false;
            for (size_t j = 0; j < done; j++)
                gone |= rp->segs[i].seq == seqs[j];
            if (!gone)
                rp->segs[keep++] = rp->segs[i];
        }
        rp->nsegs = keep;
        pthread_mutex_unlock(&rp->segs_mu);
        pthread_mutex_lock(&db->lock);
        db->wal_removed = MAX(db->wal_removed, seqs[done - 1]);
        pthread_mutex_unlock(&db->lock);
        eng_sync_dir(db, "fsync of the directory after removing WAL segments"); /* fails the engine itself */
        if (done < n)
            return;
    }
}

void replica_flushed(eng *db, uint64_t seq, uint64_t fno_end)
{
    struct replica *rp = db->rp;
    pthread_mutex_lock(&rp->unl_mu);
    if (rp->nunl == rp->capunl) {
        size_t c = rp->capunl ? rp->capunl * 2 : 16;
        void *p = realloc(rp->unl, c * sizeof *rp->unl);
        if (!p) {
            pthread_mutex_unlock(&rp->unl_mu);
            return; /* the segment stays on disk; the next open finds it */
        }
        rp->unl = p;
        rp->capunl = c;
    }
    rp->unl[rp->nunl].seq = seq;
    rp->unl[rp->nunl].fno_end = fno_end;
    rp->nunl++;
    pthread_mutex_unlock(&rp->unl_mu);
    unlink_due(db);
}

int replica_floor_saved(eng *db, uint64_t floor)
{
    struct replica *rp = db->rp;
    if (!rp)
        return TSDB_ERR_INVALID;
    if (floor + 1 > rp->apply_next && floor != 0) /* not applied: a bug of the driver (racy read: it only grows) */
        return TSDB_ERR_INVALID;
    uint64_t cur = atomic_load(&rp->floor_saved);
    while (cur < floor && !atomic_compare_exchange_weak(&rp->floor_saved, &cur, floor)) {
    }
    pthread_mutex_lock(&db->flush_lock);
    unlink_due(db);
    pthread_mutex_unlock(&db->flush_lock);
    return 0;
}

/* ------------------------------------------------------------------ apply */

static int apply_rows(eng *db, struct replica *rp, uint64_t seq, const wal_rec *wr, uint64_t pmax)
{
    /* One record is added under one hold of buf_lock: a replica's rows are visible at once (no lsn gating), and a query
     * must see a committed record whole or not at all, as a sync-mode writer's rows are. So the rows are collected
     * first (the skip of a replay takes db->lock, which sits above buf_lock) and added in one step. */
    enum { B = 256 };
    uint32_t ids_s[B];
    int64_t ts_s[B];
    uint64_t sl_s[B];
    int ty_s[B];
    uint32_t *ids = ids_s;
    int64_t *ts = ts_s;
    uint64_t *sl = sl_s;
    int *ty = ty_s;
    void *heap = NULL;
    if (wr->n > B) {
        size_t n = wr->n;
        heap = malloc(n * (sizeof *ids + sizeof *ts + sizeof *sl + sizeof *ty));
        if (!heap) {
            eng_fail(db, "applying a committed record to the memtable", ENOMEM);
            return TSDB_ERR_NOMEM;
        }
        sl = heap;
        ts = (int64_t *)(sl + n);
        ids = (uint32_t *)(ts + n);
        ty = (int *)(ids + n);
    }
    int64_t cached_p = 0;
    uint64_t cached_seq = 0;
    bool have = false;
    uint32_t scount = eng_series_count(db);
    const uint8_t *r = wr->payload;
    size_t k = 0;
    int rc = 0;
    for (size_t i = 0; i < wr->n; i++, r += WAL_ROW) {
        uint32_t id;
        int64_t t;
        uint64_t slot;
        wal_row(r, &id, &t, &slot);
        if (id >= scount && (scount = eng_series_count(db)) <= id) {
            fprintf(stderr, "tsdb: %s: the record at position %" PRIu64 " names series %" PRIu32 ", which this replica's "
                            "SERIES lacks\n", db->dir, wr->fno, id);
            free(heap);
            return TSDB_ERR_CORRUPT;
        }
        if (seq <= pmax) { /* a replay: rows a part of this segment holds already must not come back */
            int64_t p = partition_of(db, id, t);
            if (!have || p != cached_p) {
                pthread_mutex_lock(&db->lock);
                cached_seq = partition_wal_seq(db, p);
                pthread_mutex_unlock(&db->lock);
                cached_p = p;
                have = true;
            }
            if (cached_seq >= seq && eng_test_mutant != MUT_REPLICA_NO_REPLAY_SKIP)
                continue;
        }
        ids[k] = id, ts[k] = t, sl[k] = slot, ty[k] = eng_series_type(db, id);
        k++;
    }
    if (k > 0) {
        pthread_mutex_lock(&db->buf_lock);
        rc = mt_reserve_rows(db->active, ids, 0, k);
        for (size_t a = 0; rc == 0 && a < k;) {
            size_t b = a + 1;
            while (b < k && ty[b] == ty[a])
                b++;
            rc = mt_add(db->active, ids + a, 0, ts + a, sl + a, 8, b - a, 0, ty[a]);
            a = b;
        }
        if (rc == 0)
            mt_note_fno(db->active, wr->fno, wr->n);
        pthread_mutex_unlock(&db->buf_lock);
        if (rc)
            eng_fail(db, "applying a committed record to the memtable", ENOMEM);
    }
    free(heap);
    (void)rp;
    return rc;
}

/* One marker, in position order, once committed. Every type is applied in a way a replay can repeat: a restart applies
 * the markers of the segments it still holds again, from the floor. SERIES and STATE entries the dictionary holds
 * already are checked and skipped, RETENTION and CLASS records by the offset they have in their file, NOW only
 * moves the clock up. A type that carries data (COMMIT, DROP) or means a role (EPOCH) is not applied yet (steps 3
 * and 4 of the replica): the engine fails rather than skip something a read would have to show. The reserved types
 * and unknown ones are skipped, as every replay does. */
static int apply_marker(eng *db, struct replica *rp, uint64_t seq, const wal_rec *wr)
{
    (void)seq;
    mk m;
    int rc = mk_decode(wr->payload, wr->n, &m);
    if (rc != 0) {
        fprintf(stderr, "tsdb: %s: the marker at position %" PRIu64 " does not decode: the replica's log is damaged\n", db->dir,
                wr->fno);
        return rc;
    }
    if (m.span != wr->span) {
        fprintf(stderr, "tsdb: %s: the marker at position %" PRIu64 " has span %" PRIu64 " in its header and %" PRIu32 " in its "
                        "payload\n", db->dir, wr->fno, wr->span, m.span);
        return TSDB_ERR_CORRUPT;
    }
    switch (m.type) {
    case MK_NOW:
        if (eng_test_mutant != MUT_REPLICA_NO_NOW_MARKER)
            replica_now_advance(db, m.u.now.now_ms);
        return 0;
    case MK_SERIES:
        return eng_series_apply(db, m.u.series.name, m.u.series.name_len, (int)m.u.series.vtype, m.u.series.id);
    case MK_STATE:
        return eng_state_apply(db, m.u.state.name, m.u.state.name_len, m.u.state.id);
    case MK_RETENTION:
        return eng_retention_apply(db, m.u.retention.series, m.u.retention.limit_ms, m.u.retention.record_off);
    case MK_CLASS:
        return cls_apply(db, m.u.cls.series, (int)m.u.cls.cls, m.u.cls.at, m.u.cls.record_off);
    case MK_COMMIT:
    case MK_DROP:
        fprintf(stderr, "tsdb: %s: the %s marker at position %" PRIu64 " is not applied by this replica yet\n", db->dir,
                mk_type_name(m.type), wr->fno);
        return TSDB_ERR_INVALID;
    default:
        (void)rp;
        return 0; /* EPOCH (step 4), the reserved types and unknown ones */
    }
}

static int rp_read_at(eng *db, struct replica *rp, uint64_t seq, uint64_t off, size_t want)
{
    if (rp->rfd < 0 || rp->rfd_seq != seq) {
        rfd_close(rp);
        char name[64];
        wal_name(name, sizeof name, seq);
        rp->rfd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
        if (rp->rfd < 0)
            return TSDB_ERR_IO;
        rp->rfd_seq = seq;
    }
    if (want > rp->rcap) {
        uint8_t *b = realloc(rp->rbuf, want);
        if (!b)
            return TSDB_ERR_NOMEM;
        rp->rbuf = b;
        rp->rcap = want;
    }
    size_t got = 0;
    while (got < want) {
        ssize_t r = pread(rp->rfd, rp->rbuf + got, want - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return TSDB_ERR_IO;
        got += (size_t)r;
    }
    return 0;
}

static int replica_apply_locked(eng *db, struct replica *rp, uint64_t commit)
{
    int rc = eng_check(db);
    uint64_t pmax = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        pmax = MAX(pmax, db->parts[i]->wal_seq);
    pthread_mutex_unlock(&db->lock);
    while (rc == 0) {
        rseg cur;
        if (rp->apply_seq == 0 || !segs_find(rp, rp->apply_seq, &cur)) {
            if (!segs_after(rp, rp->apply_seq, &cur))
                break;
            rp->apply_seq = cur.seq;
            rp->apply_off = 0;
        }
        if (rp->apply_off >= cur.len) { /* the end of what the segment holds: sealed when a later one exists */
            rseg nx;
            if (!segs_after(rp, cur.seq, &nx))
                break;
            rp->apply_seq = nx.seq;
            rp->apply_off = 0;
            continue;
        }
        size_t want = (size_t)MIN((uint64_t)RP_CHUNK, cur.len - rp->apply_off);
        if ((rc = rp_read_at(db, rp, cur.seq, rp->apply_off, want)) != 0)
            break;
        wal_rec wr;
        size_t sz = wal_record(rp->rbuf, want, cur.seq, rp->apply_off, &wr);
        if (sz == 0 && want >= WAL_HDR) { /* a record larger than the chunk */
            size_t need = rec_size(rp->rbuf);
            if (need > want && rp->apply_off + need <= cur.len) {
                if ((rc = rp_read_at(db, rp, cur.seq, rp->apply_off, need)) != 0)
                    break;
                sz = wal_record(rp->rbuf, need, cur.seq, rp->apply_off, &wr);
            }
        }
        if (sz == 0) {
            fprintf(stderr, "tsdb: %s: the record at offset %" PRIu64 " of segment %" PRIx64 " fails its checks: the "
                            "replica's log is damaged\n", db->dir, rp->apply_off, cur.seq);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (wr.fno != rp->apply_next && eng_test_mutant != MUT_REPLICA_NO_APPLY_CONTIG) {
            fprintf(stderr, "tsdb: %s: the record at offset %" PRIu64 " of segment %" PRIx64 " has number %" PRIu64
                            ", not %" PRIu64 ": the replica's log is damaged\n", db->dir, rp->apply_off, cur.seq, wr.fno,
                    rp->apply_next);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        uint64_t last = wr.fno + wr.span - 1;
        if (eng_test_mutant == MUT_REPLICA_EARLY_SEAL && (rc = switch_memtable(db, rp, cur.seq)) != 0)
            break; /* planted: the old segment seals when the cursor reaches the next, committed or not */
        if (last > commit || (last + 1 > rp->durable_next && eng_test_mutant != MUT_REPLICA_NO_DURABLE_GATE))
            break;
        /* The memtable switches (and so the old segment seals) only here, at the first COMMITTED record of a later
         * segment: a record that may still be cut must not seal anything. The new leader that cut it holds every
         * committed record, so it holds this segment's first record and starts a segment above it: nobody appends to a
         * sealed segment, which replica_append refuses (rows added to a flushed segment would be skipped by the next
         * replay, which skips what a part of the segment holds). */
        if ((rc = switch_memtable(db, rp, cur.seq)) != 0)
            break;
        /* The clock is the leader's NOW markers (design-cluster.md 5.4, C10), not the commit_ms of a record: the leader
         * stamps commit_ms with its wall clock, which its retention clock (opt.clock_ms, the floor) need not follow. */
        if (eng_test_mutant == MUT_REPLICA_COMMIT_CLOCK) /* planted: the clock follows every record's commit_ms */
            replica_now_advance(db, wr.commit_ms);
        rc = wr.kind == WAL_KIND_ROWS ? apply_rows(db, rp, cur.seq, &wr, pmax) : apply_marker(db, rp, cur.seq, &wr);
        if (rc)
            break;
        rp->apply_off += sz;
        rp->apply_next = wr.fno + wr.span;
    }
    if (rc && !atomic_load(&db->failed))
        eng_fail(db, "applying the replica's log", EIO); /* it cannot go on from a record it cannot apply */
    return rc;
}

int replica_apply(eng *db, uint64_t commit)
{
    struct replica *rp = db->rp;
    if (!rp)
        return TSDB_ERR_INVALID;
    if (eng_test_mutant == MUT_REPLICA_APPLY_ALL) /* planted: apply above the commit point */
        commit = UINT64_MAX;
    pthread_mutex_lock(&rp->mu);
    int rc = replica_apply_locked(db, rp, commit);
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

uint64_t replica_log_end(eng *db)
{
    struct replica *rp = db->rp;
    pthread_mutex_lock(&rp->mu);
    uint64_t v = db->fno_next - 1;
    pthread_mutex_unlock(&rp->mu);
    return v;
}

uint64_t replica_durable_end(eng *db)
{
    struct replica *rp = db->rp;
    pthread_mutex_lock(&rp->mu);
    uint64_t v = rp->durable_next - 1;
    pthread_mutex_unlock(&rp->mu);
    return v;
}

uint64_t replica_applied_end(eng *db)
{
    struct replica *rp = db->rp;
    pthread_mutex_lock(&rp->mu);
    uint64_t v = rp->apply_next - 1;
    pthread_mutex_unlock(&rp->mu);
    return v;
}

/* ----------------------------------------------------------------- append */

int replica_append(eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len)
{
    struct replica *rp = db->rp;
    if (!rp || seq == 0)
        return TSDB_ERR_INVALID;
    if (len == 0)
        return 0;
    pthread_mutex_lock(&rp->mu);
    int rc = eng_check(db);
    const uint8_t *p = buf;
    uint64_t next = db->fno_next, first = 0;
    for (size_t pos = 0; rc == 0 && pos < len;) {
        wal_rec wr;
        size_t sz = wal_record(p + pos, len - pos, seq, off + pos, &wr);
        if (sz == 0) {
            rc = TSDB_ERR_CORRUPT; /* a record that fails its checks is not the leader's: refused whole */
            break;
        }
        if (wr.fno != next || wr.fno >= FNO_BOUND_MAX || wr.span >= FNO_BOUND_MAX - wr.fno) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        if (!first)
            first = wr.fno;
        next = wr.fno + wr.span;
        pos += sz;
    }
    bool have = db->wal_fd >= 0, fresh = false;
    if (rc == 0) {
        if (have && seq == db->wal_seq)
            rc = off == db->wal_off && (seq > rp->sealed_seq || eng_test_mutant == MUT_REPLICA_NO_SEALED_RULE) ? 0 : TSDB_ERR_INVALID;
        else if ((seq > rp->sealed_seq || eng_test_mutant == MUT_REPLICA_NO_SEALED_RULE) && (!have || seq > db->wal_seq))
            rc = off == 0 ? 0 : TSDB_ERR_INVALID, fresh = true;
        else
            rc = TSDB_ERR_INVALID;
    }
    if (rc == 0 && fresh) {
        /* a torn tail is a torn append only in the newest segment (wal_check_order): the old one is durable first */
        if (have) {
            if (db->opt.fsync && eng_test_mutant != MUT_REPLICA_NO_ROT_SYNC && tsdb_fdatasync(db->wal_fd) != 0) {
                eng_fail(db, "replica WAL fdatasync", errno);
                rc = TSDB_ERR_IO;
            } else {
                rp->durable_next = MAX(rp->durable_next, first);
                close(db->wal_fd);
                db->wal_fd = -1;
            }
        }
        int fd = -1;
        if (rc == 0 && (rc = wal_open_segment(db, seq, &fd)) == 0) {
            db->wal_fd = fd;
            db->wal_seq = seq;
            db->wal_off = 0;
            if ((rc = segs_push(rp, seq, 0, 0)) != 0)
                eng_fail(db, "listing a new segment", ENOMEM);
        }
    }
    if (rc == 0 && write_full(db->wal_fd, buf, len) != 0) {
        eng_fail(db, "replica WAL write", errno);
        rc = TSDB_ERR_IO;
    }
    if (rc == 0) {
        db->wal_off += len;
        db->fno_next = next;
        if (eng_test_mutant == MUT_REPLICA_ACK_UNSYNCED) /* planted: acknowledge what is not synced */
            rp->durable_next = next;
        pthread_mutex_lock(&rp->segs_mu);
        rseg *s = &rp->segs[rp->nsegs - 1];
        if (!s->first)
            s->first = first;
        s->len = db->wal_off;
        pthread_mutex_unlock(&rp->segs_mu);
        atomic_fetch_add_explicit(&db->st_wal_bytes, len, memory_order_relaxed);
    }
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

void (*replica_test_sync_hook)(eng *db); /* test hook: runs after the fdatasync, before replica_sync takes its lock again */

int replica_sync(eng *db)
{
    struct replica *rp = db->rp;
    if (!rp)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&rp->mu);
    int rc = eng_check(db);
    if (rc || db->wal_fd < 0 || rp->durable_next >= db->fno_next) {
        pthread_mutex_unlock(&rp->mu);
        return rc;
    }
    int fd = db->opt.fsync ? dup(db->wal_fd) : -1;
    uint64_t target = db->fno_next, gen = rp->cut_gen;
    pthread_mutex_unlock(&rp->mu);
    if (db->opt.fsync) {
        int err = fd < 0 ? errno : tsdb_fdatasync(fd) != 0 ? errno : 0;
        if (fd >= 0)
            close(fd);
        if (err) {
            eng_fail(db, "replica WAL fdatasync", err);
            return TSDB_ERR_IO;
        }
        atomic_fetch_add_explicit(&db->st_wal_syncs, 1, memory_order_relaxed);
    }
    if (replica_test_sync_hook)
        replica_test_sync_hook(db);
    pthread_mutex_lock(&rp->mu);
    /* gaps 9 and 12: a sync that began before a cut does not cover what was written again after it */
    uint64_t end = rp->cut_gen == gen || eng_test_mutant == MUT_REPLICA_NO_CUT_GEN ? target : MIN(target, rp->last_cut_next);
    end = MIN(end, db->fno_next);
    rp->durable_next = MAX(rp->durable_next, end);
    pthread_mutex_unlock(&rp->mu);
    return 0;
}

/* -------------------------------------------------------------------- cut */

int replica_cut(eng *db, uint64_t last)
{
    struct replica *rp = db->rp;
    if (!rp)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&rp->mu);
    pthread_mutex_lock(&db->flush_lock); /* a flush unlinks segments and edits the list: the cut sees one list */
    int rc = eng_check(db);
    if (rc == 0 && last + 1 == db->fno_next) { /* nothing above it */
        pthread_mutex_unlock(&db->flush_lock);
        pthread_mutex_unlock(&rp->mu);
        return 0;
    }
    if (rc == 0 && last + 1 > db->fno_next)
        rc = TSDB_ERR_INVALID;
    if (rc == 0 && last + 1 < rp->apply_next) {
        fprintf(stderr, "tsdb: %s: a cut after position %" PRIu64 " is below the applied end %" PRIu64 ": refused\n",
                db->dir, last, rp->apply_next - 1);
        rc = TSDB_ERR_CORRUPT;
    }
    rseg *ss = NULL;
    size_t n = 0;
    if (rc == 0) {
        pthread_mutex_lock(&rp->segs_mu);
        n = rp->nsegs;
        ss = malloc((n ? n : 1) * sizeof *ss);
        if (ss && n)
            memcpy(ss, rp->segs, n * sizeof *ss);
        pthread_mutex_unlock(&rp->segs_mu);
        if (!ss)
            rc = TSDB_ERR_NOMEM;
    }
    /* the segment that holds `last` and the offset after its record */
    long t = -1;
    uint64_t off_cut = 0;
    for (long i = (long)n - 1; rc == 0 && i >= 0; i--)
        if (ss[i].first && ss[i].first <= last) {
            t = i;
            break;
        }
    if (rc == 0 && t >= 0) {
        char name[64];
        uint8_t *fb;
        size_t flen;
        wal_name(name, sizeof name, ss[t].seq);
        if ((rc = read_file(db->dirfd, name, &fb, &flen)) == 0) {
            bool found = false;
            for (size_t pos = 0; pos < flen && !found;) {
                wal_rec wr;
                size_t sz = wal_record(fb + pos, flen - pos, ss[t].seq, pos, &wr);
                if (sz == 0 || wr.fno + wr.span - 1 > last)
                    break;
                pos += sz;
                if (wr.fno + wr.span - 1 == last)
                    found = true, off_cut = pos;
            }
            free(fb);
            if (!found) {
                fprintf(stderr, "tsdb: %s: position %" PRIu64 " is not the end of a record: the cut is refused\n",
                        db->dir, last);
                rc = TSDB_ERR_CORRUPT;
            }
        }
    } else if (rc == 0 && (n == 0 || ss[0].first != last + 1)) {
        /* every segment lies above the cut: it must be exactly the start of the retained log */
        fprintf(stderr, "tsdb: %s: position %" PRIu64 " is not a record end of the retained log: the cut is refused\n",
                db->dir, last);
        rc = TSDB_ERR_CORRUPT;
    }
    if (rc == 0 && eng_test_mutant == MUT_REPLICA_KEEP_CUT) { /* planted: a cut that changes nothing on disk */
        free(ss);
        pthread_mutex_unlock(&db->flush_lock);
        pthread_mutex_unlock(&rp->mu);
        return 0;
    }
    /* Newest first, the directory synced after each: a file system may reorder unsynced unlinks, and a crash with the
     * newest segment present and a middle one gone would leave a hole in the log (the open refuses it). With a sync per
     * unlink a crash leaves a prefix of the log. A cut is rare. */
    for (long i = (long)n - 1; rc == 0 && i > t; i--) {
        char name[64];
        wal_name(name, sizeof name, ss[i].seq);
        if (unlinkat(db->dirfd, name, 0) != 0 && errno != ENOENT) {
            eng_fail(db, "removing a segment above a cut", errno);
            rc = TSDB_ERR_IO;
        } else if (eng_test_mutant != MUT_REPLICA_NO_CUT_DIRSYNC) { /* planted: no directory fsync */
            rc = eng_sync_dir(db, "fsync of the directory after a cut"); /* fails the engine itself */
        }
    }
    if (rc == 0 && t >= 0) {
        int fd = db->wal_fd >= 0 && db->wal_seq == ss[t].seq ? dup(db->wal_fd) : -1;
        if (fd < 0) {
            char name[64];
            wal_name(name, sizeof name, ss[t].seq);
            fd = openat(db->dirfd, name, O_RDWR | O_CLOEXEC);
        }
        if (fd < 0 || (ss[t].len != off_cut && ftruncate(fd, (off_t)off_cut) != 0) ||
            (db->opt.fsync && eng_test_mutant != MUT_REPLICA_NO_CUT_SYNC && tsdb_fdatasync(fd) != 0)) {
            eng_fail(db, "cutting a segment", errno);
            rc = TSDB_ERR_IO;
        }
        if (fd >= 0)
            close(fd);
    }
    if (rc == 0) {
        if (db->wal_fd >= 0)
            close(db->wal_fd);
        db->wal_fd = -1;
        if (t >= 0) {
            char name[64];
            wal_name(name, sizeof name, ss[t].seq);
            db->wal_fd = openat(db->dirfd, name, O_WRONLY | O_APPEND | O_CLOEXEC);
            if (db->wal_fd < 0) {
                eng_fail(db, "reopening a segment after a cut", errno);
                rc = TSDB_ERR_IO;
            }
            db->wal_seq = ss[t].seq;
            db->wal_off = off_cut;
        } else {
            db->wal_off = 0;
        }
        pthread_mutex_lock(&rp->segs_mu);
        rp->nsegs = (size_t)(t + 1);
        if (t >= 0)
            rp->segs[t].len = off_cut;
        pthread_mutex_unlock(&rp->segs_mu);
        db->fno_next = last + 1;
        rp->durable_next = last + 1; /* what stays was synced (the cut's own sync, the rotations') */
        rp->cut_gen++;
        rp->last_cut_next = last + 1;
        /* the cursor: it is at the applied end, which is at or below the cut; if its segment went, it is at the end of
         * the segment that holds the cut */
        rseg dummy;
        if (rp->apply_seq && !segs_find(rp, rp->apply_seq, &dummy)) {
            rp->apply_seq = t >= 0 ? ss[t].seq : 0;
            rp->apply_off = t >= 0 ? off_cut : 0;
        }
        if (rp->act_seq && rp->act_seq > (t >= 0 ? ss[t].seq : 0) && !segs_find(rp, rp->act_seq, &dummy)) {
            /* the memtable of a segment the cut removed: nothing of it was applied (the cut is at or above the applied
             * end), so it is a placeholder again */
            pthread_mutex_lock(&db->buf_lock);
            if (db->active->rows == 0)
                db->active->wal_seq = 0, rp->act_seq = 0;
            pthread_mutex_unlock(&db->buf_lock);
        }
        rfd_close(rp);
        rp->rd_seq = 0;
    }
    free(ss);
    pthread_mutex_unlock(&db->flush_lock);
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

/* ------------------------------------------------------------------- read */

size_t replica_read_win_min = 65536; /* test hook: the smallest read window, so a test reaches the growth of the window */

int replica_read(eng *db, uint64_t from, uint8_t **buf, size_t *len, size_t cap, uint64_t *seq, uint64_t *off,
                 uint64_t *last)
{
    struct replica *rp = db->rp;
    *buf = NULL;
    *len = 0;
    if (!rp)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&rp->mu);
    int rc = 0;
    if (from > db->fno_next || from == 0) {
        pthread_mutex_unlock(&rp->mu);
        return TSDB_ERR_INVALID;
    }
    if (from == db->fno_next) {
        *last = from - 1;
        *seq = db->wal_seq;
        *off = db->wal_off;
        pthread_mutex_unlock(&rp->mu);
        return 0;
    }
    rseg s;
    bool ok = false;
    pthread_mutex_lock(&rp->segs_mu);
    for (size_t i = rp->nsegs; i-- > 0;)
        if (rp->segs[i].first && rp->segs[i].first <= from) {
            s = rp->segs[i];
            ok = true;
            break;
        }
    pthread_mutex_unlock(&rp->segs_mu);
    if (!ok) {
        pthread_mutex_unlock(&rp->mu);
        return TSDB_ERR_NOTFOUND; /* below the log's start: it is in parts, a snapshot is needed */
    }
    /* pread from where the last read of this segment ended (rd_*: the sender walks the log in order), in windows, never
     * the whole segment: a catch-up in chunks costs the bytes it sends, not a square of the segment. The segment is
     * append-only between cuts, and a cut holds rp->mu like this does. */
    char name[64];
    wal_name(name, sizeof name, s.seq);
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        pthread_mutex_unlock(&rp->mu);
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    size_t flen = (size_t)s.len;
    size_t pos0 = rp->rd_seq == s.seq && rp->rd_next && rp->rd_next <= from && rp->rd_off <= flen ? (size_t)rp->rd_off : 0;
    size_t win = MAX(cap, replica_read_win_min);
    uint8_t *fb = NULL;
    size_t start = 0, blen = 0;
    bool found = false, done = false;
    uint64_t lastpos = 0, next_fno = 0;
    size_t next_off = 0;
    uint8_t *out = NULL;
    size_t outlen = 0, outcap = 0;
    while (rc == 0 && !done && pos0 < flen) {
        size_t want = MIN(win, flen - pos0);
        uint8_t *nb = realloc(fb, want);
        if (!nb) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        fb = nb;
        ssize_t got = pread(fd, fb, want, (off_t)pos0);
        if (got < 0 || (size_t)got != want) {
            rc = TSDB_ERR_IO;
            break;
        }
        blen = want;
        size_t pos = 0;
        bool window_end = pos0 + blen < flen; /* the file goes on beyond this window */
        while (pos < blen) {
            wal_rec wr;
            size_t sz = wal_record(fb + pos, blen - pos, s.seq, pos0 + pos, &wr);
            if (sz == 0) {
                if (window_end)
                    break; /* a record that does not fit the window: read from here with a larger one */
                rc = TSDB_ERR_CORRUPT;
                break;
            }
            if (!found) {
                if (wr.fno == from)
                    found = true, start = pos0 + pos;
                else if (wr.fno > from) {
                    rc = TSDB_ERR_INVALID; /* from is inside a record */
                    break;
                }
            }
            if (found) {
                if (outlen > 0 && outlen + sz > cap) {
                    done = true;
                    break;
                }
                if (outlen + sz > outcap) {
                    size_t nc = MAX(outlen + sz, outcap * 2);
                    uint8_t *no = realloc(out, nc);
                    if (!no) {
                        rc = TSDB_ERR_NOMEM;
                        break;
                    }
                    out = no, outcap = nc;
                }
                memcpy(out + outlen, fb + pos, sz);
                outlen += sz;
                lastpos = wr.fno + wr.span - 1;
                next_fno = lastpos + 1;
                next_off = pos0 + pos + sz;
            }
            pos += sz;
        }
        if (rc || done)
            break;
        if (pos == 0 && window_end)
            win *= 2; /* one record is larger than the window */
        pos0 += pos;
    }
    free(fb);
    close(fd);
    if (rc == 0 && !found)
        rc = TSDB_ERR_INVALID;
    if (rc == 0) {
        *buf = out;
        *len = outlen;
        *seq = s.seq;
        *off = start;
        *last = lastpos;
        rp->rd_seq = s.seq, rp->rd_off = next_off, rp->rd_next = next_fno;
    } else {
        free(out);
    }
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

/* ------------------------------------------------------------ the leader side */
/*
 * The replicated backend of the namespace-event hook (internal.h, struct jr_ops; design-cluster.md 5.2, design-ha-modes.md
 * 2.1): what a leader's engine does at a large insert, a replace, a delete and a drop so that every replica carries the
 * same event out. The driver installs struct jr_ops (db->jr) before the engine is used.
 *
 * COMMIT (insert, replace, delete): the pending part files are shipped to every replica, which stages and syncs them
 * (replica_stage); then a COMMIT marker (the leader's file names, sizes, checksums; F) is appended and the writer waits
 * for its commit point; then the leader publishes as it always does (parts_commit under publish_lock). A failure after
 * the marker is out fails the engine: a replica may apply it, so the leader must not go on as if nothing happened.
 * F is the highest segment the leader has flushed (db->wal_flushed) once lw_enter's flush is done: every row of the
 * write's keys that is older than the write is in a part of the leader, in a segment at or below F, so a replica that
 * flushes through F before it commits has the same rows in parts (last write wins on each side the same way).
 *
 * DROP: the marker is the first record of the segment the flush starts (mt_flush_ex), so the replica that applies it
 * has sealed every memtable the leader flushed, and none of the rows written after it.
 */
static int leader_commit(eng *db, const struct jr_commit_arg *ja)
{
    const struct jr_ops *jr = db->jr;
    if (ja->n == 0)
        return 0;
    if (ja->n > MK_FILES_MAX)
        return TSDB_ERR_INVALID;
    size_t cap = 0;
    for (size_t i = 0; i < ja->n; i++)
        cap += 24 + 8 + 64;
    uint8_t *files = malloc(cap);
    if (!files)
        return TSDB_ERR_NOMEM;
    size_t len = 0;
    int rc = 0;
    for (size_t i = 0; i < ja->n && rc == 0; i++) {
        part *p = ja->ps[i];
        if (!p->pending[0] || !p->map) {
            rc = TSDB_ERR_INVALID; /* not a pending part */
            break;
        }
        mk_file f = {.name = p->pending, .name_len = (uint32_t)strlen(p->pending), .size = p->map_len,
                     .xxh64 = xxh64(p->map, p->map_len, 0), .header_check = p->index_hash};
        len += mk_file_put(files + len, &f);
        rc = jr->ship(jr->ctx, db, p->pending, p->map_len);
    }
    if (rc) {
        free(files);
        return rc;
    }
    pthread_mutex_lock(&db->lock);
    uint64_t through = db->wal_flushed;
    pthread_mutex_unlock(&db->lock);
    mk m = {.type = MK_COMMIT, .span = 1};
    m.u.commit = (mk_commit){.op = (uint8_t)ja->op, .keys_form = MK_KEYS_NONE, .nfiles = (uint32_t)ja->n,
                             .flush_through = through, .events = 1, .files = files, .files_len = len};
    uint64_t fno = 0;
    rc = wal_marker_mk(db, &m, &fno);
    free(files);
    if (rc)
        return rc; /* the marker may be in the log (a failed sync): the engine is failed by the WAL's own rules */
    rc = jr->wait(jr->ctx, db, fno);
    if (rc) {
        eng_fail(db, "a COMMIT marker was appended but its commit point did not come", EIO);
        return rc;
    }
    return 0;
}

static int leader_drop(eng *db, int64_t ts)
{
    const struct jr_ops *jr = db->jr;
    mk m = {.type = MK_DROP, .span = 1};
    m.u.drop = (mk_drop){.dropped_before = ts, .partition = MK_NO_PARTITION};
    uint64_t fno = 0;
    int rc = mt_flush_ex(db, &m, &fno);
    if (rc)
        return rc;
    rc = jr->wait(jr->ctx, db, fno);
    if (rc)
        eng_fail(db, "a DROP marker was appended but its commit point did not come", EIO);
    return rc;
}

int jr_intent(eng *db, enum jr_kind kind, const void *arg)
{
    if (!db->jr)
        return 0;
    switch (kind) {
    case JR_COMMIT:
        return leader_commit(db, arg);
    case JR_DROP:
        return leader_drop(db, *(const int64_t *)arg);
    default:
        return 0; /* the dictionary and retention markers are the driver's (it appends them with wal_marker_mk) */
    }
}
