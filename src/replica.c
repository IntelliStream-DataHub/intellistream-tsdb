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
 * Locks: rp->apply_mu (one applier: held across replica_apply) > rp->stage_mu (one replica_stage, and a COMMIT's
 * reads of its staged files) > rp->mu (log, cursor, memtable switching) > flush_lock > db->lock > buf_lock; segs_mu and
 * unl_mu are leaves (the flusher takes flush_lock and then needs the segment list and the unlink list, so those must
 * not sit under rp->mu). The apply holds rp->mu while it reads and applies records, but lets go of it for the slow
 * work of a marker (a COMMIT's flush and the reads of its files, a DROP's flush, a CLASS record's purge): appends,
 * syncs and the ends the driver acknowledges by do not wait for that (R2 REVIEW M1). What it did meanwhile is kept only
 * when no cut came in between (cut_gen); else it is dropped and the record read again from the cursor. Between records
 * it lets go of rp->mu whenever another thread waits for it (mu_want, rp_lock), so a long run of row records does not
 * hold the ACKs back either; one record is applied whole. replica_promote takes apply_mu, so it never sees an apply half
 * way. The log end, the durable end and the applied end are atomics, read without any lock.
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct rseg {
    uint64_t seq;
    uint64_t first; /* the first number of its first record, 0: no record */
    uint64_t len;   /* its valid bytes */
} rseg;

/* The RSTATE file: which COMMIT and DROP markers are carried out already. A replica writes it as it applies them, a
 * leader as it publishes its own (jr_precommit, jr_commit, leader_rs_drop), so a deposed leader reopened as a replica
 * skips its own published markers instead of applying them again (design-cluster.md 6.3). */
struct rstate {
    uint64_t done; /* the last position of the last COMMIT or DROP applied */
    uint64_t doing_pos, doing_b0, doing_n; /* a COMMIT being published: its position, first block, parts (0: none) */
    struct rdrop {
        uint64_t pos;  /* the DROP's position */
        int64_t until; /* dropped_before */
    } *drops;
    size_t ndrops;
    uint64_t kept; /* the wal_ segments up to this seq hold no row outside the parts: a writer's open neither replays nor
                    * removes them (a leader's flushed segments kept for its replicas, a promoted replica's) */
};

struct replica {
    pthread_mutex_t apply_mu; /* one replica_apply at a time (outermost) */
    atomic_uint mu_want;      /* threads other than the applier waiting for mu (rp_lock): the applier steps aside */
    pthread_mutex_t mu;
    pthread_mutex_t segs_mu;
    pthread_mutex_t unl_mu;
    pthread_mutex_t stage_mu; /* one replica_stage at a time (outermost: taken before mu, held across the file's I/O) */
    rseg *segs; /* the wal_ segments present, ascending by seq (segs_mu) */
    size_t nsegs, capsegs;
    /* the apply cursor: the next record to apply is at apply_off of segment apply_seq (0: before the first segment) */
    uint64_t apply_seq, apply_off;
    _Atomic uint64_t apply_next; /* the next position to apply (written under mu, read anywhere) */
    uint64_t act_seq;    /* the segment the active memtable holds (0: none yet) */
    uint64_t sealed_seq; /* the highest segment whose memtable was sealed: records of it or below are refused */
    _Atomic uint64_t durable_next; /* positions below this are durable (written under mu, read anywhere) */
    _Atomic uint64_t log_next;     /* db->fno_next, for readers without a lock (written under mu) */
    uint64_t cut_gen, last_cut_next;
    uint64_t epoch, epoch_pos; /* the newest EPOCH marker applied (0: none): its epoch and position */
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
    /* files the leader shipped for its COMMIT markers (replica_stage), and the COMMIT markers appended and not yet
     * applied: a marker's positions are not durable here until every file it names is staged (hold_cap) */
    struct rstg {
        uint64_t key, size, xxh;
    } *stg;
    size_t nstg, capstg;
    struct rcm {
        uint64_t first, last;
        uint32_t nf;
        struct rcf {
            uint64_t key, size, xxh;
            char name[64];
        } *f;
    } *cm; /* ascending by first */
    size_t ncm, capcm;
    struct rstate rs; /* the RSTATE file */
};

#define RP_CHUNK ((size_t)1 << 20)
#define RP_AGAIN 1 /* a marker's apply: a cut came while rp->mu was let go, nothing was done, read the record again */

static int replica_apply_locked(eng *db, struct replica *rp, uint64_t commit);
static int rs_load(eng *db, struct rstate *rs);
static int open_scan(eng *db, struct replica *rp);

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
    pthread_mutex_init(&rp->stage_mu, NULL);
    pthread_mutex_init(&rp->apply_mu, NULL);
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
    rp->log_next = db->fno_next;
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
    if ((rc = rs_load(db, &rp->rs)) == 0)
        rc = open_scan(db, rp);
    if (rc)
        return rc;
    pthread_mutex_lock(&rp->mu);
    rc = replica_apply_locked(db, rp, floor);
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

void replica_free(eng *db)
{
    if (db->lrs) { /* a leader's RSTATE (the leader side, below) */
        free(db->lrs->drops);
        free(db->lrs);
        db->lrs = NULL;
    }
    free(db->lf); /* the links of its COMMITs' files: they stay on disk (removed at the next attach) */
    db->lf = NULL;
    db->nlf = db->caplf = 0;
    free(db->lk); /* the leader's kept segments: they stay on disk */
    db->lk = NULL;
    db->nlk = db->caplk = 0;
    struct replica *rp = db->rp;
    if (!rp)
        return;
    rfd_close(rp);
    free(rp->rbuf);
    free(rp->segs);
    free(rp->unl);
    free(rp->stg);
    for (size_t i = 0; i < rp->ncm; i++)
        free(rp->cm[i].f);
    free(rp->cm);
    free(rp->rs.drops);
    pthread_mutex_destroy(&rp->mu);
    pthread_mutex_destroy(&rp->segs_mu);
    pthread_mutex_destroy(&rp->unl_mu);
    pthread_mutex_destroy(&rp->stage_mu);
    pthread_mutex_destroy(&rp->apply_mu);
    free(rp);
    db->rp = NULL;
}

/* ---------------------------------------------------------------- memtables */

/* The list's length changes under db->lock (a flush removes, the apply thread adds): read it there (tsan). */
static size_t nflushing_now(eng *db)
{
    pthread_mutex_lock(&db->lock);
    size_t n = db->nflushing;
    pthread_mutex_unlock(&db->lock);
    return n;
}

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
    if (nflushing_now(db) >= MAX_FLUSHING) { /* only this thread (under rp->mu) adds to the list */
        int rc = replica_flush(db);
        if (rc)
            return rc;
        if (nflushing_now(db) >= MAX_FLUSHING)
            return TSDB_ERR_IO;
    }
    memtable *nm = mt_new(seq);
    if (!nm)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&db->flush_lock); /* flush_waiting reads the list under flush_lock alone (tsan): add under it too */
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    memtable *m = db->active;
    db->active = nm;
    m->fno_end = rp->apply_next; /* the end of the old segment: FNO holds it before the segment goes */
    db->flushing[db->nflushing++] = m;
    pthread_cond_signal(&db->flush_cv);
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    pthread_mutex_unlock(&db->flush_lock);
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

/* ------------------------------------------- staged files, RSTATE, COMMIT and DROP (design-cluster.md 5.2-5.3) */
/*
 * COMMIT (an insert, replace or delete that goes to parts): the leader ships its pending part files before it appends
 * the marker. A replica keeps each as `stage_<xxh64 of the leader's name>.part` (file and directory synced) and the
 * marker's positions are not durable here (hold_cap: not acknowledged, not applied) until every file it names is
 * staged with the size and xxh64 the marker says. The apply flushes every sealed memtable (the leader flushed its own
 * before the marker: last write wins), links the staged files to tmp_0_<n>.part (pid 0 is never a writer's), and
 * commits them like a large insert (parts_commit under publish_lock). A crash anywhere leaves the stage_ files (the
 * link is swept, the stage stays) and RSTATE says whether the commit happened (see rs_committed).
 * RSTATE is rewritten whole (tmp, fsync, rename, fsync of the directory): `done` is the last position of the last COMMIT
 * or DROP applied (a replay skips markers up to it), `doing` the COMMIT being published (position, first block, parts),
 * `drops` the DROPs applied, which a replay needs to keep the rows of dropped partitions out of the memtable again.
 */
#define RS_FILE "RSTATE"
#define RS_MAGIC UINT64_C(0x5453544154535231)

static uint64_t stage_key(const char *name) { return xxh64(name, strlen(name), 0); }

static void stage_path(char *buf, size_t cap, uint64_t key) { snprintf(buf, cap, "stage_%016" PRIx64 ".part", key); }

static void put64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }

static uint64_t get64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

static struct rstg *stg_find(struct replica *rp, uint64_t key)
{
    for (size_t i = 0; i < rp->nstg; i++)
        if (rp->stg[i].key == key)
            return &rp->stg[i];
    return NULL;
}

static int stg_put(struct replica *rp, uint64_t key, uint64_t size, uint64_t xxh)
{
    struct rstg *s = stg_find(rp, key);
    if (!s) {
        if (rp->nstg == rp->capstg) {
            size_t c = rp->capstg ? rp->capstg * 2 : 16;
            struct rstg *x = realloc(rp->stg, c * sizeof *x);
            if (!x)
                return TSDB_ERR_NOMEM;
            rp->stg = x;
            rp->capstg = c;
        }
        s = &rp->stg[rp->nstg++];
    }
    *s = (struct rstg){key, size, xxh};
    return 0;
}

static void stg_del(struct replica *rp, uint64_t key)
{
    struct rstg *s = stg_find(rp, key);
    if (s)
        *s = rp->stg[--rp->nstg];
}

static bool cm_satisfied(struct replica *rp, const struct rcm *c)
{
    for (uint32_t i = 0; i < c->nf; i++) {
        const struct rstg *s = stg_find(rp, c->f[i].key);
        if (!s || s->size != c->f[i].size || s->xxh != c->f[i].xxh)
            return false;
    }
    return true;
}

/* The lowest position that is not durable here for want of a staged file: durable_next never passes the first COMMIT
 * marker whose files are not all staged (UINT64_MAX: no hold). rp->mu held. */
static uint64_t hold_cap(struct replica *rp)
{
    if (eng_test_mutant == MUT_REPLICA_NO_STAGE_HOLD)
        return UINT64_MAX;
    for (size_t i = 0; i < rp->ncm; i++)
        if (!cm_satisfied(rp, &rp->cm[i]))
            return rp->cm[i].first;
    return UINT64_MAX;
}

static void durable_raise(struct replica *rp, uint64_t v)
{
    uint64_t cap = hold_cap(rp);
    if (v > cap)
        v = cap;
    if (v > rp->durable_next)
        rp->durable_next = v;
}

/* a COMMIT marker that arrived (appended, or found at open): remembered until it is applied or cut */
static int cm_add(struct replica *rp, const wal_rec *wr, const mk *m)
{
    const mk_commit *c = &m->u.commit;
    uint64_t last = wr->fno + wr->span - 1;
    if (last <= rp->rs.done)
        return 0; /* applied already (RSTATE): nothing to stage */
    if (rp->ncm == rp->capcm) {
        size_t cap = rp->capcm ? rp->capcm * 2 : 8;
        struct rcm *x = realloc(rp->cm, cap * sizeof *x);
        if (!x)
            return TSDB_ERR_NOMEM;
        rp->cm = x;
        rp->capcm = cap;
    }
    struct rcm *x = &rp->cm[rp->ncm];
    memset(x, 0, sizeof *x);
    x->first = wr->fno;
    x->last = last;
    x->f = calloc(c->nfiles ? c->nfiles : 1, sizeof *x->f);
    if (!x->f)
        return TSDB_ERR_NOMEM;
    size_t pos = 0;
    mk_file f;
    while (x->nf < c->nfiles && mk_file_next(c, &pos, &f)) {
        struct rcf *e = &x->f[x->nf];
        if (f.name_len == 0 || f.name_len >= sizeof e->name) {
            free(x->f);
            return TSDB_ERR_CORRUPT;
        }
        memcpy(e->name, f.name, f.name_len);
        e->name[f.name_len] = 0;
        e->key = stage_key(e->name);
        e->size = f.size;
        e->xxh = f.xxh64;
        x->nf++;
    }
    for (uint32_t i = 0; i < x->nf; i++)
        if (!part_file_name(x->f[i].name)) {
            free(x->f);
            return TSDB_ERR_CORRUPT;
        }
    if (x->nf != c->nfiles) {
        free(x->f);
        return TSDB_ERR_CORRUPT;
    }
    rp->ncm++;
    return 0;
}

/* forgets the i-th COMMIT marker and removes the files it staged (applied, or cut) */
static void cm_remove(eng *db, struct replica *rp, size_t i)
{
    struct rcm *x = &rp->cm[i];
    for (uint32_t k = 0; k < x->nf; k++) {
        char sp[40];
        stage_path(sp, sizeof sp, x->f[k].key);
        if (eng_test_mutant != MUT_REPLICA_KEEP_STAGED)
            unlinkat(db->dirfd, sp, 0);
        stg_del(rp, x->f[k].key);
    }
    free(x->f);
    memmove(x, x + 1, (rp->ncm - i - 1) * sizeof *x);
    rp->ncm--;
}

/* Whether a COMMIT marker still to be applied names `key` with exactly this size and checksum: the staged file is
 * what an acknowledged position relies on (rp->mu held). */
static bool stage_wanted(struct replica *rp, uint64_t key, uint64_t size, uint64_t xxh)
{
    for (size_t i = 0; i < rp->ncm; i++)
        for (uint32_t k = 0; k < rp->cm[i].nf; k++) {
            const struct rcf *f = &rp->cm[i].f[k];
            if (f->key == key && f->size == size && f->xxh == xxh)
                return true;
        }
    return false;
}

/* A file is never written in place: the driver sends one again after a lost acknowledgement, and the process can die in
 * the middle of that (a torn file under a marker whose positions were acknowledged). The new bytes go to
 * stage_<key>.tmp, are synced and renamed over the name; a stage of what is staged already does nothing, and a file
 * that a pending marker relies on is not replaced by other bytes under the same name (names repeat across leader
 * incarnations: the new bytes can only be for a later marker, which the leader sends again once this one is applied).
 * Never O_TRUNC the staged name either: between the link to tmp_0_ and the removal after the apply it is a name of a
 * published part (a mapped file). */
/* rp->mu for a thread other than the applier: it says it waits, so the applier lets go between records */
static void rp_lock(struct replica *rp)
{
    atomic_fetch_add(&rp->mu_want, 1);
    pthread_mutex_lock(&rp->mu);
    atomic_fetch_sub(&rp->mu_want, 1);
}

int replica_stage(eng *db, const char *name, const void *buf, size_t len)
{
    struct replica *rp = db->rp;
    if (!rp || !name || !part_file_name(name) || strlen(name) >= 64 || len == 0)
        return TSDB_ERR_INVALID;
    const bool trunc = eng_test_mutant == MUT_REPLICA_STAGE_TRUNC;
    uint64_t key = stage_key(name), xx = xxh64(buf, len, 0);
    char sp[40], tp[48];
    stage_path(sp, sizeof sp, key);
    snprintf(tp, sizeof tp, "stage_%016" PRIx64 ".tmp", key);
    pthread_mutex_lock(&rp->stage_mu);
    int rc = 0;
    if (!trunc) {
        rp_lock(rp);
        const struct rstg *old = stg_find(rp, key);
        if (old && old->size == len && old->xxh == xx)
            rc = 1; /* staged already */
        else if (old && stage_wanted(rp, key, old->size, old->xxh))
            rc = TSDB_ERR_INVALID;
        pthread_mutex_unlock(&rp->mu);
        if (rc) {
            pthread_mutex_unlock(&rp->stage_mu);
            return rc == 1 ? 0 : rc;
        }
    }
    int fd = openat(db->dirfd, trunc ? sp : tp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0 || write_full(fd, buf, len) != 0 ||
        (db->opt.fsync && eng_test_mutant != MUT_REPLICA_NO_STAGE_SYNC && tsdb_fdatasync(fd) != 0))
        rc = TSDB_ERR_IO;
    if (fd >= 0)
        close(fd);
    if (rc == 0 && !trunc && renameat(db->dirfd, tp, db->dirfd, sp) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && db->opt.fsync && eng_test_mutant != MUT_REPLICA_NO_STAGE_SYNC && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO;
    if (rc) {
        unlinkat(db->dirfd, trunc ? sp : tp, 0); /* the name that held the old bytes is untouched (not so in place) */
        pthread_mutex_unlock(&rp->stage_mu);
        return rc;
    }
    rp_lock(rp);
    rc = stg_put(rp, key, len, xx);
    pthread_mutex_unlock(&rp->mu);
    pthread_mutex_unlock(&rp->stage_mu);
    return rc;
}

int replica_spill_open(eng *db, char *tmp, size_t cap)
{
    static unsigned seq;
    if (!db->rp || cap < 48)
        return TSDB_ERR_INVALID;
    snprintf(tmp, cap, "stage_s%d_%u.tmp", (int)getpid(), __atomic_fetch_add(&seq, 1, __ATOMIC_RELAXED));
    unlinkat(db->dirfd, tmp, 0); /* a new inode always (a failed write may have left pages dropped under an old one) */
    int fd = openat(db->dirfd, tmp, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    return fd < 0 ? TSDB_ERR_IO : fd;
}

void replica_spill_drop(eng *db, const char *tmp)
{
    if (db && tmp && tmp[0])
        unlinkat(db->dirfd, tmp, 0);
}

int replica_stage_spilled(eng *db, const char *name, int fd, const char *tmp, size_t len)
{
    struct replica *rp = db->rp;
    struct stat st;
    if (!rp || !name || !part_file_name(name) || strlen(name) >= 64 || len == 0 || fd < 0 ||
        strncmp(tmp, "stage_s", 7) != 0) {
        replica_spill_drop(db, tmp);
        return TSDB_ERR_INVALID;
    }
    if (fstat(fd, &st) != 0 || (uint64_t)st.st_size != len) {
        replica_spill_drop(db, tmp);
        return TSDB_ERR_IO;
    }
    void *m = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0); /* the page cache, never the heap */
    if (m == MAP_FAILED) {
        replica_spill_drop(db, tmp);
        return TSDB_ERR_IO;
    }
    uint64_t key = stage_key(name), xx = xxh64(m, len, 0);
    munmap(m, len);
    char sp[40];
    stage_path(sp, sizeof sp, key);
    pthread_mutex_lock(&rp->stage_mu);
    int rc = 0;
    rp_lock(rp);
    const struct rstg *old = stg_find(rp, key);
    if (old && old->size == len && old->xxh == xx)
        rc = 1; /* staged already */
    else if (old && stage_wanted(rp, key, old->size, old->xxh))
        rc = TSDB_ERR_INVALID;
    pthread_mutex_unlock(&rp->mu);
    const bool sync = db->opt.fsync && eng_test_mutant != MUT_REPLICA_NO_STAGE_SYNC;
    if (rc == 0 && sync && tsdb_fdatasync(fd) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && renameat(db->dirfd, tmp, db->dirfd, sp) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && sync && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO;
    if (rc) {
        unlinkat(db->dirfd, tmp, 0);
        pthread_mutex_unlock(&rp->stage_mu);
        return rc == 1 ? 0 : rc;
    }
    rp_lock(rp);
    rc = stg_put(rp, key, len, xx);
    pthread_mutex_unlock(&rp->mu);
    pthread_mutex_unlock(&rp->stage_mu);
    return rc;
}

int replica_missing(eng *db, uint64_t *pos, char *name, size_t cap, uint64_t *size)
{
    struct replica *rp = db->rp;
    if (!rp)
        return 0;
    int found = 0;
    rp_lock(rp);
    for (size_t i = 0; i < rp->ncm && !found; i++)
        for (uint32_t k = 0; k < rp->cm[i].nf; k++) {
            const struct rcf *f = &rp->cm[i].f[k];
            const struct rstg *s = stg_find(rp, f->key);
            if (s && s->size == f->size && s->xxh == f->xxh)
                continue;
            snprintf(name, cap, "%s", f->name);
            *size = f->size;
            *pos = rp->cm[i].first;
            found = 1;
            break;
        }
    pthread_mutex_unlock(&rp->mu);
    return found;
}

/* ---- RSTATE ---- */

static int rs_save(eng *db, struct rstate *rs)
{
    size_t n = 8 + 4 * 8 + 8 + rs->ndrops * 16 + 8 + 8, o = 0;
    uint8_t *b = malloc(n);
    if (!b)
        return TSDB_ERR_NOMEM;
    put64(b + o, RS_MAGIC), o += 8;
    put64(b + o, rs->done), o += 8;
    put64(b + o, rs->doing_pos), o += 8;
    put64(b + o, rs->doing_b0), o += 8;
    put64(b + o, rs->doing_n), o += 8;
    put64(b + o, rs->ndrops), o += 8;
    for (size_t i = 0; i < rs->ndrops; i++) {
        put64(b + o, rs->drops[i].pos), o += 8;
        put64(b + o, (uint64_t)rs->drops[i].until), o += 8;
    }
    put64(b + o, rs->kept), o += 8;
    put64(b + o, xxh64(b, o, 0));
    int fd = openat(db->dirfd, RS_FILE ".tmp", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, b, n) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0)
        close(fd);
    free(b);
    if (rc == 0 && renameat(db->dirfd, RS_FILE ".tmp", db->dirfd, RS_FILE) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO;
    if (rc)
        eng_fail(db, "saving RSTATE", errno);
    return rc;
}

static int rs_load(eng *db, struct rstate *rs)
{
    uint8_t *b;
    size_t len;
    if (faccessat(db->dirfd, RS_FILE, F_OK, 0) != 0 && errno == ENOENT)
        return 0; /* never applied a COMMIT or a DROP */
    int rc = read_file(db->dirfd, RS_FILE, &b, &len);
    if (rc)
        return rc;
    bool ok = len >= 64 && get64(b) == RS_MAGIC && get64(b + len - 8) == xxh64(b, len - 8, 0);
    uint64_t nd = ok ? get64(b + 40) : 0;
    ok = ok && nd <= (len - 64) / 16 && len == 64 + nd * 16;
    if (!ok) {
        free(b);
        fprintf(stderr, "tsdb: %s: RSTATE is damaged: the replica's state is lost, rebuild it\n", db->dir);
        return TSDB_ERR_CORRUPT;
    }
    rs->done = get64(b + 8);
    rs->doing_pos = get64(b + 16);
    rs->doing_b0 = get64(b + 24);
    rs->doing_n = get64(b + 32);
    if (nd) {
        rs->drops = malloc(nd * sizeof *rs->drops);
        if (!rs->drops) {
            free(b);
            return TSDB_ERR_NOMEM;
        }
        for (uint64_t i = 0; i < nd; i++) {
            rs->drops[i].pos = get64(b + 48 + i * 16);
            rs->drops[i].until = (int64_t)get64(b + 56 + i * 16);
        }
        rs->ndrops = nd;
    }
    rs->kept = get64(b + 48 + nd * 16);
    free(b);
    return 0;
}

int rs_kept_seq(eng *db, uint64_t *seq)
{
    struct rstate rs = {0};
    int rc = rs_load(db, &rs);
    free(rs.drops);
    *seq = rc == 0 && eng_test_mutant != MUT_KEPT_REPLAYED ? rs.kept : 0;
    return rc;
}

/* A drop whose position lies below every retained record cannot matter to a replay any more */
static void rs_prune_drops(struct replica *rp)
{
    uint64_t lo = UINT64_MAX;
    pthread_mutex_lock(&rp->segs_mu);
    for (size_t i = 0; i < rp->nsegs; i++)
        if (rp->segs[i].first) {
            lo = rp->segs[i].first;
            break;
        }
    pthread_mutex_unlock(&rp->segs_mu);
    size_t k = 0;
    for (size_t i = 0; i < rp->rs.ndrops; i++)
        if (rp->rs.drops[i].pos > lo || lo == UINT64_MAX)
            rp->rs.drops[k++] = rp->rs.drops[i];
    rp->rs.ndrops = k;
}

/* Whether the COMMIT in `doing` reached the parts: a listed part covers its first block (a merge keeps the range of
 * its sources; blocks are never reused). intents_recover ran at the open, so the parts are all or none. */
static bool rs_committed(eng *db, struct replica *rp)
{
    if (eng_test_mutant == MUT_REPLICA_RS_COMMITTED_TRUE || eng_test_mutant == MUT_REPLICA_RS_COMMITTED_FALSE)
        return eng_test_mutant == MUT_REPLICA_RS_COMMITTED_TRUE; /* planted: the answer without looking */
    bool yes = false;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && !yes; i++)
        yes = db->parts[i]->min_block <= rp->rs.doing_b0 && rp->rs.doing_b0 <= db->parts[i]->max_block;
    pthread_mutex_unlock(&db->lock);
    return yes;
}

int (*replica_test_apply_hook)(eng *db, int at); /* test hook: the applier's windows (RH_*, replica.h) */
#define RP_HOOK(db, at) (replica_test_apply_hook ? replica_test_apply_hook(db, at) : 0)

/* Drops what a prepare made: the parts and their tmp_0_ links (the staged files stay for the marker, or went with a cut) */
static void prep_discard(eng *db, part **ps, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (ps[i]) {
            char tp[sizeof ps[i]->pending];
            snprintf(tp, sizeof tp, "%s", ps[i]->pending);
            part_unref(ps[i]);
            unlinkat(db->dirfd, tp, 0);
        }
}

/* A COMMIT's prepare, without rp->mu: every sealed memtable to parts (the leader flushed through F before it wrote),
 * then, under stage_mu, the staged files read, checked against the marker and linked to their pending names, and
 * opened. `fs` is the marker's file list as it was under rp->mu (an append may move rp->cm meanwhile). */
static int commit_prepare(eng *db, struct replica *rp, const wal_rec *wr, const mk_commit *c, const struct rcf *fs,
                          size_t n, part **ps)
{
    int rc = 0;
    if (eng_test_mutant != MUT_REPLICA_NO_COMMIT_FLUSH)
        rc = replica_flush(db);
    RP_HOOK(db, RH_COMMIT_FLUSHED);
    if (rc)
        return rc;
    pthread_mutex_lock(&rp->stage_mu); /* no replica_stage renames a staged file while it is read and linked */
    for (size_t i = 0; i < n && rc == 0; i++) {
        const struct rcf *f = &fs[i];
        char sp[40], tp[64];
        stage_path(sp, sizeof sp, f->key);
        snprintf(tp, sizeof tp, "tmp_0_%" PRIu64 ".part", f->key);
        uint8_t *fb;
        size_t flen;
        if ((rc = read_file(db->dirfd, sp, &fb, &flen)) != 0) {
            fprintf(stderr, "tsdb: %s: staged file %s of the COMMIT at position %" PRIu64 " is not readable\n", db->dir, sp,
                    wr->fno);
            break;
        }
        bool good = flen == f->size && xxh64(fb, flen, 0) == f->xxh;
        free(fb);
        if (!good) {
            fprintf(stderr, "tsdb: %s: staged file %s does not match the COMMIT at position %" PRIu64 "\n", db->dir, sp,
                    wr->fno);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        unlinkat(db->dirfd, tp, 0);
        if (linkat(db->dirfd, sp, db->dirfd, tp, 0) != 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        part *p;
        if ((rc = RP_HOOK(db, RH_COMMIT_OPEN) ? TSDB_ERR_CORRUPT /* a test's refused open */
                                              : part_open_as(db->dirfd, tp, f->name, db->opt.pin_index, db->parts_v8, &p)) != 0) {
            unlinkat(db->dirfd, tp, 0);
            break;
        }
        snprintf(p->pending, sizeof p->pending, "%s", tp);
        ps[i] = p;
        mk_file mf;
        size_t pos = 0;
        uint64_t hc = 0;
        for (size_t k = 0; k <= i && mk_file_next(c, &pos, &mf); k++)
            hc = mf.header_check;
        if (p->index_hash != hc || p->grows != db->granule_rows) {
            fprintf(stderr, "tsdb: %s: staged file %s does not have the header the COMMIT at position %" PRIu64 " names\n",
                    db->dir, sp, wr->fno);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        p->verify = db->opt.verify_checksums;
    }
    pthread_mutex_unlock(&rp->stage_mu);
    return rc;
}

/* rp->mu held; let go of across the prepare (commit_prepare), so the log goes on taking appends and syncs. Returns
 * RP_AGAIN when a cut came meanwhile: what the prepare made is dropped (the cut may have removed this very marker and
 * its files), and the loop reads the record at the cursor again. The publish runs under rp->mu: no cut between the
 * check and the parts. */
static int apply_commit(eng *db, struct replica *rp, uint64_t seq, const wal_rec *wr, const mk *m)
{
    const mk_commit *c = &m->u.commit;
    uint64_t last = wr->fno + wr->span - 1;
    size_t ci = rp->ncm;
    for (size_t i = 0; i < rp->ncm; i++)
        if (rp->cm[i].last == last)
            ci = i;
    if (last <= rp->rs.done && eng_test_mutant != MUT_REPLICA_NO_DONE_SKIP)
        return 0; /* a replay: its parts are listed */
    if (rp->rs.doing_pos == last && eng_test_mutant != MUT_REPLICA_NO_DONE_SKIP && rs_committed(db, rp)) {
        rp->rs.done = last;
        rp->rs.doing_pos = rp->rs.doing_b0 = rp->rs.doing_n = 0;
        int rc = rs_save(db, &rp->rs);
        if (rc == 0 && ci < rp->ncm)
            cm_remove(db, rp, ci);
        return rc;
    }
    if (c->flush_through >= seq || c->nfiles == 0 || ci == rp->ncm || rp->cm[ci].nf != c->nfiles) {
        fprintf(stderr, "tsdb: %s: the COMMIT at position %" PRIu64 " (flush through %" PRIu64 ", segment %" PRIx64 ") is "
                        "not one this replica can apply\n", db->dir, wr->fno, c->flush_through, seq);
        return TSDB_ERR_CORRUPT;
    }
    size_t n = c->nfiles;
    part **ps = calloc(n, sizeof *ps);
    struct rcf *fs = malloc(n * sizeof *fs);
    if (!ps || !fs) {
        free(ps);
        free(fs);
        return TSDB_ERR_NOMEM;
    }
    memcpy(fs, rp->cm[ci].f, n * sizeof *fs);
    uint64_t gen = rp->cut_gen;
    const bool hold = eng_test_mutant == MUT_REPLICA_APPLY_HOLDS_MU; /* planted: rp->mu held across the prepare */
    if (!hold)
        pthread_mutex_unlock(&rp->mu);
    int rc = commit_prepare(db, rp, wr, c, fs, n, ps);
    RP_HOOK(db, RH_COMMIT_PREPARED);
    if (!hold)
        pthread_mutex_lock(&rp->mu);
    free(fs);
    if (rp->cut_gen != gen && eng_test_mutant != MUT_REPLICA_PREP_NO_CUT_GEN) {
        prep_discard(db, ps, n);
        free(ps);
        return RP_AGAIN;
    }
    ci = rp->ncm; /* a cut (only with the mutant) may have removed it: found again */
    for (size_t i = 0; i < rp->ncm; i++)
        if (rp->cm[i].last == last)
            ci = i;
    if (rc == 0) {
        pthread_mutex_lock(&db->publish_lock);
        rp->rs.doing_pos = last;
        rp->rs.doing_b0 = atomic_load(&db->next_block);
        rp->rs.doing_n = n;
        rc = rs_save(db, &rp->rs);
        if (rc == 0)
            rc = parts_commit(db, ps, n);
        if (rc == 0) {
            pthread_mutex_lock(&db->lock);
            for (size_t i = 0; i < n; i++)
                db_parts_insert_locked(db, ps[i]);
            pthread_cond_signal(&db->merge_cv);
            pthread_mutex_unlock(&db->lock);
        }
        pthread_mutex_unlock(&db->publish_lock);
    }
    if (rc) {
        for (size_t i = 0; i < n; i++)
            if (ps[i])
                part_unref(ps[i]); /* the files stay: the engine fails, the next open sorts them out */
        free(ps);
        return rc;
    }
    free(ps);
    rc = manifest_publish(db);
    rp->rs.done = last;
    rp->rs.doing_pos = rp->rs.doing_b0 = rp->rs.doing_n = 0;
    if (rc == 0)
        rc = rs_save(db, &rp->rs);
    if (rc == 0 && ci < rp->ncm)
        cm_remove(db, rp, ci);
    return rc;
}

static int apply_drop(eng *db, struct replica *rp, uint64_t seq, const wal_rec *wr, const mk *m)
{
    (void)seq;
    uint64_t last = wr->fno + wr->span - 1;
    int64_t ts = m->u.drop.dropped_before;
    if (m->u.drop.partition != MK_NO_PARTITION) {
        fprintf(stderr, "tsdb: %s: a DROP of one partition (position %" PRIu64 ") is not applied by this replica\n", db->dir,
                wr->fno);
        return TSDB_ERR_INVALID;
    }
    if (last <= rp->rs.done)
        return 0;
    pthread_mutex_lock(&db->buf_lock);
    size_t rows = db->active->rows;
    pthread_mutex_unlock(&db->buf_lock);
    if (rows != 0) { /* the leader's marker begins its segment: rows before it would be dropped with it or not by chance */
        fprintf(stderr, "tsdb: %s: the DROP at position %" PRIu64 " found rows of its own segment before it\n", db->dir,
                wr->fno);
        return TSDB_ERR_CORRUPT;
    }
    int rc = 0;
    if (eng_test_mutant != MUT_REPLICA_NO_DROP_FLUSH) {
        /* the flush without rp->mu (a sealed memtable can take seconds); nothing seals meanwhile (one applier) */
        uint64_t gen = rp->cut_gen;
        pthread_mutex_unlock(&rp->mu);
        rc = replica_flush(db);
        RP_HOOK(db, RH_DROP);
        pthread_mutex_lock(&rp->mu);
        if (rp->cut_gen != gen)
            return RP_AGAIN; /* the flush is harmless: the record is read again */
    }
    if (rc == 0)
        rc = drop_retire(db, ts);
    if (rc)
        return rc;
    rp->rs.done = last;
    struct rdrop *d = realloc(rp->rs.drops, (rp->rs.ndrops + 1) * sizeof *d);
    if (!d)
        return TSDB_ERR_NOMEM;
    rp->rs.drops = d;
    rp->rs.drops[rp->rs.ndrops++] = (struct rdrop){last, ts};
    rs_prune_drops(rp);
    if (eng_test_mutant == MUT_REPLICA_DROP_NO_SAVE)
        return 0; /* planted: the DROP is carried out but RSTATE does not say so */
    return rs_save(db, &rp->rs);
}

/* The open: the staged files that are on disk, the COMMIT markers above RSTATE's `done` (their files must be staged
 * before their positions are durable), and the staged files no marker names (shipped for a marker that never arrived:
 * the leader sends them again) removed. */
static int open_scan(eng *db, struct replica *rp)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    struct dirent *e;
    int rc = 0;
    for (errno = 0; rc == 0 && (e = readdir(d)); errno = 0) {
        uint64_t key;
        int consumed = 0;
        size_t nl = strlen(e->d_name);
        if (strncmp(e->d_name, "stage_", 6) == 0 && nl > 4 && strcmp(e->d_name + nl - 4, ".tmp") == 0) {
            unlinkat(db->dirfd, e->d_name, 0); /* a stage that did not finish: the leader sends the file again */
            continue;
        }
        if (strncmp(e->d_name, "stage_", 6) != 0 || sscanf(e->d_name, "stage_%" SCNx64 ".part%n", &key, &consumed) != 1 ||
            e->d_name[consumed])
            continue;
        uint8_t *fb;
        size_t flen;
        if ((rc = read_file(db->dirfd, e->d_name, &fb, &flen)) == 0) {
            rc = stg_put(rp, key, flen, xxh64(fb, flen, 0));
            free(fb);
        }
    }
    if (errno != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    closedir(d);
    for (size_t k = 0; k < rp->nsegs && rc == 0; k++) {
        char name[64];
        uint8_t *fb;
        size_t flen;
        wal_name(name, sizeof name, rp->segs[k].seq);
        if ((rc = read_file(db->dirfd, name, &fb, &flen)) != 0)
            break;
        for (size_t pos = 0; pos < flen && pos < rp->segs[k].len && rc == 0;) {
            wal_rec wr;
            size_t sz = wal_record(fb + pos, flen - pos, rp->segs[k].seq, pos, &wr);
            if (sz == 0)
                break;
            mk m;
            if (wr.kind == WAL_KIND_MARKER && wr.mtype == MK_COMMIT && (rc = mk_decode(wr.payload, wr.n, &m)) == 0 &&
                m.type == MK_COMMIT)
                rc = cm_add(rp, &wr, &m);
            pos += sz;
        }
        free(fb);
    }
    for (size_t i = rp->nstg; rc == 0 && i-- > 0;) {
        bool named = false;
        for (size_t c = 0; c < rp->ncm && !named; c++)
            for (uint32_t f = 0; f < rp->cm[c].nf; f++)
                named |= rp->cm[c].f[f].key == rp->stg[i].key;
        if (!named) {
            char sp[40];
            stage_path(sp, sizeof sp, rp->stg[i].key);
            unlinkat(db->dirfd, sp, 0);
            stg_del(rp, rp->stg[i].key);
        }
    }
    uint64_t cap = hold_cap(rp);
    if (rc == 0 && rp->durable_next > cap)
        rp->durable_next = cap;
    return rc;
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
    /* rows older than a DROP that this replica applied already, in partitions that DROP retired, must not come back
     * from a segment a replay reads again (the part that would have said so is gone with the partition) */
    int64_t dmax = INT64_MIN;
    bool dropped = false;
    for (size_t i = 0; i < rp->rs.ndrops && eng_test_mutant != MUT_REPLICA_NO_DROP_SKIP; i++)
        if (wr->fno + wr->span - 1 < rp->rs.drops[i].pos) {
            dmax = dropped ? MAX(dmax, rp->rs.drops[i].until) : rp->rs.drops[i].until;
            dropped = true;
        }
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
        if (dropped) {
            int64_t lo, end;
            partition_range(db, partition_of(db, id, t), &lo, &end);
            if (end <= dmax)
                continue;
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
    return rc;
}

/* One marker, in position order, once committed. Every type is applied in a way a replay can repeat: a restart applies
 * the markers of the segments it still holds again, from the floor. SERIES and STATE entries the dictionary holds
 * already are checked and skipped, RETENTION and CLASS records by the offset they have in their file, NOW only
 * moves the clock up. COMMIT and DROP are applied by apply_commit and apply_drop (staged files, RSTATE), EPOCH sets the
 * epoch and its position. The reserved types and unknown ones are skipped, as every replay does. */
static int apply_marker(eng *db, struct replica *rp, uint64_t seq, const wal_rec *wr)
{
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
    case MK_CLASS: {
        /* the purge (a merge, seconds) without rp->mu (R3b M2); then the record under it, unless a cut came */
        uint64_t gen = rp->cut_gen;
        pthread_mutex_unlock(&rp->mu);
        rc = cls_purge(db, m.u.cls.series, (int)m.u.cls.cls, m.u.cls.at, m.u.cls.record_off);
        RP_HOOK(db, RH_CLASS);
        pthread_mutex_lock(&rp->mu);
        if (rp->cut_gen != gen)
            return RP_AGAIN;
        return rc ? rc : cls_apply(db, m.u.cls.series, (int)m.u.cls.cls, m.u.cls.at, m.u.cls.record_off);
    }
    case MK_COMMIT:
        return apply_commit(db, rp, seq, wr, &m);
    case MK_DROP:
        return apply_drop(db, rp, seq, wr, &m);
    case MK_EPOCH:
        /* The marker sits in the segment its epoch names, (e << 40) (6.2 step 3): segments only go up, so epochs do too,
         * and a record of a lower epoch's leader cannot pass as the new one's. */
        if ((seq >> 40) != m.u.epoch.epoch && eng_test_mutant != MUT_REPLICA_EPOCH_SEG) {
            fprintf(stderr, "tsdb: %s: the EPOCH marker at position %" PRIu64 " names epoch %" PRIu64 " (the log was at %" PRIu64
                            ", the marker is in segment %" PRIx64 "): the replica's log is damaged\n", db->dir, wr->fno,
                    m.u.epoch.epoch, rp->epoch, seq);
            return TSDB_ERR_CORRUPT;
        }
        rp->epoch = m.u.epoch.epoch;
        rp->epoch_pos = wr->fno;
        return 0;
    default:
        return 0; /* the reserved types and unknown ones */
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
        if (wr.kind == WAL_KIND_ROWS)
            RP_HOOK(db, RH_ROWS_HELD);
        rc = wr.kind == WAL_KIND_ROWS ? apply_rows(db, rp, cur.seq, &wr, pmax) : apply_marker(db, rp, cur.seq, &wr);
        if (rc == RP_AGAIN) { /* a cut came while the marker's slow part ran without rp->mu: read again from the cursor */
            rc = 0;
            continue;
        }
        if (rc)
            break;
        rp->apply_off += sz;
        rp->apply_next = wr.fno + wr.span;
        /* Between records the applier lets another thread that wants rp->mu (an append, a sync, a stage) have it, so a
         * commit point that moves over many row records does not stall the ACKs behind them (r3g REVIEW F2). A row
         * record is applied whole under rp->mu: a cut must not remove a record whose rows are half in the memtable.
         * The yield needs no cut_gen check: a cut cannot go below apply_next, and the loop reads again from the
         * cursor, which the cut keeps right. The waiter says it waits (mu_want), and the applier steps aside until it
         * has the lock: glibc's mutex is not fair, and an unlock followed by a lock would keep it. */
        if ((atomic_load(&rp->mu_want) != 0 || replica_test_apply_hook) && eng_test_mutant != MUT_REPLICA_ROWS_HOLD_MU) {
            pthread_mutex_unlock(&rp->mu);
            for (int i = 0; i < 1000 && atomic_load(&rp->mu_want) != 0; i++)
                sched_yield();
            RP_HOOK(db, RH_ROWS_YIELD);
            pthread_mutex_lock(&rp->mu);
        } else if (replica_test_apply_hook)
            RP_HOOK(db, RH_ROWS_YIELD); /* the planted mutant: rp->mu held */
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
    pthread_mutex_lock(&rp->apply_mu);
    pthread_mutex_lock(&rp->mu);
    int rc = replica_apply_locked(db, rp, commit);
    pthread_mutex_unlock(&rp->mu);
    pthread_mutex_unlock(&rp->apply_mu);
    return rc;
}

/* The three ends are read without a lock: the driver acknowledges by them while an apply holds rp->mu. Each is
 * written under rp->mu as one store, so a reader sees a value the end had (a cut may lower the log and durable ends). */
uint64_t replica_log_end(eng *db) { return atomic_load(&db->rp->log_next) - 1; }

uint64_t replica_durable_end(eng *db) { return atomic_load(&db->rp->durable_next) - 1; }

uint64_t replica_applied_end(eng *db) { return atomic_load(&db->rp->apply_next) - 1; }

/* ----------------------------------------------------------------- append */

int replica_append(eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len)
{
    struct replica *rp = db->rp;
    if (!rp || seq == 0)
        return TSDB_ERR_INVALID;
    if (len == 0)
        return 0;
    rp_lock(rp);
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
                durable_raise(rp, first);
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
        pthread_mutex_lock(&db->buf_lock); /* manifest_publish reads it under buf_lock (the flusher's thread) */
        db->fno_next = next;
        pthread_mutex_unlock(&db->buf_lock);
        rp->log_next = next;
        if (eng_test_mutant == MUT_REPLICA_ACK_UNSYNCED) /* planted: acknowledge what is not synced */
            rp->durable_next = next;
        pthread_mutex_lock(&rp->segs_mu);
        rseg *s = &rp->segs[rp->nsegs - 1];
        if (!s->first)
            s->first = first;
        s->len = db->wal_off;
        pthread_mutex_unlock(&rp->segs_mu);
        atomic_fetch_add_explicit(&db->st_wal_bytes, len, memory_order_relaxed);
        /* COMMIT markers among them: their files must be staged before their positions count as durable */
        for (size_t pos = 0; pos < len && rc == 0;) {
            wal_rec wr;
            size_t sz = wal_record(p + pos, len - pos, seq, off + pos, &wr);
            mk m;
            if (wr.kind == WAL_KIND_MARKER && wr.mtype == MK_COMMIT && (rc = mk_decode(wr.payload, wr.n, &m)) == 0 &&
                m.type == MK_COMMIT)
                rc = cm_add(rp, &wr, &m);
            pos += sz;
        }
        if (rc)
            eng_fail(db, "a COMMIT marker that cannot be tracked", EIO);
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
    rp_lock(rp);
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
    rp_lock(rp);
    /* gaps 9 and 12: a sync that began before a cut does not cover what was written again after it */
    uint64_t end = rp->cut_gen == gen || eng_test_mutant == MUT_REPLICA_NO_CUT_GEN ? target : MIN(target, rp->last_cut_next);
    end = MIN(end, db->fno_next);
    durable_raise(rp, end);
    pthread_mutex_unlock(&rp->mu);
    return 0;
}

/* -------------------------------------------------------------------- cut */

int replica_cut(eng *db, uint64_t last)
{
    struct replica *rp = db->rp;
    if (!rp)
        return TSDB_ERR_INVALID;
    rp_lock(rp);
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
        pthread_mutex_lock(&db->buf_lock);
        db->fno_next = last + 1;
        pthread_mutex_unlock(&db->buf_lock);
        rp->log_next = last + 1;
        for (size_t i = rp->ncm; i-- > 0;) /* the COMMIT markers above the cut never happened: their files go */
            if (rp->cm[i].first > last)
                cm_remove(db, rp, i);
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

/* One read of segment r->seq (r->flen bytes of it are the log's) from position r->from: whole records from the one that
 * starts there, at most r->cap bytes unless the first is larger, scanned from byte r->pos0 (a record start at or
 * before it) in windows, never the whole segment: a catch-up in chunks costs the bytes it sends, not a square of the
 * segment. r->found says whether a record starts at r->from; r->tail_ok: a record that does not parse at the end is
 * where the writer's bytes end (a leader's newest segments, written while this reads), not damage. */
struct segrd {
    uint64_t seq, from, last;
    size_t flen, pos0, cap, start, next_off, outlen;
    bool tail_ok, found;
    uint8_t *out;
};

static int seg_scan(eng *db, struct segrd *r)
{
    char name[64];
    wal_name(name, sizeof name, r->seq);
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    size_t flen = r->flen, pos0 = r->pos0, cap = r->cap;
    uint64_t from = r->from;
    size_t win = MAX(cap, replica_read_win_min);
    uint8_t *fb = NULL;
    size_t blen = 0, outcap = 0;
    bool done = false;
    int rc = 0;
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
            size_t sz = wal_record(fb + pos, blen - pos, r->seq, pos0 + pos, &wr);
            if (sz == 0) {
                if (window_end)
                    break; /* a record that does not fit the window: read from here with a larger one */
                if (r->tail_ok)
                    done = true; /* what the writer has written so far ends here */
                else
                    rc = TSDB_ERR_CORRUPT;
                break;
            }
            if (!r->found) {
                if (wr.fno == from)
                    r->found = true, r->start = pos0 + pos;
                else if (wr.fno > from) {
                    rc = TSDB_ERR_INVALID; /* from is inside a record */
                    break;
                }
            }
            if (r->found) {
                if (r->outlen > 0 && r->outlen + sz > cap) {
                    done = true;
                    break;
                }
                if (r->outlen + sz > outcap) {
                    size_t nc = MAX(r->outlen + sz, outcap * 2);
                    uint8_t *no = realloc(r->out, nc);
                    if (!no) {
                        rc = TSDB_ERR_NOMEM;
                        break;
                    }
                    r->out = no, outcap = nc;
                }
                memcpy(r->out + r->outlen, fb + pos, sz);
                r->outlen += sz;
                r->last = wr.fno + wr.span - 1;
                r->next_off = pos0 + pos + sz;
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
    return rc;
}

/* The newest WAL segment below `below` that is on disk (0: none), by a listing: a leader's kept segments need not
 * follow one another (a promotion begins the new leader's log at epoch << 40, above the replica's own segments) */
static int seg_below(eng *db, uint64_t below, uint64_t *prev)
{
    *prev = 0;
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) {
        uint64_t sq;
        int consumed = 0;
        if (!wal_file_name(e->d_name) || sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &sq, &consumed) != 1 ||
            e->d_name[consumed])
            continue;
        if (sq < below && sq > *prev)
            *prev = sq;
    }
    int rc = errno ? TSDB_ERR_IO : 0; /* a readdir error is an error, never the end of the directory */
    closedir(d);
    return rc;
}

/* A writer's (a leader's) log by position: what it keeps of it (its retained segments, eng_leader_keep, and the ones
 * not flushed yet) and the current segment as far as the WAL writer role has written it, which is what the driver's
 * `bytes` hook has been handed already or is about to be. The segment that holds `from` is the newest whose first
 * record is at or below it. Numbers follow one another except where a promotion jumped to the epoch's (seg_below finds
 * the kept ones under the jump); no segment below that means the position is in parts (TSDB_ERR_NOTFOUND: a
 * snapshot). The two newest segments may end in a record still being written (the
 * current one, and the one a rotation is finishing): their bytes end at the first record that does not parse. */
static int leader_read(eng *db, uint64_t from, uint8_t **buf, size_t *len, size_t cap, uint64_t *seq, uint64_t *off,
                       uint64_t *last)
{
    pthread_mutex_lock(&db->buf_lock);
    uint64_t cur = db->wal_seq, next = db->fno_next;
    pthread_mutex_unlock(&db->buf_lock);
    if (from == 0 || from > next)
        return TSDB_ERR_INVALID;
    *last = from - 1, *seq = cur, *off = 0;
    for (uint64_t sq = cur; sq > 0; sq--) {
        char name[64];
        wal_name(name, sizeof name, sq);
        int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
        if (fd < 0 && errno == ENOENT) {
            uint64_t prev;
            int lrc = eng_test_mutant == MUT_LEADER_READ_NO_GAP ? (prev = 0, 0) : seg_below(db, sq, &prev);
            if (lrc)
                return lrc;
            if (prev == 0)
                return TSDB_ERR_NOTFOUND; /* gone below `from`'s segment: in parts */
            sq = prev + 1; /* the loop steps to it */
            continue;
        }
        if (fd < 0)
            return TSDB_ERR_IO;
        struct stat st;
        uint8_t h[WAL_HDR];
        uint64_t first = 0;
        bool newest = sq + 1 >= cur;
        int rc = 0;
        if (fstat(fd, &st) != 0)
            rc = TSDB_ERR_IO;
        else if ((size_t)st.st_size >= WAL_HDR && pread(fd, h, WAL_HDR, 0) == WAL_HDR)
            memcpy(&first, h + 16, 8);
        else if (!newest)
            rc = TSDB_ERR_CORRUPT; /* a sealed segment without a record */
        close(fd);
        if (rc)
            return rc;
        if (first == 0 || first > from)
            continue; /* empty (just begun), or it starts above `from` */
        pthread_mutex_lock(&db->lead_mu);
        bool behind = db->lrd_next <= from || eng_test_mutant == MUT_LEADER_READ_CURSOR_ANY; /* the cursor is ours */
        size_t pos0 = db->lrd_seq == sq && db->lrd_next && behind && db->lrd_off <= (uint64_t)st.st_size
                          ? (size_t)db->lrd_off
                          : 0;
        pthread_mutex_unlock(&db->lead_mu);
        struct segrd r = {.seq = sq, .flen = (size_t)st.st_size, .pos0 = pos0, .from = from, .cap = cap,
                          .tail_ok = newest || eng_test_mutant == MUT_LEADER_READ_TAILOK_SEALED};
        rc = seg_scan(db, &r);
        if (rc == 0 && !r.found && !newest && eng_test_mutant != MUT_LEADER_READ_NOTFOUND_SEALED)
            rc = TSDB_ERR_INVALID; /* a sealed segment that has the position inside a record, or not at all */
        if (rc) {
            free(r.out);
            return rc;
        }
        if (!r.found)
            return 0; /* not written yet */
        *buf = r.out, *len = r.outlen, *seq = sq, *off = r.start, *last = r.last;
        pthread_mutex_lock(&db->lead_mu);
        db->lrd_seq = sq, db->lrd_off = r.next_off, db->lrd_next = r.last + 1;
        pthread_mutex_unlock(&db->lead_mu);
        return 0;
    }
    return TSDB_ERR_NOTFOUND;
}

int replica_read(eng *db, uint64_t from, uint8_t **buf, size_t *len, size_t cap, uint64_t *seq, uint64_t *off,
                 uint64_t *last)
{
    struct replica *rp = db->rp;
    *buf = NULL;
    *len = 0;
    if (!rp)
        return db->wal_enabled && !db->opt.read_only ? leader_read(db, from, buf, len, cap, seq, off, last)
                                                     : TSDB_ERR_INVALID;
    rp_lock(rp);
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
    /* pread from where the last read of this segment ended (rd_*: the sender walks the log in order). The segment is
     * append-only between cuts, and a cut holds rp->mu like this does. */
    size_t pos0 = rp->rd_seq == s.seq && rp->rd_next && rp->rd_next <= from && rp->rd_off <= s.len ? (size_t)rp->rd_off : 0;
    struct segrd r = {.seq = s.seq, .flen = (size_t)s.len, .pos0 = pos0, .from = from, .cap = cap};
    rc = seg_scan(db, &r);
    if (rc == 0 && !r.found)
        rc = TSDB_ERR_INVALID;
    if (rc == 0) {
        *buf = r.out;
        *len = r.outlen;
        *seq = s.seq;
        *off = r.start;
        *last = r.last;
        rp->rd_seq = s.seq, rp->rd_off = r.next_off, rp->rd_next = r.last + 1;
    } else {
        free(r.out);
    }
    pthread_mutex_unlock(&rp->mu);
    return rc;
}

/* -------------------------------------------------------------------- promotion (design-cluster.md 6.2 steps 3-4) */

/* The new leader's first record: an EPOCH marker as the first record of the segment `epoch << 40`, at the end of the log
 * it has (the old epoch's tail included: it keeps every record, as Raft does). The only record a replica engine makes
 * itself, and its number is the next one of the log, never a new sequence. The driver ships the log to the other
 * members, and when the commit point passes the marker (and so the tail) calls replica_apply and then replica_promote. */
int replica_epoch_begin(eng *db, uint64_t epoch, const char *owner, uint64_t *pos)
{
    struct replica *rp = db->rp;
    size_t ol = owner ? strlen(owner) : 0;
    if (!rp || epoch == 0 || epoch >= ((uint64_t)1 << 22) || ol > MK_NAME_MAX)
        return TSDB_ERR_INVALID;
    uint64_t seq = epoch << 40;
    pthread_mutex_lock(&rp->mu);
    int rc = eng_check(db);
    uint64_t fno = db->fno_next;
    /* the segment must be above every one the log has and every one in parts: a lower epoch's leader is not ours */
    if (rc == 0 && ((db->wal_fd >= 0 && seq <= db->wal_seq) || seq <= rp->sealed_seq || seq <= rp->act_seq))
        rc = TSDB_ERR_INVALID;
    pthread_mutex_unlock(&rp->mu);
    if (rc)
        return rc;
    mk m = {.type = MK_EPOCH, .span = 1};
    m.u.epoch = (mk_epoch){.epoch = epoch, .owner = owner, .owner_len = (uint32_t)ol};
    uint8_t *pl;
    size_t pn;
    if ((rc = mk_encode(&m, &pl, &pn)) != 0)
        return rc;
    size_t sz = WAL_HDR + pn;
    uint8_t *rec = calloc(1, sz);
    if (!rec) {
        free(pl);
        return TSDB_ERR_NOMEM;
    }
    uint32_t w = (uint32_t)pn | (uint32_t)WAL_KIND_MARKER << 28;
    int64_t now = eng_wall_ms();
    memcpy(rec, &w, 4);
    memcpy(rec + 16, &fno, 8);
    memcpy(rec + 24, &now, 8);
    memcpy(rec + WAL_HDR, pl, pn);
    free(pl);
    wal_reseal(rec, seq, 0);
    rc = replica_append(db, seq, 0, rec, sz);
    free(rec);
    if (rc == 0 && pos)
        *pos = fno;
    return rc;
}

/* Turns the replica engine into a writer: `*dbp` is closed and its directory opened again as a leader's, which the
 * caller then equips (db->jr). Only when nothing uncommitted can be in the way: the EPOCH marker at `epoch_pos` and
 * everything before it is applied, the log is applied to its end and durable, no COMMIT is waiting for its files and
 * the active memtable is empty. Everything applied is put into parts (FNO holds the log's end first, so numbers go on
 * from it) and the replica's segments stay, named in RSTATE (`kept`) as holding nothing outside the parts: the writer's
 * open neither replays them (rows a drop took out of parts would come back) nor removes them, and the leader keeps them
 * for its replicas until the driver's keep bound passes them (eng_leader_keep). The numbers continue (fno_next = the
 * log end + 1), the writer's first segment is above the epoch's. A failure leaves the replica as it was, or (a crash
 * after RSTATE was saved) a directory a replica open or a writer's open recovers; TSDB_ERR_INVALID: a precondition does
 * not hold.
 * The writer gets the caller's latest_cache (a replica runs without the table) and the replica's RSTATE. */
void (*replica_test_promote_hook)(void); /* test hook: after RSTATE names the kept segments, before the engine is closed */

int replica_promote(eng **dbp, uint64_t epoch_pos, eng **out)
{
    eng *db = *dbp;
    struct replica *rp = db->rp;
    if (!rp || epoch_pos == 0)
        return TSDB_ERR_INVALID;
    /* apply_mu first: an apply that let go of rp->mu (a COMMIT's prepare, a DROP's flush, a CLASS purge, between
     * records) ends before the checks, so a promote racing it waits and sees everything applied instead of being
     * refused half way (r3g REVIEW F5) */
    const bool amu = eng_test_mutant != MUT_REPLICA_PROMOTE_NO_APPLY_MU;
    if (amu)
        pthread_mutex_lock(&rp->apply_mu);
    pthread_mutex_lock(&rp->mu);
    int rc = eng_check(db);
    if (rc == 0 && eng_test_mutant != MUT_REPLICA_PROMOTE_UNAPPLIED &&
        (rp->apply_next <= epoch_pos || rp->apply_next != db->fno_next ||
         rp->durable_next < db->fno_next || rp->ncm != 0))
        rc = TSDB_ERR_INVALID;
    if (rc == 0 && rp->epoch_pos != 0 && rp->epoch_pos > epoch_pos)
        rc = TSDB_ERR_INVALID; /* a later EPOCH marker is applied: the caller's is not the last */
    if (rc == 0 && db->active && db->active->rows != 0 && eng_test_mutant != MUT_REPLICA_PROMOTE_UNAPPLIED)
        rc = TSDB_ERR_INVALID;
    if (rc == 0 && (rc = replica_flush(db)) == 0 && nflushing_now(db) != 0)
        rc = TSDB_ERR_IO;
    uint64_t end = db->fno_next;
    if (rc == 0)
        rc = fno_require(db, end); /* the epoch's segment holds it too; the bound is not left to that */
    /* no COMMIT is waiting (ncm == 0): every file still staged was sent after its marker was applied, or for one that
     * never came, and the old leader sends nothing more */
    for (size_t i = rp->nstg; rc == 0 && i-- > 0 && eng_test_mutant != MUT_REPLICA_NO_STAGE_ORPHANS;) {
        char sp[40];
        stage_path(sp, sizeof sp, rp->stg[i].key);
        unlinkat(db->dirfd, sp, 0);
        stg_del(rp, rp->stg[i].key);
    }
    uint64_t *seqs = NULL;
    size_t ns = 0;
    if (rc == 0) {
        pthread_mutex_lock(&rp->segs_mu);
        ns = rp->nsegs;
        seqs = ns ? malloc(ns * sizeof *seqs) : NULL;
        if (ns && !seqs)
            rc = TSDB_ERR_NOMEM;
        for (size_t i = 0; seqs && i < ns; i++)
            seqs[i] = rp->segs[i].seq;
        pthread_mutex_unlock(&rp->segs_mu);
    }
    pthread_mutex_unlock(&rp->mu);
    if (amu)
        pthread_mutex_unlock(&rp->apply_mu);
    if (rc) {
        free(seqs);
        return rc;
    }
    tsdb_options opt = db->opt;
    if (eng_test_mutant != MUT_PROMOTE_NO_CARRY)
        opt.latest_cache = db->latest_cache_asked; /* the replica ran without the latest table; the leader has one */
    char *dir = strdup(db->dir);
    if (!dir) {
        free(seqs);
        return TSDB_ERR_NOMEM;
    }
    /* Every segment stays (the catch-up source of the new leader's replicas, eng_leader_keep): none holds a row outside
     * the parts, which RSTATE says before the writer's open sees them, so the open neither replays them (a drop's rows
     * would come back) nor removes them; the newest is the epoch's, and the writer's first segment is above it. */
    if (ns && eng_test_mutant != MUT_REPLICA_PROMOTE_KEEP_SEGS) { /* planted: the segments stay, unnamed in RSTATE */
        pthread_mutex_lock(&rp->mu);
        rp->rs.kept = MAX(rp->rs.kept, seqs[ns - 1]);
        rc = rs_save(db, &rp->rs);
        pthread_mutex_unlock(&rp->mu);
    }
    free(seqs);
    if (rc == 0 && replica_test_promote_hook)
        replica_test_promote_hook();
    if (rc == 0 && eng_test_mutant == MUT_PROMOTE_NO_CARRY)
        unlinkat(db->dirfd, RS_FILE, 0); /* planted: the old promote's removal of RSTATE */
    if (rc == 0) {
        /* RSTATE stays: the leader carries it on (leader_rs_open), and should it be deposed and reopened as a replica,
         * the COMMITs and DROPs it applied as a replica are still in its `done` and `drops` */
        eng_close(db);
        *dbp = NULL;
        rc = eng_open(dir, &opt, out);
    }
    free(dir);
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
/* The wait of a marker the leader appended (`what` names it): the driver's (jr_ops.wait, which a test transport pumps
 * in), then the engine's own copy of the commit point. A deposed leader's failed wait is indeterminate and leaves the
 * engine running (fail-stop would end tsdbd and every volume with it; the close and reopen as a replica that follows
 * cut what the new leader's history does not hold). Any other failure fails the engine: a replica may apply the
 * marker, so the leader must not go on as if nothing happened. */
static int leader_marker_wait(eng *db, uint64_t last, const char *what)
{
    const struct jr_ops *jr = eng_jr(db);
    int rc = jr->wait ? jr->wait(jr->ctx, db, last) : 0;
    if (rc == 0)
        rc = eng_leader_wait(db, last, false);
    if (rc == 0)
        return 0;
    if (atomic_load(&db->lead_deposed) && eng_test_mutant != MUT_LEADER_DEPOSE_FAILS)
        return TSDB_ERR_INDETERMINATE;
    eng_fail(db, what, EIO);
    return rc;
}

/* The position of the marker this thread's write appended and saw committed (leader_commit, leader_drop), which the
 * publish that follows on the same thread records in the leader's RSTATE (jr_precommit, jr_commit, leader_rs_drop) */
static __thread uint64_t lead_pos, lead_drop_pos;

/* ---- the leader's links of its COMMITs' files (design-cluster.md 5.2, M4) ----
 * Each file a leader's COMMIT marker names is hard linked as stage_<key>.part, the name a replica stages it under, so a
 * replica that missed it can fetch it (eng_leader_file_open) after the leader merged the part away, and a deposed
 * leader reopened as a replica finds the files of the markers it had not published (open_scan keeps the stage_ files
 * that a marker above RSTATE's `done` names and removes the rest). The links go with the retained segment that holds
 * their marker (leader_links_drop, from leader_unlink_due); the list is in memory only: links of an earlier leadership
 * are removed at attach (leader_links_sweep). */
static void leader_links_unlink(eng *db, const uint64_t *keys, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        char sp[40];
        stage_path(sp, sizeof sp, keys[i]);
        unlinkat(db->dirfd, sp, 0);
    }
}

/* Room for a COMMIT's n links in db->lf, taken before its marker is appended: the note itself (leader_links_note, under
 * buf_lock) cannot fail, and a marker in the log always has its links served (r3g REVIEW F1). */
static int leader_links_reserve(eng *db, size_t n)
{
    pthread_mutex_lock(&db->lead_mu);
    if (db->nlf + db->rlf + n > db->caplf) {
        size_t c = db->caplf ? db->caplf : 16;
        while (c < db->nlf + db->rlf + n)
            c *= 2;
        void *p = realloc(db->lf, c * sizeof *db->lf);
        if (!p) {
            pthread_mutex_unlock(&db->lead_mu);
            return TSDB_ERR_NOMEM;
        }
        db->lf = p;
        db->caplf = c;
    }
    db->rlf += n;
    pthread_mutex_unlock(&db->lead_mu);
    return 0;
}

static void leader_links_release(eng *db, size_t n)
{
    pthread_mutex_lock(&db->lead_mu);
    db->rlf -= n;
    pthread_mutex_unlock(&db->lead_mu);
}

struct lnote {
    const uint64_t *keys;
    size_t n;
};

/* Under buf_lock (wal_markers_mk_at), the marker's number just assigned: the links are served from before any reader
 * can see the marker (noted after the append, a replica that asked between its sync and the note got NOTFOUND, which
 * sends it to a snapshot). Uses the room leader_links_reserve took. */
static void leader_links_note(eng *db, uint64_t fno, void *arg)
{
    const struct lnote *ln = arg;
    pthread_mutex_lock(&db->lead_mu);
    for (size_t i = 0; i < ln->n; i++)
        db->lf[db->nlf++] = (struct lkfile){fno, ln->keys[i]};
    db->rlf -= ln->n;
    pthread_mutex_unlock(&db->lead_mu);
}

void leader_links_drop(eng *db, uint64_t fno_end)
{
    uint64_t keys[64];
    for (;;) {
        size_t n = 0, k = 0;
        pthread_mutex_lock(&db->lead_mu);
        for (size_t i = 0; i < db->nlf; i++) {
            if (db->lf[i].fno < fno_end && n < 64)
                keys[n++] = db->lf[i].key;
            else
                db->lf[k++] = db->lf[i];
        }
        bool more = n == 64 && k < db->nlf;
        db->nlf = k;
        pthread_mutex_unlock(&db->lead_mu);
        leader_links_unlink(db, keys, n); /* outside lead_mu, a leaf */
        if (!more)
            return;
    }
}

void leader_links_sweep(eng *db)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return; /* the links stay: a leak, never a missing file */
    rewinddir(d);
    struct dirent *e;
    while ((e = readdir(d))) {
        uint64_t key;
        int consumed = 0;
        if (sscanf(e->d_name, "stage_%" SCNx64 ".part%n", &key, &consumed) == 1 && !e->d_name[consumed])
            unlinkat(db->dirfd, e->d_name, 0);
    }
    closedir(d);
}

/* A file of the COMMIT marker at `pos`, by its name in the marker. Only a link this leadership noted for that very
 * marker is served: names repeat across engines in one process (the tmp counter starts at 0 at every open, a pid
 * repeats in a container), and a promoted leader keeps the replica-era segments, whose markers may name a file that a
 * COMMIT of this leadership links under the same key; serving that one would stall a replica's catch-up on a file
 * that fails its check, where NOTFOUND sends it to a snapshot (r3a2 R5). A link noted and then dropped with its
 * segment between the lookup and the open is ENOENT: NOTFOUND too. */
void (*leader_test_marker_hook)(eng *db, uint64_t fno);

int eng_leader_file_open(eng *db, uint64_t pos, const char *name)
{
    const uint64_t key = stage_key(name);
    bool noted = eng_test_mutant == MUT_LEADER_LINK_BY_NAME;
    pthread_mutex_lock(&db->lead_mu);
    for (size_t i = 0; i < db->nlf && !noted; i++)
        noted = db->lf[i].fno == pos && db->lf[i].key == key;
    pthread_mutex_unlock(&db->lead_mu);
    if (!noted)
        return TSDB_ERR_NOTFOUND;
    char sp[40];
    stage_path(sp, sizeof sp, key);
    int fd = openat(db->dirfd, sp, O_RDONLY | O_CLOEXEC);
    return fd >= 0 ? fd : errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
}

static int leader_commit(eng *db, const struct jr_commit_arg *ja)
{
    const struct jr_ops *jr = eng_jr(db);
    if (ja->n == 0)
        return 0;
    if (ja->n > MK_FILES_MAX)
        return TSDB_ERR_INVALID;
    size_t cap = 0;
    for (size_t i = 0; i < ja->n; i++)
        cap += 24 + 8 + 64;
    uint8_t *files = malloc(cap);
    uint64_t *keys = calloc(ja->n, sizeof *keys);
    if (!files || !keys) {
        free(files);
        free(keys);
        return TSDB_ERR_NOMEM;
    }
    size_t len = 0;
    int rc = 0;
    for (size_t i = 0; i < ja->n && rc == 0; i++) {
        part *p = ja->ps[i];
        if (!p->pending[0] || !p->map) {
            rc = TSDB_ERR_INVALID; /* not a pending part */
            break;
        }
        int pid;
        unsigned cnt;
        char mname[96];
        if (sscanf(p->pending, "tmp_%d_%u.part", &pid, &cnt) != 2) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        snprintf(mname, sizeof mname, "p%" PRId64 "_%d_%u_0.part", p->partition, pid, cnt);
        mk_file f = {.name = mname, .name_len = (uint32_t)strlen(mname), .size = p->map_len,
                     .xxh64 = xxh64(p->map, p->map_len, 0), .header_check = p->index_hash};
        len += mk_file_put(files + len, &f);
        rc = jr->ship(jr->ctx, db, p->pending, mname, p->map_len);
        keys[i] = stage_key(mname);
        if (rc == 0 && eng_test_mutant != MUT_LEADER_NO_HARDLINK) {
            char sp[40];
            stage_path(sp, sizeof sp, keys[i]);
            unlinkat(db->dirfd, sp, 0); /* left by a marker whose append failed, or an earlier leadership */
            if (linkat(db->dirfd, p->pending, db->dirfd, sp, 0) != 0)
                rc = TSDB_ERR_IO;
        }
    }
    if (jr->ship_wait) {
        int wr = jr->ship_wait(jr->ctx, db, rc != 0);
        if (rc == 0)
            rc = wr;
    }
    /* The links are named before the marker can be durable anywhere: a leader that loses power after its marker and
     * before parts_commit has its tmp_ files removed by the open (load_parts, db.c), and reopened as a replica
     * (design-cluster.md 6.3) it applies the marker from these, as any replica does from what it staged. */
    if (rc == 0 && eng_test_mutant != MUT_LEADER_NO_HARDLINK)
        rc = eng_sync_dir(db, "the links of a COMMIT's files");
    if (rc == 0)
        rc = leader_links_reserve(db, ja->n);
    if (rc) {
        free(files);
        leader_links_unlink(db, keys, ja->n);
        free(keys);
        return rc;
    }
    pthread_mutex_lock(&db->lock);
    uint64_t through = db->wal_flushed;
    pthread_mutex_unlock(&db->lock);
    mk m = {.type = MK_COMMIT, .span = 1};
    m.u.commit = (mk_commit){.op = (uint8_t)ja->op, .keys_form = MK_KEYS_NONE, .nfiles = (uint32_t)ja->n,
                             .flush_through = through, .events = 1, .files = files, .files_len = len};
    uint64_t fno = 0, lst = 0;
    struct lnote ln = {keys, ja->n};
    const bool late = eng_test_mutant == MUT_LEADER_NOTE_LATE; /* planted: noted after the append and its sync */
    rc = wal_markers_mk_at(db, &m, 1, &fno, &lst, late ? NULL : leader_links_note, &ln);
    free(files);
    if (fno && leader_test_marker_hook)
        leader_test_marker_hook(db, fno);
    if (fno && late)
        leader_links_note(db, fno, &ln);
    if (!fno) {
        leader_links_release(db, ja->n);
        leader_links_unlink(db, keys, ja->n); /* no marker names them */
    } /* else noted: in the log, they go with its segment */
    free(keys);
    if (rc)
        return rc; /* the marker may be in the log (a failed sync): the engine is failed by the WAL's own rules */
    rc = leader_marker_wait(db, fno, "a COMMIT marker was appended but its commit point did not come");
    lead_pos = rc == 0 ? fno : 0;
    return rc;
}

static int leader_drop(eng *db, int64_t ts)
{
    mk m = {.type = MK_DROP, .span = 1};
    m.u.drop = (mk_drop){.dropped_before = ts, .partition = MK_NO_PARTITION};
    uint64_t fno = 0;
    leader_now_marker(db); /* the replicas' clock first (design-cluster.md 5.4); refused, the DROP is refused too */
    /* eng_drop_before holds flush_lock until the retire (planted MUT_LEADER_DROP_UNHELD: it does not, as before) */
    int rc = eng_test_mutant == MUT_LEADER_DROP_UNHELD ? mt_flush_ex(db, &m, &fno) : mt_flush_held(db, &m, &fno);
    if (rc)
        return fno ? rc : leader_unappended(rc); /* no marker in the log: definitely not applied */
    if (fno == 0) /* a backstop: never retire without the marker */
        return TSDB_ERR_IO;
    rc = leader_marker_wait(db, fno, "a DROP marker was appended but its commit point did not come");
    lead_drop_pos = rc == 0 ? fno : 0;
    return rc;
}

int jr_markers(eng *db, const mk *m, size_t n, const char *what)
{
    if (!eng_jr(db) || db->rp || n == 0)
        return 0;
    /* one unit: one admission for the whole batch (wal_markers_mk), so a refusal (min_isr, not leading) leaves none
     * of them in the log and the write is simply refused; one sync for all of them */
    uint64_t fno = 0, last = 0;
    int rc = eng_test_mutant == MUT_LEADER_MARKERS_SPLIT ? 0 : wal_markers_mk(db, m, n, &fno, &last);
    if (eng_test_mutant == MUT_LEADER_MARKERS_SPLIT) /* planted: one admission and one sync per marker */
        for (size_t i = 0; i < n && rc == 0; i++) {
            uint64_t l = 0;
            rc = wal_markers_mk(db, &m[i], 1, &fno, &l);
            if (l)
                last = l;
        }
    if (rc && last == 0)
        return leader_unappended(rc); /* none is in the log: definitely not applied */
    if (rc) {
        /* in the log (a failed sync fails the engine by the WAL's rules; a planted split may leave a prefix): the
         * markers may commit without the records they name, so only a deposed leader (cut by the next one) goes on */
        if (!atomic_load(&db->lead_deposed))
            eng_fail(db, what, EIO);
        return TSDB_ERR_INDETERMINATE;
    }
    if (eng_test_mutant == MUT_LEADER_DICT_EARLY) /* planted: the record goes to the file before the commit point */
        return 0;
    return leader_marker_wait(db, last, what);
}

static int jr_dict(eng *db, uint16_t type, uint32_t id, const char *name, size_t len, int vtype)
{
    mk m = {.type = type, .span = 1};
    if (type == MK_SERIES)
        m.u.series = (mk_series){.id = id, .vtype = (uint32_t)vtype, .name = name, .name_len = (uint32_t)len};
    else
        m.u.state = (mk_state){.id = id, .name = name, .name_len = (uint32_t)len};
    return jr_markers(db, &m, 1, "a SERIES or STATE marker was appended but its commit point did not come");
}

int jr_dict_series(void *db, uint32_t id, const char *name, size_t len, int type)
{
    return jr_dict(db, MK_SERIES, id, name, len, type);
}

int jr_dict_state(void *db, uint32_t id, const char *name, size_t len, int type)
{
    return jr_dict(db, MK_STATE, id, name, len, type);
}

static int leader_retention(eng *db, const struct jr_ret_arg *ra)
{
    mk m = {.type = MK_RETENTION, .span = 1};
    m.u.retention = (mk_retention){.series = ra->series, .limit_ms = ra->limit_ms, .record_off = ra->off};
    return jr_markers(db, &m, 1, "a RETENTION marker was appended but its commit point did not come");
}

int jr_intent(eng *db, enum jr_kind kind, const void *arg)
{
    if (!eng_jr(db))
        return 0;
    switch (kind) {
    case JR_COMMIT:
        return leader_commit(db, arg);
    case JR_DROP:
        return leader_drop(db, *(const int64_t *)arg);
    case JR_RETENTION:
        return leader_retention(db, arg);
    default:
        return 0; /* SERIES, STATE and CLASS go through jr_markers from their dictionaries */
    }
}

/* ---- the leader's RSTATE (design-cluster.md 6.3: a deposed leader reopened as a replica) ----
 * A replica records in RSTATE the COMMIT and DROP markers it carried out (apply_commit, apply_drop), so a replay of its
 * log skips them; a leader publishes its own markers' effects without applying them, and records them the same way:
 * `doing` (the position and the first block) before parts_commit, `done` after it, a DROP with its `drops` entry after
 * its retire. Reopened as a replica (eng_open_replica on the same directory), the deposed leader then skips every
 * marker it published (its parts are listed: there are no staged files to apply them from) and settles one whose
 * publish a crash interrupted by its first block (rs_committed), as a replica does. All writes under publish_lock. */
int leader_rs_open(eng *db)
{
    struct rstate *rs = calloc(1, sizeof *rs);
    if (!rs)
        return TSDB_ERR_NOMEM;
    int rc = rs_load(db, rs); /* a promoted replica's carries on (replica_promote keeps it) */
    if (rc) {
        free(rs->drops);
        free(rs);
        return rc;
    }
    db->lrs = rs;
    return 0;
}

int leader_rs_kept(eng *db, uint64_t seq)
{
    struct rstate *rs = db->lrs;
    if (!rs || eng_test_mutant == MUT_LEADER_KEPT_NOSAVE)
        return 0;
    pthread_mutex_lock(&db->publish_lock); /* RSTATE's other writers (jr_precommit, jr_commit, leader_rs_drop) hold it */
    int rc = 0;
    if (seq > rs->kept) {
        rs->kept = seq;
        rc = rs_save(db, rs);
    }
    pthread_mutex_unlock(&db->publish_lock);
    return rc;
}

void leader_rs_prune(eng *db, uint64_t lo)
{
    struct rstate *rs = db->lrs;
    if (!rs || eng_test_mutant == MUT_LEADER_RS_NO_PRUNE)
        return;
    if (eng_test_mutant == MUT_LEADER_RS_PRUNE_ALL)
        lo = UINT64_MAX; /* planted: a DROP the retained log still holds goes too */
    pthread_mutex_lock(&db->publish_lock); /* RSTATE's writers hold it */
    /* As rs_prune_drops on a replica: a DROP below every retained record has no rows before it left to replay. One at
     * `lo` itself is kept (the next segment need not start exactly there). */
    size_t k = 0;
    for (size_t i = 0; i < rs->ndrops; i++)
        if (rs->drops[i].pos >= lo)
            rs->drops[k++] = rs->drops[i];
    rs->ndrops = k;
    pthread_mutex_unlock(&db->publish_lock);
}

long leader_rs_ndrops(eng *db, uint64_t *first)
{
    struct rstate *rs = db->lrs;
    if (!rs)
        return -1;
    pthread_mutex_lock(&db->publish_lock);
    long n = (long)rs->ndrops;
    *first = n ? rs->drops[0].pos : 0;
    pthread_mutex_unlock(&db->publish_lock);
    return n;
}

int jr_precommit(eng *db, size_t n)
{
    struct rstate *rs = db->lrs;
    if (!eng_jr(db) || !rs || !lead_pos || eng_test_mutant == MUT_LEADER_NO_RSTATE)
        return 0;
    rs->doing_pos = lead_pos;
    /* block numbers are taken under publish_lock: this commit's first */
    rs->doing_b0 = atomic_load(&db->next_block) + (eng_test_mutant == MUT_LEADER_B0_OFF ? 1000 : 0);
    rs->doing_n = n;
    return rs_save(db, rs);
}

void jr_commit(eng *db, enum jr_kind kind, const void *arg)
{
    (void)arg;
    struct rstate *rs = db->lrs;
    if (kind != JR_COMMIT || !eng_jr(db) || !rs || !lead_pos)
        return;
    uint64_t pos = lead_pos;
    lead_pos = 0;
    if (eng_test_mutant == MUT_LEADER_NO_RSTATE)
        return;
    rs->done = MAX(rs->done, pos);
    rs->doing_pos = rs->doing_b0 = rs->doing_n = 0;
    rs_save(db, rs); /* fails the engine on error: a replica reopened from here would apply the COMMIT again */
}

int leader_rs_drop(eng *db, int64_t ts)
{
    struct rstate *rs = db->lrs;
    uint64_t pos = lead_drop_pos;
    lead_drop_pos = 0;
    if (!eng_jr(db) || !rs || !pos || eng_test_mutant == MUT_LEADER_NO_RSTATE)
        return 0;
    pthread_mutex_lock(&db->publish_lock);
    struct rdrop *d = realloc(rs->drops, (rs->ndrops + 1) * sizeof *d);
    int rc = d ? 0 : TSDB_ERR_NOMEM;
    if (d) {
        rs->drops = d;
        rs->drops[rs->ndrops++] = (struct rdrop){pos, ts};
        rs->done = MAX(rs->done, pos);
        rc = rs_save(db, rs);
    } else {
        eng_fail(db, "recording a DROP in RSTATE", ENOMEM);
    }
    pthread_mutex_unlock(&db->publish_lock);
    return rc;
}
