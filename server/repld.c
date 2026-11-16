/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's replication driver (docs/design-cluster.md 5.1-5.4; r3d-stream). One `struct rvol` per cluster volume this
 * server holds: a `repl` core (src/repl.h), its thread, and the volume's engine, which it takes by reference for each
 * step (tsdb_cvol_ref), never across a swap. The data links (server/peerlink.h) are the node's: every node dials a log
 * and a bulk link to each server it shares a volume with, sends on the ones it dialed and takes frames from the ones it
 * accepted; the payloads are server/replwire.h's and carry the volume number.
 *
 * The core is sans-IO; this file carries its actions out, in order, on the volume's thread:
 *   WRITE     a replica: replica_append of the record's bytes, at the segment and offset they have at the leader (the
 *             APPEND being handled holds them). The leader's own records are the engine's: nothing to do (an EPOCH
 *             marker of a leader that is still a replica engine: replica_epoch_begin, whose position must be the one the
 *             core gave it).
 *   SYNC      a replica: deferred until the inbox is empty (group commit: one fdatasync for every APPEND that came in
 *             meanwhile), then replica_sync and repl_synced(replica_durable_end): a COMMIT marker is not durable before
 *             its files are staged. The writer: its WAL syncs itself; jr_ops.synced reports each move (repl_synced).
 *   TRUNCATE  replica_cut.   SAVE  the REPL file (replfile_save: fdatasync) before anything that follows, then the floor
 *             to the engine (replica_floor_saved; the leader's keep bound, eng_leader_keep).
 *   SEND      the core's message on the log link of its addressee; an APPEND gets its records here (below).
 *   APPLY     replica_apply, or eng_leader_commit on the writer. A leader that is still a replica engine promotes
 *             itself once the commit point passed its EPOCH marker (tsdb_cvol_replace: replica_promote, then
 *             eng_leader_attach before the writer is listed, so no write goes unreplicated).
 *   REPLY     the writer's waits are the engine's (eng_leader_wait); REPL_MOVED deposes it.
 *   FAIL      the volume stops: deposed, quiesced, reported failed (DECISION D13; the process goes on).
 * Messages from peers go through the volume's inbox (their bytes copied), and the thread hands one to the core and
 * carries out every action it caused before it takes the next: a WRITE needs that message's bytes.
 *
 * Records on the wire are WAL records, byte for byte. The core's records on the leader are admissions (one repl_write
 * per jr_ops.admit), several of which may join one WAL rows record, so the driver keeps its own cursor per peer
 * (`sent`, a WAL record end) and the core's SEND APPEND says only where the peer wants to be (`want`) or that it has to
 * go back (a resync). The writer's records go out at once from jr_ops.bytes (after the leader's write(), before its
 * fdatasync, design 5.1 step 2) to every peer whose cursor is at their start; a peer behind is caught up from the log
 * (replica_read, which on the writer reads its kept segments and the current one as far as it is written). A resync
 * moves the cursor back only once what was sent after it has had RESEND_MS to arrive: the core's heartbeat acks ask for
 * a resync every time, and in-flight records would otherwise be sent twice.
 *
 * rec_find answers from the driver's index of WAL record starts (fed by the leader's bytes and the replica's WRITEs,
 * built at open by reading the log), and (pos, 1) for a position the writer admitted but has not written yet.
 *
 * Mutants (TSDB_REPLD_MUTANT, tests only; NOTES of r3d-stream says which test catches each): `commitbyleader` commits
 * at the writer's own durable end, `ackbeforesync` has a follower report its written end as durable without a sync,
 * `floornotsaved` skips the REPL file's SAVE.
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "repld.h"
#include "peerlink.h"
#include "replwire.h"
#include "../src/cvol.h"
#include "../src/repl.h"
#include "../src/replfile.h"
#include "../src/replica.h"

#define TICK_MS 5
#define RESEND_MS 50
#define READ_CAP (4u << 20)
#define MAX_NODES 64

enum { M_REPLICA = 0, M_PROMOTING, M_WRITER, M_DEAD };
enum { IN_MSG = 1, IN_FETCH, IN_META, IN_WAKE };

struct item {
    struct item *next;
    int kind;
    uint32_t node;
    size_t n;
    uint8_t data[];
};

struct rpeer {
    uint64_t sent;     /* the last position whose records went out to it (a WAL record end) */
    uint64_t want;     /* the core wants it to have up to here */
    uint64_t sent_ms;  /* when `sent` last moved forward */
    uint64_t acked;    /* the `written` of its last ACK in this run */
    uint64_t gen;      /* the log link's connection the cursor was moved on */
    int heard;
};

struct rvol {
    uint32_t number;
    char dir[4200];
    pthread_mutex_t mu; /* the core, the index, the cursors, the inbox; below buf_lock (jr_ops.admit takes it) */
    pthread_cond_t cv;
    pthread_t th;
    int started, stop, mode, failed;
    struct repl *core;
    struct replfile *rf;
    struct replfile_state st;
    struct cluster_vol_rec rec;
    int self, nnodes;
    uint64_t epoch; /* the core's, cached for the bytes path */
    struct rpeer peer[REPL_MAX_NODES];
    struct item *in_head, *in_tail;
    /* the index: starts of the WAL records from the log start, ascending; idx_end the last position indexed */
    uint64_t *idx;
    size_t nidx, cidx;
    uint64_t idx_end, log_start;
    struct repl_epoch hist[REPLFILE_MAX_HIST];
    uint32_t nhist;
    /* the message being handled */
    struct rw_msg cur;
    struct repl_rec *recs;
    uint32_t *lens;
    uint32_t crec;
    uint64_t *coff; /* byte offset of each record of cur */
    int sync_wanted;
    uint64_t synced_rep; /* the writer: the last durable end handed to repl_synced */
    uint64_t epoch_pos;  /* M_PROMOTING: the EPOCH marker's position */
    uint64_t fetch_ms;
    char fetch_name[RW_NAME_MAX + 1];
    struct jr_ops ops;
};

enum { RM_NONE, RM_COMMITBYLEADER, RM_ACKBEFORESYNC, RM_FLOORNOTSAVED };

static struct {
    pthread_mutex_t mu;
    tsdb *t;
    char dir[4096];
    struct peerlink *pl;
    uint32_t self;
    struct {
        uint32_t node;
        char addr[META_ADDR_MAX];
        struct plink *log, *bulk;
    } nodes[MAX_NODES];
    uint32_t nn;
    struct rvol *vols[CLUSTER_REPORT_VOLS];
    uint32_t nvols;
    uint64_t fileno;
    int mutant; /* RM_*: tests only */
    struct rin *ins; /* bulk files being received */
} G = {.mu = PTHREAD_MUTEX_INITIALIZER};

static void say(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void say(const char *fmt, ...)
{
    char b[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    fprintf(stderr, "tsdbd: replication: %s\n", b);
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static struct rvol *vol_find(uint32_t number)
{
    struct rvol *rv = NULL;
    pthread_mutex_lock(&G.mu);
    for (uint32_t i = 0; i < G.nvols && !rv; i++)
        if (G.vols[i]->number == number)
            rv = G.vols[i];
    pthread_mutex_unlock(&G.mu);
    return rv;
}

static int node_index(const struct rvol *rv, uint32_t node)
{
    for (uint32_t i = 0; i < rv->rec.nrep; i++)
        if (rv->rec.rep[i].node == node)
            return (int)i;
    return -1;
}

static struct plink *link_of(uint32_t node, int bulk)
{
    struct plink *l = NULL;
    pthread_mutex_lock(&G.mu);
    for (uint32_t i = 0; i < G.nn; i++)
        if (G.nodes[i].node == node)
            l = bulk ? G.nodes[i].bulk : G.nodes[i].log;
    pthread_mutex_unlock(&G.mu);
    return l;
}

/* ------------------------------------------------------------------ the index and the epoch history (under rv->mu) */

static void idx_push(struct rvol *rv, uint64_t start, uint64_t end)
{
    if (start <= rv->idx_end)
        return; /* known already (the leader's catch-up read and its bytes may both report a record) */
    if (rv->nidx == rv->cidx) {
        size_t c = rv->cidx ? rv->cidx * 2 : 1024;
        uint64_t *p = realloc(rv->idx, c * sizeof *p);
        if (!p)
            return; /* rec_find answers (pos, 1) past the index: only the leader's rounding would notice */
        rv->idx = p, rv->cidx = c;
    }
    rv->idx[rv->nidx++] = start;
    rv->idx_end = end;
}

static void idx_cut(struct rvol *rv, uint64_t last)
{
    while (rv->nidx && rv->idx[rv->nidx - 1] > last)
        rv->nidx--;
    rv->idx_end = last;
}

static void idx_trim(struct rvol *rv, uint64_t start)
{
    size_t k = 0;
    while (k < rv->nidx && k + 1 < rv->nidx && rv->idx[k + 1] <= start)
        k++;
    if (k && rv->nidx > k && rv->idx[k] <= start) {
        memmove(rv->idx, rv->idx + k, (rv->nidx - k) * sizeof *rv->idx);
        rv->nidx -= k;
    }
}

static int idx_find(void *ctx, uint64_t pos, uint64_t *s, uint32_t *n)
{
    struct rvol *rv = ctx;
    if (pos < rv->log_start)
        return 1;
    if (pos > rv->idx_end || rv->nidx == 0 || rv->idx[0] > pos) {
        if (pos <= rv->idx_end && rv->nidx && rv->idx[0] > pos)
            return 1; /* below what the index holds: trimmed with the log */
        *s = pos, *n = 1;
        return 0;
    }
    size_t lo = 0, hi = rv->nidx; /* the last start <= pos */
    while (hi - lo > 1) {
        size_t m = (lo + hi) / 2;
        if (rv->idx[m] <= pos)
            lo = m;
        else
            hi = m;
    }
    uint64_t end = lo + 1 < rv->nidx ? rv->idx[lo + 1] - 1 : rv->idx_end;
    *s = rv->idx[lo];
    *n = (uint32_t)(end - rv->idx[lo] + 1);
    return 0;
}

static uint64_t rec_start(struct rvol *rv, uint64_t pos)
{
    uint64_t s;
    uint32_t n;
    return idx_find(rv, pos, &s, &n) == 0 ? s : pos;
}

static void hist_note(struct rvol *rv, uint64_t epoch, uint64_t pos)
{
    if (rv->nhist && rv->hist[rv->nhist - 1].epoch >= epoch)
        return;
    if (rv->nhist == REPLFILE_MAX_HIST) {
        memmove(rv->hist, rv->hist + 1, (REPLFILE_MAX_HIST - 1) * sizeof rv->hist[0]);
        rv->nhist--;
    }
    rv->hist[rv->nhist++] = (struct repl_epoch){.epoch = epoch, .start = pos};
}

static void hist_cut(struct rvol *rv, uint64_t last)
{
    while (rv->nhist && rv->hist[rv->nhist - 1].start > last)
        rv->nhist--;
}

static uint64_t epoch_at(const struct rvol *rv, uint64_t pos)
{
    uint64_t e = 0;
    for (uint32_t i = 0; i < rv->nhist && rv->hist[i].start <= pos; i++)
        e = rv->hist[i].epoch;
    return pos ? e : 0;
}

/* ------------------------------------------------------------------ the log on disk */

struct segscan {
    uint64_t first_fno; /* of the lowest segment (0: none) */
};

/* The segments of the volume's directory: the epoch history they name (a segment `e << 40` and above belongs to epoch
 * e; its first record starts the epoch where it is the first of its epoch) and the first position the log holds. */
static int scan_segments(struct rvol *rv, struct repl_epoch *h, uint32_t *nh, uint64_t *first)
{
    DIR *d = opendir(rv->dir);
    if (!d)
        return -1;
    uint64_t *seqs = NULL;
    size_t n = 0, c = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        uint64_t seq;
        char tail[8];
        if (sscanf(e->d_name, "wal_%16" SCNx64 ".lo%1s", &seq, tail) != 2 || strlen(e->d_name) != 24)
            continue;
        if (n == c) {
            c = c ? 2 * c : 64;
            uint64_t *p = realloc(seqs, c * sizeof *p);
            if (!p)
                break;
            seqs = p;
        }
        seqs[n++] = seq;
    }
    closedir(d);
    for (size_t i = 1; i < n; i++) /* insertion sort: a few segments */
        for (size_t j = i; j > 0 && seqs[j - 1] > seqs[j]; j--) {
            uint64_t x = seqs[j];
            seqs[j] = seqs[j - 1], seqs[j - 1] = x;
        }
    *nh = 0;
    *first = 0;
    for (size_t i = 0; i < n; i++) {
        char nm[4300];
        uint8_t hd[WAL_HDR];
        snprintf(nm, sizeof nm, "%s/wal_%016" PRIx64 ".log", rv->dir, seqs[i]);
        int fd = open(nm, O_RDONLY | O_CLOEXEC);
        ssize_t got = fd >= 0 ? pread(fd, hd, sizeof hd, 0) : -1;
        if (fd >= 0)
            close(fd);
        if (got != (ssize_t)sizeof hd)
            continue; /* empty (a new segment) or gone meanwhile */
        uint64_t fno;
        memcpy(&fno, hd + 16, 8);
        if (!*first)
            *first = fno;
        uint64_t ep = seqs[i] >> 40;
        if (ep && (*nh == 0 || h[*nh - 1].epoch < ep) && *nh < REPLFILE_MAX_HIST)
            h[(*nh)++] = (struct repl_epoch){.epoch = ep, .start = fno};
    }
    free(seqs);
    return 0;
}

/* Reads the log from `from` into the index (at open). */
static void index_log(struct rvol *rv, struct eng *db, uint64_t from, uint64_t end)
{
    while (from && from <= end) {
        uint8_t *buf = NULL;
        size_t len = 0;
        uint64_t seq, off, last;
        if (replica_read(db, from, &buf, &len, READ_CAP, &seq, &off, &last) != 0 || len == 0) {
            free(buf);
            break;
        }
        size_t o = 0;
        while (o < len) {
            wal_rec wr;
            size_t sz = wal_record(buf + o, len - o, seq, off + o, &wr);
            if (!sz)
                break;
            idx_push(rv, wr.fno, wr.fno + wr.span - 1);
            o += sz;
        }
        free(buf);
        from = last + 1;
    }
}

/* ------------------------------------------------------------------ sending */

static void send_msg(struct rvol *rv, const struct repl_msg *m)
{
    if (m->to < 0 || (uint32_t)m->to >= rv->rec.nrep)
        return; /* the metadata group's messages go through the cluster thread (ISR changes: not wired yet) */
    struct plink *l = link_of(rv->rec.rep[m->to].node, 0);
    uint8_t b[RW_HEAD];
    size_t n = rw_encode_msg(b, sizeof b, rv->number, m, NULL, 0, 0, NULL, 0);
    if (l && n)
        plink_send(l, RW_T_MSG, b, n, 0);
}

/* An APPEND to peer `to` of the records [buf, buf + len) of segment seq at off (whole WAL records), positions
 * first .. last. Under rv->mu. 0 once queued. */
static int send_records(struct rvol *rv, int to, uint64_t seq, uint64_t off, const uint8_t *buf, size_t len,
                        uint64_t first)
{
    struct plink *l = link_of(rv->rec.rep[to].node, 0);
    if (!l)
        return -1;
    uint32_t nrec = 0;
    for (size_t o = 0; o < len;) {
        wal_rec wr;
        size_t sz = wal_record(buf + o, len - o, seq, off + o, &wr);
        if (!sz)
            return -1;
        if (nrec == rv->crec) {
            uint32_t c = rv->crec ? rv->crec * 2 : 256;
            struct repl_rec *r = realloc(rv->recs, c * sizeof *r);
            if (r)
                rv->recs = r;
            uint32_t *ln = realloc(rv->lens, c * sizeof *ln);
            if (ln)
                rv->lens = ln;
            uint64_t *co = realloc(rv->coff, c * sizeof *co);
            if (co)
                rv->coff = co;
            if (!r || !ln || !co)
                return -1;
            rv->crec = c;
        }
        rv->recs[nrec] = (struct repl_rec){.pos = wr.fno, .epoch = epoch_at(rv, wr.fno), .n = (uint32_t)wr.span};
        rv->lens[nrec++] = (uint32_t)sz;
        o += sz;
    }
    if (!nrec || rv->recs[0].pos != first)
        return -1;
    struct repl_msg m = {.type = REPL_M_APPEND, .from = rv->self, .to = to, .epoch = repl_epoch(rv->core),
                         .leader = rv->self, .prev_pos = first - 1, .prev_epoch = epoch_at(rv, first - 1),
                         .commit = repl_commit(rv->core), .nrec = nrec, .rec = rv->recs, .meta.leader = -1};
    size_t cap = rw_msg_size(nrec, len);
    if (cap > PL_FRAME_MAX)
        return -1;
    uint8_t *b = malloc(cap);
    size_t n = b ? rw_encode_msg(b, cap, rv->number, &m, rv->lens, seq, off, buf, len) : 0;
    int rc = n ? plink_send(l, RW_T_MSG, b, n, 0) : -1;
    free(b);
    return rc == PL_OK ? 0 : -1;
}

/* Catch-up of the peers whose cursor is behind what the core wants them to have: one read per peer per call. */
static void catch_up(struct rvol *rv, struct eng *db)
{
    for (int i = 0; i < (int)rv->rec.nrep; i++) {
        struct rpeer *p = &rv->peer[i];
        if (i == rv->self || p->sent >= p->want)
            continue;
        uint64_t from = p->sent + 1;
        pthread_mutex_unlock(&rv->mu);
        uint8_t *buf = NULL;
        size_t len = 0;
        uint64_t seq = 0, off = 0, last = 0;
        int rc = replica_read(db, from, &buf, &len, READ_CAP, &seq, &off, &last);
        pthread_mutex_lock(&rv->mu);
        if (rc == 0 && len && p->sent + 1 == from && send_records(rv, i, seq, off, buf, len, from) == 0) {
            p->sent = last;
            p->sent_ms = now_ms();
        }
        free(buf);
    }
}

/* jr_ops.bytes: the writer's records, before its fdatasync; out at once to every peer whose cursor is at them */
static void op_bytes(void *ctx, struct eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len,
                     uint64_t first, uint64_t last)
{
    (void)db;
    struct rvol *rv = ctx;
    pthread_mutex_lock(&rv->mu);
    for (size_t o = 0; o < len;) {
        wal_rec wr;
        size_t sz = wal_record((const uint8_t *)buf + o, len - o, seq, off + o, &wr);
        if (!sz)
            break;
        idx_push(rv, wr.fno, wr.fno + wr.span - 1);
        o += sz;
    }
    for (int i = 0; i < (int)rv->rec.nrep; i++) {
        struct rpeer *p = &rv->peer[i];
        if (i != rv->self && p->sent + 1 == first && send_records(rv, i, seq, off, buf, len, first) == 0) {
            p->sent = last;
            p->sent_ms = now_ms();
        }
    }
    pthread_mutex_unlock(&rv->mu);
}

/* jr_ops.admit, under buf_lock: the core's positions must be the engine's */
static int op_admit(void *ctx, struct eng *db, uint64_t fno, uint32_t span)
{
    struct rvol *rv = ctx;
    pthread_mutex_lock(&rv->mu);
    uint64_t pos = 0;
    int rc = rv->mode == M_WRITER && rv->core ? repl_write(rv->core, span, &pos) : REPL_E_NOTLEADER;
    if (rc == REPL_OK && pos != fno) {
        eng_fail(db, "replication: the core's position is not the log number", TSDB_ERR_CORRUPT);
        rc = REPL_E_FAILED;
    }
    pthread_cond_signal(&rv->cv);
    pthread_mutex_unlock(&rv->mu);
    if (rc == REPL_E_NOTLEADER)
        eng_leader_depose(db);
    return rc == REPL_OK ? 0 : TSDB_ERR_UNAVAILABLE;
}

/* jr_ops.synced: the writer's own durable end */
static void op_synced(void *ctx, struct eng *db, uint64_t last)
{
    (void)db;
    struct rvol *rv = ctx;
    pthread_mutex_lock(&rv->mu);
    if (rv->core && last > rv->synced_rep) {
        rv->synced_rep = last;
        repl_synced(rv->core, last);
        if (G.mutant == RM_COMMITBYLEADER)
            eng_leader_commit(db, last);
        pthread_cond_signal(&rv->cv);
    }
    pthread_mutex_unlock(&rv->mu);
}

/* ------------------------------------------------------------------ bulk files */

/* The jobs of one file to its peers. `detached`: nobody waits; the last `done` closes fd and frees it. */
struct shipwait {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int pending, failed, detached, fd;
};

static struct shipwait *ship_new(int fd)
{
    struct shipwait *w = calloc(1, sizeof *w);
    if (!w)
        return NULL;
    pthread_mutex_init(&w->mu, NULL);
    pthread_cond_init(&w->cv, NULL);
    w->fd = fd;
    return w;
}

static void ship_free(struct shipwait *w)
{
    close(w->fd);
    pthread_mutex_destroy(&w->mu);
    pthread_cond_destroy(&w->cv);
    free(w);
}

/* Lets go of w: freed now if no job holds it, else by the last done. */
static void ship_detach(struct shipwait *w)
{
    pthread_mutex_lock(&w->mu);
    int left = w->pending;
    w->detached = 1;
    pthread_mutex_unlock(&w->mu);
    if (!left)
        ship_free(w);
}

static void ship_done(void *arg, uint64_t file, int status, uint64_t sent)
{
    (void)file, (void)sent;
    struct shipwait *w = arg;
    pthread_mutex_lock(&w->mu);
    int last = --w->pending == 0 && w->detached;
    if (status != PL_OK)
        w->failed++;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
    if (last)
        ship_free(w);
}

/* Sends the file (fd, size) under marker name `mname` to `to` (a node index), or to every other replica (-1). With
 * `wait`, returns once each has answered or after 30 s (a peer that did not stage it fetches it, replica_missing). */
static void bulk_out(struct rvol *rv, int fd, uint64_t size, const char *mname, int to, struct shipwait *w)
{
    uint8_t meta[PL_META_MAX];
    size_t ml = rw_encode_bulk_meta(meta, sizeof meta, rv->number, size, mname);
    if (!ml)
        return;
    for (int i = 0; i < (int)rv->rec.nrep; i++) {
        if (i == rv->self || (to >= 0 && i != to))
            continue;
        struct plink *l = link_of(rv->rec.rep[i].node, 1);
        if (!l)
            continue;
        pthread_mutex_lock(&G.mu);
        uint64_t file = ++G.fileno;
        pthread_mutex_unlock(&G.mu);
        struct pl_job j = {.file = file, .fd = fd, .base = 0, .length = size, .meta = meta, .meta_len = ml,
                           .max_attempts = 3, .done = ship_done, .arg = w};
        pthread_mutex_lock(&w->mu);
        w->pending++;
        pthread_mutex_unlock(&w->mu);
        if (plink_bulk_send(l, &j) != PL_OK) {
            pthread_mutex_lock(&w->mu);
            w->pending--;
            pthread_mutex_unlock(&w->mu);
        }
    }
}

static void wait_ship(struct shipwait *w, int ms)
{
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += ms / 1000;
    pthread_mutex_lock(&w->mu);
    while (w->pending > 0 && pthread_cond_timedwait(&w->cv, &w->mu, &dl) != ETIMEDOUT)
        ;
    pthread_mutex_unlock(&w->mu);
}

/* jr_ops.ship: the pending part file of a COMMIT, staged by every replica it reaches (on the writer's thread) */
static int op_ship(void *ctx, struct eng *db, const char *file, const char *mname, uint64_t size)
{
    struct rvol *rv = ctx;
    int fd = openat(db->dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    struct shipwait *w = ship_new(fd);
    if (!w) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    pthread_mutex_lock(&rv->mu);
    bulk_out(rv, fd, size, mname, -1, w);
    pthread_mutex_unlock(&rv->mu);
    wait_ship(w, 30000);
    ship_detach(w); /* a job still queued reads fd later: the last done closes it */
    return 0;
}

struct rin {
    struct rin *next;
    struct plink *l;
    uint64_t file, total;
    uint32_t volume;
    char name[RW_NAME_MAX + 1];
    uint8_t *buf;
};

static struct rin *rin_find(struct plink *l, uint64_t file, int unlink_it)
{
    for (struct rin **pp = &G.ins; *pp; pp = &(*pp)->next)
        if ((*pp)->l == l && (*pp)->file == file) {
            struct rin *r = *pp;
            if (unlink_it)
                *pp = r->next;
            return r;
        }
    return NULL;
}

static int bin_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                    uint64_t *resume, int *prefix_fd)
{
    (void)arg;
    struct rin *r = calloc(1, sizeof *r);
    uint64_t size;
    if (!r || rw_decode_bulk_meta(meta, meta_len, &r->volume, &size, r->name, sizeof r->name) != 0 ||
        size != total || total > (1ull << 32) || !vol_find(r->volume) || !(r->buf = malloc(total ? total : 1))) {
        free(r);
        return -1;
    }
    r->l = l, r->file = file, r->total = total;
    *resume = 0;
    *prefix_fd = -1;
    pthread_mutex_lock(&G.mu);
    struct rin *old = rin_find(l, file, 1); /* the same file begun again (a reconnect): from 0 */
    if (old) {
        free(old->buf);
        free(old);
    }
    r->next = G.ins;
    G.ins = r;
    pthread_mutex_unlock(&G.mu);
    return 0;
}

static void post(struct rvol *rv, int kind, uint32_t node, const void *p, size_t n);

static int bin_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    (void)arg;
    pthread_mutex_lock(&G.mu);
    struct rin *r = rin_find(l, file, 0);
    pthread_mutex_unlock(&G.mu);
    if (!r || off > r->total || n > r->total - off)
        return -1;
    memcpy(r->buf + off, p, n);
    if (off + n < r->total)
        return 0;
    /* the last bytes: staged (file and directory synced) before the sender hears DONE */
    struct rvol *rv = vol_find(r->volume);
    if (!rv)
        return -1;
    struct eng *db = tsdb_cvol_ref(G.t, r->volume);
    int rc = db ? replica_stage(db, r->name, r->buf, r->total) : TSDB_ERR_UNAVAILABLE;
    tsdb_cvol_unref(&db);
    if (rc == 0)
        post(rv, IN_WAKE, 0, NULL, 0); /* a COMMIT may be durable now: sync and ACK */
    return rc == 0 ? 0 : -1;
}

static void bin_end(void *arg, struct plink *l, uint64_t file, int status)
{
    (void)arg, (void)status;
    pthread_mutex_lock(&G.mu);
    struct rin *r = rin_find(l, file, 1);
    pthread_mutex_unlock(&G.mu);
    if (r) {
        free(r->buf);
        free(r);
    }
}

/* ------------------------------------------------------------------ the inbox */

static void post(struct rvol *rv, int kind, uint32_t node, const void *p, size_t n)
{
    struct item *it = malloc(sizeof *it + n);
    if (!it)
        return; /* a lost message: the core sends again */
    it->next = NULL, it->kind = kind, it->node = node, it->n = n;
    if (n)
        memcpy(it->data, p, n);
    pthread_mutex_lock(&rv->mu);
    if (rv->in_tail)
        rv->in_tail->next = it;
    else
        rv->in_head = it;
    rv->in_tail = it;
    pthread_cond_signal(&rv->cv);
    pthread_mutex_unlock(&rv->mu);
}

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    (void)arg;
    uint32_t volume;
    if (type == RW_T_MSG && n >= 8)
        memcpy(&volume, p + 4, 4);
    else if (type == RW_T_FETCH && n >= 5)
        memcpy(&volume, p + 1, 4);
    else
        return;
    struct rvol *rv = vol_find(volume);
    if (rv)
        post(rv, type == RW_T_MSG ? IN_MSG : IN_FETCH, plink_peer(l), p, n);
}

static void on_link(void *arg, struct plink *l, int up)
{
    (void)arg, (void)l, (void)up;
}

/* ------------------------------------------------------------------ actions */

static void vol_fail(struct rvol *rv, const char *why)
{
    if (rv->failed)
        return;
    rv->failed = 1;
    say("volume %" PRIu32 ": stopped: %s", rv->number, why);
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    if (db && rv->mode == M_WRITER)
        eng_leader_depose(db);
    tsdb_cvol_unref(&db);
}

static int promote_fn(void *ctx, struct eng **old, struct eng **neu)
{
    struct rvol *rv = ctx;
    int rc = replica_promote(old, rv->epoch_pos, neu);
    if (rc == 0 && (rc = eng_leader_attach(*neu, &rv->ops)) != 0) {
        eng_close(*neu);
        *neu = NULL;
    }
    return rc;
}

/* Saves REPL with the core's fence, led and floor, the driver's log start and epoch history. Under rv->mu, which it
 * lets go of for the write. */
static int save_repl(struct rvol *rv, uint64_t fence, uint64_t led, uint64_t floor)
{
    rv->st.fence = fence;
    rv->st.led = led;
    rv->st.floor = floor;
    if (rv->log_start > rv->st.log_start && rv->log_start - 1 <= floor)
        rv->st.log_start = rv->log_start;
    struct repl_epoch *h = malloc((rv->nhist ? rv->nhist : 1) * sizeof *h);
    if (!h)
        return TSDB_ERR_NOMEM;
    memcpy(h, rv->hist, rv->nhist * sizeof *h);
    free(rv->st.hist);
    rv->st.hist = h;
    rv->st.nhist = rv->nhist;
    pthread_mutex_unlock(&rv->mu);
    int rc = replfile_save(rv->rf, &rv->st);
    pthread_mutex_lock(&rv->mu);
    return rc;
}

/* The log's start moved (segments in parts went): the core and the index hear it. */
static void log_start_check(struct rvol *rv)
{
    struct repl_epoch h[REPLFILE_MAX_HIST];
    uint32_t nh;
    uint64_t first;
    pthread_mutex_unlock(&rv->mu);
    int rc = scan_segments(rv, h, &nh, &first);
    pthread_mutex_lock(&rv->mu);
    if (rc == 0 && first > rv->log_start) {
        rv->log_start = first;
        repl_log_start(rv->core, first);
        idx_trim(rv, first);
    }
}

static void exec(struct rvol *rv, struct repl_action *a)
{
    struct eng *db = NULL;
    int rc = 0;
    switch (a->type) {
    case REPL_A_WRITE:
        if (a->src == REPL_SRC_CLIENT)
            break; /* the writer's own record: the engine wrote it */
        db = tsdb_cvol_ref(G.t, rv->number);
        if (!db) {
            vol_fail(rv, "the engine went away under a write");
            break;
        }
        if (a->src == REPL_SRC_EPOCH) {
            char owner[32];
            uint64_t pos = 0;
            snprintf(owner, sizeof owner, "node %" PRIu32, G.self);
            pthread_mutex_unlock(&rv->mu);
            rc = replica_epoch_begin(db, a->rec.epoch, owner, &pos);
            pthread_mutex_lock(&rv->mu);
            if (rc == 0 && pos != a->rec.pos)
                rc = TSDB_ERR_CORRUPT;
            if (rc == 0) {
                rv->mode = M_PROMOTING;
                rv->epoch_pos = pos;
                hist_note(rv, a->rec.epoch, pos);
                idx_push(rv, pos, pos);
            }
        } else if (a->idx < rv->cur.m.nrec) {
            uint32_t i = a->idx;
            pthread_mutex_unlock(&rv->mu);
            rc = replica_append(db, rv->cur.seq, rv->cur.off + rv->coff[i], rv->cur.bytes + rv->coff[i],
                                rv->cur.lens[i]);
            pthread_mutex_lock(&rv->mu);
            if (rc == 0) {
                hist_note(rv, a->rec.epoch, a->rec.pos);
                idx_push(rv, a->rec.pos, a->rec.pos + a->rec.n - 1);
            }
        } else {
            rc = TSDB_ERR_INVALID;
        }
        if (rc != 0) {
            char why[96];
            snprintf(why, sizeof why, "a write of the log failed: %s", tsdb_strerror(rc));
            vol_fail(rv, why);
        }
        break;
    case REPL_A_SYNC:
        if (rv->mode == M_WRITER) {
            db = tsdb_cvol_ref(G.t, rv->number);
            uint64_t d = db ? eng_wal_durable_fno(db) : 0;
            if (d > rv->synced_rep) {
                rv->synced_rep = d;
                repl_synced(rv->core, d);
            }
        } else {
            rv->sync_wanted = 1;
        }
        break;
    case REPL_A_TRUNCATE:
        db = tsdb_cvol_ref(G.t, rv->number);
        pthread_mutex_unlock(&rv->mu);
        rc = db ? replica_cut(db, a->pos) : TSDB_ERR_UNAVAILABLE;
        pthread_mutex_lock(&rv->mu);
        if (rc != 0) {
            vol_fail(rv, "a cut of the log failed");
            break;
        }
        idx_cut(rv, a->pos);
        hist_cut(rv, a->pos);
        break;
    case REPL_A_SAVE:
        rc = G.mutant == RM_FLOORNOTSAVED ? 0 : save_repl(rv, a->fence, a->led, a->floor);
        if (rc != 0) {
            vol_fail(rv, "the REPL file could not be saved");
            break;
        }
        db = tsdb_cvol_ref(G.t, rv->number);
        if (!db)
            break;
        if (rv->mode == M_WRITER) {
            uint64_t keep = a->floor;
            for (int i = 0; i < (int)rv->rec.nrep; i++)
                if (i != rv->self && rv->peer[i].acked < keep)
                    keep = rv->peer[i].acked; /* a peer never heard from holds everything */
            pthread_mutex_unlock(&rv->mu);
            eng_leader_keep(db, keep);
            pthread_mutex_lock(&rv->mu);
        } else if (a->floor) {
            pthread_mutex_unlock(&rv->mu);
            rc = replica_floor_saved(db, a->floor);
            pthread_mutex_lock(&rv->mu);
        }
        log_start_check(rv);
        break;
    case REPL_A_SEND:
        if (a->msg.type == REPL_M_APPEND && a->msg.to >= 0 && a->msg.to < (int)rv->rec.nrep) {
            struct rpeer *p = &rv->peer[a->msg.to];
            if (a->upto > p->want)
                p->want = a->upto;
            if (a->msg.prev_pos < p->sent && (now_ms() - p->sent_ms >= RESEND_MS || !p->heard)) {
                p->sent = a->msg.prev_pos ? rec_start(rv, a->msg.prev_pos + 1) - 1 : 0;
                if (p->sent > a->msg.prev_pos)
                    p->sent = a->msg.prev_pos;
            }
            if (a->upto <= a->msg.prev_pos) {
                /* a heartbeat: the commit point, after what the peer has been sent */
                struct repl_msg m = a->msg;
                if (m.prev_pos > p->sent)
                    m.prev_pos = p->sent;
                m.prev_epoch = epoch_at(rv, m.prev_pos);
                send_msg(rv, &m);
            }
        } else {
            send_msg(rv, &a->msg);
        }
        break;
    case REPL_A_APPLY:
        db = tsdb_cvol_ref(G.t, rv->number);
        if (!db)
            break;
        if (rv->mode == M_WRITER) {
            eng_leader_commit(db, a->pos);
            break;
        }
        pthread_mutex_unlock(&rv->mu);
        rc = replica_apply(db, a->pos);
        pthread_mutex_lock(&rv->mu);
        if (rc != 0) {
            vol_fail(rv, "an apply failed");
            break;
        }
        if (rv->mode == M_PROMOTING && a->pos >= rv->epoch_pos) {
            tsdb_cvol_unref(&db);
            pthread_mutex_unlock(&rv->mu);
            rc = tsdb_cvol_replace(G.t, rv->number, promote_fn, rv, CVOL_WRITER);
            pthread_mutex_lock(&rv->mu);
            if (rc != 0) {
                vol_fail(rv, "the promotion to the writer failed");
                break;
            }
            rv->mode = M_WRITER;
            rv->synced_rep = a->pos;
            say("volume %" PRIu32 ": leads epoch %" PRIu64 " from position %" PRIu64, rv->number, rv->epoch,
                rv->epoch_pos);
        }
        break;
    case REPL_A_REPLY:
        if (a->status == REPL_MOVED && (db = tsdb_cvol_ref(G.t, rv->number)) && rv->mode == M_WRITER)
            eng_leader_depose(db);
        break;
    case REPL_A_FAIL:
        vol_fail(rv, a->reason ? a->reason : "the core failed");
        break;
    case REPL_A_SNAPSHOT:
        say("volume %" PRIu32 ": the leader no longer has the records this replica needs: it needs a snapshot (not"
            " built yet); it stays as it is", rv->number);
        break;
    }
    tsdb_cvol_unref(&db);
}

static void drain(struct rvol *rv)
{
    struct repl_action a;
    while (!rv->failed && rv->core && repl_next_action(rv->core, &a)) {
        exec(rv, &a);
        rv->epoch = repl_epoch(rv->core);
    }
}

/* ------------------------------------------------------------------ events */

static struct repl_vmeta vmeta_of(const struct rvol *rv)
{
    struct repl_vmeta m = {.epoch = rv->rec.epoch, .isr_version = rv->rec.isr_version, .leader = -1};
    m.leader = node_index(rv, rv->rec.leader);
    for (uint32_t i = 0; i < rv->rec.nisr; i++) {
        int k = node_index(rv, rv->rec.isr[i]);
        if (k >= 0) {
            m.isr |= 1u << k;
            m.rid[k] = rv->rec.rep[k].rid;
        }
    }
    return m;
}

static void handle(struct rvol *rv, struct item *it)
{
    if (it->kind == IN_META) {
        struct repl_msg m = {.type = REPL_M_META, .from = REPL_META, .to = rv->self, .leader = -1};
        m.meta = vmeta_of(rv);
        m.epoch = m.meta.epoch;
        repl_recv(rv->core, &m);
        drain(rv);
        return;
    }
    int from = node_index(rv, it->node);
    if (from < 0 || from == rv->self)
        return;
    if (it->kind == IN_FETCH) {
        char name[RW_NAME_MAX + 1];
        uint32_t vol;
        if (rw_decode_fetch(it->data, it->n, &vol, name, sizeof name) != 0 || rv->mode != M_WRITER)
            return;
        struct eng *db = tsdb_cvol_ref(G.t, rv->number);
        int fd = db ? eng_leader_file_open(db, name) : TSDB_ERR_UNAVAILABLE;
        tsdb_cvol_unref(&db);
        struct stat sb;
        struct shipwait *w = fd >= 0 && fstat(fd, &sb) == 0 ? ship_new(fd) : NULL;
        if (w) {
            bulk_out(rv, fd, (uint64_t)sb.st_size, name, from, w);
            ship_detach(w);
        } else if (fd >= 0) {
            close(fd);
        } else if (fd == TSDB_ERR_NOTFOUND) {
            say("volume %" PRIu32 ": node %" PRIu32 " asks for %s, whose segment went: it needs a snapshot",
                rv->number, it->node, name);
        }
        return;
    }
    /* IN_MSG */
    size_t maxrec = it->n > RW_HEAD + RW_APPEND_HEAD ? (it->n - RW_HEAD - RW_APPEND_HEAD) / RW_DESC : 0;
    if (maxrec > rv->crec) {
        uint32_t c = (uint32_t)(maxrec < (1u << 24) ? maxrec : (1u << 24));
        struct repl_rec *r = realloc(rv->recs, c * sizeof *r);
        if (r)
            rv->recs = r;
        uint32_t *ln = realloc(rv->lens, c * sizeof *ln);
        if (ln)
            rv->lens = ln;
        uint64_t *co = realloc(rv->coff, c * sizeof *co);
        if (co)
            rv->coff = co;
        if (r && ln && co)
            rv->crec = c;
    }
    if (rw_decode_msg(it->data, it->n, &rv->cur, rv->recs, rv->lens, rv->crec) != 0 || rv->cur.volume != rv->number ||
        rv->cur.m.from != from || rv->cur.m.to != rv->self)
        return; /* not a message of this volume from that peer: dropped */
    uint64_t o = 0;
    for (uint32_t i = 0; i < rv->cur.m.nrec; i++)
        rv->coff[i] = o, o += rv->cur.lens[i];
    if (rv->cur.m.type == REPL_M_ACK) {
        struct rpeer *p = &rv->peer[from];
        p->acked = rv->cur.m.written;
        p->heard = 1;
    }
    repl_recv(rv->core, &rv->cur.m);
    drain(rv);
    rv->cur.m.nrec = 0;
    if (rv->mode == M_WRITER && rv->cur.m.type == REPL_M_ACK) {
        uint64_t f = repl_replay_floor(rv->core);
        struct eng *db = tsdb_cvol_ref(G.t, rv->number);
        if (db)
            eng_leader_replay_floor(db, f);
        tsdb_cvol_unref(&db);
    }
}

/* A replica's deferred SYNC (every WRITE handed in so far), and a fetch of a file a COMMIT marker still lacks. */
static void do_sync(struct rvol *rv)
{
    rv->sync_wanted = 0;
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    if (!db)
        return;
    int ab = G.mutant == RM_ACKBEFORESYNC && rv->mode == M_REPLICA; /* a follower's ACK, not a promotion's sync */
    pthread_mutex_unlock(&rv->mu);
    int rc = ab ? 0 : replica_sync(db);
    uint64_t d = replica_durable_end(db), w = replica_log_end(db);
    char name[RW_NAME_MAX + 1] = "";
    uint64_t size = 0;
    int missing = d < w && replica_missing(db, name, sizeof name, &size) == 1;
    if (ab && !missing)
        d = w; /* written, never synced, reported durable */
    tsdb_cvol_unref(&db);
    pthread_mutex_lock(&rv->mu);
    if (rc != 0) {
        vol_fail(rv, "a sync of the log failed");
        return;
    }
    repl_synced(rv->core, d);
    if (missing && (strcmp(name, rv->fetch_name) || now_ms() - rv->fetch_ms >= 1000)) {
        int ld = node_index(rv, rv->rec.leader);
        struct plink *l = ld >= 0 ? link_of(rv->rec.rep[ld].node, 0) : NULL;
        uint8_t b[RW_NAME_MAX + 16];
        size_t n = rw_encode_fetch(b, sizeof b, rv->number, name);
        if (l && n)
            plink_send(l, RW_T_FETCH, b, n, 0);
        snprintf(rv->fetch_name, sizeof rv->fetch_name, "%s", name);
        rv->fetch_ms = now_ms();
    }
}

static void *vol_main(void *arg)
{
    struct rvol *rv = arg;
    uint64_t next_tick = now_ms() + TICK_MS;
    pthread_mutex_lock(&rv->mu);
    while (!rv->stop) {
        drain(rv);
        if (rv->failed) {
            pthread_cond_wait(&rv->cv, &rv->mu);
            continue;
        }
        if (rv->in_head) {
            struct item *it = rv->in_head;
            rv->in_head = it->next;
            if (!rv->in_head)
                rv->in_tail = NULL;
            handle(rv, it);
            free(it);
            continue;
        }
        if (rv->sync_wanted) {
            do_sync(rv);
            continue;
        }
        uint64_t now = now_ms();
        if (now >= next_tick) {
            repl_tick(rv->core);
            next_tick += TICK_MS;
            if (next_tick < now)
                next_tick = now + TICK_MS;
            drain(rv);
            if (rv->mode == M_WRITER || rv->mode == M_PROMOTING) {
                struct eng *db = tsdb_cvol_ref(G.t, rv->number);
                if (db)
                    catch_up(rv, db);
                tsdb_cvol_unref(&db);
            }
            continue;
        }
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        uint64_t ns = (uint64_t)dl.tv_nsec + (next_tick - now) * 1000000ull;
        dl.tv_sec += (time_t)(ns / 1000000000ull);
        dl.tv_nsec = (long)(ns % 1000000000ull);
        pthread_cond_timedwait(&rv->cv, &rv->mu, &dl);
    }
    pthread_mutex_unlock(&rv->mu);
    return NULL;
}

/* ------------------------------------------------------------------ setup */

static int links_up(const struct cluster_vol_rec *r)
{
    pthread_mutex_lock(&G.mu);
    if (!G.pl) {
        struct cluster_self cs;
        if (cluster_self(&cs) != 0 || !cs.have_node) {
            pthread_mutex_unlock(&G.mu);
            return -1;
        }
        struct pl_cfg c = {.secret = cs.secret, .secret_len = cs.secret_len, .on_link = on_link,
                           .on_frame = on_frame, .bulk = {.open = bin_open, .data = bin_data, .end = bin_end}};
        c.self.id = cs.id;
        memcpy(c.self.uuid, cs.uuid, 16);
        memcpy(c.self.ident, cs.ident, 16);
        snprintf(c.advertise, sizeof c.advertise, "%s", cs.advertise);
        G.pl = peerlink_new(&c);
        G.self = cs.id;
        if (!G.pl) {
            pthread_mutex_unlock(&G.mu);
            return -1;
        }
        cluster_set_peerlink(G.pl);
    }
    for (uint32_t k = 0; k < r->nrep; k++) {
        if (r->rep[k].node == G.self || !r->rep[k].peer[0])
            continue;
        uint32_t i = 0;
        while (i < G.nn && G.nodes[i].node != r->rep[k].node)
            i++;
        if (i < G.nn) { /* dialed already: a node that came back on another peer port is dialed there */
            if (strcmp(G.nodes[i].addr, r->rep[k].peer) != 0) {
                snprintf(G.nodes[i].addr, sizeof G.nodes[i].addr, "%s", r->rep[k].peer);
                if (G.nodes[i].log)
                    plink_set_addr(G.nodes[i].log, r->rep[k].peer);
                if (G.nodes[i].bulk)
                    plink_set_addr(G.nodes[i].bulk, r->rep[k].peer);
            }
            continue;
        }
        if (G.nn == MAX_NODES)
            continue;
        G.nodes[i].node = r->rep[k].node;
        snprintf(G.nodes[i].addr, sizeof G.nodes[i].addr, "%s", r->rep[k].peer);
        G.nodes[i].log = peerlink_dial(G.pl, r->rep[k].node, r->rep[k].peer, PL_KIND_LOG);
        G.nodes[i].bulk = peerlink_dial(G.pl, r->rep[k].node, r->rep[k].peer, PL_KIND_BULK);
        G.nn++;
    }
    pthread_mutex_unlock(&G.mu);
    return 0;
}

void repld_init(tsdb *t, const char *dir)
{
    const char *mu = getenv("TSDB_REPLD_MUTANT");
    G.mutant = !mu                              ? RM_NONE
               : !strcmp(mu, "commitbyleader") ? RM_COMMITBYLEADER
               : !strcmp(mu, "ackbeforesync")  ? RM_ACKBEFORESYNC
               : !strcmp(mu, "floornotsaved")  ? RM_FLOORNOTSAVED
                                               : RM_NONE;
    if (G.mutant)
        say("MUTANT %s (tests only)", mu);
    G.t = t;
    snprintf(G.dir, sizeof G.dir, "%s", dir);
}

int repld_open(const struct cluster_vol_rec *r)
{
    if (r->nrep == 0 || r->nrep > REPL_MAX_NODES || links_up(r) != 0)
        return TSDB_ERR_INVALID;
    struct rvol *rv = calloc(1, sizeof *rv);
    if (!rv)
        return TSDB_ERR_NOMEM;
    rv->number = r->number;
    rv->rec = *r;
    snprintf(rv->dir, sizeof rv->dir, "%s/v%" PRIu32, G.dir, r->number);
    rv->self = node_index(rv, G.self);
    rv->nnodes = (int)r->nrep;
    pthread_mutex_init(&rv->mu, NULL);
    pthread_cond_init(&rv->cv, NULL);
    rv->ops = (struct jr_ops){.ctx = rv, .ship = op_ship, .admit = op_admit, .bytes = op_bytes, .synced = op_synced};
    int dfd = open(rv->dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    bool torn = false;
    int rc = dfd < 0 ? TSDB_ERR_IO : replfile_load(dfd, &rv->rf, &rv->st, &torn);
    if (dfd >= 0)
        close(dfd);
    if (rc != 0 || rv->self < 0) {
        say("volume %" PRIu32 ": REPL of %s: %s", r->number, rv->dir, rc ? tsdb_strerror(rc) : "not a replica here");
        goto fail;
    }
    if (rv->st.unsure) {
        /* fence, led, floor and log start are not trusted (replfile.h): the group's epoch stands in for fence and
         * led (this replica never leads the epoch it may have led, and refuses everything older), the floor is
         * the engine's to find again from the leader's commit point, the log start from the segments */
        uint64_t e = rv->st.fence > r->epoch ? rv->st.fence : r->epoch;
        rv->st.fence = rv->st.led = e;
        rc = replfile_save(rv->rf, &rv->st);
        if (rc != 0) {
            say("volume %" PRIu32 ": REPL of %s is unsure and could not be saved: %s", r->number, rv->dir,
                tsdb_strerror(rc));
            goto fail;
        }
    }
    rc = tsdb_cvol_open(G.t, r->number, rv->dir, true, rv->st.floor);
    if (rc != 0) {
        say("volume %" PRIu32 ": the open of %s failed: %s", r->number, rv->dir, tsdb_strerror(rc));
        goto fail;
    }
    struct repl_epoch h[REPLFILE_MAX_HIST];
    uint32_t nh = 0;
    uint64_t first = 0;
    scan_segments(rv, h, &nh, &first);
    struct eng *db = tsdb_cvol_ref(G.t, r->number);
    uint64_t log_end = db ? replica_log_end(db) : 0;
    /* the history: REPL's entries below the first segment's, then the segments' own */
    for (uint32_t i = 0; i < rv->st.nhist; i++)
        if (!nh || rv->st.hist[i].start < h[0].start)
            hist_note(rv, rv->st.hist[i].epoch, rv->st.hist[i].start);
    for (uint32_t i = 0; i < nh; i++)
        hist_note(rv, h[i].epoch, h[i].start);
    rv->log_start = first ? first : log_end + 1;
    if (rv->log_start < 1)
        rv->log_start = 1;
    rv->idx_end = rv->log_start - 1;
    if (db)
        index_log(rv, db, rv->log_start, log_end);
    tsdb_cvol_unref(&db);
    struct repl_durable d = {.rid = rv->st.rid, .fence = rv->st.fence, .led = rv->st.led, .floor = rv->st.floor,
                             .log_start = rv->log_start, .log_end = log_end, .hist = rv->hist, .nhist = rv->nhist};
    uint32_t lag = r->isr_lag_ms ? r->isr_lag_ms : 1000;
    struct repl_config cfg = {.self = rv->self, .nnodes = rv->nnodes, .min_isr = r->min_isr ? (int)r->min_isr : 1,
                              .isr_lag_ticks = lag / TICK_MS, .resend_ticks = 200 / TICK_MS, .heartbeat_ticks = 1,
                              .floor_ticks = 1000 / TICK_MS, .max_batch = 1u << 30, .window = 1u << 30,
                              .rec_find = idx_find, .ctx = rv};
    rv->core = repl_open(&cfg, &d);
    if (!rv->core) {
        say("volume %" PRIu32 ": the replication core refused the state of %s", r->number, rv->dir);
        goto fail;
    }
    rv->epoch = repl_epoch(rv->core);
    /* what the line below logs, read before the thread starts: from then on rv->st and log_start are its (under mu) */
    uint64_t said_start = rv->log_start, said_floor = rv->st.floor, said_fence = rv->st.fence;
    pthread_mutex_lock(&G.mu);
    int room = G.nvols < CLUSTER_REPORT_VOLS;
    if (room)
        G.vols[G.nvols++] = rv;
    pthread_mutex_unlock(&G.mu);
    if (!room || pthread_create(&rv->th, NULL, vol_main, rv) != 0) {
        say("volume %" PRIu32 ": no driver thread", r->number);
        return TSDB_ERR_NOMEM; /* listed but not started: its frames queue and are freed at stop */
    }
    rv->started = 1;
    post(rv, IN_META, 0, NULL, 0);
    say("volume %" PRIu32 ": replica %d of %d, log %" PRIu64 "..%" PRIu64 ", floor %" PRIu64 ", fence %" PRIu64,
        r->number, rv->self, rv->nnodes, said_start, log_end, said_floor, said_fence);
    return 0;
fail:
    if (rv->rf)
        replfile_close(rv->rf);
    replfile_state_free(&rv->st);
    free(rv);
    return rc ? rc : TSDB_ERR_INVALID;
}

void repld_update(const struct cluster_vol_rec *r)
{
    struct rvol *rv = vol_find(r->number);
    if (!rv)
        return;
    links_up(r);
    pthread_mutex_lock(&rv->mu);
    int changed = r->epoch != rv->rec.epoch || r->isr_version != rv->rec.isr_version || r->leader != rv->rec.leader;
    if (r->nrep == rv->rec.nrep)
        rv->rec = *r; /* the replica set is fixed for a volume's driver (a move: r3e and later) */
    pthread_mutex_unlock(&rv->mu);
    if (changed)
        post(rv, IN_META, 0, NULL, 0);
}

int repld_failed(uint32_t v)
{
    struct rvol *rv = vol_find(v);
    if (!rv)
        return 0;
    pthread_mutex_lock(&rv->mu);
    int f = rv->failed;
    pthread_mutex_unlock(&rv->mu);
    return f;
}

int repld_engine_failed(uint32_t v, const char *msg)
{
    struct rvol *rv = vol_find(v);
    if (!rv)
        return 0;
    pthread_mutex_lock(&rv->mu);
    int first = !rv->failed;
    vol_fail(rv, msg && msg[0] ? msg : "the engine failed"); /* deposes a writer before the quiesce hides its engine */
    pthread_mutex_unlock(&rv->mu);
    if (first)
        tsdb_volume_quiesce(G.t, v); /* clients get UNAVAILABLE and go to the volume's next leader */
    return 1;
}

void repld_stop(void)
{
    pthread_mutex_lock(&G.mu);
    uint32_t n = G.nvols;
    pthread_mutex_unlock(&G.mu);
    for (uint32_t i = 0; i < n; i++) {
        struct rvol *rv = G.vols[i];
        pthread_mutex_lock(&rv->mu);
        rv->stop = 1;
        pthread_cond_signal(&rv->cv);
        pthread_mutex_unlock(&rv->mu);
        if (rv->started)
            pthread_join(rv->th, NULL);
        rv->started = 0;
        struct eng *db = tsdb_cvol_ref(G.t, rv->number);
        if (db && rv->mode == M_WRITER)
            eng_leader_depose(db); /* nothing waits for a commit point that no longer moves */
        tsdb_cvol_unref(&db);
    }
    /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket to the peerlink any more */
    if (G.pl) {
        cluster_set_peerlink(NULL);
        peerlink_free(G.pl);
        G.pl = NULL;
    }
}
