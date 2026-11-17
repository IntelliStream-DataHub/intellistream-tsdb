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
 * `floornotsaved` skips the REPL file's SAVE (test_cluster replicate, replindet); `commitpastremoved` takes an ISR
 * request as the record at once, before the metadata group commits it (isr), `nominisr` never checks min_isr (minisr),
 * `ackbelowminisr` hands the writer a commit point reached after a shrink below min_isr (minisr), `nocap` keeps the
 * leader's segments for a lagging replica without the caps repl_retain_mb and repl_retain_s (keepcap), `noseal` leaves
 * an idle leader's last segment unsealed (no idle flush; the NOW markers go on) (idleseal). Faults for tests, TSDB_REPLD_TEST=NAME[,NAME]: `dropbulk` has the first
 * bulk file this process receives taken as staged but not kept, so its COMMIT marker finds the file missing and
 * FETCHes it; `promotewait` waits 1 s between the promotion's engine swap and the mode M_WRITER (writes then reach
 * the new writer while the driver still promotes: they must be refused for a retry, never depose it).
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
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
#define SPILL_BYTES (16u << 20) /* a bulk file above this is received into a file, not memory (r3d review M3) */
#define INBOX_BYTES (64u << 20) /* a volume's inbox of peer frames: past it the link waits, then drops (M3) */
#define INBOX_WAIT_MS 500       /* how long a link's thread waits for room (well below a link's dead_ms) */
#define IDLE_HEARTBEAT_MS 100 /* an idle volume's empty APPEND per peer (r3d review L1: was every tick) */
#define NOW_EVERY_MS 1000 /* a leader's NOW markers (design-cluster.md 5.4): its replicas' retention clock lags by at most this */
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
    uint64_t heard_ms; /* when a message of this volume last came from it (repld_unreachable) */
};

struct rvol {
    uint32_t number;
    char dir[4200];
    pthread_mutex_t mu; /* the core, the index, the cursors, the inbox; below buf_lock (jr_ops.admit takes it) */
    pthread_cond_t cv;
    pthread_t th;
    int started, stop, mode, failed, set_changed;
    struct repl *core;
    struct replfile *rf;
    struct replfile_state st;
    struct cluster_vol_rec rec;
    int self, nnodes;
    uint64_t epoch; /* the core's, cached for the bytes path */
    struct rpeer peer[REPL_MAX_NODES];
    uint64_t open_ms; /* when its driver started: a peer never heard counts as unheard since then */
    struct item *in_head, *in_tail;
    size_t in_bytes;         /* of peer frames in the inbox (bounded: inbox_bytes) */
    pthread_cond_t in_space; /* a link's thread waits here for room */
    int in_waiters, in_full; /* in_full: the bound was hit and said, until the inbox empties */
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
    /* the ISR change the core asked the metadata group for (REPL_M_ISR_REQ), reported in this server's heartbeats
     * (repld_isr_requests) while the record still is the request's basis */
    int isr_want;
    struct repl_vmeta isr_req;
    uint64_t resigned; /* the epoch a RESIGN was said for */
    /* min_isr after the append: a write admitted while the ISR was large enough, whose commit point is reached only
     * after a shrink below min_isr, is not acknowledged (the core's REPLY waits the same way, but the writer's waiters
     * go by APPLY): the commit point is held here and handed to the engine once the ISR is back at min_isr */
    uint64_t held_commit;
    int short_said;
    int snap_said; /* the SNAPSHOT notice was said */
};

enum { RM_NONE, RM_COMMITBYLEADER, RM_ACKBEFORESYNC, RM_FLOORNOTSAVED, RM_COMMITPASTREMOVED, RM_NOMINISR,
       RM_ACKBELOWMINISR, RM_SHIPPERFILE, RM_NOCAP, RM_NOSEAL, RM_NOSPILL, RM_NOINBOXBOUND,
       RM_KEEPIGNORESPEERS, RM_NOLOGSTARTCHECK, RM_STOPNODEPOSE, RM_LATECLOCK };

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
    uint64_t spill_bytes; /* a bulk file larger than this is received into a spill file (TSDB_REPLD_TEST=spill: 64 KiB) */
    int spill_said;
    size_t inbox_bytes; /* INBOX_BYTES (TSDB_REPLD_TEST=smallinbox: 256 KiB) */
    int stall_inbox;    /* TSDB_REPLD_TEST=stallinbox: no peer frame is handled while <dir>/stallinbox exists (a
                           stalled disk's stand-in) */
    int drop_bulk; /* TSDB_REPLD_TEST=dropbulk (a fault, tests only): the first bulk file received is not staged */
    int promote_wait; /* TSDB_REPLD_TEST=promotewait (tests only): 1 s between the promotion's swap and M_WRITER */
    int late_wal; /* TSDB_REPLD_TEST=latewal (tests only): repld_stop leaves its writers in place and waits 2.5 s after
                   * freeing the links, and a stopped driver admits records whatever its lease, so the engine's NOW
                   * markers reach the WAL hooks of a stopped driver (op_bytes says so once) */
    _Atomic int said_late; /* latewal's line, once */
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

static void vol_fail(struct rvol *rv, const char *why);
static void cvol_close(uint32_t v);

/* A message of the core to the metadata group. ISR_REQ is kept for this server's heartbeats (repld_isr_requests: the
 * Raft leader proposes ISR_CHANGE on the request's own basis) until the record moves; RESIGN and FENCE_REPLY belong to
 * promotion (r3e-failover): said, not sent. */
static void meta_msg(struct rvol *rv, const struct repl_msg *m)
{
    if (m->type == REPL_M_ISR_REQ) {
        if (!rv->isr_want || memcmp(&rv->isr_req, &m->meta, sizeof m->meta))
            say("volume %" PRIu32 ": asks for an ISR of %d members (ISR version %" PRIu64 ")", rv->number,
                __builtin_popcount(m->meta.isr), m->meta.isr_version);
        rv->isr_req = m->meta;
        rv->isr_want = 1;
        cluster_beat_soon();
        if (G.mutant == RM_COMMITPASTREMOVED) { /* planted: the core is told the change committed when it asks */
            struct repl_msg mm = {.type = REPL_M_META, .from = REPL_META, .to = rv->self, .leader = -1};
            mm.meta = m->meta;
            mm.meta.isr_version++;
            mm.epoch = mm.meta.epoch;
            repl_recv(rv->core, &mm);
        }
    } else if (m->type == REPL_M_RESIGN && rv->resigned != m->epoch) {
        rv->resigned = m->epoch;
        say("volume %" PRIu32 ": cannot lead epoch %" PRIu64 " (led it before a restart); waits for a promotion",
            rv->number, m->epoch);
    }
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
    if (cap > PL_FRAME_MAX) {
        if (nrec == 1) /* never sent, so never committed: a backstop for jr_rec_max_rows (src/internal.h) */
            vol_fail(rv, "a WAL record larger than a peer frame");
        return -1;
    }
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
    /* Stopped (repld_stop): the links are freed or about to be. The engine may still write: a record admitted before
     * the depose, a NOW marker of the flusher; its bytes stay in the WAL for the next epoch. */
    if (G.late_wal && rv->stop && !atomic_exchange(&G.said_late, 1)) {
        int at = 0;
        for (int i = 0; i < (int)rv->rec.nrep; i++)
            at += i != rv->self && rv->peer[i].sent + 1 == first;
        say("test: volume %" PRIu32 " writes WAL records %" PRIu64 "..%" PRIu64 " after its driver stopped (%d peers"
            " were at them)", rv->number, first, last, at);
    }
    for (int i = 0; !rv->stop && i < (int)rv->rec.nrep; i++) {
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
    /* M_PROMOTING: the new writer engine is listed (tsdb_cvol_replace) before the driver's mode says so; a write in
     * that window is refused for a retry, never taken as a sign that this node no longer leads (that deposed the
     * fresh leader for good) */
    if (rv->mode == M_PROMOTING) {
        pthread_mutex_unlock(&rv->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    /* no read lease (§7.1): a write that raced the check in tsdbd is refused the same way, never taken as a deposal */
    if (rv->mode == M_WRITER && !cluster_lease_valid() && !(G.late_wal && rv->stop)) {
        pthread_mutex_unlock(&rv->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
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

/* The jobs of one file to its peers. `detached`: nobody waits; the last `done` closes fd and frees it. `pend[i]`: the
 * jobs not done to the replica at record position i. */
struct shipwait {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int pending, failed, detached, fd;
    int pend[META_REPLICAS_MAX];
};

/* a job's `arg`: its file and the peer it goes to (freed by ship_done) */
struct shipjob {
    struct shipwait *w;
    int peer;
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
    struct shipjob *j = arg;
    struct shipwait *w = j->w;
    pthread_mutex_lock(&w->mu);
    int last = --w->pending == 0 && w->detached;
    w->pend[j->peer]--;
    if (status != PL_OK)
        w->failed++;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
    free(j);
    if (last)
        ship_free(w);
}

/* Sends the file (fd, size) under marker name `mname` to `to` (a node index), or to every other replica (-1); w
 * counts the jobs (a peer that did not stage it fetches it later, replica_missing). */
static void bulk_out(struct rvol *rv, int fd, uint64_t size, const char *mname, int to, struct shipwait *w)
{
    uint8_t meta[PL_META_MAX];
    size_t ml = rw_encode_bulk_meta(meta, sizeof meta, rv->number, size, mname);
    if (!ml)
        return;
    for (int i = 0; i < (int)rv->rec.nrep && i < META_REPLICAS_MAX; i++) {
        if (i == rv->self || (to >= 0 && i != to))
            continue;
        struct plink *l = link_of(rv->rec.rep[i].node, 1);
        struct shipjob *sj = l ? malloc(sizeof *sj) : NULL;
        if (!sj)
            continue;
        *sj = (struct shipjob){w, i};
        pthread_mutex_lock(&G.mu);
        uint64_t file = ++G.fileno;
        pthread_mutex_unlock(&G.mu);
        struct pl_job j = {.file = file, .fd = fd, .base = 0, .length = size, .meta = meta, .meta_len = ml,
                           .max_attempts = 3, .done = ship_done, .arg = sj};
        pthread_mutex_lock(&w->mu);
        w->pending++;
        w->pend[i]++;
        pthread_mutex_unlock(&w->mu);
        if (plink_bulk_send(l, &j) != PL_OK) {
            pthread_mutex_lock(&w->mu);
            w->pending--;
            w->pend[i]--;
            pthread_mutex_unlock(&w->mu);
            free(sj);
        }
    }
}

/* One COMMIT's files (M2 of the r3d review). op_ship queues a file to every replica and returns; op_ship_wait, after
 * the COMMIT's last file, waits for all of them under one deadline: SHIP_BASE_MS (or twice isr_lag_ms, if more) plus
 * the bytes at SHIP_MIN_BPS, from the first file's send. It also stops waiting for a replica the ISR no longer holds.
 * Past the deadline the marker goes out anyway: the transfers go on (the last done closes the file), a replica that
 * has not staged a file fetches it when it applies the marker (replica_missing), and one that cannot keep up lags
 * behind the marker and leaves the ISR by the core's rule (isr_lag). Waiting 30 s per file, as before, held jr_serial
 * and the large write's fence N x 30 s for a stopped replica. The batch is per thread: the engine calls ship and
 * ship_wait on the writer's thread, one COMMIT at a time. */
enum { SHIP_BASE_MS = 3000 };
#define SHIP_MIN_BPS (32ull << 20)

static __thread struct shipbatch {
    struct rvol *rv;
    struct shipwait **w;
    size_t n, cap;
    uint64_t bytes, t0;
} tl_ship;

/* holds rv->mu: whether the replica at record position i is in the ISR the record names */
static int in_isr_locked(const struct rvol *rv, int i)
{
    for (uint32_t k = 0; k < rv->rec.nisr && k < META_REPLICAS_MAX; k++)
        if (rv->rec.isr[k] == rv->rec.rep[i].node)
            return 1;
    return 0;
}

/* Waits for w's jobs to the replicas still in the ISR until `deadline` (now_ms); 1 when they are all done. */
static int ship_wait_one(struct rvol *rv, struct shipwait *w, uint64_t deadline)
{
    for (;;) {
        uint32_t isr = 0;
        pthread_mutex_lock(&rv->mu);
        for (int i = 0; i < (int)rv->rec.nrep && i < META_REPLICAS_MAX; i++)
            if (in_isr_locked(rv, i))
                isr |= 1u << i;
        pthread_mutex_unlock(&rv->mu);
        pthread_mutex_lock(&w->mu);
        int left = 0;
        for (int i = 0; i < META_REPLICAS_MAX; i++)
            if (w->pend[i] > 0 && (isr & (1u << i)))
                left++;
        uint64_t now = now_ms();
        if (!left || now >= deadline) {
            pthread_mutex_unlock(&w->mu);
            return !left;
        }
        /* woken by a done; the ISR is looked at again every 100 ms */
        uint64_t slice = deadline - now < 100 ? deadline - now : 100;
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_nsec += (long)(slice * 1000000);
        dl.tv_sec += dl.tv_nsec / 1000000000;
        dl.tv_nsec %= 1000000000;
        pthread_cond_timedwait(&w->cv, &w->mu, &dl);
        pthread_mutex_unlock(&w->mu);
    }
}

static void ship_batch_end(int wait)
{
    struct shipbatch *b = &tl_ship;
    uint64_t span = 2ull * (b->rv && b->rv->rec.isr_lag_ms ? b->rv->rec.isr_lag_ms : 1000);
    uint64_t deadline = b->t0 + (span > SHIP_BASE_MS ? span : SHIP_BASE_MS) + b->bytes * 1000 / SHIP_MIN_BPS;
    int late = 0;
    for (size_t i = 0; i < b->n; i++) {
        if (wait && !ship_wait_one(b->rv, b->w[i], deadline))
            late++;
        ship_detach(b->w[i]); /* a job still queued reads fd later: the last done closes it */
    }
    if (late)
        say("volume %" PRIu32 ": %d of %zu files of a COMMIT not staged by every ISR member in %" PRIu64
            " ms: the marker goes out, a replica without one fetches it", b->rv->number, late, b->n,
            deadline - b->t0);
    free(b->w);
    *b = (struct shipbatch){0};
}

/* jr_ops.ship: the pending part file of a COMMIT, queued to every replica (on the writer's thread) */
static int op_ship(void *ctx, struct eng *db, const char *file, const char *mname, uint64_t size)
{
    struct rvol *rv = ctx;
    struct shipbatch *b = &tl_ship;
    if (b->n && b->rv != rv)
        ship_batch_end(0); /* a batch left by an engine that never called ship_wait: not waited for */
    if (b->n == b->cap) {
        size_t cap = b->cap ? 2 * b->cap : 16;
        struct shipwait **nw = realloc(b->w, cap * sizeof *nw);
        if (!nw)
            return TSDB_ERR_NOMEM;
        b->w = nw, b->cap = cap;
    }
    int fd = openat(db->dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    struct shipwait *w = ship_new(fd);
    if (!w) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    if (!b->n)
        b->rv = rv, b->t0 = now_ms(), b->bytes = 0;
    b->w[b->n++] = w;
    b->bytes += size;
    pthread_mutex_lock(&rv->mu);
    if (!rv->stop) /* no links any more (op_bytes): no job, so the wait for this file ends at once */
        bulk_out(rv, fd, size, mname, -1, w);
    pthread_mutex_unlock(&rv->mu);
    if (G.mutant == RM_SHIPPERFILE) { /* planted: the code before the review's M2, 30 s for each file */
        pthread_mutex_lock(&w->mu);
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_sec += 30;
        while (w->pending > 0 && pthread_cond_timedwait(&w->cv, &w->mu, &dl) != ETIMEDOUT)
            ;
        pthread_mutex_unlock(&w->mu);
    }
    return 0;
}

/* jr_ops.ship_wait: after a COMMIT's last ship (and after a failed one): the one wait of the batch */
static int op_ship_wait(void *ctx, struct eng *db, int failed)
{
    (void)db;
    if (tl_ship.n && tl_ship.rv == ctx)
        ship_batch_end(!failed);
    else if (tl_ship.n)
        ship_batch_end(0);
    return 0;
}

struct rin {
    struct rin *next;
    struct plink *l;
    uint64_t file, total;
    uint32_t volume;
    char name[RW_NAME_MAX + 1];
    uint8_t *buf; /* the file in memory, or NULL: it goes to the spill file `tmp` (fd) of the volume's directory */
    int fd;
    char tmp[48];
};

/* frees a receive and removes its spill file (a transfer that did not end in a stage) */
static void rin_free(struct rin *r)
{
    if (!r)
        return;
    if (r->fd >= 0)
        close(r->fd);
    if (r->tmp[0]) {
        struct eng *db = tsdb_cvol_ref(G.t, r->volume);
        if (db)
            replica_spill_drop(db, r->tmp);
        tsdb_cvol_unref(&db);
    }
    free(r->buf);
    free(r);
}

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
    if (!r)
        return -1;
    r->fd = -1;
    if (rw_decode_bulk_meta(meta, meta_len, &r->volume, &size, r->name, sizeof r->name) != 0 || size != total ||
        total > (1ull << 32) || !vol_find(r->volume)) {
        free(r);
        return -1;
    }
    if (total > G.spill_bytes) {
        /* a large file goes to disk as it arrives, never whole into memory (r3d review M3) */
        struct eng *db = tsdb_cvol_ref(G.t, r->volume);
        r->fd = db ? replica_spill_open(db, r->tmp, sizeof r->tmp) : -1;
        tsdb_cvol_unref(&db);
        if (r->fd < 0) {
            r->tmp[0] = 0;
            free(r);
            return -1;
        }
        if (!G.spill_said) {
            G.spill_said = 1;
            say("volume %" PRIu32 ": receives %s (%" PRIu64 " bytes) into %s, not into memory", r->volume, r->name,
                total, r->tmp);
        }
    } else if (!(r->buf = malloc(total ? total : 1))) {
        free(r);
        return -1;
    }
    r->l = l, r->file = file, r->total = total;
    *resume = 0;
    *prefix_fd = -1;
    pthread_mutex_lock(&G.mu);
    struct rin *old = rin_find(l, file, 1); /* the same file begun again (a reconnect): from 0 */
    r->next = G.ins;
    G.ins = r;
    pthread_mutex_unlock(&G.mu);
    rin_free(old);
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
    if (r->buf)
        memcpy(r->buf + off, p, n);
    else
        for (size_t done = 0; done < n;) {
            ssize_t w = pwrite(r->fd, (const uint8_t *)p + done, n - done, (off_t)(off + done));
            if (w < 0 && errno == EINTR)
                continue;
            if (w <= 0)
                return -1; /* the sender stops; bin_end removes the file */
            done += (size_t)w;
        }
    if (off + n < r->total)
        return 0;
    /* the last bytes: staged (file and directory synced) before the sender hears DONE */
    struct rvol *rv = vol_find(r->volume);
    if (!rv)
        return -1;
    pthread_mutex_lock(&G.mu);
    int drop = G.drop_bulk > 0 ? G.drop_bulk-- : 0;
    pthread_mutex_unlock(&G.mu);
    if (drop) {
        say("TEST: volume %" PRIu32 ": %s received and not staged (dropbulk)", r->volume, r->name);
        return 0;
    }
    struct eng *db = tsdb_cvol_ref(G.t, r->volume);
    int rc = TSDB_ERR_UNAVAILABLE;
    if (db && r->buf)
        rc = replica_stage(db, r->name, r->buf, r->total);
    else if (db) {
        rc = replica_stage_spilled(db, r->name, r->fd, r->tmp, r->total);
        r->tmp[0] = 0; /* renamed, or removed by the failed stage */
    }
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
    rin_free(r);
}

/* ------------------------------------------------------------------ the inbox */

static int peer_kind(int kind) { return kind == IN_MSG || kind == IN_FETCH; }

/* the volume's thread took `it` off the inbox */
static void inbox_took(struct rvol *rv, const struct item *it)
{
    if (!peer_kind(it->kind))
        return;
    rv->in_bytes -= sizeof *it + it->n;
    if (rv->in_waiters)
        pthread_cond_broadcast(&rv->in_space);
    if (!rv->in_bytes && rv->in_full) {
        rv->in_full = 0;
        say("volume %" PRIu32 ": the inbox is empty again", rv->number);
    }
    if (G.stall_inbox) {
        char f[sizeof G.dir + 16];
        snprintf(f, sizeof f, "%s/stallinbox", G.dir);
        pthread_mutex_unlock(&rv->mu);
        for (struct timespec w = {0, 10000000}; access(f, F_OK) == 0;)
            nanosleep(&w, NULL);
        pthread_mutex_lock(&rv->mu);
    }
}

/* Room in the inbox for a peer frame of `need` bytes (rv->mu held), else 0: the link's thread waits a little (which
 * stops reading the link: TCP pushes back on the sender), then the frame is dropped and the core sends it again. The
 * inbox is never larger than the bound plus one frame (a frame alone always gets in), and local items are not counted
 * (they are small and must not be lost). A failed volume reads nothing: its frames are dropped at once. */
static int inbox_room(struct rvol *rv, size_t need)
{
    if (rv->in_bytes + need <= G.inbox_bytes || !rv->in_bytes)
        return 1;
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_nsec += INBOX_WAIT_MS * 1000000L;
    dl.tv_sec += dl.tv_nsec / 1000000000L;
    dl.tv_nsec %= 1000000000L;
    while (!rv->stop && !rv->failed && rv->in_bytes && rv->in_bytes + need > G.inbox_bytes) {
        rv->in_waiters++;
        int rc = pthread_cond_timedwait(&rv->in_space, &rv->mu, &dl);
        rv->in_waiters--;
        if (rc == ETIMEDOUT)
            break;
    }
    if (rv->in_bytes + need <= G.inbox_bytes || !rv->in_bytes)
        return 1;
    if (!rv->in_full) {
        rv->in_full = 1;
        say("volume %" PRIu32 ": the inbox holds %zu bytes of peer frames, its bound: frames are dropped (sent again)",
            rv->number, rv->in_bytes);
    }
    return 0;
}

static void post(struct rvol *rv, int kind, uint32_t node, const void *p, size_t n)
{
    struct item *it = malloc(sizeof *it + n);
    if (!it)
        return; /* a lost message: the core sends again */
    it->next = NULL, it->kind = kind, it->node = node, it->n = n;
    if (n)
        memcpy(it->data, p, n);
    pthread_mutex_lock(&rv->mu);
    if (peer_kind(kind)) {
        if (!inbox_room(rv, sizeof *it + n)) {
            pthread_mutex_unlock(&rv->mu);
            free(it);
            return;
        }
        rv->in_bytes += sizeof *it + n;
    }
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

static int isr_short_locked(const struct rvol *rv);

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
            wal_rec wr;
            /* the core's position is the descriptor's; the engine's is the record's own header: they must agree */
            if (wal_record(rv->cur.bytes + rv->coff[i], rv->cur.lens[i], rv->cur.seq, rv->cur.off + rv->coff[i],
                           &wr) != rv->cur.lens[i] ||
                wr.fno != a->rec.pos || wr.span != a->rec.n) {
                vol_fail(rv, "a record's header does not match its descriptor");
                break;
            }
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
                if (i != rv->self && rv->peer[i].acked < keep && G.mutant != RM_KEEPIGNORESPEERS)
                    keep = rv->peer[i].acked; /* a peer never heard from holds everything */
            /* the caps (design-cluster.md 8.3): past them a lagging peer's segments go, up to the floor, and the core
             * tells that peer to install a snapshot once the log start passed what it needs */
            uint64_t capb = G.mutant == RM_NOCAP ? 0 : (uint64_t)rv->rec.repl_retain_mb << 20;
            uint64_t capms = G.mutant == RM_NOCAP ? 0 : (uint64_t)rv->rec.repl_retain_s * 1000;
            uint64_t floor = a->floor;
            pthread_mutex_unlock(&rv->mu);
            eng_leader_keep_cap(db, capb, capms);
            eng_leader_clock(db, NOW_EVERY_MS, G.mutant == RM_NOSEAL ? 0 : rv->rec.idle_seal_ms);
            eng_leader_keep_floor(db, keep, floor);
            pthread_mutex_lock(&rv->mu);
        } else if (a->floor) {
            pthread_mutex_unlock(&rv->mu);
            rc = replica_floor_saved(db, a->floor);
            pthread_mutex_lock(&rv->mu);
        }
        if (G.mutant != RM_NOLOGSTARTCHECK) /* planted: the core never hears that the log start moved */
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
        } else if (a->msg.to == REPL_META) {
            meta_msg(rv, &a->msg);
        } else {
            send_msg(rv, &a->msg);
        }
        break;
    case REPL_A_APPLY:
        db = tsdb_cvol_ref(G.t, rv->number);
        if (!db)
            break;
        if (rv->mode == M_WRITER) {
            if (isr_short_locked(rv) && G.mutant != RM_ACKBELOWMINISR) {
                if (a->pos > rv->held_commit)
                    rv->held_commit = a->pos; /* handed over by isr_meta once the ISR is back at min_isr */
                break;
            }
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
            if (G.promote_wait) /* a fault for tests: writes reach the new writer before its mode is M_WRITER */
                nanosleep(&(struct timespec){1, 0}, NULL);
            pthread_mutex_lock(&rv->mu);
            if (rc != 0) {
                vol_fail(rv, "the promotion to the writer failed");
                break;
            }
            rv->mode = M_WRITER;
            rv->synced_rep = a->pos;
            rv->held_commit = 0;
            say("volume %" PRIu32 ": leads epoch %" PRIu64 " from position %" PRIu64, rv->number, rv->epoch,
                rv->epoch_pos);
            /* the clock from the start (design-cluster.md 5.4), not from the first SAVE as a writer, which waits for
             * applied records: an idle new leader sent no NOW markers at all. buf_lock is above rv->mu. */
            if (G.mutant != RM_LATECLOCK && (db = tsdb_cvol_ref(G.t, rv->number))) {
                pthread_mutex_unlock(&rv->mu);
                eng_leader_clock(db, NOW_EVERY_MS, G.mutant == RM_NOSEAL ? 0 : rv->rec.idle_seal_ms);
                pthread_mutex_lock(&rv->mu);
            }
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
        if (!rv->snap_said) /* the core repeats the notice until the replica catches up: said once per open */
            say("volume %" PRIu32 ": the leader no longer has the records this replica needs: it needs a snapshot (not"
                " built yet); it stays a learner", rv->number);
        rv->snap_said = 1;
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

/* The ISR is below min_isr (rv->mu held): the record's view, which the core gets next (META), so a write the core
 * still admitted under the larger ISR is held, never acknowledged, while the record says too few */
static int isr_short_locked(const struct rvol *rv)
{
    return G.mutant != RM_NOMINISR && rv->rec.min_isr > 1 && rv->rec.nisr < rv->rec.min_isr;
}

/* After a META on the writer (rv->mu held): says when the ISR fell below min_isr and when it is back, and hands a
 * held commit point to the engine once it is (every member of the larger ISR has the held positions: the core grows
 * the ISR only by a member at the commit point). */
static void isr_meta(struct rvol *rv)
{
    if (rv->mode != M_WRITER || rv->failed)
        return;
    if (isr_short_locked(rv)) {
        if (!rv->short_said)
            say("volume %" PRIu32 ": an ISR of %" PRIu32 " members, below min_isr %" PRIu32
                ": writes are refused until it grows", rv->number, rv->rec.nisr, rv->rec.min_isr);
        rv->short_said = 1;
        return;
    }
    if (rv->short_said)
        say("volume %" PRIu32 ": the ISR is back at %" PRIu32 " members (min_isr %" PRIu32 "): writes are taken",
            rv->number, rv->rec.nisr, rv->rec.min_isr);
    rv->short_said = 0;
    if (rv->held_commit) {
        struct eng *db = tsdb_cvol_ref(G.t, rv->number);
        if (db)
            eng_leader_commit(db, rv->held_commit);
        tsdb_cvol_unref(&db);
        rv->held_commit = 0;
    }
}

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
        isr_meta(rv);
        return;
    }
    int from = node_index(rv, it->node);
    if (from < 0 || from == rv->self)
        return;
    if (it->kind == IN_FETCH) {
        char name[RW_NAME_MAX + 1];
        uint32_t vol;
        uint64_t mpos;
        if (rw_decode_fetch(it->data, it->n, &vol, &mpos, name, sizeof name) != 0 || rv->mode != M_WRITER)
            return;
        struct eng *db = tsdb_cvol_ref(G.t, rv->number);
        int fd = db ? eng_leader_file_open(db, mpos, name) : TSDB_ERR_UNAVAILABLE;
        tsdb_cvol_unref(&db);
        struct stat sb;
        struct shipwait *w = fd >= 0 && fstat(fd, &sb) == 0 ? ship_new(fd) : NULL;
        if (w) {
            bulk_out(rv, fd, (uint64_t)sb.st_size, name, from, w);
            ship_detach(w);
        } else if (fd >= 0) {
            close(fd);
        } else if (fd == TSDB_ERR_NOTFOUND) {
            say("volume %" PRIu32 ": node %" PRIu32 " asks for %s of the marker at %" PRIu64
                ", which no kept segment names: it needs a snapshot", rv->number, it->node, name, mpos);
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
    rv->peer[from].heard_ms = now_ms();
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
    uint64_t size = 0, mpos = 0;
    int missing = d < w && replica_missing(db, &mpos, name, sizeof name, &size) == 1;
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
        uint8_t b[RW_NAME_MAX + 24];
        size_t n = rw_encode_fetch(b, sizeof b, rv->number, mpos, name);
        if (l && n && strcmp(name, rv->fetch_name) != 0)
            say("volume %" PRIu32 ": fetches %s (the marker at %" PRIu64 ") from node %" PRIu32, rv->number, name, mpos,
                rv->rec.leader);
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
        /* a tick that is due goes before the inbox (r3d review L2): under a steady stream of ACKs the inbox is never
         * empty, and the ticks (catch-up of a lagging peer, resends, the ISR check) would wait for it */
        uint64_t now = now_ms();
        if (rv->in_head && now < next_tick) {
            struct item *it = rv->in_head;
            rv->in_head = it->next;
            if (!rv->in_head)
                rv->in_tail = NULL;
            inbox_took(rv, it);
            handle(rv, it);
            free(it);
            continue;
        }
        if (rv->sync_wanted && now < next_tick) {
            do_sync(rv);
            continue;
        }
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
               : !strcmp(mu, "commitpastremoved") ? RM_COMMITPASTREMOVED
               : !strcmp(mu, "nominisr")       ? RM_NOMINISR
               : !strcmp(mu, "ackbelowminisr") ? RM_ACKBELOWMINISR
               : !strcmp(mu, "shipperfile")    ? RM_SHIPPERFILE
               : !strcmp(mu, "nocap")          ? RM_NOCAP
               : !strcmp(mu, "noseal")         ? RM_NOSEAL
               : !strcmp(mu, "nospill")        ? RM_NOSPILL
               : !strcmp(mu, "noinboxbound")   ? RM_NOINBOXBOUND
               : !strcmp(mu, "keepignorespeers") ? RM_KEEPIGNORESPEERS
               : !strcmp(mu, "nologstartcheck") ? RM_NOLOGSTARTCHECK
               : !strcmp(mu, "stopnodepose")   ? RM_STOPNODEPOSE
               : !strcmp(mu, "lateclock")      ? RM_LATECLOCK
                                               : RM_NONE;
    if (G.mutant)
        say("MUTANT %s (tests only)", mu);
    const char *te = getenv("TSDB_REPLD_TEST");
    G.drop_bulk = te && strstr(te, "dropbulk");
    G.promote_wait = te && strstr(te, "promotewait");
    G.late_wal = te && strstr(te, "latewal");
    G.inbox_bytes = te && strstr(te, "smallinbox") ? 256u << 10 : INBOX_BYTES;
    if (G.mutant == RM_NOINBOXBOUND)
        G.inbox_bytes = SIZE_MAX;
    G.stall_inbox = te && strstr(te, "stallinbox");
    G.spill_bytes = te && strstr(te, "spill") ? 64u << 10 : SPILL_BYTES;
    if (G.mutant == RM_NOSPILL)
        G.spill_bytes = UINT64_MAX;
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
    pthread_cond_init(&rv->in_space, NULL);
    rv->ops = (struct jr_ops){.ctx = rv, .ship = op_ship, .ship_wait = op_ship_wait, .admit = op_admit, .bytes = op_bytes, .synced = op_synced};
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
    pthread_mutex_lock(&G.mu);
    int room = G.nvols < CLUSTER_REPORT_VOLS;
    pthread_mutex_unlock(&G.mu);
    if (!room) { /* before the open: an engine without a driver would stay open, and every retry be refused */
        say("volume %" PRIu32 ": not opened: this node drives %d volumes already", r->number, CLUSTER_REPORT_VOLS);
        rc = TSDB_ERR_NOMEM;
        goto fail;
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
    int min_isr = r->min_isr && G.mutant != RM_NOMINISR ? (int)r->min_isr : 1; /* planted nominisr: never checked */
    struct repl_config cfg = {.self = rv->self, .nnodes = rv->nnodes, .min_isr = min_isr,
                              .isr_lag_ticks = lag / TICK_MS, .resend_ticks = 200 / TICK_MS, .heartbeat_ticks = 1,
                              .idle_heartbeat_ticks = IDLE_HEARTBEAT_MS / TICK_MS,
                              .floor_ticks = 1000 / TICK_MS, .max_batch = 1u << 30, .window = 1u << 30,
                              .rec_find = idx_find, .ctx = rv};
    rv->core = repl_open(&cfg, &d);
    if (!rv->core) {
        say("volume %" PRIu32 ": the replication core refused the state of %s", r->number, rv->dir);
        cvol_close(r->number);
        goto fail;
    }
    rv->epoch = repl_epoch(rv->core);
    /* what the line below logs, read before the thread starts: from then on rv->st and log_start are its (under mu) */
    uint64_t said_start = rv->log_start, said_floor = rv->st.floor, said_fence = rv->st.fence;
    rv->open_ms = now_ms(); /* before it is listed: repld_unreachable reads it from the list */
    pthread_mutex_lock(&G.mu);
    room = G.nvols < CLUSTER_REPORT_VOLS; /* checked before the open; repld_open runs on the manager thread only */
    if (room)
        G.vols[G.nvols++] = rv;
    pthread_mutex_unlock(&G.mu);
    if (!room || pthread_create(&rv->th, NULL, vol_main, rv) != 0) {
        say("volume %" PRIu32 ": no driver thread", r->number);
        if (room) {
            pthread_mutex_lock(&rv->mu);
            vol_fail(rv, "no driver thread"); /* listed but not started: reported failed, closed at stop */
            pthread_mutex_unlock(&rv->mu);
            tsdb_volume_quiesce(G.t, r->number);
        } else {
            cvol_close(r->number);
            repl_free(rv->core);
            goto fail;
        }
        return TSDB_ERR_NOMEM;
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
    int same = r->nrep == rv->rec.nrep;
    for (uint32_t i = 0; same && i < r->nrep; i++)
        same = r->rep[i].node == rv->rec.rep[i].node; /* a rid is filled in at its first open */
    if (same)
        rv->rec = *r; /* the replica set is fixed for a volume's driver (a move: r3e and later) */
    else if (!rv->set_changed) { /* a member's cursor and acks must never pass to another node: ignored, said once */
        rv->set_changed = 1;
        say("volume %" PRIu32 ": its replica set changed; this driver keeps the old one until a restart", r->number);
    }
    pthread_mutex_unlock(&rv->mu);
    if (changed)
        post(rv, IN_META, 0, NULL, 0);
}

int repld_isr_short(uint32_t v)
{
    struct rvol *rv = vol_find(v);
    if (!rv)
        return 0;
    pthread_mutex_lock(&rv->mu);
    int s = rv->mode == M_WRITER && !rv->failed && isr_short_locked(rv);
    pthread_mutex_unlock(&rv->mu);
    return s;
}

unsigned repld_isr_requests(struct cluster_isr_req *out, unsigned max)
{
    struct rvol *vs[CLUSTER_REPORT_VOLS];
    pthread_mutex_lock(&G.mu);
    uint32_t nv = G.nvols;
    memcpy(vs, G.vols, nv * sizeof *vs);
    pthread_mutex_unlock(&G.mu);
    unsigned n = 0;
    for (uint32_t i = 0; i < nv && n < max; i++) {
        struct rvol *rv = vs[i];
        pthread_mutex_lock(&rv->mu);
        const struct repl_vmeta *q = &rv->isr_req;
        if (rv->isr_want && (q->epoch != rv->rec.epoch || q->isr_version != rv->rec.isr_version))
            rv->isr_want = 0; /* the record moved: the core asks again if it still wants a change */
        if (rv->isr_want && !rv->failed && rv->core) {
            struct cluster_isr_req *o = &out[n];
            memset(o, 0, sizeof *o);
            o->volume = rv->number;
            o->epoch = q->epoch;
            o->isr_version = q->isr_version;
            o->commit = repl_commit(rv->core);
            for (uint32_t k = 0; k < rv->rec.nrep && k < META_REPLICAS_MAX; k++)
                if (q->isr & (1u << k)) {
                    o->node[o->nisr] = rv->rec.rep[k].node;
                    o->rid[o->nisr++] = q->rid[k];
                }
            n++;
        }
        pthread_mutex_unlock(&rv->mu);
    }
    return n;
}

unsigned repld_unreachable(struct cluster_unreach *out, unsigned max, uint32_t after_ms)
{
    struct rvol *vs[CLUSTER_REPORT_VOLS];
    pthread_mutex_lock(&G.mu);
    uint32_t nv = G.nvols;
    memcpy(vs, G.vols, nv * sizeof *vs);
    pthread_mutex_unlock(&G.mu);
    uint64_t now = now_ms();
    struct {
        uint32_t node;
        uint64_t last; /* the latest message from it on any volume */
    } seen[MAX_NODES];
    unsigned ns = 0;
    for (uint32_t i = 0; i < nv; i++) {
        struct rvol *rv = vs[i];
        pthread_mutex_lock(&rv->mu);
        for (uint32_t k = 0; !rv->failed && k < rv->rec.nrep && k < REPL_MAX_NODES; k++) {
            if ((int)k == rv->self || rv->rec.rep[k].learner)
                continue;
            uint64_t last = rv->peer[k].heard_ms ? rv->peer[k].heard_ms : rv->open_ms;
            unsigned j = 0;
            while (j < ns && seen[j].node != rv->rec.rep[k].node)
                j++;
            if (j == ns && ns < MAX_NODES)
                seen[ns++] = (__typeof__(seen[0])){rv->rec.rep[k].node, last};
            else if (j < ns && last > seen[j].last)
                seen[j].last = last;
        }
        pthread_mutex_unlock(&rv->mu);
    }
    unsigned n = 0;
    for (unsigned j = 0; j < ns && n < max; j++) {
        uint64_t age = now > seen[j].last ? now - seen[j].last : 0;
        if (age >= after_ms)
            out[n++] = (struct cluster_unreach){seen[j].node, (uint32_t)(age > UINT32_MAX ? UINT32_MAX : age)};
    }
    return n;
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

static int close_fn(void *ctx, struct eng **old, struct eng **neu)
{
    (void)ctx;
    eng_close(*old); /* its error is the failure the driver stopped the volume on (or an open it never finished) */
    *old = NULL;
    *neu = NULL;
    return 0;
}

/* Closes volume v's engine and leaves its slot free for a later tsdb_cvol_open. */
static void cvol_close(uint32_t v)
{
    (void)tsdb_cvol_replace(G.t, v, close_fn, NULL, CVOL_NONE); /* TSDB_ERR_IO by contract: closed, nothing new */
}

void repld_close_failed(void)
{
    pthread_mutex_lock(&G.mu);
    uint32_t n = G.nvols;
    pthread_mutex_unlock(&G.mu);
    for (uint32_t i = 0; i < n; i++)
        if (repld_failed(G.vols[i]->number) && !G.vols[i]->started)
            cvol_close(G.vols[i]->number);
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
        pthread_cond_broadcast(&rv->in_space);
        pthread_mutex_unlock(&rv->mu);
        if (rv->started)
            pthread_join(rv->th, NULL);
        rv->started = 0;
        struct eng *db = tsdb_cvol_ref(G.t, rv->number);
        if (db && rv->mode == M_WRITER && G.mutant != RM_STOPNODEPOSE && !G.late_wal)
            eng_leader_depose(db); /* nothing waits for a commit point that no longer moves */
        tsdb_cvol_unref(&db);
    }
    /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket to the peerlink any more */
    if (G.pl) {
        cluster_set_peerlink(NULL);
        peerlink_free(G.pl);
        G.pl = NULL;
    }
    /* `stop`, set under each volume's mu above, keeps the engine's hooks (op_bytes, op_ship) off the freed links */
    if (G.late_wal)
        nanosleep(&(struct timespec){2, 500000000}, NULL);
}
