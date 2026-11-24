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
 * a resync every time, and in-flight records would otherwise be sent twice. "Had time to arrive" is also the peer's
 * acknowledged end standing still for LOST_MS while the cursor is ahead of it: under writes that never pause the fast
 * path moves the cursor (and sent_ms) with every record, and a peer that refuses them all for a gap (a replica
 * restarted below the cursor) was never sent the records it lacks (the merge's jepsen-cluster kill run: a replica's log
 * stayed where it died for 30 minutes; rejoin, mutant fastride).
 *
 * rec_find answers from the driver's index of WAL record starts (fed by the leader's bytes and the replica's WRITEs,
 * built at open by reading the log), and (pos, 1) for a position the writer admitted but has not written yet.
 *
 * Mutants (TSDB_REPLD_MUTANT, tests only; NOTES of r3d-stream says which test catches each): `commitbyleader` commits
 * at the writer's own durable end, `ackbeforesync` has a follower report its written end as durable without a sync,
 * `floornotsaved` skips the REPL file's SAVE, `nodepose` never deposes a failed writer, `noquiesce` leaves a failed
 * volume's engine reachable (D13; test_cluster writerfail catches both); test_cluster repcheck catches `floornottold`
 * (a replica's saved floor never reaches its engine: its flushed segments stay), `unsureignored` (an unsure REPL taken
 * as it stands) and `admitnocheck` (no check of the core's position against the log number), and measures
 * `nofastpath` (records go out on the tick only; caught only by the latency bound TSDB_TEST_ACK_MS that
 * tests/repld_mutants.sh sets); `hbnoclamp` (a heartbeat names positions not sent yet) SURVIVES: no scenario delays a
 * link yet;
 * `nocursorup` never moves a peer's cursor up to what it acknowledged (repcheck's `stalesent` step catches it);
 * `fastride` moves the cursor back on a resync only once sent_ms is RESEND_MS old (rejoin).
 * `nojoinhold` holds no commit point for a learner close behind (join_hold_ticks 0: it joins only in a pause of the
 * writes; rejoinsteady), `inboxstarve` gives the inbox no time after a tick while the next one is due (a catch-up read
 * longer than a tick starved the other peers' ACKs; rejoinslow), `opennohold` starts a replica's core with its whole
 * log durable, past a COMMIT marker whose file it has not staged (reopenhold), `tickstretch` drops the ticks a slow
 * catch-up step made late, so the core's clock (isr_lag, the join hold) runs slower than the wall's (tickwall),
 * `stepcurrentonly` runs catch-up steps only on ticks that are not late (a thread always late never catches a peer
 * up: rejoinlate), `stallcatchup` catches a stop of the whole process up in ticks and counts its own stop as its peers'
 * silence (jointpause).
 * `demotestaleidx` rebuilds a deposed writer's index of record starts over the entry its old engine pushed after demote
 * cleared it (core_build), so the cut of its uncommitted tail stops the volume (deposetail).
 * `commitpastremoved` takes an ISR request as the record at once, before the metadata group commits it (isr),
 * `nominisr` never checks min_isr (minisr), `ackbelowminisr` hands the writer a commit point reached after a shrink
 * below min_isr (minisr), `nocap` keeps the leader's segments for a lagging replica without the caps repl_retain_mb and
 * repl_retain_s (keepcap), `noseal` leaves an idle leader's last segment unsealed (no idle flush; the NOW markers go
 * on) (idleseal), `keepwritten` keeps the leader's segments by its peers' written ends, not their durable ones
 * (replicate); tests/repld_mutants.sh names the rest and pins each mutant to its scenario's FAIL line.
 * Faults for tests, TSDB_REPLD_TEST=NAME[,NAME]: `admithold` holds one marker's admit until a transfer drains (admit_held); `dropbulk` has the first bulk file this process receives taken as staged but not kept, so its COMMIT marker finds
 * the file missing and FETCHes it; `fetchlate` has a replica send its first FETCH of a name only 5 s after it found
 * the file missing (the leader has sealed and flushed the marker's segment by then); `nofetch` has a replica send no
 * FETCH at all and say once per name that it withholds it (reopenhold kills it there); `syncpause` has a replica's
 * SYNCs wait 4 s after its core's open (its ACKs carry the durable end it opened with; reopenhold); `promotewait` waits 1 s between
 * the promotion's engine swap and the mode M_WRITER
 * (writes then reach the new writer while the driver still promotes: they must be refused for a retry, never depose
 * it); `noclock` has a leader write no NOW markers and no idle seal (writersync); `skippos` has one admit take a
 * position more once the file <dir>/skippos exists (the check in op_admit must
 * fail the volume before the write is acknowledged); `stalesent` sets one peer's cursor back to 0 once the file
 * <dir>/stalesent exists and the leader's log starts above position 1, so 0 is below the log's start (what a resync
 * that raced records in flight left behind; the cursor must move up to what the peer acknowledged, or it is never sent
 * another record; with the log still starting at 1 the catch-up from 0 finds every record and the fault proves
 * nothing: `nocursorup` survived such a run); `slowread` has every catch-up
 * read take SLOW_READ_MS more (a slow disk or NFS); `slowtick` has a writer's catch-up step take SLOW_TICK_MS at every
 * tick it runs (tickwall); `latetick` has every tick of a writer take LATE_TICK_MS more (rejoinlate); `drainhold` keeps
 * a transfer's drain from READY while <dir>/drainhold exists (rtransfer.c, transferraftdrain); `slowack` has a replica's ACKs report the durable end it had
 * SLOW_ACK_MS before (a far replica: always a few writes behind the leader's end while it keeps up); `latebytes` holds
 * the writer's first rows record written while <dir>/latebytes exists in op_bytes, after its write(), until a demote has
 * begun (deposetail).
 * TSDB_REPLD_TRACE=1 (debugging; jepsen-cluster passes the environment on) logs every 2 s per volume a line of the
 * core's state (repl_debug: per peer next, match, acked) and the driver's cursors (sent, want, acked, ages, link).
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
#include "crashpt.h"
#include "peerlink.h"
#include "replwire.h"
#include "../src/cvol.h"
#include "../src/eccold.h"
#include "../src/ecapply.h"
#include "../src/ecdiverge.h"
#include "../src/marker.h"
#include "../src/repl.h"
#include "../src/replfile.h"
#include "../src/replica.h"

#include "repld_int.h"
#include "rset.h"
#include "rsnap.h"
#include "rtransfer.h"

struct repld_g G = {.mu = PTHREAD_MUTEX_INITIALIZER};

void say(const char *fmt, ...)
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

struct rvol *vol_find(uint32_t number)
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
    /* the core's own messages: it stamps its set version (src/repl.h), and one queued before a reconfiguration is in the
     * old record's indexes, so it must keep the old version (rset.h: rset_stamp is for what the driver builds) */
    uint8_t b[RW_HEAD];
    size_t n = rw_encode_msg(b, sizeof b, rv->number, m, NULL, 0, 0, NULL, 0);
    if (l && n)
        plink_send(l, RW_T_MSG, b, n, 0);
}

static void cvol_close(uint32_t v);

/* slowack (tests): the ACK reports the highest durable end an ACK of SLOW_ACK_MS ago had, never more than it has now
 * (a lower durable end is always true) */
static void slow_ack(struct rvol *rv, struct repl_msg *m)
{
    uint64_t now = now_ms(), lag = rv->sa_last;
    rv->sa_ms[rv->sa_n % SLOW_ACK_RING] = now;
    rv->sa_dur[rv->sa_n++ % SLOW_ACK_RING] = m->durable;
    for (unsigned i = 0; i < SLOW_ACK_RING && i < rv->sa_n; i++)
        if (rv->sa_ms[i] + SLOW_ACK_MS <= now && rv->sa_dur[i] > lag)
            lag = rv->sa_dur[i];
    rv->sa_last = lag;
    if (m->durable > lag)
        m->durable = lag;
}

/* A message of the core to the metadata group. ISR_REQ is kept for this server's heartbeats (repld_isr_requests: the
 * Raft leader proposes ISR_CHANGE on the request's own basis) until the record moves. FENCE_REPLY (after the core's SAVE
 * of the fence: the action order) goes to the Raft leader that sent the fence, RESIGN to the Raft leader, both through
 * the cluster thread (cluster_vol_msg); the core repeats a RESIGN while the record names it, the proposer a fence. */
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
            /* in the core's set version like every META the driver hands over (IN_META): unstamped (0) it was dropped
             * by the core's set-version check from r4-setchg on (a volume starts at set version 1), and the mutant
             * planted nothing; a drop says so, and scen isr dies with that reason (not by the mutant's own line) */
            rset_stamp(rv, &mm);
            uint64_t d0 = repl_dropped_meta(rv->core);
            repl_recv(rv->core, &mm);
            if (repl_dropped_meta(rv->core) != d0)
                say("volume %" PRIu32 ": test: the planted META was dropped by the core", rv->number);
        }
    } else if (m->type == REPL_M_FENCE_REPLY) {
        struct cluster_vmsg q = {.kind = CL_VMSG_FENCE_REPLY, .volume = rv->number, .epoch = m->epoch,
                                 .durable = m->durable, .last_epoch = m->last_epoch, .rid = m->rid, .start = m->start,
                                 .to = rv->fence_from};
        crash_point(CP_FENCE_SAVED); /* the fence is in REPL, the reply not yet out */
        if (G.no_fence_reply) {
            char f[sizeof G.dir + 16];
            snprintf(f, sizeof f, "%s/nofencereply", G.dir);
            if (access(f, F_OK) == 0) {
                /* once per epoch and volume is enough for a test's log; per volume, not a static: every volume has
                 * a thread of its own, and many withhold at once (test_cluster manystuck, a TSan race) */
                if (rv->nofence_said != m->epoch)
                    say("volume %" PRIu32 ": withholds its answer to the fence for epoch %" PRIu64 " (nofencereply)",
                        rv->number, m->epoch);
                rv->nofence_said = m->epoch;
                return;
            }
        }
        cluster_vol_msg(&q);
    } else if (m->type == REPL_M_RESIGN) {
        if (rv->resigned != m->epoch)
            say("volume %" PRIu32 ": cannot lead epoch %" PRIu64 " (led it before a restart); resigns", rv->number,
                m->epoch);
        rv->resigned = m->epoch;
        struct cluster_vmsg q = {.kind = CL_VMSG_RESIGN, .volume = rv->number, .epoch = m->epoch, .rid = m->rid};
        cluster_vol_msg(&q);
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
    rset_stamp(rv, &m);
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

/* After `sent` or `wrote` changed: whether the cursor is ahead of what the peer acknowledged having, and since when */
static void ahead_mark(struct rpeer *p, uint64_t now)
{
    if (p->sent <= p->wrote)
        p->ahead_ms = 0;
    else if (!p->ahead_ms)
        p->ahead_ms = now;
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
        if (G.slow_read && rc == 0 && len) {
            struct timespec sl = {0, SLOW_READ_MS * 1000000L};
            nanosleep(&sl, NULL);
        }
        pthread_mutex_lock(&rv->mu);
        if (rc < 0 && rc != TSDB_ERR_NOTFOUND) { /* NOTFOUND: in parts, the core sends a snapshot */
            uint64_t now = now_ms();
            if (rc != p->rderr || now - p->rderr_ms >= 10000) {
                say("volume %" PRIu32 ": cannot read its log at %" PRIu64 " for member %d (error %d): the member does not"
                    " catch up", rv->number, from, i, rc);
                p->rderr = rc;
                p->rderr_ms = now;
            }
        }
        if (rc == 0 && len && p->sent + 1 == from && send_records(rv, i, seq, off, buf, len, from) == 0) {
            p->sent = last;
            p->sent_ms = now_ms();
            ahead_mark(p, p->sent_ms);
        }
        free(buf);
    }
}

/* latebytes (tests): a writer's record admitted before its depose whose bytes reach op_bytes only after the demote
 * began (a write held in the WAL writer while the process was stopped: jepsen-cluster pause, 2026-11-22). Held here
 * after the write() and before anything is indexed or shipped, with no lock, until the driver has no core. */
static void late_bytes_hold(struct rvol *rv, const void *buf, size_t len, uint64_t seq, uint64_t off, uint64_t first,
                            uint64_t last)
{
    int rows = 0;
    for (size_t o = 0; o < len;) {
        wal_rec wr;
        size_t sz = wal_record((const uint8_t *)buf + o, len - o, seq, off + o, &wr);
        if (!sz)
            break;
        rows |= wr.kind == WAL_KIND_ROWS;
        o += sz;
    }
    char f[sizeof G.dir + 16];
    snprintf(f, sizeof f, "%s/latebytes", G.dir);
    if (!rows || access(f, F_OK) != 0 || atomic_exchange(&G.late_bytes_done, 1))
        return;
    say("test: volume %" PRIu32 ": the bytes of %" PRIu64 "..%" PRIu64 " are held until the writer is deposed", rv->number,
        first, last);
    uint64_t t0 = now_ms();
    for (;;) {
        pthread_mutex_lock(&rv->mu);
        int gone = rv->core == NULL;
        pthread_mutex_unlock(&rv->mu);
        if (gone || now_ms() - t0 > 30000)
            break;
        struct timespec ts = {0, 10000000};
        nanosleep(&ts, NULL);
    }
    say("test: volume %" PRIu32 ": the bytes of %" PRIu64 "..%" PRIu64 " %s", rv->number, first, last,
        now_ms() - t0 > 30000 ? "go on after 30 s" : "released by the demote");
}

/* jr_ops.bytes: the writer's records, before its fdatasync; out at once to every peer whose cursor is at them */
static void op_bytes(void *ctx, struct eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len,
                     uint64_t first, uint64_t last)
{
    (void)db;
    struct rvol *rv = ctx;
    /* crash points (design-cluster 13.2): the leader has written, not synced and not sent: wal-before-send only for
     * bytes with a client's rows (r3d-isr review L3: the NOW markers, one a second, and the idle seal took the hit
     * when no write was pending); class-switch when a CLASS marker is among the records (the switch's record in the
     * log, its CLASSES record not yet) */
    int rows = 0;
    if (G.late_bytes)
        late_bytes_hold(rv, buf, len, seq, off, first, last);
    pthread_mutex_lock(&rv->mu);
    for (size_t o = 0; o < len;) {
        wal_rec wr;
        size_t sz = wal_record((const uint8_t *)buf + o, len - o, seq, off + o, &wr);
        if (!sz)
            break;
        if (wr.kind == WAL_KIND_MARKER && wr.mtype == MK_CLASS)
            crash_point(CP_CLASS_SWITCH);
        rows |= wr.kind == WAL_KIND_ROWS;
        idx_push(rv, wr.fno, wr.fno + wr.span - 1);
        o += sz;
    }
    if (rows)
        crash_point(CP_WAL_BEFORE_SEND);
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
        if (i != rv->self && p->sent + 1 == first && G.mutant != RM_NOFASTPATH &&
            send_records(rv, i, seq, off, buf, len, first) == 0) {
            p->sent = last;
            p->sent_ms = now_ms();
            ahead_mark(p, p->sent_ms);
        }
    }
    pthread_mutex_unlock(&rv->mu);
}

/* TSDB_REPLD_TEST=admithold: once <dir>/admithold holds a marker type (src/marker.h), the next admit of a marker of
 * that type on a writer removes the file, says so and waits (with buf_lock held, without rv->mu) until a transfer of
 * the volume drains, 20 s at most: a transfer with that marker in flight (test_cluster transfermarkers) */
static void admit_held(struct rvol *rv)
{
    char f[sizeof G.dir + 16], b[16] = {0};
    snprintf(f, sizeof f, "%s/admithold", G.dir);
    FILE *fp = fopen(f, "r");
    if (!fp)
        return;
    size_t n = fread(b, 1, sizeof b - 1, fp);
    fclose(fp);
    b[n] = 0;
    if (!eng_admit_marker || atoi(b) != eng_admit_marker || unlink(f) != 0)
        return;
    say("volume %" PRIu32 ": the admit of a marker of type %u held until a transfer drains", rv->number,
        (unsigned)eng_admit_marker);
    for (int i = 0; i < 2000; i++) {
        pthread_mutex_lock(&rv->mu);
        int d = rv->mode == M_WRITER && rtr_draining(rv);
        pthread_mutex_unlock(&rv->mu);
        if (d)
            break;
        usleep(10000);
    }
    say("volume %" PRIu32 ": the held admit of a marker of type %u goes on", rv->number, (unsigned)eng_admit_marker);
}

/* jr_ops.admit, under buf_lock: the core's positions must be the engine's */
static int op_admit(void *ctx, struct eng *db, uint64_t fno, uint32_t span)
{
    struct rvol *rv = ctx;
    if (G.admit_hold)
        admit_held(rv);
    pthread_mutex_lock(&rv->mu);
    uint64_t pos = 0;
    /* M_PROMOTING: the new writer engine is listed (tsdb_cvol_replace) before the driver's mode says so; a write in
     * that window is refused for a retry, never taken as a sign that this node no longer leads (that deposed the
     * fresh leader for good) */
    if (rv->mode == M_PROMOTING) {
        pthread_mutex_unlock(&rv->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    /* a planned transfer drains (rtransfer.c, D14): refused for a retry, definite (never admitted) */
    if (rv->mode == M_WRITER && !rtr_admit_ok(rv)) {
        pthread_mutex_unlock(&rv->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    /* no read lease (§7.1): a write that raced the check in tsdbd is refused the same way, never taken as a deposal */
    if (rv->mode == M_WRITER && !cluster_lease_valid() && !(G.late_wal && rv->stop)) {
        pthread_mutex_unlock(&rv->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    int rc = rv->mode == M_WRITER && rv->core ? repl_write(rv->core, span, &pos) : REPL_E_NOTLEADER;
    if (rc == REPL_OK && G.skip_pos == 1) { /* the fault: the core's positions run one ahead of the log numbers */
        char f[sizeof G.dir + 16];
        snprintf(f, sizeof f, "%s/skippos", G.dir);
        if (access(f, F_OK) == 0) {
            G.skip_pos = 2;
            rc = repl_write(rv->core, 1, &pos);
        }
    }
    if (rc == REPL_OK && pos != fno && G.mutant != RM_ADMITNOCHECK) {
        eng_fail(db, "replication: the core's position is not the log number", TSDB_ERR_CORRUPT);
        rc = REPL_E_FAILED;
    }
    pthread_cond_signal(&rv->cv);
    pthread_mutex_unlock(&rv->mu);
    if (rc == REPL_E_NOTLEADER)
        eng_leader_depose(db);
    return rc == REPL_OK ? 0 : TSDB_ERR_UNAVAILABLE;
}

/* The writer's durable end to the core (rv->mu held); db NULL for synclate's held report */
static void synced_locked(struct rvol *rv, struct eng *db, uint64_t last)
{
    if (rv->core && last > rv->synced_rep) {
        rv->synced_rep = last;
        repl_synced(rv->core, last);
        if (G.mutant == RM_COMMITBYLEADER && db)
            eng_leader_commit(db, last);
        pthread_cond_signal(&rv->cv);
    }
}

/* jr_ops.synced: the writer's own durable end */
static void op_synced(void *ctx, struct eng *db, uint64_t last)
{
    struct rvol *rv = ctx;
    pthread_mutex_lock(&rv->mu);
    if (G.sync_late && (rv->sync_n++ & 1) == 0 && last > rv->synced_rep) {
        if (last > rv->sync_late)
            rv->sync_late = last;
    } else {
        synced_locked(rv, db, last);
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
            if (in_isr_locked(rv, i) || G.mutant == RM_SHIPNOISR)
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
    uint32_t lag = 0;
    if (b->rv) { /* the writer's thread, not the volume's: rv->rec is the manager's to rewrite (rec_set) under rv->mu */
        pthread_mutex_lock(&b->rv->mu);
        lag = b->rv->rec.isr_lag_ms;
        pthread_mutex_unlock(&b->rv->mu);
    }
    uint64_t span = 2ull * (lag ? lag : 1000);
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
    if (tl_ship.n && tl_ship.rv == ctx) {
        ship_batch_end(!failed);
        if (!failed) /* a large write's files are staged on the replicas (or their deadline passed), its COMMIT not
                      * written: once per COMMIT, after its one wait (the per-file wait it followed is gone) */
            crash_point(CP_PREPARED_BEFORE_COMMIT);
    } else if (tl_ship.n)
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
    if (plink_kind(l) == PL_KIND_SNAP || rsnap_owns(l, file, meta, meta_len)) /* a snapshot's file: rsnap.c's (D7) */
        return rsnap_bin_open(arg, l, file, meta, meta_len, total, resume, prefix_fd);
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


static int bin_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    if (plink_kind(l) == PL_KIND_SNAP || rsnap_owns(l, file, NULL, 0))
        return rsnap_bin_data(arg, l, file, off, p, n);
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
    if (plink_kind(l) == PL_KIND_SNAP || rsnap_owns(l, file, NULL, 0)) {
        rsnap_bin_end(arg, l, file, status);
        return;
    }
    (void)arg, (void)status;
    pthread_mutex_lock(&G.mu);
    struct rin *r = rin_find(l, file, 1);
    pthread_mutex_unlock(&G.mu);
    rin_free(r);
}

/* ------------------------------------------------------------------ the inbox */

static int peer_kind(int kind) { return kind == IN_MSG || kind == IN_FETCH || kind == IN_SNAP; }

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
        char f[sizeof G.dir + 16], fv[sizeof G.dir + 32];
        snprintf(f, sizeof f, "%s/stallinbox", G.dir);
        snprintf(fv, sizeof fv, "%s/stallinbox.%" PRIu32, G.dir, rv->number);
        /* the stall ends at repld_stop too: a test that died with the file in place left a replica that logged
         * "shutting down" and then waited in repld_stop's join for ever, holding its io_uring ring under init, so
         * that every later io_uring run on the machine failed to start (2026-11-18) */
        for (struct timespec w = {0, 10000000}; !rv->stop && (access(f, F_OK) == 0 || access(fv, F_OK) == 0);) {
            pthread_mutex_unlock(&rv->mu);
            nanosleep(&w, NULL);
            pthread_mutex_lock(&rv->mu);
        }
    }
}

/* Room in the inbox for a peer frame of `need` bytes (rv->mu held), else 0: the link's thread waits a little (which
 * stops reading the link: TCP pushes back on the sender), then the frame is dropped and the core sends it again. The
 * inbox is never larger than the bound plus one frame (a frame alone always gets in), and local items are not counted
 * (they are small and must not be lost). A failed volume reads nothing: its frames are dropped at once.
 * The wait happens once per stall: while `in_full` is set (until the inbox empties) a frame is dropped at once. The
 * link and its thread are shared by every volume between the two nodes, so a volume whose disk is stuck (an NFS
 * server down for hours) must not hold up the others' frames: were each of its frames to wait, its leader's resends
 * alone would keep the thread asleep, and the replica would leave the ISR of every volume on that link (r3d-isr
 * review H1; test_cluster inboxshared). */
static int inbox_room(struct rvol *rv, size_t need)
{
    if (rv->in_bytes + need <= G.inbox_bytes || !rv->in_bytes)
        return 1;
    if (rv->in_full && G.mutant != RM_INBOXWAITALL)
        return 0;
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

void post(struct rvol *rv, int kind, uint32_t node, const void *p, size_t n)
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
    if (plink_kind(l) == PL_KIND_SNAP) { /* a snapshot's control frame: rsnap.c posts what it keeps (IN_SNAP) */
        rsnap_frame(l, type, p, n);
        return;
    }
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
    (void)arg;
    if (up && G.snap_dial && plink_kind(l) == PL_KIND_SNAP) /* TSDB_REPLD_TEST=snapdial: one frame on the new link */
        plink_send(l, 1, "snapdial", 8, 0);
}

/* ------------------------------------------------------------------ actions */

void vol_fail(struct rvol *rv, const char *why)
{
    if (rv->failed)
        return;
    rv->failed = 1;
    say("volume %" PRIu32 ": stopped: %s", rv->number, why);
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    if (db && rv->mode == M_WRITER && G.mutant != RM_NODEPOSE)
        eng_leader_depose(db);
    tsdb_cvol_unref(&db);
}

/* TSDB_REPLD_TEST=plantdiverge: the volume's DIVERGED file as a settle's failed digest writes it. Under rv->mu, which
 * it lets go of for the write (the engine reference must not be held across a replace, nor rv->mu across I/O). */
static void plant_diverge(struct rvol *rv)
{
    char f[sizeof G.dir + 32];
    snprintf(f, sizeof f, "%s/diverge.%" PRIu32, G.dir, rv->number);
    if (access(f, F_OK) != 0)
        return;
    pthread_mutex_unlock(&rv->mu);
    unlink(f);
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    int rc = db ? ec_diverge(db, 0, NULL, EC_DIV_TEST) : TSDB_ERR_UNAVAILABLE;
    tsdb_cvol_unref(&db);
    say("test: volume %" PRIu32 ": planted DIVERGED: %s", rv->number, rc ? tsdb_strerror(rc) : "ok");
    pthread_mutex_lock(&rv->mu);
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
int save_repl(struct rvol *rv, uint64_t fence, uint64_t led, uint64_t floor)
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
    struct eng *db = tsdb_cvol_ref(G.t, rv->number); /* the replay start the ACK carries (D17; r4-replay makes it real) */
    if (db && rv->core)
        repl_replay_start(rv->core, replica_replay_start(db));
    tsdb_cvol_unref(&db);
}

static int isr_short_locked(const struct rvol *rv);


/* TSDB_REPLD_TEST=keepall is lifted while <dir>/keepoff exists (test_cluster addswitch: a log start pinned at 1 also
 * pins the class switches' replay floor, so the scenario lifts it once its learner's own ack holds the log) */
static int keep_off(void)
{
    char f[sizeof G.dir + 16];
    snprintf(f, sizeof f, "%s/keepoff", G.dir);
    return access(f, F_OK) == 0;
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
            crash_point(CP_PROMOTE_BEFORE_EPOCH);
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
            /* syncbyhook (tests): the end op_synced handed over, never a fresh read of the engine, so that a SYNC
             * issued behind an op_synced that went past it is answered only by this comparison (writersync) */
            uint64_t d = G.sync_by_hook ? rv->synced_rep : db ? eng_wal_durable_fno(db) : 0;
            int fresh = d > rv->synced_rep;
            if (fresh)
                rv->synced_rep = d;
            /* Compared with the core's durable end, never with synced_rep: op_synced may have handed the core a
             * larger end while an earlier SYNC was in flight, which the core caps at that SYNC's end (and issues this
             * one); were the end compared with synced_rep here, this SYNC would never be answered, and with no
             * further write to sync the commit point would stay below a durable WAL for good (backpressure hung so) */
            uint64_t have = repl_durable_end(rv->core);
            if (G.no_clock && !fresh && d > have && !rv->sync_said) {
                say("test: volume %" PRIu32 ": a SYNC finds the WAL durable to %" PRIu64 ", past the core's %" PRIu64
                    " (an engine sync went past the SYNC in flight)", rv->number, d, have);
                rv->sync_said = 1;
            }
            if (G.mutant == RM_SYNCBYREP ? fresh : d > have)
                repl_synced(rv->core, d);
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
        if (a->pos < rv->idx_end)
            rtr_note_cut();
        idx_cut(rv, a->pos);
        hist_cut(rv, a->pos);
        break;
    case REPL_A_SAVE:
        rc = G.mutant == RM_FLOORNOTSAVED || (G.mutant == RM_FENCENOSYNC && a->fence > rv->st.fence)
                 ? 0
                 : save_repl(rv, a->fence, a->led, a->floor);
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
            if (G.keep_all && !keep_off()) /* until r4-xferinst: a learner added or reset later must find position 1 */
                keep = 0;
            /* the caps (design-cluster.md 8.3): past them a lagging peer's segments go, up to the floor, and the core
             * tells that peer to install a snapshot once the log start passed what it needs */
            bool nocap = G.mutant == RM_NOCAP || G.keep_all; /* keepall: a learner's position 1 outlives the caps */
            uint64_t capb = nocap ? 0 : (uint64_t)rv->rec.repl_retain_mb << 20;
            uint64_t capms = nocap ? 0 : (uint64_t)rv->rec.repl_retain_s * 1000;
            uint64_t floor = a->floor;
            pthread_mutex_unlock(&rv->mu);
            eng_leader_keep_cap(db, capb, capms);
            eng_leader_clock(db, G.no_clock ? 0 : NOW_EVERY_MS,
                             G.mutant == RM_NOSEAL || G.no_clock ? 0 : rv->rec.idle_seal_ms);
            eng_leader_keep_floor(db, keep, floor);
            pthread_mutex_lock(&rv->mu);
        } else if (a->floor) {
            uint64_t capb = (uint64_t)rv->rec.repl_retain_mb << 20, capms = (uint64_t)rv->rec.repl_retain_s * 1000;
            pthread_mutex_unlock(&rv->mu);
            replica_keep_cap(db, capb, capms); /* the settings may have changed since the open */
            rc = G.mutant == RM_FLOORNOTTOLD ? 0 : replica_floor_saved(db, a->floor);
            pthread_mutex_lock(&rv->mu);
        }
        if (G.mutant != RM_NOLOGSTARTCHECK) /* planted: the core never hears that the log start moved */
            log_start_check(rv);
        { /* the replay start (D17): the first position an open of this member may replay, past the kept segments; the
           * core sends it in its ACKs (a leader uses its own), and the leader's replay floor rises with it */
            pthread_mutex_unlock(&rv->mu);
            uint64_t rs = replica_replay_start(db);
            pthread_mutex_lock(&rv->mu); /* rv->core again: only this thread swaps it (demote, promote) */
            if (rs)
                repl_replay_start(rv->core, rs);
        }
        break;
    case REPL_A_SEND:
        if (a->msg.type == REPL_M_APPEND && a->msg.to >= 0 && a->msg.to < (int)rv->rec.nrep) {
            struct rpeer *p = &rv->peer[a->msg.to];
            if (a->upto > p->want)
                p->want = a->upto;
            if (G.stale_sent == 1 && rv->mode == M_WRITER && p->acked > 0 && rv->log_start > 1) {
                /* the fault: the cursor of a peer that has acknowledged records goes back below the log's start (only
                 * once that start is above 1: before, a cursor of 0 is no fault) */
                char f[sizeof G.dir + 16];
                snprintf(f, sizeof f, "%s/stalesent", G.dir);
                if (access(f, F_OK) == 0) {
                    G.stale_sent = 2;
                    p->sent = 0;
                    say("volume %" PRIu32 ": test fault: the cursor of peer %d set back to 0 (it acknowledged %" PRIu64
                        ", the log starts at %" PRIu64 ")", rv->number, a->msg.to, p->acked, rv->log_start);
                }
            }
            /* The peer has what it acknowledged, and the core still counts on it (it has not asked to go back below
             * it): the cursor moves up to it. A resync that moved the cursor back while records were in flight (they
             * arrived after RESEND_MS) would otherwise leave it below what the peer holds for good, and once the
             * leader's log no longer starts there (the commit point let it drop those segments) the catch-up finds
             * nothing to read and the peer is never sent another record. */
            uint64_t now = now_ms();
            if (p->acked > p->sent && a->msg.prev_pos >= p->acked && G.mutant != RM_NOCURSORUP) {
                p->sent = p->acked;
                ahead_mark(p, now);
            }
            /* sent past what it has, its end standing still for LOST_MS: what went out since did not arrive, however
             * recently the fast path sent more (it refuses each for the gap). Far above a live peer's ack latency, so a
             * slow peer is not sent its backlog twice */
            int lost = p->ahead_ms && now - p->ahead_ms >= LOST_MS && now - p->wrote_ms >= LOST_MS &&
                       G.mutant != RM_FASTRIDE;
            if (a->msg.prev_pos < p->sent && (now - p->sent_ms >= RESEND_MS || lost || !p->heard)) {
                p->sent = a->msg.prev_pos ? rec_start(rv, a->msg.prev_pos + 1) - 1 : 0;
                if (p->sent > a->msg.prev_pos)
                    p->sent = a->msg.prev_pos;
                ahead_mark(p, now);
            }
            if (a->upto <= a->msg.prev_pos) {
                /* a heartbeat: the commit point, after what the peer has been sent */
                struct repl_msg m = a->msg;
                if (m.prev_pos > p->sent && G.mutant != RM_HBNOCLAMP)
                    m.prev_pos = p->sent;
                m.prev_epoch = epoch_at(rv, m.prev_pos);
                send_msg(rv, &m);
            }
        } else if (a->msg.to == REPL_META) {
            meta_msg(rv, &a->msg);
        } else {
            if (G.slow_ack && a->msg.type == REPL_M_ACK && rv->mode == M_REPLICA)
                slow_ack(rv, &a->msg);
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
            crash_point(CP_ACKED_BEFORE_OK); /* the replicas acknowledged; no client has its answer yet */
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
            if (G.promote_slow) /* a fault for tests: a reopen of seconds (a large volume, a loaded host) */
                nanosleep(&(struct timespec){5, 0}, NULL);
            uint64_t t_promote = now_ms();
            rc = tsdb_cvol_replace(G.t, rv->number, promote_fn, rv, CVOL_WRITER);
            t_promote = now_ms() - t_promote; /* D6: the promotion is a reopen; its time is in the log line */
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
            say("volume %" PRIu32 ": leads epoch %" PRIu64 " from position %" PRIu64
                " (reopened as the writer in %" PRIu64 " ms)", rv->number, rv->epoch, rv->epoch_pos, t_promote);
            /* the clock from the start (design-cluster.md 5.4), not from the first SAVE as a writer, which waits for
             * applied records: an idle new leader sent no NOW markers at all. buf_lock is above rv->mu. */
            if (G.mutant != RM_LATECLOCK && (db = tsdb_cvol_ref(G.t, rv->number))) {
                pthread_mutex_unlock(&rv->mu);
                eng_leader_clock(db, G.no_clock ? 0 : NOW_EVERY_MS,
                                 G.mutant == RM_NOSEAL || G.no_clock ? 0 : rv->rec.idle_seal_ms);
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
    case REPL_A_SNAPSHOT: /* the core repeats the notice until the replica catches up */
        rsnap_need(rv, (int)a->pos);
        break;
    }
    tsdb_cvol_unref(&db);
}

void drain(struct rvol *rv)
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

/* rv->rec = *r (rv->mu held, or the volume not yet listed), with G.nshort kept: the write path's check of min_isr
 * (repld_isr_short, every write request) takes no lock while no volume is short (r3d-isr review M3) */
void rec_set(struct rvol *rv, const struct cluster_vol_rec *r)
{
    rv->rec = *r;
    int s = isr_short_locked(rv);
    if (s != rv->rec_short) {
        rv->rec_short = s;
        __atomic_add_fetch(&G.nshort, s ? 1 : -1, __ATOMIC_RELEASE);
    }
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
        if (db) {
            crash_point(CP_ACKED_BEFORE_OK); /* as in APPLY (r3d-isr review L1): the held commit reaches the writer */
            eng_leader_commit(db, rv->held_commit);
        }
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
            /* without it the core's unanswered_ticks never fires and a dead old leader holds a new leader's commit
             * point (its EPOCH marker first) for isr_lag_ms (test_cluster failover with isr_lag_ms 10000) */
            if (rv->rec.rep[k].unanswered)
                m.unanswered |= 1u << k;
        }
    }
    return m;
}

static void handle(struct rvol *rv, struct item *it)
{
    if (it->kind == IN_FENCE) { /* design-cluster.md 6.2 step 1: the core saves it (SAVE) before it replies */
        uint64_t fence;
        if (it->n != sizeof fence)
            return;
        memcpy(&fence, it->data, sizeof fence);
        struct repl_msg m = {.type = REPL_M_FENCE, .from = REPL_META, .to = rv->self, .epoch = fence, .leader = -1};
        rset_stamp(rv, &m);
        rv->fence_from = it->node;
        repl_recv(rv->core, &m);
        drain(rv);
        return;
    }
    if (it->kind == IN_META) {
        rset_apply(rv); /* a set change first: the META below is in the core's set version (src/repl.h) */
        struct repl_msg m = {.type = REPL_M_META, .from = REPL_META, .to = rv->self, .leader = -1};
        m.meta = vmeta_of(rv);
        m.epoch = m.meta.epoch;
        rset_stamp(rv, &m);
        repl_recv(rv->core, &m);
        drain(rv);
        isr_meta(rv);
        return;
    }
    if (it->kind == IN_SNAP) {
        rsnap_in(rv, it->node, it->data, it->n);
        return;
    }
    if (it->kind == IN_CMD) { /* a VOLCMD of the Raft leader (cluster.h struct cluster_volcmd) */
        struct cluster_volcmd c;
        if (it->n != sizeof c)
            return;
        memcpy(&c, it->data, sizeof c);
        if (c.kind == CL_VOLCMD_DROP_MEMBER)
            rset_cmd(rv, &c, it->node);
        else if (c.kind == CL_VOLCMD_TRANSFER || c.kind == CL_VOLCMD_CANCEL)
            rtr_cmd(rv, &c, it->node);
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
    if (!rset_msg_ok(rv, &rv->cur.m)) {
        rv->cur.m.nrec = 0;
        return; /* another replica set version (rset.c) */
    }
    if (G.cut_links) { /* a data link cut both ways when each end has the other's file (the peer port stays) */
        char f[sizeof G.dir + 32];
        snprintf(f, sizeof f, "%s/cutfrom.%" PRIu32, G.dir, rv->rec.rep[from].node);
        if (access(f, F_OK) == 0)
            return;
    }
    uint64_t o = 0;
    for (uint32_t i = 0; i < rv->cur.m.nrec; i++)
        rv->coff[i] = o, o += rv->cur.lens[i];
    rv->peer[from].heard_ms = now_ms();
    if (rv->cur.m.type == REPL_M_ACK) {
        struct rpeer *p = &rv->peer[from];
        p->acked = G.mutant == RM_KEEPWRITTEN ? rv->cur.m.written : rv->cur.m.durable;
        p->heard = 1;
        if (rv->cur.m.written != p->wrote || !p->wrote_ms) {
            p->wrote = rv->cur.m.written;
            p->wrote_ms = now_ms();
        }
        ahead_mark(p, now_ms());
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
    if (G.sync_pause && rv->mode == M_REPLICA && now_ms() < rv->sync_pause_end) {
        rv->sync_paused = 1; /* the tick hands it back once the pause is over */
        return;
    }
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
    if (missing && G.fetch_late && strcmp(name, rv->fetch_late_name)) { /* a new name: its first FETCH waits */
        snprintf(rv->fetch_late_name, sizeof rv->fetch_late_name, "%s", name);
        rv->fetch_late_ms = now_ms();
    }
    if (missing && G.fetch_late && now_ms() - rv->fetch_late_ms < FETCH_LATE_MS)
        missing = 0;
    if (missing && G.no_fetch) {
        if (strcmp(name, rv->fetch_name))
            say("test: volume %" PRIu32 ": withholds the FETCH of %s (the marker at %" PRIu64 ")", rv->number, name,
                mpos);
        snprintf(rv->fetch_name, sizeof rv->fetch_name, "%s", name);
        missing = 0;
    }
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

/* The writer's commit point, stuck with writes above it for COMMIT_STALL_MS (rv->mu held): says once what holds it (the
 * core's view and the engine's durable end), and once that it moves again. Nothing else: a stall is either a peer that
 * the ISR check drops after isr_lag, or a bug, and this line is what tells them apart afterwards. */
static void commit_watch(struct rvol *rv, uint64_t now)
{
    if (rv->mode != M_WRITER || !rv->core)
        return;
    uint64_t c = repl_commit(rv->core), w = repl_written(rv->core);
    if (c != rv->cstall_at || w <= c) {
        if (rv->cstall_said)
            say("volume %" PRIu32 ": the commit point moves again (%" PRIu64 ")", rv->number, c);
        rv->cstall_at = c, rv->cstall_ms = now, rv->cstall_said = 0;
        return;
    }
    if (rv->cstall_said || now - rv->cstall_ms < COMMIT_STALL_MS)
        return;
    rv->cstall_said = 1;
    char d[1024];
    repl_describe(rv->core, d, sizeof d);
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    uint64_t wd = db ? eng_wal_durable_fno(db) : 0;
    tsdb_cvol_unref(&db);
    say("volume %" PRIu32 ": the commit point has stayed at %" PRIu64 " for %d ms with writes above it: %s; the WAL is "
        "durable to %" PRIu64 ", handed to the core %" PRIu64, rv->number, c, COMMIT_STALL_MS, d, wd, rv->synced_rep);
}

/* ------------------------------------------------------------------ deposed */

static int core_build(struct rvol *rv, bool unsure, uint64_t *log_end_out, struct repl **core_out);

/* The caps of the replica the next open on this thread makes (replica_keep_next): its open flushes the segments it
 * replays, and a deposed writer's or a restarted replica's must stay for the members that lag it (design-cluster.md
 * 8.3), not go by the floor before the first save sets the caps */
static void replica_caps_next(struct rvol *rv)
{
    pthread_mutex_lock(&rv->mu);
    uint64_t capb = (uint64_t)rv->rec.repl_retain_mb << 20, capms = (uint64_t)rv->rec.repl_retain_s * 1000;
    pthread_mutex_unlock(&rv->mu);
    replica_keep_next(capb, capms);
}

static void demote_between(void) { crash_point(CP_DEPOSE_BEFORE_REOPEN); }

static int demote_fn(void *ctx, struct eng **old, struct eng **neu)
{
    const uint64_t *floor = ctx;
    return replica_demote(old, *floor, demote_between, neu);
}

/* The core stepped down (a FENCE or REFUSE with a higher epoch, a REPLY REPL_MOVED; design-cluster.md 6.3) while this
 * node was the writer or promoting: the writer is deposed (its waiting writers end indeterminate, D12: tsdbd closes
 * their connections; new writes are refused), its engine closed without a flush and the directory opened as a replica,
 * whose open cuts the tail the commit point never passed; a new core from REPL (led stays the epoch it led: it never
 * resumes it) then follows the new leader, whose QUERY/ANSWER cuts what its history does not hold. rv->mu held; let go
 * of across the swap and the core's build (both read the engine: buf_lock is above rv->mu). While it runs rv->core is
 * NULL: op_admit refuses, op_synced and the heartbeat's ISR requests skip it, and the engine is a replica's from the
 * swap on, which calls no jr hook. */
static void demote(struct rvol *rv)
{
    struct repl *old = rv->core;
    uint64_t floor = rv->st.floor, epoch = rv->epoch;
    int was = rv->mode;
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    if (db && was == M_WRITER)
        eng_leader_depose(db);
    tsdb_cvol_unref(&db);
    rv->core = NULL;
    rv->mode = M_REPLICA;
    rv->isr_want = 0;
    rv->synced_rep = rv->held_commit = rv->epoch_pos = 0;
    rv->cstall_at = rv->cstall_ms = 0;
    rv->cstall_said = rv->short_said = rv->snap_said = 0;
    for (int i = 0; i < REPL_MAX_NODES; i++) {
        struct rpeer *p = &rv->peer[i];
        p->sent = p->want = p->sent_ms = p->acked = p->gen = p->wrote = p->wrote_ms = p->ahead_ms = 0;
    }
    rv->nidx = 0;
    rv->idx_end = 0;
    rv->nhist = 0;
    pthread_mutex_unlock(&rv->mu);
    repl_free(old);
    uint64_t t0 = now_ms();
    replica_caps_next(rv);
    int rc = tsdb_cvol_replace(G.t, rv->number, demote_fn, &floor, CVOL_REPLICA);
    struct repl *core = NULL;
    uint64_t log_end = 0;
    if (rc == 0)
        rc = core_build(rv, false, &log_end, &core);
    pthread_mutex_lock(&rv->mu);
    if (rc != 0) {
        /* the engine is closed or a replica that no core drives: reported failed (D13), the group acts */
        say("volume %" PRIu32 ": deposed at epoch %" PRIu64 ", the reopen as a replica failed: %s", rv->number, epoch,
            tsdb_strerror(rc));
        rv->failed = 1;
        return;
    }
    rv->core = core;
    rv->epoch = repl_epoch(core);
    say("volume %" PRIu32 ": deposed at epoch %" PRIu64 "%s: a replica again (log ..%" PRIu64 ", fence %" PRIu64
        ", reopened in %" PRIu64 " ms)", rv->number, epoch, was == M_PROMOTING ? " while promoting" : "", log_end,
        rv->st.fence, now_ms() - t0);
    pthread_mutex_unlock(&rv->mu); /* post takes it */
    post(rv, IN_META, 0, NULL, 0);
    pthread_mutex_lock(&rv->mu);
}

/* One step of the volume thread that took STALL_MS or more (a promotion's or a depose's reopen: seconds on a large
 * volume or a loaded host, 7.7 s in jepsen; a slow fdatasync) is the thread's own silence, as a stop of the process
 * is (vol_main's wait): its peers' messages queued meanwhile. Caught up in ticks it would count every peer silent for
 * TICK_BACKLOG_MS, so a new leader asked for an ISR of one right after its slow promotion, which the group took for a
 * leader cut from its replicas and moved away (review F3, test_cluster promoteslow; mutant stallcatchup). */
static void long_step(struct rvol *rv, uint64_t t0, uint64_t *next_tick)
{
    uint64_t t1 = now_ms();
    if (t1 > t0 + STALL_MS && G.mutant != RM_STALLCATCHUP) {
        *next_tick = t1;
        rv->resumed_ms = t1;
    }
}

static void *vol_main(void *arg)
{
    struct rvol *rv = arg;
    uint64_t next_tick = now_ms() + TICK_MS, slice = 0;
    pthread_mutex_lock(&rv->mu);
    while (!rv->stop) {
        drain(rv);
        if (rv->failed) {
            if (!rv->div_rebuild) {
                pthread_cond_wait(&rv->cv, &rv->mu);
                continue;
            }
            rsnap_rebuild(rv); /* D8: a DIVERGED replica is rebuilt by a snapshot; asked again until installed */
            struct timespec until;
            clock_gettime(CLOCK_REALTIME, &until);
            until.tv_sec += 1;
            pthread_cond_timedwait(&rv->cv, &rv->mu, &until);
            continue;
        }
        if ((rv->mode == M_WRITER || rv->mode == M_PROMOTING) && !repl_is_leader(rv->core)) {
            demote(rv);
            continue;
        }
        /* a tick that is due goes before the inbox (r3d review L2): under a steady stream of ACKs the inbox is never
         * empty, and the ticks (catch-up of a lagging peer, resends, the ISR check) would wait for it. But after each
         * tick the inbox gets a tick's time (`slice`) even when the next one is due: a tick whose catch-up read takes
         * longer than a tick (a peer far behind: 4 MiB a read) made the next one due at once, and the ACKs of the
         * other peers waited in the inbox while the core's clock went on, until it dropped a healthy member from the
         * ISR for silence (a replica's rejoin, 2026-11-17: ISR 3 -> 2 -> 1; test_cluster rejoinslow) */
        uint64_t now = now_ms();
        rv->loop_ms = now;
        int mine = now < next_tick || (now < slice && G.mutant != RM_INBOXSTARVE);
        if (rv->in_head && mine) {
            struct item *it = rv->in_head;
            rv->in_head = it->next;
            if (!rv->in_head)
                rv->in_tail = NULL;
            inbox_took(rv, it);
            uint64_t h0 = now_ms();
            handle(rv, it);
            free(it);
            long_step(rv, h0, &next_tick);
            continue;
        }
        if (rv->sync_wanted && mine) {
            uint64_t h0 = now_ms();
            do_sync(rv);
            long_step(rv, h0, &next_tick);
            continue;
        }
        if (now >= next_tick) {
            /* The core counts isr_lag and the join hold in ticks, which must follow the wall clock: a tick that comes
             * a tick or more late (the catch-up step of the one before took that long) is the clock catching up, and
             * runs without a catch-up step of its own, after the inbox had its slice; the step runs again once the
             * clock is current. Each tick ran one step before and a late tick was dropped, so with catch-up reads of
             * X ms a core tick took X + 5 ms of wall time: with 20 ms reads a stopped member stalled the writes for
             * 5 s, not isr_lag (r3d-isr review M1; test_cluster tickwall, mutant tickstretch). A thread that never
             * gets current again (a full inbox under TSan) still runs a step once as long after the last one as that
             * one took: the steps get at least half its time while it is late, and a lagging peer is never starved
             * (the first version ran steps only on current ticks: rejoinsteady under TSan never joined in 30 s;
             * test_cluster rejoinlate, mutant stepcurrentonly) */
            const int stretch = G.mutant == RM_TICKSTRETCH;
            int behind = !stretch && now >= next_tick + TICK_MS;
            if (rv->sync_paused && now >= rv->sync_pause_end) {
                rv->sync_paused = 0;
                rv->sync_wanted = 1;
            }
            if (rv->sync_late) {
                uint64_t l = rv->sync_late;
                rv->sync_late = 0;
                synced_locked(rv, NULL, l);
            }
            /* D8, D13: a DIVERGED volume (its DIVERGED file: a settled copy failed its digest) must not stay in the
             * ISR, where a failover could promote it with the rows it lost, nor lead: it stops like a failed one
             * (deposed, quiesced, LOCAL_FAULT in the heartbeat) and the process serves the others. The open sets the
             * flag from the file, so a restart stops it again at its first tick. */
            if (G.plant_diverge)
                plant_diverge(rv);
            if (!rv->failed && G.mutant != RM_DIVERGEISR && tsdb_volume_diverged(G.t, rv->number) == 1) {
                vol_fail(rv, "the volume is DIVERGED (tsdb DIR volume diverged lists why)");
                rv->div_rebuild = rv->mode == M_REPLICA; /* a replica asks for a snapshot (rsnap_rebuild) */
                pthread_mutex_unlock(&rv->mu);
                if (G.mutant != RM_NOQUIESCE)
                    tsdb_volume_quiesce(G.t, rv->number); /* clients get UNAVAILABLE (DIVERGED) or MOVED on */
                pthread_mutex_lock(&rv->mu);
                continue;
            }
            if (G.snap_ask)
                rsnap_rebuild(rv); /* TSDB_REPLD_TEST=snapask */
            repl_tick(rv->core);
            commit_watch(rv, now);
            next_tick += TICK_MS;
            if (G.trace && now - rv->trace_ms >= 2000) {
                char tb[1536];
                size_t k = repl_debug(rv->core, tb, sizeof tb);
                for (int i = 0; i < (int)rv->rec.nrep && k < sizeof tb; i++) {
                    struct rpeer *p = &rv->peer[i];
                    int kk = snprintf(tb + k, sizeof tb - k, " | d%d sent %llu want %llu acked %llu heard %d sent_age %lld "
                                      "heard_age %lld link %d", i, (unsigned long long)p->sent,
                                      (unsigned long long)p->want, (unsigned long long)p->acked, p->heard,
                                      (long long)(now - p->sent_ms), (long long)(now - p->heard_ms),
                                      i != rv->self && link_of(rv->rec.rep[i].node, 0) != NULL);
                    k = kk > 0 ? k + (size_t)kk : sizeof tb;
                }
                fprintf(stderr, "tsdbd: replication: volume %" PRIu32 ": trace mode %d log_start %llu idx_end %llu nidx %zu: %s\n", rv->number, rv->mode,
                    (unsigned long long)rv->log_start, (unsigned long long)rv->idx_end, (size_t)rv->nidx, tb);
                rv->trace_ms = now;
            }
            if (stretch ? next_tick < now : next_tick + TICK_BACKLOG_MS < now)
                next_tick = stretch ? now + TICK_MS : now - TICK_BACKLOG_MS; /* a longer stall: that much is caught up */
            drain(rv);
            rsnap_tick(rv);
            rtr_tick(rv);
            if (rset_apply(rv) == 2) { /* a set change kept back at its META was applied now: the core gets the record */
                struct item mi = {.kind = IN_META};
                handle(rv, &mi);
            }
            if (G.late_tick && rv->mode == M_WRITER) {
                pthread_mutex_unlock(&rv->mu);
                struct timespec lt = {0, LATE_TICK_MS * 1000000L};
                nanosleep(&lt, NULL);
                pthread_mutex_lock(&rv->mu);
            }
            uint64_t cu0 = now_ms();
            int step = !behind || (cu0 >= rv->cu_next && G.mutant != RM_STEPCURRENTONLY);
            if (step && (rv->mode == M_WRITER || rv->mode == M_PROMOTING)) {
                struct eng *db = tsdb_cvol_ref(G.t, rv->number);
                if (db)
                    catch_up(rv, db);
                tsdb_cvol_unref(&db);
                if (G.slow_tick && rv->mode == M_WRITER) {
                    pthread_mutex_unlock(&rv->mu);
                    struct timespec st = {0, SLOW_TICK_MS * 1000000L};
                    nanosleep(&st, NULL);
                    pthread_mutex_lock(&rv->mu);
                }
                uint64_t cu1 = now_ms();
                rv->cu_next = cu1 + (cu1 - cu0);
            }
            slice = now_ms() + TICK_MS;
            continue;
        }
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        uint64_t ns = (uint64_t)dl.tv_nsec + (next_tick - now) * 1000000ull;
        dl.tv_sec += (time_t)(ns / 1000000000ull);
        dl.tv_nsec = (long)(ns % 1000000000ull);
        pthread_cond_timedwait(&rv->cv, &rv->mu, &dl);
        /* A wait that returns STALL_MS past its deadline was a stop of the whole process (the pause scenario stops a
         * leader and a replica together for 1.5 s): the time is not caught up in ticks, which would count every peer
         * silent for TICK_BACKLOG_MS and ask for an ISR of one, and repld_unreachable counts its peers as heard at
         * the resume, or this member names the leader it was stopped together with and a cut deposes it (test_cluster
         * jointpause, mutant stallcatchup). A busy step is not a stall: tickwall's slow reads are caught up. */
        uint64_t woke = now_ms();
        if (woke > next_tick + STALL_MS && G.mutant != RM_STALLCATCHUP) {
            next_tick = woke;
            rv->resumed_ms = woke;
        }
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
                if (G.nodes[i].snap)
                    plink_set_addr(G.nodes[i].snap, r->rep[k].peer);
            }
            continue;
        }
        if (G.nn == MAX_NODES)
            continue;
        G.nodes[i].node = r->rep[k].node;
        snprintf(G.nodes[i].addr, sizeof G.nodes[i].addr, "%s", r->rep[k].peer);
        G.nodes[i].log = peerlink_dial(G.pl, r->rep[k].node, r->rep[k].peer, PL_KIND_LOG);
        G.nodes[i].bulk = peerlink_dial(G.pl, r->rep[k].node, r->rep[k].peer, PL_KIND_BULK);
        if (G.snap_dial) /* kind 5 is dialed when a snapshot needs it (rsnap.c); the test dials it at once */
            G.nodes[i].snap = peerlink_dial(G.pl, r->rep[k].node, r->rep[k].peer, PL_KIND_SNAP);
        G.nn++;
    }
    pthread_mutex_unlock(&G.mu);
    return 0;
}
/* a leader's settle (src/eccold.c): its MK_COLD marker appended and synced, its commit point not yet come */
static void cold_marker_crash(struct eng *db, uint64_t pos)
{
    (void)db;
    (void)pos;
    crash_point(CP_COLD_AFTER_MARKER);
}


/* a replica's apply of MK_COLD (src/ecapply.c) */
static void cold_apply_crash(struct eng *db, int which)
{
    (void)db;
    crash_point(which == ECAPPLY_CRASH_BEFORE_APPLY ? CP_COLD_BEFORE_APPLY : CP_COLD_MID_INTENT);
}

void repld_init(tsdb *t, const char *dir)
{
    const char *mu = getenv("TSDB_REPLD_MUTANT");
    if (mu && !strcmp(mu, "replicadropsfloor")) /* the engine's mutant (src/replica.c), planted in tsdbd */
        tsdb_test_replica_drops_floor();
    if (mu && !strcmp(mu, "replaystartlog")) /* the engine's (D17): the replay start is the log start again */
        tsdb_test_replay_start_log();
    if (mu && !strcmp(mu, "replicadropsfiles"))
        tsdb_test_replica_drops_files();
    G.mutant = !mu                              ? RM_NONE
               : !strcmp(mu, "commitbyleader") ? RM_COMMITBYLEADER
               : !strcmp(mu, "ackbeforesync")  ? RM_ACKBEFORESYNC
               : !strcmp(mu, "floornotsaved")  ? RM_FLOORNOTSAVED
               : !strcmp(mu, "nodepose")       ? RM_NODEPOSE
               : !strcmp(mu, "noquiesce")      ? RM_NOQUIESCE
               : !strcmp(mu, "nofastpath")     ? RM_NOFASTPATH
               : !strcmp(mu, "floornottold")   ? RM_FLOORNOTTOLD
               : !strcmp(mu, "hbnoclamp")      ? RM_HBNOCLAMP
               : !strcmp(mu, "unsureignored")  ? RM_UNSUREIGNORED
               : !strcmp(mu, "admitnocheck")   ? RM_ADMITNOCHECK
               : !strcmp(mu, "nocursorup")     ? RM_NOCURSORUP
               : !strcmp(mu, "commitpastremoved") ? RM_COMMITPASTREMOVED
               : !strcmp(mu, "nominisr")       ? RM_NOMINISR
               : !strcmp(mu, "ackbelowminisr") ? RM_ACKBELOWMINISR
               : !strcmp(mu, "shipperfile")    ? RM_SHIPPERFILE
               : !strcmp(mu, "nocap")          ? RM_NOCAP
               : !strcmp(mu, "noseal")         ? RM_NOSEAL
               : !strcmp(mu, "nospill")        ? RM_NOSPILL
               : !strcmp(mu, "noinboxbound")   ? RM_NOINBOXBOUND
               : !strcmp(mu, "keepignorespeers") ? RM_KEEPIGNORESPEERS
               : !strcmp(mu, "keepwritten")    ? RM_KEEPWRITTEN
               : !strcmp(mu, "nologstartcheck") ? RM_NOLOGSTARTCHECK
               : !strcmp(mu, "stopnodepose")   ? RM_STOPNODEPOSE
               : !strcmp(mu, "lateclock")      ? RM_LATECLOCK
               : !strcmp(mu, "syncbyrep")      ? RM_SYNCBYREP
               : !strcmp(mu, "inboxwaitall")   ? RM_INBOXWAITALL
               : !strcmp(mu, "shipnoisr")      ? RM_SHIPNOISR
               : !strcmp(mu, "fastride")       ? RM_FASTRIDE
               : !strcmp(mu, "nojoinhold")     ? RM_NOJOINHOLD
               : !strcmp(mu, "keepunanswered") ? RM_KEEPUNANSWERED
               : !strcmp(mu, "fencenosync")    ? RM_FENCENOSYNC
               : !strcmp(mu, "inboxstarve")    ? RM_INBOXSTARVE
               : !strcmp(mu, "opennohold")     ? RM_OPENNOHOLD
               : !strcmp(mu, "promotenone")    ? RM_PROMOTENONE
               : !strcmp(mu, "tickstretch")    ? RM_TICKSTRETCH
               : !strcmp(mu, "stallcatchup")   ? RM_STALLCATCHUP
               : !strcmp(mu, "stepcurrentonly") ? RM_STEPCURRENTONLY
               : !strcmp(mu, "demotestaleidx") ? RM_DEMOTESTALEIDX
               : !strcmp(mu, "divergeisr")     ? RM_DIVERGEISR
               : !strcmp(mu, "setkeepold")     ? RM_SETKEEPOLD
               : !strcmp(mu, "setnoversion")   ? RM_SETNOVERSION
               : !strcmp(mu, "setdrvnoremap")  ? RM_SETDRVNOREMAP
               : !strcmp(mu, "setnextstale")   ? RM_SETNEXTSTALE
               : !strcmp(mu, "setoldofnode")   ? RM_SETOLDOFNODE
               : !strcmp(mu, "snapnoverify")   ? RM_SNAPNOVERIFY
               : !strcmp(mu, "snapsamelink")   ? RM_SNAPSAMELINK
               : !strcmp(mu, "snapnospacecheck") ? RM_SNAPNOSPACECHECK
               : !strcmp(mu, "snapsrcpromoting") ? RM_SNAPSRCPROMOTING
               : !strcmp(mu, "snapnodonebits") ? RM_SNAPNODONEBITS
               : !strcmp(mu, "snapsharedmax")  ? RM_SNAPSHAREDMAX
               : !strcmp(mu, "snapmakedown")   ? RM_SNAPMAKEDOWN
               : !strcmp(mu, "snapnowaitdone") ? RM_SNAPNOWAITDONE
               : !strcmp(mu, "snapnostale")    ? RM_SNAPNOSTALE
               : !strcmp(mu, "snapnostall")    ? RM_SNAPNOSTALL
               : !strcmp(mu, "snapnosrcdown")  ? RM_SNAPNOSRCDOWN
               : !strcmp(mu, "snapnosrcmax")   ? RM_SNAPNOSRCMAX
               : !strcmp(mu, "snapinstallstale") ? RM_SNAPINSTALLSTALE
               : !strcmp(mu, "snapsetnodefer") ? RM_SNAPSETNODEFER
               : !strcmp(mu, "snapdropafter")  ? RM_SNAPDROPAFTER
               : !strcmp(mu, "snapnoendafter") ? RM_SNAPNOENDAFTER
               : !strcmp(mu, "snapnofence")    ? RM_SNAPNOFENCE
               : !strcmp(mu, "snapdivlogend")  ? RM_SNAPDIVLOGEND
               : !strcmp(mu, "snapnoleftovers") ? RM_SNAPNOLEFTOVERS
               : !strcmp(mu, "snapstalekeep")  ? RM_SNAPSTALEKEEP
               : !strcmp(mu, "snapkeepdiverged") ? RM_SNAPKEEPDIVERGED
                                               : RM_NONE;
    if (G.mutant)
        say("MUTANT %s (tests only)", mu);
    const char *te = getenv("TSDB_REPLD_TEST");
    G.drop_bulk = te && strstr(te, "dropbulk");
    G.promote_wait = te && strstr(te, "promotewait");
    G.promote_slow = te && strstr(te, "promoteslow");
    G.open_slow = te && strstr(te, "openslow");
    G.skip_pos = te && strstr(te, "skippos");
    G.admit_hold = te && strstr(te, "admithold");
    G.stale_sent = te && strstr(te, "stalesent");
    G.set_stale = te && strstr(te, "setstale");
    G.keep_all = te && strstr(te, "keepall");
    G.set_drain_wait = te && strstr(te, "setdrainwait");
    G.trace = getenv("TSDB_REPLD_TRACE") != NULL;
    G.late_wal = te && strstr(te, "latewal");
    G.late_bytes = te && strstr(te, "latebytes");
    G.no_clock = te && strstr(te, "noclock");
    G.sync_by_hook = te && strstr(te, "syncbyhook");
    G.sync_late = te && strstr(te, "synclate");
    G.fetch_late = te && strstr(te, "fetchlate");
    G.no_fetch = te && strstr(te, "nofetch");
    G.sync_pause = te && strstr(te, "syncpause");
    G.snap_dial = te && strstr(te, "snapdial");
    atomic_store(&eccold_marker_hook, cold_marker_crash); /* r8l's midcommit: cold-after-marker */
    atomic_store(&ecapply_crash_hook, cold_apply_crash);  /* cold-before-apply, cold-mid-intent */
    if (!leader_test_marker_hook) /* jr_cold's hook right after the append: eccold_after_append passes the COLD on */
        leader_test_marker_hook = eccold_after_append;
    G.inbox_bytes = te && strstr(te, "smallinbox") ? 256u << 10 : INBOX_BYTES;
    if (G.mutant == RM_NOINBOXBOUND)
        G.inbox_bytes = SIZE_MAX;
    G.stall_inbox = te && strstr(te, "stallinbox");
    G.cut_links = te && strstr(te, "cutlinks");
    G.no_fence_reply = te && strstr(te, "nofencereply");
    G.plant_diverge = te && strstr(te, "plantdiverge");
    G.snap_ask = te && strstr(te, "snapask");
    G.slow_read = te && strstr(te, "slowread");
    G.slow_tick = te && strstr(te, "slowtick");
    G.late_tick = te && strstr(te, "latetick");
    G.slow_ack = te && strstr(te, "slowack");
    G.drain_hold = te && strstr(te, "drainhold");
    G.spill_bytes = te && strstr(te, "spill") ? 64u << 10 : SPILL_BYTES;
    if (G.mutant == RM_NOSPILL)
        G.spill_bytes = UINT64_MAX;
    G.t = t;
    snprintf(G.dir, sizeof G.dir, "%s", dir);
}

/* The index, the history and a new core from what the volume's directory and REPL hold: at the open, and after a
 * deposed leader's engine was reopened as a replica (demote). rv->nhist is empty, the index is emptied here; rv->rec is
 * set. Without
 * rv->mu (it reads the engine, whose buf_lock is above it): the caller lists the core under it. */
static int core_build(struct rvol *rv, bool unsure, uint64_t *log_end_out, struct repl **core_out)
{
    struct repl_epoch h[REPLFILE_MAX_HIST];
    uint32_t nh = 0;
    uint64_t first = 0;
    scan_segments(rv, h, &nh, &first);
    struct eng *db = tsdb_cvol_ref(G.t, rv->number);
    uint64_t log_end = db ? replica_log_end(db) : 0;
    *log_end_out = log_end;
    /* the durable end the core starts from: below a COMMIT marker whose files are not staged (replica.c's hold), not
     * the log end. A replica killed between such a marker and its file's FETCH otherwise acknowledged its whole log at
     * the restart (once its log was verified, before its first sync): the leader took it as holding the file, its keep
     * bound passed the marker, the segment and the link the file is served from went, and the replica asked for the
     * file for the rest of the run ("it needs a snapshot"; jepsen-cluster kill, kv and classes, 2026-11-18). Its match
     * claimed positions it could not apply too. test_cluster reopenhold; mutant opennohold */
    uint64_t dur = db ? replica_durable_end(db) : 0;
    uint64_t hold = dur < log_end && G.mutant != RM_OPENNOHOLD ? dur + 1 : 0;
    /* the history: REPL's entries below the first segment's, then the segments' own */
    for (uint32_t i = 0; i < rv->st.nhist; i++)
        if (!nh || rv->st.hist[i].start < h[0].start)
            hist_note(rv, rv->st.hist[i].epoch, rv->st.hist[i].start);
    for (uint32_t i = 0; i < nh; i++)
        hist_note(rv, h[i].epoch, h[i].start);
    /* The index is the log's alone, rebuilt whole. A deposed writer's engine writes the records it admitted before
     * the depose until demote's swap closes it, and an entry pushed after demote cleared the index, kept here below
     * the entries read from the log, left it unsorted with its first start above positions the log holds: idx_find
     * took them for trimmed, and the cut of the uncommitted tail at the new leader's ANSWER stopped the volume for good
     * ("the log does not retain a position above its applied end"; jepsen-cluster pause, 2026-11-22; test_cluster
     * deposetail, mutant demotestaleidx). The old engine is closed here (the swap waited for its writers), so nothing
     * pushes from here on: the entries read below are the only ones, in log order. */
    pthread_mutex_lock(&rv->mu);
    if (G.mutant != RM_DEMOTESTALEIDX)
        rv->nidx = 0;
    rv->log_start = first ? first : log_end + 1;
    if (rv->log_start < 1)
        rv->log_start = 1;
    rv->idx_end = rv->log_start - 1;
    pthread_mutex_unlock(&rv->mu);
    if (db)
        index_log(rv, db, rv->log_start, log_end);
    tsdb_cvol_unref(&db);
    if (unsure) {
        /* The surviving slot's floor may be older than the segments this replica removed: one goes only once a floor
         * at or past its end is saved (replica_floor_saved), so the log's start proves that floor. Left below it, the
         * core refuses the log ("the log starts above the commit floor") at this open and every later one. This save
         * makes the file trusted again (replfile_save clears the flag only with led = fence); kept set, every later
         * open would take the group's epoch over its own fence again and say unsure forever. Only the start of a
         * segment proves it: with none left the start is the log's end + 1, taken from fno_next, which FNO can hold
         * above every record this replica kept (a cut lowers fno_next in memory only), so the floor is left as the
         * slot has it (the core refuses an open it cannot prove, never takes positions as committed). */
        if (first && first - 1 > rv->st.floor)
            rv->st.floor = first - 1;
        rv->st.unsure = false;
        int rc = replfile_save(rv->rf, &rv->st);
        if (rc != 0) {
            say("volume %" PRIu32 ": REPL of %s is unsure and could not be saved: %s", rv->number, rv->dir,
                tsdb_strerror(rc));
            return rc;
        }
    }
    struct repl_durable d = {.rid = rv->st.rid, .fence = rv->st.fence, .led = rv->st.led, .floor = rv->st.floor,
                             .log_start = rv->log_start, .log_end = log_end, .hold = hold, .hist = rv->hist,
                             .nhist = rv->nhist};
    /* the record's settings under rv->mu: a demote builds the core with the lock let go of, while the manager thread's
     * repld_update rewrites rv->rec whole (rec_set; a TSan race in test_cluster manystuck) */
    pthread_mutex_lock(&rv->mu);
    uint32_t lag = rv->rec.isr_lag_ms ? rv->rec.isr_lag_ms : 1000;
    uint32_t rmin = rv->rec.min_isr, unans = rv->rec.unanswered_ms;
    uint64_t setv = rv->rec.set_version; /* the core's indexes are rv->rec's (rset.c changes both in one step) */
    pthread_mutex_unlock(&rv->mu);
    int min_isr = rmin && G.mutant != RM_NOMINISR ? (int)rmin : 1; /* planted nominisr: never checked */
    struct repl_config cfg = {.self = rv->self, .nnodes = rv->nnodes, .min_isr = min_isr,
                              .isr_lag_ticks = lag / TICK_MS, .resend_ticks = 200 / TICK_MS, .heartbeat_ticks = 1,
                              .idle_heartbeat_ticks = IDLE_HEARTBEAT_MS / TICK_MS,
                              .floor_ticks = 1000 / TICK_MS, .max_batch = 1u << 30, .window = 1u << 30,
                              .join_hold_ticks = G.mutant == RM_NOJOINHOLD ? 0 : lag / TICK_MS,
                              /* a member a promotion kept unanswered (§6.2) goes after unanswered_ms, not isr_lag */
                              .unanswered_ticks =
                                  G.mutant == RM_KEEPUNANSWERED ? 0 : (unans + TICK_MS - 1) / TICK_MS,
                              .rec_find = idx_find, .ctx = rv};
    *core_out = repl_open(&cfg, &d);
    if (*core_out)
        repl_set_version(*core_out, setv);
    rv->sync_pause_end = now_ms() + SYNC_PAUSE_MS;
    if (!*core_out) {
        say("volume %" PRIu32 ": the replication core refused the state of %s", rv->number, rv->dir);
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

int repld_open(const struct cluster_vol_rec *r)
{
    if (r->nrep == 0 || r->nrep > REPL_MAX_NODES || links_up(r) != 0)
        return TSDB_ERR_INVALID;
    struct rvol *rv = calloc(1, sizeof *rv);
    if (!rv)
        return TSDB_ERR_NOMEM;
    rv->number = r->number;
    rec_set(rv, r);
    snprintf(rv->dir, sizeof rv->dir, "%s/v%" PRIu32, G.dir, r->number);
    rv->self = node_index(rv, G.self);
    rv->nnodes = (int)r->nrep;
    pthread_mutex_init(&rv->mu, NULL);
    pthread_cond_init(&rv->cv, NULL);
    pthread_cond_init(&rv->in_space, NULL);
    rv->ops = (struct jr_ops){.ctx = rv, .ship = op_ship, .ship_wait = op_ship_wait, .admit = op_admit,
                              .bytes = op_bytes, .synced = op_synced};
    int dfd = open(rv->dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    bool torn = false;
    int rc = dfd < 0 ? TSDB_ERR_IO : replfile_load(dfd, &rv->rf, &rv->st, &torn);
    if (dfd >= 0)
        close(dfd);
    if (rc != 0 || rv->self < 0) {
        say("volume %" PRIu32 ": REPL of %s: %s", r->number, rv->dir, rc ? tsdb_strerror(rc) : "not a replica here");
        goto fail;
    }
    bool unsure = rv->st.unsure && G.mutant != RM_UNSUREIGNORED;
    if (unsure) {
        /* fence, led, floor and log start are not trusted (replfile.h): the group's epoch stands in for fence and
         * led (this replica never leads the epoch it may have led, and refuses everything older), the floor is
         * the engine's to find again from the leader's commit point, the log start from the segments. Saved below,
         * once the segments have said where the log starts. */
        uint64_t e = rv->st.fence > r->epoch ? rv->st.fence : r->epoch;
        rv->st.fence = rv->st.led = e;
    }
    pthread_mutex_lock(&G.mu);
    int room = G.nvols < CLUSTER_REPORT_VOLS;
    pthread_mutex_unlock(&G.mu);
    if (!room) { /* before the open: an engine without a driver would stay open, and every retry be refused */
        say("volume %" PRIu32 ": not opened: this node drives %d volumes already", r->number, CLUSTER_REPORT_VOLS);
        rc = TSDB_ERR_NOMEM;
        goto fail;
    }
    replica_caps_next(rv);
    rc = tsdb_cvol_open(G.t, r->number, rv->dir, true, rv->st.floor);
    if (rc != 0) {
        say("volume %" PRIu32 ": the open of %s failed: %s", r->number, rv->dir, tsdb_strerror(rc));
        goto fail;
    }
    if (G.open_slow) { /* a fault for tests: the engine is routable, the manager does not call the volume open yet */
        say("volume %" PRIu32 ": engine open, 3 s before the volume counts as open (openslow)", r->number);
        sleep(3);
    }
    uint64_t log_end = 0;
    rc = core_build(rv, unsure, &log_end, &rv->core); /* the thread is not started: nothing else reads rv->core */
    if (rc != 0) {
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
    if (rset_change(rv, r)) /* another set version: kept for the volume's thread (rset.c) */
        changed = 1;
    pthread_mutex_unlock(&rv->mu);
    if (changed)
        post(rv, IN_META, 0, NULL, 0);
}

void repld_fence(void *arg, uint32_t v, uint64_t fence, uint32_t from)
{
    (void)arg;
    struct rvol *rv = vol_find(v);
    if (rv && fence)
        post(rv, IN_FENCE, from, &fence, sizeof fence);
}

void repld_volcmd(void *arg, const struct cluster_volcmd *c, uint32_t from)
{
    (void)arg;
    struct rvol *rv = vol_find(c->volume);
    if (rv)
        post(rv, IN_CMD, from, c, sizeof *c);
}

int repld_isr_short(uint32_t v)
{
    if (!__atomic_load_n(&G.nshort, __ATOMIC_ACQUIRE))
        return 0; /* no volume is short: the common case, with no lock on the path of every write */
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
    /* from where the last report stopped: with more requests than one heartbeat carries (CLUSTER_REPORT_ISR) every
     * volume gets its turn, not the same first ones until they apply (r3d-isr review L4) */
    uint32_t rot = nv ? __atomic_load_n(&G.isr_rot, __ATOMIC_RELAXED) % nv : 0, i;
    for (i = 0; i < nv && n < max; i++) {
        struct rvol *rv = vs[(rot + i) % nv];
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
    if (n == max)
        __atomic_store_n(&G.isr_rot, (rot + i) % nv, __ATOMIC_RELAXED);
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
        /* a thread that has not looped for STALL_MS is stopped with its process or not yet back from it: its peers'
         * silence is its own (vol_main) */
        int stalled = G.mutant != RM_STALLCATCHUP && now > rv->loop_ms + STALL_MS;
        for (uint32_t k = 0; !rv->failed && !stalled && k < rv->rec.nrep && k < REPL_MAX_NODES; k++) {
            if ((int)k == rv->self || rv->rec.rep[k].learner)
                continue;
            uint64_t last = rv->peer[k].heard_ms ? rv->peer[k].heard_ms : rv->open_ms;
            if (G.mutant != RM_STALLCATCHUP && rv->resumed_ms > last)
                last = rv->resumed_ms;
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

int repld_leading(uint32_t v, uint64_t *epoch)
{
    struct rvol *rv = vol_find(v);
    if (!rv)
        return -1;
    pthread_mutex_lock(&rv->mu);
    int l = rv->mode == M_WRITER && !rv->failed && rv->core && repl_is_leader(rv->core);
    if (!l && rv->mode == M_PROMOTING && !rv->failed && rv->core && G.mutant != RM_PROMOTENONE)
        l = 2; /* named the leader, not the writer yet: the group must not take it for deposed (review F3) */
    if (l && epoch)
        *epoch = rv->mode == M_PROMOTING ? rv->epoch : repl_epoch(rv->core);
    pthread_mutex_unlock(&rv->mu);
    return l;
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
    if (first && G.mutant != RM_NOQUIESCE)
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

int repld_close(uint32_t v)
{
    return repld_close_end(v, NULL);
}

int repld_close_end(uint32_t v, uint64_t *log_end)
{
    if (log_end)
        *log_end = REPLD_END_UNKNOWN;
    pthread_mutex_lock(&G.mu);
    struct rvol *rv = NULL;
    for (uint32_t i = 0; i < G.nvols && !rv; i++)
        if (G.vols[i]->number == v) {
            rv = G.vols[i];
            memmove(&G.vols[i], &G.vols[i + 1], (G.nvols - i - 1) * sizeof G.vols[0]);
            G.nvols--;
        }
    pthread_mutex_unlock(&G.mu);
    if (rv) {
        pthread_mutex_lock(&rv->mu);
        rv->stop = 1;
        pthread_cond_signal(&rv->cv);
        pthread_cond_broadcast(&rv->in_space);
        pthread_mutex_unlock(&rv->mu);
        if (rv->started)
            pthread_join(rv->th, NULL);
        rv->started = 0;
        struct eng *db = tsdb_cvol_ref(G.t, v);
        if (db && rv->mode == M_WRITER)
            eng_leader_depose(db);
        tsdb_cvol_unref(&db);
        pthread_mutex_lock(&rv->mu);
        if (rv->rec_short)
            __atomic_add_fetch(&G.nshort, -1, __ATOMIC_RELEASE);
        rv->rec_short = 0;
        /* the driver is joined: nothing appends from here on, so this end is the one an install must stay above (a
         * DIVERGED replica's log is what a rebuild replaces: 0, as in rsnap.c's check before the close) */
        if (log_end && rv->failed && rv->div_rebuild && G.mutant != RM_SNAPDIVLOGEND)
            *log_end = 0;
        else if (log_end && rv->core)
            *log_end = repl_durable_end(rv->core);
        pthread_mutex_unlock(&rv->mu);
        say("volume %" PRIu32 ": driver stopped (the replica is closed)", v);
    }
    cvol_close(v);
    return 0;
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
    rsnap_quiesce(); /* the snapshot worker sends on the links: stopped before they go */
    /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket to the peerlink any more */
    if (G.pl) {
        cluster_set_peerlink(NULL);
        peerlink_free(G.pl);
        G.pl = NULL;
    }
    /* after the links: their threads call rsnap_frame and rsnap_bin_* until peerlink_free has joined them */
    for (uint32_t i = 0; i < n; i++)
        rsnap_stop(G.vols[i]);
    /* `stop`, set under each volume's mu above, keeps the engine's hooks (op_bytes, op_ship) off the freed links */
    if (G.late_wal)
        nanosleep(&(struct timespec){2, 500000000}, NULL);
}
