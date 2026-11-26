/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's cluster volume manager (docs/design-cluster.md §3.1, §8.5; DECISION D2 of r3-scope/PLAN.md).
 *
 * The cluster thread pushes the replicated volume records that name this server (cluster_set_volume_watch, a copy
 * under `mu`, never blocking); this thread does the I/O. For each record:
 *
 *   1. The directory <dir>/v<N> (server/cvolfs.c does the disk side of steps 1 and 2). A new one is made whole or
 *      not at all: <dir>/v<N>.tmp (removed first if a crash left one), META (the root's format lines, volume=N,
 *      volume_uuid=, cluster=, requires=cluster,replica), REPL (src/replfile.c: the replica instance `rid` from the
 *      record, generation 0) and FNO, each fsynced, the directory fsynced, renamed to v<N>, the root fsynced. An existing one must hold the record's
 *      rid (else it is another instance of the
 *      replica: refused, naming both) and a generation at or above the record's open_gen (else it is an older copy
 *      of the directory: refused, naming both).
 *   2. The open generation: REPL's generation + 1 and a random nonce of this open, saved (replfile_save: the older
 *      slot, fdatasync) before they are reported. The heartbeat carries (rid, gen, nonce); the metadata leader commits
 *      REPLICA_OPEN (cluster.c, propose_opens) when gen is above the record's. Until this server's copy shows exactly
 *      this open (gen and nonce) the volume is declared but not open: requests get UNAVAILABLE (RECOVERING), so
 *      nothing answers, acknowledges or leads under a generation the group has not recorded. The nonce is what tells
 *      two opens with one generation apart: a copy of the directory restored from generation G-1 and opened while
 *      this server's copy of the metadata still showed G-1 saves G again, which the group already holds from the open
 *      that the copy predates. A record at or above this open's generation that is not this open's, and a REPL whose
 *      generation equals the record's under another nonce (that stale open saved it), are an older copy: refused.
 *   3. The engine (tsdb_cvol_open): the record's leader as a writer, every other replica as a replica
 *      (eng_open_replica). Until r3d-stream wires the replication stream a leader takes no writes: it is opened as a
 *      replica too, unless the test switch TSDB_CLUSTER_WRITES=1 (environment, read once at start) says otherwise.
 *      A replica's engine answers READONLY; tsdbd turns that into UNAVAILABLE with TSDB_WHY_FENCED (cvol_why).
 *
 * A REPL that is missing, has no valid slot or cannot be read, one that belongs to another volume, cluster or node, and
 * a failed save (fail-stop for the volume: the handle is dropped, never retried) refuse the volume with a line naming
 * the path. A torn REPL (one slot damaged) is "unsure": its fence, led, floor and log start are not trusted (none is
 * used yet: the replica driver is not wired; the flag is saved with the next open, kept in `unsure` for r3a to gate on,
 * and cleared only by a save that sets fence = led = the group's epoch), and replfile skips a belt in the open
 * generation. The older-copy rule holds on the slot that was read, belt removed: a torn REPL is accepted only when
 * that slot is one open behind the record (rot of the newest slot), one open and a belt behind (the save of the record
 * followed a torn load) or at or above it. A copy exactly one open behind whose newest slot is damaged looks like rot
 * of the current copy: it is let in, reported unsure.
 *
 * A received snapshot's install (server/snapinst.c, R4 plan D5, D6, D18) runs on the snapshot worker through
 * cvol_install, which takes the volume from this manager for its length (MV_INSTALL: requests get UNAVAILABLE
 * RECOVERING, the manager leaves the volume alone, the old open is still what the heartbeat reports). Which path a
 * volume takes depends on what is open: an open one (MV_OPEN) has its driver stopped and its engine closed first
 * (repld_close: tsdb_cvol_replace with nothing new, so no reference, lookup or read of the old copy is left and the
 * engine's DIVERGED flag goes with it), and only then is the old REPL read as the install's `keep`; one with no engine
 * (MV_WAIT: a fresh learner or a reset member before its open was recorded; MV_REFUSED: a volume refused at start)
 * is installed as it is, with the REPL on disk as `keep` if there is one. Only a driver asks for a snapshot
 * (rsnap_need, rsnap_rebuild need its rvol), so that second path is taken only by a copy that completes after its
 * driver was closed and before the volume was opened again (a set change or a reset during the transfer): a fresh
 * learner opens an empty engine and asks from it (the first path), and a volume refused at start never asks; it is
 * not driven by a test (a requester without a driver is R4's open item, docs/tasks/r4-xferinst-followups.md). The
 * install compares P with the log end the joined driver left (repld_close_end): the driver appends until it is
 * stopped, and the manager's M.op can hold the install that long. Either way the volume then goes through
 * steps 1-3 again as for a new record (MV_REPREP: cvolfs_prepare, whose snapinst_leftovers finishes what a crash left,
 * a new open generation, REPLICA_OPEN waited for, repld_open with the installed REPL's log start, floor and history),
 * whether the install was done, refused or failed: nothing is served from the volume before that open is recorded.
 * The install's fn removes a copy it did not install before the hand-back (or leaves it with its SNAPDONE for
 * snap_leftovers): the prepare that follows runs snap_leftovers on v<N>.snap, which must not meet a removal by
 * another thread. While MV_INSTALL and MV_REPREP the heartbeat reports the volume OK with its old open generation (a
 * DIVERGED replica being rebuilt too, its driver gone): it is out of the ISR, so nothing counts on it, but STATUS
 * and the topology say OK for the length of the install.
 * tsdb_cvol_replace never opens the installed directory, so it carries no DIVERGED over (the snapshot holds no
 * DIVERGED file: a fresh copy is not the diverged one).
 * Mutants (TSDB_CVOL_MUTANT, tests only): `noopengen` opens the engine before REPLICA_OPEN commits; `anynonce` opens
 * under a record of the right generation whatever its nonce; the others are in cvolfs.h (`samerid`, `replnonce`,
 * `nononce`, `trustunsure`, `tornstale`, `nodirsync`). Test switch: TSDB_CLUSTER_TEST stale_first_watch=1
 * lowers this server's open generations in the first push of its metadata copy by one (a copy one REPLICA_OPEN
 * behind, as when the log catch-up after a restart applies in steps).
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

#include "cvol.h"
#include "cvolfs.h"
#include "repld.h"
#include "chunkpeer.h"
#include "cvolset.h"
#include "../client/tsdb_crypto.h"
#include "../src/cvol.h"
#include "../src/snapshot.h"
#include "tsdb_protocol.h"

enum { MV_NONE = 0, MV_WAIT, MV_OPEN, MV_REFUSED, MV_INSTALL /* cvol_install holds it */, MV_REPREP /* prepare again */ };

struct mvol {
    uint32_t number;
    int state;
    int writer;   /* opened as the writer */
    uint64_t rid; /* the directory's */
    uint64_t gen; /* the generation this open saved */
    uint64_t nonce; /* and its draw */
    int unsure;   /* REPL was unsure at this open: its fence, led and log start are not trusted (r3a gates on it) */
    uint64_t epoch;
    int unnamed; /* the latest push does not name this server for it (cvolset_record CVS_UNNAMED said once) */
    uint64_t rec_rid; /* the rid the record gave this server when prepare ran (refused or not): CVS_RESET compares it */
    int installed;    /* prepared again after an installed snapshot (TSDB_REPLD_MUTANT=snapservebeforeopen opens it at once) */
};

static struct {
    pthread_mutex_t op; /* above mu: the manager holds it per record, cvol_install across its close and its hand-back */
    int inst_closed;    /* under op: cvol_stop ran, no install takes a volume any more */
    int servebeforeopen; /* TSDB_REPLD_MUTANT=snapservebeforeopen */
    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t th;
    int started, stop, dirty, active;
    int synced; /* the manager has gone through the volume records of a metadata copy once (every one declared) */
    tsdb *t;
    char dir[4096];
    _Atomic uint32_t self; /* written by the watch (under mu), read by the manager thread without it */
    uint64_t incarnation; /* of this run: REPL says which run saved last (cvolfs.c) */
    struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS]; /* the latest push */
    uint32_t nrecs;
    struct mvol vols[CLUSTER_REPORT_VOLS]; /* what this manager made of them; under mu */
    uint32_t nvols;
    int writes;  /* TSDB_CLUSTER_WRITES=1 */
    int replica_reads; /* TSDB_REPLICA_READS=1: a replica with a lease answers reads (tests only, until REPLICA_OK) */
    int mutant;  /* 1 noopengen, 3 anynonce, 4 refusedsilent (the others are cvolfs.h's) */
    bool divergeisr; /* TSDB_REPLD_MUTANT=divergeisr: the planted "DIVERGED stays healthy" skips this report too */
    int stale_first; /* TSDB_CLUSTER_TEST stale_first_watch=1, until the first push */
    unsigned unreach_ms; /* UNREACH_MS, or TSDB_CVOL_TEST unreach_ms=N (tests only: a report later than the leader's
                          * shrink request, which cluster.c's cut_move holds for, test_cluster cuthold) */
} M = {.op = PTHREAD_MUTEX_INITIALIZER, .mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER};

#define say cvolfs_say

static struct mvol *mv_get(uint32_t number)
{
    for (uint32_t i = 0; i < M.nvols; i++)
        if (M.vols[i].number == number)
            return &M.vols[i];
    return NULL;
}

/* step 1 and 2 for a record not seen yet (the disk side is cvolfs_prepare); the new state under mu */
static void prepare(const struct cluster_vol_rec *r)
{
    struct mvol m = {.number = r->number, .state = MV_REFUSED, .epoch = r->epoch, .rec_rid = r->rid};
    pthread_mutex_lock(&M.mu);
    uint32_t self = M.self;
    int stale_first = M.stale_first;
    pthread_mutex_unlock(&M.mu);
    int rc = tsdb_cvol_declare(M.t, r->number, r->name);
    if (rc != 0) {
        say("volume %" PRIu32 " (%s): cannot be declared: %s", r->number, r->name, tsdb_strerror(rc));
    } else {
        struct cvolfs_rec fr = {.number = r->number,
                                .node = self,
                                .incarnation = M.incarnation,
                                .rid = r->rid,
                                .open_gen = r->open_gen,
                                .open_nonce = r->open_nonce,
                                .stale_first = stale_first};
        struct cvolfs_open o;
        snprintf(fr.name, sizeof fr.name, "%s", r->name);
        memcpy(fr.uuid, r->uuid, 16);
        memcpy(fr.cluster, r->cluster, 16);
        if (cvolfs_prepare(M.dir, &fr, &o) == 0) {
            m.rid = o.rid;
            m.gen = o.gen;
            m.nonce = o.nonce;
            m.unsure = o.unsure;
            m.state = MV_WAIT;
            say("volume %" PRIu32 " (%s): open generation %" PRIu64 ", waiting for the metadata group to record it",
                r->number, r->name, m.gen);
        }
    }
    int stored = 0;
    pthread_mutex_lock(&M.mu);
    struct mvol *old = mv_get(r->number); /* MV_REPREP: prepared again after an install */
    if (old) {
        m.installed = old->installed;
        *old = m;
        stored = 1;
    } else if (M.nvols < CLUSTER_REPORT_VOLS) {
        M.vols[M.nvols++] = m;
        stored = 1;
    }
    pthread_mutex_unlock(&M.mu);
    if (stored && m.state == MV_WAIT)
        cluster_beat_soon(); /* report the new open generation at the next duty tick instead of waiting heartbeat_ms */
}

/* step 3 once the generation is recorded */
static void open_engine(const struct cluster_vol_rec *r, struct mvol *m)
{
    char vdir[4200];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, M.dir, r->number);
    int writer = r->leader == M.self && M.writes;
    /* replication (server/repld.c) opens it as a replica with REPL's floor; the record's leader promotes itself */
    int rc = chunkpeer_cluster_prepare(r->number, vdir); /* r8f: an ec volume's META asks for the peer cold ops */
    if (rc == 0)
        rc = M.writes ? tsdb_cvol_open(M.t, r->number, vdir, !writer, 0) : repld_open(r);
    pthread_mutex_lock(&M.mu);
    if (rc == 0) {
        m->state = MV_OPEN;
        m->writer = writer;
    } else {
        m->state = MV_REFUSED;
    }
    pthread_mutex_unlock(&M.mu);
    if (rc == 0)
        say("volume %" PRIu32 " (%s): open as the %s, generation %" PRIu64, r->number, r->name,
            writer ? "writer" : r->leader == M.self ? "leader (writes off until replication: a replica)" : "replica",
            m->gen);
    else
        say("volume %" PRIu32 " (%s): the open of %s failed: %s", r->number, r->name, vdir, tsdb_strerror(rc));
}

/* cvolset_record's CVS_AGAIN (a replica reset, or a committed removal): the manager drops what it made of the volume, so
 * that prepare runs again on this push (cvolset_prepare sets the old directory aside). cvolset_close closes what is open
 * and sets a removed directory aside; the volume is kept when that fails. The new state: MV_NONE, or `st` when kept. */
static int forget(uint32_t number, int st)
{
    if (st != MV_NONE && cvolset_close(number) != 0)
        return st;
    pthread_mutex_lock(&M.mu);
    struct mvol *m = mv_get(number);
    if (m)
        *m = M.vols[--M.nvols];
    pthread_mutex_unlock(&M.mu);
    return MV_NONE;
}

static void *mgr_main(void *arg)
{
    (void)arg;
    static struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS];
    pthread_mutex_lock(&M.mu);
    for (;;) {
        while (!M.dirty && !M.stop)
            pthread_cond_wait(&M.cv, &M.mu);
        if (M.stop)
            break;
        M.dirty = 0;
        uint32_t n = M.nrecs;
        int first = !M.synced;
        memcpy(recs, M.recs, n * sizeof recs[0]);
        pthread_mutex_unlock(&M.mu);
        if (first && !M.writes)
            cvolset_sweep(M.dir); /* D12: a v<N> of a removal committed while this server was down is set aside */
        for (uint32_t i = 0; i < n && !M.stop; i++) {
            const struct cluster_vol_rec *r = &recs[i];
            pthread_mutex_lock(&M.op);
            pthread_mutex_lock(&M.mu);
            struct mvol *m = mv_get(r->number);
            int st = m ? m->state : MV_NONE;
            if (st == MV_INSTALL) { /* cvol_install's until it hands the volume back */
                pthread_mutex_unlock(&M.mu);
                pthread_mutex_unlock(&M.op);
                continue;
            }
            if (m)
                m->epoch = r->epoch;
            uint64_t mrid = m ? m->rec_rid : 0;
            if (m)
                m->unnamed = 0;
            pthread_mutex_unlock(&M.mu);
            if (st == MV_NONE)
                cvolset_record(CVS_NAMED, r->number, r);
            else if (mrid && r->rid && r->rid != mrid && cvolset_record(CVS_RESET, r->number, r) == CVS_AGAIN)
                st = forget(r->number, st);
            if (st == MV_NONE || st == MV_REPREP)
                prepare(r);
            else if (st == MV_OPEN && !M.writes)
                repld_update(r);
            pthread_mutex_lock(&M.mu);
            m = mv_get(r->number);
            int ready = 0, older = 0;
            uint64_t gen = 0;
            if (m && m->state == MV_WAIT) {
                gen = m->gen;
                if (M.mutant == 1 || (r->open_gen == m->gen && (r->open_nonce == m->nonce || M.mutant == 3)) ||
                    (r->open_gen > m->gen && M.mutant == 3) || (M.servebeforeopen && m->installed))
                    ready = 1;
                else if (r->open_gen >= m->gen)
                    older = 1, m->state = MV_REFUSED; /* the group holds another open at or above this one's */
            }
            pthread_mutex_unlock(&M.mu);
            if (ready)
                open_engine(r, m);
            if (older)
                say("volume %" PRIu32 " (%s): the metadata group recorded open generation %" PRIu64 " of another open"
                    " (this one saved %" PRIu64 "): an older copy of the directory, not opened",
                    r->number, r->name, r->open_gen, gen);
            pthread_mutex_unlock(&M.op);
        }
        /* R4 (cvolset.h): the volumes this manager knows that the push no longer names, each said once */
        uint32_t gone[CLUSTER_REPORT_VOLS], ngone = 0;
        pthread_mutex_lock(&M.mu);
        for (uint32_t k = 0; k < M.nvols && !M.stop; k++) {
            uint32_t i = 0;
            while (i < n && recs[i].number != M.vols[k].number)
                i++;
            /* a push holds at most CLUSTER_REPORT_VOLS records, the lowest numbers: one cut there names nothing past
             * its last, so a volume up there may still be named (cvolset_unnamed) */
            if (i == n && !M.vols[k].unnamed && cvolset_unnamed(recs, n, M.vols[k].number))
                M.vols[k].unnamed = 1, gone[ngone++] = M.vols[k].number;
        }
        pthread_mutex_unlock(&M.mu);
        for (uint32_t k = 0; k < ngone; k++)
            if (cvolset_record(CVS_UNNAMED, gone[k], NULL) == CVS_AGAIN) { /* a committed removal (cvolset_mgr.c) */
                pthread_mutex_lock(&M.mu);
                struct mvol *gm = mv_get(gone[k]);
                int gst = gm ? gm->state : MV_NONE;
                pthread_mutex_unlock(&M.mu);
                forget(gone[k], gst);
            }
        pthread_mutex_lock(&M.mu);
        M.synced = 1;
        if (n)
            M.stale_first = 0; /* the manager has acted on a lowered push: later ones are as they are */
    }
    pthread_mutex_unlock(&M.mu);
    return NULL;
}

static void on_watch(void *arg, uint32_t self, const struct cluster_vol_rec *v, uint32_t n)
{
    (void)arg;
    pthread_mutex_lock(&M.mu);
    M.self = self;
    if (n > CLUSTER_REPORT_VOLS)
        n = CLUSTER_REPORT_VOLS;
    memcpy(M.recs, v, n * sizeof *v);
    M.nrecs = n;
    for (uint32_t i = 0; M.stale_first && i < n; i++)
        if (M.recs[i].open_gen)
            M.recs[i].open_gen--;
    M.dirty = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
}

#define UNREACH_MS 1000 /* a peer unheard this long on every data link is reported unreachable (repld heartbeats: 5 ms) */
static void on_report(void *arg, struct cluster_report *out)
{
    (void)arg;
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < M.nvols && out->nvols < CLUSTER_REPORT_VOLS; i++) {
        const struct mvol *m = &M.vols[i];
        /* a refused volume (no valid REPL, an older copy, a failed open) is LOCAL_FAULT, D13: when this server leads
         * it, nothing else would tell the group (the server is up, and no driver runs to resign) */
        if (m->state == MV_NONE || (m->state == MV_REFUSED && M.mutant == 4))
            continue;
        struct cluster_vol_report *v = &out->vols[out->nvols++];
        v->volume = m->number;
        int writer = m->writer || (m->state == MV_OPEN && tsdb_cvol_role(M.t, m->number) == CVOL_WRITER);
        v->role = m->state == MV_OPEN && writer ? CL_ROLE_OWNER : CL_ROLE_NONE;
        v->health = m->state == MV_REFUSED ? CL_HEALTH_LOCAL_FAULT : CL_HEALTH_OK;
        v->epoch = m->epoch;
        v->rid = m->rid;
        v->open_gen = m->gen;
        v->open_nonce = m->nonce;
    }
    pthread_mutex_unlock(&M.mu);
    for (uint32_t i = 0; i < out->nvols; i++) { /* after M.mu: the driver's locks are never taken under it */
        if (repld_failed(out->vols[i].volume))
            out->vols[i].health = CL_HEALTH_LOCAL_FAULT; /* D13: reported in the next heartbeat (r3e-failover acts) */
        /* D8: a DIVERGED volume (its file, read by the open) is LOCAL_FAULT from the first heartbeat after the open,
         * not only once the driver's tick has stopped it: in between, a restarted replica would report OK */
        if (!M.divergeisr && M.t && tsdb_volume_diverged(M.t, out->vols[i].volume) == 1)
            out->vols[i].health = CL_HEALTH_LOCAL_FAULT;
        uint64_t pe = 0;
        int l = repld_leading(out->vols[i].volume, &pe); /* the driver's writer only while its core leads: a deposed */
        if (l >= 0)                                       /* one is no OWNER (the Raft leader's lease check) */
            out->vols[i].role = l == 1 ? CL_ROLE_OWNER : l == 2 ? CL_ROLE_PROMOTING : CL_ROLE_NONE;
        if (l == 2)
            out->vols[i].epoch = pe; /* the epoch it promotes to (leader_deposed in cluster.c) */
    }
    out->nunreach = repld_unreachable(out->unreach, CLUSTER_REPORT_UNREACH, M.unreach_ms);
    out->nisr = repld_isr_requests(out->isr, CLUSTER_REPORT_ISR);
}

int cvol_why(uint32_t v)
{
    if (!M.active)
        return -1;
    int why = -1;
    pthread_mutex_lock(&M.mu);
    const struct mvol *m = mv_get(v);
    int open = m && m->state == MV_OPEN, writer = m && m->writer;
    if (m && !open)
        why = TSDB_WHY_RECOVERING;
    pthread_mutex_unlock(&M.mu);
    if (open && !writer && repld_leading(v, NULL) == 1)
        writer = 1; /* promoted by the replication driver, and not deposed since */
    if (open) /* led here: answered only under a read lease (§7.1; reads and writes alike) */
        why = !writer ? TSDB_WHY_FENCED : cluster_lease_valid() ? -1 : TSDB_WHY_UNREACHABLE;
    return why;
}

int cvol_replica_reads(void) { return M.replica_reads; }

int cvol_lead(uint32_t v, uint64_t *epoch)
{
    if (epoch)
        *epoch = 0;
    if (!M.active)
        return 0;
    pthread_mutex_lock(&M.mu);
    const struct mvol *m = mv_get(v);
    int open = m && m->state == MV_OPEN, writer = m && m->writer;
    pthread_mutex_unlock(&M.mu);
    uint64_t e = 0;
    if (open && !writer && repld_leading(v, &e) == 1) {
        writer = 1; /* promoted by the replication driver (as in cvol_why) */
        if (epoch)
            *epoch = e;
    }
    return open && writer;
}

int cvol_led(uint32_t v) { return cvol_lead(v, NULL); }

int cvol_no_owner(uint32_t v)
{
    if (!M.active)
        return 0;
    int no = 0;
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < M.nrecs; i++)
        if (M.recs[i].number == v) {
            no = M.recs[i].no_owner && M.recs[i].leader != M.self;
            break;
        }
    pthread_mutex_unlock(&M.mu);
    return no;
}

int cvol_active(void) { return M.active; }

int cvol_synced(void)
{
    pthread_mutex_lock(&M.mu);
    int s = M.synced;
    pthread_mutex_unlock(&M.mu);
    return s;
}

int cvol_start(tsdb *t, const char *dir)
{
    M.t = t;
    snprintf(M.dir, sizeof M.dir, "%s", dir);
    repld_init(t, dir);
    while (M.incarnation == 0 && tsdb_random(&M.incarnation, sizeof M.incarnation) == 0)
        ;
    M.unreach_ms = UNREACH_MS;
#ifdef TSDB_TEST_HOOKS
    const char *w = getenv("TSDB_CLUSTER_WRITES");
    M.writes = w && !strcmp(w, "1");
    const char *rr = getenv("TSDB_REPLICA_READS");
    M.replica_reads = rr && !strcmp(rr, "1");
    const char *rmu = getenv("TSDB_REPLD_MUTANT");
    M.divergeisr = rmu && !strcmp(rmu, "divergeisr");
    M.servebeforeopen = rmu && !strcmp(rmu, "snapservebeforeopen");
    const char *mu = getenv("TSDB_CVOL_MUTANT");

    M.mutant = mu && !strcmp(mu, "noopengen")       ? 1
               : mu && !strcmp(mu, "anynonce")      ? 3
               : mu && !strcmp(mu, "refusedsilent") ? 4
                                                    : 0;
    cvolfs_test_mutant = !mu                         ? CVOLFS_MUT_NONE
                         : !strcmp(mu, "samerid")     ? CVOLFS_MUT_SAMERID
                         : !strcmp(mu, "replnonce")   ? CVOLFS_MUT_REPLNONCE
                         : !strcmp(mu, "nononce")     ? CVOLFS_MUT_NONONCE
                         : !strcmp(mu, "trustunsure") ? CVOLFS_MUT_TRUSTUNSURE
                         : !strcmp(mu, "tornstale")   ? CVOLFS_MUT_TORNSTALE
                         : !strcmp(mu, "nodirsync")   ? CVOLFS_MUT_NODIRSYNC
                         : !strcmp(mu, "staleopen")   ? CVOLFS_MUT_STALEOPEN
                         : !strcmp(mu, "removenodirsync") ? CVOLFS_MUT_REMOVENODIRSYNC
                         : !strcmp(mu, "removedopen") ? CVOLFS_MUT_REMOVEDOPEN
                         : !strcmp(mu, "makemissing") ? CVOLFS_MUT_MAKEMISSING
                         : !strcmp(mu, "lostasstale") ? CVOLFS_MUT_LOSTASSTALE
                                                      : CVOLFS_MUT_NONE;
    const char *ct = getenv("TSDB_CLUSTER_TEST");
    M.stale_first = ct && strstr(ct, "stale_first_watch=1");
    const char *cv = getenv("TSDB_CVOL_TEST");
    if (cv && sscanf(cv, "unreach_ms=%u", &M.unreach_ms) == 1)
        say("TSDB_CVOL_TEST: a peer is reported unreachable after %u ms (tests only)", M.unreach_ms);
    if (M.writes)
        say("TSDB_CLUSTER_WRITES=1: a volume's leader takes writes before replication is wired (tests only)");
    if (M.replica_reads)
        say("TSDB_REPLICA_READS=1: a replica holding a read lease answers reads, which may be stale (tests only)");
    if (M.mutant || cvolfs_test_mutant)
        say("MUTANT %s (tests only)", mu);
#endif
    if (pthread_create(&M.th, NULL, mgr_main, NULL) != 0) {
        say("cannot start the volume manager thread");
        return -1;
    }
    M.started = 1;
    M.active = 1;
    cluster_set_volume_watch(on_watch, NULL);
    cluster_set_fence_handler(repld_fence, NULL);
    cluster_set_drv_counts(repld_transfer_counts);
    cluster_set_volcmd_handler(repld_volcmd, NULL);
    cluster_set_reporter(on_report, NULL);
    return 0;
}

int cvol_install(uint32_t v, int (*fn)(void *ctx, const struct snap_ident *id, int had_engine, uint64_t log_end),
                 void *ctx)
{
    uint64_t log_end = REPLD_END_UNKNOWN;
    struct snap_ident id = {0};
    pthread_mutex_lock(&M.op);
    pthread_mutex_lock(&M.mu);
    struct mvol *m = M.inst_closed || M.stop || M.writes ? NULL : mv_get(v);
    const struct cluster_vol_rec *r = NULL;
    for (uint32_t i = 0; m && i < M.nrecs && !r; i++)
        if (M.recs[i].number == v)
            r = &M.recs[i];
    int st = m ? m->state : MV_NONE;
    if (!r || (st != MV_OPEN && st != MV_WAIT && st != MV_REFUSED)) {
        pthread_mutex_unlock(&M.mu);
        pthread_mutex_unlock(&M.op);
        return CVOL_NOT_TAKEN;
    }
    memcpy(id.volume_uuid, r->uuid, 16);
    memcpy(id.cluster_uuid, r->cluster, 16);
    id.node = M.self;
    id.incarnation = M.incarnation;
    id.rid = r->rid ? r->rid : m->rid;
    m->state = MV_INSTALL; /* from here on requests get RECOVERING (cvol_why) and the manager skips the volume */
    pthread_mutex_unlock(&M.mu);
    if (st == MV_OPEN)
        repld_close_end(v, &log_end); /* the driver stopped (no REPL save, no append after this) and the engine
                                       * closed: no read of the old copy */
    pthread_mutex_unlock(&M.op);
    int rc = fn(ctx, &id, st == MV_OPEN, log_end); /* fn leaves nothing of the copy for after the hand-back below */
    pthread_mutex_lock(&M.op);
    pthread_mutex_lock(&M.mu);
    if ((m = mv_get(v)) != NULL) {
        m->state = MV_REPREP; /* steps 1-3 again on the next push, as for a new record */
        m->installed = rc == 0;
    }
    M.dirty = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
    pthread_mutex_unlock(&M.op);
    return rc;
}

void cvol_stop(void)
{
    if (!M.started)
        return;
    pthread_mutex_lock(&M.mu);
    M.stop = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
    pthread_join(M.th, NULL);
    pthread_mutex_lock(&M.op); /* an install inside repld_close is waited for; later ones take nothing */
    M.inst_closed = 1;
    pthread_mutex_unlock(&M.op);
    M.started = 0;
    repld_stop(); /* after the cluster thread (tsdbd stops it first) and the manager: nothing opens or updates */
}
