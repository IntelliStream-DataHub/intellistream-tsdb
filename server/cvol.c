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
#include "disks.h"
#include "repld.h"
#include "scrubd.h"
#include "chunkpeer.h"
#include "cvolset.h"
#include "../client/tsdb_crypto.h"
#include "../src/compat.h"
#include "../src/cvol.h"
#include "../src/internal.h"
#include "../src/snapshot.h"
#include "tsdb_protocol.h"

enum { MV_NONE = 0, MV_WAIT, MV_OPEN, MV_REFUSED, MV_INSTALL /* cvol_install holds it */, MV_REPREP /* prepare again */ };

#define WAL_RATE_BUCKETS 60 /* 60 x 1 min = 1 hour sliding window (E51) */

struct mvol {
    uint32_t number;
    int state;
    uint8_t fault_reason; /* enum cluster_fault_reason when state == MV_REFUSED (R9, D6) */
    int opened_once;      /* 1 once open_engine ran for this volume in this process */
    int writer;   /* opened as the writer */
    uint64_t rid; /* the directory's */
    uint64_t gen; /* the generation this open saved */
    uint64_t nonce; /* and its draw */
    int unsure;   /* REPL was unsure at this open: its fence, led and log start are not trusted (r3a gates on it) */
    uint64_t epoch;
    int unnamed; /* the latest push does not name this server for it (cvolset_record CVS_UNNAMED said once) */
    uint64_t rec_rid; /* the rid the record gave this server when prepare ran (refused or not): CVS_RESET compares it */
    int installed;    /* prepared again after an installed snapshot (TSDB_REPLD_MUTANT=snapservebeforeopen opens it at once) */
    char disk_root[PATH_MAX]; /* R13 (D14): disk root directory holding v<N> */
    uint8_t store_id[16];     /* R13 (D14): disk store_id holding v<N> (reported in HBS_VOLUMES) */
    int refused_disk_down;    /* R13 (r13-fault): 1 when MV_REFUSED solely because its disk was non-UP */
    uint64_t ec_reopen_ns;    /* R10: last monotonic ns of follower EC-attach reopen (rate-limits retries) */
    uint32_t ec_reopen_tries; /* R10: consecutive EC-attach reopen attempts without ec_cluster (caps retry loops) */
    /* E51: 1-hour sliding window of WAL bytes written by this replica. */
    uint64_t wal_last_bytes;
    uint64_t wal_first_s;
    uint64_t wal_last_min;
    uint64_t wal_buckets[WAL_RATE_BUCKETS];
    double wal_bps;
};

static struct {
    pthread_mutex_t op; /* above mu: the manager holds it per record, cvol_install across its close and its hand-back */
    int inst_closed;    /* under op: cvol_stop ran, no install takes a volume any more */
    int servebeforeopen; /* TSDB_REPLD_MUTANT=snapservebeforeopen */
    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t th;
    int started, stop, dirty, active, disks_ready;
    int synced; /* the manager has gone through the volume records of a metadata copy once (every one declared) */
    uint32_t init_vols[CLUSTER_REPORT_VOLS]; /* R9 (D5): volumes held at startup (first push) */
    uint32_t ninit;
    int init_done; /* 1 once every startup volume finished its first open (opened or refused); never cleared again */
    unsigned slow_open_ms; /* TSDB_CLUSTER_TEST slow_open_ms=N (tests only) */
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

static uint64_t mono_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec;
}

static int store_id_nonzero(const uint8_t id[16])
{
    uint8_t acc = 0;
    for (int i = 0; i < 16; i++)
        acc |= id[i];
    return acc != 0;
}

/* Refreshes each open volume's 1-hour WAL ingest rate (bytes/s) off M.mu (mgr_main thread only). */
static void sample_wal_rates(void)
{
    uint32_t nums[CLUSTER_REPORT_VOLS];
    uint32_t n = 0;
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < M.nvols && n < CLUSTER_REPORT_VOLS; i++)
        if (M.vols[i].state == MV_OPEN)
            nums[n++] = M.vols[i].number;
    pthread_mutex_unlock(&M.mu);
    if (n == 0 || !M.t)
        return;
    uint64_t cur_bytes[CLUSTER_REPORT_VOLS];
    uint8_t have[CLUSTER_REPORT_VOLS];
    for (uint32_t i = 0; i < n; i++) {
        struct eng *db = tsdb_cvol_ref(M.t, nums[i]);
        if (db) {
            cur_bytes[i] = atomic_load_explicit(&db->st_wal_bytes, memory_order_relaxed);
            have[i] = 1;
            tsdb_cvol_unref(&db);
        } else {
            cur_bytes[i] = 0;
            have[i] = 0;
        }
    }
    uint64_t now_s = mono_sec();
    uint64_t now_min = now_s / 60;
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < n; i++) {
        if (!have[i])
            continue;
        struct mvol *m = mv_get(nums[i]);
        if (!m)
            continue;
        if (m->wal_first_s == 0) {
            /* Engine opened before first sample without timestamp: baseline at cur_bytes so we never spike on 1 s. */
            m->wal_first_s = now_s ? now_s : 1;
            m->wal_last_min = now_min;
            m->wal_last_bytes = cur_bytes[i];
        } else {
            uint64_t delta = cur_bytes[i] >= m->wal_last_bytes ? cur_bytes[i] - m->wal_last_bytes : cur_bytes[i];
            m->wal_last_bytes = cur_bytes[i];
            if (now_min > m->wal_last_min) {
                uint64_t adv = now_min - m->wal_last_min;
                if (adv >= WAL_RATE_BUCKETS) {
                    memset(m->wal_buckets, 0, sizeof m->wal_buckets);
                } else {
                    for (uint64_t k = 1; k <= adv; k++)
                        m->wal_buckets[(m->wal_last_min + k) % WAL_RATE_BUCKETS] = 0;
                }
                m->wal_last_min = now_min;
            }
            m->wal_buckets[now_min % WAL_RATE_BUCKETS] += delta;
        }
        uint64_t sum = 0;
        for (int b = 0; b < WAL_RATE_BUCKETS; b++)
            sum += m->wal_buckets[b];
        uint64_t elapsed = now_s > m->wal_first_s ? now_s - m->wal_first_s : 1;
        if (elapsed > 3600)
            elapsed = 3600;
        m->wal_bps = (double)sum / (double)elapsed;
    }
    pthread_mutex_unlock(&M.mu);
}

/* Builds the cvolfs_disk table from disks_list() (falling back to M.dir) with per-disk replica counts and 1h WAL bps. */
static int build_cvolfs_disks(struct cvolfs_disk *out, int cap, uint32_t exclude_vol)
{
    int lim = cap < DISKS_MAX ? cap : DISKS_MAX;
    struct disk_info *di = calloc((size_t)lim, sizeof *di);
    int nd = di ? disks_list(di, lim) : 0;
    if (nd == 0) {
        memset(&out[0], 0, sizeof out[0]);
        snprintf(out[0].path, sizeof out[0].path, "%s", M.dir);
        out[0].disk_class = DISK_CLASS_SSD;
        out[0].state = META_STORE_UP;
        out[0].present = 1;
        nd = 1;
    } else {
        for (int i = 0; i < nd; i++) {
            memset(&out[i], 0, sizeof out[i]);
            memcpy(out[i].path, di[i].path, sizeof out[i].path);
            out[i].path[sizeof out[i].path - 1] = '\0';
            memcpy(out[i].store_id, di[i].store_id, 16);
            out[i].disk_class = di[i].disk_class;
            out[i].state = di[i].state;
            out[i].present = di[i].present;
            out[i].recovering = di[i].recovering;
            if (di[i].have_usage) {
                out[i].have_usage = 1;
                if (di[i].young_room_bytes > 0) {
                    out[i].size_bytes = di[i].young_room_bytes;
                    out[i].used_bytes = di[i].young_used_bytes;
                } else {
                    out[i].size_bytes = 1;
                    out[i].used_bytes = 1;
                }
            }
        }
    }
    free(di);
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < M.nvols; i++) {
        const struct mvol *m = &M.vols[i];
        if (m->number == exclude_vol || m->state == MV_NONE || !m->disk_root[0])
            continue;
        int nz = store_id_nonzero(m->store_id);
        for (int d = 0; d < nd; d++) {
            if ((nz && memcmp(out[d].store_id, m->store_id, 16) == 0) ||
                (!nz && strcmp(out[d].path, m->disk_root) == 0)) {
                out[d].nreplicas++;
                out[d].wal_bps += m->wal_bps;
                break;
            }
        }
    }
    pthread_mutex_unlock(&M.mu);
    return nd;
}

/* step 1 and 2 for a record not seen yet (the disk side is cvolfs_prepare_disks); the new state under mu */
static void prepare(const struct cluster_vol_rec *r)
{
    struct mvol m = {.number = r->number, .state = MV_REFUSED, .fault_reason = CL_FAULT_REFUSED,
                     .epoch = r->epoch, .rec_rid = r->rid};
    sample_wal_rates();
    pthread_mutex_lock(&M.mu);
    uint32_t self = M.self;
    int stale_first = M.stale_first;
    pthread_mutex_unlock(&M.mu);
    int rc = tsdb_cvol_declare(M.t, r->number, r->name);
    if (rc != 0) {
        if (rc == TSDB_ERR_CORRUPT)
            m.fault_reason = CL_FAULT_CORRUPT_OPEN;
        say("volume %" PRIu32 " (%s): cannot be declared: %s", r->number, r->name, tsdb_strerror(rc));
    } else {
        struct cvolfs_rec fr = {.number = r->number,
                                .node = self,
                                .incarnation = M.incarnation,
                                .rid = r->rid,
                                .open_gen = r->open_gen,
                                .open_nonce = r->open_nonce,
                                .stale_first = stale_first,
                                .young_move_pct = r->young_move_pct,
                                .log_on_hdd = r->log_on_hdd};
        struct cvolfs_open o;
        memset(&o, 0, sizeof o);
        snprintf(fr.name, sizeof fr.name, "%s", r->name);
        memcpy(fr.uuid, r->uuid, 16);
        memcpy(fr.cluster, r->cluster, 16);
        struct cvolfs_disk *disks = calloc(DISKS_MAX, sizeof *disks);
        int prc = -1;
        if (disks) {
            int nd = build_cvolfs_disks(disks, DISKS_MAX, r->number);
            prc = cvolfs_prepare_disks(M.dir, disks, nd, &fr, &o);
            free(disks);
        }
        if (prc == 0) {
            if (r->open_gen == 0)
                scrubd_vol_reset(r->number); /* fresh/reset directory: clear any preloaded damage from set-aside dir */
            m.rid = o.rid;
            m.gen = o.gen;
            m.nonce = o.nonce;
            m.unsure = o.unsure;
            snprintf(m.disk_root, sizeof m.disk_root, "%s", o.disk_root[0] ? o.disk_root : M.dir);
            memcpy(m.store_id, o.store_id, 16);
            m.state = MV_WAIT;
            m.fault_reason = CL_FAULT_NONE;
            say("volume %" PRIu32 " (%s): open generation %" PRIu64 ", waiting for the metadata group to record it",
                r->number, r->name, m.gen);
        } else if (prc == TSDB_ERR_CORRUPT) {
            m.fault_reason = CL_FAULT_CORRUPT_OPEN;
        } else if (prc == TSDB_ERR_UNAVAILABLE) {
            m.refused_disk_down = 1;
            memcpy(m.store_id, o.store_id, 16);
        }
    }
    int stored = 0;
    pthread_mutex_lock(&M.mu);
    struct mvol *old = mv_get(r->number); /* MV_REPREP: prepared again after an install */
    if (old) {
        m.installed = old->installed;
        m.wal_last_bytes = old->wal_last_bytes;
        m.wal_first_s = old->wal_first_s;
        m.wal_last_min = old->wal_last_min;
        memcpy(m.wal_buckets, old->wal_buckets, sizeof m.wal_buckets);
        m.wal_bps = old->wal_bps;
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
    char root[PATH_MAX], vdir[4200];
    pthread_mutex_lock(&M.mu);
    snprintf(root, sizeof root, "%s", m->disk_root[0] ? m->disk_root : M.dir);
#ifdef TSDB_TEST_HOOKS
    int do_slow = M.slow_open_ms > 0 && !m->opened_once;
    unsigned slow_ms = M.slow_open_ms;
#endif
    pthread_mutex_unlock(&M.mu);
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, root, r->number);
#ifdef TSDB_TEST_HOOKS
    if (do_slow) {
        say("volume %" PRIu32 " (%s): delaying first open by %u ms (TSDB_CLUSTER_TEST slow_open_ms)", r->number,
            r->name, slow_ms);
        unsigned rem = slow_ms;
        while (rem > 0) {
            pthread_mutex_lock(&M.mu);
            int stp = M.stop;
            pthread_mutex_unlock(&M.mu);
            if (stp)
                return;
            unsigned step = rem > 50 ? 50 : rem;
            struct timespec d = {0, (long)step * 1000000L};
            nanosleep(&d, NULL);
            rem -= step;
        }
    }
#endif
    int writer = r->leader == M.self && M.writes;
    /* replication (server/repld.c) opens it as a replica with REPL's floor; the record's leader promotes itself */
    int rc = chunkpeer_cluster_prepare(r->number, vdir); /* r8f: an ec volume's META asks for the peer cold ops */
    if (rc == 0)
        rc = M.writes ? tsdb_cvol_open(M.t, r->number, vdir, !writer, 0) : repld_open(r, root);
    pthread_mutex_lock(&M.mu);
    m->opened_once = 1;
    if (rc == 0) {
        m->state = MV_OPEN;
        m->fault_reason = CL_FAULT_NONE;
        m->writer = writer;
        if (m->wal_first_s == 0) {
            uint64_t now_s = mono_sec();
            m->wal_first_s = now_s ? now_s : 1;
            m->wal_last_min = m->wal_first_s / 60;
        }
        m->wal_last_bytes = 0; /* newly opened engine starts with st_wal_bytes == 0 */
    } else {
        m->state = MV_REFUSED;
        m->fault_reason = rc == TSDB_ERR_CORRUPT ? CL_FAULT_CORRUPT_OPEN : CL_FAULT_REFUSED;
    }
    pthread_mutex_unlock(&M.mu);
    if (rc == 0) {
        disk_usage_kick();
        say("volume %" PRIu32 " (%s): open as the %s, generation %" PRIu64, r->number, r->name,
            writer ? "writer" : r->leader == M.self ? "leader (writes off until replication: a replica)" : "replica",
            m->gen);
    } else
        say("volume %" PRIu32 " (%s): the open of %s failed: %s", r->number, r->name, vdir, tsdb_strerror(rc));
}

/* R9 (D5): clear HBS_NODE `opening` once every startup volume (recorded on the first push) has finished its first open
 * (opened, refused, or removed); never sets opening=1 again for volumes added later. */
static void check_init_done(void)
{
    int done = 0;
    pthread_mutex_lock(&M.mu);
    if (M.synced && !M.init_done) {
        done = 1;
        for (uint32_t i = 0; i < M.ninit; i++) {
            const struct mvol *m = mv_get(M.init_vols[i]);
            if (m && m->state != MV_OPEN && m->state != MV_REFUSED) {
                done = 0;
                break;
            }
        }
        if (done)
            M.init_done = 1;
    }
    pthread_mutex_unlock(&M.mu);
    if (done) {
        cluster_set_opening(0);
        cluster_beat_soon();
    }
}

/* cvolset_record's CVS_AGAIN (a replica reset, or a committed removal): the manager drops what it made of the volume, so
 * that prepare runs again on this push (cvolset_prepare sets the old directory aside). cvolset_close closes what is open
 * and sets a removed directory aside; the volume is kept when that fails. The new state: MV_NONE, or `st` when kept. */
static int forget(uint32_t number, int st)
{
    if (st != MV_NONE && cvolset_close(number) != 0)
        return st;
    scrubd_vol_reset(number);
    pthread_mutex_lock(&M.mu);
    struct mvol *m = mv_get(number);
    if (m)
        *m = M.vols[--M.nvols];
    pthread_mutex_unlock(&M.mu);
    return MV_NONE;
}

static void sweep_all_disks(void)
{
    int log_on_hdd = 0;
    const struct cluster_copy *cp = cluster_copy_get();
    if (cp) {
        log_on_hdd = cp->log_on_hdd != 0;
        cluster_copy_put(cp);
    }
    struct disk_info *di = calloc(DISKS_MAX, sizeof *di);
    int nd = di ? disks_list(di, DISKS_MAX) : 0;
    const char *roots[DISKS_MAX];
    int nroots = 0;
    for (int i = 0; i < nd && nroots < DISKS_MAX; i++) {
        struct cvolfs_disk cd = {.disk_class = di[i].disk_class, .state = di[i].state, .present = di[i].present};
        memcpy(cd.path, di[i].path, sizeof cd.path);
        cd.path[sizeof cd.path - 1] = '\0';
        if (cvolfs_disk_scannable(&cd, log_on_hdd))
            roots[nroots++] = di[i].path;
    }
    if (nd == 0) {
        roots[0] = M.dir;
        nroots = 1;
    }
    if (nroots > 0)
        cvolset_sweep_disks(roots, nroots);
    free(di);
}

static void *mgr_main(void *arg)
{
    (void)arg;
    static struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS];
    pthread_mutex_lock(&M.mu);
    for (;;) {
        while ((!M.dirty || !M.disks_ready) && !M.stop) {
            if (M.disks_ready && M.synced) {
                tsdb_cond_timedwait_ms(&M.cv, &M.mu, 10000);
                if (!M.dirty && !M.stop) {
                    pthread_mutex_unlock(&M.mu);
                    sample_wal_rates();
                    pthread_mutex_lock(&M.mu);
                }
            } else {
                pthread_cond_wait(&M.cv, &M.mu);
            }
        }
        if (M.stop)
            break;
        M.dirty = 0;
        uint32_t n = M.nrecs;
        int first = !M.synced;
        memcpy(recs, M.recs, n * sizeof recs[0]);
        if (first) {
            M.ninit = n;
            for (uint32_t i = 0; i < n; i++)
                M.init_vols[i] = recs[i].number;
        }
        pthread_mutex_unlock(&M.mu);
        if (first && !M.writes)
            sweep_all_disks(); /* D12/D14: v<N> of a removal committed while down is set aside per disk */
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
            else if (st == MV_OPEN && !M.writes) {
                repld_update(r);
#ifdef TSDB_EC
                /* R10 (r10-driver): when a replicas volume converts to EC, attach the EC tier cleanly.
                 * Followers (and a 1-replica leader) reopen right away so chunkpeer_cluster_prepare writes
                 * requires=tier,ec and repld_open attaches the tier; multi-replica leaders attach during the
                 * planned transfer driven by credund.c (promote_fn / demote_fn), avoiding indeterminate writes.
                 * Gated on TSDB_EC and bounded by ec_reopen_tries with exponential backoff so a failed attach
                 * cannot spin open_gen forever. */
                if (r->redundancy == META_RED_EC && r->nstripe > 0 && r->nstripe <= CHUNK_POS_MAX &&
                    (r->leader != M.self || r->nrep <= 1)) {
                    struct timespec ts_now;
                    clock_gettime(CLOCK_MONOTONIC, &ts_now);
                    uint64_t now_ns = (uint64_t)ts_now.tv_sec * 1000000000ull + (uint64_t)ts_now.tv_nsec;
                    int try_reopen = 0;
                    pthread_mutex_lock(&M.mu);
                    struct mvol *cm = mv_get(r->number);
                    if (cm && cm->ec_reopen_tries < 5) {
                        uint64_t wait_ns = cm->ec_reopen_tries ? (1000000000ull << (cm->ec_reopen_tries - 1)) : 0;
                        if (!cm->ec_reopen_ns || now_ns - cm->ec_reopen_ns >= wait_ns)
                            try_reopen = 1;
                    }
                    pthread_mutex_unlock(&M.mu);
                    struct eng *db = try_reopen ? tsdb_cvol_ref(M.t, r->number) : NULL;
                    int has_ec = db && db->ec_cluster;
                    int need_ec = db && !db->ec_cluster;
                    tsdb_cvol_unref(&db);
                    if (has_ec) {
                        pthread_mutex_lock(&M.mu);
                        struct mvol *rm = mv_get(r->number);
                        if (rm)
                            rm->ec_reopen_tries = 0;
                        pthread_mutex_unlock(&M.mu);
                    } else if (need_ec) {
                        say("volume %" PRIu32 " (%s): reopening to attach EC tier (redundancy ec=%u+%u)", r->number,
                            r->name, r->ec_k, r->ec_m);
                        pthread_mutex_lock(&M.mu);
                        struct mvol *rm = mv_get(r->number);
                        if (rm) {
                            rm->state = MV_INSTALL;
                            rm->ec_reopen_ns = now_ns;
                            rm->ec_reopen_tries++;
                        }
                        pthread_mutex_unlock(&M.mu);
                        repld_close(r->number);
                        pthread_mutex_lock(&M.mu);
                        rm = mv_get(r->number);
                        if (rm) {
                            rm->state = MV_REPREP;
                            rm->fault_reason = CL_FAULT_NONE;
                        }
                        M.dirty = 1;
                        pthread_cond_signal(&M.cv);
                        pthread_mutex_unlock(&M.mu);
                        cluster_beat_soon();
                    }
                }
#endif
            }
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
                    older = 1, m->state = MV_REFUSED,
                    m->fault_reason = CL_FAULT_REFUSED; /* the group holds another open at or above this one's */
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
        pthread_mutex_unlock(&M.mu);
        check_init_done();
        pthread_mutex_lock(&M.mu);
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
        v->fault_reason = m->state == MV_REFUSED ? m->fault_reason : CL_FAULT_NONE;
        v->epoch = m->epoch;
        v->rid = m->rid;
        v->open_gen = m->gen;
        v->open_nonce = m->nonce;
        memcpy(v->store_id, m->store_id, 16);
    }
    pthread_mutex_unlock(&M.mu);
    for (uint32_t i = 0; i < out->nvols; i++) { /* after M.mu: the driver's locks are never taken under it */
        if (repld_failed(out->vols[i].volume)) {
            out->vols[i].health = CL_HEALTH_LOCAL_FAULT; /* D13: reported in the next heartbeat (r3e-failover acts) */
            out->vols[i].fault_reason = CL_FAULT_FAILSTOP;
        }
        /* D8: a DIVERGED volume (its file, read by the open) is LOCAL_FAULT from the first heartbeat after the open,
         * not only once the driver's tick has stopped it: in between, a restarted replica would report OK */
        if (!M.divergeisr && M.t && tsdb_volume_diverged(M.t, out->vols[i].volume) == 1) {
            out->vols[i].health = CL_HEALTH_LOCAL_FAULT;
            if (out->vols[i].fault_reason == CL_FAULT_NONE)
                out->vols[i].fault_reason = CL_FAULT_FAILSTOP;
        }
        /* R12 (D7): young-part or .ec damage found by scrubd: report CL_HEALTH_DAMAGED (does not fail-stop). */
        if (out->vols[i].health == CL_HEALTH_OK && scrubd_vol_damaged(out->vols[i].volume))
            out->vols[i].health = CL_HEALTH_DAMAGED;
        uint64_t pe = 0;
        int l = repld_leading(out->vols[i].volume, &pe); /* the driver's writer only while its core leads: a deposed */
        if (l >= 0)                                       /* one is no OWNER (the Raft leader's lease check) */
            out->vols[i].role = l == 1 ? CL_ROLE_OWNER : l == 2 ? CL_ROLE_PROMOTING : CL_ROLE_NONE;
        if (l == 2)
            out->vols[i].epoch = pe; /* the epoch it promotes to (leader_deposed in cluster.c) */
        uint64_t ls = 0, le = 0;
        if (out->vols[i].health == CL_HEALTH_OK && repld_log_bounds(out->vols[i].volume, &ls, &le) == 0) {
            out->vols[i].log_start = ls;
            out->vols[i].log_end = le;
        }
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

int cvol_vol_disk(uint32_t v, char *root_out, size_t cap, uint8_t store_id_out[16])
{
    int rc = -1;
    pthread_mutex_lock(&M.mu);
    const struct mvol *m = mv_get(v);
    if (m && m->disk_root[0]) {
        if (root_out && cap > 0)
            snprintf(root_out, cap, "%s", m->disk_root);
        if (store_id_out)
            memcpy(store_id_out, m->store_id, 16);
        rc = 0;
    }
    pthread_mutex_unlock(&M.mu);
    return rc;
}

uint32_t cvol_vols_on_disk(const uint8_t store_id[16], uint32_t *vols_out, uint32_t cap)
{
    if (!store_id || !store_id_nonzero(store_id))
        return 0;
    uint32_t n = 0;
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < M.nvols && n < cap; i++) {
        const struct mvol *m = &M.vols[i];
        if (m->state == MV_NONE)
            continue;
        if (memcmp(m->store_id, store_id, 16) == 0) {
            if (vols_out)
                vols_out[n] = m->number;
            n++;
        }
    }
    pthread_mutex_unlock(&M.mu);
    return n;
}

void cvol_disks_ready(void)
{
    pthread_mutex_lock(&M.mu);
    M.disks_ready = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
}

int cvol_start(tsdb *t, const char *dir)
{
    M.t = t;
    snprintf(M.dir, sizeof M.dir, "%s", dir);
    tsdb_cond_init_mono(&M.cv);
    M.disks_ready = 0;
    repld_init(t, dir);
    while (M.incarnation == 0 && tsdb_random(&M.incarnation, sizeof M.incarnation) == 0)
        ;
    M.unreach_ms = UNREACH_MS;
    M.ninit = 0;
    M.init_done = 0;
    M.slow_open_ms = 0;
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
                         : !strcmp(mu, "cvolfirstdisk") ? CVOLFS_MUT_FIRSTDISK
                         : !strcmp(mu, "cvoldupok")   ? CVOLFS_MUT_DUPOK
                                                      : CVOLFS_MUT_NONE;
    const char *ct = getenv("TSDB_CLUSTER_TEST");
    M.stale_first = ct && strstr(ct, "stale_first_watch=1");
    if (ct) {
        const char *so = strstr(ct, "slow_open_ms=");
        if (so && sscanf(so, "slow_open_ms=%u", &M.slow_open_ms) == 1 && M.slow_open_ms)
            say("TSDB_CLUSTER_TEST: delaying each volume's first open by %u ms (tests only)", M.slow_open_ms);
    }
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
    /* R9 (D5): opening=1 from cluster thread start until every startup volume finishes its first open */
    cluster_set_opening(1);
    if (pthread_create(&M.th, NULL, mgr_main, NULL) != 0) {
        cluster_set_opening(0);
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
    if (rc == 0)
        scrubd_vol_reset(v);
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

int cvol_disk_reopen(uint32_t v)
{
    pthread_mutex_lock(&M.op);
    pthread_mutex_lock(&M.mu);
    if (M.inst_closed || M.stop || M.writes) {
        pthread_mutex_unlock(&M.mu);
        pthread_mutex_unlock(&M.op);
        return -1;
    }
    struct mvol *m = mv_get(v);
    int st = m ? m->state : MV_NONE;
    if (!m || (st != MV_OPEN && st != MV_REFUSED)) {
        pthread_mutex_unlock(&M.mu);
        pthread_mutex_unlock(&M.op);
        return 0;
    }
    m->state = MV_INSTALL; /* requests get UNAVAILABLE RECOVERING; mgr_main skips until hand-back */
    pthread_mutex_unlock(&M.mu);
    if (st == MV_OPEN)
        repld_close(v); /* stops & joins driver, waits for refs, closes engine retiring=true (no retried fsync) */
    pthread_mutex_lock(&M.mu);
    if ((m = mv_get(v)) != NULL) {
        m->state = MV_REPREP; /* prepare + REPLICA_OPEN + repld_open replay from disk as a replica */
        m->fault_reason = CL_FAULT_NONE;
    }
    M.dirty = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
    pthread_mutex_unlock(&M.op);
    cluster_beat_soon();
    return 0;
}

int cvol_disk_reopen_refused(const uint8_t store_id[16])
{
    if (!store_id)
        return 0;
    pthread_mutex_lock(&M.op);
    pthread_mutex_lock(&M.mu);
    if (M.inst_closed || M.stop || M.writes) {
        pthread_mutex_unlock(&M.mu);
        pthread_mutex_unlock(&M.op);
        return -1;
    }
    int n = 0;
    for (uint32_t i = 0; i < M.nvols; i++) {
        struct mvol *m = &M.vols[i];
        if (m->state != MV_REFUSED || !m->refused_disk_down)
            continue;
        int zero = 1;
        for (int b = 0; b < 16; b++)
            if (m->store_id[b] != 0) {
                zero = 0;
                break;
            }
        if (zero || memcmp(m->store_id, store_id, 16) == 0) {
            m->state = MV_REPREP;
            m->fault_reason = CL_FAULT_NONE;
            m->refused_disk_down = 0;
            n++;
        }
    }
    if (n > 0) {
        M.dirty = 1;
        pthread_cond_signal(&M.cv);
    }
    pthread_mutex_unlock(&M.mu);
    pthread_mutex_unlock(&M.op);
    if (n > 0)
        cluster_beat_soon();
    return n;
}

int cvol_vol_reopening(uint32_t v)
{
    int r = 0;
    pthread_mutex_lock(&M.mu);
    const struct mvol *m = mv_get(v);
    if (m && (m->state == MV_INSTALL || m->state == MV_REPREP || m->state == MV_WAIT))
        r = 1;
    pthread_mutex_unlock(&M.mu);
    return r;
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
