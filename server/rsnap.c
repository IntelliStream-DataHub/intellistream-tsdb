/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshot transfer of a replicated volume (rsnap.h; design-cluster.md 8.1, R4 plan D2, D5, D7, D16, D18): the
 * replica's request, the leader's make and send, the receiver's files in `v<N>.snap/`, which w_finish hands to the
 * install (server/snapinst.c) on this worker, unless it is stale: P at or below the receiver's log end, or an epoch the
 * receiver is fenced above (removed; asked again at the next notice).
 *
 * Links. Snapshots travel on kind 5 (PL_KIND_SNAP) only, under a token bucket of their own (`rebuild_mb_s`,
 * peerlink_set_kind_rate): a COMMIT's file on kind 2 never waits behind one (D7). Each side dials kind 5 to the other
 * when it needs it (G.nodes[i].snap): the receiver's frames (REQ, GO, DONE) go on the receiver's dialed link, the
 * source's (BUSY, ABORT) and the files on the source's. Every frame names the volume and the request id, so the link a
 * frame arrives on does not matter; the node it comes from does (plink_peer).
 *
 *   receiver                                   source (the volume's leader, the notice names it)
 *   REQ{vol, set_version, epoch, req}   -->    BUSY{vol, req, why} (promoting, no writer engine, rebuild_max, stale)
 *                                              or snap_make into snap_<peer>_<req>/ on the worker (D18)
 *                                       <--    SNAPMAN (a bulk file, meta {vol, req, RS_MAN})
 *   free space >= total + max(10%, 1 GiB)?     (D5: the live volumes' WAL must never meet ENOSPC)
 *   GO{vol, req} or DONE{vol, req, -2}  -->
 *                                       <--    every file SNAPMAN names that is not a link of an earlier one, one at a
 *                                              time (meta {vol, req, index}), resumed from the receiver's offset
 *   DONE{vol, req, 0 or why not}        -->    snap_release on the worker
 *
 * Each file is written by the kind-5 link's own data callback (D18) into `v<N>.snap/<name>`, hashed as it arrives and
 * compared with SNAPMAN's xxh64 at its end; the link checks its own xxh64 of the bytes the sender read as well: two
 * checks, one of the wire and one against what the make recorded. The make's hash stays (the option of leaving it to
 * the sender is not taken): SNAPMAN must carry every hash before the first file leaves, so the receiver can compare
 * each file with the record and not merely with what the sender read, and the install checks against it again.
 *
 * Threads and locks. S.mu is a leaf below rv->mu (rsnap_need and rsnap_tick hold rv->mu and take it; nothing takes
 * rv->mu under it) and above G.mu (rs_link), and is never held across file I/O: a callback that writes marks its job
 * busy, lets go, writes, and takes it again. One worker thread makes, opens, removes and releases: never the driver's
 * thread, a link's or the cluster thread (D18). At most rebuild_max snapshots are alive per server, sent and received
 * together. A job is freed only by the worker (w_drop), once no callback is inside it (busy 0); every task of a job is
 * queued before its W_DROP, which is queued exactly once (`dead` set with it, under S.mu). The files a source has
 * queued on a link own their fds (struct stick): a job dropped while one is in flight leaves the fd to the link, which
 * closes it in `done`. rsnap_quiesce stops the worker before peerlink_free; what a link's callback asks after that is
 * dropped (the next open sweeps snap_*, and snap_leftovers removes a v<N>.snap without SNAPDONE).
 *
 * Retries. Nothing of this is resent by the core: a request that got no answer is sent again at a notice after
 * RS_ASK_AGAIN_MS; a BUSY waits RS_BUSY_WAIT_MS; a transfer that moves no byte for RS_STALL_MS, a notice that names
 * another source (the source died: the new leader's notice) or a receiver that is no longer a replica is abandoned (its
 * directory removed) and asked again at the next notice. A source releases its snapshot when the receiver says DONE,
 * when its link to the receiver stayed down RS_SRC_DOWN_MS, when it lost the epoch it made it in, or after RS_SRC_MAX_MS.
 */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/random.h>
#include <time.h>
#include <unistd.h>

#include "../src/cvol.h"
#include "../src/snapshot.h"
#include "peerlink.h"
#include "repld.h"
#include "rsnap.h"
#include "crashpt.h"
#include "cvol.h"
#include "snapinst.h"

/* frame types on kind 5 (below PL_USER_TYPE_MAX) */
enum { RS_T_REQ = 0x20, RS_T_BUSY = 0x21, RS_T_GO = 0x22, RS_T_DONE = 0x23, RS_T_ABORT = 0x24 };
/* BUSY's reasons */
enum { RS_B_PROMOTING = 1, RS_B_MAX = 2, RS_B_STALE = 3, RS_B_MAKE = 4 };
#define RS_MAN UINT32_MAX /* the bulk meta's index of SNAPMAN */
#define RS_META_LEN 16
#define RS_FRAME_LEN 20
#define RS_REQ_LEN 28

#define RS_ASK_AGAIN_MS 2000
#define RS_BUSY_WAIT_MS 1000
#define RS_STALL_MS 30000
#define RS_SRC_DOWN_MS 60000
#define RS_WAITDONE_MS 60000 /* source: the receiver's DONE 0 after the last file's (a lost one frees the slot) */
#define RS_SRC_MAX_MS (6ull * 3600 * 1000)
#define RS_STUB_WAIT_MS 60000 /* a copy being installed: no new request meanwhile (the install hands it back) */
#define RS_TICK_EVERY_MS 200
#define RS_MAKE_TRIES 4
#define RS_SPACE_MIN (1ull << 30)

enum { R_SRC = 1, R_DST };
/* a receiver: PREP (its directory being made) -> ASKED -> FILES (SNAPMAN in, space checked, GO sent) -> COMPLETE;
 * a source: MAKING -> MAN (SNAPMAN queued) -> WAITGO -> SENDING -> WAITDONE */
enum { S_PREP = 1, S_ASKED, S_FILES, S_COMPLETE, S_MAKING, S_MAN, S_WAITGO, S_SENDING, S_WAITDONE };
enum { W_PREP = 1, W_MAKE, W_NEXT, W_FINISH, W_DROP };

struct rjob {
    struct rjob *next;
    int role, state, dead, busy;
    uint32_t vol, peer;
    uint64_t req, epoch, set_version;
    uint64_t rate;           /* source: rebuild_mb_s in bytes/s */
    struct snap_epoch *hist; /* source: the leader's epoch history for the make */
    size_t nhist;
    char dir[PATH_MAX + 32]; /* source: the snapshot (once made); receiver: v<N>.snap */
    int made;                /* the directory exists and is this job's to remove */
    struct snap_man man;
    int have_man;
    uint8_t *fdone;          /* receiver: per file of SNAPMAN, 1 once it is in whole (a file begun again is not counted) */
    size_t fnext, nleft;      /* source: the next file to send; receiver: the files still to come */
    uint64_t total, moved;   /* the bytes of the files (SNAPMAN's left out), and those moved so far */
    /* receiver: the file being written */
    int fd;
    int cur_redo; /* the file is in already: begun again because its DONE was lost; fd is a read of it for the hash */
    uint32_t cur_idx;
    uint64_t cur_file, cur_total, got;
    struct pl_xxh h;
    struct plink *cur_l;
    uint64_t start_ms, last_ms, down_ms;
};

/* the fd of a file queued on a link: the link's until `done` */
struct stick {
    int fd;
    uint32_t vol, idx;
    uint64_t req, size;
};

struct wtask {
    struct wtask *next;
    int what;
    struct rjob *j;
};

/* per volume: the next request no sooner than this; lines said once */
struct rvs {
    uint32_t vol;
    uint64_t retry_at, tick_ms;
    int said_space, said_wait, said_stale;
};

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv; /* the worker's queue, and a job's busy reaching 0 */
    struct rjob *jobs;
    struct wtask *wh, *wt;
    pthread_t th;
    int started, stopping;
    uint64_t seq;
    struct rvs vs[CLUSTER_REPORT_VOLS];
    uint32_t nvs;
    struct rsnap_stats st;
    /* test hooks (TSDB_REPLD_TEST) */
    int hold, promoting_hook, flip, flipped, nodone, stalereq, stalesent, srcstall, srcstalled, fakesrc;
    uint64_t free_hook, slowmake_ms;
    /* timers; the hooks shorten them */
    uint64_t stall_ms, down_ms, waitdone_ms;
} S = {.mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER, .stall_ms = RS_STALL_MS,
       .down_ms = RS_SRC_DOWN_MS, .waitdone_ms = RS_WAITDONE_MS};

static pthread_once_t hooks_once = PTHREAD_ONCE_INIT;

static void hooks_read(void)
{
    const char *te = getenv("TSDB_REPLD_TEST");
    if (!te)
        return;
    S.hold = strstr(te, "snaphold") != NULL;
    S.promoting_hook = strstr(te, "snappromoting") != NULL;
    S.flip = strstr(te, "snapflip") != NULL;
    S.nodone = strstr(te, "snapnodone") != NULL;     /* the receiver's DONE 0 is never sent (lost) */
    S.stalereq = strstr(te, "snapstalereq") != NULL; /* the first request names set version + 1 */
    S.srcstall = strstr(te, "snapsrcstall") != NULL; /* the first source ignores GO (it stalls, still the leader) */
    S.fakesrc = strstr(te, "snapfakesrc") != NULL;   /* <dir>/snapfakesrc stands for one source job running */
    if (strstr(te, "snapcutdone")) /* the DONE of the second data file is lost to a cut connection */
        __atomic_store_n(&peerlink_test_cut_end, 3, __ATOMIC_RELAXED);
    const char *f = strstr(te, "snapfree=");
    if (f)
        S.free_hook = strtoull(f + 9, NULL, 10);
    struct { const char *k; uint64_t *v; } ms[] = {{"snapslowmake=", &S.slowmake_ms}, {"snapstallms=", &S.stall_ms},
                                                 {"snapdownms=", &S.down_ms}, {"snapwaitdonems=", &S.waitdone_ms}};
    for (size_t i = 0; i < sizeof ms / sizeof ms[0]; i++)
        if ((f = strstr(te, ms[i].k)))
            *ms[i].v = strtoull(f + strlen(ms[i].k), NULL, 10);
}

/* TSDB_REPLD_TEST=snapfakesrc: one source job more while <dir>/snapfakesrc exists (read before S.mu is taken) */
static int fake_src(void)
{
    if (!S.fakesrc)
        return 0;
    char hp[sizeof G.dir + 32];
    snprintf(hp, sizeof hp, "%s/snapfakesrc", G.dir);
    return access(hp, F_OK) == 0;
}

static uint64_t ms_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void put64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }
static uint32_t get32(const void *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}
static uint64_t get64(const void *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

#define STAT(f, n) __atomic_add_fetch(&S.st.f, (n), __ATOMIC_RELAXED)
#define STATGET(f) __atomic_load_n(&S.st.f, __ATOMIC_RELAXED)

void rsnap_get_stats(struct rsnap_stats *out)
{
    out->asked = STATGET(asked);
    out->sent = STATGET(sent);
    out->received = STATGET(received);
    out->refused_space = STATGET(refused_space);
    out->failed = STATGET(failed);
    out->busy = STATGET(busy);
    out->installed = STATGET(installed);
    out->refused_stale = STATGET(refused_stale);
    out->install_ms = STATGET(install_ms);
    out->bytes_sent = STATGET(bytes_sent);
    out->bytes_received = STATGET(bytes_received);
    out->last_ms = STATGET(last_ms);
}

/* ---- links */

/* the kind-5 link this server dialed to `node` (dialed now if not yet), or NULL (G.mu taken inside) */
static struct plink *rs_link(uint32_t node)
{
    struct plink *l = NULL;
    pthread_mutex_lock(&G.mu);
    for (uint32_t i = 0; i < G.nn; i++)
        if (G.nodes[i].node == node) {
            if (!G.nodes[i].snap && G.pl)
                G.nodes[i].snap = peerlink_dial(G.pl, node, G.nodes[i].addr, PL_KIND_SNAP);
            l = G.nodes[i].snap;
            break;
        }
    pthread_mutex_unlock(&G.mu);
    return l;
}

/* a control frame {vol, req, arg}; lost: the receiver asks again, the source times out */
static void rs_send(uint32_t node, uint8_t type, uint32_t vol, uint64_t req, int64_t arg)
{
    uint8_t b[RS_FRAME_LEN];
    put32(b, vol);
    put64(b + 4, req);
    put64(b + 12, (uint64_t)arg);
    struct plink *l = rs_link(node);
    if (l)
        plink_send(l, type, b, sizeof b, 0);
}

/* S.mu held. A frame on a link still connecting is lost (PL_E_DOWN): the first request of a run is usually, and
 * rsnap_need repeats it every RS_ASK_AGAIN_MS while the job waits */
static void rs_send_req(uint32_t node, uint32_t vol, uint64_t set_version, uint64_t epoch, uint64_t req)
{
    uint8_t b[RS_REQ_LEN];
    int stale = S.stalereq && !S.stalesent; /* TSDB_REPLD_TEST=snapstalereq: the first request sent is stale */
    put32(b, vol), put64(b + 4, set_version + (uint64_t)stale), put64(b + 12, epoch), put64(b + 20, req);
    struct plink *l = rs_link(node);
    if (l && plink_send(l, RS_T_REQ, b, sizeof b, 0) == PL_OK && stale) {
        S.stalesent = 1;
        say("TEST: volume %" PRIu32 ": a request names set version %" PRIu64 " (snapstalereq)", vol, set_version + 1);
    }
}

/* ---- jobs (S.mu held) */

static struct rvs *vs_get(uint32_t vol)
{
    for (uint32_t i = 0; i < S.nvs; i++)
        if (S.vs[i].vol == vol)
            return &S.vs[i];
    if (S.nvs == CLUSTER_REPORT_VOLS)
        return &S.vs[0]; /* never: one driver per volume, at most CLUSTER_REPORT_VOLS of them */
    S.vs[S.nvs].vol = vol;
    return &S.vs[S.nvs++];
}

static struct rjob *job_find(int role, uint32_t vol, uint64_t req)
{
    for (struct rjob *j = S.jobs; j; j = j->next)
        if (j->role == role && j->vol == vol && (req == 0 || j->req == req) && !j->dead)
            return j;
    return NULL;
}

/* the live jobs of one role: rebuild_max bounds the receivers and, apart, the sources of a server, so two members
 * that each receive from the other never answer each other BUSY for good */
static unsigned jobs_alive(int role, unsigned fake)
{
    unsigned n = role == R_SRC || G.mutant == RM_SNAPSHAREDMAX ? fake : 0;
    for (struct rjob *j = S.jobs; j; j = j->next)
        n += !j->dead && (j->role == role || G.mutant == RM_SNAPSHAREDMAX);
    return n;
}

static void *worker(void *arg);

static void task_add(int what, struct rjob *j)
{
    if (S.stopping)
        return; /* the worker is gone (rsnap_quiesce): rsnap_stop frees the job */
    if (!S.started && pthread_create(&S.th, NULL, worker, NULL) == 0)
        S.started = 1;
    struct wtask *t = calloc(1, sizeof *t);
    if (!t) {
        say("snapshot: out of memory queueing a task");
        return;
    }
    t->what = what, t->j = j;
    if (S.wt)
        S.wt->next = t;
    else
        S.wh = t;
    S.wt = t;
    pthread_cond_broadcast(&S.cv);
}

/* the job is over: no frame or file of it is taken any more; the worker removes its directory and frees it */
static void job_drop(struct rjob *j)
{
    if (j->dead)
        return;
    j->dead = 1;
    task_add(W_DROP, j);
}

static void dst_abandon(struct rjob *j, const char *why, int notify)
{
    if (j->dead)
        return;
    say("volume %" PRIu32 ": the snapshot from node %" PRIu32 " (request %" PRIx64 ") is abandoned: %s; asked again at"
        " the next notice", j->vol, j->peer, j->req, why);
    STAT(failed, 1);
    if (notify)
        rs_send(j->peer, RS_T_DONE, j->vol, j->req, -1);
    job_drop(j);
}

static void src_abort(struct rjob *j, const char *why)
{
    if (j->dead)
        return;
    say("volume %" PRIu32 ": the snapshot for node %" PRIu32 " (request %" PRIx64 ") is released: %s", j->vol, j->peer,
        j->req, why);
    STAT(failed, 1);
    rs_send(j->peer, RS_T_ABORT, j->vol, j->req, 0);
    job_drop(j);
}

static void job_free(struct rjob *j)
{
    if (j->fd >= 0)
        close(j->fd);
    snap_man_free(&j->man);
    free(j->fdone);
    free(j->hist);
    free(j);
}

/* ---- the receiver */

void rsnap_rebuild(struct rvol *rv)
{
    pthread_once(&hooks_once, hooks_read);
    if (!rv->div_rebuild) { /* TSDB_REPLD_TEST=snapask: asked while <dir>/snapask exists (test only: an access under
                             * rv->mu, as plantdiverge's) */
        char hp[sizeof G.dir + 32];
        snprintf(hp, sizeof hp, "%s/snapask", G.dir);
        if (access(hp, F_OK) != 0)
            return;
    }
    for (uint32_t k = 0; k < rv->rec.nrep; k++)
        if (rv->rec.rep[k].node == rv->rec.leader && rv->rec.leader != G.self) {
            rsnap_need(rv, (int)k);
            return;
        }
}

void rsnap_need(struct rvol *rv, int src)
{
    pthread_once(&hooks_once, hooks_read);
    if (src < 0 || (uint32_t)src >= rv->rec.nrep || rv->mode != M_REPLICA || (rv->failed && !rv->div_rebuild))
        return;
    uint32_t node = rv->rec.rep[src].node;
    if (node == G.self)
        return;
    unsigned fake = (unsigned)fake_src();
    uint64_t now = ms_now(), rnd = 0;
    if (getrandom(&rnd, sizeof rnd, GRND_NONBLOCK) != (ssize_t)sizeof rnd)
        rnd = now ^ ((uint64_t)G.self << 40) ^ ((uint64_t)getpid() << 16);
    pthread_mutex_lock(&S.mu);
    struct rjob *j = job_find(R_DST, rv->number, 0);
    struct rvs *v = vs_get(rv->number);
    if (j) {
        if (j->peer != node)
            dst_abandon(j, "the notice names another source (a new leader)", 1);
        else if (j->state == S_ASKED && j->fd < 0 && now - j->last_ms >= RS_ASK_AGAIN_MS) {
            j->last_ms = now;
            j->set_version = rv->rec.set_version, j->epoch = rv->epoch;
            rs_send_req(node, j->vol, j->set_version, j->epoch, j->req);
        }
        pthread_mutex_unlock(&S.mu);
        return;
    }
    if (now < v->retry_at || S.stopping) {
        pthread_mutex_unlock(&S.mu);
        return;
    }
    uint32_t max = rv->rec.rebuild_max ? rv->rec.rebuild_max : 2;
    if (jobs_alive(R_DST, fake) >= max) { /* rebuild_max: a later notice asks */
        if (!v->said_wait)
            say("volume %" PRIu32 ": needs a snapshot and waits: %u snapshots run on this server (rebuild_max)",
                rv->number, max);
        v->said_wait = 1;
        pthread_mutex_unlock(&S.mu);
        return;
    }
    v->said_wait = 0;
    if (!(j = calloc(1, sizeof *j))) {
        pthread_mutex_unlock(&S.mu);
        return;
    }
    j->role = R_DST, j->state = S_PREP, j->vol = rv->number, j->peer = node, j->fd = -1;
    /* random: a source may still serve the request of this member's previous run, whose counter started where ours did */
    j->req = rnd ^ ++S.seq;
    if (!j->req)
        j->req = 1;
    j->set_version = rv->rec.set_version, j->epoch = rv->epoch;
    j->start_ms = j->last_ms = now;
    snprintf(j->dir, sizeof j->dir, "%s/v%" PRIu32 ".snap", G.dir, rv->number);
    j->next = S.jobs;
    S.jobs = j;
    task_add(W_PREP, j);
    pthread_mutex_unlock(&S.mu);
    if (!rv->snap_said)
        say("volume %" PRIu32 ": the leader (node %" PRIu32 ") no longer has the records this replica needs: it needs"
            " a snapshot and asks the leader for one", rv->number, node);
    rv->snap_said = 1;
}

/* the space a snapshot of `total` bytes needs free beside the live volumes (D5) */
static int space_ok(uint64_t total, uint64_t *avail)
{
    uint64_t need = total + (total / 10 > RS_SPACE_MIN ? total / 10 : RS_SPACE_MIN);
    if (S.free_hook) {
        *avail = S.free_hook;
    } else {
        struct statvfs sv;
        if (statvfs(G.dir, &sv) != 0)
            return 0;
        *avail = (uint64_t)sv.f_bavail * (uint64_t)sv.f_frsize;
    }
    return *avail >= need;
}

/* SNAPMAN is in and synced (the link's thread; the caller's busy on j is let go of here): read it, check the space,
 * GO or refused */
static void dst_man(struct rjob *j)
{
    struct snap_man m;
    memset(&m, 0, sizeof m);
    int rc = snap_man_read(j->dir, &m);
    uint64_t total = 0, avail = 0;
    size_t nfiles = 0;
    for (size_t i = 0; rc == 0 && i < m.nfiles; i++)
        if (!m.files[i].link)
            total += m.files[i].size, nfiles++;
    uint8_t *fdone = rc == 0 ? calloc(m.nfiles ? m.nfiles : 1, 1) : NULL;
    if (rc == 0 && !fdone)
        rc = TSDB_ERR_NOMEM;
    int space = rc == 0 && (G.mutant == RM_SNAPNOSPACECHECK || space_ok(total, &avail));
    pthread_mutex_lock(&S.mu);
    j->busy--;
    pthread_cond_broadcast(&S.cv);
    if (j->dead || rc != 0 || !space)
        free(fdone), fdone = NULL;
    if (j->dead) {
        snap_man_free(&m);
    } else if (rc != 0) {
        snap_man_free(&m);
        dst_abandon(j, rc == TSDB_ERR_NOMEM ? "out of memory" : "its SNAPMAN does not read", 1);
    } else if (!space) {
        struct rvs *v = vs_get(j->vol);
        if (!v->said_space)
            say("volume %" PRIu32 ": a snapshot of %" PRIu64 " bytes is refused: the file system of %s has %" PRIu64
                " bytes free and needs the snapshot plus 10%% (at least 1 GiB), so the live volumes never run out;"
                " asked again at the next notice (said once)", j->vol, total, G.dir, avail);
        v->said_space = 1;
        STAT(refused_space, 1);
        snap_man_free(&m);
        rs_send(j->peer, RS_T_DONE, j->vol, j->req, -2);
        v->retry_at = ms_now() + RS_BUSY_WAIT_MS;
        job_drop(j);
    } else {
        j->man = m, j->have_man = 1, j->fdone = fdone;
        j->total = total, j->nleft = nfiles, j->state = S_FILES;
        j->last_ms = ms_now();
        vs_get(j->vol)->said_space = 0;
        if (!nfiles) {
            j->state = S_COMPLETE;
            task_add(W_FINISH, j);
        } else
            rs_send(j->peer, RS_T_GO, j->vol, j->req, 0);
    }
    pthread_mutex_unlock(&S.mu);
}

int rsnap_bin_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                   uint64_t *resume, int *prefix_fd)
{
    (void)arg;
    if (meta_len != RS_META_LEN)
        return -1;
    uint32_t vol = get32(meta), idx = get32((const uint8_t *)meta + 12);
    uint64_t req = get64((const uint8_t *)meta + 4);
    pthread_mutex_lock(&S.mu);
    struct rjob *j = job_find(R_DST, vol, req);
    if (!j || j->peer != plink_peer(l)) {
        pthread_mutex_unlock(&S.mu);
        return -1;
    }
    if (j->fd >= 0 && j->cur_idx == idx && j->cur_total == total) { /* begun again after a reconnect: resumed */
        j->cur_file = file, j->cur_l = l;
        *resume = j->got;
        *prefix_fd = j->fd;
        pthread_mutex_unlock(&S.mu);
        return 0;
    }
    const char *name = NULL;
    int redo = 0; /* begun again after its DONE was lost: the copy on disk is offered whole, hashed by the link */
    if (j->fd < 0 && (j->state == S_FILES || j->state == S_COMPLETE) && G.mutant != RM_SNAPNODONEBITS &&
        ((idx == RS_MAN && j->have_man) ||
         (idx != RS_MAN && j->fdone && idx < j->man.nfiles && j->fdone[idx] && j->man.files[idx].size == total)))
        name = idx == RS_MAN ? SNAP_MAN : j->man.files[idx].name, redo = 1;
    else if (idx == RS_MAN && j->state == S_ASKED && j->fd < 0 && total <= (64u << 20))
        name = SNAP_MAN;
    else if (idx != RS_MAN && j->state == S_FILES && j->fd < 0 && idx < j->man.nfiles && !j->man.files[idx].link &&
             j->man.files[idx].size == total)
        name = j->man.files[idx].name;
    if (!name) {
        pthread_mutex_unlock(&S.mu);
        return -1;
    }
    char path[PATH_MAX + 32 + 130];
    snprintf(path, sizeof path, "%s/%s", j->dir, name);
    j->busy++;
    pthread_mutex_unlock(&S.mu);
    int fd = redo ? open(path, O_RDONLY | O_CLOEXEC) : open(path, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    struct stat st;
    if (redo && fd >= 0 && (fstat(fd, &st) != 0 || (uint64_t)st.st_size != total))
        close(fd), fd = -1;
    pthread_mutex_lock(&S.mu);
    j->busy--;
    pthread_cond_broadcast(&S.cv);
    if (fd < 0 || j->dead) {
        if (fd >= 0)
            close(fd);
        else
            dst_abandon(j, redo ? "a file that was in could not be read again" : "a file could not be created", 1);
        pthread_mutex_unlock(&S.mu);
        return -1;
    }
    j->fd = fd, j->cur_idx = idx, j->cur_file = file, j->cur_total = total, j->cur_l = l, j->got = redo ? total : 0;
    j->cur_redo = redo;
    j->last_ms = ms_now();
    pl_xxh_init(&j->h);
    *resume = redo ? total : 0;
    *prefix_fd = redo ? fd : -1;
    pthread_mutex_unlock(&S.mu);
    return 0;
}

static struct rjob *dst_by_file(struct plink *l, uint64_t file)
{
    for (struct rjob *j = S.jobs; j; j = j->next)
        if (j->role == R_DST && !j->dead && j->fd >= 0 && j->cur_l == l && j->cur_file == file)
            return j;
    return NULL;
}

int rsnap_bin_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    (void)arg;
    pthread_mutex_lock(&S.mu);
    struct rjob *j = dst_by_file(l, file);
    if (!j || off != j->got || n > j->cur_total - off) {
        pthread_mutex_unlock(&S.mu);
        return -1;
    }
    int fd = j->fd;
    j->busy++;
    pthread_mutex_unlock(&S.mu);
    int bad = 0;
    for (size_t done = 0; done < n;) {
        ssize_t w = pwrite(fd, (const uint8_t *)p + done, n - done, (off_t)(off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            bad = 1;
            break;
        }
        done += (size_t)w;
    }
    if (!bad && G.mutant != RM_SNAPNOVERIFY)
        pl_xxh_update(&j->h, p, n); /* only the link's thread that holds the file updates it; busy keeps j */
    pthread_mutex_lock(&S.mu);
    j->busy--;
    pthread_cond_broadcast(&S.cv);
    if (!bad) {
        j->got += n, j->moved += n;
        j->last_ms = ms_now();
        STAT(bytes_received, n);
    } else
        dst_abandon(j, "writing a file failed", 1);
    pthread_mutex_unlock(&S.mu);
    return bad ? -1 : 0;
}

void rsnap_bin_end(void *arg, struct plink *l, uint64_t file, int status)
{
    (void)arg;
    pthread_mutex_lock(&S.mu);
    struct rjob *j = dst_by_file(l, file);
    if (!j || status == PL_E_DOWN) { /* down: the sender resumes it from j->got on its next connection */
        pthread_mutex_unlock(&S.mu);
        return;
    }
    if (status != PL_OK || j->got != j->cur_total) {
        dst_abandon(j, status == PL_E_HASH ? "a file arrived damaged (the link's xxh64)" : "a file did not arrive", 1);
        pthread_mutex_unlock(&S.mu);
        return;
    }
    if (j->cur_redo) { /* the link checked the copy on disk against the sender's xxh64: it was counted already */
        close(j->fd);
        j->fd = -1, j->cur_redo = 0;
        j->last_ms = ms_now();
        pthread_mutex_unlock(&S.mu);
        return;
    }
    int fd = j->fd;
    uint32_t idx = j->cur_idx;
    uint64_t want = idx == RS_MAN ? 0 : j->man.files[idx].xxh64, got = pl_xxh_final(&j->h);
    j->fd = -1;
    j->busy++;
    pthread_mutex_unlock(&S.mu);
    int synced = fdatasync(fd) == 0;
    close(fd);
    if (idx == RS_MAN && synced) {
        dst_man(j); /* lets go of busy */
        return;
    }
    pthread_mutex_lock(&S.mu);
    j->busy--;
    pthread_cond_broadcast(&S.cv);
    if (!synced)
        dst_abandon(j, "a file could not be synced", 1);
    else if (G.mutant != RM_SNAPNOVERIFY && got != want)
        dst_abandon(j, "a file's xxh64 is not the one SNAPMAN names", 1);
    else if (!j->dead && (!j->fdone[idx] || G.mutant == RM_SNAPNODONEBITS) && (j->fdone[idx] = 1) && --j->nleft == 0) {
        j->state = S_COMPLETE;
        task_add(W_FINISH, j);
    }
    pthread_mutex_unlock(&S.mu);
}

/* ---- the source */

static void src_req(struct plink *l, const uint8_t *p, size_t n)
{
    if (n < RS_REQ_LEN)
        return;
    uint32_t vol = get32(p), from = plink_peer(l);
    uint64_t setv = get64(p + 4), epoch = get64(p + 12), req = get64(p + 20);
    pthread_once(&hooks_once, hooks_read);
    unsigned fake = (unsigned)fake_src();
    struct rvol *rv = vol_find(vol);
    if (!rv) {
        rs_send(from, RS_T_BUSY, vol, req, RS_B_STALE);
        return;
    }
    int hook = 0;
    if (S.promoting_hook) { /* TSDB_REPLD_TEST=snappromoting: <dir>/snappromoting stands for M_PROMOTING */
        char hp[sizeof G.dir + 32];
        snprintf(hp, sizeof hp, "%s/snappromoting", G.dir);
        hook = access(hp, F_OK) == 0;
    }
    int why = 0, member = 0;
    uint64_t rate = 0, max = 2, my_setv = 0, my_epoch = 0;
    struct snap_epoch *hist = NULL;
    size_t nhist = 0;
    pthread_mutex_lock(&rv->mu);
    for (uint32_t k = 0; k < rv->rec.nrep; k++)
        member |= rv->rec.rep[k].node == from;
    int promoting = rv->mode == M_PROMOTING || (hook && rv->mode == M_WRITER);
    my_setv = rv->rec.set_version, my_epoch = rv->epoch;
    if (promoting && G.mutant != RM_SNAPSRCPROMOTING)
        why = RS_B_PROMOTING; /* D2: no make before its writer engine is attached */
    else if (!member || rv->failed || (rv->mode != M_WRITER && rv->mode != M_PROMOTING) ||
             ((setv != rv->rec.set_version || epoch != rv->epoch) && G.mutant != RM_SNAPNOSTALE))
        why = RS_B_STALE;
    if (!why) {
        rate = (uint64_t)(rv->rec.rebuild_mb_s ? rv->rec.rebuild_mb_s : 200) << 20;
        max = rv->rec.rebuild_max ? rv->rec.rebuild_max : 2;
        if (rv->nhist && (hist = calloc(rv->nhist, sizeof *hist)))
            for (uint32_t i = 0; i < rv->nhist; i++)
                hist[i].epoch = rv->hist[i].epoch, hist[i].first = rv->hist[i].start;
        nhist = hist ? rv->nhist : 0;
    }
    pthread_mutex_unlock(&rv->mu);
    pthread_mutex_lock(&S.mu);
    struct rjob *j = why ? NULL : job_find(R_SRC, vol, 0);
    if (j && j->peer == from && j->req == req) { /* a repeat of a request being served */
        pthread_mutex_unlock(&S.mu);
        free(hist);
        return;
    }
    if (j && j->peer == from)
        src_abort(j, "the receiver asked again"); /* it abandoned the old one */
    if (!why && ((jobs_alive(R_SRC, fake) >= max && G.mutant != RM_SNAPNOSRCMAX) || S.stopping)) {
        if (fake)
            say("TEST: volume %" PRIu32 ": a source job is counted for <dir>/snapfakesrc (snapfakesrc)", vol);
        why = RS_B_MAX;
    }
    struct rvs *v = vs_get(vol);
    if (why == RS_B_STALE && !v->said_stale)
        say("volume %" PRIu32 ": a snapshot request from node %" PRIu32 " (set version %" PRIu64 ", epoch %" PRIu64
            ") is refused as stale: this member has set version %" PRIu64 ", epoch %" PRIu64 " (said once until one"
            " is served)", vol, from, setv, epoch, my_setv, my_epoch);
    v->said_stale = why == RS_B_STALE;
    if (why || !(j = calloc(1, sizeof *j))) {
        STAT(busy, 1);
        pthread_mutex_unlock(&S.mu);
        rs_send(from, RS_T_BUSY, vol, req, why ? why : RS_B_MAX);
        free(hist);
        return;
    }
    j->role = R_SRC, j->state = S_MAKING, j->vol = vol, j->peer = from, j->req = req, j->epoch = epoch, j->fd = -1;
    j->set_version = setv, j->rate = rate, j->hist = hist, j->nhist = nhist;
    j->start_ms = j->last_ms = ms_now();
    j->next = S.jobs;
    S.jobs = j;
    task_add(W_MAKE, j);
    pthread_mutex_unlock(&S.mu);
}

/* a file of a source left its link (the link's thread) */
static void src_done(void *arg, uint64_t file, int status, uint64_t sent)
{
    (void)file;
    struct stick *t = arg;
    close(t->fd);
    pthread_mutex_lock(&S.mu);
    struct rjob *j = job_find(R_SRC, t->vol, t->req);
    if (j) {
        if (status != PL_OK)
            src_abort(j, status == PL_E_REFUSED ? "the receiver refused a file" : "a file could not be sent");
        else {
            STAT(bytes_sent, sent);
            j->moved += t->idx == RS_MAN ? 0 : t->size;
            j->last_ms = ms_now();
            if (t->idx == RS_MAN) {
                if (j->state == S_MAN)
                    j->state = S_WAITGO;
            } else
                task_add(W_NEXT, j);
        }
    }
    pthread_mutex_unlock(&S.mu);
    free(t);
}

/* queues file `idx` (RS_MAN: SNAPMAN) of the job on the link to its receiver: NULL, or why not (the worker, busy held) */
static const char *src_queue(struct rjob *j, uint32_t idx)
{
    char path[PATH_MAX + 32 + 130];
    snprintf(path, sizeof path, "%s/%s", j->dir, idx == RS_MAN ? SNAP_MAN : j->man.files[idx].name);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0) {
        if (fd >= 0)
            close(fd);
        return "a file of the snapshot could not be opened";
    }
    if (idx != RS_MAN && (uint64_t)st.st_size != j->man.files[idx].size) {
        close(fd);
        return "a file of the snapshot is not the size SNAPMAN names";
    }
    struct stick *t = calloc(1, sizeof *t);
    if (!t) {
        close(fd);
        return "out of memory";
    }
    t->fd = fd, t->vol = j->vol, t->idx = idx, t->req = j->req, t->size = (uint64_t)st.st_size;
    uint8_t meta[RS_META_LEN];
    put32(meta, j->vol), put64(meta + 4, j->req), put32(meta + 12, idx);
    struct plink *l = NULL;
    if (G.mutant == RM_SNAPSAMELINK) { /* planted: the snapshot's files on the COMMITs' link (kind 2) */
        pthread_mutex_lock(&G.mu);
        for (uint32_t i = 0; i < G.nn; i++)
            if (G.nodes[i].node == j->peer)
                l = G.nodes[i].bulk;
        pthread_mutex_unlock(&G.mu);
    } else
        l = rs_link(j->peer);
    if (!l) {
        close(fd);
        free(t);
        return "no link to the receiver";
    }
    /* rebuild_mb_s (D16): a cluster setting, the same in every record; kind 5's bucket only (D7) */
    peerlink_set_kind_rate(G.pl, PL_KIND_SNAP, j->rate);
    if (G.mutant == RM_SNAPSAMELINK)
        peerlink_set_kind_rate(G.pl, PL_KIND_BULK, j->rate);
    pthread_mutex_lock(&G.mu);
    uint64_t fileno = ++G.fileno;
    pthread_mutex_unlock(&G.mu);
    struct pl_job pj = {.file = fileno, .fd = fd, .base = 0, .length = t->size, .meta = meta,
                        .meta_len = sizeof meta, .max_attempts = 20, .done = src_done, .arg = t};
    if (plink_bulk_send(l, &pj) != PL_OK) {
        close(fd);
        free(t);
        return "the link to the receiver did not take the file";
    }
    return NULL;
}

/* ---- frames */

void rsnap_frame(struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    if (type == RS_T_REQ) {
        src_req(l, p, n);
        return;
    }
    if (type < RS_T_REQ || type > RS_T_ABORT) { /* nothing sends one but the snapdial hook (scen_volcmd) */
        say("a snapshot link (kind 5) sent a frame of unknown type %u from node %" PRIu32 "; dropped", (unsigned)type,
            plink_peer(l));
        return;
    }
    if (n < RS_FRAME_LEN)
        return;
    uint32_t vol = get32(p), from = plink_peer(l);
    uint64_t req = get64(p + 4);
    int64_t arg = (int64_t)get64(p + 12);
    pthread_mutex_lock(&S.mu);
    struct rjob *j = job_find(type == RS_T_BUSY || type == RS_T_ABORT ? R_DST : R_SRC, vol, req);
    if (!j || j->peer != from) {
        pthread_mutex_unlock(&S.mu);
        return;
    }
    switch (type) {
    case RS_T_BUSY: /* asked again after a wait (rebuild_max, a promotion, a stale record) */
        STAT(busy, 1);
        vs_get(vol)->retry_at = ms_now() + RS_BUSY_WAIT_MS;
        if (arg == RS_B_MAKE)
            dst_abandon(j, "the leader could not make it", 0);
        else {
            say("volume %" PRIu32 ": node %" PRIu32 " is busy (%s): the snapshot is asked again", vol, from,
                arg == RS_B_PROMOTING ? "promoting" : arg == RS_B_MAX ? "rebuild_max" : "not the leader of this record");
            job_drop(j);
        }
        break;
    case RS_T_ABORT:
        dst_abandon(j, "the leader released it", 0);
        break;
    case RS_T_GO:
        if (S.srcstall && !S.srcstalled) { /* TSDB_REPLD_TEST=snapsrcstall: this source sends nothing more */
            S.srcstalled = 1;
            say("TEST: volume %" PRIu32 ": the source for node %" PRIu32 " ignores GO (snapsrcstall)", vol, from);
            break;
        }
        if (j->state == S_WAITGO || j->state == S_MAN) {
            j->state = S_SENDING;
            j->last_ms = ms_now();
            task_add(W_NEXT, j);
        }
        break;
    case RS_T_DONE:
        if (arg == 0) {
            STAT(sent, 1);
            say("volume %" PRIu32 ": snapshot sent to node %" PRIu32 ": %" PRIu64 " bytes in %.1f s", vol, from,
                j->moved, (double)(ms_now() - j->start_ms) / 1000);
            job_drop(j);
        } else
            src_abort(j, arg == -2 ? "the receiver has not the space for it" : "the receiver abandoned it");
        break;
    }
    pthread_mutex_unlock(&S.mu);
}

/* IN_SNAP: nothing is posted to the volume's thread (every step runs on a link's thread or the worker) */
void rsnap_in(struct rvol *rv, uint32_t node, const uint8_t *p, size_t n) { (void)rv, (void)node, (void)p, (void)n; }

void rsnap_tick(struct rvol *rv)
{
    uint64_t now = ms_now();
    pthread_mutex_lock(&S.mu);
    struct rvs *v = vs_get(rv->number);
    if (now - v->tick_ms < RS_TICK_EVERY_MS || !S.jobs) {
        pthread_mutex_unlock(&S.mu);
        return;
    }
    v->tick_ms = now;
    for (struct rjob *j = S.jobs; j; j = j->next) {
        if (j->dead || j->vol != rv->number)
            continue;
        if (j->role == R_DST) {
            if (rv->mode != M_REPLICA)
                dst_abandon(j, "this member is no longer a replica", 1);
            else if (now - j->last_ms >= S.stall_ms && j->state != S_PREP && j->state != S_COMPLETE &&
                     G.mutant != RM_SNAPNOSTALL)
                dst_abandon(j, "nothing arrived for 30 s (the stall timer)", 1);
            continue;
        }
        struct plink *l = NULL;
        pthread_mutex_lock(&G.mu);
        for (uint32_t i = 0; i < G.nn; i++)
            if (G.nodes[i].node == j->peer)
                l = G.nodes[i].snap;
        pthread_mutex_unlock(&G.mu);
        /* the link is dialed by the first file (SNAPMAN): a make sends nothing, however long it takes */
        if ((l && plink_up(l)) || (j->state == S_MAKING && G.mutant != RM_SNAPMAKEDOWN))
            j->down_ms = 0;
        else if (!j->down_ms)
            j->down_ms = now;
        if (rv->mode != M_WRITER || rv->failed || rv->epoch != j->epoch)
            src_abort(j, "this leader lost the epoch it was made in");
        else if (j->down_ms && now - j->down_ms >= S.down_ms && G.mutant != RM_SNAPNOSRCDOWN)
            src_abort(j, "the link to the receiver stayed down 60 s");
        else if (j->state == S_WAITDONE && now - j->last_ms >= S.waitdone_ms && G.mutant != RM_SNAPNOWAITDONE) /* its DONE 0 or -1 was lost */
            src_abort(j, "the receiver never said it had the last file (60 s)");
        else if (now - j->start_ms >= RS_SRC_MAX_MS)
            src_abort(j, "it is 6 hours old");
    }
    pthread_mutex_unlock(&S.mu);
}

/* ---- the worker */

static int dir_sync(const char *path)
{
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int rc = fsync(fd);
    close(fd);
    return rc;
}

static void w_prep(struct rjob *j)
{
    struct stat st;
    int ok = !(lstat(j->dir, &st) == 0 && snap_release(j->dir) != 0);
    int err = ok ? 0 : EIO;
    if (ok && (mkdir(j->dir, 0755) != 0 || dir_sync(G.dir) != 0))
        ok = 0, err = errno;
    pthread_mutex_lock(&S.mu);
    j->made = ok;
    if (!ok) {
        say("volume %" PRIu32 ": %s could not be made for a snapshot: %s", j->vol, j->dir, strerror(err));
        vs_get(j->vol)->retry_at = ms_now() + RS_BUSY_WAIT_MS;
        job_drop(j);
    } else if (!j->dead) {
        j->state = S_ASKED;
        j->last_ms = ms_now();
        STAT(asked, 1);
        rs_send_req(j->peer, j->vol, j->set_version, j->epoch, j->req); /* lost: rsnap_need asks again */
    }
    pthread_mutex_unlock(&S.mu);
}

/* TSDB_REPLD_TEST=snapflip: the first snapshot this source makes has one byte of a copied dictionary flipped after
 * SNAPMAN recorded its hash (rot on the source between make and send; a copy, never a link of a live file) */
static void flip_one(const char *dir, const struct snap_man *m)
{
    for (size_t i = 0; i < m->nfiles; i++) {
        const struct snap_file *f = &m->files[i];
        if (f->kind != SNAP_DICT || f->link || !f->size)
            continue;
        char path[PATH_MAX + 32 + 130];
        snprintf(path, sizeof path, "%s/%s", dir, f->name);
        int fd = open(path, O_RDWR | O_CLOEXEC);
        uint8_t b;
        if (fd >= 0 && pread(fd, &b, 1, (off_t)(f->size / 2)) == 1) {
            b ^= 1;
            if (pwrite(fd, &b, 1, (off_t)(f->size / 2)) == 1)
                say("TEST: a byte of %s flipped after the make (snapflip)", path);
        }
        if (fd >= 0)
            close(fd);
        return;
    }
}

static void w_make(struct rjob *j)
{
    struct snap_info info;
    memset(&info, 0, sizeof info);
    int rc = TSDB_ERR_UNAVAILABLE;
    uint64_t slow = __atomic_exchange_n(&S.slowmake_ms, 0, __ATOMIC_RELAXED);
    if (slow) { /* TSDB_REPLD_TEST=snapslowmake=MS: the first make takes that much longer (a big volume's hashes) */
        say("TEST: volume %" PRIu32 ": the make sleeps %" PRIu64 " ms first (snapslowmake)", j->vol, slow);
        struct timespec ts = {(time_t)(slow / 1000), (long)(slow % 1000) * 1000000};
        nanosleep(&ts, NULL);
    }
    char name[96];
    snprintf(name, sizeof name, SNAP_PREFIX "%" PRIu32 "_%016" PRIx64, j->peer, j->req);
    for (int a = 0; a < RS_MAKE_TRIES; a++) {
        struct eng *db = tsdb_cvol_ref(G.t, j->vol);
        if (!db)
            break;
        memset(&info, 0, sizeof info);
        info.hist = j->hist, info.nhist = j->nhist;
        rc = snap_make(db, name, &info);
        tsdb_cvol_unref(&db);
        if (rc != SNAP_RETRY)
            break;
    }
    struct snap_man m;
    memset(&m, 0, sizeof m);
    if (rc == 0 && (rc = snap_man_read(info.path, &m)) != 0)
        snap_release(info.path);
    if (rc == 0 && S.flip && !__atomic_exchange_n(&S.flipped, 1, __ATOMIC_RELAXED))
        flip_one(info.path, &m);
    pthread_mutex_lock(&S.mu);
    if (rc != 0) {
        say("volume %" PRIu32 ": a snapshot for node %" PRIu32 " could not be made: %s", j->vol, j->peer,
            tsdb_strerror(rc));
        rs_send(j->peer, RS_T_BUSY, j->vol, j->req, RS_B_MAKE);
        STAT(failed, 1);
        job_drop(j);
        pthread_mutex_unlock(&S.mu);
        return;
    }
    snprintf(j->dir, sizeof j->dir, "%s", info.path);
    j->made = 1, j->man = m, j->have_man = 1;
    for (size_t i = 0; i < m.nfiles; i++)
        if (!m.files[i].link)
            j->total += m.files[i].size;
    if (j->dead) { /* its W_DROP, queued after this task, releases it */
        pthread_mutex_unlock(&S.mu);
        return;
    }
    say("volume %" PRIu32 ": snapshot made for node %" PRIu32 ": P %" PRIu64 ", %zu files, %" PRIu64 " bytes; sent"
        " at %" PRIu64 " MB/s", j->vol, j->peer, info.p, info.nfiles, j->total, j->rate >> 20);
    j->state = S_MAN;
    j->busy++;
    pthread_mutex_unlock(&S.mu);
    const char *why = src_queue(j, RS_MAN);
    pthread_mutex_lock(&S.mu);
    j->busy--;
    pthread_cond_broadcast(&S.cv);
    if (why)
        src_abort(j, why);
    pthread_mutex_unlock(&S.mu);
}

static void w_next(struct rjob *j)
{
    pthread_mutex_lock(&S.mu);
    if (j->dead || !j->have_man) {
        pthread_mutex_unlock(&S.mu);
        return;
    }
    while (j->fnext < j->man.nfiles && j->man.files[j->fnext].link)
        j->fnext++;
    if (j->fnext == j->man.nfiles) {
        j->state = S_WAITDONE;
        pthread_mutex_unlock(&S.mu);
        return;
    }
    uint32_t idx = (uint32_t)j->fnext++;
    j->busy++;
    pthread_mutex_unlock(&S.mu);
    const char *why = src_queue(j, idx);
    pthread_mutex_lock(&S.mu);
    j->busy--;
    pthread_cond_broadcast(&S.cv);
    if (why)
        src_abort(j, why);
    pthread_mutex_unlock(&S.mu);
}

/* removes a dropped job's directory (a source's snapshot, a receiver's partial v<N>.snap) and frees it */
static void w_drop(struct rjob *j)
{
    pthread_mutex_lock(&S.mu);
    while (j->busy)
        pthread_cond_wait(&S.cv, &S.mu);
    for (struct rjob **pp = &S.jobs; *pp; pp = &(*pp)->next)
        if (*pp == j) {
            *pp = j->next;
            break;
        }
    pthread_mutex_unlock(&S.mu);
    if (j->made && j->dir[0] && snap_release(j->dir) != 0)
        say("volume %" PRIu32 ": %s could not be removed (the next open of the volume does)", j->vol, j->dir);
    job_free(j);
}

/* A complete copy (the worker, no lock held): refused when stale, else installed (snapinst.c). v<N>.snap stays made
 * (removed by w_drop) unless the install took it or left a SNAPDONE for snap_leftovers to finish. */
static void install(struct rjob *j)
{
    uint64_t log_end = 0, fence = 0, t0 = ms_now();
    int drv = 0;
    struct rvol *rv = vol_find(j->vol);
    if (rv) { /* the driver's view; the fence is checked again from REPL once the driver is stopped (snapinst.c) */
        pthread_mutex_lock(&rv->mu);
        if (rv->core && !rv->stop) {
            drv = 1;
            /* a DIVERGED replica's log is what the copy replaces: any P rebuilds it (out of the ISR since it failed,
             * so no commit counts on the positions above P it held) */
            log_end = rv->failed && rv->div_rebuild ? 0 : repl_durable_end(rv->core);
            fence = rv->st.fence;
        }
        pthread_mutex_unlock(&rv->mu);
    }
    const struct snap_man *m = &j->man;
    if (drv && ((m->p <= log_end && G.mutant != RM_SNAPINSTALLSTALE) || fence > m->p_epoch)) {
        say("volume %" PRIu32 ": the snapshot from node %" PRIu32 " is stale and not installed: P %" PRIu64 " of epoch %"
            PRIu64 ", this replica's log ends at %" PRIu64 ", fenced at %" PRIu64 " (asked again)", j->vol, j->peer,
            m->p, m->p_epoch, log_end, fence);
        STAT(refused_stale, 1);
        pthread_mutex_lock(&S.mu);
        vs_get(j->vol)->retry_at = 0;
        pthread_mutex_unlock(&S.mu);
        return;
    }
    crash_point(CP_SNAP_RECEIVED);
    struct snapinst_result res;
    int rc = snapinst_install(G.dir, j->vol, j->dir, m->p, m->p_epoch, &res);
    uint64_t took = ms_now() - t0;
    char done[sizeof j->dir + 16];
    snprintf(done, sizeof done, "%s/" SNAP_DONE, j->dir);
    struct stat st;
    if (res.installed || lstat(done, &st) == 0)
        j->made = 0; /* moved to v<N>, or a swap for snap_leftovers to finish: never removed here */
    if (rc == 0) {
        STAT(installed, 1);
        __atomic_store_n(&S.st.install_ms, took, __ATOMIC_RELAXED);
        say("volume %" PRIu32 ": snapshot installed: P %" PRIu64 " of epoch %" PRIu64 ", REPL fence %" PRIu64
            " (it was %" PRIu64 "), open generation %" PRIu64 ", in %.1f s; the volume opens again", j->vol, m->p,
            m->p_epoch, res.fence, res.keep_fence, res.open_gen, (double)took / 1000);
    } else if (rc == 1) {
        STAT(refused_stale, 1);
    } else if (rc == CVOL_NOT_TAKEN) {
        say("volume %" PRIu32 ": the snapshot is not installed: the volume is not open here or is stopping", j->vol);
    } else {
        STAT(failed, 1);
    }
    pthread_mutex_lock(&S.mu);
    vs_get(j->vol)->retry_at = 0; /* the volume's next open asks again if it still needs one */
    pthread_mutex_unlock(&S.mu);
}

/* every file of a receiver's copy is in: the link entries made, the directory synced, the source told; then the
 * install. The job is claimed here (dead, so nothing else drops it) and freed at the end. */
static void w_finish(struct rjob *j)
{
    pthread_mutex_lock(&S.mu);
    if (j->dead) { /* abandoned meanwhile: its W_DROP follows */
        pthread_mutex_unlock(&S.mu);
        return;
    }
    j->dead = 1;
    vs_get(j->vol)->retry_at = ms_now() + RS_STUB_WAIT_MS;
    pthread_mutex_unlock(&S.mu);
    int ok = 1;
    for (size_t i = 0; ok && i < j->man.nfiles; i++) {
        const struct snap_file *f = &j->man.files[i];
        if (!f->link)
            continue;
        char a[PATH_MAX + 32 + 130], b[PATH_MAX + 32 + 130];
        snprintf(a, sizeof a, "%s/%s", j->dir, j->man.files[f->link - 1].name);
        snprintf(b, sizeof b, "%s/%s", j->dir, f->name);
        ok = link(a, b) == 0 || errno == EEXIST;
    }
    for (size_t i = 0; ok && i < j->man.nfiles; i++) { /* a backstop of the count: every file there, whole */
        char a[PATH_MAX + 32 + 130];
        struct stat st;
        snprintf(a, sizeof a, "%s/%s", j->dir, j->man.files[i].name);
        if (stat(a, &st) != 0 || (uint64_t)st.st_size != j->man.files[i].size) {
            say("volume %" PRIu32 ": the snapshot's %s is missing or not of the size SNAPMAN names", j->vol, a);
            ok = 0;
        }
    }
    ok = ok && dir_sync(j->dir) == 0;
    uint64_t took = ms_now() - j->start_ms;
    if (!ok) {
        say("volume %" PRIu32 ": the snapshot from node %" PRIu32 " is abandoned: its directory could not be"
            " completed; asked again at the next notice", j->vol, j->peer);
        STAT(failed, 1);
        rs_send(j->peer, RS_T_DONE, j->vol, j->req, -1);
        pthread_mutex_lock(&S.mu);
        vs_get(j->vol)->retry_at = 0;
        pthread_mutex_unlock(&S.mu);
        w_drop(j);
        return;
    }
    if (S.nodone) /* TSDB_REPLD_TEST=snapnodone: the DONE 0 is lost; the source frees itself */
        say("TEST: volume %" PRIu32 ": the DONE of the snapshot is not sent (snapnodone)", j->vol);
    else
        rs_send(j->peer, RS_T_DONE, j->vol, j->req, 0);
    STAT(received, 1);
    __atomic_store_n(&S.st.last_ms, took, __ATOMIC_RELAXED);
    say("volume %" PRIu32 ": snapshot received from node %" PRIu32 ": %zu files, %" PRIu64 " bytes in %.1f s (%.1f"
        " MB/s)", j->vol, j->peer, j->man.nfiles, j->total, (double)took / 1000,
        took ? (double)j->total / (1 << 20) / ((double)took / 1000) : 0.0);
    if (S.hold) { /* TSDB_REPLD_TEST=snaphold: kept as v<N>.snapheld for the test to compare with SNAPMAN */
        char held[sizeof G.dir + 32];
        snprintf(held, sizeof held, "%s/v%" PRIu32 ".snapheld", G.dir, j->vol);
        struct stat st;
        if (lstat(held, &st) == 0)
            snap_release(held);
        if (rename(j->dir, held) == 0 && dir_sync(G.dir) == 0) {
            say("TEST: volume %" PRIu32 ": the snapshot is kept in %s (snaphold)", j->vol, held);
            j->made = 0;
        }
    }
    if (j->made)
        install(j);
    w_drop(j); /* removes v<N>.snap when still made */
}

static void *worker(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&S.mu);
    for (;;) {
        while (!S.wh && !S.stopping)
            pthread_cond_wait(&S.cv, &S.mu);
        if (S.stopping)
            break;
        struct wtask *t = S.wh;
        S.wh = t->next;
        if (!S.wh)
            S.wt = NULL;
        pthread_mutex_unlock(&S.mu);
        switch (t->what) {
        case W_PREP:
            w_prep(t->j);
            break;
        case W_MAKE:
            w_make(t->j);
            break;
        case W_NEXT:
            w_next(t->j);
            break;
        case W_FINISH:
            w_finish(t->j);
            break;
        case W_DROP:
            w_drop(t->j);
            break;
        }
        free(t);
        pthread_mutex_lock(&S.mu);
    }
    pthread_mutex_unlock(&S.mu);
    return NULL;
}

void rsnap_quiesce(void)
{
    pthread_mutex_lock(&S.mu);
    S.stopping = 1;
    pthread_cond_broadcast(&S.cv);
    int started = S.started;
    S.started = 0;
    pthread_mutex_unlock(&S.mu);
    if (started)
        pthread_join(S.th, NULL);
}

void rsnap_stop(struct rvol *rv)
{
    /* the links are freed and the worker is gone: what is left of this volume is freed with no I/O (the next open
     * sweeps snap_*; snap_leftovers removes a v<N>.snap without SNAPDONE) */
    pthread_mutex_lock(&S.mu);
    for (struct wtask **pp = &S.wh; *pp;) {
        struct wtask *t = *pp;
        if (t->j->vol == rv->number) {
            *pp = t->next;
            free(t);
        } else
            pp = &t->next;
    }
    S.wt = NULL;
    for (struct wtask *t = S.wh; t; t = t->next)
        S.wt = t;
    for (struct rjob **pp = &S.jobs; *pp;) {
        struct rjob *j = *pp;
        if (j->vol == rv->number) {
            *pp = j->next;
            job_free(j);
        } else
            pp = &j->next;
    }
    pthread_mutex_unlock(&S.mu);
}

int rsnap_owns(struct plink *l, uint64_t file, const void *meta, size_t meta_len)
{
    if (G.mutant != RM_SNAPSAMELINK)
        return 0;
    if (meta) /* a begin: a snapshot's meta is RS_META_LEN bytes, a COMMIT's file's (replwire) is not */
        return meta_len == RS_META_LEN;
    pthread_mutex_lock(&S.mu);
    int own = dst_by_file(l, file) != NULL;
    pthread_mutex_unlock(&S.mu);
    return own;
}
