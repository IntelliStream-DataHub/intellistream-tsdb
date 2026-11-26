/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * jepsen-cluster: a Jepsen-style run of a tsdbd cluster (docs/design-cluster.md 13.2, design-cluster-data.md 10.3).
 *
 *   jepsen_cluster --dir DIR [--nodes 5] [--seconds 30] [--nemesis kill,partition,pause,midcommit,power,fwdstall,
 *                  kill2,slow,longdown,rebuild,churn] [--workload meta,kv-local|kv|kv-via|classes] [--seed N]
 *                  [--crashfs build/libcrashfs.so] [--outagefs build/liboutagefs.so] [--retain-s S] [--mutant NAME]
 *
 * What it is. A runner starts 3-8 tsdbd (default 5), each a direct child with --port 0 --port-file,
 * --exit-with-parent=<this pid>, --cluster, a --state-dir and a --peer-advertise that names a proxy of this harness,
 * under a random cluster secret. It keeps a node table {pid, client port, peer port, proxy port} that is refreshed
 * only from a node's port file after a restart and checked with th_owns_port (AGENTS.md, Ports): a client connects
 * only to a port of that table and asks after the connect whether the node's pid LISTENS on it; tsdbc_set_retry(c, 0),
 * so the history records what the node answered, and a MOVED would be used for its node id only.
 *
 * The proxy. One listener per node (port 0, bound before the node starts; the node advertises it), one thread per
 * accepted link, so the whole peer traffic of the metadata group (Raft, heartbeats, the watch stream, JOIN and the
 * PROBE of a node add) goes through it: every link a later step adds on the peer port is seen too. The source member
 * id is in the plaintext HELLO. From AUTH on every frame carries an HMAC tag over a per-direction sequence number, so
 * single frames cannot be dropped or reordered without the receiver ending the link: a "drop" is a blackhole of
 * the whole pair. cut[from][to] holds the bytes of that direction (not read, and not forwarded) and holds the HELLO
 * of a new link unanswered; when the cut is lifted every held link is reset (both sockets), as a healed network
 * that lost its packets would be. delay[from][to] holds whole byte runs back in order. The admin ops of the CLI
 * (id 0 in the HELLO) are never touched: they go to the real peer port as `tsdb cluster` does. The proxy counts the
 * bytes forwarded per direction: a pair that was cut must not have exchanged any (the check that fails a partition
 * which was not applied, --mutant noop-partition).
 *
 * Nemeses (rounds, each healed and followed by a settle: every node up, one leader, every member UP, the same
 * metadata): kill (SIGKILL, restart; the Raft leader half of the time), partition (random halves, the leader cut from
 * everybody, a replica cut from the Raft leader only, a bridge, one direction, delays), pause (SIGSTOP for longer
 * than node_timeout, or than the lease), midcommit (a crash point of server/crashpt.h armed with TSDB_CRASH_AT: the
 * node ends with status 99 at it, is restarted and the hit counted; an armed point that never fired fails the run),
 * power (crashfs under the node's directory, SIGUSR2, exit status 99; at half of the starts, drawn from the seed,
 * CRASHFS_TEAR: unsynced appends cut at a sector or ended in garbage, sectors rewritten in place old or new each,
 * which reaches the torn and unsure paths of the two-slot files (REPL, FNO, the Raft hard state); at the other half
 * CRASHFS_KEEP_OLD: the least a disk must keep, rewrites in place come back old).
 *
 * R4's nemeses (the R4 plan; each is refused with a line while the track it needs is not on main: `r4nem[].built`):
 * kill2 (two servers SIGKILLed at once and restarted after 5 to 60 s: a volume may be unavailable meanwhile, and once
 * both are back every acknowledged write must be there: a kill is not a lost disk), slow (one server's whole data root
 * slowed for 5 to 30 s through the outagefs shim's `slow MS`, stacked with crashfs when power runs too: --outagefs),
 * longdown (a replica of kvr down past repl_retain_s, 3 s unless --retain-s says otherwise, under writes, then back:
 * it must take a snapshot, which shows as another inode of its v<N> (vdir_ino); the run fails when no round took one),
 * rebuild (a stopped
 * replica's v<N> removed, the server started, which refuses the volume, `tsdb cluster replica reset`, rebuilt by a
 * snapshot: R4's rebuild, a lost directory; design-cluster.md 13.2's "declare a server out, add a new one" is R9's) and
 * churn (`volume transfer` and `volume move --log` at random under the workloads, a move now and then cancelled; moves go
 * between the nodes, so kvr keeps 3 replicas; when churn is the only nemesis, every node's voltrace is followed and the
 * moved-from member must not leave the ISR before the new one joined, nor with fewer ISR members: r4_move_trace_check).
 * At the end of every run with kvr: the members of the final record are the
 * replicas compared (replicas_equal, rep_check), none is a learner, and no `v<N>.snap`, `v<N>.old`, `v<N>.removed-*` or
 * `snap_*` is left on any node. R4's crash points (server/crashpt.h: snap-received, snap-swapped, set-removed,
 * transfer-ready, move-added, move-shrunk, those that exist) are terminal points of kv and classes runs whose
 * nemeses provide their context (nem_terminal_r4). A refused nemesis exits 2; JEPSEN_R4_TRY=1 runs it anyway.
 *
 * Workloads. meta: sampled STATUS of every node; a term has at most one leader (election safety), every node gets
 * back, every member UP after each heal, the metadata equal on every node. kv-local: per node, one writer and one
 * reader on that node's own series, in a cluster volume of one replica on that node (kv_setup, kv_resolve), a history of writes (acknowledged / definitely failed /
 * indeterminate) and reads, and a checker, as in jepsen.c, with writes that are totally ordered by their one writer:
 *  1 durability: the final value of a key is not from a write that completed before an applied write of the key began;
 *  2 validity: a value seen was written at that key by a write that had begun and did not definitely fail;
 *  3 atomic visibility: a read that sees w at one of its keys sees w or newer at the others;
 *  4 no stale reads: a read that began after w was acknowledged sees w or newer at w's keys;
 *  5 monotonic reads: a session's later read is not older than an earlier one at any key.
 * kv and classes (not with kv-local in one run: kv-local's nodes run with TSDB_CLUSTER_WRITES=1, these with the
 * replication driver): one cluster volume "kvr" of 3 replicas, two series with a writer and a reader each through its
 * leader (checks 1-5 as above) and a filler series of inserts of 50-1550 rows at a frontier whose rate changes every
 * minute of timestamps (most of them large writes, shipped as files); classes adds small partitions and granules,
 * quick flushes and idle merges so that series switch partition classes (CLASS markers in the stream). A replica silent
 * for isr_lag_ms (1 s) leaves kvr's ISR and the writes go on without it (one it held up until then may time out:
 * indeterminate, never a failure); it joins again once caught up. kvr's leader is a target of kill, power, pause and
 * a midcommit restart like any node (half of the picks go to it: pick_node), so the group fails the volume over under
 * load; the clients follow its leader as the record names it (kv_leader, from the sampler) and reconnect on MOVED or
 * UNAVAILABLE, and the readers go on through every failover: a node that does not lead kvr answers MOVED (tsdbd D5), so
 * a stale read can only come from a deposed leader that still answers, which its lease must prevent (check 4).
 * midcommit also arms one of the crash points of the driver (wal-before-send, acked-before-ok, prepared-before-commit,
 * class-switch: the leader ends there) or of a failover (fence-saved-before-reply: a replica, promote-before-epoch: the
 * new leader, depose-before-reopen: the old one) on every node at every start behind a trigger file, made past the
 * middle of the run (nem_terminal; a failover point after a cut of the leader from every other node, which makes the
 * failover). The trigger goes as soon as a node ended there, before anything is restarted, so the node that leads next
 * does not end at the same point (and restarts from then on arm nothing). The end check reads every replica directory in place, read-only, after the clean
 * stop (tests/cvol_check.c): the run first waits (90 s) for kvr's ISR to be whole again, then the replicas hold the
 * same rows (replicas_equal) and the keys of the leader's final read, and every replica holds each of the filler's
 * large writes whole or not at all, whole when acknowledged (fill_check).
 *
 * kv-via (--workload kv-via; S2-2, forwarding): kv's volume, writers and readers, but each client sends its operations
 * through a random member (a third of the picks of pick_node go to a node a client is on), so most of them reach kvr's
 * leader as a FORWARD of the entry node (server/fwdreq.h); `via_entry` is the choice. Checks 1-5 as in kv, plus
 *  6 read-your-writes: the read the writer thread makes on the connection of its own acknowledged write sees that
 *    write or newer (a read answered by a lagging replica fails it: the run under
 *    TSDB_REPLICA_READS=1, a test hook that lets replicas with a lease answer reads, must fail, selftest);
 *  7 old frame: nemesis fwdstall steers the writers to a random member that does not lead kvr and makes it hold its
 *    next forwards (the hook file `fwd-stall` in its directory, TSDBD_TEST_FWD_STALL) for 2 s, beyond the 1 s forward
 *    age, so a frame the entry sent late would be a write the client has long given up on: no write that begins in the
 *    window and lasts 1.3 s or more may be acknowledged by it (TSDBD_FWD_MUTANT=fwdoldframe must fail, selftest). The
 *    run also fails when no write, read and read-your-writes read went through a member that does not lead kvr, and
 *    when fwdstall held no write at all (up to 3 attempts per round; the misses are counted in the summary).
 * users (--workload users, implies --auth: every tsdbd starts with a login, the cluster with a superuser made by the CLI;
 * tests/jepsen/cluster_users.h has the design and the checks): an admin thread adds, drops and re-adds users and changes
 * their passwords and grants through random members, one prober per member logs in and reads as them; an acknowledged
 * change must hold on every member that is bound by it (not one that was paused or cut off, a restarted one only after its
 * barrier), through kill, power, partition and pause. The server mutants TSDB_CLUSTER_MUTANT=dropnotcommitted and
 * TSDB_AUTH_MUTANT=loginbeforecatchup must fail it (`make jepsen-cluster-selftest`).
 *
 * ec (--ec K+M, with kv, kv-via or classes; design-cluster-data.md 10.3, tests/jepsen/cluster_ec.h has the design and the
 * checks): kvr is an erasure-coded volume (VOLUME_ADD_EC, m = 2: 3 replicas), every node has two --chunk-dir stores made
 * with `tsdb chunk-store init` once the cluster has member ids, --metrics-port auto and a short settle grace. The keys
 * of kv are back-dated (2023), so their partition is past the grace and settles while the writers go on with late rows.
 * Extra nemeses: partial (a stripe member cut from the metadata leader only), diskloss (a node's chunk stores removed
 * and made again empty), rot (bytes flipped in a chunk at rest). Extra checks: no DIVERGED volume (/metrics, polled all
 * run) in a run without rot or disk loss, the settles counted (.ec parts on the replicas) when tsdbd settles, and the
 * final answers equal from every ISR member in turn as the leader.
 *
 * Selftest (--selftest-checker, --mutant NAME; each must fail, `make jepsen-cluster-selftest`): foreign-pid (a port file that names another
 * pid), takeover (a stale table, a foreign server on the node's old port, and a client that skips th_owns_port),
 * takeover-checked (the same with the check: refused, the run passes), noop-partition, unfired (an armed crash point
 * nobody placed), idle (a nemesis that sleeps through its naps), lost-write (a write missing from the final state), nokill, nopause,
 * nodelay and noreset (a kill, pause or delay that does nothing, a heal that resumes held links: the run fails),
 * nolongdown (longdown's node is back after 300 ms: no round takes a snapshot, the run fails), nosnapdone (rebuild's
 * check of the snapshot's mark sees none: the run fails),
 * rep-lost-write (kv: an acknowledged write missing from one replica's directory, also under midcommit),
 * rep-half-batch (kv: half of an
 * acknowledged large write of the filler missing from one replica's directory).
 *
 * Exit status: 0 ok, 1 a violation or a failed expectation, 2 the harness itself could not run.
 */
#define _GNU_SOURCE
#include "tsdb_client.h"
#include "cluster.h"
#include "crashpt.h"
#include "cvol_check.h"
#include "harness.h"
#include "meta.h"
#include "tsdb.h"
#include "users.h"
#include "outagefs_ctl.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <time.h>

#define MAXN 22
#define KEYS 40
#define KV_BASE_TS 1700000000000LL
#define RAFT_LEADER_ROLE 3
#define POWER_EXIT 99

extern char **environ;

/* ------------------------------------------------------------------ options, clock, randomness */

static int nn = 5;
static int seconds = 30;
static uint64_t seed = 1;
static const char *opt_dir = "/var/tmp/tsdb-jepsen-cluster";
static const char *crashfs_so;
static const char *mutant = "";
static char rundir[PATH_MAX - 64], secret_path[PATH_MAX];
static uint8_t secret[128];
static size_t secret_len;
static int want_meta = 1, want_kv = 1;
static int want_rep, want_classes; /* workloads kv and classes: one volume of 3 replicas, written through its leader */
static int want_via;               /* workload kv-via: kv, with the clients on entry nodes (forwarding), RYW, fwdstall */
static int want_ec_k, want_ec_m; /* --ec K+M: kvr is an erasure-coded volume (cluster_ec.h) */
static int want_auth, want_users; /* --auth: every tsdbd requires a login (a superuser from the init); workload users */
static const char *opt_peer_rdma; /* --peer-rdma auto|on|off (NULL: tsdbd default) */
static int opt_peer_rdma_pool_mb; /* --peer-rdma-pool-mb N (0: tsdbd default) */
static const char *opt_peer_rdma_dev; /* --peer-rdma-device DEV[:PORT] */
static void ju_fault(int i);      /* cluster_users.h: the nemeses tell the users workload which members may lag */
static void ju_cut_done(const uint8_t in[MAXN][MAXN], int n); /* the members the cuts leave in step stop being exempt */
static void ju_settled(void);
static struct meta_settings S;
static _Atomic uint64_t ec_replaces; /* --ec: the kv writers' replaces of one key (cluster_ec.h) */
static int ec_settles; /* this build + tsdbd settles ec volumes (TSDB_EC compiled in, chunk stores supported) */
static void ec_add_kvr(void);
static void rmtree(const char *path);

enum {
    N_KILL = 1, N_PARTITION = 2, N_PAUSE = 4, N_MIDCOMMIT = 8, N_POWER = 16, N_FWDSTALL = 32,
    N_KILL2 = 64, N_SLOW = 128, N_LONGDOWN = 256, N_REBUILD = 512, N_CHURN = 1024,
    N_PARTIAL = 2048, N_DISKLOSS = 4096, N_ROT = 8192, /* --ec runs (cluster_ec.h) */
    N_RDMA_QPERR = 16384 /* mid-stream RDMA QP error via TSDB_PLRDMA_TEST=qperr_file=... */
};
#define NNEM 15
#define N_EC (N_PARTIAL | N_DISKLOSS | N_ROT)
#define N_R4_KV (N_LONGDOWN | N_REBUILD | N_CHURN) /* these need kvr: workload kv, kv-via or classes */

/* R4's nemeses and the tracks they need. `built`: that track is on main (flip it when it merges; the run is refused
 * with a line until then, never skipped silently). JEPSEN_R4_TRY=1 runs a refused nemesis anyway (the track owner's
 * first run). Their cases of crash points follow crashpt.h, not this table. */
static const struct r4nem {
    const char *name;
    int bit, built;
    const char *needs;
} r4nem[] = {{"kill2", N_KILL2, 1, "nothing new"},
             {"slow", N_SLOW, 1, "nothing new in tsdbd (the outagefs shim: --outagefs build/liboutagefs.so)"},
             {"longdown", N_LONGDOWN, 1, "r4-xferlink and r4-xferinst (a snapshot for a replica past repl_retain_s)"},
             {"rebuild", N_REBUILD, 1, "r4-setchg and r4-xferinst (replica reset, rebuilt by a snapshot)"},
             {"churn", N_CHURN, 1, "r4-transfer, r4-setchg and r4-move (volume transfer, volume move --log)"}};
#define NR4NEM ((int)(sizeof r4nem / sizeof r4nem[0]))
static int r4_bit(const char *name)
{
    for (int k = 0; k < NR4NEM; k++)
        if (!strcmp(name, r4nem[k].name))
            return r4nem[k].bit;
    return 0;
}
/* r4-replay (the replay start apart from the log start, D17): a classes run keeps the default caps and the switches no
 * longer wait for them (cls_defer_s 0: the replay floor is the members' replay start, past their kept segments, which a
 * flush after the promotion raises within seconds). 0 puts back the cap of repl_retain_s at CLASSES_RETAIN_S. */
#define R4_REPLAY_ON_MAIN 1
static int opt_retain_s = -1;     /* --retain-s: repl_retain_s of the cluster (-1: as the run needs) */
static int retain_eff;            /* the repl_retain_s the cluster is made with (0: the default caps) */
static int cls_defer_s;           /* how long a promoted writer's class switches wait for the members' log start */
static const char *outagefs_so;   /* --outagefs: the shim under every server (the slow nemesis) */
static int use_outagefs;
static int nemeses = N_KILL | N_PARTITION | N_PAUSE | N_MIDCOMMIT;

static int64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
static int64_t now_ms(void) { return now_ns() / 1000000; }

/* JEPSEN_TRACE=1: the nemesis's actions and kvr's leader changes, stamped with CLOCK_MONOTONIC ms (one clock for every
 * process of the host), and the node and times of the operations a stale or non-monotonic read involves */
static int trace_on;
static void ev(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void ev(const char *fmt, ...)
{
    if (!trace_on)
        return;
    char b[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    fprintf(stderr, "jepsen-cluster: [%lld] %s\n", (long long)now_ms(), b);
}

static uint64_t mix(uint64_t x)
{
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
static uint64_t rng_state;
static uint64_t rnd(void) { return rng_state = mix(rng_state); }
static int rnd_range(int lo, int hi) { return lo + (int)(rnd() % (uint64_t)(hi - lo + 1)); } /* control thread only */

/* ------------------------------------------------------------------ the node table */

struct node {
    pid_t pid;      /* 0: not running */
    int cport;      /* the client port of the running server, from its port file */
    int pport;      /* its real peer port */
    int cport_stale; /* --mutant takeover: the client port the workload clients keep using (0: use cport) */
    int paused;
    int proxy_fd, proxy_port;
    int starts, armed_hit;
    int64_t proc_ns; /* when the running process was forked (now_ns); the users workload tells a restart by it */
    _Atomic uint32_t id; /* member id, 0 until it joined */
    char dir[PATH_MAX + 16], log[PATH_MAX + 32];
    char chunk[2][PATH_MAX + 32]; /* --ec: the node's two chunk stores ("" until made after the cluster has member ids) */
    int mport;                    /* its metrics port (--ec), from its port file */
    char octl[PATH_MAX + 32]; /* the outagefs shim's control socket of this node (--outagefs: the slow nemesis) */
    _Atomic uint64_t peak_rss_kb; /* highest VmHWM/VmRSS seen for this node across the run */
    _Atomic unsigned peak_fds;    /* highest open fd count seen for this node across the run */
};
static struct node nodes[MAXN];
static pthread_mutex_t tbl_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int stopping;

static void node_snap(int i, pid_t *pid, int *cport, int *pport)
{
    pthread_mutex_lock(&tbl_mu);
    if (pid)
        *pid = nodes[i].pid;
    if (cport)
        *cport = nodes[i].cport;
    if (pport)
        *pport = nodes[i].pport;
    pthread_mutex_unlock(&tbl_mu);
}

static int node_paused(int i)
{
    pthread_mutex_lock(&tbl_mu);
    int p = nodes[i].paused;
    pthread_mutex_unlock(&tbl_mu);
    return p;
}

static int idx_of_id(uint32_t id)
{
    if (!id)
        return -1;
    for (int i = 0; i < nn; i++)
        if (atomic_load(&nodes[i].id) == id)
            return i;
    return -1;
}

/* signals only a child this run started: never pid 0 or -1 (AGENTS.md) */
static int signal_child(pid_t pid, int sig)
{
    if (pid <= 0)
        return -1;
    return kill(pid, sig);
}

static void kill_everything(void)
{
    for (int i = 0; i < nn; i++)
        if (nodes[i].pid > 0) {
            signal_child(nodes[i].pid, SIGCONT);
            signal_child(nodes[i].pid, SIGKILL);
        }
}

static void rmtree(const char *path);
static int keep_run = 1;

static void die(int code, const char *fmt, ...)
{
    static int dying;
    if (dying++)
        _exit(code);
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "jepsen-cluster: FAIL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n  (seed %" PRIu64 ", nemeses %d, logs of the nodes in %s)\n", seed, nemeses, rundir);
    va_end(ap);
    atomic_store(&stopping, 1);
    kill_everything();
    exit(code);
}

/* ------------------------------------------------------------------ the proxy */

static _Atomic uint8_t cut[MAXN][MAXN];       /* [from][to]: the bytes of that direction are held */
static _Atomic uint32_t delay_ms[MAXN][MAXN]; /* [from][to]: each run of bytes waits this long first */
static _Atomic uint64_t fwd[MAXN][MAXN];      /* bytes forwarded [from][to] */
static _Atomic uint64_t held_links, reset_links, delayed_bytes, unmapped_links;
static _Atomic int table_ready; /* every member's id is in the node table (set when the setup is done) */

struct run {
    uint8_t *p;
    size_t len, off;
    int64_t due;
};
struct dirq {
    struct run *r;
    size_t n, cap, head, bytes;
};
static void dq_push(struct dirq *q, const uint8_t *p, size_t len, int64_t due)
{
    if (q->n == q->cap) {
        q->cap = q->cap ? q->cap * 2 : 16;
        q->r = realloc(q->r, q->cap * sizeof *q->r);
        if (!q->r)
            die(2, "out of memory in the proxy");
    }
    uint8_t *c = malloc(len);
    if (!c)
        die(2, "out of memory in the proxy");
    memcpy(c, p, len);
    q->r[q->n++] = (struct run){c, len, 0, due};
    q->bytes += len;
}
static void dq_free(struct dirq *q)
{
    for (size_t i = q->head; i < q->n; i++)
        free(q->r[i].p);
    free(q->r);
}

struct pc {
    int a, dst;
};
#define PROXY_QCAP (256 * 1024)

static uint32_t le32(const uint8_t *p) { return p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }

static void *pconn(void *arg)
{
    struct pc *pc = arg;
    int a = pc->a, dst = pc->dst, b = -1, src = -1, held = 0, eof[2] = {0, 0}, reset = 0, counted = 0;
    uint32_t hid = 0;
    free(pc);
    struct dirq q[2];
    memset(q, 0, sizeof q);
    uint8_t buf[16384];
    size_t hl = 0, need = 0;
    int64_t t0 = now_ms();
    /* the plaintext HELLO: [u32 len][u8 type 1][magic u32][version u8][node id u32]... */
    while (!atomic_load(&stopping) && !(need && hl >= need)) {
        struct pollfd pf = {a, POLLIN, 0};
        int r = poll(&pf, 1, 50);
        if (r < 0 && errno != EINTR)
            goto out;
        if (r > 0) {
            ssize_t n = recv(a, buf + hl, 512 - hl, 0);
            if (n <= 0)
                goto out;
            hl += (size_t)n;
            if (hl >= 4) {
                need = 4 + (size_t)le32(buf);
                if (need > 512 || need < 14)
                    goto out;
            }
        }
        if (now_ms() - t0 > 5000)
            goto out;
    }
    if (atomic_load(&stopping))
        goto out;
    if (buf[4] == 1) { /* F_HELLO */
        hid = le32(buf + 10);
        src = idx_of_id(hid);
    }
    dq_push(&q[0], buf, hl, now_ms() + (src >= 0 ? atomic_load(&delay_ms[src][dst]) : 0));
    while (!atomic_load(&stopping)) {
        /* a node just added dials with the id the cluster gave it before the harness has read it: look again, and
         * only a member still unknown once the table is complete is a link nobody could cut or see */
        if (src < 0 && hid) {
            src = idx_of_id(hid);
            if (src < 0 && atomic_load(&table_ready) && !counted) {
                counted = 1;
                atomic_fetch_add(&unmapped_links, 1);
            }
        }
        int cf = src >= 0 && atomic_load(&cut[src][dst]), cb = src >= 0 && atomic_load(&cut[dst][src]);
        if (cf || cb) {
            if (!held)
                atomic_fetch_add(&held_links, 1);
            held = 1;
        } else if (held) { /* healed: what was held is gone, both sockets are reset */
            if (!strcmp(mutant, "noreset")) { /* planted: the held bytes are delivered after the heal instead */
                held = 0;
            } else {
                reset = 1;
                break;
            }
        }
        int64_t now = now_ms();
        if (b < 0 && !cf && q[0].head < q[0].n) { /* the first byte that may pass: dial the node's real peer port */
            pid_t pid;
            int pp;
            node_snap(dst, &pid, NULL, &pp);
            if (pid <= 0 || pp <= 0)
                break;
            b = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)pp)};
            sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            if (b < 0 || connect(b, (struct sockaddr *)&sa, sizeof sa) != 0)
                break;
            if (!th_owns_port(pid, pp)) /* the node may be gone and its port another process's */
                break;
            fcntl(b, F_SETFL, fcntl(b, F_GETFL) | O_NONBLOCK);
        }
        for (int d = 0; d < 2; d++) {
            int blocked = d == 0 ? cf : cb, out = d == 0 ? b : a;
            while (!blocked && out >= 0 && q[d].head < q[d].n && q[d].r[q[d].head].due <= now) {
                struct run *r = &q[d].r[q[d].head];
                ssize_t n = send(out, r->p + r->off, r->len - r->off, MSG_NOSIGNAL | MSG_DONTWAIT);
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                    break;
                if (n < 0)
                    goto out;
                if (src >= 0)
                    atomic_fetch_add(&fwd[d == 0 ? src : dst][d == 0 ? dst : src], (uint64_t)n);
                r->off += (size_t)n;
                q[d].bytes -= (size_t)n;
                if (r->off == r->len) {
                    free(r->p);
                    q[d].head++;
                }
            }
            if (eof[d] && q[d].head == q[d].n && !blocked)
                goto out;
        }
        struct pollfd pfd[2];
        int fdidx[2], np = 0;
        for (int d = 0; d < 2; d++) {
            int in = d == 0 ? a : b, blocked = d == 0 ? cf : cb;
            if (in < 0)
                continue;
            pfd[np].fd = in;
            pfd[np].events = (!blocked && !eof[d] && q[d].bytes < PROXY_QCAP) ? POLLIN : 0;
            pfd[np].revents = 0;
            fdidx[np++] = d;
        }
        if (poll(pfd, (nfds_t)np, 10) < 0 && errno != EINTR)
            break;
        now = now_ms();
        for (int k = 0; k < np; k++) {
            int d = fdidx[k];
            if (pfd[k].revents & POLLIN) {
                ssize_t n = recv(pfd[k].fd, buf, sizeof buf, MSG_DONTWAIT);
                if (n == 0)
                    eof[d] = 1;
                else if (n < 0 && errno != EAGAIN && errno != EINTR)
                    goto out;
                else if (n > 0) {
                    int from = d == 0 ? src : dst, to = d == 0 ? dst : src;
                    uint32_t dl = from >= 0 && to >= 0 ? atomic_load(&delay_ms[from][to]) : 0;
                    if (dl)
                        atomic_fetch_add(&delayed_bytes, (uint64_t)n);
                    dq_push(&q[d], buf, (size_t)n, now + dl);
                }
            } else if (pfd[k].revents & (POLLHUP | POLLERR)) {
                eof[d] = 1;
                if (q[d].head == q[d].n)
                    goto out;
            }
        }
    }
out:
    if (reset) {
        struct linger lg = {1, 0};
        setsockopt(a, SOL_SOCKET, SO_LINGER, &lg, sizeof lg);
        if (b >= 0)
            setsockopt(b, SOL_SOCKET, SO_LINGER, &lg, sizeof lg);
        atomic_fetch_add(&reset_links, 1);
    }
    close(a);
    if (b >= 0)
        close(b);
    dq_free(&q[0]);
    dq_free(&q[1]);
    return NULL;
}

static void *accept_thread(void *arg)
{
    int i = (int)(intptr_t)arg;
    while (!atomic_load(&stopping)) {
        struct pollfd pf = {nodes[i].proxy_fd, POLLIN, 0};
        if (poll(&pf, 1, 100) <= 0)
            continue;
        int a = accept4(nodes[i].proxy_fd, NULL, NULL, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (a < 0)
            continue;
        struct pc *pc = malloc(sizeof *pc);
        if (!pc) {
            close(a);
            continue;
        }
        *pc = (struct pc){a, i};
        pthread_t t;
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
#if !defined(__SANITIZE_ADDRESS__) && !defined(__SANITIZE_THREAD__)
        pthread_attr_setstacksize(&at, 512 * 1024);
#endif
        if (pthread_create(&t, &at, pconn, pc) != 0) {
            close(a);
            free(pc);
        }
        pthread_attr_destroy(&at);
    }
    return NULL;
}

static void proxy_start(int i)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t sl = sizeof sa;
    if (fd < 0 || bind(fd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(fd, 64) != 0 ||
        getsockname(fd, (struct sockaddr *)&sa, &sl) != 0)
        die(2, "proxy listener for node %d: %s", i, strerror(errno));
    nodes[i].proxy_fd = fd;
    nodes[i].proxy_port = ntohs(sa.sin_port);
    pthread_t t;
    pthread_create(&t, NULL, accept_thread, (void *)(intptr_t)i);
    pthread_detach(t);
}

/* the cut and delay matrices: a shape is a list of directed pairs */
static uint64_t cut_snap[MAXN][MAXN];
static uint8_t intended[MAXN][MAXN]; /* what the nemesis meant to cut: the check goes by this, not by the rules set */
static int cut_active;
static int cut_violations;
static int partitions_applied, partitions_planted_noop;

static void net_heal(void)
{
    if (cut_active) {
        /* the check: while a direction was cut, no byte went through it */
        for (int f = 0; f < nn; f++)
            for (int t = 0; t < nn; t++)
                if (f != t && intended[f][t] && atomic_load(&fwd[f][t]) != cut_snap[f][t]) {
                    cut_violations++;
                    fprintf(stderr, "jepsen-cluster: node %d -> node %d exchanged %" PRIu64
                            " bytes while the pair was cut\n", f, t, atomic_load(&fwd[f][t]) - cut_snap[f][t]);
                }
    }
    cut_active = 0;
    for (int f = 0; f < MAXN; f++)
        for (int t = 0; t < MAXN; t++) {
            atomic_store(&cut[f][t], 0);
            atomic_store(&delay_ms[f][t], 0);
            intended[f][t] = 0;
        }
}

/* cuts from -> to (and back when both); `planted` is --mutant noop-partition: nothing is applied, the check stays */
static void net_cut(int from, int to, int both)
{
    ju_fault(-1); /* a member behind a cut keeps the users it has (design-auth.md 12); net_cut_done frees those the cuts miss */
    intended[from][to] = 1;
    if (both)
        intended[to][from] = 1;
    if (!strcmp(mutant, "noop-partition")) { /* the planted no-op: the rules are never set */
        partitions_planted_noop++;
        return;
    }
    atomic_store(&cut[from][to], 1);
    if (both)
        atomic_store(&cut[to][from], 1);
}

/* after the rules are set: let what was in flight pass, then take the byte counts the check compares at the heal */
static void net_cut_done(void)
{
    cut_active = 1;
    partitions_applied++;
    ju_cut_done(intended, nn);
    usleep(200 * 1000);
    for (int f = 0; f < nn; f++)
        for (int t = 0; t < nn; t++)
            cut_snap[f][t] = atomic_load(&fwd[f][t]);
}

/* ------------------------------------------------------------------ starting, stopping and asking the nodes */

static int use_crashfs;
static _Atomic uint64_t crashfs_starts, crashfs_tears; /* node starts under crashfs, and those with CRASHFS_TEAR */

struct pfcopy {
    char from[PATH_MAX + 128], to[PATH_MAX + 128];
};
/* --mutant foreign-pid: what the node's port file says, with the pid of the harness instead of the node's */
static void *pf_rewriter(void *arg)
{
    struct pfcopy *c = arg;
    for (int i = 0; i < 6000 && !atomic_load(&stopping); i++) {
        FILE *f = fopen(c->from, "r");
        if (f) {
            int p = 0, pid = 0, m = 0, q = 0;
            int n = fscanf(f, "%d %d %d %d", &p, &pid, &m, &q);
            fclose(f);
            if (n >= 2) {
                FILE *o = fopen(c->to, "w");
                if (o) {
                    fprintf(o, "%d %d %d %d\n", p, (int)getpid(), m, q);
                    fclose(o);
                }
                break;
            }
        }
        usleep(2000);
    }
    free(c);
    return NULL;
}

/* Spawns node i (fork + execve); writes its portfile path to `seen` and fork time to `*t_fork_out`. */
static pid_t spawn_node(int i, const char *crash_at, char *seen, size_t seen_cap, int64_t *t_fork_out)
{
    struct node *n = &nodes[i];
    char pf[PATH_MAX + 128], flag[64], padv[64], state[PATH_MAX + 32], io[16];
    th_portfile(pf, sizeof pf, n->dir);
    snprintf(seen, seen_cap, "%s", pf);
    th_follow_me(flag, sizeof flag);
    snprintf(padv, sizeof padv, "127.0.0.1:%d", n->proxy_port);
    snprintf(state, sizeof state, "%s/state", n->dir);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    const char *argv[64];
    char rpool[24];
    int na = 0;
    argv[na++] = "tsdbd";
    if (!want_auth) /* --auth (and the users workload): every node requires a login, as production does */
        argv[na++] = "--no-auth";
    argv[na++] = flag;
    argv[na++] = "--dir";
    argv[na++] = n->dir;
    argv[na++] = "--port";
    argv[na++] = "0";
    argv[na++] = "--port-file";
    argv[na++] = pf;
    argv[na++] = "--cluster";
    argv[na++] = "--cluster-secret-file";
    argv[na++] = secret_path;
    argv[na++] = "--state-dir";
    argv[na++] = state;
    argv[na++] = "--peer-advertise";
    argv[na++] = padv;
    if (opt_peer_rdma) {
        argv[na++] = "--peer-rdma";
        argv[na++] = opt_peer_rdma;
    }
    if (opt_peer_rdma_pool_mb > 0) {
        snprintf(rpool, sizeof rpool, "%d", opt_peer_rdma_pool_mb);
        argv[na++] = "--peer-rdma-pool-mb";
        argv[na++] = rpool;
    }
    if (opt_peer_rdma_dev) {
        argv[na++] = "--peer-rdma-device";
        argv[na++] = opt_peer_rdma_dev;
    }
    int rdma_metrics = (opt_peer_rdma && strcmp(opt_peer_rdma, "off") != 0) || (nemeses & N_RDMA_QPERR);
    if (want_rep) { /* large writes from 500 rows (the filler's go to parts: the driver ships them) */
        argv[na++] = "--wal-max-batch-rows";
        argv[na++] = "500";
    }
    if (want_classes) { /* small partitions and granules, quick flushes and idle merges: classes switch (jepsen.c) */
        argv[na++] = "--partition-ms";
        argv[na++] = "2000";
        argv[na++] = "--granule-rows";
        argv[na++] = "512";
        argv[na++] = "--flush-interval-ms";
        argv[na++] = "500";
        argv[na++] = "--merge-idle-s";
        argv[na++] = "1";
    }
    if (want_ec_k || rdma_metrics) {
        argv[na++] = "--metrics-port";
        argv[na++] = "auto";
    }
    if (want_ec_k) { /* the stores exist once the cluster has member ids (ec_stores_make); settles are quick */
        argv[na++] = "--ec-settle-grace-s";
        argv[na++] = "3";
        argv[na++] = "--ec-settle-force-s";
        argv[na++] = "5";
        argv[na++] = "--ec-settle-keep-s";
        argv[na++] = "30";
        argv[na++] = "--ec-cache-mb";
        argv[na++] = "16";
        if (!want_classes) {
            argv[na++] = "--flush-interval-ms";
            argv[na++] = "500";
            argv[na++] = "--merge-idle-s";
            argv[na++] = "1";
        }
        for (int k = 0; k < 2 && n->chunk[k][0]; k++) {
            argv[na++] = "--chunk-dir";
            argv[na++] = n->chunk[k];
        }
    }
    if ((want_ec_k || nn > 6) && i >= 2) { /* D15: io_uring on at most two nodes (8 MB memlock per user), epoll on the rest */
        argv[na++] = "--io";
        argv[na++] = "epoll";
    } else if (getenv("TSDBD_IO") && *getenv("TSDBD_IO")) {
        snprintf(io, sizeof io, "%s", getenv("TSDBD_IO"));
        argv[na++] = "--io";
        argv[na++] = io;
    }
    argv[na] = NULL;
    /* the environment is built before the fork: a child of a threaded process may only call async-signal-safe functions */
    char *envp[512];
    int ne = 0;
    const char *orig_plrdma_test = getenv("TSDB_PLRDMA_TEST");
    for (char **e = environ; *e && ne < 500; e++)
        if (strncmp(*e, "TSDB_CLUSTER_TEST=", 18) && strncmp(*e, "TSDB_CRASH_AT=", 14) && strncmp(*e, "LD_PRELOAD=", 11) &&
            strncmp(*e, "CRASHFS_", 8) && strncmp(*e, "OUTAGEFS_", 9) && strncmp(*e, "TSDB_CLUSTER_WRITES=", 20) &&
            strncmp(*e, "TSDBD_TEST_FWD_STALL=", 21) &&
            (!(nemeses & N_RDMA_QPERR) || strncmp(*e, "TSDB_PLRDMA_TEST=", 17)))
            envp[ne++] = *e;
    char e1[256], e2[2 * PATH_MAX + 64], e3[PATH_MAX + 64], e4[64], e5[PATH_MAX + 64], e6[PATH_MAX + 64], e7[PATH_MAX + 64],
         e8[PATH_MAX + 256];
    if (nemeses & N_RDMA_QPERR) {
        if (orig_plrdma_test && *orig_plrdma_test)
            snprintf(e8, sizeof e8, "TSDB_PLRDMA_TEST=%s,qperr_file=%s/rdma_qperr", orig_plrdma_test, n->dir);
        else
            snprintf(e8, sizeof e8, "TSDB_PLRDMA_TEST=qperr_file=%s/rdma_qperr", n->dir);
        envp[ne++] = e8;
    }
    if (want_via) { /* kv-via: the entry node stalls its forwards while this file exists (nem_fwdstall) */
        snprintf(e5, sizeof e5, "TSDBD_TEST_FWD_STALL=%s/fwd-stall", n->dir);
        envp[ne++] = e5;
    }
    if (!want_rep) /* the leader of a 1-replica cluster volume takes writes without the driver (kv-local) */
        envp[ne++] = "TSDB_CLUSTER_WRITES=1";
    if (nemeses & N_CHURN) /* every applied record of kvr's set and ISR in <state>/voltrace (r4_move_trace_check) */
        envp[ne++] = "TSDB_CLUSTER_TEST=vol_trace=1";
    if (crash_at) {
        snprintf(e1, sizeof e1, "TSDB_CRASH_AT=%s", crash_at);
        envp[ne++] = e1;
    }
    if (use_outagefs) { /* the slow nemesis: the node's whole directory is a domain of the shim (stacked before crashfs) */
        snprintf(e6, sizeof e6, "OUTAGEFS_ROOTS=%s", n->dir);
        snprintf(e7, sizeof e7, "OUTAGEFS_CTL=%s", n->octl);
        envp[ne++] = e6;
        envp[ne++] = e7;
    }
    if (use_outagefs || use_crashfs) {
        snprintf(e2, sizeof e2, "LD_PRELOAD=%s%s%s", use_outagefs ? outagefs_so : "", use_outagefs && use_crashfs ? ":" : "",
                 use_crashfs ? crashfs_so : "");
        envp[ne++] = e2;
    }
    if (use_crashfs) {
        snprintf(e3, sizeof e3, "CRASHFS_ROOT=%s", n->dir);
        envp[ne++] = e3;
        /* a rewrite in place that never synced comes back old (KEEP_OLD), or old or new per sector and unsynced appends
         * torn (TEAR), as on a disk; crashfs's default zeroes it, which the Raft hard state's reader rightly takes for
         * rot (test_cluster hardslot; a kv power run, seed 1, 2026-11-17) */
        uint64_t k = atomic_fetch_add(&crashfs_starts, 1);
        if (mix(seed * 1009 + k) & 1) {
            envp[ne++] = "CRASHFS_TEAR=1";
            snprintf(e4, sizeof e4, "CRASHFS_SEED=%" PRIu64, mix(seed * 7919 + k) & 0x7fffffff);
            envp[ne++] = e4;
            atomic_fetch_add(&crashfs_tears, 1);
        } else
            envp[ne++] = "CRASHFS_REWRITE=old";
    }
    envp[ne] = NULL;
    int64_t t_fork = now_ns();
    pid_t pid = fork();
    if (pid < 0)
        die(2, "fork: %s", strerror(errno));
    if (pid == 0) {
        int fd = open(n->log, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0)
            dup2(fd, 2);
        /* lets dump_threads' gdb (not an ancestor of the node) attach under yama ptrace_scope 1; kept across exec */
        if (prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0) != 0) {
        }
        execve(bin, (char *const *)argv, envp);
        _exit(127);
    }
    if (!strcmp(mutant, "foreign-pid") && i == 1 && n->starts == 0) {
        struct pfcopy *c = malloc(sizeof *c);
        snprintf(c->from, sizeof c->from, "%s", pf);
        snprintf(seen, seen_cap, "%s.seen", pf);
        snprintf(c->to, sizeof c->to, "%s", seen);
        pthread_t t;
        pthread_create(&t, NULL, pf_rewriter, c);
        pthread_detach(t);
    }
    n->starts++;
    *t_fork_out = t_fork;
    return pid;
}

static void record_node_up(int i, pid_t pid, int port, int mport, int pport, int64_t t_fork)
{
    struct node *n = &nodes[i];
    if (!pport)
        die(1, "node %d: the port file names no peer port", i);
    pthread_mutex_lock(&tbl_mu);
    n->pid = pid;
    n->cport = port;
    n->mport = mport;
    n->pport = pport;
    n->proc_ns = t_fork;
    n->paused = 0;
    pthread_mutex_unlock(&tbl_mu);
    ev("node %d up, pid %d", i, (int)pid);
}

/* Starts node i. 0: it is up (the table has its ports, read from its port file and held by its pid). 1: it exited
 * before it listened, with *status as waitpid gave it (a crash point's 99). Anything else is a failed run. */
static int start_node(int i, const char *crash_at, int *status)
{
    char seen[PATH_MAX + 136];
    int64_t t_fork = 0;
    pid_t pid = spawn_node(i, crash_at, seen, sizeof seen, &t_fork);
    int port = th_await(seen, pid, 30000);
    if (port == TH_EXITED) {
        if (status)
            *status = th_status;
        return 1;
    }
    if (port <= 0) {
        signal_child(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        die(1, "node %d: %s", i, th_why);
    }
    record_node_up(i, pid, port, th_mport, th_pport, t_fork);
    return 0;
}

/* One non-blocking poll of `portfile` for `child` (parallel start_nodes_par). Returns >0 port (with *mport, *pport set),
 * 0 still waiting, TH_EXITED (reaped, *st set), or TH_BAD (foreign pid; why set). */
static int poll_portfile(const char *portfile, pid_t child, int *mport_out, int *pport_out, int *st_out, char *why,
                         size_t wn)
{
    int st = 0;
    if (waitpid(child, &st, WNOHANG) == child) {
        if (st_out)
            *st_out = st;
        return TH_EXITED;
    }
    FILE *f = fopen(portfile, "r");
    if (!f)
        return 0;
    int port = 0, pid = 0, mport = 0, pport = 0;
    int n = fscanf(f, "%d %d %d %d", &port, &pid, &mport, &pport);
    fclose(f);
    if (n < 2 || port <= 0)
        return 0;
    if (pid != (int)child) {
        snprintf(why, wn, "harness: %.240s names pid %d, but the server started here is pid %d", portfile, pid, (int)child);
        return TH_BAD;
    }
    if (!th_owns_port(child, port) || (n >= 3 && mport && !th_owns_port(child, mport)) ||
        (n == 4 && !th_owns_port(child, pport))) {
        if (waitpid(child, &st, WNOHANG) == child) {
            if (st_out)
                *st_out = st;
            return TH_EXITED;
        }
        snprintf(why, wn, "harness: port %d is not held by the server started here (pid %d)", port, (int)child);
        return 0;
    }
    *mport_out = n >= 3 ? mport : 0;
    *pport_out = n == 4 ? pport : 0;
    return port;
}

/* Starts all nn nodes in parallel: fork all first, then poll every port file together (up to 45 s). */
static void start_nodes_par(const char *crash_at)
{
    static char seen[MAXN][PATH_MAX + 136];
    pid_t pids[MAXN];
    int64_t tfork[MAXN];
    int ready[MAXN] = {0}, left = nn;
    char why[MAXN][512];
    memset(why, 0, sizeof why);
    for (int i = 0; i < nn; i++)
        pids[i] = spawn_node(i, crash_at, seen[i], sizeof seen[i], &tfork[i]);
    int64_t t0 = now_ms();
    while (left > 0 && now_ms() - t0 < 45000) {
        for (int i = 0; i < nn; i++) {
            if (ready[i])
                continue;
            int mp = 0, pp = 0, st = 0;
            int port = poll_portfile(seen[i], pids[i], &mp, &pp, &st, why[i], sizeof why[i]);
            if (port == TH_EXITED) {
                for (int j = 0; j < nn; j++)
                    if (!ready[j] && j != i) {
                        signal_child(pids[j], SIGKILL);
                        waitpid(pids[j], NULL, 0);
                    }
                die(1, "node %d exited with status 0x%x before it listened (log %s)", i, st, nodes[i].log);
            }
            if (port == TH_BAD) {
                for (int j = 0; j < nn; j++)
                    if (!ready[j]) {
                        signal_child(pids[j], SIGKILL);
                        waitpid(pids[j], NULL, 0);
                    }
                die(1, "node %d: %s", i, why[i]);
            }
            if (port > 0) {
                record_node_up(i, pids[i], port, mp, pp, tfork[i]);
                ready[i] = 1;
                left--;
            }
        }
        if (left > 0)
            usleep(10000);
    }
    if (left > 0) {
        int bad = -1;
        for (int i = 0; i < nn; i++)
            if (!ready[i]) {
                if (bad < 0)
                    bad = i;
                signal_child(pids[i], SIGKILL);
                waitpid(pids[i], NULL, 0);
            }
        die(1, "node %d: %s", bad, why[bad][0] ? why[bad] : "wrote no port file within 45000 ms");
    }
}

/* Stops all nn nodes in parallel with SIGTERM and waits up to 20 s; returns the count of unclean stops. */
static int stop_nodes_par(void)
{
    pid_t pids[MAXN] = {0};
    int left = 0;
    for (int i = 0; i < nn; i++) {
        pthread_mutex_lock(&tbl_mu);
        pids[i] = nodes[i].pid;
        nodes[i].pid = 0;
        nodes[i].cport = nodes[i].pport = 0;
        int was_paused = nodes[i].paused;
        nodes[i].paused = 0;
        pthread_mutex_unlock(&tbl_mu);
        if (pids[i] <= 0)
            continue;
        if (was_paused)
            signal_child(pids[i], SIGCONT);
        signal_child(pids[i], SIGTERM);
        left++;
    }
    int unclean = 0;
    int64_t t0 = now_ms();
    while (left > 0 && now_ms() - t0 < 20000) {
        for (int i = 0; i < nn; i++) {
            if (pids[i] <= 0)
                continue;
            int st = 0;
            pid_t r = waitpid(pids[i], &st, WNOHANG);
            if (r == pids[i]) {
                if (!(WIFEXITED(st) && WEXITSTATUS(st) == 0)) {
                    fprintf(stderr, "jepsen-cluster: node %d did not stop cleanly on SIGTERM (status 0x%x)\n", i, st);
                    unclean++;
                }
                pids[i] = 0;
                left--;
            } else if (r < 0)
                die(2, "waitpid %d: %s", (int)pids[i], strerror(errno));
        }
        if (left > 0)
            usleep(10000);
    }
    for (int i = 0; i < nn; i++)
        if (pids[i] > 0) {
            fprintf(stderr, "jepsen-cluster: node %d did not stop within 20 s of SIGTERM: killed (log %s)\n", i, nodes[i].log);
            signal_child(pids[i], SIGKILL);
            waitpid(pids[i], NULL, 0);
            unclean++;
        }
    return unclean;
}

/* Stops node i with `sig` (SIGKILL, SIGTERM) and reaps it; the table forgets it first, so that nobody connects to a
 * port a dead node may have given up. Returns the wait status. */
static int stop_node(int i, int sig)
{
    pid_t pid;
    pthread_mutex_lock(&tbl_mu);
    pid = nodes[i].pid;
    nodes[i].pid = 0;
    nodes[i].cport = nodes[i].pport = 0;
    int was_paused = nodes[i].paused;
    nodes[i].paused = 0;
    pthread_mutex_unlock(&tbl_mu);
    if (pid <= 0)
        return 0;
    ev("node %d: signal %d to pid %d", i, sig, (int)pid);
    if (was_paused)
        signal_child(pid, SIGCONT);
    signal_child(pid, sig);
    /* a deadline: a node that hangs on SIGTERM (a thread that does not join) is a failed run with its log kept, not a
     * harness that never returns; SIGKILL comes after 20 s and the status then says so */
    int status = 0;
    int64_t t0 = now_ms();
    for (;;) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid)
            return status;
        if (r < 0)
            die(2, "waitpid %d: %s", (int)pid, strerror(errno));
        if (now_ms() - t0 > 20000) {
            if (sig == SIGKILL)
                die(1, "node %d did not die from SIGKILL within 20 s (log %s)", i, nodes[i].log);
            fprintf(stderr, "jepsen-cluster: node %d did not stop within 20 s of signal %d: killed (log %s)\n", i, sig,
                    nodes[i].log);
            signal_child(pid, SIGKILL);
            if (waitpid(pid, &status, 0) != pid)
                die(2, "waitpid %d: %s", (int)pid, strerror(errno));
            return status;
        }
        usleep(10000);
    }
}

/* waits up to ms for node i to exit by itself (a power loss, a crash point): its wait status, or -1 when it did not */
static int await_exit(int i, int ms)
{
    pid_t pid;
    node_snap(i, &pid, NULL, NULL);
    int64_t t0 = now_ms();
    for (;;) {
        int st;
        pid_t r = waitpid(pid, &st, WNOHANG);
        if (r == pid) {
            pthread_mutex_lock(&tbl_mu);
            nodes[i].pid = 0;
            nodes[i].cport = nodes[i].pport = 0;
            nodes[i].paused = 0;
            pthread_mutex_unlock(&tbl_mu);
            ev("node %d (pid %d) exited by itself, status 0x%x", i, (int)pid, st);
            return st;
        }
        if (r < 0)
            die(2, "waitpid %d: %s", (int)pid, strerror(errno));
        if (now_ms() - t0 > ms)
            return -1;
        usleep(20000);
    }
}

static void pause_node(int i, int on)
{
    pid_t pid;
    pthread_mutex_lock(&tbl_mu);
    pid = nodes[i].pid;
    nodes[i].paused = on;
    pthread_mutex_unlock(&tbl_mu);
    ev("node %d: %s", i, on ? "paused" : "resumed");
    if (strcmp(mutant, "nopause")) /* nopause: planted, nothing is stopped */
        signal_child(pid, on ? SIGSTOP : SIGCONT);
}

/* the kernel's state letter of a process ('T': stopped), 0 when it cannot be read */
static char proc_state(pid_t pid)
{
    char path[64], buf[512];
    snprintf(path, sizeof path, "/proc/%d/stat", (int)pid);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return 0;
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0)
        return 0;
    buf[n] = 0;
    char *p = strrchr(buf, ')');
    return p && p[1] == ' ' ? p[2] : 0;
}

/* waits up to ms for `pid` to be stopped (want = 1) or not (want = 0); 1 when it is so */
static int await_stopped(pid_t pid, int want, int ms)
{
    for (int t = 0; t <= ms; t += 20) {
        int stopped = proc_state(pid) == 'T' || proc_state(pid) == 't';
        if (stopped == want)
            return 1;
        usleep(20000);
    }
    return 0;
}

/* a node that is gone without the nemesis having asked for it is a failed run (SIGSEGV, fail-stop exit 1, an unplanned 99) */
static void check_unexpected_exits(void)
{
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid <= 0)
            continue;
        int st;
        if (waitpid(pid, &st, WNOHANG) == pid) {
            if (WIFEXITED(st))
                die(1, "node %d ended on its own with status %d, nobody asked it to (log %s)", i, WEXITSTATUS(st),
                    nodes[i].log);
            die(1, "node %d ended with signal %d, not a power loss (log %s)", i, WTERMSIG(st), nodes[i].log);
        }
    }
}

struct view {
    struct cluster_status st;
    struct meta_state m;
    uint8_t *raw;
};
static void view_free(struct view *v)
{
    cluster_status_free(&v->st);
    meta_free(&v->m);
    free(v->raw);
    memset(v, 0, sizeof *v);
}

/* why the last view_get of this thread failed: a settle that times out names it */
static _Thread_local char view_err[300];

/* STATUS of node i through its real peer port, as `tsdb cluster status` asks. 0, or -1 when it did not answer. */
static int view_get(int i, struct view *v, int timeout_ms)
{
    int pp;
    node_snap(i, NULL, NULL, &pp);
    memset(v, 0, sizeof *v);
    meta_init(&v->m);
    if (pp <= 0) {
        snprintf(view_err, sizeof view_err, "no peer port");
        return -1;
    }
    char addr[48], err[256] = "";
    snprintf(addr, sizeof addr, "127.0.0.1:%d", pp);
    int rc = 0, ar;
    size_t len = 0;
    int64_t t0 = now_ms();
    if ((ar = cluster_admin(addr, secret, secret_len, CLUSTER_OP_STATUS, NULL, 0, timeout_ms, &rc, &v->raw, &len, err,
                            sizeof err)) != 0 ||
        rc != 0 || cluster_status_parse(v->raw, len, &v->st) != 0) {
        snprintf(view_err, sizeof view_err, "%s after %" PRId64 " ms (%s)",
                 ar ? "no reply" : rc ? "rc != 0" : "a reply that does not parse", now_ms() - t0,
                 ar ? err : rc ? "admin error" : "status");
        free(v->raw);
        meta_free(&v->m);
        memset(v, 0, sizeof *v);
        return -1;
    }
    if (v->st.image_len && meta_snapshot_decode(&v->m, v->st.image, v->st.image_len) != META_OK) {
        snprintf(view_err, sizeof view_err, "an image of %zu bytes that does not decode", (size_t)v->st.image_len);
        view_free(v);
        return -1;
    }
    return 0;
}

static int admin_real(int i, int op, const void *req, size_t rl, int follow, int timeout_ms)
{
    int pp;
    node_snap(i, NULL, NULL, &pp);
    char target[META_ADDR_MAX + 1], err[512];
    snprintf(target, sizeof target, "127.0.0.1:%d", pp);
    for (int hop = 0;; hop++) {
        int rc;
        uint8_t *reply = NULL;
        size_t len = 0;
        if (cluster_admin(target, secret, secret_len, op, req, rl, timeout_ms, &rc, &reply, &len, err, sizeof err) != 0)
            die(1, "admin op %d to %s: %s", op, target, err);
        if (rc == CLUSTER_ERR_NOTLEADER && follow && hop < 3 && len > 0 && len < sizeof target) {
            memcpy(target, reply, len);
            target[len] = 0;
            free(reply);
            usleep(300000);
            continue;
        }
        free(reply);
        return rc;
    }
}

/* ------------------------------------------------------------------ the cluster and what must hold after a heal */

#define SETTLE_MS 60000
/* classes runs: the keep caps' age (repl_retain_s) and the longest quiet tail that waits for a first switch */
#define CLASSES_RETAIN_S 30
#define CLASSES_TAIL_MS (cls_defer_s * 1000 + 60000)

/* Every node up and answering, one leader that every core node names, every member UP, the same metadata. A node
 * stuck in a state nobody leaves is what this is for. Returns 0, or -1 with `why` naming what is not yet so. */
static int settle_bad = -1; /* the node a failing settled_once named last (STATUS), for the dump at a timeout */

/* The threads of node i: name, state and kernel wait channel (what a cluster thread that answers no STATUS waits on) */
static void dump_threads(int i)
{
    pid_t pid;
    node_snap(i, &pid, NULL, NULL);
    if (pid <= 0)
        return;
    char d[64];
    snprintf(d, sizeof d, "/proc/%d/task", (int)pid);
    DIR *dir = opendir(d);
    if (!dir)
        return;
    fprintf(stderr, "jepsen-cluster: threads of node %d (pid %d): name state wchan\n", i, (int)pid);
    struct dirent *e;
    while ((e = readdir(dir))) {
        if (e->d_name[0] == '.')
            continue;
        char p[384], comm[64] = "?", st[512] = "", wch[128] = "?";
        FILE *f;
        snprintf(p, sizeof p, "%s/%s/comm", d, e->d_name);
        if ((f = fopen(p, "r"))) {
            if (fgets(comm, sizeof comm, f))
                comm[strcspn(comm, "\n")] = 0;
            fclose(f);
        }
        snprintf(p, sizeof p, "%s/%s/stat", d, e->d_name);
        if ((f = fopen(p, "r"))) {
            if (!fgets(st, sizeof st, f))
                st[0] = 0;
            fclose(f);
        }
        const char *rp = strrchr(st, ')');
        snprintf(p, sizeof p, "%s/%s/wchan", d, e->d_name);
        if ((f = fopen(p, "r"))) {
            if (!fgets(wch, sizeof wch, f))
                snprintf(wch, sizeof wch, "?");
            fclose(f);
        }
        fprintf(stderr, "  %s %s %c %s\n", e->d_name, comm, rp && rp[1] && rp[2] ? rp[2] : '?', wch);
    }
    closedir(dir);
    if (access("/usr/bin/gdb", X_OK) == 0) { /* the node is our child: ptrace is allowed under yama scope 1 */
        char cmd[160];
        snprintf(cmd, sizeof cmd, "timeout 60 gdb -p %d -batch -ex 'thread apply all bt' </dev/null >&2 2>&1",
                 (int)pid);
        fflush(stderr);
        if (system(cmd) != 0) {
        }
    }
}

static int settled_once(char *why, size_t wn)
{
    struct view v[MAXN];
    int have[MAXN] = {0}, leaders = 0, leader_i = -1, rc = 0;
    for (int i = 0; i < nn && !rc; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid <= 0 || nodes[i].paused) {
            snprintf(why, wn, "node %d is %s", i, pid <= 0 ? "not running" : "paused");
            rc = -1;
        } else if (view_get(i, &v[i], 2500) != 0) {
            snprintf(why, wn, "node %d does not answer STATUS: %s", i, view_err);
            settle_bad = i;
            rc = -1;
        } else {
            have[i] = 1;
            if (v[i].st.mode == CLUSTER_MODE_NONE) {
                snprintf(why, wn, "node %d is in no cluster", i);
                rc = -1;
            } else if (v[i].st.role == RAFT_LEADER_ROLE && v[i].st.mode == CLUSTER_MODE_CORE) {
                leaders++;
                leader_i = i;
            }
        }
    }
    if (!rc && leaders != 1) {
        snprintf(why, wn, "%d leaders", leaders);
        rc = -1;
    }
    if (!rc) {
        struct view *l = &v[leader_i];
        uint64_t h = meta_hash(&l->m);
        if (l->m.nmembers != (uint32_t)nn) {
            snprintf(why, wn, "the leader's metadata has %u members, not %d", (unsigned)l->m.nmembers, nn);
            rc = -1;
        }
        for (uint32_t k = 0; !rc && k < l->m.nmembers; k++)
            if (l->m.members[k].state != META_UP) {
                snprintf(why, wn, "member %u is %s", (unsigned)l->m.members[k].id,
                         meta_member_state_name(l->m.members[k].state));
                rc = -1;
            }
        for (int i = 0; !rc && i < nn; i++) {
            if (v[i].st.mode == CLUSTER_MODE_CORE && v[i].st.leader != l->st.self) {
                snprintf(why, wn, "node %d names leader %u, not %u", i, (unsigned)v[i].st.leader, (unsigned)l->st.self);
                rc = -1;
            } else if (v[i].m.applied != l->m.applied || meta_hash(&v[i].m) != h) {
                snprintf(why, wn, "node %d has not caught up with the leader's metadata (applied %" PRIu64 ", leader %"
                         PRIu64 ")", i, (uint64_t)v[i].m.applied, (uint64_t)l->m.applied);
                rc = -1;
            }
        }
    }
    for (int i = 0; i < nn; i++)
        if (have[i])
            view_free(&v[i]);
    return rc;
}

static int64_t settle_ms_total, settle_ms_max;
static int settles;

static void settle(const char *after)
{
    char why[512] = "";
    int64_t t0 = now_ms();
    int good = 0;
    while (now_ms() - t0 < SETTLE_MS) {
        check_unexpected_exits();
        if (settled_once(why, sizeof why) == 0) {
            if (++good >= 2)
                break;
        } else
            good = 0;
        usleep(200000);
    }
    int64_t took = now_ms() - t0;
    if (good < 2) {
        if (settle_bad >= 0 && strstr(why, "does not answer STATUS"))
            dump_threads(settle_bad);
        die(1, "no settle within %d ms after %s: %s", SETTLE_MS, after, why);
    }
    ju_settled();
    settles++;
    settle_ms_total += took;
    if (took > settle_ms_max)
        settle_ms_max = took;
}

/* ---- the sampler: election safety, and the leaders and terms the run saw */

struct obs {
    uint64_t term;
    uint32_t self;
};
#define MAXOBS 200000
static struct obs *obs;
static _Atomic uint32_t nobs;
static _Atomic int sampler_run, last_leader = -1;
/* kvr (kv, classes): its replicas (3 for replicas=3, m + 1 for --ec K+M), the leader that rep_setup found, and
 * kv_leader: the node the record with the highest epoch names as its leader, as the sampler last saw it. */
static int rep_n = 3;
static int rep_leader = -1, rep_nodes[META_REPLICAS_MAX];
static uint32_t rep_number;
static _Atomic int kv_leader = -1;
static uint64_t kv_epoch;
static int rep_full_mask(void) { return (1 << rep_n) - 1; }

static void kv_leader_see(const struct view *v)
{
    if (rep_leader < 0)
        return;
    for (uint32_t k = 0; k < v->m.nvolumes; k++) {
        const struct meta_volume *mv = &v->m.volumes[k];
        if (mv->number != rep_number || !mv->leader || mv->epoch < kv_epoch)
            continue;
        for (int i = 0; i < nn; i++)
            if (atomic_load(&nodes[i].id) == mv->leader) {
                if (atomic_load(&kv_leader) != i || kv_epoch != mv->epoch)
                    ev("kvr: leader node %d at epoch %llu (was node %d)", i, (unsigned long long)mv->epoch,
                       atomic_load(&kv_leader));
                kv_epoch = mv->epoch;
                atomic_store(&kv_leader, i);
            }
    }
}
static pthread_t sampler_th;
static uint64_t samples_ok, samples_failed;

/* Sample VmHWM/VmRSS and open FDs for a process (for resource reporting on large runs). */
static unsigned proc_count_fds(pid_t pid)
{
    char p[64];
    snprintf(p, sizeof p, "/proc/%d/fd", (int)pid);
    DIR *d = opendir(p);
    if (!d)
        return 0;
    unsigned n = 0;
    for (struct dirent *e; (e = readdir(d));)
        if (e->d_name[0] != '.')
            n++;
    closedir(d);
    return n;
}

static uint64_t proc_rss_kb(pid_t pid)
{
    char p[64], ln[256];
    snprintf(p, sizeof p, "/proc/%d/status", (int)pid);
    FILE *f = fopen(p, "r");
    if (!f)
        return 0;
    uint64_t hwm = 0, rss = 0;
    while (fgets(ln, sizeof ln, f)) {
        unsigned long long v = 0;
        if (sscanf(ln, "VmHWM: %llu kB", &v) == 1)
            hwm = (uint64_t)v;
        else if (sscanf(ln, "VmRSS: %llu kB", &v) == 1)
            rss = (uint64_t)v;
    }
    fclose(f);
    return hwm > rss ? hwm : rss;
}

static _Atomic unsigned harness_peak_fds;
static void proc_metrics_sample(void)
{
    unsigned hf = proc_count_fds(getpid());
    unsigned prev_hf = atomic_load(&harness_peak_fds);
    if (hf > prev_hf)
        atomic_store(&harness_peak_fds, hf);
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid <= 0)
            continue;
        uint64_t rss = proc_rss_kb(pid);
        if (rss > atomic_load(&nodes[i].peak_rss_kb))
            atomic_store(&nodes[i].peak_rss_kb, rss);
        unsigned nf = proc_count_fds(pid);
        if (nf > atomic_load(&nodes[i].peak_fds))
            atomic_store(&nodes[i].peak_fds, nf);
    }
}

static void *sampler(void *arg)
{
    (void)arg;
    int64_t last_res = 0;
    while (atomic_load(&sampler_run)) {
        if (now_ms() - last_res >= 1000) {
            proc_metrics_sample();
            last_res = now_ms();
        }
        for (int i = 0; i < nn && atomic_load(&sampler_run); i++) {
            pid_t pid;
            node_snap(i, &pid, NULL, NULL);
            if (pid <= 0 || node_paused(i))
                continue;
            struct view v;
            if (view_get(i, &v, 2000) != 0) {
                samples_failed++;
                continue;
            }
            samples_ok++;
            kv_leader_see(&v);
            if (v.st.role == RAFT_LEADER_ROLE && v.st.mode == CLUSTER_MODE_CORE) {
                atomic_store(&last_leader, i);
                uint32_t k = atomic_load(&nobs);
                if (k < MAXOBS) {
                    obs[k] = (struct obs){v.st.term, v.st.self};
                    atomic_store(&nobs, k + 1);
                }
            }
            view_free(&v);
        }
        usleep(150000);
    }
    return NULL;
}

/* a term has at most one leader */
static int check_election_safety(unsigned *terms, unsigned *leader_changes)
{
    int bad = 0;
    uint32_t n = atomic_load(&nobs);
    *terms = *leader_changes = 0;
    for (uint32_t a = 0; a < n; a++) {
        int first = 1;
        for (uint32_t b = 0; b < a; b++)
            if (obs[b].term == obs[a].term) {
                first = 0;
                if (obs[b].self != obs[a].self) {
                    fprintf(stderr, "jepsen-cluster: two leaders in term %" PRIu64 ": members %u and %u\n", obs[a].term,
                            (unsigned)obs[b].self, (unsigned)obs[a].self);
                    bad++;
                }
                break;
            }
        (*terms) += first;
    }
    uint32_t last = 0;
    for (uint32_t a = 0; a < n; a++)
        if (obs[a].self != last) {
            (*leader_changes)++;
            last = obs[a].self;
        }
    return bad;
}

/* ------------------------------------------------------------------ clients */

static _Atomic unsigned refused_foreign, client_connects;

static int connect_node(int i, int true_port, tsdbc **c)
{
    pid_t pid;
    int cport;
    pthread_mutex_lock(&tbl_mu);
    pid = nodes[i].pid;
    cport = !true_port && nodes[i].cport_stale ? nodes[i].cport_stale : nodes[i].cport;
    pthread_mutex_unlock(&tbl_mu);
    if (pid <= 0 || cport <= 0)
        return -1;
    tsdbc_options o = {.connect_ms = 1000};
    if (want_auth) { /* the workloads' clients log in as the superuser of the init (cluster_users.h) */
        o.user = "jroot";
        o.password = "jroot-pw";
        o.require_auth = 1;
    }
    if (tsdbc_connect_opts("127.0.0.1", cport, &o, c) != 0)
        return -1;
    if (strcmp(mutant, "takeover") != 0 && !th_owns_port(pid, cport)) { /* "takeover" is the planted client that skips it */
        tsdbc_close(*c);
        *c = NULL;
        atomic_fetch_add(&refused_foreign, 1);
        return -1;
    }
    tsdbc_set_retry(*c, 0); /* the history records what the node answered */
    tsdbc_set_timeout(*c, 3000);
    atomic_fetch_add(&client_connects, 1);
    return 0;
}

#include "cluster_users.h"

/* ------------------------------------------------------------------ the histories and their checker */

enum { WS_INDET = 0, WS_OK = 1, WS_FAIL = 2 };
struct wr {
    int8_t nk, keys[3];
    uint8_t st;
    int8_t node; /* the node it was sent to */
    int64_t begin, end; /* end: INT64_MAX while indeterminate */
};
struct rd {
    int64_t begin, end;
    int8_t node; /* the node that answered (kv_roam: a stale read names it) */
    uint32_t seen[KEYS]; /* the id of the write whose value a key shows, 0: nothing */
    uint8_t stray;
    uint32_t own; /* kv-via, session 1: the id of the client's own write, acknowledged on this connection just before */
};
#define MAXSESS 4
struct hist {
    char name[48];
    pthread_mutex_t mu;
    struct wr *w;
    uint32_t nw, wcap;
    struct rd *r[MAXSESS];
    uint32_t nr[MAXSESS], rcap[MAXSESS];
    uint32_t final[KEYS];
    int have_final;
    uint64_t reads_failed;
};
static struct hist hists[MAXN];
#define MAX_WRITE_ID (8u << 20) /* a float32 holds integers exactly up to 2^24 */

static uint32_t hist_begin(struct hist *h, const int *keys, int nk, int node)
{
    pthread_mutex_lock(&h->mu);
    if (h->nw == h->wcap) {
        h->wcap = h->wcap ? h->wcap * 2 : 4096;
        h->w = realloc(h->w, h->wcap * sizeof *h->w);
        if (!h->w)
            die(2, "out of memory");
    }
    struct wr *w = &h->w[h->nw++];
    memset(w, 0, sizeof *w);
    w->nk = (int8_t)nk;
    w->node = (int8_t)node;
    for (int k = 0; k < nk; k++)
        w->keys[k] = (int8_t)keys[k];
    w->st = WS_INDET;
    w->begin = now_ns();
    w->end = INT64_MAX;
    uint32_t id = h->nw;
    pthread_mutex_unlock(&h->mu);
    return id;
}
static void hist_end(struct hist *h, uint32_t id, int st)
{
    pthread_mutex_lock(&h->mu);
    h->w[id - 1].st = (uint8_t)st;
    if (st != WS_INDET)
        h->w[id - 1].end = now_ns();
    pthread_mutex_unlock(&h->mu);
}
static struct rd *hist_read_slot(struct hist *h, int sess)
{
    if (h->nr[sess] == h->rcap[sess]) {
        h->rcap[sess] = h->rcap[sess] ? h->rcap[sess] * 2 : 4096;
        h->r[sess] = realloc(h->r[sess], h->rcap[sess] * sizeof *h->r[sess]);
        if (!h->r[sess])
            die(2, "out of memory");
    }
    struct rd *r = &h->r[sess][h->nr[sess]++];
    memset(r, 0, sizeof *r);
    return r;
}

/* The one place that says where kv-local's series live: node i's series is in a cluster volume of one replica that the
 * group placed on node i (kv_vol[i], made by kv_setup, which also creates the series there; a node's root takes no
 * series). The workload looks it up by name: a node that has not opened its volume yet (a restart waits for the group
 * to record the open) says NOTFOUND or UNAVAILABLE, and the client tries again. */
static char kv_vol[MAXN][META_NAME_MAX];
static int nhists;            /* histories in use: kv-local one per node, kv and classes REP_HISTS */
static int hist_node[MAXN];   /* the node a history's clients talk to */
/* kv and classes (kv_roam): a client connects to a random member, not to kvr's leader, and now and then drops its
 * connection to start again elsewhere; a replica or a member without kvr answers MOVED (route_decide in tsdbd, D5:
 * reads too), which the history counts as definitely failed, and the client follows it to the node it names (by
 * node id, through the node table: never to the address in the body). Each history logs its first moves and the node
 * it talks to after each; kv_moved counts them all. The ids were made on the leader (hist_sid): a member without kvr
 * knows no name of it. */
static int kv_roam;
static tsdb_sid hist_sid[MAXN];
static _Atomic uint64_t kv_moved;
static int kv_resolve(tsdbc *c, int hi, tsdb_sid *sid)
{
    if (kv_roam && hist_sid[hi]) {
        *sid = hist_sid[hi];
        return 0;
    }
    return tsdbc_series(c, hists[hi].name, 0, sid);
}

/* the node a history's client connects to without kv_roam: kvr's current leader (kv, classes), kv-local's fixed node */
static int hist_target(int hi)
{
    int l = atomic_load(&kv_leader);
    return want_rep && l >= 0 ? l : hist_node[hi];
}

/* the node a (re)connecting client of history hi takes: hist_target(hi), or under kv_roam the one a MOVED named, else
 * a random member */
static int kv_pick(int hi, int *next, uint64_t *r)
{
    if (!kv_roam)
        return hist_target(hi);
    int n = *next >= 0 ? *next : (int)(mix(*r += 1) % (uint64_t)nn);
    *next = -1;
    return n;
}

/* after a failed request on node `from`: a MOVED sets *next to the node it names; 1 when it was a MOVED */
static int kv_follow(tsdbc *c, int hi, int from, int *next, unsigned *logged, const char *who)
{
    tsdbc_err_info e;
    if (!kv_roam || tsdbc_last_error(c, &e) != TSDB_ERR_MOVED || !e.nmoved)
        return 0;
    int to = idx_of_id(e.moved[0].node_id);
    uint64_t n = atomic_fetch_add(&kv_moved, 1) + 1;
    *next = to;
    if (++*logged <= 5 || n % 1000 == 0)
        printf("jepsen-cluster: %s of %s: MOVED by node %d to node %d (id %u, epoch %u); it talks to node %d now "
               "(%" PRIu64 " moves)\n", who, hists[hi].name, from, to, (unsigned)e.moved[0].node_id,
               (unsigned)e.moved[0].epoch, to, n);
    return 1;
}

static int64_t key_ts(int k) { return KV_BASE_TS + (int64_t)k * 1000; }
static int ts_key(int64_t ts)
{
    int64_t d = ts - KV_BASE_TS;
    return d >= 0 && d % 1000 == 0 && d / 1000 < KEYS ? (int)(d / 1000) : -1;
}

static _Atomic int kv_run;
static _Atomic uint64_t kv_ok, kv_fail, kv_indet;
static pthread_t kv_threads[2 * MAXN + 1];
static int nkv_threads;

/* kv-via: every client sits on a random member, so most operations are forwarded to kvr's leader by the member they
 * reached (the entry node: s2f's FORWARD). via_entry[h][0] is the node the writer of history h is connected to for
 * its next request, [1] the reader's, -1 while it has none: the nemeses aim at those nodes (via_pick, nem_fwdstall).
 * via_fwd counts the operations that were answered through an entry node while kvr's leader did not change under
 * them: a run that forwarded nothing proves nothing about forwarding, so it fails. */
static _Atomic int via_entry[MAXN][2];
static _Atomic uint64_t via_fwd, via_fwd_reads, via_ryw_reads;
static _Atomic uint64_t via_picks, via_aimed; /* pick_node's picks, and those aimed at an entry node (review F8) */
static _Atomic int via_steer = -1; /* a node the writers go to now (nem_fwdstall), -1: any, as kv_roam says */
static void via_set(int hi, int role, int node)
{
    if (want_via)
        atomic_store(&via_entry[hi][role], node);
}
static void via_note(int node, int leader_before, _Atomic uint64_t *ctr)
{
    if (want_via && node != leader_before && leader_before >= 0 && leader_before == atomic_load(&kv_leader))
        atomic_fetch_add(ctr, 1);
}
/* a node some client is connected to (a writer only, with `writers`) that does not lead kvr, -1 when there is none */
static int via_pick(int writers)
{
    int cand[2 * MAXN], n = 0, l = atomic_load(&kv_leader);
    for (int hi = 0; hi < nhists; hi++)
        for (int role = 0; role < (writers ? 1 : 2); role++) {
            int x = atomic_load(&via_entry[hi][role]);
            if (x >= 0 && x != l)
                cand[n++] = x;
        }
    return n ? cand[rnd() % (uint64_t)n] : -1;
}

static int kv_read_all(tsdbc *c, tsdb_sid sid, struct rd *rd);
static void *kv_writer(void *arg)
{
    int hi = (int)(intptr_t)arg, node = hist_node[hi], next = -1;
    unsigned logged = 0;
    struct hist *h = &hists[hi];
    tsdbc *c = NULL;
    tsdb_sid sid = 0;
    uint64_t r = mix(seed * 31 + (uint64_t)hi);
    while (atomic_load(&kv_run)) {
        if (c && want_via && atomic_load(&via_steer) >= 0 && atomic_load(&via_steer) != node) { /* steered elsewhere */
            tsdbc_close(c);
            c = NULL;
        }
        if (!c) {
            via_set(hi, 0, -1);
            node = kv_pick(hi, &next, &r);
            if (want_via && atomic_load(&via_steer) >= 0)
                node = atomic_load(&via_steer);
            if (connect_node(node, 0, &c) != 0 || kv_resolve(c, hi, &sid) != 0) {
                if (c)
                    tsdbc_close(c);
                c = NULL;
                usleep(30000);
                continue;
            }
        }
        via_set(hi, 0, node);
        if (h->nw >= MAX_WRITE_ID)
            break;
        int nk = 1 + (int)(mix(r += 1) % 3), keys[3];
        for (int k = 0; k < nk;) {
            int cand = (int)(mix(r += 1) % KEYS), dup = 0;
            for (int j = 0; j < k; j++)
                dup |= keys[j] == cand;
            if (!dup)
                keys[k++] = cand;
        }
        for (int a = 0; a < nk; a++) /* ascending timestamps */
            for (int b = a + 1; b < nk; b++)
                if (keys[b] < keys[a]) {
                    int t = keys[a];
                    keys[a] = keys[b];
                    keys[b] = t;
                }
        uint32_t id = hist_begin(h, keys, nk, node);
        int64_t ts[3];
        float val[3];
        for (int k = 0; k < nk; k++) {
            ts[k] = key_ts(keys[k]);
            val[k] = (float)id;
        }
        int lead0 = atomic_load(&kv_leader);
        int rc;
        if (want_ec_k && nk == 1 && mix(r += 1) % 3 == 0) {
            /* --ec: a replace of the key's one row (a tombstone of [ts, ts + 1) and the row in one commit): into a settled
             * key it is a young part with a tombstone above the object (design-cluster-data.md E6, the read path of R8 that
             * is the easiest to get wrong); as a write of one key it fits the model of an insert */
            tsdb_values tv = {TSDB_FLOAT32, val, NULL};
            rc = tsdbc_replace_range(c, sid, ts[0], ts[0] + 1, ts, &tv, 1);
            atomic_fetch_add(&ec_replaces, 1);
        } else
            rc = tsdbc_insert_series(c, sid, ts, val, (size_t)nk);
        if (rc == 0) {
            hist_end(h, id, WS_OK);
            atomic_fetch_add(&kv_ok, 1);
            via_note(node, lead0, &via_fwd);
            /* kv-via, read-your-writes: now and then the client reads the series back on the connection that was
             * acknowledged, as the one that wrote (session 1: the reader thread owns session 0). Whatever member
             * answers, it must show this write or a newer one at every key the write set. */
            if (want_via && mix(r += 1) % 2 == 0) {
                struct rd tmp;
                memset(&tmp, 0, sizeof tmp);
                tmp.node = (int8_t)node;
                tmp.own = id;
                if (kv_read_all(c, sid, &tmp) == 0) {
                    *hist_read_slot(h, 1) = tmp;
                    atomic_fetch_add(&via_ryw_reads, 1);
                } else {
                    tsdbc_close(c);
                    c = NULL;
                }
            }
        } else if (rc == TSDB_ERR_UNAVAILABLE || rc == TSDB_ERR_MOVED || rc == TSDB_ERR_INVALID ||
                   rc == TSDB_ERR_DROPPED || rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_DENIED) {
            hist_end(h, id, WS_FAIL); /* refused: definitely not applied */
            atomic_fetch_add(&kv_fail, 1);
            if (kv_follow(c, hi, node, &next, &logged, "the writer") || rc == TSDB_ERR_MOVED ||
                rc == TSDB_ERR_UNAVAILABLE) { /* kvr may lead elsewhere now */
                tsdbc_close(c);
                c = NULL;
            }
        } else {
            hist_end(h, id, WS_INDET); /* the node may have applied it */
            atomic_fetch_add(&kv_indet, 1);
            tsdbc_close(c);
            c = NULL;
        }
        if (c && kv_roam && mix(r += 1) % 200 == 0) { /* start again on a random member */
            tsdbc_close(c);
            c = NULL;
        }
        usleep((useconds_t)(1000 + mix(r += 1) % 3000));
    }
    if (c)
        tsdbc_close(c);
    return NULL;
}

/* One read of the whole series into `rd`. 0, or the client's error. */
static int kv_read_all(tsdbc *c, tsdb_sid sid, struct rd *rd)
{
    tsdb_points p;
    rd->begin = now_ns();
    int rc = tsdbc_query_points(c, sid, INT64_MIN, INT64_MAX, &p);
    rd->end = now_ns();
    if (rc != 0)
        return rc;
    for (size_t i = 0; i < p.n; i++) {
        int k = ts_key(p.ts[i]);
        if (k < 0 || isnan(p.values[i]))
            rd->stray |= k < 0;
        else
            rd->seen[k] = (uint32_t)p.values[i];
    }
    free(p.ts);
    free(p.values);
    return 0;
}

static void *kv_reader(void *arg)
{
    int hi = (int)(intptr_t)arg, node = hist_node[hi], next = -1;
    unsigned logged = 0;
    uint64_t r = mix(seed * 37 + (uint64_t)hi);
    struct hist *h = &hists[hi];
    tsdbc *c = NULL;
    tsdb_sid sid = 0;
    while (atomic_load(&kv_run)) {
        if (!c) {
            via_set(hi, 1, -1);
            node = kv_pick(hi, &next, &r);
            if (connect_node(node, 0, &c) != 0 || kv_resolve(c, hi, &sid) != 0) {
                if (c)
                    tsdbc_close(c);
                c = NULL;
                usleep(30000);
                continue;
            }
        }
        struct rd tmp;
        memset(&tmp, 0, sizeof tmp);
        tmp.node = (int8_t)node;
        via_set(hi, 1, node);
        int lead0 = atomic_load(&kv_leader);
        if (kv_read_all(c, sid, &tmp) == 0) {
            *hist_read_slot(h, 0) = tmp;
            via_note(node, lead0, &via_fwd_reads);
        } else {
            if (!kv_follow(c, hi, node, &next, &logged, "the reader")) /* MOVED: not applied, not a failure */
                h->reads_failed++;
            tsdbc_close(c);
            c = NULL;
        }
        if (c && kv_roam && mix(r += 1) % 200 == 0) {
            tsdbc_close(c);
            c = NULL;
        }
        usleep(8000);
    }
    if (c)
        tsdbc_close(c);
    return NULL;
}

/* Before the first fault: one cluster volume of one replica per node (the group places a new volume on the node that
 * leads the fewest, so nn volumes land one on each node), then every node's series in the volume it holds, created
 * through that node (TSDB_CLUSTER_WRITES=1 lets the leader of a volume take writes). */
static void kv_setup(void)
{
    for (int i = 0; i < nn; i++) {
        char vn[16];
        uint8_t req[META_NAME_MAX + 8];
        snprintf(vn, sizeof vn, "kvl%d", i);
        size_t rl = cluster_volume_add_encode(vn, 1, req);
        int rc = admin_real(0, CLUSTER_OP_VOLUME_ADD, req, rl, 1, 15000);
        if (rc != 0)
            die(1, "volume add %s: %s", vn, cluster_strerror(rc));
    }
    int64_t t0 = now_ms();
    for (int placed = 0; !placed;) {
        struct view v;
        if (view_get(0, &v, 2000) == 0) {
            placed = 1;
            for (int i = 0; i < nn && placed; i++) {
                uint32_t id = atomic_load(&nodes[i].id);
                kv_vol[i][0] = 0;
                for (uint32_t k = 0; k < v.m.nvolumes; k++) {
                    const struct meta_volume *mv = &v.m.volumes[k];
                    if (!strncmp(mv->name, "kvl", 3) && mv->nrep == 1 && mv->rep[0].node == id && mv->rep[0].open_gen >= 1)
                        snprintf(kv_vol[i], sizeof kv_vol[i], "%s", mv->name);
                }
                placed = kv_vol[i][0] != 0;
            }
            view_free(&v);
        }
        if (!placed && now_ms() - t0 > 60000)
            die(1, "kv-local: the volumes kvl0..kvl%d were not placed one on each node and opened", nn - 1);
        if (!placed)
            usleep(200000);
    }
    nhists = nn;
    for (int i = 0; i < nn; i++) {
        snprintf(hists[i].name, sizeof hists[i].name, "kvl.n%d", i);
        hist_node[i] = i;
        pthread_mutex_init(&hists[i].mu, NULL);
        /* the group has recorded the open; the node opens its volume once it applied that too */
        for (int64_t t1 = now_ms();; usleep(100000)) {
            tsdbc *c = NULL;
            tsdb_sid sid = 0;
            int rc = connect_node(i, 1, &c);
            if (rc == 0)
                rc = tsdbc_series_at(c, hists[i].name, kv_vol[i], &sid);
            if (c)
                tsdbc_close(c);
            if (rc == 0 && TSDB_SID_VOLUME(sid) == 0)
                die(1, "node %d: series %s went to the root, not to volume %s", i, hists[i].name, kv_vol[i]);
            if (rc == 0)
                break;
            if ((rc != TSDB_ERR_UNAVAILABLE && rc != TSDB_ERR_NOTFOUND) || now_ms() - t1 > 30000)
                die(1, "node %d: cannot create the series of kv-local in volume %s: %s", i, kv_vol[i],
                    tsdbc_strerror(rc));
        }
    }
}

/* kv and classes: one cluster volume "kvr" of 3 replicas with the replication driver (no TSDB_CLUSTER_WRITES), REP_HISTS
 * series with a writer and a reader each and the filler's series, all created and written through the volume's leader
 * (the replicas answer UNAVAILABLE). The leader is never killed, powered off, paused or armed at a restart (pick_node):
 * a restarted leader never leads its epoch again and nothing fails the volume over before r3e-failover; the one fault
 * it takes is the terminal crash point (nem_terminal). */
#define REP_HISTS 2
#define FILL_BASE (KV_BASE_TS + 86400000LL) /* the filler's rows: a day after the keys, never a key */
static char rep_dir[META_REPLICAS_MAX][PATH_MAX + 32];
static _Atomic uint64_t fill_ok, fill_err;
/* The filler's batches in the order sent (one thread; read after kv_stop joined it): the timestamps [ts0, ts1] of each,
 * its row count and its outcome (WS_*). Batches never overlap (the frontier only moves ahead, also past a failed one),
 * so every row of kvr.fill belongs to exactly one. */
struct fbatch {
    int64_t ts0, ts1;
    int n, st;
};
static struct fbatch *fb;
static size_t nfb, cfb;

static int rep_leader_refresh(void);
static void rep_setup_move(void);
static int admin_any(int op, const void *req, size_t rl, int timeout_ms);

static void rep_setup(void)
{
    uint8_t req[META_NAME_MAX + 8];
    size_t rl = cluster_volume_add_encode("kvr", 3, req);
    int rc = want_ec_k ? 0 : admin_real(0, CLUSTER_OP_VOLUME_ADD, req, rl, 1, 15000);
    if (rc != 0)
        die(1, "volume add kvr: %s", cluster_strerror(rc));
    if (want_ec_k)
        ec_add_kvr();
    for (int64_t t0 = now_ms(); rep_leader < 0; usleep(200000)) {
        struct view v;
        if (now_ms() - t0 > 60000)
            die(1, "kv: the volume kvr was not opened on %d replicas with a leader within 60 s", rep_n);
        if (view_get(0, &v, 2000) != 0)
            continue;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (strcmp(mv->name, "kvr") || mv->nrep != (uint32_t)rep_n || !mv->leader)
                continue;
            int ok = 1, lead = -1;
            for (int r = 0; r < rep_n; r++) {
                rep_nodes[r] = -1;
                for (int i = 0; i < nn; i++)
                    if (atomic_load(&nodes[i].id) == mv->rep[r].node)
                        rep_nodes[r] = i;
                ok &= rep_nodes[r] >= 0 && mv->rep[r].open_gen >= 1;
                if (rep_nodes[r] >= 0 && mv->rep[r].node == mv->leader)
                    lead = rep_nodes[r];
            }
            if (ok && lead >= 0) {
                rep_number = mv->number;
                rep_leader = lead;
                kv_epoch = mv->epoch;
                atomic_store(&kv_leader, lead);
            }
        }
        view_free(&v);
    }
    for (int r = 0; r < rep_n; r++)
        snprintf(rep_dir[r], sizeof rep_dir[r], "%s/v%" PRIu32, nodes[rep_nodes[r]].dir, rep_number);
    nhists = REP_HISTS;
    static const char *const names[REP_HISTS + 1] = {"kvr.a", "kvr.b", "kvr.fill"};
    for (int i = 0; i <= REP_HISTS; i++) {
        if (i < REP_HISTS) {
            snprintf(hists[i].name, sizeof hists[i].name, "%s", names[i]);
            hist_node[i] = rep_leader;
            pthread_mutex_init(&hists[i].mu, NULL);
        }
        /* The leader promotes itself once its EPOCH marker commits. Before the workload starts a member that has not yet
         * heard the leader (a slow start of many nodes) may report it unreachable and the group moves the leadership
         * (PLANNED): MOVED, or no answer from the old leader, then re-reads the record and goes to the new leader, within
         * the same bound. A setup that cannot create its series in 30 s is a harness failure (2), not a wrong answer. */
        for (int64_t t1 = now_ms();; usleep(100000)) {
            tsdbc *c = NULL;
            tsdb_sid sid = 0;
            rc = connect_node(rep_leader, 1, &c);
            if (rc == 0)
                rc = tsdbc_series_at(c, names[i], "kvr", &sid);
            if (c)
                tsdbc_close(c);
            if (rc == 0) {
                if (i < REP_HISTS)
                    hist_sid[i] = sid;
                break;
            }
            if ((rc != TSDB_ERR_UNAVAILABLE && rc != TSDB_ERR_NOTFOUND && rc != TSDB_ERR_MOVED) || now_ms() - t1 > 30000)
                die(2, "kv setup: cannot create %s in kvr on its leader (node %d): %s", names[i], rep_leader,
                    tsdbc_strerror(rc));
            if (rep_leader_refresh())
                t1 = now_ms(); /* a new leader: its own 30 s for its promotion */
        }
        if (i == 0)
            rep_setup_move();
    }
    for (int i = 0; i < REP_HISTS; i++)
        hist_node[i] = rep_leader; /* the leader may have moved since the first series */
    (void)rep_leader_refresh();
    kv_roam = 1;
    printf("jepsen-cluster: kvr is volume %" PRIu32 " on nodes", rep_number);
    for (int r = 0; r < rep_n; r++)
        printf("%s %d", r ? "," : "", rep_nodes[r]);
    printf("; its leader is node %d; clients on random members, moved to it\n", rep_leader);
}

/* kvr's ISR as the metadata group holds it: a mask of the replicas (bit r: rep_nodes[r]) in the record with the highest
 * ISR version among the copies of every running node (one node's copy may lag), its version in *ver. -1 when no node
 * answered. The leader commits on the union of the ISR in force and one it asked for (src/repl.c), and a member joins
 * only once its log reaches the commit point, so every member of the last ISR holds every write the leader
 * acknowledged; a replica outside it was not needed for the acknowledgements since it left. */
/* The largest CLASSES on kvr's replicas, -1 when none has one: 24-byte records, the filler's first class and then one
 * per switch, so 48 or more is a switch. */
static long long classes_most(void)
{
    long long most = -1;
    for (int r = 0; r < rep_n; r++) {
        struct stat cs;
        char cl[PATH_MAX + 64];
        snprintf(cl, sizeof cl, "%.4096s/CLASSES", rep_dir[r]);
        if (stat(cl, &cs) == 0 && (long long)cs.st_size > most)
            most = (long long)cs.st_size;
    }
    return most;
}

/* kvr's replica set as the metadata group holds it (R4: it changes under churn and a reset): the record of the copy
 * with the highest (set_version, epoch) among the running nodes' copies. node[k] is the index of rep[k]'s node in the
 * table (-1: not in it), learner[k] its learner flag, isr_has[k] whether it is in the ISR. 0, or -1 when no node
 * answered or the volume is not in the copy. */
struct rrec {
    int nrep, node[META_REPLICAS_MAX], learner[META_REPLICAS_MAX], isr_has[META_REPLICAS_MAX], leader;
    int moving; /* a learner with `replaces` is in the set: a move is in flight */
    uint64_t set_version, epoch, rid[META_REPLICAS_MAX];
    uint32_t id[META_REPLICAS_MAX];
};

static int node_by_id(uint32_t id)
{
    for (int i = 0; i < nn; i++)
        if (id && atomic_load(&nodes[i].id) == id)
            return i;
    return -1;
}

static int rep_rec_get(struct rrec *o)
{
    memset(o, 0, sizeof *o);
    o->leader = -1;
    int have = 0;
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (mv->number != rep_number || (have && (mv->set_version < o->set_version ||
                                                      (mv->set_version == o->set_version && mv->epoch <= o->epoch))))
                continue;
            have = 1;
            memset(o, 0, sizeof *o);
            o->set_version = mv->set_version;
            o->epoch = mv->epoch;
            o->nrep = (int)mv->nrep;
            o->leader = node_by_id(mv->leader);
            for (int r = 0; r < o->nrep && r < META_REPLICAS_MAX; r++) {
                o->id[r] = mv->rep[r].node;
                o->node[r] = node_by_id(mv->rep[r].node);
                o->learner[r] = mv->rep[r].learner;
                o->rid[r] = mv->rep[r].rid;
                o->moving |= mv->rep[r].learner && mv->rep[r].replaces;
                for (uint32_t j = 0; j < mv->nisr; j++)
                    o->isr_has[r] |= mv->isr[j] == mv->rep[r].node;
            }
        }
        view_free(&v);
    }
    return have ? 0 : -1;
}

/* rep_setup: kvr's leader as the highest record among the running nodes' copies names it. 1 when it is another node
 * than rep_leader (rep_leader, kv_epoch and kv_leader follow it; epochs only grow). */
static int rep_leader_refresh(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0 || r.epoch < kv_epoch || r.leader == rep_leader)
        return 0;
    ev("kv setup: kvr's leader moved from node %d to node %d (epoch %llu)", rep_leader, r.leader,
       (unsigned long long)r.epoch);
    rep_leader = r.leader;
    kv_epoch = r.epoch;
    atomic_store(&kv_leader, r.leader);
    return 1;
}

/* JEPSEN_SETUP_MOVE=1 (the test of rep_setup's way round a leader change): a planned transfer of kvr to an ISR member
 * right after the first series was created, so that the next one meets MOVED. */
static void rep_setup_move(void)
{
    const char *e = getenv("JEPSEN_SETUP_MOVE");
    if (!e || atoi(e) <= 0)
        return;
    struct rrec r;
    for (int64_t t0 = now_ms(); rep_rec_get(&r) != 0 || r.leader < 0; usleep(200000))
        if (now_ms() - t0 > 30000)
            die(2, "JEPSEN_SETUP_MOVE: no record of kvr");
    for (int k = 0; k < r.nrep; k++)
        if (r.isr_has[k] && !r.learner[k] && r.node[k] >= 0 && r.node[k] != r.leader) {
            uint8_t body[16];
            size_t bl = cluster_volume_force_encode(rep_number, r.id[k], body);
            int rc = admin_any(CLUSTER_OP_VOLUME_TRANSFER, body, bl, 20000);
            ev("setup: transfer of kvr to node %d: %s", r.node[k], cluster_strerror(rc));
            printf("jepsen-cluster: setup: transfer of kvr from node %d to node %d: %s\n", r.leader, r.node[k],
                   cluster_strerror(rc));
            struct rrec r2;
            for (int64_t t0 = now_ms(); rc == 0 && now_ms() - t0 < 30000; usleep(100000))
                if (rep_rec_get(&r2) == 0 && r2.leader == r.node[k]) /* the record names it: the old one answers MOVED */
                    return;
            return;
        }
    die(2, "JEPSEN_SETUP_MOVE: no ISR member to take kvr");
}

/* After churn kvr's replicas are the record's: the end check compares those (control thread only, as every user of
 * rep_nodes and rep_dir). Only a settled record counts (3 replicas, no learner, all of them in the table). */
static int rep_sync_members(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.nrep != rep_n)
        return 0;
    for (int k = 0; k < rep_n; k++)
        if (r.learner[k] || r.node[k] < 0)
            return 0;
    for (int k = 0; k < rep_n; k++) {
        rep_nodes[k] = r.node[k];
        snprintf(rep_dir[k], sizeof rep_dir[k], "%s/v%" PRIu32, nodes[r.node[k]].dir, rep_number);
    }
    return 1;
}
static int r4_end_bad; /* the failures of the end checks of R4 (rep_final_check, r4_leftovers_check) */
static int r4_dynamic; /* churn: the replica set changes, rep_isr_mask follows the record */

static int rep_isr_mask(uint64_t *ver)
{
    int mask = -1;
    *ver = 0;
    if (r4_dynamic)
        rep_sync_members();
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (mv->number != rep_number || (mask >= 0 && mv->isr_version <= *ver))
                continue;
            *ver = mv->isr_version;
            mask = 0;
            for (int r = 0; r < rep_n; r++)
                for (uint32_t j = 0; j < mv->nisr; j++)
                    if (mv->isr[j] == atomic_load(&nodes[rep_nodes[r]].id))
                        mask |= 1 << r;
        }
        view_free(&v);
    }
    return mask;
}

/* The filler: inserts of 50-1550 rows to "kvr.fill" at a frontier that only moves ahead, a row every 1, 3, 8 or 50 ms
 * of timestamps for a minute of them at a time (classes: the series' class follows its rate), most of them large
 * writes that the driver ships as files (prepared-before-commit). Each batch is recorded (fbatch) and checked in every
 * replica directory at the end (fill_check): whole or absent, whole when acknowledged, absent when refused. */
static void *fill_writer(void *arg)
{
    (void)arg;
    static const int spacing[4] = {1, 3, 8, 50};
    int64_t *ts = malloc(1600 * sizeof *ts), front = FILL_BASE;
    float *val = malloc(1600 * sizeof *val);
    if (!ts || !val)
        die(2, "out of memory");
    uint64_t r = mix(seed * 77 + 5);
    tsdbc *c = NULL;
    tsdb_sid sid = 0;
    while (atomic_load(&kv_run)) {
        if (!c) {
            if (connect_node(atomic_load(&kv_leader), 0, &c) != 0 || tsdbc_series(c, "kvr.fill", 0, &sid) != 0) {
                if (c)
                    tsdbc_close(c);
                c = NULL;
                usleep(30000);
                continue;
            }
        }
        int n = 50 + (int)(mix(r += 1) % 1501);
        for (int j = 0; j < n; j++) {
            ts[j] = front;
            val[j] = (float)(j % 997);
            front += spacing[mix(seed + (uint64_t)((front - FILL_BASE) / 60000)) % 4];
        }
        if (nfb == cfb) {
            cfb = cfb ? 2 * cfb : 1024;
            if (!(fb = realloc(fb, cfb * sizeof *fb)))
                die(2, "out of memory");
        }
        struct fbatch *b = &fb[nfb++];
        *b = (struct fbatch){.ts0 = ts[0], .ts1 = ts[n - 1], .n = n, .st = WS_INDET};
        int rc = tsdbc_insert_series(c, sid, ts, val, (size_t)n);
        if (rc == 0) {
            b->st = WS_OK;
            atomic_fetch_add(&fill_ok, 1);
        } else {
            if (rc == TSDB_ERR_UNAVAILABLE || rc == TSDB_ERR_MOVED || rc == TSDB_ERR_INVALID || rc == TSDB_ERR_DROPPED ||
                rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_DENIED)
                b->st = WS_FAIL; /* refused: definitely not applied (as in kv_writer) */
            atomic_fetch_add(&fill_err, 1);
            tsdbc_close(c);
            c = NULL;
        }
        usleep((useconds_t)(40000 + mix(r += 1) % 80000));
    }
    if (c)
        tsdbc_close(c);
    free(ts);
    free(val);
    return NULL;
}

static void *fill_writer(void *arg);
static void kv_start(void)
{
    for (int i = 0; i < MAXN; i++)
        for (int role = 0; role < 2; role++)
            atomic_store(&via_entry[i][role], -1);
    atomic_store(&kv_run, 1);
    for (int i = 0; i < nhists; i++) {
        pthread_create(&kv_threads[nkv_threads++], NULL, kv_writer, (void *)(intptr_t)i);
        pthread_create(&kv_threads[nkv_threads++], NULL, kv_reader, (void *)(intptr_t)i);
    }
    if (want_rep)
        pthread_create(&kv_threads[nkv_threads++], NULL, fill_writer, NULL);
}

static void kv_stop(void)
{
    atomic_store(&kv_run, 0);
    for (int i = 0; i < nkv_threads; i++)
        pthread_join(kv_threads[i], NULL);
    nkv_threads = 0;
}

/* the final state of node i's series, from the node itself (its true port), retried while it settles */
static void kv_final(int i)
{
    struct hist *h = &hists[i];
    int64_t t0 = now_ms();
    while (now_ms() - t0 < 30000) {
        tsdbc *c = NULL;
        tsdb_sid sid;
        if (connect_node(hist_target(i), 1, &c) == 0) {
            struct rd rd;
            memset(&rd, 0, sizeof rd);
            if (kv_resolve(c, i, &sid) == 0 && kv_read_all(c, sid, &rd) == 0) {
                memcpy(h->final, rd.seen, sizeof h->final);
                h->have_final = !rd.stray;
                tsdbc_close(c);
                if (h->have_final)
                    return;
                die(1, "the final read of %s has a timestamp that is no key", h->name);
            }
            tsdbc_close(c);
        }
        usleep(200000);
    }
    die(1, "node %d: no final read of %s within 30 s", hist_target(i), h->name);
}

static int cmp_i64pair(const void *a, const void *b)
{
    int64_t x = ((const int64_t *)a)[0], y = ((const int64_t *)b)[0];
    return x < y ? -1 : x > y;
}

enum { V_DURABLE, V_VALID, V_ATOMIC, V_STALE, V_MONO, V_RYW, V_OLD, V_KINDS };
static const char *const viol_name[V_KINDS] = {"durability", "validity", "atomic visibility", "stale read", "monotonic reads",
                                               "read-your-writes", "old frame"};
static int viol_by[V_KINDS];

#define VIOL(kind, ...)                                                                                                   \
    do {                                                                                                              \
        viol++; /* three examples of every kind, then only the counts (a flood of one kind hides the others) */         \
        if (viol_by[kind]++ < 3) {                                                                                             \
            fprintf(stderr, "jepsen-cluster: %s: ", h->name);                                                         \
            fprintf(stderr, __VA_ARGS__);                                                                             \
            fputc('\n', stderr);                                                                                      \
        }                                                                                                             \
    } while (0)

static const char *const ws_name[] = {"indeterminate", "acknowledged", "refused"};
static void trace_op(const struct hist *h, const char *what, uint32_t id)
{
    if (!trace_on || id == 0 || id > h->nw)
        return;
    const struct wr *x = &h->w[id - 1];
    fprintf(stderr, "jepsen-cluster:   %s: write %u to node %d, [%lld, %lld] ms, %s\n", what, id, x->node,
            (long long)(x->begin / 1000000), x->end == INT64_MAX ? -1LL : (long long)(x->end / 1000000), ws_name[x->st]);
}
/* the acknowledged write of key k that ended before `t` with the latest begin */
static uint32_t trace_acked_before(const struct hist *h, int k, int64_t t)
{
    uint32_t best = 0;
    for (uint32_t id = 1; id <= h->nw; id++) {
        const struct wr *x = &h->w[id - 1];
        int has = 0;
        for (int j = 0; j < x->nk; j++)
            has |= x->keys[j] == k;
        if (has && x->st == WS_OK && x->end < t && (!best || x->begin > h->w[best - 1].begin))
            best = id;
    }
    return best;
}
static void trace_read(const struct hist *h, const char *what, const struct rd *rd)
{
    if (trace_on)
        fprintf(stderr, "jepsen-cluster:   %s: node %d, [%lld, %lld] ms\n", what, rd->node, (long long)(rd->begin / 1000000),
                (long long)(rd->end / 1000000));
    (void)h;
}

/* kv-via, fwdstall (nem_fwdstall): the windows in which an entry node held every frame before its send, so that every
 * write that began in the first part of one was older than fwd_max_age (1 s) when the stall ended. The entry node must
 * then answer it UNAVAILABLE (the frame is too old to send: its client may have resent it), definitely not applied;
 * an acknowledged write there was sent late (the fwdoldframe mutant). */
struct stall_win {
    int node;
    int64_t ts, te; /* ns: the stall file existed from ts to te */
};
#define MAXSTALLS 256
/* A request in flight when the file appeared (its frame not yet at the entry node's stall) began a little before ts and is
 * held all the same, and then the writer begins nothing else until te. Such a request took a second and more, which
 * only a held one does: so the window starts STALL_LEAD_NS early, and a write must have lasted 1.3 s. */
#define STALL_LEAD_NS 100000000LL
#define STALL_OLD_MS 1500 /* a write begun this long before te was held past fwd_max_age (1 s), with 500 ms to spare */
static struct stall_win stalls[MAXSTALLS];
static int nstalls;

/* Checks 1-5 of the header over one history. Returns the number of violations. `r[0]` is the one session of kv-local. */
static int check_hist(struct hist *h, uint64_t *seen_indet_applied)
{
    int viol = 0;
    uint32_t nw = h->nw;
    struct wr *w = h->w;
    uint8_t *seen = calloc(nw + 1, 1);
    if (!seen)
        die(2, "out of memory");
    for (int s = 0; s < MAXSESS; s++)
        for (uint32_t j = 0; j < h->nr[s]; j++)
            for (int k = 0; k < KEYS; k++) {
                uint32_t v = h->r[s][j].seen[k];
                if (v)
                    seen[v <= nw ? v : 0] = 1;
            }
    for (int k = 0; k < KEYS; k++)
        if (h->final[k])
            seen[h->final[k] <= nw ? h->final[k] : 0] = 1;
    /* 2: validity of what was seen */
#define VALID(v, k, rend, what)                                                                                       \
    do {                                                                                                              \
        uint32_t v_ = (v);                                                                                            \
        if (v_ > nw)                                                                                                  \
            VIOL(V_VALID, "%s: key %d shows %u, a write nobody made", what, k, v_);                                            \
        else {                                                                                                        \
            struct wr *w_ = &w[v_ - 1];                                                                               \
            int has_ = 0;                                                                                             \
            for (int j_ = 0; j_ < w_->nk; j_++)                                                                       \
                has_ |= w_->keys[j_] == (k);                                                                          \
            if (!has_)                                                                                                \
                VIOL(V_VALID, "%s: key %d shows write %u, which did not write that key", what, k, v_);                         \
            else if (w_->st == WS_FAIL)                                                                                \
                VIOL(V_VALID, "%s: key %d shows write %u, which was refused (definitely not applied)", what, k, v_);           \
            else if (w_->begin > (rend))                                                                              \
                VIOL(V_VALID, "%s: key %d shows write %u before it began", what, k, v_);                                       \
        }                                                                                                             \
    } while (0)
    for (int s = 0; s < MAXSESS; s++)
        for (uint32_t j = 0; j < h->nr[s]; j++) {
            struct rd *rd = &h->r[s][j];
            if (rd->stray)
                VIOL(V_VALID, "read %u: a timestamp that is no key", j);
            for (int k = 0; k < KEYS; k++)
                if (rd->seen[k])
                    VALID(rd->seen[k], k, rd->end, "validity");
        }
    for (int k = 0; k < KEYS; k++)
        if (h->final[k])
            VALID(h->final[k], k, INT64_MAX, "validity (final)");
    /* 1: durability of the final state against every applied write */
    int64_t mb[KEYS];
    int has[KEYS] = {0};
    for (int k = 0; k < KEYS; k++)
        mb[k] = INT64_MIN;
    for (uint32_t id = 1; id <= nw; id++) {
        struct wr *x = &w[id - 1];
        int applied = x->st == WS_OK || (x->st == WS_INDET && seen[id]);
        if (x->st == WS_INDET && seen[id])
            (*seen_indet_applied)++;
        for (int j = 0; applied && j < x->nk; j++) {
            has[(int)x->keys[j]] = 1;
            if (x->begin > mb[(int)x->keys[j]])
                mb[(int)x->keys[j]] = x->begin;
        }
    }
    if (h->have_final)
        for (int k = 0; k < KEYS; k++) {
            uint32_t v = h->final[k];
            if (has[k] && v == 0)
                VIOL(V_DURABLE, "durability: key %d is empty in the final state, but an applied write wrote it (a lost write)", k);
            else if (has[k] && v <= nw && w[v - 1].end < mb[k])
                VIOL(V_DURABLE, "durability: key %d ends with write %u, older than an applied write of the key (a lost write)", k, v);
        }
    /* 4 (the acknowledged writes of a key by their end, with the latest begin so far), then 3 and 5 per read */
    int64_t *ack[KEYS];
    size_t nack[KEYS];
    for (int k = 0; k < KEYS; k++) {
        size_t cnt = 0;
        for (uint32_t id = 1; id <= nw; id++)
            if (w[id - 1].st == WS_OK)
                for (int j = 0; j < w[id - 1].nk; j++)
                    cnt += w[id - 1].keys[j] == k;
        ack[k] = malloc((cnt + 1) * 2 * sizeof(int64_t));
        if (!ack[k])
            die(2, "out of memory");
        size_t m = 0;
        for (uint32_t id = 1; id <= nw; id++)
            if (w[id - 1].st == WS_OK)
                for (int j = 0; j < w[id - 1].nk; j++)
                    if (w[id - 1].keys[j] == k) {
                        ack[k][2 * m] = w[id - 1].end;
                        ack[k][2 * m + 1] = w[id - 1].begin;
                        m++;
                    }
        qsort(ack[k], m, 2 * sizeof(int64_t), cmp_i64pair);
        for (size_t j = 1; j < m; j++)
            if (ack[k][2 * j + 1] < ack[k][2 * j - 1])
                ack[k][2 * j + 1] = ack[k][2 * j - 1]; /* now the latest begin among those that ended no later */
        nack[k] = m;
    }
    for (int s = 0; s < MAXSESS; s++) {
        struct rd *prev = NULL;
        for (uint32_t j = 0; j < h->nr[s]; j++) {
            struct rd *rd = &h->r[s][j];
            /* 6 (kv-via): a client's read on the connection of its acknowledged write shows that write or a newer one.
             * Deliberately stricter than 4 about an earlier write of the same key whose outcome was unknown: check 4
             * lets it land at any time, but the entry node and the owner refuse a frame older than fwd_max_age, so an
             * old frame applied after the client's later acknowledged write and then read back is the anomaly (review
             * F5: a proxy that delays a frame without bound would show it as well; none of the nemeses does) */
            if (rd->own && rd->own <= nw)
                for (int q = 0; q < w[rd->own - 1].nk; q++) {
                    int k = w[rd->own - 1].keys[q];
                    if (rd->seen[k] < rd->own)
                        VIOL(V_RYW, "read-your-writes: key %d shows %s after the client's own acknowledged write %u (read %u, "
                                    "answered by node %d)", k, rd->seen[k] ? "an older write" : "nothing", rd->own, j,
                             rd->node);
                }
            for (int k = 0; k < KEYS; k++) {
                uint32_t v = rd->seen[k];
                /* 4: the writes of the key acknowledged before this read began */
                size_t lo = 0, hi = nack[k];
                while (lo < hi) {
                    size_t mid = (lo + hi) / 2;
                    if (ack[k][2 * mid] < rd->begin)
                        lo = mid + 1;
                    else
                        hi = mid;
                }
                if (lo > 0) {
                    int pr = viol_by[V_STALE] < 3;
                    if (v == 0)
                        VIOL(V_STALE, "stale read: key %d is empty in a read that began after it was written and acknowledged "
                                     "(read %u, answered by node %d)", k, j, rd->node);
                    else if (v <= nw && w[v - 1].end < ack[k][2 * (lo - 1) + 1])
                        VIOL(V_STALE, "stale read: key %d shows write %u, older than an acknowledged write of the key (read %u, "
                                     "answered by node %d)", k, v, j, rd->node);
                    else
                        pr = 0;
                    if (pr) {
                        trace_read(h, "the read", rd);
                        trace_op(h, "it saw", v);
                        trace_op(h, "acknowledged before it", trace_acked_before(h, k, rd->begin));
                    }
                }
                /* 3: what a write that is seen did at its other keys */
                if (v && v <= nw)
                    for (int q = 0; q < w[v - 1].nk; q++) {
                        int k2 = w[v - 1].keys[q];
                        uint32_t v2 = rd->seen[k2];
                        if (v2 == 0 || (v2 <= nw && w[v2 - 1].end < w[v - 1].begin))
                            VIOL(V_ATOMIC, "atomic visibility: read %u sees write %u at key %d, but %s at key %d", j, v, k,
                                 v2 ? "an older write" : "nothing", k2);
                    }
                /* 5: monotonic reads of a session */
                if (prev && prev->seen[k] && prev->seen[k] <= nw) {
                    uint32_t pv = prev->seen[k];
                    if (v == 0 || (v <= nw && w[v - 1].end < w[pv - 1].begin)) {
                        int pr = viol_by[V_MONO] < 3;
                        VIOL(V_MONO, "monotonic reads: key %d went back from write %u to %s", k, pv, v ? "an older one" : "nothing");
                        if (pr) {
                            trace_read(h, "the earlier read", prev);
                            trace_read(h, "the later read", rd);
                            trace_op(h, "the earlier read saw", pv);
                            trace_op(h, "the later read saw", v);
                        }
                    }
                }
            }
            prev = rd;
        }
    }
    /* 7 (kv-via, fwdstall): no write held at its entry node past fwd_max_age was applied. Known imprecision (review F6):
     * a write sent just before the file appeared (inside STALL_LEAD_NS) that waited 1.3 s or more on a slow commit
     * and was acknowledged near te matches without being an old frame; nothing else but the nemesis slows a write that
     * much in a quiet run, and the lead is what lets a writer stuck in flight when the file appeared be seen at all */
    for (int q = 0; q < nstalls; q++)
        for (uint32_t id = 1; id <= nw; id++) {
            struct wr *x = &w[id - 1];
            if (x->node == stalls[q].node && x->st == WS_OK && x->begin >= stalls[q].ts - STALL_LEAD_NS &&
                x->begin <= stalls[q].te - (int64_t)STALL_OLD_MS * 1000000 && x->end >= stalls[q].te - 200000000 &&
                x->end - x->begin >= 1300000000LL)
                VIOL(V_OLD, "old frame: write %u, sent to node %d at %lld ms, was held there until %lld ms (fwd_max_age is "
                            "1000 ms) and acknowledged at %lld ms: a frame past its age was applied", id, x->node,
                     (long long)(x->begin / 1000000), (long long)(stalls[q].te / 1000000), (long long)(x->end / 1000000));
        }
    for (int k = 0; k < KEYS; k++)
        free(ack[k]);
    free(seen);
    return viol;
}

/* ------------------------------------------------------------------ replicas equal (the hook of r3d and r3e) */

struct snapshot {
    char name[96];
    size_t n;
    int64_t *ts;
    float *v;
};
struct snaps {
    struct snapshot *s;
    size_t n, cap;
};
struct fe_ctx {
    tsdb *db;
    struct snaps *out;
};
static int fe_collect(void *ctx, tsdb_sid id)
{
    struct fe_ctx *c = ctx;
    tsdb_points p;
    memset(&p, 0, sizeof p);
    if (tsdb_query_points(c->db, id, INT64_MIN, INT64_MAX, &p) != 0)
        return 1;
    if (c->out->n == c->out->cap) {
        c->out->cap = c->out->cap ? c->out->cap * 2 : 16;
        c->out->s = realloc(c->out->s, c->out->cap * sizeof *c->out->s);
    }
    struct snapshot *s = &c->out->s[c->out->n++];
    snprintf(s->name, sizeof s->name, "%s", tsdb_series_name(c->db, id));
    s->n = p.n;
    s->ts = malloc((p.n + 1) * sizeof *s->ts);
    s->v = malloc((p.n + 1) * sizeof *s->v);
    memcpy(s->ts, p.ts, p.n * sizeof *s->ts);
    memcpy(s->v, p.values, p.n * sizeof *s->v);
    tsdb_points_free(&p);
    return 0;
}
static int cmp_snap(const void *a, const void *b) { return strcmp(((const struct snapshot *)a)->name, ((const struct snapshot *)b)->name); }
static void snaps_free(struct snaps *s)
{
    for (size_t i = 0; i < s->n; i++) {
        free(s->s[i].ts);
        free(s->s[i].v);
    }
    free(s->s);
    memset(s, 0, sizeof *s);
}
static int snaps_take(const char *dir, struct snaps *out)
{
    tsdb *db;
    tsdb_options o;
    tsdb_options_init(&o);
    memset(out, 0, sizeof *out);
    if (tsdb_open(dir, &o, &db) != 0)
        return -1;
    struct fe_ctx c = {db, out};
    int rc = tsdb_series_foreach(db, fe_collect, &c);
    if (rc == 0 && out->n)
        qsort(out->s, out->n, sizeof *out->s, cmp_snap);
    tsdb_close(db);
    return rc;
}
static int cv_collect(void *ctx, const char *name, const int64_t *ts, const float *v, size_t n)
{
    struct snaps *out = ctx;
    if (out->n == out->cap) {
        out->cap = out->cap ? out->cap * 2 : 16;
        out->s = realloc(out->s, out->cap * sizeof *out->s);
        if (!out->s)
            die(2, "out of memory");
    }
    struct snapshot *s = &out->s[out->n++];
    snprintf(s->name, sizeof s->name, "%s", name);
    s->n = n;
    s->ts = malloc((n + 1) * sizeof *s->ts);
    s->v = malloc((n + 1) * sizeof *s->v);
    if (!s->ts || !s->v)
        die(2, "out of memory");
    memcpy(s->ts, ts, n * sizeof *ts);
    memcpy(s->v, v, n * sizeof *v);
    return 0;
}
/* a cluster volume's directory, opened read-only in place (tests/cvol_check.c) */
static int snaps_take_cvol(const char *dir, struct snaps *out)
{
    memset(out, 0, sizeof *out);
    int rc = cvol_rows(dir, cv_collect, out);
    if (rc == 0 && out->n)
        qsort(out->s, out->n, sizeof *out->s, cmp_snap);
    return rc;
}
static int snaps_diff(const struct snaps *a, const struct snaps *b, const char *la, const char *lb)
{
    int diff = 0;
    if (a->n != b->n) {
        fprintf(stderr, "jepsen-cluster: %s has %zu series, %s has %zu\n", la, a->n, lb, b->n);
        return 1;
    }
    for (size_t i = 0; i < a->n; i++) {
        const struct snapshot *x = &a->s[i], *y = &b->s[i];
        if (strcmp(x->name, y->name) || x->n != y->n ||
            (x->n && (memcmp(x->ts, y->ts, x->n * sizeof *x->ts) || memcmp(x->v, y->v, x->n * sizeof *x->v)))) {
            fprintf(stderr, "jepsen-cluster: series %s differs: %s has %zu rows, %s has %s %zu\n", x->name, la, x->n, lb,
                    y->name, y->n);
            diff++;
        }
    }
    return diff;
}

/* Every replica directory of a volume, opened in-process after a clean stop: the same series, the same rows, and the
 * same again after tsdb_optimize of each (design-cluster.md 13.2). Returns the number of differences. */
static int replicas_equal(const char *const *dirs, int n, int cluster)
{
    int diff = 0;
    struct snaps first, cur;
    memset(&first, 0, sizeof first);
    for (int pass = 0; pass < 2 - cluster; pass++) { /* a replica directory is only read: no tsdb_optimize there */
        if (pass == 1)
            for (int i = 0; i < n; i++) {
                tsdb *db;
                tsdb_options o;
                tsdb_options_init(&o);
                if (tsdb_open(dirs[i], &o, &db) != 0 || tsdb_optimize(db) != 0) {
                    fprintf(stderr, "jepsen-cluster: cannot optimize %s\n", dirs[i]);
                    return diff + 1;
                }
                tsdb_close(db);
            }
        for (int i = 0; i < n; i++) {
            if ((cluster ? snaps_take_cvol(dirs[i], &cur) : snaps_take(dirs[i], &cur)) != 0) {
                fprintf(stderr, "jepsen-cluster: cannot read %s\n", dirs[i]);
                return diff + 1;
            }
            if (i == 0) {
                snaps_free(&first);
                first = cur;
            } else {
                diff += snaps_diff(&first, &cur, dirs[0], dirs[i]);
                snaps_free(&cur);
            }
        }
    }
    snaps_free(&first);
    return diff;
}

/* The replica directories of the replicated volume (kv, classes): node directory/v<number>, the leader's among them. */
static int replica_dirs(const char **dirs, int max)
{
    int n = 0;
    for (int r = 0; want_rep && r < rep_n && n < max; r++)
        dirs[n++] = rep_dir[r];
    return n;
}

/* The keys of history h in a replica's snapshot (as kv_read_all reads them through a client). */
static void snap_seen(const struct snaps *sn, const char *name, uint32_t *seen, int *stray)
{
    memset(seen, 0, KEYS * sizeof *seen);
    *stray = 0;
    for (size_t i = 0; i < sn->n; i++) {
        if (strcmp(sn->s[i].name, name))
            continue;
        for (size_t j = 0; j < sn->s[i].n; j++) {
            int k = ts_key(sn->s[i].ts[j]);
            if (k < 0)
                *stray = 1;
            else if (!isnan(sn->s[i].v[j]))
                seen[k] = (uint32_t)sn->s[i].v[j];
        }
    }
}

static int check_hist(struct hist *h, uint64_t *seen_indet_applied);

/* The filler's rows in one replica's state against its batches: a large insert is all or nothing, so every batch is
 * whole (each row at its timestamp with its value) or absent; an acknowledged one is whole where `in_isr` (its commit
 * point counted every member of the ISR: rep_isr_mask), a refused one absent; a row outside every batch is stray. This
 * holds after the terminal crash point too, which is where it matters: prepared-before-commit and class-switch cut a
 * large write or a switch in the middle. --mutant rep-half-batch drops half of one acknowledged batch from the last
 * replica's count. Returns the violations; counts the indeterminate batches this replica applied into *indet_applied. */
static int fill_check(const struct snaps *sn, int r, int in_isr, uint64_t *indet_applied)
{
    const struct snapshot *x = NULL;
    for (size_t i = 0; i < sn->n; i++)
        if (!strcmp(sn->s[i].name, "kvr.fill"))
            x = &sn->s[i];
    int *cnt = calloc(nfb + 1, sizeof *cnt), bad = 0;
    if (!cnt)
        die(2, "out of memory");
    size_t b = 0;
    for (size_t j = 0; x && j < x->n; j++) {
        int64_t t = x->ts[j];
        if (j && t <= x->ts[j - 1]) {
            fprintf(stderr, "jepsen-cluster: replica %s: kvr.fill's rows are out of order at %" PRId64 "\n", rep_dir[r], t);
            bad++;
            break;
        }
        while (b < nfb && fb[b].ts1 < t)
            b++;
        if (b == nfb || t < fb[b].ts0) {
            if (bad++ < 5)
                fprintf(stderr, "jepsen-cluster: replica %s: kvr.fill has a row at %" PRId64 " that no batch wrote\n",
                        rep_dir[r], t);
            continue;
        }
        if (x->v[j] != (float)(cnt[b] % 997) && bad++ < 5)
            fprintf(stderr, "jepsen-cluster: replica %s: kvr.fill batch %zu row %d holds %g, written %d\n", rep_dir[r], b,
                    cnt[b], (double)x->v[j], cnt[b] % 997);
        cnt[b]++;
    }
    if (r == rep_n - 1 && !strcmp(mutant, "rep-half-batch"))
        for (size_t i = 0; i < nfb; i++)
            if (fb[i].st == WS_OK && cnt[i] == fb[i].n) {
                cnt[i] /= 2;
                break;
            }
    for (size_t i = 0; i < nfb; i++) {
        const char *why = NULL;
        if (cnt[i] != 0 && cnt[i] != fb[i].n)
            why = "half of a large write";
        else if (fb[i].st == WS_OK && cnt[i] == 0 && in_isr)
            why = "an acknowledged large write is missing";
        else if (fb[i].st == WS_FAIL && cnt[i] != 0)
            why = "a refused large write was applied";
        else if (fb[i].st == WS_INDET && cnt[i])
            ++*indet_applied;
        if (why && bad++ < 5)
            fprintf(stderr, "jepsen-cluster: replica %s: %s: kvr.fill batch %zu [%" PRId64 ", %" PRId64 "] (%s) holds %d of "
                    "its %d rows\n", rep_dir[r], why, i, fb[i].ts0, fb[i].ts1,
                    fb[i].st == WS_OK ? "acknowledged" : fb[i].st == WS_FAIL ? "refused" : "indeterminate", cnt[i], fb[i].n);
    }
    free(cnt);
    return bad;
}

/* kv and classes, after the clean stop: every replica directory, read in place, against the history. With the leader
 * live to the end (its final read in h->final; the end waited for a whole ISR) each replica holds exactly the keys of
 * that read; after the terminal crash point (no final read) each replica's state passes checks 1 and 2 as a final
 * state, alone, where `isr` (a mask of the replicas in kvr's last ISR) has it, and check 2 only where not: a replica
 * that left the ISR was not needed for what the leader acknowledged since. --mutant rep-lost-write takes an
 * acknowledged write out of the state of the last replica (after the terminal point: the last one in the ISR). Returns
 * the differences and violations. */
static int rep_check(int terminal, int isr, int *viol)
{
    int bad = 0, plant = rep_n - 1;
    if (terminal)
        for (int r = 0; r < rep_n; r++)
            if (isr & 1 << r)
                plant = r;
    for (int r = 0; r < rep_n; r++) {
        int in_isr = !terminal || (isr & 1 << r);
        if (!in_isr)
            printf("jepsen-cluster: replica %s is not in kvr's last ISR: checked for validity and whole-or-absent large "
                   "writes, not for every acknowledged write\n", rep_dir[r]);
        struct snaps sn;
        int rc = snaps_take_cvol(rep_dir[r], &sn);
        if (rc != 0) {
            fprintf(stderr, "jepsen-cluster: cannot read the replica %s read-only: %d\n", rep_dir[r], rc);
            bad++;
            continue;
        }
        for (int i = 0; i < nhists; i++) {
            struct hist *h = &hists[i];
            uint32_t seen[KEYS];
            int stray;
            snap_seen(&sn, h->name, seen, &stray);
            if (r == plant && !strcmp(mutant, "rep-lost-write"))
                for (int k = 0; k < KEYS; k++)
                    if (seen[k] && seen[k] <= h->nw && h->w[seen[k] - 1].st == WS_OK) {
                        seen[k] = 0;
                        break;
                    }
            if (stray) {
                fprintf(stderr, "jepsen-cluster: replica %s: %s has a timestamp that is no key\n", rep_dir[r], h->name);
                bad++;
            }
            if (!terminal) {
                for (int k = 0; k < KEYS; k++)
                    if (seen[k] != h->final[k]) {
                        fprintf(stderr, "jepsen-cluster: replica %s: %s key %d holds write %u, the leader's final read %u\n",
                                rep_dir[r], h->name, k, seen[k], h->final[k]);
                        bad++;
                        break;
                    }
                continue;
            }
            /* the replica's state as the final state, the reads left out (they were checked once already) */
            uint32_t nr[MAXSESS], fin[KEYS];
            char name[sizeof h->name];
            memcpy(nr, h->nr, sizeof nr);
            memcpy(fin, h->final, sizeof fin);
            memcpy(name, h->name, sizeof name);
            memset(h->nr, 0, sizeof h->nr);
            memcpy(h->final, seen, sizeof seen);
            h->have_final = !stray && in_isr; /* check 1 (durability) needs a final state that must hold every write */
            snprintf(h->name, sizeof h->name, "%.20s@n%d", name, rep_nodes[r]);
            uint64_t dummy = 0;
            *viol += check_hist(h, &dummy);
            memcpy(h->nr, nr, sizeof nr);
            memcpy(h->final, fin, sizeof fin);
            memcpy(h->name, name, sizeof name);
            h->have_final = 0;
        }
        uint64_t fapplied = 0;
        int fbad = fill_check(&sn, r, in_isr, &fapplied);
        if (fbad)
            fprintf(stderr, "jepsen-cluster: replica %s: %d violation(s) of the filler's large writes\n", rep_dir[r], fbad);
        else
            printf("jepsen-cluster: replica %s: kvr.fill holds every one of its %zu batches whole or not at all "
                   "(%" PRIu64 " indeterminate applied)\n", rep_dir[r], nfb, fapplied);
        bad += fbad;
        snaps_free(&sn);
    }
    return bad;
}

/* --selftest-replicas: the comparison itself, on two databases made here (no cluster) */
static int selftest_replicas(void)
{
    char a[PATH_MAX + 8], b[PATH_MAX + 8];
    snprintf(a, sizeof a, "%s/ra", rundir);
    snprintf(b, sizeof b, "%s/rb", rundir);
    const char *dirs[2] = {a, b};
    for (int i = 0; i < 2; i++) {
        mkdir(dirs[i], 0755);
        tsdb *db;
        tsdb_options o;
        tsdb_options_init(&o);
        tsdb_sid sid;
        if (tsdb_open(dirs[i], &o, &db) != 0 || tsdb_series_at(db, "s", NULL, &sid) != 0)
            die(2, "selftest-replicas: cannot make %s", dirs[i]);
        for (int j = 0; j < 2000; j++)
            if (tsdb_append(db, sid, KV_BASE_TS + j * 1000, (double)j) != 0)
                die(2, "selftest-replicas: append");
        tsdb_close(db);
    }
    if (replicas_equal(dirs, 2, 0) != 0)
        die(1, "selftest-replicas: two equal databases differ");
    tsdb *db;
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb_sid sid;
    if (tsdb_open(b, &o, &db) != 0 || tsdb_series_at(db, "s", NULL, &sid) != 0 || tsdb_append(db, sid, KV_BASE_TS + 5000000, 1.0) != 0)
        die(2, "selftest-replicas: planting a row");
    tsdb_close(db);
    if (replicas_equal(dirs, 2, 0) == 0)
        die(1, "selftest-replicas: a row that only one replica has went unnoticed");
    printf("selftest-replicas: ok (equal replicas pass, a row on one replica is found)\n");
    return 0;
}

/* ------------------------------------------------------------------ nemeses */

/* The crash points a midcommit round may arm. `placed`: the call exists in server code (r3d and r3e flip it as they
 * place theirs); `window_ms`: how long the armed node may run before the point is called unfired; `leader`: 1, only the
 * leader of a replicated volume passes it (server/repld.c), 2, only a failover passes it (a replica saving a fence, the
 * promoted replica, the deposed leader). Neither is armed by a restart (a restart of the leader would end its
 * leadership, and a failover point would go unfired) but at every node's first start behind a trigger file, once per
 * run (nem_terminal; kv and classes), which makes the failover for a point of 2. */
static struct {
    const char *name;
    int placed, window_ms, leader;
    char r4kind; /* R4's points (added at the start from crashpt.h's names, r4cp): which operation passes it, see nem_terminal_r4 */
} cps[24] = {{"cluster-started", 1, 5000, 0, 0},
           {"serving", 1, 5000, 0, 0},
           {"wal-before-send", 1, 20000, 1, 0},
           {"acked-before-ok", 1, 20000, 1, 0},
           {"prepared-before-commit", 1, 20000, 1, 0},
           {"class-switch", 1, 30000, 1, 0},
           {"fence-saved-before-reply", 1, 30000, 2, 0},
           {"promote-before-epoch", 1, 30000, 2, 0},
           {"depose-before-reopen", 1, 30000, 2, 0}};
static int ncps = 9;
#define NCPS ncps
/* R4's crash points by the name server/crashpt.h gives them: a point is a terminal point only when the track that
 * placed it has added it there and a nemesis of this run provides its operation (S: a replica back after longdown or
 * rebuild takes a snapshot, T: a planned transfer, M: a move) */
static const struct {
    const char *name;
    char kind;
} r4cp[] = {{"snap-received", 'S'}, {"snap-swapped", 'S'}, {"set-removed", 'M'},
            {"transfer-ready", 'T'}, {"move-added", 'M'}, {"move-shrunk", 'M'}};
static int r4_cp_ok(int k)
{
    char c = cps[k].r4kind;
    return !c || (c == 'S' ? (nemeses & (N_LONGDOWN | N_REBUILD)) : (nemeses & N_CHURN));
}

static unsigned fwd_stalls_held;
static unsigned faults[NNEM], restarts, cp_hits, cp_hits_serving, cp_unfired, power_hits, power_idle;
/* every nemesis round must show the effect it is for (a kill that reaped a SIGKILLed child, a pause that left the child
 * stopped, ...): a round that did nothing is a failed run, whatever the checks of the workloads say. */
static int effect_seen;
static char unfired_msg[512];

static void hold_ms(int ms)
{
    for (int t = 0; t < ms; t += 100) {
        check_unexpected_exits();
        usleep(100000);
    }
}

/* a node to kill, power off, pause or arm: kv and classes pick kvr's leader half of the time (a failover under load),
 * `bias_leader` the Raft leader half of the time (of the rest, with kvr) */
static int pick_node(int bias_leader)
{
    int k = atomic_load(&kv_leader), l = atomic_load(&last_leader);
    if (want_via)
        atomic_fetch_add(&via_picks, 1);
    if (want_via && rnd() % 3 == 0) { /* kv-via: a third of the picks go to a node a client is connected to (an entry node) */
        int x = via_pick(0);
        if (x >= 0) {
            atomic_fetch_add(&via_aimed, 1);
            return x;
        }
    }
    if (want_rep && k >= 0 && k < nn && rnd() % 2)
        return k;
    if (bias_leader && l >= 0 && l < nn && rnd() % 2)
        return l;
    return rnd_range(0, nn - 1);
}

static int term_cp = -1, term_fired, term_done, isr_short;
static char term_trigger[PATH_MAX], term_spec[PATH_MAX + 64];

/* a restart before the terminal point keeps the node armed for it: kvr's leader and replicas are restarted too, and any
 * of them may be the one that must end there */
static void restart_node(int i)
{
    int st = 0;
    if (start_node(i, term_cp >= 0 && !term_done ? term_spec : NULL, &st) != 0)
        die(1, "node %d exited with status 0x%x right after a restart (log %s)", i, st, nodes[i].log);
    restarts++;
}

static void nem_kill(void)
{
    int i = pick_node(1);
    if (strcmp(mutant, "nokill")) { /* nokill: planted, nobody is killed */
        int st = stop_node(i, SIGKILL);
        if (WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL) /* the child was alive and the kernel reaped it as killed */
            effect_seen = 1;
        hold_ms(rnd_range(200, 1500));
        restart_node(i);
    } else
        hold_ms(rnd_range(200, 1500));
}

static void nem_power(void)
{
    int i = pick_node(1);
    pid_t pid;
    node_snap(i, &pid, NULL, NULL);
    signal_child(pid, SIGUSR2); /* the shim cuts the power at the node's next file operation */
    int st = await_exit(i, 6000);
    ev("node %d: power cut (status 0x%x)", i, st);
    if (st == -1) { /* no file operation in time: the node goes down as with kill */
        int ks = stop_node(i, SIGKILL);
        power_idle++;
        effect_seen = WIFSIGNALED(ks) && WTERMSIG(ks) == SIGKILL;
    } else if (WIFEXITED(st) && WEXITSTATUS(st) == POWER_EXIT) {
        power_hits++;
        effect_seen = 1;
    } else
        die(1, "node %d ended with status 0x%x after SIGUSR2, not a power loss (log %s)", i, st, nodes[i].log);
    hold_ms(rnd_range(200, 1000));
    restart_node(i);
}

/* Has a live node noticed that member `id` is silent? The leader's `heard` age for it, or the member's state in the
 * metadata: what a pause is meant to cause, seen from outside the paused process. */
static int silence_seen(int paused, uint32_t id)
{
    for (int j = 0; j < nn; j++) {
        if (j == paused)
            continue;
        struct view v;
        if (view_get(j, &v, 500) != 0)
            continue;
        int seen = 0;
        for (uint32_t k = 0; k < v.st.nheard; k++)
            if (v.st.heard[k].id == id && v.st.heard[k].ago_ms >= S.node_timeout_ms / 2)
                seen = 1;
        for (uint32_t k = 0; k < v.m.nmembers; k++)
            if (v.m.members[k].id == id && v.m.members[k].state != META_UP)
                seen = 1;
        view_free(&v);
        if (seen)
            return 1;
    }
    return 0;
}

static unsigned pauses_done, pauses_seen;

static void nem_pause(void)
{
    int i = pick_node(1);
    /* past the node timeout (the others mark it down) or past the lease, from the cluster's own settings */
    int ms = (rnd() % 2 ? (int)S.node_timeout_ms : (int)S.lease_s * 1000) + rnd_range(400, 1200);
    pid_t pid;
    node_snap(i, &pid, NULL, NULL);
    uint32_t id = atomic_load(&nodes[i].id);
    ju_fault(i);
    pause_node(i, 1);
    int stopped = await_stopped(pid, 1, 2000); /* the kernel says it is stopped */
    int witnessed = 0;
    for (int t = 0; t < ms; t += 300) {
        hold_ms(300);
        if (!witnessed && t >= (int)S.node_timeout_ms / 2)
            witnessed = silence_seen(i, id);
    }
    pause_node(i, 0);
    if (!await_stopped(pid, 0, 2000))
        die(1, "node %d is still stopped after SIGCONT", i);
    pauses_done++;
    pauses_seen += witnessed;
    effect_seen = stopped;
}

/* fwdstall (kv-via): the entry node of some client holds every forward before its send (the test hook
 * TSDBD_TEST_FWD_STALL: while the file exists the node stalls there) for longer than fwd_max_age (1 s). A request that
 * reached it meanwhile is too old when the stall ends and must be answered UNAVAILABLE, definitely not applied, and
 * never sent late (check 7 of check_hist). The round is a failure when no write was held a second at the node. */
#define STALL_HOLD_MS 2000
static unsigned fwdstall_once(void)
{
    int i = rnd_range(0, nn - 1), l = atomic_load(&kv_leader);
    if (i == l)
        i = (i + 1) % nn;
    /* the writers are sent to node i (they reconnect at their next request), and the stall starts once one of them is there */
    atomic_store(&via_steer, i);
    int there = 0;
    for (int64_t t0 = now_ms(); !there && now_ms() - t0 < 10000; hold_ms(100))
        there = atomic_load(&via_entry[0][0]) == i || atomic_load(&via_entry[1][0]) == i;
    if (!there)
        die(1, "the fwdstall nemesis: no writer was connected to node %d in 10 s", i);
    ev("fwdstall: node %d picked; writers at %d and %d, readers at %d and %d, kvr's leader %d", i, atomic_load(&via_entry[0][0]),
       atomic_load(&via_entry[1][0]), atomic_load(&via_entry[0][1]), atomic_load(&via_entry[1][1]), atomic_load(&kv_leader));
    char f[PATH_MAX + 16];
    snprintf(f, sizeof f, "%s/fwd-stall", nodes[i].dir);
    int fd = open(f, O_WRONLY | O_CREAT, 0644);
    if (fd < 0)
        die(2, "create %s: %s", f, strerror(errno));
    close(fd);
    int lead0 = atomic_load(&kv_leader), moved = lead0 == i;
    int64_t ts = now_ns();
    for (int t = 0; t < STALL_HOLD_MS; t += 100) {
        hold_ms(100);
        moved |= atomic_load(&kv_leader) != lead0;
    }
    int64_t te = now_ns();
    ev("fwdstall: at the end writers at %d and %d", atomic_load(&via_entry[0][0]), atomic_load(&via_entry[1][0]));
    unlink(f);
    atomic_store(&via_steer, -1);
    hold_ms(300); /* the held requests are answered once the entry node's stall loop sees the file gone */
    moved |= atomic_load(&kv_leader) != lead0;
    unsigned held = 0, inwin = 0, bysort[3] = {0, 0, 0};
    int64_t longest = 0;
    for (int hi = 0; hi < nhists; hi++) {
        struct hist *h = &hists[hi];
        pthread_mutex_lock(&h->mu);
        for (uint32_t id = 1; id <= h->nw; id++) {
            const struct wr *x = &h->w[id - 1];
            if (x->node == i && x->begin >= ts - STALL_LEAD_NS && x->begin <= te) {
                inwin++;
                bysort[x->st]++;
                if (x->end != INT64_MAX && x->end - x->begin > longest)
                    longest = x->end - x->begin;
            }
            held += x->node == i && x->begin >= ts - STALL_LEAD_NS && x->begin <= te - 1000000000LL && x->st != WS_INDET &&
                    x->end - x->begin >= 1000000000LL;
        }
        pthread_mutex_unlock(&h->mu);
    }
    ev("node %d: %u writes began in the window (indeterminate %u, acknowledged %u, refused %u), the longest %lld ms", i,
       inwin, bysort[0], bysort[1], bysort[2], (long long)(longest / 1000000));
    ev("node %d: forwards held %d ms (%u writes held a second or more%s)", i, STALL_HOLD_MS, held,
       moved ? "; kvr's leader changed meanwhile, the window is not checked" : "");
    if (!moved && nstalls < MAXSTALLS)
        stalls[nstalls++] = (struct stall_win){.node = i, .ts = ts, .te = te};
    fwd_stalls_held += held;
    return held;
}

/* A round that holds no write at its node (the writers were between two connections, or a leader change took the
 * window) is tried again, up to 3 times; the miss is counted and printed. A stall that never holds anything fails. */
static unsigned fwd_stall_misses;
static void nem_fwdstall(void)
{
    for (int attempt = 0; attempt < 3 && !effect_seen; attempt++) {
        effect_seen = fwdstall_once() > 0;
        fwd_stall_misses += !effect_seen;
    }
}

/* ------------------------------------------------------------------ R4's nemeses */

static unsigned kill2_rep, kill2_done, slow_done, longdown_done, longdown_snaps, rebuild_done, rebuild_ok, rebuild_snaps, churn_moves,
    churn_transfers, churn_refused, churn_cancels, churn_landed;

/* an admin op to some running node, which follows the Raft leader (the R4 ops are the leader's) */
static int admin_any(int op, const void *req, size_t rl, int timeout_ms)
{
    int start = rnd_range(0, nn - 1);
    for (int k = 0; k < nn; k++) {
        int i = (start + k) % nn;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid > 0 && !node_paused(i))
            return admin_real(i, op, req, rl, 1, timeout_ms);
    }
    die(2, "no node is running for an admin op");
    return -1;
}

/* The inode of node i's directory of kvr (0: none). An installed snapshot swaps v<N>.snap in for v<N> (src/snapshot.c,
 * snap_install: rename, never a copy into the old one), so a replica that took one has another inode afterwards, and a
 * replica that caught up from the log, was restarted or was only slowed never does. This is the evidence of a
 * snapshot that needs no log line: the only line with "installed" in the tree is a refusal (src/snapshot.c, a staged
 * path that is not v<N>.snap), and a successful install prints none. The old directory is removed only after the new
 * one exists, so the two never share an inode number. */
static unsigned long long vdir_ino(int i)
{
    char vd[PATH_MAX + 64];
    struct stat sb;
    snprintf(vd, sizeof vd, "%s/v%" PRIu32, nodes[i].dir, rep_number);
    return stat(vd, &sb) == 0 ? (unsigned long long)sb.st_ino : 0;
}

/* kvr whole again: rep_n replicas, no learner, the ISR all of them. 1, or 0 after `ms`. */
static int rep_no_learners(const struct rrec *r)
{
    for (int k = 0; k < r->nrep; k++)
        if (r->learner[k])
            return 0;
    return 1;
}

static int rep_wait_whole(int ms)
{
    for (int64_t t0 = now_ms(); now_ms() - t0 < ms; usleep(300000)) {
        check_unexpected_exits();
        struct rrec r;
        uint64_t ver;
        if (rep_rec_get(&r) == 0 && r.nrep == rep_n && !r.moving && rep_no_learners(&r) &&
            rep_isr_mask(&ver) == rep_full_mask())
            return 1;
    }
    return 0;
}

/* a replica of kvr that does not lead it (the leader's own failover is the other nemeses' business): its node */
static int rep_follower_pick(void)
{
    int l = atomic_load(&kv_leader), pick[META_REPLICAS_MAX], np = 0;
    for (int r = 0; r < rep_n; r++)
        if (rep_nodes[r] != l)
            pick[np++] = rep_nodes[r];
    return np ? pick[rnd_range(0, np - 1)] : -1;
}

static int hold_hi(int cap_ms) { return seconds * 200 < 5000 ? 5000 : seconds * 200 > cap_ms ? cap_ms : seconds * 200; }

/* kill2: two servers SIGKILLed at once, restarted after 5 to 60 s (less in a short run). A volume may be unavailable
 * meanwhile; once both are back every acknowledged write must be there: the checks of the workload and the end. */
static void nem_kill2(void)
{
    int a = pick_node(1), b = -1;
    /* The case this nemesis is for: two of kvr's replicas down, so the volume has no quorum and the minority that
     * is left holds acknowledged writes. By chance that is about a third of the rounds, so the first round of a run
     * with kvr and every other one at even odds takes two of its replicas (counted, and required at the end). The
     * metadata group (5 nodes) keeps its majority under any two kills. */
    if (want_rep && (kill2_done == 0 || rnd() % 2 == 0)) {
        int r1 = rnd_range(0, rep_n - 1), r2 = (r1 + rnd_range(1, rep_n - 1)) % rep_n;
        a = rep_nodes[r1];
        b = rep_nodes[r2];
        kill2_rep++;
    }
    for (int t = 0; t < 8 && (b < 0 || b == a); t++)
        b = pick_node(0);
    if (b < 0 || b == a)
        b = (a + rnd_range(1, nn - 1)) % nn;
    pid_t pa, pb;
    node_snap(a, &pa, NULL, NULL);
    node_snap(b, &pb, NULL, NULL);
    int planted = !strcmp(mutant, "nokill2"); /* planted: the second server is not killed */
    signal_child(pa, SIGKILL); /* both signalled before either is reaped: at once */
    if (!planted)
        signal_child(pb, SIGKILL);
    int sa = stop_node(a, SIGKILL), sb = planted ? 0 : stop_node(b, SIGKILL);
    effect_seen = WIFSIGNALED(sa) && WTERMSIG(sa) == SIGKILL && WIFSIGNALED(sb) && WTERMSIG(sb) == SIGKILL;
    ev("kill2: nodes %d and %d", a, b);
    int hi = hold_hi(60000);
    hold_ms(rnd_range(hi < 5000 ? hi : 5000, hi));
    restart_node(a);
    if (!planted)
        restart_node(b);
    kill2_done++;
}

/* The shim's answer for node i: its directory (the prefix the commands name) and the delay it is under. 0 or -1. */
static int outage_state(int i, char *pfx, size_t pn, int *slow_ms)
{
    char out[1024], path[PATH_MAX];
    int ms = -1;
    if (outagefs_cmd(nodes[i].octl, "state", out, sizeof out) != 0 ||
        sscanf(out, "%4095s %*s slow=%d", path, &ms) != 2)
        return -1;
    snprintf(pfx, pn, "%s", path);
    *slow_ms = ms;
    return 0;
}

/* slow: one server's data root (its state directory too: the Raft log) slowed by 10, 30 or 100 ms a call for 5 to 30 s
 * through the shim, stacked under crashfs when power runs. The effect is read back from the shim (state: slow=MS). */
static void nem_slow(void)
{
    static const int ms_tab[] = {10, 30, 100};
    int i = pick_node(1), ms = ms_tab[rnd() % 3], cur = -1;
    char pfx[PATH_MAX + 8], line[PATH_MAX + 64], out[512];
    if (outage_state(i, pfx, sizeof pfx, &cur) != 0)
        die(2, "slow: the outagefs shim of node %d does not answer on %s (the server is not under it?)", i, nodes[i].octl);
    ju_fault(i);
    if (strcmp(mutant, "noslow")) { /* noslow: planted, nothing is slowed */
        snprintf(line, sizeof line, "slow %s %d", pfx, ms);
        if (outagefs_cmd(nodes[i].octl, line, out, sizeof out) != 0)
            die(2, "the shim refused '%s': %s", line, out);
    }
    effect_seen = outage_state(i, pfx, sizeof pfx, &cur) == 0 && cur == ms;
    ev("slow: node %d by %d ms a call", i, ms);
    int hi = hold_hi(30000);
    int64_t ts = now_ns();
    hold_ms(rnd_range(hi < 5000 ? hi : 5000, hi));
    if (trace_on) { /* what the writes of the workloads cost meanwhile (JEPSEN_TRACE=1: evidence the shim slows the server) */
        int64_t te = now_ns(), din = 0, dall = 0;
        unsigned nin = 0, nall = 0;
        for (int hi2 = 0; hi2 < nhists; hi2++) {
            struct hist *h = &hists[hi2];
            pthread_mutex_lock(&h->mu);
            for (uint32_t id = 1; id <= h->nw; id++) {
                const struct wr *x = &h->w[id - 1];
                if (x->st != WS_OK || x->end == INT64_MAX)
                    continue;
                dall += x->end - x->begin, nall++;
                if (x->begin >= ts && x->end <= te)
                    din += x->end - x->begin, nin++;
            }
            pthread_mutex_unlock(&h->mu);
        }
        ev("slow: %u acknowledged writes in the window took %.2f ms on average, %u in the run so far %.2f ms", nin,
           nin ? (double)din / nin / 1e6 : 0.0, nall, nall ? (double)dall / nall / 1e6 : 0.0);
    }
    snprintf(line, sizeof line, "heal %s", pfx);
    if (outagefs_cmd(nodes[i].octl, line, out, sizeof out) != 0)
        die(2, "the shim refused '%s': %s", line, out);
    if (outage_state(i, pfx, sizeof pfx, &cur) != 0 || cur != 0)
        die(2, "slow: node %d is still slowed (%d ms) after heal", i, cur);
    slow_done++;
}

/* longdown: a replica of kvr down past repl_retain_s under writes, then back. The log it needs is gone by then: it has
 * to take a snapshot (r4-xferinst), which the logs of the nodes show; kvr must be whole again afterwards. */
static void nem_longdown(void)
{
    int i = rep_follower_pick();
    if (i < 0)
        return;
    unsigned long long ino0 = vdir_ino(i);
    int st = stop_node(i, SIGKILL);
    effect_seen = WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL;
    int retain = retain_eff > 0 ? retain_eff : 3;
    if (strcmp(mutant, "nolongdown")) /* nolongdown: planted, a plain kill (the node is back before its log is gone) */
        hold_ms(retain * 1000 + rnd_range(4000, 10000));
    else
        hold_ms(300);
    restart_node(i);
    longdown_done++;
    if (!rep_wait_whole(120000))
        die(1, "longdown: kvr was not whole again 120 s after node %d came back (a replica that stayed a learner: its log "
               "was gone and no snapshot was taken?)", i);
    unsigned long long ino1 = vdir_ino(i);
    if (ino0 && ino1 && ino0 != ino1) {
        longdown_snaps++;
        ev("longdown: node %d took a snapshot (its v%" PRIu32 " was replaced)", i, rep_number);
    } else
        ev("longdown: node %d caught up without a snapshot (its log was still there)", i);
}

/* does node i's log, from byte `off` on, have a line with both `a` and `b` (b may be NULL)? */
static int log_has_since(int i, long off, const char *a, const char *b)
{
    FILE *f = fopen(nodes[i].log, "r");
    if (!f)
        return 0;
    char line[4096];
    int found = 0;
    if (fseek(f, off, SEEK_SET) == 0)
        while (!found && fgets(line, sizeof line, f))
            found = strstr(line, a) && (!b || strstr(line, b));
    fclose(f);
    return found;
}

static long log_size(int i)
{
    struct stat sb;
    return stat(nodes[i].log, &sb) == 0 ? (long)sb.st_size : 0;
}

/* rebuild: a stopped replica's v<N> removed (a lost directory), the server started (it refuses the volume),
 * `replica reset`, and kvr rebuilt by a snapshot. */
static void nem_rebuild(void)
{
    int i = rep_follower_pick();
    if (i < 0)
        return;
    uint32_t id = atomic_load(&nodes[i].id);
    char vd[PATH_MAX + 64];
    snprintf(vd, sizeof vd, "%s/v%" PRIu32, nodes[i].dir, rep_number);
    int st = stop_node(i, SIGKILL);
    effect_seen = WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL;
    rmtree(vd);
    struct stat vsb;
    if (stat(vd, &vsb) == 0) /* the removal failed: the round would be a plain kill */
        die(2, "rebuild: cannot remove %s", vd);
    hold_ms(rnd_range(1500, 3000)); /* the ISR drops it (isr_lag_ms), so the reset may name it */
    long off = log_size(i);
    restart_node(i);
    rebuild_done++;
    /* The server must refuse a volume whose directory is gone, not make an empty one and rejoin (that would be a
     * replica with no history, "rebuilt" by the leader's log alone, the thing the reset exists to avoid). Its line
     * comes when its open gets there (a loaded machine, io_uring setup): a condition wait with a generous bound, never
     * a pause. No v<N> may exist before the reset and the log must not say "made" for it, at any moment of the wait. */
    char made[PATH_MAX + 80]; /* "volume 1 (kvr): made <path>, replica ..." is the open that makes one; the refusal says "not made again" */
    snprintf(made, sizeof made, "): made %s", vd);
    int refused = 0;
    for (int64_t t0 = now_ms(); !refused && now_ms() - t0 < 60000; ) {
        check_unexpected_exits();
        if (stat(vd, &vsb) == 0 || log_has_since(i, off, made, NULL))
            die(1, "rebuild: node %d started with its %s removed and made it again before any `replica reset`", i, vd);
        refused = log_has_since(i, off, vd, NULL);
        if (!refused)
            usleep(100000);
    }
    if (!refused)
        die(1, "rebuild: node %d, restarted with %s removed, has no line in its log that names the path and refuses the "
               "volume in 60 s (log %s)", i, vd, nodes[i].log);
    if (stat(vd, &vsb) == 0 || log_has_since(i, off, made, NULL))
        die(1, "rebuild: node %d started with its %s removed and made it again before any `replica reset`", i, vd);
    uint8_t body[16];
    size_t bl = cluster_volume_force_encode(rep_number, id, body); /* RESET's body is VOLUME_FORCE's: volume, node */
    int rc = -1;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 60000; usleep(1000000)) {
        check_unexpected_exits();
        rc = admin_any(CLUSTER_OP_REPLICA_RESET, body, bl, 15000);
        if (rc == 0)
            break;
    }
    if (rc != 0)
        die(1, "rebuild: `replica reset` of volume %" PRIu32 " on node %d (member %" PRIu32 ") was refused for 60 s: %s",
            rep_number, i, id, cluster_strerror(rc));
    if (!rep_wait_whole(180000))
        die(1, "rebuild: kvr was not whole again 180 s after the reset of node %d", i);
    struct stat sb;
    char repl[PATH_MAX + 80];
    snprintf(repl, sizeof repl, "%s/REPL", vd);
    if (stat(repl, &sb) != 0)
        die(1, "rebuild: node %d has no %s after the reset: the replica was not made again", i, repl);
    rebuild_ok++;
    /* SNAPDONE stays in the directory a snapshot's install swapped in (the replicas that were never rebuilt have none):
     * the log alone would have filled a replica with no SNAPDONE, which the run's end counts */
    char done[PATH_MAX + 80];
    snprintf(done, sizeof done, "%s/SNAPDONE", vd);
    if (stat(done, &sb) == 0 && strcmp(mutant, "nosnapdone")) /* nosnapdone: planted, the install's mark is not seen */
        rebuild_snaps++;
    else
        ev("rebuild: node %d was filled without a snapshot (no %s)", i, done);
}

/* the end of a move: the record settled (3 replicas, no learner: done or cancelled), kvr's replicas as it names them,
 * kvr whole */
static void churn_finish(int from, int to)
{
    int64_t t0 = now_ms();
    for (;; usleep(300000)) {
        check_unexpected_exits();
        struct rrec q;
        if (rep_rec_get(&q) == 0 && q.nrep == rep_n && !q.moving && rep_no_learners(&q))
            break;
        if (now_ms() - t0 > 240000)
            die(1, "churn: the move of kvr from node %d to node %d did not end in 240 s", from, to);
    }
    rep_sync_members();
    if (!rep_wait_whole(120000))
        die(1, "churn: kvr was not whole 120 s after a move");
}

static int churn_landed_check(int to)
{
    if (atomic_load(&kv_leader) != to)
        return 0;
    churn_landed++;
    return 1;
}

enum { CH_ANY, CH_TRANSFER_PREF, CH_TRANSFER_ONLY, CH_MOVE_ONLY };

/* One churn operation on kvr: a planned transfer to an ISR member, or a move of one replica to a node that holds none
 * (--log; one in four is cancelled shortly after). `from_pick` >= 0 forces the node the move takes the copy from.
 * `mode`: CH_ANY (a transfer one time in three), CH_TRANSFER_PREF, CH_TRANSFER_ONLY (-1 when no ISR member can take
 * the lead: no move stands in for it) or CH_MOVE_ONLY (what a crash point of a move needs).
 * Returns 1 when the operation was accepted, 0 when it was refused, -1 when it could not be made. A move is waited for:
 * kvr whole again with 3 replicas. */
static int churn_once(int from_pick, int mode, int wait)
{
    struct rrec r;
    for (int64_t t0 = now_ms(); rep_rec_get(&r) != 0 || r.nrep < rep_n || r.moving || r.leader < 0; usleep(300000))
        if (now_ms() - t0 > 60000)
            die(1, "churn: no settled record of kvr within 60 s (a move that never ended?)");
    int lead = r.leader, cand[MAXN], nc = 0;
    uint8_t body[16];
    size_t bl;
    int rc, accepted = 0;
    if (mode != CH_MOVE_ONLY && (mode != CH_ANY || rnd() % 3 == 0)) {
        for (int k = 0; k < r.nrep; k++)
            if (r.isr_has[k] && !r.learner[k] && r.node[k] >= 0 && r.node[k] != lead)
                cand[nc++] = k;
        if (nc) {
            int k = cand[rnd_range(0, nc - 1)], to = r.node[k];
            bl = cluster_volume_force_encode(rep_number, r.id[k], body);
            rc = admin_any(CLUSTER_OP_VOLUME_TRANSFER, body, bl, 20000);
            ev("churn: transfer of kvr to node %d: %s", to, cluster_strerror(rc));
            if (rc != 0) {
                churn_refused++;
                return 0;
            }
            churn_transfers++;
            for (int64_t t0 = now_ms(); wait && now_ms() - t0 < 30000 && !churn_landed_check(to); usleep(200000))
                check_unexpected_exits();
            return 1;
        }
        if (mode == CH_TRANSFER_ONLY)
            return -1;
    }
    /* a move: from a member (the forced one, or any), to a node that is no replica of kvr */
    int from = -1, to = -1, none[MAXN], nn_free = 0;
    for (int i = 0; i < nn; i++) {
        int in = 0;
        for (int k = 0; k < r.nrep; k++)
            in |= r.node[k] == i;
        if (!in && atomic_load(&nodes[i].id))
            none[nn_free++] = i;
    }
    if (!nn_free)
        return -1;
    to = none[rnd_range(0, nn_free - 1)];
    from = from_pick;
    for (int t = 0; from < 0 && t < 16; t++)
        from = r.node[rnd_range(0, r.nrep - 1)];
    if (from < 0)
        return -1;
    uint32_t fid = atomic_load(&nodes[from].id), tid = atomic_load(&nodes[to].id);
    bl = cluster_volume_move_encode(rep_number, fid, tid, CLUSTER_MOVE_LOG, body);
    rc = admin_any(CLUSTER_OP_VOLUME_MOVE, body, bl, 20000);
    ev("churn: move of kvr from node %d to node %d: %s", from, to, cluster_strerror(rc));
    if (rc != 0) {
        churn_refused++;
        return 0;
    }
    accepted = 1;
    churn_moves++;
    if (wait && rnd() % 4 == 0) { /* cancelled before A is removed, or too late: either way the move ends */
        hold_ms(rnd_range(300, 2000));
        bl = cluster_volume_move_encode(rep_number, fid, tid, CLUSTER_MOVE_LOG | CLUSTER_MOVE_CANCEL, body);
        rc = admin_any(CLUSTER_OP_VOLUME_MOVE, body, bl, 20000);
        ev("churn: cancel of the move: %s", cluster_strerror(rc));
        churn_cancels += rc == 0;
    }
    if (wait)
        churn_finish(from, to);
    return accepted;
}

static void nem_churn(void)
{
    /* an operation the server refuses at a moment (a target still catching up) is tried again for 20 s; one that is
     * refused all that time, with kvr settled, is a round that did nothing and fails the run like any idle nemesis */
    int acc = 0;
    for (int64_t t0 = now_ms(); acc <= 0 && now_ms() - t0 < 20000; ) {
        acc = churn_once(-1, CH_ANY, 1);
        if (acc <= 0)
            hold_ms(1000);
    }
    effect_seen = acc > 0;
}

/* An R4 crash point as the run's terminal point (server/crashpt.h names it; cps[].r4kind says which operation passes
 * it): every node is armed at its first start behind the trigger file, and the run does what makes the point happen once
 * the trigger exists: 'S' (a snapshot's receiver: snap-received, snap-swapped) a replica of kvr down past the caps and
 * back, 'T' (transfer-ready) a planned transfer, 'M' (a move: move-added, move-shrunk, set-removed) a move. A node that
 * ends there (status 99) is restarted and the operation finishes: kvr must be whole after it. */
static void nem_terminal_r4(void)
{
    const char *nm = cps[term_cp].name;
    char kind = cps[term_cp].r4kind;
    int win = cps[term_cp].window_ms, victim = -1, accepted = 1;
    if (kind == 'S') {
        for (int64_t tv = now_ms(); (victim = rep_follower_pick()) < 0 && now_ms() - tv < 30000; )
            hold_ms(500);
        if (victim < 0)
            die(2, "%s: no replica of kvr to take down in 30 s", nm);
        stop_node(victim, SIGKILL);
        hold_ms((retain_eff > 0 ? retain_eff : 3) * 1000 + rnd_range(4000, 10000));
    }
    int fd = open(term_trigger, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0)
        die(2, "create %s: %s", term_trigger, strerror(errno));
    close(fd);
    if (kind == 'S') {
        if (victim >= 0)
            restart_node(victim);
    } else {
        /* the operation the point belongs to, tried for 30 s with the trigger there (a target that is not ready, a
         * refusal at the moment): one that could not be made at all is the harness's, never a point that did not fire */
        accepted = 0;
        for (int64_t ta = now_ms(); accepted <= 0 && now_ms() - ta < 30000; ) {
            accepted = churn_once(strstr(nm, "set-removed") ? rep_follower_pick() : -1,
                                  kind == 'T' ? CH_TRANSFER_ONLY : CH_MOVE_ONLY, 0);
            if (accepted <= 0)
                hold_ms(1000);
        }
        if (accepted < 0)
            die(2, "%s: no %s could be made in 30 s (no ISR member to take the lead, no free node or member)", nm,
                kind == 'T' ? "transfer" : "move");
    }
    int ended[MAXN] = {0}, nend = 0;
    int64_t t0 = now_ms(), first = -1;
    for (;;) {
        for (int i = 0; i < nn; i++) {
            pid_t pid;
            node_snap(i, &pid, NULL, NULL);
            if (pid <= 0 || ended[i])
                continue;
            int st = await_exit(i, 0);
            if (st == -1)
                continue;
            if (!(WIFEXITED(st) && WEXITSTATUS(st) == CRASH_EXIT_STATUS))
                die(1, "node %d ended with status 0x%x with %s triggered, not at the point (log %s)", i, st, nm,
                    nodes[i].log);
            ended[i] = 1;
            nend++;
            if (first < 0) {
                first = now_ms();
                unlink(term_trigger); /* before the next node passes the point */
            }
        }
        if ((first >= 0 && now_ms() - first >= 1000) || now_ms() - t0 >= win)
            break;
        usleep(20000);
    }
    if (first < 0) {
        unlink(term_trigger);
        cp_unfired++;
        snprintf(unfired_msg, sizeof unfired_msg, "the armed crash point %s never fired within %d ms of its trigger (%s)", nm,
                 win, accepted ? "the operation was accepted" : "the operation was refused");
    } else {
        cp_hits += (unsigned)nend;
        cp_hits_serving += (unsigned)nend;
        term_fired = 1;
        printf("jepsen-cluster: terminal crash point %s: %d node(s) ended there\n", nm, nend);
    }
    effect_seen = 1;
    term_done = 1;
    hold_ms(rnd_range(200, 800));
    for (int i = 0; i < nn; i++)
        if (ended[i])
            restart_node(i);
    if (kind == 'S') {
        if (!rep_wait_whole(180000))
            die(1, "%s: kvr was not whole 180 s after the replica came back", nm);
    } else
        churn_finish(-1, -1);
}

static void nem_midcommit(void)
{
    int pick;
    if (!strcmp(mutant, "unfired")) {
        pick = 0; /* wal-before-send: nobody has placed it, so it can never fire */
        while (strcmp(cps[pick].name, "wal-before-send"))
            pick++;
    } else {
        int placed[NCPS], np = 0;
        for (int k = 0; k < NCPS; k++)
            if (cps[k].placed && !cps[k].leader)
                placed[np++] = k;
        pick = placed[rnd_range(0, np - 1)];
    }
    int i = pick_node(1);
    char spec[96];
    snprintf(spec, sizeof spec, "%s:1", cps[pick].name);
    stop_node(i, SIGKILL);
    int st = 0, up = start_node(i, spec, &st);
    if (up == 1) {
        if (WIFEXITED(st) && WEXITSTATUS(st) == CRASH_EXIT_STATUS) {
            cp_hits++;
            effect_seen = 1;
        } else
            die(1, "node %d ended with status 0x%x at its start with %s armed, not at the point (log %s)", i, st, spec,
                nodes[i].log);
    } else {
        int win = !strcmp(mutant, "unfired") ? 4000 : cps[pick].window_ms;
        st = await_exit(i, win);
        if (st == -1) {
            cp_unfired++;
            effect_seen = 1; /* the node was killed and restarted; the unfired point fails the run at the end */
            snprintf(unfired_msg, sizeof unfired_msg, "the armed crash point %s never fired on node %d within %d ms", spec, i,
                     win);
            stop_node(i, SIGKILL);
        } else if (WIFEXITED(st) && WEXITSTATUS(st) == CRASH_EXIT_STATUS) {
            cp_hits++;
            cp_hits_serving++;
            effect_seen = 1;
        } else
            die(1, "node %d ended with status 0x%x with %s armed, not at the point (log %s)", i, st, spec, nodes[i].log);
    }
    hold_ms(rnd_range(200, 800));
    restart_node(i);
}

/* The terminal crash point (kv, classes, midcommit): every node runs with TSDB_CRASH_AT=<point>:1@<trigger> until then;
 * past the middle of the run the trigger file is made. A leader point (cps[].leader 1): kvr's leader must end there
 * within the window. A failover point (2): kvr's leader is then cut from every other node, which makes the group move
 * the volume (the replicas are fenced and one is promoted), and one of kvr's nodes must end there: a replica that saved
 * the fence, the promoted one, or the old leader, deposed once the cut heals (after 10 s, or as soon as a node ended
 * there). Several nodes may end at the point at once (both replicas save a fence): every one that ends within a second
 * of the first is counted, and any other way to end is a failure. The trigger goes before anything is restarted, so
 * the node that leads next (and was armed at its first start) does not end at the same point; then the cut heals and
 * the nodes come back, and the run goes on with the volume led wherever the group put it. */
static void nem_terminal(void)
{
    if (cps[term_cp].r4kind) {
        nem_terminal_r4();
        return;
    }
    int L = atomic_load(&kv_leader), fo = cps[term_cp].leader == 2, win = cps[term_cp].window_ms;
    /* a leader promoted shortly before decides no switch until the members' log start passes its attach, up to
     * CLASSES_RETAIN_S later (CLS_DEFER; r3e-fix16: 2 of 13 runs fired 28 and 34 s after the trigger) */
    if (want_classes && !strcmp(cps[term_cp].name, "class-switch"))
        win += cls_defer_s * 1000;
    if (fo) {
        /* a failover needs another ISR member to promote: a leader that is the ISR alone (the others not yet back
         * after the faults before) is only cut, and the point never fires (seed 12103 epoll) */
        int64_t tw = now_ms();
        uint64_t ver;
        int m;
        while ((m = rep_isr_mask(&ver)) != rep_full_mask() && now_ms() - tw < 60000)
            usleep(200000);
        if (m != rep_full_mask())
            fprintf(stderr, "jepsen-cluster: the ISR of kvr is not whole (mask %d) 60 s before the failover point %s\n", m,
                    cps[term_cp].name);
        ev("ISR of kvr mask %d before the failover point (%lld ms)", m, (long long)(now_ms() - tw));
        L = atomic_load(&kv_leader);
    }
    int fd = open(term_trigger, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0)
        die(2, "create %s: %s", term_trigger, strerror(errno));
    close(fd);
    if (fo) {
        for (int j = 0; j < nn; j++)
            if (j != L)
                net_cut(L, j, 1);
        net_cut_done();
    }
    int ended[MAXN] = {0}, nend = 0, healed = !fo;
    int64_t t0 = now_ms(), first = -1;
    for (;;) {
        for (int r = 0; r < rep_n; r++) {
            int i = rep_nodes[r];
            if (!fo && (cps[term_cp].leader == 3 ? i == L : i != L)) /* 3: a replica applying a COLD (cluster_ec.h) */
                continue;
            pid_t pid;
            node_snap(i, &pid, NULL, NULL);
            if (pid <= 0 || ended[i])
                continue;
            int st = await_exit(i, 0);
            if (st == -1)
                continue;
            if (!(WIFEXITED(st) && WEXITSTATUS(st) == CRASH_EXIT_STATUS))
                die(1, "node %d (%s of kvr) ended with status 0x%x with %s triggered, not at the point (log %s)", i,
                    i == L ? "the leader" : "a replica", st, cps[term_cp].name, nodes[i].log);
            ended[i] = 1;
            nend++;
            if (first < 0) {
                first = now_ms();
                unlink(term_trigger); /* before the next leader passes the point */
            }
        }
        if (fo && !healed && (first >= 0 || now_ms() - t0 >= 10000)) {
            net_heal();
            healed = 1;
        }
        if ((first >= 0 && now_ms() - first >= 1000) || now_ms() - t0 >= win)
            break;
        usleep(20000);
    }
    if (!healed)
        net_heal();
    if (first < 0) {
        unlink(term_trigger);
        cp_unfired++;
        snprintf(unfired_msg, sizeof unfired_msg, "the armed crash point %s never fired on %s within %d ms of its "
                 "trigger", cps[term_cp].name, fo ? "any node of kvr (its leader cut from the others)" :
                 "kvr's leader", win);
        if (!fo)
            stop_node(L, SIGKILL);
    } else {
        cp_hits += (unsigned)nend;
        cp_hits_serving += (unsigned)nend;
        term_fired = 1;
        printf("jepsen-cluster: terminal crash point %s: %d node(s) ended there, %s%s\n", cps[term_cp].name, nend,
               ended[L] ? "the leader" : "not the leader", fo ? " (failover)" : "");
    }
    effect_seen = 1;
    term_done = 1;
    hold_ms(rnd_range(200, 800));
    for (int i = 0; i < nn; i++)
        if (ended[i] || (first < 0 && !fo && i == L))
            restart_node(i);
}

/* The end checks of R4 (kvr, before the nodes are stopped): the replicas are those of the final record, none of them is
 * a learner, and nothing a snapshot, a swap or a removed replica leaves behind is still on any node: v<N>.snap,
 * v<N>.old, v<N>.removed-<rid> (its background delete ran; v<N>.lost-<rid> is never deleted by design) and snap_* (the
 * source's makes), in a node's directory or in v<N>. Returns the failures. */
static int rep_final_check(void)
{
    struct rrec r;
    int bad = 0;
    if (rep_rec_get(&r) != 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: no node answered with kvr's record at the end\n");
        return 1;
    }
    for (int k = 0; k < r.nrep; k++)
        if (r.learner[k]) {
            fprintf(stderr, "jepsen-cluster: FAIL: the replica on node %d is still a learner of kvr at the end\n", r.node[k]);
            bad++;
        }
    if (r.nrep != rep_n) {
        fprintf(stderr, "jepsen-cluster: FAIL: kvr has %d replicas at the end, not %d\n", r.nrep, rep_n);
        bad++;
    }
    if (!bad && r4_dynamic)
        rep_sync_members(); /* the replicas to compare: the final record's */
    return bad;
}

static int r4_leftover_name(const char *nm)
{
    if (!strncmp(nm, "snap_", 5))
        return 1;
    if (nm[0] != 'v' || nm[1] < '0' || nm[1] > '9')
        return 0;
    const char *p = nm + 1;
    while (*p >= '0' && *p <= '9')
        p++;
    return !strcmp(p, ".snap") || !strcmp(p, ".old") || !strncmp(p, ".removed-", 9);
}

/* "v<digits>" exactly: a volume's directory */
static int r4_is_vdir(const char *nm)
{
    if (nm[0] != 'v' || nm[1] < '0' || nm[1] > '9')
        return 0;
    const char *p = nm + 1;
    while (*p >= '0' && *p <= '9')
        p++;
    return !*p;
}

/* leftovers in one directory; 1 + the number of the v<N> directories inside it, which the caller scans in turn */
static int r4_scan_dir(const char *where, int node, int say, int depth)
{
    DIR *d = opendir(where);
    if (!d)
        return 0;
    int bad = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (r4_leftover_name(e->d_name)) {
            if (say)
                fprintf(stderr, "jepsen-cluster: FAIL: %s/%s is left on node %d\n", where, e->d_name, node);
            bad++;
        } else if (depth == 0 && r4_is_vdir(e->d_name)) { /* every replicated volume, not kvr's only */
            char sub[PATH_MAX + 300];
            snprintf(sub, sizeof sub, "%s/%s", where, e->d_name);
            bad += r4_scan_dir(sub, node, say, 1);
        }
    }
    closedir(d);
    return bad;
}

static int r4_leftovers_once(int say)
{
    int bad = 0;
    for (int i = 0; i < nn; i++)
        bad += r4_scan_dir(nodes[i].dir, i, say, 0);
    return bad;
}

/* The move's own check under churn (design-cluster.md 3.2): every node's <state>/voltrace (TSDB_CLUSTER_TEST vol_trace=1,
 * a line per applied record that changed kvr's set, epoch, leader or ISR) is followed in index order. While a move from
 * A to B is in flight (B in the set with `replaces` A), A may leave the ISR only after B joined it, and the ISR it leaves
 * behind has at least as many members as the ISR had in the last record before B joined: a move never lowers the number
 * of in-sync copies. Nothing is lost when A goes early with no fault around (moveshrinkfirst under churn alone acknowledged
 * every write and compared equal), so the record is what shows it. A node's trace may skip records (a snapshot it
 * installed): a jump across B's join counts as joined first, so a gap can hide a violation, never invent one. Only the
 * move's own shrink is judged: a record whose basis (epoch, ISR version) kvr's leader asked for as a leave of A (the
 * "req" lines its driver traces with the core's reason, repld.c trace_isr_req); A going for lag (drop) is counted, not
 * judged (review F3: a slow A under load is no move's decision). Judged only when churn is the run's one nemesis. */
struct vtr {
    uint64_t index, epoch, isrv;
    uint32_t nisr, isr[META_REPLICAS_MAX + 2], nrep, rep[META_REPLICAS_MAX + 2], repl[META_REPLICAS_MAX + 2];
};

static int vtr_has(const uint32_t *a, uint32_t n, uint32_t x)
{
    for (uint32_t k = 0; k < n; k++)
        if (a[k] == x)
            return 1;
    return 0;
}

static int vtr_list(const char *p, uint32_t *out, uint32_t *rpl, uint32_t max)
{
    uint32_t n = 0;
    while (*p && *p != ' ' && *p != '\n' && n < max) {
        char *e;
        unsigned long v = strtoul(p, &e, 10);
        if (e == p)
            break;
        out[n] = (uint32_t)v;
        if (rpl)
            rpl[n] = 0;
        p = e;
        if (*p == ':' && p[1] == 'L')
            p += 2;
        if (*p == '<') {
            v = strtoul(p + 1, &e, 10);
            if (rpl)
                rpl[n] = (uint32_t)v;
            p = e;
        }
        n++;
        if (*p == ',')
            p++;
    }
    return (int)n;
}

static int vtr_parse(const char *ln, struct vtr *t)
{
    unsigned long long ix;
    unsigned v;
    if (sscanf(ln, "%llu v%u ", &ix, &v) != 2 || v != rep_number)
        return 0;
    const char *i = strstr(ln, " isr "), *r = strstr(ln, " rep "), *e = strstr(ln, " epoch "),
               *iv = strstr(ln, " isrv ");
    if (!i || !r || !e || !iv)
        return 0;
    t->index = ix;
    t->epoch = strtoull(e + 7, NULL, 10);
    t->isrv = strtoull(iv + 6, NULL, 10);
    t->nisr = (uint32_t)vtr_list(i + 5, t->isr, NULL, META_REPLICAS_MAX + 2);
    t->nrep = (uint32_t)vtr_list(r + 5, t->rep, t->repl, META_REPLICAS_MAX + 2);
    return 1;
}

static unsigned mvtr_moves, mvtr_leaves, mvtr_lag;

/* kvr's leader asked for an ISR without `a` as a move's leave at the basis (epoch, isrv): a "req" line of any node */
static int vtr_leave_asked(uint32_t a, uint64_t epoch, uint64_t isrv)
{
    int hit = 0;
    for (int i = 0; i < nn && !hit; i++) {
        char p[PATH_MAX + 48], ln[1100];
        snprintf(p, sizeof p, "%s/state/voltrace", nodes[i].dir);
        FILE *f = fopen(p, "r");
        while (f && !hit && fgets(ln, sizeof ln, f)) {
            unsigned long long e, iv;
            unsigned v;
            char why[16];
            int off = 0;
            if (sscanf(ln, "req v%u epoch %llu isrv %llu why %15s isr %n", &v, &e, &iv, why, &off) != 4 || !off ||
                v != rep_number || e != epoch || iv != isrv || strcmp(why, "leave"))
                continue;
            uint32_t isr[META_REPLICAS_MAX + 2];
            int n = vtr_list(ln + off, isr, NULL, META_REPLICAS_MAX + 2);
            hit = !vtr_has(isr, (uint32_t)n, a);
        }
        if (f)
            fclose(f);
    }
    return hit;
}

static int r4_move_trace_check(void)
{
    if (nemeses != N_CHURN) {
        printf("r4: move trace not judged (nemeses besides churn: a fault takes a replica out of the ISR)\n");
        return 0;
    }
    int bad = 0;
    for (int i = 0; i < nn; i++) {
        char p[PATH_MAX + 48];
        snprintf(p, sizeof p, "%s/state/voltrace", nodes[i].dir);
        FILE *f = fopen(p, "r");
        if (!f)
            continue;
        struct {
            uint32_t a, b, min;
            int joined, live;
        } mv[8];
        int nmv = 0, have_prev = 0;
        unsigned moves = 0, leaves = 0, lag = 0;
        struct vtr t, prev;
        char ln[1100];
        while (fgets(ln, sizeof ln, f)) {
            if (!vtr_parse(ln, &t) || (have_prev && t.index <= prev.index))
                continue;
            for (int m = 0; m < nmv; m++)
                mv[m].live = 0;
            for (uint32_t k = 0; k < t.nrep; k++) {
                if (!t.repl[k])
                    continue;
                int m = 0;
                while (m < nmv && !(mv[m].a == t.repl[k] && mv[m].b == t.rep[k]))
                    m++;
                if (m == nmv) {
                    if (nmv == 8)
                        continue;
                    memset(&mv[nmv], 0, sizeof mv[0]);
                    mv[nmv].a = t.repl[k], mv[nmv].b = t.rep[k];
                    nmv++;
                    moves++;
                }
                mv[m].live = 1;
                if (vtr_has(t.isr, t.nisr, mv[m].b))
                    mv[m].joined = 1;
                else if (!mv[m].joined)
                    mv[m].min = t.nisr;
            }
            for (int m = 0; m < nmv; m++) {
                if (!mv[m].live || !have_prev || !vtr_has(prev.isr, prev.nisr, mv[m].a) || vtr_has(t.isr, t.nisr, mv[m].a))
                    continue;
                if (!vtr_leave_asked(mv[m].a, prev.epoch, prev.isrv)) {
                    lag++; /* A went for lag: not the move's shrink */
                    continue;
                }
                leaves++;
                if (!mv[m].joined) {
                    fprintf(stderr, "jepsen-cluster: node %d: 1 violation(s): kvr's move from member %" PRIu32 " to %"
                            PRIu32 ": %" PRIu32 " left the ISR at entry %" PRIu64 " before %" PRIu32 " joined it (ISR of %"
                            PRIu32 ")\n", i, mv[m].a, mv[m].b, mv[m].a, t.index, mv[m].b, t.nisr);
                    bad++;
                } else if (t.nisr < mv[m].min) {
                    fprintf(stderr, "jepsen-cluster: node %d: 1 violation(s): kvr's move from member %" PRIu32 " to %"
                            PRIu32 ": %" PRIu32 " left the ISR at entry %" PRIu64 " with %" PRIu32 " members left, fewer "
                            "than the %" PRIu32 " before %" PRIu32 " joined\n", i, mv[m].a, mv[m].b, mv[m].a, t.index,
                            t.nisr, mv[m].min, mv[m].b);
                    bad++;
                }
            }
            for (int m = 0; m < nmv; ) /* a move that left the set ended (done or cancelled) */
                if (!mv[m].live)
                    mv[m] = mv[--nmv];
                else
                    m++;
            prev = t;
            have_prev = 1;
        }
        fclose(f);
        if (moves > mvtr_moves)
            mvtr_moves = moves, mvtr_leaves = leaves, mvtr_lag = lag;
    }
    printf("r4: move trace: %u moves followed, %u shrinks of the moved-from member judged, %u for lag not judged, %d "
           "violation(s)\n", mvtr_moves, mvtr_leaves, mvtr_lag, bad);
    return bad;
}

static int r4_leftovers_check(void)
{
    char pd[PATH_MAX + 96];
    if (!strcmp(mutant, "plant-snap-left")) { /* planted: a snapshot directory nobody removed */
        snprintf(pd, sizeof pd, "%s/v%" PRIu32 ".snap", nodes[0].dir, rep_number);
        mkdir(pd, 0755);
    }
    if (!strcmp(mutant, "plant-removed-left")) { /* planted: a removed replica's directory nobody deleted */
        snprintf(pd, sizeof pd, "%s/v%" PRIu32 ".removed-77", nodes[1].dir, rep_number);
        mkdir(pd, 0755);
    }
    int64_t t0 = now_ms();
    while (r4_leftovers_once(0) && now_ms() - t0 < 30000) /* the background delete and the sweeps take their time */
        usleep(500000);
    return r4_leftovers_once(1);
}

#include "cluster_ec.h"

/* ---- RDMA metrics probe and rdma-qperr nemesis (design-rdma.md §8, rd-hw) */

struct rdma_mstat {
    uint64_t up_rdma, up_tcp, qp_errors, credit_stalls, rnr_retries;
    uint64_t fb_qperr, fb_backoff, fb_pool, fb_other;
};
static uint64_t rdma_max_up_links;
static unsigned rdma_qperr_fired, rdma_backoff_rounds, rdma_pool_leaks;

static int rdma_tracking(void)
{
    return (opt_peer_rdma && strcmp(opt_peer_rdma, "off") != 0) || (nemeses & N_RDMA_QPERR) != 0;
}

static int rdma_poll_node(pid_t pid, int port, struct rdma_mstat *out)
{
    memset(out, 0, sizeof *out);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct timeval tv = {1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    static const char rq[] = "GET /metrics HTTP/1.0\r\n\r\n";
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0 || !th_owns_port(pid, port)) {
        close(fd);
        return -1;
    }
    for (size_t off = 0; off < sizeof rq - 1;) {
        ssize_t w = write(fd, rq + off, sizeof rq - 1 - off);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            close(fd);
            return -1;
        }
        off += (size_t)w;
    }
    char chunk[4096], line[512];
    size_t ln = 0;
    int skipping = 0, got = 0, eof = 0;
    for (;;) {
        ssize_t r = read(fd, chunk, sizeof chunk);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (r == 0) {
            eof = 1;
            break;
        }
        got = 1;
        for (ssize_t k = 0; k < r; k++) {
            if (chunk[k] != '\n') {
                if (ln + 1 < sizeof line)
                    line[ln++] = chunk[k];
                else
                    skipping = 1;
                continue;
            }
            line[ln] = 0;
            if (!skipping) {
                const char *sp = strrchr(line, ' ');
                uint64_t v = sp ? strtoull(sp + 1, NULL, 10) : 0;
                if (!strncmp(line, "tsdb_peer_links_transport{", 26)) {
                    if (strstr(line, "transport=\"rdma\""))
                        out->up_rdma += v;
                    else if (strstr(line, "transport=\"tcp\""))
                        out->up_tcp += v;
                } else if (!strncmp(line, "tsdb_peer_rdma_qp_errors_total ", 31)) {
                    out->qp_errors += v;
                } else if (!strncmp(line, "tsdb_peer_rdma_credit_stalls_total ", 35)) {
                    out->credit_stalls += v;
                } else if (!strncmp(line, "tsdb_peer_rdma_rnr_retries_total ", 33)) {
                    out->rnr_retries += v;
                } else if (!strncmp(line, "tsdb_peer_rdma_fallbacks_total{", 31)) {
                    if (strstr(line, "reason=\"qp_error\""))
                        out->fb_qperr += v;
                    else if (strstr(line, "reason=\"backoff\""))
                        out->fb_backoff += v;
                    else if (strstr(line, "reason=\"pool\""))
                        out->fb_pool += v;
                    else
                        out->fb_other += v;
                }
            }
            ln = 0;
            skipping = 0;
        }
    }
    close(fd);
    return (got && eof) ? 0 : -1;
}

static void rdma_sample_all(struct rdma_mstat *sum)
{
    memset(sum, 0, sizeof *sum);
    for (int i = 0; i < nn; i++) {
        pthread_mutex_lock(&tbl_mu);
        pid_t pid = nodes[i].pid;
        int mport = nodes[i].mport;
        int paused = nodes[i].paused;
        pthread_mutex_unlock(&tbl_mu);
        if (pid <= 0 || paused || mport <= 0)
            continue;
        struct rdma_mstat m;
        if (rdma_poll_node(pid, mport, &m) == 0) {
            sum->up_rdma += m.up_rdma;
            sum->up_tcp += m.up_tcp;
            sum->qp_errors += m.qp_errors;
            sum->credit_stalls += m.credit_stalls;
            sum->rnr_retries += m.rnr_retries;
            sum->fb_qperr += m.fb_qperr;
            sum->fb_backoff += m.fb_backoff;
            sum->fb_pool += m.fb_pool;
            sum->fb_other += m.fb_other;
        }
    }
    if (sum->up_rdma > rdma_max_up_links)
        rdma_max_up_links = sum->up_rdma;
}

static void nem_rdma_qperr(void)
{
    struct rdma_mstat st;
    rdma_sample_all(&st);
    /* Under --peer-rdma auto, if every RDMA link is already in TCP backoff, record the backoff round immediately. */
    if (st.up_rdma == 0 && rdma_qperr_fired >= 3 && st.fb_backoff > 0) {
        rdma_backoff_rounds++;
        effect_seen = 1;
        hold_ms(rnd_range(800, 1500));
        return;
    }
    /* Probe kvr's active replica nodes first (rotated), then any remaining nodes, with a hard total time cap well
     * below per_round (10 s) so backoff rounds on larger clusters never trip the idle-nemesis floor. */
    int order[MAXN], norder = 0;
    int seen[MAXN] = {0};
    int start = pick_node(1);
    if (start >= 0 && start < nn) {
        order[norder++] = start;
        seen[start] = 1;
    }
    if (want_rep && rep_n > 0) {
        int r0 = (int)(rnd() % (uint64_t)rep_n);
        for (int k = 0; k < rep_n; k++) {
            int idx = rep_nodes[(r0 + k) % rep_n];
            if (idx >= 0 && idx < nn && !seen[idx]) {
                order[norder++] = idx;
                seen[idx] = 1;
            }
        }
    }
    for (int off = 0; off < nn; off++) {
        int i = (start + off) % nn;
        if (!seen[i]) {
            order[norder++] = i;
            seen[i] = 1;
        }
    }
    int in_backoff = rdma_qperr_fired >= 3 && st.fb_backoff > 0;
    int64_t total_ms = in_backoff ? 1500 : 3500;
    int64_t per_node_ms = in_backoff ? 500 : 1000;
    int64_t deadline = now_ms() + total_ms;
    for (int p = 0; p < norder; p++) {
        int64_t rem = deadline - now_ms();
        if (rem <= 0)
            break;
        int i = order[p];
        pthread_mutex_lock(&tbl_mu);
        pid_t pid = nodes[i].pid;
        int paused = nodes[i].paused;
        pthread_mutex_unlock(&tbl_mu);
        if (pid <= 0 || paused)
            continue;
        char trig[PATH_MAX + 64];
        snprintf(trig, sizeof trig, "%s/rdma_qperr", nodes[i].dir);
        int fd = open(trig, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0)
            continue;
        close(fd);
        int64_t wait_ms = rem < per_node_ms ? rem : per_node_ms;
        int64_t t0 = now_ms();
        while (access(trig, F_OK) == 0 && now_ms() - t0 < wait_ms) {
            check_unexpected_exits();
            usleep(25000);
        }
        if (access(trig, F_OK) != 0 || (unlink(trig) != 0 && errno == ENOENT)) {
            rdma_qperr_fired++;
            effect_seen = 1;
            ev("node %d: rdma-qperr fired (%u total)", i, rdma_qperr_fired);
            hold_ms(rnd_range(300, 1200));
            return;
        }
    }
    /* Under --peer-rdma auto, after 3 QP errors within 60 s links enter TCP backoff as designed: verify backoff. */
    rdma_sample_all(&st);
    if (rdma_qperr_fired >= 3 && st.fb_backoff > 0) {
        rdma_backoff_rounds++;
        effect_seen = 1;
        hold_ms(rnd_range(800, 1500));
    }
}

static unsigned rdma_check_logs_for_leaks(void)
{
    unsigned leaks = 0;
    for (int i = 0; i < nn; i++) {
        FILE *f = fopen(nodes[i].log, "r");
        if (!f)
            continue;
        char ln[512];
        while (fgets(ln, sizeof ln, f))
            if (strstr(ln, "stay out of the pool") || strstr(ln, "the pool closed with ")) {
                fprintf(stderr, "jepsen-cluster: FAIL: node %d RDMA pool leak/drain timeout: %s", i, ln);
                leaks++;
            }
        fclose(f);
    }
    return leaks;
}

static unsigned leader_cuts, leader_replaced, delays_applied;

static void nem_partition(void)
{
    int l = atomic_load(&last_leader);
    if (l < 0 || l >= nn)
        l = rnd_range(0, nn - 1);
    /* the planted no-op needs traffic that a cut stops: the leader's links carry it every heartbeat */
    int shape = !strcmp(mutant, "noop-partition") ? rnd_range(1, 2) : !strcmp(mutant, "nodelay") ? 5 : rnd_range(0, 5);
    uint32_t n_obs0 = atomic_load(&nobs); /* shape 1: the cut leader must be replaced by a leader of the others */
    ju_fault(-1);
    uint32_t l_id = atomic_load(&nodes[l].id);
    int perm[MAXN];
    for (int i = 0; i < nn; i++)
        perm[i] = i;
    for (int i = nn - 1; i > 0; i--) {
        int j = (int)(rnd() % (uint64_t)(i + 1)), t = perm[i];
        perm[i] = perm[j];
        perm[j] = t;
    }
    switch (shape) {
    case 0: { /* random halves: a minority of 1..nn/2 nodes cut from the rest */
        int m = rnd_range(1, nn / 2);
        for (int a = 0; a < m; a++)
            for (int b = m; b < nn; b++)
                net_cut(perm[a], perm[b], 1);
        break;
    }
    case 1: /* the leader cut from every other node (a leader and its replicas) */
        leader_cuts++;
        for (int j = 0; j < nn; j++)
            if (j != l)
                net_cut(l, j, 1);
        break;
    case 2: { /* a node cut from the Raft leader only (the volume leader of r3d: for now any node) */
        int x = perm[0] == l ? perm[1] : perm[0];
        net_cut(l, x, 1);
        break;
    }
    case 3: { /* a bridge: two sides that reach each other only through one node */
        int br = perm[0], half = rnd_range(1, (nn - 1) / 2);
        for (int a = 1; a <= half; a++)
            for (int b = half + 1; b < nn; b++)
                net_cut(perm[a], perm[b], 1);
        (void)br;
        break;
    }
    case 4: /* one direction only */
        net_cut(perm[0], perm[1], 0);
        break;
    default: /* delays, no cut: whole byte runs in order, a bit over the heartbeat */
        for (int k = 0; k < 2; k++) {
            int a = perm[2 * k], b = perm[2 * k + 1];
            uint32_t d = (uint32_t)rnd_range(200, 1200);
            if (!strcmp(mutant, "nodelay")) /* planted: the delay is never set */
                continue;
            atomic_store(&delay_ms[a][b], d);
            atomic_store(&delay_ms[b][a], d);
            effect_seen = 1;
        }
        delays_applied++;
        break;
    }
    if (shape != 5) {
        net_cut_done();
        effect_seen = 1;
    }
    hold_ms(rnd_range(1500, 4500));
    if (shape == 1) {
        uint32_t n = atomic_load(&nobs);
        for (uint32_t k = n_obs0; k < n; k++)
            if (obs[k].self != l_id) {
                leader_replaced++;
                break;
            }
    }
    net_heal();
}

/* ---- --mutant takeover[-checked]: a node's old client port goes to a foreign server, the table of the workload's
 * clients is stale */
static pid_t foreign_pid;
static int takeover_done;

static void takeover_event(void)
{
    int old;
    node_snap(0, NULL, &old, NULL);
    stop_node(0, SIGKILL);
    char dir[PATH_MAX + 16], pf[PATH_MAX + 128], flag[64], port[16];
    snprintf(dir, sizeof dir, "%s/foreign", rundir);
    mkdir(dir, 0755);
    th_portfile(pf, sizeof pf, dir);
    th_follow_me(flag, sizeof flag);
    snprintf(port, sizeof port, "%d", old);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    char *argv[] = {"tsdbd", "--no-auth", flag, "--dir", dir, "--port", port, "--port-file", pf, NULL};
    pid_t pid = fork();
    if (pid < 0)
        die(2, "fork: %s", strerror(errno));
    if (pid == 0) {
        int fd = open("/dev/null", O_WRONLY);
        if (fd >= 0)
            dup2(fd, 2);
        execv(bin, argv);
        _exit(127);
    }
    int got = th_await(pf, pid, 15000);
    if (got != old) {
        fprintf(stderr, "jepsen-cluster: the takeover selftest could not take port %d: %s\n", old,
                got > 0 ? "another port" : th_why);
        die(2, "selftest setup failed");
    }
    foreign_pid = pid;
    /* the foreign server holds a series of node 0's name (in its root, as any plain tsdbd would), so a client that
     * does not check the port it reached writes there and is answered */
    tsdbc *fc = NULL;
    tsdb_sid fsid;
    if (tsdbc_connect("127.0.0.1", old, &fc) != 0 || tsdbc_series(fc, "kvl.n0", 1, &fsid) != 0)
        die(2, "selftest setup failed: the foreign server does not take series kvl.n0");
    tsdbc_close(fc);
    pthread_mutex_lock(&tbl_mu);
    nodes[0].cport_stale = old; /* the planted stale table: the workload's clients keep this port */
    pthread_mutex_unlock(&tbl_mu);
    restart_node(0);
    takeover_done = 1;
}

/* ------------------------------------------------------------------ the run */

static int cluster_form(void)
{
    meta_settings_default(&S);
    S.node_timeout_ms = 1500;
    S.heartbeat_ms = 200;
    S.lease_s = 4;
    /* classes with R4_REPLAY_ON_MAIN 0 (before r4-replay): a promoted writer decides no class switch while a member's
     * log may still replay below its attach (CLS_DEFER, design-cluster.md 8.3: the replay floor was the members' log
     * start, which the keep caps hold up to repl_retain_s back); with the default 30 min every failover ended the
     * switches for the rest of the run. 30 s
     * lets the floor pass the attach about 31 s after a promotion (flushes every 500 ms); under kill and power a new
     * epoch comes every 5 to 14 s, so those runs switch in the quiet tail after the last heal (classes_tail, up to
     * CLASSES_TAIL_MS). Shorter caps strand a member that fell further behind than them: it needs a snapshot, which
     * is not built yet (r3e-fix14: 3 s left a learner behind in 4 of 13 runs, a partition-only run among them;
     * r3e-fix15: 10 s in 2 of 13, a learner killed again and again while it caught up). */
    if (retain_eff > 0) /* --retain-s, longdown's 3 s (classes' CLASSES_RETAIN_S only with R4_REPLAY_ON_MAIN 0) */
        S.repl_retain_s = (uint32_t)retain_eff;
    /* Any S.* override added here must be mirrored in ju_init_cluster() (cluster_users.h), which runs
     * `tsdb cluster init --superuser` instead of CLUSTER_OP_INIT on odd-seed --auth runs. */
    uint8_t req[4 * (META_SETTINGS_WORDS + 1)]; /* 64 overflowed since the settings grew past 15 words (ASan) */
    size_t rl = cluster_settings_encode(&S, META_MODE_REPLICATED, req);
    int rc = (want_auth && (seed & 1)) ? ju_init_cluster(0) : admin_real(0, CLUSTER_OP_INIT, req, rl, 0, 10000);
    if (rc != 0)
        die(1, "init: %s", cluster_strerror(rc));
    int64_t t0 = now_ms();
    for (;;) {
        struct view v;
        if (view_get(0, &v, 2000) == 0) {
            int lead = v.st.role == RAFT_LEADER_ROLE;
            atomic_store(&nodes[0].id, v.st.self);
            view_free(&v);
            if (lead)
                break;
        }
        if (now_ms() - t0 > 30000)
            die(1, "node 0 does not lead the cluster it initialised");
        usleep(100000);
    }
    if (want_auth)
        ju_make_superuser(0); /* before the others join, as `tsdb cluster init --superuser` */
    for (int i = 1; i < nn; i++) {
        char padv[META_ADDR_MAX];
        snprintf(padv, sizeof padv, "127.0.0.1:%d", nodes[i].proxy_port);
        uint8_t nr[META_ADDR_MAX + 2];
        size_t n = strlen(padv);
        nr[0] = (uint8_t)n;
        nr[1] = (uint8_t)(n >> 8);
        memcpy(nr + 2, padv, n);
        rc = admin_real(0, CLUSTER_OP_NODE_ADD, nr, n + 2, 1, 15000);
        if (rc != 0)
            die(1, "node add of node %d: %s", i, cluster_strerror(rc));
        t0 = now_ms();
        while (!atomic_load(&nodes[i].id)) {
            struct view v;
            if (view_get(i, &v, 2000) == 0) {
                if (v.st.self && v.st.mode != CLUSTER_MODE_NONE)
                    atomic_store(&nodes[i].id, v.st.self);
                view_free(&v);
            }
            if (now_ms() - t0 > 30000)
                die(1, "node %d got no member id", i);
            usleep(100000);
        }
    }
    atomic_store(&table_ready, 1);
    return 0;
}

static int rm_cb(const char *p, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(p);
}
static void rmtree(const char *path) { nftw(path, rm_cb, 16, FTW_DEPTH | FTW_PHYS); }

/* --selftest-checker: checks 1-5 and the election check on histories made here (no cluster). A history that is
 * right passes; each plant (one thing wrong in it) must be found by the check that is meant for it, so a broken
 * comparison in a check fails `make jepsen-cluster-selftest` and no longer passes every gate. */
static struct hist *st_hist(void)
{
    struct hist *h = calloc(1, sizeof *h);
    if (!h)
        die(2, "out of memory");
    snprintf(h->name, sizeof h->name, "selftest");
    h->w = calloc(8, sizeof *h->w);
    h->nw = 3;
    /* write 1: keys 0 and 1 together, acknowledged; write 2: key 0, acknowledged; write 3: key 0, refused */
    h->w[0] = (struct wr){.nk = 2, .keys = {0, 1}, .st = WS_OK, .begin = 10, .end = 20};
    h->w[1] = (struct wr){.nk = 1, .keys = {0}, .st = WS_OK, .begin = 30, .end = 40};
    h->w[2] = (struct wr){.nk = 1, .keys = {0}, .st = WS_FAIL, .begin = 45, .end = 46};
    for (int s = 0; s < 2; s++) {
        h->r[s] = calloc(4, sizeof **h->r);
        h->rcap[s] = 4;
    }
    h->nr[0] = 2;
    h->r[0][0] = (struct rd){.begin = 50, .end = 60};
    h->r[0][0].seen[0] = 2;
    h->r[0][0].seen[1] = 1;
    h->r[0][1] = (struct rd){.begin = 70, .end = 80};
    h->r[0][1].seen[0] = 2;
    h->r[0][1].seen[1] = 1;
    h->final[0] = 2;
    h->final[1] = 1;
    h->have_final = 1;
    return h;
}
static void st_free(struct hist *h)
{
    free(h->w);
    for (int s = 0; s < 2; s++)
        free(h->r[s]);
    free(h);
}

static int selftest_checker(void)
{
    int bad = 0;
    struct plant {
        const char *what;
        int kind; /* the check that must fire, -1: none may */
    } pl[] = {
        {"a right history", -1},
        {"a write nobody made is seen", V_VALID},
        {"a refused write is seen", V_VALID},
        {"a write is seen at a key it did not write", V_VALID},
        {"a write seen before it began", V_VALID},
        {"the final state lacks an acknowledged write", V_DURABLE},
        {"the final state ends with an older write", V_DURABLE},
        {"half of a two-key write is seen", V_ATOMIC},
        {"an acknowledged write is not seen after its ack", V_STALE},
        {"an older write is seen after a newer one was acknowledged", V_STALE},
        {"a session reads back in time", V_MONO},
        {"a client's read after its own acknowledged write shows an older one", V_RYW},
        {"a client reads its own acknowledged write back", -1},
        {"a write held past the forward age at its entry node is acknowledged", V_OLD},
        {"a write that began late in a stall window is acknowledged", -1},
        {"a write held past the forward age is refused, as it must be", -1},
    };
    for (unsigned t = 0; t < sizeof pl / sizeof *pl; t++) {
        struct hist *h = st_hist();
        memset(viol_by, 0, sizeof viol_by);
        switch (t) {
        case 1: h->r[0][1].seen[0] = 77; break;
        case 2: h->r[0][1].seen[0] = 3; break;
        case 3: h->r[0][1].seen[5] = 1; break; /* write 1 wrote keys 0 and 1 only */
        case 4: h->r[0][0].seen[0] = 2; h->r[0][0].end = 25; break; /* write 2 began at 30 */
        case 5: h->final[0] = 0; break;
        case 6: h->final[0] = 1; break; /* write 1 is older than the acknowledged write 2 */
        case 7: h->r[0][1].seen[0] = 1; h->r[0][1].seen[1] = 0; break; /* write 1 at key 0, nothing at its key 1 */
        case 8: h->r[0][1].seen[0] = 0; h->r[0][1].seen[1] = 0; break;
        case 9: h->r[0][1].seen[0] = 1; break;
        case 10: /* nothing acknowledged after write 1: write 2 is indeterminate, seen once, then write 1 again */
            h->w[1].st = WS_INDET;
            h->w[1].end = INT64_MAX;
            h->r[0][1].seen[0] = 1;
            h->have_final = 0;
            break;
        case 11: /* write 2 (key 0) was acknowledged on the connection; the read right after shows write 1 */
            h->nr[1] = 1;
            h->r[1][0] = (struct rd){.begin = 41, .end = 42, .own = 2};
            h->r[1][0].seen[0] = 1;
            h->r[1][0].seen[1] = 1;
            h->w[1].end = 40;
            h->nr[0] = 0; /* only the session of the client is judged: the read is stale for check 4 as well, by design */
            break;
        case 12:
            h->nr[1] = 1;
            h->r[1][0] = (struct rd){.begin = 50, .end = 51, .own = 2};
            h->r[1][0].seen[0] = 2;
            h->r[1][0].seen[1] = 1;
            break;
        case 13: case 14: case 15:
            /* a stall window of node 0 from 1000 to 3000 ms (ns): write 2 began at 1100 ms */
            stalls[0] = (struct stall_win){.node = 0, .ts = 1000000000LL, .te = 3000000000LL};
            nstalls = 1;
            h->w[1].begin = t == 14 ? 2400000000LL : 1100000000LL;
            h->w[1].end = 3010000000LL;
            h->w[1].st = t == 15 ? WS_FAIL : WS_OK;
            if (t == 15)
                h->final[0] = 1; /* a refused write is never the final value */
            h->w[0].begin = 10;
            h->have_final = 0; /* the reads are of the unplanted history: only check 7 is under test */
            h->nr[0] = 0;
            break;
        }
        uint64_t ia = 0;
        int n = check_hist(h, &ia);
        int total = 0;
        for (int k = 0; k < V_KINDS; k++)
            total += viol_by[k];
        int ok = pl[t].kind < 0 ? n == 0 && total == 0 : n > 0 && viol_by[pl[t].kind] > 0;
        printf("selftest-checker: %-62s %s\n", pl[t].what, ok ? "ok" : "NOT FOUND");
        if (!ok)
            bad++;
        nstalls = 0;
        st_free(h);
    }
    /* election safety: two leaders in one term; one in each of two terms passes */
    struct obs e1[] = {{3, 1}, {3, 2}}, e2[] = {{3, 1}, {4, 2}, {4, 2}};
    struct obs *keep = obs;
    unsigned terms, changes;
    obs = e1;
    atomic_store(&nobs, 2);
    int two = check_election_safety(&terms, &changes);
    obs = e2;
    atomic_store(&nobs, 3);
    int fine = check_election_safety(&terms, &changes);
    obs = keep;
    atomic_store(&nobs, 0);
    printf("selftest-checker: %-62s %s\n", "two leaders in one term", two ? "ok" : "NOT FOUND");
    printf("selftest-checker: %-62s %s\n", "one leader in each of two terms passes", !fine ? "ok" : "FALSE ALARM");
    if (!two || fine)
        bad++;
    bad += ju_selftest();
    if (bad) {
        fprintf(stderr, "selftest-checker: %d check(s) did not do what they are for\n", bad);
        return 1;
    }
    printf("selftest-checker: ok\n");
    return 0;
}

static void usage(void)
{
    fprintf(stderr, "usage: jepsen_cluster --dir DIR [--nodes 5] [--seconds 30] [--seed N]\n"
                    "         [--nemesis kill,partition,pause,midcommit,power,fwdstall,kill2,slow,longdown,rebuild,churn,\n"
                    "                    partial,diskloss,rot (the last three with --ec),rdma-qperr] [--ec K+M]\n"
                    "         [--workload meta,kv-local|kv|kv-via|classes|users] [--auth] [--retain-s S]\n"
                    "         [--peer-rdma auto|on|off] [--peer-rdma-pool-mb N] [--peer-rdma-device DEV[:PORT]]\n"
                    "         [--crashfs build/libcrashfs.so] [--outagefs build/liboutagefs.so] [--mutant NAME]\n"
                    "         | --selftest-replicas | --selftest-checker | --built-nemeses\n");
    exit(2);
}

static int check_limits(int nodes_n, const char *dir)
{
    /* File descriptors: proxy holds full-mesh peer links (~4k sockets at 22 nodes) + listeners + client sockets; each
     * tsdbd inherits this limit (~650 FDs at 22 nodes). */
    rlim_t need_fds = (rlim_t)(nodes_n > 8 ? (nodes_n * nodes_n * 12 + 1024) : 1024);
    if (nodes_n > 8 && need_fds < 8192)
        need_fds = 8192;
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < need_fds) {
        rlim_t want = (rl.rlim_max == RLIM_INFINITY || need_fds <= rl.rlim_max) ? need_fds : rl.rlim_max;
        if (want > rl.rlim_cur) {
            struct rlimit nr = {want, rl.rlim_max};
            (void)setrlimit(RLIMIT_NOFILE, &nr);
            (void)getrlimit(RLIMIT_NOFILE, &rl);
        }
        if (rl.rlim_cur < need_fds) {
            fprintf(stderr, "jepsen-cluster: refused: open descriptor limit RLIMIT_NOFILE is %llu (hard %llu), "
                            "%d nodes need at least %llu (raise with ulimit -n)\n",
                    (unsigned long long)rl.rlim_cur, (unsigned long long)rl.rlim_max, nodes_n,
                    (unsigned long long)need_fds);
            return -1;
        }
    }
    /* Memory: check RLIMIT_AS and /proc/meminfo MemAvailable against a conservative per-node floor. */
    uint64_t need_mb = (uint64_t)nodes_n * 64;
    if (getrlimit(RLIMIT_AS, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY && rl.rlim_cur < (256ULL << 20)) {
        fprintf(stderr, "jepsen-cluster: refused: address space limit RLIMIT_AS is %llu MiB, need at least 256 MiB\n",
                (unsigned long long)(rl.rlim_cur >> 20));
        return -1;
    }
    FILE *mf = fopen("/proc/meminfo", "r");
    if (mf) {
        char ln[256];
        uint64_t avail_kb = 0, free_kb = 0;
        while (fgets(ln, sizeof ln, mf)) {
            unsigned long long v = 0;
            if (sscanf(ln, "MemAvailable: %llu kB", &v) == 1)
                avail_kb = (uint64_t)v;
            else if (sscanf(ln, "MemFree: %llu kB", &v) == 1)
                free_kb = (uint64_t)v;
        }
        fclose(mf);
        uint64_t mb = (avail_kb ? avail_kb : free_kb) / 1024;
        if (mb && mb < need_mb) {
            fprintf(stderr, "jepsen-cluster: refused: only %" PRIu64 " MiB RAM available, %d nodes need at least "
                            "%" PRIu64 " MiB\n", mb, nodes_n, need_mb);
            return -1;
        }
    }
    /* Scratch space: check free space under opt_dir (default /var/tmp). */
    struct statvfs sv;
    if (dir && statvfs(dir, &sv) == 0) {
        uint64_t avail_mb = ((uint64_t)sv.f_bavail * (uint64_t)sv.f_frsize) >> 20;
        uint64_t need_disk_mb = (uint64_t)nodes_n * 32;
        if (avail_mb < need_disk_mb) {
            fprintf(stderr, "jepsen-cluster: refused: only %" PRIu64 " MiB free on %s, %d nodes need at least "
                            "%" PRIu64 " MiB\n", avail_mb, dir, nodes_n, need_disk_mb);
            return -1;
        }
    }
    return 0;
}

int main(int argc, char **argv)
{
    int64_t wall_t0 = now_ms();
    signal(SIGPIPE, SIG_IGN);
    int self_replicas = 0, self_checker = 0;
    for (int a = 1; a < argc; a++) {
        const char *k = argv[a];
        if (!strcmp(k, "--selftest-replicas"))
            self_replicas = 1;
        else if (!strcmp(k, "--selftest-checker"))
            self_checker = 1;
        else if (!strcmp(k, "--auth"))
            want_auth = 1;
        else if (!strcmp(k, "--built-nemeses")) { /* R4's nemeses that can run now (the Makefile's loops take them from here) */
            for (int j = 0; j < NR4NEM; j++)
                if (r4nem[j].built || getenv("JEPSEN_R4_TRY"))
                    printf("%s%s", j ? " " : "", r4nem[j].name);
            printf("\n");
            return 0;
        }
        else if (a + 1 >= argc)
            usage();
        else if (!strcmp(k, "--dir"))
            opt_dir = argv[++a];
        else if (!strcmp(k, "--nodes"))
            nn = atoi(argv[++a]);
        else if (!strcmp(k, "--seconds"))
            seconds = atoi(argv[++a]);
        else if (!strcmp(k, "--seed"))
            seed = strtoull(argv[++a], NULL, 10);
        else if (!strcmp(k, "--crashfs"))
            crashfs_so = argv[++a];
        else if (!strcmp(k, "--outagefs"))
            outagefs_so = argv[++a];
        else if (!strcmp(k, "--retain-s"))
            opt_retain_s = atoi(argv[++a]);
        else if (!strcmp(k, "--mutant"))
            mutant = argv[++a];
        else if (!strcmp(k, "--peer-rdma")) {
            opt_peer_rdma = argv[++a];
            if (strcmp(opt_peer_rdma, "auto") && strcmp(opt_peer_rdma, "on") && strcmp(opt_peer_rdma, "off")) {
                fprintf(stderr, "jepsen-cluster: --peer-rdma %s: expected auto, on or off\n", opt_peer_rdma);
                return 2;
            }
        } else if (!strcmp(k, "--peer-rdma-pool-mb")) {
            opt_peer_rdma_pool_mb = atoi(argv[++a]);
            if (opt_peer_rdma_pool_mb < 1 || opt_peer_rdma_pool_mb > 16384) {
                fprintf(stderr, "jepsen-cluster: --peer-rdma-pool-mb must be 1 to 16384\n");
                return 2;
            }
        } else if (!strcmp(k, "--peer-rdma-device")) {
            opt_peer_rdma_dev = argv[++a];
        } else if (!strcmp(k, "--ec")) { /* K+M: kvr is an erasure-coded volume (cluster_ec.h) */
            if (a + 1 >= argc || sscanf(argv[++a], "%d+%d", &want_ec_k, &want_ec_m) != 2 || want_ec_k < 1 ||
                want_ec_k > META_EC_K_MAX || want_ec_m < 1 || want_ec_m > META_EC_M_MAX) {
                fprintf(stderr, "jepsen-cluster: --ec K+M: K 1 to %d and M 1 to %d (kvr has m + 1 log replicas)\n",
                        META_EC_K_MAX, META_EC_M_MAX);
                return 2;
            }
        }
        else if (!strcmp(k, "--nemesis") || !strcmp(k, "--workload")) {
            int nem = !strcmp(k, "--nemesis");
            char list[128];
            snprintf(list, sizeof list, "%s", argv[++a]);
            if (nem)
                nemeses = 0;
            else
                want_meta = want_kv = 0;
            for (char *t = strtok(list, ","); t; t = strtok(NULL, ",")) {
                if (nem && !strcmp(t, "kill"))
                    nemeses |= N_KILL;
                else if (nem && !strcmp(t, "partition"))
                    nemeses |= N_PARTITION;
                else if (nem && !strcmp(t, "pause"))
                    nemeses |= N_PAUSE;
                else if (nem && !strcmp(t, "midcommit"))
                    nemeses |= N_MIDCOMMIT;
                else if (nem && !strcmp(t, "power"))
                    nemeses |= N_POWER;
                else if (nem && !strcmp(t, "fwdstall"))
                    nemeses |= N_FWDSTALL;
                else if (nem && r4_bit(t))
                    nemeses |= r4_bit(t);
                else if (nem && !strcmp(t, "partial"))
                    nemeses |= N_PARTIAL;
                else if (nem && !strcmp(t, "diskloss"))
                    nemeses |= N_DISKLOSS;
                else if (nem && !strcmp(t, "rot"))
                    nemeses |= N_ROT;
                else if (nem && !strcmp(t, "rdma-qperr"))
                    nemeses |= N_RDMA_QPERR;
                else if (nem && !strcmp(t, "none"))
                    nemeses |= 0;
                else if (!nem && !strcmp(t, "meta"))
                    want_meta = 1;
                else if (!nem && !strcmp(t, "kv-local"))
                    want_kv = 1;
                else if (!nem && !strcmp(t, "kv"))
                    want_rep = 1;
                else if (!nem && !strcmp(t, "kv-via"))
                    want_rep = want_via = 1;
                else if (!nem && !strcmp(t, "classes"))
                    want_rep = want_classes = 1;
                else if (!nem && !strcmp(t, "users"))
                    want_users = want_auth = 1;
                else
                    usage();
            }
        } else
            usage();
    }
    if (nn < 3 || nn > MAXN || seconds < 1)
        usage();
    if ((nemeses & N_POWER) && !crashfs_so) {
        fprintf(stderr, "jepsen-cluster: the power nemesis needs --crashfs\n");
        return 2;
    }
    for (int k = 0; k < NCPS; k++) {
        int known = 0;
        for (int j = 0; j < CRASH_POINT_COUNT; j++)
            known |= !strcmp(cps[k].name, crash_point_names[j]);
        if (!known)
            die(2, "crash point %s is not in server/crashpt.h", cps[k].name);
    }
    if (want_kv && want_rep) {
        fprintf(stderr, "jepsen-cluster: kv-local (TSDB_CLUSTER_WRITES, no driver) and kv or classes (the driver) "
                        "cannot share a run\n");
        return 2;
    }
    if (want_ec_k) {
        rep_n = want_ec_m + 1;
        ec_detect();
        if (!strcmp(mutant, "ec-diverged")) /* the plant of the server: a replica turns DIVERGED (cluster_ec.h ec_plant) */
            setenv("TSDB_REPLD_TEST", "plantdiverge", 1);
        if (!want_rep || want_kv) {
            fprintf(stderr, "jepsen-cluster: --ec needs the workload kv, kv-via or classes (and not kv-local)\n");
            return 2;
        }
        if (nn < want_ec_k + want_ec_m + 1) {
            fprintf(stderr, "jepsen-cluster: --ec %d+%d needs --nodes of at least %d\n", want_ec_k, want_ec_m,
                    want_ec_k + want_ec_m + 1);
            return 2;
        }
        if ((nemeses & N_ROT) && !ec_settles) {
            fprintf(stderr, "jepsen-cluster: the rot nemesis needs a tsdbd built with TSDB_EC: there is no chunk to flip a byte in\n");
            return 2;
        }
        if ((nemeses & N_ROT) && (nemeses & N_DISKLOSS)) {
            fprintf(stderr, "jepsen-cluster: --nemesis rot and diskloss cannot be combined (each uses up to m fault positions)\n");
            return 2;
        }
    } else if (nemeses & N_EC) {
        fprintf(stderr, "jepsen-cluster: the partial, diskloss and rot nemeses need --ec K+M\n");
        return 2;
    }
    if (want_users && nn > 12) {
        fprintf(stderr, "jepsen-cluster: workload users supports at most 12 nodes (jm_clean_set is O(n^2 2^n)); got %d\n", nn);
        return 2;
    }
    if ((nemeses & N_FWDSTALL) && !want_via) {
        fprintf(stderr, "jepsen-cluster: the fwdstall nemesis needs the workload kv-via\n");
        return 2;
    }
    if ((nemeses & N_RDMA_QPERR) && opt_peer_rdma && !strcmp(opt_peer_rdma, "off")) {
        fprintf(stderr, "jepsen-cluster: the rdma-qperr nemesis cannot run with --peer-rdma off\n");
        return 2;
    }
    if ((nemeses & N_RDMA_QPERR) && !want_rep) {
        fprintf(stderr, "jepsen-cluster: the rdma-qperr nemesis needs a replicated workload (kv, kv-via or classes)\n");
        return 2;
    }
    for (int k = 0; k < NR4NEM; k++) {
        if (!(nemeses & r4nem[k].bit))
            continue;
        if (!r4nem[k].built && !getenv("JEPSEN_R4_TRY")) {
            fprintf(stderr, "jepsen-cluster: the %s nemesis needs %s, which is not on main yet (r4nem[].built in "
                            "tests/jepsen/jepsen_cluster.c; JEPSEN_R4_TRY=1 runs it anyway)\n", r4nem[k].name, r4nem[k].needs);
            return 2;
        }
        if ((r4nem[k].bit & N_R4_KV) && !want_rep) {
            fprintf(stderr, "jepsen-cluster: the %s nemesis works on kvr: it needs the workload kv, kv-via or classes\n",
                    r4nem[k].name);
            return 2;
        }
    }
    if ((nemeses & N_SLOW) && !outagefs_so) {
        fprintf(stderr, "jepsen-cluster: the slow nemesis needs --outagefs build/liboutagefs.so\n");
        return 2;
    }
    if (opt_retain_s == 0 || opt_retain_s > 100000) {
        fprintf(stderr, "jepsen-cluster: --retain-s is 1 to 100000\n");
        return 2;
    }
    use_crashfs = (nemeses & N_POWER) != 0;
    use_outagefs = (nemeses & N_SLOW) != 0;
    r4_dynamic = (nemeses & N_CHURN) != 0;
    /* repl_retain_s: --retain-s; longdown 3 s (a replica down for longer needs a snapshot); a classes run until r4-replay
     * is on main the age that lets the class switches' deferral pass (CLASSES_RETAIN_S); else the cluster's default */
    retain_eff = opt_retain_s > 0 ? opt_retain_s : (nemeses & N_LONGDOWN) ? 3 : (want_classes && !R4_REPLAY_ON_MAIN) ? CLASSES_RETAIN_S : 0;
    cls_defer_s = want_classes && !R4_REPLAY_ON_MAIN ? (retain_eff > 0 ? retain_eff : 1800) : 0;
    for (int k = 0; k < (int)(sizeof r4cp / sizeof r4cp[0]) && ncps < 16; k++)
        for (int j = 0; j < CRASH_POINT_COUNT; j++)
            if (!strcmp(r4cp[k].name, crash_point_names[j]))
                cps[ncps++] = (typeof(cps[0])){r4cp[k].name, 1, 90000, 3, r4cp[k].kind};
    if (want_ec_k && ec_settles) { /* the settle's crash points: the leader's marker, a replica's apply (leader 3) */
        static const struct {
            const char *name;
            int leader;
        } ecp[] = {{"cold-after-marker", 1}, {"cold-before-apply", 3}, {"cold-mid-intent", 3}};
        for (int k = 0; k < 3; k++)
            for (int j = 0; j < CRASH_POINT_COUNT; j++)
                if (!strcmp(ecp[k].name, crash_point_names[j]))
                    cps[ncps++] = (typeof(cps[0])){ecp[k].name, 1, 45000, ecp[k].leader, 0};
    }
    rng_state = mix(seed * 0x2545f4914f6cdd1dULL + 7);

    mkdir(opt_dir, 0755);
    if (!self_checker && !self_replicas && check_limits(nn, opt_dir) != 0)
        return 2;
    snprintf(rundir, sizeof rundir, "%s/run.XXXXXX", opt_dir);
    if (!mkdtemp(rundir)) {
        fprintf(stderr, "jepsen-cluster: mkdtemp %s: %s\n", rundir, strerror(errno));
        return 2;
    }
    if (self_checker) {
        int rc = selftest_checker();
        rmtree(rundir);
        return rc;
    }
    if (self_replicas) {
        int rc = selftest_replicas();
        rmtree(rundir);
        return rc;
    }
    snprintf(secret_path, sizeof secret_path, "%s/cluster.secret", rundir);
    char err[256];
    if (cluster_make_secret(secret_path, err, sizeof err) != 0 ||
        cluster_read_secret(secret_path, secret, sizeof secret, &secret_len, err, sizeof err) != 0)
        die(2, "the cluster secret: %s", err);
    obs = calloc(MAXOBS, sizeof *obs);

    for (int i = 0; i < nn; i++) {
        snprintf(nodes[i].dir, sizeof nodes[i].dir, "%s/n%d", rundir, i);
        snprintf(nodes[i].log, sizeof nodes[i].log, "%s/n%d.log", rundir, i);
        snprintf(nodes[i].octl, sizeof nodes[i].octl, "%s/n%d.octl", rundir, i);
        if (use_outagefs && strlen(nodes[i].octl) >= sizeof((struct sockaddr_un){0}.sun_path))
            die(2, "the outagefs control socket path %s is too long for a unix socket (use a shorter --dir)", nodes[i].octl);
        char st[PATH_MAX + 16];
        snprintf(st, sizeof st, "%s/state", nodes[i].dir);
        if (mkdir(nodes[i].dir, 0755) != 0 || mkdir(st, 0755) != 0)
            die(2, "mkdir %s: %s", st, strerror(errno));
        proxy_start(i);
    }
    trace_on = getenv("JEPSEN_TRACE") && atoi(getenv("JEPSEN_TRACE")) > 0;
    if (want_rep && (nemeses & N_MIDCOMMIT) && strcmp(mutant, "unfired")) {
        int cand[NCPS], nc = 0;
        for (int k = 0; k < NCPS; k++)
            if (cps[k].placed && cps[k].leader && r4_cp_ok(k) && (want_classes || strcmp(cps[k].name, "class-switch")))
                cand[nc++] = k;
        term_cp = cand[rnd() % (uint64_t)nc];
        const char *want_cp = getenv("JEPSEN_TERMINAL"); /* a point by name, to try one (the run's seed picks otherwise) */
        for (int k = 0; want_cp && *want_cp && k < nc; k++)
            if (!strcmp(cps[cand[k]].name, want_cp))
                term_cp = cand[k];
        snprintf(term_trigger, sizeof term_trigger, "%s/terminal.trigger", rundir);
        snprintf(term_spec, sizeof term_spec, "%s:1@%s", cps[term_cp].name, term_trigger);
    }
    start_nodes_par(term_cp >= 0 ? term_spec : NULL);
    cluster_form();
    settle("the formation");
    if (want_ec_k)
        ec_form_stores();
    if (want_kv)
        kv_setup();
    if (want_rep)
        rep_setup();
    if (rdma_tracking()) {
        struct rdma_mstat rms;
        rdma_sample_all(&rms);
    }
    atomic_store(&sampler_run, 1);
    pthread_create(&sampler_th, NULL, sampler, NULL);
    if (want_kv || want_rep)
        kv_start();
    if (want_users)
        ju_start();
    printf("jepsen-cluster: %d nodes, seed %" PRIu64 ", %d s, nemeses %d, workloads%s%s%s%s, run in %s%s%s\n", nn, seed,
           seconds, nemeses, want_meta ? " meta" : "", want_kv ? " kv-local" : "",
           want_classes ? " classes" : want_via ? " kv-via" : want_rep ? " kv" : "", want_users ? " users" : "", rundir, term_cp >= 0 ? "; terminal crash point " : "",
           term_cp >= 0 ? cps[term_cp].name : "");
    fflush(stdout);

    int kinds[NNEM], nk = 0;
    for (int b = 0; b < NNEM; b++)
        if (nemeses & (1 << b))
            kinds[nk++] = 1 << b;
    int64_t t_start = now_ms(), t_end = t_start + (int64_t)seconds * 1000;
    int takeover = !strncmp(mutant, "takeover", 8);
    unsigned rounds = 0, applied = 0;
    while (now_ms() < t_end) { /* --nemesis none: the workloads run their seconds without faults */
        check_unexpected_exits();
        if (takeover && !takeover_done && now_ms() - t_start > (int64_t)seconds * 1000 / 3) {
            takeover_event();
            settle("the takeover");
        }
        if (!nk) {
            hold_ms(500);
            continue;
        }
        int kind = kinds[rnd() % (uint64_t)nk];
        rounds++;
        if (!strcmp(mutant, "idle")) { /* the planted nemesis that sleeps through its naps */
            hold_ms(rnd_range(2000, 4000));
            continue;
        }
        applied++;
        effect_seen = 0;
        if (term_cp >= 0 && !term_done && now_ms() - t_start >= (int64_t)seconds * 600) {
            faults[3]++;
            nem_terminal();
            settle("the terminal crash point");
            hold_ms(rnd_range(300, 1200));
            continue;
        }
        switch (kind) {
        case N_KILL: faults[0]++; nem_kill(); break;
        case N_PARTITION: faults[1]++; nem_partition(); break;
        case N_PAUSE: faults[2]++; nem_pause(); break;
        case N_MIDCOMMIT: faults[3]++; nem_midcommit(); break;
        case N_POWER: faults[4]++; nem_power(); break;
        case N_FWDSTALL: faults[5]++; nem_fwdstall(); break;
        case N_KILL2: faults[6]++; nem_kill2(); break;
        case N_SLOW: faults[7]++; nem_slow(); break;
        case N_LONGDOWN: faults[8]++; nem_longdown(); break;
        case N_REBUILD: faults[9]++; nem_rebuild(); break;
        case N_CHURN: faults[10]++; nem_churn(); break;
        case N_PARTIAL: faults[11]++; nem_partial(); break;
        case N_DISKLOSS: faults[12]++; nem_diskloss(); break;
        case N_ROT: faults[13]++; nem_rot(); break;
        case N_RDMA_QPERR: faults[14]++; nem_rdma_qperr(); break;
        }
        if (rdma_tracking()) {
            struct rdma_mstat rms;
            rdma_sample_all(&rms);
        }
        if (!effect_seen)
            die(1, "the %s nemesis (round %u) had no effect: nothing was killed, stopped, cut, delayed or crashed",
                kind == N_KILL ? "kill" : kind == N_PARTITION ? "partition" : kind == N_PAUSE ? "pause" :
                kind == N_MIDCOMMIT ? "midcommit" : kind == N_FWDSTALL ? "fwdstall" : kind == N_KILL2 ? "kill2" :
                kind == N_SLOW ? "slow" : kind == N_LONGDOWN ? "longdown" : kind == N_REBUILD ? "rebuild" :
                kind == N_CHURN ? "churn" : kind == N_PARTIAL ? "partial" : kind == N_DISKLOSS ? "diskloss" :
                kind == N_ROT ? "rot" : kind == N_RDMA_QPERR ? "rdma-qperr" : "power", rounds);
        settle(kind == N_KILL ? "a kill" : kind == N_PARTITION ? "a partition" : kind == N_PAUSE ? "a pause" :
               kind == N_MIDCOMMIT ? "a crash point" : kind == N_FWDSTALL ? "a forward stall" : kind == N_KILL2 ? "two kills" :
               kind == N_SLOW ? "a slow disk" : kind == N_LONGDOWN ? "a long outage" : kind == N_REBUILD ? "a rebuild" :
               kind == N_CHURN ? "a transfer or move" : kind == N_PARTIAL ? "a partial partition" :
               kind == N_DISKLOSS ? "a disk loss" : kind == N_ROT ? "a rotted chunk" :
               kind == N_RDMA_QPERR ? "an RDMA QP error" : "a power loss");
        hold_ms(rnd_range(300, 1200));
    }
    if (term_cp >= 0 && !term_done) {
        cp_unfired++;
        snprintf(unfired_msg, sizeof unfired_msg, "the terminal crash point %s was never triggered (the run was too short)",
                 cps[term_cp].name);
    }
    /* the end: everything healed, resumed and running */
    net_heal();
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid > 0 && nodes[i].paused)
            pause_node(i, 0);
        else if (pid <= 0)
            restart_node(i);
    }
    settle("the last heal");
    if (want_users) { /* the probes go on a while after the admin ops stop: every member is clean and must agree */
        ju_stop_admin();
        hold_ms(3000);
        ju_stop();
    }
    /* classes_tail: under kill and power the writer changed too often for its switches to come out of CLS_DEFER, so a
     * run that has made none yet goes on without faults (the writers still running) until one is in a CLASSES; the
     * check at the end fails a run of 30 s or more that has none even then */
    if (want_classes && seconds >= 30 && classes_most() < 2 * 24 &&
        !(term_fired && !strcmp(cps[term_cp].name, "class-switch"))) {
        int64_t t0 = now_ms();
        while (classes_most() < 2 * 24 && now_ms() - t0 < CLASSES_TAIL_MS) {
            check_unexpected_exits();
            usleep(200000);
        }
        printf("jepsen-cluster: classes: %s %lld ms into the quiet tail after the last heal\n",
               classes_most() < 2 * 24 ? "still no switch" : "the first switch", (long long)(now_ms() - t0));
    }
    int viol = 0;
    uint64_t indet_applied = 0, nreads = 0, nwrites = 0;
    if (want_kv || want_rep) {
        kv_stop();
        if (want_rep)
            hold_ms(2000); /* the leader's commit point passes what was appended for the writes that timed out */
        if (want_rep) {
            /* a replica that left the ISR (out for isr_lag_ms under a kill, pause, partition or power loss) holds
             * what was acknowledged without it only once it has caught up and joined again; the end compares every
             * replica with the leader's final read, so it waits for that. One that never joins is a finding. */
            int64_t t0 = now_ms();
            uint64_t ver;
            int full = rep_full_mask();
            int m = rep_isr_mask(&ver);
            for (; m != full && now_ms() - t0 < 90000; m = rep_isr_mask(&ver))
                usleep(200000);
            if (m != full) {
                fprintf(stderr, "jepsen-cluster: FAIL: the ISR of kvr is not whole 90 s after the last heal (mask %d, "
                        "want %d, version %" PRIu64 "): a replica never caught up and joined again\n", m, full, ver);
                isr_short = 1;
            } else if (now_ms() - t0 > 200) {
                printf("jepsen-cluster: kvr's ISR whole again %lld ms after the writers stopped\n",
                       (long long)(now_ms() - t0));
                hold_ms(2000);
            }
        }
        if (want_rep) {
            r4_end_bad += rep_final_check();
            r4_end_bad += r4_leftovers_check();
            if (nemeses & N_CHURN)
                r4_end_bad += r4_move_trace_check();
        }
        for (int i = 0; i < nhists; i++)
            kv_final(i);
        if (want_ec_k)
            r4_end_bad += ec_members_check();
        if (!strcmp(mutant, "lost-write")) { /* planted: the final state of node 0 lacks a write that was acknowledged */
            struct hist *h = &hists[0];
            for (int k = 0; k < KEYS; k++) {
                uint32_t v = h->final[k];
                if (v && v <= h->nw && h->w[v - 1].st == WS_OK) {
                    h->final[k] = 0;
                    break;
                }
            }
        }
        for (int i = 0; i < nhists; i++) {
            viol += check_hist(&hists[i], &indet_applied);
            nwrites += hists[i].nw;
            nreads += hists[i].nr[0];
        }
    }
    atomic_store(&sampler_run, 0);
    pthread_join(sampler_th, NULL);
    proc_metrics_sample();
    unsigned terms, changes;
    int split = want_meta ? check_election_safety(&terms, &changes) : 0;
    if (!want_meta)
        terms = changes = 0;
    const char *dirs[MAXN * 3];
    int nd = replica_dirs(dirs, MAXN * 3);
    int rdiff = isr_short, isr = rep_full_mask();
    if (want_ec_k)
        r4_end_bad += ec_div_check(); /* its own FAIL line: not one of "the replicas differ" */
    struct rdma_mstat rdma_end;
    memset(&rdma_end, 0, sizeof rdma_end);
    if (rdma_tracking())
        rdma_sample_all(&rdma_end);
    /* a clean stop of every node: SIGTERM, status 0 (parallel) */
    int unclean = stop_nodes_par();
    if (rdma_tracking())
        rdma_pool_leaks = rdma_check_logs_for_leaks();
    if (foreign_pid > 0) {
        signal_child(foreign_pid, SIGTERM);
        waitpid(foreign_pid, NULL, 0);
    }
    /* Non-EC volumes: read every replica directory in place read-only (cvol_check). EC volumes have META
     * requires=tier and settled .ec parts backed by the chunk stores, so ec_members_check above already verified every
     * replica live through leadership transfers (full series equality, final keys, and fill_check). */
    if (nd > 0 && !want_ec_k) {
        rdiff += replicas_equal(dirs, nd, 1);
        rdiff += rep_check(0, isr, &viol);
        if (rdiff)
            fprintf(stderr, "jepsen-cluster: FAIL: %d difference(s) between the replicas of kvr, with its leader's "
                            "final read, or in the filler's large writes\n", rdiff);
    }
    if (want_ec_k && nd > 0)
        rdiff += ec_parts_check(dirs, nd);
    atomic_store(&stopping, 1);

    unsigned total_faults = 0;
    for (int k = 0; k < NNEM; k++)
        total_faults += faults[k];
    printf("faults applied: kill %u, partition %u (cuts applied %d%s), pause %u, midcommit %u (crash points hit %u, %u of them while serving, "
           "never fired %u), power %u (losses %u, idle %u), fwdstall %u (%u writes held, %u misses), rdma-qperr %u; restarts %u, rounds %u\n",
           faults[0], faults[1], partitions_applied, partitions_planted_noop ? ", PLANTED NO-OP" : "", faults[2],
           faults[3], cp_hits, cp_hits_serving, cp_unfired, faults[4], power_hits, power_idle, faults[5], fwd_stalls_held, fwd_stall_misses,
           faults[14], restarts, rounds);
    if (nemeses & (N_KILL2 | N_SLOW | N_R4_KV))
        printf("r4: kill2 %u rounds (%u took two of kvr's replicas), slow %u, longdown %u (%u of them with a snapshot installed), "
               "rebuild %u (%u rebuilt, %u by a snapshot), churn %u (%u moves, %u transfers accepted of which %u landed, %u cancels, %u refused)\n",
               kill2_done, kill2_rep, slow_done, longdown_done, longdown_snaps, rebuild_done,
               rebuild_ok, rebuild_snaps, faults[10], churn_moves, churn_transfers, churn_landed, churn_cancels, churn_refused);
    if (use_crashfs)
        printf("crashfs: %" PRIu64 " node starts, %" PRIu64 " of them with CRASHFS_TEAR, the rest CRASHFS_REWRITE=old\n",
               (uint64_t)atomic_load(&crashfs_starts), (uint64_t)atomic_load(&crashfs_tears));
    printf("settles: %d (mean %lld ms, max %lld ms); proxy: %" PRIu64 " links held, %" PRIu64 " reset by a heal\n", settles,
           settles ? (long long)(settle_ms_total / settles) : 0, (long long)settle_ms_max, (uint64_t)atomic_load(&held_links),
           (uint64_t)atomic_load(&reset_links));
    if (want_meta)
        printf("meta: %" PRIu64 " STATUS samples (%" PRIu64 " unanswered), %u terms with a leader, %u leader changes seen\n",
               samples_ok, samples_failed, terms, changes);
    if (want_rep) {
        struct stat cs;
        char cl[PATH_MAX + 48];
        snprintf(cl, sizeof cl, "%s/CLASSES", rep_dir[0]);
        printf("%s: filler inserts %" PRIu64 " acknowledged, %" PRIu64 " not; CLASSES %lld bytes on node %d; terminal "
               "crash point %s%s\n", want_classes ? "classes" : want_via ? "kv-via" : "kv", (uint64_t)atomic_load(&fill_ok),
               (uint64_t)atomic_load(&fill_err), stat(cl, &cs) == 0 ? (long long)cs.st_size : -1LL, rep_nodes[0],
               term_cp >= 0 ? cps[term_cp].name : "none", term_cp < 0 ? "" : term_fired ? " (fired)" : " (did not fire)");
    }
    if (want_kv || want_rep)
        printf("%s: %" PRIu64 " writes acknowledged, %" PRIu64 " refused, %" PRIu64 " indeterminate (%" PRIu64
               " of them applied), %" PRIu64 " reads, %" PRIu64 " MOVED followed; %u connections, %u refused for a port "
               "the node does not hold\n",
               want_kv ? "kv-local" : want_via ? "kv-via" : "kv", (uint64_t)atomic_load(&kv_ok), (uint64_t)atomic_load(&kv_fail), (uint64_t)atomic_load(&kv_indet),
               indet_applied, nreads, (uint64_t)atomic_load(&kv_moved), (unsigned)atomic_load(&client_connects), (unsigned)atomic_load(&refused_foreign));
    (void)nwrites;
    ec_summary();
    if (want_via)
        printf("kv-via: %" PRIu64 " writes and %" PRIu64 " reads answered through a member that did not lead kvr, %" PRIu64
               " read-your-writes reads, %d stall window(s) checked, %" PRIu64 " of %" PRIu64 " nemesis picks aimed at an entry "
               "node\n", (uint64_t)atomic_load(&via_fwd),
               (uint64_t)atomic_load(&via_fwd_reads), (uint64_t)atomic_load(&via_ryw_reads), nstalls,
               (uint64_t)atomic_load(&via_aimed), (uint64_t)atomic_load(&via_picks));
    if (rdma_tracking())
        printf("rdma: mode %s, max rdma links %" PRIu64 " (end rdma %" PRIu64 ", tcp %" PRIu64 "), qperr_fired %u "
               "(backoff rounds %u), qp_errors %" PRIu64 ", credit_stalls %" PRIu64 ", rnr_retries %" PRIu64
               ", fallbacks(qp_error=%" PRIu64 " backoff=%" PRIu64 " pool=%" PRIu64 " other=%" PRIu64 "), pool_leaks %u\n",
               opt_peer_rdma ? opt_peer_rdma : "auto", rdma_max_up_links, rdma_end.up_rdma, rdma_end.up_tcp,
               rdma_qperr_fired, rdma_backoff_rounds, rdma_end.qp_errors, rdma_end.credit_stalls, rdma_end.rnr_retries,
               rdma_end.fb_qperr, rdma_end.fb_backoff, rdma_end.fb_pool, rdma_end.fb_other, rdma_pool_leaks);
    int rc = 0;
    if (opt_peer_rdma && !strcmp(opt_peer_rdma, "on") && rdma_max_up_links == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: --peer-rdma on saw 0 RDMA links on /metrics\n");
        rc = 1;
    }
    if ((nemeses & N_RDMA_QPERR) && rdma_qperr_fired == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: rdma-qperr nemesis fired 0 QP errors\n");
        rc = 1;
    }
    if (rdma_pool_leaks > 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u RDMA pool leak(s) / drain timeout(s) in server logs\n", rdma_pool_leaks);
        rc = 1;
    }
    if (viol) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d violation(s):", viol);
        for (int k = 0; k < V_KINDS; k++)
            fprintf(stderr, " %s %d%s", viol_name[k], viol_by[k], k + 1 < V_KINDS ? "," : "\n");
        rc = 1;
    }
    if (split) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d term(s) with two leaders\n", split);
        rc = 1;
    }
    if (want_users && ju_report())
        rc = 1;
    if (cut_violations) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d cut direction(s) exchanged frames while cut: the partition was not applied\n",
                cut_violations);
        rc = 1;
    }
    if (cp_unfired) {
        fprintf(stderr, "jepsen-cluster: FAIL: %s\n", unfired_msg);
        rc = 1;
    }
    if (unclean || rdiff || r4_end_bad)
        rc = 1;
    if ((nemeses & N_KILL2) && want_rep && kill2_done >= 1 && !kill2_rep) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u kill2 round(s) and none took two of kvr's replicas\n", kill2_done);
        rc = 1;
    }
    /* a coverage miss, like an idle nemesis: at least half the rounds must have taken a snapshot (honest runs: 16 to 18
     * of 17 to 18, 7 of 8 in 120 s), or a retain cap that stopped working would pass on a round or two */
    if ((nemeses & N_LONGDOWN) && longdown_done >= 1 && 2 * longdown_snaps < longdown_done) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u longdown round(s) (a replica down past repl_retain_s %d) and only %u "
                        "took a snapshot (the directory of its volume was replaced): the others caught up from the log, "
                        "which tests nothing of R4\n", longdown_done, retain_eff > 0 ? retain_eff : 3, longdown_snaps);
        rc = 1;
    }
    if ((nemeses & N_REBUILD) && rebuild_ok >= 1 && 2 * rebuild_snaps < rebuild_ok) { /* as longdown's: the rebuild is by snapshot */
        fprintf(stderr, "jepsen-cluster: FAIL: %u rebuild round(s) and only %u were filled by a snapshot (a SNAPDONE in the "
                        "rebuilt v<N>): the others were filled from the log, which tests nothing of R4's rebuild\n",
                rebuild_ok, rebuild_snaps);
        rc = 1;
    }
    if ((nemeses & N_CHURN) && churn_transfers >= 3 && !churn_landed) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u planned transfers accepted and the leadership of kvr never reached the "
                        "named member\n", churn_transfers);
        rc = 1;
    }
    if ((nemeses & N_CHURN) && faults[10] >= 3 && !churn_moves && !churn_transfers) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u churn rounds and no transfer or move was accepted (%u refused)\n",
                faults[10], churn_refused);
        rc = 1;
    }
    if ((want_kv || want_rep) && (!atomic_load(&kv_ok) || !nreads)) { /* nothing to check is no evidence */
        fprintf(stderr, "jepsen-cluster: FAIL: the workload did nothing: %" PRIu64 " writes acknowledged, %" PRIu64
                " reads\n", (uint64_t)atomic_load(&kv_ok), nreads);
        rc = 1;
    }
    if (want_via && (!atomic_load(&via_fwd) || !atomic_load(&via_fwd_reads) || !atomic_load(&via_ryw_reads))) {
        fprintf(stderr, "jepsen-cluster: FAIL: kv-via checked no forwarding: %" PRIu64 " writes and %" PRIu64
                " reads through a member that does not lead kvr, %" PRIu64 " read-your-writes reads\n",
                (uint64_t)atomic_load(&via_fwd), (uint64_t)atomic_load(&via_fwd_reads), (uint64_t)atomic_load(&via_ryw_reads));
        rc = 1;
    }
    /* the picks a third of which should go to an entry node (via_pick): with this many, none aimed means the aim is
     * gone ((2/3)^30 by chance, and via_pick answers -1 only while no client has a non-leader member) */
    if (want_via && atomic_load(&via_picks) >= 30 && !atomic_load(&via_aimed)) {
        fprintf(stderr, "jepsen-cluster: FAIL: kv-via: none of %" PRIu64 " nemesis picks was aimed at an entry node\n",
                (uint64_t)atomic_load(&via_picks));
        rc = 1;
    }
    /* a classes run whose series never switched checked no CLASS marker; a terminal crash point that fired inside a
     * switch counts as one (its record may never have reached CLASSES) */
    if (want_classes && seconds >= 30 && !(term_fired && !strcmp(cps[term_cp].name, "class-switch"))) {
        long long most = classes_most();
        if (most < 2 * 24) { /* 24-byte records: the filler's first class, then one per switch */
            fprintf(stderr, "jepsen-cluster: FAIL: the classes workload made no class switch, not even in %d s without "
                            "faults after the last heal (CLASSES %lld bytes at most on kvr's replicas)\n",
                    CLASSES_TAIL_MS / 1000, most);
            rc = 1;
        }
    }
    /* a round of the older nemeses is 3 to 8 s. A longdown round holds the node down repl_retain_s + 4 to 10 s and then
     * waits for the snapshot, a rebuild round for the reset and the snapshot (measured: about 17 s at the default
     * retain of 3 and about 12 s). The rounds pick a nemesis at random, so the honest round is the mean of the chosen
     * ones, and the floor, as everywhere, a third of the honest rate (never below one fault). A list without these two
     * keeps its 10 s. */
    int per_round = 10;
    if (nemeses & (N_LONGDOWN | N_REBUILD)) {
        int sum = 0, n = 0;
        for (int k = 0; k < nk; k++) {
            sum += kinds[k] == N_LONGDOWN ? (retain_eff > 0 ? retain_eff : 3) + 14 : kinds[k] == N_REBUILD ? 12 : 6;
            n++;
        }
        per_round = 3 * sum / n;
    }
    unsigned expect = (unsigned)(seconds / per_round > 0 ? seconds / per_round : 1);
    if (nk && !takeover && total_faults < expect) {
        fprintf(stderr, "jepsen-cluster: FAIL: only %u faults in %d s (at least %u expected): the nemesis was idle\n",
                total_faults, seconds, expect);
        rc = 1;
    }
    if ((nemeses & N_PARTITION) && seconds >= 30 && faults[1] && !partitions_applied && !delays_applied && !partitions_planted_noop) {
        fprintf(stderr, "jepsen-cluster: FAIL: partitions were chosen and none was applied\n");
        rc = 1;
    }
    /* what the faults did, over the run (a round that does nothing is caught at its end; these are about the proxy and
     * the members: a heal that never reset a held link, a delay that never delayed a byte, a pause nobody noticed) */
    if (!takeover && partitions_applied >= 3 && !partitions_planted_noop && (!atomic_load(&held_links) || !atomic_load(&reset_links))) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d cuts applied, %" PRIu64 " links held and %" PRIu64 " reset by a heal: "
                "a held link must be reset at the heal\n", partitions_applied, (uint64_t)atomic_load(&held_links),
                (uint64_t)atomic_load(&reset_links));
        rc = 1;
    }
    if (delays_applied >= 3 && !atomic_load(&delayed_bytes)) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u delays applied and not one byte was delayed\n", delays_applied);
        rc = 1;
    }
    if (pauses_done >= 3 && !pauses_seen) {
        fprintf(stderr, "jepsen-cluster: FAIL: %u pauses and no live node ever saw the paused member silent\n", pauses_done);
        rc = 1;
    }
    if (want_meta && leader_cuts >= 4 && !leader_replaced) {
        fprintf(stderr, "jepsen-cluster: FAIL: the leader was cut from every node %u times and was never replaced\n",
                leader_cuts);
        rc = 1;
    }
    printf("effects: %u pauses seen silent by a live node of %u, %u leader cuts (%u with a new leader), %u delays "
           "(%" PRIu64 " bytes delayed)\n", pauses_seen, pauses_done, leader_cuts, leader_replaced, delays_applied,
           (uint64_t)atomic_load(&delayed_bytes));
    {
        uint64_t rss_max = 0, rss_sum = 0;
        unsigned fd_max = 0, fd_sum = 0;
        for (int i = 0; i < nn; i++) {
            uint64_t r = atomic_load(&nodes[i].peak_rss_kb);
            unsigned f = atomic_load(&nodes[i].peak_fds);
            if (r > rss_max)
                rss_max = r;
            rss_sum += r;
            if (f > fd_max)
                fd_max = f;
            fd_sum += f;
        }
        printf("resources: wall %.1f s; peak RSS per node: max %.1f MiB, mean %.1f MiB (sum %.1f MiB); "
               "peak FDs per node: max %u, mean %.1f (harness %u)\n",
               (double)(now_ms() - wall_t0) / 1000.0, (double)rss_max / 1024.0,
               nn ? (double)rss_sum / (1024.0 * nn) : 0.0, (double)rss_sum / 1024.0, fd_max,
               nn ? (double)fd_sum / nn : 0.0, (unsigned)atomic_load(&harness_peak_fds));
    }
    if (atomic_load(&unmapped_links)) {
        fprintf(stderr, "jepsen-cluster: FAIL: %" PRIu64 " proxied link(s) came from a member id that the node table lacks: "
                "they were neither cut nor counted\n", (uint64_t)atomic_load(&unmapped_links));
        rc = 1;
    }
    printf("jepsen-cluster: %s (seed %" PRIu64 ")\n", rc ? "FAILED" : "ok", seed);
    if (!rc && !getenv("JEPSEN_KEEP"))
        rmtree(rundir);
    else if (rc)
        fprintf(stderr, "  logs and state of the nodes kept in %s\n", rundir);
    (void)keep_run;
    return rc;
}
