/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * jepsen-cluster: a Jepsen-style run of a tsdbd cluster (docs/design-cluster.md 13.2, design-cluster-data.md 10.3).
 *
 *   jepsen_cluster --dir DIR [--nodes 5] [--seconds 30] [--nemesis kill,partition,pause,midcommit,power]
 *                  [--workload meta,kv-local|kv|classes] [--seed N] [--crashfs build/libcrashfs.so] [--mutant NAME]
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
 * indeterminate, never a failure); it joins again once caught up. The leader of kvr is never
 * the target of kill, power, pause or a midcommit restart (a restarted leader never leads again before r3e-failover);
 * its replicas are. midcommit also arms one of the driver's crash points (wal-before-send, acked-before-ok,
 * prepared-before-commit, class-switch) on every node at its first start behind a trigger file, made past the middle
 * of the run: the leader must end at it (nem_terminal), and the volume has no leader after that. The end check reads
 * every replica directory in place, read-only, after the clean stop (tests/cvol_check.c): with the leader alive to the
 * end, the run first waits (60 s) for kvr's ISR to be whole again, then the replicas hold the same rows
 * (replicas_equal) and the keys of the leader's final read; after the terminal point, each replica in kvr's last ISR
 * (rep_isr_mask) passes checks 1 and 2 as the final state, and the others check 2. In both cases every replica holds
 * each of the filler's large writes whole or not at all, whole when acknowledged and in the ISR (fill_check).
 *
 * Selftest (--selftest-checker, --mutant NAME; each must fail, `make jepsen-cluster-selftest`): foreign-pid (a port file that names another
 * pid), takeover (a stale table, a foreign server on the node's old port, and a client that skips th_owns_port),
 * takeover-checked (the same with the check: refused, the run passes), noop-partition, unfired (an armed crash point
 * nobody placed), idle (a nemesis that sleeps through its naps), lost-write (a write missing from the final state), nokill, nopause,
 * nodelay and noreset (a kill, pause or delay that does nothing, a heal that resumes held links: the run fails),
 * rep-lost-write (kv: an acknowledged write missing from one replica's directory; under midcommit, from a member of
 * the last ISR after the terminal point), rep-half-batch (kv: half of an
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

#include <arpa/inet.h>
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
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>

#define MAXN 8
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
static struct meta_settings S;

enum { N_KILL = 1, N_PARTITION = 2, N_PAUSE = 4, N_MIDCOMMIT = 8, N_POWER = 16 };
static int nemeses = N_KILL | N_PARTITION | N_PAUSE | N_MIDCOMMIT;

static int64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
static int64_t now_ms(void) { return now_ns() / 1000000; }

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
    _Atomic uint32_t id; /* member id, 0 until it joined */
    char dir[PATH_MAX + 16], log[PATH_MAX + 32];
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

/* Starts node i. 0: it is up (the table has its ports, read from its port file and held by its pid). 1: it exited
 * before it listened, with *status as waitpid gave it (a crash point's 99). Anything else is a failed run. */
static int start_node(int i, const char *crash_at, int *status)
{
    struct node *n = &nodes[i];
    char pf[PATH_MAX + 128], seen[PATH_MAX + 136], flag[64], padv[64], state[PATH_MAX + 32], io[16];
    th_portfile(pf, sizeof pf, n->dir);
    snprintf(seen, sizeof seen, "%s", pf);
    th_follow_me(flag, sizeof flag);
    snprintf(padv, sizeof padv, "127.0.0.1:%d", n->proxy_port);
    snprintf(state, sizeof state, "%s/state", n->dir);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    const char *argv[32];
    int na = 0;
    argv[na++] = "tsdbd";
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
    if (getenv("TSDBD_IO") && *getenv("TSDBD_IO")) {
        snprintf(io, sizeof io, "%s", getenv("TSDBD_IO"));
        argv[na++] = "--io";
        argv[na++] = io;
    }
    argv[na] = NULL;
    /* the environment is built before the fork: a child of a threaded process may only call async-signal-safe functions */
    char *envp[512];
    int ne = 0;
    for (char **e = environ; *e && ne < 500; e++)
        if (strncmp(*e, "TSDB_CLUSTER_TEST=", 18) && strncmp(*e, "TSDB_CRASH_AT=", 14) && strncmp(*e, "LD_PRELOAD=", 11) &&
            strncmp(*e, "CRASHFS_", 8) && strncmp(*e, "TSDB_CLUSTER_WRITES=", 20))
            envp[ne++] = *e;
    char e1[256], e2[PATH_MAX + 64], e3[PATH_MAX + 64], e4[64];
    if (!want_rep) /* the leader of a 1-replica cluster volume takes writes without the driver (kv-local) */
        envp[ne++] = "TSDB_CLUSTER_WRITES=1";
    if (crash_at) {
        snprintf(e1, sizeof e1, "TSDB_CRASH_AT=%s", crash_at);
        envp[ne++] = e1;
    }
    if (use_crashfs) {
        snprintf(e2, sizeof e2, "LD_PRELOAD=%s", crashfs_so);
        snprintf(e3, sizeof e3, "CRASHFS_ROOT=%s", n->dir);
        envp[ne++] = e2;
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
            envp[ne++] = "CRASHFS_KEEP_OLD=1";
    }
    envp[ne] = NULL;
    pid_t pid = fork();
    if (pid < 0)
        die(2, "fork: %s", strerror(errno));
    if (pid == 0) {
        int fd = open(n->log, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0)
            dup2(fd, 2);
        execve(bin, (char *const *)argv, envp);
        _exit(127);
    }
    if (!strcmp(mutant, "foreign-pid") && i == 1 && n->starts == 0) {
        struct pfcopy *c = malloc(sizeof *c);
        snprintf(c->from, sizeof c->from, "%s", pf);
        snprintf(seen, sizeof seen, "%s.seen", pf);
        snprintf(c->to, sizeof c->to, "%s", seen);
        pthread_t t;
        pthread_create(&t, NULL, pf_rewriter, c);
        pthread_detach(t);
    }
    n->starts++;
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
    if (!th_pport)
        die(1, "node %d: the port file names no peer port", i);
    pthread_mutex_lock(&tbl_mu);
    n->pid = pid;
    n->cport = port;
    n->pport = th_pport;
    n->paused = 0;
    pthread_mutex_unlock(&tbl_mu);
    return 0;
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

/* STATUS of node i through its real peer port, as `tsdb cluster status` asks. 0, or -1 when it did not answer. */
static int view_get(int i, struct view *v, int timeout_ms)
{
    int pp;
    node_snap(i, NULL, NULL, &pp);
    memset(v, 0, sizeof *v);
    meta_init(&v->m);
    if (pp <= 0)
        return -1;
    char addr[48], err[256];
    snprintf(addr, sizeof addr, "127.0.0.1:%d", pp);
    int rc;
    size_t len = 0;
    if (cluster_admin(addr, secret, secret_len, CLUSTER_OP_STATUS, NULL, 0, timeout_ms, &rc, &v->raw, &len, err,
                      sizeof err) != 0 || rc != 0 || cluster_status_parse(v->raw, len, &v->st) != 0) {
        free(v->raw);
        meta_free(&v->m);
        memset(v, 0, sizeof *v);
        return -1;
    }
    if (v->st.image_len && meta_snapshot_decode(&v->m, v->st.image, v->st.image_len) != META_OK) {
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

/* Every node up and answering, one leader that every core node names, every member UP, the same metadata. A node
 * stuck in a state nobody leaves is what this is for. Returns 0, or -1 with `why` naming what is not yet so. */
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
            snprintf(why, wn, "node %d does not answer STATUS", i);
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
    char why[256] = "";
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
    if (good < 2)
        die(1, "no settle within %d ms after %s: %s", SETTLE_MS, after, why);
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
static pthread_t sampler_th;
static uint64_t samples_ok, samples_failed;

static void *sampler(void *arg)
{
    (void)arg;
    while (atomic_load(&sampler_run)) {
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

/* ------------------------------------------------------------------ the histories and their checker */

enum { WS_INDET = 0, WS_OK = 1, WS_FAIL = 2 };
struct wr {
    int8_t nk, keys[3];
    uint8_t st;
    int64_t begin, end; /* end: INT64_MAX while indeterminate */
};
struct rd {
    int64_t begin, end;
    uint32_t seen[KEYS]; /* the id of the write whose value a key shows, 0: nothing */
    uint8_t stray;
    int8_t node; /* the node that answered it (kv_roam: a stale read names it) */
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

static uint32_t hist_begin(struct hist *h, const int *keys, int nk)
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

/* the node a (re)connecting client of history hi takes: hist_node[hi], or under kv_roam the one a MOVED named, else
 * a random member */
static int kv_pick(int hi, int *next, uint64_t *r)
{
    if (!kv_roam)
        return hist_node[hi];
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

static void *kv_writer(void *arg)
{
    int hi = (int)(intptr_t)arg, node = hist_node[hi], next = -1;
    unsigned logged = 0;
    struct hist *h = &hists[hi];
    tsdbc *c = NULL;
    tsdb_sid sid = 0;
    uint64_t r = mix(seed * 31 + (uint64_t)hi);
    while (atomic_load(&kv_run)) {
        if (!c) {
            node = kv_pick(hi, &next, &r);
            if (connect_node(node, 0, &c) != 0 || kv_resolve(c, hi, &sid) != 0) {
                if (c)
                    tsdbc_close(c);
                c = NULL;
                usleep(30000);
                continue;
            }
        }
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
        uint32_t id = hist_begin(h, keys, nk);
        int64_t ts[3];
        float val[3];
        for (int k = 0; k < nk; k++) {
            ts[k] = key_ts(keys[k]);
            val[k] = (float)id;
        }
        int rc = tsdbc_insert_series(c, sid, ts, val, (size_t)nk);
        if (rc == 0) {
            hist_end(h, id, WS_OK);
            atomic_fetch_add(&kv_ok, 1);
        } else if (rc == TSDB_ERR_UNAVAILABLE || rc == TSDB_ERR_MOVED || rc == TSDB_ERR_INVALID ||
                   rc == TSDB_ERR_DROPPED || rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_DENIED) {
            hist_end(h, id, WS_FAIL); /* refused: definitely not applied */
            atomic_fetch_add(&kv_fail, 1);
            if (kv_follow(c, hi, node, &next, &logged, "the writer")) {
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

/* the moment the terminal crash point was triggered (kv, classes): the leader of kvr ends at it and comes back as a
 * replica, and the volume has no leader after it (no promotion before r3e-failover): the readers stop there (a replica
 * answers MOVED or UNAVAILABLE since route_decide, never a read; before it, it answered stale reads). */
static _Atomic int64_t term_ns;

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
            node = kv_pick(hi, &next, &r);
            if (connect_node(node, 0, &c) != 0 || kv_resolve(c, hi, &sid) != 0) {
                if (c)
                    tsdbc_close(c);
                c = NULL;
                usleep(30000);
                continue;
            }
        }
        if (atomic_load(&term_ns)) {
            usleep(100000);
            continue;
        }
        struct rd tmp;
        memset(&tmp, 0, sizeof tmp);
        tmp.node = (int8_t)node;
        if (kv_read_all(c, sid, &tmp) == 0 && (!atomic_load(&term_ns) || tmp.begin < atomic_load(&term_ns)))
            *hist_read_slot(h, 0) = tmp;
        else {
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
static int rep_leader = -1, rep_nodes[3], protect_node = -1;
static uint32_t rep_number;
static char rep_dir[3][PATH_MAX + 32];
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

static void rep_setup(void)
{
    uint8_t req[META_NAME_MAX + 8];
    size_t rl = cluster_volume_add_encode("kvr", 3, req);
    int rc = admin_real(0, CLUSTER_OP_VOLUME_ADD, req, rl, 1, 15000);
    if (rc != 0)
        die(1, "volume add kvr: %s", cluster_strerror(rc));
    for (int64_t t0 = now_ms(); rep_leader < 0; usleep(200000)) {
        struct view v;
        if (now_ms() - t0 > 60000)
            die(1, "kv: the volume kvr was not opened on 3 replicas with a leader within 60 s");
        if (view_get(0, &v, 2000) != 0)
            continue;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (strcmp(mv->name, "kvr") || mv->nrep != 3 || !mv->leader)
                continue;
            int ok = 1, lead = -1;
            for (int r = 0; r < 3; r++) {
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
                rep_leader = protect_node = lead;
            }
        }
        view_free(&v);
    }
    for (int r = 0; r < 3; r++)
        snprintf(rep_dir[r], sizeof rep_dir[r], "%s/v%" PRIu32, nodes[rep_nodes[r]].dir, rep_number);
    nhists = REP_HISTS;
    static const char *const names[REP_HISTS + 1] = {"kvr.a", "kvr.b", "kvr.fill"};
    for (int i = 0; i <= REP_HISTS; i++) {
        if (i < REP_HISTS) {
            snprintf(hists[i].name, sizeof hists[i].name, "%s", names[i]);
            hist_node[i] = rep_leader;
            pthread_mutex_init(&hists[i].mu, NULL);
        }
        for (int64_t t1 = now_ms();; usleep(100000)) { /* the leader promotes itself once its EPOCH marker commits */
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
            if ((rc != TSDB_ERR_UNAVAILABLE && rc != TSDB_ERR_NOTFOUND) || now_ms() - t1 > 30000)
                die(1, "kv: cannot create %s in kvr on its leader (node %d): %s", names[i], rep_leader, tsdbc_strerror(rc));
        }
    }
    kv_roam = 1;
    printf("jepsen-cluster: kvr is volume %" PRIu32 " on nodes %d, %d, %d; its leader is node %d; clients on random "
           "members, moved to it\n", rep_number, rep_nodes[0], rep_nodes[1], rep_nodes[2], rep_leader);
}

/* kvr's ISR as the metadata group holds it: a mask of the replicas (bit r: rep_nodes[r]) in the record with the highest
 * ISR version among the copies of every running node (one node's copy may lag), its version in *ver. -1 when no node
 * answered. The leader commits on the union of the ISR in force and one it asked for (src/repl.c), and a member joins
 * only once its log reaches the commit point, so every member of the last ISR holds every write the leader
 * acknowledged; a replica outside it was not needed for the acknowledgements since it left. */
static int rep_isr_mask(uint64_t *ver)
{
    int mask = -1;
    *ver = 0;
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
            for (int r = 0; r < 3; r++)
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
            if (connect_node(rep_leader, 0, &c) != 0 || tsdbc_series(c, "kvr.fill", 0, &sid) != 0) {
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
        if (connect_node(hist_node[i], 1, &c) == 0) {
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
    die(1, "node %d: no final read of %s within 30 s", hist_node[i], h->name);
}

static int cmp_i64pair(const void *a, const void *b)
{
    int64_t x = ((const int64_t *)a)[0], y = ((const int64_t *)b)[0];
    return x < y ? -1 : x > y;
}

enum { V_DURABLE, V_VALID, V_ATOMIC, V_STALE, V_MONO, V_KINDS };
static const char *const viol_name[V_KINDS] = {"durability", "validity", "atomic visibility", "stale read", "monotonic reads"};
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
                    if (v == 0)
                        VIOL(V_STALE, "stale read: key %d is empty in a read that began after it was written and acknowledged "
                                     "(read %u, answered by node %d)", k, j, rd->node);
                    else if (v <= nw && w[v - 1].end < ack[k][2 * (lo - 1) + 1])
                        VIOL(V_STALE, "stale read: key %d shows write %u, older than an acknowledged write of the key (read %u, "
                                     "answered by node %d)", k, v, j, rd->node);
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
                    if (v == 0 || (v <= nw && w[v - 1].end < w[pv - 1].begin))
                        VIOL(V_MONO, "monotonic reads: key %d went back from write %u to %s", k, pv, v ? "an older one" : "nothing");
                }
            }
            prev = rd;
        }
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
        if (strcmp(x->name, y->name) || x->n != y->n || memcmp(x->ts, y->ts, x->n * sizeof *x->ts) ||
            memcmp(x->v, y->v, x->n * sizeof *x->v)) {
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
    for (int r = 0; want_rep && r < 3 && n < max; r++)
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
    if (r == 2 && !strcmp(mutant, "rep-half-batch"))
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
    int bad = 0, plant = 2;
    if (terminal)
        for (int r = 0; r < 3; r++)
            if (isr & 1 << r)
                plant = r;
    for (int r = 0; r < 3; r++) {
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
 * place theirs); `window_ms`: how long the armed node may run before the point is called unfired; `leader`: only the
 * leader of a replicated volume passes it (server/repld.c), so it is never armed by a restart (that would end the
 * leadership) but at every node's first start behind a trigger file, once per run (nem_terminal; kv and classes). */
static struct {
    const char *name;
    int placed, window_ms, leader;
} cps[] = {{"cluster-started", 1, 5000, 0},
           {"serving", 1, 5000, 0},
           {"wal-before-send", 1, 20000, 1},
           {"acked-before-ok", 1, 20000, 1},
           {"prepared-before-commit", 1, 20000, 1},
           {"class-switch", 1, 30000, 1}};
#define NCPS ((int)(sizeof cps / sizeof cps[0]))

static unsigned faults[8], restarts, cp_hits, cp_hits_serving, cp_unfired, power_hits, power_idle;
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

/* a node to kill, power off, pause or arm: never the replicated volume's leader (protect_node) */
static int pick_node(int bias_leader)
{
    int l = atomic_load(&last_leader);
    if (bias_leader && l >= 0 && l < nn && l != protect_node && rnd() % 2)
        return l;
    for (;;) {
        int i = rnd_range(0, nn - 1);
        if (i != protect_node)
            return i;
    }
}

static void restart_node(int i)
{
    int st = 0;
    if (start_node(i, NULL, &st) != 0)
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

/* The terminal crash point (kv, classes, midcommit): every node started first with TSDB_CRASH_AT=<point>:1@<trigger>;
 * past the middle of the run the trigger file is made and the leader of the replicated volume must end at the point
 * within its window. Restarted, it never leads that volume again (no failover before r3e): the rest of the run writes
 * fail, and the end check reads the replica directories as the final state. */
static int term_cp = -1, term_fired, term_done, isr_short;
static char term_trigger[PATH_MAX], term_spec[PATH_MAX + 64];

static void nem_terminal(void)
{
    int L = rep_leader;
    int fd = open(term_trigger, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0)
        die(2, "create %s: %s", term_trigger, strerror(errno));
    close(fd);
    atomic_store(&term_ns, now_ns());
    int win = cps[term_cp].window_ms, st = await_exit(L, win);
    if (st == -1) {
        cp_unfired++;
        snprintf(unfired_msg, sizeof unfired_msg, "the armed crash point %s never fired on node %d (the leader of kvr) "
                 "within %d ms of its trigger", cps[term_cp].name, L, win);
        stop_node(L, SIGKILL);
    } else if (WIFEXITED(st) && WEXITSTATUS(st) == CRASH_EXIT_STATUS) {
        cp_hits++;
        cp_hits_serving++;
        term_fired = 1;
    } else
        die(1, "node %d (the leader of kvr) ended with status 0x%x with %s triggered, not at the point (log %s)", L, st,
            cps[term_cp].name, nodes[L].log);
    effect_seen = 1;
    term_done = 1;
    hold_ms(rnd_range(200, 800));
    restart_node(L);
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
    uint8_t req[64];
    size_t rl = cluster_settings_encode(&S, META_MODE_REPLICATED, req);
    int rc = admin_real(0, CLUSTER_OP_INIT, req, rl, 0, 10000);
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
                    "         [--nemesis kill,partition,pause,midcommit,power] [--workload meta,kv-local]\n"
                    "         [--crashfs build/libcrashfs.so] [--mutant NAME] | --selftest-replicas | --selftest-checker\n");
    exit(2);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    int self_replicas = 0, self_checker = 0;
    for (int a = 1; a < argc; a++) {
        const char *k = argv[a];
        if (!strcmp(k, "--selftest-replicas"))
            self_replicas = 1;
        else if (!strcmp(k, "--selftest-checker"))
            self_checker = 1;
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
        else if (!strcmp(k, "--mutant"))
            mutant = argv[++a];
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
                else if (nem && !strcmp(t, "none"))
                    nemeses |= 0;
                else if (!nem && !strcmp(t, "meta"))
                    want_meta = 1;
                else if (!nem && !strcmp(t, "kv-local"))
                    want_kv = 1;
                else if (!nem && !strcmp(t, "kv"))
                    want_rep = 1;
                else if (!nem && !strcmp(t, "classes"))
                    want_rep = want_classes = 1;
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
    use_crashfs = (nemeses & N_POWER) != 0;
    rng_state = mix(seed * 0x2545f4914f6cdd1dULL + 7);

    mkdir(opt_dir, 0755);
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
        char st[PATH_MAX + 16];
        snprintf(st, sizeof st, "%s/state", nodes[i].dir);
        if (mkdir(nodes[i].dir, 0755) != 0 || mkdir(st, 0755) != 0)
            die(2, "mkdir %s: %s", st, strerror(errno));
        proxy_start(i);
    }
    if (want_rep && (nemeses & N_MIDCOMMIT) && strcmp(mutant, "unfired")) {
        int cand[NCPS], nc = 0;
        for (int k = 0; k < NCPS; k++)
            if (cps[k].placed && cps[k].leader && (want_classes || strcmp(cps[k].name, "class-switch")))
                cand[nc++] = k;
        term_cp = cand[rnd() % (uint64_t)nc];
        snprintf(term_trigger, sizeof term_trigger, "%s/terminal.trigger", rundir);
        snprintf(term_spec, sizeof term_spec, "%s:1@%s", cps[term_cp].name, term_trigger);
    }
    for (int i = 0; i < nn; i++) {
        int st = 0;
        if (start_node(i, term_cp >= 0 ? term_spec : NULL, &st) != 0)
            die(1, "node %d exited with status 0x%x before it listened (log %s)", i, st, nodes[i].log);
    }
    cluster_form();
    settle("the formation");
    if (want_kv)
        kv_setup();
    if (want_rep)
        rep_setup();
    atomic_store(&sampler_run, 1);
    pthread_create(&sampler_th, NULL, sampler, NULL);
    if (want_kv || want_rep)
        kv_start();
    printf("jepsen-cluster: %d nodes, seed %" PRIu64 ", %d s, nemeses %d, workloads%s%s%s, run in %s%s%s\n", nn, seed,
           seconds, nemeses, want_meta ? " meta" : "", want_kv ? " kv-local" : "",
           want_classes ? " classes" : want_rep ? " kv" : "", rundir, term_cp >= 0 ? "; terminal crash point " : "",
           term_cp >= 0 ? cps[term_cp].name : "");
    fflush(stdout);

    int kinds[5], nk = 0;
    for (int b = 0; b < 5; b++)
        if (nemeses & (1 << b))
            kinds[nk++] = 1 << b;
    int64_t t_start = now_ms(), t_end = t_start + (int64_t)seconds * 1000;
    int takeover = !strncmp(mutant, "takeover", 8);
    unsigned rounds = 0, applied = 0;
    while (now_ms() < t_end && (nk || takeover)) {
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
        }
        if (!effect_seen)
            die(1, "the %s nemesis (round %u) had no effect: nothing was killed, stopped, cut, delayed or crashed",
                kind == N_KILL ? "kill" : kind == N_PARTITION ? "partition" : kind == N_PAUSE ? "pause" :
                kind == N_MIDCOMMIT ? "midcommit" : "power", rounds);
        settle(kind == N_KILL ? "a kill" : kind == N_PARTITION ? "a partition" : kind == N_PAUSE ? "a pause" :
               kind == N_MIDCOMMIT ? "a crash point" : "a power loss");
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
    int viol = 0;
    uint64_t indet_applied = 0, nreads = 0, nwrites = 0;
    if (want_kv || want_rep) {
        kv_stop();
        if (want_rep)
            hold_ms(2000); /* the leader's commit point passes what was appended for the writes that timed out */
        if (want_rep && !term_fired) {
            /* a replica that left the ISR (out for isr_lag_ms under a kill, pause, partition or power loss) holds
             * what was acknowledged without it only once it has caught up and joined again; the end compares every
             * replica with the leader's final read, so it waits for that. One that never joins is a finding. */
            int64_t t0 = now_ms();
            uint64_t ver;
            int m = rep_isr_mask(&ver);
            for (; m != 7 && now_ms() - t0 < 60000; m = rep_isr_mask(&ver))
                usleep(200000);
            if (m != 7) {
                fprintf(stderr, "jepsen-cluster: FAIL: the ISR of kvr is not whole 60 s after the last heal (mask %d, "
                        "version %" PRIu64 "): a replica never caught up and joined again\n", m, ver);
                isr_short = 1;
            } else if (now_ms() - t0 > 200) {
                printf("jepsen-cluster: kvr's ISR whole again %lld ms after the writers stopped\n",
                       (long long)(now_ms() - t0));
                hold_ms(2000);
            }
        }
        for (int i = 0; i < nhists && !term_fired; i++)
            kv_final(i);
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
    unsigned terms, changes;
    int split = want_meta ? check_election_safety(&terms, &changes) : 0;
    if (!want_meta)
        terms = changes = 0;
    const char *dirs[MAXN * 3];
    int nd = replica_dirs(dirs, MAXN * 3);
    int rdiff = isr_short, isr = 7;
    if (want_rep && term_fired) {
        /* after the terminal point kvr has no leader, so nothing changes its ISR any more */
        uint64_t ver;
        isr = rep_isr_mask(&ver);
        if (isr < 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: no node answered with kvr's ISR after the terminal crash point\n");
            rdiff++;
            isr = 7;
        } else
            printf("jepsen-cluster: kvr's last ISR (version %" PRIu64 "): replicas%s%s%s\n", ver,
                   isr & 1 ? " 0" : "", isr & 2 ? " 1" : "", isr & 4 ? " 2" : "");
    }
    /* a clean stop of every node: SIGTERM, status 0 */
    int unclean = 0;
    for (int i = 0; i < nn; i++) {
        int st = stop_node(i, SIGTERM);
        if (!(WIFEXITED(st) && WEXITSTATUS(st) == 0)) {
            fprintf(stderr, "jepsen-cluster: node %d did not stop cleanly on SIGTERM (status 0x%x)\n", i, st);
            unclean++;
        }
    }
    if (foreign_pid > 0) {
        signal_child(foreign_pid, SIGTERM);
        waitpid(foreign_pid, NULL, 0);
    }
    if (nd > 0) {
        if (!term_fired) /* after the terminal point a replica may lack what the leader appended last */
            rdiff += replicas_equal(dirs, nd, 1);
        rdiff += rep_check(term_fired, isr, &viol);
        if (rdiff)
            fprintf(stderr, "jepsen-cluster: FAIL: %d difference(s) between the replicas of kvr, with its leader's "
                            "final read, or in the filler's large writes\n", rdiff);
    }
    atomic_store(&stopping, 1);

    unsigned total_faults = faults[0] + faults[1] + faults[2] + faults[3] + faults[4];
    printf("faults applied: kill %u, partition %u (cuts applied %d%s), pause %u, midcommit %u (crash points hit %u, %u of them while serving, "
           "never fired %u), power %u (losses %u, idle %u); restarts %u, rounds %u\n",
           faults[0], faults[1], partitions_applied, partitions_planted_noop ? ", PLANTED NO-OP" : "", faults[2],
           faults[3], cp_hits, cp_hits_serving, cp_unfired, faults[4], power_hits, power_idle, restarts, rounds);
    if (use_crashfs)
        printf("crashfs: %" PRIu64 " node starts, %" PRIu64 " of them with CRASHFS_TEAR, the rest CRASHFS_KEEP_OLD\n",
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
               "crash point %s%s\n", want_classes ? "classes" : "kv", (uint64_t)atomic_load(&fill_ok),
               (uint64_t)atomic_load(&fill_err), stat(cl, &cs) == 0 ? (long long)cs.st_size : -1LL, rep_nodes[0],
               term_cp >= 0 ? cps[term_cp].name : "none", term_cp < 0 ? "" : term_fired ? " (fired)" : " (did not fire)");
    }
    if (want_kv || want_rep)
        printf("%s: %" PRIu64 " writes acknowledged, %" PRIu64 " refused, %" PRIu64 " indeterminate (%" PRIu64
               " of them applied), %" PRIu64 " reads, %" PRIu64 " MOVED followed; %u connections, %u refused for a port "
               "the node does not hold\n",
               want_kv ? "kv-local" : "kv", (uint64_t)atomic_load(&kv_ok), (uint64_t)atomic_load(&kv_fail), (uint64_t)atomic_load(&kv_indet),
               indet_applied, nreads, (uint64_t)atomic_load(&kv_moved), (unsigned)atomic_load(&client_connects), (unsigned)atomic_load(&refused_foreign));
    (void)nwrites;
    int rc = 0;
    if (viol) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d violation(s) of checks 1-5:", viol);
        for (int k = 0; k < V_KINDS; k++)
            fprintf(stderr, " %s %d%s", viol_name[k], viol_by[k], k + 1 < V_KINDS ? "," : "\n");
        rc = 1;
    }
    if (split) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d term(s) with two leaders\n", split);
        rc = 1;
    }
    if (cut_violations) {
        fprintf(stderr, "jepsen-cluster: FAIL: %d cut direction(s) exchanged frames while cut: the partition was not applied\n",
                cut_violations);
        rc = 1;
    }
    if (cp_unfired) {
        fprintf(stderr, "jepsen-cluster: FAIL: %s\n", unfired_msg);
        rc = 1;
    }
    if (unclean || rdiff)
        rc = 1;
    unsigned expect = (unsigned)(seconds / 10 > 0 ? seconds / 10 : 1); /* a round is 3 to 8 s */
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
