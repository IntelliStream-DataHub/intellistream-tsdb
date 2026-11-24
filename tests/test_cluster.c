/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd --cluster end to end (docs/design-cluster.md §2, step 1 of §14): servers started as tsdbd with --port 0
 * --port-file (tests/harness.h), the metadata group on their peer ports. Each scenario starts its own servers in its
 * own directory.
 *
 * life: the life of a small cluster. `init` on one server; `node add` of the others, once through a server that is
 * not the leader (NOTLEADER names the leader, the caller follows); the voters grow 1 -> 3 -> 5 with the members (with
 * four members the fourth watches: its watch stream catches up to the leader's state); STATUS from every server
 * agrees; SIGKILL of the Raft leader: another is elected, the dead member is marked down, and after voter_replace_s it
 * leaves the voters (no healthy server is left to take its place, so four remain); a restart on the same directory
 * comes back with incarnation + 1, is marked up and becomes a voter again; SIGTERM stops every server with status 0.
 * `tsdb cluster status` (the CLI) is run once against a member and its lines checked.
 *
 * The others are regressions from the reviews of the cluster (REVIEW-jepsen, REVIEW-security):
 * secret: a wrong secret is refused; a long frame before the handshake closes the link at once (no 16 MiB buffered
 *   per connection); the secret file must be private; `tsdb cluster secret` makes one.
 * relay: a man in the middle of a CLI's handshake gets no link it can use: claiming another member's id in a relayed
 *   HELLO fails the transcript MAC, and a relayed handshake gives no write access (frames are tagged).
 * initcrash: an init cut short after NODE and before its first snapshot can be run again.
 * dirsync: a failed directory fsync after a snapshot is named is fail-stop.
 * install: a crash inside a snapshot install (log reset, snapshot not written) comes back from the image it kept
 *   before the reset, and catches up. reseed: the same with that image gone: it empties its log for a new image.
 * ids: one member id is never on two servers: a leader stopped in the middle of a node add, the add of another
 *   server through the next leader, the first again (the same id, idempotent); a copy of a member's NODE in another
 *   directory (another identity) is refused and does not move the member's address.
 * watch: a watcher whose voter hangs (SIGSTOP) watches another one and keeps up.
 *
 * Usage: test_cluster [SCRATCH [SCENARIO...]]   (TSDBD: the server, default ./build/tsdbd; TSDB_CLI: ./build/tsdb;
 *                                                 TSDBD_IO: epoll or uring, passed as --io)
 */
#define _GNU_SOURCE
#include "cluster.h"
#include "cvol_check.h"
#include "harness.h"
#include "meta.h"
#include "peerlink.h"
#include "raft/raft.h"
#include "raft/raft_log.h"
#include "snapshot.h"
#include "tsdb_client.h"
#include "users.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>

#define N 6
#define LIFE 5
#define WAIT_MS 30000

/* cluster.c's frame types on the wire, for the relay */
#define W_HELLO 1
#define W_AUTH 3
#define W_ADMIN 10
#define W_ADMIN_REPLY 11

static char root[PATH_MAX], base[PATH_MAX + 64], secret_path[PATH_MAX + 16];
static const char *scen = "";
static uint8_t secret[128];
static size_t secret_len;
static pid_t pids[N];
static int pports[N], stopped[N];
static const char *envs[N]; /* TSDB_CLUSTER_TEST of server i */
static const char *xargs[N]; /* more tsdbd options of server i, separated by spaces (--rack R, --power-feed F) */
static const char *penv[N];  /* environment of server i: NAME=VALUE, separated by spaces (LD_PRELOAD of crashfs) */
static int is_witness[N];    /* server i runs --witness: no --dir, no volumes */
static uint32_t ids[N];     /* member id of server i (0 until it is a member) */

static void die(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "test_cluster [%s]: FAIL: ", scen);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n  (server logs in %s/n*.log)\n", base);
    va_end(ap);
    static int dying;
    if (!dying++) { /* a server a sanitizer stopped (halt_on_error) shows only as a timeout here: name its report */
        for (int i = 0; i < N; i++) {
            char path[PATH_MAX + 96], ln[512];
            snprintf(path, sizeof path, "%s/n%d.log", base, i);
            FILE *fp = fopen(path, "r");
            int shown = -1;
            while (fp && shown < 12 && fgets(ln, sizeof ln, fp)) {
                if (shown < 0 && (strstr(ln, "WARNING: ThreadSanitizer") || strstr(ln, "ERROR: AddressSanitizer") ||
                                  strstr(ln, "runtime error:"))) {
                    fprintf(stderr, "  -- server %d's log has a sanitizer report (%s):\n", i, path);
                    shown = 0;
                }
                if (shown >= 0) {
                    fprintf(stderr, "  %s", ln);
                    shown++;
                }
            }
            if (fp)
                fclose(fp);
        }
    }
    if (dying == 1) { /* what every live server says, for the report */
        const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
        for (int i = 0; i < N; i++)
            if (pids[i] > 0 && pports[i] && !stopped[i]) {
                char cmd[PATH_MAX * 2 + 128];
                snprintf(cmd, sizeof cmd, "%s cluster status 127.0.0.1:%d --secret-file %s 1>&2", cli, pports[i],
                         secret_path);
                fprintf(stderr, "  -- server %d:\n", i);
                if (system(cmd) != 0)
                    fprintf(stderr, "  (no status)\n");
            }
    }
    for (int i = 0; i < N; i++)
        if (pids[i] > 0)
            kill(pids[i], SIGKILL);
    exit(1);
}

/* signals only a child this test started: never pid 0 or -1 (AGENTS.md) */
static void signal_child(pid_t pid, int sig)
{
    if (pid <= 0)
        die("refusing to signal pid %d", (int)pid);
    kill(pid, sig);
}

static uint64_t now_ms(void);

/* Whether every thread of pid is in the group stop (state T in /proc/PID/task/TID/stat); names the first that is not
 * in busy, and sets *gone when the process itself is a zombie (it exited before the pause). io_uring's kernel workers
 * (iou-wrk-*, iou-sqp-*) are not counted: they need not enter the group stop and may still complete I/O queued before
 * it, so "stopped" does not cover io_uring-driven paths (tsdbd's client loop); a replica's repld and peer links use
 * plain calls and stop with the process. */
static int all_threads_stopped(pid_t pid, char *busy, size_t n, int *gone)
{
    *gone = 0;
#ifdef __linux__
    char path[64];
    snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
    DIR *d = opendir(path);
    if (!d) {
        snprintf(busy, n, "no %s: %s", path, strerror(errno));
        return 0;
    }
    int ok = 1, seen = 0;
    struct dirent *e;
    while (ok && (e = readdir(d))) {
        if (e->d_name[0] == '.')
            continue;
        char sp[300], buf[512];
        snprintf(sp, sizeof sp, "/proc/%d/task/%s/stat", (int)pid, e->d_name);
        int fd = open(sp, O_RDONLY);
        if (fd < 0)
            continue; /* the thread exited */
        ssize_t got = read(fd, buf, sizeof buf - 1);
        close(fd);
        if (got <= 0)
            continue;
        buf[got] = 0;
        char *open_p = strchr(buf, '('), *close_p = strrchr(buf, ')');
        if (!open_p || !close_p || close_p[1] != ' ')
            continue;
        *close_p = 0;
        if (!strncmp(open_p + 1, "iou-", 4))
            continue;
        seen++;
        if (close_p[2] == 'Z' && atoi(e->d_name) == (int)pid)
            *gone = 1;
        if (close_p[2] != 'T') {
            snprintf(busy, n, "thread %.20s (%.32s) in state %c", e->d_name, open_p + 1, close_p[2]);
            ok = 0;
        }
    }
    closedir(d);
    if (ok && !seen) {
        snprintf(busy, n, "no thread of %d listed", (int)pid);
        ok = 0;
    }
    return ok;
#else
    (void)pid;
    (void)busy;
    (void)n;
    return 1; /* no /proc: the stop is only as certain as the signal (docs/tasks/macos.md) */
#endif
}

/* SIGSTOP is not a stop when kill() returns: the kernel wakes ONE thread to take the signal, and only that thread,
 * once it runs, stops the others. Until then every other thread of the server runs on (on a loaded machine for ms or
 * more): a replica "paused" before a write could still receive, log and acknowledge it (closegone-above checks that
 * no replica wrote after its pause returned). So a pause returns only once every thread is stopped.
 * TEST_CLUSTER_PAUSE_NOWAIT=1 returns right after kill(), as the old harness did; TEST_CLUSTER_PAUSE_TRACE=1 logs
 * what was still running (with NOWAIT that one look at /proc delays the return, which narrows the window). */
static void pause_server(int i, int on)
{
    signal_child(pids[i], on ? SIGSTOP : SIGCONT);
    stopped[i] = on;
    if (!on)
        return;
    const char *nw = getenv("TEST_CLUSTER_PAUSE_NOWAIT"), *tr = getenv("TEST_CLUSTER_PAUSE_TRACE");
    int nowait = nw && *nw == '1', trace = tr && *tr == '1';
    if (nowait && !trace)
        return;
    char busy[160] = "", first[160] = "";
    uint64_t t0 = now_ms();
    int polls = 0, gone;
    while (!all_threads_stopped(pids[i], busy, sizeof busy, &gone)) {
        if (gone) { /* a crash point or a fail-stop: name the exit, never wait 10 s for a stop that cannot come */
            int st = 0;
            if (waitpid(pids[i], &st, WNOHANG) == pids[i]) {
                pids[i] = 0;
                stopped[i] = 0;
                die("server %d exited (status 0x%x) before its pause", i, st);
            }
            die("server %d (pid %d) is a zombie before its pause", i, (int)pids[i]);
        }
        if (!polls++)
            snprintf(first, sizeof first, "%s", busy);
        if (nowait)
            break;
        if (now_ms() - t0 > 10000)
            die("server %d (pid %d) not stopped 10 s after SIGSTOP: %s", i, (int)pids[i], busy);
        nanosleep(&(struct timespec){0, 200000}, NULL);
    }
    if (trace)
        fprintf(stderr, "  pause server %d (pid %d): %s after %d polls, %" PRIu64 " ms%s%s\n", i, (int)pids[i],
                nowait ? "not waited" : "every thread stopped", polls, now_ms() - t0, *first ? "; at first " : "",
                first);
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void server_dir(int i, char *out, size_t n) { snprintf(out, n, "%s/n%d", base, i); }

static int auth_on[N]; /* server i runs with login (no --no-auth): its root has USERS */
/* Servers run with TSDB_REPLICA_READS=1 (replicas holding a lease answer reads, so a scenario can count a replica's
 * rows) unless a scenario sets strict_reads: then every read goes to the leader (D5), as in production. */
static int strict_reads;
static int fwd_on; /* the servers forward requests for volumes led elsewhere (tsdbd's default; else --forward off) */
static int cports[N];  /* the client port of server i */

/* Starts server i; returns th_await's answer (the client port, or TH_EXITED with th_status, or TH_BAD). */
static int start_try(int i)
{
    char dir[PATH_MAX + 80], pf[PATH_MAX + 128], log[PATH_MAX + 80], flag[64];
    server_dir(i, dir, sizeof dir);
    snprintf(log, sizeof log, "%s/n%d.log", base, i);
    th_portfile(pf, sizeof pf, dir);
    th_follow_me(flag, sizeof flag);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    pid_t pid = fork();
    if (pid < 0)
        die("fork: %s", strerror(errno));
    if (pid == 0) {
        int fd = open(log, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0)
            dup2(fd, 2);
        if (strict_reads)
            unsetenv("TSDB_REPLICA_READS");
        else
            setenv("TSDB_REPLICA_READS", "1", 1);
        if (envs[i])
            setenv("TSDB_CLUSTER_TEST", envs[i], 1);
        else
            unsetenv("TSDB_CLUSTER_TEST");
        char penvbuf[PATH_MAX * 2 + 256];
        snprintf(penvbuf, sizeof penvbuf, "%s", penv[i] ? penv[i] : "");
        for (char *t = strtok(penvbuf, " "); t; t = strtok(NULL, " ")) {
            char *eq = strchr(t, '=');
            if (eq) {
                *eq = 0;
                setenv(t, eq + 1, 1);
            }
        }
        char state[PATH_MAX + 90];
        snprintf(state, sizeof state, "%s/state", dir);
        const char *argv[40];
        int n = 0;
        argv[n++] = "tsdbd";
        if (!auth_on[i])
            argv[n++] = "--no-auth";
        argv[n++] = flag;
        if (is_witness[i])
            argv[n++] = "--witness";
        else {
            argv[n++] = "--dir";
            argv[n++] = dir;
            argv[n++] = "--port";
            argv[n++] = "0";
        }
        argv[n++] = "--port-file";
        argv[n++] = pf;
        argv[n++] = "--cluster";
        argv[n++] = "--cluster-secret-file";
        argv[n++] = secret_path;
        argv[n++] = "--state-dir";
        argv[n++] = state;
        if (!fwd_on) { /* the scenarios written before forwarding (S2-2) judge s2e's MOVED off the leader */
            argv[n++] = "--forward";
            argv[n++] = "off";
        }
        if (getenv("TSDBD_IO") && !is_witness[i]) { /* as test_server: epoll or uring (default: tsdbd's choice) */
            argv[n++] = "--io";
            argv[n++] = getenv("TSDBD_IO");
        }
        char extra[256];
        snprintf(extra, sizeof extra, "%s", xargs[i] ? xargs[i] : "");
        for (char *t = strtok(extra, " "); t && n < 38; t = strtok(NULL, " "))
            argv[n++] = t;
        argv[n] = NULL;
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    pids[i] = pid;
    stopped[i] = 0;
    int port = th_await(pf, pid, 20000);
    if (port == TH_EXITED)
        pids[i] = 0; /* reaped */
    return port;
}

static void start(int i)
{
    int port = start_try(i);
    if (port <= 0)
        die("server %d: %s", i, th_why);
    cports[i] = port;
    if (!th_pport)
        die("server %d: the port file names no peer port", i);
    pports[i] = th_pport;
}

static void stop(int i, int sig)
{
    if (stopped[i])
        pause_server(i, 0);
    signal_child(pids[i], sig);
    int status;
    if (waitpid(pids[i], &status, 0) != pids[i])
        die("waitpid %d: %s", (int)pids[i], strerror(errno));
    if (sig == SIGTERM && !(WIFEXITED(status) && WEXITSTATUS(status) == 0))
        die("server %d did not stop cleanly on SIGTERM (status 0x%x)", i, status);
    pids[i] = 0;
}

/* waits up to ms for server i to exit on its own: its exit status (128 + signal), or -1 when it did not */
static int await_exit(int i, int ms)
{
    uint64_t t0 = now_ms();
    for (;;) {
        int st;
        pid_t r = waitpid(pids[i], &st, WNOHANG);
        if (r == pids[i]) {
            pids[i] = 0;
            return WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st);
        }
        if (r < 0)
            die("waitpid %d: %s", (int)pids[i], strerror(errno));
        if (now_ms() - t0 > (uint64_t)ms)
            return -1;
        usleep(50000);
    }
}

static int log_has(int i, const char *needle)
{
    char path[PATH_MAX + 80];
    snprintf(path, sizeof path, "%s/n%d.log", base, i);
    FILE *f = fopen(path, "r");
    if (!f)
        return 0;
    char line[4096];
    int hit = 0;
    while (!hit && fgets(line, sizeof line, f))
        hit = strstr(line, needle) != NULL;
    fclose(f);
    return hit;
}

static uint32_t isr_of(int via, const char *name, uint64_t *version);
static void await_isr(int via, const char *name, uint32_t n, int ms, const char *what);

/* stops every live server (SIGTERM, status 0) and starts a new scenario in its own directory */
static void stop_all(void)
{
    for (int i = 0; i < N; i++)
        if (pids[i] > 0)
            stop(i, SIGTERM);
}

static void scenario(const char *name)
{
    stop_all();
    scen = name;
    memset(ids, 0, sizeof ids);
    memset(pports, 0, sizeof pports);
    memset(stopped, 0, sizeof stopped);
    memset(envs, 0, sizeof envs);
    memset(xargs, 0, sizeof xargs);
    memset(penv, 0, sizeof penv);
    strict_reads = 0;
    fwd_on = 0;
    memset(is_witness, 0, sizeof is_witness);
    memset(auth_on, 0, sizeof auth_on);
    snprintf(base, sizeof base, "%s/%s", root, name);
    if (mkdir(base, 0755) != 0)
        die("mkdir %s: %s", base, strerror(errno));
    printf(" %s\n", name);
    fflush(stdout);
}

static void peer_of(int i, char *out, size_t n) { snprintf(out, n, "127.0.0.1:%d", pports[i]); }

/* One admin op to server i; follows NOTLEADER once when `follow`. Returns the op's result; dies when unreachable.
 * The deadline is 30 s: the five servers of a scenario under AddressSanitizer on a machine that other gates keep busy
 * answered a STATUS after more than 10 s (test-trust gate tt4-asan: "closed the connection" at [feed], the deadline
 * of the handshake passing between its two halves); a hung server is the watchdog's to end, not this deadline's. */
static int admin(int i, int op, const void *req, size_t rl, int follow, uint8_t **reply, size_t *len)
{
    char target[META_ADDR_MAX + 1], err[1024];
    peer_of(i, target, sizeof target);
    for (int hop = 0;; hop++) {
        int rc;
        *reply = NULL;
        *len = 0;
        if (cluster_admin(target, secret, secret_len, op, req, rl, 30000, &rc, reply, len, err, sizeof err) != 0)
            die("admin op %d to %s: %s", op, target, err);
        if (rc == CLUSTER_ERR_NOTLEADER && follow && hop == 0 && *len > 0 && *len < sizeof target) {
            memcpy(target, *reply, *len);
            target[*len] = 0;
            free(*reply);
            continue;
        }
        return rc;
    }
}

struct view {
    struct cluster_status st;
    struct meta_state m;
    raft_config cfg;
    uint8_t *raw;
};

static void view_free(struct view *v)
{
    cluster_status_free(&v->st);
    meta_free(&v->m);
    free(v->raw);
}

/* STATUS of server i, its image decoded (an empty image leaves m empty) */
static void view(int i, struct view *v)
{
    size_t len;
    memset(v, 0, sizeof *v);
    meta_init(&v->m);
    int rc = admin(i, CLUSTER_OP_STATUS, NULL, 0, 0, &v->raw, &len);
    if (rc != 0)
        die("status of server %d: %s", i, cluster_strerror(rc));
    if (cluster_status_parse(v->raw, len, &v->st) != 0)
        die("status of server %d does not parse", i);
    if (v->st.image_len && meta_snapshot_decode(&v->m, v->st.image, v->st.image_len) != META_OK)
        die("the image of server %d does not decode", i);
    if (v->m.config_len && !raft_config_decode(v->m.config, v->m.config_len, &v->cfg))
        die("the configuration of server %d does not decode", i);
}

static unsigned voters(const struct view *v) { return raft_config_voters(&v->cfg); }

static int is_voter(const struct view *v, uint32_t id)
{
    const raft_member *m = raft_config_find(&v->cfg, id);
    return m && !m->learner;
}

static int leader_index(const struct view *v)
{
    for (int i = 0; i < N; i++)
        if (pids[i] > 0 && ids[i] && ids[i] == v->st.leader)
            return i;
    return -1;
}

/* Waits until `pred` holds for the view of server i (polling every 100 ms); dies naming `what` after ms. */
static void await_view_ms(int i, int (*pred)(const struct view *, void *), void *arg, const char *what, int ms)
{
    uint64_t t0 = now_ms();
    for (;;) {
        struct view v;
        view(i, &v);
        int ok = pred(&v, arg);
        uint64_t applied = v.st.applied, commit = v.st.commit;
        uint32_t leader = v.st.leader;
        unsigned nv = voters(&v);
        view_free(&v);
        if (ok)
            return;
        if (now_ms() - t0 > (uint64_t)ms)
            die("server %d: %s did not happen within %d ms (commit %" PRIu64 ", applied %" PRIu64 ", leader %" PRIu32
                ", %u voters)",
                i, what, ms, commit, applied, leader, nv);
        usleep(100000);
    }
}

static void await_view(int i, int (*pred)(const struct view *, void *), void *arg, const char *what)
{
    await_view_ms(i, pred, arg, what, WAIT_MS);
}

static int p_leader_self(const struct view *v, void *a)
{
    (void)a;
    return v->st.mode == CLUSTER_MODE_CORE && v->st.role == RAFT_LEADER && v->st.leader == v->st.self &&
           v->m.initialised;
}

static int p_voters(const struct view *v, void *a)
{
    return v->st.mode == CLUSTER_MODE_CORE && v->st.role == RAFT_LEADER && voters(v) == *(unsigned *)a &&
           v->cfg.n == *(unsigned *)a; /* no learner left over */
}

struct state_want {
    uint32_t id;
    int state;
    uint64_t min_incarnation;
};
static int p_member_state(const struct view *v, void *a)
{
    const struct state_want *w = a;
    const struct meta_member *mb = meta_member(&v->m, w->id);
    return mb && mb->state == w->state && mb->incarnation >= w->min_incarnation;
}

struct catch_up {
    uint64_t applied, hash;
};
static int p_caught_up(const struct view *v, void *a)
{
    const struct catch_up *c = a;
    /* not stale: a restarted voter shows its persisted copy, equal to the leader's, until its core has applied that far */
    return !v->st.stale && v->st.applied >= c->applied && v->m.applied >= c->applied &&
           (v->m.applied != c->applied || meta_hash(&v->m) == c->hash);
}

static size_t node_add_req(int newi, uint8_t *req)
{
    char np[META_ADDR_MAX];
    peer_of(newi, np, sizeof np);
    size_t n = strlen(np);
    req[0] = (uint8_t)n;
    req[1] = (uint8_t)(n >> 8);
    memcpy(req + 2, np, n);
    return n + 2;
}

static void node_add(int via, int newi)
{
    char np[META_ADDR_MAX];
    peer_of(newi, np, sizeof np);
    uint8_t req[META_ADDR_MAX + 2];
    size_t n = node_add_req(newi, req);
    uint8_t *reply;
    size_t len;
    int rc = admin(via, CLUSTER_OP_NODE_ADD, req, n, 1, &reply, &len);
    free(reply);
    if (rc != 0)
        die("node add of server %d through server %d: %s", newi, via, cluster_strerror(rc));
    /* its id: the member whose peer address is the new server's */
    struct view v;
    view(via, &v);
    for (uint32_t k = 0; k < v.m.nmembers; k++)
        if (!strcmp(v.m.members[k].peer, np))
            ids[newi] = v.m.members[k].id;
    view_free(&v);
    if (!ids[newi]) {
        /* `via` may be a watcher a little behind: ask the new server itself */
        uint64_t t0 = now_ms();
        while (!ids[newi] && now_ms() - t0 < WAIT_MS) {
            view(newi, &v);
            if (v.st.self && v.st.mode != CLUSTER_MODE_NONE)
                ids[newi] = v.st.self;
            view_free(&v);
            usleep(100000);
        }
    }
    if (!ids[newi])
        die("server %d got no member id", newi);
    printf("  server %d joined as member %" PRIu32 "\n", newi, ids[newi]);
}

static uint32_t init_min_isr; /* nonzero: INIT's min_isr (the volumes' min_isr, at most their replicas) */
static uint32_t init_node_timeout_ms; /* > 0: init_mode sets node_timeout to this instead of 1.5 s */
static uint32_t init_retain_s; /* nonzero: INIT's repl_retain_s */
static int init_r4_set; /* nonzero: INIT's four settings of R4 off their defaults (snapversion reads them back) */
static uint32_t init_isr_lag_ms; /* nonzero: INIT's isr_lag_ms */
static uint32_t init_rebuild_mb_s, init_rebuild_max; /* nonzero: INIT's rebuild_mb_s, rebuild_max */

/* INIT on server i with fast timings (down after 1.5 s, a dead voter replaced after replace_s); it leads */
static void init_mode(int i, uint32_t replace_s, int mode)
{
    struct meta_settings s;
    meta_settings_default(&s);
    if (init_min_isr)
        s.min_isr = init_min_isr;
    if (init_retain_s)
        s.repl_retain_s = init_retain_s;
    if (init_isr_lag_ms)
        s.isr_lag_ms = init_isr_lag_ms;
    if (init_rebuild_mb_s)
        s.rebuild_mb_s = init_rebuild_mb_s;
    if (init_rebuild_max)
        s.rebuild_max = init_rebuild_max;
    s.node_timeout_ms = init_node_timeout_ms ? init_node_timeout_ms : 1500;
    s.heartbeat_ms = 200;
    s.voter_replace_s = replace_s;
    if (init_r4_set) {
        s.rebuild_mb_s = 300;
        s.rebuild_max = 3;
        s.transfer_wait_ms = 2500;
        s.leader_balance_s = 60;
    }
    uint8_t req[128], *reply;
    size_t len;
    size_t rl = cluster_settings_encode(&s, mode, req);
    int rc = admin(i, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != 0)
        die("init: %s", cluster_strerror(rc));
    ids[i] = 1;
    await_view(i, p_leader_self, NULL, "leading the initialised cluster");
}

static void init_cluster(int i, uint32_t replace_s) { init_mode(i, replace_s, META_MODE_REPLICATED); }
static void init_fast(int i) { init_cluster(i, 3); }

/* Every live server's STATUS agrees on the leader and, once caught up, on the state (meta_hash). */
static void check_all_agree(int leader_i, uint32_t members)
{
    struct view lv;
    view(leader_i, &lv);
    struct catch_up c = {lv.m.applied, meta_hash(&lv.m)};
    uint32_t leader = lv.st.leader;
    view_free(&lv);
    for (int i = 0; i < N; i++) {
        if (pids[i] <= 0)
            continue;
        await_view(i, p_caught_up, &c, "catching up with the leader's state");
        struct view v;
        view(i, &v);
        if (v.st.self != ids[i])
            die("server %d says it is member %" PRIu32 ", expected %" PRIu32, i, v.st.self, ids[i]);
        if (v.st.mode == CLUSTER_MODE_CORE && v.st.leader != leader)
            die("server %d (core) names leader %" PRIu32 ", the leader says %" PRIu32, i, v.st.leader, leader);
        if (members && v.m.nmembers != members)
            die("server %d sees %" PRIu32 " members, expected %" PRIu32, i, v.m.nmembers, members);
        /* no stalled disk here: a leader that hands leadership on has misread a sync as stalled (the clock read
         * before the sync began made its age 2^64 - x: has1-cl6, leadership flapping and a reseed that never ended) */
        if (log_has(i, "handing leadership"))
            die("server %d handed leadership on although no disk stalled", i);
        view_free(&v);
    }
}

static void cli_status(int i, int members)
{
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char cmd[PATH_MAX * 2 + 128], peer[META_ADDR_MAX], line[1024];
    peer_of(i, peer, sizeof peer);
    snprintf(cmd, sizeof cmd, "%s cluster status %s --secret-file %s", cli, peer, secret_path);
    FILE *p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    int node = 0, group = 0, up = 0;
    while (fgets(line, sizeof line, p)) {
        node += !strncmp(line, "node ", 5);
        group += !strncmp(line, "group voters ", 13);
        up += !strncmp(line, "member ", 7) && strstr(line, " up ");
    }
    int st = pclose(p);
    if (st != 0 || node != 1 || group != 1 || up != members)
        die("`%s`: status %d, %d node, %d group, %d up member lines (expected 0, 1, 1, %d)", cmd, st, node, group,
            up, members);
}

/* the leader as server i's view names it, or -1 */
static int leader_of(int i)
{
    struct view v;
    view(i, &v);
    int l = leader_index(&v);
    view_free(&v);
    return l;
}

/* ------------------------------------------------------------------ raw links (the relay, long frames) */

static int dial_peer(int i)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)pports[i])};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || connect(fd, (struct sockaddr *)&a, sizeof a) != 0)
        die("connect to server %d: %s", i, strerror(errno));
    return fd;
}

static void send_all(int fd, const void *p, size_t n)
{
    const uint8_t *b = p;
    while (n) {
        ssize_t w = send(fd, b, n, MSG_NOSIGNAL);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return; /* the other side closed: what the caller checks next */
        b += w;
        n -= (size_t)w;
    }
}

/* 0, -1 at EOF or an error, -2 after ms */
static int read_full(int fd, void *p, size_t n, int ms)
{
    uint64_t end = now_ms() + (uint64_t)ms;
    size_t got = 0;
    while (got < n) {
        uint64_t t = now_ms();
        if (t >= end)
            return -2;
        struct pollfd pf = {.fd = fd, .events = POLLIN};
        int r = poll(&pf, 1, (int)(end - t));
        if (r < 0 && errno == EINTR)
            continue;
        if (r == 0)
            return -2;
        ssize_t k = read(fd, (uint8_t *)p + got, n - got);
        if (k < 0 && errno == EINTR)
            continue;
        if (k <= 0)
            return -1;
        got += (size_t)k;
    }
    return 0;
}

/* one whole frame, its length prefix included: 0, -1 (closed), -2 (nothing within ms) */
static int raw_frame(int fd, int ms, uint8_t **f, size_t *flen)
{
    uint8_t h[4];
    int r = read_full(fd, h, 4, ms);
    if (r)
        return r;
    uint32_t n = (uint32_t)h[0] | (uint32_t)h[1] << 8 | (uint32_t)h[2] << 16 | (uint32_t)h[3] << 24;
    if (n == 0 || n > (1u << 24))
        return -1;
    *f = malloc(4 + (size_t)n);
    if (!*f)
        die("out of memory");
    memcpy(*f, h, 4);
    r = read_full(fd, *f + 4, n, ms);
    if (r) {
        free(*f);
        return r;
    }
    *flen = 4 + (size_t)n;
    return 0;
}

static void frame_len(uint8_t *f, size_t flen)
{
    uint32_t n = (uint32_t)(flen - 4);
    for (int k = 0; k < 4; k++)
        f[k] = (uint8_t)(n >> (8 * k));
}

/* Reads frames from fd until it closes. 1 when an ADMIN_REPLY came, 0 when it closed without one, 2 when it stayed
 * open for ms. */
static int closes_without_reply(int fd, int ms)
{
    uint64_t end = now_ms() + (uint64_t)ms;
    for (;;) {
        uint64_t t = now_ms();
        if (t >= end)
            return 2;
        uint8_t *f;
        size_t fl;
        int r = raw_frame(fd, (int)(end - t), &f, &fl);
        if (r == -1)
            return 0;
        if (r == -2)
            return 2;
        int reply = f[4] == W_ADMIN_REPLY;
        free(f);
        if (reply)
            return 1;
    }
}

/* forwards bytes both ways until one side closes or ms pass */
static void pump_both(int a, int b, int ms)
{
    uint64_t end = now_ms() + (uint64_t)ms;
    uint8_t buf[65536];
    while (now_ms() < end) {
        struct pollfd pf[2] = {{.fd = a, .events = POLLIN}, {.fd = b, .events = POLLIN}};
        int r = poll(pf, 2, 100);
        if (r < 0 && errno != EINTR)
            return;
        for (int k = 0; k < 2; k++)
            if (pf[k].revents) {
                ssize_t n = read(pf[k].fd, buf, sizeof buf);
                if (n <= 0)
                    return;
                send_all(pf[1 - k].fd, buf, (size_t)n);
            }
    }
}

enum { RELAY_PLAIN, RELAY_CLAIM, RELAY_SWAP };

/* A CLI (a child process) asks STATUS of a relay this test runs, which connects to server `target`. RELAY_PLAIN
 * forwards everything: the CLI gets its answer (the control: the relay itself works). RELAY_CLAIM rewrites the CLI's
 * HELLO to claim member `cid` (its uuid, identity and peer address, with the CLI's own nonce) and passes the answer
 * back: the CLI must refuse it, and the server must answer nothing the relay sends after it. RELAY_SWAP forwards the
 * handshake as it is and puts a request of its own in place of the CLI's (with the CLI's tag): the server must close
 * the link without an answer. Returns the CLI's exit status (0: it got a STATUS reply). */
static int relay(int target, int mode, const struct view *v, uint32_t cid)
{
    int ls = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in a = {.sin_family = AF_INET};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t al = sizeof a;
    if (ls < 0 || bind(ls, (struct sockaddr *)&a, sizeof a) != 0 || listen(ls, 4) != 0 ||
        getsockname(ls, (struct sockaddr *)&a, &al) != 0)
        die("relay socket: %s", strerror(errno));
    pid_t kid = fork();
    if (kid < 0)
        die("fork: %s", strerror(errno));
    if (kid == 0) {
        char addr[64], err[512];
        snprintf(addr, sizeof addr, "127.0.0.1:%d", ntohs(a.sin_port));
        int rc;
        uint8_t *rep = NULL;
        size_t rl;
        int r = cluster_admin(addr, secret, secret_len, CLUSTER_OP_STATUS, NULL, 0, 3000, &rc, &rep, &rl, err,
                              sizeof err);
        _exit(r == 0 && rc == 0 ? 0 : 2);
    }
    struct pollfd pf = {.fd = ls, .events = POLLIN};
    if (poll(&pf, 1, 5000) != 1)
        die("the CLI did not connect to the relay");
    int cli = accept(ls, NULL, NULL);
    close(ls);
    int srv = dial_peer(target);
    uint8_t *f, *g;
    size_t fl, gl;
    if (cli < 0 || raw_frame(cli, 5000, &f, &fl) != 0 || f[4] != W_HELLO || fl < 80)
        die("relay: no HELLO from the CLI");
    if (mode == RELAY_CLAIM) {
        /* [len][type][magic 4][ver 1][id 4][uuid 16][ident 16][nonce 32][peer: u16 + bytes][kind 1] */
        const struct meta_member *m = meta_member(&v->m, cid);
        if (!m)
            die("relay: no member %" PRIu32, cid);
        size_t pl = strlen(m->peer);
        g = malloc(81 + pl);
        if (!g)
            die("out of memory");
        memcpy(g, f, 10);
        for (int k = 0; k < 4; k++)
            g[10 + k] = (uint8_t)(cid >> (8 * k));
        memcpy(g + 14, v->m.uuid, 16);
        memcpy(g + 30, m->ident, 16);
        memcpy(g + 46, f + 46, 32); /* the CLI's nonce */
        g[78] = (uint8_t)pl;
        g[79] = (uint8_t)(pl >> 8);
        memcpy(g + 80, m->peer, pl);
        g[80 + pl] = f[fl - 1]; /* the link kind (HELLO of protocol 4 ends with it) */
        gl = 81 + pl;
        frame_len(g, gl);
        free(f);
        f = g;
        fl = gl;
    }
    send_all(srv, f, fl);
    free(f);
    if (raw_frame(srv, 5000, &f, &fl) != 0)
        die("relay: no HELLO_ACK from server %d (mode %d)", target, mode);
    send_all(cli, f, fl);
    free(f);
    if (mode == RELAY_PLAIN) {
        pump_both(cli, srv, 5000);
    } else {
        /* what the CLI sends next: AUTH and its request (none when it refused the HELLO_ACK) */
        uint8_t *auth = NULL, *req = NULL;
        size_t authl = 0, reql = 0;
        if (raw_frame(cli, 1000, &auth, &authl) == 0 && raw_frame(cli, 1000, &req, &reql) != 0)
            req = NULL;
        if (mode == RELAY_SWAP && (!auth || !req || auth[4] != W_AUTH || req[4] != W_ADMIN || reql != 4 + 38))
            die("relay: the CLI sent no AUTH and request after a handshake passed through unchanged");
        if (!auth) {
            authl = 4 + 1 + 32;
            auth = calloc(1, authl);
            if (!auth)
                die("out of memory");
            auth[4] = W_AUTH;
            frame_len(auth, authl);
        }
        send_all(srv, auth, authl);
        /* the relay's own request: STATUS, request id 7, with the CLI's tag (or zeros) */
        uint8_t own[4 + 38] = {0};
        own[4] = W_ADMIN;
        own[5] = CLUSTER_OP_STATUS;
        own[6] = 7;
        if (req && reql == sizeof own)
            memcpy(own + 10, req + 10, 32);
        frame_len(own, sizeof own);
        if (mode == RELAY_CLAIM && req)
            send_all(srv, req, reql); /* the CLI took the forged handshake: what its own request gets shows it */
        else
            send_all(srv, own, sizeof own);
        free(auth);
        free(req);
        int r = closes_without_reply(srv, 3000);
        if (r == 1)
            die("a relayed handshake (mode %s) got an admin reply from server %d",
                mode == RELAY_CLAIM ? "claiming another member" : "with a request of the relay's", target);
        if (r == 2)
            die("server %d kept a relayed link (mode %d) open after a frame that does not authenticate", target, mode);
    }
    close(cli);
    close(srv);
    int st;
    if (waitpid(kid, &st, 0) != kid)
        die("waitpid: %s", strerror(errno));
    return WIFEXITED(st) ? WEXITSTATUS(st) : 128;
}

/* ------------------------------------------------------------------ scenarios */

static void scen_life(void)
{
    scenario("life");
    for (int i = 0; i < LIFE; i++)
        start(i);
    printf("  %d servers up\n", LIFE);

    init_fast(0);
    {
        struct meta_settings s;
        meta_settings_default(&s);
        uint8_t req[128], *reply;
        size_t len, rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req);
        int rc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
        free(reply);
        if (rc != CLUSTER_ERR_EXISTS)
            die("a second init answered %d (%s), expected EXISTS", rc, cluster_strerror(rc));
    }
    unsigned want = 1;
    await_view(0, p_voters, &want, "one voter");
    /* a server in no cluster refuses NODE_ADD as not the leader, with no leader to name */
    {
        uint8_t r2[META_ADDR_MAX + 2], *reply;
        size_t len, n = node_add_req(2, r2);
        int rc = admin(1, CLUSTER_OP_NODE_ADD, r2, n, 0, &reply, &len);
        free(reply);
        if (rc == 0)
            die("server 1, in no cluster, accepted a NODE_ADD");
    }
    printf("  initialised; member 1 leads\n");

    node_add(0, 1);
    node_add(0, 2);
    want = 3;
    await_view(0, p_voters, &want, "three voters with three members");
    printf("  three members, three voters\n");

    /* the fourth through a member that is not the leader: NOTLEADER, then the leader */
    node_add(1, 3);
    {
        struct view v;
        view(3, &v);
        int watching = v.st.mode == CLUSTER_MODE_WATCH;
        view_free(&v);
        want = 3;
        await_view(0, p_voters, &want, "three voters with four members");
        /* with four members one watches; it is server 3 unless the leader made it a voter in place of another */
        struct view lv;
        view(0, &lv);
        int watcher = -1;
        for (int i = 0; i < 4; i++)
            if (!raft_config_find(&lv.cfg, ids[i]))
                watcher = i;
        struct catch_up c = {lv.m.applied, meta_hash(&lv.m)};
        view_free(&lv);
        if (watcher < 0)
            die("four members and no watcher");
        await_view(watcher, p_caught_up, &c, "the watcher catching up");
        view(watcher, &v);
        if (v.st.mode != CLUSTER_MODE_WATCH)
            die("server %d is in no configuration but in mode %d", watcher, v.st.mode);
        view_free(&v);
        printf("  four members: server %d watches and caught up (server 3 %s at first)\n", watcher,
               watching ? "watched" : "did not watch");
    }
    node_add(0, 4);
    want = 5;
    await_view(0, p_voters, &want, "five voters with five members");
    printf("  five members, five voters\n");
    check_all_agree(0, LIFE);
    cli_status(2, LIFE);
    printf("  status agrees on every server; the CLI prints it\n");

    /* SIGKILL of the leader */
    struct view lv;
    view(0, &lv);
    int li = leader_index(&lv);
    uint64_t old_term = lv.st.term;
    view_free(&lv);
    if (li < 0)
        die("no live server leads");
    uint32_t dead = ids[li];
    struct state_want w = {dead, META_UP, 1};
    {
        struct view v;
        view(li, &v);
        w.min_incarnation = meta_member(&v.m, dead)->incarnation + 1;
        view_free(&v);
    }
    stop(li, SIGKILL);
    printf("  killed the leader, server %d (member %" PRIu32 ", term %" PRIu64 ")\n", li, dead, old_term);
    int other = (li + 1) % LIFE, ni = -1;
    uint64_t t0 = now_ms();
    while (ni < 0) {
        struct view v;
        view(other, &v);
        if (v.st.leader && v.st.leader != dead && v.st.term > old_term) {
            for (int i = 0; i < N; i++)
                if (pids[i] > 0 && ids[i] == v.st.leader)
                    ni = i;
        }
        view_free(&v);
        if (ni < 0 && now_ms() - t0 > WAIT_MS)
            die("no new leader within %d ms", WAIT_MS);
        if (ni < 0)
            usleep(100000);
    }
    await_view(ni, p_leader_self, NULL, "the new leader leading");
    printf("  server %d (member %" PRIu32 ") leads after %" PRIu64 " ms\n", ni, ids[ni], now_ms() - t0);
    struct state_want down = {dead, META_DOWN, 0};
    await_view(ni, p_member_state, &down, "the dead member marked down");
    /* no healthy server is left to replace it with (all four are voters), so it just goes; the leader removes no
     * healthy voter to reach the odd target of three (4 voters lose one as 3 do) */
    want = 4;
    await_view(ni, p_voters, &want, "four voters without the dead one");
    {
        struct view v;
        view(ni, &v);
        if (is_voter(&v, dead))
            die("the dead member %" PRIu32 " is still a voter", dead);
        view_free(&v);
    }
    printf("  member %" PRIu32 " down and out of the voters\n", dead);

    /* back on the same directory: incarnation + 1, up, a voter again */
    start(li);
    await_view(ni, p_member_state, &w, "the restarted member up with a new incarnation");
    want = 5;
    {
        /* the leader may change while the group grows: ask whoever leads */
        uint64_t t1 = now_ms();
        for (;;) {
            int l = leader_of(ni);
            if (l >= 0) {
                struct view x;
                view(l, &x);
                int ok = p_voters(&x, &want) && is_voter(&x, dead);
                view_free(&x);
                if (ok) {
                    ni = l;
                    break;
                }
            }
            if (now_ms() - t1 > WAIT_MS)
                die("the restarted member did not become a voter again within %d ms", WAIT_MS);
            usleep(100000);
        }
    }
    printf("  member %" PRIu32 " back: up, incarnation %" PRIu64 ", five voters\n", dead, w.min_incarnation);
    check_all_agree(ni, LIFE);
}

static void scen_secret(void)
{
    scenario("secret");
    start(0);
    /* another secret: refused in the handshake */
    uint8_t other[32];
    memcpy(other, "another-secret-of-32-bytes-00000", 32);
    char addr[META_ADDR_MAX], err[1024];
    peer_of(0, addr, sizeof addr);
    int rc;
    uint8_t *rep = NULL;
    size_t rl;
    if (cluster_admin(addr, other, sizeof other, CLUSTER_OP_STATUS, NULL, 0, 3000, &rc, &rep, &rl, err,
                      sizeof err) == 0)
        die("a STATUS with a wrong secret was answered (rc %d)", rc);
    if (!strstr(err, "secret"))
        die("a wrong secret failed with \"%s\", not a word of the secret", err);
    printf("  a wrong secret is refused: %s\n", err);
    /* The message names the cause: a peer that is slow (the deadline passed) and one that closed the link looked alike,
     * which left the tt4-asan failure of [feed] undecidable. A listener that never answers, and one that closes. */
    for (int closes = 0; closes < 2; closes++) {
        int lfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        struct sockaddr_in la = {.sin_family = AF_INET};
        la.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t ll = sizeof la;
        if (lfd < 0 || bind(lfd, (struct sockaddr *)&la, sizeof la) != 0 || listen(lfd, 4) != 0 ||
            getsockname(lfd, (struct sockaddr *)&la, &ll) != 0)
            die("listener for the cause test: %s", strerror(errno));
        char ls[64];
        snprintf(ls, sizeof ls, "127.0.0.1:%d", (int)ntohs(la.sin_port));
        pid_t kid = closes ? fork() : 0;
        if (kid == 0 && closes) { /* accepts one link and closes it at once, with the HELLO unread */
            int a = accept(lfd, NULL, NULL);
            if (a >= 0)
                close(a);
            _exit(0);
        }
        if (cluster_admin(ls, secret, secret_len, CLUSTER_OP_STATUS, NULL, 0, 400, &rc, &rep, &rl, err, sizeof err) == 0)
            die("a listener that %s gave an answer", closes ? "closes" : "never answers");
        const char *cause = strerror(closes ? ECONNRESET : ETIMEDOUT);
        if (!strstr(err, cause))
            die("a peer that %s: \"%s\", the cause (%s) is not named", closes ? "closes the link" : "never answers", err,
                cause);
        if (kid > 0)
            waitpid(kid, NULL, 0);
        close(lfd);
    }
    /* a long frame before the handshake: closed at once, never buffered */
    int fd = dial_peer(0);
    uint8_t h[5] = {0, 0, 0x10, 0, W_HELLO}; /* 1 MiB */
    uint8_t junk[4096] = {0};
    uint64_t t0 = now_ms();
    send_all(fd, h, sizeof h);
    send_all(fd, junk, sizeof junk);
    uint8_t x[64];
    int r;
    while ((r = read_full(fd, x, sizeof x, 4000)) == 0)
        ;
    uint64_t took = now_ms() - t0;
    close(fd);
    if (r == -2 || took > 1000)
        die("a 1 MiB frame before the handshake kept the connection for %" PRIu64 " ms", took);
    printf("  a 1 MiB frame before the handshake: closed after %" PRIu64 " ms\n", took);
    /* the secret file: only its owner may read it */
    char p[sizeof base + 32];
    snprintf(p, sizeof p, "%s/open-secret", base);
    FILE *f = fopen(p, "w");
    if (!f || fputs("0123456789abcdef0123456789abcdef\n", f) < 0 || fclose(f) != 0)
        die("writing %s", p);
    chmod(p, 0644);
    uint8_t buf[CLUSTER_SECRET_MAX];
    size_t len;
    if (cluster_read_secret(p, buf, sizeof buf, &len, err, sizeof err) == 0)
        die("a secret file of mode 0644 was accepted");
    snprintf(p, sizeof p, "%s/made-secret", base);
    struct stat sb;
    if (cluster_make_secret(p, err, sizeof err) != 0)
        die("cluster_make_secret: %s", err);
    if (stat(p, &sb) != 0 || (sb.st_mode & 0777) != 0600)
        die("a made secret has mode %o", (unsigned)(sb.st_mode & 0777));
    if (cluster_read_secret(p, buf, sizeof buf, &len, err, sizeof err) != 0 || len < 32)
        die("a made secret does not read back: %s", err);
    if (cluster_make_secret(p, err, sizeof err) == 0)
        die("cluster_make_secret overwrote an existing file");
    printf("  a secret file of mode 0644 is refused; a made one is 0600 and reads back\n");
}

static void scen_relay(void)
{
    scenario("relay");
    start(0);
    start(1);
    init_fast(0);
    node_add(0, 1);
    struct view v;
    view(0, &v);
    struct catch_up c = {v.m.applied, meta_hash(&v.m)};
    await_view(1, p_caught_up, &c, "catching up");
    if (relay(1, RELAY_PLAIN, &v, 0) != 0)
        die("the CLI got no answer through a relay that changes nothing (the relay itself is broken)");
    printf("  a relay that changes nothing passes the CLI's STATUS (the control)\n");
    if (relay(1, RELAY_CLAIM, &v, ids[0]) == 0)
        die("the CLI accepted a relayed handshake that claimed member %" PRIu32 " to server 1", ids[0]);
    printf("  a relayed HELLO that claims member %" PRIu32 ": the CLI refuses, server 1 answers nothing\n", ids[0]);
    if (relay(1, RELAY_SWAP, &v, 0) == 0)
        die("the CLI got an answer although the relay replaced its request");
    printf("  a relayed handshake with the relay's own request: server 1 drops the link\n");
    view_free(&v);
}

static void meta_tmp(int i, int make)
{
    char p[PATH_MAX + 128];
    snprintf(p, sizeof p, "%s/n%d/state/snapshot.tmp", base, i);
    if ((make ? mkdir(p, 0755) : rmdir(p)) != 0)
        die("%s %s: %s", make ? "mkdir" : "rmdir", p, strerror(errno));
}

/* a crash between NODE and the first snapshot of an init (a directory in the way of the snapshot's temporary file
 * fail-stops the server there): the next init finishes it */
static void scen_initcrash(void)
{
    scenario("initcrash");
    start(0);
    meta_tmp(0, 1);
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 1500;
    s.heartbeat_ms = 200;
    uint8_t req[128], *rep = NULL;
    size_t rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req), len;
    char addr[META_ADDR_MAX], err[1024];
    peer_of(0, addr, sizeof addr);
    int rc;
    if (cluster_admin(addr, secret, secret_len, CLUSTER_OP_INIT, req, rl, 5000, &rc, &rep, &len, err,
                      sizeof err) == 0) {
        free(rep);
        if (rc == 0)
            die("an init whose snapshot cannot be written succeeded");
    }
    int st = await_exit(0, 10000);
    if (st != 1 || !log_has(0, "cannot write"))
        die("the init that could not write its snapshot ended with status %d", st);
    meta_tmp(0, 0);
    start(0);
    init_fast(0);
    printf("  an init cut short before its snapshot is finished by the next init\n");
}

/* a failed directory fsync after a snapshot is named stops the server (status 1) */
static void scen_dirsync(void)
{
    scenario("dirsync");
    envs[0] = "snap_every=2,fail_snap_dirsync=1";
    start(0);
    start(1);
    /* the init, then a node add, make the entries a snapshot is due after (the server may stop before it answers) */
    struct meta_settings s;
    meta_settings_default(&s);
    uint8_t req[META_ADDR_MAX + 64], *rep = NULL;
    size_t n = cluster_settings_encode(&s, META_MODE_REPLICATED, req), len;
    char addr[META_ADDR_MAX], err[1024];
    peer_of(0, addr, sizeof addr);
    int rc;
    if (cluster_admin(addr, secret, secret_len, CLUSTER_OP_INIT, req, n, 5000, &rc, &rep, &len, err, sizeof err) == 0)
        free(rep);
    n = node_add_req(1, req);
    if (await_exit(0, 1000) < 0 &&
        cluster_admin(addr, secret, secret_len, CLUSTER_OP_NODE_ADD, req, n, 5000, &rc, &rep, &len, err,
                      sizeof err) == 0)
        free(rep);
    int st = pids[0] ? await_exit(0, WAIT_MS) : 1;
    if (st != 1 || !log_has(0, "cannot sync"))
        die("a failed directory fsync after a snapshot: status %d (expected 1 with \"cannot sync\" in the log)", st);
    printf("  a failed directory fsync after naming a snapshot stops the server\n");
}

/* a crash inside a snapshot install: the log is reset to the image's index, the snapshot not written (a directory in
 * the way of its temporary file makes restore() stop there). The restarted server must catch up, not sit at applied
 * 0 over a log based past its snapshot: from the image it kept before the reset (install), or, that image gone too
 * (damage; reseed), by emptying its log for a new image. */
enum { KEPT_TAKEN, KEPT_GONE, HEADER_TORN };

static void install_crash(int mode)
{
    int drop_kept = mode == KEPT_GONE;
    scenario(mode == KEPT_GONE ? "reseed" : mode == HEADER_TORN ? "torn" : "install");
    for (int i = 0; i < 3; i++)
        envs[i] = "snap_every=2,snap_retain=1,seg_bytes=200"; /* small segments: compaction drops whole ones */
    for (int i = 0; i < 3; i++)
        start(i);
    init_fast(0);
    node_add(0, 1); /* two members: one voter, server 1 watches */
    meta_tmp(2, 1);
    node_add(0, 2); /* three: servers 1 and 2 become learners, and the leader's log begins after 1: installs */
    int st = await_exit(2, WAIT_MS);
    if (st != 1 || !log_has(2, "cannot write"))
        die("the learner whose snapshot cannot be written ended with status %d", st);
    meta_tmp(2, 0);
    char kept[PATH_MAX + 128];
    snprintf(kept, sizeof kept, "%s/n2/state/snapshot.install", base);
    if (access(kept, F_OK) != 0)
        die("server 2 stopped inside an install without the image kept in %s", kept);
    if (drop_kept && unlink(kept) != 0)
        die("cannot remove %s: %s", kept, strerror(errno));
    if (mode == HEADER_TORN) {
        /* the crash inside the reset's last step: the segment the log starts again on is named, its header is not
         * durable. It is the lone segment, named for the image's index + 1, which the snapshot (still the old one)
         * is not at: the open refused it as damage before it knew the store the reset was made for. */
        char dir[PATH_MAX + 128], seg[PATH_MAX + 256], name[300] = "";
        snprintf(dir, sizeof dir, "%s/n2/state/raft", base);
        DIR *d = opendir(dir);
        int nseg = 0;
        for (struct dirent *e; d && (e = readdir(d));)
            if (!strncmp(e->d_name, "log-", 4)) {
                snprintf(name, sizeof name, "%s", e->d_name);
                nseg++;
            }
        if (d)
            closedir(d);
        if (nseg != 1)
            die("the install crash left %d log segments in %s, expected the one the reset made", nseg, dir);
        snprintf(seg, sizeof seg, "%s/%s", dir, name);
        if (truncate(seg, 0) != 0)
            die("cannot truncate %s: %s", seg, strerror(errno));
    }
    start(2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    check_all_agree(0, 3);
    if (drop_kept) {
        if (!log_has(2, "fencing the vote"))
            die("server 2 did not take the path of an unfinished install (the test proves nothing)");
        printf("  a crash inside an install, the kept image gone: the server empties its log, gets a new image and "
               "catches up\n");
        return;
    }
    if (!log_has(2, "taking the install image kept in"))
        die("server 2 did not take the image it kept for the unfinished install (the test proves nothing)");
    if (log_has(2, "fencing the vote"))
        die("server 2 emptied its log although it had kept the image");
    if (mode == HEADER_TORN)
        printf("  a crash inside an install, the new segment's header torn: the server takes the image it kept, and "
               "catches up\n");
    else
        printf("  a crash inside an install: the server takes the image it kept, and catches up\n");
}

static void scen_install(void) { install_crash(KEPT_TAKEN); }
static void scen_reseed(void) { install_crash(KEPT_GONE); }
static void scen_torn(void) { install_crash(HEADER_TORN); }

/* A log that ends below the snapshot beside it, as a reset stopped between its deletes (or a lazily written durable
 * floor and a rotten tail) leaves: the newest segments are removed from a follower's directory. The start makes the
 * snapshot the log's base (the store holds what the dropped entries did) and the follower catches up from the leader,
 * where it used to boot with a log ending below its applied index (proposals at an index the store has applied). */
static void scen_rebase(void)
{
    scenario("rebase");
    for (int i = 0; i < 3; i++)
        envs[i] = "snap_every=2,snap_retain=1000,seg_bytes=200"; /* every entry stays: no installs */
    for (int i = 0; i < 3; i++)
        start(i);
    init_fast(0);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    check_all_agree(0, 3);
    stop(2, SIGKILL);
    char dir[PATH_MAX + 128], path[PATH_MAX + 256];
    snprintf(path, sizeof path, "%s/n2/state/snapshot", base);
    FILE *f = fopen(path, "rb");
    uint8_t img[1 << 16];
    size_t len = f ? fread(img, 1, sizeof img, f) : 0;
    if (f)
        fclose(f);
    struct meta_state m;
    meta_init(&m);
    if (!len || meta_snapshot_decode(&m, img, len) != META_OK)
        die("server 2 has no snapshot to put the log below (%s)", path);
    uint64_t applied = m.applied;
    meta_free(&m);
    if (applied < 4)
        die("server 2's snapshot is at %" PRIu64 ", too early to put the log below", applied);
    snprintf(dir, sizeof dir, "%s/n2/state/raft", base);
    DIR *d = opendir(dir);
    int kept = 0, removed = 0;
    for (struct dirent *e; d && (e = readdir(d));) {
        unsigned long long first;
        if (sscanf(e->d_name, "log-%llu", &first) != 1)
            continue;
        if (first + 1 >= applied) { /* the segment holds an entry at or above applied - 1: the log must end lower */
            snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
            if (unlink(path) != 0)
                die("cannot remove %s: %s", path, strerror(errno));
            removed++;
        } else
            kept++;
    }
    if (d)
        closedir(d);
    /* the durable floor (the hard state's note of what was synced and committed) is what refuses a log cut below
     * it as rot; a reset lowers it to 0 first, so a log it left cut is open to this start. The test lowers it the
     * only way a file can: no hard state (this server was a learner for a moment; its term and vote come back from
     * the leader's next message). */
    snprintf(path, sizeof path, "%s/hard", dir);
    if (unlink(path) != 0)
        die("cannot remove %s: %s", path, strerror(errno));
    if (!kept || !removed)
        die("could not cut server 2's log below its snapshot at %" PRIu64 " (%d kept, %d removed)", applied, kept,
            removed);
    start(2);
    await_view(0, p_voters, &want, "three voters");
    check_all_agree(0, 3);
    if (!log_has(2, "starting the log on it"))
        die("server 2 did not start its log on the snapshot (the test proves nothing)");
    printf("  a log that ends below the snapshot: the server starts it on the snapshot and catches up\n");
}

/* An install that takes longer than voter_replace_s (here: the new servers drop the leader's images for 6 s). A core
 * booted by the leader's first APPEND has an empty configuration until the image arrives; it must keep sending
 * heartbeats, or the leader marks it down, removes it as a learner, and never adds it again (seen once under TSan on a
 * loaded machine: a learner at applied 0, "not heard" for 30 s). */
static void scen_slowinstall(void)
{
    scenario("slowinstall");
    envs[0] = "snap_every=2,snap_retain=1,seg_bytes=200";
    envs[1] = envs[2] = "snap_every=2,snap_retain=1,seg_bytes=200,hold_install_ms=6000";
    for (int i = 0; i < 3; i++)
        start(i);
    init_fast(0);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters after installs slower than voter_replace_s");
    check_all_agree(0, 3);
    if (log_has(0, "removes learner"))
        die("the leader removed a learner whose install was on its way");
    printf("  installs slower than voter_replace_s: the learners stay heard, are kept, and become voters\n");
}

/* the leader stopped while a node add is in flight; the next leader adds another server; the first again */
static void scen_ids(void)
{
    scenario("ids");
    for (int i = 0; i < 5; i++)
        start(i);
    init_fast(0);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    const int A = 3, B = 4, L = 0;
    pause_server(A, 1);
    pid_t kid = fork();
    if (kid < 0)
        die("fork: %s", strerror(errno));
    if (kid == 0) {
        uint8_t req[META_ADDR_MAX + 2], *rep = NULL;
        size_t n = node_add_req(A, req), len;
        char addr[META_ADDR_MAX], err[1024];
        peer_of(L, addr, sizeof addr);
        int rc;
        cluster_admin(addr, secret, secret_len, CLUSTER_OP_NODE_ADD, req, n, 8000, &rc, &rep, &len, err, sizeof err);
        _exit(0);
    }
    usleep(500000);
    pause_server(L, 1);
    int nl = -1;
    uint64_t t0 = now_ms();
    while (nl < 0) {
        nl = leader_of(1);
        if (nl == L)
            nl = -1;
        if (nl < 0 && now_ms() - t0 > WAIT_MS)
            die("no leader in place of the stopped one within %d ms", WAIT_MS);
        if (nl < 0)
            usleep(100000);
    }
    pause_server(L, 0);
    pause_server(A, 0);
    int st;
    waitpid(kid, &st, 0);
    node_add(nl, B);
    node_add(nl, A);
    if (ids[A] == ids[B])
        die("servers %d and %d are both member %" PRIu32, A, B, ids[A]);
    uint32_t a1 = ids[A];
    node_add(nl, A); /* again: the same id */
    if (ids[A] != a1)
        die("a second node add of server %d gave member %" PRIu32 " after %" PRIu32, A, ids[A], a1);
    int l = leader_of(nl);
    check_all_agree(l < 0 ? nl : l, 5);
    for (int i = 0; i < 5; i++) {
        struct view v;
        view(i, &v);
        for (uint32_t x = 0; x < v.m.nmembers; x++)
            for (uint32_t y = x + 1; y < v.m.nmembers; y++)
                if (!memcmp(v.m.members[x].ident, v.m.members[y].ident, 16))
                    die("members %" PRIu32 " and %" PRIu32 " have one identity", v.m.members[x].id,
                        v.m.members[y].id);
        view_free(&v);
    }
    printf("  a node add cut short by a new leader: servers %d and %d are members %" PRIu32 " and %" PRIu32 "\n", A, B,
           ids[A], ids[B]);

    /* an impostor: member 2's NODE in another directory (another identity) */
    char src[PATH_MAX + 128], dst[PATH_MAX + 128];
    snprintf(dst, sizeof dst, "%s/n5", base);
    mkdir(dst, 0755);
    snprintf(dst, sizeof dst, "%s/n5/state", base);
    mkdir(dst, 0755);
    snprintf(src, sizeof src, "%s/n1/state/NODE", base);
    snprintf(dst, sizeof dst, "%s/n5/state/NODE", base);
    {
        FILE *in = fopen(src, "rb"), *out = fopen(dst, "wb");
        char b[4096];
        size_t k;
        if (!in || !out)
            die("copying %s: %s", src, strerror(errno));
        while ((k = fread(b, 1, sizeof b, in)) > 0)
            fwrite(b, 1, k, out);
        fclose(in);
        if (fclose(out) != 0)
            die("writing %s", dst);
    }
    l = leader_of(nl);
    if (l < 0)
        die("no leader");
    struct view v;
    view(l, &v);
    char peer1[META_ADDR_MAX];
    snprintf(peer1, sizeof peer1, "%s", meta_member(&v.m, ids[1])->peer);
    view_free(&v);
    start(5);
    usleep(4000000); /* more than two node timeouts of heartbeats */
    view(l, &v);
    const struct meta_member *m = meta_member(&v.m, ids[1]);
    if (strcmp(m->peer, peer1) != 0 || m->state != META_UP)
        die("an impostor of member %" PRIu32 " moved its record to %s (state %d)", ids[1], m->peer, m->state);
    view_free(&v);
    int refused = 0;
    for (int i = 0; i < 5; i++) {
        char needle[64];
        snprintf(needle, sizeof needle, "claims node %" PRIu32, ids[1]);
        refused |= log_has(i, needle);
    }
    if (!refused)
        die("no member logged that it refused the impostor of member %" PRIu32, ids[1]);
    stop(5, SIGKILL);
    printf("  a copy of member %" PRIu32 "'s NODE elsewhere is refused; the member's address stays\n", ids[1]);
}

/* a watcher whose voter hangs: SIGSTOP of each voter in turn; the watcher must see that voter marked down */
static void scen_watch(void)
{
    scenario("watch");
    for (int i = 0; i < 4; i++)
        start(i);
    init_cluster(0, 600); /* a stopped voter stays a voter: each of the three is stopped in turn */
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    node_add(0, 3);
    await_view(0, p_voters, &want, "three voters with four members");
    struct view lv;
    view(0, &lv);
    int watcher = -1, vs[3], nv = 0;
    for (int i = 0; i < 4; i++)
        if (!raft_config_find(&lv.cfg, ids[i]))
            watcher = i;
        else if (nv < 3)
            vs[nv++] = i;
    view_free(&lv);
    if (watcher < 0 || nv != 3)
        die("four members, no watcher");
    for (int k = 0; k < 3; k++) {
        int v = vs[k], u = vs[(k + 1) % 3];
        pause_server(v, 1);
        struct state_want down = {ids[v], META_DOWN, 0};
        await_view(u, p_member_state, &down, "the stopped voter marked down");
        struct view x;
        view(u, &x);
        struct catch_up c = {x.m.applied, meta_hash(&x.m)};
        view_free(&x);
        /* a node timeout (1.5 s) behind, then another voter: well within 10 s */
        await_view_ms(watcher, p_caught_up, &c, "the watcher keeping up while a voter hangs", 10000);
        pause_server(v, 0);
        struct state_want up = {ids[v], META_UP, 0};
        await_view(u, p_member_state, &up, "the voter up again");
        printf("  voter %" PRIu32 " stopped: the watcher kept up\n", ids[v]);
    }
}

/* M11 (item 2): the leader's disk stalls (its log sync hangs, for hours on a hard mount). The cluster thread must keep
 * answering and heartbeating (the member is never marked down), and after 2 s (stall_ms=1500 here) the leader hands
 * leadership to another voter. The knob holds the worker's sync while <state-dir>/stall exists. */
static void scen_stall(void)
{
    scenario("stall");
    for (int i = 0; i < 4; i++) {
        envs[i] = "sync_stall_file=1,stall_ms=1500";
        start(i);
    }
    init_cluster(0, 600); /* a silent voter stays a voter */
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    int L = leader_of(0);
    if (L < 0 || L > 2)
        die("no leader among the voters");
    int U = (L + 1) % 3;
    char path[PATH_MAX + 160];
    snprintf(path, sizeof path, "%s/n%d/state/stall", base, L);
    FILE *f = fopen(path, "w");
    if (!f || fclose(f) != 0)
        die("making %s", path);
    /* a proposal on the stalled leader: its sync round never ends. The answer may time out or be refused. */
    char addr[META_ADDR_MAX], err[1024];
    peer_of(L, addr, sizeof addr);
    uint8_t req[META_ADDR_MAX + 2], *rep = NULL;
    size_t rl = node_add_req(3, req), replen = 0;
    int rc;
    cluster_admin(addr, secret, secret_len, CLUSTER_OP_NODE_ADD, req, rl, 1500, &rc, &rep, &replen, err,
                  sizeof err);
    free(rep);
    /* the thread is not blocked on the disk: STATUS answers at once, 2.5 s into the stall */
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < 2500)
        usleep(50000);
    uint64_t q0 = now_ms();
    struct view v;
    view(L, &v);
    view_free(&v);
    if (now_ms() - q0 > 1000)
        die("STATUS of the stalled leader took %" PRIu64 " ms: the cluster thread waits for the disk", now_ms() - q0);
    /* leadership has moved to another voter */
    uint64_t t1 = now_ms();
    int nl = -1;
    while (now_ms() - t1 < 10000) {
        view(U, &v);
        nl = leader_index(&v);
        view_free(&v);
        if (nl >= 0 && nl != L)
            break;
        usleep(100000);
    }
    if (nl < 0 || nl == L)
        die("the stalled leader did not hand leadership on within 10 s");
    printf("  leader %d stalled: server %d leads after %" PRIu64 " ms\n", L, nl, now_ms() - t0);
    if (!log_has(L, "handing leadership"))
        die("the stalled leader says nothing of handing leadership on");
    /* well past the 1.5 s of node_timeout: its heartbeats still arrive */
    t0 = now_ms();
    while (now_ms() - t0 < 3000)
        usleep(50000);
    struct state_want up = {ids[L], META_UP, 0};
    await_view_ms(nl, p_member_state, &up, "the stalled node still up (its heartbeats flow)", 3000);
    unlink(path);
    view(nl, &v);
    struct catch_up c = {v.m.applied, meta_hash(&v.m)};
    view_free(&v);
    await_view_ms(L, p_caught_up, &c, "the node catching up once its disk returns", 10000);
    printf("  the stall ended: server %d is a follower again and caught up\n", L);
}


/* A witness (item 7): a cluster member with no --dir, no engine and no volumes. It holds a Raft core (a voter), its
 * member record says volumes_ok 0, and nothing in its directory looks like a volume. */
static void scen_witness(void)
{
    scenario("witness");
    start(0);
    is_witness[1] = 1;
    xargs[1] = "--peer-port 0"; /* a witness takes the peer port 7071 unless told */
    char wdir[PATH_MAX + 160];
    snprintf(wdir, sizeof wdir, "%s/n1", base); /* no --dir makes it; the state directory goes inside */
    if (mkdir(wdir, 0755) != 0)
        die("mkdir %s: %s", wdir, strerror(errno));
    start(1);
    start(2);
    init_fast(0);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters (the witness is one)");
    struct view v;
    view(0, &v);
    const struct meta_member *w = meta_member(&v.m, ids[1]);
    if (!w || (w->flags & META_MF_VOLUMES_OK) || !(w->flags & META_MF_VOTER_OK))
        die("the witness's record has flags %u, expected voter_ok without volumes_ok", w ? (unsigned)w->flags : 99u);
    const struct meta_member *m0 = meta_member(&v.m, ids[0]);
    if (!m0 || !(m0->flags & META_MF_VOLUMES_OK))
        die("a normal member's record lacks volumes_ok");
    view_free(&v);
    char path[PATH_MAX + 160];
    snprintf(path, sizeof path, "%s/n1/SERIES", base);
    if (access(path, F_OK) == 0)
        die("the witness made %s: it holds volumes", path);
    snprintf(path, sizeof path, "%s/n1/state/NODE", base);
    if (access(path, F_OK) != 0)
        die("the witness has no NODE file in its state directory");
    printf("  a witness is a voter without volumes_ok and without a data directory\n");
}

/* Flags (item 6) and the copy (item 8): a server that reports voter_ok 0 (no room in its state directory) joins as a
 * member, is never added to the group, and, as a watcher, keeps a copy of the metadata that it serves, marked stale,
 * after a restart with the group down. */
static void scen_flags(void)
{
    scenario("flags");
    envs[2] = "free_min=9999999999999999"; /* more than any disk has: voter_ok 0 */
    envs[1] = "claim_ok=0"; /* its claims fail: claim_ok 0 in its record (the leader lifts a bar only on 0 -> 1) */
    for (int i = 0; i < 3; i++)
        start(i);
    init_fast(0);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 2;
    await_view(0, p_voters, &want, "two voters, the third server is not one");
    /* it stays out for well past voter_replace_s and the learner rounds of the leader */
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < 5000)
        usleep(100000);
    struct view v;
    view(0, &v);
    const struct meta_member *m = meta_member(&v.m, ids[2]);
    if (!m || (m->flags & META_MF_VOTER_OK) || !(m->flags & META_MF_VOLUMES_OK))
        die("server 2's record has flags %u, expected volumes_ok without voter_ok", m ? (unsigned)m->flags : 99u);
    if (v.cfg.n != 2 || raft_config_find(&v.cfg, ids[2]))
        die("a member with voter_ok 0 is in the Raft group");
    const struct meta_member *m1 = meta_member(&v.m, ids[1]);
    if (!m1 || (m1->flags & META_MF_CLAIM_OK) || !(m1->flags & META_MF_VOTER_OK))
        die("server 1's record has flags %u, expected voter_ok without claim_ok", m1 ? (unsigned)m1->flags : 99u);
    struct catch_up c = {v.m.applied, meta_hash(&v.m)};
    uint32_t members = v.m.nmembers;
    view_free(&v);
    await_view(2, p_caught_up, &c, "the watcher catching up");
    printf("  voter_ok 0 keeps a member out of the group\n");
    /* the copy: all down, then the watcher alone */
    usleep(1000000); /* the copy is written a moment after the applied entries */
    stop(2, SIGTERM);
    stop(1, SIGTERM);
    stop(0, SIGTERM);
    start(2);
    view(2, &v);
    if (!v.st.stale || v.m.nmembers != members)
        die("the restarted watcher shows stale %d and %u members, expected a stale copy with %u", (int)v.st.stale,
            (unsigned)v.m.nmembers, (unsigned)members);
    view_free(&v);
    printf("  a restarted watcher serves its copy of the metadata, marked stale\n");
}

/* Heartbeat sections (item 1): a server that reports a volume with a fault and a slow domain shows both in the
 * leader's status, and the fault timer grows while the reports keep it. */
static void scen_hbsections(void)
{
    scenario("hbsections");
    envs[1] = "vol=7/1/1/5/diskA,dom=diskA/2"; /* volume 7, role 1, health local fault, epoch 5; domain diskA FAULT */
    for (int i = 0; i < 2; i++)
        start(i);
    init_fast(0);
    node_add(0, 1);
    usleep(3500000); /* a watcher of the group reports through heartbeats as well */
    struct view v;
    view(0, &v);
    const struct cluster_heard *h = NULL;
    for (uint32_t k = 0; k < v.st.nheard; k++)
        if (v.st.heard[k].id == ids[1])
            h = &v.st.heard[k];
    if (!h || h->nvols != 1 || h->ndoms != 1)
        die("the leader's heard entry of server 1 has %u volumes and %u domains, expected 1 and 1",
            h ? (unsigned)h->nvols : 99u, h ? (unsigned)h->ndoms : 99u);
    if (h->vols[0].v.volume != 7 || h->vols[0].v.epoch != 5 || h->vols[0].v.health != 1 ||
        strcmp(h->vols[0].v.domain, "diskA") || h->doms[0].state != 2 || strcmp(h->doms[0].name, "diskA"))
        die("the volume or domain report of server 1 is not what it sent");
    if (h->vols[0].local_fault_s < 2)
        die("the fault timer of server 1's volume reads %" PRIu64 " s after 3.5 s of reports", h->vols[0].local_fault_s);
    printf("  volume and domain reports reach the leader; the fault timer runs (%" PRIu64 " s)\n",
           h->vols[0].local_fault_s);
    view_free(&v);
}

/* A Raft log fault in shared mode (item 3) ends the voter role only: the server drops its core, writes `retired`,
 * stays up as a watcher, and the leader replaces it by another member; a restart does not boot the core again. */
static void scen_retired(void)
{
    scenario("retired");
    envs[2] = "fail_sync_file=1";
    for (int i = 0; i < 4; i++)
        start(i);
    init_mode(0, 3, META_MODE_SHARED);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    char path[PATH_MAX + 160];
    snprintf(path, sizeof path, "%s/n2/state/failsync", base);
    FILE *f = fopen(path, "w");
    if (!f || fclose(f) != 0)
        die("making %s", path);
    node_add(0, 3); /* an entry: server 2 must sync it, and cannot */
    uint64_t t0 = now_ms();
    while (!log_has(2, "ends the voter role only") && now_ms() - t0 < 10000)
        usleep(100000);
    if (!log_has(2, "ends the voter role only"))
        die("server 2 did not end its voter role on a failed log sync");
    /* the log line comes before core_stop and the mark's write: wait for the mark, not just the line */
    snprintf(path, sizeof path, "%s/n2/state/retired", base);
    t0 = now_ms();
    while (access(path, F_OK) != 0 && now_ms() - t0 < 10000)
        usleep(50000);
    if (access(path, F_OK) != 0)
        die("server 2 wrote no retired mark within 10 s of its log line");
    int rc = waitpid(pids[2], NULL, WNOHANG);
    if (rc != 0)
        die("server 2 is gone: a log fault in shared mode must end the voter role only");
    /* the leader replaces it with server 3, which was added as a member beside the three */
    struct view v;
    t0 = now_ms();
    int ok = 0;
    while (now_ms() - t0 < 40000 && !ok) {
        view(0, &v);
        ok = is_voter(&v, ids[3]) && !raft_config_find(&v.cfg, ids[2]);
        view_free(&v);
        usleep(200000);
    }
    if (!ok)
        die("server 3 did not replace server 2 in the group");
    view(2, &v);
    if (v.st.mode == CLUSTER_MODE_CORE)
        die("server 2 still runs a Raft core");
    view_free(&v);
    printf("  a log fault ends the voter role: server 2 is retired, serves as a watcher, server 3 took its place\n");
    stop(2, SIGTERM);
    snprintf(path, sizeof path, "%s/n2/state/failsync", base);
    unlink(path);
    start(2);
    view(2, &v);
    if (v.st.mode == CLUSTER_MODE_CORE)
        die("a retired server booted a Raft core after its restart");
    view_free(&v);
    printf("  after a restart it still holds no core\n");
}

/* A sync round out while more entries are appended (a slow disk): the core asked for a sync once per append, and the
 * round that returns must start the next one, or the entries wait for an append that may never come (has1: a node add
 * that never answered, one start in three). Each sync takes 300 ms here, so INIT is proposed under the first round. */
static void scen_syncrace(void)
{
    scenario("syncrace");
    for (int i = 0; i < 2; i++) {
        envs[i] = "sync_delay_ms=300";
        start(i);
    }
    init_fast(0);
    node_add(0, 1);
    struct view v;
    view(0, &v);
    if (v.m.nmembers != 2)
        die("two members expected, %u", (unsigned)v.m.nmembers);
    view_free(&v);
    printf("  entries appended under an outstanding sync round are synced by the next one\n");
}

/* Power loss on the metadata state (crashfs under server 0's state directory): what the leader acknowledged, it keeps.
 * Three watchers join (the one voter commits each entry after its own sync), then the power fails at the next write
 * the server makes. After the restart the state holds everything that was acknowledged and the term has gone up.
 * Catches a log that is never synced at all (applied falls back), a hard state that is never saved (the term restarts
 * from the start) and, with `snaps` (the log is compacted behind the snapshot), a snapshot lost with its directory
 * entry where no other sync covers it; a single missing sync site is another matter, see below.
 * What each scenario catches was measured, not assumed (test-trust review,
 * 2026-11-09): with every fdatasync of raft_log.c removed `power` fails; with only the hard state's sync removed (C2)
 * `power` and `powersnap` PASS, because the term saved at the restart is synced by the log's own rounds, and only
 * `powervote` (below) fails it, in every run, and `make test` plants it (cluster-mutants). NOT caught (measured, 6 runs
 * each): the log's two fdatasync sites removed (C1) by `power`, and the snapshot's rename without its directory fsync
 * (C7, cluster.c write of the snapshot) by `powersnap`: other syncs cover them, so those two have no test yet. The clean
 * server passes every run. crashfs follows dup'ed descriptors for this (the log syncs duplicates of its segments).
 * No CRASHFS_TEAR: a tear may keep every unsynced byte, which would let the unsynced log pass now and then.
 * CRASHFS_REWRITE=old: a hard-state slot rewritten in place and not yet synced comes back as the old slot, not as zeros
 * (a blank slot beside sequence N > 1 is damage by the log's own rule, and a disk leaves old or new bytes there: with
 * zeros the restart refused to open in about 1 run in 10, 2 of 20 in tt-cluster-20b). */
static void power_loss(int snaps)
{
    scenario(snaps ? "powersnap" : "power");
    const char *so = getenv("CRASHFS_SO") ? getenv("CRASHFS_SO") : "./build/libcrashfs.so";
    if (access(so, F_OK) != 0) {
#ifdef __linux__
        /* a missing shim must not pass as a skipped test (ha-s1 review 8): make builds it with this test */
        die("no %s (make build/libcrashfs.so): the power-loss scenarios cannot run, and must not pass as skipped", so);
#else
        printf("  skipped: no %s\n", so);
        return;
#endif
    }
    /* the ASan runtime must come first in the library list: with crashfs preloaded the server refuses to start */
    int asan = 0;
#if defined(__SANITIZE_ADDRESS__)
    asan = 1;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    asan = 1;
#endif
#endif
    if (asan) {
        printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
        return;
    }
    static char pe[PATH_MAX * 2 + 128];
    char st[PATH_MAX + 160];
    snprintf(st, sizeof st, "%s/n0/state", base);
    char dir0[PATH_MAX + 160];
    snprintf(dir0, sizeof dir0, "%s/n0", base);
    if (mkdir(dir0, 0755) != 0 || mkdir(st, 0755) != 0)
        die("mkdir %s: %s", st, strerror(errno));
    snprintf(pe, sizeof pe, "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_REWRITE=old", so, st);
    penv[0] = pe;
    envs[0] = snaps ? "snap_every=2,snap_retain=1,seg_bytes=200" : NULL;
    for (int i = 1; i < 5; i++)
        envs[i] = "free_min=9999999999999999"; /* watchers: never voters, so server 0 stays the one voter */
    for (int i = 0; i < 5; i++)
        start(i);
    init_fast(0);
    for (int i = 1; i < 4; i++)
        node_add(0, i);
    /* a clean restart first: the term goes up and must be saved */
    usleep(500000);
    stop(0, SIGTERM);
    start(0);
    await_view(0, p_leader_self, NULL, "server 0 leading again");
    usleep(1500000);
    struct view v;
    view(0, &v);
    uint64_t term0 = v.st.term, applied0 = v.m.applied;
    uint32_t members0 = v.m.nmembers;
    view_free(&v);
    if (members0 != 4)
        die("server 0 has %u members before the power loss, expected 4", (unsigned)members0);
    /* the power fails at the next tracked operation: a proposal makes one */
    signal_child(pids[0], SIGUSR2);
    char addr[META_ADDR_MAX], err[1024];
    peer_of(0, addr, sizeof addr);
    uint8_t req[META_ADDR_MAX + 2], *rep = NULL;
    size_t rl = node_add_req(4, req), replen = 0;
    int rc;
    cluster_admin(addr, secret, secret_len, CLUSTER_OP_NODE_ADD, req, rl, 2000, &rc, &rep, &replen, err, sizeof err);
    free(rep);
    int status = await_exit(0, 15000);
    if (status != 99)
        die("server 0 ended with status %d after the power loss, expected 99 (crashfs)", status);
    start(0);
    await_view(0, p_leader_self, NULL, "server 0 leading after the power loss");
    /* A restarted leader knows its commit index only once an entry of its own term is committed (the one voter syncs
     * it and commits at once), so until then `applied` is the earlier copy of the state (run 17 of 20 read applied 7 of
     * 10 right after "leader"). Wait for it, bounded: a log that lost acknowledged entries never gets back to
     * applied0 (its first new entry gets applied0 - k + 1), and the check below fails with its numbers. */
    uint64_t t_applied = now_ms();
    for (;;) {
        view(0, &v);
        if ((v.m.nmembers >= members0 && v.m.applied >= applied0) || now_ms() - t_applied > 10000)
            break;
        view_free(&v);
        usleep(100000);
    }
    if (v.m.nmembers < members0 || v.m.applied < applied0)
        die("after the power loss server 0 holds %u members and applied %" PRIu64 ", it had acknowledged %u and %" PRIu64,
            (unsigned)v.m.nmembers, v.m.applied, (unsigned)members0, applied0);
    if (v.st.term <= term0)
        die("after the power loss the term is %" PRIu64 ", it was %" PRIu64 " (the hard state was not kept)", v.st.term,
            term0);
    printf("  after a power loss the leader holds all %u acknowledged members (applied %" PRIu64 " of %" PRIu64
           "), term %" PRIu64 " -> %" PRIu64 "\n",
           (unsigned)members0, v.m.applied, applied0, term0, v.st.term);
    view_free(&v);
}
static void scen_power(void) { power_loss(0); }

/* A voter's vote survives a power loss (ha-s1 review 8; the single-voter power scenario sees only the term). Three
 * voters, crashfs under n2. The leader is killed, n1 and n2 elect one in a higher term T (n2 votes for the winner or
 * for itself, saving the hard state first), then n2 loses power at its next tracked operation (a proposal's append).
 * What its state directory holds then must say it voted in T, and for whom: a hard save that kept the term but lost
 * the vote would let it vote again for another candidate in T (two leaders), and one that kept nothing is worse. */
static int powervote_round(int attempt)
{
    if (attempt == 0)
        scenario("powervote");
    else {
        char name[32];
        snprintf(name, sizeof name, "powervote%d", attempt);
        scenario(name);
    }
    const char *so = getenv("CRASHFS_SO") ? getenv("CRASHFS_SO") : "./build/libcrashfs.so";
    if (access(so, F_OK) != 0) {
#ifdef __linux__
        die("no %s (make build/libcrashfs.so): the power-loss scenarios cannot run, and must not pass as skipped", so);
#else
        printf("  skipped: no %s\n", so);
        return 1;
#endif
    }
#if defined(__SANITIZE_ADDRESS__)
    printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
    return 1;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
    return 1;
#endif
#endif
    static char pe[PATH_MAX * 2 + 128];
    char st[PATH_MAX + 160], dir2[PATH_MAX + 160];
    snprintf(dir2, sizeof dir2, "%s/n2", base);
    snprintf(st, sizeof st, "%s/n2/state", base);
    if (mkdir(dir2, 0755) != 0 || mkdir(st, 0755) != 0)
        die("mkdir %s: %s", st, strerror(errno));
    snprintf(pe, sizeof pe, "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_REWRITE=old", so, st);
    penv[2] = pe;
    envs[3] = "free_min=9999999999999999"; /* a watcher: the proposal that makes n2 append */
    for (int i = 0; i < 4; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    struct view v;
    view(0, &v);
    struct catch_up c = {v.m.applied, meta_hash(&v.m)};
    uint64_t term0 = v.st.term;
    view_free(&v);
    await_view(2, p_caught_up, &c, "n2 caught up");
    stop(0, SIGKILL);
    uint64_t t0 = now_ms(), term = 0;
    uint32_t leader = 0;
    while (now_ms() - t0 < 20000 && !leader) {
        view(2, &v);
        if (v.st.term > term0 && v.st.leader && v.st.leader != ids[0]) {
            term = v.st.term;
            leader = v.st.leader;
        }
        view_free(&v);
        if (!leader)
            usleep(100000);
    }
    if (!leader)
        die("no new leader after the first one was killed");
    if (leader == ids[2]) {
        /* n2 won the election itself: it is the leader now, and its own log syncs (its first entries) make the hard
         * state durable too, whether or not the hard save syncs. That run cannot tell a vote that was not synced
         * (planted C2: it passed in 3 of 8 runs), so the cluster is made again until n2 is the one that votes for
         * another candidate. */
        printf("  n2 won term %" PRIu64 " itself: its vote proves nothing here, trying again\n", term);
        return 0;
    }
    int li = leader == ids[1] ? 1 : 2;
    signal_child(pids[2], SIGUSR2); /* the power fails at n2's next tracked operation */
    char addr[META_ADDR_MAX], err[1024];
    peer_of(li, addr, sizeof addr);
    uint8_t req[META_ADDR_MAX + 2], *rep = NULL;
    size_t rl = node_add_req(3, req), replen = 0;
    int rc;
    cluster_admin(addr, secret, secret_len, CLUSTER_OP_NODE_ADD, req, rl, 2000, &rc, &rep, &replen, err, sizeof err);
    free(rep);
    int status = await_exit(2, 15000);
    if (status != 99)
        die("server 2 ended with status %d after the power loss, expected 99 (crashfs)", status);
    char rd[PATH_MAX + 200];
    snprintf(rd, sizeof rd, "%s/raft", st);
    raft_hard_state hs;
    memset(&hs, 0, sizeof hs);
    char lerr[512] = "";
    if (raft_log_hard_load(rd, &hs, lerr, sizeof lerr) != ISO_OK)
        die("after the power loss server 2 holds no hard state (it voted in term %" PRIu64 "): %s", term, lerr);
    if (hs.term < term || (hs.term == term && hs.vote != leader))
        die("after the power loss server 2's hard state is term %" PRIu64 " voted for %" PRIu32 ", it voted in term %" PRIu64
            " for %" PRIu32,
            (uint64_t)hs.term, (uint32_t)hs.vote, term, leader);
    printf("  a voter's vote in term %" PRIu64 " (for node %" PRIu32 ") survives a power loss\n", term, leader);
    return 1;
}
static void scen_powervote(void)
{
    for (int attempt = 0; attempt < 12; attempt++)
        if (powervote_round(attempt))
            return;
    die("n2 won its own election in 12 clusters in a row (a fair coin: 1 in 4096): the vote was never checked");
}
static void scen_powersnap(void) { power_loss(1); }

/* A power cut between a hard state save's write and its sync (jepsen-cluster kv power, seed 1, 2026-11-17: "slot 1 of the
 * raft hard state in raft/hard is blank beside sequence 6 in slot 0", and the node never started again). A disk keeps
 * the slot's old sectors or the new ones, which the reader takes; crashfs zeroed rewrites it was not told to keep, and
 * the reader rightly takes a zeroed slot beside a later save for rot. So every power-loss harness of tsdbd --cluster
 * runs crashfs with the old bytes coming back (CRASHFS_REWRITE=old, the default; main's KEEP_OLD): here a child saves six hard states (seq 6 in slot 0, seq 5 in slot 1), writes
 * the seventh save's slot 1 without its sync and loses power at exit. With the old bytes the hard state reads back as
 * term 6; with CRASHFS_REWRITE=zero it must be refused, which is the state the jepsen node was left in. */
static int hardslot_child(const char *dir)
{
    char err[512];
    raft_log *l;
    raft_log_opts o = {0};
    o.segment_bytes = RAFT_LOG_SEGMENT_MIN;
    if (raft_log_open(dir, &o, 0, 0, &l, err, sizeof err) != ISO_OK) {
        fprintf(stderr, "hardslot child: open %s: %s\n", dir, err);
        _exit(3);
    }
    for (unsigned t = 1; t <= 6; t++) {
        raft_hard_state hs = {.term = t, .vote = 1};
        if (raft_log_hard_save(l, &hs, err, sizeof err) != ISO_OK) {
            fprintf(stderr, "hardslot child: save %u: %s\n", t, err);
            _exit(3);
        }
    }
    char hp[PATH_MAX + 16];
    snprintf(hp, sizeof hp, "%s/hard", dir);
    int fd = open(hp, O_RDWR | O_CLOEXEC);
    static uint8_t slot[4096];
    memset(slot, 0xab, sizeof slot);
    if (fd < 0 || pwrite(fd, slot, sizeof slot, 4096) != (ssize_t)sizeof slot) { /* seq 7 goes to slot 1 */
        fprintf(stderr, "hardslot child: rewrite %s: %s\n", hp, strerror(errno));
        _exit(3);
    }
    exit(0); /* CRASHFS_AT_EXIT: the power fails here, before the seventh save's sync */
}

static void scen_hardslot(void)
{
    scenario("hardslot");
    const char *so = getenv("CRASHFS_SO") ? getenv("CRASHFS_SO") : "./build/libcrashfs.so";
    if (access(so, F_OK) != 0) {
#ifdef __linux__
        die("no %s (make build/libcrashfs.so): the power-loss scenarios cannot run, and must not pass as skipped", so);
#else
        printf("  skipped: no %s\n", so);
        return;
#endif
    }
#if defined(__SANITIZE_ADDRESS__)
    printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
    return;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
    return;
#endif
#endif
    char self[PATH_MAX];
    ssize_t sl = readlink("/proc/self/exe", self, sizeof self - 1);
    if (sl <= 0)
        die("readlink /proc/self/exe: %s", strerror(errno));
    self[sl] = '\0';
    for (int keep = 1; keep >= 0; keep--) {
        char top[PATH_MAX + 64], rd[PATH_MAX + 80];
        snprintf(top, sizeof top, "%s/hardslot-%d", base, keep);
        snprintf(rd, sizeof rd, "%s/raft", top);
        /* made before the child: crashfs does not track its root's own entries, and the open does not sync the parent
         * of a new directory */
        if (mkdir(top, 0755) != 0 || mkdir(rd, 0755) != 0)
            die("mkdir %s: %s", rd, strerror(errno));
        pid_t pid = fork();
        if (pid < 0)
            die("fork: %s", strerror(errno));
        if (pid == 0) {
            setenv("LD_PRELOAD", so, 1);
            setenv("CRASHFS_ROOT", top, 1);
            setenv("CRASHFS_PROB", "0", 1);
            setenv("CRASHFS_AT_EXIT", "1", 1);
            setenv("CRASHFS_REWRITE", keep ? "old" : "zero", 1); /* old bytes back (the default), or zeros (the harsher mode) */
            unsetenv("CRASHFS_TEAR");
            execl(self, self, "hardslot-child", rd, (char *)NULL);
            _exit(127);
        }
        int st = -1;
        if (waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
            die("the hardslot child (keep %d) ended with status 0x%x", keep, st);
        raft_hard_state hs;
        memset(&hs, 0, sizeof hs);
        char lerr[512] = "";
        iso_err e = raft_log_hard_load(rd, &hs, lerr, sizeof lerr);
        if (keep) {
            if (e != ISO_OK || hs.term != 6)
                die("CRASHFS_REWRITE=old: after a power cut inside the seventh hard save the state reads as rc %d term %" PRIu64
                    " (want term 6): %s",
                    (int)e, (uint64_t)hs.term, lerr);
            printf("  CRASHFS_REWRITE=old: a power cut inside a hard save leaves the previous one (term 6)\n");
        } else {
            if (e == ISO_OK)
                die("with CRASHFS_REWRITE=zero the zeroed slot read as term %" PRIu64 ": this scenario no longer reaches "
                    "the state the jepsen node was left in",
                    (uint64_t)hs.term);
            printf("  crashfs's default zeroes the slot, and the reader refuses it: %.120s\n", lerr);
        }
    }
}

/* Placement (item 10): a replacement voter comes from a power feed the voters do not use. */
static void scen_feed(void)
{
    scenario("feed");
    static const char *feed[5] = {"--power-feed A", "--power-feed A", "--power-feed B", "--power-feed A",
                                  "--power-feed C"};
    for (int i = 0; i < 5; i++) {
        xargs[i] = feed[i];
        start(i);
    }
    init_fast(0);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    node_add(0, 3); /* server 3 (feed A) joins before server 4 (feed C): the order alone would pick it */
    node_add(0, 4);
    stop(1, SIGKILL); /* a voter on feed A goes: the replacement must be the one on feed C */
    struct view v;
    uint64_t t0 = now_ms();
    int ok = 0;
    while (now_ms() - t0 < 40000 && !ok) {
        view(0, &v);
        ok = voters(&v) == 3 && v.cfg.n == 3 && !raft_config_find(&v.cfg, ids[1]);
        if (ok && (!is_voter(&v, ids[4]) || is_voter(&v, ids[3])))
            die("the replacement voter is server 3 (feed A) or not server 4 (feed C)");
        view_free(&v);
        usleep(200000);
    }
    if (!ok)
        die("the group was not repaired");
    printf("  the replacement voter comes from the feed the voters do not use\n");
}

/* REVIEW 1: a restarted member's persisted copy never goes back to an older image from a lagging voter. n2 is paused
 * while the leader commits a MEMBER_STATE and the watcher n3 persists its copy; then n3, n0 and n1 are killed, n2
 * resumed and n3 restarted: n2 is the only voter n3 can reach, and it is behind the copy. n3 used to take n2's image
 * (a persisted copy "yielded to any live image"), mark itself fresh and write the old state over its copy file. */
static int p_fresh_from(const struct view *v, void *a)
{
    return !v->st.stale && v->m.applied >= *(uint64_t *)a;
}

static void scen_copyback(void)
{
    scenario("copyback");
    envs[3] = "free_min=9999999999999999"; /* a watcher */
    for (int i = 0; i < 4; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    node_add(0, 3);
    pause_server(2, 1); /* n2 lags from here on */
    struct state_want down = {ids[2], META_DOWN, 0};
    await_view(0, p_member_state, &down, "n2 marked down");
    struct view x;
    view(0, &x);
    struct catch_up c = {x.m.applied, meta_hash(&x.m)};
    view_free(&x);
    await_view(3, p_caught_up, &c, "the watcher caught up");
    usleep(1500000); /* the copy file */
    uint64_t a = c.applied;
    stop(3, SIGKILL);
    stop(0, SIGKILL); /* the watcher's dial to them is refused at once: the lagging n2 is the one it reaches */
    stop(1, SIGKILL);
    pause_server(2, 0);
    view(2, &x);
    if (x.m.applied >= a)
        die("n2 is not behind the watcher's copy (%" PRIu64 " >= %" PRIu64 "): the test proves nothing", x.m.applied, a);
    view_free(&x);
    start(3);
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < 6000) { /* n2 answers the watcher's dial: its older image must not be taken */
        view(3, &x);
        if (x.m.applied < a || !x.st.stale)
            die("the restarted watcher took a lagging voter's image: applied %" PRIu64 " (its copy %" PRIu64
                "), stale %d",
                x.m.applied, a, (int)x.st.stale);
        view_free(&x);
        usleep(200000);
    }
    stop(3, SIGTERM); /* a copy that went back would be what this start finds */
    start(3);
    view(3, &x);
    if (x.m.applied < a || !x.st.stale)
        die("the copy file went back to applied %" PRIu64 " (was %" PRIu64 ")", x.m.applied, a);
    view_free(&x);
    printf("  a restarted watcher keeps its copy against a lagging voter's older image\n");
}

/* REVIEW 2: a voter restarted while the group is away serves its persisted copy, marked stale (it used to serve an
 * empty state, or its last snapshot, as if it were current). */
static void scen_voterstale(void)
{
    scenario("voterstale");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    struct view x;
    view(0, &x);
    struct catch_up c = {x.m.applied, meta_hash(&x.m)};
    view_free(&x);
    await_view(1, p_caught_up, &c, "n1 caught up");
    usleep(1000000);
    pause_server(0, 1); /* the group is away (paused: a restart gets a new port, and the others would not find it) */
    pause_server(2, 1);
    stop(1, SIGTERM);
    start(1);
    usleep(3000000);
    view(1, &x);
    if (!x.st.stale || x.m.applied < c.applied || x.m.nmembers != 3)
        die("the voter restarted alone shows stale %d, applied %" PRIu64 " (was %" PRIu64 "), %u members (3 expected)",
            (int)x.st.stale, x.m.applied, c.applied, x.m.nmembers);
    view_free(&x);
    pause_server(0, 0);
    pause_server(2, 0);
    await_view(1, p_fresh_from, &c.applied, "the voter confirmed once the group is back");
    printf("  a voter restarted while the group is away serves its copy, marked stale, then the live state\n");
}

/* REVIEW 4: in shared mode a fault on the state directory ends the voter role only: tsdbd and its volumes go on. */
static void scen_dirsync_shared(void)
{
    scenario("dirsyncshared");
    envs[0] = "snap_every=2,fail_snap_dirsync=1";
    start(0);
    start(1);
    struct meta_settings s;
    meta_settings_default(&s);
    uint8_t req[META_ADDR_MAX + 64], *rep = NULL;
    size_t n = cluster_settings_encode(&s, META_MODE_SHARED, req), len;
    char addr[META_ADDR_MAX], err[1024];
    peer_of(0, addr, sizeof addr);
    int rc;
    if (cluster_admin(addr, secret, secret_len, CLUSTER_OP_INIT, req, n, 5000, &rc, &rep, &len, err, sizeof err) == 0)
        free(rep);
    n = node_add_req(1, req);
    if (cluster_admin(addr, secret, secret_len, CLUSTER_OP_NODE_ADD, req, n, 5000, &rc, &rep, &len, err,
                      sizeof err) == 0)
        free(rep);
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < 10000 && !log_has(0, "ends the voter role only"))
        usleep(100000);
    if (!log_has(0, "ends the voter role only"))
        die("shared mode: a failed directory fsync after a snapshot did not end the voter role");
    if (await_exit(0, 1500) >= 0)
        die("shared mode: a failed state-directory fsync after a snapshot stopped tsdbd");
    printf("  shared mode: a failed directory fsync after a snapshot ends the voter role, tsdbd goes on\n");
}

/* REVIEW 4: the same for an install whose snapshot cannot be written (a directory in the way of its temp file). */
static void install_shared(int pend)
{
    scenario(pend ? "pendshared" : "installshared");
    for (int i = 0; i < 3; i++)
        envs[i] = "snap_every=2,snap_retain=1,seg_bytes=200";
    if (pend)
        envs[2] = "snap_every=2,snap_retain=1,seg_bytes=200,fail_pend_dirsync=1"; /* the kept image's directory sync */
    for (int i = 0; i < 3; i++)
        start(i);
    init_mode(0, 3, META_MODE_SHARED);
    node_add(0, 1);
    if (!pend)
        meta_tmp(2, 1);
    node_add(0, 2);
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < 15000 && !log_has(2, "ends the voter role only"))
        usleep(100000);
    if (!log_has(2, "ends the voter role only"))
        die("shared mode: an install that cannot be written did not end the voter role");
    if (await_exit(2, 1500) >= 0)
        die("shared mode: an install that cannot be written stopped tsdbd");
    if (!pend)
        meta_tmp(2, 0);
    printf("  shared mode: a snapshot install that cannot be %s ends the voter role, tsdbd goes on\n",
           pend ? "kept" : "written");
}
static void scen_install_shared(void) { install_shared(0); }
static void scen_pend_shared(void) { install_shared(1); }

/* REVIEW 4: a coreless member's copy file whose directory cannot be synced: replicated mode stops, shared mode logs. */
static void scen_copyfault(void)
{
    scenario("copyfault");
    envs[1] = "free_min=9999999999999999,fail_copy_dirsync=1"; /* a watcher */
    start(0);
    start(1);
    init_mode(0, 600, META_MODE_SHARED);
    node_add(0, 1);
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < 10000 && !log_has(1, "sync the directory of"))
        usleep(100000);
    if (!log_has(1, "sync the directory of"))
        die("the watcher never wrote its copy (the test proves nothing)");
    if (await_exit(1, 1500) >= 0)
        die("shared mode: a copy file whose directory cannot be synced stopped tsdbd");
    printf("  shared mode: a copy file that cannot be synced is logged, tsdbd goes on\n");
}

/* ---- replicated volumes (r3c-volumes; design-cluster.md 3.1, 8.5, DECISION D2) ---- */

/* a client library result, printed (the engine's tsdb_strerror is not linked here) */
static const char *ers(int rc)
{
    static char b[4][24];
    static int k;
    k = (k + 1) & 3;
    snprintf(b[k], sizeof b[k], "error %d", rc);
    return b[k];
}

static int vol_add(int i, const char *name, uint32_t replicas, uint32_t *number)
{
    uint8_t req[2 + META_NAME_MAX + 4], *reply;
    size_t len, rl = cluster_volume_add_encode(name, replicas, req);
    int rc = admin(i, CLUSTER_OP_VOLUME_ADD, req, rl, 1, &reply, &len);
    *number = 0;
    for (size_t k = 0; rc == 0 && k < 4 && k < len; k++)
        *number |= (uint32_t)reply[k] << (8 * k);
    free(reply);
    return rc;
}

/* volume `name` in server i's copy, or NULL (copied out: the view is freed) */
static int vol_rec(int i, const char *name, struct meta_volume *out)
{
    struct view v;
    view(i, &v);
    int found = 0;
    for (uint32_t k = 0; k < v.m.nvolumes && !found; k++)
        if (!strcmp(v.m.volumes[k].name, name)) {
            *out = v.m.volumes[k];
            out->hist = NULL;
            out->nhist = 0;
            found = 1;
        }
    view_free(&v);
    return found;
}

static const struct meta_replica *rep_of(const struct meta_volume *v, int i)
{
    for (uint32_t k = 0; k < v->nrep; k++)
        if (v->rep[k].node == ids[i])
            return &v->rep[k];
    return NULL;
}

static int server_of(uint32_t node)
{
    for (int i = 0; i < N; i++)
        if (ids[i] == node)
            return i;
    return -1;
}

/* waits until server `via`'s copy records open generation >= gen for server i (rid unchanged when rid != 0) */
static void await_open_gen(int via, const char *name, int i, uint64_t gen, uint64_t rid)
{
    uint64_t t0 = now_ms();
    struct meta_volume v;
    while (now_ms() - t0 < 30000) {
        const struct meta_replica *r;
        if (vol_rec(via, name, &v) && (r = rep_of(&v, i)) && r->open_gen >= gen) {
            if (rid && r->rid != rid)
                die("server %d's replica of %s changed rid %016" PRIx64 " -> %016" PRIx64, i, name, rid, r->rid);
            return;
        }
        usleep(100000);
    }
    die("server %d's open generation %" PRIu64 " of %s was not recorded", i, gen, name);
}

static int log_count(int i, const char *needle)
{
    char path[PATH_MAX + 80], line[4096];
    snprintf(path, sizeof path, "%s/n%d.log", base, i);
    FILE *f = fopen(path, "r");
    int hits = 0;
    while (f && fgets(line, sizeof line, f))
        hits += strstr(line, needle) != NULL;
    if (f)
        fclose(f);
    return hits;
}

static void await_log_count(int i, const char *needle, int want, int ms)
{
    uint64_t t0 = now_ms();
    while (log_count(i, needle) < want) {
        if (now_ms() - t0 > (uint64_t)ms)
            die("server %d: no %d lines with \"%s\" in %d ms", i, want, needle, ms);
        usleep(50000);
    }
}

static void slurp(const char *path, char *buf, size_t n)
{
    FILE *f = fopen(path, "r");
    if (!f)
        die("%s: %s", path, strerror(errno));
    size_t got = fread(buf, 1, n - 1, f);
    buf[got] = 0;
    fclose(f);
}

/* the REPL file of server i's v<number>: rid and open generation of the slot with the newer file generation (the
 * layout of src/replfile.c, which this test does not link: two 4 KiB slots, magic "DS_BREP2" at 0, generation at 8,
 * rid at 72, open generation at 80; the checksum is the server's to check) */
static void repl_of(int i, uint32_t number, uint64_t *rid, uint64_t *gen)
{
    char dir[PATH_MAX + 80], path[PATH_MAX + 160];
    uint8_t pg[2][4096];
    server_dir(i, dir, sizeof dir);
    snprintf(path, sizeof path, "%s/v%" PRIu32 "/REPL", dir, number);
    FILE *f = fopen(path, "r");
    if (!f)
        die("%s: %s", path, strerror(errno));
    size_t got = fread(pg, 1, sizeof pg, f);
    fclose(f);
    if (got != sizeof pg)
        die("%s: %zu bytes, not two slots", path, got);
    uint64_t g[2];
    for (int k = 0; k < 2; k++) {
        memcpy(&g[k], pg[k] + 8, 8);
        if (memcmp(pg[k], "DS_BREP2", 8) != 0)
            die("%s: slot %d has no REPL magic", path, k);
    }
    int n = g[1] > g[0];
    memcpy(rid, pg[n] + 72, 8);
    memcpy(gen, pg[n] + 80, 8);
}

static int nrows;
static int count_rows(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ctx, (void)ts, (void)v;
    nrows += (int)n;
    return 0;
}

static tsdbc *client(int i)
{
    tsdbc *c;
    int rc = tsdbc_connect("127.0.0.1", cports[i], &c);
    if (rc != 0)
        die("connect to server %d: %s", i, ers(rc));
    tsdbc_set_retry(c, 0); /* the test looks at every UNAVAILABLE */
    return c;
}

/* rc is MOVED, one entry: volume `vol` to node `node` at server s's client address (the copy's MEMBER_ADDR) */
static void expect_moved(tsdbc *c, int rc, uint32_t vol, uint32_t node, int s, const char *what)
{
    tsdbc_err_info e;
    char want[64];
    snprintf(want, sizeof want, "127.0.0.1:%d", cports[s]);
    if (rc != TSDB_ERR_MOVED || tsdbc_last_error(c, &e) != TSDB_ERR_MOVED || e.nmoved != 1 || e.moved[0].vol != vol ||
        e.moved[0].node_id != node || strcmp(e.moved[0].addr, want) != 0)
        die("%s: %s, %u MOVED (volume %u node %u at %s), want MOVED of volume %u to node %u at %s", what, ers(rc),
            rc == TSDB_ERR_MOVED ? e.nmoved : 0, rc == TSDB_ERR_MOVED && e.nmoved ? e.moved[0].vol : 0,
            rc == TSDB_ERR_MOVED && e.nmoved ? (unsigned)e.moved[0].node_id : 0,
            rc == TSDB_ERR_MOVED && e.nmoved ? e.moved[0].addr : "-", (unsigned)vol, (unsigned)node, want);
}

/* rc is UNAVAILABLE and its body names volume `vol` (TSDB_VOL_ANY: any) and reason `why` */
static void expect_unavail(tsdbc *c, int rc, uint16_t vol, int why, const char *what)
{
    tsdb_unavail e;
    unsigned n = 0;
    if (rc != TSDB_ERR_UNAVAILABLE)
        die("%s: %s, not UNAVAILABLE", what, ers(rc));
    if (tsdbc_unavailable(c, &e, 1, &n) != 0 || n != 1 || (vol != TSDB_VOL_ANY && e.vol != vol) || e.reason != why)
        die("%s: UNAVAILABLE names volume %u reason %u (%u entries), not volume %u reason %d", what, n ? e.vol : 0,
            n ? e.reason : 0, n, vol, why);
}

/* rc is server s's answer for volume `name` (number vol) that is not open there (held, refused, stale): the entry
 * node decides from the copy alone (route_decide), so the volume's leader answers UNAVAILABLE (RECOVERING) and any
 * other server MOVED to that leader, whatever its own directory holds */
static void expect_held(tsdbc *c, int rc, int s, const char *name, uint16_t vol, const char *what)
{
    struct meta_volume w;
    if (!vol_rec(0, name, &w) || w.number != vol)
        die("%s: the record of %s is missing", what, name);
    int ls = server_of(w.leader);
    if (ls < 0)
        die("%s: the leader of %s is node %u, no server", what, name, (unsigned)w.leader);
    if (ls == s)
        expect_unavail(c, rc, vol, TSDB_WHY_RECOVERING, what);
    else
        expect_moved(c, rc, vol, w.leader, ls, what);
}

/* expect_moved with the epoch checked (design-cluster.md 6.3): a write to a replicated volume on a server that does not lead it, naming
 * volume `vol` and its leader `node` at `epoch` with that server's client address */
static void expect_moved_ep(tsdbc *c, int rc, uint16_t vol, uint32_t node, uint64_t epoch, const char *what)
{
    tsdbc_err_info e;
    if (rc != TSDB_ERR_MOVED)
        die("%s: %s, not MOVED", what, ers(rc));
    if (tsdbc_last_error(c, &e) != TSDB_ERR_MOVED || !e.has_body || e.nmoved != 1)
        die("%s: MOVED without one entry (body %d, %u entries)", what, e.has_body, e.nmoved);
    int s = server_of(node);
    char port[16];
    snprintf(port, sizeof port, ":%d", s >= 0 ? cports[s] : -1);
    size_t al = strlen(e.moved[0].addr), pl = strlen(port);
    if (e.moved[0].vol != vol || e.moved[0].node_id != node || e.moved[0].epoch != epoch || al < pl ||
        strcmp(e.moved[0].addr + al - pl, port) != 0)
        die("%s: MOVED names volume %u, node %u at epoch %u, address %s; not volume %u, node %u at epoch %" PRIu64
            ", port %s", what, e.moved[0].vol, e.moved[0].node_id, e.moved[0].epoch, e.moved[0].addr, vol, node, epoch,
            port + 1);
}

/* a write of one row to a server that no longer leads `vol` after a promotion, expected to be MOVED to (node, epoch).
 * The server answers from its own copy of the record, which the group's watch delivers a little after the record
 * another server reported: until then it may say UNAVAILABLE (its volume is fenced and the record still names it) or
 * MOVED to the leader of an older epoch. Both refuse before numbering anything, so they are retried for 5 s; anything
 * else, or a stale answer after that, fails (r3e-clus4: writerdepose got UNAVAILABLE right after the record moved). */
static void insert_expect_moved(tsdbc *c, tsdb_sid id, int64_t ts, float val, uint16_t vol, uint32_t node,
                                uint64_t epoch, const char *what)
{
    for (uint64_t t0 = now_ms();; usleep(50000)) {
        int rc = tsdbc_insert_series(c, id, &ts, &val, 1);
        tsdbc_err_info e;
        int stale = rc == TSDB_ERR_UNAVAILABLE ||
                    (rc == TSDB_ERR_MOVED && tsdbc_last_error(c, &e) == TSDB_ERR_MOVED && e.has_body &&
                     e.nmoved == 1 && e.moved[0].epoch < epoch);
        if (!stale || now_ms() - t0 > 5000) {
            expect_moved_ep(c, rc, vol, node, epoch, what);
            return;
        }
    }
}

/* series pump7.flow in `vol` on server i: made, written and read back (returns its id) */
static tsdb_sid write_read(tsdbc *c, const char *vol, int64_t t0)
{
    tsdb_sid id = 0;
    int rc = tsdbc_series_at(c, "pump7.flow", vol, &id);
    if (rc != 0)
        die("series pump7.flow in %s: %s", vol, ers(rc));
    int64_t ts[3] = {t0, t0 + 1000, t0 + 2000};
    float v[3] = {1, 2, 3};
    if ((rc = tsdbc_insert_series(c, id, ts, v, 3)) != 0)
        die("insert into %s: %s", vol, ers(rc));
    return id;
}

static int rows_of(tsdbc *c, tsdb_sid id)
{
    nrows = 0;
    int rc = tsdbc_query_raw(c, id, 0, INT64_MAX / 2, count_rows, NULL);
    if (rc != 0)
        die("query: %s", ers(rc));
    return nrows;
}

static int cli_run(const char *args, const char *in, char *out, size_t n)
{
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char cmd[PATH_MAX * 3 + 256];
    snprintf(cmd, sizeof cmd, "%s%s%s %s 2>&1", in ? in : "", in ? " | " : "", cli, args);
    FILE *p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    size_t got = fread(out, 1, n - 1, p);
    out[got] = 0;
    int st = pclose(p);
    return WIFEXITED(st) ? WEXITSTATUS(st) : 255;
}

/* `tsdb ARGS` started without waiting (cli_finish reads its output and status): two CLI calls at once */
static FILE *cli_start(const char *args)
{
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char cmd[PATH_MAX * 3 + 256];
    snprintf(cmd, sizeof cmd, "%s %s 2>&1", cli, args);
    FILE *p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    return p;
}

static int cli_finish(FILE *p, char *out, size_t n)
{
    size_t got = fread(out, 1, n - 1, p);
    out[got] = 0;
    int st = pclose(p);
    return WIFEXITED(st) ? WEXITSTATUS(st) : 255;
}

struct users_want {
    const char *present[3];
    const char *absent;
};
static int p_users(const struct view *v, void *a)
{
    const struct users_want *w = a;
    for (int k = 0; k < 3 && w->present[k]; k++)
        if (!meta_user(&v->m, w->present[k]))
            return 0;
    return !w->absent || !meta_user(&v->m, w->absent);
}

static int p_users_caught_up(const struct view *v, void *a)
{
    (void)a;
    return v->st.users_caught_up == 1;
}

/* the file a server's users probe wrote (TSDB_CLUSTER_TEST users_probe=1), "" while there is none */
static void probe_read(int i, const char *name, char *out, size_t n)
{
    char path[PATH_MAX + 96];
    server_dir(i, path, sizeof path);
    strcat(path, "/state/");
    strcat(path, name);
    out[0] = 0;
    FILE *f = fopen(path, "r");
    if (f) {
        size_t got = fread(out, 1, n - 1, f);
        out[got] = 0;
        fclose(f);
    }
}

static void await_probe(int i, const char *name, const char *want, const char *what)
{
    char out[8192];
    uint64_t t0 = now_ms();
    for (probe_read(i, name, out, sizeof out); !strstr(out, want); probe_read(i, name, out, sizeof out)) {
        if (now_ms() - t0 > WAIT_MS)
            die("server %d: %s: no \"%s\" in its %s: %s", i, what, want, name, out);
        usleep(50000);
    }
}

/* S2-1: users in the metadata group. `cluster init --superuser` puts root (uid 1) in the first entries; two `cluster
 * user add` of different names through two servers at once get different uids, the same in every copy (the state
 * machine assigns them; the CLI sends uid 0, so this checks the end to end path only: the uid rules have their mutant,
 * uidfromproposer, in test_meta); a grant made through one server is listed by another; the users watch and
 * cluster_user_propose (users_probe); a drop with a member stopped (SIGSTOP) is answered after --wait-ms naming that
 * member as lagging, and the member applies the drop once it runs again. With no quorum a USER_DEL is never answered
 * OK (mutant dropnotcommitted: OK at propose). The watch ring drops entries past its bytes. A restarted member whose
 * Raft core is held reports users_caught_up only after it heard a leader's commit index and applied that far (mutants
 * caughtupearly: true from the start; floornoapplied: true once the commit index is heard). */
static void scen_users(void)
{
    scenario("users");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    /* frequent snapshots: the member restarted below loads its copy (members, addresses, carol) from one, so it reaches
     * the leader with its Raft core held; server 1 registers a users watch and drops probe-drop through
     * cluster_user_propose; a watch ring of 256 KiB, which two big blobs pass */
    envs[0] = envs[2] = "snap_every=2,ring_bytes=262144";
    envs[1] = "snap_every=2,ring_bytes=262144,users_probe=1";
    for (int i = 0; i < 3; i++)
        start(i);
    char pw[PATH_MAX + 80], args[PATH_MAX * 2 + 512], out[8192], out2[8192], p[3][META_ADDR_MAX];
    snprintf(pw, sizeof pw, "%s/pw", base);
    FILE *f = fopen(pw, "w");
    if (!f || fputs("correct horse battery\n", f) < 0 || fclose(f) != 0)
        die("cannot write %s", pw);
    for (int i = 0; i < 3; i++)
        peer_of(i, p[i], sizeof p[i]);
    /* a password file that cannot be read is found before INIT: the cluster stays uninitialised (the init below works) */
    snprintf(args, sizeof args, "cluster init %s --secret-file %s --superuser root --password-file %s/missing", p[0],
             secret_path, base);
    if (cli_run(args, NULL, out, sizeof out) == 0 || strstr(out, "initialised"))
        die("cluster init with an unreadable password file: %s", out);
    snprintf(args, sizeof args,
             "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 --voter-replace-s 600 "
             "--superuser root --password-file %s",
             p[0], secret_path, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user root: uid 1, index "))
        die("cluster init --superuser root: %s", out);
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");

    /* two adds at once, through the leader and through a follower */
    char a1[PATH_MAX * 2 + 512], a2[PATH_MAX * 2 + 512];
    snprintf(a1, sizeof a1, "cluster user add %s alice --password-file %s --secret-file %s", p[0], pw, secret_path);
    snprintf(a2, sizeof a2, "cluster user add %s bob --password-file %s --secret-file %s", p[1], pw, secret_path);
    FILE *c1 = cli_start(a1), *c2 = cli_start(a2);
    int r1 = cli_finish(c1, out, sizeof out), r2 = cli_finish(c2, out2, sizeof out2);
    if (r1 != 0 || !strstr(out, "put user alice: uid "))
        die("user add alice: %s", out);
    if (r2 != 0 || !strstr(out2, "put user bob: uid "))
        die("user add bob: %s", out2);
    struct users_want all3 = {{"root", "alice", "bob"}, NULL};
    uint32_t uid[3][3];
    for (int i = 0; i < 3; i++) {
        await_view(i, p_users, &all3, "root, alice and bob in its copy");
        struct view v;
        view(i, &v);
        for (int k = 0; k < 3; k++)
            uid[i][k] = meta_user(&v.m, all3.present[k])->uid;
        view_free(&v);
        if (uid[i][0] != 1 || uid[i][1] == uid[i][2] || uid[i][1] < 2 || uid[i][1] > 3 || uid[i][2] < 2 ||
            uid[i][2] > 3)
            die("server %d: uids root %" PRIu32 ", alice %" PRIu32 ", bob %" PRIu32 " (want 1 and 2, 3 apart)", i,
                uid[i][0], uid[i][1], uid[i][2]);
        if (i && memcmp(uid[i], uid[0], sizeof uid[0]))
            die("server %d has other uids than server 0", i);
    }
    printf("  root uid 1, alice uid %" PRIu32 ", bob uid %" PRIu32 " in every copy\n", uid[0][1], uid[0][2]);

    /* server 1's users watch has the three; probe-drop, added through server 0, is dropped by server 1's probe through
     * cluster_user_propose (a follower: it sends to the leader), in every copy */
    await_probe(1, "userswatch", "user alice uid ", "the users watch");
    await_probe(1, "userswatch", "user bob uid ", "the users watch");
    await_probe(1, "userswatch", "user root uid 1 ", "the users watch");
    snprintf(args, sizeof args, "cluster user add %s probe-drop --password-file %s --secret-file %s", p[0], pw,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user probe-drop: uid "))
        die("user add probe-drop: %s", out);
    await_probe(1, "userpropose", "rc 0 index ", "cluster_user_propose's DEL");
    for (int i = 0; i < 3; i++)
        await_view(i, p_users, &(struct users_want){{"root", "alice", "bob"}, "probe-drop"}, "probe-drop dropped");
    {
        char w[8192];
        uint64_t t1 = now_ms();
        for (probe_read(1, "userswatch", w, sizeof w); strstr(w, "probe-drop"); probe_read(1, "userswatch", w, sizeof w))
            if (now_ms() - t1 > WAIT_MS)
                die("server 1's users watch still lists probe-drop: %s", w);
            else
                usleep(50000);
    }
    printf("  users watch and cluster_user_propose: probe-drop dropped from server 1\n");

    /* a grant through a follower, listed by the third server from its copy */
    snprintf(args, sizeof args, "cluster user grant %s alice prefix:plant. read --secret-file %s", p[1], secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user alice: uid "))
        die("user grant: %s", out);
    snprintf(args, sizeof args, "cluster user list %s alice --secret-file %s", p[2], secret_path);
    uint64_t t0 = now_ms();
    while (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "\ngrant prefix:plant. read\n"))
        if (now_ms() - t0 > WAIT_MS)
            die("server 2 does not list alice's grant: %s", out);
        else
            usleep(100000);
    snprintf(args, sizeof args, "cluster user list %s root --secret-file %s", p[2], secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "\nsuperuser yes\npassword yes\n"))
        die("root is no superuser with a password: %s", out);
    /* a name longer than USERS allows is refused before anything is built (it overflowed a stack buffer) */
    {
        char lname[400];
        memset(lname, 'a', sizeof lname - 1);
        lname[sizeof lname - 1] = 0;
        snprintf(args, sizeof args, "cluster user list %s %s --secret-file %s", p[2], lname, secret_path);
        int lrc = cli_run(args, NULL, out, sizeof out);
        if (lrc != 2 || !strstr(out, "not a valid user name"))
            die("user list with a 399-byte name: rc %d: %s", lrc, out);
    }
    snprintf(args, sizeof args, "cluster user list %s --secret-file %s", p[1], secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "user root uid 1 version ") ||
        !strstr(out, "user alice uid ") || !strstr(out, "user bob uid "))
        die("user list: %s", out);

    /* a drop with server 2 stopped: answered after the wait, naming server 2 as lagging (server 1 is not) */
    pause_server(2, 1);
    snprintf(args, sizeof args, "cluster user drop %s alice --wait-ms 1500 --secret-file %s", p[0], secret_path);
    t0 = now_ms();
    int rc = cli_run(args, NULL, out, sizeof out);
    uint64_t took = now_ms() - t0;
    char lag2[64], lag1[64];
    snprintf(lag2, sizeof lag2, "lagging: node %" PRIu32 " applied ", ids[2]);
    snprintf(lag1, sizeof lag1, "lagging: node %" PRIu32 " applied ", ids[1]);
    if (rc != 0 || !strstr(out, "dropped user alice") || !strstr(out, lag2) || strstr(out, lag1) || took < 1400)
        die("user drop with server 2 stopped (%" PRIu64 " ms): %s", took, out);
    printf("  drop answered after %" PRIu64 " ms naming node %" PRIu32 " as lagging\n", took, ids[2]);
    struct users_want gone = {{"root", "bob", NULL}, "alice"};
    await_view(1, p_users, &gone, "alice dropped on server 1");
    pause_server(2, 0);
    await_view(2, p_users, &gone, "alice dropped on server 2 once it runs again");

    /* no quorum: a USER_DEL is never answered OK */
    pause_server(1, 1);
    pause_server(2, 1);
    {
        uint8_t req[64], *reply = NULL;
        size_t rl = 0, len = 0;
        req[rl++] = 3;
        req[rl++] = 0;
        memcpy(req + rl, "bob", 3);
        rl += 3;
        memset(req + rl, 0, 12); /* version 0, wait_ms 0 */
        rl += 12;
        char err[1024];
        int arc = 0;
        int sent = cluster_admin(p[0], secret, secret_len, CLUSTER_OP_USER_DEL, req, rl, 3000, &arc, &reply, &len,
                                 err, sizeof err);
        free(reply);
        if (sent == 0 && arc == 0)
            die("USER_DEL answered OK with two of three voters stopped: before it committed");
        printf("  no quorum: USER_DEL %s\n", sent ? err : cluster_strerror(arc));
    }
    pause_server(1, 0);
    pause_server(2, 0);

    /* the no-quorum check may have moved the leadership: the member stopped below is one that does not lead */
    int l = -1;
    for (uint64_t t2 = now_ms(); l < 0; usleep(100000)) {
        for (int i = 0; i < 3 && l < 0; i++) {
            struct view v;
            view(i, &v);
            if (p_leader_self(&v, NULL))
                l = i;
            view_free(&v);
        }
        if (l < 0 && now_ms() - t2 > WAIT_MS)
            die("no leader after the no-quorum check");
    }
    int x = l == 2 ? 1 : 2;
    /* the watch ring is bounded by bytes, not only by entries: every put carries the whole blob (up to 1 MiB) */
    for (int k = 0; k < 2; k++) {
        size_t bl = 200 << 10, rl = 0, len = 0;
        uint8_t *req = calloc(1, 64 + bl), *reply = NULL;
        if (!req)
            die("out of memory");
        req[rl++] = 7;
        req[rl++] = 0;
        memcpy(req + rl, k ? "bigblob" : "bigblub", 7);
        rl += 7;
        rl += 12; /* version 0 (a new user), wait_ms 0 */
        memcpy(req + rl, &(uint32_t){(uint32_t)bl}, 4); /* little-endian, as the wire */
        rl += 4 + bl;
        char err[1024];
        int arc = 0;
        if (cluster_admin(p[l], secret, secret_len, CLUSTER_OP_USER_PUT, req, rl, 10000, &arc, &reply, &len, err,
                          sizeof err) != 0 || arc != 0)
            die("USER_PUT of a 200 KiB blob: %s", arc ? cluster_strerror(arc) : err);
        free(reply);
        memcpy(req + 2, k ? "bigblob" : "bigblub", 7);
        if (cluster_admin(p[l], secret, secret_len, CLUSTER_OP_USER_DEL, req, 2 + 7 + 12, 10000, &arc, &reply, &len,
                          err, sizeof err) != 0 || arc != 0)
            die("USER_DEL of a 200 KiB blob's user: %s", arc ? cluster_strerror(arc) : err);
        free(reply);
        free(req);
    }
    /* the server checks a name itself (the CLI does too, so only a raw request reaches this): a control byte is refused */
    {
        uint8_t req[64], *reply = NULL;
        size_t rl = 0, len = 0;
        req[rl++] = 8;
        req[rl++] = 0;
        memcpy(req + rl, "bad\tname", 8);
        rl += 8;
        memset(req + rl, 0, 12); /* version 0 (a new user), wait_ms 0 */
        rl += 12;
        memcpy(req + rl, &(uint32_t){4}, 4);
        rl += 4;
        memset(req + rl, 0, 4); /* the uid placeholder */
        rl += 4;
        char err[1024];
        int arc = 0;
        int sent = cluster_admin(p[l], secret, secret_len, CLUSTER_OP_USER_PUT, req, rl, 10000, &arc, &reply, &len, err,
                                 sizeof err);
        free(reply);
        if (sent != 0 || arc != META_ERR_INVALID)
            die("USER_PUT of the name \"bad\\tname\": %s, not INVALID", sent ? err : cluster_strerror(arc));
        printf("  a name with a control byte is refused by the server\n");
    }
    t0 = now_ms();
    while (!log_has(l, "the watch ring holds") && now_ms() - t0 < WAIT_MS)
        usleep(50000);
    if (!log_has(l, "the watch ring holds"))
        die("server %d kept 400 KiB of blobs in a watch ring of 256 KiB", l);
    printf("  the watch ring drops its oldest entries past its bytes\n");
    /* a restarted member is not caught up until it heard a leader's commit index and applied it: carol is dropped while
     * member x is down; it restarts with its Raft core held (hold_raft_file): its copy comes from its last snapshot
     * (which names the members, so its heartbeats reach the leader), it hears the leader's commit index in the acks but
     * applies nothing, and must not say it is caught up while its copy still has carol */
    snprintf(args, sizeof args, "cluster user add %s carol --password-file %s --secret-file %s", p[l], pw, secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user carol: uid "))
        die("user add carol: %s", out);
    struct users_want carol = {{"root", "carol", NULL}, "alice"};
    await_view(x, p_users, &carol, "carol on the member to stop");
    stop(x, SIGTERM);
    snprintf(args, sizeof args, "cluster user drop %s carol --wait-ms 100 --secret-file %s", p[l], secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "dropped user carol"))
        die("user drop carol with server %d down: %s", x, out);
    char holdraft[PATH_MAX + 96];
    server_dir(x, holdraft, sizeof holdraft);
    strcat(holdraft, "/state/holdraft");
    FILE *hr = fopen(holdraft, "w");
    if (!hr || fclose(hr) != 0)
        die("cannot make %s", holdraft);
    envs[x] = "snap_every=2,hold_raft_file=1";
    start(x);
    /* the leader runs and is heard (its acks carry its commit index); the copy, which applies nothing, still has carol
     * (mutants caughtupearly and floornoapplied) */
    t0 = now_ms();
    while (now_ms() - t0 < 2500) {
        struct view v;
        view(x, &v);
        int cu = v.st.users_caught_up, has = meta_user(&v.m, "carol") != NULL;
        view_free(&v);
        if (!has)
            die("server %d applied carol's drop with its Raft core held", x);
        if (cu)
            die("server %d says its users are caught up while its copy still has carol, dropped while it was down", x);
        usleep(100000);
    }
    if (unlink(holdraft) != 0)
        die("cannot remove %s", holdraft);
    await_view(x, p_users_caught_up, NULL, "users caught up after a leader was heard");
    struct users_want root_only = {{"root", NULL, NULL}, "carol"};
    await_view(x, p_users, &root_only, "the restarted copy without carol");
    printf("  restarted member caught up only after hearing the leader\n");
}

/* writes `text` and a newline to base/NAME; its path in out */
static void put_file(const char *name, const char *text, char *out, size_t n)
{
    snprintf(out, n, "%s/%s", base, name);
    FILE *f = fopen(out, "w");
    if (!f || fprintf(f, "%s\n", text) < 0 || fclose(f) != 0)
        die("cannot write %s", out);
}

/* a login as user on server i: 0, or the error (the client does not retry AUTH_START) */
static int try_login(int i, const char *user, const char *pw)
{
    tsdbc *c = NULL;
    int rc = tsdbc_connect_auth("127.0.0.1", cports[i], user, pw, &c);
    if (rc == 0)
        tsdbc_close(c);
    return rc;
}

/* waits for a login that works (a change reaches a server's copy a heartbeat after it committed) */
static void await_login(int i, const char *user, const char *pw)
{
    uint64_t t0 = now_ms();
    int rc;
    while ((rc = try_login(i, user, pw)) != 0) {
        if (now_ms() - t0 > WAIT_MS)
            die("server %d: no login as %s: %s", i, user, ers(rc));
        usleep(100000);
    }
}

static tsdbc *login_on(int i, const char *user, const char *pw)
{
    tsdbc *c = NULL;
    int rc = tsdbc_connect_auth("127.0.0.1", cports[i], user, pw, &c);
    if (rc != 0)
        die("server %d: login as %s: %s", i, user, ers(rc));
    return c;
}

/* S2-1, login from the metadata group's copy on every server (server/auth.c "The cluster store"): a user added once
 * logs in on all three; a password changed over the client protocol through a follower: the old one is refused on
 * every server once the reply came (the change is answered after every member applied it, and a member's watch is
 * given the users before its heartbeat reports the index); a grant revoked and then the user dropped while a session
 * is open on the third server: its next request is DENIED, then AUTH; a user dropped while the third server was down
 * (SIGKILL), which restarts with its Raft core held (it hears the leader but applies nothing, its copy from its last
 * snapshot still has the user): every login there is UNAVAILABLE, never AUTH and never in; once it applies, the dropped
 * user is refused and root logs in. Mutants (TSDB_AUTH_MUTANT): authlocalusers (the server does not start: its root
 * has no USERS), adminlocal (the old password still works), loginbeforecatchup (the dropped user logs in). */
static void *unlink_later(void *path)
{
    usleep(1500000);
    if (unlink(path) != 0)
        fprintf(stderr, "cannot remove %s: %s\n", (const char *)path, strerror(errno));
    return NULL;
}

static void scen_login(void)
{
    scenario("login");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    const char *mut = getenv("TSDB_AUTH_MUTANT");
    static char mpenv[128], mpenv2[PATH_MAX + 256];
    char failpush[PATH_MAX + 96], holdraft[PATH_MAX + 96];
    snprintf(failpush, sizeof failpush, "%s/failpush", base);
    snprintf(mpenv, sizeof mpenv, "TSDB_AUTH_MUTANT=%s", mut ? mut : "");
    snprintf(mpenv2, sizeof mpenv2, "TSDB_AUTH_MUTANT=%s TSDB_AUTH_TEST=failpush_file=%s", mut ? mut : "", failpush);
    for (int i = 0; i < 3; i++) {
        auth_on[i] = 1;
        envs[i] = i == 2 ? "snap_every=2,hold_raft_file=1" : "snap_every=2";
        if (mut)
            penv[i] = mpenv;
        if (i == 2)
            penv[i] = mpenv2;
        start(i);
    }
    server_dir(2, holdraft, sizeof holdraft);
    strcat(holdraft, "/state/holdraft");
    char pw[PATH_MAX + 80], pw2[PATH_MAX + 80], args[PATH_MAX * 2 + 512], out[8192], p[3][META_ADDR_MAX];
    put_file("pw", "root-pw", pw, sizeof pw);
    put_file("pw2", "alice-pw", pw2, sizeof pw2);
    for (int i = 0; i < 3; i++)
        peer_of(i, p[i], sizeof p[i]);
    /* before INIT no server has heard a leader: the barrier, not "no such user" */
    int rc = try_login(0, "root", "root-pw");
    if (rc != TSDB_ERR_UNAVAILABLE)
        die("a login before the cluster exists: %s, not UNAVAILABLE", ers(rc));
    snprintf(args, sizeof args,
             "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 --voter-replace-s 600 "
             "--superuser root --password-file %s",
             p[0], secret_path, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user root: uid 1, index "))
        die("cluster init --superuser root: %s", out);
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    for (int i = 0; i < 3; i++)
        await_view(i, p_users_caught_up, NULL, "users caught up");
    /* `tsdb DIR user ...` on a cluster server's root: refused, naming `tsdb cluster user`, and no USERS made */
    {
        char root1[PATH_MAX + 80], uf[PATH_MAX + 96];
        server_dir(1, root1, sizeof root1);
        snprintf(args, sizeof args, "%s user add local --password-file %s", root1, pw2);
        if (cli_run(args, NULL, out, sizeof out) == 0 || !strstr(out, "is a cluster server's root") ||
            !strstr(out, "tsdb cluster user"))
            die("`tsdb DIR user add` on a cluster server's root was not refused: %s", out);
        snprintf(uf, sizeof uf, "%s/USERS", root1);
        if (access(uf, F_OK) == 0)
            die("`tsdb DIR user add` on a cluster server's root made %s", uf);
    }

    snprintf(args, sizeof args, "cluster user add %s alice --password-file %s --secret-file %s", p[0], pw2,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user alice: uid "))
        die("user add alice: %s", out);
    for (int i = 0; i < 3; i++) {
        await_login(i, "root", "root-pw");
        if ((rc = try_login(i, "alice", "alice-pw")) != 0)
            die("server %d: alice, added before, does not log in: %s", i, ers(rc));
        if ((rc = try_login(i, "alice", "wrong")) != TSDB_ERR_AUTH)
            die("server %d: alice with a wrong password: %s, not AUTH", i, ers(rc));
    }
    printf("  one cluster user add: the user logs in on all three servers\n");

    /* passwd over the client protocol, through a follower */
    tsdbc *r1 = login_on(1, "root", "root-pw");
    if ((rc = tsdbc_user_password(r1, "alice", "alice-new", 0)) != 0)
        die("passwd of alice through server 1: %s", ers(rc));
    for (int i = 0; i < 3; i++) {
        if ((rc = try_login(i, "alice", "alice-pw")) != TSDB_ERR_AUTH)
            die("server %d: alice's old password after the passwd was answered: %s, not AUTH", i, ers(rc));
        if ((rc = try_login(i, "alice", "alice-new")) != 0)
            die("server %d: alice's new password: %s", i, ers(rc));
    }
    /* a create through a follower: the name taken is refused as by one server */
    if ((rc = tsdbc_user_create(r1, "alice", "x-pw", 0, 0)) != TSDB_ERR_INVALID)
        die("a create of a name taken: %s, not INVALID", ers(rc));
    printf("  passwd through a follower: the old password refused on every server after the reply\n");

    /* a session open on server 2: a grant reaches it, its revoke (through server 0) and alice's drop too */
    tsdbc *a2 = login_on(2, "alice", "alice-new");
    tsdb_sid sid = 0;
    if ((rc = tsdbc_series(a2, "plant.x", 0, &sid)) != TSDB_ERR_DENIED)
        die("alice's lookup without a grant: %s, not DENIED", ers(rc));
    if ((rc = tsdbc_grant(r1, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("grant: %s", ers(rc));
    if ((rc = tsdbc_series(a2, "plant.x", 0, &sid)) != TSDB_ERR_NOTFOUND)
        die("alice's lookup after the grant was answered: %s, not NOTFOUND", ers(rc));
    tsdbc *r0 = login_on(0, "root", "root-pw");
    if ((rc = tsdbc_revoke(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("revoke: %s", ers(rc));
    if ((rc = tsdbc_series(a2, "plant.x", 0, &sid)) != TSDB_ERR_DENIED)
        die("alice's open session on server 2 after the revoke was answered: %s, not DENIED", ers(rc));
    if ((rc = tsdbc_user_drop(r0, "alice")) != 0)
        die("drop alice: %s", ers(rc));
    if ((rc = tsdbc_series(a2, "plant.x", 0, &sid)) != TSDB_ERR_AUTH)
        die("alice's open session on server 2 after her drop was answered: %s, not AUTH", ers(rc));
    tsdbc_close(a2);
    for (int i = 0; i < 3; i++)
        if ((rc = try_login(i, "alice", "alice-new")) != TSDB_ERR_AUTH)
            die("server %d: alice logs in after her drop: %s", i, ers(rc));
    printf("  an open session on another server: the revoke is DENIED, the drop AUTH on its next request\n");

    /* a push of the users that server 2 fails to take (as out of memory) is pushed again until it is taken (mutant
     * pushnoretry: server 2 keeps the old password until the next change of the users) */
    if ((rc = tsdbc_user_create(r0, "bob", "bob-pw", 0, 0)) != 0)
        die("create bob: %s", ers(rc));
    await_login(2, "bob", "bob-pw");
    char fp2[PATH_MAX + 96];
    put_file("failpush", "", fp2, sizeof fp2);
    if ((rc = tsdbc_user_password(r0, "bob", "bob-new", 0)) != 0)
        die("passwd of bob: %s", ers(rc));
    usleep(500000);
    if ((rc = try_login(2, "bob", "bob-pw")) != 0)
        die("server 2, failing its pushes: bob's old password %s, not in (the test has no teeth)", ers(rc));
    if (unlink(failpush) != 0)
        die("cannot remove %s", failpush);
    uint64_t tp = now_ms();
    while ((rc = try_login(2, "bob", "bob-pw")) != TSDB_ERR_AUTH) {
        if (now_ms() - tp > 5000)
            die("server 2, its pushes taken again: bob's old password %s after 5 s, not AUTH", ers(rc));
        usleep(100000);
    }
    printf("  a push server 2 failed to take is pushed again: the old password refused there once it is taken\n");
    /* an admin op through server 2 while its copy is behind: the conditional put finds bob changed (STALE), waits for
     * the newer copy and checks again (mutant onetry: UNAVAILABLE); and one answered only once server 2's own copy
     * has it, so its next login there sees it (mutant nolocalwait: answered once the members reported the index,
     * server 2's copy still failing its pushes) */
    tsdbc *r2 = login_on(2, "root", "root-pw");
    if (tsdbc_set_retry(r2, 0) != 0)
        die("tsdbc_set_retry");
    put_file("failpush", "", fp2, sizeof fp2);
    if ((rc = tsdbc_user_password(r0, "bob", "bob-3", 0)) != 0)
        die("passwd of bob through server 0: %s", ers(rc));
    pthread_t ul;
    if (pthread_create(&ul, NULL, unlink_later, failpush) != 0)
        die("pthread_create");
    rc = tsdbc_user_password(r2, "bob", "bob-4", 0);
    pthread_join(ul, NULL);
    if (rc != 0)
        die("passwd through server 2, its copy behind: %s (STALE not checked again on the newer copy)", ers(rc));
    put_file("failpush", "", fp2, sizeof fp2);
    if (pthread_create(&ul, NULL, unlink_later, failpush) != 0)
        die("pthread_create");
    tp = now_ms();
    rc = tsdbc_user_password(r2, "bob", "bob-5", 0);
    uint64_t took = now_ms() - tp;
    int old = try_login(2, "bob", "bob-4");
    pthread_join(ul, NULL);
    if (rc != 0)
        die("passwd through server 2: %s", ers(rc));
    if (old != TSDB_ERR_AUTH)
        die("server 2 answered a passwd after %" PRIu64 " ms, before its own copy had it: the old password %s, "
            "not AUTH", took, ers(old));
    tsdbc_close(r2);
    printf("  through a server whose copy is behind: STALE checked again on the newer copy; the reply waits for "
           "its own copy (%" PRIu64 " ms)\n", took);

    /* the actor guard: a superuser demoted through server 0 while server 2 lags (its Raft core held) has a session
     * open on server 2, whose copy still says superuser: their user create there must not commit (mutant noguard) */
    if ((rc = tsdbc_user_create(r0, "mallory", "mallory-pw", 0, TSDB_USER_SUPERUSER)) != 0)
        die("create mallory: %s", ers(rc));
    await_login(2, "mallory", "mallory-pw");
    tsdbc *m2 = login_on(2, "mallory", "mallory-pw");
    if (tsdbc_set_retry(m2, 0) != 0)
        die("tsdbc_set_retry");
    FILE *hr = fopen(holdraft, "w");
    if (!hr || fclose(hr) != 0)
        die("cannot make %s", holdraft);
    if ((rc = tsdbc_user_alter(r0, "mallory", 0)) != 0) /* server 2 lags: answered after the wait */
        die("demote mallory with server 2 held: %s", ers(rc));
    uint64_t tm = now_ms();
    rc = tsdbc_user_create(m2, "mallory2", "m2-pw", 0, TSDB_USER_SUPERUSER);
    if (rc == 0 || try_login(0, "mallory2", "m2-pw") == 0)
        die("a superuser demoted through server 0 created a superuser through lagging server 2 (%s)", ers(rc));
    if (rc != TSDB_ERR_UNAVAILABLE)
        die("the demoted actor's create on lagging server 2: %s, not UNAVAILABLE", ers(rc));
    printf("  a demoted actor's admin op on a lagging server: UNAVAILABLE after %" PRIu64 " ms, nothing made\n",
           now_ms() - tm);
    if (unlink(holdraft) != 0)
        die("cannot remove %s", holdraft);
    tp = now_ms();
    while ((rc = tsdbc_user_create(m2, "mallory2", "m2-pw", 0, TSDB_USER_SUPERUSER)) != TSDB_ERR_DENIED) {
        if (rc == 0 || now_ms() - tp > WAIT_MS)
            die("the demoted actor's create once server 2 caught up: %s, not DENIED", ers(rc));
        usleep(100000);
    }
    tsdbc_close(m2);

    /* the restart barrier: carol dropped while server 2 is down; it comes back with its Raft core held */
    snprintf(args, sizeof args, "cluster user add %s carol --password-file %s --secret-file %s", p[0], pw2,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user carol: uid "))
        die("user add carol: %s", out);
    snprintf(args, sizeof args, "cluster user add %s dave --password-file %s --secret-file %s", p[0], pw2,
             secret_path); /* a second entry: server 2's last snapshot (snap_every=2) holds carol */
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user dave: uid "))
        die("user add dave: %s", out);
    await_login(2, "carol", "alice-pw");
    stop(2, SIGKILL);
    if ((rc = tsdbc_user_drop(r0, "carol")) != 0) /* server 2 lags: answered after the wait, OK */
        die("drop carol with server 2 down: %s", ers(rc));
    hr = fopen(holdraft, "w");
    if (!hr || fclose(hr) != 0)
        die("cannot make %s", holdraft);
    start(2); /* its copy is pushed from its snapshot, carol in it (mutant barrierifempty: logins pass) */
    uint64_t t0 = now_ms();
    int tries = 0;
    while (now_ms() - t0 < 2500) {
        struct view v;
        view(2, &v);
        int has = meta_user(&v.m, "carol") != NULL;
        view_free(&v);
        if (!has)
            die("server 2 applied carol's drop with its Raft core held (the test has no teeth)");
        if ((rc = try_login(2, "carol", "alice-pw")) != TSDB_ERR_UNAVAILABLE)
            die("server 2, not caught up, its copy still holding carol: carol's login %s, not UNAVAILABLE", ers(rc));
        if ((rc = try_login(2, "root", "root-pw")) != TSDB_ERR_UNAVAILABLE)
            die("server 2, not caught up: root's login %s, not UNAVAILABLE", ers(rc));
        tries++;
        usleep(100000);
    }
    put_file("failpush", "", fp2, sizeof fp2); /* the pushes after it applies fail: the barrier waits for one taken */
    if (unlink(holdraft) != 0)
        die("cannot remove %s", holdraft);
    /* applied, its pushes failing: the barrier stays closed (mutant pushnoretry: open, its copy the snapshot's) */
    t0 = now_ms();
    int seen_drop = 0;
    while (now_ms() - t0 < WAIT_MS && !seen_drop) {
        struct view v;
        view(2, &v);
        seen_drop = meta_user(&v.m, "carol") == NULL;
        view_free(&v);
        usleep(100000);
    }
    if (!seen_drop)
        die("server 2 did not apply carol's drop once its Raft core was let go");
    usleep(1000000);
    if ((rc = try_login(2, "root", "root-pw")) != TSDB_ERR_UNAVAILABLE)
        die("server 2, applied but its users watch failing every push: root's login %s, not UNAVAILABLE", ers(rc));
    if (unlink(failpush) != 0)
        die("cannot remove %s", failpush);
    await_view(2, p_users_caught_up, NULL, "users caught up once it applies");
    if ((rc = try_login(2, "carol", "alice-pw")) != TSDB_ERR_AUTH)
        die("server 2, caught up: carol, dropped while it was down: %s, not AUTH", ers(rc));
    if ((rc = try_login(2, "root", "root-pw")) != 0 || (rc = try_login(2, "dave", "alice-pw")) != 0)
        die("server 2, caught up: root or dave: %s", ers(rc));
    printf("  restarted with its Raft core held: %d logins UNAVAILABLE; caught up, the dropped user is refused\n",
           tries);

    /* an outcome unknown (the leader stopped after it may have got the entry): no answer, the connection closes;
     * never UNAVAILABLE, which says "not applied" and has the client send it again (mutant retryunavail) */
    await_view(0, p_leader_self, NULL, "server 0 leading");
    if (tsdbc_set_retry(r1, 0) != 0 || tsdbc_set_timeout(r1, 60000) != 0)
        die("tsdbc_set_retry, tsdbc_set_timeout");
    pause_server(0, 1);
    tm = now_ms();
    rc = tsdbc_user_create(r1, "erin", "erin-pw", 0, 0);
    pause_server(0, 0);
    if (rc == 0 || rc == TSDB_ERR_UNAVAILABLE)
        die("a create whose leader stopped (outcome unknown): %s after %" PRIu64 " ms, not a closed connection",
            ers(rc), now_ms() - tm);
    tsdbc_user *who = NULL;
    if (tsdbc_whoami(r1, &who) == 0) {
        tsdbc_users_free(who, 1);
        die("the connection of an outcome unknown is still open");
    }
    printf("  an admin op of unknown outcome: no answer, the connection closed after %" PRIu64 " ms (%s)\n",
           now_ms() - tm, ers(rc));
    tsdbc_close(r0);
    tsdbc_close(r1);
}

/* A cluster of three for the users scenarios below, root the superuser, login on; server 2 gets `env2` (a test hook)
 * besides the mutants of TSDB_AUTH_MUTANT and TSDB_CLUSTER_MUTANT. Servers 1 and 2 are added only when `add`. */
static void users_cluster(const char *env2, int add)
{
    static char menv[3][PATH_MAX + 384];
    const char *am = getenv("TSDB_AUTH_MUTANT"), *cm = getenv("TSDB_CLUSTER_MUTANT");
    for (int i = 0; i < 3; i++) {
        auth_on[i] = 1;
        snprintf(menv[i], sizeof menv[i], "TSDB_AUTH_MUTANT=%s TSDB_CLUSTER_MUTANT=%s%s%s", am ? am : "",
                 cm ? cm : "", i == 2 && env2 ? " " : "", i == 2 && env2 ? env2 : "");
        penv[i] = menv[i];
        start(i);
    }
    char pw[PATH_MAX + 80], args[PATH_MAX * 2 + 512], out[8192], p0[META_ADDR_MAX];
    put_file("pw", "root-pw", pw, sizeof pw);
    peer_of(0, p0, sizeof p0);
    snprintf(args, sizeof args,
             "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 --voter-replace-s 600 "
             "--superuser root --password-file %s",
             p0, secret_path, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user root: uid 1, index "))
        die("cluster init --superuser root: %s", out);
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    if (!add)
        return;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    for (int i = 0; i < 3; i++) {
        await_view(i, p_users_caught_up, NULL, "users caught up");
        await_login(i, "root", "root-pw");
    }
}

/* a lookup of plant.x as user/pw on server i: NOTFOUND with the grant, DENIED without it; a failed login is its
 * result (plant_lookup_try) */
static int plant_lookup_try(int i, const char *user, const char *pw)
{
    tsdbc *c = NULL;
    int rc = tsdbc_connect_auth("127.0.0.1", cports[i], user, pw, &c);
    if (rc != 0)
        return rc;
    tsdb_sid sid = 0;
    rc = tsdbc_series(c, "plant.x", 0, &sid);
    tsdbc_close(c);
    return rc;
}
static int plant_lookup(int i, const char *user, const char *pw)
{
    tsdbc *c = login_on(i, user, pw);
    tsdb_sid sid = 0;
    int rc = tsdbc_series(c, "plant.x", 0, &sid);
    tsdbc_close(c);
    return rc;
}

struct stale_op {
    tsdbc *c;
    int what; /* 0 grant, 1 revoke, 2 drop, 3 passwd */
    const char *user;
    int rc;
    uint64_t end; /* now_ms() when it was answered */
};
static void *stale_op_run(void *a)
{
    struct stale_op *o = a;
    o->rc = o->what == 0   ? tsdbc_grant(o->c, o->user, TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)
            : o->what == 1 ? tsdbc_revoke(o->c, o->user, TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)
            : o->what == 2 ? tsdbc_user_drop(o->c, o->user)
                           : tsdbc_user_password(o->c, o->user, "new-pw", 0);
    o->end = now_ms();
    return NULL;
}

/* An admin op through server 2 while its copy of the users lags a change acknowledged through server 0 (its pushes
 * failing, TSDB_AUTH_TEST failpush_file, until 1.5 s later; then once SIGSTOPped): a check there finds nothing to
 * change or refuses, from a copy that does not have the change. That answer must not go out from the copy (A32): a
 * grant answered OK (already held) while the acknowledged revoke stands is an acknowledged grant that holds nowhere;
 * a revoke, drop or passwd refused NOTFOUND contradicts the acknowledged grant or add. server/auth.c admin_group: the
 * read barrier, then the check again. Mutants (TSDB_AUTH_MUTANT): nooplocal (no barrier), syncnowait (the barrier's
 * index not waited for in server 2's copy). */
static void scen_userstale(void)
{
    scenario("userstale");
    strict_reads = 1;
    char failpush[PATH_MAX + 96], fenv[PATH_MAX + 128], fp[PATH_MAX + 96];
    snprintf(failpush, sizeof failpush, "%s/failpush", base);
    snprintf(fenv, sizeof fenv, "TSDB_AUTH_TEST=failpush_file=%s", failpush);
    users_cluster(fenv, 1);
    tsdbc *r0 = login_on(0, "root", "root-pw"), *r2 = login_on(2, "root", "root-pw");
    if (tsdbc_set_retry(r2, 0) != 0)
        die("tsdbc_set_retry");
    int rc;
    if ((rc = tsdbc_user_create(r0, "alice", "alice-pw", 0, 0)) != 0 ||
        (rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("create alice, grant: %s", ers(rc));
    await_login(2, "alice", "alice-pw");
    /* what the barrier costs a plain no-op (every copy caught up): the mean of 20 grants already held */
    for (uint64_t t0 = now_ms(); plant_lookup(2, "alice", "alice-pw") != TSDB_ERR_NOTFOUND;)
        if (now_ms() - t0 > WAIT_MS)
            die("server 2 never saw alice's grant");
        else
            usleep(50000);
    uint64_t tn = now_ms();
    for (int k = 0; k < 20; k++)
        if ((rc = tsdbc_grant(r2, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
            die("a grant already held, through server 2: %s", ers(rc));
    printf("  a no-op grant through server 2, every copy caught up: %.1f ms (mean of 20)\n", (now_ms() - tn) / 20.0);
    static const char *names[] = {"grant", "revoke", "drop", "passwd"};
    for (int what = 0; what < 4; what++) {
        const char *user = what < 2 ? "alice" : what == 2 ? "bob" : "carol";
        if (what == 1 && (rc = tsdbc_revoke(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
            die("revoke through server 0: %s", ers(rc));
        if (what == 1) /* server 2 must have the revoke before it falls behind the grant */
            for (uint64_t t0 = now_ms(); plant_lookup(2, "alice", "alice-pw") != TSDB_ERR_DENIED;) {
                if (now_ms() - t0 > WAIT_MS)
                    die("server 2 never saw alice's revoke");
                usleep(50000);
            }
        put_file("failpush", "", fp, sizeof fp);
        /* the change server 2's copy does not get */
        rc = what == 0   ? tsdbc_revoke(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)
             : what == 1 ? tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)
                         : tsdbc_user_create(r0, user, "user-pw", 0, 0);
        if (rc != 0)
            die("the change through server 0 before the %s: %s", names[what], ers(rc));
        pthread_t ul;
        if (pthread_create(&ul, NULL, unlink_later, failpush) != 0)
            die("pthread_create");
        uint64_t t0 = now_ms();
        struct stale_op o = {r2, what, user, 0};
        stale_op_run(&o);
        uint64_t took = now_ms() - t0;
        pthread_join(ul, NULL);
        if (o.rc != 0)
            die("%s through server 2, its users behind an acknowledged change: %s after %" PRIu64 " ms, answered "
                "from its stale copy", names[what], ers(o.rc), took);
        /* and what it acknowledged holds in the group: server 0 says so */
        if (what == 0 && (rc = plant_lookup(0, "alice", "alice-pw")) != TSDB_ERR_NOTFOUND)
            die("grant through server 2 answered OK after %" PRIu64 " ms while the revoke stands: alice's lookup on "
                "server 0 %s, not NOTFOUND (an acknowledged grant that holds nowhere)", took, ers(rc));
        if (what == 1 && (rc = plant_lookup(0, "alice", "alice-pw")) != TSDB_ERR_DENIED)
            die("revoke through server 2: alice's lookup on server 0 %s, not DENIED", ers(rc));
        if (what >= 2 && (rc = try_login(0, user, what == 2 ? "user-pw" : "new-pw")) != (what == 2 ? TSDB_ERR_AUTH : 0))
            die("%s of %s through server 2: login on server 0 %s", names[what], user, ers(rc));
        printf("  %s through server 2, its users behind: confirmed against the group, %" PRIu64 " ms\n", names[what],
               took);
    }

    /* the same with server 2 paused (SIGSTOP) across the revoke, and the grant sent to it before it resumes */
    await_login(2, "carol", "new-pw");
    if ((rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("grant: %s", ers(rc));
    for (uint64_t t0 = now_ms(); plant_lookup(2, "alice", "alice-pw") != TSDB_ERR_NOTFOUND;) {
        if (now_ms() - t0 > WAIT_MS)
            die("server 2 never saw alice's grant");
        usleep(50000);
    }
    pause_server(2, 1);
    if ((rc = tsdbc_revoke(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("revoke through server 0 with server 2 paused: %s", ers(rc));
    struct stale_op o = {r2, 0, "alice", 0};
    pthread_t t;
    if (pthread_create(&t, NULL, stale_op_run, &o) != 0)
        die("pthread_create");
    usleep(300000);
    pause_server(2, 0);
    pthread_join(t, NULL);
    /* OK (the grant made again) or UNAVAILABLE (the barrier did not pass in time: nothing changed, the client asks
     * again); anything else is a regression that the check below would not see */
    if (o.rc != 0 && o.rc != TSDB_ERR_UNAVAILABLE)
        die("grant through server 2 paused across a revoke: %s, neither OK nor UNAVAILABLE", ers(o.rc));
    if (o.rc == 0 && (rc = plant_lookup(0, "alice", "alice-pw")) != TSDB_ERR_NOTFOUND)
        die("grant through server 2 answered OK on its resume while the revoke stands: alice's lookup on server 0 "
            "%s, not NOTFOUND (an acknowledged grant that holds nowhere)", ers(rc));
    printf("  grant through server 2 paused across a revoke: %s, and it holds\n", ers(o.rc));

    /* A no-op's OK acknowledges a state, so it waits for the members as the change did (A32): server 1 paused across
     * a grant through server 0, then the same grant through server 2 (a no-op there) is answered only once server 1
     * has the grant, so that a client of server 1 sees it after the OK. Mutant: nomembers. */
    if ((rc = tsdbc_revoke(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0 && rc != TSDB_ERR_NOTFOUND)
        die("revoke: %s", ers(rc));
    for (uint64_t t0 = now_ms(); plant_lookup(1, "alice", "alice-pw") != TSDB_ERR_DENIED;) {
        if (now_ms() - t0 > WAIT_MS)
            die("server 1 never saw alice's revoke");
        usleep(50000);
    }
    pause_server(1, 1);
    if ((rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("grant through server 0 with server 1 paused: %s", ers(rc));
    for (uint64_t t0 = now_ms(); plant_lookup(2, "alice", "alice-pw") != TSDB_ERR_NOTFOUND;) {
        if (now_ms() - t0 > WAIT_MS)
            die("server 2 never saw alice's grant");
        usleep(50000);
    }
    struct stale_op d = {r2, 0, "alice", 0};
    uint64_t t0 = now_ms();
    if (pthread_create(&t, NULL, stale_op_run, &d) != 0)
        die("pthread_create");
    usleep(1500000);
    uint64_t resumed = now_ms();
    pause_server(1, 0);
    pthread_join(t, NULL);
    uint64_t done = d.end;
    if (d.rc != 0)
        die("the grant again through server 2 (a no-op there) with server 1 paused: %s", ers(d.rc));
    if (done < resumed)
        die("the grant again through server 2 answered OK after %" PRIu64 " ms, before server 1 (paused) had the "
            "grant: a no-op's OK did not wait for the members", done - t0);
    if ((rc = plant_lookup(1, "alice", "alice-pw")) != TSDB_ERR_NOTFOUND)
        die("the grant again through server 2 answered OK, then alice's lookup on server 1 %s, not NOTFOUND: a "
            "no-op's OK did not wait for the members", ers(rc));
    printf("  a no-op through server 2 with server 1 paused: OK after %" PRIu64 " ms, %" PRIu64 " ms after server 1 "
           "resumed\n", done - t0, done - resumed);
    tsdbc_close(r0);
    tsdbc_close(r2);
}

struct promo_probe {
    int i;
    tsdbc *s;
    atomic_int stop;
    int refused, ended, logins;
    char why[160];
};
static void *promo_probe_run(void *a)
{
    struct promo_probe *p = a;
    while (!atomic_load(&p->stop)) {
        int rc = try_login(p->i, "root", "root-pw");
        p->logins++;
        if (rc != 0 && !p->refused++)
            snprintf(p->why, sizeof p->why, "login %s", ers(rc));
        tsdb_sid sid = 0;
        rc = tsdbc_series(p->s, "plant.x", 0, &sid);
        if (rc != TSDB_ERR_NOTFOUND && !p->ended++ && !p->why[0])
            snprintf(p->why, sizeof p->why, "the open session's lookup %s", ers(rc));
        usleep(2000);
    }
    return NULL;
}

/* A member that becomes a voter (a watcher made a learner, then a voter: core_boot rebuilds its metadata copy from
 * nothing or its snapshot, and the log replays) keeps answering logins from the users it had: during the rebuild no
 * login is refused there and an open session goes on (server/cluster.c users_push holds the watch while the copy is
 * below what it was given). Mutant (TSDB_CLUSTER_MUTANT): pushrebuild (the empty copy is pushed). */
static void scen_userpromote(void)
{
    scenario("userpromote");
    strict_reads = 1;
    /* the leader's log compacted (an install is needed) and server 1's install held 2 s: its core starts from nothing
     * and stays empty that long, a window no login can miss */
    envs[0] = "snap_every=2,snap_retain=1,seg_bytes=200";
    envs[1] = "snap_every=2,snap_retain=1,seg_bytes=200,hold_install_ms=2000";
    users_cluster(NULL, 0);
    node_add(0, 1); /* two members: server 1 watches, no core */
    await_view(1, p_users_caught_up, NULL, "users caught up");
    await_login(1, "root", "root-pw");
    struct promo_probe p = {.i = 1, .s = login_on(1, "root", "root-pw")};
    if (tsdbc_set_retry(p.s, 0) != 0)
        die("tsdbc_set_retry");
    pthread_t t;
    if (pthread_create(&t, NULL, promo_probe_run, &p) != 0)
        die("pthread_create");
    node_add(0, 2); /* three: servers 1 and 2 become learners, then voters */
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    await_view(1, p_users_caught_up, NULL, "users caught up");
    usleep(1000000);
    atomic_store(&p.stop, 1);
    pthread_join(t, NULL);
    tsdbc_close(p.s);
    if (p.refused || p.ended)
        die("server 1 made a voter: %d of %d logins refused, %d lookups of the open session failed (first: %s)",
            p.refused, p.logins, p.ended, p.why);
    if (log_has(1, "the cluster's users went back"))
        die("server 1 made a voter: its users went back (an older list pushed while its copy was rebuilt)");
    printf("  server 1 made a voter: %d logins and the open session's lookups all answered\n", p.logins);
}

struct replay_probe {
    atomic_int stop;
    int n, wrong;
    char why[160];
};
static void *replay_probe_run(void *a)
{
    struct replay_probe *p = a;
    while (!atomic_load(&p->stop)) {
        int rc = plant_lookup_try(1, "alice", "alice-pw2");
        p->n++;
        if (rc != TSDB_ERR_DENIED && !p->wrong++)
            snprintf(p->why, sizeof p->why, "alice's lookup %s, not DENIED", ers(rc));
        usleep(1000);
    }
    return NULL;
}

/* A promoted member's copy rebuilt by replaying the log from the start (no snapshot: the first append is 256 entries,
 * applied before the leader sends more) passes through older lists of users, non-empty ones: alice granted before her
 * revoke, with her old password. None of them may reach the users watch (users_push holds it until the copy is back at
 * what it gave, A32): the revoke would be undone on that server for the window. auth.c logs a list that goes back;
 * a probe as alice looks too (the window is short: the log line is the deterministic check; push_every_apply on
 * server 1 offers the watch every list the replay passes, where heartbeats alone hit the window by chance). Mutants
 * (TSDB_CLUSTER_MUTANT): holdempty (held only while the copy has no users), pushrebuild. */
static void scen_userreplay(void)
{
    scenario("userreplay");
    strict_reads = 1;
    envs[1] = "push_every_apply=1"; /* every list the replay passes reaches the watch, not only one at a heartbeat */
    users_cluster(NULL, 0);
    tsdbc *r0 = login_on(0, "root", "root-pw");
    int rc;
    if ((rc = tsdbc_user_create(r0, "alice", "alice-pw", 0, 0)) != 0 ||
        (rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0 ||
        (rc = tsdbc_user_create(r0, "churn", "churn-pw", 0, 0)) != 0)
        die("create alice, grant, create churn: %s", ers(rc));
    /* past the first append: the copy at entry 256 has alice granted */
    for (int k = 0; k < 150; k++)
        if ((rc = tsdbc_grant(r0, "churn", TSDB_SEL_PREFIX, "c.", TSDB_RIGHT_READ)) != 0 ||
            (rc = tsdbc_revoke(r0, "churn", TSDB_SEL_PREFIX, "c.", TSDB_RIGHT_READ)) != 0)
            die("churn %d: %s", k, ers(rc));
    if ((rc = tsdbc_revoke(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0 ||
        (rc = tsdbc_user_password(r0, "alice", "alice-pw2", 0)) != 0)
        die("revoke alice, passwd: %s", ers(rc));
    node_add(0, 1); /* server 1 watches: its copy comes whole from the stream */
    await_view(1, p_users_caught_up, NULL, "users caught up");
    await_login(1, "alice", "alice-pw2");
    if ((rc = plant_lookup(1, "alice", "alice-pw2")) != TSDB_ERR_DENIED)
        die("server 1 before its promotion: alice's lookup %s, not DENIED", ers(rc));
    struct replay_probe p = {0};
    pthread_t t;
    if (pthread_create(&t, NULL, replay_probe_run, &p) != 0)
        die("pthread_create");
    node_add(0, 2); /* servers 1 and 2 become voters: server 1's core replays from entry 1 */
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    await_view(1, p_users_caught_up, NULL, "users caught up");
    usleep(1000000);
    atomic_store(&p.stop, 1);
    pthread_join(t, NULL);
    tsdbc_close(r0);
    if (log_has(1, "the cluster's users went back"))
        die("server 1 made a voter: its users went back while its copy replayed (an older list pushed)");
    if (p.wrong)
        die("server 1 made a voter: %d of %d probes wrong (first: %s)", p.wrong, p.n, p.why);
    printf("  server 1 made a voter, its copy replayed from entry 1: the watch kept the newest users (%d probes)\n",
           p.n);
}

/* The read barrier answers only from a leader that holds its lease (A32): server 0, the leader, has its Raft core
 * held (hold_raft_file: neither ticked nor given a message; its admin ops and heartbeats go on), so servers 1 and 2
 * elect another and a revoke commits there. Server 0 still takes itself for the leader: a grant of what its copy
 * says alice holds (a no-op there) asks itself for the barrier, which must refuse (no lease): UNAVAILABLE, never an
 * OK while the revoke stands. Mutant (TSDB_CLUSTER_MUTANT): syncnolease. Not covered: the barrier's check that the
 * commit index is of the leader's term (a SYNC between an election and the new leader's first commit).
 * The revoke goes through the new leader at once: its own users' change must go to itself from the moment it leads.
 * It used to follow the address the 100 ms duty last stored, server 0's for up to 100 ms after the election; server 0,
 * held, appended the revoke and never committed it, and the revoke's connection closed after 20 s (2 of 7 runs).
 * ul_tick_stale on servers 1 and 2 keeps that duty from moving the address, so only the role change can: mutant
 * uladdrtick (the address moved by the duty alone) fails here every time. */
static void scen_userlease(void)
{
    scenario("userlease");
    strict_reads = 1;
    envs[0] = "hold_raft_file=1";
    envs[1] = envs[2] = "ul_tick_stale=1";
    users_cluster(NULL, 1);
    tsdbc *r0 = login_on(0, "root", "root-pw");
    if (tsdbc_set_retry(r0, 0) != 0)
        die("tsdbc_set_retry");
    int rc;
    if ((rc = tsdbc_user_create(r0, "alice", "alice-pw", 0, 0)) != 0 ||
        (rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("create alice, grant: %s", ers(rc));
    for (int i = 1; i < 3; i++)
        for (uint64_t t0 = now_ms(); plant_lookup(i, "alice", "alice-pw") != TSDB_ERR_NOTFOUND;) {
            if (now_ms() - t0 > WAIT_MS)
                die("server %d never saw alice's grant", i);
            usleep(50000);
        }
    char holdraft[PATH_MAX + 96];
    server_dir(0, holdraft, sizeof holdraft);
    strcat(holdraft, "/state/holdraft");
    FILE *hr = fopen(holdraft, "w");
    if (!hr || fclose(hr) != 0)
        die("cannot make %s", holdraft);
    int nl = 0;
    for (uint64_t t0 = now_ms(); !nl;) {
        for (int i = 1; i < 3 && !nl; i++) {
            struct view v;
            view(i, &v);
            if (p_leader_self(&v, NULL))
                nl = i;
            view_free(&v);
        }
        if (!nl && now_ms() - t0 > WAIT_MS)
            die("no new leader among servers 1 and 2 with server 0's Raft core held");
        if (!nl)
            usleep(100000);
    }
    tsdbc *rl = login_on(nl, "root", "root-pw"); /* retries UNAVAILABLE (30 s), as clients do */
    uint64_t tr = now_ms();
    if ((rc = tsdbc_revoke(rl, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ)) != 0)
        die("revoke through server %d, the new leader: %s after %" PRIu64 " ms (sent to server 0, the leader it "
            "replaced?)", nl, ers(rc), now_ms() - tr);
    tsdbc_close(rl);
    if ((rc = plant_lookup(0, "alice", "alice-pw")) != TSDB_ERR_NOTFOUND)
        die("server 0, its Raft core held, has alice's revoke: %s (the scenario needs it behind)", ers(rc));
    uint64_t t0 = now_ms();
    int g = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ);
    uint64_t took = now_ms() - t0;
    if (g == 0 && (rc = plant_lookup(nl, "alice", "alice-pw")) != TSDB_ERR_NOTFOUND)
        die("grant through server 0 (cut off, its lease gone) answered OK after %" PRIu64 " ms while the revoke "
            "stands: alice's lookup on server %d %s, not NOTFOUND (a barrier from a leader without its lease)", took,
            nl, ers(rc));
    if (g != TSDB_ERR_UNAVAILABLE)
        die("grant through server 0 (cut off, its lease gone): %s after %" PRIu64 " ms, not UNAVAILABLE", ers(g), took);
    if (unlink(holdraft) != 0)
        die("cannot remove %s", holdraft);
    for (uint64_t t1 = now_ms(); plant_lookup(0, "alice", "alice-pw") != TSDB_ERR_DENIED;) {
        if (now_ms() - t1 > WAIT_MS)
            die("server 0 never caught up with alice's revoke");
        usleep(50000);
    }
    tsdbc_close(r0);
    printf("  grant through a leader cut off from its group: UNAVAILABLE after %" PRIu64 " ms\n", took);
}

/* `tsdb cluster user import PEER DIR...` (tools/cluster-user-cli.c): the USERS of two stopped servers, the same (a
 * user with 5,000 grants among them), go in as they are: the verifiers copied, so the passwords log in on a cluster
 * server, the grants count and are in force; a rerun skips what the group holds the same. Two that differ (a grant
 * more in one, a user missing from the other) import nothing and name each such user, until --from-node says whose
 * copy wins. Mutant importmerge (differing users imported silently) must fail here. */
static void scen_userimport(void)
{
    scenario("userimport");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    auth_on[0] = 1;
    start(0);
    init_fast(0);
    char pw[PATH_MAX + 80], args[PATH_MAX * 4 + 512], out[16384], p0[META_ADDR_MAX];
    char a[PATH_MAX + 80], b[PATH_MAX + 80], c[PATH_MAX + 80], d[PATH_MAX + 80], f1[PATH_MAX + 96], f2[PATH_MAX + 96];
    peer_of(0, p0, sizeof p0);
    put_file("pw", "imported-pw", pw, sizeof pw);
    snprintf(a, sizeof a, "%s/old-a", base);
    snprintf(b, sizeof b, "%s/old-b", base);
    snprintf(c, sizeof c, "%s/old-c", base);
    snprintf(d, sizeof d, "%s/old-d", base);
    const char *dirs[4] = {a, b, c, d};
    for (int k = 0; k < 4; k++)
        if (mkdir(dirs[k], 0755) != 0)
            die("mkdir %s: %s", dirs[k], strerror(errno));
    snprintf(args, sizeof args, "%s user add ops --superuser --password-file %s", a, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    snprintf(args, sizeof args, "%s user add big --password-file %s", a, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    users us;
    int rc = users_open(&us, a, true);
    if (rc != 0)
        die("users_open %s: %s", a, ers(rc));
    for (int g = 0; g < 5000; g++) {
        char sel[64];
        int sl = snprintf(sel, sizeof sel, "plant.s%04d", g);
        if ((rc = users_grant(&us, 0, "big", TSDB_SEL_NAME, sel, (size_t)sl, TSDB_RIGHT_READ)) != 0)
            die("grant %d: %s", g, ers(rc));
    }
    users_close(&us);
    snprintf(f1, sizeof f1, "%s/USERS", a);
    snprintf(f2, sizeof f2, "%s/USERS", b);
    snprintf(args, sizeof args, "cp %s %s", f1, f2);
    if (system(args) != 0)
        die("%s", args);

    snprintf(args, sizeof args, "cluster user import %s %s %s --secret-file %s", p0, a, b, secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "imported 2 users (0 in the group already)"))
        die("import of two equal USERS: %s", out);
    snprintf(args, sizeof args, "cluster user list %s big --secret-file %s", p0, secret_path);
    static char big[1 << 20];
    if (cli_run(args, NULL, big, sizeof big) != 0)
        die("user list big: %.400s", big);
    int ng = 0;
    for (const char *q = big; (q = strstr(q, "\ngrant name:plant.s")); q++)
        ng++;
    if (ng != 5000)
        die("big has %d grants in the group, not 5000: %.400s", ng, big);
    await_login(0, "big", "imported-pw");
    tsdbc *cb = login_on(0, "big", "imported-pw");
    tsdb_sid sid;
    if ((rc = tsdbc_series(cb, "plant.s4999", 0, &sid)) != TSDB_ERR_NOTFOUND)
        die("big's lookup of a name granted: %s, not NOTFOUND", ers(rc));
    if ((rc = tsdbc_series(cb, "plant.t0", 0, &sid)) != TSDB_ERR_DENIED)
        die("big's lookup of a name not granted: %s, not DENIED", ers(rc));
    tsdbc_close(cb);
    await_login(0, "ops", "imported-pw");
    snprintf(args, sizeof args, "cluster user import %s %s %s --secret-file %s", p0, a, b, secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "imported 0 users (2 in the group already)"))
        die("the import again: %s", out);
    printf("  two equal USERS imported (5,000 grants; the passwords log in); a rerun skips them\n");

    /* c: dora and eve; d: dora with a grant more, no eve */
    snprintf(args, sizeof args, "%s user add dora --password-file %s", c, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    snprintf(f1, sizeof f1, "%s/USERS", c);
    snprintf(f2, sizeof f2, "%s/USERS", d);
    snprintf(args, sizeof args, "cp %s %s", f1, f2);
    if (system(args) != 0)
        die("%s", args);
    snprintf(args, sizeof args, "%s user add eve --password-file %s", c, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    snprintf(args, sizeof args, "%s user grant dora read prefix:line3.", d);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    /* frank, the same on both but for his password (A33: which one is the newest is not known; mutant
     * noverifiercmp) */
    char pwd[PATH_MAX + 80];
    put_file("pw-frank-d", "frank-d-pw", pwd, sizeof pwd);
    for (int k = 0; k < 2; k++) {
        snprintf(args, sizeof args, "%s user add frank --password-file %s", k ? d : c, k ? pwd : pw);
        if (cli_run(args, NULL, out, sizeof out) != 0)
            die("`tsdb %s`: %s", args, out);
    }
    snprintf(args, sizeof args, "cluster user import %s %s %s --secret-file %s", p0, c, d, secret_path);
    if (cli_run(args, NULL, out, sizeof out) == 0 || !strstr(out, "user dora differs between the servers") ||
        !strstr(out, "user eve differs between the servers") || !strstr(out, "user frank differs between the servers") ||
        !strstr(out, "3 users differ") || !strstr(out, "nothing imported"))
        die("import of two USERS that differ: %s", out);
    for (int k = 0; k < 2; k++) {
        snprintf(args, sizeof args, "cluster user list %s %s --secret-file %s", p0, k ? "eve" : "dora", secret_path);
        if (cli_run(args, NULL, out, sizeof out) == 0)
            die("%s is in the group after an import that differed: %s", k ? "eve" : "dora", out);
    }
    snprintf(args, sizeof args, "cluster user import %s %s %s --from-node %s --secret-file %s", p0, c, d, d,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "imported 2 users"))
        die("import with --from-node: %s", out);
    await_login(0, "frank", "frank-d-pw");
    if ((rc = try_login(0, "frank", "imported-pw")) != TSDB_ERR_AUTH)
        die("frank's password of the losing copy: %s, not AUTH", ers(rc));
    snprintf(args, sizeof args, "cluster user list %s dora --secret-file %s", p0, secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "grant prefix:line3."))
        die("dora as old-d had her: %s", out);
    snprintf(args, sizeof args, "cluster user list %s eve --secret-file %s", p0, secret_path);
    if (cli_run(args, NULL, out, sizeof out) == 0)
        die("eve, absent from the winning copy, was imported: %s", out);
    printf("  two USERS that differ: nothing imported, each user named; --from-node takes that copy\n");
}

/* 3 servers, a volume of 3 replicas: every server makes v1 with the right META and REPL and opens it once the
 * metadata group recorded its open generation; the leader (TSDB_CLUSTER_WRITES=1) takes writes, the replicas
 * refuse them (UNAVAILABLE, FENCED, naming the volume); a restart keeps the rid and raises the generation, which is
 * registered; a copy of the directory from before is refused; with no quorum a restarted server does not open its
 * volume (the generation cannot be recorded). Mutants: TSDB_CVOL_MUTANT=samerid and noopengen must fail here. */
static void scen_volumes(void)
{
    scenario("volumes");
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_CLUSTER_WRITES=1";
        envs[i] = "hold_opens_file=1";
        start(i);
    }
    init_cluster(0, 600); /* restarts take seconds: no voter is replaced meanwhile */
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc = vol_add(1, "big", 4, &num); /* through a follower: the request is followed to the leader */
    if (rc != CLUSTER_ERR_FEW)
        die("a volume of 4 replicas on 3 servers: %s, not CLUSTER_ERR_FEW", cluster_strerror(rc));
    if ((rc = vol_add(1, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    if ((rc = vol_add(0, "plant", 3, &num)) == 0)
        die("a second volume named plant was added (number %" PRIu32 ")", num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    for (int i = 0; i < 3; i++)
        await_log_count(i, "(plant): open as the", 1, 10000);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v) || v.nrep != 3 || v.number != 1)
        die("the record of plant is missing or has %u replicas", v.nrep);
    char hex[33];
    for (int k = 0; k < 16; k++)
        snprintf(hex + 2 * k, 3, "%02x", v.uuid[k]);
    uint64_t rids[3];
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80], path[PATH_MAX + 160], meta[2048], need[96];
        server_dir(i, dir, sizeof dir);
        snprintf(path, sizeof path, "%s/v1/META", dir);
        slurp(path, meta, sizeof meta);
        snprintf(need, sizeof need, "volume_uuid=%s\n", hex);
        if (!strstr(meta, "\nvolume=1\n") || !strstr(meta, need) || !strstr(meta, "\ncluster=") ||
            !strstr(meta, "requires=cluster,replica\n"))
            die("server %d: %s lacks volume=1, %s, cluster= or requires=cluster,replica:\n%s", i, path, need, meta);
        uint64_t rid, gen;
        repl_of(i, 1, &rid, &gen);
        const struct meta_replica *r = rep_of(&v, i);
        if (!r || rid != r->rid || gen != 1)
            die("server %d: REPL rid %016" PRIx64 " gen %" PRIu64 ", the record rid %016" PRIx64, i, rid, gen,
                r ? r->rid : 0);
        rids[i] = rid;
    }
    if (rids[0] == rids[1] || rids[1] == rids[2] || rids[0] == rids[2])
        die("two replicas share a rid");
    char out[8192];
    char args[PATH_MAX * 3 + 256];
    snprintf(args, sizeof args, "cluster status 127.0.0.1:%d --secret-file %s", pports[2], secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "\nvolume 1 name plant ") ||
        !strstr(out, "replica volume 1 node 1 ") || !strstr(out, "replica volume 1 node 3 "))
        die("`tsdb %s` does not list the volume and its replicas:\n%s", args, out);
    printf("  volume 1 on 3 servers: META, REPL and the open generations recorded\n");

    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = write_read(c, "plant", 1000);
    if (TSDB_SID_VOLUME(id) != 1 || rows_of(c, id) != 3)
        die("the leader: id %" PRIx64 " or the rows read back are wrong", (uint64_t)id);
    tsdb_sid x;
    if ((rc = tsdbc_series_at(c, "pump7.flow", "nosuch", &x)) != TSDB_ERR_NOTFOUND)
        die("a series in an unknown volume: %s, not NOTFOUND", ers(rc));
    if ((rc = tsdbc_series(c, "pump7.flow2", 1, &x)) == 0)
        die("a series was made in the root of a cluster node (id %" PRIx64 ")", (uint64_t)x);
    tsdbc_close(c);
    for (int i = 0; i < 3; i++) {
        if (i == L)
            continue;
        c = client(i);
        rc = tsdbc_series_at(c, "pump7.flow", "plant", &x);
        expect_moved(c, rc, 1, v.leader, L, "series made on a replica");
        int64_t ts = 5000;
        float val = 5;
        rc = tsdbc_insert_series(c, id, &ts, &val, 1);
        expect_moved(c, rc, 1, v.leader, L, "insert on a replica");
        /* the packed frames name a series per row: an id the replica's dictionary does not know yet (the leader made
         * it, nothing replicates) is still MOVED to the leader by its volume, never INVALID (route_ids) */
        tsdb_sid ids2[2] = {id, id};
        int64_t ts2[2] = {5000, 5001};
        float val2[2] = {5, 6};
        rc = tsdbc_insert(c, ids2, ts2, val2, 2);
        expect_moved(c, rc, 1, v.leader, L, "a packed insert on a replica");
        tsdb_values tv = {TSDB_FLOAT32, val2, NULL};
        rc = tsdbc_insert_values(c, ids2, ts2, &tv, 2);
        expect_moved(c, rc, 1, v.leader, L, "a packed typed insert on a replica");
        tsdbc_close(c);
    }
    printf("  the leader takes writes and reads them back, the replicas move them to it (MOVED)\n");

    /* restarts: the same rid, the generation raised, saved and recorded; the leader's rows are still there */
    char dir1[PATH_MAX + 80], v1[PATH_MAX + 120], old1[PATH_MAX + 120], cmd[PATH_MAX * 4];
    server_dir(1, dir1, sizeof dir1);
    snprintf(v1, sizeof v1, "%s/v1", dir1);
    snprintf(old1, sizeof old1, "%s/v1.gen1", dir1);
    for (int i = 0; i < 3; i++) {
        stop(i, SIGTERM);
        if (i == 1) { /* the directory as it was at generation 1, for later */
            snprintf(cmd, sizeof cmd, "cp -a %s %s", v1, old1);
            if (system(cmd) != 0)
                die("%s failed", cmd);
        }
        start(i);
        await_open_gen(i == 0 ? 1 : 0, "plant", i, 2, rids[i]);
        await_log_count(i, "(plant): open as the", 2, 30000);
        uint64_t rid, gen;
        repl_of(i, 1, &rid, &gen);
        if (rid != rids[i] || gen != 2)
            die("server %d after a restart: REPL rid %016" PRIx64 " gen %" PRIu64 " (rid %016" PRIx64 ", gen 2)", i, rid,
                gen, rids[i]);
    }
    c = client(L);
    if (rows_of(c, id) != 3)
        die("the leader lost its rows across a restart");
    tsdbc_close(c);
    printf("  every restart keeps the rid, raises the generation to 2 and the group records it\n");

    /* an older copy of the directory (generation 1 < the recorded 2) is refused, naming both */
    uint64_t inc1;
    {
        struct view w;
        view(0, &w);
        const struct meta_member *mb = meta_member(&w.m, ids[1]);
        if (!mb)
            die("server 1 is not a member");
        inc1 = mb->incarnation;
        view_free(&w);
    }
    stop(1, SIGTERM);
    snprintf(cmd, sizeof cmd, "rm -rf %s && mv %s %s", v1, old1, v1);
    if (system(cmd) != 0)
        die("%s failed", cmd);
    start(1);
    await_log_count(1, "is at open generation 1, below the 2 the metadata group recorded", 1, 40000);
    c = client(1);
    expect_held(c, tsdbc_series_at(c, "pump7.flow", "plant", &x), 1, "plant", 1, "an old copy");
    tsdbc_close(c);
    if (log_count(1, "(plant): open as the") != 2)
        die("server 1 opened its old copy of v1");
    printf("  a copy from generation 1 is refused, naming both generations\n");
    /* its log line comes from the server's own copy, before it rejoined: the volume added next needs all three up */
    struct state_want up1 = {ids[1], META_UP, inc1 + 1};
    await_view_ms(0, p_member_state, &up1, "server 1 up again after the refused copy", 40000);

    /* a volume added while the leader proposes no REPLICA_OPEN (hold_opens_file) stays closed everywhere: no server
     * serves before the group recorded its generation (mutant noopengen) */
    for (int i = 0; i < 3; i++) {
        char hold[PATH_MAX + 96];
        server_dir(i, hold, sizeof hold);
        strcat(hold, "/state/holdopens");
        FILE *h = fopen(hold, "w");
        if (!h || fclose(h) != 0)
            die("%s: %s", hold, strerror(errno));
    }
    if ((rc = vol_add(0, "tank", 3, &num)) != 0 || num != 2)
        die("volume add tank: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_log_count(i, "(tank): open generation 1, waiting", 1, 20000);
    usleep(2500000);
    for (int i = 0; i < 3; i++) {
        if (log_count(i, "(tank): open as the"))
            die("server %d opened tank before the group recorded its open generation", i);
        c = client(i);
        expect_held(c, tsdbc_series_at(c, "pump7.flow", "tank", &x), i, "tank", 2, "tank held");
        tsdbc_close(c);
    }
    for (int i = 0; i < 3; i++) {
        char hold[PATH_MAX + 96];
        server_dir(i, hold, sizeof hold);
        strcat(hold, "/state/holdopens");
        unlink(hold);
    }
    for (int i = 0; i < 3; i++)
        await_log_count(i, "(tank): open as the", 1, 20000);
    printf("  a volume whose open generations are not recorded stays closed (RECOVERING), then opens\n");

    /* a crash while making v3 leaves v3.tmp holding any of its files (META, REPL, FNO and their tmps): the next make
     * removes it whole and makes v3 (one that left FNO behind refused the volume for good) */
    char v3[PATH_MAX + 120], old3[PATH_MAX + 120];
    snprintf(v3, sizeof v3, "%s/v3", dir1);
    snprintf(old3, sizeof old3, "%s/v3.gen1", dir1);
    {
        char t3[PATH_MAX + 128];
        snprintf(t3, sizeof t3, "%s.tmp", v3);
        if (mkdir(t3, 0755) != 0)
            die("%s: %s", t3, strerror(errno));
        static const char *left[] = {"META", "REPL", "REPL.tmp", "FNO", "FNO.tmp"};
        for (size_t k = 0; k < sizeof left / sizeof left[0]; k++) {
            char f[PATH_MAX + 160];
            snprintf(f, sizeof f, "%s/%s", t3, left[k]);
            FILE *h = fopen(f, "w");
            if (!h || fputs("left by a crash\n", h) < 0 || fclose(h) != 0)
                die("%s: %s", f, strerror(errno));
        }
    }
    if ((rc = vol_add(0, "pipe", 3, &num)) != 0 || num != 3)
        die("volume add pipe: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_log_count(i, "(pipe): open as the", 1, 20000);
    printf("  a v3.tmp left by a crash while making v3 is removed whole, and v3 is made and opened\n");

    /* an older copy of v3 opened on a metadata copy one REPLICA_OPEN behind (stale_first_watch; the push waits for the
     * copy to hold the run's incarnation, so this stands for a bug there): it saves generation 2, which the group holds
     * from the open the copy predates. Neither that record (another nonce) nor, after a restart on a current copy,
     * the REPL that open saved (generation 2 under its own nonce) lets it serve. Mutants anynonce, replnonce. */
    stop(1, SIGTERM);
    snprintf(cmd, sizeof cmd, "cp -a %s %s", v3, old3);
    if (system(cmd) != 0)
        die("%s failed", cmd);
    start(1);
    await_open_gen(0, "pipe", 1, 2, 0);
    await_log_count(1, "(pipe): open as the", 2, 30000);
    stop(1, SIGTERM);
    snprintf(cmd, sizeof cmd, "rm -rf %s && mv %s %s", v3, old3, v3);
    if (system(cmd) != 0)
        die("%s failed", cmd);
    envs[1] = "hold_opens_file=1,stale_first_watch=1";
    start(1);
    envs[1] = "hold_opens_file=1";
    await_log_count(1, "(pipe): open generation 2, waiting", 2, 40000);
    if ((rc = vol_add(0, "spare", 1, &num)) != 0) /* a commit: every copy moves and is pushed again */
        die("volume add spare: %s", cluster_strerror(rc));
    await_log_count(1, "(pipe): the metadata group recorded open generation 2 of another open", 1, 30000);
    c = client(1);
    expect_held(c, tsdbc_series_at(c, "pump7.flow", "pipe", &x), 1, "pipe", 3, "a stale open");
    tsdbc_close(c);
    if (log_count(1, "(pipe): open as the") != 2)
        die("server 1 opened its older copy of v3 under the generation of the open it predates");
    stop(1, SIGTERM);
    start(1);
    await_log_count(1, "saved open generation 2 in another open than the one the metadata group recorded", 1, 40000);
    usleep(1500000);
    if (log_count(1, "(pipe): open as the") != 2)
        die("server 1 opened its older copy of v3 after a restart");
    printf("  an older copy that saved the recorded generation in a stale open is refused, also after a restart\n");

    /* without a quorum a restarted server answers RECOVERING, never NOTFOUND, and opens nothing */
    int s = L == 2 ? 1 : 2;
    for (int i = 0; i < 3; i++)
        stop(i, SIGTERM);
    start(s);
    usleep(3000000);
    c = client(s);
    /* its copy is stale (no core caught up): plant may not be declared yet (RECOVERING), or declared with its leader
     * on server L, which the copy may still show UP (MOVED there: the entry node decides from the copy alone) or not
     * (PROMOTING); never NOTFOUND, never served here */
    rc = tsdbc_series_at(c, "pump7.flow", "plant", &x);
    if (rc == TSDB_ERR_MOVED) {
        expect_moved(c, rc, 1, v.leader, L, "no quorum");
    } else {
        tsdb_unavail e;
        unsigned n = 0;
        if (rc != TSDB_ERR_UNAVAILABLE || tsdbc_unavailable(c, &e, 1, &n) != 0 || n != 1 ||
            (e.reason != TSDB_WHY_RECOVERING && e.reason != TSDB_WHY_PROMOTING))
            die("no quorum: %s (reason %u), not MOVED to the old leader or UNAVAILABLE (RECOVERING, PROMOTING)",
                ers(rc), n ? e.reason : 0);
    }
    tsdbc_close(c);
    if (log_count(s, "(plant): open as the") != 2)
        die("server %d opened plant before the metadata group recorded its generation", s);
    printf("  with no quorum a restarted server keeps its volume closed\n");
    stop(s, SIGTERM);

    /* outside a cluster the directory is refused (requires=cluster), and so is a feed */
    char vdir[PATH_MAX + 128];
    server_dir(0, vdir, sizeof vdir);
    strcat(vdir, "/v1");
    snprintf(args, sizeof args, "%s query pump7.flow 0 10000", vdir);
    if (cli_run(args, NULL, out, sizeof out) == 0 || !strstr(out, "cluster"))
        die("`tsdb %s` opened a cluster volume outside the cluster:\n%s", args, out);
    snprintf(args, sizeof args, "%s feed enable", vdir);
    if (cli_run(args, NULL, out, sizeof out) == 0 || !strstr(out, "cluster"))
        die("`tsdb %s` was not refused:\n%s", args, out);
    printf("  the CLI refuses a cluster volume (requires=cluster) and a feed on it\n");
}

/* A node refuses to start with --wal-sync-ms other than 0, with series in its root or with VOLUMES (DECISION D2:
 * the root holds USERS only); login stays per node (USERS in the root). */
/* R8 D11, E14, E26, E27 (server/chunksrv*.c): two tsdbd with --chunk-dir. A store whose META names another node or
 * another cluster refuses the start with a line naming both; a store another tsdbd holds (its flock) and one named
 * twice refuse it too. Then puts through the real peer links (the hook TSDB_CHUNKSRV_TEST in chunksrv_cluster.c):
 * a chunk put from A to B reads back whole, B's fence raised to 5 refuses a put at 3 and a delete at 4; B killed with
 * SIGKILL and restarted still holds the chunk and the fence (a put at 4 is FENCED). The cell checks, power loss around
 * PUT_OK and the unsure stores are test_chunksrv's (with its mutants). */
static void chunk_store_make(const char *path, const uint8_t uuid[16], uint32_t node)
{
    char hex[33], args[PATH_MAX + 128], out[4096];
    for (int k = 0; k < 16; k++)
        snprintf(hex + 2 * k, 3, "%02x", uuid[k]);
    snprintf(args, sizeof args, "chunk-store init %s --cluster %s --node %" PRIu32, path, hex, node);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
}

static void scen_chunksrv(void)
{
    scenario("chunksrv");
    for (int i = 0; i < 3; i++) /* three voters: a restarted one publishes its new peer address */
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    struct view v;
    view(0, &v);
    uint8_t uuid[16], other[16];
    memcpy(uuid, v.m.uuid, 16);
    const struct meta_member *mb = meta_member(&v.m, ids[1]);
    struct state_want up = {ids[1], META_UP, mb ? mb->incarnation + 1 : 1};
    view_free(&v);
    memcpy(other, uuid, 16);
    other[5] ^= 0x20;
    stop(1, SIGTERM); /* one server down at a time: the other two keep the group's quorum */
    char s0[PATH_MAX], s1[PATH_MAX], sf[PATH_MAX], so[PATH_MAX], stage[PATH_MAX], hex[33], need[256];
    snprintf(s0, sizeof s0, "%s/cs0", base);
    snprintf(s1, sizeof s1, "%s/cs1", base);
    snprintf(sf, sizeof sf, "%s/csf", base);
    snprintf(so, sizeof so, "%s/cso", base);
    snprintf(stage, sizeof stage, "%s/stage", base);
    if (mkdir(stage, 0755) != 0)
        die("mkdir %s: %s", stage, strerror(errno));
    chunk_store_make(s0, uuid, ids[0]);
    chunk_store_make(s1, uuid, ids[1]);
    chunk_store_make(sf, uuid, ids[1] + 40); /* another member's disk */
    chunk_store_make(so, other, ids[1]);     /* this node id in another cluster */
    for (int k = 0; k < 16; k++)
        snprintf(hex + 2 * k, 3, "%02x", uuid[k]);

    char xa[PATH_MAX * 2 + 64];
    snprintf(xa, sizeof xa, "--chunk-dir %s", sf);
    xargs[1] = xa;
    int rc = start_try(1);
    snprintf(need, sizeof need, "belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32, hex,
             ids[1] + 40, hex, ids[1]);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) != 1 || !log_has(1, need))
        die("a store of another node did not refuse the start naming both (%d, 0x%x)", rc, th_status);
    snprintf(xa, sizeof xa, "--chunk-dir %s", so);
    rc = start_try(1);
    char ohex[33];
    for (int k = 0; k < 16; k++)
        snprintf(ohex + 2 * k, 3, "%02x", other[k]);
    snprintf(need, sizeof need, "belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32, ohex,
             ids[1], hex, ids[1]);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) != 1 || !log_has(1, need))
        die("a store of another cluster did not refuse the start naming both (%d, 0x%x)", rc, th_status);
    snprintf(xa, sizeof xa, "--chunk-dir %s --chunk-dir %s", s1, s1);
    rc = start_try(1);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) == 0)
        die("a store named twice did not refuse the start (%d, 0x%x)", rc, th_status);
    printf("  a store of another node, of another cluster and one named twice refuse the start\n");

    snprintf(xa, sizeof xa, "--chunk-dir %s", s1);
    start(1);
    await_view(0, p_member_state, &up, "server 1 back with a new incarnation");
    stop(0, SIGTERM);
    char xb[PATH_MAX + 64], pe[PATH_MAX + 96];
    snprintf(xb, sizeof xb, "--chunk-dir %s", s1); /* server 0 names server 1's store: its flock refuses */
    xargs[0] = xb;
    rc = start_try(0);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) == 0 || !log_has(0, "is open elsewhere"))
        die("a store another tsdbd holds did not refuse the start (%d, 0x%x)", rc, th_status);
    printf("  a store another tsdbd holds refuses the start\n");

    snprintf(xb, sizeof xb, "--chunk-dir %s", s0);
    snprintf(pe, sizeof pe, "TSDB_CHUNKSRV_TEST=put:%" PRIu32 ":%s", ids[1], stage);
    penv[0] = pe;
    start(0);
    await_log_count(0, "chunksrv test: ", 1, 90000);
    if (!log_has(0, "chunksrv test: pass put"))
        die("the put through two tsdbd failed (n0.log: chunksrv test: FAIL ...)");
    printf("  a chunk put from A to B reads back whole; B's fence at 5 refuses a put at 3 and a delete at 4\n");

    struct view vk;
    view(0, &vk);
    const struct meta_member *mk = meta_member(&vk.m, ids[1]);
    up.min_incarnation = mk ? mk->incarnation + 1 : 1;
    view_free(&vk);
    stop(1, SIGKILL);
    start(1);
    await_view(0, p_member_state, &up, "the killed member back with a new incarnation");
    stop(0, SIGTERM);
    snprintf(pe, sizeof pe, "TSDB_CHUNKSRV_TEST=check:%" PRIu32 ":%s", ids[1], stage);
    start(0);
    await_log_count(0, "chunksrv test: ", 2, 90000);
    if (!log_has(0, "chunksrv test: pass check"))
        die("after B's SIGKILL the chunk or the fence was gone (n0.log: chunksrv test: FAIL ...)");
    printf("  B killed and restarted: the chunk reads back whole, a put at 4 is still FENCED\n");
}

/* R4 (the plan's D9): the metadata snapshot is version 5. A state directory of another version (a cluster a preview
 * build made before) is refused at start with a line naming both versions, never read as damage or replayed, and
 * before NODE is written again: here a one-server cluster's `snapshot`, then (that one put back) its `copy`, is
 * rewritten as version 4 (checksum and all) while it is down. Before that, INIT's four settings of R4 (cluster.c's
 * settings words 19-22) are read back from STATUS. */
uint32_t iso_crc32c(uint32_t crc, const void *buf, size_t len); /* src/raft/compat.c, as meta.c declares it */
static size_t snapv_read(const char *path, uint8_t *img, size_t cap)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        die("cannot read %s: %s", path, strerror(errno));
    size_t n = fread(img, 1, cap, f);
    fclose(f);
    if (n == cap)
        die("%s is larger than %zu bytes", path, cap);
    return n;
}
static void snapv_write(const char *path, const uint8_t *img, size_t n)
{
    FILE *f = fopen(path, "wb");
    if (!f || fwrite(img, 1, n, f) != n || fclose(f) != 0)
        die("rewriting %s", path);
}
/* `path` rewritten as version 4: the start must fail naming it and both versions, and leave NODE as it was */
static void snapv_refused(const char *dir, const char *name)
{
    static uint8_t img[1 << 20], keep[1 << 20], node0[4096], node1[4096];
    char path[PATH_MAX + 300], nodef[PATH_MAX + 300];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    snprintf(nodef, sizeof nodef, "%s/NODE", dir);
    size_t n = snapv_read(path, img, sizeof img);
    memcpy(keep, img, n);
    if (n < 28 || meta_snapshot_version(img, n) != META_SNAP_VERSION)
        die("%s is no image of version %d (%zu bytes)", path, META_SNAP_VERSION, n);
    uint32_t four = 4, crc;
    memcpy(img + 4, &four, 4);
    crc = iso_crc32c(0, img, n - 4);
    memcpy(img + n - 4, &crc, 4);
    snapv_write(path, img, n);
    size_t nn0 = snapv_read(nodef, node0, sizeof node0);
    int rc = start_try(0);
    char want[PATH_MAX + 400];
    snprintf(want, sizeof want, "%s holds metadata of snapshot version 4 and this build reads version %d", path,
             META_SNAP_VERSION);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) == 0 || !log_has(0, want))
        die("a %s of snapshot version 4 was not refused naming both versions (%d, 0x%x)", name, rc, th_status);
    size_t nn1 = snapv_read(nodef, node1, sizeof node1);
    if (nn1 != nn0 || memcmp(node0, node1, nn0) != 0)
        die("the start refused for %s of version 4 wrote NODE again", name);
    snapv_write(path, keep, n);
    printf("  a %s of snapshot version 4 is refused at start, NODE untouched: \"... %s holds metadata of snapshot "
           "version 4 and this build reads version %d\"\n", name, name, META_SNAP_VERSION);
}
static void scen_snapversion(void)
{
    scenario("snapversion");
    start(0);
    init_r4_set = 1;
    init_fast(0);
    init_r4_set = 0;
    struct view v;
    view(0, &v);
    const struct meta_settings *st = &v.m.set;
    if (st->rebuild_mb_s != 300 || st->rebuild_max != 3 || st->transfer_wait_ms != 2500 || st->leader_balance_s != 60)
        die("INIT's settings came back as rebuild_mb_s %u, rebuild_max %u, transfer_wait_ms %u, leader_balance_s %u "
            "(sent 300, 3, 2500, 60)", st->rebuild_mb_s, st->rebuild_max, st->transfer_wait_ms, st->leader_balance_s);
    view_free(&v);
    printf("  INIT's rebuild_mb_s, rebuild_max, transfer_wait_ms and leader_balance_s read back from STATUS\n");
    char dir[PATH_MAX + 200], path[PATH_MAX + 300];
    snprintf(dir, sizeof dir, "%s/n0/state", base);
    snprintf(path, sizeof path, "%s/copy", dir);
    uint64_t t0 = now_ms();
    while (access(path, F_OK) != 0 && now_ms() - t0 < 10000)
        usleep(50000);
    stop(0, SIGTERM);
    snapv_refused(dir, "snapshot");
    snapv_refused(dir, "copy");
}

static void scen_volrefuse(void)
{
    scenario("volrefuse");
    char out[8192], args[PATH_MAX * 3 + 256], dir[PATH_MAX + 80], in[PATH_MAX + 80];
    xargs[0] = "--wal-sync-ms 10";
    int rc = start_try(0);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) != 2 ||
        !log_has(0, "--cluster needs --wal-sync-ms 0"))
        die("--cluster with --wal-sync-ms 10 was not refused with status 2 (%d, 0x%x)", rc, th_status);

    server_dir(1, dir, sizeof dir);
    snprintf(args, sizeof args, "%s insert pump7.flow", dir);
    if (cli_run(args, "echo 1000,1", out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    rc = start_try(1);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) != 1 ||
        !log_has(1, "the root holds 1 series"))
        die("a node with a series in its root started (%d, 0x%x)", rc, th_status);

    server_dir(2, dir, sizeof dir);
    start(2); /* a node that ran once (its root exists), then a volume added to it locally while it was down */
    stop(2, SIGTERM);
    snprintf(in, sizeof in, "%s/n2-local", base);
    if (mkdir(in, 0755) != 0)
        die("mkdir %s: %s", in, strerror(errno));
    snprintf(args, sizeof args, "%s volume add %s", dir, in);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    rc = start_try(2);
    if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) != 1 || !log_has(2, "VOLUMES lists volume 1"))
        die("a node with a local volume started (%d, 0x%x)", rc, th_status);
    printf("  --wal-sync-ms 10, a series in the root and a local volume are refused at start, naming them\n");

    /* S2-1: a root that holds USERS is refused (with and without login: the file would look valid), naming the
     * import; without it the server starts, marks its root, and `tsdb DIR user` refuses there naming `tsdb cluster
     * user`; a superuser of the metadata group writes to a volume of 1 replica */
    server_dir(3, dir, sizeof dir);
    snprintf(in, sizeof in, "%s/pw", base);
    FILE *f = fopen(in, "w");
    if (!f || fputs("s3cret-pw\n", f) < 0 || fclose(f) != 0)
        die("%s: %s", in, strerror(errno));
    snprintf(args, sizeof args, "%s user add admin --superuser --password-file %s", dir, in);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("`tsdb %s`: %s", args, out);
    for (int on = 0; on < 2; on++) {
        auth_on[3] = on;
        rc = start_try(3);
        if (rc != TH_EXITED || !WIFEXITED(th_status) || WEXITSTATUS(th_status) == 0 ||
            !log_has(3, "never from its root: move the file aside (out of "))
            die("a cluster node with USERS in its root started (login %s; %d, 0x%x)", on ? "on" : "off", rc,
                th_status);
    }
    char upath[PATH_MAX + 96];
    snprintf(upath, sizeof upath, "%s/USERS", dir);
    if (unlink(upath) != 0)
        die("unlink %s: %s", upath, strerror(errno));
    penv[3] = "TSDB_CLUSTER_WRITES=1";
    start(3);
    snprintf(args, sizeof args, "%s user add admin --superuser --password-file %s", dir, in);
    if (cli_run(args, NULL, out, sizeof out) == 0 || !strstr(out, "use 'tsdb cluster user add PEER"))
        die("`tsdb DIR user add` on a cluster node's root: %s", out);
    if (access(upath, F_OK) == 0)
        die("`tsdb DIR user add` made %s on a cluster node's root", upath);
    init_fast(3);
    char p3[META_ADDR_MAX];
    peer_of(3, p3, sizeof p3);
    snprintf(args, sizeof args, "cluster user add %s admin --superuser --password-file %s --secret-file %s", p3, in,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "put user admin: uid "))
        die("cluster user add admin: %s", out);
    printf("  a root USERS is refused naming the import; `tsdb DIR user` refuses on a cluster root\n");
    uint32_t num;
    if ((rc = vol_add(3, "solo", 1, &num)) != 0 || num != 1)
        die("volume add solo: %s", cluster_strerror(rc));
    await_open_gen(3, "solo", 3, 1, 0);
    await_log_count(3, "(solo): open as the writer", 1, 10000);
    tsdbc *c;
    if ((rc = tsdbc_connect_auth("127.0.0.1", cports[3], "admin", "wrong", &c)) != TSDB_ERR_AUTH)
        die("a wrong password: %s, not AUTH", ers(rc));
    if ((rc = tsdbc_connect_auth("127.0.0.1", cports[3], "admin", "s3cret-pw", &c)) != 0)
        die("login: %s", ers(rc));
    tsdb_sid id = write_read(c, "solo", 1000);
    if (rows_of(c, id) != 3)
        die("the rows written with a login are not read back");
    tsdbc_close(c);
    printf("  login with a user of the metadata group; a volume of 1 replica takes writes\n");
}


/* links: the hand-off of data links in the cluster thread's acceptor (server/cluster.c handshake(), conn_handoff), in
 * this process (cluster_start here, no tsdbd): a data link's HELLO is refused while no peerlink is set, and for a
 * server that is no member of the cluster, one with another identity, another or no cluster uuid; a member's is handed
 * to the peerlink, comes up on both sides and carries a frame; the metadata links go on working beside them. */
static _Atomic int lk_ups[2], lk_frames[2];
static void lk_on_link(void *arg, struct plink *l, int up)
{
    (void)l;
    if (up)
        atomic_fetch_add(&lk_ups[(intptr_t)arg], 1);
}
static void lk_on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    (void)l, (void)type, (void)p, (void)n;
    atomic_fetch_add(&lk_frames[(intptr_t)arg], 1);
}

static struct peerlink *lk_peerlink(int which, uint32_t id, const uint8_t uuid[16], const uint8_t ident[16])
{
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.self.id = id;
    memcpy(c.self.uuid, uuid, 16);
    memcpy(c.self.ident, ident, 16);
    c.secret = secret;
    c.secret_len = secret_len;
    c.arg = (void *)(intptr_t)which;
    c.on_link = lk_on_link;
    c.on_frame = lk_on_frame;
    c.ping_ms = 100;
    c.dead_ms = 1000;
    c.backoff_min_ms = 20;
    c.backoff_max_ms = 100;
    struct peerlink *pl = peerlink_new(&c);
    if (!pl)
        die("peerlink_new");
    return pl;
}

/* a dialer as (id, uuid, ident) to `addr`: does a link of `kind` come up within ms? */
static int lk_dials(const char *addr, uint32_t id, const uint8_t uuid[16], const uint8_t ident[16], int kind, int ms)
{
    struct peerlink *b = lk_peerlink(1, id, uuid, ident);
    int before = atomic_load(&lk_ups[1]);
    struct plink *l = peerlink_dial(b, 0, addr, kind);
    if (!l)
        die("peerlink_dial");
    int up = 0;
    for (int t = 0; t < ms && !up; t += 20) {
        up = atomic_load(&lk_ups[1]) > before && plink_up(l);
        if (!up)
            usleep(20 * 1000);
    }
    if (up) { /* a frame from the dialer reaches the server's peerlink */
        int f0 = atomic_load(&lk_frames[0]);
        if (plink_send(l, 1, "hello", 5, 1000) != PL_OK)
            up = 0;
        for (int t = 0; t < 3000 && atomic_load(&lk_frames[0]) == f0; t += 10)
            usleep(10 * 1000);
        if (atomic_load(&lk_frames[0]) == f0)
            die("links: a frame over the data link did not arrive");
    }
    peerlink_free(b);
    return up;
}

/* every node holds a FWD and a FWDQ link to every other member, with no replicated volume anywhere (s2d), and
 * dials a member again after it restarts */
static void fwd_await(int i, uint32_t peer, const char *kind, int conn)
{
    char needle[128];
    snprintf(needle, sizeof needle, "fwd link to node %" PRIu32 " (%s) up, connection %d", peer, kind, conn);
    uint64_t t0 = now_ms();
    while (!log_has(i, needle) && now_ms() - t0 < WAIT_MS)
        usleep(50000);
    if (!log_has(i, needle))
        die("server %d: no \"%s\"", i, needle);
}

static void scen_fwdlinks(void)
{
    scenario("fwdlinks");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    for (int i = 0; i < 3; i++)
        for (int j = 0; j < 3; j++)
            if (i != j) {
                fwd_await(i, ids[j], "fwd", 1);
                fwd_await(i, ids[j], "fwdq", 1);
            }
    stop(2, SIGTERM);
    start(2);
    for (int i = 0; i < 2; i++) {
        fwd_await(i, ids[2], "fwd", 2);
        fwd_await(i, ids[2], "fwdq", 2);
    }
    printf("  fwdlinks: 3 servers, each with FWD and FWDQ links to both others; both redial a restarted one\n");
}

static void scen_links(void)
{
    scenario("links");
    char sd[PATH_MAX + 128], padv[64], err[1024];
    snprintf(sd, sizeof sd, "%s/state", base);
    int pfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t sl = sizeof sa;
    int one = 1;
    setsockopt(pfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (pfd < 0 || bind(pfd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(pfd, 64) != 0 ||
        getsockname(pfd, (struct sockaddr *)&sa, &sl) != 0)
        die("links: listener: %s", strerror(errno));
    snprintf(padv, sizeof padv, "127.0.0.1:%d", ntohs(sa.sin_port));
    struct cluster_cfg cc = {.state_dir = sd, .peer_fd = pfd, .advertise = "127.0.0.1:1", .peer_advertise = padv,
                             .rack = "", .power_feed = "", .bmc = "", .secret = secret, .secret_len = secret_len};
    if (cluster_start(&cc) != 0)
        die("links: cluster_start");
    struct meta_settings s;
    meta_settings_default(&s);
    uint8_t req[128], *rep = NULL;
    size_t rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req), len;
    int rc;
    if (cluster_admin(padv, secret, secret_len, CLUSTER_OP_INIT, req, rl, 10000, &rc, &rep, &len, err, sizeof err) != 0 ||
        rc != 0)
        die("links: init: %s", err);
    free(rep);
    struct cluster_self me;
    if (cluster_self(&me) != 0 || me.id != 1)
        die("links: cluster_self after init: have_node %d id %u", me.have_node, (unsigned)me.id);
    /* a second member: a real tsdbd, added through this node (the leader) */
    start(1);
    char np[META_ADDR_MAX];
    peer_of(1, np, sizeof np);
    uint8_t areq[META_ADDR_MAX + 2];
    size_t an = node_add_req(1, areq);
    if (cluster_admin(padv, secret, secret_len, CLUSTER_OP_NODE_ADD, areq, an, 20000, &rc, &rep, &len, err, sizeof err) != 0 ||
        rc != 0)
        die("links: node add: %s", err);
    free(rep);
    uint32_t mid = 0;
    uint8_t mident[16] = {0};
    for (uint64_t t0 = now_ms(); !mid && now_ms() - t0 < WAIT_MS; usleep(100000)) {
        struct view v;
        view(1, &v);
        const struct meta_member *m = v.st.self ? meta_member(&v.m, v.st.self) : NULL;
        static const uint8_t unknown[16]; /* zero: not known; a random ident may begin with a zero byte */
        if (m && memcmp(m->ident, unknown, 16) != 0) {
            mid = m->id;
            memcpy(mident, m->ident, 16);
        }
        view_free(&v);
    }
    if (!mid)
        die("links: the second member did not show up");
    uint8_t zero[16] = {0}, other[16], bad_ident[16], rnd[16];
    memcpy(other, me.uuid, 16);
    other[0] ^= 1;
    memcpy(bad_ident, mident, 16);
    bad_ident[3] ^= 1;
    for (int i = 0; i < 16; i++)
        rnd[i] = (uint8_t)(0x40 + i);

    /* no peerlink set: refused (every other check passes) */
    if (lk_dials(padv, mid, me.uuid, mident, PL_KIND_LOG, 1500))
        die("links: a data link came up while no peerlink was set");
    struct peerlink *srv = lk_peerlink(0, me.id, me.uuid, me.ident);
    /* the acceptor's table (cluster_set_peerlink_kind): a kind goes only to the peerlink registered for it (mutant
     * kindall); the metadata kind and kinds past the table cannot be registered */
    if (cluster_set_peerlink_kind(PL_KIND_META, srv) != -1 || cluster_set_peerlink_kind(CLUSTER_PL_KINDS, srv) != -1 ||
        cluster_set_peerlink_kind(-1, srv) != -1)
        die("links: the table took the metadata kind or a kind out of range");
    cluster_set_peerlink_kind(PL_KIND_LOG, srv);
    if (lk_dials(padv, mid, me.uuid, mident, PL_KIND_BULK, 1500))
        die("links: a bulk link came up while only the log kind had a peerlink");
    if (!lk_dials(padv, mid, me.uuid, mident, PL_KIND_LOG, 10000))
        die("links: a log link did not come up with the log kind registered");
    cluster_set_peerlink(srv);
    if (lk_dials(padv, mid, me.uuid, bad_ident, PL_KIND_LOG, 1500))
        die("links: a data link came up for a member's id with another identity");
    if (lk_dials(padv, me.id, me.uuid, me.ident, PL_KIND_LOG, 1500))
        die("links: a data link came up from this server's own identity");
    if (lk_dials(padv, 7, me.uuid, rnd, PL_KIND_LOG, 1500))
        die("links: a data link came up for an id that is no member");
    if (lk_dials(padv, 0, me.uuid, rnd, PL_KIND_LOG, 1500))
        die("links: a data link came up for id 0");
    if (lk_dials(padv, mid, other, mident, PL_KIND_LOG, 1500))
        die("links: a data link came up for another cluster's uuid");
    if (lk_dials(padv, mid, zero, mident, PL_KIND_LOG, 1500))
        die("links: a data link came up for a zero cluster uuid");
    /* the member: both kinds come up */
    if (!lk_dials(padv, mid, me.uuid, mident, PL_KIND_LOG, 10000))
        die("links: a member's log link did not come up");
    if (!lk_dials(padv, mid, me.uuid, mident, PL_KIND_BULK, 10000))
        die("links: a member's bulk link did not come up");
    if (!lk_dials(padv, mid, me.uuid, mident, PL_KIND_SNAP, 10000)) /* R4's snapshot kind, handed to the same peerlink */
        die("links: a member's snapshot link (kind 5) did not come up");
    /* the metadata port still serves its admin ops */
    if (cluster_admin(padv, secret, secret_len, CLUSTER_OP_STATUS, NULL, 0, 5000, &rc, &rep, &len, err, sizeof err) != 0 ||
        rc != 0)
        die("links: status after the data links: %s", err);
    free(rep);
    cluster_set_peerlink(NULL);
    cluster_stop();
    peerlink_free(srv);
    stop_all();
    printf("  data links: refused without a peerlink, for a kind without one, for a non-member, a wrong identity or uuid; a "
           "member's come up\n");
}

/* replicate: a volume of 3 replicas with the replication driver (server/repld.c; no TSDB_CLUSTER_WRITES): the leader
 * promotes itself once its EPOCH marker is committed, takes writes, and every replica holds the same rows. */
static const char *const rep_names[] = {"pump1.flow", "pump2.flow", "pump3.big", "pump4.slow"};
#define REP_NAMES 4

/* waits until the leader takes a series creation in vol (it is still promoting: UNAVAILABLE) */
static tsdb_sid rep_series(tsdbc *c, const char *name, const char *vol, int ms)
{
    uint64_t t0 = now_ms();
    for (;;) {
        tsdb_sid id = 0;
        int rc = tsdbc_series_at(c, name, vol, &id);
        if (rc == 0)
            return id;
        if (rc != TSDB_ERR_UNAVAILABLE || now_ms() - t0 > (uint64_t)ms)
            die("series %s in %s: %s", name, vol, ers(rc));
        struct timespec ts = {0, 20000000};
        nanosleep(&ts, NULL);
    }
}

/* every server's copy of volume 1, read in-process after a clean stop: the same rows and digests, `want` rows each */
static void rep_same(const uint64_t *want, const char *what)
{
    uint64_t rows[3][REP_NAMES], h[3][REP_NAMES];
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80], v[PATH_MAX + 120];
        server_dir(i, dir, sizeof dir);
        snprintf(v, sizeof v, "%s/v1", dir);
        int rc = cvol_digest(v, rep_names, REP_NAMES, rows[i], h[i]);
        if (rc != 0)
            die("%s: open %s read-only: %s", what, v, ers(rc));
        static const char *const files[] = {"SERIES", "CLASSES", "RETENTION"};
        for (int f = 0; i && f < 3; f++) {
            char a[PATH_MAX + 160], b[PATH_MAX + 160], cmd[2 * PATH_MAX + 400];
            snprintf(a, sizeof a, "%s/n0/v1/%s", base, files[f]);
            snprintf(b, sizeof b, "%s/n%d/v1/%s", base, i, files[f]);
            snprintf(cmd, sizeof cmd, "cmp -s '%s' '%s'", a, b);
            if (access(a, F_OK) != 0) { /* CLASSES comes with a series' first class switch */
                if (f == 0 || access(b, F_OK) == 0)
                    die("%s: server 0 has no %s%s", what, files[f], f ? " and another server has" : "");
                continue;
            }
            if (system(cmd) != 0)
                die("%s: server %d's %s differs from server 0's", what, i, files[f]);
        }
        for (int k = 0; k < REP_NAMES; k++) {
            if (rows[i][k] != want[k])
                die("%s: server %d holds %" PRIu64 " rows of %s, not %" PRIu64, what, i, rows[i][k], rep_names[k],
                    want[k]);
            if (i && h[i][k] != h[0][k])
                die("%s: server %d's rows of %s differ from server 0's", what, i, rep_names[k]);
        }
    }
}

struct rep_writer {
    int port;
    tsdb_sid id;
    int64_t t0;
    int count, done, rc;
    int big; /* > 0: one insert of this many rows instead of `count` single rows */
    int acked; /* rows acknowledged so far (rows t0, t0 + 1000, ...) */
};
static int rep_timeout_ms; /* > 0: every answer to a rep_write_thread within this, or the write fails (a hang shows as
                              a failure, not as a test that never ends) */

/* small writes of one row each from another connection, while the main thread kills and restarts a replica */
static void *rep_write_thread(void *a)
{
    struct rep_writer *w = a;
    tsdbc *c;
    w->rc = tsdbc_connect("127.0.0.1", w->port, &c);
    if (w->rc == 0) {
        tsdbc_set_retry(c, 0);
        if (rep_timeout_ms > 0)
            tsdbc_set_timeout(c, rep_timeout_ms);
        if (w->big > 0) {
            int64_t *ts = malloc((size_t)w->big * sizeof *ts);
            float *val = malloc((size_t)w->big * sizeof *val);
            if (!ts || !val)
                die("out of memory");
            for (int j = 0; j < w->big; j++)
                ts[j] = w->t0 + 1000LL * j, val[j] = (float)(j % 613);
            w->rc = tsdbc_insert_series(c, w->id, ts, val, w->big);
            if (w->rc == 0)
                __atomic_store_n(&w->acked, w->big, __ATOMIC_RELEASE);
            free(ts);
            free(val);
        }
        for (int j = 0; j < w->count && w->rc == 0; j++) {
            int64_t ts = w->t0 + 1000LL * j;
            float val = (float)j;
            w->rc = tsdbc_insert_series(c, w->id, &ts, &val, 1);
            if (w->rc == 0)
                __atomic_store_n(&w->acked, j + 1, __ATOMIC_RELEASE);
        }
        tsdbc_close(c);
    }
    __atomic_store_n(&w->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

static void scen_replicate(void)
{
    scenario("replicate");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id[REP_NAMES];
    for (int k = 0; k < REP_NAMES; k++)
        id[k] = rep_series(c, rep_names[k], "plant", 30000);
    uint64_t n[REP_NAMES] = {0};
    /* small writes, one row each: the log stream and the commit point */
    for (int j = 0; j < 200; j++) {
        int64_t ts = 1000 + 1000LL * j;
        float val = (float)j;
        if ((rc = tsdbc_insert_series(c, id[0], &ts, &val, 1)) != 0)
            die("small insert %d: %s", j, ers(rc));
        n[0]++;
    }
    {
        int64_t ts[3] = {1000, 2000, 3000};
        float val[3] = {1, 2, 3};
        if ((rc = tsdbc_insert_series(c, id[1], ts, val, 3)) != 0)
            die("insert of 3 rows: %s", ers(rc));
        n[1] += 3;
    }
    /* a large insert (straight to parts: shipped on the bulk link, staged, committed by a marker) */
    {
        enum { BIG = 100000 };
        int64_t *ts = malloc(BIG * sizeof *ts);
        float *val = malloc(BIG * sizeof *val);
        if (!ts || !val)
            die("out of memory");
        for (int j = 0; j < BIG; j++)
            ts[j] = 1000 + 1000LL * j, val[j] = (float)(j % 977);
        if ((rc = tsdbc_insert_series(c, id[2], ts, val, BIG)) != 0)
            die("large insert: %s", ers(rc));
        n[2] += BIG;
        /* a delete, and a replace of a range by fewer rows */
        if ((rc = tsdbc_delete_range(c, id[2], 1000 + 1000LL * 100, 1000 + 1000LL * 200)) != 0)
            die("delete: %s", ers(rc));
        for (int j = 0; j < 10; j++)
            ts[j] = 1000 + 1000LL * (500 + 7 * j), val[j] = -1;
        tsdb_values tv = {TSDB_FLOAT32, val, NULL};
        if ((rc = tsdbc_replace_range(c, id[2], 1000 + 1000LL * 500, 1000 + 1000LL * 600, ts, &tv, 10)) != 0)
            die("replace: %s", ers(rc));
        n[2] = (uint64_t)rows_of(c, id[2]); /* the replicas must hold what the leader answers */
        if (n[2] >= BIG - 100)
            die("the delete and the replace removed nothing: %" PRIu64 " rows", n[2]);
        free(ts);
        free(val);
    }
    /* a slow fresh series: its first flush (flush_interval_ms 5 s) gives it a class of its own (a CLASS marker) */
    {
        int64_t ts[200];
        float val[200];
        for (int j = 0; j < 200; j++)
            ts[j] = 3600000LL * j, val[j] = (float)j;
        if ((rc = tsdbc_insert_series(c, id[3], ts, val, 200)) != 0)
            die("insert of the slow series: %s", ers(rc));
        n[3] = 200;
        /* RETENTION: a limit that keeps every row (the rows are from 1970) */
        if ((rc = tsdbc_set_retention(c, id[0], 100LL * 366 * 86400000)) != 0)
            die("retention: %s", ers(rc));
        char cls[PATH_MAX + 120];
        snprintf(cls, sizeof cls, "%s/n%d/v1/CLASSES", base, L);
        uint64_t t0 = now_ms();
        while (access(cls, F_OK) != 0) {
            if (now_ms() - t0 > 30000)
                die("the slow series got no class of its own (no %s)", cls);
            struct timespec w = {0, 100000000};
            nanosleep(&w, NULL);
        }
        int64_t t1 = 3600000LL * 200;
        float v1 = 7;
        if ((rc = tsdbc_insert_series(c, id[3], &t1, &v1, 1)) != 0) /* after the switch */
            die("insert after the class switch: %s", ers(rc));
        n[3]++;
    }
    for (int k = 0; k < REP_NAMES; k++)
        if ((uint64_t)rows_of(c, id[k]) != n[k])
            die("the leader reads %d rows of %s, not %" PRIu64, rows_of(c, id[k]), rep_names[k], n[k]);
    printf("  the leader promoted itself and took small and large writes, a delete, a replace, a retention limit and a class switch\n");

    /* a replica killed while the leader takes writes: restarted, it catches up from the leader's log (a restart of
     * every server would come back on new peer ports, which the metadata group does not know) */
    int R = (L + 1) % 3;
    stop(R, SIGKILL);
    struct rep_writer w = {cports[L], id[0], 10000000, 300, 0, 0, 0, 0};
    /* and a large insert: its part is shipped while R is down; R, back, does not stage the first file it receives
     * (TSDB_REPLD_TEST=dropbulk), finds the COMMIT marker in the log without the file and fetches it from the leader
     * by the marker's position (eng_leader_file_open serves a name only for the marker that named it) */
    struct rep_writer wb = {cports[L], id[2], 2000000000, 0, 0, 0, 100000, 0}; /* above wal_max_batch_rows */
    pthread_t th, thb;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0 || pthread_create(&thb, NULL, rep_write_thread, &wb) != 0)
        die("pthread_create");
    struct timespec pause = {1, 500000000};
    nanosleep(&pause, NULL);
    /* the part shipped to R once it is back is lost: R must fetch it, and asks only 5 s after it found the file missing
     * (fetchlate), when its log has long passed the marker and the leader has sealed and flushed the marker's segment:
     * the leader keeps that segment, and the link the file is served from, by R's durable end, which stops below the
     * marker (keepwritten: by R's written end it went, and R never got the file) */
    penv[R] = "TSDB_REPLD_TEST=dropbulk,fetchlate";
    start(R);
    penv[R] = NULL;
    uint64_t t0 = now_ms();
    while (!__atomic_load_n(&w.done, __ATOMIC_ACQUIRE) || !__atomic_load_n(&wb.done, __ATOMIC_ACQUIRE)) {
        if (now_ms() - t0 > 60000)
            die("300 small writes and a large one with a replica down and back did not finish in 60 s");
        struct timespec ms = {0, 10000000};
        nanosleep(&ms, NULL);
    }
    pthread_join(th, NULL);
    pthread_join(thb, NULL);
    if (w.rc != 0)
        die("a small write with a replica down: %s", ers(w.rc));
    if (wb.rc != 0)
        die("a large write with a replica down: %s", ers(wb.rc));
    n[0] += 300;
    n[2] += 100000;
    /* the writes no longer wait for R: once R is silent for isr_lag_ms it leaves the ISR and they go on. R catches up
     * as a learner and grows back; only then has it surely seen the COMMIT marker and fetched the part. 60 s: under
     * TSan the restart, the catch-up and the fetch (5 s late on purpose) took the whole of 30 s (r3d-isr g3 tsan).
     * A replica that refuses its own log is named apart from a slow rejoin. (Under the default keep caps R removes no
     * segment here, so floornotsaved is repcheck's: its caps are 2 s.) */
    for (uint64_t t1 = now_ms(); isr_of(L, "plant", NULL) != 3;) {
        if (log_has(R, "the log starts above the commit floor"))
            die("the replica restarted under writes refused its log: the log starts above the commit floor");
        if (now_ms() - t1 > 60000)
            die("the replica restarted under writes: the ISR of plant has %" PRIu32 " members, not 3, after 60000 ms",
                isr_of(L, "plant", NULL));
        struct timespec ms = {0, 50000000};
        nanosleep(&ms, NULL);
    }
    if (!log_has(R, "fetches"))
        die("the replica restarted under a large write never fetched its part from the leader");
    await_open_gen(L, "plant", R, 2, 0);
    if ((uint64_t)rows_of(c, id[0]) != n[0])
        die("the leader reads %d rows of %s, not %" PRIu64, rows_of(c, id[0]), rep_names[0], n[0]);
    tsdbc_close(c);
    stop_all();
    rep_same(n, "after a clean stop");
    printf("  a replica killed under writes and restarted catches up, fetching the part it missed; every replica holds the leader's rows\n");
}

/* the ISR of volume `name` in server via's copy: its member count, *version its ISR version */
static uint32_t isr_of(int via, const char *name, uint64_t *version)
{
    struct meta_volume v;
    if (!vol_rec(via, name, &v))
        die("no record of %s", name);
    if (version)
        *version = v.isr_version;
    return v.nisr;
}

static void await_isr(int via, const char *name, uint32_t n, int ms, const char *what)
{
    uint64_t t0 = now_ms();
    while (isr_of(via, name, NULL) != n) {
        if (now_ms() - t0 > (uint64_t)ms)
            die("%s: the ISR of %s has %" PRIu32 " members, not %" PRIu32 ", after %d ms", what, name,
                isr_of(via, name, NULL), n, ms);
        struct timespec w = {0, 50000000};
        nanosleep(&w, NULL);
    }
}

static void hold_isr(int on)
{
    for (int i = 0; i < 3; i++) {
        char hold[PATH_MAX + 96];
        server_dir(i, hold, sizeof hold);
        strcat(hold, "/state/holdisr");
        if (on) {
            FILE *h = fopen(hold, "w");
            if (!h || fclose(h) != 0)
                die("%s: %s", hold, strerror(errno));
        } else if (unlink(hold) != 0 && errno != ENOENT) {
            die("%s: %s", hold, strerror(errno));
        }
    }
}

/* waits for a writer thread; dies after ms */
static void await_writer(struct rep_writer *w, pthread_t th, int ms, const char *what)
{
    uint64_t t0 = now_ms();
    while (!__atomic_load_n(&w->done, __ATOMIC_ACQUIRE)) {
        if (now_ms() - t0 > (uint64_t)ms)
            die("%s: not answered after %d ms", what, ms);
        struct timespec w10 = {0, 10000000};
        nanosleep(&w10, NULL);
    }
    pthread_join(th, NULL);
    if (w->rc != 0)
        die("%s: %s", what, ers(w->rc));
}

/* isr (design-cluster.md 6.1, 8.1 step 5): a replica that stops acknowledging leaves the ISR after isr_lag_ms through
 * an ISR_CHANGE the Raft leader proposes on the request's basis, and writes go on without it; back, it catches up and
 * joins again. While the change is not committed (hold_isr_file) the commit point still waits for the member (mutant
 * commitpastremoved); a late duplicate of the old shrink request (isr_replay) is refused at the apply (mutant
 * shrinknocond). */
static void scen_isr(void)
{
    scenario("isr");
    for (int i = 0; i < 3; i++) {
        envs[i] = "hold_isr_file=1,isr_replay=1";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    uint64_t rows = 0;
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    rows++;
    await_isr(0, "plant", 3, 5000, "every replica up");

    /* a killed replica while the metadata group commits no ISR change: the write waits */
    int R1 = (L + 1) % 3, R2 = (L + 2) % 3;
    hold_isr(1);
    stop(R1, SIGKILL);
    struct rep_writer w = {cports[L], id, 2000, 1, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    struct timespec s3 = {3, 0};
    nanosleep(&s3, NULL);
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE))
        die("a write was acknowledged without a killed ISR member before the ISR change committed (rc %d)", w.rc);
    if (isr_of(0, "plant", NULL) != 3)
        die("the ISR changed while the proposals were held");
    if (!log_has(L, "asks for an ISR of 2 members"))
        die("the leader never asked for an ISR without the killed replica");
    hold_isr(0);
    await_writer(&w, th, 5000, "the write held by an uncommitted ISR change, once the change could commit");
    rows++;
    await_isr(0, "plant", 2, 5000, "a replica killed");
    /* writes go on without it, each quickly */
    for (int j = 0; j < 50; j++) {
        uint64_t t0 = now_ms();
        int64_t t = 10000 + 1000LL * j;
        if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0)
            die("a write with a replica out of the ISR: %s", ers(rc));
        if (now_ms() - t0 > 1000)
            die("a write with a replica out of the ISR took %" PRIu64 " ms", now_ms() - t0);
        rows++;
    }
    uint64_t v1;
    isr_of(0, "plant", &v1);
    /* back, it catches up and joins; the old shrink request replayed late (isr_replay) must change nothing */
    start(R1);
    await_isr(0, "plant", 3, 30000, "the killed replica restarted");
    uint64_t v2;
    isr_of(0, "plant", &v2);
    nanosleep(&s3, NULL);
    uint64_t v3;
    if (isr_of(0, "plant", &v3) != 3 || v3 != v2)
        die("after the replica joined again the ISR moved: %" PRIu32 " members, version %" PRIu64 " -> %" PRIu64
            " (a stale ISR request applied)", isr_of(0, "plant", NULL), v2, v3);
    int replayed = 0;
    for (int i = 0; i < 3; i++)
        replayed |= log_has(i, "test: replays an ISR request");
    if (!replayed)
        die("no server replayed the old ISR request: the check above proves nothing");
    printf("  a killed replica leaves the ISR only once the change committed (versions %" PRIu64 ", %" PRIu64
           "), writes go on, and it joins again; a stale request is refused\n", v1, v2);

    /* a stopped replica (SIGSTOP: its stream breaks, no ACK) likewise, and comes back by itself */
    pause_server(R2, 1);
    for (int j = 0; j < 20; j++) {
        int64_t t = 100000 + 1000LL * j;
        if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0)
            die("a write with a replica stopped: %s", ers(rc));
        rows++;
    }
    await_isr(0, "plant", 2, 10000, "a replica stopped");
    pause_server(R2, 0);
    await_isr(0, "plant", 3, 30000, "the stopped replica continued");
    if ((uint64_t)rows_of(c, id) != rows)
        die("the leader reads %d rows, not %" PRIu64, rows_of(c, id), rows);
    tsdbc_close(c);
    stop_all();
    for (int i = 0; i < 3; i++)
        envs[i] = NULL;
    uint64_t wantr[REP_NAMES] = {rows, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
    printf("  a stopped replica leaves the ISR and joins again once it runs; every replica holds the leader's rows\n");
}

/* topology: TOPOLOGY, STATUS and MOVED of a cluster node come from its metadata copy (cluster_copy_get; S2-1). */
static const char *const topo_vols[] = {"east", "west"};

/* server i's TOPOLOGY against the records of server via's copy: the three members (client address, alive unless
 * server `down`), and for each volume one entry per replica with the record's epoch, its member's client address and
 * the role D9 gives it (leader OWNER, ISR STANDBY, else FOLLOWER); a replica of server `down` UNAVAILABLE
 * (UNREACHABLE). 0 when they agree, else a line in why; *followers counts the FOLLOWER entries. */
static int topo_agrees(int i, int via, int down, char *why, size_t wn, int *followers)
{
    tsdbc *c = client(i);
    tsdbc_topo *t = NULL;
    int rc = tsdbc_topology(c, &t), bad = 0;
    tsdbc_close(c);
    if (rc != 0) {
        snprintf(why, wn, "TOPOLOGY: %s", ers(rc));
        return -1;
    }
    *followers = 0;
    if (t->nnodes != 3) {
        snprintf(why, wn, "%zu nodes, not 3", t->nnodes);
        bad = 1;
    }
    for (int s = 0; s < 3 && !bad; s++) {
        char want[64];
        snprintf(want, sizeof want, "127.0.0.1:%d", cports[s]);
        size_t k = 0;
        while (k < t->nnodes && t->nodes[k].node_id != ids[s])
            k++;
        if (k == t->nnodes || strcmp(t->nodes[k].addr, want) != 0 || t->nodes[k].alive != (s != down)) {
            snprintf(why, wn, "node %u (server %d): %s, alive %d (want %s, alive %d)", (unsigned)ids[s], s,
                     k < t->nnodes ? t->nodes[k].addr : "missing", k < t->nnodes ? t->nodes[k].alive : -1, want,
                     s != down);
            bad = 1;
        }
    }
    size_t seen = 0;
    for (int n = 0; n < 2 && !bad; n++) {
        struct meta_volume v;
        if (!vol_rec(via, topo_vols[n], &v))
            die("topology: no record of %s", topo_vols[n]);
        for (uint32_t k = 0; k < v.nrep && !bad; k++) {
            uint32_t node = v.rep[k].node;
            int in_isr = 0;
            for (uint32_t j = 0; j < v.nisr; j++)
                in_isr |= v.isr[j] == node;
            int role = node == v.leader ? TSDB_ROLE_OWNER : in_isr ? TSDB_ROLE_STANDBY : TSDB_ROLE_FOLLOWER;
            int s = server_of(node);
            char want[64];
            snprintf(want, sizeof want, "127.0.0.1:%d", s >= 0 ? cports[s] : 0);
            size_t e = 0;
            while (e < t->nvols && !(t->vols[e].vol == v.number && t->vols[e].owner == node))
                e++;
            if (e == t->nvols) {
                snprintf(why, wn, "%s: no entry for node %u", topo_vols[n], (unsigned)node);
                bad = 1;
                break;
            }
            const tsdbc_vol *tv = &t->vols[e];
            seen++;
            *followers += tv->role == TSDB_ROLE_FOLLOWER;
            if (tv->role != role || tv->epoch != v.epoch || strcmp(tv->addr, want) != 0) {
                snprintf(why, wn, "%s node %u: role %u epoch %u addr %s, the record says role %d epoch %" PRIu64 " %s",
                         topo_vols[n], (unsigned)node, tv->role, tv->epoch, tv->addr, role, v.epoch, want);
                bad = 1;
            } else if (s == down && role != TSDB_ROLE_OWNER &&
                       (tv->health != TSDB_HEALTH_UNAVAILABLE || tv->reason != TSDB_WHY_UNREACHABLE)) {
                snprintf(why, wn, "%s: the down replica %u has health %u reason %u", topo_vols[n], (unsigned)node,
                         tv->health, tv->reason);
                bad = 1;
            } else if (s == down && role == TSDB_ROLE_OWNER &&
                       (tv->health != TSDB_HEALTH_UNAVAILABLE || tv->reason != TSDB_WHY_NO_OWNER)) {
                snprintf(why, wn, "%s: the down leader %u has health %u reason %u, not NO_OWNER", topo_vols[n],
                         (unsigned)node, tv->health, tv->reason);
                bad = 1;
            } else if (s != down && role == TSDB_ROLE_OWNER && i != s && tv->health != TSDB_HEALTH_AVAILABLE) {
                snprintf(why, wn, "%s: the leader %u, up and elsewhere, has health %u reason %u", topo_vols[n],
                         (unsigned)node, tv->health, tv->reason);
                bad = 1;
            }
        }
    }
    if (!bad && seen != t->nvols) {
        snprintf(why, wn, "%zu entries, the records have %zu", t->nvols, seen);
        bad = 1;
    }
    tsdbc_topology_free(t);
    return bad ? -1 : 0;
}

static void await_topo(int via, int down, int ms, int need_follower, const char *what)
{
    char why[512] = "";
    uint64_t t0 = now_ms();
    for (;;) {
        int ok = 1, fol = 0;
        for (int i = 0; i < 3 && ok; i++) {
            int f = 0;
            if (i != down && topo_agrees(i, via, down, why, sizeof why, &f) != 0) {
                ok = 0;
                snprintf(why + strlen(why), sizeof why - strlen(why), " (server %d)", i);
            }
            fol += f;
        }
        if (ok && (!need_follower || fol))
            return;
        if (ok)
            snprintf(why, sizeof why, "no entry is a FOLLOWER");
        if (now_ms() - t0 > (uint64_t)ms)
            die("%s: TOPOLOGY does not agree with the records after %d ms: %s", what, ms, why);
        usleep(100000);
    }
}

/* STATUS through c lists every server but `self` as a peer: its client and peer addresses, alive unless `down` */
static void status_peers(tsdbc *c, int self, int down, const char *what)
{
    char *text = NULL, line[160];
    int rc = tsdbc_status(c, &text);
    if (rc != 0)
        die("%s: STATUS: %s", what, ers(rc));
    if (!strstr(text, "\ncluster.applied="))
        die("%s: STATUS has no cluster.applied line", what);
    for (int s = 0; s < 3; s++) {
        if (s == self)
            continue;
        snprintf(line, sizeof line, "\npeer.%u.addr=127.0.0.1:%d\n", (unsigned)ids[s], cports[s]);
        int ok = strstr(text, line) != NULL;
        snprintf(line, sizeof line, "\npeer.%u.peer=127.0.0.1:%d\n", (unsigned)ids[s], pports[s]);
        ok = ok && strstr(text, line);
        snprintf(line, sizeof line, "\npeer.%u.alive=%d\n", (unsigned)ids[s], s != down);
        if (!ok || !strstr(text, line))
            die("%s: STATUS of server %d does not list server %d (node %u) as a peer at %d/%d, alive %d:\n%s", what,
                self, s, (unsigned)ids[s], cports[s], pports[s], s != down, text);
    }
    snprintf(line, sizeof line, "\npeer.%u.", (unsigned)ids[self]);
    if (strstr(text, line))
        die("%s: STATUS of server %d lists itself as a peer", what, self);
    free(text);
}

/* STATUS of server i through c says node.id=want */
static void status_id(tsdbc *c, uint32_t want, const char *what)
{
    char *text = NULL, line[64];
    int rc = tsdbc_status(c, &text);
    if (rc != 0)
        die("%s: STATUS: %s", what, ers(rc));
    snprintf(line, sizeof line, "node.id=%u\n", (unsigned)want);
    if (strncmp(text, line, strlen(line)) != 0)
        die("%s: STATUS begins \"%.40s\", not %s", what, text, line);
    free(text);
}

static void scen_topology(void)
{
    scenario("topology");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDBD_TEST_ROUTE_MOVED=1";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num[2];
    int rc;
    if ((rc = vol_add(0, "east", 3, &num[0])) != 0 || (rc = vol_add(0, "west", 2, &num[1])) != 0)
        die("volume add: %s", cluster_strerror(rc));
    for (int n = 0; n < 2; n++) {
        struct meta_volume v;
        if (!vol_rec(0, topo_vols[n], &v))
            die("no record of %s", topo_vols[n]);
        for (uint32_t k = 0; k < v.nrep; k++)
            await_open_gen(0, topo_vols[n], server_of(v.rep[k].node), 1, 0);
    }
    await_isr(0, "east", 3, 20000, "east with every replica up");
    /* every node answers the same leaders, epochs and replica sets (the records of server 0's copy) */
    await_topo(0, -1, 20000, 0, "every server up");
    /* STATUS: each server its own id (one of them is not 1) */
    for (int i = 0; i < 3; i++) {
        tsdbc *c = client(i);
        status_id(c, ids[i], "STATUS");
        status_peers(c, i, -1, "STATUS");
        tsdbc_close(c);
    }
    struct meta_volume ev;
    if (!vol_rec(0, "east", &ev))
        die("no record of east");
    int L = server_of(ev.leader);
    if (L < 0 || L > 2)
        die("the leader of east is node %u", ev.leader);
    int k = L == 0 ? 1 : 3 - L; /* neither the leader of east nor server 0 (the metadata group's first leader) */
    struct meta_volume wv;
    if (!vol_rec(0, "west", &wv))
        die("no record of west");
    int W = server_of(wv.leader);
    if (W > 0 && W != L)
        k = W;         /* kill west's leader where that is allowed: route_moved then says PROMOTING (leader not UP) */
    int j = 3 - L - k; /* the third: neither leads east */
    /* the replica of west that is not its leader, for route_moved's PROMOTING once that leader is down */
    int wr = -1;
    for (uint32_t r = 0; r < wv.nrep; r++)
        if (wv.rep[r].node != wv.leader)
            wr = server_of(wv.rep[r].node);
    tsdb_sid wid = 0;
    if (W == k && wr >= 0 && wr != k) {
        tsdbc *wc = client(W);
        wid = rep_series(wc, "pump.west", "west", 30000);
        tsdbc_close(wc);
    }
    /* MOVED from the copy (the test hook routes a write to a volume led elsewhere): the leader's id, epoch and client
     * address; the C client follows it and writes there */
    {
        tsdbc *lc = client(L);
        tsdb_sid id = rep_series(lc, "pump.moved", "east", 30000);
        tsdbc_close(lc);
        tsdbc *c = client(j);
        int64_t ts = 1000;
        float val = 1;
        rc = tsdbc_insert_series(c, id, &ts, &val, 1); /* by id: the hook sees the volume of an id */
        tsdbc_err_info e;
        char want_addr[64];
        snprintf(want_addr, sizeof want_addr, "127.0.0.1:%d", cports[L]);
        if (rc != TSDB_ERR_MOVED || tsdbc_last_error(c, &e) != TSDB_ERR_MOVED || e.nmoved != 1 ||
            e.moved[0].node_id != ev.leader || e.moved[0].epoch != ev.epoch || e.moved[0].vol != ev.number ||
            strcmp(e.moved[0].addr, want_addr) != 0)
            die("a write to east on server %d: %s, MOVED to node %u epoch %u at %s (want node %u epoch %" PRIu64
                " at %s)", j, ers(rc), e.nmoved ? (unsigned)e.moved[0].node_id : 0, e.nmoved ? e.moved[0].epoch : 0,
                e.nmoved ? e.moved[0].addr : "-", (unsigned)ev.leader, ev.epoch, want_addr);
        tsdbc_set_retry(c, 20000);
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("the client did not follow MOVED to the leader of east: %s", ers(rc));
        status_id(c, ev.leader, "STATUS after MOVED"); /* the connection is the leader's now */
        tsdbc_close(c);
    }
    /* a member marked down: alive 0 on every other node; once east's ISR drops it its entry is a FOLLOWER */
    stop(k, SIGKILL);
    struct state_want sw = {ids[k], META_DOWN, 0};
    await_view(L, p_member_state, &sw, "the killed member marked down");
    await_isr(L, "east", 2, 30000, "east without the killed replica");
    await_topo(L, k, 30000, 1, "a member down");
    {
        tsdbc *c = client(L);
        status_peers(c, L, k, "STATUS with a member down");
        tsdbc_close(c);
    }
    /* west's leader killed: a write on its other replica gets UNAVAILABLE (PROMOTING, the record names a leader whose
     * member is not UP: route_fill) from route_moved, not MOVED to the dead leader, while the record still names it (a
     * promotion would move the record on: then no verdict) */
    if (wid) {
        uint64_t t0 = now_ms();
        for (;;) {
            tsdbc *c = client(wr);
            int64_t ts = 2000;
            float val = 2;
            int rc2 = tsdbc_insert_series(c, wid, &ts, &val, 1);
            tsdbc_err_info e;
            tsdbc_last_error(c, &e);
            tsdbc_close(c);
            struct meta_volume now;
            if (!vol_rec(L, "west", &now) || now.leader != wv.leader) {
                printf("  (west was promoted away from its killed leader: no PROMOTING check)\n");
                break;
            }
            if (rc2 == TSDB_ERR_UNAVAILABLE && e.nunavail == 1 && e.unavail[0].reason == TSDB_WHY_PROMOTING &&
                e.nmoved == 0)
                break;
            if (now_ms() - t0 > 20000)
                die("a write to west on server %d with its leader (server %d) down: %s, %u MOVED, reason %u (want "
                    "UNAVAILABLE, PROMOTING)", wr, k, ers(rc2), e.nmoved, e.nunavail ? e.unavail[0].reason : 0);
            usleep(100000);
        }
    } else {
        printf("  (west's leader is server 0 or east's leader: no PROMOTING check of route_moved)\n");
    }
    stop_all();
    printf("  TOPOLOGY from the copy agrees on every node (roles, epochs, addresses, alive); STATUS names each node; "
           "MOVED names the leader and the C client follows it\n");
}

/* movedself: route_moved on the leader itself (TSDBD_TEST_ROUTE_MOVED=2 routes every INSERT to a cluster volume,
 * one led here too) answers UNAVAILABLE, never MOVED to its own address (a client would follow that in a circle
 * until its retry budget ran out); mutant ownerself */
static void scen_movedself(void)
{
    scenario("movedself");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    penv[0] = "TSDBD_TEST_ROUTE_MOVED=2";
    start(0);
    init_cluster(0, 600);
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "solo", 1, &num)) != 0)
        die("volume add: %s", cluster_strerror(rc));
    await_open_gen(0, "solo", 0, 1, 0);
    tsdbc *c = client(0);
    tsdb_sid id = rep_series(c, "pump.self", "solo", 30000);
    int64_t ts = 1000;
    float val = 1;
    uint64_t t0 = now_ms();
    for (;;) { /* the copy may not name this server as the leader yet: then NO_OWNER as well */
        rc = tsdbc_insert_series(c, id, &ts, &val, 1);
        tsdbc_err_info e;
        tsdbc_last_error(c, &e);
        if (rc == TSDB_ERR_MOVED || e.nmoved)
            die("the leader answered MOVED to itself (node %u at %s)", e.nmoved ? (unsigned)e.moved[0].node_id : 0,
                e.nmoved ? e.moved[0].addr : "-");
        if (rc != TSDB_ERR_UNAVAILABLE || e.nunavail != 1 || e.unavail[0].vol != num)
            die("the routed write on the leader: %s (%d), %u entries, volume %u, after %llu ms (want UNAVAILABLE for "
                "volume %u)", ers(rc), rc, e.nunavail, e.nunavail ? (unsigned)e.unavail[0].vol : 0,
                (unsigned long long)(now_ms() - t0), (unsigned)num);
        if (now_ms() - t0 > 3000)
            break;
        usleep(100000);
    }
    tsdbc_close(c);
    stop_all();
    printf("  route_moved on the volume's own leader answers UNAVAILABLE, never MOVED to itself\n");
}

/* rc is MOVED to the leader `node` of volume `vol` at server s's client address (the copy's MOVED, never UNAVAILABLE) */
static tsdbc *login_retry(int i, const char *user, const char *pw, int budget_ms)
{
    tsdbc *c = login_on(i, user, pw);
    if (tsdbc_set_retry(c, budget_ms) != 0)
        die("tsdbc_set_retry");
    return c;
}

/* route (S2-1, s2e: the entry node's decision). Three servers with login on; east on all three, south and north on one
 * each, lone on all three but shown with no leader in every copy (TSDB_CLUSTER_TEST copy_noleader). On a server that
 * does not lead east, writes and reads by id, by path and in packed frames are MOVED to east's leader (never answered
 * there: a replica serves no read), and the C client follows MOVED to the leader within its budget; lone answers
 * UNAVAILABLE (NO_OWNER) off its real leader; a user with a prefix grant writes through a server that holds no copy of
 * south and is moved, not DENIED, and an id outside the grant is still DENIED at the owner; a frame naming volumes
 * led on two servers is INVALID and writes nothing; a volume the copy has no record of is NO_OWNER, never an empty read.
 * Mutants (TSDB_ROUTE_MUTANT): rightsbeforeroute, movedoffcopy, replicaread, spanaccepted, nocopyserved. */
static int count_series(void *ctx, const tsdb_series_result *r)
{
    (void)r;
    ++*(int *)ctx;
    return 0;
}

/* The requests that name many series by id, sent off the owner of `id`'s volume: QUERY_MULTI, QUERY_LATEST,
 * STATS_SERIES and the unpacked many-series inserts (INSERT and INSERT_VALUES, compression off) are each MOVED to node
 * `node` at server s, and none is answered here (route_ids in every handler, review F2, F3a-c) */
static void moved_many(tsdbc *c, tsdb_sid id, uint32_t vol, uint32_t node, int s, const char *where)
{
    char what[160];
    int got = 0;
    tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = 0, .end = INT64_MAX / 2, .opts = {TSDB_ASC, 0}};
    snprintf(what, sizeof what, "QUERY_MULTI %s", where);
    expect_moved(c, tsdbc_query_multi(c, &q, &id, 1, NULL, count_series, &got), vol, node, s, what);
    if (got != 0)
        die("%s answered %d series before MOVED", what, got);
    tsdb_latest_result lr;
    snprintf(what, sizeof what, "QUERY_LATEST %s", where);
    expect_moved(c, tsdbc_latest(c, &id, 1, NULL, &lr), vol, node, s, what);
    tsdb_series_stat st;
    snprintf(what, sizeof what, "STATS_SERIES %s", where);
    expect_moved(c, tsdbc_series_stats(c, &id, 1, 0, &st), vol, node, s, what);
    tsdb_sid two[2] = {id, id};
    int64_t ts[2] = {5000, 6000};
    float val[2] = {5, 6};
    tsdb_values col = {TSDB_FLOAT32, val, NULL};
    int rc;
    if ((rc = tsdbc_set_compression(c, 0)) != 0)
        die("compression off: %s", ers(rc));
    snprintf(what, sizeof what, "an unpacked many-series INSERT %s", where);
    expect_moved(c, tsdbc_insert(c, two, ts, val, 2), vol, node, s, what);
    snprintf(what, sizeof what, "an unpacked many-series INSERT_VALUES %s", where);
    expect_moved(c, tsdbc_insert_values(c, two, ts, &col, 2), vol, node, s, what);
    if ((rc = tsdbc_set_compression(c, 1)) != 0)
        die("compression on: %s", ers(rc));
}

static void scen_route(void)
{
    scenario("route");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    const char *mut = getenv("TSDB_ROUTE_MUTANT");
    static char menv[128];
    snprintf(menv, sizeof menv, "TSDB_ROUTE_MUTANT=%s", mut ? mut : "");
    for (int i = 0; i < 3; i++) {
        auth_on[i] = 1;
        envs[i] = "copy_noleader=2";
        if (mut)
            penv[i] = menv;
        start(i);
    }
    char pw[PATH_MAX + 80], pw2[PATH_MAX + 80], args[PATH_MAX * 2 + 512], out[8192], p0[META_ADDR_MAX];
    put_file("pw", "root-pw", pw, sizeof pw);
    put_file("pw2", "alice-pw", pw2, sizeof pw2);
    peer_of(0, p0, sizeof p0);
    snprintf(args, sizeof args,
             "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 --voter-replace-s 600 "
             "--superuser root --password-file %s",
             p0, secret_path, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("cluster init --superuser root: %s", out);
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    snprintf(args, sizeof args, "cluster user add %s alice --password-file %s --secret-file %s", p0, pw2,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("user add alice: %s", out);
    for (int i = 0; i < 3; i++) {
        await_view(i, p_users_caught_up, NULL, "users caught up");
        await_login(i, "root", "root-pw");
    }
    static const char *const names[4] = {"east", "lone", "south", "north"};
    static const uint32_t reps[4] = {3, 3, 1, 1};
    uint32_t num[4];
    struct meta_volume v[4];
    int rc;
    for (int n = 0; n < 4; n++)
        if ((rc = vol_add(0, names[n], reps[n], &num[n])) != 0)
            die("volume add %s: %s", names[n], cluster_strerror(rc));
    if (num[1] != 2)
        die("lone is volume %u, not 2 (copy_noleader names 2)", (unsigned)num[1]);
    for (int n = 0; n < 4; n++) {
        if (!vol_rec(0, names[n], &v[n]))
            die("no record of %s", names[n]);
        for (uint32_t k = 0; k < v[n].nrep; k++)
            await_open_gen(0, names[n], server_of(v[n].rep[k].node), 1, 0);
    }
    await_isr(0, "east", 3, 20000, "east with every replica up");
    int L = server_of(v[0].leader), R = (L + 1) % 3, LL = server_of(v[1].leader);
    {
        tsdbc *r0 = login_on(0, "root", "root-pw");
        if ((rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ | TSDB_RIGHT_INSERT)) != 0)
            die("grant: %s", ers(rc));
        tsdbc_close(r0);
    }

    /* east's leader serves; a replica of it moves writes, reads, paths and packed frames to the leader */
    tsdbc *c = login_retry(L, "root", "root-pw", 0);
    tsdb_sid id = rep_series(c, "plant.a", "east", 30000), other = rep_series(c, "other.a", "east", 30000);
    int64_t ts[2] = {1000, 2000};
    float val[2] = {1, 2};
    if ((rc = tsdbc_insert_series(c, id, ts, val, 1)) != 0)
        die("a write on east's leader: %s", ers(rc));
    tsdbc_close(c);
    c = login_retry(R, "root", "root-pw", 0);
    tsdb_sid x;
    expect_moved(c, tsdbc_insert_series(c, id, ts + 1, val + 1, 1), num[0], v[0].leader, L, "a write on a replica");
    nrows = 0;
    expect_moved(c, tsdbc_query_raw(c, id, 0, INT64_MAX / 2, count_rows, NULL), num[0], v[0].leader, L,
                 "a read on a replica");
    if (nrows != 0)
        die("a replica answered %d rows of a read before MOVED", nrows);
    expect_moved(c, tsdbc_series_at(c, "plant.b", "east", &x), num[0], v[0].leader, L, "a series by path on a replica");
    tsdb_sid two[2] = {id, id};
    expect_moved(c, tsdbc_insert(c, two, ts, val, 2), num[0], v[0].leader, L, "a packed write on a replica");
    moved_many(c, id, num[0], v[0].leader, L, "on a replica");
    tsdbc_close(c);
    c = login_retry(R, "root", "root-pw", 20000);
    if ((rc = tsdbc_insert_series(c, id, ts + 1, val + 1, 1)) != 0)
        die("the client did not follow MOVED to east's leader: %s", ers(rc));
    status_id(c, v[0].leader, "STATUS after a write was moved");
    tsdbc_close(c);
    c = login_retry(R, "root", "root-pw", 20000);
    if (rows_of(c, id) != 2)
        die("the read through a replica, moved to the leader, has %d rows, not 2", nrows);
    status_id(c, v[0].leader, "STATUS after a read was moved");
    tsdb_latest_result lr;
    if ((rc = tsdbc_latest(c, &id, 1, NULL, &lr)) != 0 || lr.status != 0 || lr.ts != 2000)
        die("QUERY_LATEST through a replica, moved to the leader: %s, status %d, ts %" PRId64 ", not 2000", ers(rc),
            (int)lr.status, lr.ts);
    tsdbc_close(c);
    printf("  a replica moves writes, reads, paths, packed and many-series frames, QUERY_MULTI, QUERY_LATEST and "
           "STATS_SERIES to the leader; the client follows MOVED\n");

    /* lone: no leader in the copy; its real leader serves it (led here), every other server answers NO_OWNER */
    int nl = LL == 0 ? 1 : 0;
    c = login_retry(nl, "root", "root-pw", 0);
    expect_unavail(c, tsdbc_series_at(c, "plant.c", "lone", &x), (uint16_t)num[1], TSDB_WHY_NO_OWNER,
                   "a volume with no leader in the copy");
    /* a volume the copy has no record of (here never made; in jepsen-cluster a restarted server before its copy was
     * built): not open here, so the engine would answer an empty read; NO_OWNER until the copy knows it */
    tsdb_sid ghost = TSDB_SID(9, 0);
    nrows = 0;
    expect_unavail(c, tsdbc_query_raw(c, ghost, 0, INT64_MAX / 2, count_rows, NULL), 9, TSDB_WHY_NO_OWNER,
                   "a read of a volume the copy does not know");
    if (nrows != 0)
        die("a read of a volume the copy does not know answered %d rows", nrows);
    expect_unavail(c, tsdbc_insert_series(c, ghost, ts, val, 1), 9, TSDB_WHY_NO_OWNER,
                   "a write to a volume the copy does not know");
    tsdbc_close(c);
    printf("  a volume with no leader in the copy, or none in it: UNAVAILABLE (NO_OWNER)\n");

    /* south, on one server: a user with a prefix grant writes by id through a server without a copy of it and
     * is moved, never DENIED there (the id is not resolved through that server's names); outside the grant: DENIED at
     * the owner */
    int so = server_of(v[2].leader), to = (so + 1) % 3, no = server_of(v[3].leader);
    c = login_retry(so, "root", "root-pw", 0);
    tsdb_sid sid = rep_series(c, "plant.s", "south", 30000), sother = rep_series(c, "other.s", "south", 30000);
    tsdbc_close(c);
    tsdbc *a = login_retry(to, "alice", "alice-pw", 0);
    expect_moved(a, tsdbc_insert_series(a, sid, ts, val, 1), num[2], v[2].leader, so, "alice's write off the owner");
    moved_many(a, sid, num[2], v[2].leader, so, "by alice off the owner");
    tsdbc_close(a);
    a = login_retry(to, "alice", "alice-pw", 20000);
    if ((rc = tsdbc_insert_series(a, sid, ts, val, 1)) != 0)
        die("alice's write through server %d, moved to south's owner: %s", to, ers(rc));
    tsdbc_close(a);
    a = login_retry(to, "alice", "alice-pw", 20000);
    if ((rc = tsdbc_insert_series(a, sother, ts, val, 1)) != TSDB_ERR_DENIED)
        die("alice's write outside her grant, moved to the owner: %s, not DENIED", ers(rc));
    tsdbc_close(a);
    a = login_retry(R, "alice", "alice-pw", 20000);
    if ((rc = tsdbc_insert_series(a, other, ts, val, 1)) != TSDB_ERR_DENIED)
        die("alice's write to east outside her grant through a replica: %s, not DENIED", ers(rc));
    tsdbc_close(a);
    printf("  a user is moved before her rights are checked, and DENIED at the owner outside her grant\n");

    /* a frame naming volumes led on two servers: INVALID, nothing written */
    int sa = -1, sb = -1;
    tsdb_sid ia = 0, ib = 0;
    if (so != L)
        sa = L, sb = so, ia = id, ib = sid;
    else if (no != L) {
        c = login_retry(no, "root", "root-pw", 0);
        ib = rep_series(c, "plant.n", "north", 30000);
        tsdbc_close(c);
        sa = L, sb = no, ia = id;
    }
    /* placement is the metadata's: when east, south and north are led on one server, more volumes are made until
     * one is led elsewhere (the check never skips itself: review F4) */
    for (int k = 1; sa < 0 && k <= 6; k++) {
        char nm[16];
        uint32_t nn;
        struct meta_volume w;
        snprintf(nm, sizeof nm, "span%d", k);
        if ((rc = vol_add(0, nm, 1, &nn)) != 0)
            die("volume add %s: %s", nm, cluster_strerror(rc));
        if (!vol_rec(0, nm, &w))
            die("no record of %s", nm);
        int ws = server_of(w.leader);
        if (ws < 0)
            die("%s is led by node %u, no server", nm, (unsigned)w.leader);
        await_open_gen(0, nm, ws, 1, 0);
        if (ws != L) {
            c = login_retry(ws, "root", "root-pw", 0);
            ib = rep_series(c, "plant.p", nm, 30000);
            tsdbc_close(c);
            sa = L, sb = ws, ia = id;
        }
    }
    if (sa < 0)
        die("east, south, north and six more volumes are all led on server %d: no spanning check", L);
    {
        tsdb_sid span[2] = {ia, ib};
        int64_t sts[2] = {9000, 9000};
        for (int via = 0; via < 3; via++) {
            c = login_retry(via, "root", "root-pw", 0);
            if ((rc = tsdbc_insert(c, span, sts, val, 2)) != TSDB_ERR_INVALID)
                die("a frame naming volumes led on servers %d and %d, through server %d: %s, not INVALID", sa, sb, via,
                    ers(rc));
            tsdbc_close(c);
        }
        for (int k = 0; k < 2; k++) {
            c = login_retry(k ? sb : sa, "root", "root-pw", 0);
            nrows = 0;
            if ((rc = tsdbc_query_raw(c, span[k], 9000, 9001, count_rows, NULL)) != 0 || nrows != 0)
                die("the refused spanning frame left %d rows on server %d (%s)", nrows, k ? sb : sa, ers(rc));
            tsdbc_close(c);
        }
        printf("  a frame naming volumes led on two servers: INVALID on every server, nothing written\n");
    }
    stop_all();
}

/* forward (S2-2: one hop to the volume's leader). Three servers with login on, strict reads, every server with two
 * workers (--workers 2); east on all three. Through a replica R of east, writes (by id, packed, many-series, a series by
 * path) are applied on east's leader L and every read (raw, buckets, LTTB, TWA, QUERY_MULTI, QUERY_LATEST,
 * STATS_SERIES) answers what L answers, with LAST_STATS from L's tail and the hop counted in R's STATUS; a user's
 * forwarded write is judged by her rights at the owner (DENIED outside her grant); a frame held at the entry past
 * fwd_max_age_ms is UNAVAILABLE and never applied; a forward waiting on a stopped owner leaves R's own volumes served
 * (the wait counts as a waiting worker); a written forward whose link breaks closes the client's connection (unknown
 * outcome), never UNAVAILABLE; a user added while the owner's users lag (TSDB_CLUSTER_TEST hold_users_file) is retried
 * through UNAVAILABLE until the owner has her, never refused AUTH. Through TSDBD_TEST_FWD_KNOBS: a request held at the
 * entry after its parse (j) or at the owner before it runs (k) past fwd_max_age_ms is UNAVAILABLE and never applied; a
 * forward the owner cannot read closes the client's connection at once (l: the owner's unknown outcome, D12); a
 * forward stamped with another epoch than its owner's is refused (n: UNAVAILABLE or MOVED, not applied); a client that leaves while its forward waits has
 * it given up within about a second (m, STATUS fwd.inflight). Mutants (TSDBD_FWD_MUTANT): fwdoldframe, fwdunavonbreak,
 * fwdnouser, fwdnowait, fwdauthlag, fwdarrival, fwdownerage, fwdownerclose, fwdtailclose, fwdepoch, fwdgone. */
static uint64_t fw_fnv(uint64_t h, const void *p, size_t n)
{
    const uint8_t *b = p;
    for (size_t i = 0; i < n; i++)
        h = (h ^ b[i]) * 1099511628211ull;
    return h;
}

struct fw_acc {
    uint64_t h;
    size_t n;
};

static int fw_vals(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    struct fw_acc *a = ctx;
    a->h = fw_fnv(a->h, ts, n * sizeof *ts);
    if (v->type == TSDB_FLOAT32)
        a->h = fw_fnv(a->h, v->values, n * sizeof(float));
    a->n += n;
    return 0;
}

static int fw_raw(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    struct fw_acc *a = ctx;
    a->h = fw_fnv(fw_fnv(a->h, ts, n * sizeof *ts), v, n * sizeof *v);
    a->n += n;
    return 0;
}

static int fw_multi(void *ctx, const tsdb_series_result *r)
{
    struct fw_acc *a = ctx;
    if (r->status != 0)
        die("QUERY_MULTI: series %zu answered %s", r->index, ers(r->status));
    a->h = fw_fnv(fw_fnv(a->h, &r->index, sizeof r->index), r->ts, r->n * sizeof *r->ts);
    a->n += r->n;
    return 0;
}

/* every read of ids[0..n) through c, as one line to compare between servers */
static void fw_answers(tsdbc *c, const tsdb_sid *ids, size_t n, char *out, size_t cap, const char *where)
{
    int rc;
    size_t o = 0;
    for (size_t k = 0; k < n; k++) {
        struct fw_acc raw = {14695981039346656037ull, 0}, lt = raw;
        if ((rc = tsdbc_query_raw(c, ids[k], 0, INT64_MAX / 2, fw_raw, &raw)) != 0)
            die("QUERY_RAW %s: %s", where, ers(rc));
        tsdb_query_stats qs;
        if ((rc = tsdbc_last_query_stats(c, &qs)) != 0 || qs.rows != raw.n)
            die("LAST_STATS %s after %zu rows: %s, rows %" PRIu64, where, raw.n, ers(rc), qs.rows);
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if ((rc = tsdbc_query_agg(c, ids[k], 0, INT64_MAX / 2, 60000, &b, &nb)) != 0)
            die("buckets %s: %s", where, ers(rc));
        uint64_t hb = fw_fnv(14695981039346656037ull, b, nb * sizeof *b);
        free(b);
        if ((rc = tsdbc_query_lttb(c, ids[k], 0, INT64_MAX / 2, 100, fw_vals, &lt)) != 0)
            die("LTTB %s: %s", where, ers(rc));
        tsdb_twa_bucket *t = NULL;
        size_t nt = 0;
        tsdb_twa_opts to = {0};
        if ((rc = tsdbc_query_twa(c, ids[k], 0, 10000000, 600000, &to, &t, &nt)) != 0)
            die("TWA %s: %s", where, ers(rc));
        uint64_t ht = fw_fnv(14695981039346656037ull, t, nt * sizeof *t);
        free(t);
        o += (size_t)snprintf(out + o, o < cap ? cap - o : 0,
                              "[%zu] raw %zu/%016" PRIx64 " buckets %zu/%016" PRIx64 " lttb %zu/%016" PRIx64
                              " twa %zu/%016" PRIx64 "; ",
                              k, raw.n, raw.h, nb, hb, lt.n, lt.h, nt, ht);
    }
    struct fw_acc m = {14695981039346656037ull, 0};
    tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = 0, .end = INT64_MAX / 2, .opts = {TSDB_ASC, 0}};
    if ((rc = tsdbc_query_multi(c, &q, ids, n, NULL, fw_multi, &m)) != 0)
        die("QUERY_MULTI %s: %s", where, ers(rc));
    tsdb_latest_result lr[8];
    tsdb_series_stat st[8];
    if (n > 8)
        die("fw_answers: %zu ids", n);
    if ((rc = tsdbc_latest(c, ids, n, NULL, lr)) != 0)
        die("QUERY_LATEST %s: %s", where, ers(rc));
    if ((rc = tsdbc_series_stats(c, ids, n, 0, st)) != 0)
        die("STATS_SERIES %s: %s", where, ers(rc));
    o += (size_t)snprintf(out + o, o < cap ? cap - o : 0, "multi %zu/%016" PRIx64 " latest", m.n, m.h);
    for (size_t k = 0; k < n; k++)
        o += (size_t)snprintf(out + o, o < cap ? cap - o : 0, " %d/%" PRId64 "/%g stats %d/%" PRIu64, (int)lr[k].status,
                              lr[k].ts, (double)lr[k].v.f32, (int)st[k].status, st[k].rows);
}

/* the frames forwarded from this server to node `peer` (STATUS peer.<peer>.fwd.frames) */
static uint64_t fw_frames(tsdbc *c, uint32_t peer)
{
    char *text = NULL, key[64];
    int rc = tsdbc_status(c, &text);
    if (rc != 0)
        die("STATUS: %s", ers(rc));
    snprintf(key, sizeof key, "\npeer.%u.fwd.frames=", (unsigned)peer);
    const char *p = strstr(text, key);
    uint64_t n = p ? strtoull(p + strlen(key), NULL, 10) : 0;
    free(text);
    return n;
}

struct fw_write {
    tsdbc *c;
    tsdb_sid id;
    int64_t ts;
    int rc;
    uint64_t ms;
};

static void *fw_write_run(void *arg)
{
    struct fw_write *w = arg;
    float v = 7;
    uint64_t t0 = now_ms();
    w->rc = tsdbc_insert_series(w->c, w->id, &w->ts, &v, 1);
    w->ms = now_ms() - t0;
    return NULL;
}

/* rows of id at ts on server s (through its leader: s is the leader) */
static int fw_has(int s, tsdb_sid id, int64_t ts)
{
    tsdbc *c = login_retry(s, "root", "root-pw", 0);
    nrows = 0;
    int rc = tsdbc_query_raw(c, id, ts, ts + 1, count_rows, NULL);
    if (rc != 0)
        die("read of %" PRId64 " on server %d: %s", ts, s, ers(rc));
    tsdbc_close(c);
    return nrows;
}

/* a value of STATUS on c ("fwd.inflight"); 0 when absent */
static uint64_t fw_stat(tsdbc *c, const char *name)
{
    char *text = NULL, key[96];
    int rc = tsdbc_status(c, &text);
    if (rc != 0)
        die("STATUS: %s", ers(rc));
    snprintf(key, sizeof key, "\n%s=", name);
    const char *p = strstr(text, key);
    uint64_t n = p ? strtoull(p + strlen(key), NULL, 10) : 0;
    free(text);
    return n;
}

/* TSDBD_TEST_FWD_KNOBS (server/tsdbd.c fwd_knob): <base>/fwdknobs/NAME exists while on */
static void fw_knob(const char *name, int on)
{
    char p[PATH_MAX + 80];
    snprintf(p, sizeof p, "%s/fwdknobs", base);
    mkdir(p, 0755);
    snprintf(p, sizeof p, "%s/fwdknobs/%s", base, name);
    if (on) {
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0)
            die("%s: %s", p, strerror(errno));
        close(fd);
    } else if (unlink(p) != 0 && errno != ENOENT) {
        die("unlink %s: %s", p, strerror(errno));
    }
}

/* one write through R held by the knob for 1.5 s: it must be answered UNAVAILABLE and never reach L */
static void fw_held_write(int L, int R, tsdb_sid id, int64_t ts, const char *knob, const char *what)
{
    struct fw_write w = {.c = login_retry(R, "root", "root-pw", 0), .id = id, .ts = ts};
    fw_knob(knob, 1);
    pthread_t th;
    if (pthread_create(&th, NULL, fw_write_run, &w) != 0)
        die("pthread_create");
    usleep(1500000);
    fw_knob(knob, 0);
    pthread_join(th, NULL);
    tsdbc_close(w.c);
    if (w.rc != TSDB_ERR_UNAVAILABLE)
        die("a write held 1.5 s %s: %s, not UNAVAILABLE", what, ers(w.rc));
    usleep(300000);
    if (fw_has(L, id, ts) != 0)
        die("a write held 1.5 s %s and answered UNAVAILABLE was applied on the leader", what);
    printf("  a write held past fwd_max_age_ms %s is UNAVAILABLE and never applied\n", what);
}

static void scen_forward(void)
{
    scenario("forward");
    strict_reads = 1;
    fwd_on = 1;
    const char *mut = getenv("TSDBD_FWD_MUTANT");
    char stall[PATH_MAX + 80];
    snprintf(stall, sizeof stall, "%s/fwd-stall", base);
    static char menv[PATH_MAX * 2 + 400];
    snprintf(menv, sizeof menv, "TSDBD_FWD_MUTANT=%s TSDBD_TEST_FWD_STALL=%s TSDBD_TEST_FWD_KNOBS=%s/fwdknobs",
             mut ? mut : "", stall, base);
    for (int i = 0; i < 3; i++) {
        auth_on[i] = 1;
        penv[i] = menv;
        envs[i] = "hold_users_file=1"; /* part i: <state>/holdusers on the owner makes its users lag */
        xargs[i] = "--workers 2";
        start(i);
    }
    char pw[PATH_MAX + 80], pw2[PATH_MAX + 80], args[PATH_MAX * 2 + 512], out[8192], p0[META_ADDR_MAX];
    put_file("pw", "root-pw", pw, sizeof pw);
    put_file("pw2", "alice-pw", pw2, sizeof pw2);
    peer_of(0, p0, sizeof p0);
    snprintf(args, sizeof args,
             "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 --voter-replace-s 600 "
             "--superuser root --password-file %s",
             p0, secret_path, pw);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("cluster init --superuser root: %s", out);
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    snprintf(args, sizeof args, "cluster user add %s alice --password-file %s --secret-file %s", p0, pw2,
             secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0)
        die("user add alice: %s", out);
    for (int i = 0; i < 3; i++) {
        await_view(i, p_users_caught_up, NULL, "users caught up");
        await_login(i, "root", "root-pw");
    }
    uint32_t num;
    struct meta_volume v;
    int rc;
    if ((rc = vol_add(0, "east", 3, &num)) != 0)
        die("volume add east: %s", cluster_strerror(rc));
    if (!vol_rec(0, "east", &v))
        die("no record of east");
    for (uint32_t k = 0; k < v.nrep; k++)
        await_open_gen(0, "east", server_of(v.rep[k].node), 1, 0);
    await_isr(0, "east", 3, 20000, "east with every replica up");
    int L = server_of(v.leader), R = (L + 1) % 3;
    {
        tsdbc *r0 = login_on(0, "root", "root-pw");
        if ((rc = tsdbc_grant(r0, "alice", TSDB_SEL_PREFIX, "plant.", TSDB_RIGHT_READ | TSDB_RIGHT_INSERT)) != 0)
            die("grant: %s", ers(rc));
        tsdbc_close(r0);
    }

    /* a. through the replica R: every write lands on L, every read answers L's answer */
    tsdbc *c = login_retry(L, "root", "root-pw", 0);
    tsdb_sid id = rep_series(c, "plant.a", "east", 30000), other = rep_series(c, "other.a", "east", 30000);
    tsdbc_close(c);
    c = login_retry(R, "root", "root-pw", 0); /* budget 0: a MOVED would fail the call */
    uint64_t f0 = fw_frames(c, v.leader);
    tsdb_sid pid;
    if ((rc = tsdbc_series_at(c, "plant.p", "east", &pid)) != 0)
        die("a series by path through the replica: %s", ers(rc));
    enum { NR = 3000 };
    static int64_t ts[NR];
    static float val[NR];
    for (int k = 0; k < NR; k++)
        ts[k] = 1000 + (int64_t)k * 1000, val[k] = (float)(k % 97) * 0.5f;
    if ((rc = tsdbc_insert_series(c, id, ts, val, NR / 2)) != 0)
        die("a write by id through the replica: %s", ers(rc));
    tsdb_sid two[4] = {id, pid, other, id};
    int64_t ts2[4] = {ts[NR / 2], 1000, 1000, ts[NR / 2 + 1]};
    float v2[4] = {1, 2, 3, 4};
    if ((rc = tsdbc_insert(c, two, ts2, v2, 4)) != 0) /* packed */
        die("a packed write through the replica: %s", ers(rc));
    if ((rc = tsdbc_set_compression(c, 0)) != 0)
        die("compression off: %s", ers(rc));
    int64_t ts3[2] = {ts[NR / 2 + 2], 2000};
    tsdb_sid two3[2] = {id, pid};
    if ((rc = tsdbc_insert(c, two3, ts3, v2, 2)) != 0)
        die("an unpacked many-series write through the replica: %s", ers(rc));
    if ((rc = tsdbc_set_compression(c, 1)) != 0)
        die("compression on: %s", ers(rc));
    if ((rc = tsdbc_insert_series(c, id, ts + NR / 2 + 3, val + NR / 2 + 3, NR - NR / 2 - 3)) != 0)
        die("a second write by id through the replica: %s", ers(rc));
    static char ar[4096], al[4096];
    tsdb_sid q3[3] = {id, pid, other};
    fw_answers(c, q3, 3, ar, sizeof ar, "through the replica");
    uint64_t f1 = fw_frames(c, v.leader);
    tsdbc_close(c);
    c = login_retry(L, "root", "root-pw", 0);
    fw_answers(c, q3, 3, al, sizeof al, "on the leader");
    if (rows_of(c, id) != NR)
        die("east's leader holds %d rows of plant.a, not %d", nrows, NR);
    tsdbc_close(c);
    if (strcmp(ar, al) != 0)
        die("the answers through the replica differ from the leader's:\n  R: %s\n  L: %s", ar, al);
    if (f1 < f0 + 10)
        die("STATUS of the replica counts %" PRIu64 " frames forwarded to node %u, not at least 10 more than %" PRIu64,
            f1, (unsigned)v.leader, f0);
    printf("  through a replica: writes by id, packed, many-series and by path land on the leader; raw, buckets, LTTB, "
           "TWA, QUERY_MULTI, QUERY_LATEST and STATS_SERIES answer the leader's answers (%" PRIu64 " frames)\n",
           f1 - f0);

    /* b. a user's forwarded write is judged at the owner by her rights */
    tsdbc *a = login_retry(R, "alice", "alice-pw", 0);
    int64_t tb = 99000000;
    float vb = 1;
    if ((rc = tsdbc_insert_series(a, id, &tb, &vb, 1)) != 0)
        die("alice's write in her grant through the replica: %s", ers(rc));
    if ((rc = tsdbc_insert_series(a, other, &tb, &vb, 1)) != TSDB_ERR_DENIED)
        die("alice's write outside her grant through the replica: %s, not DENIED", ers(rc));
    tsdbc_close(a);
    if (fw_has(L, id, tb) != 1 || fw_has(L, other, tb) != 0)
        die("after alice's writes through the replica the leader holds %d row(s) in her grant and %d outside it",
            fw_has(L, id, tb), fw_has(L, other, tb));
    printf("  a user's write through a replica is applied in her grant and DENIED at the owner outside it\n");

    /* i. a user added while the owner's users lag (its watch held): she logs in on the entry, her forwarded write is
     * UNAVAILABLE (RELOADING) at the owner until it has her, which the client retries, never AUTH (fwdauthlag) */
    {
        char holdu[PATH_MAX + 96], pw3[PATH_MAX + 80];
        server_dir(L, holdu, sizeof holdu);
        strcat(holdu, "/state/holdusers");
        FILE *hu = fopen(holdu, "w");
        if (!hu)
            die("%s: %s", holdu, strerror(errno));
        fclose(hu);
        usleep(300000); /* a round of L's cluster thread: its watch has stopped */
        put_file("pw3", "bob-pw", pw3, sizeof pw3);
        snprintf(args, sizeof args, "cluster user add %s bob --superuser --password-file %s --secret-file %s --wait-ms 200",
                 p0, pw3, secret_path);
        if (cli_run(args, NULL, out, sizeof out) != 0 && !strstr(out, "lag"))
            die("user add bob: %s", out);
        await_login(R, "bob", "bob-pw");
        struct fw_write wb = {.c = login_retry(R, "bob", "bob-pw", 20000), .id = id, .ts = 97000000};
        pthread_t tb2;
        if (pthread_create(&tb2, NULL, fw_write_run, &wb) != 0)
            die("pthread_create");
        usleep(1000000);
        unlink(holdu);
        pthread_join(tb2, NULL);
        tsdbc_close(wb.c);
        if (wb.rc != 0)
            die("bob's write through the replica while the owner's users lagged: %s, not retried until it had him",
                ers(wb.rc));
        if (wb.ms < 800)
            die("bob's write through the replica was answered after %" PRIu64 " ms with the owner's users held 1 s: "
                "the lag was not exercised", wb.ms);
        if (fw_has(L, id, wb.ts) != 1)
            die("bob's write through the replica is not on the leader");
        printf("  a user added while the owner's users lagged: her forwarded write waited %" PRIu64
               " ms (UNAVAILABLE, retried), never AUTH\n", wb.ms);
    }

    /* c. a frame held at the entry past fwd_max_age_ms (1 s): UNAVAILABLE, never applied */
    struct fw_write w = {.c = login_retry(R, "root", "root-pw", 0), .id = id, .ts = 98000000};
    put_file("fwd-stall", "", stall, sizeof stall);
    pthread_t th;
    if (pthread_create(&th, NULL, fw_write_run, &w) != 0)
        die("pthread_create");
    usleep(1500000);
    unlink(stall);
    pthread_join(th, NULL);
    tsdbc_close(w.c);
    if (w.rc != TSDB_ERR_UNAVAILABLE)
        die("a write held 1.5 s at the entry: %s, not UNAVAILABLE", ers(w.rc));
    usleep(300000);
    if (fw_has(L, id, w.ts) != 0)
        die("a write held 1.5 s at the entry and answered UNAVAILABLE was applied on the leader");
    printf("  a write held past fwd_max_age_ms at the entry is UNAVAILABLE and never applied\n");

    /* j. held at the entry after its parse but before its forward is built: the age counts from the parse (a request
     *    that waited for a worker is as old as one that waited for the link) */
    fw_held_write(L, R, id, 98000100, "prestall", "at the entry before its forward began");
    /* k. held at the owner before it runs the forward: the owner's own check (envelope age plus its queue) */
    fw_held_write(L, R, id, 98000200, "ownerstall", "at the owner before it ran the forward");

    /* l. a forward the owner cannot read: the owner answers by closing (unknown outcome, D12), and the entry closes the
     *    client's connection at once instead of leaving it waiting for a reply that never comes */
    {
        struct fw_write wl = {.c = login_retry(R, "root", "root-pw", 0), .id = id, .ts = 98000300};
        tsdbc_set_timeout(wl.c, 8000);
        fw_knob("badenv", 1);
        fw_write_run(&wl);
        fw_knob("badenv", 0);
        tsdbc_close(wl.c);
        if (wl.rc != TSDB_ERR_IO || wl.ms >= 4000)
            die("a forward the owner could not read: %s after %" PRIu64 " ms, not a closed connection at once (IO)",
                ers(wl.rc), wl.ms);
        if (fw_has(L, id, wl.ts) != 0)
            die("a forward the owner could not read was applied");
        printf("  a forward the owner could not read closes the client's connection (%" PRIu64 " ms)\n", wl.ms);
    }

    /* n. the owner leads at another epoch than the one the entry stamped: refused before anything is done, never run.
     *    The owner's copy names itself, so route_fill answers UNAVAILABLE (MOVED to itself would send the client round in
     *    a circle); an owner whose copy names another leader answers MOVED. Either says "not applied": the client resends
     *    and the entry node routes again. */
    {
        tsdbc *cn = login_retry(R, "root", "root-pw", 0);
        int64_t tn = 98000400;
        float vn = 1;
        fw_knob("epoch", 1);
        rc = tsdbc_insert_series(cn, id, &tn, &vn, 1);
        fw_knob("epoch", 0);
        tsdbc_close(cn);
        if (rc != TSDB_ERR_MOVED && rc != TSDB_ERR_UNAVAILABLE)
            die("a forward stamped with another epoch than its owner's: %s, not MOVED or UNAVAILABLE", ers(rc));
        if (fw_has(L, id, tn) != 0)
            die("a forward stamped with another epoch than its owner's was applied");
        printf("  a forward stamped with another epoch than its owner leads at is %s, never run\n", ers(rc));
    }

    /* m. a client that leaves while its forward waits (the owner holds it): the entry gives the forward up (cancel) in
     *    about a second, not after fwd_reply_ms (30 s) or whenever the owner answers */
    {
        tsdbc *cm = login_retry(R, "root", "root-pw", 0), *sm = login_retry(R, "root", "root-pw", 0);
        tsdbc_set_timeout(cm, 300);
        int64_t tm = 98000500;
        float vm = 1;
        fw_knob("ownerstall", 1);
        rc = tsdbc_insert_series(cm, id, &tm, &vm, 1);
        uint64_t inflight = fw_stat(sm, "fwd.inflight");
        if (inflight != 1) {
            fw_knob("ownerstall", 0);
            die("a forward held at the owner: %" PRIu64 " forwards in flight on the entry, not 1", inflight);
        }
        tsdbc_close(cm);
        uint64_t t0 = now_ms(), gone_ms = 0;
        while ((inflight = fw_stat(sm, "fwd.inflight")) != 0 && now_ms() - t0 < 3000)
            usleep(20000);
        gone_ms = now_ms() - t0;
        uint64_t left = inflight;
        fw_knob("ownerstall", 0);
        tsdbc_close(sm);
        if (rc != TSDB_ERR_IO)
            die("a forward held at the owner past the client's 300 ms timeout: %s, not IO", ers(rc));
        if (left != 0)
            die("a forward whose client left is still in flight on the entry %" PRIu64 " ms later (owner holding it)",
                gone_ms);
        printf("  a forward whose client left is given up %" PRIu64 " ms after the client closed\n", gone_ms);
    }

    /* e. forwards waiting on a stopped owner hold no worker of R: a volume led on R is served at once */
    char solo[16] = "";
    for (int k = 1; k <= 8 && !*solo; k++) {
        char nm[16];
        uint32_t nn;
        struct meta_volume s;
        snprintf(nm, sizeof nm, "solo%d", k);
        if ((rc = vol_add(0, nm, 1, &nn)) != 0)
            die("volume add %s: %s", nm, cluster_strerror(rc));
        if (!vol_rec(0, nm, &s))
            die("no record of %s", nm);
        await_open_gen(0, nm, server_of(s.leader), 1, 0);
        if (server_of(s.leader) == R)
            snprintf(solo, sizeof solo, "%s", nm);
    }
    if (!*solo)
        die("no volume of 8 with one replica is led on server %d", R);
    c = login_retry(R, "root", "root-pw", 0);
    tsdb_sid lid = rep_series(c, "plant.l", solo, 30000);
    struct fw_write hw[2];
    pthread_t ht[2];
    for (int k = 0; k < 2; k++)
        hw[k] = (struct fw_write){.c = login_retry(R, "root", "root-pw", 0), .id = id, .ts = 97000000 + k};
    pause_server(L, 1);
    for (int k = 0; k < 2; k++)
        if (pthread_create(&ht[k], NULL, fw_write_run, &hw[k]) != 0)
            die("pthread_create");
    usleep(200000);
    uint64_t t0 = now_ms();
    int64_t tl = 1000;
    float vl = 1;
    rc = tsdbc_insert_series(c, lid, &tl, &vl, 1);
    uint64_t took = now_ms() - t0;
    usleep(took < 1000 ? (useconds_t)(1000 - took) * 1000 : 0);
    pause_server(L, 0);
    for (int k = 0; k < 2; k++) {
        pthread_join(ht[k], NULL);
        tsdbc_close(hw[k].c);
    }
    tsdbc_close(c);
    if (rc != 0)
        die("a write to %s led on server %d while two forwards wait on the stopped leader of east: %s", solo, R,
            ers(rc));
    if (took > 600)
        die("a write to %s led on server %d took %" PRIu64 " ms while two forwards waited on a stopped owner (--workers 2)",
            solo, R, took);
    printf("  two forwards waiting on a stopped owner leave a volume led on the entry served (%" PRIu64 " ms, --workers 2); "
           "they then answered %s and %s\n",
           took, hw[0].rc ? ers(hw[0].rc) : "OK", hw[1].rc ? ers(hw[1].rc) : "OK");

    /* d. a forward written to an owner that dies before it answers: the client's connection is closed (the outcome is
     * unknown), never UNAVAILABLE (which says nothing was applied) */
    await_isr(0, "east", 3, 20000, "east with every replica up again");
    if (!vol_rec(0, "east", &v))
        die("no record of east");
    L = server_of(v.leader), R = (L + 1) % 3;
    w = (struct fw_write){.c = login_retry(R, "root", "root-pw", 0), .id = id, .ts = 96000000};
    pause_server(L, 1);
    if (pthread_create(&th, NULL, fw_write_run, &w) != 0)
        die("pthread_create");
    usleep(300000);
    signal_child(pids[L], SIGKILL); /* never SIGCONT first: the owner must not answer */
    int st;
    if (waitpid(pids[L], &st, 0) != pids[L])
        die("waitpid %d: %s", (int)pids[L], strerror(errno));
    pids[L] = 0, stopped[L] = 0;
    pthread_join(th, NULL);
    tsdbc_close(w.c);
    if (w.rc != TSDB_ERR_IO)
        die("a forwarded write whose owner was killed before it answered: %s after %" PRIu64 " ms, not a closed "
            "connection (IO)",
            ers(w.rc), w.ms);
    printf("  a forwarded write whose owner died before it answered closes the client's connection (%" PRIu64
           " ms)\n",
           w.ms);
}

/* ccstress: the copy's get/put/swap in-process (cluster_copy_stress, no server): 32 readers loop get/put while 5,000
 * snapshots are swapped in. The writer must get through (one counter for every reader starved it for good: mutant
 * cconeslot), and no reader may see its snapshot change or freed (ccnowait; ASan names the use after free). */
static void scen_ccstress(void)
{
    scenario("ccstress");
    char why[256];
    int rc = cluster_copy_stress(32, 5000, 30000, why, sizeof why);
    if (rc != 0)
        die("the copy's get/put/swap under load: %s", why);
    printf("  the copy's get/put/swap under load: %s\n", why);
}

/* copyhold: a reader that holds a snapshot of the copy while 50 more are published keeps what it held (the
 * cluster thread frees a snapshot only at its last reference); mutant snapfreeheld */
static void scen_copyhold(void)
{
    scenario("copyhold");
    strict_reads = 1; /* a replica moves reads to the leader (D5), as in production */
    envs[0] = "copy_hold=50";
    start(0);
    init_cluster(0, 600);
    tsdbc *c = client(0);
    uint64_t t0 = now_ms();
    while (!log_has(0, "copy hold: intact") && !log_has(0, "copy hold: FAILED")) {
        tsdbc_topo *t = NULL; /* readers beside it */
        int rc = tsdbc_topology(c, &t);
        if (rc == 0)
            tsdbc_topology_free(t);
        else if (rc != TSDB_ERR_UNAVAILABLE)
            die("TOPOLOGY beside the hold: %s", ers(rc));
        if (await_exit(0, 0) >= 0)
            die("the server exited while a reader held a snapshot");
        if (now_ms() - t0 > 60000)
            die("no verdict of the hold check in 60 s");
        usleep(20000);
    }
    tsdbc_close(c);
    if (log_has(0, "copy hold: FAILED") || log_has(0, "copy: a snapshot"))
        die("the held snapshot changed or a reference went wrong (see %s/n0.log)", base);
    stop_all();
    printf("  a snapshot held across 50 published ones is unchanged\n");
}

/* minisr: with min_isr 3, a leader whose ISR shrank to 2 refuses a write before anything is written (REPL_E_ISR:
 * UNAVAILABLE, WHY UNREACHABLE), serves reads, and takes writes again once the replica is back in the ISR (mutant
 * nominisr). A write admitted while the ISR was 3 whose commit point comes only after the shrink is not acknowledged
 * until the ISR is back (mutant ackbelowminisr). The second kill happens while nothing is written: a silent member
 * goes after isr_lag (repl.c MUT(40)). With three nodes min_isr 3 stands for "two of three replicas down": two down
 * would take the metadata group's quorum, and with it every ISR change. */
static void scen_minisr(void)
{
    scenario("minisr");
    init_min_isr = 3;
    for (int i = 0; i < 3; i++) {
        envs[i] = "hold_isr_file=1";
        start(i);
    }
    init_cluster(0, 600);
    init_min_isr = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v = {0};
    if (!vol_rec(0, "plant", &v) || v.min_isr != 3)
        die("plant has no record or min_isr %" PRIu32 ", not 3", v.min_isr);
    int L = server_of(v.leader);
    tsdbc *c = client(L);
    tsdbc_set_retry(c, 0);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    ts = 2000;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a second write with every replica up: %s", ers(rc));
    await_isr(0, "plant", 3, 5000, "every replica up");
    int R1 = (L + 1) % 3;
    /* a write admitted with the ISR at 3, its commit point reached only once the shrink to 2 committed: held */
    hold_isr(1);
    stop(R1, SIGKILL);
    struct rep_writer w = {cports[L], id, 2500, 1, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    struct timespec s2 = {2, 0};
    nanosleep(&s2, NULL);
    hold_isr(0);
    await_isr(0, "plant", 2, 10000, "a replica killed");
    uint64_t tw = now_ms();
    while (!log_has(L, "below min_isr 3: writes are refused")) {
        if (now_ms() - tw > 5000)
            die("the leader never said that its ISR is below min_isr");
        nanosleep(&(struct timespec){0, 20000000}, NULL);
    }
    nanosleep(&s2, NULL);
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE))
        die("a write committed only after the ISR shrank below min_isr was answered (rc %d)", w.rc);
    for (int j = 0; j < 5; j++) {
        uint64_t t0 = now_ms();
        ts = 3000 + 1000LL * j;
        rc = tsdbc_insert_series(c, id, &ts, &val, 1);
        if (rc != TSDB_ERR_UNAVAILABLE)
            die("a write below min_isr was answered %d (%s), not UNAVAILABLE", rc, ers(rc));
        if (now_ms() - t0 > 1000)
            die("a write below min_isr took %" PRIu64 " ms to be refused", now_ms() - t0);
        tsdb_unavail u[4];
        unsigned nu = 0;
        if (tsdbc_unavailable(c, u, 4, &nu) != 0 || nu < 1 || u[0].reason == TSDB_WHY_NONE)
            die("a write below min_isr was refused without a reason (%u entries)", nu);
        if (u[0].reason != TSDB_WHY_UNREACHABLE)
            die("a write below min_isr was refused with reason %u, not UNREACHABLE", u[0].reason);
    }
    if (rows_of(c, id) != 2)
        die("the leader reads %d rows after the refused writes, not 2", rows_of(c, id));
    start(R1);
    await_isr(0, "plant", 3, 30000, "the replica back");
    await_writer(&w, th, 10000, "the write held below min_isr, once the replica is back");
    ts = 9000;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write once the replicas are back: %s", ers(rc));
    if (rows_of(c, id) != 4)
        die("the leader reads %d rows, not 4 (a refused write was written)", rows_of(c, id));
    tsdbc_close(c);
    stop_all();
    for (int i = 0; i < 3; i++)
        envs[i] = NULL;
    uint64_t wantr[REP_NAMES] = {4, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
    printf("  below min_isr a write is refused at once with a reason and nothing written, one committed only after the"
           " shrink waits for the ISR to grow; above it writes go on\n");
}

/* closegone (a leader's shutdown with its replicas gone): an orphaned tsdbd once logged "shutting down" as the leader of
 * a volume whose replicas had stopped and then sat for hours in futex waits. A close must not wait for a commit point
 * that cannot move: tsdbd deposes the leader (repld_stop, eng_close) and the rows above the commit point stay in the
 * WAL for the next epoch. SIGTERM ends the leader in bounded time with nothing above its commit point, and with a write
 * waiting there (its replicas stopped before they acknowledged it, then killed; two of three voters gone, so no ISR
 * change can commit). --exit-with-parent stops a server the same way (it sets `stopping` as SIGTERM does). The write
 * is answered with an error; every directory opens afterwards, the replicas without the row. */
static int term_within(int i, int ms, const char *what)
{
    uint64_t t0 = now_ms();
    signal_child(pids[i], SIGTERM);
    int st = await_exit(i, ms);
    if (st < 0)
        die("%s: server %d still runs %d ms after SIGTERM (its close waits for a commit point that cannot move)", what,
            i, ms);
    if (st != 0)
        die("%s: server %d exited with status %d on SIGTERM, not 0", what, i, st);
    stopped[i] = 0;
    return (int)(now_ms() - t0);
}

/* a cluster of three in its own directory with volume plant on all of them, two rows written and the ISR full; the
 * leader's index. (A scenario never restarts all three: every restart takes new ports, and no server would know where
 * another one is.) */
static int closegone_setup(const char *name, const char *env, tsdb_sid *id)
{
    scenario(name);
    for (int i = 0; i < 3; i++) {
        penv[i] = env;
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdbc_set_retry(c, 0);
    tsdbc_set_timeout(c, 20000); /* a commit point that never moves (writersync's mutant) fails, never hangs, the test */
    *id = rep_series(c, rep_names[0], "plant", 30000);
    float val = 1;
    for (int64_t ts = 1000; ts <= 2000; ts += 1000)
        if ((rc = tsdbc_insert_series(c, *id, &ts, &val, 1)) != 0)
            die("a write with every replica up: %s", ers(rc));
    await_isr(0, "plant", 3, 5000, "every replica up");
    tsdbc_close(c);
    return L;
}

/* What a volume directory's WAL is: its segments' names and sizes (order-free), and their bytes. A stopped server writes
 * nothing, so these do not change between its pause and its SIGKILL. */
static uint64_t wal_print(const char *vd, uint64_t *bytes)
{
    uint64_t h = 0;
    *bytes = 0;
    DIR *d = opendir(vd);
    if (!d)
        die("opendir %s: %s", vd, strerror(errno));
    struct dirent *e;
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, "wal_", 4) != 0)
            continue;
        char p[PATH_MAX + 400];
        struct stat st;
        snprintf(p, sizeof p, "%s/%s", vd, e->d_name);
        if (stat(p, &st) != 0)
            die("stat %s: %s", p, strerror(errno));
        uint64_t f = 1469598103934665603ULL;
        for (const char *c = e->d_name; *c; c++)
            f = (f ^ (unsigned char)*c) * 1099511628211ULL;
        h += (f ^ (uint64_t)st.st_size) * 0x9e3779b97f4a7c15ULL;
        *bytes += (uint64_t)st.st_size;
    }
    closedir(d);
    return h;
}

static void scen_closegone(void)
{
    tsdb_sid id;
    /* 1: both replicas stopped cleanly, nothing above the leader's commit point */
    int L = closegone_setup("closegone", NULL, &id);
    stop((L + 1) % 3, SIGTERM);
    stop((L + 2) % 3, SIGTERM);
    int ms1 = term_within(L, 15000, "the leader with its replicas stopped, nothing above its commit point");

    /* 2: a write waits above the commit point: the replicas are stopped (SIGSTOP) before they acknowledge it */
    L = closegone_setup("closegone-above", NULL, &id);
    pause_server((L + 1) % 3, 1);
    pause_server((L + 2) % 3, 1);
    /* the replicas' WAL once both pauses returned: should a replica still log the write, that names the cause (the
     * pause was no stop yet), not "holds 3 rows" below */
    uint64_t wh[3] = {0}, wb[3] = {0};
    for (int r = 0; r < 3; r++)
        if (r != L) {
            char dir[PATH_MAX + 80], vd[PATH_MAX + 120];
            server_dir(r, dir, sizeof dir);
            snprintf(vd, sizeof vd, "%s/v1", dir);
            wh[r] = wal_print(vd, &wb[r]);
        }
    struct rep_writer w = {cports[L], id, 3000, 1, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    nanosleep(&(struct timespec){1, 500000000}, NULL);
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE))
        die("a write with both replicas stopped was answered (%s): nothing waits above the commit point", ers(w.rc));
    for (int r = 0; r < 3; r++) {
        if (r == L)
            continue;
        signal_child(pids[r], SIGKILL); /* a stopped process dies of SIGKILL without running again */
        int st;
        if (waitpid(pids[r], &st, 0) != pids[r])
            die("waitpid %d: %s", (int)pids[r], strerror(errno));
        pids[r] = 0;
        stopped[r] = 0;
        char dir[PATH_MAX + 80], vd[PATH_MAX + 120];
        server_dir(r, dir, sizeof dir);
        snprintf(vd, sizeof vd, "%s/v1", dir);
        uint64_t b, hh = wal_print(vd, &b);
        if (hh != wh[r])
            die("server %d (a replica) wrote its WAL after its pause returned (%" PRIu64 " -> %" PRIu64 " bytes)", r,
                wb[r], b);
    }
    int ms2 = term_within(L, 15000, "the leader with a write above its commit point and its replicas killed");
    uint64_t tw = now_ms();
    while (!__atomic_load_n(&w.done, __ATOMIC_ACQUIRE)) {
        if (now_ms() - tw > 5000)
            die("the write above the commit point got no answer after its server stopped");
        nanosleep(&(struct timespec){0, 10000000}, NULL);
    }
    pthread_join(th, NULL);
    if (w.rc == 0)
        die("a write no replica acknowledged was answered as written");
    /* every directory opens; the replicas never saw the row, the leader's WAL may keep it for the next epoch */
    uint64_t lrows = 0;
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80], vd[PATH_MAX + 120];
        server_dir(i, dir, sizeof dir);
        snprintf(vd, sizeof vd, "%s/v1", dir);
        uint64_t rows[REP_NAMES], h[REP_NAMES];
        int rc = cvol_digest(vd, rep_names, REP_NAMES, rows, h);
        if (rc != 0)
            die("open %s read-only after the stop: %s", vd, ers(rc));
        if (i == L ? rows[0] < 2 || rows[0] > 3 : rows[0] != 2)
            die("server %d (%s) holds %" PRIu64 " rows after the stop", i, i == L ? "the leader" : "a replica",
                rows[0]);
        if (i == L)
            lrows = rows[0];
    }

    /* 3: the engine writes after the driver stopped (a record admitted before the depose, a NOW marker while the
     * read lease lasts): latewal keeps the leader in place 2.5 s past the freeing of the links and lets the stopped
     * driver admit records whatever its lease, so the flusher's NOW markers (one a second from the moment the volume
     * is led, mutant lateclock: only from its first SAVE as a writer, after this test's SIGTERM) come through the WAL
     * hooks. They used to send on the freed links (a use after free: status 139 or an ASan report). The leader's log
     * says that the hooks ran after the stop, or this case proved nothing. */
    L = closegone_setup("closegone-late", "TSDB_REPLD_TEST=latewal", &id);
    int ms3 = term_within(L, 15000, "the leader writing NOW markers after its driver stopped");
    if (!log_has(L, "after its driver stopped"))
        die("closegone-late: the leader wrote no WAL record after its driver stopped (no NOW marker in 2.5 s: is its "
            "clock started when it begins to lead?)");
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    printf("  a leader whose replicas are gone stops on SIGTERM in %d ms (nothing above its commit point) and %d ms (a"
           " write waiting there, answered %s; its directory reads %" PRIu64 " rows); its WAL written after the driver"
           " stopped, in %d ms\n",
           ms1, ms2, ers(w.rc), lrows, ms3);
}

/* writersync: the writer's SYNC actions against its engine's own reports of a durable WAL. With concurrent writes the
 * engine's group sync covers more than the SYNC in flight was issued for; the core caps what it is told at that SYNC's
 * end and issues another, which the driver once answered only when the WAL had moved past its last report: it had
 * not, so the core's durable end, and the commit point, stayed behind a durable WAL until a later record (backpressure
 * hung there after its last write, its flusher waiting; mutant syncbyrep). Batches of eight connections writing single
 * rows: at the end of a batch, or once every writer waits, nothing moves the WAL, since noclock turns the NOW markers
 * and the idle seal off. The leader's log says the overtaking happened, or this case proved nothing. syncbyhook makes
 * the driver answer a SYNC from what op_synced reported, never from a fresh read of the engine (which used to rescue
 * the old code whenever the engine had synced again before op_synced ran), and synclate holds every other report of
 * op_synced back to the driver's next tick, so that the report after it covers records written behind the SYNC in
 * flight: the overtaking happens in the first batch, and under syncbyrep the core's durable end lags one report behind
 * for good, which leaves the last write of a batch unanswered. */
static void scen_writersync(void)
{
    tsdb_sid id;
    int L = closegone_setup("writersync", "TSDB_REPLD_TEST=noclock,syncbyhook,synclate", &id);
    enum { CONNS = 8, BATCHES = 10, MAX_BATCHES = 100, WRITES = 20 };
    struct rep_writer w[CONNS];
    pthread_t th[CONNS];
    rep_timeout_ms = 5000;
    uint64_t t0 = now_ms();
    int b, seen = 0, seen_at = -1;
    /* BATCHES at least, on until the overtaking was seen (synclate makes it happen in the first), and two batches past it */
    for (b = 0; b < MAX_BATCHES && (b < BATCHES || !seen || b <= seen_at + 2); b++) {
        for (int k = 0; k < CONNS; k++) {
            w[k] = (struct rep_writer){cports[L], id, 100000000LL * (b * CONNS + k + 1), WRITES, 0, 0, 0, 0};
            if (pthread_create(&th[k], NULL, rep_write_thread, &w[k]) != 0)
                die("pthread_create");
        }
        for (int k = 0; k < CONNS; k++)
            pthread_join(th[k], NULL);
        for (int k = 0; k < CONNS; k++)
            if (w[k].rc != 0)
                die("writersync: batch %d, a write from connection %d: %s (its commit point stayed below the "
                    "leader's durable WAL?)", b, k, ers(w[k].rc));
        if (!seen && (seen = log_has(L, "an engine sync went past the SYNC in flight")))
            seen_at = b;
    }
    uint64_t ms = now_ms() - t0;
    rep_timeout_ms = 0;
    if (!seen)
        die("writersync: no engine sync went past a SYNC in flight in %d batches (the case proved nothing)", b);
    tsdbc *c = client(L);
    int n = rows_of(c, id);
    tsdbc_close(c);
    if (n != 2 + CONNS * b * WRITES)
        die("writersync: the leader reads %d rows, not %d", n, 2 + CONNS * b * WRITES);
    stop_all();
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    printf("  %d batches of %d connections x %d single-row writes in %" PRIu64 " ms, the engine's syncs overtaking the"
           " SYNC in flight first in batch %d\n", b, CONNS, WRITES, ms, seen_at);
}

/* the WAL segments in server i's directory of volume num */
static int wal_count(int i, uint32_t num)
{
    char dir[PATH_MAX + 80], v[PATH_MAX + 120];
    server_dir(i, dir, sizeof dir);
    snprintf(v, sizeof v, "%s/v%" PRIu32, dir, num);
    DIR *d = opendir(v);
    if (!d)
        die("opendir %s: %s", v, strerror(errno));
    int n = 0;
    errno = 0;
    for (struct dirent *e; (e = readdir(d));)
        n += !strncmp(e->d_name, "wal_", 4);
    if (errno)
        die("readdir %s: %s", v, strerror(errno));
    closedir(d);
    return n;
}

/* A writer of 1000-row inserts on one connection until `stop` (inboxshared's stalled volume) */
struct flood {
    int port;
    tsdb_sid id;
    int stop, rc, rounds;
};
static void *flood_thread(void *a)
{
    struct flood *f = a;
    while (!__atomic_load_n(&f->stop, __ATOMIC_ACQUIRE)) {
        struct rep_writer w = {f->port, f->id, 1000000000LL * (__atomic_load_n(&f->rounds, __ATOMIC_RELAXED) + 1), 0, 0,
                               0, 1000, 0};
        rep_write_thread(&w);
        if (w.rc != 0) {
            __atomic_store_n(&f->rc, w.rc, __ATOMIC_RELEASE);
            break;
        }
        __atomic_add_fetch(&f->rounds, 1, __ATOMIC_RELEASE); /* rejoin reads it while this runs */
#if defined(__SANITIZE_THREAD__)
        /* Under TSan the catch-up of a restarted replica is slowed several times more than a client's write into a
         * tmpfs: two writers of 1000-row inserts outran it, and rejoinsteady's learner never got close enough to join
         * in 30 s, with the old tick rule as with the new (2026-11-18, /tmp a tmpfs: 2 of 3 and 1 of 3 runs). The
         * writes still never pause; they come at most 100 a second per writer. */
        nanosleep(&(struct timespec){0, 10000000}, NULL);
#endif
    }
    return NULL;
}

/* inboxshared (r3d-isr review H1): one data link carries every volume between two nodes, and its frames are handed
 * to the volumes' inboxes on the link's one thread. A volume whose inbox is full and stays so (its disk stuck: here
 * TSDB_REPLD_TEST=stallinbox with <dir>/stallinbox.<number>) must not hold that thread: the other volume, led by the
 * same node, keeps the stalled node in its ISR and its single-row writes answer as fast as before. The thread used to
 * wait 500 ms for every frame of the full volume (mutant inboxwaitall), and its leader's resends kept it asleep. */
static void scen_inboxshared(void)
{
    scenario("inboxshared");
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=smallinbox,stallinbox";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    /* two volumes with the same leader: of four, two share one of the three nodes */
    static const char *const names[] = {"va", "vb", "vc", "vd"};
    uint32_t num[4];
    int lead[4], A = -1, B = -1;
    for (int k = 0; k < 4 && B < 0; k++) {
        int rc = vol_add(0, names[k], 3, &num[k]);
        if (rc != 0)
            die("volume add %s: %s", names[k], cluster_strerror(rc));
        for (int i = 0; i < 3; i++)
            await_open_gen(0, names[k], i, 1, 0);
        struct meta_volume v;
        if (!vol_rec(0, names[k], &v))
            die("no record of %s", names[k]);
        lead[k] = server_of(v.leader);
        for (int j = 0; j < k && B < 0; j++)
            if (lead[j] == lead[k])
                A = j, B = k;
    }
    if (B < 0)
        die("inboxshared: no two of four volumes have one leader");
    int L = lead[A], R = (L + 1) % 3;
    if (L < 0 || L > 2)
        die("the leader of %s is not one of the nodes", names[A]);
    tsdbc *c = client(L);
    tsdb_sid sa = rep_series(c, "stalled", names[A], 30000), sb = rep_series(c, "healthy", names[B], 30000);
    await_isr(L, names[A], 3, 15000, "every replica of the volume to stall up");
    await_isr(L, names[B], 3, 15000, "every replica of the healthy volume up");
    char stall[PATH_MAX + 96];
    server_dir(R, stall, sizeof stall);
    snprintf(stall + strlen(stall), sizeof stall - strlen(stall), "/stallinbox.%" PRIu32, num[A]);
    FILE *sf = fopen(stall, "w");
    if (!sf)
        die("create %s", stall);
    fclose(sf);
    struct flood f[2] = {{cports[L], sa, 0, 0, 0}, {cports[L], sa, 0, 0, 0}};
    pthread_t th[2];
    for (int k = 0; k < 2; k++) {
        f[k].rounds = k * 1000; /* other timestamps */
        if (pthread_create(&th[k], NULL, flood_thread, &f[k]) != 0)
            die("pthread_create");
    }
    /* 8 s of single-row writes to the healthy volume, its ISR checked between them */
    uint64_t t0 = now_ms(), worst = 0, last_isr = 0;
    int writes = 0, full_at = -1;
    while (now_ms() - t0 < 8000) {
        int64_t ts = 1000LL * (writes + 1);
        float val = (float)writes;
        uint64_t w0 = now_ms();
        int rc = tsdbc_insert_series(c, sb, &ts, &val, 1);
        uint64_t ms = now_ms() - w0;
        if (rc != 0)
            die("inboxshared: a write to the healthy volume: %s", ers(rc));
        if (ms > worst)
            worst = ms;
        writes++;
        if (full_at < 0 && log_has(R, "frames are dropped"))
            full_at = writes;
        if (now_ms() - last_isr >= 100) {
            last_isr = now_ms();
            uint32_t n = isr_of(L, names[B], NULL);
            if (n != 3)
                die("inboxshared: the healthy volume %s has %" PRIu32 " ISR members while %s's inbox on n%d is full"
                    " (the shared link's thread waited for it)", names[B], n, names[A], R);
        }
    }
    for (int k = 0; k < 2; k++)
        __atomic_store_n(&f[k].stop, 1, __ATOMIC_RELEASE);
    for (int k = 0; k < 2; k++) {
        pthread_join(th[k], NULL);
        if (f[k].rc != 0)
            die("inboxshared: a write to the stalled volume: %s", ers(f[k].rc));
    }
    if (full_at < 0)
        die("inboxshared: the stalled volume's inbox never filled (the case proved nothing)");
    if (worst > 1000)
        die("inboxshared: a single-row write to the healthy volume took %" PRIu64 " ms while %s's inbox was full",
            worst, names[A]);
    unlink(stall);
    await_isr(L, names[A], 3, 60000, "the stalled volume's replica catching up");
    if (rows_of(c, sb) != writes)
        die("inboxshared: the leader reads %d rows of the healthy volume, not %d", rows_of(c, sb), writes);
    tsdbc_close(c);
    stop_all();
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    printf("  %s stalled on n%d (inbox full from write %d): %d writes to %s, the slowest %" PRIu64 " ms, its ISR "
           "stayed 3\n", names[A], R, full_at, writes, names[B], worst);
}

/* backpressure into a replica (r3d review M3): a replica that stops handling its peer frames (TSDB_REPLD_TEST=stallinbox
 * while <dir>/stallinbox exists: a stalled disk's stand-in) leaves the ISR, and with a small inbox bound (smallinbox)
 * drops the frames past the bound instead of holding the leader's stream in memory; unstalled, it catches up from the
 * resends and grows back; a part above the spill threshold (spill: 64
 * KiB) is received into a stage_s* file, not memory, and every replica ends with the leader's rows */
static void scen_backpressure(void)
{
    scenario("backpressure");
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=spill";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    int R = (L + 1) % 3, S2 = (L + 2) % 3;
    stop(R, SIGTERM);
    penv[R] = "TSDB_REPLD_TEST=spill,smallinbox,stallinbox";
    start(R);
    tsdbc *c = client(L);
    tsdb_sid id[REP_NAMES];
    for (int k = 0; k < REP_NAMES; k++)
        id[k] = rep_series(c, rep_names[k], "plant", 30000);
    await_isr(L, "plant", 3, 15000, "every replica up");
    uint64_t n[REP_NAMES] = {0};
    /* R stalls: it leaves the ISR and the writes go on at full speed, streamed to R, whose inbox fills */
    char stall[PATH_MAX + 80];
    server_dir(R, stall, sizeof stall);
    strcat(stall, "/stallinbox");
    FILE *sf = fopen(stall, "w");
    if (!sf)
        die("create %s", stall);
    fclose(sf);
    /* 1000-row writes from four connections for 4 s */
    struct rep_writer w[4];
    pthread_t th[4];
    uint64_t t0 = now_ms();
    int rounds = 0;
    while (now_ms() - t0 < 4000) {
        for (int k = 0; k < 4; k++) {
            w[k] = (struct rep_writer){cports[L], id[0], 4000000LL * rounds + 1000000LL * k, 0, 0, 0,
                                       1000, 0};
            if (pthread_create(&th[k], NULL, rep_write_thread, &w[k]) != 0)
                die("pthread_create");
        }
        for (int k = 0; k < 4; k++) {
            pthread_join(th[k], NULL);
            if (w[k].rc != 0)
                die("a write with a stalled replica: %s", ers(w[k].rc));
        }
        n[0] += 4000;
        rounds++;
    }
    /* and a large write: its part is shipped as a file above the spill threshold */
    struct rep_writer wb = {cports[L], id[2], 2000000000, 0, 0, 0, 100000, 0};
    rep_write_thread(&wb);
    if (wb.rc != 0)
        die("a large write with a stalled replica: %s", ers(wb.rc));
    n[2] += 100000;
    if (!log_has(R, "frames are dropped"))
        die("the stalled replica held the leader's whole stream in its inbox (no frame dropped at the bound)");
    unlink(stall);
    if (!log_has(S2, "not into memory"))
        die("a replica received a part above the spill threshold into memory");
    await_isr(L, "plant", 3, 60000, "the stalled replica catching up");
    if ((uint64_t)rows_of(c, id[0]) != n[0])
        die("the leader reads %d rows of %s, not %" PRIu64, rows_of(c, id[0]), rep_names[0], n[0]);
    tsdbc_close(c);
    stop_all();
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    rep_same(n, "after a clean stop");
    printf("  a stalled replica drops frames past its inbox bound and catches up (%d rounds of writes), a large part is"
           " received into a file; every replica holds the leader's rows\n", rounds);
}

/* stallstop (2026-11-18): a replica whose volume thread is stalled (TSDB_REPLD_TEST=stallinbox, <dir>/stallinbox in
 * place) stops cleanly on SIGTERM within seconds. The stall used to end only when the file went, so a test that died
 * with the file in place left its replica in repld_stop's join for ever, under init and holding an io_uring ring:
 * every later io_uring jepsen-cluster run on the machine failed with "io_uring: Cannot allocate memory". */
static void scen_stallstop(void)
{
    scenario("stallstop");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    int R = (L + 1) % 3;
    stop(R, SIGTERM);
    penv[R] = "TSDB_REPLD_TEST=smallinbox,stallinbox";
    start(R);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    tsdbc_close(c);
    await_isr(L, "plant", 3, 15000, "every replica up");
    char stall[PATH_MAX + 80];
    server_dir(R, stall, sizeof stall);
    strcat(stall, "/stallinbox");
    FILE *sf = fopen(stall, "w");
    if (!sf)
        die("create %s", stall);
    fclose(sf);
    /* writes until R's inbox is past its bound: its volume thread is then asleep in the stall */
    uint64_t t0 = now_ms();
    for (int round = 0; !log_has(R, "frames are dropped"); round++) {
        if (now_ms() - t0 > 20000) {
            unlink(stall);
            die("the stalled replica's inbox never filled in 20 s (the case proved nothing)");
        }
        struct rep_writer w = {cports[L], id, 1000000LL * round + 1, 0, 0, 0, 1000, 0};
        rep_write_thread(&w);
        if (w.rc != 0) {
            unlink(stall);
            die("a write with a stalled replica: %s", ers(w.rc));
        }
    }
    signal_child(pids[R], SIGTERM);
    int st = await_exit(R, 10000);
    unlink(stall);
    if (st != 0) {
        if (st < 0) {
            signal_child(pids[R], SIGKILL);
            waitpid(pids[R], NULL, 0);
            pids[R] = 0;
        }
        die("a replica stalled in its inbox %s on SIGTERM", st < 0 ? "did not stop in 10 s" : "did not stop cleanly");
    }
    penv[R] = NULL;
    stop_all();
    printf("  a replica stalled in its inbox stops cleanly on SIGTERM\n");
}

/* rejoin (the merge's jepsen-cluster kill run, 2026-11-17): a replica killed and restarted below what the leader sent
 * it, while the writes never pause, joins the ISR again while they go on. The leader's fast path kept the peer's cursor at the live end, moving
 * sent_ms with every record, so the restarted replica refused every APPEND for the gap and the driver never moved the
 * cursor back to what it held (mutant fastride): in that run a replica's log stayed where it died for 30 minutes.
 *
 * rejoinsteady: the same with two writers (a write always in flight) and the restarted replica's ACKs reporting what
 * it had 100 ms before (TSDB_REPLD_TEST=slowack: a far replica, always a few writes behind the leader's end). A
 * learner joined only once its log reached the commit point, which under such writes it never does: the run of the
 * merge had n1 out of the ISR for 30 minutes while it received everything. The leader now holds the commit point
 * for a learner close behind until it has it (repl.c isr_check; mutant nojoinhold: the old rule).
 *
 * rejoinslow: the leader's catch-up reads take 10 ms each (TSDB_REPLD_TEST=slowread: longer than a tick). The driver
 * ran a due tick (with its catch-up) before the inbox, so with each tick longer than one the next was always due and
 * the healthy replica's ACKs waited in the inbox while the core's clock ran: the leader dropped it for silence (in
 * the merge's repro the ISR went 3 -> 2 -> 1 during a rejoin). After each tick the inbox now gets a tick's time
 * (mutant inboxstarve: none). The healthy replica must stay in the ISR throughout.
 *
 * rejoinlate: every tick of the leader takes 6 ms (TSDB_REPLD_TEST=latetick), so its volume thread is never current
 * (as under TSan with a full inbox). Late ticks run without a catch-up step (tickwall), but one still runs as long
 * after the last as that took: a version that ran steps only on current ticks never caught the replica up
 * (rejoinsteady under TSan; mutant stepcurrentonly). */
enum { REJ_GAP, REJ_STEADY, REJ_SLOW, REJ_LATE };
static void rejoin_run(const char *name, int how)
{
    scenario(name);
    for (int i = 0; i < 3; i++) {
        penv[i] = how == REJ_SLOW ? "TSDB_REPLD_TEST=slowread" : how == REJ_LATE ? "TSDB_REPLD_TEST=latetick" : NULL;
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    int nf = how == REJ_STEADY ? 2 : 1;
    tsdb_sid id[2];
    for (int k = 0; k < nf; k++)
        id[k] = rep_series(c, rep_names[k], "plant", 30000);
    await_isr(L, "plant", 3, 10000, "every replica up");
    struct flood f[2];
    pthread_t th[2];
    for (int k = 0; k < nf; k++) {
        f[k] = (struct flood){cports[L], id[k], 0, 0, 0};
        if (pthread_create(&th[k], NULL, flood_thread, &f[k]) != 0)
            die("pthread_create");
    }
    struct timespec s1 = {1, 0};
    nanosleep(&s1, NULL);
    /* stopped first (SIGSTOP: its link stays up and the leader's records go into it until its queue is full), then
     * killed while stopped: what was sent and never read is lost, so the replica comes back below the cursor */
    int R = (L + 1) % 3;
    pause_server(R, 1);
    uint64_t t0 = now_ms();
    uint32_t n;
    while ((n = isr_of(L, "plant", NULL)) == 3) {
        if (now_ms() - t0 > 10000)
            die("a replica stopped under writes: the ISR of plant has 3 members after 10000 ms");
        struct timespec ms = {0, 50000000};
        nanosleep(&ms, NULL);
    }
    if (how == REJ_SLOW && (n < 2 || log_has(L, "asks for an ISR of 1 members")))
        die("the leader dropped the healthy replica from the ISR while another was away (its ACKs waited behind the "
            "catch-up)");
    nanosleep(&s1, NULL);
    signal_child(pids[R], SIGKILL);
    if (waitpid(pids[R], NULL, 0) != pids[R])
        die("waitpid %d: %s", (int)pids[R], strerror(errno));
    pids[R] = 0;
    stopped[R] = 0;
    int r0 = 0;
    for (int k = 0; k < nf; k++)
        r0 += __atomic_load_n(&f[k].rounds, __ATOMIC_RELAXED);
    if (how == REJ_STEADY)
        penv[R] = "TSDB_REPLD_TEST=slowack";
    start(R);
    t0 = now_ms();
    int rounds = 0;
    while ((n = isr_of(L, "plant", NULL)) != 3) {
        rounds = 0;
        for (int k = 0; k < nf; k++) {
            if (__atomic_load_n(&f[k].rc, __ATOMIC_RELAXED))
                die("a write while the replica was away: %s", ers(f[k].rc));
            rounds += __atomic_load_n(&f[k].rounds, __ATOMIC_RELAXED);
        }
        if (how == REJ_SLOW && (n < 2 || log_has(L, "asks for an ISR of 1 members")))
            die("the leader dropped the healthy replica from the ISR while another caught up (its ACKs waited behind "
                "the catch-up)");
        if (now_ms() - t0 > 30000)
            die("the replica restarted under writes that never pause did not join the ISR again in 30 s (%d writes "
                "since its restart)", rounds - r0);
        struct timespec ms = {0, 50000000};
        nanosleep(&ms, NULL);
    }
    rounds = 0;
    for (int k = 0; k < nf; k++)
        rounds += __atomic_load_n(&f[k].rounds, __ATOMIC_RELAXED);
    int joined = rounds - r0;
    uint64_t took = now_ms() - t0;
    for (int k = 0; k < nf; k++) {
        __atomic_store_n(&f[k].stop, 1, __ATOMIC_RELEASE);
        pthread_join(th[k], NULL);
        if (f[k].rc != 0)
            die("a write while the replica was away: %s", ers(f[k].rc));
    }
    if (how == REJ_SLOW && log_has(L, "asks for an ISR of 1 members"))
        die("the leader dropped the healthy replica from the ISR while another caught up (its ACKs waited behind the "
            "catch-up)");
    if (joined < 5)
        die("only %d writes went on between the restart and the join: the writes paused, so this proved nothing", joined);
    tsdbc_close(c);
    stop_all();
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    uint64_t wantr[REP_NAMES] = {0};
    for (int k = 0; k < nf; k++)
        wantr[k] = (uint64_t)f[k].rounds * 1000;
    rep_same(wantr, "after a clean stop");
    printf("  a replica killed and restarted under writes that never paused joined the ISR again in %" PRIu64 " ms (%d "
           "writes in between)%s; every replica holds the leader's rows\n", took, joined,
           how == REJ_STEADY ? ", its ACKs 100 ms late" : how == REJ_SLOW ? ", the leader's reads slow, the healthy "
           "replica in the ISR throughout" : "");
}

static void scen_rejoin(void) { rejoin_run("rejoin", REJ_GAP); }
static void scen_rejoinsteady(void) { rejoin_run("rejoinsteady", REJ_STEADY); }
static void scen_rejoinslow(void) { rejoin_run("rejoinslow", REJ_SLOW); }
static void scen_rejoinlate(void) { rejoin_run("rejoinlate", REJ_LATE); }

/* tickwall (r3d-isr review M1): isr_lag is wall time. The core counts it in ticks, and every catch-up step of the
 * leader ran inside a tick, a late tick dropped: with a step of 20 ms (TSDB_REPLD_TEST=slowtick, every tick) a core
 * tick took 25 ms, and a stopped member held the writes for 5 s instead of isr_lag (1 s). Now a late tick is the
 * clock catching up and runs without a step (mutant tickstretch: the old rule). A member stopped under writes must
 * leave the ISR within 3.5 s (isr_lag, the ask and the metadata group's round, with room for a slow machine). */
static void scen_tickwall(void)
{
    scenario("tickwall");
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=slowtick";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    await_isr(L, "plant", 3, 10000, "every replica up");
    struct flood f = {cports[L], id, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, flood_thread, &f) != 0)
        die("pthread_create");
    struct timespec s1 = {1, 0};
    nanosleep(&s1, NULL);
    int R = (L + 1) % 3;
    pause_server(R, 1);
    uint64_t t0 = now_ms(), took;
    while (isr_of(L, "plant", NULL) == 3) {
        if (now_ms() - t0 > 20000)
            die("a member stopped under writes: the ISR of plant has 3 members after 20000 ms");
        struct timespec ms = {0, 10000000};
        nanosleep(&ms, NULL);
    }
    took = now_ms() - t0;
    pause_server(R, 0);
    __atomic_store_n(&f.stop, 1, __ATOMIC_RELEASE);
    pthread_join(th, NULL);
    if (f.rc != 0)
        die("a write while a member was stopped: %s", ers(f.rc));
    if (took > 3500)
        die("a member stopped under writes left the ISR only after %" PRIu64 " ms (isr_lag 1000 ms): the core's ticks "
            "ran slower than the wall clock", took);
    await_isr(L, "plant", 3, 60000, "the stopped member back");
    tsdbc_close(c);
    stop_all();
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    uint64_t wantr[REP_NAMES] = {(uint64_t)f.rounds * 1000, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
    printf("  with a catch-up step of 20 ms at every tick a member stopped under writes left the ISR after %" PRIu64
           " ms\n", took);
}

/* The keep caps (design-cluster.md 8.3): a replica out of the ISR holds the leader's flushed segments only for
 * repl_retain_s (2 s here); past it they go, and the replica, back, is told it needs a snapshot and stays a learner. */
static void scen_keepcap(void)
{
    scenario("keepcap");
    init_retain_s = 2;
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    init_retain_s = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    await_isr(0, "plant", 3, 5000, "every replica up");
    int R = (L + 1) % 3;
    stop(R, SIGKILL);
    await_isr(0, "plant", 2, 10000, "a replica killed");
    /* a segment a second, each flushed: with the replica away the keep bound stays where it was */
    struct timespec s1 = {1, 0};
    int64_t t = 10000;
    for (int k = 0; k < 7; k++) {
        int64_t tv[100];
        float vv[100];
        for (int j = 0; j < 100; j++)
            tv[j] = t++, vv[j] = (float)j;
        if ((rc = tsdbc_insert_series(c, id, tv, vv, 100)) != 0)
            die("a write with a replica away: %s", ers(rc));
        if ((rc = tsdbc_flush(c)) != 0)
            die("flush: %s", ers(rc));
        nanosleep(&s1, NULL);
    }
    if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0) /* a commit after the last flush: the log start moves */
        die("a write with a replica away: %s", ers(rc));
    int nw = wal_count(L, 1);
    if (nw > 4)
        die("the leader keeps %d WAL segments for a replica away longer than repl_retain_s (7 flushed, 1 s apart)",
            nw);
    start(R);
    uint64_t t0 = now_ms();
    while (!log_has(R, "it needs a snapshot")) {
        if (now_ms() - t0 > 15000)
            die("the replica back past the keep cap never heard it needs a snapshot");
        struct timespec ms100 = {0, 100000000};
        nanosleep(&ms100, NULL);
    }
    struct timespec s3 = {3, 0};
    nanosleep(&s3, NULL);
    if (isr_of(0, "plant", NULL) != 2)
        die("a replica that needs a snapshot joined the ISR");
    t++;
    if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0)
        die("a write with a replica that needs a snapshot: %s", ers(rc));
    tsdbc_close(c);
    stop_all();
    printf("  past repl_retain_s the leader keeps %d WAL segments, the replica back is told it needs a snapshot and"
           " stays out of the ISR\n", nw);
}

/* ---- snapshot transfer (r4-xferlink; server/rsnap.c, R4 plan D2, D5, D7, D18) */

struct snapset {
    int L, R, O; /* the leader of plant, the replica that needs a snapshot, the other member */
    int lead[2];  /* the leader of each volume */
    int nvol;
    tsdbc *c[2];
    tsdb_sid id[2];
    int64_t t;
};

/* the snap.* lines of server i's STATUS (server/rsnap.c's counters) */
struct rsnap_stats {
    uint64_t asked, sent, received, refused_space, failed, busy, bytes_sent, bytes_received;
};
static void status_snap(int i, struct rsnap_stats *st)
{
    tsdbc *c = client(i);
    char *text = NULL;
    int rc = tsdbc_status(c, &text);
    if (rc != 0)
        die("STATUS of server %d: %s", i, ers(rc));
    static const char *keys[] = {"asked", "sent", "received", "refused_space", "failed", "busy", "bytes_sent",
                                 "bytes_received"};
    uint64_t *v[] = {&st->asked, &st->sent, &st->received, &st->refused_space, &st->failed, &st->busy,
                     &st->bytes_sent, &st->bytes_received};
    for (size_t k = 0; k < sizeof keys / sizeof keys[0]; k++) {
        char key[64];
        snprintf(key, sizeof key, "\nsnap.%s=", keys[k]);
        const char *at = strstr(text, key);
        if (!at)
            die("STATUS of server %d has no snap.%s line:\n%s", i, keys[k], text);
        *v[k] = strtoull(at + strlen(key), NULL, 10);
    }
    free(text);
    tsdbc_close(c);
}

static const char *snap_vols[2] = {"plant", "plant2"};

/* n rows of random values (about 3.6 bytes each in a part) from ts *t on */
static void snap_rows(tsdbc *c, tsdb_sid id, int64_t *t, size_t n)
{
    size_t step = n < 1000000 ? n : 1000000;
    int64_t *tv = malloc(step * sizeof *tv);
    float *vv = malloc(step * sizeof *vv);
    if (!tv || !vv)
        die("out of memory");
    static uint64_t x = 88172645463325252ull;
    for (size_t done = 0; done < n;) {
        size_t k = n - done < step ? n - done : step;
        for (size_t j = 0; j < k; j++) {
            x ^= x << 13, x ^= x >> 7, x ^= x << 17;
            tv[j] = (*t)++;
            vv[j] = (float)(x >> 40) / 1024.0f;
        }
        int rc = tsdbc_insert_series(c, id, tv, vv, k);
        if (rc != 0)
            die("a write of %zu rows: %s", k, ers(rc));
        done += k;
    }
    free(tv);
    free(vv);
}

/* keepcap's way to a replica that needs a snapshot: nvol volumes of three replicas, `big` rows of random values in
 * each with every replica up, then R (a member that leads none of them) killed and seven flushed segments written a
 * second apart, past repl_retain_s (2 s). R is not started again: the caller sets penv[R] first. */
static void snap_setup(int nvol, size_t big, struct snapset *s)
{
    memset(s, 0, sizeof *s);
    s->nvol = nvol;
    init_retain_s = 2;
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    init_retain_s = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    int rc;
    for (int k = 0; k < nvol; k++) {
        uint32_t num;
        if ((rc = vol_add(0, snap_vols[k], 3, &num)) != 0 || num != (uint32_t)k + 1)
            die("volume add %s: %s, number %" PRIu32, snap_vols[k], cluster_strerror(rc), num);
        for (int i = 0; i < 3; i++)
            await_open_gen(0, snap_vols[k], i, 1, 0);
        struct meta_volume v;
        if (!vol_rec(0, snap_vols[k], &v))
            die("no record of %s", snap_vols[k]);
        s->lead[k] = server_of(v.leader);
        if (s->lead[k] < 0 || s->lead[k] > 2)
            die("the leader of %s is node %u", snap_vols[k], v.leader);
    }
    s->L = s->lead[0];
    s->R = (s->L + 1) % 3;
    if (nvol > 1 && s->R == s->lead[1])
        s->R = (s->R + 1) % 3;
    if (s->R == s->L || (nvol > 1 && s->R == s->lead[1]))
        die("no member leads none of the volumes");
    s->O = 3 - s->L - s->R;
    s->t = 1000;
    for (int k = 0; k < nvol; k++) {
        s->c[k] = client(s->lead[k]);
        s->id[k] = rep_series(s->c[k], rep_names[0], snap_vols[k], 30000);
        int64_t t = s->t;
        snap_rows(s->c[k], s->id[k], &t, big ? big : 1);
        await_isr(0, snap_vols[k], 3, 5000, "every replica up");
    }
    s->t += (int64_t)(big ? big : 1);
    stop(s->R, SIGKILL);
    for (int k = 0; k < nvol; k++)
        await_isr(0, snap_vols[k], 2, 10000, "a replica killed");
    struct timespec s1 = {1, 0};
    for (int r = 0; r < 7; r++) {
        for (int k = 0; k < nvol; k++) {
            int64_t t = s->t;
            snap_rows(s->c[k], s->id[k], &t, 100);
            if ((rc = tsdbc_flush(s->c[k])) != 0)
                die("flush: %s", ers(rc));
        }
        s->t += 100;
        nanosleep(&s1, NULL);
    }
    for (int k = 0; k < nvol; k++) { /* a commit after the last flush: the log start moves */
        int64_t t = s->t;
        snap_rows(s->c[k], s->id[k], &t, 1);
    }
    s->t++;
}

static void snap_close(struct snapset *s)
{
    for (int k = 0; k < s->nvol; k++)
        if (s->c[k])
            tsdbc_close(s->c[k]);
    s->c[0] = s->c[1] = NULL;
}

static void await_log(int i, const char *needle, int ms, const char *what)
{
    uint64_t t0 = now_ms();
    while (!log_has(i, needle)) {
        if (now_ms() - t0 > (uint64_t)ms)
            die("%s (server %d never logged \"%s\" in %d ms)", what, i, needle, ms);
        usleep(50000);
    }
}

/* the last line of server i's log with `needle`, into out ("" when none) */
static void log_line(int i, const char *needle, char *out, size_t n)
{
    char path[PATH_MAX + 80], line[4096];
    snprintf(path, sizeof path, "%s/n%d.log", base, i);
    FILE *f = fopen(path, "r");
    out[0] = 0;
    while (f && fgets(line, sizeof line, f))
        if (strstr(line, needle))
            snprintf(out, n, "%s", line);
    if (f)
        fclose(f);
}

/* entries of server i's directory `sub` whose names begin with `prefix` (-1: no such directory) */
static int dir_count(int i, const char *sub, const char *prefix)
{
    char d[PATH_MAX + 80], p[PATH_MAX + 200];
    server_dir(i, d, sizeof d);
    snprintf(p, sizeof p, "%s/%s", d, sub);
    DIR *dd = opendir(p);
    if (!dd)
        return errno == ENOENT ? -1 : (die("opendir %s: %s", p, strerror(errno)), -1);
    int n = 0;
    for (struct dirent *e; (e = readdir(dd));)
        n += strcmp(e->d_name, ".") && strcmp(e->d_name, "..") && !strncmp(e->d_name, prefix, strlen(prefix));
    closedir(dd);
    return n;
}

/* the copy server i received (TSDB_REPLD_TEST=snaphold keeps it as v<num>.snapheld) holds exactly the files its
 * SNAPMAN names, each of the size and xxh64 SNAPMAN records; returns their bytes (links counted once) */
static uint64_t snap_verify_held(int i, uint32_t num, size_t *nfiles)
{
    char d[PATH_MAX + 80], held[PATH_MAX + 120], f[PATH_MAX + 300];
    server_dir(i, d, sizeof d);
    snprintf(held, sizeof held, "%s/v%" PRIu32 ".snapheld", d, num);
    struct snap_man m;
    memset(&m, 0, sizeof m);
    int rc = snap_man_read(held, &m);
    if (rc != 0)
        die("the snapshot server %d received (%s): its SNAPMAN does not read: %s", i, held, ers(rc));
    uint64_t bytes = 0;
    for (size_t k = 0; k < m.nfiles; k++) {
        snprintf(f, sizeof f, "%s/%s", held, m.files[k].name);
        int fd = open(f, O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            die("the received snapshot lacks %s, which its SNAPMAN names: %s", m.files[k].name, strerror(errno));
        uint64_t size = 0, x = 0;
        if (snap_xxh64_fd(fd, &size, &x) != 0)
            die("read %s: %s", f, strerror(errno));
        close(fd);
        if (size != m.files[k].size || x != m.files[k].xxh64)
            die("the received snapshot's %s has %" PRIu64 " bytes, xxh64 %016" PRIx64 "; its SNAPMAN says %" PRIu64
                ", %016" PRIx64, m.files[k].name, size, x, m.files[k].size, m.files[k].xxh64);
        if (!m.files[k].link)
            bytes += size;
    }
    char sub[64];
    snprintf(sub, sizeof sub, "v%" PRIu32 ".snapheld", num);
    int n = dir_count(i, sub, "");
    if (n != (int)m.nfiles + 1)
        die("the received snapshot holds %d files besides SNAPMAN, which names %zu", n - 1, m.nfiles);
    *nfiles = m.nfiles;
    snap_man_free(&m);
    return bytes;
}

/* the leader's snap_* directories gone (released) within ms */
static void await_released(int L, uint32_t num, int ms)
{
    char sub[32];
    snprintf(sub, sizeof sub, "v%" PRIu32, num);
    uint64_t t0 = now_ms();
    int n;
    while ((n = dir_count(L, sub, SNAP_PREFIX)) > 0) {
        if (now_ms() - t0 > (uint64_t)ms)
            die("the leader still holds %d snapshot directories %d ms after the receiver had them all", n, ms);
        usleep(50000);
    }
}

/* snapxfer: a replica back past the keep caps asks its leader for a snapshot, which the leader makes and sends; the
 * first copy, with a byte flipped on the source after the make, is refused and asked again; the copy received matches
 * its SNAPMAN file for file, and the leader releases its snapshot. The first make takes 4 s, twice the source's
 * link-down timer (2 s here): the source dials its kind-5 link with the first file, so during that make it has none
 * (snaplost's stale refusal dials it first, which is why the check is here), and a make must not count as down
 * (review F3). */
static void scen_snapxfer(void)
{
    scenario("snapxfer");
    for (int i = 0; i < 3; i++) /* the first snapshot made has a byte flipped after SNAPMAN took its hash */
        penv[i] = "TSDB_REPLD_TEST=snapflip,snapslowmake=4000,snapdownms=2000";
    struct snapset s;
    snap_setup(1, 200000, &s);
    char held[PATH_MAX + 64], needle[128];
    penv[s.R] = "TSDB_REPLD_TEST=snaphold";
    start(s.R);
    await_log(s.R, "it needs a snapshot and asks the leader", 15000, "the replica back past the keep cap never asked");
    await_log(s.R, "(snaphold)", 60000, "the replica never received the whole snapshot");
    if (!log_has(s.L, "(snapslowmake)"))
        die("the leader's make did not take the hook's 4 s: the test proves nothing");
    if (log_has(s.L, "stayed down"))
        die("the leader released a snapshot while making it (its link-down timer ran during the make)");
    if (!log_has(s.L, "(snapflip)") || !log_has(s.R, "a file's xxh64 is not the one SNAPMAN names"))
        die("the copy with a flipped byte was not refused (flipped on the leader: %d)", log_has(s.L, "(snapflip)"));
    snprintf(needle, sizeof needle, "snapshot sent to node %" PRIu32 ":", ids[s.R]);
    await_log(s.L, needle, 10000, "the leader never heard the receiver had every file");
    size_t nf = 0;
    uint64_t bytes = snap_verify_held(s.R, 1, &nf);
    await_released(s.L, 1, 5000);
    if (dir_count(s.R, "v1.snap", "") >= 0)
        die("v1.snap is still on the receiver beside the kept copy");
    if (!log_has(s.L, "snapshot made for node"))
        die("the leader never said it made the snapshot");
    struct rsnap_stats sl, sr;
    status_snap(s.L, &sl);
    status_snap(s.R, &sr);
    if (sl.sent != 1 || sl.bytes_sent < bytes || sr.asked < 1 || sr.received != 1 || sr.bytes_received < bytes)
        die("STATUS: the leader sent %" PRIu64 " (%" PRIu64 " bytes), the receiver asked %" PRIu64 " and received %"
            PRIu64 " (%" PRIu64 " bytes); the copy has %" PRIu64 " bytes", sl.sent, sl.bytes_sent, sr.asked,
            sr.received, sr.bytes_received, bytes);
    snprintf(held, sizeof held, "snapshot received from node %" PRIu32 ":", ids[s.L]);
    char line[4096];
    log_line(s.R, held, line, sizeof line);
    int64_t t = s.t++;
    float val = 1;
    int rc = tsdbc_insert_series(s.c[0], s.id[0], &t, &val, 1);
    if (rc != 0)
        die("a write after the snapshot: %s", ers(rc));
    snap_close(&s);
    stop_all();
    printf("  the replica back past the keep cap asked for a snapshot and received %zu files, %" PRIu64 " bytes, each"
           " as SNAPMAN records it; the leader released its copy\n  %s", nf, bytes, line);
}

/* snapnospace (D5): a snapshot larger than the receiver's free space (the hook reports 4 KiB) is refused once SNAPMAN
 * is in, before any file: said once, counted, asked again at a later notice, no file of it written, and the live
 * volumes go on. */
static void scen_snapnospace(void)
{
    scenario("snapnospace");
    struct snapset s;
    snap_setup(1, 0, &s);
    penv[s.R] = "TSDB_REPLD_TEST=snaphold,snapfree=4096";
    start(s.R);
    /* snapfree=4096 lies between the copy's size (a few KB) and that size plus the margin (1 GiB at least): refused for
     * the margin, which D5 keeps for the live volumes (a copy larger than the free space is refused a fortiori) */
    await_log(s.R, "is refused: the file system", 20000,
              "a snapshot whose size plus the margin exceeds the free space was not refused");
    char rl[4096];
    log_line(s.R, "is refused: the file system", rl, sizeof rl);
    const char *of = strstr(rl, "a snapshot of ");
    uint64_t refused = of ? strtoull(of + 14, NULL, 10) : 0;
    if (!refused || refused >= 4096)
        die("the refused snapshot has %" PRIu64 " bytes: not the case of a copy below the free space (%s)", refused, rl);
    char needle[128];
    snprintf(needle, sizeof needle, "snapshot made for node %" PRIu32 ":", ids[s.R]);
    await_log_count(s.L, needle, 2, 20000); /* asked again */
    if (log_count(s.R, "is refused: the file system") != 1)
        die("the refusal for space was said %d times (once per volume)", log_count(s.R, "is refused: the file system"));
    if (log_has(s.R, "snapshot received from node") || dir_count(s.R, "v1.snapheld", "") >= 0)
        die("a snapshot refused for its space was taken");
    snprintf(needle, sizeof needle, "snapshot sent to node %" PRIu32 ":", ids[s.R]);
    if (log_has(s.L, needle))
        die("the leader sent the files of a snapshot the receiver refused");
    int n = dir_count(s.R, "v1.snap", "");
    if (n > 1)
        die("the receiver that refused a snapshot for its space holds %d of its files", n);
    struct rsnap_stats st;
    status_snap(s.R, &st);
    if (st.refused_space < 1 || st.received)
        die("STATUS of the receiver: %" PRIu64 " refused for space, %" PRIu64 " received (want >= 1, 0)",
            st.refused_space, st.received);
    int64_t t = s.t++;
    float val = 1;
    int rc = tsdbc_insert_series(s.c[0], s.id[0], &t, &val, 1);
    if (rc != 0)
        die("a write while the receiver refuses snapshots: %s", ers(rc));
    snap_close(&s);
    stop_all();
    printf("  a snapshot above the free space was refused once SNAPMAN was in (counted %" PRIu64 "), asked again,"
           " no file written\n", st.refused_space);
}

/* snapbusy (D2): a leader that is promoting (the hook) answers BUSY and the receiver asks again once it is not; with
 * rebuild_max 1 a second volume's snapshot waits for the first. */
static void scen_snapbusy(void)
{
    scenario("snapbusy");
    for (int i = 0; i < 3; i++)
        penv[i] = "TSDB_REPLD_TEST=snaphold,snappromoting,snapfakesrc";
    init_rebuild_max = 1, init_rebuild_mb_s = 2;
    struct snapset s;
    snap_setup(2, 1000000, &s);
    init_rebuild_max = 0, init_rebuild_mb_s = 0;
    char hook[PATH_MAX + 80], d[PATH_MAX + 40];
    server_dir(s.L, d, sizeof d);
    snprintf(hook, sizeof hook, "%s/snappromoting", d);
    int fd = open(hook, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        die("%s: %s", hook, strerror(errno));
    close(fd);
    /* a source job on each side (the hook): the leader's fills its rebuild_max, the receiver's must not count
     * against its receivers (two members feeding each other would answer each other BUSY for good) */
    char fakel[PATH_MAX + 80], faker[PATH_MAX + 80];
    snprintf(fakel, sizeof fakel, "%s/snapfakesrc", d);
    server_dir(s.R, d, sizeof d);
    snprintf(faker, sizeof faker, "%s/snapfakesrc", d);
    for (int k = 0; k < 2; k++) {
        if ((fd = open(k ? faker : fakel, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644)) < 0)
            die("%s: %s", k ? faker : fakel, strerror(errno));
        close(fd);
    }
    start(s.R);
    await_log(s.R, "it needs a snapshot and asks the leader", 20000,
              "the receiver did not ask while a source job ran on it (rebuild_max counted the source)");
    await_log(s.R, "is busy (promoting)", 20000, "a leader that is promoting did not answer BUSY");
    char needle[128];
    snprintf(needle, sizeof needle, "volume 1: snapshot made for node %" PRIu32 ":", ids[s.R]);
    if (log_has(s.L, needle))
        die("a leader that is promoting made a snapshot");
    unlink(hook);
    await_log(s.R, "is busy (rebuild_max)", 20000, "a leader running rebuild_max sources did not answer BUSY");
    if (log_has(s.L, needle))
        die("a leader running rebuild_max sources made a snapshot");
    unlink(fakel);
    await_log_count(s.R, "snapshot received from node", 2, 90000);
    if (!log_has(s.R, "needs a snapshot and waits"))
        die("two snapshots ran at once on the receiver with rebuild_max 1");
    size_t nf;
    snap_verify_held(s.R, 1, &nf);
    snap_verify_held(s.R, 2, &nf);
    struct rsnap_stats st;
    status_snap(s.R, &st);
    if (st.busy < 1 || st.received != 2)
        die("STATUS of the receiver: %" PRIu64 " busy, %" PRIu64 " received (want >= 1, 2)", st.busy, st.received);
    snap_close(&s);
    stop_all();
    printf("  a promoting leader and one at rebuild_max answered BUSY and were asked again; with rebuild_max 1 the"
           " second snapshot waited, and a source job did not hold the receiver\n");
}

/* snapsrckill: the source killed in the middle of a transfer: the receiver drops its partial copy when the new
 * leader's notice names another source, and asks the new leader, whose copy arrives whole. */
static void scen_snapsrckill(void)
{
    scenario("snapsrckill");
    init_rebuild_mb_s = 2;
    struct snapset s;
    snap_setup(1, 2000000, &s);
    init_rebuild_mb_s = 0;
    penv[s.R] = "TSDB_REPLD_TEST=snaphold";
    start(s.R);
    char needle[128];
    snprintf(needle, sizeof needle, "snapshot made for node %" PRIu32 ":", ids[s.R]);
    await_log(s.L, needle, 20000, "the leader never made the snapshot");
    usleep(1500000);
    if (log_has(s.R, "snapshot received from node"))
        die("the snapshot arrived before the source could be killed (too small for this test)");
    if (dir_count(s.R, "v1.snap", "") < 2)
        die("the receiver holds no file of the snapshot 1.5 s into its transfer");
    snap_close(&s);
    stop(s.L, SIGKILL);
    await_log(s.R, "is abandoned", 40000, "the receiver kept a transfer from a killed source");
    if (!log_has(s.R, "the notice names another source"))
        die("the receiver dropped the killed source's transfer for another reason than the new leader's notice");
    snprintf(needle, sizeof needle, "snapshot received from node %" PRIu32 ":", ids[s.O]);
    await_log(s.R, needle, 90000, "the new leader's snapshot never arrived");
    size_t nf;
    uint64_t bytes = snap_verify_held(s.R, 1, &nf);
    stop_all();
    printf("  the source killed mid-transfer: the partial copy was dropped and the new leader's (%zu files, %" PRIu64
           " bytes) arrived whole\n", nf, bytes);
}

/* snaplarge (D7): a large insert while a snapshot moves at rebuild_mb_s 5 commits within a second (the snapshot's
 * kind 5 does not hold the COMMITs' kind 2), and the transfer takes at least its size over the rate. */
static void scen_snaplarge(void)
{
    scenario("snaplarge");
    init_rebuild_mb_s = 5;
    struct snapset s;
    snap_setup(1, 6000000, &s);
    init_rebuild_mb_s = 0;
    penv[s.R] = "TSDB_REPLD_TEST=snaphold";
    start(s.R);
    char needle[128], line[4096];
    snprintf(needle, sizeof needle, "snapshot made for node %" PRIu32 ":", ids[s.R]);
    await_log(s.L, needle, 30000, "the leader never made the snapshot");
    usleep(500000);
    uint64_t t0 = now_ms();
    snap_rows(s.c[0], s.id[0], &s.t, 2000000);
    uint64_t took = now_ms() - t0;
    if (log_has(s.R, "snapshot received from node"))
        die("the snapshot was in before the large insert ended: the test proves nothing");
    if (took >= 1000)
        die("a large insert of 2000000 rows took %" PRIu64 " ms while a snapshot moved (kind 5 holds kind 2)", took);
    await_log(s.R, "(snaphold)", 120000, "the snapshot never arrived");
    size_t nf;
    uint64_t bytes = snap_verify_held(s.R, 1, &nf);
    log_line(s.R, "snapshot received from node", line, sizeof line);
    const char *in = strstr(line, " bytes in ");
    double secs = in ? strtod(in + 10, NULL) : 0;
    double least = (double)bytes / (5 << 20);
    if (secs < least * 0.9)
        die("%" PRIu64 " bytes moved in %.1f s at rebuild_mb_s 5 (at least %.1f s)", bytes, secs, least);
    snap_close(&s);
    stop_all();
    printf("  a large insert of 2000000 rows took %" PRIu64 " ms during a snapshot of %" PRIu64 " bytes, which took"
           " %.1f s at 5 MB/s\n", took, bytes, secs);
}

/* snaplost: the losses of a transfer's control frames. The first request names a newer set version (the hook): the
 * leader refuses it as stale and is asked again (a make past the link-down timer is snapxfer's). The DONE of a data
 * file is lost to a cut
 * connection after the receiver had the file whole: the sender begins it again, and the receiver must neither count
 * it twice nor call an incomplete copy received (F1). The receiver's DONE 0 is lost: the source, waiting for it,
 * releases its copy after its timer (3 s here) instead of holding it and a rebuild_max slot for 6 hours (F2). */
static void scen_snaplost(void)
{
    scenario("snaplost");
    for (int i = 0; i < 3; i++)
        penv[i] = "TSDB_REPLD_TEST=snapdownms=2000,snapwaitdonems=3000";
    struct snapset s;
    snap_setup(1, 200000, &s);
    penv[s.R] = "TSDB_REPLD_TEST=snaphold,snapcutdone,snapnodone,snapstalereq";
    start(s.R);
    await_log(s.R, "(snapstalereq)", 15000, "the replica back past the keep cap never asked");
    await_log(s.L, "is refused as stale", 15000, "a request of another set version was served");
    await_log(s.R, "(snaphold)", 40000, "the replica never received the whole snapshot");
    uint64_t t0 = now_ms();
    if (!log_has(s.R, "(snapcutdone)"))
        die("no DONE of a file was lost to a cut connection: the test proves nothing");
    size_t nf = 0;
    uint64_t bytes = snap_verify_held(s.R, 1, &nf);
    struct rsnap_stats sr;
    status_snap(s.R, &sr);
    if (sr.received != 1 || sr.failed != 0)
        die("STATUS of the receiver: %" PRIu64 " received, %" PRIu64 " failed (want 1, 0): the file begun again after"
            " its lost DONE broke the first copy", sr.received, sr.failed);
    char needle[160];
    snprintf(needle, sizeof needle, "snapshot made for node %" PRIu32 ":", ids[s.R]);
    if (log_has(s.L, "stayed down") || log_count(s.L, needle) != 1)
        die("the leader made %d snapshots for one served request (want 1)", log_count(s.L, needle));
    await_log(s.L, "never said it had the last file", 15000, "the leader kept waiting for a DONE that was lost");
    await_released(s.L, 1, 5000);
    printf("  a stale request refused; a file begun again after its lost DONE"
           " counted once (%zu files, %" PRIu64 " bytes, whole); the source freed %.1f s after the copy was in although"
           " its DONE was lost\n", nf, bytes, (double)(now_ms() - t0) / 1000);
    snap_close(&s);
    stop_all();
}

/* snapstall: a source that stalls while it stays the leader (the hook: the first ignores GO) is left by the receiver
 * when nothing arrives for its stall timer (3 s here), which asks again; a receiver killed while its source waits
 * for its DONE (lost, the hook) releases the source after the link-down timer (2 s here), well before the 60 s of
 * the DONE timer. Each release here has no other path that would end it. */
static void scen_snapstall(void)
{
    scenario("snapstall");
    for (int i = 0; i < 3; i++)
        penv[i] = "TSDB_REPLD_TEST=snapsrcstall,snapdownms=2000";
    struct snapset s;
    snap_setup(1, 0, &s);
    penv[s.R] = "TSDB_REPLD_TEST=snaphold,snapstallms=3000,snapnodone";
    start(s.R);
    await_log(s.R, "(snaphold)", 40000, "the replica never received the snapshot");
    if (!log_has(s.L, "(snapsrcstall)") || !log_has(s.R, "nothing arrived for"))
        die("the receiver of a stalled source did not leave it (stalled: %d)", log_has(s.L, "(snapsrcstall)"));
    size_t nf;
    snap_verify_held(s.R, 1, &nf);
    char needle[160];
    snprintf(needle, sizeof needle, "snapshot made for node %" PRIu32 ":", ids[s.R]);
    if (log_count(s.L, needle) != 2)
        die("the leader made %d snapshots (want 2: the stalled one and the one asked again)", log_count(s.L, needle));
    usleep(500000); /* the source waits for the DONE that is not sent */
    char sub[32];
    snprintf(sub, sizeof sub, "v1");
    if (dir_count(s.L, sub, SNAP_PREFIX) < 1)
        die("the leader released its copy before the receiver's link went down: the test proves nothing");
    stop(s.R, SIGKILL);
    uint64_t t0 = now_ms();
    await_log(s.L, "stayed down", 10000, "a source whose receiver is gone was not released");
    await_released(s.L, 1, 5000);
    snap_close(&s);
    stop_all();
    printf("  a stalled source left after the stall timer and asked again; a source whose receiver died released"
           " after %.1f s\n", (double)(now_ms() - t0) / 1000);
}

/* keepfloor: below the caps the leader keeps every segment a replica away has not acked (design-cluster.md 8.3). The
 * replica is killed, the leader writes and flushes segments past it, and the replica back must catch up from them,
 * join the ISR again and never be told it needs a snapshot (r3d review: keep_ignores_peers survived every test). */
static void scen_keepfloor(void)
{
    scenario("keepfloor");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    uint64_t rows = 0;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    rows++;
    await_isr(0, "plant", 3, 5000, "every replica up");
    int R = (L + 1) % 3;
    stop(R, SIGKILL);
    await_isr(0, "plant", 2, 10000, "a replica killed");
    int64_t t = 10000;
    for (int k = 0; k < 5; k++) {
        int64_t tv[100];
        float vv[100];
        for (int j = 0; j < 100; j++)
            tv[j] = t++, vv[j] = (float)j;
        if ((rc = tsdbc_insert_series(c, id, tv, vv, 100)) != 0)
            die("a write with a replica away: %s", ers(rc));
        rows += 100;
        if ((rc = tsdbc_flush(c)) != 0)
            die("flush: %s", ers(rc));
    }
    if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0) /* a commit after the last flush: a save of the keep */
        die("a write with a replica away: %s", ers(rc));
    rows++;
    struct timespec s1 = {1, 0};
    nanosleep(&s1, NULL);
    int nw = wal_count(L, 1);
    if (nw < 5)
        die("the leader keeps %d WAL segments, not the 5 flushed ones a replica away has not acked", nw);
    start(R);
    await_isr(0, "plant", 3, 30000, "the killed replica back and caught up from the kept segments");
    if (log_has(R, "it needs a snapshot"))
        die("a replica back within the keep caps was told it needs a snapshot");
    if ((uint64_t)rows_of(c, id) != rows)
        die("the leader reads %d rows, not %" PRIu64, rows_of(c, id), rows);
    tsdbc_close(c);
    stop_all();
    uint64_t wantr[REP_NAMES] = {rows, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
    printf("  with a replica away the leader keeps its %d flushed segments; the replica back catches up from them and"
           " holds the leader's rows\n", nw);
}

/* reopenhold: a replica killed with a COMMIT marker in its log whose file it has not staged starts its core with the
 * durable end below that marker (repl_durable.hold), never at its log end. It did start at the log end: its first ACK
 * after the restart claimed the whole log, the leader's keep bound passed the marker, the segment and the link the
 * file is served from went, and the replica asked for the file in vain for the rest of the run (jepsen-cluster kill,
 * kv and classes, 2026-11-18: "it needs a snapshot", the ISR never whole again). R does not stage the large write's
 * file (dropbulk) and sends no FETCH (nofetch); its log passes the marker and the leader seals the marker's segment;
 * R is killed, and back its syncs wait 4 s (syncpause: its ACKs carry the durable end its core opened with) and it
 * fetches only 5 s late (fetchlate): time enough for the leader, which goes on writing, to save its keep bound by a
 * false ACK and drop the segment. Mutant opennohold. */
static void scen_reopenhold(void)
{
    scenario("reopenhold");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id0 = rep_series(c, rep_names[0], "plant", 30000), id2 = rep_series(c, rep_names[2], "plant", 30000);
    uint64_t n[REP_NAMES] = {0};
    int64_t t = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id0, &t, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    n[0]++;
    await_isr(0, "plant", 3, 10000, "every replica up");
    int R = (L + 1) % 3;
    stop(R, SIGKILL);
    await_isr(0, "plant", 2, 10000, "a replica killed");
    penv[R] = "TSDB_REPLD_TEST=dropbulk,nofetch";
    start(R);
    penv[R] = NULL;
    await_open_gen(L, "plant", R, 2, 0);
    struct rep_writer wb = {cports[L], id2, 2000000000, 0, 0, 0, 100000, 0}; /* above wal_max_batch_rows */
    rep_write_thread(&wb);
    if (wb.rc != 0)
        die("a large write with a replica that drops its file: %s", ers(wb.rc));
    n[2] += 100000;
    for (uint64_t t0 = now_ms(); !log_has(R, "withholds the FETCH");) {
        if (now_ms() - t0 > 20000)
            die("the replica that dropped the large write's file did not find its COMMIT marker without it in 20 s");
        struct timespec ms = {0, 50000000};
        nanosleep(&ms, NULL);
    }
    /* the leader seals the marker's segment, and R's log passes its end. The large write flushed the memtable before
     * its marker, and a flush of an empty memtable starts no segment: a row first, or the marker's segment stays open
     * until an idle seal that the 1.5 s below may miss, and the leader drops it only after R fetched (the mutant
     * survived 2 runs of 3) */
    t += 1000;
    if ((rc = tsdbc_insert_series(c, id0, &t, &val, 1)) != 0)
        die("a write after the large one: %s", ers(rc));
    n[0]++;
    if ((rc = tsdbc_flush(c)) != 0)
        die("flush: %s", ers(rc));
    for (int k = 0; k < 20; k++) {
        t += 1000;
        if ((rc = tsdbc_insert_series(c, id0, &t, &val, 1)) != 0)
            die("a write after the large one: %s", ers(rc));
        n[0]++;
    }
    struct timespec s15 = {1, 500000000};
    nanosleep(&s15, NULL);
    stop(R, SIGKILL);
    penv[R] = "TSDB_REPLD_TEST=fetchlate,syncpause";
    start(R);
    penv[R] = NULL;
    /* writes go on meanwhile: the leader saves its floor, and with it the keep bound, only as the commit point moves */
    for (uint64_t t0 = now_ms(); isr_of(L, "plant", NULL) != 3 || !log_has(R, "fetches");) {
        t += 1000;
        if ((rc = tsdbc_insert_series(c, id0, &t, &val, 1)) != 0)
            die("a write while the replica is back: %s", ers(rc));
        n[0]++;
        if (log_has(L, "it needs a snapshot"))
            die("the leader dropped the file of a COMMIT marker that a restarted replica had not staged: it needs a "
                "snapshot");
        if (now_ms() - t0 > 60000)
            die("the replica restarted without the large write's file: ISR %" PRIu32 " members, fetched %d, after "
                "60000 ms", isr_of(L, "plant", NULL), log_has(R, "fetches"));
        struct timespec ms = {0, 50000000};
        nanosleep(&ms, NULL);
    }
    struct timespec s1 = {1, 0};
    nanosleep(&s1, NULL);
    if (log_has(L, "it needs a snapshot"))
        die("the leader dropped the file of a COMMIT marker that a restarted replica had not staged: it needs a "
            "snapshot");
    t += 1000;
    if ((rc = tsdbc_insert_series(c, id0, &t, &val, 1)) != 0)
        die("a write with every replica back: %s", ers(rc));
    n[0]++;
    await_isr(0, "plant", 3, 30000, "every replica back");
    tsdbc_close(c);
    stop_all();
    rep_same(n, "after a clean stop");
    printf("  a replica killed with a COMMIT marker whose file it had not staged acknowledges below it when back; the"
           " leader keeps the file, the replica fetches it and holds the leader's rows\n");
}

/* the part files in server i's copy of volume 1 (retired ones too: a merge leaves its sources under retired_) */
static int part_count(int i)
{
    char dir[PATH_MAX + 80], v[PATH_MAX + 120];
    server_dir(i, dir, sizeof dir);
    snprintf(v, sizeof v, "%s/v1", dir);
    DIR *d = opendir(v);
    if (!d)
        die("opendir %s: %s", v, strerror(errno));
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t l = strlen(e->d_name);
        n += l > 5 && !strcmp(e->d_name + l - 5, ".part") && strncmp(e->d_name, "tmp_", 4) &&
             strncmp(e->d_name, "stage_", 6) && strncmp(e->d_name, "retired_", 8);
    }
    closedir(d);
    return n;
}

/* the newest WAL segment of server i's copy of volume 1: one more for each flush (no promotion in between) */
static uint64_t wal_newest(int i)
{
    char dir[PATH_MAX + 80], v[PATH_MAX + 120];
    server_dir(i, dir, sizeof dir);
    snprintf(v, sizeof v, "%s/v1", dir);
    DIR *d = opendir(v);
    if (!d)
        die("opendir %s: %s", v, strerror(errno));
    uint64_t hi = 0, seq;
    struct dirent *e;
    while ((e = readdir(d)))
        if (sscanf(e->d_name, "wal_%" SCNx64 ".log", &seq) == 1 && seq > hi)
            hi = seq;
    closedir(d);
    return hi;
}

/* The idle seal (design-cluster.md 5.3, D7): with flushes 10 min apart, an idle leader flushes its memtable after
 * idle_seal_ms (1 s) and its next NOW marker starts a new segment, so the replicas seal and flush theirs too. */
static void scen_idleseal(void)
{
    scenario("idleseal");
    for (int i = 0; i < 3; i++) {
        xargs[i] = "--flush-interval-ms 600000";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t tv[100];
    float vv[100];
    for (int j = 0; j < 100; j++)
        tv[j] = 1000 + j, vv[j] = (float)j;
    if ((rc = tsdbc_insert_series(c, id, tv, vv, 100)) != 0)
        die("a write: %s", ers(rc));
    uint64_t t0 = now_ms(), took[3] = {0, 0, 0};
    for (int left = 3; left;) {
        if (now_ms() - t0 > 6000)
            die("an idle volume's rows were not in parts on every server 6 s after the write (leader %d: %d parts,"
                " replicas %d, %d)", L, part_count(L), part_count((L + 1) % 3), part_count((L + 2) % 3));
        for (int i = 0; i < 3; i++)
            if (!took[i] && part_count(i) > 0)
                took[i] = now_ms() - t0, left--;
        struct timespec ms50 = {0, 50000000};
        nanosleep(&ms50, NULL);
    }
    printf("  an idle volume is in parts after %" PRIu64 " ms on the leader, %" PRIu64 " and %" PRIu64
           " ms on the replicas (idle_seal_ms 1000, flushes 10 min apart)\n",
           took[L], took[(L + 1) % 3], took[(L + 2) % 3]);
    /* the cost: a write every 2 s is a flush every 2 s (the parts of an hour, before merges) */
    uint64_t w0 = wal_newest(L);
    for (int k = 0; k < 10; k++) {
        int64_t t = 5000 + k;
        if ((rc = tsdbc_insert_series(c, id, &t, vv, 1)) != 0)
            die("a write: %s", ers(rc));
        struct timespec s2 = {2, 0};
        nanosleep(&s2, NULL);
    }
    int made = (int)(wal_newest(L) - w0);
    printf("  a write every 2 s: %d flushes in 20 s on the leader (about %d parts an hour, before merges)\n", made,
           made * 180);
    if (made < 5)
        die("a write every 2 s made %d flushes in 20 s: the idle seal does not flush", made);
    /* and an idle volume's WAL grows by its NOW markers alone */
    char dir[PATH_MAX + 80], wp[PATH_MAX + 160];
    server_dir(L, dir, sizeof dir);
    struct stat st0, st1;
    struct timespec s5 = {5, 0};
    /* The last write's seal may still be under way (slow under ASan): its segment then goes once flushed and the NOW
     * marker starts the next one. Measure a segment that stays for the whole 5 s; a third roll is a failure. */
    for (int tries = 0;; tries++) {
        uint64_t w = wal_newest(L);
        snprintf(wp, sizeof wp, "%s/v1/wal_%016" PRIx64 ".log", dir, w);
        if (stat(wp, &st0) == 0 && (nanosleep(&s5, NULL), stat(wp, &st1)) == 0)
            break;
        int e = errno;
        if (e != ENOENT || tries == 2 || wal_newest(L) == w)
            die("stat %s: %s", wp, strerror(e));
        printf("  the newest segment rolled while measured (the last seal still running); again\n");
    }
    printf("  an idle leader's WAL grows by %lld bytes in 5 s (NOW markers; about %lld KB an hour)\n",
           (long long)(st1.st_size - st0.st_size), (long long)(st1.st_size - st0.st_size) * 720 / 1024);
    tsdbc_close(c);
    stop_all();
    uint64_t wantr[REP_NAMES] = {110, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
}

/* shipstall (the r3d review's M2): a large write over ten partitions with a replica stopped (SIGSTOP: its links stay
 * up, no DONE comes back) waits for its files under one deadline per COMMIT, and not past the moment the stopped
 * replica leaves the ISR; it used to wait 30 s per file (mutant shipperfile). Running again, the replica fetches what
 * it missed, joins the ISR and holds the leader's rows. */
static void scen_shipstall(void)
{
    scenario("shipstall");
    for (int i = 0; i < 3; i++)
        start(i);
    init_isr_lag_ms = 500; /* the stopped replica leaves the ISR well before the ship deadline (3 s) */
    init_cluster(0, 600);
    init_isr_lag_ms = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts1 = 1000;
    float val1 = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts1, &val1, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    await_isr(0, "plant", 3, 5000, "every replica up");

    enum { BIG = 100000 };
    int64_t *ts = malloc(BIG * sizeof *ts);
    float *val = malloc(BIG * sizeof *val);
    if (!ts || !val)
        die("out of memory");
    for (int j = 0; j < BIG; j++) /* ten days from 2024-01-01: ten partitions, ten files in one COMMIT */
        ts[j] = 1704067200000LL + 8640LL * j, val[j] = (float)(j % 977);
    int R = (L + 1) % 3;
    pause_server(R, 1);
    uint64_t t0 = now_ms();
    if ((rc = tsdbc_insert_series(c, id, ts, val, BIG)) != 0)
        die("a large write with a replica stopped: %s", ers(rc));
    uint64_t took = now_ms() - t0;
    free(ts);
    free(val);
    /* the write ends once the stopped replica has left the ISR (isr_lag_ms 500, then a heartbeat and the metadata
     * group: about 1 s), not at the ship deadline (3 s: mutant shipnoisr) nor later (r3d-isr review M6: a bound of
     * 15 s let a deadline five times too long pass) */
    if (took > 2500)
        die("a large write over ten partitions with a replica stopped took %" PRIu64 " ms (the ship wait did not end"
            " when the replica left the ISR?)", took);
    await_isr(0, "plant", 2, 10000, "a replica stopped under a large write");
    pause_server(R, 0);
    await_isr(0, "plant", 3, 30000, "the stopped replica continued");
    uint64_t rows = 1 + BIG;
    if ((uint64_t)rows_of(c, id) != rows)
        die("the leader reads %d rows, not %" PRIu64, rows_of(c, id), rows);
    tsdbc_close(c);
    stop_all();
    uint64_t wantr[REP_NAMES] = {rows, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
    printf("  a large write with a replica stopped took %" PRIu64 " ms; the replica caught up and holds its rows\n",
           took);
}

/* replindet (D12): a replicated write whose outcome is unknown (appended on the leader, which then stops, deposing
 * itself, before the commit point passed it) never reaches the client as TSDB_ERR_INDETERMINATE (-110) or as a
 * refusal: tsdbd closes the connection without an answer, and the client sees a lost connection (TSDB_ERR_IO). */
static void scen_replindet(void)
{
    scenario("replindet");
    for (int i = 0; i < 3; i++) {
        /* every node may lead: rep_series' retries reach the new writer between its swap and M_WRITER, where a
         * write once deposed the fresh leader for good (the volume then answered UNAVAILABLE until a restart) */
        penv[i] = "TSDB_REPLD_TEST=promotewait";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    tsdbc_close(c);
    int R1 = (L + 1) % 3, R2 = (L + 2) % 3;
    pause_server(R1, 1); /* no replica acknowledges: the commit point stops below the next write */
    pause_server(R2, 1);
    struct rep_writer w = {cports[L], id, 2000, 1, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    struct timespec ms500 = {0, 500000000};
    nanosleep(&ms500, NULL);
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE))
        die("a write with both replicas paused was answered: %s", ers(w.rc));
    stop(L, SIGTERM); /* the driver deposes the leader: the waiting write's outcome is unknown */
    pthread_join(th, NULL);
    if (w.rc != TSDB_ERR_IO)
        die("a write of unknown outcome was answered %d (%s), not with a closed connection", w.rc, ers(w.rc));
    pause_server(R1, 0);
    pause_server(R2, 0);
    stop_all();
    printf("  a write left of unknown outcome by its leader's stop gets a closed connection, never an answer\n");
}

/* The rows of `id` in [from, to) on server i, once its answer is not UNAVAILABLE (a promotion under way): -1 after ms */
static int rows_in(int i, tsdb_sid id, int64_t from, int64_t to, int ms)
{
    uint64_t t0 = now_ms();
    for (;;) {
        tsdbc *c = client(i);
        nrows = 0;
        int rc = tsdbc_query_raw(c, id, from, to, count_rows, NULL);
        tsdbc_close(c);
        if (rc == 0)
            return nrows;
        if (now_ms() - t0 > (uint64_t)ms)
            return -1;
        usleep(50000);
    }
}

/* Says which of the rows t0, t0 + 1000, ... (n of them) server i lacks: a failed check names what it lost */
static unsigned char *seen_rows;
static int seen_n;
static int64_t seen_t0;
static int mark_rows(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ctx, (void)v;
    for (size_t k = 0; k < n; k++) {
        int64_t j = (ts[k] - seen_t0) / 1000;
        if (j >= 0 && j < seen_n && (ts[k] - seen_t0) % 1000 == 0)
            seen_rows[j] = 1;
    }
    return 0;
}

static void missing_rows(int i, tsdb_sid id, int64_t t0, int n)
{
    seen_rows = calloc((size_t)n + 1, 1);
    if (!seen_rows)
        return;
    seen_n = n, seen_t0 = t0;
    tsdbc *c = client(i);
    int rc = tsdbc_query_raw(c, id, t0, t0 + 1000LL * n, mark_rows, NULL);
    tsdbc_close(c);
    printf("  server %d (query %s) lacks:", i, ers(rc));
    for (int j = 0; j < n; j++)
        if (!seen_rows[j])
            printf(" row %d (ts %" PRId64 ")", j, (int64_t)(t0 + 1000LL * j));
    printf("\n");
    fflush(stdout);
    free(seen_rows);
    seen_rows = NULL;
}

/* design-cluster.md 6.2-6.3 (r3e-failover): the leader of a replicated volume is killed under writes. The Raft leader
 * sees it down (node_timeout, then its lease's end), fences the ISR, and a replica leads at epoch e+1; every
 * acknowledged write is there; the new leader takes writes; the old leader restarts as a replica (led e: it never
 * resumes), is cut by the new leader's history, catches up and holds the same rows as the others. */
static void scen_failover(void)
{
    scenario("failover");
    strict_reads = 1; /* every read on the leader (D5), as in production */
    for (int i = 0; i < 3; i++)
        start(i);
    /* isr_lag_ms well above unanswered_ms (1000): the killed old leader, which the promotion keeps in the ISR as
     * unanswered, must leave it by unanswered_ms, not by the lag rule (design-cluster.md 6.2; mutant keepunanswered) */
    init_isr_lag_ms = 10000;
    init_cluster(0, 600);
    init_isr_lag_ms = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "fo", 3, &num)) != 0 || num != 1)
        die("volume add fo: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "fo", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "fo", &v))
        die("no record of fo");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of fo is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "fo", 30000);
    tsdbc_close(c);
    await_isr(0, "fo", 3, 30000, "every replica in the ISR before the kill");
    struct rep_writer w = {cports[L], id, 1000000, 1000000, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    uint64_t t0 = now_ms();
    while (__atomic_load_n(&w.acked, __ATOMIC_ACQUIRE) < 300) {
        if (now_ms() - t0 > 30000)
            die("the writer had %d rows acknowledged in 30 s", __atomic_load_n(&w.acked, __ATOMIC_ACQUIRE));
        usleep(10000);
    }
    signal_child(pids[L], SIGKILL);
    uint64_t tk = now_ms();
    if (await_exit(L, 5000) < 0)
        die("server %d did not die of SIGKILL", L);
    pthread_join(th, NULL);
    int acked = __atomic_load_n(&w.acked, __ATOMIC_ACQUIRE);
    int via = (L + 1) % 3, Nw = -1;
    for (;;) {
        if (vol_rec(via, "fo", &v) && v.leader != ids[L] && v.epoch > e0) {
            Nw = server_of(v.leader);
            break;
        }
        if (now_ms() - tk > 30000)
            die("no new leader of fo 30 s after its leader was killed (epoch %" PRIu64 ", leader %u)", v.epoch, v.leader);
        usleep(20000);
    }
    if (v.epoch != e0 + 1 || Nw < 0 || Nw == L)
        die("the new record of fo: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    uint64_t tr = now_ms();
    /* the promotion kept the dead old leader in the ISR, unanswered: it goes after unanswered_ms (1 s), well before the
     * lag rule's 10 s would take it (until then it holds the new leader's commit point, its EPOCH marker first) */
    {
        uint64_t version;
        while (isr_of(via, "fo", &version) != 2) {
            if (now_ms() - tr > 4000)
                die("the killed old leader was still in the ISR of fo %" PRIu64 " ms after the promotion (unanswered_ms "
                    "1000, isr_lag_ms 10000)", now_ms() - tr);
            usleep(50000);
        }
        printf("  the killed old leader left the ISR %" PRIu64 " ms after the record named the new leader\n",
               now_ms() - tr);
    }
    /* a replica answers reads from what it applied (it may lag the commit point): the new leader's own answer counts,
     * once it leads (after its EPOCH marker is committed and applied, and the engine reopened as the writer) */
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    uint64_t tl = now_ms();
    int got = rows_in(Nw, id, 1000000, 1000000 + 1000LL * acked, 30000); /* [from, to): rows 0 .. acked - 1 */
    if (got != acked) {
        missing_rows(Nw, id, 1000000, acked);
        die("the new leader (server %d) holds %d of the %d acknowledged rows", Nw, got, acked);
    }
    printf("  leader %d killed after %d acknowledged rows: the record names server %d at epoch %" PRIu64 " after %" PRIu64
           " ms, which leads after %" PRIu64 " ms and holds every one\n", L, acked, Nw, v.epoch, tr - tk, tl - tk);
    /* the new leader takes writes */
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the new leader: %s", ers(rc));
    tsdbc_close(c);
    /* a write to the other replica is MOVED to the new leader, and a client with a budget follows it there */
    int R = 3 - L - Nw;
    c = client(R);
    ts = 9001000;
    insert_expect_moved(c, id, ts, val, 1, v.leader, v.epoch, "a write to a replica after the failover");
    tsdbc_set_retry(c, 10000);
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write that follows MOVED from server %d: %s", R, ers(rc));
    tsdbc_close(c);
    /* the old leader restarts as a replica and rejoins; it never resumes leading: writes to it are MOVED */
    start(L);
    await_isr(via, "fo", 3, 60000, "the old leader back in the ISR");
    c = client(L);
    ts = 9002000;
    insert_expect_moved(c, id, ts, val, 1, v.leader, v.epoch, "a write to the old leader after it rejoined");
    tsdbc_close(c);
    int total = rows_in(Nw, id, 0, INT64_MAX / 2, 10000);
    if (total < acked + 2)
        die("the new leader holds %d rows after the rejoin, fewer than the %d acknowledged", total, acked + 2);
    stop_all();
    uint64_t wantr[REP_NAMES] = {0};
    wantr[0] = (uint64_t)total;
    rep_same(wantr, "after the failover");
    printf("  the old leader came back as a replica, was cut and caught up: every server holds the same %d rows\n",
           total);
}

/* failoverkeep: a replica keeps its flushed segments under the retention caps past its commit floor (design-cluster.md
 * 8.3), so that, promoted, it can serve a member that was away. R is killed, the leader L writes and flushes segments
 * past it (B, the other replica, flushes them too and its floor passes them), L is killed and B leads; R back must catch
 * up from B's segments and join the ISR, never be told it needs a snapshot (jepsen-cluster partition, fHdR4T: a
 * promoted replica had unlinked what a laggard still needed). Mutant replicadropsfloor (the floor alone again). */
static void scen_failoverkeep(void)
{
    scenario("failoverkeep");
    /* two witnesses (servers 3, 4): five voters, so the metadata group keeps a majority with L and R down, and the
     * volume's replicas are on servers 0-2 */
    for (int i = 3; i < 5; i++) {
        is_witness[i] = 1;
        xargs[i] = "--peer-port 0";
        char wdir[PATH_MAX + 160];
        snprintf(wdir, sizeof wdir, "%s/n%d", base, i);
        if (mkdir(wdir, 0755) != 0)
            die("mkdir %s: %s", wdir, strerror(errno));
    }
    for (int i = 0; i < 5; i++)
        start(i);
    init_cluster(0, 600);
    for (int i = 1; i < 5; i++)
        node_add(0, i);
    unsigned want = 5;
    await_view(0, p_voters, &want, "five voters (two witnesses)");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "fk", 3, &num)) != 0 || num != 1)
        die("volume add fk: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "fk", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "fk", &v))
        die("no record of fk");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of fk is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "fk", 30000);
    int64_t ts = 1000;
    float val = 1;
    uint64_t rows = 0;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    rows++;
    await_isr(L, "fk", 3, 5000, "every replica up");
    int R = (L + 1) % 3, B = (L + 2) % 3;
    stop(R, SIGKILL);
    await_isr(L, "fk", 2, 10000, "a replica killed");
    int64_t t = 10000;
    for (int k = 0; k < 5; k++) {
        int64_t tv[100];
        float vv[100];
        for (int j = 0; j < 100; j++)
            tv[j] = t++, vv[j] = (float)j;
        if ((rc = tsdbc_insert_series(c, id, tv, vv, 100)) != 0)
            die("a write with a replica away: %s", ers(rc));
        rows += 100;
        if ((rc = tsdbc_flush(c)) != 0)
            die("flush: %s", ers(rc));
    }
    if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0) /* a commit after the last flush: B's floor passes them */
        die("a write with a replica away: %s", ers(rc));
    rows++;
    tsdbc_close(c);
    struct timespec s1 = {1, 0};
    nanosleep(&s1, NULL);
    int nb = wal_count(B, 1);
    stop(L, SIGKILL);
    uint64_t tk = now_ms();
    for (;;) {
        if (vol_rec(B, "fk", &v) && v.leader == ids[B] && v.epoch > e0)
            break;
        if (now_ms() - tk > 30000)
            die("server %d was not promoted 30 s after the leader was killed (epoch %" PRIu64 ", leader %u)", B, v.epoch,
                v.leader);
        usleep(20000);
    }
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(B, lead, 1, 30000);
    start(R);
    uint64_t t0 = now_ms();
    for (;;) {
        if (log_has(R, "it needs a snapshot"))
            die("the replica away during the failover was told it needs a snapshot by the promoted leader (which held %d"
                " WAL segments before the failover)", nb);
        int in = 0;
        if (vol_rec(B, "fk", &v))
            for (uint32_t k = 0; k < v.nisr; k++)
                in |= v.isr[k] == ids[R];
        if (in)
            break;
        if (now_ms() - t0 > 30000)
            die("the replica away during the failover did not join the new leader's ISR in 30 s");
        usleep(50000);
    }
    c = client(B);
    tsdbc_set_retry(c, 10000);
    t++;
    if ((rc = tsdbc_insert_series(c, id, &t, &val, 1)) != 0)
        die("a write to the new leader: %s", ers(rc));
    rows++;
    if ((uint64_t)rows_of(c, id) != rows)
        die("the new leader reads %d rows, not %" PRIu64, rows_of(c, id), rows);
    tsdbc_close(c);
    start(L);
    await_isr(B, "fk", 3, 60000, "the old leader back");
    stop_all();
    uint64_t wantr[REP_NAMES] = {rows, 0, 0, 0};
    rep_same(wantr, "after a clean stop");
    printf("  the promoted replica had kept %d WAL segments past its floor; the member away caught up from them and "
           "every server holds the same %" PRIu64 " rows\n", nb, rows);
}

static long long file_len(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 ? (long long)st.st_size : -1;
}

/* classfailover (D17): at the default caps (repl_retain_s 1800) a class switch on the new leader after a failover waits
 * only until every member acknowledges in the new epoch and has its old segments in parts and named in RSTATE `kept`:
 * a few seconds. Before D17 a replica's replay start was its log start, which the keep caps hold up to repl_retain_s
 * back, and the switch waited (CLS_DEFER) that long. The replicas' flushed segments stay (the caps), and every member
 * holds the same rows at the end. Mutant replaystartlog (the replay start is the log start again). */
static void scen_classfailover(void)
{
    scenario("classfailover");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "cf", 3, &num)) != 0 || num != 1)
        die("volume add cf: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "cf", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "cf", &v))
        die("no record of cf");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of cf is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "cf", 30000);
    await_isr(L, "cf", 3, 30000, "every replica up");
    int64_t t = 1000;
    uint64_t rows = 0;
    for (int k = 0; k < 4; k++) { /* flushed segments on every member before the failover */
        int64_t tv[100];
        float vv[100];
        for (int j = 0; j < 100; j++)
            tv[j] = t++, vv[j] = (float)j;
        if ((rc = tsdbc_insert_series(c, id, tv, vv, 100)) != 0)
            die("a write before the failover: %s", ers(rc));
        rows += 100;
        if ((rc = tsdbc_flush(c)) != 0)
            die("flush: %s", ers(rc));
    }
    /* a slow fresh series: its first flush (flush_interval_ms 5 s) gives it a long class (CLASSES on every member) */
    tsdb_sid slow = rep_series(c, rep_names[3], "cf", 30000);
    int64_t ts[200];
    float val[200];
    for (int j = 0; j < 200; j++)
        ts[j] = 3600000LL * j, val[j] = (float)j;
    if ((rc = tsdbc_insert_series(c, slow, ts, val, 200)) != 0)
        die("insert of the slow series: %s", ers(rc));
    uint64_t srows = 200;
    tsdbc_close(c);
    char cls[PATH_MAX + 120];
    for (int i = 0; i < 3; i++) {
        snprintf(cls, sizeof cls, "%s/n%d/v1/CLASSES", base, i);
        uint64_t tc = now_ms();
        while (file_len(cls) <= 0) {
            if (now_ms() - tc > 30000)
                die("the slow series got no class of its own on server %d (no %s)", i, cls);
            usleep(100000);
        }
    }
    struct timespec s1 = {1, 0};
    nanosleep(&s1, NULL);
    stop(L, SIGKILL);
    int B = -1;
    uint64_t tk = now_ms();
    for (;;) {
        for (int i = 0; i < 3 && B < 0; i++)
            if (i != L && vol_rec(i, "cf", &v) && v.epoch > e0 && server_of(v.leader) == i)
                B = i;
        if (B >= 0)
            break;
        if (now_ms() - tk > 30000)
            die("no server was promoted 30 s after the leader was killed");
        usleep(20000);
    }
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(B, lead, 1, 30000);
    start(L);
    await_isr(B, "cf", 3, 60000, "the old leader back");
    uint64_t t_isr = now_ms(); /* every member acknowledges in the new epoch from here */
    snprintf(cls, sizeof cls, "%s/n%d/v1/CLASSES", base, B);
    long long len0 = file_len(cls);
    c = client(B);
    tsdbc_set_retry(c, 10000);
    /* the slow series gets much faster: the next flush moves it to class 0, a switch whose T must lie past every row a
     * member may replay, which waits for the replay floor (CLS_DEFER) and is measured again at each flush */
    int64_t st = 3600000LL * 200;
    while (file_len(cls) <= len0) {
        if (now_ms() - t_isr > 60000)
            die("a class switch on the new leader waited more than 60 s after every member was back in its ISR (the "
                "replay floor stayed below the epoch: %s %lld bytes)", cls, file_len(cls));
        for (int j = 0; j < 200; j++)
            ts[j] = st + j, val[j] = (float)j;
        if ((rc = tsdbc_insert_series(c, slow, ts, val, 200)) != 0)
            die("a write to the new leader: %s", ers(rc));
        st += 200;
        srows += 200;
        usleep(500000);
    }
    uint64_t waited = now_ms() - t_isr;
    if ((uint64_t)rows_of(c, id) != rows || (uint64_t)rows_of(c, slow) != srows)
        die("the new leader reads %d and %d rows, not %" PRIu64 " and %" PRIu64, rows_of(c, id), rows_of(c, slow), rows,
            srows);
    tsdbc_close(c);
    for (int i = 0; i < 3; i++) /* (the leader lets go of what every replica holds: eng_leader_keep) */
        if (i != B && wal_count(i, 1) < 2)
            die("replica %d keeps %d WAL segments of cf: the flushed ones went before the caps (repl_retain_s 1800)", i,
                wal_count(i, 1));
    stop_all();
    uint64_t wantr[REP_NAMES] = {rows, 0, 0, srows};
    rep_same(wantr, "after a clean stop");
    printf("  after a failover at the default caps a class switch waited %" PRIu64 " ms after every member was back in "
           "the ISR; the replicas keep their flushed segments and every server holds the same rows\n", waited);
}

/* D13: an engine failure (EIO on a WAL fdatasync, crashfs) of one replica of a cluster volume stops that volume
 * there, quiesced and reported LOCAL_FAULT in the server's heartbeat; the server itself goes on (its root answers)
 * and exits cleanly on SIGTERM. Without D13 tsdbd exits with status 1 on any failed volume outside a domain. */
static void scen_replfail(void)
{
    scenario("replfail");
    /* the ASan runtime must come first in the library list: with crashfs preloaded the server refuses to start */
#if defined(__SANITIZE_ADDRESS__)
    printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
    return;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    printf("  skipped: crashfs cannot be preloaded into an AddressSanitizer build\n");
    return;
#endif
#endif
    const char *so = getenv("CRASHFS_SO") ? getenv("CRASHFS_SO") : "./build/libcrashfs.so";
    static char pe[3][PATH_MAX * 3 + 256];
    char mark[3][PATH_MAX + 80];
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80];
        server_dir(i, dir, sizeof dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST)
            die("mkdir %s: %s", dir, strerror(errno));
        snprintf(mark[i], sizeof mark[i], "%s/eio-n%d", base, i);
        snprintf(pe[i], sizeof pe[i],
                 "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_MODE=eio CRASHFS_EIO_ONLY=wal_ CRASHFS_EIO_ON=fdatasync "
                 "CRASHFS_MARK=%s",
                 so, dir, mark[i]);
        penv[i] = pe[i];
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    tsdbc_close(c);
    int R = (L + 1) % 3;
    signal_child(pids[R], SIGUSR2); /* crashfs: the next fdatasync of a wal_ file there fails with EIO */
    struct rep_writer w = {cports[L], id, 2000, 1000000, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    uint64_t t0 = now_ms();
    while (access(mark[R], F_OK) != 0) {
        if (now_ms() - t0 > 30000)
            die("no fdatasync of server %d's log failed in 30 s", R);
        usleep(20000);
    }
    if (await_exit(R, 2000) >= 0)
        die("server %d exited on its cluster volume's engine failure (D13: only the volume stops)", R);
    /* the heartbeat of R reports the volume LOCAL_FAULT to the metadata leader */
    int fault = 0;
    for (t0 = now_ms(); !fault && now_ms() - t0 < 10000; usleep(100000))
        for (int i = 0; i < 3 && !fault; i++) {
            struct view vw;
            if (i == R)
                continue;
            view(i, &vw);
            for (uint32_t k = 0; k < vw.st.nheard; k++)
                if (vw.st.heard[k].id == ids[R])
                    for (uint32_t j = 0; j < vw.st.heard[k].nvols; j++)
                        if (vw.st.heard[k].vols[j].v.volume == num &&
                            vw.st.heard[k].vols[j].v.health == CL_HEALTH_LOCAL_FAULT)
                            fault = 1;
        }
    if (!fault)
        die("server %d's heartbeat never reported volume %" PRIu32 " LOCAL_FAULT", R, num);
    tsdbc *cr = client(R); /* the root of R still answers */
    tsdb_sid any;
    if ((rc = tsdbc_series(cr, "replfail.none", 0, &any)) != TSDB_ERR_NOTFOUND)
        die("a lookup in server %d's root after its volume failed: %s", R, ers(rc));
    tsdbc_close(cr);
    stop(R, SIGTERM); /* a clean stop: the failed volume is no failure of the server */
    stop(L, SIGTERM);
    pthread_join(th, NULL);
    stop_all();
    printf("  a replica whose engine failed stops that volume only, reports it LOCAL_FAULT and stops cleanly\n");
}

/* crashfs is preloaded into these scenarios' servers: 0 when it can be, else why not (printed as the skip) */
static const char *crashfs_skip(const char **so)
{
    *so = getenv("CRASHFS_SO") ? getenv("CRASHFS_SO") : "./build/libcrashfs.so";
#if defined(__SANITIZE_ADDRESS__)
    return "crashfs cannot be preloaded into an AddressSanitizer build";
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    return "crashfs cannot be preloaded into an AddressSanitizer build";
#endif
#endif
    if (access(*so, F_OK) != 0) {
#ifdef __linux__
        die("no %s (make build/libcrashfs.so): the scenario cannot run, and must not pass as skipped", *so);
#else
        return "no libcrashfs.so";
#endif
    }
    return NULL;
}

/* the heartbeat of server R reports volume num LOCAL_FAULT to the others within 10 s */
static void await_local_fault(int R, uint32_t num, int nserv)
{
    for (uint64_t t0 = now_ms(); now_ms() - t0 < 10000; usleep(100000))
        for (int i = 0; i < nserv; i++) {
            struct view vw;
            if (i == R || pids[i] <= 0)
                continue;
            view(i, &vw);
            int fault = 0;
            for (uint32_t k = 0; k < vw.st.nheard; k++)
                if (vw.st.heard[k].id == ids[R])
                    for (uint32_t j = 0; j < vw.st.heard[k].nvols; j++)
                        fault |= vw.st.heard[k].vols[j].v.volume == num &&
                                 vw.st.heard[k].vols[j].v.health == CL_HEALTH_LOCAL_FAULT;
            view_free(&vw);
            if (fault)
                return;
        }
    die("server %d's heartbeat never reported volume %" PRIu32 " LOCAL_FAULT", R, num);
}

/* ackdurable (M6, design 5.1: a write is committed once it is durable on every member of the ISR): writes are
 * acknowledged, then the power fails (crashfs: what was never synced is gone) on both replicas at their next
 * append and then on the leader. Each replica, restarted with the leader still down (so it cannot catch up from the
 * leader) and stopped cleanly, holds every acknowledged row, read in-process and read-only. Two witnesses keep the
 * metadata group's quorum while the data nodes are down, and learn the restarted replicas' new peer ports.
 * TSDB_REPLD_MUTANT=ackbeforesync (a replica reports its written end as durable without a sync) fails it. */
static void scen_ackdurable(void)
{
    scenario("ackdurable");
    const char *so, *skip = crashfs_skip(&so);
    if (skip) {
        printf("  skipped: %s\n", skip);
        return;
    }
    static char pe[3][PATH_MAX * 2 + 128];
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80];
        server_dir(i, dir, sizeof dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST)
            die("mkdir %s: %s", dir, strerror(errno));
        snprintf(pe[i], sizeof pe[i], "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_REWRITE=old", so, dir);
        penv[i] = pe[i];
    }
    for (int i = 3; i < 5; i++) {
        is_witness[i] = 1;
        xargs[i] = "--peer-port 0"; /* a witness takes the peer port 7071 unless told */
        char wdir[PATH_MAX + 80];
        server_dir(i, wdir, sizeof wdir); /* no --dir makes it; the state directory goes inside */
        if (mkdir(wdir, 0755) != 0)
            die("mkdir %s: %s", wdir, strerror(errno));
    }
    for (int i = 0; i < 5; i++)
        start(i);
    init_cluster(0, 600);
    for (int i = 1; i < 5; i++)
        node_add(0, i);
    unsigned want = 5;
    await_view(0, p_voters, &want, "five voters, two of them witnesses");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    enum { ACKED = 40 };
    for (int j = 0; j < ACKED; j++) {
        int64_t ts = 1000 + 1000LL * j;
        float val = (float)j;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d with every replica up: %s", j, ers(rc));
    }
    tsdbc_close(c);
    /* at once (a replica's flush would make the rows durable in a part): the power fails at each replica's next
     * tracked operation, the append of the next write, which therefore is never acknowledged */
    int R1 = (L + 1) % 3, R2 = (L + 2) % 3;
    signal_child(pids[R1], SIGUSR2);
    signal_child(pids[R2], SIGUSR2);
    struct rep_writer w1 = {cports[L], id, 1000 + 1000LL * ACKED, 1, 0, 0, 0, 0};
    pthread_t th1, th2;
    if (pthread_create(&th1, NULL, rep_write_thread, &w1) != 0)
        die("pthread_create");
    for (int r = 0; r < 2; r++) {
        int R = r ? R2 : R1, st = await_exit(R, 15000);
        if (st != 99)
            die("server %d ended with status %d after the power loss, expected 99 (crashfs)", R, st);
    }
    signal_child(pids[L], SIGUSR2); /* and the leader's, at its next append */
    struct rep_writer w2 = {cports[L], id, 1000 + 1000LL * (ACKED + 1), 1, 0, 0, 0, 0};
    if (pthread_create(&th2, NULL, rep_write_thread, &w2) != 0)
        die("pthread_create");
    int st = await_exit(L, 15000);
    if (st != 99)
        die("server %d (the leader) ended with status %d after the power loss, expected 99 (crashfs)", L, st);
    pthread_join(th1, NULL);
    pthread_join(th2, NULL);
    if (w1.rc == 0 || w2.rc == 0)
        die("a write was acknowledged with both replicas down");
    /* one at a time: a restarted server has a new peer port, which it tells the group (the witnesses) */
    int R[2] = {R1, R2};
    for (int r = 0; r < 2; r++) {
        start(R[r]);
        await_open_gen(3, "plant", R[r], 2, 0);
    }
    for (int r = 0; r < 2; r++)
        stop(R[r], SIGTERM);
    for (int r = 0; r < 2; r++) {
        char dir[PATH_MAX + 80], vd[PATH_MAX + 120];
        server_dir(R[r], dir, sizeof dir);
        snprintf(vd, sizeof vd, "%s/v1", dir);
        uint64_t rows, h;
        if ((rc = cvol_digest(vd, rep_names, 1, &rows, &h)) != 0)
            die("open %s read-only: %s", vd, ers(rc));
        if (rows < ACKED || rows > ACKED + 2)
            die("after a power loss replica %d holds %" PRIu64 " rows, %d were acknowledged (at most 2 more were sent)",
                R[r], rows, ACKED);
    }
    stop_all();
    printf("  %d acknowledged writes survive a power loss of both replicas and then of the leader, on each replica\n",
           ACKED);
}

/* a write on its own connection; afterwards `after` is what a lookup in the root on that same connection gave */
struct wf_writer {
    tsdbc *c;
    tsdb_sid id;
    int64_t ts;
    int rc, after, done;
};
static void *wf_write_thread(void *a)
{
    struct wf_writer *w = a;
    int64_t ts = w->ts;
    float val = 5;
    w->rc = tsdbc_insert_series(w->c, w->id, &ts, &val, 1);
    tsdb_sid any;
    w->after = tsdbc_series(w->c, "writerfail.none", 0, &any);
    __atomic_store_n(&w->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

/* writerfail (M5, D13 on the writer): the leader's engine fails (crashfs: EIO on its next fdatasync of a wal_ file:
 * a second write's) while a write waits for a paused replica. The driver deposes the writer: the outcome of both
 * writes is unknown (each went to the replicas), so their connections close without an answer (D12); the volume is
 * quiesced (UNAVAILABLE), the group promotes a replica at e+1 and writes and reads on the old leader are MOVED to it; the
 * server reports the volume LOCAL_FAULT, answers in its root, keeps its vote and its other
 * volumes (a later volume add commits and opens there) and stops cleanly. writerfail runs on strict reads (D5, the
 * production path), where routing refuses the failed volume's reads before its engine is asked, so the quiesce
 * itself is observed by writerfailq: the same run under TSDB_REPLICA_READS=1, whose routing lets a read of the failed
 * volume through to its engine (a replica's FENCED with a lease), so only the quiesce answers it UNAVAILABLE (then
 * MOVED by frame_put). TSDB_REPLD_MUTANT=nodepose fails both, noquiesce writerfailq. */
/* waits up to 30 s for server `via`'s copy of volume `name` to name a leader other than node `old` at an epoch above
 * e0 (a promotion, design-cluster.md 6.2); returns its server */
static int await_promoted(int via, const char *name, uint32_t old, uint64_t e0, struct meta_volume *v)
{
    for (uint64_t t0 = now_ms();; usleep(20000)) {
        if (vol_rec(via, name, v) && v->leader != old && v->epoch > e0)
            return server_of(v->leader);
        if (now_ms() - t0 > 30000)
            die("no new leader of %s 30 s after its leader %u failed it (epoch %" PRIu64 ", leader %u)", name, old,
                v->epoch, v->leader);
    }
}

static void writerfail_run(const char *name, int strict)
{
    scenario(name);
    const char *so, *skip = crashfs_skip(&so);
    if (skip) {
        printf("  skipped: %s\n", skip);
        return;
    }
    strict_reads = strict; /* 1: every read on the leader (D5), as in production; 0: the engine's own refusal */
    static char pe[3][PATH_MAX * 3 + 256];
    static char mark[3][PATH_MAX + 80];
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80];
        server_dir(i, dir, sizeof dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST)
            die("mkdir %s: %s", dir, strerror(errno));
        snprintf(mark[i], sizeof mark[i], "%s/eio-n%d", base, i);
        snprintf(pe[i], sizeof pe[i],
                 "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_MODE=eio CRASHFS_EIO_ONLY=wal_ CRASHFS_EIO_ON=fdatasync "
                 "CRASHFS_MARK=%s",
                 so, dir, mark[i]);
        penv[i] = pe[i];
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    int R1 = (L + 1) % 3;
    pause_server(R1, 1); /* the commit point stops below the next write (the other replica keeps the lease) */
    struct wf_writer w[2] = {{client(L), id, 5000, 0, 0, 0}, {client(L), id, 6000, 0, 0, 0}};
    pthread_t th[2];
    if (pthread_create(&th[0], NULL, wf_write_thread, &w[0]) != 0)
        die("pthread_create");
    struct timespec ms300 = {0, 300000000};
    nanosleep(&ms300, NULL);
    if (__atomic_load_n(&w[0].done, __ATOMIC_ACQUIRE))
        die("a write with a replica paused was answered: %s", ers(w[0].rc));
    signal_child(pids[L], SIGUSR2); /* crashfs: the leader's next fdatasync of a wal_ file (w[1]'s) fails with EIO */
    if (pthread_create(&th[1], NULL, wf_write_thread, &w[1]) != 0)
        die("pthread_create");
    uint64_t t0 = now_ms();
    while (access(mark[L], F_OK) != 0) {
        if (now_ms() - t0 > 30000)
            die("no fdatasync of server %d's log failed in 30 s", L);
        usleep(20000);
    }
    static const char *const which[2] = {"waiting for the commit point", "whose sync failed"};
    for (int k = 0; k < 2; k++) {
        for (t0 = now_ms(); !__atomic_load_n(&w[k].done, __ATOMIC_ACQUIRE); usleep(20000))
            if (now_ms() - t0 > 10000)
                die("the write %s on the failed writer was not ended in 10 s (the writer was not deposed)", which[k]);
        pthread_join(th[k], NULL);
        if (w[k].rc != TSDB_ERR_IO || w[k].after != TSDB_ERR_IO)
            die("the write %s on the failed writer got %d (%s), then its connection %d (%s): not a closed connection",
                which[k], w[k].rc, ers(w[k].rc), w[k].after, ers(w[k].after));
        tsdbc_close(w[k].c);
    }
    if (await_exit(L, 1000) >= 0)
        die("server %d exited on its cluster volume's engine failure (D13: only the volume stops)", L);
    pause_server(R1, 0);
    /* the failed volume: the group promotes a replica (reason FENCED, D13), writes and reads on L are MOVED to it
     * (reads too, D5: the failed volume answers nothing); the root answers */
    uint64_t e0 = v.epoch;
    int Nw = await_promoted((L + 1) % 3, "plant", ids[L], e0, &v);
    if (Nw < 0 || Nw == L || v.epoch != e0 + 1)
        die("the new record of plant: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    tsdbc *c2 = client(L);
    ts = 7000;
    insert_expect_moved(c2, id, ts, val, 1, v.leader, v.epoch, "a write to the failed volume");
    tsdb_points p;
    rc = tsdbc_query_points(c2, id, INT64_MIN, INT64_MAX, &p); /* D5: reads go to the leader as well; writerfailq: the
                                                                  * quiesced engine's UNAVAILABLE, MOVED by frame_put */
    expect_moved_ep(c2, rc, 1, v.leader, v.epoch, "a read of the failed volume");
    tsdb_sid any;
    if ((rc = tsdbc_series(c2, "writerfail.none", 0, &any)) != TSDB_ERR_NOTFOUND)
        die("a lookup in server %d's root after its volume failed: %s", L, ers(rc));
    tsdbc_close(c2);
    tsdbc_close(c);
    await_local_fault(L, num, 3);
    /* its vote and its other volumes: a volume added now commits (L is one of three voters) and opens on L too */
    struct view vw;
    view(L, &vw);
    int voter = is_voter(&vw, ids[L]);
    view_free(&vw);
    if (!voter)
        die("server %d is no voter after its volume failed", L);
    if ((rc = vol_add((L + 2) % 3, "second", 3, &num)) != 0 || num != 2)
        die("volume add second: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen((L + 2) % 3, "second", i, 1, 0);
    /* the replicas are given no position past the failure: R1 has been resumed for a while (anything the driver still
     * shipped would have reached it), more writes were refused meanwhile, and each replica holds the acknowledged row
     * and at most the two whose outcome is unknown (5000, 6000), never a refused one */
    for (int k = 0; k < 5; k++) {
        tsdbc *c3 = client(L);
        ts = 8000 + k;
        insert_expect_moved(c3, id, ts, val, 1, v.leader, v.epoch, "a later write to the failed volume");
        tsdbc_close(c3);
        usleep(200000);
    }
    for (int i = 0; i < 3; i++)
        stop(i, SIGTERM); /* a clean stop: the failed volume is no failure of the server */
    for (int r = 0; r < 3; r++) {
        if (r == L)
            continue;
        char dir[PATH_MAX + 80], vd[PATH_MAX + 120];
        server_dir(r, dir, sizeof dir);
        snprintf(vd, sizeof vd, "%s/v1", dir);
        uint64_t rows, h;
        if ((rc = cvol_digest(vd, rep_names, 1, &rows, &h)) != 0)
            die("open %s read-only: %s", vd, ers(rc));
        if (rows < 1 || rows > 3)
            die("replica %d holds %" PRIu64 " rows: 1 was acknowledged, 3 written before the volume failed", r, rows);
    }
    stop_all();
    printf("  a leader whose engine failed closes the connections of the writes in flight, a replica leads at epoch "
           "%" PRIu64 ", the old leader answers writes and reads MOVED, keeps its root, its vote and its "
           "other volumes and stops cleanly; the replicas hold nothing "
           "written after the failure\n",
           v.epoch);
}

/* GET /metrics from a metrics port (--metrics-port auto): the reply in buf, which it NUL-terminates */
static void metrics_get(int port, char *buf, size_t n)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (fd < 0 || port <= 0 || connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0)
        die("connect to the metrics port %d: %s", port, strerror(errno));
    struct timeval tv = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    static const char req[] = "GET /metrics HTTP/1.0\r\n\r\n";
    if (write(fd, req, sizeof req - 1) != (ssize_t)sizeof req - 1)
        die("write to the metrics port: %s", strerror(errno));
    size_t got = 0;
    for (ssize_t k; got < n - 1 && (k = read(fd, buf + got, n - 1 - got)) > 0;)
        got += (size_t)k;
    buf[got] = 0;
    close(fd);
}

/* tsdb_volume_diverged of volume num on server i's metrics port is `want` (0 or 1) */
static void metrics_diverged(int i, int port, uint32_t num, int want, const char *what)
{
    static char buf[1 << 20];
    metrics_get(port, buf, sizeof buf);
    char key[64];
    snprintf(key, sizeof key, "\ntsdb_volume_diverged{volume=\"%" PRIu32 "\",path=\"", num);
    const char *l = strstr(buf, key);
    const char *e = l ? strstr(l + 1, "\"} ") : NULL;
    if (!e || atoi(e + 3) != want)
        die("%s: server %d's metrics say tsdb_volume_diverged of volume %" PRIu32 " is %s, not %d", what, i, num,
            e ? e + 3 : "missing", want);
}

/* STATUS of server i says vol.<num>.health=unavailable and vol.<num>.reason=diverged within ms */
static void await_status_diverged(int i, uint32_t num, int ms, const char *what)
{
    char line[96], *text = NULL;
    snprintf(line, sizeof line, "\nvol.%" PRIu32 ".health=unavailable\nvol.%" PRIu32 ".reason=diverged\n", num, num);
    for (uint64_t t0 = now_ms();; usleep(100000)) {
        tsdbc *c = client(i);
        int rc = tsdbc_status(c, &text);
        tsdbc_close(c);
        if (rc == 0 && strstr(text, line)) {
            free(text);
            return;
        }
        if (now_ms() - t0 > (uint64_t)ms)
            die("%s: STATUS of server %d does not say volume %" PRIu32 " is diverged after %d ms (%s):\n%s", what, i,
                num, ms, ers(rc), rc == 0 ? text : "");
        free(text);
        text = NULL;
    }
}

static void touch_plant(int i, uint32_t num)
{
    char f[PATH_MAX + 120];
    server_dir(i, f, sizeof f);
    snprintf(f + strlen(f), sizeof f - strlen(f), "/diverge.%" PRIu32, num);
    FILE *fp = fopen(f, "w");
    if (!fp)
        die("create %s: %s", f, strerror(errno));
    fclose(fp);
}

/* diverged (r8i, design-cluster-data.md D8, D13, D23): a volume whose DIVERGED file says a settled copy failed its
 * digest (planted by TSDB_REPLD_TEST=plantdiverge, the same ec_diverge a settle calls) stops like a failed one. On a
 * replica: LOCAL_FAULT in its heartbeat, out of the ISR, STATUS and /metrics name it DIVERGED, the writes go on at the
 * leader. On a leader: deposed, another member leads, a client is MOVED there and writes there. A restart opens the
 * volume DIVERGED again (the file), and `tsdb DIR volume diverged --clear` refuses a cluster volume naming R4.
 * TSDB_REPLD_MUTANT=divergeisr (the driver ignores DIVERGED) fails it. */
static void scen_diverged(void)
{
    scenario("diverged");
    static int mports[3];
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=plantdiverge";
        xargs[i] = "--metrics-port auto";
        start(i);
        mports[i] = th_mport;
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num, num2;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    if ((rc = vol_add(0, "lead", 3, &num2)) != 0 || num2 != 2)
        die("volume add lead: %s, number %" PRIu32, cluster_strerror(rc), num2);
    for (int i = 0; i < 3; i++) {
        await_open_gen(0, "plant", i, 1, 0);
        await_open_gen(0, "lead", i, 1, 0);
    }
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    await_isr(L, "plant", 3, 15000, "every replica of plant up");
    metrics_diverged(L, mports[L], num, 0, "before any plant");

    /* a replica */
    int R = (L + 1) % 3;
    touch_plant(R, num);
    await_local_fault(R, num, 3);
    await_isr(L, "plant", 2, 15000, "a DIVERGED replica leaves the ISR");
    await_status_diverged(R, num, 5000, "a DIVERGED replica");
    metrics_diverged(R, mports[R], num, 1, "a DIVERGED replica");
    metrics_diverged(R, mports[R], num2, 0, "the other volume of a DIVERGED replica's server");
    for (int k = 0; k < 5; k++) {
        ts = 2000 + k;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("a write after a replica diverged: %s", ers(rc));
    }
    tsdbc_close(c);
    if (await_exit(R, 500) >= 0)
        die("server %d exited on its volume's DIVERGED (D13: only the volume stops)", R);

    /* a leader */
    struct meta_volume v2;
    if (!vol_rec(0, "lead", &v2))
        die("no record of lead");
    int L2 = server_of(v2.leader);
    if (L2 < 0 || L2 > 2)
        die("the leader of lead is node %u", v2.leader);
    c = client(L2);
    tsdb_sid id2 = rep_series(c, rep_names[1], "lead", 30000);
    ts = 1000;
    if ((rc = tsdbc_insert_series(c, id2, &ts, &val, 1)) != 0)
        die("a write to lead: %s", ers(rc));
    await_isr(L2, "lead", 3, 15000, "every replica of lead up");
    uint64_t e0 = v2.epoch;
    touch_plant(L2, num2);
    int N2 = await_promoted((L2 + 1) % 3, "lead", ids[L2], e0, &v2);
    if (N2 < 0 || N2 == L2)
        die("the new record of lead: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v2.epoch, e0, v2.leader);
    await_status_diverged(L2, num2, 5000, "a DIVERGED leader");
    metrics_diverged(L2, mports[L2], num2, 1, "a DIVERGED leader");
    ts = 3000;
    insert_expect_moved(c, id2, ts, val, 2, v2.leader, v2.epoch, "a write to the DIVERGED leader");
    tsdbc_close(c);
    c = client(N2);
    for (uint64_t t0 = now_ms(); (rc = tsdbc_insert_series(c, id2, &ts, &val, 1)) != 0; usleep(100000))
        if (now_ms() - t0 > 15000)
            die("a write to lead's new leader (server %d): %s", N2, ers(rc));
    tsdbc_close(c);
    await_local_fault(L2, num2, 3);

    /* a restart opens them DIVERGED again: the file, not memory */
    int again[2] = {R, L2};
    for (int k = 0; k < 2; k++) {
        int i = again[k];
        if (k == 1 && i == R)
            break;
        stop(i, SIGTERM);
        start(i);
        mports[i] = th_mport;
        uint32_t vn = i == R ? num : num2;
        await_status_diverged(i, vn, 20000, "a DIVERGED volume after a restart");
        metrics_diverged(i, mports[i], vn, 1, "a DIVERGED volume after a restart");
        await_local_fault(i, vn, 3);
    }
    usleep(3000000);
    if ((rc = (int)isr_of(L, "plant", NULL)) != 2)
        die("the DIVERGED replica is back in the ISR of plant after its restart (%d members)", rc);

    /* clearing is the rebuild's (R4), not the CLI's */
    stop(R, SIGTERM);
    char dir[PATH_MAX + 80], args[PATH_MAX * 2 + 64], out[4096];
    server_dir(R, dir, sizeof dir);
    snprintf(args, sizeof args, "%s/v1 volume diverged", dir);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "DIVERGED"))
        die("tsdb %s lists no DIVERGED record:\n%s", args, out);
    snprintf(args, sizeof args, "%s/v1 volume diverged --clear", dir);
    if (cli_run(args, NULL, out, sizeof out) == 0 || !strstr(out, "R4"))
        die("tsdb %s on a cluster volume was not refused naming R4:\n%s", args, out);
    snprintf(args, sizeof args, "%s/v1 volume diverged", dir);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "DIVERGED"))
        die("the refused --clear removed the DIVERGED record:\n%s", out);
    stop_all();
    printf("  a DIVERGED replica leaves the ISR (LOCAL_FAULT) while writes go on, a DIVERGED leader is deposed and "
           "lead moves to server %d at epoch %" PRIu64 ", STATUS and /metrics say diverged, a restart keeps it, "
           "--clear is refused on a cluster volume\n",
           N2, v2.epoch);
}

static void scen_writerfail(void) { writerfail_run("writerfail", 1); }
static void scen_writerfailq(void) { writerfail_run("writerfailq", 0); }

/* writerdepose (M5, D13 with a healthy engine): the leader's driver fails the volume while its engine is fine
 * (crashfs: EIO on the leader's next fdatasync of its REPL file, the save of the commit floor at most once a second
 * after the floor moved), with a write waiting for a paused replica. Nothing in the engine ends that wait: only the
 * deposal does (eng_leader_depose in vol_fail), and its outcome is unknown (its records went to the replicas), so its
 * connection closes without an answer (D12). A replica is promoted at e+1; a later write on the old leader is MOVED
 * to it and never numbered; the replicas hold no row past what was sent before the failure. TSDB_REPLD_MUTANT=nodepose
 * fails it (the write waits for good). */
static void scen_writerdepose(void)
{
    scenario("writerdepose");
    const char *so, *skip = crashfs_skip(&so);
    if (skip) {
        printf("  skipped: %s\n", skip);
        return;
    }
    static char pe[3][PATH_MAX * 3 + 256];
    static char mark[3][PATH_MAX + 80];
    for (int i = 0; i < 3; i++) {
        char dir[PATH_MAX + 80];
        server_dir(i, dir, sizeof dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST)
            die("mkdir %s: %s", dir, strerror(errno));
        snprintf(mark[i], sizeof mark[i], "%s/eio-n%d", base, i);
        snprintf(pe[i], sizeof pe[i],
                 "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_MODE=eio CRASHFS_EIO_ONLY=REPL CRASHFS_EIO_ON=fdatasync "
                 "CRASHFS_MARK=%s",
                 so, dir, mark[i]);
        penv[i] = pe[i];
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    int sent = 1, acked = 1;
    /* the floor is saved at once after a commit when its last save is a second old, else a second after that save:
     * the commit of `ts` saves it now, the next commit moves it again and its save comes a second later */
    struct timespec ms1200 = {1, 200000000}, ms100 = {0, 100000000};
    nanosleep(&ms1200, NULL);
    ts += 1000;
    sent++;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    acked++;
    nanosleep(&ms100, NULL); /* that save is done (unharmed) */
    ts += 1000;
    sent++;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    acked++;
    uint64_t t_moved = now_ms();
    signal_child(pids[L], SIGUSR2); /* crashfs: the leader's next fdatasync of REPL (that save) fails with EIO */
    int R1 = (L + 1) % 3;
    pause_server(R1, 1); /* the commit point stops below the next write */
    struct wf_writer w = {client(L), id, ts + 1000, 0, 0, 0};
    sent++;
    pthread_t th;
    if (pthread_create(&th, NULL, wf_write_thread, &w) != 0)
        die("pthread_create");
    while (access(mark[L], F_OK) != 0) {
        if (now_ms() - t_moved > 30000)
            die("no fdatasync of server %d's REPL failed in 30 s", L);
        usleep(20000);
    }
    for (uint64_t t0 = now_ms(); !__atomic_load_n(&w.done, __ATOMIC_ACQUIRE); usleep(20000))
        if (now_ms() - t0 > 10000)
            die("the write waiting for the commit point on the failed writer was not ended in 10 s (the writer was "
                "not deposed)");
    pthread_join(th, NULL);
    if (w.rc != TSDB_ERR_IO || w.after != TSDB_ERR_IO)
        die("the write waiting for the commit point on the failed writer got %d (%s), then its connection %d (%s): "
            "not a closed connection (UNAVAILABLE: the save failed before the write was numbered)",
            w.rc, ers(w.rc), w.after, ers(w.after));
    tsdbc_close(w.c);
    tsdbc_close(c);
    pause_server(R1, 0);
    await_local_fault(L, num, 3);
    /* a write after the failure: never numbered; once a replica leads (D13 promotes away from L) it is MOVED there */
    uint64_t e0 = v.epoch;
    int Nw = await_promoted((L + 1) % 3, "plant", ids[L], e0, &v);
    if (Nw < 0 || Nw == L || v.epoch != e0 + 1)
        die("the new record of plant: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    tsdbc *c2 = client(L);
    ts += 100000;
    insert_expect_moved(c2, id, ts, val, 1, v.leader, v.epoch, "a write to the failed volume");
    tsdb_sid any;
    if ((rc = tsdbc_series(c2, "writerfail.none", 0, &any)) != TSDB_ERR_NOTFOUND)
        die("a lookup in server %d's root after its volume failed: %s", L, ers(rc));
    tsdbc_close(c2);
    if (await_exit(L, 500) >= 0)
        die("server %d exited on its cluster volume's failure (D13: only the volume stops)", L);
    for (int i = 0; i < 3; i++)
        stop(i, SIGTERM);
    for (int r = 0; r < 3; r++) {
        if (r == L)
            continue;
        char dir[PATH_MAX + 80], vd[PATH_MAX + 120];
        server_dir(r, dir, sizeof dir);
        snprintf(vd, sizeof vd, "%s/v1", dir);
        uint64_t rows, h;
        if ((rc = cvol_digest(vd, rep_names, 1, &rows, &h)) != 0)
            die("open %s read-only: %s", vd, ers(rc));
        if (rows < (uint64_t)acked || rows > (uint64_t)sent)
            die("replica %d holds %" PRIu64 " rows: %d were acknowledged, %d sent before the volume failed", r, rows,
                acked, sent);
    }
    stop_all();
    printf("  a leader whose driver failed the volume (its engine healthy) closes the connection of the write waiting "
           "for its commit point and sends the next to the promoted replica (MOVED); the replicas hold between %d and "
           "%d rows\n",
           acked, sent);
}

/* ------------------------------------------------------------ leases (§7.1) and the serving loop's stamp (§6.1) */

/* a read on c by `kind` % 3: QUERY_RAW, QUERY_LATEST or QUERY_MULTI (the two ids paths of the lease check) */
struct lread {
    uint64_t sent;
    int rc;
};
static int lease_read(tsdbc *c, tsdb_sid id, int kind, int *rows)
{
    if (kind % 3 == 2) {
        tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = 0, .end = INT64_MAX / 2};
        tsdb_series_result *r = NULL;
        int rc = tsdbc_query_multi_collect(c, &q, &id, 1, NULL, &r);
        if (rc == 0 && r[0].status != 0)
            rc = r[0].status;
        *rows = rc == 0 ? (int)r[0].n : 0;
        if (r)
            tsdbc_series_results_free(r, 1);
        return rc;
    }
    if (kind % 3 == 1) {
        tsdb_latest_result r;
        int rc = tsdbc_latest(c, &id, 1, NULL, &r);
        if (rc == 0 && r.status != 0)
            rc = r.status;
        *rows = rc == 0 && r.kind == TSDB_LATEST_VALUE;
        return rc;
    }
    nrows = 0;
    int rc = tsdbc_query_raw(c, id, 0, INT64_MAX / 2, count_rows, NULL);
    *rows = nrows;
    return rc;
}

/* reads on c until one succeeds (or `ms` passed): the time it was sent, or 0 */
static uint64_t lease_until_ok(tsdbc *c, tsdb_sid id, int ms, int want_rows)
{
    uint64_t t0 = now_ms();
    for (int k = 0; now_ms() - t0 < (uint64_t)ms; k++) {
        uint64_t t = now_ms();
        int rows, rc = lease_read(c, id, k % 3, &rows);
        if (rc == 0) {
            if (rows != want_rows)
                die("a read answered %d rows, %d were written", rows, want_rows);
            return t;
        }
        if (rc != TSDB_ERR_UNAVAILABLE)
            die("a read: %s", ers(rc));
        struct timespec d = {0, 2000000};
        nanosleep(&d, NULL);
    }
    return 0;
}

/* The volume leader X (not the Raft leader R) holds a lease the Raft leader's HB_ACKs extend: send time + lease_ms *
 * (1 - drift). R paused at T0: X's last grant was for a heartbeat sent before T0, so it answers nothing sent after
 * T0 + 800 ms (lease_ms 1000, drift 20%), and no new Raft leader can grant before an election timeout (1 s at least)
 * has passed: every read sent in (T0 + 810, T0 + 940) must be refused, and some must have been sent. Mutants:
 * leaseanyvoter (the third server's acks keep the lease), nodrift (it runs to T0 + 1000). Then X itself paused past
 * lease and node_timeout, the others paused, X resumed: nobody renews, it answers nothing; all resumed: the row
 * written before is read back. */
struct stalled_read {
    int server;
    tsdb_sid id;
    int rc;
};
static void *stalled_read_thread(void *a)
{
    struct stalled_read *sr = a;
    tsdbc *c = client(sr->server);
    tsdbc_set_retry(c, 0); /* the first answer, not a retried one */
    int rows;
    sr->rc = lease_read(c, sr->id, 0, &rows);
    tsdbc_close(c);
    return NULL;
}

static void scen_lease(void)
{
    scenario("lease");
    static char pe[3][PATH_MAX + 80];
    char stall[3][PATH_MAX + 64];
    for (int i = 0; i < 3; i++) {
        snprintf(stall[i], sizeof stall[i], "%s/rstall-n%d", base, i);
        unlink(stall[i]);
        snprintf(pe[i], sizeof pe[i], "TSDBD_TEST_READ_STALL=%s", stall[i]);
        penv[i] = pe[i];
        start(i);
    }
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 3000;
    s.heartbeat_ms = 50;
    s.lease_ms = 1000;
    s.drift_ppm = 200000;
    s.voter_replace_s = 600;
    uint8_t req[128], *reply;
    size_t len;
    size_t rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req);
    int rc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != 0)
        die("init: %s", cluster_strerror(rc));
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    int R = 0, X = -1;
    char name[16];
    struct meta_volume v;
    for (int k = 0; k < 8 && X < 0; k++) {
        uint32_t num;
        snprintf(name, sizeof name, "lease%d", k);
        if ((rc = vol_add(0, name, 3, &num)) != 0)
            die("volume add %s: %s", name, cluster_strerror(rc));
        if (!vol_rec(0, name, &v))
            die("no record of %s", name);
        int L = server_of(v.leader);
        if (L >= 0 && L <= 2 && L != R)
            X = L;
    }
    if (X < 0)
        die("eight volumes, every one led by the Raft leader");
    int F = 3 - R - X;
    printf("  Raft leader %d, %s led by %d, third server %d\n", R, name, X, F);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, name, i, 1, 0);
    tsdbc *c = client(X);
    tsdb_sid id = rep_series(c, rep_names[0], name, 30000);
    int64_t ts = 1000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write: %s", ers(rc));
    if (!lease_until_ok(c, id, 10000, 1))
        die("no read answered on %d before the pause", X);

    /* a: the Raft leader paused */
    static struct lread log[1 << 16];
    size_t nl = 0;
    pause_server(R, 1);
    uint64_t T0 = now_ms();
    while (now_ms() - T0 < 3000 && nl < sizeof log / sizeof log[0]) {
        int rows;
        log[nl].sent = now_ms();
        log[nl].rc = lease_read(c, id, (int)(nl % 3), &rows);
        if (log[nl].rc != 0 && log[nl].rc != TSDB_ERR_UNAVAILABLE)
            die("a read (%s) %" PRIu64 " ms into the pause: %s", nl & 1 ? "latest" : "raw", log[nl].sent - T0,
                ers(log[nl].rc));
        nl++;
        struct timespec d = {0, 100000}; /* the log spans the whole pause */
        nanosleep(&d, NULL);
    }
    pause_server(R, 0);
    size_t refused = 0, early_ok = 0;
    uint64_t last_ok = 0, first_refused = 0;
    for (size_t k = 0; k < nl; k++) {
        uint64_t at = log[k].sent - T0;
        if (log[k].rc == 0 && at > 810 && at < 940)
            die("a read sent %" PRIu64 " ms after the Raft leader paused was answered: past the lease (ends by 800)", at);
        if (log[k].rc == 0 && at <= 810)
            early_ok++, last_ok = at;
        if (log[k].rc != 0 && !first_refused)
            first_refused = at;
        refused += log[k].rc != 0 && at > 810 && at < 940;
    }
    if (!refused)
        die("no read was sent between 810 and 940 ms into the pause (%zu reads)", nl);
    printf("  pause of the Raft leader: %zu reads, the last answered at %" PRIu64 " ms, the first refused at %" PRIu64
           " ms, %zu refused in the window\n", nl, last_ok, first_refused, refused);
    if (!lease_until_ok(c, id, 20000, 1))
        die("no read answered on %d after the Raft leader came back", X);
    /* review F1: the paused Raft leader, resumed after an election, answered the heartbeats queued meanwhile against
     * the core's clock from before the pause (mutant nocoreclock: the code before the fix) */
    for (int i = 0; i < 3; i++)
        if (log_has(i, "past the end of its own"))
            die("server %d granted a lease after its own check-quorum lease had ended", i);

    /* c (review F4): a read admitted under the lease and stalled before its snapshot (TSDBD_TEST_READ_STALL) while the
     * other two are paused past X's lease (both: phase a moved the Raft leadership, which may now be X's own): it must
     * be refused, not answered from a snapshot taken after the lease */
    int fdst = open(stall[X], O_CREAT | O_WRONLY, 0644);
    if (fdst < 0)
        die("create %s: %s", stall[X], strerror(errno));
    close(fdst);
    struct stalled_read sr = {X, id, 1};
    pthread_t sth;
    if (pthread_create(&sth, NULL, stalled_read_thread, &sr) != 0)
        die("pthread_create");
    struct timespec d0 = {0, 300000000}; /* the read is admitted and waits */
    nanosleep(&d0, NULL);
    pause_server(R, 1);
    pause_server(F, 1);
    struct timespec d1 = {1, 500000000}; /* past X's lease (800 ms), or its core's check-quorum lease as Raft leader */
    nanosleep(&d1, NULL);
    unlink(stall[X]);
    pthread_join(sth, NULL);
    pause_server(R, 0);
    pause_server(F, 0);
    if (sr.rc != TSDB_ERR_UNAVAILABLE)
        die("a read admitted under the lease and stalled past it answered %s, not UNAVAILABLE", ers(sr.rc));
    printf("  a read stalled between admission and snapshot past the lease: refused\n");
    if (!lease_until_ok(c, id, 20000, 1))
        die("no read answered on %d after the stalled read", X);

    /* b: X paused past its lease and node_timeout; the others paused; X resumed: nobody renews its lease */
    pause_server(X, 1);
    struct timespec d = {4, 500000000};
    nanosleep(&d, NULL);
    pause_server(R, 1);
    pause_server(F, 1);
    pause_server(X, 0);
    uint64_t t1 = now_ms();
    int nb = 0;
    while (now_ms() - t1 < 1500) {
        int rows;
        rc = lease_read(c, id, nb % 3, &rows);
        if (rc == 0)
            die("server %d answered a read %" PRIu64 " ms after it was resumed with every other server paused",
                X, now_ms() - t1);
        nb++;
    }
    pause_server(R, 0);
    pause_server(F, 0);
    /* X was away past node_timeout: the group declared it down and moved the volume to another member, and X, deposed,
     * answers MOVED (it serves no reads for a volume it does not lead). The row is read back where MOVED points. */
    int at = X;
    for (uint64_t t2 = now_ms();;) {
        int rows;
        if (now_ms() - t2 > 30000)
            die("no read answered after every server was resumed (last asked server %d)", at);
        rc = lease_read(c, id, 2, &rows); /* a raw query: MOVED for the whole request, with its body */
        if (rc == 0) {
            if (rows != 1)
                die("a read on server %d answered %d rows, 1 was written", at, rows);
            break;
        }
        tsdbc_err_info e;
        if (rc == TSDB_ERR_MOVED && tsdbc_last_error(c, &e) == TSDB_ERR_MOVED && e.has_body && e.nmoved == 1) {
            int s = server_of(e.moved[0].node_id);
            if (s < 0 || s > 2)
                die("a read on server %d: MOVED to node %u, no server of this test", at, e.moved[0].node_id);
            if (s != at) {
                tsdbc_close(c);
                c = client(s);
                at = s;
            }
        } else if (rc != TSDB_ERR_UNAVAILABLE && rc != TSDB_ERR_MOVED) {
            die("a read on server %d: %s", at, ers(rc));
        }
        usleep(20000);
    }
    printf("  %d paused past its lease: %d reads refused while nobody renewed it, then answered on %d\n", X, nb, at);
    tsdbc_close(c);
}

/* n servers, server 0 INITs with heartbeats of 50 ms and the given lease, drift and node_timeout, the others join */
static void lease_cluster(int n, uint32_t lease_ms, uint32_t drift_ppm, uint32_t node_timeout_ms)
{
    for (int i = 0; i < n; i++)
        start(i);
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = node_timeout_ms;
    s.heartbeat_ms = 50;
    s.lease_ms = lease_ms;
    s.drift_ppm = drift_ppm;
    s.voter_replace_s = 600;
    uint8_t req[128], *reply;
    size_t len;
    size_t rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req);
    int rc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != 0)
        die("init: %s", cluster_strerror(rc));
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    for (int i = 1; i < n; i++)
        node_add(0, i);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
}

/* review F3: a Raft leader declares a member down no sooner than one lease (with the drift margin) after its last grant
 * to it, even where node_timeout is shorter: lease 4000 ms, drift 20 %, node_timeout 1500. Server 2 paused after the
 * leader has led for longer than a lease: DOWN at 4800 ms at the earliest (less a heartbeat for the last grant), not at
 * 1500. Mutant nopromotewait (node_timeout alone) is DOWN after about 1.5 s. */
static void scen_leasedown(void)
{
    scenario("leasedown");
    lease_cluster(3, 4000, 200000, 1500);
    struct timespec d = {5, 500000000}; /* the leader's own wait of one lease is over */
    nanosleep(&d, NULL);
    pause_server(2, 1);
    uint64_t t0 = now_ms();
    struct state_want down = {ids[2], META_DOWN, 0};
    await_view(0, p_member_state, &down, "the paused server marked DOWN");
    uint64_t at = now_ms() - t0;
    pause_server(2, 0);
    if (at < 4600)
        die("server 2 was marked DOWN %" PRIu64 " ms after it was paused: its lease (4800 ms) could still hold", at);
    printf("  paused member: DOWN after %" PRIu64 " ms (lease with margin 4800, node_timeout 1500)\n", at);
    struct state_want up = {ids[2], META_UP, 0};
    await_view(0, p_member_state, &up, "the server UP again");
}

/* review F2: the Raft leader R, alive and still leading, loses its quorum (the two other voters paused) while the volume
 * leader W, the member that is no voter, still reaches it. R grants only while its core's check-quorum lease holds
 * (it ends by 667 ms: E 1000 * 0.8 / 1.2), so W's lease ends by 667 + 800: every read sent in (1550, 1700) ms is
 * refused. Mutant noquorum grants until check-quorum deposes R (1000 to 2000 ms): W answers until 1750 at least. */
static void scen_leasequorum(void)
{
    scenario("leasequorum");
    lease_cluster(4, 1000, 200000, 3000);
    struct view vw;
    int W = -1;
    for (uint64_t t0 = now_ms(); W < 0 && now_ms() - t0 < (uint64_t)WAIT_MS;) {
        view(0, &vw);
        for (int i = 1; i < 4; i++)
            if (!is_voter(&vw, ids[i]))
                W = i;
        int lead = vw.st.role == RAFT_LEADER;
        view_free(&vw);
        if (!lead)
            die("server 0 no longer leads");
        if (W < 0) {
            struct timespec d = {0, 50000000};
            nanosleep(&d, NULL);
        }
    }
    if (W < 0)
        die("no member outside the voters");
    int A = -1, B = -1;
    for (int i = 1; i < 4; i++)
        if (i != W && A < 0)
            A = i;
        else if (i != W)
            B = i;
    char name[16];
    struct meta_volume v;
    int found = 0;
    for (int k = 0; k < 40 && !found; k++) {
        uint32_t num;
        snprintf(name, sizeof name, "lq%d", k);
        int rc = vol_add(0, name, 3, &num);
        if (rc != 0)
            die("volume add %s: %s", name, cluster_strerror(rc));
        if (!vol_rec(0, name, &v))
            die("no record of %s", name);
        found = server_of(v.leader) == W;
    }
    if (!found)
        die("forty volumes, none led by server %d", W);
    printf("  Raft leader 0, voters 0 %d %d, %s led by %d (no voter)\n", A, B, name, W);
    for (uint32_t r = 0; r < v.nrep; r++) {
        int i = server_of(v.rep[r].node);
        if (i >= 0)
            await_open_gen(0, name, i, 1, 0);
    }
    tsdbc *c = client(W);
    tsdb_sid id = rep_series(c, rep_names[0], name, 30000);
    int64_t ts = 1000;
    float val = 7;
    int rc = tsdbc_insert_series(c, id, &ts, &val, 1);
    if (rc != 0)
        die("a write: %s", ers(rc));
    if (!lease_until_ok(c, id, 10000, 1))
        die("no read answered on %d before the pause", W);
    static struct lread log[1 << 16];
    size_t nl = 0;
    pause_server(A, 1);
    pause_server(B, 1);
    uint64_t T0 = now_ms();
    while (now_ms() - T0 < 2500 && nl < sizeof log / sizeof log[0]) {
        int rows;
        log[nl].sent = now_ms();
        log[nl].rc = lease_read(c, id, (int)(nl % 3), &rows);
        if (log[nl].rc != 0 && log[nl].rc != TSDB_ERR_UNAVAILABLE)
            die("a read %" PRIu64 " ms into the pause: %s", log[nl].sent - T0, ers(log[nl].rc));
        nl++;
        struct timespec d = {0, 100000};
        nanosleep(&d, NULL);
    }
    pause_server(A, 0);
    pause_server(B, 0);
    size_t refused = 0;
    uint64_t last_ok = 0;
    for (size_t k = 0; k < nl; k++) {
        uint64_t at = log[k].sent - T0;
        if (log[k].rc == 0 && at > 1550 && at < 1700)
            die("a read sent %" PRIu64 " ms after the Raft leader lost its quorum was answered (its lease ends by 667)",
                at);
        if (log[k].rc == 0)
            last_ok = at;
        refused += log[k].rc != 0 && at > 1550 && at < 1700;
    }
    if (!refused)
        die("no read was sent between 1550 and 1700 ms (%zu reads)", nl);
    printf("  the Raft leader without its quorum: %zu reads, the last answered at %" PRIu64 " ms\n", nl, last_ok);
    if (!lease_until_ok(c, id, 30000, 1))
        die("no read answered on %d once the voters were back", W);
    tsdbc_close(c);
}

/* The serving loop of a server that is not the Raft leader hangs (TSDBD_TEST_LOOP_STALL): its cluster thread still
 * answers, but it sends no heartbeat, so the Raft leader marks it DOWN; once the loop runs again it is UP. Mutant
 * stampignored (heartbeats whatever the stamp says) keeps it UP. */
static void scen_hungloop(void)
{
    scenario("hungloop");
    static char pe[3][PATH_MAX + 80];
    char stall[3][PATH_MAX + 64];
    for (int i = 0; i < 3; i++) {
        snprintf(stall[i], sizeof stall[i], "%s/stall-n%d", base, i);
        unlink(stall[i]);
        snprintf(pe[i], sizeof pe[i], "TSDBD_TEST_LOOP_STALL=%s", stall[i]);
        penv[i] = pe[i];
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    int S = 2;
    int fd = open(stall[S], O_CREAT | O_WRONLY, 0644);
    if (fd < 0)
        die("create %s: %s", stall[S], strerror(errno));
    close(fd);
    uint64_t t0 = now_ms();
    struct state_want down = {ids[S], META_DOWN, 0};
    await_view(0, p_member_state, &down, "the server with the hung loop marked DOWN");
    printf("  server %d's loop hung: DOWN after %" PRIu64 " ms\n", S, now_ms() - t0);
    struct view vw;
    view(S, &vw); /* its cluster thread still answers */
    view_free(&vw);
    unlink(stall[S]);
    t0 = now_ms();
    struct state_want up = {ids[S], META_UP, 0};
    await_view(0, p_member_state, &up, "the server UP again once its loop runs");
    printf("  its loop ran again: UP after %" PRIu64 " ms\n", now_ms() - t0);
}

/* the newer valid-looking slot of server i's REPL of volume num (by generation): its page */
static void repl_page(int i, uint32_t num, uint8_t pg[2][4096], int *newer, char *path, size_t pn)
{
    snprintf(path, pn, "%s/n%d/v%" PRIu32 "/REPL", base, i, num);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0 || pread(fd, pg, 2 * 4096, 0) != 2 * 4096)
        die("%s: %s", path, strerror(errno));
    close(fd);
    uint64_t g0, g1;
    memcpy(&g0, pg[0] + 8, 8);
    memcpy(&g1, pg[1] + 8, 8);
    *newer = g1 > g0;
}

static uint64_t now_us(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000u + (uint64_t)t.tv_nsec / 1000u;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

/* repcheck: three duties of the replication driver that no other scenario sees, each the target of one of its mutants
 * (server/repld.c): a replica's saved floor reaches its engine, which then removes its flushed segments once they are
 * also past the keep caps (repl_retain_s is 2 s here; floornottold keeps them all) and, restarted, opens its log from
 * the floor it saved (floornotsaved never saves it: the log starts above it, the core refuses); a replica whose newest REPL slot is damaged opens "unsure" and its first save at the open takes the
 * group's epoch as fence and led, which clears the flag (unsureignored keeps the older slot's values and the flag);
 * a write whose position in the core is not its log number fails the volume before it is acknowledged (admitnocheck
 * acknowledges it; the fault is TSDB_REPLD_TEST=skippos). It also prints the latency of single-row writes (the fast
 * path sends a batch to the replicas from the bytes callback; nofastpath leaves that to the 5 ms tick). */
static void scen_repcheck(void)
{
    scenario("repcheck");
    for (int i = 0; i < 3; i++) {
        xargs[i] = "--flush-interval-ms 200";
        penv[i] = "TSDB_REPLD_TEST=skippos,stalesent";
        start(i);
    }
    init_retain_s = 2; /* a replica keeps flushed segments past its floor for repl_retain_s (design-cluster.md 8.3) */
    init_cluster(0, 600);
    init_retain_s = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "plant", 3, &num)) != 0 || num != 1)
        die("volume add plant: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "plant", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "plant", &v))
        die("no record of plant");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of plant is node %u", v.leader);
    int R = (L + 1) % 3;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "plant", 30000);

    /* single-row writes over about 2 s: flushes every 200 ms on every node. TSDB_TEST_ACK_MS (tests/repld_mutants.sh)
     * bounds their median, the best of up to three rounds: the fast path sends a batch from the bytes callback, the
     * tick would add up to 5 ms to each write */
    enum { NW = 300 };
    static uint64_t lat[NW];
    const char *ackms = getenv("TSDB_TEST_ACK_MS");
    double best = 1e9;
    for (int round = 0; round < 3; round++) {
        for (int j = 0; j < NW; j++) {
            int64_t ts = 1000 + 1000LL * (j + NW * round);
            float val = (float)j;
            uint64_t t0 = now_us();
            if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
                die("small insert %d: %s", j, ers(rc));
            lat[j] = now_us() - t0;
            if (j % 20 == 19)
                usleep(120000);
        }
        qsort(lat, NW, sizeof lat[0], cmp_u64);
        printf("  %d single-row writes on a volume of 3 replicas: median %.2f ms, 90th percentile %.2f ms\n", NW,
               lat[NW / 2] / 1000.0, lat[NW * 9 / 10] / 1000.0);
        best = lat[NW / 2] / 1000.0 < best ? lat[NW / 2] / 1000.0 : best;
        if (!ackms || best < atof(ackms))
            break;
    }
    if (ackms && best >= atof(ackms))
        die("the median latency of a single-row write is %.2f ms (best of three rounds), not below %s ms", best, ackms);
    int nw = 0;
    uint64_t t0 = now_ms();
    for (;;) {
        nw = wal_count(R, num);
        if (nw <= 2)
            break;
        if (now_ms() - t0 > 20000)
            die("replica %d still holds %d WAL segments 20 s after the writes (its saved floor never removed its "
                "flushed segments past the keep caps)", R, nw);
        usleep(100000);
    }
    printf("  the replica removed its flushed segments once its floor was saved (%d left)\n", nw);
    /* a clean restart: the segments went, so the log starts above position 1, and the floor that let them go must be
     * in REPL, or the core refuses the log at this open and every later one (floornotsaved: the engine was told the
     * floor, the save was skipped). Done here because only these 2 s keep caps let a replica's segments go in a test. */
    {
        int o = log_count(R, " of 3, log ");
        stop(R, SIGTERM);
        start(R);
        await_log_count(R, " of 3, log ", o + 1, 30000);
        /* the core's refusal is said by the volume's thread, after the open's line, and the ISR may still hold R for
         * isr_lag_ms: 2 s for the line before the ISR is asked */
        for (uint64_t t1 = now_ms(); isr_of(L, "plant", NULL) != 3 || now_ms() - t1 < 2000;) {
            if (log_has(R, "the log starts above the commit floor"))
                die("replica %d restarted after its flushed segments went refused its log: the log starts above the "
                    "commit floor (the floor that let them go was not saved)", R);
            if (now_ms() - t1 > 30000)
                die("replica %d restarted after its flushed segments went: the ISR of plant has %" PRIu32
                    " members, not 3, after 30000 ms", R, isr_of(L, "plant", NULL));
            usleep(50000);
        }
        if (log_has(R, "the log starts above the commit floor"))
            die("replica %d restarted after its flushed segments went refused its log: the log starts above the "
                "commit floor (the floor that let them go was not saved)", R);
        printf("  restarted, the replica opened its log from the floor it saved and joined the ISR again\n");
    }

    /* damage the newer REPL slot of the replica, restart it: it opens unsure, and the open's save clears that */
    stop(R, SIGTERM);
    uint8_t pg[2][4096];
    int nk;
    char path[PATH_MAX + 120];
    repl_page(R, num, pg, &nk, path, sizeof path);
    {
        int fd = open(path, O_WRONLY | O_CLOEXEC);
        uint8_t b = pg[nk][30] ^ 0x5a; /* inside the volume uuid: the slot's checksum fails */
        if (fd < 0 || pwrite(fd, &b, 1, (off_t)nk * 4096 + 30) != 1 || fsync(fd) != 0)
            die("damaging %s: %s", path, strerror(errno));
        close(fd);
    }
    uint64_t rid, gen;
    {
        uint64_t g;
        memcpy(&g, pg[nk] + 80, 8);
        gen = g;
        memcpy(&rid, pg[nk] + 72, 8);
    }
    /* the open generation is registered by the volume manager before the driver opens (and saves REPL): wait for the
     * driver's line */
    int opens = log_count(R, " of 3, log ");
    start(R);
    await_open_gen(L, "plant", R, gen + 1, rid);
    await_log_count(R, " of 3, log ", opens + 1, 30000);
    if (!vol_rec(L, "plant", &v))
        die("no record of plant");
    uint64_t epoch = v.epoch;
    stop(R, SIGTERM);
    repl_page(R, num, pg, &nk, path, sizeof path);
    uint32_t unsure;
    uint64_t fence, led;
    memcpy(&unsure, pg[nk] + 60, 4);
    memcpy(&fence, pg[nk] + 88, 8);
    memcpy(&led, pg[nk] + 96, 8);
    if (unsure != 0 || fence != led || fence < epoch)
        die("replica %d opened with a damaged REPL slot and saved unsure %u, fence %" PRIu64 ", led %" PRIu64
            " (the group's epoch is %" PRIu64 ")", R, unsure, fence, led, epoch);
    printf("  a replica with a damaged REPL slot opened unsure and took the group's epoch %" PRIu64 " as its fence\n",
           epoch);
    start(R);
    await_open_gen(L, "plant", R, gen + 2, rid);
    await_log_count(R, " of 3, log ", opens + 2, 30000);
    /* the surviving slot's floor was older than the segments the replica had removed: the open takes the log's start
     * as the floor's lower bound, or the core refuses the log at this open and every later one */
    usleep(300000);
    if (log_has(R, "the log starts above the commit floor"))
        die("replica %d stopped after an open with a damaged REPL slot: the log starts above the commit floor it saved",
            R);

    /* a peer's cursor set back below the leader's log start (its segments went with the commit point): the cursor must
     * move up to what the peer acknowledged, or the catch-up finds nothing to read and no write commits again (jepsen
     * kv, seed 1: a resync raced records in flight) */
    {
        char stale[PATH_MAX + 80];
        snprintf(stale, sizeof stale, "%s/n%d/stalesent", base, L);
        int sfd = open(stale, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (sfd < 0)
            die("%s: %s", stale, strerror(errno));
        close(sfd);
        t0 = now_ms();
        while (!log_has(L, "test fault: the cursor of peer")) {
            if (now_ms() - t0 > 10000)
                die("the stalesent fault did not fire in 10 s");
            usleep(10000);
        }
        struct rep_writer sw = {cports[L], id, 4000000, 1, 0, 0, 0, 0};
        pthread_t sth;
        if (pthread_create(&sth, NULL, rep_write_thread, &sw) != 0)
            die("pthread_create");
        t0 = now_ms();
        while (!__atomic_load_n(&sw.done, __ATOMIC_ACQUIRE)) {
            if (now_ms() - t0 > 10000)
                die("no write was acknowledged in 10 s after a peer's cursor went back below the leader's log start "
                    "(the cursor never moved up to what the peer acknowledged)");
            usleep(10000);
        }
        pthread_join(sth, NULL);
        if (sw.rc != 0)
            die("the write after a peer's cursor went back failed: %s", ers(sw.rc));
        /* With the ISR shrink the writes go on without the stuck peer (it leaves the ISR after isr_lag_ms): the
         * stall is then that peer's, which never gets another record and never joins again */
        await_isr(L, "plant", 3, 20000,
                  "after a peer's cursor went back below the leader's log start (the cursor never moved up to what the "
                  "peer acknowledged)");
        printf("  a peer's cursor set back below the leader's log start moved up to what it acknowledged\n");
    }

    /* the core's position one ahead of the log number: the write fails the volume, unacknowledged */
    char skip[PATH_MAX + 80];
    snprintf(skip, sizeof skip, "%s/n%d/skippos", base, L);
    int fd = open(skip, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0)
        die("%s: %s", skip, strerror(errno));
    close(fd);
    struct rep_writer w = {cports[L], id, 5000000, 1, 0, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, rep_write_thread, &w) != 0)
        die("pthread_create");
    t0 = now_ms();
    while (!__atomic_load_n(&w.done, __ATOMIC_ACQUIRE)) {
        if (now_ms() - t0 > 15000)
            die("a write whose core position is not its log number neither failed nor was acknowledged in 15 s");
        usleep(10000);
    }
    pthread_join(th, NULL);
    if (w.rc == 0)
        die("a write whose core position is not its log number was acknowledged");
    if (!log_has(L, "the core's position is not the log number"))
        die("the leader did not fail the volume on a core position that is not the log number");
    printf("  a write whose core position is not its log number failed the volume (%s), unacknowledged\n", ers(w.rc));
    tsdbc_close(c);
    stop_all();
}


/* design-cluster.md 6.3 (r3e-failover): the leader of a replicated volume is paused (SIGSTOP) past its lease and
 * node_timeout with a write waiting for its commit point (both replicas were paused first, and nothing can shrink
 * the ISR without a Raft quorum). The replicas resume and one is promoted at e+1; it takes a write. The old leader
 * resumes with the others paused: it answers no read (no lease, deposed or not). Once every server runs, its waiting
 * write ends acknowledged (the replicas read the record from their sockets, acknowledged it, and the new epoch starts
 * past it: the old leader may see those acknowledgements before the fence) or with a closed connection (D12: of
 * unknown outcome), never MOVED or UNAVAILABLE; a new write is MOVED to the new leader and a client with a budget
 * follows it there; the old leader rejoins as a replica (cut, caught up) with the same rows as the others. Which of
 * the two ends the waiting write gets is a race, so the planted fault deposemoved is writerdepose's to catch. */
/* the fence in server i's REPL of volume number (the slot with the newer generation; offset 88, src/replfile.c) */
static uint64_t repl_fence_of(int i, uint32_t number)
{
    char dir[PATH_MAX + 80], path[PATH_MAX + 160];
    uint8_t pg[2][4096];
    server_dir(i, dir, sizeof dir);
    snprintf(path, sizeof path, "%s/v%" PRIu32 "/REPL", dir, number);
    FILE *f = fopen(path, "r");
    if (!f)
        die("%s: %s", path, strerror(errno));
    size_t got = fread(pg, 1, sizeof pg, f);
    fclose(f);
    if (got != sizeof pg)
        die("%s: %zu bytes, not two slots", path, got);
    uint64_t g[2], fence;
    memcpy(&g[0], pg[0] + 8, 8);
    memcpy(&g[1], pg[1] + 8, 8);
    memcpy(&fence, pg[g[1] > g[0]] + 88, 8);
    return fence;
}

/* Crash points of a promotion (design-cluster.md 6.2), a replica armed, the leader killed under none of its writes lost:
 * fencecrash: a fenced replica saves the fence in REPL before it answers, so one that stops at fence-saved-before-reply
 * (its reply not yet out) holds the new epoch's fence on disk (mutant fencenosync answers with the fence unsaved); the
 * other replica is promoted alone. promotecrash: both replicas armed with promote-before-epoch, the one the record
 * names stops before its EPOCH marker; restarted, it never resumes that epoch (it resigns, or is promoted again later)
 * and some replica leads with every acknowledged row. deposecrash: the leader paused past its lease and node_timeout is
 * promoted away; resumed, it stops at depose-before-reopen (writer closed, replica not yet open) and, restarted, opens
 * as a replica, rejoins and holds the same rows. Each time the crashed server rejoins. */
static void crash_promo(int promote)
{
    scenario(promote == 2 ? "deposecrash" : promote ? "promotecrash" : "fencecrash");
    for (int i = 0; i < 3; i++) {
        penv[i] = promote == 2 ? "TSDB_CRASH_AT=depose-before-reopen" : NULL; /* only a demotion passes it */
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "fc", 3, &num)) != 0)
        die("volume add fc: %s", cluster_strerror(rc));
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "fc", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "fc", &v))
        die("no record of fc");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of fc is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "fc", 30000);
    for (int j = 0; j < 20; j++) {
        int64_t ts = 1000000 + 1000LL * j;
        float val = (float)j;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d to the leader: %s", j, ers(rc));
    }
    tsdbc_close(c);
    await_isr(0, "fc", 3, 30000, "every replica in the ISR");
    /* the crashing replica is one that is not the Raft leader (the fences come from there) */
    int R = -1;
    for (int i = 0; i < 3 && R < 0; i++)
        if (i != L && i != 0)
            R = i;
    int Q = 3 - L - R;
    const char *spec = promote ? "TSDB_CRASH_AT=promote-before-epoch" : "TSDB_CRASH_AT=fence-saved-before-reply";
    for (int k = 0; k < (promote == 2 ? 0 : promote ? 2 : 1); k++) {
        int x = k ? Q : R;
        stop(x, SIGTERM);
        penv[x] = spec;
        start(x);
        /* back as a replica (its volume opens once the group holds its new incarnation) before the next one stops:
         * two servers restarted at new peer ports at once cannot find each other (scen_minisr) */
        await_log_count(x, "open as the replica", 2, 60000);
        await_isr(k ? R : Q, "fc", 3, 60000, "the restarted replica back in the ISR");
    }
    if (promote == 2) {
        /* paused past its lease and node_timeout, the leader is promoted away; resumed, it is deposed and stops
         * between the close of its writer and the open as a replica; restarted, it opens as a replica and rejoins */
        pause_server(L, 1);
        uint64_t t1 = now_ms();
        while (!(vol_rec(Q, "fc", &v) && v.epoch > e0 && v.leader != ids[L])) {
            if (now_ms() - t1 > 30000)
                die("no new leader of fc 30 s after its leader was paused");
            usleep(50000);
        }
        pause_server(L, 0);
        int st = await_exit(L, 30000);
        for (int i = 0; i < 3; i++)
            penv[i] = NULL;
        if (st != 99)
            die("the deposed leader %d did not stop at depose-before-reopen (status %d)", L, st);
        printf("  the deposed leader %d stopped at depose-before-reopen\n", L);
        start(L);
    } else {
        signal_child(pids[L], SIGKILL);
        if (await_exit(L, 5000) < 0)
            die("server %d did not die of SIGKILL", L);
    }
    if (promote == 2) {
    } else if (promote) {
        /* the one named first stops; the other may be named after it and stop as well */
        uint64_t t1 = now_ms();
        int st = -1;
        for (;;) {
            int got_one = -1;
            for (int x = 0; x < 3 && got_one < 0; x++)
                if (x != L && pids[x] && (st = await_exit(x, 0)) >= 0)
                    got_one = x;
            if (got_one >= 0) {
                if (st != 99)
                    die("replica %d ended with status %d, not at promote-before-epoch", got_one, st);
                printf("  replica %d stopped at promote-before-epoch\n", got_one);
                penv[got_one] = NULL;
                start(got_one);
                break;
            }
            if (now_ms() - t1 > 30000)
                die("no replica stopped at promote-before-epoch 30 s after the leader was killed");
            usleep(50000);
        }
        penv[R] = penv[Q] = NULL;
    } else {
        int st = await_exit(R, 30000);
        penv[R] = NULL;
        if (st != 99)
            die("replica %d did not stop at fence-saved-before-reply (status %d)", R, st);
        uint64_t fence = repl_fence_of(R, num);
        if (fence < e0 + 1)
            die("replica %d: REPL holds fence %" PRIu64 " after it stopped at fence-saved-before-reply for epoch %" PRIu64,
                R, fence, e0 + 1);
        printf("  replica %d stopped at fence-saved-before-reply with fence %" PRIu64 " in REPL\n", R, fence);
        start(R);
    }
    uint64_t t0 = now_ms();
    for (;;) {
        if (promote) {
            /* the replica still armed may stop too when named; it is restarted unarmed */
            for (int x = 0; x < 3; x++)
                if (x != L && pids[x] && await_exit(x, 0) >= 0)
                    start(x);
        }
        int via = pids[Q] ? Q : R;
        char lead[64];
        if (vol_rec(via, "fc", &v) && v.leader != ids[L] && v.epoch > e0 && server_of(v.leader) >= 0 &&
            (snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch), log_has(server_of(v.leader), lead)))
            break;
        if (now_ms() - t0 > 30000)
            die("no new leader of fc 30 s after its leader was killed (epoch %" PRIu64 ", leader %u)", v.epoch, v.leader);
        usleep(20000);
    }
    int Nw = server_of(v.leader);
    int got = rows_in(Nw, id, 1000000, 1000000 + 1000LL * 20, 30000);
    if (got != 20)
        die("the new leader (server %d) holds %d of the 20 acknowledged rows", Nw, got);
    if (promote == 2) {
        await_log_count(L, "open as the replica", 1, 60000); /* its first open was as the leader */
        await_isr(Nw, "fc", 3, 60000, "the deposed leader back in the ISR");
        stop_all();
        uint64_t wantr[REP_NAMES] = {0};
        wantr[0] = 20;
        rep_same(wantr, "after the deposed leader rejoined");
        printf("  server %d leads fc at epoch %" PRIu64 "; the deposed leader reopened as a replica and holds the same "
               "rows\n", Nw, v.epoch);
        return;
    }
    await_isr(Nw, "fc", 2, 60000, "the two live replicas in the ISR");
    printf("  server %d leads fc at epoch %" PRIu64 " with every row; the crashed replica rejoined\n", Nw, v.epoch);
    stop_all();
}

/* leasestale (r3e-leases F5): a deposed writer that has not heard of its deposal gets no read lease. Server L, the
 * leader of the volume, has its volume thread stalled (TSDB_REPLD_TEST=stallinbox, the file in place, a write waiting
 * on its replicas' ACKs, which the stalled thread never takes) and is paused until the record names a new leader. Once
 * it runs again its driver still sees itself the writer at the old epoch (the record change waits in the stalled
 * inbox) and reports OWNER; the promotion's state is gone with the record moved, so only the check of the report
 * against the record keeps the lease from it. Reads on L must be refused (they would miss the new leader's rows) until
 * the stall ends and L, deposed, takes up the new history. TSDB_CLUSTER_MUTANT=leasestale fails it. */
static void scen_leasestale(void)
{
    scenario("leasestale");
    strict_reads = 1; /* every read on the leader (D5), as in production */
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=stallinbox";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "ls", 3, &num)) != 0 || num != 1)
        die("volume add ls: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "ls", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "ls", &v))
        die("no record of ls");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of ls is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "ls", 30000);
    for (int k = 0; k < 10; k++) {
        int64_t ts = 1000 + k;
        float val = 1;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d with every replica up: %s", k, ers(rc));
    }
    tsdbc_close(c);
    await_isr(0, "ls", 3, 30000, "every replica in the ISR before the stall");
    char stall[PATH_MAX + 80];
    server_dir(L, stall, sizeof stall);
    strcat(stall, "/stallinbox");
    FILE *sf = fopen(stall, "w");
    if (!sf)
        die("create %s", stall);
    fclose(sf);
    struct wf_writer w = {client(L), id, 2000, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, wf_write_thread, &w) != 0)
        die("pthread_create");
    nanosleep(&(struct timespec){0, 500000000}, NULL); /* the replicas' ACKs reach L's inbox: its thread stalls */
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE)) {
        unlink(stall);
        die("a write to a leader with a stalled volume thread was answered: %s", ers(w.rc));
    }
    pause_server(L, 1);
    int Nw = await_promoted((L + 1) % 3, "ls", ids[L], e0, &v);
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    tsdbc *cn = client(Nw);
    tsdbc_set_retry(cn, 10000);
    int64_t ts = 3000;
    float val = 3;
    if ((rc = tsdbc_insert_series(cn, id, &ts, &val, 1)) != 0) {
        unlink(stall);
        die("a write to the new leader: %s", ers(rc));
    }
    tsdbc_close(cn);
    pause_server(L, 0);
    /* L heartbeats again, as OWNER at e0, for 4 s: no read there may be answered */
    tsdbc *cr = client(L);
    tsdbc_set_retry(cr, 0);
    int answered = 0, refused = 0, rows = 0, last = 0;
    for (uint64_t t0 = now_ms(); now_ms() - t0 < 4000; usleep(100000)) {
        int r = lease_read(cr, id, 0, &rows);
        if (r == 0) {
            answered++;
            last = rows;
        } else if (r == TSDB_ERR_UNAVAILABLE) {
            refused++;
        }
    }
    tsdbc_close(cr);
    unlink(stall);
    if (answered)
        die("server %d, deposed at epoch %" PRIu64 " while stalled and not yet told, answered %d reads (the last with "
            "%d rows; the new leader has 11): it held a read lease",
            L, e0, answered, last);
    if (!refused)
        die("no read on the stale writer was refused UNAVAILABLE in 4 s (the case proved nothing)");
    pthread_join(th, NULL);
    tsdbc_close(w.c);
    await_isr(Nw, "ls", 3, 60000, "the stale writer back in the ISR once its stall ended");
    stop_all();
    for (int i = 0; i < 3; i++)
        penv[i] = NULL;
    printf("  a writer deposed while stalled reports itself OWNER at epoch %" PRIu64 " and gets no read lease (%d reads "
           "refused); it rejoins once the stall ends\n",
           e0, refused);
}

static void scen_fencecrash(void) { crash_promo(0); }
static void scen_promotecrash(void) { crash_promo(1); }
static void scen_deposecrash(void) { crash_promo(2); }

static void scen_pause(void)
{
    scenario("pause");
    strict_reads = 1; /* the deposed leader back in the ISR holds a lease again: its reads must go to the new leader */
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "pz", 3, &num)) != 0 || num != 1)
        die("volume add pz: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "pz", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "pz", &v))
        die("no record of pz");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of pz is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "pz", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    await_isr(0, "pz", 3, 30000, "every replica in the ISR before the pause");
    int R1 = (L + 1) % 3, R2 = (L + 2) % 3;
    pause_server(R1, 1);
    pause_server(R2, 1);
    struct wf_writer w = {client(L), id, 2000, 0, 0, 0};
    pthread_t th;
    if (pthread_create(&th, NULL, wf_write_thread, &w) != 0)
        die("pthread_create");
    nanosleep(&(struct timespec){0, 300000000}, NULL);
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE))
        die("a write with both replicas paused was answered: %s", ers(w.rc));
    pause_server(L, 1);
    uint64_t tp = now_ms();
    pause_server(R1, 0);
    pause_server(R2, 0);
    int Nw = await_promoted(R1, "pz", ids[L], e0, &v);
    if (Nw < 0 || Nw == L || v.epoch != e0 + 1)
        die("the new record of pz: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    uint64_t tr = now_ms();
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    tsdbc *cn = client(Nw);
    tsdbc_set_retry(cn, 10000);
    ts = 3000;
    if ((rc = tsdbc_insert_series(cn, id, &ts, &val, 1)) != 0)
        die("a write to the new leader: %s", ers(rc));
    tsdbc_close(cn);
    printf("  leader %d paused with a write waiting: the record names server %d at epoch %" PRIu64 " after %" PRIu64
           " ms\n", L, Nw, v.epoch, tr - tp);
    /* the old leader resumes alone: whatever it learns from what was queued for it, it answers no read */
    pause_server(R1, 1);
    pause_server(R2, 1);
    pause_server(L, 0);
    uint64_t t1 = now_ms();
    int nb = 0;
    while (now_ms() - t1 < 1500) {
        int rows;
        if ((rc = lease_read(c, id, nb % 3, &rows)) == 0)
            die("server %d answered a read %" PRIu64 " ms after it was resumed with every other server paused (%d rows)",
                L, now_ms() - t1, rows);
        nb++;
    }
    if (__atomic_load_n(&w.done, __ATOMIC_ACQUIRE) && w.rc != 0 && (w.rc != TSDB_ERR_IO || w.after != TSDB_ERR_IO))
        die("the write waiting on the paused leader got %d (%s), then its connection %d (%s): not a closed connection",
            w.rc, ers(w.rc), w.after, ers(w.after));
    pause_server(R1, 0);
    pause_server(R2, 0);
    for (uint64_t t0 = now_ms(); !__atomic_load_n(&w.done, __ATOMIC_ACQUIRE); usleep(20000))
        if (now_ms() - t0 > 15000)
            die("the write waiting on the deposed leader %d was not ended in 15 s", L);
    pthread_join(th, NULL);
    if (w.rc != 0 && (w.rc != TSDB_ERR_IO || w.after != TSDB_ERR_IO))
        die("the write waiting on the deposed leader got %d (%s), then its connection %d (%s): neither acknowledged "
            "nor a closed connection (D12)", w.rc, ers(w.rc), w.after, ers(w.after));
    tsdbc_close(w.c);
    tsdbc_close(c);
    printf("  resumed alone it answered none of %d reads; its waiting write ended %s\n", nb,
           w.rc == 0 ? "acknowledged (committed in the new epoch)" : "with a closed connection");
    /* new writes on the old leader are MOVED, and a client with a budget follows them */
    c = client(L);
    ts = 4000;
    insert_expect_moved(c, id, ts, val, 1, v.leader, v.epoch, "a write to the deposed leader");
    tsdbc_set_retry(c, 10000);
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write that follows MOVED from the deposed leader %d: %s", L, ers(rc));
    tsdbc_close(c);
    await_isr(R1, "pz", 3, 60000, "the deposed leader back in the ISR");
    /* back in the ISR the deposed leader holds a read lease again, but the writes the new leader acknowledges reach it
     * only at its next apply: every read there is MOVED to the new leader (D5, review F2), never answered stale. A
     * write acknowledged by the new leader is in every answer a client following MOVED from L gets. */
    c = client(L);
    for (int k = 0; k < 3; k++) {
        int rows = -1;
        rc = lease_read(c, id, k, &rows);
        if (rc == 0)
            die("the deposed leader %d, back in the ISR, answered a read (kind %d, %d rows): a replica's answer may "
                "miss a write the new leader acknowledged", L, k, rows);
        expect_moved_ep(c, rc, (uint16_t)num, v.leader, v.epoch, "a read on the deposed leader");
    }
    tsdbc_set_retry(c, 10000);
    tsdbc *cw = client(Nw);
    /* The pauses above (the old leader resumed alone, its peers paused, then all resumed) can make the metadata group
     * elect again (term 3 or 4) about now, and a lease that lapses meanwhile refuses a write UNAVAILABLE until the new
     * term's heartbeats are answered (§7.1, nothing written): retried, as by any client. What this part checks is the
     * reads through the deposed leader. (ASan under load: 2 of 12 runs of c0047c5 failed here with retry 0, each
     * with such an election.) */
    tsdbc_set_retry(cw, 10000);
    for (int k = 0; k < 20; k++) {
        ts = 10000 + k;
        if ((rc = tsdbc_insert_series(cw, id, &ts, &val, 1)) != 0)
            die("write %d on the new leader: %s", k, ers(rc));
        int want = rows_in(Nw, id, 0, INT64_MAX / 2, 10000), rows = -1;
        if ((rc = lease_read(c, id, k % 2 ? 2 : 0, &rows)) != 0 || rows != want) /* a count: raw, QUERY_MULTI */
            die("a read through the deposed leader %d after write %d was acknowledged: %d (%s), %d rows of %d", L, k,
                rc, ers(rc), rows, want);
    }
    tsdbc_close(cw);
    tsdbc_close(c);
    int total = rows_in(Nw, id, 0, INT64_MAX / 2, 10000);
    if (total != 24 && !(total == 23 && w.rc != 0))
        die("the new leader holds %d rows: 23 acknowledged, and the waiting write %s", total,
            w.rc == 0 ? "acknowledged too" : "of unknown outcome");
    stop_all();
    uint64_t wantr[REP_NAMES] = {0};
    wantr[0] = (uint64_t)total;
    rep_same(wantr, "after the pause");
    printf("  the deposed leader rejoined as a replica: every server holds the same %d rows\n", total);
}

/* fwdlease (S2-2): a forwarded read is judged by the owner's read lease, as a read sent to it directly. scen_lease's
 * phase a with every read sent to the third server F, which forwards it to the volume leader X: the Raft leader R
 * paused at T0, X's lease ends by T0 + 800 ms (lease_ms 1000, drift 20%) and no new Raft leader can grant before an
 * election timeout, so every read F forwards in (T0 + 810, T0 + 940) must be refused, and some must have been sent.
 * Mutant fwdnolease (the owner skips the lease for a forward). */
static void scen_fwdlease(void)
{
    scenario("fwdlease");
    strict_reads = 1;
    fwd_on = 1;
    const char *mut = getenv("TSDBD_FWD_MUTANT");
    static char menv[160];
    snprintf(menv, sizeof menv, "TSDBD_FWD_MUTANT=%s", mut ? mut : "");
    for (int i = 0; i < 3; i++) {
        penv[i] = menv;
        start(i);
    }
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 3000;
    s.heartbeat_ms = 50;
    s.lease_ms = 1000;
    s.drift_ppm = 200000;
    s.voter_replace_s = 600;
    uint8_t req[128], *reply;
    size_t len;
    size_t rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req);
    int rc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != 0)
        die("init: %s", cluster_strerror(rc));
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    int R = 0, X = -1;
    char name[16];
    struct meta_volume v;
    for (int k = 0; k < 8 && X < 0; k++) {
        uint32_t num;
        snprintf(name, sizeof name, "fl%d", k);
        if ((rc = vol_add(0, name, 3, &num)) != 0)
            die("volume add %s: %s", name, cluster_strerror(rc));
        if (!vol_rec(0, name, &v))
            die("no record of %s", name);
        int L = server_of(v.leader);
        if (L >= 0 && L <= 2 && L != R)
            X = L;
    }
    if (X < 0)
        die("eight volumes, every one led by the Raft leader");
    int F = 3 - R - X;
    for (int i = 0; i < 3; i++)
        await_open_gen(0, name, i, 1, 0);
    tsdbc *cx = client(X);
    tsdb_sid id = rep_series(cx, rep_names[0], name, 30000);
    int64_t ts = 1000;
    float val = 7;
    if ((rc = tsdbc_insert_series(cx, id, &ts, &val, 1)) != 0)
        die("a write: %s", ers(rc));
    tsdbc_close(cx);
    tsdbc *c = client(F);
    tsdbc_set_retry(c, 0); /* the first answer, not a retried one */
    if (!lease_until_ok(c, id, 10000, 1))
        die("no read through %d (forwarded to %d) answered before the pause", F, X);
    uint64_t f0 = fw_frames(c, v.leader);

    static struct lread log[1 << 16];
    size_t nl = 0;
    pause_server(R, 1);
    uint64_t T0 = now_ms();
    while (now_ms() - T0 < 2000 && nl < sizeof log / sizeof log[0]) {
        int rows;
        log[nl].sent = now_ms();
        log[nl].rc = lease_read(c, id, (int)(nl % 3), &rows);
        if (log[nl].rc != 0 && log[nl].rc != TSDB_ERR_UNAVAILABLE)
            die("a read through %d %" PRIu64 " ms into the pause: %s", F, log[nl].sent - T0, ers(log[nl].rc));
        nl++;
        struct timespec d = {0, 100000};
        nanosleep(&d, NULL);
    }
    pause_server(R, 0);
    size_t refused = 0;
    for (size_t k = 0; k < nl; k++) {
        uint64_t at = log[k].sent - T0;
        if (log[k].rc == 0 && at > 810 && at < 940)
            die("a read forwarded through %d %" PRIu64 " ms after the Raft leader paused was answered by %d: past its "
                "lease (ends by 800)", F, at, X);
        refused += log[k].rc != 0 && at > 810 && at < 940;
    }
    if (!refused)
        die("no read was sent through %d between 810 and 940 ms into the pause (%zu reads)", F, nl);
    uint64_t f1 = fw_frames(c, v.leader);
    if (f1 < f0 + nl)
        die("%zu reads through %d, %" PRIu64 " frames forwarded to %d", nl, F, f1 - f0, X);
    if (!lease_until_ok(c, id, 20000, 1))
        die("no read through %d answered after the Raft leader came back", F);
    tsdbc_close(c);
    stop_all();
    printf("  reads forwarded from %d to %d with the Raft leader %d paused: %zu refused in (810, 940) ms, none answered "
           "past the lease\n", F, X, R, refused);
}

/* fwddepose (S2-2): a forward that reaches a leader deposed since the entry node's copy was taken is answered MOVED by
 * it, never forwarded a second hop. fd on three servers; the replicas' copies held (TSDB_CLUSTER_TEST hold_copy_file)
 * while the leader L is paused past node_timeout and a replica N is promoted; N's copy is let go, the entry E's kept;
 * L resumes, rejoins as a replica and learns N leads: a write through E goes to L at the old epoch and must come back
 * MOVED, not applied. Mutant fwdtwice (L forwards it on to N). */
static void scen_fwddepose(void)
{
    scenario("fwddepose");
    strict_reads = 1;
    fwd_on = 1;
    const char *mut = getenv("TSDBD_FWD_MUTANT");
    static char menv[160];
    snprintf(menv, sizeof menv, "TSDBD_FWD_MUTANT=%s", mut ? mut : "");
    for (int i = 0; i < 3; i++) {
        penv[i] = menv;
        envs[i] = "hold_copy_file=1";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "fd", 3, &num)) != 0)
        die("volume add fd: %s", cluster_strerror(rc));
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "fd", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "fd", &v))
        die("no record of fd");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of fd is node %u", v.leader);
    uint64_t e0 = v.epoch;
    uint32_t oldlead = v.leader;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "fd", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    tsdbc_close(c);
    await_isr(0, "fd", 3, 30000, "every replica in the ISR before the pause");
    char hold[3][PATH_MAX + 96];
    for (int i = 0; i < 3; i++) {
        server_dir(i, hold[i], sizeof hold[i]);
        strcat(hold[i], "/state/holdcopy");
        if (i == L)
            continue;
        FILE *h = fopen(hold[i], "w");
        if (!h)
            die("%s: %s", hold[i], strerror(errno));
        fclose(h);
    }
    usleep(300000); /* a round of their cluster threads */
    pause_server(L, 1);
    int Nw = await_promoted((L + 1) % 3, "fd", ids[L], e0, &v);
    if (Nw < 0 || Nw == L || v.epoch <= e0)
        die("the new record of fd: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    unlink(hold[Nw]);
    int E = 3 - L - Nw;
    pause_server(L, 0);
    /* L deposed and back as a replica, its own copy naming Nw */
    uint64_t t0 = now_ms();
    for (;;) {
        struct meta_volume w;
        if (vol_rec(L, "fd", &w) && w.leader == v.leader && w.epoch == v.epoch)
            break;
        if (now_ms() - t0 > 30000)
            die("server %d never saw fd led by %u at epoch %" PRIu64, L, v.leader, v.epoch);
        usleep(50000);
    }
    await_isr(Nw, "fd", 3, 30000, "the deposed leader back in the ISR");
    usleep(300000);
    c = client(E);
    tsdbc_set_retry(c, 0);
    uint64_t f0 = fw_frames(c, oldlead);
    ts = 5000;
    t0 = now_ms();
    while ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) == TSDB_ERR_UNAVAILABLE && now_ms() - t0 < 10000)
        usleep(100000); /* E's link to L coming back */
    uint64_t f1 = fw_frames(c, oldlead);
    tsdbc_close(c);
    if (f1 <= f0)
        die("the write through %d (its copy held at epoch %" PRIu64 ") was not forwarded to %d: %s", E, e0, L, ers(rc));
    if (rc != TSDB_ERR_MOVED)
        die("a write forwarded to the deposed leader %d: %s, not MOVED", L, ers(rc));
    unlink(hold[E]);
    tsdbc *cn = client(Nw);
    nrows = 0;
    if ((rc = tsdbc_query_raw(cn, id, 5000, 5001, count_rows, NULL)) != 0)
        die("a read on the new leader: %s", ers(rc));
    tsdbc_close(cn);
    if (nrows != 0)
        die("the write answered MOVED by the deposed leader is on the new leader");
    stop_all();
    printf("  a write forwarded by %d (copy held at epoch %" PRIu64 ") to the deposed leader %d: MOVED, not applied on "
           "the new leader %d (epoch %" PRIu64 ")\n", E, e0, L, Nw, v.epoch);
}

/* fwdload (S2-2): forwarding under load, without login. Three servers, --workers 2 --fwd-workers 1 on each; two volumes of
 * one replica, vx led on X and vy on Y (X != Y).
 * f. A slow reader (QUERY_RAW of 6 M rows through Y, about 5 s to read) beside a stream of one-row inserts through Y to
 *    the same owner X: the reader's chunks wait for credit on X's only reserved worker, which must count as waiting
 *    (the pool grows), so every insert answers and their p99 stays small; the reader gets every row (ha.md 15.4: the
 *    insert p99 holds and the link survives). Mutant fwdchunknowait.
 * g. X and Y forward to each other under load: 4 clients on each write the other's volume and read 5-chunk replies
 *    from it; everything answers within the bound. Mutant fwdinline (a forward served on the links' dispatcher: two
 *    owners each wait for credit that only the other's dispatcher would let through). */
struct fl_read {
    tsdbc *c;
    tsdb_sid id;
    int64_t lo, hi;
    uint64_t rows, ms;
    int rc, slow;
    atomic_int done; /* set after rc and ms: the stream of inserts in f polls it while the reader runs */
};

static int fl_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts, (void)v;
    struct fl_read *r = ctx;
    r->rows += n;
    if (r->slow) {
        struct timespec d = {0, (long)n * 800}; /* 6 M rows: about 5 s */
        nanosleep(&d, NULL);
    }
    return 0;
}

static void *fl_read_run(void *arg)
{
    struct fl_read *r = arg;
    uint64_t t0 = now_ms();
    r->rc = tsdbc_query_raw(r->c, r->id, r->lo, r->hi, fl_cb, r);
    r->ms = now_ms() - t0;
    atomic_store(&r->done, 1);
    return NULL;
}

#define FL_BIG 6000000
#define FL_T0 1700000000000LL

/* n rows of id from FL_T0, 10 ms apart, in calls of 500,000, on the leader */
static void fl_fill(tsdbc *c, tsdb_sid id, size_t n)
{
    size_t step = 500000;
    int64_t *ts = malloc(step * sizeof *ts);
    float *v = malloc(step * sizeof *v);
    if (!ts || !v)
        die("out of memory");
    for (size_t k = 0; k < n; k += step) {
        size_t m = n - k < step ? n - k : step;
        for (size_t i = 0; i < m; i++)
            ts[i] = FL_T0 + (int64_t)(k + i) * 10, v[i] = (float)(k + i);
        int rc = tsdbc_insert_series(c, id, ts, v, m);
        if (rc != 0)
            die("loading %zu rows: %s", n, ers(rc));
    }
    free(ts);
    free(v);
}

static int fl_cmp(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

struct fl_peer {
    int s;              /* entry server */
    tsdb_sid big;       /* the other volume's big series (read) */
    char vol[16];       /* the other volume (written) */
    int k, rc, iters;
    const char *what;
    atomic_int *done;
};

static void *fl_peer_run(void *arg)
{
    struct fl_peer *p = arg;
    tsdbc *c = client(p->s);
    char nm[48];
    snprintf(nm, sizeof nm, "g.%d.%d", p->s, p->k);
    tsdb_sid id = rep_series(c, nm, p->vol, 30000);
    int64_t ts[100];
    float v[100];
    for (int it = 0; it < 200 && !p->rc; it++, p->iters++) {
        for (int i = 0; i < 100; i++)
            ts[i] = FL_T0 + (int64_t)(it * 100 + i) * 1000, v[i] = (float)i;
        if ((p->rc = tsdbc_insert_series(c, id, ts, v, 100)) != 0) {
            p->what = "insert";
            break;
        }
        if (it % 40 == 20) { /* 400,000 rows: 4.8 MB, five chunks, past the first credit of four */
            struct fl_read r = {.c = c, .id = p->big, .lo = FL_T0 + (int64_t)p->k * 1000000, .hi = 0};
            r.hi = r.lo + 400000LL * 10;
            fl_read_run(&r);
            if ((p->rc = r.rc) == 0 && r.rows != 400000)
                p->rc = -1000000 - (int)r.rows;
            if (p->rc)
                p->what = "read";
        }
    }
    tsdbc_close(c);
    atomic_fetch_add(p->done, 1);
    return NULL;
}

static void scen_fwdload(void)
{
    scenario("fwdload");
    strict_reads = 1;
    fwd_on = 1;
    const char *mut = getenv("TSDBD_FWD_MUTANT");
    static char menv[160];
    snprintf(menv, sizeof menv, "TSDBD_FWD_MUTANT=%s", mut ? mut : "");
    for (int i = 0; i < 3; i++) {
        penv[i] = menv;
        xargs[i] = "--workers 2 --fwd-workers 1";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    int X = -1, Y = -1, rc;
    char vx[16] = "", vy[16] = "";
    for (int k = 0; k < 12 && Y < 0; k++) {
        char nm[16];
        uint32_t num;
        struct meta_volume s;
        snprintf(nm, sizeof nm, "ld%d", k);
        if ((rc = vol_add(0, nm, 1, &num)) != 0)
            die("volume add %s: %s", nm, cluster_strerror(rc));
        if (!vol_rec(0, nm, &s))
            die("no record of %s", nm);
        int l = server_of(s.leader);
        await_open_gen(0, nm, l, 1, 0);
        if (X < 0)
            X = l, snprintf(vx, sizeof vx, "%s", nm);
        else if (l != X)
            Y = l, snprintf(vy, sizeof vy, "%s", nm);
    }
    if (Y < 0)
        die("12 volumes of one replica, all led on server %d", X);

    tsdbc *cx = client(X), *cy = client(Y);
    tsdb_sid bigx = rep_series(cx, "f.big", vx, 30000), bigy = rep_series(cy, "g.big", vy, 30000);
    tsdb_sid small = rep_series(cx, "f.small", vx, 30000);
    fl_fill(cx, bigx, FL_BIG);
    fl_fill(cy, bigy, 4 * 1000000 / 10 + 400000); /* every thread's window of g (k * 1e6 ms + 400,000 rows) */
    tsdbc_close(cx);
    tsdbc_close(cy);

    /* f. the slow reader and the insert stream, both through Y to X */
    struct fl_read r = {.c = client(Y), .id = bigx, .lo = 0, .hi = INT64_MAX / 2, .slow = 1};
    pthread_t th;
    if (pthread_create(&th, NULL, fl_read_run, &r) != 0)
        die("pthread_create");
    usleep(700000);
    tsdbc *ci = client(Y);
    uint64_t lat[4096];
    size_t nl = 0;
    for (int64_t t = FL_T0; nl < sizeof lat / sizeof lat[0]; t += 1000) {
        float v = 1;
        uint64_t t0 = now_ms();
        rc = tsdbc_insert_series(ci, small, &t, &v, 1);
        lat[nl++] = now_ms() - t0;
        if (rc != 0)
            die("insert %zu through %d to %d beside a slow reader: %s after %" PRIu64 " ms", nl, Y, X, ers(rc),
                lat[nl - 1]);
        if (atomic_load(&r.done))
            break;
        usleep(5000);
    }
    pthread_join(th, NULL);
    tsdbc_close(r.c);
    tsdbc_close(ci);
    if (r.rc != 0 || r.rows != FL_BIG)
        die("the slow reader through %d: %s, %" PRIu64 " rows of %d", Y, ers(r.rc), r.rows, FL_BIG);
    if (r.ms < 3000)
        die("the slow reader took %" PRIu64 " ms: not slow enough to hold the owner's worker", r.ms);
    qsort(lat, nl, sizeof lat[0], fl_cmp);
    uint64_t p99 = lat[nl * 99 / 100], mx = lat[nl - 1];
    if (nl < 50)
        die("only %zu inserts beside the slow reader", nl);
    if (p99 > 300)
        die("inserts through %d to %d beside a slow reader (%" PRIu64 " ms, --fwd-workers 1): p99 %" PRIu64
            " ms, max %" PRIu64 " ms over %zu",
            Y, X, r.ms, p99, mx, nl);
    printf("  a slow reader of %d rows through %d (%" PRIu64 " ms) beside %zu inserts to the same owner %d: insert "
           "p50 %" PRIu64 " ms, p99 %" PRIu64 " ms, max %" PRIu64 " ms\n",
           FL_BIG, Y, r.ms, nl, X, lat[nl / 2], p99, mx);

    /* g. X and Y forward to each other: writes to the other's volume, reads of 5 chunks from it */
    atomic_int done = 0;
    struct fl_peer pp[8];
    pthread_t pt[8];
    for (int k = 0; k < 8; k++) {
        int s = k < 4 ? X : Y;
        pp[k] = (struct fl_peer){.s = s, .k = k % 4, .big = s == X ? bigy : bigx, .done = &done};
        snprintf(pp[k].vol, sizeof pp[k].vol, "%s", s == X ? vy : vx);
        if (pthread_create(&pt[k], NULL, fl_peer_run, &pp[k]) != 0)
            die("pthread_create");
    }
    uint64_t t0 = now_ms();
    while (atomic_load(&done) < 8) {
        if (now_ms() - t0 > 90000) {
            for (int k = 0; k < 8; k++)
                fprintf(stderr, "  client %d on %d: %d of 200 rounds\n", k, pp[k].s, pp[k].iters);
            die("servers %d and %d forwarding to each other (--workers 2) did not finish in 90 s", X, Y);
        }
        usleep(20000);
    }
    uint64_t took = now_ms() - t0;
    for (int k = 0; k < 8; k++) {
        pthread_join(pt[k], NULL);
        if (pp[k].rc)
            die("client %d on %d forwarding to %d: its %s after %d rounds: %s", k, pp[k].s, pp[k].s == X ? Y : X,
                pp[k].what, pp[k].iters, pp[k].rc < -1000000 ? "a short read" : ers(pp[k].rc));
    }
    stop_all();
    printf("  servers %d and %d forwarding to each other (8 clients, --workers 2): 1,600 inserts and 40 reads of 4.8 MB "
           "in %" PRIu64 " ms\n",
           X, Y, took);
}


/* promoteslow (review F3): a promotion that takes longer than the 3 s after which the Raft leader takes a record leader
 * whose driver does not lead for deposed (leader_deposed: a fence nobody followed up). Every server's swap to the writer
 * waits 5 s first (TSDB_REPLD_TEST=promoteslow: a reopen of a large volume or on a loaded host; jepsen saw 7.7 s). The
 * new leader reports PROMOTING to the record's epoch meanwhile, so the record stays at e0 + 1 and that leader takes
 * writes; mutant promotenone (role NONE while promoting) fences it again and the record moves on to e0 + 2 and beyond
 * (the volume's first promotion is as slow, so under the mutant it may get no writer at all). The swap's 5 s in the
 * volume thread are the thread's own silence, not its peers': the new leader must not ask for an ISR of one after it
 * (repld.c long_step), which the group takes for a leader cut from its replicas and moves away. */
/* tsdbc-bench via (S2-2): the bench of the forwarding hop runs end to end against three members, a volume of three
 * replicas, once through its leader and once through a replica, every write and read checked by the bench itself and
 * the leader holding every row at the end; the entry node must have forwarded frames (STATUS), the numbers must be
 * printed (hop, mean and sd over the reps, CPU per 1000 requests of the three processes). A bench that is given an
 * entry that is no server must fail, not print numbers. */
static void scen_viabench(void)
{
    scenario("viabench");
    strict_reads = 1;
    fwd_on = 1;
    for (int i = 0; i < 3; i++) {
        xargs[i] = "--workers 2";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    struct meta_volume v;
    int rc;
    if ((rc = vol_add(0, "east", 3, &num)) != 0)
        die("volume add east: %s", cluster_strerror(rc));
    if (!vol_rec(0, "east", &v))
        die("no record of east");
    for (uint32_t k = 0; k < v.nrep; k++)
        await_open_gen(0, "east", server_of(v.rep[k].node), 1, 0);
    await_isr(0, "east", 3, 20000, "east with every replica up");
    int L = server_of(v.leader), R = (L + 1) % 3;
    tsdbc *cr = client(R);
    uint64_t f0 = fw_frames(cr, v.leader);

    const char *bench = getenv("TSDBC_BENCH") ? getenv("TSDBC_BENCH") : "./build/tsdbc-bench";
    char cmd[PATH_MAX + 512], line[1024], text[16384];
    snprintf(cmd, sizeof cmd,
             "%s via --host 127.0.0.1 --port %d --via 127.0.0.1:%d --volume east --conns 2 --rows 50 --seconds 1 "
             "--reps 2 --pids %d,%d,%d 2>&1",
             bench, cports[L], cports[R], (int)pids[0], (int)pids[1], (int)pids[2]);
    FILE *p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    size_t o = 0;
    text[0] = 0;
    while (fgets(line, sizeof line, p))
        o += (size_t)snprintf(text + o, o < sizeof text ? sizeof text - o : 0, "%s", line);
    int st = pclose(p);
    if (st != 0)
        die("`%s` failed (status %d):\n%s", cmd, st, text);
    static const char *const must[] = {"rep 1 ", "rep 2 ", "direct", " via ", "hop (via - direct)", "write p50 (ms)",
                                       "read p99 (ms)", "requests a second", "CPU ms per 1000 requests",
                                       "the leader holds them all"};
    for (unsigned k = 0; k < sizeof must / sizeof *must; k++)
        if (!strstr(text, must[k]))
            die("the bench's output lacks '%s':\n%s", must[k], text);
    uint64_t f1 = fw_frames(cr, v.leader);
    if (f1 <= f0 + 10)
        die("the bench ran through server %d and it forwarded %" PRIu64 " frames to the leader %d (STATUS "
            "peer.<id>.fwd.frames %" PRIu64 " to %" PRIu64 ")", R, f1 - f0, L, f0, f1);
    tsdbc_close(cr);
    printf("  tsdbc-bench via through server %d to the leader %d: %" PRIu64 " frames forwarded; its output:\n%s", R, L,
           f1 - f0, text);

    /* an entry that is no server: an error, no numbers */
    snprintf(cmd, sizeof cmd,
             "%s via --host 127.0.0.1 --port %d --via 127.0.0.1:1 --volume east --conns 1 --seconds 1 --reps 1 2>&1",
             bench, cports[L]);
    p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    o = 0;
    text[0] = 0;
    while (fgets(line, sizeof line, p))
        o += (size_t)snprintf(text + o, o < sizeof text ? sizeof text - o : 0, "%s", line);
    st = pclose(p);
    if (st == 0 || strstr(text, "hop (via - direct)"))
        die("the bench against an entry that is no server went well (status %d):\n%s", st, text);
    printf("  an entry that is no server: %s", text);

    /* an entry that forwards nothing (the leader itself): its numbers would be "direct" under the name "via" (review
     * F7): the bench must say so and print no hop */
    snprintf(cmd, sizeof cmd,
             "%s via --host 127.0.0.1 --port %d --via 127.0.0.1:%d --volume east --conns 1 --seconds 1 --reps 2 2>&1",
             bench, cports[L], cports[L]);
    p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    o = 0;
    text[0] = 0;
    while (fgets(line, sizeof line, p))
        o += (size_t)snprintf(text + o, o < sizeof text ? sizeof text - o : 0, "%s", line);
    st = pclose(p);
    if (st == 0 || strstr(text, "hop (via - direct)") || !strstr(text, "forwarded 0 frames"))
        die("the bench against an entry that forwards nothing went well or said nothing (status %d):\n%s", st, text);
    printf("  an entry that forwards nothing: refused\n");
}

static void scen_promoteslow(void)
{
    scenario("promoteslow");
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=promoteslow";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "ps", 3, &num)) != 0 || num != 1)
        die("volume add ps: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "ps", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "ps", &v))
        die("no record of ps");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of ps is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "ps", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    tsdbc_close(c);
    await_isr(0, "ps", 3, 30000, "every replica in the ISR before the kill");
    signal_child(pids[L], SIGKILL);
    if (await_exit(L, 5000) < 0)
        die("server %d did not die of SIGKILL", L);
    int via = (L + 1) % 3;
    int Nw = await_promoted(via, "ps", ids[L], e0, &v);
    if (Nw < 0 || Nw == L || v.epoch != e0 + 1)
        die("the new record of ps: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    uint64_t tr = now_ms();
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", e0 + 1);
    await_log_count(Nw, lead, 1, 30000);
    uint64_t tl = now_ms();
    /* past the 5 s of the swap and the 3 s of leader_deposed, with margin: the record must not have moved on */
    while (now_ms() - tr < 12000) {
        struct meta_volume w;
        if (vol_rec(via, "ps", &w) && (w.epoch != e0 + 1 || w.leader != v.leader))
            die("the record of ps moved past epoch %" PRIu64 " (leader %u): epoch %" PRIu64 ", leader %u, %" PRIu64
                " ms after the promotion; a slow promotion was taken for a deposed leader", e0 + 1, v.leader, w.epoch,
                w.leader, now_ms() - tr);
        usleep(100000);
    }
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    ts = 2000;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the slowly promoted leader %d: %s", Nw, ers(rc));
    tsdbc_close(c);
    printf("  a promotion of %" PRIu64 " ms (5 s of it before the swap): the record stayed at epoch %" PRIu64
           " and server %d took writes\n", tl - tr, e0 + 1, Nw);
}


/* openread (r3e fixer, jepsen kill-classes): a restarted server opens a replicated volume's engine as a replica at REPL's
 * floor (repld_open) a moment before the volume manager counts the volume open; a restarted leader's floor lies below
 * what it acknowledged before the kill, and a read it answered in that window went back in time (stale read and
 * monotonic reads in jepsen-cluster). TSDB_REPLD_TEST=openslow holds the window open for 3 s: a raw query and a
 * QUERY_LATEST on that server meanwhile are refused (UNAVAILABLE, RECOVERING), never answered. */
static void scen_openread(void)
{
    scenario("openread");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "or", 3, &num)) != 0 || num != 1)
        die("volume add or: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "or", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "or", &v))
        die("no record of or");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of or is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "or", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every replica up: %s", ers(rc));
    tsdbc_close(c);
    await_isr(0, "or", 3, 30000, "every replica in the ISR before the restart");
    int R = (L + 1) % 3;
    signal_child(pids[R], SIGKILL);
    if (await_exit(R, 5000) < 0)
        die("server %d did not die of SIGKILL", R);
    penv[R] = "TSDB_REPLD_TEST=openslow";
    start(R);
    penv[R] = NULL;
    await_log_count(R, "(openslow)", 1, 30000);
    c = client(R);
    tsdb_points p;
    rc = tsdbc_query_points(c, id, INT64_MIN, INT64_MAX, &p);
    if (rc == 0)
        die("a raw query on server %d while its volume opens: answered (%zu rows) from the engine at REPL's floor", R,
            p.n);
    expect_unavail(c, rc, (uint16_t)num, TSDB_WHY_RECOVERING, "a raw query while the volume opens");
    tsdb_latest_result lr;
    rc = tsdbc_latest(c, &id, 1, NULL, &lr);
    if (rc == 0 && lr.status == 0)
        die("QUERY_LATEST on server %d while its volume opens: answered", R);
    expect_unavail(c, rc, (uint16_t)num, TSDB_WHY_RECOVERING, "QUERY_LATEST while the volume opens");
    tsdbc_close(c);
    printf("  server %d refused a raw query and QUERY_LATEST while its replica engine was open but the volume was not\n",
           R);
}

/* openreadlead (int-r review F2): openread on the production path (strict reads, D5) and for the LEADER: L, which
 * acknowledged writes, is killed and restarted with TSDB_REPLD_TEST=openslow, so its engine is open as a replica at
 * REPL's floor (below what L acknowledged) for 3 s before the manager counts the volume open. A raw query and a
 * QUERY_LATEST on L meanwhile go through route_decide (route_fill: UNAVAILABLE RECOVERING where the copy still names
 * L, MOVED where it names the leader that replaced it) and must never be answered from that engine. */
static void scen_openreadlead(void)
{
    scenario("openreadlead");
    strict_reads = 1;
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "ol", 3, &num)) != 0 || num != 1)
        die("volume add ol: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "ol", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "ol", &v))
        die("no record of ol");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of ol is node %u", v.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "ol", 30000);
    int acked = 0;
    for (; acked < 50; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(0, "ol", 3, 30000, "every replica in the ISR before the restart");
    signal_child(pids[L], SIGKILL);
    if (await_exit(L, 5000) < 0)
        die("server %d did not die of SIGKILL", L);
    penv[L] = "TSDB_REPLD_TEST=openslow";
    start(L);
    penv[L] = NULL;
    await_log_count(L, "(openslow)", 1, 30000);
    c = client(L);
    tsdbc_set_retry(c, 0); /* the first answer, not a followed one */
    tsdb_points p;
    rc = tsdbc_query_points(c, id, INT64_MIN, INT64_MAX, &p);
    if (rc == 0)
        die("a raw query on the restarted leader %d while its volume opens: answered (%zu of %d acknowledged rows) "
            "from the engine at REPL's floor", L, p.n, acked);
    int why = -1;
    tsdb_unavail e;
    unsigned n = 0;
    if (rc == TSDB_ERR_UNAVAILABLE && tsdbc_unavailable(c, &e, 1, &n) == 0 && n == 1)
        why = e.reason;
    if (!(rc == TSDB_ERR_MOVED || (rc == TSDB_ERR_UNAVAILABLE && why == TSDB_WHY_RECOVERING)))
        die("a raw query on the restarted leader %d while its volume opens: %s (reason %d), not UNAVAILABLE "
            "(RECOVERING) or MOVED", L, ers(rc), why);
    const char *raw = rc == TSDB_ERR_MOVED ? "MOVED" : "UNAVAILABLE (RECOVERING)";
    tsdb_latest_result lr;
    rc = tsdbc_latest(c, &id, 1, NULL, &lr);
    if (rc == 0 && lr.status == 0)
        die("QUERY_LATEST on the restarted leader %d while its volume opens: answered", L);
    if (rc == 0)
        rc = lr.status;
    why = -1, n = 0;
    if (rc == TSDB_ERR_UNAVAILABLE && tsdbc_unavailable(c, &e, 1, &n) == 0 && n == 1)
        why = e.reason;
    if (!(rc == TSDB_ERR_MOVED || (rc == TSDB_ERR_UNAVAILABLE && why == TSDB_WHY_RECOVERING)))
        die("QUERY_LATEST on the restarted leader %d while its volume opens: %s (reason %d), not UNAVAILABLE "
            "(RECOVERING) or MOVED", L, ers(rc), why);
    tsdbc_close(c);
    printf("  restarted leader %d, its engine open at REPL's floor: a raw query %s, QUERY_LATEST %s\n", L, raw,
           rc == TSDB_ERR_MOVED ? "MOVED" : "UNAVAILABLE (RECOVERING)");
}

/* jointpause (r3e jc7, finding P): every server of a volume stopped together for 2 s (SIGSTOP: a VM pause, a host
 * that froze) and resumed. Nobody was silent but for the stop itself: the leader must not catch the 2 s up in ticks
 * (every peer then looks silent for isr_lag and it asks for an ISR of one) and no member may name a peer unreachable for
 * its own stop (a cut that deposes the leader: the pause scenario's write after MOVED got a closed connection). The
 * record keeps its epoch and its ISR version, and a write is acknowledged (mutant stallcatchup). */
static void scen_jointpause(void)
{
    scenario("jointpause");
    for (int i = 0; i < 3; i++)
        start(i);
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "jp", 3, &num)) != 0 || num != 1)
        die("volume add jp: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "jp", i, 1, 0);
    struct meta_volume v0, v;
    if (!vol_rec(0, "jp", &v0))
        die("no record of jp");
    int L = server_of(v0.leader);
    if (L < 0 || L > 2)
        die("the leader of jp is node %u", v0.leader);
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "jp", 30000);
    int64_t ts = 1000;
    float val = 1;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write with every server up: %s", ers(rc));
    await_isr(0, "jp", 3, 30000, "every replica in the ISR before the stop");
    if (!vol_rec(0, "jp", &v0))
        die("no record of jp");
    for (int round = 0; round < 3; round++) {
        for (int i = 0; i < 3; i++)
            pause_server(i, 1);
        nanosleep(&(struct timespec){2, 0}, NULL);
        for (int i = 0; i < 3; i++)
            pause_server(i, 0);
        for (uint64_t t0 = now_ms(); now_ms() - t0 < 4000; usleep(20000)) {
            for (int i = 0; i < 3; i++)
                if (vol_rec(i, "jp", &v) && (v.epoch != v0.epoch || v.isr_version != v0.isr_version || v.nisr != 3))
                    die("round %d: after a stop of every server the record of jp (via server %d) has epoch %" PRIu64
                        " (was %" PRIu64 "), ISR version %" PRIu64 " (was %" PRIu64 ") of %" PRIu32 " members: a "
                        "member took the stop for its peers' silence", round, i, v.epoch, v0.epoch, v.isr_version,
                        v0.isr_version, v.nisr);
        }
        ts = 2000 + round;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("round %d: a write to the leader after a stop of every server: %s", round, ers(rc));
    }
    tsdbc_close(c);
    stop_all();
    uint64_t wantr[REP_NAMES] = {4, 0, 0, 0};
    rep_same(wantr, "after three stops of every server");
    printf("  three stops of every server for 2 s: epoch %" PRIu64 " and ISR version %" PRIu64 " kept, writes go on\n",
           v0.epoch, v0.isr_version);
}

/* design-cluster.md 6.2 (r3e-failover, item 6): the leader of a replicated volume is killed and restarted before the
 * group sees it down (node_timeout 20 s here). Its REPL says it led epoch e, so it never resumes: its core sends
 * RESIGN(v, e), the Raft leader fences the ISR at once (reason RESIGNED, no node_timeout) and the record moves to e+1, the restarted server again
 * when it is still the best; every acknowledged write is there and the new leader takes writes. */
static void scen_resign(void)
{
    scenario("resign");
    strict_reads = 1; /* every read on the leader (D5), as in production */
    for (int i = 0; i < 3; i++)
        start(i);
    init_node_timeout_ms = 20000; /* the restart (open generation recorded, then the core) must beat detection */
    init_cluster(0, 600);
    init_node_timeout_ms = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "rs", 3, &num)) != 0 || num != 1)
        die("volume add rs: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "rs", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "rs", &v))
        die("no record of rs");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of rs is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "rs", 30000);
    int acked = 0;
    for (; acked < 50; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(0, "rs", 3, 30000, "every replica in the ISR before the restart");
    signal_child(pids[L], SIGKILL);
    if (await_exit(L, 5000) < 0)
        die("server %d did not die of SIGKILL", L);
    uint64_t tk = now_ms();
    start(L);
    uint64_t ts0 = now_ms();
    char line[96];
    snprintf(line, sizeof line, "cannot lead epoch %" PRIu64 " (led it before a restart); resigns", e0);
    await_log_count(L, line, 1, 30000);
    int via = (L + 1) % 3;
    for (;;) {
        if (vol_rec(via, "rs", &v) && v.epoch > e0)
            break;
        if (now_ms() - ts0 > 30000)
            die("no new record of rs 30 s after its leader restarted (epoch %" PRIu64 ", leader %u)", v.epoch, v.leader);
        usleep(20000);
    }
    uint64_t tr = now_ms();
    int Nw = server_of(v.leader);
    if (v.epoch != e0 + 1 || Nw < 0 || Nw > 2)
        die("the new record of rs: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    /* the RESIGN moves it, not detection: the restart usually made another server the Raft leader, which must record
     * the new incarnation (the volume opens only then) without waiting out its node_timeout of 20 s first; and the
     * restarted server, at a new peer port nobody dials, must find the others at once (its watch), not after 10 s */
    if (tr - ts0 >= 10000)
        die("the record of rs moved %" PRIu64 " ms after its leader restarted: that is detection (node_timeout 20 s), "
            "not the RESIGN", tr - ts0);
    int resigned = 0;
    for (int i = 0; i < 3; i++)
        resigned += log_count(i, "(reason 7); fences");
    if (!resigned)
        die("the record moved to epoch %" PRIu64 " but no Raft leader promoted for a RESIGN (reason 7)", v.epoch);
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    int got = rows_in(Nw, id, 1000000, 1000000 + 1000LL * acked, 30000);
    if (got != acked)
        die("the leader at epoch %" PRIu64 " (server %d) holds %d of the %d acknowledged rows", v.epoch, Nw, got, acked);
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the leader at epoch %" PRIu64 ": %s", v.epoch, ers(rc));
    tsdbc_close(c);
    printf("  leader %d restarted %" PRIu64 " ms after its kill resigned epoch %" PRIu64 ": server %d%s leads epoch %"
           PRIu64 " %" PRIu64 " ms after the restart, with all %d acknowledged rows\n", L, ts0 - tk, e0, Nw,
           Nw == L ? " (the restarted one, still the best)" : "", v.epoch, tr - ts0, acked);
    await_isr(via, "rs", 3, 60000, "every server in the ISR after the resign");
    stop_all();
    uint64_t wantr[REP_NAMES] = {0};
    wantr[0] = (uint64_t)acked + 1;
    rep_same(wantr, "after the resign");
}

/* D13 for a volume the restarted leader cannot open (r3c review F4): the leader of a replicated volume is killed, its
 * REPL removed, and it is restarted before the group sees it down (node_timeout 20 s). The manager refuses the volume
 * (MV_REFUSED: no driver, no RESIGN), and the server is UP, so only its heartbeat can tell the group: it reports the
 * volume LOCAL_FAULT, and the Raft leader promotes another replica at e+1 (reason FENCED) well before node_timeout.
 * Before, a refused volume was left out of the report and the volume had no writer for as long as the server ran. */
static void scen_refused(void)
{
    scenario("refused");
    for (int i = 0; i < 3; i++)
        start(i);
    init_node_timeout_ms = 20000;
    init_cluster(0, 600);
    init_node_timeout_ms = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "rf", 3, &num)) != 0 || num != 1)
        die("volume add rf: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "rf", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "rf", &v))
        die("no record of rf");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of rf is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "rf", 30000);
    int acked = 0;
    for (; acked < 50; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(0, "rf", 3, 30000, "every replica in the ISR before the restart");
    signal_child(pids[L], SIGKILL);
    if (await_exit(L, 5000) < 0)
        die("server %d did not die of SIGKILL", L);
    char path[2 * PATH_MAX];
    snprintf(path, sizeof path, "%s/n%d/v%" PRIu32 "/REPL", base, L, num);
    if (unlink(path) != 0)
        die("unlink %s: %s", path, strerror(errno));
    start(L);
    uint64_t ts0 = now_ms();
    await_log_count(L, "/REPL is missing: the volume is not opened", 1, 30000);
    int via = (L + 1) % 3;
    for (;;) {
        if (vol_rec(via, "rf", &v) && v.epoch > e0)
            break;
        if (now_ms() - ts0 > 15000)
            die("no new record of rf 15 s after its leader came back unable to open it (epoch %" PRIu64 ", leader %u): "
                "the refused volume was not reported", v.epoch, v.leader);
        usleep(20000);
    }
    uint64_t tr = now_ms();
    int Nw = server_of(v.leader);
    if (v.epoch != e0 + 1 || Nw < 0 || Nw > 2 || Nw == L)
        die("the new record of rf: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    int fenced = 0;
    for (int i = 0; i < 3; i++)
        fenced += log_count(i, "(reason 4); fences");
    if (!fenced)
        die("the record moved to epoch %" PRIu64 " but no Raft leader promoted for a fault (reason 4)", v.epoch);
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    int got = rows_in(Nw, id, 1000000, 1000000 + 1000LL * acked, 30000);
    if (got != acked)
        die("the leader at epoch %" PRIu64 " (server %d) holds %d of the %d acknowledged rows", v.epoch, Nw, got, acked);
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the leader at epoch %" PRIu64 ": %s", v.epoch, ers(rc));
    tsdbc_close(c);
    printf("  leader %d restarted without its REPL: refused, reported, server %d leads epoch %" PRIu64 " %" PRIu64
           " ms after the restart, with all %d acknowledged rows\n", L, Nw, v.epoch, tr - ts0, acked);
    stop_all();
}

static void cut_file(int at, int from, int on)
{
    char f[PATH_MAX + 64];
    server_dir(at, f, sizeof f);
    snprintf(f + strlen(f), sizeof f - strlen(f), "/cutfrom.%" PRIu32, ids[from]);
    if (on) {
        FILE *fp = fopen(f, "w");
        if (!fp)
            die("create %s", f);
        fclose(fp);
    } else if (unlink(f) != 0 && errno != ENOENT) {
        die("unlink %s", f);
    }
}

/* oneway (review F6): the case the fence-then-promote wait exists for. The volume leader L (not the Raft leader R) is
 * cut from its replicas on the data links (TSDB_REPLD_TEST=cutlinks), drops every FENCE it receives and hears no
 * volume record (TSDB_CLUSTER_TEST drop_fence_file=1), while its heartbeats still reach R: it stays UP, holds a lease
 * and answers its clients. R moves the volume (the replicas report losing L) and may name the new leader only once L
 * answered the fence or L's last lease has ended, and gives L no lease meanwhile; the move does not give up at
 * PROMO_MOVE_NS while a replica answered (it did, and the ISR shrank to L alone with no new leader). A reader on L logs every read; once the new leader
 * acknowledged its first write, no read sent to L after that ack may be answered (a stale read: L does not have the
 * row). Mutants: fencelease (L keeps its lease: no promotion within 30 s), oldpass (the promotion goes ahead at once: L
 * answers reads after the ack). */
struct ow_read {
    uint64_t sent;
    int rc, kind, rows;
};
struct ow_reader {
    int server;
    tsdb_sid id;
    struct ow_read *log;
    size_t n, cap;
    int stop; /* __atomic */
};
static void *ow_reader_thread(void *a)
{
    struct ow_reader *r = a;
    tsdbc *c = client(r->server);
    for (int k = 0; r->n < r->cap && !__atomic_load_n(&r->stop, __ATOMIC_RELAXED); k++) {
        struct ow_read *e = &r->log[r->n];
        e->kind = k % 3;
        e->sent = now_ms();
        e->rc = lease_read(c, r->id, e->kind, &e->rows);
        r->n++;
        usleep(200);
    }
    tsdbc_close(c);
    return NULL;
}

/* L leads `name` at e0 with `acked` rows of `id`, every server in its ISR: cut it one way (above), read on it through
 * the promotion and check that no read sent after the new leader's first acknowledged write is answered; then the links
 * come back and the new leader holds every row. replay: L sends again the fence answers it gave while it was promoted
 * (oneway2). */
static void ow_cut_promote(int L, const char *name, tsdb_sid id, int acked, uint32_t num, uint64_t e0, uint32_t old,
                           int replay)
{
    struct meta_volume v;
    int rc;
    struct ow_reader rd = {.server = L, .id = id, .cap = 1u << 20};
    if (!(rd.log = calloc(rd.cap, sizeof *rd.log)))
        die("out of memory");
    pthread_t th;
    if (pthread_create(&th, NULL, ow_reader_thread, &rd) != 0)
        die("pthread_create");
    char df[PATH_MAX + 64];
    server_dir(L, df, sizeof df);
    strcat(df, "/state/dropfence");
    FILE *fp = fopen(df, "w");
    if (!fp)
        die("create %s: %s", df, strerror(errno));
    fclose(fp);
    char rf[PATH_MAX + 64];
    server_dir(L, rf, sizeof rf);
    strcat(rf, "/state/replayreply");
    if (replay) {
        if (!(fp = fopen(rf, "w")))
            die("create %s: %s", rf, strerror(errno));
        fclose(fp);
    }
    uint64_t t0 = now_ms();
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 1);
            cut_file(i, L, 1);
        }
    int Nw = await_promoted(0, name, old, e0, &v);
    uint64_t tp = now_ms();
    if (Nw == L || Nw < 0 || Nw > 2)
        die("the record of %s after the cut names node %u at epoch %" PRIu64, name, v.leader, v.epoch);
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    tsdbc *cn = client(Nw);
    tsdbc_set_retry(cn, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(cn, id, &ts, &val, 1)) != 0)
        die("the first write to the leader at epoch %" PRIu64 " (server %d): %s", v.epoch, Nw, ers(rc));
    uint64_t tack = now_ms();
    tsdbc_close(cn);
    usleep(2000000); /* reads on L go on past the ack */
    __atomic_store_n(&rd.stop, 1, __ATOMIC_RELAXED);
    pthread_join(th, NULL);
    size_t after = 0, answered = 0;
    uint64_t last_ok = 0;
    for (size_t i = 0; i < rd.n; i++) {
        const struct ow_read *e = &rd.log[i];
        if (e->rc == 0) {
            answered++;
            last_ok = e->sent;
            int want = e->kind == 1 ? 1 : acked;
            if (e->rows != want)
                die("a read on the old leader (kind %d) %" PRIu64 " ms after the cut answered %d rows, not %d", e->kind,
                    e->sent - t0, e->rows, want);
        } else if (e->rc != TSDB_ERR_UNAVAILABLE && e->rc != TSDB_ERR_MOVED) {
            die("a read on the old leader %" PRIu64 " ms after the cut: %s", e->sent - t0, ers(e->rc));
        }
        if (e->sent > tack) {
            after++;
            if (e->rc == 0)
                die("a read sent to the old leader %" PRIu64 " ms after the new leader's first acknowledged write was "
                    "answered (%" PRIu64 " ms after the cut, promotion seen at %" PRIu64 " ms): a stale read",
                    e->sent - tack, e->sent - t0, tp - t0);
        }
    }
    if (!after || !answered)
        die("%zu reads on the old leader after the ack, %zu answered in all (%zu reads): the test saw nothing", after,
            answered, rd.n);
    free(rd.log);
    unlink(df); /* L hears the record again: a write is MOVED */
    unlink(rf);
    tsdbc *co = client(L);
    insert_expect_moved(co, id, 9500000, 1, (uint16_t)num, v.leader, v.epoch, "a write to the old leader");
    tsdbc_close(co);
    printf("  leader %d cut from its replicas, fences dropped, heartbeats whole: server %d leads epoch %" PRIu64
           " %" PRIu64 " ms after the cut; %zu reads on %d, the last answered at %" PRIu64 " ms, none of %zu after the "
           "new leader's first ack (%" PRIu64 " ms)\n", L, Nw, v.epoch, tp - t0, rd.n, L, last_ok - t0, after,
           tack - t0);
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 0);
            cut_file(i, L, 0);
        }
    int got = rows_in(Nw, id, 1000000, 9000001, 30000);
    if (got != acked + 1)
        die("the leader at epoch %" PRIu64 " (server %d) holds %d rows, %d were acknowledged", v.epoch, Nw, got,
            acked + 1);
    await_isr(Nw, name, 3, 60000, "every server in the ISR once the links are back");
}

static void scen_oneway(void)
{
    scenario("oneway");
    strict_reads = 1; /* every read on the leader (D5): a replica's reads are not the question here */
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=cutlinks";
        envs[i] = "drop_fence_file=1";
    }
    lease_cluster(3, 5000, 200000, 3000); /* L's lease runs 4 s past its last grant; R waits 6 s */
    int L = -1, rc;
    uint32_t num = 0;
    char name[16];
    struct meta_volume v;
    for (int k = 0; k < 8 && L < 0; k++) {
        snprintf(name, sizeof name, "ow%d", k);
        if ((rc = vol_add(0, name, 3, &num)) != 0)
            die("volume add %s: %s", name, cluster_strerror(rc));
        if (!vol_rec(0, name, &v))
            die("no record of %s", name);
        int s = server_of(v.leader);
        if (s >= 1 && s <= 2)
            L = s;
    }
    if (L < 0)
        die("eight volumes, every one led by the Raft leader");
    for (int i = 0; i < 3; i++) /* every server opened it: a series made before L knows the volume is NOTFOUND */
        await_open_gen(0, name, i, 1, 0);
    uint64_t e0 = v.epoch;
    uint32_t old = v.leader;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], name, 30000);
    int acked = 0;
    for (; acked < 20; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(L, name, 3, 30000, "every replica in the ISR before the cut");
    ow_cut_promote(L, name, id, acked, num, e0, old, 0);
    stop_all();
}

/* oneway2 (review R2): a late answer to an earlier fence must not count as the old leader's answer now. Server 0 (the
 * Raft leader) leads; its data links are cut and a replica N is promoted to e0 + 1, keeping a copy of its answer to
 * fence e0 + 1 (TSDB_CLUSTER_TEST hold_reply_file=1). Once every server is back in the ISR, N is cut one way as in
 * oneway and sends that old answer again every round: the promotion to e0 + 2 still waits for N's lease. Mutant
 * anyepoch (an answer of any epoch counted) promotes at once and N answers reads after the new leader's first ack. */
static void scen_oneway2(void)
{
    scenario("oneway2");
    strict_reads = 1;
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=cutlinks";
        envs[i] = "drop_fence_file=1,hold_reply_file=1";
    }
    lease_cluster(3, 5000, 200000, 3000);
    int rc, L0 = -1;
    uint32_t num = 0;
    char name[16];
    struct meta_volume v;
    for (int k = 0; k < 8 && L0 < 0; k++) {
        snprintf(name, sizeof name, "ox%d", k);
        if ((rc = vol_add(0, name, 3, &num)) != 0)
            die("volume add %s: %s", name, cluster_strerror(rc));
        if (!vol_rec(0, name, &v))
            die("no record of %s", name);
        if (server_of(v.leader) == 0)
            L0 = 0;
    }
    if (L0 < 0)
        die("eight volumes, none led by the Raft leader");
    for (int i = 0; i < 3; i++)
        await_open_gen(0, name, i, 1, 0);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(0);
    tsdb_sid id = rep_series(c, rep_names[0], name, 30000);
    int acked = 0;
    for (; acked < 20; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(0, name, 3, 30000, "every replica in the ISR before the first cut");
    char hf[3][PATH_MAX + 64];
    for (int i = 1; i < 3; i++) {
        server_dir(i, hf[i], sizeof hf[i]);
        strcat(hf[i], "/state/holdreply");
        FILE *fp = fopen(hf[i], "w");
        if (!fp)
            die("create %s: %s", hf[i], strerror(errno));
        fclose(fp);
    }
    for (int i = 1; i < 3; i++) {
        cut_file(0, i, 1);
        cut_file(i, 0, 1);
    }
    int Nn = await_promoted(0, name, ids[0], e0, &v);
    if (Nn < 1 || Nn > 2)
        die("the record of %s after the first cut names node %u at epoch %" PRIu64, name, v.leader, v.epoch);
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nn, lead, 1, 30000);
    for (int i = 1; i < 3; i++) {
        cut_file(0, i, 0);
        cut_file(i, 0, 0);
        unlink(hf[i]);
    }
    char kept[96];
    snprintf(kept, sizeof kept, "keeps a copy of its answer to fence %" PRIu64 " of volume %" PRIu32, e0 + 1, num);
    if (log_count(Nn, kept) < 1)
        die("server %d led epoch %" PRIu64 " without keeping its answer to the fence (no \"%s\")", Nn, v.epoch, kept);
    await_isr(Nn, name, 3, 60000, "every server in the ISR after the first promotion");
    printf("  server %d leads epoch %" PRIu64 " after the first cut and replays its answer to fence %" PRIu64
           " through the second\n", Nn, v.epoch, e0 + 1);
    ow_cut_promote(Nn, name, id, acked, num, v.epoch, v.leader, 1);
    stop_all();
}

/* manystuck (review F4): cutstuck with more volumes than the proposer's old fixed tables of 16. 72 volumes; the server
 * other than the Raft leader that leads the most (more than 16) is cut from its replicas while they answer no fence: every move is given up and
 * the leader deposed in all of them at once. Once the members answer, each must be taken as resigned and get a leader
 * past its epoch: with 16 slots per table one was overwritten at random each round, and a deposed leader's 3 s timer
 * rarely ran out (the old tables: the last taken as resigned 10 s, re-led 13 s after the members answer). */
static void nofence_file(int at, int on);
static void scen_manystuck(void)
{
    scenario("manystuck");
    enum { NV = 72 };
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=cutlinks,nofencereply";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    static char names[NV][16];
    uint32_t nums[NV];
    uint64_t eps[NV];
    int lead[NV], count[3] = {0};
    int rc;
    for (int k = 0; k < NV; k++) {
        snprintf(names[k], sizeof names[k], "ms%d", k);
        if ((rc = vol_add(0, names[k], 3, &nums[k])) != 0)
            die("volume add %s: %s", names[k], cluster_strerror(rc));
    }
    for (int k = 0; k < NV; k++) {
        for (int i = 0; i < 3; i++)
            await_open_gen(0, names[k], i, 1, 0);
        struct meta_volume v;
        if (!vol_rec(0, names[k], &v))
            die("no record of %s", names[k]);
        lead[k] = server_of(v.leader);
        eps[k] = v.epoch;
        if (lead[k] < 0 || lead[k] > 2)
            die("the leader of %s is node %u", names[k], v.leader);
        count[lead[k]]++;
    }
    int L = count[2] > count[1] ? 2 : 1; /* not the Raft leader: cut, it would not move its volumes but fence them */
    if (count[L] <= 16)
        die("server %d leads %d of %d volumes, not more than 16", L, count[L], NV);
    for (int k = 0; k < NV; k++)
        if (lead[k] == L)
            await_isr(L, names[k], 3, 30000, "every replica in the ISR before the cut");
    for (int i = 0; i < 3; i++)
        if (i != L)
            nofence_file(i, 1);
    uint64_t t0 = now_ms();
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 1);
            cut_file(i, L, 1);
        }
    char line[96];
    for (int k = 0; k < NV; k++) /* every move given up and the old leader deposed, in each of its volumes */
        if (lead[k] == L) {
            snprintf(line, sizeof line, "volume %" PRIu32 ": no replica to move the leadership of epoch %" PRIu64 " to;",
                     nums[k], eps[k]);
            await_log_count(0, line, 1, 60000); /* server 0 is the Raft leader throughout */
            snprintf(line, sizeof line, "volume %" PRIu32 ": deposed at epoch %" PRIu64 "", nums[k], eps[k]);
            await_log_count(L, line, 1, 60000);
        }
    uint64_t tg = now_ms();
    for (int i = 0; i < 3; i++)
        if (i != L)
            nofence_file(i, 0);
    /* the proposer's own decision, at most DEPOSED_HOLD_NS (3 s) after each move was given up: 6 s. A new leader's
     * reopen of 24 volumes at once may take longer on a loaded host: 30 s for those */
    int left = count[L];
    uint64_t taken[NV] = {0}, tlast = 0;
    for (; left > 0; usleep(100000)) {
        for (int k = 0; k < NV; k++) {
            if (lead[k] != L || taken[k])
                continue;
            snprintf(line, sizeof line, "volume %" PRIu32 ": leader %" PRIu32 " of epoch %" PRIu64 " is up but",
                     nums[k], ids[L], eps[k]);
            if (log_count(0, line) > 0) {
                taken[k] = tlast = now_ms();
                left--;
            }
        }
        if (left > 0 && now_ms() - tg > 6000) {
            for (int k = 0; k < NV; k++)
                if (lead[k] == L && !taken[k])
                    die("%d of the %d volumes server %d led are not taken as its resignation 6 s after the members "
                        "answer again (%s among them): the proposer lost their deposed time", left, count[L], L,
                        names[k]);
        }
    }
    left = count[L];
    uint64_t done[NV] = {0};
    for (; left > 0; usleep(100000)) {
        for (int k = 0; k < NV; k++) {
            struct meta_volume v;
            if (lead[k] != L || done[k] || !vol_rec(0, names[k], &v) || v.epoch <= eps[k])
                continue;
            int Nw = server_of(v.leader);
            snprintf(line, sizeof line, "volume %" PRIu32 ": leads epoch %" PRIu64 " ", nums[k], v.epoch);
            if (Nw >= 0 && Nw <= 2 && log_count(Nw, line) > 0) {
                done[k] = now_ms();
                left--;
            }
        }
        if (left > 0 && now_ms() - tg > 30000) {
            for (int k = 0; k < NV; k++)
                if (lead[k] == L && !done[k])
                    die("%d of the %d volumes server %d led have no leader past their epoch 30 s after the members "
                        "answer again (%s among them)", left, count[L], L, names[k]);
        }
    }
    uint64_t last = 0;
    for (int k = 0; k < NV; k++)
        if (done[k] > last)
            last = done[k];
    printf("  server %d led %d of %d volumes; cut, every move given up, deposed in all %" PRIu64 " ms after the cut; "
           "each taken as resigned, the last %" PRIu64 " ms, and has a new leader, the last %" PRIu64 " ms after the "
           "members answer again\n", L, count[L], NV, tg - t0, tlast - tg, last - tg);
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 0);
            cut_file(i, L, 0);
        }
    stop_all();
}

/* A leader cut from its replicas on the data links (TSDB_REPLD_TEST=cutlinks; the peer port, heartbeats and the Raft
 * group stay whole) asks to shrink the ISR to itself; the Raft leader, which hears the replicas and reads in their
 * heartbeats that they lost the leader, moves the leadership to one of them instead (design-cluster.md 6.1): no ISR of
 * one member at the old epoch, a replica leads e+1 with every acknowledged row, a write to the old leader is MOVED, and
 * the old leader rejoins once the links are back. TSDB_CLUSTER_MUTANT=cutshrinks (the shrink proposed) fails it. */
/* cut, and cuthold (review R13): the replicas report the cut leader unreachable only after 1.8 s (TSDB_CVOL_TEST
 * unreach_ms), later than the leader asks to drop them (unanswered_ms), so the Raft leader must hold that shrink
 * below 2 (cut_move, CUT_HOLD_NS) until the report comes; mutant nocuthold shrinks the ISR to the cut leader alone,
 * and no replica is promoted. */
static void cut_run(int late)
{
    for (int i = 0; i < 3; i++) {
        penv[i] = late ? "TSDB_REPLD_TEST=cutlinks TSDB_CVOL_TEST=unreach_ms=1800" : "TSDB_REPLD_TEST=cutlinks";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "rs", 3, &num)) != 0 || num != 1)
        die("volume add rs: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "rs", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "rs", &v))
        die("no record of rs");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of rs is node %u", v.leader);
    uint64_t e0 = v.epoch;
    uint32_t old = v.leader;
    tsdbc *c = client(L);
    tsdbc_set_retry(c, 0); /* the MOVED below is to be seen, not followed */
    tsdb_sid id = rep_series(c, rep_names[0], "rs", 30000);
    int acked = 0;
    for (; acked < 50; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    await_isr(L, "rs", 3, 30000, "every replica in the ISR before the cut");
    /* A replica still connecting while the 50 writes run may leave the ISR and come back: only shrinks after the cut
     * count (clus8, 2026-11-19: ISR version 2 of one member at commit point 54, before the cut). */
    char line[96];
    snprintf(line, sizeof line, "ISR of 1 members (epoch %" PRIu64 ",", e0);
    int shrunk0 = 0;
    for (int i = 0; i < 3; i++)
        shrunk0 += log_count(i, line);
    uint64_t t0 = now_ms();
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 1);
            cut_file(i, L, 1);
        }
    int Nw = await_promoted((L + 1) % 3, "rs", old, e0, &v);
    uint64_t tm = now_ms();
    if (v.epoch != e0 + 1 || Nw == L)
        die("the record of rs after the cut: epoch %" PRIu64 " (was %" PRIu64 "), leader %u", v.epoch, e0, v.leader);
    int moved = 0, shrunk = -shrunk0;
    for (int i = 0; i < 3; i++) {
        moved += log_count(i, "(reason 3); fences");
        shrunk += log_count(i, line);
    }
    if (shrunk || !moved)
        die("the cut leader's ISR: %d proposals of one member at epoch %" PRIu64 ", %d moves (want 0 and 1 at least)",
            shrunk, e0, moved);
    char lead[64];
    snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
    await_log_count(Nw, lead, 1, 30000);
    int got = rows_in(Nw, id, 1000000, 1000000 + 1000LL * acked, 30000);
    if (got != acked)
        die("the leader at epoch %" PRIu64 " (server %d) holds %d of the %d acknowledged rows", v.epoch, Nw, got, acked);
    insert_expect_moved(c, id, 5000000, 1, (uint16_t)num, v.leader, v.epoch, "a write to the cut-off old leader");
    tsdbc_close(c);
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the leader at epoch %" PRIu64 ": %s", v.epoch, ers(rc));
    tsdbc_close(c);
    printf("  leader %d cut from its replicas: server %d leads epoch %" PRIu64 " %" PRIu64 " ms after the cut, with all "
           "%d acknowledged rows; no ISR of one\n", L, Nw, v.epoch, tm - t0, acked);
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 0);
            cut_file(i, L, 0);
        }
    await_isr(Nw, "rs", 3, 60000, "every server in the ISR once the links are back");
    stop_all();
    uint64_t wantr[REP_NAMES] = {0};
    wantr[0] = (uint64_t)acked + 1;
    rep_same(wantr, "after the cut");
}

static void scen_cut(void)
{
    scenario("cut");
    cut_run(0);
}

static void scen_cuthold(void)
{
    scenario("cuthold");
    cut_run(1);
}

/* 1 if one of server i's WAL segments of volume 1 holds a row with timestamp ts (rows are raw: ts at byte 4 of each) */
static int wal_holds_ts(int i, int64_t ts)
{
    char dir[PATH_MAX + 80];
    server_dir(i, dir, sizeof dir);
    strcat(dir, "/v1");
    DIR *d = opendir(dir);
    if (!d)
        return 0;
    int found = 0;
    struct dirent *e;
    while (!found && (e = readdir(d))) {
        if (strncmp(e->d_name, "wal_", 4) != 0)
            continue;
        char f[PATH_MAX + 400];
        snprintf(f, sizeof f, "%s/%s", dir, e->d_name);
        FILE *fp = fopen(f, "rb");
        if (!fp)
            continue;
        static uint8_t buf[8 << 20];
        size_t n = fread(buf, 1, sizeof buf, fp);
        fclose(fp);
        found = memmem(buf, n, &ts, sizeof ts) != NULL;
    }
    closedir(d);
    return found;
}

/* deposetail (jepsen-cluster pause, seed 123, 2026-11-22): a deposed writer whose log runs past the commit point, with
 * a record of its uncommitted tail written only after its demote began, rejoins as a replica. L's replicas hold R1,
 * whose acknowledgements L never heard (cutfrom files at L), so the new leader's epoch e0 ends above L's commit floor;
 * R2 is admitted at L once it is cut both ways, and its bytes reach op_bytes only after the demote cleared the index
 * (TSDB_REPLD_TEST=latebytes: held there until the driver has no core, as a write held in the WAL writer of a stopped
 * process was). The demote's reopen must cut R2 (the new leader never had it) back to the new leader's end of e0, not
 * stop the volume ("the log does not retain a position above its applied end": the stale entry of R2 left at the front
 * of the rebuilt index of record starts), and L must catch up and join the ISR again, holding R1 (committed by the new
 * leader) and every row written there, and not R2. TSDB_REPLD_MUTANT=demotestaleidx (the index rebuilt over what the
 * old engine pushed in the window) fails it. */
static void scen_deposetail(void)
{
    scenario("deposetail");
    init_isr_lag_ms = 2000; /* L, deaf to its replicas between R1 and the full cut, asks to shrink only after this */
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=cutlinks,latebytes";
        start(i);
    }
    init_cluster(0, 600);
    init_isr_lag_ms = 0;
    node_add(0, 1);
    node_add(0, 2);
    unsigned want3 = 3;
    await_view(0, p_voters, &want3, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "rs", 3, &num)) != 0 || num != 1)
        die("volume add rs: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "rs", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "rs", &v))
        die("no record of rs");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of rs is node %u", v.leader);
    uint64_t e0 = v.epoch;
    uint32_t old = v.leader;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "rs", 30000);
    int acked = 0;
    for (; acked < 50; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(L, "rs", 3, 30000, "every replica in the ISR before the cut");
    /* R1: reaches both replicas, its acknowledgements never reach L */
    for (int i = 0; i < 3; i++)
        if (i != L)
            cut_file(L, i, 1);
    const int64_t r1 = 7300000123LL, r2 = 7400000321LL;
    struct rep_writer w1 = {.port = cports[L], .id = id, .t0 = r1, .count = 1};
    pthread_t t1;
    if (pthread_create(&t1, NULL, rep_write_thread, &w1) != 0)
        die("pthread_create");
    for (uint64_t t0 = now_ms(); !(wal_holds_ts((L + 1) % 3, r1) && wal_holds_ts((L + 2) % 3, r1)); usleep(2000))
        if (now_ms() - t0 > 10000)
            die("R1 did not reach both replicas' logs in 10 s");
    /* R2: L cut both ways, then written at L and held before op_bytes indexes it */
    for (int i = 0; i < 3; i++)
        if (i != L)
            cut_file(i, L, 1);
    char lb[PATH_MAX + 96];
    server_dir(L, lb, sizeof lb);
    strcat(lb, "/latebytes");
    FILE *fp = fopen(lb, "w");
    if (!fp || fclose(fp) != 0)
        die("%s: %s", lb, strerror(errno));
    struct rep_writer w2 = {.port = cports[L], .id = id, .t0 = r2, .count = 1};
    pthread_t t2;
    if (pthread_create(&t2, NULL, rep_write_thread, &w2) != 0)
        die("pthread_create");
    await_log_count(L, "are held until the writer is deposed", 1, 10000);
    int Nw = await_promoted((L + 1) % 3, "rs", old, e0, &v);
    if (Nw == L)
        die("the record of rs after the cut: leader %u, epoch %" PRIu64, v.leader, v.epoch);
    await_log_count(L, "released by the demote", 1, 30000);
    await_log_count(L, ": a replica again", 1, 30000);
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 0);
            cut_file(i, L, 0);
        }
    uint64_t th = now_ms();
    for (int k = 0; k < 2; k++) { /* both of L's writes end without an acknowledgement: deposed, or their link closed */
        struct rep_writer *w = k ? &w2 : &w1;
        for (uint64_t t0 = now_ms(); !__atomic_load_n(&w->done, __ATOMIC_ACQUIRE); usleep(10000))
            if (now_ms() - t0 > 30000)
                die("R%d on the deposed leader was not answered in 30 s", k + 1);
        pthread_join(k ? t2 : t1, NULL);
        if (w->rc == 0)
            die("R%d was acknowledged by the deposed leader", k + 1);
    }
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    int more = 5;
    for (int k = 0; k < more; k++) {
        int64_t ts = 9000000 + 1000LL * k;
        float val = (float)k;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d to the leader at epoch %" PRIu64 ": %s", k, v.epoch, ers(rc));
    }
    tsdbc_close(c);
    await_isr(Nw, "rs", 3, 30000, "the deposed writer back in the ISR once the links are back");
    if (log_count(L, "stopped:"))
        die("the deposed writer stopped its volume");
    printf("  writer %d deposed with R2 written after its demote began: a replica again, back in the ISR %" PRIu64
           " ms after the heal\n", L, now_ms() - th);
    stop_all();
    uint64_t wantr[REP_NAMES] = {0};
    wantr[0] = (uint64_t)acked + 1 + (uint64_t)more; /* R1 committed by the new leader, R2 cut */
    rep_same(wantr, "after the deposed writer rejoined");
}

/* readdepose (int-r review F1): a read on the leader L of a replicated volume, admitted under its lease and the
 * volume led here, then held before its snapshot (TSDBD_TEST_READ_STALL), while L is deposed: cut from its replicas on
 * the data links (TSDB_REPLD_TEST=cutlinks), the group moves the volume and L, fenced, reopens its engine as a
 * replica at REPL's floor ("a replica again"). L's lease goes on (its heartbeats reach the Raft leader), so a check of
 * the lease alone lets the read through to the replica engine, which may lie below writes acknowledged before the
 * read was sent. Once the stall ends the read must be refused (UNAVAILABLE or MOVED), never answered: a raw query (the
 * single-volume path) and a QUERY_LATEST and a QUERY_MULTI (the ids path). Strict reads (D5), as in production.
 * TSDB_REPLD_MUTANT=leadnow (the check before the fix: a lapse on a volume led now) fails it. */
struct held_read {
    int server, kind;
    tsdb_sid id;
    int rc, rows;
    atomic_int done;
};
static void *held_read_thread(void *a)
{
    struct held_read *h = a;
    tsdbc *c = client(h->server);
    tsdbc_set_retry(c, 0); /* the first answer, not a followed one */
    h->rc = lease_read(c, h->id, h->kind, &h->rows);
    tsdbc_close(c);
    atomic_store(&h->done, 1);
    return NULL;
}

static void scen_readdepose(void)
{
    scenario("readdepose");
    strict_reads = 1; /* the production path: reads only on the leader (D5) */
    static char pe[3][PATH_MAX + 128];
    char stall[3][PATH_MAX + 64];
    for (int i = 0; i < 3; i++) {
        snprintf(stall[i], sizeof stall[i], "%s/rstall-n%d", base, i);
        unlink(stall[i]);
        snprintf(pe[i], sizeof pe[i], "TSDB_REPLD_TEST=cutlinks TSDBD_TEST_READ_STALL=%s", stall[i]);
        penv[i] = pe[i];
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "rd", 3, &num)) != 0 || num != 1)
        die("volume add rd: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "rd", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "rd", &v))
        die("no record of rd");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of rd is node %u", v.leader);
    uint64_t e0 = v.epoch;
    uint32_t old = v.leader;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "rd", 30000);
    int acked = 0;
    for (; acked < 50; acked++) { /* W0: acknowledged before any read below was sent */
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(L, "rd", 3, 30000, "every replica in the ISR before the reads");
    int got = rows_in(L, id, 1000000, 1000000 + 1000LL * acked, 30000);
    if (got != acked)
        die("the leader %d answers %d of the %d acknowledged rows before the stall", L, got, acked);
    int fd = open(stall[L], O_CREAT | O_WRONLY, 0644);
    if (fd < 0)
        die("create %s: %s", stall[L], strerror(errno));
    close(fd);
    struct held_read h[3];
    pthread_t th[3];
    for (int k = 0; k < 3; k++) {
        h[k] = (struct held_read){.server = L, .kind = k, .id = id, .rc = 1};
        atomic_init(&h[k].done, 0);
        if (pthread_create(&th[k], NULL, held_read_thread, &h[k]) != 0)
            die("pthread_create");
    }
    nanosleep(&(struct timespec){0, 500000000}, NULL);
    for (int k = 0; k < 3; k++)
        if (atomic_load(&h[k].done))
            die("read kind %d on the leader %d came back (%s) before its stall ended: not admitted", k, L,
                ers(h[k].rc));
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 1);
            cut_file(i, L, 1);
        }
    int Nw = await_promoted((L + 1) % 3, "rd", old, e0, &v);
    char deposed[64];
    snprintf(deposed, sizeof deposed, "deposed at epoch %" PRIu64 ":", e0);
    await_log_count(L, deposed, 1, 30000);
    await_log_count(L, "a replica again", 1, 30000); /* the demotion's reopen: the engine is a replica's now */
    unlink(stall[L]);
    for (int k = 0; k < 3; k++)
        pthread_join(th[k], NULL);
    static const char *const kn[3] = {"a raw query", "QUERY_LATEST", "QUERY_MULTI"};
    for (int k = 0; k < 3; k++) {
        if (h[k].rc == 0)
            die("%s admitted on the leader %d under its lease and held while it was deposed answered (%d rows%s) from "
                "the replica engine of its demotion", kn[k], L, h[k].rows,
                h[k].kind == 0 && h[k].rows < acked ? ", fewer than the acknowledged" : "");
        if (h[k].rc != TSDB_ERR_UNAVAILABLE && h[k].rc != TSDB_ERR_MOVED)
            die("%s held across the deposal of its leader got %s, not UNAVAILABLE or MOVED", kn[k], ers(h[k].rc));
    }
    printf("  reads admitted on leader %d and held across its deposal (server %d leads epoch %" PRIu64 "): refused "
           "(%s, %s, %s)\n", L, Nw, v.epoch, ers(h[0].rc), ers(h[1].rc), ers(h[2].rc));
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 0);
            cut_file(i, L, 0);
        }
    stop_all();
}

static void nofence_file(int at, int on)
{
    char f[PATH_MAX + 64];
    server_dir(at, f, sizeof f);
    snprintf(f + strlen(f), sizeof f - strlen(f), "/nofencereply");
    if (on) {
        FILE *fp = fopen(f, "w");
        if (!fp)
            die("create %s", f);
        fclose(fp);
    } else if (unlink(f) != 0 && errno != ENOENT) {
        die("unlink %s", f);
    }
}

/* cutstuck (jepsen-cluster meta,classes kill, 2026-11-19: kvr had no writer for the second half of the run): a move of
 * the leadership away from a cut leader (cut) fences every ISR member, the old leader too, which answers and is
 * deposed; when no candidate answers (TSDB_REPLD_TEST=nofencereply on the replicas: they save the fence and say
 * nothing, as a member killed before its answer) the Raft leader gives the move up, and the record still names the old
 * leader at the old epoch while its core waits for a record past its fence. The group must take that as the old
 * leader's resignation and promote again once the members answer: a leader past the old epoch, which accepts a write
 * and holds every acknowledged row. TSDB_CLUSTER_MUTANT=deposedstuck (never taken as resigned) fails it. */
static void scen_cutstuck(void)
{
    scenario("cutstuck");
    for (int i = 0; i < 3; i++) {
        penv[i] = "TSDB_REPLD_TEST=cutlinks,nofencereply";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1);
    node_add(0, 2);
    unsigned want = 3;
    await_view(0, p_voters, &want, "three voters");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "rs", 3, &num)) != 0 || num != 1)
        die("volume add rs: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "rs", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "rs", &v))
        die("no record of rs");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of rs is node %u", v.leader);
    uint64_t e0 = v.epoch;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "rs", 30000);
    int acked = 0;
    for (; acked < 50; acked++) {
        int64_t ts = 1000000 + 1000LL * acked;
        float val = (float)acked;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", acked, ers(rc));
    }
    tsdbc_close(c);
    await_isr(L, "rs", 3, 30000, "every replica in the ISR before the cut");
    for (int i = 0; i < 3; i++)
        if (i != L)
            nofence_file(i, 1);
    uint64_t t0 = now_ms();
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 1);
            cut_file(i, L, 1);
        }
    char gaveup[96], deposed[64];
    snprintf(gaveup, sizeof gaveup, "no replica to move the leadership of epoch %" PRIu64 " to;", e0);
    snprintf(deposed, sizeof deposed, "deposed at epoch %" PRIu64 ":", e0);
    for (int n = 0; n < 1;) {
        n = 0;
        for (int i = 0; i < 3; i++)
            n += log_count(i, gaveup);
        if (now_ms() - t0 > 30000)
            die("no Raft leader gave the move of the cut leader up in 30 s");
        usleep(50000);
    }
    await_log_count(L, deposed, 1, 10000);
    uint64_t tg = now_ms();
    for (int i = 0; i < 3; i++)
        if (i != L)
            nofence_file(i, 0);
    /* a leader past e0 that leads (the old one may be granted e + 1 first and moved from again while it promotes) */
    int Nw = -1;
    for (char lead[64];; usleep(50000)) {
        if (vol_rec(L, "rs", &v) && v.epoch > e0 && (Nw = server_of(v.leader)) >= 0 && Nw <= 2) {
            snprintf(lead, sizeof lead, "leads epoch %" PRIu64 " ", v.epoch);
            if (log_count(Nw, lead) > 0)
                break;
        }
        if (now_ms() - tg > 30000)
            die("rs has no leader past epoch %" PRIu64 " 30 s after the move was given up (record: leader %u at epoch "
                "%" PRIu64 "): the deposed leader is still named", e0, v.leader, v.epoch);
    }
    uint64_t tl = now_ms();
    c = client(Nw);
    tsdbc_set_retry(c, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the leader at epoch %" PRIu64 " (server %d): %s", v.epoch, Nw, ers(rc));
    tsdbc_close(c);
    int got = rows_in(Nw, id, 1000000, 1000000 + 1000LL * acked, 30000);
    if (got != acked)
        die("the leader at epoch %" PRIu64 " (server %d) holds %d of the %d acknowledged rows", v.epoch, Nw, got, acked);
    printf("  the move away from cut leader %d given up %llu ms after the cut (the old leader deposed); server %d leads "
           "epoch %" PRIu64 " %llu ms after the members answer again, with all %d acknowledged rows\n", L,
           (unsigned long long)(tg - t0), Nw, v.epoch, (unsigned long long)(tl - tg), acked);
    for (int i = 0; i < 3; i++)
        if (i != L) {
            cut_file(L, i, 0);
            cut_file(i, L, 0);
        }
    await_isr(Nw, "rs", 3, 60000, "every server in the ISR once the links are back");
    stop_all();
    uint64_t wantr[REP_NAMES] = {0};
    wantr[0] = (uint64_t)acked + 1;
    rep_same(wantr, "after the cut and the given-up move");
}

/* No ISR member up (design-cluster.md 6.2): a replica outside the ISR is up, but the group cannot promote it. Clients
 * get UNAVAILABLE(NO_OWNER) from it (not MOVED to the dead leader), the leader of the group says NO_OWNER, and
 * `tsdb cluster volume force-leader` makes the replica the leader at e + 1: its rows stand, the rows the old ISR
 * acknowledged after the cut are lost (unclean, said so), and the old members return and are cut to its history. Two
 * witnesses keep the metadata group's majority with two of the three data servers dead. */
static void scen_noowner(void)
{
    scenario("noowner");
    for (int i = 0; i < 3; i++)
        penv[i] = "TSDB_REPLD_TEST=cutlinks";
    for (int i = 3; i < 5; i++) {
        is_witness[i] = 1;
        xargs[i] = "--peer-port 0"; /* a witness takes the peer port 7071 unless told */
        char wdir[PATH_MAX + 80];
        server_dir(i, wdir, sizeof wdir);
        if (mkdir(wdir, 0755) != 0)
            die("mkdir %s: %s", wdir, strerror(errno));
    }
    for (int i = 0; i < 5; i++)
        start(i);
    init_cluster(0, 600);
    for (int i = 1; i < 5; i++)
        node_add(0, i);
    unsigned want = 5;
    await_view(0, p_voters, &want, "five voters, two of them witnesses");
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "nw", 3, &num)) != 0 || num != 1)
        die("volume add nw: %s, number %" PRIu32, cluster_strerror(rc), num);
    for (int i = 0; i < 3; i++)
        await_open_gen(0, "nw", i, 1, 0);
    struct meta_volume v;
    if (!vol_rec(0, "nw", &v))
        die("no record of nw");
    int L = server_of(v.leader);
    if (L < 0 || L > 2)
        die("the leader of nw is node %u", v.leader);
    int Cs = (L + 1) % 3, B = (L + 2) % 3;
    uint32_t old = v.leader;
    tsdbc *c = client(L);
    tsdb_sid id = rep_series(c, rep_names[0], "nw", 30000);
    enum { BEFORE = 20, AFTER = 10 };
    await_isr(L, "nw", 3, 30000, "every replica in the ISR");
    for (int j = 0; j < BEFORE; j++) {
        int64_t ts = 1000000 + 1000LL * j;
        float val = (float)j;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d: %s", j, ers(rc));
    }
    for (uint64_t t1 = now_ms(); rows_in(Cs, id, 1000000, 1000000 + 1000LL * BEFORE, 30000) != BEFORE; usleep(50000))
        if (now_ms() - t1 > 30000)
            die("server %d does not hold the %d rows written with every replica in the ISR", Cs, BEFORE);
    cut_file(L, Cs, 1);
    cut_file(Cs, L, 1);
    await_isr(L, "nw", 2, 30000, "the cut replica out of the ISR");
    for (int j = BEFORE; j < BEFORE + AFTER; j++) {
        int64_t ts = 1000000 + 1000LL * j;
        float val = (float)j;
        if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
            die("write %d with ISR {leader, server %d}: %s", j, B, ers(rc));
    }
    tsdbc_close(c);
    if (!vol_rec(Cs, "nw", &v))
        die("no record of nw");
    uint64_t e0 = v.epoch;
    { /* while an ISR member is up the group promotes by itself: force-leader is refused and moves nothing (review R6) */
        uint8_t freq[8], *frep = NULL;
        size_t flen = 0, frl = cluster_volume_force_encode(num, ids[Cs], freq);
        rc = admin(Cs, CLUSTER_OP_VOLUME_FORCE, freq, frl, 1, &frep, &flen);
        if (rc != CLUSTER_ERR_STATE)
            die("force-leader of server %d with ISR {leader, server %d} up: %s, not refused (%.*s)", Cs, B,
                cluster_strerror(rc), (int)flen, frep ? (const char *)frep : "");
        printf("  force-leader with the ISR up: refused (%.*s)\n", (int)flen, frep ? (const char *)frep : "");
        free(frep);
        if (!vol_rec(Cs, "nw", &v) || v.epoch != e0 || v.leader != old)
            die("a refused force-leader moved nw (epoch %" PRIu64 ", leader %u)", v.epoch, v.leader);
    }
    stop(L, SIGKILL);
    stop(B, SIGKILL);
    uint64_t t0 = now_ms();
    c = client(Cs);
    tsdbc_set_retry(c, 0);
    for (;; usleep(50000)) {
        int64_t ts = 8000000;
        float val = 1;
        rc = tsdbc_insert_series(c, id, &ts, &val, 1);
        tsdb_unavail e;
        unsigned ne = 0;
        if (rc == TSDB_ERR_UNAVAILABLE && tsdbc_unavailable(c, &e, 1, &ne) == 0 && ne == 1 &&
            e.reason == TSDB_WHY_NO_OWNER)
            break;
        if (rc == 0)
            die("server %d acknowledged a write with no ISR member up", Cs);
        if (now_ms() - t0 > 30000)
            die("no UNAVAILABLE(NO_OWNER) from server %d 30 s after its ISR died (last: %s)", Cs, ers(rc));
    }
    int said = 0;
    for (uint64_t t1 = now_ms(); !said; usleep(50000)) {
        for (int i = 0; i < 5; i++)
            if (i != L && i != B)
                said += log_count(i, "NO_OWNER: no ISR member answers");
        if (!said && now_ms() - t1 > 30000)
            die("the leader of the group never said NO_OWNER");
    }
    if (!vol_rec(Cs, "nw", &v) || v.epoch != e0 || v.leader != old)
        die("the record of nw moved with no ISR member up (epoch %" PRIu64 ", leader %u)", v.epoch, v.leader);
    uint8_t req[8], *reply = NULL;
    size_t len = 0;
    uint32_t cnode = ids[Cs];
    size_t rl = cluster_volume_force_encode(num, cnode, req);
    if ((rc = admin(Cs, CLUSTER_OP_VOLUME_FORCE, req, rl, 1, &reply, &len)) != 0)
        die("force-leader %" PRIu32 " %" PRIu32 ": %s (%.*s)", num, cnode, cluster_strerror(rc), (int)len,
            reply ? (const char *)reply : "");
    printf("  force-leader: %.*s\n", (int)len, (const char *)reply);
    free(reply);
    int Nw = await_promoted(Cs, "nw", old, e0, &v);
    if (Nw != Cs || v.epoch != e0 + 1)
        die("after force-leader: leader server %d at epoch %" PRIu64 " (want %d at %" PRIu64 ")", Nw, v.epoch, Cs,
            e0 + 1);
    await_isr(Cs, "nw", 1, 30000, "the dead members out of the forced leader's ISR (unanswered)");
    int logged = 0;
    for (uint64_t t1 = now_ms(); !logged; usleep(50000)) {
        for (int i = 0; i < 5; i++)
            if (i != L && i != B)
                logged += log_count(i, "it holds every write acknowledged before that shrink");
        if (!logged && now_ms() - t1 > 10000)
            die("no log line compares the forced leader's end with the last shrink's commit point");
    }
    tsdbc_set_retry(c, 10000);
    int64_t ts = 9000000;
    float val = 7;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the forced leader: %s", ers(rc));
    tsdbc_close(c);
    int got = rows_in(Cs, id, 1000000, 1000000 + 1000LL * (BEFORE + AFTER), 30000);
    if (got != BEFORE)
        die("the forced leader holds %d rows of the first %d, want exactly the %d written before the cut", got,
            BEFORE + AFTER, BEFORE);
    printf("  no ISR member up: NO_OWNER %" PRIu64 " ms after the kill; force-leader made server %d leader at epoch "
           "%" PRIu64 " with the %d rows from before the cut (the %d after it are lost, as said)\n",
           now_ms() - t0, Cs, v.epoch, BEFORE, AFTER);
    cut_file(L, Cs, 0);
    cut_file(Cs, L, 0);
    /* An old member whose REPL floor had reached rows past the forced leader's history (committed at epoch e0)
     * reopens with them applied: it cannot cut below what it applied, so it stops the volume, loudly, rather than
     * rejoin with rows the leader lacks (rebuilding it from the leader is not built). One whose floor was still at or
     * below the forced leader's start (the floor is saved after the commit point moves, and the kill came right after
     * the last acknowledgement; jc5, 2026-11-20) reopens without them, cuts its log there and rejoins: then it must
     * hold exactly the forced leader's rows, none of the lost ones. Either way per member. */
    start(L);
    start(B);
    int olds[2] = {L, B}, stopped[2] = {0, 0}, joined[2] = {0, 0};
    for (uint64_t t1 = now_ms();; usleep(50000)) {
        if (!vol_rec(Cs, "nw", &v))
            die("no record of nw");
        int left = 0;
        for (int k = 0; k < 2; k++) {
            stopped[k] = log_count(olds[k], "stopped: a cut below the applied end") > 0;
            joined[k] = 0;
            for (uint32_t j = 0; j < v.nisr; j++)
                joined[k] |= v.isr[j] == ids[olds[k]];
            if (stopped[k] && joined[k])
                die("server %d stopped at a cut below its applied end and is in the ISR", olds[k]);
            left += !stopped[k] && !joined[k];
        }
        if (!left)
            break;
        if (now_ms() - t1 > 60000)
            die("60 s after the old members returned: server %d %s, server %d %s (want each stopped or in the ISR)",
                L, stopped[0] ? "stopped" : joined[0] ? "joined" : "neither", B,
                stopped[1] ? "stopped" : joined[1] ? "joined" : "neither");
    }
    if (v.leader != ids[Cs] || v.nisr != 1u + (unsigned)joined[0] + (unsigned)joined[1])
        die("after the old members returned: leader %u, ISR of %u (want %u and the %d that rejoined)", v.leader,
            v.nisr, ids[Cs], joined[0] + joined[1]);
    c = client(Cs);
    tsdbc_set_retry(c, 10000);
    ts = 9001000;
    if ((rc = tsdbc_insert_series(c, id, &ts, &val, 1)) != 0)
        die("a write to the forced leader after the old members returned: %s", ers(rc));
    tsdbc_close(c);
    if ((got = rows_in(Cs, id, 0, INT64_MAX, 30000)) != BEFORE + 2)
        die("the forced leader holds %d rows, want %d", got, BEFORE + 2);
    for (int k = 0; k < 2; k++) {
        if (!joined[k])
            continue;
        uint64_t t1 = now_ms();
        while ((got = rows_in(olds[k], id, 0, INT64_MAX, 30000)) != BEFORE + 2 && now_ms() - t1 < 30000)
            usleep(100000);
        if (got != BEFORE + 2)
            die("server %d rejoined the forced leader's ISR and holds %d rows, want its %d", olds[k], got, BEFORE + 2);
        if ((got = rows_in(olds[k], id, 1000000 + 1000LL * BEFORE, 1000000 + 1000LL * (BEFORE + AFTER), 30000)) != 0)
            die("server %d rejoined and holds %d of the %d rows lost with the force", olds[k], got, AFTER);
    }
    printf("  the old members returned: server %d %s, server %d %s (a cut below the applied end stops; a member that "
           "had not applied past the forced leader's start cuts and rejoins without the lost rows)\n",
           L, stopped[0] ? "stopped" : "rejoined", B, stopped[1] ? "stopped" : "rejoined");
    stop_all();
}

static int rm_one(const char *path, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st, (void)flag, (void)ftw;
    return remove(path) != 0 && errno != ENOENT ? -1 : 0;
}

/* r4stubs (r4-iface): R4's operator verbs reach their stubs on the Raft leader and answer "not built" (each later track
 * replaces its line here with its own scenario), and `tsdb cluster init` stores R4's four settings, which STATUS prints
 * back; out-of-range values are refused by the CLI before anything is sent, and by the group if they get past it. */
static void scen_r4stubs(void)
{
    scenario("r4stubs");
    start(0);
    char p0[META_ADDR_MAX], args[PATH_MAX * 2 + 512], out[8192];
    peer_of(0, p0, sizeof p0);
    static const char *const bad[] = {"--rebuild-max 17",       "--rebuild-max 0",        "--rebuild-mb-s 0",
                                      "--transfer-wait-ms 99",   "--transfer-wait-ms 60001", "--leader-balance-s 86401",
                                      "--leader-balance-s x",    "--leader-balance-s -1"};
    for (size_t k = 0; k < sizeof bad / sizeof bad[0]; k++) {
        snprintf(args, sizeof args, "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 %s", p0,
                 secret_path, bad[k]);
        int rc = cli_run(args, NULL, out, sizeof out);
        if (rc != 2 || strstr(out, "initialised"))
            die("cluster init %s: exit %d (not 2): %s", bad[k], rc, out);
    }
    /* past the CLI: an INIT body with rebuild_max 17 is refused by the state machine (settings_valid) */
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 1500, s.heartbeat_ms = 200, s.rebuild_max = 17;
    uint8_t req[128], *reply = NULL;
    size_t len = 0, rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req);
    int arc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (arc == 0)
        die("an INIT with rebuild_max 17 was taken");
    printf("  out-of-range R4 settings refused (CLI exit 2; the group: %s)\n", cluster_strerror(arc));
    snprintf(args, sizeof args,
             "cluster init %s --secret-file %s --node-timeout-ms 1500 --heartbeat-ms 200 --rebuild-mb-s 150 "
             "--rebuild-max 4 --transfer-wait-ms 3000 --leader-balance-s 120", p0, secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 || !strstr(out, "initialised"))
        die("cluster init with R4's settings: %s", out);
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    snprintf(args, sizeof args, "cluster status %s --secret-file %s", p0, secret_path);
    if (cli_run(args, NULL, out, sizeof out) != 0 ||
        !strstr(out, " rebuild_mb_s 150 rebuild_max 4 transfer_wait_ms 3000 leader_balance_s 120\n"))
        die("status does not print R4's settings as init stored them: %s", out);
    printf("  init stored rebuild_mb_s 150, rebuild_max 4, transfer_wait_ms 3000, leader_balance_s 120\n");
    /* the stubs decode the body and say it back: the CLI's encoding against the server's decoder, field by field */
    static const char *const verbs[][2] = {
        {"volume transfer %s 3 5", "not built yet (transfer of volume 3 to node 5)"},
        {"volume move %s 3 --log --from 5 --to 6", "not built yet (move of volume 3 from node 5 to node 6, log)"},
        {"volume move %s 3 --log --from 6 --to 5 --cancel",
         "not built yet (move of volume 3 from node 6 to node 5, log, cancel)"},
        {"replica reset %s 4 6", "not built yet (reset of volume 4 on node 6)"}};
    for (size_t k = 0; k < sizeof verbs / sizeof verbs[0]; k++) {
        char v[256];
        snprintf(v, sizeof v, verbs[k][0], p0);
        snprintf(args, sizeof args, "cluster %s --secret-file %s", v, secret_path);
        int rc = cli_run(args, NULL, out, sizeof out);
        if (rc != 1 || !strstr(out, verbs[k][1]))
            die("tsdb cluster %s: exit %d, not \"%s\": %s", v, rc, verbs[k][1], out);
        printf("  %s: %s", v, out);
    }
    /* the bodies past the CLI: one of another length is refused by the decoders (the stubs say its length) */
    uint8_t body[16] = {0};
    static const uint8_t ops[3] = {CLUSTER_OP_VOLUME_TRANSFER, CLUSTER_OP_VOLUME_MOVE, CLUSTER_OP_REPLICA_RESET};
    for (int k = 0; k < 3; k++) {
        reply = NULL, len = 0;
        arc = admin(0, ops[k], body, 12, 0, &reply, &len);
        if (arc != META_ERR_INVALID || !reply || len < 21 || memcmp(reply, "not built yet (a body of 12 bytes)", 21))
            die("op %d with a 12-byte body: %s", ops[k], cluster_strerror(arc));
        free(reply);
    }
    /* the CLI's own checks of a move: --log, --from and --to are required, and two different nodes */
    static const char *const badmove[] = {"1 --from 1 --to 2", "1 --log --from 1", "1 --log --from 2 --to 2",
                                          "x --log --from 1 --to 2"};
    for (size_t k = 0; k < sizeof badmove / sizeof badmove[0]; k++) {
        snprintf(args, sizeof args, "cluster volume move %s %s --secret-file %s", p0, badmove[k], secret_path);
        int rc = cli_run(args, NULL, out, sizeof out);
        if (rc != 2)
            die("tsdb cluster volume move %s: exit %d, not 2: %s", badmove[k], rc, out);
    }
}

/* volcmd (r4-iface): the plumbing R4's tracks build on, before any of them sends. The Raft leader (server 0, hook
 * TSDB_CLUSTER_TEST volcmd_file=1) sends each VOLCMD kind to server 1 (the F_VOLCMD frame) and to itself (the
 * self-delivery of cl_volcmd_send); each server's driver routes it (IN_CMD: TRANSFER and CANCEL to rtransfer.c,
 * DROP_MEMBER to rset.c), whose stubs say every field and the sender. And each driver dials the other's snapshot kind
 * (TSDB_REPLD_TEST=snapdial): tsdbd's acceptor hands kind 5 to the driver, whose on_frame passes it to rsnap.c. */
static void scen_volcmd(void)
{
    scenario("volcmd");
    for (int i = 0; i < 2; i++) {
        penv[i] = "TSDB_REPLD_TEST=snapdial";
        envs[i] = "volcmd_file=1";
        start(i);
    }
    init_cluster(0, 600);
    node_add(0, 1); /* a member (a group of two keeps one voter): the volume's replicas need only that */
    uint32_t num;
    int rc;
    if ((rc = vol_add(0, "cmdv", 2, &num)) != 0)
        die("volume add cmdv: %s", cluster_strerror(rc));
    for (int i = 0; i < 2; i++)
        await_open_gen(0, "cmdv", i, 1, 0);
    /* R4's admin ops on a follower: NOTLEADER (with the leader's peer address), never handed to the stubs */
    uint8_t body[8], *reply = NULL;
    size_t blen = cluster_volume_force_encode(num, ids[1], body), rlen = 0;
    if ((rc = admin(1, CLUSTER_OP_VOLUME_TRANSFER, body, blen, 0, &reply, &rlen)) != CLUSTER_ERR_NOTLEADER)
        die("a VOLUME_TRANSFER to a follower: %s, not NOTLEADER", cluster_strerror(rc));
    free(reply);
    for (int i = 0; i < 2; i++)
        await_log_count(i, "a snapshot link (kind 5) sent a frame of unknown type", 1, 20000);
    printf("  each driver's kind-5 link reached the other's rsnap_frame\n");
    char path[PATH_MAX + 64];
    server_dir(0, path, sizeof path);
    strcat(path, "/state/volcmd.tmp");
    FILE *f = fopen(path, "w");
    if (!f)
        die("%s: %s", path, strerror(errno));
    for (int i = 0; i < 2; i++) /* TO KIND VOLUME EPOCH NODE ARG */
        for (int k = 1; k <= 3; k++)
            fprintf(f, "%u %d %" PRIu32 " %d %d %d\n", ids[i], k, num, 100 * (i + 1) + k, 7 + k, 4000 + 10 * i + k);
    if (fclose(f) != 0)
        die("%s: %s", path, strerror(errno));
    char to[PATH_MAX + 64];
    snprintf(to, sizeof to, "%.*s", (int)(strlen(path) - 4), path);
    if (rename(path, to) != 0)
        die("rename %s: %s", path, strerror(errno));
    static const char *const what[4] = {NULL, "TRANSFER to node", "DROP_MEMBER of node", "CANCEL to node"};
    for (int i = 0; i < 2; i++)
        for (int k = 1; k <= 3; k++) {
            char line[256];
            snprintf(line, sizeof line, "volume %" PRIu32 ": %s %d at epoch %d (arg %d) from node %u: ", num, what[k],
                     7 + k, 100 * (i + 1) + k, 4000 + 10 * i + k, ids[0]);
            await_log_count(i, line, 1, 20000);
        }
    if (access(to, F_OK) == 0)
        die("the leader kept %s after sending every command", to);
    printf("  every VOLCMD kind reached the member's and the leader's own driver with its fields\n");
    stop_all();
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    th_watchdog("test_cluster", 1800);
    if (argc == 3 && !strcmp(argv[1], "hardslot-child"))
        return hardslot_child(argv[2]);
    /* TMPDIR when no directory is given: /tmp is often a RAM tmpfs, and a run writes over a GB */
    const char *scratch = argc > 1 && *argv[1] ? argv[1] : getenv("TMPDIR");
    if (!scratch || !*scratch)
        scratch = "/tmp";
    snprintf(root, sizeof root, "%s/tsdb_cluster.XXXXXX", scratch);
    if (!mkdtemp(root)) {
        fprintf(stderr, "test_cluster: mkdtemp %s: %s\n", root, strerror(errno));
        return 1;
    }
    snprintf(base, sizeof base, "%s", root);
    snprintf(secret_path, sizeof secret_path, "%s/cluster.secret", root);
    /* a secret of its own per run (the one a worktree running at the same time shares would no longer separate runs) */
    char serr[256];
    if (cluster_make_secret(secret_path, serr, sizeof serr) != 0 ||
        cluster_read_secret(secret_path, secret, sizeof secret, &secret_len, serr, sizeof serr) != 0)
        die("the cluster secret %s: %s", secret_path, serr);

    static const struct {
        const char *name;
        void (*fn)(void);
    } all[] = {{"secret", scen_secret},     {"relay", scen_relay},     {"initcrash", scen_initcrash},
               {"dirsync", scen_dirsync},   {"install", scen_install}, {"reseed", scen_reseed},
               {"torn", scen_torn},         {"rebase", scen_rebase},
               {"slowinstall", scen_slowinstall},
               {"ids", scen_ids},           {"watch", scen_watch},     {"stall", scen_stall},
               {"witness", scen_witness},   {"users", scen_users}, {"login", scen_login}, {"userimport", scen_userimport},
               {"userstale", scen_userstale}, {"userpromote", scen_userpromote}, {"userreplay", scen_userreplay},
               {"userlease", scen_userlease},   {"flags", scen_flags},     {"hbsections", scen_hbsections},
               {"retired", scen_retired},   {"feed", scen_feed}, {"syncrace", scen_syncrace}, {"power", scen_power}, {"powersnap", scen_powersnap},   {"life", scen_life},
               {"copyback", scen_copyback}, {"voterstale", scen_voterstale}, {"dirsyncshared", scen_dirsync_shared},
               {"installshared", scen_install_shared}, {"powervote", scen_powervote}, {"pendshared", scen_pend_shared}, {"copyfault", scen_copyfault},
               {"volumes", scen_volumes}, {"volrefuse", scen_volrefuse}, {"snapversion", scen_snapversion}, {"links", scen_links}, {"fwdlinks", scen_fwdlinks}, {"replicate", scen_replicate}, {"replindet", scen_replindet}, {"replfail", scen_replfail}, {"failover", scen_failover}, {"failoverkeep", scen_failoverkeep}, {"classfailover", scen_classfailover}, {"fencecrash", scen_fencecrash}, {"promotecrash", scen_promotecrash}, {"deposecrash", scen_deposecrash}, {"pause", scen_pause}, {"promoteslow", scen_promoteslow}, {"openread", scen_openread}, {"openreadlead", scen_openreadlead}, {"jointpause", scen_jointpause}, {"leasestale", scen_leasestale}, {"resign", scen_resign}, {"refused", scen_refused}, {"cut", scen_cut}, {"cuthold", scen_cuthold}, {"deposetail", scen_deposetail}, {"readdepose", scen_readdepose}, {"oneway", scen_oneway}, {"oneway2", scen_oneway2}, {"cutstuck", scen_cutstuck}, {"manystuck", scen_manystuck}, {"noowner", scen_noowner}, {"isr", scen_isr}, {"topology", scen_topology}, {"copyhold", scen_copyhold}, {"movedself", scen_movedself}, {"route", scen_route}, {"forward", scen_forward}, {"fwdlease", scen_fwdlease}, {"fwddepose", scen_fwddepose}, {"fwdload", scen_fwdload}, {"viabench", scen_viabench}, {"ccstress", scen_ccstress}, {"minisr", scen_minisr}, {"closegone", scen_closegone}, {"writersync", scen_writersync}, {"shipstall", scen_shipstall}, {"keepcap", scen_keepcap}, {"snapxfer", scen_snapxfer}, {"snaplarge", scen_snaplarge}, {"snapnospace", scen_snapnospace}, {"snapbusy", scen_snapbusy}, {"snapsrckill", scen_snapsrckill}, {"snaplost", scen_snaplost}, {"snapstall", scen_snapstall}, {"rejoin", scen_rejoin}, {"rejoinsteady", scen_rejoinsteady}, {"rejoinslow", scen_rejoinslow}, {"tickwall", scen_tickwall}, {"rejoinlate", scen_rejoinlate}, {"keepfloor", scen_keepfloor}, {"reopenhold", scen_reopenhold}, {"backpressure", scen_backpressure}, {"stallstop", scen_stallstop}, {"inboxshared", scen_inboxshared}, {"idleseal", scen_idleseal}, {"chunksrv", scen_chunksrv}, {"r4stubs", scen_r4stubs}, {"volcmd", scen_volcmd},
               {"ackdurable", scen_ackdurable}, {"writerfail", scen_writerfail}, {"writerfailq", scen_writerfailq}, {"diverged", scen_diverged}, {"writerdepose", scen_writerdepose}, {"lease", scen_lease}, {"hungloop", scen_hungloop},
               {"leasedown", scen_leasedown}, {"leasequorum", scen_leasequorum}, {"hardslot", scen_hardslot},
               {"repcheck", scen_repcheck}};
    int n = (int)(sizeof all / sizeof all[0]);
    for (int a = 2; a < argc; a++) { /* a name that matches nothing would run nothing and print OK */
        int known = 0;
        for (int k = 0; k < n; k++)
            known |= !strcmp(argv[a], all[k].name);
        if (!known)
            die("unknown scenario \"%s\"", argv[a]);
    }
    for (int k = 0; k < n; k++) {
        int run = argc <= 2;
        for (int a = 2; a < argc; a++)
            run |= !strcmp(argv[a], all[k].name);
        if (run)
            all[k].fn();
    }
    stop_all();
    /* a passing run removes its directory (a failing one dies above and keeps it for a look);
       TSDB_KEEP_SCRATCH=1 keeps it */
    const char *keep = getenv("TSDB_KEEP_SCRATCH");
    if (keep && *keep && strcmp(keep, "0"))
        printf("test_cluster: OK (%s kept)\n", root);
    else if (nftw(root, rm_one, 16, FTW_DEPTH | FTW_PHYS) != 0)
        printf("test_cluster: OK (could not remove all of %s: %s)\n", root, strerror(errno));
    else
        printf("test_cluster: OK\n");
    return 0;
}
