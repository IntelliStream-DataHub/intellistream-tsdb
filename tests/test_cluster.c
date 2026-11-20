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
 * Usage: test_cluster [SCRATCH [SCENARIO...]]   (TSDBD: the server, default ./build/tsdbd; TSDB_CLI: ./build/tsdb)
 */
#define _GNU_SOURCE
#include "cluster.h"
#include "cvol_check.h"
#include "harness.h"
#include "meta.h"
#include "peerlink.h"
#include "raft/raft.h"
#include "raft/raft_log.h"
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
    if (!dying++) { /* what every live server says, for the report */
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

static void pause_server(int i, int on)
{
    signal_child(pids[i], on ? SIGSTOP : SIGCONT);
    stopped[i] = on;
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void server_dir(int i, char *out, size_t n) { snprintf(out, n, "%s/n%d", base, i); }

static int auth_on[N]; /* server i runs with login (no --no-auth): its root has USERS */
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
static uint64_t wal_lowest(int i, uint32_t num);

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
    memset(is_witness, 0, sizeof is_witness);
    memset(auth_on, 0, sizeof auth_on);
    snprintf(base, sizeof base, "%s/%s", root, name);
    if (mkdir(base, 0755) != 0)
        die("mkdir %s: %s", base, strerror(errno));
    printf(" %s\n", name);
    fflush(stdout);
}

static void peer_of(int i, char *out, size_t n) { snprintf(out, n, "127.0.0.1:%d", pports[i]); }

/* One admin op to server i; follows NOTLEADER once when `follow`. Returns the op's result; dies when unreachable. */
static int admin(int i, int op, const void *req, size_t rl, int follow, uint8_t **reply, size_t *len)
{
    char target[META_ADDR_MAX + 1], err[1024];
    peer_of(i, target, sizeof target);
    for (int hop = 0;; hop++) {
        int rc;
        *reply = NULL;
        *len = 0;
        if (cluster_admin(target, secret, secret_len, op, req, rl, 10000, &rc, reply, len, err, sizeof err) != 0)
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
static uint32_t init_retain_s; /* nonzero: INIT's repl_retain_s */
static uint32_t init_isr_lag_ms; /* nonzero: INIT's isr_lag_ms */

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
    s.node_timeout_ms = 1500;
    s.heartbeat_ms = 200;
    s.voter_replace_s = replace_s;
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
        die("server 2 wrote no retired mark");
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
 * Catches a log that is never synced (applied falls back), a hard state that is never saved (the term restarts from
 * the start) and a snapshot whose directory entry is not synced (with `snaps`: the log was compacted behind it, and the
 * state goes back to an earlier image). Planted in a copy of the server, `power` fails on the unsynced log (C1) and on
 * the unsaved hard state (C2), `powersnap` on those and on the snapshot without its directory sync (C7); the clean
 * server passes every run. crashfs follows dup'ed descriptors for this (the log syncs duplicates of its segments).
 * No CRASHFS_TEAR: a tear may keep every unsynced byte, which would let the unsynced log pass now and then. */
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
    snprintf(pe, sizeof pe, "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_KEEP_OLD=1", so, st);
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
    view(0, &v);
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
static void scen_powervote(void)
{
    scenario("powervote");
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
    static char pe[PATH_MAX * 2 + 128];
    char st[PATH_MAX + 160], dir2[PATH_MAX + 160];
    snprintf(dir2, sizeof dir2, "%s/n2", base);
    snprintf(st, sizeof st, "%s/n2/state", base);
    if (mkdir(dir2, 0755) != 0 || mkdir(st, 0755) != 0)
        die("mkdir %s: %s", st, strerror(errno));
    snprintf(pe, sizeof pe, "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_KEEP_OLD=1", so, st);
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
}
static void scen_powersnap(void) { power_loss(1); }

/* A power cut between a hard state save's write and its sync (jepsen-cluster kv power, seed 1, 2026-11-17: "slot 1 of the
 * raft hard state in raft/hard is blank beside sequence 6 in slot 0", and the node never started again). A disk keeps
 * the slot's old sectors or the new ones, which the reader takes; crashfs zeroed rewrites it was not told to keep, and
 * the reader rightly takes a zeroed slot beside a later save for rot. So every power-loss harness of tsdbd --cluster
 * runs crashfs with CRASHFS_KEEP_OLD=1: here a child saves six hard states (seq 6 in slot 0, seq 5 in slot 1), writes
 * the seventh save's slot 1 without its sync and loses power at exit. With KEEP_OLD the hard state reads back as
 * term 6; without it (crashfs's default) it must be refused, which is the state the jepsen node was left in. */
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
            if (keep)
                setenv("CRASHFS_KEEP_OLD", "1", 1);
            else
                unsetenv("CRASHFS_KEEP_OLD");
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
                die("CRASHFS_KEEP_OLD: after a power cut inside the seventh hard save the state reads as rc %d term %" PRIu64
                    " (want term 6): %s",
                    (int)e, (uint64_t)hs.term, lerr);
            printf("  CRASHFS_KEEP_OLD: a power cut inside a hard save leaves the previous one (term 6)\n");
        } else {
            if (e == ISO_OK)
                die("without CRASHFS_KEEP_OLD the zeroed slot read as term %" PRIu64 ": this scenario no longer reaches "
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

/* `tsdb cluster user import PEER DIR...` (tools/cluster-user-cli.c): the USERS of two stopped servers, the same (a
 * user with 5,000 grants among them), go in as they are: the verifiers copied, so the passwords log in on a cluster
 * server, the grants count and are in force; a rerun skips what the group holds the same. Two that differ (a grant
 * more in one, a user missing from the other) import nothing and name each such user, until --from-node says whose
 * copy wins. Mutant importmerge (differing users imported silently) must fail here. */
static void scen_userimport(void)
{
    scenario("userimport");
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
    expect_unavail(c, tsdbc_series_at(c, "pump7.flow", "plant", &x), 1, TSDB_WHY_RECOVERING, "an old copy");
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
        expect_unavail(c, tsdbc_series_at(c, "pump7.flow", "tank", &x), 2, TSDB_WHY_RECOVERING, "tank held");
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
    expect_unavail(c, tsdbc_series_at(c, "pump7.flow", "pipe", &x), 3, TSDB_WHY_RECOVERING, "a stale open");
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
    /* its copy is stale (no core caught up): plant may not be declared yet, still never NOTFOUND */
    expect_unavail(c, tsdbc_series_at(c, "pump7.flow", "plant", &x), TSDB_VOL_ANY, TSDB_WHY_RECOVERING, "no quorum");
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
            free(ts);
            free(val);
        }
        for (int j = 0; j < w->count && w->rc == 0; j++) {
            int64_t ts = w->t0 + 1000LL * j;
            float val = (float)j;
            w->rc = tsdbc_insert_series(c, w->id, &ts, &val, 1);
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
    /* R has removed a segment of its own (its floor passed a flushed one) before it dies: the floor that allowed it
     * must be in R's REPL by then, or R's log starts above the floor it reads back (floornotsaved, caught only when
     * R's flush and floor had moved by this point: once in four runs they had not) */
    for (uint64_t t1 = now_ms(); wal_lowest(R, 1) <= 1 && now_ms() - t1 < 20000;) {
        struct timespec ms = {0, 50000000};
        nanosleep(&ms, NULL);
    }
    if (wal_lowest(R, 1) <= 1)
        die("replica %d removed none of its WAL segments in 20 s (a flush every 5 s, the floor every second)", R);
    stop(R, SIGKILL);
    struct rep_writer w = {cports[L], id[0], 10000000, 300, 0, 0, 0};
    /* and a large insert: its part is shipped while R is down; R, back, does not stage the first file it receives
     * (TSDB_REPLD_TEST=dropbulk), finds the COMMIT marker in the log without the file and fetches it from the leader
     * by the marker's position (eng_leader_file_open serves a name only for the marker that named it) */
    struct rep_writer wb = {cports[L], id[2], 2000000000, 0, 0, 0, 100000}; /* above wal_max_batch_rows */
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
     * A replica that refuses its own log (the leader dropped segments past a floor it never saved: floornotsaved) is
     * named apart from a slow rejoin, so that mutant's catch cannot be a timeout for any other reason */
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
    struct rep_writer w = {cports[L], id, 2000, 1, 0, 0, 0};
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
 * led on two servers is INVALID and writes nothing. Mutants (TSDB_ROUTE_MUTANT): rightsbeforeroute, movedoffcopy,
 * replicaread, spanaccepted. */
static void scen_route(void)
{
    scenario("route");
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
    tsdbc_close(c);
    printf("  a replica moves writes, reads, paths and packed frames to the leader; the client follows MOVED\n");

    /* lone: no leader in the copy; its real leader serves it (led here), every other server answers NO_OWNER */
    int nl = LL == 0 ? 1 : 0;
    c = login_retry(nl, "root", "root-pw", 0);
    expect_unavail(c, tsdbc_series_at(c, "plant.c", "lone", &x), (uint16_t)num[1], TSDB_WHY_NO_OWNER,
                   "a volume with no leader in the copy");
    tsdbc_close(c);
    printf("  a volume with no leader in the copy: UNAVAILABLE (NO_OWNER)\n");

    /* south, on one server: a user with a prefix grant writes by id through a server without a copy of it and
     * is moved, never DENIED there (the id is not resolved through that server's names); outside the grant: DENIED at
     * the owner */
    int so = server_of(v[2].leader), to = (so + 1) % 3, no = server_of(v[3].leader);
    c = login_retry(so, "root", "root-pw", 0);
    tsdb_sid sid = rep_series(c, "plant.s", "south", 30000), sother = rep_series(c, "other.s", "south", 30000);
    tsdbc_close(c);
    tsdbc *a = login_retry(to, "alice", "alice-pw", 0);
    expect_moved(a, tsdbc_insert_series(a, sid, ts, val, 1), num[2], v[2].leader, so, "alice's write off the owner");
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
    if (sa < 0) {
        printf("  (east, south and north are led on one server: no spanning check)\n");
    } else {
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
    struct rep_writer w = {cports[L], id, 2500, 1, 0, 0, 0};
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
    struct rep_writer w = {cports[L], id, 3000, 1, 0, 0, 0};
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
            w[k] = (struct rep_writer){cports[L], id, 100000000LL * (b * CONNS + k + 1), WRITES, 0, 0, 0};
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

/* the lowest WAL segment number in server i's directory of volume num; UINT64_MAX when it has none (a volume that
 * flushed everything and removed its segments also passed number 1) */
static uint64_t wal_lowest(int i, uint32_t num)
{
    char dir[PATH_MAX + 80], v[PATH_MAX + 120];
    server_dir(i, dir, sizeof dir);
    snprintf(v, sizeof v, "%s/v%" PRIu32, dir, num);
    DIR *d = opendir(v);
    if (!d)
        die("opendir %s: %s", v, strerror(errno));
    uint64_t lo = UINT64_MAX, seq;
    errno = 0;
    for (struct dirent *e; (e = readdir(d));)
        if (sscanf(e->d_name, "wal_%" SCNx64 ".log", &seq) == 1 && seq < lo)
            lo = seq;
    if (errno)
        die("readdir %s: %s", v, strerror(errno));
    closedir(d);
    return lo;
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
                               0, 1000};
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
                                       1000};
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
    struct rep_writer wb = {cports[L], id[2], 2000000000, 0, 0, 0, 100000};
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
        struct rep_writer w = {cports[L], id, 1000000LL * round + 1, 0, 0, 0, 1000};
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
    struct rep_writer wb = {cports[L], id2, 2000000000, 0, 0, 0, 100000}; /* above wal_max_batch_rows */
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
    /* the leader seals the marker's segment, and R's log passes its end */
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
    snprintf(wp, sizeof wp, "%s/v1/wal_%016" PRIx64 ".log", dir, wal_newest(L));
    struct stat st0, st1;
    struct timespec s5 = {5, 0};
    if (stat(wp, &st0) != 0 || (nanosleep(&s5, NULL), stat(wp, &st1)) != 0)
        die("stat %s: %s", wp, strerror(errno));
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
    struct rep_writer w = {cports[L], id, 2000, 1, 0, 0, 0};
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
    struct rep_writer w = {cports[L], id, 2000, 1000000, 0, 0, 0};
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
        snprintf(pe[i], sizeof pe[i], "LD_PRELOAD=%s CRASHFS_ROOT=%s CRASHFS_KEEP_OLD=1", so, dir);
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
    struct rep_writer w1 = {cports[L], id, 1000 + 1000LL * ACKED, 1, 0, 0, 0};
    pthread_t th1, th2;
    if (pthread_create(&th1, NULL, rep_write_thread, &w1) != 0)
        die("pthread_create");
    for (int r = 0; r < 2; r++) {
        int R = r ? R2 : R1, st = await_exit(R, 15000);
        if (st != 99)
            die("server %d ended with status %d after the power loss, expected 99 (crashfs)", R, st);
    }
    signal_child(pids[L], SIGUSR2); /* and the leader's, at its next append */
    struct rep_writer w2 = {cports[L], id, 1000 + 1000LL * (ACKED + 1), 1, 0, 0, 0};
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
 * writes is unknown (each went to the replicas), so their connections close without an answer (D12); the volume answers UNAVAILABLE from then on (quiesced: clients go to its
 * next leader); the server reports the volume LOCAL_FAULT, answers in its root, keeps its vote and its other
 * volumes (a later volume add commits and opens there) and stops cleanly. TSDB_REPLD_MUTANT=nodepose and noquiesce
 * fail it. */
static void scen_writerfail(void)
{
    scenario("writerfail");
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
    /* the failed volume: UNAVAILABLE for writes and reads; the root answers */
    tsdbc *c2 = client(L);
    ts = 7000;
    if ((rc = tsdbc_insert_series(c2, id, &ts, &val, 1)) != TSDB_ERR_UNAVAILABLE)
        die("a write to the failed volume: %s, not UNAVAILABLE", ers(rc));
    tsdb_points p;
    if ((rc = tsdbc_query_points(c2, id, INT64_MIN, INT64_MAX, &p)) != TSDB_ERR_UNAVAILABLE)
        die("a read of the failed volume: %s, not UNAVAILABLE", ers(rc));
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
        if ((rc = tsdbc_insert_series(c3, id, &ts, &val, 1)) != TSDB_ERR_UNAVAILABLE)
            die("write %d to the failed volume: %s, not UNAVAILABLE", k, ers(rc));
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
    printf("  a leader whose engine failed closes the connections of the writes in flight, answers UNAVAILABLE for "
           "the volume, keeps its root, its vote and its other volumes and stops cleanly; the replicas hold nothing "
           "written after the failure\n");
}

/* writerdepose (M5, D13 with a healthy engine): the leader's driver fails the volume while its engine is fine
 * (crashfs: EIO on the leader's next fdatasync of its REPL file, the save of the commit floor at most once a second
 * after the floor moved), with a write waiting for a paused replica. Nothing in the engine ends that wait: only the
 * deposal does (eng_leader_depose in vol_fail), and its outcome is unknown (its records went to the replicas), so its
 * connection closes without an answer (D12). A later write is refused (UNAVAILABLE) and never numbered; the replicas
 * hold no row past what was sent before the failure. TSDB_REPLD_MUTANT=nodepose fails it (the write waits for good). */
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
    /* a write after the failure: refused, never numbered */
    tsdbc *c2 = client(L);
    ts += 100000;
    if ((rc = tsdbc_insert_series(c2, id, &ts, &val, 1)) != TSDB_ERR_UNAVAILABLE)
        die("a write to the failed volume: %s, not UNAVAILABLE", ers(rc));
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
           "for its commit point and refuses the next; the replicas hold between %d and %d rows\n",
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
    if (!lease_until_ok(c, id, 30000, 1))
        die("no read answered on %d after every server was resumed", X);
    printf("  %d paused past its lease: %d reads refused while nobody renewed it, then answered\n", X, nb);
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
 * (server/repld.c): a replica's saved floor reaches its engine, which then removes its flushed segments (floornottold
 * keeps them all); a replica whose newest REPL slot is damaged opens "unsure" and its first save at the open takes the
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
                "flushed segments)", R, nw);
        usleep(100000);
    }
    printf("  the replica removed its flushed segments once its floor was saved (%d left)\n", nw);

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
        struct rep_writer sw = {cports[L], id, 4000000, 1, 0, 0, 0};
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
    struct rep_writer w = {cports[L], id, 5000000, 1, 0, 0, 0};
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

static int rm_one(const char *path, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st, (void)flag, (void)ftw;
    return remove(path) != 0 && errno != ENOENT ? -1 : 0;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
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
               {"witness", scen_witness},   {"users", scen_users}, {"login", scen_login}, {"userimport", scen_userimport},   {"flags", scen_flags},     {"hbsections", scen_hbsections},
               {"retired", scen_retired},   {"feed", scen_feed}, {"syncrace", scen_syncrace}, {"power", scen_power}, {"powersnap", scen_powersnap},   {"life", scen_life},
               {"copyback", scen_copyback}, {"voterstale", scen_voterstale}, {"dirsyncshared", scen_dirsync_shared},
               {"installshared", scen_install_shared}, {"powervote", scen_powervote}, {"pendshared", scen_pend_shared}, {"copyfault", scen_copyfault},
               {"volumes", scen_volumes}, {"volrefuse", scen_volrefuse}, {"links", scen_links}, {"replicate", scen_replicate}, {"replindet", scen_replindet}, {"replfail", scen_replfail}, {"isr", scen_isr}, {"topology", scen_topology}, {"copyhold", scen_copyhold}, {"movedself", scen_movedself}, {"route", scen_route}, {"ccstress", scen_ccstress}, {"minisr", scen_minisr}, {"closegone", scen_closegone}, {"writersync", scen_writersync}, {"shipstall", scen_shipstall}, {"keepcap", scen_keepcap}, {"rejoin", scen_rejoin}, {"rejoinsteady", scen_rejoinsteady}, {"rejoinslow", scen_rejoinslow}, {"tickwall", scen_tickwall}, {"rejoinlate", scen_rejoinlate}, {"keepfloor", scen_keepfloor}, {"reopenhold", scen_reopenhold}, {"backpressure", scen_backpressure}, {"stallstop", scen_stallstop}, {"inboxshared", scen_inboxshared}, {"idleseal", scen_idleseal},
               {"ackdurable", scen_ackdurable}, {"writerfail", scen_writerfail}, {"writerdepose", scen_writerdepose}, {"lease", scen_lease}, {"hungloop", scen_hungloop},
               {"leasedown", scen_leasedown}, {"leasequorum", scen_leasequorum}, {"hardslot", scen_hardslot},
               {"repcheck", scen_repcheck}};
    int n = (int)(sizeof all / sizeof all[0]);
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
