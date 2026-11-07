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
#include "harness.h"
#include "meta.h"
#include "raft/raft.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <poll.h>
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
static uint8_t secret[64];
static size_t secret_len;
static pid_t pids[N];
static int pports[N], stopped[N];
static const char *envs[N]; /* TSDB_CLUSTER_TEST of server i */
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

static void start(int i)
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
        char state[PATH_MAX + 90];
        snprintf(state, sizeof state, "%s/state", dir);
        const char *argv[] = {"tsdbd", "--no-auth", flag, "--dir", dir, "--port", "0", "--port-file", pf,
                              "--cluster", "--cluster-secret-file", secret_path, "--state-dir", state, NULL};
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    pids[i] = pid;
    stopped[i] = 0;
    if (th_await(pf, pid, 20000) <= 0)
        die("server %d: %s", i, th_why);
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
    return v->st.applied >= c->applied && v->m.applied >= c->applied && (v->m.applied != c->applied ||
                                                                          meta_hash(&v->m) == c->hash);
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

/* INIT on server i with fast timings (down after 1.5 s, a dead voter replaced after replace_s); it leads */
static void init_cluster(int i, uint32_t replace_s)
{
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 1500;
    s.heartbeat_ms = 200;
    s.voter_replace_s = replace_s;
    uint8_t req[64], *reply;
    size_t len;
    size_t rl = cluster_settings_encode(&s, META_MODE_REPLICATED, req);
    int rc = admin(i, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != 0)
        die("init: %s", cluster_strerror(rc));
    ids[i] = 1;
    await_view(i, p_leader_self, NULL, "leading the initialised cluster");
}

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
        /* [len][type][magic 4][ver 1][id 4][uuid 16][ident 16][nonce 32][peer: u16 + bytes] */
        const struct meta_member *m = meta_member(&v->m, cid);
        if (!m)
            die("relay: no member %" PRIu32, cid);
        size_t pl = strlen(m->peer);
        g = malloc(80 + pl);
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
        gl = 80 + pl;
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
        uint8_t req[64], *reply;
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
    uint8_t req[64], *rep = NULL;
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

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    const char *scratch = argc > 1 ? argv[1] : "/tmp";
    snprintf(root, sizeof root, "%s/tsdb_cluster.XXXXXX", scratch);
    if (!mkdtemp(root)) {
        fprintf(stderr, "test_cluster: mkdtemp %s: %s\n", root, strerror(errno));
        return 1;
    }
    snprintf(base, sizeof base, "%s", root);
    snprintf(secret_path, sizeof secret_path, "%s/cluster.secret", root);
    secret_len = 32;
    memcpy(secret, "test-cluster-secret-0123456789ab", secret_len);
    FILE *f = fopen(secret_path, "w");
    if (!f || fwrite(secret, 1, secret_len, f) != secret_len || fclose(f) != 0)
        die("writing %s", secret_path);
    chmod(secret_path, 0600);

    static const struct {
        const char *name;
        void (*fn)(void);
    } all[] = {{"secret", scen_secret},     {"relay", scen_relay},     {"initcrash", scen_initcrash},
               {"dirsync", scen_dirsync},   {"install", scen_install}, {"reseed", scen_reseed},
               {"torn", scen_torn},         {"rebase", scen_rebase},
               {"slowinstall", scen_slowinstall},
               {"ids", scen_ids},           {"watch", scen_watch},     {"stall", scen_stall},
               {"life", scen_life}};
    int n = (int)(sizeof all / sizeof all[0]);
    for (int k = 0; k < n; k++) {
        int run = argc <= 2;
        for (int a = 2; a < argc; a++)
            run |= !strcmp(argv[a], all[k].name);
        if (run)
            all[k].fn();
    }
    stop_all();
    printf("test_cluster: OK (%s)\n", root);
    return 0;
}
