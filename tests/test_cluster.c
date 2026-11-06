/*
 * tsdbd --cluster end to end (docs/design-cluster.md §2, step 1 of §14): five servers, each started as tsdbd with
 * --port 0 --port-file (tests/harness.h), the metadata group on their peer ports.
 *
 * The life of a small cluster: `init` on one server; `node add` of the others, once through a server that is not the
 * leader (NOTLEADER names the leader, the caller follows); the voters grow 1 -> 3 -> 5 with the members (with four
 * members the fourth watches: its watch stream catches up to the leader's state); STATUS from every server agrees;
 * SIGKILL of the Raft leader: another is elected, the dead member is marked down, and after voter_replace_s it leaves
 * the voters (no healthy server is left to take its place, so four remain); a restart on the same directory comes back with incarnation + 1, is
 * marked up and becomes a voter again; SIGTERM stops every server with status 0. `tsdb cluster status` (the CLI) is
 * run once against a member and its lines checked.
 *
 * Usage: test_cluster [SCRATCH]   (TSDBD: the server, default ./build/tsdbd; TSDB_CLI: ./build/tsdb)
 */
#define _GNU_SOURCE
#include "cluster.h"
#include "harness.h"
#include "meta.h"
#include "raft/raft.h"

#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>

#define N 5
#define WAIT_MS 30000

static char base[PATH_MAX], secret_path[PATH_MAX + 16];
static uint8_t secret[64];
static size_t secret_len;
static pid_t pids[N];
static int pports[N];
static uint32_t ids[N]; /* member id of server i (0 until it is a member) */
static int fails;

static void die(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "test_cluster: FAIL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n  (server logs in %s/n*.log)\n", base);
    va_end(ap);
    static int dying;
    if (!dying++) { /* what every live server says, for the report */
        const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
        for (int i = 0; i < N; i++)
            if (pids[i] > 0 && pports[i]) {
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

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void start(int i)
{
    char dir[PATH_MAX + 16], pf[PATH_MAX + 64], log[PATH_MAX + 16], flag[64];
    snprintf(dir, sizeof dir, "%s/n%d", base, i);
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
        const char *argv[] = {"tsdbd", "--no-auth", flag, "--dir", dir, "--port", "0", "--port-file", pf,
                              "--cluster", "--cluster-secret-file", secret_path, NULL};
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    pids[i] = pid;
    if (th_await(pf, pid, 20000) <= 0)
        die("server %d: %s", i, th_why);
    if (!th_pport)
        die("server %d: the port file names no peer port", i);
    pports[i] = th_pport;
}

static void stop(int i, int sig)
{
    signal_child(pids[i], sig);
    int status;
    if (waitpid(pids[i], &status, 0) != pids[i])
        die("waitpid %d: %s", (int)pids[i], strerror(errno));
    if (sig == SIGTERM && !(WIFEXITED(status) && WEXITSTATUS(status) == 0))
        die("server %d did not stop cleanly on SIGTERM (status 0x%x)", i, status);
    pids[i] = 0;
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
    free(v->st.heard);
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

/* Waits until `pred` holds for the view of server i (polling every 100 ms); dies naming `what` after WAIT_MS. */
static void await_view(int i, int (*pred)(const struct view *, void *), void *arg, const char *what)
{
    uint64_t t0 = now_ms();
    for (;;) {
        struct view v;
        view(i, &v);
        int ok = pred(&v, arg);
        uint64_t applied = v.st.applied;
        uint32_t leader = v.st.leader;
        unsigned nv = voters(&v);
        view_free(&v);
        if (ok)
            return;
        if (now_ms() - t0 > WAIT_MS)
            die("server %d: %s did not happen within %d ms (applied %" PRIu64 ", leader %" PRIu32 ", %u voters)", i,
                what, WAIT_MS, applied, leader, nv);
        usleep(100000);
    }
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

static void node_add(int via, int newi)
{
    char np[META_ADDR_MAX];
    peer_of(newi, np, sizeof np);
    uint8_t req[META_ADDR_MAX + 2];
    size_t n = strlen(np);
    req[0] = (uint8_t)n;
    req[1] = (uint8_t)(n >> 8);
    memcpy(req + 2, np, n);
    uint8_t *reply;
    size_t len;
    int rc = admin(via, CLUSTER_OP_NODE_ADD, req, n + 2, 1, &reply, &len);
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

/* Every live server's STATUS agrees on the leader and, once caught up, on the state (meta_hash). */
static void check_all_agree(int leader_i)
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
        if (v.m.nmembers != (uint32_t)N && pids[N - 1] > 0 && ids[N - 1])
            die("server %d sees %" PRIu32 " members, expected %d", i, v.m.nmembers, N);
        view_free(&v);
    }
}

static void cli_status(int i)
{
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char cmd[PATH_MAX * 2 + 128], peer[META_ADDR_MAX], line[1024];
    peer_of(i, peer, sizeof peer);
    snprintf(cmd, sizeof cmd, "%s cluster status %s --secret-file %s", cli, peer, secret_path);
    FILE *p = popen(cmd, "r");
    if (!p)
        die("popen %s: %s", cmd, strerror(errno));
    int node = 0, group = 0, members = 0;
    while (fgets(line, sizeof line, p)) {
        node += !strncmp(line, "node ", 5);
        group += !strncmp(line, "group voters ", 13);
        members += !strncmp(line, "member ", 7) && strstr(line, " up ");
    }
    int st = pclose(p);
    if (st != 0 || node != 1 || group != 1 || members != N)
        die("`%s`: status %d, %d node, %d group, %d up member lines (expected 0, 1, 1, %d)", cmd, st, node, group,
            members, N);
}

int main(int argc, char **argv)
{
    const char *scratch = argc > 1 ? argv[1] : "/tmp";
    snprintf(base, sizeof base, "%s/tsdb_cluster.XXXXXX", scratch);
    if (!mkdtemp(base)) {
        fprintf(stderr, "test_cluster: mkdtemp %s: %s\n", base, strerror(errno));
        return 1;
    }
    snprintf(secret_path, sizeof secret_path, "%s/secret", base);
    secret_len = 32;
    memcpy(secret, "test-cluster-secret-0123456789ab", secret_len);
    FILE *f = fopen(secret_path, "w");
    if (!f || fwrite(secret, 1, secret_len, f) != secret_len || fclose(f) != 0)
        die("writing %s", secret_path);
    chmod(secret_path, 0600);

    for (int i = 0; i < N; i++)
        start(i);
    printf("  %d servers up\n", N);

    /* init on server 0 with fast timings: down after 1.5 s, a dead voter replaced after 3 s */
    struct meta_settings s;
    meta_settings_default(&s);
    s.node_timeout_ms = 1500;
    s.heartbeat_ms = 200;
    s.voter_replace_s = 3;
    uint8_t req[64], *reply;
    size_t len;
    size_t rl = cluster_settings_encode(&s, req);
    int rc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != 0)
        die("init: %s", cluster_strerror(rc));
    rc = admin(0, CLUSTER_OP_INIT, req, rl, 0, &reply, &len);
    free(reply);
    if (rc != CLUSTER_ERR_EXISTS)
        die("a second init answered %d (%s), expected EXISTS", rc, cluster_strerror(rc));
    ids[0] = 1;
    await_view(0, p_leader_self, NULL, "leading the initialised cluster");
    unsigned want = 1;
    await_view(0, p_voters, &want, "one voter");
    /* a server in no cluster refuses NODE_ADD as not the leader, with no leader to name */
    {
        char np[META_ADDR_MAX];
        peer_of(2, np, sizeof np);
        uint8_t r2[META_ADDR_MAX + 2];
        size_t n = strlen(np);
        r2[0] = (uint8_t)n;
        r2[1] = 0;
        memcpy(r2 + 2, np, n);
        rc = admin(1, CLUSTER_OP_NODE_ADD, r2, n + 2, 0, &reply, &len);
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
    check_all_agree(0);
    cli_status(2);
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
    int other = (li + 1) % N, ni = -1;
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
            struct view v;
            view(ni, &v);
            int l = leader_index(&v);
            view_free(&v);
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
    check_all_agree(ni);

    for (int i = 0; i < N; i++)
        stop(i, SIGTERM);
    printf("test_cluster: OK%s (%s)\n", fails ? " with failures" : "", base);
    return fails ? 1 : 0;
}
