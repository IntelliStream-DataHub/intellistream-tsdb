/* SPDX-License-Identifier: AGPL-3.0-only */
/* tsdbd with a volume on a storage domain under the outagefs shim, and the C client's protocol 13 side (ha.md 9,
 * 15.4): while the domain is DOWN a request for the volume is refused with TSDB_ERR_UNAVAILABLE and its body (the
 * volume, the reason, the retry_after); TOPOLOGY says the volume is unavailable and the root is not; a client with
 * its time budget sends the request again and succeeds once the volume has recovered, the row exactly once; a
 * budget shorter than the outage returns the status.
 *
 * Usage: test_unavailable [scratch directory]  (re-executes itself under build/liboutagefs.so; TSDBD, TSDBD_IO as
 * in test_outage) */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include <limits.h>
#include "harness.h"
#include "outagefs_ctl.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>

static int failures;
#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                        \
        }                                                                      \
    } while (0)

static char ctl[256], root[512];
static int port;
static pid_t server;

static double now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e3 + t.tv_nsec / 1e6;
}
static void msleep(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static int fault(const char *cmd, const char *dir)
{
    char line[600], out[256];
    snprintf(line, sizeof line, "%s %s", cmd, dir);
    int rc = outagefs_cmd(ctl, line, out, sizeof out);
    if (rc)
        fprintf(stderr, "ctl '%s': %s\n", line, out);
    return rc;
}

static pid_t start_server(const char *dir_a)
{
    char pf[PATH_MAX + 64], follow[64], da[PATH_MAX + 32];
    th_portfile(pf, sizeof pf, root);
    th_follow_me(follow, sizeof follow);
    snprintf(da, sizeof da, "%s", dir_a);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    setenv("TSDBD_TEST_HEALTH", "down=2500,read=200,write=500,recover=1500,fast=400,spawn=60000", 1);
    pid_t pid = fork();
    if (pid == 0) {
        const char *argv[32] = {"tsdbd", "--dir", root, "--port", "0", "--port-file", pf, "--workers", "8", "--no-auth",
                                follow, "--health-domain", da};
        int n = 13;
        if (getenv("TSDBD_IO")) {
            argv[n++] = "--io";
            argv[n++] = getenv("TSDBD_IO");
        }
        argv[n] = NULL;
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    int p = th_await(pf, pid, 30000);
    unlink(pf);
    if (p <= 0) {
        fputs(th_why, stderr);
        exit(1);
    }
    port = p;
    return pid;
}

static tsdb_sid sid_a, sid_root;

struct caller {
    int budget_ms, rc;
    double ms;
    int64_t ts;
};

static void *call_insert(void *arg)
{
    struct caller *k = arg;
    tsdbc *c;
    if (tsdbc_connect_timeout("127.0.0.1", port, 3000, &c)) {
        k->rc = -1;
        return NULL;
    }
    tsdbc_set_retry(c, k->budget_ms);
    float v = 7;
    double t0 = now_ms();
    k->rc = tsdbc_insert_series(c, sid_a, &k->ts, &v, 1);
    k->ms = now_ms() - t0;
    tsdbc_close(c);
    return NULL;
}

static int count_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts;
    (void)v;
    *(size_t *)ctx += n;
    return 0;
}

/* ---- a raw connection, for what a client of another protocol version sees */

static bool rd_all(int fd, void *p, size_t n)
{
    uint8_t *o = p;
    while (n) {
        ssize_t r = read(fd, o, n);
        if (r <= 0)
            return false;
        o += r;
        n -= (size_t)r;
    }
    return true;
}

/* Sends one request frame and reads its reply: returns the length field of the reply (id, op, status, body) or
 * -1, with the status in *status. */
static long raw_call(int fd, uint32_t id, uint8_t op, const void *pl, size_t n, int32_t *status)
{
    uint8_t h[9];
    uint32_t len = (uint32_t)(5 + n);
    memcpy(h, &len, 4);
    memcpy(h + 4, &id, 4);
    h[8] = op;
    if (write(fd, h, 9) != 9 || (n && write(fd, pl, n) != (ssize_t)n))
        return -1;
    uint8_t rh[13];
    if (!rd_all(fd, rh, 13))
        return -1;
    memcpy(&len, rh, 4);
    memcpy(status, rh + 9, 4);
    uint8_t *rest = malloc(len);
    bool ok = len < 9 || rd_all(fd, rest, len - 9);
    free(rest);
    return ok ? (long)len : -1;
}

static int raw_open(uint32_t version)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || connect(fd, (struct sockaddr *)&a, sizeof a) != 0)
        return -1;
    int32_t st;
    if (raw_call(fd, 1, TSDB_OP_HELLO, &version, 4, &st) < 0 || st != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* A QUERY_RAW of sid_a, refused: the reply is the op's bare error shape (u8 last, u32 n: 14 bytes after the length
 * field) for a client that said 12, and longer by the body for one that said 13. */
static void check_version_gate(void)
{
    uint8_t q[24] = {0};
    memcpy(q, &sid_a, 8);
    int64_t end = 100000;
    memcpy(q + 16, &end, 8);
    for (uint32_t ver = 12; ver <= 13; ver++) {
        int fd = raw_open(ver);
        CHECK(fd >= 0);
        if (fd < 0)
            continue;
        int32_t st = 0;
        long len = raw_call(fd, 2, TSDB_OP_QUERY_RAW, q, sizeof q, &st);
        CHECK(st == TSDB_ERR_UNAVAILABLE);
        CHECK(ver == 12 ? len == 14 : len > 14); /* mutation "body for every connection": 12 gets it too */
        /* and the connection still answers */
        uint8_t rq[24] = {0};
        tsdb_sid r = sid_root;
        memcpy(rq, &r, 8);
        memcpy(rq + 16, &end, 8);
        CHECK(raw_call(fd, 3, TSDB_OP_QUERY_RAW, rq, sizeof rq, &st) > 0 && st == 0);
        close(fd);
    }
}

/* QUERY_MULTI over a series of the down volume and one of the root: the down series answers UNAVAILABLE in its
 * own frame (bare), the other series is answered, the call returns 0 and the connection stays usable. */
static void check_multi_during_outage(tsdbc *c)
{
    tsdb_sid ids[2] = {sid_a, sid_root};
    for (int kind = TSDB_MULTI_VALUES; kind <= TSDB_MULTI_BUCKETS; kind++) {
        tsdb_multi_query q = {.kind = kind, .start = 0, .end = 100000, .interval_ms = 1000};
        tsdb_series_result *out = NULL;
        int rc = tsdbc_query_multi_collect(c, &q, ids, 2, NULL, &out);
        CHECK(rc == 0);
        if (rc == 0 && out) {
            CHECK(out[0].status == TSDB_ERR_UNAVAILABLE);
            CHECK(out[1].status == 0);
            tsdbc_series_results_free(out, 2);
        }
        int64_t ts = 6000 + kind;
        float v = 1;
        CHECK(tsdbc_insert_series(c, sid_root, &ts, &v, 1) == 0); /* the connection is not lost */
    }
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    const char *names[] = {"a"};
    if (outagefs_reexec(argv, base, names, 1) != 0)
        return 1;
    const char *dir_a = outagefs_dir("a");
    snprintf(ctl, sizeof ctl, "%s", getenv("OUTAGEFS_CTL"));
    snprintf(root, sizeof root, "%s.root", getenv("OUTAGEFS_CTL"));
    signal(SIGPIPE, SIG_IGN);

    tsdb *db;
    tsdb_options o;
    tsdb_options_init(&o);
    o.create_volumes = 1;
    CHECK(tsdb_open(root, &o, &db) == 0);
    uint32_t vi;
    CHECK(tsdb_volume_add(db, dir_a, &vi) == 0);
    CHECK(tsdb_series_at(db, "pump.a", dir_a, &sid_a) == 0);
    CHECK(tsdb_series(db, "pump.root", &sid_root) == 0);
    CHECK(tsdb_close(db) == 0);
    uint16_t vol_a = (uint16_t)TSDB_SID_VOLUME(sid_a);

    server = start_server(dir_a);
    tsdbc *c;
    CHECK(tsdbc_connect_timeout("127.0.0.1", port, 3000, &c) == 0);
    tsdbc_set_retry(c, 0);
    int64_t t0s = 1000;
    float v = 1;
    CHECK(tsdbc_insert_series(c, sid_a, &t0s, &v, 1) == 0);

    /* TOPOLOGY of a healthy single server: itself, both volumes available */
    tsdbc_topo *t = NULL;
    int trc = tsdbc_topology(c, &t);
    if (trc)
        fprintf(stderr, "topology: %d\n", trc);
    CHECK(trc == 0 && t);
    if (t) {
        CHECK(t->nnodes == 1 && t->nvols == 2);
        for (size_t i = 0; i < t->nvols; i++)
            CHECK(t->vols[i].health == TSDB_HEALTH_AVAILABLE && t->vols[i].role == TSDB_ROLE_OWNER);
        tsdbc_topology_free(t);
    }
    char *status = NULL;
    CHECK(tsdbc_status(c, &status) == 0 && status && strstr(status, "volumes"));
    free(status);

    /* the domain hangs and goes DOWN */
    CHECK(fault("hang", dir_a) == 0);
    msleep(4500);
    int64_t ts = 2000;
    int rc = tsdbc_insert_series(c, sid_a, &ts, &v, 1);
    CHECK(rc == TSDB_ERR_UNAVAILABLE);
    tsdbc_err_info ei;
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_UNAVAILABLE); /* mutation "drop the body": has_body is 0 */
    CHECK(ei.has_body && ei.nunavail >= 1);
    if (ei.nunavail >= 1) {
        CHECK(ei.unavail[0].vol == vol_a);
        CHECK(ei.unavail[0].reason != TSDB_WHY_NONE && ei.unavail[0].reason <= TSDB_WHY_LAST);
        CHECK(ei.unavail[0].retry_after_ms > 0);
        CHECK(ei.unavail[0].since_ms > 0);
    }
    /* the root answers while A is away */
    ts = 2001;
    CHECK(tsdbc_insert_series(c, sid_root, &ts, &v, 1) == 0);
    check_multi_during_outage(c);
    check_version_gate();
    t = NULL;
    CHECK(tsdbc_topology(c, &t) == 0 && t);
    if (t) {
        int seen_a = 0;
        for (size_t i = 0; i < t->nvols; i++) {
            if (t->vols[i].vol == vol_a) {
                seen_a = 1;
                CHECK(t->vols[i].health == TSDB_HEALTH_UNAVAILABLE && t->vols[i].reason != TSDB_WHY_NONE);
            } else {
                CHECK(t->vols[i].health == TSDB_HEALTH_AVAILABLE);
            }
        }
        CHECK(seen_a);
        tsdbc_topology_free(t);
    }

    /* a budget shorter than the outage: the status comes back after about the budget */
    struct caller shortb = {.budget_ms = 1200, .ts = 3000};
    call_insert(&shortb);
    CHECK(shortb.rc == TSDB_ERR_UNAVAILABLE);
    CHECK(shortb.ms >= 600 && shortb.ms < 4000);

    /* the default budget: the call waits, the volume heals, the call succeeds (once) */
    struct caller longb = {.budget_ms = 30000, .ts = 4000};
    pthread_t th;
    pthread_create(&th, NULL, call_insert, &longb);
    msleep(1500);
    CHECK(fault("heal", dir_a) == 0);
    pthread_join(th, NULL);
    CHECK(longb.rc == 0); /* mutation "no retry": UNAVAILABLE */
    CHECK(longb.ms >= 1200 && longb.ms < 25000);

    /* the rows: 1000 and 4000 of A, not 2000 and 3000 (refused) */
    size_t n = 0;
    int qrc = -1;
    for (int i = 0; i < 100 && qrc; i++) {
        n = 0;
        qrc = tsdbc_query_raw(c, sid_a, 0, 100000, count_cb, &n);
        if (qrc)
            msleep(100);
    }
    CHECK(qrc == 0 && n == 2);

    tsdbc_close(c);
    CHECK(kill(server, SIGTERM) == 0);
    int st;
    waitpid(server, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    fprintf(stderr, "test_unavailable: %s\n", failures ? "FAILED" : "ok");
    return failures ? 1 : 0;
}
