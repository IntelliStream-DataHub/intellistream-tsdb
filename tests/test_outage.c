/* tsdbd with two volumes on two storage domains under the outagefs shim (ha.md 15.3 and 15.5, N1).
 *
 * The root is local; volume A and volume B each are a domain (--health-domain). One writer thread per volume inserts
 * rows (ts = counter) and records what the server said: acknowledged, refused with TSDB_ERR_UNAVAILABLE ("definitely
 * not applied"), or anything else (indeterminate). The faults come from the shim's control socket. After each one
 * the rows are read back and checked: every acknowledged row is there, no refused row is, and the server is still the
 * process this test started.
 *
 *   hang   A hangs for a few seconds longer than the DOWN threshold, then heals: B's writes keep their latency, A's
 *          answer UNAVAILABLE quickly once it is DOWN, the call that hung finishes, A recovers by itself.
 *   eio    every call on A fails: the engine fails stop, the server does not exit (the volume is in a domain), A
 *          recovers after the heal by a reopen, and what was acknowledged before is there.
 *
 * Usage: test_outage [scratch directory]  (re-executes itself under build/liboutagefs.so; TSDBD, TSDBD_IO as in
 * test_server) */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include <limits.h>
#include "harness.h"
#include "outagefs_ctl.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
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

static int fault(const char *cmd, const char *dir, const char *arg)
{
    char line[600], out[256];
    snprintf(line, sizeof line, "%s %s%s%s", cmd, dir, arg ? " " : "", arg ? arg : "");
    int rc = outagefs_cmd(ctl, line, out, sizeof out);
    if (rc)
        fprintf(stderr, "ctl '%s': %s\n", line, out);
    return rc;
}

static pid_t start_server(const char *dir_a, const char *dir_b)
{
    char pf[PATH_MAX + 64], follow[64], da[PATH_MAX + 32], db_[PATH_MAX + 32];
    th_portfile(pf, sizeof pf, root);
    th_follow_me(follow, sizeof follow);
    snprintf(da, sizeof da, "%s", dir_a);
    snprintf(db_, sizeof db_, "%s", dir_b);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    setenv("TSDBD_TEST_HEALTH", "down=2500,read=200,write=500,recover=1500,fast=400,spawn=60000", 1);
    pid_t pid = fork();
    if (pid == 0) {
        const char *argv[32] = {"tsdbd", "--dir", root, "--port", "0", "--port-file", pf, "--workers", "4", "--no-auth",
                                follow, "--health-domain", da, "--health-domain", db_};
        int n = 15;
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

/* what the writers saw of one series */
typedef struct side {
    tsdb_sid id;
    const char *name;
    int64_t next;                   /* the next ts to write */
    int64_t acked[20000], nacked;   /* the server said ok */
    int64_t refused[20000], nrefused; /* UNAVAILABLE: not applied */
    int64_t maybe, worst_ok_ms;     /* indeterminate answers; the slowest acknowledged call */
    int refused_fast;               /* UNAVAILABLE answers that came in under 500 ms */
} side;

static side sa, sb;
static atomic_bool stop_writers;

static void *writer(void *arg)
{
    side *s = arg;
    tsdbc *c;
    if (tsdbc_connect_timeout("127.0.0.1", port, 3000, &c))
        return NULL;
    while (!atomic_load(&stop_writers)) {
        int64_t ts = s->next++;
        float v = (float)ts;
        double t0 = now_ms();
        int rc = tsdbc_insert_series(c, s->id, &ts, &v, 1);
        double dt = now_ms() - t0;
        if (rc == 0) {
            if (s->nacked < 20000)
                s->acked[s->nacked++] = ts;
            if (dt > s->worst_ok_ms)
                s->worst_ok_ms = (int64_t)dt;
        } else if (rc == TSDB_ERR_UNAVAILABLE) {
            if (s->nrefused < 20000)
                s->refused[s->nrefused++] = ts;
            if (dt < 500)
                s->refused_fast++;
            msleep(20);
        } else {
            s->maybe++;
            msleep(20);
            if (rc == TSDB_ERR_IO) { /* the connection may be gone */
                tsdbc_close(c);
                c = NULL;
                for (int i = 0; i < 50 && !c; i++)
                    if (tsdbc_connect_timeout("127.0.0.1", port, 1000, &c))
                        c = NULL, msleep(100);
                if (!c)
                    return NULL;
            }
        }
        msleep(5);
    }
    tsdbc_close(c);
    return NULL;
}

typedef struct seen {
    int64_t *ts;
    size_t n, cap;
} seen;
static int collect(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    seen *s = ctx;
    (void)v;
    for (size_t i = 0; i < n; i++) {
        if (s->n == s->cap) {
            s->cap = s->cap ? s->cap * 2 : 1024;
            s->ts = realloc(s->ts, s->cap * sizeof *s->ts);
        }
        s->ts[s->n++] = ts[i];
    }
    return 0;
}

static int has(const seen *s, int64_t ts)
{
    size_t lo = 0, hi = s->n;
    while (lo < hi) {
        size_t m = (lo + hi) / 2;
        if (s->ts[m] < ts)
            lo = m + 1;
        else
            hi = m;
    }
    return lo < s->n && s->ts[lo] == ts;
}

/* Reads the series back (retrying while the volume is unavailable) and checks the history. */
static void verify(const char *what, side *s)
{
    tsdbc *c;
    CHECK(tsdbc_connect_timeout("127.0.0.1", port, 3000, &c) == 0);
    if (!c)
        return;
    seen got = {0};
    int rc = -1;
    for (int i = 0; i < 300 && rc != 0; i++) {
        got.n = 0;
        rc = tsdbc_query_raw(c, s->id, 0, INT64_MAX / 2, collect, &got);
        if (rc)
            msleep(100);
    }
    if (rc) {
        fprintf(stderr, "%s: %s: read back failed: %d\n", what, s->name, rc);
        failures++;
    }
    int64_t lost = 0, ghost = 0;
    for (int64_t i = 0; i < s->nacked; i++)
        if (!has(&got, s->acked[i]))
            lost++;
    for (int64_t i = 0; i < s->nrefused; i++)
        if (has(&got, s->refused[i]))
            ghost++;
    fprintf(stderr, "%s: %s: %lld acknowledged, %lld refused, %lld indeterminate, %zu rows; lost %lld, refused but visible %lld\n",
            what, s->name, (long long)s->nacked, (long long)s->nrefused, (long long)s->maybe, got.n, (long long)lost,
            (long long)ghost);
    CHECK(lost == 0);
    CHECK(ghost == 0);
    free(got.ts);
    tsdbc_close(c);
}

static int alive(void) { return waitpid(server, NULL, WNOHANG) == 0; }

/* Waits until a write on s is acknowledged (the volume recovered). */
static int await_writes(side *s, int timeout_ms)
{
    tsdbc *c;
    if (tsdbc_connect_timeout("127.0.0.1", port, 3000, &c))
        return -1;
    double t0 = now_ms();
    int rc = -1;
    while (now_ms() - t0 < timeout_ms) {
        int64_t ts = 1000000000 + (int64_t)(now_ms());
        float v = 0;
        rc = tsdbc_insert_series(c, s->id, &ts, &v, 1);
        if (rc == 0)
            break;
        msleep(100);
    }
    tsdbc_close(c);
    return rc;
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    const char *names[] = {"a", "b"};
    if (outagefs_reexec(argv, base, names, 2) != 0)
        return 1;
    const char *dir_a = outagefs_dir("a"), *dir_b = outagefs_dir("b");
    snprintf(ctl, sizeof ctl, "%s", getenv("OUTAGEFS_CTL"));
    snprintf(root, sizeof root, "%s.root", getenv("OUTAGEFS_CTL"));
    signal(SIGPIPE, SIG_IGN);

    /* the database: a root, two volumes, a series in each */
    tsdb *db;
    tsdb_options o;
    tsdb_options_init(&o);
    o.create_volumes = 1;
    CHECK(tsdb_open(root, &o, &db) == 0);
    uint32_t vi;
    CHECK(tsdb_volume_add(db, dir_a, &vi) == 0);
    CHECK(tsdb_volume_add(db, dir_b, &vi) == 0);
    sa.name = "pump.a";
    sb.name = "pump.b";
    CHECK(tsdb_series_at(db, sa.name, dir_a, &sa.id) == 0);
    CHECK(tsdb_series_at(db, sb.name, dir_b, &sb.id) == 0);
    CHECK(tsdb_close(db) == 0);

    server = start_server(dir_a, dir_b);
    pthread_t ta, tb;
    pthread_create(&ta, NULL, writer, &sa);
    pthread_create(&tb, NULL, writer, &sb);
    msleep(1500);
    CHECK(sa.nacked > 10 && sb.nacked > 10);

    /* 1. hang A: B goes on at its pace; A's callers get UNAVAILABLE once the domain is DOWN; heal; A is back */
    int64_t b_before = sb.nacked, a_before = sa.nacked;
    CHECK(fault("hang", dir_a, NULL) == 0);
    msleep(4500); /* past the DOWN threshold (2.5 s here); A's own writer is inside a hung call until the heal */
    /* a request that arrives now is refused at once: DOWN quiesces the volume, and at most four wait inside it */
    tsdbc *pc;
    CHECK(tsdbc_connect_timeout("127.0.0.1", port, 3000, &pc) == 0);
    for (int i = 0; i < 3 && pc; i++) {
        int64_t ts = 2000000000 + i;
        float v = 1;
        double t0 = now_ms();
        int rc = tsdbc_insert_series(pc, sa.id, &ts, &v, 1);
        CHECK(rc == TSDB_ERR_UNAVAILABLE);
        CHECK(now_ms() - t0 < 500);
    }
    if (pc)
        tsdbc_close(pc);
    msleep(2500);
    CHECK(sb.nacked - b_before > 100);          /* B kept writing */
    CHECK(sb.worst_ok_ms < 1000);               /* and at its latency */
    CHECK(sa.nacked - a_before <= 1);           /* nothing on A was acknowledged while it hung */
    CHECK(alive());
    CHECK(fault("heal", dir_a, NULL) == 0);
    CHECK(await_writes(&sa, 30000) == 0);
    msleep(1000);
    CHECK(alive());
    verify("hang", &sa);
    verify("hang", &sb);

    /* 2. eio on A: the engine fails stop; the server stays (A is in a domain) and A recovers after the heal */
    int64_t a_mid = sa.nacked;
    CHECK(fault("eio", dir_a, NULL) == 0);
    msleep(4000);
    CHECK(alive());
    CHECK(sa.nacked - a_mid <= 1);
    CHECK(fault("heal", dir_a, NULL) == 0);
    CHECK(await_writes(&sa, 40000) == 0);
    msleep(1000);
    CHECK(alive());
    int64_t a_after = sa.nacked;
    msleep(1500);
    CHECK(sa.nacked > a_after); /* writes go on */

    atomic_store(&stop_writers, true);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    verify("eio", &sa);
    verify("eio", &sb);
    CHECK(alive());
    kill(server, SIGTERM);
    int st;
    waitpid(server, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    fprintf(stderr, "test_outage: %s\n", failures ? "FAILED" : "ok");
    return failures ? 1 : 0;
}
