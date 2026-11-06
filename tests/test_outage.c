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
 *   busy   (no fault) 16 connections read one volume: a busy volume is not an unavailable one, nothing is refused.
 *   packed (during the hang) 16 connections send packed multi-series inserts to A: B keeps its pace anyway.
 *   diverged  a part of A is removed while A is down: A comes back DIVERGED (refused, the server stays), and a second
 *          outage of its domain does not recover it again.
 *   eiodrop  writes on A are dropped and the first fsync of each descriptor fails (fsyncgate), then the server is
 *          killed and restarted: everything acknowledged is still there (an engine that retried its fsync fails this).
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
        const char *argv[32] = {"tsdbd", "--dir", root, "--port", "0", "--port-file", pf, "--workers", "8", "--no-auth",
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
    /* the counts are read by the main thread while the writer runs: an entry is stored before its count grows */
    int64_t acked[20000];
    _Atomic int64_t nacked;         /* the server said ok */
    int64_t refused[20000];
    _Atomic int64_t nrefused;       /* UNAVAILABLE: not applied */
    _Atomic int64_t maybe, worst_ok_ms; /* indeterminate answers; the slowest acknowledged call */
    int refused_fast;               /* UNAVAILABLE answers that came in under 500 ms */
} side;

static side sa, sb;
static atomic_bool stop_writers, stop_extra;
static _Atomic int64_t extra_ok, extra_unavail, extra_other;

typedef struct seen {
    int64_t *ts;
    size_t n, cap;
} seen;
static int collect_extra(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ctx, (void)ts, (void)v, (void)n;
    return 0;
}

/* 16 of these read A (busy) or send packed multi-series inserts to A (packed) */
static void *extra_main(void *arg)
{
    intptr_t k = (intptr_t)arg, packed = k >= 1000;
    tsdbc *c;
    if (tsdbc_connect_timeout("127.0.0.1", port, 3000, &c))
        return NULL;
    int64_t base = 3000000000LL + k * 1000000LL;
    while (!atomic_load(&stop_extra)) {
        int rc;
        if (packed) {
            tsdb_sid ids[2] = {sa.id, sa.id};
            int64_t ts[2] = {base, base + 1};
            float v[2] = {1, 2};
            base += 2;
            rc = tsdbc_insert(c, ids, ts, v, 2);
        } else {
            seen got = {0};
            rc = tsdbc_query_raw(c, sa.id, 0, 100, collect_extra, &got);
            free(got.ts);
        }
        if (rc == 0)
            atomic_fetch_add(&extra_ok, 1);
        else if (rc == TSDB_ERR_UNAVAILABLE) {
            atomic_fetch_add(&extra_unavail, 1);
            msleep(20);
        } else {
            atomic_fetch_add(&extra_other, 1);
            msleep(20);
        }
    }
    tsdbc_close(c);
    return NULL;
}

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
            int64_t n = atomic_load(&s->nacked);
            if (n < 20000) {
                s->acked[n] = ts;
                atomic_store(&s->nacked, n + 1);
            }
            if (dt > atomic_load(&s->worst_ok_ms))
                atomic_store(&s->worst_ok_ms, (int64_t)dt);
        } else if (rc == TSDB_ERR_UNAVAILABLE) {
            int64_t n = atomic_load(&s->nrefused);
            if (n < 20000) {
                s->refused[n] = ts;
                atomic_store(&s->nrefused, n + 1);
            }
            if (dt < 500)
                s->refused_fast++;
            msleep(20);
        } else {
            atomic_fetch_add(&s->maybe, 1);
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
    /* The writers go on while this runs: judge only what was acknowledged or refused before the read began (a row
     * acknowledged after the query started is not expected in its answer). */
    int64_t nacked = s->nacked, nrefused = s->nrefused;
    atomic_thread_fence(memory_order_seq_cst);
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
    for (int64_t i = 0; i < nacked; i++)
        if (!has(&got, s->acked[i]))
            lost++;
    for (int64_t i = 0; i < nrefused; i++)
        if (has(&got, s->refused[i]))
            ghost++;
    fprintf(stderr, "%s: %s: %lld acknowledged, %lld refused, %lld indeterminate, %zu rows; lost %lld, refused but visible %lld\n",
            what, s->name, (long long)nacked, (long long)nrefused, (long long)s->maybe, got.n, (long long)lost,
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

    /* 0. a busy volume with its domain UP refuses nothing (more callers than the four a hung one may hold) */
    pthread_t xs[16];
    for (int i = 0; i < 16; i++)
        pthread_create(&xs[i], NULL, extra_main, (void *)(intptr_t)i);
    msleep(2000);
    atomic_store(&stop_extra, true);
    for (int i = 0; i < 16; i++)
        pthread_join(xs[i], NULL);
    fprintf(stderr, "busy: %lld ok, %lld unavailable, %lld other\n", (long long)extra_ok, (long long)extra_unavail,
            (long long)extra_other);
    CHECK(extra_ok > 100);
    CHECK(extra_unavail == 0);
    atomic_store(&stop_extra, false);
    extra_ok = extra_unavail = extra_other = 0;

    /* 1. hang A: B goes on at its pace; A's callers get UNAVAILABLE once the domain is DOWN; heal; A is back */
    int64_t b_before = sb.nacked, a_before = sa.nacked;
    CHECK(fault("hang", dir_a, NULL) == 0);
    for (int i = 0; i < 16; i++) /* packed inserts to A: they hang inside it, and must not take every worker */
        pthread_create(&xs[i], NULL, extra_main, (void *)(intptr_t)(1000 + i));
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
    /* and at its latency: the packed inserts of A may hold every worker until the domain is seen to be in trouble
     * (about a second), but not the whole outage (6 s here, as it was when they were not counted) */
    CHECK(sb.worst_ok_ms < 3000);
    CHECK(sa.nacked - a_before <= 1);           /* nothing on A was acknowledged while it hung */
    CHECK(alive());
    atomic_store(&stop_extra, true);
    CHECK(fault("heal", dir_a, NULL) == 0);
    for (int i = 0; i < 16; i++)
        pthread_join(xs[i], NULL);
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

    /* 2b. the kernel drops the pages of a failed writeback (fsyncgate): writes on A are thrown away, the first fsync
     * of each descriptor fails and later ones succeed. A volume that retried its fsync would acknowledge rows that
     * are not on the storage; the server is killed afterwards, so only what reached the disk is left to read */
    int64_t a_drop = sa.nacked;
    CHECK(fault("eiodrop", dir_a, NULL) == 0);
    msleep(3000);
    CHECK(alive());
    CHECK(sa.nacked - a_drop <= 1);
    CHECK(fault("heal", dir_a, NULL) == 0);
    CHECK(await_writes(&sa, 40000) == 0);
    msleep(1500);

    atomic_store(&stop_writers, true);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    verify("eio", &sa);
    verify("eio", &sb);
    CHECK(alive());
    kill(server, SIGKILL);
    waitpid(server, NULL, 0);
    server = start_server(dir_a, dir_b);
    verify("restart", &sa);
    verify("restart", &sb);

    /* 3. a part of A lost while A is down: DIVERGED, and a second outage does not resume it */
    {
        tsdbc *c;
        CHECK(tsdbc_connect_timeout("127.0.0.1", port, 3000, &c) == 0);
        CHECK(c && tsdbc_flush(c) == 0); /* the rows so far are in parts */
        CHECK(fault("hang", dir_a, NULL) == 0);
        msleep(3500); /* DOWN */
        /* without the shim in the child: its init would take over the control socket */
        char cmd[PATH_MAX + 64];
        snprintf(cmd, sizeof cmd, "rm -f '%s'/p*.part", dir_a);
        pid_t rm = fork();
        if (rm == 0) {
            unsetenv("LD_PRELOAD");
            execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
            _exit(127);
        }
        int rst = -1;
        CHECK(rm > 0 && waitpid(rm, &rst, 0) == rm && WIFEXITED(rst) && WEXITSTATUS(rst) == 0);
        CHECK(fault("heal", dir_a, NULL) == 0);
        msleep(6000); /* RECOVERING, the reopen, the report */
        int64_t ts = 4000000000LL;
        float v = 1;
        int rc = c ? tsdbc_insert_series(c, sa.id, &ts, &v, 1) : -1;
        CHECK(rc == TSDB_ERR_UNAVAILABLE);
        CHECK(fault("hang", dir_a, NULL) == 0);
        msleep(3500);
        CHECK(fault("heal", dir_a, NULL) == 0);
        msleep(8000); /* long enough for a second recovery to have resumed it, as it once did */
        ts++;
        rc = c ? tsdbc_insert_series(c, sa.id, &ts, &v, 1) : -1;
        CHECK(rc == TSDB_ERR_UNAVAILABLE);
        ts = 4000000100LL; /* B is untouched */
        rc = c ? tsdbc_insert_series(c, sb.id, &ts, &v, 1) : -1;
        CHECK(rc == 0);
        if (c)
            tsdbc_close(c);
    }
    CHECK(alive());
    kill(server, SIGTERM);
    int st;
    waitpid(server, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    fprintf(stderr, "test_outage: %s\n", failures ? "FAILED" : "ok");
    return failures ? 1 : 0;
}
