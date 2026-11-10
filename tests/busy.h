/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef TSDB_TEST_BUSY_H
#define TSDB_TEST_BUSY_H
/* A test whose thresholds are timings (the storage health probes of test_health, test_outage and test_unavailable:
 * hundreds of ms scaled down from seconds, against a real disk) says nothing when its own threads were not served: a
 * probe thread that waits for a CPU, or an fsync that waits for the disk, for that long is a slow probe, and a domain
 * that no fault touched goes DEGRADED or DOWN. Such a test runs as an outer process that forks the real run. The rules
 * that keep this from hiding a bug:
 *
 *  1. Two kinds of check. CHECK (TH_CHECK) is a verdict about the code: a lost row, a refused row that shows up, a
 *     wrong answer, a wrong state, a missing file. It fails the run at once, on any machine, and the run is never
 *     repeated. CHECK_T (TH_CHECK_T) is a timing check: "this happened within N ms". Only these can be excused.
 *  2. A timing failure is excused only by evidence from THIS run, taken over the window of the failing check (from the
 *     end of the previous check to the failure): the run's own gauge (below) saw a thread woken 30 ms late, a thread
 *     wait 30 ms for a CPU in one 100 ms slot (run_delay in /proc/<tid>/schedstat), or a canary fdatasync on the
 *     scratch file system take 100 ms. The pressure of the whole machine (/proc/pressure) is printed and never
 *     enough: on a dev box it is above 20% with nothing of the test in it, and a logic bug that failed one run in three
 *     passed every time behind it.
 *     CHECK_D (TH_CHECK_D) is a verdict that depends on a timed step before it (see below).
 *  3. The run exits TH_EXIT_BUSY (and the outer process runs it again, after a pause, up to four times in all, with a
 *     line that says so) only when it had no plain failure and every timing failure was excused. Else it exits 1.
 *
 * The gauge is a thread (th_gauge_start) that every 10 ms notes how late it was woken, and every 100 ms how long an
 * fdatasync of a scratch file took and how long each thread of this process (and of the pids given to
 * th_gauge_watch_pid, the server under test) waited in the run queue, into slots of 100 ms. Known limit: a bug that
 * itself starves the CPU (a spin) or hammers the disk can make the gauge see load it caused; test_health bounds the CPU
 * time of its probe threads for the first. Usage: `return th_run_retrying("test_x", argc, argv, run_test);` as main,
 * th_gauge_start(scratch) in run_test, the CHECK macros below, `return th_finish("test_x");` at the end. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "watchdog.h"

#define TH_EXIT_BUSY 3
#define TH_ATTEMPTS 4        /* runs in all */
#define TH_BUSY_WAKE_MS 30   /* a thread woken this late (or that waited this long for a CPU in a slot): busy CPUs */
#define TH_BUSY_FSYNC_MS 100 /* a canary fdatasync this slow: a busy disk */
#define TH_SLOT_MS 100
#define TH_NSLOT 1024 /* 100 s of history */

/* ---- the run's own evidence ---- */
enum { TH_EV_WAKE, TH_EV_RUNQ, TH_EV_FSYNC };
typedef struct {
    long idx; /* now_ms / TH_SLOT_MS + 1; 0 = unused */
    int v[3];
} th_slot;
static th_slot th_slots[TH_NSLOT];
static pthread_mutex_t th_ev_mu = PTHREAD_MUTEX_INITIALIZER;
static int th_nplain, th_ntiming, th_nexcused;
static long th_last_mark_ms;

static inline long th_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Notes a measurement of `kind` (milliseconds) at this moment. A test may feed its own (test_health's nap). */
static inline void th_note(int kind, int ms)
{
    long idx = th_now_ms() / TH_SLOT_MS + 1;
    pthread_mutex_lock(&th_ev_mu);
    th_slot *s = &th_slots[idx % TH_NSLOT];
    if (s->idx != idx) {
        memset(s, 0, sizeof *s);
        s->idx = idx;
    }
    if (ms > s->v[kind])
        s->v[kind] = ms;
    pthread_mutex_unlock(&th_ev_mu);
}

/* The largest of each kind over [t0, t1] (ms, th_now_ms's clock); returns whether any is over its threshold. */
static inline int th_evidence(long t0, long t1, int *wake, int *runq, int *fsync)
{
    int m[3] = {0, 0, 0};
    long a = t0 / TH_SLOT_MS + 1, b = t1 / TH_SLOT_MS + 1;
    if (b - a >= TH_NSLOT)
        a = b - TH_NSLOT + 1;
    pthread_mutex_lock(&th_ev_mu);
    for (long i = a; i <= b; i++) {
        th_slot *s = &th_slots[i % TH_NSLOT];
        if (s->idx != i)
            continue;
        for (int k = 0; k < 3; k++)
            if (s->v[k] > m[k])
                m[k] = s->v[k];
    }
    pthread_mutex_unlock(&th_ev_mu);
    *wake = m[TH_EV_WAKE];
    *runq = m[TH_EV_RUNQ];
    *fsync = m[TH_EV_FSYNC];
    return m[TH_EV_WAKE] >= TH_BUSY_WAKE_MS || m[TH_EV_RUNQ] >= TH_BUSY_WAKE_MS || m[TH_EV_FSYNC] >= TH_BUSY_FSYNC_MS;
}

/* The "some" pressure of `res` ("cpu" or "io") for `key` ("avg10=" or "avg60="): whole percent, 0 where there is none.
 * Printed with a failure, never a reason to excuse it. */
static inline int th_pressure(const char *res, const char *key)
{
    char path[320], b[256];
    const char *dir = getenv("TH_PRESSURE_DIR"); /* test_busy's fake files */
    snprintf(path, sizeof path, "%s/%s", dir ? dir : "/proc/pressure", res);
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return 0;
    ssize_t n = read(fd, b, sizeof b - 1);
    close(fd);
    if (n <= 0)
        return 0;
    b[n] = 0;
    const char *q = strstr(b, key); /* the first line is "some" */
    return q ? atoi(q + strlen(key)) : 0;
}

/* The checks. th_mark ends the window of the next timing check: it starts where the last check ended. */
static inline void th_mark(void)
{
    __atomic_store_n(&th_last_mark_ms, th_now_ms(), __ATOMIC_RELAXED);
}

static inline void th_fail_plain(const char *file, int line, const char *what)
{
    fprintf(stderr, "%s:%d: FAILED: %s\n", file, line, what);
    __atomic_fetch_add(&th_nplain, 1, __ATOMIC_RELAXED);
    th_mark();
}

static inline void th_fail_timing(const char *file, int line, const char *what)
{
    long now = th_now_ms(), t0 = __atomic_load_n(&th_last_mark_ms, __ATOMIC_RELAXED);
    if (t0 <= 0 || t0 > now - 1000)
        t0 = now - 1000;
    if (t0 < now - 60000)
        t0 = now - 60000;
    int wake, runq, fsync;
    int ex = th_evidence(t0, now, &wake, &runq, &fsync);
    fprintf(stderr,
            "%s:%d: FAILED (timing): %s\n  in the last %ld ms of this run: largest wake-up delay %d ms, longest wait for a "
            "CPU in a %d ms slot %d ms, slowest canary fdatasync %d ms: %s\n",
            file, line, what, now - t0, wake, TH_SLOT_MS, runq, fsync,
            ex ? "this run saw a busy machine" : "no sign of load in this run, the failure stands");
    __atomic_fetch_add(&th_ntiming, 1, __ATOMIC_RELAXED);
    if (ex)
        __atomic_fetch_add(&th_nexcused, 1, __ATOMIC_RELAXED);
    th_mark();
}

/* CHECK: a verdict about the code. Never excused, never repeated. */
#define TH_CHECK(cond)                                                                                                 \
    do {                                                                                                               \
        if (!(cond))                                                                                                   \
            th_fail_plain(__FILE__, __LINE__, #cond);                                                                  \
        else                                                                                                           \
            th_mark();                                                                                                 \
    } while (0)
/* CHECK_T: "within N ms". Excused only by this run's own evidence of load, over the window of the check. */
#define TH_CHECK_T(cond)                                                                                               \
    do {                                                                                                               \
        if (!(cond))                                                                                                   \
            th_fail_timing(__FILE__, __LINE__, #cond);                                                                 \
        else                                                                                                           \
            th_mark();                                                                                                 \
    } while (0)

/* CHECK_D: a verdict that reads what a timed step before it was to bring about (the state a wait was waiting for). With
 * no timing failure before it, it is a CHECK; after one, a failure of it follows from that one, and is judged like it. */
#define TH_CHECK_D(cond)                                                                                               \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            if (__atomic_load_n(&th_ntiming, __ATOMIC_RELAXED) > 0)                                                    \
                th_fail_timing(__FILE__, __LINE__, #cond);                                                             \
            else                                                                                                       \
                th_fail_plain(__FILE__, __LINE__, #cond);                                                              \
        } else                                                                                                         \
            th_mark();                                                                                                 \
    } while (0)

/* The run's end: 0 without failures; TH_EXIT_BUSY when there was no plain failure and every timing failure was excused
 * (the outer process runs it again); else 1. The pressure of the machine is printed beside it. */
static inline int th_finish(const char *name)
{
    int plain = th_nplain, tim = th_ntiming, ex = th_nexcused;
    if (!plain && !tim)
        return 0;
    fprintf(stderr,
            "%s: %d plain failures, %d timing failures (%d excused by this run's own evidence); machine pressure: CPU "
            "%d%% IO %d%% (10 s), IO %d%% (60 s)\n",
            name, plain, tim, ex, th_pressure("cpu", "avg10="), th_pressure("io", "avg10="), th_pressure("io", "avg60="));
    if (plain || ex < tim)
        return 1;
    return TH_EXIT_BUSY;
}

/* ---- the gauge thread ---- */
#define TH_MAXWATCH 8
#define TH_MAXTID 1024
static char th_gauge_dir[1024];
static pid_t th_gauge_pids[TH_MAXWATCH];
static int th_gauge_npids;
static struct {
    pid_t pid, tid;
    unsigned long long rd; /* run_delay ns at the last look */
} th_tids[TH_MAXTID];
static int th_ntids;
static volatile pid_t th_gauge_tid; /* the gauge's own thread, for a test that sums the CPU time of the others */

/* A server under test: its threads' waits for a CPU count too. */
static inline void th_gauge_watch_pid(pid_t pid)
{
    pthread_mutex_lock(&th_ev_mu);
    if (th_gauge_npids < TH_MAXWATCH)
        th_gauge_pids[th_gauge_npids++] = pid;
    pthread_mutex_unlock(&th_ev_mu);
}

static void th_gauge_runq_pid(pid_t pid)
{
    char path[96];
    snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
    DIR *d = opendir(path);
    if (!d)
        return;
    struct dirent *e;
    int worst = 0;
    while ((e = readdir(d))) {
        pid_t tid = (pid_t)atoi(e->d_name);
        if (tid <= 0)
            continue;
        snprintf(path, sizeof path, "/proc/%d/task/%d/schedstat", (int)pid, (int)tid);
        FILE *f = fopen(path, "r");
        if (!f)
            continue;
        unsigned long long run = 0, rd = 0;
        int ok = fscanf(f, "%llu %llu", &run, &rd) == 2;
        fclose(f);
        if (!ok)
            continue;
        int i = 0;
        while (i < th_ntids && !(th_tids[i].pid == pid && th_tids[i].tid == tid))
            i++;
        if (i == th_ntids) {
            if (th_ntids == TH_MAXTID)
                continue;
            th_tids[th_ntids].pid = pid;
            th_tids[th_ntids].tid = tid;
            th_tids[th_ntids].rd = rd; /* a new thread: its first look is the baseline */
            th_ntids++;
            continue;
        }
        if (rd > th_tids[i].rd) {
            int ms = (int)((rd - th_tids[i].rd) / 1000000ULL);
            if (ms > worst)
                worst = ms;
        }
        th_tids[i].rd = rd;
    }
    closedir(d);
    if (worst > 0)
        th_note(TH_EV_RUNQ, worst);
}

static void *th_gauge_main(void *arg)
{
    (void)arg;
    th_gauge_tid = (pid_t)syscall(SYS_gettid);
    int fd = -1;
    if (th_gauge_dir[0]) {
        char p[1100];
        snprintf(p, sizeof p, "%s/.th_canary.%d", th_gauge_dir, (int)getpid());
        fd = open(p, O_RDWR | O_CREAT | O_TRUNC, 0600);
        if (fd >= 0)
            unlink(p); /* the descriptor keeps it: nothing left behind */
    }
    char page[4096];
    memset(page, 0x5a, sizeof page);
    for (long tick = 0;; tick++) {
        struct timespec ts = {0, 10 * 1000000L}, a, b;
        clock_gettime(CLOCK_MONOTONIC, &a);
        nanosleep(&ts, NULL);
        clock_gettime(CLOCK_MONOTONIC, &b);
        int late = (int)((b.tv_sec - a.tv_sec) * 1000 + (b.tv_nsec - a.tv_nsec) / 1000000) - 10;
        if (late > 0)
            th_note(TH_EV_WAKE, late);
        if (tick % 10 == 9) {
            if (fd >= 0) {
                clock_gettime(CLOCK_MONOTONIC, &a);
                ssize_t w = pwrite(fd, page, sizeof page, (off_t)((tick / 10) % 64) * 4096);
                int sy = fdatasync(fd);
                clock_gettime(CLOCK_MONOTONIC, &b);
                if (w == (ssize_t)sizeof page && sy == 0)
                    th_note(TH_EV_FSYNC, (int)((b.tv_sec - a.tv_sec) * 1000 + (b.tv_nsec - a.tv_nsec) / 1000000));
            }
            th_gauge_runq_pid(getpid());
            pthread_mutex_lock(&th_ev_mu);
            int n = th_gauge_npids;
            pid_t ps[TH_MAXWATCH];
            memcpy(ps, th_gauge_pids, sizeof ps);
            pthread_mutex_unlock(&th_ev_mu);
            for (int i = 0; i < n; i++)
                th_gauge_runq_pid(ps[i]);
        }
    }
    return NULL;
}

/* Starts the gauge; `scratch_dir` (may be NULL: no canary) is a directory on the file system the test writes to. */
static inline void th_gauge_start(const char *scratch_dir)
{
    snprintf(th_gauge_dir, sizeof th_gauge_dir, "%s", scratch_dir ? scratch_dir : "");
    pthread_t t;
    if (pthread_create(&t, NULL, th_gauge_main, NULL) == 0)
        pthread_detach(t);
    th_mark();
}

static inline int th_run_retrying(const char *name, int argc, char **argv, int (*run)(int, char **))
{
    if (getenv("TH_RETRY_INNER") || getenv("OUTAGEFS_CTL")) /* the run itself (the shim's exec is one too) */
        return run(argc, argv);
    char wd[96];
    snprintf(wd, sizeof wd, "%s (outer)", name);
    th_watchdog(wd, 3600);
    setenv("TH_RETRY_INNER", "1", 1);
    int rc = 1;
    const char *ps = getenv("TH_RETRY_PAUSE_S");
    unsigned pause_s = ps ? (unsigned)atoi(ps) : 20;
    for (int attempt = 1; attempt <= TH_ATTEMPTS; attempt++) {
        fflush(NULL);
        pid_t pid = fork();
        if (pid < 0)
            return 1;
        if (pid == 0) {
            execv("/proc/self/exe", argv);
            _exit(127);
        }
        int st = 0;
        if (waitpid(pid, &st, 0) != pid)
            return 1;
        rc = WIFEXITED(st) ? WEXITSTATUS(st) : 1;
        if (rc != TH_EXIT_BUSY)
            break;
        fprintf(stderr, "%s: attempt %d: every failure was a timing check and this run saw a busy machine: %s\n", name,
                attempt, attempt < TH_ATTEMPTS ? "running it again after a pause" : "no more attempts: the failure stands");
        if (attempt < TH_ATTEMPTS)
            sleep(pause_s);
    }
    return rc == TH_EXIT_BUSY ? 1 : rc;
}
#endif
