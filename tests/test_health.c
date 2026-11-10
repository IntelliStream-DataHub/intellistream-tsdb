/* SPDX-License-Identifier: AGPL-3.0-only */
/* Storage domain health (src/health.c, ha.md 15.2): probes, the states and what moves between them, against the
 * outagefs shim. Three domains, A and A2 (two filesystems of one server) and B (another server). The thresholds are
 * scaled down so the whole test takes about 30 s. Usage: test_health [scratch directory]. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "outagefs_ctl.h"

#include <dirent.h>
#include "busy.h"
#include "watchdog.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>

/* CHECK is a verdict about the code and fails the run at once; CHECK_T is "within N ms", excused only by this run's own
 * evidence of load (busy.h). */
#define CHECK(cond) TH_CHECK(cond)
#define CHECK_T(cond) TH_CHECK_T(cond)
#define CHECK_TW(cond, t0, t1) TH_CHECK_TW(cond, t0, t1)

static char ctl[256];
static tsdb_health *h;
static int dom_a, dom_a2, dom_b;

static atomic_int changes;
static pthread_mutex_t lg_mu = PTHREAD_MUTEX_INITIALIZER;
static char lg[8192];
static long b_change_ms; /* th_now_ms of the first change of domain 2 (B: only ENOSPC touches it), 0 = none */

static void on_change(void *ctx, int dom, int old, int nw)
{
    (void)ctx;
    atomic_fetch_add(&changes, 1);
    pthread_mutex_lock(&lg_mu);
    size_t l = strlen(lg);
    snprintf(lg + l, sizeof lg - l, "%d:%d>%d ", dom, old, nw);
    if (dom == 2 && !b_change_ms)
        b_change_ms = th_now_ms();
    pthread_mutex_unlock(&lg_mu);
}

/* With a timing failure: what each domain's last probes took, to tell a slow disk from a wrong state. */
static void diag(void)
{
    for (int d = 0; h && d < 3; d++) {
        tsdb_domain_info in;
        if (tsdb_health_info(h, d, &in) == 0)
            fprintf(stderr, "  domain %d (%s): state %d for %lld ms, last read %d ms, last write %d ms, probes %llu ok %llu failed\n",
                    d, in.name, in.state, (long long)in.age_ms, in.read_ms, in.write_ms, (unsigned long long)in.probes_ok,
                    (unsigned long long)in.probes_err);
    }
}

static int cmd(const char *verb, const char *dir, const char *arg)
{
    char line[600];
    snprintf(line, sizeof line, "%s %s%s%s", verb, dir, arg ? " " : "", arg ? arg : "");
    return outagefs_cmd(ctl, line, NULL, 0);
}

/* How late this thread was woken: the test's own gauge of a busy machine, with the gauge thread's (busy.h). The
 * thresholds below are a few hundred ms (scaled down from seconds), so a probe thread that waits for a CPU for that
 * long flips a domain to DEGRADED that no fault touched: a timing failure with load in it is not a verdict. */
static void nap(int ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L}, a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    nanosleep(&ts, NULL);
    clock_gettime(CLOCK_MONOTONIC, &b);
    int over = (int)((b.tv_sec - a.tv_sec) * 1000 + (b.tv_nsec - a.tv_nsec) / 1000000) - ms;
    if (over > 0)
        th_note(TH_EV_WAKE, over);
}

/* The CPU time (user + system, ms) of every thread of this process except the caller's, summed: the probe threads of
 * the health object (the test's own thread sleeps; the gauge is left out). A probe loop that spins instead of waiting shows here. */
static long threads_cpu_ms(void)
{
    long ticks = 0;
    DIR *d = opendir("/proc/self/task");
    if (!d)
        return -1;
    struct dirent *e;
    pid_t me = (pid_t)syscall(SYS_gettid);
    while ((e = readdir(d))) {
        int tid = atoi(e->d_name);
        if (tid <= 0 || tid == me || tid == th_gauge_tid_get())
            continue;
        char path[64], b[1024];
        snprintf(path, sizeof path, "/proc/self/task/%d/stat", tid);
        int fd = open(path, O_RDONLY);
        if (fd < 0)
            continue;
        ssize_t n = read(fd, b, sizeof b - 1);
        close(fd);
        if (n <= 0)
            continue;
        b[n] = 0;
        char *q = strrchr(b, ')'); /* the name may hold spaces; fields from the state on */
        unsigned long ut = 0, st = 0;
        if (q && sscanf(q + 2, "%*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu", &ut, &st) == 2)
            ticks += (long)(ut + st);
    }
    closedir(d);
    return ticks * 1000 / sysconf(_SC_CLK_TCK);
}

/* waits up to ms for a domain to be in a state */
static bool wait_state(int dom, int st, int ms)
{
    for (int t = 0; t < ms; t += 20) {
        if (tsdb_health_state(h, dom) == st)
            return true;
        nap(20);
    }
    return tsdb_health_state(h, dom) == st;
}

static bool stays(int dom, int st, int ms)
{
    for (int t = 0; t < ms; t += 20) {
        if (tsdb_health_state(h, dom) != st)
            return false;
        nap(20);
    }
    return true;
}

/* tsdb_health_options is public and 0.10.0 released it: a program built against that header sets fields by offset, so
 * a new field goes at the end (flap_recover_ms was once put after recover_ms and moved every later field). */
#include <stddef.h>
#if defined(__LP64__)
_Static_assert(offsetof(tsdb_health_options, recover_ms) == 12, "tsdb_health_options layout of 0.10.0");
_Static_assert(offsetof(tsdb_health_options, fast_ms) == 16, "tsdb_health_options layout of 0.10.0");
_Static_assert(offsetof(tsdb_health_options, auto_up) == 40, "tsdb_health_options layout of 0.10.0");
_Static_assert(offsetof(tsdb_health_options, node) == 48, "tsdb_health_options layout of 0.10.0");
_Static_assert(offsetof(tsdb_health_options, on_change) == 56, "tsdb_health_options layout of 0.10.0");
_Static_assert(offsetof(tsdb_health_options, ctx) == 64, "tsdb_health_options layout of 0.10.0");
#endif

static int run_test(int argc, char **argv);

/* The thresholds here are scaled down to hundreds of ms, and the test failed ("test_health:140/183/264") on a busy
 * machine with domains in DEGRADED or DOWN that no fault touched (log: 2:0>1 2:1>0, a domain with only ENOSPC): see
 * busy.h, which runs it again when a failed run saw the machine busy (its CPUs, or its disk). */
int main(int argc, char **argv) { return th_run_retrying("test_health", argc, argv, run_test); }

static int run_test(int argc, char **argv)
{
    th_watchdog("test_health", 1200);
    const char *base = argc > 1 ? argv[1] : "/tmp";
    const char *const names[] = {"a", "a2", "b"};
    if (outagefs_reexec(argv, base, names, 3))
        return 1;
    th_gauge_start(base);
    th_diag = diag;
    snprintf(ctl, sizeof ctl, "%s", getenv("OUTAGEFS_CTL"));
    const char *da = outagefs_dir("a"), *da2 = outagefs_dir("a2"), *db = outagefs_dir("b");

    tsdb_health_options o = {.down_ms = 1500,
                             .read_every_ms = 100,
                             .write_every_ms = 200,
                             .recover_ms = 1000,
                             .fast_ms = 500,
                             .spawn_ms = 2500,
                             .degraded_min_ms = 300,
                             .degraded_window_ms = 1000,
                             .degraded_exit_ms = 1500,
                             .auto_up = 1,
                             .node = "t1",
                             .on_change = on_change};
    h = tsdb_health_open(&o);
    CHECK(h != NULL);
    dom_a = tsdb_health_add_domain(h, "a", da);
    dom_a2 = tsdb_health_add_domain(h, "a2", da2);
    dom_b = tsdb_health_add_domain(h, "b", db);
    CHECK(dom_a == 0 && dom_a2 == 1 && dom_b == 2 && tsdb_health_domains(h) == 3);

    /* everything UP, probe files made, probes counted */
    long cpu0 = threads_cpu_ms();
    nap(1200);
    for (int d = 0; d < 3; d++) {
        tsdb_domain_info in;
        CHECK(tsdb_health_info(h, d, &in) == 0);
        CHECK_T(in.state == TSDB_DOM_UP);
        CHECK_T(in.probes_ok >= 5);
        CHECK(in.probes_err == 0); /* a probe that failed with no fault is a bug, slow or not */
        CHECK_T(in.threads == 1);
    }
    char p[512];
    snprintf(p, sizeof p, "%s/PROBE", da);
    CHECK(access(p, F_OK) == 0);
    snprintf(p, sizeof p, "%s/PROBE.t1", da);
    CHECK(access(p, F_OK) == 0);
    CHECK_T(atomic_load(&changes) == 0);
    /* idle: the probe threads wait between probes. A deadline in the past turns the wait into a busy loop (a thread of
     * the health object burning a CPU: the test's own thread sleeps). CPU time, not wall time: load does not move it. */
    nap(800);
    long cpu1 = threads_cpu_ms();
    fprintf(stderr, "probe threads used %ld ms of CPU in 2 s idle\n", cpu1 - cpu0);
    CHECK(cpu0 >= 0 && cpu1 >= cpu0 && cpu1 - cpu0 < 400);

    /* hang: only A goes DOWN, after about down_ms; its sibling and the other server stay UP */
    CHECK(cmd("hang", da, NULL) == 0);
    CHECK_T(stays(dom_a, TSDB_DOM_UP, 1000)); /* not yet: a probe has been out for 1 s of the 1.5 */
    CHECK_T(wait_state(dom_a, TSDB_DOM_DOWN, 2000));
    CHECK_T(tsdb_health_state(h, dom_a2) == TSDB_DOM_UP && tsdb_health_state(h, dom_b) == TSDB_DOM_UP);
    tsdb_domain_info in;
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK(in.outage_id == 1);
    CHECK_T(in.stuck_ms > 1000);
    /* stuck past spawn_ms: a second probe thread */
    nap(1500);
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK_T(in.threads == 2);
    CHECK_T(tsdb_health_state(h, dom_a) == TSDB_DOM_DOWN);
    /* heal: the stuck threads return, write probes work for recover_ms, then UP (auto_up) */
    CHECK(cmd("heal", da, NULL) == 0);
    CHECK_T(wait_state(dom_a, TSDB_DOM_RECOVERING, 2500) || tsdb_health_state(h, dom_a) == TSDB_DOM_UP);
    CHECK_T(wait_state(dom_a, TSDB_DOM_UP, 2000));
    nap(300);
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK_T(in.threads == 1);
    CHECK_T(in.age_ms < 5000);
    CHECK(in.outage_id == 1);

    /* a read-only heal is not enough: with writes failing, the domain does not leave DOWN (reads would pass) */
    CHECK(cmd("eio", da, NULL) == 0);
    CHECK_T(wait_state(dom_a, TSDB_DOM_DOWN, 2500)); /* three failed probes */
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK(in.outage_id == 2);
    CHECK_T(in.probes_err >= 3);
    CHECK(cmd("heal", da, NULL) == 0);
    CHECK_T(wait_state(dom_a, TSDB_DOM_UP, 4000));

    /* full: ENOSPC on the write probe sets `full` and nothing else */
    CHECK(cmd("enospc", db, NULL) == 0);
    nap(600);
    CHECK(tsdb_health_info(h, dom_b, &in) == 0);
    CHECK_T(in.full == 1);
    CHECK_T(in.state == TSDB_DOM_UP);
    CHECK(cmd("heal", db, NULL) == 0);
    nap(600);
    CHECK(tsdb_health_info(h, dom_b, &in) == 0);
    CHECK_T(in.full == 0);
    CHECK_T(in.state == TSDB_DOM_UP);

    /* slow: DEGRADED, nothing is refused (not DOWN); leaves after degraded_exit_ms under the threshold */
    CHECK(cmd("slow", da2, "400") == 0);
    CHECK_T(wait_state(dom_a2, TSDB_DOM_DEGRADED, 3000));
    CHECK_T(stays(dom_a2, TSDB_DOM_DEGRADED, 800));
    CHECK_T(tsdb_health_state(h, dom_a) == TSDB_DOM_UP);
    CHECK(cmd("heal", da2, NULL) == 0);
    CHECK_T(stays(dom_a2, TSDB_DOM_DEGRADED, 500)); /* hysteresis: not at once */
    CHECK_T(wait_state(dom_a2, TSDB_DOM_UP, 4500));

    /* the callback saw the transitions of the domains, and B's none. These are verdicts about the code and fail at
     * once; but they rest on the transitions the timed waits above saw, so after a timing failure (which is judged
     * by its own evidence, and a run that excuses it is run again) the log says nothing and is not judged. */
    pthread_mutex_lock(&lg_mu);
    if (th_ntiming == 0) {
        CHECK(strstr(lg, "0:0>2") && strstr(lg, "0:2>3") && strstr(lg, "0:3>0"));
        CHECK(strstr(lg, "1:0>1") && strstr(lg, "1:1>0"));
        /* B went DEGRADED or DOWN with no fault but ENOSPC: a probe of B was slow (or failed) for a reason outside the
         * code (the disk, the CPU) or the code is wrong. Which one is the evidence of this run around the change. */
        CHECK_TW(!strstr(lg, "2:"), b_change_ms - 3000, b_change_ms + 500);
    } else {
        fprintf(stderr, "the callback log is not judged after a timing failure (log: %s)\n", lg);
    }
    pthread_mutex_unlock(&lg_mu);

    /* a manual owner: with auto_up off, RECOVERING stays until tsdb_health_set_up */
    tsdb_health_close(h);
    h = NULL;
    o.auto_up = 0;
    o.on_change = NULL;
    h = tsdb_health_open(&o);
    int d0 = tsdb_health_add_domain(h, "b", db);
    CHECK(cmd("eio", db, NULL) == 0);
    CHECK_T(wait_state(d0, TSDB_DOM_DOWN, 2500));
    CHECK(cmd("heal", db, NULL) == 0);
    CHECK_T(wait_state(d0, TSDB_DOM_RECOVERING, 4000));
    CHECK_T(stays(d0, TSDB_DOM_RECOVERING, 1500));
    tsdb_health_set_up(h, d0);
    CHECK(tsdb_health_state(h, d0) == TSDB_DOM_UP);
    tsdb_health_close(h);
    h = NULL;

    /* read-only: no write probe, no files made */
    char ro[300];
    snprintf(ro, sizeof ro, "%s/ro", db);
    mkdir(ro, 0755);
    o.read_only = 1;
    h = tsdb_health_open(&o);
    int dr = tsdb_health_add_domain(h, "ro", ro);
    nap(600);
    CHECK(tsdb_health_state(h, dr) == TSDB_DOM_DOWN || tsdb_health_state(h, dr) == TSDB_DOM_UP);
    snprintf(p, sizeof p, "%s/PROBE.t1", ro);
    CHECK(access(p, F_OK) != 0);
    tsdb_health_close(h);
    h = NULL;

    int rc = th_finish("test_health");
    if (rc) {
        fprintf(stderr, "test_health: failed (log: %s)\n", lg);
        return rc;
    }
    printf("test_health: ok\n");
    return 0;
}
