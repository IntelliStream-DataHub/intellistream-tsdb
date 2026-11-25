/* SPDX-License-Identifier: AGPL-3.0-only */
/* Storage domain health (src/health.c, ha.md 15.2): probes, the states and what moves between them, against the
 * outagefs shim. Three domains, A and A2 (two filesystems of one server) and B (another server). The thresholds are
 * scaled down so the whole test takes about 30 s. Usage: test_health [scratch directory]. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "outagefs_ctl.h"
#include "../src/health_rules.h"

#include <dirent.h>
#include "watchdog.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>

/* Every check is a verdict and a failure is a failure: nothing is run again, and no failure is excused by how busy the
 * machine was. What the test does not fail on is a moment tsdb does not promise: a state change is promised, not its
 * time (down_ms and the other thresholds bound how EARLY it may come, which the "stays" checks hold), so a wait for a
 * state gives the promised time plus LATE_MS, and a count that only follows the probes' cadence is waited for. A machine
 * too busy for probes of a few hundred ms to run on time sends a domain DEGRADED or DOWN that no fault touched, and the
 * test then fails: that is investigated (and CI runs this group with fewer jobs at once), not run again. */
static int failures;
#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond);                                        \
            failures++;                                                                                                \
            diag();                                                                                                    \
        }                                                                                                              \
    } while (0)
#define LATE_MS 20000

static char ctl[256];
static tsdb_health *h;
static int dom_a, dom_a2, dom_b;

static atomic_int changes;
static pthread_mutex_t lg_mu = PTHREAD_MUTEX_INITIALIZER;
static char lg[8192];

static void on_change(void *ctx, int dom, int old, int nw)
{
    (void)ctx;
    atomic_fetch_add(&changes, 1);
    pthread_mutex_lock(&lg_mu);
    size_t l = strlen(lg);
    snprintf(lg + l, sizeof lg - l, "%d:%d>%d ", dom, old, nw);
    pthread_mutex_unlock(&lg_mu);
}

/* With a failure: what each domain's last probes took, to tell a slow disk from a wrong state. */
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

static void nap(int ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

/* The CPU time (user + system, ms) of every thread of this process except the caller's, summed: the probe threads of
 * the health object (the test's own thread sleeps). A probe loop that spins instead of waiting shows here. */
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
        if (tid <= 0 || tid == me)
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

/* waits for a domain to be in a state: the promised time (ms) plus LATE_MS (see CHECK) */
static bool wait_state(int dom, int st, int ms)
{
    ms += LATE_MS;
    for (int t = 0; t < ms; t += 20) {
        if (tsdb_health_state(h, dom) == st)
            return true;
        nap(20);
    }
    return tsdb_health_state(h, dom) == st;
}

/* waits for the `full` flag of a domain to be `v` (the write probe sets it: its cadence, not a promised time) */
static void wait_full(int dom, int v)
{
    tsdb_domain_info in;
    for (int t = 0; t < LATE_MS; t += 20) {
        if (tsdb_health_info(h, dom, &in) == 0 && in.full == v)
            return;
        nap(20);
    }
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

/* The thresholds here are scaled down to hundreds of ms (a probe that waits a few hundred ms for a CPU is a slow probe, and a
 * domain with no fault goes DEGRADED): the test wants a machine that is not starved, and says so when it fails. */
int main(int argc, char **argv)
{
    th_watchdog("test_health", 1200);
    const char *base = argc > 1 ? argv[1] : "/tmp";
    const char *const names[] = {"a", "a2", "b"};
    if (outagefs_reexec(argv, base, names, 3))
        return 1;
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
        CHECK(in.state == TSDB_DOM_UP);
        for (int t = 0; t < LATE_MS && in.probes_ok < 5; t += 20) { /* five probes: the cadence's count, not a promised time */
            nap(20);
            tsdb_health_info(h, d, &in);
        }
        CHECK(in.probes_ok >= 5);
        CHECK(in.probes_err == 0); /* a probe that failed with no fault is a bug, slow or not */
        CHECK(in.threads == 1);
    }
    char p[512];
    snprintf(p, sizeof p, "%s/PROBE", da);
    CHECK(access(p, F_OK) == 0);
    snprintf(p, sizeof p, "%s/PROBE.t1", da);
    CHECK(access(p, F_OK) == 0);
    CHECK(atomic_load(&changes) == 0);
    /* idle: the probe threads wait between probes. A deadline in the past turns the wait into a busy loop (a thread of
     * the health object burning a CPU: the test's own thread sleeps). CPU time, not wall time: load does not move it. */
    nap(800);
    long cpu1 = threads_cpu_ms();
    fprintf(stderr, "probe threads used %ld ms of CPU in 2 s idle\n", cpu1 - cpu0);
    CHECK(cpu0 >= 0 && cpu1 >= cpu0 && cpu1 - cpu0 < 400);

    /* hang: only A goes DOWN, after about down_ms; its sibling and the other server stay UP */
    CHECK(cmd("hang", da, NULL) == 0);
    CHECK(stays(dom_a, TSDB_DOM_UP, 1000)); /* not yet: a probe has been out for 1 s of the 1.5 */
    CHECK(wait_state(dom_a, TSDB_DOM_DOWN, 2000)); /* DOWN once a probe has been out for down_ms: promised, late or not */
    CHECK(tsdb_health_state(h, dom_a2) == TSDB_DOM_UP && tsdb_health_state(h, dom_b) == TSDB_DOM_UP);
    tsdb_domain_info in;
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK(in.outage_id == 1);
    CHECK(in.stuck_ms > 1000);
    /* stuck past spawn_ms: a second probe thread */
    nap(1500);
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK(in.threads == 2);
    CHECK(tsdb_health_state(h, dom_a) == TSDB_DOM_DOWN);
    /* heal: the stuck threads return, write probes work for recover_ms, then UP (auto_up) */
    CHECK(cmd("heal", da, NULL) == 0);
    for (int t = 0; t < LATE_MS + 2500 && tsdb_health_state(h, dom_a) != TSDB_DOM_RECOVERING &&
                    tsdb_health_state(h, dom_a) != TSDB_DOM_UP; t += 20)
        nap(20);
    CHECK(tsdb_health_state(h, dom_a) == TSDB_DOM_RECOVERING || tsdb_health_state(h, dom_a) == TSDB_DOM_UP);
    CHECK(wait_state(dom_a, TSDB_DOM_UP, 2000));
    nap(300);
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK(in.threads == 1);
    CHECK(in.age_ms < 5000 + LATE_MS);
    CHECK(in.outage_id == 1);

    /* a read-only heal is not enough: with writes failing, the domain does not leave DOWN (reads would pass) */
    CHECK(cmd("eio", da, NULL) == 0);
    CHECK(wait_state(dom_a, TSDB_DOM_DOWN, 2500)); /* three failed probes */
    CHECK(tsdb_health_info(h, dom_a, &in) == 0);
    CHECK(in.outage_id == 2);
    CHECK(in.probes_err >= 3);
    CHECK(cmd("heal", da, NULL) == 0);
    CHECK(wait_state(dom_a, TSDB_DOM_UP, 4000));

    /* full: ENOSPC on the write probe sets `full` and nothing else */
    CHECK(cmd("enospc", db, NULL) == 0);
    wait_full(dom_b, 1);
    CHECK(tsdb_health_info(h, dom_b, &in) == 0);
    CHECK(in.full == 1);
    CHECK(in.state == TSDB_DOM_UP);
    CHECK(cmd("heal", db, NULL) == 0);
    wait_full(dom_b, 0);
    CHECK(tsdb_health_info(h, dom_b, &in) == 0);
    CHECK(in.full == 0);
    CHECK(in.state == TSDB_DOM_UP);

    /* slow: DEGRADED, nothing is refused (not DOWN); leaves after degraded_exit_ms under the threshold */
    CHECK(cmd("slow", da2, "400") == 0);
    CHECK(wait_state(dom_a2, TSDB_DOM_DEGRADED, 3000));
    CHECK(stays(dom_a2, TSDB_DOM_DEGRADED, 800));
    CHECK(tsdb_health_state(h, dom_a) == TSDB_DOM_UP);
    CHECK(cmd("heal", da2, NULL) == 0);
    CHECK(stays(dom_a2, TSDB_DOM_DEGRADED, 500)); /* hysteresis: not at once */
    CHECK(wait_state(dom_a2, TSDB_DOM_UP, 4500));

    /* the callback saw the transitions of the domains, and B's none: B went DEGRADED or DOWN with no fault but ENOSPC, which
     * is a probe of it that was slow (a starved machine) or the code is wrong; either way the run fails and says which
     * probes were slow (diag). */
    pthread_mutex_lock(&lg_mu);
    CHECK(strstr(lg, "0:0>2") && strstr(lg, "0:2>3") && strstr(lg, "0:3>0"));
    CHECK(strstr(lg, "1:0>1") && strstr(lg, "1:1>0"));
    CHECK(!strstr(lg, "2:"));
    pthread_mutex_unlock(&lg_mu);

    /* a manual owner: with auto_up off, RECOVERING stays until tsdb_health_set_up */
    tsdb_health_close(h);
    h = NULL;
    o.auto_up = 0;
    o.on_change = NULL;
    h = tsdb_health_open(&o);
    int d0 = tsdb_health_add_domain(h, "b", db);
    CHECK(cmd("eio", db, NULL) == 0);
    CHECK(wait_state(d0, TSDB_DOM_DOWN, 2500));
    CHECK(cmd("heal", db, NULL) == 0);
    CHECK(wait_state(d0, TSDB_DOM_RECOVERING, 4000));
    CHECK(stays(d0, TSDB_DOM_RECOVERING, 1500));
    tsdb_health_set_up(h, d0);
    CHECK(tsdb_health_state(h, d0) == TSDB_DOM_UP);
    tsdb_health_close(h);
    h = NULL;

    /* flapping (health_rules.h): the third DOWN in the window needs HEALTH_FLAP_NEED_MS of good probes, far more than
     * recover_ms, and a slow probe in the meantime starts that count over: it says so once (stderr), because a
     * flapping domain on a slow disk stays DOWN with every probe working and nothing else tells why. */
    o.auto_up = 1;
    h = tsdb_health_open(&o);
    int df = tsdb_health_add_domain(h, "flap", db);
    CHECK(HEALTH_FLAP_DOWNS == 3 && HEALTH_FLAP_NEED_MS > 10 * o.recover_ms);
    for (int i = 0; i < HEALTH_FLAP_DOWNS; i++) {
        CHECK(cmd("eio", db, NULL) == 0);
        CHECK(wait_state(df, TSDB_DOM_DOWN, 2500));
        CHECK(cmd("heal", db, NULL) == 0);
        if (i + 1 < HEALTH_FLAP_DOWNS)
            CHECK(wait_state(df, TSDB_DOM_UP, 4500)); /* a first and a second DOWN leave after recover_ms */
    }
    CHECK(stays(df, TSDB_DOM_DOWN, 3 * o.recover_ms)); /* the third does not */
    char errlog[512];
    snprintf(errlog, sizeof errlog, "%s/health-stderr.%d", base, (int)getpid());
    int saved = dup(2), efd = open(errlog, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(saved >= 0 && efd >= 0);
    fflush(stderr);
    dup2(efd, 2);
    close(efd);
    CHECK(cmd("slow", db, "700") == 0); /* every probe works, none within fast_ms (500) */
    /* the line comes when the first slow probe ends: waited for (a condition, not a promised time: how long a
     * probe takes on a loaded or instrumented machine is not tsdb's promise), then a few more slow probes must not
     * add another */
    char txt[4096] = {0};
    int lines = 0;
    for (int t = 0; t < LATE_MS && lines == 0; t += 50) {
        nap(50);
        FILE *f = fopen(errlog, "r");
        size_t n = f ? fread(txt, 1, sizeof txt - 1, f) : 0;
        txt[n] = 0;
        if (f)
            fclose(f);
        for (const char *q = txt; (q = strstr(q, "start over")) != NULL; q++)
            lines++;
    }
    nap(3000);
    CHECK(tsdb_health_state(h, df) == TSDB_DOM_DOWN);
    CHECK(cmd("heal", db, NULL) == 0);
    fflush(stderr);
    dup2(saved, 2);
    close(saved);
    {
        FILE *f = fopen(errlog, "r");
        size_t n = f ? fread(txt, 1, sizeof txt - 1, f) : 0;
        txt[n] = 0;
        if (f)
            fclose(f);
        lines = 0;
        for (const char *q = txt; (q = strstr(q, "start over")) != NULL; q++)
            lines++;
        CHECK(lines == 1); /* the first slow probe of this DOWN, not one per probe */
        CHECK(strstr(txt, "storage domain flap: a write probe took") != NULL);
        char want[64];
        snprintf(want, sizeof want, "%d ms of good probes", HEALTH_FLAP_NEED_MS);
        CHECK(strstr(txt, want) != NULL);
        unlink(errlog);
    }
    tsdb_health_close(h);

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

    /* cold-start spike: a slow first write probe (such as cold O_CREAT + fdatasync on XFS, below degraded_min_ms so the
     * domain stays UP) must not poison the baseline and keep a later genuinely degraded period from entering DEGRADED */
    o.read_only = 0;
    CHECK(cmd("slow", da2, "70") == 0);
    h = tsdb_health_open(&o);
    int dc = tsdb_health_add_domain(h, "cold", da2);
    for (int t = 0; t < LATE_MS; t += 20) {
        if (tsdb_health_info(h, dc, &in) == 0 && in.probes_ok >= 1 && in.write_ms >= 150)
            break;
        nap(20);
    }
    CHECK(cmd("heal", da2, NULL) == 0);
    for (int t = 0; t < LATE_MS; t += 20) {
        if (tsdb_health_info(h, dc, &in) == 0 && in.probes_ok >= 15)
            break;
        nap(20);
    }
    CHECK(cmd("slow", da2, "400") == 0);
    CHECK(wait_state(dc, TSDB_DOM_DEGRADED, 3000));
    CHECK(cmd("heal", da2, NULL) == 0);
    tsdb_health_close(h);
    h = NULL;

    if (failures) {
        fprintf(stderr, "test_health: %d failures (log: %s)\n", failures, lg);
        return 1;
    }
    printf("test_health: ok\n");
    return 0;
}
