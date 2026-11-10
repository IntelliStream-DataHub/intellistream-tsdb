/* SPDX-License-Identifier: AGPL-3.0-only */
/* tests/busy.h: what may excuse a failed run and what may not. Each scenario runs this program again as a test under
 * busy.h's wrapper (with fake pressure files, TH_PRESSURE_DIR, and a file that counts its runs); the rules:
 *  - a plain check (CHECK) that fails fails the run at once, whatever the machine looks like (no second run);
 *  - a timing check (CHECK_T) is excused only by evidence from the run's own gauge (a thread woken 30 ms late, a thread
 *    that waited 30 ms for a CPU in a slot, a canary fdatasync of 100 ms) inside the window of the failing check; 29 ms
 *    is not enough, nor is the pressure of the whole machine at 90%, nor evidence from before the window;
 *  - then the run is run again (four runs in all), and a failure every run repeats stands;
 *  - a check that reads what a timed step was to bring about (CHECK_D) follows the timing failure before it, and alone is
 *    plain;
 *  - the gauge thread itself sees CPU contention (four threads on one CPU).
 * Usage: test_busy [scratch directory]. */
#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "busy.h"

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

enum { K_T = 1, K_P = 2, K_D = 4 }; /* which kinds of check the scenario fails */
struct scen {
    const char *name;
    int pct;               /* the fake machine pressure, whole percent */
    int wake, runq, fsync; /* what the run's own gauge saw (ms), 0 = nothing */
    int kinds;             /* the checks that fail */
    int flaky;             /* the failure is only in the first run */
    int stale;             /* the evidence is noted, then the window moves past it */
    int rc, runs;          /* expected: exit status of the outer process, number of runs */
};

static const struct scen scens[] = {
    {"pass", 90, 50, 0, 0, 0, 0, 0, 0, 1},
    /* a timing failure with evidence of this run: run again */
    {"t-wake-30-flaky", 0, 30, 0, 0, K_T, 1, 0, 0, 2},
    {"t-wake-30-bug", 0, 30, 0, 0, K_T, 0, 0, 1, 4},
    {"t-runq-30-flaky", 0, 0, 30, 0, K_T, 1, 0, 0, 2},
    {"t-fsync-100-flaky", 0, 0, 0, 100, K_T, 1, 0, 0, 2},
    /* just under the thresholds: no excuse */
    {"t-wake-29", 0, 29, 0, 0, K_T, 0, 0, 1, 1},
    {"t-runq-29", 0, 0, 29, 0, K_T, 0, 0, 1, 1},
    {"t-fsync-99", 0, 0, 0, 99, K_T, 0, 0, 1, 1},
    /* the machine's pressure is no evidence (the dev box sits at 20% with nothing of the test in it) */
    {"t-psi-only", 90, 0, 0, 0, K_T, 1, 0, 1, 1},
    {"t-psi-only-29", 90, 29, 0, 0, K_T, 1, 0, 1, 1},
    /* evidence from before the failing window */
    {"t-stale-evidence", 0, 50, 0, 0, K_T, 1, 1, 1, 1},
    /* a plain failure on a busy machine fails at once, with or without a timing failure beside it */
    {"p-busy", 90, 50, 0, 0, K_P, 1, 0, 1, 1},
    {"p-busy-gauge", 90, 50, 50, 500, K_P, 0, 0, 1, 1},
    {"p-and-t", 90, 50, 0, 0, K_P | K_T, 1, 0, 1, 1},
    {"p-idle", 0, 0, 0, 0, K_P, 1, 0, 1, 1},
    /* a dependent check: with the timing failure before it, judged like it; alone, a plain failure */
    {"d-after-t", 0, 50, 0, 0, K_T | K_D, 1, 0, 0, 2},
    {"d-after-t-noevidence", 0, 0, 0, 0, K_T | K_D, 1, 0, 1, 1},
    {"d-alone", 0, 50, 0, 0, K_D, 1, 0, 1, 1},
};
#define NSCEN (int)(sizeof scens / sizeof scens[0])

static int count_runs(const char *file)
{
    int n = 0;
    FILE *f = fopen(file, "r");
    if (f) {
        if (fscanf(f, "%d", &n) != 1)
            n = 0;
        fclose(f);
    }
    return n;
}

/* the test under the wrapper: argv = prog --scenario NAME COUNTFILE */
static int the_test(int argc, char **argv)
{
    (void)argc;
    const char *name = argv[2], *file = argv[3];
    const struct scen *sc = NULL;
    for (int i = 0; i < NSCEN; i++)
        if (!strcmp(scens[i].name, name))
            sc = &scens[i];
    if (!sc)
        return 98;
    int n = count_runs(file) + 1;
    FILE *f = fopen(file, "w");
    if (!f)
        return 99;
    fprintf(f, "%d\n", n);
    fclose(f);
    if (sc->wake)
        th_note(TH_EV_WAKE, sc->wake);
    if (sc->runq)
        th_note(TH_EV_RUNQ, sc->runq);
    if (sc->fsync)
        th_note(TH_EV_FSYNC, sc->fsync);
    if (sc->stale) {
        usleep(1300 * 1000); /* the window of the next check starts at the mark: after the evidence */
        th_mark();
    }
    if (!(sc->flaky && n >= 2)) {
        if (sc->kinds & K_T)
            TH_CHECK_T(n < 0);
        if (sc->kinds & K_D)
            TH_CHECK_D(n < 0);
        if (sc->kinds & K_P)
            TH_CHECK(n < 0);
    }
    return th_finish("test_busy scenario");
}

static void fake(const char *dir, int pct)
{
    const char *res[] = {"cpu", "io"};
    for (int i = 0; i < 2; i++) {
        char p[400];
        snprintf(p, sizeof p, "%s/%s", dir, res[i]);
        FILE *f = fopen(p, "w");
        if (!f) {
            perror(p);
            exit(1);
        }
        fprintf(f, "some avg10=%d.00 avg60=%d.00 avg300=%d.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n", pct,
                pct, pct);
        fclose(f);
    }
}

static int scenario(char *prog, const char *dir, const struct scen *sc, int *runs)
{
    char count[400];
    snprintf(count, sizeof count, "%s/count.%s", dir, sc->name);
    unlink(count);
    fake(dir, sc->pct);
    pid_t pid = fork();
    if (pid == 0) {
        setenv("TH_PRESSURE_DIR", dir, 1);
        setenv("TH_RETRY_PAUSE_S", "0", 1);
        unsetenv("TH_RETRY_INNER");
        unsetenv("OUTAGEFS_CTL");
        if (!freopen("/dev/null", "w", stderr))
            _exit(126);
        execl(prog, prog, "--scenario", sc->name, count, (char *)NULL);
        _exit(127);
    }
    int st = 0;
    waitpid(pid, &st, 0);
    *runs = count_runs(count);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

/* four threads on one CPU: each waits for it about three quarters of the time */
static volatile int hog_stop;
static void *hog(void *arg)
{
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET((int)(intptr_t)arg, &cs);
    sched_setaffinity(0, sizeof cs, &cs);
    while (!hog_stop)
        ;
    return NULL;
}

static void gauge_sees_contention(const char *dir)
{
    cpu_set_t all;
    if (sched_getaffinity(0, sizeof all, &all) != 0 || CPU_COUNT(&all) < 1) {
        fprintf(stderr, "test_busy: no CPU affinity here: the gauge's contention test is skipped\n");
        return;
    }
    int cpu = 0;
    while (!CPU_ISSET(cpu, &all))
        cpu++;
    cpu_set_t one;
    CPU_ZERO(&one);
    CPU_SET(cpu, &one);
    sched_setaffinity(0, sizeof one, &one); /* the gauge thread inherits it: it too sits behind the hogs */
    th_gauge_start(dir);
    long t0 = th_now_ms();
    pthread_t hs[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&hs[i], NULL, hog, (void *)(intptr_t)cpu);
    usleep(1500 * 1000);
    hog_stop = 1;
    for (int i = 0; i < 4; i++)
        pthread_join(hs[i], NULL);
    int wake, runq, fsync;
    int ex = th_evidence(t0, th_now_ms(), &wake, &runq, &fsync);
    fprintf(stderr, "gauge under four threads on one CPU: wake-up %d ms, run queue %d ms, canary fdatasync %d ms\n", wake, runq,
            fsync);
    CHECK(ex && (runq >= TH_BUSY_WAKE_MS || wake >= TH_BUSY_WAKE_MS)); /* the other threads' waits count */
    sched_setaffinity(0, sizeof all, &all);
}

int main(int argc, char **argv)
{
    if (argc == 4 && !strcmp(argv[1], "--scenario"))
        return th_run_retrying("test_busy scenario", argc, argv, the_test);
    th_watchdog("test_busy", 600);
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char dir[300];
    snprintf(dir, sizeof dir, "%s/test_busy.XXXXXX", base);
    if (!mkdtemp(dir)) {
        perror(dir);
        return 1;
    }
    char prog[512];
    ssize_t n = readlink("/proc/self/exe", prog, sizeof prog - 1);
    if (n <= 0)
        return 1;
    prog[n] = 0;
    for (int i = 0; i < NSCEN; i++) {
        int runs, rc = scenario(prog, dir, &scens[i], &runs);
        if (rc != scens[i].rc || runs != scens[i].runs) {
            fprintf(stderr, "scenario %s: exit %d after %d runs, expected %d after %d\n", scens[i].name, rc, runs,
                    scens[i].rc, scens[i].runs);
            failures++;
        }
    }
    gauge_sees_contention(dir);
    char cmd[400];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        failures++;
    if (failures) {
        fprintf(stderr, "test_busy: %d failures\n", failures);
        return 1;
    }
    printf("test_busy: ok\n");
    return 0;
}
