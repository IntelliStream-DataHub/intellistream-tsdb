/* SPDX-License-Identifier: AGPL-3.0-only */
/* The watchdog of tests/watchdog.h: (1) it starts no thread (a test that forks a child which starts threads dies under
 * ThreadSanitizer when the parent has a second thread: that broke `make tsan` for every such test), (2) a child that
 * outlives it exits with status 124 and says which test hung, (3) a child that finishes in time is left alone, (4) the
 * deadline is fixed: it ends the child at its first expiry, never renewed (the machine's load is not asked), and
 * TH_WATCHDOG_SCALE multiplies it (the CI job's way to give a slow runner more time). */
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "watchdog.h"

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static int threads_now(void)
{
#ifdef __linux__
    int n = 0;
    char cmd[64];
    snprintf(cmd, sizeof cmd, "ls /proc/%d/task", (int)getpid());
    FILE *f = popen(cmd, "r");
    if (!f)
        return -1;
    char line[64];
    while (fgets(line, sizeof line, f))
        n++;
    pclose(f);
    return n;
#else
    return 1; /* no /proc: the thread fallback is known to add one */
#endif
}

/* This test's own bound: a watchdog that never ends its child (disarmed, or renewing for ever) would leave the children
 * below sleeping and this test waiting for them for ever, and a job with it. It is SIGALRM
 * (the watchdog under test uses SIGRTMIN) and a child dies with its parent. */
static void own_bound(int sig)
{
    (void)sig;
    static const char m[] = "\nFAIL test_watchdog: still running after its own bound, a child's watchdog never ended it "
                            "(exit 124)\n";
    if (write(2, m, sizeof m - 1) < 0)
        _exit(124);
    _exit(124);
}

static void die_with_parent(void)
{
#ifdef __linux__
    prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
}

int main(void)
{
    signal(SIGALRM, own_bound);
    alarm((unsigned)(60 * TH_SAN_SCALE));
    /* the parent does not have a watchdog of its own here: the children arm one each. The first child's message goes
     * to a pipe: it starts with "FAIL" (it is what a hung test prints), and a passing run must not print that. */
    int pf[2];
    CHECK(pipe(pf) == 0);
    pid_t pid = fork();
    if (pid == 0) {
        die_with_parent();
        close(pf[0]);
        dup2(pf[1], 2);
        int before = threads_now(); /* not 1 under TSan, which has a thread of its own */
        th_watchdog("test_watchdog child", 1);
#ifdef __linux__
        if (threads_now() != before) { /* a thread would break fork + threads under TSan */
            fprintf(stderr, "the watchdog started a thread (%d threads, %d before)\n", threads_now(), before);
            _exit(3);
        }
#endif
        for (;;)
            sleep(1); /* hung: the watchdog must end this */
    }
    int st = 0;
    close(pf[1]);
    CHECK(waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 124);
    char msg[512];
    ssize_t got = read(pf[0], msg, sizeof msg - 1);
    msg[got > 0 ? got : 0] = 0;
    close(pf[0]);
    if (!strstr(msg, "test_watchdog child: watchdog: still running after") || !strstr(msg, "it is hung (exit 124)")) {
        fprintf(stderr, "the watchdog's message names no test or no hang: \"%s\"\n", msg);
        failures++;
    }
    /* the deadline is fixed: a child with a 1 s deadline ends after 1 s (times the sanitizer's scale), though the machine
     * is as busy as can be (nothing of it is read), and TH_WATCHDOG_SCALE=2 makes it 2 s */
    for (int scale = 1; scale <= 2; scale++) {
        int pe[2];
        CHECK(pipe(pe) == 0);
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        pid = fork();
        if (pid == 0) {
            die_with_parent();
            close(pe[0]);
            dup2(pe[1], 2);
            if (scale == 2)
                setenv("TH_WATCHDOG_SCALE", "2", 1);
            th_watchdog("test_watchdog fixed child", 1);
            for (;;)
                sleep(1);
        }
        close(pe[1]);
        CHECK(waitpid(pid, &st, 0) == pid);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double took = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
        double one = (double)TH_SAN_SCALE * scale;
        got = read(pe[0], msg, sizeof msg - 1);
        msg[got > 0 ? got : 0] = 0;
        close(pe[0]);
        CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 124);
        CHECK(strstr(msg, "fixed child: watchdog: still running after"));
        CHECK(took >= 0.95 * one); /* not before its deadline */
        CHECK(took < 1.9 * one);   /* and not renewed: a second deadline would take twice as long */
        CHECK(!strstr(msg, "busy") && !strstr(msg, "renewal"));
    }
    pid = fork();
    if (pid == 0) {
        die_with_parent();
        th_watchdog("test_watchdog quick child", 5);
        _exit(0); /* finishes in time */
    }
    CHECK(waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    if (failures) {
        fprintf(stderr, "test_watchdog: %d failures\n", failures);
        return 1;
    }
    printf("test_watchdog: ok\n");
    return 0;
}
