/* SPDX-License-Identifier: AGPL-3.0-only */
/* The watchdog of tests/watchdog.h: (1) it starts no thread (a test that forks a child which starts threads dies under
 * ThreadSanitizer when the parent has a second thread: that broke `make tsan` for every such test), (2) a child that
 * outlives it exits with status 124 and says which test hung, (3) a child that finishes in time is left alone, (4) on a
 * machine busy for minutes (a fake pressure file) the deadline is renewed twice, on an idle one it is not. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>

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

int main(void)
{
    /* the parent does not have a watchdog of its own here: the children arm one each. The first child's message goes
     * to a pipe: it starts with "FAIL" (it is what a hung test prints), and a passing run must not print that. */
    setenv("TH_WATCHDOG_PSI", "/nonexistent", 1); /* an idle machine, whatever the real one is doing */
    int pf[2];
    CHECK(pipe(pf) == 0);
    pid_t pid = fork();
    if (pid == 0) {
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
    /* a busy machine (a fake pressure file) renews the deadline twice: the child ends after three deadlines, with the
     * note; an idle one (the same file with a low number) ends at the first */
    for (int busy = 0; busy < 2; busy++) {
        char psi[] = "/tmp/test_watchdog_psi.XXXXXX";
        int fd = mkstemp(psi);
        CHECK(fd >= 0);
        const char *txt = busy ? "some avg10=90.00 avg60=90.00 avg300=87.31 total=1\n"
                               : "some avg10=0.00 avg60=0.00 avg300=2.10 total=1\n";
        CHECK(write(fd, txt, strlen(txt)) == (ssize_t)strlen(txt));
        close(fd);
        int pe[2];
        CHECK(pipe(pe) == 0);
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        pid = fork();
        if (pid == 0) {
            close(pe[0]);
            dup2(pe[1], 2);
            setenv("TH_WATCHDOG_PSI", psi, 1);
            th_watchdog("test_watchdog pressure child", 1);
            for (;;)
                sleep(1);
        }
        close(pe[1]);
        CHECK(waitpid(pid, &st, 0) == pid);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double took = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
        double one = (double)TH_SAN_SCALE;
        got = read(pe[0], msg, sizeof msg - 1);
        msg[got > 0 ? got : 0] = 0;
        close(pe[0]);
        unlink(psi);
        CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 124);
        CHECK(strstr(msg, "pressure child: watchdog: still running after"));
        if (busy) {
            CHECK(took >= 2.9 * one); /* three deadlines: the first two renewed */
            CHECK(strstr(msg, "the machine is busy"));
        } else {
            CHECK(took < 2.0 * one);
            CHECK(!strstr(msg, "the machine is busy"));
        }
    }
    pid = fork();
    if (pid == 0) {
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
