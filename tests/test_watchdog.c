/* SPDX-License-Identifier: AGPL-3.0-only */
/* The watchdog of tests/watchdog.h: (1) it starts no thread (a test that forks a child which starts threads dies under
 * ThreadSanitizer when the parent has a second thread: that broke `make tsan` for every such test), (2) a child that
 * outlives it exits with status 124 and says which test hung, (3) a child that finishes in time is left alone. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

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
    /* the parent does not have a watchdog of its own here: the children arm one each */
    pid_t pid = fork();
    if (pid == 0) {
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
    CHECK(waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 124);
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
