/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef TSDB_TEST_BUSY_H
#define TSDB_TEST_BUSY_H
/* A test whose thresholds are timings (the storage health probes of test_health, test_outage and test_unavailable:
 * hundreds of ms scaled down from seconds, against a real disk) says nothing when the machine is busy: a probe thread
 * that waits for a CPU, or an fsync that waits for the disk, for that long is a slow probe, and a domain that no fault
 * touched goes DEGRADED or DOWN (make test and make asan of the test-trust gates failed that way at test_health:183 and
 * :264 and test_outage:660, beside other gates). Such a test runs as an outer process that forks the real run: a failed
 * run that saw a busy machine (the run's own wake-up delay, the machine's CPU or IO pressure) exits TH_EXIT_BUSY and is
 * run again, up to three times, with a line that says so. A failure with no load in it, or one every run repeats (a
 * bug), fails. Usage: `return th_run_retrying("test_x", argc, argv, run_test);` as main, and `return
 * th_failed(wake_delay_ms)` where the run ends with failures (wake_delay_ms: -1 when it did not measure one). */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "watchdog.h"

#define TH_EXIT_BUSY 3
#define TH_BUSY_WAKE_MS 30 /* a thread woken this late says the CPUs were busy */
#define TH_BUSY_PCT 20     /* "some" pressure of /proc/pressure/{cpu,io} over 10 or 60 s, whole percent */

/* The "some" pressure of `res` ("cpu" or "io") for `key` ("avg10=" or "avg60="): whole percent, 0 where there is none. */
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

/* A failed run's exit status: TH_EXIT_BUSY when the machine was busy (and the numbers, to stderr), else 1. */
static inline int th_failed(int wake_delay_ms)
{
    int cpu10 = th_pressure("cpu", "avg10="), io10 = th_pressure("io", "avg10="), io60 = th_pressure("io", "avg60=");
    int busy = wake_delay_ms >= TH_BUSY_WAKE_MS || cpu10 >= TH_BUSY_PCT || io10 >= TH_BUSY_PCT || io60 >= TH_BUSY_PCT;
    fprintf(stderr, "  largest wake-up delay %d ms, CPU pressure %d%%, IO pressure %d%% (10 s) %d%% (60 s)%s\n",
            wake_delay_ms, cpu10, io10, io60, busy ? ": the machine was busy" : "");
    return busy ? TH_EXIT_BUSY : 1;
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
    for (int attempt = 1; attempt <= 3; attempt++) {
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
        fprintf(stderr, "%s: attempt %d failed with the machine busy: running it again%s\n", name, attempt,
                attempt < 3 ? "" : " (no more: the failure stands)");
    }
    return rc == TH_EXIT_BUSY ? 1 : rc;
}
#endif
