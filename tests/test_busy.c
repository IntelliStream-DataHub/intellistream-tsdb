/* SPDX-License-Identifier: AGPL-3.0-only */
/* tests/busy.h: a test that failed on a busy machine runs again (up to 3 runs); one that failed on an idle machine, or
 * that fails on every run, fails. Each scenario runs this program again as the test under busy.h's wrapper, with fake
 * pressure files (TH_PRESSURE_DIR) and a file that counts its runs. */
#define _GNU_SOURCE
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
    const char *scen = argv[2], *file = argv[3];
    int n = count_runs(file) + 1;
    FILE *f = fopen(file, "w");
    if (!f)
        return 99;
    fprintf(f, "%d\n", n);
    fclose(f);
    if (!strcmp(scen, "flaky") && n >= 2)
        return 0; /* passes on its second run */
    return th_failed(-1);
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

static int scenario(char *prog, const char *dir, const char *name, int pct, int *runs)
{
    char count[400];
    snprintf(count, sizeof count, "%s/count.%s", dir, name);
    unlink(count);
    fake(dir, pct);
    pid_t pid = fork();
    if (pid == 0) {
        setenv("TH_PRESSURE_DIR", dir, 1);
        unsetenv("TH_RETRY_INNER");
        unsetenv("OUTAGEFS_CTL");
        if (!freopen("/dev/null", "w", stderr))
            _exit(126);
        execl(prog, prog, "--scenario", name, count, (char *)NULL);
        _exit(127);
    }
    int st = 0;
    waitpid(pid, &st, 0);
    *runs = count_runs(count);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

int main(int argc, char **argv)
{
    if (argc == 4 && !strcmp(argv[1], "--scenario"))
        return th_run_retrying("test_busy scenario", argc, argv, the_test);
    char dir[] = "/tmp/test_busy.XXXXXX";
    if (!mkdtemp(dir))
        return 1;
    char prog[512];
    ssize_t n = readlink("/proc/self/exe", prog, sizeof prog - 1);
    if (n <= 0)
        return 1;
    prog[n] = 0;
    int runs, rc;
    rc = scenario(prog, dir, "flaky", 90, &runs); /* fails once on a busy machine, then passes: 2 runs, pass */
    CHECK(rc == 0 && runs == 2);
    rc = scenario(prog, dir, "bug", 90, &runs); /* fails every run on a busy machine: 3 runs, fail */
    CHECK(rc == 1 && runs == 3);
    rc = scenario(prog, dir, "flaky", 0, &runs); /* the same failure on an idle machine: no second run */
    CHECK(rc == 1 && runs == 1);
    char cmd[600];
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
