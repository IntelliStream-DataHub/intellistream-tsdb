/* SPDX-License-Identifier: AGPL-3.0-only */
/* A tsdbd with a volume on a storage domain under the outagefs shim, for the tests of the Rust, Python and Java
 * clients (the C client's own test is test_unavailable.c): it makes a database (the root and volume A, the series
 * pump.a in A and pump.root in the root), starts tsdbd on it, prints one line
 *
 *   READY port sid_a sid_root vol_a
 *
 * and then answers one command per stdin line with "ok": `down` (A's storage hangs; replies once the server has
 * declared the volume DOWN, about 4.5 s), `heal` (A's storage answers again; the volume recovers by itself in
 * about two seconds), `quit`. End of stdin quits too: the server is stopped (by its pid) and its status checked
 * (the last line printed is "exit N").
 *
 * Usage: outage_server [scratch directory]  (re-executes itself under build/liboutagefs.so; OUTAGEFS_LIB, TSDBD,
 * TSDBD_IO as in test_unavailable) */
#define _GNU_SOURCE
#include "tsdb.h"
#include <limits.h>
#include "harness.h"
#include "outagefs_ctl.h"

#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>

static char ctl[256], root[512];

static void msleep(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static int fault(const char *cmd, const char *dir)
{
    char line[600], out[256];
    snprintf(line, sizeof line, "%s %s", cmd, dir);
    int rc = outagefs_cmd(ctl, line, out, sizeof out);
    if (rc)
        fprintf(stderr, "ctl '%s': %s\n", line, out);
    return rc;
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    const char *names[] = {"a"};
    if (outagefs_reexec(argv, base, names, 1) != 0)
        return 1;
    const char *dir_a = outagefs_dir("a");
    snprintf(ctl, sizeof ctl, "%s", getenv("OUTAGEFS_CTL"));
    snprintf(root, sizeof root, "%s.root", getenv("OUTAGEFS_CTL"));
    signal(SIGPIPE, SIG_IGN);

    tsdb *db;
    tsdb_options o;
    tsdb_options_init(&o);
    o.create_volumes = 1;
    tsdb_sid sid_a, sid_root;
    uint32_t vi;
    if (tsdb_open(root, &o, &db) != 0 || tsdb_volume_add(db, dir_a, &vi) != 0 ||
        tsdb_series_at(db, "pump.a", dir_a, &sid_a) != 0 || tsdb_series(db, "pump.root", &sid_root) != 0 ||
        tsdb_close(db) != 0) {
        fprintf(stderr, "outage_server: cannot make the database\n");
        return 1;
    }

    char pf[PATH_MAX + 64], follow[64], da[PATH_MAX + 32];
    th_portfile(pf, sizeof pf, root);
    th_follow_me(follow, sizeof follow);
    snprintf(da, sizeof da, "%s", dir_a);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    setenv("TSDBD_TEST_HEALTH", "down=2500,read=200,write=500,recover=1500,fast=400,spawn=60000", 1);
    pid_t pid = fork();
    if (pid == 0) {
        const char *av[32] = {"tsdbd", "--dir", root, "--port", "0", "--port-file", pf, "--workers", "8", "--no-auth",
                              follow, "--health-domain", da};
        int n = 13;
        if (getenv("TSDBD_IO")) {
            av[n++] = "--io";
            av[n++] = getenv("TSDBD_IO");
        }
        av[n] = NULL;
        execv(bin, (char *const *)av);
        _exit(127);
    }
    int port = th_await(pf, pid, 30000);
    unlink(pf);
    if (port <= 0) {
        fputs(th_why, stderr);
        return 1;
    }
    printf("READY %d %llu %llu %u\n", port, (unsigned long long)sid_a, (unsigned long long)sid_root,
           (unsigned)TSDB_SID_VOLUME(sid_a));
    fflush(stdout);

    char line[128];
    while (fgets(line, sizeof line, stdin)) {
        int rc = 0;
        if (!strncmp(line, "down", 4)) {
            rc = fault("hang", dir_a);
            msleep(4500);
        } else if (!strncmp(line, "heal", 4)) {
            rc = fault("heal", dir_a);
        } else if (!strncmp(line, "quit", 4)) {
            break;
        } else {
            rc = -1;
        }
        puts(rc ? "err" : "ok");
        fflush(stdout);
    }
    int st = 1;
    if (pid > 1) {
        kill(pid, SIGTERM);
        waitpid(pid, &st, 0);
    }
    printf("exit %d\n", WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st));
    return WIFEXITED(st) && WEXITSTATUS(st) == 0 ? 0 : 1;
}
