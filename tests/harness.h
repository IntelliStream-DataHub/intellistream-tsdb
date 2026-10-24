/* Shared by the C test harnesses that start tsdbd (test_server, test_stability, jepsen, test_ports).
 *
 * A harness never computes a port. It starts tsdbd with `--port 0 --port-file FILE` and reads the port from that
 * file, which the server writes (as "PORT PID") once its listener is up. th_await() also makes sure the answer
 * comes from the child it forked: the file names that child's pid, the child is alive, and the listening socket of
 * that port belongs to it (read from /proc). A server that failed to start, or a port someone else holds, is an
 * error the harness reports (th_why) and stops on: it must never go on to talk to a server it did not start. */
#ifndef TSDB_TEST_HARNESS_H
#define TSDB_TEST_HARNESS_H
/* (a caller defines _GNU_SOURCE first, as the C sources here do) */
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/* A fresh path for a port file: BASE.port.PID.N, with the file removed so that a stale one cannot be read. */
static inline void th_portfile(char *buf, size_t n, const char *base)
{
    static int seq;
    /* beside the directory, not inside its name prefix: the crashfs shim tracks everything that starts with
     * CRASHFS_ROOT and must not spend its faults on a port file */
    const char *slash = strrchr(base, '/');
    snprintf(buf, n, "%.*sport.%s.%d.%d", slash ? (int)(slash - base + 1) : 0, base, slash ? slash + 1 : base,
             (int)getpid(), seq++);
    unlink(buf);
}

/* Listening sockets already proven to be on a port: (port << 48 | inode). Listing /proc/net/tcp costs milliseconds
 * once it holds the TIME_WAIT sockets of a few thousand reconnects, and jepsen's clients ask on every connect. A
 * socket inode names one socket for as long as someone holds it, so "pid has inode I open" proves "pid listens on
 * the port I was found on" with a scan of the pid's few descriptors only; a dead server holds nothing and falls
 * through to the full scan, which then finds no listener. */
#define TH_CACHE 16
static _Atomic unsigned long long th_cache[TH_CACHE];
static _Atomic unsigned th_cache_next;
static _Atomic unsigned th_scans; /* full /proc/net/tcp listings so far (the test of the cache reads it) */

static inline void th_cache_add(int port, unsigned long ino)
{
    unsigned long long v = ((unsigned long long)port << 48) | (ino & 0xffffffffffffULL);
    for (int i = 0; i < TH_CACHE; i++)
        if (atomic_load(&th_cache[i]) == v)
            return;
    atomic_store(&th_cache[atomic_fetch_add(&th_cache_next, 1) % TH_CACHE], v);
}

/* Does `pid` hold a socket with one of these inodes (`want`, n of them)? Returns the inode, or 0. */
static inline unsigned long th_fd_socket(pid_t pid, const unsigned long *want, int n)
{
    char dirp[64];
    snprintf(dirp, sizeof dirp, "/proc/%d/fd", (int)pid);
    DIR *d = opendir(dirp);
    if (!d)
        return 0;
    unsigned long found = 0;
    struct dirent *e;
    while (!found && (e = readdir(d))) {
        char lp[PATH_MAX], target[128];
        snprintf(lp, sizeof lp, "%s/%s", dirp, e->d_name);
        ssize_t r = readlink(lp, target, sizeof target - 1);
        if (r <= 0)
            continue;
        target[r] = 0;
        unsigned long ino;
        if (sscanf(target, "socket:[%lu]", &ino) == 1)
            for (int i = 0; i < n; i++)
                if (want[i] == ino)
                    found = ino;
    }
    closedir(d);
    return found;
}

/* Is a listening TCP socket on `port` held by process `pid`? */
static inline int th_owns_port(pid_t pid, int port)
{
    unsigned long inodes[64];
    int ni = 0;
    for (int i = 0; i < TH_CACHE; i++) { /* sockets proven to listen on this port before */
        unsigned long long v = atomic_load(&th_cache[i]);
        if ((int)(v >> 48) == port && v && ni < 64)
            inodes[ni++] = (unsigned long)(v & 0xffffffffffffULL);
    }
    if (ni > 0 && th_fd_socket(pid, inodes, ni))
        return 1;
    ni = 0;
    atomic_fetch_add(&th_scans, 1);
    for (int v6 = 0; v6 < 2; v6++) {
        FILE *f = fopen(v6 ? "/proc/net/tcp6" : "/proc/net/tcp", "r");
        if (!f)
            continue;
        char line[512];
        if (!fgets(line, sizeof line, f)) {
            fclose(f);
            continue;
        }
        while (fgets(line, sizeof line, f) && ni < 64) {
            unsigned lport, st;
            unsigned long ino;
            if (sscanf(line, " %*d: %*x:%x %*x:%*x %x %*x:%*x %*x:%*x %*x %*u %*u %lu", &lport, &st, &ino) == 3 &&
                st == 0x0A && (int)lport == port)
                inodes[ni++] = ino;
        }
        fclose(f);
    }
    if (ni == 0)
        return 0;
    unsigned long ino = th_fd_socket(pid, inodes, ni);
    if (ino)
        th_cache_add(port, ino);
    return ino != 0;
}

/* Why the last th_await() failed, ready to print. */
static char th_why[4352];
static int th_mport; /* the metrics port of the last th_await(), 0 if the server has none (--metrics-port auto) */
#define TH_EXITED (-1) /* the child exited before it listened (reaped) */
#define TH_BAD (-2)    /* anything else: another pid, a port that is not the child's, no file in time */

/* Waits up to timeout_ms for the port file of `child`. Returns the port, or TH_EXITED / TH_BAD with th_why set. */
static inline int th_await(const char *portfile, pid_t child, int timeout_ms)
{
    for (int waited = 0; waited <= timeout_ms; waited += 5) {
        int status;
        pid_t r = waitpid(child, &status, WNOHANG);
        if (r == child) {
            if (WIFEXITED(status))
                snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) exited with status %d before it listened\n",
                         (int)child, WEXITSTATUS(status));
            else
                snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) died of signal %d before it listened\n",
                         (int)child, WIFSIGNALED(status) ? WTERMSIG(status) : 0);
            return TH_EXITED;
        }
        FILE *f = fopen(portfile, "r");
        if (f) {
            int port = 0, pid = 0, mport = 0;
            int n = fscanf(f, "%d %d %d", &port, &pid, &mport);
            fclose(f);
            if (n >= 2 && port > 0) {
                if (pid != (int)child) {
                    snprintf(th_why, sizeof th_why,
                             "harness: %s names pid %d, but the server started here is pid %d\n", portfile, pid,
                             (int)child);
                    return TH_BAD;
                }
                if (!th_owns_port(child, port)) {
                    snprintf(th_why, sizeof th_why,
                             "harness: port %d is not held by the server started here (pid %d)\n", port, (int)child);
                    return TH_BAD;
                }
                if (n == 3 && !th_owns_port(child, mport)) {
                    snprintf(th_why, sizeof th_why,
                             "harness: metrics port %d is not held by the server started here (pid %d)\n", mport,
                             (int)child);
                    return TH_BAD;
                }
                th_mport = n == 3 ? mport : 0;
                return port;
            }
        }
        usleep(5000);
    }
    snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) wrote no %s within %d ms\n", (int)child, portfile,
             timeout_ms);
    return TH_BAD;
}
#endif
