/* Shared by the C test harnesses that start tsdbd (test_server, test_stability, jepsen, test_ports).
 * They pass th_follow_me()'s --exit-with-parent=PID as well: a test killed by a timeout leaves no server behind.
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

/* "--exit-with-parent=PID" for a tsdbd this process is about to fork, PID being this process: the server stops once
 * the test is gone, and refuses to start when it is gone already. Call it before the fork: in the child, getppid()
 * may already name a subreaper that adopted it. */
static inline const char *th_follow_me(char *buf, size_t n)
{
    snprintf(buf, n, "--exit-with-parent=%d", (int)getpid());
    return buf;
}

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

/* th_owns_port(pid, port) asks, after a client connected to the port, whether pid holds a socket that is LISTENING
 * on it now. Only that proves the connection reached pid: a socket that listens now has listened since its listen()
 * (tsdbd never listens twice), so pid listened when the client connected, and no one else could (no SO_REUSEPORT).
 * Holding the socket is not enough: a server on its way out (tsdbd after a storage failure: shutdown() of the
 * listener, 200 ms of grace, then close) holds a socket that no longer listens, and its port is free for another
 * process to bind meanwhile, whose server then answers the client (jepsen, 2026-11-09: one run's clients wrote into
 * another run's database; test_ports taken_over). The listeners on a port come from the kernel's socket diagnostics
 * (netlink), which walk the listening sockets only: /proc/net/tcp, the fallback, costs milliseconds once it holds
 * the TIME_WAIT sockets of a few thousand reconnects, and jepsen's clients ask on every connect. */
static _Atomic unsigned th_scans; /* /proc/net/tcp listings so far (test_ports checks that netlink spares them) */

#ifdef __APPLE__
/* macOS has no /proc: libproc lists the descriptors of a process of ours (its sockets, with their state and local
 * port), and a socket is told apart by the address of its kernel object (soi_so), the counterpart of an inode. */
#include <arpa/inet.h>
#include <libproc.h>
#include <sys/proc_info.h>

/* Is a TCP socket of pid's in the LISTEN state on `port`? (only pid's own descriptors are walked: no cost to save) */
static inline int th_owns_port(pid_t pid, int port)
{
    int bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, NULL, 0);
    if (bytes <= 0)
        return 0;
    struct proc_fdinfo *fds = malloc((size_t)bytes);
    if (!fds)
        return 0;
    bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds, bytes);
    int found = 0;
    for (int i = 0; bytes > 0 && !found && i < bytes / (int)sizeof *fds; i++) {
        struct socket_fdinfo si;
        if (fds[i].proc_fdtype != PROX_FDTYPE_SOCKET ||
            proc_pidfdinfo(pid, fds[i].proc_fd, PROC_PIDFDSOCKETINFO, &si, sizeof si) != (int)sizeof si)
            continue;
        found = si.psi.soi_kind == SOCKINFO_TCP && si.psi.soi_proto.pri_tcp.tcpsi_state == TSI_S_LISTEN &&
                ntohs((uint16_t)si.psi.soi_proto.pri_tcp.tcpsi_ini.insi_lport) == port;
    }
    free(fds);
    return found;
}
#else /* Linux */
#include <arpa/inet.h>
#include <linux/inet_diag.h>
#include <linux/netlink.h>
#include <linux/sock_diag.h>
#include <netinet/in.h>
#include <sys/socket.h>

#define TH_TCP_LISTEN 10 /* the kernel's TCP_LISTEN state */

/* The inodes of the TCP sockets (IPv4 and IPv6) listening on `port` now, from sock_diag: up to max of them, their
 * count, or -1 if the kernel did not answer (the caller then lists /proc/net/tcp). */
static inline int th_listeners(int port, unsigned long *ino, int max)
{
    int n = 0;
    for (int v6 = 0; v6 < 2; v6++) {
        int s = socket(AF_NETLINK, SOCK_DGRAM | SOCK_CLOEXEC, NETLINK_SOCK_DIAG);
        if (s < 0)
            return -1;
        struct {
            struct nlmsghdr nh;
            struct inet_diag_req_v2 r;
        } req;
        memset(&req, 0, sizeof req);
        req.nh.nlmsg_len = sizeof req;
        req.nh.nlmsg_type = SOCK_DIAG_BY_FAMILY;
        req.nh.nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
        req.r.sdiag_family = v6 ? AF_INET6 : AF_INET;
        req.r.sdiag_protocol = IPPROTO_TCP;
        req.r.idiag_states = 1u << TH_TCP_LISTEN; /* the listening table only, never the TIME_WAIT sockets */
        req.r.id.idiag_sport = htons((uint16_t)port);
        struct sockaddr_nl to = {.nl_family = AF_NETLINK};
        int bad = sendto(s, &req, sizeof req, 0, (struct sockaddr *)&to, sizeof to) != (ssize_t)sizeof req;
        for (int done = 0; !bad && !done;) {
            long buf[8192 / sizeof(long)];
            ssize_t len = recv(s, buf, sizeof buf, 0);
            if (len <= 0) {
                bad = 1;
                break;
            }
            for (struct nlmsghdr *h = (struct nlmsghdr *)buf; NLMSG_OK(h, len); h = NLMSG_NEXT(h, len)) {
                if (h->nlmsg_type == NLMSG_DONE) {
                    done = 1;
                    break;
                }
                if (h->nlmsg_type == NLMSG_ERROR) {
                    bad = 1;
                    break;
                }
                const struct inet_diag_msg *m = NLMSG_DATA(h);
                if (h->nlmsg_type == SOCK_DIAG_BY_FAMILY && m->idiag_state == TH_TCP_LISTEN &&
                    ntohs(m->id.idiag_sport) == port && n < max)
                    ino[n++] = m->idiag_inode;
            }
        }
        close(s);
        if (bad)
            return -1;
    }
    return n;
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

/* Is a TCP socket held by process `pid` listening on `port` now? */
static inline int th_owns_port(pid_t pid, int port)
{
    unsigned long inodes[64];
    int ni = th_listeners(port, inodes, 64);
    if (ni >= 0)
        return ni > 0 && th_fd_socket(pid, inodes, ni) != 0;
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
    return ni > 0 && th_fd_socket(pid, inodes, ni) != 0;
}
#endif /* __APPLE__ */

/* Why the last th_await() failed, ready to print. */
static char th_why[4352];
static int th_mport; /* the metrics port of the last th_await(), 0 if the server has none (--metrics-port auto) */
static int th_pport; /* the peer port of the last th_await(), 0 if the server has none (tsdbd --cluster) */
#define TH_EXITED (-1) /* the child exited before it listened (reaped) */
#define TH_BAD (-2)    /* anything else: another pid, a port that is not the child's, no file in time */

/* Set by th_await when it reaps the child: the wait status of a server that exited before it listened. */
static int th_status;
/* Test hook: called right before th_await asks whether the child holds its port (test_ports makes the child die
 * exactly there, the race a server that power-cuts itself right after writing its port file runs into). */
static void (*th_before_owns)(pid_t child);

/* Reaps `child` if it has exited: sets th_why and th_status, returns 1. */
static inline int th_reaped(pid_t child)
{
    int status;
    if (waitpid(child, &status, WNOHANG) != child)
        return 0;
    th_status = status;
    if (WIFEXITED(status))
        snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) exited with status %d before it listened\n",
                 (int)child, WEXITSTATUS(status));
    else
        snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) died of signal %d before it listened\n",
                 (int)child, WIFSIGNALED(status) ? WTERMSIG(status) : 0);
    return 1;
}

/* Waits up to timeout_ms for the port file of `child`. Returns the port, or TH_EXITED / TH_BAD with th_why set.
 * A child that named itself in the file held the port when it wrote it; holding it no longer means it is dying (a
 * power cut by crashfs right after the write), and a process in exit has closed its sockets before waitpid sees it.
 * So a port that is not held is asked about again until the timeout, and only a child still alive then is TH_BAD. */
static inline int th_await(const char *portfile, pid_t child, int timeout_ms)
{
    int unheld = 0; /* th_why says which port the child does not hold */
    for (int waited = 0; waited <= timeout_ms; waited += 5) {
        if (th_reaped(child))
            return TH_EXITED;
        FILE *f = fopen(portfile, "r");
        if (f) {
            int port = 0, pid = 0, mport = 0, pport = 0;
            int n = fscanf(f, "%d %d %d %d", &port, &pid, &mport, &pport); /* tsdbd --cluster: PEER as a fourth */
            fclose(f);
            if (n >= 2 && port > 0) {
                if (pid != (int)child) {
                    snprintf(th_why, sizeof th_why,
                             "harness: %s names pid %d, but the server started here is pid %d\n", portfile, pid,
                             (int)child);
                    return TH_BAD;
                }
                if (th_before_owns)
                    th_before_owns(child);
                if (!th_owns_port(child, port)) {
                    /* a server that wrote its port file and then died (a fault that hits a background file
                     * operation) holds nothing: that is an exit, not a foreign port */
                    if (th_reaped(child))
                        return TH_EXITED;
                    snprintf(th_why, sizeof th_why,
                             "harness: port %d is not held by the server started here (pid %d)\n", port, (int)child);
                    unheld = 1;
                    usleep(5000);
                    continue;
                }
                if (n >= 3 && mport && !th_owns_port(child, mport)) {
                    if (th_reaped(child))
                        return TH_EXITED;
                    snprintf(th_why, sizeof th_why,
                             "harness: metrics port %d is not held by the server started here (pid %d)\n", mport,
                             (int)child);
                    unheld = 1;
                    usleep(5000);
                    continue;
                }
                if (n == 4 && !th_owns_port(child, pport)) {
                    if (th_reaped(child))
                        return TH_EXITED;
                    snprintf(th_why, sizeof th_why,
                             "harness: peer port %d is not held by the server started here (pid %d)\n", pport,
                             (int)child);
                    unheld = 1;
                    usleep(5000);
                    continue;
                }
                th_mport = n >= 3 ? mport : 0;
                th_pport = n == 4 ? pport : 0;
                return port;
            }
        }
        usleep(5000);
    }
    if (!unheld)
        snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) wrote no %s within %d ms\n", (int)child, portfile,
                 timeout_ms);
    return TH_BAD;
}
#endif
