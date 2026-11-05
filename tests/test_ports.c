/* Ports: tsdbd --port 0 --port-file, and the harness side of it (tests/harness.h).
 *
 * Two servers started at once get different ports and each port file names its own server; a server that cannot
 * start (a taken port, a bad directory, an unwritable port file) is reported as such and the harness never
 * connects to whoever holds the port; a port file that names another process, or a port another process holds,
 * is refused. A server started with --exit-with-parent stops cleanly once the process that started it is killed;
 * one without the flag keeps running. Usage: test_ports [scratch directory, default /tmp]. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_protocol.h"
#include "harness.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/prctl.h>
#include <sys/socket.h>

static int failures;
#define CHECK(cond)                                                      \
    do {                                                                 \
        if (!(cond)) {                                                   \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                  \
        }                                                                \
    } while (0)

static const char *base = "/tmp";

static void sig(pid_t pid, int s)
{
    if (pid > 0) /* never 0 or -1: that reaches a process group, or every process of the user */
        kill(pid, s);
}

/* tsdbd with the given arguments (NULL-terminated), stderr to `log` (NULL: /dev/null); --exit-with-parent unless
 * `orphan_ok` */
static pid_t spawn_with(const char *const *args, const char *log, bool orphan_ok)
{
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    pid_t pid = fork();
    if (pid == 0) {
        int dn = log ? open(log, O_WRONLY | O_CREAT | O_TRUNC, 0644) : open("/dev/null", O_WRONLY);
        if (dn >= 0)
            dup2(dn, 2);
        const char *argv[32] = {"tsdbd", "--no-auth", "--exit-with-parent"};
        int n = orphan_ok ? 2 : 3;
        for (int i = 0; args[i] && n < 31; i++)
            argv[n++] = args[i];
        argv[n] = NULL;
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    return pid;
}

static pid_t spawn(const char *const *args) { return spawn_with(args, NULL, false); }

static bool serves(int port)
{
    tsdbc *c;
    if (tsdbc_connect("127.0.0.1", port, &c) != 0)
        return false;
    tsdb_sid id;
    int rc = tsdbc_series(c, "probe", 0, &id);
    tsdbc_close(c);
    return rc == 0 || rc == TSDB_ERR_NOTFOUND;
}

static bool series_exists(int port, const char *name)
{
    tsdbc *c;
    if (tsdbc_connect("127.0.0.1", port, &c) != 0)
        return false;
    tsdb_sid id;
    int rc = tsdbc_series(c, name, 0, &id);
    tsdbc_close(c);
    return rc == 0;
}

/* th_before_owns hook: the child is killed and left a zombie (not reaped), as a server that power-cuts itself
 * between writing its port file and the harness asking who holds the port. */
static void die_now(pid_t child)
{
    sig(child, SIGKILL);
    siginfo_t si;
    si.si_pid = 0;
    waitid(P_PID, (id_t)child, &si, WEXITED | WNOWAIT);
}

static void stop(pid_t pid)
{
    sig(pid, SIGTERM);
    int st;
    waitpid(pid, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
}

/* A listening socket on a port the kernel chose; returns the fd, the port in *port. */
static int occupy(int *port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    socklen_t sl = sizeof sa;
    if (fd < 0 || bind(fd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(fd, 8) != 0 ||
        getsockname(fd, (struct sockaddr *)&sa, &sl) != 0) {
        perror("occupy");
        exit(1);
    }
    *port = ntohs(sa.sin_port);
    return fd;
}

static void rmrf(const char *path)
{
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", path);
    if (system(cmd) != 0)
        failures++;
}

static bool exists(const char *path) { return access(path, F_OK) == 0; }

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(text, f);
        fclose(f);
    }
}

static bool has_line(const char *path, const char *text)
{
    char line[1024];
    bool found = false;
    FILE *f = fopen(path, "r");
    while (f && !found && fgets(line, sizeof line, f))
        found = strstr(line, text) != NULL;
    if (f)
        fclose(f);
    return found;
}

static uint64_t rows_of(int port, const char *name)
{
    tsdbc *c;
    uint64_t n = UINT64_MAX;
    if (tsdbc_connect("127.0.0.1", port, &c) != 0)
        return n;
    tsdb_sid id;
    tsdb_bucket *b = NULL;
    size_t nb = 0;
    if (tsdbc_series(c, name, 0, &id) == 0 && tsdbc_query_agg(c, id, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0)
        n = nb ? b[0].count : 0;
    free(b);
    tsdbc_close(c);
    return n;
}

/* --exit-with-parent: a test killed by a timeout used to leave its server running under init for hours, holding an
 * io_uring ring. A middle process starts tsdbd (with or without the flag), the test writes rows through it and then
 * SIGKILLs the middle process. This process is a subreaper meanwhile, so the orphan is reparented here (its
 * getppid() changes exactly as it would to init) and its exit status can be read. With the flag the server stops
 * within a few seconds with status 0 (as on SIGTERM: one log line, the memtable flushed) and a new server on the
 * directory has every row; without it the server keeps serving until it is stopped here, by pid. */
static void orphan(bool flag)
{
    char d[600], pf[700], log[700];
    snprintf(d, sizeof d, "%s/ports-%d-orphan%d", base, (int)getpid(), (int)flag);
    snprintf(log, sizeof log, "%s.log", d);
    rmrf(d);
    th_portfile(pf, sizeof pf, d);
    unlink(pf);
    CHECK(prctl(PR_SET_CHILD_SUBREAPER, 1) == 0);
    pid_t mid = fork();
    if (mid == 0) {
        const char *a[] = {"--dir", d, "--port", "0", "--port-file", pf, "--workers", "2", "--no-fsync", NULL};
        pid_t s = spawn_with(a, log, !flag);
        if (th_await(pf, s, 30000) <= 0) {
            fputs(th_why, stderr);
            sig(s, SIGKILL);
            _exit(1);
        }
        for (;;)
            pause();
    }
    int port = 0, srv = 0;
    for (int waited = 0; waited < 30000 && mid > 0; waited += 5) {
        if (waitpid(mid, NULL, WNOHANG) == mid) {
            mid = 0; /* the server did not start (a tsdbd without the flag refuses it: status 2) */
            break;
        }
        FILE *f = fopen(pf, "r");
        int n = f ? fscanf(f, "%d %d", &port, &srv) : 0;
        if (f)
            fclose(f);
        if (n == 2 && port > 0 && srv > 0 && th_owns_port(srv, port))
            break;
        port = srv = 0;
        usleep(5000);
    }
    CHECK(port > 0 && srv > 0 && srv != mid);
    if (port > 0 && srv > 0) {
        tsdbc *c = NULL;
        tsdb_sid id = TSDB_SID_NONE;
        int64_t ts[1000];
        float v[1000];
        for (int i = 0; i < 1000; i++) {
            ts[i] = 1700000000000LL + i * 1000;
            v[i] = (float)i;
        }
        CHECK(tsdbc_connect("127.0.0.1", port, &c) == 0 && tsdbc_series(c, "o", 1, &id) == 0 &&
              tsdbc_insert_series(c, id, ts, v, 1000) == 0);
        if (c)
            tsdbc_close(c);
    }
    if (mid > 0) {
        sig(mid, SIGKILL); /* the test's parent dies: the server is an orphan now */
        waitpid(mid, NULL, 0);
    }
    if (srv > 0) {
        int st = 0;
        pid_t r = 0;
        for (int waited = 0; waited < (flag ? 10000 : 3000) && (r = waitpid(srv, &st, WNOHANG)) == 0; waited += 10)
            usleep(10000);
        if (flag) {
            CHECK(r == srv && WIFEXITED(st) && WEXITSTATUS(st) == 0);
            CHECK(has_line(log, "--exit-with-parent: parent"));
            if (r != srv) {
                sig(srv, SIGKILL);
                waitpid(srv, NULL, 0);
            }
            /* the database opens clean, with every row the orphan acknowledged */
            const char *a[] = {"--dir", d, "--port", "0", "--port-file", pf, "--workers", "2", "--no-fsync", NULL};
            unlink(pf); /* the orphan's own */
            pid_t s = spawn(a);
            int p = th_await(pf, s, 30000);
            CHECK(p > 0);
            if (p > 0) {
                CHECK(rows_of(p, "o") == 1000);
                stop(s);
            } else {
                fputs(th_why, stderr);
                sig(s, SIGKILL);
                waitpid(s, NULL, 0);
            }
        } else {
            CHECK(r == 0 && serves(port)); /* still running three seconds (three ticks) later */
            stop(srv);                     /* by its pid: it is this process's child now */
        }
    }
    CHECK(prctl(PR_SET_CHILD_SUBREAPER, 0) == 0);
    unlink(pf);
    unlink(log);
    rmrf(d);
}

int main(int argc, char **argv)
{
    if (argc > 1)
        base = argv[1];
    char d1[600], d2[600], pf1[700], pf2[700];
    snprintf(d1, sizeof d1, "%s/ports-%d-a", base, (int)getpid());
    snprintf(d2, sizeof d2, "%s/ports-%d-b", base, (int)getpid());
    rmrf(d1);
    rmrf(d2);

    /* two servers at once: different ports, each file names its server and the port it holds */
    th_portfile(pf1, sizeof pf1, d1);
    th_portfile(pf2, sizeof pf2, d2);
    const char *a1[] = {"--dir", d1, "--port", "0", "--port-file", pf1, "--workers", "2", "--no-fsync", NULL};
    const char *a2[] = {"--dir", d2, "--port", "0", "--port-file", pf2, "--workers", "2", "--no-fsync", NULL};
    pid_t s1 = spawn(a1), s2 = spawn(a2);
    int p1 = th_await(pf1, s1, 30000), p2 = th_await(pf2, s2, 30000);
    if (p1 <= 0 || p2 <= 0) {
        fputs(th_why, stderr);
        sig(s1, SIGKILL);
        sig(s2, SIGKILL);
        return 1;
    }
    CHECK(p1 != p2);
    CHECK(p1 != 7070 && p2 != 7070); /* neither is the default port */
    CHECK(th_owns_port(s1, p1) && th_owns_port(s2, p2));
    CHECK(!th_owns_port(s1, p2) && !th_owns_port(s2, p1)); /* each port is held by its own server only */
    /* th_await proved both ports: asking again must not list /proc/net/tcp (jepsen's clients ask on every connect and
     * the listing grows with every reconnect: about 15 ms per ask in a 20 s power run) */
    {
        unsigned before = atomic_load(&th_scans);
        for (int i = 0; i < 100; i++)
            CHECK(th_owns_port(s1, p1) && th_owns_port(s2, p2));
        CHECK(atomic_load(&th_scans) == before);
    }
    CHECK(serves(p1) && serves(p2));
    {
        tsdbc *c;
        tsdb_sid id;
        CHECK(tsdbc_connect("127.0.0.1", p1, &c) == 0 && tsdbc_series(c, "only.on.one", 1, &id) == 0);
        tsdbc_close(c);
        CHECK(series_exists(p1, "only.on.one") && !series_exists(p2, "only.on.one"));
    }
    /* the file is complete once it exists: no temporary left beside it */
    {
        char mine[64]; /* only our own files: other runs may have a temporary in flight in the same scratch dir */
        snprintf(mine, sizeof mine, "port.ports-%d-", (int)getpid());
        DIR *d = opendir(base);
        struct dirent *e;
        int tmp = 0;
        while (d && (e = readdir(d)))
            tmp += !strncmp(e->d_name, mine, strlen(mine)) && strstr(e->d_name, ".tmp.");
        if (d)
            closedir(d);
        CHECK(tmp == 0);
    }

    /* --metrics-port auto: the kernel chooses that port too, and the file names it as a third number */
    {
        char pf[700], d5[700], txt[64];
        snprintf(d5, sizeof d5, "%s-m", d1);
        rmrf(d5);
        th_portfile(pf, sizeof pf, d5);
        const char *a[] = {"--dir", d5, "--port", "0", "--port-file", pf, "--workers", "2", "--no-fsync",
                           "--metrics-port", "auto", NULL};
        pid_t s = spawn(a);
        int p = th_await(pf, s, 30000);
        CHECK(p > 0 && p != p1 && p != p2);
        CHECK(th_mport > 0 && th_mport != p && th_mport != p1 && th_mport != p2);
        CHECK(th_owns_port(s, th_mport) && !th_owns_port(s1, th_mport));
        CHECK(serves(p));
        snprintf(txt, sizeof txt, "%d %d %d\n", p, (int)s, p1); /* a metrics port that is another server's: refused */
        put(pf, txt);
        CHECK(th_await(pf, s, 1000) == TH_BAD);
        stop(s);
        unlink(pf);
        rmrf(d5);
    }
    /* a server without --metrics-port reports none */
    CHECK(th_await(pf1, s1, 1000) == p1 && th_mport == 0);

    /* a port file that names another server, or a port another server holds: refused */
    {
        char bad[700], txt[64];
        th_portfile(bad, sizeof bad, d1);
        snprintf(txt, sizeof txt, "%d %d\n", p1, (int)s2); /* s1's port, s2's pid */
        put(bad, txt);
        CHECK(th_await(bad, s1, 1000) == TH_BAD); /* the file names a pid that is not s1 */
        snprintf(txt, sizeof txt, "%d %d\n", p2, (int)s1); /* the right pid, the other server's port */
        put(bad, txt);
        CHECK(th_await(bad, s1, 1000) == TH_BAD);
        snprintf(txt, sizeof txt, "%d %d\n", p1, (int)s1); /* the truth */
        put(bad, txt);
        CHECK(th_await(bad, s1, 1000) == p1);
        unlink(bad);
        CHECK(th_await(bad, s1, 100) == TH_BAD); /* no file: a timeout, not a guess */
    }

    /* a server that wrote its port file and died before the harness checked the port is an exit (TH_EXITED, which
     * jepsen answers with a clean restart), not a "foreign port" (TH_BAD, which stops the run) */
    {
        char tmp[700], txt[64];
        th_portfile(tmp, sizeof tmp, d1);
        pid_t z = fork();
        if (z == 0) {
            pause();
            _exit(0);
        }
        snprintf(txt, sizeof txt, "%d %d\n", p1, (int)z); /* a port nobody holds under z's pid, like a dead server's */
        put(tmp, txt);
        th_before_owns = die_now;
        int r = th_await(tmp, z, 5000);
        th_before_owns = NULL;
        CHECK(r == TH_EXITED);
        CHECK(WIFSIGNALED(th_status) && WTERMSIG(th_status) == SIGKILL);
        unlink(tmp);
    }

    /* the same, caught earlier: the child is still alive when the harness first asks (a process in exit has closed
     * its sockets before waitpid sees it; CI hit that window) and exits a little later: still an exit, not TH_BAD */
    {
        char tmp[700], txt[64];
        th_portfile(tmp, sizeof tmp, d1);
        pid_t z = fork();
        if (z == 0) {
            usleep(200000);
            _exit(3);
        }
        snprintf(txt, sizeof txt, "%d %d\n", p1, (int)z);
        put(tmp, txt);
        int r = th_await(tmp, z, 5000);
        CHECK(r == TH_EXITED);
        CHECK(WIFEXITED(th_status) && WEXITSTATUS(th_status) == 3);
        unlink(tmp);
    }

    /* a port somebody else holds: tsdbd gives up, the harness is told so and never connects */
    {
        int taken;
        int fd = occupy(&taken);
        char pf[700], tport[16];
        th_portfile(pf, sizeof pf, d1);
        snprintf(tport, sizeof tport, "%d", taken);
        char d3[700];
        snprintf(d3, sizeof d3, "%s-c", d1);
        rmrf(d3);
        const char *a[] = {"--dir", d3, "--port", tport, "--port-file", pf, "--workers", "2", NULL};
        pid_t s = spawn(a);
        int r = th_await(pf, s, 30000);
        CHECK(r == TH_EXITED);
        CHECK(strstr(th_why, "exited with status") != NULL);
        CHECK(!exists(pf));
        close(fd);
        rmrf(d3);
    }

    /* a bad directory; and a stale port file, which the server removes instead of leaving to be believed */
    {
        char pf[700], bad[700];
        th_portfile(pf, sizeof pf, d1);
        snprintf(bad, sizeof bad, "%s/not-a-dir/db", d2); /* below a regular file */
        char file[700];
        snprintf(file, sizeof file, "%s/not-a-dir", d2);
        put(file, "x");
        put(pf, "4242 4242\n");
        const char *a[] = {"--dir", bad, "--port", "0", "--port-file", pf, NULL};
        pid_t s = spawn(a);
        int r = th_await(pf, s, 30000);
        CHECK(r == TH_EXITED || r == TH_BAD); /* never a port */
        if (r == TH_BAD) { /* it read the stale file before the server removed it: not the child's pid */
            int st = 0;
            for (int i = 0; i < 2000 && waitpid(s, &st, WNOHANG) != s; i++)
                usleep(5000);
        }
        CHECK(!exists(pf));
    }

    /* a port file that cannot be written: the server does not go on serving without telling where */
    {
        char pf[700];
        snprintf(pf, sizeof pf, "%s/no-such-dir/port", d2);
        char d4[700];
        snprintf(d4, sizeof d4, "%s-d", d1);
        rmrf(d4);
        const char *a[] = {"--dir", d4, "--port", "0", "--port-file", pf, "--workers", "2", NULL};
        pid_t s = spawn(a);
        int st = 0;
        for (int i = 0; i < 3000; i++) {
            if (waitpid(s, &st, WNOHANG) == s)
                break;
            usleep(5000);
            st = -1;
        }
        CHECK(st != -1 && WIFEXITED(st) && WEXITSTATUS(st) != 0);
        if (st == -1) {
            sig(s, SIGKILL);
            waitpid(s, NULL, 0);
        }
        rmrf(d4);
    }

    orphan(true);
    orphan(false);

    stop(s1);
    stop(s2);
    CHECK(!serves(p1) && !serves(p2));
    CHECK(!th_owns_port(s1, p1) && !th_owns_port(s2, p2)); /* a cached proof is no proof once the holder is gone */
    unlink(pf1);
    unlink(pf2);
    rmrf(d1);
    rmrf(d2);
    if (failures) {
        fprintf(stderr, "test_ports: %d failures\n", failures);
        return 1;
    }
    printf("test_ports: ok\n");
    return 0;
}
