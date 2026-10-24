/*
 * Stability of tsdbd as a process: signals, restarts, dead listeners, and
 * clients that misbehave. Each check runs under a watchdog, so a server or a
 * client that hangs fails the test instead of stopping it.
 *
 *   test_stability [scratch dir]        (TSDBD=path picks the server binary, TSDBD_IO=uring|epoll its backend)
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_protocol.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

static const char *base = "/tmp";
static char spawn_pf[PATH_MAX]; /* the port file of the server spawn() started last */
static const int64_t T0 = 1700000000000;

/* every server this test started and has not reaped, for the watchdog */
static pid_t live[16];
static const char *current = "";

/* kill() with pid 0 or -1 signals the process group, or every process of the
 * user (the terminal, the shell, everything): a failed start must never get
 * there. */
static void signal_pid(pid_t pid, int sig)
{
    if (pid <= 0) {
        fprintf(stderr, "test_stability: no server to signal (pid %d)\n", (int)pid);
        failures++;
        return;
    }
    kill(pid, sig);
}

static void forget(pid_t pid)
{
    for (int i = 0; i < 16; i++)
        if (live[i] == pid)
            live[i] = 0;
}

static void on_alarm(int sig)
{
    (void)sig;
    static const char msg[] = "test_stability: HUNG in ";
    if (write(2, msg, sizeof msg - 1) < 0 || write(2, current, strlen(current)) < 0 || write(2, "\n", 1) < 0) {
    }
    for (int i = 0; i < 16; i++)
        if (live[i] > 0)
            signal_pid(live[i], SIGKILL);
    _exit(1);
}

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

static void sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000};
    nanosleep(&d, NULL);
}

static void make_dir(char *dir, size_t len, const char *what)
{
    snprintf(dir, len, "%s/stability-%s-%d", base, what, (int)getpid());
    char cmd[700];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        exit(1);
}

static void remove_dir(const char *dir)
{
    char cmd[700];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

/* For the next spawn: io_uring_setup fails with this errno (0: it does not),
 * and the server's stderr goes to this file (NULL: inherited). */
static int deny_uring;
static const char *spawn_log;

/* In the child before exec: io_uring_setup fails with err, as it does under
 * kernel.io_uring_disabled=2 (EPERM, RHEL 10), on a kernel built without
 * io_uring (ENOSYS) or under a security module that denies it (EACCES). A
 * seccomp filter rather than an LD_PRELOAD shim: liburing makes the system call
 * itself. Calls of another architecture's ABI (x32, ia32) are let through: the
 * number would mean something else there. */
#if defined(__x86_64__)
#define TEST_AUDIT_ARCH AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#define TEST_AUDIT_ARCH AUDIT_ARCH_AARCH64
#endif
static void refuse_io_uring(int err)
{
    struct sock_filter f[] = {
#ifdef TEST_AUDIT_ARCH
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, TEST_AUDIT_ARCH, 0, 3),
#endif
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_io_uring_setup, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | ((unsigned)err & SECCOMP_RET_DATA)),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog prog = {sizeof f / sizeof f[0], f};
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 || prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
        perror("seccomp");
        _exit(126);
    }
}

/* A restart on the same explicit port (the kill -9 and follower tests) lies in the ephemeral range now. That is
 * safe in practice because the kernel gives bind(0) odd and connect() even ports, so a client socket does not take
 * a server's port; another harness's bind(0) could, and then the restart fails loudly (TH_EXITED), never silently.
 * Starts tsdbd on `port`, 0 for any free one (then tsdbd tells where it listens through spawn_pf, see start());
 * extra arguments end with NULL. Does not wait for it. */
static pid_t spawn(const char *dir, int port, const char *extra1, const char *extra2)
{
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    th_portfile(spawn_pf, sizeof spawn_pf, dir);
    pid_t pid = fork();
    if (pid == 0) {
        const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
        if (spawn_log) {
            int fd = open(spawn_log, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
            if (fd < 0 || dup2(fd, 2) < 0)
                _exit(126);
        }
        if (deny_uring)
            refuse_io_uring(deny_uring);
        const char *io = getenv("TSDBD_IO"); /* the event loop backend: make test runs uring and epoll */
        if (io)
            execl(bin, "tsdbd", "--dir", dir, "--port", portstr, "--port-file", spawn_pf, "--workers", "4", "--io", io, extra1, extra2,
                  (char *)NULL);
        else
            execl(bin, "tsdbd", "--dir", dir, "--port", portstr, "--port-file", spawn_pf, "--workers", "4",
                  extra1, extra2, (char *)NULL);
        _exit(127);
    }
    for (int i = 0; i < 16; i++)
        if (!live[i]) {
            live[i] = pid;
            break;
        }
    return pid;
}

/* A HELLO round trip, with a timeout: the server really serves (a socket that
 * only accepts is not enough). */
static bool hello(int port)
{
    tsdbc *c;
    if (tsdbc_connect("127.0.0.1", port, &c) != 0)
        return false;
    tsdbc_set_timeout(c, 2000);
    uint32_t id;
    int rc = tsdbc_series(c, "probe", 0, &id); /* any request: NOTFOUND is an answer */
    tsdbc_close(c);
    return rc == 0 || rc == TSDB_ERR_NOTFOUND;
}

/* Starts tsdbd on *port (0: any free port, which is stored there) and waits until the server started here
 * serves: its port file names its pid and the port is its own (th_await), then a request is answered. */
static pid_t start(const char *dir, int *port, const char *extra1, const char *extra2)
{
    pid_t pid = spawn(dir, *port, extra1, extra2);
    int p = th_await(spawn_pf, pid, 30000);
    unlink(spawn_pf);
    if (p <= 0) {
        fprintf(stderr, "%s: %s", current, th_why);
        failures++;
        if (p == TH_BAD)
            signal_pid(pid, SIGKILL), waitpid(pid, NULL, 0);
        forget(pid);
        return -1;
    }
    *port = p;
    for (int i = 0; i < 300; i++) {
        if (hello(p))
            return pid;
        sleep_ms(10);
    }
    fprintf(stderr, "%s: tsdbd did not start serving\n", current);
    failures++;
    return pid;
}

/* Waits up to `seconds` for pid; returns its wait status, or -1 (then kills it). */
static int wait_exit(pid_t pid, double seconds)
{
    double t0 = now();
    int st;
    while (now() - t0 < seconds) {
        if (waitpid(pid, &st, WNOHANG) == pid) {
            forget(pid);
            return st;
        }
        sleep_ms(5);
    }
    signal_pid(pid, SIGKILL);
    waitpid(pid, &st, 0);
    forget(pid);
    return -1;
}

static bool clean_exit(int st) { return st != -1 && WIFEXITED(st) && WEXITSTATUS(st) == 0; }

/* Nothing listens: a connect is refused at once, and the port can be bound. */
static bool port_free(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    bool refused = connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0 && errno == ECONNREFUSED;
    close(fd);
    fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    bool bindable = bind(fd, (struct sockaddr *)&sa, sizeof sa) == 0;
    close(fd);
    return refused && bindable;
}

static int raw_connect(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        close(fd);
        return -1;
    }
    struct timeval tv = {5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    return fd;
}

static uint64_t count_rows(int port, const char *name)
{
    tsdbc *c;
    uint64_t n = UINT64_MAX;
    if (tsdbc_connect("127.0.0.1", port, &c) != 0)
        return n;
    tsdbc_set_timeout(c, 5000);
    uint32_t id;
    tsdb_bucket *b;
    size_t nb;
    if (tsdbc_series(c, name, 0, &id) == 0 && tsdbc_query_agg(c, id, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0) {
        n = nb ? b[0].count : 0;
        free(b);
    }
    tsdbc_close(c);
    return n;
}

/* Inserts rows [k0, k0 + n) of series `name`; returns 0 when acknowledged. */
static int insert(int port, const char *name, int64_t k0, size_t n)
{
    tsdbc *c;
    if (tsdbc_connect("127.0.0.1", port, &c) != 0)
        return -1;
    tsdbc_set_timeout(c, 10000);
    uint32_t id;
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    for (size_t i = 0; i < n; i++) {
        ts[i] = T0 + (k0 + (int64_t)i) * 1000;
        v[i] = (float)(k0 + (int64_t)i);
    }
    int rc = tsdbc_series(c, name, 1, &id);
    if (rc == 0)
        rc = tsdbc_insert_series(c, id, ts, v, n);
    tsdbc_close(c);
    free(ts);
    free(v);
    return rc;
}

/* ------------------------------------------------------------------ tests */

/* SIGTERM and SIGINT: a clean exit, status 0, the port free at once, and
 * every acknowledged row there after a restart. */
static void test_graceful(int sig, const char *what)
{
    current = what;
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, what);
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    CHECK(insert(port, "g", 0, 100) == 0);      /* through the WAL: in the memtable */
    CHECK(insert(port, "g", 100, 100000) == 0); /* straight to a part */
    signal_pid(pid, sig);
    int st = wait_exit(pid, 10);
    CHECK(clean_exit(st));
    CHECK(port_free(port));
    pid = start(dir, &port, NULL, NULL);
    CHECK(count_rows(port, "g") == 100100);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* Regression: after SIGKILL the dead server's listening socket stayed alive
 * for a moment (io_uring held it): a server started at once could not bind,
 * or clients reached the dead listener and waited. Every acknowledged row
 * survives, and the new server serves at once. */
static void test_kill9_restart(void)
{
    current = "kill -9 and restart";
    alarm(120);
    char dir[512];
    make_dir(dir, sizeof dir, "kill9");
    int port = 0;
    uint64_t acked = 0;
    for (int i = 0; i < 15; i++) {
        pid_t pid = start(dir, &port, NULL, NULL);
        CHECK(count_rows(port, "k") == (acked ? acked : UINT64_MAX)); /* the series exists after the first round */
        CHECK(insert(port, "k", (int64_t)acked, 50) == 0);
        acked += 50;
        signal_pid(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        forget(pid);
    }
    pid_t pid = start(dir, &port, NULL, NULL);
    CHECK(count_rows(port, "k") == acked);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* SIGHUP is ignored; SIGPIPE from a client that vanished mid-stream is too. */
static void test_hup_and_pipe(void)
{
    current = "SIGHUP and SIGPIPE";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "hup");
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    signal_pid(pid, SIGHUP);
    sleep_ms(200);
    CHECK(hello(port));
    /* a large result, abandoned with a reset after the first bytes */
    CHECK(insert(port, "p", 0, 2000000) == 0);
    tsdbc *c;
    uint32_t id;
    CHECK(tsdbc_connect("127.0.0.1", port, &c) == 0 && tsdbc_series(c, "p", 0, &id) == 0);
    tsdbc_close(c);
    for (int round = 0; round < 5; round++) {
        int fd = raw_connect(port);
        uint8_t q[29] = {0};
        int64_t lo = INT64_MIN, hi = INT64_MAX;
        memcpy(q, &id, 4);
        memcpy(q + 4, &lo, 8);
        memcpy(q + 12, &hi, 8);
        uint8_t h[9];
        uint32_t flen = 5 + sizeof q, rid = 1;
        memcpy(h, &flen, 4);
        memcpy(h + 4, &rid, 4);
        h[8] = TSDB_OP_QUERY_RAW;
        CHECK(write(fd, h, 9) == 9 && write(fd, q, sizeof q) == (ssize_t)sizeof q);
        char buf[4096];
        CHECK(read(fd, buf, sizeof buf) > 0);
        struct linger lg = {1, 0}; /* close with a reset */
        setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof lg);
        close(fd);
    }
    sleep_ms(100);
    CHECK(hello(port));
    int st;
    CHECK(waitpid(pid, &st, WNOHANG) == 0); /* still running */
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* SIGSTOP and SIGCONT: a request sent while it is stopped completes after. */
static void test_stop_cont(void)
{
    current = "SIGSTOP and SIGCONT";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "stop");
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    signal_pid(pid, SIGSTOP);
    pid_t kid = fork();
    if (kid == 0) {
        _exit(insert(port, "s", 0, 1000) == 0 ? 0 : 1); /* blocks until the server runs again */
    }
    sleep_ms(500);
    int st;
    CHECK(waitpid(kid, &st, WNOHANG) == 0); /* still waiting */
    signal_pid(pid, SIGCONT);
    CHECK(waitpid(kid, &st, 0) == kid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    CHECK(count_rows(port, "s") == 1000);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* Shutdown with clients in every state: streaming a result they do not read
 * (its worker waits for output space), idle, and half way through sending a
 * frame. The server still exits promptly. */
static void test_shutdown_with_clients(void)
{
    current = "shutdown with stuck clients";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "stuck");
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    CHECK(insert(port, "big", 0, 3000000) == 0);
    tsdbc *c;
    uint32_t id;
    CHECK(tsdbc_connect("127.0.0.1", port, &c) == 0 && tsdbc_series(c, "big", 0, &id) == 0);
    tsdbc_close(c);
    int fds[100], nfd = 0;
    for (int i = 0; i < 4; i++) { /* raw queries never read: output fills, workers block */
        int fd = raw_connect(port);
        uint8_t f[9 + 20];
        uint32_t flen = 25, rid = 1;
        int64_t lo = INT64_MIN, hi = INT64_MAX;
        memcpy(f, &flen, 4);
        memcpy(f + 4, &rid, 4);
        f[8] = TSDB_OP_QUERY_RAW;
        memcpy(f + 9, &id, 4);
        memcpy(f + 13, &lo, 8);
        memcpy(f + 21, &hi, 8);
        CHECK(write(fd, f, sizeof f) == (ssize_t)sizeof f);
        fds[nfd++] = fd;
    }
    for (int i = 0; i < 50; i++) /* idle */
        fds[nfd++] = raw_connect(port);
    int half = raw_connect(port); /* a frame announced as 1000 bytes, 9 sent */
    uint8_t part[9] = {0xe8, 0x03, 0, 0, 1, 0, 0, 0, TSDB_OP_INSERT};
    CHECK(write(half, part, sizeof part) == (ssize_t)sizeof part);
    fds[nfd++] = half;
    sleep_ms(300);
    CHECK(hello(port)); /* others are still served */
    double t0 = now();
    signal_pid(pid, SIGTERM);
    int st = wait_exit(pid, 10);
    CHECK(clean_exit(st));
    if (st != -1)
        CHECK(now() - t0 < 5);
    CHECK(port_free(port));
    for (int i = 0; i < nfd; i++)
        close(fds[i]);
    remove_dir(dir);
}

/* Starting where something already runs: a taken port, a directory another
 * writer holds. The second server must give up with an error, not hang or
 * serve from a database it does not own. */
static void test_conflicts(void)
{
    current = "port and directory conflicts";
    alarm(60);
    char dir[512], dir2[512];
    make_dir(dir, sizeof dir, "conflict");
    make_dir(dir2, sizeof dir2, "conflict2");
    int port = 0;
    pid_t a = start(dir, &port, NULL, NULL);
    pid_t b = spawn(dir2, port, NULL, NULL); /* same port */
    int st = wait_exit(b, 10);
    CHECK(st != -1 && WIFEXITED(st) && WEXITSTATUS(st) != 0);
    CHECK(access(spawn_pf, F_OK) != 0); /* it never claimed to listen */
    unlink(spawn_pf);
    pid_t c = spawn(dir, 0, NULL, NULL); /* same directory */
    st = wait_exit(c, 10);
    CHECK(st != -1 && WIFEXITED(st) && WEXITSTATUS(st) != 0);
    CHECK(access(spawn_pf, F_OK) != 0); /* nor this one */
    unlink(spawn_pf);
    CHECK(hello(port)); /* the first one is untouched */
    signal_pid(a, SIGTERM);
    CHECK(clean_exit(wait_exit(a, 10)));
    remove_dir(dir);
    remove_dir(dir2);
}

/* Many connections opened and dropped at once, some mid-request, while others
 * query: the server answers throughout and holds no descriptors afterwards. */
typedef struct storm {
    int port, n, failures;
} storm;

static void *storm_main(void *arg)
{
    storm *s = arg;
    for (int i = 0; i < s->n; i++) {
        int fd = raw_connect(s->port);
        if (fd < 0) {
            s->failures++;
            continue;
        }
        if (i % 3 == 0) { /* half a header, then gone */
            uint8_t junk[5] = {9, 0, 0, 0, 1};
            if (write(fd, junk, sizeof junk) < 0)
                s->failures++;
        }
        if (i % 7 == 0) {
            struct linger lg = {1, 0};
            setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof lg);
        }
        close(fd);
    }
    return NULL;
}

static int count_fds(pid_t pid)
{
    char path[64];
    snprintf(path, sizeof path, "/proc/%d/fd", (int)pid);
    DIR *d = opendir(path);
    int n = 0;
    if (!d)
        return -1;
    while (readdir(d))
        n++;
    closedir(d);
    return n;
}

static void test_storm(void)
{
    current = "connection storm";
    alarm(90);
    char dir[512];
    make_dir(dir, sizeof dir, "storm");
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    CHECK(insert(port, "q", 0, 10000) == 0);
    /* The server closes its end of the insert connection only once it sees the client's close, so a count taken
     * at once can still include it (seen with epoll): the baseline is the count once it has stopped falling. */
    int before = count_fds(pid);
    for (int stable = 0, i = 0; stable < 5 && i < 100; i++) {
        sleep_ms(20);
        int n = count_fds(pid);
        stable = n == before ? stable + 1 : 0;
        before = n;
    }
    enum { NT = 8 };
    pthread_t th[NT];
    storm s[NT];
    for (int i = 0; i < NT; i++) {
        s[i] = (storm){port, 500, 0};
        pthread_create(&th[i], NULL, storm_main, &s[i]);
    }
    int answered = 0;
    for (int i = 0; i < 50; i++)
        answered += count_rows(port, "q") == 10000;
    for (int i = 0; i < NT; i++) {
        pthread_join(th[i], NULL);
        CHECK(s[i].failures == 0);
    }
    CHECK(answered == 50);
    int after = -1;
    for (int i = 0; i < 100 && after != before; i++) { /* closes reach the server shortly after */
        sleep_ms(20);
        after = count_fds(pid);
    }
    if (after != before) {
        fprintf(stderr, "%s: %d descriptors before, %d after:\n", current, before, after);
        char cmd[128];
        snprintf(cmd, sizeof cmd, "ls -l /proc/%d/fd >&2", (int)pid);
        if (system(cmd) != 0)
            fprintf(stderr, "(no listing)\n");
    }
    CHECK(after == before);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* Asks for every row of series id in one QUERY_RAW; the reply streams in chunks. */
static void send_query_raw(int fd, uint32_t id)
{
    uint8_t f[9 + 20];
    uint32_t flen = 25, rid = 7;
    int64_t lo = INT64_MIN, hi = INT64_MAX;
    memcpy(f, &flen, 4);
    memcpy(f + 4, &rid, 4);
    f[8] = TSDB_OP_QUERY_RAW;
    memcpy(f + 9, &id, 4);
    memcpy(f + 13, &lo, 8);
    memcpy(f + 21, &hi, 8);
    CHECK(write(fd, f, sizeof f) == (ssize_t)sizeof f);
}

static bool read_full(int fd, void *p, size_t n)
{
    for (size_t got = 0; got < n;) {
        ssize_t r = read(fd, (uint8_t *)p + got, n - got);
        if (r <= 0)
            return false;
        got += (size_t)r;
    }
    return true;
}

/* Reads a QUERY_RAW reply to the end, pausing between reads like a slow
 * consumer; returns the rows, or UINT64_MAX when the stream breaks, a chunk
 * fails or timestamps do not increase. */
static uint64_t read_raw_slowly(int fd)
{
    uint64_t rows = 0;
    int64_t prev = INT64_MIN;
    size_t since_pause = 0;
    static uint8_t body[16384 * 12 + 64];
    for (;;) {
        uint8_t h[18]; /* length, id, op, status, last, n */
        if (!read_full(fd, h, sizeof h))
            return UINT64_MAX;
        uint32_t len, n;
        int32_t st;
        memcpy(&len, h, 4);
        memcpy(&st, h + 9, 4);
        memcpy(&n, h + 14, 4);
        if (st != 0 || h[8] != TSDB_OP_REPLY || len != 14 + (uint64_t)n * 12 || n > 16384)
            return UINT64_MAX;
        if (!read_full(fd, body, (size_t)n * 12))
            return UINT64_MAX;
        for (uint32_t i = 0; i < n; i++) {
            int64_t t;
            memcpy(&t, body + (size_t)i * 8, 8);
            if (t <= prev)
                return UINT64_MAX;
            prev = t;
        }
        rows += n;
        if ((since_pause += (size_t)n * 12) > (256u << 10)) {
            since_pause = 0;
            sleep_ms(1); /* the server finds the socket full and waits for it to drain */
        }
        if (h[13])
            return rows;
    }
}

/* Back-pressure: a slow reader gets a large result whole and in order; clients
 * that read nothing hold no more than their workers (extra ones start, so
 * others are served) and lose the connection after --send-timeout. */
static void test_slow_readers(void)
{
    current = "slow and stuck readers";
    alarm(120);
    char dir[512];
    make_dir(dir, sizeof dir, "slow");
    int port = 0;
    pid_t pid = start(dir, &port, "--workers=1", "--send-timeout=2");
    enum { N = 3000000 };
    CHECK(insert(port, "big", 0, N) == 0);
    tsdbc *c;
    uint32_t id = 0;
    CHECK(tsdbc_connect("127.0.0.1", port, &c) == 0 && tsdbc_series(c, "big", 0, &id) == 0);
    tsdbc_close(c);

    int fd = raw_connect(port);
    send_query_raw(fd, id);
    CHECK(read_raw_slowly(fd) == N);
    close(fd);

    int stuck[2];
    for (int i = 0; i < 2; i++) {
        stuck[i] = raw_connect(port);
        send_query_raw(stuck[i], id);
    }
    sleep_ms(300);
    CHECK(hello(port)); /* the only worker and an extra one wait for output space */
    sleep_ms(4000);     /* past the send timeout */
    for (int i = 0; i < 2; i++) {
        size_t got = 0;
        uint8_t b[65536];
        ssize_t r;
        while ((r = read(stuck[i], b, sizeof b)) > 0)
            got += (size_t)r;
        CHECK(r == 0 || errno == ECONNRESET); /* cut off, not still streaming (EAGAIN after 5 s) */
        CHECK(got < (size_t)N * 12);
        close(stuck[i]);
    }
    CHECK(count_rows(port, "big") == N);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* CPU time (user + system) the thread tid of process pid has used, in seconds. */
static double thread_cpu(pid_t pid, pid_t tid)
{
    char path[64], buf[1024];
    snprintf(path, sizeof path, "/proc/%d/task/%d/stat", (int)pid, (int)tid);
    FILE *f = fopen(path, "r");
    if (!f)
        return -1;
    size_t n = fread(buf, 1, sizeof buf - 1, f);
    fclose(f);
    buf[n] = 0;
    char *p = strrchr(buf, ')'); /* the name may hold spaces */
    unsigned long long ut, st;
    /* after the name: state (field 3) ... utime (14) and stime (15) */
    if (!p || sscanf(p + 1, " %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %llu %llu", &ut, &st) != 2)
        return -1;
    return (double)(ut + st) / (double)sysconf(_SC_CLK_TCK);
}

/* Regression: a client that stops reading must cost the event loop nothing
 * while its send waits. An epoll loop that keeps such a socket marked writable
 * after EAGAIN tries the send again on every round and burns a core per stuck
 * client, which no other test notices. */
static void test_stuck_reader_idle(void)
{
    current = "stuck reader: idle event loop";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "idle");
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    enum { N = 1000000 }; /* 12 MB of reply: far beyond both socket buffers */
    CHECK(insert(port, "big", 0, N) == 0);
    tsdbc *c;
    uint32_t id = 0;
    CHECK(tsdbc_connect("127.0.0.1", port, &c) == 0 && tsdbc_series(c, "big", 0, &id) == 0);
    tsdbc_close(c);
    int fd = raw_connect(port);
    int small = 4096; /* and no receive buffer autotuning */
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof small);
    send_query_raw(fd, id);
    sleep_ms(500); /* the send now waits for the client */
    double t0 = thread_cpu(pid, pid), w0 = now();
    sleep_ms(2000);
    double t1 = thread_cpu(pid, pid), w1 = now();
    CHECK(t0 >= 0 && t1 >= 0);
    if (t1 - t0 > 0.25 * (w1 - w0)) { /* a spinning loop uses all of it; an idle one about none */
        fprintf(stderr, "%s: event loop used %.2f s of CPU in %.2f s\n", current, t1 - t0, w1 - w0);
        failures++;
    }
    CHECK(hello(port));
    close(fd);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* A request any server answers at once (an unknown operation: INVALID). */
static bool answers(int fd)
{
    uint8_t f[9] = {5, 0, 0, 0, 1, 0, 0, 0, 250}, r[13];
    return write(fd, f, sizeof f) == (ssize_t)sizeof f && read_full(fd, r, sizeof r) && r[8] == TSDB_OP_REPLY;
}

/* --max-connections: one more is accepted and closed at once; when one
 * leaves, the next gets in. */
static void test_connection_limit(void)
{
    current = "connection limit";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "limit");
    int port = 0;
    pid_t pid = start(dir, &port, "--max-connections=20", NULL);
    sleep_ms(200); /* until the server has seen start's probe go */
    int fds[20];
    for (int i = 0; i < 20; i++) {
        fds[i] = raw_connect(port);
        CHECK(fds[i] >= 0 && answers(fds[i]));
    }
    int over = raw_connect(port);
    uint8_t b[16];
    CHECK(over >= 0 && !answers(over));
    errno = 0;
    CHECK(read(over, b, sizeof b) <= 0 && errno != EAGAIN); /* closed, not just slow */
    close(over);
    close(fds[0]);
    bool in = false;
    for (int i = 0; i < 100 && !in; i++) { /* once the server has seen the close */
        sleep_ms(20);
        int fd = raw_connect(port);
        in = fd >= 0 && answers(fd);
        if (fd >= 0) {
            if (in)
                fds[0] = fd;
            else
                close(fd);
        }
    }
    CHECK(in);
    for (int i = 0; i < 20; i++)
        if (fds[i] >= 0)
            close(fds[i]);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

/* A client sends its last requests and its end of stream while the server is
 * stopped, so both are waiting when it looks again: every request is answered,
 * in order, and then the server closes. (epoll: one event says readable and
 * hung up; a recv that empties the socket must not be taken as "wait for the
 * next edge", which never comes.) */
static void test_half_close_while_stopped(void)
{
    current = "end of stream while stopped";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "halfclose");
    int port = 0;
    pid_t pid = start(dir, &port, NULL, NULL);
    for (int round = 0; round < 3; round++) {
        int fd = raw_connect(port);
        CHECK(fd >= 0 && answers(fd)); /* registered, and idle again */
        sleep_ms(50);
        signal_pid(pid, SIGSTOP);
        sleep_ms(50);
        uint8_t f[50 * 9];
        for (uint32_t i = 0; i < 50; i++) {
            uint8_t h[9] = {5, 0, 0, 0, 0, 0, 0, 0, 250};
            memcpy(h + 4, &i, 4);
            memcpy(f + i * 9, h, 9);
        }
        CHECK(write(fd, f, sizeof f) == (ssize_t)sizeof f);
        shutdown(fd, SHUT_WR);
        sleep_ms(50);
        signal_pid(pid, SIGCONT);
        uint32_t n = 0;
        uint8_t r[13];
        while (read_full(fd, r, sizeof r)) {
            uint32_t rid;
            memcpy(&rid, r + 4, 4);
            CHECK(rid == n && r[8] == TSDB_OP_REPLY);
            n++;
        }
        CHECK(n == 50);
        errno = 0;
        CHECK(read(fd, r, sizeof r) == 0); /* the end, not a timeout */
        close(fd);
    }
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
    remove_dir(dir);
}

static bool file_has(const char *path, const char *text)
{
    char buf[8192];
    FILE *f = fopen(path, "r");
    if (!f)
        return false;
    size_t n = fread(buf, 1, sizeof buf - 1, f);
    fclose(f);
    buf[n] = 0;
    return strstr(buf, text) != NULL;
}

/* AlmaLinux and RHEL 10 refuse io_uring: --io auto (the default) must serve on
 * epoll and say why, whatever the error, and --io uring must fail as before.
 * Skipped in the epoll pass of make test: it picks the backend itself, so the
 * other pass has already run it. */
static void test_io_fallback(void)
{
    const char *io = getenv("TSDBD_IO");
    if (io && strcmp(io, "epoll") == 0)
        return;
    current = "io_uring refused: epoll fallback";
    alarm(120);
    char dir[512], log[600];
    make_dir(dir, sizeof dir, "iofallback");
    snprintf(log, sizeof log, "%s.log", dir);
    spawn_log = log;
    /* policy (RHEL 10), not built in, too old for the flags, denied by a security module */
    static const int errs[] = {EPERM, ENOSYS, EINVAL, EACCES};
    enum { NERRS = sizeof errs / sizeof errs[0] };
    for (int i = 0; i < NERRS; i++) {
        int port = 0;
        deny_uring = errs[i];
        pid_t pid = start(dir, &port, "--io", "auto"); /* last wins over TSDBD_IO */
        deny_uring = 0;
        CHECK(pid > 0);
        if (pid <= 0)
            continue;
        CHECK(insert(port, "fb", i * 1000, 1000) == 0);
        CHECK(count_rows(port, "fb") == (uint64_t)(i + 1) * 1000);
        signal_pid(pid, SIGTERM);
        CHECK(clean_exit(wait_exit(pid, 10)));
        char want[128];
        snprintf(want, sizeof want, "io_uring unavailable (%s", strerror(errs[i]));
        CHECK(file_has(log, want));
        CHECK(file_has(log, "using epoll"));
        CHECK(file_has(log, ", io epoll\n"));
    }
    /* asked for io_uring explicitly: no fallback */
    int port = 0;
    deny_uring = EPERM;
    pid_t pid = spawn(dir, port, "--io", "uring");
    deny_uring = 0;
    int st = wait_exit(pid, 10);
    CHECK(st != -1 && WIFEXITED(st) && WEXITSTATUS(st) == 1);
    CHECK(file_has(log, "tsdbd: io_uring: Operation not permitted"));
    CHECK(!file_has(log, "using epoll"));
    CHECK(access(spawn_pf, F_OK) != 0); /* it never claimed to listen */
    unlink(spawn_pf);
    spawn_log = NULL;
    unlink(log);
    remove_dir(dir);
}

/* A follower is stopped and restarted under a running writer; the writer does
 * not notice, and the follower sees the writer's data again. */
static void test_follower(void)
{
    current = "follower signals";
    alarm(60);
    char dir[512];
    make_dir(dir, sizeof dir, "follow");
    int wport = 0, fport = 0;
    pid_t w = start(dir, &wport, NULL, NULL);
    CHECK(insert(wport, "f", 0, 5000) == 0);
    for (int i = 0; i < 3; i++) {
        pid_t f = start(dir, &fport, "--follow", NULL);
        uint64_t n = 0;
        for (int k = 0; k < 200 && n != 5000; k++) { /* a few refreshes */
            n = count_rows(fport, "f");
            sleep_ms(20);
        }
        CHECK(n == 5000);
        signal_pid(f, i == 1 ? SIGKILL : SIGTERM);
        int st = wait_exit(f, 10);
        CHECK(i == 1 ? st != -1 : clean_exit(st));
        CHECK(port_free(fport));
    }
    CHECK(hello(wport));
    signal_pid(w, SIGTERM);
    CHECK(clean_exit(wait_exit(w, 10)));
    remove_dir(dir);
}

int main(int argc, char **argv)
{
    if (argc > 1)
        base = argv[1];
    signal(SIGALRM, on_alarm);
    signal(SIGPIPE, SIG_IGN);
    test_graceful(SIGTERM, "SIGTERM");
    test_graceful(SIGINT, "SIGINT");
    test_kill9_restart();
    test_hup_and_pipe();
    test_stop_cont();
    test_shutdown_with_clients();
    test_conflicts();
    test_storm();
    test_follower();
    test_slow_readers();
    test_stuck_reader_idle();
    test_connection_limit();
    test_half_close_while_stopped();
    test_io_fallback();
    alarm(0);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("stability tests passed\n");
    return 0;
}
