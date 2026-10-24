/*
 * Stability of tsdbd as a process: signals, restarts, dead listeners, and
 * clients that misbehave. Each check runs under a watchdog, so a server or a
 * client that hangs fails the test instead of stopping it.
 *
 *   test_stability [scratch dir]        (TSDBD=path picks the server binary)
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_protocol.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

static const char *base = "/tmp";
static int next_port;
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

/* Starts tsdbd; extra arguments end with NULL. Does not wait for it. */
static pid_t spawn(const char *dir, int port, const char *extra1, const char *extra2)
{
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    pid_t pid = fork();
    if (pid == 0) {
        const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
        execl(bin, "tsdbd", "--dir", dir, "--port", portstr, "--workers", "4", extra1, extra2, (char *)NULL);
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

static pid_t start(const char *dir, int port, const char *extra1, const char *extra2)
{
    pid_t pid = spawn(dir, port, extra1, extra2);
    for (int i = 0; i < 300; i++) {
        if (hello(port))
            return pid;
        int st;
        if (waitpid(pid, &st, WNOHANG) == pid) {
            forget(pid);
            fprintf(stderr, "%s: tsdbd exited at start\n", current);
            failures++;
            return -1;
        }
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
    int port = next_port++;
    pid_t pid = start(dir, port, NULL, NULL);
    CHECK(insert(port, "g", 0, 100) == 0);      /* through the WAL: in the memtable */
    CHECK(insert(port, "g", 100, 100000) == 0); /* straight to a part */
    signal_pid(pid, sig);
    int st = wait_exit(pid, 10);
    CHECK(clean_exit(st));
    CHECK(port_free(port));
    pid = start(dir, port, NULL, NULL);
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
    int port = next_port++;
    uint64_t acked = 0;
    for (int i = 0; i < 15; i++) {
        pid_t pid = start(dir, port, NULL, NULL);
        CHECK(count_rows(port, "k") == (acked ? acked : UINT64_MAX)); /* the series exists after the first round */
        CHECK(insert(port, "k", (int64_t)acked, 50) == 0);
        acked += 50;
        signal_pid(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        forget(pid);
    }
    pid_t pid = start(dir, port, NULL, NULL);
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
    int port = next_port++;
    pid_t pid = start(dir, port, NULL, NULL);
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
    int port = next_port++;
    pid_t pid = start(dir, port, NULL, NULL);
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
    int port = next_port++;
    pid_t pid = start(dir, port, NULL, NULL);
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
    int port = next_port++, port2 = next_port++;
    pid_t a = start(dir, port, NULL, NULL);
    pid_t b = spawn(dir2, port, NULL, NULL); /* same port */
    int st = wait_exit(b, 10);
    CHECK(st != -1 && WIFEXITED(st) && WEXITSTATUS(st) != 0);
    pid_t c = spawn(dir, port2, NULL, NULL); /* same directory */
    st = wait_exit(c, 10);
    CHECK(st != -1 && WIFEXITED(st) && WEXITSTATUS(st) != 0);
    CHECK(port_free(port2));
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
    int port = next_port++;
    pid_t pid = start(dir, port, NULL, NULL);
    CHECK(insert(port, "q", 0, 10000) == 0);
    int before = count_fds(pid);
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
    CHECK(after == before);
    signal_pid(pid, SIGTERM);
    CHECK(clean_exit(wait_exit(pid, 10)));
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
    int wport = next_port++, fport = next_port++;
    pid_t w = start(dir, wport, NULL, NULL);
    CHECK(insert(wport, "f", 0, 5000) == 0);
    for (int i = 0; i < 3; i++) {
        pid_t f = start(dir, fport, "--follow", NULL);
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
    /* below the ephemeral range (32768 and up): a client socket may hold any port there */
    next_port = 10000 + (int)(getpid() % 20000);
    test_graceful(SIGTERM, "SIGTERM");
    test_graceful(SIGINT, "SIGINT");
    test_kill9_restart();
    test_hup_and_pipe();
    test_stop_cont();
    test_shutdown_with_clients();
    test_conflicts();
    test_storm();
    test_follower();
    alarm(0);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("stability tests passed\n");
    return 0;
}
