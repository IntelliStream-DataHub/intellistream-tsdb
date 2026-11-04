/* Shared by the login tests (tests/test_auth.c, tests/test_auth_ops.c): tsdbd started with login on, users made with
 * the CLI, and raw frames with a SCRAM client of their own, for what the client library never sends (malformed
 * logins, every op number). A caller defines _GNU_SOURCE and includes tsdb_client.h, tsdb_crypto.h, tsdb_protocol.h,
 * users.h and harness.h first. */
#ifndef TSDB_TEST_AUTH_HARNESS_H
#define TSDB_TEST_AUTH_HARNESS_H

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>

static int failures;
#define CHECK(cond)                                                                                                \
    do {                                                                                                           \
        if (!(cond)) {                                                                                             \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                              \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)
#define OK(expr)                                                                                                   \
    do {                                                                                                           \
        int rc_ = (expr);                                                                                          \
        if (rc_ != 0) {                                                                                            \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdbc_strerror(rc_));                 \
            exit(1);                                                                                               \
        }                                                                                                          \
    } while (0)

static int port;
static char cli[PATH_MAX + 8], srvlog[PATH_MAX + 32], clilog[PATH_MAX + 32];
static const int64_t T0 = 1700000000000;

/* The paths the helpers use: the server's log and the CLI's beside dir, the CLI beside TSDBD. */
static inline void ah_setup(const char *dir)
{
    snprintf(srvlog, sizeof srvlog, "%s.log", dir);
    snprintf(clilog, sizeof clilog, "%s.cli.log", dir);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    const char *slash = strrchr(bin, '/');
    snprintf(cli, sizeof cli, "%.*stsdb", slash ? (int)(slash - bin + 1) : 0, bin);
    unlink(srvlog);
    unlink(clilog);
}

static inline double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + t.tv_nsec / 1e9;
}

static inline void nap_ms(int ms)
{
    struct timespec t = {ms / 1000, (long)(ms % 1000) * 1000000};
    nanosleep(&t, NULL);
}

/* ------------------------------------------------------------ the server */

/* tsdbd on dir with login on (no --no-auth unless extra has it), its stderr appended to srvlog. */
static inline pid_t spawn(const char *dir, const char *const *extra, char *pf, size_t pfn)
{
    th_portfile(pf, pfn, dir);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    pid_t pid = fork();
    if (pid == 0) {
        int fd = open(srvlog, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0)
            dup2(fd, 2);
        const char *argv[32] = {"tsdbd", "--dir", dir, "--port", "0", "--port-file", pf, "--workers", "2", "--no-fsync"};
        int n = 10;
        if (getenv("TSDBD_IO")) {
            argv[n++] = "--io";
            argv[n++] = getenv("TSDBD_IO");
        }
        for (int i = 0; extra && extra[i] && n < 31; i++)
            argv[n++] = extra[i];
        argv[n] = NULL;
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    return pid;
}

static inline pid_t start(const char *dir, const char *const *extra)
{
    char pf[PATH_MAX + 64];
    pid_t pid = spawn(dir, extra, pf, sizeof pf);
    int p = th_await(pf, pid, 30000);
    unlink(pf);
    if (p <= 0) {
        fputs(th_why, stderr);
        fprintf(stderr, "server log: %s\n", srvlog);
        if (p != TH_EXITED && pid > 0) {
            kill(pid, SIGKILL);
            waitpid(pid, NULL, 0);
        }
        exit(1);
    }
    port = p;
    return pid;
}

/* A start that must fail: the server's exit status, or -1 when it listened after all (stopped here). */
static inline int start_fails(const char *dir, const char *const *extra)
{
    char pf[PATH_MAX + 64];
    pid_t pid = spawn(dir, extra, pf, sizeof pf);
    int p = th_await(pf, pid, 30000);
    unlink(pf);
    if (p == TH_EXITED)
        return WIFEXITED(th_status) ? WEXITSTATUS(th_status) : 128 + WTERMSIG(th_status);
    if (pid > 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
    }
    return -1;
}

static inline void stop(pid_t pid)
{
    if (pid <= 0)
        return;
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

/* How often needle appears in the server's log. */
static inline int log_count(const char *needle)
{
    FILE *f = fopen(srvlog, "r");
    if (!f)
        return 0;
    char line[4096];
    int n = 0;
    while (fgets(line, sizeof line, f))
        n += strstr(line, needle) != NULL;
    fclose(f);
    return n;
}

/* `tsdb DIR user ARGS` with TSDB_PASSWORD=pw (NULL: unset): its exit status. */
static inline int cli_user(const char *dir, const char *pw, const char *fmt, ...)
{
    char args[1024], cmd[3 * PATH_MAX + 1200];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(args, sizeof args, fmt, ap);
    va_end(ap);
    snprintf(cmd, sizeof cmd, "%s%s%s %s %s user %s </dev/null >>%s 2>&1", pw ? "TSDB_PASSWORD='" : "",
             pw ? pw : "", pw ? "'" : "", cli, dir, args, clilog);
    int st = system(cmd);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static inline tsdbc *login_as(const char *user, const char *pw)
{
    tsdbc *c = NULL;
    OK(tsdbc_connect_auth("127.0.0.1", port, user, pw, &c));
    return c;
}

static inline int count_fn(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts;
    (void)v;
    *(size_t *)ctx += n;
    return 0;
}

static inline int query(tsdbc *c, tsdb_sid id)
{
    size_t n = 0;
    return tsdbc_query_raw(c, id, T0, T0 + 1000, count_fn, &n);
}

/* ------------------------------------------------------------ raw frames */

static inline int raw_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        perror("connect");
        exit(1);
    }
    struct timeval tv = {20, 0}; /* a server that neither answers nor closes fails the test, not hangs it */
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    return fd;
}

static inline void raw_send(int fd, uint32_t id, uint8_t op, const void *payload, uint32_t len)
{
    uint8_t h[9];
    uint32_t flen = len + 5;
    memcpy(h, &flen, 4);
    memcpy(h + 4, &id, 4);
    h[8] = op;
    if (write(fd, h, 9) != 9 || (len && write(fd, payload, len) != (ssize_t)len))
        return; /* the server may close first: the reply (or its absence) tells */
}

static inline bool read_full(int fd, void *p, size_t n)
{
    for (size_t got = 0; got < n;) {
        ssize_t r = read(fd, (uint8_t *)p + got, n - got);
        if (r <= 0)
            return false;
        got += (size_t)r;
    }
    return true;
}

/* One reply frame: its status (INT32_MIN at the end of the stream), its id to *id when not NULL, the bytes after the
 * status to rest. Bytes past cap are read and dropped (*nrest says how many there were). The whole frame (header
 * included, at most 13 + cap bytes) to frame when not NULL, for comparing two answers byte for byte. */
static inline int32_t raw_frame(int fd, uint32_t *id, uint8_t *rest, size_t cap, size_t *nrest, uint8_t *frame,
                                size_t *nframe)
{
    uint8_t h[13];
    if (!read_full(fd, h, 13))
        return INT32_MIN;
    uint32_t flen;
    int32_t status;
    memcpy(&flen, h, 4);
    memcpy(&status, h + 9, 4);
    if (id)
        memcpy(id, h + 4, 4);
    size_t n = flen - 9, keep = n < cap ? n : cap;
    if (h[8] != TSDB_OP_REPLY || flen < 9 || !read_full(fd, rest, keep)) {
        CHECK(!"a reply frame");
        return INT32_MIN;
    }
    for (size_t left = n - keep; left > 0;) { /* a large reply (QUERY_RAW chunks): only its start matters here */
        uint8_t skip[4096];
        size_t k = left < sizeof skip ? left : sizeof skip;
        if (!read_full(fd, skip, k))
            return INT32_MIN;
        left -= k;
    }
    *nrest = n;
    if (frame) {
        memcpy(frame, h, 13);
        memcpy(frame + 13, rest, keep);
        *nframe = 13 + keep;
    }
    return status;
}

static inline int32_t raw_reply(int fd, uint8_t *rest, size_t cap, size_t *nrest)
{
    return raw_frame(fd, NULL, rest, cap, nrest, NULL, NULL);
}

static inline bool closed(int fd)
{
    uint8_t b[64];
    size_t n;
    return raw_reply(fd, b, sizeof b, &n) == INT32_MIN;
}

static inline int32_t raw_hello(int fd)
{
    uint32_t v = TSDB_PROTOCOL_VERSION;
    raw_send(fd, 1, TSDB_OP_HELLO, &v, 4);
    uint8_t b[256];
    size_t n;
    int32_t st = raw_reply(fd, b, sizeof b, &n);
    if (st == 0) { /* u32 version, u16 flags: login is required here */
        uint16_t flags;
        memcpy(&flags, b + 4, 2);
        CHECK(n >= 8 && (flags & TSDB_HELLO_AUTH));
    }
    return st;
}

/* AUTH_START with mechanism mech and the client-first-message cf; the server-first-message to sf. */
static inline int32_t raw_start_mech(int fd, const char *mech, const char *cf, char *sf, size_t cap)
{
    uint8_t b[1024];
    size_t ml = strlen(mech), cl = strlen(cf);
    uint16_t l = (uint16_t)cl;
    b[0] = (uint8_t)ml;
    memcpy(b + 1, mech, ml);
    memcpy(b + 1 + ml, &l, 2);
    memcpy(b + 3 + ml, cf, cl);
    raw_send(fd, 2, TSDB_OP_AUTH_START, b, (uint32_t)(3 + ml + cl));
    uint8_t r[600];
    size_t n = 0;
    int32_t st = raw_reply(fd, r, sizeof r, &n);
    if (sf)
        sf[0] = 0;
    if (st == 0) {
        uint16_t sl;
        memcpy(&sl, r, 2);
        CHECK(n == 2u + sl && sl < cap);
        if (sf && n == 2u + sl && sl < cap) {
            memcpy(sf, r + 2, sl);
            sf[sl] = 0;
        }
    } else {
        CHECK(n == 0); /* a failure has no body */
    }
    return st;
}

static inline int32_t raw_start(int fd, const char *cf, char *sf, size_t cap)
{
    return raw_start_mech(fd, TSDB_SCRAM_SHA256, cf, sf, cap);
}

static inline void raw_continue_send(int fd, const char *final)
{
    uint8_t b[1024];
    size_t fl = strlen(final);
    uint16_t l = (uint16_t)fl;
    memcpy(b, &l, 2);
    memcpy(b + 2, final, fl);
    raw_send(fd, 3, TSDB_OP_AUTH_CONTINUE, b, (uint32_t)(2 + fl));
}

/* The client's side of SCRAM from the server-first-message: the client-final-message and the signature the
 * server must answer with. */
static inline void client_final(const char *user, const char *pw, const char *cnonce, const char *sf, char *out,
                                size_t cap, char *want_v, size_t vcap)
{
    const char *s = strstr(sf, ",s="), *i = strstr(sf, ",i=");
    CHECK(s && i && strncmp(sf, "r=", 2) == 0);
    if (!s || !i) {
        out[0] = 0;
        return;
    }
    uint8_t salt[SALT_MAX];
    size_t sl = 0;
    CHECK(tsdb_base64_decode(s + 3, (size_t)(i - s - 3), salt, sizeof salt, &sl) == 0);
    uint32_t iter = (uint32_t)strtoul(i + 3, NULL, 10);
    uint8_t ck[32], sk[32], svk[32], sig[32], proof[32], ssig[32];
    tsdb_scram_keys(pw, strlen(pw), salt, sl, iter, ck, sk, svk);
    char am[1400], wo[512];
    snprintf(wo, sizeof wo, "c=biws,r=%.*s", (int)(s - sf - 2), sf + 2);
    snprintf(am, sizeof am, "n=%s,r=%s,%s,%s", user, cnonce, sf, wo);
    tsdb_hmac_sha256(sk, 32, am, strlen(am), sig);
    for (int k = 0; k < 32; k++)
        proof[k] = ck[k] ^ sig[k];
    char pb[tsdb_base64_len(32) + 1], sb[tsdb_base64_len(32) + 1];
    tsdb_base64_encode(proof, 32, pb);
    snprintf(out, cap, "%s,p=%s", wo, pb);
    tsdb_hmac_sha256(svk, 32, am, strlen(am), ssig);
    tsdb_base64_encode(ssig, 32, sb);
    snprintf(want_v, vcap, "v=%s", sb);
}

/* A whole login over raw frames: 0, or the failing status. */
static inline int32_t raw_login(int fd, const char *user, const char *pw)
{
    int32_t st = raw_hello(fd);
    char cf[300], sf[600], fin[700], v[64];
    snprintf(cf, sizeof cf, "n,,n=%s,r=testnonce", user);
    if (st != 0 || (st = raw_start(fd, cf, sf, sizeof sf)) != 0)
        return st;
    client_final(user, pw, "testnonce", sf, fin, sizeof fin, v, sizeof v);
    raw_continue_send(fd, fin);
    uint8_t r[256];
    size_t n = 0;
    if ((st = raw_reply(fd, r, sizeof r, &n)) == 0) {
        uint16_t l;
        memcpy(&l, r, 2);
        CHECK(n == 2u + l && l == strlen(v) && memcmp(r + 2, v, l) == 0); /* the server proved itself */
    }
    return st;
}

#endif
