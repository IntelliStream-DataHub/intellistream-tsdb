/* Integration test: starts build/tsdbd and talks to it through the client library. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_pack.h"
#include "tsdb_protocol.h"
#include "metrics.h"
#include "samekeys.h"
#include "lttb_ref.h"
#include "harness.h"

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <zstd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                     \
    do {                                                                                             \
        int rc_ = (expr);                                                                            \
        if (rc_ != 0) {                                                                              \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdbc_strerror(rc_)); \
            exit(1);                                                                                 \
        }                                                                                            \
    } while (0)

static int port; /* where the server started last listens: tsdbd chose it, see start_server_with */
static const int64_t T0 = 1699999980000; /* on a minute boundary: buckets below are whole */

/* When set, the next server runs under the crashfs shim in eio mode: SIGUSR2
 * makes its next write or sync of a WAL segment fail, and the shim writes the
 * failed file's path to this file. */
static const char *eio_mark;

/* extra: more arguments, NULL-terminated, or NULL */
static pid_t start_server_with(const char *dir, const char *const *extra)
{
    char pf[PATH_MAX + 64], shim[PATH_MAX], near[PATH_MAX];
    th_portfile(pf, sizeof pf, dir);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd"; /* e.g. a sanitizer build */
    /* the shim of the same build, next to the server, or where TSDB_CRASHFS says (the dist tarball's tests/) */
    if (getenv("TSDB_CRASHFS"))
        snprintf(near, sizeof near, "%s", getenv("TSDB_CRASHFS"));
    else
        snprintf(near, sizeof near, "%.*slibcrashfs.so", (int)(strrchr(bin, '/') ? strrchr(bin, '/') - bin + 1 : 0), bin);
    if (eio_mark && !realpath(near, shim)) {
        fprintf(stderr, "%s is missing\n", near);
        exit(1);
    }
    pid_t pid = fork();
    if (pid == 0) {
        if (eio_mark) {
            char asan[512]; /* an ASan build of the shim, preloaded, comes before the ASan runtime */
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            setenv("LD_PRELOAD", shim, 1);
            setenv("CRASHFS_ROOT", dir, 1);
            setenv("CRASHFS_MODE", "eio", 1);
            setenv("CRASHFS_EIO_ONLY", "wal_", 1);
            setenv("CRASHFS_MARK", eio_mark, 1);
        }
        const char *argv[32] = {"tsdbd", "--dir", dir, "--port", "0", "--port-file", pf, "--workers", "4", "--no-fsync"};
        int n = eio_mark ? 9 : 10; /* under the shim, syncs are what make data survive its rollback at exit */
        if (getenv("TSDBD_IO")) { /* the event loop backend: make test runs uring and epoll */
            argv[n++] = "--io";
            argv[n++] = getenv("TSDBD_IO");
        }
        for (int i = 0; extra && extra[i] && n < 31; i++)
            argv[n++] = extra[i];
        argv[n] = NULL;
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    int p = th_await(pf, pid, 30000); /* the port and pid of the server started here, nobody else's */
    unlink(pf);
    if (p <= 0) {
        fputs(th_why, stderr);
        if (p != TH_EXITED && pid > 0) {
            kill(pid, SIGKILL);
            waitpid(pid, NULL, 0);
        }
        exit(1);
    }
    port = p;
    return pid;
}

static pid_t start_server(const char *dir) { return start_server_with(dir, NULL); }

/* kill() with pid 0 or -1 signals the process group, or every process of the user. */
static void signal_pid(pid_t pid, int sig)
{
    if (pid > 0)
        kill(pid, sig);
}

static void stop_server(pid_t pid)
{
    signal_pid(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

/* Raw protocol access, for what the client library never sends. */
static int raw_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        perror("connect");
        exit(1);
    }
    return fd;
}

static void raw_send(int fd, uint32_t id, uint8_t op, const void *payload, uint32_t len)
{
    uint8_t h[9];
    uint32_t flen = len + 5;
    memcpy(h, &flen, 4);
    memcpy(h + 4, &id, 4);
    h[8] = op;
    CHECK(write(fd, h, 9) == 9);
    CHECK(len == 0 || write(fd, payload, len) == (ssize_t)len);
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

/* Reads one reply frame; returns its status, or INT32_MIN at the end of the stream.
 * The body after the status goes to rest[cap], its length to *nrest. */
static int32_t raw_reply(int fd, uint32_t *id, uint8_t *rest, size_t cap, size_t *nrest)
{
    uint8_t h[13];
    if (!read_full(fd, h, 13))
        return INT32_MIN;
    uint32_t flen;
    int32_t status;
    memcpy(&flen, h, 4);
    memcpy(id, h + 4, 4);
    memcpy(&status, h + 9, 4);
    size_t n = flen - 9;
    CHECK(h[8] == TSDB_OP_REPLY && n <= cap);
    if (n > cap || !read_full(fd, rest, n))
        return INT32_MIN;
    *nrest = n;
    return status;
}

static void raw_insert(int fd, uint32_t id, uint32_t series, int64_t ts, float v)
{
    uint8_t b[20];
    uint32_t one = 1;
    memcpy(b, &one, 4);
    memcpy(b + 4, &series, 4);
    memcpy(b + 8, &ts, 8);
    memcpy(b + 16, &v, 4);
    raw_send(fd, id, TSDB_OP_INSERT, b, sizeof b);
}

static uint64_t count_of(tsdbc *c, uint32_t series)
{
    tsdb_bucket *b;
    size_t nb;
    uint64_t n = 0;
    if (tsdbc_query_agg(c, series, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0) {
        n = nb ? b[0].count : 0;
        free(b);
    }
    return n;
}

/* Regression: requests a client sent before closing were dropped once the
 * server saw the close, so pipelined inserts without a wait got lost. */
static void test_close(tsdbc *c)
{
    uint32_t d, e;
    OK(tsdbc_series(c, "close.async", 1, &d));
    OK(tsdbc_series(c, "close.halfclose", 1, &e));
    tsdbc *c2;
    OK(tsdbc_connect("127.0.0.1", port, &c2));
    enum { NA = 500 };
    for (int i = 0; i < NA; i++) {
        int64_t t = T0 + i;
        float x = (float)i;
        OK(tsdbc_insert_series_async(c2, d, &t, &x, 1));
    }
    tsdbc_close(c2); /* without tsdbc_wait: close collects the acknowledgements */
    CHECK(count_of(c, d) == NA);

    /* a client that half-closes after sending still gets every request
     * applied and every reply, then the end of the stream */
    int rfd = raw_connect();
    for (int i = 0; i < 50; i++)
        raw_insert(rfd, (uint32_t)i, e, T0 + i, 2.0f);
    shutdown(rfd, SHUT_WR);
    int replies = 0;
    uint32_t rid;
    uint8_t rest[64];
    size_t nrest;
    for (int32_t st; (st = raw_reply(rfd, &rid, rest, sizeof rest, &nrest)) != INT32_MIN;) {
        CHECK(st == 0 && rid == (uint32_t)replies);
        replies++;
    }
    CHECK(replies == 50);
    close(rfd);
    CHECK(count_of(c, e) == 50);
}

/* Regressions in request and reply framing. */
/* HELLO answers the version both sides speak: an older client (whose tests compare it with
 * its own constant) hears its own version, a newer one the server's. */
static void test_hello(void)
{
    int fd = raw_connect();
    const uint32_t asks[] = {4, 5, TSDB_PROTOCOL_VERSION, TSDB_PROTOCOL_VERSION + 7, 0};
    const uint32_t want[] = {4, 5, TSDB_PROTOCOL_VERSION, TSDB_PROTOCOL_VERSION, TSDB_PROTOCOL_VERSION};
    for (uint32_t i = 0; i < 6; i++) {
        raw_send(fd, 70 + i, TSDB_OP_HELLO, i < 5 ? &asks[i] : NULL, i < 5 ? 4 : 0); /* the last: no version */
        uint32_t rid, v = 0;
        uint8_t rest[16];
        size_t nrest = 0;
        CHECK(raw_reply(fd, &rid, rest, sizeof rest, &nrest) == 0 && rid == 70 + i && nrest == 4);
        memcpy(&v, rest, 4);
        CHECK(v == (i < 5 ? want[i] : TSDB_PROTOCOL_VERSION));
    }
    close(fd);
}

/* A server older than the client: it answers HELLO with fake_version and refuses everything else, counting it.
 * The client must refuse what that version does not know without sending it. */
static uint32_t fake_version;
static int fake_others; /* read after the join */

static void *fake_server(void *arg)
{
    int fd = accept(*(int *)arg, NULL, NULL);
    uint8_t h[9], body[256];
    while (fd >= 0 && read_full(fd, h, 9)) {
        uint32_t flen, id;
        memcpy(&flen, h, 4);
        memcpy(&id, h + 4, 4);
        if (flen < 5 || flen - 5 > sizeof body || !read_full(fd, body, flen - 5))
            break;
        int32_t st = h[8] == TSDB_OP_HELLO ? 0 : TSDB_ERR_INVALID;
        fake_others += h[8] != TSDB_OP_HELLO;
        uint8_t r[17];
        uint32_t rlen = st ? 9 : 13;
        memcpy(r, &rlen, 4);
        memcpy(r + 4, &id, 4);
        r[8] = TSDB_OP_REPLY;
        memcpy(r + 9, &st, 4);
        memcpy(r + 13, &fake_version, 4);
        if (write(fd, r, 4 + rlen) != (ssize_t)(4 + rlen))
            break;
    }
    if (fd >= 0)
        close(fd);
    return NULL;
}

static void test_old_server(uint32_t version)
{
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    socklen_t sl = sizeof sa;
    CHECK(bind(lfd, (struct sockaddr *)&sa, sizeof sa) == 0 && listen(lfd, 1) == 0 &&
          getsockname(lfd, (struct sockaddr *)&sa, &sl) == 0);
    fake_version = version;
    fake_others = 0;
    pthread_t th;
    CHECK(pthread_create(&th, NULL, fake_server, &lfd) == 0);
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", ntohs(sa.sin_port), &c));
    OK(tsdbc_set_compression(c, 0));
    CHECK(tsdbc_server_version(c) == version);
    int64_t lim = 7, t = T0;
    double one = 1;
    tsdb_values col = {TSDB_FLOAT64, &one, NULL};
    if (version < 7) {
        CHECK(tsdbc_delete_range(c, 1, T0, T0 + 10) == TSDB_ERR_INVALID);
        CHECK(tsdbc_replace_range(c, 1, T0, T0 + 10, &t, &col, 1) == TSDB_ERR_INVALID);
        CHECK(tsdbc_set_retention(c, 1, 1000) == TSDB_ERR_INVALID);
        CHECK(tsdbc_get_retention(c, 1, &lim) == TSDB_ERR_INVALID);
    }
    if (version < 9) { /* LTTB's order and limit are version 9; the plain form still goes out */
        tsdb_query_opts desc = {TSDB_DESC, 0}, lim = {TSDB_ASC, 5};
        CHECK(tsdbc_query_lttb_ex(c, 1, T0, T0 + 10, 100, &desc, NULL, NULL) == TSDB_ERR_INVALID);
        CHECK(tsdbc_query_lttb_ex(c, 1, T0, T0 + 10, 100, &lim, NULL, NULL) == TSDB_ERR_INVALID);
    }
    if (version < 6) {
        tsdb_twa_bucket *b = NULL;
        size_t nb = 0;
        CHECK(tsdbc_query_twa(c, 1, T0, T0 + 10, 0, NULL, &b, &nb) == TSDB_ERR_INVALID && nb == 0);
        free(b);
    }
    tsdbc_close(c);
    pthread_join(th, NULL);
    close(lfd);
    CHECK(fake_others == 0); /* only HELLO went out */
}

static void test_framing(void)
{
    int rfd = raw_connect();
    uint32_t rid;
    uint8_t rest[64];
    size_t nrest = 0;

    /* an invalid QUERY_RAW was answered with a bare status, without the `last`
     * and `n` fields every raw reply has, so clients misread it */
    uint8_t q[19] = {0};
    raw_send(rfd, 7, TSDB_OP_QUERY_RAW, q, sizeof q);
    CHECK(raw_reply(rfd, &rid, rest, sizeof rest, &nrest) == TSDB_ERR_INVALID);
    CHECK(rid == 7 && nrest == 5 && rest[0] == 1 && rest[1] == 0 && rest[2] == 0);

    /* a name with a NUL was cut at it, naming another series */
    uint8_t sq[6] = {1, 3, 0, 'a', 0, 'b'};
    raw_send(rfd, 8, TSDB_OP_SERIES, sq, sizeof sq);
    CHECK(raw_reply(rfd, &rid, rest, sizeof rest, &nrest) == TSDB_ERR_INVALID && rid == 8);
    close(rfd);
}

/* Value types over the wire (protocol version 2). */
typedef struct vrows {
    size_t n;
    int64_t ts[64];
    int64_t i[64];
    bool known[64];
    int type;
} vrows;

static int collect_values(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    vrows *r = ctx;
    r->type = col->type;
    for (size_t k = 0; k < n && r->n < 64; k++, r->n++) {
        r->ts[r->n] = ts[k];
        r->known[r->n] = !col->valid || ((col->valid[k >> 3] >> (k & 7)) & 1);
        switch (col->type) {
        case TSDB_FLOAT64: r->i[r->n] = (int64_t)((const double *)col->values)[k]; break;
        case TSDB_INT64: r->i[r->n] = ((const int64_t *)col->values)[k]; break;
        case TSDB_BOOL: r->i[r->n] = ((const uint8_t *)col->values)[k]; break;
        case TSDB_STATE: r->i[r->n] = ((const uint32_t *)col->values)[k]; break;
        default: r->i[r->n] = (int64_t)((const float *)col->values)[k]; break;
        }
    }
    return 0;
}

static void test_types(tsdbc *c)
{
    uint32_t d, i, b, st, again;
    OK(tsdbc_series_create(c, "t.f64", TSDB_FLOAT64, NULL, &d));
    OK(tsdbc_series_create(c, "t.i64", TSDB_INT64, NULL, &i));
    OK(tsdbc_series_create(c, "t.bool", TSDB_BOOL, NULL, &b));
    OK(tsdbc_series_create(c, "t.state", TSDB_STATE, NULL, &st));
    CHECK(tsdbc_series_create(c, "t.f64", TSDB_INT64, NULL, &again) == TSDB_ERR_INVALID);
    tsdb_type ty;
    OK(tsdbc_series_type(c, i, &ty));
    CHECK(ty == TSDB_INT64);

    /* one series, with nulls: rows 3 and 9 unknown */
    int64_t ts[12];
    int64_t iv[12];
    uint8_t valid[2] = {0xff, 0xff};
    for (int k = 0; k < 12; k++) {
        ts[k] = T0 + k * 1000;
        iv[k] = (int64_t)k * 1000000000000 - 5;
    }
    valid[0] &= (uint8_t)~(1u << 3);
    valid[1] &= (uint8_t)~(1u << 1);
    tsdb_values col = {TSDB_INT64, iv, valid};
    OK(tsdbc_insert_series_values(c, i, ts, &col, 12));
    vrows r = {0};
    OK(tsdbc_query_values(c, i, INT64_MIN, INT64_MAX, NULL, collect_values, &r));
    CHECK(r.n == 12 && r.type == TSDB_INT64);
    for (size_t k = 0; k < r.n; k++)
        CHECK(r.ts[k] == ts[k] && r.known[k] == (k != 3 && k != 9) && (!r.known[k] || r.i[k] == iv[k]));
    tsdb_query_opts newest = {TSDB_DESC, 2};
    r.n = 0;
    OK(tsdbc_query_values(c, i, INT64_MIN, INT64_MAX, &newest, collect_values, &r));
    CHECK(r.n == 2 && r.ts[0] == ts[11] && r.ts[1] == ts[10]);
    tsdb_bucket *bk;
    size_t nb;
    OK(tsdbc_query_agg(c, i, INT64_MIN, INT64_MAX, 0, &bk, &nb));
    tsdb_query_stats qs;
    OK(tsdbc_last_query_stats(c, &qs)); /* the server's account of that query */
    CHECK(qs.result_rows == 1 && qs.rows == 12 && qs.memtable_rows + qs.rows_decoded + qs.rows_from_stats == 12 &&
          qs.bytes == 12 * 16 && qs.elapsed_s > 0);
    int64_t want = 0;
    for (int k = 0; k < 12; k++)
        want += k == 3 || k == 9 ? 0 : iv[k];
    CHECK(nb == 1 && bk[0].count == 10 && bk[0].isum == want && bk[0].imin == iv[0] && bk[0].imax == iv[11]);
    free(bk);
    tsdb_group g = {TSDB_SECOND, 4, "UTC"};
    tsdb_query_opts last = {TSDB_DESC, 1};
    OK(tsdbc_query_group(c, i, INT64_MIN, INT64_MAX, &g, &last, &bk, &nb));
    CHECK(nb == 1 && bk[0].start == T0 + 8000 && bk[0].count == 3 && bk[0].isum == iv[8] + iv[10] + iv[11]);
    free(bk);

    /* many series in one frame: float64 rows for d, then bools, states */
    uint32_t sv[4] = {d, d, d, d};
    double dv[4] = {1.5, 2.25, -3.0, 1e300};
    OK(tsdbc_insert_values(c, sv, ts, &(tsdb_values){TSDB_FLOAT64, dv, NULL}, 4));
    OK(tsdbc_query_agg(c, d, INT64_MIN, INT64_MAX, 0, &bk, &nb));
    CHECK(nb == 1 && bk[0].count == 4 && bk[0].min == -3.0 && bk[0].max == 1e300);
    free(bk);
    uint8_t bv[5] = {1, 0, 1, 1, 0};
    uint8_t bvalid = 0x1f & ~0x04;
    OK(tsdbc_insert_series_values(c, b, ts, &(tsdb_values){TSDB_BOOL, bv, &bvalid}, 5));
    r.n = 0;
    OK(tsdbc_query_values(c, b, INT64_MIN, INT64_MAX, NULL, collect_values, &r));
    CHECK(r.n == 5 && r.type == TSDB_BOOL && r.i[0] == 1 && r.i[1] == 0 && !r.known[2] && r.i[3] == 1);
    OK(tsdbc_query_agg(c, b, INT64_MIN, INT64_MAX, 0, &bk, &nb));
    CHECK(nb == 1 && bk[0].count == 4 && bk[0].isum == 2);
    free(bk);
    uint32_t on, off, code;
    OK(tsdbc_state(c, st, "on", &on));
    OK(tsdbc_state(c, st, "off", &off));
    CHECK(tsdbc_state_lookup(c, st, "standby", &code) == TSDB_ERR_NOTFOUND);
    OK(tsdbc_state_lookup(c, st, "off", &code));
    CHECK(code == off && on != off);
    char name[16];
    OK(tsdbc_state_name(c, st, on, name, sizeof name));
    CHECK(strcmp(name, "on") == 0);
    CHECK(tsdbc_state_name(c, st, 999, name, sizeof name) == TSDB_ERR_NOTFOUND);
    uint32_t codes[3] = {on, off, on};
    uint32_t stv[3] = {st, st, st};
    OK(tsdbc_insert_values(c, stv, ts, &(tsdb_values){TSDB_STATE, codes, NULL}, 3));
    r.n = 0;
    OK(tsdbc_query_values(c, st, INT64_MIN, INT64_MAX, NULL, collect_values, &r));
    CHECK(r.n == 3 && r.i[0] == on && r.i[1] == off && r.i[2] == on);

    /* the version 1 operations: float32 only */
    float f = 1;
    CHECK(tsdbc_insert_series(c, d, ts, &f, 1) == TSDB_ERR_INVALID);
    tsdb_points pts;
    CHECK(tsdbc_query_points(c, d, INT64_MIN, INT64_MAX, &pts) == TSDB_ERR_INVALID);
    CHECK(tsdbc_insert_values(c, sv, ts, &(tsdb_values){TSDB_INT64, iv, NULL}, 4) == TSDB_ERR_INVALID);

    /* Regression: INSERT_VALUES ignored its reserved header bytes and flag
     * bits, so they could never be given a meaning */
    int fd = raw_connect();
    for (int bad = 0; bad < 2; bad++) {
        uint8_t f[TSDB_VALUES_HEADER + 8 + 8] = {0};
        uint32_t one = 1;
        memcpy(f, &one, 4);
        memcpy(f + 4, &i, 4);
        f[8] = TSDB_INT64;
        if (bad)
            f[12] = 1; /* a reserved byte */
        else
            f[9] = 0x80; /* an unknown flag */
        memcpy(f + 16, &ts[0], 8);
        memcpy(f + 24, &iv[0], 8);
        raw_send(fd, 40 + (uint32_t)bad, TSDB_OP_INSERT_VALUES, f, sizeof f);
        uint32_t rid0;
        uint8_t rest0[64];
        size_t nrest0;
        CHECK(raw_reply(fd, &rid0, rest0, sizeof rest0, &nrest0) == TSDB_ERR_INVALID && rid0 == 40 + (uint32_t)bad);
    }
    close(fd);

    /* QUERY_AGG still answers with version 1 rows (f32 min and max) */
    fd = raw_connect();
    uint8_t q[28];
    int64_t lo = INT64_MIN, hi = INT64_MAX, every = 0;
    memcpy(q, &d, 4);
    memcpy(q + 4, &lo, 8);
    memcpy(q + 12, &hi, 8);
    memcpy(q + 20, &every, 8);
    raw_send(fd, 5, TSDB_OP_QUERY_AGG, q, sizeof q);
    uint32_t rid;
    uint8_t rest[64];
    size_t nrest = 0;
    CHECK(raw_reply(fd, &rid, rest, sizeof rest, &nrest) == 0);
    float mn, mx;
    memcpy(&mn, rest + 4 + 24, 4);
    memcpy(&mx, rest + 4 + 28, 4);
    CHECK(nrest == 4 + TSDB_AGG_ROW && mn == -3.0f && isinf(mx));
    close(fd);
}

/* NaN results through the server: one quiet NaN 0x7ff8000000000000 (0x7fc00000 in the float32 min and max of
 * version 1 rows), whatever payloads the data had; raw values keep theirs. */
static bool canon64(double d)
{
    uint64_t b;
    memcpy(&b, &d, 8);
    return d == d || b == 0x7ff8000000000000ull;
}

typedef struct nan_rows {
    uint64_t *bits;
    size_t n, cap;
    int type;
} nan_rows;

static int collect_bits(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    nan_rows *r = ctx;
    for (size_t k = 0; k < n && r->n < r->cap; k++, r->n++) {
        bool known = !col->valid || ((col->valid[k >> 3] >> (k & 7)) & 1);
        uint64_t s = 0;
        if (col->type == TSDB_FLOAT64)
            memcpy(&s, (const double *)col->values + k, 8);
        else
            memcpy(&s, (const float *)col->values + k, 4);
        r->bits[r->n] = known ? s : UINT64_MAX;
    }
    return 0;
}

static void test_nan(tsdbc *c)
{
    enum { NN = 6000 };
    static const uint64_t p64[] = {0x7ff8000000000001ull, 0xfff8000000000000ull, 0x7ff0000000000001ull,
                                   0x7ff0000000001001ull, 0x7ff8000000001000ull, 0x7ff8000000001002ull,
                                   0xffffffffffffffffull, 0x7ff8dead00000000ull};
    static const uint32_t p32[] = {0x7fc00001u, 0xffc00000u, 0x7f800001u, 0x7f801001u,
                                   0x7fc01000u, 0x7fc01002u, 0xffffffffu, 0x7fc0deadu};
    uint64_t x = 0x2545f4914f6cdd1dull;
    uint32_t ids[2];
    OK(tsdbc_series_create(c, "nan.f32", TSDB_FLOAT32, NULL, &ids[0]));
    OK(tsdbc_series_create(c, "nan.f64", TSDB_FLOAT64, NULL, &ids[1]));
    static uint64_t ref[2][NN];
    int64_t *ts = malloc(NN * sizeof *ts);
    uint64_t *v = malloc(NN * 8);
    for (int pass = 0; pass < 2; pass++) {
        for (int t = 0; t < 2; t++) {
            /* pass 0 everything in batches, flushed; pass 1 every 4th row again, newer NaNs over older ones */
            size_t n = 0;
            for (size_t k = pass; k < NN; k += pass ? 4 : 1, n++) {
                x ^= x << 13, x ^= x >> 7, x ^= x << 17;
                bool nan = x % 9 == 0 || (k >= 1000 && k < 1100);
                double d = (double)(k % 101) - 20.5;
                uint64_t s;
                if (t) {
                    s = nan ? p64[x / 9 % 8] : 0;
                    if (!nan)
                        memcpy(&s, &d, 8);
                } else {
                    float f = (float)d;
                    uint32_t u = nan ? p32[x / 9 % 8] : 0;
                    if (!nan)
                        memcpy(&u, &f, 4);
                    s = u;
                }
                ts[n] = T0 + 86400000 + (int64_t)k * 1000;
                if (t)
                    v[n] = s;
                else
                    memcpy((uint32_t *)v + n, &(uint32_t){(uint32_t)s}, 4);
                ref[t][k] = s;
            }
            for (size_t o = 0; o < n; o += 700) /* frames of 700: some into the memtable, some into parts */
                OK(tsdbc_insert_series_values(c, ids[t], ts + o,
                                              &(tsdb_values){t ? TSDB_FLOAT64 : TSDB_FLOAT32,
                                                             t ? (void *)(v + o) : (void *)((uint32_t *)v + o), NULL},
                                              n - o < 700 ? n - o : 700));
        }
        if (pass == 0)
            OK(tsdbc_flush(c));
    }
    free(ts);
    free(v);
    const int64_t lo = T0 + 86400000, hi = lo + NN * 1000;
    for (int t = 0; t < 2; t++) {
        tsdb_bucket *bk;
        size_t nb, nan_sums = 0, nan_mins = 0, bad = 0;
        static const int64_t iv[] = {0, 7000, 60000, 3600000};
        for (size_t i = 0; i < 4; i++) {
            OK(tsdbc_query_agg(c, ids[t], lo, hi, iv[i], &bk, &nb));
            for (size_t k = 0; k < nb; k++) {
                nan_sums += isnan(bk[k].sum);
                nan_mins += isnan(bk[k].min);
                bad += !canon64(bk[k].sum) + !canon64(bk[k].min) + !canon64(bk[k].max);
            }
            /* tsdbc_query_agg_ex: newest first, cut to the limit, the same buckets */
            tsdb_bucket *bx;
            size_t nx;
            tsdb_query_opts o = {TSDB_DESC, 3};
            OK(tsdbc_query_agg_ex(c, ids[t], lo, hi, iv[i], &o, &bx, &nx));
            CHECK(nx == (nb < 3 ? nb : 3));
            for (size_t k = 0; k < nx && k < nb; k++)
                CHECK(!memcmp(&bx[k], &bk[nb - 1 - k], sizeof *bx));
            free(bx);
            free(bk);
        }
        OK(tsdbc_query_group(c, ids[t], lo, hi, &(tsdb_group){TSDB_MINUTE, 5, NULL}, NULL, &bk, &nb));
        for (size_t k = 0; k < nb; k++)
            bad += !canon64(bk[k].sum) + !canon64(bk[k].min) + !canon64(bk[k].max);
        free(bk);
        int64_t bounds[] = {lo, lo + 999 * 1000, lo + 1000 * 1000, lo + 1100 * 1000, lo + 4321 * 1000};
        OK(tsdbc_query_bounds(c, ids[t], lo, hi, bounds, 5, NULL, &bk, &nb));
        CHECK(nb == 5 && bk[2].count == 100 && isnan(bk[2].min) && isnan(bk[2].max) && isnan(bk[2].sum));
        for (size_t k = 0; k < nb; k++)
            bad += !canon64(bk[k].sum) + !canon64(bk[k].min) + !canon64(bk[k].max);
        free(bk);
        if (bad)
            fprintf(stderr, "  %s: %zu NaN results are not 0x7ff8000000000000\n", t ? "float64" : "float32", bad);
        CHECK(bad == 0 && nan_sums > 10 && nan_mins >= 2);

        /* version 1 rows (QUERY_AGG): double sum, float32 min and max */
        int fd = raw_connect();
        uint8_t q[28];
        int64_t a = lo + 1000 * 1000, z = lo + 1100 * 1000, every = 0;
        memcpy(q, &ids[t], 4);
        memcpy(q + 4, &a, 8);
        memcpy(q + 12, &z, 8);
        memcpy(q + 20, &every, 8);
        raw_send(fd, 7, TSDB_OP_QUERY_AGG, q, sizeof q);
        uint32_t rid;
        uint8_t rest[64];
        size_t nrest = 0;
        CHECK(raw_reply(fd, &rid, rest, sizeof rest, &nrest) == 0 && nrest == 4 + TSDB_AGG_ROW);
        uint64_t sb;
        uint32_t mnb, mxb;
        memcpy(&sb, rest + 4 + 16, 8);
        memcpy(&mnb, rest + 4 + 24, 4);
        memcpy(&mxb, rest + 4 + 28, 4);
        CHECK(sb == 0x7ff8000000000000ull && mnb == 0x7fc00000u && mxb == 0x7fc00000u);
        close(fd);

        /* raw: the payloads as written */
        nan_rows r = {malloc(NN * 8), 0, NN, t};
        OK(tsdbc_query_values(c, ids[t], lo, hi, NULL, collect_bits, &r));
        bool same = r.n == NN;
        for (size_t k = 0; same && k < NN; k++)
            same = r.bits[k] == ref[t][k];
        CHECK(same);
        free(r.bits);
    }
}

typedef struct counter {
    size_t rows, calls, stop_after;
    int64_t last;
    bool ordered;
} counter;

static int count_rows(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    counter *k = ctx;
    (void)v;
    for (size_t i = 0; i < n; i++) {
        if (k->rows && ts[i] <= k->last)
            k->ordered = false;
        k->last = ts[i];
        k->rows++;
    }
    return ++k->calls == k->stop_after;
}

typedef struct cjob {
    uint32_t id;
    int errors;
} cjob;

static void *client_thread(void *arg)
{
    cjob *j = arg;
    tsdbc *c;
    if (tsdbc_connect("127.0.0.1", port, &c)) {
        j->errors++;
        return NULL;
    }
    int64_t ts[1000];
    float v[1000];
    for (int round = 0; round < 50; round++) {
        for (int i = 0; i < 1000; i++) {
            ts[i] = T0 + (int64_t)(round * 1000 + i) * 1000;
            v[i] = (float)j->id;
        }
        if (tsdbc_insert_series_async(c, j->id, ts, v, 1000))
            j->errors++;
    }
    if (tsdbc_wait(c))
        j->errors++;
    tsdb_bucket *b;
    size_t nb;
    if (tsdbc_query_agg(c, j->id, INT64_MIN, INT64_MAX, 0, &b, &nb) || nb != 1 || b[0].count != 50000 ||
        b[0].min != (float)j->id || b[0].max != (float)j->id)
        j->errors++;
    else
        free(b);
    tsdbc_close(c);
    return NULL;
}

/* LTTB through the server: the points chosen against the reference over QUERY_VALUES */
typedef struct lpts {
    pt *p;
    size_t n, cap;
    bool had_valid;
} lpts;

static int lttb_collect(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    lpts *o = ctx;
    o->had_valid |= col->valid != NULL;
    for (size_t i = 0; i < n; i++) {
        uint64_t bits = 0;
        double y = 0;
        if (!col_point(col, i, &bits, &y))
            continue;
        if (o->n == o->cap) {
            o->cap = o->cap ? o->cap * 2 : 4096;
            o->p = realloc(o->p, o->cap * sizeof *o->p);
        }
        o->p[o->n++] = (pt){ts[i], bits, y};
    }
    return 0;
}

static int lttb_stop(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n) { return 1; }

static void test_lttb_remote(tsdbc *c)
{
    uint32_t f, bo, st;
    OK(tsdbc_series_create(c, "lttb.f64", TSDB_FLOAT64, NULL, &f));
    OK(tsdbc_series_create(c, "lttb.bool", TSDB_BOOL, NULL, &bo));
    OK(tsdbc_series_create(c, "lttb.state", TSDB_STATE, NULL, &st));
    enum { N = 150000 };
    int64_t *ts = malloc(N * sizeof *ts);
    double *dv = malloc(N * sizeof *dv);
    uint8_t *bv = malloc(N), *valid = malloc((N + 7) / 8);
    memset(valid, 0xff, (N + 7) / 8);
    for (int i = 0; i < N; i++) {
        ts[i] = T0 + (int64_t)i * 250 + i % 7;
        dv[i] = i % 1013 == 0 ? NAN : sin(i / 300.0) * 50 + (i % 4999 == 0 ? 400 : 0);
        bv[i] = sin(i / 77.0) > 0.3;
        if (i % 97 == 0)
            valid[i >> 3] &= (uint8_t)~(1u << (i & 7));
    }
    OK(tsdbc_insert_series_values(c, f, ts, &(tsdb_values){TSDB_FLOAT64, dv, valid}, N));
    OK(tsdbc_insert_series_values(c, bo, ts, &(tsdb_values){TSDB_BOOL, bv, valid}, N));
    size_t ths[] = {3, 500, 40000, N + 10};
    uint32_t ser[] = {f, bo};
    for (size_t s = 0; s < 2; s++)
        for (size_t k = 0; k < sizeof ths / sizeof *ths; k++) {
            lpts raw = {0}, got = {0};
            OK(tsdbc_query_values(c, ser[s], INT64_MIN, INT64_MAX, NULL, lttb_collect, &raw));
            OK(tsdbc_query_lttb(c, ser[s], INT64_MIN, INT64_MAX, ths[k], lttb_collect, &got));
            tsdb_query_stats qs;
            OK(tsdbc_last_query_stats(c, &qs));
            size_t *idx = malloc(raw.n * sizeof *idx);
            size_t m = ref_lttb(raw.p, raw.n, ths[k], idx);
            bool same = got.n == m && !got.had_valid && qs.result_rows == m && m == (raw.n < ths[k] ? raw.n : ths[k]);
            for (size_t i = 0; same && i < m; i++)
                same = got.p[i].ts == raw.p[idx[i]].ts && got.p[i].bits == raw.p[idx[i]].bits;
            CHECK(same);
            free(idx);
            free(raw.p);
            free(got.p);
        }
    lpts none = {0};
    CHECK(tsdbc_server_version(c) == TSDB_PROTOCOL_VERSION); /* QUERY_LTTB needs 5 */
    CHECK(tsdbc_query_lttb(c, f, INT64_MIN, INT64_MAX, 2, lttb_collect, &none) == TSDB_ERR_INVALID);
    CHECK(tsdbc_query_lttb(c, st, INT64_MIN, INT64_MAX, 10, lttb_collect, &none) == TSDB_ERR_INVALID);
    CHECK(tsdbc_query_lttb(c, f, INT64_MIN, INT64_MAX, 40000, lttb_stop, NULL) == TSDB_ERR_STOPPED);
    OK(tsdbc_query_lttb(c, f, T0, T0 + 1000000, 10, lttb_collect, &none)); /* the connection goes on */
    CHECK(none.n == 10);
    free(none.p);

    /* version 9: newest first and limits, the same points */
    for (size_t k = 0; k < sizeof ths / sizeof *ths; k++) {
        lpts asc = {0}, d = {0}, dl = {0}, al = {0};
        tsdb_query_opts desc = {TSDB_DESC, 0}, dlim = {TSDB_DESC, 7}, alim = {TSDB_ASC, 5};
        OK(tsdbc_query_lttb_ex(c, f, INT64_MIN, INT64_MAX, ths[k], NULL, lttb_collect, &asc));
        OK(tsdbc_query_lttb_ex(c, f, INT64_MIN, INT64_MAX, ths[k], &desc, lttb_collect, &d));
        OK(tsdbc_query_lttb_ex(c, f, INT64_MIN, INT64_MAX, ths[k], &dlim, lttb_collect, &dl));
        OK(tsdbc_query_lttb_ex(c, f, INT64_MIN, INT64_MAX, ths[k], &alim, lttb_collect, &al));
        bool same = asc.n > 0 && d.n == asc.n && dl.n == (asc.n < 7 ? asc.n : 7) && al.n == (asc.n < 5 ? asc.n : 5);
        for (size_t i = 0; same && i < d.n; i++)
            same = d.p[i].ts == asc.p[asc.n - 1 - i].ts && d.p[i].bits == asc.p[asc.n - 1 - i].bits &&
                   (i >= dl.n || (dl.p[i].ts == d.p[i].ts && dl.p[i].bits == d.p[i].bits)) &&
                   (i >= al.n || (al.p[i].ts == asc.p[i].ts && al.p[i].bits == asc.p[i].bits));
        CHECK(same);
        free(asc.p), free(d.p), free(dl.p), free(al.p);
    }

    /* the request is exactly 28 bytes, or 37 with order and limit: other lengths are refused */
    int fd = raw_connect();
    uint8_t q[38] = {0};
    int64_t lo = INT64_MIN, hi = INT64_MAX;
    uint64_t t = 10;
    memcpy(q, &f, 4);
    memcpy(q + 4, &lo, 8);
    memcpy(q + 12, &hi, 8);
    memcpy(q + 20, &t, 8);
    for (uint32_t len = 27; len <= 38; len += len == 29 ? 7 : 2) {
        raw_send(fd, 40 + len, TSDB_OP_QUERY_LTTB, q, len);
        uint32_t rid;
        uint8_t rest[256];
        size_t nrest;
        int32_t st = raw_reply(fd, &rid, rest, sizeof rest, &nrest);
        CHECK(rid == 40 + len && st == TSDB_ERR_INVALID);
    }
    close(fd);
    free(ts);
    free(dv);
    free(bv);
    free(valid);
}

/* Deletes, replaces and retention over the wire (protocol version 7). */
static void test_delete(tsdbc *c)
{
    uint32_t s;
    OK(tsdbc_series_create(c, "del.i64", TSDB_INT64, NULL, &s));
    int64_t ts[100], v[100];
    for (int k = 0; k < 100; k++)
        ts[k] = T0 + k * 1000, v[k] = k;
    tsdb_values col = {TSDB_INT64, v, NULL};
    OK(tsdbc_insert_series_values(c, s, ts, &col, 100));
    OK(tsdbc_delete_range(c, s, T0 + 10000, T0 + 20000));
    tsdb_bucket *bk;
    size_t nb;
    OK(tsdbc_query_agg(c, s, INT64_MIN, INT64_MAX, 0, &bk, &nb));
    CHECK(nb == 1 && bk[0].count == 90 && bk[0].isum == 4950 - 145);
    free(bk);

    /* replace with nulls; a row outside the range refuses the whole replace */
    int64_t rts[3] = {T0 + 50000, T0 + 50500, T0 + 51000}, rv[3] = {7, 8, 9};
    uint8_t valid = 0x5; /* row 1 is null */
    tsdb_values rcol = {TSDB_INT64, rv, &valid};
    CHECK(tsdbc_replace_range(c, s, T0 + 50000, T0 + 51000, rts, &rcol, 3) == TSDB_ERR_INVALID);
    OK(tsdbc_replace_range(c, s, T0 + 50000, T0 + 60000, rts, &rcol, 3));
    vrows r = {0};
    OK(tsdbc_query_values(c, s, T0 + 50000, T0 + 60000, NULL, collect_values, &r));
    CHECK(r.n == 3 && r.ts[1] == T0 + 50500 && !r.known[1] && r.known[2] && r.i[2] == 9);
    OK(tsdbc_query_agg(c, s, INT64_MIN, INT64_MAX, 0, &bk, &nb));
    CHECK(nb == 1 && bk[0].count == 82);
    free(bk);
    OK(tsdbc_replace_range(c, s, T0 + 50000, T0 + 60000, NULL, &rcol, 0)); /* n = 0: a delete */
    OK(tsdbc_query_agg(c, s, T0 + 50000, T0 + 60000, 0, &bk, &nb));
    CHECK(nb == 0);
    free(bk);

    int64_t lim = -1;
    OK(tsdbc_get_retention(c, s, &lim));
    CHECK(lim == 0);
    OK(tsdbc_set_retention(c, s, 86400000));
    OK(tsdbc_get_retention(c, s, &lim));
    CHECK(lim == 86400000);
    CHECK(tsdbc_set_retention(c, s, -1) == TSDB_ERR_INVALID);
    OK(tsdbc_set_retention(c, s, 0));
}

/* The packed insert body round trips for every type and layout, and anything
 * that does not hold exactly n rows is refused. */
static void test_pack_roundtrip(void)
{
    static const size_t sizes[] = {0, 1, 7, 8, 9, 1000, 65537};
    uint64_t r = 0x9e3779b97f4a7c15ull;
    for (unsigned type = TSDB_FLOAT32; type <= TSDB_STATE; type++)
        for (size_t si = 0; si < sizeof sizes / sizeof *sizes; si++)
            for (int flags = 0; flags < 4; flags++) {
                size_t n = sizes[si];
                int valid = flags & 1, many = flags & 2;
                unsigned w = tsdb_wire_width(type);
                int64_t *ts = malloc((n + 1) * 8), *ts2 = malloc((n + 1) * 8);
                uint8_t *v = malloc(n * w + 1), *v2 = malloc(n * w + 1);
                uint8_t *bits = calloc(n / 8 + 1, 1), *bits2 = calloc(n / 8 + 1, 1);
                uint32_t *sv = malloc((n + 1) * 4), *sv2 = malloc((n + 1) * 4);
                for (size_t i = 0; i < n; i++) {
                    r = r * 6364136223846793005ull + 1442695040888963407ull;
                    /* regular clocks, jumps, and the extremes (deltas wrap) */
                    ts[i] = i % 97 == 0 ? (int64_t)r : i % 89 == 0 ? INT64_MIN : T0 + (int64_t)i * 1000;
                    for (unsigned b = 0; b < w; b++)
                        v[i * w + b] = (uint8_t)(r >> (8 * b));
                    if (type == TSDB_BOOL)
                        v[i] &= 1;
                    sv[i] = i % 5 == 0 ? UINT32_MAX - (uint32_t)i : (uint32_t)(r >> 40);
                    if (valid && (r >> 13) & 1)
                        bits[i / 8] |= (uint8_t)(1u << (i % 8));
                }
                uint8_t *raw = malloc(tsdb_pack_raw_bound(n, type, valid, many) + 1);
                size_t len = tsdb_pack_reshape(n, type, ts, v, valid ? bits : NULL, many ? sv : NULL, raw);
                CHECK(len <= tsdb_pack_raw_bound(n, type, valid, many));
                int rc = tsdb_pack_unshape(raw, len, n, type, valid, many, ts2, v2, bits2, sv2);
                bool same = rc == 0 && (!n || (memcmp(ts, ts2, n * 8) == 0 && memcmp(v, v2, n * w) == 0)) &&
                            (!valid || memcmp(bits, bits2, (n + 7) / 8) == 0) && (!many || !n || memcmp(sv, sv2, n * 4) == 0);
                if (!same)
                    fprintf(stderr, "  pack round trip failed: type %u, n %zu, valid %d, many %d\n", type, n, valid, many);
                CHECK(same);
                if (len) { /* one byte short, or one byte too many */
                    CHECK(tsdb_pack_unshape(raw, len - 1, n, type, valid, many, ts2, v2, bits2, sv2) != 0);
                    raw[len] = 0;
                    CHECK(tsdb_pack_unshape(raw, len + 1, n, type, valid, many, ts2, v2, bits2, sv2) != 0);
                }
                free(ts), free(ts2), free(v), free(v2), free(bits), free(bits2), free(sv), free(sv2), free(raw);
            }
}

/* Inserts arrive packed (the default) or plain, with the same result; a
 * packed frame that does not decode is refused and the server carries on. */
/* Regression: the first packed insert asked the server's version through a
 * call that first collected the pipelined acknowledgements; an error among
 * them was taken for HELLO's, lost, and the connection never packed again. */
static void test_hello_keeps_errors(uint32_t f64)
{
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    OK(tsdbc_set_compression(c, 0));
    int64_t t = T0;
    float bad = 1;
    OK(tsdbc_insert_async(c, &f64, &t, &bad, 1)); /* float32 rows for a float64 series */
    OK(tsdbc_set_compression(c, 1));
    enum { N = 10000 };
    int64_t *ts = malloc(N * 8);
    double *v = malloc(N * 8);
    for (int i = 0; i < N; i++) {
        ts[i] = T0 + i * 1000;
        v[i] = 20 + (i % 10) / 10.0;
    }
    tsdb_values col = {TSDB_FLOAT64, v, NULL};
    uint32_t *ids = malloc(N * 4);
    for (int i = 0; i < N; i++)
        ids[i] = f64;
    OK(tsdbc_insert_values_async(c, ids, ts, &col, N));
    CHECK(tsdbc_wait(c) != 0); /* the float32 insert's error, not lost */
    uint64_t before = tsdbc_bytes_sent(c);
    OK(tsdbc_insert_values(c, ids, ts, &col, N));
    CHECK(tsdbc_bytes_sent(c) - before < N * 4); /* packed: plain would be 20 bytes a row */
    free(ids);
    free(ts);
    free(v);
    tsdbc_close(c);
}

/* Version 4: the server knows UTC only; clients send the bounds of local periods. */
/* QUERY_TWA: the interval, calendar, local and bounds forms, against hand-computed values */
/* QUERY_BUCKETS with a limit stops early in the server as well, for the interval form (unit 0, which
 * tsdbc_query_agg_ex sends) as for calendar groups: 60 days at one row a minute, a granule a day, and the last (or
 * first) 3 hours touch one or two granules, not 60. */
static void test_limits_remote(tsdbc *c)
{
    enum { DAYS = 60, N = DAYS * 1440 };
    uint32_t s;
    OK(tsdbc_series_create(c, "limits.year", TSDB_FLOAT64, NULL, &s));
    const int64_t t0 = 1700000000000LL / 86400000 * 86400000, t1 = t0 + (int64_t)N * 60000;
    int64_t *ts = malloc(N * sizeof *ts);
    double *v = malloc(N * sizeof *v);
    for (int i = 0; i < N; i++)
        ts[i] = t0 + (int64_t)i * 60000, v[i] = i % 977;
    for (int o = 0; o < N; o += 1440)
        OK(tsdbc_insert_series_values(c, s, ts + o, &(tsdb_values){TSDB_FLOAT64, v + o, NULL}, 1440));
    OK(tsdbc_flush(c));
    free(ts);
    free(v);
    tsdb_bucket *all, *b;
    size_t nall, n;
    tsdb_query_stats st;
    OK(tsdbc_query_agg(c, s, t0, t1, 3600000, &all, &nall));
    OK(tsdbc_last_query_stats(c, &st));
    CHECK(nall == DAYS * 24 && st.granules >= DAYS);
    for (int k = 0; k < 4; k++) {
        tsdb_query_opts q = {k & 1 ? TSDB_DESC : TSDB_ASC, 3};
        if (k < 2)
            OK(tsdbc_query_agg_ex(c, s, t0, t1, 3600000, &q, &b, &n));
        else
            OK(tsdbc_query_group(c, s, t0, t1, &(tsdb_group){TSDB_HOUR, 1, NULL}, &q, &b, &n));
        OK(tsdbc_last_query_stats(c, &st));
        if (st.granules > 2 || st.rows_decoded > 2 * 1440)
            fprintf(stderr, "  limits %s %s: %llu granules, %llu rows decoded for 3 buckets\n",
                    k < 2 ? "agg_ex" : "group", k & 1 ? "desc" : "asc", (unsigned long long)st.granules,
                    (unsigned long long)st.rows_decoded);
        CHECK(st.granules <= 2 && st.rows_decoded <= 2 * 1440);
        CHECK(n == 3);
        for (size_t i = 0; i < n && n == 3; i++)
            CHECK(!memcmp(&b[i], &all[k & 1 ? nall - 1 - i : i], sizeof *b));
        free(b);
    }
    free(all);
}

static void test_twa(tsdbc *c)
{
    uint32_t s, st;
    OK(tsdbc_series_create(c, "twa.level", TSDB_FLOAT64, NULL, &s));
    int64_t t0 = 1700000000000LL / 86400000 * 86400000; /* a UTC midnight */
    int64_t ts[] = {t0, t0 + 1000, t0 + 2000, t0 + 3000, t0 + 10000};
    double v[] = {10, 20, 0, 30, 40};
    uint8_t valid = 0xff & ~(1u << 2);
    OK(tsdbc_insert_series_values(c, s, ts, &(tsdb_values){TSDB_FLOAT64, v, &valid}, 5));
    OK(tsdbc_wait(c));
    tsdb_twa_bucket *b;
    size_t n;
    OK(tsdbc_query_twa(c, s, t0, t0 + 20000, 0, NULL, &b, &n));
    CHECK(n == 1 && b[0].start == t0 && b[0].covered_ms == 9000 && b[0].count == 4 && b[0].area == 240000);
    free(b);
    tsdb_twa_opts lin = {.method = TSDB_TWA_LINEAR};
    OK(tsdbc_query_twa(c, s, t0, t0 + 20000, 4000, &lin, &b, &n));
    /* [t0, t0 + 4000): 10 -> 20, then 30 -> 40 cut at t0 + 4000 */
    CHECK(n == 3 && b[0].covered_ms == 2000 && fabs(b[0].area - (15000 + (60 + 10.0 / 7) / 2 * 1000)) < 1e-6 &&
          b[1].covered_ms == 4000);
    free(b);
    /* the staleness limit reaches the server: the 7 s from t0 + 3000 to t0 + 10000 no longer count */
    OK(tsdbc_query_twa(c, s, t0, t0 + 20000, 0, &(tsdb_twa_opts){.max_gap_ms = 5000}, &b, &n));
    CHECK(n == 1 && b[0].covered_ms == 2000 && b[0].count == 4 && b[0].area == 30000);
    free(b);
    tsdb_twa_opts stale = {.max_gap_ms = 5000, .order = TSDB_DESC, .limit = 1};
    OK(tsdbc_query_twa(c, s, t0, t0 + 20000, 1000, &stale, &b, &n));
    CHECK(n == 1 && b[0].start == t0 + 10000 && b[0].covered_ms == 0 && b[0].count == 1 && isnan(b[0].twa));
    free(b);
    tsdb_group day = {TSDB_DAY, 1, NULL};
    OK(tsdbc_query_twa_group(c, s, t0 - 5000, t0 + 20000, &day, NULL, &b, &n));
    CHECK(n == 1 && b[0].start == t0 && b[0].covered_ms == 9000);
    free(b);
    tsdb_group oslo = {TSDB_DAY, 1, "Europe/Oslo"}; /* a local day begins at 23:00 UTC in November */
    OK(tsdbc_query_twa_group(c, s, t0, t0 + 20000, &oslo, NULL, &b, &n));
    CHECK(n == 1 && b[0].start == t0 - 3600000 && b[0].covered_ms == 9000);
    free(b);
    int64_t bounds[] = {t0 + 500, t0 + 2500};
    OK(tsdbc_query_twa_bounds(c, s, t0, t0 + 20000, bounds, 2, NULL, &b, &n));
    CHECK(n == 2 && b[0].start == t0 + 500 && b[0].covered_ms == 1500 && b[1].covered_ms == 7000 &&
          b[1].area == 7000 * 30.0);
    free(b);
    OK(tsdbc_series_create(c, "twa.state", TSDB_STATE, NULL, &st));
    CHECK(tsdbc_query_twa(c, st, t0, t0 + 1, 0, NULL, &b, &n) == TSDB_ERR_INVALID);
    CHECK(tsdbc_query_twa(c, s, t0, t0 + 1, 0, &(tsdb_twa_opts){.method = 9}, &b, &n) == TSDB_ERR_INVALID);
    CHECK(tsdbc_query_twa_bounds(c, s, t0, t0 + 1, NULL, 0, NULL, &b, &n) == TSDB_ERR_INVALID);
    /* the connection still works after the errors */
    OK(tsdbc_query_twa(c, s, t0, t0 + 20000, 0, NULL, &b, &n));
    CHECK(n == 1);
    free(b);
}

static void test_bounds(tsdbc *c, uint32_t a, size_t n)
{
    tsdb_bucket *bk, *bb;
    size_t nb, nbb;
    tsdb_group day = {TSDB_DAY, 1, "Europe/Oslo"};
    OK(tsdbc_query_group(c, a, INT64_MIN, INT64_MAX, &day, NULL, &bk, &nb));
    CHECK(nb >= 2);
    uint64_t rows = 0;
    bool midnight = true;
    for (size_t k = 0; k < nb; k++) {
        rows += bk[k].count;
        midnight &= bk[k].start % 86400000 == 23 * 3600000; /* Oslo is UTC+1 in November */
    }
    CHECK(rows == n && midnight);

    /* the same bounds sent by hand */
    int64_t *bounds = malloc(nb * 8);
    for (size_t k = 0; k < nb; k++)
        bounds[k] = bk[k].start;
    OK(tsdbc_query_bounds(c, a, INT64_MIN, INT64_MAX, bounds, nb, NULL, &bb, &nbb));
    CHECK(nbb == nb && memcmp(bb, bk, nb * sizeof *bk) == 0);
    free(bb);
    tsdb_query_opts last = {TSDB_DESC, 1};
    OK(tsdbc_query_bounds(c, a, INT64_MIN, INT64_MAX, bounds, nb, &last, &bb, &nbb));
    CHECK(nbb == 1 && bb[0].start == bk[nb - 1].start && bb[0].count == bk[nb - 1].count);
    free(bb);
    if (nb >= 2) {
        int64_t swapped[2] = {bounds[1], bounds[0]};
        CHECK(tsdbc_query_bounds(c, a, INT64_MIN, INT64_MAX, swapped, 2, NULL, &bb, &nbb) == TSDB_ERR_INVALID);
    }
    CHECK(tsdbc_query_bounds(c, a, INT64_MIN, INT64_MAX, bounds, 0, NULL, &bb, &nbb) == TSDB_ERR_INVALID);
    free(bounds);
    free(bk);

    /* a time zone sent to the server itself is refused; UTC is fine */
    int fd = raw_connect();
    for (int utc = 0; utc < 2; utc++) {
        const char *zone = utc ? "UTC" : "Europe/Oslo";
        uint8_t q[64] = {0};
        int64_t lo = INT64_MIN, hi = INT64_MAX;
        uint32_t count = 1;
        uint16_t zl = (uint16_t)strlen(zone);
        memcpy(q, &a, 4);
        memcpy(q + 4, &lo, 8);
        memcpy(q + 12, &hi, 8);
        q[20] = TSDB_DAY;
        memcpy(q + 21, &count, 4);
        memcpy(q + 34, &zl, 2);
        memcpy(q + 36, zone, zl);
        raw_send(fd, 30 + (uint32_t)utc, TSDB_OP_QUERY_GROUP, q, 36u + zl);
        uint32_t rid;
        uint8_t rest[1 << 16];
        size_t nrest;
        int32_t st = raw_reply(fd, &rid, rest, sizeof rest, &nrest);
        CHECK(rid == 30u + (uint32_t)utc && st == (utc ? 0 : TSDB_ERR_INVALID));
    }
    close(fd);
}

/* The spread (protocol version 8): through the client, and by hand: without the fields byte the reply keeps
 * its 64-byte rows (what older clients read), with TSDB_SPREAD the rows carry it, other bits are refused. */
static void test_spread_remote(tsdbc *c)
{
    uint32_t d;
    OK(tsdbc_series_create(c, "spread.f64", TSDB_FLOAT64, NULL, &d));
    int64_t ts[5];
    double v[5] = {101326, 101324, 101327, 101323, 0};
    uint8_t valid = 0x0f; /* the last one null */
    for (int k = 0; k < 5; k++)
        ts[k] = T0 + k * 1000;
    OK(tsdbc_insert_series_values(c, d, ts, &(tsdb_values){TSDB_FLOAT64, v, &valid}, 5));
    CHECK(tsdbc_server_version(c) == TSDB_PROTOCOL_VERSION);
    tsdb_spread_bucket *bk;
    size_t nb;
    OK(tsdbc_query_spread(c, d, INT64_MIN, INT64_MAX, 0, NULL, &bk, &nb));
    CHECK(nb == 1 && bk[0].b.count == 4 && bk[0].range == 4 && bk[0].var_pop == 2.5 && bk[0].var_samp == 10.0 / 3);
    CHECK(nb == 1 && bk[0].stddev_pop == sqrt(2.5) && bk[0].stddev_samp == sqrt(10.0 / 3));
    tsdb_bucket *pb;
    size_t np;
    OK(tsdbc_query_agg(c, d, INT64_MIN, INT64_MAX, 0, &pb, &np));
    CHECK(np == 1 && nb == 1 && memcmp(&pb[0], &bk[0].b, sizeof *pb) == 0);
    free(pb);
    int64_t bounds[2] = {T0, T0 + 2000};
    tsdb_spread_bucket *bb;
    size_t nbb;
    tsdb_query_opts desc = {TSDB_DESC, 1};
    OK(tsdbc_query_spread_bounds(c, d, INT64_MIN, INT64_MAX, bounds, 2, NULL, &bb, &nbb));
    CHECK(nbb == 2 && bb[0].b.count == 2 && bb[0].range == 2 && bb[0].var_pop == 1 && bb[1].var_samp == 8);
    free(bb);
    OK(tsdbc_query_spread_bounds(c, d, INT64_MIN, INT64_MAX, bounds, 2, &desc, &bb, &nbb));
    CHECK(nbb == 1 && bb[0].b.start == T0 + 2000 && bb[0].var_samp == 8);
    free(bb);
    tsdb_group sec2 = {TSDB_SECOND, 2, NULL}, berlin = {TSDB_DAY, 1, "Europe/Berlin"};
    tsdb_query_opts desc_all = {TSDB_DESC, 0};
    OK(tsdbc_query_spread_group(c, d, INT64_MIN, T0 + 4000, &sec2, &desc_all, &bb, &nbb));
    CHECK(nbb == 2 && bb[0].b.start == T0 + 2000 && bb[0].var_samp == 8 && bb[1].var_pop == 1);
    free(bb);
    OK(tsdbc_query_spread_group(c, d, INT64_MIN, INT64_MAX, &berlin, NULL, &bb, &nbb)); /* local: bounds */
    CHECK(nbb == 1 && bb[0].b.count == 4 && bb[0].var_pop == 2.5);
    free(bb);

    int fd = raw_connect();
    for (int fields = -1; fields <= 2; fields++) {
        uint8_t q[45] = {0};
        int64_t lo = INT64_MIN, hi = INT64_MAX;
        memcpy(q, &d, 4);
        memcpy(q + 4, &lo, 8);
        memcpy(q + 12, &hi, 8);
        q[44] = (uint8_t)fields;
        raw_send(fd, 40 + (uint32_t)(fields + 1), TSDB_OP_QUERY_BUCKETS, q, fields < 0 ? 44 : 45);
        uint32_t rid, n = 0;
        uint8_t rest[256];
        size_t nrest = 0;
        int32_t st = raw_reply(fd, &rid, rest, sizeof rest, &nrest);
        CHECK(rid == 40u + (uint32_t)(fields + 1));
        if (fields == 2) {
            CHECK(st == TSDB_ERR_INVALID);
            continue;
        }
        memcpy(&n, rest, 4);
        size_t row = fields == TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW;
        CHECK(st == 0 && n == 1 && nrest == 4 + row);
        if (st == 0 && n == 1 && nrest == 4 + row && row == TSDB_SPREAD_ROW) {
            double x[5];
            memcpy(x, rest + 4 + 64, sizeof x);
            CHECK(x[0] == bk[0].range && x[1] == bk[0].var_pop && x[2] == bk[0].var_samp &&
                  x[3] == bk[0].stddev_pop && x[4] == bk[0].stddev_samp);
        }
    }
    /* QUERY_BOUNDS with the byte after the bounds; a reserved bit is refused */
    for (int fields = 1; fields <= 3; fields++) {
        uint8_t q[33 + 16 + 1] = {0};
        int64_t lo = INT64_MIN, hi = INT64_MAX;
        uint32_t n2 = 2;
        memcpy(q, &d, 4);
        memcpy(q + 4, &lo, 8);
        memcpy(q + 12, &hi, 8);
        memcpy(q + 29, &n2, 4);
        memcpy(q + 33, bounds, 16);
        q[49] = (uint8_t)fields;
        raw_send(fd, 50 + (uint32_t)fields, TSDB_OP_QUERY_BOUNDS, q, sizeof q);
        uint32_t rid, n = 0;
        uint8_t rest[512];
        size_t nrest = 0;
        int32_t st = raw_reply(fd, &rid, rest, sizeof rest, &nrest);
        CHECK(rid == 50u + (uint32_t)fields);
        if (fields != TSDB_SPREAD) {
            CHECK(st == TSDB_ERR_INVALID);
            continue;
        }
        memcpy(&n, rest, 4);
        CHECK(st == 0 && n == 2 && nrest == 4 + 2 * TSDB_SPREAD_ROW);
    }
    close(fd);
    free(bk);
}

/* A server of protocol version 6 (before the spread), as far as bucket queries go: HELLO answers 6, a
 * QUERY_BUCKETS or QUERY_BOUNDS with a byte after the zone or bounds is refused as such a server does, one
 * without it gets a bucket of 64 bytes. Counts the bucket requests it sees. */
static const int64_t t0_old = 1700000000000;

typedef struct old_server {
    int lfd, port;
    int bucket_reqs, refused;
} old_server;

static void old_reply(int fd, uint32_t id, int32_t st, const void *body, uint32_t n)
{
    uint8_t h[13];
    uint32_t flen = 9 + n;
    memcpy(h, &flen, 4);
    memcpy(h + 4, &id, 4);
    h[8] = TSDB_OP_REPLY;
    memcpy(h + 9, &st, 4);
    CHECK(write(fd, h, 13) == 13);
    CHECK(n == 0 || write(fd, body, n) == (ssize_t)n);
}

static void *old_server_run(void *arg)
{
    old_server *o = arg;
    int fd = accept(o->lfd, NULL, NULL);
    uint8_t h[9], p[4096];
    while (fd >= 0 && read_full(fd, h, 9)) {
        uint32_t flen, id;
        memcpy(&flen, h, 4);
        memcpy(&id, h + 4, 4);
        uint32_t len = flen - 5;
        if (len > sizeof p || !read_full(fd, p, len))
            break;
        if (h[8] == TSDB_OP_HELLO) {
            uint32_t six = 6;
            old_reply(fd, id, 0, &six, 4);
            continue;
        }
        bool exact = false;
        if (h[8] == TSDB_OP_QUERY_BUCKETS && len >= 44) {
            uint16_t zl;
            memcpy(&zl, p + 42, 2);
            exact = len == 44u + zl;
        } else if (h[8] == TSDB_OP_QUERY_BOUNDS && len >= 33) {
            uint32_t n;
            memcpy(&n, p + 29, 4);
            exact = len == 33 + (size_t)n * 8;
        } else {
            old_reply(fd, id, TSDB_ERR_INVALID, NULL, 0);
            continue;
        }
        o->bucket_reqs++;
        if (!exact) {
            o->refused++;
            old_reply(fd, id, TSDB_ERR_INVALID, NULL, 0);
            continue;
        }
        uint8_t body[4 + TSDB_BUCKET_ROW] = {0};
        uint32_t one = 1;
        uint64_t count = 3;
        memcpy(body, &one, 4);
        memcpy(body + 4, &t0_old, 8);
        memcpy(body + 12, &count, 8);
        old_reply(fd, id, 0, body, sizeof body);
    }
    if (fd >= 0)
        close(fd);
    return NULL;
}

/* The client against a version 6 server: plain bucket queries send exactly what such a server takes and read
 * its 64-byte rows; the spread is refused by the client without a request (the server would refuse it). */
static void test_spread_old_server(void)
{
    old_server o = {0};
    o.lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    socklen_t sl = sizeof sa;
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    CHECK(bind(o.lfd, (struct sockaddr *)&sa, sizeof sa) == 0 && listen(o.lfd, 1) == 0);
    CHECK(getsockname(o.lfd, (struct sockaddr *)&sa, &sl) == 0);
    o.port = ntohs(sa.sin_port);
    pthread_t th;
    pthread_create(&th, NULL, old_server_run, &o);
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", o.port, &c));
    CHECK(tsdbc_server_version(c) == 6);
    tsdb_bucket *b;
    size_t n;
    int64_t bounds[1] = {0};
    tsdb_group g = {TSDB_SECOND, 1, NULL};
    OK(tsdbc_query_agg(c, 1, 0, INT64_MAX, 1000, &b, &n));
    CHECK(n == 1 && b[0].start == t0_old && b[0].count == 3);
    free(b);
    OK(tsdbc_query_group(c, 1, 0, INT64_MAX, &g, NULL, &b, &n));
    CHECK(n == 1 && b[0].count == 3);
    free(b);
    OK(tsdbc_query_bounds(c, 1, 0, INT64_MAX, bounds, 1, NULL, &b, &n));
    CHECK(n == 1 && b[0].count == 3);
    free(b);
    tsdb_spread_bucket *sb = (tsdb_spread_bucket *)1;
    n = 7;
    CHECK(tsdbc_query_spread(c, 1, 0, INT64_MAX, 1000, NULL, &sb, &n) == TSDB_ERR_INVALID && !sb && !n);
    CHECK(tsdbc_query_spread_group(c, 1, 0, INT64_MAX, &g, NULL, &sb, &n) == TSDB_ERR_INVALID);
    CHECK(tsdbc_query_spread_bounds(c, 1, 0, INT64_MAX, bounds, 1, NULL, &sb, &n) == TSDB_ERR_INVALID);
    tsdbc_close(c);
    pthread_join(th, NULL);
    close(o.lfd);
    CHECK(o.bucket_reqs == 3 && o.refused == 0);
    if (o.bucket_reqs != 3 || o.refused)
        fprintf(stderr, "old server: %d bucket requests, %d refused\n", o.bucket_reqs, o.refused);
}

static void test_packed_inserts(tsdbc *c)
{
    uint32_t a, b;
    OK(tsdbc_series_create(c, "pk.on", TSDB_FLOAT64, NULL, &a));
    OK(tsdbc_series_create(c, "pk.off", TSDB_FLOAT64, NULL, &b));
    enum { N = 100000 };
    int64_t *ts = malloc(N * 8);
    double *v = malloc(N * 8);
    for (int i = 0; i < N; i++) {
        ts[i] = T0 + i * 1000;
        v[i] = 20 + (i % 100) / 10.0;
    }
    tsdb_values col = {TSDB_FLOAT64, v, NULL};
    OK(tsdbc_insert_series_values(c, a, ts, &col, N));
    OK(tsdbc_set_compression(c, 0));
    OK(tsdbc_insert_series_values(c, b, ts, &col, N));
    OK(tsdbc_set_compression(c, 1));
    tsdb_bucket *ba, *bb;
    size_t na, nb;
    OK(tsdbc_query_agg(c, a, INT64_MIN, INT64_MAX, 0, &ba, &na));
    OK(tsdbc_query_agg(c, b, INT64_MIN, INT64_MAX, 0, &bb, &nb));
    CHECK(na == 1 && nb == 1 && ba[0].count == N && bb[0].count == N && ba[0].sum == bb[0].sum);
    free(ba);
    free(bb);
    CHECK(tsdbc_set_compression(c, 1000) == TSDB_ERR_INVALID);

    /* a packed frame whose zstd frame is damaged, and one that claims more rows */
    int fd = raw_connect();
    for (int bad = 0; bad < 2; bad++) {
        uint8_t raw[64], z[128], f[TSDB_VALUES_HEADER + 4 + 128] = {0};
        int64_t t1 = T0;
        double x = 1;
        size_t rl = tsdb_pack_reshape(1, TSDB_FLOAT64, &t1, &x, NULL, NULL, raw);
        size_t zl = ZSTD_compress(z, sizeof z, raw, rl, 1);
        uint32_t n = bad ? 2 : 1, rl32 = (uint32_t)rl;
        memcpy(f, &n, 4);
        memcpy(f + 4, &a, 4);
        f[8] = TSDB_FLOAT64;
        f[9] = TSDB_PACKED;
        memcpy(f + TSDB_VALUES_HEADER, &rl32, 4);
        memcpy(f + TSDB_VALUES_HEADER + 4, z, zl);
        if (!bad)
            f[TSDB_VALUES_HEADER + 4 + zl / 2] ^= 0xff;
        raw_send(fd, 60 + (uint32_t)bad, TSDB_OP_INSERT_VALUES, f, (uint32_t)(TSDB_VALUES_HEADER + 4 + zl));
        uint32_t rid;
        uint8_t rest[64];
        size_t nrest;
        CHECK(raw_reply(fd, &rid, rest, sizeof rest, &nrest) == TSDB_ERR_INVALID && rid == 60 + (uint32_t)bad);
    }
    close(fd);
    OK(tsdbc_query_agg(c, a, INT64_MIN, INT64_MAX, 0, &ba, &na));
    CHECK(na == 1 && ba[0].count == N); /* nothing of them got in */
    free(ba);
    free(ts);
    free(v);
    test_hello_keeps_errors(a);
}

/* ------------------------------------------ concurrent writes to the same keys */

/* Several connections write the same series and timestamps at once
 * (samekeys.h has the model and the checks): small and large inserts, typed
 * with nulls, waiting for each or pipelined, packed or plain, while other
 * connections flush, optimize and query. Pipelined inserts of one connection
 * are applied in order, so a writer's last round still wins over its others. */
typedef struct skn {
    const sk_cfg *c;
    uint32_t ids[SK_MAX_SERIES];
    atomic_int started[SK_MAX_WRITERS]; /* rounds sent */
    atomic_int acked[SK_MAX_WRITERS];   /* rounds acknowledged */
    int last_kind[SK_MAX_WRITERS];
    atomic_int writing, errors;
    atomic_long snapshots, torn_across, aggs, pipelined, packed;
} skn;

typedef struct sknt {
    skn *x;
    int i;
} sknt;

static tsdbc *sk_connect(void)
{
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    return c;
}

static void *skn_writer(void *a)
{
    sknt *t = a;
    skn *x = t->x;
    const sk_cfg *c = x->c;
    tsdbc *cn = sk_connect();
    int w = t->i;
    for (int r = 0; r < c->rounds; r++) {
        uint64_t seed = sk_mix(((uint64_t)w << 32) ^ (uint64_t)r ^ 0xc11e47ull);
        int kind = sk_pick(c, seed >> 40);
        bool async = (seed >> 8) % 2, packed = (seed >> 9) % 2, plain = c->type == TSDB_FLOAT32 && !c->nulls;
        OK(tsdbc_set_compression(cn, packed ? 1 : 0));
        atomic_store(&x->started[w], r + 1);
        bool per = kind == SK_SERIES || kind == SK_REPLACE;
        for (uint32_t s = 0; s < (per ? c->nseries : 1); s++) {
            sk_batch b;
            bool large = kind == SK_LARGE || (per && (seed >> 24) % 2);
            sk_build(c, x->ids, w, r, large, (int)(seed % 3), seed, per ? s : UINT32_MAX, &b);
            tsdb_values col = {(tsdb_type)c->type, b.v, c->nulls ? b.valid : NULL};
            const float *f = (const float *)(void *)b.v;
            if (kind == SK_REPLACE) /* synchronous: the replace has no pipelined form */
                OK(tsdbc_replace_range(cn, b.series[0], sk_ts(c, (uint32_t)w * c->shift),
                                       sk_ts(c, (uint32_t)w * c->shift + c->nts), b.ts, &col, b.n));
            else if (kind == SK_SERIES)
                OK(async ? tsdbc_insert_series_values_async(cn, b.series[0], b.ts, &col, b.n)
                         : tsdbc_insert_series_values(cn, b.series[0], b.ts, &col, b.n));
            else if (plain)
                OK(async ? tsdbc_insert_async(cn, b.series, b.ts, f, b.n) : tsdbc_insert(cn, b.series, b.ts, f, b.n));
            else
                OK(async ? tsdbc_insert_values_async(cn, b.series, b.ts, &col, b.n)
                         : tsdbc_insert_values(cn, b.series, b.ts, &col, b.n));
            sk_batch_free(&b);
        }
        atomic_fetch_add(&x->pipelined, async);
        atomic_fetch_add(&x->packed, packed);
        if (async && (seed >> 10) % 4 == 0) /* else several stay in flight */
            OK(tsdbc_wait(cn));
        if (!async || (seed >> 10) % 4 == 0)
            atomic_store(&x->acked[w], r + 1); /* a wait collects every earlier one too */
        x->last_kind[w] = kind;
    }
    OK(tsdbc_wait(cn));
    atomic_store(&x->acked[w], c->rounds);
    tsdbc_close(cn);
    atomic_fetch_sub(&x->writing, 1);
    return NULL;
}

static void *skn_reader(void *a)
{
    sknt *t = a;
    skn *x = t->x;
    const sk_cfg *c = x->c;
    tsdbc *cn = sk_connect();
    uint32_t span = sk_span(c);
    sk_seen seen;
    sk_seen_init(&seen, c);
    sk_val *row = malloc(span * sizeof *row);
    int started[SK_MAX_WRITERS], acked[SK_MAX_WRITERS];
    bool batches = !(c->kinds & SK_APPEND);
    for (uint64_t it = 0; atomic_load(&x->writing) > 0; it++) {
        int64_t prev = -1;
        for (uint32_t s = 0; s < c->nseries; s++) {
            for (int w = 0; w < c->writers; w++)
                acked[w] = atomic_load(&x->acked[w]);
            sk_collect k;
            sk_collect_begin(&k, c, row, false);
            OK(tsdbc_query_values(cn, x->ids[s], c->t0, sk_end(c), NULL, sk_collect_cb, &k));
            for (int w = 0; w < c->writers; w++)
                started[w] = atomic_load(&x->started[w]);
            int64_t id;
            int e = k.errors + sk_check_snapshot(c, s, row, acked, started, &seen, &id);
            if (s > 0 && id >= 0 && prev >= 0 && id != prev)
                atomic_fetch_add(&x->torn_across, 1);
            prev = id;
            atomic_fetch_add(&x->snapshots, 1);
            if (batches && c->shift == 0 && (it + s) % 3 == (uint64_t)t->i % 3) {
                tsdb_bucket *b;
                size_t nb;
                OK(tsdbc_query_agg(cn, x->ids[s], c->t0, sk_end(c), 0, &b, &nb));
                for (int w = 0; w < c->writers; w++)
                    started[w] = atomic_load(&x->started[w]);
                e += sk_check_agg(c, s, b, nb, started);
                free(b);
                atomic_fetch_add(&x->aggs, 1);
            }
            if (e)
                atomic_fetch_add(&x->errors, e);
        }
    }
    free(row);
    sk_seen_free(&seen);
    tsdbc_close(cn);
    return NULL;
}

static void *skn_maintenance(void *a)
{
    skn *x = ((sknt *)a)->x;
    tsdbc *cn = sk_connect();
    for (int i = 0; atomic_load(&x->writing) > 0; i++) {
        usleep(10000);
        OK(i % 10 == 9 ? tsdbc_optimize(cn) : tsdbc_flush(cn));
    }
    tsdbc_close(cn);
    return NULL;
}

static void skn_read_all(const sk_cfg *c, const uint32_t *ids, sk_val *st)
{
    tsdbc *cn = sk_connect();
    for (uint32_t s = 0; s < c->nseries; s++) {
        sk_collect k;
        sk_collect_begin(&k, c, st + (size_t)s * sk_span(c), false);
        OK(tsdbc_query_values(cn, ids[s], c->t0, sk_end(c), NULL, sk_collect_cb, &k));
        CHECK(k.errors == 0);
    }
    tsdbc_close(cn);
}

static void skn_same(const sk_cfg *c, const uint32_t *ids, const sk_val *want, const char *after)
{
    size_t nk = sk_nkeys(c);
    sk_val *st = calloc(nk, sizeof *st);
    skn_read_all(c, ids, st);
    bool same = true;
    for (size_t i = 0; i < nk; i++)
        same &= st[i].st == want[i].st && st[i].code == want[i].code;
    if (!same)
        fprintf(stderr, "  %s: the state changed after %s\n", c->name, after);
    CHECK(same);
    free(st);
}

/* One run: writers, readers, maintenance; the final state is checked and left in st. */
static void skn_run(skn *x, int k, sk_val *st)
{
    const sk_cfg *c = x->c;
    tsdbc *cn = sk_connect();
    for (uint32_t s = 0; s < c->nseries; s++) {
        char name[64];
        snprintf(name, sizeof name, "same%d.%u", k, s);
        OK(tsdbc_series_create(cn, name, (tsdb_type)c->type, NULL, &x->ids[s]));
    }
    tsdbc_close(cn);
    enum { READERS = 3 };
    pthread_t th[SK_MAX_WRITERS + READERS + 1];
    sknt args[SK_MAX_WRITERS + READERS + 1];
    int n = 0;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    atomic_store(&x->writing, c->writers);
    for (int w = 0; w < c->writers; w++, n++) {
        args[n] = (sknt){x, w};
        pthread_create(&th[n], NULL, skn_writer, &args[n]);
    }
    for (int i = 0; i < READERS; i++, n++) {
        args[n] = (sknt){x, i};
        pthread_create(&th[n], NULL, skn_reader, &args[n]);
    }
    args[n] = (sknt){x, 0};
    pthread_create(&th[n], NULL, skn_maintenance, &args[n]);
    n++;
    for (int i = 0; i < n; i++)
        pthread_join(th[i], NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    CHECK(atomic_load(&x->errors) == 0);
    skn_read_all(c, x->ids, st);
    CHECK(sk_check_final(c, st, x->last_kind) == 0);
    printf("  %-28s %d connections x %d rounds (%ld pipelined, %ld packed), %u keys: %ld snapshots (%ld aggregates), "
           "%ld torn across series, %.2f s\n",
           c->name, c->writers, c->rounds, (long)x->pipelined, (long)x->packed, sk_nkeys(c), (long)x->snapshots,
           (long)x->aggs, (long)x->torn_across,
           (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9);
}

static void test_same_keys(const char *dir)
{
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
    /* a write of every key once goes WAL + memtable, twice (240 rows) a part per partition */
    static const char *const args[] = {"--wal-max-batch-rows", "360", "--partition-ms", "3600000", "--workers", "8",
                                       NULL};
    pid_t srv = start_server_with(dir, args);
    const int64_t t0 = T0 + 1234;
    const unsigned batches = SK_SMALL | SK_LARGE;
    enum { NC = 6 };
    static sk_cfg c[NC];
    c[0] = (sk_cfg){"float32, same keys", 6, 1000, 3, 80, 0, t0, 97000, TSDB_FLOAT32, false, batches};
    c[1] = (sk_cfg){"int64 + nulls, same keys", 6, 1000, 3, 80, 0, t0, 97000, TSDB_INT64, true, batches};
    c[2] = (sk_cfg){"float32, shifted keys", 6, 1000, 3, 80, 20, t0, 97000, TSDB_FLOAT32, false, batches};
    c[3] = (sk_cfg){"float64 + nulls, per series", 6, 600, 3, 80, 37, t0, 97000, TSDB_FLOAT64, true,
                    batches | SK_SERIES};
    c[4] = (sk_cfg){"int64 + nulls, replaces", 5, 150, 3, 80, 23, t0, 97000, TSDB_INT64, true,
                    batches | SK_REPLACE};
    c[5] = (sk_cfg){"float32, then SIGKILL", 5, 600, 3, 80, 10, t0, 97000, TSDB_FLOAT32, false, batches};
    skn *x = calloc(NC, sizeof *x);
    sk_val *st[NC];
    for (int k = 0; k < NC; k++) {
        x[k].c = &c[k];
        st[k] = calloc(sk_nkeys(&c[k]), sizeof *st[k]);
        skn_run(&x[k], k, st[k]);
        if (k == NC - 1)
            break; /* the last one stays in the WAL for the crash below */
        tsdbc *cn = sk_connect();
        OK(tsdbc_flush(cn));
        skn_same(&c[k], x[k].ids, st[k], "a flush");
        OK(tsdbc_optimize(cn));
        skn_same(&c[k], x[k].ids, st[k], "an optimize");
        tsdbc_close(cn);
    }
    /* a crash: the memtable comes back from the WAL, next to the parts written meanwhile */
    signal_pid(srv, SIGKILL);
    int status;
    waitpid(srv, &status, 0);
    srv = start_server_with(dir, args);
    for (int k = 0; k < NC; k++)
        skn_same(&c[k], x[k].ids, st[k], "SIGKILL and a restart");
    tsdbc *cn = sk_connect();
    OK(tsdbc_optimize(cn));
    tsdbc_close(cn);
    stop_server(srv);
    srv = start_server_with(dir, args);
    for (int k = 0; k < NC; k++)
        skn_same(&c[k], x[k].ids, st[k], "an optimize, SIGTERM and a restart");
    stop_server(srv);
    for (int k = 0; k < NC; k++)
        free(st[k]);
    free(x);
    CHECK(system(cmd) == 0);
}

/* Fail-stop: a WAL write or sync fails (EIO from the crashfs shim, which
 * from then on lies about that file's syncs, as Linux may after a failed
 * writeback). The insert whose rows went through the failed call gets an I/O
 * error, the server exits with status 1 instead of acknowledging anything
 * more, and a restart has every acknowledged row. */
static void test_wal_failstop(const char *dir)
{
    char cmd[1200], mark[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && mkdir -p '%s'", dir, dir);
    if (system(cmd) != 0) {
        failures++;
        return;
    }
    snprintf(mark, sizeof mark, "%s.mark", dir);
    unlink(mark);
    eio_mark = mark;
    pid_t srv = start_server_with(dir, NULL);
    eio_mark = NULL;
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    uint32_t id;
    OK(tsdbc_series(c, "failstop", 1, &id));
    enum { ROWS = 10 };
    int64_t ts[ROWS];
    float v[ROWS] = {0};
    uint64_t acked = 0;
    int first_error = 0, after = 0;
    for (int b = 0; b < 400; b++) {
        if (b == 50)
            signal_pid(srv, SIGUSR2); /* the next WAL write or fdatasync fails */
        for (int i = 0; i < ROWS; i++)
            ts[i] = T0 + (int64_t)(b * ROWS + i) * 1000;
        int rc = tsdbc_insert_series(c, id, ts, v, ROWS);
        if (rc == 0 && !first_error)
            acked += ROWS;
        else if (rc == 0)
            after++; /* acknowledged after a failure: what fail-stop forbids */
        else if (!first_error)
            first_error = rc;
        if (first_error && rc != 0 && b > 60)
            break;
    }
    CHECK(acked >= 50 * ROWS);
    CHECK(first_error == TSDB_ERR_IO);
    CHECK(after == 0);
    tsdbc_close(c);
    int status = 0;
    pid_t got = 0;
    for (int i = 0; i < 300 && (got = waitpid(srv, &status, WNOHANG)) == 0; i++) /* stops in about 220 ms */
        usleep(10000);
    CHECK(got == srv);
    if (got != srv) {
        signal_pid(srv, SIGKILL);
        waitpid(srv, &status, 0);
    }
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 1);
    CHECK(access(mark, F_OK) == 0);
    srv = start_server_with(dir, NULL);
    OK(tsdbc_connect("127.0.0.1", port, &c));
    uint64_t n = count_of(c, id);
    if (n < acked || n > acked + ROWS)
        fprintf(stderr, "after the restart %llu rows, %llu acknowledged\n", (unsigned long long)n,
                (unsigned long long)acked);
    CHECK(n >= acked && n <= acked + ROWS);
    OK(tsdbc_insert_series(c, id, ts, v, ROWS)); /* the restarted server takes writes */
    tsdbc_close(c);
    stop_server(srv);
    unlink(mark);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (!failures && system(cmd) != 0)
        failures++;
}

/* ------------------------------------------------------------ metrics endpoint */

static int mport;

static int metrics_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)mport)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* Sends req (n bytes) and reads until the server closes or `wait_s` passes. Returns the
 * malloc'd reply (NUL-terminated), its length in *len; NULL when the connection failed. */
static char *http_exchange(const char *req, size_t n, int wait_s, size_t *len)
{
    int fd = metrics_connect();
    if (fd < 0)
        return NULL;
    struct timeval tv = {wait_s, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    for (size_t sent = 0; sent < n;) {
        ssize_t w = send(fd, req + sent, n - sent, MSG_NOSIGNAL);
        if (w <= 0)
            break; /* the server may already have answered and closed */
        sent += (size_t)w;
    }
    size_t cap = 1 << 16, got = 0;
    char *buf = malloc(cap);
    for (;;) {
        if (got + 4096 > cap)
            buf = realloc(buf, cap *= 2);
        ssize_t r = recv(fd, buf + got, cap - got - 1, 0);
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    buf[got] = '\0';
    close(fd);
    *len = got;
    return buf;
}

#define HTTP_EXCHANGE(req, wait_s, len) http_exchange(req, sizeof(req) - 1, wait_s, len)

static int http_status(const char *reply) { return strncmp(reply, "HTTP/1.1 ", 9) == 0 ? atoi(reply + 9) : -1; }

/* One parsed sample. */
typedef struct {
    char name[96], labels[256];
    double v;
} msample;
typedef struct {
    msample *s;
    size_t n, cap;
} mset;

static bool name_char(char ch, bool first)
{
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == ':' || (!first && ch >= '0' && ch <= '9');
}

/* A strict reader of the text exposition format 0.0.4 (what promtool check metrics enforces for the
 * parts we emit): HELP and TYPE before the samples of a family and once each, names and labels
 * well formed, quoted label values with only \\ \" \n escapes, numeric values, histogram buckets
 * cumulative with le="+Inf" equal to _count. Fills `out`; returns false (after saying why) if not. */
static bool parse_exposition(const char *body, mset *out)
{
    struct fam {
        char name[96], type[16];
        bool help, sampled;
    } fams[128];
    size_t nf = 0;
    memset(out, 0, sizeof *out);
    const char *p = body;
    int lineno = 0;
#define BAD(...)                                           \
    do {                                                   \
        fprintf(stderr, "metrics line %d: ", lineno);      \
        fprintf(stderr, __VA_ARGS__);                      \
        fprintf(stderr, "\n");                             \
        return false;                                      \
    } while (0)
    while (*p) {
        const char *e = strchr(p, '\n');
        if (!e)
            BAD("no newline at the end");
        lineno++;
        size_t ll = (size_t)(e - p);
        char line[1024];
        if (ll >= sizeof line)
            BAD("line too long");
        memcpy(line, p, ll);
        line[ll] = '\0';
        p = e + 1;
        if (ll == 0)
            continue;
        if (line[0] == '#') {
            char kind[8], name[96], rest[600] = "";
            int k = sscanf(line, "# %7s %95s %599[^\n]", kind, name, rest);
            if (k < 2)
                continue; /* a plain comment */
            size_t i;
            for (i = 0; i < nf && strcmp(fams[i].name, name); i++)
                ;
            if (strcmp(kind, "TYPE") == 0) {
                if (i < nf && fams[i].type[0])
                    BAD("TYPE of %s twice", name);
                if (i < nf && fams[i].sampled)
                    BAD("TYPE of %s after its samples", name);
                if (k < 3 || !(strcmp(rest, "counter") == 0 || strcmp(rest, "gauge") == 0 || strcmp(rest, "histogram") == 0))
                    BAD("TYPE %s: unknown type '%s'", name, rest);
                if (i == nf) {
                    if (nf == 128)
                        BAD("too many families");
                    fams[nf++] = (struct fam){0};
                    snprintf(fams[i].name, sizeof fams[i].name, "%s", name);
                }
                snprintf(fams[i].type, sizeof fams[i].type, "%s", rest);
                if (strcmp(rest, "counter") == 0 && (strlen(name) < 6 || strcmp(name + strlen(name) - 6, "_total")))
                    BAD("counter %s does not end in _total", name);
            } else if (strcmp(kind, "HELP") == 0) {
                if (i == nf) {
                    fams[nf] = (struct fam){0};
                    snprintf(fams[nf].name, sizeof fams[nf].name, "%s", name);
                    i = nf++;
                }
                if (fams[i].help)
                    BAD("HELP of %s twice", name);
                fams[i].help = true;
            }
            continue;
        }
        /* sample: name[{labels}] value */
        msample sm = {0};
        const char *q = line;
        size_t nl = 0;
        while (name_char(*q, nl == 0) && nl < sizeof sm.name - 1)
            sm.name[nl++] = *q++;
        if (nl == 0)
            BAD("sample without a name: %s", line);
        if (*q == '{') {
            const char *lb = ++q;
            while (*q != '}') {
                size_t kl = 0;
                while (name_char(q[kl], kl == 0) && q[kl] != ':')
                    kl++;
                if (kl == 0 || q[kl] != '=' || q[kl + 1] != '"')
                    BAD("bad label in: %s", line);
                q += kl + 2;
                for (;;) {
                    if (*q == '\0')
                        BAD("unterminated label value: %s", line);
                    if (*q == '\\') {
                        if (q[1] != '\\' && q[1] != '"' && q[1] != 'n')
                            BAD("bad escape: %s", line);
                        q += 2;
                    } else if (*q == '"') {
                        q++;
                        break;
                    } else {
                        q++;
                    }
                }
                if (*q == ',')
                    q++;
                else if (*q != '}')
                    BAD("bad label list: %s", line);
            }
            size_t lablen = (size_t)(q - lb);
            if (lablen >= sizeof sm.labels)
                BAD("labels too long");
            memcpy(sm.labels, lb, lablen);
            q++;
        }
        if (*q != ' ')
            BAD("no space before the value: %s", line);
        char *endp;
        sm.v = strtod(q + 1, &endp);
        if (endp == q + 1 || *endp != '\0')
            BAD("bad value: %s", line);
        /* the family: the name itself, or a histogram's _bucket, _sum, _count */
        size_t i;
        for (i = 0; i < nf; i++) {
            size_t fl = strlen(fams[i].name);
            if (strcmp(fams[i].name, sm.name) == 0 ||
                (strcmp(fams[i].type, "histogram") == 0 && strncmp(sm.name, fams[i].name, fl) == 0 &&
                 (strcmp(sm.name + fl, "_bucket") == 0 || strcmp(sm.name + fl, "_sum") == 0 ||
                  strcmp(sm.name + fl, "_count") == 0)))
                break;
        }
        if (i == nf || !fams[i].type[0] || !fams[i].help)
            BAD("sample %s without HELP and TYPE before it", sm.name);
        fams[i].sampled = true;
        if (out->n == out->cap) {
            out->cap = out->cap ? out->cap * 2 : 256;
            out->s = realloc(out->s, out->cap * sizeof *out->s);
        }
        out->s[out->n++] = sm;
    }
    /* histograms: per label set, buckets cumulative in file order, +Inf last and equal to _count */
    for (size_t i = 0; i < out->n; i++) {
        const msample *a = &out->s[i];
        size_t nl = strlen(a->name);
        if (nl < 7 || strcmp(a->name + nl - 7, "_bucket"))
            continue;
        char base[96], series[256], inf_labels[300];
        snprintf(base, sizeof base, "%.*s", (int)nl - 7, a->name);
        const char *le = strstr(a->labels, ",le=\"");
        if (!le)
            BAD("bucket without le: %s", a->labels);
        snprintf(series, sizeof series, "%.*s", (int)(le - a->labels), a->labels);
        snprintf(inf_labels, sizeof inf_labels, "%s,le=\"+Inf\"", series);
        if (i + 1 < out->n && strcmp(out->s[i + 1].name, a->name) == 0 && strncmp(out->s[i + 1].labels, series, strlen(series)) == 0 &&
            out->s[i + 1].v < a->v)
            BAD("%s buckets decrease", base);
        if (strcmp(a->labels, inf_labels) == 0) {
            char cname[128];
            snprintf(cname, sizeof cname, "%s_count", base);
            bool found = false;
            for (size_t j = 0; j < out->n; j++)
                if (strcmp(out->s[j].name, cname) == 0 && strcmp(out->s[j].labels, series) == 0) {
                    found = true;
                    if (out->s[j].v != a->v)
                        BAD("%s{%s}: _count %g != +Inf bucket %g", base, series, out->s[j].v, a->v);
                }
            if (!found)
                BAD("%s{%s} has no _count", base, series);
        }
    }
#undef BAD
    return true;
}

/* Value of the first sample called `name` whose labels contain `lab` (NULL: any); NAN if none. */
static double mval(const mset *m, const char *name, const char *lab)
{
    for (size_t i = 0; i < m->n; i++)
        if (strcmp(m->s[i].name, name) == 0 && (!lab || strstr(m->s[i].labels, lab)))
            return m->s[i].v;
    return NAN;
}

/* Sum of the values of every sample called `name` (labels ignored). */
static double msum(const mset *m, const char *name)
{
    double t = 0;
    for (size_t i = 0; i < m->n; i++)
        if (strcmp(m->s[i].name, name) == 0)
            t += m->s[i].v;
    return t;
}

/* Scrapes /metrics and parses it; fails the test if it does not work. */
static bool scrape(mset *out)
{
    static const char req[] = "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/plain\r\n\r\n";
    size_t len;
    char *r = http_exchange(req, sizeof req - 1, 5, &len);
    if (!r) {
        CHECK(!"metrics port refused the connection");
        memset(out, 0, sizeof *out);
        return false;
    }
    char *body = strstr(r, "\r\n\r\n");
    bool ok = http_status(r) == 200 && body && strstr(r, "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n");
    if (ok) {
        body += 4;
        const char *cl = strstr(r, "Content-Length: ");
        ok = cl && (size_t)atol(cl + 16) == strlen(body) && parse_exposition(body, out);
    }
    if (!ok) {
        free(out->s); /* a parse that failed halfway has filled it in part */
        memset(out, 0, sizeof *out);
    }
    free(r);
    CHECK(ok);
    return ok;
}

static void mset_free(mset *m)
{
    free(m->s);
    memset(m, 0, sizeof *m);
}

/* Counters never go down between two scrapes. */
static void check_monotone(const mset *a, const mset *b)
{
    for (size_t i = 0; i < a->n; i++) {
        size_t nl = strlen(a->s[i].name);
        bool counter = (nl > 6 && strcmp(a->s[i].name + nl - 6, "_total") == 0) ||
                       (nl > 7 && strcmp(a->s[i].name + nl - 7, "_bucket") == 0) ||
                       (nl > 6 && strcmp(a->s[i].name + nl - 6, "_count") == 0) ||
                       (nl > 4 && strcmp(a->s[i].name + nl - 4, "_sum") == 0);
        if (!counter)
            continue;
        for (size_t j = 0; j < b->n; j++)
            if (strcmp(b->s[j].name, a->s[i].name) == 0 && strcmp(b->s[j].labels, a->s[i].labels) == 0) {
                if (b->s[j].v < a->s[i].v) {
                    fprintf(stderr, "%s{%s} went down: %g -> %g\n", a->s[i].name, a->s[i].labels, a->s[i].v, b->s[j].v);
                    failures++;
                }
                break;
            }
    }
}

typedef struct {
    uint32_t id;
    int errors;
} mjob;

enum { M_THREADS = 4, M_ROUNDS = 20, M_ROWS = 1000, M_QUERIES = 5 };

/* Every TSDB_OP_X in tsdb_protocol.h must have a tsdb_requests_total{op="x"} sample: an op added to the
 * protocol (by this or another branch) and left out of the table in metrics.c would be counted as
 * "other" and a query op would miss the latency histogram. */
static void check_ops_named(const mset *m)
{
    const char *paths[] = {"include/tsdb_protocol.h", "../include/tsdb_protocol.h", NULL};
    FILE *f = NULL;
    for (int i = 0; paths[i] && !f; i++)
        f = fopen(paths[i], "r");
    CHECK(f != NULL);
    if (!f)
        return;
    char line[256];
    int seen = 0;
    while (fgets(line, sizeof line, f)) {
        char name[64];
        int num;
        if (sscanf(line, " TSDB_OP_%63[A-Z_] = %d", name, &num) != 2 || strcmp(name, "REPLY") == 0)
            continue;
        char want[96];
        size_t k = 0;
        for (const char *q = name; *q && k < 60; q++)
            want[k++] = (char)(*q >= 'A' && *q <= 'Z' ? *q - 'A' + 'a' : *q);
        want[k] = 0;
        char lab[128];
        snprintf(lab, sizeof lab, "op=\"%s\"", want);
        double v = mval(m, "tsdb_requests_total", lab);
        CHECK(!isnan(v));
        if (isnan(v))
            fprintf(stderr, "TSDB_OP_%s = %d has no tsdb_requests_total{%s}: add it to the ops table in server/metrics.c\n",
                    name, num, lab);
        if (strncmp(want, "query_", 6) == 0) /* every query op has a latency histogram */
            CHECK(!isnan(mval(m, "tsdb_query_duration_seconds_count", lab)));
        seen++;
    }
    fclose(f);
    CHECK(seen >= 20);
}

static void *metrics_load_thread(void *arg)
{
    mjob *j = arg;
    tsdbc *c;
    if (tsdbc_connect("127.0.0.1", port, &c)) {
        j->errors++;
        return NULL;
    }
    int64_t ts[M_ROWS];
    float v[M_ROWS];
    for (int round = 0; round < M_ROUNDS; round++) {
        for (int i = 0; i < M_ROWS; i++) {
            ts[i] = T0 + (int64_t)(round * M_ROWS + i) * 1000;
            v[i] = (float)i;
        }
        if (tsdbc_insert_series(c, j->id, ts, v, M_ROWS))
            j->errors++;
        if (round % (M_ROUNDS / M_QUERIES) == 0) {
            tsdb_bucket *b;
            size_t nb;
            if (tsdbc_query_agg(c, j->id, INT64_MIN, INT64_MAX, 0, &b, &nb))
                j->errors++;
            else
                free(b);
        }
    }
    tsdb_points p;
    if (tsdbc_query_points(c, j->id, INT64_MIN, INT64_MAX, &p))
        j->errors++;
    else {
        free(p.ts);
        free(p.values);
    }
    tsdbc_close(c);
    return NULL;
}

static int64_t mono_us(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

/* Slowest of n small queries on the main port, microseconds. */
static int64_t main_port_worst_us(tsdbc *c, uint32_t id, int n)
{
    int64_t worst = 0;
    for (int i = 0; i < n; i++) {
        int64_t t = mono_us();
        tsdb_bucket *b;
        size_t nb;
        if (tsdbc_query_agg(c, id, T0, T0 + 60000, 0, &b, &nb) == 0)
            free(b);
        else
            CHECK(!"main port query failed");
        int64_t d = mono_us() - t;
        if (d > worst)
            worst = d;
    }
    return worst;
}

static void test_metrics_buckets(void)
{
    CHECK(metrics_bucket_for(0) == 0 && metrics_bucket_for(0.0004) == 0 && metrics_bucket_for(0.0005) == 0);
    CHECK(metrics_bucket_for(0.0006) == 1 && metrics_bucket_for(0.001) == 1);
    CHECK(metrics_bucket_for(0.003) == 3 && metrics_bucket_for(0.1) == 7 && metrics_bucket_for(0.5) == 9);
    CHECK(metrics_bucket_for(1) == 10 && metrics_bucket_for(30) == 14); /* bounds are inclusive, as in Prometheus */
    CHECK(metrics_bucket_for(30.1) == 15 && metrics_bucket_for(1e9) == 15);
}

static void test_metrics(const char *dir)
{
    test_metrics_buckets();
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0) {
        failures++;
        return;
    }
    const char *extra[] = {"--metrics-port", "auto", "--metrics-bind", "127.0.0.1", NULL};
    pid_t srv = start_server_with(dir, extra);
    mport = th_mport; /* the port file names the metrics port of this server */
    CHECK(mport > 0 && mport != port);

    /* the endpoint: format, headers, the paths and methods it refuses */
    mset m0;
    scrape(&m0);
    CHECK(mval(&m0, "tsdb_build_info", "version=\"") == 1);
    CHECK(mval(&m0, "tsdb_role_info", "role=\"writer\"") == 1);
    const char *iob = getenv("TSDBD_IO");
    if (iob && strcmp(iob, "auto") != 0 && strcmp(iob, "uring") != 0)
        CHECK(mval(&m0, "tsdb_io_backend_info", "backend=\"epoll\"") == 1);
    CHECK(mval(&m0, "tsdb_io_backend_info", NULL) == 1);
    CHECK(mval(&m0, "tsdb_workers", NULL) == 4);
    CHECK(mval(&m0, "tsdb_connections_max", NULL) == 10000);
    CHECK(mval(&m0, "tsdb_failed", NULL) == 0);
    CHECK(mval(&m0, "tsdb_rows", "volume=\"0\"") == 0);
    CHECK(mval(&m0, "tsdb_start_time_seconds", NULL) > 1e9);
    check_ops_named(&m0);
    mset_free(&m0);
    size_t len;
    char *r = HTTP_EXCHANGE("GET /nothing HTTP/1.1\r\n\r\n", 5, &len);
    CHECK(r && http_status(r) == 404);
    free(r);
    r = HTTP_EXCHANGE("GET /metrics?x=1 HTTP/1.0\r\n\r\n", 5, &len);
    CHECK(r && http_status(r) == 200 && strstr(r, "tsdb_build_info"));
    free(r);
    r = HTTP_EXCHANGE("POST /metrics HTTP/1.1\r\nContent-Length: 4\r\n\r\nbody", 5, &len);
    CHECK(r && http_status(r) == 405);
    free(r);
    r = HTTP_EXCHANGE("HEAD /metrics HTTP/1.1\r\n\r\n", 5, &len);
    CHECK(r && http_status(r) == 200 && strstr(r, "Content-Length: ") && !strstr(r, "tsdb_build_info"));
    free(r);
    r = HTTP_EXCHANGE("\x01\xff\xfe garbage\r\n\r\n", 5, &len);
    CHECK(r && http_status(r) == 400);
    free(r);
    {
        char big[9000];
        memset(big, 'a', sizeof big);
        memcpy(big, "GET /metrics HTTP/1.1\r\nX: ", 26);
        r = http_exchange(big, sizeof big, 5, &len); /* a head that never ends */
        CHECK(r && http_status(r) == 431);
        free(r);
    }

    /* scraping while clients insert and query from several connections */
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    pthread_t th[M_THREADS];
    mjob jobs[M_THREADS];
    mset before, prev;
    scrape(&before);
    prev = (mset){0};
    for (int i = 0; i < M_THREADS; i++) {
        char sname[32];
        snprintf(sname, sizeof sname, "metrics.%d", i);
        OK(tsdbc_series(c, sname, 1, &jobs[i].id));
        jobs[i].errors = 0;
        pthread_create(&th[i], NULL, metrics_load_thread, &jobs[i]);
    }
    int scrapes_during = 0, saw_conns = 0, saw_busy = 0;
    for (int done = 0; done < 1;) {
        mset cur;
        if (scrape(&cur)) {
            scrapes_during++;
            if (prev.s)
                check_monotone(&prev, &cur);
            saw_conns += mval(&cur, "tsdb_connections", NULL) >= 2; /* mine and the load threads' */
            saw_busy += mval(&cur, "tsdb_workers_busy", NULL) >= 1;
            CHECK(mval(&cur, "tsdb_workers_busy", NULL) <= mval(&cur, "tsdb_workers", NULL));
            mset_free(&prev);
            prev = cur;
        }
        if (scrapes_during >= 400)
            break;
        done = 1;
        for (int i = 0; i < M_THREADS; i++) {
            /* a thread is finished when its series has all its rows */
            tsdb_bucket *b;
            size_t nb;
            if (tsdbc_query_agg(c, jobs[i].id, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0) {
                done &= nb == 1 && b[0].count == (uint64_t)M_ROUNDS * M_ROWS;
                free(b);
            }
        }
    }
    for (int i = 0; i < M_THREADS; i++) {
        pthread_join(th[i], NULL);
        CHECK(jobs[i].errors == 0);
    }
    CHECK(scrapes_during >= 3);
    CHECK(saw_conns >= 1);
    OK(tsdbc_flush(c));
    {   /* one request the server refuses: a counted error */
        int rfd = raw_connect();
        uint8_t short_payload[3] = {1, 2, 3};
        uint8_t rest[64];
        size_t nrest;
        uint32_t rid;
        raw_send(rfd, 7, TSDB_OP_QUERY_AGG, short_payload, sizeof short_payload);
        CHECK(raw_reply(rfd, &rid, rest, sizeof rest, &nrest) == TSDB_ERR_INVALID);
        close(rfd);
    }
    (void)saw_busy; /* busy workers are a snapshot: reported, but too fast here to rely on */

    mset after;
    scrape(&after);
    check_monotone(&before, &after);
    double rows = (double)M_THREADS * M_ROUNDS * M_ROWS;
    CHECK(mval(&after, "tsdb_inserted_rows_total", NULL) - mval(&before, "tsdb_inserted_rows_total", NULL) == rows);
    double ins = mval(&after, "tsdb_requests_total", "op=\"insert\"") + mval(&after, "tsdb_requests_total", "op=\"insert_values\"") -
                 mval(&before, "tsdb_requests_total", "op=\"insert\"") - mval(&before, "tsdb_requests_total", "op=\"insert_values\"");
    CHECK(ins == (double)M_THREADS * M_ROUNDS);
    double qcount = 0;
    for (size_t i = 0; i < after.n; i++)
        if (strcmp(after.s[i].name, "tsdb_query_duration_seconds_count") == 0)
            qcount += after.s[i].v;
    double q0 = msum(&before, "tsdb_query_duration_seconds_count");
    CHECK(qcount - q0 >= (double)M_THREADS * (M_QUERIES + 1));
    CHECK(msum(&after, "tsdb_query_duration_seconds_sum") > msum(&before, "tsdb_query_duration_seconds_sum"));
    {   /* a query of a few thousand rows takes far less than 30 s: it must not sit in the +Inf bucket,
         * and a fast one is in a bucket at or below 1 s */
        double agg_inf = mval(&after, "tsdb_query_duration_seconds_bucket", "op=\"query_agg\",le=\"+Inf\"");
        CHECK(agg_inf > 0);
        CHECK(mval(&after, "tsdb_query_duration_seconds_bucket", "op=\"query_agg\",le=\"30\"") == agg_inf);
        CHECK(mval(&after, "tsdb_query_duration_seconds_bucket", "op=\"query_agg\",le=\"1\"") == agg_inf);
        CHECK(mval(&after, "tsdb_query_duration_seconds_bucket", "op=\"query_agg\",le=\"0.0005\"") <= agg_inf);
    }
    double inv = mval(&after, "tsdb_request_errors_total", "code=\"invalid\"") - mval(&before, "tsdb_request_errors_total", "code=\"invalid\"");
    CHECK(inv == 1);
    if (inv != 1)
        fprintf(stderr, "invalid errors: %g\n", inv);
    CHECK(mval(&after, "tsdb_connections_accepted_total", NULL) - mval(&before, "tsdb_connections_accepted_total", NULL) >= M_THREADS);
    CHECK(mval(&after, "tsdb_rows", "volume=\"0\"") == rows); /* flushed: all in parts */
    CHECK(mval(&after, "tsdb_memtable_rows", "volume=\"0\"") == 0);
    CHECK(mval(&after, "tsdb_parts", "volume=\"0\"") >= 1);
    CHECK(mval(&after, "tsdb_partitions", "volume=\"0\"") >= 1);
    CHECK(mval(&after, "tsdb_granules", "volume=\"0\"") >= 1);
    CHECK(mval(&after, "tsdb_disk_bytes", "volume=\"0\"") > 0);
    CHECK(mval(&after, "tsdb_wal_bytes_total", "volume=\"0\"") >= rows * 16);
    CHECK(mval(&after, "tsdb_failed", "volume=\"0\"") == 0);
    CHECK(mval(&after, "tsdb_metrics_scrapes_total", NULL) >= scrapes_during);
    CHECK(isnan(mval(&after, "tsdb_follow_refresh_age_seconds", NULL))); /* a writer has no lag */
    mset_free(&before);
    mset_free(&prev);

    /* a client that went away is no longer a connection */
    tsdb_stats dummy;
    OK(tsdbc_stats(c, &dummy));
    tsdbc_close(c);
    double conns = 1;
    for (int i = 0; i < 100 && conns != 0; i++) {
        mset m;
        scrape(&m);
        conns = mval(&m, "tsdb_connections", NULL);
        mset_free(&m);
        if (conns != 0)
            usleep(20000);
    }
    CHECK(conns == 0);
    mset_free(&after);

    /* Hostile scrapers: connections that start a request and never end it, one that sends a byte
     * every 100 ms, and a crowd of silent ones. The main port is as fast as without them and a
     * real scrape still works, even with every slot taken. */
    OK(tsdbc_connect("127.0.0.1", port, &c));
    uint32_t sid;
    OK(tsdbc_series(c, "metrics.0", 0, &sid));
    int64_t base_worst = main_port_worst_us(c, sid, 300);
    enum { SLOW = 30, SILENT = 100 };
    int slow[SLOW + SILENT];
    for (int i = 0; i < SLOW + SILENT; i++) {
        slow[i] = metrics_connect();
        CHECK(slow[i] >= 0);
        if (i < SLOW && slow[i] >= 0)
            CHECK(send(slow[i], "GET /metrics HTTP/1.1\r\nHost: x\r\n", 32, MSG_NOSIGNAL) == 32);
    }
    int drip = metrics_connect();
    int64_t t_start = mono_us();
    int dripped = 0;
    int64_t worst = 0;
    mset m;
    while (mono_us() - t_start < 1500000) {
        if (drip >= 0 && send(drip, "G", 1, MSG_NOSIGNAL) == 1)
            dripped++;
        int64_t w = main_port_worst_us(c, sid, 20);
        if (w > worst)
            worst = w;
        usleep(100000);
    }
    CHECK(dripped >= 5);
    CHECK(worst < 500000 || worst < 20 * base_worst); /* no stall: a query takes microseconds, allow a noisy machine */
    int64_t t0s = mono_us();
    CHECK(scrape(&m));
    CHECK(mono_us() - t0s < 1000000); /* served at once, not after the idle ones time out */
    CHECK(mval(&m, "tsdb_metrics_rejected_total", NULL) >= 1); /* the crowd pushed out the oldest */
    mset_free(&m);
    /* those that never finished are closed by the server, without a reply, within the deadline */
    int closed = 0;
    for (int i = 0; i < SLOW + SILENT; i++) {
        if (slow[i] < 0)
            continue;
        struct timeval tv = {4, 0};
        setsockopt(slow[i], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        char b1[64];
        ssize_t rr = recv(slow[i], b1, sizeof b1, 0);
        closed += rr == 0 || (rr < 0 && errno == ECONNRESET);
    }
    CHECK(closed == SLOW + SILENT);
    if (drip >= 0) {
        char b1[64];
        struct timeval tv = {4, 0};
        setsockopt(drip, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        CHECK(recv(drip, b1, sizeof b1, 0) <= 0);
    }
    for (int i = 0; i < SLOW + SILENT; i++)
        if (slow[i] >= 0)
            close(slow[i]);
    if (drip >= 0)
        close(drip);
    CHECK(main_port_worst_us(c, sid, 10) < 500000);
    CHECK(scrape(&m));
    mset_free(&m);

    /* Stopping the server with a half-finished scrape connection open is as quick as ever. */
    int hang = metrics_connect();
    if (hang >= 0)
        CHECK(send(hang, "GET /met", 8, MSG_NOSIGNAL) == 8);
    tsdbc_close(c);
    int64_t ts0 = mono_us();
    stop_server(srv);
    CHECK(mono_us() - ts0 < 3000000);
    if (hang >= 0)
        close(hang);
    CHECK(metrics_connect() < 0); /* the port is closed with the server */

    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (!failures && system(cmd) != 0)
        failures++;
}

/* The parts of the metrics that need their own server: the connection limit, a volume path that needs
 * label escaping, error codes other than "invalid", merges. */
static void test_metrics_more(const char *dir)
{
    char cmd[2400], vol[1100];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0) {
        failures++;
        return;
    }
    const char *extra[] = {"--metrics-port", "auto", "--metrics-bind", "127.0.0.1", "--max-connections", "6", NULL};
    pid_t srv = start_server_with(dir, extra);
    mport = th_mport;
    CHECK(mport > 0 && mport != port);
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));

    /* a bad or unavailable --metrics-port is an error at startup, not a silent gap in monitoring */
    {
        const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
        const char *bad[] = {"abc", "12x", "0", "70000", "-1", ""};
        for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
            snprintf(cmd, sizeof cmd, "'%s' --dir '%s.bad' --port 0 --metrics-port '%s' >/dev/null 2>&1", bin, dir,
                     bad[i]);
            int st = system(cmd);
            CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 2);
        }
        snprintf(cmd, sizeof cmd, "'%s' --dir '%s.bad' --port 0 --metrics-port %d >/dev/null 2>&1", bin, dir,
                 mport); /* the port the running server holds */
        int st = system(cmd);
        CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 1);
        snprintf(cmd, sizeof cmd, "rm -rf '%s.bad'", dir);
        if (system(cmd) != 0)
            failures++;
    }

    /* a volume whose directory name has a double quote and a backslash: the label must be escaped
     * (Prometheus drops a whole scrape with a malformed line) */
    snprintf(vol, sizeof vol, "%s.vol\"q\\z", dir);
    uint32_t vid, sid;
    OK(tsdbc_series_at(c, "weird", vol, &vid));
    OK(tsdbc_series(c, "plain", 1, &sid));
    {
        mset m;
        CHECK(scrape(&m)); /* the strict parser rejects an unescaped quote */
        CHECK(mval(&m, "tsdb_rows", "volume=\"1\"") == 0);
        bool escaped = false;
        for (size_t i = 0; i < m.n; i++)
            escaped |= strcmp(m.s[i].name, "tsdb_rows") == 0 && strstr(m.s[i].labels, ".vol\\\"q\\\\z\"") != NULL;
        CHECK(escaped);
        mset_free(&m);
    }

    /* merges: several parts in a partition, then optimize */
    for (int k = 0; k < 4; k++) {
        int64_t ts[100];
        float v[100];
        for (int i = 0; i < 100; i++) {
            ts[i] = 1700000000000 + (int64_t)(k * 100 + i) * 1000;
            v[i] = (float)i;
        }
        OK(tsdbc_insert_series(c, sid, ts, v, 100));
        OK(tsdbc_flush(c));
    }
    OK(tsdbc_optimize(c)); /* (the background merger may have started on its own: only the end state is certain) */
    {
        mset m;
        CHECK(scrape(&m));
        CHECK(mval(&m, "tsdb_merges_total", "volume=\"0\"") >= 1);
        CHECK(mval(&m, "tsdb_merged_rows_total", "volume=\"0\"") >= 400);
        CHECK(mval(&m, "tsdb_parts", "volume=\"0\"") == 1);
        mset_free(&m);
    }

    /* error codes: an unknown series and too many buckets, each counted under its own name */
    {
        mset b4;
        scrape(&b4);
        tsdb_bucket *bk;
        size_t nb;
        uint32_t nid;
        int rc1 = tsdbc_series(c, "no such series", 0, &nid);
        CHECK(rc1 == TSDB_ERR_NOTFOUND);
        /* data 200 000 s apart, 1 ms buckets: over the limit of 64 Mi buckets */
        int64_t far_ts[2] = {1800000000000, 1800000000000 + 200000000};
        float far_v[2] = {1, 2};
        OK(tsdbc_insert_series(c, sid, far_ts, far_v, 2));
        int rc2 = tsdbc_query_agg(c, sid, 0, INT64_MAX / 2, 1, &bk, &nb);
        CHECK(rc2 == TSDB_ERR_TOO_MANY_BUCKETS);
        mset af;
        scrape(&af);
        double all = 0;
        for (size_t i = 0; i < af.n; i++)
            if (strcmp(af.s[i].name, "tsdb_request_errors_total") == 0)
                all += af.s[i].v;
        for (size_t i = 0; i < b4.n; i++)
            if (strcmp(b4.s[i].name, "tsdb_request_errors_total") == 0)
                all -= b4.s[i].v;
        CHECK(all == 2);
        CHECK(mval(&af, "tsdb_request_errors_total", "code=\"too_many_buckets\"") -
                  mval(&b4, "tsdb_request_errors_total", "code=\"too_many_buckets\"") == 1);
        CHECK(mval(&af, "tsdb_request_errors_total", "code=\"notfound\"") -
                  mval(&b4, "tsdb_request_errors_total", "code=\"notfound\"") == 1);
        mset_free(&b4);
        mset_free(&af);
    }

    /* the connection limit: connections over --max-connections are closed and counted */
    int extra_fd[12];
    for (int i = 0; i < 12; i++) {
        extra_fd[i] = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
        inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
        CHECK(connect(extra_fd[i], (struct sockaddr *)&sa, sizeof sa) == 0);
    }
    double rej = 0;
    for (int i = 0; i < 100 && rej < 6; i++) { /* 12 more than the 5 free slots: at least 6 are turned away */
        mset m;
        scrape(&m);
        rej = mval(&m, "tsdb_connections_rejected_total", NULL);
        CHECK(mval(&m, "tsdb_connections", NULL) <= 6);
        mset_free(&m);
        if (rej < 6)
            usleep(20000);
    }
    CHECK(rej >= 6);
    for (int i = 0; i < 12; i++)
        close(extra_fd[i]);
    tsdbc_close(c);
    stop_server(srv);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'", dir, vol);
    if (!failures && system(cmd) != 0)
        failures++;
}

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdbd-test-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    char cmd[1300];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s.volume'", dir, dir);
    if (system(cmd) != 0)
        return 1;
    pid_t srv = start_server(dir);

    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    uint32_t a, b, again;
    OK(tsdbc_series(c, "tank.level_bar", 1, &a));
    OK(tsdbc_series(c, "pump.flow_m3h", 1, &b));
    OK(tsdbc_series(c, "tank.level_bar", 0, &again));
    CHECK(again == a && a != b);
    CHECK(tsdbc_series(c, "missing", 0, &again) == TSDB_ERR_NOTFOUND);
    char name[64];
    OK(tsdbc_series_name(c, b, name, sizeof name));
    CHECK(strcmp(name, "pump.flow_m3h") == 0);
    char vol[600];
    snprintf(vol, sizeof vol, "%s.volume", dir);
    uint32_t onvol;
    OK(tsdbc_series_at(c, "remote.on_volume", vol, &onvol));
    int64_t vt = T0;
    float vv = 3.5f;
    OK(tsdbc_insert_series(c, onvol, &vt, &vv, 1));
    tsdb_points vp;
    OK(tsdbc_query_points(c, onvol, INT64_MIN, INT64_MAX, &vp));
    CHECK(vp.n == 1 && vp.values[0] == 3.5f);
    free(vp.ts);
    free(vp.values);

    /* one series, large enough to stream in several chunks */
    enum { N = 200000 };
    int64_t *ts = malloc(N * sizeof *ts);
    float *v = malloc(N * sizeof *v);
    for (int i = 0; i < N; i++) {
        ts[i] = T0 + (int64_t)i * 1000;
        v[i] = (float)(i % 1000) / 10.0f;
    }
    OK(tsdbc_insert_series(c, a, ts, v, N));

    /* rows of several series in one batch, and a pipelined rewrite of the same keys */
    uint32_t s2[3] = {b, b, a};
    int64_t t2[3] = {T0, T0 + 1000, T0};
    float v2[3] = {1.5f, 2.5f, -7.0f};
    OK(tsdbc_insert_async(c, s2, t2, v2, 3));
    v2[1] = 9.0f;
    OK(tsdbc_insert_async(c, s2, t2, v2, 3));
    OK(tsdbc_wait(c));

    tsdb_points p;
    OK(tsdbc_query_points(c, a, INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == N);
    CHECK(p.n > 0 && p.values[0] == -7.0f); /* replaced through the network too */
    bool same = true;
    for (size_t i = 1; i < p.n; i++)
        same &= p.ts[i] == ts[i] && p.values[i] == v[i];
    CHECK(same);
    free(p.ts);
    free(p.values);
    OK(tsdbc_query_points(c, b, INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == 2 && p.values[1] == 9.0f);
    free(p.ts);
    free(p.values);

    /* aggregates against a local computation */
    tsdb_bucket *bk;
    size_t nb;
    OK(tsdbc_query_agg(c, a, T0, T0 + 3600000, 60000, &bk, &nb));
    CHECK(nb == 60);
    bool agg_ok = true;
    for (size_t k = 0; k < nb; k++) {
        double sum = 0;
        float mn = INFINITY, mx = -INFINITY;
        for (int i = (int)k * 60; i < (int)k * 60 + 60; i++) {
            float x = i == 0 ? -7.0f : v[i];
            sum += x;
            mn = fminf(mn, x);
            mx = fmaxf(mx, x);
        }
        agg_ok &= bk[k].start == T0 + (int64_t)k * 60000 && bk[k].count == 60 && fabs(bk[k].sum - sum) < 1e-6 &&
                  bk[k].min == mn && bk[k].max == mx;
    }
    CHECK(agg_ok);
    free(bk);

    /* newest first with a limit, and calendar grouping in a time zone, over the wire */
    tsdb_query_opts desc = {TSDB_DESC, 5};
    OK(tsdbc_query_points_ex(c, a, INT64_MIN, INT64_MAX, &desc, &p));
    CHECK(p.n == 5 && p.ts[0] == ts[N - 1] && p.ts[4] == ts[N - 5]);
    free(p.ts);
    free(p.values);
    tsdb_group day = {TSDB_DAY, 1, "Europe/Oslo"};
    tsdb_query_opts last2 = {TSDB_DESC, 2};
    OK(tsdbc_query_group(c, a, INT64_MIN, INT64_MAX, &day, &last2, &bk, &nb));
    CHECK(nb == 2 && bk[0].start > bk[1].start && bk[0].start - bk[1].start == 86400000);
    free(bk);

    /* stopping a streamed query early leaves the connection usable */
    counter k = {0, 0, 1, 0, true};
    CHECK(tsdbc_query_raw(c, a, INT64_MIN, INT64_MAX, count_rows, &k) == TSDB_ERR_STOPPED);
    CHECK(k.calls == 1 && k.rows < N);
    k = (counter){0, 0, 0, 0, true};
    OK(tsdbc_query_raw(c, a, T0 + 5000, T0 + 15000, count_rows, &k));
    CHECK(k.rows == 10 && k.ordered);
    CHECK(tsdbc_query_agg(c, a, T0, T0 + 1, -5, &bk, &nb) == TSDB_ERR_INVALID);

    tsdb_stats st;
    OK(tsdbc_stats(c, &st));
    CHECK(st.rows + st.memtable_rows >= (uint64_t)N + 2);
    OK(tsdbc_flush(c));
    OK(tsdbc_commit(c));

    /* concurrent clients, pipelining */
    enum { NC = 8 };
    pthread_t th[NC];
    cjob jobs[NC];
    for (int i = 0; i < NC; i++) {
        char sname[32];
        snprintf(sname, sizeof sname, "vib.%d", i);
        OK(tsdbc_series(c, sname, 1, &jobs[i].id));
        jobs[i].errors = 0;
        pthread_create(&th[i], NULL, client_thread, &jobs[i]);
    }
    for (int i = 0; i < NC; i++) {
        pthread_join(th[i], NULL);
        CHECK(jobs[i].errors == 0);
    }

    /* garbage on another connection is dropped; the server stays up */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    CHECK(connect(fd, (struct sockaddr *)&sa, sizeof sa) == 0);
    uint8_t junk[16] = {0xff, 0xff, 0xff, 0xff, 1, 2, 3};
    CHECK(write(fd, junk, sizeof junk) == (ssize_t)sizeof junk);
    char tmp[16];
    CHECK(read(fd, tmp, sizeof tmp) <= 0); /* closed by the server */
    close(fd);

    test_close(c);
    test_framing();
    test_hello();
    test_old_server(8);
    test_old_server(6);
    test_old_server(5);
    test_types(c);
    test_nan(c);
    test_lttb_remote(c);
    test_pack_roundtrip();
    test_packed_inserts(c);
    test_bounds(c, a, N);
    test_twa(c);
    test_limits_remote(c);
    test_spread_remote(c);
    test_spread_old_server();
    test_delete(c);

    OK(tsdbc_stats(c, &st));
    tsdbc_close(c);

    stop_server(srv);

    /* Regression: the listening socket outlived a stopped server for a moment
     * (io_uring held it), so a server restarted at once could not bind, or
     * clients reached the old listener and lost their connection. */
    for (int i = 0; i < 5; i++) {
        srv = start_server(dir);
        tsdbc *c3;
        OK(tsdbc_connect("127.0.0.1", port, &c3));
        CHECK(count_of(c3, a) == N);
        tsdbc_close(c3);
        stop_server(srv);
    }


    /* everything is on disk */
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1;
    tsdb *db;
    if (tsdb_open(dir, &o, &db) == 0) {
        tsdb_bucket *bb;
        size_t n2;
        CHECK(tsdb_query_agg(db, a, INT64_MIN, INT64_MAX, 0, &bb, &n2) == 0 && n2 == 1 && bb[0].count == N);
        free(bb);
        tsdb_close(db);
    } else {
        CHECK(!"reopen");
    }
    if (system(cmd) != 0)
        return 1;
    free(ts);
    free(v);

    printf("concurrent writes to the same keys\n");
    snprintf(dir, sizeof dir, "%s/tsdbd-samekeys-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    test_same_keys(dir);
    printf("fail-stop after a WAL write error\n");
    snprintf(dir, sizeof dir, "%s/tsdbd-failstop-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    test_wal_failstop(dir);
    printf("prometheus metrics\n");
    snprintf(dir, sizeof dir, "%s/tsdbd-metrics-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    test_metrics(dir);
    snprintf(dir, sizeof dir, "%s/tsdbd-metrics2-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    test_metrics_more(dir);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("server tests passed\n");
    return 0;
}
