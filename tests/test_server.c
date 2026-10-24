/* Integration test: starts build/tsdbd and talks to it through the client library. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_pack.h"
#include "tsdb_protocol.h"
#include "samekeys.h"

#include <arpa/inet.h>
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

static int port;
static const int64_t T0 = 1699999980000; /* on a minute boundary: buckets below are whole */

/* When set, the next server runs under the crashfs shim in eio mode: SIGUSR2
 * makes its next write or sync of a WAL segment fail, and the shim writes the
 * failed file's path to this file. */
static const char *eio_mark;

/* extra: more arguments, NULL-terminated, or NULL */
static pid_t start_server_with(const char *dir, const char *const *extra)
{
    char portstr[16], shim[PATH_MAX], near[PATH_MAX];
    snprintf(portstr, sizeof portstr, "%d", port);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd"; /* e.g. a sanitizer build */
    /* the shim of the same build, next to the server */
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
        const char *argv[32] = {"tsdbd", "--dir", dir, "--port", portstr, "--workers", "4", "--no-fsync"};
        int n = eio_mark ? 7 : 8; /* under the shim, syncs are what make data survive its rollback at exit */
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
    for (int i = 0; i < 100; i++) { /* wait until it accepts */
        tsdbc *c;
        if (tsdbc_connect("127.0.0.1", port, &c) == 0) {
            tsdbc_close(c);
            return pid;
        }
        usleep(20000);
    }
    fprintf(stderr, "tsdbd did not start\n");
    exit(1);
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
        for (uint32_t s = 0; s < (kind == SK_SERIES ? c->nseries : 1); s++) {
            sk_batch b;
            bool large = kind == SK_LARGE || (kind == SK_SERIES && (seed >> 24) % 2);
            sk_build(c, x->ids, w, r, large, (int)(seed % 3), seed, kind == SK_SERIES ? s : UINT32_MAX, &b);
            tsdb_values col = {(tsdb_type)c->type, b.v, c->nulls ? b.valid : NULL};
            const float *f = (const float *)(void *)b.v;
            if (kind == SK_SERIES)
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
    enum { NC = 5 };
    static sk_cfg c[NC];
    c[0] = (sk_cfg){"float32, same keys", 6, 1000, 3, 80, 0, t0, 97000, TSDB_FLOAT32, false, batches};
    c[1] = (sk_cfg){"int64 + nulls, same keys", 6, 1000, 3, 80, 0, t0, 97000, TSDB_INT64, true, batches};
    c[2] = (sk_cfg){"float32, shifted keys", 6, 1000, 3, 80, 20, t0, 97000, TSDB_FLOAT32, false, batches};
    c[3] = (sk_cfg){"float64 + nulls, per series", 6, 600, 3, 80, 37, t0, 97000, TSDB_FLOAT64, true,
                    batches | SK_SERIES};
    c[4] = (sk_cfg){"float32, then SIGKILL", 5, 600, 3, 80, 10, t0, 97000, TSDB_FLOAT32, false, batches};
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

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdbd-test-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    port = 10000 + (int)(getpid() % 20000); /* below the ephemeral range, where client sockets take ports */
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
    test_types(c);
    test_pack_roundtrip();
    test_packed_inserts(c);
    test_bounds(c, a, N);

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
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("server tests passed\n");
    return 0;
}
