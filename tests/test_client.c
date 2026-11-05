/* The C client against fake servers in this process: replies out of step, a
 * reply that comes after the caller gave up, malformed name replies, a stream
 * cut short, a series whose value type changes between chunks (a heap overflow
 * of the multi collector once), a timeout inside a large frame, a server that hangs up, the
 * keepalive defaults, and a connect that would otherwise wait for the kernel. Nothing
 * here starts tsdbd; the fake servers listen on port 0 and hand out the port
 * they got. */
#define _GNU_SOURCE
#include "tsdb_client.h"
#include "tsdb_pack.h"
#include "tsdb_protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

/* ------------------------------------------------------------ fake server */

typedef void (*script_fn)(int fd);

struct fake {
    int lfd, port;
    script_fn script;
    pthread_t th;
};

static bool read_req(int fd, uint32_t *id, uint8_t *op, uint8_t *body, size_t cap, size_t *blen);
static void reply(int fd, uint32_t id, int32_t status, const void *body, size_t n);

/* The client says HELLO at connect: the fake answers it with its own version and no login (protocol 12: u16 flags,
 * u16 mechanisms), then runs the script. */
static void *fake_main(void *arg)
{
    struct fake *f = arg;
    int fd = accept(f->lfd, NULL, NULL);
    if (fd >= 0) {
        uint8_t body[16], op;
        uint32_t id, v = TSDB_PROTOCOL_VERSION;
        uint8_t h[8] = {0};
        memcpy(h, &v, 4);
        size_t n;
        if (read_req(fd, &id, &op, body, sizeof body, &n) && op == TSDB_OP_HELLO) {
            reply(fd, id, 0, h, sizeof h);
            f->script(fd);
        }
        close(fd);
    }
    return NULL;
}

static void fake_start(struct fake *f, script_fn script)
{
    f->lfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t al = sizeof a;
    if (f->lfd < 0 || bind(f->lfd, (struct sockaddr *)&a, sizeof a) != 0 || listen(f->lfd, 4) != 0 ||
        getsockname(f->lfd, (struct sockaddr *)&a, &al) != 0) {
        perror("fake server");
        exit(2);
    }
    f->port = ntohs(a.sin_port);
    f->script = script;
    pthread_create(&f->th, NULL, fake_main, f);
}

static void fake_stop(struct fake *f)
{
    pthread_join(f->th, NULL);
    close(f->lfd);
}

static bool read_all(int fd, void *p, size_t n)
{
    uint8_t *o = p;
    while (n) {
        ssize_t r = read(fd, o, n);
        if (r <= 0)
            return false;
        o += r;
        n -= (size_t)r;
    }
    return true;
}

/* Reads one request; returns its id and op, the body (up to cap bytes) in body. */
static bool read_req(int fd, uint32_t *id, uint8_t *op, uint8_t *body, size_t cap, size_t *blen)
{
    uint8_t h[TSDB_FRAME_HEADER];
    if (!read_all(fd, h, sizeof h))
        return false;
    uint32_t len;
    memcpy(&len, h, 4);
    memcpy(id, h + 4, 4);
    *op = h[8];
    size_t n = len - 5;
    if (n > cap)
        return false;
    *blen = n;
    return read_all(fd, body, n);
}

/* MSG_NOSIGNAL: a client that marked the connection broken closes it, and the
 * fake's next frame must not kill the test with SIGPIPE. SIGPIPE stays at its
 * default action in this process, so test_peer_closed sees the client's own. */
static bool send_all(int fd, const void *p, size_t n)
{
    const uint8_t *o = p;
    while (n) {
        ssize_t w = send(fd, o, n, MSG_NOSIGNAL);
        if (w <= 0)
            return false;
        o += w;
        n -= (size_t)w;
    }
    return true;
}

static void reply(int fd, uint32_t id, int32_t status, const void *body, size_t n)
{
    uint8_t h[13];
    uint32_t len = (uint32_t)(9 + n);
    memcpy(h, &len, 4);
    memcpy(h + 4, &id, 4);
    h[8] = TSDB_OP_REPLY;
    memcpy(h + 9, &status, 4);
    if (!send_all(fd, h, sizeof h) || (n && !send_all(fd, body, n)))
        return; /* the client hung up */
}

/* Answers each SERIES request with the next id (100, 101, ...), the first one
 * only after 500 ms: a slow server, not a malicious one. */
static void slow_series(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint64_t next = 100;
    uint8_t op;
    for (int i = 0; read_req(fd, &id, &op, body, sizeof body, &n); i++) {
        if (i == 0)
            usleep(500 * 1000);
        reply(fd, id, 0, &next, 8);
        next++;
    }
}

/* A call that timed out leaves its reply on the way: the next call must not
 * take it for its own (it would get pump1's id for pump2, and every insert for
 * pump2 would land in pump1). */
static void test_timeout_breaks_connection(void)
{
    struct fake f;
    fake_start(&f, slow_series);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    CHECK(tsdbc_set_timeout(c, 200) == 0);
    tsdb_sid id = 0;
    CHECK(tsdbc_series(c, "pump1.flow", 1, &id) == TSDB_ERR_IO);
    usleep(600 * 1000); /* the late reply has arrived by now */
    id = 0;
    int rc = tsdbc_series(c, "pump2.flow", 1, &id);
    CHECK(rc == TSDB_ERR_IO);
    CHECK(id == 0);
    CHECK(tsdbc_wait(c) == TSDB_ERR_IO);
    tsdbc_close(c);
    fake_stop(&f);
}

/* A reply with the wrong request id: protocol error, and the connection stays broken. */
static void wrong_id(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint64_t sid = 7;
    uint8_t op;
    while (read_req(fd, &id, &op, body, sizeof body, &n))
        reply(fd, id + 5, 0, &sid, 8);
}

static void test_reply_id_checked(void)
{
    struct fake f;
    fake_start(&f, wrong_id);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    tsdb_sid id = 0;
    CHECK(tsdbc_series(c, "a", 1, &id) == TSDB_ERR_CORRUPT);
    CHECK(id == 0);
    CHECK(tsdbc_series(c, "b", 1, &id) == TSDB_ERR_IO);
    tsdbc_close(c);
    fake_stop(&f);
}

/* SERIES_NAME replies: one byte; a name with NUL; a name longer than the buffer. */
static void bad_names(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint8_t op;
    for (int i = 0; read_req(fd, &id, &op, body, sizeof body, &n); i++) {
        if (i == 0) {
            uint8_t one = 5;
            reply(fd, id, 0, &one, 1);
        } else if (i == 1) {
            uint8_t b[] = {3, 0, 'a', 0, 'b'};
            reply(fd, id, 0, b, sizeof b);
        } else {
            uint8_t b[] = {6, 0, 'p', 'u', 'm', 'p', '1', '0'};
            reply(fd, id, 0, b, sizeof b);
        }
    }
}

static void test_name_replies(void)
{
    struct fake f;
    fake_start(&f, bad_names);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    char buf[64];
    CHECK(tsdbc_series_name(c, 1, buf, sizeof buf) == TSDB_ERR_CORRUPT);
    CHECK(tsdbc_series_name(c, 1, buf, sizeof buf) == TSDB_ERR_CORRUPT); /* complete frame: still in step */
    char small[6];                                                        /* "pump1" fits, "pump10" must not be cut to it */
    CHECK(tsdbc_series_name(c, 1, small, sizeof small) == TSDB_ERR_INVALID);
    CHECK(tsdbc_series_name(c, 1, buf, sizeof buf) == 0 && strcmp(buf, "pump10") == 0);
    tsdbc_close(c);
    fake_stop(&f);
}

/* A raw query stream whose second frame is malformed: the rest of the stream
 * may follow, so the connection is broken; the next call must not read it. */
static void bad_stream(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint8_t op;
    if (!read_req(fd, &id, &op, body, sizeof body, &n))
        return;
    uint8_t f1[5 + 12] = {0, 1, 0, 0, 0};
    reply(fd, id, 0, f1, sizeof f1);
    uint8_t f2[5] = {0, 9, 0, 0, 0}; /* claims 9 rows, holds none */
    reply(fd, id, 0, f2, sizeof f2);
    uint8_t f3[5] = {1, 0, 0, 0, 0};
    reply(fd, id, 0, f3, sizeof f3);
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        uint64_t sid = 3;
        reply(fd, id, 0, &sid, 8);
    }
}

static int count_rows(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts;
    (void)v;
    *(size_t *)ctx += n;
    return 0;
}

static void test_stream_cut(void)
{
    struct fake f;
    fake_start(&f, bad_stream);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    size_t rows = 0;
    CHECK(tsdbc_query_raw(c, 1, 0, 100, count_rows, &rows) == TSDB_ERR_CORRUPT);
    CHECK(rows == 1);
    tsdb_sid id = 0;
    CHECK(tsdbc_series(c, "x", 1, &id) == TSDB_ERR_IO);
    tsdbc_close(c);
    fake_stop(&f);
}

/* A well-behaved stream of several frames followed by a plain call: the
 * request ids line up, no false alarm. */
static void good_stream(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint8_t op;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        if (op == TSDB_OP_QUERY_RAW) {
            uint8_t fr[5 + 12] = {0, 1, 0, 0, 0};
            reply(fd, id, 0, fr, sizeof fr);
            reply(fd, id, 0, fr, sizeof fr);
            fr[0] = 1;
            reply(fd, id, 0, fr, sizeof fr);
        } else {
            uint64_t sid = 42;
            reply(fd, id, 0, &sid, 8);
        }
    }
}

static void test_stream_in_step(void)
{
    struct fake f;
    fake_start(&f, good_stream);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    for (int i = 0; i < 3; i++) {
        size_t rows = 0;
        CHECK(tsdbc_query_raw(c, 1, 0, 100, count_rows, &rows) == 0);
        CHECK(rows == 3);
        tsdb_sid id = 0;
        CHECK(tsdbc_series(c, "x", 1, &id) == 0 && id == 42);
    }
    tsdbc_close(c);
    fake_stop(&f);
}

/* A listener whose accept queue is full drops further SYNs, as a host that is
 * gone does: the connect gives up after its timeout instead of the kernel's
 * two minutes of retries. */
static void test_connect_timeout(void)
{
    int lfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t al = sizeof a;
    if (lfd < 0 || bind(lfd, (struct sockaddr *)&a, sizeof a) != 0 || listen(lfd, 0) != 0 ||
        getsockname(lfd, (struct sockaddr *)&a, &al) != 0) {
        perror("listener");
        exit(2);
    }
    int fill[4];
    for (int i = 0; i < 4; i++) { /* fill the queue; the later ones hang in SYN_SENT */
        fill[i] = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        connect(fill[i], (struct sockaddr *)&a, sizeof a);
    }
    usleep(100 * 1000);
    tsdbc *c = NULL;
    double t0 = now_s();
    int rc = tsdbc_connect_timeout("127.0.0.1", ntohs(a.sin_port), 300, &c);
    double el = now_s() - t0;
    if (rc == 0) { /* SYN cookies or a kernel that queues more: nothing to measure here */
        fprintf(stderr, "test_connect_timeout: the kernel accepted the connect; skipped\n");
        tsdbc_close(c);
    } else {
        CHECK(rc == TSDB_ERR_IO);
        CHECK(el >= 0.25 && el < 3.0);
    }
    CHECK(tsdbc_connect_timeout("127.0.0.1", 1, -1, &c) == TSDB_ERR_INVALID);
    for (int i = 0; i < 4; i++)
        close(fill[i]);
    close(lfd);
}

/* A raw frame larger than the client's 1 MiB read buffer, which goes straight
 * into place: half of it, a pause past the client's timeout, the rest, then
 * answers to anything else. */
#define BIG_ROWS (200 * 1000)
static void big_slow_frame(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint8_t op;
    if (!read_req(fd, &id, &op, body, sizeof body, &n))
        return;
    size_t blen = 5 + (size_t)BIG_ROWS * 12;
    uint8_t *f = calloc(1, 13 + blen);
    uint32_t len = (uint32_t)(9 + blen), rows = BIG_ROWS;
    memcpy(f, &len, 4);
    memcpy(f + 4, &id, 4);
    f[8] = TSDB_OP_REPLY;
    f[13] = 1;
    memcpy(f + 14, &rows, 4);
    size_t half = (13 + blen) / 2;
    send_all(fd, f, half);
    usleep(500 * 1000);
    send_all(fd, f + half, 13 + blen - half);
    free(f);
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        uint64_t sid = 7;
        reply(fd, id, 0, &sid, 8);
    }
}

/* A timeout in the middle of a large body leaves the rest of it on the way: the
 * connection is broken, not read from mid-frame. */
static void test_timeout_in_large_body(void)
{
    struct fake f;
    fake_start(&f, big_slow_frame);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    CHECK(tsdbc_set_timeout(c, 200) == 0);
    size_t rows = 0;
    CHECK(tsdbc_query_raw(c, 1, 0, 100, count_rows, &rows) == TSDB_ERR_IO);
    usleep(600 * 1000);
    tsdb_sid id = 0;
    CHECK(tsdbc_series(c, "x", 1, &id) == TSDB_ERR_IO);
    CHECK(id == 0);
    tsdbc_close(c);
    fake_stop(&f);
}

/* QUERY_VALUES streams of several frames, each followed by a plain call: the
 * stream's frames all count as one reply. */
static void good_values(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint8_t op;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        if (op == TSDB_OP_QUERY_VALUES) {
            uint8_t fr[8 + 12] = {0, TSDB_FLOAT32, 0, 0, 1, 0, 0, 0};
            for (int i = 0; i < 3; i++) {
                fr[0] = i == 2;
                int64_t ts = i;
                memcpy(fr + 8, &ts, 8);
                reply(fd, id, 0, fr, sizeof fr);
            }
        } else {
            uint64_t sid = 42;
            reply(fd, id, 0, &sid, 8);
        }
    }
}

static int count_values(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts;
    (void)v;
    *(size_t *)ctx += n;
    return 0;
}

static void test_values_stream_in_step(void)
{
    struct fake f;
    fake_start(&f, good_values);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    for (int i = 0; i < 3; i++) {
        size_t rows = 0;
        CHECK(tsdbc_query_values(c, 1, 0, 100, NULL, count_values, &rows) == 0);
        CHECK(rows == 3);
        tsdb_sid id = 0;
        CHECK(tsdbc_series(c, "x", 1, &id) == 0 && id == 42);
    }
    tsdbc_close(c);
    fake_stop(&f);
}

/* One QUERY_VALUES / LTTB chunk of n rows of `type` (timestamps 0 .. n-1, every value byte `fill`), after the u32
 * index of a QUERY_MULTI series unless ix is NULL. */
static void values_chunk(int fd, uint32_t id, const uint32_t *ix, bool last, uint8_t type, uint32_t n, uint8_t fill)
{
    size_t pre = ix ? TSDB_MULTI_PREFIX : 0, aw = tsdb_wire_width(type), len = pre + 8 + (size_t)n * (8 + aw);
    uint8_t *b = calloc(len, 1);
    if (!b)
        return;
    if (ix)
        memcpy(b, ix, 4);
    b[pre] = last;
    b[pre + 1] = type;
    memcpy(b + pre + 4, &n, 4);
    for (uint32_t i = 0; i < n; i++) {
        int64_t ts = i;
        memcpy(b + pre + 8 + (size_t)i * 8, &ts, 8);
    }
    memset(b + pre + 8 + (size_t)n * 8, fill, (size_t)n * aw);
    reply(fd, id, 0, b, len);
    free(b);
}

static void multi_end(int fd, uint32_t id, uint32_t answered)
{
    uint32_t b[2] = {TSDB_MULTI_END, answered};
    reply(fd, id, 0, b, sizeof b);
}

/* A QUERY_MULTI whose series 0 changes type between its chunks: one bool row, then 1000 float64 rows. tsdbd never
 * sends that (a series' type is fixed at creation), but a hostile server could: the collector sized the series'
 * buffer for 1024 bools, and the float64 rows are 8000 bytes. Then a QUERY_VALUES stream that does the same. */
static void type_switch(int fd)
{
    uint8_t body[4096];
    size_t n;
    uint32_t id;
    uint8_t op;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        if (op == TSDB_OP_QUERY_MULTI) {
            uint32_t ix = 0;
            values_chunk(fd, id, &ix, false, TSDB_BOOL, 1, 1);
            values_chunk(fd, id, &ix, true, TSDB_FLOAT64, 1000, 0x41);
            multi_end(fd, id, 1);
        } else if (op == TSDB_OP_QUERY_VALUES) {
            values_chunk(fd, id, NULL, false, TSDB_FLOAT32, 1, 0);
            values_chunk(fd, id, NULL, true, TSDB_FLOAT64, 4, 0x41);
        } else {
            uint64_t sid = 42;
            reply(fd, id, 0, &sid, 8);
        }
    }
}

/* Series of different types in one QUERY_MULTI, the first in two chunks: each keeps its own type. */
static void multi_types(int fd)
{
    uint8_t body[4096];
    size_t n;
    uint32_t id;
    uint8_t op;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        if (op == TSDB_OP_QUERY_MULTI) {
            uint32_t ix = 0;
            values_chunk(fd, id, &ix, false, TSDB_BOOL, 1, 1);
            values_chunk(fd, id, &ix, true, TSDB_BOOL, 3, 1);
            ix = 1;
            values_chunk(fd, id, &ix, true, TSDB_FLOAT64, 1000, 0x41);
            multi_end(fd, id, 2);
        } else {
            uint64_t sid = 42;
            reply(fd, id, 0, &sid, 8);
        }
    }
}

static int values_types(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts;
    (void)n;
    *(unsigned *)ctx |= 1u << v->type;
    return 0;
}

static void test_type_switch(void)
{
    tsdb_sid s[2] = {TSDB_SID(0, 0), TSDB_SID(0, 1)};
    tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = INT64_MIN, .end = INT64_MAX};
    struct fake f;
    tsdbc *c;
    tsdb_sid id = 0;

    /* a type switch within one series is corrupt, and the connection is broken: nothing written past a buffer */
    fake_start(&f, type_switch);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    tsdb_series_result *r = NULL;
    CHECK(tsdbc_query_multi_collect(c, &q, s, 1, NULL, &r) == TSDB_ERR_CORRUPT);
    CHECK(r == NULL);
    tsdbc_series_results_free(r, 1);
    CHECK(tsdbc_series(c, "x", 1, &id) == TSDB_ERR_IO);
    tsdbc_close(c);
    fake_stop(&f);

    /* the same in a single series' stream: the callback sees the first type only */
    fake_start(&f, type_switch);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    unsigned seen = 0;
    CHECK(tsdbc_query_values(c, s[0], 0, 100, NULL, values_types, &seen) == TSDB_ERR_CORRUPT);
    CHECK(seen == 1u << TSDB_FLOAT32);
    CHECK(tsdbc_series(c, "x", 1, &id) == TSDB_ERR_IO);
    tsdbc_close(c);
    fake_stop(&f);

    /* series of different types, one in two chunks: collected whole, and the connection stays in step */
    fake_start(&f, multi_types);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    r = NULL;
    CHECK(tsdbc_query_multi_collect(c, &q, s, 2, NULL, &r) == 0);
    if (r) {
        CHECK(r[0].values.type == TSDB_BOOL && r[0].n == 4 && r[0].ts[3] == 2);
        CHECK(r[1].values.type == TSDB_FLOAT64 && r[1].n == 1000 && r[1].ts[999] == 999);
        bool same = true;
        for (size_t i = 0; r[1].n == 1000 && i < 8000; i++)
            same &= ((const uint8_t *)r[1].values.values)[i] == 0x41;
        CHECK(same);
    }
    tsdbc_series_results_free(r, 2);
    CHECK(tsdbc_series(c, "x", 1, &id) == 0 && id == 42);
    tsdbc_close(c);
    fake_stop(&f);
}

/* The socket of the connection to port: the client keeps its fd to itself. */
static int client_fd(int port)
{
    for (int fd = 3; fd < 1024; fd++) {
        struct sockaddr_in a;
        socklen_t al = sizeof a;
        if (getpeername(fd, (struct sockaddr *)&a, &al) == 0 && a.sin_family == AF_INET && ntohs(a.sin_port) == port)
            return fd;
    }
    return -1;
}

static int sockopt(int fd, int level, int name)
{
    int v = -1;
    socklen_t l = sizeof v;
    return getsockopt(fd, level, name, &v, &l) == 0 ? v : -1;
}

static void quiet(int fd)
{
    uint8_t b[1];
    while (read(fd, b, 1) > 0)
        ;
}

static void hang_up(int fd) { (void)fd; }

/* A server that hangs up while requests are still being sent (tsdbd refusing a
 * frame, a crash, a shutdown): the send fails with TSDB_ERR_IO, and SIGPIPE,
 * left at its default action here, must not end the process. */
static void test_peer_closed(void)
{
    struct fake f;
    fake_start(&f, hang_up);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    fake_stop(&f);
    usleep(100 * 1000);
    enum { N = 1000 };
    static tsdb_sid series[N];
    static int64_t ts[N];
    static float v[N];
    int rc = 0;
    for (int i = 0; i < 100 && rc == 0; i++) { /* the first send may still go out; the RST answers it */
        rc = tsdbc_insert_async(c, series, ts, v, N);
        usleep(10 * 1000);
    }
    CHECK(rc == TSDB_ERR_IO);
    CHECK(tsdbc_wait(c) == TSDB_ERR_IO);
    tsdbc_close(c);

    /* The kernel reports a reset to the first send as ECONNRESET without a
     * signal, and the client sends nothing more on a broken connection; a send
     * on a socket already shut down for writing (its error consumed elsewhere)
     * is the one that raises SIGPIPE, so every send must carry MSG_NOSIGNAL. */
    fake_start(&f, quiet);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    int fd = client_fd(f.port);
    CHECK(fd >= 0 && shutdown(fd, SHUT_WR) == 0);
    CHECK(tsdbc_insert_async(c, series, ts, v, N) == TSDB_ERR_IO);
    CHECK(tsdbc_wait(c) == TSDB_ERR_IO);
    tsdbc_close(c);
    fake_stop(&f);
}

static void test_keepalive_args(void)
{
    struct fake f;
    fake_start(&f, quiet);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    int fd = client_fd(f.port); /* keepalive on by default: 30 s idle, every 10 s, 3 probes (as Rust and Java) */
    CHECK(fd >= 0);
    CHECK(sockopt(fd, SOL_SOCKET, SO_KEEPALIVE) == 1);
    CHECK(sockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE) == 30);
    CHECK(sockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL) == 10);
    CHECK(sockopt(fd, IPPROTO_TCP, TCP_KEEPCNT) == 3);
    CHECK(tsdbc_set_keepalive(c, -1, 10, 3) == TSDB_ERR_INVALID);
    CHECK(tsdbc_set_keepalive(c, 30, 0, 3) == TSDB_ERR_INVALID);
    CHECK(tsdbc_set_keepalive(c, 5, 1, 2) == 0);
    CHECK(tsdbc_set_keepalive(c, 0, 0, 0) == 0);
    CHECK(sockopt(fd, SOL_SOCKET, SO_KEEPALIVE) == 0);
    tsdbc_close(c);
    fake_stop(&f);
}

/* Ids are u64 on the wire (protocol 11): an id above 2^32 goes out whole, and one comes back whole. */
static uint8_t wide_body[16];
static size_t wide_len;

static void wide_ids(int fd)
{
    uint8_t body[256];
    size_t n;
    uint32_t id;
    uint8_t op;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        if (op == TSDB_OP_SERIES_TYPE) {
            wide_len = n;
            memcpy(wide_body, body, n < sizeof wide_body ? n : sizeof wide_body);
            uint8_t t = TSDB_INT64;
            reply(fd, id, 0, &t, 1);
        } else {
            uint64_t sid = 0xffffffff00000001ull;
            reply(fd, id, 0, &sid, 8);
        }
    }
}

static void test_wide_ids(void)
{
    struct fake f;
    fake_start(&f, wide_ids);
    tsdbc *c;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    tsdb_type t = TSDB_FLOAT32;
    CHECK(tsdbc_series_type(c, 0x100000005ull, &t) == 0 && t == TSDB_INT64);
    uint64_t sent = 0;
    memcpy(&sent, wide_body, 8);
    CHECK(wide_len == 8 && sent == 0x100000005ull);
    tsdb_sid id = 0;
    CHECK(tsdbc_series(c, "x", 1, &id) == 0 && id == 0xffffffff00000001ull);
    tsdbc_close(c);
    fake_stop(&f);
}

/* The packed series column: zigzag varints of the wrapping 64-bit difference, so ids that jump across 2^32 either
 * way, or from 0 to UINT64_MAX - 1 and back, round trip, and the decoder takes every u64. */
static void test_pack_wide_ids(void)
{
    static const tsdb_sid cases[][6] = {
        {5, 0x100000005ull, 3, 0xffffffffull, 0x100000000ull, 0},
        {0, UINT64_MAX - 1, 0, UINT64_MAX - 1, 1, UINT64_MAX},
        {0xffffffff00000001ull, 1, 0x8000000000000000ull, 0x7fffffffffffffffull, UINT64_MAX, 0},
    };
    enum { N = 6 };
    for (size_t k = 0; k < sizeof cases / sizeof cases[0]; k++) {
        int64_t ts[N], ts2[N];
        float v[N], v2[N];
        tsdb_sid ids2[N];
        for (int i = 0; i < N; i++) {
            ts[i] = 1700000000000 + i * 1000;
            v[i] = (float)i;
        }
        size_t bound = tsdb_pack_raw_bound(N, TSDB_FLOAT32, 0, 1);
        uint8_t raw[512];
        CHECK(bound <= sizeof raw);
        size_t len = tsdb_pack_reshape(N, TSDB_FLOAT32, ts, v, NULL, cases[k], raw);
        CHECK(len <= bound);
        memset(ids2, 0, sizeof ids2);
        CHECK(tsdb_pack_unshape(raw, len, N, TSDB_FLOAT32, 0, 1, ts2, (uint8_t *)v2, NULL, ids2) == 0);
        CHECK(memcmp(ids2, cases[k], sizeof ids2) == 0 && memcmp(ts2, ts, sizeof ts) == 0 &&
              memcmp(v2, v, sizeof v) == 0);
        /* a body one byte short is refused, not read past */
        CHECK(tsdb_pack_unshape(raw, len - 1, N, TSDB_FLOAT32, 0, 1, ts2, (uint8_t *)v2, NULL, ids2) != 0);
    }
}

int main(void)
{
    test_pack_wide_ids();
    test_wide_ids();
    test_timeout_breaks_connection();
    test_reply_id_checked();
    test_name_replies();
    test_stream_cut();
    test_stream_in_step();
    test_timeout_in_large_body();
    test_values_stream_in_step();
    test_type_switch();
    test_peer_closed();
    test_connect_timeout();
    test_keepalive_args();
    if (failures) {
        fprintf(stderr, "test_client: %d failures\n", failures);
        return 1;
    }
    printf("test_client: ok\n");
    return 0;
}
