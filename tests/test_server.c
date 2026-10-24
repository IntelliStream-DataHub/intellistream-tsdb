/* Integration test: starts build/tsdbd and talks to it through the client library. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_protocol.h"

#include <arpa/inet.h>
#include <math.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
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

static pid_t start_server(const char *dir)
{
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    pid_t pid = fork();
    if (pid == 0) {
        const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd"; /* e.g. a sanitizer build */
        execl(bin, "tsdbd", "--dir", dir, "--port", portstr, "--no-fsync", "--workers", "4", (char *)NULL);
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
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("server tests passed\n");
    return 0;
}
