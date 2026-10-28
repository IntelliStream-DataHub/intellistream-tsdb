/* QUERY_MULTI and CANCEL (protocol 10) against a tsdbd started here: every inner op answers each series exactly
 * as the single request does, malformed requests get END INVALID alone, and stopping early leaves the connection
 * usable. Usage: test_server_multi [scratch dir]; TSDBD and TSDBD_IO as for test_server. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_protocol.h"
#include "harness.h"

#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                  \
    do {                                                                                          \
        int rc_ = (expr);                                                                         \
        if (rc_ != 0) {                                                                           \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdbc_strerror(rc_)); \
            exit(1);                                                                              \
        }                                                                                         \
    } while (0)

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define SANITIZED 1 /* shadow memory and quarantine are anonymous: the memory bounds mean nothing, timings neither */
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define SANITIZED 1
#endif
#endif
#ifndef SANITIZED
#define SANITIZED 0
#endif

static int port;
static const int64_t T0 = 1711846800000; /* 2024-03-31 01:00 UTC: Oslo's clocks go forward that night */

static pid_t start_server(const char *dir, const char *const *extra)
{
    char pf[PATH_MAX + 64];
    th_portfile(pf, sizeof pf, dir);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    pid_t pid = fork();
    if (pid == 0) {
        const char *argv[32] = {"tsdbd", "--dir", dir, "--port", "0", "--port-file", pf, "--workers", "4", "--no-fsync"};
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
    int p = th_await(pf, pid, 30000);
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

static void stop_server(pid_t pid)
{
    if (pid > 0)
        kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

/* ---- the single-series answer, collected like tsdbc_query_multi_collect ---- */

typedef struct one {
    int status;
    size_t n;
    int64_t *ts;
    uint8_t *v;
    uint8_t valid[1 << 14];
    bool nulls;
    tsdb_type type;
    void *buckets;
} one;

static int one_rows(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    one *o = ctx;
    unsigned aw = tsdb_wire_width(col->type);
    o->ts = realloc(o->ts, (o->n + n) * 8);
    o->v = realloc(o->v, (o->n + n) * aw + 1);
    memcpy(o->ts + o->n, ts, n * 8);
    memcpy(o->v + o->n * aw, col->values, n * aw);
    for (size_t i = 0; i < n; i++) {
        bool k = !col->valid || ((col->valid[i >> 3] >> (i & 7)) & 1);
        size_t j = o->n + i;
        if (j < sizeof o->valid * 8)
            o->valid[j >> 3] |= (uint8_t)(k << (j & 7));
        o->nulls |= !k;
    }
    o->type = col->type;
    o->n += n;
    return 0;
}

static size_t stride_of(tsdb_multi_kind k)
{
    return k == TSDB_MULTI_TWA      ? sizeof(tsdb_twa_bucket)
           : k == TSDB_MULTI_SPREAD ? sizeof(tsdb_spread_bucket)
           : k == TSDB_MULTI_BUCKETS ? sizeof(tsdb_bucket)
                                     : 0;
}

static void single(tsdbc *c, const tsdb_multi_query *q, size_t i, uint32_t s, one *o)
{
    memset(o, 0, sizeof *o);
    int64_t st = q->starts ? q->starts[i] : q->start, en = q->ends ? q->ends[i] : q->end;
    int64_t iv = q->intervals ? q->intervals[i] : q->interval_ms;
    size_t nb = 0;
    void *b = NULL;
    switch (q->kind) {
    case TSDB_MULTI_VALUES: o->status = tsdbc_query_values(c, s, st, en, &q->opts, one_rows, o); break;
    case TSDB_MULTI_LTTB: o->status = tsdbc_query_lttb_ex(c, s, st, en, q->threshold, &q->opts, one_rows, o); break;
    case TSDB_MULTI_BUCKETS:
        o->status = q->nbounds ? tsdbc_query_bounds(c, s, st, en, q->bounds, q->nbounds, &q->opts, (tsdb_bucket **)&b, &nb)
                    : q->group ? tsdbc_query_group(c, s, st, en, q->group, &q->opts, (tsdb_bucket **)&b, &nb)
                               : tsdbc_query_agg_ex(c, s, st, en, iv, &q->opts, (tsdb_bucket **)&b, &nb);
        break;
    case TSDB_MULTI_SPREAD:
        o->status = q->nbounds ? tsdbc_query_spread_bounds(c, s, st, en, q->bounds, q->nbounds, &q->opts,
                                                           (tsdb_spread_bucket **)&b, &nb)
                    : q->group ? tsdbc_query_spread_group(c, s, st, en, q->group, &q->opts, (tsdb_spread_bucket **)&b, &nb)
                               : tsdbc_query_spread(c, s, st, en, iv, &q->opts, (tsdb_spread_bucket **)&b, &nb);
        break;
    case TSDB_MULTI_TWA:
        o->status = q->nbounds ? tsdbc_query_twa_bounds(c, s, st, en, q->bounds, q->nbounds, &q->twa,
                                                        (tsdb_twa_bucket **)&b, &nb)
                    : q->group ? tsdbc_query_twa_group(c, s, st, en, q->group, &q->twa, (tsdb_twa_bucket **)&b, &nb)
                               : tsdbc_query_twa(c, s, st, en, iv, &q->twa, (tsdb_twa_bucket **)&b, &nb);
        break;
    }
    if (stride_of(q->kind)) {
        o->buckets = b;
        o->n = o->status ? 0 : nb;
    }
}

static bool same(const tsdb_multi_query *q, const tsdb_series_result *r, const one *o, const char *what, size_t i)
{
    bool ok = r->status == o->status && r->n == o->n;
    if (ok && stride_of(q->kind))
        ok = o->n == 0 || memcmp(r->buckets, o->buckets, o->n * stride_of(q->kind)) == 0;
    else if (ok && o->n) {
        unsigned aw = tsdb_wire_width(o->type);
        ok = r->values.type == o->type && memcmp(r->ts, o->ts, o->n * 8) == 0 &&
             memcmp(r->values.values, o->v, o->n * aw) == 0 && (!r->values.valid) == !o->nulls;
        for (size_t j = 0; ok && o->nulls && j < o->n && j < sizeof o->valid * 8; j++)
            ok = ((r->values.valid[j >> 3] >> (j & 7)) & 1) == ((o->valid[j >> 3] >> (j & 7)) & 1);
    }
    if (!ok)
        fprintf(stderr, "%s: series %zu: multi status %d n %zu, single status %d n %zu\n", what, i, r->status, r->n,
                o->status, o->n);
    return ok;
}

static void compare(tsdbc *c, const tsdb_multi_query *q, const uint32_t *ids, size_t n, const char *what)
{
    tsdb_series_result *r = NULL;
    int rc = tsdbc_query_multi_collect(c, q, ids, n, NULL, &r);
    if (rc != 0) {
        fprintf(stderr, "%s: multi -> %s\n", what, tsdbc_strerror(rc));
        failures++;
        tsdbc_series_results_free(r, n);
        return;
    }
    for (size_t i = 0; i < n; i++) {
        one o;
        single(c, q, i, ids[i], &o);
        CHECK(r[i].index == i && r[i].series == ids[i]);
        if (!same(q, &r[i], &o, what, i))
            failures++;
        free(o.ts);
        free(o.v);
        free(o.buckets);
    }
    tsdbc_series_results_free(r, n);
}

/* ---- raw frames ---- */

static int raw_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || connect(fd, (struct sockaddr *)&a, sizeof a) != 0) {
        perror("connect");
        exit(1);
    }
    return fd;
}

/* tsdb_request_errors_total{code="invalid"} from the metrics port, -1 if the scrape fails */
static long invalid_errors(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)th_mport)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd < 0 || th_mport <= 0 || connect(fd, (struct sockaddr *)&a, sizeof a) != 0) {
        if (fd >= 0)
            close(fd);
        return -1;
    }
    struct timeval tv = {5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    static const char req[] = "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";
    if (send(fd, req, sizeof req - 1, MSG_NOSIGNAL) != (ssize_t)(sizeof req - 1)) {
        close(fd);
        return -1;
    }
    static char buf[1 << 20];
    size_t got = 0;
    ssize_t r;
    while (got < sizeof buf - 1 && (r = recv(fd, buf + got, sizeof buf - 1 - got, 0)) > 0)
        got += (size_t)r;
    buf[got] = '\0';
    close(fd);
    const char *key = "tsdb_request_errors_total{code=\"invalid\"} ", *at = strstr(buf, key);
    return at ? atol(at + strlen(key)) : -1;
}

static void raw_send(int fd, uint32_t id, uint8_t op, const void *p, size_t len)
{
    uint8_t h[9];
    uint32_t l = (uint32_t)(5 + len);
    memcpy(h, &l, 4);
    memcpy(h + 4, &id, 4);
    h[8] = op;
    CHECK(send(fd, h, 9, MSG_NOSIGNAL) == 9);
    CHECK(len == 0 || send(fd, p, len, MSG_NOSIGNAL) == (ssize_t)len);
}

static bool read_full(int fd, void *p, size_t n)
{
    uint8_t *o = p;
    while (n) {
        ssize_t r = recv(fd, o, n, 0);
        if (r <= 0)
            return false;
        o += r;
        n -= (size_t)r;
    }
    return true;
}

/* one reply frame: status, index (multi frames), body length; the body is read into buf */
static bool raw_frame(int fd, uint32_t *id, int32_t *st, uint32_t *ix, uint8_t *buf, size_t cap, size_t *blen)
{
    uint8_t h[17];
    uint32_t len;
    if (!read_full(fd, h, 17))
        return false;
    memcpy(&len, h, 4);
    memcpy(id, h + 4, 4);
    memcpy(st, h + 9, 4);
    memcpy(ix, h + 13, 4);
    if (len < 13 || len - 13 > cap)
        return false;
    *blen = len - 13;
    return read_full(fd, buf, *blen);
}

static size_t multi_frame(uint8_t *out, uint8_t op, uint8_t flags, uint32_t n, const uint32_t *ids,
                          const void *tmpl, size_t tlen)
{
    memset(out, 0, 24);
    out[0] = op;
    out[1] = flags;
    memcpy(out + 4, &n, 4);
    uint32_t tl = (uint32_t)tlen;
    memcpy(out + 16, &tl, 4);
    size_t at = 24;
    memcpy(out + at, ids, (size_t)n * 4);
    at += (size_t)n * 4;
    while (at % 8)
        out[at++] = 0;
    memcpy(out + at, tmpl, tlen);
    return at + tlen;
}

static void values_tmpl(uint8_t *t, uint32_t series, int64_t start, int64_t end)
{
    memset(t, 0, 29);
    memcpy(t, &series, 4);
    memcpy(t + 4, &start, 8);
    memcpy(t + 12, &end, 8);
}

/* a malformed request gets the END frame alone, with INVALID and 0 answered; the connection goes on */
static void expect_end_invalid(int fd, const uint8_t *req, size_t len, const char *what)
{
    static uint32_t id = 100;
    raw_send(fd, ++id, TSDB_OP_QUERY_MULTI, req, len);
    uint8_t buf[64];
    uint32_t rid, ix, answered = 99;
    int32_t st;
    size_t bl;
    bool ok = raw_frame(fd, &rid, &st, &ix, buf, sizeof buf, &bl);
    if (ok && bl == 4)
        memcpy(&answered, buf, 4);
    if (!(ok && rid == id && ix == TSDB_MULTI_END && st == TSDB_ERR_INVALID && bl == 4 && answered == 0)) {
        fprintf(stderr, "%s: not END INVALID alone (ok %d ix %u st %d len %zu)\n", what, ok, ix, st, bl);
        failures++;
    }
}

static void test_malformed(uint32_t a)
{
    int fd = raw_connect();
    uint8_t t[64], req[256];
    uint32_t ids[2] = {a, a};
    values_tmpl(t, TSDB_MANY_SERIES, T0, T0 + 3600000);
    size_t len = multi_frame(req, TSDB_OP_QUERY_VALUES, 0, 2, ids, t, 29);
    uint8_t bad[256];
    memcpy(bad, req, len);
    bad[0] = TSDB_OP_QUERY_RAW;
    expect_end_invalid(fd, bad, len, "inner op RAW");
    memcpy(bad, req, len);
    bad[0] = TSDB_OP_QUERY_MULTI;
    expect_end_invalid(fd, bad, len, "inner op MULTI");
    memcpy(bad, req, len);
    bad[1] = 32;
    expect_end_invalid(fd, bad, len, "reserved flag");
    memcpy(bad, req, len);
    bad[1] = TSDB_MULTI_PER_INTERVAL;
    expect_end_invalid(fd, bad, len, "PER_INTERVAL on VALUES (and the arrays then too short)");
    memcpy(bad, req, len);
    bad[1] = TSDB_MULTI_WIRE_TRIM_LO;
    expect_end_invalid(fd, bad, len, "TRIM on VALUES");
    memcpy(bad, req, len);
    memset(bad + 4, 0, 4);
    expect_end_invalid(fd, bad, len, "n = 0");
    memcpy(bad, req, len);
    uint32_t big = TSDB_MULTI_MAX_SERIES + 1;
    memcpy(bad + 4, &big, 4);
    expect_end_invalid(fd, bad, len, "n over the cap");
    memcpy(bad, req, len);
    bad[20] = 1;
    expect_end_invalid(fd, bad, len, "the reserved u32");
    memcpy(bad, req, len);
    bad[16] = 28;
    expect_end_invalid(fd, bad, len, "tlen short");
    expect_end_invalid(fd, req, len - 1, "length short");
    memcpy(bad, req, len);
    bad[24 + 8] = 0; /* a padding byte would be at 32 with n = 2: none, so poke the template's series */
    memcpy(bad + 32, &a, 4);
    expect_end_invalid(fd, bad, len, "template names a series");
    expect_end_invalid(fd, req, 10, "header short");
    /* PER_START with a start in the template */
    {
        uint8_t r2[256];
        memset(r2, 0, sizeof r2);
        r2[0] = TSDB_OP_QUERY_VALUES;
        r2[1] = TSDB_MULTI_PER_START;
        uint32_t n = 2, tl = 29;
        memcpy(r2 + 4, &n, 4);
        memcpy(r2 + 16, &tl, 4);
        int64_t starts[2] = {T0, T0};
        memcpy(r2 + 24, starts, 16);
        memcpy(r2 + 40, ids, 8);
        values_tmpl(r2 + 48, TSDB_MANY_SERIES, T0, T0 + 1000);
        expect_end_invalid(fd, r2, 48 + 29, "PER_START with a start in the template");
        values_tmpl(r2 + 48, TSDB_MANY_SERIES, 0, T0 + 1000);
        raw_send(fd, 7, TSDB_OP_QUERY_MULTI, r2, 48 + 29);
        uint8_t buf[1 << 16];
        uint32_t rid, ix;
        int32_t st;
        size_t bl;
        int frames = 0;
        while (raw_frame(fd, &rid, &st, &ix, buf, sizeof buf, &bl) && ix != TSDB_MULTI_END)
            frames++;
        CHECK(ix == TSDB_MULTI_END && st == 0 && frames == 2);
    }
    /* one series: 4 bytes of padding after its id, which must be zero */
    {
        uint8_t r1[128];
        size_t l1 = multi_frame(r1, TSDB_OP_QUERY_VALUES, 0, 1, ids, t, 29);
        CHECK(l1 == 32 + 29);
        r1[29] = 1;
        expect_end_invalid(fd, r1, l1, "nonzero padding");
    }
    /* CANCEL of an unknown id: no reply, and the connection goes on */
    uint32_t nobody = 12345;
    raw_send(fd, nobody, TSDB_OP_CANCEL, &nobody, 4);
    expect_end_invalid(fd, req, 10, "after CANCEL of an unknown id");
    /* a CANCEL of the wrong length has no reply either, but counts as an INVALID request */
    long e0 = invalid_errors();
    uint8_t five[5] = {0};
    raw_send(fd, nobody, TSDB_OP_CANCEL, five, 5);
    expect_end_invalid(fd, req, 10, "after a CANCEL of 5 bytes");
    long e1 = invalid_errors();
    if (!(e0 >= 0 && e1 == e0 + 2)) {
        fprintf(stderr, "a malformed CANCEL is not counted: invalid errors %ld, then %ld\n", e0, e1);
        failures++;
    }
    close(fd);
}

/* ---- data ---- */

static uint32_t ids[6];

static void load(tsdbc *c)
{
    static const tsdb_type types[5] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL, TSDB_STATE};
    static const char *names[5] = {"m.f32", "m.f64", "m.i64", "m.bool", "m.state"};
    enum { N = 20000 };
    int64_t *ts = malloc(N * 8);
    uint8_t *v = malloc(N * 8), valid[N / 8];
    for (int k = 0; k < 5; k++) {
        OK(tsdbc_series_create(c, names[k], types[k], NULL, &ids[k]));
        memset(valid, 0xff, sizeof valid);
        for (int i = 0; i < N; i++) {
            ts[i] = T0 - 7200000 + (int64_t)i * (1000 + 37 * k);
            double x = 20 + 10 * ((i * 7919 + k * 31) % 1000) / 1000.0;
            switch (types[k]) {
            case TSDB_FLOAT32: ((float *)v)[i] = (float)x; break;
            case TSDB_FLOAT64: ((double *)v)[i] = x / 3; break;
            case TSDB_INT64: ((int64_t *)v)[i] = (int64_t)(x * 1000) - 25000; break;
            case TSDB_BOOL: v[i] = (uint8_t)(i / 17 % 2); break;
            case TSDB_STATE: ((uint32_t *)v)[i] = (uint32_t)(i / 100 % 5); break;
            }
            if (i % 97 == 5)
                valid[i / 8] &= (uint8_t)~(1u << (i % 8));
        }
        tsdb_values col = {types[k], v, valid};
        OK(tsdbc_insert_series_values(c, ids[k], ts, &col, N / 2));
        OK(tsdbc_flush(c)); /* a part, then memtable rows above it */
        tsdb_values col2 = {types[k], v + (N / 2) * tsdb_wire_width(types[k]), NULL};
        OK(tsdbc_insert_series_values(c, ids[k], ts + N / 2, &col2, N / 2));
    }
    ids[5] = 999999; /* unknown */
    free(ts);
    free(v);
}

/* ---- frames at the limits, a client that stops reading, CANCEL, pipelining, disconnects, statistics ---- */

static const int64_t TB = T0 + 30LL * 86400000; /* the big series: one row a millisecond from here */
enum { BIG_ROWS = 4194304 };
static uint32_t big_id;

static void load_big(tsdbc *c)
{
    OK(tsdbc_series_create(c, "m.big", TSDB_FLOAT32, NULL, &big_id));
    int64_t *ts = malloc((size_t)BIG_ROWS * 8);
    float *v = malloc((size_t)BIG_ROWS * 4);
    for (size_t i = 0; i < BIG_ROWS; i++) {
        ts[i] = TB + (int64_t)i;
        v[i] = (float)(i % 1000);
    }
    tsdb_values col = {TSDB_FLOAT32, v, NULL};
    OK(tsdbc_insert_series_values(c, big_id, ts, &col, BIG_ROWS));
    OK(tsdbc_flush(c));
    free(ts);
    free(v);
}

static double now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec * 1e3 + (double)t.tv_nsec / 1e6;
}

/* anonymous memory of the server: its output buffers and results, not the part files it maps */
static long rss_anon_kb(pid_t pid)
{
    char path[64], line[256];
    snprintf(path, sizeof path, "/proc/%d/status", (int)pid);
    FILE *f = fopen(path, "r");
    long kb = -1;
    while (f && fgets(line, sizeof line, f))
        if (sscanf(line, "RssAnon: %ld", &kb) == 1)
            break;
    if (f)
        fclose(f);
    return kb;
}

/* The largest bucket reply a frame carries (with the index, TSDB_MAX_FRAME + 4 at most) goes out whole; one bucket
 * more is TOO_MANY_BUCKETS for that series alone. VALUES chunks that end exactly at a chunk's size. */
static void test_full_frame(tsdbc *c)
{
    const size_t full = (TSDB_MAX_FRAME - 13) / TSDB_BUCKET_ROW;
    uint32_t two[2] = {big_id, ids[0]};
    tsdb_multi_query q = {.kind = TSDB_MULTI_BUCKETS, .start = TB, .end = TB + (int64_t)full, .interval_ms = 1};
    tsdb_series_result *r = NULL;
    CHECK(tsdbc_query_multi_collect(c, &q, two, 2, NULL, &r) == 0);
    CHECK(r && r[0].status == 0 && r[0].n == full && r[1].status == 0);
    CHECK(r && r[0].n == full && ((const tsdb_bucket *)r[0].buckets)[full - 1].start == TB + (int64_t)full - 1);
    tsdbc_series_results_free(r, 2);
    compare(c, &q, two, 2, "one full frame of buckets");
    q.end++;
    CHECK(tsdbc_query_multi_collect(c, &q, two, 2, NULL, &r) == 0);
    CHECK(r && r[0].status == TSDB_ERR_TOO_MANY_BUCKETS && r[0].n == 0 && r[1].status == 0);
    tsdbc_series_results_free(r, 2);
    compare(c, &q, two, 2, "one bucket over a frame");
    uint32_t bb[3] = {big_id, ids[1], big_id};
    for (int64_t rows = 16383; rows <= 32769; rows += rows == 16385 ? 32766 - 16385 : 1) {
        tsdb_multi_query v = {.kind = TSDB_MULTI_VALUES, .start = TB, .end = TB + rows, .opts = {TSDB_ASC, 0}};
        compare(c, &v, bb, 3, "values around a full chunk");
    }
}

static void raw_values_multi(int fd, uint32_t id, const uint32_t *list, uint32_t n)
{
    uint8_t t[29], *req = malloc(24 + (size_t)n * 4 + 8 + 29);
    values_tmpl(t, TSDB_MANY_SERIES, INT64_MIN, INT64_MAX);
    size_t len = multi_frame(req, TSDB_OP_QUERY_VALUES, 0, n, list, t, 29);
    raw_send(fd, id, TSDB_OP_QUERY_MULTI, req, len);
    free(req);
}

/* Reads the frames of request `id` up to its END; indexes must not go back. Returns the END status, or 1 when the
 * connection ended first. *seen counts the series that had frames. */
static int read_to_end(int fd, uint32_t id, uint32_t *answered, uint32_t *seen)
{
    static uint8_t buf[(TSDB_MAX_FRAME >> 4) + 64];
    uint32_t rid, ix, last = 0;
    int32_t st;
    *seen = 0;
    for (;;) {
        uint8_t h[17];
        uint32_t len;
        if (!read_full(fd, h, 17))
            return 1;
        memcpy(&len, h, 4);
        memcpy(&rid, h + 4, 4);
        memcpy(&st, h + 9, 4);
        memcpy(&ix, h + 13, 4);
        if (len < 13 || len - 13 > sizeof buf || !read_full(fd, buf, len - 13))
            return 1;
        CHECK(rid == id);
        if (ix == TSDB_MULTI_END) {
            CHECK(len - 13 == 4);
            memcpy(answered, buf, 4);
            return st;
        }
        CHECK(ix >= last);
        if (ix != last || *seen == 0)
            (*seen)++;
        last = ix;
    }
}

/* A client that sends a large QUERY_MULTI and reads nothing: the server holds at most the results ahead of the head
 * and its output cap, other connections are answered at once, and --send-timeout ends the connection. */
static void test_stall(tsdbc *c, pid_t pid, int send_timeout_s)
{
    uint32_t list[64];
    for (int i = 0; i < 64; i++)
        list[i] = i % 2 ? big_id : ids[i / 2 % 5];
    long before = rss_anon_kb(pid);
    int fd = raw_connect();
    double t0 = now_ms();
    raw_values_multi(fd, 7, list, 64);
    usleep(1500000);
    long grown = rss_anon_kb(pid) - before;
    fprintf(stderr, "stalled reader: server anonymous memory grew %ld KiB\n", grown);
    CHECK(SANITIZED || grown <= (64 + 8 + 48) * 1024L);
    tsdbc *c2;
    OK(tsdbc_connect("127.0.0.1", port, &c2));
    double best = 1e9;
    for (int k = 0; k < 5; k++) {
        one o = {0};
        double a = now_ms();
        CHECK(tsdbc_query_values(c2, ids[0], T0, T0 + 1, NULL, one_rows, &o) == 0);
        double d = now_ms() - a;
        best = d < best ? d : best;
        free(o.ts);
        free(o.v);
    }
    fprintf(stderr, "stalled reader: point query on another connection %.2f ms\n", best);
    CHECK(SANITIZED || best < 50);
    tsdbc_close(c2);
    /* past the send timeout the server has shut the connection: what it had sent drains, then EOF */
    double wait = send_timeout_s * 1e3 + 1500 - (now_ms() - t0);
    if (wait > 0)
        usleep((useconds_t)(wait * 1000));
    struct timeval tv = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    uint32_t answered = 0, seen;
    CHECK(read_to_end(fd, 7, &answered, &seen) == 1); /* no END: the connection ended */
    close(fd);
    one o = {0};
    CHECK(tsdbc_query_values(c, ids[0], T0, T0 + 1, NULL, one_rows, &o) == 0 && o.n == 1);
    free(o.ts);
    free(o.v);
}

/* The same with only small replies (a few KiB per series, gathered into batches): the batches go out as they fill,
 * so the server holds at most its output cap and the results ahead, not the 300 MiB of the whole answer. */
static void test_stall_small(pid_t pid)
{
    uint32_t n = TSDB_MULTI_MAX_SERIES;
    uint32_t *list = malloc((size_t)n * 4);
    uint8_t t[29], *req = malloc(24 + (size_t)n * 4 + 8 + 29);
    for (uint32_t i = 0; i < n; i++)
        list[i] = ids[i % 5];
    values_tmpl(t, TSDB_MANY_SERIES, T0, T0 + 400000); /* about 400 rows of each */
    size_t len = multi_frame(req, TSDB_OP_QUERY_VALUES, 0, n, list, t, 29);
    long before = rss_anon_kb(pid);
    int fd = raw_connect();
    raw_send(fd, 9, TSDB_OP_QUERY_MULTI, req, len);
    usleep(2000000);
    long grown = rss_anon_kb(pid) - before;
    fprintf(stderr, "stalled reader, small replies: server anonymous memory grew %ld KiB\n", grown);
    CHECK(SANITIZED || grown <= (64 + 8 + 48) * 1024L);
    close(fd);
    free(list);
    free(req);
}

/* On one connection, sent before anything is read: A (large), B, a single query C, then CANCEL B (queued) and
 * CANCEL of an id nobody uses. A is answered whole, B ends STOPPED with nothing answered, C comes after, in order.
 * Then D is cancelled while it runs, and the connection goes on. */
static void test_cancel_pipeline(void)
{
    int fd = raw_connect();
    uint32_t a[4] = {big_id, big_id, ids[0], big_id}, b[2] = {ids[1], ids[2]};
    raw_values_multi(fd, 1, a, 4);
    raw_values_multi(fd, 2, b, 2);
    uint8_t t[29];
    values_tmpl(t, ids[0], T0, T0 + 1);
    raw_send(fd, 3, TSDB_OP_QUERY_VALUES, t, 29);
    uint32_t cid = 2, nobody = 77;
    raw_send(fd, 2, TSDB_OP_CANCEL, &cid, 4);
    raw_send(fd, 77, TSDB_OP_CANCEL, &nobody, 4);
    uint32_t answered = 99, seen = 0;
    CHECK(read_to_end(fd, 1, &answered, &seen) == 0 && answered == 4 && seen == 4);
    CHECK(read_to_end(fd, 2, &answered, &seen) == TSDB_ERR_STOPPED && answered == 0 && seen == 0);
    uint8_t buf[256];
    uint32_t rid, ix;
    int32_t st;
    size_t bl;
    CHECK(raw_frame(fd, &rid, &st, &ix, buf, sizeof buf, &bl) && rid == 3 && st == 0 && (ix & 0xff) == 1);
    uint32_t d[32];
    for (int i = 0; i < 32; i++)
        d[i] = big_id;
    raw_values_multi(fd, 4, d, 32);
    uint8_t h[17];
    uint32_t len = 0;
    static uint8_t body[(TSDB_MAX_FRAME >> 4) + 64];
    CHECK(read_full(fd, h, 17));
    memcpy(&len, h, 4);
    CHECK(len >= 13 && len - 13 <= sizeof body && read_full(fd, body, len - 13));
    uint32_t did = 4;
    raw_send(fd, 4, TSDB_OP_CANCEL, &did, 4);
    CHECK(read_to_end(fd, 4, &answered, &seen) == TSDB_ERR_STOPPED && answered < 32);
    raw_send(fd, 5, TSDB_OP_QUERY_VALUES, t, 29);
    CHECK(raw_frame(fd, &rid, &st, &ix, buf, sizeof buf, &bl) && rid == 5 && st == 0);
    close(fd);
}

typedef struct {
    int fd;
    const uint8_t *fill;
    size_t nfill;
    atomic_bool stop;
} flood;

static bool send_all(int fd, const uint8_t *p, size_t n)
{
    while (n) {
        ssize_t k = send(fd, p, n, MSG_NOSIGNAL);
        if (k <= 0)
            return false;
        p += k;
        n -= (size_t)k;
    }
    return true;
}

static void *flood_fn(void *arg)
{
    flood *f = arg;
    static uint8_t cancels[4096 * 13];
    for (int i = 0; i < 4096; i++) {
        uint32_t l = 9, id = 0, nobody = 77;
        memcpy(cancels + i * 13, &l, 4);
        memcpy(cancels + i * 13 + 4, &id, 4);
        cancels[i * 13 + 8] = TSDB_OP_CANCEL;
        memcpy(cancels + i * 13 + 9, &nobody, 4);
    }
    if (!send_all(f->fd, f->fill, f->nfill))
        return NULL;
    while (!atomic_load(&f->stop) && send_all(f->fd, cancels, sizeof cancels)) {
    }
    return NULL;
}

/* A connection stalls its reply (it does not read), queues many requests behind it and floods CANCELs naming
 * nobody: the loop thread must not walk the queue for every CANCEL (it did: 350 ms for a point query on another
 * connection with 30,000 LAST_STATS queued). Queued with 30,000 LAST_STATS, then with 20,000 small multis. */
static void test_cancel_flood(void)
{
    uint32_t d[32];
    for (int i = 0; i < 32; i++)
        d[i] = big_id;
    for (int kind = 0; kind < 2; kind++) {
        size_t nreq = kind ? 20000 : 30000, per = 0;
        uint8_t m[128], t[29];
        if (kind) {
            values_tmpl(t, TSDB_MANY_SERIES, T0, T0 + 1);
            per = multi_frame(m + 9, TSDB_OP_QUERY_VALUES, 0, 1, ids, t, 29);
        }
        uint8_t *fill = malloc(nreq * (9 + per));
        CHECK(fill != NULL);
        for (size_t i = 0; i < nreq; i++) {
            uint8_t *f = fill + i * (9 + per);
            uint32_t l = (uint32_t)(5 + per), id = (uint32_t)(5000 + i);
            memcpy(f, &l, 4);
            memcpy(f + 4, &id, 4);
            f[8] = kind ? TSDB_OP_QUERY_MULTI : TSDB_OP_LAST_STATS;
            memcpy(f + 9, m + 9, per);
        }
        int fd = raw_connect();
        int small = 4096;
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof small);
        raw_values_multi(fd, 1, d, 32); /* its reply stalls the connection's worker */
        flood f = {.fd = fd, .fill = fill, .nfill = nreq * (9 + per)};
        pthread_t th;
        CHECK(pthread_create(&th, NULL, flood_fn, &f) == 0);
        usleep(500000);
        tsdbc *c2;
        OK(tsdbc_connect("127.0.0.1", port, &c2));
        double sum = 0, worst = 0;
        for (int k = 0; k < 20; k++) {
            one o = {0};
            double a = now_ms();
            CHECK(tsdbc_query_values(c2, ids[0], T0, T0 + 1, NULL, one_rows, &o) == 0 && o.n == 1);
            double dt = now_ms() - a;
            sum += dt;
            worst = dt > worst ? dt : worst;
            free(o.ts);
            free(o.v);
        }
        fprintf(stderr, "CANCEL flood behind %zu %s: point query on another connection mean %.2f ms, max %.2f\n",
                nreq, kind ? "multis" : "LAST_STATS", sum / 20, worst);
        CHECK(SANITIZED || sum / 20 < 50);
        atomic_store(&f.stop, true);
        shutdown(fd, SHUT_RDWR);
        pthread_join(th, NULL);
        close(fd);
        free(fill);
        tsdbc_close(c2);
    }
}

/* Clients that go away in the middle of large replies (more of them than workers) leave the server serving. */
static void test_disconnect(tsdbc *c, pid_t pid)
{
    long before = rss_anon_kb(pid);
    uint32_t d[32];
    for (int i = 0; i < 32; i++)
        d[i] = big_id;
    for (int k = 0; k < 8; k++) {
        int fd = raw_connect();
        raw_values_multi(fd, 9, d, 32);
        uint8_t buf[4096];
        CHECK(read_full(fd, buf, sizeof buf));
        close(fd);
    }
    double a = now_ms();
    one o = {0};
    CHECK(tsdbc_query_values(c, ids[0], T0, T0 + 1, NULL, one_rows, &o) == 0 && o.n == 1);
    free(o.ts);
    free(o.v);
    CHECK(now_ms() - a < 5000);
    usleep(500000);
    long grown = rss_anon_kb(pid) - before;
    fprintf(stderr, "after disconnects: server anonymous memory grew %ld KiB\n", grown);
    CHECK(SANITIZED || grown <= (64 + 8 + 48) * 1024L);
}

/* LAST_STATS after a QUERY_MULTI on one thread: the sums of the single requests' */
static void test_stats(tsdbc *c, const tsdb_multi_query *q, const uint32_t *list, size_t n)
{
    tsdb_query_stats sum = {0}, st;
    for (size_t i = 0; i < n; i++) {
        one o;
        single(c, q, i, list[i], &o);
        free(o.ts);
        free(o.v);
        free(o.buckets);
        OK(tsdbc_last_query_stats(c, &st));
        sum.rows_decoded += st.rows_decoded;
        sum.rows_from_stats += st.rows_from_stats;
        sum.memtable_rows += st.memtable_rows;
        sum.parts += st.parts;
        sum.granules += st.granules;
        sum.result_rows += st.result_rows;
    }
    tsdb_multi_opts one_thread = {.threads = 1};
    tsdb_series_result *r = NULL;
    OK(tsdbc_query_multi_collect(c, q, list, n, &one_thread, &r));
    tsdbc_series_results_free(r, n);
    OK(tsdbc_last_query_stats(c, &st));
    CHECK(st.rows_decoded == sum.rows_decoded && st.rows_from_stats == sum.rows_from_stats);
    CHECK(st.memtable_rows == sum.memtable_rows && st.parts == sum.parts && st.granules == sum.granules);
    CHECK(st.result_rows == sum.result_rows && st.result_rows > 0);
}

typedef struct stopper {
    int calls, stop_at;
} stopper;

static int stop_fn(void *ctx, const tsdb_series_result *r)
{
    (void)r;
    stopper *s = ctx;
    return ++s->calls >= s->stop_at;
}

/* every op and mode, multi against single; stopping; max_rows; LAST_STATS */
static void run_cases(tsdbc *c)
{
    uint32_t list[13];
    size_t n = 0;
    for (int k = 0; k < 6; k++)
        list[n++] = ids[k];
    list[n++] = ids[0]; /* duplicates are answered each time */
    list[n++] = ids[2];
    int64_t starts[13], ends[13], ivs[13];
    for (size_t i = 0; i < n; i++) {
        starts[i] = T0 - 3600000 + (int64_t)i * 60000;
        ends[i] = T0 + 3600000 * 2 - (int64_t)i * 120000;
        ivs[i] = 60000 * (int64_t)(1 + i % 4);
    }
    int64_t lo = T0 - 7200000, hi = T0 + 4 * 3600000;
    tsdb_group utc_h = {TSDB_HOUR, 1, NULL}, oslo_d = {TSDB_DAY, 1, "Europe/Oslo"},
               oslo_h = {TSDB_HOUR, 1, "Europe/Oslo"}, lh = {TSDB_HOUR, 2, "Australia/Lord_Howe"};
    int64_t bounds[4] = {T0 - 3600000, T0, T0 + 600000, T0 + 7200000};
    tsdb_query_opts asc = {TSDB_ASC, 0}, desc5 = {TSDB_DESC, 5};
    tsdb_twa_opts twa = {.method = TSDB_TWA_STEP, .max_gap_ms = 0, .order = TSDB_ASC, .limit = 0};
    tsdb_twa_opts twal = {.method = TSDB_TWA_LINEAR, .max_gap_ms = 30000, .order = TSDB_DESC, .limit = 3};

    struct {
        const char *what;
        tsdb_multi_query q;
    } cases[] = {
        {"values", {.kind = TSDB_MULTI_VALUES, .start = lo, .end = hi, .opts = asc}},
        {"values desc 5", {.kind = TSDB_MULTI_VALUES, .start = lo, .end = hi, .opts = desc5}},
        {"values open", {.kind = TSDB_MULTI_VALUES, .start = INT64_MIN, .end = INT64_MAX, .opts = asc}},
        {"values per range", {.kind = TSDB_MULTI_VALUES, .starts = starts, .ends = ends, .opts = asc}},
        {"lttb", {.kind = TSDB_MULTI_LTTB, .start = lo, .end = hi, .opts = asc, .threshold = 50}},
        {"lttb desc 5", {.kind = TSDB_MULTI_LTTB, .start = lo, .end = hi, .opts = desc5, .threshold = 50}},
        {"buckets 1m", {.kind = TSDB_MULTI_BUCKETS, .start = lo, .end = hi, .opts = asc, .interval_ms = 60000}},
        {"buckets one", {.kind = TSDB_MULTI_BUCKETS, .start = lo, .end = hi, .opts = asc}},
        {"buckets per interval", {.kind = TSDB_MULTI_BUCKETS, .starts = starts, .ends = ends, .opts = desc5,
                                  .intervals = ivs}},
        {"buckets utc hour", {.kind = TSDB_MULTI_BUCKETS, .start = lo, .end = hi, .opts = asc, .group = &utc_h}},
        {"buckets oslo day", {.kind = TSDB_MULTI_BUCKETS, .start = INT64_MIN, .end = INT64_MAX, .opts = asc,
                              .group = &oslo_d}},
        {"buckets oslo hour per range", {.kind = TSDB_MULTI_BUCKETS, .starts = starts, .ends = ends, .opts = asc,
                                         .group = &oslo_h}},
        {"buckets lord howe", {.kind = TSDB_MULTI_BUCKETS, .start = lo, .end = hi, .opts = desc5, .group = &lh}},
        {"bounds", {.kind = TSDB_MULTI_BUCKETS, .start = lo, .end = hi, .opts = asc, .bounds = bounds, .nbounds = 4}},
        {"spread 10m", {.kind = TSDB_MULTI_SPREAD, .start = lo, .end = hi, .opts = asc, .interval_ms = 600000}},
        {"spread oslo hour", {.kind = TSDB_MULTI_SPREAD, .start = lo, .end = hi, .opts = asc, .group = &oslo_h}},
        {"spread bounds", {.kind = TSDB_MULTI_SPREAD, .start = lo, .end = hi, .opts = asc, .bounds = bounds,
                           .nbounds = 4}},
        {"twa 5m", {.kind = TSDB_MULTI_TWA, .start = lo, .end = hi, .interval_ms = 300000, .twa = twa}},
        {"twa linear per interval", {.kind = TSDB_MULTI_TWA, .starts = starts, .ends = ends, .intervals = ivs,
                                     .twa = twal}},
        {"twa utc hour", {.kind = TSDB_MULTI_TWA, .start = lo, .end = hi, .group = &utc_h, .twa = twa}},
        {"twa oslo hour", {.kind = TSDB_MULTI_TWA, .start = lo, .end = hi, .group = &oslo_h, .twa = twa}},
        {"twa oslo day open", {.kind = TSDB_MULTI_TWA, .start = INT64_MIN, .end = INT64_MAX, .group = &oslo_d,
                               .twa = twa}},
        {"twa oslo hour per range", {.kind = TSDB_MULTI_TWA, .starts = starts, .ends = ends, .group = &oslo_h,
                                     .twa = twa}},
        {"twa bounds", {.kind = TSDB_MULTI_TWA, .start = lo, .end = hi, .bounds = bounds, .nbounds = 4, .twa = twa}},
    };
    for (size_t k = 0; k < sizeof cases / sizeof cases[0]; k++)
        compare(c, &cases[k].q, list, n, cases[k].what);

    /* stopping after the first delivery: CANCEL, the rest of the reply read, the connection usable */
    stopper sp = {0, 1};
    tsdb_multi_query vq = {.kind = TSDB_MULTI_VALUES, .start = lo, .end = hi, .opts = asc};
    CHECK(tsdbc_query_multi(c, &vq, list, n, NULL, stop_fn, &sp) == TSDB_ERR_STOPPED);
    CHECK(sp.calls == 1);
    compare(c, &cases[6].q, list, n, "after a stop");

    /* max_rows: the series that would pass it is not sent */
    tsdb_multi_opts mo = {.max_rows = 100};
    tsdb_series_result *r = NULL;
    CHECK(tsdbc_query_multi_collect(c, &cases[6].q, list, n, &mo, &r) == TSDB_ERR_TOO_MANY_BUCKETS);
    tsdbc_series_results_free(r, n);
    test_stats(c, &cases[6].q, list, n);
    test_stats(c, &cases[17].q, list, n);
    test_stats(c, &cases[0].q, list, n);

}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/test_server_multi-%d", base, (int)getpid());
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    if (system(cmd) != 0)
        return 1;
    mkdir(dir, 0755);
    static const char *const first[] = {"--send-timeout", "3", "--metrics-port", "auto", "--metrics-bind", "127.0.0.1",
                                        NULL};
    pid_t pid = start_server(dir, first);
    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    CHECK(tsdbc_server_version(c) == 10);
    load(c);

    int64_t lo = T0 - 7200000, hi = T0 + 4 * 3600000;
    tsdb_query_opts asc = {TSDB_ASC, 0};
    tsdb_series_result *r = NULL;
    run_cases(c);
    load_big(c);
    test_full_frame(c);
    test_cancel_pipeline();
    test_cancel_flood();
    test_disconnect(c, pid);
    test_stall(c, pid, 3);
    test_stall_small(pid);

    /* more than one request's worth of series: split, indexes running on */
    size_t big = TSDB_MULTI_MAX_SERIES + 3;
    uint32_t *many = malloc(big * 4);
    for (size_t i = 0; i < big; i++)
        many[i] = ids[i % 6];
    tsdb_multi_query one_q = {.kind = TSDB_MULTI_BUCKETS, .start = lo, .end = hi, .opts = asc};
    CHECK(tsdbc_query_multi_collect(c, &one_q, many, big, NULL, &r) == 0);
    for (size_t i = 0; r && i < big; i += 4099)
        CHECK(r[i].index == i && r[i].series == many[i] && r[i].status == 0 && r[i].n == r[i % 6].n);
    tsdbc_series_results_free(r, big);
    free(many);

    test_malformed(ids[0]);
    tsdbc_close(c);
    stop_server(pid);

    /* the same answers from a server with one query thread */
    static const char *const one_thread[] = {"--query-threads", "1", "--request-threads", "1", NULL};
    pid = start_server(dir, one_thread);
    OK(tsdbc_connect("127.0.0.1", port, &c));
    run_cases(c);
    test_full_frame(c);
    tsdbc_close(c);
    stop_server(pid);

    /* --multi-max-rows caps what a request may ask for: max_rows above it, or none, ends at the server's cap */
    static const char *const capped[] = {"--multi-max-rows", "10", NULL};
    pid = start_server(dir, capped);
    OK(tsdbc_connect("127.0.0.1", port, &c));
    tsdb_multi_query vq = {.kind = TSDB_MULTI_VALUES, .start = lo, .end = hi, .opts = asc};
    for (int k = 0; k < 2; k++) {
        tsdb_multi_opts mo = {.max_rows = k ? 1000000 : 0};
        r = NULL;
        int rc = tsdbc_query_multi_collect(c, &vq, ids, 6, &mo, &r);
        if (rc != TSDB_ERR_TOO_MANY_BUCKETS) {
            fprintf(stderr, "--multi-max-rows 10, max_rows %llu: %d, not TOO_MANY_BUCKETS\n",
                    (unsigned long long)mo.max_rows, rc);
            failures++;
        }
        if (r)
            tsdbc_series_results_free(r, 6);
    }
    tsdbc_close(c);
    stop_server(pid);
    snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    if (system(cmd) != 0)
        failures++;
    if (failures) {
        fprintf(stderr, "test_server_multi: %d failures\n", failures);
        return 1;
    }
    printf("test_server_multi passed\n");
    return 0;
}
