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

static void stop_server(pid_t pid)
{
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
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
    port = 20000 + (int)(getpid() % 20000);
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
    OK(tsdbc_stats(c, &st));
    tsdbc_close(c);

    stop_server(srv);

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
