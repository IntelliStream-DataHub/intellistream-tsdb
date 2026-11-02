/*
 * Prometheus metrics for tsdbd: GET /metrics in the text exposition format 0.0.4.
 *
 * The listener is its own socket on its own port (--metrics-port), served by
 * one thread of its own and one request per connection, so the event loop, the
 * workers and the main port never see a scraper. What a slow or hostile client
 * can do is bounded:
 *   - one poll loop watches up to MAX_PEND connections that have not finished
 *     their request or reply; a request must be complete within REQ_DEADLINE_MS of the
 *     accept (a total, so one byte a second does not help) and fits in REQ_MAX
 *     bytes; when the slots are full the oldest connection is dropped, so a
 *     crowd of idle connections cannot lock real scrapers out;
 *   - the reply is handed to the kernel within SEND_BUDGET_MS or dropped; it is
 *     sent without blocking, as the socket takes it (POLLOUT in the same loop), so
 *     a client that does not read delays nobody else;
 *   - only GET and HEAD of /metrics are answered, the connection always closes;
 *   - a slot holds at most one rendered reply, so at most MAX_PEND of them exist.
 * The thread never runs a query: it reads atomic counters and
 * tsdb_get_volume_stats (one short lock per volume).
 *
 * The server hooks (below the counters) only add to relaxed atomics: counters of
 * requests by op, errors by status code, a latency histogram for the query
 * ops, connections and busy workers. Everything else (rows, bytes, parts,
 * merges, WAL, follower lag) is read from the engine when scraped.
 */
#define _GNU_SOURCE
#include "metrics.h"
#include "tsdb_protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#ifndef TSDB_BUILD_VERSION
#define TSDB_BUILD_VERSION "dev"
#endif

#define REQ_MAX 4096
#define REQ_DEADLINE_MS 2000 /* from accept to the end of the request head */
#define SEND_BUDGET_MS 1000  /* to hand the reply to the kernel */

/* ------------------------------------------------------------ counters */

#define NOPS 64 /* TSDB_OP_* are below this; a request with another op counts as "other" */
#define OP_OTHER NOPS /* its own slot after the real ops: op 63 is not "other" */
#define NERR 10 /* -TSDB_ERR_* 1..9; slot 0 is anything else */
#define NBUCKETS 15

static const double bucket_le[NBUCKETS] = {0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
                                           0.25,   0.5,   1,      2.5,   5,    10,    30};
static const char *const bucket_str[NBUCKETS] = {"0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1",
                                                 "0.25",   "0.5",   "1",      "2.5",   "5",    "10",    "30"};

typedef struct {
    atomic_uint_fast64_t requests[NOPS + 1];
    atomic_uint_fast64_t hist[NOPS + 1][NBUCKETS + 1]; /* per bucket, not cumulative; the last is above 30 s */
    atomic_uint_fast64_t hist_sum_ns[NOPS + 1];
    atomic_uint_fast64_t errors[NERR];
    atomic_uint_fast64_t conns_accepted, conns_rejected;
    atomic_int_fast64_t conns_active, workers_busy;
    atomic_uint_fast64_t rows_inserted;
    atomic_uint_fast64_t multi_series, multi_deferred;
    atomic_uint_fast64_t scrapes, scrape_rejects;
} counters;
static counters M;
static atomic_bool on; /* set by metrics_enable before the server starts; the hooks do nothing until then */

/* Every TSDB_OP_* the server answers has a row here (test_metrics reads tsdb_protocol.h and fails
 * for an op without one, so a new op cannot be left out unnoticed). `query` ops get a latency histogram. */
static const struct {
    const char *name;
    bool query;
} ops[NOPS] = {
    [TSDB_OP_HELLO] = {"hello"},
    [TSDB_OP_SERIES] = {"series"},
    [TSDB_OP_SERIES_NAME] = {"series_name"},
    [TSDB_OP_INSERT] = {"insert"},
    [TSDB_OP_QUERY_RAW] = {"query_raw", true},
    [TSDB_OP_COMMIT] = {"commit"},
    [TSDB_OP_FLUSH] = {"flush"},
    [TSDB_OP_OPTIMIZE] = {"optimize"},
    [TSDB_OP_STATS] = {"stats"},
    [TSDB_OP_SERIES_CREATE] = {"series_create"},
    [TSDB_OP_SERIES_TYPE] = {"series_type"},
    [TSDB_OP_INSERT_VALUES] = {"insert_values"},
    [TSDB_OP_QUERY_VALUES] = {"query_values", true},
    [TSDB_OP_STATE] = {"state"},
    [TSDB_OP_STATE_NAME] = {"state_name"},
    [TSDB_OP_QUERY_BUCKETS] = {"query_buckets", true},
    [TSDB_OP_LAST_STATS] = {"last_stats"},
    [TSDB_OP_QUERY_BOUNDS] = {"query_bounds", true},
    [TSDB_OP_QUERY_LTTB] = {"query_lttb", true},
    [TSDB_OP_QUERY_TWA] = {"query_twa", true},
    [TSDB_OP_QUERY_MULTI] = {"query_multi", true},
    [TSDB_OP_CANCEL] = {"cancel"},
    [TSDB_OP_DELETE_RANGE] = {"delete_range"},
    [TSDB_OP_REPLACE_RANGE] = {"replace_range"},
    [TSDB_OP_SET_RETENTION] = {"set_retention"},
    [TSDB_OP_GET_RETENTION] = {"get_retention"},
};

static bool is_query_op(unsigned o) { return o < NOPS && ops[o].query; }

static const char *const err_name[NERR] = {"other", "io", "nomem", "corrupt", "invalid", "locked",
                                           "notfound", "too_many_buckets", "stopped", "readonly"};

static int64_t now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}

void metrics_enable(void) { atomic_store(&on, true); }
#define HOOK_OFF() if (!atomic_load_explicit(&on, memory_order_relaxed)) return

void metrics_conn_opened(void)
{
    HOOK_OFF();
    atomic_fetch_add_explicit(&M.conns_accepted, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&M.conns_active, 1, memory_order_relaxed);
}
void metrics_conn_closed(void)
{
    HOOK_OFF();
    atomic_fetch_sub_explicit(&M.conns_active, 1, memory_order_relaxed);
}
void metrics_conn_rejected(void)
{
    HOOK_OFF();
    atomic_fetch_add_explicit(&M.conns_rejected, 1, memory_order_relaxed);
}

int64_t metrics_request_begin(void)
{
    if (!atomic_load_explicit(&on, memory_order_relaxed))
        return 0;
    atomic_fetch_add_explicit(&M.workers_busy, 1, memory_order_relaxed);
    return now_ns();
}

int metrics_bucket_for(double seconds)
{
    int b = 0;
    while (b < NBUCKETS && seconds > bucket_le[b])
        b++;
    return b;
}

void metrics_request_end(uint8_t op, int64_t began)
{
    HOOK_OFF();
    int64_t ns = now_ns() - began;
    atomic_fetch_sub_explicit(&M.workers_busy, 1, memory_order_relaxed);
    unsigned o = op < NOPS && ops[op].name ? op : OP_OTHER;
    atomic_fetch_add_explicit(&M.requests[o], 1, memory_order_relaxed);
    if (is_query_op(o)) {
        int b = metrics_bucket_for((double)ns / 1e9);
        atomic_fetch_add_explicit(&M.hist[o][b], 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&M.hist_sum_ns[o], ns > 0 ? (uint64_t)ns : 0, memory_order_relaxed);
    }
}

void metrics_error(int32_t status)
{
    HOOK_OFF();
    int64_t k = -(int64_t)status;
    unsigned i = k >= 1 && k < NERR ? (unsigned)k : 0;
    atomic_fetch_add_explicit(&M.errors[i], 1, memory_order_relaxed);
}

void metrics_rows_inserted(uint64_t n)
{
    HOOK_OFF();
    atomic_fetch_add_explicit(&M.rows_inserted, n, memory_order_relaxed);
}

void metrics_multi_series(uint64_t n)
{
    HOOK_OFF();
    atomic_fetch_add_explicit(&M.multi_series, n, memory_order_relaxed);
}

void metrics_multi_deferred(void)
{
    HOOK_OFF();
    atomic_fetch_add_explicit(&M.multi_deferred, 1, memory_order_relaxed);
}

/* ------------------------------------------------------ text rendering */

typedef struct {
    char *p;
    size_t len, cap;
    bool oom;
} sb;

static void sb_add(sb *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void sb_add(sb *b, const char *fmt, ...)
{
    if (b->oom)
        return;
    for (;;) {
        va_list ap;
        va_start(ap, fmt);
        int n = vsnprintf(b->p ? b->p + b->len : NULL, b->cap - b->len, fmt, ap);
        va_end(ap);
        if (n < 0) {
            b->oom = true;
            return;
        }
        if ((size_t)n < b->cap - b->len) {
            b->len += (size_t)n;
            return;
        }
        size_t cap = b->cap ? b->cap * 2 : 8192;
        while (cap < b->len + (size_t)n + 1)
            cap *= 2;
        char *q = realloc(b->p, cap);
        if (!q) {
            b->oom = true;
            return;
        }
        b->p = q;
        b->cap = cap;
    }
}

/* A label value: backslash, double quote and newline escaped. */
static void sb_label(sb *b, const char *s)
{
    for (; *s; s++) {
        if (*s == '\\' || *s == '"')
            sb_add(b, "\\%c", *s);
        else if (*s == '\n')
            sb_add(b, "\\n");
        else
            sb_add(b, "%c", *s);
    }
}

static void family(sb *b, const char *name, const char *type, const char *help)
{
    sb_add(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, type);
}

static metrics_cfg cfg;
static time_t start_time;

static void render(sb *b)
{
    uint32_t nv = tsdb_volume_count(cfg.db);
    tsdb_volume_stats *vs = calloc(nv ? nv : 1, sizeof *vs);
    if (!vs) {
        b->oom = true;
        return;
    }
    for (uint32_t i = 0; i < nv; i++)
        if (tsdb_get_volume_stats(cfg.db, i, &vs[i]) != 0)
            memset(&vs[i], 0, sizeof vs[i]);

    /* "avx2, codecs x86-64-v3" -> simd avx2, codecs x86-64-v3 */
    char simd[64], codecs[64] = "native";
    snprintf(simd, sizeof simd, "%s", tsdb_simd_level());
    char *cp = strstr(simd, ", codecs ");
    if (cp) {
        snprintf(codecs, sizeof codecs, "%s", cp + 9);
        *cp = '\0';
    }
    family(b, "tsdb_build_info", "gauge", "Build and runtime: release version, build id, SIMD kernel set, codec CPU level (always 1).");
    sb_add(b, "tsdb_build_info{version=\"");
    sb_label(b, tsdb_version());
    sb_add(b, "\",build=\"");
    sb_label(b, TSDB_BUILD_VERSION);
    sb_add(b, "\",simd=\"");
    sb_label(b, simd);
    sb_add(b, "\",cpu_level=\"");
    sb_label(b, codecs);
    sb_add(b, "\"} 1\n");
    family(b, "tsdb_io_backend_info", "gauge", "Event loop backend of the main port (always 1).");
    sb_add(b, "tsdb_io_backend_info{backend=\"%s\"} 1\n", cfg.io);
    family(b, "tsdb_role_info", "gauge", "writer, read-only or follower (always 1).");
    sb_add(b, "tsdb_role_info{role=\"%s\"} 1\n", cfg.role);
    family(b, "tsdb_start_time_seconds", "gauge", "Start time of the server, Unix seconds.");
    sb_add(b, "tsdb_start_time_seconds %lld\n", (long long)start_time);

#define VOL_FAMILY(name, type, help, field)                                                                           \
    do {                                                                                                              \
        family(b, name, type, help);                                                                                  \
        for (uint32_t i = 0; i < nv; i++) {                                                                           \
            sb_add(b, name "{volume=\"%u\",path=\"", i);                                                              \
            sb_label(b, tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "");                              \
            sb_add(b, "\"} %llu\n", (unsigned long long)vs[i].field);                                                 \
        }                                                                                                             \
    } while (0)

    VOL_FAMILY("tsdb_rows", "gauge", "Rows in parts on disk, before merges drop overwritten ones.", rows);
    VOL_FAMILY("tsdb_disk_bytes", "gauge", "Bytes of the part files.", bytes);
    VOL_FAMILY("tsdb_parts", "gauge", "Part files.", parts);
    VOL_FAMILY("tsdb_partitions", "gauge", "Time partitions that have parts.", partitions);
    VOL_FAMILY("tsdb_granules", "gauge", "Granules in all parts.", granules);
    VOL_FAMILY("tsdb_memtable_rows", "gauge", "Rows in memtables (WAL-backed, not yet in parts).", memtable_rows);
    VOL_FAMILY("tsdb_merges_total", "counter", "Merges finished since start.", merges);
    VOL_FAMILY("tsdb_merged_rows_total", "counter", "Rows written by merges since start.", merged_rows);
    VOL_FAMILY("tsdb_merging_parts", "gauge", "Parts a running merge reads now.", merging_parts);
    VOL_FAMILY("tsdb_wal_bytes_total", "counter", "Bytes written to the WAL since start.", wal_bytes);
    VOL_FAMILY("tsdb_wal_syncs_total", "counter", "WAL fdatasync calls since start.", wal_syncs);
    VOL_FAMILY("tsdb_broken_parts", "gauge", "Parts a follower found damaged and left out.", broken_parts);
    VOL_FAMILY("tsdb_failed", "gauge", "1 once the volume is in its fail-stop state (writes return IO errors).",
               failed);
    if (strcmp(cfg.role, "follower") == 0) {
        VOL_FAMILY("tsdb_follow_errors_total", "counter", "Follower refreshes that failed.", follow_errors);
        family(b, "tsdb_follow_refresh_age_seconds", "gauge",
               "Seconds since the follower last refreshed from its writer successfully.");
        for (uint32_t i = 0; i < nv; i++) {
            sb_add(b, "tsdb_follow_refresh_age_seconds{volume=\"%u\",path=\"", i);
            sb_label(b, tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "");
            sb_add(b, "\"} %.3f\n", (double)vs[i].follow_refresh_age_ms / 1000.0);
        }
    }
    free(vs);

    family(b, "tsdb_requests_total", "counter", "Requests handled by the main port, by operation.");
    for (unsigned o = 1; o <= NOPS; o++)
        if (o == OP_OTHER || ops[o].name)
            sb_add(b, "tsdb_requests_total{op=\"%s\"} %llu\n", o == OP_OTHER ? "other" : ops[o].name,
                   (unsigned long long)atomic_load_explicit(&M.requests[o], memory_order_relaxed));
    family(b, "tsdb_inserted_rows_total", "counter", "Rows accepted by successful insert requests.");
    sb_add(b, "tsdb_inserted_rows_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.rows_inserted, memory_order_relaxed));
    family(b, "tsdb_multi_series_total", "counter", "Series answered by QUERY_MULTI requests.");
    sb_add(b, "tsdb_multi_series_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.multi_series, memory_order_relaxed));
    family(b, "tsdb_multi_deferred_total", "counter",
           "QUERY_MULTI series too large to hold ahead of their turn, run again at it.");
    sb_add(b, "tsdb_multi_deferred_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.multi_deferred, memory_order_relaxed));
    family(b, "tsdb_request_errors_total", "counter", "Replies with an error status, by TSDB_ERR_* code.");
    for (unsigned k = 1; k < NERR; k++)
        sb_add(b, "tsdb_request_errors_total{code=\"%s\"} %llu\n", err_name[k],
               (unsigned long long)atomic_load_explicit(&M.errors[k], memory_order_relaxed));
    sb_add(b, "tsdb_request_errors_total{code=\"other\"} %llu\n",
           (unsigned long long)atomic_load_explicit(&M.errors[0], memory_order_relaxed));

    family(b, "tsdb_query_duration_seconds", "histogram",
           "Time a worker spent on a query request, from taking it to handing the reply to the kernel (a slow client can add to it).");
    for (unsigned o = 1; o < NOPS; o++) {
        if (!is_query_op(o))
            continue;
        uint64_t cum = 0;
        for (int k = 0; k < NBUCKETS; k++) {
            cum += atomic_load_explicit(&M.hist[o][k], memory_order_relaxed);
            sb_add(b, "tsdb_query_duration_seconds_bucket{op=\"%s\",le=\"%s\"} %llu\n", ops[o].name, bucket_str[k],
                   (unsigned long long)cum);
        }
        cum += atomic_load_explicit(&M.hist[o][NBUCKETS], memory_order_relaxed);
        sb_add(b, "tsdb_query_duration_seconds_bucket{op=\"%s\",le=\"+Inf\"} %llu\n", ops[o].name,
               (unsigned long long)cum);
        sb_add(b, "tsdb_query_duration_seconds_sum{op=\"%s\"} %.9g\n", ops[o].name,
               (double)atomic_load_explicit(&M.hist_sum_ns[o], memory_order_relaxed) / 1e9);
        sb_add(b, "tsdb_query_duration_seconds_count{op=\"%s\"} %llu\n", ops[o].name, (unsigned long long)cum);
    }

    int64_t active = atomic_load_explicit(&M.conns_active, memory_order_relaxed);
    family(b, "tsdb_connections", "gauge", "Open client connections on the main port.");
    sb_add(b, "tsdb_connections %lld\n", (long long)(active > 0 ? active : 0));
    family(b, "tsdb_connections_max", "gauge", "The --max-connections limit.");
    sb_add(b, "tsdb_connections_max %zu\n", cfg.max_conns);
    family(b, "tsdb_connections_accepted_total", "counter", "Connections accepted on the main port.");
    sb_add(b, "tsdb_connections_accepted_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.conns_accepted, memory_order_relaxed));
    family(b, "tsdb_connections_rejected_total", "counter", "Connections closed at once for the connection limit.");
    sb_add(b, "tsdb_connections_rejected_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.conns_rejected, memory_order_relaxed));
    int64_t busy = atomic_load_explicit(&M.workers_busy, memory_order_relaxed);
    family(b, "tsdb_workers", "gauge", "Worker threads (the --workers count, plus extras started for slow clients).");
    sb_add(b, "tsdb_workers %zu\n", cfg.workers ? cfg.workers() : 0);
    family(b, "tsdb_request_memory_bytes", "gauge",
           "Requests received and not yet run, large receive buffers and packed inserts being unpacked.");
    sb_add(b, "tsdb_request_memory_bytes %zu\n", cfg.request_memory ? cfg.request_memory() : 0);
    family(b, "tsdb_request_memory_limit_bytes", "gauge", "--max-request-memory.");
    sb_add(b, "tsdb_request_memory_limit_bytes %zu\n", cfg.request_memory_limit);
    family(b, "tsdb_workers_busy", "gauge", "Worker threads running a request now.");
    sb_add(b, "tsdb_workers_busy %lld\n", (long long)(busy > 0 ? busy : 0));
    family(b, "tsdb_metrics_scrapes_total", "counter", "Scrapes of this endpoint answered.");
    sb_add(b, "tsdb_metrics_scrapes_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.scrapes, memory_order_relaxed));
    family(b, "tsdb_metrics_rejected_total", "counter", "Requests to this port that were not a valid GET /metrics.");
    sb_add(b, "tsdb_metrics_rejected_total %llu\n",
           (unsigned long long)atomic_load_explicit(&M.scrape_rejects, memory_order_relaxed));
}

/* ---------------------------------------------------------------- HTTP */

static int64_t mono_ms_now(void) { return now_ns() / 1000000; }

/* Connections that have not sent a whole request head yet, or not taken the whole reply. One thread
 * watches them all with poll, so a client that sends nothing, or a byte a second, holds a slot for
 * REQ_DEADLINE_MS and nothing else, and one that does not read its reply holds it for SEND_BUDGET_MS:
 * other scrapers are served meanwhile. When all slots are taken the oldest connection (the likeliest
 * to be dead weight) is closed to make room. */
#define MAX_PEND 64
typedef struct {
    int fd;
    int64_t deadline; /* monotonic ms: of the request head, then of the reply */
    char *out;        /* the reply (malloc'd); NULL while the request is read */
    size_t out_len, out_off;
    bool scrape; /* a 200 for /metrics: counted once all of it was sent */
    size_t n;
    char buf[REQ_MAX];
} pend;
static pend P[MAX_PEND];
static size_t npend;

static void pend_close(size_t i)
{
    close(P[i].fd);
    free(P[i].out);
    if (i + 1 < npend)
        memcpy(&P[i], &P[npend - 1], offsetof(pend, buf) + P[npend - 1].n); /* header and the bytes so far */
    npend--;
}

/* Sends what the socket takes now: 1 all sent, 0 the rest waits for POLLOUT, -1 failed. */
static int send_some(pend *q)
{
    while (q->out_off < q->out_len) {
        ssize_t w = send(q->fd, q->out + q->out_off, q->out_len - q->out_off, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (w > 0)
            q->out_off += (size_t)w;
        else if (w < 0 && errno == EAGAIN)
            return 0;
        else if (w == 0 || errno != EINTR)
            return -1;
    }
    return 1;
}

/* An error reply: a status line and the text. */
static void reply_simple(pend *q, int code, const char *text)
{
    q->out = malloc(256);
    if (!q->out)
        return; /* closed without a reply */
    int n = snprintf(q->out, 256,
                     "HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s\n",
                     code, text, strlen(text) + 1, text);
    q->out_len = (size_t)n;
}

/* The request head in q->buf is complete: puts the reply in q->out. */
static void answer(pend *q)
{
    const char *buf = q->buf;
    size_t n = q->n;
    /* request line: METHOD SP target SP HTTP/1.x */
    const char *sp1 = memchr(buf, ' ', n);
    const char *eol = memchr(buf, '\n', n);
    const char *sp2 = sp1 && eol && sp1 + 1 < eol ? memchr(sp1 + 1, ' ', (size_t)(eol - sp1 - 1)) : NULL;
    if (!sp1 || !eol || !sp2 || memchr(buf, 0, n)) {
        atomic_fetch_add(&M.scrape_rejects, 1);
        reply_simple(q, 400, "Bad Request");
        return;
    }
    bool get = sp1 - buf == 3 && memcmp(buf, "GET", 3) == 0, head = sp1 - buf == 4 && memcmp(buf, "HEAD", 4) == 0;
    if (!get && !head) {
        atomic_fetch_add(&M.scrape_rejects, 1);
        reply_simple(q, 405, "Method Not Allowed");
        return;
    }
    size_t tl = (size_t)(sp2 - sp1 - 1), pl = 0;
    const char *t = sp1 + 1;
    while (pl < tl && t[pl] != '?')
        pl++;
    if (pl != 8 || memcmp(t, "/metrics", 8) != 0) {
        atomic_fetch_add(&M.scrape_rejects, 1);
        reply_simple(q, 404, "Not Found");
        return;
    }
    sb b = {0};
    render(&b);
    char h[256];
    int hn = snprintf(h, sizeof h,
                      "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                      "Content-Length: %zu\r\nConnection: close\r\n\r\n",
                      b.len);
    size_t body = head ? 0 : b.len;
    q->out = b.oom ? NULL : malloc((size_t)hn + body);
    if (!q->out) {
        free(b.p);
        reply_simple(q, 500, "Internal Server Error");
        return;
    }
    memcpy(q->out, h, (size_t)hn);
    if (body)
        memcpy(q->out + hn, b.p, body);
    free(b.p);
    q->out_len = (size_t)hn + body;
    q->scrape = true;
}

static pthread_t thread;
static bool started;
static atomic_bool stop_flag;

static void accept_some(void)
{
    for (int k = 0; k < 16; k++) {
        int fd = accept4(cfg.listen_fd, NULL, NULL, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
                struct timespec d = {0, 10000000}; /* out of descriptors: let some close */
                nanosleep(&d, NULL);
            }
            return; /* EAGAIN: all taken */
        }
        if (npend == MAX_PEND) {
            size_t oldest = 0;
            for (size_t i = 1; i < npend; i++)
                if (P[i].deadline < P[oldest].deadline)
                    oldest = i;
            atomic_fetch_add(&M.scrape_rejects, 1);
            pend_close(oldest);
        }
        P[npend].fd = fd;
        P[npend].deadline = mono_ms_now() + REQ_DEADLINE_MS;
        P[npend].n = 0;
        P[npend].out = NULL;
        P[npend].out_len = P[npend].out_off = 0;
        P[npend].scrape = false;
        npend++;
    }
}

static void *metrics_main(void *arg)
{
    (void)arg;
    struct pollfd pf[MAX_PEND + 1];
    while (!atomic_load(&stop_flag)) {
        pf[0] = (struct pollfd){.fd = cfg.listen_fd, .events = POLLIN};
        int64_t now = mono_ms_now(), wake = now + 200; /* also the latency of stopping */
        for (size_t i = 0; i < npend; i++) {
            pf[i + 1] = (struct pollfd){.fd = P[i].fd, .events = P[i].out ? POLLOUT : POLLIN};
            if (P[i].deadline < wake)
                wake = P[i].deadline;
        }
        int r = poll(pf, npend + 1, (int)(wake > now ? wake - now : 0));
        if (r < 0 && errno != EINTR)
            break;
        /* pf[i + 1] is P[i] as it was at the poll; walk backwards, so closing swaps in only entries already seen */
        size_t polled = npend;
        for (size_t i = polled; i-- > 0;) {
            pend *q = &P[i];
            bool done = false;
            if (q->out) {
                if (r > 0 && pf[i + 1].revents) {
                    int s = send_some(q);
                    if (s == 1 && q->scrape)
                        atomic_fetch_add(&M.scrapes, 1);
                    done = s != 0;
                }
                if (!done && mono_ms_now() >= q->deadline)
                    done = true; /* did not take its reply in time */
            } else if (r > 0 && (pf[i + 1].revents & (POLLIN | POLLHUP | POLLERR))) {
                ssize_t got = recv(q->fd, q->buf + q->n, REQ_MAX - q->n, MSG_DONTWAIT);
                if (got > 0) {
                    q->n += (size_t)got;
                    bool answered = true;
                    if (memmem(q->buf, q->n, "\r\n\r\n", 4) || memmem(q->buf, q->n, "\n\n", 2)) {
                        answer(q);
                    } else if (q->n == REQ_MAX) {
                        atomic_fetch_add(&M.scrape_rejects, 1);
                        reply_simple(q, 431, "Request Header Fields Too Large");
                    } else {
                        answered = false;
                    }
                    if (answered && q->out) { /* most replies fit the socket buffer at once */
                        q->deadline = mono_ms_now() + SEND_BUDGET_MS;
                        int s = send_some(q);
                        if (s == 1 && q->scrape)
                            atomic_fetch_add(&M.scrapes, 1);
                        done = s != 0;
                    } else if (answered) {
                        done = true; /* no memory for the reply */
                    }
                } else if (got == 0 || (errno != EAGAIN && errno != EINTR)) {
                    done = true; /* gone before it finished */
                }
            }
            if (!done && !q->out && mono_ms_now() >= q->deadline) {
                atomic_fetch_add(&M.scrape_rejects, 1);
                done = true; /* too slow: closed without a reply */
            }
            if (done) {
                char junk[REQ_MAX]; /* a body that came with the head: unread data would turn the close into a reset */
                recv(q->fd, junk, sizeof junk, MSG_DONTWAIT);
                pend_close(i);
            }
        }
        if (r > 0 && (pf[0].revents & POLLIN))
            accept_some();
    }
    while (npend)
        pend_close(npend - 1);
    return NULL;
}

int metrics_start(const metrics_cfg *c)
{
    cfg = *c;
    start_time = time(NULL);
    int fl = fcntl(cfg.listen_fd, F_GETFL);
    if (fl < 0 || fcntl(cfg.listen_fd, F_SETFL, fl | O_NONBLOCK) != 0) /* accept_some loops until EAGAIN */
        return -errno;
    atomic_store(&stop_flag, false);
    int rc = pthread_create(&thread, NULL, metrics_main, NULL);
    if (rc)
        return -rc;
    started = true;
    return 0;
}

void metrics_stop(void)
{
    if (!started)
        return;
    atomic_store(&stop_flag, true);
    pthread_join(thread, NULL);
    close(cfg.listen_fd);
    started = false;
}
