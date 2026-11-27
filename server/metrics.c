/* SPDX-License-Identifier: AGPL-3.0-only */
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
#include "creport.h" /* struct creport_snap layout only: no direct call, so binaries without creport.o still link */
#include "margins.h" /* enum margin_tier only: no call, so every binary with metrics.c links without margins.c */
#include "tsdb_protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>
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

#include "../src/compat.h" /* macOS: accept4, thread stacks; nothing on Linux */

#ifndef TSDB_BUILD_VERSION
#define TSDB_BUILD_VERSION "dev"
#endif

#define REQ_MAX 4096
#define REQ_DEADLINE_MS 2000 /* from accept to the end of the request head */
#define SEND_BUDGET_MS 1000  /* to hand the reply to the kernel */

/* ------------------------------------------------------------ counters */

#define NOPS 64 /* TSDB_OP_* are below this; a request with another op counts as "other" */
#define OP_OTHER NOPS /* its own slot after the real ops: op 63 is not "other" */
#define NERR 18 /* -TSDB_ERR_* 1..17 with a name in err_name; slot 0 is anything else */
#define NBUCKETS 15

static const double bucket_le[NBUCKETS] = {0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
                                           0.25,   0.5,   1,      2.5,   5,    10,    30};
static const char *const bucket_str[NBUCKETS] = {"0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1",
                                                 "0.25",   "0.5",   "1",      "2.5",   "5",    "10",    "30"};

enum { QOP_RAW, QOP_VALUES, QOP_BUCKETS, QOP_BOUNDS, QOP_LTTB, QOP_TWA, QOP_MULTI, QOP_LATEST, NQOPS };

static int query_op_idx(unsigned o)
{
    switch (o) {
    case TSDB_OP_QUERY_RAW: return QOP_RAW;
    case TSDB_OP_QUERY_VALUES: return QOP_VALUES;
    case TSDB_OP_QUERY_BUCKETS: return QOP_BUCKETS;
    case TSDB_OP_QUERY_BOUNDS: return QOP_BOUNDS;
    case TSDB_OP_QUERY_LTTB: return QOP_LTTB;
    case TSDB_OP_QUERY_TWA: return QOP_TWA;
    case TSDB_OP_QUERY_MULTI: return QOP_MULTI;
    case TSDB_OP_QUERY_LATEST: return QOP_LATEST;
    default: return -1;
    }
}

typedef struct metrics_shard {
    _Alignas(64) struct metrics_shard *next; /* protected by shard_mu */
    atomic_int_fast64_t workers_busy;        /* per-thread busy count; summed on scrape */
    atomic_uint_fast64_t rows_inserted;
    atomic_uint_fast64_t multi_series, multi_deferred;
    atomic_uint_fast64_t logins[METRICS_LOGIN_KINDS];
    atomic_uint_fast64_t errors[NERR];
    atomic_uint_fast64_t requests[NOPS + 1];
    atomic_uint_fast64_t hist_sum_ns[NQOPS];
    atomic_uint_fast64_t hist[NQOPS][NBUCKETS + 1]; /* per bucket, not cumulative; the last is above 30 s */
    atomic_uint_fast64_t wal_sync_hist[NBUCKETS + 1]; /* WAL sync duration buckets (for §8.4 p99 halving) */
} metrics_shard;
_Static_assert(sizeof(metrics_shard) % 64 == 0, "metrics_shard must fill whole cache lines");
_Static_assert(NBUCKETS + 1 == METRICS_HIST_BUCKETS, "METRICS_HIST_BUCKETS must equal NBUCKETS + 1");

typedef struct metrics_totals {
    uint64_t requests[NOPS + 1];
    uint64_t hist[NQOPS][NBUCKETS + 1];
    uint64_t hist_sum_ns[NQOPS];
    uint64_t wal_sync_hist[NBUCKETS + 1];
    uint64_t errors[NERR];
    uint64_t rows_inserted;
    uint64_t multi_series, multi_deferred;
    uint64_t logins[METRICS_LOGIN_KINDS];
    int64_t workers_busy;
} metrics_totals;

typedef struct {
    _Alignas(64) atomic_uint_fast64_t conns_accepted;
    atomic_uint_fast64_t conns_rejected;
    atomic_int_fast64_t conns_active;
    _Alignas(64) atomic_uint_fast64_t scrapes;
    atomic_uint_fast64_t scrape_rejects;
} global_counters;
_Static_assert(sizeof(global_counters) == 128, "global_counters must occupy two cache lines");
static global_counters M;

/* set by metrics_enable before the server starts; the hooks do nothing until then */
static struct {
    _Alignas(64) atomic_bool v;
} metrics_on;
_Static_assert(sizeof(metrics_on) == 64, "metrics_on must occupy its own cache line");

static struct {
    _Alignas(64) pthread_mutex_t mu;
    metrics_shard *list;
    metrics_totals retired; /* folded counts of exited threads, protected by mu */
} shard_reg = {.mu = PTHREAD_MUTEX_INITIALIZER};
_Static_assert(sizeof(shard_reg) % 64 == 0, "shard_reg must fill whole cache lines");

static __thread metrics_shard *tls_shard;
static __thread int tls_busy_counted;
static pthread_key_t shard_key;
static bool shard_key_ok;
static pthread_once_t shard_once = PTHREAD_ONCE_INIT;

/* Every TSDB_OP_* has a row here, made from the op list (TSDB_OPS in tsdb_protocol.h; test_metrics checks each
 * appears). `query` ops get a latency histogram. */
#define OP_NAME_(name, num, metric) [num] = {#metric, false},
static struct {
    const char *name;
    bool query;
} ops[NOPS] = {TSDB_OPS(OP_NAME_)};
#undef OP_NAME_
_Static_assert(TSDB_OP_USER_LIST < NOPS, "an op number past the metrics table");

/* by -TSDB_ERR_*; NULL slots (-10 to -15, ha.md's) are counted as "other" until they exist */
static const char *const err_name[NERR] = {"other",  "io",      "nomem",   "corrupt", "invalid", "locked",
                                           "notfound", "too_many_buckets", "stopped", "readonly", NULL, NULL,
                                           NULL,     NULL,      NULL,      NULL,      "denied",  "auth"};

static int64_t now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}

static inline void shard_add(atomic_uint_fast64_t *p, uint64_t n)
{
    atomic_store_explicit(p, atomic_load_explicit(p, memory_order_relaxed) + n, memory_order_relaxed);
}

static inline void shard_add_i64(atomic_int_fast64_t *p, int64_t d)
{
    atomic_store_explicit(p, atomic_load_explicit(p, memory_order_relaxed) + d, memory_order_relaxed);
}

static void shard_fold_into(metrics_totals *dst, const metrics_shard *s)
{
    dst->workers_busy += atomic_load_explicit(&s->workers_busy, memory_order_relaxed);
    dst->rows_inserted += atomic_load_explicit(&s->rows_inserted, memory_order_relaxed);
    dst->multi_series += atomic_load_explicit(&s->multi_series, memory_order_relaxed);
    dst->multi_deferred += atomic_load_explicit(&s->multi_deferred, memory_order_relaxed);
    for (int k = 0; k < METRICS_LOGIN_KINDS; k++)
        dst->logins[k] += atomic_load_explicit(&s->logins[k], memory_order_relaxed);
    for (unsigned k = 0; k < NERR; k++)
        dst->errors[k] += atomic_load_explicit(&s->errors[k], memory_order_relaxed);
    for (unsigned o = 0; o <= NOPS; o++)
        dst->requests[o] += atomic_load_explicit(&s->requests[o], memory_order_relaxed);
    for (int q = 0; q < NQOPS; q++) {
        dst->hist_sum_ns[q] += atomic_load_explicit(&s->hist_sum_ns[q], memory_order_relaxed);
        for (int b = 0; b <= NBUCKETS; b++)
            dst->hist[q][b] += atomic_load_explicit(&s->hist[q][b], memory_order_relaxed);
    }
    for (int b = 0; b <= NBUCKETS; b++)
        dst->wal_sync_hist[b] += atomic_load_explicit(&s->wal_sync_hist[b], memory_order_relaxed);
}

static void shard_dtor(void *p)
{
    metrics_shard *s = p;
    if (!s)
        return;
    if (tls_shard == s) {
        tls_shard = NULL;
        tls_busy_counted = 0;
    }
    pthread_mutex_lock(&shard_reg.mu);
    for (metrics_shard **pp = &shard_reg.list; *pp; pp = &(*pp)->next) {
        if (*pp == s) {
            *pp = s->next;
            break;
        }
    }
    shard_fold_into(&shard_reg.retired, s);
    pthread_mutex_unlock(&shard_reg.mu);
    free(s);
}

static void shard_atexit(void)
{
    metrics_shard *s = tls_shard;
    if (!s)
        return;
    if (shard_key_ok)
        pthread_setspecific(shard_key, NULL);
    shard_dtor(s);
}

static void shard_key_init(void)
{
    shard_key_ok = pthread_key_create(&shard_key, shard_dtor) == 0;
    if (shard_key_ok)
        atexit(shard_atexit);
}

static metrics_shard *shard_alloc(void)
{
    pthread_once(&shard_once, shard_key_init);
    if (!shard_key_ok)
        return NULL;
    metrics_shard *s = aligned_alloc(64, sizeof *s);
    if (!s)
        return NULL;
    memset(s, 0, sizeof *s);
    if (pthread_setspecific(shard_key, s) != 0) {
        free(s);
        return NULL;
    }
    pthread_mutex_lock(&shard_reg.mu);
    s->next = shard_reg.list;
    shard_reg.list = s;
    pthread_mutex_unlock(&shard_reg.mu);
    tls_shard = s;
    return s;
}

static inline metrics_shard *shard_get(void)
{
    metrics_shard *s = tls_shard;
    return s ? s : shard_alloc();
}

static void metrics_snapshot(metrics_totals *out)
{
    pthread_mutex_lock(&shard_reg.mu);
    *out = shard_reg.retired;
    for (const metrics_shard *s = shard_reg.list; s; s = s->next)
        shard_fold_into(out, s);
    pthread_mutex_unlock(&shard_reg.mu);
}

void metrics_enable(void) { atomic_store(&metrics_on.v, true); }
#define HOOK_OFF() if (!atomic_load_explicit(&metrics_on.v, memory_order_relaxed)) return

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

int64_t metrics_request_begin(uint8_t op)
{
    if (!atomic_load_explicit(&metrics_on.v, memory_order_relaxed))
        return 0;
    metrics_shard *s = shard_get();
    if (s) {
        shard_add_i64(&s->workers_busy, 1);
        tls_busy_counted++;
    }
    return query_op_idx(op) >= 0 ? now_ns() : 0;
}

int metrics_bucket_for(double seconds)
{
    int b = 0;
    while (b < NBUCKETS && seconds > bucket_le[b])
        b++;
    return b;
}

void metrics_wal_sync(int64_t ns)
{
    HOOK_OFF();
    metrics_shard *s = shard_get();
    if (!s)
        return;
    int b = metrics_bucket_for(ns > 0 ? (double)ns / 1e9 : 0.0);
    shard_add(&s->wal_sync_hist[b], 1);
}

double metrics_hist_p99(const uint64_t delta[METRICS_HIST_BUCKETS])
{
    uint64_t total = 0;
    for (int b = 0; b < METRICS_HIST_BUCKETS; b++)
        total += delta[b];
    if (total == 0)
        return 0.0;
    double target = 0.99 * (double)total;
    uint64_t cum = 0;
    for (int b = 0; b < NBUCKETS; b++) {
        uint64_t next = cum + delta[b];
        if ((double)next >= target && delta[b] > 0) {
            double lo = b == 0 ? 0.0 : bucket_le[b - 1];
            double hi = bucket_le[b];
            double frac = (target - (double)cum) / (double)delta[b];
            if (frac < 0.0)
                frac = 0.0;
            if (frac > 1.0)
                frac = 1.0;
            return lo + (hi - lo) * frac;
        }
        cum = next;
    }
    return bucket_le[NBUCKETS - 1];
}

void metrics_p99_win_init(metrics_p99_win *w)
{
    if (w)
        memset(w, 0, sizeof *w);
}

static void p99_series_step(metrics_p99_series *s, const uint64_t cum[METRICS_HIST_BUCKETS])
{
    if (!cum)
        return;
    int cur_idx = s->sec_head;
    memcpy(s->sec_ring[cur_idx], cum, METRICS_HIST_BUCKETS * sizeof cum[0]);
    s->sec_head = (s->sec_head + 1) % 61;
    if (s->sec_count < 61)
        s->sec_count++;
    if (s->sec_count < 2) {
        s->last_p99_1m = 0.0;
        return;
    }
    int old_idx = (s->sec_head - s->sec_count + 61) % 61;
    uint64_t delta[METRICS_HIST_BUCKETS];
    for (int b = 0; b < METRICS_HIST_BUCKETS; b++) {
        uint64_t c = s->sec_ring[cur_idx][b];
        uint64_t o = s->sec_ring[old_idx][b];
        delta[b] = c >= o ? c - o : 0;
    }
    double p99 = metrics_hist_p99(delta);
    s->last_p99_1m = p99;
    if (++s->sec_in_min >= 60) {
        s->sec_in_min = 0;
        if (p99 > 0.0) {
            s->min_p99[s->min_head] = p99;
            s->min_head = (s->min_head + 1) % 60;
            if (s->min_count < 60)
                s->min_count++;
        }
    }
    if (s->min_count > 0) {
        double tmp[60];
        int n = s->min_count;
        for (int i = 0; i < n; i++) {
            double v = s->min_p99[i];
            int j = i;
            while (j > 0 && tmp[j - 1] > v) {
                tmp[j] = tmp[j - 1];
                j--;
            }
            tmp[j] = v;
        }
        s->last_median_1h = (n & 1) ? tmp[n / 2] : 0.5 * (tmp[n / 2 - 1] + tmp[n / 2]);
    } else {
        s->last_median_1h = 0.0;
    }
}

int metrics_p99_win_step(metrics_p99_win *w, const uint64_t query_cum[METRICS_HIST_BUCKETS],
                         const uint64_t wal_cum[METRICS_HIST_BUCKETS])
{
    if (!w)
        return 0;
    p99_series_step(&w->query, query_cum);
    p99_series_step(&w->wal_sync, wal_cum);
    int q_hi = w->query.last_p99_1m > 0.0 && w->query.last_median_1h > 0.0 &&
               w->query.last_p99_1m > 2.0 * w->query.last_median_1h;
    int w_hi = w->wal_sync.last_p99_1m > 0.0 && w->wal_sync.last_median_1h > 0.0 &&
               w->wal_sync.last_p99_1m > 2.0 * w->wal_sync.last_median_1h;
    w->halved = (q_hi || w_hi) ? 1 : 0;
    return w->halved;
}

void metrics_p99_snapshot(uint64_t query_cum[METRICS_HIST_BUCKETS], uint64_t wal_cum[METRICS_HIST_BUCKETS])
{
    metrics_totals tot;
    metrics_snapshot(&tot);
    if (query_cum) {
        memset(query_cum, 0, METRICS_HIST_BUCKETS * sizeof query_cum[0]);
        for (int q = 0; q < NQOPS; q++)
            for (int b = 0; b < METRICS_HIST_BUCKETS; b++)
                query_cum[b] += tot.hist[q][b];
    }
    if (wal_cum)
        memcpy(wal_cum, tot.wal_sync_hist, METRICS_HIST_BUCKETS * sizeof wal_cum[0]);
}

void metrics_request_end(uint8_t op, int64_t began)
{
    HOOK_OFF();
    unsigned o = op < NOPS && ops[op].name ? op : OP_OTHER;
    int q = query_op_idx(o);
    int64_t ns = q >= 0 ? now_ns() - began : 0;
    metrics_shard *s = shard_get();
    if (!s)
        return;
    if (tls_busy_counted > 0) {
        tls_busy_counted--;
        shard_add_i64(&s->workers_busy, -1);
    }
    shard_add(&s->requests[o], 1);
    if (q >= 0) {
        int b = metrics_bucket_for((double)ns / 1e9);
        shard_add(&s->hist[q][b], 1);
        shard_add(&s->hist_sum_ns[q], ns > 0 ? (uint64_t)ns : 0);
    }
}

void metrics_error(int32_t status)
{
    HOOK_OFF();
    metrics_shard *s = shard_get();
    if (!s)
        return;
    int64_t k = -(int64_t)status;
    unsigned i = k >= 1 && k < NERR && err_name[k] ? (unsigned)k : 0;
    shard_add(&s->errors[i], 1);
}

void metrics_login(int what)
{
    HOOK_OFF();
    if (what >= 0 && what < METRICS_LOGIN_KINDS) {
        metrics_shard *s = shard_get();
        if (s)
            shard_add(&s->logins[what], 1);
    }
}

void metrics_rows_inserted(uint64_t n)
{
    HOOK_OFF();
    metrics_shard *s = shard_get();
    if (s)
        shard_add(&s->rows_inserted, n);
}

void metrics_multi_series(uint64_t n)
{
    HOOK_OFF();
    metrics_shard *s = shard_get();
    if (s)
        shard_add(&s->multi_series, n);
}

void metrics_multi_deferred(void)
{
    HOOK_OFF();
    metrics_shard *s = shard_get();
    if (s)
        shard_add(&s->multi_deferred, 1);
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

static void render_creport_samples(sb *b, const struct creport_snap_family *f)
{
    for (size_t j = 0; j < f->nsamples; j++) {
        const struct creport_snap_sample *s = &f->samples[j];
        sb_add(b, "%s", f->name);
        if (s->nlabels > 0) {
            sb_add(b, "{");
            for (uint8_t k = 0; k < s->nlabels; k++) {
                sb_add(b, "%s%s=\"", k ? "," : "", s->labels[k].key);
                sb_label(b, s->labels[k].val);
                sb_add(b, "\"");
            }
            sb_add(b, "}");
        }
        if (s->val_kind == CREPORT_VAL_I64)
            sb_add(b, " %lld\n", (long long)s->ival);
        else if (s->val_kind == CREPORT_VAL_U64)
            sb_add(b, " %llu\n", (unsigned long long)s->uval);
        else
            sb_add(b, " %.9g\n", s->dval);
    }
}

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
    struct creport_snap *cs = (cfg.cluster_snap && cfg.cluster_snap_free) ? cfg.cluster_snap() : NULL;

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
    VOL_FAMILY("tsdb_disk_bytes", "gauge", "Bytes of the part files (and per local cluster disk by use).", bytes);
    if (cs) {
        for (size_t i = 0; i < cs->nfamilies; i++) {
            if (strcmp(cs->families[i].name, "tsdb_disk_bytes") == 0)
                render_creport_samples(b, &cs->families[i]);
        }
    }
    VOL_FAMILY("tsdb_parts", "gauge", "Part files.", parts);
    VOL_FAMILY("tsdb_partitions", "gauge", "Time partitions that have parts.", partitions);
    VOL_FAMILY("tsdb_granules", "gauge", "Granules in all parts.", granules);
    VOL_FAMILY("tsdb_memtable_rows", "gauge", "Rows in memtables (WAL-backed, not yet in parts).", memtable_rows);
    VOL_FAMILY("tsdb_merges_total", "counter", "Merges finished since start.", merges);
    VOL_FAMILY("tsdb_merged_rows_total", "counter", "Rows written by merges since start.", merged_rows);
    VOL_FAMILY("tsdb_merging_parts", "gauge", "Parts a running merge reads now.", merging_parts);
    VOL_FAMILY("tsdb_wal_bytes_total", "counter", "Bytes written to the WAL since start.", wal_bytes);
    VOL_FAMILY("tsdb_wal_syncs_total", "counter", "WAL fdatasync calls since start.", wal_syncs);
    VOL_FAMILY("tsdb_broken_parts", "gauge", "Parts found damaged: left out by a follower, or of merges and drops by a writer (index changed since the open, or a merge that could not decode a granule).", broken_parts);
    VOL_FAMILY("tsdb_pinned_index_bytes", "gauge", "Heap bytes of the parts' index copies (--pin-index).", pinned_bytes);
    VOL_FAMILY("tsdb_logical_bytes", "gauge",
               "What the rows in parts would take uncompressed: rows x (8 + width of the series' type). 0 with --no-stats.",
               logical_bytes);
    VOL_FAMILY("tsdb_ts_bytes", "gauge", "Compressed timestamps in parts (from the granule index). 0 with --no-stats.",
               ts_bytes);
    VOL_FAMILY("tsdb_value_bytes", "gauge", "Compressed values in parts (from the granule index). 0 with --no-stats.",
               value_bytes);
    VOL_FAMILY("tsdb_index_bytes", "gauge", "Granule index entries in parts. 0 with --no-stats.", index_bytes);
    VOL_FAMILY("tsdb_other_bytes", "gauge",
               "Part headers, footers, tombstones and the fixed part of each index. 0 with --no-stats.", other_bytes);
    VOL_FAMILY("tsdb_stats_parts_counted", "gauge", "Parts the statistics sweeper has counted.", stats_parts_counted);
    VOL_FAMILY("tsdb_stats_parts_listed", "gauge",
               "Parts listed; the totals above are exact once this equals tsdb_stats_parts_counted.",
               stats_parts_listed);
    VOL_FAMILY("tsdb_stats_ring_overflows_total", "counter",
               "Part events the statistics ring dropped (the sweeper then compared its records with the part list).",
               stats_ring_overflows);
    family(b, "tsdb_stats_cpu_seconds_total", "counter", "CPU time of the statistics sweeper thread.");
    for (uint32_t i = 0; i < nv; i++) {
        sb_add(b, "tsdb_stats_cpu_seconds_total{volume=\"%u\",path=\"", i);
        sb_label(b, tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "");
        sb_add(b, "\"} %.6f\n", (double)vs[i].stats_cpu_ns / 1e9);
    }
    VOL_FAMILY("tsdb_failed", "gauge", "1 once the volume is in its fail-stop state (writes return IO errors).",
               failed);
    if (cfg.vol_diverged) { /* tsdbd's one answer (vol_diverged): the DIVERGED file or the outage reopen's report */
        family(b, "tsdb_volume_diverged", "gauge",
               "1 while the volume is DIVERGED (refused until an operator acts: tsdb DIR volume diverged).");
        for (uint32_t i = 0; i < nv; i++) {
            sb_add(b, "tsdb_volume_diverged{volume=\"%u\",path=\"", i);
            sb_label(b, tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "");
            sb_add(b, "\"} %d\n", cfg.vol_diverged(i) ? 1 : 0);
        }
    }
    /* the latest table (QUERY_LATEST): per volume; all 0 for a volume without a table */
    tsdb_latest_stats *ls = calloc(nv ? nv : 1, sizeof *ls);
    if (ls) {
        for (uint32_t i = 0; i < nv; i++)
            if (tsdb_get_latest_stats(cfg.db, i, &ls[i]) != 0)
                memset(&ls[i], 0, sizeof ls[i]);
#define LVOL_(name, type, help, expr)                                                                                 \
    do {                                                                                                              \
        family(b, name, type, help);                                                                                  \
        for (uint32_t i = 0; i < nv; i++) {                                                                           \
            sb_add(b, name "{volume=\"%u\",path=\"", i);                                                             \
            sb_label(b, tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "");                              \
            sb_add(b, "\"} %llu\n", (unsigned long long)(expr));                                                     \
        }                                                                                                             \
    } while (0)
#define LSER_(path, field)                                                                                            \
    for (uint32_t i = 0; i < nv; i++) {                                                                               \
        sb_add(b, "tsdb_latest_series_total{volume=\"%u\",path=\"", i);                                              \
        sb_label(b, tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "");                                  \
        sb_add(b, "\",how=\"" path "\"} %llu\n", (unsigned long long)ls[i].field);                                 \
    }
        family(b, "tsdb_latest_series_total", "counter",
               "Series QUERY_LATEST (and tsdb_latest) answered, by how: from the latest table or by a search of the parts.");
        LSER_("table", table)
        LSER_("search", searched)
#undef LSER_
        LVOL_("tsdb_latest_floor_reads_total", "counter",
              "Table entries read as unknown below a tsdb_drop_before floor (answered by a search).", ls[i].floor_reads);
        family(b, "tsdb_latest_fills_total", "counter", "The warmer's batches: stored, or refused (a write section ran after its cut).");
        for (uint32_t i = 0; i < nv; i++) {
            const char *pth = tsdb_volume_path(cfg.db, i) ? tsdb_volume_path(cfg.db, i) : "";
            sb_add(b, "tsdb_latest_fills_total{volume=\"%u\",path=\"", i);
            sb_label(b, pth);
            sb_add(b, "\",result=\"stored\"} %llu\n", (unsigned long long)ls[i].fills_stored);
            sb_add(b, "tsdb_latest_fills_total{volume=\"%u\",path=\"", i);
            sb_label(b, pth);
            sb_add(b, "\",result=\"refused\"} %llu\n", (unsigned long long)ls[i].fills_refused);
        }
        LVOL_("tsdb_latest_unknown", "gauge", "Table entries left to fill (the warmer works them off).", ls[i].unknown);
        LVOL_("tsdb_latest_table_bytes", "gauge", "Memory the volume's latest table takes (16 bytes a series, in pages).",
              ls[i].table_bytes);
        LVOL_("tsdb_latest_budget_capped", "gauge",
              "1 when --latest-cache refused the volume a page: the ids beyond are answered by a search.", ls[i].capped);
#undef LVOL_
        free(ls);
    }
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

    tsdb_query_totals qt;
    if (tsdb_query_totals_get(cfg.db, &qt) != 0)
        memset(&qt, 0, sizeof qt);
    family(b, "tsdb_queries_total", "counter",
           "Outermost queries the engine ran (all volumes; QUERY_MULTI once per call). 0 with --no-stats.");
    sb_add(b, "tsdb_queries_total %llu\n", (unsigned long long)qt.queries);
    family(b, "tsdb_query_rows_total", "counter", "Rows queries covered, by where the answer came from.");
    sb_add(b, "tsdb_query_rows_total{source=\"decoded\"} %llu\n", (unsigned long long)qt.rows_decoded);
    sb_add(b, "tsdb_query_rows_total{source=\"stats\"} %llu\n", (unsigned long long)qt.rows_from_stats);
    sb_add(b, "tsdb_query_rows_total{source=\"memtable\"} %llu\n", (unsigned long long)qt.rows_from_memtable);
    family(b, "tsdb_query_bytes_read_total", "counter", "Compressed bytes of the granules queries decoded.");
    sb_add(b, "tsdb_query_bytes_read_total %llu\n", (unsigned long long)qt.bytes_read);
    family(b, "tsdb_query_parts_total", "counter", "Parts queries touched.");
    sb_add(b, "tsdb_query_parts_total %llu\n", (unsigned long long)qt.parts);
    family(b, "tsdb_query_granules_total", "counter", "Granules queries covered, by how they were answered.");
    sb_add(b, "tsdb_query_granules_total{source=\"decoded\"} %llu\n",
           (unsigned long long)(qt.granules - qt.granules_from_stats));
    sb_add(b, "tsdb_query_granules_total{source=\"stats\"} %llu\n", (unsigned long long)qt.granules_from_stats);
    family(b, "tsdb_query_result_rows_total", "counter", "Rows or buckets queries returned.");
    sb_add(b, "tsdb_query_result_rows_total %llu\n", (unsigned long long)qt.result_rows);
    family(b, "tsdb_query_engine_seconds_total", "counter", "Wall time inside the engine's queries, summed.");
    sb_add(b, "tsdb_query_engine_seconds_total %.6f\n", (double)qt.query_ns / 1e9);
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        family(b, "tsdb_major_faults_total", "counter",
               "Major page faults of the server process (reads of mapped parts that went to storage).");
        sb_add(b, "tsdb_major_faults_total %llu\n", (unsigned long long)ru.ru_majflt);
    }

    metrics_totals tot;
    metrics_snapshot(&tot);

    family(b, "tsdb_requests_total", "counter", "Requests handled by the main port, by operation.");
    for (unsigned o = 1; o <= NOPS; o++)
        if (o == OP_OTHER || ops[o].name)
            sb_add(b, "tsdb_requests_total{op=\"%s\"} %llu\n", o == OP_OTHER ? "other" : ops[o].name,
                   (unsigned long long)tot.requests[o]);
    family(b, "tsdb_inserted_rows_total", "counter", "Rows accepted by successful insert requests.");
    sb_add(b, "tsdb_inserted_rows_total %llu\n", (unsigned long long)tot.rows_inserted);
    family(b, "tsdb_multi_series_total", "counter", "Series answered by QUERY_MULTI requests.");
    sb_add(b, "tsdb_multi_series_total %llu\n", (unsigned long long)tot.multi_series);
    family(b, "tsdb_multi_deferred_total", "counter",
           "QUERY_MULTI series too large to hold ahead of their turn, run again at it.");
    sb_add(b, "tsdb_multi_deferred_total %llu\n", (unsigned long long)tot.multi_deferred);
    family(b, "tsdb_request_errors_total", "counter", "Replies with an error status, by TSDB_ERR_* code.");
    for (unsigned k = 1; k < NERR; k++)
        if (err_name[k])
            sb_add(b, "tsdb_request_errors_total{code=\"%s\"} %llu\n", err_name[k], (unsigned long long)tot.errors[k]);
    sb_add(b, "tsdb_request_errors_total{code=\"other\"} %llu\n", (unsigned long long)tot.errors[0]);
    family(b, "tsdb_logins_total", "counter",
           "Login attempts by outcome: ok, failed (wrong password, unknown user or a malformed exchange), "
           "timeout (not logged in within --login-timeout), refused (another op or a frame too large before login).");
    static const char *const login_name[METRICS_LOGIN_KINDS] = {"ok", "failed", "timeout", "refused"};
    for (int k = 0; k < METRICS_LOGIN_KINDS; k++)
        sb_add(b, "tsdb_logins_total{result=\"%s\"} %llu\n", login_name[k], (unsigned long long)tot.logins[k]);

    if (cfg.fwd_peers) { /* forwarding one hop (S2-2): per member this server forwarded requests to */
        metrics_fwd_peer fp[64];
        size_t nf = cfg.fwd_peers(fp, sizeof fp / sizeof fp[0]);
        family(b, "tsdb_fwd_frames_total", "counter", "Requests this server forwarded to the member that leads their volume.");
        for (size_t i = 0; i < nf; i++)
            sb_add(b, "tsdb_fwd_frames_total{peer=\"%u\"} %llu\n", (unsigned)fp[i].peer, (unsigned long long)fp[i].frames);
        family(b, "tsdb_fwd_bytes_total", "counter", "Bytes of the requests this server forwarded, envelope included.");
        for (size_t i = 0; i < nf; i++)
            sb_add(b, "tsdb_fwd_bytes_total{peer=\"%u\"} %llu\n", (unsigned)fp[i].peer, (unsigned long long)fp[i].bytes);
        family(b, "tsdb_fwd_hop_seconds", "summary",
               "Hop latency of forwarded requests, from the send to the reply's last byte (since the start; buckets of a "
               "fourth of a power of two).");
        for (size_t i = 0; i < nf; i++)
            sb_add(b, "tsdb_fwd_hop_seconds{peer=\"%u\",quantile=\"0.5\"} %.6f\ntsdb_fwd_hop_seconds{peer=\"%u\",quantile=\"0.99\"} %.6f\n",
                   (unsigned)fp[i].peer, fp[i].p50_us / 1e6, (unsigned)fp[i].peer, fp[i].p99_us / 1e6);
    }

    if (cfg.chunk_peers) { /* chunk read hedging and read-avoid per storage peer (R8-2, E31) */
        metrics_chunk_peer *cp = malloc(METRICS_CHUNK_PEERS_MAX * sizeof *cp);
        if (cp) {
            size_t nc = cfg.chunk_peers(cp, METRICS_CHUNK_PEERS_MAX);
            family(b, "tsdb_chunk_reads_total", "counter", "Chunk cell read requests sent to this storage peer.");
            for (size_t i = 0; i < nc; i++)
                sb_add(b, "tsdb_chunk_reads_total{peer=\"%u\"} %llu\n", (unsigned)cp[i].peer,
                       (unsigned long long)cp[i].reads);
            family(b, "tsdb_chunk_read_hedges_total", "counter",
                   "Hedged chunk cell reads launched when this storage peer exceeded its hedge timer (E31).");
            for (size_t i = 0; i < nc; i++)
                sb_add(b, "tsdb_chunk_read_hedges_total{peer=\"%u\"} %llu\n", (unsigned)cp[i].peer,
                       (unsigned long long)cp[i].hedges);
            family(b, "tsdb_chunk_read_avoids_total", "counter",
                   "Chunk cell reads decoded around this storage peer without waiting while it was in read-avoid (E31).");
            for (size_t i = 0; i < nc; i++)
                sb_add(b, "tsdb_chunk_read_avoids_total{peer=\"%u\"} %llu\n", (unsigned)cp[i].peer,
                       (unsigned long long)cp[i].avoids);
            family(b, "tsdb_chunk_decoded_cells_total", "counter",
                   "Chunk cells decoded from other stripe positions in place of this storage peer.");
            for (size_t i = 0; i < nc; i++)
                sb_add(b, "tsdb_chunk_decoded_cells_total{peer=\"%u\"} %llu\n", (unsigned)cp[i].peer,
                       (unsigned long long)cp[i].decoded_cells);
            family(b, "tsdb_chunk_read_avoid", "gauge",
                   "1 while this storage peer is in read-avoid (decoded around without waiting), else 0 (E31).");
            for (size_t i = 0; i < nc; i++)
                sb_add(b, "tsdb_chunk_read_avoid{peer=\"%u\"} %d\n", (unsigned)cp[i].peer, cp[i].read_avoid ? 1 : 0);
            free(cp);
        }
    }

    if (cfg.peer_links) { /* the peer links' frame check (--peer-check; a link runs at the stronger end's level) */
        static const char *const kname[METRICS_PL_NKINDS] = {"meta", "log", "bulk", "fwd", "fwdq", "snap", "chunk",
                                                             "chunkb", "chunkr"};
        static const char *const cname[4] = {"", "none", "header", "full"};
        uint64_t up[METRICS_PL_NKINDS][4];
        cfg.peer_links(up);
        family(b, "tsdb_peer_links", "gauge",
               "Peer link connections up now, by link kind and the frame check they run (none, header or full).");
        for (int k = 1; k < METRICS_PL_NKINDS; k++) /* the metadata link (kind 0) keeps its HMAC whatever the level */
            for (int c = 1; c < 4; c++)
                sb_add(b, "tsdb_peer_links{kind=\"%s\",check=\"%s\"} %llu\n", kname[k], cname[c],
                       (unsigned long long)up[k][c]);
    }

    if (cfg.peer_rdma) { /* peer links by transport (tcp vs rdma) and RDMA fault counters (design-rdma.md §5.2) */
        static const char *const kname[METRICS_PL_NKINDS] = {"meta", "log", "bulk", "fwd", "fwdq", "snap", "chunk",
                                                             "chunkb", "chunkr"};
        static const char *const fbname[METRICS_RDMA_FB_MAX] = {
            "not_built", "off", "kind", "subnet", "pool", "qp", "timeout", "auth", "qp_error", "backoff", "bad",
        };
        metrics_peer_rdma rd;
        memset(&rd, 0, sizeof rd);
        cfg.peer_rdma(&rd);
        family(b, "tsdb_peer_links_transport", "gauge",
               "Peer link connections up now, by link kind and transport (tcp or rdma).");
        for (int k = 1; k < METRICS_PL_NKINDS; k++) {
            sb_add(b, "tsdb_peer_links_transport{kind=\"%s\",transport=\"tcp\"} %llu\n", kname[k],
                   (unsigned long long)rd.up[k][0]);
            sb_add(b, "tsdb_peer_links_transport{kind=\"%s\",transport=\"rdma\"} %llu\n", kname[k],
                   (unsigned long long)rd.up[k][1]);
        }
        family(b, "tsdb_peer_rdma_fallbacks_total", "counter",
               "RDMA peer link upgrade fallbacks and declines, by reason.");
        for (int r = 0; r < METRICS_RDMA_FB_MAX; r++)
            sb_add(b, "tsdb_peer_rdma_fallbacks_total{reason=\"%s\"} %llu\n", fbname[r],
                   (unsigned long long)rd.fallbacks[r]);
        family(b, "tsdb_peer_rdma_credit_stalls_total", "counter",
               "Times an RDMA peer link write waited for a peer data credit.");
        sb_add(b, "tsdb_peer_rdma_credit_stalls_total %llu\n", (unsigned long long)rd.credit_stalls);
        family(b, "tsdb_peer_rdma_rnr_retries_total", "counter",
               "RDMA peer link completions failed with receiver-not-ready retry exceeded.");
        sb_add(b, "tsdb_peer_rdma_rnr_retries_total %llu\n", (unsigned long long)rd.rnr_retries);
        family(b, "tsdb_peer_rdma_qp_errors_total", "counter",
               "RDMA peer link queue pair completion or state-transition errors.");
        sb_add(b, "tsdb_peer_rdma_qp_errors_total %llu\n", (unsigned long long)rd.qp_errors);
    }

    if (cfg.margins) { /* the cluster volumes' margins (server/margins.h; design-cluster-data.md E15, E24) */
        static const char *const tier_names[] = {"none", "ok", "unavailable", "corrupt", "lost"};
        _Static_assert(sizeof tier_names / sizeof *tier_names == MARGIN_TIER_LOST + 1, "a name per enum margin_tier");
        metrics_margin *mg = NULL;
        size_t nm = cfg.margins(&mg); /* every cluster volume of the copy: a monitoring gap must not be silent */
        if (nm == (size_t)-1) {
            family(b, "tsdb_volume_margins_missing", "gauge",
                   "1 when this scrape could not list the cluster volumes' margins (out of memory).");
            sb_add(b, "tsdb_volume_margins_missing 1\n");
            nm = 0;
        }
        family(b, "tsdb_volume_young_margin", "gauge",
               "Losses an acknowledged row of the cluster volume survives now: its in-sync replicas minus one.");
        for (size_t i = 0; i < nm; i++)
            if (!mg[i].young_na) /* a shared volume's candidates hold no copies */
                sb_add(b, "tsdb_volume_young_margin{volume=\"%u\"} %d\n", (unsigned)mg[i].vol, mg[i].young);
        family(b, "tsdb_volume_settled_margin", "gauge",
               "Losses the settled objects of an erasure-coded cluster volume survive now: the positions of its stripe "
               "group that are present minus k (negative: settled reads short of positions).");
        for (size_t i = 0; i < nm; i++)
            if (mg[i].tier != MARGIN_TIER_NONE)
                sb_add(b, "tsdb_volume_settled_margin{volume=\"%u\"} %d\n", (unsigned)mg[i].vol, mg[i].settled);
        family(b, "tsdb_volume_settled_state", "gauge",
               "1 for the state of an erasure-coded cluster volume's settled tier: ok, unavailable (fewer than k "
               "positions present), corrupt (a read answered CORRUPT here) or lost (more than m positions destroyed).");
        for (size_t i = 0; i < nm; i++)
            for (int t = MARGIN_TIER_OK; mg[i].tier != MARGIN_TIER_NONE && t <= MARGIN_TIER_LOST; t++)
                sb_add(b, "tsdb_volume_settled_state{volume=\"%u\",state=\"%s\"} %d\n", (unsigned)mg[i].vol,
                       tier_names[t], mg[i].tier == t);
        free(mg);
    }

    if (cfg.snap) { /* snapshot transfers (R4, server/rsnap.c) */
        metrics_snap sn;
        cfg.snap(&sn);
        family(b, "tsdb_snapshot_total", "counter",
               "Snapshot transfers of replicated volumes by outcome: asked (as a receiver), sent, received, refused_space "
               "(the receiver's free space), failed (abandoned or released before the end), busy (BUSY answers), installed, "
               "refused_stale (a copy not installed: its P at or below the receiver's log end, or its epoch below the "
               "receiver's fence).");
        sb_add(b, "tsdb_snapshot_total{event=\"asked\"} %llu\ntsdb_snapshot_total{event=\"sent\"} %llu\n"
                  "tsdb_snapshot_total{event=\"received\"} %llu\ntsdb_snapshot_total{event=\"refused_space\"} %llu\n"
                  "tsdb_snapshot_total{event=\"failed\"} %llu\ntsdb_snapshot_total{event=\"busy\"} %llu\n"
                  "tsdb_snapshot_total{event=\"installed\"} %llu\ntsdb_snapshot_total{event=\"refused_stale\"} %llu\n",
               (unsigned long long)sn.asked, (unsigned long long)sn.sent, (unsigned long long)sn.received,
               (unsigned long long)sn.refused_space, (unsigned long long)sn.failed, (unsigned long long)sn.busy,
               (unsigned long long)sn.installed, (unsigned long long)sn.refused_stale);
        family(b, "tsdb_snapshot_bytes_total", "counter", "Bytes of snapshot files this server sent and received.");
        sb_add(b, "tsdb_snapshot_bytes_total{dir=\"sent\"} %llu\ntsdb_snapshot_bytes_total{dir=\"received\"} %llu\n",
               (unsigned long long)sn.bytes_sent, (unsigned long long)sn.bytes_received);
        family(b, "tsdb_snapshot_last_seconds", "gauge",
               "The last snapshot this server received: seconds from its request to its last file.");
        sb_add(b, "tsdb_snapshot_last_seconds %.3f\n", sn.last_seconds);
        family(b, "tsdb_snapshot_install_last_seconds", "gauge",
               "The last snapshot this server installed: seconds from the end of its transfer to the volume handed back.");
        sb_add(b, "tsdb_snapshot_install_last_seconds %.3f\n", sn.install_seconds);
    }

    if (cfg.pace) { /* rebuild and balancer pacing (R9 D18, E13, §8.4; server/pace.h) */
        static const char *const pdir[2] = {"send", "recv"};
        static const char *const pcls[METRICS_PACE_CLASSES] = {"margin0", "margin1", "margin2plus", "balance"};
        metrics_pace pc;
        memset(&pc, 0, sizeof pc);
        cfg.pace(&pc);
        family(b, "tsdb_pace_rate_bytes_per_second", "gauge",
               "Current effective rebuild/balance pace rate in bytes/s by direction (0 when unlimited at margin 0).");
        for (int d = 0; d < 2; d++)
            sb_add(b, "tsdb_pace_rate_bytes_per_second{dir=\"%s\"} %llu\n", pdir[d],
                   (unsigned long long)pc.rate_bytes_s[d]);
        family(b, "tsdb_pace_halved", "gauge",
               "1 while rebuild/balance pacing is halved for elevated foreground query or WAL sync p99 latency, else 0.");
        sb_add(b, "tsdb_pace_halved %d\n", pc.halved ? 1 : 0);
        family(b, "tsdb_pace_bytes_total", "counter",
               "Bytes paced by direction and margin class (margin0, margin1, margin2plus, balance).");
        for (int d = 0; d < 2; d++)
            for (int c = 0; c < METRICS_PACE_CLASSES; c++)
                sb_add(b, "tsdb_pace_bytes_total{dir=\"%s\",class=\"%s\"} %llu\n", pdir[d], pcls[c],
                       (unsigned long long)pc.bytes[d][c]);
    }

    if (cs) { /* R14 cluster metrics registry (server/creport.h; lock-free render) */
        for (size_t i = 0; i < cs->nfamilies; i++) {
            const struct creport_snap_family *f = &cs->families[i];
            if (strcmp(f->name, "tsdb_disk_bytes") == 0)
                continue; /* emitted contiguously under the single tsdb_disk_bytes family above */
            family(b, f->name, f->type == CREPORT_COUNTER ? "counter" : "gauge", f->help);
            render_creport_samples(b, f);
        }
        cfg.cluster_snap_free(cs);
    }

    if (cfg.tier) { /* single-server tier metrics (R15 r15-full, E53, §15.7); omitted when no tier is configured */
        metrics_tier *tm = calloc(1, sizeof *tm);
        if (tm && cfg.tier(tm)) {
            family(b, "tsdb_tier_bytes", "gauge",
                   "Filesystem bytes of each open local tier store, by use (used, free, total).");
            for (int i = 0; i < tm->nstores; i++) {
                if (!tm->stores[i].has_bytes)
                    continue;
                int idx = tm->stores[i].index;
                sb_add(b, "tsdb_tier_bytes{store=\"%d\",use=\"used\"} %llu\n", idx,
                       (unsigned long long)tm->stores[i].used_bytes);
                sb_add(b, "tsdb_tier_bytes{store=\"%d\",use=\"free\"} %llu\n", idx,
                       (unsigned long long)tm->stores[i].free_bytes);
                sb_add(b, "tsdb_tier_bytes{store=\"%d\",use=\"total\"} %llu\n", idx,
                       (unsigned long long)tm->stores[i].total_bytes);
            }
            family(b, "tsdb_tier_settles_waiting", "gauge",
                   "Tiered volumes currently waiting to settle because a store is down, full (>95%), or the tier is halted.");
            sb_add(b, "tsdb_tier_settles_waiting %llu\n", (unsigned long long)tm->settles_waiting);
            family(b, "tsdb_tier_store_state", "gauge",
                   "Local tier store state (0=up, 1=missing, 2=other_id, 3=damaged, 4=ioerr, 5=replacing, 6=stalled).");
            for (int i = 0; i < tm->nstores; i++)
                sb_add(b, "tsdb_tier_store_state{store=\"%d\"} %d\n", tm->stores[i].index, tm->stores[i].state);
            family(b, "tsdb_volume_full", "gauge",
                   "1 while the tiered volume is FULL (>= young_high_pct, refusing inserts until < young_high_pct - 2), else 0.");
            for (uint32_t i = 0; i < tm->nvols; i++)
                sb_add(b, "tsdb_volume_full{volume=\"%u\"} %d\n", (unsigned)i, tm->vol_full[i] ? 1 : 0);
            family(b, "tsdb_tier_salvaged_parts_total", "counter",
                   "Lost erasure-coded parts retired by local tier salvage.");
            sb_add(b, "tsdb_tier_salvaged_parts_total %llu\n", (unsigned long long)tm->salvaged_parts);
            family(b, "tsdb_tier_salvaged_rows_total", "counter",
                   "Rows lost in erasure-coded parts retired by local tier salvage (from the local index).");
            sb_add(b, "tsdb_tier_salvaged_rows_total %llu\n", (unsigned long long)tm->salvaged_rows);
        }
        if (tm)
            free(tm->vol_full);
        free(tm);
    }

    family(b, "tsdb_query_duration_seconds", "histogram",
           "Time a worker spent on a query request, from taking it to handing the reply to the kernel (a slow client can add to it).");
    for (unsigned o = 1; o < NOPS; o++) {
        int q = query_op_idx(o);
        if (q < 0)
            continue;
        uint64_t cum = 0;
        for (int k = 0; k < NBUCKETS; k++) {
            cum += tot.hist[q][k];
            sb_add(b, "tsdb_query_duration_seconds_bucket{op=\"%s\",le=\"%s\"} %llu\n", ops[o].name, bucket_str[k],
                   (unsigned long long)cum);
        }
        cum += tot.hist[q][NBUCKETS];
        sb_add(b, "tsdb_query_duration_seconds_bucket{op=\"%s\",le=\"+Inf\"} %llu\n", ops[o].name,
               (unsigned long long)cum);
        sb_add(b, "tsdb_query_duration_seconds_sum{op=\"%s\"} %.9g\n", ops[o].name,
               (double)tot.hist_sum_ns[q] / 1e9);
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
    int64_t busy = tot.workers_busy;
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
    metrics_p99_win p99w;
    metrics_p99_win_init(&p99w);
    int64_t next_p99_ms = mono_ms_now() + 1000;
    while (!atomic_load(&stop_flag)) {
        int64_t now = mono_ms_now(), wake = now + 200; /* also the latency of stopping */
        if (cfg.on_p99_halved && now >= next_p99_ms) {
            next_p99_ms = now + 1000;
            uint64_t qcum[METRICS_HIST_BUCKETS], wcum[METRICS_HIST_BUCKETS];
            metrics_p99_snapshot(qcum, wcum);
            cfg.on_p99_halved(metrics_p99_win_step(&p99w, qcum, wcum));
        }
        if (cfg.listen_fd < 0) {
            /* Headless mode (cluster server without --metrics-port): only step the 1-s p99 halving window. */
            (void)poll(NULL, 0, (int)(wake > now ? wake - now : 0));
            continue;
        }
        pf[0] = (struct pollfd){.fd = cfg.listen_fd, .events = POLLIN};
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
    if (cfg.listen_fd >= 0) {
        int fl = fcntl(cfg.listen_fd, F_GETFL);
        if (fl < 0 || fcntl(cfg.listen_fd, F_SETFL, fl | O_NONBLOCK) != 0) /* accept_some loops until EAGAIN */
            return -errno;
    }
    atomic_store(&stop_flag, false);
    int rc = tsdb_thread_create(&thread, metrics_main, NULL);
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
    if (cfg.listen_fd >= 0)
        close(cfg.listen_fd);
    started = false;
}
