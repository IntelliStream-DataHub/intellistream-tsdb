/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Prometheus metrics for tsdbd (--metrics-port), in server/metrics.c.
 *
 * tsdbd.c calls the small hooks below, which only bump atomic counters; the
 * HTTP side lives on a thread of its own and never takes a lock the request
 * path holds for long. See metrics.c for the design.
 */
#ifndef TSDBD_METRICS_H
#define TSDBD_METRICS_H

#include "tsdb.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* one peer this server forwarded requests to (S2-2) */
typedef struct metrics_fwd_peer {
    uint32_t peer;
    uint64_t frames, bytes;
    double p50_us, p99_us; /* hop latency: send to the reply's last byte */
} metrics_fwd_peer;

/* snapshot transfers of a cluster server (server/rsnap.c; repld.h struct rsnap_stats) */
typedef struct metrics_snap {
    uint64_t asked, sent, received, refused_space, failed, busy, bytes_sent, bytes_received;
    double last_seconds; /* the last snapshot received, from its request to its last file */
    uint64_t installed, refused_stale; /* installs done; copies not installed as stale (server/snapinst_run.c) */
    double install_seconds; /* the last install, from the end of its transfer to the volume handed back */
} metrics_snap;

/* the margins of one cluster volume (server/margins.h) */
typedef struct metrics_margin {
    uint32_t vol;
    int young, settled; /* settled: tier MARGIN_TIER_NONE has none */
    bool young_na;      /* a shared volume: no young margin */
    uint8_t tier;       /* enum margin_tier */
} metrics_margin;

/* chunk read hedging and read-avoid counters per storage peer (R8-2, E31; server/chunkpeer.h) */
#define METRICS_CHUNK_PEERS_MAX 256
typedef struct metrics_chunk_peer {
    uint32_t peer;
    uint64_t reads, hedges, avoids, decoded_cells;
    int read_avoid;
} metrics_chunk_peer;

/* peer link RDMA transport and fault counters (design-rdma.md §5.2; peerlink.h + plrdma.h) */
#define METRICS_PL_NKINDS 9
#define METRICS_RDMA_FB_MAX 11
typedef struct metrics_peer_rdma {
    uint64_t up[METRICS_PL_NKINDS][2];       /* [kind][0=tcp, 1=rdma] */
    uint64_t fallbacks[METRICS_RDMA_FB_MAX]; /* indexed by PL_RDMA_FB_* */
    uint64_t credit_stalls;
    uint64_t rnr_retries;
    uint64_t qp_errors;
} metrics_peer_rdma;

/* rebuild and balancer pacing state and per-class counters (R9 D18, E13, §8.4; server/pace.h) */
#define METRICS_PACE_CLASSES 4 /* margin0, margin1, margin2plus, balance */
typedef struct metrics_pace {
    uint64_t rate_bytes_s[2];                /* [0=send, 1=recv]; 0 = unlimited (margin 0) */
    uint64_t bytes[2][METRICS_PACE_CLASSES]; /* bytes paced by direction and margin class */
    int halved;                              /* 1 while foreground p99 halving is active, else 0 */
} metrics_pace;

/* 1-minute p99 vs 1-hour median sliding-window tracker (§8.4, r9-pace), stepped once per second. */
#define METRICS_HIST_BUCKETS 16 /* 15 finite le bounds (0.5 ms .. 30 s) + +Inf */
typedef struct metrics_p99_series {
    uint64_t sec_ring[61][METRICS_HIST_BUCKETS];
    int sec_head, sec_count;
    double min_p99[60];
    int min_head, min_count;
    int sec_in_min;
    double last_p99_1m;
    double last_median_1h;
} metrics_p99_series;

typedef struct metrics_p99_win {
    metrics_p99_series query;
    metrics_p99_series wal_sync;
    int halved;
} metrics_p99_win;

/* single-server tier metrics (R15 r15-full, E53, §15.7; omitted when no tier is configured) */
#define METRICS_TIER_STORES_MAX 64
typedef struct metrics_tier_store {
    int index;
    int state; /* CHUNK_STORE_* (0=up, 1=missing, 2=other_id, 3=damaged, 4=ioerr, 5=replacing, 6=stalled) */
    bool has_bytes;
    uint64_t used_bytes, free_bytes, total_bytes;
    /* R15 r15-scrub per-store counters and oldest pass age */
    bool has_scrub;
    uint64_t scrub_chunks_checked;
    uint64_t scrub_damaged;
    uint64_t scrub_repaired;
    uint64_t scrub_unrepairable;
    uint64_t scrub_age_s;
} metrics_tier_store;
typedef struct metrics_tier {
    int nstores;
    metrics_tier_store stores[METRICS_TIER_STORES_MAX];
    uint64_t settles_waiting;
    uint64_t salvaged_parts;
    uint64_t salvaged_rows;
    uint64_t scrub_oldest_age_s;
    bool has_scrub;
    uint32_t nvols;
    uint8_t *vol_full; /* malloc'ed by cfg.tier callback (length nvols), freed by metrics.c */
} metrics_tier;

struct creport_snap;

typedef struct metrics_cfg {
    tsdb *db;
    int listen_fd;           /* bound and listening; metrics_start takes it over */
    const char *io;          /* "io_uring" or "epoll" */
    const char *role;        /* "writer", "read-only" or "follower" */
    size_t max_conns;        /* --max-connections */
    size_t (*workers)(void); /* worker threads running now (base plus extra) */
    size_t (*request_memory)(void); /* bytes counted against --max-request-memory now */
    size_t request_memory_limit;    /* --max-request-memory in bytes */
    size_t (*fwd_peers)(metrics_fwd_peer *out, size_t cap); /* NULL: not a cluster server */
    void (*snap)(metrics_snap *out); /* NULL: not a cluster server */
    int (*vol_diverged)(uint32_t v); /* 1 while volume v is DIVERGED (tsdbd's vol_diverged); NULL: no family */
    /* the cluster volumes' margins, *out malloc'ed (freed by the caller), (size_t)-1 when it could not be allocated;
     * NULL: not a cluster server */
    size_t (*margins)(metrics_margin **out);
    /* the peer data links up by kind and frame check type (peerlink_up_by_check); NULL: not a cluster server */
    void (*peer_links)(uint64_t out[METRICS_PL_NKINDS][4]);
    /* the peer data links by transport and RDMA counters; NULL: not a cluster server */
    void (*peer_rdma)(metrics_peer_rdma *out);
    /* chunk read hedging, read-avoid and decoded cell counters per storage peer (E31); NULL: not a cluster server */
    size_t (*chunk_peers)(metrics_chunk_peer *out, size_t cap);
    /* rebuild and balancer pacing counters (R9 D18); NULL: not a cluster server */
    void (*pace)(metrics_pace *out);
    /* foreground p99 halving callback (§8.4), invoked once per second on the metrics thread; NULL: disabled */
    void (*on_p99_halved)(int halved);
    /* cluster metrics registry snapshot (R14, server/creport.h); NULL: standalone (output unchanged) */
    struct creport_snap *(*cluster_snap)(void);
    void (*cluster_snap_free)(struct creport_snap *snap);
    /* single-server tier metrics (R15 r15-full); NULL when no tier is configured (standalone output unchanged) */
    bool (*tier)(metrics_tier *out);
} metrics_cfg;

/* Starts the metrics thread. Returns 0, or a negative errno. */
int metrics_start(const metrics_cfg *cfg);
/* Ends the thread (call before tsdb_close). Closes the listening socket. */
void metrics_stop(void);

/* Turns the hooks on; call before the first connection. Without it (no --metrics-port) every hook
 * returns at once, so a server that does not export metrics pays one predictable branch per call. */
void metrics_enable(void);

/* ---- hooks for the server; each is a few relaxed atomic operations ---- */
void metrics_conn_opened(void);
void metrics_conn_closed(void);
void metrics_conn_rejected(void);
/* A worker starts / finishes a request; begin returns the start time for query ops (0 otherwise) to pass to end.
 * Both calls must be paired on the same thread with the same op (workers_busy is tracked per thread and only query
 * ops carry a monotonic timestamp in began). */
int64_t metrics_request_begin(uint8_t op);
void metrics_request_end(uint8_t op, int64_t began);
/* The histogram bucket for a duration: 0..14 for the `le` bounds 0.5 ms .. 30 s, 15 for +Inf. */
int metrics_bucket_for(double seconds);
/* Record a WAL sync duration (ns) into the per-thread shard's WAL sync histogram (relaxed atomic). */
void metrics_wal_sync(int64_t ns);
/* Compute interpolated p99 (seconds) from a delta histogram of METRICS_HIST_BUCKETS buckets (0.0 if empty). */
double metrics_hist_p99(const uint64_t delta[METRICS_HIST_BUCKETS]);
/* Initialize / advance the 1-minute p99 vs 1-hour median halving tracker (returns 1 if halved, else 0). */
void metrics_p99_win_init(metrics_p99_win *w);
int metrics_p99_win_step(metrics_p99_win *w, const uint64_t query_cum[METRICS_HIST_BUCKETS],
                         const uint64_t wal_cum[METRICS_HIST_BUCKETS]);
/* Snapshot current cumulative query and WAL sync histograms across all thread shards. */
void metrics_p99_snapshot(uint64_t query_cum[METRICS_HIST_BUCKETS], uint64_t wal_cum[METRICS_HIST_BUCKETS]);
void metrics_error(int32_t status); /* a reply with a non-zero status */
void metrics_rows_inserted(uint64_t n);
void metrics_multi_series(uint64_t n); /* series a QUERY_MULTI answered */
void metrics_multi_deferred(void);      /* a QUERY_MULTI series rerun at its turn */
/* A login's outcome (no user names: the metrics port is unauthenticated, A21) */
enum { METRICS_LOGIN_OK, METRICS_LOGIN_FAILED, METRICS_LOGIN_TIMEOUT, METRICS_LOGIN_REFUSED, METRICS_LOGIN_KINDS };
void metrics_login(int what);

#endif
