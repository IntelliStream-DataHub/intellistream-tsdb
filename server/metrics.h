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

/* peer link RDMA transport and fault counters (design-rdma.md §5.2; peerlink.h + plrdma.h) */
#define METRICS_RDMA_FB_MAX 11
typedef struct metrics_peer_rdma {
    uint64_t up[8][2];                    /* [kind][0=tcp, 1=rdma] */
    uint64_t fallbacks[METRICS_RDMA_FB_MAX]; /* indexed by PL_RDMA_FB_* */
    uint64_t credit_stalls;
    uint64_t rnr_retries;
    uint64_t qp_errors;
} metrics_peer_rdma;

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
    void (*peer_links)(uint64_t out[8][4]);
    /* the peer data links by transport and RDMA counters; NULL: not a cluster server */
    void (*peer_rdma)(metrics_peer_rdma *out);
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
/* A worker starts / finishes a request; begin returns the start time to pass to end. */
int64_t metrics_request_begin(void);
void metrics_request_end(uint8_t op, int64_t began);
/* The histogram bucket for a duration: 0..14 for the `le` bounds 0.5 ms .. 30 s, 15 for +Inf. */
int metrics_bucket_for(double seconds);
void metrics_error(int32_t status); /* a reply with a non-zero status */
void metrics_rows_inserted(uint64_t n);
void metrics_multi_series(uint64_t n); /* series a QUERY_MULTI answered */
void metrics_multi_deferred(void);      /* a QUERY_MULTI series rerun at its turn */
/* A login's outcome (no user names: the metrics port is unauthenticated, A21) */
enum { METRICS_LOGIN_OK, METRICS_LOGIN_FAILED, METRICS_LOGIN_TIMEOUT, METRICS_LOGIN_REFUSED, METRICS_LOGIN_KINDS };
void metrics_login(int what);

#endif
