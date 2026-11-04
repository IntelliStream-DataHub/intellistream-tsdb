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

#include <stddef.h>
#include <stdint.h>

typedef struct metrics_cfg {
    tsdb *db;
    int listen_fd;           /* bound and listening; metrics_start takes it over */
    const char *io;          /* "io_uring" or "epoll" */
    const char *role;        /* "writer", "read-only" or "follower" */
    size_t max_conns;        /* --max-connections */
    size_t (*workers)(void); /* worker threads running now (base plus extra) */
    size_t (*request_memory)(void); /* bytes counted against --max-request-memory now */
    size_t request_memory_limit;    /* --max-request-memory in bytes */
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
