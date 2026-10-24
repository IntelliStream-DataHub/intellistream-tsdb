/*
 * Client for tsdbd. Mirrors the embedded API in tsdb.h; one tsdbc is one TCP
 * connection and must be used by one thread at a time (open one per thread).
 *
 *   tsdbc *c;
 *   tsdbc_connect("127.0.0.1", 7070, &c);
 *   uint32_t id;
 *   tsdbc_series(c, "pump7.flow", 1, &id);
 *   tsdbc_insert_series(c, id, ts, values, n);
 *   tsdbc_query_agg(c, id, start, end, 60000, &buckets, &nb);
 *   tsdbc_close(c);
 *
 * Inserts can be pipelined: tsdbc_insert_async sends without waiting for the
 * acknowledgement, tsdbc_wait collects them (and returns the first error).
 * The server applies requests of one connection in order.
 */
#ifndef TSDB_CLIENT_H
#define TSDB_CLIENT_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdbc tsdbc;

int tsdbc_connect(const char *host, int port, tsdbc **out);
void tsdbc_close(tsdbc *c); /* waits for pipelined inserts first; use tsdbc_wait to see their errors */
const char *tsdbc_strerror(int err); /* the client does not link the storage engine */

int tsdbc_series(tsdbc *c, const char *name, int create, uint32_t *id);
/* Creates the series in the volume `path` on the server (see tsdb_series_at). */
int tsdbc_series_at(tsdbc *c, const char *name, const char *path, uint32_t *id);
int tsdbc_series_name(tsdbc *c, uint32_t id, char *buf, size_t len);

/* Inserts travel packed and zstd compressed (level 1 by default) when the
 * server speaks protocol version 3; 0 sends plain arrays, which costs 16 bytes
 * a row on the wire instead of a few, but no CPU. Negative levels are faster. */
int tsdbc_set_compression(tsdbc *c, int level);
/* Bytes written to the server so far, frame headers included. */
uint64_t tsdbc_bytes_sent(const tsdbc *c);
/* Bounds every wait for the server (send or receive) to ms milliseconds; 0, the
 * default, waits forever. A call that times out returns TSDB_ERR_IO and the
 * connection is of no further use: close it. */
int tsdbc_set_timeout(tsdbc *c, int ms);

int tsdbc_insert(tsdbc *c, const uint32_t *series, const int64_t *ts, const float *values, size_t n);
int tsdbc_insert_series(tsdbc *c, uint32_t series, const int64_t *ts, const float *values, size_t n);
int tsdbc_insert_async(tsdbc *c, const uint32_t *series, const int64_t *ts, const float *values, size_t n);
int tsdbc_insert_series_async(tsdbc *c, uint32_t series, const int64_t *ts, const float *values, size_t n);
int tsdbc_wait(tsdbc *c);

int tsdbc_query_raw(tsdbc *c, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx);
int tsdbc_query_points(tsdbc *c, uint32_t series, int64_t start, int64_t end, tsdb_points *out);
int tsdbc_query_agg(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                    size_t *nout);
/* Ordering, limits and calendar grouping, as tsdb_query_raw_ex / tsdb_query_group_ex. */
int tsdbc_query_raw_ex(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_raw_fn fn, void *ctx);
int tsdbc_query_points_ex(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                          tsdb_points *out);
/* With a time zone in `group`, the client computes the UTC bounds of the
 * local periods (tsdb_calendar.h) and asks for those: the server knows UTC only. */
int tsdbc_query_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);
/* Buckets beginning at given UTC instants, as tsdb_query_bounds. */
int tsdbc_query_bounds(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                       const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);
/* The spread (range, variances, standard deviations), as tsdb_query_spread,
 * _spread_group and _spread_bounds (tsdb.h); a group in a time zone other than
 * UTC is turned into bounds here. Needs a server of protocol version 8
 * (TSDB_ERR_INVALID from an older one, without asking it). */
int tsdbc_query_spread(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
int tsdbc_query_spread_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                             const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
int tsdbc_query_spread_bounds(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds,
                              size_t n, const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
/* Time-weighted averages, as tsdb_query_twa, _twa_group and _twa_bounds (tsdb.h);
 * a group in a time zone other than UTC is turned into bounds here. Needs a
 * server of protocol version 5 (TSDB_ERR_INVALID from an older one). */
int tsdbc_query_twa(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                    const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
int tsdbc_query_twa_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                          const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
int tsdbc_query_twa_bounds(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                           const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);

/* Value types and nulls (protocol version 2), as tsdb_series_create,
 * tsdb_insert_values, tsdb_query_values and the state dictionary. */
int tsdbc_series_create(tsdbc *c, const char *name, tsdb_type type, const char *path, uint32_t *id);
int tsdbc_series_type(tsdbc *c, uint32_t id, tsdb_type *type);
int tsdbc_insert_values(tsdbc *c, const uint32_t *series, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdbc_insert_series_values(tsdbc *c, uint32_t series, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdbc_insert_values_async(tsdbc *c, const uint32_t *series, const int64_t *ts, const tsdb_values *values,
                              size_t n);
int tsdbc_insert_series_values_async(tsdbc *c, uint32_t series, const int64_t *ts, const tsdb_values *values,
                                     size_t n);
int tsdbc_query_values(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_values_fn fn, void *ctx);
/* The server's protocol version from HELLO (asked once per connection; 1 for a
 * server that predates HELLO), after waiting for pipelined inserts; 0 if the
 * connection failed. */
uint32_t tsdbc_server_version(tsdbc *c);
/* LTTB downsampling, as tsdb_query_lttb (protocol version 5: against an older
 * server it returns TSDB_ERR_INVALID without asking, as such a server would
 * for the unknown op; the same code as a bad threshold, so a caller that must
 * tell them apart checks tsdbc_server_version first). */
int tsdbc_query_lttb(tsdbc *c, uint32_t series, int64_t start, int64_t end, size_t threshold, tsdb_values_fn fn,
                     void *ctx);
int tsdbc_state(tsdbc *c, uint32_t series, const char *name, uint32_t *code);
int tsdbc_state_lookup(tsdbc *c, uint32_t series, const char *name, uint32_t *code);
int tsdbc_state_name(tsdbc *c, uint32_t series, uint32_t code, char *buf, size_t len);

/* What the server did for this connection's previous query (tsdb_last_query_stats there). */
int tsdbc_last_query_stats(tsdbc *c, tsdb_query_stats *st);

/* Deletes, replaces and retention, as tsdb_delete_range, tsdb_replace_range, tsdb_set_retention and
 * tsdb_get_retention (protocol version 7; with an older server: TSDB_ERR_INVALID). A replace travels as
 * one frame, so n is limited by the frame size (about 256 MiB). */
int tsdbc_delete_range(tsdbc *c, uint32_t series, int64_t start, int64_t end);
int tsdbc_replace_range(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                        const tsdb_values *values, size_t n);
int tsdbc_set_retention(tsdbc *c, uint32_t series, int64_t limit_ms);
int tsdbc_get_retention(tsdbc *c, uint32_t series, int64_t *limit_ms);

int tsdbc_commit(tsdbc *c);
int tsdbc_flush(tsdbc *c);
int tsdbc_optimize(tsdbc *c);
int tsdbc_stats(tsdbc *c, tsdb_stats *st);

#ifdef __cplusplus
}
#endif

#endif
