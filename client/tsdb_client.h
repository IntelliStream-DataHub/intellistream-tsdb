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
void tsdbc_close(tsdbc *c);
const char *tsdbc_strerror(int err); /* the client does not link the storage engine */

int tsdbc_series(tsdbc *c, const char *name, int create, uint32_t *id);
/* Creates the series in the volume `path` on the server (see tsdb_series_at). */
int tsdbc_series_at(tsdbc *c, const char *name, const char *path, uint32_t *id);
int tsdbc_series_name(tsdbc *c, uint32_t id, char *buf, size_t len);

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
int tsdbc_query_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);

int tsdbc_commit(tsdbc *c);
int tsdbc_flush(tsdbc *c);
int tsdbc_optimize(tsdbc *c);
int tsdbc_stats(tsdbc *c, tsdb_stats *st);

#ifdef __cplusplus
}
#endif

#endif
