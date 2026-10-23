/*
 * tsdb wire protocol, spoken by tsdbd and the client library.
 *
 * Everything is little endian. Every message is a frame:
 *
 *   u32 length    bytes that follow this field
 *   u32 id        chosen by the client, echoed in every response frame
 *   u8  op        request: TSDB_OP_*; response: TSDB_OP_REPLY
 *   ...           payload
 *
 * A response payload starts with an i32 status (0 or a TSDB_ERR_* code).
 * Requests on one connection are executed in order, one at a time, so a
 * client may pipeline them and still rely on "last write wins". Arrays are
 * columnar (all timestamps, then all values).
 *
 * Requests and their successful responses:
 *
 *   HELLO       u32 version                    -> u32 version
 *   SERIES      u8 create, u16 len, name [, u16 plen, path]  -> u32 id
 *               (path: the volume to create the series in; see tsdb_series_at)
 *   SERIES_NAME u32 id                         -> u16 len, name
 *   INSERT      u32 n, u32 series, i64 ts[n], f32 v[n]           -> (nothing)
 *               or, with a series per row (series == TSDB_MANY_SERIES):
 *               u32 n, u32 0xffffffff, i64 ts[n], f32 v[n], u32 series[n]
 *               (every array stays naturally aligned in the payload)
 *   QUERY_RAW   u32 series, i64 start, i64 end [, u8 order, u64 limit]
 *               -> chunks, each a REPLY frame: u8 last, u32 n, i64 ts[n], f32 v[n]
 *               (order 1 = newest first; limit 0 = all)
 *   QUERY_AGG   u32 series, i64 start, i64 end, i64 interval
 *               -> u32 n, n x (i64 start, u64 count, f64 sum, f32 min, f32 max)
 *   QUERY_GROUP u32 series, i64 start, i64 end, u8 unit (0: one bucket, else tsdb_unit),
 *               u32 count, u8 order, u64 limit, u16 len, time zone
 *               -> as QUERY_AGG
 *   COMMIT, FLUSH, OPTIMIZE                    -> (nothing)
 *   STATS                                      -> tsdb_stats as 8 x u64
 */
#ifndef TSDB_PROTOCOL_H
#define TSDB_PROTOCOL_H

#define TSDB_PROTOCOL_VERSION 1
#define TSDB_DEFAULT_PORT 7070
#define TSDB_MAX_FRAME (256u << 20)
#define TSDB_FRAME_HEADER 9 /* length + id + op */
#define TSDB_MANY_SERIES 0xffffffffu

enum {
    TSDB_OP_REPLY = 0,
    TSDB_OP_HELLO = 1,
    TSDB_OP_SERIES = 2,
    TSDB_OP_SERIES_NAME = 3,
    TSDB_OP_INSERT = 4,
    TSDB_OP_QUERY_RAW = 5,
    TSDB_OP_QUERY_AGG = 6,
    TSDB_OP_COMMIT = 7,
    TSDB_OP_FLUSH = 8,
    TSDB_OP_OPTIMIZE = 9,
    TSDB_OP_STATS = 10,
    TSDB_OP_QUERY_GROUP = 11,
};

#define TSDB_AGG_ROW 32 /* bytes per bucket in a QUERY_AGG reply */

#endif
