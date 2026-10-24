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
 *
 * Version 2: value types and nulls. INSERT, QUERY_RAW, QUERY_AGG and
 * QUERY_GROUP keep their version 1 layouts (float32 series only for the
 * first two; their buckets carry f32 min and max).
 *
 *   SERIES_CREATE u8 type, u16 len, name, u16 plen, path (plen 0: the database directory)
 *               -> u32 id; an existing series of another type is TSDB_ERR_INVALID
 *   SERIES_TYPE u32 id                         -> u8 type
 *   INSERT_VALUES u32 n, u32 series (or TSDB_MANY_SERIES), u8 type, u8 flags, u16 0, u32 0,
 *               i64 ts[n], values[n], [u8 valid[(n + 7) / 8]], [pad to 4, u32 series[n]]
 *               -> (nothing)
 *               values are f32, f64, i64, u8 (bool) or u32 (state codes); with
 *               flags & TSDB_VALID a bitmap follows, bit i (LSB first) set when
 *               value i is known
 *   QUERY_VALUES as QUERY_RAW
 *               -> chunks: u8 last, u8 type, u8 flags, u8 0, u32 n, i64 ts[n], values[n],
 *                  [u8 valid[(n + 7) / 8] when flags & TSDB_VALID]
 *               (the u16 and u32 after flags, and flag bits other than
 *               TSDB_VALID and TSDB_PACKED, are reserved: they must be zero)
 *               Version 3: with flags & TSDB_PACKED the arrays after the header
 *               are replaced by u32 raw_len and a zstd frame of their packed
 *               form (client/tsdb_pack.h): delta varints, byte planes. A
 *               client sends it only to a server whose HELLO says 3 or more.
 *   STATE       u32 series, u8 create, u16 len, name  -> u32 code
 *               (the series only picks the volume, whose dictionary it is)
 *   STATE_NAME  u32 series, u32 code           -> u16 len, name
 *   QUERY_BUCKETS u32 series, i64 start, i64 end, i64 interval, u8 unit, u32 count,
 *               u8 order, u64 limit, u16 len, time zone
 *               (unit 0: buckets of `interval` ms, or one bucket when 0; else
 *               calendar grouping as QUERY_GROUP)
 *               -> u32 n, n x (i64 start, u64 count, f64 sum, f64 min, f64 max,
 *                              i64 isum, i64 imin, i64 imax)
 *   Version 4: the server knows UTC only. QUERY_GROUP and QUERY_BUCKETS take
 *   no time zone (a zone other than "UTC" is TSDB_ERR_INVALID); for local
 *   periods a client computes their UTC bounds (tsdb_calendar.h) and sends
 *   QUERY_BOUNDS u32 series, i64 start, i64 end, u8 order, u64 limit, u32 n,
 *               i64 bounds[n]                   -> as QUERY_BUCKETS
 *               (bucket i begins at bounds[i]; bounds strictly increase)
 *   LAST_STATS                                 -> f64 elapsed_s, then the ten u64 of
 *               tsdb_query_stats in order: the connection's previous query
 *   Version 5: LTTB downsampling (tsdb_query_lttb). An older server answers
 *   the op with TSDB_ERR_INVALID; clients check HELLO for 5 first.
 *   QUERY_LTTB  u32 series, i64 start, i64 end, u64 threshold
 *               -> chunks as QUERY_VALUES (never with TSDB_VALID: no nulls),
 *               the chosen points in time order with their stored values
 *               (threshold < 3 or a state series: TSDB_ERR_INVALID)
 *   Version 6: time-weighted averages (tsdb_query_twa in tsdb.h has the rules). averages (tsdb_query_twa in tsdb.h has the rules).
 *   QUERY_TWA   u32 series, i64 start, i64 end, u8 method (0 STEP, 1 LINEAR), u8 order,
 *               u8 unit, u8 0, i64 max_gap, u64 limit, i64 interval, u32 count, u32 n,
 *               i64 bounds[n]                   (TSDB_TWA_HEADER bytes before the bounds)
 *               buckets: n > 0: bounds as QUERY_BOUNDS (unit and interval 0); else
 *               unit > 0: UTC calendar groups of `count` units; else buckets of
 *               `interval` ms, one bucket when 0
 *               -> u32 n, n x (i64 start, f64 twa, f64 area, i64 covered_ms, u64 count)
 *               A client sends it only to a server whose HELLO says 6 or more.
 */
#ifndef TSDB_PROTOCOL_H
#define TSDB_PROTOCOL_H

#define TSDB_PROTOCOL_VERSION 6
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
    TSDB_OP_SERIES_CREATE = 12,
    TSDB_OP_SERIES_TYPE = 13,
    TSDB_OP_INSERT_VALUES = 14,
    TSDB_OP_QUERY_VALUES = 15,
    TSDB_OP_STATE = 16,
    TSDB_OP_STATE_NAME = 17,
    TSDB_OP_QUERY_BUCKETS = 18,
    TSDB_OP_LAST_STATS = 19,
    TSDB_OP_QUERY_BOUNDS = 20,
    TSDB_OP_QUERY_LTTB = 21,
    TSDB_OP_QUERY_TWA = 22,
};

#define TSDB_AGG_ROW 32    /* bytes per bucket in a QUERY_AGG or QUERY_GROUP reply */
#define TSDB_BUCKET_ROW 64 /* bytes per bucket in a QUERY_BUCKETS reply */
#define TSDB_TWA_HEADER 56 /* bytes of a QUERY_TWA request before the bounds */
#define TSDB_TWA_ROW 40    /* bytes per bucket in a QUERY_TWA reply */
#define TSDB_VALUES_HEADER 16
#define TSDB_STATS_REPLY 88 /* bytes in a LAST_STATS reply after the status */
#define TSDB_VALID 1  /* flags: a validity bitmap follows the values */
#define TSDB_PACKED 2 /* flags: the arrays are packed and zstd compressed (version 3) */

/* bytes per value on the wire: bool is one byte */
static inline unsigned tsdb_wire_width(unsigned type) { return type == 3 ? 1 : type == 1 || type == 2 ? 8 : 4; }

#endif
