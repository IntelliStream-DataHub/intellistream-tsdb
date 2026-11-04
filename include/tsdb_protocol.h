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
 * Series ids are u64 (tsdb_sid in tsdb.h) since version 11; every id array
 * starts at an 8-aligned offset of the payload.
 *
 * Requests and their successful responses:
 *
 *   HELLO       u32 version                    -> u32 version
 *               the lower of the client's and the server's: the version both
 *               speak. Version 11 changed the width of every series id, so
 *               there is nothing a version-10 peer could do safely: tsdbd
 *               answers a HELLO below 11 with TSDB_ERR_INVALID and closes the
 *               connection, and a client whose server answers below 11 fails
 *               its connect with TSDB_ERR_INVALID.
 *   SERIES      u8 create, u16 len, name [, u16 plen, path]  -> u64 id
 *               (path: the volume to create the series in, one the operator added with
 *               `tsdb <dir> volume add`: tsdbd never adds one, NOTFOUND otherwise; see tsdb_series_at)
 *   SERIES_NAME u64 id                         -> u16 len, name
 *   INSERT      u32 n, u32 0, u64 series, i64 ts[n], f32 v[n]    -> (nothing)
 *               or, with a series per row (series == TSDB_MANY_SERIES):
 *               u32 n, u32 0, u64 TSDB_MANY_SERIES, i64 ts[n], f32 v[n], zero bytes to a multiple of 8,
 *               u64 series[n]
 *   QUERY_RAW   u64 series, i64 start, i64 end [, u8 order, u64 limit]
 *               -> chunks, each a REPLY frame: u8 last, u32 n, i64 ts[n], f32 v[n]
 *               (order 1 = newest first; limit 0 = all)
 *   COMMIT, FLUSH, OPTIMIZE                    -> (nothing)
 *   STATS                                      -> tsdb_stats as 8 x u64
 *
 * Ops 6 (QUERY_AGG) and 11 (QUERY_GROUP), the version-1 aggregates with f32
 * min and max and no nulls, were removed in version 11 (QUERY_BUCKETS and
 * QUERY_BOUNDS answer the same queries); their numbers are unknown ops.
 *
 * Version 2: value types and nulls. INSERT and QUERY_RAW keep their version 1
 * layouts (float32 series only).
 *
 *   SERIES_CREATE u8 type, u16 len, name, u16 plen, path (plen 0: the database directory)
 *               -> u64 id; an existing series of another type is TSDB_ERR_INVALID
 *   SERIES_TYPE u64 id                         -> u8 type
 *   INSERT_VALUES u32 n, u8 type, u8 flags, u16 0, u64 series (or TSDB_MANY_SERIES),
 *               i64 ts[n], values[n], [u8 valid[(n + 7) / 8]], [zero bytes to a multiple of 8, u64 series[n]]
 *               -> (nothing)
 *               values are f32, f64, i64, u8 (bool) or u32 (state codes); with
 *               flags & TSDB_VALID a bitmap follows, bit i (LSB first) set when
 *               value i is known; a ts of INT64_MAX fails the whole batch with
 *               TSDB_ERR_INVALID (no half-open query range reaches it)
 *   QUERY_VALUES as QUERY_RAW
 *               -> chunks: u8 last, u8 type, u8 flags, u8 0, u32 n, i64 ts[n], values[n],
 *                  [u8 valid[(n + 7) / 8] when flags & TSDB_VALID]
 *               (the u16 and u32 after flags, and flag bits other than
 *               TSDB_VALID and TSDB_PACKED, are reserved: they must be zero)
 *               Version 3: with flags & TSDB_PACKED the arrays after the header
 *               are replaced by u32 raw_len and a zstd frame of their packed
 *               form (client/tsdb_pack.h): delta varints, byte planes (the
 *               series column: zigzag varints of the wrapping 64-bit
 *               difference from the previous id, the first from 0).
 *   STATE       u64 series, u8 create, u16 len, name  -> u32 code
 *               (the series only picks the volume, whose dictionary it is)
 *   STATE_NAME  u64 series, u32 code           -> u16 len, name
 *   QUERY_BUCKETS u64 series, i64 start, i64 end, i64 interval, u8 unit, u32 count,
 *               u8 order, u64 limit, u16 len, time zone
 *               (unit 0: buckets of `interval` ms, or one bucket when 0; else
 *               `count` calendar units (tsdb_unit) per bucket)
 *               -> u32 n, n x (i64 start, u64 count, f64 sum, f64 min, f64 max,
 *                              i64 isum, i64 imin, i64 imax)
 *   Version 4: the server knows UTC only. QUERY_BUCKETS takes
 *   no time zone (a zone other than "UTC" is TSDB_ERR_INVALID); for local
 *   periods a client computes their UTC bounds (tsdb_calendar.h) and sends
 *   QUERY_BOUNDS u64 series, i64 start, i64 end, u8 order, u64 limit, u32 n,
 *               i64 bounds[n]                   -> as QUERY_BUCKETS
 *               (bucket i begins at bounds[i]; bounds strictly increase)
 *   LAST_STATS                                 -> f64 elapsed_s, then the ten u64 of
 *               tsdb_query_stats in order: the connection's previous query
 *   Version 5: LTTB downsampling (tsdb_query_lttb). An older server answers
 *   the op with TSDB_ERR_INVALID; clients check HELLO for 5 first.
 *   QUERY_LTTB  u64 series, i64 start, i64 end, u64 threshold
 *               -> chunks as QUERY_VALUES (never with TSDB_VALID: no nulls),
 *               the chosen points in time order with their stored values
 *               (threshold < 3 or a state series: TSDB_ERR_INVALID)
 *   Version 6: time-weighted averages (tsdb_query_twa in tsdb.h has the rules).
 *   QUERY_TWA   u64 series, i64 start, i64 end, u8 method (0 STEP, 1 LINEAR), u8 order,
 *               u8 unit, u8 0, i64 max_gap, u64 limit, i64 interval, u32 count, u32 n,
 *               i64 bounds[n]                   (TSDB_TWA_HEADER bytes before the bounds)
 *               buckets: n > 0: bounds as QUERY_BOUNDS (unit and interval 0); else
 *               unit > 0: UTC calendar groups of `count` units; else buckets of
 *               `interval` ms, one bucket when 0
 *               -> u32 n, n x (i64 start, f64 twa, f64 area, i64 covered_ms, u64 count)
 *               A client sends it only to a server whose HELLO says 6 or more.
 *   Version 7: deletes and retention (tsdb_delete_range and friends). An older
 *   server answers these ops with TSDB_ERR_INVALID; clients check HELLO for 7 first.
 *   DELETE_RANGE u64 series, i64 start, i64 end  -> (nothing)
 *   REPLACE_RANGE u32 n, u8 type, u8 flags, u16 0, u64 series, i64 start, i64 end,
 *               i64 ts[n], values[n], [u8 valid[(n + 7) / 8]]  -> (nothing)
 *               (as INSERT_VALUES for one series, unpacked; flags: TSDB_VALID only)
 *   SET_RETENTION u64 series, i64 limit_ms      -> (nothing)
 *   GET_RETENTION u64 series                    -> i64 limit_ms
 *   Version 8: the spread (range, variances, standard deviations;
 *   tsdb_spread_bucket). QUERY_BUCKETS and QUERY_BOUNDS take an optional u8
 *   `fields` after the zone or the bounds; with fields & TSDB_SPREAD each
 *   bucket of the reply carries
 *               f64 range, f64 var_pop, f64 var_samp, f64 stddev_pop, f64 stddev_samp
 *   after its 64 bytes (TSDB_SPREAD_ROW bytes per bucket). Other bits are
 *   reserved (TSDB_ERR_INVALID). Without the byte the reply is as before (and
 *   the server does not compute the spread), so older clients read it
 *   unchanged; a client sends it only to a server whose HELLO says 8 or more
 *   (an older one answers TSDB_ERR_INVALID).
 *   Version 9: order and limit for LTTB. QUERY_LTTB takes an optional
 *               u8 order, u64 limit
 *   after the threshold (41 bytes instead of 32): the same points, newest
 *   first with order 1, and only the first `limit` of them in that order (0:
 *   all); the choice of points does not change. A client sends the longer
 *   form only to a server whose HELLO says 9 or more (an older one answers
 *   TSDB_ERR_INVALID); the 32-byte form is unchanged.
 *   A server of version 9 also stops bucketed queries with a limit early
 *   (QUERY_BUCKETS, QUERY_BOUNDS, QUERY_TWA compute only about the buckets
 *   they return); their replies do not change.
 *   Version 10: many series in one request (tsdb_query_multi in tsdb.h has the rules).
 *   QUERY_MULTI u8 op (QUERY_VALUES, QUERY_BUCKETS, QUERY_BOUNDS, QUERY_LTTB or QUERY_TWA), u8 flags,
 *               u16 threads (0: the server's --request-threads; more is clamped to it), u32 n (1..65536),
 *               u64 max_rows (rows or buckets sent in all; 0: --multi-max-rows; more is clamped), u32 tlen, u32 0,
 *               [i64 starts[n]] [i64 ends[n]] [i64 intervals[n]] (each with its flag: PER_START, PER_END,
 *               PER_INTERVAL), u64 series[n], then template[tlen]: the inner op's
 *               payload with series TSDB_MANY_SERIES, and 0 in every field a flag replaces. The length is exactly that.
 *               PER_INTERVAL: only the interval form of QUERY_BUCKETS and QUERY_TWA; TRIM_LO / TRIM_HI (bounds
 *               of QUERY_BOUNDS or QUERY_TWA only): series i's bounds start at the last one at or before its first
 *               row in range / end at the last one at or before its last row, as a client trims local periods.
 *               Other flag bits are reserved. Duplicate series are answered each time.
 *               -> for each series in index order, exactly the frames the inner op sends for it, each with
 *                  u32 index after the status (TSDB_MULTI_PREFIX; frames up to TSDB_MAX_FRAME + 4); a series'
 *                  status is its own (INVALID for a bad per-series value or a malformed id; an unknown id: 0
 *                  and no rows); then
 *                  the END frame, last: index 0xffffffff, the request's status (0, INVALID, STOPPED,
 *                  TOO_MANY_BUCKETS past max_rows, NOMEM) and u32 answered (series sent). A bad header or
 *                  template gets the END frame alone, with INVALID.
 *   CANCEL      u32 id                          -> no reply
 *               a QUERY_MULTI with that id, running or queued on this connection, ends with END STOPPED after the
 *               series in flight; other ids are ignored. Clients send both only to a server whose HELLO says 10.
 *               tsdbd reads a connection in order and stops reading it while 4 MiB of requests or 16 QUERY_MULTIs
 *               wait on it: a CANCEL sent behind those is read only as they run.
 *   Version 11: 64-bit series ids (tsdb_sid). Every series id in a request or
 *   reply is a u64; the reserved u32 of DELETE_RANGE and SET_RETENTION and
 *   the u32 0 of the INSERT_VALUES header were taken by it (the header stays
 *   16 bytes, type and flags moved to offsets 4 and 5); QUERY_AGG and
 *   QUERY_GROUP were removed. Replies of queries and QUERY_MULTI's u32 index
 *   (an index into the request, not an id) did not change.
 *   Version 12: login and access control (docs/design-auth.md). HELLO's reply grows:
 *   HELLO       u32 version                    -> u32 version, u16 flags, u16 n, n x (u8 len, mechanism)
 *               flags: TSDB_HELLO_AUTH (login required); mechanisms today: "SCRAM-SHA-256". A server
 *               below 12 or a client below 12 is refused as below 11 was (TSDB_ERR_INVALID).
 *   AUTH_START  u8 len, mechanism, u16 len, client-first-message  -> u16 len, server-first-message
 *   AUTH_CONTINUE u16 len, client-final-message                   -> u16 len, server-final-message
 *               SCRAM-SHA-256 (RFC 5802, RFC 7677) with gs2 header "n,," and the user in n=; any failure
 *               (unknown user, wrong proof, anything malformed) is TSDB_ERR_AUTH with no body, and the
 *               connection closes. Before login a connection may send only HELLO, AUTH_START and
 *               AUTH_CONTINUE, each frame at most TSDB_PRELOGIN_MAX bytes (a longer one closes it at once);
 *               any other op is answered TSDB_ERR_AUTH and closes it; a CANCEL closes it. AUTH_* after
 *               login: TSDB_ERR_INVALID.
 *   WHOAMI                                     -> u16 len, user, u8 flags, u32 n, n x grant
 *   USER_CREATE u16 len, name, u8 flags, verifier   -> (nothing)
 *   USER_DROP   u16 len, name                  -> (nothing)
 *   USER_ALTER  u16 len, name, u8 flags        -> (nothing)    (flags: TSDB_USER_SUPERUSER)
 *   USER_PASSWORD u16 len, name, verifier      -> (nothing)
 *   GRANT, REVOKE u16 len, name, u8 kind, u8 rights, u16 len, selector  -> (nothing)
 *   USER_LIST                                  -> chunks: u8 last, u32 n, n x (u16 len, name, u8 flags,
 *               u32 g, g x grant)
 *               verifier: u32 iterations, u8 salt_len, salt, u8 stored_key[32], u8 server_key[32]
 *               grant: u8 kind (TSDB_SEL_*), u8 rights (TSDB_RIGHT_*), u16 len, selector
 *   A request the user holds no right for is TSDB_ERR_DENIED, also when its series does not exist (only a user
 *   with the right on all series sees NOTFOUND); QUERY_MULTI answers a denied series with its own status
 *   TSDB_ERR_DENIED and the others as before. A user dropped while connected is answered TSDB_ERR_AUTH and
 *   the connection closes.
 */
#ifndef TSDB_PROTOCOL_H
#define TSDB_PROTOCOL_H

#include <stdint.h>

#define TSDB_PROTOCOL_VERSION 12
#define TSDB_PROTOCOL_MIN 12 /* the oldest version tsdbd and the clients speak */
#define TSDB_DEFAULT_PORT 7070
#define TSDB_MAX_FRAME (256u << 20)
#define TSDB_FRAME_HEADER 9 /* length + id + op */
#define TSDB_MANY_SERIES UINT64_MAX /* the series field of a request with a series per row; never an id */

/* Every request op: X(NAME, number, metric name). The server's rights table (server/auth.c), its metrics names and
 * the test of every op across tenants (tests/test_auth_ops.c) are generated from this one list, so an op added
 * here without a rights entry is refused (TSDB_ERR_DENIED) and the test names it.
 * 6 (QUERY_AGG) and 11 (QUERY_GROUP) were removed in version 11. */
#define TSDB_OPS(X)                                        \
    X(HELLO, 1, hello)                                     \
    X(SERIES, 2, series)                                   \
    X(SERIES_NAME, 3, series_name)                         \
    X(INSERT, 4, insert)                                   \
    X(QUERY_RAW, 5, query_raw)                             \
    X(COMMIT, 7, commit)                                   \
    X(FLUSH, 8, flush)                                     \
    X(OPTIMIZE, 9, optimize)                               \
    X(STATS, 10, stats)                                    \
    X(SERIES_CREATE, 12, series_create)                    \
    X(SERIES_TYPE, 13, series_type)                        \
    X(INSERT_VALUES, 14, insert_values)                    \
    X(QUERY_VALUES, 15, query_values)                      \
    X(STATE, 16, state)                                    \
    X(STATE_NAME, 17, state_name)                          \
    X(QUERY_BUCKETS, 18, query_buckets)                    \
    X(LAST_STATS, 19, last_stats)                          \
    X(QUERY_BOUNDS, 20, query_bounds)                      \
    X(QUERY_LTTB, 21, query_lttb)                          \
    X(QUERY_TWA, 22, query_twa)                            \
    X(QUERY_MULTI, 23, query_multi)                        \
    X(CANCEL, 24, cancel)                                  \
    X(DELETE_RANGE, 25, delete_range)                      \
    X(REPLACE_RANGE, 26, replace_range)                    \
    X(SET_RETENTION, 27, set_retention)                    \
    X(GET_RETENTION, 28, get_retention)                    \
    X(AUTH_START, 29, auth_start)                          \
    X(AUTH_CONTINUE, 30, auth_continue)                    \
    X(WHOAMI, 31, whoami)                                  \
    X(USER_CREATE, 32, user_create)                        \
    X(USER_DROP, 33, user_drop)                            \
    X(USER_ALTER, 34, user_alter)                          \
    X(USER_PASSWORD, 35, user_password)                    \
    X(GRANT, 36, grant)                                    \
    X(REVOKE, 37, revoke)                                  \
    X(USER_LIST, 38, user_list)

#define TSDB_OP_ENUM_(name, num, metric) TSDB_OP_##name = num,
enum { TSDB_OP_REPLY = 0, TSDB_OPS(TSDB_OP_ENUM_) };
#undef TSDB_OP_ENUM_

/* Version 12: login and access control */
#define TSDB_HELLO_AUTH 1          /* HELLO reply flags: login is required */
#define TSDB_PRELOGIN_MAX 4096     /* the largest frame (with its length field) before login */
#define TSDB_SCRAM_SHA256 "SCRAM-SHA-256"
#define TSDB_USER_SUPERUSER 1      /* user flags */
enum { TSDB_SEL_ALL = 0, TSDB_SEL_PREFIX = 1, TSDB_SEL_NAME = 2, TSDB_SEL_GLOB = 3 };
enum { TSDB_RIGHT_READ = 1, TSDB_RIGHT_INSERT = 2, TSDB_RIGHT_DELETE = 4, TSDB_RIGHT_ADMIN = 8 };

#define TSDB_BUCKET_ROW 64 /* bytes per bucket in a QUERY_BUCKETS reply */
#define TSDB_SPREAD 1       /* QUERY_BUCKETS/QUERY_BOUNDS fields: the spread follows each bucket (version 8) */
#define TSDB_SPREAD_ROW 104 /* bytes per bucket in a reply with TSDB_SPREAD */

#define TSDB_TWA_HEADER 60 /* bytes of a QUERY_TWA request before the bounds */
#define TSDB_TWA_ROW 40    /* bytes per bucket in a QUERY_TWA reply */
#define TSDB_VALUES_HEADER 16
#define TSDB_REPLACE_HEADER 32 /* REPLACE_RANGE: the INSERT_VALUES header, then start and end */
#define TSDB_STATS_REPLY 88 /* bytes in a LAST_STATS reply after the status */
#define TSDB_VALID 1  /* flags: a validity bitmap follows the values */
#define TSDB_PACKED 2 /* flags: the arrays are packed and zstd compressed (version 3) */

#define TSDB_MULTI_HEADER 24            /* QUERY_MULTI bytes before the per-series arrays (version 10) */
#define TSDB_MULTI_MAX_SERIES 65536u     /* series in one QUERY_MULTI */
#define TSDB_MULTI_PREFIX 4              /* a QUERY_MULTI reply frame: u32 index after the status */
#define TSDB_MULTI_END 0xffffffffu       /* the index of the END frame */
#define TSDB_MULTI_PER_START 1           /* QUERY_MULTI flags */
#define TSDB_MULTI_PER_END 2
#define TSDB_MULTI_PER_INTERVAL 4
#define TSDB_MULTI_WIRE_TRIM_LO 8
#define TSDB_MULTI_WIRE_TRIM_HI 16

/* bytes per value on the wire: bool is one byte */
static inline unsigned tsdb_wire_width(unsigned type) { return type == 3 ? 1 : type == 1 || type == 2 ? 8 : 4; }

#endif
