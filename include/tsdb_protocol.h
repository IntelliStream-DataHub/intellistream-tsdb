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
 *               (threshold < 3 or a state series: TSDB_ERR_INVALID; a threshold above the server's
 *               --lttb-max-threshold, default 16,777,216, is taken as that, here and in QUERY_MULTI)
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
 *               Bounds that do not strictly increase answer TSDB_ERR_INVALID for every series, trimmed or not.
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
 *   Version 13: the HA bodies (docs/ha.md 9 and 15.4, docs/design-ha-modes.md 2), defined once here and coded once in
 *   tsdb_wire.h. The floor stays 12: the new statuses only ever answer requests a version-12 client already sends, and
 *   a version-12 client treats any status it does not know as an error. A reply with one of these statuses carries the
 *   op's own reply shape for an error (nothing for most ops; for a streaming op the empty last chunk, QUERY_RAW
 *   u8 last, u32 n; QUERY_VALUES and QUERY_LTTB u8 last, u8 type, u8 flags, u8 0, u32 n; QUERY_MULTI's END frame:
 *   u32 index, u32 answered) and then the body:
 *   TSDB_ERR_UNAVAILABLE (-11)  u16 n, n x entry, [u64 resend_from]
 *               entry (TSDB_UNAVAIL_ENTRY bytes): u16 vol, u8 reason (TSDB_WHY_*), u8 flags, u32 epoch, i64 since_ms,
 *               u32 retry_after_ms, u64 outage_id. Definitely not applied: the client retries the same request after
 *               retry_after_ms (the largest of the entries) within its time budget. vol is TSDB_VOL_ANY (0xffff) when
 *               the server cannot say which volume (a request that names none, or a library-level refusal);
 *               epoch is 0 without HA; since_ms is Unix ms of the volume's last state change (0: unknown);
 *               outage_id counts the times the volume left AVAILABLE. resend_from is sent only for a frame that
 *               carried a seq (none does yet: no stream state in 13).
 *   TSDB_ERR_MOVED (-10)        u16 n, n x {u16 vol, u32 epoch, u8 flags, u32 node_id, u16 len, addr}, [u64 resend_from]
 *               flags TSDB_MOVED_HANDOFF: a clean release is under way. Definitely not applied; the client refreshes
 *               TOPOLOGY, or connects to addr, and sends the request again. addr is "host:port" (empty: no address
 *               known, the client asks TOPOLOGY).
 *   TSDB_ERR_PARTIAL (-12)      u16 nv, nv x {u16 vol, i16 status}, u32 nrows, u8 not_applied[(nrows + 7) / 8],
 *               then u16 n, n x entry (the UNAVAILABLE entries of the refused volumes). Bit i of not_applied is set
 *               when row i of the frame was not applied (status UNAVAILABLE or MOVED: send again; DROPPED: never).
 *   TSDB_ERR_DROPPED (-13)      u16 vol, i64 dropped_before. Final: not applied and never will be.
 *   TOPOLOGY    [u64 since_gen]  ->  u64 gen, u32 nn, nn x {u32 node_id, u8 alive, u16 len, addr},
 *               u32 n, n x {u16 vol, u8 role (TSDB_ROLE_*), u8 health (TSDB_HEALTH_*), u32 epoch, u8 reason, u8 0,
 *               u16 domain, i64 since_ms, u32 owner (a node_id), u16 server, u16 len, addr, u16 plen, path}
 *               An empty request is since_gen 0. since_gen is reserved: version 13 answers in full whatever it is
 *               (the clients send none), so no client may rely on a short reply; a version that answers an
 *               unchanged gen shortly will say so by its number. gen changes whenever anything in the reply does
 *               (clients poll; there is no push). domain and server are 0xffff when unknown. addr is the owner's address for clients;
 *               empty means "the node you are connected to". path is the volume's directory, sent only to a superuser
 *               (a client needs ids, not paths). tsdbd without a cluster answers itself as node 1, every volume
 *               as OWNER (FOLLOWER under --follow), with the health of its storage domain.
 *   STATUS      -> u32 len, then len bytes of `key=value` lines (docs/ha.md 15.4); superuser only.
 *   Version 13 also moves the planned numbers of the other designs one up: QUERY_LATEST is protocol 14, the change
 *   feed 15, statistics 16 (their op numbers are unchanged: 39 to 47; TOPOLOGY is 48 and STATUS 49).

 *   Version 14: QUERY_LATEST (docs/design-latest.md 6.2), the newest value of each of many series. The floor stays 12:
 *   a client sends the op only to a server whose HELLO says 14 or more (an older one: TSDB_ERR_INVALID, nothing sent).
 *   QUERY_LATEST u8 flags, u8 0, u16 threads (0: the server's --request-threads; more is clamped), u32 n (1..1048576),
 *               i64 end, [i64 ends[n]], u64 ids[n]
 *               flags: TSDB_LATEST_WITH_NULLS 1 (the newest row, a null included), TSDB_LATEST_PER_END 2 (ends[] is
 *               present and `end` must be 0); other bits are reserved (TSDB_ERR_INVALID). `end`: only rows before it;
 *               TSDB_TS_MAX: no bound. The length is exactly that.
 *               -> frames, each: i32 status, u32 first, u32 m, u8 last, u8 0, u16 0, then m records of 24 bytes:
 *                  i32 status, u8 kind (0 value, 1 null, 2 none), u8 type, u16 0, i64 ts, u64 value (the series'
 *                  slot zero-extended: float32 bits in the low 4 bytes, bool 0/1, state code, float64 or int64 bits;
 *                  0 with NULL and NONE). Records are in index order, at most 65,536 per frame; the frame with
 *                  last = 1 ends the reply. Kind NULL appears only with WITH_NULLS. A record with status NOTFOUND or
 *                  DENIED has kind NONE, type 0, ts 0, value 0. The frame's own status is the request's: INVALID
 *                  for a bad header (one frame, m = 0, last = 1), STOPPED after a CANCEL, NOMEM, or one of the HA
 *                  statuses (these carry the empty frame shape above, then their body).
 *               A denied id (no `read` on its series, existing or not) answers DENIED in its own record; a missing id
 *               answers NOTFOUND for a user with `read` on all series. CANCEL names a QUERY_LATEST as it does a
 *               QUERY_MULTI: it ends after the chunk in flight with STOPPED.
 */
#ifndef TSDB_PROTOCOL_H
#define TSDB_PROTOCOL_H

#include <stdint.h>

#define TSDB_PROTOCOL_VERSION 14
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
    X(USER_LIST, 38, user_list)                            \
    X(QUERY_LATEST, 39, query_latest)                      \
    X(TOPOLOGY, 48, topology)                              \
    X(STATUS, 49, status)

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

/* Version 13: the HA bodies. Reasons a volume is not available (ha.md 15.2; 0: none given). Never renumbered:
 * clients log them and metrics label by them. */
enum {
    TSDB_WHY_NONE = 0, TSDB_WHY_NFS_DOWN = 1, TSDB_WHY_NOT_MOUNTED = 2, TSDB_WHY_STUCK = 3, TSDB_WHY_FULL = 4,
    TSDB_WHY_BUSY = 5, TSDB_WHY_ROOT_DOWN = 6, TSDB_WHY_LOCKED = 7, TSDB_WHY_RECOVERING = 8, TSDB_WHY_DIVERGED = 9,
    TSDB_WHY_MAINTENANCE = 10, TSDB_WHY_SUSPECT = 11, TSDB_WHY_PROMOTING = 12, TSDB_WHY_FENCED = 13,
    TSDB_WHY_MISMATCH = 14, TSDB_WHY_NO_OWNER = 15, TSDB_WHY_UNREACHABLE = 16, TSDB_WHY_RELOADING = 17,
    TSDB_WHY_LAST = 17
};
enum { TSDB_HEALTH_AVAILABLE = 0, TSDB_HEALTH_DEGRADED = 1, TSDB_HEALTH_UNAVAILABLE = 2 };
enum { TSDB_ROLE_NONE = 0, TSDB_ROLE_OWNER = 1, TSDB_ROLE_STANDBY = 2, TSDB_ROLE_FOLLOWER = 3 };
#define TSDB_MOVED_HANDOFF 1        /* MOVED entry flags */
#define TSDB_VOL_ANY 0xffffu        /* the vol of an entry that names no one volume */
#define TSDB_UNAVAIL_ENTRY 28       /* bytes of one UNAVAILABLE entry */
#define TSDB_UNAVAIL_MAX 1024u      /* entries in one body: a decoder refuses more (a body is never that large) */
#define TSDB_RETRY_MIN_MS 1         /* retry_after_ms is clamped by the client to [1 ms, TSDB_RETRY_MAX_MS] */
#define TSDB_RETRY_MAX_MS 60000u
#define TSDB_BUDGET_MS 30000        /* the default time budget of a client retrying UNAVAILABLE and MOVED */
#define TSDB_TOPOLOGY_MAX_NODES 4096u
#define TSDB_TOPOLOGY_MAX_VOLS 65535u
#define TSDB_ADDR_MAX 255u          /* the longest address in a body */

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

#define TSDB_LATEST_HEADER 16            /* QUERY_LATEST bytes before the arrays (version 14) */
#define TSDB_LATEST_MAX_IDS 1048576u     /* ids in one QUERY_LATEST (1 << 20) */
#define TSDB_LATEST_FRAME_IDS 65536u     /* records in one reply frame, at most */
#define TSDB_LATEST_PREFIX 12            /* a reply frame after its status: u32 first, u32 m, u8 last, 3 zero bytes */
#define TSDB_LATEST_REC 24               /* bytes of one reply record */
#define TSDB_LATEST_WITH_NULLS 1         /* QUERY_LATEST flags */
#define TSDB_LATEST_PER_END 2

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
