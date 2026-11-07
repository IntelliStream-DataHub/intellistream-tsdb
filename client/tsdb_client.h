/*
 * Client for tsdbd. Mirrors the embedded API in tsdb.h; one tsdbc is one TCP
 * connection and must be used by one thread at a time (open one per thread).
 *
 *   tsdbc *c;
 *   tsdbc_connect_auth("127.0.0.1", 7070, user, password, &c);  (a user with read and insert on the series)
 *   tsdb_sid id;
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
#include "tsdb_wire.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdbc tsdbc;

/* Connects, giving up on each address after TSDBC_CONNECT_TIMEOUT_MS (10 s, as
 * the Rust, Python and Java clients do). TCP keepalive is on: the first probe
 * after 30 s of silence, then every 10 s, and the connection is dropped after 3
 * unanswered probes, so a server that vanished without a reset is noticed
 * about a minute later instead of never. Requests wait without a time limit
 * unless tsdbc_set_timeout says otherwise. */
#define TSDBC_CONNECT_TIMEOUT_MS 10000
#define TSDBC_KEEPALIVE_IDLE_S 30
#define TSDBC_KEEPALIVE_INTERVAL_S 10
#define TSDBC_KEEPALIVE_COUNT 3
/* Connecting asks the server's protocol version (HELLO): a server below 12 (TSDB_PROTOCOL_MIN) is refused with
 * TSDB_ERR_INVALID and a line on stderr. Without a user it works only with a tsdbd started with --no-auth; one that
 * requires a login answers TSDB_ERR_AUTH (tsdbc_connect_auth, tsdbc_connect_opts). */
int tsdbc_connect(const char *host, int port, tsdbc **out);
/* The same with its own connect timeout; 0 waits as long as the kernel does. */
int tsdbc_connect_timeout(const char *host, int port, int connect_ms, tsdbc **out);

/* Login (SCRAM-SHA-256, docs/design-auth.md). The client checks the server's signature, so a server that does not
 * hold the user's verifier fails the connect too. PBKDF2 runs once per (user, password, salt, iterations) per
 * process: the keys it gives are cached, and later connects cost four HMACs. Any failure (unknown user, wrong
 * password, a server that cannot prove itself) is TSDB_ERR_AUTH.
 *
 * A password is 1 to TSDBC_PASSWORD_MAX bytes, in every client and the CLI; a password file holds it in its first
 * line, without the "\n" or "\r\n" that ends it. An empty or longer password, or a password file that cannot be read
 * or holds none, is TSDB_ERR_INVALID (a file's path and the reason go to stderr), never retried as a network error;
 * so is a user with neither password nor password_file. */
#define TSDBC_PASSWORD_MAX 1023
typedef struct tsdbc_options {
    const char *user;          /* NULL: no login (a tsdbd with --no-auth) */
    const char *password;      /* the password's bytes (UTF-8, no normalization), or */
    const char *password_file; /* the first line of this file */
    int connect_ms;            /* 0: TSDBC_CONNECT_TIMEOUT_MS; below 0 waits as long as the kernel does */
    int require_auth;          /* refuse (TSDB_ERR_AUTH) a server that does not ask for a login */
} tsdbc_options;
/* With credentials, a server that does not ask for a login (--no-auth) is used without one unless require_auth. */
int tsdbc_connect_opts(const char *host, int port, const tsdbc_options *o, tsdbc **out);
int tsdbc_connect_auth(const char *host, int port, const char *user, const char *password, tsdbc **out);

/* Users and grants (tsdbd's USERS, managed over the wire by a superuser or a user with admin on a selector; a
 * tsdbd with --no-auth refuses these with TSDB_ERR_DENIED: use `tsdb <dir> user` there). Flags: TSDB_USER_SUPERUSER.
 * A grant's kind is TSDB_SEL_ALL, _PREFIX, _NAME or _GLOB (tsdb_protocol.h), rights TSDB_RIGHT_* bits. The
 * password never leaves the client: its verifier (a fresh 16-byte salt, iterations, 0 for 4096) is computed here;
 * a password outside the rule above is TSDB_ERR_INVALID. A grant read back may carry a kind or rights bits this
 * client does not know (a later server's): they are kept as they came. */
typedef struct tsdbc_user_grant {
    uint8_t kind, rights;
    char *selector; /* "" for all */
} tsdbc_user_grant;
typedef struct tsdbc_user {
    char *name;
    uint8_t flags;
    uint32_t ngrants;
    tsdbc_user_grant *grants;
} tsdbc_user;
int tsdbc_whoami(tsdbc *c, tsdbc_user **out); /* "" and TSDB_USER_SUPERUSER without login; free with tsdbc_users_free(out, 1) */
int tsdbc_user_list(tsdbc *c, tsdbc_user **out, size_t *n); /* the users this one may see (itself, whom it manages) */
void tsdbc_users_free(tsdbc_user *u, size_t n);
int tsdbc_user_create(tsdbc *c, const char *name, const char *password, uint32_t iterations, uint8_t flags);
int tsdbc_user_drop(tsdbc *c, const char *name);
int tsdbc_user_alter(tsdbc *c, const char *name, uint8_t flags);
int tsdbc_user_password(tsdbc *c, const char *name, const char *password, uint32_t iterations);
int tsdbc_grant(tsdbc *c, const char *name, uint8_t kind, const char *selector, uint8_t rights);
int tsdbc_revoke(tsdbc *c, const char *name, uint8_t kind, const char *selector, uint8_t rights);
/* Keepalive probes: idle_s of silence before the first, interval_s between
 * them, count unanswered before the connection is dropped; idle_s = 0 turns
 * keepalive off. */
int tsdbc_set_keepalive(tsdbc *c, int idle_s, int interval_s, int count);
void tsdbc_close(tsdbc *c); /* waits for pipelined inserts first; use tsdbc_wait to see their errors */
const char *tsdbc_strerror(int err); /* the client does not link the storage engine */

int tsdbc_series(tsdbc *c, const char *name, int create, tsdb_sid *id);
/* Creates the series in the volume `path` on the server (see tsdb_series_at), one the operator added with
 * `tsdb <dir> volume add`: tsdbd never adds one (TSDB_ERR_NOTFOUND). */
int tsdbc_series_at(tsdbc *c, const char *name, const char *path, tsdb_sid *id);
/* TSDB_ERR_INVALID if the name and its NUL do not fit in len bytes (65536 always do). */
int tsdbc_series_name(tsdbc *c, tsdb_sid id, char *buf, size_t len);

/* Inserts travel packed and zstd compressed (level 1 by default); 0 sends plain
 * arrays, which costs 16 bytes a row on the wire instead of a few, but no CPU.
 * Negative levels are faster. */
int tsdbc_set_compression(tsdbc *c, int level);
/* Bytes written to the server so far, frame headers included. */
uint64_t tsdbc_bytes_sent(const tsdbc *c);
/* Bounds every wait for the server (send or receive) to ms milliseconds; 0, the
 * default, waits forever. A call that times out returns TSDB_ERR_IO and the
 * connection is of no further use: the reply may still come, so every later
 * call on it fails with TSDB_ERR_IO too (as after any I/O or protocol error).
 * Close it and connect again. */
int tsdbc_set_timeout(tsdbc *c, int ms);

int tsdbc_insert(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const float *values, size_t n);
int tsdbc_insert_series(tsdbc *c, tsdb_sid series, const int64_t *ts, const float *values, size_t n);
int tsdbc_insert_async(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const float *values, size_t n);
int tsdbc_insert_series_async(tsdbc *c, tsdb_sid series, const int64_t *ts, const float *values, size_t n);
int tsdbc_wait(tsdbc *c);

/* ---- Protocol 13: errors with a body, retry, several nodes, TOPOLOGY and STATUS (docs/ha.md 9, 15.4) ----
 *
 * A request the server refuses before it does anything answers TSDB_ERR_UNAVAILABLE (the volume's storage is down or
 * recovering) or TSDB_ERR_MOVED (another node owns the volume): definitely not applied. The client sends it again,
 * after the retry_after_ms the reply names, until the time budget is spent (30 s; tsdbc_set_retry), and then returns
 * the status. A MOVED goes to the node it names, or another node of the connect string (tsdbc_connect_nodes), and
 * is sent again there. Pipelined inserts (tsdbc_insert_async) that were refused are sent again in order from the
 * first refused one, those acknowledged after it too (last write wins keeps that safe: the same rows, the same
 * order). Not retried: a query whose reply has begun (rows were delivered), QUERY_MULTI, and an I/O error or
 * timeout (the request may have been applied: the caller decides). TSDB_ERR_PARTIAL and TSDB_ERR_DROPPED are
 * returned with their bodies; a server of one node never sends them. DROPPED rows are never to be sent again.
 * A single request that is refused for the whole budget costs that budget in latency: set it lower (or 0 for none)
 * for a caller that has its own retry.
 */
#define TSDBC_ERR_ENTRIES 16 /* entries of a body kept; the totals say how many there were */
typedef struct tsdbc_moved {
    uint16_t vol;
    uint32_t epoch;
    uint8_t flags; /* TSDB_MOVED_HANDOFF */
    uint32_t node_id;
    char addr[TSDB_ADDR_MAX + 1]; /* empty: not named */
} tsdbc_moved;
typedef struct tsdbc_err_info {
    int status;   /* TSDB_ERR_MOVED, _UNAVAILABLE, _PARTIAL, _DROPPED of the latest such reply; 0 before any */
    int has_body; /* 0: the server sent none (protocol 12) or it was malformed: only the status is known */
    unsigned nunavail, unavail_total; /* UNAVAILABLE entries (also PARTIAL's refused volumes) */
    tsdb_unavail unavail[TSDBC_ERR_ENTRIES];
    unsigned nmoved, moved_total;
    tsdbc_moved moved[TSDBC_ERR_ENTRIES];
    int has_resend;
    uint64_t resend_from; /* of an UNAVAILABLE or MOVED for a frame that carried a seq */
    unsigned nvols;       /* PARTIAL: the per-volume statuses, */
    tsdb_partial_vol vols[TSDBC_ERR_ENTRIES];
    uint32_t nrows;                /* the rows of the frame, */
    const uint8_t *not_applied;    /* and their bitmap (bit i set: row i not applied); valid until the next call */
    uint16_t dropped_vol;          /* DROPPED */
    int64_t dropped_before;
} tsdbc_err_info;
/* The latest such reply's body (a copy; not_applied points into the connection's buffer); its status. */
int tsdbc_last_error(const tsdbc *c, tsdbc_err_info *out);
/* The UNAVAILABLE entries of the latest reply, up to cap; *n is how many there were. INVALID if it was not UNAVAILABLE. */
int tsdbc_unavailable(const tsdbc *c, tsdb_unavail *e, unsigned cap, unsigned *n);
/* The time budget of the retries of one request in ms (default TSDB_BUDGET_MS). 0 turns retries off, which is
 * possible only while no request is in flight (it keeps its frame for a resend); TSDB_ERR_INVALID otherwise. */
int tsdbc_set_retry(tsdbc *c, int budget_ms);
/* Connects to "host:port,host:port,..." (IPv6 as [::1]:7070; the default port 7070): the list is shuffled once and
 * the first node that answers (HELLO and login) is the connection; a MOVED without an address, or one whose
 * address cannot be reached, goes on to the next node. TSDB_ERR_IO when none answers. The password is kept
 * (and wiped at tsdbc_close) for the reconnects. */
int tsdbc_connect_nodes(const char *nodes, const tsdbc_options *o, tsdbc **out);

typedef struct tsdbc_node {
    uint32_t node_id;
    int alive;
    char addr[TSDB_ADDR_MAX + 1]; /* empty: the node you are connected to */
} tsdbc_node;
typedef struct tsdbc_vol {
    uint16_t vol;
    uint8_t role, health; /* TSDB_ROLE_*, TSDB_HEALTH_* */
    uint32_t epoch;
    uint8_t reason; /* TSDB_WHY_* */
    uint16_t domain; /* 0xffff: unknown */
    int64_t since_ms;
    uint32_t owner;  /* a node_id */
    uint16_t server;
    char addr[TSDB_ADDR_MAX + 1];
    char *path; /* the volume's directory; empty unless the user is a superuser */
} tsdbc_vol;
typedef struct tsdbc_topology {
    uint64_t gen;
    size_t nnodes, nvols;
    tsdbc_node *nodes;
    tsdbc_vol *vols;
} tsdbc_topo;
/* TOPOLOGY (protocol 13): TSDB_ERR_INVALID from an older server. Free with tsdbc_topology_free. */
int tsdbc_topology(tsdbc *c, tsdbc_topo **out);
void tsdbc_topology_free(tsdbc_topo *t);
/* STATUS (a superuser; protocol 13): key=value lines, malloc'd and NUL terminated (free it). */
int tsdbc_status(tsdbc *c, char **text);


int tsdbc_query_raw(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx);
int tsdbc_query_points(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, tsdb_points *out);
int tsdbc_query_agg(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                    size_t *nout);
/* With order and limit, as tsdb_query_agg_ex (opts NULL: as tsdbc_query_agg). */
int tsdbc_query_agg_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);
/* Ordering, limits and calendar grouping, as tsdb_query_raw_ex / tsdb_query_group_ex. */
int tsdbc_query_raw_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_raw_fn fn, void *ctx);
int tsdbc_query_points_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                          tsdb_points *out);
/* With a time zone in `group`, the client computes the UTC bounds of the
 * local periods (tsdb_calendar.h) and asks for those: the server knows UTC only. */
int tsdbc_query_group(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);
/* Buckets beginning at given UTC instants, as tsdb_query_bounds. */
int tsdbc_query_bounds(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                       const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);
/* The spread (range, variances, standard deviations), as tsdb_query_spread,
 * _spread_group and _spread_bounds (tsdb.h); a group in a time zone other than
 * UTC is turned into bounds here. */
int tsdbc_query_spread(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
int tsdbc_query_spread_group(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                             const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
int tsdbc_query_spread_bounds(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds,
                              size_t n, const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
/* Time-weighted averages, as tsdb_query_twa, _twa_group and _twa_bounds (tsdb.h);
 * a group in a time zone other than UTC is turned into bounds here. */
int tsdbc_query_twa(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms,
                    const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
int tsdbc_query_twa_group(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                          const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
int tsdbc_query_twa_bounds(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                           const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);

/* Value types and nulls, as tsdb_series_create,
 * tsdb_insert_values, tsdb_query_values and the state dictionary. */
int tsdbc_series_create(tsdbc *c, const char *name, tsdb_type type, const char *path, tsdb_sid *id);
int tsdbc_series_type(tsdbc *c, tsdb_sid id, tsdb_type *type);
int tsdbc_insert_values(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdbc_insert_series_values(tsdbc *c, tsdb_sid series, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdbc_insert_values_async(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const tsdb_values *values,
                              size_t n);
int tsdbc_insert_series_values_async(tsdbc *c, tsdb_sid series, const int64_t *ts, const tsdb_values *values,
                                     size_t n);
int tsdbc_query_values(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_values_fn fn, void *ctx);
/* The server's protocol version from HELLO (asked once per connection; 1 for a
 * server that predates HELLO), after waiting for pipelined inserts; 0 if the
 * connection failed. */
uint32_t tsdbc_server_version(tsdbc *c);
/* LTTB downsampling, as tsdb_query_lttb. */
int tsdbc_query_lttb(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, size_t threshold, tsdb_values_fn fn,
                     void *ctx);
/* Newest first or cut to a limit, as tsdb_query_lttb_ex. */
int tsdbc_query_lttb_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, size_t threshold,
                        const tsdb_query_opts *opts, tsdb_values_fn fn, void *ctx);
int tsdbc_state(tsdbc *c, tsdb_sid series, const char *name, uint32_t *code);
int tsdbc_state_lookup(tsdbc *c, tsdb_sid series, const char *name, uint32_t *code);
int tsdbc_state_name(tsdbc *c, tsdb_sid series, uint32_t code, char *buf, size_t len);

/* What the server did for this connection's previous query (tsdb_last_query_stats there). */
int tsdbc_last_query_stats(tsdbc *c, tsdb_query_stats *st);

/* Deletes, replaces and retention, as tsdb_delete_range, tsdb_replace_range, tsdb_set_retention and
 * tsdb_get_retention. A replace travels as
 * one frame, so n is limited by the frame size (about 256 MiB). */
int tsdbc_delete_range(tsdbc *c, tsdb_sid series, int64_t start, int64_t end);
int tsdbc_replace_range(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *ts,
                        const tsdb_values *values, size_t n);
int tsdbc_set_retention(tsdbc *c, tsdb_sid series, int64_t limit_ms);
int tsdbc_get_retention(tsdbc *c, tsdb_sid series, int64_t *limit_ms);

/* Many series in one request, with
 * the types, calls and statuses of tsdb_query_multi (tsdb.h). mo->threads and mo->max_rows go to the server
 * (0: its --request-threads and --multi-max-rows; larger values are clamped there); mo->cancelled is polled after
 * each delivery; the other hooks are the server's own. A list over 65,536 series goes in several requests, with
 * the indexes running on. Local periods (q->group with a zone other than UTC) cost two more requests (the first
 * and last row of every series) to make one union of UTC bounds, which the server trims per series; bounds too
 * many for a frame go series by series. fn returning nonzero sends CANCEL, reads the rest of the reply and
 * returns TSDB_ERR_STOPPED; the connection stays usable. */
int tsdbc_query_multi(tsdbc *c, const tsdb_multi_query *q, const tsdb_sid *series, size_t n,
                      const tsdb_multi_opts *mo, tsdb_multi_fn fn, void *ctx);
/* The same into out[n], as tsdb_query_multi_collect; free with tsdbc_series_results_free (the client does not
 * link the engine). On TSDB_ERR_STOPPED or TOO_MANY_BUCKETS out holds the series answered before. */
int tsdbc_query_multi_collect(tsdbc *c, const tsdb_multi_query *q, const tsdb_sid *series, size_t n,
                              const tsdb_multi_opts *mo, tsdb_series_result **out);
void tsdbc_series_results_free(tsdb_series_result *r, size_t n);

/* The newest value of each series, as tsdb_latest (tsdb.h; out[i] for ids[i], n up to 1 << 20, opts NULL: no bound,
 * the newest non-null value). Needs a server that speaks protocol 14: against an older one the call fails with
 * TSDB_ERR_INVALID and sends nothing. A series the user may not read answers status TSDB_ERR_DENIED whether or not it
 * exists; one that does not exist (for a user with `read` on all) TSDB_ERR_NOTFOUND (kind NONE, type 0). A read-only
 * request: UNAVAILABLE and MOVED before the first reply frame are retried as for other reads. */
int tsdbc_latest(tsdbc *c, const tsdb_sid *ids, size_t n, const tsdb_latest_opts *opts, tsdb_latest_result *out);
/* The same by name. A name is resolved once per connection (tsdbc_series without create) and its id kept, since an id
 * never changes meaning; a name that does not resolve answers NOTFOUND or DENIED in its own out[i]. An empty or NULL
 * name answers TSDB_ERR_INVALID in its record. */
int tsdbc_latest_by_name(tsdbc *c, const char *const *names, size_t n, const tsdb_latest_opts *opts,
                         tsdb_latest_result *out);

int tsdbc_commit(tsdbc *c);
int tsdbc_flush(tsdbc *c);
int tsdbc_optimize(tsdbc *c);
int tsdbc_stats(tsdbc *c, tsdb_stats *st);

#ifdef __cplusplus
}
#endif

#endif
