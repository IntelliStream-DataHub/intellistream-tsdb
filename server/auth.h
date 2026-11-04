/*
 * Login and access control in tsdbd (docs/design-auth.md §3 and §5): the table of what each op needs, SCRAM-SHA-256
 * on the server side, the users of USERS (a snapshot for logins, so USERS I/O never holds them up), and the rights
 * map each logged-in user's connections share. tsdbd.c calls auth_admit before every request and the auth_* checks where series ids are only known
 * inside a request (a row per series, QUERY_MULTI).
 */
#ifndef TSDBD_AUTH_H
#define TSDBD_AUTH_H

#include "tsdb.h"
#include "users.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* What an op needs (A14): before login only, any login, rights on its series, superuser, ... An op without an
 * entry (who == P_NONE) is refused with TSDB_ERR_DENIED. */
enum { P_NONE, P_ANY, P_PRE, P_LOGIN, P_SERIES, P_SUPER, P_STATS, P_COMMIT, P_ADMIN };
/* Where its series are: an id at payload offset 0 or 8 (at 8: TSDB_MANY_SERIES means an id per row, checked by
 * the handler), a name after u8, u16 len (offset 3), QUERY_MULTI's ids (the handler), none. */
enum { W_NONE, W_ID0, W_ID8, W_NAME3, W_MULTI };
/* How the op answers an error: a status, a raw chunk (u8 last, u32 n), a values chunk (8 bytes), QUERY_MULTI's END. */
enum { S_STATUS, S_RAW, S_VALUES, S_MULTI };
#define NEED_ANY 0x80 /* any right on the series */

typedef struct auth_op {
    uint8_t who, where, shape;
    uint8_t need;     /* R_* bits, or NEED_ANY */
    uint8_t create_at; /* 1 + the offset of a u8 `create` that raises the need to R_INSERT (SERIES, STATE); 0 none */
} auth_op;

extern const auth_op auth_ops[256];

typedef struct auth_user auth_user;

enum { AS_NONE, AS_STARTED, AS_IN, AS_FAILED };

/* One connection's login (worker side: requests of a connection run one at a time). */
typedef struct auth_conn {
    uint8_t state;
    auth_user *au; /* logged in as; NULL before login and with --no-auth (no checks) */
    uint64_t last_id; /* the previous id checked and its map byte: a run of rows of one series costs one compare */
    uint8_t last_byte;
    /* between AUTH_START and AUTH_CONTINUE */
    uint32_t uid; /* 0: a name not in USERS (the mock verifier, A4) */
    uint8_t stored_key[32], server_key[32];
    char *msg; /* client-first-message-bare "," server-first-message */
    size_t msg_len;
    char nonce[320];
    size_t nonce_len;
    char name[USER_NAME_MAX + 1];
} auth_conn;

/* Opens USERS in dir. enabled: login is required (A13: no superuser refuses the start, with the message the design
 * gives); writable: admin ops may change USERS (not --read-only or --follow). 0, or a TSDB_ERR_* after a message. */
int auth_init(const char *dir, bool enabled, bool writable, tsdb *db);
bool auth_enabled(void);
/* The reload thread: USERS on a one-second tick (A10), and what it changed reaches the connections. */
int auth_start_reload(void);
void auth_shutdown(void);
/* Reads USERS now (the thread's step; tests and the fuzzer call it directly). */
void auth_reload(void);

void auth_conn_init(auth_conn *a);
void auth_conn_free(auth_conn *a);

/* HELLO's reply after the version: u16 flags, u16 n, n x (u8 len, mechanism). Returns its length (out >= 32). */
size_t auth_hello(uint8_t *out);
/* The SCRAM steps. 0 and the reply body (u16 len, message) in out, or TSDB_ERR_AUTH / TSDB_ERR_INVALID (the login
 * failed: state AS_FAILED). peer names the client in the log line of a failure. */
int auth_scram_start(auth_conn *a, const uint8_t *p, size_t len, uint8_t *out, size_t cap, size_t *out_len,
                     const char *peer);
int auth_scram_continue(auth_conn *a, const uint8_t *p, size_t len, uint8_t *out, size_t cap, size_t *out_len,
                        const char *peer);

/* Before each request of a logged-in connection: takes the user's new state when it changed (A15). 0, or
 * TSDB_ERR_AUTH when the user was dropped (the connection closes). */
int auth_fresh(auth_conn *a);
/* The op table's checks for a request (who, and the series at W_ID0, W_ID8 or W_NAME3): 0, TSDB_ERR_DENIED,
 * TSDB_ERR_INVALID (too short to hold its series), TSDB_ERR_AUTH (not logged in, or AUTH_* answered elsewhere). */
int auth_check(auth_conn *a, uint8_t op, const uint8_t *p, size_t len);
/* The rights on one id / every id of a request (all or nothing, A16): 0 or TSDB_ERR_DENIED. */
int auth_id(auth_conn *a, tsdb_sid id, uint8_t need);
int auth_ids(auth_conn *a, const tsdb_sid *ids, size_t n, uint8_t need);
/* Whether the connection holds `need` on every series (no per-id checks needed). */
bool auth_all(const auth_conn *a, uint8_t need);

/* The admin ops (USER_*, GRANT, REVOKE, WHOAMI): 0 and a reply body in *out (malloc'd, may be NULL), or a
 * TSDB_ERR_*. USER_LIST answers through emit, chunks of whole entries; its return is emit's or an error. */
int auth_admin(auth_conn *a, uint8_t op, const uint8_t *p, size_t len, uint8_t **out, size_t *out_len);
int auth_user_list(auth_conn *a, int (*emit)(void *ctx, bool last, uint32_t n, const uint8_t *e, size_t len),
                   void *ctx);

/* Test hook: the server's part of the nonce, used as is when set (the RFC 7677 test and the fuzzer replay fixed
 * exchanges). Printable, no ',', under 64 bytes. */
extern const char *auth_test_nonce;
/* Test hooks: the HMACs computed by logins so far; called by auth_reload before it reads USERS (users_mu held). */
extern atomic_uint auth_test_hmacs;
extern void (*auth_test_reload_hook)(void);
/* The bytes the rights maps of all users may hold together (auth_init: a sixteenth of the RAM unless set before),
 * and what they hold. */
extern size_t auth_map_budget;
size_t auth_map_total(void);

#endif
