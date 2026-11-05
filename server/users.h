/*
 * Users, their verifiers and grants (docs/design-auth.md §2 and §4): the file USERS in the database root, and the
 * grant model as pure functions: selectors (all, prefix, name, glob), rights, rights_of, the inside-rules (A25) and
 * who may manage whom (A9). Shared by tsdbd and the CLI (`tsdb <dir> user ...`), which both append to USERS under
 * its OFD lock. A `users` is not thread-safe: tsdbd serializes calls with its own mutex.
 */
#ifndef TSDB_USERS_H
#define TSDB_USERS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* Selector kinds and rights, as on the wire (tsdb_protocol.h from protocol 12) and in GRANT records. */
enum { SEL_ALL = 0, SEL_PREFIX = 1, SEL_NAME = 2, SEL_GLOB = 3 };
enum { R_READ = 1, R_INSERT = 2, R_DELETE = 4, R_ADMIN = 8, R_WRITE = R_INSERT | R_DELETE, R_EVERY = 15 };
#define USER_SUPERUSER 1 /* the only user flag */

#define USER_NAME_MAX 128
#define SEL_MAX 65535  /* a name or prefix selector (u16 on the wire) */
#define GLOB_MAX 256   /* a glob as given, before normalization (A24) */
#define USER_GLOBS 64  /* glob grants per user */
#define SALT_MIN 16
#define SALT_MAX 64
#define ITER_MIN 4096

typedef struct user_verifier {
    uint32_t iterations;
    uint8_t salt_len;
    uint8_t salt[SALT_MAX];
    uint8_t stored_key[32], server_key[32];
} user_verifier;

/* A grant, normalized: a glob that is a name or a prefix is stored as one (A24), a run of '*' as one '*', and only
 * '*', '?' and '\' escaped, so that two globs of one set compare equal more often (never less soundly). */
typedef struct user_grant {
    uint8_t kind, rights;
    uint16_t len;
    char *sel;       /* NUL-terminated; a glob in its escaped form */
    char *lit;       /* glob: the literal prefix (bytes before the first wildcard), unescaped */
    uint16_t lit_len;
    char *suf;       /* glob: the literal suffix (bytes after the last wildcard), unescaped */
    uint16_t suf_len;
    struct glob_prog *prog; /* glob: compiled for glob_match's linear run (users.c) */
} user_grant;

struct umatch;
struct glob_prog;

typedef struct user {
    uint32_t uid; /* assigned at creation, never reused */
    uint8_t flags;
    char name[USER_NAME_MAX + 1];
    bool has_verifier;
    user_verifier v;
    user_grant *g;
    uint32_t ng, gcap;
    struct umatch *m; /* rights_of's index, built on first use */
    uint64_t batch;   /* the load batch that made this copy (users.c) */
} user;

typedef struct users {
    char dir[4096], path[4200];
    bool writable;
    int dfd;         /* dir, opened once: USERS and USERS.compact are reached through it, never through a symlink */
    int fd;          /* USERS, open for appending and its lock; -1 until a change needs it */
    dev_t dev;       /* the file read so far, and how far (whole records) */
    ino_t ino;
    uint64_t off, last_off; /* end of the records applied; start of the last one */
    uint64_t last_check;
    bool have_header;
    uint8_t mock_key[32];
    uint8_t file_id[16]; /* the HEADER's: new with every file, so a file put in place of another is never taken for it */
    bool name_synced;    /* this process fsynced the directory since the file of synced_id got its name */
    uint8_t synced_id[16];
    user **u; /* live users, by uid */
    uint32_t n, cap;
    struct uname *names; /* the live users by name (open addressing on the name's hash) */
    uint32_t names_mask, names_n;
    uint32_t max_uid; /* the highest uid ever created */
    uint64_t batch;
    bool damaged; /* a reload found damage: the last good state stays, changes are refused (A11) */
    bool failed;  /* a write or sync of USERS failed: nothing more is acknowledged (fail-stop) */
    void (*on_fail)(void *arg, const char *msg); /* tsdbd exits; NULL: the call returns TSDB_ERR_IO */
    void *fail_arg;
} users;

/* ------------------------------------------------------------ the file */

/* Reads <dir>/USERS (a missing file is an empty one). writable: changes may follow (the file is created by the first
 * one). TSDB_ERR_CORRUPT for damage before a valid record or a record that makes no sense; TSDB_ERR_IO. */
int users_open(users *us, const char *dir, bool writable);
/* Reads what was appended since, or the whole file when it was replaced (a compaction). An incomplete record at the
 * end is read next time. Damage: TSDB_ERR_CORRUPT, the state stays as it was and `damaged` is set. */
int users_reload(users *us);
void users_close(users *us);

/* Changes. `actor` is the uid of the user asking, 0 for the operator at the shell (the CLI: who can write the
 * directory holds every right already). Each takes the lock, reads what others appended, checks the change against
 * that (A9; TSDB_ERR_DENIED), appends, fdatasyncs and applies it. TSDB_ERR_NOTFOUND for a user the actor could
 * create but that does not exist (DENIED for one it could not), TSDB_ERR_INVALID for a bad name, selector, rights
 * or verifier or a name that exists, TSDB_ERR_IO after a write or sync failed (fail-stop), TSDB_ERR_CORRUPT while
 * `damaged`, TSDB_ERR_READONLY without `writable`. An answer that changes nothing (flags as they are, a grant
 * already held) is given only after an fdatasync of the file: the state it rests on may be another writer's
 * unsynced append. users_revoke of rights the user holds none of on that selector: TSDB_ERR_NOTFOUND (DENIED to an
 * actor that could not create the name). Revoking read, insert or delete from a grant holding admin takes admin
 * too (admin implies them) and keeps the other implied rights. */
int users_create(users *us, uint32_t actor, const char *name, uint8_t flags, const user_verifier *v, uint32_t *uid);
int users_drop(users *us, uint32_t actor, const char *name);
int users_set_verifier(users *us, uint32_t actor, const char *name, const user_verifier *v);
int users_set_flags(users *us, uint32_t actor, const char *name, uint8_t flags);
int users_grant(users *us, uint32_t actor, const char *name, uint8_t kind, const char *sel, size_t len,
                uint8_t rights);
int users_revoke(users *us, uint32_t actor, const char *name, uint8_t kind, const char *sel, size_t len,
                 uint8_t rights);
/* Rewrites USERS with one record per live fact (tmp, fsync, rename, directory fsync) under the lock, and under a lock
 * on the new file from before the rename until the directory fsync. */
int users_compact(users *us);

/* A private deep copy of a user (its grants and verifier, no index yet), for a reader that must not share the
 * `users` state: tsdbd's per-user rights map works on one. NULL when out of memory. */
user *user_copy(const user *u);
void user_release(user *u);

const user *users_find(const users *us, const char *name);
const user *users_by_uid(const users *us, uint32_t uid);
uint32_t users_superusers(const users *us);

/* ------------------------------------------------------- the grant model */

bool user_name_valid(const char *name, size_t len);
bool verifier_valid(const user_verifier *v);
/* Normalizes a selector (A24): out gets the kind and the bytes (NUL-terminated, at most len + 1 bytes). 0 or
 * TSDB_ERR_INVALID (a bad kind, an empty name, a NUL byte, over SEL_MAX or GLOB_MAX, a trailing lone '\'). */
int sel_normalize(uint8_t kind, const char *in, size_t len, uint8_t *out_kind, char *out, size_t *out_len);
/* Parses "all", "prefix:P", "name:N", "glob:G" into its kind and bytes (not normalized). */
int sel_parse(const char *text, uint8_t *kind, const char **sel, size_t *len);
/* Rights as text, one grammar in tsdbd, the CLI and every client (design-auth.md §2): names separated by commas,
 * spaces around a name ignored; "read", "insert", "delete", "admin", "write" (insert and delete), "none" (no bits)
 * and "0xHH" (two hex digits: bits a later version may name). rights_parse: 0 and *out, or TSDB_ERR_INVALID (an empty
 * text or name, any other word). rights_text writes what rights_parse reads back: the four names in that order, the
 * other bits as one "0xhh", "none" for 0. */
int rights_parse(const char *text, uint8_t *out);
#define RIGHTS_TEXT_MAX sizeof "read,insert,delete,admin,0xf0"
const char *rights_text(uint8_t r, char buf[RIGHTS_TEXT_MAX]);

bool glob_match(const char *glob, size_t glen, const char *name, size_t len);
/* Whether the selector matches the series name. */
bool grant_matches(const user_grant *g, const char *name, size_t len);
/* The rights of u on the series name: every right for a superuser; else the union over the matching grants, with
 * admin implying read, insert and delete. Indexed (§2 "Matching"); rights_of_slow tries every grant. */
uint8_t rights_of(user *u, const char *name, size_t len);
uint8_t rights_of_slow(const user *u, const char *name, size_t len);
/* Builds rights_of's index of u now (false: out of memory). A user that threads share gets it before it is shared:
 * rights_of then only reads u, and needs no lock. */
bool user_index_build(user *u);

/* Selector s lies inside t (§2's table; both normalized). */
bool sel_inside(const user_grant *s, const user_grant *t);
/* x may create, drop, set the password of, grant to, revoke from, list. x NULL is the operator (every right).
 * They use x's rights_of index (built on first use), so they are as little thread-safe as rights_of. */
bool user_manages(const user *x, const user *u);
bool may_create(const user *x, const char *name, uint8_t flags);
bool may_grant(const user *x, const user *u, const user_grant *s);
bool may_revoke(const user *x, const user_grant *s);
bool may_passwd(const user *x, const user *u);
bool may_list(const user *x, const user *u);

#endif
