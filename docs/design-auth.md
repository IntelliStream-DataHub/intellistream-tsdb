# Login and access control for tsdbd

Status: design, not implemented. Branch `auth-design`, written on 2026-11-04 from main at 3bd4e96 and revised the
same day with the user's answers to its open questions (A9, A13, A23-A27). Code references are `file:line` at that commit.

Decided by the user on 2026-11-04 and not reopened here: this lands before the 0.9.0 tag, as protocol 12, with all
four clients, the Pulsar sink and the CLI in the same change; login is SCRAM-SHA-256 as PostgreSQL does it, and the
server stores verifiers only; no TLS yet (channel binding comes with it); tenants are name prefixes
([design-series64.md](design-series64.md) §2.3); permissions are grants of independent rights on one series, a
prefix, a glob or all series; embedded use (libtsdb and the embedded Rust, Python and Java bindings) has no login.
Answered on 2026-11-04: write is split into insert and delete; globs are in 0.9; login is required by default;
tenant admins are in 0.9; state names shared within a volume are accepted and documented.

## Summary

- **Login.** A client proves a password with SCRAM-SHA-256 (RFC 5802, RFC 7677) right after HELLO. The server
  keeps per user only a salt, an iteration count, StoredKey and ServerKey: whoever reads them cannot log in with
  them. Both sides prove themselves, so a client also knows it reached a server that knows its verifier.
- **Grants.** A user holds a list of grants `(selector, rights)`. A selector is one exact series name, a name
  prefix (a tenant is just a prefix, `acme/`), a glob (`acme/*/temp?`: `*` any run of bytes, `?` one byte) or
  all series. The rights are independent bits: `read`, `insert`, `delete` and `admin`; `write` is shorthand for
  insert and delete. Grants only add: a user's rights on a series are the union of every grant that matches its
  name. A **superuser** may
  do everything, including managing users and the database-wide operations.
- **Where users live.** A file `USERS` in the database root: append-only checksummed records, mode 0600, its own
  lock, so `tsdb <dir> user ...` works while tsdbd runs and tsdbd picks changes up within a second. The first
  superuser is added that way, offline.
- **Enforcement.** tsdbd checks every request against one table indexed by op number before it calls the engine;
  an op missing from the table is refused. Names are checked directly; ids through their names, with a per-user
  map of rights by id that the user's connections share. Measured: 0.3 to 1.3 ns per row of an insert, against
  about 48 ns per row that tsdbd spends on an insert today; one branch per request for a superuser or a grant on
  all series. Globs are matched only when the map misses (about 9 ns per glob tried, measured). A series the user
  may not touch and a series that does not exist answer the same.
- **Default.** tsdbd requires login and refuses to start without a superuser, unless started with `--no-auth`.
  Every quick start gains the `tsdb <dir> user add NAME --superuser` step. Test harnesses pass `--no-auth`, except
  the tests of this feature and one variant each of the integration matrix, jepsen and the benchmarks.
- **Before and after.** When the work is done, `tools/auth-bench.sh` compares main before auth (3bd4e96) with the
  auth build without login and with three kinds of user, inserts and queries, repeated; the results go into
  BENCHMARKS.md (§9).
- **No new C dependency.** SHA-256, HMAC and PBKDF2 are written in C (about 250 lines) and checked against the
  NIST and RFC vectors; Rust uses the RustCrypto `sha2` and `hmac` crates, Java `javax.crypto`, Python the Rust
  crate.

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| A1 | **SCRAM-SHA-256 with the PostgreSQL message flow**: HELLO, then `AUTH_START` (mechanism, client-first-message) and `AUTH_CONTINUE` (client-final-message); the server answers server-first and then server-final, or `TSDB_ERR_AUTH` (§3). The server stores `(salt, iterations, StoredKey, ServerKey)` only. | A password sent in clear and hashed by the server; an HMAC challenge over a shared secret (as the peer links of ha.md D35) | Decided. A shared secret stored by the server is as good as the password to whoever reads the file; a SCRAM verifier is not. SCRAM needs no TLS to keep the password off the wire. |
| A2 | **Login is part of protocol 12.** HELLO's reply says whether login is required and lists the mechanisms; the HELLO floor becomes 12. A client may send `AUTH_START` right behind HELLO without waiting, so a login costs one round trip more than today. | A separate login port; credentials inside HELLO | One port and one frame format keep every harness and firewall rule as it is. Credentials in HELLO would fix the mechanism before the server has said which it offers, which is what lets `SCRAM-SHA-256-PLUS` come later without a break. |
| A3 | **No channel binding now.** Clients send the gs2 header `n,,`. When TLS comes, the server adds `SCRAM-SHA-256-PLUS` to the HELLO list and clients on TLS choose it with `p=tls-server-end-point`; a client that could bind but was not offered it sends `y,,`, which a server that offers PLUS refuses (RFC 5802 §6 downgrade rule). | Designing TLS now | Decided: TLS is a later track. The mechanism list and the gs2 header are the only places channel binding touches, so it is a flag, not a protocol break. |
| A4 | **An unknown user looks like a wrong password.** For a name not in USERS the server answers with a mock salt, `HMAC(mock_key, name)` truncated to 16 bytes, the default iteration count and a fresh nonce, runs the same proof check against a mock StoredKey and fails with the same `TSDB_ERR_AUTH` and no body. `mock_key` is 32 random bytes in the first record of USERS, so the mock salt for a name is the same after a restart. Every AUTH_START computes the mock values and picks the real ones for a known user without a branch, so both kinds of name do the same work (step 2 review: the unknown name was 2 us slower and told). | Failing at once with NOTFOUND; a random salt per attempt | PostgreSQL does the same (`scram_mock_salt`). A random salt per attempt would tell unknown names apart by a salt that changes; a salt kept per name across restarts does not. |
| A5 | **Before login, only HELLO, `AUTH_START` and `AUTH_CONTINUE`, each at most 4 KiB, and the login done within `--login-timeout` (default 10 s).** Any other op is answered `TSDB_ERR_AUTH` and the connection closes; a frame announced larger than 4 KiB closes it at once, before any of it is buffered. A failed login closes the connection. No delay, no lockout. | A delay after a failure; locking an account after N failures; a cap on unauthenticated connections | The threat model is a backend on an isolated network (SECURITY.md, the user's stance): only major issues. A lockout is a lever anyone can pull to lock the Pulsar sink out. The 4 KiB limit and the timeout stop the one major issue: an anonymous client holding `--max-request-memory` with 256 MiB frames. |
| A6 | **Grants are `(selector, rights)`.** Selectors: `all`, `prefix:P`, `name:N`, `glob:G` (A24). Rights: `read`, `insert`, `delete`, `admin`, independent bits. Grants only add (no deny); a user's rights on a series are the union of the grants whose selector matches its name. | Roles per prefix (the brief's first version); deny grants; per-volume grants | The user refined the model to independent rights on series selectors (TASK 4b). Deny grants make "what may this user do" depend on order and precedence, and make the escalation rule (A9) hard to check. A volume is where data is stored, not whom it belongs to (series64 §2.3). |
| A7 | **A prefix is a byte prefix of the name**, nothing more: `prefix:acme` also matches `acmecorp/x`. `tsdb user grant` warns when a prefix does not end in `/`, the convention the docs use for tenants. | A path boundary rule (`acme` matches `acme/...` only) | Series names have no structure today, and some deployments use `.` or `:` as separators. A rule that knows `/` would impose one convention on all of them; a warning catches the usual mistake without that. |
| A8 | **A superuser** (a flag on the user) has every right on every series and is the only one who may run the database-wide ops (`FLUSH`, `OPTIMIZE`, `STATS`), create superusers and manage users outside a tenant. Volumes are still added only with the offline CLI (AGENTS.md "Volumes"). | Superuser without data rights (PostgreSQL-style split of `pg_read_all_data`) | One flag is simpler to reason about; an operator who wants an ingest account without delete rights creates a normal user for it. |
| A9 | **Tenant admins** (decided for 0.9 on 2026-11-04). `admin` on a prefix P lets a user create users whose names start with P, set the password of or drop a user named under P whose every grant lies inside the admin's own `admin` selectors, and grant or revoke rights on selectors inside its `admin` selectors to and from the users it manages. `admin` on a selector implies `read`, `insert` and `delete` on it. Nobody can give what they do not administer, and nobody can take over (set the password of) a user holding a right they could not have given. | Superuser-only user management; ownership recorded per user | The brief's admin role per tenant, restated in grants. The "every grant inside" rule closes the takeover path: a superuser that shares `name:shared/weather` with `acme/bob` moves bob's password out of acme's admins' reach. Caveat (review of step 1): the rule stops a *later* takeover only; an acme admin who created bob or set his password before the share still knows it and can log in as bob and read the shared series. So a cross-tenant share goes to a user whose password the tenant admins never set, or the superuser resets the password when sharing (operations docs and SECURITY.md, step 4). User names under the prefix give a tenant its own user namespace without another field. |
| A10 | **Users live in `USERS` in the database root**: append-only records with a length, type and checksum (xxh64, as the other append-only files), mode 0600, appended under an OFD write lock on the file (as VOLUMES, src/volumes.c:182-245). The CLI may append while tsdbd runs; tsdbd reads new records on its one-second tick and after each of its own appends. `tsdb <dir> user compact` rewrites the file (tmp, fsync, rename, directory fsync) under the lock; a reader that sees a new inode reads it again from the start. | A separate file named by a flag; a text file edited by hand; users inside META | A follower on another host reads the same file over NFS with no copy to keep in step, and the operator backs up one directory. Append-only records give atomic changes and a cut torn tail, which a hand-edited file does not. |
| A11 | **USERS errors are fail-stop.** A failed write or fdatasync of USERS (or of the root directory after creating it) stops tsdbd with status 1, like `eng_fail`; the change is not acknowledged. At open, damage follows the rules of the other files: a torn tail is cut, a bad record with a valid one after it fails the open with `TSDB_ERR_CORRUPT`. A reload while running that finds damage logs it, keeps the last good state and refuses admin ops until a restart. | Retrying the fsync; skipping a bad record | A revoke that was acknowledged must not come back after a power loss, and a retried fsync proves nothing (AGENTS.md "Fail-stop"). A skipped GRANT or DROP record changes who may read what without anyone noticing. |
| A12 | **The first superuser is added offline**: `tsdb <dir> user add NAME --superuser` (password from a prompt, `--password-file` or `TSDB_PASSWORD`; never argv). It works with or without tsdbd running. | An online "first login sets the password"; a bootstrap token printed at start | Whoever can write the database directory already holds every right; the file permissions are the trust boundary. An online bootstrap is a window in which the first client to connect becomes superuser. |
| A13 | **Login is required by default** (decided on 2026-11-04). tsdbd with no superuser in USERS refuses to start and names the command that adds one, unless started with `--no-auth`, which logs a warning line and lets every client do everything, as today. | Login only when USERS has users; off unless `--auth` | A missing or moved USERS file must not turn access control off without a word: fail closed. `--no-auth` keeps today's behaviour one flag away for tests and trusted loopback setups. |
| A14 | **One table, indexed by op number, says what each op needs** (§5): the rights, where its series are (a name, an id at an offset, an id per row, the ids of a QUERY_MULTI, none) and whether it is pre-login, any-login or superuser. `handle()` (server/tsdbd.c:1087) consults it before the switch; an op with no entry is refused with `TSDB_ERR_DENIED`. The op list becomes an X-macro in tsdb_protocol.h, so the table, the metrics names and the test of §9 are generated from one list. | A check written in each `case` | A new op that forgets its check is then denied, not open, and the test fails for it. |
| A15 | **Ids are checked through their names, with a per-user rights map** shared by all connections of the user: per volume, a radix table of one byte per local id (`0` unknown, else `0x80` with the rights), filled on first use from the name (`tsdb_series_name`, src/volumes.c:1327) and the grants. Grant changes to a user replace its map; other users keep theirs. Ids that resolve to no name (unknown, or not yet seen by a follower) answer DENIED and are not cached. A connection also remembers the previous row's id, so a run of rows of one series costs one compare. | A cache per connection (the brief's suggestion); resolving grants to id sets up front; ids scoped by tenant | Per connection, the Pulsar sink's many connections would each warm and hold their own copy of the same map. Resolving up front cannot follow prefix grants to series created later and would scan up to a billion names. Tenant-scoped ids were ruled out by the series64 decision. Names never change and ids are never reused, so a cached answer is stale only when the user's grants change. |
| A16 | **Denied and missing look alike.** A request naming a series by name is checked before the engine looks it up; a request naming an id the user holds no right on answers `TSDB_ERR_DENIED` whether the id exists or not. Only a user with the needed right on all series sees `TSDB_ERR_NOTFOUND`. New codes: `TSDB_ERR_DENIED = -16`, `TSDB_ERR_AUTH = -17` (-10 to -15 are taken by ha.md). | NOTFOUND for unknown, DENIED for foreign | Otherwise any user could list which names and ids exist in other tenants. |
| A17 | **QUERY_MULTI answers what it may.** Each denied series gets its own status `TSDB_ERR_DENIED` (as a malformed id gets INVALID today), the others are answered; the END frame keeps its meaning. | Refusing the whole request | A dashboard over series of mixed grants still draws what it may; QUERY_MULTI already has a status per series. |
| A18 | **Passwords never travel.** Creating a user and setting a password send a verifier the client computed (random salt, iterations, StoredKey, ServerKey), as PostgreSQL's `\password` does. The server checks its shape: salt 16 to 64 bytes, at least 4096 iterations. | The server computing the verifier from a password | Without TLS a password on the wire is readable; a verifier sniffed in transit gives no more than a sniffed login exchange (an offline guessing target). |
| A19 | **Crypto without new C dependencies**: SHA-256, HMAC-SHA-256, PBKDF2-HMAC-SHA-256, base64 and a constant-time compare in `client/tsdb_crypto.c` (about 250 lines), shared by tsdbd, the C client and the CLI; nonces from `getrandom(2)`. Rust: RustCrypto `sha2` and `hmac`, PBKDF2 as a ten-line loop, nonces through `libc::getrandom` (libc is already a dependency). Java: `MessageDigest`, `Mac("HmacSHA256")`, `SecureRandom`, PBKDF2 as the same loop over UTF-8 bytes. Python: the Rust crate. | Linking libcrypto | The server only computes a few HMACs per login, so speed does not matter there; the client's PBKDF2 runs once per process (§7). libcrypto's soname differs between distributions (1.1, 3), which the portable tarball cannot follow (tools/check-glibc.sh), and the client library would gain a dependency it now lacks. |
| A20 | **Passwords are bytes**: the UTF-8 of the string, without SASLprep. | SASLprep (RFC 4013) | SASLprep needs Unicode mapping tables in four languages for passwords outside ASCII only; every client here encodes the same way, so they agree. The docs recommend ASCII passwords. |
| A21 | **The metrics port stays unauthenticated**; it gains login counters and nothing per user. Logs name the user and the peer address of a failed login (at most one line a second), never a password, proof, verifier or nonce. | Login on the metrics port | Prometheus scrapes without SCRAM. The metrics are server-wide counts and gauges (by op, error code, role), no series names or values (docs/metrics.md); bind it to a trusted interface (`--metrics-bind`). |
| A22 | **One branch, merged once.** The build steps of §11 are reviewed one by one on a branch `auth` and merged to main together. | Merging the server and C client first | AGENTS.md: a protocol change reaches all four clients in the same commit to main. |
| A23 | **Write is split into `insert` and `delete`** (decided on 2026-11-04); `write` is shorthand for both, and `REPLACE_RANGE` needs both. | One `write` bit | An ingest credential (the Pulsar sink, a collector) is the one most likely to leak; with the split it cannot erase history or shorten retention. |
| A24 | **Globs are in 0.9** (decided on 2026-11-04, against the first recommendation) with the smallest syntax that is still useful: `*` matches any run of bytes (also none, also `/`), `?` exactly one byte, `\` makes the next byte literal (a glob ending in a lone `\` is refused); nothing else is special. At most 256 bytes, at most 64 glob grants per user. A glob without a wildcard is stored as `name:`, a literal followed by one final `*` as `prefix:`, so each set has one form. Matching is a bit-parallel run of the glob compiled once per grant (one state bit per token, at most 5 words): linear in the name, whatever the glob's shape, no recursion, no allocation (step 2 review: the greedy matcher first chosen backtracked to the last `*`, and 64 globs against a 32,767-byte name cost 288 ms under the user's lock; now 7 ms, outside any lock). Names over the engine's 32,767 bytes are refused before any matching. | Character classes `[a-z]`; gitignore-style `*` within a segment and `**` across segments; regular expressions | Names have no fixed separator (A7): `*` versus `**` needs one, and would quietly treat `.` or `:` names differently from `/` names. Classes and regular expressions add parsing, escaping rules in four clients and a CLI, and patterns whose cost depends on their shape; a byte prefix, `*` and `?` cover "every site's temperature" and "these line numbers". The caps bound the work of one rights-map miss. |
| A25 | **Inside-rules for globs are conservative.** The literal prefix L of a glob is its bytes before the first wildcard; every name it matches starts with L. Glob S is inside `all`; inside `prefix:P` when L starts with P; inside glob T only when S equals T byte for byte. `name:N` is inside glob T when T matches N. `prefix:P` is never inside a glob. | Deciding inclusion between two globs; prefixes inside globs such as `a?c*` | Each rule is sound and one line of code. Whether one glob's set lies inside another's is a language-inclusion problem; a prefix is an infinite set, and the cases where a glob covers all of it are rare enough not to be worth a proof. An admin holding only glob `admin` grants can therefore pass on that glob and single names, not prefixes, and cannot create users (that needs `admin` on a prefix or `all`). |
| A26 | **State names are shared within a volume, accepted for 0.9** (decided on 2026-11-04): SECURITY.md says so and the operations docs advise a volume per tenant where state labels are sensitive. | Per-series state dictionaries | They would change the disk format and the STATE ops before the release, for labels such as `RUNNING`. |
| A27 | **A before-and-after benchmark closes the work** (the user's request): `tools/auth-bench.sh` runs main at 3bd4e96 against the auth build with `--no-auth` and with login, on a quiet machine, repeated, and its results go into BENCHMARKS.md (§9). | Only the perf gate of step 2 | The gate catches a regression during the work; the user wants a number to publish, rerunnable on other machines. |
| A28 | **`--no-auth` refuses the user admin ops** (decided on 2026-11-04, step 2). Without login, connections need no login and every data op runs unchecked, but USER_CREATE, USER_DROP, USER_ALTER, USER_PASSWORD, GRANT, REVOKE and USER_LIST are answered TSDB_ERR_DENIED (a log line at most once a second); USERS is not opened. Users are managed with the offline `tsdb <dir> user` verbs. WHOAMI answers an empty name with the superuser flag. | Admin ops act as the operator (actor 0) on USERS | A server run without login for a while must not let any network client plant a superuser in USERS that stays valid once login is on (fail closed, as A13). |
| A29 | **A reload that shows no superuser keeps serving** (step 2, the question step 1 left). Logged-in users keep their grants, admin ops follow users.c's rules (only a superuser could create one again, so none can until the CLI adds one), one log line per change into that state; the next start refuses (A13). | Stop the server | A CLI slip (dropping or demoting the last superuser) must not take a production server down; nothing is opened that was closed, and the restart fails closed. |

## 1. Threat model and scope

tsdbd runs as a backend on an isolated network (SECURITY.md; the production network is NFS over RDMA, no public
exposure). Login is there so that one leaked or misused credential is limited to what it was granted, so tenants
cannot read or delete each other's series, and so an ingest account cannot delete history. It does not protect
against: someone who can read the network (no TLS: they see queries and values, and a sniffed login is an offline
guessing target), someone with access to the database directory or the host (they are superuser in effect), and
denial of service by an authenticated client. Each of these is written into SECURITY.md.

Embedded use opens the files directly and has no login (decided). `tsdb <dir> ...` commands are embedded use too:
the file permissions decide who may run them.

## 2. Users, grants and rights

**A user** has a name, a number, a flag `superuser`, one verifier and a list of grants. Names are 1 to 128 bytes of
`A-Z a-z 0-9 . _ - @ / : +`, not starting with `/`: the set needs no escaping inside SCRAM's `n=` attribute (which
reserves `,` and `=`) and allows tenant prefixes (`acme/ingest`). The number is assigned at creation and never
reused, so a user dropped and created again under the same name does not inherit grants or open sessions.

**Selectors**

| selector | matches the series named S when |
|---|---|
| `all` | always |
| `prefix:P` | the first `len(P)` bytes of S are P |
| `name:N` | S equals N |
| `glob:G` | G matches all of S: `*` any run of bytes, `?` one byte, `\x` the byte x (A24) |

**Rights**

| right | allows (§5 has the full table) |
|---|---|
| `read` | queries of every kind, `LAST_STATS`, looking the series up, its name, type, retention and state names |
| `insert` | everything `read`'s lookups allow, plus inserts, creating the series, adding state names |
| `delete` | `DELETE_RANGE`, `SET_RETENTION`; `REPLACE_RANGE` needs `insert` and `delete` |
| `admin` | `read`, `insert` and `delete`, plus managing users under it (A9) |

`write` is accepted everywhere as shorthand for `insert,delete`. A user with only `insert` (the Pulsar sink, user
bar of the brief) may look up and create series and insert into them, but not read them back; a user with only
`read` on a few `name:` grants (user foo) may query exactly those.

**Matching** (`rights_of(user, name)`): the OR of the user's `all` bits, the bits of an exact grant found in a hash
table of names, and, for each distinct length L among the user's prefixes (at most a few in practice), the bits of
a prefix grant found by looking up the first L bytes in a second hash table, and the bits of every glob that
matches. Globs are kept in the same way by their literal prefix (bytes before the first wildcard): for each
distinct literal-prefix length, one probe finds the globs whose literal prefix the name starts with, and only those
are run, after a check of their literal suffix (bytes after the last wildcard). Thousands of `name:` grants cost
one hash probe; a hundred prefixes of ten different lengths cost ten; a glob costs about 9 ns when it is run
(measured: 64 globs of the shape `acme/site07/*/temp?` tried in full against 30-byte names, 579 ns a name; an
adversarial `*a*a*a*a*a*a*a*a*b` against a 1000-byte name, 0.7 µs).

**Who may grant what** (A9, A25). Selector S is *inside* T when:

| S \ T | `all` | `prefix:P` | `glob:G` | `name:N` |
|---|---|---|---|---|
| `all` | yes | no | no | no |
| `prefix:Q` | yes | Q starts with P | never | no |
| `glob:H` (literal prefix L) | yes | L starts with P | H equals G | no |
| `name:M` | yes | M starts with P | G matches M | M equals N |

Glob forms are normalized before the comparison (A24), so `glob:acme/*` is compared as `prefix:acme/`. User X *manages* user U when X is a superuser, or U is
not a superuser, U's name starts with a prefix on which X holds `admin` (or X holds `admin` on `all`), and every
grant of U is inside one of X's `admin` selectors. A glob `admin` grant never makes X manage anyone: user names are
created and managed under prefixes only. Then:

| op | allowed when |
|---|---|
| create user U | X is superuser; or U's name starts with a prefix X administers. Only superusers create superusers |
| drop U, set U's password | X manages U; anyone may set their own password |
| grant rights R on S to U | X manages U and S is inside one of X's `admin` selectors |
| revoke rights R on S from U | S is inside one of X's `admin` selectors (any U: a tenant may cut a share of its own data). Revoking `read`, `insert` or `delete` from a grant holding `admin` revokes `admin` too and keeps the other implied rights (A23: cutting an ingest credential's `delete` must take effect); revoking `admin` takes the rights it implied with it, keeping those granted as such. A revoke of rights U holds none of on S answers `TSDB_ERR_NOTFOUND`, and the CLI exits non-zero; to an X that could not create U's name, `TSDB_ERR_DENIED`, as for a name that does not exist |
| set or clear `superuser` | X is superuser; the last superuser cannot be dropped or demoted online |
| list users | superuser: all; others: the users they manage, and themselves |

Cross-tenant shares (`acme/bob` reads `name:weather/oslo`) are made by a superuser. They move bob's password out of
acme's admins' reach, which is the point: whoever can set bob's password can read Oslo's weather. It does not
take back a password they already know (they created bob, or set his password before the share): share to a user
whose password the tenant admins never set, or reset it as superuser when sharing.

## 3. Login on protocol 12

Frames (new ops 29 to 38; payloads little endian like the rest of tsdb_protocol.h):

```
HELLO          u32 version                         -> u32 version, u16 flags, u16 n, n x (u8 len, mechanism)
                                                      flags: TSDB_HELLO_AUTH (login required)
                                                      mechanisms today: "SCRAM-SHA-256"
AUTH_START     u8 len, mechanism, u16 len, client-first-message   -> u16 len, server-first-message
AUTH_CONTINUE  u16 len, client-final-message                      -> u16 len, server-final-message
WHOAMI                                             -> u16 len, user, u8 flags, u32 n, n x grant
USER_CREATE    u16 len, name, u8 flags, verifier   -> (nothing)
USER_DROP      u16 len, name                       -> (nothing)
USER_ALTER     u16 len, name, u8 flags             -> (nothing)            (superuser flag)
USER_PASSWORD  u16 len, name, verifier             -> (nothing)
GRANT, REVOKE  u16 len, name, u8 kind, u8 rights, u16 len, selector -> (nothing)
USER_LIST                                          -> chunks: u8 last, u32 n, n x (user, flags, grants)

verifier: u32 iterations, u8 salt_len, salt, u8 stored_key[32], u8 server_key[32]
grant:    u8 kind (0 all, 1 prefix, 2 name, 3 glob), u8 rights (1 read, 2 insert, 4 delete, 8 admin), u16 len, selector
```

The exchange is RFC 5802's, with the user in `n=`:

```
client-first   n,,n=acme/ingest,r=<client nonce: 24 base64 chars from 18 random bytes>
server-first   r=<client nonce><server nonce>,s=<base64 salt>,i=4096
client-final   c=biws,r=<both nonces>,p=<base64 ClientProof>
server-final   v=<base64 ServerSignature>
```

- The server parses strictly: an `a=` (authorization identity), an extension attribute, a gs2 header other than
  `n,,` (until TLS), a nonce that does not continue the client's, or anything malformed fails the login with
  `TSDB_ERR_AUTH` and no body, exactly as a wrong proof does. The proof is compared in constant time.
- The client checks ServerSignature before it sends anything else and fails the connect with `TSDB_ERR_AUTH` if it
  does not match: the server proved it holds this user's verifier.
- With `--no-auth`, HELLO's flags say so and clients skip the exchange; a client given credentials logs in anyway
  if the server offers it, and a client that requires login (an option, default off) refuses a server that does not.
- `AUTH_*` after login, or a second `AUTH_START`, is `TSDB_ERR_INVALID`; a connection logs in once.
- In parse() (server/tsdbd.c:1841) a connection that has not logged in accepts only the three pre-login ops of at
  most 4 KiB, holds at most two frames queued (frames already received past them are parsed as each queued one is
  dispatched, so a third pipelined frame is answered: step 2 review), and stops reading after `AUTH_CONTINUE` until
  the worker has set the outcome; another op is queued with `eof` set, as an old client's HELLO is today (server/tsdbd.c:1921), and the
  worker answers it `TSDB_ERR_AUTH`. `CANCEL` before login closes the connection. The once-a-second tick that
  enforces `--frame-timeout` (server/tsdbd.c:1993) also closes connections not logged in after `--login-timeout`.
- The SCRAM steps run on a worker like any request; the server's part is a hash lookup and four HMAC-SHA-256 of
  under 200 bytes, a few microseconds.

## 4. The USERS file

```
USERS: records of  u32 len, u8 type, payload, zeros to a multiple of 8, u64 check
       check = xxh64 of everything before it in the record, seeded with the record's file offset
       type bit 0x80 (MORE): the change goes on in the next record (USER then VERIFIER of one `user add`)

HEADER    u32 format (1), u8 mock_key[32], u8 file_id[16]  first record, written when the file is created
                                                           and by a compaction (new random file_id, same mock_key)
USER      u32 uid, u8 flags, u16 len, name                 creates a user (uid above every earlier one)
VERIFIER  u32 uid, verifier                                replaces the user's verifier
FLAGS     u32 uid, u8 flags                                superuser on or off
GRANT     u32 uid, u8 kind, u8 rights, u16 len, selector   sets the user's rights on that selector (0 removes it)
DROP      u32 uid                                          the user and its grants are gone
```

- GRANT kinds are those of the wire (0 all, 1 prefix, 2 name, 3 glob); a glob is stored normalized (A24), and a
  grant over the caps (256 bytes, 64 globs per user) is refused with `TSDB_ERR_INVALID` before anything is written.
- A GRANT record holds the resulting rights, not a difference, so replaying the file twice gives the same state.
- Records are 8-byte aligned, so a damaged record is told from a torn tail by a valid record after it (as SERIES);
  pad bytes must be zero. A change of several records counts only when its last one is there (MORE), so a torn
  `user add` never leaves a user without its password.
- **Appending** (tsdbd's admin ops and the CLI): take the OFD write lock, read any records added since the last
  read, check the change against that state (A9), append, fdatasync (and fsync the root directory the first time
  this process changes that file, by its file_id: a writer that died before its directory fsync left a name a
  power loss can take back), apply in memory, release, reply. Failure of the write or the sync is fail-stop (A11),
  and the failed records are cut off again before the lock goes (after a failed writeback their pages are clean but
  maybe not on the disk; a retry must not read them as made). An answer that changes nothing (a grant held, flags as
  they are, NOTFOUND) is given only after an fdatasync. A compaction locks its new file before the rename and keeps
  it locked until the directory fsync. Mode 0600, owner and group those of the database directory: a writer runs as
  the directory's owner or as root (who gives the file to that owner); a USERS readable by others is set to 0600 at
  open, or refused when that fails.
- **Reading** at open: all records; a torn tail is cut by the next writer under the lock; damage fails the open.
  tsdbd keeps `(inode, offset)` and the HEADER's file_id: on its tick it `fstat`s the path and reads what was
  appended, or the whole file when the inode changed (a compaction), and re-reads the HEADER and the last record it
  read: another file_id or another last record is another file (a reader that closes the file between ticks can
  meet a later compaction's file under the same inode number and at the same size; ext4 and NFS reuse them). A
  partial record at the end is not yet complete: read it next time.
- **Followers** (`--follow`) and `--read-only` servers read USERS the same way and refuse admin ops with
  `TSDB_ERR_READONLY`. Over NFS the OFD lock is a server lock (ha.md D2), so the CLI on another host appends
  safely. A revoke made on the writer reaches a follower within its tick plus the NFS attribute cache (`actimeo`).
- **Size**: about 60 to 150 bytes a record. Ten thousand users with a hundred grants each is about 100 MB to read at
  open; `tsdb <dir> user compact` rewrites the file with one record per live fact.
- **Export and import** (`tsdb export`) carry series, not users; USERS is copied with the directory or recreated.
  It is a new root file, so the disk format version does not change: a database without it opens as before.
- **HA later** (ha.md): the file lives in the database root, which every node reaches. An entry node that forwards
  a request (D29) checks it with the client's grants first and forwards it over the authenticated peer link (D35)
  with the user's number, which the owner checks again; the details belong to that track.

## 5. Enforcement

**Locks (step 2 review).** USERS is read and written under one mutex that may be held across its I/O (the reload's
open and read, an admin op's append and fdatasync: on NFS, as long as a server is away). Logins and requests never
take it: after each change its holder publishes a snapshot of the users (by uid and by name, with the mock key),
which AUTH_START, AUTH_CONTINUE and a stale connection read under a leaf lock never held across I/O. A hung USERS
delays admin ops and the next reload only. A user's state carries its rights_of index built before it is shared,
so a name check or a map miss reads it without a lock: one user's slow check cannot park its other requests. A
failed login's log line is rate-limited with an atomic.

**When the rights change.** A connection holds a reference to its user's state (verifier, grants, rights map). A
change to the user marks that state stale and installs a new one. Each request starts with one relaxed atomic load
of the stale flag; a stale connection takes the new state, or, if the user was dropped, is answered `TSDB_ERR_AUTH`
and closed. A request already running finishes under the rights it started with. A password change does not end
existing sessions (as in PostgreSQL); dropping the user does.

**The table.** Every op, what it needs, and where the series are:

| op | needs | series |
|---|---|---|
| HELLO, AUTH_START, AUTH_CONTINUE | nothing (before login only) | |
| CANCEL, LAST_STATS, WHOAMI | a login | (own connection only, as today) |
| SERIES, create 0 | any right | name |
| SERIES, create 1; SERIES_CREATE | insert | name (any volume the operator added) |
| SERIES_NAME, SERIES_TYPE, GET_RETENTION | any right | id |
| STATE, create 0; STATE_NAME | any right | id (picks the volume's dictionary) |
| STATE, create 1 | insert | id |
| INSERT, INSERT_VALUES (plain and packed) | insert | id, or an id per row (packed: after unpacking); one id without the right refuses the whole frame (inserts are all or nothing) |
| QUERY_RAW, QUERY_VALUES, QUERY_LTTB, QUERY_BUCKETS, QUERY_BOUNDS, QUERY_TWA | read | id |
| QUERY_MULTI | read | each of its ids; DENIED per series (A17) |
| DELETE_RANGE, SET_RETENTION | delete | id |
| REPLACE_RANGE | insert and delete | id |
| COMMIT | insert or delete on some selector | (durability of what was written) |
| FLUSH, OPTIMIZE | superuser | (database-wide work) |
| STATS | superuser, or read on `all` | (totals over every tenant) |
| USER_*, GRANT, REVOKE, USER_LIST | per §2 | user names and selectors |

There is no op that lists series today; one added later filters by `read` or `insert` and goes in the table like
the rest. A user with no grants can log in, ask WHOAMI and change its own password, nothing else.

**State names are per volume** (one STATES dictionary per volume, not per series). A user with a right on any series
of a volume can therefore look up the state names other tenants added in that volume (`STATE_NAME`). Tenants who
must not see each other's state labels get their own volumes (A26: accepted for 0.9, documented in SECURITY.md).

**Hot path.** For each id the check is, in order:

1. the user holds the needed right on `all` (superusers too): nothing more, one branch per request;
2. the id equals the previous row's: same answer;
3. the rights map: `volume -> 512 pointers -> 1024 pointers -> 4096 bytes`, allocated on first use, the leaf byte
   loaded and stored with relaxed atomics (every writer stores the same value); a hit costs a few loads;
4. a miss: `tsdb_series_name` (a route and an array index), then `rights_of` (§2, globs included), then store
   the byte. Globs therefore cost nothing on a hit: a series' answer is computed once per user.

Measured on the Ryzen 9 7950X (scratch microbenchmark of steps 2 and 3, 10,000-row frames): 0.3 ns a row when rows
come in runs of 100 per series, 0.8 ns a row with ids random over 1,000 series, 1.3 ns a row with ids random over
1 million series (a 1 MB map, in L2). tsdbd loads 82.6 M rows/s over 4 connections, about 48 ns a row per
connection, so the check costs at most about 3% and usually under 1%; queries check one id per request. A miss
costs an estimated 100 to 300 ns, once per id per user until its grants change: a first QUERY_MULTI over 65,536
fresh ids pays about 13 ms once. Memory: 4 KB per 4096-id block a user touches plus 12 KB per volume, allocated
only for users with connections; a user with a grant on `all` has no map. A cap per user (64 MB) stops filling
the map past it; checks then compute every time, which is slower, never wrong.

## 6. Clients, CLI and the Pulsar sink

- **C**: `tsdbc_connect_auth(host, port, user, password, &c)` and `tsdbc_options` with `user`, `password`,
  `password_file` and `require_auth`; `tsdbc_connect` without a user works only with a `--no-auth` server.
  `tsdbc_whoami`, `tsdbc_user_create`, `_drop`, `_alter`, `_password`, `tsdbc_grant`, `tsdbc_revoke`,
  `tsdbc_user_list`. The password stays in memory for reconnects; the client caches `ClientKey` and `ServerKey` per
  `(user, salt, iterations)`, so PBKDF2 runs once per process, not per connection.
- **Rust, Python, Java**: the same options and calls under their names (`ConnectOptions::user`, `Client(...,
  user=, password=)`, `Client.connect(Options...)`); Rust's and Java's pools and Python's through Rust share one
  key cache. The embedded bindings (`Db`, `EmbeddedDb`) do not change.
- **CLI**: offline, `tsdb <dir> user add|drop|passwd|grant|revoke|list|compact`, working on USERS only (the engine is
  not opened, so it runs beside tsdbd). Online, the same verbs with `--server HOST:PORT --login NAME` for tenant
  admins without shell access, through the C client. Passwords come from a prompt without echo, `--password-file`
  or `TSDB_PASSWORD`, never from argv (visible in `ps`). `passwd` computes the verifier locally (A18).
- **Pulsar sink**: `user` and `password` in TsdbSinkConfig, `password` declared `sensitive = true` and loaded with
  `IOConfigUtils.loadWithSecrets` (pulsar-io-common), so it comes from Pulsar's secrets provider, never from the plain config;
  `passwordFile` as an alternative. `TSDB_ERR_AUTH` at connect is retried with the sink's backoff and logged once per
  backoff step (Pulsar buffers meanwhile, as for UNAVAILABLE); `TSDB_ERR_DENIED` on a record is permanent and goes
  to the dead-letter path (ErrorClassifier).
- **tsdbc-bench, scale-bench**: `--user`/`--password-file`, so the benchmarks run with login (§9).

## 7. Costs

| what | cost |
|---|---|
| a login, client side | PBKDF2 with 4096 iterations: 0.79 ms measured with OpenSSL (SHA-NI) on the 7950X; the portable C version is estimated at 2 to 4 ms; once per process with the key cache; then four HMACs |
| a login, server side | a hash lookup and four HMACs: microseconds |
| a login, network | HELLO and AUTH_START pipelined, then AUTH_CONTINUE: one round trip more than today |
| a request | one relaxed load of the stale flag |
| an insert row | 0 (grant on `all`), 0.3 to 1.3 ns measured (map), about 200 ns once per new id, plus about 9 ns per glob run on that miss |
| memory | about 4 KB per 4096 ids touched per user with connections; USERS in memory about its size on disk |

The perf gate measures the end-to-end numbers: tsdbc-bench insert and query with a prefix-grant user against
`--no-auth`, on a quiet machine, repeated (benchmark policy). Expected within 3%.

## 8. Turning it on

- Default: login required (A13). `tsdbd` refuses to start without a superuser in USERS, with the message
  `no superuser: run 'tsdb <dir> user add NAME --superuser', or start with --no-auth`.
- `--no-auth`: today's behaviour for data ops, a warning line at start, the HELLO flag clear; the user admin ops
  are refused (A28).
- `--login-timeout S` (default 10). The banner says whether login is on and how many users there are.
- The `There is no authentication` paragraph of tsdbd's header (server/tsdbd.c:77) and its counterpart in the
  memory notes (server/tsdbd.c:74-75) change accordingly.

Harnesses that start tsdbd today pass `--no-auth`: `tests/harness.h`, `tests/integration/run.py` (`Server`), the
client test helpers under `client/*/tests`, `connectors/pulsar` tests, `tests/jepsen`, the fuzzer, `tools/bench-tsdb.sh`
and the benchmarks. Each gets a login variant (§9), with a USERS file made by the CLI in the scratch directory.

## 9. Tests

- **Crypto vectors** (`tests/test_crypto.c`): SHA-256 from FIPS 180-4 (`abc`, the 448-bit message, a million `a`),
  HMAC-SHA-256 RFC 4231 cases 1 to 7, PBKDF2-HMAC-SHA-256 from RFC 7914 §11, base64 RFC 4648 §10.
- **The RFC 7677 §3 exchange** (user `user`, password `pencil`) with a fixed nonce, in tsdbd's SCRAM code and in each
  of the four clients, byte for byte.
- **Login** (`tests/test_auth.c`, a server test on uring and epoll): right password; wrong password; unknown user
  (the server-first of an unknown name has the same shape and the same salt on two tries and across a restart, and
  the failure is the same bytes as a wrong password's); malformed messages; `a=`, `y,,`, `p=`; pre-login op;
  pre-login frame over 4 KiB; login timeout; AUTH after login; a dropped user's open connection; a revoke seen by
  the next request; CLI edits seen within the tick; a follower reading USERS.
- **Every op across tenants** (`tests/test_auth_ops.c`): generated from the X-macro op list. For each op, a minimal
  well-formed frame from a user with all rights on `acme/` naming a series of `globex/` (by name and by id), and
  from users holding each single right, against the table of §5; an unknown op number and an op added to the list
  without a table entry are denied. Plus QUERY_MULTI with mixed series, a packed and a per-row insert with one
  foreign id among thousands (the whole frame is refused), and the escalation cases of §2 (grant outside one's
  admin selector, password of a user with an outside grant, the last superuser).
- **USERS file**: torn tail, bad record before a good one, concurrent appends from the CLI and tsdbd, compaction
  while tsdbd reads, fail-stop on EIO and ENOSPC (`test_failstop` style), power loss with crashfs (an acknowledged
  revoke survives).
- **Fuzz**: `tests/fuzz/frames` gains pre-login corpora (HELLO, AUTH_START, AUTH_CONTINUE sequences) and the
  fuzzer logs in before mutating post-login frames, so both sides are covered.
- **Integration matrix**: `--auth on` as a matrix dimension for the C, Rust, Python and Java clients and the CLI;
  a check that a client can read another tenant's data fails the run (`mutate.py` gets a mutation that skips the
  check in `handle()`, which the matrix must catch).
- **Jepsen**: workload `kv` with login on under kill -9 and power loss, so restart, USERS reload and reconnect with
  login are in the histories.
- **Globs** (`tests/test_auth.c`, a table): `*` empty, in the middle, several, at both ends; `?` against one byte
  and none; `\*`, `\?`, `\\` and a trailing `\`; `/`, `.`, `:` and bytes above 127 as ordinary bytes; a name of
  65,535 bytes; the normalization to `name:` and `prefix:`; the caps (257 bytes, the 65th glob grant). Escalation
  with globs: every cell of the inside table of §2 for and against, including a glob admin granting a prefix
  (refused), a different glob of the same literal prefix (refused) and a name it matches (allowed). The rights map
  with globs: a miss that only a glob answers, then the hit; a grant change that removes the glob.
- **Every op across tenants** runs once more with the user's rights given by a glob (`glob:acme/*`-style grants
  normalize to prefixes, so the test uses `acme/?*/x*` shapes), and the integration matrix's login dimension has
  a glob-grant user.
- **Perf gate** (step 2): tsdbc-bench with and without login, as §7.
- **Before and after** (A27, step 5): `tools/auth-bench.sh [--reps N] [--quick] [--dir SCRATCH] [--out DIR]`
  builds main at 3bd4e96 in a worktree of its own and the auth build, both optimized (as `tools/bench-tsdb.sh`
  builds, never debug or sanitizer builds), loads the same data, and runs tsdbc-bench against each tsdbd (started
  with `--port 0 --port-file`) in five configurations: main; auth with `--no-auth`; auth with a user holding
  `prefix:` grants; a user holding 5,000 `name:` grants (every series the benchmark touches among them); a user
  holding 64 `glob:` grants of which one matches. For each: inserts (one series per request, and a series per row
  over 1,000 series), queries of one series (raw rows and buckets), and QUERY_MULTI over 100 series, on 1 and 32
  connections. Each number is run `--reps` times (5) and reported as mean, standard deviation and lowest-highest,
  with the machine (CPU, governor, commit, compiler) and a warning when the machine is not quiet; the summary uses
  `tools/bench-summary.py`. The results go into BENCHMARKS.md, in a section "Login and access control", with the
  command that made them.

## 10. Docs to update

The quick starts, which must now add a superuser before tsdbd starts: README.md "Quick start (5 minutes, one
machine)" (tests/test_quickstart.sh runs that block word for word, so the test changes with it),
site/docs/quickstart.html (its blocks are taken from the README), dist/README.dist, and dist/tsdbd.service (a
comment that tsdbd refuses to start until `tsdb <dir> user add NAME --superuser` has run). BENCHMARKS.md (the
before-and-after section, A27). SECURITY.md (what login covers and what it does not: no TLS, the metrics port,
state names per volume with the advice of a volume per tenant, directory access), docs/design.md (the server section near line 1584), docs/roadmap.md (row "Authentication, TLS, access
control": TLS remains), CHANGELOG.md, tsdb_protocol.h's history comment (version 12), docs/metrics.md (login
counters), the client READMEs, the Pulsar sink README, site/docs (server.html, clients.html, operations.html,
cli.html), and AGENTS.md: an invariant bullet (every op through the table; ids through names; denied equals
missing; USERS fail-stop), the protocol history, the layout (`client/tsdb_crypto.c`, `server/users.c`,
`server/auth.c`), the lock order (user ops take no engine lock; the USERS OFD lock and the auth state's mutex are
leaves), and `--no-auth` in the Ports/harness paragraph.

## 11. Build plan

Each step is a commit on the branch `auth`, built, reviewed adversarially and fixed as a track (tsdb-tracks), and
the branch merges to main once, after step 5 (A22), so BENCHMARKS.md arrives with the change it measures.

| step | content | gates |
|---|---|---|
| 1 | `client/tsdb_crypto.c` with vectors; `server/users.c` (USERS format, lock, load, append, compact), the grant matcher with globs and the inside-rules, and the CLI's offline `user` verbs | `make test`, `make asan`, `make tsan`; the crashfs power-loss test of USERS (it is a durable file: a short `make jepsen` run on kv with a USERS change in the setup) |
| 2 | Protocol 12 in tsdbd: HELLO, SCRAM, pre-login limits, the op table and its generated test, the rights map, admin ops, `--no-auth`, `--login-timeout`; the C client and the CLI's `--server` verbs; `tests/harness.h` and the C server tests | the above + `make fuzz-frames FUZZ_SECONDS=600` with the new corpora; tsdbc-bench with and without login |
| 3 | Rust (and with it Python) and Java clients: login, key cache, admin calls, the RFC 7677 test, their test helpers | `make test-clients`, `make test-rust`, `test-python`, `test-java` |
| 4 | Pulsar sink (config, secrets, error classes), `tests/integration/run.py` login dimension and mutation, jepsen login variant, bench scripts, all docs of §10 | `mvn test` in connectors/pulsar and `connectors/pulsar/test/integration.sh`; `make integration-tests`; `make jepsen`; `make test-clients`; the perf gate on a quiet machine; docs checked against the code; `tests/test_quickstart.sh` |
| 5 | `tools/auth-bench.sh` and its run: main at 3bd4e96 against the finished branch (A27), results into BENCHMARKS.md | `--quick` runs to the end; the full run on a quiet machine (no builds or tests alongside, no ClickHouse container), 5 repetitions; the numbers checked against the logs |

Steps 1 and 2 touch server/tsdbd.c near `handle()` and parse(), where the lww-fix track adds its `worker_waits`
hook; whichever lands second rebases.

Left by step 1 for step 2:
- The CLI may drop or demote the last superuser offline (with a warning; tsdbd then refuses to start, A13). Step 2
  decides what a *running* tsdbd does when a reload shows no superuser (keep serving logged-in sessions and refuse
  new admin ops, or stop), and tests it.
- A `users` is not thread-safe: `rights_of` and the manage checks build each user's index on first use, writing
  through a pointer reached from shared state. tsdbd holds one mutex around every `users` call (reload, admin ops,
  rights lookups at login), or builds every index at load and treats the state as read-only between reloads.
  Lookups by name are hashed and the manage checks indexed (step 1), so the mutex is held for microseconds.

## Open questions

None. The five questions of the first version were answered on 2026-11-04 and are decisions now:

| question | answer | decision |
|---|---|---|
| Split write into insert and delete? | yes, as designed | A23 |
| Glob selectors? | yes, in 0.9 | A24, A25 |
| Login required by default, `--no-auth` to turn it off? | yes | A13 |
| Tenant admins in 0.9? | yes, as designed | A9 |
| State names visible across tenants sharing a volume? | accept for 0.9, document it | A26 |
