# Configuration files

`tsdbd` and the `tsdb` command line tool can read their settings from a `.properties` file. The command line keeps
working as before; a file is a way to write down what a server runs with, in one place a person (or a configuration
management tool) can read, review and keep in version control. The embedded library takes its options from code and
reads no file.

```properties
# /etc/tsdb/tsdbd.properties
storage.dir = /var/lib/tsdb
storage.verify.checksums = true

server.listen = 127.0.0.1
server.port = 7070
server.io = epoll
server.send.timeout = 30s

query.threads = 32
metrics.port = 9100

# lists are comma-separated
health.domains = /mnt/nfs1, /mnt/nfs2
```

`tsdbd --config FILE` reads that file. Without `--config`, `tsdbd` reads `/etc/tsdb/tsdbd.properties` when it exists;
`--no-config` reads nothing. A server started with `--exit-with-parent` (the flag of the test harnesses) does not read
that default file either: a test's server must not take the port, the login or the fsync setting of the service that
is installed on the same machine; it reads a file only when `--config` names one. A default file that exists but
cannot be looked at (a directory without the right to enter it) is an error, not "no file". (The tests put the default
file elsewhere with the environment variable `TSDBD_DEFAULT_CONFIG`; it names a path, never a setting.)
`tsdbd --check-config` checks the file and the command line, prints every setting
with where its value came from, and exits (status 0 when all is well, 2 when not): run it before a restart.
`tsdbd --config-keys` prints the tables below.

## The file

The syntax is that of Java's `.properties` files, a little stricter where Java would hide a mistake:

- `key = value`, `key: value` or `key value`; white space around the separator and at the start of a line is skipped.
- Lines that start with `#` or `!` are comments. Blank lines are skipped. A comment is a whole line: a `#` after a
  value is part of the value.
- A line that ends in a backslash continues on the next line (whose leading white space is skipped).
- Escapes in keys and values: `\t \n \r \f \\ \= \: \# \! \,` a backslash and a space, and `\uXXXX`. Any other
  backslash sequence is an error (Java would drop the backslash, so `C:\data` and a typo pass silently there).
- The file is UTF-8. Java's reader assumes ISO-8859-1 and wants `\uXXXX` for everything else; this one does not
  follow that: write the characters themselves. Invalid UTF-8 is an error.
- White space at the end of a value is dropped (Java keeps it, which makes `dir = /data ` a directory with a space
  in its name that nobody sees). To keep one, escape it: `\ `.

**Lists** are comma-separated: `health.domains = /mnt/nfs1, /mnt/nfs2`. White space around the commas is
trimmed. An element that contains a comma, or begins or ends with a space, is escaped with a backslash:
`\,` and `\ `. An empty element (`a,,b`, `a,`) is an error. A list key stands for an option that may be given many
times on the command line (`--health-domain` once per element).

**Values.** A boolean is `true` or `false` (nothing else: `yes`, `on` and `1` are errors, so there is one way to
write it). A number is a whole number. A duration is a whole number in the unit the option has (listed in the
tables) or a number with a unit: `500ms`, `30s`, `5m`, `1h`, `1d`. A size is a whole number in the unit of the
option or a number with `B`, `K`, `M`, `G` or `T` (binary: `1K` is 1024 bytes). Where an option takes a negative
number to switch something off (`merge.idle = -1`) the file takes it too.

## Rules

- **Precedence:** built-in default, then the file, then the command line. There is no environment layer: the
  program has no environment variables for settings, and a third source would only make it harder to say why a
  server runs as it does. An option that appears on the command line is not taken from the file at all; so a list
  on the command line replaces the file's list, it does not add to it. That needs the option's whole name:
  `getopt_long` would take `--work` for `--workers`, which the rule would not see, so an abbreviated option is an
  error (`--work is an abbreviation of --workers`).
- **A flag that the file sets can be set back on the command line:** every boolean option also takes `=true` or
  `=false` (`--read-only=false`, `--verify-checksums=true`). For the options that are written the other way round,
  `--no-auth` and `--no-fsync` and `--no-stats`, the word is about the option: `--no-auth=false` is login on
  (`auth.enabled = true`), and a server whose file says `auth.enabled = false` says so, with the file and the line, in
  its warning at start.
- **A mistake is an error, with the file and the line** (`tsdbd: /etc/tsdb/tsdbd.properties:7: unknown key
  "storage.dri"`): an unknown key (a typo must not leave a server running with a default), a duplicate key (which
  of the two would count?), a value that does not fit the key, a list where one value is expected. This holds for
  the whole file, also for a line that the command line overrides.
- **No secrets in the file.** There is no key for a password. Keys that name a file with a secret
  (`cluster.secret.file`, `client.login.password.file`) are fine; the file has the mode of a secret (600).
- **The file must not be writable by its group or by others.** Whoever can change it decides where the server
  stores data and who may log in (`auth.enabled = false`), so tsdbd refuses a file with write bits beyond the
  owner's (`chmod go-w FILE`), as it does for the cluster secret.
- **No reload.** A change takes effect at the next start (`systemctl restart tsdbd`). `SIGHUP` is ignored, as
  before.
- **One table.** The keys are rows of one table in the source (`server/tsdbd_keys.h`, `tools/tsdb-cli.c`), which the
  option parser, `--config-keys` and this page all read. `tests/test_config.sh` fails when an option of tsdbd has no
  row (or a row names no option), and when the tables below differ from what the program prints.
- **Options without a key** are those that only make sense on one command line: `--version`, the options that name
  or check the file, and `--exit-with-parent`, which test harnesses use and which must not be a permanent part of
  how a server is configured.
- Disks: the keys of the `storage.` group are reserved for the device classes (`storage.data.dirs` and its
  neighbours, `docs/design-cluster-data.md`). They are not built yet.

## tsdbd keys

<!-- keys:tsdbd:begin -->
| Key | Option | Value | Default | Meaning |
|---|---|---|---|---|
| `storage.dir` | `--dir` | string | unset | The database directory (required, unless this is a witness). |
| `storage.fsync` | `--no-fsync` (when false) | boolean | true | false turns fsync off: a power loss then looks like damage and fails the next open. For tests and benchmarks only. |
| `storage.read.only` | `--read-only` | boolean | false | Serve queries only, from a database another server writes. |
| `storage.follow` | `--follow` | boolean | false | Follow a database that another writer on shared storage updates (read-only; see docs/ha.md). |
| `storage.refresh` | `--refresh-ms` | duration (plain: ms) | 100ms | How often a follower looks for new parts. |
| `storage.verify.checksums` | `--verify-checksums` | boolean | false | Check the hash of every data block read. Turn it on unless the file system checks its data itself (ZFS). |
| `storage.pin.index` | `--pin-index` | boolean | false | Keep the granule index of every part in memory. |
| `storage.partition` | `--partition-ms` | duration (plain: ms) | 1d | Length of a base partition. Used only when the database is created. |
| `storage.partition.classes` | `--partition-classes` | integer | 1 | 1: series of different rates go to partitions of 1, 7, 28 or 364 days; 0: all to base partitions. Used only when the database is created. |
| `storage.granule.rows` | `--granule-rows` | integer | 8192 | Rows per granule. Used only when the database is created. |
| `server.listen` | `--listen` | string | 127.0.0.1 | The address clients connect to (there is no TLS: a trusted interface). |
| `server.port` | `--port` | integer | 7070 | The client port; 0 lets the kernel choose (see server.port.file). |
| `server.port.file` | `--port-file` | string | unset | A file that tsdbd writes, once it serves, with "PORT PID" (and the metrics port), for a launcher to read. |
| `server.workers` | `--workers` | integer | unset | Worker threads. Default: the number of CPUs. |
| `server.max.connections` | `--max-connections` | integer | 10000 | Most client connections (also bounded by the descriptor limit). |
| `server.io` | `--io` | auto,uring,epoll | auto | The event loop: io_uring, or epoll where io_uring is disabled. |
| `server.send.timeout` | `--send-timeout` | duration (plain: s) | 60s | Close a connection whose reader takes no reply for this long. |
| `server.frame.timeout` | `--frame-timeout` | duration (plain: s) | 60s | Close a connection that stops in the middle of a request. |
| `server.max.request.memory` | `--max-request-memory` | size (plain: MiB) | unset | Memory all requests in flight may hold. Default: an eighth of the RAM, at least 1G. |
| `auth.enabled` | `--no-auth` (when false) | boolean | true | false: no login, every client may read and write every series, the user admin operations are refused. A trusted network only. |
| `auth.login.timeout` | `--login-timeout` | duration (plain: s) | 10s | Close a connection that has not logged in within this time. |
| `wal.sync` | `--wal-sync-ms` | duration (plain: ms) | 0 | 0: every write returns after its own fsync (shared by concurrent writers); a time: sync that often and bound the loss of a power cut to it; -1: no WAL. |
| `wal.max.batch.rows` | `--wal-max-batch-rows` | integer | 65536 | Most rows of one insert that go through the WAL. |
| `merge.idle` | `--merge-idle-s` | duration (plain: s) | 300s | Merge a partition completely once it has had no new rows for this long; -1 never. |
| `flush.interval` | `--flush-interval-ms` | duration (plain: ms) | 5s | How long rows stay in memory before they are flushed to a part. |
| `insert.zstd.level` | `--insert-zstd-level` | integer | 0 | The zstd level of parts that inserts and flushes write; 0 none, -1 the level merges use. |
| `query.threads` | `--query-threads` | integer | unset | Threads one tsdbd uses for queries. Default: the number of CPUs. |
| `query.request.threads` | `--request-threads` | integer | unset | Most threads one request may use. Default: all of them. |
| `query.multi.max.rows` | `--multi-max-rows` | integer | 268435456 | Most rows one QUERY_MULTI answer may carry. |
| `query.lttb.max.threshold` | `--lttb-max-threshold` | integer | 16777216 | Most points an LTTB query may ask for. |
| `metrics.port` | `--metrics-port` | string | unset | The Prometheus port (a number, or auto for one the kernel chooses). Off by default. |
| `metrics.bind` | `--metrics-bind` | string | unset | The address of the metrics port. Default: server.listen. |
| `stats.enabled` | `--no-stats` (when false) | boolean | true | false turns the statistics (docs/design-stats.md) off. |
| `latest.cache` | `--latest-cache` | size (plain: bytes) | unset | Memory of the latest-value table. Default: a sixteenth of the RAM. |
| `health.domains` | `--health-domain` | list | unset | Path prefixes of storage that can fail alone (an NFS mount each): a volume under one answers UNAVAILABLE while its storage is down and the others go on. A comma-separated list. |
| `cluster.enabled` | `--cluster` | boolean | false | Join a cluster (needs cluster.secret.file and cluster.state.dir). |
| `cluster.peer.port` | `--peer-port` | integer | unset | The port of the other members. Default: server.port + 1. |
| `cluster.peer.listen` | `--peer-listen` | string | unset | The address of the peer port. Default: server.listen. |
| `cluster.advertise` | `--advertise` | string | unset | The host name other members and clients use for this one. |
| `cluster.rack` | `--rack` | string | unset | The failure domain of this node (a rack name). |
| `cluster.secret.file` | `--cluster-secret-file` | string | unset | The file with the cluster's shared secret (mode 600). |
| `cluster.state.dir` | `--state-dir` | string | unset | Where the metadata group keeps its state (a local disk, never storage.dir). |
| `cluster.power.feed` | `--power-feed` | string | unset | The power feed of this node, a failure domain. |
| `cluster.bmc` | `--bmc` | string | unset | The address of this node's BMC, for fencing. |
| `cluster.witness` | `--witness` | boolean | false | A member that only votes: no volumes, no client port. |
| `cluster.read.barrier` | `--read-barrier` | on,off | on | Whether reads wait for the leader's confirmation. |

Options with no key:

| Option | Why |
|---|---|
| `--version` | Prints the version and exits. |
| `--config` | Names the file itself. |
| `--no-config` | Reads no file, not even /etc/tsdb/tsdbd.properties. |
| `--check-config` | Checks the file and the command line, prints the effective settings and exits. |
| `--config-keys` | Prints the table of keys (Markdown) and exits. |
| `--exit-with-parent` | Test harnesses only: stop when the parent process goes away. A file must not make a server depend on it. |
<!-- keys:tsdbd:end -->

## The tsdb command line tool

`tsdb` reads a file for its connection settings only, the options of the commands that talk to a server:
`tsdb user VERB ... --server HOST:PORT` (the user commands that work through a running tsdbd) and `tsdb cluster`.
Commands that open a database directory take everything from their arguments. `tsdb --config FILE ...` names the
file; without it `tsdb` reads the first of `$XDG_CONFIG_HOME/tsdb/tsdb.properties`,
`~/.config/tsdb/tsdb.properties` and `/etc/tsdb/tsdb.properties` that exists; `--no-config` reads none. With
`client.server` set, `tsdb user VERB ...` goes through that server even without `--server`. `tsdb --check-config`
and `tsdb --config-keys` do what they do for tsdbd. The same rules apply (errors with the line, mode, precedence).

```properties
# ~/.config/tsdb/tsdb.properties
client.server = db1:7070
client.login = admin
client.login.password.file = /home/me/.tsdb-password
```

<!-- keys:tsdb:begin -->
| Key | Option | Value | Default | Meaning |
|---|---|---|---|---|
| `client.server` | `--server` | string | unset | HOST:PORT of a running tsdbd: `tsdb user VERB ... ` goes through it (even without --server) and logs in as client.login. |
| `client.login` | `--login` | string | unset | The user to log in as. |
| `client.login.password.file` | `--login-password-file` | string | unset | A file with that user's password (mode 600). Passwords are never written in the configuration file itself. |
| `cluster.secret.file` | `--secret-file` | string | unset | The cluster's shared secret file, for `tsdb cluster`. |

Options with no key:

| Option | Why |
|---|---|
| `--config` | Names the file itself (the first argument: `tsdb --config FILE ...`). |
| `--no-config` | Reads no file. |
| `--check-config` | Checks the file and prints the effective settings (`tsdb --check-config`). |
| `--config-keys` | Prints the table of keys (Markdown). |
<!-- keys:tsdb:end -->
