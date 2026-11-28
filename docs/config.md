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
is installed on the same machine; it reads a file only when `--config` names one. When such a server finds the default file it prints one line on
stderr that names it and says how to use it (`pass it with --config FILE`), so a service started with the flag by
mistake is not silently running on defaults. A default file that exists but
cannot be looked at (a directory without the right to enter it) is an error, not "no file". (`TSDBD_DEFAULT_CONFIG` is a test hook: the environment variable names another path for the default file, so the
tests need no `/etc`; it names a path, never a setting, and is not for production.)
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
  error (`--work is an abbreviation of --workers`). **When an option or flag is given more than once on the command
  line, the last one counts**, in every form (`--no-auth --no-auth=false` is login on, `--no-auth=false --no-auth` is
  login off); `--check-config` shows the same.
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
- **Configuration is not reloaded on `SIGHUP`.** A change to `.properties` settings or flags takes effect at the next
  start (`systemctl restart tsdbd`). `SIGHUP` reloads `--tenants FILE` (`tenants.file`) when set, and is ignored
  otherwise.
- **One table.** The keys are rows of one table in the source (`server/tsdbd_keys.h`, `tools/tsdb-cli.c`), which the
  option parser, `--config-keys` and this page all read. `tests/test_config.sh` fails when an option of tsdbd has no
  row (or a row names no option), and when the tables below differ from what the program prints.
- **Options without a key** are those that only make sense on one command line: `--version`, the options that name
  or check the file, and `--exit-with-parent`, which test harnesses use and which must not be a permanent part of
  how a server is configured.
- **`--cluster=false` on the command line turns the cluster off** whatever the file says: the file's other `cluster.*`
  keys are then left out (tsdbd prints one line that names them), where they would otherwise be refused (cluster
  settings without the cluster). A cluster option on the command line next to `--cluster=false` is an error, as is a
  file that sets `cluster.*` keys and `cluster.enabled = false`: the message names the settings and where they are.
- **Single-server tier keys (`storage.tiers`, `storage.tier.cache`):** `TIERS` in the database root is the source of
  truth for a single-server settled tier ([tier.md](tier.md)); `tsdbd` opens it automatically without any key. When
  `storage.tiers` (`--tier`) is set, `tsdbd` checks at startup that its list matches `TIERS`'s active (non-lost,
  non-shrinking, non-removed) stores by path and class and refuses to start on any difference; it never creates or adds
  a store, and is refused with `--cluster`. `storage.tier.cache` (`--tier-cache-mb`) sets this process's decoded
  granule cache budget (default: a tenth of the RAM); persistent tier settings (`--redundancy`, `--after`,
  `--high-pct`, `--young-high-pct`, `--rebuild-mb-s`, `--scrub-mb-s`, `--scrub-days`) live in `TIERS` via
  `tsdb DIR tier set`.
- **Cluster disk keys (`storage.disks`, `cluster.role`, `cluster.allow.shared.device`, `cluster.adopt`):** in cluster
  mode, `storage.disks` (`--disk`) declares the node's initialised disks (`PATH[:CLASS]`, comma-separated; each
  initialised with `tsdb disk init`; replaces the removed `--chunk-dir` / `cluster.chunk.dir`). `cluster.role` (`--role`)
  narrows the capability derived from the disks (`both`, `compute`, or `storage`), `cluster.allow.shared.device`
  (`--allow-shared-device`) lifts the `st_dev` isolation check for test harnesses and single-disk dev hosts (emitting a
  health warning), and `cluster.adopt` (`--adopt OLD`) accepts a replaced member's disks as pending adoption until
  `tsdb cluster node replace OLD NEW` commits.

## tsdbd keys

<!-- keys:tsdbd:begin -->
| Key | Option | Value | Default | Meaning |
|---|---|---|---|---|
| `storage.dir` | `--dir` | string | unset | The database directory (required, unless this is a witness or a cluster node with storage.disks; in cluster mode it is also treated as a disk using its DISK metadata without /sys detection). |
| `storage.fsync` | `--no-fsync` (when false) | boolean | true | false turns fsync off: a power loss then looks like damage and fails the next open. For tests and benchmarks only. |
| `storage.read.only` | `--read-only` | boolean | false | Serve queries only, from a database another server writes. |
| `storage.follow` | `--follow` | boolean | false | Follow a database that another writer on shared storage updates (read-only; see docs/ha.md). |
| `storage.refresh` | `--refresh-ms` | duration (plain: ms) | 100ms | How often a follower looks for new parts. |
| `storage.verify.checksums` | `--verify-checksums` | boolean | false | Check the hash of every data block read. Turn it on unless the file system checks its data itself (ZFS). |
| `storage.pin.index` | `--pin-index` | boolean | false | Keep the granule index of every part in memory. |
| `storage.partition` | `--partition-ms` | duration (plain: ms) | 1d | Length of a base partition. Used only when the database is created. |
| `storage.partition.classes` | `--partition-classes` | integer | 1 | 1: series of different rates go to partitions of 1, 7, 28 or 364 days; 0: all to base partitions. Used only when the database is created. |
| `storage.granule.rows` | `--granule-rows` | integer | 8192 | Rows per granule. Used only when the database is created. |
| `storage.disks` | `--disk` | list | unset | The disks of a cluster node, PATH[:CLASS] each (nvme, ssd or hdd; comma-separated; each initialised with tsdb disk init; ':' in the final path component separates :CLASS). |
| `storage.tiers` | `--tier` | list | unset | The stores of the single-server tier, PATH[:CLASS] each: checked against TIERS at start (never adds a store; a mismatch refuses to start). |
| `storage.tier.cache` | `--tier-cache-mb` | size (plain: MiB) | unset | Memory of the single-server tier's decoded granule cache. Default: a tenth of the RAM. |
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
| `flush.buffer.rows` | `--buffer-rows` | integer | 1048576 | Memtable rows per volume before it is flushed to a part; at most 16777216. |
| `insert.zstd.level` | `--insert-zstd-level` | integer | 0 | The zstd level of parts that inserts and flushes write; 0 none, -1 the level merges use. |
| `query.threads` | `--query-threads` | integer | unset | Threads one tsdbd uses for queries. Default: the number of CPUs. |
| `query.request.threads` | `--request-threads` | integer | unset | Most threads one request may use. Default: all of them. |
| `query.multi.max.rows` | `--multi-max-rows` | integer | 268435456 | Most rows one QUERY_MULTI answer may carry. |
| `query.lttb.max.threshold` | `--lttb-max-threshold` | integer | 16777216 | Most points an LTTB query may ask for. |
| `metrics.port` | `--metrics-port` | string | unset | The Prometheus port (a number, or auto for one the kernel chooses). Off by default. |
| `metrics.bind` | `--metrics-bind` | string | unset | The address of the metrics port. Default: server.listen. |
| `stats.enabled` | `--no-stats` (when false) | boolean | true | false turns the statistics (docs/design-stats.md) off. |
| `tenants.file` | `--tenants` | string | unset | The tenant list (docs/design-stats.md, section 2.1). Default: the prefix: grants that tenant admins hold admin on. |
| `tenants.settle` | `--tenants-settle` | duration (plain: s) | 30s | How long a changed tenant list takes to settle before it applies. |
| `latest.cache` | `--latest-cache` | size (plain: bytes) | unset | Memory of the latest-value table. Default: a sixteenth of the RAM. |
| `health.domains` | `--health-domain` | list | unset | Path prefixes of storage that can fail alone (an NFS mount each): a volume under one answers UNAVAILABLE while its storage is down and the others go on. A comma-separated list. |
| `cluster.enabled` | `--cluster` | boolean | false | Join a cluster (needs cluster.secret.file and cluster.state.dir). |
| `cluster.peer.port` | `--peer-port` | integer | unset | The port of the other members. Default: server.port + 1. |
| `cluster.peer.listen` | `--peer-listen` | string | unset | The address of the peer port. Default: server.listen. |
| `cluster.advertise` | `--advertise` | string | unset | The host name other members and clients use for this one. |
| `cluster.peer.advertise` | `--peer-advertise` | string | unset | The peer address (host:port) put in the metadata instead of the advertised host and the real peer port, for a proxy or NAT in front of the peer port. |
| `cluster.rack` | `--rack` | string | unset | The failure domain of this node (a rack name). |
| `cluster.secret.file` | `--cluster-secret-file` | string | unset | The file with the cluster's shared secret (mode 600). |
| `cluster.state.dir` | `--state-dir` | string | unset | Where the metadata group keeps its state (a local disk, never storage.dir). |
| `cluster.power.feed` | `--power-feed` | string | unset | The power feed of this node, a failure domain. |
| `cluster.bmc` | `--bmc` | string | unset | The address of this node's BMC, for fencing. |
| `cluster.witness` | `--witness` | boolean | false | A member that only votes: no volumes, no client port. |
| `cluster.read.barrier` | `--read-barrier` | on,off | on | Whether reads wait for the leader's confirmation. |
| `cluster.peer.check` | `--peer-check` | none,header,full | none | The check on every frame of the data links between members: none (the payloads' own checksums and the receivers' bounds checks), header (8 bytes over each frame's header and its payload's first 256 bytes) or full (16 bytes over every byte). A link runs at the stronger of its two ends' levels. |
| `cluster.peer.rdma` | `--peer-rdma` | off,auto,on | auto | Upgrade data links between members to RDMA verbs (docs/design-rdma.md): off (TCP only), auto (upgrade where both ends can, else TCP), or on (a data link that cannot upgrade ends, and tsdbd refuses to start without an RDMA pool). |
| `cluster.peer.rdma.device` | `--peer-rdma-device` | list | unset | RDMA device and port (NAME[:PORT], comma-separated list for multiple fabrics). Default: the first active InfiniBand port (RoCE and iWARP ports are used only when named). |
| `cluster.peer.rdma.kinds` | `--peer-rdma-kinds` | list | log,bulk,fwd,fwdq,snap,chunk,chunkb,chunkr | Data link kinds that upgrade to RDMA (comma-separated; the metadata link always stays TCP). |
| `cluster.peer.rdma.pool_mb` | `--peer-rdma-pool-mb` | size (plain: MiB) | 64 | Registered memory per tsdbd and RDMA device, shared by its peerlinks (default 64 MiB: 32 KiB slots, 1 MiB per connection; pools under 16 MiB use compact slots automatically; needs LimitMEMLOCK above this plus QP and io_uring rings). |
| `cluster.peer.rdma.sl` | `--peer-rdma-sl` | list | 0 | InfiniBand service level (0 to 15) of RDMA queue pairs: one number for every kind, or KIND=SL entries (log=1,bulk=2,snap=2). |
| `cluster.peer.rdma.retry_s` | `--peer-rdma-retry-s` | duration (plain: s) | 600s | How long a data link that settled on TCP after a transient RDMA failure waits before offering RDMA again on the live link (0 disables re-offers). |
| `cluster.peer.rdma.poll_us` | `--peer-rdma-poll-us` | integer | 0 | Microseconds to busy-poll an RDMA completion queue before sleeping on completion events (0 to 10000). |
| `cluster.peer.rdma.cm` | `--peer-rdma-cm` | boolean | false | Use rdma_cm (Option C, dynamically loading librdmacm) to establish RDMA connections instead of exchanging QP numbers over TCP; required for iWARP and optional on InfiniBand/RoCE. |
| `cluster.peer.allow_ipoib` | `--peer-allow-ipoib` | boolean | false | true lets the TCP peer address bind or dial through an IPoIB interface (ARPHRD_INFINIBAND); off by default so no TCP runs over IPoIB. |
| `cluster.forward` | `--forward` | on,off | on | Whether a request for a volume another member leads is forwarded there (on) or answered MOVED to it (off). |
| `cluster.fwd.wait` | `--fwd-wait-ms` | duration (plain: ms) | 2s | How long a request for a volume another member leads may wait to be sent there (forwarded) before it is answered UNAVAILABLE (definite: nothing reached the leader). |
| `cluster.fwd.max.age` | `--fwd-max-age-ms` | duration (plain: ms) | 1s | A request older than this since it was read is never forwarded, and its leader refuses it (UNAVAILABLE). At most 2 s: clients resend no earlier than 3 s after sending. |
| `cluster.fwd.reply` | `--fwd-reply-ms` | duration (plain: ms) | 30s | How long a forwarded request may wait for its leader's reply before the client's connection is closed without an answer (its outcome is unknown). 0: no limit (a dead link still ends it). |
| `cluster.fwd.workers` | `--fwd-workers` | integer | 4 | Worker threads reserved for requests other members forward here (more start while all of them wait). |
| `cluster.role` | `--role` | both,compute,storage | both | Declared role of this member: both (compute and storage as disks allow), compute (log replicas only), or storage (chunk stores only). |
| `cluster.allow.shared.device` | `--allow-shared-device` | boolean | false | Allow a disk on the same file system as /, cluster.state.dir or another disk (for test harnesses and one-disk development hosts; health warns while set). |
| `cluster.adopt` | `--adopt` | integer | 0 | Adopt disks belonging to replaced member id OLD after tsdb cluster node replace OLD NEW commits (0: off). |
| `cluster.chunk.rate` | `--chunk-rate-mb-s` | integer | 256 | MB/s of chunk puts to other members, all links together: a cap of its own, apart from replication's. |
| `ec.settle.grace` | `--ec-settle-grace-s` | duration (plain: s) | 1h | How long after a partition's end it is erasure-coded (settled). |
| `ec.settle.keep` | `--ec-settle-keep-s` | duration (plain: s) | 1d | How long the parts a settle replaced are kept. |
| `ec.settle.force` | `--ec-settle-force-s` | duration (plain: s) | 1h | How long a settle waits for the partition's rows in memory to be flushed before it flushes them. |
| `ec.settle.wait.max` | `--ec-settle-wait-max-s` | duration (plain: s) | 2h | How long a settle waits for every member of its stripe to be up before it settles without one. |
| `ec.cache` | `--ec-cache-mb` | size (plain: MiB) | unset | Memory of the cache of decoded erasure-coded data. Default: a tenth of the RAM. |

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
`tsdb user VERB ... --server HOST:PORT` (the user commands that work through a running tsdbd), `tsdb stats series|tenants|hot ... --server HOST:PORT` and `tsdb cluster`.
Commands that open a database directory take everything from their arguments. `tsdb --config FILE ...` names the
file; without it `tsdb` reads the first of `$XDG_CONFIG_HOME/tsdb/tsdb.properties`,
`~/.config/tsdb/tsdb.properties` and `/etc/tsdb/tsdb.properties` that exists; `--no-config` reads none. With
`client.server` set, `tsdb user VERB ...` and `tsdb stats series|tenants|hot ...` go through that server even without `--server`. `tsdb --check-config`
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
| `client.server` | `--server` | string | unset | HOST:PORT of a running tsdbd: `tsdb user VERB ... ` and `tsdb stats series`, `tenants` and `hot` go through it (even without --server) and log in as client.login. |
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
