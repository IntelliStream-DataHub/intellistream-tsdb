# Changelog

All notable changes to tsdb are listed here, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the versions follow
[Semantic Versioning](https://semver.org/), with the exception described in
[docs/compatibility.md](docs/compatibility.md): before 1.0, a 0.9.x release may still change the
disk format and the wire protocol.

## [0.9.1] - 2026-11-07

A patch release: **two data fixes**, storage outage handling for NFS, macOS as a developer platform, and smaller
fixes to the server. **Disk format version 8 and wire protocol version 12, as in 0.9.0**, and the embedded C ABI is
still version 3 (it only gained functions and structs). A database of 0.9.0 opens as it is. The wire protocol did
not change (the new status `TSDB_ERR_UNAVAILABLE` only appears for volumes that an operator put in a
`--health-domain`); use the same release on the server and its clients all the same.

### Changed

- **Disk format 9** (step S0 of docs/design-ha-modes.md; the disk half). The WAL record has a 32-byte header with a log
  number (`fno`) and the group commit's time, and can be a marker; the part header is 48 bytes (the range of log numbers
  of its rows, and `written_ms`); every volume has an `FNO` file, two checksummed slots that bound the numbers so
  that they never go back across opens (before a WAL segment is unlinked it holds the segment's end: one `fdatasync`
  per flush); `META` can say `requires=` (a binary refuses a feature it does not know, naming it); `MANIFEST` has an
  `fno_next` line (information for followers). The cost is 16 more bytes per WAL record: not measurable with an
  fsync per call, 1 to 8% on appends without one (BENCHMARKS.md, "Format 9").
  **Carrying a database over:** a format-8 database is refused, naming both versions. `tsdb DIR upgrade` carries it
  over in place when it was closed cleanly with 0.9.0 (no WAL segment, intent or tmp part; stop tsdbd and every
  follower first): FNO, MANIFEST and META of each volume, the root last, safe to run again. The parts keep their old
  headers until merges rewrite them (`META` says `parts_v8=1` meanwhile; merges of idle partitions and `tsdb DIR
  optimize` do it, the open logs how many are left). Export and import (docs/export-format.md) still always works.
  See docs/compatibility.md. With one bad `FNO` slot (a torn store or damage; they look alike) the open uses the other
  and skips the numbers ahead by 2^40, so none is handed out twice; do not run 0.9.0's `tsdb` (`volume add`) on an
  upgraded database.
- New status codes -18 (`TSDB_ERR_FEED_GONE`) and -19 (`TSDB_ERR_FEED_RESET`) are reserved in `tsdb.h` for the change
  feed; nothing returns them yet.

### Added

- **Marker payloads and the namespace-event hook** (docs/design-ha-modes.md 2.1): `COMMIT`, `DROP`, `RETENTION` and
  `EPOCH` have one encoder and one decoder (`src/marker.c`, fuzzed), the type numbers of the feed's and the cluster's
  other markers are reserved, and the engine calls a no-op `jr_intent` / `jr_commit` hook where names and files
  become visible. No backend yet: no marker is written outside the tests.
- `tsdb_upgrade(dir)` in `tsdb.h` and `tsdb DIR upgrade` (above).

### Fixed

- **A deleted partition no longer comes back after a crash while a merge of it was running.**
  `tsdb_drop_before` (and retention dropping an expired partition) acknowledged the drop, but a merge of that
  partition that was already running had named its output (durably) before it found its sources dropped, and only
  unlinked the output. After a kill -9 or a power loss the open listed that output alone, with every dropped row.
  The drop now waits for a merge of the partitions it drops, no new merge starts in them while it waits (a stream
  of merges could otherwise keep it waiting), and retention leaves a partition with a merge in flight to its next
  pass. The merge's own discard of such an output (a backstop) renames it to `retired_` and syncs the directory.
  Tests in `tests/test_tsdb.c` (the drop held against a merge on both sides of the naming, at lifetimes 0 and 480,
  under kill -9 and a simulated power loss; `drop_blocks_new_merges`).

- **A dropped partition no longer comes back after a crash when `obsolete_part_lifetime_s` is 0 or less.** A dropped
  part kept its listed name while any query held a reference to it; `tsdb_drop_before` (or retention) acknowledged
  after the directory fsync, a crash then made the open list the part again, and the dropped rows came back (nothing
  covers a dropped part). Replaced and dropped parts are now renamed to `retired_<name>` at every lifetime, before
  that fsync; at a lifetime of 0 or less the last reference deletes the retired name, and the open deletes any left
  by a crash.
- **Inserts no longer wait for ever on a partition whose merges keep failing.** An insert that leaves a partition with
  more than 300 parts waits for merges (like `parts_to_delay_insert`), and the wait had no end: a merge that failed
  on a granule with damaged payload (the merge checks each granule's hash, the open does not) was retried every
  second, the count never fell, and every insert into the partition, so every worker of `tsdbd`, waited for ever (found
  by `make jepsen`, nemesis `misdirect`, with `verify_checksums` off; the same with it on). Now a merge that cannot
  decode a granule of a source part marks that part, like an index that no longer matches its checksum: logged once,
  counted once in `tsdb_broken_parts`, and left out of merges and drops with its partition (`tsdb_optimize` merges the
  rest and returns `TSDB_ERR_CORRUPT`). The mark is kept in memory only, and the log says to restore the part: queries
  that decode the granule fail with `TSDB_ERR_CORRUPT`, or without `verify_checksums` may return wrong values. The wait
  ends at once for a partition with a marked part and when the engine has failed, and after 30 s in which the count did
  not fall and no merge of the partition ran (for example parts that cannot pair under `max_part_bytes`), or after
  30 s from the first merge of the partition that failed during the wait (a full disk, EIO on the merge output: a
  merge that takes minutes would otherwise stretch the 30 s to hours). The first wait that runs out remembers the
  partition as stalled, so later inserts into it, and the other parts of the same insert, go on at once until its
  count falls or a merge of it commits, instead of each paying 30 s. The rows are published before the wait, so the
  insert still succeeds; the partition's part count grows until the part is restored. `tests/test_damage.c`
  (`test_wait_merges_damaged`, `test_wait_merges_stalled`, `test_wait_merges_failing`).
- **A volume recovery in `tsdbd` no longer frees an engine that a name or type lookup still holds.** While a volume
  is quiesced the server still answers names and types, each holding a reference on the engine, and
  `tsdb_volume_reopen` freed the old engine after swapping it without waiting for a reference taken after its own
  check (a use after free under ASan). It now waits for them to give theirs back before it frees the engine.
- **The loss of a part after a storage outage is no longer hidden by the new engine's merges.** `tsdb_volume_reopen`
  compared the parts the halted engine had with the new engine's list after the new engine's merge and flush threads
  had started: a merge across the gap of a lost part made it look covered (the loss was not reported, no DIVERGED),
  healthy parts being merged were reported lost, and the loop read part pointers a merge freed. The comparison now
  runs inside the open, before those threads start. Tests in `tests/test_recover.c`
  (`meta_reference_at_reopen`, `merge_clears_loss`).
- **A step of the wall clock no longer deletes retired parts early.** The `obsolete_part_lifetime_s` of a part a
  merge replaced (`retired_*`, which a follower on another host may still have mapped) is now counted on the monotonic
  clock, also for the retired files a restart finds. A clock set forward (NTP, an operator, a resumed VM) used to
  delete them at once. `tests/test_graveyard.c` steps a fake wall clock.
- tsdbd closes a connection whose output has not moved for `--send-timeout` seconds even when its whole reply fit in
  the server's buffers (so no worker waited on it): before, a client that sent a query and never read the answer held
  up to about 17 MB per connection for as long as it kept the socket open, outside `--max-request-memory`.
- tsdbd no longer cuts a client that keeps reading its reply, only slowly. A reply larger than 8 MB was lost by a
  client that read less than about 9 MB per `--send-timeout` (150 KB/s at the default of 60 s): a worker waiting for
  output space gave up that long after its wait began, whether the output moved or not. Progress is now any change
  in the socket's queue, checked every second, so the floor is gone for replies of any size. Time the server itself
  was stopped (SIGSTOP, a frozen container) no longer counts against `--send-timeout`, `--frame-timeout` or
  `--login-timeout`.
- `tsdb_query_multi` (QUERY_MULTI) checks the bounds of a bounds query once per call instead of once per series:
  65,536 series over the 33 million bounds of one frame took about 250 s of comparisons. Bounds that do not
  strictly increase now answer `TSDB_ERR_INVALID` for every series, trimmed or not, as the single-series call does
  on the whole bounds. Before, a trimmed series was answered from its own part of the bounds: buckets when that
  part increased, empty when the series had no rows. No client sends such bounds.
- `tsdb_query_multi` claims the first series for the caller before it wakes its helpers. A helper that ran first
  took it as a series ahead: collected and held, or, over 65,536 rows, computed, thrown away and run again at its
  turn. Measured with `tsdbc-bench query --multi 100 --conns 1` (100 series x 1 M rows, optimized; 0.9.0 and this
  change interleaved, 5 runs each; series per second, mean +- sd, lowest-highest):

  | query | 0.9.0 | this change |
  |---|---:|---:|
  | raw, 3600 samples | 79,247 +- 1,699 (77,153-81,053) | 79,511 +- 1,084 (77,652-80,413) |
  | 86,400 samples in 288 buckets | 70,629 +- 1,967 (68,029-72,463) | 70,889 +- 1,363 (68,679-71,847) |
  | 604,800 samples in 168 buckets | 40,522 +- 967 (39,116-41,286) | 37,939 +- 5,485 (28,190-41,001) |
  | TWA, 604,800 samples in 168 buckets | 11,073 +- 184 (10,746-11,188) | 10,963 +- 240 (10,692-11,237) |
  | the whole range in one bucket | 86,061 +- 705 (85,121-86,737) | 84,584 +- 675 (83,664-85,296) |

  The same within the noise, but for the whole range in one bucket (about 12 us a series, from granule statistics):
  1.7% fewer, in each of the five pairs of runs. The 168-bucket column has one slow run (28,190); the other four
  average 40,376. Every answer here is far below 65,536 rows, so the change has nothing to save in this benchmark;
  it saves where the first series' answer is larger.

### Added

- **Storage outages on NFS: the single-server parts of docs/ha.md section 15** (step N1). `tsdbd --health-domain PREFIX`
  (once per NFS server or filesystem) probes each domain with `O_DIRECT` reads and writes and tracks UP, DEGRADED,
  DOWN and RECOVERING (`tsdb_health_*` in `tsdb.h`). Requests for a volume of a DOWN domain answer
  `TSDB_ERR_UNAVAILABLE` (new, -11) at once, at most 4 inside the volume (the cap applies only while the domain is not
  UP), and the other volumes go on. A volume whose reopen finds acknowledged parts, RETENTION or CLASSES records gone
  stays refused (DIVERGED) until tsdbd is restarted, also through a later outage. A tsdbd restarted during an outage
  serves nothing until the heal, and SIGTERM during a hang waits for the heal. A write or sync
  error on such a volume no longer ends tsdbd: the volume is stopped, reopened in place from what is on disk (the WAL
  is replayed, a retried fsync is never trusted), checked against what the engine remembers (WAL tail, SERIES, STATES,
  parts) and resumed; a volume that lost acknowledged parts stays stopped and says which. The root, USERS and volumes
  outside a domain still exit with status 1. New API: `tsdb_volume_quiesce`, `_idle`, `_reopen`, `_resume`,
  `tsdb_set_abandon_flag` and `TSDB_ERR_ABANDONED` (-14). An error while reading a directory now fails the open
  instead of ending the scan. New tests: `outagefs` (a fault shim: hang, EIO, ESTALE, ENOSPC, slow, rollback per path
  prefix), `test_health`, `test_abandon`, `test_recover`, `test_outage`, and an `outage` nemesis in `make jepsen`;
  [docs/tasks/nfs-outage.md](docs/tasks/nfs-outage.md) has the runs for a real NFS server (with
  `build/outage-load`). Not yet: mountinfo discovery of domains, parked requests with a deadline, a bounded queue per
  volume, SIGTERM while a call is stuck, metrics, a local high-water cache.
- **macOS (Apple silicon and Intel) as a developer platform** ([docs/macos.md](docs/macos.md)), not for
  production. `make` (zstd from Homebrew), `make test IO=epoll` and `make test-clients` work there; `tsdbd` runs its
  epoll loop over kqueue (no io_uring), and the embedded Rust, Python and Java builds carry `libtsdb.dylib`. Every
  `fsync` and `fdatasync` is followed by `fcntl(F_FULLFSYNC)`, because a plain `fsync` on macOS leaves the data in
  the drive's cache (`TSDB_FAST_FSYNC=1` gives that up for throwaway data, and says so on stderr). A `tsdb import`
  into a new directory syncs every file there, for want of `syncfs`. Integer variances have the same bits as on x86:
  on Apple silicon `src/ext80.h` does in integers what x86 does in x87 `long double`. The crashfs power-loss runs, the
  hung-volume tests, `make jepsen`, `make dist` and the sanitizers stay Linux only. The Linux build is unchanged:
  the macOS code is behind `__APPLE__` or in inline `tsdb_*` wrappers that are the plain call on Linux, and the
  Linux objects are the same machine code as before.
- tsdbd `--lttb-max-threshold N` (default 16,777,216): the largest LTTB threshold the server computes, for
  QUERY_LTTB and QUERY_MULTI; a larger one is taken as this. A range of no more rows than that still answers every
  point. Newest-first LTTB holds its chosen points until the last is chosen, so before this a threshold near the
  rows of a series of billions held gigabytes for one request, outside `--max-request-memory`.
- **`tsdb_latest`** (embedded API in `tsdb.h`, with `tsdb_latest_opts` and `tsdb_latest_result`): the newest row of
  each of many series, optionally before a bound per series and optionally the newest row even when it is a null,
  with last-write-wins, tombstones, retention and partition classes honoured. It searches the partitions newest
  first and stops as soon as no older part can hold a newer row, so it needs no snapshot and no table. Not yet in
  `tsdbd`, the clients or the protocol. `tests/test_latest.c` compares every answer with a full scan after random
  sequences of inserts, deletes, replaces, retention, drops, class switches, flushes, merges, a reopen and a crash.
- Development tooling that does not change a running tsdb: fuzz harnesses `codecx`, `import`, `users` and `classes`
  next to the existing ones, `make fuzz FUZZ_ARCH=...` and `make asan ASAN_ARCH=...` to run them at a CPU level;
  `.github/workflows/release.yml`, which builds the tarball of a pushed tag `vX.Y.Z`, tests it and attaches it to the
  GitHub release (`docs/release.md`, `tools/release-notes.sh`); and a sans-IO replication core (`src/repl.c`) with
  its simulator (`tests/sim_repl.c`), which `make test` runs but the library does not contain. New designs, not
  built yet: `docs/design-cluster.md`, `docs/design-ha-modes.md`, `docs/design-cdc.md`, `docs/design-stats.md`,
  `docs/design-latest.md`.

### Changed

- An operator's `volume add` (and `import --create-volumes`) refuses a directory that holds a file with a name the
  volume would take for one of its own: `STATES`, `RETENTION`, `CLASSES`, a WAL segment (`wal_<16 hex>.log`), a
  part, a part's temporary, a retired part, an intent or `META.tmp`. The first open would otherwise have cut such a file of the
  user's as a torn tail, replayed it, run it or deleted it. The message names the file; nothing is written.

### Known limitations

The list of 0.9.0 below still holds, with these changes:

- `TSDB_ERR_UNAVAILABLE` (-11) and `TSDB_ERR_ABANDONED` (-14) are known to the engine and to `tsdbd`, but the C, Rust,
  Python and Java clients show them as "unknown error" with the code (-11 means: not applied, try again later).
- Latest-value lookups exist in the embedded API (`tsdb_latest`) only; there is none in `tsdbd` or the clients yet.
- Storage outage handling (`--health-domain`) is the first step: no discovery of domains from the mounts, no
  deadline for a parked request, no queue bound per volume, no metrics yet (see `docs/tasks/nfs-outage.md`).
- Merges do not retry a part with a damaged granule, and an insert into its partition goes on (the part count of the
  partition grows) until the part is restored from a backup.

## [0.9.0] - 2026-11-05

The first release. **Disk format version 8, wire protocol version 12** (the server and the clients refuse a peer
below 12 at HELLO), **embedded C ABI version 3**, series ids of 64 bits, **login required by default**. Both
formats may still change in a 0.9.x
release and freeze at 1.0 ([docs/compatibility.md](docs/compatibility.md)). Nothing was released before this
version, so there is nothing to upgrade from; if you ran a development build, a database of disk format 5, 6 or
7 is refused at open and is carried over with `tsdb export` (the build that wrote it) and `tsdb import` (this
one), see "Changed".

### Added

The engine (the library, `libtsdb`)
- Series of (time in milliseconds, value) points. A series has one value type: float32, float64, int64, bool
  or state (a text label from a small set). Any value may be null.
- MergeTree-style storage: every insert writes an immutable, sorted part; parts live in time partitions and
  are merged in the background. Last write wins: a value written for a time that exists replaces it.
- Compression chosen per block (fixed point, XOR, byte split, bit-packed deltas, a dictionary for readings that
  take few distinct values, ZSTD): about 1.2 bytes per row on mixed sensor data. A float64 series fed float32
  readings (a collector that widens what the sensor gives it) stores them as float32, exactly. Pressures
  converted to psi come down to 0.14-0.20 times their float64 size with the dictionary codec. Background merges
  try the better codecs, re-encode what inserts wrote and rewrite a partition's only insert-written part once the
  partition is idle (`merge_idle_s`) or on `tsdb_optimize`.
- Inserts and memtable flushes write their parts without zstd (`tsdb_options.insert_zstd_level`, default 0;
  tsdbd `--insert-zstd-level`): background merges re-encode every such part at `zstd_level` anyway, with others
  or alone once its partition is idle, so the parts at rest are the same bytes, and only parts not merged yet
  are larger (1.68 instead of 1.12 bytes per row for a CPU temperature). Where nothing merges
  (`merge_idle_s = -1`, `merge_threads = 0`) parts stay as inserted. `-1` writes inserts at `zstd_level`. Also in
  the Rust, Python and Java embedded bindings (`insert_zstd_level`, `insertZstdLevel`);
  `tsdb-bench --insert-zstd N --merge-threads N --fresh`, `insert-bench` and `scale-bench load --insert-zstd N`.
- Partition classes: each series gets partitions of 1, 7, 28 or 364 base partitions (`partition_ms`), the
  shortest that holds at least eight granules of it, chosen from its rate (a fresh series from its first batch,
  others when an idle merge measures them; a band widened 2x either way keeps them from flapping, and a class is
  at most an eighth of the series' retention). Granules still end at every day edge, so bucketed queries answer
  from granule statistics wherever day partitions do, with the same answers bit for bit: a disk temperature read
  every minute takes 0.268 instead of 0.326 bytes per reading, and every query measured is as fast as with day
  partitions or faster; fast series keep their day partitions exactly. `tsdb_options.partition_classes`
  (default 1, fixed at creation), `tsdb_series_class`, `tsdb_part_info.cls`, `.start_ms` and `.end_ms`;
  `tsdb <dir> parts` and `series` print the class. `tsdb_drop_before` drops whole partitions as before, so a slow
  series' rows go in steps of its partition length (7, 28 or 364 days); retention still clamps queries to the
  exact cutoff.
- Queries: raw points (oldest or newest first, with a limit), count/sum/min/max/average per bucket (fixed
  buckets and calendar buckets in a time zone), the spread (range, standard deviation, variance), LTTB
  downsampling and time-weighted averages (step or linear, a gap limit). Aggregates come from stored granule
  statistics wherever a bucket covers whole granules; sums of int64 are exact. Every kind takes an order and a
  limit, and bucketed queries with a limit stop early.
- Many series in one request: `tsdb_query_multi` and `tsdb_query_multi_collect` answer values, buckets (fixed,
  calendar in any zone, bounds), the spread, TWA and LTTB for up to 65,536 series, in the order asked, every
  series read from one snapshot (an insert into several series is seen by all of them or none), with a status per
  series. The series run in parallel under a memory budget (results held ahead of their turn are charged to
  `--max-request-memory`; large series run at their turn, streaming).
- One query scheduler per database (`query_threads`, `request_threads`): concurrent queries share the CPUs by
  groups capped at `request_threads`, the calling thread always works, and decoding of series already started
  comes before new series. The threads one query step uses follow the rows it decodes, so small reads stay on one
  thread; answers are bit-identical at any thread count.
- Range delete and atomic replace of one series, retention per series, several volumes (directories or mounts).
- Durable writes: a write is on disk when it returns (WAL with group commit, or a periodic sync). A failed
  fsync is fail-stop. Tested by killing the process, simulated power loss and injected I/O errors (`make jepsen`).
- One writer and any number of read-only followers on other hosts over NFS.
- SIMD aggregation kernels for float32, float64, int64 and bool at four CPU levels (AVX-512, AVX2, AVX,
  SSE4.2, and a scalar fallback) that give identical results on every CPU.
- tsdbd says at startup when a database or volume directory is on ext2/3/4 or XFS and
  `--verify-checksums` is off: those file systems keep no checksums of the data, so a damaged or misdirected
  data block would be read as wrong values. Turn the option on there, or use a file system that checks its data
  (ZFS).

The server (`tsdbd`)
- One event loop on io_uring or epoll (epoll is chosen by itself where io_uring is disabled, as on AlmaLinux 10
  and RHEL 10).
- Compressed, packed inserts; the clients send about 6 bytes per point.
- QUERY_MULTI answers many series in one request; CANCEL stops a running request.
- Prometheus metrics on a port of their own (`--metrics-port`, docs/metrics.md), including
  `tsdb_multi_series_total`, `tsdb_multi_deferred_total`, `tsdb_requests_total{op="query_multi"}` and
  `tsdb_pinned_index_bytes`.
- Options: `--port 0 --port-file FILE` for scripts and tests, `--version`, `--query-threads N` (threads for
  queries, default one per CPU), `--request-threads N` (at most this many for one request, default all),
  `--multi-max-rows N` (rows or buckets of one QUERY_MULTI, default 268,435,456), `--granule-rows N` (at
  creation), `--merge-idle-s N`, `--flush-interval-ms N`, `--pin-index`, `--insert-zstd-level N`.
- `--exit-with-parent`: the server stops as on SIGTERM (clean close, memtable flushed, status 0) once the
  process that started it is gone, checked once a second; it refuses to start if the parent is already init, and
  with `--exit-with-parent=PID` if the parent is not PID (a launcher that died before tsdbd read its options, under
  a subreaper such as `systemd --user`). Every test harness, the client test helpers and the benchmark scripts pass
  it with their own pid, so a test killed by a timeout no longer leaves its server running.

Logins and access control (protocol 12, [docs/design-auth.md](docs/design-auth.md))
- tsdbd lets in only clients that log in with SCRAM-SHA-256, as PostgreSQL does: the password never crosses the
  network, both sides prove themselves, and the server keeps only a salted verifier per user in `USERS` in the
  database directory (append-only checksummed records, mode 0600, fail-stop on a failed write or sync). An
  unknown user fails exactly like a wrong password. Before login a connection may send only the login messages
  (at most 4 KiB each) and must finish within `--login-timeout` (10 s).
- Rights per user: `read`, `insert`, `delete` and `admin` (`write` is insert and delete) on `all` series, a name
  prefix (`prefix:acme/`, a tenant), one series (`name:N`) or a glob (`glob:acme/*/temp?`: `*` any bytes, `?` one
  byte). Grants only add. A superuser may do everything. A series a user holds no right on answers like a
  missing one (`TSDB_ERR_DENIED` for ids, whether they exist or not); QUERY_MULTI answers the series it may and
  gives each denied one its own status. Every op passes one table indexed by op number; an op missing from it is
  refused.
- Tenant admins: `admin` on a prefix lets a user create, drop and set the password of users named under it and
  grant them rights inside its own `admin` selectors, never more.
- tsdbd refuses to start without a superuser and names the command that adds one. `--no-auth` turns logins off
  (every client may do everything, a warning at start; the user admin requests are refused).
- `tsdb <dir> user add|passwd|drop|alter|grant|revoke|list|compact` edits USERS offline, also while tsdbd runs
  (it reads changes within a second); `tsdb user ... --server HOST:PORT --login NAME` does the same through tsdbd,
  for tenant admins. Passwords come from a prompt, `--password-file` or `TSDB_PASSWORD`, never from the command
  line, and only the verifier leaves the client.
- All four clients log in (C `tsdbc_connect_auth` and `tsdbc_connect_opts`; Rust `Client::connect_opts` with
  `ConnectOptions`; Python `Client(..., user=, password=)` or `password_file=`; Java `TsdbClient.connect(host, port,
  ConnectOptions)`), with a password or a password file, and manage users and grants (`whoami`, `user_create` /
  `userCreate`, `grant`, ...). PBKDF2 runs once per process: the keys are cached. Without a user they work only
  with a tsdbd started with `--no-auth`.
- New error codes `TSDB_ERR_DENIED` (-16) and `TSDB_ERR_AUTH` (-17); the metric `tsdb_logins_total{result}`.
- `examples/client.c` takes a user and a password file.
- The Pulsar sink logs in: `user` and `passwordFile` (a 0600 file on the worker host), or `password` as a Pulsar
  secret (refused in the plain config; Pulsar's default secrets provider is clear text and shows the secret like
  the config, so use it only with a real provider such as Kubernetes secrets). A refused login is retried with the sink's backoff while Pulsar holds the backlog; a
  record the user may not write (`TSDB_ERR_DENIED`) goes to the dead-letter path, the rest of its batch is written.
  `insert` on the sink's series is all the user needs.

Clients and bindings
- Clients of tsdbd in C, Rust, Python and Java, all at protocol 12, with the same operations: the spread (range,
  variance, standard deviation) in fixed, calendar and bounds buckets; time-weighted averages; LTTB; aggregates
  with an order and a limit that stop early; range delete, replace and retention; and many series in one call
  (`tsdbc_query_multi` in C; `query_{rows,buckets,spread,twa,lttb}_multi` and `query_chunks_multi` in Rust
  `Client`, `Pool` and `Db`; the same five in Python with numpy columns and `errors="raise"|"status"`;
  `query*Multi` in Java `TsdbClient` and `EmbeddedDb`). Lists longer than a request holds are split by the
  clients; local time zones are handled by the clients as for single series. A call that needs a newer server
  than the one connected fails with `Invalid` before anything is sent.
- Embedded bindings (no server): Rust `Db`, Python `Db` and Java 22+ `EmbeddedDb`, with the same operations as the
  clients, including `partition_classes`, `pin_index` / `pinIndex` and `series_class` / `seriesClass`. Embedded
  and TCP answers are compared in each client's tests.
- Batch calls, run in parallel. Rust: `Db::query_points_many`, `query_agg_many`, `insert_many` and `Pool`, a fixed
  set of connections to one tsdbd with the same three calls. Python: `Db.query_raw_many`, `query_agg_many`,
  `insert_many` and `tsdb_client.Pool`, with the GIL released for the whole batch. A failed batch raises the
  error of its lowest failing entry and names it (Rust `Error::Batch`, Python `TsdbError.index`); every entry
  below it ran. Inserts that share a series keep their list order. `Db.last_query_stats()` after a batch reports
  the totals of its queries. After a connection breaks (a server restart), a `Pool` replaces every connection
  made before it before its next use, and a query whose connection broke runs once more; inserts are not retried.
- Python returns the spread as extra numpy columns of `Buckets` and TWA as `TwaBuckets`; raw query results are
  numpy arrays over the result vectors, without a copy.
- Package names carry the company: Rust crates `intellistream-tsdb-client` and `intellistream-tsdb-sys`, Python
  distribution `intellistream-tsdb-client`; the imports stay `tsdb_client` and `tsdb_sys`, and the Java artifacts
  (`ai.intellistream:tsdb-client`) are unchanged.
- Apache Pulsar 5 sink (`connectors/pulsar`): Pulsar holds the points while tsdb is down, and a point is taken off
  Pulsar once it is on disk. A sink instance takes all series or chosen ones (`series`, `seriesPrefixes`,
  `seriesPattern`; the rest are acknowledged and counted in `tsdb_skipped_records` / `tsdb_skipped_points`), and
  `volumes` creates new series in the volume their name maps to (a volume tsdbd does not know dead-letters the
  message). It runs only with the processing guarantee `ATLEAST_ONCE` (the default), which loses nothing as tsdb
  writes are idempotent: `EFFECTIVELY_ONCE` (cumulative acknowledgements) and `ATMOST_ONCE` could take messages off
  Pulsar before tsdb has them, and the sink refuses to start with either.

The command line (`tsdb`) and tools
- `tsdb` creates series, inserts, queries, deletes, sets retention, adds volumes, shows stats, parts and series,
  and optimizes.
- `tsdb <dir> export` and `tsdb <dir> import FILE`: a versioned, checksummed stream of whole databases or some
  series (types, volumes, retention, state names, values bit for bit), for backups and for carrying a database
  to a new format version. Every later tsdb reads every stream an earlier one wrote
  ([docs/export-format.md](docs/export-format.md)). An import from a file is checked whole before anything is
  written; `--skip-existing`, `--merge`, `--replace`, `--volume OLD=NEW`, `--volume-root OLD=NEW`,
  `--create-volumes`, `--no-retention`, `--check`. An import into a new database is built beside it, synced once
  at the end and then renamed to its name, so an interrupted one leaves no database under that name.
- `make jepsen` fault injection (kill -9, simulated power loss, pauses, I/O errors) on two workloads, `kv` and
  `classes` (series whose rate changes switch classes under every nemesis while large inserts, replaces and
  deletes race the switches), and two more nemeses, `misdirect` (the crashfs shim sends writes and reads of
  database files to other offsets or files and swaps pages of a mapped part, with `--verify-checksums` and
  without) and `rot` (bits flipped in a file, or the file truncated, while the server is down). The claim they
  check: tsdb answers correctly or reports the damage. Known limits, counted apart: without `--verify-checksums`
  damaged part data reads as wrong values, and damage to the last record of the WAL or an append-only file is cut
  as a torn tail ("Damage on disk" in docs/design.md). The weekly CI run makes every jepsen run 5 minutes long
  (`JEPSEN_SECONDS`).
- Benchmarks: `tsdbc-bench query --multi K`, TWA and whole-range classes; `bench/dbcmp` `tsdbq`/`embq`
  `--batch K`, `twa` and qps classes `NAME.bK`; `build/threads-bench`, one thread against many per work size.

Portable build and project files
- `make dist` makes a tarball (`tsdb-0.9.0-x86_64-linux.tar.gz`) for x86-64-v2 CPUs and newer, with glibc 2.34 or
  newer, and the test suite that runs from it. It is checked on AlmaLinux 10 (Podman) and under an emulated
  Westmere and Sandy Bridge. It carries `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES` (zstd and liburing are
  linked into its binaries). So do the other packages: the Python wheel and sdist (`THIRD-PARTY-NOTICES` of the
  linked Rust crates and zstd, made by `tools/gen-third-party.py`; maturin 1.9 or newer builds it), the Rust
  crates, the two Java jars (`META-INF`) and the Pulsar NAR (also zstd-jni, zstd and Jackson). `make
  test-licenses`, part of `make test-clients`, fails when a package lacks them.
- `make integration-tests` runs tsdbd, the CLI and all clients end to end.
- One version: the file `VERSION`; `make check-version` makes sure the header, the binaries and the client
  manifests agree.
- `CONTRIBUTING.md`, `SECURITY.md` and `docs/licenses.md` (the licenses of everything tsdb builds on).
- Documentation: the README compares tsdb with the other databases in a few charts (drawn by
  `tools/readme-charts.py` from `tools/readme-charts.json`), [BENCHMARKS.md](BENCHMARKS.md) holds the full 0.9.0
  comparison, and `site/` is a documentation site published to GitHub Pages (`.github/workflows/pages.yml`).
  `make check-site` (part of `make test`) checks that the site is well formed, its links work, its code blocks
  are the tested programs and its chart numbers are in the published results.

### Changed

For anyone who ran a development build; a first install is not affected.
- **Disk format 8.** Series ids are 64-bit: `(volume + 1) << 32 | local`, the volume's number plus one in the high
  half, the series' index in that volume's `SERIES` file in the low half (`tsdb_sid`, `TSDB_SID`,
  `TSDB_SID_VOLUME`, `TSDB_SID_LOCAL`, `TSDB_SID_NONE`), so routing a series to its volume is a shift. The root
  file `CATALOG` is gone (it only routed a dense 32-bit id to its volume): a series is created with one append to
  its volume's `SERIES` file instead of two, and nothing is read from or kept for the root at an open (at a
  billion series, a 16 GB file and 16-32 GB of memory less). Each volume's `META` holds its number (`volume=N`, 0
  in the database directory), which the open checks against the line of `VOLUMES` the directory is on
  (`TSDB_ERR_CORRUPT`, with a line that names both), so a cached id never reaches another volume's series.
  Nothing costs a bit more per row. Format 8 also holds what formats 6 and 7 introduced: block headers that name
  their codec in 7 bits (the dictionary codec), part keys `4 x first base partition + class` with the `CLASSES`
  file, zstd frames without magic number and content size, and a packed granule index (each part's entries
  bit-packed at a fixed width per field, read in place from the mapping, about 32 bytes an entry instead of 160,
  behind a 64-byte footer that gives the index's length; every field decodes to the exact bits written, the
  writer checks by decoding before the part is named, and a damaged index is `TSDB_ERR_CORRUPT` at open). A year
  of minute readings of 10,000 disks takes 939 instead of 1,407 MB, the index 118 instead of 564 MB of page
  cache, an open 24 instead of 58 ms warm and 77 instead of 341 ms cold; the 100 M-row mixed set 1.14 instead of
  1.17 bytes a row. Queries answered from granule statistics run 1.4 to 1.8 times faster than with the 160-byte
  entries; answers are identical.
- **A database of another disk format is refused at open**, with a message that names both versions (`tsdb: the
  database has disk format version 7; tsdb 0.9.0 reads version 8 only. ...`). Carry it over with `tsdb export`
  (the build that wrote it) and `tsdb import` (this one); every series then gets its class on its first batch.
  The export stream is unchanged, version 1: it names series, never ids, so every stream written so far imports
  as before.
- **Wire protocol 12** (11 widened the ids, 12 added login). Every series id in a request or reply is a `u64`, and
  `tsdbd` and the clients refuse a peer below 12 at HELLO (`TSDB_ERR_INVALID`; there is no operation that a
  version-10 peer and this one could both do safely). The two version-1 aggregate operations QUERY_AGG and
  QUERY_GROUP are removed: QUERY_BUCKETS and QUERY_BOUNDS answer the same queries. Ids cost 4 bytes more per
  request (nothing more where a reserved word took it, about nothing in packed inserts). HELLO's reply says
  whether the server wants a login. tsdbd no longer starts on a database without a superuser: add one with
  `tsdb <dir> user add NAME --superuser`, or start it with `--no-auth` to keep the old open behaviour (tests and
  try-outs only). Clients connecting without a user work only with `--no-auth`; with one they log in. Upgrade the
  server and its clients together.
- **Series ids in the programming interfaces.** Every id is at least 2^32 and 0 is never one, so an id cut to 32
  bits in a cast, a Java `int`, a Rust `as u32` or a Python `"I"` is refused instead of being taken for another
  series: a query of such an id (or of one with any of bits 48-63 set) answers `TSDB_ERR_INVALID` rather than an
  empty range, while a well formed id that is unknown still has no rows. Ids are not dense any more: list series
  with `tsdb_series_foreach` (C), `series_ids` (Rust and Python `Db`) or `seriesIds` (Java `EmbeddedDb`), or
  `TSDB_SID(v, 0 .. tsdb_volume_series_count(db, v) - 1)`, never `0 .. tsdb_series_count() - 1` (which now returns
  `uint64_t`); look ids up by name. `tsdb_abi_version` (3 in this release) names the layout of the C ABI the
  bindings check.
  C `tsdb_sid`, Rust `SeriesId` (`u64`), Python `int` (numpy `uint64` or `int64` columns; a 32-bit column raises
  `TypeError`), Java `long` and `long[]` (the methods that took `int` ids are gone, not overloaded). `tsdb series`
  prints the id. The Pulsar sink keeps its ids as `long`.
- Series creation no longer takes one lock for the whole database across the append and its fsync: a volume on a
  slow or hung file system holds up only the names being created in it, while other names, in any volume, and
  lookups go on. Two creators of one name still take turns and get one series. Opening a volume that was added
  while the database is open, and a follower's read of a volume's `SERIES`, hold no lock that lookups and
  creations in the other volumes need either.
- `verify_checksums` (`--verify-checksums`) now means the granule checksums only. The new option `pin_index`
  (`tsdbd --pin-index`, off by default) reads the granule index and tombstones from the copy that was checked at
  open, so damage that appears later cannot change answers; the copy costs about 32 to 51 bytes of RAM per
  granule in every process that opens the part (at least one granule per series and partition: 51 GB per
  partition generation at 10^9 series), reported as `tsdb_volume_stats.pinned_bytes` and the metric
  `tsdb_pinned_index_bytes`. `pin_index` sits in what was padding of `tsdb_options` (ABI 2), and
  `insert_zstd_level` (see "Added") follows it and grows the struct by 8 bytes: `TSDB_ABI_VERSION` is 3.
- Hour cuts: a granule ends at a UTC hour edge when at least 3000 rows of its series lie in the hour before it
  (a row every 1.2 s or faster), so hourly buckets and the days of zones a whole number of hours from UTC
  (Europe/Oslo, but not Asia/Kolkata) are answered from granule statistics instead of decoding the granules around
  each edge. The rule counts the rows of the hour: a series below 3000 rows an hour keeps its granules as before,
  whether steady or in bursts (a burst is as fast as 1 Hz while it lasts), and whatever its class. Cost: more,
  smaller granules, each extra one 60-320 bytes depending on the codecs (an index entry, the frames of a timestamp
  and a value block, compression context): 1-6% more bytes on the 1 Hz data sets measured (15%, or 0.018 bytes a
  row, on a nearly constant one), up to 10% just above the threshold (1.1-1.2 s), under 1% for faster series and
  none for slower ones. The layout changes, not the format (no `FORMAT_VERSION` bump): readers take any granule
  boundaries, and a database written before keeps its granules until merges rewrite them. Merges copy granules
  only where a writer given their rows would cut them the same way, so a merged layout stays put. Answers stay the
  same; only float sums (float32 and float64, summed in doubles per granule), variances and TWA areas can differ
  in the last bits.
- Python: aggregate queries (`query_agg`, `query_group`, `query_bounds` and the rest) return `Buckets` instead of
  a list of `Bucket`: a `collections.abc.Sequence` of `Bucket` (indexing, slices, iteration, `+` and `==` with
  lists, `count()`, `index()`), with a numpy column per field (`start`, `counts`, `sum`, `min`, `max`, `avg`,
  `isum`, `imin`, `imax`). It is no longer a `list` (`isinstance(b, list)` is false, `append` is gone) and does
  not pickle. Numpy input arrays are copied once with the GIL released instead of while holding it.
- Rust `Db::query_points_many` and `query_agg_many` (and Python's `Db` batch calls over them) run through
  `tsdb_query_multi` instead of one query per rayon task.
- `tsdb_open` refuses a `value_codec` other than `TSDB_VALUES_AUTO`, `_GORILLA` and `_CHIMP` (`TSDB_ERR_INVALID`)
  instead of taking any other number as AUTO.
- Values: float32 and float64 decode faster (fixed point with a 4-lane prefix sum), and AUTO adds packed bit
  deltas (both types) and ALP_rd (float64) as candidates, ranked on a ZSTD-compressed sample of the block.

### Fixed

Writes
- Last write wins in real time. A small write (WAL and memtable) to a key of a large insert, replace or delete could be
  acknowledged while that large write was between its memtable flush and its publish, and then kept winning over it,
  because memtable rows rank above every part: a read showed the small write and, at another key, the large write
  not yet; the large write was acknowledged; reads still showed the small write. Such a small write now waits until
  the large write is published (only one whose rows fall inside the large write's keys: a timestamp range per
  bucket of series), and wins as the newer write. `make jepsen` checks this with a fifth rule: a write acknowledged
  before a read saw another write not yet in effect is older than it, although the two overlap in time; the old
  server failed it in every 20-second run. The wait lasts as long as the large write (seconds for hundreds of
  millions of rows), and while overlapping large writes follow each other without a gap, a small write into their
  keys waits for all of them. A write that waited is refused when the engine failed meanwhile. tsdbd counts such a
  waiting request and the large write it waits for as waiting workers and starts extra ones, so queries and other
  writes keep being answered; embedded servers can do the same with the new `tsdb_set_write_wait_hook`. tsdbd starts
  at most 256 extra workers: more than `--workers` + 256 connections writing into a large write's keys at once
  stall it again until the publish, so keep the writing connections during a big backfill below that, or split the
  backfill into smaller calls (docs/design.md, the server section).

Damage and recovery
- A damaged `SERIES`, `STATES`, `RETENTION` or `CLASSES` record, a misdirected write or a read that returned other
  bytes than the disk holds is no longer taken for a torn tail, which lost acknowledged data silently.
  - The last `SERIES` or `STATES` record, damaged at rest, was cut as a torn tail although a `RETENTION` or
    `CLASSES` record, or a syncing writer's MANIFEST, proved it acknowledged: the next series got its id and
    inherited its retention limit (its rows hidden at once and dropped by merges) or class, or a new state name
    its code. The open now refuses that with `TSDB_ERR_CORRUPT` and changes nothing. MANIFEST has a new optional
    line `states N`.
  - A misdirected write over the last records of `SERIES` made the open cut them as a torn tail, and every series
    they named, with all its rows in parts, was gone without an error. The open now fails with
    `TSDB_ERR_CORRUPT` when a part holds a series `SERIES` does not have (one comparison per part). Likewise for
    rows still in the WAL: the replay skipped rows of a series `SERIES` lacked as those of a series never
    created. It now fails the open with `TSDB_ERR_CORRUPT`, naming the segment, the offset and the series, and
    keeps the segment.
  - A read at the open that returned other bytes than the disk holds could make the last records of the WAL,
    `SERIES`, `STATES`, `RETENTION` or `CLASSES` look torn, and the open cut them: acknowledged rows lost to a
    fault that does not repeat. The tail is now read again from the disk before it is cut; if the reads differ,
    the open fails with `TSDB_ERR_IO` and cuts nothing. Every cut is logged ("cutting a torn tail").
  - A torn end of a WAL segment that a later segment with records follows was cut like a torn append, losing
    acknowledged rows; a power loss cannot do that (each segment is synced before the next takes rows). The open
    now refuses it and names the segment to move aside.
  - `RETENTION` logged `cutting a torn tail` without cutting anything (also in an open that was then refused); its
    torn tail is now cut, after the open's checks, like the others.
- A refused open no longer changes any file: the torn tails of `SERIES`, `STATES` and `CLASSES` were cut before
  the checks that then proved the cut record was acknowledged and refused the open, so a record with one flipped
  bit, which could have been repaired, was gone for good. A writer now cuts torn tails only after the parts,
  `CLASSES` and every WAL segment passed their checks (the WAL is checked whole before the replay writes
  anything), and the refusal names the way out (restore `SERIES`, or move a segment aside; `tsdb export` is
  refused the same way).
- A misdirected write into the granule index of a part the server had open could crash a merge (SIGSEGV; found by
  the jepsen nemesis `misdirect`): the index is checked at the open and then read in the part's mapping, where
  later damage shows. Every entry number taken from the index now stays inside it. Without `pin_index` such
  damage gives wrong answers, like damage to data, and a merge hashes each source's index again first and fails
  on a mismatch rather than write damaged statistics under a fresh hash (before, it copied them, and the damage
  became undetectable); measuring series rates skips runs naming a series beyond `SERIES`, so no class record is
  written for it.
- A retention drop decided from series ids read in the part's mapping: an index damaged after the open could make
  a partition of a series without retention look expired, and it was retired for good. The drop checks each
  part's index first. A part that fails that check, a merge's or the rate measurement's is now marked: logged
  once (a failing merge was retried and logged every second, forever), counted in `broken_parts`, and left out
  of merge selection, so retention drops and the other merges go on (before, nothing expired was ever dropped
  again); `tsdb_optimize` merges the rest and returns `TSDB_ERR_CORRUPT`.
- A WAL segment whose unlink failed after its flush (an I/O error) was counted as removed: an empty merge output
  that keeps the segment's deleted rows from replaying could then be dropped, and the rows came back at the next
  open. The segment now stays counted until a later flush removes it.
- Opening a database no longer deletes a user's files in a volume directory (a volume may be a directory that held
  other files before it was added). The cleanup at open used to remove every name starting with `tmp_` and read
  (then remove) every `txn_*` file as an insert intent; it now touches only the exact names tsdb writes
  (`tmp_<pid>_<n>.part`, `META.tmp`, `txn_<pid>_<n>.intent`), and likewise reads as parts, WAL segments and
  retired parts only `p<partition>_<min>_<max>_<level>.part`, `wal_<16 hex digits>.log` and `retired_` + a part
  name, so a file like `plan.part` or `wal_notes.log` no longer fails the open or gets replayed.
- A part file changed on disk while a query read it could corrupt memory (found by the release security review).
  Without `pin_index` (the default) queries read a part's granule index straight from the file's mapping. A raw
  query sized its buffer from a granule's row count, read the count again, and decoded that many rows: if the count
  had grown in between (a misdirected write, a page read again from a failing disk or from NFS), the rows were
  written past the end of the buffer. A merge had the same gap. Every decode is now bounded by the buffer it
  writes, and a merge refuses a count that changed; such damage now gives `TSDB_ERR_CORRUPT` or wrong answers,
  as documented, never a write outside a buffer. The cost is one comparison per granule.
- One damaged byte in `VOLUMES` could make volumes disappear and their numbers be reused (found by the release
  security review). A NUL byte inside a line, a lost newline between two lines, or two damaged lines made the open
  take the rest of the file for a half-written last line and cut it, with no error: the series in those volumes
  were gone, and the next `volume add` got a cut volume's number, so series ids that clients kept pointed at the
  new volume's series. Now only a tail with no newline at all (an add that died while writing its line) is cut;
  any other damage fails the open, a read-only open and `volume add` with `TSDB_ERR_CORRUPT`, naming the line, and
  the file is left as it was for the operator to repair.
- A crafted `RETENTION` or `CLASSES` record naming series 4294967295 made the open reserve 32 GB or 4 GB of memory
  before it noticed that the series does not exist. Records of series that `SERIES` does not have are no longer
  applied: a writer's open fails on them as before, and a reader leaves them for a later refresh (the writer may
  have created the series a moment ago).

Followers
- Followers, and readers that open through MANIFEST (volumes included), served series whose `SERIES` record was
  damaged as never created: open fine, lookups NOTFOUND, rows unreachable, no error and no log line. After
  reading `SERIES` again they now need as many records as MANIFEST counts and each new part may name no series
  beyond them; otherwise the previous view stays (like for a broken part) and, after the follower patience, the
  refresh or the open fails with `TSDB_ERR_CORRUPT`, logged once and counted in `broken_parts`. A writer without
  fsync says so in MANIFEST (`fsync 0`, a new optional line): a follower then waits a minute for a missing
  record, since NFS can show the writer's files in any order, and says it may still be on its way instead of
  calling `SERIES` damaged. A reader that opens while a writer creates series reads `SERIES` again first (before,
  it could miss the new series' rows); a follower that still lacks the series after reading `SERIES` again stops
  before that WAL record and reports the damage once it outlasts its patience for NFS.
- Followers timed their patience for a series `SERIES` lacks from the start of a gap, which a writer creating
  series steadily keeps open: after a minute (or 2 s for a syncing writer) every refresh failed and the view
  froze. The patience now runs per record, and the views of a writer without fsync are taken meanwhile. A
  follower also notices `SERIES` cut below, or rewritten at, the last record it read.
- A follower could stop seeing new rows after the writer restarted. Every writer numbers its MANIFEST versions
  from 1 again, and a follower took a MANIFEST only when its version changed: after writers that died before they
  flushed (kill -9, power loss), the new writer could publish the version the follower already had, and the
  follower kept the old parts and the old WAL segment until the writer published again, which an idle writer may
  never do. A follower now also compares the MANIFEST's checksum (`make jepsen`, power nemesis: "the follower
  does not catch up").

Values and queries
- The float32 and float64 codecs no longer depend on the CPU's rounding mode. A host application that had set
  `FE_UPWARD`, `FE_DOWNWARD` or `FE_TOWARDZERO` (the engine never does) stored other bytes than at
  round-to-nearest, and in some blocks values that decoded one unit in the last place off at round-to-nearest,
  silently; a block read in another mode (the mode is per thread, and the scheduler's helper threads keep the one
  their creator had when the database was opened) gave other values. The block entry points now run with
  round-to-nearest and restore the caller's mode (a few register accesses per block: no measurable cost).
  Granule statistics (sums, time-weighted areas) are still computed in the calling thread's mode.
- Python: big-endian numpy arrays (`>i8`, `>f4`) were read byte-swapped (PyO3 takes '>' for native on
  little-endian machines); they are now converted element by element.

Memory and speed
- Large inserts (8 million rows or more per call) faulted their memory in again on every call since the 64-bit
  series ids: `tsdb_insert` narrowed the ids into a fresh allocation per batch, which glibc serves from a new
  mmap above 32 MB (4 bytes per row; a batch spanning volumes, 16). They now go to the inserting thread's
  grow-only buffers, as the engine's own sort buffers do: 32 billion rows loaded with 1 million page faults
  instead of 32 million, and a third of the system time. `scale-bench load` writes the ids as it generates the
  rows rather than in a second pass.
- tsdbd no longer copies a large reply whole into a connection's output buffer: a bucket, aggregate or TWA reply
  of up to a frame (256 MB) went in at once, in a buffer rounded up to 512 MB, while the engine's result was
  still held, so one request peaked at about 500 MB of server memory. Replies now reach the output in pieces of
  1 MB, the worker waiting while the output is over its cap (8 MB), as for streamed rows; the peak is the result
  plus a few MB. The wire format is unchanged.

Import
- `tsdb import` refuses a target directory that has files in it and holds no database, before writing anything,
  also through a symlink to one (it used to build the database among those files). A missing or empty directory
  and an existing database are imported into as before; volume directories are unchanged.
- `tsdb import` into a new database: after a failure it no longer tells you to delete a volume directory that was
  there before the import (it may hold your other files). It removes only the files the import provably wrote in
  the volumes it added (tsdb's own file names, not there before), leaves and names anything else, also what
  another process put there meanwhile, and removes the directory only if the import made it and it is empty then;
  a sibling left by a kill names the tsdb files to remove. A target that is a symlink to an empty directory, or a
  dangling symlink, imports at the end of the link instead of failing at the final rename.

### Security hardening (input safety)

- C client: a QUERY_MULTI reply whose series changed value type between its chunks (a bool chunk, then a float64
  one) made `tsdbc_query_multi_collect` write past the series' buffer, which it had sized by the first chunk's type:
  a heap overflow of server-chosen length and bytes. tsdbd never sends that (a series' type is fixed), but a
  hostile server or a man in the middle could. A chunk of another type within one series is now
  `TSDB_ERR_CORRUPT` and breaks the connection, in multi replies and in single-series QUERY_VALUES / LTTB streams
  alike (as the Rust and Java clients already did), and the collector refuses such rows on its own as well. The
  local-period bucket calls no longer overflow `hi + 1` when a server reports a row at `INT64_MAX`.
- `tsdb <dir> ...` run as root on a directory another user owns no longer follows links that user planted there
  (release security review): `user add` through a dangling `USERS` link created any file (`/etc/ld.so.preload`)
  and gave it to the directory's owner, `user compact` through a `USERS.compact` link overwrote and chowned any
  file (`/etc/shadow`), and even `user list` chmodded the file a `USERS` link named. As root, every command now
  first becomes the directory's owner and group for good (setgroups, setgid, setuid; it says so in one line and
  refuses when root could come back); a directory of root's keeps root. Files named on the command line
  (`--password-file`, `export -o`, `import FILE`) are then opened as that user. `USERS` and `USERS.compact` are
  opened through the directory's descriptor with `O_NOFOLLOW` (tsdbd's reads too), `USERS.compact` is made new
  with `O_EXCL` after unlinking whatever had the name, and a `USERS` that is not a regular file or has a second
  name (a hard link) is refused before any chown or chmod.
- Volume paths chosen by a client are checked; a client can never make the server add a volume.
- Query memory follows the rows and buckets a query really has; request memory, frame size and the time to
  deliver a frame are bounded; flags and numbers on the server, CLI and clients are checked.
- Crafted files do not corrupt memory: decoders and file loaders are fuzzed, and damage is never silent (a
  damaged part or log fails the open, it is not skipped).

### Known limitations

- No TLS: data, queries and login exchanges travel in the clear (a recorded login lets an eavesdropper guess
  the password offline), and the metrics port has no login. tsdbd belongs on a trusted network only. Put it
  behind a firewall or a private network, and bind it to the interface you mean (`--listen`, `--metrics-bind`).
  The library and `tsdb <dir>` open the files directly and need no login: the file permissions decide. State
  names are shared by the series of a volume, across tenants, and a series id shows how many series its volume
  held before (SECURITY.md).
- One writing server per database. Read-only followers can read the same directory; automatic failover is
  designed (docs/ha.md) but not built.
- The disk format (version 8) and the wire protocol (version 12) may still change in a 0.9.x release, with
  export and import, or a migration, to carry data over. A database of another format version is refused
  when it is opened, with a message that says so. They freeze at 1.0. See docs/compatibility.md.
- Client and server of different 0.9.x releases may not understand each other: use the same release on both
  sides.
- Without `--verify-checksums` a damaged granule reads as wrong values (on ext4 and XFS, which keep no data
  checksums, turn it on), and without `--pin-index` damage to a part's index after the open can give wrong
  answers.
- No SQL, no tables, no latest-value lookups or series metadata (the industrial context belongs to the
  DataHub platform).
- Linux on x86-64 only for the tarball; other platforms build from source.
