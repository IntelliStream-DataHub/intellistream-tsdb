# Changelog

All notable changes to tsdb are listed here, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the versions follow
[Semantic Versioning](https://semver.org/), with the exception described in
[docs/compatibility.md](docs/compatibility.md): before 1.0, a 0.9.x release may still change the
disk format and the wire protocol.

## [Unreleased]

### Added
- Many series in one request (protocol 10): QUERY_MULTI answers values, buckets (fixed, calendar in any zone,
  bounds), the spread, TWA and LTTB for up to 65,536 series in one request, in the order asked, every series
  read from one snapshot (an insert into several series is seen by all of them or none), with a status per
  series. The server runs the series in parallel under a memory budget (results held ahead of their turn are
  charged to `--max-request-memory`; large series run at their turn, streaming). CANCEL stops a running
  request. Embedded: `tsdb_query_multi`, `tsdb_query_multi_collect`. Clients: `tsdbc_query_multi` (C),
  `query_{rows,buckets,spread,twa,lttb}_multi` and `query_chunks_multi` (Rust `Client`, `Pool`, `Db`), the
  same five in Python with numpy columns and `errors="raise"|"status"`, `query*Multi` in Java `TsdbClient` and
  `EmbeddedDb`. Longer lists are split by the clients; local time zones are handled by the clients as for
  single series.
- tsdbd `--query-threads N` (threads for queries, default one per CPU), `--request-threads N` (at most this many
  for one request, default all) and `--multi-max-rows N` (rows or buckets of one QUERY_MULTI, default
  268,435,456); `tsdb_options.request_threads`. Metrics `tsdb_multi_series_total`, `tsdb_multi_deferred_total`
  and `tsdb_requests_total{op="query_multi"}`.
- Benchmarks: `tsdbc-bench query --multi K`, TWA and whole-range classes; `bench/dbcmp` `tsdbq`/`embq`
  `--batch K`, `twa` and qps classes `NAME.bK`; `build/threads-bench`, one thread against many per work size.
- The Rust, Python and Java clients and the three embedded bindings at protocol 9: the spread (range, variance,
  standard deviation) in fixed, calendar and bounds buckets; time-weighted averages (step or linear, a gap limit,
  order and limit); LTTB with an order and a limit; aggregates with an order and a limit that stop early. The
  embedded bindings also gain range delete, replace and retention. A call that needs a newer server than the one
  connected fails with `Invalid` before anything is sent. Embedded and TCP answers are compared in each client's
  tests. Python returns the spread as extra numpy columns of `Buckets` and TWA as `TwaBuckets`.
- Batch calls: many queries or inserts in one call, run in parallel. Rust: `Db::query_points_many`,
  `query_agg_many`, `insert_many` (on every CPU) and `Pool`, a fixed set of connections to one tsdbd with the
  same three calls. Python: `Db.query_raw_many`, `query_agg_many`, `insert_many` and `tsdb_client.Pool`, with
  the GIL released for the whole batch. A failed batch raises the error of its lowest failing entry and names
  it (Rust `Error::Batch`, Python `TsdbError.index`); every entry below it ran. Inserts that share a series keep
  their list order. `Db.last_query_stats()` after a batch reports the totals of its queries.
- `Pool`: after a connection breaks (a server restart), every connection made before it is replaced before its
  next use, and a query whose connection broke runs once more; inserts are not retried.
- `tsdb <dir> export` and `tsdb <dir> import FILE`: a versioned, checksummed stream of whole databases or some
  series (types, volumes, retention, state names, values bit for bit), for backups and for carrying a database
  to a new format version. Every later tsdb reads every stream an earlier one wrote
  ([docs/export-format.md](docs/export-format.md)). An import from a file is checked whole before anything is
  written; `--skip-existing`, `--merge`, `--replace`, `--volume OLD=NEW`, `--volume-root OLD=NEW`,
  `--create-volumes`, `--no-retention`, `--check`. An import into a new database is built beside it,
  synced once at the end and then renamed to its name, so an interrupted one leaves no database under that name.

### Changed
- The wire protocol is version 10. Clients of this release speak it and gate the new calls on the server's HELLO.
- One query scheduler per database instead of a thread pool per volume: concurrent queries share the CPUs by
  groups capped at `request_threads`, the calling thread always works, and decoding of series already started
  comes before new series.
- The threads one query step uses follow the rows it decodes (`QUERY_ROWS_PER_THREAD`, measured) instead of a
  fixed cliff at 8 granules: small reads stay on one thread. Answers are bit-identical at any thread count.
- Rust `Db::query_points_many` and `query_agg_many` (and Python's `Db` batch calls over them) run through
  `tsdb_query_multi` instead of one query per rayon task.
- Python: aggregate queries (`query_agg`, `query_group`, `query_bounds` and the rest) return `Buckets` instead of
  a list of `Bucket`: a `collections.abc.Sequence` of `Bucket` (indexing, slices, iteration, `+` and `==` with
  lists, `count()`, `index()`), with a numpy column per field (`start`, `counts`, `sum`, `min`, `max`, `avg`,
  `isum`, `imin`, `imax`). It is no longer a `list` (`isinstance(b, list)` is false, `append` is gone) and does
  not pickle. Raw query results are numpy arrays over the result vectors, without a copy.
- Python: numpy input arrays are copied once with the GIL released instead of while holding it.
- **Disk format version 6.** Block headers name their codec in 7 bits instead of 4, and a new codec, the
  dictionary (the distinct values of a block and each value's rank among them), stores readings that take few
  distinct values: pressures converted to psi come down to 0.14-0.20 times their float64 size and 0.44-0.63 times
  their float32 size, the whole UCI hydraulic rig to 0.29 times (float64) and 0.65 times (float32). Background
  merges try it (inserts keep their speed and encode as before), re-encode the granules of parts inserts wrote
  instead of copying them, and rewrite a partition's only insert-written part once the partition is idle
  (`merge_idle_s`; after real merges and retention drops) or on `tsdb_optimize`, which waits for such a rewrite in
  flight. A database of format 5 is refused at open; carry it over with
  `tsdb <dir> export` from the release that wrote it and `tsdb <newdir> import` with this one
  ([docs/compatibility.md](docs/compatibility.md)).
- Values: a float64 series fed float32 readings (a collector that widens what the sensor gives it) stores them as
  float32, exactly (every bit of every value comes back): the UCI hydraulic rig widened to float64 takes 0.45 times
  the space (as much as stored as float32) and inserts in 0.67 times the time; real doubles are unaffected.
- Values: float32 and float64 decode faster (fixed point with a 4-lane prefix sum), and AUTO adds packed bit
  deltas (both types) and ALP_rd (float64) as candidates, ranked on a ZSTD-compressed sample of the block.

### Fixed
- tsdbd no longer copies a large reply whole into a connection's output buffer: a bucket, aggregate or TWA reply of up
  to a frame (256 MB) went in at once, in a buffer rounded up to 512 MB, while the engine's result was still held,
  so one request peaked at about 500 MB of server memory. Replies now reach the output in pieces of 1 MB, the
  worker waiting while the output is over its cap (8 MB), as for streamed rows; the peak is the result plus a few
  MB. The wire format is unchanged.
- `tsdb_open` refuses a `value_codec` other than `TSDB_VALUES_AUTO`, `_GORILLA` and `_CHIMP` (`TSDB_ERR_INVALID`)
  instead of taking any other number as AUTO.
- `tsdb import` refuses a target directory that has files in it and holds no database, before writing anything,
  also through a symlink to one (it used to build the database among those files). A missing or empty directory
  and an existing database are imported into as before; volume directories are unchanged.
- Opening a database no longer deletes a user's files in a volume directory (a volume may be a directory that held
  other files before it was added). The cleanup at open used to remove every name starting with `tmp_` and read
  (then remove) every `txn_*` file as an insert intent; it now touches only the exact names tsdb writes
  (`tmp_<pid>_<n>.part`, `META.tmp`, `txn_<pid>_<n>.intent`), and likewise reads as parts, WAL segments and retired
  parts only `p<partition>_<min>_<max>_<level>.part`, `wal_<16 hex digits>.log` and `retired_` + a part name, so
  a file like `plan.part` or `wal_notes.log` no longer fails the open or gets replayed.
- `tsdb import` into a new database: after a failure it no longer tells you to delete a volume directory that was
  there before the import (it may hold your other files). It removes only the files the import provably wrote in
  the volumes it added (tsdb's own file names, not there before), leaves and names anything else, also what another
  process put there meanwhile, and removes the directory only if the import made it and it is empty then; a sibling
  left by a kill names the tsdb files to remove.
  A target that is a symlink to an empty directory, or a dangling symlink, imports at the end of the link instead
  of failing at the final rename.
- Python: big-endian numpy arrays (`>i8`, `>f4`) were read byte-swapped (PyO3 takes '>' for native on
  little-endian machines); they are now converted element by element.

## [0.9.0] - unreleased (tag v0.9.0 when the checklist in docs/release.md is done)

The first release. Disk format version 5, wire protocol version 9.

### What it contains

Engine (the library, `libtsdb`)
- Series of (time in milliseconds, value) points. A series has one value type: float32, float64, int64, bool
  or state (a text label from a small set). Any value may be null.
- MergeTree-style storage: every insert writes an immutable, sorted part; parts live in time partitions and
  are merged in the background. Last write wins: a value written for a time that exists replaces it.
- Compression chosen per block (fixed point, XOR, byte split, bit-packed deltas, ZSTD): about 1.2 bytes per
  row on mixed sensor data.
- Queries: raw points (oldest or newest first, with a limit), count/sum/min/max/average per bucket (fixed
  buckets and calendar buckets in a time zone), the spread (range, standard deviation, variance), LTTB
  downsampling and time-weighted averages. Aggregates come from stored granule statistics wherever a bucket
  covers whole granules; sums of int64 are exact.
- Range delete and atomic replace of one series, retention per series, several volumes (directories or
  mounts).
- Durable writes: a write is on disk when it returns (WAL with group commit, or a periodic sync). A failed
  fsync is fail-stop. Tested by killing the process, simulated power loss and injected I/O errors
  (`make jepsen`).
- One writer and any number of read-only followers on other hosts over NFS.
- SIMD aggregation kernels for float32, float64, int64 and bool at four CPU levels (AVX-512, AVX2, AVX,
  SSE4.2) that give identical results on every CPU.

Server (`tsdbd`)
- One event loop on io_uring or epoll (epoll is chosen by itself where io_uring is disabled, as on
  AlmaLinux 10 and RHEL 10).
- Compressed, packed inserts; the clients send about 6 bytes per point.
- Prometheus metrics on a port of their own (`--metrics-port`, docs/metrics.md).
- `--port 0 --port-file FILE` for scripts and tests; `tsdbd --version`.

Clients and bindings
- Clients of tsdbd in C, Rust, Python and Java, all at protocol 9.
- Embedded bindings (no server): Rust, Python and Java 22+, with the same operations as the clients.
- Command-line tool `tsdb` (create, import, query, delete, retention, volumes, stats, optimize).
- Apache Pulsar 5 sink (`connectors/pulsar`): Pulsar holds the points while tsdb is down, and a point is
  taken off Pulsar once it is on disk.

Security hardening (input safety)
- Volume paths chosen by a client are checked; a client can never make the server add a volume.
- Query memory follows the rows and buckets a query really has; request memory, frame size and the time to
  deliver a frame are bounded; flags and numbers on the server, CLI and clients are checked.
- Crafted files do not corrupt memory: decoders and file loaders are fuzzed, and damage is never silent
  (a damaged part or log fails the open, it is not skipped).

Portable build and tooling
- `make dist` makes a tarball (`tsdb-0.9.0-x86_64-linux.tar.gz`) for x86-64-v2 CPUs and newer, with
  glibc 2.34 or newer, and the test suite that runs from it. It is checked on AlmaLinux 10 (Podman) and
  under an emulated Westmere and Sandy Bridge.
- `make integration-tests` runs tsdbd, the CLI and all clients end to end.
- One version: the file `VERSION`; `make check-version` makes sure the header, the binaries and the client
  manifests agree.

### Known limitations

- No authentication, no TLS and no access control: tsdbd belongs on a trusted network only. Put it behind a
  firewall or a private network, and bind it to the interface you mean (`--listen`).
- One writing server per database. Read-only followers can read the same directory; automatic failover is
  designed (docs/ha.md) but not built.
- The disk format (version 5) and the wire protocol (version 9) may still change in a 0.9.x release, with
  export and import, or a migration, to carry data over. A database of another format version is refused
  when it is opened, with a message that says so. They freeze at 1.0. See docs/compatibility.md.
- Client and server of different 0.9.x releases may not understand each other: use the same release on both
  sides.
- No SQL, no tables, no latest-value lookups or series metadata (the industrial context belongs to the
  DataHub platform).
- Linux on x86-64 only for the tarball; other platforms build from source.
