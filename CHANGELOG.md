# Changelog

All notable changes to tsdb are listed here, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the versions follow
[Semantic Versioning](https://semver.org/), with the exception described in
[docs/compatibility.md](docs/compatibility.md): before 1.0, a 0.9.x release may still change the
disk format and the wire protocol.

## [Unreleased]

### Added
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
- Python: aggregate queries (`query_agg`, `query_group`, `query_bounds` and the rest) return `Buckets` instead of
  a list of `Bucket`: a `collections.abc.Sequence` of `Bucket` (indexing, slices, iteration, `+` and `==` with
  lists, `count()`, `index()`), with a numpy column per field (`start`, `counts`, `sum`, `min`, `max`, `avg`,
  `isum`, `imin`, `imax`). It is no longer a `list` (`isinstance(b, list)` is false, `append` is gone) and does
  not pickle. Raw query results are numpy arrays over the result vectors, without a copy.
- Python: numpy input arrays are copied once with the GIL released instead of while holding it.

### Fixed
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
