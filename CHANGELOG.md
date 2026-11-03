# Changelog

All notable changes to tsdb are listed here, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the versions follow
[Semantic Versioning](https://semver.org/), with the exception described in
[docs/compatibility.md](docs/compatibility.md): before 1.0, a 0.9.x release may still change the
disk format and the wire protocol.

## [Unreleased]

### Added
- tsdbd says at startup when a database or volume directory is on ext2/3/4 or XFS and `--verify-checksums` is
  off: those file systems keep no checksums of the data, so a damaged or misdirected data block would be read as
  wrong values. Turn the option on there, or use a file system that checks its data (ZFS).
- `CONTRIBUTING.md`, `SECURITY.md` and `docs/licenses.md` (the licenses of everything tsdb builds on). The `make dist`
  tarball now carries `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES` (zstd and liburing are linked into its binaries).
- Partition classes: each series gets partitions of 1, 7, 28 or 364 base partitions (`partition_ms`), the
  shortest that holds at least eight granules of it, chosen from its rate (a fresh series from its first batch,
  others when an idle merge measures them; a band widened 2x either way keeps them from flapping, and a class is
  at most an eighth of the series' retention). In class partitions granules still end at every day edge, so
  bucketed queries answer from granule statistics wherever day partitions do, with the same answers bit for bit:
  a disk temperature read every minute takes 0.268 instead of 0.326 bytes per reading, and every query measured
  is as fast as with day partitions or faster; fast series keep their day partitions exactly. `tsdb_options.partition_classes` (default 1, fixed at creation), `tsdb_series_class`,
  `tsdb_part_info.cls`, `.start_ms` and `.end_ms`; `partition_classes` and the part fields in the Rust, Python
  and Java embedded bindings (`series_class`, `seriesClass`); `tsdb <dir> parts` and `series` print the class.
  tsdbd `--granule-rows N` (at creation), `--merge-idle-s N` and `--flush-interval-ms N`. `make jepsen` runs a
  second workload, `classes`: series whose rate changes switch classes under every nemesis while large inserts,
  replaces and deletes race the switches. Every placement (a large insert, a replace, a flush) reads one frozen
  class history; the part writer refuses rows out of (series, time) order, and the open checks every part
  against `CLASSES` (`TSDB_ERR_CORRUPT` with a log line).
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
- Disk format 7: part keys are `4 x first base partition + class`, the new `CLASSES` file, zstd frames
  without magic number and content size, and a packed granule index: each part's entries are bit-packed at a
  fixed width per field (a frame of reference per field and part: a base, a slope for offsets and starts that
  grow, shared trailing zeros, float32 bits for float statistics a float32 holds exactly, derived TWA areas as a
  flag) and read in place from the mapping, about 32 bytes an entry instead of 160, behind a 64-byte footer that
  gives the index's length. Every field decodes to the exact bits written (the writer checks by decoding before
  the part is named; a damaged index is `TSDB_ERR_CORRUPT` at open). A year of minute readings of 10,000 disks:
  939 instead of 1,407 MB (0.179 instead of 0.268 bytes a reading), the index 118 instead of 564 MB of page
  cache, an open in 24 instead of 58 ms warm and 77 instead of 341 ms cold; the 100 M-row mixed set 1.14 instead
  of 1.17 bytes a row. Queries answered from granule statistics run faster: whole-range aggregates 1.5 to 1.8
  times, day buckets over a year 1.4 to 1.7 times (one thread), TWA through tsdbd 1.48 times (1 connection) and
  1.63 times (32); queries that decode rows and inserts are unchanged (within noise). Answers are identical.
  A version-6 database is refused at open; carry it over with
  `tsdb export` (old release) and `tsdb import` (this one), which gives every series its class on its first
  batch. `tsdb_drop_before` drops whole partitions as before, so a slow series' rows go in steps of its
  partition length (7, 28 or 364 days); retention still clamps queries to the exact cutoff.
- Series ids are 64-bit. A series' id is `(volume + 1) << 32 | local`: the volume's number plus one in the high
  half, the series' index in that volume's `SERIES` file in the low half (`tsdb_sid`, `TSDB_SID`,
  `TSDB_SID_VOLUME`, `TSDB_SID_LOCAL`, `TSDB_SID_NONE`). Every id is at least 2^32 and 0 is never one, so
  an id cut to 32 bits in a cast, a Java `int`, a Rust `as u32` or a Python `"I"` is refused instead of being
  taken for another series; a query of such an id (or of one with any of bits 48-63 set) answers `TSDB_ERR_INVALID`
  rather than an empty range, while a well formed id that is unknown still has no rows. Ids are not dense any
  more: list series with `tsdb_series_foreach` (C), `series_ids` (Rust and Python `Db`) or `seriesIds` (Java
  `EmbeddedDb`), or `TSDB_SID(v, 0 .. tsdb_volume_series_count(db, v) - 1)`, never `0 .. tsdb_series_count() - 1`
  (which now returns `uint64_t`); look ids up by name. `tsdb_abi_version` names the layout of the C ABI the
  bindings check. C `tsdb_sid`, Rust `SeriesId` (`u64`), Python `int` (numpy
  `uint64` or `int64` columns; a 32-bit column raises `TypeError`), Java `long` and `long[]` (the methods that took `int` ids are
  gone, not overloaded). `tsdb series` prints the id. The Pulsar sink keeps its ids as `long`. The
  benchmarks find their ids by name and keep the series number of their row files as an index into that table.
- The root file `CATALOG` is gone. It only routed a dense 32-bit id to its volume; with the volume inside the id,
  routing is a shift. A series is created with one append to its volume's `SERIES` file instead of two, and
  nothing is read from or kept for the root at an open (at a billion series, a 16 GB file and 16-32 GB of memory
  less). A format-7 database, which has one, is refused at open (below).
- Series creation no longer takes one lock for the whole database across the append and its fsync: a volume on a
  slow or hung file system holds up only the names being created in it, while other names, in any volume, and
  lookups go on. Two creators of one name still take turns and get one series. Opening a volume that was added
  while the database is open, and a follower's read of a volume's `SERIES`, hold no lock that lookups and
  creations in the other volumes need either.
- Disk format 8: no `CATALOG`, and each volume's `META` holds its number (`volume=N`, 0 in the database
  directory), which the open checks against the line of `VOLUMES` the directory is on (`TSDB_ERR_CORRUPT`, with a
  line that names both), so a cached id never reaches another volume's series. Nothing inside a volume changed
  (WAL, parts, granule index, `CLASSES`, `RETENTION`, `SERIES`), and nothing costs a bit more per row. A version-7
  database is refused at open, with the message that names both versions; carry it over with `tsdb export` (the
  release that wrote it) and `tsdb import` (this one). The export stream is unchanged, version 1 still: it
  names series, never ids, so every stream written so far imports as before
  ([docs/export-format.md](docs/export-format.md)).
- The wire protocol is version 11: every series id in a request or reply is a `u64`, and `tsdbd` and the
  clients refuse a peer below 11 at HELLO (`TSDB_ERR_INVALID`; there is no operation that a version-10 peer and
  this one could both do safely). The two version-1 aggregate operations QUERY_AGG and QUERY_GROUP are removed:
  QUERY_BUCKETS and QUERY_BOUNDS answer the same queries. Ids cost 4 bytes more per request
  (nothing more where a reserved word took it, about nothing in packed inserts). The gates on protocol versions
  3 to 10 in the clients are gone, since they always pass. Upgrade the server and its clients together.
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
- A follower could stop seeing new rows after the writer restarted. Every writer numbers its MANIFEST versions from 1
  again, and a follower took a MANIFEST only when its version changed: after writers that died before they flushed
  (kill -9, power loss), the new writer could publish the version the follower already had, and the follower kept
  the old parts and the old WAL segment until the writer published again, which an idle writer may never do. A
  follower now also compares the MANIFEST's checksum (`make jepsen`, power nemesis: "the follower does not catch up").
- The float32 and float64 codecs no longer depend on the CPU's rounding mode. A host application that had set
  `FE_UPWARD`, `FE_DOWNWARD` or `FE_TOWARDZERO` (the engine never does) stored other bytes than at round-to-nearest,
  and in some blocks values that decoded one unit in the last place off at round-to-nearest, silently; a block read in
  another mode (the mode is per thread, and the scheduler's helper threads keep the one their creator had when the
  database was opened) gave other values. The block entry points now run with round-to-nearest and restore the caller's mode (a few register accesses
  per block: no measurable cost). Granule statistics (sums, time-weighted areas) are still computed in the calling
  thread's mode.
- tsdbd no longer copies a large reply whole into a connection's output buffer: a bucket, aggregate or TWA reply of up
  to a frame (256 MB) went in at once, in a buffer rounded up to 512 MB, while the engine's result was still held,
  so one request peaked at about 500 MB of server memory. Replies now reach the output in pieces of 1 MB, the
  worker waiting while the output is over its cap (8 MB), as for streamed rows; the peak is the result plus a few
  MB. The wire format is unchanged.
- A WAL segment whose unlink failed after its flush (an I/O error) was counted as removed: an empty merge output
  that keeps the segment's deleted rows from replaying could then be dropped, and the rows came back at the next
  open. The segment now stays counted until a later flush removes it.
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
- Package names carry the company: Rust crates `intellistream-tsdb-client` and `intellistream-tsdb-sys`, Python
  distribution `intellistream-tsdb-client`; the imports stay `tsdb_client` and `tsdb_sys`, and the Java artifacts
  (`ai.intellistream:tsdb-client`) are unchanged.
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
