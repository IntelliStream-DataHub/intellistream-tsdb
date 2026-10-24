# Design

How tsdb stores and serves data. The [README](../README.md) has the quick
start, the tests and the benchmark results.

## Data model and semantics

- A database holds named **series**. A series has one value type, chosen when
  it is created: `float32` (the default), `float64`, `int64`, `bool`, or
  `state`, a string from a small set (on/off/fault, a machine mode) stored as a
  code from the volume's state dictionary (`tsdb_state`, `tsdb_state_name`).
- Every point is `(DateTime64(3) timestamp, value)`; timestamps are
  milliseconds since the epoch.
- Any value may be **null**: unknown, as with a sensor that did not answer.
  Columns carry a validity bitmap at the API (`tsdb_values`); inside, a null is
  a value the type reserves (`INT64_MIN`, state code 0, one NaN payload per
  float type), so those cannot be stored as known values. Aggregates skip
  nulls: `count` is the number of known values, and a bucket of only nulls
  is returned with count 0. Other NaNs are values: ignored by min/max, they
  propagate into sum.
- `(series, ts)` is unique, like a `ReplacingMergeTree` that is always read
  with `FINAL`: writing a timestamp that exists replaces its value (a null
  replaces too), the last write wins (within one batch, the later row), and
  queries never return duplicates.
- Time ranges are half open, `[start, end)`.
- Aggregates: count, sum, min, max, avg. Integer types (int64, bool as 0/1)
  aggregate exactly (`isum`, `imin`, `imax`; the sum saturates at the int64
  limits, `sum` holds it as a double). For states only the count is meaningful.
- Buckets are fixed lengths aligned to the epoch, like `toStartOfInterval`,
  or calendar periods (day, ISO week, month, quarter, year) in UTC
  (`tsdb_query_group`), or buckets that begin at given instants
  (`tsdb_query_bounds`).
- The engine and the server know UTC only. Local time is the caller's
  business: `tsdb_calendar_bounds` (`include/tsdb_calendar.h`, compiled into
  both the engine and the client library) turns the periods of a time zone into
  UTC bounds, and `tsdb_query_group_local` and `tsdbc_query_group` send those.
  A period runs from its first instant to the next period's first instant, so
  a local midnight that occurs twice begins the day the first time.
- All functions are thread safe. One process opens a database for writing
  (`flock`); any number may open it `read_only` or follow it (below).

## Query statistics

Every query counts what it touched, on the thread that runs it and on the
pool's workers that help it: the parts and granules with rows of the series in
the range, the granules answered from their index entry, the rows and
compressed bytes decoded, the rows read from memtables, and the rows or
buckets returned. `tsdb_last_query_stats` returns them for the calling
thread's last query; `tsdbd` keeps them per connection for `LAST_STATS`, and
the command line prints them after each query. "Rows" are the rows a query
covered, as ClickHouse counts processed rows: a granule answered from its
statistics counts its rows without reading them, which is how a scan can
report billions of rows a second.

## Storage

A database is a directory:

```
META                       format version, partition size, granule size
SERIES                     append-only series dictionary (name -> dense id, value type)
STATES                     append-only dictionary of state names (name -> code)
LOCK
MANIFEST                   the published part set, for followers
wal_<seq>.log              write-ahead log of the current memtable
p<partition>_<minblock>_<maxblock>_<level>.part
```

A database with volumes (`tsdb_series_at`) also has `VOLUMES` (their paths)
and `CATALOG` (global series id -> volume and local id) in the root; each
volume is a directory of the layout above.

Each part is one immutable file holding the rows of every series written in
one insert (or produced by one merge), sorted by `(series, ts)`:

```
header | ts block, value block, ts block, value block, ... | granule index | footer
```

A granule is up to 8192 rows of one series. Its 80-byte index entry holds
the series, min/max timestamp, row count, null count, the sum, min and max of
the known values (doubles for floats; exact for integers: int64 min/max and a
128-bit sum), block offsets and a checksum. Format version 1 parts (64-byte
entries, float32 statistics) are still read; their index is converted when
they are opened. The index is the sparse primary index of MergeTree: finding a series'
rows in a time range is a binary search, and aggregates over whole granules
are answered from the stored statistics without decompressing anything.

Parts are grouped by time partition (one day by default) and merged in the
background with ClickHouse's `SimpleMergeSelector` heuristics: only adjacent
block ranges are merged, so "newest part wins" stays well defined. A merge
copies granules that need no re-encoding as compressed bytes.

Writes are published atomically (`tmp_*.part`, fsync, rename, fsync dir).
On open, leftovers of interrupted writes are removed, parts covered by a
merged part are deleted, and broken parts are renamed to `*.broken`.

### Write paths

- **Large batches** (`wal_max_batch_rows`, default 65536 rows, and more)
  become parts directly, one per partition, like a MergeTree INSERT.
- **Small batches and single points** go to a write-ahead log and an
  in-memory memtable that queries read immediately. With `wal_sync_ms = 0`
  (the default) a write returns after `fdatasync`, and concurrent writers share
  one fsync (group commit). With `wal_sync_ms = 100` writes return at once and
  the WAL is synced every 100 ms. The memtable becomes parts when it reaches
  `buffer_rows` or `flush_interval_ms`.

Each part records the WAL segment it contains, so recovery replays a segment
only into partitions that do not already hold its rows, even after a crash in
the middle of a flush.

### Compression

Every granule picks its own encodings, with ZSTD (level 1) on top whenever it
saves a few percent:

**Timestamps**: DoubleDelta (ClickHouse's bit format) for regular clocks,
about 0.03 bits per row. When the clock jitters, DoubleDelta doubles the
noise, so the granule instead stores the **residual from a regular grid**
(`ts[i] - (ts0 + i * step)`) or **frame-of-reference deltas**, as 1 to 8 byte
words, whichever is estimated smaller.

**float32 values** (`value_codec = TSDB_VALUES_AUTO`):

1. **Fixed point** (decimal or binary): if the values are `k * 10^-e`
   (a sensor with a resolution, like 21.3 or 1.024) or `k * 2^-b` (ADC counts),
   store the integers `k` as 1/2/4-byte zigzag deltas. The encoder checks
   that every value round-trips bit for bit; the rare value that does not
   (NaN, -0.0, a glitch) is kept as an exception. The scale is chosen on
   samples and then verified on all values. Ideas from ALP (SIGMOD 2024).
2. Otherwise **Chimp** (an improved Gorilla), **bit delta** or **byte-stream
   split**. Cheap entropy estimates shortlist two candidates and both are
   compressed for real; the smaller is kept.

`TSDB_VALUES_GORILLA` or `TSDB_VALUES_CHIMP` force one XOR codec instead.
`build/codec-bench` compares Gorilla, Chimp, Chimp128, Patas, fixed point
and the simple transforms on each sensor type, with and without ZSTD:

| sensor (bits per value, codec + ZSTD) | Gorilla | Chimp | Chimp128 | Patas | tsdb AUTO |
|---|---:|---:|---:|---:|---:|
| SMART disk temperature, 0.1 degC, 60 s | 2.66 | 2.33 | 5.93 | 3.39 | **1.35** |
| CPU temperature, spikes to 100 degC, 1 s | 10.82 | 11.34 | 11.86 | 10.75 | **3.60** |
| tank pressure, 0.001 bar, 10 s | 9.09 | 8.85 | 8.79 | 7.45 | **2.01** |
| tank pressure in psi (converted, unrounded) | 23.77 | 19.57 | 19.62 | 27.24 | **19.56** |
| vibration, 16-bit ADC at 1 kHz | 21.51 | 23.58 | 30.78 | 27.87 | **14.70** |
| pipeline flow rate, 0.01 m3/h | 23.08 | 20.85 | 19.94 | 17.76 | **11.41** |
| pipeline flow totalizer, m3 | 8.68 | 6.07 | 6.10 | 8.40 | **1.67** |
| uniform random floats | 32.32 | 32.90 | 32.90 | 39.91 | **28.95** |

**float64 values**: the same fixed-point idea at double precision (the
decoder divides by the power of ten, which is correctly rounded, so a value
that came from decimal text rebuilds bit for bit), then Gorilla's XOR encoding
of the 64-bit patterns and byte-stream split as alternatives; the smallest
after ZSTD is kept.

**int64, bool and state codes**: zigzag deltas bit-packed per vector of 1024
values. Counters, codes that rarely change and flags come down to a few bits a
value and constant runs to none. `build/type-bench` measures these.

### SIMD

Gorilla, Chimp and DoubleDelta decoding is bit-serial: where value i+1 starts
depends on value i, so it cannot be vectorized. The aggregation kernels are
dispatched at runtime to AVX-512, AVX2 or scalar code, and the fixed-point
encoder is written so the compiler vectorizes it. AVX-512 is not used on
Skylake-SP, Cascade Lake and Cooper Lake Xeons, which lower the clock of the
core after 512-bit instructions. `TSDB_SIMD=avx512|avx2|scalar` overrides the
choice.

## Server and clients

`tsdbd` serves a database over TCP with a small binary protocol
([`include/tsdb_protocol.h`](include/tsdb_protocol.h)): length-prefixed frames,
columnar arrays, results of raw queries streamed in chunks. One thread runs an
io_uring event loop (multishot accept, recv/send, an eventfd from the workers);
requests run on a worker pool, in order per connection, so a client can
pipeline inserts and still get "last write wins". A client that reads slowly
pauses its query instead of making the server buffer its result.

```sh
build/tsdbd --dir /data/sensors --listen 127.0.0.1 --port 7070 --wal-sync-ms 10
```

Connections: up to `--max-connections` at once (default 10,000; the descriptor
limit is raised to the hard limit at start). An idle connection costs one file
descriptor and about 16 KB; buffers grow for large frames and shrink back.
Requests run on `--workers` threads (default: one per CPU) and the rest wait
in their connection's queue. Measured with `tsdbc-bench connections`: 10,000
connections open at once, all querying, 85,000 queries/s in total, no
failures; the server's resident memory went from 148 MB at 1,000 connections
to 231 MB at 10,000.

There is no authentication or TLS: listen on loopback or a trusted network.

The client library (`build/libtsdbclient.a`, [`client/tsdb_client.h`](client/tsdb_client.h))
mirrors the embedded API and does not link the storage engine:

```c
tsdbc *c;
tsdbc_connect("127.0.0.1", 7070, &c);
uint32_t id;
tsdbc_series(c, "pump7.flow", 1, &id);
tsdbc_insert_series_async(c, id, ts, values, n);   /* pipelined */
tsdbc_wait(c);                                     /* acknowledgements */
tsdbc_query_agg(c, id, start, end, 60000, &b, &nb);
tsdbc_close(c);                                    /* collects outstanding acknowledgements first */
```

Typed series, nulls and states use the protocol's version 2 operations
(`tsdbc_series_create`, `tsdbc_insert_values`, `tsdbc_query_values`,
`tsdbc_state`); the version 1 operations are unchanged, so older clients keep
working with float32 series.

Requests the server has received run even if the client disconnects before
reading the replies; a client that half-closes its socket still gets every reply.

Clients in other languages, each with a quick start in its README:

| | directory | build and test |
|---|---|---|
| C | [`client/`](client) | `make`, `make test` |
| Rust (std only, blocking) | [`client/rust`](client/rust) | `make test-rust` |
| Python (PyO3 over the Rust client, numpy optional) | [`client/python`](client/python) | `make test-python` |
| Java 17+ (Maven, no runtime dependencies) | [`client/java`](client/java) | `make test-java` |

## Command line

```sh
build/tsdb /data/sensors create pump7.runtime_s --type int64   # float32 unless --type
build/tsdb /data/sensors create pump7.mode --type state
build/tsdb /data/sensors import < rows.csv          # series,time,value (null or empty: null)
build/tsdb /data/sensors insert pump7.flow < p.csv  # time,value
build/tsdb /data/sensors query pump7.flow 2024-01-01 2024-01-02 avg 5m
build/tsdb /data/sensors query pump7.flow now-1h now raw
build/tsdb /data/sensors stats
build/tsdb /data/sensors parts
build/tsdb /data/sensors optimize
build/tsdb /data/sensors drop-before 2023-01-01
```

## One writer, many readers (also over NFS)

One process writes; any number of processes follow it, on the same host or
on other hosts that mount the directory over NFS (NFS over RDMA works well):

```c
tsdb_options o;
tsdb_options_init(&o);
o.follow = 1;            /* read only, and keep up with the writer */
o.refresh_ms = 100;      /* how often to look */
tsdb_open("/mnt/sensors", &o, &db);
```

```sh
build/tsdbd --dir /data/sensors --port 7070                    # the writer
build/tsdbd --dir /mnt/sensors  --port 7070 --follow            # query servers, anywhere
```

The writer keeps a `MANIFEST`: the active parts, the series count, and which
WAL segments are already in parts. Followers poll it, open new parts, drop
replaced ones, read new series names, and tail the WAL segments that are not
in parts yet, so rows acknowledged by the writer are visible to followers
within about `refresh_ms` (plus `wal_sync_ms` if the writer syncs
periodically). A segment's rows leave a follower's memory in the same step as
the parts that hold them arrive, so queries never see a row twice or miss one.

What makes this safe on NFS:

- `MANIFEST` is rewritten in place with a checksum rather than renamed: NFS
  revalidates a file when it is reopened (close-to-open consistency), but a
  name looked up again can come from the directory cache. A torn read fails
  the checksum and is retried at the next poll.
- A file deleted on the NFS server is gone for every client, even one that has
  it mapped (the next page fault raises SIGBUS). Parts replaced by a merge are
  therefore renamed to `retired_*` at once and deleted only
  `obsolete_part_lifetime_s` later (480 s by default, ClickHouse's
  `old_parts_lifetime`). Renaming does not disturb readers that have the file
  open, and a follower that is a step behind finds a part under its retired name.
- Mount followers with `lookupcache=positive`, so a part or WAL segment created
  a moment ago is not hidden behind a cached "no such file".
- The writer must sync the WAL (`fsync = 1`, the default) for its rows to
  reach the server; NFS clients only write back on fsync or close.

Only one writer may exist (it takes a `flock` on `LOCK`, which NFSv4 honours).
`tests/test_follow.c` runs a writer and a follower as separate processes on one
directory while the writer appends, overwrites, merges and retires parts.
It has not been run against an actual NFS server in this repository's tests.

