# tsdb

An embedded time series database in C, modelled on ClickHouse's MergeTree,
for one shape of data: named series of `(DateTime64(3), Float32)` points.
No SQL: you query a series over a time range, raw or aggregated
(count, sum, min, max, avg), optionally in time buckets.

```c
tsdb *db;
tsdb_open("/data/sensors", NULL, &db);

uint32_t id;
tsdb_series(db, "pump7.pressure_bar", &id);            /* get or create */

tsdb_insert_series(db, id, ts, values, n);             /* a batch: durable on return */
tsdb_append(db, id, ts_ms, 3.142f);                    /* one point: WAL, group commit */

tsdb_points p;                                         /* raw, [start, end), time order */
tsdb_query_points(db, id, start, end, &p);

tsdb_bucket *b; size_t nb;                             /* 5 minute buckets */
tsdb_query_agg(db, id, start, end, 300000, &b, &nb);
printf("%f\n", tsdb_bucket_avg(&b[0]));

tsdb_close(db);
```

The full API is in [`include/tsdb.h`](include/tsdb.h).

## Semantics

- `(series, ts)` is unique, like a `ReplacingMergeTree` that is always read
  with `FINAL`: writing a timestamp that exists replaces its value, the last
  write wins (within one batch, the later row), and queries never return
  duplicates.
- Time ranges are half open, `[start, end)`, in milliseconds since the epoch.
- Aggregate buckets are aligned to the epoch, like `toStartOfInterval`.
  min/max ignore NaN (a bucket of only NaN gives NaN); NaN propagates into sum.
- All functions are thread safe. One process opens a database for writing
  (`flock`); any number may open it `read_only`.

## Storage

A database is a directory:

```
META                       format version, partition size, granule size
SERIES                     append-only series dictionary (name -> dense id)
LOCK
wal_<seq>.log              write-ahead log of the current memtable
p<partition>_<minblock>_<maxblock>_<level>.part
```

Each part is one immutable file holding the rows of every series written in
one insert (or produced by one merge), sorted by `(series, ts)`:

```
header | ts block, value block, ts block, value block, ... | granule index | footer
```

A granule is up to 8192 rows of one series. Its 64-byte index entry holds
the series, min/max timestamp, count, sum, min, max, block offsets and a
checksum. The index is the sparse primary index of MergeTree: finding a series'
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

**Values** (`value_codec = TSDB_VALUES_AUTO`):

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

### SIMD

Gorilla, Chimp and DoubleDelta decoding is bit-serial: where value i+1 starts
depends on value i, so it cannot be vectorized. The aggregation kernels are
dispatched at runtime to AVX-512, AVX2 or scalar code, and the fixed-point
encoder is written so the compiler vectorizes it. AVX-512 is not used on
Skylake-SP, Cascade Lake and Cooper Lake Xeons, which lower the clock of the
core after 512-bit instructions. `TSDB_SIMD=avx512|avx2|scalar` overrides the
choice.

## Building

Needs a C11 compiler, pthreads, libzstd, and liburing for the server.

```sh
make            # build/libtsdb.a, build/tsdb, benchmarks
make test       # unit, crash-recovery and server tests
make asan       # the tests under AddressSanitizer + UBSan
make PORTABLE=1 # without -march=native
```

## Server and client

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
build/tsdb /data/sensors import < rows.csv          # series,time,value
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

## Benchmarks

Machine: AMD Ryzen 9 7950X (16 cores / 32 threads, AVX-512), 64 GB RAM.
ClickHouse 26.8.2.7 in Podman on the same machine.

Dataset: 100 series x 1,000,000 samples = 100 M rows of the seven sensor types
above, mixed (`tsdb-bench --export`), written once to a RowBinary file that
both systems load. ClickHouse table: `PARTITION BY toDate(ts) ORDER BY (series, ts)`
with its recommended time series codecs unless noted; tsdb with its defaults
(1 day partitions, 8192-row granules). Data on tmpfs, so these numbers are
CPU, not disk; durable writes on NVMe have their own table.

### Compression

| | bytes / row | ratio vs 12 bytes |
|---|---:|---:|
| ClickHouse, `ts DoubleDelta, ZSTD(1)`, `value Gorilla, ZSTD(1)` | 2.235 | 5.4x |
| ClickHouse, `Delta, ZSTD(1)` on both (the best combination found) | 1.799 | 6.7x |
| **tsdb** | **1.232** | **9.7x** |

tsdb per row: timestamps 2.05 bits, values 7.68 bits, granule index 0.12 bits.
ClickHouse at its best: timestamps 2.98 bits, values 7.40 bits, and about 4 bits
of marks, primary index and checksums. The per-sensor value codec table is in
[Compression](#compression).

### Write speed

Loading the 100 M rows:

Rates in million rows per second.

| | wall time | M rows / s |
|---|---:|---:|
| ClickHouse `INSERT ... FORMAT RowBinary` (the server used 10 threads, 8.1 CPU s) | 2.34 s | 43 M |
| **tsdb, 1 thread, embedded** | **1.85 s** | **54 M** |
| tsdb, 4 threads, embedded | 0.55 s | 182 M |
| tsdb, 16 threads, embedded | 0.30 s | 338 M |
| **tsdbd over TCP, 1 connection** (pipelined 1 M-row batches) | **2.24 s** | **45 M** |
| tsdbd over TCP, 4 connections | 0.76 s | 131 M |
| ClickHouse `OPTIMIZE FINAL` (MergeTree / ReplacingMergeTree) | 4.7 / 6.0 s | |
| tsdb `tsdb_optimize` | 0.64 - 0.94 s | |

Durable inserts on a real disk: `build/insert-bench`, fsync on, ext4 on a WD
SN720 NVMe drive (LUKS), 1000 series of the mixed sensors arriving the way
collectors send them (every series at time t, then t + 1). One `fdatasync` on
this drive takes about 0.7 ms.

| how | rows per call | calls / s | rows / s | p50 latency | p99 latency |
|---|---:|---:|---:|---:|---:|
| `tsdb_append`, WAL fsync on every call, 1 thread | 1 | 1,441 | 1,441 | 0.68 ms | 0.79 ms |
| same, 16 threads (group commit) | 1 | 12,356 | 12,356 | 1.39 ms | 1.60 ms |
| same, 64 threads (group commit) | 1 | 42,464 | 42,464 | 1.51 ms | 1.68 ms |
| `tsdb_append`, WAL fsync every 10 ms, 1 thread | 1 | 9.9 million | 9.9 million | < 1 us | < 1 us |
| `tsdb_append`, WAL fsync every 50 ms, 1 thread | 1 | 10.0 million | 10.0 million | < 1 us | < 1 us |
| `tsdb_append`, WAL fsync every 100 ms, 1 thread | 1 | 10.0 million | 10.0 million | < 1 us | < 1 us |
| `tsdb_insert`, 1 row written as its own part (no WAL) | 1 | 557 | 557 | 1.56 ms | 2.50 ms |
| `tsdb_insert` through WAL + memtable | 10 | 1,371 | 13,708 | 0.71 ms | 0.83 ms |
| | 100 | 1,361 | 136,087 | 0.72 ms | 0.80 ms |
| | 1,000 | 1,293 | 1.3 million | 0.75 ms | 1.22 ms |
| | 10,000 | 1,047 | 10.5 million | 0.89 ms | 1.99 ms |
| `tsdb_insert` straight to parts | 100,000 | 73 | 7.3 million | 13.8 ms | 19.2 ms |
| | 1 million | 30 | 29.6 million | 33 ms | 46 ms |
| | 10 million | 4 | 38.8 million | 255 ms | 275 ms |
| | 100 million | | 41.3 million | 2.4 s | |
| | 1 billion | | 42.2 million | 23.7 s | |

Durability options: with `wal_sync_ms = 0` (the default) every write returns
after its own fsync, shared by concurrent writers (group commit). A periodic
sync of 10 ms costs no measurable throughput against 100 ms and bounds what a
power failure can lose to the last 10 ms of acknowledged writes.

### Read speed

Whole range, all 100 series. Bucket sizes are in samples so each sensor type
does the same work (`GROUP BY series, intDiv(ts, step * N)` in ClickHouse).
ClickHouse: best of three; `ReplacingMergeTree ... FINAL` was within a few percent.

| | ClickHouse 1 thread | ClickHouse 32 threads | **tsdb 1 thread** | **tsdb 16 threads** |
|---|---:|---:|---:|---:|
| raw rows in time order | 1.491 s | 0.287 s | **0.361 s** | **0.038 s** |
| count/sum/min/max | 1.225 s | 0.079 s | **0.002 s** | **0.002 s** |
| buckets of 86400 samples | 1.793 s | 0.210 s | **0.004 s** | **0.003 s** |
| buckets of 3600 samples | 1.864 s | 0.202 s | **0.246 s** | **0.022 s** |
| buckets of 60 samples | 2.101 s | 0.248 s | **0.280 s** | **0.036 s** |
| buckets of 10 samples | 2.630 s | 0.716 s | **0.377 s** | **0.074 s** |

In rows per second: a raw scan that decodes and returns every row runs at
280 million rows/s on one core and **2.6 billion rows/s** on 16 (one query per
series, in parallel; a single raw query is single threaded). Aggregation that
decodes every value (buckets of 60 samples) reaches 2.8 billion rows/s.

Granules that lie inside one bucket are answered from the granule index
without decompressing, which is why whole-range and coarse aggregates take
milliseconds: the whole-range count/sum/min/max reads 24,136 index entries
instead of 100 million rows. Dividing rows by time there gives about 50
billion rows/s, but that is an effective rate, not a scan rate.

Random time windows on one series, queries per second. Over TCP both run
behind their protocol on loopback (`clickhouse-benchmark` against
`tsdbc-bench` and `tsdbd`); the embedded columns call the library directly.

| | ClickHouse c=1 | **tsdbd c=1** | ClickHouse c=32 | **tsdbd c=32** | tsdbd follower c=32 | embedded 1 thr | embedded 32 thr |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw, 3600 samples | 643 | **14,131** | 6,772 | **88,469** | 83,733 | 35,640 | 547,204 |
| 86400 samples into 288 buckets | 241 | **10,511** | 1,588 | **58,476** | 57,650 | 18,488 | 77,025 |
| 604800 samples into 168 buckets | 106 | **5,210** | 285 | **12,450** | 12,655 | 6,295 | 14,165 |

The 7-day aggregates cover 7.5 billion rows/s over TCP and 8.6 billion rows/s
embedded, mostly from granule statistics (only the granules at the window's
edges and inside partial buckets are decoded).

Rewriting existing timestamps has a read cost until merges catch up: with
every row written twice, aggregates have to merge both copies instead of using
the granule statistics, and ran about 14x slower until `tsdb_optimize` (or the
background merges) folded them together.

### Reproducing

```sh
build/codec-bench                                    # value codecs per sensor type
build/tsdb-bench --export data.rowbinary --series 100 --points 1000000
build/tsdb-bench --dir /tmp/b --series 100 --points 1000000 [--query-threads 1]
build/insert-bench --dir /var/tmp/b                  # durable inserts, 1 row .. 1e9 rows per batch
build/tsdbd --dir /tmp/srv &                         # then, against it:
build/tsdbc-bench load data.rowbinary --conns 4
build/tsdbc-bench query --conns 32
build/tsdbc-bench connections --conns 10000
```
