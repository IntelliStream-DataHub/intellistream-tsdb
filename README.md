# tsdb

An embedded time series database in C, modelled on ClickHouse's MergeTree.
Named series of timestamped values (float32, float64, int64, bool, or a state
from a small set; any value may be null), queried by time range: raw, or
aggregated (count, sum, min, max, avg) in fixed or calendar buckets. No SQL.
It comes with `tsdbd`, an io_uring TCP server, and clients for C, Rust,
Python and Java.

On 100 billion sensor readings, against ClickHouse on the same machine:
stored in **116 GB instead of 170 GB**, loaded in **15 minutes instead of 45**
(until merges settled), whole-table aggregates in **0.16 s instead of 59 s**,
single-series queries **2.3 to 190 times faster**
([details](#100-billion-rows-tsdb-and-clickhouse)).

How it works: [docs/design.md](docs/design.md).

## Quick start

Needs a C11 compiler, pthreads, libzstd, and liburing for the server.

```sh
make                       # library, CLI, server, client library, benchmarks into build/
```

From the command line:

```sh
build/tsdb /data/sensors create pump7.mode --type state       # float32 unless --type
build/tsdb /data/sensors import < rows.csv                     # series,time,value (null or empty: null)
build/tsdb /data/sensors query pump7.flow 2024-01-01 2024-01-02 avg 5m
build/tsdb /data/sensors query pump7.flow now-1h now raw --desc --limit 10
build/tsdb /data/sensors query pump7.flow 2024-01-01 2025-01-01 all 1mo --tz Europe/Oslo
```

Embedded in C ([`include/tsdb.h`](include/tsdb.h)):

```c
tsdb *db;
tsdb_open("/data/sensors", NULL, &db);

uint32_t flow, runtime;
tsdb_series(db, "pump7.flow", &flow);                               /* float32 */
tsdb_series_create(db, "pump7.runtime_s", TSDB_INT64, NULL, &runtime);

tsdb_insert_series(db, flow, ts, values, n);                         /* durable on return */
tsdb_values col = {TSDB_INT64, seconds, valid};                      /* valid: bitmap, NULL = no nulls */
tsdb_insert_series_values(db, runtime, ts, &col, n);

tsdb_bucket *b; size_t nb;
tsdb_group day = {TSDB_DAY, 1, "Europe/Oslo"};
tsdb_query_group(db, flow, start, end, &day, &b, &nb);               /* count, sum, min, max per day */
printf("%f\n", tsdb_bucket_avg(&b[0]));
free(b);
tsdb_close(db);
```

As a server ([`include/tsdb_protocol.h`](include/tsdb_protocol.h)), with one
writer and any number of followers on the same directory, also over NFS:

```sh
build/tsdbd --dir /data/sensors --port 7070 --wal-sync-ms 10
build/tsdbd --dir /mnt/sensors  --port 7070 --follow          # read-only, follows the writer
```

| client | where | quick start |
|---|---|---|
| C | [`client/tsdb_client.h`](client/tsdb_client.h) | below |
| Rust (std only) | [`client/rust`](client/rust) | [README](client/rust/README.md) |
| Python (PyO3 over the Rust client, numpy optional) | [`client/python`](client/python) | [README](client/python/README.md) |
| Java 17+ (no runtime dependencies) | [`client/java`](client/java) | [README](client/java/README.md) |

```c
tsdbc *c;
tsdbc_connect("127.0.0.1", 7070, &c);
uint32_t id;
tsdbc_series(c, "pump7.flow", 1, &id);
tsdbc_insert_series_async(c, id, ts, values, n);   /* pipelined */
tsdbc_query_agg(c, id, start, end, 60000, &b, &nb);
tsdbc_close(c);                                    /* waits for the acknowledgements */
```

## Tests

```sh
make test          # unit, crash-recovery, value type, follower, volume and server tests
make asan          # the same under AddressSanitizer + UBSan
make jepsen        # fault injection: kill -9, simulated power loss, pauses (Jepsen-style checker)
make test-clients  # the Rust, Python and Java clients against build/tsdbd (needs cargo, python3, mvn)
```

Tests take a scratch directory as their first argument
(`./build/test_tsdb /tmp/x`); `make` uses /tmp. ThreadSanitizer is not wired
into make: build the way `make asan` does, with `-fsanitize=thread`.

## Benchmarks

| tool | measures |
|---|---|
| `build/codec-bench` | float32 value codecs per sensor type: bits per value, speed |
| `build/type-bench` | the float64, int64, bool and state codecs |
| `build/tsdb-bench` | load, compression and queries on 100 M rows; `--export` writes the rows for ClickHouse |
| `build/insert-bench` | durable inserts on a real disk, 1 row to 1 billion rows per call |
| `build/tsdbc-bench` | the server: loading, queries, 10,000 connections |
| `build/scale-bench` | 100 billion rows: load, then a query suite, with the same queries as SQL for ClickHouse |

```sh
build/codec-bench
build/tsdb-bench --dir /tmp/b --series 100 --points 1000000
build/insert-bench --dir /var/tmp/b
build/tsdbd --dir /tmp/srv & build/tsdbc-bench query --conns 32
build/scale-bench load --dir /var/tmp/big --series 10000 --points 10000000 --threads 8
build/scale-bench query --dir /var/tmp/big --threads 32 --sql /tmp/sql
tools/scale-bench-ch.sh /tmp/sql scale.big            # the same queries in ClickHouse
```

ClickHouse runs in Podman, bound to 127.0.0.1, with its data on the same
file system as tsdb's:

```sh
podman run -d --name ch --network host -v /var/tmp/ch:/var/lib/clickhouse \
    -v ./listen.xml:/etc/clickhouse-server/config.d/zz-listen.xml docker.io/clickhouse/clickhouse-server
build/scale-bench export --from 0 --to 1250 | clickhouse client -q "INSERT INTO scale.big FORMAT RowBinary"
```

## Results

Machine: AMD Ryzen 9 7950X (16 cores / 32 threads, AVX-512), 64 GB RAM,
WD SN720 NVMe (LUKS, ext4). ClickHouse 26.8.2.7.

### 100 billion rows, tsdb and ClickHouse

10,000 series x 10 million samples: the seven sensor types below, mixed, all
stamped at 1 Hz (115.7 days from 2024-01-01). Both load exactly the same rows
in the same order: 8 streams, each sending 100 series 100,000 samples at a
time. ClickHouse: `MergeTree PARTITION BY toDate(ts) ORDER BY (series, ts)`,
`Delta, ZSTD(1)` on time and value (its best codecs on this data), default
settings otherwise; tsdb with its defaults (1-day partitions, 8192-row granules).
The data set is two to three times the RAM, so large scans read from disk.

| | tsdb | ClickHouse |
|---|---:|---:|
| size on disk | **116,113 MB** (1.161 bytes/row) | 170,098 MB (1.701 bytes/row) |
| of which timestamps / values / index, bits per row | 2.00 / 7.22 / 0.07 | 2.54 / 11.03 / 0.02 series, plus marks |
| load, 8 streams | **880 s** (114 M rows/s) | 1,107 s (90 M rows/s) |
| until background merges settled | **900 s** (1,035 parts) | 2,676 s (868 parts) |

ClickHouse with 10 M-row insert blocks ran out of memory (its limit here was
25 GiB) in one of the 8 streams; the numbers are with its default blocks of
1 M rows, which made 91,000 parts to merge.

Queries: results read in full. Random windows on random series, mean of 20
to 1,000 queries; whole-table and 100-series queries, median of 2 or 3.
Second of two runs. ClickHouse gets each query over one connection and runs it
on 32 threads; tsdb answers a query over many series with one query per
series on 32 threads (32 connections to tsdbd). The results agree: count, sum
(to 1e-14), min and max of the whole table are the same in both.

| query | rows | ClickHouse | **tsdbd** (TCP) | tsdb embedded | ClickHouse / tsdbd |
|---|---:|---:|---:|---:|---:|
| a random single row | 1 | 2.68 ms | **0.29 ms** | 0.30 ms | 9x |
| aggregate a random window of one series | 100 K | 5.78 ms | **0.57 ms** | 0.63 ms | 10x |
| | 1 M | 11.3 ms | **0.58 ms** | 0.66 ms | 20x |
| | 10 M (all of it) | 43.3 ms | **0.23 ms** | 0.17 ms | 190x |
| aggregate 100 series | 1 G | 738 ms | **2.1 ms** | 2.1 ms | 350x |
| aggregate the whole table | 100 G | 58.7 s | **0.17 s** | 0.16 s | 340x |
| one series by hour | 10 M | 49.1 ms | **16.3 ms** | 19.4 ms | 3x |
| one series by minute | 10 M | 63.2 ms | **27.2 ms** | 20.3 ms | 2.3x |
| 100 series by series and day (Europe/Oslo) | 1 G | 959 ms | **172 ms** | 273 ms | 5.6x |
| 100 series by series and hour | 1 G | 982 ms | **344 ms** | 365 ms | 2.9x |
| whole table by series and day (UTC) | 100 G | 96.7 s | **0.18 s** | 0.15 s | 540x |
| whole table by series and day (Europe/Oslo) | 100 G | 95.7 s | **37.6 s** | 27.9 s | 2.5x |
| whole table by series and hour | 100 G | 100.7 s | **46.8 s** | 47.0 s | 2.2x |

Where the answer lies in the granule statistics (every bucket covers whole
granules), tsdb reads one index entry per 8192 rows: whole-table aggregates
and UTC days take milliseconds. Hours, minutes and local days that do not line up
with granules have to decode them, and then both systems read the data from
disk; tsdb reads less of it.

### 100 million rows

100 series x 1,000,000 samples of the same seven sensor types at their own
rates (`tsdb-bench --export`), written once to a RowBinary file that both
systems load. ClickHouse table: `PARTITION BY toDate(ts) ORDER BY (series, ts)`
with its recommended time series codecs unless noted. Data on tmpfs, so these
numbers are CPU, not disk; durable writes on NVMe have their own table.

#### Compression

| | bytes / row | ratio vs 12 bytes |
|---|---:|---:|
| ClickHouse, `ts DoubleDelta, ZSTD(1)`, `value Gorilla, ZSTD(1)` | 2.235 | 5.4x |
| ClickHouse, `Delta, ZSTD(1)` on both (the best combination found) | 1.799 | 6.7x |
| **tsdb** | **1.232** | **9.7x** |

tsdb per row: timestamps 2.05 bits, values 7.68 bits, granule index 0.12 bits.
ClickHouse at its best: timestamps 2.98 bits, values 7.40 bits, and about 4 bits
of marks, primary index and checksums. The per-sensor value codecs are below.

#### Write speed

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

#### Read speed

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

### Value codecs

float32, bits per value after ZSTD(1) (`build/codec-bench`):

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

The other types (`build/type-bench`), bits per value with ZSTD(1), and
decoding speed on one core:

| data | type | bits / value | vs raw | decode, M values/s | codec |
|---|---|---:|---:|---:|---|
| SMART disk temperature, 0.1 degC | float64 | 0.88 | 73x | 749 | fixed point |
| CPU temperature, spikes to 100 degC | float64 | 3.10 | 21x | 722 | fixed point |
| tank pressure, 0.001 bar | float64 | 1.82 | 35x | 421 | fixed point |
| tank pressure in psi (unrounded) | float64 | 48.87 | 1.3x | 1,032 | byte split |
| vibration, 16-bit ADC | float64 | 14.03 | 4.6x | 620 | fixed point (binary) |
| pipeline flow rate, 0.01 m3/h | float64 | 10.80 | 5.9x | 903 | fixed point |
| pipeline totalizer, m3 | float64 | 1.56 | 41x | 397 | fixed point |
| uniform random | float64 | 59.24 | 1.1x | 526 | byte split |
| totalizer in whole liters | int64 | 1.18 | 54x | 431 | bit-packed deltas |
| a rate, random 50,000 - 70,000 | int64 | 16.01 | 4.0x | 1,076 | bit-packed deltas |
| an event sequence | int64 | 0.04 | 1,680x | 963 | bit-packed deltas |
| a door, a few changes an hour | bool | 0.06 | 130x | 1,118 | bit-packed deltas |
| coin flips | bool | 1.29 | 6.2x | 794 | bit-packed deltas |
| a machine mode of 5 states | state | 0.13 | 245x | 896 | bit-packed deltas |
