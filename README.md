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
([details](#100-billion-rows-tsdb-and-clickhouse)). On 100 million rows it is
also smaller and faster on every query than QuestDB, TimescaleDB, InfluxDB 2
and InfluxDB 3 ([details](#100-million-rows-six-databases)).

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

Every query reports what it read, as ClickHouse's client does
(`tsdb_last_query_stats`, or `LAST_STATS` over TCP). One series of the
100-billion-row benchmark, by day in UTC and then in Oslo, where the granules
around local midnight have to be decoded:

```
116 buckets in set. Elapsed: 0.000366 sec. Processed 10.00 million rows, 120.00 MB (27.32 billion rows/s, 327.80 GB/s)
116 parts, 1273 granules (1273 from their statistics); 0 rows decoded from 0.00 B, 0 in memory
116 buckets in set. Elapsed: 0.001737 sec. Processed 10.00 million rows, 120.00 MB (5.76 billion rows/s, 69.08 GB/s)
116 parts, 1273 granules (1158 from their statistics); 608001 rows decoded from 692.52 KB, 0 in memory
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
make tsan          # the same under ThreadSanitizer, built in build-tsan/ (the server too)
make jepsen        # fault injection: kill -9, simulated power loss, pauses (Jepsen-style checker)
make test-clients  # the Rust, Python and Java clients against build/tsdbd (needs cargo, python3, mvn)
```

Tests take a scratch directory as their first argument
(`./build/test_tsdb /tmp/x`); `make` uses /tmp.

## Benchmarks

| tool | measures |
|---|---|
| `build/codec-bench` | float32 value codecs per sensor type: bits per value, speed |
| `build/type-bench` | the float64, int64, bool and state codecs |
| `build/tsdb-bench` | load, compression and queries on 100 M rows; `--export` writes the rows for ClickHouse |
| `build/insert-bench` | durable inserts on a real disk, 1 row to 1 billion rows per call |
| `build/tsdbc-bench` | the server: loading, queries, 10,000 connections |
| `build/scale-bench` | 100 billion rows: load, then a query suite, with the same queries as SQL for ClickHouse |
| [`bench/dbcmp`](bench/dbcmp) | tsdb against ClickHouse, QuestDB, TimescaleDB and InfluxDB 2 and 3 on 100 M rows; its README has the steps |

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

### 100 million rows, six databases

The same 100 M rows (100 series x 1 M samples of the seven sensor types at
their own rates, `tsdb-bench --export`) in each system, one at a time, data on
the NVMe, every server in Podman on 127.0.0.1, each set up as its docs
recommend for time series. Every query's answer was checked against tsdb's:
no differences.

| | version | setup |
|---|---|---|
| tsdb | this repository | defaults; loaded through tsdbd, 4 connections |
| ClickHouse | 26.8.2.7 | `PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)`, `DoubleDelta`/`Gorilla` + `ZSTD(1)`; one RowBinary insert |
| QuestDB | 10.1.0 | `series SYMBOL`, `PARTITION BY DAY WAL`; line protocol, 8 connections |
| TimescaleDB | 2.31.0, PostgreSQL 17.11 | hypertable, 1-day chunks, compressed (`segmentby series, orderby ts`); `timescaledb-parallel-copy`, 16 workers |
| InfluxDB | 2.7.12 | one measurement, tag `series`; line protocol, 8 writers |
| InfluxDB 3 Core | 3.12.1 | line protocol, 16 writers; SQL |

float32 values are stored as 4-byte floats where the system has them, as
doubles in InfluxDB. tsdb, PostgreSQL and InfluxDB sync to disk before they
acknowledge a write; ClickHouse and QuestDB by default do not.

| | load | rows/s | background work until settled | size | bytes/row |
|---|---:|---:|---|---:|---:|
| **tsdb** (tsdbd) | **1.21 s** | **82.6 M** | none needed | **119 MB** | **1.19** |
| ClickHouse | 2.31 s | 43 M | merges, 26 s to 45 parts, then 45 min more | 221 MB | 2.21 |
| QuestDB | 3.08 s (5.9 s to queryable) | 32 M | WAL apply 2.8 s | 1,657 MB (no compression in open source) | 16.6 |
| TimescaleDB | 29.6 s | 3.4 M | compressing 696 chunks: 12.3 s | 404 MB (7,198 MB before) | 4.04 |
| InfluxDB 2 | 28.2 s | 3.6 M | 11 min (10 of them its snapshot wait) | 315 MB | 3.15 |
| InfluxDB 3 Core | 99.6 s | 1.0 M | Parquet only an hour later: 100,001 files | 723 MB (plus 2.8 GB of WAL) | 7.6 |

Whole range, all 100 series, one client (tsdb: one query per series, on 1
or on 16 connections), median of 5:

| | count/sum/min/max per series | buckets of 3600 samples | buckets of 60 samples (1.67 M buckets) |
|---|---:|---:|---:|
| **tsdbd, 16 connections** | **5.2 ms** | **18.3 ms** | **40.7 ms** |
| tsdbd, 1 connection | 45.7 ms | 63.6 ms | 158 ms |
| tsdbd, 1 connection, partitions merged | 5.0 ms | 23.7 ms | 116 ms |
| ClickHouse | 51 ms | 187 ms | 368 ms |
| QuestDB | 36 ms | 282 ms | 1,288 ms |
| TimescaleDB | 479 ms | 3,270 ms | 3,827 ms |
| InfluxDB 2 (Flux; InfluxQL was slower) | 2,294 ms | 2,371 ms | 6,937 ms |
| InfluxDB 3 Core | 3,199 ms | 45,059 ms | 45,364 ms |

Random windows on random series, queries per second with 1 / 32 clients
(15 s each, the same 10,000 queries per class for every system):

| | raw rows, 3600 samples | 86400 samples into 288 buckets | 604800 samples into 168 buckets | a single row |
|---|---:|---:|---:|---:|
| **tsdbd** | **14,574 / 88,029** | **6,468 / 56,631** | **2,490 / 13,835** | **20,192 / 127,187** |
| tsdbd, partitions merged (1 client) | 16,719 | 8,604 | 5,860 | 24,543 |
| ClickHouse | 687 / 9,567 | 317 / 3,415 | 167 / 1,025 | 862 / 12,095 |
| QuestDB | 799 / 10,745 | 188 / 562 | 92 / 177 | 4,122 / 75,208 |
| TimescaleDB | 978 / 15,018 | 172 / 2,923 | 25 / 390 | 3,482 / 70,412 |
| InfluxDB 2 (faster of InfluxQL and Flux) | 421 / 4,402 | 188 / 929 | 112 / 409 | 3,127 / 31,590 |
| InfluxDB 3 Core | 170 / 444 | 13.8 / 23.5 | 3.3 / 4.0 | 607 / 5,052 |

tsdb was measured with the parts its background merges left a minute after
the load (1,009 in 696 partitions). Batches from 4 connections arrive
interleaved in time, and the parts merged from them overlap, so some granules
had to be decoded rather than answered from their statistics; the "partitions
merged" rows are the same data after `tsdb_optimize`. Background merges now
do that by themselves once a partition has had no new rows for 5 minutes
(`merge_idle_s`). Notes on the others: Flux was only fast written as separate
pipelines (a shared source disables its pushdown, and the whole-range query
then ran influxd out of memory); InfluxDB 3 Core keeps 10-minute Parquet files
without compacting them; TimescaleDB ran the whole-range aggregates without
parallel workers; a symbol index in QuestDB doubles point lookups at 32
clients and slows its aggregates up to 4x.

### Durable writes

On a real disk: `build/insert-bench`, fsync on, ext4 on a WD
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
