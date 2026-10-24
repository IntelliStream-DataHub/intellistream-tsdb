# tsdb

A small, fast time series database written in C.

At [intellistream.ai](https://intellistream.ai) we work a lot with industrial
data: sensors on pumps, tanks, pipelines and machines that report a value every
second or every minute, for years. We built tsdb to:

- **store that data for less money**: it needs a third to a half less disk
  than ClickHouse, and a tenth of the raw size;
- **cost less to run** than a ClickHouse cluster: one small program that works
  well with its defaults, and no cluster to look after;
- **answer time series questions faster**: "the 5-minute averages of this
  sensor yesterday" takes 0.15 ms, 20 times faster than ClickHouse.

tsdb uses the same storage design as ClickHouse (MergeTree), but keeps only
what time series need. There is no SQL and there are no tables: you store
**series** (one per sensor) of **(time, value)** points, and you read them back
by time range, either as raw points or as summaries (count, sum, min, max,
average). You can link tsdb into your program as a library, or run its server,
`tsdbd`, and talk to it from C, Rust, Python or Java.

## Features

- Values can be float32, float64, int64, bool or a state (a text label from a
  small set, like "running" or "stopped"). Any value may be missing (null).
  Times are in milliseconds.
- Writing a value for a time that already exists replaces it. You never get
  duplicates back.
- Read raw points (oldest or newest first, with a limit), or summaries per
  time bucket: fixed buckets (every 5 minutes) or calendar ones (day, week,
  month, quarter, year). The server works in UTC; the clients turn your local
  time zone into UTC for you.
- Safe writes: data is on disk when a write returns. We test this by killing
  the server and by simulating power loss.
- One writer and any number of read-only followers can share a directory,
  also over NFS.
- The clients compress the data before they send it, so an insert uses about
  6 bytes per point on the network instead of 16.

## How it compares

100 million sensor readings (100 sensors, 1 million readings each), loaded and
queried on the same machine (AMD Ryzen 9 7950X, NVMe disk):

| database | insert speed (rows per second) | query time | size on disk | compression |
|---|---:|---:|---:|---:|
| **tsdb** | **82.6 million** | **0.15 ms** | **119 MB** | **10x** |
| ClickHouse | 43 million | 3.2 ms | 221 MB | 5.4x |
| DuckDB (\*) | 28 million | 3.7 ms | 307 MB | 3.9x |
| InfluxDB 2 | 3.6 million | 5.3 ms | 315 MB | 3.8x |
| TimescaleDB | 3.4 million | 5.8 ms | 404 MB | 3.0x |
| InfluxDB 3 Core | 1.0 million | 72 ms | 723 MB | 1.7x |
| QuestDB | 32 million | 5.3 ms | 1,657 MB | 0.7x |

- **Query time:** one day of one sensor as 5-minute averages (86,400 readings
  into 288 buckets), one client, average over thousands of random days.
- **Compression:** the raw size (8 bytes of time and 4 bytes of value per
  reading, 1.2 GB) divided by the size on disk.
- (\*) DuckDB is a library, not a server. Compare it with tsdb used as a
  library: 101 million rows per second and 0.07 ms per query.

At 100 billion rows, tsdb needs 116 GB where ClickHouse needs 170 GB. A summary
of the whole table takes 0.17 seconds instead of 59.

The test data behaves like real industrial sensors, not random numbers: disk
temperatures that creep by 0.1 °C, CPU temperatures with spikes, tanks that
fill and drain, a fast vibration sensor, pipeline flows with shutdowns. Every
answer was checked against tsdb's. All the numbers and how to run the
benchmarks yourself are in [AGENTS.md](AGENTS.md#benchmarks).

## How the compression works

Sensor data has patterns, and tsdb stores the pattern instead of the numbers.
The data is cut into blocks of 8192 readings of one sensor, and each block is
packed on its own.

**Times.** Sensors report at a steady pace: 10:00:00, 10:00:10, 10:00:20, …
tsdb stores the first time and the step (10 seconds). After that it only
stores how much each step differs from the one before. That is almost always
0, which costs almost nothing. A clock that wobbles a little costs a few bits.

**Values.** A sensor measures in fixed steps: a pressure gauge in steps of
0.001 bar, a thermometer in steps of 0.1 °C. So the readings 1.204, 1.205,
1.205, 1.207 are really the whole numbers 1204, 1205, 1205, 1207. tsdb stores
the first one and then the changes: +1, 0, +2. Small changes need only a few
bits each, instead of the 32 bits of a float.

**Everything else.** When values do not follow fixed steps, tsdb compares each
value's bits with the previous value's and stores only the bits that changed
(the Gorilla and Chimp methods). Each block tries the methods and keeps the
smallest result. Then a general-purpose compressor (zstd) goes over it, but
only where that still makes it smaller.

The result: a reading that takes 12 bytes raw takes about 1.2 bytes on disk.
A slow temperature sensor needs less than 2 bits per value.

**Why this also makes it fast.** Each block also keeps a short summary: how
many readings it has, and their sum, smallest and largest value. For a
question like "the average per day", most blocks fall wholly inside one day,
so tsdb reads their summaries and never unpacks them. The readings of one
sensor are stored together, so finding them is quick, and the blocks that do
have to be unpacked are unpacked in parallel.

More detail: [docs/design.md](docs/design.md).

## Getting started

You need a C compiler (C11), libzstd, and liburing for the server.

```sh
make            # builds everything into build/
```

### From the command line

```sh
build/tsdb /data/sensors import < rows.csv          # lines of: series,time,value
build/tsdb /data/sensors query pump7.flow 2024-01-01 2024-01-02 avg 5m
build/tsdb /data/sensors query pump7.flow now-1h now raw --desc --limit 10
build/tsdb /data/sensors query pump7.flow 2024-01-01 2025-01-01 all 1mo --tz Europe/Oslo
```

### As a library in C

See [`include/tsdb.h`](include/tsdb.h).

```c
tsdb *db;
tsdb_open("/data/sensors", NULL, &db);

uint32_t flow;
tsdb_series(db, "pump7.flow", &flow);                 /* find or create a series */
tsdb_insert_series(db, flow, times, values, n);       /* on disk when it returns */

tsdb_bucket *b; size_t nb;
tsdb_group day = {TSDB_DAY, 1, "Europe/Oslo"};
tsdb_query_group_local(db, flow, start, end, &day, NULL, &b, &nb);   /* one summary per day */
printf("first day's average: %f\n", tsdb_bucket_avg(&b[0]));
free(b);
tsdb_close(db);
```

### As a server

```sh
build/tsdbd --dir /data/sensors --port 7070
build/tsdbd --dir /mnt/sensors --port 7070 --follow    # a read-only copy that follows the writer
```

| client | quick start |
|---|---|
| C | [`client/tsdb_client.h`](client/tsdb_client.h), example below |
| Rust | [client/rust](client/rust/README.md) |
| Python | [client/python](client/python/README.md) |
| Java 17+ | [client/java](client/java/README.md) |

```c
tsdbc *c;
tsdbc_connect("127.0.0.1", 7070, &c);
uint32_t id;
tsdbc_series(c, "pump7.flow", 1, &id);
tsdbc_insert_series(c, id, times, values, n);
tsdbc_query_agg(c, id, start, end, 60000, &b, &nb);    /* one summary per minute */
tsdbc_close(c);
```

## Tests

```sh
make test          # all unit, crash-recovery and server tests
make asan          # the same, checking for memory errors
make tsan          # the same, checking threads for data races
make jepsen        # crash tests: kill -9, power loss, pauses
make test-clients  # the Rust, Python and Java clients (needs cargo, python3, mvn)
```
