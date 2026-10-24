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
- Downsample a series to N points that keep its shape, for charts: the peaks
  and dips stay (LTTB), and every point returned is a real stored point.
- Time-weighted averages for sensors that report irregularly or only on
  change: each value counts for as long as it held, stepwise or on a straight
  line between readings, and not across an outage longer than you allow. Wide
  buckets (days of minute readings) come mostly from stored summaries.
- Sums of int64 values are exact. Sums of float values can differ in the last
  digits from one run to the next, as in ClickHouse, because the order of the
  additions changes. If exact sums matter, store the value as an int64 in fixed
  point: 21.3 °C as 21300 milli-degrees.
- Delete the values of one series in a time range, or replace them with new
  ones in one step (readers see the old values or the new ones, never a mix).
  Give a series a retention period, and values older than that disappear on
  their own. Over the wire these need protocol version 7 (tsdbd and the C
  client of this release; the CLI has `delete` and `retention`).
- Safe writes: data is on disk when a write returns. We test this by killing
  the server and by simulating power loss.
- One writer and any number of read-only followers can share a directory,
  also over NFS.
- The clients compress the data before they send it, so an insert uses about
  6 bytes per point on the network instead of 16.
- An Apache Pulsar 5 sink ([connectors/pulsar](connectors/pulsar/README.md))
  writes points from Pulsar into tsdb. Pulsar holds them while tsdb is down,
  and a point is taken off Pulsar only once it is safely on disk in tsdb
  (in tsdbd's default sync mode).

## How it compares

100 million sensor readings (100 sensors, 1 million readings each), loaded and
queried on the same machine (AMD Ryzen 9 7950X, NVMe disk). Two tables: tsdb as
a library inside your program, and tsdb as a server used through its clients
over TCP. Each is compared only with systems used the same way.

**Embedded: a library inside your program, no network**

| database | insert (rows per second) | query (rows per second) | query time | size on disk | compression |
|---|---:|---:|---:|---:|---:|
| **tsdb** | **101 million** | **2.5 billion** | **0.07 ms** | **119 MB** | **10x** |
| DuckDB | 28 million | 552 million | 3.7 ms | 307 MB | 3.9x |

**Server: through the clients over TCP**

| database | insert (rows per second) | query (rows per second) | query time | size on disk | compression |
|---|---:|---:|---:|---:|---:|
| **tsdb** (tsdbd) | **82.6 million** | **862 million** | **0.15 ms** | **119 MB** | **10x** |
| ClickHouse | 43 million | 272 million | 3.2 ms | 221 MB | 5.4x |
| InfluxDB 2 | 3.6 million | 14 million | 5.3 ms | 315 MB | 3.8x |
| TimescaleDB | 3.4 million | 26 million | 5.8 ms | 404 MB | 3.0x |
| InfluxDB 3 Core | 1.0 million | 2.2 million | 72 ms | 723 MB | 1.7x |
| QuestDB (\*) | 32 million | 78 million | 5.3 ms | 1,657 MB | 0.7x |

- **Query (rows per second):** all 100 sensors over the whole range in buckets
  of 60 readings, a query that has to read every row: 100 million rows divided
  by the time it took. tsdb used one thread (embedded) or one connection
  (server); DuckDB and ClickHouse ran the query on 32 threads. Summaries over
  whole blocks are much faster still, because tsdb answers them from each
  block's summary without reading the rows.
- **Query time:** one day of one sensor as 5-minute averages (86,400 readings
  into 288 buckets), one client, average over thousands of random days.
- **Compression:** the raw size (8 bytes of time and 4 bytes of value per
  reading, 1.2 GB) divided by the size on disk.
- **Inserts:** embedded, 4 threads (DuckDB got its rows sorted beforehand, not
  counted); server, 4 connections for tsdb, each system's recommended loader
  for the others.
- (\*) QuestDB stores its data uncompressed by default, as measured here,
  which is why it is larger than the raw data. Its open source version can
  compress only if you convert partitions to Parquet by hand or run it on a
  compressing file system such as ZFS; converting partitions automatically as
  they age is a feature of its paid Enterprise edition.
- Coming with the next measurement: ClickHouse embedded (chDB) in the first
  table, Prometheus in the second, and columns for LTTB downsampling and
  time-weighted averages.

**Compression per kind of sensor (values only)**

| sensor | tsdb: bits per reading | compression | 1 million readings | Gorilla (InfluxDB, Prometheus, ClickHouse) | ALP (DuckDB) |
|---|---:|---:|---:|---:|---:|
| SMART disk temperature, 0.1 °C, every 60 s | **0.88** | 36.4x | 110 KB | 2.66 | 2.36 |
| CPU temperature with spikes, every 1 s | **2.99** | 10.7x | 374 KB | 10.82 | 4.97 |
| tank pressure, 0.001 bar, every 10 s | **1.82** | 17.6x | 228 KB | 9.09 | 5.97 |
| tank pressure converted to psi (not rounded) | **19.50** | 1.6x | 2.44 MB | 23.77 | - |
| vibration, 16-bit sensor at 1000 readings/s | **14.02** | 2.3x | 1.75 MB | 21.51 | - |
| pipeline flow rate, 0.01 m³/h | **10.72** | 3.0x | 1.34 MB | 23.08 | 12.29 |
| pipeline flow totalizer, m³ | **1.68** | 19.0x | 210 KB | 8.68 | - |
| random numbers (no pattern at all) | **27.29** | 1.2x | 3.41 MB | 32.32 | - |

- Bits per reading for the value alone: a raw float takes 32. Timestamps come
  on top and cost almost nothing on a steady clock. Measured with
  `build/codec-bench`; sizes are exact.
- Every method here has zstd on top. ClickHouse adds zstd to Gorilla like this;
  InfluxDB, Prometheus and DuckDB do not, so their real sizes are larger than
  shown: the comparison favours them.
- This table compares compression methods. The next full measurement replaces
  it with each database's real size on disk per kind of sensor, one column per
  database.
- "-": ALP does not apply to that data (it is not in fixed decimal steps); DuckDB
  then uses another method.
- The more regular the sensor, the bigger the gain: slow, rounded sensors
  shrink 10 to 36 times; noisy or unrounded data has less pattern to remove.

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

This part is our own implementation, built on ideas from ALP (Afroozeh, Kuffó
and Boncz, SIGMOD 2024), the method DuckDB also uses for decimals:

- tsdb looks at a small sample of each block to find the step size, instead of
  trying every possibility on every reading;
- the odd reading that does not fit the step (a glitch, a missing value) is
  stored on the side as an exception, so it does not spoil the whole block;
- the changes are packed in groups of 1,024 at a fixed number of bits each, so
  unpacking them is a quick, simple loop.

What we added for sensor data: steps that are powers of two as well as of ten
(a 16-bit sensor's raw counts), storing the changes between neighbouring
readings rather than the readings themselves, and choosing per block.

**Everything else.** When values do not follow fixed steps, tsdb tries other
methods: comparing each value's bits with the previous value's and storing only
the bits that changed (the Gorilla and Chimp methods), or ALP's second method,
which splits each value into its first bits, which repeat a lot and go into a
small dictionary, and its last bits, which are stored as they are. Each block
tries the methods and keeps the smallest result. Then a general-purpose
compressor (zstd) goes over it, but only where that still makes it smaller.

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

You need a C compiler (C11), libzstd, and liburing for the server (not with `make NO_URING=1`).

### Quick start (5 minutes, one machine)

This works from a fresh checkout. It only writes under `/tmp/tsdb-qs`, needs no root,
and, besides the build requirements above, needs no tools other than `make`, `awk` and a shell. Paste the whole block into a
terminal, in the top directory of the checkout.

<!-- quickstart: tests/test_quickstart.sh runs this block word for word -->
```sh
# 1. Build. Everything lands in build/.
make

# 2. One day of sample data: one reading a minute from the sensor "pump7.flow".
#    A line is: series name, time in milliseconds since 1970, value.
#    LC_ALL=C makes awk write 50.04, not 50,04, whatever language your system speaks.
rm -rf /tmp/tsdb-qs && mkdir -p /tmp/tsdb-qs
LC_ALL=C awk 'BEGIN { for (i = 0; i < 1440; i++) printf "pump7.flow,%d000,%.2f\n", 1717200000 + i * 60, 50 + 10 * sin(i / 229) }' > /tmp/tsdb-qs/rows.csv

# 3. Put it in a database. A database is just a directory; tsdb creates it.
build/tsdb /tmp/tsdb-qs/db import < /tmp/tsdb-qs/rows.csv

# 4. Ask for it: the first 3 rows, the average of every hour, and a summary of every day in Oslo.
build/tsdb /tmp/tsdb-qs/db query pump7.flow 2024-06-01 2024-06-02 raw --limit 3
build/tsdb /tmp/tsdb-qs/db query pump7.flow 2024-06-01 2024-06-02 avg 1h
build/tsdb /tmp/tsdb-qs/db query pump7.flow 2024-06-01 2024-06-03 all 1d --tz Europe/Oslo

# 5. The same from a C program, with no server: it writes a day and reads it back.
build/embedded-example /tmp/tsdb-qs/lib

# 6. Start the server in the background. It saves its process id, so we can stop exactly this one.
build/tsdbd --dir /tmp/tsdb-qs/server --port 17070 > /tmp/tsdb-qs/tsdbd.log 2>&1 &
echo $! > /tmp/tsdb-qs/tsdbd.pid
for i in $(seq 100); do grep -q serving /tmp/tsdb-qs/tsdbd.log && break; sleep 0.1; done

# 7. A client writes a day to the server and reads it back over the network.
build/client-example 127.0.0.1 17070

# 8. Stop the server, wait until it has gone (it finishes its writes first), then look at what it stored.
kill $(cat /tmp/tsdb-qs/tsdbd.pid)
while kill -0 $(cat /tmp/tsdb-qs/tsdbd.pid) 2>/dev/null; do sleep 0.1; done
build/tsdb /tmp/tsdb-qs/server stats
```

What you should see (shortened). Steps 3 and 4. The test checks every line here against a real run,
except the lines with `...` and the timings:

<!-- quickstart-output -->
```
inserted 1440 rows in 0.00 s (563951 rows/s)

2024-06-01 00:00:00.000    50
2024-06-01 00:01:00.000    50.0400009
2024-06-01 00:02:00.000    50.0900002
3 rows in set. Elapsed: 0.000052 sec. ...

2024-06-01 00:00:00.000    51.2811666
2024-06-01 01:00:00.000    53.799
...                                        (24 lines, one per hour)
24 buckets in set. Elapsed: 0.000065 sec. ...

time                       count   sum                  min                 max                 avg
2024-05-31 22:00:00.000    1320    66304.249984741211   40                  60                  50.2304924
2024-06-01 22:00:00.000    120     5696.0299949645996   45.040000915527344  50.009998321533203  47.4669166
2 buckets in set. Elapsed: 0.000028 sec. ...
```

The times are in UTC. Oslo is two hours ahead in summer, so midnight in Oslo is 22:00 UTC the evening before.
Our day of data lies in two Oslo days, so you get two lines.

Steps 5 and 7 print the same lines, one from the library and one from the server:

<!-- quickstart-output -->
```
wrote 1440 rows
read 1440 rows, the first: 1717200000000 50.00
24 hours, the first average: 51.28
Oslo day 1: 1320 rows, average 50.23
Oslo day 2: 120 rows, average 47.47
```

Step 6 writes this line to `/tmp/tsdb-qs/tsdbd.log` (the details differ on your machine):

```
tsdbd: serving /tmp/tsdb-qs/server on 127.0.0.1:17070, 32 workers, up to 10000 connections, simd avx512, io io_uring
```

Step 8 prints a table with `rows 1440`: what the client wrote is on disk in the server's directory.

If the server cannot start because the port is taken (the log then says "cannot listen"), something else uses 17070.
Pick another number in steps 6 and 7. If the server is still running after a mistake, stop it
with `kill $(cat /tmp/tsdb-qs/tsdbd.pid)`. Do not use `pkill` or `killall`: they stop
every process with that name, also ones that are not yours. When you are done, `rm -rf /tmp/tsdb-qs`.

The two programs are short and worth reading. Each is a complete program that `make` builds:

- [`examples/embedded.c`](examples/embedded.c): tsdb as a library, inside your own program.
- [`examples/client.c`](examples/client.c): a program that talks to `tsdbd`.

The Rust, Python and Java clients have their own quick starts (see "As a server" below).

### On another machine

`make` builds for this machine's CPU. To run tsdb on another one, AlmaLinux 10
for example, build a portable tarball and copy it over:

```sh
make dist       # build-dist/tsdb-VERSION-x86_64-linux.tar.gz
```

It runs on any x86-64 Linux with glibc 2.34 or newer and an x86-64-v2 CPU
(Intel Nehalem, Sandy Bridge and later; AMD Bulldozer and later), so on both the default and the x86-64-v2 builds of
AlmaLinux 10, and it picks faster code at startup on CPUs with AVX2 or AVX-512.
Unpack it there, run `tests/run-tests.sh` once if you like, and start
`bin/tsdbd`; the tarball's README.dist has the details and a systemd unit.
`make dist-check-almalinux10` runs its tests in an AlmaLinux 10 container (Podman).

### From the command line

The quick start used these. Use a directory you can write to instead of `/data/sensors`.

```sh
build/tsdb /data/sensors import < rows.csv          # lines of: series,time,value
build/tsdb /data/sensors query pump7.flow 2024-01-01 2024-01-02 avg 5m
build/tsdb /data/sensors query pump7.flow now-1h now raw --desc --limit 10
build/tsdb /data/sensors query pump7.flow 2024-01-01 2025-01-01 lttb 1000   # 1000 points for a chart
build/tsdb /data/sensors query pump7.flow 2024-01-01 2025-01-01 all 1mo --tz Europe/Oslo
build/tsdb /data/sensors query pump7.temp 2024-01-01 2024-02-01 twa 1d --linear --max-gap 10m
```

Run `build/tsdb` without arguments for all commands, time formats and groups.

### As a library in C

See [`include/tsdb.h`](include/tsdb.h) and the complete program
[`examples/embedded.c`](examples/embedded.c). The key lines:

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

`times` (milliseconds since 1970) and `values` are arrays of `n` rows, and `start` and `end` are
times too; the example program fills them in. Include `tsdb.h`, and `tsdb_calendar.h` for time zones,
and link with `build/libtsdb.a -lzstd -lpthread -lm`.

### As a server

```sh
build/tsdbd --dir /data/sensors --port 7070
build/tsdbd --dir /mnt/sensors --port 7070 --follow    # a read-only copy that follows the writer
```

On AlmaLinux and RHEL 10, where io_uring is disabled by default, tsdbd runs on epoll by itself
(`--io auto`, the default; `--io epoll` skips the attempt) and says so when it starts;
`make NO_URING=1` builds it without io_uring, so liburing need not be installed there.

`--metrics-port N` serves Prometheus metrics (`GET /metrics`) on a second port of its own: rows and
bytes on disk, parts, merges, WAL, request and error counters, a query latency histogram, connections,
fail-stop and follower refresh age. See [docs/metrics.md](docs/metrics.md).

| client | quick start |
|---|---|
| C | [`client/tsdb_client.h`](client/tsdb_client.h), [`examples/client.c`](examples/client.c) |
| Rust | [client/rust](client/rust/README.md) |
| Python | [client/python](client/python/README.md) |
| Java 17+ | [client/java](client/java/README.md) |

The key lines of the C client (link with `build/libtsdbclient.a -lzstd -lpthread -lm`):

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
make jepsen        # crash tests: kill -9, power loss, pauses, disk errors
make test-clients  # the Rust, Python and Java clients (needs cargo, python3, mvn)
make integration-tests  # tsdbd, the CLI and all four clients end to end, both server backends (same tools)
```
