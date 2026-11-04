# tsdb

[![CI](https://github.com/IntelliStream-DataHub/intellistream-tsdb/actions/workflows/ci.yml/badge.svg)](https://github.com/IntelliStream-DataHub/intellistream-tsdb/actions/workflows/ci.yml)

A small, fast time series database written in C.

At [IntelliStream](https://intellistream.ai) we work a lot with industrial
data: sensors on pumps, tanks, pipelines and machines that report a value every
second or every minute, for years. Our DataHub platform first stored this data in
ClickHouse, because of everything we tried it had the best balance between
compression and speed. But ClickHouse is a general analytics database, and time
series are only one of the many things it has to be good at. With a coding model
as capable as Claude Opus 5.5, we asked a simple question: what if we wrote a
database that does nothing but time series, from scratch, keeping the best ideas
of ClickHouse's storage design and adding ALP-style compression for the sensor
values? tsdb is the result. We built it to:

- **store that data for less money**: tsdb shrinks it to about a tenth of its
  raw size, and needs less than half the disk ClickHouse needs for the same
  data (the median over 31 data sets we test with);
- **cost less to run** than a ClickHouse cluster: one small program that works
  well with its defaults, and no cluster to look after;
- **answer time series questions faster**: "the 5-minute averages of this
  sensor yesterday" takes 0.09 ms, 34 times faster than ClickHouse.

We compared it with ClickHouse, InfluxDB, VictoriaMetrics, QuestDB, TimescaleDB
and others on the same machine and the same data; the numbers are below.

tsdb uses the same storage design as ClickHouse (MergeTree), but keeps only
what time series need. There is no SQL and there are no tables: you store
**series** (one per sensor) of **(time, value)** points, and you read them back
by time range, either as raw points or as summaries (count, sum, min, max,
average). You can link tsdb into your program as a library, or run its server,
`tsdbd`, and talk to it from C, Rust, Python or Java.

tsdb is still at an early stage. We are adding more functionality, and more
performance optimizations for different CPU architectures.

## Features

- Values can be float32, float64, int64, bool or a state (a text label from a
  small set, like "running" or "stopped"). Any value may be missing (null).
  Times are in milliseconds.
- Writing a value for a time that already exists replaces it. You never get
  duplicates back.
- Read raw points (oldest or newest first, with a limit), or summaries per
  time bucket: fixed buckets (every 5 minutes) or calendar ones (day, week,
  month, quarter, year). The server works in UTC; the clients turn your local
  time zone into UTC for you. A limit stops early: "the last 10 hours" of a
  year of data reads about 10 hours of it.
- Downsample a series to N points that keep its shape, for charts: the peaks
  and dips stay (LTTB), and every point returned is a real stored point.
- Besides count, sum, min, max and average, each bucket tells how much the
  values spread: the range (max - min) and the standard deviation and
  variance. They stay exact for a sensor with a large offset and a small
  spread (air pressure: 101325 Pa, +- 5), and are exact for int64 values.
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
  their own. They work over the wire in tsdbd and the C,
  Rust, Python and Java clients of this release (the CLI has `delete` and
  `retention`).
- Ask about many series in one request: the last hour of 200 sensors, or a
  day of every pump by hour. The server works on the series in parallel and
  sends the answers back in your order, all read at the same instant, so a
  write to several series shows up in all of them or in none. Over the wire
  this works in tsdbd and the clients of this release.
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

We ran tsdb 0.9.0 and the other databases on one machine (AMD Ryzen 9 7950X, 64 GB RAM, NVMe disk), one at a
time, on the same data, with the same memory limit of 48 GB. Every answer was checked against tsdb's before it was
timed. Every timing was repeated 5 times. "slower" says how many times longer the other database took than tsdb.

**Servers: 100 million readings** (100 sensors, 1 million readings each). tsdb ran as a server, over TCP.

| database | size, bytes per reading | compression | ingest, rows/s | slower | read, rows/s | slower | TWA | slower | LTTB | slower | max memory |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **tsdb** | **0.78** | **15.4x** | **85.8 M** | | **1,524 M** | | **36.4 ms** | | **456 ms** | | **2.0 GB** |
| ClickHouse | 2.51 | 4.8x | 47.5 M | 1.8x | 265 M | 5.7x | 1,063 ms | 29x | 540 ms | 1.2x | 43.1 GB [1] |
| InfluxDB 2 [2] | 34.5 | 0.3x | 3.59 M | 24x | 14.6 M | 104x | not measured [3] | | not measured | | 7.9 GB |
| InfluxDB 3 | 9.59 | 1.3x | 1.34 M | 64x | 2.23 M | 685x | 8,488 ms | 233x | not measured | | 48 GB (limit, ran out) [4] |
| VictoriaMetrics | 1.60 | 7.5x | 33.4 M | 2.6x | 106 M | 14x | no function | | no function | | 5.9 GB |
| QuestDB [5] | 50.3 | 0.2x | 32.9 M | 2.6x | 75.8 M | 20x | 1,336 ms | 37x | not measured | | 47.9 GB [6] |
| TimescaleDB | 3.42 | 3.5x | 3.49 M | 25x | 23.6 M | 65x | 22,869 ms | 628x | 12,256 ms | 27x | 48 GB (limit, ran out) [4] |
| IoTDB | 2.60 | 4.6x | 4.19 M | 20x | 76.1 M | 20x | 88,214 ms | 2,423x | no function | | 18.6 GB |
| TDengine | 2.71 | 4.4x | 4.22 M | 20x | 153 M | 9.9x | 43,254 ms | 1,188x | no function | | 48 GB (limit, failed) [4] |
| Prometheus [7] | 2.83 | 4.2x | 9.46 M | 9.1x | 5.84 M | 261x | not measured | | not measured | | 1.1 GB |

- **Size:** bytes on disk per reading, the median over the 31 data sets we test with (8 made-up sensor kinds and
  23 real sensors from a hydraulic test rig).
- **Compression:** the raw 12 bytes of a reading (8 for the time, 4 for the value) divided by that size.
- **Ingest:** 100 million rows divided by the time the load took, until the loader got its last answer. tsdb syncs
  every write to disk before it answers; ClickHouse, VictoriaMetrics and QuestDB do not.
- **Read:** all 100 sensors over the whole history in buckets of 60 readings, in one request: 100 million rows
  divided by the median time. Every row is read.
- **TWA:** a time-weighted average of all 100 sensors in hourly buckets, over the whole history (the step form).
- **LTTB:** downsampling all 100 sensors over the whole history to 1,000 points each. tsdb got one request per
  sensor, one after another (on 16 connections at once it takes 42 ms); ClickHouse ran one query on 32 threads.
- **Max memory:** the most the server held, without the file cache, while it answered the questions of this
  benchmark (all of them, not only the ones in this table).

[1] ClickHouse refused one question at its own limit of 43.2 GB: 100 time-weighted averages at once, from 32
clients. At 16 clients it answered.
[2] InfluxDB is the most popular time series database: number 1 in the
[DB-Engines ranking](https://db-engines.com/en/ranking/time+series+dbms) (November 2026). InfluxDB 2 had a 24 GB
limit; for every question its time is the faster of its two query languages, InfluxQL and Flux.
[3] InfluxDB 2's time-weighted average gives other answers than tsdb's in a third of the buckets (it does not carry
readings across bucket edges), so it was not timed.
[4] The server used up the 48 GB limit and was stopped by the system, or a question failed there. This happened
with 32 clients asking about many sensors at once. The details, and comparisons at 16 clients, are in BENCHMARKS.md.
[5] QuestDB in its default format, which is not compressed. Converted to Parquet with ZSTD by hand, it needs 1.02
bytes per reading (11.7x), but converting the 100 million readings took 28 minutes.
[6] Close to the limit. QuestDB refused the largest questions at its own memory limit.
[7] Loaded by remote write; the size is that of its backfilled blocks.

**Embedded: a library inside your program**, the same 100 million readings in one process, with no network. chDB
is ClickHouse as a library.

| database | size on disk | compression | ingest, rows/s | slower | read, rows/s | slower | TWA | slower | LTTB | slower | max memory |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **tsdb** | **114 MB** | **10.5x** | **144.8 M** | | **1,697 M** | | **38.7 ms** | | **404 ms** | | not measured |
| chDB | 219 MB | 5.5x | 62.5 M | 2.3x | 23.9 M | 71x | 1,054 ms | 27x | 432 ms | 1.1x | not measured |
| DuckDB | 306 MB | 3.9x | 27.5 M | 5.3x | 23.9 M | 71x | 1,820 ms | 47x | not measured | | not measured |

- **Size:** the 100 million readings on disk, after each library's final merge. **Read:** rows summed up per
  second while answering random days of one sensor in 5-minute buckets, one call at a time. tsdb uses one thread
  per call; chDB and DuckDB use 32. TWA and LTTB as above. DuckDB got its readings sorted beforehand (not counted).

**100 billion readings: tsdb and ClickHouse** (10,000 sensors, 10 million readings each, loaded by 8 streams).

| database | size on disk | compression | ingest, rows/s | slower | read, rows/s | slower | max memory |
|---|---:|---:|---:|---:|---:|---:|---:|
| **tsdb** | **137.8 GB** | **8.7x** | **109.4 M** | | **1,910 M** | | **1.1 GB** |
| ClickHouse | 212.1 GB | 5.7x | 51.6 M | 2.1x | 886 M | 2.2x | 5.9 GB |

- **Read:** the whole table in hourly buckets, 100 billion rows divided by the median time. Time-weighted averages
  and LTTB were not measured at this size. **Max memory:** without the file cache, while answering the queries.

We left out the questions that tsdb answers from the summaries it stores, such as a count over all the data: they
measure a lookup, not reading data. The test data behaves like real sensors: slow temperatures, spikes, tanks that
fill and drain, a fast vibration sensor, flows with shutdowns, and clocks that are a few milliseconds off.

More in [BENCHMARKS.md](BENCHMARKS.md): every database question by question, time-weighted averages in batches,
failures and their reasons, memory, and how to run the benchmarks yourself. The release record, with every
system's settings, is [bench/dbcmp/results/0.9.0.md](bench/dbcmp/results/0.9.0.md), and every single measurement is
in [bench/dbcmp/results/0.9.0-tables.md](bench/dbcmp/results/0.9.0-tables.md).

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

**Version 0.9.0** is the first release ([CHANGELOG.md](CHANGELOG.md)). The disk format and the wire protocol
may still change in a 0.9.x release and freeze at 1.0; what that means for your data is in
[docs/compatibility.md](docs/compatibility.md). `tsdbd --version` and `tsdb --version` print the version.

## Getting started

You need a C compiler (C11), `make`, libzstd, and liburing for the server (not with `make NO_URING=1`).
Python 3 is used by some of the tests.

On Debian or Ubuntu:

```sh
sudo apt install build-essential libzstd-dev liburing-dev python3
```

On Fedora, RHEL or AlmaLinux:

```sh
sudo dnf install gcc make libzstd-devel liburing-devel python3
```

On RHEL or AlmaLinux, if `liburing-devel` is not found, enable the CRB repository
(`sudo dnf config-manager --set-enabled crb`), or leave liburing out and build with `make NO_URING=1`: io_uring is
disabled by default there anyway, and tsdbd then uses epoll.

The Rust, Python and Java clients need their own tools on top (`cargo`, `python3`, `mvn`); their READMEs say which.

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

time                       count   sum                  min                 max                 avg         range               stddev_pop  stddev_samp
2024-05-31 22:00:00.000    1320    66304.249984741211   40                  60                  50.2304924  20                  7.32618041  7.32895705
2024-06-01 22:00:00.000    120     5696.0299949645996   45.040000915527344  50.009998321533203  47.4669166  4.9699974060058594  1.45185671  1.45794418
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

### Which value type to choose

A series gets its type when you create it, and keeps it. Series created without a type are float32.

| your values | type | why |
|---|---|---|
| most sensors: temperatures, pressures, flows, levels (a PLC's REAL, most OPC UA values) | **float32** (the default) | smallest and fastest; about 7 significant digits, more than a sensor delivers |
| values that need more than 7 significant digits: a totalizer at 12,345,678.9 m³, energy counters, long running sums | **float64** | about 15 significant digits |
| counters and amounts in a fixed unit: liters, Wh, events | **int64** | exact, sums included; a totalizer in whole liters takes about 1.2 bits per reading |
| on/off, open/closed | **bool** | |
| modes and status: "running", "stopped", "fault" | **state** | stored as small codes, with the names kept once |

Store a value in the type it comes in. A float32 reading stored as float64 does not get more precise: 35.1
becomes 35.0999999…, which takes up to 2.6 times the space of the same readings as float32, and is slower to
read.

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
build/tsdb /data/sensors query pump7.flow 2024-01-01 2025-01-01 avg 1h --desc --limit 10   # the last 10 hours
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

tsdb_sid flow;                                        /* 64-bit; look it up by name, never count ids */
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

`--port 0` binds a free port the kernel chooses, and `--port-file FILE` writes `PORT PID` to FILE (through a temporary
file and a rename, once the server serves) and the start-up line names the port: a script or test that starts a server
reads the port from there instead of guessing a free one.

On AlmaLinux and RHEL 10, where io_uring is disabled by default, tsdbd runs on epoll by itself
(`--io auto`, the default; `--io epoll` skips the attempt) and says so when it starts;
`make NO_URING=1` builds it without io_uring, so liburing need not be installed there.

On ext4 or XFS, start tsdbd with `--verify-checksums`: those file systems keep no checksums of your data, so
without it a damaged disk block can come back as wrong values instead of an error. tsdbd reminds you at startup.
ZFS checks its data itself and needs no option.

`--verify-checksums` checks the data each query reads. Each part's index (which rows are where, and the sums,
minimums and maximums queries answer from) is checked when the part is opened and then read from the file as it
is. If the disk damages it after that, answers can be wrong until the next restart refuses the part.
`--pin-index` keeps a copy of every index in memory, as it was checked, so that cannot happen. The copy costs
about 51 bytes of RAM per granule (a block of up to 8192 rows of one series), in the server and in every
follower: about 4 MB per billion rows of busy series, but at least one granule per series and partition, so
with many slow series it adds up (a billion series take about 51 GB per partition). The metric
`tsdb_pinned_index_bytes` shows it. Turn it on where you use `--verify-checksums` and the RAM is there.

`--metrics-port N` serves Prometheus metrics (`GET /metrics`) on a second port of its own: rows and
bytes on disk, parts, merges, WAL, request and error counters, a query latency histogram, connections,
fail-stop and follower refresh age. See [docs/metrics.md](docs/metrics.md).

Queries run on `--query-threads` threads (default one per CPU). One request uses at most `--request-threads` of
them (default all), so that a few large requests leave room for the others. A small query runs on one thread; a
large one, or a request for many series, spreads over more.

| client | quick start |
|---|---|
| C | [`client/tsdb_client.h`](client/tsdb_client.h), [`examples/client.c`](examples/client.c) |
| Rust (package `intellistream-tsdb-client`, `use tsdb_client::...`) | [client/rust](client/rust/README.md) |
| Python (package `intellistream-tsdb-client`, `import tsdb_client`) | [client/python](client/python/README.md) |
| Java 17+ | [client/java](client/java/README.md) |
| Java 22+, embedded (no server) | [client/java-embedded](client/java-embedded/README.md) |

The key lines of the C client (link with `build/libtsdbclient.a -lzstd -lpthread -lm`):

```c
tsdbc *c;
tsdbc_connect("127.0.0.1", 7070, &c);
tsdb_sid id;                                         /* 64-bit; look it up by name, never count ids */
tsdbc_series(c, "pump7.flow", 1, &id);
tsdbc_insert_series(c, id, times, values, n);
tsdbc_query_agg(c, id, start, end, 60000, &b, &nb);    /* one summary per minute */
tsdbc_close(c);
```

All four clients give up on a connect after 10 seconds and turn TCP keepalive
on (a server that vanished without a word is noticed about a minute later).
A request waits as long as the server takes unless you set a timeout
(`tsdbc_set_timeout`); after a timeout or any connection error the client
refuses further calls on that connection, so close it and connect again.

### Backup and moving to a new version

`export` writes the whole database (or some series) into one file; `import` reads it into another database.
Use it for backups, to move a database to another disk or machine, and when a new tsdb version cannot open
the files of an older one (before 1.0 that can happen in a 0.9.x release; the release notes say so):

```sh
build/tsdb /data/sensors export -o sensors.tsdbdump      # with the OLD tsdb; also while tsdbd is running
build/tsdb /data/sensors-new import sensors.tsdbdump     # with the NEW tsdb, into a new directory
build/tsdb /data/sensors export --series 'pump7.*' --from 2024-01-01 --to 2024-02-01 | ssh other \
    tsdb /data/sensors import                             # some series and one month, straight to another host
```

The file has what your queries see (deleted rows are gone), with each series' type, retention and state names,
every value exactly as stored, and a checksum on every piece. Every later tsdb can import it. `import` checks
the whole file before it writes anything, so a damaged file changes nothing; `import --check FILE` only checks.
Series that already exist are refused unless you pass `--skip-existing`, `--merge` or `--replace`. Import into
a new or empty directory, or into an existing database: a directory with other files in it is refused, and
nothing is written there. Stop tsdbd before importing into its directory. [docs/export-format.md](docs/export-format.md) describes the file.

**If an import stops halfway.** Into a new database (a directory that is missing or empty), `import` builds
the database in a directory beside it, `DIR.importing-<number>`, without waiting for the disk, makes it safe
on disk once at the end, and only then gives it its name. So an interrupted import (a crash, a power loss, a
kill, an error) leaves no database under that name. After an error it removes the files it wrote itself, in the
volumes too, and nothing else: your other files, and a volume directory that was there before, stay. After a
crash or a kill it may leave the `.importing` directory and tsdb files in the volumes it added; the next import
into the same directory names them. Delete them and run the import again.
Into an existing database, `import` writes as carefully as tsdbd does: if it stops halfway, run it again with
`--merge`, which writes the same rows again.

**Upgrading a database with volumes.** Series in volumes (other directories, such as NFS mounts) keep their
volume path in the file. The volume directories of the old database belong to it, so the new database needs
volume directories of its own, and you need room for two copies of the data until you delete the old one.
Tell `import` where each old volume goes: `--volume OLD=NEW` for one volume, or `--volume-root OLD=NEW` for
every volume under the directory OLD, and `--create-volumes` to add the new ones to the new database:

```sh
# old database /data/sensors with volumes /mnt/nfs1/tsdb/sensors and /mnt/nfs2/tsdb/sensors
build/tsdb /data/sensors export -o /backup/sensors.tsdbdump                 # with the OLD tsdb
build/tsdb /data/sensors-new import /backup/sensors.tsdbdump --create-volumes \
    --volume /mnt/nfs1/tsdb/sensors=/mnt/nfs1/tsdb/sensors-new \
    --volume /mnt/nfs2/tsdb/sensors=/mnt/nfs2/tsdb/sensors-new                # with the NEW tsdb
# or, when all volumes live under one directory, one rule for all of them:
build/tsdb /data/sensors-new import /backup/sensors.tsdbdump --create-volumes \
    --volume-root /mnt/tsdb/sensors=/mnt/tsdb/sensors-new
```

Then point tsdbd at the new directory, check your queries, and only then delete the old database and its
volume directories. To keep the old paths instead: export, then move the old directories aside (`mv /data/sensors
/data/sensors-old`, the same for each volume directory), then import into `/data/sensors` with `--create-volumes`
and no `--volume`: the volumes are created again at their old paths.
`--volume OLD=` (nothing after `=`) puts that volume's series into the database directory itself.

## Tests

```sh
make test          # all unit, crash-recovery and server tests
make asan          # the same, checking for memory errors
make tsan          # the same, checking threads for data races
make jepsen        # crash tests: kill -9, power loss, pauses, disk errors
make test-clients  # the Rust, Python and Java clients (needs cargo, python3, mvn)
make integration-tests  # tsdbd, the CLI and all four clients end to end, both server backends (same tools)
```

## License

tsdb is open source under the [Apache License 2.0](LICENSE). Copyright 2026 IntelliStream AS; see [NOTICE](NOTICE).
The libraries tsdb builds on, and their licenses, are listed in [docs/licenses.md](docs/licenses.md).

## Contributing and security

Want to help? [CONTRIBUTING.md](CONTRIBUTING.md) tells you how to build, test and send a change.
tsdb is made for a trusted network (no login or encryption yet): please read [SECURITY.md](SECURITY.md) before you
run it anywhere else, and to report a vulnerability.
