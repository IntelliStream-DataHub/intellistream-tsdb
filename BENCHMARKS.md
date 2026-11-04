# tsdb benchmarks

This page explains how fast tsdb 0.9.0 is, and how small it stores data, next to other time series databases. The
[README](README.md#how-it-compares) has the short summary. The release record, with every system's version,
settings and memory limit, is [bench/dbcmp/results/0.9.0.md](bench/dbcmp/results/0.9.0.md). Every single
measurement, with its spread and number of runs, is in
[bench/dbcmp/results/0.9.0-tables.md](bench/dbcmp/results/0.9.0-tables.md).

## How it was run

- **One machine:** AMD Ryzen 9 7950X (16 cores, 32 threads), 64 GB RAM, an NVMe disk. One database at a time,
  every server in Podman on 127.0.0.1. Measured from 2026-10-29 to 2026-11-04.
- **The same memory for everyone:** every server, tsdb's too, ran with a hard limit of 48 GB and no swap. InfluxDB 2
  had 24 GB (it used at most 8.3 GB). A server that ran out, or refused a question because of its own limit, is
  shown as such: that is a result.
- **Answers checked first:** every answer was compared with tsdb's before it was timed. A system that gave other
  answers was not timed for that question.
- **5 runs:** every timing was repeated 5 times. "Median" is the median time of one request, averaged over the 5
  runs. "Requests per second" is the mean over the runs.
- **The data:** 100 sensors with 1 million readings each, 100 million readings in all, from seven kinds of sensors:
  disk temperatures that creep by 0.1 °C, CPU temperatures with spikes, tanks that fill and drain, a fast
  vibration sensor, pipeline flows with shutdowns, a flow totalizer, and random numbers. Most clocks are 5 to 50
  ms off their schedule. The 100-billion-row run uses 10,000 sensors with 10 million readings each.
- **What a question is:** a day is 86,400 readings in 288 buckets of 5 minutes; a week is 604,800 readings in 168
  hourly buckets. "Raw" is 3,600 readings as they are. "100 series" means one request names 100 sensors: tsdb
  answers that as one request, SQL databases get one query for the 100.
- **Statistics lookups are left out:** tsdb keeps a summary (count, sum, min, max and more) for every block of 8192
  readings. A question whose buckets cover whole blocks, such as a count over all the data, is answered from those
  summaries in microseconds. That measures a lookup, not reading data, so such questions are left out here (they
  are in the full tables). Every bucket in the tables below is smaller than a block, so tsdb reads the data.
- **16 clients:** some systems ran out of memory when 32 clients asked about 100 sensors at once. Those questions
  were timed again at 16 clients, for that system and for tsdb. The 16-client row is the fair comparison there.
- **Ratios:** "slower" says how many times longer the other database took than tsdb (5x: 5 times as long).

## The summary

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
  sensor, one after another.
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
with 32 clients asking about many sensors at once. The details, and comparisons at 16 clients, are in the sections below.
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

## ClickHouse

ClickHouse 26.8.2.7, a general analytics database and the one tsdb's storage design comes from. Table
`PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)` with DoubleDelta and Gorilla codecs plus ZSTD(1), its best on
this data. It does not sync a write to disk before it answers.

- **Size:** the median over the 31 data sets is 2.9 times tsdb's (with ZSTD(22) after OPTIMIZE FINAL, its smallest
  setting: 2.6 times).
- **Load:** 2.11 s for the 100 million readings, then 0.22 s to sync; its merges settled 24.9 s later. tsdb: 1.17 s,
  every write synced.

**Where tsdb is furthest ahead of ClickHouse** (one client):

| question | tsdb | ClickHouse | slower |
|---|---:|---:|---:|
| one day of 100 sensors | **1.29 ms** | 125 ms | 97x |
| one week of 100 sensors | **4.89 ms** | 274 ms | 56x |
| one week of one sensor | **0.125 ms** | 5.60 ms | 45x |
| one day of one sensor | **0.090 ms** | 3.09 ms | 34x |

The gap is largest when one request names many sensors: tsdb answers all 100 in one go.

All questions, median time of one request:

| question | clients | tsdb | ClickHouse | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 191 ms | 23x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 377 ms | 5.7x |
| one day of one series | 1 | **0.090 ms** | 3.09 ms | 34x |
| one day of one series | 32 | **0.466 ms** | 8.92 ms | 19x |
| one week of one series | 1 | **0.125 ms** | 5.60 ms | 45x |
| one week of one series | 32 | **1.45 ms** | 29.9 ms | 21x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 1.46 ms | 33x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 3.89 ms | 11x |
| one reading | 1 | **0.032 ms** | 1.06 ms | 33x |
| one reading | 32 | **0.237 ms** | 2.88 ms | 12x |
| one day of 100 series | 1 | **1.29 ms** | 125 ms | 97x |
| one day of 100 series | 32 | **28.6 ms** | 808 ms | 28x |
| one day of 100 series | 16 | **18.3 ms** | 398 ms | 22x |
| one week of 100 series | 1 | **4.89 ms** | 274 ms | 56x |
| one week of 100 series | 32 | **137 ms** | 3.71 s | 27x |
| one week of 100 series | 16 | **74.3 ms** | 1.91 s | 26x |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 64.6 ms | 53x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 283 ms | 11x |
| 3,600 raw readings of 100 series | 16 | **14.1 ms** | 274 ms | 20x |
| one reading of 100 series | 32 | **2.67 ms** | 186 ms | 70x |
| one reading of 100 series | 16 | **1.61 ms** | 102 ms | 63x |

**Time-weighted averages.** ClickHouse has no time-weighted average function, so it gets the same calculation
written in SQL with window functions. Its answers agreed with tsdb's.

| question | clients | tsdb | ClickHouse | slower |
|---|---:|---:|---:|---:|
| whole history of 100 sensors, hourly, step | 1 | **36.4 ms** | 1,063 ms | 29x |
| whole history of 100 sensors, hourly, linear | 1 | **43.3 ms** | 1,747 ms | 40x |
| 10 one-day windows | 1 | **0.843 ms** | 63.7 ms | 76x |
| 100 one-day windows | 1 | **3.62 ms** | 896 ms | 248x |
| | 32 | **96.1 ms** | 7.39 s | 77x |
| | 16 | **50.2 ms** | 5.63 s | 112x |
| 10 one-week windows | 1 | **1.98 ms** | 102 ms | 52x |
| 100 one-week windows | 1 | **18.0 ms** | 1.30 s | 72x |
| | 32 | **550 ms** | failed [c] | |
| | 16 | **279 ms** | 11.0 s | 39x |

**LTTB** (all 100 sensors downsampled to 1,000 points each): tsdb 456 ms with one request per sensor on one
connection, ClickHouse 540 ms in one query (tsdb 1.2x faster).

**Memory:** at most 5.6 GB without the file cache over a pass of every question; 43.1 GB in the failed batch [c].

[c] With 32 clients asking for 100 one-week time-weighted averages each, ClickHouse refused 3 of 5 runs at its own
memory limit of 43.2 GB (Code 241) and the other 2 failed. At 16 clients it answered.

## InfluxDB 2 and InfluxDB 3

InfluxDB is the most popular time series database: number 1 in the
[DB-Engines ranking](https://db-engines.com/en/ranking/time+series+dbms) (November 2026, with about twice the score
of number 2, Prometheus). InfluxDB 2 remains one of its most widely deployed versions.

**InfluxDB 2.7.12** ran with its defaults, except that it compacts old data after one minute without writes, so
that its background work ends sooner. Like tsdb, it syncs a write to disk before it answers. It had a 24 GB memory
limit and used at most 7.9 GB (8.3 GB with the file cache). It has two query languages, InfluxQL (Q) and Flux (F);
each time below is the faster of the two.

- **Size:** the median over the 31 data sets is 20.4 times tsdb's (5.2 to 400 times). At its best it needs 4.8
  bytes per reading where tsdb needs 0.93; at its worst 256 bytes where tsdb needs 0.64.
- **Load:** 27.9 s (tsdb 1.17 s, 24x faster), then 664 s until its compactions went quiet (tsdb 0.05 s). Its five
  runs logged different background-work steps, but these times agree.
- **Time-weighted averages:** its `timeWeightedAvg` interpolates only inside each bucket, so a third of the buckets
  differ from tsdb's. Not timed. **LTTB:** not measured.
- **Failures:** none.

| question | clients | tsdb | InfluxDB 2 | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 2.36 s (F) | 284x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 6.85 s (F) | 104x |
| one day of one series | 1 | **0.090 ms** | 5.04 ms (F) | 56x |
| one day of one series | 32 | **0.466 ms** | 33.6 ms (Q) | 72x |
| one week of one series | 1 | **0.125 ms** | 8.43 ms (F) | 67x |
| one week of one series | 32 | **1.45 ms** | 79.7 ms (F) | 55x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 2.23 ms (Q) | 51x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 6.74 ms (Q) | 20x |
| one reading | 1 | **0.032 ms** | 0.291 ms (Q) | 9.1x |
| one reading | 32 | **0.237 ms** | 0.831 ms (Q) | 3.5x |
| one day of 100 series | 1 | **1.29 ms** | 293 ms (F) | 227x |
| one day of 100 series | 32 | **28.6 ms** | 3.34 s (Q) | 117x |
| one week of 100 series | 1 | **4.89 ms** | 348 ms (F) | 71x |
| one week of 100 series | 32 | **137 ms** | 9.04 s (F) | 66x |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 207 ms (Q) | 169x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 648 ms (Q) | 25x |
| one reading of 100 series | 32 | **2.67 ms** | 62.4 ms (Q) | 23x |

**InfluxDB 3 Core 3.12.0**, queried with SQL once its Parquet files were written. It had 48 GB of memory (an
earlier attempt used 28 GB).

- **Size:** the median over the 31 data sets is 10.3 times tsdb's (4.4 to 977 times).
- **Load:** 74.7 s (tsdb 64x faster).
- **Time-weighted averages,** whole history, hourly: step 8.49 s (tsdb 233x faster), linear 9.03 s (209x). Its
  batches were not run: the check of a single window did not finish within an hour.
- **LTTB:** not measured. **100-series batches:** not measured.
- **Failures:** with 32 clients asking for 10 one-week windows each, the kernel stopped it at the 48 GB limit in 4
  of 5 runs.

| question | clients | tsdb | InfluxDB 3 | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 44.6 s | 5,374x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 44.9 s | 685x |
| one day of one series | 1 | **0.090 ms** | 13.7 ms | 153x |
| one day of one series | 32 | **0.466 ms** | 346 ms | 742x |
| one week of one series | 1 | **0.125 ms** | 55.8 ms | 446x |
| one week of one series | 32 | **1.45 ms** | 1.67 s | 1,148x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 4.40 ms | 100x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 29.2 ms | 86x |
| one reading | 1 | **0.032 ms** | 1.37 ms | 43x |
| one reading | 32 | **0.237 ms** | 5.04 ms | 21x |

## VictoriaMetrics

VictoriaMetrics v1.154.0 (single node), a fast database for monitoring metrics, loaded by Prometheus remote
write. It answers a write from memory and writes it to disk within 5 seconds.

- **Size:** the median over the 31 data sets is 2.1 times tsdb's (1.2 to 2.6 times), the closest of the servers
  in their default setup.
- **Load:** 3.00 s, then 1.54 s to flush and sync (tsdb 1.17 s, 2.6x faster).
- **Time-weighted averages and LTTB:** its query language, MetricsQL, has neither (its `integrate()` is a step
  integral without the covered time).
- **Memory:** at most 5.9 GB. **Failures:** none.

| question | clients | tsdb | VictoriaMetrics | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 208 ms | 25x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 947 ms | 14x |
| one day of one series | 1 | **0.090 ms** | 4.95 ms | 55x |
| one day of one series | 32 | **0.466 ms** | 61.1 ms | 131x |
| one week of one series | 1 | **0.125 ms** | 21.9 ms | 175x |
| one week of one series | 32 | **1.45 ms** | 172 ms | 118x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 0.876 ms | 20x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 6.97 ms | 21x |
| one reading | 1 | **0.032 ms** | 0.408 ms | 13x |
| one reading | 32 | **0.237 ms** | 1.19 ms | 5.0x |
| one day of 100 series | 1 | **1.29 ms** | 613 ms | 474x |
| one day of 100 series | 32 | **28.6 ms** | 5.11 s | 179x |
| one week of 100 series | 1 | **4.89 ms** | 2.40 s | 492x |
| one week of 100 series | 32 | **137 ms** | 14.8 s | 108x |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 99.0 ms | 81x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 574 ms | 22x |
| one reading of 100 series | 32 | **2.67 ms** | 133 ms | 50x |

## QuestDB

QuestDB 10.0.1, a fast SQL database for time series. It does not sync a write to disk before it answers.

- **Size:** in its default format it does not compress: the median over the 31 data sets is 56 times tsdb's. Its
  open source version compresses only partitions you convert to Parquet by hand: with ZSTD the median is 1.2
  times tsdb's (1.0 to 7.4 times), the closest of all, and with LZ4 2.3 times. Converting the 100 million readings
  to Parquet with ZSTD took 28 minutes (1,702 s after the load); with LZ4 3 s.
- **Load:** 3.04 s, then 2.9 s until its WAL was applied and 1.6 s to sync (tsdb 1.17 s, 2.6x faster).
- **Memory:** close to the 48 GB limit (47.9 GB without the file cache): it keeps memory from earlier questions.

**Time-weighted averages** (a better query was written for it during the run; the older rows are not used):

| question | clients | tsdb | QuestDB | slower |
|---|---:|---:|---:|---:|
| whole history of 100 sensors, hourly, step | 1 | **36.4 ms** | 1,336 ms | 37x |
| 10 one-day windows | 1 | **0.843 ms** | 576 ms | 683x |
| 100 one-day windows | 1 | **3.62 ms** | 6.90 s | 1,908x |
| | 32 and 16 | **96.1 / 50.2 ms** | refused [d] | |
| 10 one-week windows | 1 | **1.98 ms** | 4.51 s | 2,284x |
| 100 one-week windows | 1 | **18.0 ms** | failed its answer check (Java heap space) | |

The linear form was not timed: the server was down or timed out when it was checked. **LTTB:** not measured.

Its default format, median time of one request:

| question | clients | tsdb | QuestDB | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 266 ms | 32x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 1.32 s | 20x |
| one day of one series | 1 | **0.090 ms** | 5.61 ms | 62x |
| one day of one series | 32 | **0.466 ms** | 56.7 ms | 122x |
| one week of one series | 1 | **0.125 ms** | 11.0 ms | 88x |
| one week of one series | 32 | **1.45 ms** | 166 ms | 114x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 1.19 ms | 27x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 2.47 ms | 7.3x |
| one reading | 1 | **0.032 ms** | 0.209 ms | 6.5x |
| one reading | 32 | **0.237 ms** | 0.309 ms | 1.3x |
| one day of 100 series | 1 | **1.29 ms** | 500 ms | 387x |
| one day of 100 series | 32 | **28.6 ms** | refused [a] | |
| one day of 100 series | 16 | **18.3 ms** | 2.75 s | 151x |
| one week of 100 series | 1 | **4.89 ms** | 702 ms | 144x |
| one week of 100 series | 32 | **137 ms** | refused [a] | |
| one week of 100 series | 16 | **74.3 ms** | refused [a] | |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 313 ms | 256x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 580 ms | 22x |
| 3,600 raw readings of 100 series | 16 | **14.1 ms** | 339 ms | 24x |
| one reading of 100 series | 32 | **2.67 ms** | 11.1 ms | 4.1x |
| one reading of 100 series | 16 | **1.61 ms** | 5.73 ms | 3.6x |

[a] QuestDB refused every run: "global RSS memory limit exceeded", its own limit. At 32 clients it held 19.4 GB,
15.7 GB of it from earlier questions.
[d] Refused at 32 and at 16 clients: "table busy [reason=pool size exceeded]", its pool of table readers ran out,
not its memory.

**Parquet formats**, one client: one day of one sensor 8.64 ms (LZ4) and 11.3 ms (ZSTD); one reading 0.54 ms and
1.35 ms; whole history in buckets of 3,600 readings 372 ms and 613 ms; time-weighted averages of the whole history
1,475 ms and 1,271 ms. With 32 clients asking about 100 sensors, some runs were stopped by the kernel at 48 GB;
at 16 clients the ZSTD variant answered one day of 100 sensors in 9.76 s (tsdb 18.3 ms). The LZ4 variant failed at
16 clients: one of five runs of one day of 100 sensors ("could not mmap"), and a week and single readings of 100
sensors, where no run finished (the server dropped the connection or stopped).

## TimescaleDB

TimescaleDB 2.30.1 on PostgreSQL 17, with compression on (`segmentby series, orderby ts`), set up with
timescaledb-tune. Like tsdb, it syncs a write to disk before it answers. Time-weighted averages and LTTB come from
its toolkit (the timescaledb-ha image).

- **Size:** the median over the 31 data sets is 3.9 times tsdb's (1.9 to 293 times).
- **Load:** 28.6 s with 16 workers, then 12.3 s to compress 696 chunks (tsdb 1.17 s).
- **Time-weighted averages:** whole history of 100 sensors, hourly: step 22.9 s (tsdb 628x faster), linear 22.9 s
  (528x). 10 one-day windows 1.25 s (tsdb 0.843 ms); 100 one-day windows 15.7 s (tsdb 3.62 ms); 100 one-week
  windows 68.8 s (tsdb 18.0 ms). With 16 or 32 clients, batches of 100 ran out of memory [a].
- **LTTB:** 12.3 s (tsdb 456 ms, 27x faster).
- **Memory:** it reached the 48 GB limit with many clients.

| question | clients | tsdb | TimescaleDB | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 3.60 s | 434x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 4.24 s | 65x |
| one day of one series | 1 | **0.090 ms** | 4.91 ms | 55x |
| one day of one series | 32 | **0.466 ms** | 8.66 ms | 19x |
| one week of one series | 1 | **0.125 ms** | 29.8 ms | 239x |
| one week of one series | 32 | **1.45 ms** | 55.0 ms | 38x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 0.934 ms | 21x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 2.00 ms | 5.9x |
| one reading | 1 | **0.032 ms** | 0.287 ms | 9.0x |
| one reading | 32 | **0.237 ms** | 0.449 ms | 1.9x |
| one day of 100 series | 1 | **1.29 ms** | 463 ms | 358x |
| one day of 100 series | 32 | **28.6 ms** | 1.55 s | 54x |
| one day of 100 series | 16 | **18.3 ms** | 980 ms | 54x |
| one week of 100 series | 1 | **4.89 ms** | 2.74 s | 560x |
| one week of 100 series | 32 | **137 ms** | ran out of memory [a] | |
| one week of 100 series | 16 | **74.3 ms** | 5.77 s [b] | 78x |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 108 ms | 88x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 244 ms | 9.4x |
| 3,600 raw readings of 100 series | 16 | **14.1 ms** | 147 ms | 10x |
| one reading of 100 series | 32 | **2.67 ms** | 46.8 ms | 18x |
| one reading of 100 series | 16 | **1.61 ms** | 28.2 ms | 17x |

[a] With 32 clients asking for a week of 100 sensors, the kernel stopped the server at the 48 GB limit twice, so
the question was stopped. At 32 clients it ran out of memory with 2, 1 and 0 parallel workers per query, so it
ran with its defaults; at 16 clients it ran with 2. Time-weighted averages of 100 windows ran out of memory at 16
and 32 clients with 2, 1 and 0 workers.
[b] One of five runs was stopped by the kernel and repeated; its memory stood at the limit.

## IoTDB and TDengine

**Apache IoTDB 2.0.11** (table model, its default codecs), loaded through its native tablets with 8 processes and
its WAL synced. **TDengine 3.4.2.8** (community edition), loaded by line protocol with its WAL synced.

- **Size:** IoTDB's median over the 31 data sets is 3.1 times tsdb's (with CHIMP + ZSTD: 1.9 times). TDengine's
  is 3.1 times (with zstd: 2.4 times).
- **Load:** IoTDB 23.9 s, TDengine 23.7 s (tsdb 1.17 s, 20x faster).
- **Time-weighted averages,** whole history, hourly, step: IoTDB 88.2 s (tsdb 2,423x faster), TDengine 43.3 s
  (1,188x). IoTDB's linear form: 107 s. TDengine's linear form gives other answers in 1,583 of 339,774 buckets:
  not timed. 10 one-day windows: IoTDB 371 ms, TDengine 596 ms (tsdb 0.843 ms).
- **LTTB:** neither has the function.
- **Memory:** IoTDB at most 18.6 GB, TDengine 28.6 GB, and 48 GB where it failed.
- **Failures:** IoTDB refused the 32-client day and week batches of 100 sensors with its own memory pool (code
  719, "not enough memory to execute current fragment instance"). TDengine failed 100 one-day time-weighted
  averages at 32 clients (status -1) with its memory at the 48 GB limit.

IoTDB:

| question | clients | tsdb | IoTDB | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 275 ms | 33x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 1.31 s | 20x |
| one day of one series | 1 | **0.090 ms** | 3.33 ms | 37x |
| one day of one series | 32 | **0.466 ms** | 7.91 ms | 17x |
| one week of one series | 1 | **0.125 ms** | 13.2 ms | 106x |
| one week of one series | 32 | **1.45 ms** | 28.7 ms | 20x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 1.54 ms | 35x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 4.52 ms | 13x |
| one reading | 1 | **0.032 ms** | 0.929 ms | 29x |
| one reading | 32 | **0.237 ms** | 2.62 ms | 11x |
| one day of 100 series | 1 | **1.29 ms** | 312 ms | 241x |
| one day of 100 series | 32 | **28.6 ms** | refused [a] | |
| one week of 100 series | 1 | **4.89 ms** | 376 ms | 77x |
| one week of 100 series | 32 | **137 ms** | refused [a] | |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 201 ms | 165x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 989 ms | 38x |
| one reading of 100 series | 32 | **2.67 ms** | 666 ms | 249x |

TDengine:

| question | clients | tsdb | TDengine | slower |
|---|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 124 ms | 15x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 652 ms | 9.9x |
| one day of one series | 1 | **0.090 ms** | 2.73 ms | 30x |
| one day of one series | 32 | **0.466 ms** | 10.3 ms | 22x |
| one week of one series | 1 | **0.125 ms** | 8.29 ms | 66x |
| one week of one series | 32 | **1.45 ms** | 20.0 ms | 14x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 2.17 ms | 49x |
| 3,600 raw readings of one series | 32 | **0.339 ms** | 8.61 ms | 25x |
| one reading | 1 | **0.032 ms** | 1.33 ms | 42x |
| one reading | 32 | **0.237 ms** | 6.42 ms | 27x |
| one day of 100 series | 1 | **1.29 ms** | 100 ms | 78x |
| one day of 100 series | 32 | **28.6 ms** | 1.23 s | 43x |
| one week of 100 series | 1 | **4.89 ms** | 147 ms | 30x |
| one week of 100 series | 32 | **137 ms** | 2.41 s | 18x |
| 3,600 raw readings of 100 series | 1 | **1.22 ms** | 113 ms | 92x |
| 3,600 raw readings of 100 series | 32 | **25.8 ms** | 1.06 s | 41x |
| one reading of 100 series | 32 | **2.67 ms** | 733 ms | 274x |

## Prometheus

Prometheus v3.15.0, loaded two ways: backfilled with promtool (595 s), and by remote write (10.6 s, tsdb 9.1x
faster). Remote write acknowledges from its head without syncing every request.

- **Size:** the median over the 31 data sets is 3.7 times tsdb's (backfilled blocks).
- **Time-weighted averages and LTTB:** not measured. **Memory:** at most 1.1 GB. **Failures:** none.

| question | clients | tsdb | backfilled | remote write | slower (backfilled) |
|---|---:|---:|---:|---:|---:|
| whole history of 100 series, buckets of 3,600 readings | 1 | **8.3 ms** | 13.7 s | 15.6 s | 1,656x |
| whole history of 100 series, buckets of 60 readings | 1 | **65.6 ms** | 15.2 s | 17.1 s | 231x |
| one day of one series | 1 | **0.090 ms** | 15.2 ms | 31.0 ms | 169x |
| one day of one series | 32 | **0.466 ms** | 47.1 ms | 138 ms | 101x |
| one week of one series | 1 | **0.125 ms** | 84.1 ms | 107 ms | 673x |
| 3,600 raw readings of one series | 1 | **0.044 ms** | 0.537 ms | 1.17 ms | 12x |
| one reading | 1 | **0.032 ms** | 0.213 ms | 0.771 ms | 6.7x |
| one reading | 32 | **0.237 ms** | 2.88 ms | 9.49 ms | 12x |

## Size per kind of sensor

Bytes per reading on disk over 31 data sets: 8 generated sensor kinds of 1 million readings each, and 23 real
sensors from a hydraulic test rig. Each size compared to tsdb's (tsdb = 1x). tsdb is the smallest on all 31.
Measured on disk format 7; format 8 did not change a part's bytes per row.

| database | smallest | largest | median |
|---|---:|---:|---:|
| **tsdb** | **1x** | **1x** | **1x** |
| QuestDB, Parquet ZSTD (converted by hand) | 1.0x | 7.4x | 1.2x |
| IoTDB, CHIMP + ZSTD | 1.2x | 3.9x | 1.9x |
| VictoriaMetrics | 1.2x | 2.6x | 2.1x |
| TDengine, zstd | 1.1x | 5.7x | 2.4x |
| ClickHouse, ZSTD(22) after OPTIMIZE FINAL | 1.2x | 4.4x | 2.6x |
| ClickHouse, its best codecs | 1.2x | 4.8x | 2.9x |
| IoTDB (default) | 1.3x | 5.4x | 3.1x |
| TDengine (default) | 1.2x | 9.5x | 3.1x |
| Prometheus (backfilled) | 1.4x | 6.6x | 3.7x |
| TimescaleDB (compressed) | 1.9x | 293x | 3.9x |
| DuckDB | 1.2x | 9.1x | 4.4x |
| InfluxDB 3 | 4.4x | 977x | 10.3x |
| InfluxDB 2 | 5.2x | 400x | 20.4x |
| QuestDB (default format) | 19x | 611x | 56x |

The generated sensor kinds, bytes per reading (raw: 12):

| sensor | tsdb | ClickHouse | VictoriaMetrics | QuestDB, Parquet ZSTD | IoTDB, CHIMP + ZSTD | TDengine, zstd | TimescaleDB | InfluxDB 2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| disk temperature, 0.1 °C, every 60 s | **0.175** | 0.449 | 0.260 | 1.286 | 0.675 | 0.879 | 51.2 | 34.5 |
| CPU temperature with spikes, every 1 s | **1.124** | 2.438 | 1.806 | 1.895 | 2.248 | 2.595 | 7.04 | 37.3 |
| tank pressure, 0.001 bar, every 10 s | **0.248** | 1.185 | 0.538 | 0.642 | 0.946 | 1.312 | 9.61 | 35.0 |
| tank pressure in psi (not rounded) | **2.202** | 3.032 | 5.031 | 3.731 | 2.540 | 2.655 | 16.2 | 36.7 |
| vibration, 16-bit sensor at 1,000 readings/s | **1.760** | 2.800 | 4.436 | 2.307 | 3.875 | 2.370 | 3.42 | 36.6 |
| pipeline flow rate, 0.01 m³/h | **1.850** | 3.914 | 3.232 | 2.658 | 3.089 | 3.234 | 6.96 | 37.8 |
| pipeline flow totalizer, m³ | **0.724** | 2.039 | 0.835 | 2.161 | 1.326 | 1.694 | 5.92 | 36.9 |
| random numbers | **3.426** | 4.058 | 8.887 | 4.017 | 4.114 | 3.730 | 6.97 | 38.2 |

The more regular the sensor, the bigger the gain. Random numbers have no pattern to remove, so every database is
close there.

## 100 billion rows: tsdb and ClickHouse

10,000 sensors with 10 million readings each, loaded the same way into both by 8 streams. ClickHouse's queries ran
after its background merges had settled.

| | tsdb | ClickHouse | slower |
|---|---:|---:|---:|
| size on disk | **137.8 GB** (1.38 bytes per reading) | 212.1 GB (2.12 bytes per reading) | |
| load | **914 s** (109.4 M rows/s) | 1,937 s (51.6 M rows/s) | 2.1x |
| load until background merges were done | **3,259 s** | 4,507 s | 1.4x |
| memory while answering, without the file cache | **1.1 GB** | 5.9 GB | |

The first tsdb run on disk format 8 took 1,480 s to load and does not count. Most of the difference was the
drive's state: that run started 4 s after another job ended, on a disk 80% full. 5 to 15% was a code regression
(page faults on every batch), fixed before the rerun. An A/B run at 32 billion rows on a rested drive took 236 s
with the old code and 236 s with the fixed one.

Median time of one query:

| question | readings | tsdb (server) | tsdb (embedded) | ClickHouse | slower (than tsdb's server) |
|---|---:|---:|---:|---:|---:|
| one random reading | 1 | **0.31 ms** | 0.26 ms | 2 ms | 6.4x |
| aggregate a window of one sensor [e] | 1 M | **0.61 ms** | 0.52 ms | 8 ms | 13x |
| one sensor by hour | 10 M | **17.2 ms** | 16.3 ms | 42 ms | 2.4x |
| one sensor by minute | 10 M | **26.9 ms** | 18.2 ms | 54 ms | 2.0x |
| 100 sensors by day, Europe/Oslo [e] | 1 G | **109 ms** | 72.9 ms | 1,071 ms | 9.9x |
| 100 sensors by hour | 1 G | **361 ms** | 360 ms | 1,100 ms | 3.0x |
| everything by day, Europe/Oslo [e] | 100 G | **41.3 s** | 38.0 s | 108.3 s | 2.6x |
| everything by hour | 100 G | **52.4 s** | 49.4 s | 112.9 s | 2.2x |

[e] Mostly answered from tsdb's block summaries: the blocks inside the window or the local day come from them, the
blocks at its edges (around midnight in Oslo) are read. Hours and minutes cut through nearly every block, so both
systems read the data, and there tsdb is 2 to 3 times faster. The 100-sensor Oslo days spread widely between runs
(medians from 13 to 162 ms). ClickHouse's benchmark tool reports whole milliseconds, so its small times are
approximate.

## Embedded: tsdb, chDB and DuckDB

The same 100 million readings in one process, through each library's C API. chDB is ClickHouse as a library (with
its codecs). DuckDB got its readings sorted by sensor and time first (0.68 s, not counted), so its indexes help it;
it has no primary key, so a newer reading does not replace an older one with the same time.

| | tsdb | chDB | DuckDB |
|---|---:|---:|---:|
| load 100 million readings | **0.70 s** (4 threads, synced) | 1.60 s | 3.65 s (4 appenders) |
| merging afterwards | 0.64 s | 5.05 s (OPTIMIZE FINAL) | 0.02 s (CHECKPOINT) |
| size on disk | **114 MB** | 219 MB (1.9x) | 306 MB (2.7x) |

Median time of one call, one thread (chDB and DuckDB run each query on 32 threads):

| question | tsdb | chDB | DuckDB | slower (the faster of chDB and DuckDB) |
|---|---:|---:|---:|---:|
| one day of one sensor | **0.050 ms** | 3.63 ms | 3.63 ms | 73x |
| one week of one sensor | **0.085 ms** | 4.94 ms | 5.50 ms | 58x |
| 3,600 raw readings of one sensor | **0.014 ms** | 1.54 ms | 0.826 ms | 59x |
| one reading | **0.010 ms** | 1.23 ms | 0.715 ms | 72x |
| time-weighted averages, whole history, hourly | **38.7 ms** | 1,054 ms | 1,820 ms | 27x |
| LTTB, whole history | **404 ms** | 432 ms | not measured | 1.1x |

With 32 calling threads, one day of one sensor: tsdb 120,182 calls per second, chDB 4,334, DuckDB 2,998. chDB with
its default LZ4 instead of codecs needs 513 MB.

## Memory

The most each server held while answering the 100-million-row questions, without / with the file cache. The pass
that sets these runs every question once, one after another, so memory left over from earlier questions counts.

| database | memory limit | peak, without / with file cache |
|---|---:|---:|
| **tsdb** | 48 GB | **1.9 / 2.0 GB** |
| VictoriaMetrics | 48 GB | 5.9 / 6.1 GB |
| ClickHouse | 48 GB | 5.6 / 20.2 GB |
| InfluxDB 2 | 24 GB | 7.9 / 8.3 GB |
| IoTDB | 48 GB | 18.5 / 18.8 GB |
| TDengine | 48 GB | 28.6 / 31.2 GB |
| QuestDB, Parquet ZSTD | 48 GB | 41.4 / 41.7 GB |
| QuestDB | 48 GB | 47.9 / 48.0 GB |
| InfluxDB 3 | 48 GB | 48.0 / 48.0 GB (ran out) |
| TimescaleDB | 48 GB | 48.0 / 48.0 GB (ran out) |
| Prometheus (remote write) | 48 GB | 1.1 / 2.5 GB, the load included |

Including the load, tsdb peaked at 1.9 / 2.0 GB as well. In the timed runs, the most tsdb held was 2.0 GB
without the file cache (2.2 GB with it). The highest in timed runs elsewhere: ClickHouse 43.1 GB in the batch it
refused, QuestDB 47.2 GB, TDengine and TimescaleDB 48 GB where they failed. At 100 billion rows, tsdb held at
most 1.1 GB and ClickHouse 5.9 GB without the file cache; with it, both fill the memory they have, since the data
is larger than the machine's RAM. The embedded libraries' memory was not measured. Every class's peak, run by run,
is in [0.9.0-tables.md](bench/dbcmp/results/0.9.0-tables.md) (the memory column and each system's memory sweep).

## Reproduce it yourself

**tsdb alone, on your machine:** `tools/bench-tsdb.sh` builds an optimized copy in `build-bench/`, runs tsdb's own
benchmarks several times, and writes a summary with the mean and spread of every number to `bench-results/`.

```sh
tools/bench-tsdb.sh --quick       # a few minutes: checks that it works here (the numbers mean nothing)
tools/bench-tsdb.sh               # --standard: 100 series x 1 million rows, 5 runs, about 40 minutes
tools/bench-tsdb.sh --full        # also float64 and int64, bigger inserts, and a large data set (about 1.5 hours)
```

`--reps N`, `--dir SCRATCH` and `--out DIR` change the runs and the places; `tools/bench-tsdb.sh --help` lists the
rest.

**The full comparison:** `bench/dbcmp/release/run.sh` runs every database, one phase per command. It needs
Podman (never Docker), gcc and make, `psql`, curl, jq, bc, Python 3, libpq and snappy's shared libraries, the
DuckDB and chDB C libraries for the embedded phase, a quiet machine with the `performance` CPU governor, and a lot
of disk: about 32 GB for the 100-million-row phases and over 320 GB free for the 100-billion-row phase.
`bench/dbcmp/README.md` has the setup; `bench/dbcmp/release/env.sh` has the paths and the pinned image digests,
which you pull first.

```sh
make && make -C bench/dbcmp
bench/dbcmp/release/run.sh smoke          # every system on tiny data; every answer check must pass
BENCH_LOCK=/path/BENCH.lock bench/dbcmp/release/run.sh prep p1 p2 p3 p4   # or: all
bench/dbcmp/release/run.sh p6             # the extra pass: TWA batches and each question's peak memory
bench/dbcmp/release/run.sh p5             # 100 billion rows, tsdb and ClickHouse (only when named)
MANY_CLIENTS=16 CLASS_FILTER=b100 P6_SYSTEMS="tsdb qdb0 qdb1 qdb2" bench/dbcmp/release/run.sh p6   # 16 clients
```

| phase | what it does |
|---|---|
| `prep` | the inputs, not timed |
| `p1` | size on disk per kind of sensor, every system and variant |
| `p2` | the 100-million-row table on every server: load, answer checks, queries |
| `p3` | embedded: tsdb, DuckDB, chDB |
| `p4` | Prometheus: backfill and remote write |
| `p5` | 100 billion rows, tsdb and ClickHouse |
| `p6` | the extra pass: time-weighted average batches, and every question's peak memory |

Every server runs with the same memory limit (`SERVER_MEM`, 48g by default). The results go to
`$DBCMP/results/`, and `tables.md` there collects every number with how each system was set up.
