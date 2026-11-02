# Embedded and tsdbd clients

```
date:      2026-10-30T00:12:18Z
cpu:       Intel(R) Core(TM) i9-10850K CPU @ 3.60GHz, 20 logical CPUs
memory:    31 GiB
kernel:    Linux 6.17.0-42-generic
governor:  powersave
data dir:  /var/tmp/cbench (ext4)
git:       2694f60 (modified)
load avg at start: 1.06 1.61 2.61
cc:        cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
run:       series=100 points=1000000 repeat=5 seconds/class=3 conns=(1 4 16) qthreads=(1 32) embedded=(c) clients=(c)
tsdbd:     defaults: fsync on, wal_sync_ms 0 (an insert is acknowledged once durable), zstd 1 storage, 20 workers
```

Load average (1 min) before each measurement: 1.20 to 15.54. It includes this benchmark's own earlier runs.

CPUs kept busy by other processes in the second before each measurement: 0.27 to 0.92.

Correctness: 62 loads checked against the file (row count, sum and every timestamp and value of every series), 0 mismatches.

Each cell is the median of the repetitions (± half the spread, as a percentage of the median). Embedded rows call libtsdb in this process (from C, Rust or Python); the others go through tsdbd on 127.0.0.1.

## Load: M rows/s

|  | 1 conn/thr | 4 conn/thr | 16 conn/thr |
|---|---:|---:|---:|
| embedded C, fsync on | 30.25 (±6%) | 89.72 (±3%) | 141.86 (±2%) |
| embedded C, fsync off | 33.79 (±1%) | 117.97 (±2%) | 195.89 (±2%) |
| C over TCP, zstd level 1 | 19.59 (±22%) | 59.80 (±27%) | 87.87 (±8%) |
| C over TCP, plain | 27.27 (±2%) | 75.96 (±5%) | 86.58 (±2%) |

## Load: client CPU seconds per M rows (the whole client process)

|  | 1 conn/thr | 4 conn/thr | 16 conn/thr |
|---|---:|---:|---:|
| embedded C, fsync on | 0.05 | 0.05 | 0.07 |
| embedded C, fsync off | 0.05 | 0.05 | 0.08 |
| C over TCP, zstd level 1 | 0.02 | 0.03 | 0.06 |
| C over TCP, plain | 0.00 | 0.01 | 0.01 |

## Load: bytes per row on the wire

|  | 1 conn/thr | 4 conn/thr | 16 conn/thr |
|---|---:|---:|---:|
| C over TCP, zstd level 1 | 5.07 | 5.07 | 5.07 |
| C over TCP, plain | 16.00 | 16.00 | 16.00 |

## Load: tsdbd CPU seconds per M rows

|  | 1 conn/thr | 4 conn/thr | 16 conn/thr |
|---|---:|---:|---:|
| C over TCP, zstd level 1 | 0.07 | 0.08 | 0.11 |
| C over TCP, plain | 0.06 | 0.06 | 0.06 |

## Queries: raw, 3600 samples

|  | 1 thr: q/s | µs/q | Mrows/s | client CPU µs/q | 32 thr: q/s | µs/q | Mrows/s | client CPU µs/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| embedded C | 46590 (±1%) | 21.5 | 167.7 | 21 | 522650 (±5%) | 61.2 | 1881.5 | 38 |
| C over TCP | 16849 (±2%) | 59.4 | 60.7 | 11 | 88876 (±1%) | 360.1 | 320.0 | 17 |

## Queries: agg, 86400 samples in buckets of 300

|  | 1 thr: q/s | µs/q | Mrows/s | client CPU µs/q | 32 thr: q/s | µs/q | Mrows/s | client CPU µs/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| embedded C | 15383 (±1%) | 65.0 | 1329.1 | 258 | 53307 (±2%) | 600.3 | 4605.7 | 367 |
| C over TCP | 9694 (±1%) | 103.2 | 837.6 | 15 | 41071 (±4%) | 779.1 | 3548.5 | 23 |

## Queries: agg, 604800 samples in buckets of 3600

|  | 1 thr: q/s | µs/q | Mrows/s | client CPU µs/q | 32 thr: q/s | µs/q | Mrows/s | client CPU µs/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| embedded C | 7366 (±2%) | 135.8 | 4454.8 | 1929 | 10147 (±3%) | 3153.6 | 6137.1 | 1936 |
| C over TCP | 5809 (±0%) | 172.1 | 3513.4 | 15 | 9847 (±2%) | 3249.6 | 5955.7 | 21 |

µs/q is the time one thread or connection spends per query; client CPU is all threads of the client process, so embedded rows include the engine's own decoding threads and Java the JIT and the collector.
