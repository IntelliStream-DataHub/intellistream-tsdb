# Embedded and tsdbd clients

```
date:      2026-10-30T00:27:32Z
cpu:       Intel(R) Core(TM) i9-10850K CPU @ 3.60GHz, 20 logical CPUs
memory:    31 GiB
kernel:    Linux 6.17.0-42-generic
governor:  powersave
data dir:  /var/tmp/cbench-main (ext4)
git:       eb58c21
load avg at start: 1.75 6.06 4.76
cc:        cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
run:       series=100 points=1000000 repeat=5 seconds/class=3 conns=(1 4 16) qthreads=(1 32) embedded=(c) clients=(c)
tsdbd:     defaults: fsync on, wal_sync_ms 0 (an insert is acknowledged once durable), zstd 1 storage, 20 workers
```

Load average (1 min) before each measurement: 1.95 to 15.77. It includes this benchmark's own earlier runs.

CPUs kept busy by other processes in the second before each measurement: 0.34 to 0.88.

Correctness: 62 loads checked against the file (row count, sum and every timestamp and value of every series), 0 mismatches.

Each cell is the median of the repetitions (± half the spread, as a percentage of the median). Embedded rows call libtsdb in this process (from C, Rust or Python); the others go through tsdbd on 127.0.0.1.

## Load: M rows/s

|  | 1 conn/thr | 4 conn/thr | 16 conn/thr |
|---|---:|---:|---:|
| embedded C, fsync on | 30.07 (±1%) | 93.08 (±4%) | 146.93 (±3%) |
| embedded C, fsync off | 33.73 (±1%) | 117.54 (±3%) | 197.04 (±1%) |
| C over TCP, zstd level 1 | 19.36 (±3%) | 59.08 (±1%) | 91.03 (±1%) |
| C over TCP, plain | 26.97 (±1%) | 79.47 (±8%) | 87.18 (±4%) |

## Load: client CPU seconds per M rows (the whole client process)

|  | 1 conn/thr | 4 conn/thr | 16 conn/thr |
|---|---:|---:|---:|
| embedded C, fsync on | 0.05 | 0.05 | 0.07 |
| embedded C, fsync off | 0.05 | 0.05 | 0.07 |
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
| C over TCP, zstd level 1 | 0.07 | 0.07 | 0.11 |
| C over TCP, plain | 0.07 | 0.06 | 0.06 |

## Queries: raw, 3600 samples

|  | 1 thr: q/s | µs/q | Mrows/s | client CPU µs/q | 32 thr: q/s | µs/q | Mrows/s | client CPU µs/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| embedded C | 37788 (±1%) | 26.5 | 136.0 | 26 | 429527 (±4%) | 74.5 | 1546.3 | 46 |
| C over TCP | 15491 (±3%) | 64.6 | 55.8 | 12 | 86517 (±1%) | 369.9 | 311.5 | 18 |

## Queries: agg, 86400 samples in buckets of 300

|  | 1 thr: q/s | µs/q | Mrows/s | client CPU µs/q | 32 thr: q/s | µs/q | Mrows/s | client CPU µs/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| embedded C | 13137 (±1%) | 76.1 | 1135.0 | 304 | 41905 (±3%) | 763.6 | 3620.6 | 465 |
| C over TCP | 8750 (±1%) | 114.3 | 756.0 | 15 | 35654 (±2%) | 897.5 | 3080.5 | 23 |

## Queries: agg, 604800 samples in buckets of 3600

|  | 1 thr: q/s | µs/q | Mrows/s | client CPU µs/q | 32 thr: q/s | µs/q | Mrows/s | client CPU µs/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| embedded C | 6138 (±1%) | 162.9 | 3712.4 | 2390 | 8049 (±3%) | 3975.5 | 4868.2 | 2422 |
| C over TCP | 5010 (±0%) | 199.6 | 3029.8 | 15 | 7968 (±1%) | 4015.9 | 4819.3 | 21 |

µs/q is the time one thread or connection spends per query; client CPU is all threads of the client process, so embedded rows include the engine's own decoding threads and Java the JIT and the collector.
