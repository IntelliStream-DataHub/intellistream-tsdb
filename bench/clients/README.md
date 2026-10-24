# Embedded C and the four tsdbd clients, side by side

One data set, loaded and queried by the same program in seven ways:

| row in the tables | program | path |
|---|---|---|
| embedded C, fsync on / off | `build/tsdb-bench --load`, `--qps` | libtsdb in the process: no server, no network |
| embedded Java (FFM), fsync on / off | `client/java-embedded/.../bench/EmbeddedBench.java --load`, `--qps` | `EmbeddedDb`: libtsdb in the JVM through the FFM API (Java 22+) |
| C over TCP | `build/tsdbc-bench` | `client/tsdb_client.c` to tsdbd |
| Rust over TCP | `client/rust/examples/insert_bench.rs`, `query_bench.rs` | `tsdb-client` crate |
| Python over TCP | `client/python/bench_insert.py`, `bench_query.py` | `tsdb_client` (PyO3 over the Rust crate), threads |
| Java over TCP | `client/java/.../bench/InsertBench.java`, `QueryBench.java` | `TsdbClient` |

The difference between the first and the other rows is what the server and the network path cost; between the
TCP rows, what each client library costs.

## Running it

```sh
bench/clients/run.sh                       # 100 series x 1M points, 3 repetitions, about 1 to 2 hours
bench/clients/run.sh --work /data/cbench   # keep the data on a disk of your choice (default /var/tmp/cbench)
bench/clients/run.sh --series 10 --points 100000 --repeat 1 --seconds 1   # a smoke run: minutes, numbers meaningless
```

Options: `--port` (tsdbd, default 17070), `--conns "1 4 16"` (loader connections or threads), `--qthreads "1 32"`,
`--clients "c rust python java java-embedded"`, `--only load|query`, `--repeat N`, `--seconds S` (per query class), `--no-build`,
`--force`.

It needs cargo (Rust), python3 with venv and pip (a virtual environment `build/pyvenv` gets maturin and numpy;
set `PYVENV` to use another), and Java 17+ with Maven; zstd-jni is taken from `~/.m2`. The Python client
runs with numpy, which it uses to slice the file.

**Run it on a quiet machine.** The script refuses to start when the 1-minute load average is above 2 (`--force`
overrides, and the numbers then say nothing). Nothing else should run meanwhile: no builds, no tests, no
containers. Before every measurement it also waits (up to a minute, then stops the run) until other processes keep
no more than one CPU busy, measured over a second from `/proc/stat` (the load average cannot be used there: it
still carries the benchmark's own previous run); the load average and the busy CPUs before every measurement are
kept and printed with the results. It refuses to start when something already listens on `--port`.

What it does:

1. Builds everything (`make`, `cargo build --release --examples`, `maturin develop --release`, `mvn package`).
2. Records the machine: CPU, memory, kernel, governor, file system, git revision, tool versions.
3. Exports the data set once with `tsdb-bench --export` (mixed sensor kinds, time-major).
4. Loads, per repetition: embedded C and embedded Java with 1, 4 and 16 threads and fsync on and off; then for each client 1, 4 and
   16 connections with zstd level 1 (the client default) and with plain arrays. Before every TCP load the
   tsdbd data directory is wiped and tsdbd restarted. After every load the contents are checked against the
   file: in every series the same row count and sum (from the aggregate path the query benchmarks use) and the
   same rows read back, checked by a hash over all (timestamp, value) pairs, so a loader that stores a wrong
   timestamp fails (`tsdb-bench --load` does it in process, `tsdb-bench --verify` after the embedded Java load,
   `tsdbc-bench verify` through the server). A mismatch
   stops the run.
5. Queries: the data set is loaded once into the embedded database (embedded C and embedded Java query it in
   turn) and once into tsdbd (and merged, `tsdb_optimize`), then for each repetition and each of 1 and 32 threads or connections, every system runs
   the three window classes for `--seconds` each.
6. Prints Markdown (`summarize.py`): the machine, the load tables and one table per query class, each cell the
   median of the repetitions with the spread, embedded first.

## What is measured, and what is the same everywhere

- **Loads** send the whole file in contiguous slices, one per connection or thread, in batches of 1,000,000
  rows, with at most four unacknowledged batches per connection (embedded: the call returns when the rows
  are durable). The timed part starts after the series exist and the file is in memory.
- **tsdbd** runs with its defaults: `fsync` on and `wal_sync_ms` 0, so an insert is acknowledged once it is
  durable, as for the embedded rows with fsync on. The embedded rows with fsync off show what that
  durability costs.
- **Query windows** (`tools/cbench.h`, copied line by line into `examples/common/mod.rs`, `bench_common.py`,
  `BenchCommon.java`): each thread has its own generator, seeded `mix(thread + 991)`; a window is a random
  series and a start, `raw` 3600 samples, `agg300` 86,400 samples in buckets of 300 and `agg3600` 604,800 samples
  in buckets of 3600, in samples of the series' own step (1 ms to 60 s), so every kind of sensor gets comparable
  work. Raw rows are counted, never kept; aggregates are counted from the buckets. Each thread works for
  `--seconds`. With a small data set a window is cut short by the end of the data, the same everywhere.
- **Client CPU** is the CPU time of the whole client process over the timed part (user + system, all threads:
  `getrusage` in C, `/proc/self/stat` in Rust, `time.process_time` in Python, `ProcessCpuTime` in Java, which
  includes the JIT and the garbage collector). For the embedded rows it is the engine itself. **tsdbd CPU**
  per million rows is read from `/proc/<pid>/stat` around each TCP load.
- **Bytes on the wire** are those each client reports having written to its sockets, headers included.

Caveats worth knowing when reading the tables: the Python threads share one interpreter (the client releases
the GIL while it waits for the server or sends, but the code around each call does not run in parallel), so
32 Python connections measure the interpreter as much as the server; Java is warmed up for 1.5 s per class
(untimed, one connection, `--warmup`) before it is measured, which is most but not all of the JIT's work; with `query_threads` at its
default each query of the library or the server uses all CPUs for its own decoding, so 32 concurrent queries
compete for them, embedded or not.

## The programs

```sh
build/tsdb-bench --export data.rb --series 100 --points 1000000
build/tsdb-bench --dir /var/tmp/e --load data.rb --threads 4 [--no-fsync] [--optimize]   # loads, then verifies
build/tsdb-bench --dir /var/tmp/e --verify data.rb                                        # verifies an existing database
build/tsdb-bench --dir /var/tmp/e --qps 32 --series 100 --points 1000000 --seconds 3     # the three classes

build/tsdbd --dir /var/tmp/d --port 17070 &
build/tsdbc-bench --port 17070 --conns 4 --compression 1 load data.rb
build/tsdbc-bench --port 17070 verify data.rb          # count, sum and every (timestamp, value) of every series
build/tsdbc-bench --port 17070 optimize                # flush and merge
build/tsdbc-bench --port 17070 --conns 32 --series 100 --points 1000000 --seconds 3 query
client/rust/target/release/examples/query_bench --addr 127.0.0.1:17070 --conns 32
build/pyvenv/bin/python client/python/bench_query.py --addr 127.0.0.1:17070 --conns 32
java --enable-native-access=ALL-UNNAMED -cp client/java/target/tsdb-client-0.1.0-SNAPSHOT.jar:$ZSTD_JNI \
    ai.intellistream.tsdb.bench.QueryBench --port 17070 --conns 32
```

Every program prints one human line per measurement and one `RESULT kind=... client=...` line that
`run.sh` collects into `results-*/results.log`.

## What stops or flags a run

`run.sh` exits non-zero when a load does not store the file's rows, when a preparation step fails (the query data
set's loads, `optimize`, `verify`), when a query class returns no rows (an empty or wrong server; `--qps` also
refuses a directory that is not a database instead of creating one), and when `summarize.py` finds that the
systems did not do the same query work: rows per query must agree between the systems within 10 % for every class
and thread count (raw is exactly the 3600-sample window), which catches a client drawing other windows.
`make bench-clients-smoke` tests these checks on a tiny set: wrong row count, one wrong value, every timestamp
shifted by 1 ms and one row moved by 500 ms are all rejected, by the TCP and the embedded verification.
