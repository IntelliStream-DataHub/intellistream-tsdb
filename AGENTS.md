# AGENTS.md

Notes for coding agents working on tsdb: an embedded time series database in
C (MergeTree-style storage), the `tsdbd` server and its client.

## Commands

```sh
make                      # library, CLI, server, client, benchmarks (into build/)
make test                 # all unit, integration and server tests
make test IO=epoll        # server tests on epoll only (where io_uring is disabled)
make asan                 # the tests under AddressSanitizer + UBSan
make tsan                 # the tests under ThreadSanitizer, in build-tsan/ (about 2.5 minutes)
make jepsen               # fault injection: kill -9, simulated power loss, pauses, EIO
make PORTABLE=1           # without -march=native
make NO_URING=1           # tsdbd on epoll only, without liburing (after make clean)
make dist                 # portable tarball (x86-64-v2 + v3/v4 codecs) in build-dist/; checks glibc and ISA
make dist-test            # the tarball's own test suite, at every CPU level (TSDB_CPU=v2|v3|v4)
make dist-check-almalinux10   # that suite in an AlmaLinux 10 container (Podman)
make dist-check-sandybridge   # TSDB_CPU=v2, and under qemu-x86_64 -cpu Westmere/SandyBridge if installed
make test-clients         # Rust, Python and Java clients against build/tsdbd (needs cargo, python3, mvn)
make hooks                # once per clone: the commit-msg hook (no session links, sign-off last)
make integration-tests    # tsdbd + CLI + C/Rust/Python/Java clients end to end, uring and epoll (~2 min, same tools);
                          # INTEG_ARGS="--only matrix,follower --io epoll"; logs in build/integ/logs/; mutate.py checks it has teeth;
                          # make integration-selftest tests the harness itself (seconds)
```

Requirements: C11 (gnu11), pthreads, libzstd, liburing (server only; not with `NO_URING=1`).
Tests take a scratch directory as their first argument
(`./build/test_tsdb /tmp/x`); default `/tmp`.

Run the tests that cover what you changed, and the sanitizers for anything
concurrent or touching memory layout. A change to how data reaches the disk
(writes, fsync, renames, recovery, merges, the WAL) also needs `make jepsen`.
`make tsan` builds into `build-tsan/`, the server under test included, and
leaves `build/` alone. `make asan` starts with `make clean`: while anything
else uses `build/` (a running tsdbd, client tests, another agent), run it in a
copy of the tree instead. Stop only the tsdbd processes you started (by pid),
never all of them (and no `pkill -f`: it matches your own shell). In test code,
signal a process only through a helper that refuses pid 0 and -1: `kill(-1, sig)`
reaches every process of the user, the terminal included, and a server that
failed to start is exactly when a test holds such a pid.

Ports: no harness picks one. Several worktrees test at once, and a port computed from the pid or found free and
released again can be taken by another run's tsdbd, whose clients then answer yours (jepsen once reported 192 million
"lost writes" that way). Start tsdbd with `--port 0 --port-file FILE` and read "PORT PID" from FILE, which tsdbd writes
(atomically, once it serves; a third number is the metrics port of `--metrics-port auto`). Check that the pid is the child you started (`tests/harness.h` also checks that the child
holds the port), and fail if the server exited or the file names someone else. A restart gets a new port; clients
reconnect to the one in the new file. `th_owns_port` remembers the listener's socket inode, so a client that asks on every
connect scans only the server's descriptors, not `/proc/net/tcp` (which grows with every reconnect). `tests/test_ports.c` is the test of this; C harnesses include `tests/harness.h`,
`tests/integration/run.py` has `Server`, and the client test helpers under `client/*/tests` do the same in their language.

Every bug fix comes with a regression test that fails without the fix (check
by building the old code). Query timings go on a quiet machine: no builds or
tests alongside them, and ClickHouse in its own container competes per
cgroup, so `nice` does not keep your work out of its way.

## Layout

- `include/tsdb.h`: public API. `include/tsdb_protocol.h`: wire protocol.
- `src/volumes.c`: the public `tsdb`, which routes each series to a volume.
  Everything else in `src/` is the engine for one directory (`eng`, declared
  in `src/internal.h`).
- `src/part.c`: part files, the writer, atomic commits (intent files).
- `src/insert.c`: sorting and writing batches. `src/memtable.c`: WAL and memtable.
- `src/merge.c`: merge selection and execution. `src/query.c`: snapshots, raw
  queries, aggregation, bucketing. `src/manifest.c`: MANIFEST, retired parts, followers.
- `src/codec*.c`: codecs; `src/simd.c`: dispatched aggregation kernels.
  `src/levels.h`: the functions compiled once per CPU level in `make dist`;
  `src/cpu.c`: picks the level at startup and dispatches to it.
- `tools/mkdist.sh`, `tools/check-glibc.sh`, `tools/check-isa.sh`, `dist/`:
  the portable tarball. `tests/distro/`: running it in containers and qemu.
- `client/tsdb_calendar.c`: time zones (TZif and POSIX rules) and the UTC
  bounds of local periods. The engine and the server know UTC only; the
  clients turn local periods into bounds (QUERY_BOUNDS). `src/calendar.c`:
  the same for embedded use (`tsdb_query_group_local`).
- `server/tsdbd.c`: the server, one event loop on io_uring or epoll (`--io`; epoll where io_uring
  is disabled, as on RHEL/AlmaLinux 10); `make test` and `make jepsen` run the server tests on both.
  `server/metrics.c`: the `--metrics-port` thread (Prometheus text format, own poll loop, counters
  the server bumps through small hooks); `tsdb_get_volume_stats` feeds it. `docs/metrics.md` lists the metrics.
  `client/`: C client library (does not link the engine);
  `client/rust`, `client/python` (PyO3 over the Rust crate), `client/java`: the other clients.
  A protocol change has to reach all four; `make test-clients` runs their tests.
- `connectors/pulsar/`: Apache Pulsar 5 IO sink (Java, a NAR) over the Java client; `mvn test` there needs
  build/tsdbd, `connectors/pulsar/test/integration.sh` runs Pulsar 5.0.0 in Podman with faults.
- `.claude/`: shared Claude Code setup. `settings.json` runs `hooks/guard-bash.sh` (refuses `pkill -f`,
  `killall`, `kill` with pid -1 or 0, `docker`) and `hooks/guard-push.sh` (refuses a push while unpushed
  commits carry a session link); `skills/tsdb-tracks/` is how changes are built, reviewed, gated and merged
  with subagents; `agents/iso-waiter.md` waits on detached runs. Personal settings go in the ignored
  `.claude/settings.local.json`. `.githooks/commit-msg`: the commit rules for everyone (`make hooks`).
- `tools/`: CLI and benchmarks. `tests/`: tests; `tests/jepsen/`: the
  fault-injection harness and the `crashfs` LD_PRELOAD power-loss simulator.

## Invariants that are easy to break

- **Last write wins.** Among parts, a higher block number is newer; memtables
  are newer than every part. Block numbers are assigned, files renamed and parts
  listed in one step under `publish_lock` (`parts_publish`, `mt_flush`).
  Allocating a block number anywhere else lets a merge span a part that is
  still in flight and resurrect older values.
- **Merges** only combine adjacent block ranges within a partition. On equal
  keys the newest source wins.
- **Range tombstones** (deletes, replaces) live only in parts, never in the WAL
  or a memtable. A part's tombstones hide rows of lower block numbers in the
  same partition, never its own rows or memtable rows. Deletes flush the
  memtable first and commit their parts under `publish_lock` like a large
  insert; a replace commits tombstones and rows in one `parts_commit`. A
  granule a tombstone touches is decoded, never answered from its statistics.
  A merge output with no rows is kept as an empty part: its `wal_seq` stops a
  stale WAL segment from replaying deleted rows back. It goes (with a partition
  left with nothing else) only once every segment up to its `wal_seq` is
  unlinked and the directory synced (`wal_removed_synced`).
- **Retention** limits live in the append-only `RETENTION` file (beside
  `SERIES`). Retention "now" (`eng_now_ms`) never goes back; queries clamp to
  the cutoff, merges drop expired rows, the merge thread drops whole expired
  partitions by retiring them. A query that reads before its start (TWA's
  carried-in reading) stops at the cutoff as well.
- **Snapshot windows.** Readers use the per-part entry windows of their
  snapshot (`sn.gi`/`sn.gj`, `snap_find`), also to step to granules outside
  their range; never `part_find` on `sn.parts`: granules a tombstone touches
  were moved to mem runs with their live rows, and a direct lookup sees them
  twice, deleted rows included. New granule statistics must be computed by the
  part writer (merges re-encode every granule a tombstone touched) and read only
  through these windows.
- **Inserts are all or nothing.** All parts of one insert are written pending,
  then committed through an intent file (`parts_commit`). Never publish them one
  by one.
- **Durability.** A new file is durable after fsync of the file and of its
  directory; a rename after fsync of the directory. In sync mode
  (`wal_sync_ms = 0`) a memtable row is visible to queries only once the WAL is
  synced up to it (`mt_collect` with `visible`).
- **Fail-stop** ("fsyncgate"). A failed write or sync whose data an
  acknowledgement may already rely on (the WAL, a directory fsync after names
  became visible, MANIFEST, SERIES, STATES, CATALOG) calls `eng_fail`: after it
  the engine acknowledges nothing, names no new file (a merge in flight drops
  its output), and `tsdb_failed` reports it;
  tsdbd exits with status 1. A retried fsync can return 0 after the kernel
  dropped the dirty pages, so it proves nothing; only a reopen, which replays
  from what is on disk, is safe. Every directory fsync goes through
  `eng_sync_dir`: they share one fd, which reports a writeback error once, so
  they take turns and none succeeds after the engine failed. Errors on files nothing relies on yet (a
  `tmp_*` part, a merge output before it is named) fail just the call; the
  directory fsync after naming a merged part is fail-stop. A dictionary marks
  itself `broken` under its own lock before the engine fails, so a creator
  waiting for that lock cannot append behind a torn record. ENOSPC on the WAL,
  MANIFEST or a dictionary is fail-stop too. A failed flush leaves its
  memtable in `flushing` for a retry, and the next `mt_flush` must still flush
  the active memtable after it, or a large insert lands under older memtable
  rows. Parts already listed are never discarded after a failure
  (`db_add_parts` cannot fail). `test_failstop` and `jepsen --nemesis eio`.
- **Crash recovery** relies on each part's `wal_seq`: a WAL segment is replayed
  only into partitions without a part from that segment.
- **NFS followers.** MANIFEST is rewritten in place with a checksum, never
  renamed. Replaced parts are renamed to `retired_*` and deleted after
  `obsolete_part_lifetime_s`, never unlinked at once: a follower on another
  host may still have them mapped.
- **Granules** never span series; their count/nulls/sum/min/max, and m2 (floats) or the sum of
  squares (integers), must match the rows exactly, because aggregates use them instead of decoding.
- **Value types.** A series' type is fixed at creation and lives in the SERIES
  dictionary, not in the parts. Values travel as slots of `type_width(type)`
  bytes (4: float32, bool, state; 8: float64, int64); the memtable and the WAL
  hold 8-byte slots for every type. Null is a reserved value per type
  (`type_null`), so a null written later replaces an older value like any
  other write. Aggregates skip nulls; integer sums are exact (128 bits).
- **Lock order:** `flush_lock` > the WAL writer role > `publish_lock` >
  `manifest_lock` > `db->lock` > `buf_lock` > `dir_sync_lock` > `fail_lock`. The `crashfs` lock in tests is
  innermost.
- **Directory scans** use `fdopendir(dup(dirfd))` followed by `rewinddir`: the
  dup shares its offset with `dirfd`.
- The on-disk format is version 5 (`FORMAT_VERSION`): 160-byte granule entries
  with typed, time-weighted and spread statistics, a checksum over each part's header, index and footer,
  checksummed WAL, SERIES/STATES, CATALOG, VOLUMES and intent records. Before
  the first release, older formats need not stay readable (the user decided
  this on 2026-10-24): a layout change bumps `FORMAT_VERSION`, and a database of
  another version is refused at open. Value blocks name their codec, so new
  codecs need no version bump.
- **Damage is never silent.** A torn tail of an append-only file is cut; a bad
  record with a valid one after it, a read error or a broken part fails the open
  (`TSDB_ERR_CORRUPT` / `TSDB_ERR_IO`) with a log line. Never treat a read error
  as end of file, skip a part or drop a WAL record quietly. The one exception:
  granule payloads are hashed only with `verify_checksums` (default off), so
  without it a damaged granule decodes to wrong values; decoders must therefore
  be safe (no UB, no overread) on any bytes. With `fsync = 0` a power loss looks
  like damage and fails the open.
- **Protocol.** There has been no release yet, so the wire protocol may change
  without keeping old clients working (the user decided this on 2026-10-24): no
  legacy paths for earlier protocol versions in new designs. Every change must
  reach the C, Rust, Python and Java clients in the same commit, with
  `make test-clients` green. History so far: version 2 typed operations, 3
  packed inserts, 4 QUERY_BOUNDS (the server knows UTC only).

- **CPU levels.** `make dist` compiles for x86-64-v2 and compiles codec.c,
  codec_alp.c, codec_typed.c, codec_xor.c and values.c again for v3 and v4;
  `cpu.c` picks one set at startup. A function exported by those files needs
  an entry in `src/levels.h` (the linker complains otherwise); a new hot path
  elsewhere stays at v2 in the tarball unless it moves into one of them. Every
  level, and every simd.c kernel, must give byte-identical blocks and identical
  values and aggregates, since parts move between CPUs: no FMA contraction
  (`-ffp-contract=off`), no libm functions glibc dispatches by CPU (log2, exp,
  pow...: write the arithmetic out, as `log2_portable`), the f32 kernels sum in
  one order (32 lanes), NaN sums are canonical (`canon_nan`). Those files must
  not have constructors or other startup code: a v3/v4 copy's would run on
  every CPU before the dispatch (SIGILL on a CPU without AVX); use constant
  tables. `tests/test_levels.c` compares all of it and checks the dispatch;
  add new codecs and edge cases there. `tools/check-isa.sh` (run by `make dist`)
  checks the rest from the objects. The normal `make` (-march=native) has no levels.

## Conventions

- Match the surrounding code: 4-space indent, lines up to about 120
  characters, `snake_case`, errors as negative `TSDB_ERR_*` codes.
- Comments say why, not what; file headers describe the design of the file.
- New behaviour comes with a test, and with numbers if it is about speed or size
  (`build/codec-bench`, `build/tsdb-bench`, `build/insert-bench`, `build/tsdbc-bench`).
- Comparisons with ClickHouse run it in **Podman** (never Docker), bound to
  127.0.0.1, with data on the same filesystem as tsdb's.
- Never commit `build/` or benchmark data. Commits carry a DCO
  `Signed-off-by` as the last trailer (`git commit -s`) and no links to agent
  sessions.

## Benchmarks

The README keeps one comparison table; the full results live here. Rerun what
a change could move, on a quiet machine, and update the numbers.

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

## Benchmark results

These comparisons were measured before the changes of 2026-10-24 (compressed
client inserts, fail-stop, the epoll backend, the portable build). The user
decided to rerun the full comparison against the other databases, and the 100 B
run against ClickHouse, only when a release is near; `bench/dbcmp/README.md`
has the steps, Prometheus included. Internal benchmarks (flags, float32 vs
float64, io_uring vs epoll, clients) are rerun whenever the machine is quiet.

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

### 100 million rows, seven databases

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

**Embedded: tsdb and DuckDB.** DuckDB (1.5.6) is a library too, so it is
compared with tsdb embedded: both in one process, through their C APIs, the
same rows and queries, every answer checked against tsdb's (no differences).
DuckDB: one table `(series INTEGER, ts TIMESTAMP_MS, value FLOAT)`, the rows
sorted by series and time before loading (0.77 s, not counted) so its min/max
indexes prune, loaded through 4 appenders, then `CHECKPOINT`; 32 threads.
It has no primary key, so it lacks tsdb's last-write-wins (with one, every
append would pay a key check).

| | load | size | bytes/row |
|---|---:|---:|---:|
| **tsdb** (4 threads, unsorted input) | **0.99 s** (101 M rows/s), then `tsdb_optimize` 1.2 s | **119 MB** | **1.19** |
| DuckDB (4 appenders, sorted input) | 3.58 s (28 M rows/s), `CHECKPOINT` 0.03 s | 307 MB | 3.07 |

| | count/sum/min/max per series | buckets of 3600 samples | buckets of 60 samples |
|---|---:|---:|---:|
| **tsdb**, 1 calling thread | **1.8 ms** | **21.2 ms** | **40.5 ms** |
| DuckDB, one query on 32 threads | 59.5 ms | 141 ms | 181 ms |

| queries/s, 1 / 32 threads | raw rows, 3600 samples | 86400 samples into 288 buckets | 604800 samples into 168 buckets | a single row |
|---|---:|---:|---:|---:|
| **tsdb** | **36,410 / 490,572** | **14,261 / 83,229** | **7,633 / 15,470** | **59,891 / 637,288** |
| DuckDB (prepared statements) | 1,189 / 3,474 | 271 / 3,038 | 175 / 793 | 1,301 / 3,555 |

Some of the build and test work behind this README ran on the machine while
the DuckDB numbers were taken, which can only have slowed both sides; the
margins are 4 to 150 times.

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

### Query statistics

Every query reports what it read, as ClickHouse's client does
(`tsdb_last_query_stats`, or `LAST_STATS` over TCP). One series of the
100-billion-row benchmark, by day in UTC and then in Oslo (before the server
became UTC-only; the CLI now sends the Oslo days as bounds), where the granules
around local midnight have to be decoded:

```
116 buckets in set. Elapsed: 0.000366 sec. Processed 10.00 million rows, 120.00 MB (27.32 billion rows/s, 327.80 GB/s)
116 parts, 1273 granules (1273 from their statistics); 0 rows decoded from 0.00 B, 0 in memory
116 buckets in set. Elapsed: 0.001737 sec. Processed 10.00 million rows, 120.00 MB (5.76 billion rows/s, 69.08 GB/s)
116 parts, 1273 granules (1158 from their statistics); 608001 rows decoded from 692.52 KB, 0 in memory
```
