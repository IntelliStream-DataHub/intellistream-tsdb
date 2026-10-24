# AGENTS.md

Notes for coding agents working on tsdb: an embedded time series database in
C (MergeTree-style storage), the `tsdbd` server and its client.

## Commands

```sh
make                      # library, CLI, server, client, benchmarks (into build/)
make test                 # all unit, integration and server tests
make asan                 # the tests under AddressSanitizer + UBSan
make jepsen               # fault injection: kill -9, simulated power loss, pauses
make PORTABLE=1           # without -march=native
make test-clients         # Rust, Python and Java clients against build/tsdbd (needs cargo, python3, mvn)
```

Requirements: C11 (gnu11), pthreads, libzstd, liburing (server only).
Tests take a scratch directory as their first argument
(`./build/test_tsdb /tmp/x`); default `/tmp`.

Run the tests that cover what you changed, and the sanitizers for anything
concurrent or touching memory layout. A change to how data reaches the disk
(writes, fsync, renames, recovery, merges, the WAL) also needs `make jepsen`.
ThreadSanitizer is not wired into make; build it the way `make asan` does,
with `-fsanitize=thread`.

## Layout

- `include/tsdb.h`: public API. `include/tsdb_protocol.h`: wire protocol.
- `src/volumes.c`: the public `tsdb`, which routes each series to a volume.
  Everything else in `src/` is the engine for one directory (`eng`, declared
  in `src/internal.h`).
- `src/part.c`: part files, the writer, atomic commits (intent files).
- `src/insert.c`: sorting and writing batches. `src/memtable.c`: WAL and memtable.
- `src/merge.c`: merge selection and execution. `src/query.c`: snapshots, raw
  queries, aggregation, bucketing. `src/manifest.c`: MANIFEST, retired parts, followers.
- `src/codec*.c`: codecs; `src/simd.c`: dispatched aggregation kernels;
  `src/tz.c`: time zones (TZif and POSIX rules).
- `server/tsdbd.c`: io_uring server. `client/`: C client library (does not link the engine);
  `client/rust`, `client/python` (PyO3 over the Rust crate), `client/java`: the other clients.
  A protocol change has to reach all four; `make test-clients` runs their tests.
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
- **Inserts are all or nothing.** All parts of one insert are written pending,
  then committed through an intent file (`parts_commit`). Never publish them one
  by one.
- **Durability.** A new file is durable after fsync of the file and of its
  directory; a rename after fsync of the directory. In sync mode
  (`wal_sync_ms = 0`) a memtable row is visible to queries only once the WAL is
  synced up to it (`mt_collect` with `visible`).
- **Crash recovery** relies on each part's `wal_seq`: a WAL segment is replayed
  only into partitions without a part from that segment.
- **NFS followers.** MANIFEST is rewritten in place with a checksum, never
  renamed. Replaced parts are renamed to `retired_*` and deleted after
  `obsolete_part_lifetime_s`, never unlinked at once: a follower on another
  host may still have them mapped.
- **Granules** never span series; their count/sum/min/max must match the rows
  exactly, because aggregates use them instead of decoding.
- **Lock order:** `flush_lock` > the WAL writer role > `publish_lock` >
  `manifest_lock` > `db->lock` > `buf_lock`. The `crashfs` lock in tests is innermost.
- **Directory scans** use `fdopendir(dup(dirfd))` followed by `rewinddir`: the
  dup shares its offset with `dirfd`.
- The on-disk format is version 1 (`FORMAT_VERSION`). Value blocks name their
  codec, so new codecs can be added without a version bump; changing an
  existing layout needs one.

## Conventions

- Match the surrounding code: 4-space indent, lines up to about 120
  characters, `snake_case`, errors as negative `TSDB_ERR_*` codes.
- Comments say why, not what; file headers describe the design of the file.
- Every bug fix comes with a regression test that fails without the fix.
- New behaviour comes with a test, and with numbers if it is about speed or size
  (`build/codec-bench`, `build/tsdb-bench`, `build/insert-bench`, `build/tsdbc-bench`).
- Comparisons with ClickHouse run it in **Podman** (never Docker), bound to
  127.0.0.1, with data on the same filesystem as tsdb's.
- Never commit `build/` or benchmark data. Commits carry a DCO
  `Signed-off-by` as the last trailer (`git commit -s`) and no links to agent
  sessions.
