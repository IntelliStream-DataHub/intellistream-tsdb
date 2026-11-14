# AGENTS.md

Notes for coding agents working on tsdb: an embedded time series database in
C (MergeTree-style storage), the `tsdbd` server and its client.

## Commands

```sh
make                      # library, CLI, server, client, benchmarks (into build/)
make test                 # all unit, integration and server tests
make test IO=epoll        # server tests on epoll only (where io_uring is disabled)
make asan                 # the tests under AddressSanitizer + UBSan
make tsan                 # the tests under ThreadSanitizer, in build-tsan/ (about 80 minutes on 4 CPUs; CI runs the groups of
                          # make test as separate jobs: make tsan TEST_TARGET=test-meta, see TEST_GROUPS in the Makefile)
make jepsen               # fault injection: kill -9, simulated power loss, pauses, EIO; workloads kv and classes;
                          # misdirected I/O (verify_checksums on and off) and damage at rest (rot) on kv;
                          # kv also with login on (jepsen --auth) under kill and power, on each backend;
                          # JEPSEN_SECONDS=300 makes every run that long (the weekly CI run)
                          # (JEPSEN_WORKLOADS=classes: series switching partition classes under every nemesis)
                          # (kv also writes nulls and checks every QUERY_LATEST answer, and the final ones; tests/jepsen/mutant-latest.sh
                          # plants stale latest-table entries in tsdbd, which the check must fail: jepsen-selftest runs it)
make fuzz-frames FUZZ_SECONDS=600   # mutate tsdbd request frames under ASan+UBSan (build-fuzz-frames/); a crash-* input,
                          # once fixed, goes to tests/fuzz/frames/, which make test replays
make fuzz FUZZ_SECONDS=600 [FUZZ="codecx import"]   # the on-disk harnesses (tests/fuzz/fuzz_<name>.c: codec, codecx = codec
                          # with an output buffer of exactly n values, part, manifest, wal, dict, volumes, tz, import = the
                          # export stream, users = USERS, classes = CLASSES, marker = WAL marker payloads, chunk = a chunk file, ec = the decode path of objects and the codec) under ASan+UBSan in build-fuzz/; make test
                          # replays tests/fuzz/corpus/<name>/ (codecx uses codec's). A crash-* input, once fixed, goes there.
                          # At a CPU level: make fuzz FUZZ_ARCH=-march=x86-64-v4 (or -v3; its own dir build-fuzz-v4, and a changed level in a FUZZ_B you name
                          # empties it; make fuzz-v4-check replays part and codecx at v4 under ASan with use-after-return on; the
                          # decoders compile differently at each; the harnesses' 64-byte locals are _Alignas(64) because
                          # ASan's fake stack aligns to 32). make asan ASAN_ARCH=-march=x86-64-v4 likewise (engine code copies the
                          # footer the same way, so that run turns detect_stack_use_after_return off).
make PORTABLE=1           # without -march=native
make NO_URING=1           # tsdbd on epoll only, without liburing (after make clean)
make dist                 # portable tarball (x86-64-v2 + v3/v4 codecs) in build-dist/; checks glibc and ISA
make dist-test            # the tarball's own test suite, at every CPU level (TSDB_CPU=v2|v3|v4)
make dist-check-almalinux10   # that suite in an AlmaLinux 10 container (Podman)
make dist-check-sandybridge   # TSDB_CPU=v2, and under qemu-x86_64 -cpu Westmere/SandyBridge if installed
make meta-soak            # the metadata simulator (tests/test_meta.c) over META_SOAK_SEEDS=5000 seeds of 30 virtual s
make check-version        # VERSION, tsdb.h, tsdbd/tsdb --version and the client manifests agree (make test runs it)
make test-clients         # Rust, Python and Java clients (network and embedded) against build/tsdbd, and test-licenses
                          # (needs cargo, python3, mvn); make test-spdx: the licence line of every file (part of make test)
make hooks                # once per clone: the commit-msg hook (no session links, sign-off last)
make integration-tests    # tsdbd + CLI + C/Rust/Python/Java clients end to end, uring and epoll (~2 min, same tools);
                          # INTEG_ARGS="--only matrix,follower --io epoll"; logs in build/integ/logs/; mutate.py checks it has teeth;
                          # run.py --auth off,on (default both): matrix and cli also as users with grants, cross-tenant reads fail;
                          # make integration-selftest tests the harness itself (seconds)
```

Requirements: C11 (gnu11), pthreads, libzstd, liburing (server only; not with `NO_URING=1`).
Platforms: Linux is the one production platform; every gate, benchmark and durability claim is about Linux. macOS
(arm64, x86-64) is a developer platform only (docs/macos.md): it must build and pass `make test IO=epoll`, but never
at any cost to Linux. macOS code goes behind `#ifdef __APPLE__` or into the portability headers (`src/compat.h`,
`server/epoll_compat.h`, `tests/portable.h`), whose Linux side is the plain call inline, so the Linux objects stay
the same machine code (check with `objdump -d` against main). A new Linux-only call in shared code needs its shim
there; a test that cannot run on macOS prints that it is skipped. Answers must have the same bits there: arm64 has
no x87 `long double` (int_variance takes `src/ext80.h`). Nobody here has a Mac: what needs one goes into
docs/tasks/macos.md.
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
reconnect to the one in the new file. Ask after the connect whether the pid holds a socket that LISTENS on the port now
(`th_owns_port`: the listeners from netlink sock_diag, not `/proc/net/tcp`, which grows with every reconnect), never
whether it merely holds the socket: a failing tsdbd shuts its listener down 200 ms before it closes it, and another
run's server can get the port meanwhile (jepsen eio, 2026-11-09: one run's clients wrote into another's database).
`tests/test_ports.c` is the test of this; C harnesses include `tests/harness.h`,
`tests/integration/run.py` has `Server`, and the client test helpers under `client/*/tests` do the same in their language.
Every harness also passes `--exit-with-parent=PID`, PID being its own pid (the server stops cleanly once its parent is
gone, and refuses to start under any other parent): a test killed by a timeout otherwise leaves its tsdbd running
under init, holding an io_uring ring, until a later run cannot get one. C: `th_follow_me` before the fork; bash:
`me=$BASHPID` in the shell that runs `tsdbd ... &` (`$BASHPID` written on that line expands in the child, and `$$`
is wrong in a subshell). Start tsdbd as a direct child of the test (or through wrappers that exec), and keep the
flag out of supervised servers. `tests/test_ports.c` (`orphan`, `launcher_race`) checks it.

Login: tsdbd requires one by default and refuses to start while USERS has no superuser. A harness that tests something
other than login starts it with `--no-auth` (every data op runs unchecked; the user admin ops are refused). A login
test makes USERS first, with the CLI (`tsdb DIR user add NAME --superuser --password-file F`, never a password in argv)
or `tests/auth_harness.h`, and starts tsdbd without the flag; the client helpers take a flag or a root password for it.

Every bug fix comes with a regression test that fails without the fix (check
by building the old code). Query timings go on a quiet machine: no builds or
tests alongside them, and ClickHouse in its own container competes per
cgroup, so `nice` does not keep your work out of its way.

## Layout

- `include/tsdb.h`: public API. `include/tsdb_protocol.h`: wire protocol.
- `src/volumes.c`: the public `tsdb`, which routes each series to a volume.
  Everything else in `src/` is the engine for one directory (`eng`, declared
  in `src/internal.h`).
- `src/part.c`: part files, the writer, atomic commits (intent files). `src/gindex.c`: the packed granule
  index (`gix_encode`, `gix_parse`; the readers `gx_*` and `gx_load` are inline in `internal.h`).
- `src/insert.c`: sorting and writing batches. `src/memtable.c`: WAL and memtable. Nothing that holds `buf_lock` may touch a large block of fresh memory (the series array is a lazily touched
  mapping, `mt_s_grow`), and big buffers freed on the flush path go through `tsdb_free_big` (`src/compat.h`): a `munmap`
  or a memset of tens of MB stalls every append and lookup for ms (`tests/test_flush_tail.c`). `src/fno.c`: the `FNO` file (the
  durable bound of the WAL's log numbers). `src/marker.c`, `src/marker.h`: the marker payloads (one encoder and
  decoder; the `jr_intent`/`jr_commit` hook that will carry them is in `src/internal.h`, a no-op).
- `src/stats_tenant.c`, `src/stats_hot.c` (`src/stats_priv.h`): statistics steps 3-4: the tenant list, map and
  per-partition vectors, the HyperLogLogs of active series, the hot lists (reads, writes, bytes per row) and the
  sampled page-cache residency; `tests/test_tenants.c` is their oracle, `tests/test_cli_stats.sh` the CLI's test.
- `src/stats.c`: statistics (`docs/design-stats.md`, steps 1-2): the per-thread query counters
  (`tsdb_query_totals`), the event ring and sweeper thread behind `tsdb_volume_stats`' column totals, and the
  per-series walk of `tsdb_series_stats`. `tests/test_stats.c` is its oracle (`stats_ring_cap`, `stats_pause` and
  `sstats_budget` are its hooks).
- `src/merge.c`: merge selection and execution. `src/query.c`: snapshots, raw
  queries, aggregation, bucketing. `src/manifest.c`: MANIFEST, retired parts, followers.
- `src/sched.c`: the query scheduler, one per `tsdb` handle (`query_threads - 1` helpers): fork/join jobs, one
  group per outermost query or multi-series call capped at `request_threads`, the caller always works.
  `query_threads_for` (query.c) picks the threads one query step uses from the rows it decodes
  (`QUERY_ROWS_PER_THREAD`, `TWA_ROW_WEIGHT`, measured with `build/threads-bench`; `tests/test_threads.c` checks
  that answers are bit-identical at every count).
- `src/multi.c`: `tsdb_query_multi`, many series in one call from one cut of every volume (`cur_cuts`), delivered
  in index order; series ahead of the head run under a memory budget (`cur_budget`) or are deferred to their turn.
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
  `server/auth.c`: login (SCRAM-SHA-256) and the rights checks: `auth_ops`, one row per op of `TSDB_OPS`
  (who may send it, where its series are, the rights it needs), which `handle()` consults before its switch, and
  the per-user rights maps. `server/users.c`: the USERS file, shared with `tools/users-cli.c` (`tsdb DIR user ...`,
  which edits it beside a running tsdbd). `client/tsdb_crypto.c`: SHA-256, HMAC, PBKDF2 and base64 for SCRAM, used
  by tsdbd, the C client and the CLI. `docs/design-auth.md` is the design (decisions A1-A29).
  `client/`: C client library (does not link the engine);
  `client/rust`, `client/python` (PyO3 over the Rust crate; its shared glue is `client/python-common`), `client/java`:
  the other clients, network only and Apache-2.0. The embedded bindings, which contain the engine and are AGPL-3.0:
  `client/rust-embedded` (`tsdb_embedded::Db`; it uses the client crate's `#[doc(hidden)] __private` module and
  `client/rust-sys`), `client/python-embedded` (the wheel `intellistream-tsdb-embedded`, module `tsdb_embedded`) and
  `client/java-embedded`. A protocol change has to reach all four clients; `make test-clients` runs their tests.
- The cluster (`docs/design-cluster.md`; step 1 of its §14 so far: the metadata group only, nothing on the insert
  path). `src/raft/`: the Raft core copied from intellistream-objects (`UPSTREAM`; `tools/sync-raft.sh` copies it
  again, never edit the copies; `compat.[ch]` is tsdb's shim for what they include). `server/meta.c`: the metadata
  state machine (members, volumes, replicas, epochs, ISR, slots, users; entries of ops, snapshots, the §13.1
  invariants as `meta_check_step`). `server/cluster.c`: tsdbd's `--cluster` thread (own poll loop on the peer port:
  the Raft core, the watch stream, heartbeats, the admin ops of `tools/cluster-cli.c`, `tsdb cluster init|node
  add|status`). `tests/test_meta.c`: the state machine and a simulator of it under real Raft cores (`make meta-soak`
  for thousands of seeds; `make meta-mutants` plants bugs in meta.c and requires test_meta (its unit tests or the simulator's checker) to catch each);
  `tests/test_cluster.c`: tsdbd end to end (init, node add, leader killed, restart, witness, flags, retired, stalled
  disk, power loss under crashfs). The metadata state (NODE, IDENT, `raft/`, snapshot, `copy`, `retired`) lives in
  `--state-dir` on a local file system; log syncs and snapshot writes run on a disk worker thread (`dw_*`), never on
  the cluster thread; the hard-state save stays on the cluster thread (its slot sequence is shared with the durable
  floor, which the copied log does not lock). `tests/jepsen/crashfs.c` follows dup'ed descriptors (the Raft log syncs
  duplicates of its segments).
- `src/ec/`: the erasure codec (Reed-Solomon over GF(2^8); `gf256*.c`, `rs.[ch]`) copied from intellistream-objects
  (`UPSTREAM`; `tools/sync-ec.sh` copies it again, `--check` compares; never edit the copies; `compat.[ch]` and
  `compat/` are tsdb's own). Built without gf256's constructor (`-DISO_GF256_NO_BOOT`): `src/cpu.c` hands the level to
  `tsdb_ec_select`, which caps what `iso_cpu_has` says, so a `TSDB_CPU=v2` process never runs an AVX kernel
  (`tools/check-isa.sh` allows the wide kernels by name). In `libtsdb` on x86-64 only, never in a client library
  (`tools/check-ec-symbols.sh`). `tests/test_rs.c` (exhaustive erasure patterns, every level and kernel, mutants via
  `--mutant coef|kernel|guard`; it also checks that the ladder picks the widest kernel the level allows),
  `tests/test_levels.c`, `build/ec-bench`; `tools/check-exports.sh` (libtsdb.so exports the public headers' names only:
  `tsdb_ec_*` are `local:` in the map) and `tools/check-ec-baseline.sh` (no SSSE3 outside its kernel in a generic build).
  The embedded bindings' crates (`client/rust-sys` build.rs, `vendor.sh`) copy `src/*.c` only and build without `src/ec`
  and without `TSDB_EC`: an engine caller of `iso_rs_*` needs an `#ifdef TSDB_EC` or those crates stop linking.
- `src/chunk.h` (the API; internal, the engine does not call it yet: R7 does), `src/chunk.c` (the geometry of an object,
  the chunk file format, the encoder), `src/chunkstore.c` (chunk stores: META with the store id, `s<volume>/EPOCH`, the
  fences, the directory backend), `src/chunkread.c` (reads of any byte range with decode): design-cluster-data.md R6.
  The geometry (k', m, cell, length) is recorded in the object and in every chunk header and never taken from settings
  (E2); a chunk whose header, size, sums table, footer or identity is wrong, or whose cell fails its sum, is an erasure,
  never decoder input, and a short read is an error, never the end (`CHUNK_MUT_EOF`). The rebuilt cell of a decode is
  checked against the object record's sum when the caller has it. A temporary file is always a new inode (`tmp_*`,
  removed at open): one that a failed write or sync left may have had its pages dropped. A put or delete below a volume's
  EPOCH (two checksummed slots, FNO's scheme, fail-stop) is refused; a failed directory fsync after a name appeared or
  went, or any failed EPOCH write or sync, fails the store (`chunk_store_failed`: it acknowledges nothing more, reads
  go on). `tests/test_chunk.c` (exhaustive damage patterns for every geometry, power loss and EIO under crashfs, mutants
  via `--mutant nosums|nofence|eof|settings|nosync|epochnosync`), `tests/fuzz/fuzz_chunk.c`, `tests/fuzz/fuzz_ec.c`.
  The embedded bindings copy `src/*.c`: the files build without `TSDB_EC` (`chunk_encode` then says `TSDB_ERR_INVALID`).
- `connectors/pulsar/`: Apache Pulsar 5 IO sink (Java, a NAR) over the Java client; `mvn test` there needs
  build/tsdbd, `connectors/pulsar/test/integration.sh` runs Pulsar 5.0.0 in Podman with faults.
- `.claude/`: shared Claude Code setup. `settings.json` runs `hooks/guard-bash.sh` (refuses `pkill -f`,
  `killall`, `kill` with pid -1 or 0, `docker`) and `hooks/guard-push.sh` (refuses a push while unpushed
  commits carry a session link); `skills/tsdb-tracks/` is how changes are built, reviewed, gated and merged
  with subagents; `skills/subagent-economy/` keeps those subagents cheap (detached runs, a waiter, checkpoints;
  copy it to `~/.claude/skills/` to use it in other projects); `skills/jepsen-review/` is the adversarial
  review of durability, recovery and failover code in the Jepsen style (claims, faults, histories, a checker,
  evidence); `skills/job-watchdog/` (with `agents/job-watchdog.md` and `subagent-economy/scripts/watchdog.sh`) checks the detached jobs hourly for ones that died or hang, stops a hung one by its own process group, fixes harness causes and reruns it; `agents/iso-waiter.md` waits on detached runs. Personal settings go in the ignored
  `.claude/settings.local.json`. `.githooks/commit-msg`: the commit rules for everyone (`make hooks`).
- `.github/workflows/release.yml`: a pushed tag `vX.Y.Z` (or Actions -> release -> Run workflow with an existing tag)
  builds the tarball from exactly the tag (`.github/scripts/release-build.sh`: make dist, dist-test,
  dist-check-sandybridge, dist-check-almalinux10), attests its provenance (only in a run from the tag: the
  attestation names the run's ref) and attaches it and its `.sha256` to the GitHub release
  (`.github/scripts/release-publish.sh`: curl and GITHUB_TOKEN, never gh; a draft until the assets are up, and left
  a draft when the runner had no AVX-512; a published release's files are replaced only with `replace`). The
  text is `docs/releases/vX.Y.Z.md`, else the CHANGELOG section (`tools/release-notes.sh`). `make test` runs
  `tests/test_release_notes.sh` and `tests/test_release_publish.py` (the API calls against a local mock).
- `tools/dump.c`: `tsdb export` / `import` (public API only), the stream of `docs/export-format.md`, which every
  later version must keep reading (`tests/data/export-v1.tsdbdump`, `tests/test_export.c`).
- Configuration files (`docs/config.md`): `tools/props.[ch]` is the `.properties` parser, `tools/cfg.[ch]` the table
  layer, `server/tsdbd_keys.h` the keys of tsdbd (one row per option of `opts[]` in `server/tsdbd.c`; a new option
  needs a row, with a key or with the reason it has none) and `cli_keys` in `tools/tsdb-cli.c` those of the CLI. The
  file's settings are spliced into argv in front of the real command line, so the program's own option parser stays the
  only reader and the command line wins. The tables of `docs/config.md` between the `keys:` markers are what
  `tsdbd --config-keys` and `tsdb --config-keys` print (`tests/test_config.sh` compares them; paste the output after
  a change). `tests/test_props.c` and `tests/fuzz/fuzz_props.c` test the parser. The keys of `storage.` for the
  device classes are reserved, not built.
- `tools/`: CLI and benchmarks. `tests/`: tests; `tests/jepsen/`: the
  fault-injection harness and the `crashfs` LD_PRELOAD power-loss simulator.

## Invariants that are easy to break

- **Last write wins.** Among parts, a higher block number is newer; memtables
  are newer than every part. Block numbers are assigned, files renamed and parts
  listed in one step under `publish_lock` (`parts_publish`, `mt_flush`).
  Allocating a block number anywhere else lets a merge span a part that is
  still in flight and resurrect older values. Because memtable rows rank above
  parts, a large insert, replace or delete registers its keys (`lw_fence`: a
  timestamp range per bucket of series) with `lw_enter` in the step that decides
  whether to flush the memtable, and leaves (`lw_leave`) as soon as its parts are
  published or failed; `mem_insert` waits on the cv of a fence that a row of its
  batch is inside, then checks `eng_check` before it appends (the large write's
  own publish may have failed the engine; `test_failstop`). Otherwise a small
  write acknowledged before the large write was visible would win over it
  (jepsen check 5; `test_lww_realtime`). The wait is reported through
  `tsdb_set_write_wait_hook` (the waiter, and once the large write's thread), so
  tsdbd starts extra workers (`test_fence_workers` in test_server). Never let a
  small write pass a fence registered after it began waiting: that fence's flush
  has run, and the anomaly is back (no ticket order; a stream of overlapping
  large writes can delay a small write as long as it lasts).
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
  A drop never overlaps a merge of its partition: `eng_drop_before` waits on `merged_cv` while a part of a partition
  it drops is `merging` (the merge names its output before it takes `db->lock`, so a drop between the two would
  acknowledge with the output still to be listed), and `ttl_select_locked` skips a partition with a merging part.
  `merge_commit_locked`'s dropped branch is a backstop: it retires the output by rename and directory fsync.
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
  became visible, MANIFEST, SERIES, STATES) calls `eng_fail`: after it
  the engine acknowledges nothing, names no new file (a merge in flight drops
  its output), and `tsdb_failed` reports it;
  tsdbd exits with status 1, except for a volume under a `--health-domain` (ha.md 15; the root, USERS and every
  volume outside a domain still end the process): that volume is quiesced, answers `TSDB_ERR_UNAVAILABLE`, and
  `dom_recover` reopens it in the process once its domain is not DOWN (the section on **Storage outages** below).
  A retried fsync can return 0 after the kernel
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
- **Volumes** are added only by an operator (`tsdb <dir> volume add`, `tsdb_volume_add[_dir]`) or by embedded
  `tsdb_series_at` with `create_volumes` (tsdbd and the CLI set it to 0: a client naming an unknown path gets
  `TSDB_ERR_NOTFOUND`, and no directory or file is made). Every add checks the path (`path_valid`: absolute, no
  control bytes, no `.`/`..`; not in or around the root or another volume; not another database's directory) and
  appends to VOLUMES under an OFD write lock, which a writer's open also takes (not held while the directory is
  made; its parent is synced before the line is appended); a running writer picks new lines up when a request names
  a path it does not know (`volume_resolve`), having resolved the client's path before `create_lock`. A volume's
  `META` holds its number (`volume=N`, 0 in the root), written by the add before its VOLUMES line (tmp, rename,
  directory fsync) and checked at every open against the line the directory is on (`TSDB_ERR_CORRUPT` naming both):
  the number is inside ids that clients keep, so one directory reached by two paths must not pass.
- **Series ids** are `tsdb_sid`, a `uint64_t`: `(volume + 1) << 32 | local` (`TSDB_SID`, `TSDB_SID_VOLUME`,
  `TSDB_SID_LOCAL`, `TSDB_SID_NONE`), so every id is in [2^32, 2^48) and 0 is never one. `local` is the index in the
  volume's `SERIES` (below 2^31). Inside a volume only local ids exist: every file (WAL, parts, granule index,
  `CLASSES`, `RETENTION`, `SERIES`) and the engine's per-series arrays hold the 32-bit local id, and `src/volumes.c`
  turns an id into (volume, local) by shifting; there is no CATALOG and no table to route with. Ids are not dense:
  never loop `0 .. tsdb_series_count() - 1` or build an id from an index; list with `tsdb_series_foreach` (or
  `TSDB_SID(v, 0 .. tsdb_volume_series_count(db, v) - 1)`) and find ids by name. An id with a zero volume field, bits
  48-63 set or a volume or local number that does not exist is an unknown id (`TSDB_ERR_INVALID` for writes,
  `TSDB_ERR_NOTFOUND` for lookups). A 32-bit variable, `%u`, Java `int`, Rust `as u32` or Python `"I"` that holds an
  id is a bug that tests with ids above 2^32 find; row files of the benchmarks keep a u32 series NUMBER that indexes
  the bench's table of ids, never an id. Ids are never reused and mean nothing in another database (export and
  import name series, not ids). `tsdb_abi_version` (3 in 0.9.0 and 0.9.1, 7 in 0.10.0: 4 and 6 the statistics, 5 the latest table, 7 `tsdb_latest_stats`) changes when the C structs or signatures the bindings use change.
- **Log numbers (`fno`) never go back.** Every WAL record (format 9: a 32-byte header with `fno` and `commit_ms`) takes
  numbers (rows: one per row; a marker: `span`), assigned in `wal_buffer` under `buf_lock`. They must never be
  handed out twice across opens, whatever was removed in between (a segment after its flush, a part by retention,
  an empty merge output). The durable bound is the volume's `FNO` file (`src/fno.c`: two checksummed slots on
  separate 4 KiB pages, the older generation overwritten and `fdatasync`ed, fail-stop on any failure): before a WAL
  segment is unlinked (`wal_unlink_upto` after a flush, the replay at open, the empty segment at close) `FNO` holds
  at least the segment's end (`fno_require`), and a clean close stores the exact next number. At open `fno_next` =
  max(the slot with the higher generation, the end of the valid records of every segment, 1 + the largest part
  `fno_hi`); one bad slot is a torn store or damage to the newest slot (they look alike), so the other is used and the
  numbers skip ahead by `FNO_BELT` (2^40: a gap, never a repeat); both bad or no file in a format-9 volume is
  `TSDB_ERR_CORRUPT` (a volume that never held a record, 0.9.0's add leaves one, gets its `FNO` at the first open).
  Bounds at or above `FNO_BOUND_MAX` (2^62 minus two belts) fail the open, and the writer fails the engine rather than
  number past it. **MANIFEST
  cannot be the bound**: the writer never reads it at open and it is rewritten in place without a sync, so a power
  loss tears it; its `fno_next` line is information for followers, and followers never read `FNO`. A part's
  `fno_lo, fno_hi` cover the rows records it holds, never markers (0, 0 for a large insert, replace or delete), an
  extra lower bound only. A replay and a follower skip markers and count their numbers; a marker is never rows.
  `tests/test_fno.c` (power loss at every step; damage to the newest slot; numbers going back inside a segment or
  across two; the limit; `requires=`; mutations: numbers restarting from parts, the bound from MANIFEST, a segment
  unlinked before `FNO` holds its end, a marker replayed as rows). The jepsen rot nemesis leaves `FNO` alone: no
  workload there sees a number handed out twice.
- **Retained segments** (change feed, `docs/design-cdc.md` 2.2, 2.6, 2.13; `src/feed.h`). With a feed on (a `FEED` file and
  `requires=feed` in META; both are made by `tsdb DIR feed enable` or `tsdb_feed_enable`, root volume first, FEEDSLOTS between)
  a WAL segment the engine is done with is renamed `fwal_<seq>.log` (flush, open, close) instead of unlinked; with it off
  every byte is as before. The order that keeps this safe: at open a replayed segment gets the two-read check, the cut of its
  torn tail, an `fdatasync`, then `fno_require`, then the rename (a `fwal_` is whole or does not exist); a failed unlink or
  rename at open stops there and holds `wal_removed` behind it; `wal_close` never unlinks a segment that was written to (a
  marker is not a row); a new segment's seq counts `fwal_` names (`wal_open`), or a rename could overwrite one (it is
  `RENAME_NOREPLACE` where the file system allows). The cleaner (one thread per engine, `src/feedclean.c`) makes its floor
  durable in `FEEDSLOTS` (scratch half, `fdatasync`, picked by `gen`) before any unlink, never removes the newest segment, and
  takes `feed_lock` only for the index; its `eng_sync_dir` runs after that lock is released. An open brings each volume to the
  root's state (`feed_open_reconcile`); disable removes FEED first. The renewal of `feed_id` (torn FNO at open, `feed new-id`)
  writes the new FEED before FNO is raised. Tests: `tests/test_feedstore.c`, `tests/test_cli_feed.sh`, `jepsen --feed`
  with `tests/jepsen/feedcheck.c`.
- **Crash recovery** relies on each part's `wal_seq`: a WAL segment is replayed
  only into partitions without a part from that segment.
- **NFS followers.** MANIFEST is rewritten in place with a checksum, never
  renamed. Replaced parts are renamed to `retired_*` and deleted after
  `obsolete_part_lifetime_s`, never unlinked at once: a follower on another
  host may still have them mapped.
- **The granule index** is read in place from the part's mapping: readers use `gx_count`, `gx_ts`,
  `gx_series`, `gx_load(p, i, GW_*, &g)` (only the groups they need) or `granule_read_at`, never a
  `struct granule` array on disk (that 160-byte struct is the decoded form; there is no `p->g`). A new
  statistic needs a `GF_` field, its `field_value` in gindex.c and a `GW_` group in `gx_load`, and cases in
  `tests/test_gindex.c`. The encoding is integer and compiled once: every CPU decodes the same bits.
- **Granules** never span series, and in class 1-3 partitions never a base partition (day) edge either
  (`GRANULE_CUT`, `pw_add`; a merge copies a granule verbatim only inside one day): day and coarser buckets then
  answer from statistics as in day partitions, bit for bit. Nor does a granule span a UTC hour edge where its series
  is dense (hour cuts, `HOUR_CUT_MIN` = 3000, compile-time; `hour_lim`, `hk_cut`): at least 3000 rows of the series,
  as given to that writer, lie in the hour before the edge. Hours and the days of whole-hour zones then answer from
  statistics too. The rule counts the rows of the hour, never the rate of the last few (a burst is as fast as 1 Hz
  while it lasts: a bursty series below 3000 rows an hour keeps its 8192-row granules) nor the class (a short
  retention keeps slow series in class 0). A writer counts only the rows it was given, so a part that begins less
  than 3000 rows before an edge (a flush, the first edge of an insert) does not cut there; the merges that see the
  hour do. A merge copies a granule verbatim only where one writer given its rows would cut them the same way
  (`pw_copy_fits`): no cut inside it, and a short one must end where the writer ends it (an hour cut, a day cut, or
  the series' last row in the merge) with nothing of the series before that end left (in another source or later in
  its own run); the counts come from the granule's count, window and longest gap between rows (`tw_gap`), and from
  its decoded timestamps where those do not decide. Pending rows of the series go out before a copy only where a day
  or hour cut lies between (`pw_hcut_before`). These rules shape the layout only: what makes a copy correct is
  `before_rest` and no tombstone or retention filter. Readers take any granule boundaries, so hour cuts are not part
  of the format; a database written before keeps its granules until merges rewrite them. `test_classes hourcut`
  checks it (the hook `hour_cut_min`, 0: off, gives a database without them). Answers do not depend on the layout,
  but for the last bits of float sums (float32 values too are summed in doubles per granule: exact only while their
  range and count fit 53 bits), variances and TWA areas, which combine per granule.
  Their count/nulls/sum/min/max, and m2 (floats) or the sum of squares (integers), must match the rows exactly,
  because aggregates use them instead of decoding.
- **Value types.** A series' type is fixed at creation and lives in the SERIES
  dictionary, not in the parts. Values travel as slots of `type_width(type)`
  bytes (4: float32, bool, state; 8: float64, int64); the memtable and the WAL
  hold 8-byte slots for every type. Null is a reserved value per type
  (`type_null`), so a null written later replaces an older value like any
  other write. Aggregates skip nulls; integer sums are exact (128 bits).
- **Rounding mode.** The float codecs keep an integer only when it rebuilds the value bit for bit, so their blocks and
  decoded values depend on the CPU's rounding mode, which is per thread (a host application may set it; the engine
  never does). The four block entry points that do such arithmetic (`block_encode_f32`, `block_encode_vals`,
  `block_decode_f32`, `block_decode_vals`) run with round-to-nearest and put the caller's mode back (`src/fpmode.h`,
  `fp_nearest` / `fp_restore`, around a noinline body: GCC does not model MXCSR). A new entry that reaches the codecs
  another way needs the same; `tests/test_levels.c` (`rounding_modes`) encodes and decodes under every mode. Not
  covered: granule statistics (sums, TWA areas) and the index's derived areas are computed in the calling thread's
  mode, so a part written under another mode has other bits there (data blocks are identical).
- **Partition classes** (`src/classes.c`). A series' rows go to partitions of 1, 7, 28 or 364 base partitions by
  its class; the key is `4 x first base partition + class` (`partition_classes = 0`: the base number). A switch
  (series, T, class) never rewrites rows: T is past every row of the series in parts and memtables and every row
  of a flushed memtable whose WAL segment may still replay (`db->wtail`, per series, kept until
  `wal_removed_synced` passes its segment), so a replay maps each WAL row to the key its flush used: a deleted row
  merged away cannot come back under another key. A segment counts as gone only when it was unlinked before a
  directory fsync (`eng_sync_dir` moves `wal_removed_synced` to the `wal_removed` it read before the fsync); a
  failed unlink holds `wal_removed` back until a later flush removes it. A T more than 366 days ahead of now is not
  written (the series keeps its class until the far row is gone).
  **One (series, ts) is never in two keys**: queries rank rows by part `max_block`, which is right only within a
  key (a merge in one key can span another key's tombstone and let an older copy win). So **every placement reads
  one frozen history**: a large insert, a replace and a flush hold `cls_rw` for reading (`cls_freeze`/`cls_thaw`)
  from their first class lookup until their parts are listed, never while waiting for a merge or a flush, and a
  switch takes it for writing; never re-read the live history in the middle of a placement. Backstops: the part
  writer refuses rows out of (series, ts) order (`TSDB_ERR_CORRUPT`, nothing written), and the open checks every
  part against CLASSES (`cls_check_parts`; a granule that spans a switch is decoded and judged by its rows). A
  replace classifies a fresh series like a large insert. `CLASSES` is append-only (24-byte checksummed records),
  synced (and its directory, when new) before anything relies on a record, fail-stop on error, torn tail cut,
  loaded before the WAL replay.
  `tsdb_part_info.partition` stays the base partition number (where the partition starts); `cls`, `start_ms`,
  `end_ms` say the rest. Tests that count parts per base partition or name `p<N>_` files set
  `partition_classes = 0`.
- **Login and rights** (`server/auth.c`, `docs/design-auth.md`). Every op needs a row in `auth_ops` (who may send
  it, the rights, where its series are: a name, an id, an id per row, QUERY_MULTI's ids); a new op in `TSDB_OPS`
  without one is refused and fails `tests/test_auth_ops.c`. Ids that only an op's case finds (a row per series of a
  packed insert, QUERY_MULTI) are checked in that case. Grants name series (`all`, `prefix:`, `name:`, `glob:`), so
  an id is checked through its name (`tsdb_series_name`) and the answer cached in the user's rights map: names never
  change and ids are never reused, which keeps the cache right until the user's grants change (then the map goes
  with the user's state, marked stale; a request already running finishes under its old rights). An id that resolves
  to no name is DENIED and not cached. **Denied and missing look alike**: a name outside the user's grants, and any
  id the user holds no right on, is `TSDB_ERR_DENIED` whether the series exists or not; a missing id is DENIED for
  every user without the needed right on `all`. A missing name inside the user's grants is `TSDB_ERR_NOTFOUND` (the
  clients and the sink rely on it: look up, NOTFOUND, then create), as is anything missing for a user with the
  right on `all`; QUERY_MULTI
  answers DENIED per series, an insert frame with one denied row is refused whole. Before login only HELLO,
  AUTH_START and AUTH_CONTINUE pass; every failed login is `TSDB_ERR_AUTH` with no body, an unknown user included.
  Passwords never travel (clients send a verifier) and never appear in argv or logs. USERS keeps the rules of the
  other append-only files (checksummed records, OFD write lock, torn tail cut, damage fails the open), and a failed
  write or fdatasync of it is fail-stop (tsdbd exits with status 1 before acknowledging): an acknowledged revoke
  must not come back after a power loss. `--no-auth` checks nothing and refuses the user admin ops.
- **Storage outages** (ha.md 15, N1; `src/health.c`, `server/domains.h`, `src/revalidate.c`). A volume on NFS that is
  down must refuse fast while the other volumes go on, and come back without losing or resurrecting anything.
  `tsdb_health_*` (probe threads with `O_DIRECT`, no engine call) gives each domain UP, DEGRADED, DOWN, RECOVERING.
  Every `route()`-family lookup in `volumes.c` takes a reference on the engine (`refs`, the `EREF` cleanup attribute)
  and refuses a quiesced volume with `TSDB_ERR_UNAVAILABLE`; `tsdb_volume_quiesce`, `_idle` (waits for refs to reach
  0, with no deadline when asked: a call stuck in NFS cannot be interrupted), `_reopen`, `_resume`. A reopen halts the
  old engine (`eng_halt`: joins the flusher and merges, no flush), checks what is on disk against what it remembers
  (`tsdb_volume_report`: WAL tail by checksum through a fresh `O_DIRECT` read, then a re-log of the memtable into a new
  segment when it does not match; SERIES and STATES repaired from memory; parts gone and not merged into others are
  `lost_parts`, as is a RETENTION or CLASSES shorter than the engine's, `lost_log_bytes`; a merge the storage took back
  counts as lost too; the volume is left quiesced, DIVERGED, until `tsdb_volume_resume`, and tsdbd keeps it so until a
  restart), opens a new engine on a dup of
  the old lock fd (the flock is never released) and swaps it under the wrlock; the old engine is retired, never
  freed with its name strings (`kept_names`). Never retry the failed fsync of the old fds: only a reopen that replays
  from disk is trusted. The re-log writes rows up to `wal_durable` in sync mode (rows past it were never acknowledged)
  and every memtable row with `wal_sync_ms > 0` (acknowledged by contract). A request parked on a stuck volume is
  abandoned through `tsdb_set_abandon_flag` (a thread-local pointer set by the request's thread): `mem_insert` checks it
  before the WAL append (after the fence wait), `parts_publish` and `delete.c` check it under `publish_lock` before
  `parts_commit` (never inside `parts_commit`: a flush carries other writers' acknowledged rows); a helper thread that
  one day publishes must be given the flag explicitly. `tests/jepsen/outagefs.c` (`liboutagefs.so`, stacks with
  crashfs) is the fault shim; `test_outage` and jepsen's `outage` nemesis use it. mmap page faults are not gated by
  the shim. Not built yet: mountinfo discovery, parking and BUSY, a per-volume bounded queue, SIGTERM with a stuck call,
  metrics, the volumes cache, parallel open.
- **Statistics are never on the append path** (`src/stats.c`). Nothing is added to a write, the WAL, the memtable, the
  part writer or a merge: the two functions that change a part list (`db_parts_insert_locked`,
  `db_parts_remove_locked`) push one event into a bounded ring under the `db->lock` they hold, and a nice-19
  sweeper thread (throttled to `stats_sweep_mb_s`, a hard cap) walks each added part's granule index once. A full
  ring drops events and the sweeper reconciles its records with the part list (never before the open has finished:
  `load_parts` changes the list without `db->lock`; an overflow stays set until `stats_open_done`), so the totals are
  the sum of the records and cannot drift. Statistics never write a file, never call `eng_fail` and keep answering after the engine
  failed; a failed allocation means "not counted" (`stats_parts_counted < stats_parts_listed`), never a failed
  query or write. Query counters are one block per (thread, handle), written without a read-modify-write
  (`qs_end`, about 10 ns); a thread that outlives `tsdb_close` holds a reference on the registry, not on the handle.
  A new number that is kept per part goes into `part_walk` and must make the four byte columns add up to the part's
  file (`test_stats`: `bytes == sum of tsdb_foreach_part`). **Tenant totals are the sum of the partition vectors**
  (`src/stats_tenant.c`): a total changes only together with the vector it comes from, under `es->mu`, so it cannot
  drift or go negative; a merge or a delete marks its partition stale and the recount replaces the vector whole; a
  list change moves only the series whose tenant changed, and never costs more than counting the partition again
  (`move_pv` compares the two). **The tenant side never fails the engine**: an allocation that fails there calls
  `tenants_fail` (no `abort`), the tenant numbers of that volume stop and read PARTIAL until a reopen, and every entry
  point returns at once while `tdead` is set. `tsdb_set_tenants` and a volume's catch-up with its listing
  (`volume_open`, `volume_reopen`) take `set_mu` (below `create_lock` and `layout_lock`, above the `tsdb` rwlock), so a
  list is never freed while another call pushes it and no volume misses a list set while it was being listed. A
  tenant read is PARTIAL while the engine's applied list (`es->applied`) is not the handle's. Per-tenant numbers are
  never put on the metrics port (no login there; design-stats.md section 10).
- **The latest table** (`src/ltab.c`, `docs/design-latest.md`). Per series it holds the newest non-null row in the
  parts, or a bound on it (unknown); it is read and written only under `latest_rw` (`lt->rw`), and every write section
  bumps the volume's `lgen`. Writers: publishers (in the write section that lists their parts, after `parts_commit`,
  folding each part's newest non-null row and null range; a flush of 50,000 series or more goes through the window:
  its unlocked stores only ever store the flush's own row, the rest is noted and applied in the closing section while
  `pending` makes every other writer wait), retention lengthening, `tsdb_drop_before` (the floor), a merge that lists
  across a retention change (`ret_gen`), and the warmer, the only filler, which stores a parts-only search only if
  `lgen` is unchanged since its cut and no window is open. Lookups never write it. Whoever makes an entry unknown gives
  it a correct bound and hints its id to the warmer. A new listing point must fold there, nulls included; anything else
  that removes live rows (other than rows hidden by a tombstone of a later block, rows a later write overwrote or rows
  already expired) must bump `lgen` in a write section and make the entries it may affect unknown. The table must
  always give the slow path's answer (`tests/test_latest.c` compares both after every random step). Entry words are
  loaded and stored with relaxed atomics (lookups race the window's stores on purpose); test hooks go through atomics.
  `latest_rw` is held across no I/O but `tsdb_drop_before`'s renames of the partitions it retires (`db_retire_locked`,
  in its write section; `db->lock` was held across them already), and never across a lookup's reads of granules. Followers have no table.
- **Lock order:** `flush_lock` > `cls_rw` > `class_lock` > the WAL writer role > `publish_lock` >
  `manifest_lock` > `db->lock` > `buf_lock` > `dir_sync_lock` > `fail_lock`; `cls_mu` (the class history map)
  is a leaf, and so is the statistics' `es->mu` (`stats.c`: never held across I/O or an allocation of a part; the
  sweeper takes `db->lock` only with it released). The latest table (design-latest.md section 10): `publish_lock` > `latest_rw` > `manifest_lock` > `db->lock` >
  `buf_lock` (a publisher's write section); `ret_lock` > `latest_rw` > `db->lock` (a limit change); `tsdb_drop_before` and a merge
  that lists across a limit change take `latest_rw` (write) > `db->lock` (a merge lets go of `db->lock` to take it); lookups and
  the warmer take `latest_rw` (read) > `db->lock`, a lookup then `buf_lock` in sub-holds. The window's `pmu`/`pcv` (a writer
  waits there with no other lock, `lt_end` takes `pmu` under `latest_rw`) and the warmer's hint mutex are leaves. A small write waiting for a large write's publish (the fence's cv, on `buf_lock`) holds no other lock and has
  not yet appended anything, so nothing a registered large write waits for (a flush, the WAL writer role,
  `publish_lock`, `cls_rw`) waits for it. The write wait hook (`tsdb_set_write_wait_hook`) is a leaf: its +1 and
  the waiter's -1 run with no engine lock, the large write's -1 (`lw_leave`) with `cls_rw` held for reading; tsdbd's
  takes only `work_lock`. Series creation (`volumes.c`): a reserved name (`name_reserve`; its `name_mu` is a leaf) >
  `create_lock` (only around `volume_resolve` and volume adds) or, in a follower, `layout_lock` (only around the
  opens of new volumes in `volume_at`; a lookup or a route to a volume already open never takes it, and a lookup
  reads SERIES of the open volumes before it opens new ones) > the `tsdb` rwlock (never held across I/O: a new
  volume is opened first and listed under it, by `volume_open`, one at a time under `create_lock` or
  `layout_lock`; should two ever open one, the first to list it wins). `create_lock` and `layout_lock` are held
  across a new volume's whole open, so they come before every engine lock. An engine's `refresh_lock` > a
  dictionary's `append_lock` (held across its write and fdatasync, or a follower's
  read of it in `series_reload`) > its `lock` (never held across I/O), and `broken` is set under `lock` before
  `append_lock` is released (also when a durable record could not be added in memory). The `crashfs` lock in tests is innermost. The scheduler's mutex (`sched.c`) and a multi call's (`multi.c`) are leaves: never held while a job's
  `fn`, a query or the callback runs, nor while an engine lock is taken. A job's `fn` waits only for the items of its
  own nested jobs (a fork/join tree), never for a lock another job's worker may hold while waiting for helpers.
  tsdbd's login (`server/auth.c`): `users_mu` (the USERS state; held across USERS's I/O and its OFD lock, so on NFS
  as long as a server is away) > `auth_mu` (the published snapshot of the users and the registry of logged-in
  users; a leaf, never held across I/O). Logins and requests take only `auth_mu`, never `users_mu`, and no engine
  lock is taken under either; a user's rights index is built before it is shared and read without a lock.
- **Directory scans** use `fdopendir(dup(dirfd))` followed by `rewinddir`: the
  dup shares its offset with `dirfd`.
- **Versions.** The release version is the file `VERSION` (0.11.0); `tools/check-version.sh` compares it with
  `include/tsdb.h`, the binaries and the manifests of the clients and the connector. A release is cut by
  `docs/release.md`; what 0.9.x and 1.0 promise is `docs/compatibility.md`; the notes are `CHANGELOG.md`.
  `make dist` names the tarball `tsdb-VERSION-x86_64-linux.tar.gz`.
- The on-disk format is version 9 (`FORMAT_VERSION`; 9: the WAL record has a 32-byte header with `fno` and
  `commit_ms` and may be a marker, the part header is 48 bytes with `fno_lo`, `fno_hi` and `written_ms`, every volume
  has an `FNO` file, META may say `requires=` (an open refuses a feature it does not know, naming it) and
  `parts_v8=1`; a format-8 database is carried over by `tsdb DIR upgrade`, below; 6: a 7-bit codec kind in block headers; 7: partition keys with
  a class, CLASSES, zstd frames without magic and content size, the packed granule index and a 64-byte footer
  with `index_len`; 8: no CATALOG, the volume number in each volume's META, 64-bit series ids): granule entries
  with typed, time-weighted and spread statistics, bit-packed per part (about 32 bytes each), a checksum over each
  part's header, index and footer, checksummed WAL, SERIES/STATES, VOLUMES and intent records. Until
  1.0 (the format freezes there; decided on 2026-10-25) older formats need not stay readable: a
  layout change bumps `FORMAT_VERSION` and goes in the changelog with a way to carry data over, and a database
  of another version is refused at open with a message that names both versions. Value blocks name their codec, so new
  codecs need no version bump.
- **`tsdb DIR upgrade`** (`tsdb_upgrade`, `up_*` at the end of `src/volumes.c`) carries a format-8 database over in place.
  It refuses, before writing anything, on a `wal_*` segment, an intent, a `tmp_*` part, a held writer lock or a
  damaged MANIFEST (not on `retired_*`: those are normal after a clean close); holds every directory's `LOCK` and the
  VOLUMES OFD lock for the whole run; per volume makes `FNO` (synced with its directory), rewrites MANIFEST as 9 (tmp,
  fsync, rename) and writes META (version 9, `parts_v8=1`) last; volumes first, the root last; safe to run again.
  Parts keep their 24-byte headers: `part_open_as` reads one only where META says `parts_v8=1` and only when header and
  footer both say 8 (a v8 part anywhere else is damage), `rewrite_alone` (`src/merge.c`) lets an idle partition's merge
  or `tsdb_optimize` rewrite one alone, the open logs how many are left, and `v8_parts_account` (`src/db.c`) takes the
  line out of META when none is. Followers on other hosts cannot be detected: they are stopped first
  (docs/compatibility.md). **The v8-part reader is removed before 1.0** (docs/tasks/pre-1.0-v8-parts.md).
  `tests/test_upgrade.c` unpacks a database 0.9.0 wrote (`tests/data/v8-fixture.tar`, made by
  `tests/data/make-v8-fixture.sh` with the tag's binary, with its 0.9.0 export), upgrades it, compares every series
  with the import of that export, merges, reopens, loses power at every step (crashfs) and plants "a v8 header hashed
  as 48 bytes". `TSDB_V090_CLI=/path/to/0.9.0/tsdb` adds a check that 0.9.0 itself refuses the result.
- **Damage is never silent.** A torn tail of an append-only file is cut; a bad
  record with a valid one after it, a read error or a broken part fails the open
  (`TSDB_ERR_CORRUPT` / `TSDB_ERR_IO`) with a log line. Never treat a read error
  as end of file, skip a part or drop a WAL record quietly. The one exception:
  granule payloads are hashed only with `verify_checksums` (default off), so
  without it a damaged granule decodes to wrong values; decoders must therefore
  be safe (no UB, no overread) on any bytes. With `fsync = 0` a power loss looks
  like damage and fails the open.
- **Protocol.** Until 1.0 (0.9.0 is the first release, 2026-11-05) the wire protocol may change
  without keeping old clients working (decided on 2026-10-24; the changelog of the release that moves it names
  the new version and the floor): no legacy paths for earlier protocol versions in new designs. Every change must
  reach the C, Rust, Python and Java clients in the same commit, with
  `make test-clients` green. History so far: version 2 typed operations, 3
  packed inserts, 4 QUERY_BOUNDS (the server knows UTC only), 5 LTTB, 6 TWA,
  7 range delete, replace and retention, 8 the spread in bucket replies, 9
  order and limit for LTTB (and early-stopping limits on bucketed queries), 10
  QUERY_MULTI (many series in one request, one snapshot) and CANCEL, 11 u64 series
  ids (`tsdb_sid`; QUERY_AGG and QUERY_GROUP removed; HELLO floor 11: tsdbd refuses a
  client below 11 and a client a server below 11, `TSDB_ERR_INVALID`), 12 login and
  access control (HELLO's reply says whether login is required and lists the mechanisms;
  AUTH_START and AUTH_CONTINUE run SCRAM-SHA-256; WHOAMI, the user admin ops,
  `TSDB_ERR_DENIED` and `TSDB_ERR_AUTH`; the op list is the X-macro `TSDB_OPS`; HELLO floor 12).
  13 the HA bodies (include/tsdb_wire.h, docs/ha.md 15.4): `TSDB_ERR_MOVED` -10, `UNAVAILABLE` -11, `PARTIAL` -12 and
  `DROPPED` -13 with their bodies, the ops TOPOLOGY (48) and STATUS (49); floor still 12. The four clients retry
  UNAVAILABLE after `retry_after` and follow MOVED within a time budget (`tsdbc_set_retry`, 30 s by default), and
  connect to a list of nodes; tsdbd sends only UNAVAILABLE (MOVED, PARTIAL and DROPPED wait for the cluster).
  The planned protocols moved up: QUERY_LATEST 14, the change feed 15, statistics 16.
  All four clients speak 13 and no call is gated on a version below the floor;
  the embedded bindings (Rust `tsdb_embedded::Db`, Python `tsdb_embedded.Db`, Java `EmbeddedDb`) have the
  same operations, and their tests compare them with tsdbd's answers.

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
- **Licences** (`LICENSING.md`; from 0.11.0): the server, the engine, the tools, the tests, the benchmarks and the
  embedded bindings are `AGPL-3.0-only`; the client libraries and what only they use (`client/tsdb_{client,crypto,
  calendar,pack}.{c,h}`, `include/tsdb.h`, `tsdb_protocol.h`, `tsdb_wire.h`, `tsdb_calendar.h`, `examples/client.c`,
  `client/rust`, `client/python`, `client/python-common`, `client/java`, `connectors/pulsar`) are `Apache-2.0`.
  Every source file starts with an `SPDX-License-Identifier:` line (the second line after a `#!`) naming the licence of its
  directory; `tools/check-spdx.sh` (in `make test`, `tests/test_check_spdx.sh` tests it) fails on a file without one, on
  the wrong one, on a wrong `license` field in a manifest, and when an Apache-2.0 file includes, imports or depends on an
  AGPL-3.0-only one (the other way round is fine). A new directory defaults to AGPL; one that must be Apache-2.0 needs a
  line in `expected()` of the script and in `LICENSING.md`. A new package carries `LICENSE` and `NOTICE` (an Apache
  package `LICENSES/NOTICE.apache`, an AGPL one the root `NOTICE`; and the Apache
  text when it is AGPL and packs Apache files): `tools/check-package-licenses.sh`. Outside contributions need the grant
  of `CONTRIBUTING.md`. Never move code from an AGPL directory into an Apache one (the owner decides that).
- Never commit `build/` or benchmark data. Commits carry a DCO
  `Signed-off-by` as the last trailer (`git commit -s`) and no links to agent
  sessions.

## Benchmarks

The README keeps one comparison table; the full results live here. Rerun what
a change could move, on a quiet machine, and update the numbers.

The generated data (`tools/datasets.h`) has seven sensor kinds, each with its own sample interval and clock jitter
(uniform, plus or minus the value, so timestamps stay strictly increasing: the jitter is below half the interval,
which `tests/test_types.c` checks):

| kind | interval | jitter |
|---|---:|---:|
| `hdd_temp` | 60 s | none: regular on purpose, the one constant-interval series (DoubleDelta, about 0.03 bits per row) |
| `cpu_temp` | 1 s | 20 ms |
| `tank_bar`, `tank_psi` | 10 s | 50 ms |
| `vibration` | 1 ms | none: at millisecond resolution a 1 ms clock has no room for jitter without duplicate or reordered timestamps |
| `flow_rate`, `flow_total` | 1 s | 5 ms |
| `random` | 1 s | 5 ms |

The real hydraulic rig data (`bench/hydraulic`) has no timestamps; it gets regular generated ones. Before the jitter
change of 2026-10-29 (a deliberate change) `tank_bar`, `tank_psi` and `random` were regular as well, so the size and
timing numbers below that include timestamps (bytes per row, the mixed data sets) predate it; the value-only codec
tables do not depend on it. The next full run measures the jittered data.

**One command for all of tsdb's own benchmarks** (for a fresh clone on another machine): `tools/bench-tsdb.sh`
(`--quick` to see that it works, `--standard` the default, `--full`; `--reps N`, `--dir SCRATCH`, `--out DIR`,
`--portable` for the `make dist` build, `--cpu-compare` to add `bench/cpu-compare`). It checks the prerequisites,
builds an optimized copy from scratch in `build-bench/` (never a debug or sanitizer build; `CFLAGS`/`ARCH` from the
environment are dropped), records the machine (CPU, memory, disk, governor, git commit, compiler and flags) and
warns when it is not quiet. It then runs codec-bench, type-bench and decode-bench, `tools/fold-bench.sh`,
tsdb-bench, tsdbd (started with `--port 0 --port-file`) with tsdbc-bench, insert-bench and, in `--full`, scale-bench
(the size picked from the free disk and memory), each `--reps` times (5), and writes `summary.md` and
`summary.json` (mean, sd and lowest-highest run of every number) and the raw logs to `bench-results/HOST-DATE/`
(`tools/bench-summary.py` parses them). Rough time: `--quick` 3 to 5 minutes, `--standard` about 40 minutes (plus the build),
`--full` about 1.5 hours. The dbcmp comparisons and the 100 B run are separate (see below).

| tool | measures |
|---|---|
| `tools/bench-tsdb.sh` | all of the below except dbcmp and hydraulic, repeated, with a summary; see above |
| `tools/auth-bench.sh` | what login and grants cost: main before, main without auth and the auth build (`--no-auth`, a superuser, 8 prefix, 5,000 name and 64 glob grants), each built from `git archive`; queries, QUERY_MULTI, inserts and logins on 1 and 32 connections, repeated and interleaved (`--quick`, `--reps N`, `--lock FILE`); `tools/auth-bench-client.c` has the insert and login workloads |
| `build/codec-bench` | float32 value codecs per sensor type: bits per value, speed; `--file F` for raw float32 values from a file |
| `build/type-bench` | the float64, int64, bool and state codecs; `--file F float32\|float64` for raw values from a file |
| `build/decode-bench` | bit unpacking per width (loops, the AVX2 kernel, PDEP) and the int_bp and float64 decimal codecs, for comparing CPUs; [`bench/cpu-compare`](bench/cpu-compare) has the Hyper-Threading and threshold sweeps |
| [`bench/hydraulic`](bench/hydraulic) | the codecs on real sensor data (UCI hydraulic test rig, 17 sensors): `get.sh`, `convert.py`, `run.sh` |
| `build/tsdb-bench` | load, compression and queries on 100 M rows; `--type float64` or `int64` stores another value type; `--export` writes the rows for ClickHouse |
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
client inserts, fail-stop, the epoll backend, the portable build). The plan
is to rerun the full comparison against the other databases, and the 100 B
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
| tank pressure in psi (converted, unrounded) | 23.77 | 19.57 | 19.62 | 27.24 | **17.49** |
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
