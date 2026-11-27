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
make tsan-par             # make tsan with the groups of TEST_GROUPS and the rest of make test side by side (tools/tsan-par.sh:
                          # one build, then one make per group, logs in $TSAN_PAR_LOGS; at most TSAN_PAR_JOBS (6) makes at once; not yet run whole, its memory peak
                          # unmeasured: the weekly gate stays `make tsan` with its groups as jobs until a full run is recorded)
                          # Planted bugs (every `--mutant` loop of the Makefile, tests/*_mutants.sh) run in parallel through
                          # tools/mutants.sh, each in a scratch dir of its own: MUTANT_JOBS (default half the CPUs, shared by all
                          # helpers of the user through a pool in $XDG_RUNTIME_DIR; 1 = one after the other, and a set MUTANT_JOBS
                          # is no pool), MUTANT_URING_JOBS (default 2: the rings of tsdbd count against the 8 MB memlock),
                          # MUTANT_TIMEOUT (a hang is a survivor), MUTANT_SLOT_DIR + MUTANT_SLOTS (one pool for several helpers);
                          # it names every survivor and keeps its log; a new loop: `$(MUT) --name X --list "a b" -- ./$(B)/X --mutant @M@ @D@`
make jepsen               # fault injection: kill -9, simulated power loss, pauses, EIO; workloads kv and classes;
                          # misdirected I/O (verify_checksums on and off) and damage at rest (rot) on kv;
                          # kv also with login on (jepsen --auth) under kill and power, on each backend;
                          # JEPSEN_SECONDS=300 makes every run that long (the weekly CI run)
                          # (JEPSEN_WORKLOADS=classes: series switching partition classes under every nemesis)
                          # (JEPSEN_TIER, "3 6" by default: a directory tier of 2+1 and of 4+2 stores, `jepsen --tier N`, under the
                          # eager policy: kill, power, eio and settle-steps (TSDB_SETTLE_CRASH_AT: a power cut at each named step of a
                          # settle) on kv and classes, both backends, JEPSEN_TIER_SEEDS 1 2; a run in which nothing settled fails;
                          # JEPSEN_TIER= turns them off; tests/jepsen/mutant-settle.sh plants a settle that ranks its .ec below the
                          # rows it replaced and a swap that renames early: jepsen-selftest runs it, both must be caught)
                          # (kv also writes nulls and checks every QUERY_LATEST answer, and the final ones; tests/jepsen/mutant-latest.sh
                          # plants stale latest-table entries in tsdbd, which the check must fail: jepsen-selftest runs it)
                          # jepsen exits 0 valid, 1 VIOLATION (a wrong answer of tsdb: never repeated), 2 harness failure, 3 INCONCLUSIVE
                          # (a liveness or coverage check of the run itself failed: the nemesis too slow for its floor, a server dead
                          # too long, a gap, a step that idled, the kv or classes workload that wrote or switched too little, the healthy outage volume
                          # slowed; says nothing about tsdb). The final state and the whole history are judged first, whatever the liveness
                          # checks found: a lost, resurrected or wrong row is exit 1 on its own attempt, never an INCONCLUSIVE rerun
                          # (tests/jepsen/mutant-loss.sh plants a loss in tsdbd beside a lazy nemesis; jepsen-selftest runs it).
                          # A tsdbd that ends by itself (a crash, an exit of its own) or stops answering is a VIOLATION on that attempt under
                          # every nemesis and under none: every request has a deadline (60 s; JEPSEN_REQ_DEADLINE_S), no answer is
                          # "server stopped answering", joins and stops are bounded and the harness stops its servers by pid at every end.
                          # How soon writes resume after an outage heal is liveness (INCONCLUSIVE). A tsdbd that does not start is a harness
                          # error (exit 2) unless its log says the data is damaged (a violation). tests/jepsen/mutant-crash.sh plants a crash
                          # (JV_CRASH) and a hang (JV_HANG) in a copy of tsdbd and requires exit 1 at the first attempt under each nemesis.
                          # tests/jepsen/rerun.sh runs every gate's jepsen: an INCONCLUSIVE run is rerun with the next seed, twice at
                          # most, printed each time; three in a row fail the gate with all three messages; the summary line at the
                          # end of make jepsen counts the reruns. jepsen-selftest requires each planted nemesis to end in that failure
                          # with its check named (tests/jepsen/expect.sh), the honest ones to end valid
make jepsen-cluster       # 5 tsdbd (a proxy of the harness in front of every peer port) under kill, partition, pause, midcommit
                          # (crash points: server/crashpt.h, TSDB_CRASH_AT=name[:n] -> _exit(99)) and power (crashfs), workloads meta
                          # and kv-local (a cluster volume of 1 replica per node), and users (every tsdbd requires a login, an admin thread
                          # changes users, passwords and grants, every node is probed with logins and reads: an acknowledged change must hold
                          # on every member; tests/jepsen/cluster_users.h, what it does not cover: docs/tasks/users-stale-answers.md; --auth runs the other
                          # workloads with a login too), and kv-via (kv's volume of 3 replicas with every client on a random member, so
                          # the operations are forwarded; check 6 read-your-writes on the connection of an acknowledged write, and the
                          # nemesis fwdstall: a non-leader holds its forwards for 2 s, check 7 "old frame": no write held that long may
                          # be acknowledged; JEPSEN_VIA_NEMESES, default fwdstall, runs it as an extra loop; the selftest plants fwdoldframe and
                          # TSDB_REPLICA_READS=1 with no mutant; kv-via costs one more run of JEPSEN_SECONDS per nemesis and backend
                          # on top of kv, which already roams and forwards: JEPSEN_CLUSTER_WORKLOADS leaves it out);
                          # JEPSEN_SECONDS, JEPSEN_NEMESES, JEPSEN_SEED,
                          # JEPSEN_NODES, both IO backends; tests/jepsen/jepsen_cluster.c; make jepsen-cluster-selftest plants what it must catch (not part of jepsen-cluster)
                          # (kv/classes: the nemeses aim at kvr's leader too, the clients roam and follow MOVED to it; JEPSEN_TERMINAL=name
                          # picks the run's terminal crash point, failover points included; JEPSEN_SETUP_MOVE=1 transfers kvr right after its first series: the kv setup follows a moved leader,
                          # and a setup that cannot create its series is exit 2; JEPSEN_TRACE=1 stamps the nemesis's
                          # actions and kvr's leader changes in ms and details the first stale reads)
                          # R4's nemeses (header of jepsen_cluster.c; all five are built: one whose `built` flag is 0 is refused with a line,
                          # exit 2, `jepsen_cluster --built-nemeses` lists what runs, JEPSEN_R4_TRY=1 runs it anyway): kill2 (two servers at
                          # once; the first round and half the others take two of kvr's replicas), slow (outagefs `slow MS` on one
                          # node's data root, `--outagefs build/liboutagefs.so`: fd calls only, not mmap faults), longdown (a replica
                          # down past repl_retain_s, `--retain-s S`, 3 s: it must take a snapshot, seen as another inode of its v<N>;
                          # a run in which fewer than half the rounds did fails; the selftest plants `snapdroppart`, a snapshot that leaves a part out), rebuild (v<N> removed, the server must refuse it, `replica reset`), churn
                          # (`volume transfer` and `volume move --log`); the end checks compare the members of the final record, want no
                          # learner and no v<N>.snap, v<N>.old, v<N>.removed-* or snap_* on any node. make jepsen-cluster runs them
                          # after the others on kv and classes (JEPSEN_R4_WORKLOADS, JEPSEN_R4_NEMESES, `none` leaves them out): IO x 2 x
                          # the built ones more runs of JEPSEN_SECONDS: 20 runs, about 100 min at 300 s)
                          # `jepsen-cluster-ec` (`JEPSEN_EC=4+2`, `JEPSEN_EC_NODES=7`; `JEPSEN_EC=` skips): runs `--ec K+M`
                          # (`tests/jepsen/cluster_ec.h`, `nodes >= 2` use `--io epoll` per D15), two chunk stores per node,
                          # nemeses `kill partition partial pause diskloss rot midcommit power` (when `TSDB_EC` is built),
                          # live per-replica verification across all ISR members via `VOLUME_TRANSFER`, `kvr.fill` batch
                          # check, and settled `.ec` part count; selftest checks `ec-diverged`, `ec-member-differs`,
                          # `ec-notransfer`, `ec-nosettle`, and server mutants `ycold`, `ecabovey`, `cutnogate`;
                          # `jepsen_cluster --ec 16+4 --nodes 22` tests 22-node geometry (2 io_uring + 20 epoll)
make fuzz-frames FUZZ_SECONDS=600   # mutate tsdbd request frames under ASan+UBSan (build-fuzz-frames/); a crash-* input,
                          # once fixed, goes to tests/fuzz/frames/, which make test replays
make fuzz FUZZ_SECONDS=600 [FUZZ="codecx import"]   # the on-disk harnesses (tests/fuzz/fuzz_<name>.c: codec, codecx = codec
                          # with an output buffer of exactly n values, part, manifest, wal, dict, volumes, tz, import = the
                          # export stream, users = USERS, classes = CLASSES, marker = WAL marker payloads, chunk = a chunk file, ec = the decode path of objects and the codec, ecpart = a settled part's .ec file and its object record) under ASan+UBSan in build-fuzz/; make test
                          # replays tests/fuzz/corpus/<name>/ (codecx uses codec's). A crash-* input, once fixed, goes there.
                          # At a CPU level: make fuzz FUZZ_ARCH=-march=x86-64-v4 (or -v3; its own dir build-fuzz-v4, and a changed level in a FUZZ_B you name
                          # empties it; make fuzz-v4-check replays part and codecx at v4 under ASan with use-after-return on; the
                          # decoders compile differently at each; the harnesses' 64-byte locals are _Alignas(64) because
                          # ASan's fake stack aligns to 32). make asan ASAN_ARCH=-march=x86-64-v4 likewise (engine code copies the
                          # footer the same way, so that run turns detect_stack_use_after_return off).
make PORTABLE=1           # without -march=native
make NO_URING=1           # tsdbd on epoll only, without liburing (after make clean)
make NO_EC=1              # an x86-64 engine without the erasure codec (what the embedded bindings and other machines build; after
                          # make clean, or with its own B=build-noec): every R7 stub and test must build and pass both ways; the tier
                          # tests print that they are skipped, and a configured tier is refused. Only NO_EC=1 turns it off
make dist                 # portable tarball (x86-64-v2 + v3/v4 codecs) in build-dist/; checks glibc and ISA
make dist-test            # the tarball's own test suite, at every CPU level (TSDB_CPU=v2|v3|v4)
make dist-check-almalinux10   # that suite in an AlmaLinux 10 container (Podman)
make dist-check-sandybridge   # TSDB_CPU=v2, and under qemu-x86_64 -cpu Westmere/SandyBridge if installed
make meta-soak            # the metadata simulator (tests/test_meta.c) over META_SOAK_SEEDS=5000 seeds of 30 virtual s
make check-version        # VERSION, tsdb.h, tsdbd/tsdb --version and the client manifests agree (make test runs it)
make test-clients         # Rust, Python and Java clients (network and embedded) against build/tsdbd, and test-licenses
                          # (needs cargo, python3, mvn); make test-spdx: the licence line of every file (part of make test)
make test-rdma            # real-verbs RDMA tests (test_peerlink --rdma and test_cluster rdma; docs/design-rdma.md §8):
                          # uses an active IB HCA or Soft-RoCE rxe* device (tests/rdma_dev.h, tools/setup-rxe.sh), with
                          # pool_mb 1-2 and TSDB_PLRDMA_TEST=rnr_retry=0; skips cleanly on NO_RDMA=1 or without a device
                          # unless TSDB_RDMA_REQUIRED=1 is set (which fails instead of skipping; TSDB_RDMA_DEV=DEV[:PORT] pins one)
make hooks                # once per clone: the commit-msg hook (no session links, sign-off last)
make integration-tests    # tsdbd + CLI + C/Rust/Python/Java clients end to end, uring and epoll (~2 min, same tools);
                          # INTEG_ARGS="--only matrix,follower --io epoll"; logs in build/integ/logs/;
                          # make integration-mutants (mutate.py: 23 planted bugs, ~40 min, CI weekly) checks it has teeth;
                          # run.py --auth off,on (default both): matrix and cli also as users with grants, cross-tenant reads fail;
                          # make integration-selftest tests the harness itself (seconds)
```

Requirements: C11 (gnu11), pthreads, libzstd, liburing (server only; not with `NO_URING=1`), rdma-core's headers
(`libibverbs-dev` / `rdma-core-devel`, or `RDMA_CFLAGS=-I<dir>`) for tsdbd's verbs transport (detected; without them
`NO_RDMA=1` with one line, and `make dist` refuses unless `NO_RDMA=1` is given: the release carries it). No RDMA library
is linked: libibverbs is loaded with dlopen at run time, so one tsdbd runs with and without rdma-core
(`tools/check-rdma-syms.sh`, in `make test`). macOS never builds it.
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

Run the tests that cover what you changed (integration tests of the behaviour, end to end where it shows), and
those test binaries under the sanitizers for anything concurrent or touching memory layout. A change to how data
reaches the disk (writes, fsync, renames, recovery, merges, the WAL) also needs a power-loss or crash-point case for
that path. The full gates (`make asan`, `make tsan`, `make jepsen`, the cluster gate, the mutant suites) run once a
week on main (.claude/skills/tsdb-tracks/SKILL.md), not per change.
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
  durable bound of the WAL's log numbers). `src/replfile.c`: a replica directory's REPL file (rid, open generation and its
  nonce (format 2), fence, `led`, epoch history, log start, commit floor; FNO's two-slot scheme; a load with one bad slot is "unsure": the
  open generation skips a belt and the caller takes fence, `led` and log start from the metadata group, never from the
  surviving slot (the `unsure` flag is stored by the next save and cleared only by one with `led` = `fence`: a restart does not make the older
  slot's values trusted); `tests/test_replfile.c`, `fuzz_repl`). tsdbd's volume manager keeps its replicas' open generation and
  nonce in it: `server/cvolfs.c` (make `v<N>`: META, REPL, FNO in `v<N>.tmp`, renamed; load, check and raise REPL at every
  open; R13 `r13-cvolfs` E48/E51/D14: `cvolfs_prepare_disks`/`cvolfs_pick_disk` place a new `v<N>` on the UP fast disk
  with fewest replicas weighted by 1h WAL bytes/s below `young_move_pct` (`nvme` before `ssd`; `log_on_hdd` allows `hdd`
  only when no fast disk is UP), scan every UP fast disk (and `hdd` with `log_on_hdd`) plus probe non-UP disks so a copy
  on a DOWN disk is never duplicated, refuse duplicate `v<N>` across two disks naming both paths, keep R4 siblings and
  snapshot staging/space checks on `v<N>`'s disk, and report each volume's disk `store_id` in `HBS_VOLUMES`/`STATUS`
  alongside local `cvol_vol_disk`/`cvol_vols_on_disk`; a missing, unreadable or CORRUPT REPL, another identity and a failed
  save refuse the volume with a line naming the path; `tests/test_cvolfs.c`: power loss at every step under crashfs on
  primary and second disk, mutants `nosync nodirsync nononce trustunsure tornstale samerid removenodirsync cvolfirstdisk
  cvoldupok`, also settable as `TSDB_CVOL_MUTANT` in tsdbd for `test_cluster volumes` and `test_cluster twodisks`;
  `make cvolfs-mutants`), `server/cvol.c` the manager thread above it.
  Replica set changes (R4, r4-setchg; design-cluster.md 8.3): `server/rset.c` applies a new `set_version` in the driver
  on the volume thread (`rset_change` keeps a newer record, `rset_apply` drains the core, `repl_reconfig` with `old_of`
  by node AND rid, so a reset member is new; the driver's cursors move with their members; a newer record that came
  during the drain restarts the apply; messages of another set version are dropped and counted; it defers during a
  snapshot), `server/cvolset.c` the manager's side on disk (a removed replica closed, renamed `.removed-<rid>`
  and deleted in the background; at prepare, a v<N> of another rid that the group never saw opened is set aside as
  `stale-<rid>`, or `lost-<rid>` after a reset this server saw; a missing v<N> with a recorded open is refused naming
  `tsdb cluster replica reset`), `server/cvolset_mgr.c` (the push's RESET and UNNAMED, the start sweep),
  `server/cset.c` (REPLICA_RESET on the Raft leader, refused for the leader and ISR members; the `setop_file` test hook
  for ADD/REMOVE). Tests: `test_cluster addreplica setnext addswitch removereplica removeindex removedown setstale
  setchurn resetlost`, `test_cvolfs` setpower; `make rset-mutants` (tests/rset_mutants.sh). `TSDB_REPLD_TEST=keepall` keeps every
  leader segment, for the tests that fill a learner from the log (a fresh learner otherwise gets a snapshot, below).
  `src/marker.c`, `src/marker.h`: the marker payloads (one encoder and
  decoder; `MK_COLD` = 11, `MK_GC` = 12, `MK_DIGEST` = 13, `MK_UNSETTLE` = 14 = `MK_TYPE_LAST`, `MK_EC_LATER_LO..HI` = 15..15 reserved
  for R10 (`MK_DROP_OBJ`) and admitted nowhere; `mk_type_admitted` gates both writer and replay on `requires=ec|tier`; `test_marker`
  mutants `mkcoldbounds mkadmitall ecpposloose mkwriteany`; `jr_intent`/`jr_commit` and `jr_cold`/
  `jr_markers_append`/`jr_markers_wait` in `src/replica.c` carry them).
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
  by tsdbd, the C client and the CLI. `docs/design-auth.md` is the design (decisions A1-A33).
  `client/`: C client library (does not link the engine);
  `client/rust`, `client/python` (PyO3 over the Rust crate; its shared glue is `client/python-common`), `client/java`:
  the other clients, network only and Apache-2.0. The embedded bindings, which contain the engine and are AGPL-3.0:
  `client/rust-embedded` (`tsdb_embedded::Db`; it uses the client crate's `#[doc(hidden)] __private` module and
  `client/rust-sys`), `client/python-embedded` (the wheel `intellistream-tsdb-embedded`, module `tsdb_embedded`) and
  `client/java-embedded`. A protocol change has to reach all four clients; `make test-clients` runs their tests.
- The cluster (`docs/design-cluster.md`, `docs/design-cluster-data.md`): metadata group, replicated and erasure-coded
  cluster volumes, failover, moves, transfers, and snapshot rebuilds. `src/raft/`: the Raft core copied from intellistream-objects (`UPSTREAM`; `tools/sync-raft.sh` copies it
  again, never edit the copies; `compat.[ch]` is tsdb's shim for what they include). `server/meta.c`: the metadata
  state machine (members, volumes, replicas, epochs, ISR, slots, users; entries of ops, snapshots, the §13.1
  invariants as `meta_check_step`; R4's replica set: `set_version`, raised by exactly REPLICA_ADD, REPLICA_REMOVE and
  REPLICA_RESET, one move per volume (an ADD with `replaces`; the REMOVE of the replaced member puts the learner into
  its slot, so rep[0] stays the preferred leader's), a planned grant's `target` and `min_end`;
  R8: a volume's redundancy, an EC volume's stripe group (picked by the proposer), `stripe_version`, `gc_suspended`,
  ops STRIPE_SET, GC_SUSPEND, GC_RESUME (`test_cluster ecadd`; an EC add is admin op VOLUME_ADD_EC, so an older server
  refuses it); `cluster_copy_caught_up_at(copy)` (D9: the copy has applied the leader's commit index heard last and that
  heartbeat's lease still holds, read on the caller's clock; `test_cluster copycaught`, mutants `copynodeadline
  copynofloor` in `make copy-mutants`); R13 (`r13-meta`): per-member registered disks (`META_DISKS_MAX = 64`:
  `store_id`, `nvme`/`ssd`/`hdd`, `log`/`chunk`, size, sync_us, `UP`/`DOWN`/`OUT`/`DESTROYED`), declared `role`
  (`both`/`compute`/`storage`) and computed `capability`, ops `STORE_ADD`/`STORE_CLASS`/`STORE_STATE`/`MEMBER_ROLE`,
  R12/R13/R9 settings (`META_SETTINGS_WORDS = 39`: `scrub_days`, `scrub_mb_s`, `scrub_out_count`, `log_on_hdd`,
  `young_warn_pct`, `young_move_pct`, `disk_stall_s`, `balance`), `cluster_member_disks()`; R14 (`r14-settings`, D25):
  `settings_version` in `meta_state` (1 at INIT, +1 per `META_OP_SETTINGS`), CAS + D25 init-only whitelist and range
  validation at both proposer (`CLUSTER_OP_SETTINGS` = 21) and `meta_apply`, live settings watch on consumers,
  `tsdb cluster settings show|set`, `test_cluster settings`, `make settings-mutants` (`settingsany`, `settingsnocas`);
  snapshot version 12 and entry magic "TME9": a state directory or log of another version is refused naming both, never upgraded);
  R9 operator ops (`server/cnode.c`: Raft leader handlers for `node hold|out|drain|replace|destroyed` and
  `volume move --pos`, answered after commit, plus `cnode_duty` DRAINING->OUT, replace completion/abandon, and live
  OUT leader handoff; `test_cluster nodehold nodeout nodedrain nodereplace nodedestroyed posmove`; `make ops-mutants`
  for `drainleaves` and `replaceclear`).
  `server/cluster.c`: tsdbd's `--cluster` thread (own poll loop on the peer port:
  the Raft core, the watch stream, heartbeats, the admin ops of `tools/cluster-cli.c`, `tsdb cluster init|node
  add|status`; users: USER_PUT/DEL answered after commit with the members still behind, the uid assigned at apply,
  puts conditional on the user's version, `cluster_users_caught_up`, `tools/cluster-user-cli.c` for `tsdb cluster
  user ...`, `test_cluster users`). `src/repl.[ch]`: the replication core (sans-IO); a set change reaches a running core as
  `repl_reconfig` (drain its actions first, `REPL_E_PENDING`; hand it only records of its set version), every message
  carries the set version (`repl_set_version`, others dropped and counted) and the ACK the replay start
  (`repl_replay_start`, D17); `repl_leave` and `repl_meta_promote_to` for moves (design-cluster.md 8.3, R4);
  `tests/test_repl.c`, `tests/sim_repl.c` (`make sim-repl-mutants`, `make sim-repl-soak`); replwire `RW_VERSION` 3, cluster
  version (`PL_CL_VERSION`) 11 (11: columnar WAL records in APPEND; 10 brought this): every driver message
  (replication messages, FETCH, a bulk file's meta) ends its
  fields with the driver check, an XXH3-64 that the decoder verifies before any field is read out (`RW_E_CHECK`:
  dropped and counted, the resend repairs it), at every check level, `none` included, never over record bytes
  (rd-drvcheck, review F3: a flipped ACK `durable` inside [durable, written]; `tests/test_replwire.c`, `test_cluster
  drvcheck` with TSDB_REPLD_TEST `flipraw=ack|head|desc`, mutants `checknocmp checkshort checklate`, `build/rw-bench`);
  the data links' frame check is an option (`--peer-check none|header|full`, key
  `cluster.peer.check`, `none` by default on every transport; `header` XXH3-64 over the length, type, zstd header and
  the first 256 payload bytes, `full` XXH3-128 over the frame; `server/plcheck.h`, xxHash vendored unchanged in
  `server/xxhash/`; seeded with the frame's number and a salt per connection from the handshake, whose HMAC-SHA256
  still proves the secret; the level is a check type negotiated in HELLO, a link runs at the stronger end's;
  `plink_check`, STATUS `links.KIND.*`, metrics `tsdb_peer_links`; docs/design-rdma.md §6). Under `none` the receivers
  are the guard, in every mode: bounds and consistency checks (repld `msg_refused`: ACK beyond the log, a record
  unlike its descriptor, metadata-only types from a peer, an epoch above the member's highest, descriptor epochs out
  of order; driver frames on BULK; DONE/RESUME status; fwd's checked frame id and op of the link's kind; chunksrv's
  checked put meta, requests and answers, answers matched by op; open under `none`: an ACK's `durable` flipped inside
  [durable, written], design-rdma.md §6), each with a
  test and a mutant, and every receive path safe on any bytes (fuzz-replframe per level). `test_peerlink --check
  LEVEL` (mutants `notag noseq nosalt checktype samesalt notranscript dialanytype notype halfcmp seq8` at header and
  full; `donestatus`, `noidcheck`); HELLO and
  HELLO_ACK end with a capability byte, `PL_CAP_RDMA` sent 0 until rd-upgrade, unknown bits ignored). A link connection's
  bytes go through `struct pl_transport` (`server/pltransport.h`: read, writev, poll_fds with `*ready`, poll_ready, close
  last after on_link(down); the handshake, connect and accept stay on TCP); the TCP transport is in peerlink.c,
  `tests/pl_fake.h` an in-memory pair (level and edge modes) that `peerlink_test_attach` runs a link on: `test_peerlink
  fake|fakeready|fakeend` (mutants `noready closefirst wnoshort nhandall`).
  `server/plrdma.[ch]` (rd-verbs; docs/design-rdma.md §4, D7-D9): the verbs transport under that seam, for rd-upgrade to
  switch a link connection to: one registered pool per tsdbd and device (one slot size; memlock checked, naming
  RLIMIT_MEMLOCK), one RC queue pair per link connection with its own receive ring (data and control slots), SEND/RECV
  only, credits (a SEND never finds no posted receive; 4 to 255 control slots, fewer storm), WRITTEN = posted, completions on the completion channel's fd in
  the link's poll, the drain (error state, a marker, then destroy) before any slot goes back; `plr_ep_encode/decode` are
  the numbers the upgrade exchanges over the TCP link. Every verb goes through `struct plr_ops`: `server/plrdma_dl.c`
  fills it by dlopen of libibverbs (`NO_RDMA=1` leaves it out), `tests/plr_fake.h` is a fake fabric that checks
  ownership and flushes a failed queue pair asynchronously, as an HCA does. `tests/test_plrdma.c` (mutants `writtencqe
  noctl ctlstorm pausectl nodrain unsigearly nocredit splitoff room crown norecvmark norearm anchorbyte`;
  `--verbs-listen`/`--verbs-dial` run two hosts on real InfiniBand with `rnr_retry` 0, the numbers of design-rdma.md §3.6).
  `server/repld.c`: the replication driver, one thread and one `repl` core per replicated volume
  (log records on the peer data links, `server/replwire.[ch]` their encoding, fuzzed by `fuzz-replframe`; part files
  over the bulk link; commit point; promotion of the record's leader); a leader's WAL record stays below
  `jr_rec_max_rows` so that it fits one peer frame; its index of record starts (`idx_*`, rec_find) is sorted and is
  rebuilt whole by `core_build` (a deposed writer's engine writes what it admitted until demote closes it: an entry
  kept from that window stopped the volume at the cut of its uncommitted tail; `test_cluster deposetail`). `TSDB_REPLD_MUTANT` and `TSDB_REPLD_TEST` are its test hooks, `TSDB_REPLD_TRACE=1` logs the core's and the cursors' state every 2 s
  (`make repld-mutants`); `test_cluster replicate|replindet|replfail` its end-to-end tests. `server/fwd.[ch]`: the forwarding links' transport (S2-2), a peerlink of its own on every member with link kinds
  FWD (3) and FWDQ (4) to every other member (`server/fwd_cluster.c` keeps them to the metadata copy's members), per frame
  NEVER_WRITTEN (no byte reached the socket: `plink_sendv` tracked) or WRITTEN, replies only from the connection the frame
  went on, CREDIT for streamed query replies; `test_peerlink kinds|toobig|written|wbcut|fwdstall|fwd|fwdgrow|fwdredial|fwdslow|fwdstream`
  (mutants `queuedasunsent kindshared lateclose nocredit inboxwait inboxdrop chunkfirst wbwritten pausedead`); a full inbox
  pauses the link (`plink_pause`: no reading, keepalive goes on) and never blocks its thread; `fwd_cancel`; the
  instance is freed only after tsdbd's workers are joined (`fwd_cluster_free`), `test_cluster fwdlinks`. The entry node's decision is `route_decide` in `server/tsdbd.c` (D5, S2-1:
  served only where every cluster volume of the request is led here, else MOVED to the copy's leader, reads included, or
  UNAVAILABLE NO_OWNER/PROMOTING; before any right on an id is checked; a frame spanning two owners is INVALID;
  docs/design-cluster.md 6.4; NO_OWNER when no ISR member is UP, 6.2); a refusal after it (a leader deposed by a failover
  before the engine call) takes the same answer (`route_fill` in `frame_put`), and `TSDB_REPLICA_READS=1` (a test hook) lets
  a replica that holds a lease serve reads (test_cluster defaults to it: a scenario about a read's safety sets
  `strict_reads`, the production path); a read admitted under the lease (`lguard`) records which of its volumes are led
  here and at which epoch, and its first reply frame (after the snapshot) refuses it when that leadership changed
  (deposed, or deposed and promoted again) or the lease lapsed meanwhile: never "led now", which a read held across a
  deposal answers from the demoted replica engine (`test_cluster readdepose`, mutant `leadnow` in repld_mutants.sh); `TSDB_ROUTE_MUTANT` plants its bugs, `test_cluster route` must catch each.
  Forwarding (S2-2, `--forward on`, the default; `server/fwdreq.[ch]` the envelope and the entry's rules): where
  route_decide would answer MOVED, the entry sends the frame one hop to the leader (`fwd_entry`, never retried: unsent by
  `fwd_wait_ms` is UNAVAILABLE, written and lost closes the client's connection) and relays the reply; the owner runs it
  on reserved workers (`fwd_serve`) through the same gates with the user's rights, MOVED when not led at the envelope's
  epoch, UNAVAILABLE while its users lag the envelope's index; `tests/test_fwd.c` (fake link), `test_cluster
  forward|fwdseries|fwdseriesstrict|fwdlease|fwddepose|fwdload`, `make fwd-mutants` (`TSDBD_FWD_MUTANT`; test hooks `TSDBD_TEST_FWD_STALL`,
  `TSDB_CLUSTER_TEST hold_copy_file|hold_users_file`); end to end through entry nodes: `test_cluster viabench`, jepsen-cluster `kv-via` with
  `fwdstall` (read-your-writes, "old frame"). Planned leader transfer and leader balance (R4 D14, D15;
  design-cluster.md 3.2 "as built"): `server/rtransfer.c` the volume leader's drain, READY and hold (TSDB_REPLD_MUTANT
  `transfernodrain transferadmit transferresume`), `server/ctransfer.c` the Raft leader's TRANSFER, CANCEL, targeted
  promotion and balance (TSDB_CLUSTER_MUTANT `transferanycand transferfewreplies balanceflap nocancel readynocancel`);
  transfer ids increase across Raft leaders and every VOLCMD of a transfer carries one; `make transfer-mutants`
  (tests/transfer_mutants.sh), `test_cluster transfer|transferslow|transferhold|transferstale|transfermarkers|transferkill|
  transferraftkill|balance|balancetwo|balancecut` (hooks: TSDB_REPLD_TEST `admithold`, TSDB_CLUSTER_TEST `drop_fence_file`,
  `lose_ready_file`; `TR_TRANSFERS` the count of `transfer`, TR_DEBUG=1 its slow writes). Replica moves (R4, r4-move; design-cluster.md 3.2
  "as built"): `server/cmove.c` on the Raft leader (`tsdb cluster volume move PEER V --log --from A --to B [--cancel]`:
  ADD of B with `replaces` A, wait for B in the ISR noting `min_isr`, a transfer first when A leads, DROP_MEMBER to the
  volume leader (`rset.c` -> `repl_leave(core, A, min_isr)`), REMOVE of A; resumed from the record by a new Raft leader;
  STATUS's `moves` counters and lines; crash points `move-added`, `move-shrunk`; TSDB_CLUSTER_MUTANT `moveshrinkfirst
  movenoreplaces moveremovebeforeshrink moveleaderfirst movenominisr`, `make move-mutants` (tests/move_mutants.sh);
  `test_cluster move|movelag|moveleader|moverep0|moveraft|movekill|moveold`, which follow every record of the volume
  through `vol_trace` and require the ISR without A never smaller than before B joined; hook TSDB_CMOVE_TEST
  `hold_shrink=PATH`); jepsen-cluster's `churn` nemesis moves kvr's replicas at random (the same ISR check over every node's voltrace when it runs alone). Users are the metadata group's: `tsdb cluster user ...`, logins on every member
  (Login and rights, below; docs/cluster-preview.md "Logins"). `tests/test_meta.c`: the state machine and a simulator of it under real Raft cores (`make meta-soak`
  for thousands of seeds; `make meta-mutants` plants bugs in meta.c and requires test_meta (its unit tests or the simulator's checker) to catch each);
  `tests/test_cluster.c`: tsdbd end to end (init, node add, leader killed, restart, witness, flags, retired, stalled
  disk, power loss under crashfs; `lease` and `hungloop` the leases and the serving loop's stamp, whose mutants
  `make lease-mutants` plants). The metadata state (NODE, IDENT, `raft/`, snapshot, `copy`, `retired`) lives in
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
- `src/chunk.h` (the API; internal, called by the engine only through R7's tier, below), `src/chunk.c` (the geometry of an object,
  the chunk file format, the encoder), `src/chunkstore.c` (chunk stores: META with the store id, `s<volume>/EPOCH`, the
  fences, the directory backend), `src/chunkread.c` (reads of any byte range with decode): design-cluster-data.md R6.
  The geometry (k', m, cell, length) is recorded in the object and in every chunk header and never taken from settings
  (E2); a chunk whose header, size, sums table, footer or identity is wrong, or whose cell fails its sum, is an erasure,
  never decoder input, and a short read is an error, never the end (`CHUNK_MUT_EOF`). The rebuilt cell of a decode is
  checked against the object record's sum when the caller has it. A temporary file is always a new inode (`tmp_*`,
  removed at open): one that a failed write or sync left may have had its pages dropped. A put or delete below a volume's
  EPOCH (two checksummed slots, FNO's scheme, fail-stop) is refused; a failed directory fsync after a name appeared or
  went, or any failed EPOCH write or sync, fails the store (`chunk_store_failed`: it acknowledges nothing more, reads
  go on). A store directory has one owner: `chunk_store_open` takes a `flock` (a second open is `CHUNK_ERR_BUSY`),
  because two owners would each keep their own epoch and one would write it back lower. A volume with one damaged EPOCH
  slot opens "unsure" (a torn store and rot look alike): puts and deletes are refused (`CHUNK_ERR_EPOCH_UNSURE`) until
  `chunk_store_raise` / `chunk_be_see_epoch` is given the epoch the metadata group holds; a writer's own epoch never
  stands in for the lost fence. A readdir error is an error, never the end of a directory. `chunk_be_open` takes a NULL
  path for a store known to be down (its index stays; named stores must open). `tests/test_chunk.c` (exhaustive damage patterns for every geometry, power loss and EIO under crashfs, mutants
  via `--mutant nosums|nofence|eof|settings|nosync|epochnosync`), `tests/fuzz/fuzz_chunk.c`, `tests/fuzz/fuzz_ec.c`.
  The embedded bindings copy `src/*.c`: the files build without `TSDB_EC` (`chunk_encode` then says `TSDB_ERR_INVALID`).
- `src/chunk*.c` (objects, chunk files, stores, the directory backend; R6): `chunkstore.c` (files, EPOCH, `chunk_be_open2` with
  `min_up`: a store that is missing, has another id, a damaged META or EIO is DOWN and read around, BUSY and one store twice
  always fail the open), `chunkread.c` (decode; UNAVAILABLE while stores are down, CORRUPT only when lost), `chunkheal.c`
  (heal, repair, settle, attach, replace: rebuilt chunks are checked against the survivors' sums, written as new files at
  the object's epoch, never from fewer than k' positions, never for what `live` says is deleted; a new store gets every
  fence before its first chunk). `tests/test_chunk.c`, `tests/test_chunk_heal.c` (mutants in the Makefile).
- `src/ecgc.c`, `src/ecgcmk.c` (R7, r7-gc; R8, r8k-gc; D6, D13, E14): retired `.ec` parts and their objects. Single-server
  tiers keep R7's rules: a retired `.ec` keeps its `retired_` name until every chunk of its object is deleted
  (`part_unref` leaves it: `ecgc_owns_name`); nothing is deleted while any `.ec` part of the object is open (holds in
  the tier acquired in `part_open_ec` via `ecgc_hold` and dropped in `ecgc_last_unref`, keyed by `const part *` and
  `(volume, oid)` in O(1) open-addressing hash tables); chunk I/O runs on the tier's worker, never under an engine lock;
  the open sweeps orphans (D6: after recovery, before any thread starts) by file name, reading one header per object to
  check `vuuid`. On cluster EC volumes (`db->ec_cluster`): `ecgc_purge_entry` returns false and `ecgc_open_sweep` is
  skipped; deletes go exclusively through committed `MK_GC` via `ecgcmk_work` on the tier's GC worker (`ecgc_tier_work`).
  The leader appends `MK_GC` (`MK_GC_RETIRED` for retired `.ec` objects past `obsolete_part_lifetime_s` with zero holds,
  even when `gc_suspended`; `MK_GC_ORPHAN` for orphans meeting E14's four conditions: not named by any listed or retired
  `.ec` or any WAL `MK_COLD`/`MK_DIGEST` record committed or uncommitted, written in an earlier epoch `< leader_epoch`,
  older than `GCMK_ORPHAN_GRACE_DEFAULT_MS` 60000 ms, and `gc_suspended` false), waits for commit, issues `CHUNK_DEL` to
  all stripe positions with its `lead_epoch` (`coldvol_epoch`) and the `MK_GC` marker's position (`mk_pos > 0`; `ABSENT`
  counts as deleted), and appends `MK_GC_DONE` once every position answers; `CHUNK_ERR_FENCED` stops GC on a deposed
  leader (`fenced`). Every member applies `MK_GC` in `ecgcmk_apply`: retires any still-listed `.ec` of those oids first,
  records pending oids in the durable checksummed `GCMK` file (48-byte offset-bound xxh64 records, torn tail cut,
  compacted via `GCMK.tmp` + `tsdb_fdatasync` + `rename` + dir fsync), unlinks `retired_` names with directory fsync,
  and clears `GCMK` + lingering `retired_` entries on `MK_GC_DONE`; a promoted leader drains pending oids from `GCMK`.
  R9 (r9-whole, D12) adds `MK_GC` reasons `MK_GC_RELEASE` (4) and `MK_GC_RELEASE_DONE` (5) carrying a 32-byte release
  body `{volume, pos, node, version}` with `noids == 0` and corresponding `GCMK` ops `GCMK_OP_REL_ADD` (3) /
  `GCMK_OP_REL_DONE` (4): once a replacement holder is whole (its `CHUNK_LIST` verified against `OBJLIST` on a caught-up
  copy), the leader commits `MK_GC_RELEASE` (`CP_RELEASE_GC`), issues guarded `CS_OP_DEL_REL` (verifying `caught_up`,
  whole state, exact `set_ver`, and server-side `rel_guard` before every delete), and commits `MK_GC_RELEASE_DONE`.
  `tests/test_ecgc.c` (mutants `gcunlinkfirst gcunrefleak gcnohold sweeplive sweepinflight sweepforeign unlistednoop
  noretrytimer gcuncommitted gcnamedorphan gcsuspendignored holdatretire gcreplicanoop releasenomark`), `test_cluster ecgc`,
  `test_cluster ecwhole`, `make ecgc-mutants` (`gcsuspendignored gcreplicanoop`), and `make whole-mutants`
  (`wholeearly releasenomark readfromfilling`).
- `src/snapshot.[ch]` (R4 D3, D4; design-cluster.md 5.2-5.3, 8.1): the make side of a leader's snapshot. `snap_make`
  takes the cut P (the commit point) under `flush_lock` then `publish_lock` with no I/O (the part list, the memtables'
  segments and S, RSTATE, the COMMIT markers' files), releases both, hard links every part before it reads any (by its
  `retired_` name when a merge renamed one after the cut; neither: `SNAP_RETRY`; a memtable's segment the keep bound
  removed before the listing is `SNAP_RETRY` too, never CORRUPT), the segments S..P (P's copied up to its record end), the
  `stage_` files of COMMITs above `done` and the `keep_` files of those at or below it, copies SERIES, STATES, RETENTION
  and CLASSES to the length read under their one lock each (taken alone), then fsyncs every file and the directory and
  writes `SNAPMAN` last (temporary, fsync, rename, directory fsync; version 2: a `keep_` file that is a listed part's
  inode is named as that entry's link, read once). `snap_man_read` refuses `.`, `..`, its own names, a name twice,
  unknown kinds and bad links (a SNAPMAN comes from another node). `snap_release` removes a snapshot, and the open of an
  engine removes every `snap_*` directory (no make survives its engine). `tests/test_snapshot.c` (`snap_verify_cut`, the
  windows, merges during the links, a segment removed before and after the listing, random writers, power loss at every
  step under crashfs with the link model; the verify also requires everything the leader had at the cut, noted by
  `snap_test_cut` under the cut's locks; mutants `snapafterp snapdictshort snapnokeep snapnostage snapnoretired
  snapnodirsync snapnoneedseq snapnosegretry`).
  The install side (r4-snapinst, snapshot.c's second half): `snap_install(root, N, "<root>/vN.snap", id, keep)` checks
  every file against SNAPMAN and every part's checksums, refuses a file SNAPMAN does not name, writes META, FNO = P + 1,
  REPL (floor P; D6 with `keep`, the replaced directory's REPL: fence never lowered, led never raised, unsure kept with
  the open generation's belt) and `SNAPDONE`, each synced, then swaps vN -> vN.old, vN.snap -> vN, syncs the root,
  removes vN.old and syncs the root again: a 0 means vN is the new directory on disk, before any recovery.
  `snap_leftovers` (before vN is looked at) finishes an install whose SNAPDONE is there and removes or renames back
  the rest. SNAPMAN never names the install's own files (SNAPDONE, REPL, FNO, LOCK, META.tmp): leftovers trusts SNAPDONE
  by its name. vN.old without vN is refused (leftovers puts it back first). `rm_dir_at` removes subdirectories too (a
  former leader's vN holds its `snap_*` directories until a writable open sweeps them) and syncs a directory after each
  pass of unlinks (crashfs keeps unsynced unlinks aside in the directory, and on disk an emptied directory must be
  durable before its rmdir). Tests: `test_snapshot` install cases
  (made under writers, installed, compared with a replica fed 1..P, fed on to the leader's end, promoted and serving
  every COMMIT file; `keep`; `verify`: each check of the verify alone on a damaged or foreign copy, and what a good
  install wrote; power loss at every step of an install under crashfs with tearing (SNAPDONE durable: new), the step that ran
  whole with 16 seeds; `--only install` runs these alone), mutants `snaplowerfence snapraiseled snapkeepunsure
  snapswapfirst snapinstnodirsync`. A crashfs child that must lose power at its end exits with `exit`, never `_exit`
  (the loss at exit is the shim's destructor).
  An erasure-coded cluster volume (META `requires=` with `ec`; snap-ec): SNAPMAN version 3 gives every entry a `ppos`
  (0 = none; only PART and EC entries may carry one), the make lists the cut's `.ec` parts (kind `SNAP_EC`, their index
  file and object record; the chunks stay on the stores) and takes each listed name's PPOS record under `db->lock`; the
  install opens every `.ec` with `part_open_ec_at` (hashes, record; the record's ppos must be at or below P and SNAPMAN's
  must be what the leader's swap recorded: the record's ppos, or PPOS_UNKNOWN for a record ppos of 0), requires a ppos on every part of an ec volume and none elsewhere, and writes `PPOS` (`ppos_write_new`, synced with
  the directory) BEFORE META, FNO, REPL and SNAPDONE (PPOS is one of the install's own names). `settled_` and `retired_`
  names never travel (snap_man_read refuses them in every kind); COLD's `stage_`/`keep_` files travel as a COMMIT's.
  The install never checks that an object's chunks are on the stores (it does not open them; r8k's MK_GC keeps them).
  A member built from a snapshot has no `settled_` copy of the objects settled before P, so it adds no digest of them.
  `test_snapshot --only ec` (`ec`: snapshots made under writers around a settle, one between its COLD marker and its
  swap, installed, read back from the stores, fed on, no chunk removed by the receiver's open; each install check of an
  ec copy alone on a damaged or foreign copy; `ec power`: crashfs at every step of such an install); mutants
  `snapinstnoppos snappposafter snapcarrysettled snapskipec snapecnoverify snapecnozeq snapecnozp snapecnoppos` (their
  own Makefile loop, built only with `TSDB_EC`); `--only man` runs SNAPMAN's refusals (mutant `snapnoreserved`).
- `server/rsnap.c`, `server/snapinst.c`, `server/snapinst_run.c` (R4 r4-xferlink, r4-xferinst; D2, D7, D16, D18;
  design-cluster.md 8.1 "Built in R4"): a snapshot's transfer and install. The receiver's driver calls `rsnap_need` at the
  core's `REPL_A_SNAPSHOT` (the notice names the leader); frames REQ, BUSY, GO, DONE, ABORT and the files go on the
  snapshot link kind 5 (`PL_KIND_SNAP`, its own token bucket `rebuild_mb_s`, `peerlink_set_kind_rate`: a COMMIT's file on
  kind 2 never waits behind one), SNAPMAN first, then each file hashed as it arrives by the link's own data callback.
  One worker thread per server makes, releases and installs (`rebuild_max` jobs alive per role, sent and received counted
  apart); never the driver's, a link's or the cluster thread. The receiver checks free space (total + max(10%, 1 GiB))
  before it says GO. `snapinst_run.c` takes the volume from the manager (`cvol_install`: RECOVERING, driver stopped, engine
  closed), reads the old REPL as `keep` only then, runs `snap_install`, and the manager opens the volume as a new record
  (new open generation, REPLICA_OPEN waited for); P at or below the stopped driver's log end, or an epoch below the fence,
  is "stale and not installed", checked before and after the close. `snapinst_leftovers` runs `snap_leftovers` at
  `cvolfs_prepare`. Crash points `snap-received`, `snap-swapped`, `snap-halfswap`; STATUS `snap.*` and the metrics
  `tsdb_snapshot_*` (docs/metrics.md). Tests: `test_cluster snapxfer snaplarge snapnospace snapbusy snapsrckill snaplost
  snapstall snapcatch snapkill snappower snapfence snapdiverged addsnap keepcap`, `test_cvolfs`; mutants through
  `make repld-mutants` (`snapnoverify snapsamelink snapnospacecheck snapsrcpromoting snapnosrcmax snapsharedmax
  snapnodonebits snapmakedown snapnowaitdone snapnostale snapnostall snapnosrcdown snapstalekeep snapinstallstale
  snapservebeforeopen snapkeepdiverged snapsetnodefer snapclosekeep snapdropafter snapnoendafter snapnofence
  snapdivlogend snapnoleftovers`; each pinned to its FAIL line in `tests/repld_mutants.sh`). Not built: the receive-side
  rate limit, reuse of files the receiver holds (D4). An EC volume's `.ec` parts travel with their PPOS
  records (snap-ec, `src/snapshot.[ch]` above).
- `server/repld_int.h` (r4-iface): the replication driver's structs (`rvol`, `rpeer`, `item`, `G`) and the few helpers
  that `repld.c` shares with `rsnap.c`, `rset.c` and `rtransfer.c` (the names it exports for them are `rd_*`, `#define`d
  back to the short ones in the header, so each source reads as before); the lock rules of `repld.c` hold in every file that includes it.
  `server/cluster_int.h`: what `ctransfer.c`, `cmove.c`, `cset.c`, `cnode.c` and `cheal.c` (the Raft leader's side) need of `cluster.c` (`cl_*`:
  the clock, this node, the leader's peer, a proposal and its reply, the duty hooks, reported volume health) and the entry points `cluster.c` calls;
  all of it runs on the cluster thread only (`cheal.c` uses a private leaf lock only to hand its blocked-volume snapshot to the metrics collector).
  `server/cheal.c` (R12, r12-heal; PLAN.md D8): automatic healing of `CL_HEALTH_DAMAGED` replicas on the Raft leader (`cheal_duty`: planned transfer via `ctransfer.c` for a DAMAGED leader to a healthy non-DAMAGED ISR member, `REPLICA_RESET` for a DAMAGED non-leader non-ISR replica while healthy non-DAMAGED ISR `>= min_isr`, one rebuild at a time, 10 min per-volume cooldown with exponential failure/recurrence backoff up to 1 h, `tsdb_cluster_auto_resets_total{why}` and `tsdb_cluster_heal_blocked{volume,why}` via `creport.h`; `TSDB_CLUSTER_TEST` `heal_off=1`, `heal_cooldown_ms=N`; `TSDB_CLUSTER_MUTANT` `healleader`, `healbelowmin`, `healrepeat`; `test_cluster scrubheal`, `make heal-mutants`). R4's wire pieces: admin ops 10 to 12
  (`CLUSTER_OP_VOLUME_TRANSFER`, `_VOLUME_MOVE`, `_REPLICA_RESET` in `cluster.h`), the Raft leader's command to a volume's member
  (`F_VOLCMD`, `CL_VOLCMD_*`) and the member's READY, link kind 5, peer protocol 7 for these, 8 now.
- `src/coldops.h`, `src/coldops_dir.c` (R8, E22): the cold ops of one volume (put, open/read, delete, list, owner,
  epoch, see_epoch, writable, all_up); settle.c, eccache.c, eckeep.c, ecgc.c and tier.c reach the stores only through them
  and never name a store index. Per attached engine (`tier_attach`: ops, settings, epoch provider; a refcounted `coldvol`
  in the tier's registry, `tier_cold_get` by volume); the epoch is the provider's (`tier_epoch`, 1 on a single server),
  and an unsure store is raised only by it (mutant `selfepoch`, `test_tier`).
- `src/settle.c`, `src/ecpolicy.c` (R7, design-cluster-data.md E5-E7, E26): `eng_settle` turns a key's parts into `.ec`
  parts in ONE pass over each part's map (each stripe copied once; the granule crcs, the header, index and footer hash
  and the CLASSES judgement are checked on that copy, which is what is encoded: never read the map twice), then
  `ec_index_write` and one `ec_swap_commit`. Pinned by `settling` under `db->lock`; merging, damaged, v8, pending and
  granule-less parts are skipped, a dropping key is skipped whole; refused on a cluster or replica volume (D1), without
  a tier and while a store is not UP (`tier_writable`). A CORRUPT check marks the part damaged; any failure hands its
  own chunks to r7-gc, each object once (a NOMEM intent write: the caller, never the swap too). A refused swap
  (`swap_discard`) hands them over only after the directory fsync of its unlinks (until then a power cut can bring the
  intent back with its temporaries and roll it forward), also after a failed intent write, and never on a failed
  engine (the next open's sweep takes them). One settle thread per tier (nice 19, one settle at a time; never `jr_serial`, `flush_lock`,
  `cls_rw`); `eng_halt` and `eng_close` stop and wait for a settle of their engine. The policy (`ecpolicy_pick_locked`,
  ages from `eng_now_ms`): class 0 a key after `grace_s` once no merge of it is selectable; classes 1-3 part by part, at
  most once per `coarse_s` from the newest `.ec` record's settle time; `eager` (jepsen) after a merge round or above
  1 MiB, 30 a minute; a failed key waits an hour. `tests/test_settle.c` (mutants `ecrankdown settlenocheck
  settletwopass settlebusy coarsewhole`; the failure paths' handoffs `gcskip* gccommitted gcdouble` (gcpaths), `gcearly
  nullnosync syncfailgo` (gcpower, crashfs), `nohaltstop` (halt)); `tests/test_settle_oracle.c` against an unsettled
  twin (`tests/oracle_mutants.sh`: a mutant counts as caught only by its own line).
- `src/tier.[ch]`, `src/ecpart.[ch]`, `src/eccache.[ch]`, `src/ecswap.[ch]`, `src/ecskip.h`, `src/eckeep.[ch]` (R7,
  design-cluster-data.md §2.3-2.4, §15.7; the plan's decisions D1-D11): settled parts on ONE server, behind a switch that is
  test-only until R15. A handle's `struct tier` (`src/tier.c`, one per `tsdb`, reference-counted: every engine of the handle,
  retired ones included, holds one, and the last to let go closes it) owns the one chunk backend (`chunk_be_open2`, `min_up`
  = stores - m: a store that is missing, damaged or EIO at open is DOWN and read around), the cache, the settle thread and
  the gc worker. It is configured by `TSDB_TEST_TIER=k=2,m=1,dirs=/a:/b:/c[,grace_s=,keep_s=,coarse_s=,cache_mb=,eager=]`
  (grammar in `src/tier.h`) and acts only with `TSDB_TEST_TIER_I_KNOW=1` beside it (D3: set alone the open fails with a line,
  so a stray variable never writes `requires=tier` into META); there are no public options and no `TIERS` before R15. The
  open of a volume writes `vuuid=` and `requires=tier` to META (tmp, fsync, rename) and raises every store's epoch to the
  constant 1 (D4); it refuses `TSDB_TEST_TIER` on a read-only open or a follower when no `TIERS` file is present (with `TIERS`, writer timing overrides are ignored and only `cache_mb` applies), a `requires=cluster|replica` volume (D1, D7) and a
  build without `TSDB_EC`; read-only opens and followers of a `TIERS` database open a read-only tier (R15 D13, `CHUNK_OPEN_RDONLY`: no flock, no `EPOCH` write, no `tmp_*` cleanup, no write/delete/heal; every non-`LOST` store is required with `store_id` and `db_id` checked, `.ec` parts are read through the follower's own `eccache`, the store set from open is kept until reopen without a health/attach worker, and `ecgc` keeps retired chunks for `obsolete_part_lifetime_s`); a store without META is a lost disk (DOWN, never remade) once any volume exists. `tier_writable`
  turns false, with one line, once a store is not UP, failed in service or did not take the volume's epoch at open (mutant `tierfence`): settles and chunk deletes pause while any store is DOWN (`DOWN_IOERR` / `DOWN_STALLED`) and resume once all stores recover to UP (a fence halt stays sticky until reopen); reads go on. `src/tier_health.c` (R15 D8): per-store detached `O_DIRECT` probe threads (`<store>/PROBE.tier`, default 10 s stall bound), runtime `chunk_be_set_down(be, i, reason)` on refcounted `chunk_store`s, a dedicated attach/heal worker (never on the probe thread: verifies META `id`/`db_id`, runs `chunk_be_attach`, sweeps missed-delete orphan chunks, reopens cached handles, resumes settles/GC), `--health-domain` prefix matching over tier stores in `tsdbd` without quiescing volumes (`STATUS` reports `tier.stores` and `tier.store.<i>.{state,path,domain}`), abandon-flag checks in `src/chunkread.c` and `src/eccache.c` returning `TSDB_ERR_UNAVAILABLE`, and mutants `downnoread` and `attachnoheal` in `test_tier`. `src/tierfill.[ch]` (R15 r15-full, E53, §15.7, D7): per-engine atomic `full` flag (enters at integer fill % >= `young_high_pct`, clears at `< young_high_pct - 2`), seeded synchronously at `tier_engine_ready` and kept fresh every 1 s by a per-volume detached-capable sampler thread (never holds `t->eng_active` across `statvfs` I/O, never blocks other volumes or close on an NFS hang); `src/volumes.c` `route_write` / `insert_slots` refuse inserts, appends, and non-empty replaces with `TSDB_ERR_UNAVAILABLE` (deletes including `tsdb_replace_range` with `n == 0` and drops pass; mutant `fullnoflag`); `src/chunkstore.c` 95% regular-put cap scoped to single-server tiers via `chunk_be_set_put_cap` (`CHUNK_ERR_FULL`, rebuild/heal puts pass, cached store bytes for `/metrics` without syscalls; mutant `storeput96`); `server/metrics.c` tier metrics (`tsdb_tier_bytes`, `tsdb_tier_settles_waiting`, `tsdb_tier_store_state`, `tsdb_volume_full`), `server/tsdbd.c` standalone `TSDB_WHY_FULL` / `STATUS` / `TOPOLOGY`, and `dist/alerts/tsdb.rules.yml` checked by `tests/test_alert_rules.sh`. `src/ecpart.c`: the `.ec` file (`p<K>_<A>_<B>_<L>.ec`, the name and block range of its source,
  D2): the source's header, granule index, tombstones and footer verbatim (so `gx_*` read the index in place, unchanged),
  then the object record (oid, geometry, `object_len`, `object_xxh64`, the source's per-cell sums, `writer_epoch`,
  `settle_ms`, the class check, `positions` (R9 E10: 0 everywhere, or on cluster EC volumes all `k'+m` bits or, when `m >= 2`, `k'+m-1` bits for a degraded settle; single-server tiers still refuse non-zero), `ppos` and `stripe_version` (R8, cluster EC only)) and a
  64-byte trailer that hashes both; `part_open_ec` refuses a granule that does not lie inside the object's data in index
  order (a fuzz finding: a count alone let a 536-byte file walk 10^8 granules); the open fails on an `.ec` and a `.part` of
  one name with no intent, an `.ec` without `requires=tier`, and a record whose class check failed. `tests/test_ecpart.c`
  (mutants in the Makefile), `tests/fuzz/fuzz_ecpart.c`. `src/eccache.c`, the read seam: `ec_granule_read` (reached from
  `granule_read`, the only door to granule bytes of a settled part) takes a granule's bytes from a cache entry keyed
  (oid, offset), filled by one read of the object's byte range and CHECKED THERE against the record's sums, whatever
  `verify_checksums` says, then decoded from the pinned entry with `verify=false`; a hit whose length or crc differs from
  the granule's is CORRUPT; a damaged cell is rebuilt from the other positions (a crc mismatch reads once more as whole
  cells); an error with a store DOWN and fewer than k' positions left is `TSDB_ERR_UNAVAILABLE`, a lost object CORRUPT naming
  the granule. A byte budget (`cache_mb`, 0: 10% of RAM) with LRU, an LRU of open objects under an fd budget (pinned ones
  never closed); the cache mutex is a leaf, never held across I/O. Statistics count an `.ec` part's bytes
  (`eccache_part_bytes`) and its residency (`eccache_resident`). `src/ecswap.c`: the swap, intent v4, below under "Settled
  parts". `src/ecskip.h`: `part_merge_barrier` (an `.ec` or a `settling` part). `src/eckeep.c`: `settled_*.part`, the source
  a swap renamed, is a graveyard kind (`GY_SETTLED`) kept for `keep_s` counted from the `.ec` record's `settle_ms` (never
  an mtime: a rename keeps it, and a restart would find every file due) and until its digest matched. The digest (one thread
  per engine, nice 19, `eckeep_digest_mb_s`) reads the object whole, every cell checked, and compares its xxh64 and the
  `settled_` file's with the record's; every `settled_` file is digested again at each open before any is released; a store
  that is not UP holds it, never "matches" by reading around. A mismatch sets `db->diverged`, and the `route()` family
  (`eng_refused`) answers `TSDB_ERR_UNAVAILABLE` for the volume until a restart, an in-process reopen carrying the flag
  (D5). `tests/test_tier.c`, `test_seam.c`, `test_ecskip.c`, `test_ecswap.c`, `test_keep.c`, `test_ecnames.c` (every R7
  file name against every predicate, `make NO_EC=1`), `test_settle.c`, `test_settle_oracle.c`, `test_ecgc.c`, each with
  its planted bugs (`--mutant NAME`, a Makefile loop that requires each to fail). A test that cannot run without the codec
  prints that it is skipped.
- `src/chunkscrub.[ch]` (per-store background chunk scrubber; R12, r12-chunk, E25, E26, §5.9): walks every volume's
  chunks per store verifying header/size/sums/footer and every cell's xxh64 (`chunk_rd_scrub_ex`), paced to `scrub_mb_s`
  (default 50 MiB/s) within `scrub_days` (default 14 d), paused during heals/fills (`sfill`), remote chunk reads
  (`CHUNKR`), and put bursts (`CHUNKB`). Advisory `<store>/SCRUB` cursor persisted via tmp/fsync/rename/dir-fsync on an
  independent store dirfd; missing or corrupt `SCRUB` logs and restarts cleanly. Damaged chunks (including EIO, never
  clean) are reported once per pass via `CS_OP_DAMAGED` on R8's `CHUNK` link to the volume leader
  (`server/chunksrv_cluster.c`, `server/chunkpeer_cluster.c`; leader-side cell repair orchestration belongs to the
  separate repair track); UTC-day damage above `scrub_out_count` (default 8) marks
  the store `CHUNK_STORE_DOWN_DAMAGED` locally (`chunk_be_mark_down`, `chunk_be_open_store` returns NULL so reads decode
  around it and puts/lists/deletes/heals skip it) and suspends further walking of the failing store. Heartbeat section `CREPORT_SEC_CHUNKSCRUB` (9) and Prometheus metrics
  `tsdb_chunk_scrub_*` (`docs/metrics.md`). Test knobs `TSDB_CHUNKSCRUB_TEST` and `TSDB_CHUNKSCRUB_MUTANT`
  (`TSDB_TEST_HOOKS`). `tests/test_chunkscrub.c`, `make chunkscrub-mutants`
  (`scrubskipcell scrubeioclean scrubrestart`), `test_cluster scrub`.
- `server/chunkrepair.[ch]` (volume-leader chunk repair; R12, r12-repair, E11, E14, E25, E26, §5.9, D5, D9): bounded,
  deduped `(volume, oid, pos)` repair queue with per-target-node serialization, exponential-backoff retries on
  transient `UNAVAILABLE`/`FAILED`, and a bounded post-failure cooldown per `(volume, oid, pos)` (default 30 s) so
  repeated read/scrub hits on a broken chunk do not storm peers; fed by `CS_OP_DAMAGED` (`cp_damaged_srv`) and
  read-path bad survivors (`chunkpeer.on_damaged`, skipping `META_STRIPE_FILLING` positions). Verifies live /
  not-retired / not-pending-`MK_GC` (`ecgc_oid_is_live`) before decoding and again right before the replace-put under
  `ecgc_oid_lock` (shared with `ecgcmk` `delete_cluster_object`, D9; both `oid_mu` and the engine ref are released
  before `net_put`). Decodes from `k` sum-verified survivors via `iso_rs`, checks every rebuilt cell against the object
  record's `sums` (E26; `< k` good with damaged survivors counts `unrecoverable`, emits a health log line, and writes
  nothing), and sends a v2 replace-put (`CS_PUT_MAGIC_V2`, `CS_PUT_F_REPLACE`: preserves `hdr_epoch = writer_epoch`
  under leader's pinned `fence_epoch`, refuses open/commit unless the target chunk file still exists under `st->mu` so
  late puts never resurrect deleted chunks or duplicate across stores, and runs `chunk_rd_scrub` read-back before
  `PUT_OK`). Exports
  `tsdb_chunk_repairs_total{result}`, `tsdb_chunk_repair_bytes_total`, and `tsdb_chunk_repair_queue_depth`. Test hooks
  `TSDB_REPAIR_MUTANT` and `TSDB_REPAIR_TEST=pause_before_put=PATH` (`TSDB_TEST_HOOKS`). `tests/test_chunkrepair.c`,
  `test_cluster scrubrepair`, `make repair-mutants` (`repairnolive repairnosums repairownepoch`).
- `server/chunksrv.[ch]`, `server/chunksrv_cluster.c` (R8 D9-D11, E14, E26, E27): the chunk server, a peerlink of its own on
  every cluster member with kinds `PL_KIND_CHUNK` (6, requests and answers, worked by 4 FIFO workers) and `PL_KIND_CHUNKB`
  (7, puts as bulk files under `--chunk-rate-mb-s`; the link's callbacks only queue, a put thread per peer does the store
  I/O (peerlink.h:24-26), and the put queue is bounded per peer at 16 MiB, with a 32 MiB shared request queue). Every epoch-bearing request raises the volume's fence first
  (E14; read-only `CS_OP_LIST` with `epoch == 0`, as sent by `sreconcile` and `ecgc`, skips `cs_see` so listing does not raise an unsure `EPOCH` as a side effect); an unsure store is raised only from `chunksrv_epoch_source` (`chunkpeer_epoch_source`, D9), and then to
  max(source, backend fence, every sure store's epoch), never from a request. LIST pages never split the copies of one
  (oid, pos). `chunksrv_cluster.c` opens the chunk stores selected by `server/disks.[ch]` (`<disk>/chunks/` of capacity
  disks, or of fast disks on servers without capacity disks unless `--role compute`; ordered by `<state-dir>/DISKS` with
  a `NULL` `CHUNK_STORE_DOWN_MISSING` slot for temporarily missing configured chunk disks so `hash % n` placement stays
  stable and reads answer `UNAVAILABLE`), listens once a member, follows the
  members (`chunksrv_keep_peers` stops those that left); EC cluster volumes are production in 0.11.0 (no `--ec-experimental`
  flag). `TSDB_CHUNKSRV_TEST` is its test hook for `test_cluster chunksrv`. `tests/test_chunksrv.c` (mutants
  `chunknofence chunkselfraise putokearly nocellcheck storeforeign nometacheck sumsonly noreqcheck noopmatch norespcheck`),
  `test_peerlink chunk`, fuzz-replframe mode 9. Open: a store that hangs (not fails) in
  fdatasync holds the backend's fence mutex and stalls every request (R13's health domains).
- `src/chunkmove.[ch]` (R13 `r13-lmove`; E50, §15.3, D18): local chunk moves between two stores of one backend
  (`chunk_move(be, id, from, to, opts)`): raises the volume's fence on `to` first (refuses `CHUNK_ERR_EPOCH_UNSURE`),
  copies cells paced to `balance_mb_s` (falling back to `rebuild_mb_s`, then 50 MiB/s) into `tmp_*` on `to`, writes
  tail and fsyncs `tmp_*` outside the per-`(volume, oid)` stripe lock (`oid_mu`), then under `oid_mu` verifies source
  `(st_dev, st_ino)` unchanged (aborting on concurrent `MK_GC` delete, retrying on `r12-repair` replace-put), commits
  rename + dir fsync (`defer_loc`), verifies every cell on `to` against its sums (unlinking corrupt target on failure),
  updates the local off-primary index (`loc_tab`), and unlinks the source + fsyncs its directory. Start-time dedupe in
  `src/chunkstore.c` (`be_open_dedupe`, enabled when `chunk_be_opts.store_pref` is set) verifies duplicate copies at
  open, keeps the whole copy on the preferred store (capacity over fast / drain target), drops damaged copies, and
  leaves all-damaged copies intact. Background local drain driver in `server/disks.c` (`disks_drain_start`/`stop`,
  `disks_mark_draining`) drains E50 fast-disk chunk stores when capacity disks are present (and prepares runtime drain
  for `r13-dev`), skips draining stores for new puts in `server/chunksrv.c` and `server/sfill.c`, reports
  `drain_bytes_left` via `r14-progress`'s `HBS_PROGRESS` callback, emits `warn=drain_no_space` in `HBS_DISKS` and
  `tsdb_disk_drain_*` / `tsdb_chunk_peerlink_bytes_total` / `tsdb_chunk_srv_bytes_total` metrics (`docs/metrics.md`),
  and skips unsure-EPOCH volumes with exponential backoff instead of stalling other volumes. Test knobs:
  `TSDB_LMOVE_MUTANT` (`moveunlinkfirst`, `movekeepwrong`) and `TSDB_DRAIN_RATE_BPS` (`TSDB_TEST_HOOKS`).
  `tests/test_chunkmove.c`, `make lmove-mutants`, `test_cluster hddadd`.
- `server/disks.[ch]` (R13 `r13-start`; E47-E49, E58, D11-D13): `--disk PATH[:CLASS]` (repeatable, `storage.disks`),
  `--role compute|storage|both` (`cluster.role`), `--allow-shared-device` (`cluster.allow.shared.device`, warns on health
  and `tsdb_disk_shared_device`), replacing `--chunk-dir` and `tsdb chunk-store init`. In cluster mode `--dir` is a disk
  too (uses its `DISK` metadata class without `/sys` detection, with a warning; missing `DISK` on `--dir` refuses the
  start). Two-phase startup: `disks_prepare()` checks `st_dev` against `/`, `--state-dir` and sibling `--disk` paths,
  probes `flock` and `<state-dir>/NODE` ownership before `tsdb_open`, loads `DISK` (`src/disk.h`) and `<state-dir>/DISKS`
  (pinning the primary disk when `--dir` is omitted, and reporting known missing secondary stores `DOWN` instead of new);
  `disks_start()` verifies cluster/node ownership and non-`DESTROYED` state against `DISK` and the initial metadata copy
  (`cluster_initial_disk_lookup`), refuses explicit `:CLASS` mismatches naming both, logs/counts `/sys` detection
  disagreement (`tsdb_disk_class_mismatch`), binds unbound disks (`disk_bind`), and exports metrics. `disks_creport_register()`
  registers `HBS_DISKS` (`creport` section 12) in `cluster_start()` for all members including witnesses. The Raft leader
  proposes `STORE_ADD` for unknown `UP` store ids and `MEMBER_ROLE` on role change.
  `test_cluster disks`, `make disks-mutants` (`TSDB_DISKS_MUTANT`: `nodevcheck classsilent missingasnew`).
  R13 `r13-fault` (E57, `docs/design-cluster-data.md` §15.6, `PLAN.md` D16): `disk_probe_*` runs one `health.c`
  `O_DIRECT` probe domain per present `--disk` (`down_ms` tracks `disk_stall_s` via `tsdb_health_set_down_ms`,
  `probe_hold_locked`/`probe_users` drain active probe callers before `disk_probe_stop` closes `probe_h`); a `DOWN`
  fast disk deposes/quiesces only the volumes on that disk (`repld_disk_down`) without exiting the process, and
  `DOWN -> RECOVERING` reopens them from disk (`cvol_disk_reopen`, `cvol_disk_reopen_refused` for disk-down refusals
  only) to rejoin ISR; chunk stores mark `DOWN_IOERR`/`DOWN_STALLED` on EIO/ENOSPC or probe `DOWN`, route new puts to
  surviving UP stores, and serve `PL_KIND_CHUNK` through bounded per-store 2-worker pools (routing non-UP stores to the
  shared pool so hung disks never starve other stores or pause the shared link); `HBS_DISKS` carries live per-store
  state and `tsdb_disk_state{store,class}` is exported via `creport`.
  Tests: `test_chunksrv` (`test_spool_fallback`), `test_cluster diskstall diskeio`, `make diskfault-mutants`
  (`TSDB_DISKS_MUTANT`: `stallexit stallnoresign storeeioexit`).
  R13 `r13-fill` (E30, E50, E53, `docs/design-cluster-data.md` §15.4, `PLAN.md` D17): `disk_usage_*` runs one
  refcounted, detachable 256 KiB worker per present UP disk (skipping non-UP/stalled disks, bounded 500 ms stop)
  sampling bytes by use (`young`, `retained`, `retired`, `local`, `index`, `chunks`; O(1) `statvfs` for chunks on
  dedicated disks) every 10 s (`TSDB_DISK_USAGE_MS` / `TSDB_DISK_USAGE_FILE` supporting `<pct> <pct>`, `bytes`,
  `phantom`, `size` under `TSDB_TEST_HOOKS`), reporting `HBS_DISKS` v3 with 7-day young peak and fill flags
  (`75%` `warn=filling`, `80%` `warn=move_needed` + placement exclusion, `90%` `warn=full` with hysteresis `< high - 2`
  scoped ONLY to volumes on the full disk via `disks_vol_full` / `RW_ACK_FULL` -> `TSDB_WHY_FULL`, while volumes on
  other disks of the same server write on). At `>= 90%`, `eng_set_disk_full` releases matched retired settle copies
  immediately (`src/eckeep.c`), holds non-shrinking merges while allowing TTL rewrites and tombstone merges
  (`src/merge.c`) and lets `src/ecpolicy.c` settle unmerged/level-0 keys out of FULL; chunk stores past the cluster
  fill mark / shared young reserve soft-refuse regular puts with `CHUNK_ERR_FULL` (store stays `UP`) while local
  rebuilds (`server/sfill.c`) and remote repairs (`CS_PUT_F_REPLACE`) stay allowed up to 95%. Exports
  `tsdb_disk_bytes{store,class,use}`, `tsdb_disk_size_bytes{store,class}`, `tsdb_volume_full{volume}`, local
  `disk.<id>.*` / `vol.<N>.early_releases` / `vol.<N>.merges_held` and Raft leader `cluster.disks.<class>.*` in STATUS.
  Tests: `test_chunksrv` (`test_fill_mark`), `test_cluster diskfill`, `make diskfill-mutants`
  (`TSDB_DISKS_MUTANT`: `fullall fullmergeon fullnorelease`).
- `server/chunkpeer.[ch]`, `server/chunkpeer_cluster.c` (R8 r8f/r8k): `coldops_peer`, the `cold_ops` implementation for
  cluster EC volumes (`requires=tier,ec` set by `chunkpeer_cluster_prepare` before `cvol` open). `put_stream` stages each
  position as an unlinked temporary (`mkstemp` + `unlink`, sums then cells), writes the leader's own position locally
  and sends others over `CHUNKB`, comparing every `PUT_OK`'s `cs_put_answer_hash` (no degraded settle in R8: E10 is
  R9's); `obj_open`/`obj_read` fan out per position on detached threads (`k' + 200 ms` grace, `UNAVAILABLE` vs `CORRUPT`
  per E15); `del` (R7 queue/sweep) refuses on cluster volumes while `del_gc` sends `CHUNK_DEL` only with a committed
  `MK_GC` position (`mk_pos > 0`); `see_epoch` / `writable` re-check unfenced or unsure members at most once a second.
  Epoch provider: `coldvol.lead_epoch` when leading (set at attach and `wal_open`, so a deposed leader with a current
  copy still puts/deletes at its own epoch and is fenced), else `chunkpeer_epoch` (copy's epoch); `chunkpeer_epoch_source`
  answers only while `cluster_copy_caught_up_at()`. `chunksrv_cluster_hold`/`release` guard shutdown. R9 (r9-whole, D10)
  adds `obj_open_ver` (reads pre-barrier `< V` objects at a `FILLING` position from UP `prev` A, else treats as
  `CHUNK_ST_DOWN` erasure unless B reported the object filled for `(pos, set_ver)`; `eccache_reopen_vol` and filled-table
  purge run on stripe transitions), `CS_OP_DEL_REL` with server-side `cp_rel_guard`, and `CP_WHOLE_PROPOSED` /
  `CP_RELEASE_GC`. `tests/test_chunkpeer.c`
  (mutants `ownepoch nosumcmp staleraise delbeforegc sticky unsuredown hedgeall noavoid hedgecorrupt readfromfilling`),
  `tests/test_sfill.c` (mutants `fillnocheck fillfewerk fillsurvivortrust wholeearly`), `test_cluster ecbasic|ec16|ecwhole`,
  `make chunkpeer-mutants` (`ownepoch staleraise`), and `make whole-mutants` (`wholeearly releasenomark readfromfilling`).
- `src/ecdiverge.[ch]` (R8 r8i; D8, D23, E26): durable `DIVERGED` file in the volume directory (48-byte offset-bound
  xxh64 records, torn tail cut after `tail_confirm`, damage fails open, `fdatasync` + `eng_sync_dir` on first record
  before `db->diverged` is visible, fail-stop on write/sync error), read by `ecdiverge_open` before any engine thread
  starts. `tsdb DIR volume diverged [--clear]` prints or clears it (refused with `TSDB_ERR_LOCKED` while open, and with
  `TSDB_ERR_INVALID` on cluster volumes `META_REQ_CLUSTER`; use `tsdb cluster replica reset` to rebuild the replica instead). In `server/tsdbd.c`, one path
  `vol_diverged()` (`tsdb_volume_diverged` OR `dom.vol[v].diverged`), checked first in `vol_health`, drives `STATUS`,
  `UNAVAILABLE` (`TSDB_WHY_DIVERGED`, 60 s retry), `TOPOLOGY` and `tsdb_volume_diverged` metric (root volume included);
  `server/repld.c` stops a diverged replicated volume (`vol_fail`, leaves ISR, leader deposed) and `server/cvol.c`
  `on_report` sets `CL_HEALTH_LOCAL_FAULT` from the first heartbeat. `tests/test_diverge.c` (mutants `nocorrupt seed0
  nocut notailconfirm readerrend nofail nodirsync nofdatasync metaopen`), `fuzz_diverge`, `test_keep` (`divergemem`,
  `clearcluster`), `test_cluster diverged` (`TSDB_REPLD_TEST=plantdiverge`, `repld_mutants` `divergeisr`),
  `test_cli_diverged.sh`.
- `server/margins.[ch]` (R8 r8o; E15, E24): `margins_of` computes `young_margin` (`nisr - 1`, omitted for shared volumes)
  and, for EC volumes, `settled_margin` (present stripe positions minus `k`, counting UP/DRAINING members not declared
  destroyed) and `settled_state` (`ok`, `unavailable`, `corrupt`, `lost`). Exposed in `STATUS` and metrics
  (`tsdb_volume_young_margin`, `tsdb_volume_settled_margin`, `tsdb_volume_settled_state`, `tsdb_volume_margins_missing`;
  TOPOLOGY waits for protocol 16). `TSDB_WHY_LOST` = 18 (`TSDB_RETRY_MAX_MS`, never retried by the four clients).
  `tests/test_margins.c` (mutants `marginoff youngrep lostasdown sharedcopies corruptfirst jepsenlost`), `test_cluster
  margins`, `make margin-mutants` (`marginoff youngrep lostasdown lostnowait youngcorrupt`).
- `connectors/pulsar/`: Apache Pulsar 5 IO sink (Java, a NAR) over the Java client; `mvn test` there needs
  build/tsdbd, `connectors/pulsar/test/integration.sh` runs Pulsar 5.0.0 in Podman with faults.
- `.claude/`: shared Claude Code setup. `settings.json` runs `hooks/guard-bash.sh` (refuses `pkill -f`,
  `killall`, `kill` with pid -1 or 0, `docker`) and `hooks/guard-push.sh` (refuses a push while unpushed
  commits carry a session link, or match a line of a private pattern file kept outside the repository,
  `~/.config/claude-private-patterns`: internal hosts and the like never reach GitHub); `skills/tsdb-tracks/` is how changes are built, reviewed, gated and merged
  with subagents; `skills/subagent-economy/` keeps those subagents cheap (detached runs, a waiter, checkpoints; build machines: one `SE_REMOTE`, or a fleet
  `SE_REMOTES="user@hostA:3 user@hostB:8:tag"` in its config.sh that bgrun.sh picks a free slot from, `make test-se-fleet`;
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
  by one. The open's roll-forward (`intents_recover`) fsyncs the directory after an intent it rolls forward, renamed
  by this open or not (final names seen at open may be in the page cache only), and before it unlinks the intent:
  unsynced directory operations reach the disk in any order, and an unlink kept with a rename lost leaves the insert
  half there (`tests/test_intents.c`, crashfs with `CRASHFS_TEAR`; mutant `unlinkfirst`).
  **Drops are atomic too** (`drop_retire_parts_locked`, part.c "atomic drops"): `tsdb_drop_before`, the TTL drop of the
  merge thread (one intent per pass) and a replica's DROP write a drop intent (`tsdb-drop 1 <n>`, the part names, under
  a `txn_` name) and sync it and the directory before the first `retired_` rename, unlink it only after the directory
  fsync that follows the renames, and `intents_recover` rolls a whole one forward (an `.ec` name to `retired_<name>.ec`,
  before the listing and ecgc's sweep) in a second pass, after every insert and settle-swap intent: a drop does not wait
  for a settle, and a swap rolled forward hands the drop its `.ec` (a listed source found `settled_` is retired through
  its `.ec`); a torn one retired nothing; one whose checksum holds but names no part (MANIFEST, a path), or an existence
  check that fails with EIO, fails the open. The renames still go in block order, oldest first
  (the TTL drop went newest first): a prefix keeps every key at its newest value. `drop_retire` prepares the intent
  (write, `fdatasync`, and pre-rename directory `fsync`) outside `latest_rw` and `db->lock` (`drop_intent_prepare`)
  after waiting for in-flight merges, re-encodes, and active standalone publishers touching partitions `<= ts`
  (`db->drop_settled` blocks new standalone publishers touching those partitions while readers, writes to newer
  partitions, and R7 1-to-1 settle swaps run unblocked), then takes `latest_rw` and `db->lock` for the part-set check
  and renames (retrying on a concurrent settle swap); `ttl_select_locked` still runs `drop_retire_parts_locked` under
  `db->lock`. A failed intent write (a full disk: a drop of two parts or more
  needs a new file) fails just the call, except for `tsdb_drop_before` on a leader or a replica (the DROP marker is in
  the log, `drop_marker_logged`): fail-stop; a retention pass logs no marker and is never fail-stop there.
  `tests/test_drops.c` (kill, crashfs for the drop, the open and the retention drop, beside a swap intent, bad names,
  EIO, a full disk; mutants `nointent nosync unlinkfirst ttlreverse skipec dropfirst nosettled two ttlunlinkfirst
  novalidate eioasgone ttlfailstop`).
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
  A cluster volume that replicates (`server/repld.c`, D13) is the other exception: its driver stops it (a writer is
  deposed), it is quiesced and reported LOCAL_FAULT in the heartbeat, tsdbd serves its other volumes and its vote and
  exits 0 on SIGTERM (the failed volume is closed before `tsdb_close`, whose errors still set status 1).
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
  numbers skip ahead by `FNO_BELT` (2^40: a gap, never a repeat); both bad or no file in a format-9 or 10 volume is
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
  only into partitions without a part from that segment: a replicated leader keeps its flushed
  segments for the replicas (`eng_leader_keep`): one goes only once its last position is at or below min(the driver's
  keep bound, the commit point), after `fno_require`, by its own name (`lk_unlink_upto`: kept numbers jump at a promotion,
  so never through `wal_unlink_upto`'s run of consecutive ones); `wal_removed` never passes one still on disk.
  A replica keeps its flushed segments as well, past its saved commit floor, under the same caps (`replica_keep_cap`,
  `replica_keep_next` before its open; `unl_cap_bound` counts from the newest back), so that, promoted, it can serve a
  member that was away (`test_cluster failoverkeep`, mutant `replicadropsfloor`); never one above the floor.
  The files of the committed COMMIT markers in kept segments stay with them on every member as `keep_<pos>_<key>.part`
  (a replica's apply and open rename the stage_ file; a leader serves them from its attach, by position, never by name
  alone; `test_replica keepfiles`, mutant `replicadropsfiles`).
  Kept segments (a leader's flushed ones, a promoted replica's, and a replica's flushed ones at or below its saved
  floor, `kept_save` by the applier under `apply_mu`, never `rp->mu`) are named in RSTATE (`kept`, saved before a drop
  can take their rows out of parts): a writer's open neither replays nor removes a segment at or below it, a replica's
  open neither those at or below its floor (one above the floor, a deposed leader's, is replayed: a new leader may cut
  there). The replay start (D17, `replica_replay_start`, the ACK's `replay`, the leader's replay floor) is the first
  position any open of the member may replay: past the durably saved `kept` only, and on a leader past a kept segment
  only once the commit floor the driver saved covers its end (`lrq`, `leader_replay_floor`): the same directory
  reopened as a replica at that floor replays it (`test_replica` LR and PL, mutants `replayleadnofloor replayopennone
  replayopennofloor`).
- **Snapshots** (R4; `src/snapshot.c`, `server/rsnap.c`, `server/snapinst_run.c`; design-cluster.md 8.1). A leader's
  snapshot is its **crash image at the cut P** (its commit point): what a restart of the leader at P would find, taken
  without stopping writes. The cut reads memory only, under `flush_lock` then `publish_lock`, and lets go of both before it
  touches a file; the files are hard links (a part a merge renamed meanwhile by its `retired_` name; neither: the make
  retries, never CORRUPT), then **the lock barrier**: SERIES, STATES, RETENTION and CLASSES are copied last, each after
  taking its own lock (`append_lock`, `ret_lock`, `class_lock`) alone and reading the file's length, so every record
  whose marker is at or below P is in the copy (its writer held that lock from marker to record) and later ones are
  applied again harmlessly; never nest those locks or take one under an engine lock (no new lock-order edge). A new file
  that records something a marker names must be copied under that rule or be one of the files the cut lists.
  **The install never lowers a promise and never invents one:** the new directory's fence is the old one's raised to P's
  epoch, never lowered; its `led` is never raised; an unsure REPL stays unsure (with the open generation's belt carried);
  FNO is P + 1; the old directory's REPL is read only after its driver has stopped saving it (a FENCE between the read and
  the close would be lost); a snapshot whose P is at or below the log end of the receiver, or whose epoch is below its
  fence, is not installed (stale), checked again after the close; and the swap is `v<N>` -> `v<N>.old`, `v<N>.snap` ->
  `v<N>`, root fsync, remove (`snap_leftovers` decides every crash state; neither release `v<N>.snap` nor open `v<N>`
  before it ran). A rebuilt copy needs twice the volume's space meanwhile: the receiver checks free space first, so a
  rebuild never runs a live volume's WAL into ENOSPC (fail-stop). `src/snapshot.c` is in the embedded bindings' copy of
  `src/*.c` and builds without `TSDB_EC`. Tests `test_snapshot` (windows, merges during the links, random writers,
  power loss at every step), mutants in the Makefile's `test_snapshot` loops (`snapafterp` ... `snapinstnodirsync`, `snapnoreserved`; `snapinstnoppos` ...
  `snapecnoppos` with `--only ec`).
- **Replica set versions** (R4; `server/meta.c`, `src/repl.c`, `server/rset.c`). A volume's `set_version` is raised by
  exactly REPLICA_ADD, REPLICA_REMOVE and REPLICA_RESET and by nothing else; every replication message carries it and a
  receiver drops (and counts) another version's (a FENCE excepted); the driver adopts a new version only on the volume's
  thread, with the core's actions drained (`REPL_E_PENDING`), remapping its per-index state by node AND rid in the same
  step, and hands the core only records of the core's own version (`repl_dropped_meta`); a leader whose change removes a
  member of its record's ISR commits nothing until it adopts the new record. A removed or reset member's directory is
  set aside by rename (`.removed-<rid>`, `.stale-<rid>`, `.lost-<rid>`) with a root fsync before anything relies on it;
  a `v<N>` the record does not name is never opened, and one that is missing while the group recorded an open of it is
  refused, never made again by itself. One move per volume; a move's shrink never takes the ISR below the size it had
  before the new member joined (`repl_leave`, `min_isr`).
- **Settled parts** (R7 single-server, R8-1 cluster EC, R10 single-server re-encode; `src/ecpart.c`, `src/ecswap.c`,
  `src/settle.c`, `src/eccold.c`, `src/ecapply.c`, `src/ppos.c`, `src/ecdigest.c`, `src/ecdiverge.c`, `src/eckeep.c`,
  `src/ecgc.c`, `src/ecgcmk.c`; `docs/design-cluster-data.md` §2.3). On one server (R7, D2) a settle turns
  `p<K>_<A>_<B>_<L>.part` into `.ec` of the same name and block range (no gate, cut, marker or `ppos`); on a cluster
  EC volume (R8-1) the leader settles whole keys at a cut `Z` with `MK_COLD`, `PPOS`, `MK_DIGEST`, `MK_GC`, and durable
  `DIVERGED`. Every path that takes a part out of the list must respect that:
  - **Never a merge input.** An `.ec` part and a `settling` one are barriers in merge selection, rewrites, `tsdb_optimize`
    and the TTL rewrite (`part_merge_barrier`), never a source of `pw_copy_granule`; an `.ec` part is not counted in
    `partition_parts` (a settling part still counts: it is a `.part` until the swap), and in `db_wait_merges` (backpressure) it is a wall, not a pair, so an insert never waits for a
    settle in flight when no pair is left. A drop retires them (a settling part without waiting for the settle). On a
    cluster EC leader (`db->ec_cluster`), `ttl_select_locked` (`src/merge.c`) also skips a key with a settling part
    (mutant `ttlsettling`).
  - **The swap checks its inputs under `db->lock`, in the hold of its renames** (`ec_swap_commit`): every source still
    listed, not dropped, not merging; else `TSDB_ERR_STOPPED`, nothing renamed, the temporaries unlinked and the objects
    handed to r7-gc. On a failed engine the answer is `TSDB_ERR_IO` and nothing is renamed or removed: the next open
    decides (only 0 means the `.ec` parts are listed; after IO a caller must not treat them as never listed). A drop takes no `publish_lock` (`drop_retire` takes `latest_rw` and
    `db->lock`), so a check made before those locks would list an `.ec` for a part the drop retired and bring its rows
    back (mutant `swapnodropcheck`). The swap is **intent v4** (`tsdb-intent 4 n`, n `L tmp final` lines, n `R part
    settled_part` lines, a hash; R10 single-server re-encode `ec_swap_reencode` uses `tsdb-intent 4 1 <nin>` with `R
    <p.ec> <retired_p.ec>` for `.ec` sources and `lt_merge_marks` in its write section), written and synced with its
    directory before any rename. Recovery asks one question:
    did the swap rename anything? Any `.ec` (under its name or `retired_`) or `settled_` name on disk means the check
    passed and the swap stands (a drop after it retires only the `.ec` parts), and a missing source then proves nothing;
    nothing renamed with every tmp and source present rolls forward, with one missing rolls back; damage (a sealed
    intent that breaks the name rules, an `.ec` under no name while its source is still `.part`, a source under
    `retired_` and not `settled_` although the swap renamed, a lookup error that is not ENOENT) fails the open with the intent kept. The directory is synced before the intent is unlinked, as every
    rolled-forward intent now is (`tests/test_intents.c`). On a cluster EC volume (R8) `ecapply_swap_pre` sets
    `*logged` (so a refusal from there on fails the engine and never hands objects to `r7-gc` or the caller: mutant
    `swaploggedgc`), records `PPOS` for every `.ec` name at `Z` (`src/ppos.h`, never after the renames: mutants
    `pposswapnone`, `pposswapafter`), and records leader RSTATE `doing` (`jr_cold_precommit`) before the intent's commit
    point, then `ecapply_swap_post` writes `done` (`jr_cold_commit`) after the renames (`test_eccold swappower`,
    `test_ecapply`, mutant `leaderswapnors`).
  - **`PPOS` (R8, `src/ppos.[ch]`; D2, D3).** Cluster EC volumes (`db->ec_cluster`) keep a per-volume checksummed
    append-only `PPOS` file (32-byte records `{name xxh64, ppos, flags, 0, offset-seeded xxh64}`, torn tail cut at open,
    bad record before a valid one fails open, compacted via `PPOS.tmp` when > half dead). Every listed part (`.ec` too)
    must have a record at open (`TSDB_ERR_CORRUPT` otherwise); `PPOS_UNKNOWN` (`UINT64_MAX`) marks parts of unknown
    position. Appended and `fdatasync`ed before the part name is visible: a flush gets each part's `ppos` from a
    per-memtable map `base_partition -> highest fno` fed under `buf_lock` at all three append sites (`mem_insert`, replay,
    `apply_rows` via `eng_ppos_rows`; fallback `fno_hi` if map lost), `parts_commit` uses `jr_part_pos(db)`, merges take
    the max `ppos_of` over inputs, and settles use `Z`. `tests/test_ppos.c` (crashfs at every step, mutants `pposafter
    pposnosync pposflushhi pposreplay pposunknown0 pposfallback0 pposnofail pposnobadgood pposflags`).
  - **The leader's settle cut (R8, `src/eccold.[ch]`, `src/ecpolicy.c`; D4, D18, D21).** The cluster policy settles
    whole keys only of any class (no per-part, coarse or eager settle) once `phi + grace_s <= now` (by `MK_NOW`), no
    merge is selectable, no row expires within `keep_s + retain_s + 1 h` (`expires_within`), level > 0 only (D18), never
    on a diverged engine, waiting for natural flushes until `grace_s + force_s`, and capped per marker (`MK_FILES_MAX`).
    `eccold_settle`: (1) pin `I` (`settling`); (2) encode & put objects outside every lock (epoch `coldvol_epoch()`,
    `sv0`), waiting for positions only if they dropped during puts (up to `settle_wait_max_s`, never under the gate;
    gives up at once if a position is down before the first put); (3) close gate `lw_enter` (`jr_serial` + fence over
    all `MK_BUCKETS` series buckets of key's time range), `L = log_end`, rotate/flush memtable if overlapping P (`F =
    wal_flushed`), `eng_leader_wait(L)`, `Z = log_end`, re-check stripe; (4) `Y` = listed non-.ec parts of P not in `I`
    (merging -> `STOPPED`); (5) write `.ec` files (`ppos = Z`, `stripe_version = sv0`) and append/wait `MK_COLD` via
    `jr_cold` (crash point `cold-after-marker` right after append; `INDETERMINATE` if deposed: engine runs on); (6)
    `ec_swap_commit` (non-zero after committed marker fails the engine), then `lw_leave`. Degraded settle (R9, E10): after
    `ec.settle_wait_max_s` with exact 1 position DOWN and `m >= 2` (never 2: logs once and waits; wait clock restarts when
    the down mask changes or goes stale), writes `k'+m-1` positions, records `positions`, verifies every written position
    is UP in the current epoch right before `MK_COLD` (`TSDB_ECCOLD_TEST=pause_pre_marker=PATH` test hook), and notifies
    `sreconcile_note_missing` after `lw_leave`. `tests/test_eccold.c` (mutants
    `cutnogate cutnowait ttlsettling cutnorotate fencekeyonly stripewaitfenced pposswapnone pposswapafter jrbarge degtwo degnoupcheck degnopositions`), `test_cluster ecdegraded`, `make degsettle-mutants`.
  - **Startup and return chunk reconciliation** (R9, r9-reconcile; `server/sreconcile.[ch]`, `server/sfill.[ch]`,
    `server/chunksrv.[ch]` `CS_OP_GCPENDING` (op 9) / `CS_OP_RECONCILE` (op 10), `server/chunkpeer_cluster.c`;
    design-cluster-data.md §5.2, E10, E14, E27, E28; D14, D15): on every member start and return (`DOWN -> UP`,
    `OUT -> UP`, or lease-lapse reconnect), hands work off the fill watch thread to `reconcile_main`, checks local
    chunk store ids against `<state-dir>/chunk_stores` (protected by an `XXH64` trailer; read/format/checksum errors
    log loudly and refuse to overwrite; a writable store answering with another store id is logged once with path and
    both ids, refilled completely, and its new id persisted only after affected positions reconcile cleanly; a
    non-writable DOWN or live-vs-disk diverged store is logged once and waits for store recovery/restart without
    re-streaming `OBJLIST`), streams the leader's full `OBJLIST` (`max_version = 0`) page-by-page against local
    `CHUNK_LIST` (re-checking `vol_view` per page), enqueues missing/damaged/replaced-store chunks onto `sfill` at the
    volume's margin priority (`sfill_enqueue_obj` with a cancellation predicate when a position moves away, progress
    merged into `HBS_FILL` at the current `stripe_version`), checks extra local chunks against `CS_OP_GCPENDING`
    (wire-flagged truncation; leaving committed `MK_GC` oids to r8k and reporting confirmed orphans without deleting),
    and drains `sreconcile_note_missing` off the watch thread via `CS_OP_RECONCILE`. Like r9-fill's `OBJLIST`/`REOPEN`,
    chunk ops 9 and 10 are added without a `PL_CL_VERSION` bump. `tests/test_sreconcile.c`,
    `test_cluster ecback ecbackempty ecbackgc`, `make reconcile-mutants` (`backfullcopy backskipmissing`).
  - **Returning server during rebuild** (R9, r9-return; `server/place.[ch]` `place_return`, `server/cluster.c`,
    `server/cmove.c` `cmove_cancel_auto`, `server/repld.[ch]` `repld_log_bounds`, `server/cvol.c`,
    `server/chunkpeer.c`, `server/chunkpeer_cluster.c`; design-cluster-data.md E28, §5.4; D12, D13, D16):
    `HBS_VOLUMES` carries trailing per-volume `(log_start, log_end)` (backward-compatible). The cluster thread tracks
    OUT/DOWN evictions across all voters (`apply_entry` fast-pathed via `meta_entry_has_op_mask` for `STRIPE_SET` /
    `REPLICA_ADD`). When an evicted member returns UP with intact chunk store ids, `place_return` proposes
    `STRIPE_SET` back to it (firing `CP_RETURN_SET`, `"return-set"`) for any position whose replacement is still
    `META_STRIPE_FILLING` (never after `WHOLE` or a `whole == 1` fill report, and never breaking E9 rack survivability);
    reads during return fall back from partial `prev` to the active holder before reporting `CHUNK_ST_ABSENT`, and
    partial fills on the displaced replacement are released only via committed `MK_GC_RELEASE` once the returned holder
    reaches `WHOLE`. For an unjoined log move whose returning replica is UP, healthy, and covered by retained segments
    (`log_start <= 1 || log_end >= log_start`), `place_return` emits `PLACE_PROP_MOVE_CANCEL` (`cmove_cancel_auto`).
    `tests/test_place.c` (`--mutant takebackwhole`), `tests/sim_place.c` / `tests/sim_place.sh` (`takebackwhole
    partialdelnow`), `test_cluster ecbackduring ecbackafterwhole logbackduring`, and `make return-mutants`
    (`takebackwhole partialdelnow`).
  - **Cluster balancer** (R9, r9-balance; `server/balance.c`, `server/place.[ch]`, `server/cluster.[ch]`,
    `server/meta.[ch]`, `server/sfill.[ch]`, `server/rsnap.c`, `server/chunkpeer_cluster.c`, `tools/cluster-cli.c`;
    design-cluster.md §3.2, design-cluster-data.md §6.2–6.3, E9, E51, D18–D20):
    `balance_plan()` / `balance_plan_ex()` / `balance_plan_summary()` are pure greedy planners (no I/O, no clock,
    no locks) over log replica lists (even replicas across compute-capable UP members, preferred-leader `rep[0]`
    preference, pair spread, E9 rack survivability never broken, R13 fast-log/role/fill respected), EC stripe
    positions (chunk byte utilization per capacity when >=5% imbalanced and capacity-weighted position counts within
    the same storage disk class, never exceeding the fill mark), and leaders via R4 planned transfers (when
    `leader_balance_s == 0`, leaving R4's `ctransfer.c` untouched when `> 0`). Bounds: <=5% of positions moving at
    once (min 1, per group and cluster-wide), automatic plans moving >25% of cluster positions stop and log
    (override via `CLUSTER_OP_BALANCE = 22` / `tsdb cluster balance [--apply]`, cleared on term change or new
    rebuild), never runs while any rebuild/drain/OUT/DOWN/fault/conversion is active, and never lowers any margin
    (add-before-drop for log moves, `FILLING -> WHOLE` + 1x copy from UP `prev` before `MK_GC` release for stripe
    moves). Controlled by live-changeable `balance` setting (`on|off`, default `on`, `META_SNAP_VERSION = 12` /
    `"TME9"`); balancer transfers pace under `PACE_MARGIN_BALANCE` (`balance_mb_s`, yielding whenever any rebuild is
    active). `tests/test_place.c` (`--mutant balanceunbounded`), `tests/sim_place.c` / `tests/sim_place.sh`
    (`balanceunbounded`), `tests/test_sfill.c` (balance yields to rebuild without worker-pool deadlock),
    `test_cluster posbalance`, and `make balance-mutants` (`TSDB_PLACE_MUTANT=balanceunbounded`,
    `TSDB_SFILL_MUTANT=balancedecode`).
  - **A replica applies a committed MK_COLD** (R8, r8h; `src/ecapply.c`, design-cluster-data.md 2.3 step 6): it checks the
    marker (every object record names its oid, the key and the marker's Z; one `.ec` file per object; each Y's header
    check), stages the files, numbers the `.ec` parts in the leader's block order with Y above them (never the marker's
    order), records PPOS for every new name at Z (Y too, which design 2.3 lets keep the leader's ppos: at Z is
    conservative for the `<= Z` rule) and RSTATE `doing` before the intent, then under `publish_lock`, `latest_rw` and
    `db->lock` checks its own parts of the key (any with a ppos above Z, or with no PPOS record, which is unknown: nothing
    retired, durable `DIVERGED`; none listed: the key was dropped, nothing listed) and writes **intent v5** (`N tmp final` per
    `.ec`, `Y tmp final`, `R part settled_part` per own part, a hash), fdatasyncs it and the directory, renames N, Y, R and
    syncs the directory. A merge of its own parts in flight finds them `dropped` and drops its output. Recovery
    (`ecapply_recover`, first pass of `intents_recover`): any new name on disk means the apply stands; nothing renamed
    with every tmp and source there rolls forward; a tmp gone rolls back (the replay applies again); an own part gone
    rolls back DIVERGED; the directory is synced before the intent is unlinked. A v5 intent torn into its head line is
    reported by the generic path with insert wording. `ecapply_cold_decide` (RSTATE `doing` at a replay, on a leader too:
    its swap records the COLD): every oid listed is done, none is not, some or one twice is damage. Crash points
    `cold-before-apply`, `cold-mid-intent` (after the `.ec` renames). `tests/test_ecapply.c` (power loss at every step
    of the apply and of the recovering open, a tampered marker, several objects, a second COLD of a key; 17 mutants in the
    Makefile: `recnodirsync v5nodirsync coldnoflushthrough ycold ecabovey pposignored leaderswapnors pposafterswap
    mergekept notfoundzero noorder checkz seenskip noyhcheck decidesome recrollmissing recnodiverge`).
  - **The settle reads each byte once.** `eng_settle` checks the granule crcs, the footer hash and the class judgement on
    the stripe copy it then encodes (`chunk_be_put_stream`): a check on a second read could pass on bytes that differ
    from the encoded ones (mutant `settletwopass`). A CORRUPT check marks the part damaged and writes no chunk.
  - **The read seam always checks**, with `verify_checksums` off too (above): an `.ec` granule is never decoded from
    bytes nobody checked, so a damaged chunk is an erasure and never wrong values (mutants `seamnoverify`, `cachekeyoff`).
    `src/eccache.c` also runs sequential granule prefetch (R8 D19, `r8p-prefetch`): a miss continuing a walk in 16
    per-cache stream slots reads `N` = 4..32 granules in one RAW range read (bounded by `budget/4` and 4 MiB, pinned
    entries never evicted, falling back to single demand reads on range failure; `test_seam` mutants `prefetchnocheck
    prefetchevictpinned prefetchnodouble prefetchdupkey prefetchforgetlive prefetchnofallback`).
  - **The source stays for `keep_s` from the record's `settle_ms`**, and for as long as its digest has not matched.
    On one server (and for a cluster leader's own `settled_` files), `src/eckeep.c` runs R7's byte digest; in a cluster
    (R8, `src/ecdigest.[ch]`, `src/eckeep.c`; E26, D6, D7, D25), the leader computes `ec_logical_digest` of `I ∪ Y`
    (rows per series in `(series, ts)` order visible under the retention cutoff at `now_ms` without clamping, plus
    canonical per-series tombstone union clipped to `[max(cutoff, key start), key end)` when `MKD_TOMBS` is set under
    `ret_lock`) and appends `MK_DIGEST` (`now_ms`, `MKD_TOMBS`, oids, digest). A replica's COLD apply binds its retired
    `settled_` files in an unarmed `digest_<key>_<z>` job file, `ecdigest_apply` arms it (`ECDJ_ARMED`, superseding
    earlier unarmed jobs of the key below `z` as `ECDJ_SUPERSEDED`), and `eckeep`'s thread verifies granule checksums
    and digests: a match writes `ECDJ_MATCHED` before release at `settle_keep_s` (unlink + `eng_sync_dir`, job file
    removed after its last copy). For **D25 rot** (`ECKEEP_ROT`: logged, `eckeep_copies_lost++`, `.ec` served, released
    at deadline), leader/single-server byte-digest checks (`digest_part`) trigger it when the chunk object matches its
    record while the kept `settled_` copy fails its checksums, whereas replica `MK_DIGEST` jobs (`ecdigest_job_check`)
    verify the retired copies' checksums against `MK_DIGEST` without re-reading the chunk object (failing checksums or a
    missing copy without `ECDJ_MATCHED` -> `ECKEEP_ROT`). A digest mismatch (or unreadable rows) with intact checksums
    triggers durable `DIVERGED` (`ec_diverge`, `src/ecdiverge.[ch]`): `DIVERGED` file is synced before `db->diverged` is
    set, read at every open so restarts keep refusing the volume (`TSDB_ERR_UNAVAILABLE`, `LOCAL_FAULT` in cluster
    heartbeats, `tsdb_volume_diverged` = 1), until `tsdb DIR volume diverged --clear` (refused on cluster volumes, D23;
    use `tsdb cluster replica reset` to rebuild the replica instead). On a single server (`src/ecunsettle.[ch]`, R15
    `r15-reverse`, D14), `volume diverged --clear` calls `ec_unsettle_diverged_cli` -> `eng_unsettle(db, key)` to restore
    every `.ec` part `P` of each diverged key to its exact local `P.part` (preserving block range and level; Source 1:
    verified `settled_P.part`; Source 2: decoded from the tier with cell-sum, `object_xxh64` byte digest, and full part
    granule/footer verification into `tmp_*.part`) and retire `P.ec -> retired_P.ec` through **intent v5** reverse lines
    (`L <settled_P.part|tmp_*.part> <P.part>`, `U <P.ec> <retired_P.ec>`, `ec_swap_unsettle` / `unsettle_recover` in
    `src/ecswap.c`), clearing durable `DIVERGED` only after every unsettle swap is durable. `ec_swap_unsettle` serializes
    with `tsdb_drop_before` via `db_pub_begin`/`db_pub_end` (and `ttl_select_locked` in `src/merge.c` skips keys with a
    `settling` `.ec` part), `drop_name_recover` in `src/part.c` maps a drop intent naming an
    unsettled `P.ec` onto `P.part`, `eckeep_unsettle_locked` clears keep/graveyard state on both fast and decode paths,
    and both `ec_swap_commit` and `eng_unsettle` refuse a part whose `retired_<P>.ec` is still awaiting GC.
    `tests/test_ecunsettle.c` (crashfs power-loss sweeps on both sources, `make ecunsettle-mutants`:
    `unsettlenodecodecheck`, `unsettlerank`), `tests/test_ecdigest.c` (mutants `digestbytes digestnotomb
    digestnoretention digestbelowtrue digestnots digestfreezerows digestlocalbelow`), `test_ecapply --case digest|digestpower`
    (mutants `digestnocompare digestanyz digestnosupersede digestnocrcwalk digestrotmatches digestnojobremove
    digestjobnosync`), `test_keep` (mutants `divergemem clearcluster releasenosync rotdiverges`).
  - **Chunks are deleted at the last reference.** A retired `.ec` keeps its `retired_` name until every chunk of its
    object is gone, and nothing is deleted while any `.ec` part of the object is open (`part_open_ec` -> `ecgc_hold`,
    `part_unref` -> `ecgc_last_unref`, O(1) holds in the tier keyed by part pointer and `(volume, oid)`). On a single
    server the open sweeps orphans (`ecgc_open_sweep`, D6) before any engine thread starts; on a cluster EC volume both
    R7's purge and open sweep are skipped and deletes go solely through committed `MK_GC` (`src/ecgcmk.c`, E14, D13).
    Never put chunks in a single-server store before the sweep without `ecgc_test_nosweep`. No engine lock is held
    across an encode, a put or a chunk delete.
  - **Refused where unbuilt**: parts without granules and v8 parts are never settled (a settle must also never take a
    part `rewrite_alone` would still take, `src/ecskip.h`: single-server `settleable` checks it only through
    `merge_selectable`, while the cluster branch of `ecpolicy_pick_locked` (`db->ec_cluster`) and `eng_settle` explicitly
    require `p->level > 0`, D18); a non-leader replica, a
    follower or read-only open (R15 D13: followers of a `TIERS` database open a read-only tier and read `.ec` parts
    through their own `eccache`; a follower without a tier that meets an `.ec` name still stops with one line per D7),
    a build without `TSDB_EC` and a
    tier with a store that is not UP settle nothing. `TSDB_TEST_TIER` needs `TSDB_TEST_TIER_I_KNOW=1` and is test-only
    for single servers until R15; cluster EC volumes require no test variable and attach `coldops_peer` via
    `chunkpeer_cluster_prepare` (`requires=cluster,tier,ec` in META). From `requires=tier` on, every open with no tier
    configured (including a read-only or follower open without `TIERS`) and every build without
    `TSDB_EC` fails. The embedded bindings copy `src/*.c` and build without `TSDB_EC`: a volume whose META says
    `requires=tier` is then refused at open.
  - **Late rows** go to young parts above the object and settle again as further objects of the key (E6); `eng_reencode` /
    `ecpolicy_pick_reencode` (R10) combines them (`> ecpolicy_objects_max` or `> 10%` tombstones) on both single-server
    tiers and cluster EC leaders (`r10-crepl`, D24). On a cluster EC leader (`eccold_reencode`), re-encode enforces the
    suffix LWW invariant (no `.ec` above `I`, no young part below `I`), merges/verifies pre-put, puts to the stripe group,
    gates and cuts at `Z`, appends `MK_COLD` naming both the new object (or empty `.part` as `Y`) and the retired oids
    (`nretired`/`retired`), swaps via `ec_swap_reencode` with `ecapply_swap_pre/post` and `need_digest` GC gating, verifies
    the post-swap read-back logical digest (`ecdigest_leader_reencode`), commits `MK_DIGEST`, and only then releases retired
    objects to `MK_GC` (`need_applied = digest_pos`) and young sources to `eckeep`. Replicas apply `MK_COLD` in `ecapply.c`
    (retiring named `.ec` parts as `retired_*.ec` and own non-`.ec` parts `<= Z` as `settled_*` under intent v5, diverging
    on parts `> Z`, with `ecapply_cold_decide_ex` handling empty-output recovery) and verify `MK_DIGEST` synchronously on
    jobs with `retired_*.ec` before advancing `applied` (with bounded transient-`UNAVAILABLE` retry, never acknowledging on
    read failure or mismatch). Test hooks `TSDB_REENCODE_HOLD`, `TSDB_ECCOLD_TEST=pause_before_digest=<path>`,
    `TSDB_ECCOLD_MUTANT=retiredunnamed`, `TSDB_ECGC_MUTANT=gcbeforedigest`; tests `test_ecapply --case reencode`,
    `test_cluster reencode` (in `CLUSTER_SCEN_3`), and `make reencode-mutants` (`retiredunnamed`, `gcbeforedigest`).
    Cluster unsettle (`r10-unsettle`, `MK_UNSETTLE = 14`, §6.4, D14, D27) converts settled keys back to plain `.part`
    files on all log replicas: `eng_ec_keys` lists settled keys, `eng_unsettle_key` prepares local `.part` files unlocked
    (Source 1 `settled_` or Source 2 cluster chunk decode with sum/digest/granule checks), takes `jr_serial`, arms
    `db->unsettle_hold_pos` before appending `MK_UNSETTLE{key, oids[]}`, waits for commit, and swaps via
    `ec_swap_unsettle_at(..., pos)` (recording PPOS durably before intent v5, clearing `unsettle_hold_pos` under
    `publish_lock` after post-rename dir fsync, and cleaning up stale `digest_<key>_<z>` jobs via
    `eckeep_unsettle_cleanup_jobs`). While `unsettle_hold_pos > 0` (including after an `INDETERMINATE` depose when
    `pos != 0`), `REPL_A_SAVE` clamps `REPL.floor` and `keep_floor` below it regardless of `rv->mode`, and `snap_make`
    (loading `lead_commit` before checking the hold under `publish_lock`) returns `SNAP_RETRY`. Replicas apply
    `MK_UNSETTLE` in `ec_unsettle_apply` (dropping `rp->mu` across decode with bounded transient-`UNAVAILABLE` retry,
    swapping durably before acknowledging). Every peer's `REPL_M_ACK` reports `r->applied` in its `commit` field
    (`repl_applied_floor` tracks the minimum across all log replicas, resetting on `rid` change or `repl_reconfig`),
    and `ecgc` gates cluster `MK_GC` of unsettled objects (`need_applied`, plus `ecgc_eng_ready` failover gating for
    `loaded_from_disk` retired `.ec` objects not referenced by a live `.ec`) until every log replica has applied
    `MK_UNSETTLE`. Progress counters export via `ecunsettle_get_progress`. Crash points `unsettle-mid-apply` and
    `unsettle-after-marker`; test hooks `TSDB_ECUNSETTLE_TEST=trigger=<path>[,after_marker_ms=<N>]` and
    `TSDB_ECUNSETTLE_MUTANT`; tests `test_ecapply --case unsettle`, `test_repl` (`applied_floor`), `test_cluster unsettle`
    (in `CLUSTER_SCEN_3`), and `make unsettle-mutants` (`unsettleleaderonly`, `unsettlenogcgate`, `unsettlenofloorclamp`,
    `unsettlesnapnohold`). Answers do not change (`tests/test_settle_oracle.c`).
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
  `wal_removed_synced` passes its segment; on a replica until that or its replay start, RSTATE `kept` as saved, the
  start its ACK sends; an entry is sorted only when a fold past the cap or a switch needs it, under `flush_lock`), so a replay maps each WAL row to the key its flush used: a deleted row
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
  On a replicated volume (design-cluster 5.4) a replica takes the leader's T exactly, so the leader's T lies past every
  row any member may still replay (its WAL tails are pruned by the members' replay floor, never by its own removed
  segments, and a replica checks that T against its tails up to its replay start, never its removals alone, and a switch waits while that floor is unknown or below its open), and the CLASS marker precedes the
  CLASSES record inside the switch.
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
  On a `--cluster` server (design-auth.md §12, A30-A33) the users are the metadata group's: auth builds its snapshot
  from the users watch (users.c's group store, no file), a root holding USERS is refused at start naming
  `tsdb cluster user import`, logins answer UNAVAILABLE until `cluster_users_caught_up()`, the admin ops propose
  through `cluster_user_propose` and answer once committed and back in the local copy (a failed proposal changes
  nothing); one the copy finds a no-op or refuses is answered only after a read barrier (`cluster_user_sync`: the
  leader's commit index under its lease, this copy's users through it; a no-op also waits for the members) and a check
  again, never from a lagging copy; `users_push` keeps the last list while a promoted member's copy is rebuilt
  (`core_boot`), so logins and sessions there go on; and `tsdb DIR user` refuses a root marked `CLUSTER_USERS`.
  `test_cluster login|userimport|volrefuse|userstale|userpromote|userreplay|userlease`, `make auth-mutants`.
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
  part writer or a merge: the functions that change a part list (`db_parts_insert_locked`,
  `db_parts_remove_locked`, and R7's swap `ec_swap_commit` in `src/ecswap.c`, which replaces a part by its `.ec` in place)
  push one event into a bounded ring under the `db->lock` they hold, and a nice-19
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
  `latest_rw` is held across no I/O but `tsdb_drop_before`'s renames of the partitions it retires
  (`drop_retire_prepared_locked`, in its write section; `db->lock` is held across them too, while `drop_intent_prepare`'s
  write, `fdatasync`, and pre-rename directory `fsync` run outside both locks except on the retry-cap fallback), the settle swap's 2n renames (`ec_swap_commit`,
  `src/ecswap.c`: its input check and renames are one step against a drop, which takes no `publish_lock`; on NFS,
  snapshots and lookups of the volume wait for those round trips) and a replica's cold apply (`src/ecapply.c`: its
  check of its own parts, the v5 intent's write and fdatasync, two directory fsyncs and n + nown renames, one step
  against a drop or merge; reads of a replica volume wait for them once per COLD), and never across a lookup's reads of granules. Followers have no table.
- **Lock order:** (a leader of a replicated volume, `db->jr`: `jr_serial` first, held by a large write from `lw_enter` to `lw_leave` and by `eng_drop_before` across its marker and retire, so marker order is effect order; it is a ticket lock (`jr_serial_lock`, taken in arrival order: a plain mutex let a thread writing back to back starve the settle's gate and the other large writers for tens of seconds, `test_eccold ticket|turns`, mutant `jrbarge`; `lw_leave` lets go of it by `lw_enter`'s decision, `lw_fence.serial`, never by `db->jr` again, which `eng_leader_attach` sets while the engine runs: a turn never let go of, or let go of twice, stops every later large write, drop and settle of the volume, `test_eccold jrpair`; TSan's lock-order checks do not see it); that drop also holds `flush_lock` under `jr_serial` across its marker, flush and retire, so no flush lists rows written after the marker before the retire; a replica's `apply_mu` (one applier) is outermost of the replica's own locks, then `stage_mu`, then its `mu`, which the applier lets go of across a COMMIT's flush and staging and a CLASS marker's purge, and checks `cut_gen` after) `flush_lock` > `cls_rw` > `class_lock` > the WAL writer role > `publish_lock` >
  `manifest_lock` > `db->lock` > `buf_lock` > `dir_sync_lock` > `fail_lock`;
  R8 additions: a leader's settle (`src/eccold.c`, `src/settle.h`, `src/settle.c`) takes `jr_serial` (from `lw_enter`)
  > `flush_lock` > `cls_rw` (when rotating a memtable) > `lead_mu` (`eng_leader_wait`) > the WAL writer role and
  `buf_lock` (`jr_cold`) > `publish_lock` > `ppos_mu` / `latest_rw` > `db->lock` (the stripe check and bounded wait run
  before `lw_enter`, never under the fence; no engine lock is held across chunk puts); a leader's unsettle
  (`src/ecunsettle.c` `eng_unsettle_key`) decodes unlocked and then takes `jr_serial` > `db->lock` > `buf_lock` (briefly
  to arm `unsettle_hold_pos`), then `jr_markers_append`/`jr_markers_wait` and `ec_swap_unsettle_at` (`publish_lock` >
  `ppos_mu` / `latest_rw` > `manifest_lock` > `db->lock`); `PPOS` (`src/ppos.h`) takes
  `publish_lock` > `ppos_mu` (its append, `fdatasync` and creation `eng_sync_dir`) > `ppos` map mutex (a leaf, never
  held across I/O; `ppos_of` takes only it), and `ppos_mu` is never taken under `db->lock`, `latest_rw` or `buf_lock`
  (records go out before those are taken); `DIVERGED` (`src/ecdiverge.h`) takes its mutex > `dir_sync_lock` > `fail_lock`
  under no engine lock; `GCMK` (`src/ecgcmk.c`) takes `apply_mu` > `rp->mu` (or leader GC worker `wmu`) > `oid_mu`
  (`ecgc_oid_lock`, 64-stripe mutex array on `ecgc_tier` serializing `ecgcmk` `delete_cluster_object` against
  `cp_repair_check_and_put`'s `ecgc_oid_is_live`; `cp_repair_check_and_put` releases `oid_mu` and its engine ref before
  `net_put`, never holding either across network I/O) > `db->lock` / `g->fmu` > `g->mu` (leaf), and `latest_rw` >
  `db->lock` > `ecgc_tier.mu` (a leaf: never held across I/O or another lock).
  `cls_mu` (the class history map)
  is a leaf, and so are the granule cache's mutex (`src/eccache.c`: never held across I/O, an open or close, a decode or
  another lock: a fill waits on its entry, not on the mutex), the digest's (`src/eckeep.c`), the tier's gc mutex (`src/ecgc.c`: the purge takes it under `db->lock`; never held across I/O), the tier's registry of
  cold volumes (`t->cmu`, `src/tier.c`: only around the array and `eng_active`/`eng_cv` drain; the last put, which frees a context, and any store I/O stay outside it), the chunk backend's `be->st_mu` (`src/chunkstore.c`: pure leaf under `be->mu`, also taken briefly from `store_fail` and `chunk_be_set_ioerr_cb`/`ioerr_cv` drain), the tier health mutex `th->mu` (`src/tier_health.c`: pure leaf, never held across I/O or engine locks),
  and the statistics' `es->mu` (`stats.c`: never held across I/O or an allocation of a part; the
  sweeper takes `db->lock` only with it released). R7's settle: the policy's `ps->pmu` (`src/ecpolicy.c`: its fail list and eager count, taken under
  `db->lock` while the settle thread picks) is a leaf; the tier's settle mutex `st->mu` (`src/settle.c`) is never held
  across an engine call (the thread lets go of it around a settle), and `eng_halt`/`eng_close` wait on its cv with no
  engine lock held; no engine lock is held across a single-server settle's encode or put, a chunk delete, a cache fill or a digest read (they
  run on the settle thread, the gc worker, the digest thread or the reader's own thread, with the part pinned by a reference; a cluster leader's settle holds `jr_serial` only from the gate `lw_enter` through the swap).
  A leader's snapshot (`src/snapshot.c`) adds no edge: its cut takes `flush_lock` > `publish_lock` (and inside them
  `db->lock`, `buf_lock`, `lead_mu` briefly), and after releasing them its barrier takes each dictionary's `append_lock`,
  `ret_lock` and `class_lock` one at a time, alone, under no other engine lock.
  The latest table (design-latest.md section 10): `publish_lock` > `latest_rw` > `manifest_lock` > `db->lock` >
  `buf_lock` (a publisher's write section); `ret_lock` > `latest_rw` > `db->lock` (a limit change); `tsdb_drop_before` and a merge
  that lists across a limit change take `latest_rw` (write) > `db->lock` (a merge lets go of `db->lock` to take it); lookups and
  the warmer take `latest_rw` (read) > `db->lock`, a lookup then `buf_lock` in sub-holds. The window's `pmu`/`pcv` (a writer
  waits there with no other lock, `lt_end` takes `pmu` under `latest_rw`) and the warmer's hint mutex are leaves. A leader's
  `lead_mu` (its commit point, `eng_leader_wait`; src/memtable.c) is a leaf: `eng_fail` takes it last to wake the
  waiters; a wait for the commit point may hold `flush_lock`, `cls_rw` or `jr_serial`, because the commit point moves on
  positions already appended and needs no engine lock. A driver's `jr_ops.admit` runs under `buf_lock`, so `buf_lock` >
  the driver's core mutex > `lead_mu`, `fail_lock` (under its mutex the driver may call `eng_leader_commit`,
  `eng_leader_depose`, `eng_fail`, which take only those; `eng_leader_depose` takes no `buf_lock`: admit may call it).
  A small write waiting for a large write's publish (the fence's cv, on `buf_lock`) holds no other lock and has
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
  tsdbd's login (`server/auth.c`): on a cluster server the cluster thread's `uw_mu` (`server/cluster.c`, held across
  the users watch's call) comes first, and `cluster_user_propose` is never called under `users_mu` or `auth_mu`; then
  `users_mu` (the USERS state; held across USERS's I/O and its OFD lock, so on NFS
  as long as a server is away) > `auth_mu` (the published snapshot of the users and the registry of logged-in
  users; never held across I/O) > `tn_mu` (`server/tenants.c`: the tenant list, its slots and the pending list; auth_mu takes
  it to find a user's tenant (`tn_slot_user`) and a failed login to count under one, and USERS's publish calls `tn_offer`
  with `users_mu` held; a leaf, never held across I/O or an engine call: the tick thread lets go of it around
  `tsdb_set_tenants`) and `blk_mu` (the registry of the counter blocks, a leaf that a counting thread never takes). Logins and requests take only `auth_mu`, never `users_mu`, and no engine
  lock is taken under either; a user's rights index is built before it is shared and read without a lock.
- **Directory scans** use `fdopendir(dup(dirfd))` followed by `rewinddir`: the
  dup shares its offset with `dirfd`.
- **Versions.** The release version is the file `VERSION` (0.11.0); `tools/check-version.sh` compares it with
  `include/tsdb.h`, the binaries and the manifests of the clients and the connector. A release is cut by
  `docs/release.md`; what 0.9.x and 1.0 promise is `docs/compatibility.md`; the notes are `CHANGELOG.md`.
  `make dist` names the tarball `tsdb-VERSION-x86_64-linux.tar.gz`.
- The on-disk format is version 10 (`FORMAT_VERSION`; 10: columnar WAL records (`WAL_KIND_COL` = 2, a write of 256
  rows or more as zstd-compressed columns, `src/memtable.c` `wal_col_*`) and the `F64_FOR` flag (0x40) in
  `BLK_F64_DECIMAL`'s scale byte (an `int_for` integer stream), nothing else; a format-9 database (0.10.x) is refused at
  open and by `tsdb DIR upgrade` with a line naming 9, 10, export with 0.10.x and import here
  (`EXPORT_ONLY_FORMAT_VERSION`; `test_damage` `test_format_version`): there is no 9 -> 10 path in place; parts this
  release writes say 10 and a format-10 database never holds a part that says 9; 9: the WAL record has a 32-byte
  header with `fno` and `commit_ms` and may be a marker, the part header is 48 bytes with `fno_lo`, `fno_hi` and
  `written_ms`, every volume has an `FNO` file, META may say `requires=` (an open refuses a feature it does not know,
  naming it) and `parts_v8=1`; a format-8 database is carried over by `tsdb DIR upgrade`, below; 6: a 7-bit codec kind in block headers; 7: partition keys with
  a class, CLASSES, zstd frames without magic and content size, the packed granule index and a 64-byte footer
  with `index_len`; 8: no CATALOG, the volume number in each volume's META, 64-bit series ids): granule entries
  with typed, time-weighted and spread statistics, bit-packed per part (about 32 bytes each), a checksum over each
  part's header, index and footer, checksummed WAL, SERIES/STATES, VOLUMES and intent records. Until
  1.0 (the format freezes there; decided on 2026-10-25) older formats need not stay readable: a
  layout change bumps `FORMAT_VERSION` and goes in the changelog with a way to carry data over, and a database
  of another version is refused at open with a message that names both versions. Value blocks name their codec, so new
  codecs need no version bump; a new flag or variant inside an existing block kind, or a new WAL record kind, does (an
  older binary must refuse the database at open, not call the block damage or cut the record as a torn tail: format 10).
  Tests use `FORMAT_VERSION` rather than a literal where C allows it, so a bump touches few of them.
- **`tsdb DIR upgrade`** (`tsdb_upgrade`, `up_*` at the end of `src/volumes.c`) carries a format-8 database over in place,
  straight to the current format (10; `PREV_FORMAT_VERSION` stays 8, and a format-9 database is refused: export, import).
  It refuses, before writing anything, on a `wal_*` segment, an intent, a `tmp_*` part, a held writer lock or a
  damaged MANIFEST (not on `retired_*`: those are normal after a clean close); holds every directory's `LOCK` and the
  VOLUMES OFD lock for the whole run; per volume makes `FNO` (synced with its directory), rewrites MANIFEST as 10 (tmp,
  fsync, rename) and writes META (version 10, `parts_v8=1`) last; volumes first, the root last; safe to run again.
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
  be safe (no UB, no overread) on any bytes. Wrong timestamps are refused all the same, with or without the
  checksum: `granule_decode` fails a granule (`TSDB_ERR_CORRUPT`) whose decoded timestamps do not strictly increase
  within its entry's [ts_min, ts_max], because every reader takes that as given: `src_fill` clips a query's range by
  the entry's ends, and the bucketers index their arrays by each row's bucket (`fold_into` wrote outside them;
  `test_damage` `test_ts_damage`, `test_ts_verified`). The order is checked inside the timestamp decoders
  (`block_decode_ts_sorted`: one overflow test per DoubleDelta run of unchanged deltas, a compare per other row while
  it is in a register; `test_tsdb` `test_ts_sorted`, `fuzz_codec` against the plain decoders), the window at the two
  ends; `GW_LOC` loads them. A new timestamp transform needs its `_sorted` form. With `fsync = 0` a power loss looks
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
  connect to a list of nodes; tsdbd sends UNAVAILABLE and MOVED (PARTIAL and DROPPED are still unused).
  14 QUERY_LATEST (39). 15 statistics (STATS_SERIES 46, STATS_TENANTS 47; docs/design-stats.md 5; `server/tenants.[ch]`:
  tsdbd's tenant list and per-caller counters; floor still 12). The change feed is planned as 16.
  All four clients speak 15 and no call is gated on a version below the floor;
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
- **A test never retries and never excuses a failure by the load of the machine** (the `tests/busy.h` mechanism is gone).
  A correctness check and a timing check both fail when they fail; a bound on a time is what tsdb documents or promises
  (the 60 s flapping rule, `--send-timeout`, a client's retry budget), a state that is only promised to come is a
  condition wait with a generous bound, never a tuned number. The watchdog (`tests/watchdog.h`) has a fixed deadline
  (`TH_WATCHDOG_SCALE` gives a CI job more). A red run on an overloaded machine is investigated; CI runs the timing-heavy
  groups with fewer jobs at once. The jepsen nemesis is judged (`nemesis_check` in
  `tests/jepsen/jepsen.c`, right after the run, before the final state and the history check) from the faults it issued, each
  with its time (a kill, an EIO the shim logged in its mark file, a plug the shim logged in its pull file, a pause...):
  a floor from the time a server was listening (at least a third of an honest rate, never below 3), one in each third of
  the run, and from a watcher thread that polls the server pid every 50 ms on its own clock: no server dead for more than
  250 ms, the final stretch up to the end of the run included; and no time idle inside its fault step. A failed
  check of that kind is INCONCLUSIVE (exit 3), never a violation: a machine that stalls a thread makes an honest run fail
  it once in a few dozen, so `tests/jepsen/rerun.sh` repeats the run with the next seed (twice at most, three in a row
  fail the gate) and no excuse is taken off a bound for time a thread waited for a CPU (a spinning nemesis and a starved
  one look alike to the kernel); only a wrong answer of tsdb is a violation, and a violation is never repeated.
  `make jepsen-selftest` plants the idle nemeses (once, a burst and then asleep, naps that do not look, steps and waits that
  sleep or spin, a nemesis that never restarts the server, a shim that ignores SIGUSR2, clients that write nothing) and
  requires each to end in the three-INCONCLUSIVE gate failure with its check named, the honest odd ones (a late last pull,
  a slow mark) to end valid, and a hung server, or data lost beside a lazy nemesis (`tests/jepsen/mutant-loss.sh`), to be a
  violation at once.
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
| `build/tsdbc-bench` | the server: loading, queries, 10,000 connections; `via --via ADDR --volume NAME`: the cost of the forwarding hop (direct to the leader against through another member, repeated; `test_cluster viabench`) |
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
