# Task: tune tsdb for an Intel Core i9-10850K (Comet Lake, AVX2)

A task to work on the 10850K machine on its own, from a fresh checkout. Read [AGENTS.md](../../AGENTS.md)
first: its rules ("CPU levels" above all) hold here too.

## Why this machine

tsdb is developed and tuned on AMD Zen 4 (a Ryzen 9 7950X). The 10850K is an Intel Comet Lake: a
Skylake-family core with AVX2 and BMI2 but no AVX-512, so tsdb runs its **v3** level there (`TSDB_CPU=v3`;
the portable build picks it at startup). That is the level of most Intel servers from Haswell to Comet Lake
and of many cloud VMs. Questions:

1. How fast is tsdb at v2 and v3 on an Intel core, against the same runs on Zen 4 forced to those levels?
2. Are the per-CPU thresholds right for Intel? Where the short-run kernels hand over to the wide ones
   (`AVX2_SHORT` and the per-kernel-set thresholds in `src/simd.c`, `simd_short`, the LTTB and integer
   thresholds) were measured on Zen 4 only. If Intel wants other values, the dispatch can choose them per CPU
   vendor or model at startup (`src/simd.c` `simd_select`, `src/cpu.c`), as long as every choice gives the same
   bits.
3. Does BMI2 (PDEP/PEXT, fast on Intel) unpack bit-packed integers faster than today's v3 code in the decimal
   and integer codecs (`src/codec.c`, `src/codec_typed.c`)? (PDEP/PEXT are slow on AMD before Zen 3, so a PDEP
   path must be chosen by CPU, not by the instruction being present.)
4. The float64 decimal encode and decode paths that need AVX-512 today run at v2 speed on v3 CPUs. The codec
   work of 2026-10-26 did not add AVX2 versions (no AVX2-only CPU to tune them on): they are step 4 below.

## Set up the machine

1. Tools: gcc 14 or newer, make, the libzstd and liburing development packages, python3, rsync.
   Ubuntu: `sudo apt install build-essential libzstd-dev liburing-dev python3 rsync`.
   AlmaLinux: `sudo dnf install gcc make libzstd-devel liburing-devel python3 rsync`.
2. CPU governor on performance for every timing (`sudo cpupower frequency-set -g performance`, or
   `powerprofilesctl set performance`); check with
   `cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort -u` (one line: `performance`). Turbo can
   make runs vary: note in the report whether it was on, and keep it the same for all runs.
3. Clone and build: `git clone <repo> tsdb && cd tsdb && make && make dist`.
4. Check: `make test` and `make dist-test` pass, and `build/tsdbd --version` prints the version;
   `TSDB_CPU` unset, `build/tsdb-bench` should report the v3 codecs (the startup line names the level).
5. About 5 GB of free disk on a real disk (not a RAM tmpfs) for data and results, e.g. `/var/tmp`.

## How to measure

- Nothing else heavy runs during a timing (no builds, tests or other benchmarks).
- Every number is the MEAN of several runs (at least 5; 11 for the short fold cases), with the standard
  deviation and the lowest and highest run. Never a single run or the median.
- Before every timing: governor `performance`, and the machine idle (fewer than 1.5 CPUs busy over 2 s).
- Save the raw output of every run, not only the summary.

## Steps

1. **Baseline at v2 and v3.** First `tools/bench-tsdb.sh --standard --portable --cpu-compare` (about an hour and a quarter; it builds the
   release build from scratch, records the machine, runs the folds, the codecs (with `decode-bench`), `tsdb-bench`, `tsdbd` with
   `tsdbc-bench` and `insert-bench` five times each, then `ht-sweep.sh` and `thresholds.sh` of `bench/cpu-compare` once, and writes `bench-results/*/summary.md` and `summary.json`; add
   `--reps 11` for the short fold cases, `--full` for float64 and int64). That covers the first three bullets
   below except the forced `TSDB_CPU` levels; the real sensor data and `bench/clients` stay manual. On a
   `make dist` build, by hand:
   - `tools/fold-bench.sh build-dist 11` (it runs every level the CPU has: here v2 scalar, SSE, AVX and v3);
   - `build-dist/codec-bench` and `build-dist/type-bench` with `TSDB_CPU=v2` and `TSDB_CPU=v3`;
   - `build/tsdb-bench --dir /var/tmp/b --series 100 --points 1000000`, also with `--type float64` and
     `--type int64` (load and query classes);
   - the real sensor data: [bench/hydraulic/README.md](../../bench/hydraulic/README.md), three commands;
   - the server and clients: `bench/clients/run.sh --work /var/tmp/cbench --clients c --repeat 5` (it
     exports the data, starts its own tsdbd with `--port 0 --port-file`, loads and queries; see
     [bench/clients/README.md](../../bench/clients/README.md)).
   Run the same with `TSDB_CPU=v3` on the 7950X if you can, so the two CPUs compare on the same code path.
2. **Thresholds.** For each threshold between a short-run and a wide kernel at v3 (and the v2 kernel sets),
   measure both paths around it on this CPU (fold-bench `--rows` sweeps). If Intel's crossover differs from
   Zen 4's, propose per-CPU values; results must stay bit-identical on both sides of every threshold
   (`tests/test_levels.c` covers both sides; extend it if a threshold moves).
3. **PDEP/PEXT bit unpacking.** A prototype decoder for bit-packed integers using BMI2 PDEP/PEXT as a
   dispatched kernel, chosen only on CPUs where PDEP is fast (Intel since Haswell, AMD since Zen 3). Output
   identical to today's decoder for every input (cases in `tests/test_levels.c`: random bit widths 0-64,
   lengths around block edges), decoders safe on any bytes (`make fuzz`). Measure decode speed with codec-bench
   and type-bench against today's v3 path.
4. **AVX2 float64 decimal paths** (not written yet): AVX2 versions of the float64 fixed-point encode (the scale
   test and the conversion to integers) and decode (integers to doubles), byte-identical to the v2 path and
   checked in `tests/test_levels.c`; measure them here against the v2 path they replace on v3 CPUs, and keep
   them only if they are faster here.
5. **Hyper-Threading.** The 10850K has 10 cores and 20 threads. With the query scheduler (`--query-threads`, after
   the multi-series work of October 2026), time large and small queries and multi-series requests at 1, 2, 4, 5,
   10 and 20 query threads, means of 5: does the second thread per core help (decode is integer and AVX2 work, and
   the two threads share the core's units) or hurt? Should the default be one thread per physical core
   (`/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`) on this CPU? Compare with the 7950X's 16 vs 32.

## Done when

- A report (`docs/tasks/intel-10850k-results.md`): the tables of steps 1-4 (means, sd, min-max), what is worth
  changing, and what is not and why.
- Any code change on its own branch, with tests, passing `make test`, `make asan`, `make tsan`, `make dist`,
  `make dist-test` and `make dist-check-sandybridge`, and identical results at every CPU level. A threshold
  or kernel that helps Intel must not slow down AMD Zen 4 (check on the 7950X).
- Commits signed off (`git commit -s`), no links to agent sessions (`make hooks` installs the check).
