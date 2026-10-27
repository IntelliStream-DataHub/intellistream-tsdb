# Task: what a Ryzen 9 9950X (Zen 5) can do for tsdb

A task to work on the 9950X machine on its own, from a fresh checkout. Read [AGENTS.md](../../AGENTS.md)
first: its rules ("CPU levels" above all) hold here too.

## Why this machine

tsdb is developed on a Ryzen 9 7950X (Zen 4). Zen 4 runs each 512-bit AVX-512 instruction as two 256-bit
halves; the 9950X (Zen 5) runs them at full width. It also has every extension Zen 4 has beyond the x86-64-v4
baseline (VBMI, VBMI2, GFNI, BITALG, VPOPCNTDQ, BF16), plus AVX512-VP2INTERSECT and AVX-VNNI. Three questions:

1. How fast is tsdb on Zen 5, level by level, against the 7950X numbers?
2. Should the v4 level (AVX-512) use 512-bit vectors? Today it prefers 256-bit ones
   (`LV_FLAGS_v4 := -march=x86-64-v4 -mprefer-vector-width=256` in the Makefile, the same in
   `client/rust-sys/build.rs`), which was right for CPUs that slow down on 512-bit instructions.
3. Can VBMI (VPMULTISHIFTQB, VPERMB) unpack bit-packed integers faster? That is the core of the decimal and
   integer codecs (`src/codec.c`, `src/codec_typed.c`).

## Set up the machine

1. Tools: gcc 14 or newer, make, the libzstd and liburing development packages, python3, rsync.
   Ubuntu: `sudo apt install build-essential libzstd-dev liburing-dev python3 rsync`.
   AlmaLinux: `sudo dnf install gcc make libzstd-devel liburing-devel python3 rsync`.
2. CPU governor on performance for every timing: `powerprofilesctl set performance` (or
   `sudo cpupower frequency-set -g performance`); check with
   `cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort -u` (one line: `performance`).
3. Clone and build: `git clone <repo> tsdb && cd tsdb && make && make dist`.
4. Check: `make test` and `make dist-test` pass, and `build/tsdbd --version` prints the version.
5. About 5 GB of free disk on a real disk (not a RAM tmpfs) for data and results, e.g. `/var/tmp`.

## How to measure

- Nothing else heavy runs during a timing (no builds, tests or other benchmarks).
- Every number is the MEAN of several runs (at least 5; 11 for the short fold cases), with the standard
  deviation and the lowest and highest run. Never a single run or the median.
- Before every timing: governor `performance`, and the machine idle (fewer than 1.5 CPUs busy over 2 s).
- Save the raw output of every run, not only the summary.

## Steps

1. **Baseline, every level.** On a `make dist` build:
   - `tools/fold-bench.sh build-dist 11` (the folds: every type, aggregate, spread, TWA, LTTB, at v2 scalar,
     SSE, AVX, v3 and v4);
   - `build-dist/codec-bench` and `build-dist/type-bench` (the portable build; `build/` has the `make` ones), with
     `TSDB_CPU=v2`, `v3`, `v4`;
   - `build/tsdb-bench --dir /var/tmp/b --series 100 --points 1000000` and with `--type float64` and
     `--type int64` (load and query classes);
   - the real sensor data: [bench/hydraulic/README.md](../../bench/hydraulic/README.md), three commands.
   Compare with the same runs on the 7950X (the numbers in AGENTS.md "Benchmark results" are older; run the
   same commands there too if you can).
2. **512-bit or 256-bit vectors at v4.** Build `make dist` twice: as is, and with
   `-mprefer-vector-width=512` in `LV_FLAGS_v4`. Run step 1's fold-bench, codec-bench and tsdb-bench at v4
   with both. A change is worth making only if it is faster on Zen 5 and not slower on Zen 4 (the 7950X); if
   the answer differs by CPU, the dispatch can pick per CPU (src/cpu.c picks the level at startup).
3. **VBMI bit unpacking.** A prototype decoder for bit-packed integers using VPMULTISHIFTQB and VPERMB, as a
   new dispatched kernel (the way `src/simd.c` dispatches kernels by CPU features), behind a check that the CPU
   has AVX512_VBMI. Its output must be identical to today's decoder for every input (add cases to
   `tests/test_levels.c`, including random bit widths 0-64 and lengths around block edges), and decoders must
   stay safe on any bytes (`make fuzz`). Measure decode speed with codec-bench and type-bench against today's
   v4 path, on the 9950X and the 7950X.
4. **The v4 code layout at 7-16 rows.** After the short-run work of October 2026, float32 aggregates of 7-16
   values on the v4 copy are 1-2.5% below the code before it (medians 0.975-0.992, on the 7950X), from how the
   compiler lays out the v4 copy of `src/values.c` (the user accepted this for 0.9.0). Check it on Zen 5 and,
   with steps 2 and 3 changing the same code, try to remove it.
5. **Look for uses of VP2INTERSECT** (finding equal elements of two vectors): merging sorted timestamps or
   series ids are candidates. Only if a measurement shows a gain.
6. **SMT (two threads per core).** With the query scheduler (`--query-threads`, after the multi-series work of
   October 2026), time large and small queries and multi-series requests at 1, 2, 4, 8, 16 and 32 query threads
   (16 = one per core, 32 = both SMT threads), means of 5. Zen 5 runs AVX-512 at full width, so two threads on one
   core compete for the same vector units: check whether 32 threads beat 16 for decode-heavy queries, and whether
   the default (one thread per logical CPU) should become one per physical core
   (`/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`). Compare with the same sweep on the 7950X, and
   note the two CCDs (8 cores each, L3 per CCD): does keeping one query on one CCD help?

## Done when

- A report (`docs/tasks/ryzen-9950x-results.md`): the tables of steps 1-4 (means, sd, min-max), what is worth
  changing, and what is not and why.
- Any code change on its own branch, with tests, passing `make test`, `make asan`, `make tsan`, `make dist`,
  `make dist-test` and `make dist-check-sandybridge`, and identical results at every CPU level.
- Commits signed off (`git commit -s`), no links to agent sessions (`make hooks` installs the check).
