# Comparing CPUs

The scripts and tools that produced `docs/tasks/intel-10850k-results.md`, so the same numbers can be taken on another
CPU (a Ryzen 9 7950X, a 9950X, an Intel server) and set side by side. Nothing here changes the repository: the
threshold sweep works on a scratch copy.

| what | how | answers |
|---|---|---|
| `tools/decode-bench.c` (`build/decode-bench`) | `build/decode-bench [--rounds 7] [--ms 100]` | Speed of bit unpacking per width (loops, the AVX2 shuffle kernel, PDEP) and of int_bp and float64 decimal decode and encode. Is the shuffle kernel worth enabling at v4 on this CPU? Is PDEP ever ahead (Zen 3 and later, Intel)? |
| `ht-sweep.sh` | `bench/cpu-compare/ht-sweep.sh [--server]` | What the second hardware thread of a core is worth (pinned core and sibling sets), how one query scales with `--query-threads`, and with `--server` how `tsdbd` scales on a multi-series request. Reads the topology from /sys, so it runs on any core count. |
| `thresholds.sh` + `thresholds-env.patch` | `bench/cpu-compare/thresholds.sh --cpu v3` (and `--cpu v4`) | Where the short-run kernels hand over to the wide ones on this CPU, against the constants in `src/simd.c`. |
| `bench/hydraulic/run.sh`, `bench/clients/run.sh` | see their READMEs | Compression on real data (it must not move: no byte on disk changes), and loads and queries through `tsdbd`. |

## The one-command version

`tools/bench-tsdb.sh --standard --portable --cpu-compare` builds the release build from scratch, records the machine,
runs `decode-bench`, the folds, the codecs, `tsdb-bench`, `tsdbd` and `insert-bench` five times each, then
`ht-sweep.sh` and `thresholds.sh` (v3, and v4 on an AVX-512 CPU) below, and writes `bench-results/*/summary.md` and
`summary.json` (about an hour and a quarter). It covers steps 1 and 3 to 5 of the order below; what it does not do (the
forced `-DTSDB_UNPACK_AVX2` build of step 2, the sanitizers, the hydraulic data, `bench/clients`) stays manual. The two
scripts here run on their own just as before; the one-command run keeps their output as it prints it, one run of each.

## Which code runs where

The 10850K runs the **v3** level. A 7950X or 9950X picks **v4**, where the AVX2 shuffle unpack kernel is compiled out
(`__AVX512BW__`) and the AVX2 float64 decimal path is compiled out (`__AVX512DQ__`; at v4 the compiler vectorizes the
decimal decode itself but not the encode), so the changes of this branch do not run there by default. To compare the same code across CPUs run `TSDB_CPU=v3` (a `make dist` or
CPU_LEVELS build), or build with `ARCH=-march=x86-64-v3`; measure native v4 next to it.

## Order for a new machine

Quiet machine (no builds, no desktop load), governor `performance`, gcc 14 or newer, liburing installed.

1. `git worktree add ../tsdb-main main` and build both trees (`make`): the branch under test and `main` for the
   "before". `make test` on the branch; `make dist && make dist-test` for the portable tarball and every level.
2. `build/decode-bench`, then again with the shuffle kernel forced at v4:
   `make B=build-u CFLAGS="-O3 -g -DTSDB_UNPACK_AVX2" build-u/decode-bench build-u/test_levels && build-u/decode-bench && build-u/test_levels`
   (`test_levels` must still pass: it compares every level byte for byte). On an AVX-512 CPU the default build
   keeps the loops (`__AVX512BW__`), and the second run shows whether the kernel would be ahead of them. The knob is
   experimental and **unverified on AVX-512 hardware** (it was only compiled for it), off in every default build, and the
   objects depend on the headers listed in the Makefile rules but not on `CFLAGS`: use a separate build directory, as above.
3. `tools/fold-bench.sh build-dist 11` (every level this CPU has), and
   `bench/cpu-compare/thresholds.sh --cpu v3 --vary` (and `--cpu v4` on AVX-512 CPUs).
4. `bench/cpu-compare/ht-sweep.sh --server` (about 20 minutes at 5 rounds of 3 s).
5. `make tsan` may abort every test with `FATAL: ThreadSanitizer: unexpected memory mapping` (and finish in under a
   minute) on a kernel with high address randomization: run it as `setarch x86_64 -R make tsan`. `make test` was red
   on every machine until the `test_gindex` fix (the last commit of the branch); on `main` it still is.
6. `bench/hydraulic/get.sh`, `convert.py`, `run.sh` on both trees: the tables must be identical.
7. `bench/clients/run.sh --clients c --repeat 5` on both trees (about 12 minutes each; run them one after the other, the
   load average must be below 2 at the start).

For the same code path on every CPU use `TSDB_CPU=v3` (a CPU_LEVELS build, `make dist`): the 7950X and 9950X then run
the AVX2 kernels the 10850K runs, and the comparison is the CPU, not the code. Without it they run v4.

Every number is a mean of interleaved runs with the standard deviation; keep the raw output and say in the report
whether the governor was `performance`, whether turbo was on, and what else was running.
