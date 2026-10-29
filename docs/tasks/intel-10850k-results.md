# Results: tsdb on an Intel Core i9-10850K (partial)

Measured 2026-10-29 on the 10850K (10 cores / 20 threads, AVX2, no AVX-512), Ubuntu, gcc 13.3 (the task asks for 14+),
governor `powersave` under `intel_pstate` (HWP, EPP `balance_performance`; no sudo to change it), the desktop idle
at about 1.5 CPUs. Binaries: a portable-style build (`-march=x86-64-v2`, codecs also at v3 and v4, `NO_URING=1`: liburing
headers are missing, so no `tsdbd`, clients or `make dist`). Raw output was kept in `/var/tmp/i10850k/`.
Not done yet: steps 2 (thresholds), 3 (PDEP/PEXT), 5 (Hyper-Threading), the hydraulic data, the server and clients,
and the same runs on the 7950X.

## Step 1: v2 against v3 (`tools/fold-bench.sh build-lv 5`, `codec-bench`, `type-bench`; medians, one run each for the codecs)

| M rows/s | v2 SSE | v3 AVX2 | gain |
|---|---:|---:|---:|
| agg float32 | 2745 | 5716 | 2.1x |
| agg float64 | 2280 | 5059 | 2.2x |
| agg int64 | 1576 | 3017 | 1.9x |
| agg bool | 3572 | 8895 | 2.5x |
| twa float32 | 862 | 1442 | 1.7x |
| lttb float32 | 950 | 1640 | 1.7x |

Spread over 5 rounds under 2% (fold-bench prints median and min-max, not mean and sd). Codec decode, M values/s: gorilla
488 to 665, alp 1199 to 1464, float64 decimal 445 to 498, int64 bit-packed 964 to 1078.
`tsdb-bench` (100 M rows, 100 series, v3, one run per type): load 48-51 M rows/s, 1.26-1.40 bytes/row.
One thread against 20 on the random-window aggregates scales only 1.4x (604,800-sample windows) to 3.3x (86,400):
worth the Hyper-Threading sweep.

## Step 4: AVX2 float64 decimal (`src/codec_typed.c`, v3 level and `-march=native` without AVX-512DQ)

The float64 decimal codec converts int64 and double one value at a time below AVX-512DQ and divides one at a time.
Now four values at a time: the integer by the 1.5 * 2^52 mantissa trick (exact for |k| < 2^51), the rounding by adding
the same constant (ties to even, as `llrint`), the same IEEE multiply or divide as the scalar code. A group of four
with an exception, or a value at or beyond 2^51 scaled, takes the one-at-a-time code, so the bytes are identical; a
decoded block with any integer at or beyond 2^51 in magnitude (damage) is converted scalar as a whole. v2 and v4 are untouched.

One 8192-value granule, 7 runs of 3 rounds, interleaved old and new, v3, mean (sd of the run means):

| case | encode before | encode after | decode before | decode after |
|---|---:|---:|---:|---:|
| 10^-2, k / 100 | 381 (3) | 557 (7) | 580 (7) | 714 (4) |
| 10^-2, k * 0.01 | 406 (4) | 584 (8) | 776 (14) | 891 (3) |
| 10^-5, divide | 392 (2) | 575 (8) | 581 (4) | 713 (7) |
| 10^-9, multiply | 439 (4) | 656 (9) | 775 (21) | 888 (11) |
| 10^-12, divide | 412 (3) | 616 (11) | 578 (8) | 711 (11) |

Encode +45 to +50%, decode +15 to +23% (M values/s). `type-bench` float64 decode (zstd and the integer unpacking
included), 7 runs: cpu_temp 515 to 532, flow_rate 670 to 717, flow_total 310 to 331, hdd_temp 500 to 523, tank_bar 491 to
505, vibration 502 to 610: +3 to +6%, vibration +21%. The integer unpacking (about 0.9 ns a value) is what is left of decode.

Checks: `tests/test_levels.c` has 92 float64 decimal inputs (every decimal and binary scale, both ways of rebuilding,
exceptions, lengths around four, integers at 2^51 and 2^52) and 400 crafted blocks with integers anywhere in the int64
range against a plain reference: identical at every level, and three deliberate mutations of the AVX2 code are caught.
`make test IO=epoll` (native build) passes except `test_gindex` (2 failures), which fails the same way on the commit
before this change. `test_levels` and `test_types` pass under ASan + UBSan on the native AVX2 build and at v2 and v3
in `build-lv`; `tools/check-isa.sh` is clean. No byte of a block changes, so no jepsen run. Not run: `make dist-test`,
`dist-check-sandybridge` (they need static liburing), `make tsan` (no concurrency touched), the 7950X.
