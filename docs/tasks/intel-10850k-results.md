# Results: tsdb on an Intel Core i9-10850K (Comet Lake, AVX2)

Task: [intel-10850k.md](intel-10850k.md). Measured 2026-10-29 and 30 on the machine below. Branch
`intel-10850k-avx2-f64-decimal`, on top of `main` (eb58c21).

| | |
|---|---|
| CPU | i9-10850K, 10 cores / 20 threads (siblings are cpu N and N+10), AVX2, BMI2, no AVX-512; the engine runs its **v3** level |
| memory, disk | 31 GiB; ext4 on an SSD (`/var/tmp`, the root disk) |
| governor | `powersave` under `intel_pstate` (HWP, EPP `balance_performance`); there is no sudo to change it, so frequency scales with load and the numbers carry that |
| compiler | gcc 13.3 (the task asks for 14 or newer: not available), `-O3` |
| load | a desktop session (browser, IDE, music) ran throughout: 0.3 to 0.9 CPUs busy in the second before each client measurement, about 1.5 over a day |
| not possible here | `make dist` and what builds on it (`dist-test`, `dist-check-sandybridge`, `dist-check-almalinux10`: no static liburing, no sudo), the 7950X and 9950X |

Raw output of the main runs is in [intel-10850k-raw/](intel-10850k-raw/). Everything the numbers need to be repeated on
another CPU is in [bench/cpu-compare/](../../bench/cpu-compare/README.md); the last section lists the order.

## Summary

**Does the code need its own path for this CPU? No vendor or model path. Two improvements in the v3 (AVX2) level**, which
every CPU that runs v3 gets (measured here only), with byte-identical output at every CPU level, and one build fix:

| change | commit | effect on the 10850K (v3) |
|---|---|---|
| AVX2 float64 decimal encode and decode (step 4) | 480b0ef | encode +45 to +50%, decode +15 to +23% on a granule |
| AVX2 shuffle kernel for bit unpacking, widths 1-25 (step 3) | f6964cd | unpack 5.5-8.5x (1.6-2.4 to 13-15 G values/s); `int_bp` decode 2.3x; float64 decimal decode 1.5-1.8x more; float32 ALP decode 3.6x; AUTO decode with ZSTD 2.4x |
| Makefile: `src/bitpack.h` is a dependency of the object rules | f6964cd | an edit of it used to rebuild nothing |

End to end through `bench/clients/run.sh` (the C client, embedded and `tsdbd`, 5 repeats, main against this branch,
same machine and script): **queries +9% to +27%** (embedded raw 3600-row windows 37.8k to 46.6k q/s on one thread; 86,400-row
aggregates over TCP 8.8k to 9.7k and, on 32 connections, 35.7k to 41.1k), **loads unchanged within noise** (bound by fsync and
the disk; the TCP zstd cells vary 8-27%). The hydraulic compression table is identical to main's.

What was measured and **not changed**: the AVX2 thresholds (step 2: crossovers on this CPU differ by 8 to 16 rows, and 96
for float32 without nulls, from Zen 4's, but no query is faster with other values), PDEP/PEXT (step 3: a plain AVX2
kernel is faster beyond 8 bits and works on every CPU), the default of one query thread per logical CPU (step 5: the second
thread of a core is worth 11 to 23%, and nothing is slower with it).

**On the 7950X and 9950X neither change runs by default**: they pick the v4 level, where the shuffle kernel is compiled out
(`__AVX512BW__`) and the AVX2 decimal path is compiled out (`__AVX512DQ__`; the v4 object vectorizes the decimal *decode*
conversions itself, with packed `vcvtqq2pd`, but the *encode* loop stays scalar, so the AVX2 encoder may pay at v4 too:
untested). To measure the same
code the 10850K runs, use `TSDB_CPU=v3` (a `make dist` or CPU_LEVELS build) or an `-march=x86-64-v3` build, and measure
native v4 next to it.

Also fixed, last: **`test_gindex` failed on `main` too** (a NaN-payload mismatch between the index encoder and decoder at
`-O1` and above, see Verification). `gix_encode` no longer derives `tw_step` and `tw_lin` from a NaN; a new test fails
without the fix; `make test` is clean now (it was not before the fix). Real inserts with NaN readings never reproduced it
(0 of 108 refused).

Still open, for the 7950X and the 9950X: whether the unpack kernel should also run at **v4** (it is enabled only where there
is no AVX-512BW, because it could not be measured on AVX-512 hardware; `-DTSDB_UNPACK_AVX2` and `build/decode-bench`
test it), the thresholds and the thread-scaling there, and the same before/after of the client runs.

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
(An earlier note here read the `tsdb-bench` "1 thr" and "20 thr" rows as one thread against 20 and called the scaling
poor, 1.4x to 3.3x. That was wrong: its "1 thr" row is one client that already runs every query thread inside the
query, and "20 thr" is 20 clients. Step 5 below has the real scaling.)

## Step 2: thresholds (measured, not changed)

Method: a scratch copy of the tree whose `simd_select` reads every AVX2 threshold from the environment, so one
`fold-bench` binary runs the wide kernels at every row count ("W": `simd_short` and the integer and LTTB limits at 0) and
the small kernels or `values.c` loops at every row count ("S": all at SIZE_MAX), at v3, `--ms 50`, 11 interleaved rounds per
row count, 21 row counts from 4 to 1024 (and again with `--vary`, 15 row counts from 12 to 160, so the predictor cannot
learn one granule). W/S is the ratio of the means; a cell counts only beyond 2% or two sd. Raw output: `/var/tmp/i10850k/thr/`.

| threshold | now (AVX2) | W / S on the 10850K, `--vary` | crossover here |
|---|---:|---|---:|
| `AVX2_SHORT`, float32 | 128 | no nulls: 0.88 at 24, 1.23 at 32, 1.2-1.3 from 32 to 160; with nulls: 1.16-1.23 at multiples of 32 (32, 64, 96, 128, 160), 0.68-0.89 between them up to 112 | 32 without nulls; 128 with nulls |
| `AVX2_SHORT`, float64 | 128 | no nulls: 0.65 at 24-28, 0.94 at 48, 0.98 at 64, 1.07 at 80, 1.09 at 96, 1.12 at 112; with nulls: 0.94 at 64, 1.02 at 96, 1.06 at 128 | 80-96 without nulls; 128 with nulls |
| `i64` (agg, int64) | 16 | 0.82 at 16-20, 0.95 at 24, 1.23 at 32 (with nulls 1.04 at 24) | 24 |
| `i64_dev` (spread, int64) | 24 | 0.81-0.96 up to 40, 1.02 at 48; with nulls 0.91 at 24, 1.08 at 32 | 40 |
| `u32` (bool, state: agg and spread) | 32 | 0.90 at 28, 1.42 at 32 | 32 (unchanged) |
| LTTB small | 8 | 0.77-0.89 at 8 and 12 (all types), 1.2-1.4 at 16 (no nulls) | 16 |

(The wide float kernels fold a last partial group of 32 values one at a time, so with nulls their speed rises and falls
with n mod 32; that, not the CPU, is why 128 is a sound limit there.) TWA has no speed threshold in this sense
(`TWA_SEQ_MAX` fixes the summation order): W/S is 1.00 everywhere.

The crossovers differ a little from the Zen 4 values (16, 24, 32, 8 and 128, set on a 7950X), the integer and LTTB ones
by 8 to 16 rows, the float32 one by 96 rows without nulls. But **no query changes**. `tsdb-bench` on the 100 M-row
database, 7 interleaved runs per setting, M rows/s (default thresholds, wide always, small always):

| query | default | wide always | small always |
|---|---:|---:|---:|
| agg, buckets of 60 samples (60-row folds, decodes all) | 2,204 | 2,135 | 2,185 |
| agg, buckets of 10 samples | 647 | 601 | 656 |
| agg, buckets of 3600 samples | 5,071 | 4,872 | 4,860 |
| group by 15 minutes | 2,610 | 2,457 | 2,530 |
| random windows, 86,400 samples, 20 clients (q/s) | 53,950 | 54,209 | 51,328 |

The default is as fast as, or faster than, either extreme on every line (the sd of "wide always" is 4 to 11% where
the default's is 1 to 2%). A fold of a few dozen rows is some tens of nanoseconds in a query whose granule decode costs
microseconds, and a run that short only comes from the edge of a range or from buckets of a few dozen rows. So a per-vendor table
(`GenuineIntel` getting 24, 40, 32, 16 and a float limit that depends on whether the run has nulls) would buy up to
1.3x on a fold that is not on the critical path, for a vendor check at startup, two more sets of constants to keep right
at both sides in `tests/test_levels.c`, and values measured on one Intel core (not Haswell, Skylake or Ice Lake). Not
worth it; the numbers above are the values to use if a workload of 20-60-row granules ever appears. Not measured: the
same sweep on the 7950X, to see whether Zen 4's own values still hold.

## Step 3: PDEP/PEXT bit unpacking: not used; an AVX2 shuffle kernel instead (`src/bitpack.h`)

`unpack()` (the bit-packed integers of int_bp, decimal_bp, delta_bp, ALP, and the float64 decimal codec) ran at 1.6-2.4 G
values/s at v3, the cost of one 8-byte load, shift and mask per value that GCC does not vectorize well. Two prototypes
on 1024 values (one vector of the codecs), mean of 7 runs, M values/s:

| w | today | PDEP | AVX2 shuffle |
|---:|---:|---:|---:|
| 1-7 | 1,750-1,960 | 16,300-17,700 | 14,100-14,800 |
| 8 | 2,424 | 17,384 | 14,601 |
| 9-15 | 1,640-1,790 | 9,460-9,650 | 13,770-14,140 |
| 16 | 2,427 | 9,626 | 14,039 |
| 17-23 | 1,635-1,785 | (not tried) | 13,390-14,010 |
| 25 | 1,778 | (not tried) | 13,160 |

PDEP (w <= 8: one `pdep` into bytes, widened by `vpmovzxbd`; w 9-16: two into 16-bit lanes) wins only up to w = 8, by about
1.2x, loses 1.5x to the shuffle kernel from 9 to 16, covers no width above 16, and is microcoded on Zen 1 and 2: it would
need a CPU-model dispatch for a gain on the narrowest widths only. The shuffle kernel needs no dispatch: for w = 1..25 it
handles eight values (w bytes) a step, two 128-bit loads (the second at byte w / 2), one `vpshufb` with a per-width constant
control that moves the four bytes each value starts in into its 32-bit lane, `vpsrlvd` and a mask (w + 7 <= 32); the tables are
constants built with macros (no startup code). It is in `unpack()` for 1 <= w <= 25, n >= 8; the last n % 8 values, w = 0,
w >= 26 and short runs take the old loops. Only `__AVX2__` without `__AVX512BW__` uses it: the v3 level and `-march=native` on
CPUs without AVX-512; v2 and v4 are unchanged (v4 for want of an AVX-512 machine to measure on; AVX2 code would run there).
The loads read up to 16 - ceil(w / 2) (at most 15) bytes past the packed bytes, inside BLK_SLACK (32) as the old loops'
7 were (audited: part.c decodes from mapped parts followed by the footer, the zstd scratch is reserved with BLK_SLACK,
the fuzz harness and the tests pad); output is exactly n values. Bytes are the same: nothing about the format changes.

Effect (v3, 7 interleaved runs of old and new, mean (sd); M values/s):

| | before | after | |
|---|---:|---:|---:|
| `int_bp_decode`, one granule | 1,080-1,096 | 2,462-2,491 | 2.3x |
| float64 decimal decode (k * 10^-e), `mb` | 856-868 | 1,545-1,565 | 1.8x |
| float64 decimal decode (k / 10^e) | 694-697 | 1,066-1,075 | 1.5x |
| float64 decimal encode | 548-645 | the same | (pack is scalar, untouched) |

`type-bench` float64 and integer decode, with ZSTD and everything else (7 runs): cpu_temp 509 to 689, flow_rate 688 to 1,068,
hdd_temp 503 to 659, tank_bar 490 to 670, vibration 575 to 817, flow_total 317 to 348; int64 sequence 881 to 1,646, rate
1,040 to 1,639, totalizer 381 to 421; bool door 1,073 to 1,527, coin flips 692 to 1,047; state machine mode 780 to 1,260. The
bytesplit datasets (random, tank_psi: no bit packing) are unchanged. `codec-bench` float32 decode, codec alone: ALP 1,477 to
5,281, ALP_rd 640 to 1,698, decimal_bp 1,013 to 2,093, bit-delta 1,154 to 2,649, AUTO 1,070 to 2,103; with ZSTD(1): ALP 769 to
1,275, AUTO 695 to 1,669. Others unchanged.

Checks: `tests/test_levels.c` decodes int_bp blocks of every width 0-32 at 32 lengths around 8, 1024 and 8192 at every level
against `ref_ibp_decode`, and runs `unpack` alone on a heap buffer with exactly the bytes its contract allows past the
packed ones (ASan: a read beyond is an error; this runs in a `-march=native` build, where the AVX2 path is compiled in).
Four mutations of the kernel (the second load's bit offset, its byte offset, the shift mask, the loop bound) each fail it.
`test_levels`, `test_types`, `test_damage`, `test_crafted` and `test_tsdb` pass under ASan + UBSan on the native build;
`make test IO=epoll` passes except `test_gindex` (two failures that exist without this change, fixed in the last commit);
`tools/check-isa.sh` is clean.
The Makefile object rules did not list `src/bitpack.h`, so an edit of it rebuilt nothing (my first measurement ran stale
objects and showed no change): it is a dependency of the four rules now. No jepsen (no byte on disk changes). Not run: the
7950X (v4 is unchanged there by construction), `make dist-test`.

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
`make test IO=epoll` (native build) passed except `test_gindex` (2 failures), which failed the same way on the commit
before this change (fixed in the last commit). `test_levels` and `test_types` pass under ASan + UBSan on the native AVX2 build and at v2 and v3
in `build-lv`; `tools/check-isa.sh` is clean. No byte of a block changes, so no jepsen run. Not run: `make dist-test`,
`dist-check-sandybridge` (they need static liburing), `make tsan` (no concurrency touched), the 7950X.

## Step 5: Hyper-Threading (10 cores, 20 threads; siblings are cpu N and N+10)

Measured with the final code by `bench/cpu-compare/ht-sweep.sh --server`: v3, the 100-series x 1 M-row float32
database (133 MiB, in the page cache, an untimed pass first), five interleaved rounds of 3 s per cell, means with sd, the
desktop running (the 20-thread cells carry 2 to 5% sd for that reason; the others under 1.5%). Raw output:
`intel-10850k-raw/ht-sweep.final.txt`.

**A. What the second thread of a core is worth.** `taskset` sets, as many client threads as CPUs, each with
`query_threads` = CPUs (tsdb-bench `--qps`), random windows. q/s, and the factor over one CPU:

| CPUs | set | raw 3600 | x | agg 86400 | x | agg 604800 | x |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | cpu 0 | 46,451 | 1.00 | 4,597 | 1.00 | 925 | 1.00 |
| 2 | two cores | 91,337 | 1.97 | 9,112 | 1.98 | 1,846 | 2.00 |
| 2 | one core, both threads | 56,387 | 1.21 | 5,619 | 1.22 | 1,122 | 1.21 |
| 4 | four cores | 180,972 | 3.90 | 18,072 | 3.93 | 3,638 | 3.93 |
| 4 | two cores, both threads | 112,574 | 2.42 | 11,196 | 2.44 | 2,209 | 2.39 |
| 5 | five cores | 225,885 | 4.86 | 22,525 | 4.90 | 4,561 | 4.93 |
| 10 | ten cores, one thread each | 443,971 | 9.56 | 44,887 | 9.76 | 8,938 | 9.66 |
| 10 | five cores, both threads | 277,331 | 5.97 | 27,691 | 6.02 | 5,497 | 5.94 |
| 20 | everything | 525,254 | 11.31 | 52,359 | 11.39 | 9,926 | 10.73 |

Physical cores scale linearly (97-98% per core up to 10). The second thread of a loaded core adds **21-23%** (one core,
two cores, five cores), and **11-18% on the whole chip** (10 cores to 20 threads: raw +18%, agg 86400 +17%, agg 604800 +11%):
decode is integer and AVX2 work and the two threads share the core's units, but the gain is real and never a loss. (An
earlier sweep, before the unpack kernel was in, gave 15-19% and 12-15%.)

**B. One large query, unpinned** (one client, `--query-threads` Q; q/s):

| Q | raw 3600 | agg 86400 | agg 604800 |
|---:|---:|---:|---:|
| 1 | 46,333 | 4,585 | 926 |
| 2 | 46,318 | 8,186 | 1,765 |
| 4 | 46,397 | 13,709 | 3,278 |
| 10 | 45,940 | 15,324 | 6,202 |
| 20 | 46,281 | 15,384 | 7,371 |

Raw windows (3600 rows) and the 86,400-row aggregates (about 5 threads of `QUERY_ROWS_PER_THREAD` = 16384 rows) are capped
by the rows they decode, not by Hyper-Threading: nothing above 5 threads helps them, and nothing hurts. The 604,800-row
window uses every thread: 10 threads give 6.7x, 20 give 8.0x (+19% over 10). `threads-bench` (an earlier run, before the
unpack kernel; one query over 1024 granules, 7 runs of 150 ms; mean us per query, one thread against all): agg 13,018
to 2,967 us at 5 threads (4.4x), 1,810 at 10 (7.1x), 1,514 at 20 (8.4x); STEP TWA 52,501 to 11,069 (4.7x), 5,900 (8.9x),
5,534 (9.5x): TWA gains only 7% from the second thread, agg 20%. Queries of 1 to 8 granules are as fast at 20 threads as
at 5 (13.5, 36.0 and 34.6 us for 1 and 8 granules at 5, 20 threads).

**C. Many clients** (an earlier run, before the unpack kernel; 20 clients, Q query threads): throughput is flat in Q
within the noise, 1 to 20 threads: raw 402,600 to 426,900, agg 86400 41,100 to 42,900, agg 604800 7,930 to 8,230. The
machine is saturated by the clients, so more helper threads add nothing, and cost 2-6% on raw windows at Q = 20 against
Q = 1 (about three sd; small windows run on their caller and the helpers only add wake-ups).

**D. tsdbd**, one connection (`tsdbc-bench query --multi 100`: 100 windows per QUERY_MULTI, the series spread over the
server's threads), q/s by `--query-threads`:

| Q | raw | agg 86400 | agg 604800 | twa 604800 |
|---:|---:|---:|---:|---:|
| 1 | 24,485 | 4,391 | 904 | 281 |
| 2 | 41,440 | 8,320 | 1,697 | 545 |
| 4 | 64,221 | 15,853 | 3,320 | 1,052 |
| 10 | 61,824 | 33,040 | 7,088 | 2,364 |
| 20 | 61,506 | 42,762 | 9,840 | 2,733 |

Multi-series requests scale to 9.7-10.9x at 20 threads for the aggregates: +29% over 10 threads for 86,400-row windows,
+39% for 604,800-row windows, +16% for TWA. The raw class stops at about 4 threads (24k to 64k, then flat): one connection
and the reply path are the limit, not the decode. The gain from 10 to 20 threads is more than the 11-18% of A because the
client runs on the same machine and competes for the cores with a 10-thread server; a pinned client and server were not
run.

**What follows.** Do not change the default of one query thread per logical CPU. Limiting it to one per physical core
would lose 11-39% on large and multi-series queries and gain nothing: no configuration is slower with 20 threads than with
10, except a 2-6% throughput cost for tiny windows under a saturating client load, which a fixed 10 would pay too.
Capacity planning for such CPUs should count a core as about 1.2 threads of tsdb work. Not measured: the 7950X and 9950X
(16 against 32 threads; Zen's SMT may differ).

## Hydraulic data (real sensors, `bench/hydraulic/run.sh`)

The 17 UCI hydraulic-rig sensors, float32 and float64 (and the pressures converted to psi): bits per value of tsdb and
of the other codecs, every granule decoded again and checked bit for bit. **The table of this branch and the table of
`main` are identical** (`diff` of the two outputs: no difference; `intel-10850k-raw/hydraulic-table.md`): the AVX2 paths
change no byte. 24 of the 46 rows differ by 0.01 bit (smaller) from the table in `bench/hydraulic/README.md`, which
predates other commits on `main` (2026-10-26); this work did not move them. The table is about size, not speed, and does
not depend on the CPU.

## Clients and the server (`bench/clients/run.sh --clients c --repeat 5`)

Embedded C and the C client over TCP to `tsdbd` (epoll: no liburing), the 100 M-row set, 5 repeats, medians. Run on the
branch (2694f60) and then on `main` (eb58c21) (two sequential runs of about 12 minutes, not interleaved), both built with
`-march=native` (here AVX2), the machine quiet before every measurement (0.27 to 0.92 CPUs busy), 62 loads per run checked against the file (row count,
sum, every timestamp and value): 0 mismatches in both runs. Queries, q/s, main -> branch:

| query | embedded, 1 thread | embedded, 32 threads | TCP, 1 connection | TCP, 32 connections |
|---|---:|---:|---:|---:|
| raw, 3600 samples | 37,788 -> 46,590 (+23%) | 429,527 -> 522,650 (+22%) | 15,491 -> 16,849 (+9%) | 86,517 -> 88,876 (+3%) |
| agg, 86,400 samples in buckets of 300 | 13,137 -> 15,383 (+17%) | 41,905 -> 53,307 (+27%) | 8,750 -> 9,694 (+11%) | 35,654 -> 41,071 (+15%) |
| agg, 604,800 samples in buckets of 3600 | 6,138 -> 7,366 (+20%) | 8,049 -> 10,147 (+26%) | 5,010 -> 5,809 (+16%) | 7,968 -> 9,847 (+24%) |

Loads, M rows/s with 1 / 4 / 16 threads or connections, main -> branch: embedded fsync on 30.1 -> 30.3, 93.1 -> 89.7, 146.9
-> 141.9; fsync off 33.7 -> 33.8, 117.5 -> 118.0, 197.0 -> 195.9; TCP zstd 19.4 -> 19.6, 59.1 -> 59.8, 91.0 -> 87.9; TCP plain 27.0 ->
27.3, 79.5 -> 76.0, 87.2 -> 86.6. Within the noise: the spread of the TCP zstd cells is 8-27% of the median, so only the query gains are
clearly beyond it. Loading is bound by fsync and the disk, not by decode. The Rust, Python and Java clients were not run (the task asks for `--clients c`;
cargo, Maven and a Python environment were not set up).

## Verification

| check | build | result |
|---|---|---|
| `make test IO=epoll` (`NO_URING=1`) | native, gcc 13.3 | **clean after the `test_gindex` fix** (exit 0, no `-i`); before it every test passed except `test_gindex` (2 failures, also on `main`) |
| `tests/test_levels` | v2 scalar/SSE/AVX, v3 scalar/SSE/AVX/AVX2, and native | identical output at every level, 405 inputs; 92 float64 decimal inputs, 400 crafted decimal blocks, widths 0-32 at 32 lengths for unpack, `unpack` on an exactly sized buffer |
| mutations of the new code (7: 3 in the decimal path, 4 in the unpack kernel) | test_levels | each one fails the suite |
| `make asan` (full) | generic x86-64 (`ARCH=` in the target), copy of the tree | pass, no report; `test_gindex` passes there too |
| ASan + UBSan on `test_levels`, `test_types`, `test_damage`, `test_crafted`, `test_tsdb` | native (AVX2 paths compiled in) | pass, no report |
| `make tsan` | generic x86-64, `IO=epoll` | pass, no report, **under `setarch -R`** (below) |
| `tools/check-isa.sh` | v2/v3/v4 build | clean: nothing above v2 outside the dispatch |
| jepsen | - | not run: no byte on disk changes, no write or recovery path touched |
| `make dist`, `dist-test`, `dist-check-sandybridge`, `dist-check-almalinux10` | - | **not run**: `make dist` needs a static liburing, which is not installed here (Podman is) |

`make tsan` aborts every test with `FATAL: ThreadSanitizer: unexpected memory mapping` on this kernel (6.17) with gcc
13.3's TSan runtime: a known incompatibility with the kernel's address randomization (`vm.mmap_rnd_bits`; it cannot be read
or lowered here without root).
`setarch x86_64 -R make tsan` (no randomization, no root) runs it. `make asan` and `make tsan` build with `ARCH=` (generic
x86-64), so neither compiles the AVX2 kernels: the native ASan + UBSan run above covers those.

**`test_gindex` (cause found, fixed in the last commit).** Two cases ("random fields" #113 and #293) fail with "the granule index encoder
lost bits" on `main` as on this branch, in any build with `-O1` or higher (gcc 13.3: `-O1`, `-O2`, `-O3`, with or without
`-march=native`, `-fno-tree-vectorize`), and pass at `-O0`, with `-fsanitize=undefined` at `-O2`, and under the TSan and
ASan builds; clang (`-O1`, MemorySanitizer build) fails many more cases, among them the "float32 special" ones.
`struct granule` has no padding and UBSan reports nothing. Printing the differing bytes in a scratch copy: always bytes 128-129,
the low payload bytes of `tw_lin`; the entries have `tw_first` and `tw_last` both NaN with different payloads
(`fffffffffffffa26` and `fffffffffffffc42`) and a finite `fsum`. `gix_dlin(sum, first, last, gap)` is
`(sum - (first + last) * 0.5) * gap`; which operand's payload `first + last` returns when both are NaN is the compiler's
choice (the result of an IEEE operation on two NaNs has an unspecified payload; x86 returns the first source operand's), and
the encoder's evaluation (`gindex.c`, deciding that `tw_lin` is derivable) and the decoder's (`gx_load`, inlined elsewhere)
are compiled with different operand orders at `-O1` and up. So the encoder flags the field as derived, the decoder derives
other bits, and the encoder's own round-trip `memcmp` refuses the index. At `-O0` the two agree by accident.

Is real data exposed? It needs first and last both NaN with different payloads and a **finite** sum, which a series with
NaN readings does not give (the sum is NaN then); nulls share one payload. Inserts of a regular float32 series with NaN
first, last and middle readings (6 x 6 payload pairs, 3 shapes, 108 inserts, native `-O3`) were all accepted. So the test
built an input real data probably cannot, but the encoder's choice depended on the compiler (and on the CPU: x86 and Arm pick
different payloads), which is reason enough.

**The fix** (`src/gindex.c`, in `gix_encode`, the loop that fills `skip[]`): never derive from a NaN.

```c
bool nan = e->fsum != e->fsum || e->tw_first != e->tw_first || e->tw_last != e->tw_last;
skip[2 * i] = !nan && same_bits(gix_dstep(...), e->tw_step);
skip[2 * i + 1] = !nan && same_bits(gix_dlin(...), e->tw_lin);
```

A NaN entry keeps `tw_step` and `tw_lin` as explicit fields (a few bytes more for such a granule); the decoder already reads
explicit fields, so no format changes and every index written before still reads the same. A new `test_nan_ends` in
`tests/test_gindex.c` (NaN ends with four different payloads, a finite sum, `tw_lin` and `tw_step` set to each payload and
to what `gix_dlin`/`gix_dstep` give; every entry round-trips alone and together) fails on the old encoder at native `-O3`
(27 failures: the 2 random cases of before and 25 of these) and passes with the fix at native `-O3`, generic `-O3` and `-O0`; `make test IO=epoll` is clean.

## Repeating this on the 7950X and the 9950X

[bench/cpu-compare/README.md](../../bench/cpu-compare/README.md) has the order and the commands. In short: build `main` and
this branch, run `build/decode-bench` (also with `-DTSDB_UNPACK_AVX2`), `thresholds.sh --cpu v3` (and `v4`),
`ht-sweep.sh --server`, `bench/hydraulic/run.sh` and `bench/clients/run.sh --clients c --repeat 5` on both. Questions the
other CPUs answer. First, **both changes are compiled out at v4**, the level these CPUs pick: run with `TSDB_CPU=v3` (and
`TSDB_CPU=v4` next to it) to see them, and run `test_levels` with `-DTSDB_UNPACK_AVX2`, which will be the first time the
shuffle kernel runs on AVX-512 hardware (here it was only compiled for it).

1. **Unpack kernel at v4.** `decode-bench` shows the loops, the kernel and PDEP side by side per width; on a 7950X
   (Zen 4 runs 512-bit operations in 256-bit halves) and a 9950X (Zen 5 has full-width units) it shows whether the kernel, which runs there too, beats the
   loops that v4 uses now, and whether PDEP (fast on Zen 3 and later) ever wins on AMD. If it wins, remove
   `!defined(__AVX512BW__)` in `src/bitpack.h` and run `make dist-test` at every level.
2. **Thresholds** (`thresholds.sh`): are Zen 4's constants in `src/simd.c` right on the 9950X, and do the 10850K's
   crossovers (table in step 2) hold on AMD? A per-vendor table is only worth it if a crossover moves by a factor
   and a query follows.
3. **Hyper-Threading** (`ht-sweep.sh`): the second thread is worth 11 to 23% here; the same on 16 cores / 32 threads?
4. **The AVX2 decimal path against v4.** At v4 `src/codec_typed.c` leaves the conversions to the compiler
   (`!defined(__AVX512DQ__)`). Checked in the objects (`objdump` of `f64_decimal_decode_v4` and `f64_decimal_encode_keep_v4`):
   the v4 *decode* has packed `vcvtqq2pd` conversions, the v4 *encode* none. Compare `decode-bench`'s float64 decimal
   rows at `TSDB_CPU=v3` and `TSDB_CPU=v4` on the same machine: if v3's encode is faster, the encoder's guard should drop
   `__AVX512DQ__` (the decoder's probably not).
5. **Before and after the same way** (`bench/clients/run.sh` on `main` and on the branch): on this CPU queries gained
   9-27%; is it the same on AMD?

