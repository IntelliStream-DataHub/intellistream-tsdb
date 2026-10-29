# Results: tsdb on an Intel Core i9-10850K (partial)

Measured 2026-10-29 on the 10850K (10 cores / 20 threads, AVX2, no AVX-512), Ubuntu, gcc 13.3 (the task asks for 14+),
governor `powersave` under `intel_pstate` (HWP, EPP `balance_performance`; no sudo to change it), the desktop idle
at about 1.5 CPUs. Binaries: a portable-style build (`-march=x86-64-v2`, codecs also at v3 and v4, `NO_URING=1`: liburing
headers are missing, so no `tsdbd`, clients or `make dist`). Raw output was kept in `/var/tmp/i10850k/`.
Not done yet: steps 2 (thresholds), 3 (PDEP/PEXT), the hydraulic data, the server and clients,
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
(An earlier note here read the `tsdb-bench` "1 thr" and "20 thr" rows as one thread against 20 and called the scaling
poor, 1.4x to 3.3x. That was wrong: its "1 thr" row is one client that already runs every query thread inside the
query, and "20 thr" is 20 clients. Step 5 below has the real scaling.)

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

## Step 5: Hyper-Threading (10 cores, 20 threads; siblings are cpu N and N+10)

Setup: v3, the 100-series x 1 M-row float32 database (133 MiB, in the page cache), five interleaved rounds of 3 s per
cell (means; sd is under 1.5% unless a cell says more), governor `powersave`, the desktop at about 1.5 CPUs.
Raw output: `/var/tmp/i10850k/ht/`.

**A. What the second thread of a core is worth.** `taskset` sets, as many client threads as CPUs, each with
`query_threads` = CPUs (tsdb-bench `--qps`), random windows. q/s, and the factor over one CPU:

| CPUs | set | raw 3600 | x | agg 86400 | x | agg 604800 | x |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | cpu 0 | 38,246 | 1.0 | 3,900 | 1.0 | 768 | 1.0 |
| 2 | two cores | 75,339 | 1.97 | 7,694 | 1.97 | 1,522 | 1.98 |
| 2 | one core, both threads | 44,990 | 1.18 | 4,595 | 1.18 | 898 | 1.17 |
| 4 | four cores | 148,265 | 3.88 | 15,162 | 3.89 | 2,967 | 3.86 |
| 4 | two cores, both threads | 89,196 | 2.33 | 9,087 | 2.33 | 1,753 | 2.28 |
| 5 | five cores | 186,288 | 4.87 | 18,896 | 4.84 | 3,718 | 4.84 |
| 10 | ten cores, one thread each | 369,035 | 9.65 | 37,477 | 9.61 | 7,221 | 9.40 |
| 10 | five cores, both threads | 220,039 | 5.75 | 22,459 | 5.76 | 4,330 | 5.64 |
| 20 | everything | 414,271 | 10.83 | 43,085 | 11.05 | 8,091 | 10.54 |

Physical cores scale linearly (97-98% per core up to 10). The second thread of a loaded core adds 15-19% (18% on one
core, 18% on 2 to 5 cores) and 12-15% on the whole chip (10 cores to 20 threads): the decode is integer and AVX2 work,
and the two threads share the core's units, but the gain is real and it is never a loss.

**B. One large query, unpinned** (one client, `--query-threads` Q; q/s):

| Q | raw 3600 | agg 86400 | agg 604800 |
|---:|---:|---:|---:|
| 1 | 38,207 | 3,888 | 765 |
| 2 | 38,382 | 6,986 | 1,463 |
| 4 | 38,212 | 11,889 | 2,741 |
| 5 | 38,452 | 13,251 | 3,341 |
| 10 | 38,204 | 13,261 | 5,312 |
| 20 | 38,417 | 13,204 | 6,194 |

Raw windows (3600 rows) and the 86,400-row aggregates (about 5 threads of `QUERY_ROWS_PER_THREAD` = 16384 rows) are
capped by the rows they decode, not by Hyper-Threading: nothing above 5 threads helps them, and nothing hurts. The
604,800-row window uses every thread: 10 threads give 6.9x, 20 give 8.1x (+17% over 10).
`threads-bench` (one query over 1024 granules, 7 runs of 150 ms; mean us per query, one thread against all): agg 13,018
to 2,967 us at 5 threads (4.4x), 1,810 at 10 (7.1x), 1,514 at 20 (8.4x); STEP TWA 52,501 to 11,069 (4.7x), 5,900
(8.9x), 5,534 (9.5x): TWA gains only 7% from the second thread, agg 20%. Queries of 1 to 8 granules are as fast at
20 threads as at 5 (13.5, 36.0 and 34.6 us for 1 and 8 granules at 5, 20 threads).

**C. Many clients** (20 clients, Q query threads): throughput is flat in Q within the noise, 1 to 20 threads: raw
402,600 to 426,900, agg 86400 41,100 to 42,900, agg 604800 7,930 to 8,230. The machine is saturated by the clients,
so more helper threads add nothing, and cost 2-6% on raw windows at Q = 20 against Q = 1 (about three sd; small
windows run on their caller and the helpers only add wake-ups).

**D. tsdbd**, one connection (`tsdbc-bench query --multi 100`: 100 windows per QUERY_MULTI, the series spread over the
server's threads), q/s by `--query-threads`:

| Q | raw | agg 86400 | agg 604800 | twa 604800 |
|---:|---:|---:|---:|---:|
| 1 | 21,321 | 3,693 | 758 | 268 |
| 2 | 36,464 | 6,949 | 1,415 | 517 |
| 4 | 61,342 | 13,270 | 2,789 | 992 |
| 5 | 61,567 | 16,512 | 3,426 | 1,240 |
| 10 | 59,144 | 28,068 | 6,032 | 2,211 |
| 20 | 58,762 | 37,504 | 7,949 | 2,585 |

Multi-series requests scale to 10.2x at 20 threads for the aggregates (+34% over 10 threads, +32% for 604,800-row
windows, +17% for TWA). The raw class stops at about 4 threads: one connection and the reply path are the limit. This
is more than the 12-15% of A; the client runs on the same machine and takes CPU from a 10-thread server, so part of
the gain is the client and the server not competing. With one request per window (`--multi 0`) the aggregates stop at
the per-query cap again (agg 86400: 8,756 at 10, 8,734 at 20; agg 604800: 4,274 and 5,056, +18%).

**What follows.** Do not change the default of one query thread per logical CPU. Limiting it to one per physical core
would lose 12-19% on large and multi-series queries (up to a third in the loopback case) and gain nothing: no
configuration is slower with 20 threads than with 10, except a 2-6% throughput cost for tiny windows under a
saturating client load, which a fixed 10 would pay too. The second thread of a core is worth about 15% here, so
capacity planning for such CPUs should count a core as about 1.15 threads of tsdb work. Not measured: the 7950X
(16 against 32 threads; Zen 4's SMT may differ), and a pinned run of B/D with the client on other cores, which would
separate the client's share in D.
