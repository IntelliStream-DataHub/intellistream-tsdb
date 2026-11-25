# Task: how fast is the erasure codec on each CPU

The cluster's settled data is stored erasure-coded ([design-cluster-data.md](../design-cluster-data.md), E32): a stripe
of k data shards gets m parity shards (Reed-Solomon over GF(2^8)), and a read that finds a shard missing rebuilds it.
The codec (`src/ec`, copied from intellistream-objects) has kernels for SSSE3, AVX2, AVX-512 and GFNI, and
`build/ec-bench` times them one core at a time. This task is to run it on the two machines the project does not
develop on, and send the numbers back. About 10 minutes per machine.

## Run

1. Set up as in [ryzen-9950x.md](ryzen-9950x.md) (tools, governor `performance`, a quiet machine: nothing else running).
2. `make && make test` (the codec's own test is `build/test_rs`, about 25 s), then:

   ```sh
   nproc; lscpu | grep -E 'Model name|Flags' | head -2 > ec-bench-machine.txt
   build/ec-bench --rounds 7 --ms 300 | tee ec-bench-1MiB.txt
   build/ec-bench --rounds 7 --ms 300 --len 4194304 --geoms 8+4,16+4 | tee ec-bench-4MiB.txt
   ```

   It lists, for each CPU level the machine has (`base` = x86-64-v2, `v3`, `v4`) and each kernel the level allows
   (`scalar`, `ssse3`, `avx2`, `avx512`, `gfni256`, `gfni512`), for 4+2, 8+2, 8+4 and 16+4: **encode**, **decode**
   (data shards lost, rebuilt, matrix inversion included) and **rebuild (plan)**, in MB/s of the stripe's data
   (k x shard), mean and sd of the rounds. One core; at 1 MiB per shard the larger geometries leave L2, at 4 MiB all of
   them are in memory.

3. On the 9950X also `build/ec-bench --level v4 --rounds 7 --ms 300` with the CPU on its normal and on its power-saving
   profile, and say what the clock did (`grep MHz /proc/cpuinfo` while it runs): 512-bit instructions have slowed
   some CPUs' clocks.

## Questions

- Which kernel does `iso_gf256_mul_add` pick at `v4` here (`build/ec-bench` marks none: the dispatch picks the first
  available of gfni512, avx512, gfni256, avx2, ssse3), and is it the fastest in the table? The 7950X (Zen 4) runs 512-bit instructions as two 256-bit halves, the 9950X (Zen 5) at full width, so the order
  may differ between them (the 7950X table is at the end of this file). If another kernel wins by more than the sd on one of the two machines, say which, and for
  which geometry: the order of `iso_gf256_init`'s ladder is upstream's to change (intellistream-objects), not a
  change to the copy.
- The i9-10850K has AVX2 but no AVX-512 or GFNI: the table must show `base` and `v3` only, with `v3` avx2 well ahead
  of ssse3. Anything else (a kernel above its level) is a bug in the cap: send the whole output.
- How far is each machine from its memory speed at 4 MiB? (A decode cannot be faster than reading k shards and writing
  m.)

## Send back

The three files above (`ec-bench-machine.txt`, `ec-bench-1MiB.txt`, `ec-bench-4MiB.txt`) and the output of
`build/test_rs | tail -3`. The 7950X numbers are at the end of this file
and in [design-cluster-data.md](../design-cluster-data.md).

## The 7950X numbers (Zen 4, 32 threads, governor `performance`, measured 2026-11-13 under the bench lock)

One core, MB/s of the stripe's data (k x shard), mean of 3 runs (1 MiB shards) or 2 runs (4 MiB), each run 5 rounds of at
least 200 ms; the sd inside a run is 1 to 5 percent, so differences under that are noise. Decode and rebuild equal
encode within the sd except for the 512-bit kernels (below). Machine busy before the run: 1.5 percent of its ticks.
The scalar kernel is 0.9 to 1.9 GB/s everywhere and left out.

1 MiB per shard (encode / decode):

| geometry | base ssse3 | v3 avx2 | v3 gfni256 | v4 avx2 | v4 avx512 | v4 gfni256 | v4 gfni512 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 4+2 | 19001 / 18886 | 29974 / 30016 | 31574 / 31722 | 30084 / 29819 | 27895 / 30851 | 31244 / 31482 | 28869 / 31412 |
| 8+2 | 20124 / 20011 | 31927 / 31727 | 33211 / 33160 | 31892 / 32022 | 30910 / 32577 | 33331 / 33357 | 31258 / 33030 |
| 8+4 | 9940 / 9843 | 15611 / 15671 | 16555 / 16646 | 15898 / 15876 | 15808 / 16352 | 16736 / 16795 | 16074 / 16474 |
| 16+4 | 9912 / 9810 | 15064 / 14923 | 15838 / 15695 | 15344 / 15310 | 15265 / 15583 | 16063 / 16027 | 15124 / 15229 |

4 MiB per shard, encode / decode (every shard set is in memory):

| geometry | base ssse3 | v3 avx2 | v3 gfni256 | v4 avx512 | v4 gfni256 | v4 gfni512 |
|---|---:|---:|---:|---:|---:|---:|
| 8+4 | 7089 / 7067 | 7524 / 7572 | 8286 / 8062 | 8155 / 8119 | 8211 / 8188 | 8032 / 7937 |
| 16+4 | 7751 / 7450 | 7054 / 6972 | 7592 / 7458 | 7153 / 7195 | 7404 / 7321 | 7388 / 7461 |

What it shows:

- On this machine the fastest kernel is **gfni256** at every geometry (v3 or v4 alike), 4 to 5 percent ahead of avx2 and
  about 1.6 times ssse3. At `v4` the dispatch picks gfni512 first, which is 4 to 8 percent slower than gfni256 at 1 MiB
  (Zen 4 runs 512-bit instructions as two 256-bit halves) and 5 to 10 percent slower on encode than on decode: the
  512-bit encode is the one number that sits below its own decode. At 4 MiB the kernels are within the sd of each other
  (memory bound). Whether the 9950X (full-width 512) reverses the order is what the run there answers.
- EU1 (design-cluster-data.md: "GFNI kernels encode k = 16, m = 4 above 8 GB/s a core on Zen 4/5"): 16 GB/s of data a core
  at 1 MiB shards, but 7.4 GB/s at 4 MiB, where the stripe (20 shards of 4 MiB) no longer fits the caches and the core
  reads and writes memory. The claim holds when the stripe is cache-sized and misses by 8 percent when it is not. The 4 MiB rows are the mean of 2
  runs (1 MiB: 3), so read them as 7.0 to 7.8 GB/s for every kernel (ssse3 7.75, gfni256 at v3 7.59, v4 7.40): the spread is
  inside the sd, and no kernel is the best there.
  The user accepted this on 2026-11-13: 7.4 GB/s a core at 4 MiB shards is enough; the claim stands as measured.
- Memory bound at 4 MiB, but not at the stripe's own traffic: `iso_rs_encode` (src/ec/rs.c, `for i < m: for j < k:
  iso_gf256_mul_add(out, shards[j], shard_len, ...)`) makes one pass over all k data shards per parity shard, each pass
  over the whole shard, so data that does not stay in the caches is read m times, and each output shard is read and
  written k times. 16+4 at 4 MiB reads 4 x 64 MiB per 64 MiB of data: 7.4 GB/s of data is about 30 GB/s of reads, not
  the 9.3 GB/s that reading 16 and writing 4 shards once would take. The same shows at 1 MiB: 8+4 and 16+4 run at half
  the speed of 4+2 and 8+2 on both machines (twice the passes). Working the shards in cache-sized columns (every parity
  of one column before the next) reads the data once; done in intellistream-objects dc0018b3 (see the i5-13400 section below).

## The i5-13400 numbers (Raptor Lake, 6 P-cores + 4 E-cores, AVX2 + GFNI, no AVX-512; measured 2026-11-13)

Built here with `-march=raptorlake`, run on P-core 2 (`taskset -c 2`), governor `performance`, the clock at 4.6 GHz
throughout, the machine otherwise idle; one run of 7 rounds of at least 300 ms. `build/test_rs`: ok (6 kernel
configurations, 159000 erasure patterns). The table has `base` and `v3` only, and `v3` offers avx2 and gfni256: no
kernel above its level. At `v3` the dispatch picks gfni256, the fastest here at every geometry (1 to 3 percent ahead
of avx2, past the sd in most rows). Encode / decode, MB/s of data; scalar is 0.8 to 1.7 GB/s.

| shard | geometry | base ssse3 | v3 avx2 | v3 gfni256 |
|---|---|---:|---:|---:|
| 1 MiB | 4+2 | 11050 / 11092 | 11647 / 11650 | 11906 / 11856 |
| 1 MiB | 8+2 | 11843 / 11847 | 12290 / 12272 | 12520 / 12149 |
| 1 MiB | 8+4 | 5771 / 5877 | 6120 / 6072 | 6216 / 6180 |
| 1 MiB | 16+4 | 5251 / 5156 | 5617 / 5627 | 5744 / 5779 |
| 4 MiB | 8+4 | 4039 / 4044 | 4265 / 4284 | 4353 / 4389 |
| 4 MiB | 16+4 | 4291 / 4307 | 4530 / 4542 | 4608 / 4651 |

The kernels are within 10 percent of each other even at 1 MiB (a 1.25 MiB L2 per P-core: the 1 MiB stripes already
read from L3), and a core does 2.6 times less than a 7950X core at 1 MiB, 1.6 times less at 4 MiB: on this CPU the
pass structure above, not the multiply, sets the speed.

### After the dot kernel (intellistream-objects commit dc0018b3, synced into src/ec)

Every kernel now computes up to four parity shards at once in registers and reads each data shard once, working in
8 KiB column tiles (`ISO_RS_TILE`; tiles from 4 KiB to none were within noise for m <= 4). Same machine, same flags
and pinning, 3 runs of 7 rounds of at least 300 ms, mean of the 3 (sd of the means under 3 percent except the avx2
and gfni256 4 MiB rows, up to 12 percent). `build/test_rs`: ok. Encode / decode, MB/s of data; the factor is encode
after / before from the first table.

| shard | geometry | base ssse3 | v3 avx2 | v3 gfni256 | gfni256 factor |
|---|---|---:|---:|---:|---:|
| 1 MiB | 4+2 | 15948 / 15714 | 27617 / 27297 | 43520 / 43347 | 3.7x |
| 1 MiB | 8+2 | 16699 / 16620 | 26328 / 26696 | 51707 / 51318 | 4.1x |
| 1 MiB | 8+4 | 9656 / 9706 | 16045 / 16115 | 33090 / 32671 | 5.3x |
| 1 MiB | 16+4 | 9668 / 9654 | 16204 / 16190 | 33966 / 31747 | 5.9x |
| 4 MiB | 8+4 | 8627 / 8653 | 13570 / 14880 | 23619 / 24211 | 5.4x |
| 4 MiB | 16+4 | 9469 / 9559 | 14792 / 15209 | 27093 / 26042 | 5.9x |

The 4 MiB gfni256 rows are at the 23 to 30 GB/s that reading the data once and writing the parity once takes here. The
avx2 kernel (the one a CPU with AVX2 and no GFNI or AVX-512 takes, such as an i9-10850K) gains 2.2 to 2.8 times at 1 MiB and 2.4 to
3.5 times at 4 MiB, ssse3 1.5 to 2.3 times. That CPU was not measured: the same code runs there (test_rs passes as
that CPU under qemu, `tests/distro/sandybridge.sh`), but its speed per clock will be lower than this table's avx2
column. The avx2 loop is bound by its 8 vpshufb per source vector of a 4-output group and by the front end, not by
loads, and the Skylake family runs vpshufb ymm on one port and allocates 4 micro-ops a cycle, where the i5's P-core
has two ports and 6. At 4 MiB it is whatever its memory gives one core.

## The Xeon Gold 6148 numbers (Skylake-SP, 2 sockets x 20 cores / 80 threads, AVX-512, no GFNI; measured 2026-11-25)

Built with `-march=native` (`-DTSDB_EC`), governor `performance`, one run of 7 rounds of at least 300 ms per shard
size; 1-min load average before the 1 MiB run was `0.54` (before 4 MiB: `7.34` from a preceding multi-thread load).
`build/test_rs`: `ok (9 kernel configurations, 238500 erasure patterns decoded)`. At `v4` the dispatch picks `avx512`
(no GFNI on Skylake-SP), which is the fastest kernel at every geometry. Encode / decode / rebuild (plan), MB/s of data;
scalar is 0.4 to 1.0 GB/s.

1 MiB per shard (`--len 1048576`):

| geometry | base ssse3 | v3 avx2 | v4 avx2 | v4 avx512 |
|---|---:|---:|---:|---:|
| 4+2 | 8615 / 8565 / 8574 | 12986 / 13186 / 13043 | 13117 / 12926 / 13033 | 17156 / 17355 / 17496 |
| 8+2 | 8711 / 8726 / 8766 | 14108 / 13970 / 14075 | 13198 / 13529 / 13974 | 19216 / 19141 / 19106 |
| 8+4 | 5151 / 5166 / 5147 | 9403 / 9325 / 9258 | 9026 / 8995 / 8788 | 13254 / 13201 / 13126 |
| 16+4 | 5299 / 4955 / 5197 | 9848 / 9297 / 9738 | 8971 / 8435 / 8741 | 14066 / 13979 / 14031 |

4 MiB per shard (`--len 4194304`):

| geometry | base ssse3 | v3 avx2 | v4 avx2 | v4 avx512 |
|---|---:|---:|---:|---:|
| 8+4 | 4316 / 4336 / 4360 | 8421 / 8415 / 8711 | 8777 / 8798 / 8900 | 10069 / 9997 / 10036 |
| 16+4 | 4783 / 4859 / 4872 | 8327 / 8793 / 8823 | 8704 / 9085 / 9108 | 9550 / 9631 / 9660 |

What it shows:

- `v4 avx512` beats `v4 avx2` and `v3 avx2` across all geometries: +31% to +56% at 1 MiB shards (17.2-19.2 GB/s at m=2,
  13.3-14.1 GB/s at m=4) and +6% to +15% at 4 MiB shards (9.6-10.1 GB/s), well above the 8 GB/s per core claim (EU1).

### 1 GiB object settle, digest, fold-bounds, and PPOS insert costs (Xeon Gold 6148, Micron 2450 NVMe)

Measured on the same machine (`fsync=1`, `zstd=1`, directory tier over NVMe):

- **1 GiB `.part` settle and digest** (`1,073,852,312` B = `1024.11 MiB`, `33,600` granules, derived `1 MiB` cell; mean of 3 runs at `load < 1.6`):
  - `4+2` (`6` chunks, `1,610,957,184` B): settle `2.340 s` (`458.9 MB/s`, `0.874 CPU s/GB`, peak RSS `1040.1 MiB`);
    unthrottled cold digest + release `1.664 s` (`652.0 MB/s` obj / `1304.1 MB/s` total read, `0.811 CPU s/GB`, peak RSS `17.4 MiB`);
    default `64 MB/s` throttled cold digest + release `32.025 s` (`33.6 MB/s` obj / `67.1 MB/s` read, `1.077 CPU s/GB`).
  - `8+2` (`10` chunks, `1,342,520,128` B): settle `2.309 s` (`469.0 MB/s`, `0.831 CPU s/GB`, peak RSS `1048.2 MiB`);
    unthrottled cold digest + release `1.883 s` (`608.4 MB/s` obj / `1216.7 MB/s` read, `0.819 CPU s/GB`, peak RSS `29.9 MiB`);
    default `64 MB/s` throttled cold digest + release `32.026 s` (`33.6 MB/s` obj / `67.1 MB/s` read, `1.093 CPU s/GB`).
    For a small `8.96 MiB` part deriving a `144 KiB` (`147,456` B) cell at `8+2`: settle `0.065 s` (`144.4 MB/s`, `1.351 CPU s/GB`),
    unthrottled cold digest + release `0.033 s` (`484.8 MB/s` obj, `1.665 CPU s/GB`, peak RSS `6.9 MiB`), throttled `64 MB/s` release `0.293 s`.
  - `16+4` (`20` chunks, `1,342,742,240` B): settle `2.398 s` (`447.8 MB/s`, `1.006 CPU s/GB`, peak RSS `1065.8 MiB`);
    unthrottled cold digest + release `1.767 s` (`614.6 MB/s` obj / `1229.3 MB/s` read, `1.002 CPU s/GB`, peak RSS `53.7 MiB`);
    default `64 MB/s` throttled cold digest + release `32.031 s` (`33.5 MB/s` obj / `67.1 MB/s` read, `1.324 CPU s/GB`).
  - Settle peak RSS tracks the `mmap` of the 1 GiB source part plus one stripe's buffers (~16-42 MiB); digest uses `pread` in stripe pieces and keeps peak RSS to `17-54 MiB`.
- **`fold-bounds` (`block_decode_ts_sorted` + bounds check) and `verify_checksums`** (5 interleaved rounds on 100 M rows):
  - Micro (`8192`-row granule): `block_decode_ts_sorted` + `[ts_min, ts_max]` check adds `+0.007 ns/row` (`+2.7%` to `+3.3%`) on regular 60s DoubleDelta runs (`3701-4344 M rows/s`) and `+0.22-0.23 ns/row` (`+18%` to `+21%`) on zstd-compressed jittered series (`689-755 M rows/s`).
  - Macro (`verify_checksums=1` vs `0` on 100 M rows): `raw scan` `283.0 -> 275.7 M rows/s` (`+2.67%`), `agg 3600` `6430.5 -> 6185.6 M rows/s` (`+3.96%`), `agg 60` `1603.3 -> 1493.5 M rows/s` (`+7.35%`), `agg 10` `749.3 -> 620.2 M rows/s` (`+20.82%`), `group 15m` `1955.0 -> 1788.1 M rows/s` (`+9.33%`), `group 1h` `4251.5 -> 3938.9 M rows/s` (`+7.94%`).
- **`PPOS` insert overhead (`plain` vs `requires=cluster,ec`)** (5 interleaved rounds, 1000 mixed series):
  - `100k -> parts (fsync=1, mt=0)`: `7.56 -> 6.96 M rows/s` (`-7.93%`, p50 `13.35 -> 13.87 ms`, `+0.68%` CPU, `151` PPOS records / `4832 B`); with `fsync=0, mt=0`: `16.51 -> 16.34 M rows/s` (`-1.07%`).
  - `100k -> WAL+memtable (fsync=1, mt=0)`: `11.92 -> 10.56 M rows/s` (`-11.45%`, p50 `7.06 -> 8.21 ms`, `+21.70%` CPU).
  - `10k -> WAL+memtable (fsync=1, mt=0)`: `3.42 -> 3.18 M rows/s` (`-6.91%`, p50 `2.60 -> 2.87 ms`, `+17.84%` CPU).

