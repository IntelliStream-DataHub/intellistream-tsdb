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
- Memory bound at 4 MiB: 16+4 reads 16 and writes 4 shards of 4 MiB per stripe, 7.4 GB/s of data is about 9.3 GB/s of
  memory traffic a core; the kernels have run out of what one core's memory path gives.
