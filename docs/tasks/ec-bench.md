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
  may differ between them (the 7950X table is in the report of the track `r5-ec`). If another kernel wins by more than the sd on one of the two machines, say which, and for
  which geometry: the order of `iso_gf256_init`'s ladder is upstream's to change (intellistream-objects), not a
  change to the copy.
- The i9-10850K has AVX2 but no AVX-512 or GFNI: the table must show `base` and `v3` only, with `v3` avx2 well ahead
  of ssse3. Anything else (a kernel above its level) is a bug in the cap: send the whole output.
- How far is each machine from its memory speed at 4 MiB? (A decode cannot be faster than reading k shards and writing
  m.)

## Send back

The three files above (`ec-bench-machine.txt`, `ec-bench-1MiB.txt`, `ec-bench-4MiB.txt`) and the output of
`build/test_rs | tail -3`. The 7950X numbers measured while this was written are in the report of the track `r5-ec`
and in [design-cluster-data.md](../design-cluster-data.md).
