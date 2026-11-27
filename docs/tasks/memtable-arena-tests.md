# The memtable arena's tests: catch the planted bugs, and the growth stall under buf_lock

**Open (2026-11-27).** From the review of b0770cad ("memtable: per-memtable bump arena for series rows and deferred
mt_unref outside flush_lock"), which went to main without a track review. The code itself was found correct (readers
copy under `buf_lock`, blocks reach the free list only after the series points at its new block, sizes and
alignment hold, the rollback and the deferred unref are right); its test is what is thin.

## 1. `test_arena_high_cardinality` (tests/test_flush_tail.c) misses most planted bugs

The review planted eight bugs in `src/memtable.c` (`mt_reserve_to`, the slab free list, `mt_big`, the rollback of
`mt_reserve_rows`). `test_flush_tail` caught three: an unref inside `flush_lock`, a wrong `MT_BIG_PAD`, a short copy
of `ts` on growth. It let five through:

- no copy of `v` when a series grows into a bigger block;
- no copy of `lsn` when a series grows;
- a free-list pop that does not move the head (one block handed to two series);
- the old block freed before it is copied;
- no free in the rollback of a failed `mt_reserve_rows` (a leak only).

`test_tsdb` happens to catch the free-list and the missing-`v` mutants, so the suite is not blind, but the arena's own
test should catch every one. Why it misses them: the value checks look only at a series' newest row
(test_flush_tail.c, the two `mt_collect` blocks after the growth loops), and the rollback is checked only by
`lgcap == 0`.

To do:

1. After the growth loops, compare **every row** of each grown series (the 20-row series through the slab classes
   4 -> 8 -> 16 -> 32, a series of more than 32 rows for 64 and 128, and both `mt_big` series through 128 -> 16384)
   with what was written: timestamps, values and the lsn of each row (`mt_collect` with `visible` bounds that tell
   the lsns apart, or the rows read directly).
2. Make two series grow **interleaved**, so a block one of them gives back is popped by the other: a pop that does
   not move the head then shows as two series sharing rows.
3. Check the rollback by memory as well as by `lgcap`: run the failing `mt_reserve_rows` many times and require the
   arena's free-list length (or a test counter of `mt_big` blocks alive) to stay flat; under ASan the leak shows too.
4. Plant the five mutants above (a `--mutant NAME` switch in the test, or `TSDB_TEST_HOOKS`-gated hooks in
   memtable.c, as the other engine mutants do) and add a Makefile loop through `tools/mutants.sh` that requires
   `test_flush_tail` alone to fail on each. Run the binary under ASan once
   (`make B=build-asan build-asan/test_flush_tail`).

## 2. A large series' growth stalls the volume under `buf_lock`

Above 128 rows a series' growth mallocs a fresh block of the new capacity, copies `n * 24` bytes into it and frees the
old one, all under `buf_lock` (`mt_reserve_to`). AGENTS.md says nothing holding `buf_lock` may touch a large block of
fresh memory. Measured in the review: the worst single `mt_add` at 8M rows of one series took 54.6 ms, against 33.4 ms
with the three `realloc`s before; about the same as before at 1M-4M rows. `--buffer-rows` is capped at 16M rows
(TSDB_BUFFER_ROWS_MAX, held back at 32M), so one series can reach about 32M rows in one memtable: a copy of up to
768 MB under the lock.

To do: measure the worst `mt_add` at 16M and 32M rows of one series (a case in `tests/test_flush_tail.c` beside the
others that bound `buf_lock` stalls, or `build/insert-bench`), and if it is beyond the bound those tests hold, grow
large series in chunks (a list of blocks per series, or a mapping that is grown with `mremap`) instead of one copy.
