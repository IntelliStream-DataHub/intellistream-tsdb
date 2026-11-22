# R10 takes re-encode (the plan's D9)

**Open. Owner: R10 (docs/design-cluster-data.md §11, about 4-5 track-days instead of 3).** R7 settles; it never
combines. The first version of R7 had a re-encode track (TASK-r7-reencode); D9 moved it here to shorten R7's critical
path. R10 re-encodes for conversions (§6.4) anyway, so this is one mechanism with two triggers.

## What it is

Late rows settle as further objects of a key (E6), and tombstones in young parts keep hiding rows of objects. Answers
stay right without a re-encode (`tests/test_settle_oracle.c` judges settled against unsettled twins), but a key with late
rows reads more objects and decodes rows a tombstone hides. E6's rule: re-encode when a key has more than
`ec_objects_max` (4) objects, or tombstones cover more than 10% of their rows. A re-encode combines the objects and the
young parts of the key into one object and one `.ec` part, through the same swap.

## What R7 gives it

`ec_swap_commit` (all parts of one key or none, the input check under `db->lock`, the recovery rules: `src/ecswap.c`
header), `eng_settle`'s one-pass check and encode, the read seam through which a source `.ec` part is read, the delete
queue and the sweep (`src/ecgc.c`: the retired object's chunks go at the last unref), `settled_` with its digest. R7's
jepsen runs report parts per partition: in eager mode (jepsen only: every merged part settles, `.ec` parts are never
merged again, and each restart resets the 30 a minute) a 300 s run ends with partitions of hundreds to 1372 listed
parts, nearly all `.ec`. That is R10's measure of how bad a key gets before it lands; the default policy settles a class-0
key only after `grace_s` and its merges.

## What R10 must add (from the critique of the first plan)

- **Bound the input**: at most `max_part_bytes` (4 GiB, `src/db.c`, `src/merge.c`) and `MAX_MERGE_PARTS` (100,
  `src/internal.h`) per re-encode, and count only the objects such a re-encode could combine; back off after a failure.
  Else a key of five 4 GiB objects triggers for ever. Test with `max_part_bytes` small.
- **A source that is DOWN or CORRUPT aborts without marking the part.** `.ec` sources reach `merge_parts`, which marks a
  part on a failed decode and counts `merge_fail_seq` (`src/merge.c`); a source that is DOWN (`TSDB_ERR_UNAVAILABLE`)
  or CORRUPT through the seam must abort the re-encode and leave the part alone (a store being away is not damage to the
  part). The settle's rule is the model: a CORRUPT check marks the part damaged, a failure hands its own chunks to r7-gc.
- **The swap needs intent v4 `R <pX.ec> <retired_pX.ec>` lines** beside the `L` and `R <part> <settled_part>` lines
  R7 has (an `.ec` source is retired, not settled: its object goes by the delete queue), and `lt_merge_marks` in its write
  section (the latest table treats a re-encode as a merge). The input check and the recovery rule apply unchanged,
  and `ecswap_recover`'s name rules (an `.ec` under none of its names while its source is `.part`) need the new shape.
- **A re-encode is a merge** for the latest table and for `db_wait_merges`' count: it must not be invisible to
  backpressure, and `part_merge_barrier` must let the re-encode take what the merge selection may not (an `.ec` part).
- **The logical digest** `ec_digest_rows` (rows per series and an xxh64 over the visible rows) is built here, not in R7:
  it is what E26 describes and what the replicas compare (docs/tasks/r8-from-r7.md item 6). A re-encode's retired
  objects are checked by it before their chunks go.
- **A re-encode's own policy**: the E6 trigger in `src/ecpolicy.c`, a backoff per key, no re-encode while the key's
  digest of an earlier settle is pending (`eckeep`), and the eager policy bounded so that jepsen runs stay small.
- **Mutants** in R6's pattern for each new check (an enum in the code, `--mutant NAME`, a Makefile line that requires the
  failure): a re-encode that drops a hidden row's tombstone, one that ranks its output below a young part, one that marks
  a part damaged on UNAVAILABLE.

## Open items of the R7 reports that fall here

- `granule_read` on an `.ec` source through `pw_copy_granule` is refused with a "bug" line
  (`TSDB_ERR_INVALID`); a re-encode decodes rows, never copies a granule of an `.ec` part. Keep that, and test a source
  that is partly `.ec`, partly `.part`.
- The v8-part case: `eng_settle` and the policy skip parts with a 24-byte header; a re-encode of a key that holds one must
  rewrite it first (`rewrite_alone`, docs/tasks/pre-1.0-v8-parts.md) and the same test on the real v8 fixture is missing
  for the settle too (r7-settle open item).
- The 4 GiB big-part case (1 MiB cells, fd count, memory, time) was never written: the settle's `TSDB_TEST_BIG=1` case is
  open, and a re-encode of several of them is the same case with more objects open at once (the seam's fd budget counts
  `chunk_npos` per object).
- Settle cost numbers (CPU, memory and time per GiB, at each geometry) were never measured; R11 needs them and a
  re-encode reads and writes every object it combines.
