# R8 takes what R7 left out

**Open. Owner: R8 (docs/design-cluster-data.md §11, about 9-11 track-days instead of 6-8).** R7 was built as the settle
of ONE server with a directory backend (the plan's decisions D1-D11, 2026-11-17): `.ec` parts that keep their source's
name and block range, intent v4, the read seam, `settled_*` with a digest, the delete queue. Everything that needs a
second member of a replica group was left for R8. `AGENTS.md` "Settled parts" says what exists; this file says what
does not, and what R8 must not take for granted.

## What R7 refuses today (D1, D7)

A settle never runs, and a tier never opens, on a volume whose META says `requires=cluster` or `requires=replica`, on a
read-only open, on a follower, and in a build without `TSDB_EC`. `eng_settle` and `tier_engine_attach` carry the
refusals (`tests/test_tier.c` `refuse`, `test_settle.c`). R8 removes the first one, for a leader and for a replica, in
`src/db.c`, `src/replica.c`, `src/memtable.c` and `server/repld.c`, which no R7 track touched.

## D1: the replicated half

1. **`ppos` and its durable home.** The design says "recorded in its MANIFEST entry" (§2.3). MANIFEST names parts and
   nothing else: `manifest_publish` writes a line per part name, rewrites the file in place without a sync, and the
   writer never reads it at open (FNO is the durable bound for exactly that reason). Choose the home first: a checksummed
   sidecar, or the part header (48 bytes with `fno_lo`, `fno_hi`, `written_ms`: no room, so a layout change that E17's
   "no new format number" must allow). The memtable keeps the per-key maximum position as it appends. The `.ec` record
   has `ppos` and `stripe_version` (written as 0, refused when not, `record_valid` in `src/ecpart.c`: bump
   `EC_RECORD_VERSION` or teach the check) and `positions` (a mask of the positions written, 0: all, for E10's
   degraded settle).
2. **The gate and the cut** (§2.3 steps 3-4): close P's settle gate before `lw_enter`, register the large-write fence
   over P's key range, wait for the commit point, flush through, take Z and `Y`. On one server the swap's input check
   under `db->lock` replaced all of it; on a leader the check must stay (a drop takes no `publish_lock`) and the gate
   comes on top. Lock order: `jr_serial` first, as for a large write.
3. **`Y`** and its shipping to the log replicas as pending files. A part written during steps 1-3 ranks above the `.ec`
   by its block number on the leader; on a replica the block numbers are the replica's own.
4. **Names on a replica.** §2.3 step 6 lists the `.ec` files under fresh block numbers in the leader's order; R7's D2
   keeps the source's name. Decide which holds on a replica (its `.ec` takes its own copy's name, or fresh numbers with
   `Y` renamed above). Whatever is chosen, the swap's input check and recovery rules (`src/ecswap.c` header) apply to the
   replica's apply, which is one intent as in step 6.
5. **`MK_COLD` and `MK_DIGEST`** (markers 11 and the digest; `src/marker.h`, `MK_COLD = MK_TYPE_LAST` reserved in format
   9), with the object records, names, sizes and xxh64 of the `.ec` and `Y` files, and the oids a re-encode retires
   (R10).
6. **E26's logical digest.** R7 built a byte digest: the object read whole, its xxh64 against the record's
   `object_xxh64` and against the `settled_` file's (`src/eckeep.c`). E26 specifies the rows per series and an xxh64 over
   the visible rows of the retired parts, compared between log replicas. A byte digest cannot be compared across
   replicas (their files differ); R8 owns it (docs/tasks/r10-reencode.md uses it and does not build it). Until then E26 must not read as built.
7. **DIVERGED on a part above Z** (a replica whose part has `ppos` between Z and the marker stops and is rebuilt) and the
   way out of DIVERGED in general. R7's is a flag on the engine (`db->diverged`) kept in memory only: a restart clears it, and the new
   engine serves the `.ec` part until its open digest has run and failed again, which waits while any store is not UP
   (an in-process reopen carries the flag, `src/volumes.c`; a restart does not). Make it durable, or make the open
   refuse until the digest has run. tsdbd cannot even say DIVERGED (docs/tasks/r7-keep-followups.md: `unav_entry`, `vol_health`). D5 deferred "a reverse intent
   that swaps `settled_` back" to R15, and a cluster needs it earlier than a test-only tier does: decide with R15.
8. **Shipping `.ec` files** over the bulk link, and a replica that misses one fetching it before it acknowledges the
   marker (§2.3 "Crash and failover"); the leader keeps what a marker names until its segment leaves retention.
9. **Settle on a replica's volume while the digest of an earlier one is pending.** `eng_settle` does not look at
   `db->diverged`: on a volume whose digest failed the settle thread goes on settling and each swap renames more
   sources to `settled_` (each held until its own digest matches). Decide whether the policy skips a diverged engine.

## D4: the epoch authority

R7's chunk epoch is the constant 1, raised in every store at open (`chunk_be_see_epoch(be, vol, 1)`, `tier_engine_attach`),
and a store with one damaged EPOCH slot is raised by itself (`tierfence`, `noselfraise` pinned this). Nothing fences a
second writer of the stores except the `flock` of `chunk_store_open`. In a cluster the epoch comes from the metadata
group (§5.7): a leader writes under its volume epoch, a deposed one is refused by the stores, and an unsure store gets
the group's epoch, never its own. R8 replaces the constant, keeps "never from the writer's own epoch" and tests a leader
change in the middle of a settle (the orphans' rule E14 and the `MK_GC` of a committed marker).

## D7: followers

A follower, and a build without `TSDB_EC`, refuse a tier today; a running follower that meets an `.ec` name fails its
engine with one line (`tier_follower_ec`; an embedded follower keeps answering from its last view, because no query path
checks `eng_fail`). Opening the stores read-only for a follower (`chunk_store_open` with no `flock` and no EPOCH write,
and `ecpart_follower_open`, a stub that returns CORRUPT) is R8 or R15, with the rule that a follower on another host needs
the stores on shared storage.

## D8: prefetch

The seam reads a granule's bytes on a miss and fills the cache entry (keyed (oid, offset)); nothing reads ahead. A
query over a long range pays one range read per granule not cached (`ec_granule_read`). Prefetch the next granules of the
same series and part when a read is sequential, with a budget in the cache, and measure it on a real drive (R11 owns the
numbers: nothing is claimed without one).

## The `Y` mutant

R7's gate said "the `Y` mutant" and could not have it: there is no `Y` on one server. R8 plants it in the apply (step 6):
a settle that retires a part of `Y` with the parts of I (or lists the `.ec` above `Y`), which loses the rows written
during steps 1-3 or ranks the old rows over them. `test_cluster` / `jepsen-cluster` with writes into settling keys must
fail with it, in the pattern of R6 (an enum in the code, `--mutant NAME`, a Makefile line that requires the failure),
beside `ecrankdown` and `swapnodropcheck`, which R7's tests already catch on one server.

## Open items of the R7 reports that fall to R8

- tsdbd and DIVERGED: no accessor for `db->diverged`; STATUS and the metrics show the volume AVAILABLE
  (r7-keep-followups.md). The flag is carried across `tsdb_volume_reopen` and `tsdb_cvol_replace` (the second untested:
  cluster volumes had no tier).
- A hold across a volume reopen: holds on an object are taken at retire only; a reopen whose old engine
  still has a reader on a part the new engine retires is not held (r7-gc F5). Plan: take the hold in `part_open_ec`,
  drop the one in `ecgc_retire_locked`, key holds by part pointer in a hash (O(1) at each unref with 10k parts).
- `ecgc` runs its own worker thread; r7-settle could call `ecgc_tier_work` from the settle thread (r7-gc item 2).
- `src/ecswap.c` holds `latest_rw` and `db->lock` across 2n renames (r7-swap M2, kept as the task said): on NFS,
  snapshots and latest lookups wait for those round trips. The alternative (mark the inputs `merging`, rename unlocked,
  relist) needs the swap to refuse under `drop_pending` as well, because `drop_retire` lets go of `db->lock` between its
  merging wait and its retire. R8 meets NFS tiers first; decide there.
- The tier's stores are not bound to a database (no `db_id`, store ids not pinned: `chunk_be_open2` is given NULL ids), so
  a disk that comes back as another store is not detected, and a copy of a database directory on the same stores shares
  the vuuid, which the sweep's rule cannot tell from itself. R15 adds `db_id` and store META (design §15.7); a cluster
  member needs it first if a server's disks move.
- The seam's fallback backend states and `eccache_reopen` for heal/replace/settle are in place but nothing calls them
  after a heal in tsdbd (r7-seam #3).
- `tsdb_query_multi` over settled parts has no seam test of its own (the oracle covers it end to end); the cluster's
  `route_decide` is not exercised with a diverged volume.

## What R8 inherits and must keep (do not weaken)

The settle checks the bytes it encodes, once. The seam always checks. A drop and a swap meet under `db->lock`. The
sweep runs before the threads. A retired `.ec` keeps its name until its chunks are gone. A key is settled all or nothing.
`make test NO_EC=1` passes. These each have a mutant in `Makefile` that fails a test; list them in R8's review.

## Other open items of the R7 reports (no owner yet; whoever next touches the file)

Tests that were not written:
- A settle of a key whose CLASSES history contradicts a granule: "a class contradiction aborts" has `judge_rows` and the
  `noclass`/`judgerows` cases but no planted case end to end (r7-settle). Likewise the forged-index checks `pastobj`,
  `timeorder`, `outside`, `overlap` (they need a rewritten packed index and footer hash; the open's own checks stand in
  front of them), and the policy's `testmark` and `poldropping`.
- A settle test on the real v8 fixture (`tests/test_upgrade.c`'s database with a tier); the 4 GiB part
  (`TSDB_TEST_BIG=1`); a settle's cost per GiB at each geometry; the open's orphan sweep with 10k and 100k objects; the
  digest's throttle (`eckeep_digest_mb_s`) against a real disk; the cost of `chunk_be_put_object`'s extra copy
  (+6-7% measured, within noise; `put_object` could call `chunk_encode` directly if heal throughput matters).
- `eckeep`: `each()`, `feed_engines` and `cvol_series_make` have no test; a store going away in the middle of a digest read
  cannot be tested (no runtime UP -> not-UP transition exists in R6's backend); no crashfs case for the release of a
  `settled_` file (a plain unlink: a file that comes back after a power loss is digested again, and `ecswap_recover` takes
  a `settled_` name as "the swap stands"). `make jepsen`'s tier runs now settle under kill, power, eio and `settle-steps`,
  but their release deadline (`keep_s` 5) is the only exercise of it.
- `tier`: the directory fsync of `meta_edit` is unobserved (the open's later directory fsyncs cover it); the reopen test
  is in-process, not under outagefs; META is read in 512-byte pieces; a read-only open is refused rather than served.
- `ecpart`: `cell_bytes` is not capped below `CHUNK_CELL_LIMIT` (chunk.h: a recorded cell may be any size, E2).
- The seam: `cache_mb` is a key of `TSDB_TEST_TIER` and is not in docs/config.md (it joins `storage.` with R15); the
  byte columns of statistics use `eccache_part_bytes` for an `.ec` part.

Harness and engine:
- `settle-steps` does not judge a server's exit status (only power does): a transient exit 1 at start passes unnoticed;
  count it there with io_uring's ENOMEM (8 MB memlock) excluded. A store EIO halts the tier for the rest of that tsdbd's
  life (`tier_writable`), so the settle-count minimum applies only to runs without one.
- Statistics of a multi call (sums over series) vary run to run at query threads above 1 even with nothing settled
  (deferred series are counted by whichever thread runs them); `test_settle_oracle` compares them at one thread, and
  `ORACLE_MULTI_STATS_MT=1 --mutant nomark` reproduces it (engine, not a settle bug).
- The oracle compares the plan counters (`rows_decoded`, `from_stats`, granules, parts) only under `plan_exact()`
  (`ORACLE_PLAN=0` turns it off): decide which counters are the contract once settled parts read through the seam.
- `-Wrestrict` false positives at -O1 in `tests/test_settle.c` `fresh()` and `tests/test_settle_oracle.c`.
- A damaged `retired_*.ec` record fails the open with a line (r7-gc F6): decide whether that should refuse or log.
- `settleable()` (`src/ecpolicy.c`) refuses v8 headers but not insert-written parts: it relies on `merge_selectable`,
  which is false with `merge_threads` 0, so then an insert-written part can settle, against the rule of `src/ecskip.h`
  (such parts keep their fast-codec blocks for ever once `.ec`). Layout only; check `insert_written` in `settleable`.
- r7-jepsen's REPORT names a TSan report in `test_replica` (`replica.c`, `wal_off` read without its lock). Not confirmed
  fixed here (224fc8d, `ld_last` under `buf_lock`, may be the fix): rerun `make tsan TEST_TARGET=test-meta`'s replica
  group before relying on it.
