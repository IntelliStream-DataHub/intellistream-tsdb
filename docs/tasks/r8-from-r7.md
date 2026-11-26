# R8 takes what R7 left out

**R8-1 done (2026-11-26); remaining items split across R8-2, R9, R10, R11, R13, R15 ([design-cluster-data.md](../design-cluster-data.md) §11).**
R7 was built as the settle of ONE server with a directory backend (D1-D11, 2026-11-17). R8-1 implemented the replicated
cluster half: `PPOS`, the settle gate and cut, `Y`, `MK_COLD`, `MK_DIGEST` with E26's logical digest and D25 rot
handling, replica apply with intent v5, durable `DIVERGED`, epoch-fenced chunk servers/clients (`chunksrv`/`chunkpeer`),
and `MK_GC`/`GCMK` garbage collection. R8-2 tracks `r8p-prefetch` (done), `r8o-margins` (STATUS/metrics done; TOPOLOGY
waits for protocol 16), `r8q-geom` (done), and `r8n-hedge` (remaining). D24 moved degraded settles, per-part cluster
settles, and `eccache_reopen` after a heal to **R9**, server roles (`compute`/`storage`/`both`) to **R13**, and reverse
intent (`settled_` back), tier store `db_id`, and followers over a tier to **R15**. `AGENTS.md` "Settled parts"
describes what is now built; below is the status of each R8 item.

## What R7 refused today (D1, D7)

- **Done (R8-1):** cluster EC volumes (`requires=cluster,tier,ec`) open and settle via `eccold_settle` (`src/eccold.c`),
  `ecapply_*` (`src/ecapply.c`), `src/db.c`, `src/replica.c`, `server/chunkpeer_cluster.c`, and `server/repld.c`.
- **Remaining:** read-only opens, followers (`D7` -> **R15**), single-server tiers without `TSDB_TEST_TIER` (**R15**),
  and builds with `NO_EC=1` still refuse tiers and `.ec` parts.

## D1: the replicated half

1. **Done (R8-1, `src/ppos.[ch]`): `ppos` and its durable home.** Checksummed, append-only per-volume `PPOS` sidecar
   (32-byte offset-seeded xxh64 records, torn tail cut, compacted at open) fed at all three memtable append sites via
   a per-memtable `base_partition -> highest fno` map (`eng_ppos_rows` / `ppos_note`), `jr_part_pos` on `COMMIT`, max over inputs on
   merge (`src/merge.c`), and `Z` on settle. `PPOS_UNKNOWN` = `UINT64_MAX`. `.ec` record format v1 (`EC_RECORD_VERSION` = 1,
   unchanged; `record_valid` admits non-zero `ppos` and `stripe_version` on cluster EC volumes) carries `ppos` and
   `stripe_version`; `positions` (degraded settle mask) remains reserved for **R9**.
2. **Done (R8-1, `src/eccold.[ch]`): the gate and the cut.** `lw_enter` takes `jr_serial` (FIFO ticket lock) + key-range
   fence over `MK_BUCKETS`, rotates/flushes overlapping memtable (`F`), waits for commit point past `L` (`eng_leader_wait`),
   takes `Z`, collects `Y` (`test_eccold`). Swap keeps its input check under `db->lock`.
3. **Done (R8-1, `src/eccold.c`, `src/ecapply.c`):** on the leader, `Y` ranks above `.ec` by block numbers; on a replica,
   COLD apply gives `.ec` parts fresh block numbers in the leader's order and renumbers `Y` above `.ec`.
4. **Done (R8-1, `src/ecapply.c`, `src/ecswap.c` intent v5):** replicas apply `MK_COLD` with fresh `.ec` block numbers,
   updated `.ec` record metadata (`min_block`, `max_block`, `source`), and **intent v5** (`N`, `Y`, `R`, `end <xxh64>`).
5. **Done (R8-1, `src/marker.[ch]`):** `MK_COLD` (11), `MK_GC` (12), and `MK_DIGEST` (13 = `MK_TYPE_LAST`) implemented;
   types `14..15` (`MK_DROP_OBJ`, `MK_UNSETTLE`) remain reserved for **R10**.
6. **Done (R8-1, `src/ecdigest.[ch]`, `src/eckeep.c`):** E26 logical digest (`ec_logical_digest` over visible rows in
   `(series, ts)` order + clipped canonical tombstone union when `MKD_TOMBS` is set) compared against committed
   `MK_DIGEST`; D25 distinguishes checksum rot (`ECKEEP_ROT`: `eckeep_copies_lost++`, serve `.ec`) from logical mismatch
   (durable `DIVERGED`). Single-server and leader `settled_` files keep R7's byte digest.
7. **Done (R8-1, `src/ecdiverge.[ch]`, `src/ecapply.c`, `server/tsdbd.c`, `server/repld.c`, `server/cvol.c`):** a replica
   part above `Z` (or missing `PPOS`) stops apply and writes durable `DIVERGED`, causing `tsdbd`/`repld` to fail the
   replica (`vol_fail`, leave ISR, `LOCAL_FAULT` heartbeat, `TSDB_WHY_DIVERGED`, `tsdb_volume_diverged`). `tsdb DIR
   volume diverged --clear` is refused on cluster volumes (D23; use `tsdb cluster replica reset` to rebuild the replica
   instead). Reverse intent (D5:
   swapping `settled_` back) moved to **R15** (D24).
8. **Done (R8-1, `server/chunksrv.[ch]`, `server/chunkpeer.[ch]`):** leader writes stripe chunks to all positions over
   `PL_KIND_CHUNK`/`CHUNKB` and commits `MK_COLD`; members read through `coldops_peer` (`k' + 200 ms` grace). Degraded
   settles (E10) moved to **R9** (D24).
9. **Done (R8-1, `src/ecpolicy.c`):** cluster policy skips diverged engines (`db->diverged`), settles whole keys only
   (D21 -> per-part moves to **R9**), and requires `p->level > 0` (D18).

## D4: the epoch authority

**Done (R8-1, `server/chunkpeer_cluster.c`, `server/chunksrv.c`, `src/ecgc.c`, `src/ecgcmk.c`):** leader puts/deletes use
`coldvol.lead_epoch` (`wal_open` / attach); unsure chunk stores raise epoch only from `chunksrv_epoch_source`
(`chunkpeer_epoch_source` when metadata copy is caught up), never from requests or own store; `MK_GC` (`MK_GC_RETIRED`,
`MK_GC_ORPHAN`, `MK_GC_DONE`) + durable `GCMK` implement E14 cluster orphan & retired collection across leader changes.

## D7: followers

**Moved to R15 (D24):** followers over a tier (`chunk_store_open` read-only and `ecpart_follower_open`, shared storage)
remain unbuilt (`server/tsdbd.c` / engine still fail a follower that opens a tier or meets an `.ec` name).

## D8: prefetch

**Done (R8-2 `r8p-prefetch`, `src/eccache.c`); real-drive tuning -> R11:** sequential granule prefetch (16 stream slots,
`4..32` granules per RAW range read, `budget/4` and 4 MiB caps) is built with mutants in `test_seam`. Real-drive
throughput measurements belong to **R11**.

## The `Y` mutant

**Done (R8-1):** planted in `src/ecapply.c` / `src/eccold.c` (`ycold`, `ecabovey`, `cutnogate`, `coldnoflushthrough`,
`checkz`, `noyhcheck`, etc.) and tested by `test_eccold`, `test_ecapply`, and `make jepsen-cluster-ec` (`jepsen-cluster-selftest`
runs `ycold`, `ecabovey`, `cutnogate`).

## Open items of the R7 reports that fall to R8

- **Done (R8-1):** `tsdbd` and `DIVERGED`: `tsdb_volume_diverged` accessor, `STATUS`, `UNAVAILABLE` (`TSDB_WHY_DIVERGED`),
  `tsdb_volume_diverged` metric, `repld` `vol_fail`, and `LOCAL_FAULT` heartbeat are built and tested (`test_diverge`,
  `test_cluster diverged`).
- **Done (R8-1, `r8k-gc`):** hold across a volume reopen (`F5`): holds are taken in `part_open_ec` (`ecgc_hold`) and
  dropped in `ecgc_last_unref`, stored in O(1) open-addressing hash tables keyed by `const part *` and `(volume, oid)`.
- **Open (design choice):** `ecgc` still runs its own tier worker thread (calling `ecgc_tier_work`, which also drives
  `ecgcmk_work`), rather than piggybacking on the settle thread.
- **Moved to R15:** `src/ecswap.c` holds `latest_rw` and `db->lock` across `2n` renames (r7-swap M2): decide when NFS
  tiers arrive in R15.
- **Done for cluster / moved to R15 for single server (D24):** cluster `chunksrv` stores bind to cluster UUID + node ID
  at `tsdb chunk-store init` and verify META at open (`server/chunksrv_cluster.c`); single-server tier store META with
  `db_id` moves to **R15**.
- **Moved to R9 (D24):** `eccache_reopen` call after a heal in `tsdbd` (`r7-seam #3`).
- **Done (R8-1 / `r8p-prefetch`):** `route_decide` with a diverged volume is covered by `test_cluster diverged` and
  `jepsen-cluster-ec`; `tsdb_query_multi` over settled parts has a standalone seam test (`tests/test_seam.c` `case_multi`,
  added in `r8p-prefetch`).
- **Done (R8-1):** `test_cluster` move/transfer of a settled EC volume (`test_cluster ecbasic|ec16`, `jepsen-cluster-ec`
  `VOLUME_TRANSFER`) and `MK_GC` coordination across leader drops/purges (`r8k-gc`, `test_ecgc`, `test_cluster ecgc`).

## What R8 inherits and must keep (do not weaken)

The settle checks the bytes it encodes, once. The seam always checks. A drop and a swap meet under `db->lock`. The
sweep runs before the threads (skipped on cluster EC volumes in favour of `MK_GC`). A retired `.ec` keeps its name until
its chunks are gone (or `MK_GC` deletes them on every member). A key is settled all or nothing. `make test NO_EC=1`
passes. All enforced by the `Makefile` mutant loops (`test_ppos`, `test_eccold`, `test_ecapply`, `test_ecdigest`,
`test_diverge`, `test_ecgc`, `test_ecswap`, `test_settle`, `test_seam`, `test_chunksrv`, `test_chunkpeer`, `test_margins`,
`jepsen-cluster-selftest`).

## Other open items of the R7 reports (no owner yet; whoever next touches the file)

Tests that were not written:
- A settle of a key whose CLASSES history contradicts a granule: "a class contradiction aborts" has `judge_rows` and the
  `noclass`/`judgerows` cases but no planted case end to end (r7-settle). Likewise the forged-index checks `pastobj`,
  `timeorder`, `outside`, `overlap` (they need a rewritten packed index and footer hash; the open's own checks stand in
  front of them), and the policy's `testmark` and `poldropping`.
- A settle test on the real v8 fixture (`tests/test_upgrade.c`'s database with a tier); the 4 GiB part
  (`TSDB_TEST_BIG=1`); a settle's cost per GiB at each geometry (**R11**); the open's orphan sweep with 10k and 100k objects; the
  digest's throttle (`eckeep_digest_mb_s`) against a real disk; the cost of `chunk_be_put_object`'s extra copy
  (+6-7% measured, within noise; `put_object` could call `chunk_encode` directly if heal throughput matters).
- `eckeep`: `each()`, `feed_engines` and `cvol_series_make` have no test; a store going away in the middle of a digest read
  cannot be tested in R6's directory backend (covered on cluster volumes via `chunksrv`); no crashfs case for the release of a
  `settled_` file (a plain unlink: a file that comes back after a power loss is digested again, and `ecswap_recover` takes
  a `settled_` name as "the swap stands"). `make jepsen`'s tier runs now settle under kill, power, eio and `settle-steps`,
  but their release deadline (`keep_s` 5) is the only exercise of it.
- `tier`: the directory fsync of `meta_edit` is unobserved (the open's later directory fsyncs cover it); the reopen test
  is in-process, not under outagefs; META is read in 512-byte pieces; a read-only open is refused rather than served.
- `ecpart`: `cell_bytes` is not capped below `CHUNK_CELL_LIMIT` (chunk.h: a recorded cell may be any size, E2).
- The seam: `cache_mb` is a key of `TSDB_TEST_TIER` (cluster builds expose `ec.cache` in `docs/config.md`; single-server
  tiers join `storage.` with **R15**); the byte columns of statistics use `eccache_part_bytes` for an `.ec` part.

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
- Single-server `settleable()` (`src/ecpolicy.c:147-151`, called from the single-server branches of `ecpolicy_pick_locked`
  at `src/ecpolicy.c:293-308`) refuses v8 headers but not insert-written parts: it relies on
  `merge_selectable`, which is false with `merge_threads` 0, so then an insert-written part can settle on a single
  server, against the rule of `src/ecskip.h` (the cluster branch of `ecpolicy_pick_locked` (`db->ec_cluster`) and
  `eng_settle` explicitly require `p->level > 0`, D18). Layout only;
  check `insert_written` in single-server `settleable`.
- r7-jepsen's REPORT names a TSan report in `test_replica` (`replica.c`, `wal_off` read without its lock). Not confirmed
  fixed here (224fc8d, `ld_last` under `buf_lock`, may be the fix): rerun `make tsan TEST_TARGET=test-meta`'s replica
  group before relying on it.
