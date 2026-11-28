# Single-server settled tier

A single `tsdbd` server (without `--cluster`) can keep recent data on fast local storage (NVMe or SSD) and move older
partitions onto larger, slower chunk stores (typically local hard drives, or a ZFS/NFS export). Older partitions are
erasure coded across the tier's chunk stores while their granule indexes stay on the fast disk, so time-bucket
statistics queries keep answering from NVMe without touching the hard drives.

This differs from `tsdb DIR volume add /hdd/v1`: a volume stays on one filesystem for its whole life, including today's
inserts and queries. A settled tier keeps the WAL, memtables, recent `.part` files, and `.ec` granule indexes on the
fast filesystem, and moves older partitions onto the tier stores in the background.

A single-server tier protects against drive failures (up to `m` drives with `k+m` erasure coding), not against losing
the host, controller, or power supply. Use backups or a replicated cluster for host-level durability.

## Setting up a tier

1. Mount each tier drive on its own filesystem (separate from `/`, the database root, any volume, and every other tier
   store).
2. Add the stores with `tsdb DIR tier add`:

```sh
tsdb /nvme/db tier add /hdd0/tsdb-tier:hdd
tsdb /nvme/db tier add /hdd1/tsdb-tier:hdd
tsdb /nvme/db tier add /hdd2/tsdb-tier:hdd
tsdb /nvme/db tier add /hdd3/tsdb-tier:hdd
tsdb /nvme/db tier add /hdd4/tsdb-tier:hdd
tsdb /nvme/db tier add /hdd5/tsdb-tier:hdd
tsdb /nvme/db tier set --redundancy 4+2 --after 7d
tsdb /nvme/db tier status
```

Each path must be absolute and either an empty directory or one level below an existing directory (created by
`tier add`). The optional `:CLASS` suffix records the store's device class (`hdd`, `ssd`, `nvme`, or `unknown` when
omitted). For single-disk development or test hosts, `--allow-shared-device` bypasses the separate-device check.

The first `tier add` generates a random 16-byte `db_id` in the database root's `META` file, initializes each store's
`META` with its own 16-byte `id` and `db=<db_id>` binding, and appends a checksummed record to `<root>/TIERS` (beside
`VOLUMES`). A store belonging to another database is refused at add and at open.

### Starting `tsdbd`

`tsdbd` reads `<root>/TIERS` automatically — no flag is required to open a tiered database, because a forgotten flag
must never open a database without its settled partitions. When a volume first attaches to the tier, `requires=tier`
and a volume UUID (`vuuid=`) are written to its `META` file; older binaries and `NO_EC=1` builds refuse to open it.

You can optionally list the tier stores in `tsdbd.properties` (or with repeatable `--tier`) as a startup guard, and
configure the decoded granule cache budget:

```properties
storage.dir = /nvme/db
storage.tiers = /hdd0/tsdb-tier:hdd, /hdd1/tsdb-tier:hdd, /hdd2/tsdb-tier:hdd, \
                /hdd3/tsdb-tier:hdd, /hdd4/tsdb-tier:hdd, /hdd5/tsdb-tier:hdd
storage.tier.cache = 8G
```

`storage.tiers` never creates or adds a store; at startup `tsdbd` verifies that its paths and classes match the active
(non-lost, non-shrinking, non-removed) stores in `TIERS`, and refuses to start on any difference.

### Live changes while `tsdbd` is running

Once a database is already open with an active tier, `tsdb DIR tier add`, `tsdb DIR tier set`, and `tsdb DIR tier lost`
can run beside the running writer. The writer's background tier worker checks `TIERS` every second and applies new
stores, updated settings, replacements, and shrinks without a restart. If `tsdbd` started before the database had an
active tier (for example, before the first two non-single-copy stores were added), restart `tsdbd` once the stores are
added so the tier opens.

## Defaults and settings

### Default redundancy by store count

When `--redundancy` is not set explicitly, the default geometry follows the number of active stores:

| Active stores | Default redundancy | Notes |
|---|---|---|
| 1+ stores, all `--single-copy` | `1+0` | Redundancy provided underneath (ZFS, RAID, NFS/ZFS) |
| 1 store (not `--single-copy`) | inactive | Tier stays inactive until a second store is added |
| 2 stores | `1+1` | Mirror |
| 3 to 5 stores | `(n - 1)+1` | `2+1`, `3+1`, or `4+1` |
| 6 or more stores | `4+2` | Survives two lost stores |

Each settled object records its own `(k', m, cell, length)` in its `.ec` index and chunk headers. Changing redundancy
with `tsdb DIR tier set --redundancy K+M [--volume N]` applies immediately to new objects and triggers background
re-encoding of older objects whose geometry differs.

### Timing, capacity, and pacing defaults

All persistent tier settings live in `TIERS` and can be changed database-wide or per volume (`--volume N`) with
`tsdb DIR tier set`:

| Setting | CLI flag | Default | Meaning |
|---|---|---|---|
| Settle age | `--after DUR` | `7d` (`604800s`) | Wait this long after a partition's end (or newest row for coarse classes 1–3) before settling |
| Early-settle mark | `--high-pct N` | `80`% | Above this fill on the volume's filesystem, settle finished partitions early, oldest key first |
| Insert-refusal mark | `--young-high-pct N` | `90`% | Above this fill on the volume's filesystem, refuse inserts (`TSDB_WHY_FULL`); clears below `young_high_pct - 2` |
| Rebuild pace | `--rebuild-mb-s N` | `200` | Pace cap (MB/s) for store replacement rebuilds and `--shrink` re-encodes |
| Scrub pace | `--scrub-mb-s N` | `50` | Pace cap (MB/s) for background chunk scrub |
| Scrub interval | `--scrub-days N` | `14` | Target days between full scrub passes per store (`0` disables periodic scrub) |

Fixed engine defaults and process-level settings:

- **Decoded granule cache** (`storage.tier.cache` / `--tier-cache-mb` / `tsdb_tier_set_cache_bytes`): 10% of physical
  RAM (fallback 256 MiB). Set per process, not in `TIERS`.
- **Store put cap**: `95`% filesystem fill. Regular settles stop putting chunks to a store above 95% full; rebuild and
  heal puts may use the remaining 5%.
- **Kept source copies (`keep_s`)**: `24h` (`86400s`). After a settle swap, the source `.part` is renamed to
  `settled_*.part`, verified by a background byte digest against the chunk object, and kept for 24 hours from the
  settle timestamp before release.
- **Coarse-partition settle pace (`coarse_s`)**: `7d` (`604800s`). Multi-day partitions (classes 1–3) settle part by
  part at most once per `coarse_s`.
- **Retired chunk retention (`obsolete_part_lifetime_s`)**: `480s` (8 minutes). Retired `.ec` files and their chunks
  are kept this long before deletion so followers finishing reads do not lose chunks in flight.
- **Store stall timeout**: `10s`. Each active store is probed every second with a 4 KiB `O_DIRECT` write, `fdatasync`,
  and read-back (`<store>/PROBE.tier`); a probe stuck for 10 seconds marks the store `stalled` (`DOWN`).
- **Automatic re-encode**: combines a partition key's settled objects when it accumulates more than 4 combinable `.ec`
  objects, more than 10% tombstoned rows, or objects in an older geometry.

## Single-copy stores on ZFS or NFS

When the underlying storage already provides redundancy (a ZFS pool, hardware RAID, or an NFS export backed by ZFS),
erasure coding across stores is unnecessary. Add one or more stores with `--single-copy`:

```sh
tsdb /nvme/db tier add /mnt/nfs-hdd0/tsdb-tier:hdd --single-copy
tsdb /nvme/db tier status
```

Rules for `--single-copy`:

- Redundancy `1+0` (`m = 0`) is permitted only with `k = 1` and only when **every** active store in `TIERS` is marked
  `--single-copy`. Mixing `--single-copy` and plain stores requires `m >= 1`.
- Each settled object is stored whole as a single chunk (`k' = 1`, `m = 0`) on one store selected by object-id hash.
  If you add multiple `--single-copy` exports, losing one export loses only the objects hashed onto that export, not
  every object in the database.
- `tier status` reports `margin: underneath`.
- Put each NFS tier mount under its own `--health-domain` (`health.domains` in `tsdbd.properties`) so an unreachable
  NFS server marks that tier store `DOWN` immediately without quiescing local volumes or stalling local writes.
- Background scrub still verifies every chunk's checksums on `--single-copy` stores, but operates report-only: damaged
  chunks increment `tsdb_tier_scrub_unrepairable_total` and fire `TsdbTierScrubUnrepairable` without modifying or
  deleting the chunk.

## Reads, late rows, deletes, and scrub

- **Queries**: time-bucket queries answered from granule statistics read only the `.ec` index on the fast disk. Queries
  that decode granules fetch only the needed byte ranges with `pread` (tier files are never `mmap`'d) through the
  decoded granule cache, verifying every granule checksum and cell sum even when `storage.verify.checksums = false`.
  If a store is `DOWN` or a cell fails verification, the reader decodes from `k'` surviving stores automatically.
- **Late rows and deletes**: rows arriving for an already-settled partition go into new young `.part` files above the
  `.ec` parts; deletes write tombstones as usual. Normal merges never touch `.ec` parts; background re-encode combines
  fragmented objects and reclaims tombstones.
- **Settle gating**: on a single server, settles pause while any active store is not `UP` (no degraded settle) and
  resume automatically once every store is `UP` again.
- **Background scrub**: runs on the low-priority (`nice 19`) tier worker, walking active `UP` stores oldest-first
  every `scrub_days` at `scrub_mb_s`. Progress and counters persist in `<store>/SCRUB` so restarts resume mid-pass.
  Scrub yields immediately to rebuilds, shrinks, and store attaches. On EC stores (`m > 0`), damaged data or parity
  cells are repaired automatically from `k'` verified survivors.

## Drive failures and recovery runbook

### 1. Transient store outage (I/O error, stall, or `--health-domain` DOWN)

An in-service I/O error, a probe stalled past 10 seconds, or a `--health-domain` covering the store's path going `DOWN`
marks the store `DOWN` (`ioerr` or `stalled`) at runtime:

- Reads decode around the `DOWN` store while at least `k'` positions of an object remain reachable.
- If fewer than `k'` positions are reachable while a store is transiently `DOWN`, reads return `TSDB_ERR_UNAVAILABLE`
  (retryable), never `TSDB_ERR_IO`.
- Settles and chunk deletes pause (`tsdb_tier_settles_waiting` rises); local writes and statistics continue normally.
- There is **no automatic permanent eviction** on a single server. When the store passes 3 consecutive fast probes (or
  its health domain recovers), the tier worker verifies the store's `id` and `db_id`, heals any missing or damaged
  chunks, sweeps missed deletes, and resumes settles and GC.

### 2. Permanently lost drive within redundancy (`<= m` stores lost)

Check store indices, IDs, and paths with `tsdb /nvme/db tier status`. Every `tier lost` command accepts `STORE` as a
0-based index (`0` or `s0`), a 32-hex store ID, or the store's path.

**Replace the drive (recommended — keeps capacity and redundancy):**

```sh
tsdb /nvme/db tier lost 2 --replace /hdd-new/tsdb-tier:hdd
```

This records `LOST` and `REPLACED_BY` in `TIERS`, binds the replacement store to `db_id`, places a `REBUILDING` marker
in the new store, and wakes the running writer's tier worker to rebuild all positions of slot 2 from surviving stores at
`rebuild_mb_s`. If `tsdbd` restarts or loses power mid-rebuild, the `REBUILDING` marker resumes the rebuild cleanly on
the next open. Watch progress with `tsdb /nvme/db tier status` (or `--json`).

**Shrink instead of replacing (only when remaining stores still satisfy `k + m`):**

```sh
tsdb /nvme/db tier lost 2 --shrink
```

This marks slot 2 `LOST` and `SHRINKING`. The tier worker re-encodes every live `.ec` object that placed a chunk on
slot 2 onto the remaining active stores at `rebuild_mb_s` and marks slot 2 `REMOVED` once zero live objects remain.
`tier lost --shrink` is refused if the remaining stores are fewer than `k + m` of the global or any per-volume
redundancy setting; lower redundancy first with `tier set --redundancy` if needed.

### 3. Lost drives past redundancy (`> m` stores lost, or a lost `--single-copy` store)

An object with more than `m` positions on `LOST` stores cannot be decoded:

- Data reads of affected partitions return `TSDB_ERR_IO` naming the affected partition.
- Statistics queries over the intact local `.ec` index still answer.
- Young `.part` files and unaffected `.ec` partitions remain readable and writable.

**Salvage runbook:**

1. Stop `tsdbd` (offline `tier salvage` takes the database and store locks and refuses to run while a writer holds them).
2. Confirm that every genuinely dead drive is declared `LOST`, and wait for any transiently `DOWN` drive to come back.
   `tier salvage` refuses while any non-`LOST` store is `DOWN` (it might still recover) unless `--force` is passed.
3. Run salvage across all partitions (or one partition key with `--partition P`):

```sh
tsdb /nvme/db tier salvage
# or a single partition key:
# tsdb /nvme/db tier salvage --partition 20500
```

   For each unrecoverable `.ec` part, salvage first checks whether an intact local `settled_*.part` copy still exists
   and restores it in place with zero rows lost. Truly unrecoverable `.ec` parts are retired atomically via the salvage
   intent (`X` lines), keeping all young `.part` files and unaffected `.ec` parts, invalidating the latest table for
   affected series, and printing the count of restored parts, retired parts, partitions, and upper-bound rows lost.
4. Replace each `LOST` store before starting `tsdbd` again so the tier meets `min_up` and settles can resume:

```sh
tsdb /nvme/db tier lost 2 --replace /hdd-new-a/tsdb-tier:hdd
tsdb /nvme/db tier lost 3 --replace /hdd-new-b/tsdb-tier:hdd
```

5. Start `tsdbd` and verify rebuild completion with `tsdb /nvme/db tier status`.

### 4. Clearing a `DIVERGED` single-server volume

If a background digest check finds a logical mismatch between a kept `settled_*.part` copy and its settled `.ec`
object (with intact checksums on both), the volume writes a durable `DIVERGED` marker and answers
`TSDB_ERR_UNAVAILABLE` (`TSDB_WHY_DIVERGED`) across restarts.

Inspect and recover by stopping `tsdbd` and unsettling the diverged keys back to local `.part` files:

```sh
tsdb /nvme/db volume diverged                  # inspect (pass VOL_PATH for an extra volume)
tsdb /nvme/db volume diverged --clear          # restores .part files and clears DIVERGED once durable
```

`volume diverged --clear` restores each `.ec` part of every diverged key from its verified `settled_*.part` copy (or
decodes it from the tier with full sum, digest, and granule verification), swaps the `.part` files back in place via
intent v5 `U` lines, and removes `DIVERGED` only after the swap is durable on disk.

## FULL backpressure and alerts

A single-server tiered database protects its fast filesystem from hitting `ENOSPC` (which fail-stops the WAL) in three
stages:

1. **`tier_high_pct` (default 80%)**: the settle worker stops waiting for `tier_after_s` and settles finished
   partitions immediately, oldest key first.
2. **Tier store put cap (95%)**: once a tier store exceeds 95% full, regular chunk puts stop (`CHUNK_ERR_FULL`) and
   `tsdb_tier_settles_waiting` becomes non-zero while the fast disk absorbs new writes.
3. **`young_high_pct` (default 90%)**: a per-volume sampler sets an atomic `FULL` flag (`tsdb_volume_full{volume} == 1`).
   Inserts, appends, and non-empty range replaces to that volume are refused at the route with `TSDB_ERR_UNAVAILABLE`
   (`TSDB_WHY_FULL` in `tsdbd`) before touching the WAL. Deletes (including `replace_range` with 0 rows) and
   `drop-before` continue to work so you can free space. `FULL` clears automatically once filesystem usage drops below
   `young_high_pct - 2` (default 88%).

### Prometheus metrics and alert rules

Enable `--metrics-port` on `tsdbd` (see [metrics.md](metrics.md#local-tier-metrics-single-server-r15)) and load
`dist/alerts/tsdb.rules.yml` into Prometheus:

| Alert | Severity | Condition | Action |
|---|---|---|---|
| `TsdbTierFull7d` | warning | Store projected to reach 95% within 7 days (1-day slope) or already `>= 95%` | Add tier stores with `tier add` or tighten retention before settles stall |
| `TsdbTierStoreDown` | critical | `tsdb_tier_store_state` is neither `0` (up) nor `5` (replacing) for 1m | Check drive/mount/NFS health; run `tier lost --replace` or `--shrink` if dead |
| `TsdbInsertsRefusedFull` | critical | `tsdb_volume_full == 1` | Restore tier settling (fix DOWN/full stores), add storage, or drop old data |
| `TsdbTierScrubUnrepairable` | critical | `increase(tsdb_tier_scrub_unrepairable_total[24h]) > 0` | Check `tsdbd` logs for affected volume/object/partition; run `tier salvage` if needed |
| `TsdbTierScrubOverdue` | warning | `tsdb_tier_scrub_oldest_age_seconds > 28d` for 1h | Check for blocked rebuild/shrink, DOWN store, or overly low `--scrub-mb-s` |

## Followers (`--follow`) and read-only opens

A read-only or follower `tsdbd` (`--read-only` or `--follow`; see [ha.md](ha.md)) opens a `TIERS` database with a
read-only tier automatically:

- Tier stores are opened read-only (`CHUNK_OPEN_RDONLY`): no `flock`, no `EPOCH` writes, no `tmp_*` cleanup, no chunk
  deletes, and no background health, rebuild, or scrub worker.
- Every non-`LOST` store listed in `TIERS` must be reachable at the **exact same path** with matching `store_id` and
  `db_id`, or the follower refuses to start naming the store. Because a follower on another host cannot reach a
  writer's local drives, remote followers require the tier stores to live on shared storage (such as NFS exports
  mounted at the same paths on both hosts).
- The writer retains retired `.ec` chunks for `obsolete_part_lifetime_s` (default 480 s) so followers finish in-flight
  reads safely.
- A follower keeps the store set loaded at open: if a store is replaced later or encounters a runtime I/O error, the
  follower decodes around it while `k'` positions remain and picks up the replacement when reopened/restarted.

## Embedded bindings and limits

- **Embedded bindings (Rust, Python, Java)**: on x86-64 (unless compiled with `NO_EC=1`), the embedded libraries
  compile the erasure codec (`TSDB_EC`), so a tiered database created with the CLI opens and serves queries and writes
  embedded unchanged. Non-x86-64 builds and `NO_EC=1` builds refuse to open a `TIERS` / `requires=tier` database.
  Tier administration is not exposed in the language bindings — use the `tsdb` CLI (or the C API in `include/tsdb.h`:
  `tsdb_tier_add`, `tsdb_tier_add_dir`, `tsdb_tier_lost`, `tsdb_tier_salvage`, `tsdb_tier_set_cache_bytes`;
  `tsdb_tier_set` and `tsdb_tier_status` are CLI-only in 0.11.0).
- **No `tier remove`**: there is no command to unsettle a whole database and remove the tier in 0.11.0. To change
  redundancy, use `tier set --redundancy` (background re-encode migrates existing objects); to remove a dead store
  without replacing it, use `tier lost STORE --shrink`.
- **Shared-mode and cluster volumes**: cluster volumes (both replicated and `--mode shared`) refuse a local `TIERS`
  tier. Replicated cluster volumes use cluster erasure coding (`--redundancy ec=k+m` with `--disk`); shared-mode
  volumes have no EC tier in 0.11.0.
- **Format-8 upgrades**: a database upgraded in place from format 8 (`tsdb DIR upgrade`) cannot add a tier while
  `parts_v8=1` remains in `META`; run `tsdb DIR optimize` first to rewrite v8 parts.
- **Store limit**: at most 64 tier stores per database (`TIERS_STORES_MAX`).
- **Test hook**: `TSDB_TEST_TIER` (with `TSDB_TEST_TIER_I_KNOW=1`) is a test-only override for timing and cache
  parameters in test harnesses; beside a `TIERS` file, `dirs=`, `k=`, and `m=` are refused. Production deployments
  never set `TSDB_TEST_TIER`.
