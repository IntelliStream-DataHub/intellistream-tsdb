# Statistics per series and per tenant

Status: design, not implemented. Branch `stats-design`, written on 2026-11-06 from main at e33d87f and revised the
same day after an adversarial review (findings H1-H4, M1-M10, L1-L10; §11 says where each one is answered).
Code references are `file:line` at e33d87f.

The user's request (2026-11-06): "we also need statistics for each timeseries and tenant, like compression ratio,
logical disk space, cache hit rate, and more if you have suggestions, performance is more important than accuracy
here".

Related designs: [design-auth.md](design-auth.md) (tenants are name prefixes, tenant admins, rights),
[design-latest.md](design-latest.md) (the latest-value table and its hit counters, protocol 14),
[design-cdc.md](design-cdc.md) (feed slots and their lag, protocol 15), [design-ha-modes.md](design-ha-modes.md) and
[design-cluster.md](design-cluster.md) (volumes on failover pairs and on replicas; format 9 in step S0),
[metrics.md](metrics.md) (the Prometheus port).

## Summary

- **Two kinds of numbers, counted in two places.** What a series or tenant *stores* (rows, bytes per column,
  compression, time range, parts and granules) is read from the granule index, which already holds a row count and
  the compressed size of every granule's timestamps and values. What a tenant *does* (queries, inserts, rows read,
  errors, denied requests) is counted on the request path, per thread, under the tenant of the user who asked.
- **Nothing on the append path.** `tsdb_append`, the memtable, the WAL, the part writer and the merge get no new
  code. The two functions that change the part list (`db_parts_insert_locked`, `db_parts_remove_locked`) each push
  one event into a ring, under the `db->lock` they already hold. A background thread, the *sweeper*, reads a new
  part's index once, while its pages are still in memory, and holds the part only for that walk.
- **Per tenant, storage is kept per partition**: one small vector per (partition, tenant). A partition that retention
  retires is subtracted without reading anything. After a merge, the partition's vector is recomputed from its
  index, at most every 30 s and within a throttle. Tenant totals are always the sum of what is stored, so they cannot
  drift or go below zero.
- **Per series, nothing is kept in memory.** A series' storage numbers are computed when asked, from its index runs,
  with a cost bound per request. A small "hot series" list (space-saving top-k, sampled) names the busiest series.
- **A tenant is a configured name prefix** (`--tenants FILE`, or by default every `prefix:` that a tenant admin holds
  `admin` on). Storage belongs to the tenant of the series' name, requests to the tenant of the user. A list change
  moves only the series whose tenant changes.
- **Exposure:** two new ops, `STATS_SERIES` (46) and `STATS_TENANTS` (47), in their own protocol version (the next one
  free on main when they merge, 16 if 13, 14 and 15 are in by then); `tsdb DIR stats series|tenants|hot` in the CLI; C
  API calls; the four clients and the embedded bindings in one commit; new server-wide and per-volume metrics. Per-tenant
  numbers are never on the metrics port (it has no login): only the STATS ops give them. Never a label per series.
- **Format 9 (S0):** one optional 8-byte field in the part header (when the part's newest rows were written), so the
  last write time survives a reopen. Step S0 of design-ha-modes.md §7 carries it (decided 2026-11-06). Nothing else is persisted: counters start at zero with each process, and storage
  numbers are rebuilt from the indexes, following the open.
- **Budget, as a rule:** stats on against stats off moves insert throughput, query throughput and query p99 by less
  than 1%. The sweeper is capped by its throttle (at most about 6% of one core at the default, under 1% at the
  100 M-row benchmark load). When there is more work than the cap allows, the numbers get older; the sweeper does
  not use more CPU.
- **A fix comes first (step 0).** The review found an existing bug: with `obsolete_part_lifetime_s <= 0`, a part
  that retention or `drop_before` removed comes back after a crash if anything still held a reference to it.

## 0. What exists (checked)

| What | Where |
|---|---|
| Per-query numbers of the calling thread: rows decoded, rows from granule statistics, memtable rows, bytes read, parts, granules, granules from statistics | `tsdb_query_stats`, include/tsdb.h:656-673; accumulated in `qstats` with relaxed atomics, src/internal.h:121-134; folded in `qs_end`, src/volumes.c:1860-1880 |
| Logical bytes of a query are `rows x (8 + type width)` | src/volumes.c:1872 |
| Database totals (parts, partitions, rows, bytes, `merges`, `merged_rows`) and per-volume gauges for the metrics port | `tsdb_stats` include/tsdb.h:683-700; `tsdb_volume_stats` :704-719; `merges`, `merged_rows` from src/db.c:1029-1030 |
| `tsdb_get_volume_stats` walks the list under `db->lock` and takes no references; `db_snapshot` copies the list with a reference per part | src/db.c:1002-1028; src/db.c:176-192 |
| The part list changes only in `db_parts_insert_locked` (callers: inserts, flushes, merges, deletes) and `db_parts_remove_locked` (via `db_retire_locked`), both with `db->lock` held; a follower swaps its whole list at refresh | src/db.c:102-136; src/insert.c:633, src/memtable.c:803, src/merge.c:463, src/delete.c:168, src/manifest.c:144; src/manifest.c:579-600 |
| With `obsolete_part_lifetime_s <= 0` a retired part is not renamed; it is unlinked when its last reference goes, and the open lists parts by scanning the directory | src/manifest.c:123-127; src/part.c:245-253; src/db.c:434-509 |
| `part_open` hashes the header, the whole index and the trailer, so the open already reads every index | src/part.c:222, `part_hash` :60-67 |
| The `STATS` op (10): `tsdb_stats` as 8 x u64, superuser or read on `all` | include/tsdb_protocol.h:40, 212; server/auth.c:85, 910; server/tsdbd.c:1833-1840 |
| The CLI's `stats` assumes 12 bytes per raw row (float32) for every type; the CLI opens read-only for commands that do not write, so it runs beside a running tsdbd | tools/tsdb-cli.c:818-829; :759-764 |
| Protocol 13 (HA bodies, TOPOLOGY 48 and STATUS 49), ops 1-38 as before; design-latest takes 39 and protocol 14; design-cdc takes 40-45 and protocol 15 | include/tsdb_protocol.h:192, 204-239; design-latest.md L14; design-cdc.md:744-784 |
| The largest frame is 256 MB; a larger reply closes the connection | include/tsdb_protocol.h:195; server/tsdbd.c:647-651 |
| The metrics thread: global atomic counters, hooks that return at once when the port is off | server/metrics.c:68-80, 110-190; server/metrics.h:37-51 |
| Metrics never carry user or series names; per-volume gauges carry the path | docs/metrics.md:5-7; design-auth.md A21 |
| Tenants are byte prefixes of series names; tenant admins hold `admin` on a selector; users of a tenant are named under it | design-auth.md A7, A9 |
| Per-user rights map, one byte per local id in a radix table; a user's state is replaced when its grants change | server/auth.c:98-119; design-auth.md A15 |
| tsdbd opens the database first and starts login after (`auth_init` needs the handle) | server/tsdbd.c:3192, 3201 |
| Granule index entry: count, ts_min, span, offset, `ts_len`, `val_len`, nulls, ...; one *run* per series (`rser`, `rstart`); `gix_run` finds a series' entries by binary search | src/internal.h:633-650, 697-711 |
| Index reads are clamped to the mapping, so a damaged index gives wrong numbers, never a read outside it, and damage that arrives after the open can show | src/internal.h:687-695 |
| Part: rows, min/max ts, `written` (after a reopen: the file's mtime), `level`, `dropped`, index length | src/internal.h:726-757; src/part.c:182 |
| Level 0 parts are written by inserts and flushes; merges write level + 1 | src/merge.c:62, 436 |
| `dropped` is set when retention retires a partition or `drop_before` drops it | src/merge.c:573; src/db.c:988 |
| Memtable: per-series slots indexed by id, `created_ms`; the active memtable's slots are reallocated under `buf_lock`; followers tail the WAL into memtables (`db->tails`) | src/internal.h:1145-1155; src/memtable.c:219; src/manifest.c:5-11 |
| The series dictionary keeps a pointer, a malloc'd name, a type byte and hash slots per series (roughly 60 bytes) | src/internal.h:980-1002 |
| Parts are read through `mmap`; there is no cache of decoded granules and no `mincore` call anywhere | src/part.c:174; `grep mincore src` is empty |
| tsdbd's worker runs `handle`, then the request metrics and the copy of the last query stats; error statuses are counted in `reply` | server/tsdbd.c:1869-1880; :653 |
| A cluster fans database-wide ops (STATS among them) out per volume to the leaders; the cold tier keeps the index local | design-cluster.md:680, 79 |
| Feed slots have per-slot lag metrics | design-cdc.md:588-589 |

## 1. Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| ST1 | **Storage numbers are physical**: rows and bytes as stored in parts and memtables, including rows that a newer write, a tombstone or retention hides until a merge removes them. Exact live rows are a flag (`EXACT_ROWS`, at most 1,024 ids): the rows, nulls included, that a raw query of the whole range would return, counted through the query's snapshot windows (from granule counts where nothing overlaps, decoded where it does), run quietly: it does not change the thread's `tsdb_last_query_stats`, the query counters or the hot lists. | Live rows always (a count per series, deduplicated across overlapping parts); an aggregate's `count` (it leaves out nulls, include/tsdb.h:317) | A live count decodes every granule a tombstone touches and every overlap: a query's cost. "Performance before accuracy": the physical count is free from the index and converges on the live count as merges settle. |
| ST2 | **Per-series storage numbers are computed when asked, with a cost bound.** Per volume, the requested local ids are sorted. A part whose series range (its first and last run) misses them is skipped. In every other part the walk takes the cheaper of a binary search per id (n x log2 runs) and one merge pass over the run table (runs + n). A request may spend at most 10^8 index probes (about a second of one core); past that the reply ends early (`done` < n) and the client sends the rest. Runs on a worker under `request_threads`, and CANCEL stops it. | A maintained record per series; a binary search per (id, part) with no limit | At 10^9 series even 64 bytes each is 64 GB. Without the bound, 65,536 ids x 3,000 parts x 23 probes is minutes of work that any reader could ask for (review H1). |
| ST3 | **The sweeper learns of every list change from events.** While stats run, `db_parts_insert_locked` pushes ADD (a reference on the part, its level, and whether the WAL replay at open made it) and `db_parts_remove_locked` pushes REMOVE (the part's identity, and `dropped`) into a bounded ring, single producer under `db->lock`, single consumer the sweeper. The sweeper drains it each second, sooner when it is half full. It releases the reference of an ADD as soon as it has walked the part, or at once without a walk when the part is already `dropped`. If the ring overflows, the sweeper reconciles against the list (copied under `db->lock` without references) and flags ingest as approximate for that interval. A follower gets no events from its list swap; its sweeper diffs its list after each refresh. | Sampling the list each second (a part merged away within the second is never seen, so ingest is not exact: M2); a reference on every counted part (keeps removed parts on disk: H4); hooks in the part writer or the merge | Two O(1) stores under a lock already held, a few per flush or merge (never per row). The events change nothing about which parts are listed or in what order. A level-0 part is walked right after it is written, while its pages are in the page cache. |
| ST4 | **Tenant storage is kept per (volume, partition).** Each counted part has a 64-byte record: identity, partition key, level, the tenant-map generation it was counted under, and its column totals. Each partition has a sparse vector with a 56-byte entry for each tenant that has rows in it: rows, nulls, logical, timestamp, value and index bytes, granules. An ADD of a level-0 part adds the part's split to the vector. A merge output or any REMOVE marks the partition stale. The sweeper then recomputes the partition: it walks the partition's listed parts, holding a reference only while it walks each one, and replaces the vector. It does this at most once per `stats_recompute_s` (30 s) per partition, and within the throttle. A retired or dropped partition is subtracted whole, with no walk. | A tenant vector per part (10^4 parts x 10^4 tenants x 40 B = 4 GB); walking a part again when it leaves the list (needs its mapping, so a reference; reads months-old cold indexes of retired partitions; may attribute differently the second time: H3, H4, M1) | Memory follows partitions x tenants present (§3.6). Retention costs nothing. A recompute reads the index the merge just wrote, plus the partition's other parts: an index is about 1% of the data that the merge itself read and wrote. Replacing a vector wholesale cannot drift. |
| ST5 | **A tenant is a configured prefix.** In tsdbd: `--tenants FILE`, one prefix per line, reloaded on SIGHUP. Without it, the tenants are the prefixes of every `admin` grant on a `prefix:` selector in USERS. `admin` on `name:` or `glob:` defines no tenant, and `admin` on `all` is superuser-like. The longest matching prefix wins (`acme/eu/` before `acme/`); unmatched series and users belong to tenant `(none)`. The list reaches the engine in `tsdb_options` at open: tsdbd reads the file, or reads USERS with server/users.c, before `tsdb_open`. Later lists go through `tsdb_set_tenants`. tsdbd passes a new list only when the *set* of prefixes changed and has stayed the same for 30 s. Tenant numbers are never reused within a process; at most 65,535 prefixes. | The first name segment up to `/`; a user's grants; tenant ids stored with each series; recomputing the default on every USERS change | A7 keeps names free of any separator convention. Grants overlap and change, so a series would belong to several users. Storing a tenant per series in SERIES is a format change, and tenants are an operator's view that should be changeable. Tenant admins already define prefixes (A9). A grant that is not a tenant change (a user's read grant, a password) changes no prefix and costs nothing. |
| ST6 | **Storage and ingest are attributed by data, requests by caller.** A series belongs to the tenant its name matches. A user belongs to the longest tenant prefix P that its name starts with *and* that holds every one of its grants (`prefix:` and `name:` under P, `glob:` whose literal head is under P); otherwise to `(none)`. The user's tenant is computed when its state is built (server/auth.c:108-119) and stored there. | Attributing queries to the tenants of the series they read; attributing users by name alone | A query's cost belongs to who asked; the data's cost to whose data it is. The request path does no lookup per series, so QUERY_MULTI over a million ids is one update. By name alone, a superuser could create `acme/x` with grants outside `acme/`, and acme's admin would see its requests (L5). A superuser's requests (the Pulsar sink, say) count under `(none)`; its rows still count under each series' tenant in storage and ingest. |
| ST7 | **The tenant map**: per volume, 2 bytes per local id (1 byte while there are at most 255 tenants), in pages of 65,536 ids; a page whose ids all have one tenant is stored as that number in the page table and allocates nothing. The sweeper fills it in the background from the names, using a byte trie of the prefixes (about 10-20 ns per name; the names are already in RAM). Before it walks a part, it extends the map to the part's largest id. A part that names an id the dictionary does not have yet (a follower ahead of its SERIES) is put off to a later tick. | Matching names on the request or write path; a hash from name to tenant; filling the map while SERIES loads | A lookup in id order is about 2 ns and takes no lock. Filling during the load would add 10-20 s to the open of a 10^9-series volume, and the open is on the failover path (design-ha-modes.md). **Plan for the worst case, 2 GB per 10^9 series (1 GB with at most 255 tenants)**: the Pulsar sink creates series as it first sees them, across all tenants' topics, so ids interleave by tenant. |
| ST8 | **Ingest per tenant comes from level-0 ADD events** (rows, logical bytes, the active-series HLL, the hot writes list). This is exact on the writer. Flushes of the WAL replay at open are flagged in the event and not counted: the previous process already acknowledged those rows. A follower reports no ingest: it only sees the parts that happened to be listed at its refreshes. | Counting in `tsdb_append` / `mem_insert` | The append path runs at 10 M calls/s on one thread (AGENTS.md "Durable writes"), about 100 ns a call: 5 ns more would cost 5%. At the part boundary the count lags by one flush and costs nothing on that path. |
| ST9 | **Query counters live in per-thread blocks, one per (thread, handle).** A thread finds its block in a small thread-local array keyed by a handle number that is never reused (a process-wide counter), so two handles in one thread stay apart. Blocks belong to a registry that the handle and every block hold a reference on: `tsdb_close` drops the handle's reference, and a thread that exits later folds its block into the registry's retired total and drops its own. A thread that outlives the handle never touches freed memory. Blocks are 128-byte aligned (Intel prefetches pairs of lines). The owner writes with relaxed stores and no read-modify-write; readers sum. If allocating a block fails, that thread simply does not count: a query never fails because of stats. | Global atomics (as server/metrics.c:68-77 does); sharding by a hash of the thread; one `__thread` pointer per process | An uncontended `lock xadd` costs 5-20 cycles and a contended one far more; ten of them per query would be measurable on the 600 k/s embedded point lookups. A block owned by one thread needs no atomics on x86-64 or arm64. One pointer per thread mixes up handles and leaves a dangling destructor after close (M6). |
| ST10 | **Hot series, three lists.** *Reads*: sampled weighted space-saving, k = 256 per thread block. A query is sampled with probability 1/8 by a per-thread xorshift geometric skip, not 1 in 8 by a counter, which a round-robin client would alias (weight 8). *Writes*: space-saving over the series of level-0 parts, weighted by rows, fed by the sweeper, not sampled. *Bytes per row*: not a frequency top-k, but a min-heap of the k worst series by bytes per row over runs with at least one full granule in parts of level > 0, deduplicated by id. Each list has two windows, the current and the previous 60 s; the owner rotates them at its next update, and readers ignore a window by its timestamp once it is older than two periods (an idle thread never rotates). A seqlock per window keeps a reader from seeing a torn (id, estimate, error) triple. The hot-list memory of a block is allocated on the thread's first sample. | Exact counters per series; count-min sketch | Exact counters cost 16 bytes x 10^9. Count-min answers "how much for this series" but cannot list the top without a candidate set. Space-saving lists them with a known error. Lists of several threads are combined by the mergeable-summaries rule: an id missing from one thread's list counts that thread's minimum. Then every series above W / k appears, and each estimate is high by at most W / k plus the sampling error (§7 tests both). |
| ST11 | **Active series per tenant: HyperLogLog**, 1,024 registers (about 3% standard error), one per tenant per window (the current and the previous hour), fed by level-0 ADD walks. The number of series per tenant is exact (from the tenant map). | A bit per series per window | 10^9 bits x 2 windows = 250 MB for a number nobody needs to the last series. |
| ST12 | **Cache hit rates are reported for the caches tsdb has**: granule statistics (granules and rows answered from the index against decoded), memtable rows, the page cache (major faults of the process from `getrusage(RUSAGE_SELF).ru_majflt`, which macOS has too, against bytes decoded; residency of a series or a tenant by `mincore`, on request or sampled, through a shim in src/compat.h because macOS takes `char *`), the latest-value table (design-latest.md §6.5), the rights map (counted per request, never per row). There is no decoded-granule cache to report. | A user-space block cache to make a hit rate measurable; `/proc/self/stat` | Parts are `mmap`'d (src/part.c:174), so the page cache is the cache. Adding a cache just to measure it would cost memory and a copy on every read. `getrusage` is one portable call. |
| ST13 | **Nothing is persisted, except one optional field in format 9.** Counters count from the start of the process. Storage numbers are rebuilt after an open: the sweeper follows the open (§3.1). Format 9 (step S0 of design-ha-modes.md §7, which carries the field; decided 2026-11-06) adds `written_ms` to the part header (not the 64-byte footer): when the part's newest rows were inserted, and for a merge output the newest of its inputs. For a part written before format 9 (no field), after a reopen a series' last write falls back to the part file's mtime (src/part.c:182), which a merge moves, and the reply flags it. | A STATS file updated on every flush; totals in the 64-byte footer | A file written on the flush path is new fail-stop surface and new jepsen work, for numbers that can be rebuilt. The footer's size is fixed by a static assert (src/internal.h:596). Without a written time, "last write" would be wrong after every reopen for a series that a merge rewrote (L1). |
| ST14 | **Two ops**: `STATS_SERIES` (46: ids in, fixed records out) and `STATS_TENANTS` (47: tenants, with optional per-volume rows, per-caller blocks and hot lists, and a resume cursor). Rights: read on each series for the first (DENIED per series, like QUERY_MULTI); for the second, as §4 "Rights". | An op per statistic; a free-prefix op that scans names | Fewer ops, fewer rows in `auth_ops`. A free prefix would scan 10^9 names per request. |
| ST15 | **Protocol: its own version**, the next one free on main when the stats ops merge (16 if 13, 14 and 15 are in by then), never a ride-along with another unreleased version. New ops only, floor stays 12; the CHANGELOG of the release names the version and the floor. Op numbers 46 and 47 unless taken by then. | Riding along with 13 or 14 while unreleased | With a ride-along, one version number on main would mean different sets of ops over time, and a client built in between could not tell (L10). A version number costs nothing. |
| ST16 | **Metrics port**: new server-wide and per-volume metrics only. **No per-tenant metric of any kind, and no `--metrics-tenants` option** (decided 2026-11-06). The port has no login (A21), so per-tenant numbers (and so the names of tenants) are reachable only through the STATS ops, which require login and the rights of §4. Per-tenant request counters are still kept per tenant number (§3.4) for the by-caller blocks of `STATS_TENANTS`. No label per series or per user. | Per-tenant metrics on the port, by default or as an opt-in with a capped, sticky top N | A21: labels on a port without login must not name tenants. The opt-in was offered and not taken. Per-volume gauges carry the path, as before (docs/metrics.md). |
| ST17 | **Stats never block, write or fail-stop.** The ring push is two stores under the `db->lock` the list change already holds. The sweeper takes `db->lock` only to copy one partition's parts with references (as `db_snapshot` does), and the dictionary's `lock` only for a batch of names. A per-series call reads the parts and the flushing and tail memtables under one `db->lock` hold, and the active memtable under `buf_lock` in batches of at most 256 ids (§3.2). `stats_mu` and the registry's mutex are leaves. Stats never write a file, never call `eng_fail`, and keep answering after the engine failed (as queries do). | Exact numbers under `publish_lock`; one `buf_lock` hold for a whole request | Numbers a few seconds old are fine; a stats call that delays a publish is not. Holding `buf_lock` across 65,536 ids would stall the append path. |
| ST18 | **Three settings**, `tsdb_options.stats`: -1 off (no ring, no sweeper, no blocks: hooks stop at one predictable branch); 0, the engine default, *lazy* (query counters on; the sweeper starts at the first tenant or volume stats call, and per-series calls work at once); 1, start with the open (tsdbd's default; `tsdbd --no-stats` sets -1). | On for every open; off by default | The CLI, `tsdb export`, the embedded bindings, tests and benchmarks should not start a thread that reads the whole index (M3). An operator should not have to restart tsdbd to find out which tenant fills the disk. |
| ST19 | **Fix first: a dropped part must not come back after a crash.** With `obsolete_part_lifetime_s <= 0`, `db_retire_locked` renames a `dropped` part to `retired_*` too, as it does with a positive lifetime (the open deletes `retired_*` files, src/db.c:451), and the unlink still waits for the last reference. This is a separate fix with its own regression test and a jepsen run, before step 1. | Leaving it, since only queries hold parts long today | A query running across `drop_before` already opens the window today. The event ring holds an ADD reference for up to a second, so it would widen the window (H4). |

## 2. The statistics

"Exact" means exact for what it measures (physical storage, requests since start). "Stale by" is the most it lags
behind what happened.

### 2.1 Per series (`STATS_SERIES`, `tsdb_series_stats`)

| Statistic | Exact / approximate | Stale by | Source |
|---|---|---|---|
| `rows` stored (parts + memtables) | exact physical; live rows only after merges (ST1) | 0 (flag `MEM_APPROX` if a flush raced twice, §3.2) | index `count` per entry of the series' runs; memtable slots |
| `memtable_rows` (active, flushing and, on a follower, its WAL tails) | exact | as `rows` | memtable slot by id (src/internal.h:1147) |
| `nulls` stored | exact physical | 0 | index `nulls` |
| `logical_bytes` = part rows x (8 + type width) | exact physical | 0 | as `qs_end` (src/volumes.c:1872); part rows = `rows - memtable_rows` |
| `ts_bytes`, `value_bytes` on disk | exact | 0 | sums of `ts_len`, `val_len` |
| `index_bytes` | the series' share: its entries x stride + 8 per run; each part's fixed index overhead (header, field table) goes to the volume's `other_bytes` | 0 | the part's `gix` (src/internal.h:645-650) |
| `tomb_bytes` | exact: 24 x the series' tombstones | 0 | part trailer (src/internal.h:603-608) |
| compression ratio = `logical_bytes` / (ts + value + index + tomb bytes) | derived from part rows and part bytes only, so memtable rows do not inflate it | 0 | |
| `first_ts`, `last_ts` | physical; `first_ts` raised to the retention cutoff; when every stored row is before the cutoff, both are absent (flag `NO_RANGE`) | 0 | first and last entry's ts range; memtable min/max |
| `last_write_ms` | approximate: the newest `written` of a part holding the series, or the memtable's `created_ms` when it holds rows; flag `WRITE_FROM_MTIME` when it comes from a part this process opened rather than wrote (ST13) | one flush | src/internal.h:747, 1154 |
| `parts`, `partitions`, `granules`, `full_granules` (count = the part's granule rows) | exact | 0 | runs and entries |
| `class` | exact | 0 | `tsdb_series_class` |
| `resident_bytes` (flag `RESIDENT`, n <= 1,024) | exact for the pages at the time of the call | 0 | one `mincore` per (part, series): a series' granules are contiguous in a part |
| `reads_est`, `writes_est` (and `reads_err`, `writes_err` for a superuser or read on `all`; else 0 with flag `NO_BOUND`) | approximate (ST10); 0 when not in a hot list | 60 s window | hot lists |
| latest cached (flag `LATEST_CACHED`) | exact | 0 | design-latest's table, when built |
| `live_rows` (flag `EXACT_ROWS`, n <= 1,024) | exact, nulls included (ST1) | 0 | a quiet count through the snapshot windows |

The walk uses the part list under its references, not a query snapshot's entry windows (`sn.gi`/`sn.gj`): it counts
what is on disk, including granules a tombstone touches, and never answers a value. AGENTS.md's rule ("never
`part_find` on `sn.parts`") is about answers; this walk gives none. `EXACT_ROWS` is an answer and goes through the
windows.

### 2.2 Per tenant (`STATS_TENANTS`, `tsdb_tenant_stats`)

By data (storage and ingest), per volume and summed:

| Statistic | Exact / approximate | Stale by | Source |
|---|---|---|---|
| `series` | exact | one tick after creation | tenant map |
| `series_created` (since start) | exact | one tick | tenant map frontier |
| `active_series` (current and previous hour) | approximate, about 3% (ST11) | one flush + one tick | HLL from level-0 parts |
| `rows`, `nulls`, `logical_bytes`, `ts_bytes`, `value_bytes`, `index_bytes`, `granules` | exact physical once the tenant's partitions are recomputed | one tick after a flush; after a merge, until the partition's recompute (30 s, longer when the throttle binds; the reply's `stale_s` is the oldest pending); after an open, until the catch-up ends | partition vectors (ST4) |
| compression ratio, bytes per row | derived | as above | |
| `ingested_rows`, `ingested_bytes` (since start; writer only) | exact, the WAL replay excluded (ST8); approximate over an interval in which the ring overflowed (flag) | one flush + one tick | level-0 ADD events |
| `merged_rows` (rows written by merges, since start); write amplification = (ingested + merged) / ingested | exact per recompute: the rows of level > 0 parts that a recompute sees for the first time. This is the per-tenant split of the volume's existing `merged_rows` (src/db.c:1030), which it sums to once nothing is stale | until the recompute | partition recompute |
| `merge_removed_rows` (rows merges did not carry over: overwritten, deleted, expired) | exact as a sum; not split by cause | until the recompute | per recompute: old vector + level-0 rows added since - new vector (§3.1) |
| `retention_dropped_rows` (whole partitions retired or dropped) | exact when the partition was not stale; otherwise it also counts the rows that the pending merge had removed (the volume-wide number, from the part records, is exact) | one tick | REMOVE events with `dropped` |
| `resident_bytes` | approximate: sampled `mincore` of up to 64 granules per part per 10 minutes | 10 min | sweeper |

By caller (tsdbd only; the user's tenant by ST6):

| Statistic | Exact / approximate | Stale by | Source |
|---|---|---|---|
| requests by op group (query, insert, delete, admin, other) | exact | 0 | worker loop, server/tsdbd.c:1869-1872 |
| query rows covered: decoded, from granule statistics, from memtables; bytes read; result rows; query time | exact | 0 | the request's `tsdb_query_stats` |
| inserted rows and bytes | exact (acknowledged inserts) | 0 | as `metrics_rows_inserted` (server/tsdbd.c:1843-1846) |
| errors (invalid, notfound, io, other); `denied` (whole requests, and QUERY_MULTI and STATS_SERIES series) | exact | 0 | `reply` (server/tsdbd.c:653), the per-series cases |
| failed logins (by the tenant of the name tried) | exact | 0 | login path |
| feed slots owned by the tenant's users and their largest lag | exact | as the feed's metrics | design-cdc.md:588-589, when built |

Hot lists (flag `HOT`, k up to 256 per kind): the hottest series by reads, by written rows and by bytes per row,
filtered to the tenants asked for and to series the user may read. A quiet tenant's hottest series may not reach a
global list. A superuser, or a user with read on `all`, also gets each list's threshold (W / k: every series above it
appears). A tenant admin does not, because the threshold reveals the load of the whole server (L5).

### 2.3 Server-wide and per volume (metrics, `tsdb_volume_stats`)

New per-volume fields: `logical_bytes`, `ts_bytes`, `value_bytes`, `index_bytes`, `other_bytes` (part headers,
footers, tombstones, the fixed part of each index), `stats_parts_counted`, `stats_parts_listed`,
`stats_stale_partitions`, `stats_ring_overflows`. Volume column totals come from the part records, so they are exact
one tick after any change, stale partitions or not. New counters (per-thread blocks, ST9): query granules and rows by
source (`stats`, `decoded`, `memtable`), bytes read, queries; `tsdb_major_faults_total` from `getrusage` at scrape
time (the page cache's misses: compare with `rate(tsdb_query_bytes_read_total) / 4096`); the rights map's hits and
resolutions, counted per request; the sweeper's CPU seconds and the tenant map's bytes. With `--pin-index` the index
is on the heap and never faults, which the metric then shows.

## 3. Where the numbers come from, and what they cost

### 3.1 The sweeper

**Each tick** (1 s, or sooner when the ring is half full), per volume:

1. Drain the ring in order. An ADD whose part is already `dropped`: release the reference, nothing else. An ADD of a
   level-0 part: extend the tenant map to its largest id (or put the part off, ST7), walk it, add its split to the
   partition's vector (or, if the partition is stale, to the partition's pending level-0 sum `A0`), add to ingest
   (unless it came from the replay), the HLL and the hot writes list, store its record, release the reference. An
   ADD of a level > 0 part (a merge output): store a record with its totals (read from the part, no walk), mark the
   partition stale, release the reference. A REMOVE: subtract the record's totals from the volume's column totals.
   If `dropped`, and once no listed part of that partition is left, subtract the partition's vector (plus `A0`)
   whole from the tenants and add its rows to `retention_dropped_rows`. Otherwise mark the partition stale.
2. Recompute stale partitions, oldest first, each at most once per `stats_recompute_s` (30 s), within the throttle.
   Take `db->lock`, copy that partition's listed parts with a reference each (as `db_snapshot`), and release the
   lock. Walk each part and release its reference. The new vector `V'` replaces the old vector `V`. For each tenant:
   totals += `V'` - `V`; `merge_removed_rows` += `V` + `A0` - `V'` (rows); `merged_rows` += the rows of the level > 0
   parts that this recompute saw for the first time. Then `A0` is cleared and the partition is no longer stale. If
   `V` + `A0` - `V'` comes out negative for a tenant, the inputs were damaged after the open: clamp to 0 and log one
   line per partition.
3. Backlog (the catch-up after an open, the moves after a list change, §4): the remaining parts, throttled.

**The invariant** (H3): a tenant's totals are always the sum of the stored partition vectors (and their `A0`). A
vector changes in exactly four ways: a walked part's split is added under the current map; a recompute replaces the
whole vector; a list change moves the numbers of parts counted under the previous map (§4); a drop subtracts the
whole vector. Nothing is ever subtracted that was not added in the same terms, so the totals cannot drift and cannot
go below zero. As a check on damage, each walk compares the sum of its split over tenants with the part's own column
totals; on a mismatch it marks the partition stale and logs one line.

**The walk** of one part: for each run, the tenant from the map (about 2 ns, ids ascending) and the series' type from
the dictionary (for logical bytes); then for each entry `count`, `nulls`, `ts_len` and `val_len` (four `gix_get`, a
few shifts each). Estimated at 5-10 ns per entry and 5 ns per run: a part of 10^6 single-granule runs costs 10-15 ms
of one background core. Totals live behind the leaf mutex `stats_mu`, taken once per part.

**The throttle** (`stats_sweep_mb_s`, default 256 MB/s of index walked, the thread at nice 19) is a hard cap:
256 MB/s is about 8 M entries/s, about 60 ms of CPU a second, or 6% of one core at most. Demand above the cap makes
the numbers older (`stale_s`, `progress`), never the sweeper costlier.

**Steady-state demand** (M4). Each index entry is walked once when its level-0 part arrives, and about once per merge
level in recomputes, plus the partition's other parts when a recompute runs. At the 100 M-row benchmark load this is
well under 1% of one core. A sparse load is the expensive case: 10^8 series at 1 M rows/s, so a series writes once
in 100 s, every row is its own index entry in a level-0 part, and today's partition settles at about one entry per
series (10^8 entries, 3.2 GB of index). That costs about 1 M entries/s for the level-0 walks (0.75% of a core) and,
with one recompute of today's partition every 30 s, about 3.3 M entries/s (2.5%). The merges of that same load decode
and re-encode every row at every level, so the target is stated against them: **sweeper CPU at most 5% of merge CPU**,
measured (§8).

**Open-time catch-up** (M3). `part_open` already reads and hashes every index (src/part.c:222). With `stats = 1`
(tsdbd) and the tenant list in the options, the open pushes each part into the ring as it lists it. The sweeper,
running on another core, walks it right after it was opened, while its index pages are still in the page cache. The
index is then read from disk once, not twice, even when the index is larger than RAM (320 GB of index against 256 GB
of RAM at the target). What remains is CPU: about 75 s of one background core per 10^10 entries. The open itself waits
for nothing. Until the catch-up ends, tenant replies carry `PARTIAL` and `progress`; per-series numbers are right at
once. With `stats = 0` the catch-up runs at the first stats call, from disk.

### 3.2 A per-series request

Cost: for each volume, over the listed parts whose series range meets the request,
min(n_v x ceil(log2 runs_p), runs_p + n_v) probes, where n_v is the number of requested ids in that volume. At most
10^8 probes per request (ST2). For a series of the 100-billion-row benchmark (116 parts, about 10^4 runs each) the
walk costs what a statistics-only whole-range query costs: 0.17-0.23 ms. At 10^7 runs per part it is 23 probes per
part, mostly cache misses: about 1 us per (part, series) for single ids, and the merge pass for large requests.

Memtables (M5): under one `db->lock` hold, the sweeper's event sequence number is read, the part list is copied with
references (as `db_snapshot`), and the slots of the flushing memtables and of a follower's WAL tails are read for the
request's ids, in batches of at most 256 ids per hold (about 5 us each). The active memtable is read under `buf_lock`
(after `db->lock`, AGENTS.md lock order), in batches of at most 256 ids. If the event sequence number moved in the
meantime (a flush listed a part), the memtable reads are done once more; if it moves again, the reply sets
`MEM_APPROX` (a row may count in both places or in neither, for at most one flush).

### 3.3 The query path

At the end of the outermost query (`qs_end`, src/volumes.c:1860), the numbers already gathered in `qstats` go into
the thread's block for that handle: a scan of a thread-local array of up to 8 (handle number, block) pairs (1-2 ns),
then about ten relaxed loads and stores (ST9), 3-5 ns in all. A sampled query (1 in 8 on average, ST10) also updates
the hot-reads summary (a probe of a 512-slot table; on a miss, a min-heap replace with k = 256): about 40 ns, 5 ns
averaged. QUERY_MULTI updates the counters once per call and samples per series (src/multi.c). A thread's first query
on a handle registers a block under the registry's leaf mutex (about 1 us, once).

Budget check against the fastest query there is: embedded single-row lookups at 59,891 queries/s per thread
(17 us each, AGENTS.md "Embedded"): 10 ns is 0.06%.

### 3.4 The request path in tsdbd

After `handle(r)` (server/tsdbd.c:1870), the worker adds to its block under the tenant number stored in the user's
state (ST6): the op group, the query stats it already copies (:1874-1879), inserted rows. Error statuses are counted
in `reply` (:653), which has the connection and so the user. There are 24 counters of 8 bytes per tenant, and a
worker's table grows in pages of 64 tenant numbers as it meets them. A list change gives new users' states new tenant
numbers. Counts already made stay where they were made, so a tenant's counters only grow (ST16).

### 3.5 The write path

No new code in `tsdb_append`, `mem_insert`, the WAL, the flush, the part writer or the merge. Two O(1) pushes go into
the functions that list and unlist parts, a few per flush or merge, never per row. This is the rule that keeps the
10 M appends/s path where it is.

### 3.6 Memory

| | 10^6 series | 10^8 series | 10^9 series |
|---|---:|---:|---:|
| tenant map, planning number (ids interleaved by tenant, as the Pulsar sink creates them) | 2 MB (1 MB) | 200 MB (100 MB) | 2 GB (1 GB) |
| tenant map, best case (series created in runs per tenant) | page table only, 32 B per 65,536 ids | | 0.5 MB |
| per series otherwise | 0 | 0 | 0 |
| per counted part | 64 B | 64 B | 64 B |
| per (partition, tenant present) | 56 B | | |
| per tenant per volume (totals, ingest, merges, retention) | 320 B | | |
| HLL per tenant (2 windows x 1 KB) | 2 KB | | |
| hot lists per thread block, allocated on first sample (3 kinds x 2 windows x 256 x 24 B) | 37 KB | | |
| per-tenant request counters per worker | 192 B per tenant that worker served | | |

(Numbers in brackets: the 1-byte map, while there are at most 255 tenants.) Partition vectors at the extreme: 365
day partitions x 10^4 tenants all present in every partition x 56 B = 204 MB per volume; with classes (7, 28 and
364-day partitions) far fewer. 10,000 tenants on 10 volumes: 32 MB of totals and 20 MB of HLL; 64 workers serving
10,000 tenants: 120 MB at most.

## 4. Tenants

- **Definition** (ST5): a sorted list of prefixes; a name's tenant is the longest prefix it starts with, else
  `(none)`. A user's tenant follows ST6 (its name and all its grants under the prefix).
- **Source**: `tsdbd --tenants FILE` (text, one prefix per line, `#` comments; SIGHUP reloads it). Without it, the
  `prefix:` selectors of every `admin` grant in USERS, read before `tsdb_open` and then from each USERS snapshot.
  Embedded: `tsdb_options.tenants` / `ntenants`, then `tsdb_set_tenants(db, prefixes, n)`. The offline CLI takes
  `--tenants FILE` or reads USERS beside the database.
- **A change of the list** (H2) is applied only when the set of prefixes changed and has been stable for 30 s, and
  only to the series whose tenant changes. A new prefix P takes its series from exactly one tenant T: the one with the
  longest proper prefix of P, or `(none)`. A removed prefix gives its series back to that tenant. The ids that move
  are found by scanning the tenant map for T's ids (2 bytes per id, sequential: about 0.5 s at 10^9) and matching
  only their names. The map is updated and its generation goes up. Then, partition by partition, the parts counted
  under the old generation are walked for the moved ids only (the same choice of binary search or merge pass as
  ST2), and their numbers move from T to P in the partition's vector. A partition that turns stale in the meantime is
  recomputed instead, which does the move with it. Until the move is done, T and P are marked `PARTIAL` and keep
  their old numbers, labelled with the old generation. Every other tenant is untouched and exact. A wholly new list
  is handled the same way, one changed prefix at a time; the worst case is one full walk of the index, throttled.
  Per-caller counters do not move (§3.4).
- **Rollup**: maintained, never on demand (ST4, ST7). At 10^9 series a rollup on demand would walk every name and
  every index entry per request.
- **Rights** (ST14; design-auth.md A16):
  - A superuser or a user with read on `all` sees every tenant, the global progress fields and the hot-list
    thresholds. A tenant it names that is not in the list is `NOTFOUND`.
  - A user holding `admin` on `prefix:Q` sees each tenant whose prefix starts with Q, and gets that tenant's own
    `progress` and `stale_s`. A tenant it names that starts with Q but is not in the list is `NOTFOUND`; anything
    else it names is `DENIED`.
  - Everyone else: `DENIED`. With `--no-auth` every tenant is visible, as data ops are.

## 5. Exposure

### 5.1 Wire

All integers little-endian, as the other ops. Record sizes are asserted in the C client and in `test_server`.

`STATS_SERIES` (46) request: `u32 n` (1 .. 65,536), `u32 flags` (`RESIDENT` 1, `EXACT_ROWS` 2; either with n > 1,024
is `TSDB_ERR_INVALID`), `u64 ids[n]`. Reply: `u32 done`, `u32 0`, then `done` records of 176 bytes in request order.
The records stop early only at the work budget (ST2); the client sends ids `done ..` again. At most 11.5 MB.

| Offset | Field | Offset | Field |
|---:|---|---:|---|
| 0 | i32 status (0, DENIED, NOTFOUND) | 88 | u64 granules |
| 4 | u8 class | 96 | u64 full_granules |
| 5 | u8 type | 104 | i64 first_ts |
| 6 | u16 flags (`MEM_APPROX` 1, `NO_RANGE` 2, `WRITE_FROM_MTIME` 4, `NO_BOUND` 8, `LATEST_CACHED` 16) | 112 | i64 last_ts |
| 8 | u64 rows | 120 | i64 last_write_ms |
| 16 | u64 nulls | 128 | u64 resident_bytes (`RESIDENT`, else 0) |
| 24 | u64 memtable_rows | 136 | u64 reads_est |
| 32 | u64 logical_bytes | 144 | u64 reads_err |
| 40 | u64 ts_bytes | 152 | u64 writes_est |
| 48 | u64 value_bytes | 160 | u64 writes_err |
| 56 | u64 index_bytes | 168 | u64 live_rows (`EXACT_ROWS`, else 0) |
| 64 | u64 tomb_bytes | 176 | end |
| 72 | u64 parts | | |
| 80 | u64 partitions | | |

Every field after `flags` is 0 when `status` is not 0.

`STATS_TENANTS` (47) request: `u16 flags` (`VOLUMES` 1, `HOT` 2, `CALLER` 4), `u16 k` (hot entries per kind, at most
256), `u32 n`, `u32 cursor` (0 at first), `u32 0`, then n x {`u16 len`, prefix bytes} (n = 0: every tenant the user
may see). Reply header, 40 bytes: `u32 count` (the number of rows in this reply, a tenant's per-volume rows included),
`u32 flags` (`PARTIAL` 1: a sweep or a move is not done; `INGEST_APPROX` 2: the ring overflowed since start),
`u32 next_cursor` (0xffffffff: done), `u32 generation` (of the tenant list), `u64 list_hash` (so a client that sums
several servers can tell their lists apart), `u64 parts_counted`, `u64 parts_listed` (both 0 unless superuser or read
on `all`). A reply stops at 64 MB, well under the 256 MB frame limit, and gives the cursor to go on from.

Each row: a head of `i32 status`, `u32 volume` (0xffffffff: the sum over volumes), `u32 flags` (`PARTIAL` 1),
`u16 len`, `u16 nhot`, then the prefix padded with zeros to a multiple of 8. Then the by-data block, 18 x u64 in this
order: series, series_created, active_series_cur, active_series_prev, rows, nulls, logical_bytes, ts_bytes,
value_bytes, index_bytes, granules, ingested_rows, ingested_bytes, merged_rows, merge_removed_rows,
retention_dropped_rows, resident_bytes, stale_s. With `CALLER`, on the summed row only: the by-caller block, 20 x u64:
requests_query, requests_insert, requests_delete, requests_admin, requests_other, rows_decoded, rows_from_stats,
rows_from_memtable, bytes_read, result_rows, query_us, inserted_rows, inserted_bytes, errors_invalid,
errors_notfound, errors_io, errors_other, denied_requests, denied_series, failed_logins. With `HOT`, on the summed row
only: `nhot` entries of 32 bytes {`u8 kind` (1 reads, 2 writes, 3 bytes per row), `u8[7]` 0, `u64 id`,
`u64 estimate`, `u64 error`}, and for a superuser one more entry per kind with id 0 whose estimate is the threshold.

Rights rows in `auth_ops` (server/auth.c), so `tests/test_auth_ops.c` covers both ops. `STATS_SERIES` checks every
id in its case, like QUERY_MULTI (read right, DENIED per series). `STATS_TENANTS` is P_LOGIN and checks each tenant in
its case (§4). The existing `STATS` op (10) stays as it is.

### 5.2 C API (`include/tsdb.h`)

```c
typedef struct tsdb_series_stats {    /* the 176-byte record of §5.1, field by field */
    int32_t status; uint8_t cls, type; uint16_t flags;
    uint64_t rows, nulls, memtable_rows, logical_bytes, ts_bytes, value_bytes, index_bytes, tomb_bytes;
    uint64_t parts, partitions, granules, full_granules;
    int64_t first_ts, last_ts, last_write_ms;
    uint64_t resident_bytes, reads_est, reads_err, writes_est, writes_err, live_rows;
} tsdb_series_stats;
int tsdb_series_stats(tsdb *db, const tsdb_sid *ids, size_t n, int flags, tsdb_series_stats *out, size_t *done);
int tsdb_set_tenants(tsdb *db, const char *const *prefixes, size_t n);
typedef struct tsdb_tenant_stats { uint32_t flags, generation; uint64_t v[18]; /* §5.1's order */ } tsdb_tenant_stats;
int tsdb_tenant_stats(tsdb *db, const char *prefix, uint32_t volume, tsdb_tenant_stats *out);
typedef struct tsdb_hot { tsdb_sid id; uint64_t estimate, error; } tsdb_hot;
int tsdb_hot_series(tsdb *db, int kind, tsdb_hot *out, size_t k, size_t *n, uint64_t *threshold);
```

`tsdb_volume_stats` gains the fields of §2.3, and `tsdb_options` gains `stats`, `tenants` and `ntenants` (ST18,
ST5): `TSDB_ABI_VERSION` goes up (include/tsdb.h:796), shared with design-latest's bump if both land in one release.

### 5.3 CLI, clients, metrics

- `tsdb DIR stats` (fixed: logical bytes by each series' type, not 12 bytes a row, tools/tsdb-cli.c:823),
  `tsdb DIR stats series NAME...`, `tsdb DIR stats tenants [PREFIX...] [--volumes]`, `tsdb DIR stats hot`.
  Offline, the CLI opens read-only (tools/tsdb-cli.c:764), so it works beside a running tsdbd. For tenants it runs its
  own sweep first, unthrottled, and prints its progress: minutes at 10^9 series. Per-series answers are immediate.
  Each command also takes `--server HOST:PORT --login NAME --password-file F` (never a password in argv) for a running
  tsdbd: the only way to see per-caller counters and hot reads.
- C, Rust, Python and Java clients: `series_stats(ids)` (looping on `done`), `series_stats_by_name`,
  `tenant_stats(prefixes, flags)` (looping on the cursor); the embedded bindings (Rust `Db`, Python `Db`, Java
  `EmbeddedDb`) the same, compared with tsdbd's answers in their tests. All in one commit with the server ops,
  `make test-clients` green (AGENTS.md "Protocol").
- Metrics (docs/metrics.md): §2.3 only, server-wide and per volume. No per-tenant metric (ST16): per-tenant numbers
  come through the STATS ops, which need login. Never a label per series.

### 5.4 Cluster and HA

Storage numbers and per-caller counters come from two different sets of servers (L10). Storage numbers are per
volume: a client in direct mode, or the entry server in simple mode (design-cluster.md:680), asks each volume's
leader and sums the per-volume rows. Replicas count their own storage too, but only leaders are asked, so nothing is
counted twice. Per-caller counters are per node: the client asks every node and sums. The tenant lists must be the
same on every node; the reply's `list_hash` lets the client or entry server detect a difference and flag the sum. On
an NFS failover (design-ha-modes.md) the new writer's counters start at zero, and its sweeper follows its own open.
When the cold tier lands, `ts_bytes` and `value_bytes` split into local and cold (the index stays local,
design-cluster.md:79, so the walk is unchanged).

## 6. Correctness boundaries

- **Never slows writes, merges or queries beyond the budget.** Nothing on the append path (§3.5); on the query path
  only per-thread stores (§3.3); two O(1) pushes per list change; the sweeper is throttled and at nice 19 (§3.1);
  `db->lock` only to copy one partition's list or a batch of memtable slots.
- **Merges, deletes, replaces**: a merge output is ADDed and its inputs are REMOVEd; the partition is recomputed.
  Tombstones and the rows they hide count as stored until a merge drops them (ST1). A replace's parts are level 0 and
  count as ingest.
- **Retention**: retired partitions leave with `dropped` and are subtracted whole; expired rows inside a partition
  count as stored until a merge drops them; `first_ts` is raised to the cutoff, and a series with nothing after it
  reports `NO_RANGE`.
- **Class switches** move no rows (AGENTS.md "Partition classes"): later rows of the series land in another key's
  partition and are counted there; nothing is recomputed.
- **Dropped parts and references** (H4): the ring's ADD references live until the next tick at most; a `dropped`
  part's reference is released without a walk; recomputes hold a reference only while they walk the part. With
  ST19 in place, no reference holder, stats or query, can bring a dropped part back after a crash.
- **Reopen**: counters start at zero; storage is rebuilt by following the open (§3.1); the tenant map is rebuilt from
  the names in the background.
- **Followers**: their own view and sweeper, which diffs the list after each refresh; `memtable_rows` includes the
  WAL tails; no ingest (ST8); parts naming ids beyond the follower's SERIES are put off (ST7). Per-caller counters are
  the follower's own requests.
- **Fail-stop**: stats keep answering after `eng_fail`; they never write or fail anything (ST17).
- **Damage**: index reads are clamped, so a damaged index gives wrong numbers, never a crash. A part's split is
  checked against its own totals and a negative merge difference is clamped (§3.1), with a log line either way. A
  part found `damaged` is still counted (its bytes are on disk) and is in `tsdb_broken_parts`.
- **Lock order** (AGENTS.md): the ring is written under `db->lock` and read lock-free by the sweeper (one producer
  at a time, since `db->lock` serializes them; one consumer). `stats_mu` and the registry's mutex are leaves. The
  sweeper and per-series calls take `db->lock` (and, for the active memtable, `buf_lock` after it) and the
  dictionary's `lock` only with no stats lock held, never across I/O, for at most 256 ids or one partition's list.
- **Platforms**: `mincore` goes through a shim in src/compat.h (its vector is `char *` on macOS); `getrusage` is
  portable; a residency test that cannot run prints that it is skipped (AGENTS.md "Platforms").
- **New invariant for AGENTS.md**: "Statistics are never on the append path (append, memtable, WAL, flush, part
  writer, merge); the list functions push one event each, under the lock they hold; statistics never write a file,
  never call `eng_fail`, hold a part only while walking it, and their locks are leaves."

## 7. Tests

- `tests/test_stats.c` (engine), with an **oracle**: for each series, decode everything with a raw query and the
  index through `granule_read_at`, and compare: `rows`, `nulls`, byte sums, `parts`, `granules` exact; after
  `tsdb_optimize` with no tombstones left, `rows` equals the live count and `live_rows`. After a flush, and once
  nothing is stale, tenant totals equal the sum of the per-series part numbers of the tenant's series exactly. Run
  over float32, float64, int64, bool and state series, with nulls, with ids above 2^32 and several volumes.
- **Every way the list changes**: flush, large insert, merge, delete, replace, retention retire, `drop_before`, class
  switch, follower refresh: after each, wait until nothing is stale and compare with the oracle.
  `retention_dropped_rows` equals the rows of the dropped partitions.
- **No drift (H3)**: add and remove parts across a tenant-list change, at the map's frontier, and on a follower
  whose writer runs without fsync (parts that name ids its SERIES lacks); totals never wrap and match the oracle.
- **Ingest (M2)**: a load with `tsdb_optimize` running at once, so level-0 parts are merged away within a tick:
  `ingested_rows` equals the rows inserted; after a crash and a reopen the replayed rows are not counted again; a
  forced ring overflow sets `INGEST_APPROX`.
- **List changes (H2)**: add a sub-prefix, remove one, change nothing but a read grant: only the affected tenants
  turn `PARTIAL`, the others stay exact, and the result matches a fresh open with the new list.
- **Dropped parts (H4, ST19)**: with `obsolete_part_lifetime_s = 0`, `drop_before` while a query and the ring hold
  the part, crash (crashfs), reopen: the rows stay gone. Fails on main today. `test_tsdb.c` :1122 and :1439 (unlink
  at the last reference) keep passing with stats at 0 and at 1.
- **Memtables (M5)**: per-series calls racing flushes count each row once (or flag `MEM_APPROX`); a follower's
  `memtable_rows` includes its tails.
- **Threads and handles (M6)**: two handles queried from one thread keep separate counts; a thread that outlives
  `tsdb_close` exits cleanly (ASan); a failed block allocation leaves the query's answer unchanged.
- **Approximations within bounds**: hot lists on a Zipf stream with exact counts kept by the test (fixed seed): every
  series above W / k + 3 sampling standard deviations appears, every estimate is within its error; a round-robin
  stream over 8 series is sampled evenly (L8); the bytes-per-row list holds each series once. HLL within 10% on
  10^5 series. `last_write_ms` within one flush interval, and flagged after a reopen.
- **Cost bound (H1)**: a part with 10^7 runs and a request of 65,536 ids stops at the budget with `done` < n and the
  client finishes; `RESIDENT` with n > 1,024 is refused; CANCEL stops a large request.
- **Residency**: `posix_fadvise(DONTNEED)` on a part, `resident_bytes` drops; a query, it rises (prints skipped where
  `mincore` differs).
- **Boundaries**: stats calls after `eng_fail` answer; `stats = -1` leaves no ring, no thread and zero counters;
  `stats = 0` starts no thread until a tenant call; 10^7 series with an interleaved tenant pattern for the map's
  worst case; an expired series reports `NO_RANGE`.
- **Server**: `test_server` for both ops with the record sizes and offsets of §5.1 asserted; the cursor and the
  64 MB cap; the rights of §4 (a tenant admin sees its tenants and gets NOTFOUND for a missing one under its prefix,
  DENIED elsewhere; per-series DENIED; no thresholds or global fields for a tenant admin); a user named under a
  tenant but with grants outside it counts under `(none)`; `--no-auth`; `test_auth_ops` gets its rows; metrics names
  for the new ops; the metrics port carries no tenant name and no per-tenant number (scrape it with tenants present
  and check the text). Clients:
  `make test-clients`; `make integration-tests` gets a stats case per client.
- **Jepsen**: only for ST19, which changes how a drop reaches the disk. The statistics themselves change nothing
  there.

## 8. Benchmarks

The budget is a rule (Summary): stats on against off, repeated 5 times interleaved on a quiet machine, means and
standard deviations reported (benchmark policy):

| Benchmark | Measures | Pass |
|---|---|---|
| `build/insert-bench`: `tsdb_append` 1 thread with `wal_sync_ms = 10`; WAL inserts of 1,000 rows; large inserts of 1 M rows | insert throughput and p99 | within 1% (append: identical, no new code) |
| `build/tsdbc-bench query --conns 1` and `32` | queries/s, p99 | within 1% |
| `build/tsdb-bench` query suite, embedded, one handle and two handles in one thread | per-query time, single-row lookups above all | within 1% |
| merges settled after a load (`tsdb-bench`) | time to settle | within 1% |
| sweeper, 100 M-row set | catch-up time following the open, steady CPU under the benchmark load | under 1% of one core steady |
| sweeper, sparse: 10^8 series at 1 M rows/s | sweeper CPU against merge CPU; `stale_s` | at most 5% of merge CPU; capped by the throttle |
| `STATS_SERIES` on parts with 10^7 runs | 1 id, 1,024 ids, 65,536 ids; time and probes | within the budget; the merge pass wins for large n |
| memory | RSS with 10^8 series, tenants in runs and interleaved | as §3.6 |

## 9. Build plan

| Step | What | Gate | Effort (days) |
|---|---|---|---:|
| 0 | ST19: rename dropped parts at lifetime <= 0; crash regression test that fails on main | make test, asan, jepsen | 1 |
| 1 | Per-thread blocks and registry, the `qs_end` hook, global counters, `tsdb_volume_stats` fields, metrics, the `stats` option, `getrusage` | make test, asan, tsan; query benchmarks | 2 |
| 2 | Event ring, sweeper, part records, volume column totals, catch-up following the open, `tsdb_series_stats` with its cost bound, CLI `stats series` and the fixed `stats` | make test, asan, tsan; oracle; insert benchmarks | 4 |
| 3 | Tenants: list in options, map, partition vectors and recomputes, drops, ingest, merges, retention, HLL, incremental list changes, `tsdb_set_tenants`, CLI `stats tenants` | make test, asan, tsan; oracle on every list change; drift tests | 5 |
| 4 | Hot lists (reads, writes, bytes per row) and residency | make test; bound tests | 2 |
| 5 | One commit: tsdbd's per-tenant counters, `--tenants` and the default from `admin` grants, ops 46-47 with `auth_ops` rows; the C, Rust, Python and Java clients and embedded bindings; CLI `--server` | make test (both backends), tsan, `test_auth_ops`, make test-clients, integration-tests | 5 |
| 6 | Benchmarks (§8), docs: metrics.md, protocol comment, CHANGELOG (version and floor), AGENTS.md invariant and layout line | numbers on a quiet machine | 2 |

About 21 days. Step 0 stands alone and should go first whatever happens to the rest. Steps 1-2 are useful without
tenants; 3 needs 2; 5 needs 3 and 4. Format 9 (S0): `written_ms` in the part header, about half a day inside S0 of
design-ha-modes.md §7 (decided 2026-11-06, §10); this design does not wait for it.

## 10. Decisions on the open questions (user, 2026-11-06)

The six questions of the first draft were answered on 2026-11-06. Open questions: none open.

| # | Question | Decision (2026-11-06) |
|---|---|---|
| 1 | Tenant source without `--tenants` | **As recommended** (ST5). Without a `--tenants` file, the tenants are the `prefix:` grants that tenant admins hold `admin` on. Past about 10^7 series, operators configure `--tenants FILE`, because every change of the default list costs a background name scan of the moving tenant. |
| 2 | Per-tenant numbers on the metrics port | **Never.** The recommended opt-in (`--metrics-tenants N`) was not taken. Per-tenant numbers are only available through the STATS ops, which require login. The option and every per-tenant metric are gone from the design (ST16, §5.3, §7, §9). Per-volume and server-wide metrics stay. |
| 3 | A droppable stats cache file | **As recommended.** Not in the first build; following the open reads the index from disk once (§3.1). Add one (not a format change: a missing or mismatched cache means a rebuild) if the measured catch-up after an open on production-sized data exceeds 10 minutes. |
| 4 | Exact live rows on request | **As recommended** (ST1). `EXACT_ROWS` is a flag, limited to 1,024 ids per request; the default stays physical. |
| 5 | Format 9: `written_ms` in the part header | **Yes.** An 8-byte write time goes into the part header in step S0 of design-ha-modes.md §7, which carries it (one line added to the S0 list there). ST13 describes the field; a part written before format 9 has none and falls back to the file's mtime. |
| 6 | Hot-list estimates for users with read rights | **As recommended** (ST10). Any user with read rights on a series sees its estimates, without error bounds or thresholds. Tenant-level numbers stay with tenant admins (their tenants) and superusers. |

## 11. After the review

| Finding | Answer | Where |
|---|---|---|
| H1 no cost bound on `STATS_SERIES` | fixed: sorted ids, part pruning, binary search or merge pass by cost, 10^8-probe budget with `done`, `RESIDENT`/`EXACT_ROWS` at most 1,024 ids, CANCEL, a 10^7-run benchmark | ST2, §3.2, §5.1, §7, §8 |
| H2 tenant-list change rebuilds everything | fixed: list in `tsdb_options` (one sweep at start), applied only when the prefix set changes and after 30 s, incremental move of the affected series, old numbers served under the old generation; decision 1 (§10) says configure `--tenants` past 10^7 series | ST5, §4, §10 |
| H3 add and subtract can attribute differently | fixed: per-partition vectors replaced wholesale, records tagged with the map generation, map extended before a walk, follower parts put off, split checked against the part's totals, clamp and log, drift tests | ST4, ST7, §3.1 invariant, §7 |
| H4 sweeper references keep dropped parts | fixed: no long-lived references (ADD until the next tick, dropped released at once, recompute only while walking); the existing hazard gets its own fix and crash test (ST19, step 0) | ST3, ST19, §6, §7, §9 |
| M1 retention re-reads cold indexes | fixed: per-(partition, tenant) vectors; a drop subtracts with no walk | ST4, §3.1 |
| M2 ingest misses fast merges, double-counts replay | fixed: list events instead of sampling; replay flushes flagged and excluded; follower reports no ingest; overflow flag | ST3, ST8, §2.2 |
| M3 second full index pass; on for every open | fixed: the sweeper follows the open while pages are hot; `stats = 0` is lazy, tsdbd uses 1. Decision 3 (§10) keeps "not in the first build", because following the open removes the second disk read | ST18, §3.1, §10 |
| M4 1% budget fails with sparse granules | fixed: the throttle is a hard cap (about 6% of a core at most); the steady target is stated against merge CPU and measured with 10^8 sparse series. Inline per-tenant deltas are not needed: drops no longer walk | §3.1, §8 |
| M5 memtable reads without `buf_lock` | fixed: parts, flushing and tail memtables under one `db->lock` hold; active memtable under `buf_lock` in batches of 256; retry on a flush, `MEM_APPROX` | ST17, §3.2, §6 |
| M6 per-thread blocks with two handles or after close | fixed: blocks keyed by a never-reused handle number, refcounted registry, 128-byte alignment, seqlock, no counting on allocation failure, lazy hot lists | ST9, ST10 |
| M7 per-tenant metrics not monotonic | moot: the metrics design that answered it (sticky export set, accumulating `(other)`, no `volume` label) is gone, because per-tenant metrics are never on the metrics port (decision 2, §10). Counters kept per tenant number (§3.4) still make the STATS by-caller blocks grow only | ST16, §3.4, §10 |
| M8 wire layout inconsistent, frame overflow | fixed: offset table (176 bytes), fixed block orders, `nhot`, cursor, 64 MB cap, `count` counts rows | §5.1 |
| M9 protocol split over two steps | fixed: one step and one commit | §9 step 5 |
| M10 "~0" map memory | fixed: 2 GB (1 GB with a 1-byte map) per 10^9 is the planning number. Filling the map during the SERIES load is rejected: it would add 10-20 s to a failover open; it fills in the background instead | ST7, §3.6 |
| L1 last write wrong after reopen | fixed: `written_ms` in the format 9 part header (decision 5, §10; step S0 of design-ha-modes.md); parts from before it are flagged `WRITE_FROM_MTIME` | ST13, §2.1 |
| L2 wrong statements | fixed: `db_snapshot` copies with references, `tsdb_get_volume_stats` does not; followers tail the WAL; existing `merges`/`merged_rows` and how the per-tenant split relates | §0, §2.2, §6 |
| L3 `live_rows` leaves out nulls | fixed: nulls included, run quietly | ST1, §2.1 |
| L4 unknown tenant always DENIED | fixed: NOTFOUND per A16 | §4 |
| L5 tenant admins see global load | fixed: no thresholds, error bounds or global fields for them; users attributed only when all their grants are under the tenant | ST6, §2.2, §4, §5.1 |
| L6 `admin` on other selectors | fixed: only `prefix:` defines a tenant | ST5 |
| L7 numbers that do not match | fixed: ratios from part rows and bytes; index overhead to `other_bytes`; tests compare after a flush; `NO_RANGE` for expired series | §2.1, §2.3, §7 |
| L8 hot-list gaps | fixed: geometric-skip sampling, min-heap for bytes per row, window timestamps, mergeable-summaries rule | ST10, §7 |
| L9 rights-map counters per row | fixed: per request | ST12, §2.3 |
| L10 platform, CLI, cluster, protocol number | fixed: `getrusage` and a `mincore` shim; `--password-file`; two fan-out sets and `list_hash`; its own protocol version. Answered: the offline CLI opens read-only (tools/tsdb-cli.c:764) and does run beside tsdbd; it sweeps first, unthrottled | ST12, ST15, §5.3, §5.4, §6 |
