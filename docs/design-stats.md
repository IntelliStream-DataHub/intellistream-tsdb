# Statistics per series and per tenant

Status: design, not implemented. Branch `stats-design`, written on 2026-11-06 from main at e33d87f.
Code references are `file:line` at e33d87f.

The user's request (2026-11-06): "we also need statistics for each timeseries and tenant, like compression ratio,
logical disk space, cache hit rate, and more if you have suggestions, performance is more important than accuracy
here".

Related designs: [design-auth.md](design-auth.md) (tenants are name prefixes, tenant admins, rights),
[design-latest.md](design-latest.md) (the latest-value table and its hit counters, protocol 13),
[design-cdc.md](design-cdc.md) (feed slots and their lag, protocol 14), [design-ha-modes.md](design-ha-modes.md) and
[design-cluster.md](design-cluster.md) (volumes on failover pairs and on replicas; format 9 in step S0),
[metrics.md](metrics.md) (the Prometheus port).

## Summary

- **Two kinds of numbers, counted in two places.** What a series or tenant *stores* (rows, bytes per column,
  compression, time range, parts and granules) is read from the granule index, which already holds a row count and
  the compressed size of every granule's timestamps and values. What a tenant *does* (queries, inserts, rows read,
  errors, denied requests) is counted on the request path, per thread, by the user who asked.
- **Nothing is added to the write path.** `tsdb_append`, the memtable, the WAL and the part writer get no new code.
  Storage numbers come from a background thread (the *sweeper*) that walks each part's index once when the part is
  listed and once when it goes. The query path gets about ten stores into a per-thread block at the end of a query.
- **Per series, nothing is kept in memory.** A series' storage numbers are computed when asked, from the index runs
  of that series (the same lookups a whole-range query does, without decoding). Request counts per series are not
  kept for every series: a small "hot series" list (space-saving top-k, sampled) names the busiest ones.
- **A tenant is a configured name prefix** (`--tenants FILE`, or by default every prefix a tenant admin holds
  `admin` on). Storage is attributed by the series' name, requests by the user's name. A map of 2 bytes per series
  (much less when tenants create their series in runs) lets the sweeper attribute storage without touching names.
- **Exposure:** two new ops, `STATS_SERIES` (46) and `STATS_TENANTS` (47), in protocol 15 or riding along with 13
  or 14 if one of them is still unreleased; `tsdb DIR stats series|tenants|hot` in the CLI; C API calls; the four
  clients and the embedded bindings; new server-wide and per-volume metrics, and per-tenant metrics only when asked
  for (`--metrics-tenants N`, capped). Never a label per series.
- **No format change.** Format 9 needs nothing for this. Counters start at zero with each process (as Prometheus
  expects); storage numbers are rebuilt from the indexes in the background after an open.
- **Budget, as a rule:** stats on against stats off moves insert throughput, query throughput and query p99 by less
  than 1%, and the sweeper uses at most 1% of one core once it has caught up.

## 0. What exists (checked)

| What | Where |
|---|---|
| Per-query numbers of the calling thread: rows decoded, rows from granule statistics, memtable rows, bytes read, parts, granules, granules from statistics | `tsdb_query_stats`, include/tsdb.h:656-673; accumulated in `qstats` with relaxed atomics, src/internal.h:121-134; folded in `qs_end`, src/volumes.c:1860-1880 |
| Logical bytes of a query are `rows x (8 + type width)` | src/volumes.c:1872 |
| Database totals (parts, partitions, rows, bytes, merges) and per-volume gauges for the metrics port | `tsdb_stats` include/tsdb.h:683-700; `tsdb_volume_stats` :704-719 |
| The `STATS` op (10): `tsdb_stats` as 8 x u64, superuser or read on `all` | include/tsdb_protocol.h:40, 212; server/auth.c:85, 910; server/tsdbd.c:1833-1840 |
| The CLI's `stats` assumes 12 bytes per raw row (float32) for every type | tools/tsdb-cli.c:818-829 |
| Protocol 12, ops up to 38; design-latest takes 39 and protocol 13; design-cdc takes 40-45 and protocol 14 | include/tsdb_protocol.h:192, 204-239; design-latest.md L14; design-cdc.md:744-784 |
| The metrics thread: global atomic counters, hooks that return at once when the port is off | server/metrics.c:68-80, 110-190; server/metrics.h:37-51 |
| Metrics never carry user or series names; per-volume gauges carry the path | docs/metrics.md:5-7; design-auth.md A21 |
| Tenants are byte prefixes of series names; tenant admins hold `admin` on a prefix; users of a tenant are named under it | design-auth.md A7, A9 |
| Per-user rights map, one byte per local id in a radix table; a user's state is replaced when its grants change | server/auth.c:98-119; design-auth.md A15 |
| Granule index entry: count, ts_min, span, offset, `ts_len`, `val_len`, nulls, ...; one *run* per series (`rser`, `rstart`); `gix_run` finds a series' entries by binary search | src/internal.h:633-650, 697-711 |
| Index reads are clamped, so a damaged run table gives wrong numbers, never a read outside the index | src/internal.h:691-695 |
| Part: rows, min/max ts, `written` (when its newest rows were inserted), `level`, `dropped`, index length | src/internal.h:726-757 |
| Level 0 parts are written by inserts and flushes; merges write level + 1 | src/merge.c:62, 436 |
| `dropped` is set when retention retires a partition or `drop_before` drops it | src/merge.c:573; src/db.c:988 |
| Memtable: per-series slots indexed by id, `created_ms` | src/internal.h:1145-1155 |
| The series dictionary keeps a pointer, a malloc'd name, a type byte and hash slots per series (roughly 60 bytes) | src/internal.h:980-1002 |
| Parts are read through `mmap`; there is no cache of decoded granules and no `mincore` call anywhere | src/part.c:174; `grep mincore src` is empty |
| tsdbd's worker runs `handle`, then the request metrics and the copy of the last query stats | server/tsdbd.c:1869-1880; error statuses are counted in `reply`, :653 |
| A cluster fans database-wide ops (STATS among them) out per volume to the leaders; the cold tier keeps the index local | design-cluster.md:680, 79 |
| Feed slots have per-slot lag metrics | design-cdc.md:588-589 |

## 1. Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| ST1 | **Storage numbers are physical**: rows and bytes as stored in parts and memtables, including rows that a newer write, a tombstone or retention hides until a merge removes them. Exact live rows are a flag (`EXACT_ROWS`) that runs a count query. | Live rows always (a count per series, deduplicated across overlapping parts) | A live count decodes every granule a tombstone touches and every overlap: a query's cost. "Performance before accuracy": the physical count is free from the index and converges on the live count as merges settle. |
| ST2 | **Per-series storage numbers are computed when asked**, from the series' index runs in each listed part (count, `ts_len`, `val_len`, nulls, ts range); nothing per series is held in memory. | A maintained record per series | At 10^9 series even 64 bytes each is 64 GB. The on-demand walk costs what a whole-range statistics-only query costs (0.17-0.23 ms for a series of 10 M rows in 116 parts, AGENTS.md benchmark table). |
| ST3 | **The sweeper**: one background thread per `tsdb` handle reconciles, each second, the listed parts of every volume with the parts it has counted. A new part's index is walked once and its numbers added (per volume, per tenant, per column); a part no longer listed is walked again and subtracted. The sweeper holds a reference on each counted part, so the second walk reads a mapping that is still there. | Hooks in `db_parts_insert_locked`, `db_parts_remove_locked` (src/db.c:103-136), the follower's list swap (src/manifest.c:579-600) and `pw_finish_mode` (src/part.c:724) | No change to code where last-write-wins and publication order live. Reconciling against the list covers every way a list changes (publish, merge, retire, drop, follower refresh) with one rule. The walk reads index pages the writer or the merge just touched, so they are in the page cache. |
| ST4 | **Per-part summaries are not kept per tenant.** A counted part keeps a 64-byte record (its column totals and a reference); per-tenant deltas are recomputed by the second walk. | A per-tenant vector per part | With 10^4 tenants in a volume, every part holds series of all of them: 10^4 parts x 10^4 tenants x 40 bytes = 4 GB. A second walk of an index already in memory costs a few nanoseconds per entry. |
| ST5 | **A tenant is a configured prefix.** tsdbd reads `--tenants FILE` (one prefix per line, reloaded on SIGHUP); without it, the tenants are the prefixes of every `admin` grant in USERS. The longest matching prefix wins (`acme/eu/` before `acme/`); unmatched series and users belong to tenant `(none)`. Embedded: `tsdb_set_tenants`. At most 65,535 tenants. | The first name segment up to `/`; a user's grants; tenant ids stored with each series | A7 keeps names free of any separator convention. Grants overlap and change; a series would belong to several users. Storing a tenant per series in SERIES is a format change, and tenants are an operator's view that should be changeable. Tenant admins already define prefixes (A9), so the default needs no configuration where they exist. |
| ST6 | **Storage and ingest are attributed by data, requests by caller.** A series belongs to the tenant its name matches; a request belongs to the tenant its user's name matches. | Attributing queries to the tenants of the series they read | A query's cost belongs to who asked; the data's cost to whose data it is. Caller attribution needs no lookup per series (the user is on the connection), so QUERY_MULTI over a million ids costs one update. A superuser's requests (the Pulsar sink, say) count under the superuser's own tenant, usually `(none)`; its rows still count under each series' tenant in storage and ingest. |
| ST7 | **The tenant map**: per volume, 2 bytes per local id, in pages of 65,536 ids; a page whose ids all belong to one tenant is stored as that tenant number in the page table and allocates nothing. The sweeper fills it in id order from the names (ids past its frontier get a tenant within one tick). | Matching names on the request or write path; a hash of name to tenant | A u16 lookup in id order is ~2 ns and needs no lock; prefix matching is 50-100 ns and touches the name arena. Series are created in runs per tenant in practice, so most pages are uniform. Worst case (tenants interleaved id by id) is 2 GB at 10^9 series, about 3% of the series dictionary itself. Series creation gets no new code. |
| ST8 | **Ingest per tenant is counted from level-0 parts** as the sweeper first sees them (rows, logical bytes, active series). Merge output rows per tenant come from level > 0 parts (write amplification per tenant). | Counting in `tsdb_append` / `mem_insert` | The append path runs at 10 M calls/s on one thread (AGENTS.md "Durable writes"), about 100 ns a call: 5 ns more would cost 5%. Counting at the part boundary lags by one flush and costs nothing on the path. |
| ST9 | **Request counters live in per-thread blocks**, one per thread that ever ran a query or a request: plain stores by the owning thread (relaxed atomic loads and stores, no read-modify-write), summed by readers. A thread that exits folds its block into a retired total. | Global atomics (as server/metrics.c:68-77 does); sharding by a hash of the thread | An uncontended `lock xadd` costs 5-20 cycles and a contended one far more; ten of them per query is measurable on the 600 k queries/s embedded point lookups. A block owned by one thread needs no atomics on x86-64 or arm64. |
| ST10 | **Hot series: sampled weighted space-saving**, k = 256 per thread block and kind, 1 query in 8 sampled (weight 8). Kinds: reads (queries), writes (rows of level-0 parts, fed by the sweeper, not sampled), bytes per row (worst runs with at least one full granule in parts of level > 0). Two windows, the current and the previous 60 s. | Exact counters per series; count-min sketch | Exact counters cost 16 bytes x 10^9. Count-min answers "how much for this series" but cannot list the top without a candidate set; space-saving lists them with a known error (every series above W / k appears; each estimate is high by at most W / k plus the sampling error). |
| ST11 | **Active series per tenant: HyperLogLog**, 1,024 registers (about 3% standard error), one per tenant per window (current and previous hour), updated by the sweeper from level-0 parts. Series count per tenant is exact (from the tenant map). | A bit per series per window | 10^9 bits x 2 windows = 250 MB for a number nobody needs to the last series. |
| ST12 | **Cache hit rates are reported for the caches tsdb has**: granule statistics (granules and rows answered from the index against decoded), memtable rows, the page cache (major faults of the process against bytes decoded; residency of a series or tenant by `mincore`, on request and sampled), the latest-value table (design-latest.md §6.5), the rights map. There is no decoded-granule cache to report. | A user-space block cache to make a hit rate measurable | Parts are `mmap`'d (src/part.c:174); the page cache is the cache. Adding one to measure it would cost memory and a copy on every read. |
| ST13 | **Nothing is persisted.** Counters count from the start of the process; storage numbers are rebuilt by the sweeper after an open, at a throttled rate, and replies say how far it got. | A STATS file updated on every flush; fields in the part footer (format 9) | A file written on the flush path is new fail-stop surface and new jepsen work for numbers that are rebuildable. The footer is 64 bytes by a static assert (src/internal.h:596) and would freeze at 1.0 for a number one index walk gives. A droppable cache file is open question 3. |
| ST14 | **Two ops**: `STATS_SERIES` (46: ids, fixed records back) and `STATS_TENANTS` (47: tenants, with optional per-volume rows and hot lists). Rights: read on each series for the first (DENIED per series, like QUERY_MULTI); superuser, read on `all`, or `admin` on a prefix that covers the tenant for the second. | An op per statistic; a prefix op that scans names | Fewer ops, fewer rows in `auth_ops`. A free prefix would scan 10^9 names per request. |
| ST15 | **Protocol**: new ops only, floor stays 12. The stats ops take the next version number not yet released when they merge: they ride along with 13 or 14 if that version is still unreleased, else 15. Op numbers are fixed (46, 47) regardless. | Its own version always | A protocol version is a promise per release; two unreleased versions that only add ops are one step for clients ("new ops only, as protocol 13 did", design-cdc.md:744). |
| ST16 | **Metrics port**: new server-wide and per-volume metrics; per-tenant metrics only with `--metrics-tenants N` (the N tenants with the most bytes on disk, the rest summed into `tenant="(other)"`, N at most 1,000). Never a label per series or per user. | Per-tenant metrics by default | A21: the port has no login and its labels must not name tenants unless the operator asks. With N capped, cardinality stays bounded. |
| ST17 | **Stats never block, write or fail-stop.** The sweeper and the stats calls take `db->lock` only to copy the part list with references (as `tsdb_get_volume_stats` does), the series dictionary's `lock` only for a batch of names, and their own locks are leaves. They never write a file, never call `eng_fail`, and keep answering after the engine failed (as queries do). | Exact numbers under `publish_lock` | Numbers a few seconds stale are fine; a stats call that delays a publish is not. |
| ST18 | **On by default**, with `tsdb_options.stats = -1` (and `tsdbd --no-stats`) turning it off: no sweeper, no blocks, hooks return at one predictable branch. | Off by default | The budget (§6) keeps it below noise; an operator should not have to restart to find out which tenant fills the disk. |

## 2. The statistics

"Exact" means exact for what it measures (physical storage, requests since start). Staleness is the most it lags
behind what happened.

### 2.1 Per series (`STATS_SERIES`, `tsdb_series_stats`)

| Statistic | Exact / approximate | Stale by | Source |
|---|---|---|---|
| `rows` stored (parts + memtable) | exact physical; live rows only after merges (ST1) | 0 | index `count` per entry of the series' runs; memtable slot |
| `nulls` stored | exact physical | 0 | index `nulls` |
| `memtable_rows` | exact | 0 | memtable slot by id (src/internal.h:1147) |
| `logical_bytes` = rows x (8 + type width) | exact physical | 0 | as `qs_end` (src/volumes.c:1872) |
| `ts_bytes`, `value_bytes` on disk | exact | 0 | sums of `ts_len`, `val_len` |
| `index_bytes` | exact share: entries x stride + 8 per run | 0 | the part's `gix` (src/internal.h:645-650) |
| `tomb_bytes` | exact: 24 x the series' tombstones | 0 | part trailer (src/internal.h:603-608) |
| compression ratio = logical / (ts + value + index + tomb bytes) | derived | 0 | |
| `first_ts`, `last_ts` | physical; `first_ts` raised to the retention cutoff | 0 | first and last entry's ts range; memtable min/max |
| `last_write_s` | approximate: the newest `written` of a part holding the series, or the memtable's `created_ms` when it holds rows | one flush | src/internal.h:747, 1154 |
| `parts`, `partitions`, `granules`, `full_granules` (count = the part's granule rows) | exact | 0 | runs and entries |
| `class` | exact | 0 | `tsdb_series_class` |
| `resident_bytes` (flag `RESIDENT`) | exact for the pages at the time of the call | 0 | one `mincore` per (part, series): a series' granules are contiguous in a part |
| `reads_est`, `reads_err`, `writes_est`, `writes_err` | approximate (ST10); 0 with an error bound when not in the hot list | 60 s window | hot lists |
| `latest_cached` | exact | 0 | design-latest's table, when built |
| `live_rows` (flag `EXACT_ROWS`) | exact | 0 | a count query |

The walk uses the part list under its references, not a query snapshot's entry windows (`sn.gi`/`sn.gj`): it counts
what is on disk, including granules a tombstone touches, and never answers a value. AGENTS.md's rule ("never
`part_find` on `sn.parts`") is about answers; this walk gives none.

### 2.2 Per tenant (`STATS_TENANTS`, `tsdb_tenant_stats`)

By data (storage and ingest), per volume and summed:

| Statistic | Exact / approximate | Stale by | Source |
|---|---|---|---|
| `series` | exact | one sweeper tick after creation | tenant map |
| `series_created` (since start) | exact | one tick | tenant map frontier |
| `active_series` (current and previous hour) | approximate, about 3% (ST11) | one flush + one tick | HLL from level-0 parts |
| `rows`, `nulls`, `logical_bytes`, `ts_bytes`, `value_bytes`, `index_bytes`, `granules`, `parts_with_rows` | exact physical | one tick after a publish; after an open, until the first sweep ends (`progress`) | sweeper |
| compression ratio, bytes per row | derived | as above | |
| `ingested_rows`, `ingested_bytes` (since start) | exact for rows that reached parts | one flush + one tick | level-0 parts |
| `merged_rows` (written by merges, since start); write amplification = (ingested + merged) / ingested | exact | one tick | level > 0 parts |
| `retention_dropped_rows` (whole partitions retired or dropped) | exact | one tick | unlisted parts with `dropped` (src/merge.c:573, src/db.c:988) |
| `merge_removed_rows` (rows merges did not carry over: overwritten, deleted, expired) | exact as a sum; not split by cause per tenant | one tick | inputs minus output of each merge, per tenant |
| `resident_bytes` | approximate: sampled `mincore` of up to 64 granules per part per 10 minutes | 10 min | sweeper |

By caller (tsdbd only; the user's name decides the tenant):

| Statistic | Exact / approximate | Stale by | Source |
|---|---|---|---|
| requests by op group (query, insert, delete, admin, other) | exact | 0 | worker loop, server/tsdbd.c:1869-1872 |
| query rows covered: decoded, from granule statistics, from memtables; bytes read; result rows; query seconds | exact | 0 | the request's `tsdb_query_stats` |
| inserted rows and bytes | exact (acknowledged inserts) | 0 | as `metrics_rows_inserted` (server/tsdbd.c:1843-1846) |
| errors by code; `denied` (whole requests and QUERY_MULTI series) | exact | 0 | `reply` (server/tsdbd.c:653), the QUERY_MULTI case |
| failed logins (by the name tried) | exact | 0 | login path |
| feed slots owned by the tenant's users and their largest lag | exact | as the feed's metrics | design-cdc.md:588-589, when built |

Hot lists (flag `HOT`, k up to 256 per kind): the hottest series by reads, by written rows and by bytes per row,
filtered to the tenants asked for and to series the user may read. A quiet tenant's hottest series may not reach a
global list; the list says the threshold below which it cannot see (ST10).

### 2.3 Server-wide and per volume (metrics, `tsdb_volume_stats`)

New per-volume fields: `logical_bytes`, `ts_bytes`, `value_bytes`, `index_bytes`, `other_bytes` (headers, footers,
tombstones), `stats_progress` (parts counted / listed). New counters (per-thread blocks, ST9): query granules and rows
by source (`stats`, `decoded`, `memtable`), bytes read, queries; `tsdb_major_faults_total` from `/proc/self/stat`
read at scrape time (the page cache's misses: compare with `rate(tsdb_query_bytes_read_total) / 4096`); the rights
map's hits and resolutions; the sweeper's CPU seconds and the tenant map's bytes. With `--pin-index` the index is on
the heap and never faults, which the metric then shows.

## 3. Where the numbers come from, and what they cost

### 3.1 The sweeper

Each second, per volume: take `db->lock`, copy the listed parts with a reference each, release it (as
`tsdb_get_volume_stats`). Compare with the counted set (both sorted by `part_cmp`, a single merge pass, O(parts)).
Then, in this order:

1. **Parts no longer listed** (the counted set holds their reference, so their mapping is there): walk the index,
   subtract the part's totals and its per-tenant deltas; if `dropped`, add the rows to `retention_dropped_rows`;
   if a merge output that replaced it is listed, the difference goes to `merge_removed_rows`. Drop the reference.
   These come first so that a merge's input and output do not both count for long, and so references go quickly.
2. **New parts**: walk the index, add; a level-0 part adds to ingest, to the active-series HLL and to the hot writes
   list; a part of level > 0 adds to `merged_rows` and feeds the bytes-per-row list.
3. **Backlog** after an open, or after a change of the tenant list: the remaining parts, throttled.

The walk per part: for each run, the tenant from the map (~2 ns, ids ascending), then for each entry `count`,
`nulls`, `ts_len`, `val_len` (four `gix_get`, a few shifts each). Estimated 5-10 ns per entry and 5 ns per run; a
part of 10^6 single-granule runs costs 10-15 ms of one background core. Totals live behind the leaf mutex
`stats_mu`, taken once per part.

A reference held by the sweeper delays the unlink of an obsolete merge input by at most one tick; retired parts are
kept for `obsolete_part_lifetime_s` anyway.

**Open-time catch-up.** The first sweep reads every part's index once: about 32 bytes per granule (AGENTS.md "The
granule index"). The 100-billion-row benchmark has about 12 M granules, under 1 GB of index: seconds on NVMe. A
database of 10^9 series in many partitions can have 10^10 entries or more, and its first sweep takes as long as
reading that index (`stats_sweep_mb_s`, default 256 MB/s, so as not to compete with queries), minutes up to an hour.
Until then replies carry `progress` and the tenant totals are partial; per-series numbers (computed on demand) are
right at once.

### 3.2 The query path

At the end of the outermost query (`qs_end`, src/volumes.c:1860), the numbers already gathered in `qstats` go into
the thread's block: about ten relaxed loads and stores (ST9), estimated 3-5 ns. One query in eight also updates the
thread's hot-reads summary (a probe of a 512-slot table, on a miss a min-heap replace of k = 256): about 40 ns,
5 ns averaged. QUERY_MULTI updates once per call and samples per series (src/multi.c). The thread's block is found by
a `__thread` pointer; the first query of a new thread registers one under a leaf mutex.

Budget check against the fastest query there is: embedded single-row lookups at 59,891 queries/s per thread
(17 us each, AGENTS.md "Embedded"): 10 ns is 0.06%.

### 3.3 The request path in tsdbd

After `handle(r)` (server/tsdbd.c:1870), the worker adds to its block, indexed by the user's uid: the op group, the
query stats it already copies (:1874-1879), inserted rows. Error statuses are counted in `reply` (:653), which has
the connection and so the user. Per uid, 24 counters of 8 bytes; blocks grow in pages of 64 uids as users appear.
The uid-to-tenant table is rebuilt with each USERS snapshot (server/auth.c:122-130) and when the tenant list changes;
counters stay keyed by uid, so a grant change (which replaces the user's state, server/auth.c:108-119) loses nothing.

### 3.4 The write path

No new code in `tsdb_append`, `mem_insert`, the WAL, the flush, the part writer or the merge. This is the rule that
keeps the 10 M appends/s path where it is.

### 3.5 Memory

| | 10^6 series | 10^8 series | 10^9 series |
|---|---:|---:|---:|
| tenant map, series created in runs per tenant | ~0 (page table 256 KB per volume) | ~0 | ~0 |
| tenant map, worst case (tenants interleaved id by id) | 2 MB | 200 MB | 2 GB |
| per series otherwise | 0 | 0 | 0 |
| per counted part | 64 B | 64 B | 64 B |
| per tenant per volume (about 40 counters) | 320 B | | |
| HLL per tenant (2 windows x 1 KB) | 2 KB | | |
| hot lists per thread (3 kinds x 2 windows x 256 x 24 B) | 37 KB | | |
| per-uid request counters per worker | 192 B per user that worker served | | |

10,000 tenants on 10 volumes: 32 MB of totals and 20 MB of HLL; 64 workers serving 10,000 users: 120 MB at most.

## 4. Tenants

- **Definition** (ST5): a sorted list of prefixes; a name's tenant is the longest prefix it starts with, else
  `(none)`. A user's tenant is found the same way from the user name (A9 names users under their tenant's prefix).
- **Source**: `tsdbd --tenants FILE` (text, one prefix per line, `#` comments; SIGHUP reloads it), else the prefixes
  of every `admin` grant in USERS, recomputed when USERS changes. Embedded: `tsdb_set_tenants(db, prefixes, n)`.
  The CLI's offline `tsdb DIR stats tenants` takes `--tenants FILE` or reads USERS beside the database.
- **A change of the list** discards the tenant map and the per-tenant storage totals and rebuilds both in the
  background (`progress` says how far). Per-caller counters stay (they are by uid); the uid-to-tenant table is
  recomputed at once.
- **Rollup**: maintained, never on demand (ST3, ST7). At 10^9 series a rollup on demand would walk every name and
  every index entry per request.
- **Rights** (ST14): superuser or read on `all` sees every tenant; a user holding `admin` on `prefix:Q` sees each
  tenant whose prefix starts with Q. Anything else, an unknown tenant included, is DENIED (design-auth.md A16:
  denied and missing look alike). With `--no-auth` every tenant is visible, as data ops are.

## 5. Exposure

### 5.1 Wire

```
STATS_SERIES (46)   u32 n (1 .. 65,536), u32 flags (RESIDENT 1, EXACT_ROWS 2), u64 ids[n]
  -> n records of 160 bytes, in request order: i32 status (0, DENIED, NOTFOUND), u8 class, u8 type, u16 0,
     u64 rows, nulls, memtable_rows, logical_bytes, ts_bytes, value_bytes, index_bytes, tomb_bytes,
     u64 parts, partitions, granules, full_granules, i64 first_ts, last_ts, last_write_s,
     u64 resident_bytes, reads_est, reads_err, writes_est, writes_err (all 0 when status != 0), u64 live_rows
     (EXACT_ROWS; else 0), u8 latest_cached, u8[7] 0
STATS_TENANTS (47)  u16 flags (VOLUMES 1, HOT 2, CALLER 4), u16 k (hot list length, <= 256), u32 n,
                    n x {u16 len, prefix} (n = 0: every tenant the user may see)
  -> u32 count, u32 flags (PARTIAL 1: a sweep has not finished), u64 parts_counted, u64 parts_listed,
     count x {i32 status, u16 len, prefix, u32 volume (0xffffffff: all), the by-data block of §2.2 as u64s,
              the by-caller block (CALLER), hot entries (HOT): u8 kind, u64 id, u64 estimate, u64 error}
```

Rights rows in `auth_ops` (server/auth.c), so `tests/test_auth_ops.c` covers both ops: `STATS_SERIES` checks every id
in its case like QUERY_MULTI (read right, DENIED per series); `STATS_TENANTS` is P_LOGIN and checks each tenant in its
case (§4). The existing `STATS` op (10) stays as it is. Large `STATS_SERIES` requests run on a worker under the
request's thread cap, like a query.

Cluster and HA: the reply's per-volume rows let a client in direct mode sum a tenant over the leaders of its volumes,
and the entry server in simple mode does the same (design-cluster.md:680). Replicas count their own storage too, but
only leaders are asked, so nothing is counted twice. Per-caller counters are per node; the client sums them. On an
NFS failover (design-ha-modes.md) the new writer's counters start at zero and its sweeper rebuilds storage numbers
from the indexes over NFS (throttled, §3.1). When the cold tier lands, `ts_bytes` and `value_bytes` split into local
and cold (the index stays local, design-cluster.md:79, so the walk is unchanged).

### 5.2 C API (`include/tsdb.h`)

```c
typedef struct tsdb_series_stats { int32_t status; uint8_t cls, type, latest_cached; /* then the fields of 46 */ }
    tsdb_series_stats;
int tsdb_series_stats(tsdb *db, const tsdb_sid *ids, size_t n, int flags, tsdb_series_stats *out);
int tsdb_set_tenants(tsdb *db, const char *const *prefixes, size_t n);
typedef struct tsdb_tenant_stats { /* the by-data block of §2.2, and progress */ } tsdb_tenant_stats;
int tsdb_tenant_stats(tsdb *db, const char *prefix, uint32_t volume, tsdb_tenant_stats *out);
typedef struct tsdb_hot { tsdb_sid id; uint64_t estimate, error; } tsdb_hot;
int tsdb_hot_series(tsdb *db, int kind, tsdb_hot *out, size_t k, size_t *n);
```

`tsdb_volume_stats` gains the fields of §2.3 and `tsdb_options` gains `stats` (ST18): `TSDB_ABI_VERSION` goes up
(include/tsdb.h:796), shared with design-latest's bump if both land in one release.

### 5.3 CLI, clients, metrics

- `tsdb DIR stats` (fixed: logical bytes by each series' type, not 12 bytes a row, tools/tsdb-cli.c:823),
  `tsdb DIR stats series NAME...`, `tsdb DIR stats tenants [PREFIX...] [--volumes]`, `tsdb DIR stats hot`; each
  also with `--server HOST:PORT --login NAME` for a running tsdbd (the only way to see per-caller counters).
- C, Rust, Python and Java clients: `series_stats(ids)`, `series_stats_by_name`, `tenant_stats(prefixes, flags)`;
  the embedded bindings (Rust `Db`, Python `Db`, Java `EmbeddedDb`) the same, compared with tsdbd's answers in their
  tests. Same commit, `make test-clients` green (AGENTS.md "Protocol").
- Metrics (docs/metrics.md): §2.3, and with `--metrics-tenants N` the by-data and by-caller blocks per tenant
  (`tsdb_tenant_disk_bytes{tenant,column}`, `tsdb_tenant_rows`, `tsdb_tenant_series`, `tsdb_tenant_requests_total
  {tenant,group}`, `tsdb_tenant_denied_total{tenant}`, ...). Never per series.

## 6. Correctness boundaries

- **Never slows writes, merges or queries beyond the budget.** Nothing on the write path (§3.4); on the query path
  only per-thread stores (§3.2); the sweeper runs at nice 19 and is throttled (§3.1); `db->lock` only to copy the
  list.
- **Merges, deletes, replaces**: a merge output is a new part, its inputs leave the list; the sweeper adds one and
  subtracts the others. Tombstones and the rows they hide count as stored until a merge drops them (ST1).
- **Retention**: retired partitions leave the list with `dropped`; expired rows inside a partition count as stored
  until a merge drops them; `first_ts` is raised to the cutoff.
- **Class switches** move no rows (AGENTS.md "Partition classes"): nothing changes.
- **Reopen**: counters start at zero; storage is rebuilt (§3.1). The tenant map is rebuilt from the names.
- **Followers**: their view, their sweeper; level-0 parts appearing in the view count as ingest (rows the writer
  holds only in its memtable are not seen until flushed). Per-caller counters are the follower's own requests.
- **Fail-stop**: stats keep answering after `eng_fail`; they never write or fail anything (ST17).
- **Damage**: index reads are clamped (src/internal.h:691-695), so a damaged index gives wrong numbers, never a
  crash; a part found `damaged` is still counted (its bytes are on disk) and is in `tsdb_broken_parts`.
- **Lock order** (AGENTS.md): `stats_mu` and the block registry's mutex are leaves; the sweeper takes `db->lock` and
  the dictionary's `lock` only with no stats lock held, never across I/O.
- **New invariant for AGENTS.md**: "Statistics are never on the write path (append, memtable, WAL, flush, part
  writer, merge), never write a file, never call `eng_fail`, and their locks are leaves."

## 7. Tests

- `tests/test_stats.c` (engine), with an **oracle**: for each series, decode everything with a raw query and the
  index through `granule_read_at`, and compare: `rows`, `nulls`, byte sums, `parts`, `granules` exact; after
  `tsdb_optimize` with no tombstones left, `rows` equals the live count. Tenant totals, once `progress` is complete,
  equal the sum of the per-series numbers of the tenant's series exactly. Run over float32, float64, int64, bool and
  state series, with ids above 2^32 and several volumes.
- **Every way the list changes**: flush, large insert, merge, delete, replace, retention retire, `drop_before`, class
  switch, follower refresh: after each, wait for one tick and compare with the oracle. `retention_dropped_rows`
  equals the rows of the dropped partitions.
- **Approximations within bounds**: hot lists on a Zipf stream with exact counts kept by the test (fixed seed): every
  series above W / k + 3 sampling standard deviations appears, every estimate within its error. HLL within 10% on
  10^5 series. `last_write_s` within one flush interval.
- **Residency**: `posix_fadvise(DONTNEED)` on a part, `resident_bytes` drops; a query, it rises (Linux; prints skipped
  where `mincore` differs).
- **Boundaries**: stats calls after `eng_fail` answer; `stats = -1` leaves no thread and zero counters; a tenant list
  change rebuilds; 10^7 series with an interleaved tenant pattern for the map's worst case.
- **Server**: `test_server` for both ops, the rights (a tenant admin sees its tenant and no other; an unknown tenant is
  DENIED; per-series DENIED), `--no-auth`; `test_auth_ops` gets its rows; metrics names for the new ops;
  `--metrics-tenants` cap. Clients: `make test-clients`; `make integration-tests` gets a stats case per client.
- No jepsen: nothing here changes how data reaches the disk.

## 8. Benchmarks

The budget is a rule (ST18, Summary): stats on against off, repeated 5 times interleaved on a quiet machine, means
and standard deviations reported (benchmark policy):

| Benchmark | Measures | Pass |
|---|---|---|
| `build/insert-bench`: `tsdb_append` 1 thread with `wal_sync_ms = 10`; WAL inserts of 1,000 rows; large inserts of 1 M rows | insert throughput and p99 | within 1% (append: identical, no new code) |
| `build/tsdbc-bench query --conns 1` and `32` | queries/s, p99 | within 1% |
| `build/tsdb-bench` query suite, embedded | per-query time, single-row lookups above all | within 1% |
| merges settled after a load (`tsdb-bench`) | time to settle | within 1% |
| sweeper | CPU and wall time of the first sweep on the 100 M-row set and a 10^7-series set; steady-state CPU under a 1 M rows/s load | at most 1% of one core steady |
| memory | RSS with 10^8 series, tenants in runs and interleaved | as §3.5 |

## 9. Build plan

| Step | What | Gate | Effort (days) |
|---|---|---|---:|
| 1 | Per-thread blocks, the `qs_end` hook, global counters, `tsdb_volume_stats` fields, metrics; `stats` option | make test, asan, tsan; query benchmarks | 2 |
| 2 | Sweeper with volume column totals, `tsdb_series_stats` (on demand), CLI `stats series` and the fixed `stats` | make test, asan, tsan; oracle | 3 |
| 3 | Tenants: list, map, per-tenant totals, ingest, merges, retention, HLL, `tsdb_set_tenants`, CLI `stats tenants` | make test, asan, tsan; oracle on every list change | 4 |
| 4 | Hot lists (reads, writes, bytes per row) and residency | make test; bound tests | 2 |
| 5 | tsdbd: per-uid counters, `--tenants`, the default from `admin` grants, ops 46-47 with `auth_ops` rows, `--metrics-tenants` | make test (both backends), tsan, `test_auth_ops` | 3 |
| 6 | C, Rust, Python, Java clients and embedded bindings | make test-clients, integration-tests | 3 |
| 7 | Benchmarks (§8), docs: metrics.md, protocol comment, CHANGELOG, AGENTS.md invariant and layout line | numbers on a quiet machine | 2 |

About 19 days. Steps 1-2 stand alone (useful without tenants); 5 needs 3; 6 needs 5. Format 9 (S0) needs nothing from
this design and this design needs nothing from it.

## 10. Open questions (with recommended answers)

1. **Without `--tenants`, should the tenants default to the `admin` grant prefixes?** Recommended: yes; a deployment
   with tenant admins gets per-tenant numbers without configuring anything, and `--tenants` overrides it.
2. **Per-tenant metrics on the unauthenticated metrics port: opt-in?** Recommended: yes, `--metrics-tenants N`,
   off by default (A21), N capped at 1,000.
3. **A droppable stats cache file**, so a 10^9-series database does not reread its whole index after every open?
   Recommended: not in the first build; add it (not a format change: a missing or mismatched cache means a rebuild)
   if the measured first sweep on production-sized data exceeds 10 minutes.
4. **Exact live rows on request (`EXACT_ROWS`, a count query per series)?** Recommended: yes, as a flag; the default
   stays physical.
5. **Protocol number**: ride along with 13 or 14 if unreleased when this merges, else 15? Recommended: yes (ST15).
6. **May a user with read rights see a series' hot-list estimates**, which reveal how much others query it?
   Recommended: yes for series the user may read; tenant-level numbers stay with tenant admins and superusers.
