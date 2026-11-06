# The latest point of every series, from a sharded in-memory table

Status: design, not implemented. Branch `latest-design`, written on 2026-11-06 from main at ea89968. Code references
are `file:line` at that commit.

Decided by the user on 2026-11-06:
- tsdb serves the **latest datapoint per series**. This reverses the line "Latest-value lookups ... not planned" of
  2026-10-24 ([roadmap.md](roadmap.md), "Not planned"); the build plan removes it there.
- The answers come **from a distributed, sharded in-memory cache**.

Related designs: [design-cluster.md](design-cluster.md) (volumes led and replicated; this table must fit it),
[design-auth.md](design-auth.md) (rights), [design-series64.md](design-series64.md) (ids name their volume),
design-cdc.md on branch `cdc-design` (positions, `fno`), [compatibility.md](compatibility.md) (what a 0.9.x may
change).

## Summary

- **One question, answered in under a microsecond per series**: "what is the newest point of these series?", for
  1 to a million ids in one request (`QUERY_LATEST`, protocol 13; `tsdb_latest` in C).
- **"Newest" means the greatest timestamp** among the series' live rows, with the value that last-write-wins gives
  at that timestamp. It is exactly what a descending raw query with limit 1 answers today; that query is the
  reference, and the tests compare against it.
- **The cache is sharded by volume** and lives inside each volume's engine: no separate cache service. Every series
  id names its volume, so a lookup routes like any request. In the cluster the volume's leader answers, and its
  replicas keep the same table warm because they apply the same commits.
- **Two layers.** A per-volume **latest table** holds, for each series, the newest live row *in the parts* (16
  bytes: timestamp and value). The **memtables** already hold the newer rows; a one-field addition (`top`, free in
  the struct's padding) finds a series' newest memtable row in O(1). A lookup takes the newer of the two.
- **The table changes only where the parts list changes**: when parts are listed (flush, large insert, delete,
  replace, WAL replay at open), under a lock that lookups share. A lookup therefore never sees a row before
  queries do, nor misses one that queries see. Merges, class switches and the retention clock never touch it.
- **Deletes that remove the newest row** mark the entry *unknown*; the next lookup (or the deleting thread) finds
  the next one with a descending read of one granule per source and fills it in.
- **Cost on the insert path**: a compare and a 4-byte store per memtable row, on a cache line the insert already
  writes; per published part, one table store per series it holds. No new lock on the insert path.
- **Memory**: 16 bytes per series per copy: 16 MB for a million series, 1.6 GB for 10^8, 16 GB for 10^9 (48 GB
  for 10^9 in a cluster of three copies, 6 GB on each of 8 servers).
- **Rebuilt at open** from the granule index (each granule's last value and timestamp, already stored), inside a
  loop the open already runs; a few cases the index cannot give exactly (state series, int64 beyond 2^53, NaN)
  start unknown and are filled in the background. No new file, no format change.
- **As of a time, skipping nulls**: the same op with an end bound or a flag; those go the slow way (one granule per
  source, read backwards), not through the table, unless the bound is above the cached row.
- **Rights**: `read` on each series; denied and missing look alike, as everywhere.

## Assumptions (decisions the user may change)

| # | Assumption | What changes if it does not hold |
|---|---|---|
| A1 | **Latest = the row with the greatest timestamp** among the series' live rows; at that timestamp, the value last-write-wins keeps. Not the most recently *written* row. | "Most recently written" needs a write sequence per row in parts, which tsdb does not keep, and would disagree with every query. It would be a different feature (closer to the change feed of design-cdc.md). |
| A2 | **A null at the greatest timestamp is returned as a null** (with its timestamp). A flag `SKIP_NULLS` asks for the newest non-null row instead (§5.2). | Returning the newest non-null by default would hide that a sensor reports "no value" now; the cost is the same either way (the flag is the slow path). Open question 1. |
| A3 | **Sharded by volume, inside the engine.** Each volume keeps its series' newest points; in the cluster the volume's leader answers and its replicas keep the table warm. No cache service of its own. | A separate cache tier is rejected in L2; it cannot be exact without being on the write path anyway. |
| A4 | **Exact and fresh.** A lookup reflects every acknowledged write and delete, and never a write that queries do not show yet, by the same snapshot rules as a query. | A "fast but maybe stale" mode would need no lock at all, but every client would have to guess how stale. Not offered. |
| A5 | **One instant per chunk of up to 4096 series of one volume**, not per request: a request for 100,000 series may see an insert in some series and not yet in others of the same insert. Each series' answer is exact (A4). | One instant per volume per request needs a lock held across the whole request, which would hold up every publish. Open question 4. QUERY_MULTI with `VALUES`, descending, limit 1 keeps its one cut per volume for those who need it. |
| A6 | **On by default** (`tsdb_options.latest_cache = 1`, tsdbd `--latest-cache on`). Off: no table, no memory, and `QUERY_LATEST` still answers, exactly, through the slow path. | Off by default saves 16 bytes per series for users who never ask. Open question 2. |

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| L1 | **Latest is defined by the existing query**: `tsdb_query_values(series, cutoff, end, DESC, limit 1)` is the reference answer, bit for bit (value bits included, NaN payloads too). | A definition of its own | One meaning in the whole database; the tests have an oracle that is already tested; deletes, retention, LWW and memtable visibility come for free. |
| L2 | **The cache is part of each volume's engine** (`src/latest.c`), sharded by volume like everything else. | A cache service in front (Redis or a tsdb tier); a cache per tsdbd keyed by name | A separate copy must be kept in step with LWW, deletes, replaces, retention and failover; that is the engine's job and it already does it. A cache beside the engine is either on the write path (then it is this design, one hop further) or stale. |
| L3 | **Two layers: a table for the parts, the memtables for newer rows.** | One table updated by every row | Rows in the memtable become visible only when synced (sync mode) or committed (cluster), not when appended; a single table would need a WAL position per entry and an update pass after every group commit, on the commit's critical path. The memtables already hold those rows in series order with their positions. |
| L4 | **Table entry: 16 bytes**, `i64 ts`, `u64 slot` (the series' value slot zero-extended, nulls as the type's null slot, as in the memtable). `ts = TSDB_TS_MAX` (never a row's timestamp) marks the two non-row states: slot 0 **empty** (no rows in parts), slot 1 **unknown**. | 24 bytes with the part's block number; a type tag per entry; a table of doubles (`GF_LAST`) | A writer folds parts in block order (publish_lock), so it never needs the block; the type is per series (SERIES). Doubles are inexact for int64 above 2^53 and absent for state series. |
| L5 | **The table is paged**: 65,536 entries (1 MiB) per page, a page table sized for 2^31 local ids (256 KiB), pages added as SERIES grows, never moved. | One array, reallocated | Growing a 1.6 GB array copies it under the lock. Pages make growth O(1) and keep entries in place. |
| L6 | **Memtable layer: `top`**, a u32 in `mt_series` (the index of the row with the greatest timestamp, the later one on ties), set by `mt_add`. It fits in the struct's padding (40 bytes stay 40). | A copy of the newest row in each `mt_series` (64 bytes) | Same answer with one more cache miss per lookup; 24 more bytes per series slot in every memtable, which is dense by local id. |
| L7 | **The table changes only where the parts list changes**, after the commit point (intent and renames durable, `parts_commit` returned 0) and before the listing, under a per-volume **`latest_rw`** held for writing until the listing is done. Lookups hold it for reading per chunk. | Changing it after the listing; a seqlock | After the listing, a lookup could see a query's answer missing for a moment (and the reverse order, before the commit point, could show a write that then fails). A seqlock retries forever under steady large inserts; a writer-preferring rwlock bounds both waits. |
| L8 | **Merges, class switches and the retention clock never change the table.** Lookups apply the cutoff (`eng_cutoff`) at read time, as queries do. | Updating it on merges | A merge keeps every live row; it only drops rows below the cutoff and rows tombstones hid, which the table never holds as live. |
| L9 | **Deletes and replaces fold by a rule** (§3.4): the entry stays, moves to the replace's newest row, or becomes unknown. | Recomputing every touched series at once under the lock | The rule is exact in every case it decides; only "the newest row was deleted and nothing replaced it" needs a read, and that read must not run under the lock. |
| L10 | **Unknown entries are filled by a read** of the parts only, the newest row found by walking back one granule per source (§3.8), stored only if no publish happened since the read began (`lgen`). Filled by the next lookup, by a delete for its own series, and by a background warmer after open. | Filling under the lock; filling from the index alone | A read under the lock stalls publishes for the time of a decode. A granule touched by a tombstone must be decoded (snapshot windows), so the index alone cannot answer. |
| L11 | **Rebuilt at open**, inside `cls_check_parts` (classes.c:1095), which already walks every run of every part: per run, the last entry's timestamp, last value and null flag (`GF_TSMIN + GF_SPAN`, `GF_LAST`, `GF_FLAGS`), folded by (ts, block); then every tombstone checked against the result. No file of its own. | A LATEST file written at close; a lazy build on first use | A file is one more durable thing with fail-stop and torn-tail rules, and is wrong after a crash anyway. The index already holds what the build needs. A lazy build delays the first answers by minutes on a big volume. |
| L12 | **What the index cannot give exactly starts unknown**: state series (no time-weighted statistics, twa.c:126-131), int64 whose last value as a double is at or beyond 2^53 in magnitude (twa.c:70-73), NaN last values (the payload). | Decoding their last granule during the open | A million state series would add 10 to 50 s to the open. Unknown entries answer exactly through the slow path until the warmer fills them. |
| L13 | **NFS followers keep the block number too** (24-byte entries) and fold each refresh's new parts by (ts, block), with the tombstone rule against `min_block`. | Rebuilding at every refresh | A follower sees merge outputs as new parts; without the block, a merge output could overwrite a newer row at an equal timestamp. A rebuild per refresh costs the whole index walk. |
| L14 | **One op, `QUERY_LATEST`**, protocol 13: ids (1 to 1,048,576), an optional end bound (one, or one per id), a `SKIP_NULLS` flag; replies of fixed 24-byte records in index order. The floor stays 12. | Names in the request; a QUERY_MULTI kind | Clients already resolve and cache ids (insert paths need them). A QUERY_MULTI kind would carry a template and a frame per series: 30 bytes of framing for 24 of answer. |
| L15 | **Rights: `read` on each series**, checked per id in the op's case as QUERY_MULTI does; a denied id answers `TSDB_ERR_DENIED` in its own record, whether or not it exists. | Refusing the whole request | The same rule as QUERY_MULTI: a dashboard with one revoked series still shows the rest. |
| L16 | **The cluster changes nothing in the table**: replicas apply `COMMIT` markers through the same listing path and see memtable rows up to the commit point, so their tables match the leader's at every commit point. Clients split by leader as QUERY_MULTI does (cluster C16); `REPLICA_OK` and `min_pos` as for queries. | A cluster-wide latest service | It would be a second replicated state to fence at failover. Here a new leader's table is already warm. |
| L17 | **The slow path gets a real "limit 1"**: a descending read that starts with one granule per source, not 65,536 rows (`desc_window_rows`, query.c:1926). | Using raw_desc as it is | raw_desc decodes about 8 granules to return one row; the slow path is the as-of, skip-nulls and unknown path, so it should cost one granule. |
| L18 | **No format change.** The index already stores what the build needs (format 8). An exact last slot in the index (`GF_LASTV`) would make the build exact for every type; it goes into format 9 if and when the change feed and the cluster bump it, not on its own. | A format bump now | A format change costs every user an export and import (compatibility.md); a few unknown entries filled in the background do not justify it. Open question 5. |

## 1. What "latest" means

For a series s, a lookup with end bound `end` (default `TSDB_TS_MAX`, "no bound") answers:

- the row with the greatest timestamp `t` such that `cutoff(s) <= t < end` among s's live rows (rows a tombstone
  hides and expired rows are not live; memtable rows count once they are visible, the same rule as queries);
- its value, as the last write at `t` left it (last-write-wins, so a later null at `t` makes it a null);
- a kind: **value**, **null** (the row is a null), or **none** (no live row before `end`; also for an unknown id).

With `SKIP_NULLS` the row is the greatest `t` whose value is not null (A2). The answer equals
`tsdb_query_values(s, cutoff, end, {DESC, 1})` (L1) with a null mapped to kind null; the tests check exactly this.

Two consequences worth saying:
- A stray row far in the future (a device with a wrong clock) is the latest until it is deleted. A client that
  wants "the newest point that is not in the future" sends `end = now + 1`; that is the slow path (§5.1).
- A series that has never been written, and one whose every row expired or was deleted, both answer **none**.

## 2. The two layers

### 2.1 The latest table (parts)

Per volume (engine), indexed by local id: `ts` and `slot` (L4), in pages of 65,536 entries (L5). A new series gets
an empty entry (its page is created, zeroed to "empty", when SERIES grows past the last page; the series' first
rows reach parts only through a listing, which folds them in).

`latest_rw` (a `pthread_rwlock_t`, writer-preferring: `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`) guards the
entries and `lgen`, a counter that every change bumps (it tells a fill whether anything changed, §3.8).

Memory, 16 bytes per series per copy (page table 256 KiB per volume besides):

| series | one copy | three copies (cluster) | per server, 8 servers |
|---:|---:|---:|---:|
| 10^6 | 16 MB | 48 MB | 6 MB |
| 10^8 | 1.6 GB | 4.8 GB | 600 MB |
| 10^9 | 16 GB | 48 GB | 6 GB |

The production servers have 256 GB; 6 GB for a billion series is within the sizing rule of design-cluster.md §10.2.
NFS followers add 8 bytes (L13).

### 2.2 The memtable layer

The active memtable, the flushing ones (up to `MAX_FLUSHING` = 4, internal.h:1192) and a follower's WAL tails hold
every row that is newer than the parts, indexed by local id (`mt_series`, internal.h:1120). `mt_add`
(memtable.c:137) gains one line per row: `if (!s->n || ts[i] >= s->ts[s->top]) s->top = s->n;` before the append.
The `mt_series` line is already in cache there (it just wrote `n` and checked `sorted`).

A memtable's newest *visible* row of s: if `lsn[top] <= visible` (always, outside sync mode and the cluster), it
is row `top`. Otherwise the newest row is still in flight (its group commit has not synced); then, for a sorted
series, binary search the visible prefix by `lsn` (as `mt_collect` does, memtable.c:226) and take its last row; for
an unsorted one, scan the visible prefix. On equal timestamps within one memtable the later row wins, as
`keep_last` does in queries.

### 2.3 A lookup

For the ids of one volume, in chunks of up to 4096 (A5):

1. Take `latest_rw` for reading.
2. Take `db->lock`, then `buf_lock`, as `snapshot_take` does (query.c:215-246): read `visible` (in sync mode
   `wal_durable`, in the cluster the commit point, else no limit), and for each id the newest visible row of the
   active memtable and of each follower tail. Release `buf_lock`. Reference the flushing memtables and release
   `db->lock`; they are immutable and read without a lock.
3. For each id: the candidates are, newest source first, the active memtable, the flushing memtables (newest
   first), the table entry. The answer is the candidate with the greatest timestamp; on equal timestamps the newer
   source wins (memtables rank above parts, as in queries). An unknown entry sends the id to the slow path
   (§3.8): the parts may hold a row newer than any memtable row.
4. Release `latest_rw`. Apply the cutoff: a newest row below `eng_cutoff(s)` means no live row (every other row
   is older still). Apply the end bound and `SKIP_NULLS` (§5).

Cost, estimated (U1): an id costs a table read (one cache miss at large sizes, none when the ids come in order) and
one `mt_series` read per memtable holding the series (one miss, plus about two for the row). About 0.2 to 0.4 µs
per id on one thread; 100,000 ids in 20 to 40 ms on one thread, split over the request's threads (the query
scheduler, `request_threads`) by chunk. `buf_lock` is held for at most a chunk's memtable reads (about 4096 x 0.1
µs = 0.4 ms worst case; chunks of 1024 if the measurements say inserts notice).

## 3. Keeping it exact under every write

### 3.1 Small inserts (the WAL and memtable path)

Nothing but `top` (§2.2). A row appended to the active memtable is found by step 2 of a lookup exactly when
`mt_collect` would show it to a query: the same `visible` bound. **Sync mode** (`wal_sync_ms = 0`): a row is
returned only once the WAL is synced up to it; the row that is still in flight costs a binary search, not a wrong
answer. **Periodic sync**: visible at once, as for queries. **Cluster**: visible at the commit point (cluster C7).

A small write waiting on a large write's fence (`lw_fence`, internal.h:1490-1514) has appended nothing, so no
lookup can see it before the large write is listed: the fence order holds for lookups as it holds for queries.

### 3.2 Flushes

`flush_waiting` (memtable.c:777) commits a memtable's parts, then lists them and removes the memtable from
`flushing` in one `db->lock` section (memtable.c:801-810). The table changes between `parts_commit` and that
section, under `latest_rw` for writing, released after the section:

- `mt_write` already sorts each series and keeps the last write per timestamp; it records each series' newest row
  `(ts, slot)` in a list (one entry per series in the memtable) handed to the publish.
- Fold, per series: if the entry is empty, or known with `ts <= new.ts`, store the new row (the flushed part has a
  higher block number than every listed part, so it wins ties). An unknown entry stays unknown.

A lookup sees either the memtable in `flushing` (and maybe also the updated entry: the same row twice, harmless)
or the listed parts and the updated entry; never neither, because both changes happen under the write lock.

To keep the write lock short, the publisher computes the new entries before taking it (only publishers change
entries, and they are serialised by `publish_lock`, so the table is stable for it), then stores them. Stores go in
id order: a flush of a million series writes about 16 MB in page order, 2 to 5 ms (U2); lookups of that volume
wait that long, queries do not.

### 3.3 Large inserts

`insert_rows` (insert.c:640) writes parts, then `parts_publish` (insert.c:626) commits and lists them under
`publish_lock`. The part writer notes each series' last row as it closes the series' run (one compare per series
boundary); the fold is that of a flush, at the same place (after `parts_commit`, before `db_add_parts`,
db.c:149). The replay at open publishes through `parts_publish` too (memtable.c:1209) and folds the same way.

The fence is untouched: the large write's parts are listed and folded under one write lock, then `lw_leave` lets
the small writes behind it in. A small write that waited therefore lands in a memtable after the fold; a lookup
ranks it above the large write's row at an equal timestamp, which is last-write-wins in real time (jepsen check 5).

### 3.4 Deletes and replaces

delete.c:150-170 commits tombstone parts (and, for a replace, the rows' parts) and lists them under `publish_lock`.
Under the write lock, per series s of the commit, with `D` the union of its tombstone ranges and `R` its newest row
among the commit's rows (none for a delete):

| entry before | rule | why |
|---|---|---|
| empty | `R` if any, else stays empty | nothing older to hide |
| unknown | stays unknown | nothing known to compare |
| known, `ts` not in `D` | the newer of entry and `R` (`R` wins ties) | the tombstones did not touch the newest row |
| known, `ts` in `[a, b)` of `D`, `R` exists and `R.ts >= a` | `R` | every older row that survives is below `a`, so below `R` |
| known, `ts` in `D`, otherwise | **unknown** | the next live row is somewhere below; only a read can say where |

Tombstones hide only rows of lower blocks (design.md, "Tombstones live in parts"), and every listed part has a
lower block than the commit, so the rule never asks about blocks. Memtable rows are never hidden; a delete
flushes the memtable before it writes its tombstones, so any row a lookup still finds in a memtable is newer than
the delete.

After its publish, a delete fills the entries it made unknown (§3.8) before it returns: a delete names one series,
so that is one read, and the next lookup is fast again.

### 3.5 Retention

Lookups clamp to `eng_cutoff(s)` at read time (L8), as `eng_query_raw_ex` does (query.c:2024). The entry keeps the
newest row in parts whatever its age; if it is below the cutoff, every row of the series is, and the answer is
none. The cutoff only rises (`eng_now_ms` never goes back), with one exception: `tsdb_set_retention` with a
longer limit lowers it, and rows a merge already dropped do not come back while the entry may name one. So a
retention change that lowers a series' cutoff marks its entry unknown when the entry's `ts` lies below the old
cutoff (under the write lock, after the RETENTION record is synced). The merge thread's partition drops remove
only expired rows: no change.

### 3.6 `tsdb_drop_before`, class switches, merges

- `tsdb_drop_before(t)` removes whole partitions below `t`: every entry with `ts < t` becomes unknown (refilled
  lazily; most such series have nothing left and become empty).
- Class switches move no row (AGENTS.md, "Partition classes"): no change. One (series, ts) is never in two keys,
  so ties between parts of different keys cannot happen.
- Merges (merge.c:463) list their output and retire the sources with the same live rows: no change (L8).

### 3.7 Fail-stop

The table changes only after `parts_commit` succeeded (the commit point) and only together with a listing, so a
publish that fails (and the engine with it) leaves the table as the parts list: as queries see it. After
`eng_fail` no new file is named and nothing is listed, so the table stops changing; memtable rows stay invisible
above `wal_durable` in sync mode. Lookups answer what queries answer on a failed engine, until tsdbd exits.

### 3.8 Unknown entries and the fill

A fill computes the newest live row of s **in the parts only** (memtables are the other layer):

1. Read `lgen` (under the read lock) as `g`.
2. Take a parts-only snapshot of s over `[cutoff, TSDB_TS_MAX)` (a `snapshot_take` variant that skips memtables;
   inside a `QUERY_LATEST` request, from the request's cut, `cur_cuts`, so that 100,000 fills do not each copy the
   parts list).
3. `latest_at` (§5.1): one granule per source, read backwards.
4. Take the write lock; if `lgen == g` and the entry is still unknown, store the result (or empty) and bump `lgen`.
   Otherwise drop it (a publish came in between; the lookup answers from the slow path anyway).

The lookup that found the entry unknown answers through the full slow path (memtables included) at once, exact by
construction, and leaves the fill to run after its answer is sent. A **background warmer** per volume (one low
priority job of the query scheduler) fills entries left unknown by the open (§3.9), in id order, a few thousand a
second per volume so it never competes with queries; `tsdb_latest_unknown` (a metric) says how many remain.

### 3.9 Open, crash recovery

At open, after `load_parts` and before the WAL replay (db.c:646), `cls_check_parts` walks every run of every part
and already reads each entry's timestamps. It also builds the table, with a temporary block array:

- per run (series s in part p): the last entry's `ts_max`; if it is greater than the entry's `ts`, or equal and
  `p`'s block is higher, read `GF_LAST` and the `GT_LAST_NULL` flag (two field reads, only when it wins) and store
  it, unless the value is not exact (L12), in which case store the timestamp and block with "value unknown";
- then every part's tombstones: a range of s that holds the entry's `ts`, in a part whose `min_block` is above the
  entry's block, makes the entry unknown (the row is hidden; what survives below is not known from the index);
- entries never touched: empty; entries "value unknown": unknown.

The WAL replay then publishes its rows through `parts_publish`, which folds them (§3.3). A crash leaves nothing to
repair: the table is memory only, rebuilt from parts that are themselves crash-consistent (intent files).

Time: the walk is the open's own; the build adds a compare per run and two field reads per winning run (U3:
below 10% of `cls_check_parts`, to be measured). Unknown entries after the open: every state series, int64 series
with huge last values, NaN last values, and series whose newest rows were deleted but not yet merged. They answer
through the slow path (exact) until the warmer reaches them. Estimate for 10^8 series in a cluster (about 3.75 x
10^7 series copies per server): the build is seconds per server, run per volume in parallel as volumes open; the
warmer, at 20 µs per fill and a few thousand fills per second per volume, takes minutes for a million state series
per server, with exact answers meanwhile.

### 3.10 NFS followers

A follower lists parts from MANIFEST and tails the writer's WAL (`db->tails`). Its table keeps the block number
(L13). At each refresh, under the write lock, before the new list is installed: every newly listed part (new
publishes and merge outputs alike) folds by the open's rule (`(ts, block)` from the index, tombstones against
`min_block`); a partition dropped (retention, `drop_before`) makes the entries with `ts` below its end unknown.
Merge outputs fold to the rows they already had, and a merge output can never overwrite a newer row at an equal
timestamp, because its block range is below that row's block. WAL tail rows are the memtable layer (visible
always, as `snapshot_take` reads them with no bound, query.c:240).

## 4. Cost on the insert path

| path | per row | per batch or part | lock |
|---|---|---|---|
| small insert (memtable) | one compare, sometimes a u32 store, on the `mt_series` line `mt_add` writes anyway | none | `buf_lock`, already held |
| flush | none | one `(ts, slot)` per series in the memtable, from `mt_write`'s sorted runs; one 16-byte store per series | `latest_rw` for writing, for the stores only (§3.2) |
| large insert | none (the part writer compares at each series boundary) | as a flush, per series of the batch | same |
| delete, replace | none | the §3.4 rule per series of the commit, and one fill per entry made unknown | same |
| merge | none | none | none |

No new lock on the insert path. Writer threads do not contend more than today: memtable appends are serialised by
`buf_lock` already, publishes by `publish_lock`. What is new is that a publish waits for lookups holding the read
lock (at most a chunk, about 0.4 ms), and lookups wait for a publish's stores (a few ms for a million-series
flush). The benchmark plan (§9) measures insert throughput with the table on and off.

## 5. Queries beyond "the newest"

### 5.1 As of a time (an end bound)

`end` below the timestamp a lookup found in step 3 means the answer lies below; the table cannot help. The id goes
to `latest_at(snapshot, s, end)`, the slow path, also used by fills and `SKIP_NULLS`:

- per source of the snapshot (parts through their snapshot windows, `snap_find`; memtable runs): the last granule
  whose `ts_min < end`. If its `ts_max < end` and it is a plain granule (not moved to a mem run by a tombstone,
  not a state series, not an int64 beyond 2^53 or a NaN, L12), its candidate is `(ts_max, GF_LAST)` from the index
  without decoding; otherwise decode that one granule and take its last row below `end`;
- the candidate with the greatest timestamp wins, the higher source priority (`mem_prio`) on ties; candidates from
  the index whose timestamp ties another source's are decoded first only if they would lose the tie, which the
  priority says without decoding.

That is at most one granule decoded per source, and usually none. It replaces raw_desc for `limit = 1` (L17);
raw_desc keeps its windows for larger limits. Inside one `QUERY_LATEST` the snapshots come from one cut per volume
(as QUERY_MULTI's), so an as-of request for 100,000 series reads one parts list per volume, not 100,000.

### 5.2 Skipping nulls

When the newest row is a null and `SKIP_NULLS` is set: `latest_at` again with `end` = that row's timestamp, and per
source granules whose null count equals their row count are stepped over from the index; others are decoded. A
series that has been null for weeks walks back that far (the cost of a descending query to its last value).

### 5.3 Statistics

`tsdb_last_query_stats` after a lookup counts the granules and rows the slow path decoded; two new counters say how
many series the table answered and how many went the slow way (also as metrics, §6.5).

## 6. Interfaces

### 6.1 C API (`include/tsdb.h`)

```c
typedef enum { TSDB_LATEST_VALUE = 0, TSDB_LATEST_NULL = 1, TSDB_LATEST_NONE = 2 } tsdb_latest_kind;

typedef struct tsdb_latest {
    int32_t status;   /* 0, or this series' error: TSDB_ERR_INVALID for a malformed id */
    uint8_t kind;     /* tsdb_latest_kind; NONE for an unknown id */
    uint8_t type;     /* the series' tsdb_type (0 with an unknown id) */
    int64_t ts;       /* with VALUE and NULL */
    union { float f32; double f64; int64_t i64; uint8_t b; uint32_t state; } v; /* with VALUE */
} tsdb_latest;

typedef struct tsdb_latest_opts {
    int64_t end;          /* only rows before it; TSDB_TS_MAX: no bound */
    const int64_t *ends;  /* per series instead of end, or NULL */
    int skip_nulls;       /* nonzero: the newest non-null row */
    int threads;          /* 0: tsdb_options.request_threads; more is clamped to it */
} tsdb_latest_opts;

/* out[i] for ids[i], in index order; n up to 1 << 20. opts NULL: no bound, nulls returned. Returns 0,
 * TSDB_ERR_INVALID (n out of range) or TSDB_ERR_NOMEM; per-series problems are in out[i].status. */
int tsdb_latest(tsdb *db, const tsdb_sid *ids, size_t n, const tsdb_latest_opts *opts, tsdb_latest *out);
```

`tsdb_options.latest_cache` (default 1, A6). Ids are 64-bit (`tsdb_sid`), never narrowed (AGENTS.md, "Series
ids"); the call splits them by volume and runs each volume's chunks on the query scheduler. The CLI gets
`tsdb DIR latest NAME...`. `tsdb_abi_version` goes up (new struct and call used by the bindings).

### 6.2 Wire (protocol 13)

```
QUERY_LATEST (39)  u8 flags, u8 0, u16 threads, u32 n (1..1048576), i64 end, [i64 ends[n]], u64 ids[n]
                   flags: TSDB_LATEST_SKIP_NULLS 1, TSDB_LATEST_PER_END 2 (ends[] present; `end` must be 0);
                   other bits reserved (TSDB_ERR_INVALID). The length is exactly that.
  -> frames, each: i32 status, u32 first, u32 m, u8 last, u8 0, u16 0, then m records of 24 bytes:
                   i32 status, u8 kind, u8 type, u16 0, i64 ts, u64 value (the series' slot zero-extended:
                   float32 bits in the low 4 bytes, bool 0/1, state code; 0 with NULL and NONE)
                   records in index order, at most 65,536 per frame; the frame with last = 1 ends the reply.
                   A bad header gets one frame with INVALID, m = 0, last = 1.
```

- Version rules (compatibility.md): the server speaks 13; the floor stays 12. A client asked for `latest` by a
  caller while connected to a 12 server fails with `TSDB_ERR_INVALID` without sending. HELLO, every other op and
  the floor are unchanged, so a 13 client talks to a 12 server for everything else.
- tsdbd runs it on a worker like QUERY_MULTI (the event loop only parses), counts it against the connection's
  request budget, and honours CANCEL between chunks as it does for QUERY_MULTI (version 10 rules).
- Records are fixed size so that clients decode them without branching; 100,000 ids answer in 2.4 MB.

### 6.3 Rights

`auth_ops` gets a row for QUERY_LATEST: any login, right `read`, series "the ids of a list, checked in the case"
(as QUERY_MULTI, design-auth.md A14). In the case, each id goes through the user's rights map (A15); a denied id,
and a missing id for a user without `read` on `all`, gets `status = TSDB_ERR_DENIED` in its record and the rest
are answered. A missing id for a user with `read` on `all` (and with `--no-auth`) is kind NONE, status 0, as an
unknown id is for queries. `tests/test_auth_ops.c` fails until the row exists.

### 6.4 Clients and bindings (same commit, `make test-clients`)

- C client: `tsdbc_latest(c, ids, n, opts, out)` with the structs of §6.1.
- Rust: `Client::latest(&[Sid], &LatestOpts) -> Result<Vec<Latest>>`, `Latest { status, kind, ts, value: Value }`;
  the embedded `Db::latest` the same.
- Python (PyO3 over the crate): `latest(ids, end=None, ends=None, skip_nulls=False)` returning a list of
  `(ts, value)` tuples, `None` for NONE, a `DeniedError` instance in place for a denied id (or raising, by a flag);
  embedded `Db.latest` the same.
- Java: `long[]` ids (never `int`), `Latest[] latest(long[] ids, LatestOptions o)`; `EmbeddedDb.latest` the same.
- The Pulsar sink does not use it.

Each client's tests compare its answers with the embedded binding's and with tsdbd's for the same data, nulls,
denied ids and unknown ids included.

### 6.5 Metrics (`docs/metrics.md`)

`tsdb_latest_requests_total`, `tsdb_latest_series_total{path="table"|"slow"}`, `tsdb_latest_unknown` (entries left
to fill, per volume), `tsdb_latest_table_bytes`, `tsdb_latest_wait_seconds_total` (time lookups waited for a
publish's write lock).

## 7. The cluster

The design follows design-cluster.md without additions to its log or its Raft state:

- **Shards are volumes.** A series id names its volume; the volume's leader answers (cluster C1, C15). In direct
  mode the client splits a `QUERY_LATEST` by the leader of each id's volume (TOPOLOGY), sends one request per
  server with its share, and puts the records back in index order (the frames carry `first`); in simple mode the
  entry server does it. This is cluster C16 for QUERY_MULTI, with no budget to split.
- **Replicas keep the table warm.** A replica applies a `COMMIT` marker by `parts_commit` and listing (cluster
  §5.2): the same fold. Its memtable rows are visible up to the commit point (C7), the same `visible` that
  lookups use. `NOW` markers move the cutoff, which lookups read; a `RETENTION` marker that lowers a cutoff and a
  `DROP` marker make entries unknown as in §3.5 and §3.6; `CLASS` changes nothing. So every in-sync replica's
  table, read at a commit point, answers what the leader's answers at that point.
- **Failover**: the new leader was an in-sync replica, so its table is already complete; nothing is rebuilt. A
  returning replica that cuts a divergent WAL tail (C14) never listed parts above the commit point, so its table
  holds nothing uncommitted; its memtables are cut with the WAL.
- **A new replica** (learner, cluster §8.1) opens its snapshot like any open (§3.9) and then applies the log.
- **Reads on replicas**: `REPLICA_OK` per request, and `min_pos` for read-your-writes (cluster §7.2), apply to
  `QUERY_LATEST` as to queries; a leader answers only within its lease (§7.1).
- **Cold tier**: every replica keeps the index of cold partitions locally (cluster §9), so the open's build is
  unchanged; a fill or an as-of read that needs a cold granule's rows reads them from the cold tier (rare: only a
  series whose newest rows are cold and were deleted, or an as-of bound in cold time).

Memory in the cluster: three copies (A3), so 48 GB for a billion series spread over the servers (§2.1). A
replication factor of 4 (cluster D1) adds a copy.

## 8. Tests

- **`tests/test_latest.c`, an oracle test.** Random sequences on a few hundred series of every type, ids above
  2^32 and in several volumes: small and large inserts (in and out of order, duplicate timestamps, nulls, NaN with
  payloads, int64 near `INT64_MIN`/`INT64_MAX` and beyond 2^53), deletes of the newest rows and of other ranges,
  replaces with and without rows, retention set, raised and lowered with a test clock, `drop_before`, class
  switches (partition classes on), flushes, merges and `tsdb_optimize`, close and reopen, reopen after a simulated
  crash (fork, kill), sync and periodic WAL modes. After every step, for every series: `tsdb_latest` equals
  `tsdb_query_values` ascending over everything, last row (the oracle is a full scan, not the descending path),
  bit for bit; with random end bounds and with `SKIP_NULLS` too. Also with `latest_cache = 0` (the slow path alone)
  and with `desc_window_rows` small.
- **Concurrency** (in the same file, run under `make tsan`): writer threads doing small and large inserts and
  deletes, lookup threads checking that each answer is one the history allows (its row was written by a write that
  had begun; no acknowledged newer row is missed; a series' answers never go back in time without a delete in
  between). Test hooks pause a publisher between its stores and its listing, and a fill between its read and its
  store, to force the races of §3.2 and §3.8.
- **Followers**: a writer and a follower on one directory, lookups compared after each refresh, merges running.
- **tsdbd** (`tests/test_server.c`, uring and epoll): the frame, chunks across frames, unknown, malformed and
  denied ids, `PER_END`, CANCEL, a 12 client against a 13 server and the reverse.
- **Rights**: `test_auth_ops` row; denied and missing alike for a user without `read` on `all`.
- **Fuzzing**: QUERY_LATEST seeds in `tests/fuzz/frames/` and `make fuzz-frames FUZZ_SECONDS=600`.
- **Jepsen** (kv and classes workloads, every nemesis, `--auth` too): clients also send `QUERY_LATEST`. A latest
  answer `(t, id)` for series s is checked as two reads of the existing model: a read of key `t` that returned
  write `id` (checks 1 to 5: no phantom, no lost, no future, no stale value) and a range read `(t, end)` that
  returned nothing (no acknowledged write above `t` that a later write or delete had not removed). An answer of
  none is a range read of everything returning nothing.
- **Integration** (`tests/integration/run.py`): `latest` in the matrix, every client, both backends, auth on and
  off.
- **Clients**: `make test-clients`, each comparing tsdbd, the embedded binding and the C API.

## 9. Benchmarks to run (quiet machine, five runs, the mean)

- **Insert cost**: `build/insert-bench` (1 to 10,000 rows per call through the memtable, 1 M rows straight to
  parts, 1 and 64 threads) with `latest_cache` on and off. Expected: within noise on the memtable path, below 2%
  on large inserts.
- **Lookups**: `build/tsdbc-bench latest --ids 1|100000 --conns 1|32` against tsdbd on the 100 M-row data set
  (100 series) and on 10^6 and 10^7 series (scale-bench data); the same embedded (`build/tsdb-bench`, a latest
  class). Requests per second and series per second; with steady inserts running alongside, and with the slow
  path forced (`latest_cache` off) to show what the table buys.
- **As of and skip nulls**: the same, with an end bound a day back.
- **Rebuild**: open time of a scale-bench database with the table on and off; time until `tsdb_latest_unknown`
  reaches 0 with a quarter of the series of type state.

`tools/bench-tsdb.sh` gets the latest classes, and AGENTS.md's benchmark section the numbers.

## 10. Build plan

Each step is a track (tsdb-tracks): build, adversarial review, fix, gates, merge.

| step | what | gates | effort |
|---|---|---|---|
| 1 | `latest_at` (the one-granule descending read, §5.1) and `tsdb_latest` on the slow path alone (no table), C API, `tests/test_latest.c` with the oracle | `make test`, `make asan` | 2 days |
| 2 | The table: `src/latest.c`, pages, `latest_rw`, the folds at every listing (flush, insert, replay, delete, replace), retention and `drop_before`, the build in `cls_check_parts`, fills and the warmer, `top` in `mt_add`, followers; concurrency tests and hooks | `make test`, `asan`, `tsan`, `make jepsen` (it touches the publish path) | 4 to 5 days |
| 3 | Protocol 13: QUERY_LATEST in tsdbd, `auth_ops`, the C, Rust, Python and Java clients and embedded bindings, the CLI, metrics, fuzz seeds; jepsen's latest check | `make test`, `test-clients`, `integration-tests`, `fuzz-frames` 600 s, `make jepsen` with the new check | 4 days |
| 4 | Benchmarks (§9), README and docs (protocol comment, metrics.md, CHANGELOG, roadmap: the "not planned" line goes), AGENTS.md (the invariant below) | numbers on a quiet machine | 1 to 2 days |
| 5 | Cluster: the client split per leader, REPLICA_OK and `min_pos` for the op; with the cluster's own steps | cluster simulators and `make jepsen-cluster` | part of the cluster work |

About two weeks for steps 1 to 4.

**AGENTS.md gains one invariant**: *The latest table changes only together with the parts list* (after
`parts_commit`, under `latest_rw` for writing, released after the listing); a new listing point must fold there,
and anything that removes live rows other than tombstones of a later block (a retention limit lowered, a drop)
must make the entries it may affect unknown. Lock order: `publish_lock` > `latest_rw` > `db->lock`; lookups take
`latest_rw` (read) > `db->lock` > `buf_lock`; never held across I/O.

**What lands in a 0.9.x**: protocol 13 (one new op; floor still 12, so no client breaks) and the C API
(`tsdb_abi_version` up). No disk format change: nothing to migrate. The changelog says under "Added": the op, the
call, the option, the metrics; under "Changed": protocol 13, the ABI version.

## Open questions

1. **A null at the newest timestamp: return the null, or the newest non-null?** Recommended: return the null by
   default (it is what the series says now), `SKIP_NULLS` for the newest known value (A2).
2. **On by default?** Recommended: on, for tsdbd and embedded (16 bytes per series; a few seconds at open on big
   volumes); off is one option for users who never ask (A6).
3. **Names in the request?** Recommended: ids only on the wire; the clients resolve names (and cache the ids) in
   a `latest_by_name` helper. A "latest of every series under a selector" op (prefix or glob, for dashboards) can
   follow if asked; it needs a name listing per volume, which is its own design.
4. **One instant per request, or per chunk of 4096 series?** Recommended: per chunk (A5), documented; QUERY_MULTI
   (values, descending, limit 1) remains for the rare caller that needs one cut per volume.
5. **An exact last value in the granule index (`GF_LASTV`)?** Recommended: yes, but only inside format 9 when the
   change feed and the cluster bump the format; until then state series and huge int64 values start unknown after
   an open and are filled in the background (L12, L18).
6. **Future timestamps**: should "latest" default to `end = now + 1` so a device with a wrong clock does not stick?
   Recommended: no; the default is the greatest timestamp (A1), and callers that distrust clocks pass the bound
   (the slow path). A metric of series whose latest is more than an hour ahead of the clock would help operators.

## Unverified assumptions

- U1: lookup cost of 0.2 to 0.4 µs per id on one thread; to be measured (§9).
- U2: 2 to 5 ms of stores for a million-series flush, during which lookups of that volume wait.
- U3: the build adds below 10% to `cls_check_parts` at open.
- U4: a fill (parts-only snapshot from the request's cut, one granule per source) costs about 20 µs.
