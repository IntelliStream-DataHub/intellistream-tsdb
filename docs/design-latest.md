# The latest point of every series, from a sharded in-memory table

Status: design, not implemented. Branch `latest-design`, written on 2026-11-06 from main at ea89968, revised the
same day after a correctness review and a performance review, and again after a verification of both (what changed:
"Changes after review", at the end).
Code references are `file:line` at ea89968.

Decided by the user on 2026-11-06:
- tsdb serves the **latest datapoint per series**. This reverses the line "Latest-value lookups ... not planned" of
  2026-10-24 ([roadmap.md](roadmap.md), "Not planned"); the build plan removes it there.
- The answers come **from a distributed, sharded in-memory cache**.

Related designs: [design-cluster.md](design-cluster.md) (volumes led and replicated; this table must fit it),
[design-auth.md](design-auth.md) (rights), [design-series64.md](design-series64.md) (ids name their volume),
design-cdc.md on branch `cdc-design` (positions, `fno`), [compatibility.md](compatibility.md) (what a 0.9.x may
change).

## Summary

- **One question, answered in well under a microsecond per series**: "what is the newest point of these series?",
  for 1 to a million ids in one request (`QUERY_LATEST`, protocol 13; `tsdb_latest` in C).
- **"Newest" means the greatest timestamp** among the series' live rows, with the value that last-write-wins gives
  at that timestamp. It is exactly what a descending raw query with limit 1 answers today; that query is the
  reference, and the tests compare against it.
- **The cache is sharded by volume** and lives inside each volume's engine: no separate cache service. Every series
  id names its volume, so a lookup routes like any request. In the cluster the volume's leader answers, and its
  replicas keep the same table warm because they apply the same commits.
- **Two layers.** A per-volume **latest table** holds, for each series, the newest row *in the parts* (16 bytes).
  The **memtables** already hold the newer rows; a one-field addition (`top`, free in the struct's padding) finds a
  series' newest memtable row in O(1). A lookup takes the newer of the two.
- **Entries are read and written only under one per-volume lock, `latest_rw`**: publishers (flush, large insert,
  delete, replace, WAL replay) change them in the same write section that lists their parts; merges list under it;
  retention changes, drops and queued fills write under it too. A lookup therefore never sees a row before queries
  do, nor misses one that queries see.
- **An entry can be *unknown*** (after a delete of the newest row, a retention limit lengthened, a drop, or an open
  that could not read the value exactly). An unknown entry carries a timestamp bound, so the next publish of a newer
  row settles it, and a per-entry generation, so a fill (a one-granule read of the parts) stores its answer only if
  nothing changed that series since the fill's snapshot. Fills go through a per-volume queue, so no reader ever
  waits for the write lock.
- **Cost on the insert path**: a compare and sometimes a 4-byte store per memtable row, on a cache line the insert
  already writes; per published part, one compare and store per series it holds, under the write lock. Lookups
  hold `latest_rw` for reading in holds of at most 256 ids (about 25 to 50 µs, no I/O: every read of a granule runs
  after the hold) and the small-insert lock (`buf_lock`) in sub-holds of 128 ids, about 2 µs each.
- **Memory**: 16 bytes per series per copy, within a byte budget (`latest_cache`, default RAM/16); series beyond the
  budget are answered exactly by the slow path. 16 MB for a million series, 1.6 GB for 10^8, 6 GB per server for
  10^9 in a cluster of three copies on 8 servers.
- **Rebuilt at open** from the granule index (each granule's last value and timestamp, already stored), inside a
  loop the open already runs, which this design splits by series range over the open's threads, with no transient
  memory beyond the table. What the index cannot give exactly (state
  series, int64 beyond 2^53, NaN payloads) starts unknown with its timestamp known; the next flush of the series
  settles it, and a warmer at the scheduler's full width fills the rest within seconds. No format change.
- **As of a time, skipping nulls**: the same op with an end bound or a flag; those go the slow way (one granule per
  source, newest partitions first), not through the table, unless the bound is above the cached row.
- **Rights**: `read` on each series; denied and missing look alike, as everywhere; a missing id is `NOTFOUND`.

## Assumptions (decisions the user may change)

| # | Assumption | What changes if it does not hold |
|---|---|---|
| A1 | **Latest = the row with the greatest timestamp** among the series' live rows; at that timestamp, the value last-write-wins keeps. Not the most recently *written* row. | "Most recently written" needs a write sequence per row in parts, which tsdb does not keep, and would disagree with every query. It would be a different feature (closer to the change feed of design-cdc.md). |
| A2 | **A null at the greatest timestamp is returned as a null** (with its timestamp). A flag `SKIP_NULLS` asks for the newest non-null row instead (§5.2). | Returning the newest non-null by default would hide that a sensor reports "no value" now; the cost is the same either way (the flag is the slow path). Open question 1. |
| A3 | **Sharded by volume, inside the engine.** Each volume keeps its series' newest points; in the cluster the volume's leader answers and its replicas keep the table warm. No cache service of its own. | A separate cache tier is rejected in L2 and §7.3; it cannot be exact without being on the write path anyway. |
| A4 | **Exact and fresh.** A lookup reflects every acknowledged write and delete, and never a write that queries do not show yet, by the same snapshot rules as a query. | A "fast but maybe stale" mode would need no lock at all, but every client would have to guess how stale. Not offered. |
| A5 | **One instant per series, not per request**: each series' answer is the state at one instant during the request; two series of one request may be read at different instants (a request for 100,000 series may see an insert in some series and not yet in others). | One instant per volume per request needs a lock held across the whole request, which would hold up every publish. Open question 4. QUERY_MULTI with `VALUES`, descending, limit 1 keeps its one cut per volume for those who need it. |
| A6 | **On by default**, as a byte budget: `tsdb_options.latest_cache` (default RAM/16 for the process, shared by its volumes; tsdbd `--latest-cache BYTES`, `0` off). Series beyond the budget, and every series when it is 0, are answered exactly through the slow path. | Off by default saves 16 bytes per series for users who never ask. Open question 2. |

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| L1 | **Latest is defined by the existing query**: `tsdb_query_values(series, cutoff, end, DESC, limit 1)` is the reference answer, bit for bit (value bits included, NaN payloads too). | A definition of its own | One meaning in the whole database; the tests have an oracle that is already tested; deletes, retention, LWW and memtable visibility come for free. |
| L2 | **The cache is part of each volume's engine** (`src/latest.c`), sharded by volume like everything else. | A cache service in front (Redis or a tsdb tier); a cache per tsdbd keyed by name | A separate copy must be kept in step with LWW, deletes, replaces, retention and failover; that is the engine's job and it already does it. A cache beside the engine is either on the write path (then it is this design, one hop further) or stale. §7.3 compares the two in full. |
| L3 | **Two layers: a table for the parts, the memtables for newer rows.** | One table updated by every row | Rows in the memtable become visible only when synced (sync mode) or committed (cluster), not when appended; a single table would need a WAL position per entry and an update pass after every group commit, on the commit's critical path. The memtables already hold those rows in series order with their positions. |
| L4 | **Table entry: 16 bytes plus one bit**: `u64 t` = the timestamp XOR `INT64_MAX`, `u64 x`, and an *unknown* bit in a per-page bitmap. All-zero memory is **empty** (`t = 0` is `TSDB_TS_MAX`, which is never a row's timestamp). **Known**: bit clear, a row's timestamp, `x` = its value slot. **Unknown**: bit set, the timestamp field holds a **bound `u`** (every live row of the series in parts has `ts <= u`; `TSDB_TS_MAX`: no bound) and `x` holds the **touch generation** (§2.1). | `ts = TSDB_TS_MAX` with slot codes for the states; 24 bytes with the block number; a type tag per entry; a table of doubles (`GF_LAST`) | Zeroed pages must mean "empty", not a row at 1970 with value 0 (timestamp 0 and negative ones are valid, tsdb.h:80-82). The bound lets a later publish settle an unknown entry without a read; the touch generation makes fills safe per series. The type is per series (SERIES). Doubles are inexact for int64 above 2^53 and absent for state series. |
| L5 | **The table is paged**: 65,536 entries (1 MiB, plus an 8 KiB bitmap) per page, a page table sized for 2^31 local ids (256 KiB), pages made in id order and never moved or given back, within the byte budget (A6), which volumes share first come, first served. The first page the budget refuses caps the volume: ids from there on have no entry and are answered by the slow path. When budget is freed (a volume closes, a replica moves away), the cap lifts, and every page the volume makes from then on starts with all its entries unknown with no bound (the warmer fills them, §3.8). | One array, reallocated; eviction of cold pages | Growing a 1.6 GB array copies it under the lock. Pages made in id order by the first fold that needs them start empty correctly (no earlier listing held a row of their ids, since that listing would have made them); after a cap that no longer holds, so they start unknown. Eviction would need a policy; a cap is exact and simple. |
| L6 | **Memtable layer: `top`**, a u32 in `mt_series` (the index of the row with the greatest timestamp, the later one on ties), set by `mt_add`. It fits in the struct's padding (40 bytes stay 40). | A copy of the newest row in each `mt_series` (64 bytes) | Same answer with one more cache miss per lookup; 24 more bytes per series slot in every memtable, which is dense by local id. |
| L7 | **Entries are read and written only under `latest_rw`** (a per-volume writer-preferring rwlock). Writers, each in a write section: publishers (between `parts_commit` returning 0 and the end of their listing, which happens in the same section), the drain of the fill queue, a retention limit lengthened, `drop_before` and its page sweep, every merge's listing (which marks entries only after a retention change), a follower's refresh. Lookups hold it for reading in holds of at most 256 ids, with no I/O in them. Publishers compute outside the lock only what does not depend on the table (each series' newest row of their parts); compare and store happen inside. | Computing the fold before the write lock; changing entries after the listing; a seqlock | The first review found that a fold computed outside the lock races fills (a stale row stored for good). After the listing, a lookup could see a query's answer missing for a moment; before the commit point, a write that then fails. A seqlock retries forever under steady large inserts; a writer-preferring rwlock bounds both waits. §3.2 has a measured exception for flushes. |
| L8 | **Merges, class switches and the retention clock never change the table**, with one exception: a merge during which a retention limit changed (§3.5). Lookups apply the cutoff (`eng_cutoff`) at read time, as queries do. | Updating it on every merge | A merge keeps every live row; it drops rows below the cutoff, which lookups clamp, and rows tombstones hid, which the table never holds as live. |
| L9 | **Publishes fold by rules** (§3.3, §3.4): a new newest row replaces an older known one or settles an unknown one whose bound it reaches; a tombstone over the known newest row makes the entry unknown with the bound just below the tombstone. | Recomputing every touched series at once under the lock | The rules are exact in every case they decide; only "the newest row was deleted and nothing replaced it" needs a read, and that read must not run under the lock. |
| L10 | **Unknown entries are filled by a read** of the parts only (§3.8), with no cutoff and no end bound, from a cut taken with the read lock held and searched after it is released; the fill stores only if the entry is still unknown, its touch generation is not newer than the cut, and no drop (and on a follower no refresh that removed parts) came after the cut. Fills are queued and stored by a drain in a write section that a writer holds anyway, or by the merge thread. Filled by the lookup that met it, by a delete for its own series, and by a warmer after open. | Filling under the lock; one generation per volume; filling from the index alone; each reader storing its own fills | A read under the lock stalls publishes for the time of a decode. A per-volume generation drops every fill on a busy volume (the reviews' liveness finding). A granule touched by a tombstone must be decoded (snapshot windows), so the index alone cannot answer. A write section per chunk convoys lookups on the writer-preferring lock and parks scheduler helpers in `wrlock` (the verification's P3). |
| L11 | **Rebuilt at open**, inside `cls_check_parts` (classes.c:1095), which already walks every run of every part in (partition, block) order. The walk is split into blocks of 32,768 series ids that the open's threads take in turn; for its block a thread visits every part's runs of those ids, so the block's 512 KiB of table stays in its cache across all partitions. The table itself holds the walk's state (timestamp, part and granule of the newest run so far), tombstones are applied as the walk meets them, and one pass at the end of each block reads `GF_LAST` and the null flag once per series. | A LATEST file written at close; a lazy build on first use; a transient array per series; the serial walk | A file is one more durable thing with fail-stop and torn-tail rules, and is wrong after a crash anyway. A lazy build delays the first answers by minutes on a big volume. Reading `GF_LAST` per run would cost series x partitions field reads (the performance review); per series it is one. A serial walk that touches a table line per run moves terabytes at 10^8 series (the verification's P5). |
| L12 | **What the index cannot give exactly starts unknown with its timestamp as the bound**: state series (no time-weighted statistics, twa.c:126-131), int64 whose last value as a double is at or beyond 2^53 in magnitude (twa.c:70-73), NaN last values (the payload). The next flush or insert of the series settles it (its row is at or past the bound); a warmer fills the rest at the scheduler's full width right after the open. | Decoding their last granule during the open | The warmer does the same decoding in parallel and does not delay the open; series that are still written are settled by their next flush without any read. `GF_LASTV` in the first format bump removes the case (L18). |
| L13 | **NFS followers keep the block number too** (24-byte entries), fold each refresh's new parts by `(ts, block)` with a shadow check against tombstones of higher blocks, re-derive every entry whose row lay in a part that left the list, and refuse fills whose cut predates a refresh that removed parts (`rm_gen`, §3.10). The refresh computes its changes outside the lock and stores only the entries that change; it is the follower's only writer of the table. | Rebuilding at every refresh; folding only the new parts; re-touching every unknown entry of a leaving part | A follower sees merge outputs as new parts and can miss a tombstone that was written and merged away between two refreshes (the correctness review); only the parts that leave say which entries may have lost their row, and an unknown entry's fill may have read such a row (the verification's V2). |
| L14 | **One op, `QUERY_LATEST`**, protocol 13: ids (1 to 1,048,576), an optional end bound (one, or one per id), a `SKIP_NULLS` flag; replies of fixed 24-byte records in index order. The floor stays 12. | Names in the request; a QUERY_MULTI kind | Clients already resolve and cache ids (insert paths need them). A QUERY_MULTI kind would carry a template and a frame per series: 30 bytes of framing for 24 of answer. |
| L15 | **Rights: `read` on each series**, checked per id in the op's case as QUERY_MULTI does; a denied id answers `TSDB_ERR_DENIED` in its own record, whether or not it exists; a missing id is `TSDB_ERR_NOTFOUND` for a user with `read` on `all` (and with `--no-auth`). | Refusing the whole request; "none" for a missing id | The same rule as QUERY_MULTI and as lookups by id elsewhere (AGENTS.md, "Series ids"; design-auth A16). |
| L16 | **The cluster changes nothing in the table**: replicas apply `COMMIT` markers through the same listing path and see memtable rows up to the commit point, so their tables match the leader's at every commit point. Clients split by leader as QUERY_MULTI does (cluster C16); `REPLICA_OK` and `min_pos` as for queries. | A cluster-wide latest service | It would be a second replicated state to fence at failover. Here a new leader's table is already warm. |
| L17 | **The slow path gets a real "limit 1"** (`latest_at`, §5.1): partitions newest first, a part's snapshot window computed only when it is visited, one granule per source, stopping as soon as no older partition can hold a newer row. | Using raw_desc as it is | raw_desc decodes about 8 granules to return one row and enumerates every granule of the series in every part (query.c:1930-1953): O(parts) per series. |
| L18 | **No format change.** The index already stores what the build needs (format 8). An exact last slot in the index (`GF_LASTV`, state codes included) goes into the first format bump that happens for any reason. | A format bump now | A format change costs every user an export and import (compatibility.md); with the warmer at full width and settling by the next flush, a few unknown entries after an open do not justify it. Open question 5. |
| L19 | **Lookups touch the volume's locks in short holds**: `latest_rw` for reading in holds of at most 256 ids, never across a read of a granule; `db->lock` only to reference the memtables and read `visible` (and, when an id needs the slow path, to take a cut: O(parts) references, no I/O); `buf_lock` in sub-holds of at most 128 ids, with the lines they need prefetched. | Holding `db->lock` and `buf_lock` across a 4096-id chunk; running the slow path inside the read hold | The performance review: those are the volume's query and small-insert mutexes; long holds stall inserts and make lookups of one volume run one at a time. The verification (V7, P1): a slow path inside the read hold holds `latest_rw` for tens of milliseconds, while a flush waits for it holding `publish_lock` and `cls_rw`, and the flusher is also the periodic WAL syncer. |

## 1. What "latest" means

For a series s, a lookup with end bound `end` (default `TSDB_TS_MAX`, "no bound") answers:

- the row with the greatest timestamp `t` such that `cutoff(s) <= t < end` among s's live rows (rows a tombstone
  hides and expired rows are not live; memtable rows count once they are visible, the same rule as queries);
- its value, as the last write at `t` left it (last-write-wins, so a later null at `t` makes it a null);
- a kind: **value**, **null** (the row is a null), or **none** (no live row before `end`).

An id that names no series is not "none": its record has status `TSDB_ERR_NOTFOUND` (or `TSDB_ERR_DENIED`, §6.3).

With `SKIP_NULLS` the row is the greatest `t` whose last-write-wins value is not null (A2). The answer equals
`tsdb_query_values(s, cutoff, end, {DESC, 1})` (L1) with a null mapped to kind null; the tests check exactly this.

Two consequences worth saying:
- A stray row far in the future (a device with a wrong clock) is the latest until it is deleted. A client that
  wants "the newest point that is not in the future" sends `end = now + 1`; that is the slow path (§5.1).
- A series that has never been written, and one whose every row expired or was deleted, both answer **none**.

## 2. The two layers

### 2.1 The latest table (parts)

Per volume (engine), indexed by local id, in pages of 65,536 entries (L4, L5). States and what they promise:

| state | encoding | promise |
|---|---|---|
| empty | all zero | no row of the series in the parts |
| known `(ts, slot)` | bit clear, `t = ts ^ INT64_MAX`, `x = slot` | the newest live row in the parts is this one (the cutoff aside: lookups clamp) |
| unknown `(u, touch)` | bit set, `t = u ^ INT64_MAX`, `x = touch` | every live row in the parts has `ts <= u`; nothing more is known |

A page made when ids first need it is zeroed: empty, which is right because no listing before held a row of those
ids (L5). A page made by the build at open follows the same order.

`latest_rw` (a `pthread_rwlock_t`, writer-preferring: `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`) guards the
entries, the bitmaps, the page table, `lgen`, the drop mark (§3.6) and a follower's `rm_gen` (§3.10). **`lgen`**
counts the changes that can make a fill wrong: every publish, retention change, drop, merge listed across a
retention change and follower refresh bumps it in its write section (fills and plain merge listings do not). A
writer that leaves or makes an entry unknown sets its **touch** to the new `lgen`; a fill whose cut was taken at
`lgen = g` (with the read lock held, so the cut holds every change up to `g`) may store only into an entry that is
still unknown with `touch <= g` (§3.8). So a fill is refused only when *its own* series changed, which keeps fills
converging on a volume that publishes every few milliseconds. Two rare events remove rows without saying which
series lost them: a drop, and on a follower a refresh that removed parts. They refuse every fill whose cut is older
(`g` below the last drop's `mark_gen`, or below `rm_gen`).

### 2.2 The memtable layer

The active memtable, the flushing one (at most one: a memtable joins `flushing` only when none is there,
memtable.c:843; the array has room for `MAX_FLUSHING` = 4, internal.h:1192) and a follower's WAL tails hold every
row that is newer than the parts, indexed by local id (`mt_series`, internal.h:1120). `mt_add`
(memtable.c:137) gains one line per row: `if (!s->n || ts[i] >= s->ts[s->top]) s->top = s->n;` before the append.
The `mt_series` line is already in cache there (it just wrote `n` and checked `sorted`).

A memtable's newest *visible* row of s: if `lsn[top] <= visible` (always, outside sync mode and the cluster), it
is row `top`. Otherwise the newest row is in flight (its group commit has not synced yet). Rows are appended in
`lsn` order, so the visible prefix is found by binary search on `lsn`; for a sorted series its last row is the
answer. For an unsorted series the lookup does not scan under the lock: it notes `lsn[top]`, sets the id aside,
and after the chunk waits until `wal_durable` passes it (one group commit, which the row's own writer is waiting
for; on a failed engine it stops waiting) and looks the id up again. Should `top` be in flight again (a series
written out of order without pause), it scans the visible prefix in pieces of at most 4,096 rows, taking
`buf_lock` again for each (rows are only appended, so the prefix does not change). A metric counts both
(`tsdb_latest_inflight_total{how="wait"|"scan"}`). On equal timestamps within one memtable the later row wins, as
`keep_last` does in queries.

### 2.3 A lookup

For the ids of one volume, in chunks of up to 4096. A chunk is put in local-id order (each id keeps its index for the
reply): one O(n) pass checks whether it already is (dashboards usually send ids in order), else a two-pass radix
sort on the 32-bit local id, about 10 ns per id. Each chunk is read in **holds of at most 256 ids**:

1. Take `latest_rw` for reading. Read `lgen` as `g`.
2. Take `db->lock`: reference the active and flushing memtables (and a follower's tails), read `visible` (in sync
   mode `wal_durable`, in the cluster the commit point, else no limit). Release it. That is O(1). It is consistent
   with the table: no publish can fold or list while this hold lasts.
3. Read the table entries, each id's cutoff (`eng_cutoff(s)`, a relaxed atomic load with no lock,
   retention.c:211-223) and the flushing memtables (immutable, no lock). The cutoff is read here, inside the hold,
   because a lengthened limit and the marking of the entries it affects happen in one write section (§3.5); read
   after the hold, a new cutoff could meet an old entry that names a row merged away long ago.
4. Read the active memtable and the tails in sub-holds of `buf_lock` of at most 128 ids. Before each sub-hold,
   prefetch the ids' `mt_series` lines through the array pointer read with a relaxed atomic load (`mt_reserve`
   stores it with a release store when it reallocates, memtable.c:94-130; a prefetch of memory just freed is
   harmless, and the atomic keeps the read of the pointer itself out of a data race). Inside the sub-hold, read
   `top` and `lsn[top]` for the 128 ids and prefetch their row lines, then read the rows: two passes, so the misses
   overlap. About 15 ns per id under the lock (U1).
5. For each id the candidates are, newest source first: the active memtable, the flushing memtable, on a follower
   the WAL tails (newest segment first), then the table entry. The answer is the candidate with the greatest
   timestamp; on equal timestamps the newer source wins (memtables rank above parts, as in queries;
   query.c:240-277). An empty entry has no candidate. A newest row below the id's cutoff means no live row (every
   other row is older still). Some ids cannot be answered this way:
   - **An unknown entry `(u, touch)`**: if a memtable candidate has `ts >= u`, it is the answer (every row in the
     parts is at or below `u`, and memtables win ties); no read is needed, and the next flush settles the entry.
     Otherwise the id needs a **fill search**.
   - **An end bound below the found row, `SKIP_NULLS` with a null found, an id beyond the budget's cap**: the id
     needs a **bounded search**.
   The first time an id of the chunk needs a search, the hold takes the chunk's **cut** (`cut_take`, query.c:93:
   references to the parts, the memtables and `visible`, O(parts) under `db->lock`, no I/O) and keeps `g` as the
   cut's generation. Later holds of the chunk reuse it.
6. Release `latest_rw`.
7. Outside every lock, run the searches on the cut (§5.1). A search uses only the cut, its memtables with the cut's
   `visible` and its parts, never the hold's reads, so its answer is the state at one instant (the cut's), as a
   query's is.
   - A fill search computes the newest row of s in the cut's parts with no end bound, no `SKIP_NULLS` and no
     cutoff, to its own stop rule (memtable rows are not candidates in it). That result is the fill: it goes to the
     volume's fill queue with `g` (§3.8). The answer is the newer of it and the cut's memtable rows of s, then
     clamped to the cutoff.
   - A bounded search runs `latest_at` with the bound and flags over the cut's memtables and parts. It gives no fill.
   Ids set aside in step 4 (§2.2) are looked up again.
8. Send the chunk's records; release the cut and the memtable references.

With `latest_cache = 0` there is no table: a chunk takes no `latest_rw` and only its cut. A request whose every id
needs a search (`SKIP_NULLS` over null series, as of a day back) holds `latest_rw` only for the table reads and the
cut, never for its reads of granules.

A chunk's ids are thus answered at the instants of its holds, and its searched ids at the instant of its cut, which
lies in the first hold that needed it. Each instant falls within the request (A4, A5); a search never mixes the
hold's memtable reads with the cut's parts, so a flush between the two cannot lose a row.

Cost, estimated (U1): an id costs a table read (one miss at large sizes, fewer when ids are dense), about two
overlapped misses per memtable that holds the series, and its share of the sort. About 0.1 to 0.2 µs per id on one
thread; 100,000 ids in 10 to 20 ms on one thread, split over the request's threads (the query scheduler,
`request_threads`) by chunk. Lookups of one volume run in parallel (the read lock is shared). A hold of 256 ids lasts
about 25 to 50 µs, which bounds how long a publish waits to enter its write section. What lookups share with inserts
is `buf_lock`, about 15 ns per id, so 32 connections at 20 M ids/s keep it about 30% busy in holds of 2 µs.

**The insert gate** (step 2, §10). Inserts under 32 connections of 100,000-id lookups are compared with the same
inserts under **no lookups at all** (not with the table off: then every lookup is the slow path, itself a load on
`buf_lock` and the cut). The same inserts under a QUERY_MULTI load (values, descending, limit 1) at the same ids per
second are reported beside them, as what such a dashboard costs inserts today. Pass:
- **sync mode** (`wal_sync_ms = 0`): throughput and p99 within 5%;
- **periodic mode**: throughput within 5%, p99 within 5 µs absolute (a single-row append's p99 is below 1 µs, so a
  ratio would only measure the futex wake of one contended sub-hold);
- both on a **hot set** (1,000 series, every one looked up) and a **spread set** (10^6 series);
- publish latency of a flush and of a one-row part (p50 1.56 ms today) while 32 connections send 100,000-id
  `SKIP_NULLS` and as-of requests: p50 within 5%, p99 within 0.1 ms (one read hold).

If it fails, by cause:
- **Waits on `buf_lock`**: a paged `mt_series` array that never moves, and row storage freed only with its memtable,
  so lookups read the active memtable without `buf_lock`; or sub-holds of 32 ids.
- **Coherence misses on the hot set**: `mt_add` writes lines that lookups just read: the `mt_series` line, and
  `ts`, `v` and `lsn` at row `n`, which share a line with row `top = n - 1` seven times in eight, up to four
  read-for-ownership misses per row. Removing the mutex does not remove them. Each memtable then keeps one compact
  `(ts, slot, lsn)` top record per series, written by `mt_add` with the row, so a lookup reads one line and an
  append invalidates at most one line in a reader's cache. Reading a series while it is written cannot cost less.

### 2.4 Memory per series

| what | bytes | status |
|---|---|---|
| latest table, writer and cluster replica | 16 per series per copy, plus 1/8 (bitmap); 256 KiB page table per volume | this design, within `latest_cache` |
| latest table, NFS follower | 24 per series, plus 1/8 | this design |
| open build | nothing beyond the table (L11) | this design |
| a request's slow path | one cut per chunk that needs it (a pointer and a reference per part) | this design |
| a publish's fold list | 24 per distinct series of the publish, while it publishes (24 MB for a flush of 10^6 series; volumes that flush at once add up) | this design |
| the fill queue | at most 65,536 entries of 32 bytes per volume (2 MiB) | this design |
| a follower's refresh | about 32 per entry the refresh changes, until its write section (32 MB for a refresh that brings a flush of 10^6 series); its notes are streamed per partition, never held for the whole refresh (§3.10) | this design |
| memtable `mt_series` | 40 per local id up to the highest id written, per memtable (the active one and at most one flushing, §2.2), arrays doubling (memtable.c:94) | today; `top` fits in its padding |
| rights maps | 1 per id per logged-in user with grants, at most 64 MiB per user and RAM/16 in all (auth.c:27, 47) | today |

| series in the database | table, one copy | three copies | per server, 8 servers |
|---:|---:|---:|---:|
| 10^6 | 16 MB | 48 MB | 6 MB |
| 10^8 | 1.6 GB | 4.8 GB | 600 MB |
| 10^9 | 16 GB | 48 GB | 6 GB (2.3% of 256 GB) |

The table is not what dominates: at 10^9 series with three copies on 8 servers, each server holds 3.75 x 10^8
series copies, and memtables whose series are all written within a memtable's life take 40 bytes each, 15 GB for
the active memtables and up to 30 GB with one flushing beside each. design-cluster.md §10.2 counts memory with this
line. The default budget (RAM/16, 16 GB on a 256 GB server) holds a table of 10^9 entries.

## 3. Keeping it exact under every write

The rules below share one fold. When a publish brings `R`, the newest row of series s among its rows (for a part,
the series' last row; for several parts of one commit, folded in block order so that the later part wins equal
timestamps, insert.c:611), the entry becomes:

| entry before | after |
|---|---|
| empty | `R` |
| known `k` | `R` if `R.ts >= k.ts` (the publish has a higher block than every listed part, so it wins ties), else `k` |
| unknown `(u, touch)` | `R` if `R.ts >= u` (every live row is at or below `u`, and `R` wins a tie), else unknown `(u, lgen')` |

`lgen'` is the generation this write section sets. On the writer every publish has higher blocks than every listed
part (publish_lock, `parts_publish`), which is what makes the ties come out right without blocks in the entries.

### 3.1 Small inserts (the WAL and memtable path)

Nothing but `top` (§2.2). A row appended to the active memtable is found by a lookup exactly when `mt_collect`
would show it to a query: the same `visible` bound. **Sync mode** (`wal_sync_ms = 0`): a row is returned only once
the WAL is synced up to it. **Periodic sync**: visible at once, as for queries. **Cluster**: visible at the commit
point (cluster C7).

A small write waiting on a large write's fence (`lw_fence`, internal.h:1490-1514) has appended nothing, so no
lookup can see it before the large write is listed: the fence order holds for lookups as it holds for queries.

### 3.2 Flushes

`flush_waiting` (memtable.c:777) commits a memtable's parts, then lists them and removes the memtable from
`flushing` in one `db->lock` section (memtable.c:801-810).

- Before taking the lock, `mt_write`, which already sorts each series and keeps the last write per timestamp,
  hands the publish a list of each series' newest row `(id, ts, slot)`, in id order. That list does not depend on
  the table.
- After `parts_commit` returned 0, the publisher takes `latest_rw` for writing, folds the list (the table of §3),
  bumps `lgen`, lists the parts and removes the memtable from `flushing`, and releases it.

A lookup sees either the memtable in `flushing` (and the old entry) or the listed parts and the new entry, never a
mix.

**Hold time, and the window for large flushes.** A flush holds `buffer_rows` rows (1 M by default, db.c:48); at
10^8 series written once a second that is about a million distinct series, stored sparsely in the table: about a
million cache misses, 5 to 15 ms of write hold about once a second per volume (U2), during which that volume's
lookups wait (queries do not). That estimate is above 2 ms, so step 2 builds the following window from the start
(not as a contingency), used by flushes (and only flushes) whose fold list is longer than a threshold (about
50,000 series, set from the measurement of lookup p99 under steady flushes of 10^6 series):

1. Under the write lock, set the volume's `publish_pending`, reserve `lgen' = lgen + 1` (no other writer can bump
   `lgen` until the window closes), and release it.
2. Compare and store each entry with relaxed 8-byte atomic loads and stores (lookups then read entries with relaxed
   atomic loads too: free on x86). An entry that stays unknown (`R.ts < u`) gets touch `lgen'`, the reserved value:
   a fill whose cut was taken in the window has `g = lgen' - 1`, lacks the flush's parts, and must be refused.
3. Under the write lock: list, remove the memtable from `flushing`, set `lgen = lgen'`, clear `publish_pending`.

This is exact because every stored row is still in the flushing memtable, which every lookup in that window reads,
and which ranks above the table on equal timestamps. A torn read (new timestamp with the old slot, or the old
timestamp with the new one, or the bit and the fields out of step) always shows a timestamp at or below the
memtable's row for that series, so the memtable's row wins; and a flush never turns an entry unknown. While
`publish_pending` is set, the other writers that bump `lgen` or change entries (retention, drops, a merge listing
that must mark, §3.5) release the write lock and wait on a condition variable until it clears; none of them runs on
a scheduler helper. A plain merge listing changes no entry and goes ahead. A flush in its window that meets a page
the drop mark asks to sweep (§3.6) sweeps it with the same atomic stores and sets the page's `swept` last: a half
swept page reads the same as an unswept one. A drain of the fill queue that finds it set leaves the queue for the
next drain and waits for nothing. Large inserts, deletes and replaces keep their stores inside the write section,
since no memtable holds their rows.

### 3.3 Large inserts and the WAL replay

`insert_rows` (insert.c:640) writes parts, then `parts_publish` (insert.c:626) commits and lists them under
`publish_lock`. The part writer notes each series' last row as it closes the series' run (one compare per series
boundary). A large unsorted insert is written in chunks whose later part wins duplicates (insert.c:611), so the
parts of one commit are folded in block order: the later part's row wins an equal timestamp. The fold runs at the
same place as a flush's (after `parts_commit`, in the write section that ends after `db_add_parts`, db.c:149). The
replay at open publishes through `parts_publish` too (memtable.c:1209) and folds the same way.

The fence is untouched: the large write's parts are listed and folded in one write section, then `lw_leave` lets
the small writes behind it in. A small write that waited therefore lands in a memtable after the fold; a lookup
ranks it above the large write's row at an equal timestamp, which is last-write-wins in real time (jepsen check 5).

### 3.4 Deletes and replaces

delete.c:150-170 commits tombstone parts (and, for a replace, the rows' parts) and lists them under `publish_lock`.
In the write section of that listing, per series s of the commit, with `D` its tombstone ranges and `R` its newest
row among the commit's rows (none for a delete):

| entry before | rule | why |
|---|---|---|
| empty | the fold with `R`, if any | nothing older to hide |
| known `k`, `k.ts` in no range of `D` | the fold with `R` | the tombstones did not touch the newest row |
| known `k`, `k.ts` in `[a, b)` of `D` | unknown `(a - 1, lgen')`, then the fold with `R`; if `a = INT64_MIN`, empty, then the fold | every row at or above `a` in a lower block is hidden, and `k` was the newest, so every live row is below `a` |
| unknown `(u, touch)` | the fold with `R` (touch set to `lgen'` if it stays unknown) | tombstones only remove rows, so the bound still holds |

Tombstones hide only rows of lower blocks (design.md, "Tombstones live in parts"), and every listed part has a
lower block than the commit, so the rule never asks about blocks. A tombstone in another partition key than the
row's does not hide it; the rule treats it as hiding (unknown), which is safe and costs a fill. Memtable rows are
never hidden; a delete flushes the memtable before it writes its tombstones, so any row a lookup still finds in a
memtable is newer than the delete.

After its publish, and after `lw_leave`, `cls_thaw` and `publish_lock` are released (delete.c:172-174), a delete
runs a fill search for the entry it made unknown and queues the result (§3.8): a delete names one series, so that is
one read on the caller's thread with no lock held, and small writes into the range do not wait for it.

### 3.5 Retention

Lookups clamp to `eng_cutoff(s)` at read time (L8), as `eng_query_raw_ex` does (query.c:2024). An entry holds the
newest row in parts whatever its age (fills and the build search with no cutoff, so **empty means "no row in
parts"**, not "no live row"). If the entry's row is below the cutoff, every row of the series is, and the answer is
none.

The cutoff only rises (`eng_now_ms` never goes back), except when `tsdb_set_retention` gives a series a longer
limit (it is per series, tsdb.h:280). Rows below the old cutoff may already be gone (merged away) while the entry
names one, so:

- `tsdb_set_retention` writes and syncs its RETENTION record under `ret_lock` as today, then takes `latest_rw` for
  writing and, in one section, applies the limit in memory (`ret_put_locked`, retention.c:43, which bumps
  `ret_gen`) and, if the cutoff went down, turns a known entry with `ts` below the old cutoff into unknown
  `(ts, lgen')` (an unknown one gets touch `lgen'`). No lookup can see the new limit with the old entry. One series:
  O(1). Lock order: `ret_lock` > `latest_rw`.
- Merges read the limit as they go (merge.c:241, 271, 334). A merge that started before a change can drop rows the
  new limit keeps, while a fill with a later cut (which still lists the merge's sources) finds one. So a merge
  records `ret_gen` when it starts, and **every merge listing takes `latest_rw` for writing before `db->lock`**: the
  merge thread already releases `db->lock` around `merge_parts` (merge.c:626-633), and so does `eng_optimize`
  (merge.c:711-715); both now take `latest_rw`, then `db->lock`, then call `merge_commit_locked`. Taking
  `latest_rw` inside `merge_commit_locked`, which runs with `db->lock` held (merge.c:453), would invert the lock
  order. In that write section the merge drains the fill queue (§3.8) and compares its `ret_gen` with the current
  one; `tsdb_set_retention` bumps `ret_gen` inside its own write section, so the compare cannot race it.
  - Equal (the common case): it lists, and changes no entry and not `lgen`.
  - Different: it bumps `lgen` and, for every series with a run in its sources, makes every entry whose rows the
    merge may have dropped refuse older fills: a known entry with `ts` in the merge's time range becomes unknown
    `(ts, lgen')`, and **an unknown entry gets touch `lgen'`** (its fill may have read a row the merge dropped). A
    known entry above the range has no row in the merge. Rare (a retention change during a merge), O(series of the
    merge).
  Merges take no `publish_lock` and need none (the order allows `latest_rw` without it; making merges wait for a
  flush's fsync would be new contention). A merge waits at most one read hold (256 ids) to list. The merge thread
  is not a scheduler helper.

The merge thread's partition drops remove only expired rows (selection and retirement in one `db->lock` hold,
merge.c:533-579, and a cut needs `db->lock`, so no fill straddles them): no change.

### 3.6 `tsdb_drop_before`, class switches, merges

- **`tsdb_drop_before(t)`** retires every partition that ends at or before `t` (db.c:979-992, today under
  `db->lock` only). It takes `latest_rw` for writing, then `db->lock`, and in that one section retires the parts,
  bumps `lgen` and sets the volume's **drop mark**: `bound = max(bound, t)`, `epoch + 1`, `mark_gen = lgen`. O(1)
  besides the retiring. No 1.6 GB scan under the lock (the performance review's L1). Lock order: `latest_rw` >
  `db->lock`, as for lookups. Three rules make the mark exact:
  1. **Reading.** A known entry with `ts < bound` on a page whose `swept` epoch is older than `epoch` reads as
     unknown `(ts, mark_gen)` to lookups. Its row may have been dropped, and every live row is at or below `ts`.
     Entries with `ts >= bound` are untouched: their partition ends after `t`.
  2. **Writers sweep first.** Any writer about to change an entry on such a page (a publisher's fold, the drain of
     a fill, a retention or merge marking) first sweeps the whole page in the same write section: every known
     entry with `ts < bound` becomes a real unknown `(ts, mark_gen)`, and the page's `swept` becomes `epoch`
     (1 MiB, about 50 µs, at most once per page per drop). So a known entry on an unswept page has not been written
     since the drop, and rule 1 is right for it. Without this, a large insert at `ts` 500 after `drop_before(1000)`
     would store known 500, which rule 1 would then read as unknown `(500, mark_gen)`, and a fill whose cut was
     taken before that insert would store its empty answer over it (the verification's V1).
  3. **Older fills are refused.** A fill whose cut was taken before the drop (`g < mark_gen`) is never stored.
     The drop names no series, so an entry that was unknown already, with a touch older than its fill, would
     otherwise take a row from a partition the drop retired. Drops are rare; fills taken after converge.
  `drop_before` then sweeps the remaining pages itself before it returns, one page per write section; the warmer
  (§3.8) fills what the drop left unknown.
- Class switches move no row (AGENTS.md, "Partition classes"): no change. One (series, ts) is never in two keys,
  so ties between parts of different keys cannot happen.
- Merges (merge.c:463) list their output and retire the sources with the same live rows: no change (L8), apart
  from §3.5's retention case.

Considered in review: the performance review asked for the same mark "for a retention change that applies to
every series". Retention limits are per series (`tsdb_set_retention(db, series, limit)`, tsdb.h:280), so a change
marks one entry; a client that lengthens the limit of a million series sends a million calls, each O(1) under the
lock.

### 3.7 Fail-stop

The table changes only after `parts_commit` succeeded (the commit point) and only in the section of a listing, so a
publish that fails (and the engine with it) leaves the table as the parts list: as queries see it. After
`eng_fail` no new file is named and nothing is listed, so publishes stop changing it; memtable rows stay invisible
above `wal_durable` in sync mode (a lookup waiting for an in-flight row stops waiting, §2.2). Lookups answer what
queries answer on a failed engine, until tsdbd exits.

### 3.8 Unknown entries and the fill

A fill is the newest row of s **in the parts only**, with **no cutoff** (§3.5), **no end bound and no
`SKIP_NULLS`**: a fill search (`latest_at`, §5.1, in its parts-only form) over the parts of a cut, run to its own
stop rule, with `g` the `lgen` read in the same read hold in which the cut was taken. Memtable rows are never
candidates in it: §5.1's early stop counts them, and a memtable row newer than every part would stop the search
before it visits a partition, so the result would be empty or an older row, stored for good. A search with an end
bound or `SKIP_NULLS` computes something else and never gives a fill. On a follower the result carries the block of
its row (the `max_block` of the part whose granule, or mem run for a granule a tombstone touched, held it).

**The fill queue.** The result `(id, row or empty, g)` goes into the volume's fill queue: an array under a leaf
mutex, at most 65,536 entries. A full queue drops the fill (`tsdb_latest_fills_total{result="dropped"}`): fills are
an optimisation, and the slow path stays exact without them. No thread that computes a fill waits for the write
lock.

**The drain**, in a write section that is not inside a §3.2 window, takes up to 4,096 queued fills and stores each
one (sweeping its page first if the drop mark asks, §3.6) only if:
1. the entry still reads unknown and its touch is at or below `g` (for an entry unknown by the drop mark,
   `mark_gen <= g`), and
2. `g` is not below the last drop's `mark_gen` (§3.6) nor, on a follower, below `rm_gen` (§3.10).
Otherwise the fill is dropped: something that touched s since the cut was taken, and that the cut may lack, has
changed the entry or will be read by the next fill. Fills do not bump `lgen`.

Who drains, on a writer or a cluster replica: every write section that is taken anyway (publishers, merge listings,
retention changes, drops) drains first, before its own changes and before it bumps `lgen`, which is the same as the
fills having been stored just before; the merge thread, at its timed wake (at least once a second,
merge.c:620-623) and when the queue passes 1,024 entries (the pusher signals `merge_cv`); the warmer's thread after
each batch. On a follower the refresh drains it in its write section, after its own changes (§3.10). None of these
is a scheduler helper, so no query thread is ever parked in `wrlock`.

Why this is safe: every change that could make the fill's answer wrong (a publish of s, a delete of s, a retention
change of s, a merge after a retention change) either settled the entry (it is no longer unknown) or set its touch
above `g`; a drop, and a follower's refresh that removed parts, refuse every older fill. A change listed before `g`
is in the cut, because the cut and `g` were read in one read hold and every such change bumps `lgen` in a write
section. A deferred store is as safe as an immediate one: the conditions are checked when it is stored.

Where fills come from:
- **The lookup that met an unknown entry** and found no memtable row at or above its bound runs a fill search
  after its read hold (§2.3 step 7). The answer needs that search anyway; the fill costs a queue push.
- **A delete** searches its own series (§3.4).
- **The warmer**: right after an open, after a drop, and after the budget's cap lifts (L5), a thread of the volume
  walks the unknown entries in id order, in batches of 1,024 ids. For each batch it takes a cut and `g` in one read
  hold and runs the fill searches as one fork/join group on the query scheduler, capped at `request_threads` like
  any request, so queries share the helpers with it as with any other request. The helpers push to the queue; the
  warmer's thread drains it. `tsdb_latest_unknown` (a metric, per volume) says how many remain.

### 3.9 Open, crash recovery

At open, after `load_parts` and before the WAL replay (db.c:646), `cls_check_parts` (classes.c:1095) checks every
granule of every run of every part, in one loop today. This design splits that loop into **blocks of 32,768 series
ids** that the open's threads (as many as `query_threads`) take from a shared counter. For its block a thread visits
the parts in `db->parts` order and, in each, the runs of the block's ids (found with `gix_run`, one binary search per
part and block) and their tombstones (a binary search in the part's trailer, sorted by `(series, start)`,
internal.h:597-600). The checks are per run and read-only (`check_granule` reads the part; `cls_at` takes `cls_mu`;
each thread has its own `codec_ctx`), so they split cleanly; the first failure any thread finds fails the open (which
part the log line names may differ from today's serial order). The block's 512 KiB of table stays in the thread's
cache across all partitions, and blocks are disjoint, so the build uses plain stores.

`db->parts` is in (partition, min_block) order (manifest.c:283): within one partition key a part visited later has
higher blocks, and across keys a series never has two rows at one timestamp. But keys overlap in time (partition
classes, a switch inside a day), so a series' rows are not visited in time order. Per series of the block:

1. Per run (series s in part p): the run's last granule gives `ts_max` (the check already reads it). If the entry is
   empty, or known with `ts_max >= ts`, or unknown with `ts_max >= u`, the entry becomes `(ts_max, p, granule)`: the
   part and granule indexes held for now in `x`. Nothing else is read. The entry's timestamp (or bound) is then
   always the greatest timestamp of every row of s visited so far.
2. Per tombstone range `[a, b)` of s in p, before p's runs fold: if the entry is known, came from a part of p's
   partition visited before p (a lower block), and its `ts` lies in the range, it becomes **unknown `(k.ts)`**,
   not `(a - 1)` as on the writer (§3.4). Rows of another key visited earlier may lie between `a` and `k.ts` and be
   live: a switch from class X to Y inside the range, a delete over both keys (each key's tombstone is clamped to
   its partition, delete.c:36), and an older row inserted into X afterwards; X sorts first, so Y's tombstone meets
   a known row from Y while X's live row sits below it (the verification's V6). `k.ts` bounds every row seen so
   far, and later runs fold by step 1. Unknown entries keep their bound.
3. At the end of the block, one pass over its known entries: read `GF_LAST` and the `GT_LAST_NULL` flag of the noted
   granule once per series, and store the slot. Where the value is not exact (L12), the entry becomes unknown
   `(ts)`: its timestamp is known, its value is not.

No transient array: the table holds the walk's state. The WAL replay then publishes its rows through
`parts_publish`, which folds them (§3.3). A crash leaves nothing to repair: the table is memory only, rebuilt from
parts that are themselves crash-consistent (intent files).

Cost (U3). At 10^8 series and 365 partitions there are about 3.65 x 10^10 runs; at 10 to 30 ns per run the serial
walk of today takes 6 to 18 minutes, and a build that touched a table line per run in that walk would move 1.2 TB
(dense parts) to 4.7 TB (sparse parts: series of classes 1 to 3, one line per run) through memory, 1 to 3 minutes
more on one core (the verification's P5). Split by blocks, the table is read and written about once per open
(its slice stays in cache), the index is read as before, and both the checks and the build run on every thread:
about 11 to 34 s of walk on 32 threads with a warm page cache, I/O-bound on a cold one. Step 1 adds a compare and a
store in cache per run, step 3 two field reads per series. Volumes open one after another today (volumes.c:1054);
opening them in parallel is a separate change and not assumed here. The benchmark reports absolute seconds, with
dense and sparse parts, build on and off.

Unknown entries after the open: every state series, int64 series with huge last values, NaN last values, and series
whose newest rows were deleted but not yet merged. The first three have a bound equal to their newest timestamp, so
**the next flush or insert of the series settles them** (§3, the fold); only series that are no longer written need
a fill. The warmer does those: at about 10 to 20 µs per fill with a warm page cache (U4), 2.5 million unknown series
(a quarter of 10^7 series being state series) take 1 to 2 s on 32 threads. On a cold cache over NFS each fill needs
at least one random read of a data granule: at 20,000 to 100,000 reads per second that is 25 to 125 s (U5). Answers
are exact meanwhile, from the slow path.

### 3.10 NFS followers

A follower lists parts from MANIFEST and tails the writer's WAL (`db->tails`). `follow_refresh` installs the
writer's whole list at once (manifest.c:572-596), so a follower can skip states: a delete's tombstone part can be
written and merged away between two of its refreshes, together with the row it hid (merge.c:249-280 keeps
tombstones only while an older part remains). Folding only new parts would keep that row (the correctness review's
H3). Its table therefore keeps each known entry's block number (24-byte entries).

On a follower **the refresh is the only writer of the table**: it holds `refresh_lock` throughout
(manifest.c:426-639, so `refresh_lock` > `latest_rw`), retention and drops are the writer's (they reach the follower
as parts that leave), and fills are only queued: the refresh drains them. So the refresh can read entries without
any lock, and `lgen' = lgen + 1` is known before its write section. Every refresh does this:

1. **Outside the lock** (the parts' indexes are mapped files, on NFS), one partition at a time, merge-joining by
   series id the runs of the partition's leaving parts, the runs of its new parts and the tombstones of the new
   list (each part's trailer is sorted by series). For each new part q and series s in it: s's newest row in q
   `(ts, slot, block = q.max_block)` and whether a tombstone of s in a part of the new list, in q's partition, with
   a higher `min_block` covers that `ts` ("shadowed"). From those and the entry, compute s's new entry:
   - **Leaving parts**: a known entry `k` whose block lies in a leaving part's range is re-derived from the new part
     of that partition whose block range covers it (a merge output `M`), if any. If `M`'s newest row of s is not
     shadowed and its timestamp is at or above `k.ts`, the entry becomes that row, with `M`'s block (the merge
     output holds the last-write-wins result at that timestamp, a merged replace included). Otherwise (the row was
     deleted by a tombstone that was merged away, or dropped by retention, or its partition was dropped) it becomes
     unknown `(max(k.ts, M's newest row of s), lgen')`: a shadowed newest row of `M` hides only rows at or above its
     tombstone's start, and `M`'s live rows below it may lie above `k.ts`.
   - **New parts**, in block order, the writer's fold with blocks:
     - not shadowed `R`: replaces a known entry if `R.ts > k.ts`, or equal with a higher block; settles an unknown
       entry only if `R.ts > u` (strictly: on a follower a new merge output can have lower blocks than a listed
       row at `u`); fills an empty one.
     - shadowed `R`: a known entry with `k.ts <= R.ts` and an empty one become unknown `(R.ts, lgen')`.
     - **any `R`, shadowed or not, that does not settle an unknown entry raises its bound**:
       `u = max(u, R.ts)`, touch `lgen'`. A shadowed `R` hides only the rows its tombstone covers; q's other rows
       of s lie below `R` and may lie above `u` (the verification's V3: entry known 950 in A; on the writer
       B = {960, 990}, a tombstone [980, 1000) above it, and a merge of A and B into M = {950, 960, 990}; without
       the raise the entry stays unknown 950 while 960 is live, and a later 955 would settle it).
   - **Tombstones of new parts** over a known entry of a lower block in their partition make it unknown
     `(k.ts, lgen')` (not `a - 1`: on a follower an old part with a higher block than the tombstone can hold a
     live row in the range), and an unknown entry of a series with a tombstone in a new part gets touch `lgen'`.
   A **changed** entry is recorded as `(id, new entry)`. An entry whose row survives unchanged is not written, and
   it keeps its old block: live block ranges in a partition are disjoint and the merge output's range contains
   the old block, so every later comparison by block comes out the same. The common case of a merge-only refresh
   then records almost nothing.
2. **Under the write lock**: set `lgen = lgen'`, store the recorded entries, install the list, and if any part left
   the list set **`rm_gen = lgen'`**. Then drain the fill queue (§3.8), which refuses every fill whose `g` is below
   `rm_gen`: a fill whose cut predates a refresh that removed parts may have read a row those parts held, and an
   unknown entry carries no block to say whether it did (the verification's V2: a delete and a full merge of the
   partition between two refreshes leave s with no rows in the new parts, so nothing would touch the entry).
   Refreshes come every `refresh_ms` (100 ms by default, db.c:52), so fills started after one converge.
3. The warmer runs as on the writer. A fill on a follower carries the block of its row (§3.8).

Cost. The work outside the lock is a merge join over the changed partitions' runs, streamed, so its memory is the
recorded changes only, about 32 bytes each (§2.4). The write section stores only what changed: for a refresh that
brings a merge of parts with 10^7 series, the survivors are not written, and the section is about what the
writer's own section was for the parts that brought new rows; for a refresh that brings a flush of 10^6 series,
about as long as the writer's flush (U2, U7). The §3.2 window does not apply on a follower (24-byte entries cannot
be stored atomically, and no flushing memtable covers the new parts' rows); the benchmark measures follower lookup
p99 during merges of parts with 10^7 series and during flushes.

WAL tail rows are the memtable layer (visible always, as `snapshot_take` reads them with no bound, query.c:240),
ranked newest segment first, above the parts. A follower's open builds the table as the writer's does, keeping the
block of each entry's part. A follower that should not spend the memory runs with `latest_cache = 0`.

## 4. Cost on the insert path

| path | per row | per batch or part | lock |
|---|---|---|---|
| small insert (memtable) | one compare, sometimes a u32 store, on the `mt_series` line `mt_add` writes anyway | none | `buf_lock`, already held; lookups take it in 2 µs sub-holds (§2.3) |
| flush | none | one `(ts, slot)` per series from `mt_write`'s sorted runs; one compare and store per series | `latest_rw` for writing, for the fold and the listing (§3.2; outside it if measured necessary) |
| large insert | none (the part writer compares at each series boundary) | as a flush, per series of the batch | `latest_rw` for writing, with the listing |
| delete, replace | none | the §3.4 rule per series of the commit, and one fill afterwards | same |
| merge | none | none, unless a retention limit changed during it (§3.5) | `latest_rw` for writing at its listing, O(1) plus the fill drain (the merge thread, not a query thread) |

Writer threads do not contend more than today: memtable appends are serialised by `buf_lock` already, publishes by
`publish_lock`. What is new:
- a publish (and a merge listing) waits for the lookups holding the read lock: at most one read hold, 256 ids,
  about 25 to 50 µs, with no I/O in it (§2.3), so a flush that waits holding `publish_lock` and `cls_rw` holds them
  that much longer, and a one-row part (p50 1.56 ms) about 3% longer at worst;
- inserts share `buf_lock` with lookups in 2 µs holds;
- lookups wait for a publish's write section (§3.2), and for the fill drains it carries (at most 4,096 stores).
The benchmark gate of step 2 (§2.3, §10) measures all three against inserts with no lookups at all.

## 5. Queries beyond "the newest"

### 5.1 `latest_at`, the slow path (as of a time, fills, unknown entries)

`end` below the timestamp a lookup found means the answer lies below; the table cannot help. The id goes to
`latest_at(cut, s, end, flags)`, which also serves fills, unknown entries, `SKIP_NULLS` and `latest_cache = 0`. It
always runs after the lookup's read hold, on the chunk's cut, with no lock of the table held (§2.3). In its
**parts-only form** (a fill search: no memtable sources, no end bound, no `SKIP_NULLS`, no cutoff) its result is a
fill; in every other form it is only an answer.

- **Memtable sources** (active, flushing, tails), unless parts-only: the newest row of s below `end` with
  `lsn <= ` the cut's `visible`, the later row on equal timestamps. A sorted run: a binary search for `end`, then
  back over rows above `visible`. Otherwise the active memtable's rows of s are copied in pieces of 4,096 rows, one
  `buf_lock` hold per piece (rows are only appended, and `visible` is fixed), and searched, or sorted for
  `SKIP_NULLS`, outside the lock. `mt_collect` would sort, and allocate, under `buf_lock` (memtable.c:219-270);
  the flushing memtable and the tails are immutable or read the same way.
- **Parts, newest partitions first**: the cut's distinct partitions, ordered by their end (`partition_range`; one
  sort per cut, shared by every id of the chunk). For each partition in turn, skipping those that start at or after
  `end`: compute s's snapshot windows in that partition's parts only (`snap_find`, with the partition's tombstones
  applied as `snapshot_take` does, so granules a tombstone touched are read from their mem runs); a part with no run
  of s costs one binary search in its run list. A part's tombstones of s are found by a binary search on series in
  its trailer (sorted by `(series, start)`, internal.h:597-600), not by the scan of every tombstone that
  `snapshot_prepare` does today (query.c:440-446); a merged tombstone part can hold 10^5 of them (merge.c:256-274),
  and this step changes `snapshot_prepare` the same way. Per source, take the last granule with `ts_min < end`: if
  its `ts_max < end` and it is a plain granule (not moved to a mem run, not a state series, not an int64 beyond 2^53
  or a NaN, L12), its candidate is `(ts_max, GF_LAST)` from the index without decoding; otherwise decode that one
  granule and take its last row below `end`.
- **Stop** as soon as the best candidate's timestamp is at or above the next partition's end: no row of an older
  partition can be newer, and no timestamp is in two keys. In the parts-only form the candidates are the parts'
  only, so a memtable row never stops a fill search early.
- The candidate with the greatest timestamp wins, the higher source priority (`mem_prio`) on ties.

A series usually needs one partition (up to four keys overlap in time with partition classes). Cost (U4): about 5 to
20 µs with one granule decoded, independent of the number of parts in the volume, to be measured at 10, 100 and
1,000 partitions, with a partition of 10^5 tombstones and an unmerged hot partition of about 30 parts. A series
whose newest row is far back walks the partitions in between at one binary search per part each, once: its fill then
answers from the table. It replaces raw_desc for `limit = 1` (L17); raw_desc keeps its windows for larger limits.
Inside one `QUERY_LATEST` the cuts come one per chunk (§2.3), not one per series.

### 5.2 Skipping nulls

`SKIP_NULLS` asks for the newest timestamp whose **last-write-wins** value is not null. Within each source, equal
timestamps first resolve to their last write: `mt_add` counts an equal timestamp as sorted (memtable.c:145), so a
sorted run can hold `[T: 5.0, T: null]`, whose value at `T` is the null. Per source, `latest_at` then finds the
newest non-null row below the bound, stepping over granules whose null count equals their row count (from
the index) and decoding the others. A source's candidate at `T` is accepted only if **no higher-priority source has
a row at `T`**: a newer source's null at `T` overwrote it (its non-null at `T` would itself have been a candidate
and won the tie). The check reads the higher-priority sources' granules that span `T` (their `[ts_min, ts_max]`,
stepped-over null granules included) and decodes one where `T` falls inside it; memtable runs are searched
directly. A rejected candidate moves that source's bound below `T` and the search goes on. A series that has been
null for weeks walks back that far (the cost of a descending query to its last value).

### 5.3 Statistics

`tsdb_last_query_stats` after a lookup counts the granules and rows the slow path decoded; two new counters say how
many series the table answered and how many went the slow way (also as metrics, §6.5).

## 6. Interfaces

### 6.1 C API (`include/tsdb.h`)

```c
typedef enum { TSDB_LATEST_VALUE = 0, TSDB_LATEST_NULL = 1, TSDB_LATEST_NONE = 2 } tsdb_latest_kind;

typedef struct tsdb_latest {
    int32_t status;   /* 0, TSDB_ERR_NOTFOUND (no such series) or TSDB_ERR_DENIED (tsdbd) */
    uint8_t kind;     /* tsdb_latest_kind; NONE whenever status != 0 */
    uint8_t type;     /* the series' tsdb_type; 0 whenever status != 0 */
    int64_t ts;       /* with VALUE and NULL; 0 otherwise */
    union { float f32; double f64; int64_t i64; uint8_t b; uint32_t state; } v; /* with VALUE; 0 otherwise */
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

An id that names no series (zero volume field, bits 48-63 set, a volume or local number that does not exist) is
`TSDB_ERR_NOTFOUND` in its record, as for every lookup by id (AGENTS.md, "Series ids"). A follower, or a cluster
replica answering with `REPLICA_OK`, that meets a local number beyond its SERIES reads SERIES again once per
request (`series_reload`, as the replay does, memtable.c:1157-1160) before it answers NOTFOUND, so a series the
writer has just created is not reported missing. The reload runs outside `latest_rw` (it takes the dictionary's
`append_lock` and reads SERIES, I/O on NFS); the ids it finds are then looked up in a hold of their own.

`tsdb_options.latest_cache` is a byte budget (A6). Ids are 64-bit (`tsdb_sid`), never narrowed; the call splits
them by volume and runs each volume's chunks on the query scheduler. The CLI gets `tsdb DIR latest NAME...`.
`tsdb_abi_version` goes up (new struct and call used by the bindings).

### 6.2 Wire (protocol 13)

```
QUERY_LATEST (39)  u8 flags, u8 0, u16 threads, u32 n (1..1048576), i64 end, [i64 ends[n]], u64 ids[n]
                   flags: TSDB_LATEST_SKIP_NULLS 1, TSDB_LATEST_PER_END 2 (ends[] present; `end` must be 0);
                   other bits reserved (TSDB_ERR_INVALID). The length is exactly that.
  -> frames, each: i32 status, u32 first, u32 m, u8 last, u8 0, u16 0, then m records of 24 bytes:
                   i32 status, u8 kind, u8 type, u16 0, i64 ts, u64 value (the series' slot zero-extended:
                   float32 bits in the low 4 bytes, bool 0/1, state code; 0 with NULL and NONE)
                   records in index order, at most 65,536 per frame; the frame with last = 1 ends the reply.
                   A record with status NOTFOUND or DENIED has kind NONE, type 0, ts 0, value 0.
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
(as QUERY_MULTI, design-auth.md A14). In the case, each id goes through the user's rights map (A15):
- a user without `read` on the id's series gets `TSDB_ERR_DENIED`, and so does a missing id for a user without
  `read` on `all` (denied and missing look alike);
- a missing id for a user with `read` on `all`, and with `--no-auth`, gets `TSDB_ERR_NOTFOUND` (A16);
- both carry kind NONE, type 0, ts 0 and value 0, so nothing in the record tells whether a denied series exists.

`tests/test_auth_ops.c` fails until the row exists.

### 6.4 Clients and bindings (same commit, `make test-clients`)

- C client: `tsdbc_latest(c, ids, n, opts, out)` with the structs of §6.1.
- Rust: `Client::latest(&[Sid], &LatestOpts) -> Result<Vec<Latest>>`, `Latest { status, kind, ts, value: Value }`;
  the embedded `Db::latest` the same.
- Python (PyO3 over the crate): `latest(ids, end=None, ends=None, skip_nulls=False)` returning a list of
  `(ts, value)` tuples, `None` for NONE, a `NotFoundError` or `DeniedError` instance in place for a missing or
  denied id (or raising, by a flag); embedded `Db.latest` the same.
- Java: `long[]` ids (never `int`), `Latest[] latest(long[] ids, LatestOptions o)`; `EmbeddedDb.latest` the same.
- The Pulsar sink does not use it.

Each client's tests compare its answers with the embedded binding's and with tsdbd's for the same data, nulls,
denied ids and missing ids included.

### 6.5 Metrics (`docs/metrics.md`)

`tsdb_latest_requests_total`, `tsdb_latest_series_total{path="table"|"slow"}`, `tsdb_latest_unknown` (entries left
to fill, per volume), `tsdb_latest_table_bytes`, `tsdb_latest_budget_capped` (volumes whose pages the budget
refused), `tsdb_latest_wait_seconds_total` (time lookups waited for a write section),
`tsdb_latest_inflight_total{how}` (§2.2), `tsdb_latest_fills_total{result="stored"|"dropped"}`.

## 7. The cluster

### 7.1 How it fits

The design follows design-cluster.md without additions to its log or its Raft state:

- **Shards are volumes.** A series id names its volume; the volume's leader answers (cluster C1, C15). In direct
  mode the client splits a `QUERY_LATEST` by the leader of each id's volume (TOPOLOGY), sends one request per
  server with its share, and puts the records back in index order (the frames carry `first`); in simple mode the
  entry server does it. This is cluster C16 for QUERY_MULTI, with no budget to split.
- **Replicas keep the table warm.** A replica applies a `COMMIT` marker by `parts_commit` and listing (cluster
  §5.2): the same fold. Its memtable rows are visible up to the commit point (C7), the same `visible` that
  lookups use. `NOW` markers move the cutoff, which lookups read; a `RETENTION` marker goes through §3.5 and a
  `DROP` marker through §3.6; `CLASS` changes nothing. So every in-sync replica's table, read at a commit point,
  answers what the leader's answers at that point.
- **Failover**: the new leader was an in-sync replica, so its table is already complete; nothing is rebuilt. A
  returning replica that cuts a divergent WAL tail (C14) never listed parts above the commit point, so its table
  holds nothing uncommitted; its memtables are cut with the WAL.
- **A new replica** (learner, cluster §8.1) opens its snapshot like any open (§3.9) and then applies the log.
- **Cold tier**: every replica keeps the index of cold partitions locally (cluster §9), so the open's build is
  unchanged; a fill or an as-of read that needs a cold granule's rows reads them from the cold tier (rare: only a
  series whose newest rows are cold and were deleted, or an as-of bound in cold time).

Memory in the cluster: three copies (A3), so 48 GB for a billion series spread over the servers (§2.4). A
replication factor of 4 (cluster D1) adds a copy.

### 7.2 Hot volumes and reads on replicas

Hash placement (cluster §3.3) spreads lookups over volumes on average, but a dashboard on one site's series, or
series created in a named volume, can make one volume hot. All of its lookups go to its leader, or with
`REPLICA_OK` to at most its three (four) in-sync copies (cluster §7.2), and adding servers does not split a volume.
With L19 a volume's lookups run in parallel on its server's cores, sharing only `buf_lock` in short holds; the
ceiling of one volume is then its server's cores. **`REPLICA_OK` with `min_pos`** (read-your-writes after the
client's own insert) is the documented knob for read-heavy dashboards: it multiplies one volume's lookup capacity
by the replication factor, with exact answers at the replica's commit point.

The gate (step 2, §10): one volume, 1 to 32 connections of 100,000-id requests, while it takes 1 M rows/s of
inserts. If it serves 20 M ids/s or more with inserts within §2.3's gate (against the same inserts with no lookups;
5% on throughput, and on p99 in sync mode, 5 µs absolute on p99 in periodic mode; hot and spread sets), no cache tier
is needed; if not, §2.3's remedies (the `buf_lock`-free memtable read, the compact top record) come first.

### 7.3 Considered: a Redis-style cache beside the database

What such a cache wins: reads scale out with any number of replicas; it is isolated from the database's insert
locks; it survives a tsdb restart; operators know it.

What it loses: it is not exact (dual writes race last-write-wins; it never sees deletes, replaces, retention or
sync-mode visibility; a failure between the insert and the SET leaves it wrong); it goes cold when it restarts; it
is a second system to run and to fence at failover; it takes 60 to 100 bytes per key as strings (20 to 30 in packed
hashes) against 16 here; and it has its own authentication, apart from tsdb's grants.

The in-engine table wins exactness, memory, one hop and the grants. It loses read scale-out beyond the replication
factor, which §7.2's gate measures.

## 8. Tests

- **`tests/test_latest.c`, an oracle test.** Random sequences on a few hundred series of every type, ids above
  2^32 and in several volumes: small and large inserts (in and out of order, duplicate timestamps, keys duplicated
  across `SORT_CHUNK_ROWS` chunks of one insert, nulls, NaN with payloads, int64 near `INT64_MIN`/`INT64_MAX` and
  beyond 2^53, timestamp 0 and negative timestamps), never-written series, deletes of the newest rows and of other
  ranges (from `INT64_MIN` too), replaces with and without rows, retention set, raised and lowered with a test
  clock (also during a merge), `drop_before`, class switches (partition classes on), flushes, merges and
  `tsdb_optimize`, close and reopen, reopen after a simulated crash (fork, kill), sync and periodic WAL modes, a
  budget that caps the table mid-volume, and a cap that lifts. Nulls written over values across sources (a memtable
  over a part, a later part over an earlier one) and within one memtable (duplicate timestamps, a null last) for
  `SKIP_NULLS`. With partition classes on: a switch inside a day, a delete across both keys, an older row inserted
  into the old key afterwards, a reopen, then a publish of a row between the tombstone's start and that row (§3.9,
  V6). After every step, for every series: `tsdb_latest` equals
  `tsdb_query_values` ascending over everything, last row (the oracle is a full scan, not the descending path),
  bit for bit; with random end bounds and with `SKIP_NULLS` too. Also with `latest_cache = 0` (the slow path alone)
  and with `desc_window_rows` small. A check after each step that every entry keeps its promise (§2.1) against a
  parts-only scan.
- **Concurrency** (in the same file, run under `make tsan`): writer threads doing small and large inserts and
  deletes, lookup threads checking that each answer is one the history allows (its row was written by a write that
  had begun; no acknowledged newer row is missed; a series' answers never go back in time without a delete in
  between). Test hooks force the races the reviews found: a fill that took its cut before a flush, a delete or a
  retention change of its series, and stores after it (must be dropped); a cut taken in a QUERY_LATEST chunk before
  a publish, with the fill after; a publisher paused between its precomputed list and its write section while a
  fill stores; a merge running across a retention lengthening, with a fill whose cut was taken after the change
  (the entry unknown already); a lookup paused between its entry read and its cutoff read while a lengthening runs;
  a drop between a lookup's entry read and its sweep; a publish below `t` after `drop_before(t)` with a fill whose
  cut was taken before that publish (§3.6 rule 2); a fill whose cut was taken before a drop, stored after the sweep
  (rule 3); a fill search where a memtable row is newer than every part (it must not stop early, §5.1); an in-flight
  top row in sync mode. With the §3.2 window, a lookup paused inside a flush's stores, and a fill whose cut was taken
  in the window. A lookup thread sending `SKIP_NULLS` and as-of requests while a flush waits: the flush's wait for
  the write lock stays below one read hold (no slow path inside a hold).
- **Followers**: a writer and a follower on one directory, lookups compared after each refresh, merges running;
  and a delete followed by a full merge of the partition between two refreshes (the deleted row must not come
  back), also with a follower fill paused across that refresh (`rm_gen`, V2); the merge of a known row's part with
  a newer part whose newest row a later tombstone shadows, then a row between (V3); and a retention drop of rows
  between two refreshes.
- **tsdbd** (`tests/test_server.c`, uring and epoll): the frame, chunks across frames, missing, malformed and
  denied ids (NOTFOUND and DENIED records with type 0), `PER_END`, CANCEL, a 12 client against a 13 server and the
  reverse.
- **Rights**: `test_auth_ops` row; denied and missing alike for a user without `read` on `all`, NOTFOUND with it.
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
- **Inserts under lookups** (the gate of step 2, §2.3): insert rows/s and p99 at 1 and 64 writer threads, sync and
  periodic modes, hot set (1,000 series) and spread set (10^6), while 32 connections send 100,000-id lookups to the
  same volume, against the same inserts with no lookups; beside it the same under a QUERY_MULTI (values,
  descending, limit 1) load at equal ids/s. The publish latency of a flush and of a one-row part while 32
  connections send 100,000-id `SKIP_NULLS` and as-of requests. One volume's ids/s at 1 to 32 connections while it
  takes 1 M rows/s (§7.2).
- **Lookups**: `build/tsdbc-bench latest --ids 1|4096|100000 --conns 1|32`, ids sorted and random (the sort's
  share counted in U1), against tsdbd
  on the 100 M-row data set (100 series) and on 10^6, 10^7 and 10^8 series per volume (scale-bench data); the same
  embedded (`build/tsdb-bench`, a latest class). With steady inserts running alongside, and with the slow path
  forced (`latest_cache` 0) to show what the table buys. Compared with what exists today: one id against a
  descending limit-1 query, 100,000 ids against QUERY_MULTI (values, descending, limit 1).
- **Flush holds**: lookup p99 under steady flushes of 10^6 series, stores inside the write section and in the §3.2
  window, which sets the window's threshold (U2).
- **Slow path**: as of a day back, `SKIP_NULLS`, and fills, at 10, 100 and 1,000 partitions per volume, a partition
  with 10^5 tombstones, and an unmerged hot partition of about 30 parts (U4).
- **Open and warm-up**: open time in absolute seconds with the build on and off, at 10^7 and 10^8 series x 30 and
  365 partitions, dense and sparse parts (series of classes 1 to 3), page cache warm and cold (U3); the time until
  `tsdb_latest_unknown` reaches 0 with a quarter of 10^7 series of type state, written and not written after the
  open, warm and cold (U5).
- **Followers**: follower lookup p99 during refreshes that bring merges of parts with 10^7 series, and flushes of
  10^6 series (U7).
- **Memory**: resident bytes per series at 10^7: the table, memtables, rights maps, the open's peak.

`tools/bench-tsdb.sh` gets the latest classes, and AGENTS.md's benchmark section the numbers.

## 10. Build plan

Each step is a track (tsdb-tracks): build, adversarial review, fix, gates, merge.

| step | what | gates | effort |
|---|---|---|---|
| 1 | `latest_at` (§5.1: partitions newest first, one granule per source, `SKIP_NULLS` with the shadow check) and `tsdb_latest` on the slow path alone (no table), C API, `tests/test_latest.c` with the oracle | `make test`, `make asan`; slow-path numbers at 10, 100, 1,000 partitions | 3 days |
| 2 | The table: `src/latest.c`, pages and budget, `latest_rw`, touch generations, the folds at every listing (flush, insert, replay, delete, replace), the §3.2 window for large flushes, retention and every merge listing under `latest_rw`, `drop_before` with the mark, the sweep-first rule and the sweep, the build in `cls_check_parts` split by id blocks, the fill queue, its drains and the warmer, `top` in `mt_add`, the lookup with read holds of 256 ids and the searches after them, followers (`rm_gen`, changes computed outside the lock); concurrency tests and hooks | `make test`, `asan`, `tsan`, `make jepsen` (it touches the publish path); the benchmark gates of §2.3 (inserts against no lookups; publish latency under slow-path lookups), flush holds | 8 to 9 days |
| 3 | Protocol 13: QUERY_LATEST in tsdbd, `auth_ops`, the C, Rust, Python and Java clients and embedded bindings, the CLI, metrics, fuzz seeds; jepsen's latest check | `make test`, `test-clients`, `integration-tests`, `fuzz-frames` 600 s, `make jepsen` with the new check | 4 days |
| 4 | Benchmarks (§9), README and docs (protocol comment, metrics.md, CHANGELOG, roadmap: the "not planned" line goes), AGENTS.md (the invariant below) | numbers on a quiet machine | 2 days |
| 5 | Cluster: the client split per leader, REPLICA_OK and `min_pos` for the op; with the cluster's own steps | cluster simulators and `make jepsen-cluster` | part of the cluster work |

About three and a half weeks for steps 1 to 4.

**AGENTS.md gains one invariant**: *The latest table is read and written only under `latest_rw`.* Its writers are
publishers (in the write section that lists their parts, after `parts_commit`), the drain of the fill queue (only
into an unknown entry whose touch is not newer than the fill's cut, and only for a cut not older than the last drop
or, on a follower, the last refresh that removed parts), retention changes, drops and their sweep, every merge
listing (which marks entries, unknown ones included, only after a retention change), and a follower's refresh; a
writer that changes an entry on a page the drop mark has not swept sweeps the page first. A new listing point must
fold there; anything that removes live rows other than tombstones of a later block must make the entries it may
affect unknown and set their touch, or refuse every older fill. A fill comes only from a parts-only search with no
end bound and no `SKIP_NULLS`. Lock order: `publish_lock` > `latest_rw` > `manifest_lock` > `db->lock` > `buf_lock`;
`ret_lock` > `latest_rw`; on a follower `refresh_lock` > `latest_rw`; merges and drops take `latest_rw` (write) >
`db->lock`; lookups take `latest_rw` (read) > `db->lock`, then `buf_lock` in sub-holds; the fill queue's mutex is a
leaf. `latest_rw` is never held across I/O: lookups read granules only after their read hold, and a follower's
`series_reload` runs outside it. No scheduler helper waits for `latest_rw` in write mode.

**What lands in a 0.9.x**: protocol 13 (one new op; floor still 12, so no client breaks) and the C API
(`tsdb_abi_version` up). No disk format change: nothing to migrate. The changelog says under "Added": the op, the
call, the option, the metrics; under "Changed": protocol 13, the ABI version.

## Open questions

1. **A null at the newest timestamp: return the null, or the newest non-null?** Recommended: return the null by
   default (it is what the series says now), `SKIP_NULLS` for the newest known value (A2).
2. **On by default, and with what budget?** Recommended: on, for tsdbd and embedded, with a budget of RAM/16 per
   process (16 GB on the production servers: 10^9 entries); 0 turns it off for users who never ask (A6).
3. **Names in the request?** Recommended: ids only on the wire; the clients resolve names (and cache the ids) in
   a `latest_by_name` helper. A "latest of every series under a selector" op (prefix or glob, for dashboards) can
   follow if asked; it needs a name listing per volume, which is its own design.
4. **One instant per request, or per series?** Recommended: per series (A5), documented; QUERY_MULTI (values,
   descending, limit 1) remains for the rare caller that needs one cut per volume.
5. **An exact last value in the granule index (`GF_LASTV`, state codes included)?** Recommended: yes, in the first
   format bump that happens for any reason; until then state series and huge int64 values start unknown after an
   open, are settled by their next flush or filled by the warmer within seconds (minutes on a cold cache over NFS;
   L12, L18, U5).
6. **Future timestamps**: should "latest" default to `end = now + 1` so a device with a wrong clock does not stick?
   Recommended: no; the default is the greatest timestamp (A1), and callers that distrust clocks pass the bound
   (the slow path). A metric of series whose latest is more than an hour ahead of the clock would help operators.

## Unverified assumptions

- U1: lookup cost of 0.1 to 0.2 µs per id on one thread, the sort included, about 15 ns of it under `buf_lock`; a
  read hold of 256 ids lasts 25 to 50 µs; to be measured (§9).
- U2: 5 to 15 ms of write hold for the stores of a million-series flush, during which lookups of that volume wait.
- U3: split by id blocks, `cls_check_parts` with the build takes about 11 to 34 s at 10^8 series x 365 partitions
  on 32 threads with a warm page cache, and the build adds below 10% to it, dense or sparse parts; reported in
  absolute seconds.
- U4: a slow-path read (a fill, an as-of bound) costs 5 to 20 µs, whatever the number of partitions or tombstones.
- U5: with a quarter of 10^7 series of type state, the warmer reaches `tsdb_latest_unknown = 0` within 2 s of the
  open on 32 threads with a warm page cache; 25 to 125 s cold over NFS.
- U6: one volume serves 20 M ids/s with its inserts within §2.3's gate (§7.2).
- U7: a follower refresh's write section is about as long as the writer's own section for the same new rows, and
  nearly empty for a merge-only refresh.

## Changes after review (2026-11-06)

| finding | where it is now |
|---|---|
| Correctness H1, performance H2(a): the publisher's fold was computed outside the lock and raced fills | L7, §3.2: only the per-series newest rows are computed outside; compare and store inside the write section. The invariant names every writer. |
| Correctness H2, performance H2(b): a fill read `lgen` after the request's cut | §2.3 step 5, §3.8: the cut and `g` are read in one read hold; per-entry touch generations (§2.1) |
| Correctness H3: a follower missed tombstones merged away between refreshes | §3.10: leaving parts re-derive their entries; new parts fold with blocks and a shadow check |
| Correctness M1: retention lowered | §3.5: fills search without a cutoff (empty = no row in parts); limit and marking in one section; merges across a retention change mark their range |
| Correctness M2: `drop_before` outside the lock order | §3.6: `latest_rw` then `db->lock`, retire and mark in one section |
| Correctness M3: `SKIP_NULLS` lost the shadow of a newer null | §5.2: a candidate is accepted only if no higher-priority source has a row at its timestamp |
| Correctness M4: zeroed pages read as rows at 1970 | L4, §2.1: the timestamp is stored XOR `INT64_MAX`; all-zero is empty |
| Correctness M5: unknown ids answered "none" | L15, §6.1-6.3: NOTFOUND per record after one SERIES reload on followers and replicas; DENIED and NOTFOUND records carry type 0 |
| Correctness low: liveness of one `lgen`; tails missing from the ranking; chunked large inserts; the delete's fill under locks | §2.1 (touch), §2.3 step 5, §3.3, §3.4 |
| Performance H1: lookups held `db->lock` and `buf_lock` for a whole chunk | L19, §2.3: O(1) under `db->lock`, 128-id sub-holds of `buf_lock` with prefetch; gate and fallback |
| Performance M1: the slow path was O(parts) | L17, §5.1: partitions newest first, windows per visited partition, early stop |
| Performance M2: the rebuild read `GF_LAST` per run; volumes do not open in parallel | L11, §3.9: one read per series after the walk, in parallel; the claim about volumes is dropped |
| Performance M3: the warmer was too slow | L12, §3.8, §3.9: bounds let the next flush settle live series; the warmer runs at full width; `GF_LASTV` in the first format bump |
| Performance M4: flush stores held the write lock for milliseconds | §3.2: measured; a publish-pending window for flushes if the hold exceeds 2 ms |
| Performance M5: memory had no budget and the doc counted only the table | A6, L5, §2.4: a byte budget; the per-series memory line; design-cluster.md §10.2 counts memory |
| Performance M6: hot volumes | §7.2, §7.3: the gate, `REPLICA_OK` with `min_pos` as the read knob, the comparison with a cache beside the database |
| Performance L1: whole-table scans on drops and retention | §3.6: the drop mark and a sweeper; retention is per series (refuted for retention, §3.6) |
| Performance L2: the in-flight scan under `buf_lock` | §2.2: wait for the group commit and look again; a scan in pieces only as a last resort |

Second round: a verification of the revised design, correctness (V) and performance (P), the same day.

| finding | where it is now |
|---|---|
| V1 (high): the drop mark lost writes made after the drop, and missed entries already unknown | §3.6: writers sweep a page before they change an entry on it; fills whose cut predates the last drop are refused (§3.8); tests in §8 |
| V2 (high): a follower refresh that removed rows did not re-touch unknown entries | §3.10: `rm_gen`, the generation of the last refresh that removed parts; older fills are refused |
| V3 (high): a shadowed newest row did not raise an unknown entry's bound on a follower | §3.10: any new row that does not settle an unknown entry raises its bound; a failed re-derivation takes the covering part's newest row into the bound |
| V4: a merge across a retention change skipped unknown entries; its `ret_gen` compare was outside the lock, and taking `latest_rw` in `merge_commit_locked` would invert the order | §3.5: every merge listing takes `latest_rw` (write) before `db->lock` and compares under it; unknown entries get the touch too; no `publish_lock` |
| V5: lookups applied the cutoff after releasing `latest_rw` | §2.3 step 3: the cutoff is read inside the hold |
| V6: the open's tombstone bound `a - 1` was wrong when keys overlap in time | §3.9 step 2: unknown `(k.ts)`, the greatest row seen so far; an oracle case with classes and a reopen (§8) |
| V7, P1 (high): the slow path ran inside the read hold | §2.3: the hold takes only entries, memtable reads, the cutoff and the cut; searches run after it; holds of at most 256 ids; publish latency gated under slow-path lookups (§2.3, §9) |
| V8: a lookup's fill was not the parts' newest row (early stop on memtable rows, end bounds, `SKIP_NULLS`) | §3.8, §5.1: fills only from the parts-only form; an unknown entry met by a memtable row at or above its bound is answered without a search |
| P2: the insert gate's baseline was degraded, its p99 could not hold in periodic mode, and the hot set's coherence misses | §2.3: baseline with no lookups, a QUERY_MULTI reference, per-mode criteria, hot and spread sets; remedies by cause (compact top record; no `mt_collect` sort under `buf_lock`, §5.1) |
| P3: a write section per chunk and per warmer batch convoyed lookups and parked scheduler helpers | §3.8: a per-volume fill queue drained in write sections taken anyway, by the merge thread, the warmer's thread, or the follower's refresh; no helper waits for the write lock |
| P4: a follower refresh did O(series) under the write lock and kept uncounted notes | §3.10: changes computed outside the lock (the refresh is the follower's only writer), unchanged entries keep their block and are not written, notes streamed per partition; §2.4 counts what is kept; U7 |
| P5: the open build's memory traffic, and a serial walk | L11, §3.9: the walk split into blocks of 32,768 ids over the open's threads, each block's table in cache; U3 in absolute seconds with sparse parts |
| V9 (low): the §3.2 window's touch generation | §3.2: `lgen'` reserved in step 1, used as the touch in step 2, set in step 3 |
| V10 (low): `SKIP_NULLS` and duplicate timestamps within one memtable | §5.2: each source resolves equal timestamps to the last write first; oracle case |
| V11, P9 (low): `refresh_lock` order, `series_reload` under the lock, the prefetch's data race | §3.10 and the invariant; §6.1; §2.3 step 4 (the array pointer is an atomic) |
| V12 (low): follower fills need a block | §3.8, §3.10 |
| P6 (low): at most one memtable flushes; transients uncounted; the budget across volumes | §2.2, §2.4 (and design-cluster.md §10.2: 15 to 30 GB); L5: a cap lifts, later pages start unknown |
| P7 (low): sorting a chunk | §2.3: an O(n) check, else a radix sort; counted in U1 |
| P8 (low): tombstone scans in the slow path | §5.1: a binary search on series in each part's trailer, `snapshot_prepare` too |
| P10 (low): U5 assumed a warm cache | §3.9, U5: 25 to 125 s cold over NFS |
| P11 (low): the §3.2 window is predictable | §3.2: built in step 2, with a threshold set by measurement; step 2's effort |
