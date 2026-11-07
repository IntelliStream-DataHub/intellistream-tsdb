# The latest point of every series, from a sharded in-memory table

Status: design; step 1 (the slow path) is on main, step 2 (the table, `src/ltab.c`) is built on branch `latest-s2`. Branch `latest-design`, written on 2026-11-06 from main at ea89968, revised the
same day after a correctness review and a performance review, again after a verification of both, and a third time
after a second verification and a simplification review that took the user's answers below into account (what
changed in each round: "Changes after review", at the end).
Code references are `file:line` at ea89968.

Decided by the user on 2026-11-06:
- tsdb serves the **latest datapoint per series**. This reverses the line "Latest-value lookups ... not planned" of
  2026-10-24 ([roadmap.md](roadmap.md), "Not planned"); the build plan removes it there.
- The answers come **from a distributed, sharded in-memory cache**.
- **The default answer is the newest non-null value.** The table stores each series' newest non-null row. A null
  written at a newer timestamp leaves it in place; a null that overwrites the cached row at the same timestamp (last
  write wins) invalidates it. An option (`WITH_NULLS`) returns the true newest row, a null included.
- **On by default**, with a budget of RAM/16 (`latest_cache`).
- **Ids only on the wire**; the clients get `latest_by_name` helpers.
- **An exact value in the granule index** comes with the next format bump that happens for another reason.

Related designs: [design-cluster.md](design-cluster.md) (volumes led and replicated; this table must fit it),
[design-auth.md](design-auth.md) (rights), [design-series64.md](design-series64.md) (ids name their volume),
design-cdc.md on branch `cdc-design` (positions, `fno`), [compatibility.md](compatibility.md) (what a 0.9.x may
change).

## Summary

- **One question, answered in well under a microsecond per series**: "what is the newest value of these series?",
  for 1 to a million ids in one request (`QUERY_LATEST`, protocol 14; `tsdb_latest` in C).
- **"Newest value" means the greatest timestamp whose last-write-wins value is not null**, among the series' live
  rows. It is exactly the last non-null row of an ascending raw query over everything; that query is the reference,
  and the tests compare against it. With `WITH_NULLS` the answer is the last row of that query, null or not.
- **The cache is sharded by volume** and lives inside each volume's engine: no separate cache service. Every series
  id names its volume, so a lookup routes like any request. In the cluster the volume's leader answers, and its
  replicas keep the same table warm because they apply the same commits.
- **Two layers.** A per-volume **latest table** holds, for each series, the newest non-null row *in the parts* (16
  bytes). The **memtables** already hold the newer rows; a field `top_nn` and a few flag bits (free in the struct's
  padding) find a series' newest non-null memtable row in O(1). A lookup takes the newest of them, after checking that
  no newer source wrote a null at that same timestamp.
- **Entries are read and written only under one per-volume lock, `latest_rw`**: publishers (flush, large insert,
  delete, replace, WAL replay) change them in the same write section that lists their parts; retention changes,
  drops, the rare merge listed across a retention change and the warmer write under it too. A lookup therefore never
  sees a row before queries do, nor misses one that queries see.
- **An entry can be *unknown*** (after a delete of the newest row, a null over it, a retention limit lengthened, or
  an open that could not read the value exactly). An unknown entry carries a timestamp bound, so the next publish of
  a newer non-null row settles it. **Lookups never write the table**: an unknown entry with no memtable row at or
  above its bound is answered by a search on a snapshot, and its id goes to the volume's **warmer**, the only thread
  that fills entries. The warmer stores a batch only if no write section ran since the batch's snapshot (a
  per-volume generation, `lgen`).
- **Cost on the insert path**: a null test and sometimes a 4-byte store per memtable row, on a cache line the insert
  already writes; per published part, one compare and store per series it holds, under the write lock. Lookups hold
  `latest_rw` for reading in holds of at most 256 ids (about 25 to 50 µs, no I/O, no snapshot taken) and the
  small-insert lock (`buf_lock`) in sub-holds of 128 ids, about 2 µs each.
- **Memory**: 16 bytes per series per copy, within a byte budget (`latest_cache`, default RAM/16); series beyond the
  budget are answered exactly by the slow path. 16 MB for a million series, 1.6 GB for 10^8, 6 GB per server for
  10^9 in a cluster of three copies on 8 servers. NFS followers keep no table (§3.10).
- **Rebuilt at open** from the granule index (each granule's last value, its null flag and its timestamp, already
  stored), inside a loop the open already runs, which this design splits by series range over the open's threads,
  with no transient memory beyond the table. What the index cannot give exactly (state series, int64 beyond 2^53,
  NaN payloads, series whose last row is a null) starts unknown with a bound; the next flush of a non-null row
  settles it, and the warmer at the scheduler's full width fills the rest within seconds. No format change.
- **As of a time, nulls included**: the same op with an end bound or a flag; those go the slow way (one granule per
  source, newest partitions first), not through the table, unless the bound is above the cached row.
- **Rights**: `read` on each series; denied and missing look alike, as everywhere; a missing id is `NOTFOUND`.

## Assumptions (decisions the user may change)

| # | Assumption | What changes if it does not hold |
|---|---|---|
| A1 | **Latest = the greatest timestamp** among the series' live rows whose last-write-wins value is not null (A2). Not the most recently *written* row. | "Most recently written" needs a write sequence per row in parts, which tsdb does not keep, and would disagree with every query. It would be a different feature (closer to the change feed of design-cdc.md). |
| A2 | **Decided (user, 2026-11-06): the newest non-null value by default.** `WITH_NULLS` returns the newest row, which may be a null (kind NULL appears only with it); it takes the slow path. | If the table had to answer `WITH_NULLS` fast, each entry would need a "newer null" bit kept by every publish; added only if measurements ask for it. |
| A3 | **Sharded by volume, inside the engine.** Each volume keeps its series' newest points; in the cluster the volume's leader answers and its replicas keep the table warm. No cache service of its own. | A separate cache tier is rejected in L2 and §7.3; it cannot be exact without being on the write path anyway. |
| A4 | **Exact and fresh.** A lookup reflects every acknowledged write and delete, and never a write that queries do not show yet, by the same snapshot rules as a query. | A "fast but maybe stale" mode would need no lock at all, but every client would have to guess how stale. Not offered. |
| A5 | **One instant per series, not per request**: each series' answer is the state at one instant during the request; two series of one request may be read at different instants (a request for 100,000 series may see an insert in some series and not yet in others). | One instant per volume per request needs a lock held across the whole request, which would hold up every publish. Decision D1. QUERY_MULTI with `VALUES`, descending, limit 1 keeps its one cut per volume for those who need it. |
| A6 | **Decided (user): on by default**, as a byte budget: `tsdb_options.latest_cache` (default RAM/16 for the process, shared by its volumes; tsdbd `--latest-cache BYTES`, `0` off). Series beyond the budget, every series when it is 0, and every series on an NFS follower are answered exactly through the slow path. | |

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| L1 | **Latest is defined by the existing query**: the last non-null row of `tsdb_query_values(series, cutoff, end, ASC)` is the reference answer, bit for bit (value bits included, NaN payloads too); with `WITH_NULLS` its last row. | A definition of its own | One meaning in the whole database; the tests have an oracle that is already tested; deletes, retention, LWW and memtable visibility come for free. |
| L2 | **The cache is part of each volume's engine** (`src/latest.c`), sharded by volume like everything else. | A cache service in front (Redis or a tsdb tier); a cache per tsdbd keyed by name | A separate copy must be kept in step with LWW, deletes, replaces, retention and failover; that is the engine's job and it already does it. A cache beside the engine is either on the write path (then it is this design, one hop further) or stale. §7.3 compares the two in full. |
| L3 | **Two layers: a table for the parts, the memtables for newer rows.** | One table updated by every row | Rows in the memtable become visible only when synced (sync mode) or committed (cluster), not when appended; a single table would need a WAL position per entry and an update pass after every group commit, on the commit's critical path. The memtables already hold those rows in series order with their positions. |
| L4 | **Table entry: 16 bytes plus one bit**: `u64 t` = the timestamp XOR `INT64_MAX`, `u64 x`, and an *unknown* bit in a per-page bitmap. All-zero memory is **empty** (`t = 0` is `TSDB_TS_MAX`, which is never a row's timestamp). **Known**: bit clear, the newest non-null row's timestamp, `x` = its value slot. **Unknown**: bit set, the timestamp field holds a **bound `u`** (every live non-null row of the series in parts has `ts <= u`), `x` = 0. | `ts = TSDB_TS_MAX` with slot codes for the states; 24 bytes with the block number; a type tag per entry; a table of doubles (`GF_LAST`); a per-entry generation | Zeroed pages must mean "empty", not a row at 1970 with value 0 (timestamp 0 and negative ones are valid, tsdb.h:80-82). The bound lets a later publish settle an unknown entry without a read. The type is per series (SERIES). Doubles are inexact for int64 above 2^53 and absent for state series. A per-entry generation existed only for fills by lookups, which are gone (L10). |
| L5 | **The table is paged**: 65,536 entries (1 MiB, plus an 8 KiB bitmap) per page, a page table sized for 2^31 local ids (256 KiB), pages made in id order and never moved or given back, within the byte budget (A6), which volumes share first come, first served. The first page the budget refuses caps the volume until it is opened again: ids from there on have no entry and are answered by the slow path. | One array, reallocated; eviction of cold pages; lifting the cap when budget is freed | Growing a 1.6 GB array copies it under the lock. Pages made in id order by the first fold that needs them start empty correctly (no earlier listing held a row of their ids, since that listing would have made them). A cap that lifts would make pages whose ids have rows already, a state of its own ("unknown, no bound") that no publish settles; a reopen rebuilds within the new budget anyway. Eviction would need a policy; a cap is exact and simple. |
| L6 | **Memtable layer: `top_nn`**, a u32 in `mt_series` (the index of the newest non-null row, the later one on ties), the series' type byte, and four flag bits in what is today the `sorted` byte: `sorted`, `has_nn`, `has_null`, `nn_stale`. `mt_add` keeps them. They fit in the struct's padding (40 bytes stay 40). | A copy of the newest row in each `mt_series` (64 bytes); `top` (any value) and `top_nn` both (48 bytes) | Same answer with one more cache miss per lookup; 24 more bytes per series slot in every memtable, which is dense by local id. Two indexes would add 20% to `mt_series` (the verification's N5), and `WITH_NULLS` takes the slow path anyway. |
| L7 | **Entries are read and written only under `latest_rw`** (a per-volume writer-preferring rwlock), and **every write section bumps `lgen`**. Writers: publishers (between `parts_commit` returning 0 and the end of their listing, which happens in the same section), a retention limit lengthened, `drop_before`, a merge listed across a retention change, and the warmer. Lookups hold it for reading in holds of at most 256 ids, with no I/O and no snapshot in them. Publishers compute outside the lock only what does not depend on the table (each series' newest non-null row and null range in their parts); compare and store happen inside. | Computing the fold before the write lock; changing entries after the listing; a seqlock | The first review found that a fold computed outside the lock races fills (a stale row stored for good). After the listing, a lookup could see a query's answer missing for a moment; before the commit point, a write that then fails. A seqlock retries forever under steady large inserts; a writer-preferring rwlock bounds both waits. §3.2 has a measured exception for large flushes. |
| L8 | **Merges, class switches and the retention clock never change the table**, with one exception: a merge during which a retention limit changed (§3.5). Lookups apply the cutoff (`eng_cutoff`) at read time, as queries do. | Updating it on every merge | A merge keeps the last-write-wins value of every live (series, ts), nulls included; it drops rows below the cutoff, which lookups clamp, rows tombstones hid and rows a later write overwrote, none of which the table holds as its row. |
| L9 | **Publishes fold by rules** (§3, §3.4): a newer non-null row replaces a known entry or settles an unknown one whose bound it reaches; a null of the commit that may lie at the known row's timestamp, or a tombstone over it, makes the entry unknown with a bound. | Recomputing every touched series at once under the lock | The rules are exact in every case they decide; only "the newest non-null row was overwritten or deleted and nothing newer replaced it" needs a read, and that read must not run under the lock. |
| L10 | **One filler per volume: the warmer** (§3.8). It takes a cut and `g = lgen` in one read hold, searches the parts on the scheduler's helpers after the hold, and stores the batch in one write section only if `lgen == g` and no §3.2 window is open. **Lookups never write the table**: they answer an unknown entry by a search and push its id to the warmer's hint set. Whoever makes an entry unknown gives it a correct bound and pushes its id. | Fills by every lookup, through a queue, with a generation per entry (the design before the third round); no warmer at all | Fills stored after their snapshot produced 8 of the 27 findings of the first three rounds, half of them the same omission (a writer did not touch an unknown entry); every new event that makes an entry unknown (the null rule is one) would need its own touch rule. A per-volume generation is enough when there is one filler that retries: a parts-only search depends only on the parts list, which every write section changes and bumps. Without a warmer, dead series with unknown entries stay slow for ever (§3.8). |
| L11 | **Rebuilt at open**, inside `cls_check_parts` (classes.c:1095), which already walks every run of every part in (partition, block) order. The walk is split into blocks of 32,768 series ids that the open's threads take in turn; for its block a thread visits every part's runs of those ids, so the block's 512 KiB of table stays in its cache across all partitions. The table itself holds the walk's state (timestamp, part and granule of the newest run so far), tombstones are applied as the walk meets them, and one pass at the end of each block reads `GF_LAST`, the null flag and the null count once per series. The volume's pages are reserved from the budget before the walk, in id order. | A LATEST file written at close; a lazy build on first use; a transient array per series; the serial walk | A file is one more durable thing with fail-stop and torn-tail rules, and is wrong after a crash anyway. A lazy build delays the first answers by minutes on a big volume. Reading `GF_LAST` per run would cost series x partitions field reads (the performance review); per series it is one. A serial walk that touches a table line per run moves terabytes at 10^8 series (the verification's P5). Reserving pages first keeps "the first refused page caps the volume" true when threads build blocks out of order (the verification's N9). |
| L12 | **What the index cannot give exactly starts unknown with a bound**: state series (no time-weighted statistics, twa.c:126-131; the bound is the timestamp, or one below it when the granule is all null), int64 whose last value as a double is at or beyond 2^53 in magnitude (twa.c:70-73), NaN last values (the payload), and a last row that is a null (`GT_LAST_NULL`, twa.c:107-109; the bound is one below its timestamp). The next flush or insert of a non-null row at or past the bound settles it; the warmer fills the rest at the scheduler's full width right after the open. | Decoding their last granule during the open | The warmer does the same decoding in parallel and does not delay the open; series that are still written are settled by their next flush without any read. The format bump of L18 removes the case. |
| L13 | **No latest table on NFS followers** in this design: `--follow` (and an embedded follower open) forces `latest_cache = 0`; a follower answers every id by the slow path (§3.10). A follower table is a later step (§11). | The incremental follower table of the earlier rounds (24-byte entries, folds by block, shadow checks, re-derivation from leaving parts, `rm_gen`) | It produced six findings in three rounds and needed a seventh (the verification's N3: a null in an already listed part with a higher block than a new merge output). The cluster does not need it: NFS followers stay for single-server use, and in a cluster replicas take their role (design-cluster.md:22, 739) and use the writer's path (L16). The slow path costs 50 to 100 times more per id there. |
| L14 | **One op, `QUERY_LATEST`**, protocol 14: ids (1 to 1,048,576), an optional end bound (one, or one per id), a `WITH_NULLS` flag; replies of fixed 24-byte records in index order. The floor stays 12. Clients add `latest_by_name`. | Names in the request; a QUERY_MULTI kind | Clients already resolve and cache ids (insert paths need them). A QUERY_MULTI kind would carry a template and a frame per series: 30 bytes of framing for 24 of answer. |
| L15 | **Rights: `read` on each series**, checked per id in the op's case as QUERY_MULTI does; a denied id answers `TSDB_ERR_DENIED` in its own record, whether or not it exists; a missing id is `TSDB_ERR_NOTFOUND` for a user with `read` on `all` (and with `--no-auth`). | Refusing the whole request; "none" for a missing id | The same rule as QUERY_MULTI and as lookups by id elsewhere (AGENTS.md, "Series ids"; design-auth A16). |
| L16 | **The cluster changes nothing in the table**: replicas apply `COMMIT` markers through the same listing path and see memtable rows up to the commit point, so their tables match the leader's at every commit point. Clients split by leader as QUERY_MULTI does (cluster C16); `REPLICA_OK` and `min_pos` as for queries. | A cluster-wide latest service | It would be a second replicated state to fence at failover. Here a new leader's table is already warm. |
| L17 | **The slow path gets a real "limit 1"** (`latest_at`, §5.1): partitions newest first, those that end at or before the cutoff skipped, a part's snapshot window computed only when it is visited and only if its series range holds the id, one granule per source, stopping as soon as no older partition can hold a newer row. | Using raw_desc as it is | raw_desc decodes about 8 granules to return one row and enumerates every granule of the series in every part (query.c:1930-1953): O(parts) per series. |
| L18 | **No format change now.** The index already stores what the build needs (format 8). The next format bump that happens for any reason adds, per granule, **the newest non-null slot and its timestamp** (or "none" for an all-null granule), state codes included. | A format bump now; storing only the last slot | A format change costs every user an export and import (compatibility.md). An exact last slot would still leave every series that ends in nulls (an offline sensor) unknown after an open (the simplification review's S7); the newest non-null slot makes the open build exact for every type. |
| L19 | **Lookups touch the volume's locks in short holds**: `latest_rw` for reading in holds of at most 256 ids, never across a read of a granule or a snapshot; `db->lock` only to reference the memtables and read `visible`; `buf_lock` in sub-holds of at most 128 ids, with the lines they need prefetched. A chunk that needs searches takes its cut (O(parts) under `db->lock`) after its holds, outside `latest_rw`. | Holding `db->lock` and `buf_lock` across a 4096-id chunk; running the slow path or taking the cut inside the read hold | The performance review: those are the volume's query and small-insert mutexes; long holds stall inserts and make lookups of one volume run one at a time. The verification (V7, P1): a slow path inside the read hold holds `latest_rw` for tens of milliseconds while a flush waits for it holding `publish_lock` and `cls_rw`. Lookups no longer need `lgen`, so their cut leaves the hold too (S1). |
| L20 | **Drops leave a floor**: `drop_before(t)` sets the volume's `drop_floor = max(drop_floor, t)` in the write section that retires the partitions. A known entry with `ts < drop_floor` reads as unknown `(ts)` to everyone (lookups, folds, the warmer), always. | A drop mark with epochs, per-page `swept` and a sweep-first rule for writers; a sweep under the lock | The mark could put a sweep of the whole table into one publisher's write section (the simplification review's S3: up to 76 ms with `publish_lock` held). The floor is exact (§3.6) and O(1); its cost is that series whose newest non-null row lies below a drop's `t` and survived it take the slow path until they are written again. |

## 1. What "latest" means

For a series s, a lookup with end bound `end` (default `TSDB_TS_MAX`, "no bound") answers:

- the greatest timestamp `t` such that `cutoff(s) <= t < end`, among s's live rows (rows a tombstone hides and
  expired rows are not live; memtable rows count once they are visible, the same rule as queries), **whose
  last-write-wins value is not null**: a later null at `t` overwrites the value, a null at another timestamp does
  not matter;
- its value, as the last write at `t` left it;
- a kind: **value**, or **none** (no live non-null row before `end`).

With `WITH_NULLS` the row is the one with the greatest `t` whatever its value, and a third kind, **null**, appears:
the last write at `t` was a null.

An id that names no series is not "none": its record has status `TSDB_ERR_NOTFOUND` (or `TSDB_ERR_DENIED`, §6.3).

The answer equals the last non-null row of `tsdb_query_values(s, cutoff, end, ASC)` (with `WITH_NULLS`: its last
row, a null mapped to kind null) (L1); the tests check exactly this.

Consequences worth saying:
- A sensor that has reported nulls for an hour answers its last value from before that hour. A client that needs to
  know that the newest point is a null asks with `WITH_NULLS` (the slow path, §5.1).
- A stray row far in the future (a device with a wrong clock) is the latest until it is deleted or overwritten by a
  null. A client that wants "the newest point that is not in the future" sends `end = now + 1`; that is the slow
  path (§5.1). Operators see such series in `tsdb_latest_future_series` (§6.5, D2).
- A series that has never been written, one that has written only nulls, and one whose every non-null row expired or
  was deleted, all answer **none**.

## 2. The two layers

### 2.1 The latest table (parts)

Per volume (engine), indexed by local id, in pages of 65,536 entries (L4, L5). States and what they promise, "row"
meaning a live row of the series in the parts whose last-write-wins value is not null:

| state | encoding | promise |
|---|---|---|
| empty | all zero | no non-null row of the series in the parts |
| known `(ts, slot)` | bit clear, `t = ts ^ INT64_MAX`, `x = slot` | the newest non-null row in the parts is this one (the cutoff aside: lookups clamp) |
| unknown `u` | bit set, `t = u ^ INT64_MAX`, `x = 0` | every non-null row in the parts has `ts <= u`; nothing more is known |

A known entry with `ts < drop_floor` (L20, §3.6) keeps its encoding but promises only what an unknown `(ts)` does.

A page made when ids first need it is zeroed: empty, which is right because no listing before held a row of those
ids (L5). A page made by the build at open follows the same order.

`latest_rw` (a `pthread_rwlock_t`, writer-preferring: `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`) guards the
entries, the bitmaps, the page table, `drop_floor`, `publish_pending` (§3.2) and **`lgen`**, a per-volume counter
that **every write section bumps**. Only the warmer reads `lgen` (§3.8): a parts-only search taken on a cut at
`lgen = g` is stored only if `lgen` is still `g`, so no write section ran between the search's snapshot and its
store. Lookups do not read it.

### 2.2 The memtable layer

The active memtable and the flushing one (at most one: a memtable joins `flushing` only when none is there,
memtable.c:843; the array has room for `MAX_FLUSHING` = 4, internal.h:1192) hold every row that is newer than the
parts, indexed by local id (`mt_series`, internal.h:1120-1126: three pointers, `n`, `cap` and the `sorted` bool, 33
bytes padded to 40). This design adds a u32 `top_nn` and a type byte, and turns `sorted` into a flags byte:

| field | meaning, kept by `mt_add` (memtable.c:137) |
|---|---|
| `type` | the series' type, read once from SERIES when the series' first row enters this memtable; `type_null(type)` is its null slot |
| `sorted` | as today: non-decreasing timestamps in insertion order |
| `has_nn`, `top_nn` | a non-null row was added; `top_nn` is the index of the newest one (the later on equal timestamps) |
| `has_null` | a null was added |
| `nn_stale` | a null was added at `ts[top_nn]` after `top_nn`: last-write-wins made that row a null |

Per row, before the append: a non-null row with `!has_nn` or `ts >= ts[top_nn]` sets `top_nn = n`, `has_nn`, and
clears `nn_stale` (a newer non-null row, or a non-null row at the same timestamp that overwrites the null); a null
sets `has_null`, and `nn_stale` if `has_nn` and `ts == ts[top_nn]`. A null at a newer timestamp changes nothing
else, and one at an older timestamp cannot touch `top_nn`. The `mt_series` line is already in cache there (`mt_add`
just wrote `n` and checked `sorted`). 33 + 1 + 4 bytes, with the u32 at offset 36: 40 bytes stay 40.

A memtable's **candidate** for s is row `top_nn` if `has_nn`, `!nn_stale` and `lsn[top_nn] <= visible` (always
outside sync mode and the cluster). In every other case (`nn_stale`: the newest non-null row was overwritten and the
next one needs a scan; or `top_nn` in flight: its group commit has not synced yet, and the newest visible non-null
row needs a scan back over nulls) the memtable cannot answer in O(1), and the id goes to the search on the chunk's cut
(§2.3, §5.1), which reads the memtable in pieces outside `buf_lock`. Both are rare outside sync mode; in sync mode a
search whose answer is a memtable row costs about a microsecond (§5.1's stop rule ends it before any part). A metric
counts them (`tsdb_latest_search_total{why}`, §6.5).

**Shadowing.** A candidate at `T` from a lower-priority source (the flushing memtable, or the table) is the answer
only if no higher-priority memtable's last write at `T` is a null (the verification's N1: a correction written as a
null over a part's row). A memtable **cannot shadow** `T` if `!has_null`, or if it is `sorted` and all its rows,
visible or not, lie above or below `T` (`ts[0] > T`, or `ts[n - 1] < T`). An offline sensor that reports nulls
after its last value is the common case, and passes in O(1): its nulls are newer than `T`. In every other case the
id goes to the search on the cut, which applies §5.2's rule exactly.

### 2.3 A lookup

For the ids of one volume, in chunks of up to 4096. A chunk is put in local-id order (each id keeps its index for the
reply): one O(n) pass checks whether it already is (dashboards usually send ids in order), else a two-pass radix
sort on the 32-bit local id, about 10 ns per id. Each chunk is read in **holds of at most 256 ids**:

1. Take `latest_rw` for reading.
2. Take `db->lock`: reference the active and flushing memtables and read `visible` (in sync mode `wal_durable`, in
   the cluster the commit point, else no limit). Release it. That is O(1). It is consistent with the table: no
   publish can fold or list while this hold lasts.
3. Read the table entries, `drop_floor`, each id's cutoff (`eng_cutoff(s)`, a relaxed atomic load with no lock,
   retention.c:211-223) and the flushing memtable (immutable, no lock). The cutoff is read inside the hold because a
   lengthened limit and the marking of the entries it affects happen in one write section (§3.5); read after the
   hold, a new cutoff could meet an old entry that names a row merged away long ago.
4. Read the active memtable in sub-holds of `buf_lock` of at most 128 ids. Before each sub-hold, prefetch the ids'
   `mt_series` lines through the array pointer read with a relaxed atomic load (`mt_reserve` stores it with a
   release store when it reallocates, memtable.c:94-130; a prefetch of memory just freed is harmless, and the atomic
   keeps the read of the pointer itself out of a data race). Inside the sub-hold, read the flags, `top_nn` and
   `lsn[top_nn]` for the 128 ids and prefetch their row lines (and, for a series with `has_null`, its first and last
   timestamps), then read the rows: two passes, so the misses overlap. About 15 ns per id under the lock
   (U1).
5. Decide each id. Its sources, highest priority first: the active memtable, the flushing memtable, the table (a
   known entry below `drop_floor` counts as unknown, L20). Take the candidate with the greatest timestamp, the
   higher-priority source on equal timestamps (memtables rank above parts, as in queries; query.c:240-277), and
   check it against the higher-priority memtables for a null at its timestamp (§2.2). If it passes and lies below
   `end`, it is the answer: kind value, or none if its timestamp is below the cutoff (every other non-null row is
   older still). An empty entry and a memtable without a candidate add none; no candidate at all is none. The id
   **needs a search** instead when:
   - the entry is **unknown `u`** and no memtable candidate has `ts >= u` (with one that passes the shadow check, it is the
     answer: every non-null row in the parts is at or below `u`, and memtables win ties; the next flush settles the
     entry);
     the id also goes to the warmer's hint set (§3.8);
   - a memtable cannot give its candidate in O(1) (§2.2: `nn_stale`, `top_nn` in flight), or the best candidate is
     shadowed or may be;
   - the found row is at or above `end`; `WITH_NULLS` is set; the id is beyond the budget's cap.
6. Release `latest_rw`.

After the chunk's holds, if any id needs a search: read those ids' cutoffs, then take the chunk's **cut**
(`cut_take`, query.c:93: references to the parts, the memtables and `visible`, O(parts) under `db->lock`, no I/O),
outside `latest_rw`. The cutoff is read before the cut, as queries read it before their snapshot
(query.c:2024-2027), so a search answers what a query started at that moment answers (the verification's N8). Then
run `latest_at` (§5.1) for those ids on the cut, with the bound and flags, on the request's threads. A search uses
only the cut, never the hold's reads, so its answer is the state at one instant (the cut's), as a query's is. Send
the chunk's records; release the cut and the memtable references.

With `latest_cache = 0` (and on every NFS follower) there is no table: a chunk takes no `latest_rw` and only its cut.

A chunk's ids are thus answered at the instants of its holds, and its searched ids at the instant of its cut. Each
instant falls within the request (A4, A5); a search never mixes the hold's memtable reads with the cut's parts, so a
flush between the two cannot lose a row. Lookups never write the table.

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
- both on a **hot set** (1,000 series, every one looked up) and a **spread set** (10^6 series), each with a tenth of
  the series writing nulls;
- publish latency of a flush and of a one-row part (p50 1.56 ms today) while 32 connections send 100,000-id
  `WITH_NULLS` and as-of requests: p50 within 5%, p99 within 0.1 ms (one read hold).

If it fails, by cause:
- **Waits on `buf_lock`**: a paged `mt_series` array that never moves, and row storage freed only with its memtable,
  so lookups read the active memtable without `buf_lock`; or sub-holds of 32 ids.
- **Coherence misses on the hot set**: `mt_add` writes lines that lookups just read: the `mt_series` line, and
  `ts`, `v` and `lsn` at row `n`, which share a line with row `top_nn = n - 1` seven times in eight, up to four
  read-for-ownership misses per row. Removing the mutex does not remove them. Each memtable then keeps one compact
  `(ts, slot, lsn)` record of the newest non-null row per series, written by `mt_add` with the row, so a lookup reads
  one line and an append invalidates at most one line in a reader's cache. Reading a series while it is written
  cannot cost less.

### 2.4 Memory per series

| what | bytes | status |
|---|---|---|
| latest table, writer and cluster replica | 16 per series per copy, plus 1/8 (bitmap); 256 KiB page table per volume | this design, within `latest_cache` |
| latest table, NFS follower | none (§3.10) | this design |
| open build | nothing beyond the table (L11) | this design |
| a request's slow path | one cut per chunk that needs it (a pointer and a reference per part) | this design |
| a publish's fold list | 24 per distinct series of the publish, 16 more for a series with nulls in it, while it publishes (24 to 40 MB for a flush of 10^6 series; volumes that flush at once add up) | this design |
| the warmer's hint set | at most 65,536 local ids per volume (256 KiB) | this design |
| memtable `mt_series` | 40 per local id up to the highest id written, per memtable (the active one and at most one flushing, §2.2), arrays doubling (memtable.c:94) | today; `top_nn`, the type and the flags fit in its padding |
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

The rules below share one fold. A publish brings, for each series s with rows in a part of it (for several parts of
one commit, part by part in block order, so that the later part wins equal timestamps, insert.c:611):
- `R`: s's newest **non-null** row in the part, after last-write-wins within the part (a part holds one row per
  `(series, ts)`); none if every row of s in the part is a null;
- `N`: the range `[null_lo, null_hi]` of the timestamps at which s's row in the part is a null; none if there is
  none. Every timestamp the part nulls lies in it; a timestamp in it need not be null (the range is conservative).

Every series of the part is in the publish's list, the all-null ones included (the verification's N2b). The entry
becomes, with a known entry below `drop_floor` read as unknown `(k.ts)` (L20):

| entry before | after |
|---|---|
| empty | `R`, or empty if there is no `R` |
| known `k` | `R` if `R.ts >= k.ts` (the publish has a higher block than every listed part, so it wins ties); else, if `k.ts` lies in `N`, **unknown `(k.ts)`** (the null may have overwritten `k`; every other non-null row is below `k.ts` or at it); else `k` |
| unknown `u` | `R` if `R.ts >= u` (every non-null row is at or below `u`, and `R` wins a tie), else unknown `u` |

A null at a timestamp above `k.ts` leaves `k` (the user's rule). A null at `k.ts` makes the entry unknown, never
known at an older row: the older non-null row is not known without a read. The bound is `k.ts`, not `k.ts - 1`,
because `N` is a range: if `k.ts` was not nulled, `k` is still live. On the writer every publish has higher blocks
than every listed part (publish_lock, `parts_publish`), which is what makes the ties come out right without blocks in
the entries. A writer that makes an entry unknown pushes its id to the warmer's hint set after its write section
(§3.8).

### 3.1 Small inserts (the WAL and memtable path)

Nothing but `mt_add`'s fields (§2.2). A row appended to the active memtable is found by a lookup exactly when
`mt_collect` would show it to a query: the same `visible` bound. **Sync mode** (`wal_sync_ms = 0`): a row is
returned only once the WAL is synced up to it. **Periodic sync**: visible at once, as for queries. **Cluster**:
visible at the commit point (cluster C7).

A small write waiting on a large write's fence (`lw_fence`, internal.h:1490-1514) has appended nothing, so no
lookup can see it before the large write is listed: the fence order holds for lookups as it holds for queries.

### 3.2 Flushes

`flush_waiting` (memtable.c:777) commits a memtable's parts, then lists them and removes the memtable from
`flushing` in one `db->lock` section (memtable.c:801-810).

- Before taking the lock, `mt_write`, which already sorts each series and keeps the last write per timestamp,
  hands the publish a list of `(id, R, N)` per series, in id order. That list does not depend on the table.
- After `parts_commit` returned 0, the publisher takes `latest_rw` for writing, folds the list (the table of §3),
  lists the parts and removes the memtable from `flushing`, bumps `lgen`, and releases it.

A lookup sees either the memtable in `flushing` (and the old entry) or the listed parts and the new entry, never a
mix.

**Hold time, and the window for large flushes.** A flush holds `buffer_rows` rows (1 M by default, db.c:48); at
10^8 series written once a second that is about a million distinct series, stored sparsely in the table: about a
million cache misses, 5 to 15 ms of write hold about once a second per volume (U2), during which that volume's
lookups wait (queries do not). That estimate is above 2 ms, so step 2 builds the following window from the start
(not as a contingency), used by flushes (and only flushes) whose fold list is longer than a threshold (about
50,000 series, set from the measurement of lookup p99 under steady flushes of 10^6 series):

1. Under the write lock: set the volume's `publish_pending`, bump `lgen`, release it.
2. Without the lock, with relaxed 8-byte atomic loads and stores (lookups then read entries with relaxed atomic loads
   too: free on x86), make **only the changes that store the flush's own row**: empty to `R`, known `k` to `R` with
   `R.ts >= k.ts`, unknown `u` to `R` with `R.ts >= u`. Every change that makes an entry unknown (`k.ts` in `N`) is
   noted for step 3.
3. Under the write lock: make the noted entries unknown, list, remove the memtable from `flushing`, bump `lgen`,
   clear `publish_pending`. After the release, push the noted ids to the hint set.

This is exact because every store of step 2 writes `R`, which is the flushing memtable's own newest non-null row of
s, and every lookup in the window reads that memtable, which ranks above the table on equal timestamps. A torn read
(the new timestamp with the old slot, the old timestamp with the new one, or the bit and the fields out of step)
always shows a timestamp at or below `R.ts`, so the memtable's candidate at `R.ts` wins; and if a null in the active
memtable shadows `R.ts`, the lookup searches the cut, which holds either the memtable or the listed parts, never a
mix. Entries step 2 does not store are unchanged until step 3: a lookup reads the old entry together with the
flushing memtable, which is the state before the flush, and a null of the memtable at the old row's timestamp
shadows the entry through §2.2's check. Making an entry unknown in step 2 instead would let a torn read show a
bound as a known row with the old slot, or (with a conservative `N`) answer a bound as a value (the verification's
N4); deferring it to the write lock closes that.

While `publish_pending` is set, the other writers (retention, drops, a merge listed across a retention change, the
warmer) release the write lock and wait on a condition variable until it clears; none of them runs on a scheduler
helper. Merges that list without a retention change do not take `latest_rw` at all (§3.5). Large inserts, deletes
and replaces keep their stores inside the write section, since no memtable holds their rows.

### 3.3 Large inserts and the WAL replay

`insert_rows` (insert.c:640) writes parts, then `parts_publish` (insert.c:626) commits and lists them under
`publish_lock`. The part writer's statistics pass already tests each row for null (the null count); it also keeps,
per run, the last non-null row and the first and last null timestamps, which are `R` and `N` (exact bounds: a run is
sorted and holds one row per timestamp). A large unsorted insert is written in chunks whose later part wins
duplicates (insert.c:611), so the parts of one commit are folded part by part in block order: a later part's null at
an earlier part's `R.ts` makes the entry unknown, and a later part's row wins an equal timestamp. The fold runs at
the same place as a flush's (after `parts_commit`, in the write section that ends after `db_add_parts`, db.c:149). The
replay at open publishes through `parts_publish` too (memtable.c:1209) and folds the same way.

The fence is untouched: the large write's parts are listed and folded in one write section, then `lw_leave` lets
the small writes behind it in. A small write that waited therefore lands in a memtable after the fold; a lookup
ranks it above the large write's row at an equal timestamp, which is last-write-wins in real time (jepsen check 5).

### 3.4 Deletes and replaces

delete.c:150-170 commits tombstone parts (and, for a replace, the rows' parts) and lists them under `publish_lock`.
A delete writes a tombstone in every partition whose listed parts hold rows of s in the range (delete.c:47-60), so
every listed row of s in the requested ranges `D` is hidden, whatever its partition key. In the write section of
that listing, per series s of the commit, with `R` and `N` from the commit's rows (none for a delete):

| entry before | rule | why |
|---|---|---|
| empty | the fold with `R`, `N` | nothing older to hide |
| known `k`, `k.ts` in no range of `D` | the fold with `R`, `N` | the tombstones did not touch the newest non-null row |
| known `k`, `k.ts` in `[a, b)` of `D` | unknown `(a - 1)`, then the fold with `R`; if `a = INT64_MIN`, empty, then the fold | every row at or above `a` in a lower block is hidden, and `k` was the newest non-null row, so every non-null row is below `a` |
| unknown `u` | the fold with `R`, `N` | tombstones only remove rows, so the bound still holds |

Tombstones hide only rows of lower blocks (design.md, "Tombstones live in parts"), and every listed part has a
lower block than the commit, so the rule never asks about blocks. Memtable rows are never hidden; a delete flushes
the memtable before it writes its tombstones, so any row a lookup still finds in a memtable is newer than the delete.
An entry the delete made unknown goes to the hint set after the write section; the warmer settles it one batch later
(§3.8). The delete runs no search of its own.

### 3.5 Retention

Lookups clamp to `eng_cutoff(s)` at read time (L8), as `eng_query_raw_ex` does (query.c:2024). An entry holds the
newest non-null row in parts whatever its age (the warmer and the build search with no cutoff, so **empty means "no
non-null row in parts"**, not "none live"). If the entry's row is below the cutoff, every non-null row of the series
is, and the answer is none.

The cutoff only rises (`eng_now_ms` never goes back), except when `tsdb_set_retention` gives a series a longer
limit (it is per series, tsdb.h:280). Rows below the old cutoff may already be gone (merged away) while the entry
names one, so:

- `tsdb_set_retention` writes and syncs its RETENTION record under `ret_lock` as today, then takes `latest_rw` for
  writing and then `db->lock`, and in that one section applies the limit in memory (`ret_put_locked`, which bumps
  `ret_gen`, retention.c:45) and, if the cutoff went down, turns a known entry with `ts` below the old cutoff into
  unknown `(ts)`; it bumps `lgen`, and after the section pushes the id to the hint set. No lookup can see the new
  limit with the old entry. One series: O(1). Lock order: `ret_lock` > `latest_rw` > `db->lock`.
- Merges read the limit as they go (merge.c:241, 271, 334). A merge that started before a change can drop rows the
  new limit keeps. So a merge records `ret_gen` when it starts and compares it where it lists, under `db->lock`
  (the merge loop, merge.c:631-633, and `eng_optimize`, merge.c:711-715, before `merge_commit_locked`).
  `tsdb_set_retention` bumps `ret_gen` under `db->lock`, so the compare cannot race it (the simplification review's
  S5).
  - **Equal** (the common case): it lists as today, with no `latest_rw`. Its output holds the same last-write-wins
    values as its sources, less rows that were expired under the limit in force, which lookups clamp (L8).
  - **Different** (rare: a retention change during the merge): it releases `db->lock`, takes `latest_rw` for writing
    (waiting while `publish_pending` is set), takes `db->lock` again, and checks that its sources are still listed
    and not dropped (`p->dropped`, db.c:986, already makes a merge discard its result). Then, for every series with
    a run in its sources, a known entry with `ts` in the merge's time range becomes unknown `(ts)`; it bumps `lgen`
    and lists. After the section it pushes the ids it changed. O(series of the merge).
  Merges take no `publish_lock` and need none. The merge thread is not a scheduler helper.

Why that is enough. If the merge lists first, the rows it dropped were expired under the old limit, and the
lengthening then marks every entry below the old cutoff. If the change comes first, the merge sees it at its compare.
A warmer batch whose cut straddles either is refused, since both bump `lgen`.

The merge thread's partition drops remove only expired rows (selection and retirement in one `db->lock` hold,
merge.c:533-579, which reads the limits there, so it lies wholly before or after a change): no change to the table.

### 3.6 `tsdb_drop_before`, class switches, merges

- **`tsdb_drop_before(t)`** retires every partition that ends at or before `t` (db.c:979-992, today under
  `db->lock` only). It takes `latest_rw` for writing, then `db->lock`, and in that one section retires the parts,
  sets **`drop_floor = max(drop_floor, t)`** and bumps `lgen`. O(1) besides the retiring; no scan of the table under
  the lock (the performance review's L1). Lock order: `latest_rw` > `db->lock`, as for lookups. It then wakes the
  warmer for a walk of the entries below the floor (§3.8).

  A known entry with `ts < drop_floor` reads as unknown `(ts)` to lookups, to folds and to the warmer, always. That
  is exact: a drop retires only partitions that end at or before `t` (db.c:984-988), so an entry at or above `t` keeps
  its row; below the floor, the entry is still an upper bound of every non-null row, because drops only remove rows,
  and later publishes fold into it by the unknown rule. Nothing is swept, and no writer ever sweeps.

  The cost: an entry below the floor stays unknown until a publish of a non-null row at or above its timestamp. The
  warmer's walk stores empty for series with no non-null row left (the usual case after a drop: the series' rows
  were all in the dropped partitions). A series with surviving rows below `t` (in a partition of a long class that
  ends after `t`, or a backfill below `t` after the drop) keeps being searched until it is written again; that
  series has not been written since `t`, which a drop usually leaves well behind. The metric
  `tsdb_latest_search_total{why="floor"}` counts them (decision D3).
- Class switches move no row (AGENTS.md, "Partition classes"): no change. One (series, ts) is never in two keys,
  so ties between parts of different keys cannot happen.
- Merges (merge.c:463) list their output and retire the sources with the same last-write-wins values: no change
  (L8), apart from §3.5's retention case.

Retention limits are per series (`tsdb_set_retention(db, series, limit)`, tsdb.h:280), so a lengthening marks one
entry; a client that lengthens the limit of a million series sends a million calls, each O(1) under the lock.

### 3.7 Fail-stop

The table changes only after `parts_commit` succeeded (the commit point) and only in the section of a listing, so a
publish that fails (and the engine with it) leaves the table as the parts list: as queries see it. After
`eng_fail` no new file is named and nothing is listed, so publishes stop changing it; memtable rows stay invisible
above `wal_durable` in sync mode. Lookups answer what queries answer on a failed engine, until tsdbd exits. The
warmer stops at `eng_check`.

### 3.8 Unknown entries and the warmer

**The fill** of an entry is the newest non-null row of s **in the parts only**, with **no cutoff** (§3.5) and no
end bound: `latest_at` (§5.1) in its parts-only form, with §5.2's rule across the parts (a candidate at `T` counts
only if no part of a higher block in its partition has a null at `T`). Memtable rows are never candidates in it: a
memtable row newer than every part would stop the search before it visits a partition (the verification's V8).

**Who fills: the warmer, and only the warmer.** One thread per `tsdb` handle serves the volumes in turn; it sleeps
until there is work. A volume's work comes from:
- its **hint set**: local ids pushed by lookups that searched an unknown entry (§2.3 step 5), and by every writer
  that made an entry unknown (the fold's null rule, deletes, retention lengthening, a merge across a retention
  change). A set of at most 65,536 ids under a leaf mutex; a push to a full set sets the volume's `rescan` flag
  instead;
- a **walk** of the unknown bitmap, after an open's build and after a `rescan`; a walk of the known entries below the
  floor, after a drop (§3.6). A walk reads the table page by page in read holds of one page.

**A batch** of up to `B` ids of one volume:
1. Take `latest_rw` for reading; if `publish_pending` is set, release it and wait for the window to close. Read
   `g = lgen`, take the cut (`cut_take`, under `db->lock`, inside the read hold, so the cut holds every change up to
   `g`), and keep the batch's ids whose entries still need a fill. Release it.
2. Run the fills as one fork/join group on the query scheduler, capped at `request_threads` like any request, so
   queries share the helpers with it. No lock of the table is held.
3. Take `latest_rw` for writing. If `lgen == g` and `publish_pending` is clear, store each result into its entry if
   the entry still reads as it did in step 1 (still unknown; for a drop walk, still the same known row below the
   floor, and then only an empty result is stored). Bump `lgen`. Release. Otherwise store nothing (the batch is
   **refused**) and retry it.

`B` adapts: the batch aims at about 0.5 ms from its cut to its store, starts at 64 ids, doubles after a stored batch
that took less, and halves after a refusal, down to one id. On a cold cache over NFS the time, not the count, is
fixed (32 parallel reads of about 100 µs is about 64 ids per 0.2 ms).

**Why this is exact.** A fill depends only on the parts list and the retention limits of its cut. Every change to the
list or the limits that can change a series' fill bumps `lgen` in a write section of `latest_rw`: every publish
(flush, large insert, replay, delete, replace), a retention change, a drop, a merge listed across a retention change.
What changes the list without a write section changes no fill's answer for a lookup: a plain merge keeps every live
last-write-wins value and drops only expired rows (which lookups clamp, and which a later lengthening marks) and rows
hidden or overwritten (which are never the newest non-null row); a TTL partition drop removes only expired rows. So a
batch stored with `lgen == g` stores what a fill on a cut taken at that moment would find. Nothing else writes the
table between, since every writer bumps `lgen`. A stored result can only be a state the entry's promise allows; the
next publish folds into it by §3's rules.

**Liveness.** A batch is refused if any write section runs inside it: with publishes as a Poisson stream at rate `r`
and a batch of duration `T`, with probability `1 - exp(-r T)`. At `T` = 0.5 ms that is 0.05% at 1 publish per second,
5% at 100/s and 39% at 1,000/s (insert-bench tops out at 557 one-row parts/s per process); halving `B` at each
refusal brings a batch to one id, about 10 to 20 µs, refused 1 to 2% of the time at 1,000/s. Answers are exact
meanwhile, from the slow path. Live series do not depend on it: their next flush settles them. A per-entry
generation (the design of the earlier rounds) would tolerate higher rates; if `{result="refused"}` ever dominates, it
can come back as a liveness aid for the warmer alone.

**Steady state.** An unknown entry of a live series is answered by its memtable row at or above the bound, with no
search, until its flush settles it. An unknown entry of a dead series (its newest non-null row deleted, nulled, or
below a lengthened limit) is settled one batch, about a millisecond, after its writer pushes the id. Hot ids met by
lookups after an open jump ahead through the hint set. One write section per batch, on the warmer's own thread, only
while there is work; no scheduler helper ever waits for the write lock. `tsdb_latest_unknown` (per volume) says how
many entries remain, `tsdb_latest_fills_total{result="stored"|"refused"}` how the batches fared.

Rejected: no warmer at all, only publishes settling entries. Dead series would then stay slow for ever: after an
open with a quarter of 10^7 series of type state, 2.5 M entries are unknown and the dead ones never publish again; a
dashboard of 100,000 ids with 10% dead series would cost 10,000 searches per request, 0.05 to 10 s of CPU (each 5 to
20 µs, up to 1 ms for a series whose newest row is far back).

### 3.9 Open, crash recovery

At open, after `load_parts` and before the WAL replay (db.c:646), `cls_check_parts` (classes.c:1095) checks every
granule of every run of every part, in one loop today. This design splits that loop into **blocks of 32,768 series
ids** that the open's threads (as many as `query_threads`) take from a shared counter. Before the walk the volume
reserves its pages from the budget, in id order up to its highest local id, stopping at the first page the budget
refuses (L5); blocks beyond that are walked for the checks and build nothing. For its block a thread visits the parts
in `db->parts` order and, in each, the runs of the block's ids (found with `gix_run`, one binary search per part and
block) and their tombstones (a binary search in the part's trailer, sorted by `(series, start)`,
internal.h:597-600). The checks are per run and read-only (`check_granule` reads the part; `cls_at` takes `cls_mu`;
each thread has its own `codec_ctx`), so they split cleanly; the first failure any thread finds fails the open (which
part the log line names may differ from today's serial order). The block's 512 KiB of table stays in the thread's
cache across all partitions, and blocks are disjoint, so the build uses plain stores.

`db->parts` is in (partition, min_block) order (manifest.c:283): within one partition key a part visited later has
higher blocks, and across keys a series never has two rows at one timestamp. But keys overlap in time (partition
classes, a switch inside a day), so a series' rows are not visited in time order. Per series of the block:

1. Per run (series s in part p): the run's last granule gives `ts_max` (the check already reads it). If the entry is
   empty, or noted with `ts_max >= ts`, or unknown with `ts_max >= u`, the entry becomes `(ts_max, p, granule)`: the
   part and granule indexes held for now in `x`. Nothing else is read. The entry's timestamp (or bound) is then
   always the greatest timestamp of every row of s visited so far, null or not.
2. Per tombstone range `[a, b)` of s in p, before p's runs fold: if the entry is noted, came from a part of p's
   partition visited before p (a lower block), and its `ts` lies in the range, it becomes **unknown `(k.ts)`**,
   not `(a - 1)` as on the writer (§3.4). Rows of another key visited earlier may lie between `a` and `k.ts` and be
   live: a switch from class X to Y inside the range, a delete over both keys (each key's tombstone is clamped to
   its partition, delete.c:36), and an older row inserted into X afterwards; X sorts first, so Y's tombstone meets
   a noted row from Y while X's live row sits below it (the verification's V6). `k.ts` bounds every row seen so
   far, and later runs fold by step 1. Unknown entries keep their bound.
3. At the end of the block, one pass over its noted entries: read the noted granule's `GF_LAST`, its
   `GT_LAST_NULL` flag and its null count, and decide:
   - the last row is non-null and its value exact: known `(ts, slot)`. The noted row is the last write at `ts`: a
     part holds one row per timestamp, and a part of the same key with a higher block and a row at `ts` would have
     replaced the note in step 1;
   - the last row is a null (`GT_LAST_NULL` on a type with time-weighted statistics): unknown `(ts - 1)`, or empty if
     `ts = INT64_MIN` (no non-null row can be at `ts`, and every row seen is at or below it);
   - a state series (no time-weighted statistics, both null flags always set, twa.c:129): unknown `(ts - 1)` if the
     granule's null count equals its row count, else unknown `(ts)`;
   - the value is not exact (int64 at or beyond 2^53, NaN, L12): unknown `(ts)`.

No transient array: the table holds the walk's state. The WAL replay then publishes its rows through
`parts_publish`, which folds them (§3.3). A crash leaves nothing to repair: the table is memory only, rebuilt from
parts that are themselves crash-consistent (intent files).

Cost (U3). At 10^8 series and 365 partitions there are about 3.65 x 10^10 runs; at 10 to 30 ns per run the serial
walk of today takes 6 to 18 minutes, and a build that touched a table line per run in that walk would move 1.2 TB
(dense parts) to 4.7 TB (sparse parts: series of classes 1 to 3, one line per run) through memory, 1 to 3 minutes
more on one core (the verification's P5). Split by blocks, the table is read and written about once per open
(its slice stays in cache), the index is read as before, and both the checks and the build run on every thread:
about 11 to 34 s of walk on 32 threads with a warm page cache, I/O-bound on a cold one. Step 1 adds a compare and a
store in cache per run, step 3 three field reads per series. Volumes open one after another today (volumes.c:1054);
opening them in parallel is a separate change and not assumed here. The benchmark reports absolute seconds, with
dense and sparse parts, build on and off.

Unknown entries after the open: every state series, int64 series with huge last values, NaN last values, series
whose last row is a null (an offline sensor that reports nulls), and series whose newest rows were deleted but not
yet merged. Those whose bound is their newest timestamp or one below it are **settled by the next flush or insert of
a non-null row** (§3, the fold); series that are no longer written, or that write only nulls, need the warmer. At
about 10 to 20 µs per fill with a warm page cache (U4), 2.5 million unknown series (a quarter of 10^7 series being
state series) take 1 to 2 s on 32 threads. A series whose last non-null row lies far back walks back over granules
that are wholly null from the index alone (§5.2). On a cold cache over NFS each fill needs at least one random read of
a data granule: at 20,000 to 100,000 reads per second that is 25 to 125 s (U5). Answers are exact meanwhile, from the
slow path. The format bump of L18 removes all of these but the deleted ones.

### 3.10 NFS followers

A follower has no latest table (L13): `--follow`, and an embedded open as a follower, force `latest_cache = 0`, and
a log line says so if the option asked for a budget. Every id is answered by `latest_at` on the chunk's cut (§5.1),
whose sources are the WAL tails (newest segment first, visible always, as `snapshot_take` reads them with no bound,
query.c:240) and the parts, with §5.2's rule across them. That rule ranks parts by block within a partition, so the
sequence that defeated the incremental follower table (the verification's N3: a merge output `M` holding `(T, 5.0)`
arrives after a part with a higher block that holds a null at `T`) is answered right by construction: `M`'s candidate
at `T` is shadowed.

Cost: about 5 to 20 µs per id warm (U4), against 0.1 to 0.2 µs from a table: 50 to 100 times more. 100,000 ids then
cost 0.5 to 2 s of CPU, 16 to 60 ms on 32 threads: about what QUERY_MULTI (values, descending, limit 1) costs today,
less where the old path enumerated every part (L17). `tsdb_latest_series_total{path="search"}` counts them. A follower
saves 16 bytes per series. A table for followers is a later step (§11).

## 4. Cost on the insert path

| path | per row | per batch or part | lock |
|---|---|---|---|
| small insert (memtable) | a null test, one compare, sometimes a u32 store and a flag, on the `mt_series` line `mt_add` writes anyway | none | `buf_lock`, already held; lookups take it in 2 µs sub-holds (§2.3) |
| flush | none | `(R, N)` per series from `mt_write`'s sorted runs; one compare and store per series | `latest_rw` for writing, for the fold and the listing (§3.2; a window for large flushes) |
| large insert | none beyond the statistics pass (the part writer keeps `R` and `N` per run) | as a flush, per series of the batch | `latest_rw` for writing, with the listing |
| delete, replace | none | the §3.4 rule per series of the commit | same |
| merge | none | none, unless a retention limit changed during it (§3.5) | none; `latest_rw` for writing only in that rare case |

Writer threads do not contend more than today: memtable appends are serialised by `buf_lock` already, publishes by
`publish_lock`. What is new:
- a publish waits for the lookups holding the read lock: at most one read hold, 256 ids, about 25 to 50 µs, with no
  I/O and no snapshot in it (§2.3), so a flush that waits holding `publish_lock` and `cls_rw` holds them that much
  longer, and a one-row part (p50 1.56 ms) about 3% longer at worst;
- inserts share `buf_lock` with lookups in 2 µs holds;
- lookups wait for a publish's write section (§3.2) and for the warmer's (one per batch, while it has work).
The benchmark gate of step 2 (§2.3, §10) measures all three against inserts with no lookups at all.

## 5. Queries beyond "the newest"

### 5.1 `latest_at`, the slow path (as of a time, `WITH_NULLS`, unknown entries, fills)

`end` below the timestamp a lookup found means the answer lies below; the table cannot help. The id goes to
`latest_at(cut, s, end, flags)`, which also serves `WITH_NULLS`, unknown entries, ids the memtable layer cannot answer
in O(1), followers, `latest_cache = 0`, and the warmer's fills. It always runs on a cut, with no lock of the table
held (§2.3, §3.8). Without `WITH_NULLS` it finds the newest non-null row (§5.2); with it, the newest row. In its
**parts-only form** (the warmer's fill: no memtable sources, no end bound, no cutoff) its result is a fill; in every
other form it is only an answer.

- **Memtable sources** (active, flushing, a follower's tails), unless parts-only: the newest row of s below `end`
  with `lsn <=` the cut's `visible`, the later row on equal timestamps, non-null unless `WITH_NULLS`. A sorted run: a
  binary search for `end`, then back over rows above `visible` and, without `WITH_NULLS`, over rows a later write at
  their timestamp nulled. The active memtable is read in pieces of at most 4,096 rows, one `buf_lock` hold per piece
  (rows are only appended, and `visible` is fixed), and an unsorted series is copied that way and sorted outside the
  lock; `mt_collect` would sort, and allocate, under `buf_lock` (memtable.c:219-270). The flushing memtable and the
  tails are immutable.
- **Parts, newest partitions first**: the cut's distinct partitions, ordered by their end (`partition_range`; one
  sort per cut, shared by every id of the chunk). Unless parts-only, a binary search skips every partition that ends
  at or before the series' cutoff (the simplification review's S4: an expired series costs O(log partitions), not a
  walk). For each remaining partition in turn, skipping those that start at or after `end`: for each part, compare
  s with the part's first and last series id (O(1)) before a binary search in its run list; compute s's snapshot
  windows in that partition's parts only (`snap_find`, with the partition's tombstones applied as `snapshot_take`
  does, so granules a tombstone touched are read from their mem runs). A part's tombstones of s are found by a binary
  search on series in its trailer (sorted by `(series, start)`, internal.h:597-600), not by the scan of every
  tombstone that `snapshot_prepare` does today (query.c:440-446); a merged tombstone part can hold 10^5 of them
  (merge.c:256-274), and this step changes `snapshot_prepare` the same way. Per source, take the last granule with
  `ts_min < end`: if its `ts_max < end`, it is a plain granule (not moved to a mem run, not a state series, not an
  int64 beyond 2^53 or a NaN, L12) and its last row is the candidate the search wants (non-null, or `WITH_NULLS`),
  its candidate is `(ts_max, GF_LAST)` from the index without decoding; otherwise decode that one granule and take
  its last suitable row below `end` (§5.2 for nulls).
- **Stop** as soon as the best accepted candidate's timestamp is at or above the next partition's end: no row of an
  older partition can be newer, and no timestamp is in two keys. In the parts-only form the candidates are the parts'
  only, so a memtable row never stops a fill early. A memtable candidate newer than the newest partition's end stops
  the search before any part: that is why a search of a series in flight costs about a microsecond (§2.2).
- The candidate with the greatest timestamp wins, the higher source priority (`mem_prio`) on ties.

A series usually needs one partition (up to four keys overlap in time with partition classes). Cost (U4): about 5 to
20 µs with one granule decoded, independent of the number of parts in the volume, to be measured at 10, 100 and
1,000 partitions, with a partition of 10^5 tombstones and an unmerged hot partition of about 30 parts, and for a
series with no row and a series wholly expired. A series whose newest non-null row is far back walks the partitions
in between at one O(1) check per part, plus a binary search where the part's id range holds it; a fill of such a
series then answers from the table. It replaces raw_desc for `limit = 1` (L17); raw_desc keeps its windows for larger
limits. Inside one `QUERY_LATEST` the cuts come one per chunk (§2.3), not one per series.

### 5.2 Nulls (the default rule)

Without `WITH_NULLS`, `latest_at` looks for the newest timestamp whose **last-write-wins** value is not null. Within
each source, equal timestamps first resolve to their last write: `mt_add` counts an equal timestamp as sorted
(memtable.c:145), so a sorted run can hold `[T: 5.0, T: null]`, whose value at `T` is the null. Per source,
`latest_at` then finds the newest non-null row below the bound, stepping over granules whose null count equals their
row count (from the index) and decoding the others. A source's candidate at `T` is accepted only if **no
higher-priority source has a row at `T`**: a newer source's null at `T` overwrote it (its non-null at `T` would itself
have been a candidate and won the tie). Priority is the memtables above the parts, newer memtables (and newer tail
segments) first, and among parts of one partition key the higher block; parts of different keys never share a
timestamp. The check reads the higher-priority sources' granules that span `T` (their `[ts_min, ts_max]`,
stepped-over null granules included) and decodes one only where `T` falls inside it and it holds nulls; memtable runs
are searched directly. A rejected candidate moves that source's bound below `T` and the search goes on. A series that
has been null for weeks walks back that far over the index's null counts (the cost of a descending query to its last
value). With `WITH_NULLS` no candidate needs the check: the newest row of the highest-priority source at the greatest
timestamp is the answer.

### 5.3 Statistics

`tsdb_last_query_stats` after a lookup counts the granules and rows the slow path decoded; two new counters say how
many series the table answered and how many were searched (also as metrics, §6.5).

## 6. Interfaces

### 6.1 C API (`include/tsdb.h`)

```c
typedef enum { TSDB_LATEST_VALUE = 0, TSDB_LATEST_NULL = 1, TSDB_LATEST_NONE = 2 } tsdb_latest_kind;

typedef struct tsdb_latest {
    int32_t status;   /* 0, TSDB_ERR_NOTFOUND (no such series) or TSDB_ERR_DENIED (tsdbd) */
    uint8_t kind;     /* tsdb_latest_kind (NULL only with with_nulls); NONE whenever status != 0 */
    uint8_t type;     /* the series' tsdb_type; 0 whenever status != 0 */
    int64_t ts;       /* with VALUE and NULL; 0 otherwise */
    union { float f32; double f64; int64_t i64; uint8_t b; uint32_t state; } v; /* with VALUE; 0 otherwise */
} tsdb_latest;

typedef struct tsdb_latest_opts {
    int64_t end;          /* only rows before it; TSDB_TS_MAX: no bound */
    const int64_t *ends;  /* per series instead of end, or NULL */
    int with_nulls;       /* nonzero: the newest row, which may be a null (the slow path) */
    int threads;          /* 0: tsdb_options.request_threads; more is clamped to it */
} tsdb_latest_opts;

/* out[i] for ids[i], in index order; n up to 1 << 20. opts NULL: no bound, the newest non-null value. Returns 0,
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
them by volume and runs each volume's chunks on the query scheduler. The CLI gets `tsdb DIR latest NAME...`
(`--with-nulls`, `--end`). `tsdb_abi_version` goes up (new struct and call used by the bindings).

### 6.2 Wire (protocol 14)

```
QUERY_LATEST (39)  u8 flags, u8 0, u16 threads, u32 n (1..1048576), i64 end, [i64 ends[n]], u64 ids[n]
                   flags: TSDB_LATEST_WITH_NULLS 1, TSDB_LATEST_PER_END 2 (ends[] present; `end` must be 0);
                   other bits reserved (TSDB_ERR_INVALID). The length is exactly that.
  -> frames, each: i32 status, u32 first, u32 m, u8 last, u8 0, u16 0, then m records of 24 bytes:
                   i32 status, u8 kind, u8 type, u16 0, i64 ts, u64 value (the series' slot zero-extended:
                   float32 bits in the low 4 bytes, bool 0/1, state code; 0 with NULL and NONE)
                   records in index order, at most 65,536 per frame; the frame with last = 1 ends the reply.
                   Kind NULL appears only with WITH_NULLS.
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

- C client: `tsdbc_latest(c, ids, n, opts, out)` with the structs of §6.1, and `tsdbc_latest_by_name(c, names, n,
  opts, out)`.
- Rust: `Client::latest(&[Sid], &LatestOpts) -> Result<Vec<Latest>>`, `Latest { status, kind, ts, value: Value }`,
  and `Client::latest_by_name(&[&str], &LatestOpts)`; the embedded `Db::latest` and `Db::latest_by_name` the same.
- Python (PyO3 over the crate): `latest(ids, end=None, ends=None, with_nulls=False)` and `latest_by_name(names, ...)`
  returning a list of `(ts, value)` tuples, `None` for NONE, a `NotFoundError` or `DeniedError` instance in place
  for a missing or denied series (or raising, by a flag); embedded `Db.latest` the same.
- Java: `long[]` ids (never `int`), `Latest[] latest(long[] ids, LatestOptions o)` and
  `Latest[] latestByName(String[] names, LatestOptions o)`; `EmbeddedDb` the same.
- **`latest_by_name`** resolves each name to an id through the client's id cache (the insert paths keep one), looks
  up the misses in one batch (the existing name lookup op), and sends ids. A name that does not resolve gets the
  lookup's answer in its own record: `TSDB_ERR_NOTFOUND`, or `TSDB_ERR_DENIED` for a name outside the user's grants
  (AGENTS.md, "Login and rights"). Names never travel in `QUERY_LATEST`; ids never change meaning, so the cache is
  never stale.
- The Pulsar sink does not use it.

Each client's tests compare its answers with the embedded binding's and with tsdbd's for the same data, nulls (with
and without `with_nulls`), denied ids and names and missing ones included.

### 6.5 Metrics (`docs/metrics.md`)

`tsdb_latest_requests_total`, `tsdb_latest_series_total{path="table"|"search"}`,
`tsdb_latest_search_total{why="unknown"|"inflight"|"stale"|"shadow"|"bound"|"with_nulls"|"cap"|"floor"|"follower"}`,
`tsdb_latest_unknown` (entries left to fill, per volume), `tsdb_latest_table_bytes`, `tsdb_latest_budget_capped`
(volumes whose pages the budget refused), `tsdb_latest_wait_seconds_total` (time lookups waited for a write section),
`tsdb_latest_fills_total{result="stored"|"refused"}`, and `tsdb_latest_future_series` (series whose cached newest
row is more than one hour ahead of the clock, per volume; D2). The warmer and the open's build count it as they walk,
and publishes keep it current from the entries they store, at the cost of one compare against a cached `now + 1 h`.

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
  `DROP` marker through §3.6 (the floor); `CLASS` changes nothing. Each replica runs its own warmer. So every in-sync
  replica's table, read at a commit point, answers what the leader's answers at that point.
- **Failover**: the new leader was an in-sync replica, so its table is already complete; nothing is rebuilt. A
  returning replica that cuts a divergent WAL tail (C14) never listed parts above the commit point, so its table
  holds nothing uncommitted; its memtables are cut with the WAL.
- **A new replica** (learner, cluster §8.1) opens its snapshot like any open (§3.9) and then applies the log.
- **Cold tier**: every replica keeps the index of cold partitions locally (cluster §9), so the open's build is
  unchanged; a fill or an as-of read that needs a cold granule's rows reads them from the cold tier (rare: only a
  series whose newest non-null rows are cold and were deleted or nulled, or an as-of bound in cold time).

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

What it loses: it is not exact (dual writes race last-write-wins; it never sees deletes, replaces, retention, a null
written over a value, or sync-mode visibility; a failure between the insert and the SET leaves it wrong); it goes
cold when it restarts; it is a second system to run and to fence at failover; it takes 60 to 100 bytes per key as
strings (20 to 30 in packed hashes) against 16 here; and it has its own authentication, apart from tsdb's grants.

The in-engine table wins exactness, memory, one hop and the grants. It loses read scale-out beyond the replication
factor, which §7.2's gate measures.

## 8. Tests

- **`tests/test_latest.c`, an oracle test.** Random sequences on a few hundred series of every type, ids above
  2^32 and in several volumes: small and large inserts (in and out of order, duplicate timestamps, keys duplicated
  across `SORT_CHUNK_ROWS` chunks of one insert, nulls, NaN with payloads, int64 near `INT64_MIN`/`INT64_MAX` and
  beyond 2^53, timestamp 0 and negative timestamps), never-written series, series that write only nulls, deletes of
  the newest rows and of other ranges (from `INT64_MIN` too), replaces with and without rows, retention set, raised
  and lowered with a test clock (also during a merge), `drop_before` (also with a class-3 partition that ends after
  `t`, and a backfill below the floor), class switches (partition classes on), flushes, merges and `tsdb_optimize`,
  close and reopen, reopen after a simulated crash (fork, kill), sync and periodic WAL modes, a budget that caps the
  table mid-volume. **Nulls over the cached row** (the user's rule): a null at the cached timestamp through the
  active memtable over a part's row, and over the flushing memtable's row (N1); a null at the cached timestamp in a
  commit with an older non-null row in the same commit (N2a); an all-null commit while the entry is unknown (N2b); a
  later chunk of one large insert nulling an earlier chunk's newest row; a null at a newer timestamp (the entry
  stays); a non-null row written again at a nulled timestamp; within one memtable, a null after the newest non-null
  row at its timestamp (`nn_stale`), then an older non-null row; a series whose last row in every part is a null,
  across a reopen (N7). With partition classes on: a switch inside a day, a delete across both keys, an older row
  inserted into the old key afterwards, a reopen, then a publish of a row between the tombstone's start and that row
  (§3.9, V6). After every step, for every series: `tsdb_latest` equals the last non-null row of `tsdb_query_values`
  ascending over everything, and with `WITH_NULLS` its last row (the oracle is a full scan, not the descending path),
  bit for bit; with random end bounds too. Also with `latest_cache = 0` (the slow path alone) and with
  `desc_window_rows` small. A check after each step that every entry keeps its promise (§2.1) against a parts-only
  scan.
- **Concurrency** (in the same file, run under `make tsan`): writer threads doing small and large inserts (with
  nulls) and deletes, lookup threads checking that each answer is one the history allows (its row was written by a
  non-null write that had begun; no acknowledged newer non-null row that no later write nulled or deleted is missed;
  a series' answers never go back in time without a delete or a null in between). Test hooks force the races: **a
  warmer batch with a publish, a drop, a retention change, or a merge listed across a lengthening between its cut and
  its store is refused** (the stored entry would be wrong without the refusal); a warmer batch whose cut was taken
  inside a §3.2 window; a lookup paused inside a window's stores, with a null of the flushing memtable at the cached
  timestamp (the invalidation deferred to step 3, N4); a publisher paused between its precomputed list and its write
  section; a merge running across a retention lengthening; a lookup paused between its entry read and its cutoff
  read while a lengthening runs; a drop between a lookup's entry read and its use; a publish below `t` after
  `drop_before(t)`; a search where a memtable row is newer than every part (it answers without visiting parts, and a
  fill must not stop early, §5.1); an in-flight `top_nn` in sync mode. A lookup thread sending `WITH_NULLS` and as-of
  requests while a flush waits: the flush's wait for the write lock stays below one read hold.
- **Followers** (no table, §3.10): a writer and a follower on one directory, lookups compared with the oracle after
  each refresh, merges running; the N3 sequence (a part with a null at `T` listed, then a merge output of lower blocks
  holding a value at `T`); a delete followed by a full merge of the partition between two refreshes; a retention drop
  of rows between two refreshes.
- **tsdbd** (`tests/test_server.c`, uring and epoll): the frame, chunks across frames, missing, malformed and
  denied ids (NOTFOUND and DENIED records with type 0), `PER_END`, `WITH_NULLS`, CANCEL, a 12 client against a 13
  server and the reverse.
- **Rights**: `test_auth_ops` row; denied and missing alike for a user without `read` on `all`, NOTFOUND with it;
  `latest_by_name` with names outside the grants.
- **Fuzzing**: QUERY_LATEST seeds in `tests/fuzz/frames/` and `make fuzz-frames FUZZ_SECONDS=600`.
- **Jepsen** (kv and classes workloads, every nemesis, `--auth` too): the kv workload today writes no nulls
  (tests/jepsen/jepsen.c); it learns to write some (a tenth of its writes, recorded as null writes in the history),
  and clients also send `QUERY_LATEST`. A latest answer `(t, id)` for series s is checked as two reads of the
  existing model: a read of key `t` that returned the non-null write `id` (checks 1 to 5: no phantom, no lost, no
  future, no stale value), and a range read `(t, end)` that found no non-null value (every acknowledged write above
  `t` was a null, or was overwritten by a null or deleted). An answer of none is the same range read over everything.
  With `WITH_NULLS`, a null answer at `t` is a read of key `t` that returned a null write.
- **Integration** (`tests/integration/run.py`): `latest` and `latest_by_name` in the matrix, every client, both
  backends, auth on and off.
- **Clients**: `make test-clients`, each comparing tsdbd, the embedded binding and the C API.

## 9. Benchmarks to run (quiet machine, five runs, the mean)

- **Insert cost**: `build/insert-bench` (1 to 10,000 rows per call through the memtable, 1 M rows straight to
  parts, 1 and 64 threads) with `latest_cache` on and off. Expected: within noise on the memtable path, below 2%
  on large inserts.
- **Inserts under lookups** (the gate of step 2, §2.3): insert rows/s and p99 at 1 and 64 writer threads, sync and
  periodic modes, hot set (1,000 series) and spread set (10^6), a tenth of the series writing nulls, while 32
  connections send 100,000-id lookups to the same volume, against the same inserts with no lookups; beside it the
  same under a QUERY_MULTI (values, descending, limit 1) load at equal ids/s. The publish latency of a flush and of a
  one-row part while 32 connections send 100,000-id `WITH_NULLS` and as-of requests. One volume's ids/s at 1 to 32
  connections while it takes 1 M rows/s (§7.2).
- **Lookups**: `build/tsdbc-bench latest --ids 1|4096|100000 --conns 1|32`, ids sorted and random (the sort's
  share counted in U1), against tsdbd on the 100 M-row data set (100 series) and on 10^6, 10^7 and 10^8 series per
  volume (scale-bench data); the same embedded (`build/tsdb-bench`, a latest class). With steady inserts running
  alongside, and with the slow path forced (`latest_cache` 0) to show what the table buys. Compared with what exists
  today: one id against a descending limit-1 query, 100,000 ids against QUERY_MULTI (values, descending, limit 1).
- **Flush holds**: lookup p99 under steady flushes of 10^6 series, stores inside the write section and in the §3.2
  window, which sets the window's threshold (U2).
- **Slow path**: as of a day back, `WITH_NULLS`, the default over series null for a day, fills, a series with no row
  and a series wholly expired, at 10, 100 and 1,000 partitions per volume, a partition with 10^5 tombstones, and an
  unmerged hot partition of about 30 parts (U4).
- **Warmer**: refused batches at 1, 100 and 1,000 publishes per second (U7).
- **Open and warm-up**: open time in absolute seconds with the build on and off, at 10^7 and 10^8 series x 30 and
  365 partitions, dense and sparse parts (series of classes 1 to 3), page cache warm and cold (U3); the time until
  `tsdb_latest_unknown` reaches 0 with a quarter of 10^7 series of type state and a tenth ending in nulls, written and
  not written after the open, warm and cold (U5).
- **Followers**: follower lookups per second and p99 at 1 and 32 connections, against the writer's (§3.10).
- **Memory**: resident bytes per series at 10^7: the table, memtables, rights maps, the open's peak.

`tools/bench-tsdb.sh` gets the latest classes, and AGENTS.md's benchmark section the numbers.

### 9.1 Measured at step 2: insert cost with the table (2026-11-07)

`build/insert-bench --latest-cache BYTES` (0 = no table), the machine otherwise idle (each run under the BENCH lock,
`--seconds 3`), 1,000 series of the mixed sensors, ext4 on NVMe. Table on is the default (RAM/16).

Large batches and the memtable path, 3 interleaved runs each (rows/s, no lookups running): parts 1 M rows
52 / 53 / 52 M on against 52 / 43 / 53 M off; parts 10 M rows 57 / 55 / 59 M on against 57 / 55 / 47 M off; WAL and
memtable at 10,000 rows per call 9.50 / 9.43 / 9.55 M on against 9.51 / 9.35 / 9.57 M off. The runs are within each
other's noise (the lowest "off" values are runs that followed a 10 M insert's disk writeback).

Durable one-row appends (the fsync-bound rows), order off, on, on, off, four times, 20 s apart; inserts/s, mean of 8
runs per mode (one outlier run of each mode, at 586 and 817 per second with one thread, is disk noise: the median
and the p50 and p99 latencies are the same):

| appenders | table off | table on |
|---:|---:|---:|
| 1 | 1,344 (median 1,467) | 1,367 (median 1,452) |
| 16 | 11,388 | 12,277 |
| 64 | 41,308 | 42,835 |
| 256 | 143,518 | 142,833 |

So at **1,000 series and no nulls** the table costs nothing this benchmark can measure on the insert path. That is the
only series count measured here, and the one where the fold has almost nothing to do (the fold list is short and
`lf_granule`'s null passes find no nulls): the review measured a flush of 10^6 series at +8% (about 48 ms) against no
table, so §2.3's "no measurable cost" is NOT shown at scale; §9.2 has the flush measurement. Not yet measured: inserts
under lookups through the server (step 3) and the other items above.

### 9.2 Measured at step 2: a flush of 10^6 series, with and without the table and its window (2026-11-07)

`build/latest-bench` (tools/latest-bench.c, embedded, not in `make`), each run under the exclusive BENCH lock, the machine
otherwise idle, ext4 on NVMe. Every round writes one row for each of 10^6 series (a tenth of the rows null), then
`tsdb_flush`es while readers look up random ids and one writer thread appends one row per call. Five repetitions x
modes interleaved (on = default table with the §3.2 window, nowin = table without the window, off = `latest_cache` 0);
round 1 of each run is a warm-up and left out, so 25 rounds per mode. A is the table path (1 reader, 256 ids per call),
B the slow path (`WITH_NULLS`, 2 readers x 4,096 ids, 3 repetitions, 15 rounds, table on or off).

| | flush, mean (sd) / median | lookups completed during the flush | lookup p50 / p99 | max lookup / max append |
|---|---:|---:|---:|---:|
| A, table on (window) | 768 (30) / 761 ms | 20,059 | 35 / 58 us | 16.6 / 16.5 ms |
| A, table on, no window | 775 (62) / 763 ms | 20,455 | 34 / 55 us | 16.7 / 28.2 ms |
| A, table off (slow path) | 745 (47) / 730 ms | 4,826 | 147 / 206 us | 16.6 / 16.4 ms |
| B, table on (`WITH_NULLS`) | 759 (65) / 750 ms | 364 | 4.1 / 9.1 ms | 20.5 / 38.0 ms |
| B, table off | 713 (23) / 723 ms | 344 | 4.0 / 9.0 ms | 21.0 / 14.9 ms |

- **Flush cost of the table**: +3 to +4% on the medians (about 30 ms for 10^6 series, about 30 ns per series for the
  fold). That is above the 2% this design expected for large inserts and is what a flush of 10^6 one-row series pays;
  a flush with fewer series per row (the insert-bench data) pays nothing visible (§9.1). The run-to-run sd of one mode
  is 4 to 8%, so only the medians of 25 rounds say this. The window itself changes neither the flush time nor the lookup
  p99 (A on against nowin). The slow path (B) is as slow with the table as without: its lookups skip the table.
- **Lookups during a flush**: the table answers 4x as many lookups with a p50 of 35 us against 147 us.
- **The multi-ms worst case is not the table.** Max lookup and max append are the same ~16 ms in every mode, table off
  included, and the max append equals the max lookup in the same round: one pause that both threads were caught in.
  `strace -f -T` showed the flusher in `munmap` of buffers of 184 MB and 168 MB for 7 to 8 ms, and that was taken for
  the cause. It was one cause of two (found by the `flush-tail` track, 2026-11-07, `perf record -e context-switches -g`
  and a poll of `/proc/PID/task/*/wchan`: the readers and the appender sit in `buf_lock`, and the holder is the
  appender itself): (1) right after a flush swaps in a fresh memtable the first row with a high series id grows the
  series array (`mt_reserve`) to id+1 entries, 40 MB at 10^6 series, and the `realloc` plus the `memset` of the new
  tail touched every page under `buf_lock`: about 9 ms, twice (a doubling), and every append and lookup waited. The
  array is now an anonymous mapping from 1 MB on (zero pages, grown by `mremap`, touched as series arrive:
  `mt_s_grow`). (2) the flusher's big buffers went back through `munmap`, which holds the mmap lock for writing for
  about 40 us per MB; they are now zapped in 2 MB slices of `madvise(MADV_DONTNEED)` first (`tsdb_free_big`,
  `src/compat.h`; the read lock, so others fault in between). The ablation (15 rounds per variant, rounds 2 to 4 of 5
  runs, `--series 1000000`) shows both are needed: with (1) alone the worst case stays at 8 to 10 ms in the first
  rounds after a start (the buffers are then new) and the mean of the max lookup is 4.3 ms; with both 0.7 ms. Mode
  "on" keeps a worst lookup of 2.2 ms that this change does not touch (not measured further: the lookups of that mode go through the table's own path). The listing section (parts listed under `db->lock`) takes under 10 us.
- The window threshold (`LT_WINDOW_MIN` 50,000) stays. This measurement shows no difference between on and nowin at
  10^6 series (the pause above hides the window's effect on the worst case; the review's own run saw worst lookups of
  2 to 6 ms with the window and 5 to 12 ms without). The window threshold itself is not measured below 10^6 series.

## 10. Build plan

Each step is a track (tsdb-tracks): build, adversarial review, fix, gates, merge.

| step | what | gates | effort |
|---|---|---|---|
| 1 | `latest_at` (§5.1: partitions newest first, cutoff skip, O(1) id-range checks, one granule per source, the non-null default with §5.2's shadow rule, `WITH_NULLS`) and `tsdb_latest` on the slow path alone (no table), C API, `tests/test_latest.c` with the oracle | `make test`, `make asan`; slow-path numbers at 10, 100, 1,000 partitions | 3 days |
| 2 | The table: `src/latest.c`, pages and budget (a sticky cap), `latest_rw` and `lgen`, the folds with `R` and `N` at every listing (flush, insert, replay, delete, replace), the §3.2 window for large flushes, retention under `latest_rw` and the merge's `ret_gen` compare, `drop_before` with the floor, the build in `cls_check_parts` split by id blocks, the warmer and its hint set, `top_nn` and the flags in `mt_add`, the lookup with read holds of 256 ids and the searches after them, `latest_cache = 0` on followers; concurrency tests and hooks | `make test`, `asan`, `tsan`, `make jepsen` (it touches the publish path); the benchmark gates of §2.3 (inserts against no lookups; publish latency under slow-path lookups), flush holds | 6 to 7 days |
| 3 | Protocol 14: QUERY_LATEST in tsdbd, `auth_ops`, the C, Rust, Python and Java clients and embedded bindings with `latest_by_name`, the CLI, metrics, fuzz seeds; jepsen's latest check and null writes | `make test`, `test-clients`, `integration-tests`, `fuzz-frames` 600 s, `make jepsen` with the new check | 4 to 5 days |
| 4 | Benchmarks (§9), README and docs (protocol comment, metrics.md, CHANGELOG, roadmap: the "not planned" line goes), AGENTS.md (the invariant below) | numbers on a quiet machine | 2 days |
| 5 | Cluster: the client split per leader, REPLICA_OK and `min_pos` for the op; with the cluster's own steps | cluster simulators and `make jepsen-cluster` | part of the cluster work |

About three weeks for steps 1 to 4.

**AGENTS.md gains one invariant**: *The latest table holds, per series, the newest non-null row in the parts, or a
bound on it (unknown); it is read and written only under `latest_rw`, and every write section bumps the volume's
`lgen`.* Its writers are publishers (in the write section that lists their parts, after `parts_commit`, folding each
part's newest non-null row and null range; a large flush through the publish-pending window, whose unlocked stores
only ever store the flush's own row), retention lengthening (with `db->lock`, where `ret_gen` moves), `drop_before`
(the floor), a merge that lists across a retention change (it compares `ret_gen` under `db->lock`), and the warmer,
the only filler, which stores a parts-only search only if `lgen` is unchanged since its cut and no window is open.
Lookups never write it. Whoever makes an entry unknown gives it a correct bound and pushes its id to the warmer's
hints. A new listing point must fold there, nulls included; anything else that removes live rows, other than rows
hidden by a tombstone of a later block, rows a later write overwrote or rows already expired, must bump `lgen` in a
write section and make the entries it may affect unknown. Lock order: `publish_lock` > `latest_rw` > `manifest_lock`
> `db->lock` > `buf_lock`; `ret_lock` > `latest_rw` > `db->lock`; drops and the rare merge take `latest_rw` (write) >
`db->lock`; lookups and the warmer take `latest_rw` (read) > `db->lock`, lookups then `buf_lock` in sub-holds; the
hint set's mutex and the window's `pmu` are leaves. `latest_rw` is held across no I/O except the renames of the partitions `tsdb_drop_before` retires (in its write section, where `db->lock` was held across them already): lookups take their cut and read granules only after
their read hold, and `series_reload` runs outside it. No scheduler helper waits for `latest_rw` in write mode. NFS
followers have no table.

**What lands in a 0.9.x**: protocol 14 (one new op; floor still 12, so no client breaks) and the C API
(`tsdb_abi_version` up). No disk format change: nothing to migrate. The changelog says under "Added": the op, the
call, the option, the client helpers, the metrics; under "Changed": protocol 14, the ABI version.

## 11. Later: a latest table on NFS followers

Not in this design (L13). If followers' lookups turn out to matter, a follower table needs what the earlier rounds
designed (commit 94f1430, §3.10): 24-byte entries with the block number, a fold of each refresh's new parts by
`(ts, block)`, a check against tombstones **and nulls** of listed parts with higher blocks in the same partition (N3),
re-derivation of entries whose row lay in a part that left the list, and a refusal of warmer batches whose cut
predates a refresh that removed parts. Its acceptance tests are the traps found so far: a tombstone written and merged
away between two refreshes (H3), a fill whose cut predates a refresh that removed the series' rows (V2), a shadowed
newest row of a merge output that must still raise an unknown entry's bound (V3), fills that carry the block of their
row (V12), and a null in a listed part above a new merge output's row (N3). The warmer of §3.8 and the per-volume
`lgen` carry over unchanged.

## Open questions

None. The six that remained were closed on 2026-11-06 by taking the recommended answers as defaults (D1 to D6
below); the user may change any of them.

## Decisions on the former open questions (2026-11-06)

Taken as defaults on 2026-11-06, the recommended answer of each; the user may change them.

| # | Decision | Revisit when |
|---|---|---|
| D1 | **Per-series instants** (A5). Each series is read at one instant during the request; two series of one request may be read at different instants. This is documented in tsdb.h, the wire doc and the clients. QUERY_MULTI (`VALUES`, descending, limit 1) stays for the rare caller that needs one cut per volume. | A user needs one instant per request for 10^5 ids at publish rates the lock would not hold up; that would be a new op or flag, not a change of this one. |
| D2 | **No `now + 1` clamp.** The default answer is the greatest timestamp (A1); a caller that distrusts device clocks passes the end bound (slow path, §5.1). Added: the metric `tsdb_latest_future_series` (§6.5), series whose cached newest row is more than one hour ahead of the clock. | The metric shows far-future rows in real data and operators want them hidden by default: then a per-volume clamp option, off by default. |
| D3 | **The drop floor is accepted** (L20, §3.6): series whose newest non-null row lies below a drop's `t` and survived it take the slow path until written again, counted by `tsdb_latest_search_total{why="floor"}`. | The metric shows many such series: give each page a floor epoch that the warmer (never a publisher) advances after it has walked the page. |
| D4 | **`WITH_NULLS` takes the slow path** (5 to 20 µs per id, §5.1), as A2 and §5.2 say. | A dashboard needs it fast, measured first: a per-entry "a newer null exists" bit that publishes set and the memtable layer reads. |
| D5 | **No latest table on NFS followers** (L13): `--follow` forces `latest_cache = 0` and lookups cost 50 to 100 times more per id there (`why="follower"`). §11 stays a later step. | A single-server deployment asks for fast lookups on its followers: build §11. |
| D6 | **The budget's cap is sticky until the volume reopens** (L5). Ids beyond it are answered exactly by the slow path (`why="cap"`); a reopen sizes the table to the budget then free. | Operators need to lift the cap without a restart: that is the "unknown, no bound" state L5 rejected, so it needs its own design. |

## Unverified assumptions

- U1: lookup cost of 0.1 to 0.2 µs per id on one thread, the sort included, about 15 ns of it under `buf_lock`; a
  read hold of 256 ids lasts 25 to 50 µs; to be measured (§9).
- U2: 5 to 15 ms of write hold for the stores of a million-series flush, during which lookups of that volume wait.
- U3: split by id blocks, `cls_check_parts` with the build takes about 11 to 34 s at 10^8 series x 365 partitions
  on 32 threads with a warm page cache, and the build adds below 10% to it, dense or sparse parts; reported in
  absolute seconds.
- U4: a slow-path read (a fill, an as-of bound, a search for an unknown entry) costs 5 to 20 µs, whatever the number
  of partitions or tombstones; a series with no row or wholly expired costs no more (the cutoff skip, the id-range
  check); a series null for a day costs a walk over index entries, no decode.
- U5: with a quarter of 10^7 series of type state and a tenth ending in nulls, the warmer reaches
  `tsdb_latest_unknown = 0` within 2 s of the open on 32 threads with a warm page cache; 25 to 125 s cold over NFS.
- U6: one volume serves 20 M ids/s with its inserts within §2.3's gate (§7.2).
- U7: warmer batches are refused about `1 - exp(-r x 0.5 ms)` of the time at `r` publishes per second, and the
  adaptive batch keeps `tsdb_latest_unknown` falling at 1,000 publishes per second.

## Changes after review (2026-11-06)

| finding | where it is now |
|---|---|
| Correctness H1, performance H2(a): the publisher's fold was computed outside the lock and raced fills | L7, §3.2: only the per-series `(R, N)` are computed outside; compare and store inside the write section. Since the third round only the warmer fills (L10). |
| Correctness H2, performance H2(b): a fill read `lgen` after the request's cut | §3.8: the warmer takes its cut and `g` in one read hold (lookups no longer fill) |
| Correctness H3: a follower missed tombstones merged away between refreshes | Third round: no table on followers (L13, §3.10); an acceptance test of §11 |
| Correctness M1: retention lowered | §3.5: fills search without a cutoff (empty = no non-null row in parts); limit and marking in one section; merges across a retention change mark their range |
| Correctness M2: `drop_before` outside the lock order | §3.6: `latest_rw` then `db->lock`, retire and floor in one section |
| Correctness M3: `SKIP_NULLS` lost the shadow of a newer null | §5.2: now the default rule; a candidate is accepted only if no higher-priority source has a row at its timestamp |
| Correctness M4: zeroed pages read as rows at 1970 | L4, §2.1: the timestamp is stored XOR `INT64_MAX`; all-zero is empty |
| Correctness M5: unknown ids answered "none" | L15, §6.1-6.3: NOTFOUND per record after one SERIES reload on followers and replicas; DENIED and NOTFOUND records carry type 0 |
| Correctness low: liveness of one `lgen`; tails missing from the ranking; chunked large inserts; the delete's fill under locks | §3.8 (adaptive warmer batches, measured in U7), §5.1 (tails), §3.3, §3.4 (no delete fill) |
| Performance H1: lookups held `db->lock` and `buf_lock` for a whole chunk | L19, §2.3: O(1) under `db->lock`, 128-id sub-holds of `buf_lock` with prefetch; gate and fallback |
| Performance M1: the slow path was O(parts) | L17, §5.1: partitions newest first, windows per visited partition, early stop, cutoff skip, id-range checks |
| Performance M2: the rebuild read `GF_LAST` per run; volumes do not open in parallel | L11, §3.9: one read per series after the walk, in parallel; the claim about volumes is dropped |
| Performance M3: the warmer was too slow | L12, §3.8, §3.9: bounds let the next flush settle live series; the warmer runs at full width; the format bump of L18 |
| Performance M4: flush stores held the write lock for milliseconds | §3.2: a publish-pending window for large flushes |
| Performance M5: memory had no budget and the doc counted only the table | A6, L5, §2.4: a byte budget; the per-series memory line; design-cluster.md §10.2 counts memory |
| Performance M6: hot volumes | §7.2, §7.3: the gate, `REPLICA_OK` with `min_pos` as the read knob, the comparison with a cache beside the database |
| Performance L1: whole-table scans on drops and retention | §3.6: the drop floor (third round), no scan; retention is per series |
| Performance L2: the in-flight scan under `buf_lock` | §2.2: an in-flight `top_nn` goes to the search on the cut, which reads the memtable in pieces outside the lock |

Second round: a verification of the revised design, correctness (V) and performance (P), the same day.

| finding | where it is now |
|---|---|
| V1 (high): the drop mark lost writes made after the drop, and missed entries already unknown | Third round: the floor (L20, §3.6) replaces the mark; the warmer refuses any batch across a drop (`lgen`) |
| V2 (high): a follower refresh that removed rows did not re-touch unknown entries | Third round: no follower table; an acceptance test of §11 |
| V3 (high): a shadowed newest row did not raise an unknown entry's bound on a follower | Third round: no follower table; an acceptance test of §11 |
| V4: a merge across a retention change skipped unknown entries; its `ret_gen` compare was outside the lock | §3.5: the compare under `db->lock`, where `tsdb_set_retention` now bumps `ret_gen`; unknown entries need nothing (the warmer refuses across the merge's `lgen` bump) |
| V5: lookups applied the cutoff after releasing `latest_rw` | §2.3 step 3: the cutoff is read inside the hold; for searches, before the cut |
| V6: the open's tombstone bound `a - 1` was wrong when keys overlap in time | §3.9 step 2: unknown `(k.ts)`, the greatest row seen so far; an oracle case with classes and a reopen (§8) |
| V7, P1 (high): the slow path ran inside the read hold | §2.3: the hold takes only entries, memtable reads and the cutoff; the cut and the searches come after it |
| V8: a lookup's fill was not the parts' newest row | §3.8, §5.1: only the warmer fills, from the parts-only form |
| P2: the insert gate's baseline and criteria | §2.3: baseline with no lookups, a QUERY_MULTI reference, per-mode criteria, hot and spread sets, now with nulls |
| P3: write sections per chunk parked scheduler helpers | §3.8: one write section per warmer batch, on the warmer's own thread |
| P4: a follower refresh did O(series) under the write lock | Third round: no follower table |
| P5: the open build's memory traffic, and a serial walk | L11, §3.9: the walk split into blocks of 32,768 ids over the open's threads |
| V9, V10, V11, V12, P6-P11 (low) | §3.2 (window), §5.2 (equal timestamps), §6.1 (`series_reload` outside the lock), §11 (follower blocks), §2.2, §2.4, L5, §2.3 (sort), §5.1 (tombstone search), U5, §3.2 (window built in step 2) |

Third round: a second verification under the user's answers (N), and a simplification review (S), the same day.

| finding | where it is now |
|---|---|
| User's answers: newest non-null by default, on at RAM/16, ids on the wire with `latest_by_name`, the value in the index at the next format bump | Header, A2, A6, §1, L1, L14, L18, §5.2, §6; open questions 1, 2, 3 and 5 of the second round closed |
| N1 (high): the fast path took a candidate a newer source had nulled at the same timestamp | §2.2 (shadowing, `has_null`), §2.3 step 5: the best candidate is checked against the higher-priority memtables, O(1) in the common case, otherwise a search; oracle cases (§8) |
| N2 (high): the fold did not invalidate on a null at the cached timestamp, and dropped all-null series from its list | §3: `N` per series of every part, the rule "known `k` with `k.ts` in `N` and no newer `R` becomes unknown `(k.ts)`", every series in the list; (b) needs no touch any more (S1) |
| N3 (high): a follower took a merge output's row that a listed null overwrote | §3.10: no follower table; the slow path's §5.2 rule ranks parts by block; a follower test; an acceptance test of §11 |
| N4: the §3.2 window could expose an invalidation as a known row | §3.2: step 2 only stores the flush's own row; invalidations wait for step 3, under the write lock |
| N5: `top` tracked any value, and two indexes would cost 20% of `mt_series` | L6, §2.2: one `top_nn`, the type byte and flags `has_nn`, `has_null`, `nn_stale`; 40 bytes stay 40 |
| N6: the old default was encoded throughout; `WITH_NULLS` cannot come from a table of non-null rows | §1, §2.1, §5.2, §6.1, §6.2, §8 (oracle, jepsen with null writes); `WITH_NULLS` takes the slow path (decision D4) |
| N7 (low): the open read `GF_LAST` without its null flag | §3.9 step 3: `GT_LAST_NULL` gives unknown `(ts - 1)`; state series by their null count; U5 counts series ending in nulls |
| N8 (low): a cut reused across holds met a later cutoff | §2.3: searches read their cutoffs right before the cut, as queries do; no cut is reused across holds |
| N9 (low): pages made after the cap lifted, and pages built out of order | L5: the cap is sticky until reopen (S8); L11, §3.9: pages reserved in id order before the walk |
| S1 (high): fills stored after their snapshot were the densest source of bugs | L10, §3.8: lookups never write the table; one warmer per volume validated by `lgen`; every write section bumps `lgen` |
| S2 (high): the follower table | L13, §3.10, §11: no table on followers in this design |
| S3: the sweep-first rule could put a whole-table sweep into a publisher's write section | L20, §3.6: the drop floor |
| S4: `latest_at` had no stop for a series with no candidate | L17, §5.1: the cutoff skip and the O(1) id-range check per part; U4 cases |
| S5: every merge listing took `latest_rw` | §3.5: only a merge whose `ret_gen` changed takes it |
| S6: the user's default was missing | as N1-N7 |
| S7 (low): the format bump should store the newest non-null slot | L18 |
| S8 (low): the cap lift made a state no publish settles | L5: a sticky cap |

**Removed in the third round**, and what replaced it:
- per-entry touch generations, the fill queue with its six drain sites, fills by lookups and by deletes, the two
  search forms in a lookup, `mark_gen` and `rm_gen`: one warmer per volume, a per-volume `lgen` (L10, §3.8);
- the drop mark with its epochs, per-page `swept`, the sweep-first rule and `drop_before`'s sweep loop: `drop_floor`
  (L20);
- `latest_rw` on every merge listing: a `ret_gen` compare under `db->lock` (§3.5);
- the follower table, its block numbers, shadow checks and re-derivation, and the `refresh_lock` > `latest_rw`
  edge: the slow path on followers (L13, §11);
- the cap that lifts, and its "unknown with no bound" state: a sticky cap (L5);
- `top` of any value and the in-flight wait-and-look-again path: `top_nn` with flags, and the search on the cut
  (§2.2).
