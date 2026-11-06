# Change feed (CDC) and a Pulsar source

Status: design, not implemented. First written on 2026-11-02 (branch `cdc-design`, 94cf56c, from main at 87eb49e);
revised on 2026-11-06 on branch `cdc-design2` from main at 0683460, with the user's answers to the first version's
ten questions (F1-F10) and everything merged since: 64-bit ids, login and rights, the 0.9.0 release, the
last-write-wins fence, the two HA modes and the latest-value design. Line numbers are at 0683460 unless marked.

The user decided on 2026-11-02 to have a Pulsar **source** that forwards every new write of selected series live
(one, several, all, or by name prefix; tenants are name prefixes), shortly after tsdb acknowledges it, and resumes
after a restart of tsdbd or of the connector without gaps and without duplicates a consumer cannot detect. Writes into
the past, replaces and deletes are forwarded too. This reverses the earlier "no subscriptions in tsdb" decision for
this use only.

Related designs: [design-ha-modes.md](design-ha-modes.md) (the two HA modes; step S0 builds this design's disk
format), [design-cluster.md](design-cluster.md) (§4: the WAL record of §2.1 is the replication unit, and `fno` is the
cluster's position, C6), [ha.md](ha.md) (shared mode: the ledger, chained files, the ack gate),
[design-auth.md](design-auth.md) (rights, `auth_ops`), [design-latest.md](design-latest.md) (protocol 13, which this
design follows), [design-series64.md](design-series64.md) (ids), [compatibility.md](compatibility.md) (what a 0.9.x
may change and how it says so).

## Decisions

The user's answers of 2026-11-06 to the first version's questions:

| # | Decision | Rejected | Why |
|---|---|---|---|
| F1 | **Feed retention per volume: 24 h and 64 GiB by default, with slots** (user, 2026-11-06, question 1). A named consumer (the Pulsar source) holds retention back through a slot, up to a cap; at the cap the slot is invalidated (its consumer gets `FEED_GONE`), the database never fills up (§2.7). | Client-held cursors only (the first version's recommendation) | A source that stops for a weekend (a Pulsar outage, a broken deployment) should resume, not resync, when the disk allows it; the cap keeps PostgreSQL's abandoned-slot problem out. |
| F2 | **`REASSERT` is a should-never-happen check with a counter** (settled by later work, 2026-11-06, question 2). The last-write-wins anomaly of the first version's §1.6 is fixed in 0.9.0 by `lw_fence` (AGENTS.md "Last write wins"), so position order is last-write-wins order for every key (§1.6). The reader checks it, counts and logs a violation, and repairs the consumer's state (§1.6). | Repair as a normal path; no check | The fence makes the repair dead code in a correct engine; a check keeps a future engine change (the cluster's apply path) from silently breaking mirrors. |
| F3 | **Forwarded: deletes (also those that delete nothing), `set_retention`, `drop_before`. Not forwarded: retention expiry** (user, question 3). | Forwarding expiry | All three are explicit calls; expiry is a function of the clock (§1.1). |
| F4 | **The feed is enabled per database, off by default** (user, question 4). | Per volume; on by default | One switch for the operator; a database without consumers pays nothing beyond F9. |
| F5 | **Followers serve the feed in v1** (user, question 5): NFS followers through `FEEDPOS` (§2.8), and in replicated mode replicas up to their commit point (§2.9). | The writer only | Production reads from followers over NFS. |
| F6 | **At least once, with a dedup key per event: `(volume, event)`** (user, question 6). No `EFFECTIVELY_ONCE`; the source refuses it, as the sink does (TsdbSink.java:88-101). | Broker deduplication through `EFFECTIVELY_ONCE` | Pulsar's effectively-once for connectors was unverified on Pulsar 5 and scalable topics, and the sink already runs only `ATLEAST_ONCE`; every duplicate is an identical copy with the same key, which a consumer drops by comparing numbers. |
| F7 | **Avro first** (user, question 7): the source publishes Avro records with a registered schema (§4.3); JSON is the option. The sink learns to read them, so a tsdb-to-tsdb mirror through Pulsar works (§4.7, step F9). | The sink's JSON first | A schema in Pulsar's registry is what downstream consumers (Flink, other sinks) expect, with evolution rules the broker enforces. |
| F8 | **Protocol 14** (settled: 12 is login, 13 latest values). **The disk side is format 9, built in S0** (design-ha-modes.md §7, design-cluster.md step 0); §5 lists exactly what of S0 the feed needs. The feed's own files sit behind `requires=feed` in META, so the feed needs no format bump of its own. | Protocol 12 (the first version) | Both numbers were taken by work that merged first. |
| F9 | **One WAL format: +16 bytes per WAL record even with the feed off** (user, question 9). A record is one group commit, not one row. | Two record formats | One replay path, and the cluster uses the same record (C6). |
| F10 | **Compression of retained segments later, measured first** (user, question 10): step L1 of §8. | In v1 | The production ingest rate decides whether 64 GiB holds 24 h; measure before building. |

Decisions of this revision (the user may change them):

| # | Decision | Rejected | Why |
|---|---|---|---|
| F11 | **Slot changes are WAL markers** (create, advance, invalidate, reset, drop), checkpointed into a per-volume `FEEDSLOTS` file. The slot *registry* (names, owners, caps) is kept in the root volume on a single server and in the metadata group in both HA modes. | A slot file rewritten with `fdatasync` at every advance; slots in the metadata group only | Markers are durable with the group commit they ride on, fenced in shared mode (the WAL is a chained file), replicated in replicated mode, and seen by followers that tail the WAL: one mechanism in every mode, and a takeover or failover keeps slots for free. Advances through Raft would be one commit per second per slot per volume. |
| F12 | **A subscription delivers only series the user may `read`**; explicit ids without the right make the subscribe `DENIED`, prefixes, globs and `all` silently skip what the user cannot read (denied and missing look alike). Grants are re-applied mid-stream (§3.2). The selection uses the grant grammar (`all`, `prefix:`, `name:`, `glob:`) plus ids. | Feed access as its own right; checking only at subscribe | The feed is a read of the series; one grammar and one matcher (`rights_of`) serve both. A stream lives for days, so a revoke must reach it. |
| F13 | **An event is delivered only up to the position its volume could acknowledge**: `wal_durable` on a single server, the ack gate in shared mode (ha M16), the commit point in replicated mode. | Delivering what is written | A delivered event must never be taken back, by a crash, a fence or a failover that cuts uncommitted records. |
| F14 | **One `COMMIT` payload** for the feed's marker, shared mode's ledger record and the cluster's `COMMIT` marker (M15). The feed's marker carries the whole payload, so the per-commit file (`fcommit_`) lives only until the marker is durable. | A kept `fcommit_` file per commit for the retention (the first version) | One encoder; no file per commit for 24 h; in replicated mode the cluster's marker *is* the feed's. |
| F15 | **The Pulsar source keeps its positions in its slot**, not in Pulsar's state store. | `SourceContext.putState` (the first version) | One store fewer, and putState on a Pulsar 5 function worker was unverified; the slot is durable, fenced and moves with the volume. |

## Summary

- **Where events come from.** Two sources the engine already makes durable, plus one small record:
  1. small writes: the **WAL**. A flushed segment is no longer unlinked but renamed `fwal_<seq>.log` and kept for
     the feed's retention. No row is written twice.
  2. large inserts, replaces, deletes: **hard links** `f<part name>` to the parts a direct commit published, so
     merges cannot take their rows away, and on a single server an intent file for every direct commit (also of one
     part), kept as `fcommit_<block>` until its marker is durable.
  3. a **marker** record appended to the WAL right after each direct commit, under `publish_lock` and before the
     commit's fence is lifted: it fixes the commit's place among the small writes and carries the `COMMIT` payload.
     Markers also record `set_retention`, `drop_before` and slot changes.
- **Positions.** Every WAL record carries `fno`, a 64-bit **feed number** per volume that never goes back; in
  replicated mode it is the cluster's position. An event's number is unique in its volume; **`(volume, event)` is the
  dedup key**. A **cursor** per volume is `(feed_id, event, check)`; `check` (32 bits of the record's hash) refuses a
  cursor from another history (a restored backup, a storage rollback, a stale copy): `FEED_RESET`, never a silent
  resume.
- **Order.** Per volume, events arrive in strictly increasing number. Since 0.9.0's fence, applying a series' events
  in that order gives exactly tsdb's state for it; the reader checks this and repairs and counts a violation (F2).
  No order across volumes.
- **Durability.** Only what the volume could acknowledge is delivered (F13). A crash or a failover never takes back a
  delivered event. A write whose client saw an error but which reached the disk is delivered after the restart.
- **Never forwarded:** memtable flushes, merges, `tsdb_optimize`, class switches, retention expiry and the dropping of
  expired partitions.
- **Retention and slots.** Per volume by age (24 h) and size (64 GiB), oldest segment first, except what a **slot**
  holds: a named, durable consumer position (owner, per-volume positions) that holds retention back up to a cap (7
  days, 256 GiB per volume, 10% free space). At the cap the slot is invalidated: its consumer gets `FEED_GONE` and
  resyncs; writes never wait and the disk never fills. Slots survive restarts, takeovers and failovers.
- **Protocol 14.** `FEED_SUBSCRIBE` (selectors, volumes, a slot, cursors) answered by a stream (`HEAD`, `SERIES`,
  `STATE`, `ROWS`, `DELETE`, `RETENTION`, `DROP`, `POS`, `VOLUME_END`, `END`), `FEED_ADVANCE` (on the stream),
  `FEED_INFO`, `FEED_SLOT_CREATE`, `_DROP`, `_LIST`; each with a row in `auth_ops`. tsdbd runs streams on feed pump
  threads that never wait; writers never wait for consumers.
- **Embedded:** `tsdb_feed_*` and `tsdb_feed_slot_*`, one reader (`src/feed.c`) for tsdbd and the library.
- **Pulsar source** in the existing NAR: Avro records (schema `ai.intellistream.tsdb.feed.Event`, evolution
  `FULL_TRANSITIVE`) keyed by series name, `ATLEAST_ONCE` only, the sink's selection keys, positions in a slot.
  The sink learns the record's `op` (delete, replace, retention), so tsdb -> Pulsar -> tsdb mirrors.
- **HA.** Both modes keep the feed and its slots across a takeover or failover: shared mode because the files move
  with the volume (and the ledger's `COMMIT` rebuilds a lost marker), replicated mode because the WAL, its markers
  and the retention are the same on every replica (§2.9).
- **Cost with the feed off:** 16 bytes more per WAL record (F9). With it on: no extra fsync on the small-write path;
  one intent (about 1.4 ms on the SN720) more for a large write that commits a single part; the retained WAL (20
  bytes a row) and the parts merges replaced while links hold them.

## 0. What the code does today (each fact checked at 0683460 unless marked)

| fact | where |
|---|---|
| A WAL record is `[u32 rows][u32 header check][u64 hash][rows x (u32 series, i64 ts, u64 slot)]`, one per group commit; the check binds it to its segment and offset; segments are `wal_<16 hex>.log` | memtable.c:8-13, 341, 1072-1076 |
| `wal_appended`/`wal_durable` count rows and start again at every open: no persistent row number; `wal_flushed`, `wal_removed`, `wal_removed_synced` | internal.h:1374-1393 |
| Small writes are buffered in `wal_buffer` under `buf_lock`; in sync mode the call waits for `wal_commit` | memtable.c:429, 500 |
| **Last write wins in real time**: a large insert, replace or delete registers an `lw_fence` (`lw_enter`, which reports whether the memtable holds rows, so the caller flushes it), and `mem_insert` waits on the fence's cv while a row of its batch is inside it; `lw_leave` after the publish | memtable.c:638-720; db.c:908-914; delete.c:103-120; insert.c:648-673 |
| `parts_commit` numbers the parts under `publish_lock` and writes an intent (`tsdb-intent 3`) only for several parts | part.c:889, 904, 909 |
| `intents_recover` rolls a complete intent forward at open | part.c:1026 |
| A flush commits its parts, sets `wal_flushed`, publishes MANIFEST, then unlinks the segments (`wal_unlink_upto`); at open every leftover segment is replayed and unlinked; `wal_close` unlinks too | memtable.c:754, 816, 828, 1082, 1233 |
| `test_precommit` fires before the publish of a large insert and of a delete or replace | insert.c:661, delete.c:139 |
| `tsdb_drop_before` is public but not in the protocol | tsdb.h:681; no op in tsdb_protocol.h |
| Ops go up to `USER_LIST` (38); protocol 12, floor 12; design-latest.md takes 39 (`QUERY_LATEST`) and protocol 13 | tsdb_protocol.h:192-193, 228-239 |
| Status codes: -10 to -15 kept for HA, -16 `DENIED`, -17 `AUTH` | tsdb.h:74-76 |
| `FORMAT_VERSION` 8 | internal.h:41 |
| tsdbd: output capped at `OUT_CAP` (8 MiB); `CANCEL` handled in `parse()`, stops QUERY_MULTI (`cancel_multi`) | tsdbd.c:160-161, 2273, 2343 |
| crashfs intercepts `rename`, `renameat`, `unlinkat`, not `linkat` | tests/jepsen/crashfs.c:174-212 |
| The Pulsar sink runs only `ATLEAST_ONCE`; it reads JSON and Avro `GenericRecord`s, also one series with arrays; its selection is `series`, `seriesPrefixes`, `seriesPattern`, added together | TsdbSink.java:88-101; PointParser.java:128-134, 202-225; SeriesSelection.java:16-26 |
| Merges keep the highest `wal_seq` of their sources and retire them (`retired_*`); expiry drops rows and whole partitions in the merge thread (at 87eb49e) | merge.c |
| Followers tail MANIFEST and the WAL segments it names (at 87eb49e) | manifest.c:1-23 |

## 1. The feed model

### 1.1 Events

The feed of a volume is a sequence of events. Each names one series (`tsdb_sid`) except `DROP`.

| event | made by | content |
|---|---|---|
| `ROWS` (origin `WAL`) | `tsdb_append`, small `tsdb_insert*` | the rows of one series in one WAL record, in WAL order (a key written twice in a record appears twice; the later one wins, as in tsdb) |
| `ROWS` (origin `BULK`) | a large insert | the rows of one series in one granule of one part of the commit |
| `DELETE` (origin `DELETE`) | `tsdb_delete_range` | series, `[start, end)` as the caller gave it (not clipped to partitions), also when it found nothing to delete (F3) |
| `DELETE` + `ROWS` (origin `REPLACE`) | `tsdb_replace_range` | the range, then the new rows; the range comes first |
| `ROWS` (origin `REASSERT`) | the reader, only when the check of §1.6 fails | rows that win over a direct commit in tsdb although their position is before it |
| `RETENTION` | `tsdb_set_retention` | series, the new limit |
| `DROP` | `tsdb_drop_before` | volume-wide: every row before the partition boundary at or below `ts` is gone |

Events of one operation share a **group**: each carries `group_last`, the number of the operation's last event, so a
consumer that wants a large insert or a replace to appear at once buffers until it has seen `group_last`.

Series names and state names are not events. A subscription announces a series (`SERIES`: id, type, name, retention)
and a state code (`STATE`) the first time one of its events needs them.

**Never forwarded, and why** (F3):

- **Memtable flushes, merges, `tsdb_optimize`, class switches.** They move rows between files and apply tombstones;
  every query answers the same before and after. They are not in the WAL and make no direct commit (§2.3).
- **Retention expiry.** It is a function of the clock (`eng_now_ms`), applied by merges at times unrelated to the
  writes, so it has no place in the write order; a consumer applies the same policy, which `SERIES` and `RETENTION`
  give it. Documented consequence: a mirror keeps rows tsdb has expired unless it applies the limit, and a write
  older than the cutoff is forwarded (it was acknowledged) although tsdb's queries never show it.
- **Series creation without rows**, `tsdb_volume_add`, `FLUSH`, `COMMIT`, user and slot administration: no data.

### 1.2 Feed numbers, events, positions

**`fno`.** Each volume numbers its WAL: a rows record takes one number per row, a marker takes `span` numbers (one
for most kinds, the events it expands into for `COMMIT`, §2.1). The record header holds its first number, so every
number is durable with its record. Numbers never go back (§2.1), and gaps are allowed. In replicated mode `fno` is the
cluster's position (design-cluster.md C6), identical on every replica.

**Event number** = for rows, the `fno` of the event's last row; for a `COMMIT`'s events, `fno + k` for the k-th event
in the reader's fixed order; for `DELETE` without rows, `RETENTION`, `DROP`, the marker's number. Chunking is part of
the definition and fixed (per WAL record and series, up to 8192 rows; per part, series and granule for bulk rows),
never the consumer's choice: two readers of the same history, on the writer, a follower or a replica, see the same
events with the same numbers. **`(volume, event)` is the dedup key** (F6).

**Position frames.** `POS (volume, feed_id, event, check)` says "every event of this volume up to `event` has been
sent". The server sends one after each WAL record, between chunks of a large commit, and when idle.

**Cursor** (per volume, 24 bytes): `u64 feed_id, u64 event, u32 check, u32 volume`. `feed_id` is a random 64-bit
value made when the feed is enabled on the volume (§2.13), so a cursor of another volume, another database or an
earlier enablement is refused. `check` is the low 32 bits of the `xxh64` of the record that holds `event`. On
subscribe the server finds that record and compares:

| cursor | answer |
|---|---|
| `feed_id` unknown on this volume | `FEED_RESET` |
| `event` beyond the volume's next number | `FEED_RESET` (this history is behind the consumer's: rollback, restore) |
| the record holding `event` has another hash | `FEED_RESET` (another history reused the numbers) |
| `event` older than the oldest event kept | `FEED_GONE`, with the oldest number |
| otherwise | the stream starts after `event` |

A volume without a cursor starts at its slot's position (§2.7) if the subscription names a slot, else at its head
(`FROM_HEAD`, the default) or its oldest event (`FROM_OLDEST`).

### 1.3 Order a consumer can rely on

1. Per volume, events arrive in strictly increasing event number, and `POS` never goes back. So per series.
2. Applying a series' events in that order (rows: upsert by `(series, ts)`, the later value wins; `DELETE`: remove the
   range) yields exactly tsdb's rows of that series as of the last applied `POS`, retention aside (§1.6 says why).
   This is the property the checker tests (§6.3).
3. The events of one direct commit are contiguous; `group_last` marks their end.
4. **Nothing across volumes.** Each volume has its own engine, WAL and numbering; a multi-series insert whose rows
   land in two volumes is two independent operations in tsdb already. A consumer needing a cross-volume cut uses the
   per-volume cursors as a vector.
5. Requests of one tsdbd connection run in order, so one client's writes to one volume appear in the order it sent
   them.

### 1.4 Delivery semantics

**At least once, with exact deduplication** (F6): after a resume from a cursor or a slot, everything after it is sent
again, identical (same numbers, same content: the history is immutable). A consumer drops an event whose number is not
above the last it applied for that volume (or for that series: the same test per key). Every duplicate is exactly one
earlier event.

A consumer that applies events as upserts and range deletes in order needs no deduplication at all for its final
state: re-applying a suffix of a key's events after the whole sequence leaves every key as the suffix's last event
left it, which is what it was. Duplicates then only show older values for a moment. Exactly once end to end needs the
consumer to store its position atomically with its effect.

### 1.5 When an event may be delivered (F13)

| setting | delivered when | a crash can lose |
|---|---|---|
| single server, `wal_sync_ms = 0` (default), `fsync = 1` | the record is fsync'd (`wal_durable`) | nothing delivered |
| single server, `wal_sync_ms = N > 0` | the same: rows wait for the background sync (lag up to N ms) | nothing delivered (tsdb itself can lose up to N ms of acknowledged small writes, never delivered) |
| shared mode (ha.md) | the record is fsync'd **and** the owner's ack gate passes it (design-ha-modes.md M16: RECHECK, the continuity check) | nothing delivered; a deposed owner delivers nothing it could not acknowledge |
| replicated mode | the commit point passed the record (design-cluster.md §4.2) | nothing delivered: a failover cuts only uncommitted records |
| NFS follower | up to `FEEDPOS` (§2.8) | nothing delivered |
| `fsync = 0` | after the write(2) | delivered events, on power loss; refused in both HA modes; a warning at subscribe |
| `wal_sync_ms = -1` (no WAL) | the feed cannot be enabled: `TSDB_ERR_INVALID` | |

A record that was written but whose fsync failed (fail-stop) is never delivered by that process; if the restart finds
it intact, tsdb has it and the feed delivers it then: the feed can carry a write whose client got `TSDB_ERR_IO`,
exactly as tsdb's queries can show it.

### 1.6 Position order is last-write-wins order (F2)

tsdb ranks a memtable row above every part. In the first version of this design, a small write acknowledged while a
large write to the same keys was in flight won over it although it was acknowledged first, and the feed repaired that
with `REASSERT` events. **0.9.0 fixed the anomaly in the engine** (`lw_fence`, AGENTS.md "Last write wins",
`test_lww_realtime`), and with it position order equals last-write-wins order for every key:

- A row in the large write's keys that is in a memtable when the fence is registered is flushed before the large
  write publishes (`lw_enter` reports pending rows, the caller flushes: db.c:908, delete.c:103), so it gets a lower
  block number. Its position is before the marker, since the marker is appended only at the publish.
- A small write into the keys after the fence is registered waits on the fence's cv (memtable.c:693-720) and appends
  only after `lw_leave`. The marker is appended under `publish_lock` before `lw_leave` (§2.3), so the row's position
  is after the marker, and it ranks above the commit in tsdb too.
- Rows outside the keys never conflict with the commit.

The same argument holds per replica in replicated mode (design-cluster.md §5.2: rows before the `COMMIT` marker in
the keys are flushed through `F` first, rows after it were held by the fence).

**The check.** The reader verifies the property for every direct commit it delivers: no row in the commit's keys lies
between segment `F + 1` (the marker's `F`, §2.3) and the marker. Per subscription it keeps a summary of the rows it
read in the last four segments, in the fence's own shape (4096 buckets of series with a timestamp range each, 64 KiB a
segment); a commit's keys are tested against the summaries of the segments after `F`, and only a bucket hit (a real
violation or a bucket collision) rescans those segments exactly. A segment older than the summaries is rescanned. When
the check finds rows:
- it logs once per (volume, marker): the commit, the series and the positions;
- it counts `tsdb_feed_lww_violations_total` (an alert on any value above 0);
- it repairs: after the commit's events it sends, for every key, the newest such row as a `REASSERT` event, numbered
  from the marker's reserve (§2.3), so a mirror still ends with tsdb's state.

`tests/test_feed.c` and the jepsen checker require the counter to stay 0; a test hook that skips the fence
(`test_no_lw_fence`) must make it fire, which checks the check has teeth. The first version's `REASSERT_OFF` flag
and the source's `reassert` option are gone.

## 2. Engine changes

### 2.1 WAL record, version 2 (format 9, built in S0)

```
[u32 len | kind << 28][u32 header check][u64 hash][u64 fno][i64 commit_ms][payload]
```

32 bytes of header instead of 16 (F9). `commit_ms` is the wall clock of the group commit: the event time in Pulsar
and the feed's lag in milliseconds. The hash covers `fno`, `commit_ms` and the payload; the header check still binds
the record to its segment and offset.

- **kind 0, rows**: `len` = rows (below 2^28; `buffer_rows` defaults to 1 M); payload as today. Takes `len` numbers.
- **kind 1, marker**: `len` = payload bytes; the payload starts with `u16 type, u16 flags, u32 span` and takes `span`
  numbers (at least 1). A replay or a follower skips marker types it does not apply and continues the numbering;
  `requires=` in META (§5) keeps a binary from opening a volume with markers it would have to apply and does not know.

Marker types (S0 defines the payloads once, design-ha-modes.md §2.1, M15):

| type | payload | written by | used by |
|---|---|---|---|
| `COMMIT` | op (insert, replace, delete), the files (final name, size, xxh64, header checksum), `F`, the logical range (delete, replace) or the keys in the fence's form (one range, or the 4096-bucket table), rows, events, `reserve` | the feed (single server, shared mode); the cluster (replicated) | the feed's events, replica apply (cluster) |
| `DROP` | `wm_gen`, `dropped_before` | `tsdb_drop_before` | the feed; cluster apply |
| `RETENTION` | series, limit | `tsdb_set_retention` | the feed; cluster apply |
| `FEED_SLOT` | action (create, advance, invalidate, reset, drop), slot number, per action: name, owner, caps, `(event, check)` | slot ops (§2.7) | slots on every reader |
| `EPOCH`, `CLASS`, `SERIES`, `STATE`, `SLOT`, `NOW`, `COLD` | design-cluster.md §4.1 | the cluster | replicated mode only; the feed skips them |

`fno` is assigned in `wal_buffer` under `buf_lock`, like `wal_appended`; no new lock on the hot path. With the feed
off on a single server no markers are written; the 16 bytes and the numbering are paid anyway (F9), and the cluster
needs both.

**`fno` across opens.** MANIFEST records `fno_next` each time it is written (a flush publishes it before it unlinks
or renames segments), so the number continues even when no segment is left (feed off, clean close). At open, `fno`
= the larger of MANIFEST's `fno_next` and the end of the newest segment (`wal_` or `fwal_`), and, with the feed on,
`FEED`'s floor.

### 2.2 Retained segments

With the feed on, `wal_unlink_upto` (memtable.c:754), the replay at open and `wal_close` (memtable.c:1233)
**rename** `wal_<seq>.log` to `fwal_<seq>.log` instead of unlinking it. For the engine the segment is gone exactly as
if unlinked: replay and followers look only for `wal_` names (memtable.c:1072-1095), so `wal_removed`,
`wal_removed_synced` (moved by `eng_sync_dir` after a directory fsync, as for an unlink) and everything that relies on
them (empty merge outputs, `db->wtail` for class switches) keep their meaning. A rename that fails is handled like a
failed unlink today: the segment stays and `wal_removed` does not pass it.

A sealed `fwal_` segment is entirely durable: `mt_flush` writes and fsyncs the rest of the old segment before the
flush, and the flush's parts are synced before the rename. In replicated mode a segment's name already carries the
epoch (`(E << 40) | n`); the rename keeps it.

### 2.3 Direct commits: links, intents, markers

A direct commit is any `parts_commit` that is not a flush: large inserts, replaces and deletes, and the regenerated
ones of §2.4. With the feed on, on a single server:

1. **Links.** After the parts are numbered (under `publish_lock`), each pending `tmp_*` part gets a hard link
   `f<final name>` (`linkat`), before the intent. Merges later retire and delete the part's name; the link keeps the
   inode, and the feed reads what the commit wrote, not what later writes left of it.
2. **Always an intent**, also for one part (today only for several, part.c:904). Format `tsdb-intent 4`: the rename
   lines as now, then one `feed` line with the `COMMIT` payload (hex), then the `end` checksum. Its fdatasync and
   directory fsync make the links durable with it. The intent is the commit point, as today.
3. After the renames and their directory fsync, the intent is **renamed** to `fcommit_<block>` instead of unlinked
   (no wait for that rename: if it is lost, `intents_recover` finds the complete intent at open and renames it then).
4. **Marker.** Still under `publish_lock`, the committer appends the `COMMIT` marker to the WAL buffer under
   `buf_lock`, with `F` = `wal_flushed` and `S_c` = the active segment, both read under `publish_lock`, where flushes
   also commit. `span` = the commit's events (one per granule of its parts, plus one per range) + `reserve`, where
   `reserve` = rows in the active and flushing memtables (an O(1) bound on the `REASSERT` rows of §1.6; numbers are
   free, so the reserve costs nothing). Taking `buf_lock` under `publish_lock` follows the lock order; the marker
   takes no WAL writer role and no sync, which is what ha.md D3 objects to. Markers are therefore in block order, and
   a durable marker implies every earlier one is durable.
5. **Before `lw_leave`.** The marker is in the buffer before the commit's fence is lifted (insert.c:648-673,
   delete.c:103-120 call `lw_leave` after the publish), which is what §1.6 relies on.
6. The caller does **not** wait for the marker to be durable: the commit is durable through the intent, and a lost
   marker is rebuilt at open (§2.4). The marker becomes deliverable with the next group commit or flush.
7. `fcommit_<block>` is removed once `wal_durable` passes its marker (by the next flush; the marker carries the whole
   payload, F14).

Operations without parts: a delete that finds no rows, `tsdb_set_retention`, `tsdb_drop_before`. They append a marker
only and, in sync mode, wait for it (`wal_commit`) before returning, since nothing else would bring it back after a
crash. The retention marker is appended after the `RETENTION` append is synced; the drop marker after the partitions
are retired.

A merge never calls this path. Flushes call `parts_commit` with a flag that skips it.

In shared and replicated mode the commit point is not an intent (§2.9): the links are made with the pending parts,
and the marker follows the mode's commit.

### 2.4 Recovery at open

After `intents_recover` (which now also turns complete version-4 intents into `fcommit_` files) and the WAL replay
(which renames replayed segments to `fwal_`):

- **`fno` continues** (§2.1).
- **Lost markers are rebuilt.** A marker can be missing only if its segment was not yet sealed when the process died.
  The marker lies in `S_c` or `S_c + 1`: the active segment can rotate once between the commit and its marker, never
  twice, since a second rotation waits for the first flush, which needs `publish_lock`. For every `fcommit_` whose
  `S_c + 1` is at or after the first segment the replay found unflushed, the open looks for its marker in those two
  segments; the missing ones form a suffix in block order and are appended, in that order, at the start of the new
  segment, with `F` and the payload from the `fcommit_` file. Then the `fcommit_` files are removed (step 7).
- **Orphans go.** `f*.part` links that no retained marker or `fcommit_` names are removed (their insert was rolled
  back), as `tmp_*` files are now.
- **Slots** are loaded from `FEEDSLOTS` and the `FEED_SLOT` markers after its checkpoint (§2.7).

### 2.5 The reader (`src/feed.c`)

One implementation behind `tsdb_feed_*` (§3.7), used by tsdbd and embedded callers. Per volume it keeps:

- an index of retained segments: `seq -> first fno` (16 bytes per segment; 17,280 a day at the 5 s flush interval),
  built from the first record of each and kept up to date as segments are sealed; a record inside a segment is found
  by reading its record headers once (on resume only);
- the **deliverable position** `(seq, offset, next fno)` of F13, which the engine moves after each successful fsync
  (single server), at each ack-gate pass (shared mode) or commit-point advance (replicated mode), and signals through
  an eventfd when anyone subscribes (an `atomic_bool` keeps it free otherwise).

It reads with `pread` (never mmap: a file removed on another NFS host raises SIGBUS when mapped), in pieces of at most
1 MiB. Rows records: filter by series (a bitmap over the volume's local ids, built lazily: ids by membership,
selectors and rights by name the first time an id is seen, §3.2), group by series, emit in order of each group's last
`fno`. `COMMIT` markers: open the linked parts, walk the granule index (`gx_series`) to the selected series, decode
one granule at a time, emit the range first for delete and replace, then the check of §1.6.

A subscription to a few series still reads every row of its volume: about 1 ns a row on one core to skip, plus the
check's summary, so 10 M rows/s of ingest costs about 1-2% of a core per subscription at the head. Many subscriptions
on one hot volume would share one scan; that is a later optimization (L2), not needed for one source per volume set.

### 2.6 Retention of the feed, and the lag bound

Each volume's merge thread removes, oldest first, sealed segments whose last record's `commit_ms` is older than
`feed_retention_s` (default 24 h) or that lie beyond `feed_max_bytes` (default 64 GiB: the retained segments, the
`fcommit_` files and the bytes links alone keep alive, together), with four exceptions:

- never the newest segment;
- never a segment the §1.6 check of a retained marker needs (one below the smallest `F + 1` of the markers after it,
  usually one or two segments);
- never a segment holding events after a valid slot's durable position (§2.7);
- in replicated mode, never above the saved commit floor, and the cluster's catch-up retention (`repl_retain_mb`)
  is the same mechanism (design-cluster.md §8.3): a segment goes when no rule keeps it.

Removing a segment removes the links of the markers in it, after the segment (a crash in between leaves orphans that
§2.4 removes). The oldest kept event is the first `fno` of the oldest segment.

A consumer learns that it fell behind in three ways: `HEAD` and every `POS` frame carry the oldest kept event;
`FEED_INFO` reports it; and a subscribe or resume below it gets `FEED_GONE` with that number. The remedy is a resync:
`FEED_INFO` (the heads), a snapshot query of the selected series (`QUERY_MULTI`), then a subscription from those
heads; rows written in between are delivered twice, which upserts absorb. tsdb never blocks, slows or fails a write
because of a consumer.

### 2.7 Slots (F1, F11)

A **slot** is a named, durable consumer position. It holds the feed's retention back, up to a cap, on every volume it
covers.

| field | meaning |
|---|---|
| name | 1 to 128 bytes in the user-name alphabet (design-auth.md §2), unique in the database |
| number | assigned at creation, never reused (a slot dropped and created again under its name is a new slot) |
| owner | a user (tsdbd with login) who may subscribe with it and advance it; without login, none |
| volumes | all (`ALL_VOLUMES`: volumes added later too) or a list |
| caps | `max_age_s` (default 7 days), `max_bytes` per volume (default 256 GiB), both settable per slot |
| state | `active` or `invalid` (and why: age, bytes, free space) |
| per volume | the position `(event, check)`: every event up to it is consumed |

**Where it lives.** Changes are `FEED_SLOT` markers in each covered volume's WAL (create, advance, invalidate, reset,
drop), durable with the group commit they ride on. Each volume's engine applies them to its slot table and
checkpoints the table into `FEEDSLOTS` (two checksummed 4 KiB slots written alternately, in place like MANIFEST, never
renamed; the slot holding the higher `fno` wins): at most once per flush when a slot changed, without a sync, and with
an `fdatasync` before the cleaner removes a segment, so a checkpoint is durable before any marker it replaces can
go. At open, slots = the checkpoint + the `FEED_SLOT` markers after its `fno`. Followers do the same from the files.

The **registry** (name to number, owner, caps, volumes) is in the root volume's slot table on a single server: a
volume added later gets a create marker, at its head, for every `ALL_VOLUMES` slot (the add writes it before the
volume serves). In both HA modes the registry is in the metadata group, beside users (design-ha-modes.md M2): the
owner of each volume writes the create marker when it sees a registry entry (watch, or at claim).

**Create** (`FEED_SLOT_CREATE`): writes the registry entry, then a create marker in every covered volume at its head,
and in sync mode waits for them to be durable. The reply carries the start cursor per volume. Create is idempotent by
name with the same spec; a volume that is UNAVAILABLE (HA) makes the reply UNAVAILABLE with its number, and a retry
completes it. `RESET` (a flag of create) puts an existing slot back to `active` at the heads: a consumer's last step
of a resync.

**Advance** (`FEED_ADVANCE`, on the stream or on its own): per volume, a cursor that must be above the slot's position
and at or below the deliverable position, with a matching `check`; otherwise the advance is refused for that volume
(`FEED_RESET`) and the slot keeps its position. The engine keeps the newest advance per slot in memory and writes at
most one marker per slot per `feed_slot_sync_ms` (1 s) per volume, and none when the advance passes nothing but slot
markers (so an idle volume does not log a marker a second because the consumer acknowledges the previous marker).
A crash loses at most the last second of advances: the slot goes back a little, which only holds more and redelivers
duplicates (F6). The cleaner uses only positions whose markers are durable.

**The cap.** At each cleaner pass, per volume, a valid slot is **invalidated** when:
1. its position is older than `max_age_s` (by `commit_ms`), or
2. the volume's retained feed is above the slot's `max_bytes` and the slot is what keeps it there, or
3. the volume's filesystem has less than `feed_min_free_pct` (10%) free (`statvfs`), slots holding the most first,
   until the feed's own retention is all that is left.

Invalidation is an `invalidate` marker (with the reason) in that volume, a log line, and a metric; retention then
ignores the slot in that volume, and the next resume through it there gets `VOLUME_END FEED_GONE` with the flag
`SLOT_INVALID` and the oldest kept event. The consumer resyncs and resets the slot. Writes never wait for a slot, and
the disk never fills because of one: at worst a consumer resyncs.

**A stuck slot is visible before the cap.** Per slot and volume: `tsdb_feed_slot_lag_events`,
`tsdb_feed_slot_lag_seconds` (now minus the `commit_ms` at its position), `tsdb_feed_slot_held_bytes` (what retention
keeps only because of it), `tsdb_feed_slot_cap_ratio` (the larger of held/`max_bytes` and age/`max_age_s`),
`tsdb_feed_slot_connected` (a stream uses it), `tsdb_feed_slot_invalidated_total{reason}`. tsdbd logs at 50% and 90%
of the cap and when a slot has had no stream for an hour; `docs/metrics.md` gives the alert rules (cap ratio above
0.5; any invalidation; not connected for 15 minutes).

**Drop** removes the registry entry and writes a drop marker in every volume; retention forgets it.

**Followers and replicas.** An NFS follower and a replica read the slot table (checkpoint and markers) and let a
subscription start from a slot, but cannot change one: slot ops and `FEED_ADVANCE` on them get `TSDB_ERR_READONLY`.
A consumer that reads from a follower advances its slot on the writer (the source's `slotHosts`, §4.2).

**HA.** A takeover (shared mode) or a failover (replicated mode) keeps every slot: the markers and `FEEDSLOTS` are in
the volume (shared mode: on the shared storage, the WAL and `FEEDSLOTS` written through chained fds; replicated mode:
the markers replicate with the log, and every replica applies them and retains what they hold). A storage rollback can
take back advances (safe: the slot holds more) or a reset (the consumer resyncs on `FEED_RESET` anyway).

### 2.8 NFS followers (F5)

Followers (`--follow`, possibly on another host over NFS) serve the feed: the files are in the volume directory, and
a follower reads them like the writer. What a follower cannot know is how far the active segment is durable: over NFS
it may see bytes the writer has written and not yet fsync'd. So the writer rewrites a small `FEEDPOS` file (`seq,
offset, fno`, checksummed, in place like MANIFEST) at most every `feed_pos_ms` (default 50 ms), after an fsync and
with a value that fsync covered (in shared mode, a value the ack gate passed), and does not fsync `FEEDPOS` itself: a
lost update leaves an older, still true, value. Followers deliver sealed segments in full and the active one up to
`FEEDPOS`, lagging the writer by up to `feed_pos_ms + refresh_ms`. A segment removed by the writer's retention while a
follower reads it gives that volume's stream `FEED_GONE` (ESTALE), never a gap. Followers never write feed files.

### 2.9 The HA modes

**Shared mode** (one copy on NFS/ZFS, an owner per volume and epoch; ha.md, design-ha-modes.md):
- The feed's files (`fwal_`, links, `FEED`, `FEEDSLOTS`, `FEEDPOS`) live in the volume directory and move with the
  volume. `FEED`, `FEEDSLOTS` and `FEEDPOS` join the chained files (M3), so a deposed owner's writes to them are
  refused by the NFS server like its WAL writes.
- The commit point is the ledger's `COMMIT` record (ha volumes have no intents, ha.md:237); its payload is the shared
  `COMMIT` payload (F14), so there is no `fcommit_` file. Links are made with the pending parts and are durable with
  their directory fsync before `ledger_edit`. The marker is appended under `publish_lock` after `jr_commit`, before
  `lw_leave`, as on a single server; a lost marker is rebuilt at open from the ledger. **Ledger compaction keeps a
  `COMMIT` until its marker is durable** (a rule for N3/N4).
- The renames to `fwal_` and the cleaner's removals are namespace effects, recorded through the journal hook (M15's
  `SEG`, `PURGE`) at the call sites N4 builds.
- Delivery stops at the ack gate (F13). After M17's cold open the feed continues from what storage holds; a pool
  rollback that took back delivered records makes the `check` of a consumer's cursor fail: `FEED_RESET`, never reused
  numbers.
- `feed_id` and the feed setting are in the volume's metadata record; the owner writes `FEED` from it at claim.
- A stream on a volume that moves ends with `VOLUME_END MOVED`; the consumer resubscribes at the new owner (TOPOLOGY)
  with its cursors or slot, and nothing else changes.

**Replicated mode** (design-cluster.md):
- `fno` is the position; the WAL record of §2.1 is the replication unit; `check` is the same on every replica.
- The cluster's `COMMIT` marker, appended before `publish_lock` and durable on the ISR before it is applied (§5.2
  there), **is** the feed's marker: no intent, no `fcommit_`, no rebuild. Each replica links the parts it applies.
- Delivery stops at the commit point (F13).
- Every replica keeps `fwal_` segments under the feed's retention and the slots' holds, so a promoted replica has the
  history and the slots. With the feed on, a replica's disk holds the retained feed three times; `feed_max_bytes`
  is per replica.
- Replicas serve the feed to requests with `REPLICA_OK` (D5 there); a leader change ends a stream on the old leader
  with `VOLUME_END MOVED`, and the consumer resubscribes anywhere.

### 2.10 Fail-stop and damage

- Appending a marker can fail only for memory (the buffer); the WAL write that follows is fail-stop as today.
- A failed `linkat` or intent write before the commit point fails the call (nothing committed, as for `tmp_*` parts);
  the directory fsync after the intent is fail-stop already. The rename to `fcommit_` is not relied on (§2.3).
- A failed `FEEDSLOTS` write or fdatasync stops the cleaner's pass (nothing removed) and is retried; it is not
  fail-stop, since the markers it checkpoints are still in the retained segments.
- After `eng_fail` the feed delivers up to the last deliverable position and then ends every stream with
  `TSDB_ERR_IO`; tsdbd exits as today.
- A damaged retained segment or `fcommit_` (bad hash with a valid record after it, a read error) ends the volume's
  stream with `TSDB_ERR_CORRUPT` or `TSDB_ERR_IO` and a log line; it is never skipped. A torn tail exists only in the
  active segment and lies beyond the deliverable position, which the reader never passes. Granule payloads of linked
  parts are verified when `verify_checksums` is on, as for queries.
- The open does not fail on feed files except `FEED` and `FEEDSLOTS` (damage to both slots of the file fails the
  open, as damage to MANIFEST does): a damaged retained segment is the feed's problem (the stream reports it), not
  the database's.

### 2.11 Lock order

New: the marker append takes `buf_lock` inside `publish_lock` (already allowed). `feed_lock` (the reader's segment
index, the slot table and the cleaner) is a leaf, never held while an engine lock is taken; the cleaner takes
`dir_sync_lock` through `eng_sync_dir` only after releasing it. The reader takes no engine lock except to read
`wal_flushed` and the deliverable position (atomics) and the series dictionary (its own lock). A slot op appends its
marker like a small write (`buf_lock`, then the writer role for the sync), holding no other engine lock.

### 2.12 Costs

| | feed off | feed on |
|---|---|---|
| WAL, per group commit | +16 B (F9) | +16 B |
| small write latency | 0 | 0 (no extra fsync; an eventfd write per group commit only while someone subscribes) |
| large insert or delete of one part | 0 | +1 fdatasync of the intent + 1 directory fsync (~1.4 ms on the SN720, against 13.8 ms for 100,000 rows); none in HA modes, which commit through the ledger or the marker anyway |
| large insert of several parts | 0 | `linkat` per part (no extra fsync) |
| no-op delete, `set_retention`, `drop_before` | 0 | one WAL group commit |
| slots | 0 | one marker of about 64 B per slot per volume per second while it advances; a 4 KiB `FEEDSLOTS` write per flush when a slot changed |
| disk | 0 | retained WAL: 20 B/row + 32 B/record; links: the bytes of direct parts a merge replaced, for the retention; what slots hold, up to their caps |
| memory, engine | 0 | the segment index (16 B per retained segment), the slot table |
| memory, per subscription | | ≤ 1 MiB read buffer + 1 decoded granule + two bitmaps over the volume's series (announced, selected: 256 KiB at 1 M series) + the check's summaries (256 KiB) |

Retained WAL at 10 M rows/s for 24 h is 17 TB uncompressed: high ingest needs a short `feed_retention_s` or the
size bound, or compression of sealed segments, which comes later and only after measurement (F10, step L1: zstd over
the packed form of `client/tsdb_pack.h`, done by the merge thread, `fwal_<seq>.zst`, records and hashes kept).

### 2.13 Enabling (F4)

The feed is a persistent property of a database, off by default, not a server flag (a restart without the flag would
delete the history consumers rely on): `tsdb <dir> feed enable [--retention 24h] [--max-bytes 64G]` adds `feed` to
`requires=` in every volume's META, then writes a `FEED` file in every volume (`feed_id`, retention, the `fno` floor;
checksummed, synced with its directory); a volume added later gets both. `tsdb <dir> feed disable` removes the slots,
`FEED` and the retained files, then `feed` from `requires=`; a later enable makes a new `feed_id`, so old cursors get
`FEED_RESET`. Embedded: `tsdb_feed_enable(db, &opts)`, `tsdb_feed_disable(db)`. Refused with `wal_sync_ms = -1`. In
HA modes: `tsdb cluster feed enable`, which sets it in the metadata group (§2.9). The CLI also has `tsdb <dir> feed
info` and `tsdb <dir> feed slot add|drop|reset|list`.

## 3. Protocol 14, server and clients

### 3.1 Ops

`TSDB_PROTOCOL_VERSION` 14; the floor stays 12 (new ops only, as protocol 13 did): clients send these only to a
server whose HELLO says 14 or more, and an older server answers them `TSDB_ERR_INVALID`. The numbers follow
`QUERY_LATEST` (39) and are final when protocol 13 has landed.

```
FEED_SUBSCRIBE (40)  u8 flags, u8 0, u16 nsel, u32 nvol, u32 ncur, u16 slot_len, u16 0,
                     u32 volumes[nvol], [u32 0 to a multiple of 8],
                     cursors[ncur] x {u64 feed_id, u64 event, u32 check, u32 volume},
                     slot name, nsel x {u8 kind (0 all, 1 prefix, 2 name, 3 glob, 4 id), u8 0, u16 len, bytes or u64 id}
     flags: FROM_OLDEST 1 (volumes with neither a cursor nor a slot start at their oldest event; else at their head),
            ALL_VOLUMES 2 (also volumes added later), PACKED 8 (ROWS may use TSDB_PACKED). Other bits reserved.
     selection: the union of the selectors, filtered by the user's read rights (§3.2); none: every series.
     Volumes: those of the id selectors, cursors, the slot and `volumes`, all with ALL_VOLUMES. A cursor overrides the
     slot's position for its volume.
  -> a stream of REPLY frames with the request's id, each: i32 status, u8 kind, u8 flags, u16 0, u32 volume, ...
     HEAD (1)      u64 feed_id, u64 oldest, u64 next, u64 start, u16 plen, path         one per volume, first
     SERIES (2)    u64 id, u8 type, u8 sflags, u16 len, name, i64 retention_ms           sflags: GRANTED 1, REVOKED 2
     STATE (3)     u64 id, u32 code, u16 len, name
     ROWS (4)      u64 event, u64 group_last, i64 commit_ms, u64 id, u8 type, u8 vflags, u8 origin, u8 0,
                   u32 n, u32 0, i64 ts[n], values[n], [u8 valid[(n + 7) / 8]]   (packed: as INSERT_VALUES)
                   origin: 0 WAL, 1 BULK, 2 REPLACE, 3 REASSERT
     DELETE (5)    u64 event, u64 group_last, i64 commit_ms, u64 id, i64 start, i64 end, u8 origin (DELETE, REPLACE)
     RETENTION (6) u64 event, i64 commit_ms, u64 id, i64 limit_ms
     DROP (7)      u64 event, i64 commit_ms, i64 before
     POS (8)       u64 feed_id, u64 event, u32 check, u32 0, u64 next, u64 oldest
     VOLUME_END (9) the volume's stream ended with `status`: FEED_GONE (+ oldest; flag SLOT_INVALID), FEED_RESET,
                   CORRUPT, IO, MOVED, UNAVAILABLE; the other volumes go on
     END (10)      the subscription ended: STOPPED after CANCEL, IO after fail-stop, AUTH when the user was dropped,
                   READONLY when a follower stops following; last frame
     ADVANCED (11) u32 n, n x {u32 volume, i32 status}: the answer to a FEED_ADVANCE sent on the stream
FEED_INFO (41)       u32 volume (0xffffffff: all) -> u32 n, n x {u32 volume, u64 feed_id, u64 oldest, u64 next,
                     u64 bytes, i64 retention_s, u64 max_bytes}
FEED_ADVANCE (42)    u16 slot_len, u16 0, u32 n, n x cursor, slot name
     On a stream connection: answered by ADVANCED in the stream. On its own connection: an ordinary reply.
FEED_SLOT_CREATE (43) u8 flags (RESET 1, ALL_VOLUMES 2), u8 0, u16 name_len, u16 owner_len, u16 0, u32 nvol,
                     i64 max_age_s, u64 max_bytes (0: the defaults), u32 volumes[nvol], name, owner
                     -> u32 n, n x cursor (the start positions)
FEED_SLOT_DROP (44)  u16 name_len, name
FEED_SLOT_LIST (45)  -> u32 n, n x {name, owner, state and reason, caps, u32 nvol, nvol x {cursor, lag events,
                     lag ms, held bytes}}
CANCEL (24)          also names a FEED_SUBSCRIBE: the stream ends after its current frame with a POS and END STOPPED
```

New status codes: `TSDB_ERR_FEED_GONE = -18`, `TSDB_ERR_FEED_RESET = -19` (after `AUTH`; -10 to -15 stay with HA);
`TSDB_ERR_INVALID` when the feed is not enabled.

A connection that sent `FEED_SUBSCRIBE` carries that stream only; a later frame other than `CANCEL` and `FEED_ADVANCE`
closes it (a client bug). Lookups go on another connection; the stream itself announces names.

### 3.2 Rights (F12)

Rows of `auth_ops` (design-auth.md §5; `tests/test_auth_ops.c` fails without them):

| op | needs | series |
|---|---|---|
| FEED_SUBSCRIBE | a login; `read` on each series delivered; with a slot, its owner or a superuser | id selectors: each id, `DENIED` for the whole request as for queries; other selectors: per series in the stream, filtered |
| FEED_ADVANCE | the slot's owner or a superuser | (positions only) |
| FEED_INFO | a login with any right | (positions and sizes, no data) |
| FEED_SLOT_CREATE (and RESET) | a superuser, or `admin` on `prefix:P` where the slot's name starts with P and the owner is a user the caller manages (A9's rule for user names) | (none) |
| FEED_SLOT_DROP | as create, or the slot's owner | |
| FEED_SLOT_LIST | a login: superusers see every slot, others the slots they own or manage | |

- **Filtering.** The reader's selected bitmap is built through the rights map: a series is selected when a selector
  matches its name and `rights_of(user, name)` has `read`; the answer is cached per id like any rights check. A
  series the user cannot read is never announced and never delivered, whether it exists or not.
- **`DROP` goes to every stream**: it is volume-wide, names no series, and every mirror needs it.
- **Rights change mid-stream.** A stream is not "a request already running": it checks the user's stale flag at each
  pump quantum. On a change it rebuilds its selection from the new state; a series that lost `read` gets a `SERIES`
  frame with `REVOKED` and nothing more, one that gained it a `SERIES` frame with `GRANTED` and its events from then
  on (no history: a consumer that wants it queries). A dropped user's streams end with `END AUTH` and the connection
  closes.
- `--no-auth`: everything passes; slot owners are not checked; slot admin ops are allowed (they are not user admin).

### 3.3 tsdbd

- **Not on the workers.** A stream would hold a worker for hours. tsdbd starts `--feed-threads` (default 2) pump
  threads; each owns some streams and serves those with something to send and room to send it, a bounded quantum
  (1 MiB) each in turn. A pump sleeps on one eventfd that the engine's deliverable-position hook and the event loop
  (output drained) signal.
- **Backpressure without waiting.** A pump appends frames to the connection's output while it is under `OUT_CAP` and
  then **parks** the stream (it never calls `out_wait`); the loop's drain below `OUT_CAP / 2` wakes it. A parked
  stream costs its buffers and its position; when it resumes behind the head it reads from the files. A client that
  reads nothing for `send_timeout_s` is closed, as today. Writers never wait for a pump; pumps never take engine
  locks a writer waits on (§2.11).
- **Admission.** `--feed-max-subscriptions` (default 256): the next gets `TSDB_ERR_NOMEM`. The request frame counts
  against `--max-request-memory` while parsed, as every request.
- **CANCEL and FEED_ADVANCE** are read in `parse()` (as CANCEL is today, tsdbd.c:2343), outside the request queue,
  so they reach a stream at once; `cancel_multi` grows a branch for streams, and an advance is handed to the stream's
  pump, which answers with `ADVANCED`.
- **Fail-stop:** pumps send `END IO` and the server exits as today.
- **Metrics** (`server/metrics.c`, `docs/metrics.md`): streams, events and bytes sent, parked streams, per volume the
  head, oldest and retained bytes, the largest lag of a stream in events and in ms, the slot metrics of §2.7, and
  `tsdb_feed_lww_violations_total` (§1.6).

### 3.4 Frames, sizes, packing

Event chunks are at most 8192 rows, so a `ROWS` frame is at most about 8192 x 17 bytes plus headers, well under
`OUT_PIECE`; no frame is split. With `PACKED` the arrays are packed and zstd-compressed as `INSERT_VALUES` does
(tsdb_pack.h), which a WAN consumer wants; the default is unpacked (cheap for the server).

### 3.5 C client

```c
typedef struct tsdbc_feed tsdbc_feed;
int tsdbc_feed_subscribe(tsdbc *c, const tsdb_feed_sel *sel, const tsdb_feed_cursor *cur, size_t ncur, int flags,
                         tsdbc_feed **out);
int tsdbc_feed_next(tsdbc_feed *f, tsdb_feed_event *ev, int timeout_ms);   /* 0, TSDB_ERR_STOPPED at END, ... */
int tsdbc_feed_advance(tsdbc_feed *f, const tsdb_feed_cursor *cur, size_t n);  /* answered by an ADVANCED event */
int tsdbc_feed_cancel(tsdbc_feed *f);                                      /* reads up to END */
void tsdbc_feed_close(tsdbc_feed *f);
int tsdbc_feed_info(tsdbc *c, uint32_t volume, tsdb_feed_info **out, size_t *n);
int tsdbc_feed_slot_create(tsdbc *c, const tsdb_feed_slot_spec *s, int flags, tsdb_feed_cursor **start, size_t *n);
int tsdbc_feed_slot_drop(tsdbc *c, const char *name);
int tsdbc_feed_slot_list(tsdbc *c, tsdb_feed_slot_info **out, size_t *n);
```

The types are the embedded ones (§3.7); the connection is dedicated to the stream until END.

### 3.6 Rust, Python, Java

All four in the same commit (AGENTS.md "Protocol"), with `make test-clients` green:

- Rust: `Client::subscribe(sel, cursors, flags) -> Feed`, `Feed: Iterator<Item = Result<FeedEvent>>` plus
  `next_timeout`, `advance`, `cancel`; `Client::slot_create/_drop/_list`; `Db::feed`, `Db::slot_*` in the embedded
  crate.
- Python: `client.subscribe(...)` returns an iterator of `FeedEvent` (numpy arrays for ts and values) with
  `advance()`; `client.slot_*`; `Db.feed`, `Db.slot_*`.
- Java: `TsdbClient.subscribe(FeedSelection, List<FeedCursor>, int flags)` returns `FeedSubscription` with
  `FeedEvent poll(Duration)`, `advance(List<FeedCursor>)`, `cancel()`, `close()`; `slotCreate/Drop/List`;
  `EmbeddedDb.feed` and the slot calls the same.

Their tests compare the client's events with the embedded feed of the same directory, as they do for queries, and
run the rights cases of §3.2 with login on.

### 3.7 Embedded API (`tsdb.h`)

```c
typedef struct tsdb_feed_cursor { uint64_t feed_id; uint64_t event; uint32_t check; uint32_t volume; } tsdb_feed_cursor;
typedef struct tsdb_feed_sel {
    const tsdb_sid *ids; size_t nids;            /* no ids and no selectors: every series */
    const char *const *selectors; size_t nselectors;   /* "all", "prefix:P", "name:N", "glob:G" */
    const uint32_t *volumes; size_t nvolumes;    /* with TSDB_FEED_ALL_VOLUMES: all */
    const char *slot;                            /* NULL: none */
} tsdb_feed_sel;
typedef struct tsdb_feed_event {
    int kind;    /* TSDB_FEED_HEAD, _SERIES, _STATE, _ROWS, _DELETE, _RETENTION, _DROP, _POS, _VOLUME_END, _ADVANCED */
    int origin, status, type, flags;
    uint32_t volume;
    uint64_t feed_id, event, group_last, oldest, next, start;
    uint32_t check;
    int64_t commit_ms;
    tsdb_sid series;
    int64_t start_ts, end_ts, limit_ms;   /* DELETE, DROP (end_ts), RETENTION */
    size_t n; const int64_t *ts; const void *values; const uint8_t *valid;   /* valid until the next call */
    const char *name; uint32_t code;                                         /* SERIES, STATE */
} tsdb_feed_event;
int tsdb_feed_open(tsdb *db, const tsdb_feed_sel *sel, const tsdb_feed_cursor *cur, size_t ncur, int flags,
                   tsdb_feed **out);
int tsdb_feed_next(tsdb_feed *f, tsdb_feed_event *ev, int timeout_ms); /* 0; TSDB_ERR_STOPPED when timed out */
int tsdb_feed_fd(tsdb_feed *f);                                        /* readable when next will not wait */
int tsdb_feed_advance(tsdb *db, const char *slot, const tsdb_feed_cursor *cur, size_t n);
void tsdb_feed_close(tsdb_feed *f);
int tsdb_feed_info(tsdb *db, uint32_t volume, tsdb_feed_info *out);
int tsdb_feed_enable(tsdb *db, const tsdb_feed_options *o);
int tsdb_feed_disable(tsdb *db);
int tsdb_feed_slot_create(tsdb *db, const tsdb_feed_slot_spec *s, int flags, tsdb_feed_cursor *start, size_t *n);
int tsdb_feed_slot_drop(tsdb *db, const char *name);
int tsdb_feed_slot_list(tsdb *db, tsdb_feed_slot_info **out, size_t *n);
```

`tsdb_struct_size` learns the new structs and `tsdb_abi_version` goes up. The embedded feed also works on a read-only
or following handle (§2.8); slot changes there are `TSDB_ERR_READONLY`.

## 4. The Pulsar source

### 4.1 Shape

`ai.intellistream.tsdb.pulsar.TsdbSource implements Source<GenericRecord>` in the existing NAR: `pulsar-io.yaml`
gains `sourceClass` and `sourceConfigClass` beside the sink's, so one archive is both. It reuses the sink's connection
handling (`TsdbConnection`, `Backoff`, the `hosts` list, login, `ErrorClassifier`) and its selection
(`SeriesSelection`). One instance holds one subscription over its volumes: with `--parallelism N`, instance i takes
the volumes whose number is i modulo N (volume numbers are stable: VOLUMES, META). All instances share one slot and
each advances only its own volumes.

The source refuses any processing guarantee but `ATLEAST_ONCE` (F6), with the sink's message.

### 4.2 Configuration

| key | default | meaning |
|---|---|---|
| `hosts`, `port`, `user`, `passwordFile`, `connectTimeoutMs`, `retry*` | as the sink | tsdbd endpoints to read from (the writer, followers, or in HA modes any node: TOPOLOGY routes) |
| `slotHosts` | `hosts` | where slot ops and advances go when `hosts` are followers (single server with NFS followers) |
| `slot` | required | the slot's name; the user must own it (§3.2) |
| `createSlot` | `false` | create the slot at start if it does not exist (the user needs the right to) |
| `series`, `seriesPrefixes`, `seriesPattern` | all series | the sink's keys and meaning, added together. Names and prefixes go to tsdbd as `name:`/`prefix:` selectors; a pattern is applied in the source on top of a server-side `all`, once per series name and remembered, as in the sink |
| `seriesGlobs` | empty | `glob:` selectors, matched in tsdbd |
| `topic` | required | the default output topic |
| `topics` | empty | `{"acme.": "topic://acme/tsdb/points", ...}`: the longest matching prefix picks the topic |
| `format` | `avro` | `avro` (§4.3) or `json` (§4.4) |
| `start` | `head` | where a volume starts when the slot is new: `head` or `oldest`; afterwards always at the slot |
| `onGone` | `fail` | on `FEED_GONE` (also an invalidated slot): `fail` (stop the instance and alert) or `resync` (a `resync` message per volume, a snapshot of the selected series as `snapshot` messages, a slot `RESET`, then on from the heads) |
| `onReset` | `fail` | the same for `FEED_RESET` (a different history: an operator decides) |
| `maxInFlightRecords`, `maxInFlightBytes` | `50000`, `64 MiB` | records handed to Pulsar and not yet acknowledged; above, the source stops reading the socket, and tsdbd parks the stream |
| `advanceMs` | `1000` | how often the acknowledged positions are sent to the slot |

### 4.3 Avro messages (F7)

One message per `ROWS` or `DELETE` event (one per chunk of at most 8192 rows), key = series name (per-key order on
scalable topics and Key_Shared). The record (`ai.intellistream.tsdb.feed.Event`, shipped as
`connectors/pulsar/src/main/avro/Event.avsc` and printed in `docs/feed.md`):

| field | Avro type | meaning |
|---|---|---|
| `op` | enum `Op` {`UNKNOWN`, `rows`, `delete`, `replace`, `retention`, `drop`, `resync`, `snapshot`}, default `UNKNOWN` | what the record does |
| `series` | `["null", "string"]` | the name (null for `drop`, `resync`) |
| `type` | `["null", enum Type {UNKNOWN, float64, float32, int64, bool, state}]`, enum default `UNKNOWN` | the series' type, in the sink's type names |
| `ts` | array of `long` (`timestamp-millis`) | `rows`, `snapshot`, the rows of a `replace` |
| `value` | array of `["null", "double", "float", "long", "boolean", "string"]` | one per `ts`; state values as names; the sink's `value` |
| `start`, `end` | `["null", "long"]` | `delete`, `replace` |
| `limitMs`, `before` | `["null", "long"]` | `retention`, `drop` |
| `volume` | `int` | the dedup key, with `event` |
| `event`, `groupLast` | `long` | the event's number, its group's last |
| `feedId` | `long` | the volume's feed identity |
| `origin` | enum {`UNKNOWN`, `WAL`, `BULK`, `REPLACE`, `REASSERT`, `SNAPSHOT`}, default `UNKNOWN` | where the rows came from |
| `commitMs` | `long` (`timestamp-millis`) | the group commit's time; also the message's event time |

A replace is its `replace` record followed by `rows` records with origin `REPLACE`, the last one at `groupLast`.
`retention` goes to the series' topic with its key; `drop` and `resync` to the default topic, keyed `tsdb:<volume>`.
Properties repeat the dedup key for consumers that do not decode: `tsdb-volume`, `tsdb-event`, `tsdb-feed-id`.

**Schema in Pulsar.** Each record's `getSchema()` is `Schema.AVRO` of the class generated from `Event.avsc`, so the
broker registers it on the topic at the first publish and rejects producers with an incompatible one. The operator
sets the namespace's compatibility strategy to `FULL_TRANSITIVE` (docs/feed.md), so old consumers read new records
and new consumers read old ones. **Evolution rules**, kept by every later version of the source: new fields only
with a default (unions with `null` first, default `null`); never remove, rename or retype a field; enum symbols only
appended, every enum with the default `UNKNOWN`; the schema's `doc` names the source version. A change outside these
rules is a new record name and a new topic. The producer compresses batches with ZSTD (`compressionType`), since
`value` is a union per element (about 15 bytes a row before compression).

### 4.4 JSON (`format: json`)

The same fields as JSON objects, in the sink's input shape for rows (`{"series": ..., "type": ..., "ts": [...],
"value": [...]}`), so a mirror over a schema-less topic works too.

### 4.5 Positions, restart and failover

- **What is stored.** Per volume, the cursor of the last `POS` all of whose records Pulsar acknowledged
  (`Record.ack()` comes when the broker persisted the message). The source keeps, per volume, the records in flight
  in event order and moves the cursor to a `POS` once every record before it is acknowledged; every `advanceMs` it
  sends `FEED_ADVANCE` with those cursors on the stream (or to `slotHosts`). The slot is the only store (F15).
- **tsdbd restarts or a connection breaks:** the source reconnects with backoff and resubscribes from the last `POS`
  it **received** (its in-memory position, since everything before it is already with the producer), so nothing is
  sent twice.
- **The source restarts** (crash, upgrade, rescheduling): it subscribes with the slot. Events between the slot's
  durable position and what Pulsar already has are sent again, identical (F6). After a tsdbd crash the slot may also
  be up to a second behind.
- **`Record.fail()`**: the source stops (an exception from `read()`), the runtime restarts it, and it resumes from the
  slot; order is kept because nothing after the failed record was advanced.
- **Takeover or failover (HA):** `VOLUME_END MOVED` or a broken connection; the source resubscribes through TOPOLOGY
  with its received positions, or with the slot after its own restart. The slot moved with the volume (§2.7).
  `FEED_RESET` (a rolled-back history) stops it unless `onReset = resync`.

### 4.6 Delivery semantics end to end

- **At least once** (F6). Duplicates come only after a restart of the source or of tsdbd, each an identical copy of
  an earlier message with the same `(volume, event)`; a consumer drops a message whose `event` is not above the last
  it applied for that volume (or key). Order per key holds: a key's messages go through one producer, in order.
- **No gaps:** the slot never passes an unacknowledged record; retention keeps what the slot holds up to its cap; a
  `FEED_GONE` stops the source (or resyncs, explicitly, with control messages in the topic), never skips.
- **Ordering across topics** (two prefixes to two topics): none, as across volumes.

### 4.7 The sink as the mirror's other half

The sink learns the `Event` record (step F9), in Avro and JSON, keeping its existing point shapes:
- `rows` and `snapshot`: its existing "one series with arrays" path; `type` creates the series as today.
- `delete`: flushes the series' pending points, then `DELETE_RANGE` (the sink's user needs `delete`).
- `replace`: buffers the group up to `groupLast` and sends one `REPLACE_RANGE`; a group above `maxReplaceRows`
  (default 10 M rows) is applied as a delete and inserts, not atomically, with a log line and a counter.
- `retention`: `SET_RETENTION`.
- `drop`: counted and skipped (no wire op applies it; a mirror keeps the rows, as with expiry), with a log line.
- `resync`: logged; the snapshot that follows is applied as upserts.
- `UNKNOWN` (a newer source): dead-lettered.

The sink needs no deduplication: applying a key's events in order, duplicates included, ends in the same state
(§1.4). Its own rule stays: `ATLEAST_ONCE` only.

### 4.8 Metrics

Through the source context: `tsdb_source_events`, `tsdb_source_rows`, `tsdb_source_bytes`, `tsdb_source_lag_events`
(head minus the received position, from `POS`), `tsdb_source_lag_ms` (now minus the `commit_ms` of the last event),
`tsdb_source_retention_margin_events` (received position minus the oldest kept), `tsdb_source_inflight`,
`tsdb_source_reconnects`, `tsdb_source_resyncs`, `tsdb_source_advance_age_ms`. The slot's own metrics are tsdbd's
(§2.7).

### 4.9 Build and tests

As the sink: `mvn package` builds the one NAR; `mvn test` runs unit tests (a fake feed: cursor bookkeeping with
out-of-order acks, chunk-to-record mapping, the Avro schema against `Event.avsc` and the evolution rules (a test that
reads records of the previous schema version), topic choice, selection parity with the sink) and tests against
`build/tsdbd` started with `--port 0 --port-file` and login on: kill -9 of tsdbd during a stream, resume from the slot
without gap; `FEED_GONE` with a tiny `max_bytes`; deletes and replaces as `op` records.
`connectors/pulsar/test/integration.sh` gains a source mode (`MODE=source`, Podman, Pulsar 5.0.0 pinned by digest,
bound to 127.0.0.1): writers into tsdbd A, the source into a scalable topic with an Avro schema, the sink from it into
tsdbd B, faults (kill -9 of tsdbd A, of the source, of the sink; tsdbd A stopped for `STOP_S`; a broker restart), then
**A and B must hold the same rows** for the selected series, and every event must be in the topic at least once, each
copy identical.

## 5. S0: what of format 9 the feed needs

S0 (design-ha-modes.md §7, design-cluster.md step 0) is one change before 1.0. The feed needs these pieces of it, and
they are all of its disk format apart from the files behind `requires=feed`:

| piece | the feed uses it for | others |
|---|---|---|
| WAL record v2 (§2.1): 32-byte header with `fno`, `commit_ms`, `kind`; markers with `type, flags, span`; replay and followers skip markers and keep numbering | positions, cursors, event time, all markers | the cluster's replication unit |
| `fno` assigned in `wal_buffer`; MANIFEST's `fno_next` (§2.1) | numbers that never go back across opens | the cluster's positions |
| Marker payloads `COMMIT`, `DROP`, `RETENTION` (one encoder, M15) and the type numbers of `FEED_SLOT` and of the cluster's kinds, reserved | the feed's events; the ledger and the cluster share `COMMIT` | M15 |
| META `requires=` (an open refuses a feature it does not know) | `requires=feed` (§2.13): a binary without the feed never opens a database with feed files (it would remove links as orphans and rename nothing) | `nfs-ha`, `cluster` |
| Status codes -18, -19 reserved | `FEED_GONE`, `FEED_RESET` | |

Not the feed's (S0 builds them for HA): the part header's position range (`fno_lo, fno_hi`; the feed names parts
through markers and does not read it), the wire bodies (MOVED, UNAVAILABLE, TOPOLOGY, STATUS), `EPOCH` and the
cluster's other marker kinds. The feed's own pieces come later and change no format: `tsdb-intent 4`, `fwal_`,
`fcommit_`, links, `FEED`, `FEEDSLOTS`, `FEEDPOS`, all behind `requires=feed`.

Two points for S0 to settle with design-cluster.md: §4.1 there says every marker takes one number, where the `COMMIT`
of §2.1 takes `span` (the events it expands into: the cluster only needs the first); and MANIFEST's `fno_next` is new
to both designs.

**Migration note** (S0's to write; 0.9.0's rules, compatibility.md): format 9 changes the WAL record, MANIFEST and the
part header. A format-8 database is refused with the message that names both versions; the way over is the export
and import of `docs/export-format.md`, which always works. If S0 keeps format-8 part headers readable (a version in
the header), an in-place `tsdb <dir> upgrade` is cheap: it refuses unless the database was closed cleanly with no WAL
segment left, then writes MANIFEST and META version 9 (open question 5).

## 6. Test plan

### 6.1 Engine and library (`tests/test_feed.c`, in `make test`, `make asan`, `make tsan`)

- Every event kind from every path: appends, small and large inserts (one and several partitions), replaces, deletes
  with and without rows, writes into the past (an old partition, a long class), `set_retention`, `drop_before`.
- **Merges are not forwarded:** a feed read before and after `tsdb_optimize`, idle merges and class switches is the
  same; merges of parts whose links the feed holds leave the feed's rows intact.
- **Last write wins (§1.6):** the `test_precommit` hook appends small writes to the same keys while a large insert, a
  delete and a replace are in flight; the feed applied to a model equals the database, and
  `tsdb_feed_lww_violations_total` stays 0. With `test_no_lw_fence` the counter rises and the model still equals the
  database (the repair works); without the repair (a second hook) the model differs (the test has teeth).
- **Recovery:** with crashfs, power loss at every step of a direct commit (links, intent, its fsync, renames, the
  rename to `fcommit_`, the marker, the `fcommit_` removal); after reopen the feed has the commit iff the database
  has it, once, at one position, and the positions of everything delivered before the crash are unchanged. crashfs
  learns `linkat`.
- **Cursors:** resume at every `POS` of a run gives the same suffix; a cursor of another `feed_id`, past the head, or
  with a wrong `check` (a copy of the directory that took other writes) is `FEED_RESET`; below the oldest is
  `FEED_GONE`.
- **Retention:** by age and size, never the newest segment, never one the §1.6 check needs; orphans after a crash
  between segment and link removal.
- **Slots:** a slot holds retention past age and size; its cap by age, by bytes and by free space (a hook for
  `statvfs`) invalidates it, retention then catches up, and the resume gets `FEED_GONE` with `SLOT_INVALID`; reset;
  drop; advances coalesced and the idle-volume case writes no marker; power loss at every step (create, advance,
  checkpoint, cleaner) never loses a durable create and never lets retention pass a durable position; a volume added
  later gets `ALL_VOLUMES` slots; a follower reads slots and refuses changes.
- **Invariants that must not move:** `wal_removed_synced` with renames (empty merge outputs still go, test_merge),
  class switch `wtail` (test_classes), WAL replay skips markers (test_wal), followers skip markers, `fno` never goes
  back across opens with the feed off and on.
- **Sync modes:** with `wal_sync_ms = 50` nothing is delivered before its sync (a hook that blocks the sync).
- **Followers:** a follower's feed equals the writer's; it never passes `FEEDPOS`; retention under it gives
  `FEED_GONE`.
- **Many series:** 1 M series in a volume, a prefix subscription to 100 of them, bitmaps bounded.
- `tests/test_levels.c` is not affected (no codec change).

### 6.2 Server (`tests/test_server.c`, the auth tests, `make fuzz-frames`)

- Rights: a user with `read prefix:acme.` subscribing to `all` sees only acme's series; an explicit id outside is
  `DENIED`; a revoke mid-stream stops a series (`REVOKED`), a grant starts one (`GRANTED`), a user drop ends the stream
  (`END AUTH`); slot ops by owner, tenant admin, other users and superusers per §3.2; `test_auth_ops` covers the six
  ops.
- A consumer that never reads: writers' latency unchanged, tsdbd's memory flat, the connection closed after
  `send_timeout_s`. `FEED_ADVANCE` and `CANCEL` on a parked stream.
- `make fuzz-frames` gets FEED_SUBSCRIBE, FEED_ADVANCE and slot inputs (selectors, cursors, slot names, CANCEL).

### 6.3 Jepsen: workload `feed` (`make jepsen JEPSEN_WORKLOADS=feed`)

Clients write as the `kv` and `classes` workloads do (unique value ids; small and large batches, replaces, deletes,
writes into the past) while three consumers subscribe: one stays up across faults (reconnects from its received
position), one is killed and restarted from cursors saved at random older `POS` frames, one uses a slot and advances
it. Nemeses: kill, power (crashfs, with `CRASHFS_TEAR`), pause, eio; also with `--auth`. The server runs with small
partitions, flushes every 500 ms, idle merges after 1 s, `wal_sync_ms` 0 and, in a second run, 50. The checker:

1. **No gap:** every acknowledged write's rows (and every acknowledged delete's range) appear in the feed of each
   consumer that subscribed before it.
2. **No phantom:** every row in the feed comes from a write that began; a definitely failed write is never in it.
3. **Durability:** every event delivered before a crash is in the feed after it, with the same number and content.
4. **State equality:** at the end, the feed applied to an empty model equals the database's final state (queried),
   for every consumer; and at every `POS` the model is a state the history allows (rule 1 of jepsen.c, per key).
5. **Determinism and order:** event numbers strictly increase per volume; a restarted consumer receives, from its
   cursor on, exactly what the first delivery had there.
6. **No forwarding of merges and expiry:** the number of row events equals the rows of applied writes;
   `tsdb_feed_lww_violations_total` is 0.
7. **Lag bound:** with a tiny `feed_max_bytes`, a stopped consumer without a slot gets `FEED_GONE`, never a silent
   gap; the slot consumer never gets `FEED_GONE` below its cap and always gets it (never a gap) above.

### 6.4 HA (with design-ha-modes.md's jepsen-ha and design-cluster.md's jepsen-cluster)

The `feed` workload runs under `make jepsen-ha` (owner kill, takeover, pool rollback) and `make jepsen-cluster`
(leader kill, partition, promotion): a slot consumer resumes on the new owner or leader without a gap; nothing
delivered is taken back by a failover (no uncommitted record delivered in replicated mode); a storage rollback gives
`FEED_RESET`, never reused numbers.

## 7. Alternatives considered and rejected

| alternative | why not |
|---|---|
| Tail the WAL only | Large inserts never reach it; tombstones live only in parts; segments are unlinked once flushed, so a position would not survive a flush. |
| Poll with queries | Cannot see deletes or overwrites, has no position, and writes into the past need a scan of all time. |
| Decode MANIFEST changes (new parts) | A flush part holds WAL rows seconds old; a merge output is not a write but looks like one; a delete's logical range is lost; after a merge the commit's own rows are gone, so no resume. |
| A FEED log with a copy of every write | Doubles WAL bytes and adds a second fdatasync to every group commit (0.7 ms on the SN720); large inserts written twice. Kept as the plan for an object-store backend, which has no hard links. |
| Order by block numbers, delivering small writes only once flushed | Small writes would wait up to `flush_interval_ms` (5 s). Not needed since the fence made position order exact (§1.6). |
| Hold direct parts from merging until the feed is past them (instead of links) | Parts pile up for the retention period, and queries slow down with them; links cost only disk. |
| Slots without a cap (PostgreSQL's replication slots) | An abandoned slot fills the disk and stops the database. The cap invalidates the slot instead (F1). |
| Slot positions in a file rewritten with `fdatasync` at every advance | Not replicated in replicated mode and not seen by followers through the WAL; markers are all three (F11). |
| Slots and their positions in the metadata group | One Raft commit per second per slot per volume; the registry alone is there (F11). |
| Positions in Pulsar's state store | One more store, unverified on Pulsar 5 workers; the slot is enough (F15). |
| `EFFECTIVELY_ONCE` through broker deduplication | Unverified on Pulsar 5 and scalable topics; the sink refuses it already; the dedup key makes duplicates harmless (F6). |
| Forward expiry | §1.1. |
| Markers synced under `publish_lock` before the commit | Takes the WAL writer role inside `publish_lock`, against the lock order (ha.md D3). The intent (or ledger record) is already a durable record of the commit; the marker only places it. |
| A tee in the Pulsar sink (publish what it writes) | Misses every write that does not come through the sink, and deletes; no order with other writers. |
| A sidecar process tailing the files | Cannot know what is durable; with `FEEDPOS` it is what a follower does. |

## 8. Build plan

Each step is a track (build, adversarial review, fixes; `.claude/skills/tsdb-tracks`) with the gates of AGENTS.md:
`make test`, `make asan`, `make tsan` for every step, `make jepsen` for anything that changes how data reaches the
disk, `make test-clients` and `make integration-tests` for client changes. Track-days as in design-ha-modes.md §7.

| step | content | gates beyond `make test`, asan, tsan | track-days |
|---|---|---|---:|
| P0 | **Pulsar 5 probe** (Podman, Pulsar 5.0.0): a source publishing `Schema.AVRO` records on a scalable topic registers the schema; `FULL_TRANSITIVE` refuses an incompatible change and accepts an added field with a default; the sink reads the records as `GenericRecord` (arrays of unions included); ZSTD batches. Hard links and `statvfs` on the production NFS mount go into N0's probe | none (a report) | 0.5 |
| S0 | **Format 9** (design-ha-modes.md §7), with the feed's pieces of §5 and the migration note; shared with HA | jepsen (all workloads), check-version | (HA's 2-3) |
| F1 | `FEED` file, `requires=feed`, enable and disable (CLI, embedded), retained segments (rename at flush, open, close), the retention cleaner without slots; the `wal_removed` and `wtail` regression tests | jepsen kill, power | 1-2 |
| F2 | Direct commits: intent v4 always, links, `COMMIT` markers before `lw_leave`, `fcommit_` lifetime, markers for no-op deletes, retention and drop; recovery and orphans at open; crashfs `linkat` | jepsen power, eio | 2-3 |
| F3 | Reader `src/feed.c`, the embedded API, the deliverable-position hook, the §1.6 check with its counter and hooks; `tests/test_feed.c` | jepsen kill (a feed smoke check) | 2-3 |
| F4 | Slots: markers, `FEEDSLOTS`, registry in the root volume, caps and invalidation, coalesced advances, the cleaner's holds, slot metrics hooks, CLI | jepsen power, eio | 2-3 |
| F5 | NFS followers: `FEEDPOS` writer and follower reading, slots read-only | follower tests | 1 |
| F6 | Protocol 14 and tsdbd: the six ops, `auth_ops` rows and the rights of §3.2, pumps, admission, `CANCEL` and `FEED_ADVANCE` in `parse()`, metrics, `docs/metrics.md`; server and auth tests, fuzz seeds | `fuzz-frames` 600 s, `test_auth_ops` | 2-3 |
| F7 | Clients: C, Rust, Python, Java and the embedded bindings, one commit | `test-clients`, `integration-tests` | 2 |
| F8 | Jepsen `feed` workload and checker (§6.3), with `--auth` | `make jepsen JEPSEN_WORKLOADS=feed`, 300 s per nemesis | 2 |
| F9 | Pulsar source (Avro and JSON), the sink's `Event` reader (§4.7), `mvn test`, `integration.sh MODE=source` (the mirror check) | `mvn test`, `integration.sh MODE=source` | 3 |
| F10 | Docs: `docs/feed.md` (operators: enabling, retention, slots, alerts, the Avro schema and its evolution), `CHANGELOG.md` (protocol 14, `requires=feed`), AGENTS.md invariants (retained segments are not WAL; markers in block order and before `lw_leave`; delivery only up to the deliverable position; links; slot caps), the connector README | none | 1 |
| H1 | Shared mode (after N4 and N7): the feed's call sites in the journal hook, `COMMIT` from the ledger, the compaction rule, chained `FEED`/`FEEDSLOTS`/`FEEDPOS`, delivery at the ack gate, registry in the metadata group, `MOVED` on takeover | `make jepsen-ha` with the feed workload | 2-3 |
| H2 | Replicated mode (after R3): the cluster's `COMMIT` as the feed's marker, delivery at the commit point, retention on every replica, `REPLICA_OK` streams | `make jepsen-cluster` with the feed workload | 2 |
| L1 | Later, by measurement (F10): compressed sealed segments, once the production ingest rate is known and a benchmark shows what 64 GiB holds | benchmarks on a quiet machine, several runs, the mean | 2 |
| L2 | Later: one shared scan per hot volume for many streams | tsdbc-bench with 64 streams | 2 |

Order: S0 lands with the HA work before 1.0; P0 any time before F9; F1-F10 follow S0 on one track (F5 and F7 can run
beside F6 and F8); H1 and H2 after their HA steps. F1-F10 sum to about 18-23 track-days.

**Size, by area (estimate, lines):** engine (WAL, commits, recovery, retention, slots) 2,000 + reader 1,200 + tests
2,000; tsdbd and rights 1,100 + tests 600; protocol header 200; clients C 500, Rust 600, Python 300, Java 700,
embedded bindings 500; Pulsar source 1,300 + sink reader 300 + tests 1,000 + script 250; Jepsen workload and checker
900. About 13,500 in all, half of it tests.

## Open questions for the user

1. **Slot caps by default: 7 days, 256 GiB per volume, and invalidation below 10% free space?** Recommended: yes;
   each slot can set its own age and bytes, and the free-space floor is a database setting.
2. **Who may create slots:** superusers and tenant admins for slot names under their prefix (§3.2, as for user
   names), or superusers only? Recommended: both, so a tenant runs its own source without a superuser.
3. **When the last-write-wins check fails** (§1.6): repair with `REASSERT` events and alert (recommended: a mirror
   stays right while the engine bug is found), or end the volume's stream with `CORRUPT`?
4. **`drop_before` in mirrors:** the sink skips `drop` (recommended: no wire op for it in protocol 14; mirrors keep
   the rows, as with expiry), or add a superuser `DROP_BEFORE` op so the sink can apply it?
5. **Format 8 to 9:** export and import only, or also an in-place `tsdb <dir> upgrade` for a cleanly closed
   database (recommended, if S0 keeps format-8 part headers readable: production data then needs no export)?
6. **Rights changes on a running stream:** applied at once with `GRANTED`/`REVOKED` (recommended: a revoke must reach
   a stream that lives for days), or only at the next subscribe?

## Unverified assumptions

- Pulsar 5's function runtime registers a source's `Schema.AVRO` on scalable topics (`topic://`) and enforces the
  namespace's compatibility strategy there; the sink reads such records as `GenericRecord`. (Step P0.)
- Hard links on the production NFSv4 mount to ZFS behave as on a local file system (link count, a link kept after
  the other name is removed, visible to other hosts with `lookupcache=positive`). To be added to N0's probe
  (design-ha-modes.md §7).
- `statvfs` on the production NFS mounts reports the ZFS dataset's free space and quota, so the free-space cap sees
  what the pool sees. (N0's probe.)
- The production ingest rate decides whether the uncompressed retained WAL fits 64 GiB for 24 h; not measured (F10).
- The rotation argument of §2.4 (a second rotation waits for the first flush, which needs `publish_lock`) was checked
  at 87eb49e; F2 re-checks it against memtable.c at 0683460.
