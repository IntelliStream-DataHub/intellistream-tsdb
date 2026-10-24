# tsdb HA: one writer per volume, with failover over NFSv4

Status: design, not implemented. Scope: shared storage on NFSv4.1/4.2 over RDMA. An object-store backend (intellistream-objects) is planned for later, behind the interface in §12, and is not built now.

Code references are `file:line` at HEAD `d52afaa`. A `~` before a line number means it comes from the research map at `d2c2767` and may be a few lines off.

## Goals and non-goals

**Goals**
- **No acknowledged write is lost** by any single fault: a process crash, a node crash, a partition, a pause, or an NFS server restart.
- **Never two writers for the same data.** At most one *committing* writer per volume at any time. Writers that come out of a fault late can do no harm.
- **Automatic failover per volume.** A volume (one directory, `src/volumes.c`) is the unit of ownership. N volumes have N independent writers spread over the nodes.
- **Inserts stay as fast as today.** The small-insert hot path is still one `write` plus one `fdatasync` per group commit. There is no synchronous replication, no per-node copy of the data and no consensus protocol. The NFS server is the only arbiter.

**Non-goals for version 1**
- HA of the NFS server itself.
- Warm (in-place) promotion, read-your-writes tokens, and a forwarding proxy for old clients. These are phase 2.
- The object-store backend.

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| D1 | The unit of ownership is the volume. Each volume has its own lease, epoch and failover. | One writer for the whole database | The user wants several writers. Series already route to volumes. |
| D2 | The arbiter is an NFSv4 **OFD write lock** (`F_OFD_SETLK`, whole file) on `<vol>/LOCK`. This is subject to the step-0 probe (U1, U21). | `flock`; POSIX `F_SETLK`; Raft/etcd | `flock` becomes a server lock only without `local_lock`. Any `close()` of the file drops a POSIX lock. The user ruled out an external coordinator. |
| D3 | A per-volume **ledger** inside `LOCK`, written through the lock fd, is the only commit log for namespace effects (part commits, flushes, merges, drops, purges, segment creation). WAL rows stay in the WAL. | Edit records in the WAL group commit | Putting COMMIT records in the WAL from inside `publish_lock` would invert the documented lock order. `mt_flush` leaves the writer role before it takes `publish_lock` (memtable.c:557-566). |
| D4 | Block numbers and `wal_seq` are `(E<<40)\|n`. Temporary, segment and MANIFEST names carry E. | Directory listing as the truth | Files a stale writer creates are always older and never collide. Name formats stay the same. |
| D5 | **The filesystem picks the backend, not the caller's flags.** Every writer open of a format-3 volume on NFS (library, `tsdb-cli`, `tsdbd`, HA or not) uses the NFS backend: OFD lock, chained fds, ack gate, and a preflight check. Local filesystems use the `flock` backend. A volume whose META says `ha=1` refuses writer opens that are not managed by an HA manager. | An HA-only commit path; trusting `--ha` | A writer without fencing on NFS breaks the single-writer guarantee (finding 2). |
| D6 | **Edit discipline.** Each ledger edit is validated, appended, fdatasynced, folded into the in-memory ledger state (`lstate`) and applied in memory, all under `ledger_lock`. A checkpoint is always a serialization of `lstate` taken under `ledger_lock`. | Checkpoints built from `db->parts`; merges outside the ordering | Concurrent MERGE, DROP and PURGE would otherwise drop records or contradict each other (findings 4, 9, 10). |
| D7 | **fd lifecycle.** Commit fds belong to a *grant* object owned by the HA slot. Abandon and release poison them with `dup2`, never with `close`, until every engine thread has drained. | Closing fds in `eng_abandon` | A reused fd number would carry a stale thread's write into a live lock, possibly another volume's (findings 1, 8, 19). |
| D8 | Promotion in version 1 is a cold open of a writer `eng` next to the existing one. | In-place role flip | It has the smallest surface for half-promoted states. Node faults are bounded below by the 10 s lease anyway. |
| D9 | CATALOG ids stay dense, appended under an OFD lock. Names are unique across **all** volumes' SERIES, orphans included. | Per-volume hashed ids | Hashed ids break explicit placement and cap the number of volumes. |
| D10 | MANIFEST is a hint for followers only. There is **one file per epoch** (`MANIFEST.<E>`), rewritten with O_DIRECT and no `ftruncate`, and it lists live segments explicitly. | One MANIFEST rewritten in place; an fsynced checkpoint on every publish | A killed owner's in-flight O_DIRECT write can only hit a file nobody reads any more (finding 5). Listing segments explicitly fixes the follower gaps at epoch boundaries (findings 14, 16). |
| D11 | The safety argument uses no clock. The ack gate and a resumable SUSPECT state are defense in depth. Every runtime deadline uses `CLOCK_MONOTONIC`. | A clock-based fence; wall-clock deadlines | Safety must not depend on clocks (finding 6). |
| D12 | Standbys poll with a non-blocking `F_OFD_SETLK` on dedicated HA threads, ranked over candidates only, and fall back to polling by everyone when the owner's heartbeat goes stale. They never call `F_GETLK`. | `F_OFD_SETLKW`; `F_GETLK` probes | A LOCKT expires a courtesy holder. CB_NOTIFY_LOCK over RDMA is unverified. |
| D13 | Insert frames carry `(vol, epoch, seq)`. After any non-OK reply the server refuses later frames of that session and volume until the refused one is resent. | Epoch stamp alone; sticky MOVED | SUSPECT→OWNER within one epoch would otherwise reorder a pipeline (findings 7, 22). |
| D14 | A partition between clients and the owner, with NFS still reachable, does not cause failover. | Client reachability votes | The NFS server is the only arbiter. |
| D15 | HA refuses `fsync=0`, `wal_sync_ms≠0` and `obsolete_part_lifetime_s < 120`. | Allowing them | With those settings an ack comes before durability (tsdb.h:~64-66). |
| D16 | The lock probe on the real NFSv4.2/RDMA mount is step 0. | Testing last | D2 and several assumptions depend on it. |
| D17 | The systemd watchdog measures CPU-side progress only. Slow NFS leads to SUSPECT, not to a kill. A file stuck while the server is otherwise healthy leads to a kill, then escalation. | Watchdog on NFS round trips | An NFS stall must not turn into a cluster-wide kill storm (finding 17). |
| D18 | Standbys do not tail WALs. Followers are opened lazily. Reads go to the owner by default. | Every node following every volume | That costs O(N × ingest) NFS reads for nothing (finding 23). |
| D19 | tsdbd admits requests per volume: a bounded number in the engine per volume, per-volume queues, and a separate read pool. | One shared FIFO of blocking workers | One stuck volume would otherwise starve all the others (finding 18). |

## 1. Roles and topology

**Nodes.**
- Run one `tsdbd --ha --node-id N --advertise host:port --volumes all|list` per host.
- Every node mounts the export at the same path, because VOLUMES stores `realpath` (volumes.c:~118-126).
- Every node has a unique `nfs.nfs4_unique_id`.
- The candidate set of each volume comes from a `NODES` file in the root, written by `tsdb-cli ha init` and `ha add-node`, filtered by each node's `--volumes`.

**Per-volume slot in `tsdb`:** `{eng *follower_or_null, eng *writer_or_null, grant *g, role, epoch, pins}`. Pins are needed because `insert_slots` uses `eng*` after dropping `t->lock` (volumes.c:~651-659).

| Role | Meaning |
|---|---|
| NONE | This node neither owns nor follows the volume. It answers MOVED for writes and opens a lazy follower for reads (§7). |
| FOLLOWER | Today's follower engine (manifest.c:~400-425), opened on demand and closed after 10 min idle. |
| PROMOTING | Holds the lock and is recovering (§4). Writes get UNAVAILABLE. |
| OWNER(E) | The single writer. |
| SUSPECT | The last confirmed fenced write is too old (§2.7). No acks and no namespace operations. Returns to OWNER on the next successful probe. |
| FENCED | A fenced write failed, or the gate gave up. The engine is abandoned (§2.6) and the slot goes to NONE once its threads have drained. |

**Role checks.** `opt.read_only` is read in about 15 places: db.c:581, 601, 613; memtable.c:508; manifest.c:~43, ~283; volumes.c:236, 289, 320, 334, 336, 379, 397, 415, 436, 542, 605, 614. In HA mode these become per-slot role checks.

**API.** `tsdb_open` in HA mode opens no engines eagerly. The HA manager calls `tsdb_volume_claim(t, v)` and `tsdb_volume_release(t, v, handoff_to)`.

## 2. Arbiter and fencing

### 2.1 Backend selection and preflight

At writer open, `statfs` on the volume picks the backend.
- **`NFS_SUPER_MAGIC`: NFS backend.** Preflight refuses to open unless all of the following hold:
  - `vers=4.1|4.2`, `hard`, no `local_lock` (with it, OFD locks stay local and fence nothing), no `nolock`;
  - `nfs.recover_lost_locks=N`;
  - `lookupcache=positive|none`;
  - for HA mode only, the server's `nfsv4leasetime` is at least `--ha-lease-ms`, and write delegations are off (checked through `/proc/fs/nfsd` by `tsdb-cli ha preflight` on the server; tsdbd logs what it can see from the client side).
- **Anything else: `flock` backend.** It has no fencing and is valid only on a single host. It is the path `make jepsen` exercises every day.

**`ha=1` volumes.** META v3 `ha=1` (also stored in the LOCK header) means writer opens are allowed only through an HA manager (`opt.ha_managed`). `tsdb-cli` write commands on such a database go through tsdbd over the protocol. A direct writer open returns `TSDB_ERR_READONLY` with a message.

### 2.2 Lock recipe and chained fds

**LOCK.**
- Open `<vol>/LOCK` with `O_RDWR|O_CLOEXEC` and without `O_CREAT`. It must start with a valid header carrying the volume uuid, so a deleted or recreated LOCK cannot create a second lock domain. Only `tsdb-cli ha init` creates it.
- Today's binary opens LOCK with `O_CREAT` and no `O_TRUNC` (db.c:401), so it cannot wipe the ledger. META `version=3` refuses it anyway (db.c:228).
- Lock call: `F_OFD_SETLK`, `F_WRLCK`, whole file. Never `dup` a locked fd. Never `F_GETLK`.

**Chained fds.** These files are OFD-locked (uncontested) on the fd the owner writes through:
- SERIES and STATES;
- every WAL segment;
- CATALOG while creating series.

A conflicting LOCK request expires the old client's entire state (`nfsd4_lm_lock_expirable`, nfs4state.c:L7966). All its locks become `NFS_LOCK_LOST`, which is sticky, and I/O through those descriptions fails: EIO for read, write, fsync and O_DIRECT, EBADF for `ftruncate` (`nfs4_copy_lock_stateid`, nfs4state.c:L941-947).

**Continuity lemma.** A lock taken after the client state was destroyed belongs to freshly recovered state and is not lost. So a chained fd may carry a commit only after a later fenced write through an already-trusted fd has succeeded. The root of trust is the epoch write through LOCK (§4, step 4). New segments are proven by their `SEG` record, and CATALOG by the SERIES append that follows it (§5).

**One mutex per fenced description.** Every `pwrite`, `fdatasync` and `ftruncate` on a fenced description is done under the single mutex that owns that description:
- LOCK: `ledger_lock`. The heartbeat is a `ledger_lock` client.
- Segment: the WAL writer role.
- SERIES and STATES: the dictionary lock.
- CATALOG: `create_lock`.

Linux reports a writeback error once per `struct file` (`file_check_and_advance_wb_err`), so two independent syncers on one description could swallow each other's error (findings 11, 25). Each description also has a `fence_gen` counter, bumped on any error. An append counts as durable only if `fence_gen` did not change across its `pwrite` and `fdatasync`.

**No O_DIRECT or killable writes to files a successor also writes.** On process exit, `do_unlk` skips waiting for I/O and sends LOCKU (fs/nfs/file.c:827-834), and an O_DIRECT write RPC can outlive the killed syscall (direct.c:239). Buffered writes are flushed by `->flush` at close, before the OFD lock is released at `__fput` (U5). The only O_DIRECT write is `MANIFEST.<E>`, a file only epoch E writes (D10).

### 2.3 LOCK file layout

| Offset | Contents |
|---|---|
| 0 and 4096 | **Owner slots**, two 4 KiB slots: `{magic, uuid, epoch, node_id, advertise, pid, boot_id, nfs_client_id, state (RECOVERING / OWNED / HANDOFF), handoff_to, heartbeat_ctr, xxh64}` |
| 1 MiB | Ledger region A |
| 1 MiB + 256 MiB | Ledger region B (the file is sparse) |

**Slots strictly alternate.** Every slot write (claim, OWNED, heartbeat, handoff) goes to the slot that does *not* hold the newest valid record. A torn write, for example an NFS server crash before COMMIT, can then only destroy a slot that is not the newest (findings 12, 26).

**Regions.** Each region starts with a checkpoint:

`{magic, uuid, gen, epoch, next_block, next_wal_n, series_n, states_n, live parts[], retired[] with remaining lifetime in seconds, live segments[], drop predicates[], orphans[] with remaining lifetime, xxh64}`

Records follow the checkpoint: `{u32 len, u32 gen, u32 epoch, u8 kind, payload, xxh32 seeded with (uuid, gen)}`. A record written into the wrong file or region fails its checksum.

| Kind | Payload |
|---|---|
| `EPOCH` | E |
| `SEG` | seq |
| `COMMIT` | (tmp, final, blocks)… |
| `FLUSHED` | seq, (tmp, final, blocks)… |
| `MERGE` | (tmp, final), sources… |
| `DROP` | predicate `partition_end ≤ ts` |
| `PURGE` | names… |

**Region rules.**
- The active region is the one whose valid checkpoint has the higher `gen`.
- Replay stops at the first bad record, or at the first record whose `gen` does not match.
- Compaction runs under `ledger_lock` alone: serialize `lstate` into the other region, `fdatasync`, switch. It runs when the active region is half full, at every takeover (§4, step 6), or every 10 min. A torn checkpoint fails its checksum and the old region is still intact.

**Lock order:** `flush_lock` > WAL writer role > `publish_lock` > **`ledger_lock`** > `manifest_lock` > `db->lock` > `buf_lock`. The dictionary and CATALOG locks are taken without holding any of these.

### 2.4 Epochs

- E is a u24 per volume. Legacy data counts as epoch 0.
- **Claiming E.** E = `max(both valid slots, active checkpoint epoch, EPOCH records after it) + 1`. It is written to the inactive slot and then as an `EPOCH` record, both fdatasynced, before any E-named file exists.
- **Names and numbers.**
  - `next_block = E<<40` replaces db.c:347;
  - segments are `wal_<(E<<40)|n>`, replacing memtable.c:~731;
  - temporary parts are `tmp_<E>_<node>_<n>.part`, replacing part.c:~212;
  - followers read `MANIFEST.<E>`.
- **EEXIST** on the successor's own segment `wal_<(E<<40)|1>` means an epoch was reused. That is CORRUPT and raises an alarm; it is not retried.
- **Replay filter.** Direct inserts pass `wal_seq=0` (db.c:577), so they never suppress replay. A stale flush part carries an older-epoch `wal_seq`, so it cannot suppress replay of a newer segment (memtable.c:~769).
- **WAL checksums** in format-3 segments seed xxh32 with `(uuid, seq)`, so a record written into the wrong segment fails its check. Format-1/2 WAL records are still read as today.
- **Block adjacency.** Merge adjacency is by sorted order. db.c:347 is believed to be the only `+1`; U16 checks this.

### 2.5 Commit points and the edit discipline

`lstate` is the in-memory fold of the checkpoint plus the records after it: live parts, retired names with monotonic deadlines, live segments, drop predicates, orphans and counters. It changes only under `ledger_lock`.

**`ledger_edit(rec)`:**
1. Take `ledger_lock`. Check `role == OWNER(E)` and that `grant->gen` is unchanged.
2. **Validate against `lstate`:**
   - MERGE: every source is live and no drop predicate covers it;
   - PURGE: each name is retired and its lifetime has passed;
   - DROP: always valid.
   - If validation fails, release the lock and discard the unrecorded tmp.
3. `pwrite` and `fdatasync` through the LOCK fd. On error: FENCED (§2.6), return `TSDB_ERR_IO`.
4. Fold the record into `lstate`.
5. Renames `tmp→final` (or `p→retired_p`). Then apply the edit to `db->parts` and the graveyard under `db->lock`, exactly mirroring `lstate`.
6. Release `ledger_lock`. Unlinks authorized by the record (PURGE, a segment after FLUSHED) may follow. MANIFEST is published after that.

| Effect | Today | New |
|---|---|---|
| Small insert ack | WAL `write` + `fdatasync`, `wal_durable` (memtable.c:463-468) | The same calls, on a chained segment fd. Then the ack gate (§2.7). **The hot path is unchanged.** |
| Rotation | `wal_open_segment` inside the writer role (memtable.c:524) | The next segment is pre-created outside the writer role: `O_CREAT\|O_EXCL`, OFD lock, `fsync(dirfd)`, `SEG` record. Rotation only swaps fds. |
| Direct insert (insert.c:467) | Intent file, renames, `fsync(dirfd)`, unlink (part.c:457-503) | Pending tmp parts (`fdatasync`), `fsync(dirfd)` (needed locally, U9 on NFS), blocks assigned under `publish_lock`, then `ledger_edit(COMMIT)` for any number of parts. |
| Flush (memtable.c:557-591) | `parts_commit`, list, unlink segment | Under `publish_lock`: `ledger_edit(FLUSHED s)`, then unlink `wal_s`. |
| Merge (merge.c:313-334) | Output renamed directly (part.c:~406); sources retired under `db->lock` | The output stays tmp until `ledger_edit(MERGE)`. `merge_commit_locked`'s "dropped" path (merge.c:305-308) can then only meet an unrecorded tmp. |
| `eng_drop_before` (db.c:624) | Retire | `ledger_edit(DROP predicate)`. Replay applies the predicate to every part of the matching partitions, merge outputs recorded later included. |
| `graveyard_purge` (manifest.c:104) | `unlinkat` once the wall-clock deadline (manifest.c:37) passes | `ledger_edit(PURGE)` with monotonic deadlines, then unlink. |
| Series or state id (series.c:231) | `O_APPEND` write + `fdatasync` | `pwrite` at an explicit offset on the chained fd, then `fdatasync`. The id is assigned after success (series.c:235). |
| MANIFEST (manifest.c:67) | Shared file: `pwrite` + `ftruncate`, no fsync | `MANIFEST.<E>`, created with `O_EXCL`. O_DIRECT `pwrite` at 0 with `{len, E, version, xxh64}`; no `ftruncate`. It lists live segments. |

**Rules that follow from this.**
- **Rename failure after a durable record.** If a rename fails after a durable COMMIT, FLUSHED or MERGE record, the caller gets `TSDB_ERR_IO` (indeterminate), the volume goes FENCED, and a successor rolls the edit forward.
- **No unlinks of adopted names outside PURGE.** The runtime never unlinks a name `lstate` holds, except through PURGE. With the local backend and a lifetime of 0, `part_unref` may still unlink retired names (part.c:166-167). Only *live* names that are missing make a volume CORRUPT.
- **Intents.** `txn_*` intent files no longer exist for format-3 volumes. `intents_recover` (part.c:506) stays for legacy volumes.

### 2.6 fd lifecycle, abandon and the abort rule

**The grant.** The grant object holds the LOCK fd, the chained fds, `dirfd`, a generation `gen` and the engine's thread handles. Queries never use grant fds: they use mmaps and separate unlocked read fds. Pinned `eng`s therefore keep working after the grant is released (finding 19).

**`eng_abandon(grant)`.** It runs on FENCED, on a gate timeout, and on SIGTERM after a failed drain.
1. Set role FENCED and bump `grant->gen` (atomic).
2. `dup2(poison, fd)` over every grant fd, including `dirfd`. The poison is a read-only pipe end, on which `pwrite`, `write`, `fdatasync`, `ftruncate` and `renameat`/`unlinkat` relative to it all fail (U20).
   - The fd number stays reserved, so no other open can reuse it (findings 1, 8).
   - `dup2` runs `filp_close` on the old description, which flushes dirty pages. The OFD lock is released when the last reference drops, which is after any syscall still in flight on it returns (U19). So in-flight I/O lands *before* a successor can be granted.
3. Stop and join the flusher, mergers, heartbeat and pre-creator. Group-commit leaders are tsdbd workers: they see FENCED or the poison and return `TSDB_ERR_IO`.
4. Close the poison fds only after every thread has exited. Until then the slot stays `draining`, and this process does not claim the volume again.
5. A thread stuck in uninterruptible NFS I/O keeps its description, and with it the lock, alive. §8 turns that into kill and escalation.

**Abort rule.** After any failed write, sync or `ftruncate` on a fenced fd, or a changed `fence_gen`, the engine does none of these:
- `parts_discard` (part.c:446) or `pw_abort` (part.c:~349);
- renames, unlinks or purges;
- flushes, `eng_flush` or `eng_close`.

It calls only `eng_abandon`. A record whose `fdatasync` failed may still be inside the successor's replay range.

**Clean release** (handoff, SIGTERM, `ha move`):
1. Stop admitting frames: reply MOVED, with the `handoff_to` owner.
2. Drain in-flight requests, then `mt_flush` and compact.
3. Write the slot `{HANDOFF, handoff_to}`.
4. Stop the threads, poison and close the chained fds, then the LOCK fd.

### 2.7 Ack gate and SUSPECT

The engine checks the gate once per group commit. tsdbd checks it again before the reply (tsdbd.c:~479-497, ~631-659, ~361-395):

```
ack iff role == OWNER(frame.epoch) && sync returned 0 && fence_gen unchanged
     && now_mono < t_send(last successful fenced write) + lease − margin     /* margin = max(2 s, 15% lease) */
```

**Probes.**
- The heartbeat runs on a dedicated per-volume HA thread every `lease/5`. It takes `ledger_lock` with a trylock; if the lock is busy, the holder's `fdatasync` is the probe. It writes the alternate slot with `heartbeat_ctr+1` and fdatasyncs it.
- Any successful WAL or ledger `fdatasync` also refreshes `t_send`.
- **Epoch read-back**, every `lease/5`: an O_DIRECT `pread` of both slots through a second, unlocked fd. A higher epoch, or a different `nfs_client_id` under our epoch, means FENCED. This catches a reclaim the server should have refused (U4).

**Refusals.**
- A refusal after engine work replies `TSDB_ERR_IO` (indeterminate). A refusal before engine work replies UNAVAILABLE or MOVED (definitely not applied).
- When the deadline passes the volume goes SUSPECT. A later successful probe proves the lock was never lost, because LOST is sticky, so the volume returns to OWNER. This rides out an NFS server restart or a stall shorter than the lease.
- **Stuck I/O.** If a fenced write on one volume has been outstanding for more than 3×lease while probes on other volumes (or a node probe file) succeed, the file is stuck and the server is not. The node stops feeding the watchdog (§8).

## 3. Why there is never more than one committing writer, and no acked write is lost

No clock appears in L1-L8.

**L1 (one grant).** The server grants the LOCK write lock to at most one open file description. Epoch E+1's grant (time T) comes after E's lock state is gone, in one of two ways:
- **(a) Release.** E's process exited, or its grant was poisoned and closed (§2.6). Every description is flushed at `filp_close` before the OFD lock is released at `__fput` (U5, U19).
- **(b) Expiry.** The conflicting LOCK expired E's whole client synchronously (nfs4state.c:L7966). Every lock state of that client is then LOST.

**L2 (epochs only grow).** E+1 is computed under the lock from every durable source of E (§2.4). Slot writes strictly alternate, so a torn write cannot hide E. `O_EXCL` on the own segment turns any violation into CORRUPT, never into silent reuse.

**L3 (commits are fenced).** Every ack and every ledger record is a write plus `fdatasync` through a description that holds a proven lock (continuity lemma, §2.2).
- In case (b), those descriptions are LOST and their writes fail when the RPC is prepared (U1).
- In case (a), no thread can start new I/O on the description after the poison, and I/O already in flight completes before T.
- The ack also needs `fence_gen` unchanged, so an error that a sibling thread consumed still blocks the ack.

**L4 (the successor reads after T).** It reads the ledger through the newly locked LOCK fd; taking the lock drops the cache (`do_setlk`). It locks each chained file before reading it. Segments are read with O_DIRECT.

**L5 (no acked write is lost).**
1. An ack means `fdatasync` succeeded through a fenced segment fd before T, so the bytes were stable on the server.
2. The segment was in `lstate` before its first ack: its `SEG` record succeeded before the segment became active.
3. Acked records form a prefix of intact records. A failed sync means FENCED and no further appends, so replaying up to the first bad record covers every ack.
4. A direct insert is acked only after its COMMIT is durable. The successor applies that COMMIT, rolling a rename forward if needed.

**L6 (late namespace operations are harmless).** A late writer can do only three things:
- **(i)** Carry out an edit it recorded before T. The successor applies the same edit. Names are never reused, so a second rename finds ENOENT.
- **(ii)** Touch E-named files that are not in `lstate`. Nobody adopts them, and they become orphans purged through PURGE.
- **(iii)** Nothing at all through poisoned fds.

Unlinks of adopted names happen only after a PURGE record, which a fenced writer cannot write. A tmp named in a record whose outcome is unknown is never unlinked (abort rule). An adopted name missing as both final and tmp makes the volume CORRUPT; it is never silently dropped.

**L7 (series ids).** CATALOG ids are assigned under the CATALOG lock, which is proven by the fenced SERIES append before the CATALOG write. Names are checked against every volume (§5).

**L8 (ordering per session).** A frame is applied only if its `(vol, epoch)` matches the owner, and only in sequence order after any refusal (§9). So a resend can never land after a newer frame of the same session.

**Two more points.**
- **COMMIT carries no stateid**, but it only stabilizes pages that the server accepted under a live stateid. After a server reboot the verifier changes, the client resends with the lost stateid, that fails with EIO, and nothing is acked.
- **What the clock-based belts cover.** The gate and SUSPECT add protection if U1 or U3 turn out wrong. Tests run every case with `CRASHFS_FENCE=none` as well, to show which belt holds on its own.

## 4. Takeover (standby S, volume v)

1. **Grant.** On the HA thread, `F_OFD_SETLK` on S's held LOCK fd succeeds. This is T. The role becomes PROMOTING.
2. **Read the slots and the ledger through the lock fd.**
   - Verify the uuid.
   - Duplicate client id: if `nfs_client_id` equals ours under another `node_id`, refuse and alarm.
   - Handoff rule: honour a given `(epoch, handoff_to ≠ self)` at most once, for 2 s plus one poll interval measured on S's monotonic clock from first sight. Then ignore it (finding 18).
3. **Lock the chained files.** SERIES, STATES and every live segment, polling `F_OFD_SETLK` every 20 ms for up to 30 s, because a local dying process may still be flushing. On timeout: unlock everything, back off, and log the holder from the slot.
4. **Root write.** Claim E (§2.4): write the inactive slot `{E, RECOVERING}` and fdatasync it, then append an `EPOCH E` record. This proves every lock from step 3. On failure, abandon.
5. **Recover.**
   - `fdatasync` SERIES and STATES through the new fds, so the server has stable copies.
   - Truncate torn tails (series.c:147).
   - Fold the checkpoint and the records into `lstate`.
   - Roll forward renames whose tmp exists and whose final name does not.
   - Open adopted parts by name (no readdir trust). Missing as both: CORRUPT.
   - Give retired names fresh monotonic lifetimes.
   - Replay each live segment with O_DIRECT reads up to its first bad record, with the per-partition filter (memtable.c:~769). The rows become parts through `FLUSHED s`.
   - Run readdir only to list orphans: names from epochs below E that are not in `lstate`. They get a lifetime and go through PURGE.
6. **Own segment and checkpoint.** Create `wal_<(E<<40)|1>` with `O_EXCL` (EEXIST is CORRUPT), lock it, `fsync(dirfd)`, append `SEG`. Compact `lstate` into the other region (gen+1). Any late write from E then lands in a region nobody replays.
7. **Publish.** Create `MANIFEST.<E>` with `O_EXCL`, version 1, listing its segments. Write the slot `{E, OWNED, advertise}` (alternating).
8. **Swap.** Under `t->lock`, install the writer `eng`. Any follower `eng` is freed when its pins drain. Start the flusher, mergers, heartbeat and pre-creator. Send `sd_notify STATUS=`.

**Legacy volumes (`tsdb-cli ha init`).**
- It runs today's directory-is-truth recovery once (`intents_recover` part.c:506, `load_parts` db.c:250, `wal_open` memtable.c:693), under the lock. It then writes LOCK's header, the first checkpoint and META v3 `ha=1`.
- **It requires `--legacy-writers-unmounted`.** Every node that ever ran a pre-HA writer must have unmounted and remounted the export, or rebooted, since its last writer ran. `umount` fails while any process still has a file open there, so this is a check the operator can actually make. A partitioned legacy writer has no fenced I/O, and the lock conflict would silently expire it (finding 2, case C). The runbook also shows `/proc/fs/nfsd/clients/*/states` on the server.

**Phase 2 (warm promotion).** Reuse the follower's mapped parts by name and install the filtered segments into `db->flushing`; up to 5 old segments against `MAX_FLUSHING`=4, so the overflow is flushed synchronously. Explicit segment lists in MANIFEST (D10) keep followers tailing old-epoch segments until FLUSHED. It is enabled only after `jepsen-ha` passes on the cold path.

## 5. Series creation and CATALOG

The owner of the target volume creates the series. Ids stay dense and `tsdb_series_at` placement is unchanged.

1. **Take the CATALOG lock.** OFD-lock CATALOG non-blocking, with jittered exponential back-off (1, 2, 4 … 50 ms). A holder releases after one batch.
2. **Check the name everywhere.**
   - O_DIRECT-read CATALOG past the known size.
   - O_DIRECT-read every volume's SERIES tail past its known length: about V READ RPCs per batch.
   - Check the name against CATALOG names *and* against orphans: SERIES entries that no CATALOG entry points to.
   - Found in CATALOG: return that id (INVALID if the type differs).
   - Found as an orphan in volume w: if this node owns w, adopt it (step 5 with `{w, local}`); otherwise unlock and reply MOVED to w's owner (finding 3).
3. **Fenced SERIES append** in the owned volume (`eng_series`). Its success proves the CATALOG lock. A failure means this node lost the volume: unlock, abort.
4. Truncate a torn CATALOG tail (`size % 8`), only after step 3.
5. `pwrite` the 8-byte record at `size`, then `fdatasync`. The id is `size/8`, replacing `t->ncat` (volumes.c:486).
6. Pass the ack gate, then unlock.

**Rules.**
- `adopt_orphans` (volumes.c:249) runs only under the CATALOG lock, only by the orphan volume's owner after its takeover step 5 (which stabilized SERIES), and only for orphans whose name is not reachable through CATALOG. Other orphans stay dead local ids: no global id points to them, so nothing can insert into them.
- VOLUMES, ID and NODES appends are admin-only (`tsdb-cli ha`), under the CATALOG lock.
- Every role calls `catch_up` on an unknown id; today only followers do (volumes.c:379, 397, 415).
- **Throughput.** Creations are group-committed per node: requests queue for up to 2 ms and run under one lock acquisition. `tsdb_series_create_many` exposes this to the API.
- **If the lock holder's node dies,** creation stalls for up to one lease. Inserts into existing series are unaffected.

## 6. Placement, polling and handoff

**Rank.** For volume v, rank comes from a rendezvous hash of (volume uuid, node id) over the **candidates of v**, *excluding the node named as owner in the current slot*.
- Rank 1 polls `F_OFD_SETLK` every 250 ms.
- Rank 2 polls every 1 s.
- Every other candidate reads the slots (one O_DIRECT READ) every 1 s. If `heartbeat_ctr` has not advanced for `lease/2` on its own monotonic clock, it polls at 250 ms with rank-staggered jitter (finding 24).
- Each volume's polling runs on its own HA thread. A non-blocking SETLK still blocks in the kernel through NFS4ERR_GRACE retries (U17).
- Cost at V=32, N=4: about 200 small RPCs per second.

**Rebalancing** works through hints and never affects safety.
1. A rank-1 node (rank computed without the owner exclusion) that has been healthy for 10 min writes `WANT.<node>` with a counter it bumps every 10 s.
2. The owner hands over (§2.6, clean release) only if it saw that counter advance within the last 30 s on its own clock, at most once per hour per volume. A node that lost or was fenced on v gets exponential back-off before it may ask again.
3. The new owner deletes `WANT.<self>`.

**Operator moves.** `tsdb-cli ha move <vol> <node>` and `ha drain <node>` check that the target is a live candidate (TOPOLOGY/STATUS) before writing `handoff_to`.

## 7. Followers and reads

- **Lazy followers.** Standbys do not tail WALs. A node opens a follower for a volume only when it must serve a read for it, and closes it after 10 min idle.
- **Default read routing:** to the owner. If the owner answers MOVED or UNAVAILABLE, or cannot be reached, the client falls back to any node, which opens a lazy follower. Read replicas can be pinned with `--follow-volumes` (open question 3).
- **What a follower reads each refresh.**
  - The slots (O_DIRECT) give E.
  - It reads `MANIFEST.<E>` and accepts only an `(E, version)` larger than its last.
  - It tails exactly the segments that MANIFEST lists, whatever their epoch, using O_DIRECT `pread` past the known offset. Two things follow: no page cache shared with a fenced writer on the same node can serve bytes the server does not have (finding 15), and no `+1` arithmetic over seqs (finding 16).
  - A checksum failure is retried on the next refresh, and a stall of more than 10 refreshes is reported in STATUS.
- **Promotion.** The promoting node keeps answering reads from its old `eng` until the swap. Queries keep their references, so no row disappears or appears twice.
- **Fenced node.** It serves no reads for v until it has applied a MANIFEST with an epoch above the fenced one.
- **Read-your-writes (phase 2).** A token `(seg, offset)` from the ack is satisfied once the follower has applied that segment to that offset, or has applied a MANIFEST that no longer lists the segment. It does not depend on the epoch (finding 14).

## 8. Server: admission, watchdog, escalation

**Per-volume admission (D19).**
- At most 4 requests per volume inside the engine: the group-commit leader plus followers. Further frames wait in a per-volume queue that holds no worker.
- Frames for SUSPECT or PROMOTING volumes are refused with UNAVAILABLE before they reach a worker.
- Queries run in a separate worker pool.
- Today's single FIFO (tsdbd.c:830) and one-request-per-connection `busy` flag (tsdbd.c:917-924) stay in place for ordering within a connection.

**systemd.**
- `Type=notify`, `NotifyAccess=main`, `WatchdogSec=6` (at least 2× the heartbeat period), `WatchdogSignal=SIGKILL`, `Restart=always`, `RestartSec=1`.
- tsdbd speaks `$NOTIFY_SOCKET` directly, without libsystemd.
- `EXTEND_TIMEOUT_USEC` during recovery.

**Watchdog (D17).** `WATCHDOG=1` is sent only if all of these hold:
- the io_uring loop handled a self-posted wake through `wake_fd` within 1 s;
- each worker's progress stamp, updated at request boundaries and inside long query loops, moved within 5 s or the worker is idle;
- each owned volume's HA thread *issued* a probe within `lease/5`. Completion is not required: NFS slowness leads to SUSPECT, not to a kill;
- no per-volume stuck-I/O condition (§2.7).

**Escalation.** A process in D state survives SIGKILL and keeps its lease.
- Unit: `TimeoutAbortSec=5s`, `FailureAction=reboot-force` (U15).
- Fallback: a tiny local helper on non-NFS storage that triggers `sysrq b` when the tsdbd pid outlives SIGKILL by 10 s.
- `RuntimeWatchdogSec` covers PID 1 and kernel hangs only.
- Documented bound: watchdog time + 10 s + reboot.

**SIGTERM** does a clean release of every owned volume (§2.6), so a standby takes over within one poll.

## 9. Protocol changes (version 5; versions 1-4 unchanged)

**HELLO.**
- Request: `u32 version` followed, for `version ≥ 5`, by `u8 session_id[16]`. Reply: `u32 version`.
- The server tracks a version per connection, defaulting to 0, because the C client often sends no HELLO (tsdb_client.c:~662-678).
- When a session id is already held by an older connection, the server drops that connection's queued frames and waits for the one in progress before it replies.

**INSERT_VALUES with the new flag `TSDB_HA = 4`.**
- The header (tsdb_protocol.h; reserved bytes checked at tsdbd.c:636-638) is reused as follows:
  - `u16` at offset 10 = volume;
  - `u32` at offset 12 = expected epoch;
  - a `u64 seq` follows the 16-byte header, so the arrays still start 8-aligned at offset 24.
- Old servers reject the flag and the non-zero reserved bytes with INVALID.
- v5 clients send inserts only as INSERT_VALUES with TSDB_HA. Packed frames work the same way.
- **Sequence rule** per `(session, vol, epoch)` on the server:
  - Any non-OK reply to `seq = s` (MOVED, UNAVAILABLE, IO) sets `blocked_at = s`.
  - While `blocked_at` is set, a frame with `seq ≠ blocked_at` gets UNAVAILABLE with body `u64 resend_from`.
  - `seq == blocked_at` clears it.
  - A new epoch starts with fresh state.

**SERIES / SERIES_CREATE replies** for v5 clients: `u32 id, u16 vol, u32 epoch`.

**New ops (next free op numbers).**
- `TOPOLOGY` → `u32 n, n × {u16 vol, u8 role, u8 0, u32 epoch, u16 len, addr}`. Any node builds it from the owner slots (O_DIRECT, refreshed every `refresh_ms`).
- `STATUS` → `u32 len`, followed by `key=value` text: node identity, and per volume the role, epoch, lease time left, probe RTT, gate refusals, fences, takeovers with per-step durations, ledger and WAL fsync p99, follower lag. `tsdb-cli ha status` and `--metrics-port` (Prometheus text) serve the same data.

**New statuses.**
- `TSDB_ERR_MOVED = -10`, body `u16 n, n × {u16 vol, u32 epoch, u16 len, addr}`. Sent before engine work only: definitely not applied.
- `TSDB_ERR_UNAVAILABLE = -11`, body `[u64 resend_from]`. Sent before engine work only.
- Errors after engine work started are `TSDB_ERR_IO` (indeterminate).

**Old clients.** `insert_slots` checks that every volume in a frame is owned before writing any of it. Old clients get READONLY on volumes this node does not own; reads work on every node.

**All four clients** (C `client/tsdb_client.c`, Rust `client/rust/src/client.rs`, Python via `client/python/src/lib.rs`, Java `TsdbClient.java`) must:
1. Send HELLO v5 with a random session id.
2. Keep a cache `vol → (addr, epoch)`, replaced only by a higher epoch.
3. Split MANY_SERIES batches per volume (already "durable per volume", volumes.c:~610-611; document it) and pipeline each volume straight to its owner with per-volume `seq`.
4. Keep every unacked packed frame. C must copy frames out of its shared zstd buffer (tsdb_client.c:~686-720) and needs a window cap. Rust (`MAX_PIPELINE` 1024) and Java (`MAX_IN_FLIGHT` 4096) already have caps.
5. On any non-OK, an I/O error or a timeout: refresh TOPOLOGY, reconnect with the same session, and resend every frame of that volume from the first refused or unacked one, in order, **including frames acked after it**, stamped with the new epoch.
6. **Retry by time budget, not by hops.** The default budget is 30 s (lease + margin + takeover p99), and UNAVAILABLE does not count as a hop.
7. **Detect a dead owner** with `TCP_USER_TIMEOUT` and keepalive at about 3 s. While any frame has been outstanding for more than 1 s, poll TOPOLOGY on another node every `refresh_ms` and resend as soon as the epoch increases (finding 21).
8. Connections become recoverable instead of permanently `broken`/`lost` (client.rs:~895-905, Java ~1413-1419). C checks reply ids and reports a status per frame (tsdb_client.c:~201-241).
9. Map -10 and -11: Rust `Status` (error.rs:~38-47), Java `Status`, `tsdbc_strerror`, `to_pyerr` (lib.rs:~41).

**Resend semantics.** Ordered at-least-once per session and volume. Idempotent under last-write-wins if no other producer writes the same keys and no `drop_before` runs in between. This is documented in the client API.

## 10. Failure modes (lease = 10 s)

| Failure | What happens | Failover | Why it is safe |
|---|---|---|---|
| tsdbd crash or kill -9 | Exit flushes each description, then releases its locks. Rank 1 polls within 250 ms; step 3 waits for any flush still running. | 0.25 s + cold open (U10: 0.5-3 s) | L1(a), L5. Bytes past the first bad record were never acked. |
| Clean SIGTERM, `ha move`, drain | Clean release (§2.6). The target is preferred for 2 s. | about 1 s | Nothing is in flight when the lock is released. |
| Node power loss | Renewals stop; after lease + up to 1 s the client is COURTESY (nfs4state.c:L6819). The next poll expires it and gets the lock in the same call. | ≈ 11.5 s + open | L1(b). The dead node does no I/O. |
| Node partitioned from NFS, then healed | Hard-mount writes hang, so no acks; the gate moves its volumes to SUSPECT. The standby takes over at ≈ 11.5 s. On heal, the old node's locks are LOST: EIO, FENCED, abandon. Every volume of that node moves (expiry is client-wide). If nobody contested, the client is reactivated and its volumes return to OWNER. | ≈ 11.5 s | L3: LOST is sticky. Late dirty pages on chained fds fail; pages on unchained files land only in unadopted names (L6). |
| Partition between clients and the owner only | No failover (D14). Operator `ha drain`. | none | One arbiter. |
| Process hung, kernel alive | Renewals continue and the lock is kept, so it is safe but unavailable. The watchdog SIGKILLs it, which becomes the kill case. | ≈ 6.5 s | L1(a). |
| Process in D state | SIGKILL does not complete. Escalation reboots the node (§8), which becomes power loss. | watchdog + 10 s + reboot + 11.5 s | The lock is held throughout, so there is never a second writer. |
| SIGSTOP of the process | The same as a hang. | ≈ 6.5 s | L1(a). |
| VM pause (renewals stop) | Expiry; EIO on resume. The monotonic clock may be frozen, so the lock alone carries safety. | ≈ 11.5 s | L1(b), L3. The gate is only a belt. |
| NFS server stall shorter than the lease | SUSPECT, no acks, back to OWNER after the stall. No kill, because the watchdog does not wait for NFS. | none | LOST is sticky, so a successful probe proves continuity. |
| NFS server restart | The owner reclaims within grace (nfs4proc.c:L10422) and is SUSPECT meanwhile. Standbys get NFS4ERR_GRACE and wait. An owner that misses grace becomes LOST, FENCED, then fails over. | 0, or grace + poll | U4. The epoch read-back catches a reclaim that should have been refused. |
| NFS server crash during a slot write | The torn slot is never the newest (strict alternation). E is also recovered from the checkpoint and EPOCH records. | — | L2. |
| Transient EIO or ENOSPC on a fenced fd | FENCED and abandon (poison, no close), then a fresh claim by any candidate. This node does not claim again until it has drained. | poll + open | §2.6. The fd number cannot be reused. |
| EIO or ENOSPC on a tmp part (unfenced) | The flush or merge fails and the tmp is unrecorded, so it is retried. The active memtable is rotated before any direct insert (bug fix B5). | none | Unrecorded tmps are never adopted. |
| One file stuck, server healthy | No progress for 3×lease while other probes succeed: kill, then escalation. | ≈ 30 s + kill | The lock is held until the process exits. |
| One volume stuck | Its frames queue per volume. Other volumes and reads keep their workers. | — | §8 admission. |
| Handoff target dies | The `handoff_to` hint is honoured once for 2 s, then ignored. | 2 s + poll | §4 step 2. |
| Rank 1 and rank 2 both down | The heartbeat is stale for `lease/2`, so every candidate polls. | lease/2 + poll | §6. |
| CATALOG lock holder dies | Creation stalls for up to one lease. Inserts continue. | ≤ 11.5 s for creation | L7. |
| Clock step on the owner | Deadlines are monotonic. Checkpoints store remaining lifetimes. | — | D11. |
| Second tsdbd on the same node | It becomes a candidate (standby). | — | L1. |
| Pre-HA binary | META v3 refuses it (db.c:228). | — | — |
| Library or CLI writer on an `ha=1` volume | Refused (D5). | — | — |
| Writer on a `local_lock` mount | Preflight refuses. | — | §2.1. |
| Legacy writer still running at `ha init` | `ha init` refuses without the unmount attestation. | — | §4. |
| Deleted LOCK | Opens refuse (no `O_CREAT`). | — | §2.2. |
| Duplicate `nfs4_unique_id` | The slot's `nfs_client_id` matches under another `node_id`: refuse to start and alarm. | — | — |

## 11. Configuration

- **NFS server:** `/etc/nfs.conf` `[nfsd] lease-time=10 grace-time=10` (10 s is the floor, nfsctl.c:L949; the proc files can be written only while nfsd is stopped); exports `rw,sync`; `fs.leases-enable=0` (no delegations, U2); nfsdcld running.
- **NFS clients:** `vers=4.2,proto=rdma,port=20049,hard,lookupcache=positive`, no `local_lock`; `options nfs recover_lost_locks=0 nfs4_unique_id=<uuid>`.
- **tsdbd:** `--ha --node-id --advertise --volumes --ha-lease-ms 10000 --ha-margin-ms 2000 --ha-poll-ms 250`. HA mode refuses `--no-fsync`, `--wal-sync-ms ≠ 0` and `obsolete_part_lifetime_s < 120` (D15).
- **Logging:** every transition is logged with vol, epoch, node, monotonic time and wall time.

## 12. Backend interface (`src/ha.h`)

```c
typedef struct ha_backend_ops {
    int  (*acquire)(ha_vol *, ha_grant **out);            /* non-blocking; BUSY; never a test-lock */
    int  (*claim_epoch)(ha_grant *, uint32_t *E);         /* max(slots, checkpoint, EPOCH)+1; root of trust */
    int  (*chain)(ha_grant *, int fd);                    /* lock fd; trusted after the next fenced write */
    int  (*log_append)(ha_grant *, const void *, size_t); /* durable, or TSDB_ERR_FENCED */
    int  (*log_checkpoint)(ha_grant *, const void *, size_t);
    int  (*log_read)(ha_grant *, ha_log_cb, void *);      /* after acquire, uncached */
    int  (*probe)(ha_grant *, int64_t *sent_mono_ns);     /* heartbeat + epoch read-back */
    int  (*owner)(ha_vol *, ha_owner *);                  /* any node, no side effects */
    void (*release)(ha_grant *, const char *handoff_to);  /* drained engine only */
    void (*abandon)(ha_grant *);                          /* poison; never close before drain */
    int64_t lease_ns, margin_ns;
} ha_backend_ops;
```

Backends: `ha_local.c` (`flock`, no fencing), `ha_nfs.c`, `ha_fake.c` (tests).

**The engine's contract with storage.**
1. A fenced log per volume.
2. Fenced WAL appends (chained fds on NFS).
3. Immutable, epoch-named blobs that are adopted only through a log edit.

**Later, for intellistream-objects** (not built now):
- The lease is a conditional PUT of a lease object carrying E.
- Log records are put-if-absent `log/<E>/<n>` objects. A successor seals the log by writing `end+1`, which is a real compare-and-swap fence.
- WAL records go into that log; parts stay blobs.
- This requires If-None-Match and If-Match (U12).

## 13. Test plan

**Regression tests.** Each must fail on the old code, checked by building it.
- B1: `parts_publish` discards committed parts after a `manifest_publish` error: unlink and dangling pointers (insert.c:501, memtable.c:~781).
- B2: a partial insert reported as success (part.c:~494).
- B3: ignored fsync results (part.c:~413, ~499, ~543; memtable.c:~334, ~792; db.c:~205, ~643).
- B4: the MANIFEST version restarts after a writer restart (manifest.c:57), so followers miss a publish.
- B5: one failed `mt_write`, then a small insert of x=v1, then a direct insert of x=v2. Today `mt_flush` does not rotate the active memtable while `nflushing > 0` (memtable.c:517), so v1 wins over v2 (finding 13).
- B6: a wall-clock step purges the graveyard early (manifest.c:37, 104), tested with a fake time source.

**Unit tests** (`tests/test_ledger.c`, `ha_fake`):
- Record encode and decode, torn records, region switch, and every edit applied twice.
- **Property test:** a checkpoint equals the fold of the records it replaces, under random concurrent COMMIT, FLUSHED, MERGE, DROP and PURGE with compaction injected between append and apply (findings 4, 10).
- MERGE against `drop_before` in both orders: never CORRUPT, never resurrected data (finding 9).
- Epoch claim with each slot torn and with the checkpoint ahead of the slots. EEXIST on the own segment gives CORRUPT.
- Ack-gate and SUSPECT arithmetic; the `fence_gen` race, where a sibling thread consumes the errseq error (finding 11).
- The sequence rule from §9, including SUSPECT→OWNER within one epoch (findings 7, 22).
- **Poison:** after abandon, a thread holding an old fd number gets an error and the number is not reused while another volume is promoted in the same process (findings 1, 8).
- CATALOG: torn tail; two processes creating the same name; an orphan in volume 1 while creating in volume 2 (finding 3).

**crashfs** (`tests/jepsen/crashfs.c`):
- Hook `fcntl`/`fcntl64` (OFD and POSIX), `flock`, `pread`, `dup2`.
- `CRASHFS_LEASE=<socket>`: an arbiter thread in jepsen plus a renewal sidecar per victim, which stands in for the kernel NFS client.
- `CRASHFS_FENCE=lockfile|none|all`.
- `CRASHFS_MODE=nfs`: namespace operations are never undone, and a random subset of dirty 4 KiB pages is lost.
- `CRASHFS_STALL=<path>:<ms>`: block one file's I/O.
- A witness journal of every mutation, `(pid, epoch, op, path, result, t)`.

**`tests/test_failover.c` in `make test`.** L = 300 ms, guarded `signal_pid`, `alarm`, `live[]`.
- kill handover;
- SIGUSR1 lease loss;
- a late WAL append, rotation, rename, purge, `parts_discard` and MANIFEST write after takeover, each of which must change nothing;
- node pause, then SIGCONT: no ack;
- SIGSTOP with renewals still running: the watchdog kills;
- standby race: exactly one EPOCH;
- the other volumes undisturbed, including while one volume's fsync is stalled (finding 18);
- both tmp and final missing gives CORRUPT;
- handoff to a node that is killed at once: an owner within 2 s plus one poll (finding 18);
- `ha move` while a query holds the old writer `eng` for 30 s: takeover under 1 s (finding 19);
- a 3 s stall of NFS through crashfs: no kill and no epoch change (finding 17);
- a writer open of an `ha=1` volume without HA management is refused (finding 2).

Every case also runs with `FENCE=none`, to show which belt holds on its own.

**`make jepsen-ha`.** `--nodes 3 --volumes 4`, about 20 s per nemesis: lease, partition, node-pause, pause-watchdog, kill, power-nfs, nfs-stall, handoff, and mix. It keeps checks 1-4 from jepsen.c, plus:
- 5: no ack after a newer grant;
- 6: monotonic reads per session;
- 7: one committing epoch at a time, from the witness journal;
- 8: every node converges, and the restart comparison stays;
- 9: client-measured failover (fault to first client-visible ack) under 3L.

**Manual: `tests/ha/nfs-check.sh` and `tests/ha/lockprobe.c`** on two hosts over RDMA.
- Partition with `ip link set <rdma netdev> down`, because netfilter does not see RoCE. On one host, use two network namespaces with NFS over TCP.
- Scenarios:
  - (a) kill -9;
  - (b) SIGSTOP for 3L;
  - (c) partition 2L, then heal;
  - (d) power loss via sysrq-b;
  - (e) NFS server restart within and outside grace;
  - (f) SIGTERM handover;
  - (g) one volume faulted while the others run;
  - (h) a file frozen in D state leads to escalation and a reboot.
- Preflight asserts the mount and server settings from §11.

## 14. Implementation plan

Each step lands with its tests green: `make test`, `make asan`, plus `make jepsen` for any step that changes how data reaches the disk.

0. **Lock probe and preflight.** `tests/ha/lockprobe.c`, `tests/ha/nfs-check.sh`. Proves: U1, U2, U5, U6, U7, U13, U19, U21 on the real mount. If OFD locks do not fence, D2 switches to POSIX locks with a dedicated thread owning the fds, and this document is updated before step 4.
1. **Bug fixes B1-B6.** `src/insert.c`, `part.c`, `memtable.c`, `db.c`, `manifest.c`. Proves: each regression test fails before the fix and passes after; `make jepsen`.
2. **Ledger core.** `src/ledger.c`, `src/ha.h`, `ha_local.c`, `ha_fake.c`: records, regions, `lstate` fold, compaction, epoch claim. Proves: `test_ledger` including the property test.
3. **Format-3 engine on the local backend.** `part.c`, `merge.c`, `memtable.c`, `db.c`, `manifest.c`, `series.c`, `internal.h`, `tests/test_types.c`:
   - the edit discipline (COMMIT, FLUSHED, MERGE, DROP, PURGE), pending merge outputs, pre-created segments;
   - epoch numbers and names, seeded WAL checksums;
   - open from the checkpoint, `MANIFEST.<E>` with a segment list, followers following it;
   - META v3 and migration of local volumes.

   Proves: all existing tests; `test_types` still reads v1 and v2; `make jepsen` (power, kill, pause); `make asan`; `insert-bench` and `tsdb-bench` with no regression on a quiet machine.
4. **Grant and abandon.** Grant object, poison and drain, abort rule, `eng_abandon`: `db.c`, `internal.h`, new `src/grant.c`. Proves: the poison unit test; crashfs-injected EIO leaves the volume unchanged on disk.
5. **NFS backend.** `ha_nfs.c`: OFD lock and chaining, heartbeat under `ledger_lock`, `fence_gen`, read-back, gate, SUSPECT and FENCED. crashfs lease, fence, mode and stall hooks. Engine-level `test_failover` cases. Proves: `test_failover` with `FENCE=lockfile` and `FENCE=none`.
6. **Per-volume slots.** `volumes.c`, `include/tsdb.h`: slots with pins, claim and release, backend chosen by filesystem, `ha=1` refusal, MOVED and UNAVAILABLE in the library, `catch_up` for every role. Proves: library-level failover tests and `test_volumes`.
7. **CATALOG.** Locked creation, name uniqueness over every volume including orphans, `adopt_orphans` rules, group commit, `tsdb_series_create_many`. Proves: CATALOG unit tests, and creation throughput with 4 creating nodes.
8. **tsdbd.** `server/tsdbd.c`, `include/tsdb_protocol.h`: HA manager (ranks, polling threads, handoff), per-volume admission, protocol v5 (session, seq, TOPOLOGY, STATUS), sd_notify, watchdog, metrics. Proves: `test_failover` at server level, `test_stability`, `test_server`.
9. **Clients.** C, Rust, Python and Java. Proves: `make test-clients` with a failover nemesis against a local 2-node setup.
10. **Integration.** `make jepsen-ha`; real NFS/RDMA runs (a-h); `insert-bench`/`tsdbc-bench` before and after; documentation (`docs/design.md`, AGENTS.md invariants and lock order).
11. **Phase 2.** Warm promotion, read-your-writes, forwarding proxy for old clients.

## Open questions for the user

1. **Lease time.** 10 s (the floor) gives about 11.5 s failover after node faults. 15 s tolerates more NFS server load before a false expiry but means about 17 s failover. Proposed: 10 s, margin 2 s.
2. **Automatic reboot.** A tsdbd stuck in D state keeps its volumes' locks until the node reboots. May tsdbd escalate to `reboot-force` on its own (proposed), or should that stay manual?
3. **Read routing.** Proposed: reads go to the owner, with lazy followers elsewhere only on fallback or by configuration. That saves NFS bandwidth for inserts but puts read CPU on the writers. Do you want dedicated read replicas by default?
4. **Old clients.** In version 1 they get READONLY on volumes the node does not own. Is a forwarding proxy needed in version 1?
5. **Client-only partitions** do not fail over (D14). Acceptable?
6. **The NFS server.** Is it dedicated? `fs.leases-enable=0` (no delegations) and a 10 s lease affect every client of that server.
7. **Upgrade.** `ha init` is one-way (META v3), and it needs every legacy writer node to have remounted or rebooted. Acceptable as an operational step?
8. **Series creation** is serialized across the cluster: a few hundred per second, or thousands with `create_many`. Enough for your device onboarding rates?

## Unverified assumptions

| # | Assumption | How it will be verified |
|---|---|---|
| U1 | After expiry on conflict, OFD-locked descriptions return EIO on write, fsync and O_DIRECT, and EBADF on `ftruncate`. Unlocked fds are not fenced. | lockprobe partition run (step 0) |
| U2 | `fs.leases-enable=0` makes nfsd skip write delegations silently. | Source (`nfs4_set_delegation`, `vfs_setlease`); `/proc/fs/nfsd/clients/*/states` shows no `deleg` |
| U3 | `cl_rpc_users` blocks the COURTESY transition while a WRITE compound is in flight. | Source (`nfs4_laundromat`); jepsen check 5 with partitions in the middle of a WRITE |
| U4 | nfsdcld refuses reclaims from clients it already expired. | Scenario (e) with an expired owner; the read-back alarm must not fire |
| U5 | At exit, `->flush` writes back and waits uninterruptibly before the OFD lock is released at `__fput`. | Source (`filp_flush`, `nfs_file_flush`, `locks_remove_file`); lockprobe kill-during-write run |
| U6 | O_DIRECT on NFS accepts unaligned I/O and returns errors synchronously. A `pread` past EOF returns short. | lockprobe |
| U7 | An old binary's `flock` conflicts with an OFD lock held on the same or another node. | lockprobe |
| U8 | The server's lease clock is boot time. | Source (`ktime_get_boottime_seconds`) |
| U9 | `fsync(dirfd)` costs nothing on NFS. | strace plus `nfsstat -c` delta |
| U10 | Cold-open promotion takes 0.5-3 s per volume at about 100 G/V rows. | `tsdb-bench` takeover timing |
| U11 | Polling and slot reads are negligible next to data RPCs. | `nfsstat -s` under load at V=32, N=4 |
| U12 | intellistream-objects supports If-None-Match and If-Match. | API review before backend work |
| U13 | The RDMA backchannel affects only delegations. | lockprobe over RDMA |
| U15 | systemd runs `FailureAction` when a process survives the final SIGKILL after a watchdog timeout, with `Restart=always`. | Scenario (h) on a test VM; otherwise use the local sysrq helper |
| U16 | Merge selection never assumes contiguous block numbers (db.c:347 is the only `+1`). | `grep` plus a merge test across an epoch gap |
| U17 | A non-blocking `F_OFD_SETLK` blocks in the kernel during NFS4ERR_GRACE retries. | Source (`nfs4_proc_setlk`); scenario (e) timing |
| U19 | `dup2` over an fd with a syscall in flight flushes and releases the old description only after that syscall returns. | Source (`do_dup2`, `filp_close`); unit test with a stalled crashfs write |
| U20 | On the poison descriptor, `pwrite`, `write`, `fdatasync`, `ftruncate` and `*at` calls all return errors. | Unit test |
| U21 | Without `local_lock`, NFSv4 sends OFD locks to the server with owner = description. | lockprobe across two hosts |

## 15. NFS server outages

This is §15 of this document. Subsections are numbered 15.0 to 15.10.

**Setting.** About 10 NFS servers export ZFS pools (raidz or draid, with a SLOG) over NFSv4.1/4.2 on RDMA. The servers have no failover: one can be down for hours. About 1000 volumes are spread over the servers and about 20 tsdb nodes, so every node has volumes on every server. A server can also fail in part: one of its pools suspended (`failmode=wait`), one dataset full, or one export missing.

**Goal.**
- A request for a volume that cannot be served gets an answer within `admit_deadline` (2 s by default). The answer is either "definitely not applied" (UNAVAILABLE) or "outcome unknown" (IO).
- Volumes on healthy storage keep full speed. Nothing that happens on dead storage may kill, reboot or freeze a node.
- When the storage returns, its volumes come back without an operator. Before they do, tsdb checks that the storage still holds what tsdb acknowledged.

**What this section changes elsewhere in the document.**
- §1: a per-volume health state next to the role.
- §2.7 and D17: the stuck-I/O escalation needs a successful probe of the *same filesystem*.
- §5: creation skips volumes on DOWN storage, with an abort rule.
- §8: new admission rules, watchdog exemptions, and no `FailureAction=reboot-force` for outages.
- §9: v5 without HA, D13 and HELLO amended, new bodies and statuses.
- §11: `grace-time` and the exports.
- §10, §13 and §14: new rows, tests and steps.

Section 15 can ship **before** the rest of HA, on today's engine (O0-O9).

### 15.0 Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| D20 | Keep **`hard`** mounts (§2.1). tsdb detects outages itself. | `soft`/`softerr` | A soft timeout leaves the outcome unknown. It also adds `ETIMEDOUT` to the errors whose dirty pages are dropped without redirtying (`fs/nfs/internal.h:911-929`, `write.c:972-976`). Hard mounts reduce errors but do not remove them: ENOSPC, EDQUOT, EIO and ESTALE from the server drop pages on hard mounts too. So the WAL taint (§15.3) and the SIGBUS guard are needed either way. |
| D21 | Health is tracked per **storage domain**. A domain is one NFS superblock: one exported filesystem, i.e. one ZFS dataset or pool, found through the `major:minor` field of `/proc/self/mountinfo`. Domains are grouped by server address. Each volume also has its own health (STUCK, FULL). | Health per server only; per volume only | A server can suspend one pool and serve another, or fill one dataset. With per-server health the probe says UP while volumes hang, and §2.7 then escalates. With per-volume health only, 50 independent timeouts. |
| D22 | Unavailable storage makes its volumes **UNAVAILABLE**. It never causes a kill, reboot or takeover, and threads stuck on it are exempt from every watchdog condition. | Escalation from D17 | No other node can reach the storage either. Killing nodes only loses their healthy volumes. |
| D23 | Refusals happen **at admission, before any engine work**. They return `TSDB_ERR_UNAVAILABLE` (definitely not applied). | Trying and timing out | A thread that touches a dead hard mount is lost until the storage returns. |
| D24 | A **stuck request fences its volume.** While any request is parked on volume v, v is UNAVAILABLE(STUCK) for every session. A parked request that has not yet published is *abandoned*: it discards its work instead of landing late. A parked request never touches another volume. | Letting late writes land after newer acked ones; per-session blocking | Block numbers are assigned at publish (`parts_publish`). A late direct insert would therefore win over a write acked after it. |
| D25 | Multi-volume inserts: a v5 frame writes its healthy volumes and replies `TSDB_ERR_PARTIAL` with a per-row bitmap. A v≤4 frame is refused **whole** if any target volume is unavailable. | Partial application for old clients | Old clients see PARTIAL as IO and resend the whole frame hours later. The resend overwrites newer acked values on the healthy volumes and undoes `drop_before`. |
| D26 | **tsdbd never creates a database or a volume implicitly.** It checks each path's mount identity against the node-local `catalog.cache` before opening. | Today's `eng_open` mkdir and META/ID creation | An empty export (dataset not mounted after a reboot) or a missing mount would otherwise get a fresh database. Writes to it vanish when the real one appears. |
| D27 | **Every recovery revalidates storage against memory** (WAL tail, MANIFEST, parts, CATALOG). On a mismatch the volume is DIVERGED: re-log, cold reopen, alert. | Resuming silently after a pure hang | `zpool import -m/-F` and SLOG loss roll back fdatasync-acked writes. |
| D28 | **No write is accepted for an unavailable volume:** no hinted handoff and no client spool. Apache Pulsar, in front of tsdb, is the buffer. The Pulsar sink (`connectors/pulsar`) retries a refused batch in place until the volume returns. | Server-side hints; a client spool with conditional replay | The user's architecture: Pulsar already keeps the data durably and in order per key, so an OK from tsdb keeps meaning "durable on the volume's storage". |

### 15.1 Mount options, kernel facts and rules

**Mounts.**
- Clients: `vers=4.2,proto=rdma,port=20049,hard,timeo=100,retrans=2,lookupcache=positive`, no `local_lock`. On a hard mount, `timeo` and `retrans` only control "not responding" and the RDMA disconnect (`xprtrdma/transport.c:409-426`). After that, reconnects back off from 5 s to 30 s (`xprt_rdma.h:65-66`).
- Server (changes §11): `grace-time=90`, with `lease-time=10` unchanged.
  - Grace must outlast the slowest client reconnect (30 s) with margin. Otherwise an owner in backoff misses reclaim, its lock is LOST, and the whole domain fails over at once.
  - nfsd extends grace only while someone is reclaiming (`clients_still_reclaiming`, `nfsd/nfs4state.c:6277-6288`). A client that has not reconnected yet does not count.
  - Grace delays only the restart of a server, never the failover of a node.
- Exports carry `mp` (or `mountpoint=`), so nfsd refuses to export a dataset directory that is not mounted (U34).

**Kernel facts (v6.12) that the rules rest on.**
1. Synchronous RPCs on a hard mount are killable only (`sched.c:987-989`). Waits for writeback are `TASK_UNINTERRUPTIBLE` (`mm/filemap.c:1448`): fsync, and close of a writable fd with dirty pages. Not even SIGKILL ends them.
2. A failed writeback with a "fatal" error is not redirtied. ETIMEDOUT and EIO count, and so do ENOSPC, EDQUOT and ESTALE (`internal.h:911-929`). Its pages are dropped and the error is reported once per open file description.
3. A fault on an uncached page waits, killable, without holding `mmap_lock` (`filemap.c:2373, 3433`). Any read error returns `VM_FAULT_SIGBUS` (`filemap.c:3433-3444`). That includes EIO from a degraded pool and ESTALE from a rolled-back file, **on hard mounts too**.
4. `nfs_file_mmap` may send a GETATTR under `mmap_write_lock` (`file.c:213-216`, `inode.c:1493-1503`). If that RPC goes to a dead server, every mmap, munmap and brk in the process stalls.
5. Direct reclaim can block on a dead server. `nfs_release_folio` calls `nfs_wb_folio` → `nfs_commit_inode` for clean but uncommitted (PG_private) folios when the gfp mask allows it and the caller is not kswapd (`file.c:447-460`). Any thread that enters direct reclaim can then wait for hours.
6. A process that has one thread in D state cannot finish exiting. Its shared fd table stays open, with every flock, the listening socket and every mapping.

**Rules.**
- **R1. Process-wide resources are never held across I/O to a volume.** These are: the event loop, the shared pools, global locks, `t->lock`, and `mmap_lock`.
  - All I/O to a volume runs on that volume's threads, or on a worker that admission has assigned to the volume and can write off (§15.3).
  - `part_open` keeps its `fstat` immediately before `mmap` (`part.c:75-87`), so the attribute cache is fresh and `nfs_file_mmap` sends no RPC (U39). A comment and a test with `acregmin=0` guard this.
- **R2.** Soft mounts are refused for every open. `--allow-soft-mount` overrides it.
- **R3. Bounded unstable data per file.**
  - The part writer and the WAL writer call `fdatasync` at least every `nfs_commit_bytes` (default 32 MiB, to be tuned in O0). Uncommitted folios then stay bounded, which bounds both the reclaim hazard (fact 5) and the dirty pages a dead domain pins.
  - The interval is measured with `insert-bench` and merges. O_DIRECT part writing is the fallback.
- **R4. Host settings that `tsdb-cli ha preflight` checks:**
  - **Per-bdi limits:** `strict_limit=1` and `max_bytes` in `/sys/class/bdi/<dev>/`. The sum of `max_bytes` over all NFS bdis must stay below half the global dirty limit (`vm.dirty_ratio`; about 51 GB at 20% of 256 GB). Otherwise one dead server can again throttle every writer (`mm/page-writeback.c:1082-1085`). Preflight reads `mountinfo` and `/sys/class/bdi`, computes the sum and fails over the limit.
  - **No memory limit on tsdbd:** no `MemoryHigh`/`MemoryMax`, which would push threads into direct reclaim. Raise `vm.min_free_kbytes` and `vm.watermark_scale_factor` so that kswapd does the reclaim (fact 5, U36).
  - **No statfs on NFS:** no agent may call statfs on the NFS mounts (`nfs_statfs` always sends an RPC). Exclude nfs4 from node_exporter. No `sync` and no bare `df`.
  - **Exports:** the exports use `mp`. tsdbd is started through `tsdbd.socket` (§15.3, Shutdown).

### 15.2 Storage domains and detection

**Domains.**
- At open, tsdb reads `/proc/self/mountinfo`, without stat-ing any NFS path. Each volume's path is mapped to its mount: `major:minor`, source `server:/export` and `addr=`. Volumes with the same `major:minor` form one **fs domain**. fs domains with the same `addr=` form one **server group**, used for display and for the recovery cap.
- An exported child dataset gets its own client superblock (U38). Each mount's `major:minor`, source and fstype are recorded in `catalog.cache` (§15.3). A path whose mount is missing or different is DOWN(NOT_MOUNTED), never opened.

**Probes.** Each fs domain has one probe thread on each node, and it does nothing else.
- Every 1 s it reads 4 KiB with O_DIRECT from `<first volume of the domain>/PROBE`.
- Every 5 s it does a `pwrite` and `fdatasync` of 4 KiB to `PROBE.<node_id>`.
- The probe thread is allowed to hang. A **monitor thread** that does no I/O reads the start time of the probe in flight (`CLOCK_MONOTONIC`).
- If a probe has been stuck for 60 s, a second probe thread starts. At most 2 probe threads per domain exist.

**Signals per volume (from §15.3 admission).**
- **In-engine age:** the start time of the oldest request inside the engine.
- **Parked count:** the number of requests parked on the volume (§15.3).
- **EIO or ETIMEDOUT** from the engine: the domain is probed at once.
- **ENOSPC or EDQUOT**, from the engine or from a write probe: the volume (not the domain) becomes **FULL**. Writes are refused and reads keep working.

**Domain states.**

| State | Entered when | Effect |
|---|---|---|
| UP | (start) | Normal admission. |
| DEGRADED | Write-probe p99 over the last minute is above `max(500 ms, 10 × its 1 h baseline)` | Merges are throttled to 1 per domain on this node (not paused: inserts wait in `db_wait_merges` after publishing, `db.c:150-162`, `insert.c:503`). The per-volume cap drops to 1 only while in-engine age is growing. `admit_deadline` for the domain becomes `min(10 s, max(2 s, 4 × in-engine p99))`. |
| DOWN | A probe has been outstanding for `down_ms` (default 10 s), or 3 probes in a row failed with errors other than ENOSPC/EDQUOT, or the mount is gone | Every volume is UNAVAILABLE(NFS_DOWN or NOT_MOUNTED). No merges, flushes, follower refreshes or recoveries start. |
| RECOVERING | **Write and fdatasync probes** succeed for 10 s, each within 1 s, after the last probe thread stuck on the domain has returned | Volumes are recovered one at a time (§15.5). Read probes alone never leave DOWN: the server's ARC answers reads while writes to the pool are suspended. |

**Hysteresis.** Leaving DEGRADED needs 2 min under the threshold. A domain that went DOWN 3 times in 10 minutes needs 60 s of good probes to leave DOWN. A slow domain (a resilver or a scrub) therefore stays DEGRADED and serves. It does not flap through DOWN and RECOVERING unless one probe really takes `down_ms`.

**Effective health of a volume.**
- AVAILABLE, DEGRADED, or UNAVAILABLE with a reason. The reasons are NFS_DOWN, NOT_MOUNTED, STUCK, FULL (writes only), BUSY, ROOT_DOWN, LOCKED, RECOVERING, DIVERGED and MAINTENANCE, plus SUSPECT, PROMOTING and FENCED from §1.
- **STUCK** is per volume, whatever the domain state. It holds while the volume has a parked request (D24). It also holds while `parked ≥ cap` or in-engine age exceeds `down_ms`. Parked requests count against the volume's cap, so new admission stops before more threads are lost. That bound holds even if the probe of a healthy sibling pool says UP.
- `tsdbd` publishes the domain and volume states to `/run/tsdbd/health` (tmpfs, written by the monitor thread) for the escalation helper.

**HA (changes §2.7, §8, D17).**
- An owned volume on DOWN storage goes SUSPECT through the ack gate.
- **§2.7 stuck-I/O escalation** now requires three things:
  - the volume's own fs domain is UP;
  - a write+fdatasync probe of that same fs domain succeeded after the stuck I/O began;
  - the volume is not FULL.

  A stall of a server, a pool or a dataset never escalates.
- **§8 watchdog.**
  - A thread registered as "in I/O on domain d" is exempt from the progress condition while d is not UP or its volume is STUCK. These are parked workers, read-pool threads, the opener, probe, recovery and HA threads.
  - The per-volume "HA thread issued a probe" condition is waived for volumes whose domain is not UP. Issuing is otherwise impossible: the previous synchronous probe is still stuck.
  - A jepsen-ha check (§15.8) asserts zero SIGKILLs and zero reboots during a 10-minute hang.
- **Standbys.** `F_OFD_SETLK` calls to DOWN storage block on per-volume HA threads, which are allowed to hang. When the domain returns, a standby waits for grace (90 s) plus 30 s before it polls, so a reclaiming owner keeps its volume.

### 15.3 Isolation

The row numbers refer to the stall map (rows 1-16).

**Admission in tsdbd (rows 1, 2, 14, 15). This is D19, made concrete.**
- **The loop routes by header only.**
  - A v5 insert carries `vol` (§9). Single-series frames are routed with an in-memory `route()` that never does I/O.
  - Packed MANY_SERIES frames without a single `vol` go to a small **decode pool**: zstd and unshaping (`tsdbd.c:360-392`). The pool routes them and hands them to per-volume admission. Decompression never runs on the loop.
  - Decode-pool threads do no engine I/O.
- **Refusals.**
  - Refusals for an UNAVAILABLE volume are sent from the loop or from the decode pool.
  - Refusals to v≤4 connections are **tarpitted**: the reply is held for `min(retry_after, 1 s)` on a loop timer, holding no worker, and limited per connection. Old clients that retry in a tight loop cannot load the loop.
  - STATUS counts refusals per connection.
- **Per volume:** a FIFO bounded in bytes (`vol_queue_bytes`, default 64 MiB), and a cap on requests in the engine (4). A full queue replies UNAVAILABLE(BUSY), with `retry_after` taken from the measured drain rate. Queries use the separate read pool (§8) with the same per-volume caps.

**Parking (D24).** A request that has been in the engine longer than `admit_deadline` on a volume whose domain is not UP, or whose in-engine age is growing, is parked:
1. **The volume is fenced.** It becomes UNAVAILABLE(STUCK) for every session and connection until every parked request on it has returned.
2. **The request is abandoned.** Its `abandoned` flag (atomic) is set.
   - The engine checks the flag under `publish_lock` in `parts_publish`, before allocating block numbers, and in `mem_insert` before `mt_add`.
   - An abandoned request discards its pending parts and returns `TSDB_ERR_ABANDONED`.
   - If it was already past that check, it completes. The fence in (1) makes sure nothing newer was admitted to that volume before it lands.
   - A multi-volume request checks the flag before each volume (`insert_slots`, `volumes.c:659-662`; `each()`; QUERY_GROUP), so it never touches another volume after being parked.
3. **The request is detached from its connection.**
   - The loop gives the connection a new generation and clears `busy`. The client gets `TSDB_ERR_IO`, which is indeterminate.
   - The worker's later `reply()` and `notify(done)` go to a tombstone. The tombstone is freed when the worker returns, never to `c` (`tsdbd.c:220-260, 915-924`).
4. **The pool starts a replacement worker.** Parked workers count against the volume's cap, not against `nworkers`. Parked threads are bounded by cap × volumes on the node.
5. **eng references.** A parked thread holds a reference on every `eng` it may touch. Recovery and abandonment (§15.5) wait for those references, not only for threads parked on one volume.

**D13 and HELLO (changes §9).**
- `seq == blocked_at` clears the block only when no frame of that session is still parked on the volume.
- A HELLO with a session id held by an older connection detaches that connection's queued frames and replies at once. It does not wait for a request in progress, which may be parked for hours. The STUCK fence keeps the order.

**Operations without one volume (rows 3, 4, and STATS).**
- COMMIT, FLUSH, OPTIMIZE, drop_before, STATS, `foreach_part`, QUERY_GROUP and LAST_STATS are split into **per-volume subtasks**. Each subtask goes through that volume's admission, cap and parking.
- The parent waits for at most `admit_deadline`. It reports volumes that were refused or did not finish (PARTIAL for v5, §15.4).
- **COMMIT and FLUSH** cover only the volumes the connection has written since its last COMMIT, tracked per connection. A client that never wrote to a down volume gets OK.
- **drop_before.**
  - `drop_lock` (`volumes.c:742-750`) becomes per volume.
  - A drop on an unavailable volume is recorded in `/var/lib/tsdb/<uuid>/pending_drops` (fsynced, local) and replied as pending.
  - Recovery runs it before the volume admits writes (§15.5).
- **Root I/O.** Series creation registers "I/O on domain d" around root I/O, so parking keys on the domain actually touched. It takes the root create lock with `pthread_mutex_timedlock` and refuses ROOT_DOWN on timeout.

**Global locks held across I/O (rows 5-8). Rule: never hold a lock that is not the volume's own across I/O to a volume.**
- **Row 5.** Placement and creation pick only AVAILABLE volumes. The SERIES append runs under the volume's own lock, and the CATALOG append under a short root lock.
- **Row 6: the catalog follower.**
  - No I/O ever runs under `t->lock` or on the loop. Today follower `catch_up` takes the wrlock and calls `eng_refresh(root)` and `load_layout` (`volumes.c:377-385`). Both `route()` and `tsdb_series_lookup()` call it on any miss (`388-418`).
  - Instead, each node runs one **catalog-follower thread**. It refreshes CATALOG and VOLUMES in the background, with a deadline, at most once per 100 ms and coalesced. It builds new arrays outside the lock and publishes them under a short wrlock.
  - On a miss, the caller never refreshes. It waits, on a worker and never the loop, for the next catalog generation, up to `admit_deadline`. Then it replies NOTFOUND, or UNAVAILABLE(ROOT_DOWN) if the root domain is not UP or the deadline passed.
  - `volume_open` runs outside the lock as well. This applies to every role (§5 "every role calls `catch_up`").
- **Row 7.** STATS and `foreach_part` copy the `eng` pointers and release the rdlock (`volumes.c:1035-1061`). The `renameat` in `db_retire_locked` and the `unlinkat` in `graveyard_purge` move out from under `db->lock`.
- **Row 8.** Series names are kept in `tsdb` memory. `names_find` never takes a volume's `d->lock`.

**Series creation when a volume stalls (row 5, §5).**
- Creation may skip the SERIES-tail check of volumes on DOWN storage. Nobody can append to them, so the only names missed are orphans, which §5 treats as dead local ids.
- **Abort rule.** Each creation batch carries an abort generation.
  - A batch whose SERIES append stalls past `admit_deadline` is aborted: its waiters get IO and the generation is bumped.
  - The thread re-checks the generation under the root lock immediately before the CATALOG append and `cat_add` (today these follow `eng_series` unchecked, `volumes.c:477-487`), and skips both if it changed. Its SERIES entry stays an orphan.
  - Under HA the thread holds the CATALOG fd through the grant, and aborting `dup2`s the poison over it before unlocking (§2.6). A late `pwrite` or `ftruncate` with a stale `size` then fails.

**Root volume and `catalog.cache` (row 9).**
- Each node keeps `/var/lib/tsdb/<uuid>/catalog.cache`: the CATALOG and VOLUMES prefix, its length and xxh64, the database id (`ID_FILE`) and `ROOT_ID`, and each volume's mount identity.
- Routing works from the cache while the root is down.
- Creation, and ids beyond the cached prefix, get UNAVAILABLE(ROOT_DOWN), never NOTFOUND.
- The cache is used only for the database id it names.

**mmap faults (row 10).**
- Queries are refused at admission. A query already running checks the volume state (atomic load) before each part and each granule run.
- **SIGBUS guard (version 1, all mounts).**
  - Decoding of mapped parts runs inside a per-thread `sigsetjmp` region that holds no locks and does no allocation. The part pin and the output buffers are released by the region's exit path.
  - The handler checks that `si_addr` lies in a registered part mapping and that the thread is inside a region, then `siglongjmp`s. Otherwise it restores the default action and re-raises.
  - The volume becomes UNAVAILABLE(IO_ERROR), its domain is probed, and the volume is tainted (recovery revalidates, §15.5).
- A fault in progress on a hard mount parks its worker.

**Startup (row 11, D26).**
- **No implicit creation.**
  - `tsdbd` opens the root and volumes without creating anything: no `mkdir` (`db.c:394`), no META (`db.c:236-245`), no `ID_FILE` (`volumes.c:313-320`).
  - `tsdbd --init` and `tsdb-cli init` create a database. `--db-id` (or the id in `catalog.cache`) must match `ID_FILE`.
  - Each path's mount identity is checked through `mountinfo` before any open. A mismatch or a missing mount is DOWN(NOT_MOUNTED).
  - The library keeps `create` for embedded use, but `tsdb_open` takes a new `TSDB_OPEN_NOCREATE` that tsdbd always passes.
- **Opening.**
  - `load_layout` opens volumes in parallel on opener threads, with a 10 s deadline per domain. A volume that fails or does not answer keeps its slot and routes to UNAVAILABLE.
  - Every unopened volume is retried in the background: when its domain recovers, when `mountinfo` changes (poll it every 2 s), and with backoff for any other error. Today a volume that fails at once is never retried.
  - `tsdb_open` succeeds if the root or a `catalog.cache` with the matching id is readable.
- **TSDB_ERR_LOCKED.** If the flock is held, the previous tsdbd (pid from `/run/tsdbd/pid`) may still be alive in D state. The volume is registered UNAVAILABLE(LOCKED) and retried in the background, instead of failing the open.

**Shutdown, crash and restart (row 12).**
- **SIGTERM.**
  1. Stop admission.
  2. Flush and **fully close** every volume on UP domains, in parallel: `eng_close`, including the LOCK fd, which releases the flock. In sync mode, the acked rows of DOWN volumes are in their WAL.
  3. Never touch the fds of DOWN domains.
  4. `_exit`.

  A leftover thread then pins only the dead domain's fds.
- **Crash handler** (SIGSEGV, SIGABRT, and unguarded SIGBUS).
  1. For each volume on an UP domain, `dup2` the poison (§2.6) over its write fds: WAL, part writer and `dirfd`.
  2. Close its LOCK fd.
  3. Re-raise.

  Writes already inside the kernel finish within one RPC on healthy storage, before a restart (`RestartSec=1`) can open the volume (U37). `oom_score_adj` is set low.
- **Listener.**
  - The listening socket comes from systemd socket activation (`tsdbd.socket`, `LISTEN_FDS`). A half-dead old process then cannot keep the port from the new one.
  - Today the listener uses only SO_REUSEADDR (`tsdbd.c:1146`). SO_REUSEPORT is rejected: the dead process's socket would keep receiving its share of connections.
- **systemd (changes §8).** `FailureAction=reboot-force` is removed from the unit. The escalation helper reboots only when `/run/tsdbd/health` records a §2.7 stuck file on an UP domain. It never reboots when the leftover threads belong to DOWN domains. Outside HA there is no escalation.

**WAL taint (durability).**
- On any write or `fdatasync` error, `wal_commit` sets `wal_broken` under `wal_sync_lock`, **before** it clears `wal_writing` (`memtable.c:434-474`).
- From then on, no leader advances `wal_durable` on that `eng`, every waiter returns IO, and `mem_insert` refuses.
- This is the only safe point. Today the next leader appends with O_APPEND (`memtable.c:330`) after a torn record, its `fdatasync` returns 0 because errseq was consumed, and `wal_durable` advances over the failed rows. Replay then stops at the torn record and loses the acked rows after it.
- The taint is set inside the engine, so a parked thread's error taints even when its reply goes to a tombstone.
- A tainted volume is UNAVAILABLE(IO_ERROR) and is never appended to again. Recovery uses the re-log procedure (§15.5).

### 15.4 Client-facing behaviour (protocol v5, extending §9)

**`TSDB_ERR_UNAVAILABLE = -11`** is sent only before engine work.
- v5 body: `u16 n, n × {u16 vol, u8 reason, u8 flags, u32 epoch, i64 since_ms, u32 retry_after_ms, u64 outage_id}`, then `[u64 resend_from]` when the frame carried a seq.
- `retry_after_ms = clamp(outage_age/10, 1 s, 60 s)` with ±20% jitter. For BUSY it comes from the drain rate; for MAINTENANCE it is the operator's `--until`.
- `outage_id` increments each time the volume leaves AVAILABLE (for logs and metrics).
- Streaming ops send `{last=1, n=0}` and then the body.

**`TSDB_ERR_PARTIAL = -12`**, v5 only.
- Body: `u16 nv, nv × {u16 vol, i16 status}`, `u32 nrows, u8 not_applied[(nrows+7)/8]`, then the UNAVAILABLE entries.
- A row's bit is set when its volume was refused (definitely not applied) or abandoned or indeterminate. The per-volume status says which.
- COMMIT, FLUSH and drop_before reply PARTIAL with per-volume statuses. QUERY_GROUP and LAST_STATS return the available series and end with PARTIAL listing the missing ones.

**v5 without HA.** The epoch is 0 outside HA mode. Sessions work as in §9: clients always send a random session id. This changes §9's assumption that v5 implies HA.

**New op `SERIES_VOLUME`:** `u32 n, ids[n]` → `u16 vol[n]`.

**TOPOLOGY** entries gain `u8 health, u8 reason, i64 since_ms, u16 domain, u16 server`. The request gains `u64 since_gen` and the reply `u64 gen`. There is no server push; clients poll.

**STATUS** gains `domain.<d>.{state,since,probe_ms,parked,refused}`, `vol.<n>.{health,reason}` and refusals per connection.

**Old clients (v≤4).**
- No error bodies (`tsdb_client.c:408-410`, `client.rs:813-821`).
- A refusal replies `TSDB_ERR_IO` (tarpitted, §15.3), or -11 with `legacy_unavailable_status=unavailable`.
- **A multi-volume frame is refused whole** if any target volume is unavailable, before anything is applied (D25). The option `legacy_partial=apply` restores partial application for operators who accept the overwrite risk.
- A volume that fails mid-frame still yields IO, as today.
- COMMIT and FLUSH use the per-connection written set, so they fail only if the connection wrote to an unavailable volume.
- Pipelined frames after a refused one are still applied. Old clients have no seq; this is documented.
- Late resends follow §9's caveat: last write wins by arrival, so they are idempotent only if no other producer writes the same keys and no `drop_before` runs in between.

**The four client libraries.**
- **C:** `tsdbc_unavailable(c, &info)`, per-frame statuses from `drain` (`tsdb_client.c:228-241` keeps only the first), a pipeline cap, frames copied out of the zstd buffer, and a read timeout.
- **Rust:** `Status::Unavailable(Vec<VolInfo>)` and `Status::Partial`. `wait()` returns every failed frame.
- **Python:** `TsdbUnavailable(TsdbError)` with `.volumes` and `.retry_after`.
- **Java:** `UnavailableException` and `PartialException`, plus a read timeout.
- **All four** split MANY_SERIES per volume (`SERIES_VOLUME`), keep refused and unacked frames per volume in seq order, and back off with full jitter capped by `retry_after_ms`.

**Pulsar as the buffer (D28).** Writers go through Apache Pulsar, and the Pulsar sink is the tsdb client that feeds a volume:
- It acknowledges a Pulsar message only after tsdb replied OK for the batch holding it.
- On UNAVAILABLE, BUSY or an indeterminate IO it retries the same batch in place, in order, after `retry_after_ms` with jitter. It never negatively acknowledges, because that would reorder a series.
- A late landing of an indeterminate attempt is harmless: D24 makes it land before anything admitted later to that volume, and the retry writes the same values.
- Producers key messages by series, and topics are best partitioned by volume (`SERIES_VOLUME`/TOPOLOGY), so one unavailable volume holds back only its own messages.

**What a reply means.**

| Reply | Meaning |
|---|---|
| OK | Durable on the volume's storage, as today. |
| UNAVAILABLE, or a PARTIAL row marked refused | Definitely not applied. |
| PARTIAL row marked abandoned | Not applied; the work was discarded. |
| IO, or a timeout | Indeterminate. If it lands, it lands before anything admitted later to that volume (D24). |

### 15.5 Recovery when the storage returns

1. **The domain goes RECOVERING** on write probes (§15.2). Stuck threads return on their own as the hard mount retries.
2. **Wait for references.** A volume is recovered only after every thread holding a reference on its `eng` has returned: parked workers, the opener, the flusher, mergers, the follower and the HA thread. Late writes therefore land, or are abandoned, before anything new is admitted. One recovery attempt per volume runs at a time; a new attempt starts only after the previous thread has returned. At most 4 volumes per server group per node recover at once, with 0-5 s of jitter.
3. **Revalidate (D27).** This runs for every volume, tainted or not.
   - **WAL:** an O_DIRECT read of the last durable record of the current segment. Its offset and xxh64 must match what the engine recorded at `wal_durable`, and the server size must be at least that offset.
   - **MANIFEST:** an O_DIRECT read must match the last published checksum and generation.
   - **Parts:** every live part exists with its expected size (`fstatat` on a fresh open).
   - **Pending drops:** run them now.
   - If everything matches and the volume is not tainted: resume on new fds (`wal_fd` is reopened, never reused).
4. **Re-log** (on taint, or on DIVERGED after a mismatch).
   1. Write the memtable rows with `lsn ≤ wal_durable` (`mt_collect` with `visible`) into a **new** segment, then fdatasync it and the directory. Rows past `wal_durable` were never acked and are dropped.
   2. Cold-reopen the volume from the directory (the intent files and `wal_seq` rules apply). A rollback that reverted a merge brings the source parts back under their live names, so the directory, not memory, is the truth.
   3. Parts that memory listed but the storage lost are reported (`tsdb_lost_parts{vol}`, block ranges, log at error level). Their rows are gone.
   4. In HA this is an edit under the ledger (§2.5). The volume stays DIVERGED until an operator acknowledges it, if any part was lost.
5. **Root.**
   - If CATALOG is shorter than `catalog.cache` and its prefix hash matches, a node may re-append the cached tail under the CATALOG lock, but only if every referenced volume's SERIES holds that local id with the cached name. Ids are positions, so this restores them exactly.
   - Otherwise the root is ROOT_DIVERGED. Creation is refused until `tsdb-cli catalog repair` runs.
   - A CATALOG whose prefix differs from the cache is always ROOT_DIVERGED. Creation reuses `id = size/8` (`volumes.c:483-486`), so continuing would give two series one id.
6. **NFSv4 state.**
   - After a server reboot, opens and locks are reclaimed during grace (`nfs4state.c:1638, 1881-1883`; U32). The 90 s grace covers the 30 s reconnect backoff.
   - A server that stayed up keeps an uncontested expired client as COURTESY.
   - Lost state (`NFS4ERR_EXPIRED`, `recover_lost_locks=0`) returns EIO on lock-owning fds (`nfs4state.c:998-999`), which leads to taint and the re-log path.
7. **HA writer lease.**
   - A lock that survived: the next heartbeat moves SUSPECT to OWNER, same epoch.
   - A LOST lock leads to FENCED and abandon (§2.6), then the §4 takeover at E+1. Standbys hold back for grace + 30 s (§15.2), so the old owner usually reclaims or wins.
8. **After recovery.**
   - Merges run at full rate. Admission is throttled by the part count (`PARTS_TO_DELAY_INSERT`, `db.c:150-162`): when a partition nears the limit, the volume's queue stops draining, fills and replies BUSY. Nothing holds a worker in `db_wait_merges` beyond the cap.
   - Followers refresh in random order. Clients see AVAILABLE in TOPOLOGY. Pulsar sinks resume as their `retry_after_ms` expire, and the part-count throttle (BUSY) paces the backlog they deliver.
9. **Storage that comes back elsewhere.** A pool imported on another head, or an export remounted at another path, leaves the old threads stuck for good. `tsdb-cli volume abandon <vol>` and `tsdb-cli domain abandon <d>` handle this:
   - mark the old `eng` and its threads abandoned (they leak, and their wakeups go to tombstones);
   - re-resolve the path through `mountinfo` to the new mount and domain;
   - cold-open with replay and revalidation.

   The old export must never be served again while abandoned threads exist. Under HA the new open claims a new epoch.

### 15.6 Not planned: hinted handoff

tsdb does not accept writes it cannot make durable on the volume's own storage. Data for an unavailable volume waits in Pulsar (D28), so there is no hint volume, no client spool and no conditional replay.

### 15.7 Observability and operator tools

**Metrics** (none of them touches NFS):
- `tsdb_domain_state{domain,server}` and `tsdb_probe_seconds{domain,kind}`;
- `tsdb_volume_health{vol,reason}`, `tsdb_refused_total{vol,reason}` and `tsdb_partial_total`;
- `tsdb_parked_threads{domain}` and `tsdb_abandoned_total{vol}`;
- `tsdb_lost_parts{vol}`, `tsdb_diverged{vol}`, `tsdb_outage_seconds_total` and `tsdb_recovery_seconds{step}`;
- the bdi dirty and writeback bytes.

**Logs.** Every transition is logged. Refusals are logged once per volume per minute.

**Commands.**
- `tsdb-cli ha status`, grouped by server and domain.
- `domain maint <d> on|off [--until]` drains: stop writes, flush, stop merges, `fdatasync`, then MAINTENANCE.
- `volume maint`, `domain probe`, `volume abandon` and `domain abandon`.
- `catalog repair`, and `volume ack-divergence`.

**Runbook.** A pool imported with `zpool import -m` or `-F`, or one that lost its SLOG, may have rolled back. Revalidation (§15.5) detects it, but the operator should expect DIVERGED volumes and check `tsdb_lost_parts`.

### 15.8 Test plan (extends §13)

**crashfs control socket** (`CRASHFS_CTL`).
- `hang`, `eio`, `enospc`, `estale`, `etimedout`, `slow <ms>`, `rollback` and `heal` per path prefix.
- `rollback` restores a prefix to a snapshot taken by `snap`.
- **mmap:** faults are served through `userfaultfd` (`UFFD_USER_MODE_ONLY`). The handler blocks them while hung and switches to `UFFD_FEATURE_SIGBUS` for eio and estale.

**`tests/test_outage.c` (make test).** Prefixes A, A2 and B: A and A2 are two filesystems of one "server", B is another server.
- With A hung, B's p99 stays within 1.2× of baseline and A answers within 2.5 s. With only A hung, A2 stays AVAILABLE.
- With A at `slow 500`, no volume becomes UNAVAILABLE, and merges and inserts continue.
- `heal`: A returns automatically, no acked row is lost, and no refused or abandoned row is visible.
- A PARTIAL bitmap matches the disk.

**Regression tests**, each failing on the old code (AGENTS.md):
- Stall-map rows 1, 3, 4, 5, 6 (a follower with the root hung and unknown lookups), 7, 8, 11 and 12.
- A parked direct insert, then a newer acked write to the same key, then heal: the newer value wins (D24).
- A parked MANY_SERIES frame: after heal, Y keeps the newer value.
- WAL `enospc` mid-record, then more inserts and kill -9: every acked row replays, and nothing after the error is acked.
- `rollback` of A's WAL tail and of a part: DIVERGED, re-log, and no acked row is lost that the memtable still held.
- `rollback` of the root CATALOG: ROOT_DIVERGED, and no duplicate id.
- An aborted creation whose SERIES append lands late: no second CATALOG entry.
- Root prefix missing at start: no ID or META is created, ROOT_DOWN, and recovery after "mount".
- SIGTERM with A hung, then restart: B's volumes open (flock released) and the port is served.
- A SIGBUS in a query on A (estale): the process survives, and A is UNAVAILABLE.
- A v≤4 multi-volume frame with A down is refused whole, and COMMIT on a connection that only wrote B returns OK.
- A Pulsar sink against a volume that goes down for minutes: no message is acknowledged early, none is lost, per-series order holds after recovery (connectors/pulsar integration tests).

**`make jepsen`: an `outage` nemesis** (hang, eio, slow or rollback of one prefix for 2-600 s, mixed with kill -9 and power loss). Checks: no acked write lost (outside declared rollbacks), no refused write visible, healthy throughput ≥ 80%, recovery within 30 s of heal plus grace.

**jepsen-ha:** a 10-minute hang of one domain, with zero SIGKILLs, zero reboots and zero takeovers. Server reboot with the client's RDMA reconnect at maximum backoff: owners reclaim and nothing fails over.

**Manual (`tests/ha/nfs-outage.sh`), 1 h and 3 h runs.** Ways to take storage away:
- (a) nft drop, TCP test mount only;
- (b) link down on the NFS server (RDMA);
- (c) nfsd threads set to 0;
- (d) server reboot;
- (e) `zpool export` of one pool while another stays up;
- (f) a dataset quota filled to ENOSPC;
- (g) a dataset unmounted under its export, to check `mp`.

Each run records: time to UNAVAILABLE, healthy-volume bench numbers, `df`/`sync` hangs, bdi dirty bytes, wchan of every tsdbd thread (looking for `nfs_commit_inode` in reclaim), time to recovery, reclaim, and parked threads.

### 15.9 Implementation plan

Each step lands with `make test` and `make asan`. Steps that touch the disk path also need `make jepsen`.
- **O0.** Measure on AlmaLinux 10 over RDMA: the timeouts, the dirty coupling and bdi sums, reclaim stalls under memory pressure, an mmap GETATTR, nfsd grace and reconnect, ZFS EIO over NFS, and the `nfs_commit_bytes` cost (U30-U39).
- **O1.** crashfs control socket and uffd. `test_outage.c`, failing at first.
- **O2.** `src/health.c`: domains from `mountinfo`, probes, monitor, states, STUCK and FULL; the new statuses in `tsdb.h`.
- **O3.** WAL taint in `wal_commit`; the abandoned flag in `parts_publish` and `mem_insert`; the SIGBUS guard; R3 periodic `fdatasync`.
- **O4.** No implicit creation, mount identity, `catalog.cache` with ids; the catalog-follower thread; no I/O under global locks (rows 5-9); the creation abort generation.
- **O5.** Parallel open, background reopen, LOCKED handling; SIGTERM full close, the crash handler, socket activation.
- **O6.** tsdbd admission: header routing, decode pool, per-volume queues and caps, parking with tombstones, tarpit, per-volume subtasks, per-connection COMMIT sets, pending drops. Both io_uring and epoll.
- **O7.** Recovery: reference waits, revalidation, re-log, DIVERGED, root repair, abandon. Part-count throttling.
- **O8.** Protocol v5 bodies, `SERIES_VOLUME`, TOPOLOGY, STATUS, `outage_id`; the four clients; `make test-clients`.
- **O9.** jepsen `outage` nemesis; manual runs (a)-(g).
- **O10.** HA: watchdog exemptions, the §2.7 same-filesystem rule, grace and standby hold-back, removal of `reboot-force`, the escalation helper reading `/run/tsdbd/health`. This goes into §14 steps 5 and 8.

### 15.10 Open questions for the user

1. Hard mounts with tsdb's own detection, as proposed? Is any node mounted `soft`?
2. Old clients: should a refusal be IO (tarpitted) or -11? Is whole-frame refusal of multi-volume frames from v≤4 clients acceptable? It stops batching v≤4 clients until the volume returns.
3. Root: a node-local `catalog.cache`, or CATALOG mirrored on a second server so creation survives a root outage?
4. Answered: Pulsar buffers writes during an outage (D28). Still open: the ingest rate per volume, which sizes the backlog to drain after a 3 h outage.
5. Should reads ever be served from page cache during an outage, marked stale?
6. Planned ZFS maintenance: how often? (`domain maint` drains.)
7. Do the ZFS servers run nfsdcld? What are their lease and grace times? Is raising grace to 90 s acceptable?
8. Monitoring agents on the tsdb nodes: which ones call statfs or `df`?
9. Should placement spread tenants across servers?
10. One ZFS dataset per volume, or several volumes per dataset? This decides the number of fs domains and probes.
11. `drop_before` today lets later inserts recreate dropped partitions (`db.c:624-646`). Should a per-volume dropped-before watermark reject late resends and replays below it?
12. Is there a spare head where pools can be imported? If so, `volume abandon` is the procedure.

**New unverified assumptions.**
- **U30.** The time to "not responding" and the RDMA reconnect time with `timeo=100,retrans=2`.
- **U31.** A process with a thread in D state keeps its fd table. After the changes above, only the fds of DOWN domains stay pinned.
- **U32.** The ZFS servers keep client records across a reboot.
- **U33.** Per-bdi `strict_limit` and `max_bytes`, with their sum below the global limit, stop a dead domain from throttling healthy mounts.
- **U34.** nfsd `mp` refuses to export an unmounted dataset directory on these servers.
- **U35.** ZFS returns NFS4ERR_IO for blocks with permanent errors, rather than hanging.
- **U36.** With no memcg limit and raised watermarks, tsdbd threads rarely enter direct reclaim. With `nfs_commit_bytes`, uncommitted folios are few enough that the risk is acceptable.
- **U37.** On healthy storage, in-flight write RPCs from a crashing process finish within `RestartSec`.
- **U38.** Each exported ZFS dataset gets its own client superblock (distinct `major:minor`), including child datasets under `crossmnt`.
- **U39.** An `fstat` immediately before `mmap` keeps `nfs_file_mmap` from sending a GETATTR.
