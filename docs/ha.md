# tsdb HA: one writer per volume, with failover over NFSv4

Status: design, not implemented. Scope: shared storage on NFSv4.1/4.2 over RDMA. An object-store backend (intellistream-objects) is planned for later, behind the interface in §12, and is not built now.

Code references are `file:line` at HEAD `d52afaa`. A `~` before a line number means it comes from the research map at `d2c2767` and may be a few lines off.

## Goals and non-goals

**Goals**
- **No acknowledged write is lost** by any single fault: a process crash, a node crash, a partition, a pause, or an NFS server restart.
- **Never two writers for the same data.** At most one *committing* writer per volume at any time. Writers that come out of a fault late can do no harm.
- **Automatic failover per volume.** A volume (one directory, `src/volumes.c`) is the unit of ownership. N volumes have N independent writers spread over the nodes.
- **Inserts stay as fast as today.** The small-insert hot path is still one `write` plus one `fdatasync` per group commit. There is no synchronous replication, no per-node copy of the data and no consensus protocol. The NFS server is the only arbiter.
- **Any node answers any request.** A node that does not own a series' volume forwards the request to the owner (§9.1). Clients and the Pulsar sink need only a list of nodes. By default they fetch the topology from it and send each request straight to its volume's owner (direct mode, D39); forwarding is the fallback.

**Non-goals for version 1**
- HA of the NFS server itself.
- Warm (in-place) promotion and read-your-writes tokens. These are phase 2.
- Compatibility with clients of protocol versions 1-4. There is no release yet; the protocol may change freely before 1.0, provided all four clients change together (D32). On-disk compatibility stays (AGENTS.md).
- Moving a series between volumes. A series' address names its volume (D9).
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
| D9 | **A series is addressed by `(volume path, name)`.** Its id is `(vol, local)`: the volume's number in VOLUMES (below `0xffff`) and its index in that volume's SERIES, a `u64` on the wire. Names are unique per volume only. A local id is never reused, even after a storage rollback (D36). There is no global CATALOG: a node-local `volumes.cache` maps paths to volumes (§5). | Dense global CATALOG ids under a cluster-wide OFD lock; per-volume hashed ids | The path already says where the series lives, so routing and creation need no global state, no cluster-wide lock and no root I/O. A root or CATALOG outage can no longer block creation, and a CATALOG rollback can no longer give two series one id. A series cannot move without changing its address. |
| D10 | MANIFEST is a hint for followers only. There is **one file per epoch** (`MANIFEST.<E>`), rewritten with O_DIRECT and no `ftruncate`, and it lists live segments explicitly. | One MANIFEST rewritten in place; an fsynced checkpoint on every publish | A killed owner's in-flight O_DIRECT write can only hit a file nobody reads any more (finding 5). Listing segments explicitly fixes the follower gaps at epoch boundaries (findings 14, 16). |
| D11 | The safety argument uses no clock. The ack gate and a resumable SUSPECT state are defense in depth. Every runtime deadline uses `CLOCK_MONOTONIC`. | A clock-based fence; wall-clock deadlines | Safety must not depend on clocks (finding 6). |
| D12 | Standbys poll with a non-blocking `F_OFD_SETLK` on dedicated HA threads, ranked over candidates only, and fall back to polling by everyone when the owner's heartbeat goes stale. They never call `F_GETLK`. | `F_OFD_SETLKW`; `F_GETLK` probes | A LOCKT expires a courtesy holder. CB_NOTIFY_LOCK over RDMA is unverified. |
| D13 | Insert frames carry `(vol, epoch, attempt, seq)`. After a retryable non-OK reply the server refuses later frames of that session and volume until the refused one is resent. Final replies (DROPPED, INVALID, NOTFOUND, STALE_ID) do not block (§9). | Epoch stamp alone; sticky MOVED | SUSPECT→OWNER within one epoch would otherwise reorder a pipeline (findings 7, 22). |
| D14 | A partition between clients and the owner, with NFS still reachable, does not cause failover. | Client reachability votes | The NFS server is the only arbiter. An owner cut off from its peers releases its volumes on its own (D38); that is a voluntary release, not a vote. |
| D15 | HA refuses `fsync=0`, `wal_sync_ms≠0` and `obsolete_part_lifetime_s < 120`. | Allowing them | With those settings an ack comes before durability (tsdb.h:~64-66). |
| D16 | The lock probe on the real NFSv4.2/RDMA mount is step 0. | Testing last | D2 and several assumptions depend on it. |
| D17 | The systemd watchdog measures CPU-side progress only. Slow NFS leads to SUSPECT, not to a kill. A file stuck while the server is otherwise healthy leads to a kill, then escalation. | Watchdog on NFS round trips | An NFS stall must not turn into a cluster-wide kill storm (finding 17). |
| D18 | Standbys do not tail WALs. Followers are opened lazily. Reads go to the owner by default. | Every node following every volume | That costs O(N × ingest) NFS reads for nothing (finding 23). |
| D19 | tsdbd admits requests per volume: a bounded number in the engine per volume, per-volume queues, and a separate read pool. | One shared FIFO of blocking workers | One stuck volume would otherwise starve all the others (finding 18). |
| D29 | **Built-in forwarding in version 1.** Any node accepts any request and forwards it to the owner of the series' volume, which the address decides (§9.1). Single-volume insert frames, packed or not, are forwarded without decoding. Requests that span volumes fan out per owner, and the entry node merges the replies. | Client-side routing only; a forwarding proxy in phase 2; an external proxy | The user's choice: clients and the Pulsar sink need only a node list. Forwarding cannot help when the volume's storage is down; that stays UNAVAILABLE. |
| D30 | **At most one hop, never a transparent retry.** The entry node stamps the owner epoch from its cache when it reads the frame; a node that receives a forwarded frame for a volume it does not own at that epoch replies MOVED and never forwards it again. The entry node relays every non-OK reply to the client and does not retry on its own. An OK is relayed only after the owner's OK. | Multi-hop routing with a TTL; entry-node retries after MOVED | No loop can form however stale the caches are. A retry by the entry node would race the client's own later frames of the same session and reorder them. |
| D31 | **Order across a hop.** A stream is `(session, vol)` for every frame kind: the rows of a multi-volume frame join the streams of their volumes. `seq` is one counter per session; `attempt` is per stream and rises with each resend. The owner keeps the sequence rule and the attempt fence per stream. The entry node keeps the same rule for every retryable non-OK it makes or relays, keeps a stream on one peer link and one stamped epoch while any of its frames is unanswered, and forwards a frame only within `fwd_max_age` of its arrival and while its client is still connected (§9.1). | Per-connection order only; a sticky entry node per session; one stream `ANY` for multi-volume frames; a per-connection attempt in HELLO | Two streams for one volume reorder with no fault at all; a re-hashed link or an epoch switch in the middle of a pipeline lets later frames overtake a refused one; a per-connection attempt fences a direct-mode session's other connections. A client that reconnects to another node after an entry node dies or stalls must not have an old copy of a frame land after its resend, also when the volume fails over meanwhile. |
| D32 | **No compatibility paths for protocol versions 1-4.** Version 5 is the only version. HELLO is mandatory. | Keeping versions 1-4 working (whole-frame refusal for old frames, `legacy_*` options, tarpits) | There is no release yet. Every legacy path is code and tests for clients nobody runs. |
| D33 | **A per-volume drop watermark.** `drop_before(ts)` durably raises the volume's `dropped_before` to the partition boundary at or below ts. Insert and replay rows below it are rejected with `TSDB_ERR_DROPPED`, which is final (§9). A drop newer than `now − max(partition_ms, min_retention_ms)` needs a FORCE flag, and only `tsdb-cli volume lower-watermark` lowers the watermark. | Today's behaviour: later inserts and replays recreate dropped partitions (db.c:624-646); a watermark without a guard | A late resend, a Pulsar retry after an outage or a WAL replay would otherwise bring dropped data back. It also removes the `drop_before` caveat from resend idempotence. Without the guard, one mistaken timestamp would make a volume refuse all new data for good. |
| D35 | **Peer links are authenticated.** Peers connect to a separate port and prove a cluster secret (HMAC challenge-response) that `ha init` writes outside the exports. Peer messages name nodes by `node_id`; addresses come only from NODES, and OWNERS speaks only for the sender's own volumes (§9.1). | The database id as the only check | Every HELLO reply returns the database id. A client posing as a peer could poison ownership caches, fence other sessions with forged attempts, and redirect forwarded inserts to an address it controls. |
| D36 | **High-water marks never go backwards.** Every node keeps, per volume, the highest epoch, SERIES and STATES lengths and `(wm_gen, dropped_before)` it has seen, with the volume's uuid, persisted in `volumes.cache` and exchanged in OWNERS. A claim takes an epoch above all of them; recovery fills lost ids with LOST entries and re-applies the highest watermark. The slot still decides ownership, in both directions. | Trusting the pool alone; caches that accept only higher epochs; an incarnation field in every id | LOCK, SERIES and the ledger live on one pool and roll back together (`zpool import -F`, SLOG loss). A reused epoch would reuse block numbers and pin stale routing; a reused local id would silently send a cached id's writes into another series. LOST entries avoid widening every id and every frame. |
| D37 | **Reply order and back-pressure per volume.** Replies carry the frame id and leave out of order across volumes; windows are per `(connection, volume)` and per `(peer, volume)`; streamed query replies use separate links with per-request credit (§9.1). | Replies in request order per connection; one window per peer; query chunks on the insert links | One slow owner, stuck volume or slow query reader would otherwise stall every other volume behind the same connection or link, which undoes D19 for forwarded traffic. |
| D38 | **An owner cut off from its peers releases its volumes** when better-connected nodes are alive on NFS (§9.1). | Keeping them, as D14 does for client partitions | With forwarding, simple-mode clients reach a volume only through the peer network. The release is voluntary, so fencing is unchanged; a symmetric split changes nothing. |
| D39 | **Direct mode is the default for every client** (the user's choice, 2026-10-24). A client fetches TOPOLOGY from any node of its list and sends each volume's requests straight to that volume's owner. Forwarding through any node (D29) stays as the fallback: before the first TOPOLOGY, after a MOVED until TOPOLOGY is refreshed, while the owner cannot be reached, and in **simple mode**, which a tool or ad-hoc client must choose explicitly (§9). A stream changes path only when none of its frames is unanswered, or as a resend with a new attempt. | Simple mode by default, direct mode only for the Pulsar sink and bulk loaders | Through forwarding alone, a fraction (N−1)/N of insert bytes crosses the cluster network twice (95% at 20 nodes) and pays a hop of latency and CPU on the entry node (§9.1, Cost). Direct mode costs a TOPOLOGY call and a cache per client, and one connection per owner node it writes to. Switching path with frames in flight would let a frame on one path overtake a frame on the other. |

## 1. Roles and topology

**Nodes.**
- Run one `tsdbd --ha --node-id N --advertise host:port --volumes all|list` per host.
- Every node mounts every export at the same path, because VOLUMES stores `realpath` (volumes.c:~118-126) and a series' address is that path (D9).
- Every node keeps peer links to every other node for forwarding and ownership exchange (§9.1), on a separate peer port and authenticated with the cluster secret (D35).
- Every node has a unique `nfs.nfs4_unique_id`.
- The candidate set of each volume comes from a `NODES` file in the root, written by `tsdb-cli ha init` and `ha add-node`, filtered by each node's `--volumes`.

**Per-volume slot in `tsdb`:** `{eng *follower_or_null, eng *writer_or_null, grant *g, role, epoch, pins}`. Pins are needed because `insert_slots` uses `eng*` after dropping `t->lock` (volumes.c:~651-659).

| Role | Meaning |
|---|---|
| NONE | This node neither owns nor follows the volume. It forwards requests for it to the owner (§9.1), and opens a lazy follower only for a query that allows stale reads while the owner cannot be reached (§7). |
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
- every WAL segment.

The root's files (ID, VOLUMES, NODES) are not chained: only `tsdb-cli` appends to them, under an OFD lock on the file (§5), and no ack depends on them.

A conflicting LOCK request expires the old client's entire state (`nfsd4_lm_lock_expirable`, nfs4state.c:L7966). All its locks become `NFS_LOCK_LOST`, which is sticky, and I/O through those descriptions fails: EIO for read, write, fsync and O_DIRECT, EBADF for `ftruncate` (`nfs4_copy_lock_stateid`, nfs4state.c:L941-947).

**Continuity lemma.** A lock taken after the client state was destroyed belongs to freshly recovered state and is not lost. So a chained fd may carry a commit only after a later fenced write through an already-trusted fd has succeeded. The root of trust is the epoch write through LOCK (§4, step 4). New segments are proven by their `SEG` record.

**One mutex per fenced description.** Every `pwrite`, `fdatasync` and `ftruncate` on a fenced description is done under the single mutex that owns that description:
- LOCK: `ledger_lock`. The heartbeat is a `ledger_lock` client.
- Segment: the WAL writer role.
- SERIES and STATES: the dictionary lock.

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

**Lock order:** `flush_lock` > WAL writer role > `publish_lock` > **`ledger_lock`** > `manifest_lock` > `db->lock` > `buf_lock`. The dictionary lock is taken without holding any of these. The drop watermark (D33) is raised under `publish_lock` and then `buf_lock`, in that order.

### 2.4 Epochs

- E is a u24 per volume. Legacy data counts as epoch 0.
- **Claiming E.** E = `max(both valid slots, active checkpoint epoch, EPOCH records after it, the highest epoch of v that this node or any peer has seen) + 1` (D36). The last term keeps epochs rising after a pool rollback that reverted the slots, the checkpoint and the segments together; the claimer takes it from its ownership cache and the peers' OWNERS state, collected with a 1 s deadline. An epoch at or above 2^24−1 is refused everywhere. E is written to the inactive slot and then as an `EPOCH` record, both fdatasynced, before any E-named file exists.
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
| `eng_drop_before` (db.c:624) | Flush, then retire; later inserts may recreate the partitions | `ledger_edit(DROP predicate)`, then raise the watermark `dropped_before` (D33, §9) and retire. The watermark is the largest DROP predicate in `lstate`, so it survives in the checkpoint. Replay applies the predicate to every part of the matching partitions, merge outputs recorded later included, and skips WAL rows below the watermark. |
| `graveyard_purge` (manifest.c:104) | `unlinkat` once the wall-clock deadline (manifest.c:37) passes | `ledger_edit(PURGE)` with monotonic deadlines, then unlink. |
| Series or state id (series.c:231) | `O_APPEND` write + `fdatasync` | `pwrite` at an explicit offset on the chained fd, then `fdatasync`. The id is assigned after success (series.c:235). |
| MANIFEST (manifest.c:67) | Shared file: `pwrite` + `ftruncate`, no fsync | `MANIFEST.<E>`, created with `O_EXCL`. O_DIRECT `pwrite` at 0 with `{len, E, version, xxh64}`; no `ftruncate`. It lists live segments. |

**Rules that follow from this.**
- **Rename failure after a durable record.** If a rename fails after a durable COMMIT, FLUSHED or MERGE record, the caller gets `TSDB_ERR_IO` (indeterminate), the volume goes FENCED, and a successor rolls the edit forward.
- **No unlinks of adopted names outside PURGE.** The runtime never unlinks a name `lstate` holds, except through PURGE. With the local backend and a lifetime of 0, `part_unref` may still unlink retired names (part.c:166-167). Only *live* names that are missing make a volume CORRUPT.
- **Intents.** `txn_*` intent files no longer exist for format-3 volumes. `intents_recover` (part.c:506) stays for legacy volumes.

**Range deletes, replaces and retention** (added after this plan) map onto
the same edits. A delete or replace commits tombstone parts (and a replace's
row parts) through `parts_commit` under `publish_lock`, exactly like a direct
large insert: one COMMIT edit, nothing new to replay. Tombstones only hide rows
of lower block numbers in their partition, so the ledger's block order is all
they need. A retention drop of a whole partition is a DROP predicate for that
one partition and does not raise the drop watermark (D33): a later insert into
it is a valid write. `RETENTION` is a dictionary like `SERIES`: append-only,
its length goes into the OWNERS high-water mark with the others.

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
1. Stop admitting frames: reply MOVED with the current epoch, the HANDOFF flag and `handoff_to`, never a predicted epoch. Entry nodes and direct-mode clients mark the volume handoff-pending and answer UNAVAILABLE(PROMOTING, `retry_after` 200 ms) without forwarding, until OWNERS or TOPOLOGY shows a higher epoch (§9.1). Nobody bounces between the old owner and a target that does not own the volume yet.
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

**L2 (epochs only grow).** E+1 is computed under the lock from every durable source of E and from the highest epoch any node has seen (§2.4), so a pool rollback that reverts every durable source together cannot lower it (D36). Slot writes strictly alternate, so a torn write cannot hide E. `O_EXCL` on the own segment turns any violation into CORRUPT, never into silent reuse.

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

**L7 (series ids).** A local id is assigned only after the fenced SERIES append in its own volume succeeds (series.c:231-235), so only the owner of the current epoch assigns ids, and a stale writer's append fails (L3). Ids carry their volume (D9), so no id is shared between volumes and no global state is involved (§5). A local id is never handed out twice, also after a storage rollback: lost ids become LOST entries (D36).

**L8 (ordering per stream).** A stream is `(session, vol)`. A frame is applied only if its `(vol, epoch)` matches the owner, only in sequence order after any retryable refusal, and only if its attempt is not older than the stream's newest (§9). An entry node keeps the same rule for what it refuses or relays, and keeps a stream on one link and one stamped epoch while it has unanswered frames (§9.1). So a resend can never land after a newer frame of the same stream, whether it came directly or through another node. A new owner has no memory of the stream; there the age limit, the client check and SESSION_FENCE keep old copies out (§9.1). They rest on U23 and on clients giving up on a frame no earlier than 3 s after sending it.

**Two more points.**
- **COMMIT carries no stateid**, but it only stabilizes pages that the server accepted under a live stateid. After a server reboot the verifier changes, the client resends with the lost stateid, that fails with EIO, and nothing is acked.
- **What the clock-based belts cover.** The gate and SUSPECT add protection if U1 or U3 turn out wrong. Tests run every case with `CRASHFS_FENCE=none` as well, to show which belt holds on its own.

## 4. Takeover (standby S, volume v)

A node runs at most 4 takeovers per NFS server group at once, each started after 0-5 s of jitter, as for recoveries (§15.5). Further volumes wait before their poll, so an NFS server restart that fences many owners does not bring tens of cold opens onto one cold server at once.

1. **Grant.** On the HA thread, `F_OFD_SETLK` on S's held LOCK fd succeeds. This is T. The role becomes PROMOTING.
2. **Read the slots and the ledger through the lock fd.**
   - Verify the uuid.
   - Duplicate client id: if `nfs_client_id` equals ours under another `node_id`, refuse and alarm.
   - Handoff rule: honour a given `(epoch, handoff_to ≠ self)` at most once, for 2 s plus one poll interval measured on S's monotonic clock from first sight. Then ignore it (finding 18).
3. **Lock the chained files.** SERIES, STATES and every live segment, polling `F_OFD_SETLK` every 20 ms for up to 30 s, because a local dying process may still be flushing. On timeout: unlock everything, back off, and log the holder from the slot.
4. **Root write.** Claim E (§2.4): write the inactive slot `{E, RECOVERING}` and fdatasync it, then append an `EPOCH E` record. This proves every lock from step 3. On failure, abandon.
5. **Recover.**
   - `fdatasync` SERIES and STATES through the new fds, so the server has stable copies.
   - Compare the SERIES and STATES lengths and the drop watermark with the highest values any node has seen (D36). Storage behind them was rolled back: append LOST entries up to the highest lengths, apply the highest watermark, and mark the volume DIVERGED (§15.5 step 4).
   - Truncate torn tails (series.c:147).
   - Fold the checkpoint and the records into `lstate`.
   - Roll forward renames whose tmp exists and whose final name does not.
   - Open adopted parts by name (no readdir trust). Missing as both: CORRUPT.
   - Give retired names fresh monotonic lifetimes.
   - Replay each live segment with O_DIRECT reads up to its first bad record, with the per-partition filter (memtable.c:~769). The rows become parts through `FLUSHED s`.
   - Run readdir only to list orphans: names from epochs below E that are not in `lstate`. They get a lifetime and go through PURGE.
6. **Own segment and checkpoint.** Create `wal_<(E<<40)|1>` with `O_EXCL` (EEXIST is CORRUPT), lock it, `fsync(dirfd)`, append `SEG`. Compact `lstate` into the other region (gen+1). Any late write from E then lands in a region nobody replays.
7. **Publish.** Create `MANIFEST.<E>` with `O_EXCL`, version 1, listing its segments. Write the slot `{E, OWNED, advertise}` (alternating).
8. **Swap.** Under `t->lock`, install the writer `eng`. Any follower `eng` is freed when its pins drain. Start the flusher, mergers, heartbeat and pre-creator. Push `OWNERS` to every peer (§9.1), so entry nodes route to the new owner without waiting for a MOVED. Send `sd_notify STATUS=`.

**Legacy volumes (`tsdb-cli ha init`).**
- It runs today's directory-is-truth recovery once (`intents_recover` part.c:506, `load_parts` db.c:250, `wal_open` memtable.c:693), under the lock. It then writes LOCK's header, the first checkpoint and META v3 `ha=1`.
- **It requires `--legacy-writers-unmounted`.** Every node that ever ran a pre-HA writer must have unmounted and remounted the export, or rebooted, since its last writer ran. `umount` fails while any process still has a file open there, so this is a check the operator can actually make. A partitioned legacy writer has no fenced I/O, and the lock conflict would silently expire it (finding 2, case C). The runbook also shows `/proc/fs/nfsd/clients/*/states` on the server.

**Phase 2 (warm promotion).** Reuse the follower's mapped parts by name and install the filtered segments into `db->flushing`; up to 5 old segments against `MAX_FLUSHING`=4, so the overflow is flushed synchronously. Explicit segment lists in MANIFEST (D10) keep followers tailing old-epoch segments until FLUSHED. It is enabled only after `jepsen-ha` passes on the cold path.

## 5. Series addressing and creation (D9)

A series is addressed by `(path, name)`: the absolute path of its volume, as listed in VOLUMES, and a name that is unique within that volume. The path says where the series lives, so neither routing nor creation needs a global catalog, a cluster-wide lock or the root.

**Ids.**
- A series' id is `(vol, local)`. `vol` is the volume's number, its line in VOLUMES, fixed when `tsdb-cli volume add` appends it. `local` is its index in the volume's SERIES, assigned after the fenced append (series.c:231-235).
- On the wire and in `tsdb.h` it is a `u64`, `vol << 32 | local` (`tsdb_sid`), with `vol` below `0xffff`: the insert header's `vol` is a `u16`, and `0xffff` marks a multi-volume frame (§9). Every `uint32_t series` in `tsdb.h` (tsdb.h:115-259) becomes `tsdb_sid`, and every op that takes an id takes a `u64`: SERIES_NAME, SERIES_TYPE, STATE and the queries (today `u32`: tsdbd.c:463-470, tsdb_client.c:312-316).
- **Local ids are never reused** (D36). If SERIES or STATES on storage is shorter than the longest any node has seen, recovery appends LOST entries up to that length before the volume admits anything (§15.5). A frame, query or STATE op on a LOST id gets `TSDB_ERR_STALE_ID = -14`, which is final; the client re-resolves by `(path, name)`, and a lost name is created again under a fresh id.
- Today's dense global ids go away: `t->ncat`, `cat_vol`/`cat_local`, `cat_add` and the name hash over all volumes (volumes.c:47-116), the id `size/8` (volumes.c:486), and the CATALOG lookup in `route()` (volumes.c:388-408). `route()` takes the volume from the id and checks `local` against that volume's dictionary.
- Each volume's META records its number and a uuid next to the root id it already keeps in ROOT_ID (volumes.c:155-170), and its VOLUMES line records the uuid too. Every open checks that META's `(number, uuid)` equals the VOLUMES line and the cache entry; otherwise the volume is DOWN(MISMATCH) and an alarm is raised. A lost or rolled-back VOLUMES file can therefore be rebuilt from the volumes (§15.5).

**Path → volume.**
- Every node keeps `volumes.cache` (§15.3): the VOLUMES lines with each volume's number, uuid, expected fstype, export source and aliases.
- A request's path is matched as a string against the cache, after a fixed normalisation: it must be absolute, repeated `/` collapse, a trailing `/` is stripped, and a `.` or `..` component is refused with INVALID, never resolved (the kernel resolves `..` after symlinks, which a string rule cannot). `tsdb-cli volume add --alias <path>` records extra strings for one volume, so a symlinked configuration (`/tsdb -> /mnt/tsdb`) keeps working without a stat. The SERIES reply and TOPOLOGY return the canonical path. Nothing is stat-ed: `realpath` (volumes.c:440) would send RPCs to a possibly dead server (R1, §15.1). Every node mounts every export at the same path (§1), so a path names the same volume on every node.
- A path that is not in the cache gets NOTFOUND at once, and is remembered as missing for 1 s, unless this node may be behind: a `VOLUMES_GEN` push is pending, a peer has reported a higher VOLUMES generation (§9.1), or the root domain is not UP. Then the reply is UNAVAILABLE(RELOADING, or ROOT_DOWN) with `retry_after`, and the node fetches the missing lines (§15.3). tsdbd never creates a volume for a path (D26). Today `series_make` creates one for any new path: `mkdir`, `volume_open(create)` and a VOLUMES append (volumes.c:438-476). That becomes the admin command `tsdb-cli volume add <path>`, which appends under an OFD lock on VOLUMES and then pushes the new generation to every node over the peer links (§9.1). The library keeps the implicit form only for embedded use without HA.
- **`volume add` refuses** a number above 65534; a directory whose META already has a volume number or uuid; a uuid already in VOLUMES; and a path that is a prefix of another volume's path or lies under one, the root included. `realpath` does not see through bind mounts or a second mount of one export, so these checks, and the META check at every open, are what keep two VOLUMES lines off one directory and one volume out of another's orphan scan. Before it appends, it collects `(generation, xxh64 of the VOLUMES prefix)` from every live node over the peer links and refuses if any node's cache is longer or differs, so a rolled-back VOLUMES cannot hand out a number again (§15.5 step 5). A number whose META exists somewhere is never reused.
- The root volume (plen 0) holds no series in HA mode: a SERIES request without a path is INVALID there. Today `tsdb_series(name)` and a NULL path create series in the root (volumes.c:499), so existing databases may hold some. `tsdb-cli ha init` refuses a root that holds series; `tsdb-cli volume migrate-root <path>` first copies them into a new volume, under new ids that clients re-resolve by `(path, name)`.

**Lookup and creation run on the owner of the volume** (forwarded there by any node, §9.1).
1. The owner looks the name up in that volume's in-memory dictionary. Found: reply `(id, epoch)`, or INVALID if the type differs.
2. Otherwise a fenced SERIES append (`eng_series`) creates it. A failure means this node lost the volume: FENCED, and the reply is IO.
3. Pass the ack gate (§2.7), then reply `(id, epoch)`.

**Rules.**
- Creation in different volumes shares nothing. Within a volume it is serialized by the dictionary lock and group-committed: requests queue for up to 2 ms and share one append and one `fdatasync`. `tsdb_series_create_many` exposes this to the API.
- **Unavailable storage.** If the path's volume is UNAVAILABLE (§15.2), the request gets UNAVAILABLE for exactly that volume. There is no fallback to another volume, and other paths are unaffected.
- **No moves.** A series cannot move between volumes without changing its address. Copying data to a new path is a tool's job, not part of this design.
- **Names.** The same name in two volumes is two series. `tsdb_series_lookup(t, name)` becomes `tsdb_series_lookup(t, path, name)`, and `SERIES` with `create=0` takes a path. Today `tsdb_series_at(name, path)` returns the existing series in whichever volume holds the name (volumes.c:434-435). Without HA the library keeps a cross-volume check: if the name exists in another volume it returns `TSDB_ERR_EXISTS = -15` instead of silently creating a second, empty series that would split one series' data over two volumes. In HA mode the path decides and no other volume is searched.
- **CATALOG** is no longer written. It is not needed to read existing databases either: every SERIES entry is a series of its volume. Orphans from a crash between the SERIES and CATALOG appends (volumes.c:17-19) become ordinary series, which is what `adopt_orphans` (volumes.c:249) did anyway. Listing is per volume (below); a node-local index for listing is optional and never used for routing.
- **Listing.** `tsdb_series_count` (tsdb.h:123) and the `tsdb-cli series` loop over `0..count−1` (tools/tsdb-cli.c:496-499) assume dense ids. They become `tsdb_volume_series_count(t, vol)` and `tsdb_series_foreach(t, path, fn)`. The protocol gains `LIST_SERIES` (§9), forwarded to the owner and served from its in-memory dictionary. A listing of every volume is a fan-out (§9.1) that ends with PARTIAL for the volumes that are unavailable.
- VOLUMES, ID and NODES appends are admin-only (`tsdb-cli`), each under an OFD lock on the file it appends to.
- An id whose `vol` is beyond the node's cache gets UNAVAILABLE(RELOADING) while the volumes-follower thread fetches the missing lines (§15.3); nothing refreshes inline. Every role does this; today only followers call `catch_up` (volumes.c:379, 397, 415).
- **The root's storage being down** stops only `volume add`. Routing and creation run from `volumes.cache`.

## 6. Placement, polling and handoff

**Rank.** For volume v, rank comes from a rendezvous hash of (volume uuid, node id) over the **candidates of v**, *excluding the node named as owner in the current slot*.
- Rank 1 polls `F_OFD_SETLK` every 250 ms.
- Rank 2 polls every 1 s.
- Every other candidate reads the slots (one O_DIRECT READ) every 1 s. If `heartbeat_ctr` has not advanced for `lease/2` on its own monotonic clock, it polls at 250 ms with rank-staggered jitter (finding 24).
- Each volume's polling runs on its own HA thread. A non-blocking SETLK still blocks in the kernel through NFS4ERR_GRACE retries (U17).
- Cost at V=32, N=4: about 200 small RPCs per second.

**Rebalancing** works through hints and never affects safety.
1. A rank-1 node (rank computed without the owner exclusion) that has been healthy for 10 min writes `WANT.<node>` with a counter it bumps every 10 s.
2. The owner hands over (§2.6, clean release) only if it saw that counter advance within the last 30 s on its own clock, at most once per hour per volume. A node that lost or was fenced on v gets exponential back-off before it may ask again, except when it lost the lock because the NFS server restarted (its fs domain was DOWN when the lock was LOST, §15.5): that is not a node fault, so there is no back-off, and the move back does not count against the once-per-hour limit. Placement then returns within minutes of a server restart, not hours.
3. The new owner deletes `WANT.<self>`.

**Operator moves.** `tsdb-cli ha move <vol> <node>` and `ha drain <node>` check that the target is a live candidate (TOPOLOGY/STATUS) before writing `handoff_to`.

## 7. Followers and reads

- **Lazy followers.** Standbys do not tail WALs. A node opens a follower for a volume only when it must serve a read for it, and closes it after 10 min idle.
- **Default read routing:** to the owner, directly or forwarded by the node that received the query (§9.1). If the owner cannot be reached, the receiving node serves the read from a lazy follower only when the client allows stale reads (the `STALE_OK` flag of HELLO), and only while the volume's storage is UP on that node: reads are never served from page cache during an outage (§15.3). Such replies carry a STALE flag: a follower refreshes on its own schedule, so it can miss rows the owner acked a moment ago, the connection's own writes included. Without `STALE_OK` the reply is UNAVAILABLE(UNREACHABLE). Read replicas can be pinned with `--follow-volumes` (open question 3).
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
- Today's single FIFO (tsdbd.c:830) and one-request-per-connection `busy` flag (tsdbd.c:917-924) are replaced by the per-volume order rule of §9.1 (D37): replies carry the frame id and leave as they complete, in request order per volume, and an operation on a volume waits for the connection's earlier frames on that volume. Frames for different volumes of one connection run concurrently, which forwarding needs to keep a pipeline full across a hop. Peer links are multiplexed: their order is per stream `(session, vol)`, kept by sticky links and the sequence rule.
- Forwarding (§9.1) runs on the loop and does no I/O to any volume. It never takes a worker.

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

## 9. Protocol changes (version 5)

> Numbering: protocol version 5 has since been used by LTTB downsampling (QUERY_LTTB, 2026-10-24), and 6 by
> time-weighted averages (QUERY_TWA). The changes in this design ship as the next free version (7 or later); read "version 5"/"v5" in this document
> as "the failover protocol version".


There is no release yet, so version 5 replaces versions 1-4 (D32). The server refuses a HELLO below 5, and any frame before HELLO, with INVALID and closes the connection. The protocol may keep changing before 1.0, provided the C, Rust, Python and Java clients change in the same commit (AGENTS.md). Ops that exist only for older layouts (INSERT, and the float32-only QUERY_RAW and QUERY_AGG) may be removed in the same change; nothing here depends on them. On-disk compatibility is not affected.

**HELLO.**
- Request: `u32 version, u8 session_id[16], u8 flags`. Flag `STALE_OK`: follower reads are acceptable while the owner cannot be reached (§7). Reply: `u32 version, u32 node_id, u8 db_id[16]`. Peer links use their own HELLO on the peer port (§9.1); `TSDB_PEER` on the client port is refused.
- HELLO is mandatory. Today the C client often sends none (tsdb_client.c:~662-678), and the server's per-connection version defaults to 0; both go away.
- The attempt is per stream and travels in the insert header (below), not in HELLO.
- When a session id is already held by an older connection on this node, the server detaches that connection's queued frames and replies at once (§15.3).

**INSERT_VALUES.** The 16-byte header (tsdb_protocol.h:44-47; reserved bytes checked at tsdbd.c:634-638) becomes 32 bytes, so the arrays still start 8-aligned:
- `u32 n, u32 series, u8 type, u8 flags, u16 vol, u32 epoch, u32 attempt, u32 0, u64 seq`.
- **Streams.** A stream is `(session, vol)`, whatever the frame's shape. Every insert of a session into volume v is ordered in v's stream, so a multi-volume frame and a single-volume frame for the same key cannot overtake each other.
- **Single-volume frames** (`vol < 0xffff`): `series`, and the `series[n]` array of MANY_SERIES frames, are `u32` local ids of volume `vol`. They are forwarded without decoding (§9.1).
- **Multi-volume frames** (`vol == 0xffff`, MANY_SERIES only): the header's `attempt` is 0, and the header is followed, outside any compression, by a volume table `u16 nv, u16 0, u32 0, nv × {u16 vol, u16 0, u32 attempt}`: the volumes the frame touches, each with the client's current attempt of that volume's stream. Then the array is `u64 series[n]` ids (§5), 8-aligned. The receiving node splits the rows per volume; each volume's rows become a sub-frame in that volume's stream, with the parent's seq and the attempt from the table (§9.1). A row whose volume is not in the table makes the frame INVALID.
- `epoch` 0 means "the current owner": the entry node stamps the epoch from its ownership cache when it reads the frame (§9.1). A client that knows the epoch (direct mode) sends it.
- `seq` is one counter per session, increasing over all of its insert frames, whatever their volume. The rule below only compares seqs, so one volume's seqs need not be contiguous. Packed frames carry the same header.
- `attempt` is per stream. It starts at 1, and the client increments it each time it resends that stream (item 5 below); a reconnect alone does not change it.
- **Sequence rule** on the owner, per stream, with the state `{epoch, max_attempt, last, blocked_at}`:
  - A frame whose attempt is below `max_attempt` gets UNAVAILABLE and changes nothing (the attempt fence). A frame of a higher attempt starts fresh `last` and `blocked_at`. Only an admitted frame raises `max_attempt`; a frame refused with MOVED does not. An attempt more than 2^16 above `max_attempt` is INVALID.
  - `last` is the seq of the last admitted frame. Within one attempt a frame is admitted only with `seq > last`, or as the resend of `blocked_at`, which sets `last` to it. A frame at or below `last` is a stale copy: UNAVAILABLE, and `blocked_at` is not touched.
  - A retryable non-OK reply to `seq = s` (MOVED, UNAVAILABLE, BUSY, IO, ABANDONED) sets `blocked_at = s`. Final replies (DROPPED, INVALID, NOTFOUND, STALE_ID, a type mismatch) do not: the client never resends those, so a block on them would never clear.
  - While `blocked_at` is set, a frame of the same attempt with `seq ≠ blocked_at` gets UNAVAILABLE with body `u64 resend_from`. `seq == blocked_at` clears it, and so do an empty frame with that seq (a client that gave up on the frame clears the block on purpose) and any frame of a higher attempt.
  - A new epoch starts fresh `last` and `blocked_at`; `max_attempt` is kept. The owner forgets a stream 10 min after its last frame, well past any client's retry budget.

**Drop watermark (D33).**
- Each format-3 volume has `dropped_before`, initially the minimum, and a generation `wm_gen`. `drop_before(ts)` raises it to `floor(ts / partition_ms) × partition_ms`, the end of the last partition it drops (db.c:633-635). It never decreases on its own. Watermarks compare as `(wm_gen, dropped_before)`.
- **Guard.** The owner refuses `drop_before(ts)` with INVALID when `ts > now − max(partition_ms, min_retention_ms)` on its own wall clock (default `min_retention_ms` 1 day, with 5 min of tolerance), unless the request carries FORCE. A retention job with a skewed clock, a seconds/nanoseconds mix-up or `drop_before(INT64_MAX)` would otherwise make the volume refuse all new data for good. `tsdb-cli volume lower-watermark <vol> <ts>` is the only way down: it writes a ledger record with `wm_gen + 1`, is logged, and so wins over every higher copy that nodes remember (D36).
- **Unavailable volumes.** A drop for a volume that is unavailable, or whose owner cannot be reached, is not deferred: that volume's status in the PARTIAL reply is UNAVAILABLE, and the retention job retries. drop_before is idempotent and the watermark makes a late drop safe. There is no pending-drop file: it would live on whichever node handled the request, which the volume's next owner never reads.
- Order: the DROP record is durable (§2.5); then the watermark is raised under `publish_lock` and `buf_lock`; then the memtable is flushed, skipping rows below it, and the parts of dropped partitions are retired.
- Inserts check it at admission, which decides the reply. They check it again where they already hold those locks: `mt_add` under `buf_lock`, and `parts_publish` under `publish_lock` before block numbers are assigned. Rows found below it there are discarded and count as inserted before the drop, so a request that raced with the drop cannot recreate a partition after it. WAL replay skips rows below it.
- A frame whose rows are all below the watermark gets `TSDB_ERR_DROPPED = -13`, body `u16 vol, i64 dropped_before`: definitely not applied, and final. A frame with some rows below it applies the others and replies PARTIAL (§15.4) with the volume's status DROPPED and those rows marked.
- Clients and the Pulsar sink never resend DROPPED rows; they count them (`tsdb_dropped_rows_total{vol}`). An alert fires when rows newer than `now − min_retention_ms` are DROPPED.
- `MANIFEST.<E>` carries `(wm_gen, dropped_before)`. A follower filters the rows it tails from the WAL by it, because rows admitted before a drop and discarded at `mt_add` may already be in the active segment.
- Every node remembers the highest `(wm_gen, dropped_before)` per volume from OWNERS (D36). An owner applies the higher of that and its ledger at claim and at recovery, before it admits inserts, so a pool rollback that reverted the DROP record cannot bring dropped partitions back.
- Volumes in format 1 or 2 have no watermark and behave as today until they are migrated (§14 step 3).

**SERIES / SERIES_CREATE.** Request: `u8 create (or type), u16 plen, path, u16 len, name`. The path comes first because it decides the route (§5). Reply: `u64 id, u32 epoch, u16 plen, path` (the canonical path, §5).

**New ops (next free op numbers).**
- `TOPOLOGY` → `u32 nn, nn × {u32 node_id, u8 alive, u16 len, addr}`, then `u32 n, n × {u16 vol, u8 role, u8 0, u32 epoch, u16 len, addr, u16 plen, path}`. Any node builds it from its ownership cache (§9.1) and its `volumes.cache`, with no NFS reads. Addresses come only from NODES.
- `LIST_SERIES` → request `u16 plen, path, u64 cursor, u32 limit` (an empty path lists every volume); reply `u32 n, n × {u64 id, u8 type, u16 len, name}, u64 next_cursor` (§5).
- `STATUS` → `u32 len`, followed by `key=value` text: node identity, and per volume the role, epoch, lease time left, probe RTT, gate refusals, fences, takeovers with per-step durations, ledger and WAL fsync p99, follower lag; per peer the forwarded frames and bytes, the window in use and the hop latency. `tsdb-cli ha status` and `--metrics-port` (Prometheus text) serve the same data.
- `FORWARD`, `OWNERS`, `VOLUMES_GEN`, `VOLUMES_GET`, `SESSION_FENCE` and `CREDIT`, on peer links only (§9.1).

**New statuses.**
- `TSDB_ERR_MOVED = -10`, body `u16 n, n × {u16 vol, u32 epoch, u8 flags, u32 node_id, u16 len, addr}`, then `[u64 resend_from]` when the frame carried a seq. Sent before engine work only: definitely not applied. `epoch` is the sender's current view, never a predicted one; the HANDOFF flag means a clean release is under way (§2.6). Between nodes only `node_id` counts; the address a client sees is filled in from NODES (D35).
- `TSDB_ERR_UNAVAILABLE = -11`, body in §15.4. Sent before engine work only.
- `TSDB_ERR_PARTIAL = -12` (§15.4), `TSDB_ERR_DROPPED = -13` (above), `TSDB_ERR_STALE_ID = -14` (final: re-resolve the id by path and name, §5) and `TSDB_ERR_EXISTS = -15` (library without HA, §5).
- Errors after engine work started are `TSDB_ERR_IO` (indeterminate). So is a connection lost while a frame is outstanding, whether to the owner or to an entry node.

**All four clients** (C `client/tsdb_client.c`, Rust `client/rust/src/client.rs`, Python via `client/python/src/lib.rs`, Java `TsdbClient.java`) must:
1. Take a list of nodes (`host:port,...`) and shuffle it once per client; any one that answers is enough. Send HELLO v5 with a random session id.
2. **Direct mode** (the default for every client, D39): fetch TOPOLOGY from any node of the list, keep a cache `vol → (addr, epoch)` from it, replaced by a higher epoch, or by a lower one when TOPOLOGY from two nodes agrees on it (D36), and send each volume's frames to its owner with that epoch. Addresses are the owners' `--advertise` addresses from NODES, so a direct-mode client must be able to reach every node. **Fallback to forwarding:** a stream goes through any live node of the list with epoch 0, exactly as in simple mode, (a) before the first TOPOLOGY reply, (b) after a MOVED without HANDOFF, until a refreshed TOPOLOGY (fetched at once) shows an epoch at or above the one the MOVED named, and (c) while the owner cannot be reached (connect failure, `TCP_USER_TIMEOUT`), until TOPOLOGY names a reachable owner. A MOVED with HANDOFF marks the volume handoff-pending instead (§2.6): its frames wait, they are not forwarded. **A stream changes path** (direct ↔ forwarded, or to another owner) only when none of its frames is unanswered, or as a resend with attempt + 1 (item 5), so a frame on the old path can neither overtake nor be overtaken by one on the new path. Queries and SERIES ops follow the same routing, without streams.
   **Simple mode** (explicit opt-in, for tools such as `tsdb-cli`, ad-hoc scripts, and clients that can reach only some nodes, e.g. through NAT): send every request to one node of the list with epoch 0, and let it forward. Every client library exposes it as an option (`mode=simple`); nothing selects it implicitly.
   In both modes, on a lost connection, or one it gives up on, abort it with an RST (`SO_LINGER` 0), so that the old node's kernel discards bytes it has not read yet; then reconnect with full-jitter backoff, to the owner (direct) or to a random live node of the list (simple, and the forwarding fallback). Every ~10 min (jittered), an idle connection to an entry node moves to another random node, so load spreads back after a node returns.
3. In direct mode, split MANY_SERIES batches per volume (the id carries it, §5; already "durable per volume", volumes.c:~610-611; document it) and pipeline each volume in its own stream, with seqs from the session's counter. In simple mode, a multi-volume frame (`vol = 0xffff`) is allowed. The forwarding fallback of direct mode keeps sending single-volume frames, so a stream's frames have the same shape on both paths.
4. Keep every unacked packed frame. C must copy frames out of its shared zstd buffer (tsdb_client.c:~686-720) and needs a window cap. Rust (`MAX_PIPELINE` 1024) and Java (`MAX_IN_FLIGHT` 4096) already have caps.
5. On a retryable non-OK, an I/O error or a timeout: refresh TOPOLOGY (direct mode), reconnect if needed with the same session, increment that stream's attempt, and resend every frame of the stream from the first refused or unacked one, in order, **including frames acked after it**, with their original seqs, stamped with the new epoch (direct mode to the owner) or 0 (simple mode, and the forwarding fallback). Replies to frames of an older attempt are ignored. After a PARTIAL, resend only the rows of the refused volumes: each volume's rows as a single-volume frame in that volume's stream, with the parent's seq. Never resend rows with a final status (DROPPED, INVALID, NOTFOUND, STALE_ID). A resend starts with at most 64 frames in flight and doubles that window with each round of OKs, so a node that inherits many clients is not flooded.
6. **Retry by time budget, not by count.** The default budget is 30 s (lease + margin + takeover p99), and UNAVAILABLE does not count against it.
7. **Detect a dead owner or entry node** with `TCP_USER_TIMEOUT` and keepalive at about 3 s. A frame is given up on no earlier than 3 s after it was sent, which `fwd_max_age` relies on (§9.1). While any frame has been outstanding for more than 1 s, poll TOPOLOGY on another node every `refresh_ms` and resend as soon as the epoch increases (finding 21).
8. Connections become recoverable instead of permanently `broken`/`lost` (client.rs:~895-905, Java ~1413-1419). C checks reply ids and reports a status per frame (tsdb_client.c:~201-241).
9. Map -10 to -13: Rust `Status` (error.rs:~38-47), Java `Status`, `tsdbc_strerror`, `to_pyerr` (lib.rs:~41).
10. Carry `u64` series ids (§5) everywhere an id is held today.

**Resend semantics.** Ordered at-least-once per session and stream, through any node. Idempotent under last-write-wins if no other producer writes the same keys. Rows below a volume's drop watermark are refused (D33), so a resend cannot undo a `drop_before`. This is documented in the client API.

### 9.1 Forwarding between nodes (D29-D31, D35, D37, D38)

**The entry node** is the node a request reaches. It decides by the address alone, in memory, without I/O:
- an insert by the `vol` in its header, or per row for a multi-volume frame; a query or STATE op by the `vol` in its series id; a SERIES op by its path (§5).
- If it owns the volume, it serves it.
- If its cache says the volume's storage is unavailable (the health the owner publishes, below), it replies UNAVAILABLE with the owner's reason itself. Forwarding cannot help.
- If no owner is known (PROMOTING, or nobody holds the lock), it replies UNAVAILABLE(NO_OWNER) with `retry_after` 1 s. If the volume is handoff-pending (§2.6), it replies UNAVAILABLE(PROMOTING) with `retry_after` 200 ms.
- Otherwise it forwards the request to the owner. A request that cannot be sent within `fwd_wait_ms` (default 2 s), because the owner's link is down or the owner is unknown, gets UNAVAILABLE(UNREACHABLE or NO_OWNER) with `retry_after`. That answer is definite, because nothing was sent.

**Ownership cache** on every node: `vol → {node, epoch, health, reason, since, gen}`.
- Its own volumes come from its slots, which are authoritative.
- Peers push `OWNERS` over the peer links on every role or health change, and every 1 s: the volumes the sender owns, and only those; an entry for another node's volume is ignored. An entry is replaced by a higher epoch, or by the same epoch with a higher `gen` from the same node. Epochs at or above 2^24−1 are refused.
- MOVED replies carry epochs and update the cache by the same rule. A MOVED with HANDOFF only marks the volume handoff-pending.
- **The slot decides, in both directions.** When the cached owner answers MOVED with a view that conflicts with the cache, or OWNERS from two nodes conflict, the node does one asynchronous slot read for the volume and accepts its `(node, epoch)` even if the epoch is lower. A pool rollback (§15.5) or a buggy peer therefore cannot pin misrouting until every node restarts.
- **Who reads slots.** Candidates of v already read its slots every 1 s (§6), so they know whether the owner still holds the lock. Other nodes never read slots on the request path: they wait for the new owner's OWNERS push and, after `fwd_wait_ms`, reply UNAVAILABLE. The conflict reads above run on one shared thread per node, coalesced to one per volume, with a deadline, and never while this node's fs domain for the volume is not UP. When a node's own slot reads show that the unreachable owner still holds the lock at the same epoch, the owner is alive but this node cannot reach it (D14): UNAVAILABLE(UNREACHABLE), and the client tries another node.
- **High-water marks** (D36). OWNERS entries also carry the owner's SERIES and STATES lengths, `(wm_gen, dropped_before)` and the volume's uuid, and the message carries the sender's VOLUMES generation and prefix hash. Each node keeps the highest of each per volume and persists them in `volumes.cache` every 10 s. A peer whose uuid for vol k differs from this node's makes k ROOT_DIVERGED here (UNAVAILABLE) until an operator runs `volumes repair`.
- Health is the owner's view, not the entry node's own probe, so a mount problem on the entry node does not refuse requests the owner can serve.
- Cost at 1000 volumes and 20 nodes: each node pushes about 50 entries per second to 19 peers, a few KiB/s. In steady state no node reads a slot to route.

**Forwarded frames.** `FORWARD`: `u8 session[16], u32 epoch, u16 hops (= 1), u16 0, u32 origin_node, u64 frame_id`, then the client's frame as received: `u8 op` and the payload. The attempt is in the insert header.
- The payload is the client's bytes, packed or not. A single-volume insert is sent from the receive buffer as it is, with no decoding and no copy beyond the send. The envelope's epoch replaces a 0 in the inner header.
- The owner applies the epoch check and the sequence rule, with its attempt fence, exactly as for a direct frame, and replies on the peer link with the inner reply (status and body) unchanged. Streaming replies (QUERY_*) travel on query links under credit (back-pressure, below).
- A node that receives a FORWARD for a volume it does not own at that epoch replies MOVED with its own view and never forwards it again (D30). However stale two caches are, a frame makes at most one hop.
- The entry node relays every reply as it is, including MOVED and UNAVAILABLE with `resend_from`, and updates its cache from MOVED. It never retries on its own (D30). The client resends (§9 item 5), and the updated cache sends the resend to the new owner. The cost is one client round trip per failover.

**Order across a hop (D31).**
- **Sticky links.** All frames of a stream `(session, vol)` go over one peer link. The link is chosen by hash when the stream has no unanswered frame and kept while it has any, so a change of `peer_links` or a replaced link never splits a stream's in-flight frames over two sockets. A peer link is FIFO, and the owner's per-volume queue keeps arrival order.
- **Pinned epoch.** While a stream has unanswered frames, the entry node stamps and routes its new frames with the same `(owner, epoch)` as the earlier ones, even if an OWNERS push has changed the cache meanwhile; they then fail at the old owner as the earlier ones do. The cache's epoch is used for a stream with nothing outstanding, and for a resend (a higher attempt).
- **The entry node blocks too.** It keeps `blocked_at` per stream for every retryable non-OK it makes or relays: its own UNAVAILABLE, BUSY, NO_OWNER and UNREACHABLE; the owner's MOVED, UNAVAILABLE and BUSY; IO from a broken peer link; a timeout. Later frames of the stream get UNAVAILABLE(resend_from) until the resend arrives (`seq == blocked_at`, or a higher attempt). A frame whose link failed is never sent again on that link. So no frame overtakes a refused one, whichever node refused it, and later frames of a stream cannot reach a new owner before the refused one is resent.
- **Multi-volume frames.** The volume table (§9) names a multi-volume frame's volumes before anything is decoded, so the entry node puts the frame into those volumes' streams as soon as it reads it. Later frames of the connection on those volumes wait until it has been split and its sub-frames sent; frames on other volumes do not wait.
- **Per-volume reply order** (D37). Replies carry the frame id and leave as they complete. Replies to frames on v leave in request order, and an operation on v (a query, STATE, a SERIES on v's path) waits until the connection's earlier frames on v have replied, so a connection reads its own writes, as with today's one-at-a-time rule (tsdbd.c:917-924). COMMIT, FLUSH, OPTIMIZE, STATS and drop_before wait for every earlier frame of the connection.
- **Attempt fence.** The owner keeps `max_attempt` per stream (§9). A client resends a stream with its attempt+1, through any node. An old copy that reaches the owner after the resend is refused; one that reaches it before is harmless, because the resend follows it.
- **Old frames on a stalled entry node.** The fence lives in the owner's memory, and a new owner after a failover has none. So an entry node never forwards an old frame:
  - It stamps the epoch when it reads the frame, not when it sends it.
  - It forwards a frame only within `fwd_max_age` (default 1 s) of its arrival. The age is the larger of the time since the kernel received it (`SO_TIMESTAMPING`, U23; a frame that sat unread in a socket buffer during a stall counts from arrival) and the time since the loop read it (`CLOCK_BOOTTIME`). Clients give up on a frame no earlier than 3 s after sending it (§9 item 7), so a frame is forwarded before its client can have resent it elsewhere, or not at all.
  - Before it forwards a frame, also from the per-peer queue or after a loop stall, it checks the client connection (`POLLRDHUP|POLLHUP|POLLERR`) and drops the frame if the client has gone: nobody could receive its reply. A client aborts a connection it gives up on with an RST (§9 item 2), so the kernel also discards bytes not yet read.
  - When a stream's attempt rises, the entry node sends `SESSION_FENCE{session, vol, attempt}` to every peer, best effort. Peers drop queued frames of that stream from older attempts and remember the attempt for 10 min, so a new owner refuses an old copy even if it never saw the stream before.

**What an OK means.** An OK through an entry node means the owner replied OK: durable on the volume's storage, exactly as for a direct frame. The entry node never acks on its own. If it dies, stalls or loses its link to the owner while a frame is in flight, the client's outcome is indeterminate (IO or a timeout). The client resends from the first unacked frame to another node with attempt+1. The resend writes the same values, which is idempotent under last-write-wins, and the attempt fence keeps the old copy from landing after it.

**Requests that span volumes.**
- A multi-volume insert goes to the decode pool (§15.3): zstd and unshaping, then a stable split per volume (later rows stay later, as in `insert_slots`, volumes.c:612-662). Each volume's rows become a single-volume sub-frame in that volume's stream, with the parent's session and seq and the attempt from the volume table, re-packed if the input was packed. Local volumes are applied and the others forwarded, all concurrently, under the stream rules above.
- The entry node merges the replies: OK if every volume replied OK, otherwise PARTIAL with each volume's status and the not-applied rows marked in the parent's row order (D25). A volume that replied IO has its rows marked indeterminate.
- COMMIT, FLUSH, OPTIMIZE, STATS, drop_before and an all-volume LIST_SERIES fan out per owner the same way (per-volume subtasks, §15.3). Missing volumes are reported through PARTIAL (§15.4).
- Every query op, QUERY_GROUP included, names one series and so one volume: it is forwarded, never fanned out. LAST_STATS describes the connection's last query, so the entry node remembers which node ran it and forwards LAST_STATS there.

**Back-pressure** (D37).
- The owner's per-volume queue is bounded (`vol_queue_bytes`, §15.3). When it is full the owner replies UNAVAILABLE(BUSY) with `retry_after`, and the entry node relays it. Once a volume's in-engine age passes `admit_deadline`/2, the owner refuses forwarded frames for it early with UNAVAILABLE(STUCK).
- **Per peer and volume.** The entry node keeps at most `fwd_vol_window_bytes` (default 8 MiB, and at most a quarter of the peer window) in flight per `(peer, volume)`, inside the per-peer total `fwd_window_bytes` (64 MiB). A frame that does not fit waits in that volume's share of the per-peer queue (`fwd_queue_bytes`, default 256 MiB, at most 4 × `fwd_vol_window_bytes` per volume). When its share is full the entry node replies UNAVAILABLE(BUSY) for that volume alone, under the sequence rule above. A stuck volume on owner B then holds back only its own frames, and B's other volumes keep their throughput through every entry node.
- **Per connection and volume.** Unanswered bytes are counted per `(connection, volume)`. When one volume's count passes `conn_vol_window_bytes` (default 16 MiB), the entry node keeps reading the socket and refuses that volume's further frames with BUSY. Only when the connection's total passes `conn_window_bytes` (64 MiB) does it stop reading, which is ordinary TCP back-pressure on that client alone. A multi-volume frame counts against the total until it is split. A slow owner A thus does not stop a simple-mode client's traffic to B and C.
- **Streamed replies.** QUERY_* replies travel on separate query links (`peer_query_links`, default 1 per peer), never on the links that carry inserts and their acks. Each request has a credit window (default 4 chunks of up to 1 MiB): the owner sends a chunk only within its credit, and the entry node grants more (`CREDIT`) only as its client's socket drains. Reply bytes buffered per client connection are capped (`conn_reply_bytes`, default 16 MiB). A slow reader stalls only its own query.

**Peer links** (D35).
- Each node keeps `peer_links` insert links (default 2, up to 8 under load) and `peer_query_links` query links to every other node in NODES, opened at start and reopened with backoff. A stream stays on its link while it has unanswered frames (above).
- Peers connect to a separate peer port (`--peer-port`), bound to the cluster network, and are accepted only from NODES addresses. The peer HELLO carries the node id, the database id and an HMAC challenge-response over the cluster secret, which `tsdb-cli ha init` writes to `/etc/tsdb/cluster.key` (mode 0600, outside the data exports). A mismatch is refused, so neither a client nor another cluster can pose as a peer. The database id alone proves nothing: every HELLO reply returns it.
- Between nodes, nodes are named by `node_id` only; an address is looked up in NODES, never taken from a peer's message.
- `TCP_USER_TIMEOUT` and keepalive at about 3 s, as for clients. A link that breaks fails its frames in flight with IO, which the entry node relays as indeterminate and treats as a block of each affected stream.
- Peer links are multiplexed: the owner admits their frames into the per-volume queues like any other frame and replies as each completes, matched by frame id.
- 20 nodes × 19 peers × 3 links is 1140 connections in the cluster.

**Peer isolation (D38).** With forwarding, simple-mode clients, and direct-mode clients in their forwarding fallback, reach a volume only through the peer network, so an owner that reaches NFS but no peer would leave its volumes unreachable for them, with no failover (D14). Every node records, in its `PROBE.<node_id>` files (§15.2), the set of nodes it has live peer links to. A node that, for longer than `lease/2`, reaches fewer than half of the nodes alive on NFS (their probe files advanced within 15 s), while its own NFS probes succeed and some node alive on NFS reports reaching more nodes than it does, is the cut-off one. It does a clean release of every volume it owns (§2.6, without `handoff_to`), stays out of polling for 60 s, raises an alarm and counts `tsdb_peer_isolated_total`. In a symmetric split, such as two nodes that lose each other, no node is better connected, so nobody releases. The release is voluntary, so fencing is unchanged. STATUS counts UNREACHABLE replies per owner.

**Cost.**
- Latency: one cluster round trip plus a copy per forwarded request, expected to be tens of microseconds on one switch (U22), next to the 1-2 ms NFS `fdatasync` on the ack path.
- Bandwidth: a forwarded frame crosses the cluster network twice. With requests spread evenly over N entry nodes, a fraction (N−1)/N is forwarded: 95% at 20 nodes. Packed frames keep this small next to the WAL and part traffic to NFS, but it is not free.
- CPU: only the send for single-volume frames; one decode and one re-pack for multi-volume packed frames.
- Clients and the Pulsar sink therefore route directly by default (direct mode, D39) and use forwarding as the fallback (§9 item 2). `tsdbc-bench --via <node>` measures both (§14 step 8).

**Protocol additions, peer links only.**
- `FORWARD` (above).
- `OWNERS` → `u64 gen, u32 node_id, u64 volumes_gen, u64 volumes_prefix_xxh64, u32 n, n × {u16 vol, u8 uuid[16], u32 epoch, u8 health, u8 reason, i64 since_ms, u32 series_n, u32 states_n, u32 wm_gen, i64 dropped_before}`. Entries name only the sender's own volumes.
- `VOLUMES_GEN u64 gen`, pushed by `tsdb-cli volume add`; nodes then reload VOLUMES in the background (§15.3).
- `VOLUMES_GET u64 from_gen` → the VOLUMES lines after that generation. A node behind a peer's generation fetches them this way, so a root outage does not keep it from learning a new volume. It checks each line against the volume's META (one O_DIRECT read on the opener thread, with a deadline) before routing by it.
- `SESSION_FENCE u8 session[16], u16 vol, u32 attempt` (order across a hop, above).
- `CREDIT u64 frame_id, u32 chunks` (streamed replies, above).
- UNAVAILABLE gains the reasons NO_OWNER, UNREACHABLE, RELOADING and MISMATCH.

**Without HA**, a single tsdbd owns every volume it opened, at epoch 0, and forwards nothing.

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
| NFS server restart | Grace starts when nfsd starts, after the reboot. The owner reclaims within grace (nfs4proc.c:L10422) and is SUSPECT meanwhile. Standbys get NFS4ERR_GRACE and wait. An owner whose RDMA reconnect backoff outlasts grace becomes LOST, FENCED, then fails over by cold open; no acked write is lost (§15.1, D34). Takeovers are capped at 4 per server group per node (§4), and such a loss gets no back-off (§6). | 0, or grace + poll + open | U4. The epoch read-back catches a reclaim that should have been refused. |
| NFS server crash during a slot write | The torn slot is never the newest (strict alternation). E is also recovered from the checkpoint and EPOCH records. | — | L2. |
| Transient EIO or ENOSPC on a fenced fd | FENCED and abandon (poison, no close), then a fresh claim by any candidate. This node does not claim again until it has drained. | poll + open | §2.6. The fd number cannot be reused. |
| EIO or ENOSPC on a tmp part (unfenced) | The flush or merge fails and the tmp is unrecorded, so it is retried. The active memtable is rotated before any direct insert (bug fix B5). | none | Unrecorded tmps are never adopted. |
| One file stuck, server healthy | No progress for 3×lease while other probes succeed: kill, then escalation. | ≈ 30 s + kill | The lock is held until the process exits. |
| One volume stuck | Its frames queue per volume. Other volumes and reads keep their workers. | — | §8 admission. |
| Handoff target dies | The `handoff_to` hint is honoured once for 2 s, then ignored. | 2 s + poll | §4 step 2. |
| Rank 1 and rank 2 both down | The heartbeat is stale for `lease/2`, so every candidate polls. | lease/2 + poll | §6. |
| Root volume's storage down | Routing, creation and inserts continue from `volumes.cache`. Only `volume add` fails. | — | D9, §5. |
| Entry node dies with frames in flight | The client sees IO or a timeout (indeterminate), reconnects to another node with attempt+1 and resends from the first unacked frame. Copies still on the dead node's peer link land before the resend or are refused by the attempt fence. | client reconnect (≈ 3 s) | D31, attempt fence. The entry node never acks on its own. |
| Entry node's ownership cache stale during failover | The old owner, or a fenced or non-owning node, replies MOVED; the entry node updates its cache and relays MOVED; the client resends. | one client round trip | D30: one hop, epoch check at the owner. |
| Entry node cannot reach the owner, NFS fine | UNAVAILABLE(UNREACHABLE or NO_OWNER) within `fwd_wait_ms`, definite. The client tries another node. No failover. | none | D14. |
| Owner cut off from all its peers, NFS fine | Better-connected nodes alive on NFS show it is the isolated one; it releases its volumes and candidates take them. A symmetric split (two nodes losing each other) changes nothing. | lease/2 + release + poll | D38: a voluntary release, fencing unchanged. |
| Entry node paused (SIGSTOP, VM pause) during a failover, frames buffered in its sockets or queues | On resume it drops frames older than `fwd_max_age` or whose client has gone; frames stamped with the old epoch get MOVED; SESSION_FENCE has told the peers the stream's newer attempt. | — | D31. |
| `peer_links` grows, or a link is replaced, mid-pipeline | Streams with unanswered frames keep their link; the frames of a broken link fail with IO and block their streams until resent. | — | D31, sticky links. |
| OWNERS push lands in the middle of a pipeline | The stream keeps its stamped epoch until it has nothing outstanding; a refusal at the old owner blocks the stream on the entry node. | — | D31. |
| Pool rollback of LOCK, SERIES and the ledger (`zpool import -F`, SLOG loss) | The next claim takes an epoch above every node's high-water mark; lost ids become LOST (STALE_ID); the highest watermark is re-applied; DIVERGED. | cold open | D36. |
| VOLUMES rolled back, then `volume add` from a host without a cache | Refused: a live node's cache is longer or differs. A node whose line k has another uuid than a peer's treats k as ROOT_DIVERGED. | — | D36, §5. |
| Slow reader of a large query | Its credit runs out and only that query waits. Insert acks use other links. | — | D37. |
| `drop_before` with a future timestamp or a wrong unit | INVALID unless FORCE; `volume lower-watermark` undoes a forced mistake. | — | D33. |
| Owner's storage down, request reaches another node | The entry node replies UNAVAILABLE with the owner's published reason, without a hop. | none | D22, §15.2. |
| One volume stuck or slow on an owner, reached through other nodes | That volume's share of each entry node's window and queue fills; entry nodes reply BUSY for that volume only, and the owner's other volumes keep their throughput. | — | D37. |
| Late resend or replay below a dropped partition | Refused with DROPPED (final) or skipped in replay. | — | D33. |
| Clock step on the owner | Deadlines are monotonic. Checkpoints store remaining lifetimes. | — | D11. |
| Second tsdbd on the same node | It becomes a candidate (standby). | — | L1. |
| Pre-HA binary | META v3 refuses it (db.c:228). | — | — |
| Library or CLI writer on an `ha=1` volume | Refused (D5). | — | — |
| Writer on a `local_lock` mount | Preflight refuses. | — | §2.1. |
| Legacy writer still running at `ha init` | `ha init` refuses without the unmount attestation. | — | §4. |
| Deleted LOCK | Opens refuse (no `O_CREAT`). | — | §2.2. |
| Duplicate `nfs4_unique_id` | The slot's `nfs_client_id` matches under another `node_id`: refuse to start and alarm. | — | — |

## 11. Configuration

Step-by-step setup of the NFS servers and the mounts on AlmaLinux 10, with verification commands: [nfs-server.md](nfs-server.md).

- **NFS server:** `/etc/nfs.conf` `[nfsd] lease-time=10 grace-time=10` (the user's choice, D34; 10 s is the floor for both, nfsctl.c:L949; the proc files can be written only while nfsd is stopped) and `vers4.0=n` (a v4.0 client never sends RECLAIM_COMPLETE and would hold every grace open to its full length). Fallback if forced reconnect (U41) is not possible: `grace-time=45` (D34, §15.1). Exports `rw,sync,mp,no_subtree_check` with a fixed `fsid=<uuid>`, one per ZFS pool, no `crossmnt` (§15.2); `fs.leases-enable=0` (no delegations, U2); ZFS `sync=standard` and a mirrored SLOG.
- **nfsdcld is required**, not only for the early end of grace. The EL10 kernel is built without legacy client tracking (`CONFIG_NFSD_LEGACY_CLIENT_TRACKING` is off in the CentOS Stream 10 kernel config; on AlmaLinux 10 check `grep NFSD_LEGACY /boot/config-$(uname -r)`; upstream's Kconfig default is y), so without nfsdcld nfsd refuses every reclaim (`nfsd4_client_record_check` → `nfserr_reclaim_bad`, nfs4recover.c:2136-2139, nfs4state.c:8542-8543) while still running the full grace, and every node loses its locks on each server restart. With nfsdcld, grace ends once every client in its reclaim table has sent RECLAIM_COMPLETE (U40), is skipped when the table is empty, and otherwise lasts `grace-time`, extended about every second for as long as some client reclaimed since the previous check (`clients_still_reclaiming`, nfs4state.c:6277-6293). That function means to stop the extension at 2 × lease = 20 s after nfsd started, but in 6.12 it compares `ktime_get_boottime_seconds()` with `boot_time`, which is wall-clock time (`ktime_get_real_seconds()`, nfs4state.c:8598), so the cap never fires: grace ends only when reclaims stop. Whether the EL10 kernel fixes this: UNVERIFIED (measure in O0). Its database (`/var/lib/nfs/nfsdcld/main.sqlite`) lives on the server's local OS disk. **The user believes nfsdcld is not running on the servers today** (2026-10-24); [nfs-server.md](nfs-server.md) §2.4 explains how to check (EL10's `nfs-server.service` already has `Wants=nfsdcld.service`, so it may be running anyway) and how to enable it. Enabling it needs an nfsd restart in which every client loses its locks once, so it is done before HA goes live.
- **NFS clients:** `vers=4.2,proto=rdma,port=20049,hard,timeo=100,retrans=2,lookupcache=positive`, no `local_lock`; `options nfs recover_lost_locks=0 nfs4_unique_id=<uuid>`, a fresh uuid per node (the parameter is `nfs.nfs4_unique_id`; nfs(5) writes `nfs4.`, which does not match the source).
- **tsdbd:** `--ha --node-id --advertise --volumes --ha-lease-ms 10000 --ha-margin-ms 2000 --ha-poll-ms 250 --nfs-grace-s 10` (the servers' grace, for the standby hold-back, §15.2); forwarding: `--peer-port --cluster-key /etc/tsdb/cluster.key --peer-links 2 --peer-query-links 1 --fwd-window-bytes 64M --fwd-vol-window-bytes 8M --fwd-queue-bytes 256M --fwd-max-age-ms 1000 --fwd-wait-ms 2000 --conn-window-bytes 64M --conn-vol-window-bytes 16M --conn-reply-bytes 16M`; `--min-retention-ms 86400000` (D33). HA mode refuses `--no-fsync`, `--wal-sync-ms ≠ 0` and `obsolete_part_lifetime_s < 120` (D15).
- **Logging:** every transition is logged with vol, epoch, node, monotonic time and wall time.

## 12. Backend interface (`src/ha.h`)

```c
typedef struct ha_backend_ops {
    int  (*acquire)(ha_vol *, ha_grant **out);            /* non-blocking; BUSY; never a test-lock */
    int  (*claim_epoch)(ha_grant *, uint32_t *E);         /* max(slots, checkpoint, EPOCH, high-water)+1 */
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
- Series addressing (§5): the same name in two volumes is two series; two nodes creating the same name in one volume give one id; creation in volume 1 while volume 2's storage hangs; a path not in VOLUMES gives NOTFOUND and creates nothing; path matching makes no syscall on NFS (crashfs `hang` on every prefix); an existing format-2 database with orphans opens with every SERIES entry addressable; path normalisation (a trailing `/`, `..` refused, an alias); `volume add` refusing a bind mount of an existing volume, a second mount of its export, a symlink to it and a nested path; an open whose META uuid differs gives DOWN(MISMATCH); LIST_SERIES and `tsdb-cli series` over two volumes; `tsdb_series_at` with a name that exists in another volume gives EXISTS (no HA); `ha init` refuses a root that holds series.
- Drop watermark (D33): an insert racing `drop_before` never recreates a dropped partition (inject a stall between admission and `parts_publish`, and between admission and `mt_add`); a frame wholly below the watermark gets DROPPED; a mixed frame gets PARTIAL with exactly the old rows marked; WAL replay after kill -9 skips rows below it; the watermark survives a checkpoint and a takeover. Regression test: today an insert after `drop_before` recreates the partition (db.c:624-646). Also: a future `drop_before` gives INVALID without FORCE, and `volume lower-watermark` lowers it; a follower does not show rows below the watermark that are still in the active segment; a drop while a volume is down gives PARTIAL(UNAVAILABLE), and after a failover the retried drop holds and resent old rows get DROPPED; a rollback that reverts the DROP record re-applies the watermark from the high-water marks.
- Forwarding (§9.1):
  - the entry-node sequence rule for its own refusals and for relayed MOVED, UNAVAILABLE and peer-link IO; an OWNERS push in the middle of a pipeline: no frame after s is applied before s is resent;
  - a multi-volume frame, then a single-volume frame on the same key, with an injected decode delay: the later value wins;
  - `peer_links` grown from 2 to 8 in the middle of a pipeline, and a link killed with frames in flight: per-stream order holds;
  - the attempt fence per stream, with an old-attempt copy arriving before and after the new attempt's first frame, and in direct mode with one session's connections to two owners: no cascade of refusals;
  - direct mode by default (D39): a client with no TOPOLOGY yet forwards, then switches to the owner; a MOVED during a pipeline sends the stream through an entry node until TOPOLOGY is refreshed, then back to the owner; an injected delay on the forwarded path while the client switches: no frame overtakes another of its stream (the test must fail on a client build that switches path with frames in flight);
  - a frame wholly below the watermark, then normal frames on the same stream: they are applied (DROPPED does not block);
  - replies per volume: a slow owner A does not delay replies for B and C on the same connection; one volume stalled on B leaves B's other volumes at full throughput through a third node;
  - a slow-reading QUERY_RAW client next to an insert pipeline through the same entry node and owner: the insert p99 holds and the link survives;
  - MOVED is never forwarded a second time; a handoff MOVED gives PROMOTING, not a bounce; `fwd_wait_ms` bounds a wait on a dead owner;
  - a cached epoch above the slot's (rollback) is replaced after one slot read; OWNERS for another node's volume is ignored; a peer HELLO without the cluster key, and `TSDB_PEER` on the client port, are refused;
  - a stale `volumes.cache` with a peer at a higher VOLUMES generation gives RELOADING, then the volume, never NOTFOUND;
  - peer isolation: an owner cut off in a 3-node cluster releases its volumes; a symmetric 2-node split changes nothing;
  - LAST_STATS after a forwarded query returns the owner's numbers;
  - PARTIAL merge of a multi-volume frame in the parent's row order.

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
- a writer open of an `ha=1` volume without HA management is refused (finding 2);
- a client in simple mode on node A writing to volumes owned by B and C: kill B, then kill A mid-pipeline; the client resends through C and no acked write is lost, no write lands out of order;
- a stalled entry node (SIGSTOP) holding frames in its socket buffers and its per-peer queue, resumed after the client resent through another node and after the volume failed over: no old copy is applied (age limit, client check, epoch, SESSION_FENCE);
- packed single-volume frames forwarded byte for byte (checksum of the bytes the owner receives).

Every case also runs with `FENCE=none`, to show which belt holds on its own.

**`make jepsen-ha`.** `--nodes 3 --volumes 4`, about 20 s per nemesis: lease, partition, node-pause, pause-watchdog, kill, power-nfs, nfs-stall, handoff, and mix. It keeps checks 1-4 from jepsen.c, plus:
- 5: no ack after a newer grant;
- 6: monotonic reads per session;
- 7: one committing epoch at a time, from the witness journal;
- 8: every node converges, and the restart comparison stays;
- 9: client-measured failover (fault to first client-visible ack) under 3L;
- 10: clients in simple mode through random entry nodes, with an `entry-kill` nemesis (kill -9 and pause of entry nodes only, also while a failover runs) and a `peer-partition` nemesis (links between nodes cut, NFS fine): checks 1-9 hold, and no write lands after a newer write of the same session;
- 11: a rollback of one volume's SERIES and LOCK, then a cold open on another node: no local id gets a second name, no epoch is reused, and caches follow the new owner.

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
6. **Per-volume slots.** `volumes.c`, `include/tsdb.h`: slots with pins, claim and release, backend chosen by filesystem, `ha=1` refusal, MOVED and UNAVAILABLE in the library, background VOLUMES reload for every role (§5). Proves: library-level failover tests and `test_volumes`.
7. **Series addressing and drop watermark.** `include/tsdb.h` (`tsdb_sid`, `tsdb_series_lookup` with a path, `tsdb_volume_series_count` and `tsdb_series_foreach` in place of `tsdb_series_count`, `TSDB_ERR_STALE_ID`, `TSDB_ERR_EXISTS`), `volumes.c` (ids `(vol, local)`, no CATALOG writes, no `cat_*`, path normalisation and aliases without syscalls, volume number and uuid in META, LOST entries, `tsdb-cli volume add` with its refusals, `volume migrate-root`), per-volume group-committed creation and `tsdb_series_create_many`; the watermark with its guard, generation and `lower-watermark` in `db.c`, `insert.c`, `memtable.c`, `manifest.c` and the ledger. Call sites that assume dense `u32` ids change with it: `tools/tsdb-cli.c:496-499`, the SERIES_NAME, SERIES_TYPE and STATE handlers in `server/tsdbd.c` (463-470 and its neighbours), `client/tsdb_client.c:312-316` and the other clients, and the ids built from loop indices in `tests/test_tsdb.c` and `tests/test_server.c`. Proves: the addressing and watermark unit tests, `test_volumes`, `test_types` still reading v1 and v2 databases, and the watermark regression test failing on the old code.
8. **tsdbd.** `server/tsdbd.c`, `include/tsdb_protocol.h`: HA manager (ranks, polling threads, handoff), per-volume admission, protocol v5 (mandatory HELLO with a session, the 32-byte insert header with attempt and seq, LIST_SERIES, DROPPED, TOPOLOGY, STATUS) with versions 1-4 removed, sd_notify, watchdog, metrics.
   - **Forwarding (§9.1):** authenticated peer links on their own port, sticky stream links, query links with credit, `OWNERS` with high-water marks and the ownership cache, `VOLUMES_GET`, `SESSION_FENCE`, `FORWARD` from the receive buffer with the age and client checks, the entry-node sequence rule with pinned epochs, per-volume reply order and windows, the attempt fence per stream, fan-out and PARTIAL merge, peer isolation. Proves: `test_failover` at server level, `test_stability`, `test_server`, the forwarding unit tests; `tsdbc-bench` direct and `--via` another node on a quiet machine, reporting the hop's p50/p99 latency, throughput and CPU (U22).
9. **Clients.** C, Rust, Python and Java: shuffled node lists, RST on abandon and periodic rebalancing, simple and direct mode, `u64` ids, per-stream attempts and a session-wide seq, resend slow start, LIST_SERIES, DROPPED and PARTIAL handling. Proves: `make test-clients` with a failover nemesis and an entry-node kill against a local 3-node setup.
10. **Integration.** `make jepsen-ha`; real NFS/RDMA runs (a-h); `insert-bench`/`tsdbc-bench` before and after; documentation (`docs/design.md`, AGENTS.md invariants and lock order).
11. **Phase 2.** Warm promotion and read-your-writes.

## Open questions for the user

1. **Lease time.** 10 s (the floor) gives about 11.5 s failover after node faults. 15 s tolerates more NFS server load before a false expiry but means about 17 s failover. Proposed: 10 s, margin 2 s.
2. **Automatic reboot.** A tsdbd stuck in D state keeps its volumes' locks until the node reboots. May tsdbd escalate to `reboot-force` on its own (proposed), or should that stay manual?
3. **Read routing.** Proposed: reads go to the owner (forwarded by any node), with lazy followers elsewhere only on fallback or by configuration. That saves NFS bandwidth for inserts but puts read CPU on the writers. Do you want dedicated read replicas by default?
4. **Client-only partitions** do not fail over (D14). Acceptable?
5. **The NFS server.** Is it dedicated? `fs.leases-enable=0` (no delegations) and a 10 s lease affect every client of that server.
6. **Upgrade.** `ha init` is one-way (META v3), and it needs every legacy writer node to have remounted or rebooted. Acceptable as an operational step?
7. **`u64` series ids** in `tsdb.h` and every client (§5): acceptable, given no release yet?
8. **Stale reads.** When the owner cannot be reached, a follower serves reads only to clients that set `STALE_OK` (§7). Proposed: off by default. Should some clients (dashboards) turn it on?

Answered on 2026-10-24: direct mode by default for every client, with forwarding as the fallback and simple mode as an explicit option (D39, §9 item 2).

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
| U22 | A forwarding hop adds tens of microseconds at p50 and well under 1 ms at p99, and forwarding single-volume frames costs only the send. | `tsdbc-bench` direct against `--via` another node, on a quiet machine (§14 step 8) |
| U23 | `SO_TIMESTAMPING` software receive timestamps on a TCP socket, read through io_uring `recvmsg`, give the arrival time of the bytes just read, so a frame's age includes its time in the socket buffer. | Unit test: stop the loop, write a frame, resume, check the reported age |

## 15. NFS server outages

This is §15 of this document. Subsections are numbered 15.0 to 15.10.

**Setting.** About 10 NFS servers export ZFS pools (raidz or draid, with a SLOG) over NFSv4.1/4.2 on RDMA. **Each server exports one directory per pool**; a server with several pools exports several. The servers have no failover: one can be down for hours. About 1000 volumes are spread over the servers and about 20 tsdb nodes, so every node has volumes on every server. Several volumes live as subdirectories of one export. A server can also fail in part: one of its pools suspended (`failmode=wait`) or full, or one export missing.

**Goal.**
- A request for a volume that cannot be served gets an answer within `admit_deadline` (2 s by default). The answer is either "definitely not applied" (UNAVAILABLE) or "outcome unknown" (IO).
- Volumes on healthy storage keep full speed. Nothing that happens on dead storage may kill, reboot or freeze a node.
- When the storage returns, its volumes come back without an operator. Before they do, tsdb checks that the storage still holds what tsdb acknowledged.

**What this section changes elsewhere in the document.**
- §1: a per-volume health state next to the role.
- §2.7 and D17: the stuck-I/O escalation needs a successful probe of the *same filesystem*.
- §5: creation in a volume on DOWN storage is UNAVAILABLE for that volume only.
- §8: new admission rules, watchdog exemptions, and no `FailureAction=reboot-force` for outages.
- §9: v5 without HA, D13 and HELLO amended, new bodies and statuses.
- §11: `grace-time` (D34) and the exports.
- §10, §13 and §14: new rows, tests and steps.

Section 15 can ship **before** the rest of HA, on today's engine (O0-O9).

### 15.0 Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| D20 | Keep **`hard`** mounts (§2.1). tsdb detects outages itself. | `soft`/`softerr` | A soft timeout leaves the outcome unknown. It also adds `ETIMEDOUT` to the errors whose dirty pages are dropped without redirtying (`fs/nfs/internal.h:911-929`, `write.c:972-976`). Hard mounts reduce errors but do not remove them: ENOSPC, EDQUOT, EIO and ESTALE from the server drop pages on hard mounts too. So the WAL taint (§15.3) and the SIGBUS guard are needed either way. |
| D21 | Health is tracked per **storage domain**. A domain is one NFS superblock, grouped by the `major:minor` field of `/proc/self/mountinfo`, which is recomputed at every mountinfo change and never compared with a stored value: NFS superblocks get anonymous device numbers in mount order. In the expected topology **one export = one ZFS pool = one domain**; the volumes in that export's subdirectories share its health. Domains are grouped by server address. Each volume also has its own health (STUCK, FULL). | Health per server only; per volume only | A server can suspend one pool and serve another. With per-server health the probe says UP while volumes hang, and §2.7 then escalates. With per-volume health only, 50 independent timeouts. Discovery by superblock stays general, so a server that also exports child datasets still gets one domain per filesystem. |
| D22 | Unavailable storage makes its volumes **UNAVAILABLE**. It never causes a kill, reboot or takeover, and threads stuck on it are exempt from every watchdog condition. | Escalation from D17 | No other node can reach the storage either. Killing nodes only loses their healthy volumes. |
| D23 | Refusals happen **at admission, before any engine work**. They return `TSDB_ERR_UNAVAILABLE` (definitely not applied). Reads are never served from page cache during an outage. | Trying and timing out; serving cached pages, marked stale | A thread that touches a dead hard mount is lost until the storage returns. Stale reads were ruled out by the user. |
| D24 | A **stuck request fences its volume.** While any request is parked on volume v, v is UNAVAILABLE(STUCK) for every session. A parked request that has not yet published is *abandoned*: it discards its work instead of landing late. A parked request never touches another volume. | Letting late writes land after newer acked ones; per-session blocking | Block numbers are assigned at publish (`parts_publish`). A late direct insert would therefore win over a write acked after it. |
| D25 | Multi-volume inserts write their healthy volumes and reply `TSDB_ERR_PARTIAL` with a per-row bitmap, also when the volumes have different owners (§9.1). The client resends only the refused volumes' rows. | Refusing the whole frame | A whole-frame refusal would stop a batch's healthy volumes for the whole outage. Clients resend by row and stream, so nothing newer is overwritten, and the drop watermark (D33) keeps resends from undoing `drop_before`. |
| D26 | **tsdbd never creates a database or a volume implicitly.** Before opening, it checks each path against the identity VOLUMES records for it, the same on every node: the longest-prefix mount must have the recorded fstype, and META must show the recorded uuid and number (§15.2). Volumes are added only by `tsdb-cli volume add` (§5). | Today's `eng_open` mkdir and META/ID creation | An empty export (dataset not mounted after a reboot) or a missing mount would otherwise get a fresh database. Writes to it vanish when the real one appears. |
| D27 | **Every recovery revalidates storage against memory** (WAL tail, MANIFEST, parts, SERIES length). On a mismatch the volume is DIVERGED: re-log, cold reopen, alert. | Resuming silently after a pure hang | `zpool import -m/-F` and SLOG loss roll back fdatasync-acked writes. |
| D28 | **No write is accepted for an unavailable volume:** no hinted handoff and no client spool. Apache Pulsar, in front of tsdb, is the buffer. The Pulsar sink (`connectors/pulsar`) retries a refused batch in place until the volume returns. | Server-side hints; a client spool with conditional replay | The user's architecture: Pulsar already keeps the data durably and in order per key, so an OK from tsdb keeps meaning "durable on the volume's storage". |
| D34 | **NFS grace: 10 s** (the user's choice, 2026-10-24), with nfsdcld, and **forced reconnect required**: when the health probe sees the server answer again, tsdbd makes the RDMA transport reconnect at once (U41), so nodes reclaim within seconds instead of at their next backoff (up to 30 s). If U41 fails in O0, the fallback is 45 s. `lease-time` stays 10. | 45 s without forced reconnect; 90 s; 30 s | Grace starts when nfsd starts, after the ~5 min reboot, and only lets clients reclaim; meanwhile every new open and lock of every client waits. A node that misses it loses its locks: with HA its volumes on that server fail over by cold open, without HA they take the re-log path. Either way no acked write is lost, because every commit is fenced. A shorter grace trades a shorter stall for more failovers (§15.1). 30 s equals the maximum reconnect backoff and leaves no time to connect and reclaim. |

### 15.1 Mount options, kernel facts and rules

**Mounts.**
- Clients: `vers=4.2,proto=rdma,port=20049,hard,timeo=100,retrans=2,lookupcache=positive`, no `local_lock`. On a hard mount, `timeo` and `retrans` only control "not responding" and the RDMA disconnect (`xprtrdma/transport.c:409-426`). After that, reconnects back off from 5 s to 30 s (`xprt_rdma.h:65-66`).
- Server (changes §11, D34): `grace-time=10` with nfsdcld and forced reconnect (U41), `lease-time=10` unchanged. Fallback without forced reconnect: `grace-time=45`.
  - **When grace runs.** It starts when nfsd starts on the rebooted server, so it adds to the ~5 min reboot only its own length. During grace, clients may only reclaim: every new OPEN and LOCK from every client of that server gets NFS4ERR_GRACE and waits, including new WAL segments, tmp parts and part opens on volumes whose owner reclaimed at once. I/O on reclaimed state (the open WAL segment, the LOCK fd) proceeds. Pre-created segments (§2.5) cover one rotation.
  - **Missing grace.** After the server returns, a client reconnects only when its RDMA backoff (5 s up to 30 s, `xprt_rdma.h:65-66`) next fires, so it may first try up to 30 s after nfsd started. A node that misses grace loses its state: with HA, its locks are LOST and its volumes on that server go FENCED and fail over by cold open to whichever candidate wins (§4), all at once; without HA, the lock-owning fds return EIO, which taints and takes the re-log path (§15.5). Because every commit is fenced (§3), neither path loses an acked write. The cost is takeovers (U10: 0.5-3 s per volume) and a burst of cold opens.
  - **Long grace** blocks new opens and locks for every client for its whole length. nfsd extends it only while someone is still reclaiming (`clients_still_reclaiming`, `nfsd/nfs4state.c:6277-6288`), and with nfsdcld it ends early once every client in its reclaim table has sent RECLAIM_COMPLETE (U40). A client that has not reconnected yet does not count as reclaiming; while clients keep reclaiming, grace is extended past `grace-time` about every second; the intended cap of 2 × lease (20 s) never fires in 6.12, because `clients_still_reclaiming` compares boottime seconds with the wall-clock `boot_time` (`nfs4state.c:6277-6293, 8598`; UNVERIFIED whether EL10 fixes it). Without nfsdcld every reclaim is refused (§11).
  - **Decision: 10 s with forced reconnect (D34).** Without forced reconnect, 45 s would be needed, because: A node's first reconnect lands anywhere in the 30 s after nfsd starts, and connecting and reclaiming its opens and locks takes time on top. 30 s would leave no margin: with 20 nodes, roughly every second restart would fence some node. With nfsdcld the usual case still ends in seconds, once every known client has reclaimed (U40); only a node that stays down keeps grace open to its full length. 10 s is safe, at the cost of many more failovers: a node misses it with a probability of about two thirds and loses every volume it owns on that server. With forced reconnect (U41) the probe thread reconnects every node within about a second of the server answering, so misses become rare even at 10 s; this is why D34 requires it, and why O0 must prove it before the grace is lowered in production.
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
- **Expected topology:** one export per ZFS pool, so one fs domain per pool, holding several volumes as subdirectories. About 10 servers with a few pools each give a few dozen domains, so probes cost a few dozen small RPCs per second per node. Fewer, larger volumes per pool become possible once merges split by series range; that is separate work.
- The mechanism stays general: an exported child dataset gets its own client superblock (U38), and so its own domain.
- **Mount identity is the same on every node.** VOLUMES records each volume's uuid, expected fstype and export source. At every mountinfo change a node takes the longest-prefix mount of each path, requires the recorded fstype (`nfs4` for NFS volumes), and confirms with one O_DIRECT read of the LOCK or META header, on the opener thread with a deadline, that the uuid and volume number match. A missing mount or another fstype (an unmounted export whose path falls through to the local root filesystem) is DOWN(NOT_MOUNTED); another uuid is DOWN(MISMATCH); neither is opened. `major:minor` only groups domains, because it changes across reboots and remounts. Preflight refuses autofs mountpoints: tsdb never stats a path, so it would never trigger one.

**Probes.** Each fs domain has one probe thread on each node, and it does nothing else.
- Every 1 s it reads 4 KiB with O_DIRECT from `<first volume of the domain>/PROBE`.
- Every 5 s it does a `pwrite` and `fdatasync` of 4 KiB to `PROBE.<node_id>`. The block also records the set of nodes this node has live peer links to, for peer isolation (D38, §9.1).
- The probe thread is allowed to hang. A **monitor thread** that does no I/O reads the start time of the probe in flight (`CLOCK_MONOTONIC`).
- If a probe has been stuck for 60 s, a second probe thread starts. At most 2 probe threads per domain exist.

**Signals per volume (from §15.3 admission).**
- **In-engine age:** the start time of the oldest request inside the engine.
- **Parked count:** the number of requests parked on the volume (§15.3).
- **EIO or ETIMEDOUT** from the engine: the domain is probed at once.
- **ENOSPC or EDQUOT**, from the engine or from a write probe: the volume (not the domain) becomes **FULL**. Writes are refused and reads keep working. With one export per pool, a full pool usually makes all its volumes FULL within one probe interval.

**Domain states.**

| State | Entered when | Effect |
|---|---|---|
| UP | (start) | Normal admission. |
| DEGRADED | Write-probe p99 over the last minute is above `max(500 ms, 10 × its 1 h baseline)` | Merges are throttled to 1 per domain on this node (not paused: inserts wait in `db_wait_merges` after publishing, `db.c:150-162`, `insert.c:503`). The per-volume cap drops to 1 only while in-engine age is growing. `admit_deadline` for the domain becomes `min(10 s, max(2 s, 4 × in-engine p99))`. |
| DOWN | A probe has been outstanding for `down_ms` (default 10 s), or 3 probes in a row failed with errors other than ENOSPC/EDQUOT, or the mount is gone | Every volume is UNAVAILABLE(NFS_DOWN or NOT_MOUNTED). No merges, flushes, follower refreshes or recoveries start. |
| RECOVERING | **Write and fdatasync probes** succeed for 10 s, each within 1 s, after the last probe thread stuck on the domain has returned | Volumes are recovered one at a time (§15.5). Read probes alone never leave DOWN: the server's ARC answers reads while writes to the pool are suspended. |

**Hysteresis.** Leaving DEGRADED needs 2 min under the threshold. A domain that went DOWN 3 times in 10 minutes needs 60 s of good probes to leave DOWN. A slow domain (a resilver or a scrub) therefore stays DEGRADED and serves. It does not flap through DOWN and RECOVERING unless one probe really takes `down_ms`.

**Effective health of a volume.**
- AVAILABLE, DEGRADED, or UNAVAILABLE with a reason. The reasons are NFS_DOWN, NOT_MOUNTED, STUCK, FULL (writes only), BUSY, ROOT_DOWN, LOCKED, RECOVERING, DIVERGED and MAINTENANCE, plus SUSPECT, PROMOTING and FENCED from §1, MISMATCH (§5), and NO_OWNER, UNREACHABLE and RELOADING from forwarding (§9.1).
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
- **Standbys.** `F_OFD_SETLK` calls to DOWN storage block on per-volume HA threads, which are allowed to hang. When the domain returns, a standby waits `--nfs-grace-s` (10 s, D34) plus 5 s before it polls, so a reclaiming owner keeps its volume. It stops waiting early once it sees the owner's heartbeat advance, or once a new open on the domain succeeds, which means grace is over. Polling earlier gains nothing: during grace a lock request only blocks in NFS4ERR_GRACE retries (U17).

### 15.3 Isolation

The row numbers refer to the stall map (rows 1-16).

**Admission in tsdbd (rows 1, 2, 14, 15). This is D19, made concrete.**
- **The loop routes by header only.**
  - An insert carries `vol` (§9), and every other request a series id or a path that names its volume (§5). They are routed with an in-memory `route()` that never does I/O, to a local volume or to a peer link (§9.1).
  - Multi-volume frames (`vol = 0xffff`) go to a small **decode pool**: zstd and unshaping (`tsdbd.c:360-392`). The pool routes them and hands them to per-volume admission. Decompression never runs on the loop.
  - Decode-pool threads do no engine I/O.
- **Refusals.**
  - Refusals for an UNAVAILABLE volume are sent from the loop or from the decode pool, by the owner or by an entry node from its ownership cache (§9.1).
  - Clients honour `retry_after_ms` (§15.4). There is no tarpit: there are no old clients to slow down (D32).
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
- COMMIT, FLUSH, OPTIMIZE, drop_before, STATS, `foreach_part` and an all-volume LIST_SERIES are split into **per-volume subtasks**. Queries, QUERY_GROUP included, name one series; LAST_STATS goes to the node that ran the last query (§9.1). Each subtask goes through that volume's admission, cap and parking.
- The parent waits for at most `admit_deadline`. It reports volumes that were refused or did not finish (PARTIAL for v5, §15.4).
- **COMMIT and FLUSH** cover only the volumes the connection has written since its last COMMIT, tracked per connection. A client that never wrote to a down volume gets OK.
- **drop_before.**
  - `drop_lock` (`volumes.c:742-750`) becomes per volume.
  - A drop for an unavailable volume is not deferred: PARTIAL with UNAVAILABLE for it, and the job retries (§9). The highest watermark any node has seen is re-applied at claim and at recovery (D36).
- **Root I/O.** No request does root I/O (§5). Only `tsdb-cli volume add` and the volumes-follower thread touch the root, and the follower registers "I/O on domain d" for the root's domain.

**Global locks held across I/O (rows 5-8). Rule: never hold a lock that is not the volume's own across I/O to a volume.**
- **Row 5.** Creation touches only the volume its path names (§5). The SERIES append runs under that volume's dictionary lock; there is no root lock and no root I/O on the request path.
- **Row 6: the volumes follower.**
  - No I/O ever runs under `t->lock` or on the loop. Today follower `catch_up` takes the wrlock and calls `eng_refresh(root)` and `load_layout` (`volumes.c:377-385`). Both `route()` and `tsdb_series_lookup()` call it on any miss (`388-418`).
  - Instead, each node runs one **volumes-follower thread**. It reloads VOLUMES in the background when a `VOLUMES_GEN` push arrives (§9.1) and every 60 s, with a deadline, coalesced. It builds new arrays outside the lock and publishes them under a short wrlock. CATALOG is not read (§5).
  - A series miss inside a known volume is answered from that volume's dictionary by its owner and never touches the root. A volume number or path missing from the cache never holds a worker: it gets NOTFOUND at once when this node's VOLUMES generation is the highest it has seen and no push is pending, and otherwise UNAVAILABLE(RELOADING, or ROOT_DOWN if the root domain is not UP) while the follower thread fetches the missing lines from the root or, with `VOLUMES_GET`, from a peer (§9.1).
  - `volume_open` runs outside the lock as well. This applies to every role (§5).
- **Row 7.** STATS and `foreach_part` copy the `eng` pointers and release the rdlock (`volumes.c:1035-1061`). The `renameat` in `db_retire_locked` and the `unlinkat` in `graveyard_purge` move out from under `db->lock`.
- **Row 8.** Series names are kept in `tsdb` memory. `names_find` never takes a volume's `d->lock`.

**Series creation when a volume stalls (row 5, §5).**
- A creation is a request on its volume like any other: admission, cap and parking (D24). Creation on other volumes is unaffected.
- The SERIES append is its only write. A parked creation whose append lands late just leaves the series created; the client's retry finds it by name and gets the same id. Today's abort problem, a CATALOG append after a late SERIES append (`volumes.c:477-487`), no longer exists.

**Root volume and `volumes.cache` (row 9).**
- Each node keeps `/var/lib/tsdb/<uuid>/volumes.cache`: the VOLUMES lines with each volume's number, uuid, fstype, export source and aliases, the prefix length and xxh64, the database id (`ID_FILE`), `ROOT_ID`, and the per-volume high-water marks (D36), fsynced every 10 s.
- Routing and creation work from the cache while the root is down (§5).
- Only volume numbers and paths beyond the cached prefix get UNAVAILABLE(ROOT_DOWN), never NOTFOUND.
- The cache is used only for the database id it names.

**mmap faults (row 10).**
- Queries are refused at admission. A query already running checks the volume state (atomic load) before each part and each granule run.
- **No reads from page cache during an outage** (D23). A volume that is UNAVAILABLE answers no query, even for parts whose pages happen to be cached.
- **SIGBUS guard (version 1, all mounts).**
  - Decoding of mapped parts runs inside a per-thread `sigsetjmp` region that holds no locks and does no allocation. The part pin and the output buffers are released by the region's exit path.
  - The handler checks that `si_addr` lies in a registered part mapping and that the thread is inside a region, then `siglongjmp`s. Otherwise it restores the default action and re-raises.
  - The volume becomes UNAVAILABLE(IO_ERROR), its domain is probed, and the volume is tainted (recovery revalidates, §15.5).
- A fault in progress on a hard mount parks its worker.

**Startup (row 11, D26).**
- **No implicit creation.**
  - `tsdbd` opens the root and volumes without creating anything: no `mkdir` (`db.c:394`), no META (`db.c:236-245`), no `ID_FILE` (`volumes.c:313-320`).
  - `tsdbd --init` and `tsdb-cli init` create a database. `--db-id` (or the id in `volumes.cache`) must match `ID_FILE`.
  - Each path's mount identity is checked before any open (§15.2): the fstype through `mountinfo`, then uuid and number from META. A missing mount is DOWN(NOT_MOUNTED), a mismatch DOWN(MISMATCH).
  - The library keeps `create` for embedded use, but `tsdb_open` takes a new `TSDB_OPEN_NOCREATE` that tsdbd always passes.
- **Opening.**
  - `load_layout` opens volumes in parallel on opener threads, with a 10 s deadline per domain. A volume that fails or does not answer keeps its slot and routes to UNAVAILABLE.
  - Every unopened volume is retried in the background: when its domain recovers, when `mountinfo` changes (poll it every 2 s), and with backoff for any other error. Today a volume that fails at once is never retried.
  - `tsdb_open` succeeds if the root or a `volumes.cache` with the matching id is readable.
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

**`TSDB_ERR_UNAVAILABLE = -11`** is sent only before engine work, by the owner or by an entry node (§9.1).
- Body: `u16 n, n × {u16 vol, u8 reason, u8 flags, u32 epoch, i64 since_ms, u32 retry_after_ms, u64 outage_id}`, then `[u64 resend_from]` when the frame carried a seq.
- `retry_after_ms = clamp(outage_age/10, 1 s, 60 s)` with ±20% jitter. For BUSY it comes from the drain rate; for MAINTENANCE it is the operator's `--until`.
- `outage_id` increments each time the volume leaves AVAILABLE (for logs and metrics).
- Streaming ops send `{last=1, n=0}` and then the body.

**`TSDB_ERR_PARTIAL = -12`.**
- Body: `u16 nv, nv × {u16 vol, i16 status}`, `u32 nrows, u8 not_applied[(nrows+7)/8]`, then the UNAVAILABLE entries.
- A row's bit is set when its volume was refused (definitely not applied), abandoned or indeterminate, or when the row was below the volume's drop watermark (status DROPPED: the volume's other rows were applied, the marked ones never will be, D33). The per-volume status says which.
- An entry node builds it from the owners' replies for a frame that spans owners (§9.1).
- COMMIT, FLUSH and drop_before reply PARTIAL with per-volume statuses. An all-volume LIST_SERIES returns the available volumes' series and ends with PARTIAL listing the missing volumes.

**Without HA.** The epoch is 0. Sessions, attempts and seqs work as in §9: clients always send them.

**TOPOLOGY** volume entries gain `u8 health, u8 reason, i64 since_ms, u16 domain, u16 server`, as the owner published them (§9.1). The request gains `u64 since_gen` and the reply `u64 gen`. There is no server push; clients poll.

**STATUS** gains `domain.<d>.{state,since,probe_ms,parked,refused}`, `vol.<n>.{health,reason}` and refusals per connection.

**The four client libraries.**
- **C:** `tsdbc_unavailable(c, &info)`, per-frame statuses from `drain` (`tsdb_client.c:228-241` keeps only the first), a pipeline cap, frames copied out of the zstd buffer, and a read timeout.
- **Rust:** `Status::Unavailable(Vec<VolInfo>)` and `Status::Partial`. `wait()` returns every failed frame.
- **Python:** `TsdbUnavailable(TsdbError)` with `.volumes` and `.retry_after`.
- **Java:** `UnavailableException` and `PartialException`, plus a read timeout.
- **All four** split MANY_SERIES per volume in direct mode (the id carries the volume, §5), keep refused and unacked frames per stream in seq order, drop DROPPED rows, and back off with full jitter capped by `retry_after_ms`.

**Pulsar as the buffer (D28).** Writers go through Apache Pulsar, and the Pulsar sink is the tsdb client that feeds a volume. It is configured with a node list, routes directly to owners (direct mode, §9) and falls back to forwarding through any node:
- It acknowledges a Pulsar message only after tsdb replied OK for the batch holding it.
- On UNAVAILABLE, BUSY or an indeterminate IO it retries the same batch in place, in order, after `retry_after_ms` with jitter. It never negatively acknowledges, because that would reorder a series.
- On DROPPED, or rows marked DROPPED in a PARTIAL, it acknowledges those messages and counts them: the data was deliberately dropped (D33).
- A late landing of an indeterminate attempt is harmless: D24 makes it land before anything admitted later to that volume, and the retry writes the same values.
- Producers key messages by series, and topics are best partitioned by volume (the series' path), so one unavailable volume holds back only its own messages.

**What a reply means.**

| Reply | Meaning |
|---|---|
| OK | Durable on the volume's storage, as today, whether the request went directly or through an entry node. |
| UNAVAILABLE, or a PARTIAL row marked refused | Definitely not applied. |
| PARTIAL row marked abandoned | Not applied; the work was discarded. |
| DROPPED, or a PARTIAL row marked DROPPED | Not applied, and never will be: below the drop watermark (D33). Do not resend. |
| IO, or a timeout | Indeterminate. If it lands, it lands before anything admitted later to that volume (D24) and before the stream's next attempt (D31). |

### 15.5 Recovery when the storage returns

1. **The domain goes RECOVERING** on write probes (§15.2). Stuck threads return on their own as the hard mount retries.
2. **Wait for references.** A volume is recovered only after every thread holding a reference on its `eng` has returned: parked workers, the opener, the flusher, mergers, the follower and the HA thread. Late writes therefore land, or are abandoned, before anything new is admitted. One recovery attempt per volume runs at a time; a new attempt starts only after the previous thread has returned. At most 4 volumes per server group per node recover at once, with 0-5 s of jitter.
3. **Revalidate (D27).** This runs for every volume, tainted or not.
   - **WAL:** an O_DIRECT read of the last durable record of the current segment. Its offset and xxh64 must match what the engine recorded at `wal_durable`, and the server size must be at least that offset.
   - **MANIFEST:** an O_DIRECT read must match the last published checksum and generation.
   - **Parts:** every live part exists with its expected size (`fstatat` on a fresh open).
   - **SERIES and STATES:** the server size is at least the length of every id the dictionary has handed out.
   - **Watermark and ids:** apply the higher of the ledger's watermark and the highest any node has seen, and compare the SERIES and STATES lengths with the high-water marks (D36). Storage behind them is DIVERGED.
   - If everything matches and the volume is not tainted: resume on new fds (`wal_fd` is reopened, never reused).
4. **Re-log** (on taint, or on DIVERGED after a mismatch).
   1. If SERIES or STATES lost entries, re-append first the names that memory still holds, in id order, so those ids keep their names. Memory is often gone (tsdbd restarted during the outage, the node rebooted, or another node cold-opens the volume), so any gap left up to the highest length any node has seen (D36) is filled with LOST entries: those ids are never handed out again, and frames for them get STALE_ID. Ids live only in their volume (D9), so a rollback that is not repaired here would give an old id a new name, and a client holding the cached id would write into the wrong series.
   2. Write the memtable rows with `lsn ≤ wal_durable` (`mt_collect` with `visible`) into a **new** segment, then fdatasync it and the directory. Rows past `wal_durable` were never acked and are dropped.
   3. Cold-reopen the volume from the directory (the intent files and `wal_seq` rules apply). A rollback that reverted a merge brings the source parts back under their live names, so the directory, not memory, is the truth.
   4. Parts that memory listed but the storage lost are reported (`tsdb_lost_parts{vol}`, block ranges, log at error level). Their rows are gone.
   5. In HA this is an edit under the ledger (§2.5). The volume stays DIVERGED until an operator acknowledges it, if any part was lost.
5. **Root.** It holds only ID, VOLUMES and NODES, which grow only by admin appends; series ids do not depend on it (D9).
   - If VOLUMES is shorter than `volumes.cache` and its prefix hash matches, `tsdb-cli volumes repair` re-appends the cached lines, but only after each volume's META confirms its number and the root id (§5).
   - A VOLUMES whose prefix differs from the cache is ROOT_DIVERGED. Routing, creation and inserts continue from the cache; `volume add` is refused until `volumes repair` runs. `volume add` also asks every live node for its generation and prefix hash, so running it from a host without a cache cannot hand out a rolled-back number again (§5), and peers compare each volume's uuid through OWNERS (§9.1).
6. **NFSv4 state.**
   - After a server reboot, opens and locks are reclaimed during grace (`nfs4state.c:1638, 1881-1883`; U32). The 10 s grace relies on forced reconnect (U41) to bring every node back within it; a node that still misses it takes the paths below, without losing an acked write (D34).
   - A server that stayed up keeps an uncontested expired client as COURTESY.
   - Lost state (`NFS4ERR_EXPIRED`, `recover_lost_locks=0`) returns EIO on lock-owning fds (`nfs4state.c:998-999`), which leads to taint and the re-log path.
7. **HA writer lease.**
   - A lock that survived: the next heartbeat moves SUSPECT to OWNER, same epoch.
   - A LOST lock leads to FENCED and abandon (§2.6), then the §4 takeover by a cold open, at most 4 per server group per node at once. Standbys hold back for grace + 5 s, or less (§15.2), so an old owner that reconnects within grace reclaims and keeps its volumes. A loss caused by the server restart is not a node fault: no back-off, and rebalancing may move the volume back within minutes (§6).
   - Entry nodes learn the new owner from its `OWNERS` push, or from MOVED (§9.1).
8. **After recovery.**
   - Merges run at full rate. Admission is throttled by the part count (`PARTS_TO_DELAY_INSERT`, `db.c:150-162`): when a partition nears the limit, the volume's queue stops draining, fills and replies BUSY. Nothing holds a worker in `db_wait_merges` beyond the cap.
   - Followers refresh in random order. Clients see AVAILABLE in TOPOLOGY. Pulsar sinks resume as their `retry_after_ms` expire, and the part-count throttle (BUSY) paces the backlog they deliver.
9. **Storage that comes back elsewhere.** A pool imported on another head, or an export served from another address, leaves the old threads stuck for good. The new export must be mounted at the old path on every node. `tsdb-cli volume abandon <vol>` and `tsdb-cli domain abandon <d>` handle this:
   - mark the old `eng` and its threads abandoned (they leak, and their wakeups go to tombstones);
   - `tsdb-cli volume rehome <vol> <server:/export>` records the new export source in VOLUMES and pushes it (`VOLUMES_GEN`); the path, and with it every series' address, never changes (D9);
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
- `volume add [--alias]`, `volume rehome`, `volume migrate-root`, `volume lower-watermark`, `volumes repair`, and `volume ack-divergence`.

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
- `rollback` of a volume's SERIES tail, with the owner restarted during the outage and the volume cold-opened on another node: the gap becomes LOST entries before writes are admitted, a cached old id gets STALE_ID, and no id gets a new name.
- `rollback` of the root VOLUMES: ROOT_DIVERGED; routing, creation and inserts continue, and `volume add` is refused until repair, also when run from a host without a cache.
- Mount identity: a remount and a reboot that change every NFS `major:minor` leave the volumes AVAILABLE; an unmounted export whose path falls through to the root filesystem is NOT_MOUNTED; another uuid behind the path is MISMATCH.
- A creation parked on a hung volume whose SERIES append lands after heal: the series exists once, and the client's retry gets the same id.
- Root prefix missing at start: no ID or META is created, ROOT_DOWN, and recovery after "mount".
- SIGTERM with A hung, then restart: B's volumes open (flock released) and the port is served.
- A SIGBUS in a query on A (estale): the process survives, and A is UNAVAILABLE.
- A multi-volume frame with A down: B's rows are applied, the PARTIAL bitmap marks exactly A's rows, and COMMIT on a connection that only wrote B returns OK.
- The same frame sent to an entry node that owns neither A nor B: the same PARTIAL, and A's refusal comes from the entry node without a hop.
- A Pulsar retry after a `drop_before` that covered part of the batch: DROPPED rows are acknowledged, not resent, and no dropped partition reappears.
- A Pulsar sink against a volume that goes down for minutes: no message is acknowledged early, none is lost, per-series order holds after recovery (connectors/pulsar integration tests).

**`make jepsen`: an `outage` nemesis** (hang, eio, slow or rollback of one prefix for 2-600 s, mixed with kill -9 and power loss). Checks: no acked write lost (outside declared rollbacks), no refused write visible, healthy throughput ≥ 80%, recovery within 30 s of heal plus grace.

**jepsen-ha:** a 10-minute hang of one domain, with zero SIGKILLs, zero reboots and zero takeovers. Server reboot with the clients' RDMA reconnect at maximum backoff, with grace 45 s, 30 s and 10 s: count the failovers in each run (none expected at 45 s); in every run no acked write is lost and takeovers stay within the per-server-group cap.

**Manual (`tests/ha/nfs-outage.sh`), 1 h and 3 h runs.** Ways to take storage away:
- (a) nft drop, TCP test mount only;
- (b) link down on the NFS server (RDMA);
- (c) nfsd threads set to 0;
- (d) server reboot, with `grace-time` 45, 30 and 10, recording reclaims, failovers and how early grace ended;
- (e) `zpool export` of one pool while another stays up;
- (f) a dataset quota filled to ENOSPC;
- (g) a dataset unmounted under its export, to check `mp`.

Each run records: time to UNAVAILABLE, healthy-volume bench numbers, `df`/`sync` hangs, bdi dirty bytes, wchan of every tsdbd thread (looking for `nfs_commit_inode` in reclaim), time to recovery, reclaim, and parked threads.

### 15.9 Implementation plan

Each step lands with `make test` and `make asan`. Steps that touch the disk path also need `make jepsen`.
- **O0.** Measure on AlmaLinux 10 over RDMA: the timeouts, the dirty coupling and bdi sums, reclaim stalls under memory pressure, an mmap GETATTR, nfsd grace and reconnect (and whether a reconnect can be forced, U41), ZFS EIO over NFS, and the `nfs_commit_bytes` cost (U30-U39).
- **O1.** crashfs control socket and uffd. `test_outage.c`, failing at first.
- **O2.** `src/health.c`: domains from `mountinfo`, probes, monitor, states, STUCK and FULL; the new statuses in `tsdb.h`.
- **O3.** WAL taint in `wal_commit`; the abandoned flag in `parts_publish` and `mem_insert`; the SIGBUS guard; R3 periodic `fdatasync`.
- **O4.** No implicit creation, mount identity checked by fstype and META uuid, `volumes.cache` with high-water marks; the volumes-follower thread; no I/O under global locks (rows 5-9). This needs §14 step 7 (series addressing): with ids per volume there is no CATALOG to cache and no creation abort generation.
- **O5.** Parallel open, background reopen, LOCKED handling; SIGTERM full close, the crash handler, socket activation.
- **O6.** tsdbd admission: header routing, decode pool, per-volume queues and caps, parking with tombstones, per-volume subtasks, per-connection COMMIT sets. Both io_uring and epoll.
- **O7.** Recovery: reference waits, revalidation, re-log, DIVERGED, root repair, abandon. Part-count throttling.
- **O8.** Protocol v5 bodies (UNAVAILABLE, PARTIAL, DROPPED), TOPOLOGY, STATUS, `outage_id`; the four clients; `make test-clients`. Forwarding itself is §14 step 8.
- **O9.** jepsen `outage` nemesis; manual runs (a)-(g).
- **O10.** HA: watchdog exemptions, the §2.7 same-filesystem rule, `--nfs-grace-s` and the standby hold-back with its early end, the takeover cap per server group, no back-off for losses from a server restart, removal of `reboot-force`, the escalation helper reading `/run/tsdbd/health`. This goes into §14 steps 5 and 8.

### 15.10 Open questions for the user

1. Hard mounts with tsdb's own detection, as proposed? Is any node mounted `soft`?
2. Answered: Pulsar buffers writes during an outage (D28). Still open: the ingest rate per volume, which sizes the backlog to drain after a 3 h outage.
3. Planned ZFS maintenance: how often? (`domain maint` drains.)
4. Answered on 2026-10-24: the user believes the ZFS servers do not run nfsdcld today. It is required (§11): without it nfsd refuses every reclaim. [nfs-server.md](nfs-server.md) §2.4 explains how to check (EL10's `nfs-server.service` may already pull it in) and how to enable it. Still open: the check's result on each server, and the maintenance window for the restart that enabling it needs.
5. Monitoring agents on the tsdb nodes: which ones call statfs or `df`?
6. Should placement spread tenants across servers?
7. Is there a spare head where pools can be imported? If so, `volume abandon` is the procedure.

Answered on 2026-10-24: one export per ZFS pool (D21); per-volume ids with path addressing (D9); grace (D34); never serve reads from page cache (D23); no old clients (D32); a drop watermark (D33); built-in forwarding (D29).

**New unverified assumptions.**
- **U30.** The time to "not responding" and the RDMA reconnect time with `timeo=100,retrans=2`.
- **U31.** A process with a thread in D state keeps its fd table. After the changes above, only the fds of DOWN domains stay pinned.
- **U32.** The ZFS servers keep client records across a reboot. This holds only with nfsdcld running and its database on persistent local storage (§11, [nfs-server.md](nfs-server.md) §2.4).
- **U33.** Per-bdi `strict_limit` and `max_bytes`, with their sum below the global limit, stop a dead domain from throttling healthy mounts.
- **U34.** nfsd `mp` refuses to export an unmounted dataset directory on these servers.
- **U35.** ZFS returns NFS4ERR_IO for blocks with permanent errors, rather than hanging.
- **U36.** With no memcg limit and raised watermarks, tsdbd threads rarely enter direct reclaim. With `nfs_commit_bytes`, uncommitted folios are few enough that the risk is acceptable.
- **U37.** On healthy storage, in-flight write RPCs from a crashing process finish within `RestartSec`.
- **U38.** Each exported ZFS dataset gets its own client superblock (distinct `major:minor`), including child datasets under `crossmnt`. Needed only if a pool exports child datasets; the expected topology has one filesystem per export.
- **U39.** An `fstat` immediately before `mmap` keeps `nfs_file_mmap` from sending a GETATTR.
- **U40.** With nfsdcld on these servers, grace ends early once every client in the reclaim table has sent RECLAIM_COMPLETE (`clients_still_reclaiming`, `nfsd/nfs4state.c:6277-6288`), and a client that reboots during the outage does not hold grace open. Checked in manual run (d).
- **U41.** Once the server answers on port 20049 again, writing to `/sys/kernel/sunrpc/xprt-switches/*/xprt-*/xprt_state` (or a probe over a fresh transport) makes the RDMA transport reconnect at once instead of at its next backoff. If so, the probe thread triggers it, and nodes reclaim within seconds of nfsd starting. Checked in O0 and manual run (d).
