# tsdb HA modes: shared NFS storage and the shared-nothing cluster

Status: design, not implemented. Branch `ha-modes`, written on 2026-11-06 from main at ea89968, with the metadata
group of cluster step 1 (branch `cluster-s1`, 35e1456) in view. This document is the overview of tsdb's high
availability. The two modes have their own designs: [ha.md](ha.md) (shared NFS storage) and
[design-cluster.md](design-cluster.md) (servers with their own disks). This one says how they relate, what they share,
which control plane runs both, and how a deposed writer is kept off shared storage.

Decided by the user on 2026-11-06: "I have been thinking, I still want to keep the NFS implementation." This reverses
design-cluster.md D3 ("the cluster replaces ha.md for new work"): the NFS failover of ha.md stays, **as a second HA
mode beside the shared-nothing cluster**. Production today is AlmaLinux 10 nodes without local data disks, NFSv4 over
RDMA to several ZFS servers, with no failover between the NFS servers (an outage can last hours; Pulsar buffers the
writes meanwhile). Everything else in design-cluster.md (D1, D2, D4-D6, C1-C25) stands for the cluster mode.

## Summary

- **Two modes, one product.** *Shared mode* (ha.md): one copy of each volume on NFS/ZFS, one writer per volume, and
  failover moves the writer, not the data. *Replicated mode* (design-cluster.md): three copies on servers' own disks,
  a leader per volume, and failover promotes a replica. A deployment chooses per volume; most will use one.
- **One control plane for both** (M2): the Raft metadata group of cluster step 1 (`server/meta.c`) decides who owns
  each volume and at which epoch, in both modes. In shared mode it replaces ha.md's arbiter role of the NFS lock, its
  rank polling, `WANT` files, slot heartbeats as a liveness signal, `NODES`, the `OWNERS` gossip of owners and epochs,
  and D38's isolation heuristic.
- **The fence stays on the storage** (M3). The metadata group cannot stop a deposed writer's `pwrite` on NFS; only the
  NFS server can. So shared mode keeps ha.md's NFSv4 lock on `LOCK`, chained fds, the ledger, epoch-named files and
  the abort rule (§6). The lock no longer decides who should own a volume; it only makes sure that whoever does not is
  refused by the server. Safety rests on the lock and the epochs on storage, never on the metadata group or a clock.
- **Shared by both**: the client protocol (MOVED, UNAVAILABLE, TOPOLOGY, streams with `seq` and `attempt`, one hop of
  forwarding, the drop watermark, ha.md §9's client rules), volume ownership by epoch, epoch-numbered segments
  (`(E << 40) | n`), peer links with the cluster secret, users and name slots in the metadata group, per-volume
  admission, the `tsdb cluster` CLI and the metrics.
- **Different**: durability (ZFS behind NFS against in-sync replicas), failover (a lock takeover of the same files
  against the promotion of a replica), rebuild (none against a replica rebuild), and what an NFS server outage does
  (its volumes wait, for hours if need be, against a cluster that loses nothing with two servers down).
- **Order**: shared mode ships first, because production runs on NFS (M9). The single-server NFS outage handling of
  ha.md §15 ships before any failover. The cluster's replication core (step 2) goes on in parallel; it does not block
  shared mode and shared mode does not need it.

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| M1 | **Two HA modes, per volume**: *shared* (ha.md) and *replicated* (design-cluster.md). The kind is a field of the volume in the metadata and a `requires=` feature in its META (`nfs-ha` or `cluster`). | The cluster only (design-cluster.md D3, reversed by the user); NFS only | Production has no local data disks and runs on NFS; servers with their own disks are where new deployments go. Each mode is the cheaper answer for its hardware (design-cluster.md A1). |
| M2 | **One control plane: the metadata group** (Raft, `server/meta.c`) for both modes. In shared mode it names each volume's owner and epoch, schedules takeovers, and holds members, volumes, users and name slots. | (b) ha.md's own control plane (the NFS lock as arbiter, rank polling, slots, `NODES`, `OWNERS`) for shared mode; (c) both, (b) for NFS-only deployments | §5.2: the metadata group sees the whole cluster, so it tells a node fault from a storage fault and an isolated owner from a dead control plane, which ha.md answers with heuristics (hold-back timers, D38). Keeping (b) beside it doubles the code and the jepsen matrix that must stay green forever. |
| M3 | **The fence is the storage's, not the control plane's.** Shared mode keeps ha.md §2-§3: an NFSv4 OFD lock on `LOCK`, chained fds on every file an acknowledgement relies on, the ledger as the only commit log for namespace effects, epoch-named files, the abort rule, grant and poison, the ack gate. | Epoch checks before each publish; epochs in names alone; Raft-conditional acknowledgements; a time lease from the metadata group; the ledger in Raft (§6.3) | A deposed writer can still write files on NFS. The only thing that refuses its writes is the server's lock state (ha.md L1, L3). Namespace operations carry no lock, so they are made harmless by names and fenced records instead. |
| M4 | **A claim needs both** a metadata grant `(volume, owner = this node, epoch E)` and the NFS lock, and writes E only if every epoch on storage is below E. | Either alone | The grant makes one claimant at a time the rule and the epoch monotone off the pool; the lock and the check make two claimants with stale views harmless (§6.4, G2). |
| M5 | **The lock holder is the owner until it lets go.** A deposed owner learns of a higher epoch from the metadata watch, a peer, a MOVED, or the `CLAIM` file a refused claimant writes on the volume's own storage, and then releases cleanly. It never releases only because it lost the metadata group. | Releasing on the loss of a metadata lease (D38's heuristic); no back channel | If the metadata group is down, releasing every volume would turn a control-plane fault into a full outage. An owner cut off from everything but NFS still reads its own storage, so the storage carries the news. |
| M6 | **Storage faults are told apart by the metadata group.** A volume moves when its owner reports its storage domain DOWN while another candidate reports the same domain UP for `local_fault_s` (10 s). When every node reports it DOWN, nothing moves (ha.md D22). | Moving on any fault; never moving | A node-local fault (an RDMA port, a mount) is a reason to move; a server outage is not, because no other node can reach the storage either. ha.md could only let the NFS lease expire to find out. |
| M7 | **Mixing is allowed per volume**, built and gated homogeneous first; one mixed jepsen run before it is supported. | Per cluster only; mixed from day one | Clients cannot tell the kinds apart. Mixing is the path from NFS to local disks: new series go to replicated volumes while NFS volumes age out (a series cannot move between volumes). |
| M8 | **Shared mode's metadata additions are small**: a volume kind, a storage record (path, export, fstype), candidates in the replica list with the ISR equal to all candidates, high-water marks, and domain health in heartbeats (§5.4). The epoch-conditional operations of `meta.c` are reused unchanged. | A separate state machine for shared volumes | Every candidate of a shared volume sees the same storage, so "in sync" holds for all of them by construction; `META_OP_VOLUME_LEADER`'s rule (the new leader is in the ISR) then holds as it is. |
| M9 | **Shared mode first**: ha.md §15 (outage handling, single server) now; then the metadata group, the shared protocol and the shared-mode engine; replicated mode after it, its sans-IO core in parallel. | The cluster first, as design-cluster.md §14 planned | Production runs on NFS today. Shared mode needs no new hardware, and its outage handling is useful without any failover. |
| M10 | **The ledger stays on the storage it describes.** | The ledger as Raft entries | A pool rollback (`zpool import -F`, a lost SLOG) reverts files and ledger together, which ha.md D27/D36 then detect. A ledger in Raft would name parts the storage no longer has, and put every commit, flush and merge of 1000 volumes through Raft. |

## 1. The modes and when to use which

| | **Shared mode** ([ha.md](ha.md)) | **Replicated mode** ([design-cluster.md](design-cluster.md)) |
|---|---|---|
| Hardware | Nodes without data disks; volumes on NFSv4.1/4.2 (RDMA) exports of ZFS pools | Servers with their own NVMe; nothing shared |
| Copies of the data | One, protected by the pool (raidz/draid, a mirrored SLOG) | Three (replication 3), on three servers |
| Who writes a volume | Its owner: the node holding the volume's NFS lock at the current epoch | Its leader, which replicates to the in-sync set |
| What survives | Any node fault, any number of nodes as long as one candidate is alive; not an NFS server outage (its volumes wait) | Two servers lost with their disks (min ISR 1) |
| Use it when | The storage is shared and already redundant; production today | New deployments on servers with disks; no shared storage |

A deployment with both kinds of hardware may run both kinds of volume under one metadata group (§4).

## 2. What the modes share

| Component | Where it is designed | Notes for shared mode |
|---|---|---|
| The metadata group (Raft, `server/meta.c`, watch stream, heartbeats) | design-cluster.md §2 | M2, §5. Members, volumes, owners, epochs, users, name slots. |
| Volume ownership by epoch; `(E << 40) \| n` block and segment numbers | ha.md D4, design-cluster.md §4, §6.2 | Epochs come from the metadata group in both modes. |
| Client protocol: HELLO, TOPOLOGY, direct mode by default, streams `(session, vol)` with `seq` and `attempt`, MOVED and UNAVAILABLE with their bodies, resends by time budget | ha.md §9, D13, D31, D39; design-cluster.md C17, §11.2 | Unchanged. TOPOLOGY comes from the metadata copy instead of `OWNERS`. |
| Forwarding: one hop, the entry node never retries, sticky stream links | ha.md §9.1, D29, D30, D37 | Unchanged; the ownership cache is the metadata copy (§5.3). |
| Drop watermark | ha.md D33 | In shared mode a ledger record; in replicated mode a `DROP` marker. |
| Peer links with the cluster secret | ha.md D35, design-cluster.md C22, C25 | Shared mode needs only the log-sized links (no bulk transfers). |
| Users and grants in the metadata group | design-cluster.md C22 | Replaces the root `USERS` file in both modes. |
| Name slots: names unique in the whole database | design-cluster.md C18, D4 | Replaces ha.md D9's per-volume names (§3 below). |
| Per-volume admission, reply order and back-pressure | ha.md D19, D37, §15.3 | Unchanged. |
| Health per volume: UNAVAILABLE with a reason, `retry_after` | ha.md §15.2, §15.4 | Storage domains are shared mode's; the reasons and bodies are common. |
| `tsdb cluster` CLI: init, node add/drain/out, volume add/move, status | design-cluster.md §12 | `volume add --shared PATH` or `--replicas N`; `tsdb cluster adopt` takes an existing NFS database in (§5.4). |
| Metrics | docs/metrics.md | One family per concept with a `mode` label: role, epoch, failovers, takeover time, UNAVAILABLE by reason. |

## 3. What differs

| | Shared mode | Replicated mode |
|---|---|---|
| Durability of an acknowledged write | `write` + `fdatasync` on the owner's WAL segment over NFS: stable on the ZFS pool (sync export, SLOG) | Durable on every in-sync replica (C11) |
| Insert path | Unchanged from a single server; no copy, no extra round trip | The leader's WAL bytes sent to the ISR (+0.15 ms, design-cluster.md U1) |
| Failover | The metadata group grants the volume to a candidate; it takes the NFS lock and cold-opens the same files (ha.md §4) | The metadata group fences the ISR and promotes the longest log (C12, §6.2) |
| Failover time | Node dead: about 11.5 s (the NFS lease of 10 s, then the lock) + the cold open (0.5-3 s, ha.md U10). Owner cut off but alive: the node timeout (3 s) + its clean release + the open | About 4 s |
| Fence against a deposed writer | The NFS server's lock state (§6) | Replicas refuse an old epoch (design-cluster.md §6.3) |
| Rebuild | None: the data never moved. ZFS resilvers its own disks | A new replica from a hard-linked snapshot, then catch-up (§8) |
| NFS server or storage outage | Its volumes are UNAVAILABLE until it returns (ha.md §15); Pulsar holds the writes | No shared storage; two servers may be lost |
| Merges | Once, by the owner | On every replica (C9) |
| Reads | The owner; lazy followers on NFS with `STALE_OK` (ha.md §7) | The leader; ISR members with `REPLICA_OK` (C15) |
| Moving a volume | A handoff: no data copied | A learner copies, then the leader moves |
| On disk | `LOCK` (slots, ledger), `MANIFEST.<E>`, `CLAIM`; `requires=nfs-ha` | `REPL`, marker records; `requires=cluster` |
| Read lease | Not needed for safety: the lock holder is the only writer. Reads stop when the ack gate does (§5.5) | A metadata lease bounds stale leader reads (§7.1) |

**Names.** ha.md D9 made names unique per volume (a series addressed by `(path, name)`). The decision of 2026-11-02
(series64; design-cluster.md D4) made them unique in the whole database, and the cluster keeps that with name slots.
Shared mode uses the same name slots: a slot's home volume is a volume of either kind, its owner serialises creation
of the slot's names. A path still names a volume (`volume add --shared PATH`), but a series is found by name.

## 4. Mixing modes

- **Per volume.** The metadata record of a volume has a kind; every engine opens one kind (its META says which).
  TOPOLOGY gives per volume an address and an epoch, so clients do not know the kind.
- **What a mixed node needs**: the NFS mounts of the shared volumes it is a candidate for, and local disks for the
  replicas it holds. A node may have only one of them; placement asks for the capability (a member advertises
  `shared` and/or `disks`).
- **What does not mix**: one volume is never both. A series never moves between volumes (its id names the volume), so
  migrating from NFS to local disks means creating new series in replicated volumes and letting the shared ones age
  out, or an export and import by name.
- **Recommendation (M7)**: the kind field from the first metadata release; each mode built and gated alone first;
  mixed deployments supported after a mixed `jepsen-cluster` run. Until then `tsdb cluster init` takes `--mode`.

## 5. The control plane

### 5.1 The options

- **(a)** One Raft metadata group (cluster step 1's `server/meta.c`) for both modes; in shared mode it hands out
  volume ownership and epochs, replacing ha.md's lock arbitration, ranks, slots-as-liveness, `NODES` and `OWNERS`.
- **(b)** ha.md's own control plane for shared mode: the OFD lock on `LOCK` is the arbiter, standbys poll by rank,
  heartbeats in the slots, `OWNERS` gossip, `WANT` files, the hold-back after grace, D38's isolation rule. No Raft.
- **(c)** Both: (a) where a cluster runs, (b) for NFS-only deployments.

In every option the fence on the storage stays (M3, §6): the question here is only who decides which node should own a
volume, and which epoch it gets.

### 5.2 Judged against the faults

| Fault | (a) metadata group | (b) ha.md's own control plane |
|---|---|---|
| **An NFS server out for hours** | Every node reports the domain DOWN in its heartbeat, so nothing moves (M6). Its volumes are UNAVAILABLE, the rest go on. The metadata group itself lives off that server (§5.6), so routing, creation in other volumes, logins and admin go on. When the server returns, owners reclaim in grace; an owner that missed grace reports FENCED and is granted its own volume again at E+1 (a cold open, no move). | Its volumes are UNAVAILABLE, the rest go on. Standbys block in `F_OFD_SETLK` on the dead domain and must hold back for grace + 5 s after it returns so as not to take volumes from owners still reclaiming (ha.md §15.2); an owner that missed grace loses its volumes to whichever standby wins. The root (`NODES`, `VOLUMES`) is on one server; its outage stops admin. |
| **A node crash** | Declared down after 3 s; the metadata group grants each of its volumes to one candidate at once, at most 4 takeovers per server group per node (ha.md §4's cap, now scheduled centrally). The claimant gets the lock when the dead node's NFS lease has expired: about 11.5 s. | Rank 1 polls every 250 ms and gets the lock at the same 11.5 s. Same time; more NFS round trips (about 200 small RPCs per second at V=32, N=4, ha.md U11). |
| **A partition between nodes, NFS reachable by all** | The Raft majority decides who is cut off. An owner on the minority side is declared down, its volumes granted to candidates on the majority side; it holds its locks until it learns of the higher epoch, from the `CLAIM` file at the latest (M5), then releases cleanly. A symmetric split without a majority changes nothing. | NFS cannot see the partition: everyone renews its locks, nothing moves (D14). An owner cut off from all peers releases on D38's rule ("better-connected nodes alive on NFS", read from probe files), a heuristic that a split into two halves cannot decide. |
| **A node's own NFS path fails** (RDMA port, mount), others fine | The owner reports the domain DOWN, the other candidates report it UP: after `local_fault_s` the volume is granted elsewhere (M6). The claimant gets the lock once the owner's NFS lease expires (the owner cannot renew it). When the path heals, the old owner's locks are LOST: EIO, FENCED. | The same end, by lease expiry alone: the standby's poll wins at about 11.5 s. Equal. |
| **NFS grace of 10 s** (D34) | Only a granted claimant ever asks for the lock, so nobody contends with reclaiming owners: no hold-back timer is needed. A node the metadata group knows is alive is never raced. | Standbys must hold back by a timer with an early end (heartbeat advance, a successful open), because they cannot know whether the owner is alive and reconnecting. |
| **Clock skew** | No clock in safety (§6.4). Raft and the node timeout use each node's monotonic clock; skew between nodes changes timings only. The ack gate (SUSPECT) stays a belt, as in ha.md D11. | No clock in safety either (D11). Hold-back and staleness rules use monotonic time. Equal. |
| **The control plane itself fails** (a Raft majority down) | No new grants, no moves, no admin; owners keep their locks and keep serving (M5). A node crash in that window waits for the metadata group. | Not applicable: the NFS server is the control plane, and its outage is the row above. |
| **Operational simplicity** | One control plane for both modes, one source of TOPOLOGY, users in one place, one CLI. Needs 3 voters: a 2-node shared deployment adds a witness (a metadata-only `tsdbd --witness` on any third host). | No extra process; two nodes are enough. But it is a second control plane with its own files (`NODES`, `WANT.*`, slots as liveness), gossip (`OWNERS`, high-water marks in `volumes.cache`) and timers, to build and test beside the metadata group the cluster needs anyway. |

**Decision: (a)** (M2). It is never worse on the faults that matter here and is better on three: an NFS server
returning (no hold-back races, an owner that missed grace keeps its volumes), a partition between nodes (a majority
decides, not a heuristic), and a node-local storage fault (told apart from a server outage). Its cost is a Raft group
of 3 or 5 voters, which the cluster needs anyway, and a witness for 2-node deployments. **(c) is rejected**: a second
control plane is a second set of failover paths that every later change must keep correct under jepsen, for the sake
of deployments of two nodes, which a witness serves. A 1-node deployment needs no HA: it is today's tsdbd with ha.md
§15's outage handling.

### 5.3 What the metadata group replaces in ha.md

| ha.md | Becomes |
|---|---|
| The lock as arbiter (D2): whoever gets the lock owns the volume | The lock as fence only; the metadata grant decides who may ask for it (M4) |
| Rank polling, rank-staggered polls on a stale heartbeat (§6, D12) | One granted claimant polls; nobody else touches the lock |
| `heartbeat_ctr` in the slots as the owner's liveness signal | Heartbeats to the Raft leader (design-cluster.md §2.2). The slots keep their epoch and state; the counter stays for the read-back |
| `handoff_to` hints and `WANT.<node>` files (§2.6, §6) | Moves and rebalancing as metadata operations (`VOLUME_LEADER` conditional on the epoch) |
| `NODES` in the root | Members in the metadata group |
| `OWNERS` gossip of owners and epochs (§9.1) | The metadata watch stream; TOPOLOGY from the local copy |
| Epoch claim from the high-water marks of every node (D36, §2.4) | The epoch from the grant; storage must be below it (§6.4, G2) |
| D38: an isolated owner releases | M5: an owner releases when it learns of a higher epoch; the `CLAIM` file carries it when nothing else can |
| The standby hold-back after grace (§15.2) | Not needed: a live owner is never raced |
| The takeover cap per server group (§4) | Applied by the metadata leader when it grants |
| `VOLUMES` and `volumes.cache` for routing (§5, §15.3) | Volumes in the metadata group, with their storage record; the root `VOLUMES` stays only for single-server databases |
| `USERS` in the root (design-auth.md) | Users in the metadata group (C22) |

What stays NFS-mode-only in ha.md: §2 (lock, chained fds, `LOCK` layout, epochs on storage, the ledger, grant and
abandon, the ack gate), §3 (the proof), §4 steps 2-8 (recovery under the lock), §7 (followers), §8's watchdog and
escalation, §10, §12, §15 (NFS outages).

### 5.4 Shared volumes in the metadata

Additions to `server/meta.h` (cluster-s1), all small:

- `struct meta_volume` gets `kind` (`META_VOL_REPLICATED`, `META_VOL_SHARED`) and, for shared volumes, a storage record:
  `path` (the mount path every candidate uses, ha.md §1), `export` (`server:/export`), `fstype`, and the server group
  (for the takeover cap). The uuid it has already is the volume uuid that META and `LOCK` carry.
- **Candidates are the replica list.** `rep[]` names up to `META_REPLICAS_MAX` (7) candidate nodes, 3 by default,
  chosen by the placement rules of design-cluster.md §3.2 (spread, even owners). For a shared volume the ISR is always
  every candidate: they all see the same storage, so none can fall behind. `META_OP_VOLUME_LEADER`'s rule (the new
  leader was in the ISR) then holds unchanged, and `META_OP_ISR_CHANGE` is refused for shared volumes. Moving a
  candidate is `REPLICA_ADD` and `REPLICA_REMOVE`; no data moves.
- **Epochs below 2^24 − 1** for shared volumes (ha.md §2.4: E is a u24 in block numbers); a grant beyond is refused.
- **High-water marks** (ha.md D36, the dictionary part): owners report their volumes' SERIES, STATES, CLASSES and
  RETENTION lengths and `(wm_gen, dropped_before)` in heartbeats; the Raft leader keeps the maximum and commits it
  with every ownership change of the volume (in the same entry) and at most once a minute otherwise
  (`META_OP_VOLUME_HW`, new). A claim fills LOST entries up to the highest of storage, the committed marks and the
  old owner's last heartbeat. The window this leaves (ids created less than a heartbeat before a pool rollback that
  coincides with a Raft leader change) is the same size as ha.md's gossip left.
- **Domain health** in heartbeats, not entries: per storage domain UP, DEGRADED, DOWN, RECOVERING (ha.md §15.2), and
  per owned volume its role (PROMOTING, OWNER, SUSPECT, FENCED) and health reason. TOPOLOGY carries them.
- **`tsdb cluster adopt ROOT`** takes an existing NFS database in (the successor of ha.md's `ha init`): it requires
  the unmount attestation (ha.md §4), adds each volume of the root's `VOLUMES` with its number, uuid and path, writes
  `LOCK` and the first checkpoint, and sets `requires=nfs-ha` in each META. Root series (volume 0) are migrated first
  (ha.md §5, `volume migrate-root`). One-way.

### 5.5 Takeover, release and reads under the metadata group

**Grant.** The metadata leader commits `VOLUME_LEADER(v, from E, owner S, epoch E + 1)` when (1) the owner is declared
down, (2) the owner reports v's domain DOWN while another candidate reports it UP for `local_fault_s` (M6), (3) an
operator or rebalancing moves v (a planned handoff), or (4) the owner reports v FENCED (S may be the same node).
It staggers grants: at most 4 takeovers per server group per node at a time, 0-5 s of jitter (ha.md §4, §15.5).

**Claim** (on S, on the volume's HA thread):
1. S sees the grant in its metadata copy and answers v's requests with UNAVAILABLE(PROMOTING).
2. If the old owner is alive, it sees the grant too and does a clean release (ha.md §2.6): MOVED with the new epoch,
   drain, flush, compact, slot `{RELEASED}`, poison, close. S's lock request then succeeds within one poll.
3. S polls `F_OFD_SETLK` every 250 ms (the call may block on a DOWN domain; HA threads may hang, ha.md §15.2). After
   two refusals it writes `CLAIM` (below), so an owner that cannot hear the metadata group learns of the grant.
4. On the grant of the lock, ha.md §4 steps 2-8, with step 4 changed: read every epoch on storage (both slots, the
   checkpoint, `EPOCH` records); if any is at or above E + 1, refuse, release the lock and raise an alarm (two grants
   crossed, or a writer outside the metadata group); otherwise write `{E + 1, RECOVERING}` to the inactive slot and
   the `EPOCH` record. The rest of recovery is ha.md's.
5. S reports OWNER in its next heartbeat; TOPOLOGY, which already named S at E + 1, drops PROMOTING.

If the lock stays refused while the domain is UP, S keeps polling and the alert names the holder from the slot: the
holder is alive on NFS and has not let go, and only it can (M5).

**The `CLAIM` file.** `<vol>/CLAIM`, 4 KiB, written by a refused claimant with one O_DIRECT `pwrite` at offset 0:
`{magic, volume uuid, granted epoch, node id, metadata index, xxh64}`. The owner reads it with its epoch read-back
(every lease/5, ha.md §2.7). A valid record of its own volume with an epoch above its own means: ask the metadata group
if it can be reached, else release cleanly. A torn or older record is ignored. It carries news, never authority: a
forged or stale one can only make an owner release (availability), never make two writers (§6).

**Reads.** The owner serves reads only while its ack gate holds (ha.md §2.7: a fenced write succeeded within lease −
margin, by its monotonic clock), and not at all once FENCED or released. A wrong clock there costs at most a stale
read (as design-cluster.md C13). Followers and `STALE_OK` stay as ha.md §7.

### 5.6 Where the metadata group lives

Voters keep the Raft log and hard state in `<state>/meta/raft/` (design-cluster.md §2.3): kilobytes, written a few
times a minute. On nodes without data disks it goes on the **OS disk** (recommended), which every node has for its own
system. If a node has no writable local disk at all, a voter's log may live on NFS, but then **each voter on a
different NFS server** (placement by server group), so an NFS outage takes one voter at most and the group keeps its
majority. A voter's log never shares a server with another voter's. This is open question 2.

## 6. Fencing on shared storage

This is the correctness core of shared mode. It is ha.md §2-§3, restated against the new control plane, with what
must not be dropped when the arbiter changes.

### 6.1 What a deposed writer can still do

A writer W of epoch E whose time is over (another node was granted E + 1, and may hold the lock) may:
- be paused for any length (SIGSTOP, a VM pause) with its monotonic clock frozen, then resume mid-syscall;
- be cut off from every peer and the metadata group, but still reach NFS, and not know it is deposed;
- still hold open descriptions: the WAL segment, `SERIES`, `STATES`, `CLASSES`, `RETENTION`, `LOCK`, tmp parts,
  `MANIFEST.<E>`, and `dirfd`;
- `pwrite`, `fdatasync`, `ftruncate` through them, and `renameat`, `unlinkat`, `openat(O_CREAT)` in the volume's
  directory: namespace operations carry no lock stateid, so NFS never refuses them for a lost lock.

The fence must hold against all of it without a clock and without the metadata group's help.

### 6.2 The fence

These are ha.md's rules; the change of arbiter alters none of them.

- **F1. Every byte an acknowledgement or a commit relies on is written through a locked description.** `LOCK` (slots
  and ledger) holds the OFD write lock; every WAL segment, `SERIES`, `STATES`, and now also `CLASSES` and `RETENTION`
  (append-only files added after ha.md was written; AGENTS.md lists them as fail-stop) are chained: OFD-locked on the
  description the owner writes through (ha.md §2.2). When a claimant's conflicting lock request expires W's NFS client
  state, every lock state of that client becomes LOST, which is sticky, and the server refuses I/O through those
  descriptions (EIO; ha.md L1(b), L3, U1). When W released, its descriptions were poisoned and closed before the
  lock went (L1(a), U19). This is the only storage-enforced fence NFS has, and it is the server's, so W's view and
  W's clock do not matter.
- **F2. Continuity.** A chained description carries a commit only after a later fenced write through an already
  trusted one has succeeded; the root of trust is the claimant's `EPOCH` record through `LOCK` (ha.md §2.2). A lock
  re-taken on fresh client state therefore cannot pass for an old one.
- **F3. Namespace effects are made harmless, not fenced.**
  - Names carry the epoch: segments `wal_<(E << 40) | n>`, tmp parts `tmp_<E>_<node>_<n>`, `MANIFEST.<E>` (D4, D10).
    W can only create names of epoch E, which nobody of E + 1 adopts unless a record of E names them.
  - A file joins the volume only through a ledger record (COMMIT, FLUSHED, MERGE), written under F1 (D3, D6). W's
    renames of unrecorded tmps make orphans, which the successor lists by readdir and purges.
  - An adopted name is unlinked only after a fenced PURGE record (L6); W's PURGE append fails under F1, and the abort
    rule (ha.md §2.6) forbids every cleanup after a fenced error. The successor gives retired names fresh lifetimes.
  - The successor compacts the ledger into the other region at claim (ha.md §4 step 6): a record of E still in flight
    can land only in a region nobody replays.
- **F4. Acknowledgement.** An ack needs the sync through a fenced description to return 0 and `fence_gen` unchanged
  (ha.md §2.7). The clock-based gate (SUSPECT) is a belt on top, not part of the argument.
- **F5. One writer per description and the poison.** Each fenced description has one mutex, so no sibling swallows
  its writeback error; abandon poisons by `dup2`, never `close`, so a stale thread's write cannot reach a reused fd
  number of another volume (D7).

### 6.3 Mechanisms rejected

| Mechanism | Why it does not fence |
|---|---|
| W reads an epoch file (or asks the metadata group) before each publish or ack | Check, then act: W checks E, pauses 30 s, S claims E + 1 and replays W's segment, W resumes and appends and acknowledges rows S never read. With a frozen clock nothing on W notices. |
| Epochs in file names alone | W keeps appending to its own `wal_<E…>`, which S must replay (it holds acknowledged rows); appends after S's read are acknowledged and lost. W can still unlink adopted parts. |
| A Raft-conditional acknowledgement per group commit | Puts Raft on the insert path (design-cluster.md C4, the user's stance of 2026-10-24), costs a round trip to the Raft leader per commit, and still leaves renames and unlinks unfenced. |
| A time lease from the metadata group | W can pause between the lease check and the write (Kleppmann's GC pause); in a VM pause the monotonic clock may not advance at all (ha.md §10). Time bounds only reads (§5.5). |
| Revoking access on the server (mode bits, renaming the directory) | nfsd checks access at open and lets an owner write its own files; a rename does not change the file handle W writes through. |
| The ledger in the metadata group (M10) | Would fence namespace effects (epoch-conditional entries), but not WAL appends; it puts every commit, flush and merge of every volume through Raft; and a pool rollback would leave it naming files the storage no longer has. |

### 6.4 Why it holds under the metadata group

- **G1 (one lock).** The NFS server grants `LOCK`'s write lock to at most one description at a time (ha.md L1). The
  metadata group cannot change that, in either direction.
- **G2 (epochs on storage only rise).** A node writes epoch E to storage only (i) after the metadata group committed
  `(v, owner = it, E)`, (ii) under the lock, and (iii) if every epoch on storage is below E (§5.5 step 4). Take two
  nodes with grants E1 < E2, whatever their views: if the holder of E2 claims first, the holder of E1 later finds E2
  on storage and refuses; if the holder of E1 claims first, the holder of E2 finds E1 < E2 and proceeds. So the
  sequence of epochs on storage is strictly increasing across lock grants. The metadata log is off the pool (§5.6),
  so a pool rollback cannot lower the next grant; ha.md L2 used high-water gossip for the same purpose (D36), which
  is no longer needed for epochs.
- **G3 (the rest of ha.md §3 unchanged).** L1 and L3-L8 mention neither who arbitrates nor how E was chosen beyond L2;
  they hold as written: commits are fenced (F1, F4), the successor reads after the grant (L4), no acknowledged write
  is lost (L5), late namespace operations are harmless (L6, F3), series ids are assigned only after a fenced append
  (L7), streams stay ordered (L8).
- **G4 (the metadata group's faults cost availability only).** A lost majority, a stale watch copy, a Raft leader
  change or even a bug that grants one volume to two nodes cannot make two committing writers: G1 and G2 decide. A
  node that cannot learn of its grant does not claim; an owner that cannot reach the metadata group keeps its lock and
  serves (M5).
- **G5 (liveness).** A deposed owner learns of the higher epoch from the watch, a peer's MOVED, or `CLAIM` on its own
  storage (§5.5), and releases. If it can reach none of them, it cannot reach its storage either, and then its NFS
  lease expires and the claimant's lock request expires it (F1).

**Walk-throughs.**
- *W paused past the lease, then resumed.* The metadata group declared W down and granted S; S's lock request expired
  W's client (W did not renew while paused). On resume W's in-flight `pwrite` or `fdatasync` fails with EIO (LOST):
  FENCED, abandon, no ack (F1, F4). Its rename of a tmp makes an orphan (F3).
- *W cut off from peers and the metadata group, NFS fine.* W renews its lease and keeps the lock. S is granted E + 1,
  is refused, writes `CLAIM`. Within lease/5 W reads it, releases cleanly; S gets the lock. No moment with two
  writers: W's last acknowledged rows were flushed or are in segments S replays (L5).
- *W's NFS path fails, others fine.* W's writes hang (hard mount): no acks, SUSPECT. M6 grants S; S gets the lock
  after W's lease expired. When W's path heals, its writes fail with EIO (LOST), it abandons.
- *NFS server restarts; W misses the 10 s grace.* W's state is lost: EIO on its locked descriptions, FENCED. W reports
  it; the metadata group grants v to W again at E + 1 (no move); W claims as any claimant, with replay. No
  acknowledged write is lost (ha.md D34).
- *Two grants crossed* (a metadata bug, or an operator's `force` on a stale view). G1 serialises them; G2 refuses the
  lower epoch with an alarm.

**What must not be dropped** when building shared mode on the metadata group, though the metadata group might seem to
cover it: the OFD lock on `LOCK` and the chained fds (F1), the ledger and its edit discipline (F3), epoch names
(F3), the abort rule and the poison (F5), and the claim's check of the epochs on storage (G2).

## 7. Combined build plan

Each step is a track (build, adversarial review, fixes) with the gates of AGENTS.md: `make test`, `make asan`,
`make tsan`, and `make jepsen` for anything that changes how data reaches the disk; shared-mode steps from N7 on also
`make jepsen-ha`, replicated steps from R3 on `make jepsen-cluster`; client changes `make test-clients` and
`make integration-tests`. Weeks are working weeks of one builder track including review.

| Step | Mode | Content | Gate beyond the usual | Weeks |
|---|---|---|---|---|
| S0 | both | **Before 1.0** (design-cluster.md step 0, D6): format 9 (WAL record v2 with `fno`, kinds; part position range; META `requires=`, which also carries `nfs-ha`), MOVED and UNAVAILABLE in the four clients, node lists | jepsen | 2 |
| N0 | shared | **Go/no-go on production hardware**: ha.md §14 step 0 (lock probe: U1, U2, U5-U7, U13, U19, U21) and §15.9 O0 (timeouts, grace, forced reconnect U41) on AlmaLinux 10 over NFSv4.2/RDMA | the probe's results | 1.5 |
| N1 | shared (single server) | ha.md §15 O1-O9: storage domains and probes, admission, WAL taint, recovery and revalidation, DIVERGED, the outage nemesis. Useful without failover; ships to production first. O4's precondition (series addressing) is built (series64) | jepsen with the `outage` nemesis | 6 |
| S1 | both | Rest of design-cluster.md step 1: watch stream, heartbeats, `tsdb cluster init / node add / status`; plus shared volumes (§5.4: kind, storage record, candidates, high-water marks, domain health, the takeover cap, `--witness`) | meta-soak with shared volumes | 3 |
| N2 | shared | ha.md's bug fixes B1-B6, those still open on main | jepsen | 1 |
| N3 | shared | Ledger core (ha.md §14 step 2): records, regions, `lstate`, compaction; `ha_local.c`, `ha_fake.c` | test_ledger property test | 2 |
| N4 | shared | Ledger engine (ha.md step 3): edit discipline, epoch names, `MANIFEST.<E>`, followers on it, `requires=nfs-ha`; `CLASSES` and `RETENTION` among the chained files | jepsen (power, kill, pause); benches unchanged | 4 |
| N5 | shared | Grant and abandon (ha.md step 4): poison, drain, the abort rule | crashfs EIO leaves the volume unchanged | 1.5 |
| N6 | shared | NFS backend (ha.md step 5): OFD lock, chaining, read-back, `fence_gen`, ack gate, SUSPECT | test_failover with `FENCE=lockfile` and `FENCE=none` | 3 |
| S2 | both | tsdbd peer links (cluster secret), TOPOLOGY from the metadata copy, forwarding (ha.md §9.1 without `OWNERS`), per-volume admission and reply order, users in the metadata group | test_server, forwarding tests, tsdbc-bench direct and `--via` | 4 |
| N7 | shared | Shared-mode HA manager: grants to claims (§5.5), `CLAIM`, clean release, M6 moves, re-grant after a missed grace, `tsdb cluster adopt`, watchdog and escalation (ha.md §8, §15.2) | `make jepsen-ha`: kill, power, pause, partition, NFS restart, local NFS fault | 3 |
| S3 | both | Clients (design-cluster.md step 6): direct mode, streams, the 32-byte header, QUERY_MULTI split; Pulsar sink | test-clients, integration-tests with failover | 3 |
| S4 | both | Name slots, creation and lookup across volumes (design-cluster.md step 5) | jepsen-ha / jepsen-cluster with creations | 2 |
| N8 | shared | Real NFS/RDMA runs (ha.md §13 a-h, §15 manual runs), takeover timings (U10), operator guide | the numbers | 2 |
| R1 | replicated | `src/repl.c` and its simulator (design-cluster.md step 2; in progress on `cluster-s2`) | replication simulator soak | 3 |
| R2 | replicated | Engine replica mode (step 3) | jepsen | 4 |
| R3 | replicated | Replication, ISR, promotion and leases in tsdbd (step 4 without what S2 built) | jepsen-cluster: kill, partition, pause, midcommit | 3 |
| R4 | replicated | Rebuild, catch-up, moves, leader balance (step 7) | jepsen-cluster: kill2, rebuild, slow | 3 |
| X1 | both | Cold-tier boundary (step 8); a shared volume hands partitions over the same way | midcommit at the move | 2 |
| X2 | both | Mixed deployments (M7): one metadata group with both kinds | jepsen-cluster mixed | 1 |
| X3 | both | Measurements and docs: README section, AGENTS.md invariants and lock order, operator guides | the numbers | 2 |

Totals: shared components 14 weeks, shared mode 24, replicated mode 13, both 5: about 56 weeks of one track. **Order**
(M9): N0 and N1 now; S0 before the 1.0 freeze; S1 and N2-N6 in parallel (the metadata group and the engine do not
touch each other); then S2 and N7 (shared-mode HA works end to end), S3, S4, N8. R1 continues in parallel from today;
R2-R4 follow N7. The critical path to shared-mode HA is N0, N2-N6 and N7 with its jepsen (about 16 weeks), with S1
and S2 (7 weeks, needed by N7) and then S3 on a second track: about 4 months with two tracks, plus N1 on a third.
Replicated mode follows about 3 months later.

### 7.1 What cluster steps 1 and 2 give shared mode

- **Step 1 (`cluster-s1`, 35e1456)**: the Raft core copied in, the metadata state machine (members, volumes with
  replicas, leader, epoch, ISR, epoch history, name slots, users), epoch-conditional operations applied one by one,
  snapshots, the step checker and the simulator. All of it serves shared mode as it is: an owner change is
  `META_OP_VOLUME_LEADER` with candidates as the ISR (M8). Missing for shared mode: the volume kind and storage
  record, refusing `ISR_CHANGE` on shared volumes, the u24 epoch bound, `META_OP_VOLUME_HW`, and the rest of step 1
  itself (watch stream, heartbeats with domain health, the CLI). The checker gains: a shared volume's ISR always
  equals its candidates.
- **Step 2 (`cluster-s2`, in progress)**: the sans-IO replication core. Shared mode needs none of it (one copy, no
  ISR), and is not blocked by it. What carries over is the method (a sans-IO core under a seeded simulator), which
  the shared-mode HA manager should copy: grants, lock results, heartbeats and `CLAIM` reads in; claims, releases and
  reports out; checked for G1-G5 against a fake NFS server that expires client state.

## Open questions

1. **A witness for 2-node shared deployments** instead of keeping ha.md's lock arbitration as a second control plane
   (option (c))? *Recommended: yes.* The witness is `tsdbd --witness`: a metadata voter with no volumes, on any
   third host. A 1-node deployment needs no HA.
2. **Where do voters keep the Raft log on nodes without data disks?** *Recommended: the OS disk* (kilobytes, a few
   writes a minute). Otherwise on NFS, each voter on a different NFS server (§5.6).
3. **Mixed deployments** (shared and replicated volumes in one metadata group)? *Recommended: the kind field from the
   start, each mode gated alone first, mixed supported after one mixed jepsen run (X2)*, mainly as the path from NFS
   to local disks.
4. **Shared mode first** (M9), with N1 (outage handling on today's single servers) before any failover? *Recommended:
   yes*; the cluster's replication core continues in parallel.
5. **When can step N0 run on the production NFS servers** (the lock probe and grace measurements, which need nfsd
   restarts and nfsdcld, ha.md §15.10 q4)? *Recommended: before N3*; if OFD locks do not fence over RDMA, ha.md §14
   step 0's fallback (POSIX locks owned by one thread) changes F1's mechanics, not this design.
6. **`local_fault_s` and the node timeout for shared volumes.** A move costs a cold open (0.5-3 s per volume) and an
   NFS lease wait. *Recommended: node timeout 3 s as in the cluster, `local_fault_s` 10 s*, both per-cluster settings,
   to be measured in N8.

## Unverified assumptions

| # | Assumption | How it will be verified |
|---|---|---|
| V1 | An owner reading `CLAIM` with O_DIRECT every lease/5 costs as little as ha.md's slot read-back (U11). | `nfsstat -s` under load in N8 |
| V2 | The Raft log on an OS disk syncs within a few milliseconds, so grants are not slowed. | meta-soak timings on a production node |
| V3 | A claimant's lock request expires a dead owner's client state at about 11.5 s also when the owner's other volumes on that server are owned by the same node (expiry is client-wide, ha.md §10). | lockprobe in N0 |
| V4 | Reusing `META_OP_VOLUME_LEADER` with ISR = candidates needs no change to `meta_check_step` beyond the shared-volume rule. | test_meta and the meta-soak in S1 |
