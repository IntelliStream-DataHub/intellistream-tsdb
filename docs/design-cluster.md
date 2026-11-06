# tsdb cluster: replicated volumes on servers with their own disks

Status: design, not implemented. Branch `cluster-design`, written on 2026-11-05 from main at 9b00186. Code references
are `file:line` at that commit. Builds after the 0.9.0 tag; what must be in 1.0 so that the cluster is an addition and
not a break is §11. Revised on 2026-11-06 with the user's answers to its open questions (D1-D6 below), and again on
2026-11-06 when the user kept the NFS failover as a second HA mode (D3 replaced; [design-ha-modes.md](design-ha-modes.md)).
This design is the *replicated mode* of tsdb's two HA modes; ha.md is the *shared mode*.

Decided by the user on 2026-11-05 and not reopened here:
1. A cluster (for example 8 servers) stays up, and keeps almost all of its performance, when one or two servers go
   down **with their disks**. Failover is automatic.
2. Hot data lives on replicas. Cold, old parts move to a separate tier (erasure coded; §9 is only the boundary,
   the cold tier gets its own design).
3. Raft stays **off the insert path** (the stance of 2026-10-24: Raft-replicated inserts were called a nightmare).

Decided by the user on 2026-11-06 (the open questions of the first draft):
- **D1. Minimum ISR 1 by default**, with an alert and rebuild priority for volumes down to one in-sync copy;
  `min_isr = 2`, or replication 4, as per-volume options for data that must survive a third loss (C11, §6.6, §8.4).
- **D2. The Raft core is copied into tsdb under Apache 2.0.** The user, as owner of intellistream-objects, grants
  copying its `src/raft` (core, log, wire, simulator) into tsdb under Apache 2.0, with Apache headers and a sync
  script (C3, §2.3). intellistream-objects should get a LICENSE (or a grant file naming Apache 2.0) as well, and
  tsdb's [licenses.md](licenses.md) lists the copied files when they land (build plan step 1).
- **D3. Replaced on 2026-11-06.** It read: "The cluster replaces ha.md for new work; its NFS lock, ledger and SUSPECT
  state are dropped." The user then decided: "I have been thinking, I still want to keep the NFS implementation."
  So ha.md's NFS failover stays **as a second HA mode** (shared mode) beside this one (replicated mode), for
  production's nodes without local data disks on NFS/ZFS. [design-ha-modes.md](design-ha-modes.md) is the overview:
  both modes run on this design's metadata group (§2), which in shared mode names each volume's owner and epoch; the
  NFS lock, the chained fds, the ledger and the ack gate stay there as the fence on the storage; the client rules,
  streams, forwarding, drop watermark, users and name slots are shared. Everything else here (D1, D2, D4-D6, C1-C25)
  stands for replicated mode.
- **D4. Name slots**: names stay unique in the whole cluster, which keeps the decision of 2026-11-02 (C18, §3.3).
- **D5. Reads from leaders by default**; `REPLICA_OK` per query or per client (C15, §7). The user did not answer this
  one: the recommendation is the default, and the user may change it.
- **D6. The 1.0 room of §11** (format 9 shared with the change feed, the position range in part headers, META
  `requires=`, clients retrying MOVED and UNAVAILABLE, connect strings with several servers) lands in a 0.9.x before
  the 1.0 freeze (build plan step 0). Not answered either; the recommendation is the default, as for D5.

Scope: the replicated hot tier. The metadata group, placement, the write path to in-sync replicas, failover and
fencing, reads, rebuild, the two-servers-down case, room in the 1.0 format and protocol, tests and a build plan.
**Not here**: erasure coding, shard placement, ISA-L, degraded reads. §9 says only what the hot tier hands to a
cold tier and what it needs from one.

Related designs: [design-ha-modes.md](design-ha-modes.md) (the two HA modes, what they share, one control plane, the
combined build plan), [ha.md](ha.md) (shared mode: failover over shared NFS storage, the other HA mode (D3); its
client rules, streams, forwarding and drop watermark are shared with this design, its NFS lock and ledger are shared
mode's fence, its lock arbitration is replaced by this design's metadata group), [design-series64.md](design-series64.md) (ids
`(volume + 1) << 32 | local`; names unique in the whole database), design-cdc.md on branch `cdc-design` (WAL record
version 2 with a per-volume number for every row: the cluster uses the same number), [design-auth.md](design-auth.md)
(users and grants; in a cluster they move into the metadata group).

## Summary

- **Volumes are the unit.** A cluster has many volumes (about 16 led per server). Each volume has 3 replicas on 3
  different servers (and racks, when racks are configured). One replica is the volume's **leader**: it takes the
  volume's writes and is the default for its reads. A series id already names its volume, so routing stays a shift.
- **A small Raft group keeps the map.** 5 servers (3 in a cluster of 3 or 4) run the user's sans-IO Raft core
  (intellistream-objects `src/raft`, copied into tsdb). It stores members, volumes, replicas, leaders, epochs, the
  in-sync sets, name slots and users: kilobytes, written only when something changes. No row ever goes through it.
- **Inserts are replicated by the leader, Kafka style.** The leader sends its WAL bytes, unchanged, to the volume's
  in-sync replicas while it writes them itself. A write is acknowledged when every in-sync replica, the leader
  included, has it on disk. Large inserts, deletes and replaces: the leader writes the part files once and sends the
  files; every replica commits them under its own block numbers.
- **Nothing is visible before it is committed**, on any replica. So a crashed leader never leaves data behind that
  the others lack in their parts; an unacknowledged tail of its WAL is cut when it comes back.
- **The in-sync set shrinks on failure** through the Raft group, which only accepts a change from the volume's current
  epoch. A deposed leader can therefore never acknowledge again: it needs either every in-sync replica (the new
  leader refuses it) or a smaller set (the Raft group refuses it). Clocks play no part in that.
- **Failover**: a server that misses heartbeats for 3 s is down; for each volume it led, the Raft group picks the
  in-sync replica with the longest log, raises the epoch, and clients follow TOPOLOGY and MOVED as in ha.md §9.
  Writes to those volumes pause for about 4 s; every other volume does not notice.
- **Two servers down**: every volume keeps at least one in-sync replica, so writes go on. A volume that lost two of its
  three replicas is acknowledged on one disk until a new replica has caught up (D1).
- **Rebuild** copies a replica's immutable parts by hard-linked snapshot from any in-sync replica, throttled, then
  catches up from the leader's WAL. A server back after a short outage only catches up.
- **Cold tier boundary**: a settled, idle, old partition is handed to a cold tier through four calls (put, read
  range, delete, list); every replica keeps the partition's granule index on local disk, so statistics-only queries
  stay as fast as today.
- **For 1.0**: the WAL record with a position number (shared with the change feed), a position range in part
  headers, a `requires=` line in META that refuses unknown features, and clients that retry on the two reserved
  failover statuses. The embedded library and a single tsdbd stay as they are.

## Assumptions (decisions the user may change)

| # | Assumption | What changes if it does not hold |
|---|---|---|
| A1 | **Shared-nothing.** Each server has its own disks; the cluster does not use the NFS/ZFS servers. | With shared storage, ha.md (one copy, failover by lock) is cheaper. Since 2026-11-06 both are kept as the two HA modes, one per deployment, on the same metadata group design (D3, [design-ha-modes.md](design-ha-modes.md)). |
| A2 | **Writes go on with two servers down, and no acknowledged write is lost** when up to two servers and their disks are lost. Hence: replication factor 3, acknowledge only after every in-sync replica has the write durable, and an in-sync set that shrinks on failure (Kafka's ISR). | A minimum in-sync size of 2 (a per-volume option, D1) or a replication factor of 4 trades availability or disk for safety against a third loss. |
| A3 | **Design now, build after the 0.9.0 tag**; the 1.0 format and protocol leave room (§11), so the cluster arrives in a 1.x without a break. | If the cluster must be in 1.0, the build plan (§14) moves before the freeze. |
| A4 | The servers are in one data center, 25 GbE or faster between them, round trip below 100 µs, NVMe with `fdatasync` around 0.7 ms (the README's drive). | Across sites, every small insert pays a WAN round trip. |
| A5 | Clocks are disciplined by chrony or ntpd with a bounded slew rate (500 ppm, chrony `maxslewrate 500`). Only read leases depend on it (§7); write safety does not. | Leader reads must confirm leadership per read (one round trip) instead of a lease. |

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| C1 | **The volume is the unit** of replication, leadership and placement, as in ha.md D1. A cluster has many: by default 16 led per server (128 on 8 servers), so a lost server's work and its rebuild spread over all the others. | One volume per server; per series; per partition | A series id names its volume (series64), so routing needs no table beyond volume to replicas. One volume per server would put a dead server's whole load on its two replica holders, and rebuild from two sources. Finer units would need a map the size of the data. |
| C2 | **Metadata in a Raft group of 5 voters** (3 below 5 servers; at most 5 at any size). Other servers are not Raft members: they follow the applied state through a watch stream from any voter. | etcd or ZooKeeper; every server a voter; the NFS lock of ha.md | The user ruled out an external coordinator (ha.md D2). More voters only slow commits. A shared-nothing cluster has no shared lock. 5 voters survive 2 losses, which is the requirement. |
| C3 | **The user's Raft core**, copied into `src/raft/` from intellistream-objects at a recorded commit, under Apache 2.0 by the owner's grant (D2, 2026-11-06; §2.3). | Writing a new Raft; linking the other repository | It is sans-IO, tested by a seeded simulator with crashes, torn disks and partitions, has pre-vote, check-quorum, learners, leader transfer and a durable floor. A copy keeps tsdb's build self-contained; a script re-syncs it. |
| C4 | **Raft is not on the insert path.** Each volume leader replicates to its in-sync set directly (§4, §5). | Raft per volume; majority quorum writes | The user's decision. A majority of 3 cannot write with 2 replicas down; an in-sync set can. |
| C5 | **The replicated log is the leader's WAL, byte for byte**, in the same segments, plus marker records for everything that is not rows (§4). | Each replica re-framing rows into its own WAL; shipping client frames; a separate control log | Identical bytes need no re-encoding, the existing record checks hold unchanged (they bind a record to its segment and offset), and a replica's memtables match the leader's, so "flush through segment S" means the same everywhere. |
| C6 | **One position counter per volume**: the feed number `fno` of design-cdc.md (WAL record version 2). Commit points, catch-up, read tokens and the change feed all use it. | A separate replication sequence number | One number, one WAL format (format 9), and the feed and replication agree on order by construction. |
| C7 | **Durable on receipt, applied only up to the commit point**, on every replica including the leader. Applying means: rows visible to queries, a memtable flushed, parts listed, a dictionary appended. | The leader applying at once, as today | Parts never hold uncommitted rows, so a returning ex-leader only cuts a WAL tail. The engine already has the mechanism: in sync mode a row is visible only once the WAL is synced up to it (`mt_collect` with `visible`). |
| C8 | **Large inserts, deletes and replaces: the leader writes the parts and ships the files**; the replicas store them pending, a marker commits them, and each replica commits them under its own block numbers (§5.2). | Replicas encoding the rows themselves | Encoded parts are about 1.2 bytes a row against 20 in a WAL row: 15 times less network and no encoding CPU on replicas. Block numbers live only in file names (part.c:21, 782), so the file bytes do not depend on where they are committed. |
| C9 | **Each replica flushes and merges on its own.** Replicas are logically equal (same rows, same answers), not byte equal (§5.6). | The leader shipping merged parts; merges as log commands | Shipping merges costs the network several times the compressed ingest per replica. Merges as commands would stall a replica's apply behind a merge of minutes, or need out-of-order apply. |
| C10 | **Only the leader decides** anything that depends on a clock or on local state: class switches, the retention clock (`NOW` markers), retention limits, drops, series and state creation. Replicas apply the markers (§5.4, §5.5). | Replicas deciding on their own clocks and histories | Two replicas deciding a class switch or a cutoff differently would answer differently. |
| C11 | **Commit = every in-sync replica has it durable.** The in-sync set (ISR) changes only by a Raft commit that names the volume's current epoch. The minimum ISR is 1 by default, 2 per volume as an option (D1, 2026-11-06). | Majority quorum; the leader changing the ISR on its own | With the ISR in Raft and conditional on the epoch, a deposed leader can never shrink its way to an acknowledgement (§6.3). |
| C12 | **Failover by the Raft leader**: a server is down after 3 s without a heartbeat; the new volume leader is the reachable ISR member with the longest log, after it has been fenced; the epoch rises. No unclean election: a volume whose ISR is all down waits. | Electing any live replica; client votes | Only an ISR member is known to hold every acknowledged write. |
| C13 | **Write safety uses no clock.** Clocks bound only how long a deposed leader may still answer reads (a lease, §7.1). | A time-based write fence | ha.md D11, and the Raft core's own rule: a wrong drift bound must cost at most a stale read. |
| C14 | **A returning replica cuts its divergent tail** by the epoch history (Kafka's leader epochs, KIP-101), then catches up (§8.2). | Rebuilding every returning replica | A server back after minutes would otherwise copy its whole data again. |
| C15 | **Reads go to the leader by default** (D5); any in-sync replica serves them on request, with a position token for read-your-writes (§7). | Any replica by default | A stale read by default surprises; leaders are spread evenly, so reads are spread too. |
| C16 | **QUERY_MULTI over several servers**: the client splits it per leader in direct mode and merges the replies by index; the entry server does it in simple mode. Each volume answers from its own cut (§7.3). | One snapshot across volumes | There is no cluster-wide instant without a global clock or a round of coordination per query. |
| C17 | **Clients, streams and forwarding as in ha.md** §9 and §9.1: a node list, TOPOLOGY, direct mode by default (D39), streams `(session, vol)` with `seq` and `attempt` (D13, D31), one hop of forwarding (D29, D30), MOVED and UNAVAILABLE, resends in order by time budget. | New client rules | They were designed for exactly this: a volume whose writer moves. Only TOPOLOGY's content changes. |
| C18 | **Names stay unique in the whole database** (decided 2026-11-02, confirmed with D4 on 2026-11-06) through **name slots**: a name hashes to one of 65,536 slots; each slot has a home volume, whose leader serialises creation of the slot's names and records every one of them (§3.3). | A name index in Raft; names unique per volume; asking every leader | A billion names do not fit a Raft state machine. Per-volume names reverse the user's decision. Asking every leader cannot stop two concurrent creations. |
| C19 | **Placement is explicit** in the metadata: replica lists per volume, chosen by a balancing rule (distinct servers, distinct racks, even leaders, even pairs) and changed only by moves (§3.2). | Placement computed from the member list (CRUSH, rendezvous hashing) | A computed placement moves data whenever membership changes, even briefly. Explicit lists move it only when the operator or a permanent loss says so. |
| C20 | **Rebuild from a hard-linked snapshot** of any ISR member, throttled per server, then catch-up from the leader's retained WAL segments (§8). | Copying only from the leader; replaying a log from the beginning | Spreading sources spreads the load; the WAL is not kept forever. |
| C21 | **Cold tier: only the boundary here** (§9). A whole partition, settled and idle, is handed over; every replica keeps its granule index locally. | Designing erasure coding inside tsdb now | The user moved the cold tier to its own design; isod may serve it over S3 (see Later). |
| C22 | **Users and grants live in the metadata group** in a cluster; peer links prove the cluster secret (ha.md D35), and a forwarded request carries the user it was logged in as. | A USERS file per server | Login must work on every server, and a password change must reach all of them. |
| C23 | **The replication logic is a sans-IO core** (`src/repl.c`) with its own simulator, as the Raft core is (§13.1). | Testing it only end to end | Truncation, epochs and ISR changes are where replicated logs lose data; a simulator explores millions of interleavings that a test cluster never reaches. |
| C24 | **The cluster is a tsdbd mode** (`--cluster`). The embedded library, a standalone tsdbd and NFS followers stay as they are; a cluster volume refuses a non-cluster open (§12). | A cluster mode in the embedded library | Replication needs peer links, an event loop and timers, which tsdbd has and an application embedding tsdb should not get. |
| C25 | **Two kinds of peer link per pair of servers**: log streams (small, latency bound, zstd-1 compressed) and bulk transfers (part files, snapshots; throttled). | One link for both | A 100 MB part on the same link would hold up every small insert behind it (ha.md D37 for the same reason). |

## 1. Words and roles

- **Server** (node): one `tsdbd --cluster` with its own disks, a node id and an advertised address.
- **Volume**: as today, one engine directory (an `eng`), identified by its number, which is part of every series id.
  In a cluster a volume has a name and a number given by the metadata group, and 3 **replicas**, each a directory on
  one disk of one server (`<data>/v<number>/`).
- **Leader** of a volume: the replica that takes its writes and decides its clock-dependent questions. **Epoch**: a
  number the metadata group raises each time a volume gets a new leader.
- **ISR** (in-sync replicas): the replicas that have every committed record of the volume, leader included. A write
  is acknowledged when every ISR member has it durable.
- **Learner**: a replica being built or catching up; not in the ISR.
- **Metadata group**: the 5 voters of the Raft group, and its **Raft leader** (not to be confused with volume leaders;
  any server can be both).
- **Position**: the per-volume number of a row or marker (`fno`, C6). **Commit point**: the highest position every
  ISR member has durable. **Log end**: the highest position a replica has durable.

## 2. The metadata group

### 2.1 What it stores

| Item | Content | Size |
|---|---|---|
| Cluster | uuid, format version, settings (replication factor, minimum ISR, timeouts, throttles) | < 1 KB |
| Members | node id, advertised address, peer address, rack, disks, state (up, down, draining, out), incarnation | about 200 bytes a server |
| Volumes | number, name, uuid, replicas (node, disk, state: learner or member), preferred leader, current leader, epoch, ISR, ISR version, epoch history (epoch → first position) | about 100 bytes plus 16 per epoch; 128 volumes: about 20 KB |
| Name slots | 65,536 entries: home volume, and the earlier homes of a slot that moved | 128 KB plus history |
| Users | design-auth.md's records (verifiers, grants) | about 200 bytes a user |
| Cold tier | its kind and settings (§9) | < 1 KB |

Everything fits in memory many times over; a snapshot is written as one file.

**Write rate.** Nothing in steady state: heartbeats are not entries. A server failure is one entry for the member
and one entry with every affected volume's new leader and ISR. An ISR change, a replica move, a volume or user added:
one entry each. Tens of entries a minute at worst, against a core that commits thousands a second.

**What it does not store**: rows, positions, commit points, part lists, series. Those change a million times a second
and stay with the volumes.

### 2.2 How it runs

- Voters: the first 5 servers by default, on 5 different racks when there are 5. If a voter is down for
  `voter_replace_s` (10 min), the Raft leader replaces it with a healthy server: added as a Raft learner, promoted
  when caught up, then the old voter removed (the core's `raft_change`). So the group survives losses one after
  another, not just two at once.
- Every other server holds a **watch stream** from a voter: the applied entries in index order, from a given index,
  or a snapshot first if that index is compacted. A server's copy is therefore at most one message behind; it answers
  TOPOLOGY from it. Staleness is safe because every request a volume leader receives carries the epoch, and every
  replica checks epochs (§6.3).
- **Heartbeats** go from every server to the Raft leader every 500 ms (in memory, not entries): node id, incarnation,
  the volumes it leads with their epochs, and a summary of what it cannot reach. The answer carries the lease (§7.1).
- **Failure detection** is the Raft leader's job (§6.1). A new Raft leader first waits one lease length before it
  declares anyone down, since it does not know when leases were last granted.

### 2.3 Using the user's Raft core

The core (intellistream-objects `src/raft`, read at c9c938e9) is a fit:

- **Sans-IO.** `raft_step`, `raft_tick`, `raft_propose` go in; `raft_next_action` hands out what to do, in a safe
  order: save the hard state, sync the log, send, apply, restore a snapshot. tsdbd's peer event loop drives it; the
  log syncs run on a worker, as isod does (`raft_log_sync_prepare` / `raft_log_sync_run`), so heartbeats and the
  watch never wait for a disk; a leader whose own sync is outstanding for 2 s hands leadership on. In shared mode
  (design-ha-modes.md M11) the log is on a local disk only, never NFS, and a log fault ends only the voter role.
- **The log store** (`raft_log.c`): 64 MiB segments in `<data>/meta/raft/`, checksummed records, a two-slot hard
  state, and the durable floor that tells a torn tail from rot in an acknowledged record. That matches tsdb's own rule
  "damage is never silent" (AGENTS.md).
- **What it offers that we use**: pre-vote and check-quorum (a flapping server does not depose the leader), learners
  (voter replacement), leader transfer (draining a voter), `raft_read_index` (a Raft-leader read confirmed by a
  quorum, used for the rare decisions that must see the latest state), snapshots (`raft_compact`, RESTORE).
- **What we do not use**: lease reads of the core (`lease_reads` off): metadata reads are served from the applied copy
  and fenced by epochs instead.
- **The simulator** (`raft_sim.c`) runs the real core against a fake disk, clock and network. tsdb's metadata state
  machine is plugged into it (§13.1).
- **Limits**: `RAFT_MEMBERS_MAX` is 16, which is why non-voters follow a watch stream rather than join as learners.

How to bring it in: copy `raft.[ch]`, `raft_log.[ch]`, `raft_wire.[ch]`, `raft_sim.[ch]` to `src/raft/` unchanged
(their own style, tabs, so a re-sync is a clean diff), with a small `src/raft/compat.h` for what they include from
isod: `common/util.h`, `common/rng.h`, `common/clock.h`, `common/crc32c.h`, `common/log.h` and `proto/frame.h`.
`tools/sync-raft.sh <commit>` re-copies and records the commit in `src/raft/UPSTREAM`. The intellistream-objects
repository has no LICENSE file; its owner granted copying these files into tsdb under Apache 2.0 (D2, 2026-11-06).
So: each copied file gets the Apache 2.0 header of tsdb's own files, `src/raft/UPSTREAM` names the source repository,
its commit and the grant, [licenses.md](licenses.md) lists the files under "Copied code and data", and
intellistream-objects should get a LICENSE (or a grant file naming Apache 2.0) of its own, so the grant is written
down at the source too.

## 3. Volumes, placement and routing

### 3.1 Volumes

- `tsdb cluster volume add NAME [--count N]` (or the VOLUME_ADD op, superuser) asks the Raft leader, which assigns
  the next free number (below 65,535, as series64 allows), a uuid and 3 replicas, and commits it. Each replica's
  server then makes `<data>/v<number>/` with META holding `volume=N`, `volume_uuid=`, `cluster=<uuid>` and
  `requires=cluster` (§11.1), and opens it as a replica.
- Volumes are still added only by an operator (AGENTS.md "Volumes"). The default at `cluster init` is 16 volumes per
  server.
- There is no root directory in a cluster. VOLUMES, the root SERIES (volume 0) and USERS are replaced by the
  metadata group and by name slots.

### 3.2 Placement

Rules, in order:
1. The replicas of a volume are on 3 different servers; on 3 different racks when racks are configured and there are
   at least 3.
2. Each server leads about V/N volumes (the preferred leader is the first replica in the list).
3. Each server holds about 3V/N replicas, spread over its disks by free space.
4. Each pair of servers shares about the same number of volumes, so the volumes of a lost server are rebuilt from, and
   onto, all the others rather than a few.

The Raft leader computes changes with a deterministic greedy pass over these rules and carries each out as a **move**:
add a learner on the target, let it catch up (§8), add it to the ISR, then drop the old replica from the list. A
move never lowers the number of in-sync copies. Moves happen when a server is added (`tsdb cluster node add`), drained
(`node drain`), or declared lost (`node out`, or down for `out_after_s`, 30 min by default).

**Leader balance.** When a server comes back and its replicas are in sync again, leadership moves back to the preferred
leader with a planned transfer: the old leader stops admitting writes to that volume, waits until the target's log end
equals its own, and the Raft leader commits the new leader and epoch. Writes pause for a few milliseconds and nothing
is cut.

### 3.3 Routing and names

- **By id**: the volume number is `TSDB_SID_VOLUME(id)`; TOPOLOGY says who leads it. Nothing else is needed.
- **By name**: names are unique in the whole database (series64 decision of 2026-11-02). A name's **slot** is
  `xxh64(name) mod 65536`; the slot's **home volume** comes from the metadata.
  - **Create without a volume** (the Pulsar sink, most clients): the series is created in its home volume. This also
    spreads series over volumes without the client choosing.
  - **Create in a named volume V**: the request still goes to the home volume's leader, which holds the slot's
    creation lock, checks its slot index, creates the series in V through V's leader, and records the name with V's
    id in the slot index.
  - **Slot index**: each volume keeps, for the slots it is home of, `name → id` for every series of those slots,
    wherever it lives, as a dictionary like SERIES (markers in its own volume log, §5.5). For series that live in their
    home volume it holds no extra copy of the name: SERIES is the index.
  - **Lookup by name**: one request to the home volume's leader; it answers from SERIES or the slot index, and asks the
    earlier homes of a slot only if the slot moved.
  - **Slot moves**: when volumes are added, the Raft leader moves some slots to them so new series spread there too.
    During a move, creations in that slot pause for one metadata commit; existing series stay where they are, and the
    new home asks the old one for names it does not know.
- **Shared mode differs** (design-ha-modes.md M14, §5.7): its volumes sit on NFS pools that can roll back and be away
  for hours, so it keeps no slot index of other volumes' series. A series is created only in its slot's current home,
  a slot's earlier homes are a chain in the metadata, and a lookup asks them in turn. Everything below holds for
  replicated mode.
- A creation in V and its slot record are two commits in two volumes. The home leader records the name first (a
  reservation with `id = 0`), creates in V, then records the id; a crash in between leaves a reservation that the next
  creation of that name resolves by asking V. The name never exists in two volumes.

## 4. The volume log

### 4.1 What travels

The volume log is the leader's WAL, in WAL record version 2 of design-cdc.md:
`[u32 rows | kind << 28][u32 header check][u64 hash][u64 fno][i64 commit_ms][payload]`.

- **Rows** (kind 0): one group commit, as today. Its `fno` is the position of its first row; its rows take
  consecutive numbers.
- **Markers** (kind 1): everything else, each taking one number. The cluster adds these kinds to the change feed's
  markers:

| Marker | Payload | Applied as |
|---|---|---|
| `COMMIT` | op (insert, replace, delete), the pending part files (name, size, xxh64 of the file, header checksum), `F` (flush through segment F first), the keys (series, range) | parts_commit of those files (§5.2) |
| `CLASS` | series, T, class | append to CLASSES (§5.4) |
| `SERIES`, `STATE` | name, type, local id / state code | append to SERIES / STATES |
| `SLOT` | name, id (or a reservation) | append to the slot index |
| `RETENTION` | series, limit | append to RETENTION |
| `NOW` | the leader's retention clock | the replica's `eng_now_ms` (§5.4) |
| `DROP` | `drop_before` time and watermark (ha.md D33) | retire partitions, raise the watermark |
| `COLD` | partition key, object reference, the index file | §9 |
| `EPOCH` | the new epoch and its leader | first record of every epoch |

- **Segments**: the leader's segment names, `wal_<seq>.log`, with the epoch in `seq`'s high bits
  (`(epoch << 40) | n`, ha.md D4), so two leaders never write the same segment name. A replica writes the same bytes
  at the same offsets of the same segment. The record check, which binds a record to its segment and offset, holds
  unchanged.

### 4.2 The rule that makes it safe: durable first, applied at the commit point

Every replica, the leader included:
1. **Writes** a record to its WAL as it arrives (the leader: as it forms it in `wal_buffer`), and makes the files a
   `COMMIT` names durable.
2. **Syncs** with group commit, as today, and reports its log end: the leader to itself, a replica to the leader
   in an ACK.
3. **Applies** records only up to the commit point, in position order: rows become visible to queries, a memtable
   may be flushed only if all its records are committed, a `COMMIT` lists its parts, a dictionary is appended.

The leader computes the commit point as the lowest log end over the ISR and sends it with the next records or within
5 ms. A replica keeps the commit point it last heard in memory and writes it to its `REPL` file at most once a
second (the "commit floor", like the Raft log's durable floor), so that after a restart it applies at once what it
knows is committed and waits for the leader about the rest.

Consequences:
- **Parts never hold uncommitted rows.** Whatever a crash leaves above the commit point is in the WAL only (rows and
  markers) or in pending `tmp_*` files, and a returning replica cuts it there (§8.2).
- **Reads see only committed data**, on every replica. A leader's reply OK comes after the commit point passed the
  write and the leader applied it, so a client reads its own write on the leader.
- In sync mode today, a row is visible once the WAL is synced up to it. The cluster replaces "synced" by
  "committed": the same `visible` boundary in `mt_collect`, fed by the commit point instead of `wal_durable`.

### 4.3 The REPL file

One per replica directory, two checksummed 4 KiB slots written alternately (like the Raft hard state): the volume
uuid, the cluster uuid, this replica's node id and incarnation, the **epoch history** (epoch → first position, the
last 64 epochs), the commit floor, and the highest epoch this replica has seen (its fence, §6.3). Written with
`fdatasync` before the replica acts on a new epoch.

## 5. The write path

### 5.1 Small inserts (the WAL path)

On the leader, one group commit:
1. `wal_buffer` assigns positions under `buf_lock` (design-cdc.md §2.1, no new lock) and the writer role writes the
   record.
2. The replication sender takes the bytes from the same buffer and sends them to every ISR replica and learner on
   their log stream, **before** the leader's own `fdatasync`, which runs at the same time.
3. Each replica writes the bytes at the same offset of the same segment, group-commits them with its own
   `fdatasync`, and returns `ACK(log end)`.
4. When the lowest log end over the ISR passes the record, the leader advances the commit point, makes the rows
   visible and answers the clients whose rows it covered.

Latency: `max(leader fdatasync, one-way + replica fdatasync + one-way)`; on A4's hardware about 0.85 ms against
0.68 ms today. Throughput: group commit amortises both the local and the remote syncs, as it does today for
concurrent writers. The wire carries 20 bytes a row before zstd-1 (C25); time series compress 3 to 6 times on it.

`wal_sync_ms > 0` (periodic sync) is refused in a cluster, as ha.md D15 refuses it: an acknowledgement would come
before durability.

### 5.2 Large inserts, deletes and replaces

Today: the writer registers an `lw_fence` for its keys, flushes the memtable if it holds rows inside them, writes
pending parts, and commits them through an intent file under `publish_lock` (part.c:877). Small writes into its keys
wait at the fence until the publish (memtable.c:546-660), which keeps last-write-wins in real time.

In a cluster, on the leader:
1. `lw_enter` as today. If the memtable holds rows in the keys, rotate the segment, and record `F` = that segment.
2. Write the parts pending (`pw_finish_pending`) and fsync them, as today. A large insert's parts carry `wal_seq` 0
   (db.c:917, delete.c:118), so nothing in their bytes depends on the replica.
3. Send the files on the bulk link to every ISR replica; each writes `tmp_<same name>`, fsyncs it and the directory,
   and answers PREPARED. (Files of a few MB each; at 1.2 bytes a row a 10 M row insert is about 12 MB per replica.)
4. Append a `COMMIT` marker (names, sizes, checksums, `F`, keys). It is replicated like any record.
5. When the commit point passes the marker, every replica applies it: flush its memtables through segment `F` if it
   has not already (a replica's segments are the leader's, so this is the same set of rows), then `parts_commit` of
   the pending files under its own `publish_lock` and block numbers.
6. The leader applies it the same way, then `lw_leave`, then replies OK.

The fence is held one replication round longer than today (about 1 ms): only a small write into the same series and
time range of a running large write notices.

Why this keeps last-write-wins: on each replica, records are applied in position order and block numbers are
assigned at apply time. Rows before the marker in the same keys are flushed first (step 5, `F`), so they get lower
block numbers; rows after it in the same keys could not exist before the leader's step 6 (the fence held them), so
they come after the marker and rank above it. That is the same argument as on one server, applied per replica.

Deletes and replaces are the same: the tombstone parts (and a replace's rows) are written by the leader and shipped.

A replica that misses a file a `COMMIT` names (it was a learner at step 3, or it lost a `tmp_` file) fetches it from
the leader before it acknowledges the marker: "durable" for a `COMMIT` means the marker and every file it names. At
open, a cluster replica keeps `tmp_*` files that a marker above the commit floor names; others are removed as today.

### 5.3 Flushes and segments

Each replica flushes its memtables when it likes (`buffer_rows`, `flush_interval_ms`), with two limits: only
committed segments, and at the latest when a marker's `F` asks. Segment rotation is the leader's: a replica starts a
new segment when the leader's next record names one. A replica removes a segment only after its rows are in its parts
and every marker in it is applied durably, and keeps sealed segments for catch-up within the budget of §8.3.

### 5.4 Clocks and classes

- **Retention clock.** A replica never reads its own wall clock for retention. The leader appends a `NOW` marker
  every second (and before any `DROP`); a replica's engine runs with `opt.clock_ms` returning the last applied `NOW`
  (retention.c:176-190 already takes a clock hook and keeps it from going back). A new leader starts from
  `max(own clock, last NOW)`. A replica therefore clamps up to a second later than the leader: visible only to a read
  that asked for a replica.
- **Merges** drop expired rows with the replica's clock; queries clamp to the cutoff anyway, so the answer is the
  same.
- **Partition classes.** Only the leader decides switches (at its flushes and idle merges, as today). It takes `cls_rw`
  for writing, appends a `CLASS` marker, waits for the commit point (about 1 ms), appends to CLASSES and releases
  `cls_rw`. A replica, before it appends a `CLASS` record, checks the rule of AGENTS.md on its own state: T past every
  row of the series in its memtables and in WAL segments that may still replay (`db->wtail`). Its WAL is the leader's,
  but it may keep a segment longer than the leader did, and a deleted row in such a segment would replay under the
  new key. If the check fails, the replica first flushes and unlinks the segments in question and syncs the directory
  (`wal_removed_synced`), then appends. A replica never decides a switch itself (engine option `replica = 1`).

### 5.5 Dictionaries and creation

- **Series and states**: the leader assigns the local id in memory, appends a `SERIES` (or `STATE`) marker, and when
  it is committed every replica appends to SERIES with fdatasync, fail-stop as today. The client gets the id after
  the leader's append. An uncommitted creation can be cut on a returning ex-leader; that id was never handed out, and
  the new leader may give it to another name, so "ids are never reused" holds for every id a client has seen.
- **Retention limits, drops**: `RETENTION` and `DROP` markers, applied in order. ha.md's drop watermark (D33) carries
  over: rows below it are refused (`TSDB_ERR_DROPPED`), so a resend after a drop cannot bring a partition back.
- **Users**: in the metadata group (C22).

### 5.6 Merges

Each replica merges its own parts with the existing policy. The result is logically the same everywhere: the same
rows survive, because every replica applies the same records in the same order and merges only adjacent block ranges
in which the newest source wins. Physically the parts differ, and so does the order in which float sums are added
across granules: two replicas may differ in the last bits of a float sum, as one server does before and after a
merge. Integer sums, counts, minima, maxima and raw rows are identical.

Merge CPU is spent on every replica (three times for the cluster). That is the price of no merge traffic on the
network (C9); the measurement plan (§10.3) checks it.

### 5.7 Fail-stop

A failed WAL write or sync on any replica is fail-stop for that replica (`eng_fail`), as today: it stops acknowledging,
the leader removes it from the ISR, and it is rebuilt or reopened. A fail-stop on the leader stops its acknowledgements
at once; the Raft leader sees the volume reported failed in the next heartbeat and fails it over at once (no 3 s wait).

## 6. Failover and fencing

### 6.1 Detection

- **A server**: the Raft leader declares it down after `node_timeout` (3 s) without a heartbeat, and after the
  server's read leases have run out (§7.1). It commits one entry: the member's state and, for every volume the server
  led, a new leader and epoch (§6.2); for every volume where it was in the ISR, a smaller ISR.
- **A replica, seen by its leader**: a replica that has not acknowledged a record within `isr_lag_ms` (1 s) of its
  sending, or whose log stream broke, is removed from the ISR (§6.3). Writes to that volume wait at most that long.
- **A leader that cannot reach its replicas but reaches the Raft leader**: before shrinking the ISR below 2 while the
  Raft leader still hears the replicas' heartbeats, the Raft leader moves leadership to a replica instead (ha.md D38's
  reasoning: the cut-off one should step aside, not the others).
- A tsdbd that hangs but keeps its sockets open stops heartbeating (heartbeats come from the event loop, which also
  serves the volumes), so it is treated as down. A slow disk shows as a replica falling behind: ISR removal, not
  failover.

### 6.2 Promotion

For a volume whose leader is down:
1. The Raft leader sends `FENCE(volume, epoch e+1)` to every ISR member it can reach. Each writes e+1 as its fence in
   REPL (fdatasync), refuses every record of epoch e from then on, and answers its log end.
2. It picks the member with the highest log end (ties: the preferred leader) and commits `(leader, epoch e+1, ISR =
   the members that answered)` to Raft.
3. The new leader writes an `EPOCH` marker as its first record (a new segment `(e+1) << 40 | 0`), keeps every record
   it has (the old epoch's tail included, as Raft does), and sends it all to the other ISR members. When they have it,
   the commit point passes the old tail and the tail is committed.
4. Replicas whose log has records the new leader does not have cut them (§8.2).

If no ISR member answers (all down), the volume waits: UNAVAILABLE(NO_OWNER) to clients, an alert, and a promotion
as soon as an ISR member returns. An operator can force an unclean promotion of a non-ISR replica
(`tsdb cluster volume force-leader`), which may lose acknowledged writes and says so.

Time to writes again: 3 s detection + about 10 ms for fencing, a Raft commit and the first record + the clients'
TOPOLOGY refresh. About 4 s in total for the volumes the server led. Other volumes see only the ISR shrink (§6.1,
under 1 s of waiting).

### 6.3 Why a deposed leader never acknowledges

Claim: a write is acknowledged only if every member of the volume's ISR, as Raft records it when the leader computes
the commit point, has it durable; and every new leader is chosen from that ISR.

- A leader L of epoch e acknowledges a record only when every ISR member it knows has acknowledged it, or after
  Raft committed a smaller ISR. Raft accepts an ISR change only if it names the volume's current epoch and ISR version.
- After the Raft leader decided epoch e+1, (a) any ISR change L asks for names e and is refused, and (b) the new
  leader N was in L's ISR, so L needs N's acknowledgement. N stops accepting epoch e records the moment it is fenced
  (step 1), before its log end is taken as the start of e+1. Whatever N acknowledged to L is in N's log, and N keeps
  it. Whatever it did not, L cannot acknowledge.
- So every write L acknowledged is on N. L learns of e+1 from any replica's refusal (`FENCED(e+1, leader)`), from the
  metadata watch, or from its next heartbeat; it then answers MOVED. None of this uses a clock.
- The same argument covers a leader that pauses (SIGSTOP, a VM pause) and resumes: its old-epoch records are refused.

### 6.4 Clients and the Pulsar sink

Unchanged from ha.md §9 item by item (C17): a node list; HELLO; TOPOLOGY (now from the metadata copy: per volume the
leader's address, the epoch and the replicas); direct mode; one stream per `(session, volume)` with `seq` and
`attempt`; on MOVED, UNAVAILABLE, an I/O error or a timeout, refresh TOPOLOGY and resend the stream in order from the
first unacknowledged frame, frames acknowledged after it included, with attempt + 1; retry by a time budget (30 s;
UNAVAILABLE does not count); `TCP_USER_TIMEOUT` of about 3 s. Forwarding by any server in simple mode (ha.md §9.1),
one hop, never a transparent retry.

The Pulsar sink inherits this through the Java client. Its `ErrorClassifier` already retries unknown codes; MOVED,
UNAVAILABLE and IO become explicitly retryable, DROPPED stays final. It acknowledges a Pulsar message only after tsdb's
OK, so a failover costs it a pause, never a message.

### 6.5 Requests in flight during a failover

| Where the request was | What the client sees | What the data says |
|---|---|---|
| Not yet admitted by the old leader | MOVED or UNAVAILABLE: definitely not applied | nothing |
| Written by the old leader, not on every ISR member | IO error or a timeout: indeterminate | the new leader may have it (kept and committed in e+1) or not (cut) |
| Committed, the reply lost | IO error or a timeout: indeterminate | applied |
| Acknowledged | OK | on the new leader (§6.3) |

The client resends every indeterminate frame (ordered at-least-once). A kept old-epoch copy is always before every
record of the new epoch, so a resend, or a newer write to the same key, ranks above it: last-write-wins gives the
client's final value.

### 6.6 Two servers down, end to end

8 servers, 128 volumes, replication 3, servers A and B lose power at the same moment.

- Volumes with no replica on A or B: 20 of 56 replica sets, about 46 volumes. Nothing happens.
- Volumes whose leader was on A or B: 32. Their ISR members are fenced; after about 4 s each has a new leader on one
  of the 6 others.
- Volumes with A or B as a follower only: their leaders shrink the ISR after at most 1 s.
- Volumes with both A and B among their replicas: 6 of 56, about 14 volumes. Their ISR is the one survivor, which
  takes their writes alone (minimum ISR 1). The placement gives each of them a new replica first (§8.4), so the window
  with a single copy is minutes.
- If A and B are both metadata voters, 3 of 5 voters remain: a quorum. A third voter lost before replacement
  (`voter_replace_s`) would stop failovers and ISR changes, but not writes to volumes whose ISR stays reachable.
- Load: the 6 servers now lead 128/6 ≈ 21 volumes each instead of 16: 33% more work each, which the sizing rule
  (§10.2) provisions for.
- After `out_after_s` (30 min) A and B are declared out and their replicas are rebuilt on the 6 (§8). If A comes back
  earlier, it catches up from the retained WAL instead (§8.2).

### 6.7 Failure modes

| Failure | What happens | Writes pause | Why it is safe |
|---|---|---|---|
| tsdbd killed on a leader | Heartbeats stop; failover after 3 s | about 4 s on its volumes | §6.3 |
| Server power loss | Same; its WAL tail beyond the commit point is cut when it returns | about 4 s | §6.3, §8.2 |
| Follower replica killed or slow | ISR shrink after 1 s | up to 1 s on its volumes | the ISR change is a Raft commit at the current epoch |
| Leader partitioned from everyone | Failover after 3 s; its lease ends after 2 s, so it stops reads first; its writes are refused by fenced replicas | about 4 s | §6.3, §7.1 |
| Leader partitioned from its replicas only | Leadership moves to a replica (§6.1) | about 1 s | the move is a Raft commit and a fence |
| Replica partitioned from its leader only | ISR shrink; it rejoins when the partition heals | up to 1 s | §8.2 |
| Raft leader lost | The voters elect a new one (election timeout 1-2 s); it waits one lease before declaring anyone down | none for writes; failovers delayed by about 3 s | Raft |
| Three metadata voters lost | No failovers, no ISR changes; writes go on while ISRs answer | only for volumes that need an ISR change | nothing can be decided without a quorum |
| A paused leader resumes | Its records of the old epoch are refused | none | §6.3 |
| Clock jumps or drifts beyond A5 | A deposed leader may serve reads past its lease | none | writes do not use clocks (C13) |
| Disk full on a replica | Fail-stop for that replica (ENOSPC on the WAL is fail-stop); ISR shrink | up to 1 s | §5.7 |
| Damage found in a part at read | `TSDB_ERR_CORRUPT` as today; that replica's copy is rebuilt from another (§8.5) | none | damage is never silent |

## 7. Reads

### 7.1 On the leader (the default)

A leader serves reads while it holds a **lease**: each heartbeat answer from the Raft leader extends it to
`send time + lease_ms × (1 − drift)` on the leader's monotonic clock (`lease_ms` 2 s, drift from A5). The Raft leader
does not promote another leader for the volume before `lease_ms × (1 + drift)` after the last answer it gave, and
`node_timeout` (3 s) is longer than the lease anyway. A leader whose lease ran out answers reads with UNAVAILABLE and
retry, and the client refreshes TOPOLOGY.

Leader reads see every acknowledged write (C7): read-your-writes, and monotonic reads within a session as long as
the leader does not change. After a failover the new leader has every acknowledged write, so it stays so.

### 7.2 On replicas (on request)

A query flag `REPLICA_OK` lets any ISR member answer from its committed state, which trails the leader by one message
(about 1 ms under load, 5 ms idle). For read-your-writes, insert replies carry the position `(epoch, fno)` of the
write (a body that 1.x adds for clients that ask, §11.2); a query may carry `min_pos`; a replica that has not applied
that far waits up to 50 ms, then answers MOVED to the leader. Use: dashboards and heavy scans spread over all three
copies; the Pulsar sink and anything that reads after writing stay on leaders.

### 7.3 Many series, many servers

- **QUERY_MULTI** (direct mode): the client splits the series by the leader of their volume, sends one QUERY_MULTI per
  server with the original indexes, and merges the reply frames by index (each frame already carries its index,
  `TSDB_MULTI_PREFIX`). The `max_rows` budget is split by series count and each part's END status is combined (the
  worst one wins). In simple mode the entry server does the same and relays; CANCEL is relayed to every part.
- **Snapshots**: each volume answers from its own cut, taken when its part of the request starts. A cluster has no
  instant common to all volumes; today's "one cut of every volume" holds per volume. Documented.
- **LAST_STATS** after a fanned-out request is the sum of the parts' statistics (the client sums in direct mode, the
  entry server in simple mode).
- **Database-wide operations** (STATS, FLUSH, OPTIMIZE, `drop_before`, `tsdb_series_foreach`) fan out per volume to
  leaders and answer PARTIAL (ha.md §15.4) when some volume is unavailable.

## 8. Rebuild and catch-up

### 8.1 A new replica (learner)

1. **Snapshot** on a source: any ISR member, chosen to spread load (not only the leader). Under `publish_lock` it
   makes `snap_<id>/` in the replica directory with hard links to every listed part, copies of SERIES, STATES,
   CLASSES, RETENTION, the slot index and REPL, and the list of WAL segments not yet in parts with their committed
   lengths, all as of its applied position P. Hard links keep the files through merges and retirements.
2. **Transfer** on the bulk link, throttled by a token bucket per server for sending and for receiving
   (`rebuild_mb_s`, 200 MB/s by default; 2 rebuilds at once per server), checked with xxh64 per file and each part's
   own checksums.
3. **Open** the copy as a replica at P (META with this replica's identity, REPL with the epoch history up to P).
4. **Catch up** from the leader: the log from P+1, from retained segments (§8.3). If the leader no longer has P+1, take
   a new snapshot: only parts not yet copied travel (by name and checksum).
5. **Join**: when the learner's log end has reached the commit point and stays within `isr_lag_ms`, the leader asks
   Raft to add it to the ISR (conditional on epoch and ISR version, like a removal).

### 8.2 A returning replica

A replica back after an outage or a restart:
1. Opens its volume in replica mode: applies what is below its commit floor, keeps the rest in the WAL unapplied.
2. Sends the leader its last epoch and log end. The leader answers with the end of that epoch in its own history
   (KIP-101). The replica cuts its WAL after that position (an `ftruncate` at the record's offset; later segments
   deleted), deletes pending files that only cut markers named, and syncs. Nothing listed is ever cut (C7).
3. Catches up as a learner from there (§8.1 step 4), and rejoins the ISR.

A former leader goes through the same steps; its unacknowledged tail is what gets cut.

### 8.3 Retained segments

The leader keeps sealed WAL segments of each volume after they are in parts, for catch-up: up to
`repl_retain_mb` per server (4 GB by default, split over its volumes by write rate) and at most 30 min. The change feed
of design-cdc.md retains segments the same way (`fwal_` names); one mechanism serves both. A replica away for longer
than that, or than the budget, gets a snapshot.

### 8.4 Priorities

Rebuilds run in this order: volumes whose ISR has 1 member, then volumes with 2, then moves for balance. A rebuild
yields to foreground work: the token bucket is halved while the server's query p99 or WAL sync p99 is above twice its
1-hour median.

Time: a server's hot replicas are rebuilt on all the others at once. With 2 TB of hot data per server, 6 survivors,
200 MB/s each: 2 TB / 6 / 200 MB/s ≈ 28 min.

### 8.5 Damage, disks

- A damaged part found by a query or a scrub (`verify_checksums`, or the scrub of §10.3) makes that replica
  rebuild the partition: the source sends its parts of that partition key as of a position, and the replica swaps
  them in under `publish_lock` (the cold move of §9 uses the same swap).
- A replaced disk: the replicas on it are learners again and are rebuilt (§8.1), on the same server or elsewhere.

## 9. The boundary to a cold tier

This section is all the hot tier needs; the cold tier itself (erasure coding, placement of its pieces, reconstruction,
repair, scrubbing) is a separate design.

### 9.1 When a partition is cold-eligible

A partition key of a volume is **cold-eligible** when, on its leader:
- its newest row is older than `cold_after` (per volume, for example 30 days; off by default),
- it is settled: one part, no merge pending,
- it is idle: no new rows or tombstones for `cold_idle_s` (1 day by default),
- and the memtables hold no rows of it.

The whole partition moves, never part of one.

### 9.2 The interface

```c
typedef struct tsdb_cold_ops {
    /* store len bytes from fd under key; returns only when the object is durable at the tier's own redundancy */
    int (*put)(void *ctx, const char *key, int fd, uint64_t len, uint64_t xxh64);
    /* read [off, off + len) of an object */
    int (*read)(void *ctx, const char *key, uint64_t off, uint64_t len, void *buf);
    int (*del)(void *ctx, const char *key);
    /* keys under a prefix, after `after`, at most max */
    int (*list)(void *ctx, const char *prefix, const char *after, char (*keys)[256], size_t max, size_t *n);
} tsdb_cold_ops;
```

Keys: `tsdb/<cluster uuid>/<volume uuid>/p<key>_<xxh64>.part`. The object is the part file unchanged, so the offsets in
its granule index are offsets in the object. Errors from the tier are `TSDB_ERR_IO` or UNAVAILABLE, never CORRUPT
unless a checksum says so.

### 9.3 The move

1. The leader `put`s its part. It continues only after `put` returned.
2. It appends a `COLD` marker: the key, the object key, its size and xxh64, and the part's **index file**: header,
   granule index and footer (about 32 bytes a granule: a 1 GB part's index is a few hundred KB).
3. At the commit point, every replica applies it under `publish_lock`: it writes the index file as
   `p<key>_<min>_<max>_<lvl>.cold` (block numbers covering every part of that key it replaces), lists it, and renames
   its own parts of that key to `retired_*`. They are deleted after `obsolete_part_lifetime_s`, as merges' are.
   Before the move it checks that every part it replaces holds only positions below the marker (the part header's
   position range, §11.1); the leader's checks of §9.1 make that so.
4. A `put` that fails, or a leader that changes before the marker, leaves an orphan object: the next leader `list`s the
   volume's prefix and deletes objects no committed `COLD` marker names.

### 9.4 What the hot tier needs from any cold tier

- **Durability before the switch**: `put` returns only when the object survives what the cluster promises (2 servers
  lost). The hot copies are retired only after the marker is committed, and deleted only after
  `obsolete_part_lifetime_s`.
- **Range reads** of a few KB to a few MB at any offset, with latency the operator can live with for cold queries.
- **Statistics-only queries never touch it**: the index file is local on every replica, mapped like a part's index,
  so a query answered from granule statistics costs what it costs today. A query that must decode a granule reads that
  granule's byte range, and checks its checksum always (cold reads do not depend on `verify_checksums`).
- **After a move**: late rows for a cold partition go to the memtable and new hot parts with higher block numbers, which
  rank above the cold part; a delete over it writes tombstones in hot parts. Merges never touch a `.cold` part. A
  retention drop of the whole partition deletes the object after the retirement delay.
- **Rebuild copies index files only**, never cold objects.

## 10. Performance

### 10.1 Targets and how they are met

| | Target | How |
|---|---|---|
| Small insert latency, 1 writer | p50 ≤ 0.9 ms, p99 ≤ 2 ms (0.68 / 0.79 ms on one server today) | replicas write in parallel with the leader (§5.1) |
| Small insert throughput, many writers | ≥ 90% of a single server's per volume | group commit on every replica; zstd-1 on the log stream |
| Large insert throughput, per leader | ≥ 80% of a single server | files encoded once, shipped compressed (C8) |
| Cluster ingest capacity | about N/3 servers' worth for the WAL path (every row is synced on 3), more for large inserts | |
| Queries | unchanged on the leader; one more hop only in simple mode | reads need no replication |
| Failover | writes on a dead leader's volumes back within 5 s; other volumes under 1 s of pause | §6 |
| Two of 8 down | ≥ 75% of the cluster's capacity, with the sizing rule | §10.2 |
| Rebuild | 2 TB per lost server in about 30 min, foreground p99 within 20% | §8.4 |

### 10.2 Sizing rule

With N servers that must survive f failures at full speed, run each server at no more than (N − f) / N of its
capacity in CPU, disk I/O and network: 75% for 8 servers and f = 2, 60% for 5, 87.5% for 16. Disk space: at most
(N − f) / N full, minus the rebuild headroom of one server's share. Below these, two servers down cost no
performance; above them, the cluster stays correct and slows down.

### 10.3 Measurements to plan (a quiet machine, several runs, the mean)

- Replication round trip plus remote `fdatasync`, p50/p99, against a local `fdatasync` alone.
- `build/insert-bench` against a 3-server cluster: rows per call 1 to 1 billion, 1-64 threads, beside the single
  server's table in the README.
- `build/tsdbc-bench` loading and querying through a cluster, direct and simple mode.
- Merge CPU per replica under the 100 M row load (C9's price), and the network bytes per ingested row.
- Failover time: a histogram over 100 leader kills.
- Rebuild rate against foreground p99, with and without the yield rule.
- WAL fsyncs per second per disk with 48 replicas per server (one WAL per replica).
- A scrub that compares replicas logically (rows per series and partition, xxh64 of the sorted rows) runs weekly and
  reports differences: replicas must agree.

## 11. Format and protocol: what 1.0 needs

### 11.1 On disk

| Change | In 1.0? | Why |
|---|---|---|
| WAL record version 2 with `fno` and `commit_ms` (design-cdc.md §2.1), format 9 | **yes** | The record is the replication unit. Changing it after the freeze would be a break. Shared with the change feed. |
| Marker records (kind 1) in the WAL; replay skips kinds it does not need | **yes** (the kind field; the cluster's kinds come later) | A 1.0 replay must not choke on a kind it does not know in a cluster volume it should not open anyway; a single server writes only rows (and feed markers). |
| A position range `fno_lo, fno_hi` in the part header, covered by its checksum (0 when unknown; a merge takes the min and max) | **yes** | The cold move (§9.3), partial rebuild and the change feed need to know what a part holds. |
| META `requires=<feature>,...`: an open refuses a feature it does not know | **yes** | A 1.0 server must refuse a cluster volume (and any later feature) instead of opening and writing it. |
| `wal_seq` with an epoch in the high bits | no | Segment numbers are 64-bit already; a single server's epoch is 0. |
| REPL, the slot index, `.cold` index files | no | New files in cluster volumes only, behind `requires=cluster`. |

### 11.2 On the wire

| Change | In 1.0? | Why |
|---|---|---|
| Status codes -10 MOVED and -11 UNAVAILABLE defined, with ha.md's bodies; every 1.0 client retries them and refreshes its node | **yes** | A 1.0 client must survive a failover of a 1.x cluster (in simple mode, at least once). |
| A client connect string with several nodes | yes (cheap) | The same client code then works against a cluster. |
| The 32-byte INSERT_VALUES header with `vol`, `epoch`, `attempt`, `seq` (ha.md §9) | no: 1.x, by HELLO version | A server can accept both headers by version. Without it a 1.0 client gets at-least-once without the stream ordering rule. |
| TOPOLOGY, the insert reply position, `min_pos` on queries, `REPLICA_OK` | no: 1.x | New ops and longer forms, gated on HELLO, as earlier versions did. |
| Peer-link protocol | no | Between servers of one version only (a rolling upgrade is a later design). |

## 12. What stays single-server

- The embedded library (`libtsdb`, the Rust, Python and Java embedded bindings): unchanged.
- `tsdbd` without `--cluster`: unchanged, and refuses a volume whose META requires `cluster`.
- NFS followers (`--follow`): unchanged. In a cluster, replicas take that role.
- Export and import: `tsdb export` against a cluster reads through a client (by name), not from directories.
- The CLI: `tsdb cluster ...` commands (init, node add/drain/out, volume add/move/force-leader, status) talk to any
  server.

## 13. Testing

### 13.1 Simulators

- **The metadata state machine** under the Raft core's simulator (`raft_sim`): seeded runs with crashes, partitions and
  clock drift, checking after every event: one leader per volume per epoch, epochs only rise, a new leader was in the
  ISR, the ISR only changes at the current epoch.
- **The replication core** (`src/repl.c`, sans-IO like the Raft core: records, ACKs, fences, commit points and ISR
  changes go in; sends, syncs, applies and cuts come out) under its own simulator: a fake disk that loses what was not
  synced on a crash, a network that delays, drops, duplicates, reorders and partitions, and a fake metadata service.
  Checks after every event: every acknowledged position is durable on every ISR member; no replica applies past the
  commit point; replicas' applied logs are prefixes of one another; a cut never removes an acknowledged position.
  Thousands of seeds in `make test`, millions in a soak.

### 13.2 Jepsen, extended (`make jepsen-cluster`)

The harness starts 5 tsdbd servers on one host, each with its own directory and ports (`--port 0 --port-file`, as
AGENTS.md requires), peer links through a small fault proxy in the harness (drop, delay, partition per pair; no root,
no containers needed). Workloads `kv` and `classes`, as today, through clients in direct and in simple mode, plus a
`multi` workload of QUERY_MULTI across volumes.

| Nemesis | What it does |
|---|---|
| `kill` | kill -9 a random server, a leader more often than not |
| `kill2` | two servers at once, restart them after 5-60 s |
| `power` | crashfs power loss on one or two servers (unsynced writes lost, torn tails) |
| `partition` | random splits; a leader cut off from its replicas only; a leader cut off from the Raft leader only; a bridge |
| `pause` | SIGSTOP a leader past its lease and the node timeout, then SIGCONT |
| `slow` | one server's fdatasync delayed by 0.1-2 s (crashfs), a slow disk |
| `skew` | a server's monotonic clock runs fast or slow within the configured drift, and beyond it (only stale reads may appear) |
| `midcommit` | crash points: after the leader's WAL write and before sending; after replicas acked and before the client's OK; between PREPARED and the `COMMIT` marker; during a class switch; between `put` and the `COLD` marker |
| `rebuild` | declare a server out under load, and add a new one |
| `eio`, `rot`, `misdirect` | as today, on one server at a time |

Checks: the existing checks 1-4 over the whole cluster, plus: no acknowledged write lost across any failover; reads
on leaders monotonic per session; `min_pos` reads see the write; at the end, every replica of every volume holds the
same rows (the logical scrub of §10.3), and `tsdb_optimize` per replica leaves the same row count.

### 13.3 Others

- `make integration-tests` gains a `cluster` matrix entry: the four clients and the Pulsar sink against 3 servers with
  a leader killed mid-run (resends, no message lost, no duplicate value).
- `connectors/pulsar/test/integration.sh` with a tsdb cluster and server kills.
- Unit tests per marker kind; the class-switch precondition on a replica that kept a segment longer (§5.4); `tmp_`
  files kept at open when a marker names them.

## 14. Build plan

Each step is a track (build, adversarial review, fixes) with the gates of AGENTS.md: `make test`, `make asan`,
`make tsan`, `make jepsen`; from step 4 on also `make jepsen-cluster`; client changes also `make test-clients` and
`make integration-tests`. Estimates are working weeks of one builder track including review.

| Step | Content | Gate beyond the usual | Weeks |
|---|---|---|---|
| 0 | **Before 1.0**: format 9 (WAL record v2 with `fno`, kinds; part position range; META `requires=`), shared with the change feed; MOVED and UNAVAILABLE in the four clients; node lists | jepsen (it changes the WAL) | 2 |
| 1 | Raft core copied in (Apache headers, `UPSTREAM`, `tools/sync-raft.sh`, an entry in licenses.md), compat shim, metadata state machine, watch stream, heartbeats, `tsdb cluster init / node add / status`; simulator runs | Raft simulator soak | 3 |
| 2 | `src/repl.c` sans-IO replication core and its simulator | replication simulator soak | 3 |
| 3 | Engine replica mode: apply up to the commit point, verbatim WAL writes, markers, pending files kept, replicated clock, class precondition, cut by position | jepsen (single server, regressions) | 4 |
| 4 | tsdbd peer links (log and bulk), replication, ISR changes, failover, fencing, leases, TOPOLOGY, forwarding | jepsen-cluster: kill, partition, pause, midcommit | 4 |
| 5 | Name slots, creation and lookup across volumes, users in metadata | jepsen-cluster with creations | 2 |
| 6 | Clients: direct mode, streams, the 32-byte header, positions, QUERY_MULTI split; Pulsar sink | test-clients, integration-tests cluster | 3 |
| 7 | Rebuild, catch-up, retained segments, moves, leader balance, throttling | jepsen-cluster: kill2, rebuild, slow | 3 |
| 8 | Cold boundary: `tsdb_cold_ops`, the move, `.cold` index files; a directory-backed tier for tests | jepsen-cluster: midcommit at the move | 2 |
| 9 | Measurements (§10.3), docs (README section, AGENTS.md invariants, an operator guide) | the numbers | 2 |

About 28 weeks for one track; steps 1-2 and 6 can run in parallel with 3-4, which brings it to about 5 months.

**With two HA modes (2026-11-06, D3).** The plan above is replicated mode's. The combined plan of both modes, with
estimates, is [design-ha-modes.md](design-ha-modes.md) §7; how this plan's steps map into it:

- **A deployment runs one mode** (`tsdb cluster init --mode`, design-ha-modes.md M7); there is no mixed step.
- **Shared by both modes** (built once): step 0 (S0, which also defines every wire body the HA work needs before the
  1.0 freeze, and the namespace-event payloads shared by the ledger and this design's markers, design-ha-modes.md
  §2.1), step 1 (S1, extended with shared volumes, §7.1 there), the part of step 4 that is peer links, TOPOLOGY and
  forwarding (S2; per-volume admission is built once, in ha.md's N1), step 5 (S4, name slots and users), step 6 (S3,
  clients and the Pulsar sink), step 8 (X1, the cold boundary) and step 9 (X3).
- **Replicated mode only**: step 2 (R1), step 3 (R2: the engine's replica mode, and the marker backend of the journal
  hook that N4 builds), the rest of step 4 (R3: replication, ISR, promotion, leases), step 7 (R4, rebuild).
- **Order: shared mode ships first**, because production runs on NFS today: the N0 probe on the production NFS
  servers, then the shared-mode engine and HA manager on the metadata group; replicated mode's R2-R4 after it. R1
  (step 2) continues in parallel: it is sans-IO and blocks nothing. The estimates there are in track-days, gate hours
  and external dependencies, calibrated on the first checkpoints of steps 1 and 2.
- **What steps 1 and 2 already give shared mode.** Step 1 (`cluster-s1`, 35e1456 and ede84e5: the Raft core,
  `server/meta.c` with members, volumes, leader, epoch, ISR, epoch history, name slots and users, epoch-conditional
  operations, snapshots, the step checker, the simulator, tsdbd's peer thread and the CLI) serves shared mode as it is:
  an owner change is `META_OP_VOLUME_LEADER`, with the candidates as the ISR. It must change before its formats settle
  (design-ha-modes.md §7.1): an extensible heartbeat body, heartbeats on a thread with no Raft log I/O and syncs on a
  worker (as §2.3 above already says), step-down on a stalled sync, the state directory on a local disk, a voter-role
  fault instead of `_exit` in a shared-mode deployment, and the shared-volume operations. Step 2 (`cluster-s2`,
  44b352b: the replication core) gives shared mode nothing it needs, only the method of a sans-IO core under a
  simulator, which the shared-mode HA manager copies.

## Later

- **The cold tier itself.** intellistream-objects (`isod`, at c9c938e9) is the user's distributed, erasure-coded
  object store: chunkstores on raw devices, a Raft-replicated metadata service, an S3-style gateway that Reed-Solomon
  codes objects across servers, and self-healing. The cold tier may simply be isod behind `tsdb_cold_ops` over its S3
  API, rather than erasure codes inside tsdb; the separate design should weigh that first (range GETs, `put` durability
  at the pool's redundancy, latency of cold reads, one more system to run against code tsdb would own).
- Shipping merged parts instead of merging on every replica, if the merge CPU measured in §10.3 hurts and the network
  has room (100 GbE).
- Rebuilding a returning replica partially, from the part position ranges, when its absence outlived the retained WAL.
- A rolling upgrade protocol for the peer links.
- A local cache of cold granules on replicas.
- Reads across volumes at one instant, if a user needs them.

## Open questions

None for this mode. The six questions of the first draft were answered on 2026-11-06 and are decisions D1-D6 at the
top of this document. The questions about the two modes together (voters and witnesses on nodes without data disks,
the order, the N0 window, placement of names in shared mode, the read barrier, escalation of a stuck release) are in
[design-ha-modes.md](design-ha-modes.md).

## Unverified assumptions

- U1: Replication adds about 0.15 ms to a small insert on 25 GbE (A4). To measure first (§10.3).
- U2: 48 WALs per server with group commit do not saturate the NVMe's flush rate. To measure.
- U3: The Raft core builds with tsdb's flags (gnu11, the warnings tsdb uses) with only the compat shim.
- U4: Merge CPU on three replicas fits the sizing rule at the README's ingest rates.
- U5: Leases need the slew bound of A5; chrony's default `maxslewrate` (83,333 ppm) would make them long. The operator
  guide must set it.
