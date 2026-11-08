# tsdb HA modes: shared NFS storage and the shared-nothing cluster

Status: design, not implemented. Branch `ha-modes`, written on 2026-11-06 from main at ea89968, with the metadata
group of cluster step 1 (branch `cluster-s1`, 35e1456 and ede84e5) in view; revised the same day after two reviews
(fencing and control plane; sharing, cost and operations; what changed is listed under "Review of 2026-11-06" at the
end), and again after two verification passes (NFS fencing and durability; control plane and operations, read against
cluster-s1 at e0bba99; "Verification of 2026-11-06" at the end), and closed with the user's answers to its open
questions the same day (M20-M25; "Open questions" at the end says what was asked).
This document is the overview of tsdb's high availability. The two modes have their own designs:
[ha.md](ha.md) (shared NFS storage) and [design-cluster.md](design-cluster.md) (servers with their own disks). This one
says how they relate, what they share, which control plane runs both, and how a deposed writer is kept off shared
storage.

Decided by the user on 2026-11-06: "I have been thinking, I still want to keep the NFS implementation." This reverses
design-cluster.md D3 ("the cluster replaces ha.md for new work"): the NFS failover of ha.md stays, **as a second HA
mode beside the shared-nothing cluster**. Production today is AlmaLinux 10 nodes without local data disks, NFSv4 over
RDMA to several ZFS servers, with no failover between the NFS servers (an outage can last hours; Pulsar buffers the
writes meanwhile). Everything else in design-cluster.md (D1, D2, D4-D6, C1-C25) stands for the cluster mode.

## Summary

- **Two modes, one product.** *Shared mode* (ha.md): one copy of each volume on NFS/ZFS, one writer per volume, and
  failover moves the writer, not the data. *Replicated mode* (design-cluster.md): three copies on servers' own disks,
  a leader per volume, and failover promotes a replica. **A deployment runs one mode** (M7); moving from one to the
  other is an export and import by name into a new deployment.
- **One control plane for both** (M2): the Raft metadata group of cluster step 1 (`server/meta.c`) decides who owns
  each volume and at which epoch, in both modes. In shared mode it replaces ha.md's arbiter role of the NFS lock, its
  rank polling, `WANT` files, slot heartbeats as a liveness signal, `NODES`, the `OWNERS` gossip of owners and epochs,
  and D38's isolation heuristic. Its Raft logs live **only on local disks**, never on NFS (M11).
- **The fence stays on the storage** (M3). The metadata group cannot stop a deposed writer's `pwrite` on NFS; only the
  NFS server can. So shared mode keeps ha.md's NFSv4 lock on `LOCK`, chained fds (now also `CLASSES` and
  `RETENTION`), the ledger, epoch-named files and the abort rule (§6). The lock no longer decides who should own a
  volume; it only makes sure that whoever does not is refused by the server. Safety rests on the lock and the epochs
  on storage, never on the metadata group or a clock.
- **What a pool rollback can no longer undo**: epochs, series and state ids, and drop watermarks are committed in the
  metadata group *before* storage or a client relies on them (M12), and the metadata group lives off the pools. Names
  in shared mode are kept only in the volume where their series lives (M14), so a rollback can never give one name two
  series.
- **Shared by both**: the client protocol (MOVED, UNAVAILABLE, TOPOLOGY, streams with `seq` and `attempt`, one hop of
  forwarding, the drop watermark, ha.md §9's client rules), volume ownership by epoch, epoch-numbered segments
  (`(E << 40) | n`), the namespace-event payloads and the engine's journal hook (M15), peer links with the cluster
  secret, users and name slots in the metadata group, per-volume admission, the `tsdb cluster` CLI and the metrics.
- **Different**: durability (ZFS behind NFS against in-sync replicas), failover (a lock takeover of the same files
  against the promotion of a replica), rebuild (none against a replica rebuild), and what an NFS server outage does
  (its volumes wait, for hours if need be, against a cluster that loses nothing with two servers down).
- **An owner never resumes across a storage outage** (M17): when its storage went DOWN, its NFS server restarted or
  its files went stale, it gives the volume up and claims it again at an epoch reserved for it, by a cold open that
  re-reads everything from storage. That costs a few seconds per volume after an outage that lasted minutes or hours,
  and removes the class of bugs where an owner resumes with a picture of the files that a pool rollback has undone.
  The reserved epoch also lets an owner that restarts recover its own volumes while the metadata group is down (M18).
- **Order**: shared mode ships first, with the minimum production HA set of §7 (M9, M21, decided by the user on
  2026-11-06). Single-server outage handling (N1) starts now on a test NFS server; the go/no-go probe (N0) runs in the
  next maintenance window on one production NFS server and a test pool, and gates the shared-mode failover engine
  (N3-N7). If N0 finds no server-enforced fence, failover off a dead node waits for an operator's attestation that it
  is off, unless every node has a BMC the metadata leader reaches (M22, §7).
- **Decided with the user on 2026-11-06** (M20-M25): metadata voters on the OS disks of 3 (or 5) nodes in different
  racks and power feeds; names placed by hash over all volumes, prefix groups optional; the read barrier on by
  default (`--read-barrier off` to opt out); a stuck release kills tsdbd only after 3 × lease with another candidate
  reporting the domain UP.

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| M1 | **Two HA modes**: *shared* (ha.md) and *replicated* (design-cluster.md). The kind is a field of every volume in the metadata and a `requires=` feature in its META (`nfs-ha` or `cluster`); M7 makes it the same for all volumes of a deployment. | The cluster only (design-cluster.md D3, reversed by the user); NFS only | Production has no local data disks and runs on NFS; servers with their own disks are where new deployments go. Each mode is the cheaper answer for its hardware (design-cluster.md A1). |
| M2 | **One control plane: the metadata group** (Raft, `server/meta.c`) for both modes. In shared mode it names each volume's owner and epoch, schedules takeovers, and holds members, volumes, id reservations, drop watermarks, users and name slots. | (b) ha.md's own control plane (the NFS lock as arbiter, rank polling, slots, `NODES`, `OWNERS`) for shared mode; (c) both, (b) for NFS-only deployments | §5.2: the metadata group sees the whole cluster, so it tells a node fault from a storage fault and an isolated owner from a dead control plane, which ha.md answers with heuristics (hold-back timers, D38). Keeping (b) beside it doubles the code and the jepsen matrix that must stay green forever. |
| M3 | **The fence is the storage's, not the control plane's.** Shared mode keeps ha.md §2-§3: an NFSv4 OFD lock on `LOCK`, chained fds on every file an acknowledgement relies on (WAL segments, `SERIES`, `STATES`, `CLASSES`, `RETENTION`), the ledger as the only commit log for namespace effects, epoch-named files, the abort rule, grant and poison, the ack gate. | Epoch checks before each publish; epochs in names alone; Raft-conditional acknowledgements; a time lease from the metadata group; the ledger in Raft (§6.3) | A deposed writer can still write files on NFS. The only thing that refuses its writes is the server's lock state (ha.md L1, L3). Namespace operations carry no lock, so they are made harmless by names and fenced records instead. |
| M4 | **A claim needs both** a metadata grant `(volume, owner = this node, epoch E)` and the NFS lock, and writes E only if every epoch on storage is below E. A claimant also refuses a grant at or below the highest epoch it has itself seen for the volume (its local `volumes.cache`, ha.md D36). A refused claim reports `CLAIM_REFUSED(v, highest epoch seen)` and the leader grants above it. | Either alone; a refusal that waits for an operator | The grant makes one claimant at a time the rule and the epoch monotone off the pool; the lock and the check make two claimants with stale views harmless (§6.4, G2); the local cache is a belt that does not depend on Raft; the refusal report gives every refused claim a way forward (§5.5). |
| M5 | **The lock holder is the owner until it lets go.** A deposed owner learns of a higher epoch from the metadata watch, a peer, a MOVED, or the `CLAIM` file a refused claimant writes on the volume's own storage, and then releases cleanly. It never releases only because it lost the metadata group, and on `CLAIM` alone only while the claimant is still polling (its counter in `CLAIM` advances across two read-backs, §5.5). | Releasing on the loss of a metadata lease (D38's heuristic); no back channel; releasing on any `CLAIM` above its epoch (the first revision: a claimant that had given up left the volume ownerless) | If the metadata group is down, releasing every volume would turn a control-plane fault into a full outage. An owner cut off from everything but NFS still reads its own storage, so the storage carries the news. |
| M6 | **Storage faults are told apart by the metadata group.** A volume moves when its owner reports its storage domain DOWN while another candidate reports the same domain UP for `local_fault_s` (10 s). When every node reports it DOWN, or the domain is in MAINTENANCE, nothing moves (ha.md D22). The move completes by itself only when the fault also costs the owner its NFS lease to that server; a fault that wedges one domain while the lease lives (a stuck superblock, one stuck file) completes through the stuck-release rule (§5.5): kill, then ha.md §8's escalation. | Moving on any fault; never moving | A node-local fault (an RDMA port, a mount) is a reason to move; a server outage is not, because no other node can reach the storage either. The lease is per client per server while domains are per superblock, so the lock alone cannot free a wedged domain whose server is healthy. |
| M7 | **One mode per deployment** (`tsdb cluster init --mode shared\|replicated`). The kind field stays in the metadata (cheap, and the checker uses it); no node capabilities in placement. Migration is an export and import by name into a new deployment (docs/export-format.md). | Mixing per volume (the first draft) | *Changed in review.* Mixing was justified as a path from NFS to local disks ("new series go to replicated volumes while NFS volumes age out"), which does not work: names are unique in the whole database and permanent (no series delete or rename in tsdb.h, ids never reused, retention empties partitions, not `SERIES`), so an existing sensor's name cannot be created in a replicated volume. What mixing would cost stays forever: placement by capability, nodes with mounts and disks, slot homes crossing modes, a mixed jepsen matrix. |
| M8 | **Shared mode's metadata additions are small**: a volume kind, a storage record (path, export, fstype, server group), candidates in the replica list with the ISR equal to all candidates, id reservations and drop watermarks (`VOLUME_RESERVE`, `VOLUME_DROP`), grants above a refused claim, domain MAINTENANCE, and domain health in heartbeats (§5.4). The epoch-conditional operations of `meta.c` are reused unchanged. | A separate state machine for shared volumes | Every candidate of a shared volume sees the same storage, so "in sync" holds for all of them by construction; `META_OP_VOLUME_LEADER`'s rule (the new leader is in the ISR) then holds as it is. |
| M9 | **Shared mode first**: single-server outage handling (N1) at once, on a test NFS server; N0 (the probe on production NFS) gates the failover engine (N3-N7); then the metadata group, the shared protocol, name slots and the HA manager; replicated mode after it, its sans-IO core in parallel. If N0 finds no server-enforced fence, shared mode ships without automatic failover (§7, "If N0 fails"). | The cluster first, as design-cluster.md §14 planned; N0 gating N1 (the first revision) | Production runs on NFS today. Shared mode needs no new hardware, and its outage handling is useful without any failover. N1 needs nothing from the lock probe, only timeouts that start as ha.md §15.1's defaults. |
| M10 | **The ledger stays on the storage it describes.** | The ledger as Raft entries | A pool rollback (`zpool import -F`, a lost SLOG) reverts files and ledger together, which ha.md D27/D36 then detect. A ledger in Raft would name parts the storage no longer has, and put every commit, flush and merge of 1000 volumes through Raft. |
| M11 | **Raft logs only on local disks**: a node's state directory (`--state-dir`, default `/var/lib/tsdbd`) is refused on NFS. Only members that report `voter_ok` (state directory local, writable, free space above a floor) become voters; a `tsdbd --witness` host with a disk is one (`volumes_ok` false). In shared mode a fault of the Raft log ends only the node's voter role (it reports `core_dropped`, goes on as a watcher, writes a `retired` mark and may vote again only under a new identity), never the process; the leader replaces a voter by Raft progress and that flag, not by heartbeats heard. Heartbeats and the watch run on a thread that does no Raft log I/O, and a leader whose log sync stalls for 2 s hands leadership on. | A voter's log on NFS, one per NFS server (the first draft); fail-stop of the whole tsdbd on a Raft log fault | *Changed in review.* A pool rollback reverts a voter's log and `votedFor` together with the volumes, which breaks Raft and lets an epoch be reused (G2 relied on the log being off the pool). A hung sync on a hard mount freezes a leader for a whole outage. In shared mode the data does not live with the Raft log, so a log fault must not take a healthy data plane down. *Verification:* cluster-s1's `manage_voters` judges a voter by heartbeats heard, which a dropped core keeps sending, so a voter with a full disk was never replaced and could be picked again with its old files. |
| M12 | **Ids and drops are committed to the metadata group before anyone relies on them.** An owner hands out series and state ids only below a reservation committed with `VOLUME_RESERVE` (blocks of 64 to 4096); a `drop_before` that raises the watermark is committed with `VOLUME_DROP` before it is applied, and an owner applies every committed `VOLUME_DROP` of its epoch that its copy shows, also one whose proposal it saw time out (that client got IO, indeterminate, never UNAVAILABLE). A claim fills LOST entries up to the reservation and applies the committed watermark. | High-water marks in heartbeats, committed with ownership changes and at most once a minute (the first draft) | *Changed in review.* Heartbeats live in the Raft leader's memory: a site power loss that also costs a pool its SLOG would forget up to a minute of creations, and cached ids would then write into other series (the hazard of ha.md D36). A reservation costs one Raft commit per block of creations; drops are rare. |
| M13 | **A down node's shared volumes are granted after the NFS lease** (`lease_s`, 10 s, since it was last heard), not after the node timeout (3 s, which stays for replicated volumes). A node that comes back with a new incarnation claims its volumes again at their reserved epochs (M18); if it has not within 3 × `lease_s`, they are granted elsewhere (§5.5 reason 4). | 3 s for both kinds; granting a restarted node's volumes at once (the first revision, before M18) | *Changed in review.* A claimant cannot get the lock before the dead owner's NFS lease runs out (about 11.5 s) however early it is granted, so 3 s buys no speed and turns every 3 s stall of a live owner into a release and a cold open of each of its volumes. |
| M14 | **In shared mode a series lives in the volume that was its slot's home when it was created.** No volume records names of series in other volumes; a slot's earlier homes are a chain in the metadata group (§5.7). | design-cluster.md §3.3's slot index of other volumes' series, as in replicated mode | A slot index on one NFS pool can roll back and forget a name whose series lives elsewhere (a second series with that name), and is away while its server is down (by-name lookups of healthy series stop). With every name kept beside its series, a rollback loses the name together with the series, and the LOST id makes cached ids fail loudly instead of writing elsewhere. |
| M15 | **One set of namespace-event payloads, one engine journal hook, two backends** (the ledger region of `LOCK`; the replicated marker), defined in S0 (§2.1). | Two journals built twice | The ledger and the cluster's WAL markers record the same events (COMMIT, DROP, EPOCH); N4 and R2 would otherwise change the same call sites two ways and freeze two formats. |
| M16 | **An owner acknowledges and changes the namespace only after a read-back confirms its epoch** whenever its NFS client may have recovered (leaving SUSPECT, a transport reconnect, a fenced call longer than `lease/5`): the state RECHECK. The read-back also compares the tails W wrote last (ledger, `SERIES`, `STATES`, `CLASSES`, `RETENTION`, WAL); anything shorter or older is a rollback (M17). **The ack gate is a continuity check made by the write being acknowledged**: no ack when the write was issued `lease − margin` or more after the newest fenced write that had completed before it, or when it was in flight longer than `lease/5` itself. **Owner reads wait for a fenced write issued after they arrived** (the read barrier). | Returning to OWNER on the first successful probe; reads gated by the monotonic clock; a gate measured from the last successful write (which is the write being judged, after a pause) | *Changed in review and verification.* "LOST is sticky" proves nothing when the server wrongly grants a reclaim (U4 fails); then a deposed owner could ack into a segment the successor already flushed. The first gate measured from the write it judged, so the first write after a pause passed it, with the clock running. A frozen monotonic clock in a VM pause lets any clock gate pass, and stale reads were ruled out by the user (ha.md D23). |
| M17 | **An owner never resumes a volume across a storage outage.** When the volume's domain went DOWN, a fenced call was in flight longer than `down_ms`, the domain's transport reconnected while the volume was SUSPECT, any of its files answered ESTALE, or RECHECK found a tail older than W wrote, the volume goes FENCED: abandon (poison, no cleanup), then a fresh claim at the epoch reserved for this node (M18), a cold open that reads the slots, the ledger, every chained file and the WAL from storage, with rollback detection (§5.4). A stall that never reached DOWN, without a reconnect, still returns through RECHECK. | Resuming with the in-memory `lstate` and append offsets after revalidating the WAL, MANIFEST, parts and `SERIES` (ha.md §15.5 step 3, the first revision) | *New in verification.* A pool rollback can remove records W saw fdatasynced (a `DROP` held only in the SLOG) while W keeps its lock: nfsdcld lives on the server's OS disk, so W reclaims in grace. W then appended past the rolled-back end of the ledger, and replay, which stops at the first bad record, dropped every later acknowledged COMMIT at the next takeover; a lost `CLASSES` record made the next open CORRUPT. A cold open re-reads what is on storage, so no picture of the files survives the outage that made it wrong. The price, a cold open per volume (0.5-3 s, at most 4 per server group per node), was already paid for a missed grace, and is small after an outage. It also gives up ha.md §15.5 step 4's re-log of memtable rows from memory in shared mode: acknowledged rows a rollback removed from the WAL are reported (DIVERGED, with the WAL position) and replayed from Pulsar, as they already were whenever the owner had restarted. It also takes U4 out of the server-restart case: an owner that never resumes cannot ack on a reclaim the server should have refused. |
| M18 | **Every grant reserves the next epoch for its holder, and every member keeps its metadata copy on its local disk.** `VOLUME_LEADER(v, owner W, E)` also reserves E + 1 for W: any other node's next grant is E + 2. W may claim E + 1 on its own (M17, a restart, FENCED) without a new grant, under the lock and G2, and confirms it with `VOLUME_SELF(v, W, E + 1)` (conditional on E) once the metadata group answers. The copy (snapshot and applied index, in `--state-dir`) serves routing and logins after a restart, marked stale, while the group is away. | A fresh grant for every claim (the first revision); copies only in memory on non-voters (cluster-s1) | *New in verification.* With quorum lost, any tsdbd restart (the watchdog's SIGKILL, an OOM kill, an upgrade) dropped that node's locks, and its volumes then waited for quorum with nobody owning them; the restarted node had no users, no TOPOLOGY and no slot chains. E + 1 is reserved, so nobody else can ever write it, and G2 refuses it if anyone wrote E + 2. Creation and drops stay UNAVAILABLE until `VOLUME_SELF` commits (they need the metadata group anyway). |
| M19 | **A slot move is two-phase.** The old home's owner stops creating in the slot under its dictionary lock and commits `SLOT_FREEZE(s, A, E_A)`, conditional on its epoch; only then does the leader commit `SLOT_MOVE(s, B)`. A frozen slot answers creations with MOVED. Both modes. | design-cluster.md §3.3's pause of one metadata commit | *New in verification.* A pause works only for an old home that has seen the commit: a stale owner of A could finish creating N after B's owner had asked A and created N too. The freeze is committed by A's owner itself, so no creation in A follows it, and a later owner of A inherits it by log order, as with reservations. |
| M20 | **Metadata voters on the nodes' local OS disks** (user, 2026-11-06, open question 1: "yes, the production nodes have writable local OS disks"): 3 voters, or 5, on the OS disks of tsdb nodes in different racks and on different power feeds, in a state directory on its own small filesystem or under a quota (§5.6). Members record their rack and power feed; the leader picks voters across both. No `--witness` hosts are needed in production; the witness stays for deployments that lack such nodes. | Witness hosts; voters on the NFS/ZFS servers; voters on one rack or feed | The nodes have the disks, so no extra machines; distinct racks and feeds keep a single rack or feed fault from costing the quorum. M11's rules (never on NFS, never restored, voter-role faults) apply unchanged. |
| M21 | **Shared mode first, with the minimum production HA set of §7** (user, 2026-11-06, open question 2): S0, N0, N1, N2-N7, S1, S2, S4 and N8; direct-mode clients (S3) after it; replicated mode's R2-R4 after N7, with R1 going on in parallel. | The cluster first; shared mode without S2 or S4 | Confirms M9. Production runs on NFS; the minimum set is what §7 found the failover needs (users and TOPOLOGY in the metadata copy for `adopt`, name slots for creation). |
| M22 | **N0 runs in the next maintenance window**, on one production NFS server and a test pool (user, 2026-11-06, open question 3). **If it finds no server-enforced fence, fallback (i)**: a volume moves off a dead node only after an operator attests that the node is off (`volume move V NODE --dead OLD`), **unless every node has a BMC reachable by the metadata leader**, in which case (ii), a BMC power fence before each grant. The recommended default; the user may change it. The choice is a deployment setting (`failover = lease \| attested \| bmc`, in INIT and SETTINGS); `lease` is allowed only after N0 passed. | Waiting for a window of its own; one fallback for every deployment | One server and a pool that may be restarted and rolled back cover U1-U8, U13, U17, U19, U21 and O0 without touching production volumes. (i) needs no hardware and is safe whatever N0 finds; (ii) is automatic but only as reliable as the BMCs and the path to them, so it is chosen only where every node has one. |
| M23 | **Names are placed by hash over all volumes by default; prefix groups are an option** (user, 2026-11-06, open question 4). With a prefix group, a name prefix picks a group of volumes (for example those on one server group) and the hash picks a slot home within it; a group is set before the first series of its prefix exists, since a later change would move slots (§5.7). | Grouping by default; per-plant volumes chosen by clients | Hashing spreads load and creation evenly with nothing to configure; the price, that one NFS server outage takes a slice of every plant, is what an operator can buy back per prefix where it matters. |
| M24 | **The read barrier is on by default** (M16; open question 5, the recommendation taken as the default on 2026-11-06; the user may change it). Its cost is measured in N8 (V6). `--read-barrier off` (a node setting) falls back to the clock gate, for deployments that accept stale reads after a VM pause. | Off by default; no opt-out | Stale reads were ruled out (ha.md D23), and a frozen clock in a VM pause defeats any clock gate. The barrier's expected cost is one slot write and `fdatasync` on an idle volume, nothing under insert load. |
| M25 | **A stuck release may kill tsdbd and escalate to a reboot on its own**, only when one fenced call of the volume has been outstanding for 3 × `lease_s` (30 s) **and** another candidate reports the volume's domain UP (§5.5; open question 6, the recommendation taken as the default on 2026-11-06; the user may change it). A release that is only slow is shortened, never killed. This also answers ha.md open question 2 for shared mode. | Manual reboots only; killing on any slow release | A wedged process is the only holder of a lock whose lease lives; without the kill its volumes wait for an operator. Requiring another candidate's UP report keeps a server outage, which every node sees, from killing every owner. |

## 1. The modes and when to use which

| | **Shared mode** ([ha.md](ha.md)) | **Replicated mode** ([design-cluster.md](design-cluster.md)) |
|---|---|---|
| Hardware | Nodes without data disks (an OS disk is enough); volumes on NFSv4.1/4.2 (RDMA) exports of ZFS pools; metadata voters on the OS disks of 3 or 5 of the nodes, in different racks and power feeds (M20) | Servers with their own NVMe; nothing shared |
| Copies of the data | One, protected by the pool (raidz/draid, a mirrored SLOG) | Three (replication 3), on three servers |
| Who writes a volume | Its owner: the node holding the volume's NFS lock at the current epoch | Its leader, which replicates to the in-sync set |
| What survives | Any node fault, any number of nodes as long as one candidate is alive; not an NFS server outage (its volumes wait) | Two servers lost with their disks (min ISR 1) |
| Use it when | The storage is shared and already redundant; production today | New deployments on servers with disks; no shared storage |

A deployment runs one mode (M7, §4).

## 2. What the modes share

| Component | Where it is designed | Notes for shared mode |
|---|---|---|
| The metadata group (Raft, `server/meta.c`, watch stream, heartbeats) | design-cluster.md §2 | M2, M11, §5. Members, volumes, owners, epochs, reservations, watermarks, users, name slots. |
| Volume ownership by epoch; `(E << 40) \| n` block and segment numbers | ha.md D4, design-cluster.md §4, §6.2 | Epochs come from the metadata group in both modes. |
| Client protocol: HELLO, TOPOLOGY, direct mode by default, streams `(session, vol)` with `seq` and `attempt`, MOVED, UNAVAILABLE, PARTIAL and DROPPED with their bodies, resends by time budget | ha.md §9, §15.4, D13, D31, D39; design-cluster.md C17, §11.2 | Every body is defined once, in S0, before the 1.0 protocol freeze (§7). TOPOLOGY comes from the metadata copy. |
| Forwarding: one hop, the entry node never retries, sticky stream links | ha.md §9.1, D29, D30, D37 | Unchanged; the ownership cache is the metadata copy (§5.3). |
| Drop watermark | ha.md D33 | One payload (§2.1); in shared mode also committed to the metadata group before it is applied (M12). |
| Namespace-event payloads and the engine's journal hook | §2.1 (M15) | The ledger backend is shared mode's. |
| Peer links with the cluster secret | ha.md D35, design-cluster.md C22, C25 | Shared mode needs only the log-sized links (no bulk transfers). |
| Users and grants in the metadata group | design-cluster.md C22 | Replaces the root `USERS` file in both modes; `tsdb cluster user` edits them (§8). |
| Name slots: names unique in the whole database | design-cluster.md C18, D4 | Shared mode keeps no records of other volumes' series (M14, §5.7). |
| Per-volume admission, reply order and back-pressure | ha.md D19, D37, §15.3 | Built once, in N1 (single server first); S2 adds only peer routing. |
| Health per volume: UNAVAILABLE with a reason, `retry_after` | ha.md §15.2, §15.4 | Storage domains are shared mode's; the reasons and bodies are common. |
| `tsdb cluster` CLI | design-cluster.md §12, §8 below | init (`--mode`), node add/drain/out, volume add/move, status, user; shared mode adds domain and volume maintenance, abandon, relocate and adopt (§8). |
| Metrics | docs/metrics.md | One family per concept with a `mode` label: role, epoch, failovers, takeover time, UNAVAILABLE by reason. |

### 2.1 Namespace events: one set of payloads, one hook (M15)

The ledger in `LOCK` (ha.md §2.3) and the cluster's WAL markers (design-cluster.md §4.1) record overlapping events.
S0 defines their payloads once:

| Payload | Shared mode (ledger record) | Replicated mode (marker) |
|---|---|---|
| `COMMIT` (op, files with size and checksums, `F`, keys) | yes, with the block numbers assigned | yes; blocks are assigned when each replica applies it |
| `DROP` (`wm_gen`, `dropped_before`, or one partition) | yes | yes |
| `EPOCH` (epoch, owner) | yes | yes |
| `FLUSHED`, `MERGE`, `PURGE`, `SEG` | yes (local namespace effects) | no: every replica flushes, merges and purges on its own |
| `SERIES`, `STATE`, `CLASS`, `RETENTION`, `SLOT`, `NOW`, `COLD` | no: the chained files themselves | yes (replicated dictionary appends) |

The engine calls one hook, `jr_*`, at the same call sites in both modes (insert commit, `mt_flush`, merge, drop, purge,
dictionary appends), in two phases:
- `jr_intent(ev)` before `publish_lock`: the replicated backend appends the marker and waits for the commit point;
  the ledger backend does nothing.
- `jr_commit(ev)` under `publish_lock`, after block numbers are assigned: the ledger backend runs `ledger_edit`
  (ha.md §2.5); the replicated backend does nothing (applying the marker is its commit).

N4 builds the hook, the call sites and the ledger backend; R2 adds only the marker backend.

**Built in S0:** the payloads `COMMIT` (op, files, `F`, keys in the fence's form, rows, events, `reserve`), `DROP`
(`wm_gen`, `dropped_before`, the drop's block number, an optional single partition), `RETENTION` (series, limit, the
offset of its record) and `EPOCH` (epoch, owner) have one encoder and one decoder, `src/marker.c`, canonical (decode
then encode gives the same bytes) and fuzzed (`fuzz_marker`); the type numbers of `FEED_SLOT`, `CLASS`, `SERIES`,
`STATE`, `SLOT`, `NOW` and `COLD` are reserved there and their payloads left to the backends. The hook is
`jr_intent` / `jr_commit` in `src/internal.h`, a no-op that compiles away, called at the places of the table above:
the insert, replace and delete commit (`parts_publish`, `eng_delete_range`/`eng_replace_range`), the flush
(`flush_waiting`), the merge (`merge_commit_locked`), the drop (`eng_drop_before`), the retention append and the purge
of retired parts. **Not yet called:** the dictionary appends (SERIES, STATE, CLASSES: a dictionary has no engine
pointer; the backend that needs them adds the call in `dict_get` and the class switch).

**Lock order, D3 against design-cluster.md §5.2** (checked in review): there is no contradiction. ha.md D3 rejected
appending COMMIT to the WAL group commit *from inside `publish_lock`*, because the WAL writer role ranks above
`publish_lock` (AGENTS.md lock order). The cluster appends its COMMIT marker at §5.2 step 4, *before* it takes
`publish_lock`, and assigns block numbers when the marker is applied (step 5), so it takes the writer role without
`publish_lock` held. The ledger record names final block numbers, so it is written under `publish_lock` through
`ledger_lock`, which is not the writer role. Both orders hold; the two phases of the hook are exactly the two places.

## 3. What differs

| | Shared mode | Replicated mode |
|---|---|---|
| Durability of an acknowledged write | `write` + `fdatasync` on the owner's WAL segment over NFS: stable on the ZFS pool (sync export, SLOG) | Durable on every in-sync replica (C11) |
| Insert path | Unchanged from a single server; no copy, no extra round trip | The leader's WAL bytes sent to the ISR (+0.15 ms, design-cluster.md U1) |
| Failover | The metadata group grants the volume to a candidate; it takes the NFS lock and cold-opens the same files (ha.md §4) | The metadata group fences the ISR and promotes the longest log (C12, §6.2) |
| Failover time | Node dead: about 11.5 s (the grant after the 10 s lease, then the lock) + the cold open (0.5-3 s, ha.md U10). Owner cut off but alive: 10 s + its clean release + the open | About 4 s |
| Fence against a deposed writer | The NFS server's lock state (§6) | Replicas refuse an old epoch (design-cluster.md §6.3) |
| Rebuild | None: the data never moved. ZFS resilvers its own disks | A new replica from a hard-linked snapshot, then catch-up (§8) |
| NFS server or storage outage | Its volumes are UNAVAILABLE until it returns (ha.md §15); Pulsar holds the writes | No shared storage; two servers may be lost |
| Merges | Once, by the owner | On every replica (C9) |
| Reads | The owner, behind the read barrier (M16); lazy followers on NFS with `STALE_OK` (ha.md §7) | The leader; ISR members with `REPLICA_OK` (C15) |
| Moving a volume | A handoff: no data copied | A learner copies, then the leader moves |
| Names | Kept only beside their series (M14) | A slot index of series in other volumes (design-cluster.md §3.3) |
| On disk | `LOCK` (slots, ledger), `MANIFEST.<E>`, `CLAIM`; `requires=nfs-ha` | `REPL`, marker records; `requires=cluster` |
| Read lease | Not needed: the read barrier proves the lock after the read arrived, so owner reads go on while the Raft majority is lost | A metadata lease bounds stale leader reads (design-cluster.md §7.1), so leader reads stop while the Raft majority is lost (`REPLICA_OK` reads go on) |

## 4. One mode per deployment (M7)

- `tsdb cluster init --mode shared|replicated` fixes the mode. Every `volume add` makes a volume of that kind; the
  metadata checker refuses any other.
- The kind stays a field of each volume (and a `requires=` feature in META), so a later design could allow mixing
  without a format change; nothing is built for it now.
- **Migration** from shared to replicated mode is an export and import by name (`tsdb export` / `import`,
  docs/export-format.md) from one deployment into a new one, with Pulsar holding the writes during the cut-over, or a
  later design for moving a series between volumes. A series cannot move within one deployment today: its id names
  its volume, and its name is permanent.

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
| **An NFS server out for hours** | Every node reports the domain DOWN in its heartbeat, so nothing moves (M6). Its volumes are UNAVAILABLE, the rest go on. The metadata group lives on local disks only (M11), so routing, creation in other volumes, logins and admin go on. When the server returns, each owner claims its volumes on it again at the epoch reserved for it (M17, M18): a cold open with rollback detection, at most 4 per server group per node, no move and no new grant. Whether it made grace no longer matters. | Its volumes are UNAVAILABLE, the rest go on. Standbys block in `F_OFD_SETLK` on the dead domain and must hold back for grace + 5 s after it returns so as not to take volumes from owners still reclaiming (ha.md §15.2); an owner that missed grace loses its volumes to whichever standby wins. The root (`NODES`, `VOLUMES`) is on one server; its outage stops admin. |
| **A node crash** | Unheard for `lease_s` (10 s), its shared volumes are granted, each to one candidate, at most 4 takeovers per server group per node (ha.md §4's cap, now scheduled centrally; M13). The claimant gets the lock within a poll or two, once the server has made the dead client COURTESY (up to 1 s after its lease): about 11.5 s in all. A node that restarts with a new incarnation claims its volumes again at their reserved epochs (M18), with no grant. | Rank 1 polls every 250 ms and gets the lock at the same 11.5 s. Same time; more NFS round trips (about 200 small RPCs per second at V=32, N=4, ha.md U11). |
| **A partition between nodes, NFS reachable by all** | The Raft majority decides who is cut off. An owner on the minority side is declared down, its volumes granted to candidates on the majority side; it holds its locks until it learns of the higher epoch, from the `CLAIM` file at the latest (M5), then releases cleanly. A symmetric split without a majority changes nothing. | NFS cannot see the partition: everyone renews its locks, nothing moves (D14). An owner cut off from all peers releases on D38's rule ("better-connected nodes alive on NFS", read from probe files), a heuristic that a split into two halves cannot decide. |
| **A node's own NFS path fails** (RDMA port, mount), others fine | The owner reports the domain DOWN, the other candidates report it UP: after `local_fault_s` the volume is granted elsewhere (M6). If the fault also stops the owner renewing its NFS lease to that server, the claimant gets the lock once the lease expires, and the old owner's locks are LOST when the path heals (EIO, FENCED). If the lease lives (one domain wedged, the transport fine), the claimant is refused and the owner's release blocks; the stuck-release rule (§5.5) kills the owner after `release_stuck_s`, and ha.md §8's escalation frees the lock. | The same end by lease expiry alone when the lease dies; a wedged domain with a live lease only by ha.md §2.7's stuck-file rule. |
| **NFS grace of 10 s** (D34) | Only a granted claimant ever asks for the lock, so nobody contends with reclaiming owners: no hold-back timer is needed. A node the metadata group knows is alive is never raced. | Standbys must hold back by a timer with an early end (heartbeat advance, a successful open), because they cannot know whether the owner is alive and reconnecting. |
| **A pool rollback** (`zpool import -F`, a lost SLOG) | Epochs, id reservations and drop watermarks are committed off the pool before anyone relies on them (M11, M12): the claim fills LOST ids up to the reservation, re-applies the watermark, and claims above every epoch ever granted. Names roll back with their series (M14). No owner resumes across it (M17), and the claim compares storage with the last positions it knows (§5.4) to mark the volume DIVERGED. | ha.md D36's gossip of high-water marks: anything created or dropped between the last gossip and the rollback can come back wrong. |
| **Clock skew** | No clock in safety (§6.4). Raft and the timeouts use each node's monotonic clock; skew between nodes changes timings only. The ack gate (SUSPECT) stays a belt, as in ha.md D11; reads use the read barrier (M16), not the clock. | No clock in safety either (D11). Hold-back and staleness rules use monotonic time. Equal. |
| **The control plane itself fails** (a Raft majority down) | No new grants, no moves, no admin, no drops that raise the watermark; owners keep their locks and keep serving (M5), and creation goes on until a volume's id reservation is used up. An owner that restarts (the watchdog, an OOM kill) or is FENCED by an NFS restart claims its own volumes again at its reserved epoch (M18) and serves routing and logins from its persisted copy, marked stale; creation and drops in those volumes wait for the group. A node that dies for good in that window keeps its volumes unavailable until the group returns. | Not applicable: the NFS server is the control plane, and its outage is the first row. |
| **Operational simplicity** | One control plane for both modes, one source of TOPOLOGY, users in one place, one CLI. Needs 3 voters with local disks: a deployment with fewer such hosts adds witnesses (`tsdbd --witness`, a metadata-only voter). | No extra process; two nodes are enough. But it is a second control plane with its own files (`NODES`, `WANT.*`, slots as liveness), gossip (`OWNERS`, high-water marks in `volumes.cache`) and timers, to build and test beside the metadata group the cluster needs anyway. |

**Decision: (a)** (M2). It is never worse on the faults that matter here and is better on four: an NFS server
returning (no hold-back races, an owner that missed grace keeps its volumes), a partition between nodes (a majority
decides, not a heuristic), a node-local storage fault (told apart from a server outage), and a pool rollback (what
clients cache is committed off the pool). Its cost is a Raft group of 3 or 5 voters with local disks, which the
cluster needs anyway, and witnesses where nodes have none. **(c) is rejected**: a second control plane is a second set
of failover paths that every later change must keep correct under jepsen, for the sake of deployments of two nodes,
which a witness serves. A 1-node deployment needs no HA: it is today's tsdbd with ha.md §15's outage handling.

### 5.3 What the metadata group replaces in ha.md

| ha.md | Becomes |
|---|---|
| The lock as arbiter (D2): whoever gets the lock owns the volume | The lock as fence only; the metadata grant decides who may ask for it (M4) |
| Rank polling, rank-staggered polls on a stale heartbeat (§6, D12) | One granted claimant polls, while its metadata copy is fresh; nobody else touches the lock |
| `heartbeat_ctr` in the slots as the owner's liveness signal | Heartbeats to the Raft leader (design-cluster.md §2.2). The slots keep their epoch and state; the counter stays for the read-back and the read barrier |
| `handoff_to` hints and `WANT.<node>` files (§2.6, §6) | Moves and rebalancing as metadata operations (`VOLUME_LEADER` conditional on the epoch) |
| `NODES` in the root | Members in the metadata group, with `voter_ok` and `volumes_ok` flags (M11) |
| `OWNERS` gossip of owners and epochs (§9.1) | The metadata watch stream; TOPOLOGY from the local copy |
| Epoch claim from the high-water marks of every node (D36, §2.4) | The epoch from the grant; storage must be below it (§6.4, G2), and so must the node's own `volumes.cache` (M4) |
| High-water marks of `SERIES`, `STATES` and the watermark in `OWNERS` and `volumes.cache` (D36) | Id reservations and drop watermarks committed before use (M12); the WAL and ledger positions and the lengths of `SERIES`, `STATES`, `CLASSES` and `RETENTION` travel in heartbeats, only to detect a rollback (DIVERGED, §5.4) |
| D38: an isolated owner releases | M5: an owner releases when it learns of a higher epoch; the `CLAIM` file carries it when nothing else can |
| The standby hold-back after grace (§15.2) | Not needed: a live owner is never raced |
| The takeover cap per server group (§4) | Applied by the metadata leader when it grants |
| `VOLUMES` and `volumes.cache` for routing (§5, §15.3) | Volumes in the metadata group, with their storage record; the root `VOLUMES` stays only for single-server databases. `volumes.cache` stays as each node's local record of the highest epoch it has seen per volume (M4) |
| `USERS` in the root (design-auth.md) | Users in the metadata group (C22), edited by `tsdb cluster user` |

What stays NFS-mode-only in ha.md: §2 (lock, chained fds, `LOCK` layout, epochs on storage, the ledger, grant and
abandon, the ack gate, RECHECK and the read barrier), §3 (the proof), §4 steps 2-8 (recovery under the lock), §7
(followers), §8's watchdog and escalation, §10, §12, §15 (NFS outages).

### 5.4 Shared volumes in the metadata

Additions to `server/meta.h` (cluster-s1), all small:

- **The deployment's mode** in `INIT` (M7); `VOLUME_ADD` of the other kind is refused.
- `struct meta_volume` gets `kind` (`META_VOL_REPLICATED`, `META_VOL_SHARED`) and, for shared volumes, a storage record:
  `path` (the mount path every candidate uses, ha.md §1), `export` (`server:/export`), `fstype`, and the server group
  (for the takeover cap). The uuid it has already is the volume uuid that META and `LOCK` carry.
  `VOLUME_RELOCATE` rewrites `export` (the pool came back on another head, §8).
- **Candidates are the replica list.** `rep[]` names up to `META_REPLICAS_MAX` (7) candidate nodes, 3 by default,
  chosen by the placement rules of design-cluster.md §3.2 (spread, even owners). For a shared volume the ISR is always
  every candidate: they all see the same storage, so none can fall behind. `META_OP_VOLUME_LEADER`'s rule (the new
  leader was in the ISR) then holds unchanged, and `META_OP_ISR_CHANGE` is refused for shared volumes. Moving a
  candidate is `REPLICA_ADD` and `REPLICA_REMOVE`; no data moves.
- **Epochs below 2^24 − 1** for shared volumes (ha.md §2.4: E is a u24 in block numbers); a grant beyond is refused.
  A grant `(v, W, E)` reserves E + 1 for W (M18), so a grant to another node goes to E + 2: about 8 million moves per
  volume. `VOLUME_SELF(v, W, E + 1)`, conditional on the volume's epoch being E and its owner W, records a claim W made
  on its own and reserves E + 2 for W; if the condition fails (another node was granted meanwhile), W learns of it
  from the same answer and releases (M5).
- **Id reservations** (`META_OP_VOLUME_RESERVE(v, E, series_reserved, states_reserved)`, conditional on the owner's
  epoch, M12). The owner assigns a local series id or state code only below the committed reservation. It asks for the
  next block when a quarter of the current one is left, so creation does not wait in steady state; a block is 64 ids,
  doubled while the last block lasted less than a minute, up to 4096. When the reservation is used up and the metadata
  group cannot be reached, creation in that volume answers UNAVAILABLE (definitely not created). A claim fills LOST
  entries in `SERIES` and `STATES` up to the committed reservations (ha.md §4 step 5); without a rollback that costs
  at most one unused block of LOST records per takeover. Because a reservation is conditional on the owner's epoch,
  every reservation of epoch E comes before any grant above E in the log, so a claimant's copy that holds its grant
  holds the final reservation of the epoch before (the same holds for `VOLUME_DROP`). A node that claims its own
  reserved epoch without a grant (M18) made the reservations of the epoch before itself, and hands out ids only below
  one its persisted copy holds, so its copy has the final one too.
- **Drop watermarks** (`META_OP_VOLUME_DROP(v, E, wm_gen, dropped_before)`, conditional on the epoch). A `drop_before`
  that raises the watermark (ha.md D33) is committed here first, before any engine work, then written to the ledger
  and applied. The answers follow what the owner knows (verification of 2026-11-06):
  - no proposal sent (no leader known, the copy stale): UNAVAILABLE, definitely not applied;
  - a proposal sent whose outcome is unknown (a timeout, the leader lost): IO, indeterminate. Until the outcome is
    known (the entry or a later term in the owner's copy, confirmed with `raft_read_index`), the owner refuses inserts
    below X with UNAVAILABLE, so no row it acknowledges can be under a drop that later commits;
  - committed, then a failure in the engine: IO, as before.

  The owner applies every committed `VOLUME_DROP` of its epoch that appears in its copy and is above its ledger's
  watermark, whoever proposed it and whether or not its proposal was answered: the ledger `DROP` first, then the
  watermark, as for a confirmed one. The next claim also applies the committed watermark. Retention's drops of single
  partitions do not raise the watermark and need no entry.
- **Grants above a refused claim.** A claimant whose claim storage or its own cache refuses (§5.5 step 4) proposes
  `CLAIM_REFUSED(v, E, highest epoch seen)`; the leader then grants `max(metadata epoch, highest seen) + 1`, conditional
  on the epoch, to the same node unless it is down. G2 still guards that grant. A claim that fails for a reason of the
  claimant's own (its `volumes.cache` cannot be written, its state disk is full, the mount is missing) proposes
  `CLAIM_FAILED(v, E, reason)`: the next grant goes to another candidate, and the node is not granted claims until its
  heartbeat says the fault has cleared.
- **Rollback detection** (revised in verification). Owners report per volume in heartbeats: the WAL's durable
  position (segment seq and offset at the last `wal_durable`), the ledger's (checkpoint gen and the offset after the
  last fdatasynced record), and the `SERIES`, `STATES`, `CLASSES` and `RETENTION` lengths. The leader keeps the latest
  of each in memory and commits them in every grant entry of the volume (the dead or departing owner's last heard
  values) and in each `VOLUME_RESERVE`. A claim compares storage with them: a WAL segment that is shorter than its
  position and not covered by a later `FLUSHED`, a ledger behind its position, or a dictionary shorter than its length
  marks the volume DIVERGED (ha.md D27). A node that claims its own volume again (M17, M18) compares with the exact
  positions it holds in memory, when it still has them; the heartbeat values serve a claimant on another node or a
  restarted one. What this cannot see: a rollback that only reaches back to after the last heartbeat the leader heard
  (heartbeats are 0.5 s apart, a ZFS txg is up to 5 s), and a rollback in a site-wide power loss that also takes the
  Raft leader, whose in-memory values die with it, so the claim compares with the last grant's. For those the operator
  marks the domain with `tsdb cluster domain rolled-back D` whenever ZFS reports an `import -F` or a lost SLOG (§8):
  every volume of D is DIVERGED until acknowledged. None of this is needed for safety: ids are covered by the
  reservation, the watermark by `VOLUME_DROP`, and `CLASSES` and `RETENTION` are records nobody outside the volume
  caches, which roll back with the parts they govern. It is what tells the operator to replay the lost window from
  Pulsar while Pulsar still holds it.
- **Domain health** in heartbeats, not entries: per storage domain UP, DEGRADED, DOWN, RECOVERING (ha.md §15.2), and
  per owned volume its role (PROMOTING, OWNER, SUSPECT, RECHECK, FENCED) and health reason. TOPOLOGY carries them.
  **MAINTENANCE** is a domain state *in* the metadata (`DOMAIN_STATE`, set by `tsdb cluster domain maint`): M6 never
  moves a volume of a domain in MAINTENANCE.
- **`tsdb cluster adopt ROOT`** takes an existing NFS database in (the successor of ha.md's `ha init`): it requires
  the unmount attestation (ha.md §4), adds each volume of the root's `VOLUMES` with its number, uuid and path, seeds
  each volume's epoch, reservations and watermark from its storage, writes `LOCK` and the first checkpoint, and sets
  `requires=nfs-ha` in each META. It moves the root's users into the metadata group and marks the root
  (`adopted=<cluster uuid>` in its META), so `tsdb DIR user ...` refuses and names `tsdb cluster user`. Root series
  (volume 0) are migrated first (ha.md §5, `volume migrate-root`). One-way.

### 5.5 Takeover, release and reads under the metadata group

**Grant.** The volume's epoch is E, owned by W, with E + 1 reserved for W (M18). The metadata leader commits
`VOLUME_LEADER(v, from E, owner S, epoch E + 2)` (or above a refusal, §5.4) when (1) W has not been heard for
`lease_s` (M13), (2) W reports v's domain DOWN while another candidate reports it
UP for `local_fault_s` and the domain is not in MAINTENANCE (M6), (3) an operator or rebalancing moves v (a planned
handoff), (4) W reported v FENCED, or came back with a new incarnation, and has not claimed v again (`VOLUME_SELF`,
or OWNER in its heartbeat) within 3 × `lease_s` (normally W claims E + 1 itself, M17, M18), (5) a claimant reported `CLAIM_REFUSED` (the grant goes above the
highest epoch seen), or (6) the granted claimant has reported neither OWNER nor a refusal within 3 × `lease_s`, reports
v's domain DOWN, or reported `CLAIM_FAILED`. S is a candidate whose last heartbeat reports v's domain UP and that is
not barred by a `CLAIM_FAILED`; reasons (1)-(4) and (6) never pick the node they move away from. When no candidate
qualifies there is no grant: the domain is down for everyone and the volume waits (M6). The leader staggers grants: at
most 4 takeovers per server group per node at a time, 0-5 s of jitter (ha.md §4, §15.5).

**Claim** (on S, on the volume's HA thread):
1. S sees the grant in its metadata copy and answers v's requests with UNAVAILABLE(PROMOTING).
2. If the old owner is alive, it sees the grant too and does a clean release (ha.md §2.6): MOVED with the new epoch,
   drain, flush, compact, slot `{RELEASED}`, poison, close. S's lock request then succeeds within one poll.
3. S polls `F_OFD_SETLK` every 250 ms (the call may block on a DOWN domain; HA threads may hang, ha.md §15.2), **only
   while its metadata copy is fresh** (the watch stream heard within 2 s) and its grant is still the newest for v; every
   8 polls it confirms the grant with `raft_read_index`. A claimant cut off from the metadata group stops polling and
   drops the claim: its lock request could otherwise expire a live owner's whole NFS client (ha.md §2.2), fencing all
   of that owner's volumes on the server, for a grant already superseded. After two refusals it writes `CLAIM`
   (below), and rewrites it with its poll counter raised at every poll while it polls, so an owner that cannot hear
   the metadata group learns of the grant and can tell a live claim from one given up. A claimant that stops polling
   writes `CLAIM` once more marked `withdrawn` (best effort), and takes the claim up again when its copy is fresh and
   the grant is still the newest for v. A lock call that blocked (a DOWN domain) and then returns is followed by a
   `raft_read_index` that confirms the grant before step 4; a superseded grant unlocks at once.
4. On the grant of the lock, ha.md §4 steps 2-8, with step 4 changed: read every epoch on storage (both slots, the
   checkpoint, `EPOCH` records) and the node's own `volumes.cache` entry; if any is at or above the granted epoch G,
   refuse, release the lock, report `CLAIM_REFUSED(v, G, highest seen)` and raise an alarm (two grants crossed, a
   restored metadata group, or a writer outside it); otherwise write `{G, RECOVERING}` to the inactive slot and the
   `EPOCH` record, and raise `volumes.cache` to G. Step 5 fills LOST ids up to the committed reservations, applies the
   committed watermark (M12) and compares storage with the last positions in the grant (§5.4, DIVERGED). The rest of
   recovery is ha.md's.
5. S reports OWNER in its next heartbeat; TOPOLOGY, which already named S at G, drops PROMOTING.

**Claiming again at the reserved epoch** (M17, M18). An owner W of v at E whose volume went FENCED for a reason of the
storage (M17), or a W that restarted, claims v at E + 1 itself, without waiting for a grant: when its copy (in memory,
or persisted in its state directory after a restart) names W as v's owner at E with no newer grant, and the volume's
domain is UP for W. It follows the claim above from step 3 (G = E + 1; G2 refuses if anyone wrote E + 2), with one
difference in step 5: W compares storage with the exact positions it holds in memory, if it still has them. Once the
metadata group answers, W proposes `VOLUME_SELF(v, W, E + 1)`; until it commits, W creates no series and applies no
new drop in v (UNAVAILABLE, definitely not applied). If it fails because another node was granted E + 2, W releases
(M5). A W cut off from the metadata group polls the lock only while the newest grant it knows is its own; a W whose
copy shows a newer grant to another node does not claim. A grant to another node that W cannot see (W on the minority
side of a partition) is the case step 3's freshness rule guards against; here it costs availability only (W's lock
request may expire that node's lapsed client), and G1 and G2 keep one writer.

If the lock stays refused while the domain is UP, S keeps polling and the alert names the holder from the slot: the
holder is alive on NFS and has not let go, and only it can (M5).

**Stuck release** (M6; revised in verification so that slow is not stuck). A release that is slow but moving (a
large memtable flushed over a DEGRADED pool) is shortened, not killed: once it has run `release_stuck_s` (3 ×
`lease_s`, 30 s) since the grant, W skips the rest of its flush and compaction, poisons and closes, and the claimant
replays the WAL (no acknowledged row depends on the flush). The release is **stuck** only when one fenced call of v has
been outstanding for more than 3 × `lease_s`, measured by the monitor that does no I/O (ha.md §2.7's definition), while
W's metadata copy shows v's domain UP on another candidate. Then W treats v as ha.md's "one file stuck, server
healthy": it stops feeding the watchdog, which kills tsdbd; if the kill does not complete, ha.md §8's escalation
reboots the node. Another candidate's UP report through the metadata group counts as
ha.md §15.2's "probe of the same fs domain succeeded" for this rule. This costs all of W's volumes a failover, which is
ha.md's existing price for a stuck file; it is the only way a wedged process lets go of a lock whose lease lives.
Taken as the default on 2026-11-06 (M25; the user may change it).

**The `CLAIM` file.** `<vol>/CLAIM`, 4 KiB, written by a refused claimant with one O_DIRECT `pwrite` at offset 0:
`{magic, volume uuid, granted epoch, node id, metadata index, poll counter, withdrawn, xxh64}`, rewritten at every poll
with the counter raised. The owner reads it with its epoch read-back (every lease/5, and in RECHECK, ha.md §2.7). A
valid record of its own volume with an epoch above its own is news, whatever order such records arrive in (a late
write of an older claimant may overwrite a newer one; the newer claimant's next poll rewrites it):
- if the owner can reach the metadata group, it asks, and releases if the grant is real;
- if it cannot, it releases cleanly only after it has seen the counter of one claimant advance across two read-backs
  with `withdrawn` clear: a claimant is polling now, and its lock request will succeed once the owner lets go. A
  record that does not advance, or is withdrawn, makes it keep serving and raise an alert.

A torn record is ignored. `CLAIM` carries news, never authority: a forged or stale one can only make an owner release
(availability), never make two writers (§6). If the owner did release and the claimant then gave up, the leader
re-grants by reason (6) once the group can decide.

**Reads** (M16). The owner answers a read only after a fenced write issued *after the read arrived* has succeeded: a
WAL group commit or a ledger append (`fdatasync` returned 0, `fence_gen` unchanged), or a heartbeat slot write that
the first waiting read starts when nothing is in flight. One barrier serves every read waiting for it. LOST is sticky,
so its success proves W held the lock after the read arrived, whatever W's clock says. Under insert load the barrier is
the next group commit; on an idle volume it is one slot write and `fdatasync` (V6). Not at all once FENCED, RECHECK
or released. Followers and `STALE_OK` stay as ha.md §7. The barrier is on by default; `--read-barrier off` falls back
to the clock gate for deployments that accept stale reads after a VM pause (M24, 2026-11-06).

### 5.6 Where the metadata group lives (M11)

- Voters keep the Raft log and hard state in `<state-dir>/meta/raft/` (design-cluster.md §2.3): kilobytes, written a
  few times a minute. `--state-dir` (default `/var/lib/tsdbd`) is a local filesystem: tsdbd refuses to start a voter
  whose state directory is on NFS (`statfs` type). On nodes without data disks it is the **OS disk**, which every node
  has for its own system. A node without a writable local disk is a watcher, never a voter; a deployment with fewer
  than 3 such nodes adds `tsdbd --witness` hosts (metadata voters with no volumes, any small machine with a disk).
- **Voter eligibility** (verification of 2026-11-06). Every member reports two flags in `MEMBER_ADD` and in each
  heartbeat: `voter_ok` (the state directory is local, writable, and its free space is above a floor, 1 GiB by
  default) and `volumes_ok` (false for a witness, which is never a candidate for a volume). The leader adds only
  `voter_ok` members as learners. It judges a voter by Raft progress (its `match` index advancing) and by the
  `core_dropped` and `voter_ok` flags, for `voter_replace_s`, never by heartbeats alone: a node whose core was dropped
  keeps heartbeating from its cluster thread. A dropped core writes a `retired` mark in the state directory; that node
  may become a voter again only under a new member identity with an empty log (`tsdb cluster node add` after the
  operator clears the mark), so it never boots a core from old files.
- **The state directory gets its own small filesystem, or a quota**, apart from the `/var` that holds journald and
  `systemd-coredump`: nodes built from one image fill those together, and a full disk on every voter at once loses
  quorum. tsdbd checks the free space at start and exports it (`tsdb_state_dir_free_bytes`, with an alert in the
  operator guide).
- **Where voters go** (M20: the user confirmed on 2026-11-06 that the production nodes have writable local OS disks).
  On the OS disks of 3 tsdb nodes, or 5, each in its own rack and on its own power feed (members record both, with
  `--rack` and `--power-feed`, and the leader picks voters across them).
  Never on the NFS/ZFS servers (an NFS outage would then also cost a voter, and two outages the
  quorum, and with it every failover for the other servers' volumes); each voter in its own rack and power feed; on
  the same network as the tsdb nodes and their clients (voters on a separate management network would move healthy
  owners away whenever that network is cut). The voters' disks must honour a cache flush, or have power-loss
  protection (V7): M12's safety after a site power loss rests on a majority of them keeping what they acknowledged.
- **Every member persists its metadata copy** (M18): the snapshot and the applied index in the state directory,
  written after each applied batch and synced before the owner relies on a reservation or a grant in it. After a
  restart while the group is away, the node serves TOPOLOGY, slot chains and logins from it, marked stale, and claims
  its own volumes at their reserved epochs. A user revoked while a node was cut off can still log in on it until it
  hears the group again; `tsdb cluster user revoke` therefore reports the members that have not applied the revoke
  (their applied index from heartbeats) and waits for them up to a timeout (§8).
- The node's `volumes.cache` (highest epoch seen per volume, M4) lives in the state directory too.
- **Threads.** Heartbeats and the watch stream run on a thread that does no Raft log I/O; log syncs and hard-state
  saves run on a worker (design-cluster.md §2.3 says so; cluster-s1's first peer thread syncs inline, §7.1). A leader
  whose own log sync is outstanding for more than 2 s hands leadership to an up-to-date voter (the core's leader
  transfer) and steps down.
- **Faults of the Raft log in shared mode** (`raft_log_fault`, a failed hard save) end only the voter role: the node
  stops its core, writes the `retired` mark, reports `core_dropped`, keeps its volumes, goes on as a watcher, and the
  leader replaces it as a voter (`raft_change`, after `voter_replace_s`). It may rejoin only as a new member with an
  empty log. In replicated mode, where the Raft log and
  the data share disks, fail-stop of the process stays (cluster-s1).
- **A voter's state is never restored** from a backup, an image or a VM snapshot: a restored log is a rolled-back log.
  A node whose OS disk was replaced joins again as a new member (§8.2).

### 5.7 Names in shared mode (M14)

- A name's slot is `xxh64(name) mod 65536`, as in design-cluster.md §3.3. The metadata group keeps each slot's chain
  of homes: the current home, then earlier ones, each with the metadata index from which it was home.
- **Create without a volume**: the series is created in its slot's current home, whose owner serialises creation of
  the slot's names. Before creating, it checks that no earlier home of the slot has the name (one request to each
  earlier home's owner; the chain is empty for slots that never moved).
- **Create in a named volume V**: allowed when V is the slot's current home; otherwise refused with
  `TSDB_ERR_INVALID` (the reply names the home). Single-server databases (no cluster) keep today's `tsdb_series_at`.
- **Lookup by name**: the current home, then each earlier home. If a home that must be asked is UNAVAILABLE, the
  answer is UNAVAILABLE, never NOTFOUND (the series may be there; its data is unavailable anyway).
- **No records of other volumes.** Each volume's `SERIES` holds only its own series. A rollback of a volume forgets
  names together with their series; the LOST fill (M12) makes their old ids fail loudly; the name exists at most once
  (G2 and M12 make the fill reach every id that was handed out). Replicated mode keeps design-cluster.md §3.3's slot
  index, since its volumes do not roll back.
- **Slot moves** happen only when volumes are added, and never while the slot's current or any earlier home is
  UNAVAILABLE or DIVERGED. A move is two-phase (M19), because a stale owner of the old home A (a lagging watch, a
  pause) could otherwise finish creating a name after the new home B had asked A for it and created it too:
  1. The leader commits `SLOT_MOVE_PENDING(s, A → B)`.
  2. A's owner, seeing it, takes its dictionary lock, marks s frozen (creations in s answer MOVED from then on, and
     the lock waits out any creation already running), and proposes `SLOT_FREEZE(s, A, E_A)`, conditional on A's
     epoch. It never unfreezes on its own; only a committed `SLOT_MOVE_CANCEL` does that.
  3. Once the freeze is committed, the leader commits `SLOT_MOVE(s, B)`; B's owner creates in s from then on, asking A
     (and the older homes) first.

  A later owner of A at a higher epoch finds the freeze in its copy: a freeze conditional on E_A comes before any
  grant above E_A in the log, so the copy that holds its grant holds the freeze (as with reservations, §5.4). If A's
  owner never freezes (it is stale, paused or away), the move does not happen, and the leader cancels it after a
  timeout. `test_meta` checks that no `SLOT_MOVE` commits without a freeze of the old home at its current epoch; the
  simulator and jepsen-ha move a slot while a creation runs on a paused old home. A moved slot depends on its earlier homes for creation of new names; an NFS outage of one
  then stops creation of new names in the slots that moved away from it (not lookups of series elsewhere). Moves are
  rate-limited, so a deployment that adds volumes rarely has few such slots.
- **Placement** (M23, decided by the user on 2026-11-06). By default a slot's home is chosen by hash over all volumes,
  so a plant's series spread over every NFS server and one server outage takes a slice of every plant. **Prefix
  groups** are an option: `tsdb cluster names group PREFIX --volumes V...` makes names starting
  with PREFIX hash to slots homed in that group's volumes only, so a plant can live on one server group. A group must
  be set before the first series of its prefix exists, since a later change would move slots; the command scans the
  names of every volume (a rare admin operation) and refuses a prefix that already has a series. Groups are rules in
  the metadata group, checked before the hash, longest prefix first. Built in S4.

## 6. Fencing on shared storage

This is the correctness core of shared mode. It is ha.md §2-§3, restated against the new control plane, with what
must not be dropped when the arbiter changes.

### 6.1 What a deposed writer can still do

A writer W of epoch E whose time is over (another node was granted a higher epoch, and may hold the lock) may:
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
  and ledger) holds the OFD write lock; every WAL segment, `SERIES`, `STATES`, `CLASSES` and `RETENTION` are chained:
  OFD-locked on the description the owner writes through (ha.md §2.2, which now lists all five, with `class_lock` and
  `ret_lock` as the one mutex of the last two). When a claimant's conflicting lock request expires W's NFS client
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
- **F4. Acknowledgement.** An ack needs the sync through a fenced description to return 0, `fence_gen` unchanged, and
  the role OWNER (not SUSPECT, not RECHECK) (ha.md §2.7). The clock-based gate is a belt on top, not part of the
  argument; since verification it is a continuity check made by the write itself: the write was issued less than
  `lease − margin` after the newest fenced write that had *completed* before it was issued, and was itself in flight
  less than `lease/5`. Otherwise the write is not acknowledged (IO), and the volume goes SUSPECT and then RECHECK. A
  pause long enough to lose the lease lies between two fenced writes of the volume, so the first write after it fails
  the check unless the monotonic clock was frozen across the pause. Heartbeats keep the normal gap below 2 s, so the
  check costs nothing.
- **F5. One writer per description and the poison.** Each fenced description has one mutex, so no sibling swallows
  its writeback error; abandon poisons by `dup2`, never `close`, so a stale thread's write cannot reach a reused fd
  number of another volume (D7).
- **F6. RECHECK, the belt for U4** (M16). ha.md's argument that a successful probe after SUSPECT proves the lock was
  never lost assumes the server refuses a reclaim from a client it already expired (U4, nfsdcld). Where that may have
  been violated (W leaves SUSPECT; the domain's transport reconnected, which the health monitor sees in the
  `connect_count` of `/proc/self/mountstats`, V5; a fenced call took longer than `lease/5`), W goes to RECHECK: no
  acks, no ledger edits, no renames or unlinks, no reads answered. It leaves RECHECK only after an O_DIRECT read-back,
  issued after the trigger, of both slots, the active ledger region's checkpoint generation and `CLAIM`, and, where its
  metadata copy is fresh, the copy still naming (v, W, E). Any higher epoch or generation makes it FENCED. So does
  anything *older* than W wrote (verification): a lower checkpoint generation or `heartbeat_ctr`, a ledger record at
  W's last fdatasynced offset whose checksum differs, or a `SERIES`, `STATES`, `CLASSES`, `RETENTION` or WAL tail
  shorter than W's, or whose last record differs. Those mean a rollback under W, and M17 applies.
- **F7. No resume across an outage** (M17). DOWN, a fenced call in flight longer than `down_ms`, a transport reconnect
  while SUSPECT, or ESTALE on a volume file make the volume FENCED without a read-back: the engine is abandoned and
  the node claims the volume again at its reserved epoch by a cold open (§5.5). No in-memory picture of the files
  (`lstate`, append offsets, the memtable) outlives an outage in which the pool might have been rolled back or the
  server might have granted a reclaim it should have refused.

### 6.3 Mechanisms rejected

| Mechanism | Why it does not fence |
|---|---|
| W reads an epoch file (or asks the metadata group) before each publish or ack | Check, then act: W checks E, pauses 30 s, S claims E + 1 and replays W's segment, W resumes and appends and acknowledges rows S never read. With a frozen clock nothing on W notices. |
| Epochs in file names alone | W keeps appending to its own `wal_<E…>`, which S must replay (it holds acknowledged rows); appends after S's read are acknowledged and lost. W can still unlink adopted parts. |
| A Raft-conditional acknowledgement per group commit | Puts Raft on the insert path (design-cluster.md C4, the user's stance of 2026-10-24), costs a round trip to the Raft leader per commit, and still leaves renames and unlinks unfenced. |
| A time lease from the metadata group | W can pause between the lease check and the write (Kleppmann's GC pause); in a VM pause the monotonic clock may not advance at all (ha.md §10). |
| Revoking access on the server (mode bits, renaming the directory) | nfsd checks access at open and lets an owner write its own files; a rename does not change the file handle W writes through. |
| The ledger in the metadata group (M10) | Would fence namespace effects (epoch-conditional entries), but not WAL appends; it puts every commit, flush and merge of every volume through Raft; and a pool rollback would leave it naming files the storage no longer has. |
| A read-back before every ack | Closes F6's remaining window (below) but adds an NFS round trip to every group commit; the hot path stays unchanged (ha.md §2.5). |

### 6.4 Why it holds under the metadata group

- **G1 (one lock).** The NFS server grants `LOCK`'s write lock to at most one description at a time (ha.md L1). The
  metadata group cannot change that, in either direction.
- **G2 (epochs on storage only rise, and are never reused).** A node writes epoch E to storage only (i) after the
  metadata group committed `(v, owner = it, E)`, or `(v, owner = it, E − 1)`, which reserves E for it alone (M18),
  (ii) under the lock, and (iii) if every epoch on storage, and in its own `volumes.cache`, is below E (§5.5 step 4).
  A reserved epoch is never granted to another node, so (i) still names one node per epoch. Take two nodes with grants E1 < E2, whatever their views: if the holder
  of E2 claims first, the holder of E1 later finds E2 on storage and refuses; if the holder of E1 claims first, the
  holder of E2 finds E1 < E2 and proceeds. So the sequence of epochs on storage is strictly increasing across lock
  grants. Every Raft log is on a local disk (M11), so a pool rollback cannot revert a grant, and the next grant is
  above every epoch ever granted, also those the rollback removed from storage; ha.md L2 used high-water gossip for the
  same purpose (D36), which is no longer needed for epochs. A metadata group that lost entries (a restore, a rebuild)
  grants too low; storage or the claimant's cache refuses, and `CLAIM_REFUSED` lifts the next grant above them (§5.4).
- **G3 (the rest of ha.md §3).** L1 and L3-L8 mention neither who arbitrates nor how E was chosen beyond L2;
  they hold as written: commits are fenced (F1, F4), the successor reads after the grant (L4), no acknowledged write
  is lost (L5), late namespace operations are harmless (L6, F3), series ids are assigned only after a fenced append
  (L7) and, now, only below a committed reservation (M12), streams stay ordered (L8). Where U4 might fail, RECHECK (F6)
  keeps the belt from leaving a window after the transitions it can see, and M17 (F7) keeps an owner from resuming
  after a server restart at all.
- **G4 (the metadata group's faults cost availability only).** A lost majority, a stale watch copy, a Raft leader
  change or even a bug that grants one volume to two nodes cannot make two committing writers: G1 and G2 decide. A
  node that cannot learn of its grant does not claim; a claimant cut off from it stops polling; an owner that cannot
  reach it keeps its lock and serves (M5), and releases on `CLAIM` only while a claimant is polling (§5.5). Every
  refused or failed claim leads to another grant (§5.4), and a claimant that makes no progress is replaced (§5.5,
  reason 6), so none leaves a volume ownerless until an operator acts. An owner that restarts or is FENCED while the
  group is away claims its own volumes again at the reserved epoch (M18).
- **G5 (liveness).** A deposed owner learns of the higher epoch from the watch, a peer's MOVED, or `CLAIM` on its own
  storage (§5.5), and releases. If it can reach none of them, either its NFS lease to that server dies and the
  claimant's lock request expires it (F1), or the domain is wedged on it while the lease lives and the stuck-release
  rule kills it (§5.5).

**Walk-throughs.**
- *W paused past the lease, then resumed.* The metadata group declared W down and granted S; S's lock request expired
  W's client (W did not renew while paused). On resume W's in-flight `pwrite` or `fdatasync` fails with EIO (LOST):
  FENCED, abandon, no ack (F1, F4). Its rename of a tmp makes an orphan (F3).
- *W paused past the lease, resumed, and the server restarted meanwhile* (U4 holding or not). The first fenced write
  W issues after the pause was issued more than `lease − margin` after W's last completed one, so it is not
  acknowledged (F4); the server restart shows as DOWN or a reconnect while SUSPECT, so the volume is FENCED (F7).
- *W cut off from peers and the metadata group, NFS fine.* W renews its lease and keeps the lock. S is granted E + 2
  after `lease_s`, is refused, writes `CLAIM` and keeps raising its counter. Within two read-backs (2 × lease/5) W has
  seen the counter advance, releases cleanly; S gets the lock. No moment with two writers: W's last acknowledged rows
  were flushed or are in segments S replays (L5). If S gives up first (its own copy went stale), the counter stops
  and W keeps serving.
- *W's NFS path fails, others fine.* W's writes hang (hard mount): no acks, SUSPECT. M6 grants S; S gets the lock
  after W's lease expired. When W's path heals, its writes fail with EIO (LOST), it abandons.
- *One domain wedged on W, its lease alive.* M6 grants S; S is refused; W's release blocks. After `release_stuck_s`
  W stops feeding the watchdog and is killed or rebooted (§5.5); S then gets the lock.
- *NFS server restarts, whether or not W makes the 10 s grace.* The domain went DOWN, or the transport reconnected
  while W was SUSPECT: FENCED (F7). W claims v again at its reserved E + 1 (M18), by a cold open with replay and
  rollback detection, without a grant and without a move. No acknowledged write is lost (ha.md D34), and nothing W
  held in memory is trusted.
- *The pool rolls back under an owner that keeps its lock* (verification finding 1). W appends `DROP(p)`; its
  fdatasync returns with the record only in the SLOG; the server dies; the SLOG is lost and the pool is imported with
  `-F`, so the record is gone; W reclaims in grace (nfsdcld is on the server's OS disk). The outage was DOWN, so W
  does not resume (F7): it abandons, claims E + 1, and folds the ledger as storage has it. The ledger position W last
  reported (or holds in memory) is beyond storage's, so the volume is DIVERGED, and the operator replays the window
  from Pulsar. Before M17, W resumed with its `lstate` past the end of the ledger, its next COMMIT left a hole, and
  every later acknowledged COMMIT was purged at the next takeover. Had the restart been so short that W saw only a
  reconnect while still OWNER, RECHECK's read-back of the ledger tail finds the record missing and FENCED follows.
- *U4 violated* (the reviewers' history). W was cut off from NFS, so its lease lapsed and it went SUSPECT; S claimed
  E + 2, flushed W's segment and acked rows. The server crashed before nfsdcld durably forgot W, rebooted, and in grace
  granted W's reclaim before S's. The restart is DOWN, or a reconnect while SUSPECT, so W is FENCED (F7) and never
  acks; its claim of E + 1 then finds E + 2 on storage and refuses. **Remaining window**: a W that saw none of the
  triggers and passed the continuity check, which needs a pause with a monotonic clock that did not advance across
  W's lease expiry, S's claim, the server's crash and its reboot, *and* the transport reconnect falling between two
  samples of the health monitor; then W acks until the next periodic read-back (at most lease/5). That is a frozen
  clock and a server bug at once (U4, which N0 checks); the jepsen-ha cases with a running clock must show no loss,
  and that is now a guarantee of the continuity check rather than a race.
- *Pool rollback with the owner and the Raft leader lost* (a site power loss that costs the SLOG). The Raft logs are
  on local disks: the reservation and the watermark committed before the loss survive. The claimant fills LOST ids up
  to the reservation (cached ids of rolled-back series fail with NOTFOUND instead of writing elsewhere), re-applies the
  watermark and claims above every granted epoch. It marks the volume DIVERGED only if storage is behind the positions
  committed with the last grant or reservation, which may be old: the leader's newer values died with it. So this case
  relies on the operator: ZFS reports the `import -F` or the lost SLOG, and the runbook's `tsdb cluster domain
  rolled-back D` marks every volume of D DIVERGED (§8.2).
- *Two grants crossed* (a metadata bug, or an operator's `force` on a stale view). G1 serialises them; G2 refuses the
  lower epoch with an alarm, and `CLAIM_REFUSED` lifts the next grant.

**What must not be dropped** when building shared mode on the metadata group, though the metadata group might seem to
cover it: the OFD lock on `LOCK` and the chained fds, all five kinds (F1), the ledger and its edit discipline (F3),
epoch names (F3), the abort rule and the poison (F5), RECHECK with its tail read-back and the read barrier (F6, §5.5),
the cold open after every outage (F7), and the claim's check of the epochs on storage (G2).

**jepsen-ha cases added in verification** (N7): roll back a ledger tail that holds only a `DROP` while the owner
reclaims, then run direct inserts and a takeover (no acknowledged COMMIT lost, DIVERGED raised); pause W past the
lease with a running clock across a takeover and a server restart (no ack from W); kill the Raft leader during a
`drop_before`, then backfill below it (no acknowledged row dropped); move a slot while a creation runs on a paused old
home (one series per name); restart an owner while the Raft majority is down (its volumes come back at the reserved
epoch); let a claimant give up after writing `CLAIM` (the owner keeps serving).

## 7. Combined build plan

Each step is a track (build, adversarial review, fixes) with the gates of AGENTS.md: `make test`, `make asan`,
`make tsan`, and `make jepsen` for anything that changes how data reaches the disk; shared-mode steps from N7 on also
`make jepsen-ha`, replicated steps from R3 on `make jepsen-cluster`; client changes `make test-clients` and
`make integration-tests`.

**How the estimates are made.** The first draft counted builder weeks; the tracks of this week do not fit that.
cluster-s1's first checkpoint (the Raft core copied in, `meta.c`, the simulator, 100 seeds: +11,372 lines, 35e1456)
was committed 22 minutes after its base (ea89968, 08:31), and tsdbd's peer thread and the CLI (ede84e5) 43 minutes
after; cluster-s2's core and simulator with 15 mutants (44b352b) 33 minutes after. Auth (A1-A29) went through 5 steps
and 4 reviews in about 1.5 days (2026-11-04 to 05). What sets the pace is the design gaps that reviews, simulators and
jepsen find, the run time of the gates, and things outside tsdb. So the table has three columns: **track-days**
(build, review and fix rounds, calibrated on those three tracks; the spread is the number of review rounds a step is
likely to need), **gate hours** (machine time of the gates beyond `make test`), and **external** dependencies.

| Step | Mode | Content | Track-days | Gate hours | External |
|---|---|---|---:|---:|---|
| S0 | both | **Before 1.0** (design-cluster.md step 0, D6): format 9 (built; WAL record v2 with `fno`, kinds; the `FNO` file; part position range; META `requires=` with `nfs-ha` and `cluster`; `written_ms`, an 8-byte write time in the part header, for design-stats.md ST13 (user, 2026-11-06)); the namespace-event payloads (§2.1); **every wire body**: UNAVAILABLE with reasons and `retry_after`, PARTIAL, DROPPED, MOVED, TOPOLOGY with per-volume role and health, STATUS, in the four clients | 2-3 | 4 (jepsen, test-clients, integration) | the 1.0 freeze date |
| N0 | shared | **Go/no-go on production hardware**: ha.md §14 step 0 (lock probe: U1-U8, U13, U17, U19, U21, on the production servers' kernel and nfsd; U3, `cl_rpc_users` holding off COURTESY during a WRITE, which L1 and F2 rest on, with a partition in the middle of a WRITE) and §15.9 O0 (timeouts, grace, forced reconnect U41, V5, how often the transport reconnects without a server restart, V8) on AlmaLinux 10 over NFSv4.2/RDMA. **Gates N3-N7**; tunes N1's thresholds | 0.5 | 8-16 (runs on the hardware) | **a maintenance window on the production NFS servers** (nfsd restarts; nfsdcld enabled, ha.md §15.10 q4) |
| N1 | shared (single server) | ha.md §15 single-server parts: domains and probes (O2), WAL taint (O3), parallel open and SIGTERM (O5), per-volume admission (O6, the only admission step), recovery, revalidation and DIVERGED (O7, without cross-node root repair), the outage nemesis (O9); a local high-water cache. No volumes-follower thread, no wire bodies (S0's) | 4-6 | 6 (jepsen `outage`) + manual runs (a)-(g) | a test NFS server; ha.md §15.1's defaults until N0's numbers come |
| S1 | both | Rest of design-cluster.md step 1 with the amendments of §7.1 (heartbeat sections, threads, voter-role faults, local state dir, voter eligibility and `--witness`, persisted copies) and shared volumes in `meta.c` (§5.4: reserved epochs and `VOLUME_SELF`, `CLAIM_FAILED`, slot freezes) | 3-4 | 6 (meta-soak) | — |
| N2 | shared | ha.md's bug fixes B1-B6, those still open on main | 1 | 2 (jepsen) | — |
| N3 | shared | Ledger core (ha.md §14 step 2): records, regions, `lstate`, compaction; `ha_local.c`, `ha_fake.c` | 1-2 | 2 (property test) | N0 |
| N4 | shared | Journal hook and ledger engine (ha.md step 3, M15): the two-phase hook at every call site, edit discipline, epoch names, `MANIFEST.<E>`, followers on it, `requires=nfs-ha`; all five chained files | 3-5 | 6 (jepsen power, kill, pause) + 2 (benches, quiet machine) | N0 |
| N5 | shared | Grant and abandon (ha.md step 4): poison, drain, the abort rule | 1 | 2 (crashfs EIO) | N0 |
| N6 | shared | NFS backend (ha.md step 5): OFD lock, chaining, read-back, RECHECK, read barrier, `fence_gen`, ack gate, SUSPECT | 2-3 | 3 (test_failover both FENCE) + 8 on a test NFS server | N0; a test NFS server |
| S2 | both | tsdbd peer links (cluster secret), TOPOLOGY from the metadata copy, forwarding (ha.md §9.1 without `OWNERS`), users in the metadata group. Admission is N1's | 2-3 | 3 (test_server, forwarding, tsdbc-bench) | — |
| S4 | both | Name slots: creation and lookup by name, slot chains and two-phase moves (§5.7, M19); the cross-volume slot index for replicated mode only | 1-2 | in N7's jepsen-ha | — |
| N7 | shared | Shared-mode HA manager: grants to claims (§5.5), `CLAIM` with its counter, `CLAIM_REFUSED`, `CLAIM_FAILED`, clean, shortened and stuck release, M6 moves, reservations and drops (indeterminate proposals), the cold open after every outage and the claim at the reserved epoch (M17, M18), rollback detection by positions, `tsdb cluster adopt`, `restore` and `domain rolled-back`, the shared-mode CLI (§8), watchdog and escalation (ha.md §8, §15.2) | 4-6 | 16-28 (`make jepsen-ha`: kill, power, pause, partition, NFS restart, local NFS fault, wedged domain, U4 broken by a fake nfsd, pool rollback with creations, and the cases of §6.4 added in verification) | — |
| N8 | shared | Real NFS/RDMA runs (ha.md §13 a-h, §15 manual runs), takeover timings (U10), the read barrier's cost (V6), operator guide and runbooks (§8) | 2 | 2-3 days of runs | **maintenance windows**; a pool that may be rolled back |
| S3 | both | Clients (design-cluster.md step 6): direct mode, streams, the 32-byte header, QUERY_MULTI split; Pulsar sink | 3 | 4 (test-clients, integration with failover) | — |
| R1 | replicated | `src/repl.c` and its simulator (design-cluster.md step 2; first checkpoint 44b352b, review fixes 9f2cf49; its fifteen design gaps folded into design-cluster.md on 2026-11-06; still to add: a restored-directory fault, gap 14) | 1-2 | 6 (simulator soak) | — |
| R2 | replicated | Engine replica mode (step 3); the marker backend of the journal hook | 2-4 | 6 (jepsen) | — |
| R3 | replicated | Replication, ISR, promotion and leases in tsdbd (step 4 without what S2 built) | 3-4 | 12 (jepsen-cluster: kill, partition, pause, midcommit) | servers with disks |
| R4 | replicated | Rebuild, catch-up, moves, leader balance (step 7) | 2-3 | 12 (jepsen-cluster: kill2, rebuild, slow) | servers with disks |
| X1 | both | Cold-tier boundary (step 8); a shared volume hands partitions over the same way | 2 | 4 (midcommit at the move) | — |
| X3 | both | Measurements and docs: README section, AGENTS.md invariants and lock order, operator guides | 1-2 | the benchmarks, quiet machine | — |

**S0, the wire half (built, branch s0-wire): protocol 13.** TOPOLOGY is op 48 and STATUS op 49 (ops 39-47 stay with
the latest values, the feed and statistics); the protocols they plan move up one: `QUERY_LATEST` 14, the feed 15,
statistics 16. Every body is in `include/tsdb_protocol.h` and `include/tsdb_wire.h` and described in ha.md §15.4.
tsdbd (single server) sends UNAVAILABLE with its body and answers TOPOLOGY (itself, every volume, health from the
domains) and STATUS; MOVED, PARTIAL and DROPPED are encoded, decoded and fuzzed but sent by S1 and S2 only. Not in 13:
sessions, streams, `seq`, `attempt` and `resend_from` on inserts (S2, S3); the clients' retry relies on "refused
means not applied" and resends in order from the first refused frame.

**Minimum production HA** (shared mode; corrected in verification): S0, N0, N1, N2-N7, S1, S2 (whole: users and
TOPOLOGY in the metadata copy are needed by `adopt`, which moves the root's users there), S4 and N8. N1 is in it
because storage domains and probes (M6, the stuck-release rule, `domain maint`, domain health in heartbeats),
per-volume admission (SUSPECT and PROMOTING refused before a worker) and DIVERGED are N1's. Direct-mode clients (S3)
follow it; until then clients reach owners through forwarding. N1 also ships on its own to today's single servers.

**Order** (M9):
1. **Now**: N0 is booked for the next maintenance window, on one NFS server and a test pool (M22; the window is the
   long pole); S0 (it must land before 1.0); N1 on
   a test NFS server with ha.md §15.1's default thresholds (N0's numbers tune them later); S1 and N2; R1 goes on.
2. **After N0 passes**: N3, N4, N5, N6 on one track; S2 and S4 on another; N1 finishes on the third.
3. **Then** N7 with its jepsen-ha matrix, then N8 in maintenance windows; S3 alongside.
4. **Replicated mode**: R2-R4 after N7, on servers with disks. Its per-volume admission is N1's, which no longer
   waits for N0.

**Totals** (restated in verification; the first revision's sum left N1 out and did not add up). Minimum production HA:
S0 2-3, N0 0.5, N1 4-6, N2 1, N3 1-2, N4 3-5, N5 1, N6 2-3, S1 3-4, S2 2-3, S4 1-2, N7 4-6, N8 2: **about 27-39
track-days**, which on three tracks is roughly 2-3 weeks of calendar if the maintenance windows come in time. Then R1-R4
for replicated mode, 8-13 track-days, and X1 and X3, 3-4. Gate time for the minimum: about 68-88 machine-hours of
gates plus N8's 2-3 days of real NFS runs, **about 5-7 machine-days**, mostly jepsen-ha and the NFS runs. The calendar
is set by the two maintenance windows (N0 and N8) and the review rounds, not by building.

**If N0 fails** (the no-go branch, written down before the window so that the user can choose knowing it; on
2026-11-06 the user took (i) unless every node has a BMC the metadata leader reaches, then (ii): M22, the recommended
default, which the user may change):
- *OFD locks do not fence over RDMA, POSIX locks do* (U1 holds for POSIX locks owned by one thread): ha.md §14 step
  0's fallback changes F1's mechanics, not this design.
- *No lock form fences* (U1 fails for both, U5 or U19 fails so in-flight I/O outlives the lock, or U3 fails so a
  WRITE in flight at the server lands after the claimant's lock): there is no server-enforced fence, and automatic
  failover on a lease is unsafe. Two outcomes, both keeping N1, the metadata group, forwarding and name slots:
  - **(i) Shared mode without automatic failover** (recommended unless every node has a BMC): a volume moves off a
    dead node only by `tsdb cluster volume move V NODE --dead OLD` after the operator attests that OLD is powered off
    (or its tsdbd stopped and the export unmounted, the attestation `adopt` already asks for). Outage handling,
    routing and planned moves (clean releases) work as designed; a node crash costs its volumes until an operator
    acts. N5-N7 shrink: no RECHECK, no `CLAIM`, no stuck release.
  - **(ii) A power fence before each grant**: the metadata leader turns the old owner off through its BMC (IPMI or
    Redfish) and grants only once the BMC confirms it is off. Automatic, but every node needs a BMC on a network the
    leader reaches, and a failed BMC call leaves the volume waiting.
- *U41 fails* (no forced reconnect): grace is 45 s (D34); with M17 that only lengthens the end of an outage.

### 7.1 What cluster steps 1 and 2 give shared mode, and what step 1 must change

**Status (S1, branch ha-s1, 2026-11-08).** Items 1-12, 15-22 are built; 13 and 14 are what they say (the opcode
`NAME_GROUP` is reserved and applies INVALID, the snapshot has an empty rule list; the heartbeat section carries the
oldest fenced call age). Format rulings of the advisor are in the head comment of `server/meta.h` (snapshot version 2,
`level`, `VOLUME_LEADER` carries its new epoch, the reserved epoch is implicit, `META_ERR_RANGE`, `META_ERR_BLOCKED`,
`META_ERR_ATTEST`, positions are reports, no clocks in apply). **Not as the list says:** item 2's heartbeats and watch
run on the cluster thread with no Raft log I/O, and log syncs and snapshot writes run on a disk worker; the hard-state
save (`raft_log_hard_save`) stays on the cluster thread, because its slot sequence is shared with the durable floor and
the copied Raft log has no lock for it: moving it needs a thread rule from the Raft core (upstream, not done). A hard
save is a 4 KiB `fdatasync`, so a stalled disk can still hold the cluster thread there until the leader's 2 s transfer
(which needs the thread) is reached: the transfer fires from syncs, not from hard saves.

- **Step 1 (`cluster-s1`: 35e1456 the metadata state machine, ede84e5 tsdbd's peer thread and the CLI)**: the Raft
  core copied in, the metadata state machine (members, volumes with replicas, leader, epoch, ISR, epoch history, name
  slots, users), epoch-conditional operations applied one by one, snapshots, the step checker and the simulator. All
  of it serves shared mode as it is: an owner change is `META_OP_VOLUME_LEADER` with candidates as the ISR (M8).
- **What step 1 must change, before its formats settle** (to go into cluster-s1's task; this design does not edit
  another track's files). This is the **complete list of amendments the running cluster-s1 track must take**: items
  1-9 from the review and verification, 10-14 from the user's answers of 2026-11-06 (M20-M25), 15-22 from the gaps
  that cluster step 2's simulator found in design-cluster.md (folded in there on 2026-11-06, "Corrections from step
  2's simulator"; `repl.h`'s `struct repl_vmeta` and `repl_meta_promote` at 9f2cf49 are the reference). design-cluster.md
  §14 points here rather than keeping a second list.
  1. An extensible heartbeat body: `HB{node, incarnation, applied, typed sections}`, the first sections being owned
     volumes (role, epoch, health reason, dictionary lengths) and storage domains (state). The leader keeps a
     per-(volume, domain) `local_fault_s` timer from them (M6).
  2. Heartbeats and the watch on a thread with no Raft log I/O; log syncs and hard saves on a worker; leader transfer
     when the leader's own sync is outstanding for 2 s (M11).
  3. A Raft log fault ends only the voter role in a shared-mode deployment; replicated mode keeps `_exit(1)` (M11).
  4. The metadata state in `--state-dir` on a local filesystem, not inside `--dir` (which is on NFS in shared mode).
  5. In `meta.c`: the deployment's mode in INIT; the volume kind and storage record; `ISR_CHANGE` refused for shared
     volumes; the u24 epoch bound; `VOLUME_RESERVE`, `VOLUME_DROP`, `CLAIM_REFUSED`, `VOLUME_RELOCATE`,
     `DOMAIN_STATE`; grants of shared volumes after `lease_s`, and after a new incarnation that has not claimed its
     volumes again within 3 × `lease_s` (M13, M18). The checker gains: a
     shared volume's ISR equals its candidates; epochs, reservations and watermarks never go down.
  6. *(verification)* Member flags `voter_ok` and `volumes_ok` in `MEMBER_ADD` and in heartbeats; `manage_voters`
     (cluster.c 2050-2128 at e0bba99) adds only `voter_ok` members as learners and judges a voter by Raft progress
     and a `core_dropped` flag for `voter_replace_s`, not by `heard_ns`; a dropped core writes `retired` in the state
     directory, and such a node is never picked again under its old identity (§5.6). As built, a voter whose disk
     filled kept heartbeating, was never replaced, and could be re-added to boot from its old files.
  7. *(verification)* `tsdbd --witness`: no `--dir`, no volumes, never in placement (`volumes_ok` false).
  8. *(verification)* Every member, not only those with a core, persists its copy (snapshot and applied index) in the
     state directory and serves from it after a restart, marked stale (M18). cluster-s1's watchers keep it in memory
     only.
  9. *(verification)* In `meta.c`: grants reserve the next epoch for their holder, `VOLUME_SELF`, `CLAIM_FAILED`,
     `SLOT_MOVE_PENDING`, `SLOT_FREEZE`, `SLOT_MOVE_CANCEL` (and `SLOT_MOVE` only after a freeze at the old home's
     current epoch), positions in grant and reservation entries (§5.4), and a restore generation for
     `tsdb cluster restore` (§8.1). The checker gains: no epoch is granted to a node other than the one it is
     reserved for; no slot moves without a freeze.
  10. *(user, M20)* A power feed beside the rack in `MEMBER_ADD` and the member record (`tsdbd --power-feed`, like
      `--rack`); `manage_voters` picks 3 voters (5 from 5 eligible nodes) in distinct racks and distinct power feeds
      when there are enough, among `voter_ok` members only (item 6).
  11. *(user, M22)* A deployment setting `failover = lease | attested | bmc` in INIT and SETTINGS. Under `attested`
      the leader never grants a shared volume away from a node it no longer hears (grant reasons 1 and 4 of §5.5);
      only an operator's `VOLUME_LEADER` with an attestation (`volume move V NODE --dead OLD`, which records who
      attested and when) does. `bmc` needs a BMC address per member (`MEMBER_ADD`) and a power-off confirmation
      before such a grant. `lease` is today's rule.
  12. *(user, M24)* A deployment setting `read_barrier` (on by default), which a node's `--read-barrier off`
      overrides for itself; the node reports it in its heartbeat so `tsdb cluster status` shows who runs without it.
  13. *(user, M23)* Nothing in S1 for prefix groups beyond room in the snapshot for a rule list
      (`NAME_GROUP(prefix, volumes)`), which S4 adds; slot homes stay `META_OP_SLOT_HOME` over all volumes.
  14. *(user, M25)* Nothing in `meta.c`: the stuck-release rule uses `lease_s` (item 5) and the domain health in
      heartbeats (item 1). The heartbeat's volume section carries the age of the oldest outstanding fenced call, so
      the leader and `status` can show a release on its way to the kill.
  15. *(step 2, gap 3)* **Replica instances.** Each replica entry and each ISR member names the replica's instance
      id (`rid`, 64 bits, new for every fresh directory): `struct meta_replica` gains `rid`, `isr[]` becomes (node,
      rid) pairs, `REPLICA_ADD` names the learner's rid, and `ISR_CHANGE` and `VOLUME_LEADER` carry the rid of every
      member they list. A member whose rid differs from the record's is not in the ISR, whatever its node id.
  16. *(step 2, gaps 5 and 11)* **The promoted ISR keeps the fenced members that did not answer**, the old leader
      included, marked in an `unanswered` mask of the volume record; a later `ISR_CHANGE` by the new leader clears or
      drops them. `VOLUME_LEADER` as a promotion takes the fenced set and the replies (node, rid, durable end, last
      epoch) and applies `repl_meta_promote`'s rule, so the state machine and the simulator share one function.
  17. *(step 2, gap 8)* **A promotion is conditional on the ISR version as well as the epoch** (the basis the fences
      were sent for). `meta_op_volume_leader` names only the epoch today; an `ISR_CHANGE` committed by the deposed
      leader after the fences went out would otherwise be undone by the promotion.
  18. *(step 2, gap 7)* **The epoch history is not capped at a count.** `meta.c` keeps every epoch now, which is right:
      a returning replica may hold any epoch back to its oldest retained segment. If it is ever trimmed, then by a
      position the volume's leader reports (the oldest position any replica may still hold), never to "the last
      64". The checker gains: the history only grows at its end, and each entry's start is above the previous one's.
  19. *(step 2, gap 14)* **A replica's open generation**: `REPLICA_OPEN(v, node, rid, gen)`, committed before the
      replica answers a fence, sends an ACK or leads, conditional on `gen` being above the recorded one; the record
      keeps the last `gen` per replica. A directory whose REPL generation is below the record's is a restored copy
      and is refused (design-cluster.md §8.5).
  20. *(step 2, gap 4)* Nothing in `meta.c` for "a restarted leader never resumes its epoch": the core resigns
      (`led` in REPL), and the state machine must accept a promotion away from a leader that is alive and heard
      (grant reason "the leader resigned": a `RESIGN(v, epoch)` request, the core's `REPL_M_RESIGN`), fencing as for
      a dead leader.
  21. *(step 2, gap 1)* Nothing in `meta.c` for ISR growth beyond item 15: the joining replica is counted by the
      leader from the moment it asks (R3), and `ISR_CHANGE` stays conditional on epoch and ISR version. The checker
      gains: an ISR member added by `ISR_CHANGE` had been a replica (`REPLICA_ADD`) with the same rid.
  22. *(step 2, gap 15)* **A shrink's `ISR_CHANGE` carries the leader's commit point** when it asked, kept in the
      volume record, so that `tsdb cluster status` and `volume force-leader` can show which remaining replicas hold
      everything up to it (an unclean promotion is safe exactly when nothing was acknowledged after the last shrink).
- **Step 2 (`cluster-s2`, 44b352b, then 13081cc and 9f2cf49 after review)**: the sans-IO replication core. Shared
  mode needs none of it (one copy, no ISR), and is not blocked by it. Its simulator found fifteen gaps in
  design-cluster.md, folded in there on 2026-11-06 (its "Corrections from step 2's simulator"); items 15-22 above are
  what they ask of step 1. What carries over is the method (a sans-IO core under a seeded simulator), which
  the shared-mode HA manager should copy: grants, lock results, heartbeats and `CLAIM` reads in; claims, releases and
  reports out; checked for G1-G5 against a fake NFS server that expires client state and, in one mode, breaks U4.

## 8. Operations

### 8.1 The CLI (`tsdb cluster`)

| Command | Mode | What it does |
|---|---|---|
| `init ADDR --mode shared\|replicated --secret-file F` | both | Creates the metadata group and fixes the mode (M7). |
| `node add\|drain\|out`, `status` | both | Members; `status` groups shared volumes by server and domain (ha.md §15.7). |
| `volume add --shared PATH` / `volume add` | shared / replicated | A volume of the deployment's kind; a shared volume's path is checked on every candidate (ha.md D26). |
| `volume move V NODE` | both | A planned handoff (§5.5 grant reason 3). |
| `volume move V NODE --dead OLD` / `node dead OLD` | shared | Under `failover = attested` (M22): the operator attests that OLD is powered off (or its tsdbd stopped and the export unmounted); the grant records who attested and when. `node dead` does it for every volume OLD owned. |
| `names group PREFIX --volumes V...` | both | A prefix group (M23): names with PREFIX get slot homes in those volumes only. Refused once a series with the prefix exists. |
| `user add\|revoke\|grant ...` | both | Users and grants in the metadata group. `tsdb DIR user` refuses on an adopted root and names this command. A revoke reports the members that have not applied it yet (their applied index from heartbeats) and waits for them up to `--wait` (10 s); a member cut off from the group keeps accepting the revoked user until it hears the group again. |
| `restore --from NODE` | both | The Raft majority is gone for good: rebuilds the group from the surviving voter with the highest log, after the operator confirms. It starts a new restore generation, so an old voter that comes back is refused, not merged. Shared mode seeds each volume's epoch, reservations and watermark from the owners' heartbeats and from storage (witnesses mount nothing); an owner whose `VOLUME_RESERVE` or `VOLUME_SELF` the restored group refuses because it holds an older epoch reports `CLAIM_REFUSED` with its epoch, which lifts the volume's epoch (§5.4). Replicated mode seeds from the replicas. |
| `adopt ROOT` | shared | Takes an existing NFS database in (§5.4). |
| `domain maint D on\|off [--until T]` | shared | Owners on D drain (stop writes, flush, stop merges, `fdatasync`), then the metadata marks D MAINTENANCE: nothing moves, clients get UNAVAILABLE(MAINTENANCE) with `retry_after` up to T. |
| `domain probe D` | shared | Every candidate probes D and reports; shows who sees it UP. |
| `volume relocate V server:/export` | shared | The pool came back on another head or address: rewrites the storage record (`VOLUME_RELOCATE`); the path every node mounts stays (ha.md §15.5 item 9's `rehome`). |
| `volume abandon V` / `domain abandon D` | shared | Marks the old engine and its stuck threads abandoned on their node (they leak), then grants V (or every volume of D) at a higher epoch: a cold open with revalidation. The old export must never be served again. |
| `volume ack-divergence V`, `volume lower-watermark V` | shared | As ha.md §15.7; the watermark is lowered through `VOLUME_DROP`. |
| `domain rolled-back D` | shared | ZFS reported an `import -F` or a lost SLOG for D's pool: every volume of D becomes DIVERGED, whatever the positions said (§5.4), until `volume ack-divergence`. |

### 8.2 Runbooks (one page per mode, the full ones in N8's operator guide)

**Shared mode.**

| Event | What tsdb does | What the operator does |
|---|---|---|
| An NFS server down for hours | Its volumes are UNAVAILABLE (DOWN, `retry_after` up to 60 s); nothing moves (M6); Pulsar holds the writes; the rest runs at full speed. When it returns, each owner claims its volumes on it again at their reserved epochs by a cold open (M17), 4 at a time per server group per node. | Nothing in tsdb. Afterwards `tsdb cluster status`: any DIVERGED volume means the pool lost acknowledged writes. If the pool comes back on another head or address: `volume relocate` (or `domain abandon` when threads are stuck). |
| A pool rolled back (`zpool import -F`, a lost SLOG) | No owner resumes (M17); the claim fills LOST ids up to the reservations, re-applies the watermarks, and marks a volume DIVERGED when storage is behind the positions last reported (§5.4); clients with cached ids of lost series get NOTFOUND. tsdb cannot see every rollback (one within the last heartbeat, or a site power loss that also took the Raft leader). | **Always** run `domain rolled-back D` when ZFS reports the import or the SLOG loss. Then check `tsdb_lost_parts`; replay the lost window from Pulsar if it still holds it; then `volume ack-divergence`. |
| A pool lost | Its volumes stay UNAVAILABLE. | Restore the pool from a backup into an export, `volume relocate`; the restore is a rollback (row above). |
| A node dead, under `failover = attested` (N0 found no server-enforced fence, M22) | Its volumes are UNAVAILABLE(NO_OWNER) with an alert naming the node; nothing moves on its own. | Make sure the node is off (power, or tsdbd stopped and the exports unmounted), then `tsdb cluster node dead OLD`; its volumes are granted and claimed as after a lease. Never attest a node that may still be running. |
| A node's OS disk lost | Its volumes fail over after the lease; a voter is replaced after `voter_replace_s`. | Reinstall; `node add` it as a **new** member. Never restore its state directory from a backup or image (M11). |
| A voter's state disk full or failing | It drops its core (`core_dropped`, `retired`), keeps its volumes and is replaced as a voter after `voter_replace_s` (§5.6); the free-space alert fires first. | Free the space or replace the disk; clear `retired` and `node add` it as a new member to make it a voter again. |
| The Raft majority lost | Owners keep serving; no grants, moves, admin or watermark raises; creation until reservations run out. An owner that restarts or is FENCED by an NFS restart claims its own volumes again at their reserved epochs (M18) and serves routing and logins from its persisted copy, without creation or drops in those volumes. A node that dies for good leaves its volumes waiting. | Bring voters back. Avoid planned restarts and upgrades of tsdb nodes until they are back. If they are gone for good: `tsdb cluster restore --from NODE` (§8.1). |
| Planned ZFS maintenance | — | `domain maint D on --until T`, then off. |

**Replicated mode** (design-cluster.md §6.6, §8): one server lost (its volumes promote in about 4 s; rebuild starts
after `rebuild_after_s`); two servers lost (writes go on with min ISR 1; rebuild both); a disk lost (that server's
replicas rebuild from the others); the Raft majority lost: writes go on while each volume's ISR answers, but **leader
reads stop** within `lease_ms` (2 s), because the read lease is extended only by the Raft leader's heartbeat answers
(design-cluster.md §7.1); `REPLICA_OK` reads go on. Restore as above, seeded from the replicas.

## Review of 2026-11-06

Two reviews of the first draft (283a2ea): fencing and the control plane, and sharing, cost and operations. Every
finding was taken; where a part of one was not, it says why ("Considered in review").

| Finding | Change |
|---|---|
| Fencing 1 (high): voter logs on NFS break Raft under a pool rollback; a leader's hung sync freezes the control plane | M11, §5.6: local disks only, witnesses, voter-role faults, step-down on a stalled sync; the per-node `volumes.cache` belt (M4) |
| Fencing 2 (high): high-water marks committed once a minute let ids be reused after a rollback | M12, §5.4: reservations before use, drops committed before the ack; the window sentence removed |
| Fencing 3 (medium): SUSPECT returns to OWNER before the read-back, so a broken U4 leaves a window | M16, F6: RECHECK; ha.md §2.7, §10; the jepsen-ha case with a fake nfsd |
| Fencing 4 (medium): CLASSES and RETENTION chained only in the overview | ha.md §2.2, §2.5, §2.6, §4 steps 3 and 5, test_failover |
| Fencing 5 (medium): a refused claim has no way forward | M4, §5.4 `CLAIM_REFUSED`, M13's new incarnation, seeding at adopt and restore |
| Fencing 6 (medium): M6 needs the owner's lease to die | M6, §5.2, §5.5 stuck release; ha.md §15.2 |
| Fencing 7 (low): a stale claimant can expire a live owner's whole client | §5.5 step 3: poll only with a fresh copy, `raft_read_index` every 8 polls |
| Fencing 8 (low): a frozen clock gives stale owner reads | M16, §5.5 Reads: the read barrier; open question 5 on its cost (answered: M24) |
| Sharing 1 (high): name slots carry cluster assumptions into shared mode | M14, §5.7; open question 4 (answered: M23) |
| Sharing 2 (high): a voter's storage fault takes down a healthy data plane | M11, §5.6, §7.1 items 2-4 |
| Sharing 3 (medium): a 3 s node timeout for shared volumes | M13 (was open question 6) |
| Sharing 4 (medium): mixing per volume rests on a migration path that does not work | M7, §4; X2 dropped (was open question 3) |
| Sharing 5 (medium): two journals for the same events | M15, §2.1; the D3 lock-order check |
| Sharing 6 (medium): N1 overlaps S0 and S2 | §7: S0 owns every wire body; N1 single-server only; S2 without admission; ha.md §15.4, §15.9 |
| Sharing 7 (medium): critical path without S4, N0 too late, uncalibrated weeks | §7: minimum production HA, N0 first and gating, three columns |
| Sharing 8 (medium): shared-mode operator commands missing | §8: the CLI, MAINTENANCE in the metadata, adopt marks the root, runbooks |
| Sharing 9 (low): cluster-s1 fixes its frames without shared mode's fields | §7.1: the amendments, to go into cluster-s1's task |

**Considered in review.**
- *Reservations for `CLASSES` and `RETENTION`* (fencing 2 named all four lengths): not taken. They are records, not
  ids: nobody outside the volume caches them, and a pool rollback reverts them together with the parts they govern
  (one ZFS transaction group), so the volume stays consistent; their lengths still travel for DIVERGED (§5.4). Their
  *fencing* was the real gap (fencing 4) and is fixed.
- *Closing RECHECK's last window with a read-back before every ack* (fencing 3): not taken; it would add an NFS round
  trip to every group commit (§6.3). The window needs a U4 server bug, a frozen-clock pause and a missed reconnect at
  once (§6.4); N0 checks U4 on the production servers.
- *"One of the two designs is wrong about the lock order"* (sharing 5): neither is; §2.1 explains.
- *A voter-only `--witness` on NFS* (sharing 2's fallback): not offered; a witness is a host with a disk (M11).

## Verification of 2026-11-06

Two verification passes over the revised design (12545f4): NFS-mode fencing and durability (V2-0), and the control
plane and operations, read against cluster-s1 as built at e0bba99 (V2-1). Every medium and high finding was taken;
the low ones too, except where noted.

| Finding | Change |
|---|---|
| V2-0 1 (high): an owner that keeps its lock across a pool rollback resumes with ledger and dictionary offsets past the end; later acknowledged COMMITs are purged at the next takeover | M17 and F7 (the reviewer's option (a), preferred because it removes the class: no in-memory picture survives an outage); RECHECK also reads the tails back (option (b), as a belt for a reconnect without DOWN) and treats anything older as a fault; ha.md §2.7, §15.5; jepsen-ha case (§6.4) |
| V2-0 2 (medium): the gate measured from the write it judged, so the first write after a pause passed it | M16, F4: a continuity check by the issuer, and a call in flight longer than lease/5 is not acknowledged; ha.md §2.7; §6.4's remaining window restated (a frozen clock and U4 at once) |
| V2-0 3 (medium): a `VOLUME_DROP` that timed out was answered UNAVAILABLE but could commit | M12, §5.4: IO for an unknown outcome, inserts below X refused until it is known, every committed drop of the epoch applied; ha.md §2.7 refusals; jepsen-ha case |
| V2-0 4 (medium): a rollback was not flagged DIVERGED in the common case | §5.4: WAL and ledger positions in heartbeats, grants and reservations; exact positions on a self-claim; `domain rolled-back D` (§8) and the runbook; the walk-through corrected |
| V2-0 5 (medium): a slot move had no fence against a stale old home | M19, §5.7: `SLOT_FREEZE` by the old home's owner before `SLOT_MOVE`; design-cluster.md §3.3; test_meta, simulator and jepsen cases |
| V2-0 6, V2-1 7 (low-medium): grants to a claimant that cannot reach the storage; local claim faults loop | §5.5: grants only to candidates reporting the domain UP, re-grant by reason (6), `raft_read_index` after a blocking lock call, `CLAIM_FAILED` (§5.4) |
| V2-0 7 (low): the stuck-release rule fired on slow releases | §5.5: a slow release is shortened (no flush) after `release_stuck_s`; only a call outstanding for 3 × lease is stuck |
| V2-0 8 (low): `CLAIM` written once could be overwritten by an older record | §5.5: rewritten at every poll; any valid record above the owner's epoch is news |
| V2-0 9 (low): voters' disks may lose acknowledged entries on power loss | V7; §5.6 placement; open question 1 (answered: M20) |
| V2-1 1 (high): a voter with a faulty disk is never replaced; no voter eligibility, no `--witness` step | M11, §5.6 (flags, progress-based replacement, `retired`, the state directory's own filesystem and alert), §7.1 items 6-8, S1 |
| V2-1 2 (medium): any restart during quorum loss lost that node's volumes and logins | M18: reserved epochs, `VOLUME_SELF`, persisted copies; §5.2, §8.2 corrected |
| V2-1 3 (medium): `CLAIM` could make an owner release after the claimant gave up | M5, §5.5: poll counter, `withdrawn`, release only on an advancing counter, the claimant resumes, re-grant by reason (6) |
| V2-1 4 (medium): replicated-mode leader reads stop during quorum loss, the runbook said otherwise | The simpler of the two fixes: stated in §3, §8.2 and design-cluster.md §6.7, §7.1; an ISR-based read lease is listed under "Later" there |
| V2-1 5 (medium): the minimum HA set left out N1 and cut S2; totals did not add up; users in two steps | §7: minimum restated with N1 and all of S2, totals recomputed; users in S2 in both documents |
| V2-1 6 (medium): N0 gated N1; no no-go branch; U3 missing | M9, §7: N1 starts now; N0 gates N3-N7; U3, U8, U17 in N0; "If N0 fails" with outcomes (i) and (ii); open question 3 (answered: M22) |
| V2-1 8 (low): `tsdb cluster restore` only in the runbook | §8.1 with its restore generation, seeding and refusal rules; N7 |
| V2-1 9 (low): a revoke does not reach nodes cut off from the group | §5.6, §8.1: the revoke reports and waits for laggards; the runbook says so |
| V2-1 10 (low): no guidance on where voters go | §5.6 placement; open question 1 (answered: M20) |

**Considered in verification.**
- *Keeping ha.md §15.5 step 4's re-log of memtable rows from memory in shared mode*: dropped (M17). It is exactly the
  trust in memory after an outage that finding 1 showed to be unsafe, and memory is often gone anyway; the positions
  of §5.4 make the loss visible, and Pulsar replays it.
- *The ISR-based read lease for replicated mode* (V2-1 4's second fix): later, not now. It changes the promotion rule
  of design-cluster.md §6.2 (every promotion would have to wait out the promises of the ISR members it fences), and
  quorum loss is rare where every server is a voter; the documentation fix is the honest one for now.

## Open questions

**All six were answered on 2026-11-06** and are decisions M20-M25. The user answered 1 to 4; for 5 and 6 the
recommendations were taken as defaults, which the user may change, and for 3 the fallback is the recommended default.
The questions are kept below with their answers, so the reasoning stays readable.

**Still open** (facts to collect, not choices): which nodes, racks and power feeds carry the voters, and whether
their OS disks honour a cache flush (V7, checked on one voter host in S1 or N0); the date of the next maintenance
window, and whether nfsdcld is already enabled on the server N0 uses (ha.md §15.10 q4); whether every node has a BMC
the metadata leader can reach (it decides between fallbacks (i) and (ii) if N0 fails, M22); the prefix groups, if
any, that the operator wants before the first series of those prefixes are created (M23).

1. **Do the production nodes have a writable local OS disk that can hold the Raft log** (kilobytes, a few syncs a
   minute) on a filesystem of its own or under a quota, and does it honour a cache flush (V7)? If fewer than 3 do,
   where can witnesses run? *Recommended: voters on the OS disks of 3 (or 5) nodes in different racks and power
   feeds, never on the NFS/ZFS servers, on the tsdb network; otherwise 3 small `tsdbd --witness` hosts with a disk.*
   The log must never be on NFS (M11).
   **Answered by the user on 2026-11-06: yes**, the production nodes have writable local OS disks; voters on the OS
   disks of 3 (or 5) nodes in different racks and power feeds (M20, §5.6).
2. **Shared mode first, with the minimum production HA of §7** (S0, N0, N1, N2-N7, S1, S2, S4, N8: about 27-39
   track-days and 5-7 machine-days; direct-mode clients after)? *Recommended: yes*; N1 ships to today's single
   servers first, and the cluster's replication core continues in parallel.
   **Answered by the user on 2026-11-06: yes**, shared (NFS) mode first with the minimum production HA set (M21, §7).
3. **When can N0 run on the production NFS servers, and if it finds no server-enforced fence, which fallback** (§7,
   "If N0 fails": (i) failover only after an operator attests the old node is off, or (ii) a BMC power fence before
   each grant)? It needs nfsd restarts and nfsdcld enabled (ha.md §15.10 q4), and it gates N3-N7. *Recommended: the
   next maintenance window, on one server and one test pool; S0, N1, S1 and N2 do not wait for it. Fallback (i),
   unless every node has a BMC the metadata leader can reach.*
   **Answered by the user on 2026-11-06**: N0 runs in the next maintenance window, on one NFS server and a test pool;
   if it finds no server-enforced fence, fallback (i) unless every node has a BMC reachable by the metadata leader
   (the recommended default; the user may change it). M22, §7.
4. **Placement of names in shared mode** (M14): a name's home volume is chosen by hash over all volumes, so a plant's
   series spread over every NFS server and one server outage takes a slice of every plant. Acceptable, or should
   homes be grouped (a name prefix picks a group of volumes on one server group, and the hash picks within it)?
   *Recommended: hash over all volumes by default, with prefix groups as an option the operator sets before the
   first series of the prefix exists* (a later change of the rule would move slots, §5.7).
   **Answered by the user on 2026-11-06**: hash over all volumes by default, prefix groups optional (M23, §5.7).
5. **The read barrier's cost** (M16): an owner read waits for the next fenced write after it arrived; on an idle
   volume that is one slot write and `fdatasync` over NFS (perhaps 0.2-0.5 ms, V6). *Recommended: on by default,
   since stale reads were ruled out (ha.md D23); measure in N8; offer `--read-barrier off` (the clock gate) only for
   deployments that accept stale reads after a VM pause.*
   **Default taken on 2026-11-06** (the recommendation; the user may change it): on by default, measured in N8,
   `--read-barrier off` as the opt-out (M24, §5.5).
6. **May tsdbd kill itself, and the escalation reboot the node, for a release stuck on a wedged domain whose server is
   healthy** (§5.5; ha.md open question 2 asks the same for a stuck file)? *Recommended: yes, when one fenced call has
   been outstanding for 3 × lease (30 s) and another candidate reports the domain UP*; a release that is only slow is
   shortened instead. It fails all of that node's volumes over, which is the only way the lock is freed.
   **Default taken on 2026-11-06** (the recommendation; the user may change it): yes, only after 3 × lease with
   another candidate reporting the domain UP (M25, §5.5).

## Unverified assumptions

| # | Assumption | How it will be verified |
|---|---|---|
| V1 | An owner reading `CLAIM` with O_DIRECT every lease/5 costs as little as ha.md's slot read-back (U11). | `nfsstat -s` under load in N8 |
| V2 | The Raft log on an OS disk syncs within a few milliseconds, so grants and reservations are not slowed. | meta-soak timings on a production node |
| V3 | A claimant's lock request expires a dead owner's client state at about 11.5 s also when the owner's other volumes on that server are owned by the same node (expiry is client-wide, ha.md §10). | lockprobe in N0 |
| V4 | Reusing `META_OP_VOLUME_LEADER` with ISR = candidates needs no change to `meta_check_step` beyond the shared-volume rules. | test_meta and the meta-soak in S1 |
| V5 | Every NFS server restart changes the RDMA transport's `connect_count` in `/proc/self/mountstats` on every client, so the health monitor sees it (F6). | N0, scenario (e) |
| V6 | The read barrier adds at most one slot write and `fdatasync` (about 0.2-0.5 ms over RDMA) to an owner read on an idle volume, and nothing measurable under insert load. | N8, tsdbc-bench queries on a shared volume |
| V7 | The voters' disks honour FLUSH/FUA or have power-loss protection, so a majority keeps every entry it acknowledged across a site power loss (M12 rests on it). | A power-cut test (diskchecker style) on one voter host in S1 or N0; the operator guide names the requirement |
| V8 | An RDMA transport reconnect without a server restart is rare (a reconnect while SUSPECT makes the volume FENCED, M17, and costs a cold open). | Count reconnects in N0 and over N8's runs |
| V9 | A server restart that rolled the pool back always shows on the client as DOWN (a probe outstanding for `down_ms`), a reconnect, or ESTALE: re-importing a pool takes the export away for longer than `down_ms` or changes its file handles. | N8 run with `zpool export` / `import -F` on a test pool |
