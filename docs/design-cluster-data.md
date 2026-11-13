# tsdb cluster data: erasure coding, 3 replicas, volume moves and failover

Status: design, not implemented. Branch `cluster-data-design`, written on 2026-11-09 from main at 47ae317 (0.10.0),
**revised the same day after two reviews** (durability and consistency; operations and performance at 10-100 TB per
node): their findings are fixed below or answered with the reason, and §13 lists where. Code references are
`file:line` at 47ae317 for tsdb (design-cluster.md and design-ha-modes.md as edited on this branch), and at 9c5f97d2
(origin/main, pulled 2026-11-09) for intellistream-objects (written `objects:path:line` below). Target release:
**v0.11.0**. **The user answered its five open questions the same day** (2026-11-09): E32-E36. The fifth, cluster data
is **production** in 0.11.0 and not a preview (E36), brought in the production design of §14 (E37-E46: rolling
upgrades between builds of one format, maintenance-window upgrades across a format change, backups, alerts, the
operator guide, the release gates) and the steps P1-P6 of §11. **The user decided on 2026-11-09 to keep today's
compatibility rule** (E40): before 1.0 no promise across format, metadata or peer protocol versions; the draft's
"each minor release reads the previous one's files" is rejected.

**Amended on 2026-11-11 with device classes** (§15, E47-E58; branch `device-classes` from main at 84b28e4). The user
asked: "do we have tiered storage? So hot data can be on mirrored ssd, and cold data on erasure coded pool?" and, on
hearing that roles were per server: "yes do that, so one instance can cover both nvme and hard drives". Each disk of a
tsdbd now has a class (`nvme`, `ssd`, `hdd`); the young tier goes on the fast ones and settled chunks on the hard
drives of the same server, the role follows from the disks, and a single server (no cluster) can move settled
partitions to its hard drives too, erasure coded across them (E55). **Revised the same day after a review** (§15.11):
each disk is a fault domain of its own (E57), disks are set up explicitly (E58), and a log replica no longer moves
between two disks of one server.

The user's request (2026-11-09, verbatim): "cluster should support data replication, preferably using erasure code,
test with 4+2 and 8+2 and 8 + 4 and 16+4. I know we lose performance, but it makes operation much easier. Its a
tunable config. Also support replication with 3 replicas. We should support moving of volumes and fail overs, like if
one node is down for 30 minutes, we start moving the data." and "that will go in v0.11.0". Asked during the design:
"with this new cluster implementation, will tsdb support true seperation of storage and compute?" (§9, E20-E22).

**What this reverses.** [design-cluster.md](design-cluster.md) kept erasure coding out of the cluster: "Not here:
erasure coding, shard placement, ISA-L, degraded reads" (design-cluster.md:47, struck out on this branch) and C21, "Cold
tier: only the boundary here" (design-cluster.md:127). Erasure coding is now a first-class, tunable redundancy mode of
the replicated (shared-nothing) mode, beside 3 replicas. Everything else in design-cluster.md stands: the volume as the
unit (C1), the metadata group (C2, amended by E8 for m = 4), the WAL replicated by the leader to an ISR (C4-C7, C11),
failover and fencing (C12-C14), name slots, clients. It is extended, not replaced: an erasure-coded volume **is** a
replicated volume with m + 1 copies of its young data, plus an erasure-coded tier for its settled partitions (E3).

Related: [design-cluster.md](design-cluster.md) (replicated mode), [design-ha-modes.md](design-ha-modes.md) (the two HA
modes and the combined build plan; §7 there carries this design's steps R5-R14 and P1-P6), [design-cdc.md](design-cdc.md)
(positions, `fno`).

## Summary

- **Redundancy is a per-volume setting** with a cluster default: `replicas=3` or `ec=k+m` with k+m one of 4+2, 8+2,
  8+4, 16+4 (any k ≤ 16, m ≤ 4 is accepted; those four are tested). It can be changed on a running volume, as a
  background conversion in a fixed safe order (E2; in 0.11.0, E33). A new cluster defaults to `ec=4+2` with at least
  7 storage-capable servers, else `replicas=3` (E34).
- **Two tiers per volume.** Young data (the WAL, memtables and the parts of partitions still taking rows or merging)
  is replicated exactly as design-cluster.md says, on **m + 1** servers (3 in `replicas=3`). A partition whose day is
  over and whose merges are done is **settled**: each of its part files is erasure coded across k + m servers and the
  full copies are retired. Settled bytes cost 1.25x to 1.5x the data instead of 3x (5x for m = 4 young data).
- **The settled tier is design-cluster.md §9's cold boundary moved inside the cluster.** The unit is a whole part file,
  unchanged (granule offsets stay offsets); every log replica keeps the part's granule index locally (about 0.75% of
  the part), so queries answered from statistics fetch nothing. Decoding a granule reads its bytes from the server
  holding its cell, or, degraded, the same byte range from k others.
- **Settling is a replace at a cut.** The leader encodes the partition's parts (checking every granule as it reads
  them), puts the chunks, closes the partition to writes for milliseconds, and appends a marker naming a log position
  Z: at the commit point every log replica swaps its parts of that partition holding rows up to Z for the encoded
  objects plus the few files that arrived meanwhile. Last write wins as on one server; a replica whose parts do not
  fit the cut stops instead of guessing.
- **Failure**: a server down is degraded at once (reads decode, writes go on through the ISR, settles wait up to 2 h);
  down for `rebuild_after` (30 min) it is OUT and its log copies and chunks are rebuilt elsewhere, most endangered
  first, faster the smaller the margin; back before that, it only catches up, and back during the rebuild it takes its
  positions back instead of the data moving twice. With every ISR full, m servers down lose nothing; m + 1 down make
  the affected partitions **unavailable**, and data is reported **lost** only when more than m holders are gone for
  good. A dead disk rebuilds that disk, not the server.
- **What it costs**: writes as design-cluster.md for m = 2 (3 copies) and 5 copies for m = 4; settled reads cost one
  round trip when not cached, k reads when degraded; a lost 100 TB node takes about 9 h to rebuild on 8+2 at line
  rate and 4.6 days at the default pace (k times a replica rebuild), so the pace rises as the margin falls and m = 4
  is recommended above 20-30 TB a node (§5.3).
- **Storage and compute can be separated** (E20): `compute` servers lead volumes, hold the replicated WAL, young parts
  and indexes and cache granules; `storage` servers hold chunks only; `both` is the default. They scale independently;
  a compute server holds terabytes (young data and indexes), not gigabytes.
- **Device classes** (§15, E47-E58): `tsdbd --disk PATH[:nvme|ssd|hdd]`, repeated, each disk set up once by `tsdb
  disk init` (the class detected when not given). NVMe and SSD are **fast** disks: WAL, young parts and `.ec` index
  files. Hard drives are **capacity** disks: chunks. One server with both is compute and storage at once; the role
  follows from the disks (`--role` only narrows it). A server with hard drives never puts new chunks on its NVMe; an
  NVMe-only server does. Each disk is a fault domain: a dead or hung disk stops what is on it, not the server. Inserts
  are refused before a fast disk fills, per disk. A single server gets the same split: `tsdb DIR tier add /hdd/x`, and
  partitions a week past their end move to its hard drives, erasure coded across them (a mirror on two), their index
  staying on NVMe (E55).
- **Codec**: systematic Cauchy Reed-Solomon over GF(2^8) with SSSE3/AVX2/AVX-512/GFNI kernels: intellistream-objects'
  `src/codec`, copied unchanged into `src/ec/` by `tools/sync-ec.sh`. Both repositories are AGPL-3.0-only from 0.11.0,
  so no grant is needed; the Apache-2.0 network clients never contain it (E32).
- **Production in 0.11.0** (E36): rolling upgrades one server at a time between builds whose disk format, metadata
  snapshot version and peer protocol version are the same, with a cluster version the operator raises once every
  server runs the new build (E37, E38, E41); a release that changes any of the three is upgraded cluster-wide in a
  maintenance window (stop, `tsdb DIR upgrade` or export/import, start; E39, E40, §14.2); physical, incremental backups per volume (E42); alerts shipped and tested (E43); an
  operator guide whose runbooks are tests (E44); and release gates of a 72-hour soak, jepsen-cluster on every
  geometry and an upgrade test (a dummy bump within the same formats, and the refusal across a format change; E45). Not production in 0.11.0: shared NFS mode and the
  S3/isod backend (E46, §14.6).

## Assumptions

| # | Assumption | If it does not hold |
|---|---|---|
| EA1 | design-cluster.md A1-A5 hold (own disks, 25 GbE, one data center, NVMe for the WAL). | As there. |
| EA2 | Most bytes are in partitions that end and stop changing: dense series in day partitions (class 0, AGENTS.md "Partition classes"). Slow series in 7-364-day partitions hold a minority of the bytes. **Not measured** (EU5): the production target is 10^9 series, many slow. | Coarse keys settle part by part (E5), so they do not stay at m + 1 copies for a year; if late or back-dated writes keep old partitions busy, more data stays replicated and E6's later objects bound it. |
| EA3 | Settled data is read less than young data, and mostly through granule statistics. | If decode-heavy queries over old data dominate, raise the granule cache or `ec_keep_local_h` (§3.3). |
| EA4 | One failure domain for placement is one server (one chunk of an object per server); racks when labelled. A server has several disks, each its own chunk store (E27): a disk is a smaller failure inside its server. | Several chunks of an object per server would make a server loss cost several chunks; refused (E9). |

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| E1 | **Redundancy per volume**, `replicas=3` or `ec=k+m` (k ≤ 16, m ≤ 4; 4+2, 8+2, 8+4, 16+4 tested), default from the cluster setting `redundancy` (`tsdb cluster init --redundancy`), overridable at `volume add`. | Per cluster only; per series | Volumes already carry per-volume options (`min_isr`, D1); mixing lets a small, hot, high-value volume stay on 3 replicas beside bulk EC volumes. Per series would need a map the size of the data. |
| E2 | **It can change on a running volume** (`tsdb cluster volume redundancy V ec=8+4`), in a fixed order (§6.4): a larger m first grows the log group and its ISR, then claims the new m; new settles use the new geometry; a background job re-encodes older objects at rebuild pace (about 5 days for 1 PB on 11 servers); a stripe position is dropped only when no live object names it (`STRIPE_SET` refuses otherwise); `ec` to `replicas=3` de-settles (decodes every object into full parts on every log replica) before the stripe group empties. Each object records its own geometry. | Fixed at creation; moving series to a new volume | Series ids name their volume (AGENTS.md "Series ids"), so data cannot move to another volume. Recording the geometry per object (as objects records it per manifest entry, objects:src/recovery/rebuild.c:26-28: "Everything comes from the manifest entry and never from today's pool") makes the change safe at any moment; the order keeps the volume's tolerance at the lower of the two geometries, never below it. |
| E3 | **Two tiers.** Young data (WAL, memtables, unsettled parts) is replicated on the volume's **log group** of m + 1 servers (3 for `replicas=3`) by the unchanged replication core; settled partitions are erasure coded across the volume's **stripe group** of k + m servers. | EC every part at flush; EC the WAL; stripes packed across parts; one object per series | See §2.1. In short: flush parts are small and merges rewrite them several times, so EC at flush would ship and re-encode every merge level over the network and lose C9 (each replica merges on its own); a WAL record must be durable on ack, and EC would need k + m syncs per record instead of m + 1; packing parts into shared stripes needs stripe garbage collection after deletes; 10^9 series make per-series objects impossible. |
| E4 | **m + 1 copies of young data** for `ec=k+m`. With the ISR full, every byte of the volume survives m losses, acknowledged WAL rows included. An acknowledged row is guaranteed to survive only `min_isr` − 1 losses: a slow replica leaves the ISR after `isr_lag_ms` (design-cluster.md:551), so the young tier's live margin (ISR size − 1) is reported beside the stripe margin (E24). | 3 always; fewer | The same number of losses (m) as the code, while the ISR is whole (design-cluster.md A2 with m in place of 2). The honest bound is the ISR's, which E24 sets. |
| E5 | **A partition key settles** when, by the replicated clock (`MK_NOW`, design-cluster.md §5.4, never the leader's wall clock): its end (`end_ms`) is `settle_grace_s` (3,600 s) in the past; no merge of it is pending and merge selection would combine none of its parts; every stripe position is UP (or E10's wait has run out); the memtable holds none of its rows (else it waits for the next natural flush, and forces one only after `settle_force_s`, 1 h); and no settle of it is running. **Coarse keys** (classes 1-3, 7-364 days) do not wait for their end: a part whose newest row is `settle_grace_s` old settles once merges leave it alone, at most once per `settle_coarse_s` (7 days) per key. Late rows do not block a settle. | One part only (§9.1's rule, design-cluster.md); §9.1's idle rule (`cold_idle_s`, 1 day); coarse keys only at their end | Merges never make a part above `max_part_bytes` (4 GiB, src/merge.c:103, src/db.c:62), so a key with more than 4 GiB of rows would never be one part and never settle: at 100 TB a node that is most keys. Waiting for idleness only keeps more data at m + 1 copies. A 364-day key would stay at m + 1 copies for a year. A forced flush per settle would add small parts on every replica for an hour after midnight. |
| E6 | **Settling is a replace of an input set at a cut** (§2.3): the leader pins the key's parts (the input set I, one object per part), encodes and puts them, then closes the key (a settle gate for large writes, the fence for small ones), waits until everything appended before is committed, and takes the cut Z (a log position). The marker names Z; the remainder `Y` is every other part of the key the leader holds, all with rows at or below Z. Every log replica retires **its own non-`.ec` parts of the key whose part position is at or below Z**, and the objects the marker names (a re-encode's inputs), never others. A part of the key with a position between Z and the marker breaks the gate: the replica stops (DIVERGED) and retires nothing. Later settles of the same key add objects above; a re-encode combines a key's objects when it has more than `ec_objects_max` (4) or tombstones cover more than 10% of them, by the same protocol. | Retiring "every older part" (the first draft: it retired acknowledged writes that were in neither the object nor `Y`, and later objects); retiring by block range | Replicas flush and merge on their own (C9), so their parts differ from the leader's; the cut makes "the rows of the key at or below Z" one well-defined set on every replica, and the part position says exactly which parts hold only such rows. |
| E7 | **The object is the part file unchanged**, laid out in **cells** dealt round-robin over the k data chunks; parity per stripe of k cells. The cell size is chosen per object, `clamp(L / (8k), 64 KiB, 1 MiB)` rounded to 4 KiB, and recorded in it; small objects use k' = min(k, cells) data chunks. | Contiguous chunks (L/k each); per-granule objects; one cell size for all | A series' granules lie together in a part (sorted by series, ts: src/part.c:1-2), so contiguous chunks would put a series' whole scan on one server; cells spread a partition scan over k servers and keep a granule (about 10 KB at 8,192 rows) in one cell 99% of the time at 1 MiB. Parity of a short last stripe is stored full size, so a 17 MiB object on 16+4 with 1 MiB cells would cost 1.47x, not 1.25x; at least 8 stripes per object keeps it within about 3% of the nominal. k' for small objects is objects' "derived k" (objects:src/codec/geometry.h:8-18) without a mirror rule: a one-cell object is m + 1 copies by construction. |
| E8 | **Metadata voters: 5, or 2m + 1 (at most 9) when a volume has m > 2**; amends C2's "at most 5". | 5 always | With m = 4 the data survives 4 lost servers, but 5 voters lose quorum at 3, and without quorum no ISR shrinks and no leader is promoted. Raft is off the insert path (C4), so 9 voters cost only metadata latency. (Was open question 4 of the first draft; decided here.) |
| E9 | **Placement**: the log group on m + 1 distinct compute-capable servers, the stripe group on k + m distinct storage-capable servers, one position per server, at most ceil((k + m) / racks) positions per rack; a rack loss is survivable only when that is ≤ m, and health says so. Lists are explicit (C19), positions never renumbered. Inside a server, a chunk lives on one of its disks (E27). | Two chunks of an object per server; computed placement | Objects' rules (objects:src/placement/README:144-160, one target per host, the rack cap; :92, the rack-survival condition). Positions are like objects' PG ids, never reused (objects:src/placement/README:134-135), so an object names positions, not servers, and a move rewrites no object. |
| E10 | **Settles wait for a whole stripe group for up to `settle_wait_max_s` (2 h)**, then settle **degraded**: the k + m − 1 reachable positions are written (never fewer), the missing one is recorded in the object and queued for rebuild at once. Just before the marker, every position written must still be UP in the current epoch, else the settle waits. | Waiting without bound (the first draft); objects' `min_write_shards` k + 2 (objects:src/codec/geometry.h:94) | Unbounded waiting with holds (E12) and the mass-OUT guard stops settles on almost every volume (8+2 on 11 servers: 10 of 11 servers in every stripe group) while young data grows by ingest × time × (m + 1) on the compute disks, and ENOSPC on the WAL is fail-stop. A degraded object at birth has margin m − 1, the same as an object whose holder died a moment after a full settle. One missing position at most keeps an m = 2 object above margin 0. |
| E11 | **Reads**: index local on every log replica; block bytes through one seam (`granule_read`, src/part.c:313) that reads the granule cache, the local data chunk, or the cell's server; a missing, slow or failing cell is decoded from k others **over the granule's byte range only**. Every granule read from an `.ec` part (local chunk, remote, cached or decoded) is checked against its index checksum, whatever `verify_checksums` says. When a decoded granule fails, the reader reads the whole cells of that stripe and finds the bad survivor by the encode-time cell checksums (E26), never by trying candidates against the 32-bit granule checksum. | Whole-cell decode always (objects' gateway, objects:src/gateway/pipeline.c:495); trusting `verify_checksums`; trial decode chosen by a 32-bit sum | Reed-Solomon works byte column by byte column, so a 10 KB granule needs 10 KB from each of k cells, not k MiB. A wrong survivor gives a plausible wrong answer (objects:src/recovery/rebuild.c:21-25), and "no non-cryptographic digest may be what chooses" (objects:src/recovery/rebuild.c:41-42): a 64-bit sum per 1 MiB cell computed at encode identifies the bad cell. |
| E12 | **Failure states as objects': DOWN, then OUT** after `rebuild_after_s` (1,800 s; replaces design-cluster.md's `out_after_s`, :317). A server whose heartbeats say it is **opening** (WAL replay, part checks) is held from OUT up to `opening_hold_s` (3,600 s) more; a server that reported a **fail-stop** goes OUT after `failstop_out_s` (60 s), unless held. Operator holds (`node hold N --for 2h`, at most 1 day) pause the timer; no automatic OUT while a whole rack, or more than 25% of the servers, is down, for at most `out_guard_hold_s` (2 h by default, at most 1 day), then OUT goes ahead with an alert. | Rebuilding at once; no guard; an unbounded guard (the first draft) | "The timer between them is what stops a reboot from triggering a terabyte of copying" (objects:src/cluster/cmap.h:67-70); the holds and the mass-OUT guard (objects:src/cluster/cmap.h:168, :895-925; default 25%, objects:src/mds/server.h:89-93) keep a power or switch event from starting a rebuild storm; their bounds (objects:src/mds/server.h:89-100: hold at most a day, opening 1 h, fail-stop 60 s) keep a rack that is really gone from sitting below margin forever, and a fail-stopped tsdbd (exit 1) does not come back on its own. *2026-11-11*: `failstop_out_s` applies to the device whose disk failed; a server goes OUT by it only for a fail-stop of `--state-dir` or of the process (E57). |
| E13 | **Rebuild order: most endangered first** (fewest copies or chunks left beyond the minimum: k for EC, 1 for replicas), then balance moves. **Pace by margin**: margin 0 rebuilds at full disk and network speed and is exempt from design-cluster.md §8.4's halving; margin 1 of an m ≥ 2 volume gets at least 4 × `rebuild_mb_s`; the rest `rebuild_mb_s` (200 MB/s per server) under §8.4's token bucket. A plain copy from a holder that is back is preferred over decoding. | FIFO; decode always; one pace | Objects' decision of 2026-10-20 (objects:src/mds/server.h:1852-1860): a 4+2 object two short goes before a 16+4 object two short. A copy reads 1x, a decode kx. An EC rebuild reads k times a replica rebuild's bytes, so at one pace it takes k times as long (§5.3): the exposure window, not the number of losses, is what lowers the mean time to data loss. |
| E14 | **Chunk writes and deletes are fenced by the volume epoch; deletes go through a committed marker.** A chunk header carries its writer's epoch. A storage server keeps, per volume and durably, the highest epoch it has seen in any request (not only from the metadata watch) and refuses a put or delete below it, and a delete below the chunk's own epoch. Chunks are deleted only as listed by a committed `MK_GC` marker. An object is an orphan only when no record in the deleter's log names it (committed or not), it was written in an earlier epoch than the deleter's, the deleter's `EPOCH` marker is committed, and it is not one of the deleter's own settles in flight. Orphan collection is suspended after `force-leader` or a restore until the operator resumes it. Reads are not fenced. *2026-11-11*, with one `EPOCH` per chunk store (E48): the fence is the maximum over the server's stores, kept in memory and taken over the stores present at start; before acting, the target store's `EPOCH` is raised (and synced) to at least that maximum; a new store is seeded with it at setup. | Fencing reads; trusting the sender; deleting what no committed marker names (the first draft) | Objects are immutable and checksummed, so a read from any holder is safe. A deposed leader cannot commit `MK_GC` (C11), so it can only leave orphans; a newly promoted leader whose old tail is not yet committed would otherwise delete an object whose `MK_COLD` is about to commit, and every replica would then retire its full copies. |
| E15 | **Three outcomes, never confused**: **unavailable** (more than m positions of a stripe DOWN or unreachable: UNAVAILABLE, retryable, reason `TSDB_WHY_UNREACHABLE`); **corrupt** (enough positions present but checks fail beyond what decoding repairs: `TSDB_ERR_CORRUPT` naming the partition and granule; a repair is scheduled); **lost** (more than m positions OUT whose holders the operator declared destroyed, or whose stores came back empty: `TSDB_ERR_IO` naming the partition, the volume `LOST` in TOPOLOGY, UNAVAILABLE reason `TSDB_WHY_LOST` with "do not retry"). Queries answered from statistics still answer over lost partitions (the index is local and intact). `tsdb cluster volume salvage V` drops lost objects by oid (`MK_DROP_OBJ`), keeps the young parts, and refuses while an OUT holder may still return, unless `--force`. | One error for all three; salvage by `MK_DROP` (it would retire the surviving young parts: acknowledged writes thrown away) | AGENTS.md "Damage is never silent"; a partition is not lost while its holders may return (objects keeps a DESTROYED state for this, objects:src/cluster/cmap.h:76). |
| E16 | **Shared NFS mode has no EC.** `redundancy` is replicated mode only; a shared-mode cluster refuses it. | EC over NFS | ZFS already gives the redundancy under NFS; one copy on shared storage is shared mode's point ([design-ha-modes.md](design-ha-modes.md) §1). |
| E17 | **No new disk format number.** Chunk files, `.ec` index files, the part position in MANIFEST entries, and the markers `MK_COLD` (11, reserved in format 9) and `MK_GC`, `MK_DIGEST`, `MK_DROP_OBJ`, `MK_UNSETTLE` (12-15) live only in cluster volumes behind `requires=cluster,ec` in META (format 9's mechanism, src/feedfile.c:385, src/db.c:437); they freeze at 1.0 with the rest. | Format 10 | Nothing in a single-server volume changes; the `requires=` line refuses the files in a binary that does not know them. *2026-11-11*: a single-server volume with a local tier (E55) gets `.ec` files, `ppos`, the settle markers and the settle intent too, behind `requires=tier`; one without a tier is unchanged. |
| E18 | **The metadata snapshot becomes version 3** (redundancy, two member lists per volume, roles, holds, stores, DESTROYED), and the peer protocol version 3 (chunk messages). *Decided 2026-11-09* (E36, E39, E40): no promise of in-place upgrade across snapshot versions before 1.0 (the pre-1.0 rule is kept); a later release that changes the snapshot version says in its CHANGELOG how to carry a cluster over (E39), and 0.10.0's preview group (version 2, no data path) is initialised again. | Silently keeping the preview rule; every snapshot version converted in place from 0.11.0 on | From 0.11.0 the cluster holds data, 10-100 TB a node: a format change then costs a maintenance window (E39), and the user chose not to carry a reader for old versions before 1.0 (E40). |
| E19 | **The EC codec is systematic Cauchy Reed-Solomon over GF(2^8), polynomial 0x11d**, kernels dispatched by tsdb's `src/cpu.c` (no constructors: AGENTS.md "CPU levels"), every kernel byte-identical to the scalar one. | ISA-L as a dependency; LRC or other codes | Interoperable with Jerasure, ISA-L and klauspost (objects:src/codec/gf256.h:1-5); any k of k + m reconstruct, with no unlucky sets (objects:src/codec/rs.h:11-15). Locally repairable codes save rebuild reads but are not MDS at the same space; a later change if rebuild reads hurt (§5.3). Where the code comes from: E32. |
| E20 | **Storage and compute can be separated**: server roles `compute`, `storage`, `both` (default). §9. *Amended 2026-11-11 by E49*: what a server can do follows from its disks' classes; `--role` only narrows it. | Every server both | The user's question. It costs little (two member lists exist anyway, E3) and lets bytes and CPU scale apart. |
| E21 | **A compute server's state is replicated, not small**: the young tier (a day or more of ingest at m + 1 copies), the `.ec` index files (about 0.75% of settled bytes, on each of the m + 1 log replicas) and caches. Losing one loses nothing durable (while the ISR is full, E4); rebuilding its log replicas is design-cluster.md §8.1's rebuild of terabytes: hours, spread over the other compute servers. A separated cluster needs at least **m + 2 compute servers**, so a lost one has somewhere to be rebuilt. | Stateless compute nodes (WAL on storage nodes); the index on fewer log replicas | A WAL on remote storage adds a round trip to every acknowledged insert; m + 1 local NVMe copies are the replicated mode that already exists. The index on every log replica keeps statistics queries warm right after a failover; at 0.75% its copies cost less than one more data copy of the young tier. |
| E22 | **The settled tier is an interface with backends**: the cluster's own storage servers (0.11.0), a directory (tests), and later isod or any S3 (design-cluster.md §9.2's `tsdb_cold_ops`). | Only the own backend | With an S3 backend, tsdb servers become compute only and isod does the erasure coding and its rebuild. That is the "true separation" end state; it waits for 0.12 or later (§9.4). |
| E23 | **0.11.0 holds replicated mode's data path (R2-R4), this design (R5-R14, R10 included: E33) and the production steps P1-P6 (E36)**; shared mode's steps go on in parallel and ship when their maintenance windows allow (not as production in 0.11.0, E46). | Waiting for N7 (M21's order) | The user's "that will go in v0.11.0". M21 put replicated mode after shared mode; this changes the order, not the content. Nothing moves to 0.12 (E33). |
| E24 | **The young tier's ISR floor for EC volumes**: `min_isr` defaults to **m** for `ec=k+m` volumes (2 for m = 2, 4 for m = 4), so an acknowledged row survives at least m − 1 losses at worst and m with the ISR full; `replicas=3` keeps D1's 1. TOPOLOGY and health report both margins: the young tier's (ISR size − 1) and the settled tier's (fewest positions beyond k). | D1's 1 for every volume | The user chose m = 4 to survive 4 losses; with `min_isr = 1` one slow node (or two on 8+4) leaves writes acknowledged on one copy. The cost: an EC volume refuses writes when more than one of its log replicas lags (of 3 for m = 2, of 5 for m = 4). Confirmed by the user on 2026-11-09 (E35). |
| E25 | **Scrub every chunk every `scrub_days` (14)**, paced by `scrub_mb_s` (50 MB/s per disk) below rebuild: each storage server reads its chunks and checks every cell against the encode-time cell checksums (E26); log replicas scrub `.ec` files and young parts the same way (`verify_checksums` reads). A bad cell is repaired by a range decode through the rebuild path. Damaged chunks are counted per disk; a disk with more than `scrub_out_count` (8) in a day goes OUT (E27). Health shows the oldest unscrubbed object. | Scrub on demand; checking parity against data across servers | Systematic reads never touch parity, and range reads cannot check a 1 MiB cell's sum, so rot in parity or unread data stays hidden until a degraded read or a rebuild needs it: at margin 0 that is loss. Encode-time sums make a local check sufficient: a cell that matches its encode-time sum is what the encoder produced (objects counts damage per device the same way, objects:src/cluster/cmap.h:169-170). |
| E26 | **Checks at every hand-over.** The encode checks every granule checksum and the footer hash of each part it reads (it reads every byte anyway) and aborts on a mismatch (the part is repaired from a replica first, design-cluster.md §8.5); it computes a 64-bit sum per cell for **every position**, data and parity, sends each position its sums with the put, and records all of them in the object record; a chunk server checks the cells it receives before PUT_OK and the leader compares each PUT_OK's sum. After the settle, every log replica compares a **logical digest** of the rows it retired (rows per series and an xxh64 over the visible rows in (series, ts) order) with the leader's (`MK_DIGEST`), before it deletes them; a mismatch keeps the retired parts and raises DIVERGED. Retired parts of a settle are kept `settle_keep_s` (1 day), not `obsolete_part_lifetime_s`. A rebuilt cell is checked against its encode-time sum, never self-certified. | Trusting the leader's bytes (the first draft); a digest on the apply path | Granule payloads are hashed only with `verify_checksums` (default off; src/part.c:314-318), so rot in the leader's part would be encoded with fresh, matching sums while the good replica copies are retired; and a merge bug on the leader would become the only lineage. The digest costs one decode of each settled partition per log replica, in the background (EU6). |
| E27 | **A storage server holds one chunk store per disk** (`--chunk-dir`, repeated; each with its own META and store id; *2026-11-11*: declared with `--disk PATH[:CLASS]` instead, E47, chunk stores only on capacity disks where a server has any, E50). A position stays one per server; each chunk lives on one of its disks, in the server's local index. A dead disk is a **device** DOWN/OUT: only that disk's chunks are rebuilt, onto the server's other disks when they have room, else the server's positions move by the balancer. `tsdb cluster disk out|replace NODE DISK`. | One file system over all disks (one disk loss is the whole server); RAID under the chunks (redundancy on redundancy, the 1.25x is no longer 1.25x) | At 1-2% a year per disk, a 20-node cluster of 24-disk servers loses several disks a year: the most frequent hardware event. Objects does it per device (objects:src/cluster/cmap.h:66-77; timers per device, objects:src/mds/server.h:84-100). |
| E28 | **A server back during a rebuild takes its positions back**: for each position whose replacement is not yet `whole`, a returned holder whose stores are intact gets it back with one `STRIPE_SET` conditional on `stripe_version`, and copies only what was settled while it was away; the partial fill is deleted. Where the replacement is already whole, it gets the position back only when that saves a balance move at an unchanged margin. `tsdb cluster node replace OLD NEW` gives OLD's positions to NEW directly, filled once by copy or decode. | The first draft: the returned server only serves as a copy source, then deletes everything, and the balancer moves 1/N of the cluster back onto it | A 35-minute firmware update without a hold would otherwise move the node's data twice (out by decoding at k times the reads, then back), days of traffic at reduced margin. Ceph does the same when an OSD returns during backfill. |
| E29 | **Segments are retained for a DOWN or held member** of the log group until it goes OUT or its hold ends, within `repl_retain_down_gb` per server (sized by §4's rule: ingest × `rebuild_after` × WAL bytes per row), beyond design-cluster.md §8.3's 30 min and `repl_retain_mb`. | §8.3's budget alone | At high ingest 4 GB lasts minutes, so "back within 30 minutes only catches up" would not hold, and a planned 2-hour hold would cost a snapshot of terabytes of young data. |
| E30 | **Backpressure before ENOSPC**: a log replica above `young_high_pct` (90%) of its WAL and young-data file system makes its leader refuse inserts with UNAVAILABLE reason `TSDB_WHY_FULL` (4, protocol 13) until it is below again; the operator guide sizes compute disks (§4). *2026-11-11*: measured per fast disk, and only the volumes with a log replica on the full disk are refused (E53). | Letting the WAL reach ENOSPC (fail-stop on every compute node at about the same time) | Young data grows when settles wait (E10, E12) and every compute node fills at the same rate; refusing is retryable, fail-stop is not. |
| E31 | **Hedged reads per server**: a cell read is hedged after `2 × that server's read p99` (floor 20 ms, ceiling 500 ms), not a fixed 50 ms; hedges are capped by a token bucket at 5% of reads per server; a server with 8 timeouts in 10 s is `read-avoid` for 30 s (decoded around without waiting). | A fixed 50 ms timeout with k reads per hedge (the first draft) | On HDD storage servers (§9.1) queue p99 under rebuild or scrub is routinely above 50 ms; each hedge reads k cells (16 on 16+4), raising latency further: a feedback loop that can outlive its cause. |

**Decided by the user on 2026-11-09** (the five open questions of the revised draft; E32-E36), and what production
then required (E37-E46, decided in this revision; the user may change them):

| # | Decision | Rejected | Why |
|---|---|---|---|
| E32 | **The EC codec is intellistream-objects' `src/codec`, copied unchanged** (user, open question 1): `gf256.c`, `gf256.h`, `gf256_avx2.c`, `gf256_avx512.c`, `rs.c`, `rs.h` (1,143 lines) into `src/ec/` below an AGPL-3.0-only header by `tools/sync-ec.sh` (as `tools/sync-raft.sh` does), which writes `src/ec/UPSTREAM` (repository, commit, licence); what they include from objects' `common/` is tsdb's own `src/ec/compat.h`. gf256.c's start-up constructor (objects:src/codec/gf256.c:469, which AGENTS.md "CPU levels" forbids) is replaced **upstream** by an explicit selection call that tsdb's `src/cpu.c` makes, so the copy stays byte for byte. The codec is in `libtsdb` and tsdbd (AGPL-3.0-only, the embedded bindings too). The Apache-2.0 network clients never contain it: settled data is decoded on servers only (a later direct-mode client routes to log replicas, never to storage servers), `tools/check-spdx.sh` already refuses an Apache-2.0 file that includes an AGPL one, and R5 adds a check that `libtsdbclient.a` and the client packages hold no `gf256_` or `rs_` symbol. docs/licenses.md lists the copy. | A second grant under Apache 2.0 (the revised draft's recommendation, moot since the relicence); a patched copy, the patch re-applied at every sync; our own codec from the papers (2-3 more track-days) | From 0.11.0 tsdb's server and engine are AGPL-3.0-only (the relicence, merged into main on 2026-11-09) and objects is AGPL-3.0-only with the same owner, so copying needs no grant. An unchanged copy makes `git diff src/ec` after a sync exactly the upstream change; a patch kept in tsdb would be re-applied and re-reviewed each time. The clients stay Apache-2.0 so that programs that only talk to tsdbd are not AGPL works (LICENSING.md). |
| E33 | **0.11.0 holds everything, R10 included** (user, open question 2: "even if 0.11.0 takes longer"): the prerequisites, R5-R14 with the redundancy change of a running volume (§6.4), and P1-P6. No cut line. | The recommended cut (R10 to 0.12; volumes keep the redundancy they were created with) | The user's choice; it costs R10's 3 track-days and 6 gate hours, counted in §11. |
| E34 | **The default redundancy of a new cluster** (user, open question 3, as recommended): `cluster init` sets `ec=4+2` when it sees at least 7 storage-capable servers, else `replicas=3`, and prints which and why; `--redundancy` overrides. The default is fixed at `init` and changed only by `tsdb cluster set redundancy` (for volumes added later; existing ones change by E2). The operator guide recommends m = 4 above 20-30 TB of chunks a node (§5.3). | Re-evaluating the default whenever servers join | 4+2 needs 6 distinct servers and a seventh to rebuild onto (objects' room-to-rebuild rule, objects:src/placement/README:84-99); below that, 3 replicas survive the same 2 losses with room to rebuild. A default that followed the member count would give volumes added on different days different redundancy without anyone asking. |
| E35 | **`min_isr = m` for `ec=k+m` volumes** (user, open question 4, as recommended): E24 as written; `replicas=3` keeps D1's 1. | D1's 1 for every volume | E24. |
| E36 | **Cluster data is production in 0.11.0** (user, open question 5), not a preview. What that promises, honestly (E40): on a 0.11.0 cluster, **rolling upgrades without stopping the cluster work within a format**, that is between builds with the same disk format, metadata snapshot version and peer protocol version; **a release that changes any of the three needs a maintenance window** (stop, upgrade command or export/import, start; E39) until the formats freeze at 1.0. 0.11.0 therefore holds (§14, P1-P6): rolling upgrades with the compat check (E37, E38, E41), the window procedure and its refusals (E39, E40), backup and restore (E42), shipped alerts (E43), the production operator guide (E44) and the release gates of E45. | The recommended preview for test data, with only the metadata upgradable; the draft's claim that data reaches later versions in place without stopping the cluster (rejected with E40) | The user's choice. At 10-100 TB a node, production means the failure, backup, upgrade and monitoring paths are tested; it does not mean formats stay readable across releases before 1.0, which the user decided on 2026-11-09 to keep as today's rule. |
| E37 | **Rolling upgrades only between builds of one compat triple; a cluster version says which behaviour is in force.** Every build has a **compat triple**: its disk format (`FORMAT_VERSION`), its metadata snapshot version and its peer protocol version (`CL_VERSION`); a client protocol change counts as a peer protocol change, because forwarding carries client ops. `tsdbd --compat` prints it, HELLO and heartbeats carry it, and the metadata group records the cluster's at `init`. A peer link is refused when the triples differ (both logged); a server whose directories or state files are of another triple refuses to start (the open's existing format check, naming both versions). Builds with the same triple (every patch release, and a minor release that changes none of the three) mix freely. The metadata group holds a **cluster version** (op `CLUSTER_VERSION`), set at `init` and raised only by `tsdb cluster upgrade finish`, refused until every member that is not OUT or DESTROYED reports the new build in its heartbeats (`--without NODE` lets a DOWN member be upgraded before it rejoins). It gates **behaviour, never a format**: a rule every server must apply together (a placement, settle or scrub change) takes effect only after finish, so that until then any server can go back to the old build, and a binary older than the cluster version refuses to start, naming both. | One version per cluster (design-cluster.md §11.2 until now: every upgrade a whole-cluster restart); finishing automatically when the last server reports the new build; N and N + 1 formats running together with a reader one step back (the draft, rejected with E40) | CockroachDB's cluster version and Ceph's `require-osd-release` follow the same pattern; with no format difference, the only thing left to gate is behaviour, and the operator keeps the last rollback point until finish. The compat triple makes the unsupported case a refusal at the link, not a corrupted read. |
| E38 | **One server at a time, by `tsdb cluster node prepare N`, only within one compat triple.** `tsdb cluster upgrade check --to FILE` (the new `tsdbd`, which prints its triple) compares it with the cluster's recorded triple, and `node prepare N --to FILE` runs the same check first: when the triples differ it refuses, prints both, and says "this release needs a maintenance window: see its CHANGELOG and the guide's runbook"; nothing is held or moved. Otherwise it refuses unless every volume N is in keeps a margin of at least 1 in both tiers without N (no other member of its log or stripe groups DOWN, every ISR full) and no other server is prepared and not yet back; then it holds N from OUT for `--for` (30 min by default, E12), moves N's volume leaderships away by planned transfer (design-cluster.md §3.2), hands the Raft leadership on if N has it, and reports ready. The operator stops tsdbd, installs, starts; N rejoins, catches up from retained segments (E29), and is back when it is in every ISR and stripe group again. `tsdb cluster upgrade status` lists builds, triples, the prepared server and the margins. While builds are mixed, `volume redundancy` and operator re-encodes are refused; failure handling, settles, rebuilds and scrub go on. A mixed cluster is supported without a time limit, but health warns after 24 h (E43). | Restarting on DOWN timers and holds alone; several servers at once (a rack at a time); no limit on what runs while builds are mixed; letting a rolling upgrade start and fail at the first link | A restart is a planned failure: checking the margin first means an upgrade never takes a volume to margin 0, and moving leaderships first means no client waits for a failover. Checking the triple first means a window-only release is found before a server is stopped; the link and open refusals of E37 are the backstop if an operator skips the check. A rack at a time is faster but takes m positions of every stripe group that spans the rack at once. A conversion running across builds would double the states the upgrade test must cover. |
| E39 | **A format change is a cluster-wide maintenance window, never a rolling upgrade.** When a release changes the disk format, the metadata snapshot version or the peer protocol version, its CHANGELOG says so and names the carry-over, as AGENTS.md's "Versions" and "on-disk format" paragraphs already require: (1) `tsdb cluster backup` (E42), (2) stop every tsdbd (clients get UNAVAILABLE and retry; the Pulsar sink buffers), (3) install the release, (4) carry the data over: `tsdb DIR upgrade` on each volume directory where the release ships one (today's in-place step for a database closed cleanly by the previous release), a metadata conversion the release ships (run offline on every member from the same snapshot and log, which gives the same result on each; the members compare its checksum before they start) or, where there is no converter, **export/import** into a freshly initialised cluster (`tsdb export` and `import` name series, not ids; `restore-data` reads the backup's parts and so only the format that wrote them), (5) start every server, (6) `tsdb cluster upgrade finish`. Where the format and metadata version are unchanged and only the peer protocol moved, steps 3 and 5 suffice. Golden files in `tests/data/` (`meta-snap-v3.bin` from 0.11.0) are kept for the converters a release ships, not for a reader of every old version. The per-server state (`--state-dir`, REPL, chunk store META, `EPOCH`) carries its version and is refused on a mismatch. 0.10.0's metadata group (snapshot version 2, a preview without a data path) is not converted: it is initialised again, as 0.10.0's CHANGELOG says. | A conversion at one log index while the cluster runs (the draft, rejected with E40); converting each member's snapshot as it loads | Before 1.0 the formats still move, and carrying a reader for the old ones would mean testing two of everything; a window with a backup, a converter or export/import is the honest cost, and the user chose it (E40). Converting offline from one input makes the members' states identical without a log index. |
| E40 | **No compatibility promise across format or metadata versions before 1.0 (user, 2026-11-09).** Today's rule stands (AGENTS.md "Versions" and the on-disk format paragraph): until 1.0 older formats need not stay readable; a layout change bumps `FORMAT_VERSION`, goes in the changelog with a way to carry data over (`tsdb DIR upgrade` or export/import), and a database of another version is refused at open naming both versions. The same holds for the metadata snapshot version and the peer protocol version of this design. Rolling upgrades (E37, E38, E41) work only between builds of the same compat triple; every other release is a maintenance window (E39). **AGENTS.md is not changed by this design.** | The draft's rule that each minor release opens the previous one's files in place (a version in every file or header, both forms read, the new one written after `upgrade finish`, AGENTS.md changed); every older format readable forever before 1.0 | The user's decision of 2026-11-09, after the draft proposed one step back from 0.11.0: the formats are still moving, and a reader for the previous version in every part, MANIFEST, WAL record, marker, chunk, `.ec` and metadata file is the cost the user did not want before 1.0. The price is a window for a format change, which is cheap against a cluster that cannot run mixed formats safely. At 1.0 the formats freeze and this is revisited. |
| E41 | **Clients during a rolling upgrade**: the client protocol is part of the peer protocol version for this purpose (E37), so every server of a rolling upgrade announces the same client protocol in HELLO, and forwarding never carries an op the next hop does not know. A release that raises the client protocol is a window upgrade (E39), with the clients and the sink upgraded together, the servers first, as docs/compatibility.md says. | Each server announcing its own protocol; a promise that clients of N keep working against N + 1 (the draft) | A client protocol change is already free before 1.0 (AGENTS.md "Protocol"); tying it to the peer protocol keeps a forwarded op from reaching a leader that does not know it. |
| E42 | **Backup is physical, per volume at a cut, and incremental** (§14.3): `tsdb cluster backup --to DIR` has every volume's leader take a cut at a log position P (the rebuild snapshot of design-cluster.md §8.1, under `publish_lock`, no fence), pins it (`BACKUP_PIN(volume, P)`, respected by `MK_GC` and by retirement until the backup ends or `backup_pin_max_s`, 1 day, passes) and copies through the directory backend (R6): every settled object by oid, decoded (the part file, so a backup does not depend on the geometry), every young part, the WAL tail up to P and the volume's metadata; then a metadata snapshot (users, volumes, name slots, settings). Files already in DIR (same name and checksum) are not copied again, so a daily backup copies about a day of ingest, plus the young parts that merges rewrote since the last one (EU8). A backup is complete when its manifest, written last and checksummed, lists every volume; `backup verify` reads it all back. `tsdb cluster restore-data --from DIR` fills a newly initialised cluster: the backup's metadata, every volume at its P, the objects encoded again by the new cluster's geometry, a new cluster identity (a restore generation, design-cluster.md §8.5; change feeds reset, design-cdc.md F19), orphan collection suspended until the operator resumes it (E14). `volume salvage V --from-backup DIR` puts lost objects back by oid (E15). Each volume is consistent at its own P, never at one cluster-wide instant (C16). | Logical backups only (`tsdb export` through a client: no increments, days at 100 TB a node; it stays the way across incompatible versions); file system snapshots of each server (not consistent across replicas and chunk stores, and a restored directory is a rolled-back one, design-cluster.md gap 14); a cluster-wide consistent cut | Settled objects are immutable and named by oid, so an increment is a set difference and the cost follows ingest, not size. A cut per volume is what a rebuild already takes; a cluster-wide instant would need coordination the cluster has nowhere else. Production's NFS/ZFS servers suit the directory backend as a target; S3 follows with its backend. |
| E43 | **Alerts ship with tsdb**: `dist/alerts/tsdb-cluster.rules.yml` (Prometheus rules) beside docs/metrics.md, each naming its runbook (§14.4). A test drives each rule's metric through a scripted fault on the one-machine cluster and evaluates the rule's expression against the metrics endpoint. | Documenting the metrics and leaving alerts to each operator | Margin 1 against margin 0 is the one thing an operator must not miss, and the thresholds come from this design; shipped and tested, the rules stay right as the metrics change. |
| E44 | **The production operator guide** (`docs/cluster-operations.md`, §14.5) covers install, sizing, adding and removing nodes, a dead disk, a dead node, rolling upgrades, backup and restore, monitoring and alerts, and salvage. **Every runbook is a script** in `tests/cluster/runbooks/` that runs its commands against the one-machine cluster, so the guide and the CLI cannot drift apart. | Runbooks as prose only | A runbook never run fails when it is needed, at the worst moment; the commands printed in the guide are the ones the test runs. |
| E45 | **What production requires before 0.11.0 is tagged**, all on the release commit (no gate carried over from an earlier commit): every step's gates; jepsen-cluster's full nemesis set (§10.3, the upgrade and backup nemeses included) on **every** geometry and `replicas=3`; the **72-hour soak** (§10.5); the upgrade test (§10.6: a rolling upgrade within the formats, the refusal and the window across a format change); the runbook and alert tests; the jepsen-review skill and an operations review on the final code, with no open finding rated high or critical. Performance claims only where measured: absolute numbers for a geometry only from servers with disks, and the release notes say which. | A 24-hour soak; jepsen-cluster on R8's two geometries only | A soak finds what only time shows: memory and descriptor growth, GC and retention converging, settles and scrub keeping up, restarts. 72 h covers three daily cycles of settles, retention and backups. Each geometry has its own placement, small-object k' and degraded-settle edges. |
| E46 | **Not production in 0.11.0** (§14.6): shared NFS mode (N0-N8 wait for their maintenance windows): `cluster init --mode shared` is refused without `--preview`, and its docs say so; the S3/isod backend and the external cold tier are not built; the directory backend is for tests and backups only (`init` refuses it as the settled tier without `--test-backend`; *2026-11-11*: as a single server's local tier it is production, E55). | Shipping merged shared-mode steps as production | Shared mode's go/no-go (N0) and its jepsen-ha matrix have not run; production needs both. |

**Device classes** (2026-11-11, §15; the user: "yes do that, so one instance can cover both nvme and hard drives").
Decided in this amendment with their rejected alternatives, and revised the same day after a review (§15.11; E57 and E58
are new); the user may change them.

| # | Decision | Rejected | Why |
|---|---|---|---|
| E47 | **A tsdbd declares each disk with `--disk PATH[:CLASS]`**, repeated, CLASS one of `nvme`, `ssd`, `hdd`. A disk is set up once, explicitly, by `tsdb disk init PATH[:CLASS]` (E58), which writes its **disk META** (store id, class, the `fdatasync` latency it measured); the first tsdbd that names it binds it to its node and cluster, once. Without a stated class, `disk init` detects it from the block devices under PATH (§15.1): PCIe `nvme*` devices are `nvme`, other non-rotational ones `ssd`, rotational ones `hdd`; it refuses, asking for the class, when it finds no local block device (NFS, ZFS, btrfs, tmpfs, overlay, loop, NVMe over a network) or devices of different kinds. The metadata group records the class at the disk's first heartbeat and is the record from then on: an explicit `:CLASS` that disagrees is refused at start, naming both; a detection that reads differently after a kernel or firmware update is logged and counted, never refused; only `tsdb cluster disk class NODE STORE CLASS` changes it. In cluster mode `--dir DIR` is one more disk, taken as `ssd` (fast) without detection when no class is stated. Replaces E27's `--chunk-dir`. | Flags named by use (`--data-dir PATH`, `--chunk-dir PATH`); detection only; operator-defined class names with placement rules (Ceph's device classes and CRUSH rules); the class only in the disk's own META; refusing a start when detection disagrees with the record (the first draft: a kernel that reports `rotational` differently would stop every server of a rolling OS upgrade) | A flag named after the hardware leaves placement to the cluster: the same NVMe holds chunks on an NVMe-only server and none on a server with hard drives (E50), with no command line to change. Detection alone is wrong too often: it can follow LUKS and md to their devices (the benchmark machine's NVMe is under LUKS), but dm-cache and bcache mix kinds, many virtual disks and RAID controllers report the wrong one, and NFS and ZFS have no device to ask. This design has exactly two uses for a disk (E48), so free-form classes would add a rule language to test for nothing. One record of the class (the metadata) keeps a typo on one restart from moving terabytes, and a changed report from stopping a server. |
| E48 | **Two uses, from three classes**: `nvme` and `ssd` are **fast**, `hdd` is **capacity**. Fast disks hold log replicas (`v<N>/`: WAL, MANIFEST, SERIES, STATES, REPL, young parts, `.ec` index files, retained segments, retired parts of settles, rebuild snapshots). Capacity disks hold chunk stores (`s<N>/`). The class name is kept for reports, metrics and one tie-break (`nvme` before `ssd`, E51). The granule cache stays in RAM (§3.3); there is no disk cache of chunks in 0.11.0. `--state-dir` stays its own flag, on a file system of its own (E58); tsdbd warns when it is on a capacity disk (Raft syncs of 5-10 ms slow metadata commits and failover, and inserts only while an ISR change commits). Each chunk store keeps its own per-volume `EPOCH` (E14: the fence is the maximum over the server's stores) and lists itself at start, so a store depends on no other disk of its server. | A third use for `ssd` (for example `.ec` index files on SATA SSD, WAL on NVMe); an NVMe chunk cache in front of hard drives (L2ARC-like); one `EPOCH` per server | The design's costs split two ways only: the young tier needs low `fdatasync` latency and random reads (index pages, §3.1), chunks need bytes per euro. Sync latency follows power-loss protection more than the interface: an SSD with it syncs in well under a millisecond, close to NVMe for group commit, one without it can take several ms; so `disk init` measures it and warns above `disk_sync_warn_ms` (2 ms) for a fast class (EU9). A chunk cache on NVMe would compete with the young tier for the disk whose filling refuses inserts (E53); `ec_keep_local_h` (§3.3) already keeps the newest settled day local where it is read hard. An `EPOCH` on the NVMe would be lost with it while the chunks it fences survive. |
| E49 | **What a server can do follows from its disks; `--role` only narrows it.** A server with at least one fast disk is **compute-capable**; one with at least one chunk store (a capacity disk, or a fast disk on a server without capacity disks, E50) is **storage-capable**. `--role compute\|storage\|both` (default `both`, "whatever the disks allow") restricts it, and is refused when it asks for what the disks cannot give (`--role compute` with only hard drives). The member record holds the declared role and the capability; placement uses their intersection. A server whose last fast disk is gone gets no new log replicas from that moment; the replicas it had follow E54's timers, and it is storage-only once they are gone. | `--role` as the only source (E20 as first written); roles chosen by the balancer | A server with NVMe and hard drives is the user's case: it is both, with each tier on the right disk, and needs no flag. Keeping `--role` lets an operator keep a box out of leadership (`--role storage`) or out of chunks (`--role compute`) without unplugging disks. |
| E50 | **New chunks go on capacity disks; a fast disk holds chunks only on a server that has no capacity disk.** There the fast disk is shared: chunks stop where the room they leave falls below the young tier's reserve, and never above §5.6's fill mark (§15.4). A server that gets its first capacity disk drains its old chunks off the fast disks by local moves (§15.3). A capacity disk never holds a log replica, unless the cluster setting `log_on_hdd` (off by default) is on: then a server **without fast disks** may hold log replicas on its hard drives, but only when the log group cannot be filled from servers with fast disks, and health warns while any does. | Chunks on every disk by free space; letting a server with hard drives spill chunks onto its NVMe when they fill; refusing all-NVMe and all-HDD clusters; a fixed share of a shared disk for chunks (60% in the first draft: a third of an all-flash server's room left unused) | NVMe-only servers (small clusters, all-flash) have nowhere else to put chunks, and that is today's `both` server. On a mixed server, chunks on the NVMe would take the room that keeps inserts going; when hard drives fill, placement moves positions to other servers (§5.6) instead. One log replica on a hard drive holds back its volume's commit point (5-10 ms p50 per sync with group commit), so it is the last resort of a cluster without enough fast disks, never a choice by free room. |
| E51 | **Placement by class.** Log replicas (E9's log group) go to compute-capable servers and, inside one, to the fast disk with the fewest log replicas weighted by their ingest whose projected use stays below `young_move_pct`, `nvme` before `ssd` on a tie. Stripe positions go to storage-capable servers, one per server (E9), each chunk to one of its chunk stores. The balancer **prefers stripe groups of one class**, judged by the stores the chunks are on, and mixes only when there are not k + m servers of one class; health then names the volume. Rebuilds and moves keep the use: a log replica is rebuilt only on a fast disk, a chunk only on a chunk store, and nothing is placed on a disk whose projected use after the copy passes its mark. Room to rebuild (§5.6) is counted per use. **A log replica that must leave a filling disk moves to another server** (design-cluster.md §8.1's learner, added before the old replica is dropped); in 0.11.0 it never moves between two disks of one server. A replica lost with its disk is removed first, and its replacement may then go to another fast disk of the same server. | Placement blind to classes; separate member lists per class; mixed stripe groups as normal; a learner on the same node as the replica it replaces (the first draft: `REPLICA_ADD` refuses a second replica on a node, server/meta.c:1385-1386, the ISR is a list of node ids, server/meta.h:174, and relaxing that would let one server count twice towards `min_isr`); strict `nvme` first or most free room first | The class is what the user asked placement to follow. A stripe group of mixed media makes a partition scan as slow as its hard drives and a granule read unpredictable; preferring one class costs nothing where there are enough servers. One replica per node keeps E24's promise (an acknowledged row is on `min_isr` servers) without a new identity for replicas; a move to another server never lowers the in-sync count. Strict `nvme` first would send every WAL's syncs to one device until it filled, and most room first every new replica to a newly added disk. |
| E52 | **The member record lists the disks**: per disk its store id, class, uses (log, chunk), size, measured sync latency and state (UP, DOWN, OUT, DESTROYED: E27's device states). Changes go through metadata ops added at the end of `enum meta_op_kind`: `STORE_ADD` (a set-up disk's first heartbeat, conditional on its store id being unknown), `STORE_CLASS` (`disk class`), and E27's `STORE_STATE`. Bytes per disk and per use (young data, retained, retired, local, index, chunks) travel in heartbeats only and are never committed. It is part of metadata snapshot version 3 (E18): no new version. | Classes in heartbeats only; a new snapshot version | Placement must read the class from committed state, or two metadata leaders could place by different classes. Free space changes every second and would flood the Raft log. Snapshot version 3 is not released yet, so the fields join it. |
| E53 | **Filling, per disk and per use**; percentages are of the whole disk, except the young marks of a shared disk (E50), which are of the room its chunks leave. Fast disks: at `young_warn_pct` (75%) a warning; at `young_move_pct` (80%) the balancer moves log replicas off it to other servers (E51) and places none on it; at E30's `young_high_pct` (90%) the leaders of **the volumes with a log replica on that disk** refuse inserts (UNAVAILABLE, `TSDB_WHY_FULL`), other volumes go on, retired parts of settles whose `MK_DIGEST` matched are deleted at once instead of after `settle_keep_s`, and merges whose output would not shrink the disk wait. A move needs its target to end at least 10 points below its source. Heartbeats count every use, retired parts (`settle_keep_s`, E26) and `ec_keep_local_h` copies included. Chunk stores: §5.6's fill mark, `full × (1 − the largest domain's share)`, counted over all chunk stores whatever their class (the class is a label), so an all-flash cluster has the same mark and alert; stores that fill make settles wait, so young data grows on the fast disks, which is why the chunk alert predicts the day. Metrics `tsdb_disk_bytes{node,store,class,use}` and `tsdb_use_bytes{use,class}`; four shipped alerts (E43): fast disk at 75% (warning), inserts refused for FULL (critical), chunk stores at their mark within 7 days at the current rate (warning, per class), no room to rebuild (critical, per use). | One mark per server (E30 as first written); refusing every volume of the server; letting fast disks spill to capacity disks; capacity counted by class (the first draft: an all-flash cluster had no mark and no alert) | A server with four NVMe disks fills them unevenly (each holds other volumes' replicas); refusing the server for one disk refuses volumes that have room. Moving before refusing gives the balancer 10 points of the disk to work with. Between 90% and ENOSPC (fail-stop of that disk's replicas, E57) only growth other than inserts is left: retired copies, learners, merges; each is bounded or stopped there. |
| E54 | **A lost disk, per class** (§15.6), each disk a fault domain (E57). A **capacity disk**: E27 and §5.5 as written (only its chunks, onto the server's other capacity disks below their mark, else the positions move), never onto its fast disks (E50). A **fast disk**: its log replicas are fail-stopped or, on a hang, given up after `disk_stall_s` (E57), leave their ISRs (writes go on), volumes it led fail over, and after `rebuild_after` they are removed and rebuilt as learners, on another fast disk of the same server when it has room, else elsewhere; **its server's chunk stores keep serving** (put, read, scrub, rebuild source). The last fast disk lost makes the server storage-only until one is added. A lost `--state-dir` (node identity): `node hold OLD`, the server started as NEW with OLD's disks pending adoption, `node replace OLD NEW`, the disk METAs rewritten, then each store reconciled by §5.2's two-way CHUNK_LIST against the leaders' committed objects, which fills what was settled meanwhile (a runbook, E44). | Treating any disk loss as the server's loss; rebuilding the young tier only elsewhere; adopting stores by comparing chunk lists with the metadata (the first draft: the metadata holds none) | A dead NVMe on a server with 20 hard drives would otherwise rebuild 100 TB of chunks that are intact. Adopting intact stores is E28's rule (positions back to a holder with intact stores) applied to a server that lost only its identity; the hold keeps OLD from going OUT while the operator works. |
| E55 | **One server gets a local settled tier in 0.11.0** (§15.7): `tsdb DIR tier add PATH[:CLASS]` (operator only, like `volume add`, with its path checks; appended to a checksummed, append-only `TIERS` file beside VOLUMES under its OFD lock; each tier store has a META with its id and the database's id; each volume gets `<store>/v<N>/`). **Each store is a position**: an object is the part file erasure coded k' + m over distinct stores with R5-R6's codec and cells, by default a mirror on 2 stores, k' = stores − 1 with m = 1 on 3-5, and 4+2 on 6 or more; m = 0 only with `--single-copy`, for a store that is redundant underneath (RAID, ZFS, NFS from ZFS). A volume with a tier settles a partition key **`tier_after_s` (604,800 s, 7 days) after its end**, or earlier, oldest first, when its fast disk passes `tier_high_pct` (80%), by E5-E6's settle; its `.ec` index file stays in the volume directory on the fast disk. Reads go through R7's seam with `pread` (never a mapping) and the granule cache, every granule checked; a store DOWN is read around by decoding. A lost store (`tsdb DIR tier lost STORE`) is rebuilt onto the others from k' positions; partitions are lost (E15) only past m, and `tier salvage` drops them. Inserts are refused before the fast disk fills (`TSDB_WHY_FULL`), with tier metrics and alerts. `.ec` files, `ppos` and the settle markers come to single-server volumes behind `requires=tier` (E17 amended). Shared-mode volumes refuse a tier, and the bindings get no tier call, in 0.11.0. | Moving old partitions into an HDD volume; a symlink or a whole part file moved to the hard drive and mapped there; tiers as tsdbd flags; cluster only; one bare copy on the store with the most room (the first draft: with N drives N times the chance of losing data, while the codec was built for exactly this) | Series ids name their volume (AGENTS.md "Series ids"): rows never move between volumes, so a volume on a hard drive is not a tier. A part mapped from a hard drive puts its index there too, so a statistics query would seek, and a dead drive would raise SIGBUS in whatever thread touched the page. A flag forgotten at a restart would open the database with its settled partitions missing; the record in the database refuses that ("Damage is never silent"). The user asked for cold data on an erasure-coded pool: the store as a position is the cluster's stripe with stores for servers, and R6 already writes chunks to directories. |
| E56 | **Cost: R13 grows and R15 is new** (§11, §15.10): R13 (disks) becomes "disks with classes" (setup and detection, the disk META, the member record, placement and balancing by class, local chunk moves, filling per disk and per use, each disk a fault domain with its probe and workers, the per-class losses, `sim_place` with classes): 6-8 track-days (was 2-3), 16 gate hours (was 6). R15, the single server's local tier with redundancy across stores: 5-6 track-days, 10 gate hours, needing only R7. P6 gains 2 hours for the class cases. The tests run on one machine with two scratch file systems standing in for NVMe and hard drives (classes stated), detection against a fake `/sys` tree, and outagefs's `slow` mode with a new `rate` cap standing in for a hard drive's seeks and throughput, in R13, R15 and R11 only (§15.9). | A cluster step only; tests on real hard drives in the gates; slow hard drives in every cluster test (uncosted hours in R8, R9, R12) | The single-server tier reuses R6-R7 and the user's "one instance" covers it. Real hard drives give numbers (R11), not correctness: the gates must run on any developer machine. |
| E57 | **Each disk is a fault domain of its own.** A failed write or fsync of an acknowledged chunk, or ENOSPC, on one chunk store marks that store DOWN as a device (E27), never exit 1 (§2.2 amended). An engine fail-stop (`eng_fail`) on a fast disk fail-stops the replicas on that disk only (design-cluster.md §5.7), through the quiesce and reopen path of the health domains (AGENTS.md "Storage outages"), and heartbeats report it per store. Every `--disk` gets a health probe (src/health.c, `O_DIRECT`); a disk whose probe stalls past `disk_stall_s` (10 s) is DOWN, and every leader with its replica there resigns as a fail-stopped one does. Chunk I/O runs on one worker pool per disk, never on the event loop that sends heartbeats. E12's `failstop_out_s` applies to the device; a server goes OUT by it only for a fail-stop of `--state-dir` or of something process-wide (USERS). | Exit 1 on any fail-stop (today's tsdbd, server/tsdbd.c:78-84, and §2.2 as first written); detecting a hung disk only by ISR lag | With 12-24 drives a box a disk failure is the most frequent hardware event (E27): exit 1 would make it a whole-server failure, failing over every volume the box leads and rebuilding 100 TB of intact chunks 60 s later. A hung `fdatasync` returns no error: a leader whose own sync hangs holds its commit point, so only a probe bounds how long its volumes stall. |
| E58 | **Disks are set up explicitly and checked at every start.** `tsdb disk init PATH[:CLASS]` writes the disk META; tsdbd never does. tsdbd refuses a `--disk` with no disk META, whose META names another node or cluster (except stores pending adoption, E54) or a destroyed store, or whose `st_dev` equals that of `/`, of `--state-dir` or of another `--disk` (`--allow-shared-device` lifts this one for test harnesses and one-disk development boxes; health warns while it is set). A known store whose path is back without its META, or not there, is that store missing (the device DOWN timer), never a new disk. `tier add` makes the same device checks. | Setting a disk up at its first use (the first draft) | A hard drive that fails to mount at boot leaves its mount point as an empty directory on the root file system, usually the boot SSD that also holds `--state-dir`: set up at first use it would be a new fast disk there, log replicas would be placed on it, its real store rebuilt, and the Raft log's file system filled. Two paths on one file system would count its room twice. |

## 1. Words

- **Log group** of a volume: the m + 1 replicas (3 in `replicas=3`) of design-cluster.md: leader, ISR, learners. They
  hold the WAL, memtables, young parts, and the `.ec` index files of settled objects. Only they answer queries.
- **Stripe group**: k + m **positions** (0 .. k + m − 1), each held by one storage-capable server (on one of its disks
  per chunk); data positions 0 .. k − 1, parity positions k .. k + m − 1. Empty for `replicas=3`.
- **Object**: one settled part file, erasure coded. **Cell**: `cell_bytes` of it (E7). **Stripe**: k consecutive cells
  and their m parity cells. **Chunk**: what one position holds of one object (a file).
- **Settle**: turning a partition key's parts into objects (§2.3). **Cut Z**: the log position a settle is taken at.
  **Part position** (`ppos`): the highest log position of a row a part holds (§2.3). **Settle gate**: the per-key
  flag that holds large writes into a settling key.
- **Margin**: copies (replicas) or chunks (EC) left beyond the minimum needed to read (1, or k). The young tier's
  margin is ISR size − 1 (E24).
- **DOWN** / **OUT** / **DESTROYED**: a server or disk not answering, whose data still counts / given up on, rebuilt
  elsewhere / declared by the operator never to return (E12, E15).

## 2. What is coded and what is replicated

### 2.1 Why parts, and only settled ones

Parts are immutable and already the unit of every copy in the design: large inserts ship part files (C8,
design-cluster.md:461-491), rebuild copies them by hard link (§8.1 there), the cold move hands over a whole part
(§9.3 there). A part file is self-describing: header, granule blocks, the packed index, tombstones, footer with a hash
over everything but the blocks, and a checksum per granule (src/part.c:1-21; the per-granule check at
src/part.c:313-317). So an object needs no layout of its own beyond the cells.

What is **not** coded, and why:

| Data | How it is kept | Why not EC |
|---|---|---|
| WAL records | replicated to the log group, byte for byte (C5) | A record is acknowledged when durable (C11): m + 1 syncs per group commit; EC would need k + m. |
| Memtables | each log replica's own, from the WAL | In memory. |
| Unsettled parts (flushes, merges in progress, large inserts) | each log replica's own (C9) | They are rewritten by merges within hours; coding them would ship every merge output over the network (about (k + m − 1)/k of its size per level) and remove C9. |
| Granule index of a settled part | an `.ec` file on every log replica (E21) | Read by every query; about 32 bytes a granule, which is **about 0.75% of the part** (3-8 MB per GB: the 100 B benchmark's 0.07 of 9.29 bits a row, AGENTS.md; hour cuts and short series add granules). |
| SERIES, STATES, CLASSES, RETENTION, MANIFEST, REPL, FNO, the slot index | every log replica, through WAL markers (design-cluster.md §5.4-5.5) | Small, append-only, needed at open. |
| Metadata (members, volumes, lists, users) | the metadata group (C2, E8) | Kilobytes. |

### 2.2 Layout of an object

The object is the part file's bytes, length L, cell size `cell` (E7). Cells are numbered c = 0 .. ceil(L / cell) − 1.
With k' = min(k, number of cells), cell c is in data chunk c mod k' at offset (c div k') × cell; stripe s holds cells
s·k' .. s·k' + k' − 1 (the last one padded with zeros for the parity computation; the zeros are not stored). Parity
chunk j holds parity cell j of each stripe.

A **chunk file** in one of its server's chunk stores: `<store>/s<volume>/o<oid>.<pos>.chunk`:

```
header (96 bytes): magic "TSDBCHK1", u32 version 1, u32 flags, u8 volume uuid[16], u8 oid[16],
                   u64 writer epoch, u64 reserved,
                   u16 k', u16 m, u16 pos, u16 reserved, u32 cell_bytes, u32 reserved,
                   u64 object_len, u64 chunk_len
cells             chunk_len bytes
cell checksums    u64 xxh64 per cell of this chunk (the encode-time sums, E26)
footer (16 bytes): u64 xxh64 of header and checksum table, u32 cell count, u32 magic "CHKE"
```

Written as `tmp_*`, fsync'd, renamed, directory fsync'd before the server answers PUT_OK (AGENTS.md "Durability"). A
failed fsync of anything already acknowledged is fail-stop for the chunk server (exit 1), which reconciles by
CHUNK_LIST after its restart (§5.2). *2026-11-11*: for that chunk store, not the server: the store is a device DOWN
(E27, E57), the rest of the server goes on, and the store reconciles by CHUNK_LIST when it is back.
A cell whose checksum fails is an erasure, never decoder input
(objects:src/recovery/rebuild.c:21-25). Each chunk store has a store id (in its META), which the metadata records per
server: a store that comes back with another id, or not at all, is a lost disk (E27).

Each storage server also keeps, per volume, a small `EPOCH` file: the highest volume epoch any request has shown it,
synced before it acts on a higher one (E14). *2026-11-11*: one per chunk store (E48), the fence being the maximum over
the server's stores (E14).

The **object record** (in the `MK_COLD` marker body, src/marker.h:40, reserved in format 9, defined here; and as the
trailer of the `.ec` file): `oid` (16 random bytes from the leader), partition key, the part it was encoded from
(name, block range, `ppos`), `object_len`, xxh64 of the object, k', m, `cell_bytes`, the stripe list and its
`stripe_version` it was written under, the positions written (all, or all but one for a degraded settle, E10), the
writer epoch, the class check result (§2.3 step 2), and the encode-time xxh64 of **every cell of every position**
(about 10 KB for a 1 GB object).

The **`.ec` index file** on each log replica: `p<key>_<min>_<max>_<lvl>.ec`, holding the part's header, granule index,
tombstones and footer exactly as in the part, then the object record. It is mapped like a part's index (no copy:
AGENTS.md "The granule index"), so `gx_*` and `gx_load` read it unchanged. The leader writes it and ships it to the log
replicas as a pending file, as `Y` (§2.3); it is never fetched at apply time.

### 2.3 Settling a partition key (the leader)

The protocol fixes one cut Z: the objects and `Y` hold exactly the rows of the key P from log positions at or below Z,
and nothing writes into P between Z and the marker.

**Part position.** Every part of a cluster volume has a `ppos`, recorded in its MANIFEST entry: for a flush part, the
highest position of a record holding a row of that key (the memtable keeps that maximum per partition key as it
appends; the header's `fno_hi` is not it, since a flush's `fno` range covers every key of the flush); for a part a
`COMMIT` published (large insert, delete, replace), the `COMMIT`'s position; for a merge output, the largest of its
inputs'; for an `.ec` part, its settle's Z.

1. **Choose.** Key P is eligible (E5). The leader pins its parts of P, the input set I (merge selection skips P while
   it settles).
2. **Encode, check and put**, outside every engine lock, one object per part of I: read the part by mapping; check
   every granule checksum and the footer hash as it is read (E26), and abort the settle on a mismatch; judge the part
   against CLASSES as `cls_check_parts` does at open (src/classes.c:1087, :1146) and record the result; encode stripe
   by stripe with the kernels of E19, computing every cell's sum; send chunk i to position i on the bulk link (C25)
   with its sums and the leader's epoch (the leader's own position written locally if it holds one). Each server
   checks the cells, writes the chunk durable and answers PUT_OK with its sum, which the leader compares. A position
   that is DOWN fails the settle, retried later, until `settle_wait_max_s`, then the settle goes on degraded (E10).
   Write the `.ec` index files.
3. **Close the key.** Close P's settle gate: a large insert, delete, replace or retention drop whose keys include P
   waits at the gate before it registers its fence (`lw_enter`), and the leader waits for those already past it to
   publish or fail. Then `lw_enter` over P's whole key range, the large-write fence of AGENTS.md "Last write wins":
   it is registered under `buf_lock`, where small writes append, so after it no small row of P is appended. Then wait
   until the commit point passes the log end as it was at that moment: every row of P appended before is committed
   and, on the leader, applied (in a cluster the memtable holds committed rows only, C7).
4. **Cut.** If the memtable still holds rows of P (E5 normally waited for a natural flush), rotate the WAL segment and
   flush through it. **Z** = the log end now. **`Y`** = every non-`.ec` part of P the leader holds other than I: rows
   that arrived during steps 1-3, all with `ppos` ≤ Z. Ship `Y` and the `.ec` files to the log replicas as pending
   files (design-cluster.md §5.2 step 3).
5. **Marker.** Check, just before it: the committed `stripe_version` is the one the chunks were put under (if a
   position moved meanwhile, put its chunks to the new holder first), and every position written is UP in the current
   epoch (else wait; E10). Append `MK_COLD`: Z, the object records, the names, sizes and xxh64 of the `.ec` and `Y`
   files, the oids of objects it retires (a re-encode's inputs), the `stripe_version`. Replicated like any record.
6. **Apply** at the commit point, on every log replica including the leader, under `publish_lock`, as **one intent
   record** (the intent file of `parts_commit`, extended; replayed at open): flush the memtable if it holds rows of P;
   for every non-`.ec` part of P: `ppos` ≤ Z, rename it `retired_*` (kept `settle_keep_s`, E26); `ppos` between Z and
   the marker, **stop**: the volume on this replica is DIVERGED (`TSDB_WHY_DIVERGED`), nothing is retired, and the
   replica is rebuilt (design-cluster.md §8.5); list the `.ec` files under fresh block numbers in the leader's order,
   then commit the `Y` files above them (`parts_commit`; they keep the leader's `ppos`), then retire the objects the
   marker names. A merge of P in flight on a replica drops its output, as when a replace retires its inputs. Then the
   leader leaves the fence (`lw_leave`) and opens the gate.
7. **Digest.** In the background the leader computes the logical digest of P as of Z from its own retired parts and
   appends `MK_DIGEST` (oids, digest). Each log replica computes the digest of its own retired set and compares; on a
   match the retired parts go after `settle_keep_s`, on a mismatch they stay and the volume is DIVERGED for the
   operator (E26).

**Why last write wins holds.** Write R(P, Z) for the rows of P from records at positions at or below Z. The gate and
the fence make sure that no record between Z and the marker holds a row of P or publishes a part of P (retention drops
of P are deferred too). *On the leader*, at step 4 every record at or below Z is applied, the memtable holds no row of
P, so its parts I ∪ `Y` hold exactly R(P, Z), ranked by their block numbers. *On a log replica*, at the marker's apply
every record up to the marker is applied (C7), so its rows of P, in parts and in the memtable, are again exactly
R(P, Z): the memtable's are flushed in step 6, and every part holding them has `ppos` ≤ Z. A part with `ppos` above Z
would contradict the gate, and the replica stops instead of retiring it. Replicas hold logically equal data (C9), so
the visible rows of the replica's retired set equal those of the leader's I ∪ `Y`; step 7 checks that instead of
assuming it. Inside the new set the objects keep the leader's order and `Y` ranks above them, as on the leader; older
objects of P rank below (lower block numbers), and those a re-encode replaced are retired in the same intent. Rows
after the marker get higher block numbers or stay in memtables, which rank above parts. The fence holds through the
commit wait, one replication round and a small ship: about 1-10 ms with no forced flush (E5), noticed only by a write
into P during it; a forced flush, after `settle_force_s`, adds its time (100 ms to seconds at high ingest).

**Crash and failover.** Chunks put before a marker commits are orphans: E14's rule finds them, and only a committed
`MK_GC` deletes them; a new leader never deletes an object named by any record of its log, committed or not, so a
marker in its old tail commits with its chunks intact. A replica that misses a `Y` or `.ec` file fetches it before
acknowledging the marker (as for `COMMIT`, design-cluster.md §5.2); the leader keeps every file a marker names, hard
linked beside the retained segment that holds the marker, until that segment leaves retention (E29), so a catching-up
replica finds them; a replica further behind installs a snapshot. A crash during step 6 replays the intent at open:
the old parts and the objects are never listed together. Chunk-only servers apply nothing; they learn of deletions
from committed `MK_GC` markers through the leader (§5.7).

**Re-encode and conversion** (E2, E6) use the same steps with their own I: the objects to combine (read through the
read path) and the key's young parts, pinned at the start; the output object, the cut and `Y` as above; the marker
names the retired oids. Their reading takes minutes, but the gate is closed only for steps 3-6.

### 2.4 After a settle: late rows, deletes, retention, merges

As design-cluster.md §9.4: late rows for P go to the memtable and young parts above the objects; a delete over them
writes tombstones in young parts; merges never take an `.ec` part as input. Young parts of P settle again after
`settle_grace_s` as further objects (E6); a re-encode combines them when there are more than `ec_objects_max` objects
or tombstones cover more than 10% of their rows. A retention drop of a whole partition (`MK_DROP`, src/marker.h:31)
retires the `.ec` files; the leader deletes the chunks by `MK_GC` after `obsolete_part_lifetime_s`. Expired rows inside
a settled partition are clamped by queries as today and go only when the whole partition does (partitions span at most
364 days).

At open, an `.ec` part is judged from its index only: its class check was done at the settle (step 2), and a class
switch is always past every row of its series (AGENTS.md "Partition classes"), so no later switch falls inside a
settled granule. An open never needs the stripe group.

### 2.5 `replicas=3`

Unchanged from design-cluster.md: a log group of 3, no stripe group, no settles. The design-cluster.md text is this
mode exactly; E12, E13's order, E27 (for the WAL disks: design-cluster.md §8.5's replaced disk), E28, E29 and E30 apply
to it too.

## 3. Reads

### 3.1 Where the bytes come from

Queries run on the leader (D5) or a log replica (`REPLICA_OK`); chunk-only servers never answer queries. For an
`.ec` part:
- **Statistics**: from the local `.ec` file, mapped. Nothing is fetched. At 10-100 TB of chunks a node the index is
  terabytes per compute server (§4), more than RAM, so a statistics query over old data reads index pages from NVMe
  when they are not cached: still milliseconds, not free.
- **Granule blocks**: `granule_read` (src/part.c:299-325) is the one place that reads block bytes for queries
  (`p->map + g->off`, src/part.c:313); the merge's verbatim copy (src/part.c:726, :730) never sees an `.ec` part, and
  the resident-bytes statistics (src/stats.c:1134, src/stats_hot.c:424) count an `.ec` part's cached bytes instead.
  For an `.ec` part the seam asks, in order: the granule cache; the local chunk, if this server holds the cell's
  position; the cell's server, for the granule's byte range (one round trip, pipelined with the other granules of
  the query step: R7 makes the query threads of `src/sched.c` issue the reads for every granule of their range
  before they decode the first).
- **Check**: the granule's checksum (`g->crc`, src/part.c:315-317) is checked on every granule of an `.ec` part,
  local chunk included, whatever `verify_checksums` says (E11).

### 3.2 Degraded reads

A cell whose server is DOWN, `read-avoid` or refuses, whose read is not answered within the hedge time (E31), or
whose bytes fail the check, is rebuilt from **the same byte range** of k other cells of its stripe (data cells first,
then parity), with the decode kernel; the inverted matrix is cached per erasure pattern (objects' plans,
objects:src/codec/rs.h:63-78). If the decoded granule fails its check, the reader reads the whole cells of that
stripe from every reachable position and checks each against its encode-time sum in the object record (E26): the
cells that fail are erasures, the bad holder's chunk is queued for repair, and the decode runs again from cells that
passed. Outcomes (E15): fewer than k good cells because positions are unreachable: UNAVAILABLE (retryable); fewer than
k good cells present among reachable positions: `TSDB_ERR_CORRUPT` naming the partition and the granule, and a
repair is scheduled; positions destroyed: lost.

### 3.3 The granule cache

Per server, `ec_cache_mb` (10% of RAM by default), keyed by (oid, granule offset), LRU, filled by reads and by
prefetch: a new leader (after failover or a move) prefetches the cells of the objects settled in the last
`cache_warm_h` (24 h), at most half of the cache's free room, at `cache_warm_mb_s` (200 MB/s) taken from the same token
bucket as rebuild (both read the same storage servers) and below it in priority. `ec_keep_local_h` (0 by default) keeps
the leader's full part file for that long after a settle (the space of one more copy of the newest settled day), for
sites where yesterday is read hard. (These defaults were open question 5 of the first draft; decided here, R11's
measurements may change them.)

### 3.4 Expected performance against one server today

Estimates, to be measured (§10.4). Today's numbers are AGENTS.md's (7950X, one server, NVMe). NVMe storage servers
unless the column says HDD; with HDD a granule read is a seek (5-10 ms, more under load).

| Query | Today | `replicas=3` | EC, warm cache | EC, cold cache, NVMe | EC, cold cache, HDD | EC, one position down |
|---|---:|---:|---:|---:|---:|---:|
| from statistics (whole table, 100 G rows) | 0.17 s | same | same | same (index local; slower if its pages are not in RAM) | same | same |
| one series, one window, 100 K rows (decodes ~12 granules) | 0.57 ms | same | same | + 1 round trip: ~0.7 ms | + 1 seek: ~6-12 ms | + k range reads: ~0.8 ms (HDD: the slowest of k seeks) |
| one series by hour, 10 M rows over 116 days (~12 MB) | 16.3 ms | same | same | +116 partitions' ranges at 3 GB/s, pipelined: ~20-25 ms | ~116 seeks over k servers: ~50-150 ms | ~1/(k+m) of granules at kx bytes |
| whole table by series and hour (100 G rows, ~116 GB, disk bound) | 46.8 s | same | — | bytes from k servers in parallel, NIC bound: ~same | k servers' sequential reads: ~same at k ≥ 8 | +(1/(k+m))·k extra reads for the missing share |

A position stays down for the whole rebuild (§5.3), hours to days, so the last column is not a short event.

Writes are design-cluster.md §10.1's targets for `replicas=3`, 4+2 and 8+2 (3 copies). For 8+4 and 16+4 every small
insert waits for 5 syncs: estimate p50 +0.1 ms and p99 +0.5 ms over 3 copies (the slowest of 5 `fdatasync`s).

## 4. Writes, costs and sizing

The insert path is design-cluster.md §5 with an ISR of up to m + 1: unchanged code (`src/repl.c`: the commit point is
the lowest durable end of the ISR, src/repl.c:319, :486). The settle (§2.3) adds the encode and the chunk puts once
per part.

**Bytes per row** (settled steady state, 1.2 bytes a row in a part, AGENTS.md benchmarks; W = the compressed WAL
bytes of a row on the log stream, to measure; index 0.75% of part bytes per log copy):

| Mode | Disk, settled (+ index copies) | Disk, young | Network, WAL | Network, settle (leader holds a position / compute-only leader) | Merge CPU and writes | Survives (ISR full) |
|---|---:|---:|---:|---:|---:|---:|
| `replicas=3` | 3.6 (3x) | 3.6 | 2W | 0 | 3x one server | 2 |
| `ec=4+2` | 1.8 (1.5x) + 0.03 | 3.6 | 2W | 1.5 / 1.8 | 3x | 2 |
| `ec=8+2` | 1.5 (1.25x) + 0.03 | 3.6 | 2W | 1.35 / 1.5 | 3x | 2 |
| `ec=8+4` | 1.8 (1.5x) + 0.05 | 6.0 | 4W | 1.65 / 1.8 | 5x | 4 |
| `ec=16+4` | 1.5 (1.25x) + 0.05 | 6.0 | 4W | 1.43 / 1.5 | 5x | 4 |

Settle network = 1.2 × (k + m − 1)/k, or (k + m)/k when the leader holds no position, plus the `Y` remainder (small)
and re-encodes. Every log replica merges on its own (C9), so merge CPU and merge write amplification are m + 1 times a
single server's. **Ingest capacity**: every compute server's WAL disk and CPU carry m + 1 copies of what it leads, so
a cluster takes about (compute servers) / (m + 1) single servers' worth of ingest (design-cluster.md §10.1's N/3 for
m = 2, N/5 for m = 4). Real overhead is the nominal one within about 3% thanks to E7's cell size (8 stripes or more
per object); one-cell objects are m + 1 copies.

**Encode CPU.** Encoding is m multiply-adds per input byte (objects:src/codec/gf256.h:6-8). Measured in objects on
old CPUs, k = 16, m = 4, 1 MiB shards: 3.45 GB/s a core with AVX2 (E5-2697 v4,
objects:bench/baselines/2x-se1/olav2-e5-2697v4/rs-encode.txt) and 2.41 GB/s with SSSE3 (E5-2696 v2,
objects:bench/baselines/2x-se1/olav3-e5-2696v2/rs-encode.txt); m = 2 halves the work. Zen 4 and Zen 5 reach the GFNI
kernels (objects:src/codec/gf256.h:14-20): not measured; R5 measures them. A leader settling the 100 B benchmark's
ingest (114 M rows/s × 1.2 B = 137 MB/s, AGENTS.md) spends about 4% of one AVX2 core on it, plus the granule checks of
E26 (one xxh64 pass, several GB/s a core) and the background digest (one decode per log replica, EU6).

**Node counts.**

| Mode | Servers needed | Recommended (rebuild without a replacement) | Log group | Compute servers when separated (m + 2, E21) | Voters (E8) |
|---|---:|---:|---:|---:|---:|
| `replicas=3` | 3 | 4 | 3 | — | 3-5 |
| `ec=4+2` | 6 storage | 7 | 3 | 4 | 5 |
| `ec=8+2` | 10 storage | 11 | 3 | 4 | 5 |
| `ec=8+4` | 12 storage | 13-14 | 5 | 6 | 9 |
| `ec=16+4` | 20 storage | 21-22 | 5 | 6 | 9 |

With `both` roles the compute servers are among the storage ones. A cluster of exactly k + m storage servers works but
rebuilds only onto a replacement (health warns); every volume then spans every server, so m + 1 losses make every EC
volume's settled data unavailable.

**Sizing a compute server** (the operator guide, R11, carries these rules with a calculator):
- young data = ingest (part bytes per day) × (1 day + `settle_grace_s` + `settle_wait_max_s`) × (m + 1) / compute
  servers, more for coarse keys (E5);
- index = settled part bytes × 0.0075 × (m + 1) / compute servers;
- retained segments = ingest × `rebuild_after` × W per row (E29), plus `repl_retain_mb`;
- retired parts of settles = ingest × `settle_keep_s` × (m + 1) / compute servers (E26; *2026-11-11*: as much again as
  a day of young data, 3.2 TB a server in the 8+2 row below);
- headroom to `young_high_pct` (E30) for a 2-hour hold or a guarded rack outage.

Worked out (part bytes = chunks × k/(k + m)):

| Cluster | Settled part bytes | Index, all copies | Per compute server, `both` | Per compute server, separated (m + 2) | Young data at 137 MB/s of part bytes (the 100 B load rate), per compute server |
|---|---:|---:|---:|---:|---:|
| 8+2, 11 × 100 TB chunks | 880 TB | 6.6 TB × 3 = 20 TB | 1.8 TB | 5 TB (4) | 11.8 TB/day × 3 / 11 = 3.2 TB (`both`), 8.9 TB (4) |
| 16+4, 21 × 100 TB chunks | 1.68 PB | 12.6 TB × 5 = 63 TB | 3 TB | 10.5 TB (6) | 11.8 TB/day × 5 / 21 = 2.8 TB (`both`), 9.8 TB (6) |

At 1 M rows/s (1.2 MB/s, 104 GB a day) young data is 1/114 of the last column. So a compute server needs NVMe in
terabytes, not "a fast small disk", at the 100 B benchmark's rates.

## 5. Failure handling

### 5.1 Detection and the first seconds

A server that misses heartbeats for 3 s is DOWN (design-cluster.md §6.1); a disk that returns errors on a chunk store
is DOWN as a device (E27). At once:
- volumes it led fail over (design-cluster.md §6.2: the ISR member with the longest log, epoch + 1);
- ISRs it was in shrink by the leader's lag rule (C11), so writes go on with the remaining copies down to `min_isr`
  (E24), with the young margin in TOPOLOGY;
- reads of cells it holds are degraded (§3.2);
- settles of volumes with a position on it wait, at most `settle_wait_max_s` (E10). Nothing is copied yet.

### 5.2 Back before `rebuild_after`

Its log replicas cut and catch up (design-cluster.md §8.2: KIP-101 cuts) from the segments retained for it (E29); only
beyond that budget a snapshot. Its chunk stores are checked by store id (E27): a store that came back with another id,
empty, or not at all is a lost disk, and every chunk it held is missing. Then CHUNK_LIST runs **both ways** with the
leader's list of committed objects: chunks of objects retired by a committed `MK_GC` are deleted; chunks the server
should hold but does not (a degraded settle's missing position, an fsck that dropped files, a restored directory) go
to the rebuild queue at their margin's priority. A server whose data directory was restored from a backup is a lost
disk, as design-cluster.md §8.5 rules for log replicas.

### 5.3 OUT after `rebuild_after` (30 min)

The metadata leader commits `MEMBER_STATE out` (unless held, opening or guarded, E12), then for every volume with a log
or stripe position on it, a new server chosen by E9's rules, as one `STRIPE_SET` or `REPLICA_ADD` per position:
- **Log position**: a learner, built by design-cluster.md §8.1 (hard-linked snapshot from an ISR member, catch-up,
  join). Young data and indexes: terabytes at the sizes of §4, hours.
- **Stripe position**: state `filling`. It accepts new chunks at once (so settles resume), and rebuilds the old ones:
  for each object, read **whole cells** of k surviving positions, check each against its encode-time sum (a failing
  survivor is an erasure; another survivor is read), decode, check every rebuilt cell against its encode-time sum,
  write, sync, then report. It becomes `whole` when it holds every object named by committed markers up to a
  **barrier** the leader sets when it adopts the new `stripe_version` (its log end then); settles after the barrier
  are put to it directly (§2.3 step 5).

**Priority** (E13): positions whose volumes have the smallest margin first, then by bytes. **Pacing** by margin (E13):
margin 0 at full speed, margin 1 of m ≥ 2 at least 4 × `rebuild_mb_s`, the rest `rebuild_mb_s` (200 MB/s per server)
under design-cluster.md §8.4's token bucket; at most `rebuild_concurrency` (4) objects in flight per server (objects'
worker: objects:src/recovery/worker.h:67).

**Time.** A decode reads k bytes for each byte rebuilt, and each survivor both sends and receives, so rebuilding D
bytes on N servers at r per server takes about **k·D / ((N − 1)·r)** (k = 1 for replicas), N being §4's recommended
count:

| Mode, N | 10 TB, 200 MB/s | 100 TB, 200 MB/s | 10 TB, line rate (25 GbE, 2.5 GB/s) | 100 TB, line rate |
|---|---:|---:|---:|---:|
| `replicas=3`, N=4 | 4.6 h | 46 h | 22 min | 3.7 h |
| `replicas=3`, N=11 | 1.4 h | 14 h | 7 min | 1.1 h |
| `ec=4+2`, N=7 | 9.3 h | 3.9 days | 44 min | 7.4 h |
| `ec=8+2`, N=11 | 11 h | 4.6 days | 53 min | 8.9 h |
| `ec=8+4`, N=13 | 9.3 h | 3.9 days | 44 min | 7.4 h |
| `ec=16+4`, N=21 | 11 h | 4.6 days | 53 min | 8.9 h |

Line rate means no foreground traffic. On HDD storage servers each survivor of 8+2 reads 80 TB of 1 MiB ranges at a
realistic 1-1.5 GB/s per server: 15-22 h whatever the network. For the same N and pace an EC rebuild takes **k times**
as long as a replica rebuild, and during it the affected objects run one margin short: 8+2 at margin 1, so a second
loss leaves margin 0 and a third loses data. EC tolerates as many losses as its m, but for longer windows, so its mean
time to data loss is lower than the count suggests. That is why the pace rises as the margin falls (E13), and why the
operator guide (R11) recommends **m = 4 (8+4, 16+4) above about 20-30 TB of chunks a node**, with a sizing rule that
keeps a node's rebuild under a target (12 h at line rate by default). `sim_place` computes a mean time to data loss
per geometry from server and disk failure rates (R9), so the choice between 8+2 and `replicas=3` rests on a number.
Locally repairable codes (E19) are the later remedy if rebuild reads are what hurts.

### 5.4 Back after the rebuild started

Its log replica directories are stale for positions already moved (new rids are needed to rejoin, gap 3 of
design-cluster.md). For stripe positions (E28): where the replacement is not yet `whole` and the server's stores are
intact, it takes the position back with a `STRIPE_SET` conditional on `stripe_version`, copies only the objects
settled while it was away (from the filling holder, or by decode), and the partial fill is deleted. Where the
replacement is `whole`, the old chunks serve as a copy source for anything still filling (1x reads, not kx), and are
deleted only after the leader checked the replacement's CHUNK_LIST, by a committed `MK_GC`. A log position whose
learner has not yet joined the ISR is taken back the same way when the returned replica can still catch up from
retained segments (E29). If it comes back before the metadata committed the reassignment, it keeps the position.

`tsdb cluster node replace OLD NEW` (new hardware for a dead server) assigns OLD's positions to NEW directly: filled
once, by copy where OLD's chunks are readable and by decode otherwise; no headroom on the survivors, no second move.
Test: a server back after 31 minutes moves the bytes of the objects settled meanwhile, not its whole data.

### 5.5 A dead disk

A disk that fails (I/O errors on its store, or more than `scrub_out_count` damaged chunks a day, E25) is a device
DOWN, then OUT after `rebuild_after` (the same holds and guard as servers, E12). Only its chunks are rebuilt: onto the
server's other stores while they are below their fill mark, else the server's positions on that volume move by the
balancer (copies). `tsdb cluster disk out NODE DISK` gives it up at once; `disk replace NODE DISK` adds the new store
and fills it. Time: D/disks instead of D, at the same rates as §5.3. On a compute server, a lost WAL or young-data disk
is design-cluster.md §8.5's replaced disk: its log replicas become learners and are rebuilt. *2026-11-11*: which disk
holds what follows from its class (E47-E50), and a lost fast disk leaves the server's chunk stores serving (E54, §15.6).

### 5.6 Disks full

A server stops being a placement candidate at its fill mark, `full × (1 − the largest domain's share)`, which keeps
room to rebuild the largest domain (objects:src/placement/README:84-99); rebuilds may use the room up to `full`.
ENOSPC on a chunk `tmp_*` fails that put only (nothing relies on it, AGENTS.md "Fail-stop"); ENOSPC on a WAL,
MANIFEST or dictionary is fail-stop as today, which is why a compute server refuses inserts at `young_high_pct`
before it gets there (E30). *2026-11-11*: marks per disk and per use (E53, §15.4); ENOSPC on a WAL fail-stops that
disk's replicas, not the server (E57).

### 5.7 Fencing and split brain

The log group is design-cluster.md §6 unchanged (epochs, ISR in the metadata group, no clocks). Stripe positions
learn each volume's epoch from every request, the metadata watch and FENCE (`REPL_M_FENCE`, src/repl.h:89, sent to
them too), and keep the highest durably (§2.2). They refuse CHUNK_PUT and CHUNK_DEL below it, and a delete below the
chunk's writer epoch (E14). Chunks are deleted only as a committed `MK_GC` lists them: objects a committed marker
retired, after `obsolete_part_lifetime_s`, and orphans by E14's four conditions. A partitioned or paused old leader
can therefore write orphans and delete nothing: it cannot commit `MK_GC` (C11), and storage servers that already saw
the new epoch refuse it. After `force-leader` (design-cluster.md:607-608) or a restore, the leader's log may lack later
markers; orphan collection stays off until `tsdb cluster volume gc resume V`.

### 5.8 Two at once, m at once, m + 1

| Down | `replicas=3` | EC with m = 2 | EC with m = 4 |
|---|---|---|---|
| 1 | writes and reads on | on; degraded reads | on |
| 2 | on, ISR 1 for some volumes (D1) | on; volumes with both in their stripe at margin 0 (reads still whole, no check margin); writes refused where the ISR fell below `min_isr` = 2 (E24) | on |
| 3 | volumes with all 3 replicas there: unavailable | volumes with 3 positions there: settled data **unavailable**; lost only if those servers are destroyed (E15) | on (9 voters, E8); writes refused below `min_isr` = 4 |
| 4 | | | on for reads; margin 0 for some |
| 5 | | | volumes with 5 positions there unavailable; lost only if destroyed |

Lost is reported (E15) only when the operator declares the servers destroyed (`tsdb cluster node destroyed N`) or
their stores come back empty: queries that must decode lost partitions fail with `TSDB_ERR_IO` naming them, while
queries answered from statistics still answer; TOPOLOGY marks the volume `LOST`; UNAVAILABLE carries reason
`TSDB_WHY_LOST` (18, after `TSDB_WHY_RELOADING` = 17, include/tsdb_protocol.h:313-317) with the "do not retry" flag;
`tsdb cluster volume salvage V` drops the lost objects by oid (`MK_DROP_OBJ`) so the volume serves the rest, young
parts included. A volume whose log group is wholly lost has lost acknowledged writes: salvage says how many positions
above the last settled object it cannot account for.

### 5.9 Scrub

Each storage server reads every chunk of every store within `scrub_days` (14) at `scrub_mb_s` (50 MB/s per disk; 100 TB
on 24 disks is a pass in about a day of disk time), below rebuild in priority and inside the same token bucket, and
checks every cell against its encode-time sum (E25, E26). A bad cell is repaired by a range decode from k good cells of
its stripe, written as a new chunk file (tmp, fsync, rename) and checked. Log replicas scrub `.ec` files (their footer
hash and object record) and young parts (`verify_checksums` reads) at the same pace; a damaged one is repaired from
another log replica (design-cluster.md §8.5). Metrics (docs/metrics.md): chunks scrubbed, damage found and repaired per
disk, the age of the oldest unscrubbed object; health warns past 2 × `scrub_days`.

## 6. Moves and failover

### 6.1 Failover of a leader

design-cluster.md §6 and the protocol 13 bodies (MOVED, UNAVAILABLE, TOPOLOGY; design-ha-modes.md §7 "S0, the wire
half"). The new leader holds the `.ec` index files already (E21), so statistics are warm; its granule cache is cold
and §3.3 prefetches. A settle in flight on the old leader is abandoned: its chunks are orphans unless its marker is in
the new leader's log, in which case it commits (§2.3, E14).

### 6.2 Operator moves and rebalancing

- `tsdb cluster volume move V --pos P --to N` (a stripe position) or `--log --to N` (a log replica). A stripe position
  moves by copy from its holder (1x, no decode), the target `filling` until whole (the barrier of §5.3), then one
  `STRIPE_SET` conditional on the list version; reads use the old holder until then. A log replica moves as in
  design-cluster.md §3.2 (learner, catch-up, join, drop).
- **Rebalancing** after `node add` or `node drain`: the greedy pass of design-cluster.md §3.2 over both lists, at most
  5% of a group's positions moving at once and no automatic plan that moves more than 25% (objects' balancer bounds,
  objects:src/placement/README:178-200), even leaders, chunk counts by capacity. It has its own pace, `balance_mb_s`
  (default `rebuild_mb_s`), so an operator can fill a new node fast on a quiet cluster. A new storage server receives
  about 1/N of the cluster's chunks on one receiver: 100 TB at 200 MB/s is 5.8 days, at line rate 11 h.

### 6.3 Many volumes at once

A server's positions spread over many volumes (C1), so its rebuild and its moves spread too; the priority and pacing
of §5.3 apply across them.

### 6.4 Redundancy change (E2)

In this order, each step committed before the next:
1. If the new m is larger: add learners until the log group has new m + 1 members and they are in the ISR; then set
   the new `min_isr` (E24).
2. Record the new geometry in the volume (`VOLUME_REDUNDANCY`): new settles use it, under a new stripe list.
3. Re-encode the old objects, one key at a time, by §2.3's protocol (rebuild pace; about 5 days for 1 PB on 11 servers
   at 200 MB/s each). Until it ends the volume's tolerance is the lower of the two geometries'.
4. Drop the old stripe list's positions: `STRIPE_SET` refuses to drop a position that a committed, live object names.
5. If the new m is smaller: drop log replicas last.

`ec` to `replicas=3`: first `MK_UNSETTLE` per object (every log replica reads the object through the read path,
checks it, and lists it as a normal part in its place, by the same intent swap), then the stripe group empties by
step 4. The time is the same order as a re-encode. In 0.11.0 (E33). While a rolling upgrade has versions mixed, a
redundancy change is refused, and one already running pauses at its next key until `upgrade finish` (E38).

## 7. Shared NFS mode and the existing plan

Shared mode has no redundancy setting (E16). Replicated mode's R1-R4 (design-ha-modes.md §7) stay as they are: R1's
core needs no change (an ISR of up to 5 is within `META_REPLICAS_MAX`, server/meta.h:68), R2's engine replica mode is
the log group, R3's replication is the young tier, R4's rebuild is the log-position rebuild. This design adds R5-R14
(§11) on top and moves the cold boundary X1 into R7 (its put, marker, `.ec` index and swap are the settle's).

## 8. Formats and versions

| What | Where | Freezes at 1.0? |
|---|---|---|
| Chunk file (§2.2): 96-byte header with writer epoch, encode-time cell sums | storage servers, `<store>/s<volume>/` | yes, behind `requires=cluster,ec` |
| Chunk store META with store id; per-volume `EPOCH` file. *2026-11-11*: the **disk META** of every `--disk` (store id, class, sync latency, node id, cluster uuid; E47, E58), the `EPOCH` in each chunk store (E48) | storage servers; every disk | yes |
| `.ec` index file: the part's header, index, tombstones, footer, then the object record (with every position's cell sums) | log replicas | yes |
| `ppos` in MANIFEST entries of cluster volumes, and of single-server volumes with a tier (E55) | log replicas; single servers | yes, behind `requires=cluster` or `requires=tier` |
| Markers: `MK_COLD` body (Z, object records, `.ec` and `Y` files, retired oids, `stripe_version`), and new `MK_GC` (12), `MK_DIGEST` (13), `MK_DROP_OBJ` (14), `MK_UNSETTLE` (15) | WAL; `MK_TYPE_LAST` moves to 15 (src/marker.h:41) | yes |
| The settle intent record (an extension of `parts_commit`'s) | log replicas | yes |
| `FORMAT_VERSION` | stays 9 (E17) | |
| A single server's tiers (E55): `TIERS` (beside VOLUMES; checksummed append-only records: store id, path, class, state), (and single-copy flag), each tier store's META with the database's `db_id` (in the root META), `requires=tier` in the META of a volume with a tier, `.ec` entries in its MANIFEST naming the store id; the settle markers of R7 | single-server databases | yes, behind `requires=tier`; `FORMAT_VERSION` stays 9 |
| Metadata snapshot version 3: `struct meta_volume` (server/meta.h:161-183) gains `redundancy` (kind, k, m), `min_isr` default by mode, the stripe list (up to 24 positions: 20 plus 4 moving; each pos, node, rid, state, barrier), `stripe_version`; members gain roles, holds (with their bound), chunk store ids and states, DESTROYED; *2026-11-11*: the declared role and the capability, and per disk its store id, class, uses, size and state (E49, E52) | metadata group | at 1.0 with the rest of the metadata; until then each minor release converts the previous one's in place, at the `CLUSTER_VERSION` entry (E39) |
| New ops at the end of `enum meta_op_kind` (never renumbered, server/meta.h:266-297): `STRIPE_SET` (if stripe_version == v), `NODE_HOLD`, `VOLUME_REDUNDANCY`, `NODE_REPLACE`, `STORE_STATE`, `NODE_DESTROYED`, `GC_SUSPEND`; *2026-11-11*: `STORE_ADD`, `STORE_CLASS` (E52) and the cluster setting `log_on_hdd` (E50) | metadata group | yes |
| Peer protocol version 3 (`CL_VERSION`, server/cluster.c:96): CHUNK_PUT (with cell sums), PUT_OK, CHUNK_READ (oid, pos, offset, length), CHUNK_DATA, CHUNK_LIST, CHUNK_DEL (naming its `MK_GC`), all with volume and epoch; heartbeats gain "opening" and "fail-stopped" | peers | no; a link needs the same peer protocol version on both ends (E37); a release that changes it is a maintenance window (E39) |
| Cluster version (`CLUSTER_VERSION`, a new metadata op), the compat triple in HELLO, heartbeats and the metadata group, versions in snapshots, log entries and per-server state files (E37, E39); chunk, `.ec` and part headers carry their format version, and a mismatch is refused at open (E40) | metadata group, servers | the mechanism yes; no reader of older versions before 1.0 (E40) |
| `BACKUP_PIN` (a new metadata op); the backup directory: a checksummed manifest naming each volume's P, objects as part files by oid, young parts, WAL tail, the metadata snapshot (E42) | metadata group; backup target | yes; a release restores its own backups and the previous minor's |
| `TSDB_WHY_LOST` = 18; TOPOLOGY's per-volume health gains both margins and `LOST`; `TSDB_WHY_FULL` (4) and `TSDB_WHY_UNREACHABLE` (16) exist | client protocol | the code yes; whether it needs a protocol number depends on whether every 1.0 client treats an unknown reason as retryable (to check in R8; if not, it waits for the next protocol number) |

## 9. Storage and compute separation (the user's question)

**Yes, for EC volumes.** E20-E22.

### 9.1 Roles

`tsdbd --cluster --role compute|storage|both` (default `both`), recorded in the member record (`META_OP_MEMBER_ADD`
flags, server/meta.h:267).
- **compute**: leads volumes, holds log replicas (WAL, memtables, young parts, `.ec` index files; local NVMe),
  answers queries, keeps the granule cache, runs settles and merges. Needs CPU, RAM and NVMe sized by §4 (terabytes
  at high ingest or large settled volumes).
- **storage**: holds chunks only, one store per disk (E27). No engine, no WAL, no queries: a chunk server (put, read
  range, list, delete, scrub). Needs capacity. HDDs serve it with the costs §3.4 and §5.3 state (a cold granule read
  is a seek; a rebuild is bound by the disks), and E31's hedging; NVMe where old data is decoded often.
- **both**: either, today's servers.

*2026-11-11* (E49, §15.5): the role a server can take follows from its disks. A fast disk (NVMe, SSD) makes it
compute-capable, a capacity disk (hard drive) storage-capable, so one server with NVMe and hard drives is `both` with
the young tier on the NVMe and the chunks on the hard drives; `--role` only narrows what the disks allow.

`replicas=3` volumes have no stripe group: all three copies are engines on compute-capable servers. True separation is
therefore an EC property.

### 9.2 Scaling each

More ingest or queries: add compute servers; leadership and log replicas move to them (young data and indexes:
terabytes at the sizes of §4, hours at `balance_mb_s`). More bytes: add storage servers; stripe positions rebalance
(copies, no decode; a new server fills at its own receive rate, §6.2). Neither touches the other.

### 9.3 What a compute server's death costs

Nothing durable while its volumes' ISRs were full (E4, E24): its log replicas are 1 of m + 1 copies, and its volumes
fail over in seconds. Its cache is gone; the new leaders warm theirs (§3.3): statistics queries are warm at once (the
`.ec` index files are on every log replica), decode queries pay a round trip per cell until the prefetch of the last
`cache_warm_h` is done, bounded by the free cache. Its log replicas are rebuilt elsewhere after `rebuild_after`: with
5 TB of young data and indexes on it and 3 other compute servers sending at 200 MB/s each, about 2.3 h (at line rate
about 11 min). A separated cluster needs at least m + 2 compute servers (E21), so there is a server
to rebuild onto.

### 9.4 The object store

The settled tier talks to its backend through the four calls of design-cluster.md §9.2 (put, read range, delete,
list) extended with the geometry; the cluster's own storage servers are the first backend. A second backend is
intellistream-objects (`isod`, S3 API; design-cluster.md "Later") or any S3: tsdb then runs compute servers only,
isod codes, places and rebuilds, and the tsdb cluster's storage role is empty. That costs S3 latency on decode reads
(milliseconds, hidden by the cache for recent data) and one more system to run; it gains a store shared with other
data and independent of tsdb's release cycle. **0.11.0 builds** the roles, the interface and the own backend (R6-R9);
**later** (0.12 or after): the S3/isod backend, then the external cold tier of design-cluster.md §9 as a second
settled tier behind the same interface (older than `cold_after`, moved from the own backend to S3).

## 10. Tests

### 10.1 On one machine

`tests/cluster/` starts N tsdbd processes (22 for 16+4 with two spares), each with `--port 0 --port-file F
--exit-with-parent=$BASHPID`, its own data and state directories under /var/tmp (several chunk stores each, to test
E27), roles as the test asks; the harness reads "PORT PID" and checks the pid (AGENTS.md "Ports"). Each geometry (4+2,
8+2, 8+4, 16+4) and `replicas=3` runs the same suite: insert, settle (including a key of three 4 GiB parts, E5), query
against a single-server oracle (bit-identical answers), kill a position, degraded reads, OUT and rebuild, back before
and after (after 31 minutes: the bytes moved are those settled meanwhile, E28), node replace, a dead disk, moves,
conversion. Timings for 8+4 and 16+4 come from one machine only; release notes claim no hardware numbers for them.
*2026-11-11*: each server is started with one `:nvme` and one or more `:hdd` disks on two scratch file systems, classes
always stated, each disk set up by `tsdb disk init` and `--allow-shared-device` set (E58); some servers NVMe-only or
HDD-only. The hard-drive disks run under outagefs's `slow` and `rate` only in R13's, R15's and R11's runs (§15.9), so
the gate hours of R8, R9, R12 and P6 do not change.

### 10.2 Simulators and unit tests

- `test_rs`: every erasure set of size 0 .. m for every k ≤ 16, m ≤ 4, every kernel byte-equal to scalar (as
  objects:src/codec/rs.h:13-15 does), range decode equal to whole-cell decode; in `tests/test_levels.c` for the CPU
  levels (AGENTS.md "CPU levels").
- `fuzz_chunk` and `fuzz_ec`: chunk files and `.ec` files from any bytes (AGENTS.md "Damage is never silent").
- `sim_place`: a seeded simulator of placement, failures (servers and disks), holds and their bounds, the mass-OUT
  guard and its bound, rebuild order and pace by margin, return during rebuild, node replace, moves, and the settle and
  GC states, with a checker (no object below k readable positions while the scenario allows it, margins never lowered
  by a planned move, rebuild order by margin, every list valid, E9; no `whole` before the barrier; no chunk deleted
  that a committed or pending marker names) and mutants as `sim_repl` has (design-cluster.md "Corrections from step
  2's simulator"): `Y` computed before the gate, the committed-only orphan rule, an `.ec` part retired by a settle,
  `whole` without the barrier, an unbounded guard. It also computes a mean time to data loss per geometry (§5.3).
- The settle under `test_lww_realtime`-style concurrent small writes **and** deletes, replaces and large inserts into
  the settling key; under crashfs power loss at every step of §2.3 (one process, the directory backend); a replica
  restart inside the retirement window; with the mutant "`Y` computed at step 4 without the gate", which the test must
  kill.

### 10.3 jepsen-cluster

Nemeses, per geometry: kill, partition, **partial partition** (leader to storage up, storage to metadata down), pause
(past `obsolete_part_lifetime_s`), power loss, **disk loss** (a server's directories removed; it comes back empty, new
rids), **single-disk loss** (E27), **disk loss of a storage server back within 30 min**, **slow node** (one storage
server's reads delayed past its hedge time; p99 stays bounded and total reads below 1.05 × baseline, E31), **slow
then leader disk loss** (E24), **rebuild during writes**, **node back during rebuild**, **two at once**, **m at once**,
**m + 1** (the checker expects reported unavailability, or loss after `node destroyed`, and nothing else wrong),
**chunk rot** at rest including parity (reads decode around it, scrub repairs it), **parity rot then m failures**,
**`STRIPE_SET` during a settle**, **a crash between `MK_COLD` and its commit with a failover and GC**, **a 2-hour hold
under ingest** (settles resume, catch-up without a snapshot, nothing fail-stops), clock jumps. Workloads include
deletes, replaces and large inserts into settling and re-encoding keys. Checker: design-cluster.md §13.2's (no lost
acknowledged write, last write wins, no resurrection) plus: every query answer equals the oracle or is an error that
names a partition.

Added for production (E45): **upgrade** (a rolling upgrade by `node prepare` under load, with kills during it, a
failover between servers of two versions, a server rolled back before finish, then finish; §10.6), **backup** (backups
running through failovers, settles, re-encodes and GC; each restored into a fresh cluster and every volume compared with
the oracle at its P), and **conversion** (a redundancy change running under the other nemeses). Before the tag the
whole set runs on the release commit on 4+2, 8+2, 8+4, 16+4 and `replicas=3`, `JEPSEN_SECONDS=300` per run (about 24
machine-hours).

### 10.4 Benchmarks (quiet machine, 5 runs, the mean; AGENTS.md benchmark policy)

Per mode (`replicas=3`, 4+2, 8+2, 8+4, 16+4): `build/ec-bench` (encode, decode, range decode per k, m, cell size, per
kernel; 7950X, 9950X, i9-10850K); insert-bench and tsdbc-bench through the cluster; the 100 M row query suite with a
warm cache, a cold cache, one position down and m down, on NVMe and on an HDD-backed storage configuration; settle
throughput and the settle fence's length; rebuild time against foreground p99; network bytes per row; the digest's
cost (EU6). On one machine the numbers compare the modes; absolute numbers need servers with disks.

### 10.5 Soak (E45)

72 hours on one machine (the 9950X; no benchmarks beside it), `tests/cluster/soak.sh`: 22 tsdbd processes with
several chunk stores each, volumes of every geometry and `replicas=3` in one cluster, roles mixed (some `compute`,
some `storage`); ingest at a steady rate with the 100 B benchmark's sensor mix plus deletes, replaces, large inserts
and retention short enough to drop partitions daily; queries checked against an oracle, sampled. A slow nemesis: one
fault every 20-40 minutes drawn from §10.3's set (an OUT and rebuild, a dead disk, a node back during rebuild among
them), a rolling restart of every server by `node prepare` every 12 hours, a backup every 6 hours with a restore and
comparison daily, one redundancy change. Passes when: the checker finds nothing; no fail-stop but injected ones;
after the first 24 h each server's RSS, open descriptors and threads stay within 10% of their level then; disk use
follows ingest minus retention (GC, retired parts and retained segments converge); the settle backlog and the
oldest unscrubbed object stay bounded; the alerts fire for the injected faults and for nothing else. With servers with
disks available, a 24-hour run on them before the release notes claim hardware numbers.

### 10.6 Upgrade test (E37-E41, E45)

`make upgrade-test` builds the base (the previous release tag, from `git archive`; before 0.11.0 is tagged, this
tree) and this tree with `-DTSDB_TEST_BUMP`, which changes **behaviour only** with a dummy rule gated on the cluster
version, and with `-DTSDB_TEST_BUMP_FORMAT`, which bumps the disk format, the snapshot version and the peer protocol
version once each (a header version, a snapshot field, a peer message). Two parts:
- **Within the same formats** (the first build): a cluster of every geometry starts on the base and takes
  jepsen-cluster's workload; the servers are upgraded one at a time by `node prepare`, with a kill during an upgrade,
  a failover from an old leader to a new one and back, and one server rolled back to the base before finish. Checked:
  the dummy rule is not applied anywhere before `upgrade finish` and is after it; the base binary then refuses to
  start; and jepsen-cluster's checker over the whole history.
- **Across a format change** (the second build, once per member of the triple): `upgrade check` and `node prepare`
  refuse with both triples and the window message, holding and stopping nothing; a server of the second build refuses
  to start on the base's directories and, started on an empty one, is refused at HELLO by the base's peers; a cluster
  of the second build refuses to join the base's. The window procedure of §14.2 then runs on the one-machine cluster
  (backup, stop, install, carry over by export/import, start) and the data compares equal to the oracle.
A short form (one geometry, 5 minutes) runs with the cluster tests of §10.1. From 0.12 on the base is the real
previous tag, and docs/release.md runs the long form before each tag.

## 11. Build plan for v0.11.0

Track-days as in design-ha-modes.md §7 (build, review and fix rounds); gate hours are machine time beyond `make test`,
`make asan`, `make tsan`. Prerequisites from that plan, all in 0.11.0 (E23): the rest of S1 (3-4 days, 6 h), S2 (2-3,
3 h), S4 (1-2, **4 h: its own jepsen-cluster run with name creation**, since N7's jepsen-ha is not in 0.11.0), R2 (2-4,
6 h), R3 (3-4, 12 h), R4 (2-3, 12 h): 13-20 track-days, 43 gate hours.

| Step | Content | Gate | Gate hours | Track-days |
|---|---|---|---:|---:|
| R5 | EC codec in `src/ec/`, copied unchanged by `tools/sync-ec.sh` with `src/ec/UPSTREAM` (E32), after the upstream change that replaces gf256.c's constructor by a selection call that `src/cpu.c` makes; fitting AGENTS.md "CPU levels" (`tools/check-isa.sh` must allow the AVX-512 and GFNI kernels behind target attributes, or they move into `src/levels.h`); `test_rs`, `build/ec-bench`; the check that no client library or package holds a codec symbol; docs/licenses.md's row moves from planned to present | numbers on three CPUs | — | 1-2 |
| R6 | Object layout, chunk files (header with epoch, cell sums), chunk stores per disk with ids and `EPOCH`, the directory backend; range decode; `fuzz_chunk`, `fuzz_ec` | fuzz 1 h each | 2 | 2-3 |
| R7 | Engine: `.ec` parts (the read seam, the cache, merges skip them), `ppos` in MANIFEST, the settle gate and cut, the settle intent, DIVERGED on a part above Z, encode-time checks, `MK_DIGEST` and `settle_keep_s`, per-part and coarse settles, re-encode, retention drops; X1's cold boundary folded in | `make jepsen` (single server, directory backend: kill, power during settle, deletes/replaces/large inserts into settling keys, the `Y` mutant) | 8 | 5-7 |
| R8 | tsdbd: roles, chunk peer messages, epoch fences (durable per volume), `MK_GC` and the orphan rules, GC suspension, degraded reads with E31's hedging and `read-avoid`, metadata v3, TOPOLOGY with both margins, unavailable/corrupt/lost, `TSDB_WHY_LOST` in the four clients, `min_isr` by mode | `make jepsen-cluster` (kill, partition, partial partition, pause past the lifetime, disk loss, chunk rot, crash between `MK_COLD` and commit) on 4+2 and 16+4; `make test-clients` | 12 | 6-8 |
| R9 | Failure handling: DOWN/OUT at `rebuild_after`, opening hold and fail-stop fast path, bounded holds and guard, degraded settles, retention for DOWN members, backpressure (`TSDB_WHY_FULL`), checked stripe rebuild by margin and pace, the `whole` barrier, return during rebuild and `node replace`, moves and the balancer with `balance_mb_s`, `node drain`; `sim_place` with its mutants and MTTDL | sim_place soak; jepsen-cluster (rebuild during writes, back during rebuild, two, m, m + 1, slow node, slow then disk loss, 2-hour hold, `STRIPE_SET` during a settle) on all four geometries | 24 | 6-8 |
| R12 | Scrub (E25, §5.9): chunks, `.ec` files and young parts, damage counts and disk OUT, health age, metrics | jepsen-cluster: rot (parity included) then m failures | 6 | 2 |
| R13 | Disks **with classes** (E27, §5.5; *2026-11-11*: E47-E58, §15): `tsdb disk init`, `--disk PATH[:CLASS]`, detection and its fake `/sys` test, the start checks (META, `st_dev`), disk META, the member record's disks, `STORE_ADD`/`STORE_CLASS`, roles from disks, placement and balancing by class (log replicas on fast disks, chunks on capacity disks, one-class stripe groups, projected use), local chunk moves and drains, filling per disk and per use with the four alerts' metrics, each disk a fault domain (probe, `disk_stall_s`, worker pools per disk, per-disk quiesce), device DOWN/OUT, rebuild within a server, a lost fast disk with the chunk stores serving on, the adoption runbook (`--adopt`), `disk out`, `disk replace`, `disk class`; `sim_place` with disks and classes | jepsen-cluster: EIO on one hard drive under load, a stuck NVMe, single-disk loss of each class, disk loss back within 30 min, a fast disk filling to FULL under ingest, on servers with two classes on outagefs; `sim_place` soak with the class invariants and mutants (4 h) | 16 | 6-8 |
| R14 | Operator CLI (`node hold/replace/drain/destroyed`, `disk ...`, `volume move/redundancy/salvage/gc`, `cluster status` with both margins, rebuild progress and settle backlog) and metrics (docs/metrics.md) | test_server cases | — | 2 |
| R15 | *2026-11-11*: a single server's local tier (E55, §15.7): `TIERS`, `tsdb DIR tier add/set/lost/salvage/status` with the path and device checks, tier store META with `db_id`, `requires=tier`, objects coded k' + m across stores (R5's codec, R6's cells; mirror, k' + 1, 4+2 by default, `--single-copy`), the settle by `tier_after_s` and by `tier_high_pct` on R7's settle, reads by `pread` through the seam with degraded range reads, the local rebuild of a lost store, scrub with repair, the open check and orphan scan, FULL before ENOSPC with the tier metrics and `dist/alerts/tsdb.rules.yml`, followers and health domains over tier stores, `fuzz_tiers` | `make jepsen` on one server with a six-store tier on outagefs (kill and power during a settle, EIO and stuck on one store, one store lost and rebuilt, m + 1 lost and salvaged, the tier filling under ingest); fuzz 1 h | 10 | 5-6 |
| R10 | Redundancy change on a running volume (§6.4: order, re-encode, `MK_UNSETTLE`); in 0.11.0 (E33) | jepsen-cluster with a conversion running | 6 | 3 |
| R11 | Benchmarks (§10.4, NVMe and HDD storage) and the sizing numbers for P5's guide (compute disks, index and rebuild time, m = 4 above 20-30 TB a node), README section, AGENTS.md invariants (the settle cut, chunk fences, `MK_GC`) | the numbers | — | 1-2 |
| P1 | Versions (E37-E41, §14.1-14.2): the compat triple (`tsdbd --compat`, HELLO, heartbeats, recorded at `init`) and the refusals (peer link, open, a binary older than the cluster version); `CLUSTER_VERSION`, `upgrade check`, `upgrade status` and `upgrade finish`; every behaviour change gated on the cluster version; `node prepare` (triple check, margin check, hold, leadership and Raft-leader hand-off, one at a time); the window message; versioned state files refused on a mismatch; the golden `tests/data/meta-snap-v3.bin` for converters. No reader of older versions and no AGENTS.md change (E40) | meta-soak with members of two builds of one triple (a member held back stands in for the old build); test_server cases | 4 | 3-4 |
| P2 | Upgrade test (§10.6): `make upgrade-test` with the behaviour-only bump (a rolling upgrade within the formats, the upgrade nemesis in jepsen-cluster) and the format bump (the refusal test across a change, the window procedure) | `make upgrade-test`, long form | 6 | 1-2 |
| P3 | Backup and restore (E42, §14.3): `BACKUP_PIN`, `cluster backup` (incremental, manifest last), `backup verify`, `restore-data` into a new cluster (new identity, GC suspended, feeds reset), `volume salvage --from-backup` | jepsen-cluster `backup` nemesis with restores compared against the oracle | 6 | 4-5 |
| P4 | Alerts (E43, §14.4): `dist/alerts/tsdb-cluster.rules.yml`, the metrics they need (docs/metrics.md), a test per rule | the rule tests | — | 1-2 |
| P5 | The production operator guide (E44, §14.5): `docs/cluster-operations.md`, systemd units in `dist/`, runbook scripts in `tests/cluster/runbooks/` | the runbook tests | 3 | 3-4 |
| P6 | Release gates (E45) on the release commit: jepsen-cluster's full set on all four geometries and `replicas=3` (§10.3), with the device-class cases of R13 (*2026-11-11*), the 72-hour soak (§10.5), the upgrade test, the reviews, and the fixes they find | the gates themselves | 98 (26 + 72) | 2-4 |

R5-R15: **39-51 track-days**, 84 gate hours (R11 is 1-2 since the guide moved to P5; *2026-11-11*: R13 6-8 and 16
instead of 2-3 and 6, R15 5-6 and 10, E56; it was 30-40 and 64). P1-P6: **14-21 track-days**,
117 gate hours (P6 +2, *2026-11-11*). **Total for 0.11.0, with the prerequisites: about 66-92 track-days and 244 gate
hours (about 10.2 machine-days, 3 of them the soak) before reruns after fixes** (*2026-11-11*, with device classes as
revised after review; it was 57-81 and 222) (2026-11-09, after the user's answers; it was 44-60
track-days and 107 gate hours with R10 as the first cut and the cluster data a preview, and 59-84 and 226 in the
draft that read the previous minor's files in place, which the user rejected the same day with E40). On three tracks that is
roughly 6-8 weeks of calendar (nearer 8 with device classes): 4-6 weeks of building and reviewing, set by the review
rounds, then P6's gates run in
series on the release commit (about 5 machine-days), and a failed soak costs another 72 hours. R5 and R6 need nothing
and can start now; R7 needs R2; R8 needs R3 and S2; R12 and R13 need R8 (R13's balancing by class lands in R9's
balancer, so R13 follows R9; R9's `sim_place` models each server's disks from the start, one class, and R13 adds the
classes, their invariants and mutants, and a soak in its gate); R15 needs only R7, so a single server's tier can be built and
gated before the cluster
steps; R9 needs R4 and R8; R14 runs alongside R9;
P1 needs R8 (it versions metadata v3) and should land before R9, so that every later writer is gated from the start;
P2 needs P1 and R9; P3 needs R7 and R8, its nemesis R9; P4 and P5 need R14, P5 also P1 and P3; P6 comes last.

**What waits**: the S3/isod backend and the external cold tier (§9.4); local repair codes; shipping merges; partial
rebuild of a returning log replica from position ranges (design-cluster.md "Later"; E29's retention covers the
30-minute and held cases); direct-mode clients (S3 of design-ha-modes.md, forwarding serves until then); upgrading
a rack at a time (E38); a cluster-wide consistent backup (E42); a disk cache of chunks on NVMe and more classes than
fast and capacity (E48); moves of a log replica between two disks of one server (E51); a tier for shared-mode volumes
and tier calls in the bindings (E55). Nothing of R5-R15 or P1-P6 moves to 0.12 (E33).

## 12. What intellistream-objects gives, and what not

| Taken | From | Why |
|---|---|---|
| Systematic Cauchy RS, GF(2^8) 0x11d, six kernels, plans per erasure pattern | objects:src/codec/rs.h:1-16, :63-78; gf256.h:1-24 | Exhaustively tested, interoperable, data readable without decoding |
| A failing survivor is an erasure; checks never optional; the check that chooses is not a short non-cryptographic sum | objects:src/recovery/rebuild.c:16-42 | A wrong input gives a plausible wrong answer |
| Geometry per object, never from today's settings | objects:src/recovery/rebuild.c:26-28 | Makes E2 safe |
| Derived k for small objects | objects:src/codec/geometry.h:8-18 | Small objects cost no more than copies |
| DOWN vs OUT per server **and per device**, holds, the mass-OUT guard with its bound, the opening hold, the fail-stop fast path, DESTROYED, damage counts per device | objects:src/cluster/cmap.h:66-77, :168-170, :895-925; objects:src/mds/server.h:84-100 | No rebuild storms on a reboot or a rack's power; no endless hold; a disk is not a server |
| Most endangered first | objects:src/mds/server.h:1852-1860 | Smallest margin first |
| Rebuild pacing: concurrency, byte budget, background class | objects:src/recovery/worker.h:1-21, :67 | Foreground first |
| Placement: one per host, rack cap, rack-survival condition, room to rebuild, positions never reused, balancer bounds | objects:src/placement/README:84-99, :134-160, :178-200 | The same problem |
| Degraded writes, bounded (E10: k + m − 1, after a wait) | objects:src/codec/geometry.h:94 (`min_write_shards`) | Settles must not stall for hours |

Not taken: objects' segments and manifests (tsdb's part is the segment and the `.ec` file the manifest), its mirror
rule (derived k does it), its placement groups as a separate table (a volume's stripe list is its PG), its chunkstore
on raw devices (chunk files on a file system per disk suffice for immutable MB-size objects, EU4), its whole-shard
decode on reads (E11), its k + 2 write floor (E10 allows one missing position only after a wait).

## 13. After review (2026-11-09)

Two reviews of the first draft (894e796): durability and consistency (1 critical, 4 high, 9 medium, 5 low) and
operations and performance at 10-100 TB a node (2 critical, 9 major, 9 minor). Where each went:

| Finding | Where |
|---|---|
| Durability C1: the settle retired acknowledged writes in neither `S` nor `Y` | E6, §2.3 (gate, cut Z, `ppos`, DIVERGED, the proof over parts and memtables, re-encode by the same protocol), §10.2-10.3 tests and mutant |
| H1: rot or a merge bug on the leader becomes the only lineage | E26, §2.3 steps 2 and 7 (`MK_DIGEST`, `settle_keep_s`) |
| H2: chunk deletion not fenced | E14, §2.2 (`EPOCH`, writer epoch), §2.3 "Crash and failover", §5.7, `MK_GC` |
| H3: `min_isr = 1` does not survive m losses | E4, E24, §5.8; decided by the user (E35) |
| H4 and operations M7: no scrub | E25, §5.9, R12 |
| M1: rebuild checks nothing; trial decode by a 32-bit sum | E11, E26, §3.2, §5.3 |
| M2: the second settle retires the first object | E6, §2.3 step 6 (only non-`.ec` parts, and named oids) |
| M3: `.ec` source, swap atomicity, open needs the network | §2.2 (`.ec` shipped as a pending file), §2.3 step 6 (one intent), §2.4 (class check at the settle, index-only open) |
| M4: `Y` files gone before catch-up | §2.3 "Crash and failover" (kept with retained segments), design-cluster.md §5.2 note |
| M5: stripe changes race settles; `whole` too early | §2.3 step 5, §5.3 (barrier), §5.4 (delete after CHUNK_LIST) |
| M6: a server back with a lost disk is taken as current | E27, §2.2 (store ids, chunk server fail-stop), §5.2 (CHUNK_LIST both ways) |
| M7: lost vs unavailable; salvage drops young parts | E15, §3.2, §5.8 (`MK_DROP_OBJ`, `node destroyed`) |
| M8: no safe order for a redundancy change | E2, §6.4 |
| M9: tests miss the anomalies | §10.2, §10.3 |
| L1: wall clock in E5 | E5 (`MK_NOW`) |
| L2: position down after PUT_OK | E10, §2.3 step 5 |
| L3: unbounded guard | E12 (`out_guard_hold_s`) |
| L4: IO vs CORRUPT; statistics over lost partitions | E15, §5.8 |
| L5: citations | worker.h:67 (§5.3, §12), design-cluster.md:461-491 (§2.1) |
| Operations C1: partitions above 4 GiB never settle | E5, §2.3 (one object per part, one marker), §10.1 test |
| C2: rebuild time and exposure at 10-100 TB | E13 (pace by margin), §5.3 (table, formula, m = 4 rule, MTTDL), R11 guide |
| M1: a dead disk | E27, §5.5, R13 |
| M2: back at 31 minutes moves the data twice; no `node replace` | E28, §5.4 |
| M3: settles stop without bound; ENOSPC | E10 (degraded settle), E12 (bounded guard), E30, §4 sizing |
| M4: retention shorter than the 30 minutes promised | E29, §5.2 |
| M5: index 10x underestimated; compute state is TBs | §2.1, E21, §4 (sizing tables), §9.2-9.3, separated minimum m + 2 |
| M6: HDD performance and hedging overload | E31, §3.4 (HDD column), §10.3-10.4 |
| M8: 0.11.0 holds data but cannot upgrade | E18; decided by the user (E36: production; E40: today's pre-1.0 rule kept): rolling within a format (E37, E38, E41, §14.1), a window across a change (E39, §14.2) |
| M9: plan underestimated, steps missing, gate time, S4's gate, Totals | §11 (R7-R9 re-estimated, R12-R14 added, gate hours, S4's own gate), design-ha-modes.md §7 rows, Order and Totals; no cut line (E33) |
| m1: costs missing (merges, ingest, compute-only settle, index) | §4 |
| m2: adding a node is bound by its receive rate | §6.2 (`balance_mb_s`) |
| m3: geometry change takes a week | E2, §6.4 |
| m4: coarse partitions stay replicated a year | E5 (coarse keys settle part by part), EA2, EU5 |
| m5: the fence is longer than 1-10 ms with forced flushes | E5 (wait for a natural flush), §2.3 proof paragraph, §10.4 |
| m6: licence details | E32 (the relicence makes a grant unnecessary); docs/licenses.md |
| m7: small objects pay more | E7 (cell size per object), §4 |
| m8: warm-up may not fit, competes with rebuild | §3.3 |
| m9: opening hold and fail-stop fast path | E12 |

## 14. Production in 0.11.0 (E36)

The user made the cluster data path production in 0.11.0. This section is what that adds; the decisions are E37-E46.

### 14.1 Rolling upgrades within one compat triple (E37, E38, E41)

The compat triple is the disk format, the metadata snapshot version and the peer protocol version (client protocol
included, E41). Two builds with the same triple (every patch release, and a minor release that changes none of the
three) can run in one cluster; builds with different triples cannot.

| | Same triple, before `upgrade finish` | Same triple, after `upgrade finish` | A different triple in any of the three |
|---|---|---|---|
| Peer links | the same versions | the same | refused at the link, both triples logged (E37) |
| Directories and state files | opened as they are | the same | the open refuses, naming both versions (today's rule, E40) |
| Behaviour that every server must apply together | the old rule | the new rule | — |
| Taking a server back to the older build | yes | no: forward fixes or restore (E42) | not applicable |
| Operator conversions (`volume redundancy`, re-encodes) | refused while builds are mixed (E38) | yes | — |
| `node prepare`, `upgrade check` | proceed | proceed | refuse before anything is held or stopped, and say it is a window upgrade (E38, §14.2) |
| How long | no limit; a warning and an alert after 24 h | — | — |

The procedure, which the guide's upgrade runbook runs (E44):
1. `tsdb cluster upgrade check --to FILE`: the new build's triple equals the cluster's. If it does not, stop here and
   follow §14.2.
2. `tsdb cluster upgrade status`: every server on the old build, no volume below full margin, no backup running.
3. For each server, non-voters first, then voters, the Raft leader last: `tsdb cluster node prepare N --to FILE`
   (E38), stop tsdbd, install the new build, start, and wait until `upgrade status` shows N back in every ISR and
   stripe group. A server costs a planned leadership transfer (seconds), its restart and open (an `.ec` part opens
   from its index, so the open is the WAL replay of its memtables) and its catch-up: minutes, so a 20-server cluster
   takes one to three hours.
4. `tsdb cluster upgrade finish`: the `CLUSTER_VERSION` entry commits and the behaviour gated on it takes effect.
   Before this step, going back is step 3 with the old build.

The Raft core's messages travel inside peer messages, so a sync of `src/raft` that changes `raft_wire`'s encoding is a
peer protocol change under the same rule: a window upgrade. The upgrade test (§10.6) covers it.

### 14.2 A format change: the maintenance window (E39, E40)

What an operator does when `upgrade check` or `node prepare` refuses, or when the release's CHANGELOG says the disk
format, metadata snapshot version or peer protocol version changed. The CLI's message names both triples and says:
"this release changes the disk format / the metadata version / the peer protocol and needs a maintenance window".
Nothing was held or stopped. Then:
1. `tsdb cluster backup --to DIR` (E42), so the old cluster can be put back by the old release.
2. Stop every tsdbd. Clients get UNAVAILABLE and retry; the Pulsar sink buffers (the window is hours, not days).
3. Install the new release on every server.
4. Carry the data over as the CHANGELOG says: `tsdb DIR upgrade` on each volume directory (when the release ships
   one), the release's offline metadata conversion on every member (same snapshot and log in, same bytes out; the
   members compare its checksum before they start), or, when the release ships no converter, export from the old
   cluster and import into a newly initialised one (export names series, not ids). A window where only the peer
   protocol changed needs no conversion.
5. Start every server and `tsdb cluster upgrade finish`.

A server that starts on a directory of another format refuses and names both versions; a server that links to a peer
of another triple is refused at HELLO; neither damages data, so a skipped step is found at start. The format
freezes at 1.0 (AGENTS.md), after which this section is revisited. Golden files of each released snapshot version
(`tests/data/meta-snap-v3.bin`) are kept for the converters a release ships.

### 14.3 Backup and restore (E42)

A backup per volume is a cut at P, pinned so that nothing it names is retired or collected while it is copied; the
copy goes through the directory backend to a mounted target (production's NFS/ZFS servers; S3 when that backend
exists). Objects are stored decoded, as part files named by oid, which makes increments a set difference and makes a
backup independent of the geometry it came from. `backup` takes `--volumes` and a parallelism bounded by the
rebuild token bucket (design-cluster.md §8.4), below foreground and rebuild traffic. A restore is disaster recovery
into a new cluster: the old one must never run again beside it (the new identity makes its servers' peer links and
clients' cached topologies refuse each other). What a restore loses is everything after each volume's P; Pulsar or
the source replays it if it still holds it. `volume salvage --from-backup` is the partial case: objects of a volume
reported lost (E15) are put back by oid from the newest backup that holds them, and only the rows written after that
backup's P in those partitions are gone (objects the backup lacks are dropped by `MK_DROP_OBJ`, as in E15).

### 14.4 Monitoring and alerts (E43)

`dist/alerts/tsdb-cluster.rules.yml`, every rule naming its runbook: a volume at margin 0 in either tier (page) or
below m (ticket); a volume UNAVAILABLE, LOST or DIVERGED (page); a fail-stopped server (page); the metadata quorum
lost (page); a settle backlog older than `settle_wait_max_s`; young-tier disks above 80% (before E30's 90%
backpressure); a rebuild that made no progress for 15 minutes; scrub older than 2 × `scrub_days`; a server's clock
offset above half the lease bound (design-cluster.md U5); versions mixed for more than 24 h; the last complete backup
older than its schedule allows (a setting, 26 h by default). docs/metrics.md lists the metrics; each rule has a test
(P4).

### 14.5 The operator guide (E44)

`docs/cluster-operations.md`, for the people who run it, in the plain language of the README (one runbook per page
section, each with what tsdb does, what the operator does, and the commands):
- **Install**: the tarball (`make dist`), the systemd units in `dist/` (`Restart=on-failure` with a start limit: a
  restart after a fail-stop reopens from disk, which is safe, but a disk that fails again must not loop), chrony's slew
  bound (design-cluster.md U5), the disk layout (WAL and young parts on NVMe; one chunk store per data disk; the state
  directory on the OS disk, never restored from a backup), ports and the cluster secret, `cluster init` and its default
  redundancy (E34).
- **Sizing** (§4 and R11's numbers): compute disks for young data, settle waits and indexes; storage disks; rebuild
  time per geometry and node size; when to choose m = 4.
- **Adding and removing nodes**: `node add` with its role, the balancer's pace (`balance_mb_s`), `node drain` then
  removal, voters.
- **A dead disk** (§5.5), **a dead node** (§5.1-5.4: DOWN, holds, OUT, rebuild, `node replace`, `node destroyed`),
  **two or more down** (§5.8: unavailable against lost, salvage).
- **Rolling upgrades** (§14.1), **backup and restore** (§14.3), **monitoring and alerts** (§14.4): what each alert
  means and its runbook.

### 14.6 What is not production in 0.11.0 (E46)

- **Shared NFS mode** (design-ha-modes.md N0-N8): its go/no-go on the production NFS servers (N0) and the jepsen-ha
  matrix (N7) need maintenance windows that have not come. Steps merged by 0.11.0 ship behind `--mode shared
  --preview`; the single-server parts of N1 ship on their own as their step says.
- **The S3/isod backend and the external cold tier** (§9.4, E22): not built; the settled tier is the cluster's own
  storage servers. The directory backend is for tests and as a backup target.
- **Direct-mode clients** (design-ha-modes.md S3): not built; clients reach leaders through forwarding, which is
  production and costs one hop.
- **Hardware numbers**: correctness is claimed for all four geometries and `replicas=3`; absolute performance only for
  what was measured on servers with disks (E45).
- **Upgrades from 0.10.0's preview metadata group**: none; it held no data path and is initialised again (E39, E40).

Limits, not previews: each volume is backed up at its own position, not at one cluster-wide instant; upgrades go one
server and one minor release at a time.

## 15. Device classes: NVMe and hard drives in one server (E47-E58)

The user asked on 2026-11-11 whether hot data can sit on mirrored SSDs and cold data in an erasure-coded pool, and,
after hearing that this design gave each server one role, "yes do that, so one instance can cover both nvme and hard
drives". The two tiers already exist (E3): young data replicated on m + 1 servers, settled partitions erasure coded.
What was missing is the disk: until now a server was compute or storage, and a server with both NVMe and hard drives
had no way to say which disk is which. This section gives every disk a class and lets placement follow it, in a
cluster and on a single server. It was revised the same day after a review (§15.11): each disk is now a fault domain
of its own (E57), disks are set up explicitly (E58), and a single server's tier is erasure coded across its hard
drives (E55).

"Mirrored SSD" here is the log group: m + 1 copies of the young tier on fast disks of m + 1 servers, not RAID 1 inside
one server. RAID under the young tier is allowed (it is just a file system to tsdbd) but not needed; under chunks E27
advises against it (redundancy on redundancy).

### 15.1 Setting up and declaring disks (E47, E58)

```
# once per disk, on the server (first deployment, a new or replaced disk):
tsdb disk init /nvme0/tsdb                 # class detected: nvme; fdatasync latency measured
tsdb disk init /hdd00/tsdb:hdd             # class stated
# every start:
tsdbd --cluster ... --state-dir /boot-mirror/tsdb \
      --disk /nvme0/tsdb --disk /nvme1/tsdb \
      --disk /hdd00/tsdb --disk /hdd01/tsdb ... --disk /hdd23/tsdb
```

**Config keys** (2026-11-11). The track `config-files` adds `.properties` config files with dotted keys, so every disk
flag of this design also gets a config key: `storage.disks = /nvme0:nvme, /hdd1:hdd` for `--disk` (a comma-separated
list of `PATH[:CLASS]`, as the flag repeated), and likewise for the other flags of this section. Not built here: the
key names are fixed when that track meets this one, and a flag on the command line wins over the file.

Each `--disk` is one file system on one disk (or one RAID set the operator chooses to treat as one). The class is
`nvme`, `ssd` or `hdd`; when it is left out, `disk init` finds it:
1. `stat(PATH)` gives the device number; `/sys/dev/block/MAJ:MIN` gives the block device; a partition is replaced by
   its disk. A file system with an anonymous device number has no device to ask: NFS, ZFS, tmpfs, overlay, and btrfs
   (always, one device or several).
2. A device with `slaves/` (dm-crypt, LVM, md) is replaced by all of its slaves, recursively.
3. If every leaf is named `nvme*` and its controller's `/sys/class/nvme/<ctrl>/transport` is `pcie`, the class is
   `nvme`; if every leaf has `queue/rotational` 0, `ssd`; if every leaf has 1, `hdd`.
4. Otherwise `disk init` refuses: "/x: cannot tell its class (no local block device / devices of different kinds);
   add :nvme, :ssd or :hdd". That covers the file systems of step 1, loop devices, NVMe over TCP or RDMA (remote),
   and dm-cache or bcache over a mix.

`disk init` writes the **disk META** (a fresh store id, the class, and the `fdatasync` latency of 100 syncs of 4 KiB)
and syncs it with its directory. A fast class above `disk_sync_warn_ms` (2 ms at p50) gets a warning: an SSD without
power-loss protection, or a class stated wrongly. The first tsdbd started with the disk binds it to its node and
cluster (written into the META once, logged), and its next heartbeat brings it to the metadata group (`STORE_ADD`,
with class, size, uses and latency). From then on the metadata is the record:
- an explicit `:CLASS` on the command line that disagrees with it is refused at start, naming both, so a typo in a unit
  file does not move terabytes;
- detection runs again at each start, and a disagreement is logged and counted (`tsdb_disk_class_mismatch`, a health
  line), never refused: a kernel or controller firmware that reports a disk differently must not stop every server of
  a rolling OS upgrade (RAID controllers often report `rotational` wrongly, EU9);
- `tsdb cluster disk class NODE STORE CLASS` changes it on purpose (`STORE_CLASS`), and the balancer then moves what no
  longer belongs on it.

**Checks at every start** (E58). tsdbd refuses a `--disk`:
- that has no disk META: an unmounted hard drive leaves its mount point as an empty directory on the root file system,
  which must never become a new disk;
- whose META names another node or cluster, or a store the metadata marks DESTROYED (stores pending adoption after a
  lost state directory are the one exception, §15.6);
- whose `st_dev` equals that of `/`, of `--state-dir` or of another `--disk`: two paths on one file system would count
  its room twice, and a data disk on the boot device would fill the Raft log's file system. `--allow-shared-device`
  lifts this check for test harnesses and one-disk development boxes; health warns while it is set.

A known store whose path is back without its META, or not there, is that store missing: E27's device DOWN timer runs,
never a new disk. In cluster mode `--dir DIR` is one more disk (it also needs `disk init`); without a stated class it
is taken as `ssd`, a fast disk, with no detection and a warning naming the assumption, so a harness on tmpfs and
production on NFS start. The 0.10.0 preview's group is initialised again anyway (E18).

### 15.2 What lives where (E48)

| On a fast disk (`nvme`, `ssd`) | On a capacity disk (`hdd`) |
|---|---|
| log replicas `v<N>/`: WAL segments, MANIFEST, SERIES, STATES, REPL, FNO, young parts, `.ec` index files, retained segments (E29), retired parts of settles (E26), `ec_keep_local_h` copies, rebuild snapshots | chunk stores `s<N>/`: chunk files, the per-volume `EPOCH` |
| chunk stores too, but only on a server without capacity disks, up to the young tier's reserve (E50, §15.4) | log replicas only with `log_on_hdd`, on a server without fast disks, when no fast server is left (E50) |
| the disk META | the disk META |

Not on either: the granule cache (RAM, §3.3) and `--state-dir`, the Raft log and node identity, which the operator
places on a file system of its own (E58; the guide says a small mirrored boot SSD, so that the loss of a data disk
never takes the node's identity; tsdbd warns when it detects a hard drive under it). Each chunk store keeps everything
it needs on its own disk (its `EPOCH` files, and its chunk list, rebuilt by listing at start), so it outlives any other
disk of its server (E54).

### 15.3 Placement, rebuild and balancing (E50, E51)

- **Log group** (m + 1 replicas): compute-capable servers (E9); inside a server the fast disk with the fewest log
  replicas weighted by their ingest (WAL bytes a second over the last hour) whose projected use stays below
  `young_move_pct`, `nvme` before `ssd` on a tie. A `replicas=3` volume's three copies are log replicas, so they too
  live only on fast disks. With `log_on_hdd`, a hard drive is chosen only when the group cannot be filled from servers
  with fast disks.
- **Stripe group** (k + m positions): storage-capable servers, one position each (E9); inside a server each chunk goes
  to a chunk store below its mark, by free room (E27). The balancer picks a stripe group's servers from one class first
  (judged by the stores the group's chunks are on) and mixes only when one class has fewer than k + m servers that pass
  E9's rules; health lists mixed volumes.
- **Rebuilds and moves never change the use**: a log replica is rebuilt as a learner on a fast disk, a chunk on a chunk
  store. Nothing is placed on a disk whose projected use after the copy passes its mark (`young_move_pct`, or the fill
  mark for chunks), and a balance move needs its target to end at least 10 points below its source, so two disks near
  their marks do not trade replicas back and forth. Room to rebuild is counted per use (§5.6's fill mark over chunk
  stores; E21's m + 2 rule over servers with a fast disk), and `cluster status` shows both.
- **No log replica moves between two disks of one server in 0.11.0.** A replica that must leave a filling disk moves to
  another server: design-cluster.md §8.1's learner, added before the old replica is dropped, so the in-sync count never
  falls. A learner on the same node would be a second replica of the volume there, which `REPLICA_ADD` refuses
  (server/meta.c:1385-1386); the ISR is a list of node ids (server/meta.h:174), and relaxing that would let one server
  count twice towards `min_isr` (E24). After a fast disk is lost, its replicas are removed first; a replacement may
  then go to another fast disk of the same server, one replica per node throughout. Moves within a server, with
  (node, rid) as the identity and `min_isr` counted by distinct nodes, are later work.
- **Local chunk moves** (one chunk from one store of a server to another): copied as `tmp_*` onto the target, fsync,
  rename, directory fsync, the local index updated, then the source unlinked and its directory synced. A crash leaves
  at most two copies; the listing at start checks both against their cell sums, keeps the one on the store E50 wants
  and unlinks the other. Paced by `balance_mb_s`, never on the network. Used to drain a store that still reads
  (`disk out`, `disk class`) and when hard drives are added to NVMe-only servers.
- **Adding hard drives to all-flash servers** (the usual upgrade): E50 is a rule for new chunks; once a server has a
  capacity disk, its chunks on the NVMe drain onto its hard drives by local moves. During a rollout server by server,
  stripe groups are judged by where their chunks are, so health lists the volumes whose groups are mixed until the
  drains finish, and the balancer moves no position across servers for it. The runbook (E44) "add hard drives to
  all-flash servers" gives the expected traffic: the server's own chunks, once, all local.

### 15.4 Filling, capacity accounting and alerts (E53)

Each disk reports in its heartbeats its size and its bytes by use: young data (WAL, young parts), retained segments
(E29), retired parts of settles (kept `settle_keep_s`, E26), local copies (`ec_keep_local_h`), index (`.ec` files),
chunks. The metadata leader sums them per use and class; `tsdb cluster status` prints (§15.5's example):

```
class  disks  size     young   retired  index   chunks   free    marks       days to mark
nvme   22     169 TB   35 TB   35 TB    20 TB   0        79 TB   75/80/90%   -
hdd    132    1.32 PB  -       -        -       1.10 PB  220 TB  91%         7
```

Percentages are of the whole disk, except the young marks of a shared disk, which are of the room its chunks leave
(size minus chunk bytes).

| Disk | 75% | 80% | 90% | Chunks |
|---|---|---|---|---|
| fast, log replicas only | warning alert | its log replicas move to other servers; no learner or move is placed on it | inserts refused (UNAVAILABLE, `TSDB_WHY_FULL`) for the volumes with a log replica on it; retired parts whose `MK_DIGEST` matched are deleted at once; merges whose output would not shrink it wait | — |
| fast, shared with chunks (E50) | as above, of the room the chunks leave | as above | as above | new chunks stop where the room they leave falls below the young reserve, or at the fill mark |
| capacity | — | — | — | §5.6's fill mark; rebuilds may go to `full`; settles that cannot place their chunks wait |

The **young reserve** of a shared disk is twice the 7-day peak of its young-tier bytes (young, retained, retired,
local, index), and at least 10% of the disk. On §4's 8+2 cluster built all-flash (about 100 TB of chunks and 8 TB of
young tier on some 120 TB of flash a server), chunks then fill to about 87% (the disk less the
reserve, below the 91% mark) instead of a fixed 60%, and the young tier sits at half of the room they leave.

Between 90% and ENOSPC (which fail-stops that disk's replicas, E57) only growth other than inserts is left: retired
copies, which a settle backlog releases all at once after a full or outage episode and which free nothing for a day;
learners; merge outputs. Each is bounded or stopped there, so the disk does not fill with refusals already on. The
chunk stores' fill mark is counted over all of them whatever their class, so an all-flash cluster has a mark and an
alert like a mixed one; stores that fill make settles wait and young data grow on the fast disks, which is why the
chunk alert predicts the day.

E30's refusal already existed; it is now per disk, because an NVMe of one server fills by the volumes placed on it,
not by the server's total. The four alerts join `dist/alerts/tsdb-cluster.rules.yml` with runbooks (E43, E44):
`TsdbFastDiskFilling`, `TsdbInsertsRefusedFull`, `TsdbChunkStoresFull7d{class}` (a linear prediction over the last
day), `TsdbNoRoomToRebuild{use}`.

### 15.5 Mixed clusters and roles (E49)

| A server's disks | It can be | Typical use |
|---|---|---|
| NVMe and hard drives | compute and storage: young tier on the NVMe, chunks on the hard drives | the user's case: one instance per box |
| NVMe (or SSD) only | compute and storage: young tier and chunks share the NVMe (chunks up to the young reserve) | small or all-flash clusters, today's `both` |
| NVMe only, `--role compute` | compute | separated clusters (§9): leaders, young data, indexes, cache |
| hard drives only | storage | chunk servers |
| hard drives only, `log_on_hdd` on | storage, and compute only when too few fast servers are left (slow inserts) | a cluster with no or few fast disks; health warns |
| any, `--role storage` | storage | a box kept out of leadership |

A cluster may mix all of these. `cluster init`'s default redundancy (E34) counts storage-capable servers; with mixed
servers `ec=4+2` with 7 of them is the default as before. Ingest capacity (§4) counts compute-capable servers; sizing
(§4) is now per use: the compute numbers of §4 are NVMe, the chunk numbers hard drives. A worked example, the 8+2
cluster of §4 as 11 mixed servers: per server two 7.68 TB NVMe disks (15.4 TB: 3.2 TB young data, 3.2 TB of retired
parts from the last day's settles (`settle_keep_s`), 1.8 TB of index: about 53%, with retained segments and headroom
to 75%) and 12 × 10 TB hard drives (100 TB of chunks below the 91% mark of §5.6 for 11 servers). The `cluster status`
above is that cluster a week before its hard drives reach the mark.

### 15.6 A lost or failing disk, by class (E54, E57)

Every disk is a fault domain of its own (E57): its failure stops what is on that disk, not the process. tsdbd exits 1
only for a fail-stop of `--state-dir` (the Raft log and identity) or of something process-wide (USERS). Every disk has
a probe (src/health.c, `O_DIRECT`), and chunk I/O runs on one worker pool per disk, so a hung hard drive never reaches
the event loop that sends heartbeats.

- **A hard drive**: an I/O error on an acknowledged chunk, ENOSPC, or a probe stalled past `disk_stall_s` (10 s) makes
  that store DOWN at once as a device (E27, §2.2 as amended); puts go to the server's other stores, reads of its chunks
  decode from other positions, and the server keeps leading its volumes. After `rebuild_after` (`failstop_out_s` for a
  store that failed, E12) it is OUT unless held, and its chunks are rebuilt onto the server's other hard drives below
  their mark, else its positions move to other servers. Never onto the server's NVMe. **Time for §15.5's example**: 12
  × 10 TB drives a server, about 8.3 TB of chunks each. Every lost cell is decoded from k others, so the rebuild reads
  k × 8.3 = 66 TB from the 10 other servers (§5.3's k·D/((N − 1)·r)): about 9 h at the base pace (`rebuild_mb_s`, 200
  MB/s a server). Its stripes are at margin 1 (8+2, one position gone), so E13 runs it at 4 × that pace or more: about
  2.5 h if the drives keep up (each source drive reads about 0.55 TB, the server's 11 remaining drives write about 0.75
  TB each).
- **An NVMe holding log replicas**: an error (a failed WAL write or sync, ENOSPC) fail-stops the replicas on that disk
  only (design-cluster.md §5.7), through the quiesce and reopen path of AGENTS.md "Storage outages"; leaders among them
  report it in the next heartbeat and fail over at once, followers leave their ISRs. A hang (an `fdatasync` that never
  returns, no error) is caught by the probe: past `disk_stall_s` the disk is DOWN and every leader on it resigns as a
  fail-stopped one does; until then its volumes stall, since a hung leader's sync holds the commit point and nothing
  that is not durable is acknowledged. Writes then go on (E4, E24: an EC volume with `min_isr = m` refuses writes only
  if more of its log replicas are out). After `rebuild_after` (or `disk out` at once) the lost replicas are removed and
  rebuilt as learners: on another fast disk of the same server when it has room below `young_move_pct`, else on
  another compute-capable server; most endangered first (E13). Meanwhile **the server's hard drives go on serving
  their chunks** (reads, puts of new settles, scrub, rebuild sources): a chunk store needs no other disk (E48), and
  the chunk service runs without an engine. If it was the server's last fast disk, the server takes no new log
  replica from that moment and is storage-only once the old ones are gone, until `disk replace` adds one.
- **The disk holding `--state-dir`**: the node's Raft log and identity are gone; tsdbd exits. The runbook (E44):
  1. `tsdb cluster node hold OLD --for 1d`, so OLD does not go OUT after 30 minutes and start a rebuild.
  2. A new state directory on a new disk; tsdbd started with OLD's disks and `--adopt OLD`: disks whose META names OLD
     are accepted as **pending adoption** and serve nothing; the server joins as NEW.
  3. `tsdb cluster node replace OLD NEW` commits (E28): NEW holds OLD's positions.
  4. NEW rewrites each adopted disk META to name itself (tmp, fsync, rename, directory fsync); a META that still names
     OLD after a crash is adopted again.
  5. Each adopted store is reconciled by §5.2's two-way CHUNK_LIST against the leaders' committed object records (the
     metadata holds no chunk lists: they are in each store's local index, E27, and the object records in the volume
     logs). Chunks missing, the objects settled while OLD was away included, are filled by copy or decode (E28); extra
     ones are orphans by E14's rules. Nothing intact is copied.

  Log replicas whose directories survive are not adopted (a replica directory carried to a new identity is a lost
  disk, design-cluster.md gap 14): they are rebuilt.
- **A server with both classes lost whole**: §5.3 as written, the log replicas onto other fast disks and the chunks onto
  other servers' chunk stores, each paced by margin.

### 15.7 One server: the local tier (E55)

A single tsdbd (no cluster) with NVMe and hard drives can keep recent partitions on the NVMe and older ones on the hard
drives, erasure coded across them. Today a single server can already put a **volume** on a hard drive (`tsdb DIR
volume add /hdd/v1`), but that is not tiering: a series stays in its volume for life, so all its data, today's
included, would be on the hard drive.

```
tsdb /nvme/db tier add /hdd0/tsdb-tier          # class detected (hdd), or /hdd0/tsdb-tier:hdd
tsdb /nvme/db tier add /hdd1/tsdb-tier          # ... up to /hdd5: six stores
tsdb /nvme/db tier set --redundancy 4+2 --after 7d [--volume N]
tsdb /nvme/db tier status    # stores, bytes per volume and store, margin, settles waiting, days to full, damage
```

The single-server tier has a config key too (2026-11-11): `storage.tiers` lists its stores as `tier add` does
(`PATH[:CLASS]`); `tier add` stays the command that makes and checks them (the TIERS record, the path and device checks
below). How the key and TIERS agree at start is settled with the `config-files` track.

- **Redundancy across stores**: each tier store is one position. An object is the part file erasure coded k' + m over
  k' + m distinct stores, with R5's codec and R6's cells and encode-time sums: a cluster stripe whose positions are
  stores instead of servers. The default follows the number of stores: 2, a mirror (1 + 1); 3 to 5, k' = stores − 1
  and m = 1; 6 or more, 4+2. `tier set --redundancy k+m` changes it for new objects (old ones keep theirs until a
  re-encode). m = 0 is refused unless the store is stated redundant underneath: `tier add PATH --single-copy` (RAID,
  ZFS, an NFS export from ZFS), and `tier status` then shows the margin as "underneath". One box's drives share its
  power supply and controller: this protects against a drive, not the box (backups, E42, or a cluster do that).
- **Where it is recorded**: `TIERS`, beside VOLUMES in the root directory, append-only with checksummed records (store
  id, path, class, single-copy, state), written under the VOLUMES OFD lock and synced with its directory before the
  store is used, as AGENTS.md "Volumes" does for adds. `tier add` checks the path as `volume add` does (`path_valid`:
  absolute, not in or around the root, a volume or another tier store) and refuses a path on the device of `/`, of the
  database or of another store (E58). The store's META holds its id and the database's id (a random `db_id` that the
  first `tier add` writes into the root META), so one store cannot serve two databases. A volume that has settled to a
  tier gets `requires=tier` in its META, so an older binary refuses it. tsdbd takes no flag: a database whose settled
  partitions live elsewhere must not open without them, and a flag can be forgotten.
- **When a key moves**: `tier_after_s` (7 days by default) after its end, by the settle rule of E5 with that grace
  instead of `settle_grace_s` (coarse keys of classes 1-3 part by part, E5); and earlier, oldest key first, while the
  volume's file system is above `tier_high_pct` (80%). A settle waits while fewer than k' + m stores are UP. Retention
  and partition classes are unchanged: a key that expires is dropped whether it is on the tier or not (its object is
  deleted after `obsolete_part_lifetime_s`).
- **How it moves**: E6's settle at a cut on one replica (R7 builds and gates exactly this case with the directory
  backend). Each position's chunk is written to its store as `tmp_*`, fsync'd, renamed, directory fsync'd; then the
  `.ec` index file is written in the volume directory and the full part retired under `publish_lock`, through the
  settle intent. The settle lists parts, so it folds into the latest table like any publisher (AGENTS.md "The latest
  table"), with no change to its answers. The open's orphan scan of the tier stores runs after the intent replay and
  deletes only chunks that no MANIFEST entry, settle intent or `retired_*` `.ec` file (kept for followers until
  `obsolete_part_lifetime_s`) names; on one server no settle is in flight then.
- **Reads**: statistics from the `.ec` index on the NVMe, as before. A granule to decode is read from the granule cache
  or with one `pread` of its byte range from the store holding its data cell (a seek), or, with that store DOWN, the
  same range from k' others; adjacent granules of one series in one partition (they lie together in a part) are read
  in one `pread`. Every granule is checked against its checksum (E11). Hard-drive files are never mapped. A `pread`
  on a hung drive blocks in D state like a page fault, so the gain is containment, not a timeout: an error is an error
  code for that query instead of a SIGBUS in whatever thread touched the page, a health domain over the store refuses
  new reads, and a request stuck there can be abandoned (`tsdb_set_abandon_flag`).
- **What reads cost** (EU10; R11 measures, and nothing is claimed without a real drive): a cold decode costs one seek
  (about 8 ms) per series and partition not cached. One series by the hour over 116 days, decoded: about 110 seeks,
  about 1 s cold (16 ms today on NVMe); 100 such series about 10^4 seeks, about 80 s on one drive, divided by the
  number of stores the objects spread over. None of it is paid where the buckets answer from statistics: UTC days,
  and hours of a series dense enough for hour cuts (3,000 rows an hour, AGENTS.md "Granules"), read only the index on
  the NVMe. Re-encodes, the latest table's warmer and its slow path, and scrub read the hard drives too, below queries.
- **Late rows, deletes, merges**: §2.4 as in a cluster: late rows go to new young parts above the object; deletes
  write tombstones; merges never touch `.ec` parts; a re-encode combines a key's objects (`ec_objects_max`).
- **Filling**: above `young_high_pct` (90%) of the volume's fast file system, inserts into that volume are refused
  (`TSDB_ERR_UNAVAILABLE` from the C API; tsdbd answers UNAVAILABLE with `TSDB_WHY_FULL`) before the WAL can reach
  ENOSPC (fail-stop). Tier stores take no new chunks above 95%; rebuilds may use the rest. When the tier is full, DOWN
  or short of stores, settles wait and the NVMe fills towards that mark, so the alert comes first: metrics
  `tsdb_tier_bytes{store,use}` and `tsdb_tier_settles_waiting`, days to full in `tier status`, and
  `dist/alerts/tsdb.rules.yml` (new, for single servers): `TsdbTierFull7d`, `TsdbTierStoreDown`,
  `TsdbInsertsRefusedFull`.
- **Failures**: an I/O error or a stalled probe makes a store DOWN; reads of its positions decode from the others, and
  under a `--health-domain` over its path an outage behaves the same while writes and statistics go on. At open every
  store in TIERS must be there with its ids, or the open fails naming it. `tsdb DIR tier lost STORE` declares one gone:
  its positions are rebuilt onto the other stores (or a store added to replace it) from k' others, locally, at
  `rebuild_mb_s`; partitions are lost (E15: `TSDB_ERR_IO` naming the partition, statistics still answer) only when more
  than m stores are gone, and `tsdb DIR tier salvage` then drops their objects by oid, keeping the young parts. Scrub
  (E25) reads the stores at `scrub_mb_s` and repairs a bad cell from the other positions; with `--single-copy` it
  reports only.
- **Followers** (`--follow`) open the stores listed in TIERS at the same paths and refuse to start, naming the store,
  when one is not there with its ids. A follower on another host cannot reach a writer's local drives, so it needs the
  tier on shared storage. **Shared mode** (ha.md) volumes refuse a tier in 0.11.0. **Embedded**: the engine does all of
  it, so a tiered database opens in the embedded bindings unchanged; `tier add` is the CLI and the C API
  (`tsdb_tier_add`) only.
- **Production (NFS/ZFS)**: a tier store may be a second export from a hard-drive pool, `:hdd --single-copy` (ZFS gives
  the redundancy; no device to detect), under its own `--health-domain`.
- **Formats**: `.ec` files, `ppos` in MANIFEST, the settle markers and the settle intent come to single-server volumes
  with a tier, behind `requires=tier` (E17 amended).

### 15.8 Formats that freeze at 1.0

The disk META (store id, class, measured sync latency, node and cluster once bound); the member record's disk list
(store id, class, uses, size, latency, state), the declared role and capability, `STORE_ADD`, `STORE_CLASS`,
`log_on_hdd` (all in metadata snapshot version 3, E18, not yet released); `EPOCH` per chunk store; `TIERS` (with the
single-copy flag), the tier store META, `db_id` in the root META and `requires=tier`. `FORMAT_VERSION` stays 9 (E17).
§8 lists them.

### 15.9 Tests (E56)

- **Detection and setup**: `test_disks` against a fake `/sys` tree (a test hook for its root): NVMe, SATA SSD, hard
  drive, a partition, dm-crypt over NVMe, md RAID 1 over two hard drives; refused: dm-cache over a mix, loop, NVMe over
  TCP, btrfs, no device. A class stated wrongly at restart refused; a detection that changed logged and counted, not
  refused; `disk class` accepted. An unmounted path (an empty directory on the scratch root) refused; two `--disk` on
  one file system refused without `--allow-shared-device`; a known store whose META is gone counted missing, never
  new; `--dir` on tmpfs starts as `ssd`.
- **One machine, two classes**: the cluster harness (§10.1) gives each server `:nvme` disks under one scratch file
  system and `:hdd` disks under another, classes stated and `--allow-shared-device` set. Hard drives run under
  outagefs (`tests/jepsen/outagefs.c`) with its `slow MS` mode (a seek on every call) and a new `rate MB/s` cap in
  R13's, R15's and R11's runs only. Cases: placement by class (no log replica on an `hdd`, no chunk on the NVMe of a
  mixed server, chunks on the NVMe of an NVMe-only one), one-class stripe groups, an NVMe filled to 75/80/90% under
  ingest (alert, move to another server, FULL for its volumes only, retired parts released), EIO on one hard drive
  under load (the server keeps leading; only that drive's chunks move), a stuck NVMe (its leaders fail over within
  `disk_stall_s` plus failover), a lost NVMe with every chunk of that server still read, hard drives added to
  NVMe-only servers (local drains; bytes on the network: 0), an all-flash cluster filling (the chunk alert fires), the
  state directory lost and the stores adopted by the runbook (intact bytes copied: 0), `log_on_hdd`.
- **`sim_place`** (R9's simulator) gains disks and classes, with invariants and a mutant for each: no log replica on
  an `hdd` without `log_on_hdd`, one replica of a volume per node, room per use, one class per stripe group where
  possible; a 4-hour soak in R13's gate.
- **jepsen-cluster** (R13's gate): EIO on one hard drive, a stuck NVMe, single-disk loss of each class, back within 30
  minutes, a fast disk filling to FULL; checker as §10.3.
- **Single server** (R15's gate): `make jepsen` with a six-store tier on outagefs: kill and power loss (crashfs) during
  a settle, EIO and stuck on one store (reads decode around it), one store lost and rebuilt, m + 1 lost and salvaged,
  the tier filling under ingest (FULL before ENOSPC), `--single-copy`; every answer against the oracle; `fuzz_tiers`
  on the TIERS file; test_outage with a health domain over a store.
- **Benchmarks** (R11, quiet machine, 5 runs): cold decode queries from a tier on a real hard drive where the machine
  has one, else not claimed; the shim's numbers are not hardware numbers.

### 15.10 Cost

R13 becomes "disks with classes": 6-8 track-days and 16 gate hours instead of 2-3 and 6 (the first draft of this
section said 4-5 and 10; the review added each disk as a fault domain, local chunk moves and `sim_place` with
classes). R15, the single server's tier with redundancy across stores: 5-6 track-days and 10 gate hours, needing only
R7, so it can be gated long before the cluster steps. P6 gains 2 gate hours for the class cases; P4 and P5 take the
alerts and runbooks within their estimates. The total for 0.11.0 rises from 57-81 to **66-92 track-days** and from
222 to **244 gate hours** (§11).

### 15.11 After review (2026-11-11)

One review (durability, operations, performance, consistency with E1-E46): 0 critical, 4 high, 10 medium, 12 low.
Every finding is fixed:
- **H1**, one disk's failure took the whole process: E57 (each disk a fault domain, probes, worker pools per disk),
  §2.2, E12, §15.6, R13's gate (EIO on a hard drive, a stuck NVMe).
- **H2**, moves inside a server needed two replicas on one node: E51 and §15.3 choose the review's option (a): a log
  replica leaving a filling disk moves to another server; a replica lost with its disk is removed before its
  replacement is placed, on the same server or not. The citation is now design-cluster.md §8.1. Open question 4.
- **H3**, disks set up at first use: E58, §15.1 (`tsdb disk init`, the META and `st_dev` checks, a missing META is a
  missing store); `tier add` likewise (§15.7).
- **H4**, one bare copy on one drive: E55 and §15.7 code each object across the tier stores (mirror, k' + 1, 4+2 by
  the number of stores), `--single-copy` only when stated, local rebuild, scrub repairs. Open question 2.
- **M1** E14 and §2.2 (the fence is the maximum over a server's stores). **M2** E54 and §15.6 (the adoption runbook).
  **M3** E47 and §15.1 (only an explicit class is refused). **M4** E47, §15.1, §10.1, §15.9 (`--dir` as `ssd`, classes
  stated in the harness). **M5** E53, §4, §15.4, §15.5 (retired use, projected-use placement, releases above 90%).
  **M6** E50, E53, §15.4 (accounting by use, the young reserve instead of 60%). **M7** E50, §15.3 (local chunk moves,
  the hard-drive upgrade runbook). **M8** §15.7 (FULL, tier metrics and alerts). **M9** §15.7, EU10, open question 1.
  **M10** §10.1, §11 (`sim_place` with classes and its soak in R13; slow hard drives only in R13, R15 and R11).
- **L1** E17, §8. **L2** §15.7. **L3** §15.7. **L4** E48. **L5** E51, §15.3. **L6** E50, §15.3. **L7** E48, §15.1.
  **L8** §15.1, §15.9, EU9. **L9** §8. **L10** §15.6. **L11** §15.7. **L12** E49, §15.6.

## Open questions

None. The five open questions of the revised draft were answered by the user on 2026-11-09: the licence (E32), what
0.11.0 holds (E33), the default redundancy (E34), the young tier's ISR floor (E35), and preview against production
(E36). The decisions that production then required (E37-E46) were made in this revision with their rejected
alternatives; the user may change them.

**Device classes (2026-11-11, §15)**: the four questions were answered by the user on 2026-11-11, each as
recommended. Decisions:

1. **Single server, NVMe to hard drives** (decided 2026-11-11): a partition moves to the hard-drive tier 7 days after it
   ends (`tier_after_s`, E55), or sooner, oldest first, when the NVMe passes 80% (`tier_high_pct`). Decoding queries
   over older data then seek (about 1 s cold for one series by the hour over 116 days, §15.7); day buckets and hours of
   dense series answer from the index on NVMe. *Rejected*: 1 day (less NVMe, more seeks).
2. **Single server, redundancy** (decided 2026-11-11): the tier is redundant by default: mirrored on 2 drives, one
   spare (k' + 1) on 3-5, 4+2 on 6 or more. A single copy only when the operator states that the drive is already
   protected (`--single-copy`, e.g. RAID or an NFS export from ZFS). *Rejected*: one copy by default, where a lost
   drive loses data.
3. **Cluster, yesterday** (decided 2026-11-11): there is no NVMe copy of yesterday by default: `settle_grace_s` stays at
   1 h and `ec_keep_local_h` at 0. The operator guide suggests `ec_keep_local_h = 24` where yesterday is read a lot
   (one more copy of a day on the leader's NVMe, not m + 1).
4. **No same-server NVMe moves in 0.11.0** (decided 2026-11-11, E51): a full NVMe moves its log replicas to another
   server, which costs network traffic but never lowers the number of servers holding an acknowledged row. Moving
   within a server needs replica identities by (node, rid) and `min_isr` counted by distinct nodes (about 2
   track-days), later.

None open.

## Unverified assumptions

- EU1: GFNI kernels encode k = 16, m = 4 above 8 GB/s a core on Zen 4/5. R5 measured the 7950X (Zen 4, 2026-11-13, `docs/tasks/ec-bench.md`): gfni256 16.1 GB/s a core with 1 MiB shards (cache-sized stripes), 7.4 GB/s with 4 MiB shards (memory bound), gfni512 slightly slower on Zen 4; the 9950X is still to be measured.
- EU2: A cold-cache decode query costs one round trip per partition touched with pipelining (R11).
- EU3: Young data is about one day of ingest per volume with `settle_grace_s` = 1 h and day partitions, plus the
  settle wait (EA2, §4).
- EU4: Chunk files on ext4/XFS, one file system per disk, are fast enough for storage servers; no raw-device store is
  needed.
- EU5: Coarse-class keys (7-364 days) hold a minority of the bytes on a realistic mix of 10^9 series (R11 measures).
- EU6: The logical digest (one decode of each settled partition per log replica) costs a few percent of a core at the
  100 B benchmark's ingest (R11 measures; if not, it samples series instead of decoding all).
- EU7: A server's turn in a rolling upgrade (leadership transfer, restart, open, catch-up) takes minutes at 10-100 TB
  a node, because an `.ec` part opens from its index and only memtables replay (P1 measures it; if it takes much
  longer, `node prepare`'s default hold of 30 minutes is raised).
- EU8: A daily incremental backup copies about 1-3 days of ingest: the day's rows, and young parts again each time
  merges rewrite them before their key settles (P3 measures it on the soak's ingest).
- EU9: The detection rule of §15.1 (PCIe `nvme*` names, `queue/rotational`, dm and md slaves) gives the right class
  on AlmaLinux 10 kernels for NVMe, SATA SSD, hard drives, LUKS and md, and hardware RAID controllers' volumes are the
  case most likely to report `rotational` wrongly (a stated class then wins, and the start only counts the mismatch).
  `test_disks` checks the rule on a fake `/sys`, not the kernels' reports; R13 checks it on the test machines' disks.
  The `fdatasync` latency `disk init` measures tells SSDs with power-loss protection (well under 1 ms) from those
  without (several ms); R13 checks the 2 ms warning on the test machines.
- EU10: A single server's tier answers a cold decode query in one seek per series and partition not cached (about 8 ms
  on a hard drive): 6-12 ms for §3.4's one-series window, about 1 s for one series by the hour over 116 days, about
  80 s for 100 such series on one drive (less over several stores); the settle's writes (sequential) do not slow
  queries on the NVMe. R11 measures it on a machine with a hard drive; nothing is claimed without one.
