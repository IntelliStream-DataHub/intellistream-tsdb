# tsdb cluster data: erasure coding, 3 replicas, volume moves and failover

Status: design, not implemented. Branch `cluster-data-design`, written on 2026-11-09 from main at 47ae317 (0.10.0),
**revised the same day after two reviews** (durability and consistency; operations and performance at 10-100 TB per
node): their findings are fixed below or answered with the reason, and §13 lists where. Code references are
`file:line` at 47ae317 for tsdb (design-cluster.md and design-ha-modes.md as edited on this branch), and at 9c5f97d2
(origin/main, pulled 2026-11-09) for intellistream-objects (written `objects:path:line` below). Target release:
**v0.11.0**.

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
modes and the combined build plan; §7 there carries this design's steps R5-R14), [design-cdc.md](design-cdc.md)
(positions, `fno`).

## Summary

- **Redundancy is a per-volume setting** with a cluster default: `replicas=3` or `ec=k+m` with k+m one of 4+2, 8+2,
  8+4, 16+4 (any k ≤ 16, m ≤ 4 is accepted; those four are tested). It can be changed on a running volume, as a
  background conversion in a fixed safe order (E2; the step may move to 0.12, open question 2).
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
- **Codec**: systematic Cauchy Reed-Solomon over GF(2^8) with SSSE3/AVX2/AVX-512/GFNI kernels, as in
  intellistream-objects `src/codec`. Copy it under a grant (as the Raft core was) or write our own: open question 1.

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
| E12 | **Failure states as objects': DOWN, then OUT** after `rebuild_after_s` (1,800 s; replaces design-cluster.md's `out_after_s`, :317). A server whose heartbeats say it is **opening** (WAL replay, part checks) is held from OUT up to `opening_hold_s` (3,600 s) more; a server that reported a **fail-stop** goes OUT after `failstop_out_s` (60 s), unless held. Operator holds (`node hold N --for 2h`, at most 1 day) pause the timer; no automatic OUT while a whole rack, or more than 25% of the servers, is down, for at most `out_guard_hold_s` (2 h by default, at most 1 day), then OUT goes ahead with an alert. | Rebuilding at once; no guard; an unbounded guard (the first draft) | "The timer between them is what stops a reboot from triggering a terabyte of copying" (objects:src/cluster/cmap.h:67-70); the holds and the mass-OUT guard (objects:src/cluster/cmap.h:168, :895-925; default 25%, objects:src/mds/server.h:89-93) keep a power or switch event from starting a rebuild storm; their bounds (objects:src/mds/server.h:89-100: hold at most a day, opening 1 h, fail-stop 60 s) keep a rack that is really gone from sitting below margin forever, and a fail-stopped tsdbd (exit 1) does not come back on its own. |
| E13 | **Rebuild order: most endangered first** (fewest copies or chunks left beyond the minimum: k for EC, 1 for replicas), then balance moves. **Pace by margin**: margin 0 rebuilds at full disk and network speed and is exempt from design-cluster.md §8.4's halving; margin 1 of an m ≥ 2 volume gets at least 4 × `rebuild_mb_s`; the rest `rebuild_mb_s` (200 MB/s per server) under §8.4's token bucket. A plain copy from a holder that is back is preferred over decoding. | FIFO; decode always; one pace | Objects' decision of 2026-10-20 (objects:src/mds/server.h:1852-1860): a 4+2 object two short goes before a 16+4 object two short. A copy reads 1x, a decode kx. An EC rebuild reads k times a replica rebuild's bytes, so at one pace it takes k times as long (§5.3): the exposure window, not the number of losses, is what lowers the mean time to data loss. |
| E14 | **Chunk writes and deletes are fenced by the volume epoch; deletes go through a committed marker.** A chunk header carries its writer's epoch. A storage server keeps, per volume and durably, the highest epoch it has seen in any request (not only from the metadata watch) and refuses a put or delete below it, and a delete below the chunk's own epoch. Chunks are deleted only as listed by a committed `MK_GC` marker. An object is an orphan only when no record in the deleter's log names it (committed or not), it was written in an earlier epoch than the deleter's, the deleter's `EPOCH` marker is committed, and it is not one of the deleter's own settles in flight. Orphan collection is suspended after `force-leader` or a restore until the operator resumes it. Reads are not fenced. | Fencing reads; trusting the sender; deleting what no committed marker names (the first draft) | Objects are immutable and checksummed, so a read from any holder is safe. A deposed leader cannot commit `MK_GC` (C11), so it can only leave orphans; a newly promoted leader whose old tail is not yet committed would otherwise delete an object whose `MK_COLD` is about to commit, and every replica would then retire its full copies. |
| E15 | **Three outcomes, never confused**: **unavailable** (more than m positions of a stripe DOWN or unreachable: UNAVAILABLE, retryable, reason `TSDB_WHY_UNREACHABLE`); **corrupt** (enough positions present but checks fail beyond what decoding repairs: `TSDB_ERR_CORRUPT` naming the partition and granule; a repair is scheduled); **lost** (more than m positions OUT whose holders the operator declared destroyed, or whose stores came back empty: `TSDB_ERR_IO` naming the partition, the volume `LOST` in TOPOLOGY, UNAVAILABLE reason `TSDB_WHY_LOST` with "do not retry"). Queries answered from statistics still answer over lost partitions (the index is local and intact). `tsdb cluster volume salvage V` drops lost objects by oid (`MK_DROP_OBJ`), keeps the young parts, and refuses while an OUT holder may still return, unless `--force`. | One error for all three; salvage by `MK_DROP` (it would retire the surviving young parts: acknowledged writes thrown away) | AGENTS.md "Damage is never silent"; a partition is not lost while its holders may return (objects keeps a DESTROYED state for this, objects:src/cluster/cmap.h:76). |
| E16 | **Shared NFS mode has no EC.** `redundancy` is replicated mode only; a shared-mode cluster refuses it. | EC over NFS | ZFS already gives the redundancy under NFS; one copy on shared storage is shared mode's point ([design-ha-modes.md](design-ha-modes.md) §1). |
| E17 | **No new disk format number.** Chunk files, `.ec` index files, the part position in MANIFEST entries, and the markers `MK_COLD` (11, reserved in format 9) and `MK_GC`, `MK_DIGEST`, `MK_DROP_OBJ`, `MK_UNSETTLE` (12-15) live only in cluster volumes behind `requires=cluster,ec` in META (format 9's mechanism, src/feedfile.c:385, src/db.c:437); they freeze at 1.0 with the rest. | Format 10 | Nothing in a single-server volume changes; the `requires=` line refuses the files in a binary that does not know them. |
| E18 | **The metadata snapshot becomes version 3** (redundancy, two member lists per volume, roles, holds, stores, DESTROYED), and the peer protocol version 3 (chunk messages). Whether 0.11.0's cluster data is a preview (initialised again at the next change, as from version 1 to 2) or upgraded in place from now on is open question 5. | Silently keeping the preview rule | From 0.11.0 the cluster holds data, 10-100 TB a node: re-initialising means exporting and importing it. |
| E19 | **The EC codec is systematic Cauchy Reed-Solomon over GF(2^8), polynomial 0x11d**, kernels dispatched by tsdb's `src/cpu.c` (no constructors: AGENTS.md "CPU levels"), every kernel byte-identical to the scalar one. | ISA-L as a dependency; LRC or other codes | Interoperable with Jerasure, ISA-L and klauspost (objects:src/codec/gf256.h:1-5); any k of k + m reconstruct, with no unlucky sets (objects:src/codec/rs.h:11-15). Locally repairable codes save rebuild reads but are not MDS at the same space; a later change if rebuild reads hurt (§5.3). Where the code comes from is open question 1. |
| E20 | **Storage and compute can be separated**: server roles `compute`, `storage`, `both` (default). §9. | Every server both | The user's question. It costs little (two member lists exist anyway, E3) and lets bytes and CPU scale apart. |
| E21 | **A compute server's state is replicated, not small**: the young tier (a day or more of ingest at m + 1 copies), the `.ec` index files (about 0.75% of settled bytes, on each of the m + 1 log replicas) and caches. Losing one loses nothing durable (while the ISR is full, E4); rebuilding its log replicas is design-cluster.md §8.1's rebuild of terabytes: hours, spread over the other compute servers. A separated cluster needs at least **m + 2 compute servers**, so a lost one has somewhere to be rebuilt. | Stateless compute nodes (WAL on storage nodes); the index on fewer log replicas | A WAL on remote storage adds a round trip to every acknowledged insert; m + 1 local NVMe copies are the replicated mode that already exists. The index on every log replica keeps statistics queries warm right after a failover; at 0.75% its copies cost less than one more data copy of the young tier. |
| E22 | **The settled tier is an interface with backends**: the cluster's own storage servers (0.11.0), a directory (tests), and later isod or any S3 (design-cluster.md §9.2's `tsdb_cold_ops`). | Only the own backend | With an S3 backend, tsdb servers become compute only and isod does the erasure coding and its rebuild. That is the "true separation" end state; it waits for 0.12 or later (§9.4). |
| E23 | **0.11.0 holds replicated mode's data path (R2-R4) and this design (R5-R14)**; shared mode's steps go on in parallel and ship when their maintenance windows allow. | Waiting for N7 (M21's order) | The user's "that will go in v0.11.0". M21 put replicated mode after shared mode; this changes the order, not the content (open question 2 is what may move to 0.12). |
| E24 | **The young tier's ISR floor for EC volumes**: `min_isr` defaults to **m** for `ec=k+m` volumes (2 for m = 2, 4 for m = 4), so an acknowledged row survives at least m − 1 losses at worst and m with the ISR full; `replicas=3` keeps D1's 1. TOPOLOGY and health report both margins: the young tier's (ISR size − 1) and the settled tier's (fewest positions beyond k). | D1's 1 for every volume | The user chose m = 4 to survive 4 losses; with `min_isr = 1` one slow node (or two on 8+4) leaves writes acknowledged on one copy. The cost: an EC volume refuses writes when more than one of its log replicas lags (of 3 for m = 2, of 5 for m = 4). Open question 4. |
| E25 | **Scrub every chunk every `scrub_days` (14)**, paced by `scrub_mb_s` (50 MB/s per disk) below rebuild: each storage server reads its chunks and checks every cell against the encode-time cell checksums (E26); log replicas scrub `.ec` files and young parts the same way (`verify_checksums` reads). A bad cell is repaired by a range decode through the rebuild path. Damaged chunks are counted per disk; a disk with more than `scrub_out_count` (8) in a day goes OUT (E27). Health shows the oldest unscrubbed object. | Scrub on demand; checking parity against data across servers | Systematic reads never touch parity, and range reads cannot check a 1 MiB cell's sum, so rot in parity or unread data stays hidden until a degraded read or a rebuild needs it: at margin 0 that is loss. Encode-time sums make a local check sufficient: a cell that matches its encode-time sum is what the encoder produced (objects counts damage per device the same way, objects:src/cluster/cmap.h:169-170). |
| E26 | **Checks at every hand-over.** The encode checks every granule checksum and the footer hash of each part it reads (it reads every byte anyway) and aborts on a mismatch (the part is repaired from a replica first, design-cluster.md §8.5); it computes a 64-bit sum per cell for **every position**, data and parity, sends each position its sums with the put, and records all of them in the object record; a chunk server checks the cells it receives before PUT_OK and the leader compares each PUT_OK's sum. After the settle, every log replica compares a **logical digest** of the rows it retired (rows per series and an xxh64 over the visible rows in (series, ts) order) with the leader's (`MK_DIGEST`), before it deletes them; a mismatch keeps the retired parts and raises DIVERGED. Retired parts of a settle are kept `settle_keep_s` (1 day), not `obsolete_part_lifetime_s`. A rebuilt cell is checked against its encode-time sum, never self-certified. | Trusting the leader's bytes (the first draft); a digest on the apply path | Granule payloads are hashed only with `verify_checksums` (default off; src/part.c:314-318), so rot in the leader's part would be encoded with fresh, matching sums while the good replica copies are retired; and a merge bug on the leader would become the only lineage. The digest costs one decode of each settled partition per log replica, in the background (EU6). |
| E27 | **A storage server holds one chunk store per disk** (`--chunk-dir`, repeated; each with its own META and store id). A position stays one per server; each chunk lives on one of its disks, in the server's local index. A dead disk is a **device** DOWN/OUT: only that disk's chunks are rebuilt, onto the server's other disks when they have room, else the server's positions move by the balancer. `tsdb cluster disk out|replace NODE DISK`. | One file system over all disks (one disk loss is the whole server); RAID under the chunks (redundancy on redundancy, the 1.25x is no longer 1.25x) | At 1-2% a year per disk, a 20-node cluster of 24-disk servers loses several disks a year: the most frequent hardware event. Objects does it per device (objects:src/cluster/cmap.h:66-77; timers per device, objects:src/mds/server.h:84-100). |
| E28 | **A server back during a rebuild takes its positions back**: for each position whose replacement is not yet `whole`, a returned holder whose stores are intact gets it back with one `STRIPE_SET` conditional on `stripe_version`, and copies only what was settled while it was away; the partial fill is deleted. Where the replacement is already whole, it gets the position back only when that saves a balance move at an unchanged margin. `tsdb cluster node replace OLD NEW` gives OLD's positions to NEW directly, filled once by copy or decode. | The first draft: the returned server only serves as a copy source, then deletes everything, and the balancer moves 1/N of the cluster back onto it | A 35-minute firmware update without a hold would otherwise move the node's data twice (out by decoding at k times the reads, then back), days of traffic at reduced margin. Ceph does the same when an OSD returns during backfill. |
| E29 | **Segments are retained for a DOWN or held member** of the log group until it goes OUT or its hold ends, within `repl_retain_down_gb` per server (sized by §4's rule: ingest × `rebuild_after` × WAL bytes per row), beyond design-cluster.md §8.3's 30 min and `repl_retain_mb`. | §8.3's budget alone | At high ingest 4 GB lasts minutes, so "back within 30 minutes only catches up" would not hold, and a planned 2-hour hold would cost a snapshot of terabytes of young data. |
| E30 | **Backpressure before ENOSPC**: a log replica above `young_high_pct` (90%) of its WAL and young-data file system makes its leader refuse inserts with UNAVAILABLE reason `TSDB_WHY_FULL` (4, protocol 13) until it is below again; the operator guide sizes compute disks (§4). | Letting the WAL reach ENOSPC (fail-stop on every compute node at about the same time) | Young data grows when settles wait (E10, E12) and every compute node fills at the same rate; refusing is retryable, fail-stop is not. |
| E31 | **Hedged reads per server**: a cell read is hedged after `2 × that server's read p99` (floor 20 ms, ceiling 500 ms), not a fixed 50 ms; hedges are capped by a token bucket at 5% of reads per server; a server with 8 timeouts in 10 s is `read-avoid` for 30 s (decoded around without waiting). | A fixed 50 ms timeout with k reads per hedge (the first draft) | On HDD storage servers (§9.1) queue p99 under rebuild or scrub is routinely above 50 ms; each hedge reads k cells (16 on 16+4), raising latency further: a feedback loop that can outlive its cause. |

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
CHUNK_LIST after its restart (§5.2). A cell whose checksum fails is an erasure, never decoder input
(objects:src/recovery/rebuild.c:21-25). Each chunk store has a store id (in its META), which the metadata records per
server: a store that comes back with another id, or not at all, is a lost disk (E27).

Each storage server also keeps, per volume, a small `EPOCH` file: the highest volume epoch any request has shown it,
synced before it acts on a higher one (E14).

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
is design-cluster.md §8.5's replaced disk: its log replicas become learners and are rebuilt.

### 5.6 Disks full

A server stops being a placement candidate at its fill mark, `full × (1 − the largest domain's share)`, which keeps
room to rebuild the largest domain (objects:src/placement/README:84-99); rebuilds may use the room up to `full`.
ENOSPC on a chunk `tmp_*` fails that put only (nothing relies on it, AGENTS.md "Fail-stop"); ENOSPC on a WAL,
MANIFEST or dictionary is fail-stop as today, which is why a compute server refuses inserts at `young_high_pct`
before it gets there (E30).

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
step 4. The time is the same order as a re-encode. If 0.11.0 must shrink, this is the step that waits (open question
2): volumes then keep the redundancy they were created with.

## 7. Shared NFS mode and the existing plan

Shared mode has no redundancy setting (E16). Replicated mode's R1-R4 (design-ha-modes.md §7) stay as they are: R1's
core needs no change (an ISR of up to 5 is within `META_REPLICAS_MAX`, server/meta.h:68), R2's engine replica mode is
the log group, R3's replication is the young tier, R4's rebuild is the log-position rebuild. This design adds R5-R14
(§11) on top and moves the cold boundary X1 into R7 (its put, marker, `.ec` index and swap are the settle's).

## 8. Formats and versions

| What | Where | Freezes at 1.0? |
|---|---|---|
| Chunk file (§2.2): 96-byte header with writer epoch, encode-time cell sums | storage servers, `<store>/s<volume>/` | yes, behind `requires=cluster,ec` |
| Chunk store META with store id; per-volume `EPOCH` file | storage servers | yes |
| `.ec` index file: the part's header, index, tombstones, footer, then the object record (with every position's cell sums) | log replicas | yes |
| `ppos` in MANIFEST entries of cluster volumes | log replicas | yes, behind `requires=cluster` |
| Markers: `MK_COLD` body (Z, object records, `.ec` and `Y` files, retired oids, `stripe_version`), and new `MK_GC` (12), `MK_DIGEST` (13), `MK_DROP_OBJ` (14), `MK_UNSETTLE` (15) | WAL; `MK_TYPE_LAST` moves to 15 (src/marker.h:41) | yes |
| The settle intent record (an extension of `parts_commit`'s) | log replicas | yes |
| `FORMAT_VERSION` | stays 9 (E17) | |
| Metadata snapshot version 3: `struct meta_volume` (server/meta.h:161-183) gains `redundancy` (kind, k, m), `min_isr` default by mode, the stripe list (up to 24 positions: 20 plus 4 moving; each pos, node, rid, state, barrier), `stripe_version`; members gain roles, holds (with their bound), chunk store ids and states, DESTROYED | metadata group | at 1.0 with the rest of the metadata; open question 5 decides whether 0.11.0 already upgrades it in place |
| New ops at the end of `enum meta_op_kind` (never renumbered, server/meta.h:264-295): `STRIPE_SET` (if stripe_version == v), `NODE_HOLD`, `VOLUME_REDUNDANCY`, `NODE_REPLACE`, `STORE_STATE`, `NODE_DESTROYED`, `GC_SUSPEND` | metadata group | yes |
| Peer protocol version 3 (`CL_VERSION`, server/cluster.c:95): CHUNK_PUT (with cell sums), PUT_OK, CHUNK_READ (oid, pos, offset, length), CHUNK_DATA, CHUNK_LIST, CHUNK_DEL (naming its `MK_GC`), all with volume and epoch; heartbeats gain "opening" and "fail-stopped" | peers | no (same-version peers only, design-cluster.md §11.2; open question 5) |
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

### 10.4 Benchmarks (quiet machine, 5 runs, the mean; AGENTS.md benchmark policy)

Per mode (`replicas=3`, 4+2, 8+2, 8+4, 16+4): `build/ec-bench` (encode, decode, range decode per k, m, cell size, per
kernel; 7950X, 9950X, i9-10850K); insert-bench and tsdbc-bench through the cluster; the 100 M row query suite with a
warm cache, a cold cache, one position down and m down, on NVMe and on an HDD-backed storage configuration; settle
throughput and the settle fence's length; rebuild time against foreground p99; network bytes per row; the digest's
cost (EU6). On one machine the numbers compare the modes; absolute numbers need servers with disks.

## 11. Build plan for v0.11.0

Track-days as in design-ha-modes.md §7 (build, review and fix rounds); gate hours are machine time beyond `make test`,
`make asan`, `make tsan`. Prerequisites from that plan, all in 0.11.0 (E23): the rest of S1 (3-4 days, 6 h), S2 (2-3,
3 h), S4 (1-2, **4 h: its own jepsen-cluster run with name creation**, since N7's jepsen-ha is not in 0.11.0), R2 (2-4,
6 h), R3 (3-4, 12 h), R4 (2-3, 12 h): 13-20 track-days, 43 gate hours.

| Step | Content | Gate | Gate hours | Track-days |
|---|---|---|---:|---:|
| R5 | EC codec in `src/ec/` (open question 1: a patched copy, dispatch moved to `src/cpu.c`, no constructor), fitting AGENTS.md "CPU levels" (`tools/check-isa.sh` must allow the AVX-512 and GFNI kernels behind target attributes, or they move into `src/levels.h`), `test_rs`, `build/ec-bench`; docs/licenses.md row | numbers on three CPUs | — | 1-2 |
| R6 | Object layout, chunk files (header with epoch, cell sums), chunk stores per disk with ids and `EPOCH`, the directory backend; range decode; `fuzz_chunk`, `fuzz_ec` | fuzz 1 h each | 2 | 2-3 |
| R7 | Engine: `.ec` parts (the read seam, the cache, merges skip them), `ppos` in MANIFEST, the settle gate and cut, the settle intent, DIVERGED on a part above Z, encode-time checks, `MK_DIGEST` and `settle_keep_s`, per-part and coarse settles, re-encode, retention drops; X1's cold boundary folded in | `make jepsen` (single server, directory backend: kill, power during settle, deletes/replaces/large inserts into settling keys, the `Y` mutant) | 8 | 5-7 |
| R8 | tsdbd: roles, chunk peer messages, epoch fences (durable per volume), `MK_GC` and the orphan rules, GC suspension, degraded reads with E31's hedging and `read-avoid`, metadata v3, TOPOLOGY with both margins, unavailable/corrupt/lost, `TSDB_WHY_LOST` in the four clients, `min_isr` by mode | `make jepsen-cluster` (kill, partition, partial partition, pause past the lifetime, disk loss, chunk rot, crash between `MK_COLD` and commit) on 4+2 and 16+4; `make test-clients` | 12 | 6-8 |
| R9 | Failure handling: DOWN/OUT at `rebuild_after`, opening hold and fail-stop fast path, bounded holds and guard, degraded settles, retention for DOWN members, backpressure (`TSDB_WHY_FULL`), checked stripe rebuild by margin and pace, the `whole` barrier, return during rebuild and `node replace`, moves and the balancer with `balance_mb_s`, `node drain`; `sim_place` with its mutants and MTTDL | sim_place soak; jepsen-cluster (rebuild during writes, back during rebuild, two, m, m + 1, slow node, slow then disk loss, 2-hour hold, `STRIPE_SET` during a settle) on all four geometries | 24 | 6-8 |
| R12 | Scrub (E25, §5.9): chunks, `.ec` files and young parts, damage counts and disk OUT, health age, metrics | jepsen-cluster: rot (parity included) then m failures | 6 | 2 |
| R13 | Disks (E27, §5.5): device DOWN/OUT, rebuild within a server, `disk out` and `disk replace` | jepsen-cluster: single-disk loss, disk loss back within 30 min | 6 | 2-3 |
| R14 | Operator CLI (`node hold/replace/drain/destroyed`, `disk ...`, `volume move/redundancy/salvage/gc`, `cluster status` with both margins, rebuild progress and settle backlog) and metrics (docs/metrics.md) | test_server cases | — | 2 |
| R10 | Redundancy change on a running volume (§6.4: order, re-encode, `MK_UNSETTLE`) | jepsen-cluster with a conversion running | 6 | 3 |
| R11 | Benchmarks (§10.4, NVMe and HDD storage), operator guide (sizing for compute disks, index and rebuild time, holds, salvage, m = 4 above 20-30 TB a node), README section, AGENTS.md invariants (the settle cut, chunk fences, `MK_GC`) | the numbers | — | 2 |

R5-R14: **31-40 track-days**, 64 gate hours; with the prerequisites **about 44-60 track-days and 107 gate hours
(about 4.5 machine-days) before reruns after fixes**. On three tracks that is roughly 4-5 weeks of calendar, set by
the review rounds and the gates. R5 and R6 need nothing and can start now; R7 needs R2; R8 needs R3 and S2; R12 and R13
need R8; R9 needs R4 and R8; R14 runs alongside R9.

**What waits**: the S3/isod backend and the external cold tier (§9.4); local repair codes; shipping merges; partial
rebuild of a returning log replica from position ranges (design-cluster.md "Later"; E29's retention covers the
30-minute and held cases); direct-mode clients (S3 of design-ha-modes.md, forwarding serves until then); rolling
upgrades between peer versions (open question 5). If 0.11.0 must shrink, R10 moves to 0.12 first (open question 2).

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
| H3: `min_isr = 1` does not survive m losses | E4, E24, §5.8; open question 4 |
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
| M8: 0.11.0 holds data but cannot upgrade | E18, open question 5 |
| M9: plan underestimated, steps missing, gate time, S4's gate, Totals | §11 (R7-R9 re-estimated, R12-R14 added, gate hours, S4's own gate), design-ha-modes.md §7 rows, Order and Totals; cut line in open question 2 |
| m1: costs missing (merges, ingest, compute-only settle, index) | §4 |
| m2: adding a node is bound by its receive rate | §6.2 (`balance_mb_s`) |
| m3: geometry change takes a week | E2, §6.4 |
| m4: coarse partitions stay replicated a year | E5 (coarse keys settle part by part), EA2, EU5 |
| m5: the fence is longer than 1-10 ms with forced flushes | E5 (wait for a natural flush), §2.3 proof paragraph, §10.4 |
| m6: licence details | open question 1; docs/licenses.md corrected |
| m7: small objects pay more | E7 (cell size per object), §4 |
| m8: warm-up may not fit, competes with rebuild | §3.3 |
| m9: opening hold and fail-stop fast path | E12 |

## Open questions (each with a recommendation)

1. **The licence of the EC code.** intellistream-objects is AGPL-3.0-only (objects:LICENSE since its commit 3fe89800
   of 2026-10-26, LICENSING.md:1-4) and sells a commercial licence; tsdb is Apache 2.0. The Raft core came under the
   owner's grant D2 (src/raft/UPSTREAM, docs/licenses.md:16-21), which names `src/raft` only. (a) A second grant
   naming `src/codec/gf256.c`, `gf256.h`, `gf256_avx2.c`, `gf256_avx512.c`, `rs.c`, `rs.h` (1,143 lines), copied by
   `tools/sync-ec.sh` with `src/ec/UPSTREAM`. Unlike Raft, this is **a patched copy**, not an unchanged one: the files
   include objects' `common/{cpu,clock,log,util}.h` (compat shims, as for Raft), gf256.c has a constructor
   (objects:src/codec/gf256.c:469) that AGENTS.md "CPU levels" forbids, so the dispatch moves to tsdb's `src/cpu.c`; and
   the codec lands in libtsdb (the read seam is `granule_read`), so it reaches the embedded Rust, Python and Java
   bindings, where Raft did not (docs/licenses.md:21). All 16 commits to objects' `src/codec` are the owner's, so the
   grant is possible today; it should be written into objects' LICENSING.md ("these files are also available under
   Apache 2.0"), cover later syncs, and be given knowing that Apache 2.0 carries a patent licence (its §3). (b) Our own
   from the papers (Cauchy RS; the split-table PSHUFB method of Plank, Greenan and Miller, "Screaming fast Galois field
   arithmetic using Intel SIMD instructions", FAST 2013; GFNI's affine instruction), 2-3 more track-days with the same
   tests; not a clean room, since the people and agents writing it have read objects' code. **Recommended: (a)**, the
   grant written in objects' LICENSING.md covering later syncs.
2. **What 0.11.0 holds.** The user put this design in v0.11.0 and asked for the four geometries to be tested; the
   honest size is 44-60 track-days with the prerequisites (§11). **Recommended**: 0.11.0 holds the replicated data path
   (S1 rest, S2, S4, R2-R9, R11-R14) with `replicas=3` and all four geometries, tested on one machine and in the
   simulator, and server roles (cheap; they answer the separation question); 8+4 and 16+4 hardware numbers are not
   claimed until measured on servers. **R10 (changing the redundancy of a running volume) moves to 0.12** as the
   first cut (volumes keep the redundancy they were created with); shared mode's N-steps go on in parallel and ship in
   0.11.0 only if their maintenance windows came (E23).
3. **The default redundancy of a new cluster.** **Recommended**: `ec=4+2` when `cluster init` sees at least 7
   storage-capable servers, else `replicas=3`; `init` prints which and why, and `--redundancy` overrides; the operator
   guide recommends m = 4 above 20-30 TB of chunks a node (§5.3).
4. **The young tier's ISR floor (E24).** D1 (the user's) keeps `min_isr = 1`; with it, an EC volume's acknowledged rows
   survive one loss at worst, not m. **Recommended**: `min_isr = m` by default for `ec=k+m` volumes, `replicas=3`
   unchanged, both margins in TOPOLOGY; the cost is that an EC volume refuses writes when more than one of its log
   replicas lags (m = 2), or more than one of five (m = 4).
5. **Preview or upgradeable cluster data.** From 0.11.0 the cluster holds 10-100 TB a node; the preview rule
   ("initialised again" at a metadata or peer protocol change, E18; peers of one version only, design-cluster.md
   §11.2) would then mean export and import of everything, and every tsdbd upgrade a whole-cluster restart.
   **Recommended**: 0.11.0's cluster data path ships as a **preview for test data** (CHANGELOG, docs and `cluster init`
   say so), but 0.11.0 already carries the in-place upgrade of metadata snapshot v3 to later versions (1-2 track-days in
   R8, so test clusters survive 0.12); rolling upgrades (peers speaking versions N and N + 1, with a jepsen-cluster
   upgrade one node at a time) are designed and built before the cluster is declared for production.

## Unverified assumptions

- EU1: GFNI kernels encode k = 16, m = 4 above 8 GB/s a core on Zen 4/5 (R5 measures).
- EU2: A cold-cache decode query costs one round trip per partition touched with pipelining (R11).
- EU3: Young data is about one day of ingest per volume with `settle_grace_s` = 1 h and day partitions, plus the
  settle wait (EA2, §4).
- EU4: Chunk files on ext4/XFS, one file system per disk, are fast enough for storage servers; no raw-device store is
  needed.
- EU5: Coarse-class keys (7-364 days) hold a minority of the bytes on a realistic mix of 10^9 series (R11 measures).
- EU6: The logical digest (one decode of each settled partition per log replica) costs a few percent of a core at the
  100 B benchmark's ingest (R11 measures; if not, it samples series instead of decoding all).
