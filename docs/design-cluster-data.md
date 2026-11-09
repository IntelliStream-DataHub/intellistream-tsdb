# tsdb cluster data: erasure coding, 3 replicas, volume moves and failover

Status: design, not implemented. Branch `cluster-data-design`, written on 2026-11-09 from main at 47ae317 (0.10.0).
Code references are `file:line` at that commit for tsdb (design-cluster.md and design-ha-modes.md as edited on this
branch), and at 9c5f97d2 (origin/main, pulled 2026-11-09) for
intellistream-objects (written `objects:path:line` below). Target release: **v0.11.0**.

The user's request (2026-11-09, verbatim): "cluster should support data replication, preferably using erasure code,
test with 4+2 and 8+2 and 8 + 4 and 16+4. I know we lose performance, but it makes operation much easier. Its a
tunable config. Also support replication with 3 replicas. We should support moving of volumes and fail overs, like if
one node is down for 30 minutes, we start moving the data." and "that will go in v0.11.0". Asked during the design:
"with this new cluster implementation, will tsdb support true seperation of storage and compute?" (§9, E20-E22).

**What this reverses.** [design-cluster.md](design-cluster.md) kept erasure coding out of the cluster: "Not here:
erasure coding, shard placement, ISA-L, degraded reads" (design-cluster.md:47, struck out on this branch) and C21, "Cold tier: only the
boundary here" (design-cluster.md:127). Erasure coding is now a first-class, tunable redundancy mode of the replicated
(shared-nothing) mode, beside 3 replicas. Everything else in design-cluster.md stands: the volume as the unit (C1),
the metadata group (C2, amended by E8 for m = 4), the WAL replicated by the leader to an ISR (C4-C7, C11), failover
and fencing (C12-C14), name slots, clients. It is extended, not replaced: an erasure-coded volume **is** a replicated
volume with m + 1 copies of its young data, plus an erasure-coded tier for its settled partitions (E3).

Related: [design-cluster.md](design-cluster.md) (replicated mode), [design-ha-modes.md](design-ha-modes.md) (the two HA
modes and the combined build plan, §7 there carries this design's steps R5-R11), [design-cdc.md](design-cdc.md)
(positions, `fno`).

## Summary

- **Redundancy is a per-volume setting** with a cluster default: `replicas=3` or `ec=k+m` with k+m one of 4+2, 8+2,
  8+4, 16+4 (any k ≤ 16, m ≤ 4 is accepted; those four are tested). It can be changed on a running volume, as a
  background conversion (E2).
- **Two tiers per volume.** Young data (the WAL, memtables and the parts of partitions still taking rows or merging)
  is replicated exactly as design-cluster.md says, on **m + 1** servers (3 in `replicas=3`): the same fault tolerance
  as the code. A partition that is over and merged to one part is **settled**: its part file is erasure coded across
  k + m servers and the full copies are retired. Most bytes live settled: 1.25x to 1.5x the data instead of 3x.
- **The settled tier is design-cluster.md §9's cold boundary moved inside the cluster.** The unit is a whole part file,
  unchanged (granule offsets stay offsets); every log replica keeps the part's granule index locally, so queries
  answered from statistics cost what they cost today. Decoding a granule reads its bytes from the server holding its
  cell, or, degraded, the same byte range from k others.
- **Settling is a replace.** The leader encodes its one part of the partition, puts the chunks, then appends a marker
  that, at the commit point, swaps every log replica's parts of that partition for the encoded object plus the few
  rows that arrived meanwhile (shipped as files, as large inserts are). Last write wins as on one server.
- **Failure**: a server down is degraded at once (reads decode, writes go on through the ISR, settles wait); down for
  `rebuild_after` (30 min) it is OUT and its log copies and chunks are rebuilt elsewhere, most endangered first,
  paced; back before that, it only catches up. m servers down: still no loss and still writable; m + 1: the volumes
  that had m + 1 members there report data lost, by partition, never silently.
- **Storage and compute can be separated** (E20): `compute` servers lead volumes, hold the replicated WAL and young
  parts and cache granules; `storage` servers hold chunks only; `both` is the default. They scale independently.
- **Codec**: systematic Cauchy Reed-Solomon over GF(2^8) with SSSE3/AVX2/AVX-512/GFNI kernels, as in
  intellistream-objects `src/codec`. Copy it under a grant (as the Raft core was) or write our own: open question 1.

## Assumptions

| # | Assumption | If it does not hold |
|---|---|---|
| EA1 | design-cluster.md A1-A5 hold (own disks, 25 GbE, one data center, NVMe for the WAL). | As there. |
| EA2 | Most bytes are in partitions that end and stop changing: dense series in day partitions (class 0, AGENTS.md "Partition classes"). Slow series in 7-364-day partitions are few bytes (that is why they are in coarse classes). | If late or back-dated writes keep old partitions busy, more data stays replicated (m + 1 copies); E5's grace and E6's later objects bound it. |
| EA3 | Settled data is read less than young data, and mostly through granule statistics. | If decode-heavy queries over old data dominate, raise the granule cache or `ec_keep_local_h` (E13). |
| EA4 | One failure domain is one server (one chunk of an object per server); racks when labelled. | Several chunks per server would make a server loss cost several chunks; refused (E9). |

## Decisions

| # | Decision | Rejected | Why |
|---|---|---|---|
| E1 | **Redundancy per volume**, `replicas=3` or `ec=k+m` (k ≤ 16, m ≤ 4; 4+2, 8+2, 8+4, 16+4 tested), default from the cluster setting `redundancy` (`tsdb cluster init --redundancy`), overridable at `volume add`. | Per cluster only; per series | Volumes already carry per-volume options (`min_isr`, D1); mixing lets a small, hot, high-value volume stay on 3 replicas beside bulk EC volumes. Per series would need a map the size of the data. |
| E2 | **It can change on a running volume** (`tsdb cluster volume redundancy V ec=8+4`): new settles use the new geometry at once; a background job re-encodes older objects at rebuild pace; the log group grows or shrinks by moves (design-cluster.md §3.2). Each object records its own geometry. | Fixed at creation; moving series to a new volume | Series ids name their volume (AGENTS.md "Series ids"), so data cannot move to another volume. Recording the geometry per object (as objects records it per manifest entry, objects:src/recovery/rebuild.c:26-28: "Everything comes from the manifest entry and never from today's pool") makes the change safe at any moment. |
| E3 | **Two tiers.** Young data (WAL, memtables, unsettled parts) is replicated on the volume's **log group** of m + 1 servers (3 for `replicas=3`) by the unchanged replication core; settled partitions are erasure coded across the volume's **stripe group** of k + m servers. | EC every part at flush; EC the WAL; stripes packed across parts; one object per series | See §2.1. In short: flush parts are small and merges rewrite them several times, so EC at flush would ship and re-encode every merge level over the network and lose C9 (each replica merges on its own); a WAL record must be durable on ack, and EC would need k + m syncs per record instead of m + 1; packing parts into shared stripes needs stripe garbage collection after deletes; 10^9 series make per-series objects impossible. |
| E4 | **m + 1 copies of young data** for `ec=k+m`. | 3 always; fewer | The same number of losses (m) is survived by every byte of the volume, acknowledged WAL rows included (design-cluster.md A2 with m in place of 2). |
| E5 | **A partition settles** when, on the leader: its end (`end_ms`) is `settle_grace_s` (3,600 s) in the past, it is one part with no merge pending, all k + m stripe members are UP, and no settle of it is running. Late rows do not block it. | §9.1's idle rule (`cold_idle_s`, 1 day) | Late rows go to young parts above the object (E6), so waiting for idleness only keeps more data at m + 1 copies. |
| E6 | **Settling is a replace of the partition** by the leader's bytes (§2.3): the encoded part `S` plus the leader's parts of that key published after `S` was chosen (`Y`, shipped as files like a large insert, design-cluster.md:461-492). Every log replica retires all its own parts of the key. Later settles of the same key add further objects above; a re-encode job combines a key's objects when it has more than `ec_objects_max` (4) or when tombstones cover more than 10% of them. | Retiring by position range only | Replicas flush and merge on their own (C9), so a replica's part can mix rows below and above the leader's cut; replacing the whole key with the leader's bytes avoids splitting parts. |
| E7 | **The object is the part file unchanged**, laid out in **cells** of `ec_cell_kb` (1,024 KiB) dealt round-robin over the k data chunks; parity per stripe of k cells. Small objects use k' = min(k, cells) data chunks. | Contiguous chunks (L/k each); per-granule objects | A series' granules lie together in a part (sorted by series, ts: src/part.c:1-2), so contiguous chunks would put a series' whole scan on one server; 1 MiB cells spread a partition scan over k servers and keep a granule (about 10 KB at 8,192 rows) in one cell 99% of the time. k' for small objects is objects' "derived k" (objects:src/codec/geometry.h:8-18) without a mirror rule: a one-cell object is m + 1 copies by construction. |
| E8 | **Metadata voters: 5, or 2m + 1 (at most 9) when a volume has m > 2**; amends C2's "at most 5". | 5 always | With m = 4 the data survives 4 lost servers, but 5 voters lose quorum at 3, and without quorum no ISR shrinks and no leader is promoted. Raft is off the insert path (C4), so 9 voters cost only metadata latency. |
| E9 | **Placement**: the log group on m + 1 distinct compute-capable servers, the stripe group on k + m distinct storage-capable servers, one position per server, at most ceil((k + m) / racks) positions per rack; a rack loss is survivable only when that is ≤ m, and health says so. Lists are explicit (C19), positions never renumbered. | Two chunks of an object per server; computed placement | Objects' rules (objects:src/placement/README:144-160, one target per host, the rack cap; :92, the rack-survival condition). Positions are like objects' PG ids, never reused (objects:src/placement/README:134-135), so an object names positions, not servers, and a move rewrites no object. |
| E10 | **Settles wait for a whole stripe group**; there are no degraded settle writes. | Objects' `min_write_shards` (k + 2, objects:src/codec/geometry.h:94) | The young tier already holds the data at full redundancy; waiting costs only space for at most `rebuild_after` plus a rebuild assignment. |
| E11 | **Reads**: index local on every log replica; block bytes through one seam (`granule_read`, src/part.c:313) that reads the local data chunk, the granule cache, or the cell's server; a missing or failing cell is decoded from k others **over the granule's byte range only**. Every remote or decoded granule is checked against its index checksum (always, as §9.4 of design-cluster.md says for cold reads). | Whole-cell decode (objects' gateway, objects:src/gateway/pipeline.c:495); trusting `verify_checksums` | Reed-Solomon works byte column by byte column, so a 10 KB granule needs 10 KB from each of k cells, not k MiB. A wrong survivor gives a plausible wrong answer (objects:src/recovery/rebuild.c:21-25), so checks are never optional here. |
| E12 | **Failure states as objects': DOWN, then OUT** after `rebuild_after_s` (1,800 s; replaces design-cluster.md's `out_after_s`, :317); operator holds (`node hold N --for 2h`) pause the timer; no automatic OUT while a whole rack, or more than 25% of the servers, is down. | Rebuilding at once; no guard | "The timer between them is what stops a reboot from triggering a terabyte of copying" (objects:src/cluster/cmap.h:67-70); the holds and the mass-OUT guard (objects:src/cluster/cmap.h:168, :895-925; default 25%, objects:src/mds/server.h:89-93) keep a power or switch event from starting a rebuild storm. |
| E13 | **Rebuild order: most endangered first** (fewest copies or chunks left beyond the minimum: k for EC, 1 for replicas), then balance moves; paced by design-cluster.md §8.4's token bucket. A plain copy from a holder that is back is preferred over decoding. | FIFO; decode always | Objects' decision of 2026-10-20 (objects:src/mds/server.h:1852-1860): a 4+2 object two short goes before a 16+4 object two short. A copy reads 1x, a decode kx. |
| E14 | **Chunk writes and deletes are fenced by the volume epoch**; chunk reads are not. An object is live only through a committed marker. | Fencing reads; trusting the sender | Objects are immutable and checksummed, so a read from any holder is safe; a deposed leader can only leave orphans, which the next leader deletes (design-cluster.md §9.3 step 4). |
| E15 | **m + 1 losses are reported, never hidden**: the affected partitions answer `TSDB_ERR_IO` naming the partition; the volume is `LOST` in TOPOLOGY; `tsdb cluster volume salvage V` lets the operator drop what is gone through DROP markers. | Serving what is left silently | AGENTS.md "Damage is never silent". |
| E16 | **Shared NFS mode has no EC.** `redundancy` is replicated mode only; a shared-mode cluster refuses it. | EC over NFS | ZFS already gives the redundancy under NFS; one copy on shared storage is shared mode's point ([design-ha-modes.md](design-ha-modes.md) §1). |
| E17 | **No new disk format number.** Chunk files, `.ec` index files and the `MK_COLD` body live only in cluster volumes behind `requires=cluster,ec` in META (format 9's mechanism, src/feedfile.c:385, src/db.c:437); they freeze at 1.0 with the rest. | Format 10 | Nothing in a single-server volume changes; the `requires=` line refuses the files in a binary that does not know them. |
| E18 | **The metadata snapshot becomes version 3** (redundancy, two member lists per volume, roles, holds), and the peer protocol version 3 (chunk messages). Preview clusters are initialised again (as from version 1 to 2, CHANGELOG 0.10.0). | Upgrading preview clusters | The preview says so (CHANGELOG [0.10.0], "builds of the preview are not compatible with each other across such a change"). |
| E19 | **The EC codec is systematic Cauchy Reed-Solomon over GF(2^8), polynomial 0x11d**, kernels dispatched at startup, every kernel byte-identical to the scalar one. | ISA-L as a dependency; LRC or other codes | Interoperable with Jerasure, ISA-L and klauspost (objects:src/codec/gf256.h:1-5); any k of k + m reconstruct, with no unlucky sets (objects:src/codec/rs.h:11-15). Locally repairable codes save rebuild reads but are not MDS at the same space; a later change if rebuild reads hurt. Where the code comes from is open question 1. |
| E20 | **Storage and compute can be separated**: server roles `compute`, `storage`, `both` (default). §9. | Every server both | The user's question. It costs little (two member lists exist anyway, E3) and lets bytes and CPU scale apart. |
| E21 | **A compute server's state is small and replicated**: the young tier (about a day of ingest at m + 1 copies) and caches; losing one loses nothing durable and is rebuilt from its log group in minutes. | Stateless compute nodes (WAL on storage nodes) | A WAL on remote storage adds a round trip to every acknowledged insert; m + 1 local NVMe copies are the replicated mode that already exists. |
| E22 | **The settled tier is an interface with backends**: the cluster's own storage servers (0.11.0), a directory (tests), and later isod or any S3 (design-cluster.md §9.2's `tsdb_cold_ops`). | Only the own backend | With an S3 backend, tsdb servers become compute only and isod does the erasure coding and its rebuild. That is the "true separation" end state; it waits for 0.12 or later (§9.4). |
| E23 | **0.11.0 holds replicated mode's data path (R2-R4) and this design (R5-R11)**; shared mode's steps go on in parallel and ship when their maintenance windows allow. | Waiting for N7 (M21's order) | The user's "that will go in v0.11.0". M21 put replicated mode after shared mode; this changes the order, not the content (open question 2). |

## 1. Words

- **Log group** of a volume: the m + 1 replicas (3 in `replicas=3`) of design-cluster.md: leader, ISR, learners. They
  hold the WAL, memtables, young parts, and the `.ec` index files of settled objects. Only they answer queries.
- **Stripe group**: k + m **positions** (0 .. k + m − 1), each held by one storage-capable server; data positions
  0 .. k − 1, parity positions k .. k + m − 1. Empty for `replicas=3`.
- **Object**: one settled part file, erasure coded. **Cell**: `ec_cell_kb` bytes of it. **Stripe**: k consecutive
  cells and their m parity cells. **Chunk**: what one position holds of one object (a file).
- **Settle**: turning a partition's part into an object (§2.3). **Margin**: copies (replicas) or chunks (EC) left
  beyond the minimum needed to read (1, or k).
- **DOWN** / **OUT**: a server not answering, whose data still counts / given up on, rebuilt elsewhere (E12).

## 2. What is coded and what is replicated

### 2.1 Why parts, and only settled ones

Parts are immutable and already the unit of every copy in the design: large inserts ship part files (C8,
design-cluster.md:461-492), rebuild copies them by hard link (§8.1 there), the cold move hands over a whole part
(§9.3 there). A part file is self-describing: header, granule blocks, the packed index, tombstones, footer with a hash
over everything but the blocks, and a checksum per granule (src/part.c:1-21; the per-granule check at
src/part.c:313-317). So an object needs no layout of its own beyond the cells.

What is **not** coded, and why:

| Data | How it is kept | Why not EC |
|---|---|---|
| WAL records | replicated to the log group, byte for byte (C5) | A record is acknowledged when durable (C11): m + 1 syncs per group commit; EC would need k + m. |
| Memtables | each log replica's own, from the WAL | In memory. |
| Unsettled parts (flushes, merges in progress, large inserts) | each log replica's own (C9) | They are rewritten by merges within hours; coding them would ship every merge output over the network (about (k + m − 1)/k of its size per level) and remove C9. |
| Granule index of a settled part | an `.ec` file on every log replica | Read by every query; about 32 bytes a granule (AGENTS.md "on-disk format"), a few hundred KB for a 1 GB part. |
| SERIES, STATES, CLASSES, RETENTION, MANIFEST, REPL, FNO, the slot index | every log replica, through WAL markers (design-cluster.md §5.4-5.5) | Small, append-only, needed at open. |
| Metadata (members, volumes, lists, users) | the metadata group (C2, E8) | Kilobytes. |

### 2.2 Layout of an object

The object is the part file's bytes, length L. Cells are numbered c = 0 .. ceil(L / cell) − 1. With
k' = min(k, number of cells), cell c is in data chunk c mod k' at offset (c div k') × cell; stripe s holds cells
s·k' .. s·k' + k' − 1 (the last one padded with zeros for the parity computation; the zeros are not stored). Parity
chunk j holds parity cell j of each stripe.

A **chunk file** on its server: `<data>/s<volume>/o<oid>.<pos>.chunk`:

```
header (64 bytes): magic "TSDBCHK1", u32 version 1, u32 flags, u8 volume uuid[16], u8 oid[16], u16 k', u16 m,
                   u16 pos, u16 reserved, u32 cell_bytes, u64 object_len, u64 chunk_len
cells             chunk_len bytes
cell checksums    u64 xxh64 per cell of this chunk
footer (16 bytes): u64 xxh64 of header and checksum table, u32 cell count, u32 magic "CHKE"
```

Written as `tmp_*`, fsync'd, renamed, directory fsync'd before the server answers PUT_OK (AGENTS.md "Durability"). A
cell whose checksum fails is an erasure, never decoder input (objects:src/recovery/rebuild.c:21-25).

The **object record** (in the `MK_COLD` marker body, src/marker.h:40, reserved in format 9, defined here; and as the
trailer of the `.ec` file): `oid` (16 random bytes from the leader), partition key, the block range it replaces,
`object_len`, xxh64 of the object, k', m, `cell_bytes`, the position list version it was written under, and the list
of `Y` files (§2.3) with names, sizes and xxh64.

The **`.ec` index file** on each log replica: `p<key>_<min>_<max>_<lvl>.ec`, holding the part's header, granule index,
tombstones and footer exactly as in the part, then the object record. It is mapped like a part's index (no copy:
AGENTS.md "The granule index"), so `gx_*` and `gx_load` read it unchanged.

### 2.3 Settling a partition (the leader)

1. **Choose.** Partition key P is eligible (E5). The leader pins its one part `S` of P (merge selection skips P while
   it settles) and notes `S`'s max block.
2. **Encode and put**, outside every engine lock: read `S` by mapping, encode stripe by stripe (1 MiB cells, the
   kernels of E19), send chunk i to position i on the bulk link (C25), the leader's own position written locally if
   it holds one. Each server writes the chunk durable and answers PUT_OK with its xxh64. A put to a position whose
   server is DOWN fails the settle (E10); it is retried later.
3. **Fence the key.** `lw_enter` over P's whole key range (every series, P's time range), the large-write fence of
   AGENTS.md "Last write wins". Rotate the WAL segment if the memtable holds rows of P (that segment is `F`) and flush
   through it, as design-cluster.md §5.2 step 1.
4. **Remainder.** `Y` = the leader's parts of P with block numbers above `S`'s (flushes, large inserts, tombstone
   parts published after step 1, and the flush of step 3). They are small: rows that arrived during steps 1-3. Ship
   them to the log replicas as pending files (design-cluster.md §5.2 step 3).
5. **Marker.** Append `MK_COLD` with the object record and `F`. Replicated like any record.
6. **Apply** at the commit point, on every log replica including the leader, under `publish_lock`: flush memtables
   through `F` if not done; write the `.ec` file; list it with block numbers covering every part of P it replaces;
   commit the `Y` files above it under new block numbers (`parts_commit`); rename every older part of P to
   `retired_*` (deleted after `obsolete_part_lifetime_s`, as merges' are). Then `lw_leave`.

**Why last write wins holds.** On each log replica records apply in position order (C7). At the marker every row of P
below it is applied (memtable or parts) and none above. The replica's parts of P hold exactly the rows of P below the
marker, as the leader's `S` ∪ `Y` do (replicas are logically equal, C9), so swapping them changes no answer. Inside
the new set, `Y` ranks above the object as it did above `S` on the leader. Rows after the marker get higher block
numbers or stay in memtables, which rank above parts. The fence (step 3) holds only through a flush, a small ship and
one replication round: about 1-10 ms, noticed only by a write into P during it.

**Crash and failover.** Chunks put before a marker commits are orphans: the next leader lists its stripe group's
objects and deletes those no committed marker names (design-cluster.md §9.3 step 4). A replica that misses a `Y` file
fetches it before acknowledging the marker (as for `COMMIT`, design-cluster.md §5.2). Chunk-only servers apply
nothing; they learn of deletions from the leader (§5.6).

### 2.4 After a settle: late rows, deletes, retention, merges

As design-cluster.md §9.4: late rows for P go to the memtable and young parts above the object; a delete over it
writes tombstones in young parts; merges never take an `.ec` part as input. Young parts of P settle again after
`settle_grace_s` as a further object (E6). A **re-encode** combines a key's objects and its young parts when there
are more than `ec_objects_max` objects or tombstones cover more than 10% of their rows: it reads the objects through
the read path into a local temporary part, runs a normal merge, and settles the output, replacing all of them in one
marker. A retention drop of a whole partition (`MK_DROP`, src/marker.h:31) retires the `.ec` file; the leader deletes
the chunks after `obsolete_part_lifetime_s`. Expired rows inside a settled partition are clamped by queries as
today and go only when the whole partition does (partitions span at most 364 days).

### 2.5 `replicas=3`

Unchanged from design-cluster.md: a log group of 3, no stripe group, no settles. The design-cluster.md text is this
mode exactly.

## 3. Reads

### 3.1 Where the bytes come from

Queries run on the leader (D5) or a log replica (`REPLICA_OK`); chunk-only servers never answer queries. For an
`.ec` part:
- **Statistics**: from the local `.ec` file, mapped. A query answered from granule statistics costs what it costs
  today: nothing is fetched.
- **Granule blocks**: `granule_read` (src/part.c:299-325) is the one place that reads block bytes for queries
  (`p->map + g->off`, src/part.c:313); the merge's verbatim copy (src/part.c:726, :730) never sees an `.ec` part, and
  the resident-bytes statistics (src/stats.c:1134, src/stats_hot.c:424) count an `.ec` part's cached bytes instead.
  For an `.ec` part the seam asks, in order: the granule cache; the local chunk, if this server holds the cell's
  position; the cell's server, for the granule's byte range (one round trip, pipelined with the other granules of
  the query step: R7 makes the query threads of `src/sched.c` issue the reads for every granule of their range
  before they decode the first).
- **Check**: the granule's 32-bit checksum (`g->crc`, src/part.c:315-317) is checked on every fetched or rebuilt
  granule, whatever `verify_checksums` says.

### 3.2 Degraded reads

A cell whose server is DOWN, refuses, times out (`ec_read_timeout_ms`, 50 ms, then the read is treated as missing and
hedged), or whose bytes fail the check, is rebuilt from **the same byte range** of k other cells of its stripe (data
cells first, then parity), with the decode kernel; the inverted matrix is cached per erasure pattern (objects' plans,
objects:src/codec/rs.h:63-78). If more than m cells of a stripe are missing or the rebuilt granule fails its check
after a trial with every other spare cell (objects' trial decode, objects:src/recovery/rebuild.c:28-40), the query
fails with `TSDB_ERR_CORRUPT` naming the partition and the granule, and the leader schedules a repair of the object.
With exactly m cells missing there is no spare to check against: a failing granule is an error, never a guess.

### 3.3 The granule cache

Per server, `ec_cache_mb` (10% of RAM by default), keyed by (oid, granule offset), LRU, filled by reads and by
prefetch: a new leader (after failover or a move) prefetches the cells of the objects settled in the last
`cache_warm_h` (24 h) at `cache_warm_mb_s` (200 MB/s). `ec_keep_local_h` (0 by default) keeps the leader's full part
file for that long after a settle (the space of one more copy of the newest settled day), for sites where yesterday
is read hard.

### 3.4 Expected performance against one server today

Estimates, to be measured (§10.2). Today's numbers are AGENTS.md's (7950X, one server).

| Query | Today | `replicas=3` | EC, warm cache | EC, cold cache | EC, one position down |
|---|---:|---:|---:|---:|---:|
| from statistics (whole table, 100 G rows) | 0.17 s | same | same | same | same |
| one series, one window, 100 K rows (decodes ~12 granules) | 0.57 ms | same | same | + 1 round trip: ~0.7 ms | + k range reads: ~0.8 ms |
| one series by hour, 10 M rows over 116 days (~12 MB) | 16.3 ms | same | same | +116 partitions' ranges at 3 GB/s, pipelined: ~20-25 ms | ~1/(k+m) of granules at kx bytes |
| whole table by series and hour (100 G rows, ~116 GB, disk bound) | 46.8 s | same | — | bytes from k servers in parallel, NIC bound: ~same | +(1/(k+m))·k extra reads for the missing share |

Writes are design-cluster.md §10.1's targets for `replicas=3`, 4+2 and 8+2 (3 copies). For 8+4 and 16+4 every small
insert waits for 5 syncs: estimate p50 +0.1 ms and p99 +0.5 ms over 3 copies (the slowest of 5 `fdatasync`s).

## 4. Writes and their cost

The insert path is design-cluster.md §5 with an ISR of up to m + 1: unchanged code (`src/repl.c`: the commit point is
the lowest durable end of the ISR, src/repl.c:319, :486). The settle (§2.3) adds the encode and the chunk puts once
per partition.

**Bytes per row** (settled steady state, 1.2 bytes a row in a part, AGENTS.md benchmarks; W = the compressed WAL
bytes of a row on the log stream, to measure):

| Mode | Disk, settled | Disk, young | Network, WAL | Network, settle (once) | Survives |
|---|---:|---:|---:|---:|---:|
| `replicas=3` | 3.6 (3x) | 3.6 | 2W | 0 | 2 |
| `ec=4+2` | 1.8 (1.5x) | 3.6 | 2W | 1.5 | 2 |
| `ec=8+2` | 1.5 (1.25x) | 3.6 | 2W | 1.35 | 2 |
| `ec=8+4` | 1.8 (1.5x) | 6.0 | 4W | 1.65 | 4 |
| `ec=16+4` | 1.5 (1.25x) | 6.0 | 4W | 1.43 | 4 |

Settle network = 1.2 × (k + m − 1)/k (the leader's own position local), plus the `Y` remainder (small).

**Encode CPU.** Encoding is m multiply-adds per input byte (objects:src/codec/gf256.h:6-8). Measured in objects on
old CPUs, k = 16, m = 4, 1 MiB shards: 3.45 GB/s a core with AVX2 (E5-2697 v4,
objects:bench/baselines/2x-se1/olav2-e5-2697v4/rs-encode.txt) and 2.41 GB/s with SSSE3 (E5-2696 v2,
objects:bench/baselines/2x-se1/olav3-e5-2696v2/rs-encode.txt); m = 2 halves the work. Zen 4 and Zen 5 reach the GFNI
kernels (objects:src/codec/gf256.h:14-20): not measured; R5 measures them. A leader settling the 100 B benchmark's
ingest (114 M rows/s × 1.2 B = 137 MB/s, AGENTS.md) spends about 4% of one AVX2 core on it.

**Node counts.**

| Mode | Servers needed | Recommended (rebuild without a replacement) | Log group | Voters (E8) |
|---|---:|---:|---:|---:|
| `replicas=3` | 3 | 4 | 3 | 3-5 |
| `ec=4+2` | 6 storage, 3 compute | 7 | 3 | 5 |
| `ec=8+2` | 10 storage, 3 compute | 11 | 3 | 5 |
| `ec=8+4` | 12 storage, 5 compute | 13-14 | 5 | 9 |
| `ec=16+4` | 20 storage, 5 compute | 21-22 | 5 | 9 |

With `both` roles the compute servers are among the storage ones. A cluster of exactly k + m storage servers works but
rebuilds only onto a replacement (health warns); every volume then spans every server, so m + 1 losses lose every EC
volume's settled data.

## 5. Failure handling

### 5.1 Detection and the first seconds

A server that misses heartbeats for 3 s is DOWN (design-cluster.md §6.1). At once:
- volumes it led fail over (design-cluster.md §6.2: the ISR member with the longest log, epoch + 1);
- ISRs it was in shrink by the leader's lag rule (C11), so writes go on with the remaining copies down to `min_isr`;
- reads of cells it holds are degraded (§3.2);
- settles of volumes with a position on it wait (E10). Nothing is copied yet.

### 5.2 Back before `rebuild_after`

Its log replicas cut and catch up (design-cluster.md §8.2: KIP-101 cuts, retained segments up to 30 min and
`repl_retain_mb`, a snapshot beyond). Its chunks are current: no object was settled on its volumes while it was away
(E10). It compares its chunk list with the leader's (CHUNK_LIST) and deletes chunks of objects that were retired
meanwhile.

### 5.3 OUT after `rebuild_after` (30 min)

The metadata leader commits `MEMBER_STATE out` (unless held or guarded, E12), then for every volume with a log or
stripe position on it, a new server chosen by E9's rules, as one `STRIPE_SET` or `REPLICA_ADD` per position:
- **Log position**: a learner, built by design-cluster.md §8.1 (hard-linked snapshot from an ISR member, catch-up,
  join). Young data only: gigabytes.
- **Stripe position**: state `filling`. It accepts new chunks at once (so settles resume), and rebuilds the old ones:
  for each object, read the same cell ranges of k surviving positions, decode, write, sync, then report. When it holds
  every listed object it becomes `whole`.

**Priority** (E13): positions whose volumes have the smallest margin first, then by bytes. **Pacing**: the token
bucket of design-cluster.md §8.4 (`rebuild_mb_s`, 200 MB/s per server, halved while foreground p99 is high), at most
`rebuild_concurrency` (4) objects in flight per server (objects' worker: objects:src/recovery/worker.h:56-64).

**Time.** A decode reads k bytes for each byte rebuilt. 2 TB of chunks on the lost server: `ec=8+2` on 11 servers reads
16 TB over 10 survivors, about 2.2 h at 200 MB/s each; `ec=16+4` on 21 servers about 2.3 h; at 1 GB/s per NVMe server
about 25-30 min. `replicas=3`: 28 min (design-cluster.md §8.4). This is the price of EC the user accepted; the
operator raises `rebuild_mb_s` where disks and network allow.

### 5.4 Back after the rebuild started

Its old positions are no longer its (the lists moved). Its log replica directories are stale (new rids are needed to
rejoin, gap 3 of design-cluster.md). Its chunks are immutable and checksummed, so until the new holders are `whole`
it serves as a **copy source**: a filling position copies a chunk from it (1x reads) instead of decoding (kx). Once
every replacement is `whole`, it deletes its old chunks. If it comes back before the metadata committed the
reassignment of a position, it keeps that position.

### 5.5 Disks full

A server stops being a placement candidate at its fill mark, `full × (1 − the largest domain's share)`, which keeps
room to rebuild the largest domain (objects:src/placement/README:84-99); rebuilds may use the room up to `full`.
ENOSPC on a chunk `tmp_*` fails that put only (nothing relies on it, AGENTS.md "Fail-stop"); ENOSPC on a WAL,
MANIFEST or dictionary is fail-stop as today.

### 5.6 Fencing and split brain

The log group is design-cluster.md §6 unchanged (epochs, ISR in the metadata group, no clocks). Stripe positions
learn each volume's epoch from the metadata watch and from FENCE (`REPL_M_FENCE`, src/repl.h:89, sent to them too).
They refuse CHUNK_PUT and CHUNK_DEL below it (E14). Deletes are asked only by the current leader, only for objects a
committed marker retired or no marker names (orphans), after `obsolete_part_lifetime_s`. A partitioned old leader can
therefore write orphans, and nothing else.

### 5.7 Two at once, m at once, m + 1

| Down | `replicas=3` | EC with m = 2 | EC with m = 4 |
|---|---|---|---|
| 1 | writes and reads on | on; degraded reads | on |
| 2 | on, ISR 1 for some volumes (D1) | on; volumes with both in their stripe at margin 0 (reads still whole, no check margin); ISR 1 | on |
| 3 | volumes with all 3 replicas there: unavailable; lost if the disks are gone | volumes with 3 positions there: settled data lost (E15) | on (9 voters, E8) |
| 4 | | | on; margin 0 for some |
| 5 | | | volumes with 5 positions there lost |

"Lost" is reported (E15): queries over the lost partitions fail with `TSDB_ERR_IO` naming them; TOPOLOGY marks the
volume `LOST`; UNAVAILABLE carries reason `TSDB_WHY_LOST` (18, after `TSDB_WHY_RELOADING` = 17,
include/tsdb_protocol.h:313-317) with the "do not retry" flag; `tsdb cluster volume salvage V` appends DROP markers
for what is gone so the volume serves the rest. A volume whose log group is wholly lost has lost acknowledged writes:
salvage says how many positions above the last settled object it cannot account for.

## 6. Moves and failover

- **Failover of a leader**: design-cluster.md §6 and the protocol 13 bodies (MOVED, UNAVAILABLE, TOPOLOGY;
  design-ha-modes.md §7 "S0, the wire half"). The new leader's cache is cold; §3.3 prefetches.
- **Operator moves**: `tsdb cluster volume move V --pos P --to N` (a stripe position) or `--log --to N` (a log
  replica). A stripe position moves by copy from its holder (1x, no decode), the target `filling` until whole, then
  one `STRIPE_SET` conditional on the list version; reads use the old holder until then. A log replica moves as in
  design-cluster.md §3.2 (learner, catch-up, join, drop).
- **Rebalancing** after `node add` or `node drain`: the greedy pass of design-cluster.md §3.2 over both lists, at most
  5% of a group's positions moving at once and no automatic plan that moves more than 25% (objects' balancer bounds,
  objects:src/placement/README:178-200), even leaders, chunk counts by capacity.
- **Redundancy change** (E2): new geometry recorded in the volume; new settles use it; a re-encode job walks the old
  objects at rebuild pace; the log group grows by learners or shrinks by drops.

## 7. Shared NFS mode and the existing plan

Shared mode has no redundancy setting (E16). Replicated mode's R1-R4 (design-ha-modes.md §7) stay as they are: R1's
core needs no change (an ISR of up to 5 is within `META_REPLICAS_MAX`, server/meta.h:68), R2's engine replica mode is
the log group, R3's replication is the young tier, R4's rebuild is the log-position rebuild. This design adds R5-R11
(§11) on top and moves the cold boundary X1 into R7 (its put, marker, `.ec` index and swap are the settle's).

## 8. Formats and versions

| What | Where | Freezes at 1.0? |
|---|---|---|
| Chunk file (§2.2) | storage servers, `s<volume>/` | yes, behind `requires=cluster,ec` |
| `.ec` index file: the part's header, index, tombstones, footer, then the object record | log replicas | yes |
| `MK_COLD` body (the object record and `Y`) | WAL, marker type 11 reserved in format 9 (src/marker.h:40) | yes |
| `FORMAT_VERSION` | stays 9 (E17) | |
| Metadata snapshot version 3: `struct meta_volume` (server/meta.h:161-183) gains `redundancy` (kind, k, m, cell), the stripe list (up to 24 positions: 20 plus 4 moving; each pos, node, rid, state), `stripe_version`; members gain roles and holds | metadata group | at 1.0 with the rest of the metadata (preview until then) |
| New ops at the end of `enum meta_op_kind` (never renumbered, server/meta.h:264-295): `STRIPE_SET` (if stripe_version == v), `NODE_HOLD`, `VOLUME_REDUNDANCY` | metadata group | yes |
| Peer protocol version 3 (`CL_VERSION`, server/cluster.c:95): CHUNK_PUT, PUT_OK, CHUNK_READ (oid, pos, offset, length), CHUNK_DATA, CHUNK_LIST, CHUNK_DEL, all with volume and epoch | peers | no (same-version peers only, design-cluster.md §11.2) |
| `TSDB_WHY_LOST` = 18; TOPOLOGY's per-volume health gains the margin and `LOST` | client protocol | the code yes; whether it needs a protocol number depends on whether every 1.0 client treats an unknown reason as retryable (to check in R8; if not, it waits for the next protocol number) |

## 9. Storage and compute separation (the user's question)

**Yes, for EC volumes.** E20-E22.

### 9.1 Roles

`tsdbd --cluster --role compute|storage|both` (default `both`), recorded in the member record (`META_OP_MEMBER_ADD`
flags, server/meta.h:267).
- **compute**: leads volumes, holds log replicas (WAL, memtables, young parts, `.ec` index files; local NVMe),
  answers queries, keeps the granule cache, runs settles and merges. Needs CPU, RAM and a fast small disk.
- **storage**: holds chunks only. No engine, no WAL, no queries: a chunk server (put, read range, list, delete,
  scrub). Needs capacity; HDDs serve it (a granule read is one seek).
- **both**: either, today's servers.

`replicas=3` volumes have no stripe group: all three copies are engines on compute-capable servers. True separation is
therefore an EC property.

### 9.2 Scaling each

More ingest or queries: add compute servers; leadership and log replicas move to them (young data only, so a move is
gigabytes and minutes). More bytes: add storage servers; stripe positions rebalance (copies, no decode). Neither
touches the other.

### 9.3 What a compute server's death costs

Nothing durable: its log replicas are 1 of m + 1 copies (E21), and its volumes fail over in seconds. Its cache is
gone; the new leaders warm theirs (§3.3): statistics queries are warm at once (the `.ec` index files are on every log
replica), decode queries pay a round trip per cell until the prefetch of the last `cache_warm_h` is done (24 h of
settled data per volume at 200 MB/s: about a minute per 10 GB). Its log replicas are rebuilt elsewhere after
`rebuild_after`.

### 9.4 The object store

The settled tier talks to its backend through the four calls of design-cluster.md §9.2 (put, read range, delete,
list) extended with the geometry; the cluster's own storage servers are the first backend. A second backend is
intellistream-objects (`isod`, S3 API; design-cluster.md "Later") or any S3: tsdb then runs compute servers only,
isod codes, places and rebuilds, and the tsdb cluster's storage role is empty. That costs S3 latency on decode reads
(milliseconds, hidden by the cache for recent data) and one more system to run; it gains a store shared with other
data and independent of tsdb's release cycle. **0.11.0 builds** the roles, the interface and the own backend (R6-R10);
**later** (0.12 or after): the S3/isod backend, then the external cold tier of design-cluster.md §9 as a second
settled tier behind the same interface (older than `cold_after`, moved from the own backend to S3).

## 10. Tests

### 10.1 On one machine

`tests/cluster/` starts N tsdbd processes (22 for 16+4 with two spares), each with `--port 0 --port-file F
--exit-with-parent=$BASHPID`, its own data and state directories under /var/tmp, roles as the test asks; the harness
reads "PORT PID" and checks the pid (AGENTS.md "Ports"). Each geometry (4+2, 8+2, 8+4, 16+4) and `replicas=3` runs the
same suite: insert, settle, query against a single-server oracle (bit-identical answers), kill a position, degraded
reads, OUT and rebuild, back before and after, moves, conversion.

### 10.2 Simulators and unit tests

- `test_rs`: every erasure set of size 0 .. m for every k ≤ 16, m ≤ 4, every kernel byte-equal to scalar (as
  objects:src/codec/rs.h:13-15 does), range decode equal to whole-cell decode; in `tests/test_levels.c` for the CPU
  levels (AGENTS.md "CPU levels").
- `fuzz_chunk` and `fuzz_ec`: chunk files and `.ec` files from any bytes (AGENTS.md "Damage is never silent").
- `sim_place`: a seeded simulator of placement, failures, holds, the mass-OUT guard, rebuild order and moves, with a
  checker: no object below k readable positions while the scenario allows it, margins never lowered by a planned
  move, rebuild order by margin, every list valid (E9), with mutants as `sim_repl` has (design-cluster.md "Corrections
  from step 2's simulator").
- The settle under `test_lww_realtime`-style concurrent small writes into the settling partition, and under crashfs
  power loss at every step of §2.3, with the directory backend (one process).

### 10.3 jepsen-cluster

Nemeses, per geometry: kill, partition, pause, power loss, **disk loss** (a server's directories removed; it comes
back empty, new rids), **slow node** (one storage server's reads delayed past `ec_read_timeout_ms`), **rebuild during
writes**, **node back during rebuild**, **two at once**, **m at once**, **m + 1** (the checker expects a reported
loss and nothing else wrong), **chunk rot** (bytes flipped in chunk files at rest: reads must decode around it, scrub
must repair it), clock jumps. Checker: design-cluster.md §13.2's (no lost acknowledged write, last write wins, no
resurrection) plus: every query answer equals the oracle or is an error that names a partition.

### 10.4 Benchmarks (quiet machine, 5 runs, the mean; AGENTS.md benchmark policy)

Per mode (`replicas=3`, 4+2, 8+2, 8+4, 16+4): `build/ec-bench` (encode, decode, range decode per k, m, cell size, per
kernel; 7950X, 9950X, i9-10850K); insert-bench and tsdbc-bench through the cluster; the 100 M row query suite with a
warm cache, a cold cache, one position down and m down; settle throughput; rebuild time against foreground p99;
network bytes per row. On one machine the numbers compare the modes; absolute numbers need servers with disks.

## 11. Build plan for v0.11.0

Track-days as in design-ha-modes.md §7 (build, review and fix rounds); gates beyond `make test`, `make asan`, `make
tsan`. Prerequisites from that plan, all in 0.11.0 (E23): the rest of S1 (3-4), S2 (2-3), S4 (1-2), R2 (2-4), R3
(3-4), R4 (2-3).

| Step | Content | Gate | Track-days |
|---|---|---|---:|
| R5 | EC codec in `src/ec/` (open question 1), dispatch fitting AGENTS.md "CPU levels" (`tools/check-isa.sh` must allow the AVX-512 and GFNI kernels behind target attributes, or they move into `src/levels.h`), `test_rs`, `build/ec-bench` | numbers on three CPUs | 1-2 |
| R6 | Object layout, chunk files, a local chunk store and the directory backend; range decode; `fuzz_chunk`, `fuzz_ec` | fuzz 1 h each | 2 |
| R7 | Engine: `.ec` parts (the read seam, the cache, merges skip them), the settle as a replace (`MK_COLD` body, `Y`), re-encode, retention drops; X1's cold boundary folded in | `make jepsen` (single server, directory backend: kill, power during settle) | 3-4 |
| R8 | tsdbd: roles, chunk peer messages, epoch fences on stripe positions, degraded and hedged reads, orphan GC, metadata v3 (stripe lists, holds, voters up to 9), TOPOLOGY margins and `TSDB_WHY_LOST` in the four clients | `make jepsen-cluster` (kill, partition, disk loss, chunk rot) on 4+2 and 16+4; `make test-clients` | 3-4 |
| R9 | Failure handling: DOWN/OUT at `rebuild_after`, holds, mass-OUT guard, stripe rebuild by priority and pacing, copy from a returned holder, moves and the balancer, `node drain`; `sim_place` | sim_place soak; jepsen-cluster (rebuild during writes, back during rebuild, two, m, m + 1, slow node) on all four geometries | 3-4 |
| R10 | Redundancy change on a running volume (E2) | jepsen-cluster with a conversion running | 2-3 |
| R11 | Benchmarks (§10.4), operator guide (sizing per mode, holds, salvage), README section, AGENTS.md invariants (the settle, chunk fences) | the numbers | 1-2 |

R5-R11: 15-21 track-days; with the prerequisites about 29-42 track-days, on three tracks roughly 3-4 weeks of
calendar plus gate time (jepsen-cluster on five configurations: about 2 machine-days). R5 and R6 need nothing and can
start now; R7 needs R2; R8 needs R3 and S2.

**What waits**: the S3/isod backend and the external cold tier (§9.4); local repair codes; shipping merges; partial
rebuild of a returning log replica from position ranges (design-cluster.md "Later"); direct-mode clients (S3 of
design-ha-modes.md, forwarding serves until then). If 0.11.0 must shrink, R10 is the step that moves to 0.12 (volumes
then keep the redundancy they were created with).

## 12. What intellistream-objects gives, and what not

| Taken | From | Why |
|---|---|---|
| Systematic Cauchy RS, GF(2^8) 0x11d, six kernels, plans per erasure pattern | objects:src/codec/rs.h:1-16, :63-78; gf256.h:1-24 | Exhaustively tested, interoperable, data readable without decoding |
| A failing survivor is an erasure; trial decode; checks never optional | objects:src/recovery/rebuild.c:16-40 | A wrong input gives a plausible wrong answer |
| Geometry per object, never from today's settings | objects:src/recovery/rebuild.c:26-28 | Makes E2 safe |
| Derived k for small objects | objects:src/codec/geometry.h:8-18 | Small objects cost no more than copies |
| DOWN vs OUT, holds, the mass-OUT guard | objects:src/cluster/cmap.h:67-77, :168, :895-925 | No rebuild storms on a reboot or a rack's power |
| Most endangered first | objects:src/mds/server.h:1852-1860 | Smallest margin first |
| Rebuild pacing: concurrency, byte budget, background class | objects:src/recovery/worker.h:1-21, :56-64 | Foreground first |
| Placement: one per host, rack cap, rack-survival condition, room to rebuild, positions never reused, balancer bounds | objects:src/placement/README:84-99, :134-160, :178-200 | The same problem |

Not taken: objects' segments and manifests (tsdb's part is the segment and the `.ec` file the manifest), its mirror
rule (derived k does it), its placement groups as a separate table (a volume's stripe list is its PG), its
degraded writes (E10), its chunkstore on raw devices (chunk files on a file system suffice for immutable MB-size
objects), its whole-shard decode on reads (E11).

## Open questions (each with a recommendation)

1. **The licence of the EC code.** intellistream-objects is AGPL-3.0-only (objects:LICENSE, LICENSING.md:1-4); tsdb
   is Apache 2.0. The Raft core came under the owner's grant D2 (src/raft/UPSTREAM, docs/licenses.md:16-21), which
   names `src/raft` only. (a) A second grant naming `src/codec/gf256.c`, `gf256.h`, `gf256_avx2.c`,
   `gf256_avx512.c`, `rs.c`, `rs.h` (about 1,100 lines), copied by `tools/sync-ec.sh` with `src/ec/UPSTREAM`, as Raft;
   or (b) our own from the papers (Cauchy RS; the split-table PSHUFB method of Plank, Greenan and Miller, "Screaming
   fast Galois field arithmetic using Intel SIMD instructions", FAST 2013; GFNI's affine instruction), 2-3 more
   track-days with the same tests. **Recommended: (a)**, with the grant written in intellistream-objects too (it has
   no grant file yet).
2. **0.11.0's order against M21** (shared mode first, design-ha-modes.md:85). **Recommended**: the replicated data path
   (S1 rest, S2, S4, R2-R11) is 0.11.0; shared mode's N-steps go on in parallel and ship in 0.11.0 only if their
   maintenance windows came (E23).
3. **The default redundancy of a new cluster.** **Recommended**: `ec=4+2` when `cluster init` sees at least 7
   storage-capable servers, else `replicas=3`; `init` prints which and why, and `--redundancy` overrides.
4. **Voters for m = 4 (E8).** **Recommended**: 2m + 1 = 9 voters when any volume has m = 4, so the cluster stays
   writable through 4 losses as its data survives them; else 5.
5. **Cache and local copies.** **Recommended**: `ec_cache_mb` 10% of RAM, `ec_keep_local_h` 0, prefetch of the last 24 h
   on promotion; raise `ec_keep_local_h` only where measurements (R11) show old data read hard.

## Unverified assumptions

- EU1: GFNI kernels encode k = 16, m = 4 above 8 GB/s a core on Zen 4/5 (R5 measures).
- EU2: A cold-cache decode query costs one round trip per partition touched with pipelining (R11).
- EU3: Young data is about one day of ingest per volume with `settle_grace_s` = 1 h and day partitions (EA2).
- EU4: Chunk files on ext4/XFS are fast enough for storage servers; no raw-device store is needed.
