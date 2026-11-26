# Settled sources and their digest (src/eckeep.c): what R7 left open

**Mostly closed in R8-1 (2026-11-26; see [AGENTS.md](../../AGENTS.md) "Settled parts", [docs/design-cluster-data.md](../design-cluster-data.md) §2.3, [docs/tasks/r8-from-r7.md](r8-from-r7.md)).**

## tsdbd cannot see a DIVERGED engine — Closed in R8-1 (`r8i-diverged`)

`src/ecdiverge.[ch]` makes `DIVERGED` durable (48-byte checksummed append-only records, synced before `db->diverged` is
visible, read at every open). `tsdb_volume_diverged` (`src/cvol.h`, `src/volumes.c`) exposes it to `tsdbd`: one
reporting path `vol_diverged()` (`server/tsdbd.c`) feeds `STATUS`, `UNAVAILABLE` (`TSDB_WHY_DIVERGED`, 60 s retry),
`TOPOLOGY`, and `tsdb_volume_diverged` metric for all volumes including root, while `server/repld.c` stops a diverged
replicated volume (`vol_fail`) and `server/cvol.c` reports `CL_HEALTH_LOCAL_FAULT`. `tsdb DIR volume diverged [--clear]`
inspects/clears it (refused on cluster volumes per D23; use `tsdb cluster replica reset` to rebuild the replica instead).

## Rot in the kept copy refuses the whole volume — Closed in R8-1 (`r8j-digest`, D25)

In `src/eckeep.c`, single-server and leader byte-digest checks (`digest_part`) trigger D25 rot when the chunk object
matches its record while the kept `settled_` copy fails its own granule checksums, whereas replica `MK_DIGEST` jobs
(`ecdigest_job_check`) verify the retired copies' checksums against `MK_DIGEST` without re-reading the chunk object
(corrupt or missing copies without `ECDJ_MATCHED` -> `ECKEEP_ROT`). In both paths the rotted copy is counted lost
(`eckeep_copies_lost++`), logged, released at its deadline, and the `.ec` part keeps serving; only a digest mismatch
(or unreadable rows) whose checksums hold sets `DIVERGED`.

## Settles go on on a DIVERGED volume — Closed for clusters in R8-1 (`r8g-cut`, `src/ecpolicy.c:223-224`)

The cluster settle policy skips diverged engines (`if (cluster && atomic_load(&db->diverged)) return 0;`). Note: the
single-server R7 branch still leaves `db->diverged` unchecked by `ecpolicy_pick_locked` until R15 adds the reverse
intent (`settled_` back).

## E26 describes a logical digest; R7 built a byte digest — Closed for clusters in R8-1 (`r8j-digest`)

`src/ecdigest.[ch]` implements E26's logical digest (`ec_logical_digest`, `MK_DIGEST` with `now_ms`, `MKD_TOMBS`,
canonical tombstone union, and `digest_<key>_<z>` job files on replicas). Single-server tiers and leader `settled_`
copies keep R7's byte digest.

## No fault run reaches eckeep — Closed in R8-1 (`r8j-digest`, `r8l-jc`, `r8q-geom`)

`test_keep` (`relpower` under crashfs), `test_ecapply` (`digestpower` under crashfs), and `jepsen-cluster --ec` (`rot`
and `power` nemeses) now exercise digest matching, copy rot, and durable release unlinks under power loss.

## Not covered by test_keep

- A store that goes away in the middle of a digest. In R6's backend a store leaves UP only at the open (attach and
  replace move a DOWN one to REPLACING, then UP), so nothing in a test can take one away between two reads; the
  per-read `CHUNK_ST_DOWN` check and the check after the last read are for when that exists (each alone is redundant
  with the other today).
- The diverged check in `each()` (tsdb_flush and the other calls over every volume: the volume is skipped), in
  `feed_engines` and in `cvol_series_make`, and the carry of the flag in `tsdb_cvol_replace` (cluster volumes have no
  tier yet). route_now, both paths of `insert_slots`, `series_make` and `tsdb_volume_reopen` are tested.

## The cost of a digest (no numbers yet)

Every open digests every settled_ file again: with `settle_keep_s` = 1 day and T bytes settled a day, a restart or an
in-process reopen reads 2T (the object and the file) at `eckeep_digest_mb_s` (64 MB/s, per engine: n tiered volumes
read up to n times that). Measure the digest of a 1 GB object (8+2, a 1 MiB cell and a derived 132 KiB one) for
throughput and CPU, and the time from open to the first release.
