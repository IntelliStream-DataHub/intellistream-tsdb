# Settled sources and their digest (src/eckeep.c): what R7 left open

**Open (from the r7-keep review, 2026-11-20). Owners: r7-docs, r7-settle, r7-jepsen, R15.**

## tsdbd cannot see a DIVERGED engine (R15, or before tiers reach tsdbd)

A digest that does not match sets `db->diverged`, and the `route()` family answers `TSDB_ERR_UNAVAILABLE` for the
volume. There is no accessor for the flag, so tsdbd's `unav_entry` and `vol_health` (server/tsdbd.c) report
`TSDB_WHY_DIVERGED` with its 60 s retry only for `dom.vol[v].diverged` (a reopen's report). A volume refused by eckeep
answers UNAVAILABLE with reason RECOVERING or NONE and a 1 s retry, clients retry every second for their 30 s budget, and
STATUS and the metrics show the volume AVAILABLE. Wanted: `tsdb_volume_diverged(t, v)` (or the flag in
`tsdb_volume_report` / the health call) and tsdbd saying DIVERGED. In R7 the tier is test-only (`TSDB_TEST_TIER`), so
this bites r7-jepsen's runs of tsdbd first.

## Rot in the kept copy refuses the whole volume (design, E26 / D5)

When only the settled_ file's hash differs (the object's equals the record's), the cause may be rot in the kept copy on
local disk while the `.ec` part is right; the volume is still refused. The settled_ file is a whole part: its footer
hash and granule checksums could tell rot of the copy from a settle that encoded other bytes. Decide whether that case
keeps the file, logs and serves.

## Settles go on on a DIVERGED volume (r7-settle)

Only the route() family checks the flag. The settle thread keeps settling partitions of a volume whose last settle
failed its digest, and each swap renames more sources to settled_ (each held until its own digest matched). Should the
settle policy skip a diverged engine?

## E26 describes a logical digest; R7 built a byte digest (r7-docs)

design-cluster-data.md E26 specifies a logical digest of the retired rows (rows per series and an xxh64 over the
visible rows, `MK_DIGEST`, compared per log replica). R7 compares the object read back whole (every cell checked, a bad
one rebuilt) and the settled_ file with the record's `object_xxh64`. E26 must not read as built: record the difference.

## No fault run reaches eckeep (r7-jepsen)

`make jepsen` runs no tier, so no eckeep code runs under kill, power loss or EIO there, and test_keep has no crashfs
case. The release is a plain unlink (no directory fsync): a file that comes back after a power loss is digested again,
and ecswap's recovery takes a settled_ name as "the swap stands". A tier workload with releases under power loss
belongs in r7-jepsen.

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
