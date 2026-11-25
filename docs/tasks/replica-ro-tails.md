# A read-only open of a promoted replica's directory: the tail loop after the segment jump

**Done (2026-11-25).**

`follow_refresh` (`src/manifest.c`) previously walked every integer `follow_flushed + 1 .. follow_active`
sequentially and stopped at `MAX_TAILS` (8), allocating a tail slot even for non-existent segment numbers. After a
promotion's `epoch << 40` segment jump, a MANIFEST with `wal_flushed` still below `2^40` and `wal_active >= 2^40`
filled all 8 tail slots with non-existent numbers and never reached the active segment, dropping rows in `2^40` and
above.

## Fix

- `follow_refresh` (`src/manifest.c`) keeps the sequential fast path (without a directory scan) when
  `follow_active - follow_flushed <= MAX_TAILS`, and skips missing segments (`openat` -> `ENOENT`/`ESTALE`) before
  allocating a `wal_tail` slot so missing numbers never consume `MAX_TAILS`. Real `openat` errors (`EIO`, `EACCES`,
  `EMFILE`, etc.) in both `follow_tail_one` and `tail_segment` are logged and returned as `TSDB_ERR_IO`.
- When `follow_active - follow_flushed > MAX_TAILS` (including across a `2^40` epoch jump), `follow_scan_wal` lists
  the directory with `fdopendir(fcntl(db->dirfd, F_DUPFD_CLOEXEC, 0))` + `rewinddir`, collects existing `wal_<seq>.log`
  files (`wal_file_name`) with `follow_flushed < seq <= follow_active` (plus already-open tails in range and
  `follow_active` itself so NFS followers still probe newly created segments by name under `lookupcache=positive`),
  sorts and deduplicates by `seq`, and tails them in ascending order while keeping `db->tails` sorted oldest-first. A
  segment unlinked mid-scan (`ENOENT`/`ESTALE`) is ignored without failing; if more than `MAX_TAILS` actual unflushed
  WAL segments exist on disk, `follow_tail_one` probes `openat`, logs, and returns `TSDB_ERR_IO` instead of silently
  truncating newer segments.

## Tests

- `case_ro_epoch_jump` (`tests/test_replica.c`): covers a direct replica feed (kept flushed segment `1` on disk,
  unflushed segments `2` and `3` below `2^40`, unflushed rows in `2^40` and `2^40 + 1` with `wal_flushed == 1`, fresh
  and stopped read-only opens, live `eng_refresh` across the jump and after flushing past `2^40`), a deterministic
  promotion flow where a follower replica holds 2+ unflushed segments below `2^40` plus promoted leader rows in
  `2^40 + 1`, `> MAX_TAILS` refusal and `EACCES` error propagation, and out-of-order tail insertion (`memmove` sorting
  and overwrite precedence).
- Planted mutants `roseqtails` (old sequential walk) and `roonetail` (stop after one tail once `follow_active >= 2^40`)
  are wired into `Makefile` (`test-units-h3`).
