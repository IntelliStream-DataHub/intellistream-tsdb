# The export stream format

`tsdb <dir> export` writes a database (or some of its series) as one stream, and `tsdb <dir> import` reads it
back. The stream is the way data moves from one tsdb version to another: before 1.0 a 0.9.x release may change
the on-disk format, and a database is then carried over by exporting it with the old `tsdb` and importing it with
the new one. It also serves backups and moving a database to other disks or another host.

This document is the specification. The stream format is stable: **every later tsdb (every 0.9.x and 1.x) reads
every stream an earlier one wrote.** `tests/data/export-v1.tsdbdump`, written by the first release, is imported by
`make test` (`tests/test_export.c`) to keep that promise.

## What a stream holds

What queries see at the moment each series is read, nothing else:

- every series selected (all of them, or those matching `--series` patterns), with its name, value type, volume
  path and retention limit, also when it has no rows;
- its rows in `[--from, --to)` in time order, one per timestamp, values bit for bit (NaN payloads, -0.0, the int64
  extremes, bools), nulls as nulls;
- for state series, the names of the state codes the rows use (codes are per volume, so they differ in the target
  and are mapped through their names).

**Deletes and replaces are already applied**, rows past their series' retention are not there, and of several
writes of one key only the last is. Merges, parts, partitions and granules are not in the stream: the importing
tsdb lays the data out its own way. The stream does not hold the database options (partition length, granule
rows, codecs); the target keeps its own.

Each series is read from one snapshot (one `tsdb_query_values` call), so an export taken while a writer (tsdbd)
keeps writing has every series as it was at one moment; different series may be from different moments.

## Layout

All integers are little-endian. `str32` is a `u32` byte length followed by that many bytes (UTF-8 by convention,
never NUL); `str16` the same with a `u16` length. Checksums are CRC-32C (Castagnoli, the one of iSCSI and ext4:
reflected polynomial 0x82F63B78, initial value and final XOR 0xFFFFFFFF; the check value of "123456789" is
0xE3069283).

```
stream  = header record* trailer-record (end of stream)
header  = magic "TSDBDUMP" (8 bytes)
          u32 stream version          1
          u32 header length L         bytes of the header fields below
          header fields               L bytes
          u32 crc                     CRC-32C of the 16 bytes above and the L bytes of fields
fields  = u32 source format version   the on-disk format of the database exported (FORMAT_VERSION; 0: unknown)
          u32 flags                   0; a reader refuses a stream with a flag it does not know
          i64 created                 ms since the epoch, when the export started
          i64 from, i64 to            the time range exported, [from, to); INT64_MIN, INT64_MAX: everything
          str16 tsdb version          of the exporting tsdb (git describe)
record  = u8  kind
          u8  flags                   bit 0: optional (a reader that does not know the kind may skip it)
          u16 0
          u32 payload length P        at most 64 MiB
          u32 sequence number         0 for the first record, then 1, 2, ...
          u32 crc                     CRC-32C of the 12 bytes above and the P bytes of payload
          payload                     P bytes
```

Records, in this order: for each series a SERIES record, then any number of STATES and CHUNK records, then an END
record; after the last series a TRAILER record, and nothing after it.

| kind | name | payload |
|---:|---|---|
| 1 | SERIES | `str32 name`, `str32 volume path` (empty: the database directory; else the absolute path of the volume), `u8 type` (0 float32, 1 float64, 2 int64, 3 bool, 4 state), `i64 retention` (ms; 0: none) |
| 2 | STATES | `u32 count`, then `count` times `u32 code`, `str32 name`: names of state codes, each before the first CHUNK that uses it; a code is named once per series; the names of one series may be split over several STATES records (tsdb starts a new one beyond 1 MiB) |
| 3 | CHUNK | `u32 rows` (1 to 1,048,576), `u32 raw length`, `u8 compression` (0: none, 1: zstd), then the chunk's raw bytes, compressed as one zstd frame or as they are |
| 4 | END | `u64 rows`, `u64 nulls`, `u32 chunks`: the totals of the series, which the reader checks |
| 5 | TRAILER | `u64 series`, `u64 rows`, `u64 records` (records before the trailer) |

The raw bytes of a chunk:

```
u8 flags                  bit 0: the chunk has nulls (a validity bitmap follows the values)
timestamps                varints (LEB128, 7 bits per byte, least significant first): the first timestamp
                          zigzag-encoded ((t << 1) ^ (t >> 63)), then each one's distance from the one before
                          (> 0: timestamps strictly increase, also from one chunk of a series to the next)
values                    rows x width bytes in byte planes: all rows' byte 0, then all rows' byte 1, ...
                          width and value: float32 4 (IEEE bits), float64 8 (IEEE bits), int64 8, bool 1 (0 or 1),
                          state 4 (the code, named by a STATES record before)
validity                  with flag bit 0 only: (rows + 7) / 8 bytes, bit i (LSB first) set when row i has a value
```

### Limits

What a reader accepts, and so the most a writer may write (tsdb's exporter checks these before it writes a record):

| what | at most |
|---|---|
| header fields (`L`) | 65,536 bytes |
| a record's payload (`P`) | 64 MiB (67,108,864 bytes) |
| a series name, volume path or state name | 65,536 bytes |
| rows in a CHUNK | 1,048,576 (tsdb writes 65,536) |
| a CHUNK's raw length | 1 + rows x (11 + width) bytes: the flags byte, a varint of at most 10 bytes per timestamp, the values and a validity bit per row, rounded up |

A zstd CHUNK is written as one zstd frame whose content size is the raw length. A reader decompresses its data
with zstd's one-shot decoder and checks that it yields exactly the raw length, so concatenated and skippable
frames are accepted too; a writer must not rely on that.

Record flags other than bit 0 are refused, as are header and chunk flags a reader does not know.

### Nulls and reserved values

A null's value bytes are zero. A known value never equals the value tsdb reserves for null (the float32 NaN
0x7fc01001, the float64 NaN 0x7ff8000000001001, INT64_MIN, state code 0); every timestamp lies in `[from, to)`
and below INT64_MAX.

## Reading a stream

A reader refuses, with an error that says why, a stream:

- whose magic is wrong, or whose stream version is 0 or newer than it knows ("import it with a newer tsdb");
- with any checksum that does not match, a record out of sequence, a payload longer than the limits above, a
  truncated record, a missing trailer or bytes after it;
- with a record of a kind it does not know that is not marked optional, header flags or chunk flags it does not
  know, or a compression it does not know;
- whose fields contradict each other: counts in END or TRAILER that differ from the records, timestamps out of
  order or range, a state code without a name, a bool other than 0 or 1, a value reserved for null.

How `tsdb import` writes: from a file it reads the stream twice, once to verify all of it and once to write, so a
damaged file writes nothing ("nothing was written"). From a pipe it cannot look ahead: it writes each batch (up
to `--batch-rows`, 1 M rows by default) atomically after checking it; when the stream turns out damaged, into an
existing database the series being imported is emptied again if the import created it, the error names it, and
the series before it are whole; a new database is not created at all. It exits with status 1 in every such case.

Into an existing database `import` keeps every sync of the engine: a crash or failure midway leaves the series
before it whole, and the same import rerun with `--merge` writes the same rows again. Into a new database (a
directory that is missing or empty) it builds the database in a sibling directory, `DIR.importing-PID`, writes
without syncing file by file, syncs every file system the database lives on (that directory and every volume)
once at the end, which is much faster for many small series, and then renames the sibling to `DIR` and syncs
the parent directory. It reports success and exits 0 only after that; a failed sync is an error (exit 1), after
which the sibling is removed, and so is what the import wrote in the volumes it added. There it removes only files
it provably wrote: regular files with the names tsdb writes in a volume (`LOCK`, `MANIFEST`, `META`, `ROOT_ID`,
`SERIES`, `STATES`, `RETENTION`, parts `p<partition>_<min>_<max>_<level>.part`, `wal_<16 hex digits>.log`, its own
`tmp_`/`txn_` files, `retired_` parts) that were not in the directory before the import. Anything else stays, also
what another process put there during the import (the message names such entries: "not written by tsdb: left as it
is"). A volume directory the import made is removed when nothing else is left in it; one that was there before
always stays. If `DIR` is a symlink to an empty directory, or a dangling one, the database is built at the end of the
link, which keeps pointing at it. Whatever stops it (an error, a crash, a power loss, a kill), there is no database
named `DIR` until it is complete. Volumes are named by absolute path in the root's `VOLUMES` and know the
database by its ID (`ROOT_ID`), so the rename does not detach them. A sibling left by a kill or power loss
makes the next import into `DIR` stop with a message naming it (and the volumes it lists): delete it, remove the
tsdb files it wrote in those volumes (the directory too, if that import made it; one that was there before may
hold other files of yours), and import again. An empty directory is replaced by the rename, unless it
is a mount point or its parent is not writable: then the import writes into it with every sync, as into an
existing database.

A stream on an interactive terminal is recognised only after 16 bytes or the end of input.

Series that exist in the target are refused unless `--skip-existing` (left as they are), `--merge` (the rows are
written into them; the last write wins, their other rows stay) or `--replace` (their values in the exported range
are deleted first, and their retention set to the stream's). A series of another type is always refused. A series
of a volume is created in the same path, which must be a volume of the target (`tsdb DIR volume add PATH`), unless
`--volume OLD=NEW` moves it (`--volume OLD=` into the database directory) or `--create-volumes` adds it.

## Compatibility rules

For whoever changes the format:

- A later tsdb keeps reading every stream version it ever wrote. Stream version 1 is the one of 0.9.0.
- Within one stream version, a writer may append fields at the end of the header fields or of a record's payload
  only when an older reader may ignore them; readers ignore bytes after the fields they know there (the header's
  length says where the fields end; a record's payload length where it ends). Version 1 readers check that
  a STATES payload ends where its last name ends and take the rest of a CHUNK payload as the chunk's data, so new
  fields never go into those two.
- New information an older reader may safely skip goes into a new record kind with the optional flag set.
- Anything an older reader must not misread (new value types, new compressions, a new layout of the raw chunk)
  gets a new record kind, a new type code, a new compression code or a new chunk flag (all of which older readers
  refuse), or bumps the stream version.
