# Compatibility

What a release of tsdb promises about the files it writes, the protocol it speaks and its programming
interfaces. Two things matter: **the disk format** (the files in a database directory) and **the wire
protocol** (what tsdbd and its clients say to each other). Both have a version number in the code.

| | where | now |
|---|---|---|
| release version | the file `VERSION`, `tsdb.h` (`TSDB_VERSION`), `tsdbd --version`, `tsdb --version` | 0.10.0 |
| disk format version | `FORMAT_VERSION` in `src/internal.h`, in the `META` and `MANIFEST` files | 9 (0.10.0; 0.9.0 and 0.9.1 wrote 8) |
| wire protocol version | `TSDB_PROTOCOL_VERSION` in `include/tsdb_protocol.h`, answered to `HELLO` | 14 |
| oldest protocol spoken (the floor) | `TSDB_PROTOCOL_MIN` in `include/tsdb_protocol.h` | 12 |
| embedded C ABI version | `TSDB_ABI_VERSION` in `include/tsdb.h`, `tsdb_abi_version()` | 7 |

**What each release reads and speaks:**

| release | reads disk format | wire protocol (floor) | embedded C ABI | clients it talks to |
|---|---|---|---|---|
| 0.10.0 | 9 (a format-8 database after `tsdb <dir> upgrade`, below) | 14 (12) | 7 | clients and servers of protocol 12, 13 and 14 |
| 0.9.1 | 8 | 12 (12) | 3 | protocol 12 only |
| 0.9.0 | 8 | 12 (12) | 3 | protocol 12 only |

0.10.0 changed the disk format (8 to 9), the wire protocol (12 to 14: 13 adds the HA reply bodies of docs/ha.md section
15.4, 14 adds QUERY_LATEST) and the C ABI (3 to 7: the statistics, the latest table and the tenants added fields to
`tsdb_options` and `tsdb_volume_stats`, and the functions and structs of the change feed, the statistics and the latest
table). Its floor stays 12: a client or server of 0.9.x speaks to it, with these limits. A client of 12 gets the bare
status codes it knew (the HA bodies only go to a client whose HELLO said 13), and a call that needs a newer server than
the one connected (`latest` needs 14) fails with `TSDB_ERR_INVALID` without sending. A database of 0.9.x is refused by
0.10.0 until it is upgraded, and a 0.10.0 database is refused by 0.9.x. A binary built for ABI 3 (an embedded
program) must be rebuilt: the Rust and Java bindings check `tsdb_abi_version()` against the one they were written for and refuse a mismatch
(Python goes through the Rust crate). Use the same release on the server and its clients all the same.

Preview and off by default: the cluster (`tsdbd --cluster`, `tsdb cluster ...`) is the metadata group only, with no
replication and no failover; its snapshot (version 2) and peer protocol (version 2) may change again before 1.0, and
two builds of the preview that differ there do not talk. The change feed is off until `tsdb <dir> feed enable`; with it
on, `META` says `requires=feed` and a binary without the feed refuses the directory.

0.9.1 changed neither the disk format nor the protocol, and the C ABI stayed 3: it only added functions and structs
(`tsdb_latest`, the volume and health calls) and the status codes `TSDB_ERR_UNAVAILABLE` (-11) and
`TSDB_ERR_ABANDONED` (-14). A database of 0.9.0 opens in 0.9.1.

## Disk format versions

| version | what changed |
|---|---|
| 9 | the WAL record has a 32-byte header with a log number (`fno`) and the group commit's time, and can be a marker (`docs/design-cdc.md` 2.1); the part header is 48 bytes (the range of log numbers its rows had, and the time they were written); each volume has an `FNO` file, the durable bound of the log numbers; `META` may say `requires=` (a feature this binary does not know is refused, and named) and, in an upgraded database, `parts_v8=1`; `MANIFEST` has an `fno_next` line (for followers) |
| 8 | `CATALOG` is gone: a series id is `(volume + 1) << 32 \| local`, so it names its volume, and each volume's `META` holds its number (`volume=N`, checked at open) |
| 7 | part keys `4 x first base partition + class` and the `CLASSES` file (partition classes); zstd frames without magic number and content size; the granule index bit-packed per part (about 32 bytes an entry instead of 160) behind a 64-byte footer that gives the index's length (`index_len`) |
| 6 | block headers name their codec in 7 bits; the dictionary codec |
| 5 | the spread in every granule entry (160 bytes) |

Each version is refused by every release that reads another one; carry data over with export and import. The one
exception is the step from 8 to 9, which `tsdb <dir> upgrade` does in place:

- Close the database cleanly with 0.9.0 or 0.9.1 (stop tsdbd; no `wal_*` segment may be left, nor an intent or a `tmp_*`
  part; the upgrade refuses and says which), **stop every follower on other hosts** (they cannot be seen from here, and
  a 0.9.x follower that reads a MANIFEST of version 9 reports damage), then run `tsdb <dir> upgrade` with the new
  release. It takes the locks of every volume, writes `FNO`, MANIFEST and `META` of each volume and of the root
  last, and can simply be run again if it died. Keep the export of the old release until the new one has run for a
  while: 0.9.0 and 0.9.1 refuse the upgraded database (they name the version), and a way back is the export. **Do not run
  0.9.x's `tsdb` on an upgraded database**, `volume add` above all: that release adds a volume without an `FNO` file.
  (This release makes the file at the volume's first open when the volume never held a record, and its own `volume add`
  refuses a root of another format or with an unknown `requires=`.)
- The parts are not rewritten: they keep their 24-byte headers, `META` says `parts_v8=1`, and the open reads such a
  part (only where `META` says so, never in a database created at 9). Merges of idle partitions, and `tsdb <dir>
  optimize` at once, rewrite them one by one; the open logs how many are left, and the line leaves `META` when none is.
- **Before 1.0 that allowance is removed** (`docs/tasks/pre-1.0-v8-parts.md`): run 0.9.x until the count is zero
  (or `optimize`), or export and import. A 1.0 that finds a format-8 part refuses it and says so.

## What 0.x promises

0.x (0.9.x, 0.10.x, ...) is the road to 1.0, and the format and the protocol are still being shaped (series addressed by
volume path, for example, changes both). So:

- **A 0.x release may change the disk format and the wire protocol.** Each change is in the changelog
  under "Changed", and the format or protocol version goes up by one.
- **Data does not get lost silently.** A release that changes the format says how to carry the data over:
  an export from the old release and an import into the new one (`tsdb <dir> export -o FILE` with the
  old release, `tsdb <newdir> import FILE` with the new; [export-format.md](export-format.md)), or a
  migration tool shipped with the release. Keep the old release's binaries until the data is
  in the new database.
- **A database of another format version is refused, never read as if it were the new one.**
  `tsdb_open` returns `TSDB_ERR_CORRUPT` and writes one line to stderr:

  ```
  tsdb: the database has disk format version 7; tsdb 0.10.0 reads version 9 only. Export the data with the
  release that wrote it and import it here (docs/compatibility.md)
  ```

  (For a format-8 database the line says to close it cleanly with the release that wrote it and run `tsdb DIR upgrade`,
  or to export and import.)

  `tsdbd` and `tsdb` then exit with an error and have touched nothing. A downgrade is refused the same
  way: a newer release's database is not read by an older release.
- **Use the same release for the server and its clients.** On connect a client sends `HELLO` with the
  protocol version it speaks, and the server answers with the lower of its own and the client's. Each 0.x
  release names the oldest protocol it speaks (the floor, 12 now): the server refuses a client below it,
  and a client refuses a server below it, with `TSDB_ERR_INVALID` at connect. The floor moved to 11 because
  every series id on the wire became 64 bits (a version-10 peer would read a `u32` where a `u64` is sent, so
  there is no operation both could do safely), and to 12 because every connection now logs in (a version-11
  peer has no login messages and no rights to check). Above the floor, a client
  never sends what the server's version does not know: if it asks for something that needs a newer server,
  the call fails with `TSDB_ERR_INVALID` without sending it. Across a 0.x release that changed the
  protocol we do not promise more than that: upgrade the server and its clients together, the server last.
  From 1.0 on the floor is 1.0's version and stays.
- **The programming interfaces** (`tsdb.h`, `tsdb_client.h`, `tsdb_calendar.h`, the Rust, Python and Java
  clients and bindings) may change in a 0.x release too; the changelog lists it. Names and meanings
  that do not need to change stay.
- **Not versioned at all in 0.x:** the log lines, the CLI's output text (except the data columns of
  `query`) and the benchmark tools.

## What 1.0 will promise

At 1.0 the format and the protocol freeze. From then on:

- **A 1.x release reads the databases of every earlier 1.x release** (and of 1.0), and keeps them
  readable. A change that needs a new format is either backwards compatible (new files or records that old
  data does not need, as the value codecs already work: a block names its codec) or waits for 2.0. A database
  that an older release cannot read is refused with a message, as above, never read wrongly.
- **A 1.x server speaks every protocol version of 1.0 and later**, and a client of any 1.x release works
  against any other 1.x server for the operations both know. New operations get a new protocol version
  number and fail with `TSDB_ERR_INVALID` against an older server.
- **New grant selector kinds and rights bits may appear** in the users and grants a later server reports
  (`WHOAMI`, `USER_LIST`). Clients must accept them: every 1.x client keeps a grant of a kind it does not know
  (Rust `Selector::Other`, Java `Selector.Kind.UNKNOWN`, shown as `kind#N:TEXT`; the C client keeps the kind
  byte) and shows rights bits it cannot name as `0xHH`, which every parser reads back. Such a grant cannot be
  granted or revoked from a client that does not know its kind.
- **Semantic versioning.** The C, Rust, Python and Java interfaces change incompatibly only in a new major
  version. New functions come in minor versions, fixes in patch versions.
- **Release notes** say which format and protocol versions each release reads and speaks.

The 0.9 to 1.0 road is in [roadmap.md](roadmap.md).
