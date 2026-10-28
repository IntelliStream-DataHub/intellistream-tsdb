# Compatibility

What a release of tsdb promises about the files it writes, the protocol it speaks and its programming
interfaces. Two things matter: **the disk format** (the files in a database directory) and **the wire
protocol** (what tsdbd and its clients say to each other). Both have a version number in the code.

| | where | now |
|---|---|---|
| release version | the file `VERSION`, `tsdb.h` (`TSDB_VERSION`), `tsdbd --version`, `tsdb --version` | 0.9.0 |
| disk format version | `FORMAT_VERSION` in `src/internal.h`, in the `META` and `MANIFEST` files | 7 |
| wire protocol version | `TSDB_PROTOCOL_VERSION` in `include/tsdb_protocol.h`, answered to `HELLO` | 10 |

## What 0.9.x promises

0.9.x is the road to 1.0, and the format and the protocol are still being shaped (series addressed by
volume path, for example, changes both). So:

- **A 0.9.x release may change the disk format and the wire protocol.** Each change is in the changelog
  under "Changed", and the format or protocol version goes up by one.
- **Data does not get lost silently.** A release that changes the format says how to carry the data over:
  an export from the old release and an import into the new one (`tsdb <dir> export -o FILE` with the
  old release, `tsdb <newdir> import FILE` with the new; [export-format.md](export-format.md)), or a
  migration tool shipped with the release. Keep the old release's binaries until the data is
  in the new database.
- **A database of another format version is refused, never read as if it were the new one.**
  `tsdb_open` returns `TSDB_ERR_CORRUPT` and writes one line to stderr:

  ```
  tsdb: the database has disk format version 6; tsdb 0.9.0 reads version 7 only. Export the data with the
  release that wrote it and import it here (docs/compatibility.md)
  ```

  `tsdbd` and `tsdb` then exit with an error and have touched nothing. A downgrade is refused the same
  way: a newer release's database is not read by an older release.
- **Use the same release for the server and its clients.** On connect a client sends `HELLO` with the
  protocol version it speaks, and the server answers with the lower of its own and the client's. A client
  never sends what that version does not know. If a client asks for something that needs a newer server
  (the spread needs 8, a limit with an order needs 9, many series in one request 10), the call fails with `TSDB_ERR_INVALID` without
  sending it. A server of an older protocol than the client's, and a client of an older protocol than the
  server's, thus work for the operations both know; operations that came later fail. Across a 0.9.x
  release that changed the protocol we do not promise more than that: upgrade the server and its clients
  together, the server last.
- **The programming interfaces** (`tsdb.h`, `tsdb_client.h`, `tsdb_calendar.h`, the Rust, Python and Java
  clients and bindings) may change in a 0.9.x release too; the changelog lists it. Names and meanings
  that do not need to change stay.
- **Not versioned at all in 0.9.x:** the log lines, the CLI's output text (except the data columns of
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
- **Semantic versioning.** The C, Rust, Python and Java interfaces change incompatibly only in a new major
  version. New functions come in minor versions, fixes in patch versions.
- **Release notes** say which format and protocol versions each release reads and speaks.

The 0.9 to 1.0 road is in [roadmap.md](roadmap.md).
