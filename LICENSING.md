# Licensing

tsdb is dual licensed, and its parts have two different open source licences.

## The server and the engine: AGPL-3.0-only

The database engine (`src/`, the `libtsdb` library), the server (`tsdbd`), the command line tools (`tools/`, the
`tsdb` CLI), the embedded bindings (Rust `intellistream-tsdb-embedded`, Python `intellistream-tsdb-embedded`, Java
`tsdb-embedded`), and the tests and benchmarks (`tests/`, `bench/`) are free software under the GNU Affero General
Public License, version 3 only (SPDX: `AGPL-3.0-only`). The full text is in [`LICENSE`](LICENSE).

In short: you may use, study, change and share it. If you run a modified version as a network service, or give a
modified or unmodified copy to others, the AGPL asks you to offer the source of that version (including your changes)
to its users under the same licence. A program that links or embeds `libtsdb` (it uses the engine inside its own
process, through the C API, or through any of the embedded bindings) is a work based on the engine, so the AGPL
applies to the whole program.

A commercial (enterprise) licence is also available, for anyone who wants to use the software on terms other than the
AGPL's: **contact: <to be filled by the owner>** (IntelliStream AS).

## The client libraries: Apache-2.0

Programs that only talk to a running `tsdbd` over the network are not covered by the AGPL. The client libraries are
licensed under the Apache License, Version 2.0 (SPDX: `Apache-2.0`, text in
[`LICENSES/Apache-2.0.txt`](LICENSES/Apache-2.0.txt)), so using them puts no obligation on your program:

| what | where |
|---|---|
| the C client | `client/tsdb_client.{c,h}`, `client/tsdb_crypto.{c,h}`, `client/tsdb_calendar.c`, `client/tsdb_pack.{c,h}` |
| the headers the C client needs | `include/tsdb.h`, `include/tsdb_protocol.h`, `include/tsdb_wire.h`, `include/tsdb_calendar.h` |
| the Rust network client | `client/rust` (`intellistream-tsdb-client`) |
| the Python network client | `client/python` (`intellistream-tsdb-client`, module `tsdb_client`) and `client/python-common` |
| the Java client | `client/java` (`tsdb-client`) |
| the Apache Pulsar sink | `connectors/pulsar` |
| the C client example | `examples/client.c` |

Apache-2.0 code may be part of an AGPL work, and it is: `libtsdb` contains `client/tsdb_calendar.c`, and tsdbd uses
`client/tsdb_crypto.c` and the protocol headers. The other way round is not allowed, so no Apache-2.0 file includes,
imports or depends on an AGPL-3.0-only one (`tools/check-spdx.sh` checks it).

**A header's licence does not change the terms of `libtsdb`.** `include/tsdb.h` is Apache-2.0 so that the C client
can use the types it declares, but the library it declares, `libtsdb.a` and `libtsdb.so`, is AGPL-3.0-only (the
tarball of `make dist` has both side by side). Writing a program against `tsdb.h` and linking `libtsdb` makes that
program an AGPL work; linking `libtsdbclient.a` (the C client) and talking to tsdbd does not.

Which licence a file has is in its `SPDX-License-Identifier` line (the first line of the file, or the second after
a `#!` line), and in the `license` field of its manifest.

## What changes for you, and what does not

The releases 0.9.0, 0.9.1 and 0.10.0 were published under Apache-2.0 and stay so: the code of those releases may be
used under the Apache License forever. The AGPL applies to the work from 0.11.0 on (and to later changes, which are
not available under Apache-2.0).

## Contributing

Contributions need the DCO sign-off and the licence grant of [`CONTRIBUTING.md`](CONTRIBUTING.md), which lets IntelliStream
AS offer the contribution under other terms too: that is what keeps the dual licence possible.

## Third-party code

The libraries tsdb links keep their own licences (zstd BSD/GPLv2, liburing MIT/LGPL, the Rust and Java
dependencies): [`docs/licenses.md`](docs/licenses.md) lists each one, and the packages carry their notices
(`THIRD-PARTY-NOTICES`).
