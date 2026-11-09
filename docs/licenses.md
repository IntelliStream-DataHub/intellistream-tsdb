# Licenses of what tsdb uses

tsdb has two licences (see [LICENSING.md](../LICENSING.md)): the server, the engine, the tools and the embedded
bindings are under the [GNU AGPL, version 3 only](../LICENSE); the client libraries are under the
[Apache License 2.0](../LICENSES/Apache-2.0.txt) (copyright IntelliStream AS, see [NOTICE](../NOTICE)). IntelliStream AS
also offers a commercial licence. This page lists the software and data that tsdb builds on, under which license each
comes, and how it is used. It answers three questions: *can these licenses live together with the AGPL (and, for the
clients, with Apache 2.0)?*, *can Apache 2.0 code sit inside an AGPL work?* and *what has to ship with a release?*
The audit was made on 2026-11-02 for version 0.9.0 and redone on 2026-11-09 for the licence change of 0.11.0 (the
dependencies were not changed by it, the licences they are combined with were). Redo it when a dependency is added or
a version changes (the Rust lockfiles and the poms are the source of truth).

**In short.** Everything is compatible. Nothing is GPL or LGPL in a way that reaches tsdb's own code: the licences of
all dependencies (MIT, BSD, Apache-2.0, the GCC Runtime Library Exception, LGPL by dynamic linking) can be combined
with the AGPL-3.0, and those of the clients' dependencies with Apache 2.0. Apache-2.0 code may be part of an AGPL
work (the FSF lists Apache-2.0 as compatible with GPLv3, and the AGPL-3.0 is GPLv3 with its network clause added; the
result as a whole is under the AGPL), so `libtsdb` and `tsdbd` contain the Apache-2.0 files they share with the
clients (see the table below); the other way round is not allowed, and `tools/check-spdx.sh` fails when an Apache-2.0
file includes, imports or depends on an AGPL-3.0-only one. Two libraries are linked statically into the tarball's
binaries (zstd and liburing); their licenses (BSD and MIT) ask that their notice travels with the binaries. The
tarball carries it: [dist/THIRD-PARTY-NOTICES](../dist/THIRD-PARTY-NOTICES), plus `LICENSE`, `LICENSES/Apache-2.0.txt`,
`LICENSING.md` and `NOTICE`.

## Apache-2.0 code inside the AGPL work

Programs that only talk to tsdbd over the network must not fall under the AGPL, so the client libraries stay
Apache-2.0, together with the few files they share with the engine. These files are part of the AGPL works too:

| file | Apache-2.0 because | where the AGPL work uses it |
|---|---|---|
| `include/tsdb.h`, `tsdb_protocol.h`, `tsdb_wire.h`, `tsdb_calendar.h` | the C client includes every one of them | the engine and the server include them; the dist tarball puts `tsdb.h` beside the AGPL `libtsdb`. A header's licence does not change the terms of the library it declares. |
| `client/tsdb_calendar.c` | the C client's local-time bounds | compiled into `libtsdb.a` and `libtsdb.so` (the engine's `tsdb_query_group_local` uses it) |
| `client/tsdb_crypto.{c,h}` | SCRAM-SHA-256 in the C client | `server/auth.c`, `users.c`, `cluster.c` and the CLI |
| `client/tsdb_pack.{c,h}`, `client/tsdb_client.{c,h}` | the C client | `tsdbd` (the packed-insert code in `tsdb_pack`), and the tests that drive a server |

Apache 2.0 section 4 asks that a recipient of Apache-licensed files gets the licence text: the packages that contain
them carry `LICENSES/Apache-2.0.txt` (the tarball, `client/rust-sys` and the crate, wheel and jar of the embedded
bindings), and the files keep their SPDX line and the NOTICE.

## Copied code and data

One piece of source code is copied from another project: the Raft core of the cluster (2026-11-06). The rest of this
table is close to copying without being it.

| what | where | origin and license | note |
|---|---|---|---|
| Raft core: `raft.c`, `raft.h`, `raft_log.c`, `raft_log.h`, `raft_wire.c`, `raft_wire.h`, `raft_sim.c`, `raft_sim.h` | `src/raft/` | intellistream-objects `src/raft`, IntelliStream AS, at the commit in `src/raft/UPSTREAM`; **AGPL-3.0-only**, the licence of both repositories since 2026-11-09. (Until then tsdb carried the copy under Apache 2.0 by its owner's grant of 2026-11-06, [design-cluster.md](design-cluster.md) D2; the releases 0.9.x and 0.10.0 still do.) | copied unchanged below an AGPL-3.0-only header by `tools/sync-raft.sh`, which also writes `UPSTREAM` (repository, commit, licence). `compat.h`, `compat.c` and the headers under `compat/` are tsdb's own: what the files include from isod. Linked into tsdbd, the CLI and the tests, not into libtsdb. Same owner, same licence: no grant is needed any more. |
| xxHash64 checksum | `src/codec.c` (`xxh64`) | the algorithm and constants of xxHash by Yann Collet (BSD-2-Clause) | written for tsdb from the published algorithm; no xxHash file is included. The credit is here. |
| LTTB downsampling | `src/query.c`, `tests/lttb_ref.h` | the algorithm of Sveinn Steinarsson's thesis (2013) | an algorithm is not covered by copyright; the code is our own. |
| Gorilla, Chimp, Patas, ALP codecs | `src/codec*.c` | published papers | own code. The benchmarks compare with these codecs; their reference code is not included. |
| time zone fixtures | `client/rust/tests/fixtures/zoneinfo/` | the IANA time zone database (tzdata 2026e), public domain | test data only; copied from a system's `zoneinfo`. |
| real pressure readings | `tests/hydraulic_psi.h` | UCI "Condition monitoring of hydraulic systems" data set, CC BY 4.0 (N. Helwig, E. Pignanelli, A. Schuetze, ZeMA gGmbH) | two granules of values, used by tests. The credit is in the file header. |
| the hydraulic data set | `bench/hydraulic/` | same data set, CC BY 4.0 | **not in the repository**: `get.sh` downloads it from the UCI repository when you run it. The README names the authors. |

Test data written by tsdb's own tools (`tests/data/export-v1.tsdbdump`, `tests/fuzz/corpus`, the generated sensor
data of `tools/datasets.h`) is ours.

## C: the library, the server and the command line tool

| name | version (audit) | license | how it is used |
|---|---|---|---|
| zstd (libzstd) | 1.5.7 | BSD-3-Clause (the Zstandard license is a "BSD License"; it is also offered under GPL-2.0, tsdb uses the BSD terms) | `make`: linked from the system, dynamically. `make dist`: **linked statically** into `tsdbd`, `tsdb`, the benchmarks and the tests (`ZSTD_LIB=-l:libzstd.a`). |
| liburing | 2.14 | MIT (dual licensed LGPL-2.1 or MIT; tsdb uses the MIT terms) | the server only. `make`: dynamically. `make dist`: **linked statically** (`URING_LIB=-l:liburing.a`). Not used with `NO_URING=1`. |
| libgcc | the compiler's | GPL-3.0 with the GCC Runtime Library Exception | `make dist` links it statically (`-static-libgcc`). The exception allows this for programs under any license. |
| glibc | 2.34 or newer | LGPL-2.1-or-later | linked dynamically, taken from the machine the program runs on; not shipped. |
| Linux kernel headers (`io_uring.h` via liburing) | | GPL-2.0 with the Linux-syscall-note, or MIT | the syscall note allows user programs under any license. |

`libtsdb.a` (AGPL-3.0-only, with the Apache-2.0 `tsdb_calendar.o` inside) and `libtsdbclient.a` (the C client,
Apache-2.0) in the tarball contain tsdb code only. A program that links them also links its own libzstd, so it
carries zstd's notice itself. License combinations: zstd is taken under its BSD terms (it is also offered under
GPL-2.0, which would not combine with the AGPL-3.0; the BSD terms do), liburing under MIT, libgcc under the Runtime
Library Exception and glibc by dynamic linking, so each can be linked into AGPL and Apache-2.0 programs alike.

**What ships with the tarball** (`make dist`, `tools/mkdist.sh`): `LICENSE` (the AGPL-3.0), `LICENSES/Apache-2.0.txt`,
`LICENSING.md`, `NOTICE` and `THIRD-PARTY-NOTICES` (the texts of the zstd and liburing licenses and the libgcc remark)
at its top level. `tests/distro/dist-check.sh` checks that they are there.

## Rust: `client/rust`, `client/rust-sys`, `client/rust-embedded`, `client/python`, `client/python-embedded`, `tests/integration/rust`

From `cargo metadata` on the lockfiles (the union is below; `tsdb-client` (Apache-2.0) needs the first block,
`tsdb-sys` (AGPL-3.0-only) only zstd-sys and the build tools, `tsdb-embedded` (AGPL-3.0-only) both, the Python modules
add the PyO3 block, the integration tests only `tsdb-client`'s). The dependencies did not change with the split into
the network client and the embedded crate. None is a dev-dependency of the crates; the crates are `publish = false`
for now. All are MIT, BSD or Apache-2.0 (or an OR of them): fine to link into an Apache-2.0 client and into an AGPL
work.

| name | version | license | how it is used |
|---|---|---|---|
| zstd | 0.13.3 | MIT | linked statically into the client |
| zstd-safe | 7.3.0 | BSD-3-Clause | linked statically |
| zstd-sys | 2.1.0 (zstd 1.5.7) | BSD-3-Clause | the zstd C library compiled in and linked statically (`bundled-zstd`, the default) |
| rayon, rayon-core | 1.12.0, 1.13.0 | MIT OR Apache-2.0 | linked statically (parallel decoding) |
| crossbeam-deque, -epoch, -utils | 0.8.8, 0.9.21, 0.8.23 | MIT OR Apache-2.0 | linked statically (through rayon) |
| either | 1.19.0 | MIT OR Apache-2.0 | linked statically (through rayon) |
| libc | 0.2.189 | MIT OR Apache-2.0 | linked statically |
| sha2, hmac | 0.10.9, 0.12.1 | MIT OR Apache-2.0 | linked statically (the login, SCRAM-SHA-256) |
| digest, crypto-common, block-buffer, cpufeatures | 0.10.7, 0.1.7, 0.10.4, 0.2.17 | MIT OR Apache-2.0 | linked statically (through sha2 and hmac) |
| generic-array | 0.14.7 | MIT | linked statically (through digest) |
| typenum | 1.20.1 | MIT OR Apache-2.0 | linked statically (through generic-array) |
| subtle | 2.6.1 | BSD-3-Clause | linked statically (through digest) |
| version_check | 0.9.5 | MIT/Apache-2.0 | build only (through generic-array) |
| cc | 1.6.0 | MIT OR Apache-2.0 | build only |
| find-msvc-tools, jobserver, shlex, pkg-config, cfg-if | 0.1.14, 0.1.35, 2.0.1, 0.3.34, 1.0.5 | MIT OR Apache-2.0 | build only (through cc) |
| getrandom | 0.4.3 | MIT OR Apache-2.0 | build only (through jobserver) |
| r-efi | 6.0.0 | MIT OR Apache-2.0 OR LGPL-2.1-or-later | build only, and only on UEFI targets; we take the MIT or Apache terms |

The Python modules (`client/python`, Apache-2.0, and `client/python-embedded`, AGPL-3.0-only; PyO3) add:

| name | version | license | how it is used |
|---|---|---|---|
| pyo3, pyo3-ffi | 0.29.3 | MIT OR Apache-2.0 | linked statically into the extension module |
| numpy (the Rust crate) | 0.29.0 | BSD-2-Clause | linked statically |
| ndarray | 0.17.2 | MIT OR Apache-2.0 | linked statically (through numpy) |
| matrixmultiply, rawpointer | 0.3.11, 0.2.1 | MIT/Apache-2.0 | linked statically (through ndarray) |
| num-complex, num-integer, num-traits | 0.4.6, 0.1.47, 0.2.19 | MIT OR Apache-2.0 | linked statically |
| once_cell, portable-atomic, portable-atomic-util, rustc-hash | 1.21.4, 1.15.0, 0.2.8, 2.1.3 | MIT OR Apache-2.0 | linked statically |
| pyo3-macros, pyo3-macros-backend, proc-macro2, quote, syn, heck | 0.29.3, 1.0.107, 1.0.47, 2.0.119, 0.5.0 | MIT OR Apache-2.0 | build only (procedural macros run in the compiler) |
| unicode-ident | 1.0.27 | (MIT OR Apache-2.0) AND Unicode-3.0 | build only (through syn; its Unicode data stays in the compiler) |
| pyo3-build-config, autocfg | 0.29.3, 1.5.1 | MIT OR Apache-2.0 | build only |
| target-lexicon | 0.13.5 | Apache-2.0 WITH LLVM-exception | build only |

Every license above is MIT, BSD, Apache-2.0 or an "OR" that offers one of them, apart from the Unicode data
license, which applies to compiler-side code only.

**What ships.** The two Python wheels and sdists (`client/python`: Apache-2.0 `LICENSE`; `client/python-embedded`:
AGPL-3.0 `LICENSE` plus `LICENSES/Apache-2.0.txt`) carry `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES`
(maturin `license-files`; in the wheel under `.dist-info/licenses/`). `THIRD-PARTY-NOTICES` has the license texts of
the crates that are linked into the extension module (normal dependencies, not build tools and not procedural
macros, which run in the compiler) and of the zstd C library inside zstd-sys, where a crate that offers MIT OR
Apache-2.0 is taken under MIT. It is generated from `client/python/Cargo.lock` and
`client/python-embedded/Cargo.lock` by `tools/gen-third-party.py` (`--check` fails when a lockfile moved; no
`cargo about` needed): rerun it when a dependency changes. The three crates (`client/rust`: Apache-2.0;
`client/rust-sys` and `client/rust-embedded`: AGPL-3.0-only) hold copies of their `LICENSE` and `NOTICE` in their
directories, so `cargo package --list` shows them; the AGPL crates also hold `LICENSES/Apache-2.0.txt`; the sdists
carry their crates as path dependencies and so those copies.
`client/rust-sys` vendors the engine's own C sources (`vendor.sh`, `csrc/`), among them the Apache-2.0 files of the
table above (which is why it carries the Apache text): nothing in them comes from another
project (see "Copied code" above), so the licence files are all they need. The crates do not pack their
dependencies (cargo fetches them), so a program that links them carries its dependencies' notices itself.
`tools/check-package-licenses.sh` is the check (see below).

## Python

| name | version | license | how it is used |
|---|---|---|---|
| maturin | >=1.9, <2 | MIT OR Apache-2.0 | build only (the build backend in `pyproject.toml`) |
| numpy | any | BSD-3-Clause | optional at run time (`pip install intellistream-tsdb-client[numpy]`); not shipped |
| pytest | any | MIT | tests only |
| apache-iotdb, pandas, numpy | pinned in `bench/dbcmp/release/iotdb-requirements.txt` | Apache-2.0, BSD-3-Clause, BSD-3-Clause | benchmark scripts only; installed by the person who runs the comparison, not part of tsdb |

The Python modules themselves are the Rust crates above, compiled by maturin: `tsdb_client` (Apache-2.0, the network
client) and `tsdb_embedded` (AGPL-3.0-only, the engine in the process, which depends on `tsdb_client` of the same
version for the exceptions and the shared types).

## Java: `client/java`, `client/java-embedded`, `connectors/pulsar`

From the three poms (the local Maven repository did not hold every artifact, so `mvn dependency:list` could not
be run offline; the poms of the artifacts that are there were read for their license).

| name | version | license | scope and use |
|---|---|---|---|
| zstd-jni (`com.github.luben`) | 1.5.7-6 | BSD-2-Clause (its bundled native zstd: BSD-3-Clause) | `tsdb-client`, compile. Not shaded: the user's build brings it. In the Pulsar NAR it is packed. |
| jackson-databind, -core, -annotations | 2.18.2 | Apache-2.0 | Pulsar connector, compile; packed into the NAR |
| Pulsar: pulsar-io-core, pulsar-functions-api, pulsar-client-api, pulsar-client-api-v5 | 5.0.0 | Apache-2.0 | Pulsar connector, `provided`: the Pulsar runtime has them; not packed |
| pulsar-io-common | 5.0.0 | Apache-2.0 | Pulsar connector, compile (`IOConfigUtils`, the password from Pulsar's secrets); packed into the NAR. Its logger, slog, comes with pulsar-functions-api from the runtime |
| pulsar-client-v5 | 5.0.0 | Apache-2.0 | Pulsar connector, test scope in the `integration` profile |
| junit-jupiter | 5.13.1 | EPL-2.0 | test scope in all three; never shipped |
| slf4j-simple | 2.0.16 | MIT | Pulsar connector, test scope |
| `tsdb-client` (ours) | 0.11.0 | Apache-2.0 | dependency of `tsdb-embedded` (AGPL-3.0-only) and of the connector (Apache-2.0) |
| maven-compiler-plugin 3.14.0, maven-surefire-plugin 3.5.3, maven-jar-plugin 3.4.2, maven-javadoc-plugin 3.12.0, maven-dependency-plugin 3.8.0, nifi-nar-maven-plugin 1.5.0 | | Apache-2.0 | build only |

`tsdb-embedded` (AGPL-3.0-only) bundles `libtsdb.so` (our code, AGPL-3.0-only) in its jar. That library links the system's libzstd
dynamically, so no zstd code is inside the jar. It needs Java 22 or newer (the FFM API); the JDK is the user's.

The two jars (`tsdb-client`: Apache-2.0; `tsdb-embedded`: AGPL-3.0-only, with `LICENSES/Apache-2.0.txt` as well)
carry `LICENSE` and `NOTICE` in `META-INF/` (a Maven resource that reads them from the package's own directory, or the
repository root). `tsdb-client` does not pack zstd-jni and `tsdb-embedded` packs only our
`libtsdb.so`, which links the system's libzstd dynamically, so neither needs more. (A `libtsdb.so` built with zstd
linked statically would need the zstd text of `dist/THIRD-PARTY-NOTICES` in the jar.)

The Pulsar NAR packs `tsdb-client`, pulsar-io-common, Jackson and zstd-jni (`META-INF/bundled-dependencies/`). Its own `META-INF/` has
`LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES` (`connectors/pulsar/src/main/resources/META-INF/`): the texts of
zstd-jni (BSD-2-Clause) and of zstd inside it (BSD-3-Clause), and Jackson's NOTICE (Apache-2.0 asks for it). The
bundled jars keep their own `META-INF/LICENSE` and `NOTICE` unchanged; the zstd-jni jar has none, which is why its text
is written out. jackson-core packs a shaded FastDoubleParser (MIT) whose notices are in the jackson-core jar.

## Not dependencies

The databases that `bench/dbcmp` compares tsdb with (ClickHouse, QuestDB, TimescaleDB, InfluxDB, DuckDB and the
others) run in their own containers or are downloaded by the benchmark scripts. Nothing of them is part of tsdb or
its tarball, so their licenses do not touch tsdb's. The Podman containers the scripts start use their own images.

Publishing measurements of them is allowed: the open-source editions benchmarked are under Apache-2.0 (ClickHouse,
chDB, QuestDB, IoTDB, VictoriaMetrics, Prometheus, TimescaleDB's core), MIT (DuckDB, InfluxDB 2), MIT/Apache-2.0
(InfluxDB 3 Core) or AGPL-3.0 (TDengine), and an open-source license cannot forbid benchmarks. TimescaleDB's
compression is under the Timescale License (tsl/LICENSE-TIMESCALE, posted 2020-09-24), which was read on 2026-11-02:
it restricts offering the software as a database service, derivative works and resale, and says nothing about
benchmarks or publishing test results. Enterprise editions, which can carry other terms, were not benchmarked.

## Conclusion

- **Compatible with the AGPL-3.0 and, for the clients, with Apache 2.0: yes.** Every dependency is under MIT, BSD,
  Apache-2.0, EPL-2.0 (test only, never shipped), MIT-or-LGPL (taken as MIT), or the GCC Runtime Library Exception.
  No GPL or LGPL code is linked into tsdb's binaries, jars or wheels except by dynamic linking against the system's
  glibc. No dependency of the Apache-2.0 clients is copyleft, so the clients stay usable in closed programs.
- **Apache-2.0 inside AGPL-3.0: allowed** (one direction only); `tools/check-spdx.sh` (run by `make test`) checks
  that no Apache-2.0 file depends on an AGPL one.
- **The tarball must carry** `LICENSE`, `LICENSES/Apache-2.0.txt`, `LICENSING.md`, `NOTICE` and `THIRD-PARTY-NOTICES`
  (zstd BSD text, liburing MIT text). `make dist` copies them.
- **The other packages carry theirs too** (done 2026-11-04, redone for the two licences 2026-11-09): the two Python
  wheels and sdists (`LICENSE`, `NOTICE`, `THIRD-PARTY-NOTICES`; the AGPL one also `LICENSES/Apache-2.0.txt`), the Rust
  crates (`LICENSE`, `NOTICE`; the AGPL ones also the Apache text), the jars (`META-INF/LICENSE`, `NOTICE`) and the
  Pulsar NAR (Apache-2.0, also `THIRD-PARTY-NOTICES`). `make test-licenses` (part of `make test-clients`; the sdist and the wheel
  in `make test-python-sdist`) builds each package and fails when a file is missing or differs from the root's
  `LICENSE`/`NOTICE` of its licence (`tools/check-package-licenses.sh`); `tests/distro/dist-check.sh` does it for the tarball. After a dependency change: rerun
  `tools/gen-third-party.py` and update the NAR's notices by hand.
