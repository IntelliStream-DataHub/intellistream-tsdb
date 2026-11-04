# Licenses of what tsdb uses

tsdb is licensed under the [Apache License 2.0](../LICENSE) (copyright IntelliStream AS, see [NOTICE](../NOTICE)).
This page lists the software and data that tsdb builds on, under which license each comes, and how it is used.
It answers two questions: *can these licenses live together with Apache 2.0?* and *what has to ship with a
release?* The audit was made on 2026-11-02 for version 0.9.0. Redo it when a dependency is added or a version
changes (the Rust lockfiles and the poms are the source of truth).

**In short.** Everything is compatible with Apache 2.0. Nothing is GPL or LGPL in the way tsdb uses it, and no
copyleft code is linked into any binary. Two libraries are linked statically into the tarball's binaries (zstd and
liburing); their licenses (BSD and MIT) ask that their notice travels with the binaries. The tarball
now carries it: [dist/THIRD-PARTY-NOTICES](../dist/THIRD-PARTY-NOTICES), plus `LICENSE` and `NOTICE`.

## Copied code and data

No source code from another project is copied into the repository. What is close to it:

| what | where | origin and license | note |
|---|---|---|---|
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

`libtsdb.a` and `libtsdbclient.a` in the tarball contain tsdb code only. A program that links them also links
its own libzstd, so it carries zstd's notice itself.

**What ships with the tarball** (`make dist`, `tools/mkdist.sh`): `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES`
(the texts of the zstd and liburing licenses and the libgcc remark) at its top level. `tests/distro/dist-check.sh`
checks that they are there. Before this audit the tarball had none of them; that was the one gap found.

## Rust: `client/rust`, `client/rust-sys`, `client/python`, `tests/integration/rust`

From `cargo metadata` on the four lockfiles (the union is below; `tsdb-client` needs the first block,
`tsdb-sys` only zstd-sys and the build tools, the Python module adds the PyO3 block, the integration tests only
`tsdb-client`'s). None is a dev-dependency of the crates; the crates are `publish = false` for now.

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

The Python module (`client/python`, PyO3) adds:

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

**What ships.** The Python wheel and sdist (`client/python`) carry `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES`
(maturin `license-files`; in the wheel under `.dist-info/licenses/`). `THIRD-PARTY-NOTICES` has the license texts of
the crates that are linked into the extension module (normal dependencies, not build tools and not procedural
macros, which run in the compiler) and of the zstd C library inside zstd-sys, where a crate that offers MIT OR
Apache-2.0 is taken under MIT. It is generated from `client/python/Cargo.lock` by `tools/gen-third-party.py`
(`--check` fails when the lockfile moved; no `cargo about` needed): rerun it when a dependency changes. The two
published crates (`client/rust`, `client/rust-sys`) hold copies of `LICENSE` and `NOTICE` in their directories, so
`cargo package --list` shows them; the sdist carries both crates as path dependencies and so both copies.
`client/rust-sys` vendors the engine's own C sources (`vendor.sh`, `csrc/`): nothing in them comes from another
project (see "Copied code" above), so `LICENSE` and `NOTICE` are all they need. The crates do not pack their
dependencies (cargo fetches them), so a program that links them carries its dependencies' notices itself.
`tools/check-package-licenses.sh` is the check (see below).

## Python

| name | version | license | how it is used |
|---|---|---|---|
| maturin | >=1.9, <2 | MIT OR Apache-2.0 | build only (the build backend in `pyproject.toml`) |
| numpy | any | BSD-3-Clause | optional at run time (`pip install tsdb-client[numpy]`); not shipped |
| pytest | any | MIT | tests only |
| apache-iotdb, pandas, numpy | pinned in `bench/dbcmp/release/iotdb-requirements.txt` | Apache-2.0, BSD-3-Clause, BSD-3-Clause | benchmark scripts only; installed by the person who runs the comparison, not part of tsdb |

The Python module itself is the Rust crates above, compiled by maturin.

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
| `tsdb-client` (ours) | 0.9.0 | Apache-2.0 | dependency of `tsdb-embedded` and of the connector |
| maven-compiler-plugin 3.14.0, maven-surefire-plugin 3.5.3, maven-jar-plugin 3.4.2, maven-javadoc-plugin 3.12.0, maven-dependency-plugin 3.8.0, nifi-nar-maven-plugin 1.5.0 | | Apache-2.0 | build only |

`tsdb-embedded` bundles `libtsdb.so` (our code) in its jar. That library links the system's libzstd
dynamically, so no zstd code is inside the jar. It needs Java 22 or newer (the FFM API); the JDK is the user's.

The two jars (`tsdb-client`, `tsdb-embedded`) carry `LICENSE` and `NOTICE` in `META-INF/` (a Maven resource that
reads the two files at the repository root). `tsdb-client` does not pack zstd-jni and `tsdb-embedded` packs only our
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

- **Compatible with Apache 2.0: yes.** Every dependency is under MIT, BSD, Apache-2.0, EPL-2.0 (test only, never
  shipped), MIT-or-LGPL (taken as MIT), or the GCC Runtime Library Exception. No GPL or LGPL code is linked into
  tsdb's binaries, jars or wheels.
- **The tarball must carry** `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES` (zstd BSD text, liburing MIT text).
  `make dist` now copies them.
- **The other packages carry theirs too** (done 2026-11-04): the Python wheel and sdist (`LICENSE`, `NOTICE`,
  `THIRD-PARTY-NOTICES`), the Rust crates (`LICENSE`, `NOTICE`), the jars (`META-INF/LICENSE`, `NOTICE`) and the
  Pulsar NAR (also `THIRD-PARTY-NOTICES`). `make test-licenses` (part of `make test-clients`; the sdist and the wheel
  in `make test-python-sdist`) builds each package and fails when a file is missing or differs from the root's
  `LICENSE`/`NOTICE`; `tests/distro/dist-check.sh` does it for the tarball. After a dependency change: rerun
  `tools/gen-third-party.py` and update the NAR's notices by hand.
