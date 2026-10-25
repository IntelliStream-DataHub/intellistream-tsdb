# Cutting a release

The steps to release tsdb, in order, so that a later release is done the same way. A release is cut from
`main` when it is green; nothing is pushed or tagged until every step before it is done.

Machines and tools: a quiet x86-64 Linux machine with `gcc` 15 or 16, Podman (never Docker), `cargo`,
`python3`, a JDK 22+ and `mvn`. qemu-user (`qemu-x86_64`) is optional for the Sandy Bridge check.

## 1. Decide the version

- MAJOR.MINOR.PATCH. Before 1.0, a change of the disk format or the wire protocol is allowed in a minor
  release and must be in the changelog (docs/compatibility.md); a bug fix is a patch release.
- Bump `FORMAT_VERSION` or `TSDB_PROTOCOL_VERSION` only for a real change of the format or protocol, and
  then check that the refusal message and the export/import path of docs/compatibility.md still describe it.

## 2. Bump the version everywhere

Edit `VERSION`, then the other places that must say the same:

- `include/tsdb.h`: `TSDB_VERSION_MAJOR`, `_MINOR`, `_PATCH` and `TSDB_VERSION`
- `client/rust/Cargo.toml`, `client/rust-sys/Cargo.toml`, `client/python/Cargo.toml` (`version =`), and the
  matching entries in the `Cargo.lock` files (`cargo update -p tsdb-client` or edit the three lines)
- `client/java/pom.xml`, `client/java-embedded/pom.xml` (its version and its dependency on `tsdb-client`),
  `connectors/pulsar/pom.xml` (its version and `tsdb-client.version`)
- the version in the READMEs' jar names (`grep -rn "tsdb-client-0\." --include=*.md .`)

Then `make check-version`. It compares all of them with `VERSION`, and with what `build/tsdbd --version`
and `build/tsdb --version` print; `make test` runs it too.

## 3. Changelog and docs

- `CHANGELOG.md`: move "Unreleased" under the new version with its date; list what changed in plain
  language (Added, Changed, Fixed, Removed); keep "Known limitations" true; say the format and protocol
  versions.
- Check every line of the changelog entry against the code, not against plans: e.g. each client's protocol
  version (`include/tsdb_protocol.h`, `client/rust/src/protocol.rs`, `client/java/.../Protocol.java`), and
  features the roadmap still lists as to do.
- `docs/compatibility.md`: the table at the top; what this release reads.
- `docs/roadmap.md`: what is done, what is next. README and AGENTS.md: numbers that changed.

## 4. Gates

From a clean checkout of the release commit, one after the other (run them detached and wait; the
long ones take minutes to an hour; see `.claude/skills/subagent-economy`):

```sh
make test                      # unit, integration and server tests, io_uring and epoll
make asan && make tsan         # memory errors and data races (asan starts with make clean)
make jepsen                    # fault injection, if anything about disk writes changed since the last release
make test-clients              # Rust, Python and Java clients against build/tsdbd
make integration-tests         # tsdbd + CLI + all clients end to end, uring and epoll
make dist                      # tsdb-VERSION-x86_64-linux.tar.gz in build-dist/ (checks glibc and ISA)
make dist-test                 # the tarball's own tests at every CPU level
make dist-check-almalinux10    # that suite inside AlmaLinux 10 (Podman)
make dist-check-sandybridge    # TSDB_CPU=v2, and under qemu -cpu Westmere/SandyBridge if installed
```

Every one must pass. A failure that is a flaky test is fixed or understood before the release, not
retried until green.

## 5. Release benchmarks

On a quiet machine: nothing else running (no builds, tests, ClickHouse in the background), the CPU
governor on `performance` (`cpupower frequency-set -g performance`; put it back afterwards), each
measurement an average of several runs (at least 3; say how many in the table).

- Internal numbers: `build/codec-bench`, `build/type-bench`, `build/tsdb-bench`, `build/insert-bench`,
  `build/tsdbc-bench`, `build/fold-bench`. Update the tables in AGENTS.md and README.
- The full comparison with the other databases (`bench/dbcmp`, its README has the steps) and the 100
  billion row run against ClickHouse (`build/scale-bench`), in Podman, bound to 127.0.0.1. Update the
  README's comparison table and AGENTS.md. Note the versions of the other databases.
- A regression against the previous release is explained or fixed before the tag.

## 6. Tag and publish

```sh
git status                              # clean
tools/check-version.sh build            # still agrees
git commit -s -m "Release X.Y.Z"        # the bump, changelog and benchmark numbers (no session links)
git tag -s vX.Y.Z -m "tsdb X.Y.Z"       # an annotated tag; -s signs it if you have a key (else -a)
git push origin main vX.Y.Z
```

Then attach `build-dist/tsdb-X.Y.Z-x86_64-linux.tar.gz` (and its `sha256sum`) to the release, with the
changelog entry as the release notes. `make dist` after the tag names the build `vX.Y.Z` in BUILDINFO and
`--version`; the tarball you publish is the one made from the tagged commit, which the gates above ran on.

Publishing the clients (crates, the Python wheel, Maven) is not set up yet; when it is, add the steps here.

## 7. After the release

- Start "Unreleased" in the changelog. Leave `VERSION` as it is until the next bump (a development build
  shows `X.Y.Z (build vX.Y.Z-N-gHASH)`).
- Write down in `docs/roadmap.md` what the release left for the next one.
