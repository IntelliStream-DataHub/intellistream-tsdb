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
  matching entries in the `Cargo.lock` files (`cargo update -p intellistream-tsdb-client` or edit the three lines)
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

## 4. Security review, then gates

Before the gates and the release benchmarks: a security review of everything changed since the previous
release (or the previous review). tsdb runs on backend systems behind network isolation, never on the
internet, and performance comes first, so the review reports major issues only: memory corruption from
network frames, files, export streams or client replies; one request or file that crashes the server or
exhausts its memory; data loss or corruption; writes outside the directories the user named. Fix those (at
the boundary, at no cost on hot paths) before tagging; hardening can wait.

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

Write the release text first, if the changelog section alone is not what the release page should say:
`docs/releases/vX.Y.Z.md` (`tools/release-notes.sh vX.Y.Z` prints what the page will get).

The push of the tag starts the release workflow on GitHub (`.github/workflows/release.yml`); nothing else is
done by hand, and `gh` is not needed. It:

1. checks out exactly the tag, checks that `VERSION` is the tag without its `v`, and runs `make dist`,
   `make dist-test` and `make dist-check-sandybridge` there (`.github/scripts/release-build.sh`; the build id in
   BUILDINFO and `--version` is the tag);
2. writes `tsdb-X.Y.Z-x86_64-linux.tar.gz.sha256` beside the tarball and attests the tarball's build provenance;
3. creates the GitHub release "tsdb X.Y.Z" as a draft, uploads the two files and only then publishes it. The text
   is `docs/releases/vX.Y.Z.md` as it is at the tag, else as it is on the branch the workflow ran from, else the
   tag's CHANGELOG section. A tag with a suffix (`v1.0.0-rc.1`) becomes a prerelease and never "latest".

The tarball on the release page is CI's build (Ubuntu 24.04's compiler), so its checksum differs from a local
`make dist`; the release text points to the attached `.sha256` file instead of quoting one.

To run it again for a tag that exists (a failed run, or a tag pushed before the workflow existed, as v0.9.0):
GitHub -> Actions -> release -> Run workflow, branch `main`, tag `vX.Y.Z`. It builds the tag again and replaces
the two files on the release (it deletes the old ones first); a release that is already published keeps its text,
which you edit on the release page. A draft that a failed run left is completed and published. If the tag moves
while a run is going, the run fails rather than attach a build of another commit.

`make dist-check-almalinux10` needs Podman and is not part of the workflow: run it by hand (step 4).

Publishing the clients (crates, the Python wheel, Maven) is not set up yet; when it is, add the steps here.

## 7. After the release

- Start "Unreleased" in the changelog. Leave `VERSION` as it is until the next bump (a development build
  shows `X.Y.Z (build vX.Y.Z-N-gHASH)`).
- Write down in `docs/roadmap.md` what the release left for the next one.
