# Contributing to tsdb

Thank you for wanting to help. tsdb is a time series database written in C, with a server (`tsdbd`) and clients in
C, Rust, Python and Java. This page tells you how to build it, how to test a change, and the few rules every change
follows. Bug reports, fixes, tests, documentation and new ideas are all welcome.

If you only want to try tsdb, the [README](README.md) is the better start. If you found a security problem, do not
open an issue: read [SECURITY.md](SECURITY.md).

## Where to start

- [AGENTS.md](AGENTS.md) is the manual for everyone who changes the code, people and coding agents alike. It lists
  every build and test command, the layout of the source and the **invariants** (see below). Read at least its
  "Commands" and "Invariants" sections before you change the engine.
- [docs/design.md](docs/design.md) explains how the storage works.
- [docs/roadmap.md](docs/roadmap.md) says what is planned. For a big change, open an issue first so we can agree
  on the idea before you write the code.
- [docs/compatibility.md](docs/compatibility.md) says what 0.9.x promises about the disk format and the wire
  protocol (not much: both may still change before 1.0).

## Build

You need:

- a C11 compiler (gcc; the project is built with recent versions, 13 or newer) and `make`,
- `libzstd` with its development files,
- `liburing` with its development files, for the server. Without it, build the server on epoll only with
  `make NO_URING=1` (run `make clean` first when you switch).

```sh
make                  # library, command line tool, server, client, benchmarks, all into build/
make PORTABLE=1       # without -march=native, so the binaries run on other CPUs
make NO_URING=1       # tsdbd on epoll only, without liburing
```

The Rust, Python and Java clients need `cargo`, `python3` (with `maturin`) and `mvn`. Only
`make test-clients` and `make integration-tests` use them.

## Test

```sh
make test               # all unit, crash-recovery and server tests
make test IO=epoll      # the server tests on epoll only
make asan               # the tests under AddressSanitizer and UBSan (starts with make clean)
make tsan               # the tests under ThreadSanitizer, in build-tsan/ (about 2.5 minutes)
make jepsen             # fault injection: kill -9, simulated power loss, pauses, disk errors
make fuzz               # the fuzz harnesses (FUZZ_SECONDS=600 for longer)
make test-clients       # the Rust, Python and Java clients against build/tsdbd
make integration-tests  # tsdbd, the CLI and all clients end to end (about 2 minutes)
```

Run the tests that cover what you changed. In short:

| your change | run at least |
|---|---|
| anything in `src/`, `server/`, `tools/` | `make test` |
| anything concurrent, or that touches memory layout | `make test`, `make asan`, `make tsan` |
| how data reaches the disk (writes, fsync, renames, recovery, merges, the WAL) | all of the above and `make jepsen` |
| the wire protocol or a client | `make test-clients` and `make integration-tests` |
| a codec or a SIMD kernel | `make test` (it runs `tests/test_levels.c`); `make dist-test` for the CPU levels |
| a frame the server parses | `make fuzz-frames FUZZ_SECONDS=600` |
| the version number | `make check-version` (`make test` runs it) |

`make asan` cleans `build/` first. If something else is using `build/` (a running `tsdbd`, client tests), run
it in a copy of the tree. Tests take a scratch directory as their first argument (`./build/test_tsdb /tmp/x`).

Tests and tools never pick a TCP port by themselves: start `tsdbd` with `--port 0 --port-file FILE` and read the
port from the file, as the test harnesses do. Two runs on one machine would otherwise talk to each other's
servers. [AGENTS.md](AGENTS.md) explains why.

## The rules

1. **Every bug fix comes with a regression test that fails without the fix.** Check it: build the old code
   and watch the test fail, then apply your fix. New behaviour comes with a test too.
2. **A change to the wire protocol reaches all four clients in the same commit** (C, Rust, Python and Java), with
   `make test-clients` green. Before 1.0 the protocol may change without keeping old clients working, but never
   in only one place.
3. **Respect the invariants.** The "Invariants that are easy to break" section of [AGENTS.md](AGENTS.md) lists the
   rules the storage engine depends on: last write wins, inserts are all or nothing, what is durable after which
   fsync, the lock order, what happens on an I/O error. They were each learned from a bug. If your change seems to
   need to break one, say so in the pull request and explain why.
4. **Performance claims need numbers.** If a change is about speed or size, show before and after from the
   benchmark tools (`build/codec-bench`, `build/tsdb-bench`, `build/insert-bench`, `build/tsdbc-bench`, see
   "Benchmarks" in [AGENTS.md](AGENTS.md)). Run every measurement several times and report the average, on a
   machine that does nothing else. `tools/bench-tsdb.sh` benchmarks tsdb on a fresh clone with one command and
   writes a summary with means and spreads; it is the easiest way to get comparable numbers.
5. **Damage is never silent.** Do not treat a read error as end of file, skip a broken part or drop a record
   quietly. Decoders must be safe on any bytes.
6. **Match the code around you.** 4-space indent, lines up to about 120 characters, `snake_case`, errors as negative
   `TSDB_ERR_*` codes. Comments say why, not what.
7. **Write plain documentation.** The README and the client READMEs are for people who are new to databases:
   short sentences, no jargon (or a few words of explanation when a term is needed). Details go in `docs/` or
   [AGENTS.md](AGENTS.md).
8. **Update the [CHANGELOG](CHANGELOG.md)** under "Unreleased" for anything a user can notice.

Do not commit `build*/` directories or benchmark data.

## Containers: Podman, not Docker

Anything that needs a container uses [Podman](https://podman.io/): `podman` instead of `docker`,
`podman-compose` instead of `docker compose`. Our scripts, Makefile targets (`make dist-check-almalinux10`) and
benchmark setups are written for it. A `Dockerfile` or `Containerfile` is fine, since Podman builds both.
Please do not add scripts that call the `docker` CLI or need its daemon.

## Commits and pull requests

Keep each commit to one logical change, and write the message so that it says **why**, not only what.

### Sign-off (DCO)

Every commit must carry a `Signed-off-by` line. That is the
[Developer Certificate of Origin](https://developercertificate.org/): by signing off you say that you wrote the
change (or have the right to submit it) and that you are fine with it going into the project under the
[Apache License 2.0](LICENSE). It is not a legal contract with us and needs no paperwork.

```sh
git commit -s        # adds: Signed-off-by: Your Name <you@example.com>
```

The line uses the name and email from your `git config`. Keep it as the **last** trailer of the message. Install
the check once per clone, and git will refuse a commit that does not follow the rules:

```sh
make hooks           # git config core.hooksPath .githooks
```

If you forgot, `git commit --amend -s` (or `git rebase --signoff` for several commits) fixes it.

### Pull requests

- Say what the change does and why, and how you tested it (which of the commands above).
- Keep the pull request small enough to review. Unrelated cleanups go in their own.
- A reviewer may ask for numbers, a test or a second look at an invariant. That is normal.

## AI-assisted contributions

Work done with the help of an AI coding assistant is welcome, under exactly the same rules: you are the author of
what you submit, you have read it and understood it, it passes the tests above, and you can sign off on it.

The repository is set up for [Claude Code](https://claude.com/claude-code), and you need nothing beyond Claude
Code itself:

- `AGENTS.md` is the manual an agent reads first.
- `.claude/settings.json` turns on two small hooks (`.claude/hooks/`). `guard-bash.sh` refuses commands that
  are dangerous or against the rules here: `pkill -f`, `killall`, `kill` with pid -1 or 0 (it would reach every
  process of your user), and `docker`. `guard-push.sh` refuses a `git push` while an unpushed commit carries a link
  to an AI session.
- `.claude/skills/` and `.claude/agents/` describe how larger changes are built, reviewed and checked with
  subagents (`tsdb-tracks`), how to review durability code the way Jepsen does (`jepsen-review`), and how to keep
  long-running jobs cheap to watch (`subagent-economy`, `job-watchdog`). You can ignore them.
- Personal settings go in `.claude/settings.local.json`, which git ignores.

One rule is strict: **never put a link to an AI session** (a `Claude-Session:` trailer, a
`https://claude.ai/code/session_...` URL or the like) in a commit message, a pull request, a review comment or
any file. Session links are private and add noise to the history. The `commit-msg` hook rejects them. A
`Co-Authored-By:` line naming the assistant is fine, as long as the `Signed-off-by` line stays last.

## License

By contributing you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), like the
rest of tsdb. The copyright of the project is held by IntelliStream AS; you keep the copyright of your own
contribution (the sign-off is all we ask for).
