---
name: tsdb-tracks
description: How work is done on tsdb with subagents - a build, adversarial review and fix "track" per change in its own git worktree, the gates each kind of change must pass, how branches are merged and pushed, and the benchmark policy. Use when starting, running, reviewing or merging any non-trivial change to tsdb (engine, server, clients, connectors, docs), or when asked how this project runs agents.
---

# tsdb tracks

A track is one change, built in its own worktree by a builder, attacked by an adversarial reviewer, finished
by a fixer, then merged by the main session. It uses the subagent-economy skill (long runs detached with
`bgrun.sh`, a tiny `iso-waiter` agent waits, continuations start fresh from NOTES.md, checkpoint at ~50 tool
calls). Read AGENTS.md first: its invariants and rules are the contract every track works under.

## Start a track

1. Worktree and branch from main: `git worktree add -b <branch> ../tsdb-<key> main`.
2. Scratch dir `<agents>/<key>/` (default `$HOME/.local/state/subagent-economy/agents/tsdb/<key>`, outside the
   repo and outside ~/.claude) holding `TASK.md`, written by the main session, under ~6 KB, index at the top:
   - repo, worktree, branch, base; "read AGENTS.md first"; which files other running tracks touch (avoid them);
   - the problem with evidence (file:line, failing output), and the required behaviour as numbered points;
   - tests: every bug fix gets a regression test that fails on the old code (build it to check); plant
     mutations to prove a new harness has teeth;
   - the gates for this kind of change (below), docs to update, and the commit rules (below).
3. Run `workflow-template.js` (next to this file) with the Workflow tool:
   `args: {tracks: [{key, wt, br, base: "main", risk?}], agents: "<agents root>", seBin: "<scripts dir>",
   rules: "<repo>/AGENTS.md"}`. Find seBin with
   `find ~/.claude -path '*subagent-economy/scripts' -type d -print -quit`.
   `risk: "low"` (docs, harness, benchmarks, tooling, test-only, client READMEs) runs build and fix on Sonnet;
   anything that can lose or corrupt data (engine, WAL, merges, server, protocol, FFI bindings, codecs) stays on
   the session's model. Review always runs on the session's model.
4. Several tracks may run at once if they touch different files; serialise tracks that share files.

## Gates (the builder runs them detached; the fixer reruns what a finding touched, then all once)

| change | gates |
|---|---|
| anything in src/, server/, client/*.c | `make test`; `make asan` in the worktree (it runs `make clean`); `make tsan` |
| how data reaches the disk (writes, fsync, renames, recovery, merges, WAL) | the above + `make jepsen` |
| wire protocol | the above + `make test-clients` (all four clients change in the same commit) |
| anything under client/ or connectors/ (code, tests, docstrings, stubs, constants) | that client's own suite (`make test-rust`, `test-python`, `test-java`, `mvn test` in connectors/pulsar); a protocol version bump runs `make test-clients` |
| portability | `make dist`, `make dist-check-sandybridge`, `make test-almalinux10` |
| a test or benchmark harness that starts tsdbd | `--port 0 --port-file`, never a computed port (AGENTS.md "Ports"); `make test` in two copies of the tree at once shows no collision |
| docs | every claim checked against the code or a primary source; runnable snippets run |

## Merge (main session)

- Read the track's REPORT.md and REVIEW.md "After review"; check every finding was fixed or answered.
- No session links in any commit (`git log --format=%B main..<br> | grep -ci claude.ai/code` is 0); every
  commit ends with `Co-Authored-By: ...` then `Signed-off-by:` (the commit-msg hook in .githooks enforces it;
  `make hooks` installs it).
- Fast-forward when the branch sits on main; otherwise `git merge --no-edit --signoff <br>`. Resolve conflicts
  keeping both sides' intent; then rerun `make test` (and the gates the merge could affect) on main, detached.
- Push main only when green. Remove worktrees of merged branches when no longer needed.

## Benchmarks

Timings only on a quiet machine (no builds or tests alongside, load average below 2). Internal benchmarks
(flags, float32 vs float64, io_uring vs epoll, clients embedded and over TCP) run in quiet windows; the full
comparison against other databases and the 100 B run against ClickHouse only near a release
(bench/dbcmp/README.md). Containers with Podman only, bound to 127.0.0.1, images pinned; remove benchmark data
and containers afterwards.

## Never

`pkill -f`, `killall`, `kill` with pid -1 or 0, `docker` (the project's PreToolUse hook refuses them), pushing
unreviewed work, publishing timings from a busy machine.
