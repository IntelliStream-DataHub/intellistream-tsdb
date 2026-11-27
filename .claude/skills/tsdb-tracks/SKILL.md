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
   rules: "<repo>/AGENTS.md"}`. seBin is
   `<repo>/.claude/skills/subagent-economy/scripts` (the subagent-economy skill lives in this repository).
   `risk: "low"` (docs, harness, benchmarks, tooling, test-only, client READMEs) runs build and fix on Sonnet;
   anything that can lose or corrupt data (engine, WAL, merges, server, protocol, FFI bindings, codecs) stays on
   the session's model. Review always runs on the session's model.
4. Several tracks may run at once if they touch different files; serialise tracks that share files.

## Tests per change, gates once a week (the owner's rule, 2026-11-22)

A track proves its own behaviour with integration tests aimed at what it changed; the full gates run once a week on
main, not per track or per merge. GitHub CI still runs on every push.

Per change (the builder runs them, detached when long; the fixer reruns what a finding touched):
- the regression and integration tests of the change: the new cases, the whole test binary or `test_cluster`
  scenarios they live in, end to end through tsdbd where the behaviour is visible there;
- a planted mutant per new check, caught by its test (the Makefile's mutant loop for that binary);
- those binaries under ASan or TSan when the change is concurrent or touches memory layout (only those binaries,
  `make B=build-asan ... build-asan/test_x`, not `make asan`);
- for how data reaches the disk: a crashfs power-loss or crash-point case for that path, not `make jepsen`;
- a wire protocol change: `make test-clients`; a client or connector: its own suite; docs: every claim checked;
- a harness that starts tsdbd: `--port 0 --port-file`, never a computed port (AGENTS.md "Ports").

Weekly gate (Saturdays, on main, when nobody else needs the machines; one job per machine): `make test`,
`make test IO=epoll`, `make asan`, `make tsan` (its groups as parallel jobs), `make jepsen`, the cluster gate
(`agents/tsdb/r3-gate/gate-par.sh REV`: jepsen-cluster's matrix in parallel lanes, about an hour), the mutant suites
(`repld-mutants lease-mutants copy-mutants meta-mutants auth-mutants sim-repl-mutants rset-mutants transfer-mutants margin-mutants heal-mutants`), `make dist-test` when
portability changed. What it finds becomes fix tracks. Never a 12-hour soak as a gate; long soaks only on idle machines.

## Merge (main session)

- Read the track's REPORT.md and REVIEW.md "After review"; check every finding was fixed or answered.
- No session links in any commit (`git log --format=%B main..<br> | grep -ci claude.ai/code` is 0); every
  commit ends with `Co-Authored-By: ...` then `Signed-off-by:` (the commit-msg hook in .githooks enforces it;
  `make hooks` installs it).
- Fast-forward when the branch sits on main; otherwise `git merge --no-edit --signoff <br>`. Resolve conflicts
  keeping both sides' intent; then rerun the tests of both branches' changes on the merge, detached.
- Push main only when those tests are green. Remove worktrees of merged branches when no longer needed.

## Benchmarks

Timings only on a quiet machine (no builds or tests alongside, load average below 2). Internal benchmarks
(flags, float32 vs float64, io_uring vs epoll, clients embedded and over TCP) run in quiet windows; the full
comparison against other databases and the 100 B run against ClickHouse only near a release
(bench/dbcmp/README.md). Containers with Podman only, bound to 127.0.0.1, images pinned; remove benchmark data
and containers afterwards.

## Never

`pkill -f`, `killall`, `kill` with pid -1 or 0, `docker` (the project's PreToolUse hook refuses them), pushing
unreviewed work, publishing timings from a busy machine.
