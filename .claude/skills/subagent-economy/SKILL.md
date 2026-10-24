---
name: subagent-economy
description: Use when you launch, write prompts for, or ARE a workflow subagent that builds, reviews, fixes, benchmarks or waits on long tests (integration suites, gates, fleet runs, soaks). Cuts token cost without cutting quality - never wait inside a big context, hand long runs to a tiny waiter, keep reads small, checkpoint long agents. Per-role rules for builder, reviewer, fixer, stager, fleet runner, analyst and waiter.
---

# Subagent economy

## Why (measured, 2026-10, on 29 subagents of a C storage project; `scripts/tokenstat.py` reproduces it)

| fact | number |
|---|---|
| prompt-cache writes that were a FULL re-write of the context after a gap of more than 5 min between two requests | 195 M of 212 M written tokens (92%) |
| share of relative cost (write 1.25x, read 0.1x) taken by cache writes | 77% |
| requests that follow a 5-30 min gap | 940, of which 795 re-wrote the whole context (median 230k tokens) |
| typical agent | 124 requests, starts at 44.5k tokens, ends near 260k, up to 550k |
| what stays in context longest | `cat`/`sed -n` of files 77% of tool-result residency, `git diff` 14%, the Read tool 3.5%, wait output 2.7% |

So: **a long test waited on inside a big agent costs a full context re-write every time the wait passes 5 minutes** (a 78 minute gate at 250k tokens is eight re-writes). The model does no useful thinking while it waits. Fix the waiting first, reading second, lifetime third; none of this lowers quality.

## The helpers (`scripts/` in this skill's base directory)

Use absolute paths in prompts: `<base>/scripts/<name>`, where `<base>` is the skill's base directory shown when it loads: `<repo>/.claude/skills/subagent-economy` in tsdb, which keeps the canonical copy, or `~/.claude/skills/subagent-economy` where it was copied for other projects on a machine (`cp -r <repo>/.claude/skills/subagent-economy ~/.claude/skills/`, and `.claude/agents/iso-waiter.md` to `~/.claude/agents/`). Pass it to the workflow templates as `args.seBin`.

- `bgrun.sh NAME CMD...` starts CMD detached and returns at once. Output goes to `$RUNS/NAME/out`, the exit code to `$RUNS/NAME/rc` (written last). With `SE_QUEUE` set (a queue wrapper that waits for a free slot on the machine), CMD runs behind it; `BGRUN_NOWAIT=1` skips the queue for light commands.
- `await.sh NAME [SECONDS=540]` blocks up to that long (keep it under the 10 minute tool cap) and prints ONE short status: `DONE NAME rc=N ...last 12 lines` (exit 0), `RUNNING NAME ...` (exit 10) or `died` (exit 11). It never prints the whole output.
- `rbgrun.sh NAME WORKTREE PROFILES CMD...`: `bgrun.sh` on a fleet of machines (see below); `await.sh` works unchanged.
- `usage.sh`, `save-usage.sh`: the usage gate (see below). `tokenstat.py`: measure a workflow.
- `workflow-template.js` (build → review → fix tracks), `stage-template.js` (stage a gate batch), `verify-patterns.js` (`verifyByFile`, the cheap verify fan-out): paste into a Workflow script.
- Agent `iso-waiter` (`.claude/agents/iso-waiter.md` in tsdb, or a copy in `~/.claude/agents/`): Bash only, Haiku, calls `await.sh` until DONE.

**Setup.** State lives in `SE_HOME` (default `~/.local/state/subagent-economy`, outside `~/.claude` because subagents may be refused writes there): `runs/`, `state/`, hold files, and an optional `config.sh` for this machine (queue, fleet, usage session; copy `scripts/config.example.sh`). Job names are letters, digits, `. _ -`. Give each project its own track scratch dir (templates: `args.agents`), e.g. `$SE_HOME/agents/<project>`.

## Rules for every agent

1. **Never wait for more than ~4 minutes inside your own context.** For anything longer: `bgrun.sh NAME ...`, write what you were doing and exactly how to judge the result into `<agents>/<track>/NOTES.md`, and END YOUR STAGE returning `wait: {name: "NAME", note: "what to judge"}`. The workflow script waits (with a tiny, cheap waiter agent) and starts a continuation agent with a fresh, small context that reads NOTES.md.
2. **Read narrowly.** `grep -n` first, then `sed -n A,Bp` of at most ~120 lines; never `cat` a file you have not measured (`wc -l`); read each handover file once and put what you learned in NOTES.md instead of re-reading; use `git diff --stat` then one file at a time, `| head -150`; pipe test and build output through `tail -n 40` or `grep -E 'FAIL|error|warning'`, with the full log going to a file you cite by path.
3. **Checkpoint at ~50 tool calls (or when you notice you re-read things).** Re-reading the context is ~94% of all tokens, so a shorter agent is the biggest saving there is. Write NOTES.md (decisions made, what is verified and how, what is left, the next command) and return `checkpoint: true`; the script continues in a fresh agent. A fresh agent starting at 45k tokens costs far less than request 300 of a 400k-token one.
4. Return the short schema, and put detail in files, not in your return text.

## Per role

| role | what it does | rules that matter most |
|---|---|---|
| **builder** | writes the code and tests | 1, 2 and 3. Detach every long suite/integration/coverage/sanitizer run. Between detaching and the result do not do other long work: write NOTES.md and end the stage. |
| **reviewer** | adversarial review, mutation runs | 1 and 2. Detach the mutation batch (one `bgrun` per batch of ~10 mutations, each logging one line `mutation N: KILLED/SURVIVED`). Review by `diff --stat` first and read only hunks you will comment on. |
| **fixer** | applies confirmed findings | 1, 2 and 3. Reads REVIEW.md once. Re-runs only the suites a finding touched, then the full set once at the end, detached. |
| **stager / integrator** | rebase, number bugs, stage checks, gates | A long landing gate is NEVER waited on by an agent: a queue runs it and the main session watches; the agent ends after queueing. |
| **fleet runner** | benchmark and soak orchestration | Runs are hours. The agent starts them with `bgrun`/`rbgrun`, records the output directory in NOTES.md and ends; analysis is a separate later agent that reads the result files. |
| **analyst / investigator** | read-only code and data analysis | No waits. Rule 2 above all (it reads the most). Several narrow analysts beat one that reads everything; each returns a table of claims with file:line. |
| **waiter** | does nothing but call `await.sh` until DONE | Agent type `iso-waiter`: tools = Bash only, cheapest model, ~10 lines of prompt. Each call is ~540 s; it returns the status line and nothing else. Never reads files, never reasons about results. |

## Running on a fleet (`rbgrun.sh`)

When `RB_HOSTS` (ssh targets, `user@host`) is set in `config.sh`, `rbgrun.sh NAME WORKTREE PROFILES CMD...` ships the source tree (no `.git`, no `build/`) plus `build/<profile>` dirs (comma list; `-` for none) to `/dev/shm/iso-ci/NAME` on a free machine, runs CMD there as root with nofile/memlock raised and `TMPDIR=/dev/shm/t`, and writes `runs/NAME/{out,rc,host}`. One job per machine (a lock dir there); it skips a machine running a process `RB_BUSY` names, and every machine while `$SE_HOME/HOLD.fleet` or `$SE_HOME/RESERVED` exists or `$SE_HOME/BENCH.lock` holds a live pid (create RESERVED before any benchmark, power-cut or format run on the fleet). `RB_REMOTE_ENV` adds environment there (header shims, `PKG_CONFIG_PATH`). TMPDIR is short on purpose: tests that build long paths from it make `mkdtemp` fail with EINVAL on a longer prefix.

MIRROR MODE for scripts with absolute paths (mutation batches that edit a worktree, rebuild and run): `RB_MIRROR="/abs/dir ..." RB_GIT=1 RB_BACK="/abs/logdir ..." rbgrun.sh NAME - - bash /abs/script.sh` copies the dirs to the same absolute path on the machine (only under `RB_MIRROR_ROOTS`; `RB_GIT=1` adds `$RB_REPO/.git`), runs the script there, copies RB_BACK dirs back and removes the copies; `SE_QUEUE` is replaced there by a pass-through stub. Only mirror dirs nobody else writes while it runs (copying a tree being built produced `file has vanished`). Agents never ssh to the fleet; they only call `rbgrun.sh`.

**Fleet stage check.** A stage check that fans its test lines out over the fleet in parallel cut ~2 h to 25 min. Re-run a test that fails only on the fleet locally before calling it red: green here = a FLEET-ONLY failure to judge (CPU features differ between machines), red here = red. The landing gate itself stays on one machine.

## The usage gate

`usage.sh` reads the Claude Code usage (5-hour window and week). The status line is the only place Claude Code hands these numbers out, so the status-line script must pass its input to `save-usage.sh` (one line: `printf '%s' "$input" | bash <base>/scripts/save-usage.sh`). Without readings nothing is held. `await.sh` holds a finished job while the 5-hour usage is >= `USAGE_GATE` (default 50): releases 4 min apart at >= 50%, 15 min at >= 75%, none at >= 90%, one per 30 min past 95% of the week; `$SE_HOME/HOLD_USAGE` holds everything. So continuations and later stages wait for the window instead of hitting the limit; the waiter's calls are cheap. The orchestrator also starts no new work at >= 50% and stops its newest workflows at >= 75%. `usage.sh --rate` prints the week's burn rate.

## Which model runs which stage (mixed models)

| stage | model | why |
|---|---|---|
| waiter | Haiku (`iso-waiter`) | it only calls await.sh |
| stager, merge check, docs, harness, CLI, tooling, test-only tracks (`risk: 'low'`): build and fix | Sonnet | mechanical or off the critical path |
| builder and fixer on code where a mistake loses or corrupts data | the session's (Opus) | a mistake there loses data |
| every adversarial review, any track | the session's (Opus) | two Opus reviewers caught a power-cut data loss that the fixer and every gate suite missed |

`workflow-template.js` reads `risk` and `models` from each track; `stage-template.js` stages a gate batch on Sonnet. Measure the effect with `tokenstat.py` per model and `usage.sh --rate` before widening it.

## In a workflow script

A stage that can wait returns `wait: {name}`; the script then runs the waiter and a continuation (see `workflow-template.js`). Give every role its own prompt block (the table above, 5-8 lines) rather than one shared blob, and set the waiter's `agentType: 'iso-waiter'`. Keep TASK.md files under ~6 KB with an index at the top; long background goes in a separate file the agent reads only if needed.

## Measured after

All transcripts of the project, relative cost (input 1, cache write 1.25, cache read 0.1, output 5):

| window | hours | cost/hour | cost/request | cache writes share |
|---|---|---|---|---|
| before the skill (~10 tracks) | 29 | 22.8 M | 90.5 k | 76% |
| after (~10-14 tracks, two orchestrators) | 19 | 5.4 M | 18.6 k | 49% |
| after, burst (~15 tracks + a 7-reader audit) | 2 | 25.9 M | 18.5 k | 22% |

A whole track (build + review + fix) cost 27.7-51.9 M before and 4.0-7.4 M after. The cost per request fell ~5x; the burst shows that more parallel work spends the saving again, so the weekly limit still binds at ~15 tracks. The remaining leaks seen in the burst: a builder that never checkpointed (230 calls, 452k context, 6.5 M), a stager launched outside the template (214 calls), and the orchestrator's own long context (5.5 M in two hours).

## Many short agents (measured on a 189-agent audit)

189 agents (7 readers, 182 verifiers), 2764 requests, relative cost 29 M -- and **zero** requests after a 5-minute gap: no cache-expiry waste at all. The cost was volume: each new agent writes its ~41k start context (system prompt, tools, project instructions) once -- ~50k relative each just to begin -- and reads it again on every request (61% of the cost; 94% of tool-result residency was `sed`/`cat` of source files). For a fan-out of small checks: verify adaptively (two skeptics, a third only when they disagree), batch the findings that live in one file into one verifier, and send only findings that could matter (minor and above). That would have cut this audit by roughly half. Use `verifyByFile` from `verify-patterns.js` in every fan-out workflow.

## How to check it worked

`python3 scripts/tokenstat.py <workflow transcript dir...>` (e.g. `~/.claude/projects/<project>/<session>/subagents/workflows`) prints requests, cache writes by gap bucket, relative cost, start and peak context, and the residency of tool results by kind. Compare a wave before and after; the number to watch is "full re-writes" after a 5-30 min gap (should fall to near zero) and total relative cost.
