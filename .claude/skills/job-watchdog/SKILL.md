---
name: job-watchdog
description: Hourly check of tsdb's detached jobs (bgrun.sh runs, the release benchmark, gates): find jobs that died or hang, stop a hung one, fix the cause when it is in our harness, and run it again from where it can resume. Use when a scheduled watchdog fires, or when asked whether a background job is stuck.
---

# Job watchdog

Long jobs run detached (`bgrun.sh NAME CMD`, state in `$SE_HOME/runs/NAME/{cmd,out,pid,start,rc}`, `SE_HOME` =
`~/.local/state/subagent-economy`). Two things have cost hours before: a client waiting forever on a database with
no timeout while holding `BENCH.lock` shared (every exclusive timing waited behind it for 90 minutes), and orphaned
test servers that inherited the lock's descriptor and kept it shared for hours. The watchdog finds such cases early.

## 1. Look

```sh
S=$(git rev-parse --show-toplevel)/.claude/skills/subagent-economy/scripts   # run from inside the tsdb clone
$S/watchdog.sh 45 'tsdb\|dbcmp'   # one line per running job of this project (the release run's cmd names dbcmp, not tsdb)
```

For every line that is not plainly `RUNNING`, gather evidence before deciding anything:

- `tail -5 $SE_HOME/runs/NAME/out`, and for the release benchmark (`bench/dbcmp/release/run.sh`) its own logs:
  `$DBCMP/results/driver.log` (and `steps.log` where a run writes one; default `DBCMP=/var/tmp/dbcmp`), the newest file under
  `$DBCMP/results/p*/`. The release driver writes progress there, not to `out`.
- The process tree of the job (`ps -o pid,etime,stat,wchan:20,args -g PGID`), which child is waiting and on what.
- Containers (`podman ps`): a step waiting on a database shows 0% CPU in the job's own group, because the database's
  CPU is in its container. `podman stats --no-stream NAME` shows whether the database works.
- Known long waits that are NOT hangs: InfluxDB 3 waits about an hour for its Parquet snapshot (it logs a line every
  ~20 s to its `snapshot.txt`), InfluxDB 2 waits 10 minutes for its cache snapshot, Prometheus compacts for hours
  (`prom_settle`), ClickHouse merges after a load, jepsen power runs restart the server ~80 times. The release
  driver has a time limit on every step (`STEP TIMEOUT` in driver.log) and records failures itself: a job inside a
  step's limit is not stuck.
- `WAITING` on BENCH.lock is normal while another job holds it. It is a problem only when the holder is itself
  stalled, or the holder is not a job at all (an orphaned process: `LOCK holder pid ... ()` with no job name).

A job that logs its progress outside its `out` file should get a `watch` file in its run dir listing those files
(one path per line); watchdog.sh then counts the newest of them. Add one when you start such a job.

## 2. Decide

| state | evidence | action |
|---|---|---|
| RUNNING | output or CPU | nothing |
| WAITING | holder is RUNNING | nothing |
| STALLED or a lock holder with no job | no output, no CPU, no progress in its own logs for well past the step's expected time, and no container doing work | stop it (3) |
| DIED | no rc file | find why (out, `journalctl --user`, dmesg for OOM), then rerun (4) if the work is still wanted |

When in doubt, do not stop anything: report the evidence and let the session decide. Never stop a job of another
project (the PATTERN argument keeps them out of the report), a tsdbd you did not trace to a stalled job, or the user's
own processes.

## 3. Stop

Only by the job's own process group, which `bgrun.sh` makes its session leader:

```sh
pg=$(cat $SE_HOME/runs/NAME/pid); [ "$pg" -gt 1 ] && kill -TERM -- -"$pg"
```

An orphaned lock holder (a leftover tsdbd from a killed test, parent pid 1): stop that single pid after checking its
command line names a scratch directory of a finished or dead job. Never `pkill`, `killall`, `kill -1`, `kill 0`, or
pids taken from `pgrep -f`. After stopping, remove the containers that job created (`podman rm -f rb-...`, only the
`rb-*` names of the release driver), and write why into the run dir:
`echo "stopped by the watchdog: REASON" > $SE_HOME/runs/NAME/stopped`.

If the permission check refuses the stop, do not look for another way: report the job, the evidence and the exact
command, so the user can run it.

## 4. Fix and rerun

- If the cause is in our harness (a missing timeout, a wrong path, a check that waits for something that never
  comes), fix it in the harness, commit it on the branch the job runs from (git commit -s, Co-Authored-By line before
  Signed-off-by, no session links), and note it in the job's NOTES.md.
- Rerun with the same command from `$SE_HOME/runs/NAME/cmd` under a new name (`NAME-r2`). The release driver resumes:
  phases and systems that finished are skipped (`FORCE=1` reruns them), so rerunning it continues where it stopped.
  Gates and test jobs rerun from the start.
- If the cause is not ours (a database's own bug), do not rerun blindly: record it as a result where the job's
  results go and tell the session.

## 5. Report

Return at most 8 lines: each job's state, what you did and why. Write the same into
`$SE_HOME/agents/tsdb/watchdog.log` with the time. If nothing needed doing, one line saying so.
