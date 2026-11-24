#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# bgrun.sh NAME CMD...: start CMD detached and return at once. Output -> $RUNS/NAME/out, exit code -> $RUNS/NAME/rc
# (written last). Check with await.sh NAME. When SE_QUEUE names a queue wrapper (waits for a free slot on the machine,
# then execs its arguments) CMD runs behind it; BGRUN_NOWAIT=1 skips the queue for light commands.
# A long test must never be waited on inside a big agent context: after 5 minutes without a request the
# prompt cache is gone and the NEXT request re-writes the whole context (measured: 92% of all cache writes).
# With SE_REMOTE set in config.sh the job runs on that build machine (se-remote.sh); BGRUN_LOCAL=1 keeps it here.
# With SE_REMOTES (a fleet) it runs on the first host, in config order, that has a free slot (none: the first host's
# queue), recorded in $RUNS/NAME/host; BGRUN_TAG=nvme or SE_HOST=user@hostA limits the choice to hosts with that tag or that host.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
n=$1; shift
[ -n "$n" ] && [ $# -gt 0 ] || { echo "usage: bgrun.sh NAME CMD..." >&2; exit 2; }
se_name_ok "$n"
d=$RUNS/$n; [ -e $d/pid ] && kill -0 "$(cat $d/pid)" 2>/dev/null && { echo "bgrun: $n is already running (pid $(cat $d/pid))" >&2; exit 3; }
command rm -rf "$d"; mkdir -p "$d"; date +%s > $d/start; printf '%s\n' "$*" > $d/cmd
# SE_REMOTES set and no entry valid: refuse, never quietly run HERE (un-niced) what was meant for the build machines.
if [ -n "${SE_REMOTES:-}" ] && ! se_remote_on && [ -z "$BGRUN_LOCAL" ]; then
  echo "bgrun: SE_REMOTES in $SE_HOME/config.sh has no valid entry (want user@host:SLOTS[:tag...]); BGRUN_LOCAL=1 runs here" >&2; echo 2 > $d/rc; exit 2
fi
# With a build machine (SE_REMOTE or SE_REMOTES in config.sh, se-remote.sh) the job runs THERE unless BGRUN_LOCAL=1.
if se_remote_on && [ -z "$BGRUN_LOCAL" ]; then
  . "$SE_BIN/se-remote.sh"
  # the pick and the note of it are one step: a job started right after sees this one's claim on the slot
  # (only when the choice needs a probe: a job with one fitting host or no slot to take has nothing to race for; the wait for the
  # lock is bounded, a probe under it is too, so one hung host cannot stall every job start)
  mkdir -p "$SE_HOME/state"; exec 7> "$SE_HOME/state/pick.lock"
  if [ -z "$BGRUN_NOWAIT" ] && [ "$(se_cands "$PWD" | wc -l)" -gt 1 ]; then
    flock -w 60 7 || echo "bgrun: the pick lock was busy for 60 s; picking without it" >&2
  fi
  host=$(se_pick_host "$PWD"); prc=$?
  [ $prc -ne 2 ] && [ -n "$host" ] && printf '%s\n' "$host" > $d/host
  [ -n "$BGRUN_NOWAIT" ] && : > $d/nowait
  flock -u 7; exec 7>&-
  [ $prc -ne 2 ] && [ -n "$host" ] || { echo 2 > $d/rc; exit 2; }
  se_use_host "$host" || { echo 2 > $d/rc; exit 2; }
  printf '%s\n' "$PWD" > $d/cwd; se_cwd_note "$PWD" "$host"
  roots=$(rs_job_roots "$PWD" "$*") || { echo 2 > $d/rc; exit 2; }
  RJ_N=$n RJ_D=$d RJ_HOST=$host RJ_CWD=$PWD RJ_ROOTS=$(echo $roots) RJ_NOWAIT=$BGRUN_NOWAIT nohup setsid bash "$SE_BIN/se-rjob.sh" "$@" > /dev/null 2>&1 &
  echo $! > $d/pid
  [ $prc -eq 1 ] && echo "bgrun: no build host has a free slot now: $n waits in the queue of $host" >&2
  echo "started $n on the build machine${host:+ $host} (pid $!; it copies $(echo $roots | wc -w) tree(s) there, files it writes come back, build dirs stay there: rx.sh reads them). Do NOT wait here. Write the check into NOTES.md and end your stage with wait={name:\"$n\"}; or, if you are the waiter: await.sh $n"
  exit 0
fi
W=$SE_QUEUE; [ -n "$BGRUN_NOWAIT" ] && W=
nohup setsid bash -c 'W=$1; shift; if [ -n "$W" ]; then "$W" "$@"; else "$@"; fi > "$0/out" 2>&1; echo $? > "$0/rc.tmp"; mv "$0/rc.tmp" "$0/rc"' "$d" "$W" "$@" > /dev/null 2>&1 &
echo $! > $d/pid
echo "started $n (pid $!). Do NOT wait here. Write the check into NOTES.md and end your stage with wait={name:\"$n\"}; or, if you are the waiter: await.sh $n"
