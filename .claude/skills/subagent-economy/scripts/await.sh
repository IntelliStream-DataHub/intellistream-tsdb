#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# await.sh NAME [MAX_SECONDS=540]: block up to MAX_SECONDS (keep it under the 10 minute tool cap) for a bgrun.sh job.
# Prints ONE short status, never the whole output. Exit 0 = finished (rc printed), 10 = still running, 11 = unknown/died.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
n=$1; max=${2:-540}; se_name_ok "$n" 11; d=$RUNS/$n
[ -d "$d" ] || { echo "await: no job $n in $RUNS"; exit 11; }
t0=$(date +%s)
while [ ! -e $d/rc ] && [ $(( $(date +%s) - t0 )) -lt "$max" ]; do
  kill -0 "$(cat $d/pid 2>/dev/null)" 2>/dev/null || [ -e $d/rc ] || { sleep 2; [ -e $d/rc ] || { echo "await: $n died without an exit code; last lines:"; tail -n 15 $d/out; exit 11; }; }
  timeout 5 tail -f /dev/null
done
# The usage gate (slow down past 50% of the Claude Code 5-hour window, do not stop): a finished job is reported -- and
# the agent that judges it started -- at most once per gap across every waiter: 4 min apart at >= USAGE_GATE (50%),
# 15 min at >= 75%, and not at all at >= 90% until the window recovers. Past 95% of the WEEK, one release per 30 min
# whatever the 5-hour window says. Below the gate (or with no fresh reading) nothing waits. A held waiter costs one
# cheap call every 9 minutes.
G=$SE_BIN/usage.sh
S=$SE_HOME/state/usage-release
gap() { if "$G" --gate 90; then echo hold; elif "$G" --wgate 95; then echo 1800; elif "$G" --gate 75; then echo 900; elif "$G" --gate "${USAGE_GATE:-50}"; then echo 240; else echo 0; fi; }
if [ -e $d/rc ]; then
  while :; do
    g=$(gap)
    [ "$g" = 0 ] && break
    if [ "$g" != hold ]; then
      # Take the next release slot if the last one is at least $g seconds old (one waiter at a time).
      mkdir -p "${S%/*}"
      got=$(flock "$S.lock" sh -c 'l=$(cat "$1" 2>/dev/null || echo 0); n=$(date +%s); if [ $((n - l)) -ge "$2" ]; then echo $n > "$1"; echo yes; fi' _ "$S" "$g")
      [ "$got" = yes ] && break
    fi
    if [ $(( $(date +%s) - t0 )) -ge "$max" ]; then
      echo "RUNNING $n: finished (rc=$(cat $d/rc)) but HELD by the usage gate ($g): $("$G")"; exit 10
    fi
    timeout 20 tail -f /dev/null
  done
fi
el=$(( ($(date +%s) - $(cat $d/start)) / 60 ))
if [ -e $d/rc ]; then
  echo "DONE $n rc=$(cat $d/rc) after ${el} min; output $d/out ($(wc -l < $d/out) lines); last 12:"; tail -n 12 $d/out | cut -c1-200; exit 0
fi
echo "RUNNING $n ${el} min; output $(wc -l < $d/out) lines; last line: $(tail -n 1 $d/out | cut -c1-160)"; exit 10
