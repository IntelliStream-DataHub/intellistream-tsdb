#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# bgrun.sh NAME CMD...: start CMD detached and return at once. Output -> $RUNS/NAME/out, exit code -> $RUNS/NAME/rc
# (written last). Check with await.sh NAME. When SE_QUEUE names a queue wrapper (waits for a free slot on the machine,
# then execs its arguments) CMD runs behind it; BGRUN_NOWAIT=1 skips the queue for light commands.
# A long test must never be waited on inside a big agent context: after 5 minutes without a request the
# prompt cache is gone and the NEXT request re-writes the whole context (measured: 92% of all cache writes).
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
n=$1; shift
[ -n "$n" ] && [ $# -gt 0 ] || { echo "usage: bgrun.sh NAME CMD..." >&2; exit 2; }
se_name_ok "$n"
d=$RUNS/$n; [ -e $d/pid ] && kill -0 "$(cat $d/pid)" 2>/dev/null && { echo "bgrun: $n is already running (pid $(cat $d/pid))" >&2; exit 3; }
command rm -rf "$d"; mkdir -p "$d"; date +%s > $d/start; printf '%s\n' "$*" > $d/cmd
W=$SE_QUEUE; [ -n "$BGRUN_NOWAIT" ] && W=
nohup setsid bash -c 'W=$1; shift; if [ -n "$W" ]; then "$W" "$@"; else "$@"; fi > "$0/out" 2>&1; echo $? > "$0/rc.tmp"; mv "$0/rc.tmp" "$0/rc"' "$d" "$W" "$@" > /dev/null 2>&1 &
echo $! > $d/pid
echo "started $n (pid $!). Do NOT wait here. Write the check into NOTES.md and end your stage with wait={name:\"$n\"}; or, if you are the waiter: await.sh $n"
