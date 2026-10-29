#!/bin/bash
# watchdog.sh [STALL_MIN=45] [PATTERN]: one line per detached job (bgrun.sh) that has no rc yet, and what it is doing:
#   RUNNING  NAME  out Nm ago, cpu X%      it writes output or uses CPU: leave it
#   WAITING  NAME  on BENCH.lock held by PID (JOB) for Nm   a flock waiter: fine unless the holder is STALLED
#   STALLED  NAME  out Nm ago, cpu 0%, child: CMD             no output and no CPU for STALL_MIN minutes
#   DIED     NAME  no process left and no rc                  killed (session end, OOM, reboot)
# plus the holders of BENCH.lock. A job someone stopped on purpose gets a file `stopped` (one line: why) in its run
# dir and is skipped; DIED jobs older than a day are skipped too (old runs of this and other projects share $RUNS).
# PATTERN limits the report to jobs whose cmd matches it (e.g. tsdb), so a watchdog never acts on another project's
# jobs. CPU is counted for the job's own process group only: a job waiting on a container (its CPU is in the
# container's cgroup) shows 0%, so STALLED is a lead to check, not a verdict. The script only reads /proc and the
# run directories; deciding what to do is the job of the watchdog agent (.claude/agents/job-watchdog.md, skill
# .claude/skills/job-watchdog/SKILL.md).
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
STALL_MIN=${1:-45}; PATTERN=${2:-}
L=${BENCH_LOCK:-$SE_HOME/BENCH.lock}
now=$(date +%s)
ticks=$(getconf CLK_TCK)

# CPU ticks of every process in a process group
grp_ticks() { local s=0 p; for p in $(ps -o pid= -g "$1" 2>/dev/null); do
    s=$((s + $(awk '{print $14+$15+$16+$17}' /proc/$p/stat 2>/dev/null || echo 0))); done; echo $s; }

# which job (run dir) a pid belongs to: walk up its parents (timeout(1) and setsid give steps their own process
# groups) until a process group or pid named in a job's pid file
job_of() { local p=$1 g f i; for i in $(seq 1 30); do [ -n "$p" ] && [ "$p" -gt 1 ] || return
        g=$(ps -o pgid= -p "$p" 2>/dev/null | tr -d ' '); [ -n "$g" ] || return
        f=$(grep -lx -e "$g" -e "$p" "$RUNS"/*/pid 2>/dev/null | head -1)
        [ -n "$f" ] && { basename "$(dirname "$f")"; return; }
        p=$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' '); done; }

ino=$(stat -c %i "$L" 2>/dev/null)
holders=$(grep -E ":$ino " /proc/locks 2>/dev/null | grep -v -- '->' | awk '{print $(NF-3)}' | sort -u)
for h in $holders; do
    # a step that took the lock for a moment may be gone by now: not an orphan
    if ! kill -0 "$h" 2>/dev/null; then echo "LOCK     holder pid $h exited since (a short step)"; continue; fi
    echo "LOCK     holder pid $h ($(job_of $h)) $(ps -o etime=,args= -p $h 2>/dev/null | cut -c1-100)"
done

for d in "$RUNS"/*/; do
    n=$(basename "$d"); [ -e "$d/rc" ] && continue; [ -e "$d/pid" ] || continue; [ -e "$d/stopped" ] && continue
    [ -n "$PATTERN" ] && ! grep -q -- "$PATTERN" "$d/cmd" 2>/dev/null && continue
    pg=$(cat "$d/pid")
    np=$(ps -o pid= -g "$pg" 2>/dev/null | wc -l)
    if [ "$np" -eq 0 ]; then [ $((now - $(cat $d/start))) -gt 86400 ] && continue; echo "DIED     $n  no process left and no rc (started $(date -d @$(cat $d/start) +%F_%H:%M))"; continue; fi
    age=$(( (now - $(stat -c %Y "$d/out" 2>/dev/null || cat "$d/start")) / 60 ))
    t0=$(grp_ticks "$pg"); sleep 2; t1=$(grp_ticks "$pg")
    cpu=$(( (t1 - t0) * 100 / (2 * ticks) ))
    waiter=$(for p in $(ps -o pid= -g "$pg"); do ps -o args= -p $p | grep -q '^flock' && grep -E ":$ino " /proc/locks | grep -q -- "-> .* $p " && echo $p; done | head -1)
    if [ -n "$waiter" ]; then
        h=$(echo $holders | awk '{print $1}')
        echo "WAITING  $n  on BENCH.lock (waiter $waiter) held by ${h:-?} ($( [ -n "$h" ] && job_of $h)), out ${age}m ago"
    elif [ "$cpu" -eq 0 ] && [ "$age" -ge "$STALL_MIN" ]; then
        leaf=$(ps -o pid=,etime=,args= -g "$pg" --sort=start_time | tail -1 | cut -c1-120)
        echo "STALLED  $n  out ${age}m ago, cpu 0%, newest child: $leaf"
    else
        echo "RUNNING  $n  out ${age}m ago, cpu ${cpu}%"
    fi
done
