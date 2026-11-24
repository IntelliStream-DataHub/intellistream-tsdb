#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# watchdog.sh [STALL_MIN=45] [PATTERN]: one line per detached job (bgrun.sh) that has no rc yet, and what it is doing:
#   RUNNING  NAME  out Nm ago, cpu X%      it writes output or uses CPU: leave it
#   WAITING  NAME  on BENCH.lock held by PID (JOB) for Nm   a flock waiter: fine unless the holder is STALLED
#   STALLED  NAME  out Nm ago, cpu 0%, child: CMD             no output and no CPU for STALL_MIN minutes
#   DIED     NAME  no process left and no rc                  killed (session end, OOM, reboot)
#   QUEUED   NAME  (build machine) waiting for a slot there   a job of bgrun.sh's build machine (SE_REMOTE) not started yet
# A job on the build machine (rpgid in its run dir) is judged by its process group THERE; "(build machine)" marks it,
# and the orphan check runs there too ("LOOK remote orphan ..."). Stop such a job there by that group (rpgid), then
# its local pid. With a fleet (SE_REMOTES) the job is judged on ITS host (runs/NAME/host: "(build machine HOST)") and the orphan
# check asks every host of the fleet ("LOOK remote orphan ... on HOST": stop it there, ssh HOST kill PID).
# plus the holders of BENCH.lock. "out" means the newest of the job's out file and the files its run dir's `watch` lists. A job someone stopped on purpose gets a file `stopped` (one line: why) in its run
# dir and is skipped; DIED jobs older than a day are skipped too (old runs of this and other projects share $RUNS).
# PATTERN limits the report to jobs whose cmd or working directory matches it (e.g. tsdb), so a watchdog never acts on another project's
# jobs. CPU is counted for the job's own process group only: a job waiting on a container (its CPU is in the
# container's cgroup) shows 0%, so STALLED is a lead to check, not a verdict. The script only reads /proc and the
# run directories; deciding what to do is the job of the watchdog agent (.claude/agents/job-watchdog.md, skill
# .claude/skills/job-watchdog/SKILL.md).
# Also one line per orphaned server, a process named in WATCHDOG_ORPHANS (default tsdbd) whose parent is gone
# (reparented to init, or to the user's systemd as subreaper), that was started with --port-file (what every harness
# passes; a user's own server, as the README's quick start, has a fixed port and is none of the watchdog's business)
# and whose --dir (or --dir=; a relative one taken from the process's cwd) is under /var/tmp or /tmp:
#   LOOK orphan tsdbd pid N (ppid 1, dir D, age T)   the server of a test or benchmark killed without stopping it
# (on 2026-11-05 six ran 17 h holding io_uring rings until jepsen could not create one). Harnesses pass
# --exit-with-parent=PID, so one of these is a harness without it, an old tree, or a server stopping right now.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
STALL_MIN=${1:-45}; PATTERN=${2:-}
L=${BENCH_LOCK:-$SE_HOME/BENCH.lock}

# orphaned harness servers; a function so that the same code also runs on the build machine (SE_REMOTE)
orphans() {
    for name in ${WATCHDOG_ORPHANS-tsdbd}; do
        for p in $(pgrep -u "$(id -u)" -x "$name"); do
            pp=$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' '); [ -n "$pp" ] || continue
            [ "$pp" = 1 ] || [ "$(ps -o comm= -p "$pp" 2>/dev/null)" = systemd ] || continue
            mapfile -d '' -t a 2>/dev/null < "/proc/$p/cmdline" || continue
            d= pf=
            for ((i = 1; i < ${#a[@]}; i++)); do
                case "${a[i]}" in
                --dir) d=${a[i + 1]-} ;;
                --dir=*) d=${a[i]#--dir=} ;;
                --port-file | --port-file=*) pf=1 ;;
                esac
            done
            [ -n "$pf" ] || continue # not a harness's server
            case "$d" in /*) ;; ?*) d=$(readlink "/proc/$p/cwd" 2>/dev/null)/${d#./} ;; esac
            case "$d" in /var/tmp/* | /tmp/*) ;; *) continue ;; esac
            echo "LOOK orphan $name pid $p (ppid $pp, dir $d, age $(ps -o etime= -p "$p" 2>/dev/null | tr -d ' '))"
        done
    done
}
orphans
for h in "${SE_FH[@]}"; do
    # a fleet names the host at the end of the line; one host keeps its line as it was
    suffix=; [ ${#SE_FH[@]} -gt 1 ] && suffix=" on $h"
    { declare -f orphans; echo "WATCHDOG_ORPHANS='${WATCHDOG_ORPHANS-tsdbd}'; orphans"; } |
        ssh -o BatchMode=yes -o ConnectTimeout=10 "$h" bash -s 2>/dev/null | sed "s/^LOOK /LOOK remote /; s/\$/$suffix/"
done
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
    # /proc/locks names the pid that opened the file; once it is gone the lock lives on in whoever inherited the fd
    # (a test's orphaned child kept a shared lock for 8 h this way, 2026-11-07). Name those holders: the flock
    # processes still waiting for it are listed with "->" and are not holders.
    if ! kill -0 "$h" 2>/dev/null; then
        waiters=" $(grep -E ":$ino " /proc/locks 2>/dev/null | grep -- '->' | awk '{print $(NF-3)}' | tr '\n' ' ')"
        heirs=""
        for fd in /proc/[0-9]*/fd/*; do
            [ "$(readlink "$fd" 2>/dev/null)" = "$L" ] || continue
            p=${fd#/proc/}; p=${p%%/*}; case "$waiters" in *" $p "*) continue ;; esac; heirs="$heirs $p"
        done
        heirs=$(printf '%s\n' $heirs | sort -u)
        [ -z "$heirs" ] && { echo "LOCK     holder pid $h exited since (a short step)"; continue; }
        for p in $heirs; do
            echo "LOCK     holder pid $p () inherited the lock of exited $h: $(ps -o etime=,args= -p $p 2>/dev/null | cut -c1-100)"
        done
        continue
    fi
    echo "LOCK     holder pid $h ($(job_of $h)) $(ps -o etime=,args= -p $h 2>/dev/null | cut -c1-100)"
done

for d in "$RUNS"/*/; do
    n=$(basename "$d"); [ -e "$d/rc" ] && continue; [ -e "$d/pid" ] || continue; [ -e "$d/stopped" ] && continue
    pg=$(cat "$d/pid")
    # the command or the directory it runs in (a worktree: tsdb-<track>) names the project
    [ -n "$PATTERN" ] && ! grep -q -- "$PATTERN" "$d/cmd" 2>/dev/null &&
        ! readlink "/proc/$pg/cwd" 2>/dev/null | grep -q -- "$PATTERN" && continue
    np=$(ps -o pid= -g "$pg" 2>/dev/null | wc -l)
    if [ "$np" -eq 0 ]; then [ $((now - $(cat $d/start))) -gt 86400 ] && continue; echo "DIED     $n  no process left and no rc (started $(date -d @$(cat $d/start) +%F_%H:%M))"; continue; fi
    if [ -e "$d/rpgid" ]; then
        # a job on the build machine (se-rjob.sh): the local pid only copies its output back every 15 s, so judge the
        # job's own process group THERE (its CPU over 2 s, its newest process) and its out, which the copy keeps fresh
        rg=$(cat "$d/rpgid"); age=$(( (now - $(stat -c %Y "$d/out" 2>/dev/null || cat "$d/start")) / 60 ))
        rh=$(cat "$d/host" 2>/dev/null); rh=${rh:-$SE_REMOTE}   # a job of before the fleet has no host file
        bm="build machine"; [ ${#SE_FH[@]} -gt 1 ] && bm="build machine $rh"
        if tail -1 "$d/out" 2>/dev/null | grep -q 'waiting for a slot'; then echo "QUEUED   $n  ($bm) waiting for a slot there"; continue; fi
        r=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$rh" "t() { s=0; for p in \$(ps -o pid= -g $rg); do s=\$((s + \$(awk '{print \$14+\$15+\$16+\$17}' /proc/\$p/stat 2>/dev/null || echo 0))); done; echo \$s; }
            n=\$(ps -o pid= -g $rg | wc -l); a=\$(t); sleep 2; b=\$(t); echo \$n \$(( (b - a) * 100 / (2 * \$(getconf CLK_TCK)) ))
            ps -o pid=,etime=,args= -g $rg --sort=start_time | tail -1 | cut -c1-120" 2>/dev/null)
        [ -n "$r" ] || { echo "RUNNING  $n  ($bm, not reachable now) out ${age}m ago"; continue; }
        read -r rn rcpu <<< "$(head -1 <<< "$r")"; leaf=$(sed -n 2p <<< "$r")
        if [ "$rn" -eq 0 ]; then echo "RUNNING  $n  ($bm) its group there has ended; the exit code is on its way back"
        elif [ "$rcpu" -eq 0 ] && [ "$age" -ge "$STALL_MIN" ]; then
            echo "STALLED  $n  ($bm, pgid $rg there) out ${age}m ago, cpu 0%, newest: $leaf"
        else echo "RUNNING  $n  ($bm) out ${age}m ago, cpu ${rcpu}%"; fi
        continue
    fi
    # a job that logs its progress elsewhere names those files, one per line, in $d/watch (the release driver:
    # $DBCMP/results/steps.log); the newest of them and out counts
    newest=$(stat -c %Y "$d/out" 2>/dev/null || cat "$d/start")
    [ -e "$d/watch" ] && while read -r w; do t=$(stat -c %Y "$w" 2>/dev/null) && [ "$t" -gt "$newest" ] && newest=$t
        done < "$d/watch"
    age=$(( (now - newest) / 60 ))
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
