#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Runs the planted bugs (mutants) of one test binary or scenario script in parallel and says which one survived.
#
# Usage: tools/mutants.sh [options] --list "m1 m2 ..." -- CMD ARG...
#
# CMD is run once per mutant, with @M@ replaced by the mutant's name and @D@ by that run's own scratch directory
# (created, empty, under the scratch root; no two runs share one, so tests that write fixed names inside it cannot
# collide). MUTANT_NAME and MUTANT_DIR are also in the environment. Output (stdout and stderr, in one file) goes to the
# run's log. A mutant is CAUGHT when every condition holds; the default condition is "the exit status is not 0".
#   --fail          the exit status is not 0 (the default when no other condition is given)
#   --pass          the exit status is 0 (a loop that expects success)
#   --rc N          the exit status is N
#   --grep RE       a line of the output matches RE (grep -E; repeatable, all must match); alone it ignores the status
#   --verify PROG   `PROG --mutant-verify NAME RC LOGFILE` exits 0; its first output line is shown (for the scenario
#                   tables of tests/*_mutants.sh: each row's own oracle line)
#   --detail RE     show the first line of the output that matches RE after the verdict
#   --stdout-only   stderr is discarded (only stdout is the output)
#   --uring         the runs start tsdbd on io_uring: at most MUTANT_URING_JOBS at once (default 2: the rings of all
#                   of a user's processes count against the 8 MB memlock limit together)
#   --name LABEL    prefix of the lines printed (default: the command's file name)
#   --tmp DIR       scratch root (default $TMPDIR, else /tmp)
#   --jobs N        parallel runs (default $MUTANT_JOBS, else half the CPUs; 1 runs them one after the other)
#   --timeout S     seconds a run may take (default $MUTANT_TIMEOUT, else 1800); a run that hangs is SURVIVED
#                   (timeout) and its process group is killed, never counted as caught
# Several helpers at once (make tsan-par runs the test groups side by side) share one pool when MUTANT_SLOT_DIR names a
# directory (any, created on demand) and MUTANT_SLOTS its size (default: the job limit): no more than that many runs of
# all of them are alive at once (a flock(1) on a file per slot; MUTANT_NO_FLOCK=1 or no flock: a file with the owner's pid, made
# whole by a hard link); a helper that died leaves no slot behind. When neither --jobs nor MUTANT_JOBS is given,
# a pool of that kind, per user ($XDG_RUNTIME_DIR or /tmp, tsdb-mutant-slots-UID) and half the CPUs (MUTANT_SLOTS) wide,
# is used: several worktrees testing at once do not each start half the CPUs.
# A command that cannot be run (status 126 or 127) is SURVIVED too: a missing binary must not "catch" every mutant.
# One line per mutant, in the order they finish. A survivor's log and scratch directory are kept and named; the exit
# status is 1 when any survived (all of them are named at the end, not only the first), 2 for a wrong call.
set -f
list=() cmd=() greps=() label= root= jobs= tmo= verify= detail= conds= want_rc= stdout_only= uring=
die() { echo "mutants.sh: $*" >&2; exit 2; }
while [ $# -gt 0 ]; do
    case $1 in
    --list) [ $# -ge 2 ] || die "--list needs a value"; read -r -a list <<<"$2"; shift 2 ;;
    --fail) conds="$conds fail"; shift ;;
    --pass) conds="$conds pass"; shift ;;
    --rc) [ $# -ge 2 ] || die "--rc needs a value"; conds="$conds rc"; want_rc=$2; shift 2 ;;
    --grep) [ $# -ge 2 ] || die "--grep needs a value"; greps+=("$2"); shift 2 ;;
    --verify) [ $# -ge 2 ] || die "--verify needs a value"; verify=$2; shift 2 ;;
    --detail) [ $# -ge 2 ] || die "--detail needs a value"; detail=$2; shift 2 ;;
    --stdout-only) stdout_only=1; shift ;;
    --uring) uring=1; shift ;;
    --name) [ $# -ge 2 ] || die "--name needs a value"; label=$2; shift 2 ;;
    --tmp) [ $# -ge 2 ] || die "--tmp needs a value"; root=$2; shift 2 ;;
    --jobs) [ $# -ge 2 ] || die "--jobs needs a value"; jobs=$2; shift 2 ;;
    --timeout) [ $# -ge 2 ] || die "--timeout needs a value"; tmo=$2; shift 2 ;;
    --) shift; cmd=("$@"); break ;;
    *) die "unknown option $1" ;;
    esac
done
[ ${#cmd[@]} -gt 0 ] || die "no command after --"
[ ${#list[@]} -gt 0 ] || die "empty mutant list"
[ -n "$label" ] || label=${cmd[0]##*/}
[ -n "$conds" ] || [ ${#greps[@]} -gt 0 ] || [ -n "$verify" ] || conds=" fail"
cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 2)
default_jobs=
if [ -z "$jobs" ] && [ -z "${MUTANT_JOBS:-}" ]; then default_jobs=1; fi
: "${jobs:=${MUTANT_JOBS:-$((cpus / 2))}}"
: "${tmo:=${MUTANT_TIMEOUT:-1800}}"
ujobs=${MUTANT_URING_JOBS:-2}
for v in "$jobs" "$tmo" "$ujobs"; do case $v in '' | *[!0-9]*) die "not a number: '$v'" ;; esac; done
[ "$jobs" -ge 1 ] || jobs=1
[ "$ujobs" -ge 1 ] || ujobs=1
[ -z "$uring" ] || [ "$jobs" -le "$ujobs" ] || jobs=$ujobs
: "${root:=${TMPDIR:-/tmp}}"
mkdir -p "$root" || exit 2
work=$(mktemp -d "$root/mut.XXXXXX") || exit 2

# a run's directory name: the mutant's name with anything odd (20@3000, 11,13, a/b) made safe
safe() { printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'; }
n=${#list[@]}
dirs=() pids=() starts=() state=() seen=" "
for ((i = 0; i < n; i++)); do
    s=$(safe "${list[$i]}")
    case $seen in *" $s "*) die "mutant ${list[$i]} is listed twice" ;; esac
    seen="$seen$s "
    dirs[$i]=$work/$s
    state[$i]=wait # wait, run, done
done

caught=0 survivors=() slot=()
slots=${MUTANT_SLOTS:-$jobs}
slot_dir=${MUTANT_SLOT_DIR:-}
if [ -z "$slot_dir" ] && [ -n "$default_jobs" ]; then
    # Nobody chose a job count: the helpers of all worktrees of this user (make test in several at once) share one pool,
    # half the CPUs in all, instead of half the CPUs each. A MUTANT_JOBS or --jobs is the caller's own decision.
    slot_dir=${XDG_RUNTIME_DIR:-/tmp}/tsdb-mutant-slots-$(id -u)
    slots=${MUTANT_SLOTS:-$jobs} # jobs is cpus / 2 here (at least 1)
fi
case $slots in '' | *[!0-9]*) die "not a number: '$slots'" ;; esac
if [ -n "$slot_dir" ]; then mkdir -p "$slot_dir" || exit 2; fi
# The pool. With flock(1) a slot is the file $slot_dir/slot.K and the owner holds an flock on an open descriptor of it:
# the kernel lets go when the helper ends, however it ends, so there is no stale slot and nothing to take over. Never
# unlink slot.K (a lock file that goes can be held twice). mkdir is no lock here: uutils' mkdir (Ubuntu 26.04's
# coreutils) answers 0 to two callers that race for one name, and two helpers then each ran a job in the one slot of a
# pool (test_mutants_helper, section 14). Without flock (macOS, or MUTANT_NO_FLOCK=1: the test of this path) a slot is
# the file nf.K (another name, so a pool is never shared between the two ways) holding the owner's pid, made whole by a
# hard link (ln fails when the name exists; no moment with an empty file). A slot whose owner is gone is removed by one
# reaper at a time (the O_EXCL file nf.K.reap, taken with the shell's set -C), which reads nf.K again under it and
# removes it only if it still names that dead owner: nothing is ever renamed away or put back, so a live owner's file
# is never touched. A reaper that died holding nf.K.reap is told by its pid and its file removed (best effort).
use_flock=1
if [ -n "${MUTANT_NO_FLOCK:-}" ] || ! command -v flock >/dev/null 2>&1; then use_flock=; fi
slotfd=()
take_slot() { # index: 0 when a slot of the shared pool is ours (or there is no pool)
    [ -n "$slot_dir" ] || return 0
    local k f fd p c t q
    t=$slot_dir/nf.new.$$
    [ -n "$use_flock" ] || echo $$ >"$t" 2>/dev/null || return 1
    for ((k = 1; k <= slots; k++)); do
        if [ -n "$use_flock" ]; then
            f=$slot_dir/slot.$k
            { exec {fd}>>"$f"; } 2>/dev/null || continue
            if flock -n "$fd"; then
                slotfd[$1]=$fd slot[$1]=$k
                return 0
            fi
            exec {fd}>&-
            continue
        fi
        f=$slot_dir/nf.$k
        if ln "$t" "$f" 2>/dev/null; then
            rm -f "$t"
            slot[$1]=$k
            return 0
        fi
        p=$(cat "$f" 2>/dev/null)
        case $p in '' | *[!0-9]*) continue ;; esac
        kill -0 "$p" 2>/dev/null && continue
        # its owner is gone (killed before it could give it back)
        if (set -C; echo $$ >"$f.reap") 2>/dev/null; then
            c=$(cat "$f" 2>/dev/null)
            [ "$c" != "$p" ] || rm -f "$f"
            rm -f "$f.reap"
        else
            q=$(cat "$f.reap" 2>/dev/null)
            case $q in '' | *[!0-9]*) ;; *) kill -0 "$q" 2>/dev/null || rm -f "$f.reap" ;; esac
        fi
    done
    [ -n "$use_flock" ] || rm -f "$t"
    return 1
}
give_slot() {
    [ -n "${slot[$1]:-}" ] || return 0
    if [ -n "$use_flock" ]; then
        local fd=${slotfd[$1]}
        exec {fd}>&-
    else rm -f "${slot_dir:?}/nf.${slot[$1]}"; fi
    slot[$1]= slotfd[$1]=
}
kill_group() { # a numeric pid above 1 only: kill(-1) and kill(0) would reach the user's whole session
    case $1 in '' | *[!0-9]*) return ;; esac
    [ "$1" -gt 1 ] || return
    kill -TERM -- "-$1" 2>/dev/null
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
        kill -0 "$1" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -KILL -- "-$1" 2>/dev/null
}
cleanup() {
    for ((i = 0; i < n; i++)); do
        [ "${state[$i]}" = run ] && kill_group "${pids[$i]}"
        [ -z "$slot_dir" ] || give_slot "$i"
    done
}
trap 'cleanup; exit 130' INT TERM
trap 'cleanup' EXIT

launch() { # index
    local i=$1 name=${list[$1]} d=${dirs[$1]} a args=()
    mkdir -p "$d/s" || exit 2
    for a in "${cmd[@]}"; do
        a=${a//@M@/$name}
        args+=("${a//@D@/$d/s}")
    done
    set -m # a process group of its own for the run, so that a hang can be killed with its children
    (
        set +m
        # the run must not hold the pool's locks: a slot is free when the run is over, whatever it left behind
        for a in "${slotfd[@]}"; do [ -z "$a" ] || { exec {a}>&-; }; done
        export MUTANT_NAME=$name MUTANT_DIR=$d/s
        # A SIGKILL of the helper runs no trap, and the run is in a group of its own, so nothing else would stop it:
        # a watcher in the run's group kills the group once the helper is gone (and is stopped when the run ends).
        me=$BASHPID helper=$$
        (
            while kill -0 "$helper" 2>/dev/null; do sleep 1; done
            [ "$me" -gt 1 ] && kill -KILL -- "-$me" 2>/dev/null
        ) >/dev/null 2>&1 </dev/null &
        watcher=$!
        if [ -n "$stdout_only" ]; then "${args[@]}" >"$d/out.log" 2>/dev/null </dev/null
        else "${args[@]}" >"$d/out.log" 2>&1 </dev/null; fi
        echo $? >"$d/rc.tmp"
        kill "$watcher" 2>/dev/null
        mv "$d/rc.tmp" "$d/rc"
    ) &
    pids[$i]=$!
    set +m
    starts[$i]=$SECONDS
    state[$i]=run
}

# verdict of a finished run: sets ok (1 caught) and why
judge() { # index rc
    local i=$1 rc=$2 d=${dirs[$1]} g c vo
    ok=1 why=
    if [ "$rc" = timeout ]; then ok=0 why="timeout after ${tmo} s"; return; fi
    if [ "$rc" = died ]; then ok=0 why="the run ended without a status"; return; fi
    if [ "$rc" = 126 ] || [ "$rc" = 127 ]; then
        case $conds in *" rc"*) [ "$rc" = "$want_rc" ] || { ok=0 why="the command could not be run (status $rc)"; return; } ;;
        *) ok=0 why="the command could not be run (status $rc)"; return ;; esac
    fi
    for c in $conds; do
        case $c in
        fail) [ "$rc" != 0 ] || { ok=0 why="exit status 0"; } ;;
        pass) [ "$rc" = 0 ] || { ok=0 why="exit status $rc"; } ;;
        rc) [ "$rc" = "$want_rc" ] || { ok=0 why="exit status $rc, not $want_rc"; } ;;
        esac
    done
    for g in "${greps[@]}"; do
        grep -Eq -- "$g" "$d/out.log" || { ok=0 why="${why:+$why; }no line matching /$g/"; }
    done
    if [ -n "$verify" ]; then
        vo=$("$verify" --mutant-verify "${list[$i]}" "$rc" "$d/out.log" 2>&1)
        [ $? -eq 0 ] || { ok=0; why="${why:+$why; }not caught by its own line"; }
        [ -z "$vo" ] || why="${why:+$why: }$(printf '%s' "$vo" | head -n 1 | cut -c1-300)"
        [ "$ok" = 0 ] || why=$(printf '%s' "$vo" | head -n 1 | cut -c1-300)
    fi
    if [ "$ok" = 1 ] && [ -n "$detail" ]; then
        why=$(grep -E -m1 -- "$detail" "$d/out.log" | cut -c1-300)
    fi
}

finish() { # index rc
    local i=$1 name=${list[$1]} d=${dirs[$1]} secs=$((SECONDS - ${starts[$1]}))
    state[$i]=done
    give_slot "$i"
    judge "$i" "$2"
    if [ "$ok" = 1 ]; then
        echo "$label: mutant $name: caught (${secs} s)${why:+: $why}"
        caught=$((caught + 1))
        rm -rf "$d"
    else
        echo "$label: mutant $name: SURVIVED (${secs} s): $why; log $d/out.log, scratch $d/s"
        survivors+=("$name")
    fi
}

running=0 next=0 left=$n
while [ "$left" -gt 0 ]; do
    while [ "$running" -lt "$jobs" ] && [ "$next" -lt "$n" ] && take_slot "$next"; do
        launch "$next"
        next=$((next + 1))
        running=$((running + 1))
    done
    progressed=0
    for ((i = 0; i < next; i++)); do
        [ "${state[$i]}" = run ] || continue
        if [ -f "${dirs[$i]}/rc" ]; then
            wait "${pids[$i]}" 2>/dev/null
            rc=$(cat "${dirs[$i]}/rc")
        elif [ $((SECONDS - ${starts[$i]})) -ge "$tmo" ]; then
            kill_group "${pids[$i]}"
            wait "${pids[$i]}" 2>/dev/null
            rc=timeout
        elif ! kill -0 "${pids[$i]}" 2>/dev/null; then
            [ -f "${dirs[$i]}/rc" ] || { sleep 0.2; [ -f "${dirs[$i]}/rc" ] || { rc=died; }; }
            [ "${rc:-}" = died ] || continue
            kill_group "${pids[$i]}" # what is left of its group (the watcher)
        else
            continue
        fi
        finish "$i" "$rc"
        unset rc
        running=$((running - 1))
        left=$((left - 1))
        progressed=1
    done
    [ "$progressed" = 1 ] || sleep 0.1
done

trap - EXIT
if [ ${#survivors[@]} -eq 0 ]; then
    rmdir "$work" 2>/dev/null
    echo "$label: all $n mutants caught"
    exit 0
fi
echo "$label: ${#survivors[@]} of $n mutants SURVIVED: ${survivors[*]}"
exit 1
