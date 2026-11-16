#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that dies by itself, or stops answering, must be a VIOLATION (exit 1) on the attempt where it happened, under every nemesis
# and under none (the user's scope of 2026-11-14: a server that hangs or dies other than by the planned kill or power loss is found
# and reported, never hidden behind INCONCLUSIVE, a rerun or a pass; round 11). Builds, from a copy of the tree (git archive of HEAD,
# under $TMPDIR or /var/tmp; the working tree and the shipped tsdbd are never touched), a tsdbd with two test-only mutants:
#   JV_CRASH=N   abort() on every Nth insert request (a crash of its own: SIGABRT), with the core dump the machine takes (RLIMIT_CORE is
#                left alone: a core handler that is a pipe, systemd-coredump here, dumps whatever the limit says, and the dump keeps the
#                process alive, not yet a zombie, for 0.1 s to seconds: the window in which a nemesis' SIGKILL used to hide the crash)
#   JV_EXIT=N    _exit(JV_EXITCODE, 0 by default) on every Nth insert request (an end of its own, with no signal of ours)
#   JV_DSTALL=N  a worker blocks in D state for good (vfork whose child never execs) on every Nth insert request: the kernel's stall, as an I/O stall
#                is, and not tsdbd's hang (JV_HANG: pause(), no D state)
#   JV_CRASHONCE=N  the same, once per gate (JV_ONCEFILE names the file whose creation says it happened)
#   JV_CRASH_MB=M   a thread of the server touches M MB from its first insert on, in the background: the abort then dumps a core of up to that
#                size, a dump that takes long on every core handler (touching before the abort made a window in which the nemesis killed
#                the server before it aborted, and under 16 busy loops the abort then never happened)
#   JV_HANG=N    a worker pause()s for ever on every Nth insert request (the server is up and stops answering)
# and runs them through tests/jepsen/rerun.sh:
#   crash under kill, pause, none, eio and power: exit 1 at the FIRST attempt (no "jepsen-rerun: attempt" line, never 0 or 3), naming the
#     signal and the server's log;
#   hang under kill, pause, none, eio, power and outage, with a deadline of 8 s for a request (JEPSEN_REQ_DEADLINE_S; the default is 60):
#     exit 1 at the first attempt with "server stopped answering", within the run's time plus the deadline plus a bound (timeout), and no
#     tsdbd of the mutant left behind (looked up by its executable in /proc, never by name: no pkill -f).
# On d52d0df the crash passed under kill (exit 0) and ended INCONCLUSIVE (exit 3) under pause and eio, and the hang made the harness wait
# for ever (every case ended at the timeout).
# Usage: tests/jepsen/mutant-crash.sh JEPSEN_DIR [JEPSEN_BIN]   (CASES="crash-kill hang-none" runs only those)
set -u
dir=${1:?usage: mutant-crash.sh JEPSEN_DIR [JEPSEN_BIN]}
bin=${2:-}
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
[ -n "$bin" ] || bin=$root/build/jepsen
tmp=$(mktemp -d "${TMPDIR:-/var/tmp}/mutant-crash.XXXXXX") || exit 2
git -C "$root" archive HEAD | tar -x -C "$tmp" || exit 2
f=$tmp/server/tsdbd.c
sed -i '0,/^#include <fcntl.h>$/s//#include <fcntl.h>\
#include <sys\/prctl.h>\
static _Atomic unsigned jv_cnt; \/* test-only mutants of tests\/jepsen\/mutant-crash.sh *\/\
static void *jv_touch(void *a);\
static int jv_every(const char *k)\
{\
    const char *e = getenv(k);\
    int n = e ? atoi(e) : 0;\
    static _Atomic int toucher;\
    if (n > 0 \&\& getenv("JV_CRASH_MB") \&\& !atomic_exchange(\&toucher, 1)) {\
        pthread_t th;\
        if (pthread_create(\&th, NULL, jv_touch, (void *)(size_t)atol(getenv("JV_CRASH_MB"))) == 0)\
            pthread_detach(th);\
    }\
    return n > 0 \&\& atomic_fetch_add(\&jv_cnt, 1) % (unsigned)n == (unsigned)n - 1;\
}\
static void *jv_touch(void *a)\
{\
    size_t mb = (size_t)a;\
    volatile char *p = malloc(mb << 20);\
    for (size_t i = 0; p \&\& i < (mb << 20); i += 4096)\
        p[i] = 1;\
    return NULL;\
}\
static void jv_crash(void)\
{\
    abort();\
}\
static void jv_dstall(void)\
{\
    if (vfork() == 0) {\
        prctl(PR_SET_PDEATHSIG, SIGKILL);\
        for (;;)\
            pause();\
    }\
}\
static void jv_once(void)\
{\
    const char *f = getenv("JV_ONCEFILE");\
    int fd = f ? open(f, O_WRONLY | O_CREAT | O_EXCL, 0644) : -1;\
    if (fd >= 0)\
        jv_crash();\
}/' "$f"
sed -i 's/^    case TSDB_OP_INSERT: {$/&\
        if (jv_every("JV_CRASH")) jv_crash();\
        if (jv_every("JV_CRASHONCE")) jv_once();\
        if (jv_every("JV_EXIT")) _exit(atoi(getenv("JV_EXITCODE") ? getenv("JV_EXITCODE") : "0"));\
        if (jv_every("JV_DSTALL")) jv_dstall();\
        if (jv_every("JV_HANG")) for (;;) pause();/; s/^    case TSDB_OP_INSERT_VALUES: {$/&\
        if (jv_every("JV_CRASH")) jv_crash();\
        if (jv_every("JV_CRASHONCE")) jv_once();\
        if (jv_every("JV_EXIT")) _exit(atoi(getenv("JV_EXITCODE") ? getenv("JV_EXITCODE") : "0"));\
        if (jv_every("JV_DSTALL")) jv_dstall();\
        if (jv_every("JV_HANG")) for (;;) pause();/' "$f"
if [ "$(grep -c 'jv_every(' "$f")" -ne 11 ]; then echo "mutant-crash: the hooks did not apply"; rm -rf "$tmp"; exit 1; fi
(cd "$tmp" && make -j8 build/tsdbd >"$tmp/build.log" 2>&1) || { echo "mutant-crash: build failed"; tail -5 "$tmp/build.log"; rm -rf "$tmp"; exit 1; }
mut=$tmp/build/tsdbd
crashfs=$root/build/libcrashfs.so
outagefs=$root/build/liboutagefs.so

# the pids of tsdbd processes of the mutant that are still alive (by executable, from /proc)
leftover() {
    local p
    for p in /proc/[0-9]*; do
        [ "$(readlink "$p/exe" 2>/dev/null)" = "$mut" ] && echo "${p#/proc/}"
    done
}

rc=0
want() { [ -z "${CASES:-}" ] || [[ " $CASES " == *" $1 "* ]]; }
run_case() { # name pattern what envs... -- jepsen args...
    local name=$1 pat=$2 what=$3; shift 3
    local envs=() t0 t1 out st left
    while [ "$1" != -- ]; do envs+=("$1"); shift; done
    shift
    want "$name" || return 0
    t0=$(date +%s)
    out=$(env "${envs[@]}" JEPSEN_TSDBD="$mut" timeout "${TMO:-200}" "$here/rerun.sh" "$bin" --dir "$dir" "$@" 2>&1); st=$?
    t1=$(date +%s)
    left=$(leftover | tr '\n' ' ')
    if [ $st -eq 1 ] && ! echo "$out" | grep -q 'jepsen-rerun: attempt' && echo "$out" | grep -q "$pat" && [ -z "$left" ]; then
        echo "mutant-crash: $name caught at the first attempt in $((t1 - t0)) s: $what"
    else
        echo "mutant-crash: $name: '$what' went unnoticed, was rerun, hid behind INCONCLUSIVE, hung or left a server (rc $st; left behind: ${left:-none}; $((t1 - t0)) s; wanted exit 1 at the first attempt and '$pat')"
        echo "$out" | tail -n 15
        rc=1
    fi
    for p in $left; do # by pid, and only what this case left
        kill -KILL "$p" 2>/dev/null
    done
}
# the crash is told either as the end it had ('ended with status ... signal 6') or, when the harness' own SIGKILL met the server writing its core
# (every nemesis but none: a stop that follows a crash by a few ms), as crash_check says it ('was crashing when the harness killed it')
CR='ended with status .* (signal 6 (Aborted).*by itself\|was crashing when the harness killed it'
for nem in kill pause none; do
    run_case "crash-$nem" "$CR" "a server that aborts on every 40th insert, $nem nemesis" JV_CRASH=40 -- --nemesis "$nem" --seconds 12
done
for nem in eio power; do
    run_case "crash-$nem" "$CR" "a server that aborts on every 40th insert, $nem nemesis" JV_CRASH=40 -- --nemesis "$nem" --seconds 12 \
        --crashfs "$crashfs"
done
# one abort per gate, with a core dump of up to CRASH_MB (512 by default) on the core handler of the machine: the kill
# nemesis' SIGKILL meets a server that is not a zombie yet. Under kv and under classes the kill nemesis hid it
# before round 12 (7 of 10 kv kill gates passed VALID at load 125, 3 of 6 quiet; VERIFY11 (A)).
for wl in kv classes; do
    for i in 1 2 3; do
        rm -f "$tmp/once-$wl-$i"
        run_case "crashonce-$wl-kill-$i" "$CR" \
            "one abort per gate with a core dump (the machine's core handler), kill nemesis, $wl workload" JV_CRASHONCE=${CRASH_ONCE_N:-20} \
            JV_CRASH_MB=${CRASH_MB:-512} JV_ONCEFILE="$tmp/once-$wl-$i" -- --nemesis kill --workload $wl --seconds 12
    done
done
# a server that exits by itself (status 0, or 1 with no fault in the epoch) under misdirect (with and without verify_checksums) and rot is
# no fail-stop on damage (round 12; VERIFY11 (A): rc 0 in all six)
for code in 0 1; do
    run_case "exit$code-md" "ended with exit status\\|ended with a status of its own\\|by itself: not a fail-stop" "a server that exits $code by itself, misdirect" JV_EXIT=150 \
        JV_EXITCODE=$code -- --nemesis misdirect --seconds 20 --crashfs "$crashfs"
    run_case "exit$code-mdv" "by itself: not a fail-stop" "a server that exits $code by itself, misdirect, verify_checksums" JV_EXIT=150 JV_EXITCODE=$code -- \
        --nemesis misdirect --seconds 20 --crashfs "$crashfs" --verify-checksums
    run_case "exit$code-rot" "by itself: not a fail-stop" "a server that exits $code by itself, rot" JV_EXIT=150 JV_EXITCODE=$code -- \
        --nemesis rot --seconds 20 --verify-checksums
done
# a request that misses its deadline while a thread of the server sits in D state is the kernel's: INCONCLUSIVE (exit 3, the check kernel-stall),
# never a violation (round 12; VERIFY11 (B)). The deadline is 12 s here (D state is sampled for 2 s at a stretch) and the stall's bound 20 s.
if want dstall-kill; then
    t0=$(date +%s)
    out=$(JV_DSTALL=25 JEPSEN_REQ_DEADLINE_S=12 JEPSEN_KSTALL_BOUND_S=20 JEPSEN_TSDBD="$mut" timeout "${TMO:-200}" "$bin" --dir "$dir" --nemesis kill \
        --seconds 10 2>&1); st=$?
    left=$(leftover | tr '\n' ' ')
    if [ $st -eq 3 ] && echo "$out" | grep -q 'check kernel-stall' && ! echo "$out" | grep -q 'FAILED\|server stopped answering' && [ -z "$left" ]; then
        echo "mutant-crash: dstall-kill is INCONCLUSIVE (kernel-stall), not a violation, in $(($(date +%s) - t0)) s: a worker in D state for good"
    else
        echo "mutant-crash: dstall-kill: a worker in D state was not INCONCLUSIVE with the check kernel-stall (rc $st, wanted 3; left behind: ${left:-none})"
        echo "$out" | tail -n 15
        rc=1
    fi
    for p in $left; do kill -KILL "$p" 2>/dev/null; done
fi
HP='server stopped answering'
for nem in kill pause none; do
    run_case "hang-$nem" "$HP" "a worker that hangs for ever on every 25th insert, $nem nemesis" JV_HANG=25 JEPSEN_REQ_DEADLINE_S=8 -- \
        --nemesis "$nem" --seconds 10
done
for nem in eio power; do # the shim's default odds: the quiet tail's server is a clean one (quiet_tail_server), which nothing but the harness ends
    run_case "hang-$nem" "$HP" "a worker that hangs for ever on every 25th insert, $nem nemesis" JV_HANG=25 JEPSEN_REQ_DEADLINE_S=8 -- \
        --nemesis "$nem" --seconds 10 --crashfs "$crashfs"
done
run_case "hang-outage" "$HP" "a worker that hangs for ever on every 25th insert, outage nemesis" JV_HANG=25 JEPSEN_REQ_DEADLINE_S=8 -- \
    --nemesis outage --seconds 20 --outagefs "$outagefs"
rm -rf "$tmp"
exit $rc
