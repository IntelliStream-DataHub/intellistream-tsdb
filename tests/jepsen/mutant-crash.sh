#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that dies by itself, or stops answering, must be a VIOLATION (exit 1) on the attempt where it happened, under every nemesis
# and under none (the user's scope of 2026-11-14: a server that hangs or dies other than by the planned kill or power loss is found
# and reported, never hidden behind INCONCLUSIVE, a rerun or a pass; round 11). Builds, from a copy of the tree (git archive of HEAD,
# under $TMPDIR or /var/tmp; the working tree and the shipped tsdbd are never touched), a tsdbd with two test-only mutants:
#   JV_CRASH=N   abort() on every Nth insert request (a crash of its own: SIGABRT)
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
#include <sys\/resource.h>\
static _Atomic unsigned jv_cnt; \/* test-only mutants of tests\/jepsen\/mutant-crash.sh *\/\
static int jv_every(const char *k)\
{\
    const char *e = getenv(k);\
    int n = e ? atoi(e) : 0;\
    return n > 0 \&\& atomic_fetch_add(\&jv_cnt, 1) % (unsigned)n == (unsigned)n - 1;\
}/' "$f"
sed -i 's/^    case TSDB_OP_INSERT: {$/&\
        if (jv_every("JV_CRASH")) { struct rlimit z = {0, 0}; setrlimit(RLIMIT_CORE, \&z); abort(); }\
        if (jv_every("JV_HANG")) for (;;) pause();/; s/^    case TSDB_OP_INSERT_VALUES: {$/&\
        if (jv_every("JV_CRASH")) { struct rlimit z = {0, 0}; setrlimit(RLIMIT_CORE, \&z); abort(); }\
        if (jv_every("JV_HANG")) for (;;) pause();/' "$f"
if [ "$(grep -c 'jv_every(' "$f")" -ne 5 ]; then echo "mutant-crash: the hooks did not apply"; rm -rf "$tmp"; exit 1; fi
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
CR='ended with status .* (signal 6 (Aborted).*by itself'
for nem in kill pause none; do
    run_case "crash-$nem" "$CR" "a server that aborts on every 40th insert, $nem nemesis" JV_CRASH=40 -- --nemesis "$nem" --seconds 12
done
for nem in eio power; do
    run_case "crash-$nem" "$CR" "a server that aborts on every 40th insert, $nem nemesis" JV_CRASH=40 -- --nemesis "$nem" --seconds 12 \
        --crashfs "$crashfs"
done
HP='server stopped answering'
for nem in kill pause none; do
    run_case "hang-$nem" "$HP" "a worker that hangs for ever on every 25th insert, $nem nemesis" JV_HANG=25 JEPSEN_REQ_DEADLINE_S=8 -- \
        --nemesis "$nem" --seconds 10
done
for nem in eio power; do # the shim's own odds left out (0): the last server, which the quiet tail leaves to the clients, must live
    run_case "hang-$nem" "$HP" "a worker that hangs for ever on every 25th insert, $nem nemesis" JV_HANG=25 JEPSEN_REQ_DEADLINE_S=8 \
        JEPSEN_PULL_PROB=0 JEPSEN_CRASH_PROB=0 -- --nemesis "$nem" --seconds 10 --crashfs "$crashfs"
done
run_case "hang-outage" "$HP" "a worker that hangs for ever on every 25th insert, outage nemesis" JV_HANG=25 JEPSEN_REQ_DEADLINE_S=8 -- \
    --nemesis outage --seconds 20 --outagefs "$outagefs"
rm -rf "$tmp"
exit $rc
