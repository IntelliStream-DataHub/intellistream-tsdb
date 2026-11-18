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
#   JV_HANGONCE=K  the Kth insert request makes its worker pause() for ever, once per gate (JV_ONCEFILE2 names the file whose creation says so)
#   JV_DBLIP=K2    the K2th insert puts its worker in D state for JV_DBLIP_S seconds (vfork whose child sleeps), once per gate (JV_ONCEFILE3):
#                  a D stretch that ENDS before a hung request's deadline passes (round 13, VERIFY12: it passed the hang as a kernel stall)
#   JV_OPENFAIL=1  a server whose log (the epoch's, appended to) holds the damage line an earlier server planted exits 1 at open, once per gate
#                  (JV_ONCEFILE4), with "tsdbd: open: invalid argument": a recovery bug of its own, no damage (VERIFY12 (A): it counted as refused on
#                  damage because the whole epoch's log was read); the first server of the epoch plants that line
#   JV_OPENANY=exit1|exit0|hang|dstall  the SECOND start of the tsdbd the harness runs as its writer (or, with JV_OPENWHO=follower, the follower: its only
#                  start), once per gate (JV_ONCEFILE6; JV_STARTCNT names the file whose creation says the first start has passed), does at open:
#                  exit 1 with "tsdbd: DIR: invalid argument" (a recovery bug of its own: no damage, no failure of the machine), exit 0, pause() for
#                  ever before it listens, or a thread in D state for good (vfork whose child never execs: the kernel's stall) (round 14; VERIFY13
#                  (A): the first three were a harness error, exit 2, "not a verdict about tsdb", under every nemesis but misdirect and rot)
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
cat > "$tmp/server/jv_hooks.h" <<'JVEOF'
/* test-only hooks of tests/jepsen/mutant-crash.sh (round 13) */
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
static int jv_once_file(const char *var)
{
    const char *f = getenv(var);
    int fd = f ? open(f, O_WRONLY | O_CREAT | O_EXCL, 0644) : -1;
    if (fd < 0)
        return 0;
    close(fd);
    return 1;
}
static void jv_extra(void)
{
    static _Atomic unsigned hang_n, blip_n;
    const char *e;
    if ((e = getenv("JV_HANGONCE")) && atomic_fetch_add(&hang_n, 1) + 1 == (unsigned)atoi(e) && jv_once_file("JV_ONCEFILE2"))
        for (;;)
            pause();
    if ((e = getenv("JV_DBLIP")) && atomic_fetch_add(&blip_n, 1) + 1 == (unsigned)atoi(e) && jv_once_file("JV_ONCEFILE3")) {
        int secs = getenv("JV_DBLIP_S") ? atoi(getenv("JV_DBLIP_S")) : 8;
        if (vfork() == 0) { /* the parent waits for it in D state; the child ends with the server, never holds its lock for long */
            struct timespec ts = {secs, 0};
            prctl(PR_SET_PDEATHSIG, SIGKILL);
            syscall(SYS_nanosleep, &ts, NULL);
            _exit(0);
        }
    }
}
static void jv_openfail(void)
{
    static char b[1 << 20];
    if (!getenv("JV_OPENFAIL"))
        return;
    int fd = open("/proc/self/fd/2", O_RDONLY);
    ssize_t n = fd >= 0 ? read(fd, b, sizeof b - 1) : -1;
    if (fd >= 0)
        close(fd);
    b[n > 0 ? n : 0] = 0;
    if (!strstr(b, "tsdb: jv-planted: checksum mismatch (damaged")) {
        fprintf(stderr, "tsdb: jv-planted: checksum mismatch (damaged data, planted by an earlier server)\n");
        return;
    }
    if (jv_once_file("JV_ONCEFILE4")) {
        fprintf(stderr, "tsdbd: open: invalid argument\n");
        _exit(1);
    }
}
static void jv_openany(const char *dir)
{
    static char cl[8192];
    const char *m = getenv("JV_OPENANY");
    if (!m || !getenv("JV_STARTCNT"))
        return;
    bool fol = false; /* the follower is a tsdbd too (--follow) */
    int cfd = open("/proc/self/cmdline", O_RDONLY);
    ssize_t cn = cfd >= 0 ? read(cfd, cl, sizeof cl - 1) : -1;
    if (cfd >= 0)
        close(cfd);
    for (ssize_t i = 0; i < cn; i += (ssize_t)strlen(cl + i) + 1)
        fol = fol || !strcmp(cl + i, "--follow");
    bool aim = getenv("JV_OPENWHO") && !strcmp(getenv("JV_OPENWHO"), "follower") ? fol : !fol;
    if (!aim)
        return;
    int first = fol ? 0 : jv_once_file("JV_STARTCNT"); /* the writer's first start (the series') passes */
    if (first || !jv_once_file("JV_ONCEFILE6"))
        return;
    if (!strcmp(m, "exit1")) {
        fprintf(stderr, "tsdbd: %s: invalid argument\n", dir);
        _exit(1);
    }
    if (!strcmp(m, "exit0"))
        _exit(0);
    if (!strcmp(m, "hang"))
        for (;;)
            pause();
    if (!strcmp(m, "dstall") && vfork() == 0) {
        prctl(PR_SET_PDEATHSIG, SIGKILL);
        for (;;)
            pause();
    }
}
JVEOF
sed -i '0,/^#include <fcntl.h>$/s//#include <fcntl.h>\
#include <sys\/prctl.h>\
#include "jv_hooks.h"\
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
        if (jv_every("JV_HANG")) for (;;) pause();\
        jv_extra();/; s/^    case TSDB_OP_INSERT_VALUES: {$/&\
        if (jv_every("JV_CRASH")) jv_crash();\
        if (jv_every("JV_CRASHONCE")) jv_once();\
        if (jv_every("JV_EXIT")) _exit(atoi(getenv("JV_EXITCODE") ? getenv("JV_EXITCODE") : "0"));\
        if (jv_every("JV_DSTALL")) jv_dstall();\
        if (jv_every("JV_HANG")) for (;;) pause();\
        jv_extra();/' "$f"
sed -i 's/^    int rc = tsdb_open(dir, &o, &db);$/    jv_openfail();\
    jv_openany(dir);\
&/' "$f"
if [ "$(grep -c 'jv_every(' "$f")" -ne 11 ] || [ "$(grep -c 'jv_extra();' "$f")" -ne 2 ] || [ "$(grep -c 'jv_openfail();' "$f")" -ne 1 ] || [ "$(grep -c 'jv_openany(dir);' "$f")" -ne 1 ]; then echo "mutant-crash: the hooks did not apply"; rm -rf "$tmp"; exit 1; fi
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
# a hung worker (tsdbd's own: pause(), no D state) and, at the same time, ANOTHER worker in D state for 8 s (a D stretch of the kernel that is over
# 4 s before the hung request's deadline of 12 s passes): the miss is tsdbd's, a VIOLATION on the first attempt. Round 12 called it a kernel
# stall (INCONCLUSIVE, then the rerun passed) because a D stretch had been seen since the request was sent (VERIFY12 (A): g2-hangblip40).
for nem in none pause; do
    rm -f "$tmp/hb-$nem".*
    run_case "hangblip-$nem" "$HP" "a worker that hangs for ever while another sits in D state for 8 s (over before the deadline), $nem nemesis" \
        JV_HANGONCE=40 JV_DBLIP=30 JV_DBLIP_S=8 JV_ONCEFILE2="$tmp/hb-$nem.1" JV_ONCEFILE3="$tmp/hb-$nem.2" JEPSEN_REQ_DEADLINE_S=12 -- \
        --nemesis "$nem" --seconds 10
done
# ... and the D stretch still under way when the request misses its deadline (30 s against a deadline of 12): the kernel's, INCONCLUSIVE
if want hangblip-long; then
    rm -f "$tmp/hbl".*
    t0=$(date +%s)
    out=$(JV_HANGONCE=40 JV_DBLIP=30 JV_DBLIP_S=30 JV_ONCEFILE2="$tmp/hbl.1" JV_ONCEFILE3="$tmp/hbl.2" JEPSEN_REQ_DEADLINE_S=12 \
        JEPSEN_KSTALL_BOUND_S=20 JEPSEN_TSDBD="$mut" timeout "${TMO:-200}" "$bin" --dir "$dir" --nemesis none --seconds 10 2>&1); st=$?
    left=$(leftover | tr '\n' ' ')
    if [ $st -eq 3 ] && echo "$out" | grep -q 'check kernel-stall' && ! echo "$out" | grep -q 'FAILED\|server stopped answering' && [ -z "$left" ]; then
        echo "mutant-crash: hangblip-long is INCONCLUSIVE (kernel-stall) in $(($(date +%s) - t0)) s: a D stretch still under way at the miss"
    else
        echo "mutant-crash: hangblip-long: a D stretch under way at the miss was not INCONCLUSIVE with the check kernel-stall (rc $st, wanted 3; left behind: ${left:-none})"
        echo "$out" | tail -n 15
        rc=1
    fi
    for p in $left; do kill -KILL "$p" 2>/dev/null; done
fi
# an exit 1 at open for a reason of its own (the server is not a fail-stop on damage: its own lines name none) under misdirect, with and without
# verify_checksums: a VIOLATION, not "refused on damage" (VERIFY12 (A): an earlier server's damage line in the epoch's log made it one)
OF="not a refusal on damage"
rm -f "$tmp/of-md" "$tmp/of-mdv"
run_case "openfail-md" "$OF" "a server that exits 1 at open for a reason of its own, misdirect" JV_OPENFAIL=1 JV_ONCEFILE4="$tmp/of-md" -- \
    --nemesis misdirect --seconds 20 --crashfs "$crashfs"
run_case "openfail-mdv" "$OF" "a server that exits 1 at open for a reason of its own, misdirect, verify_checksums" JV_OPENFAIL=1 JV_ONCEFILE4="$tmp/of-mdv" -- \
    --nemesis misdirect --seconds 20 --crashfs "$crashfs" --verify-checksums
# a server (the writer, and the follower) that fails at open for a reason of its own after the planned faults, under every nemesis: a VIOLATION at the FIRST
# attempt (round 14; VERIFY13 (A): exit 1 with no damage in its lines, exit 0 and a hang before it listens were "the environment, not a verdict about
# tsdb", exit 2, under every nemesis but misdirect and rot; the follower likewise). The hung server is waited for 10 s (JEPSEN_OPEN_WAIT_MS), the machine
# not stalled; one that sits in D state for good is the kernel's: exit 2 (the environment), as it always was.
OE="exits with status 1 while opening.*not a refusal on damage"
O0="exits with status 0 before it listens"
OH="is alive and wrote no port file in [0-9]* s, and the machine did not stall"
tiercli=$root/build/tsdb
oa_case() { # name pattern what mode nemesis-args...
    local name=$1 pat=$2 what=$3 mode=$4; shift 4
    want "$name" || return 0
    rm -f "$tmp/oa-$name".*
    run_case "$name" "$pat" "$what" JV_OPENANY="$mode" JV_STARTCNT="$tmp/oa-$name.1" JV_ONCEFILE6="$tmp/oa-$name.2" JEPSEN_OPEN_WAIT_MS=10000 -- "$@"
}
for nem in kill pause none; do
    oa_case "openany-exit1-$nem" "$OE" "a server whose restart exits 1 at open for a reason of its own, $nem nemesis" exit1 --nemesis "$nem" --seconds 12
    oa_case "openany-exit0-$nem" "$O0" "a server whose restart exits 0 before it listens, $nem nemesis" exit0 --nemesis "$nem" --seconds 12
    oa_case "openany-hang-$nem" "$OH" "a server whose restart hangs before it listens, $nem nemesis" hang --nemesis "$nem" --seconds 12
done
for nem in eio power; do
    oa_case "openany-exit1-$nem" "$OE" "a server whose restart exits 1 at open for a reason of its own, $nem nemesis" exit1 --nemesis "$nem" --seconds 12 \
        --crashfs "$crashfs"
    oa_case "openany-exit0-$nem" "$O0" "a server whose restart exits 0 before it listens, $nem nemesis" exit0 --nemesis "$nem" --seconds 12 --crashfs "$crashfs"
    oa_case "openany-hang-$nem" "$OH" "a server whose restart hangs before it listens, $nem nemesis" hang --nemesis "$nem" --seconds 12 --crashfs "$crashfs"
done
oa_case "openany-exit1-outage" "$OE" "a server whose restart exits 1 at open for a reason of its own, outage nemesis" exit1 --nemesis outage --seconds 20 \
    --outagefs "$outagefs"
oa_case "openany-exit1-auth" "$OE" "a server whose restart exits 1 at open for a reason of its own, login on, kill nemesis" exit1 --auth --nemesis kill --seconds 12
oa_case "openany-exit1-feed" "$OE" "a server whose restart exits 1 at open for a reason of its own, change feed on, kill nemesis" exit1 --feed --nemesis kill --seconds 12
oa_case "openany-exit1-classes" "$OE" "a server whose restart exits 1 at open for a reason of its own, classes workload, power nemesis" exit1 --workload classes \
    --nemesis power --seconds 12 --crashfs "$crashfs"
for mode in exit1 exit0 hang; do # the follower of the power nemesis
    case $mode in
    exit1) pat="the follower (pid [0-9]*) exits with status 1 while opening" ;;
    exit0) pat="the follower (pid [0-9]*) exits with status 0 before it listens" ;;
    *) pat="the follower (pid [0-9]*) is alive and wrote no port file in [0-9]* s, and the machine did not stall" ;;
    esac
    want "openfol-$mode" || continue
    rm -f "$tmp/of-$mode".*
    run_case "openfol-$mode" "$pat" "a follower that fails at open ($mode), power nemesis" JV_OPENANY=$mode JV_OPENWHO=follower \
        JV_STARTCNT="$tmp/of-$mode.1" JV_ONCEFILE6="$tmp/of-$mode.2" JEPSEN_OPEN_WAIT_MS=10000 -- --nemesis power --seconds 12 --crashfs "$crashfs"
done
# a thread of the server in D state for good at open is the kernel's stall, not a verdict: a harness error (exit 2), never a violation, never a pass
if want openany-dstall-kill; then
    rm -f "$tmp/oa-ds".*
    t0=$(date +%s)
    out=$(JV_OPENANY=dstall JV_STARTCNT="$tmp/oa-ds.1" JV_ONCEFILE6="$tmp/oa-ds.2" JEPSEN_REQ_DEADLINE_S=12 JEPSEN_OPEN_WAIT_MS=20000 JEPSEN_TSDBD="$mut" \
        timeout "${TMO:-200}" "$bin" --dir "$dir" --nemesis kill --seconds 10 2>&1); st=$?
    left=$(leftover | tr '\n' ' ')
    if [ $st -eq 2 ] && echo "$out" | grep -q 'the server does not come up (the environment, not a verdict about tsdb), and a thread of the server sat in D state' &&
        ! echo "$out" | grep -q 'FAILED' && [ -z "$left" ]; then
        echo "mutant-crash: openany-dstall-kill is a harness error (exit 2), not a violation, in $(($(date +%s) - t0)) s: a thread in D state for good at open"
    else
        echo "mutant-crash: openany-dstall-kill: a thread in D state for good at open was not a harness error (rc $st, wanted 2; left behind: ${left:-none})"
        echo "$out" | tail -n 15
        rc=1
    fi
    for p in $left; do kill -KILL "$p" 2>/dev/null; done
fi
rm -rf "$tmp"
exit $rc
