#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Runs a `make dist` build the way a Sandy Bridge (x86-64-v2 + AVX, no AVX2,
# BMI1/2, FMA, LZCNT, MOVBE) and a Westmere (x86-64-v2 without AVX: the
# oldest CPUs AlmaLinux's v2 build is for) run it: under `qemu-x86_64 -cpu
# SandyBridge` and `-cpu Westmere`, which raise SIGILL on any instruction that
# CPU lacks, and natively with TSDB_CPU=v2. LZCNT is the one slip qemu cannot show (an older CPU runs it as
# BSR); tools/check-isa.sh, which `make dist` runs, reads the code for it.
#
#   tests/distro/sandybridge.sh [build-dist]      QEMU=/path/to/qemu-x86_64   EMU_TIMEOUT=seconds (1200)
#
# Skips (exit 0, with a message) when qemu-x86_64 is missing: apt install qemu-user.
#
# qemu-user can deadlock a process the guest forks (qemu 8.2.2 in Ubuntu 24.04: test_types' crash child under
# -cpu SandyBridge sat in a futex wait for 20 minutes with no CPU time, its parent in waitpid). That is the emulator,
# not the code under test, so an emulated test that outlives EMU_TIMEOUT is killed (SIGKILL to its process group:
# a deadlocked qemu never acts on a SIGTERM, and would outlive its parent) and run
# once more in a fresh directory; a second hang fails. A test that fails is never repeated.
set -u
ulimit -c 0 # qemu-user would leave a qemu_*.core in the current directory on a SIGILL
b=${1:-build-dist}
qemu=${QEMU:-$(command -v qemu-x86_64 || true)}
emu_timeout=${EMU_TIMEOUT:-1200}
scratch=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-sandybridge.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
fail=0
rc=0
run() {
    echo "=== $*"
    "$@" >"$scratch/out" 2>&1
    local r=$?
    rc=$r
    # a failure shows all its output: the release workflow runs this, and its log is all there is to go on
    if [ $r -ne 0 ]; then
        cat "$scratch/out"
        echo "=== FAILED (exit $r): $*"
        fail=1
    else
        tail -n 3 "$scratch/out"
    fi
}
# in_time CMD...: CMD in a process group of its own, all of which is killed (SIGKILL) after EMU_TIMEOUT: status 137.
# Not timeout(1): GNU's signals the group, other implementations (uutils) only the child, and a forked qemu that
# hangs is a grandchild. The watchdog is a group of its own too, so that its sleep goes with it.
in_time() {
    setsid "$@" <&0 &
    local pid=$!
    setsid bash -c 'sleep "$1"; kill -KILL -- "-$2"' in_time "$emu_timeout" "$pid" </dev/null &
    local dog=$!
    wait "$pid"
    local r=$?
    kill -KILL -- "-$dog" 2>/dev/null
    wait "$dog" 2>/dev/null
    return $r
}
# emu CPU TEST: TEST of the build under qemu -cpu CPU, killed after EMU_TIMEOUT and run once more if it hangs
emu() {
    local cpu=$1 t=$2 before=$fail try
    for try in 1 2; do
        local args=()
        case $t in test_levels) ;; test_*) args=("$(mktemp -d "$scratch/$t.XXXXXX")") ;; esac
        run in_time "$qemu" -cpu "$cpu" "$b/$t" "${args[@]}"
        case $rc in 124 | 137) ;; *) return ;; esac
        if [ "$try" = 1 ]; then
            echo "=== HUNG: killed after $emu_timeout s (qemu-user can deadlock after a fork); once more"
            fail=$before
        else
            echo "=== HUNG again: killed after $emu_timeout s"
        fi
    done
}
export TSDBD=$b/tsdbd
for t in test_levels test_tsdb test_types codec-bench type-bench; do
    args=()
    case $t in test_*) [ $t != test_levels ] && args=("$scratch") ;; esac
    TSDB_CPU=v2 run "$b/$t" "${args[@]}"
done
if [ -z "$qemu" ] || ! [ -x "$qemu" ]; then
    echo "sandybridge: SKIPPED the emulated Westmere and Sandy Bridge: no qemu-x86_64 (apt install qemu-user, or QEMU=path)"
else
    for cpu in Westmere SandyBridge; do
        $qemu -cpu help | grep -qw $cpu || { echo "sandybridge: $qemu has no $cpu model"; exit 1; }
        for t in test_levels test_tsdb test_types codec-bench type-bench; do
            emu $cpu $t
        done
    done
    # the CLI (its constructors run before main) creates a database, writes and aggregates; read-only
    # commands refuse a directory that does not exist, so it inserts first
    seq 0 999 | sed 's/.*/170000000&000,&.5/' >"$scratch/rows"
    for cpu in Westmere SandyBridge; do
        run in_time "$qemu" -cpu $cpu "$b/tsdb" "$scratch/cli-$cpu" insert s <"$scratch/rows"
        run in_time "$qemu" -cpu $cpu "$b/tsdb" "$scratch/cli-$cpu" query s min max all
        grep -qP '^min\t1000\t500000\t' "$scratch/out" || { cat "$scratch/out"; echo "=== FAILED: $cpu: count/sum of the CLI query"; fail=1; }
        run in_time "$qemu" -cpu $cpu "$b/tsdb" "$scratch/cli-$cpu" stats
        grep -q 'codecs x86-64-v2' "$scratch/out" || { echo "=== FAILED: $cpu: the CLI did not pick the v2 codecs"; fail=1; }
        # the aggregation kernels: SSE4.2 on Westmere, AVX (no AVX2) on Sandy Bridge
        case $cpu in Westmere) k=sse ;; *) k=avx ;; esac
        echo "=== $cpu: $(grep '^simd' "$scratch/out")"
        grep -qE "^simd +$k, codecs" "$scratch/out" || { echo "=== FAILED: $cpu: the kernels are not $k"; fail=1; }
    done
fi
[ $fail -eq 0 ] && echo "sandybridge: all passed" || echo "sandybridge: FAILURES"
exit $fail
