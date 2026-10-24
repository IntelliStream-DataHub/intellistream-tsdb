#!/bin/bash
# Runs a `make dist` build the way a Sandy Bridge (x86-64-v2 + AVX, no AVX2,
# BMI1/2, FMA, LZCNT, MOVBE) runs it: under `qemu-x86_64 -cpu SandyBridge`,
# which raises SIGILL on any instruction that CPU lacks, and natively with
# TSDB_CPU=v2. LZCNT is the one slip qemu cannot show (an older CPU runs it as
# BSR); tools/check-isa.sh, which `make dist` runs, reads the code for it.
#
#   tests/distro/sandybridge.sh [build-dist]      QEMU=/path/to/qemu-x86_64
#
# Skips (exit 0, with a message) when qemu-x86_64 is missing: apt install qemu-user.
set -u
b=${1:-build-dist}
qemu=${QEMU:-$(command -v qemu-x86_64 || true)}
scratch=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-sandybridge.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
fail=0
run() {
    echo "=== $*"
    "$@" >"$scratch/out" 2>&1
    local r=$?
    tail -n 3 "$scratch/out"
    if [ $r -ne 0 ]; then echo "=== FAILED (exit $r): $*"; fail=1; fi
}
export TSDBD=$b/tsdbd
for t in test_levels test_tsdb test_types codec-bench type-bench; do
    args=()
    case $t in test_*) [ $t != test_levels ] && args=("$scratch") ;; esac
    TSDB_CPU=v2 run "$b/$t" "${args[@]}"
done
if [ -z "$qemu" ] || ! [ -x "$qemu" ]; then
    echo "sandybridge: SKIPPED the emulated Sandy Bridge: no qemu-x86_64 (apt install qemu-user, or QEMU=path)"
else
    $qemu -cpu help | grep -q SandyBridge || { echo "sandybridge: $qemu has no SandyBridge model"; exit 1; }
    for t in test_levels test_tsdb test_types codec-bench type-bench; do
        args=()
        case $t in test_*) [ $t != test_levels ] && args=("$scratch") ;; esac
        run "$qemu" -cpu SandyBridge "$b/$t" "${args[@]}"
    done
fi
[ $fail -eq 0 ] && echo "sandybridge: all passed" || echo "sandybridge: FAILURES"
exit $fail
