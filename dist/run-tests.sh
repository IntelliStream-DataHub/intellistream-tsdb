#!/bin/sh
# The test suite of a dist tarball, run where the binaries are going to run.
#
#   tests/run-tests.sh [scratch-dir]     (from the unpacked tarball; default /tmp)
#
# test_levels compares every CPU level and kernel this CPU has; test_tsdb,
# test_types and test_group then run once per level (TSDB_CPU), the way the
# same binary runs on an older CPU. A level the CPU lacks is skipped.
set -u
here=$(cd "$(dirname "$0")" && pwd)
top=$(cd "$here/.." && pwd)
scratch=${1:-/tmp}
export TSDBD="$top/bin/tsdbd"
export TSDB_CRASHFS="$here/libcrashfs.so" # the server fail-stop test fails a WAL write through it
fail=0
run() {
    echo "=== $*"
    "$@" || { echo "=== FAILED: $*"; fail=1; }
}
echo "glibc: $(getconf GNU_LIBC_VERSION 2>/dev/null); kernel: $(uname -r)"
grep -m1 "model name" /proc/cpuinfo 2>/dev/null
run "$here/test_levels"
for level in v2 v3 v4; do
    for t in test_tsdb test_types test_group; do
        TSDB_CPU=$level run "$here/$t" "$scratch"
    done
done
run "$here/test_follow" "$scratch"
run "$here/test_volumes" "$scratch"
run "$here/test_server" "$scratch"
run "$here/test_stability" "$scratch"
if [ $fail -ne 0 ]; then
    echo "run-tests: FAILURES"
    exit 1
fi
echo "run-tests: all passed"
