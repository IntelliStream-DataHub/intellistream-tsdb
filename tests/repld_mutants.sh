#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replication driver's mutants (server/repld.c, TSDB_REPLD_MUTANT): test_cluster's replicate and replindet run
# with each planted bug, and each must fail one of them. Usage: tests/repld_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb build/libcrashfs.so).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-commitbyleader ackbeforesync floornotsaved}
export CRASHFS_SO=${CRASHFS_SO:-build/libcrashfs.so} TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
survived=
for m in $muts; do
    caught=
    for s in replindet replicate; do
        if ! TSDB_REPLD_MUTANT=$m timeout 600 ./build/test_cluster "$dir" $s > "$dir/mut-$m-$s.log" 2>&1; then
            caught=$s
            break
        fi
    done
    if [ -n "$caught" ]; then
        echo "mutant $m: caught by $caught ($(grep -m1 FAIL "$dir/mut-$m-$caught.log"))"
    else
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
