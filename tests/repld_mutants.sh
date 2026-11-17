#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replication driver's mutants (server/repld.c, TSDB_REPLD_MUTANT): each planted bug runs the one test_cluster
# scenario whose oracle is meant for it, and counts as caught only when that scenario fails with its own FAIL line
# (a crash, a timeout, a startup flake or a failure for another reason is no catch: a gate that took any failure
# would not notice an oracle that lost its teeth while a side effect still failed the run). repcheck checks the
# latency of single-row writes only here (TSDB_TEST_ACK_MS: 1 to 1.5 ms with the fast path, about 4.7 ms without it on
# the build machine; too close for make test under TSan, where it only prints them).
# Usage: tests/repld_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb build/libcrashfs.so).
set -u
dir=${1:-/tmp}; shift || true
# mutant, its scenario, an extended regular expression its FAIL line must match
table=(
    "commitbyleader|replindet|a write with both replicas paused was answered"
    "floornotsaved|replicate|a replica down and back did not finish"
    "ackbeforesync|ackdurable|after a power loss replica [0-9]+ holds [0-9]+ rows"
    "nodepose|writerdepose|the writer was not deposed"
    "noquiesce|writerfail|of the failed volume: .*not UNAVAILABLE"
    "floornottold|repcheck|its saved floor never removed its flushed segments"
    "unsureignored|repcheck|opened with a damaged REPL slot and saved unsure 1"
    "admitnocheck|repcheck|a write whose core position is not its log number was acknowledged"
    "nofastpath|repcheck|the median latency of a single-row write is"
    "nocursorup|repcheck|the cursor never moved up to what the peer acknowledged"
)
all=
for e in "${table[@]}"; do all="$all ${e%%|*}"; done
muts=${*:-$all}
export CRASHFS_SO=${CRASHFS_SO:-build/libcrashfs.so} TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb} TSDB_TEST_ACK_MS=${TSDB_TEST_ACK_MS:-3}
survived=
for m in $muts; do
    row=
    for e in "${table[@]}"; do [ "${e%%|*}" = "$m" ] && row=$e; done
    if [ -z "$row" ]; then
        echo "mutant $m: no scenario is named for it in $0"
        survived="$survived $m"
        continue
    fi
    rest=${row#*|}; s=${rest%%|*}; want=${rest#*|}
    log="$dir/mut-$m-$s.log"
    TSDB_REPLD_MUTANT=$m timeout 600 ./build/test_cluster "$dir" "$s" > "$log" 2>&1
    rc=$?
    fail=$(grep -m1 FAIL "$log")
    if [ $rc -ne 0 ] && grep -m1 FAIL "$log" | grep -qE "$want"; then
        echo "mutant $m: caught by $s ($fail)"
    elif [ $rc -ne 0 ]; then
        echo "mutant $m: $s failed (rc $rc) but not with its oracle's line /$want/: ${fail:-no FAIL line}"
        survived="$survived $m"
    else
        echo "mutant $m: SURVIVED $s"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "not caught by their own oracle:$survived"; exit 1; }
echo "every mutant was caught by its own oracle"
