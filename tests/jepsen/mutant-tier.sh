#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# R15 jepsen-selftest: each planted R15 tier mutant (TSDB_TIER_MUTANT) must fail its corresponding
# jepsen nemesis with exit 1 (VIOLATION) and the expected diagnostic, never exit 0 or INCONCLUSIVE.
#   downnoread     reads still sent to a DOWN_STALLED store -> fails `store-stuck`
#   fullnoflag     tierfill_is_full() always false -> fails `fill`
#   lostnoreplace  replacement store promoted UP with 0 chunks rebuilt -> fails `store-lost`
#   salvagelatest  salvage skips latest-table invalidation & lgen bump -> fails `salvage`
# Usage: tests/jepsen/mutant-tier.sh JEPSEN_DIR [downnoread fullnoflag lostnoreplace salvagelatest]
set -u
dir=${1:?usage: mutant-tier.sh JEPSEN_DIR [downnoread|fullnoflag|lostnoreplace|salvagelatest...]}
shift
muts=${*:-downnoread fullnoflag lostnoreplace salvagelatest}
root=$(cd "$(dirname "$0")/../.." && pwd)
tsdbd=${JEPSEN_TSDBD:-$root/build/tsdbd}
tsdb=${JEPSEN_TSDB:-$root/build/tsdb}
jepsen=${JEPSEN_BIN:-$root/build/jepsen}
outagefs=${JEPSEN_OUTAGEFS:-$root/build/liboutagefs.so}
rc=0

for m in $muts; do
    if ! grep -aq "TSDB_TIER_MUTANT" "$tsdbd"; then
        if [ "${JEPSEN_REQUIRE_TIER:-0}" = 1 ]; then
            echo "mutant-tier: $m: $tsdbd has no TSDB_TIER_MUTANT hook"; rc=1
        else
            echo "mutant-tier: $m skipped: $tsdbd has no TSDB_TIER_MUTANT hook"
        fi
        continue
    fi
    case $m in
    downnoread)
        nem=store-stuck
        secs=25
        want='store-stuck: read stalled|downnoread\?|server stopped answering'
        ;;
    fullnoflag)
        nem=fill
        secs=25
        want='fill: volume at 92%.*did not refuse|fullnoflag\?'
        ;;
    lostnoreplace)
        nem=store-lost
        secs=25
        want='store-lost:.*(missing .* live pre-wipe chunks|0 chunks rebuilt|query failed)|lostnoreplace\?'
        ;;
    salvagelatest)
        nem=salvage
        secs=25
        want='salvage live-latest:.*(salvaged row|expected LATEST_NONE|expected latest ts|salvagelatest\?)'
        ;;
    *)
        echo "mutant-tier: unknown mutant $m"; exit 2
        ;;
    esac
    out=$(env TSDB_TIER_MUTANT="$m" JEPSEN_TSDBD="$tsdbd" JEPSEN_TSDB="$tsdb" \
        "$jepsen" --dir "$dir" --nemesis "$nem" --outagefs "$outagefs" --seconds "$secs" 2>&1)
    st=$?
    if [ "$st" -ne 1 ]; then
        echo "mutant-tier: $m went unnoticed on --nemesis $nem (rc $st, wanted 1)"
        echo "$out" | tail -n 15
        rc=1
    elif ! echo "$out" | grep -Eq "$want"; then
        echo "mutant-tier: $m failed --nemesis $nem, but not for the expected reason (rc $st)"
        echo "$out" | tail -n 15
        rc=1
    else
        echo "mutant-tier: $m caught: $(echo "$out" | grep -E "$want" | head -n 1)"
    fi
done
exit $rc
