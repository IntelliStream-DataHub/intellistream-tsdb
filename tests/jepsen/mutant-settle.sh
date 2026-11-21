#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# jepsen's checks must have teeth against the directory tier (R7): a tsdbd with a planted settle bug must fail a run
# with --tier, and fail it the way the check is meant to (a data check, not an open that refuses or a count).
#   ecrankdown  TSDB_SETTLE_MUTANT=ecrankdown: in memory only, a settled (.ec) part ranks below every other part of
#               its partition, so an older value wins over a newer one. Must fail the kv run under kill with a
#               durability, order, stale-read or final-state violation (checks 1 to 5, the final reads), and not with
#               "does not come up" (the open must not be what catches it: the mutant changes no name).
#   swapearly   r7-swap's mutant (the swap renames before it checks its inputs, or lists the `.ec` before its intent is
#               durable): must fail the settle-steps nemesis, where the power cut lands inside the swap, by a data check
#               or by an open that refuses and names an `.ec`, a `settled_` name or an intent, and only when the same
#               run without the mutant (the control: same seed, same steps) comes up and is valid (an open that fails
#               for a reason of the machine, e.g. io_uring out of memory, is no catch). Set by
#               TSDB_SWAP_MUTANT=swapearly, an environment variable of the tsdbd the mutant is in (the name of the
#               variable is r7-swap's: change it here if that track named it otherwise).
# A tsdbd that has no such mutant (the strings are not in the binary: a tree before r7-settle or r7-swap) is skipped
# with a line, unless JEPSEN_REQUIRE_TIER=1, which makes it a failure (`make jepsen-selftest` sets it once the tree has
# a tier: JEPSEN_TIER non-empty). Which names arm the mutants (TSDB_SETTLE_MUTANT=ecrankdown, TSDB_SWAP_MUTANT=swapearly)
# is the contract with r7-settle and r7-swap: a run that finds the needle in the binary and a mutant that does not
# arm that way fails as "went unnoticed".
# Usage: tests/jepsen/mutant-settle.sh JEPSEN_DIR [ecrankdown swapearly]   (default: both)
# (JEPSEN_TSDBD names another tsdbd than build/tsdbd, JEPSEN_BIN another jepsen than build/jepsen)
set -u
dir=${1:?usage: mutant-settle.sh JEPSEN_DIR [ecrankdown|swapearly...]}
shift
muts=${*:-ecrankdown swapearly}
root=$(cd "$(dirname "$0")/../.." && pwd)
tsdbd=${JEPSEN_TSDBD:-$root/build/tsdbd}
jepsen=${JEPSEN_BIN:-$root/build/jepsen}
crashfs=$root/build/libcrashfs.so
rc=0
for m in $muts; do
    case $m in
    ecrankdown) var=TSDB_SETTLE_MUTANT; needle=TSDB_SETTLE_MUTANT ;;
    swapearly) var=TSDB_SWAP_MUTANT; needle=swapearly ;;
    *) echo "unknown mutant $m"; exit 2 ;;
    esac
    if ! grep -aq "$needle" "$tsdbd"; then
        if [ "${JEPSEN_REQUIRE_TIER:-0}" = 1 ]; then
            echo "mutant-settle: $m: $tsdbd has no such mutant"; rc=1
        else
            echo "mutant-settle: $m skipped: $tsdbd has no such mutant (no settle, or no swap, yet)"
        fi
        continue
    fi
    case $m in
    ecrankdown)
        out=$(env TSDB_SETTLE_MUTANT=ecrankdown JEPSEN_TSDBD="$tsdbd" "$jepsen" --dir "$dir" --nemesis kill \
            --seconds 20 --tier 3 2>&1); st=$?
        want='lost: final holds|final holds .* completed before|: stale$|: torn$|acknowledged before a read saw|^  final: series|^  final latest'
        ;;
    swapearly)
        out=$(env TSDB_SWAP_MUTANT=swapearly JEPSEN_TSDBD="$tsdbd" "$jepsen" --dir "$dir" --nemesis settle-steps \
            --crashfs "$crashfs" --seconds 40 --tier 3 2>&1); st=$?
        want='lost: final holds|final holds .* completed before|: stale$|: torn$|acknowledged before a read saw|^  final: series|^  final latest'
        # jepsen says "does not come up" on one line and the server's log tail, which names the file, below it
        refused='does not come up|cannot open'
        named='^tsdb: .*settle intent .*(damage|the open fails)'
        ;;
    esac
    # the refusal's own line: one below jepsen's "does not come up", never any line of the run that names an intent
    after=$(echo "$out" | awk -v r="${refused:-^$}" 'f; $0 ~ r {f=1}')
    if [ $m = swapearly ] && [ $st -eq 2 ] && echo "$after" | grep -Eq "$named"; then
        # a refused open: a catch only if the control comes up
        ctl=$(env JEPSEN_TSDBD="$tsdbd" "$jepsen" --dir "$dir" --nemesis settle-steps --crashfs "$crashfs" \
            --seconds 40 --tier 3 2>&1); cst=$?
        if [ $cst -eq 0 ]; then
            echo "mutant-settle: $m caught: $(echo "$after" | grep -E "$named" | head -n 1)"
        else
            echo "mutant-settle: $m: the open refused, but the control without the mutant failed too (rc $cst): no catch"
            echo "$ctl" | tail -n 8; rc=1
        fi
    elif [ $st -ne 1 ]; then
        echo "mutant-settle: $m went unnoticed (rc $st)"; echo "$out" | tail -n 8; rc=1
    elif echo "$out" | grep -q 'does not come up'; then
        echo "mutant-settle: $m was caught by an open that refused, not by a check (rc $st)"; echo "$out" | tail -n 8; rc=1
    elif ! echo "$out" | grep -Eq "$want"; then
        echo "mutant-settle: $m failed the run, but not by a data check (rc $st)"; echo "$out" | tail -n 8; rc=1
    else
        echo "mutant-settle: $m caught: $(echo "$out" | grep -E "$want" | head -n 1)"
    fi
done
exit $rc
