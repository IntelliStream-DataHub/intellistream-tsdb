#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Every bug planted in server/meta.c (MUT(n)) must be caught by test_meta: the unit tests or the simulator's checker.
# Usage: meta_mutants.sh [--part K/N] BUILD/test_meta_mut [MUTANT...]
# --part K/N runs every Nth mutant of the list, starting with the Kth (CI splits the list over runners).
set -u
part=
if [ "${1:-}" = --part ]; then
    part=$2
    shift 2
fi
bin=$1
shift
muts=${*:-1 2 3 4 5 6 7 8 9 10 11 12 13 25 28 31 35}
if [ -n "$part" ]; then
    k=${part%/*}
    n=${part#*/}
    case $k$n in '' | *[!0-9]*) echo "meta_mutants.sh: --part K/N, got '$part'" >&2; exit 2 ;; esac
    [ "$k" -ge 1 ] && [ "$k" -le "$n" ] || { echo "meta_mutants.sh: --part K/N needs 1 <= K <= N" >&2; exit 2; }
    sel=
    i=0
    for m in $muts; do
        [ $((i % n + 1)) -eq "$k" ] && sel="$sel $m"
        i=$((i + 1))
    done
    muts=$sel
fi
dir=$(mktemp -d /var/tmp/meta-mut.XXXXXX) || exit 1
trap 'rmdir "$dir" 2>/dev/null' EXIT
bad=0
for n in $muts; do
    if "$bin" "$dir" --mutant "$n" --seeds 6 --seconds 8 >/dev/null 2>&1; then
        echo "meta mutant $n: NOT caught"
        bad=1
    else
        echo "meta mutant $n: caught"
    fi
done
exit $bad
