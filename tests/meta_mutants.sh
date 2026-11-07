#!/bin/bash
# Every bug planted in server/meta.c (MUT(n)) must be caught by test_meta: the unit tests or the simulator's checker.
# Usage: meta_mutants.sh BUILD/test_meta_mut [MUTANT...]
set -u
bin=$1
shift
muts=${*:-1 2 3 4 5 6 7 8 9 10 11 12 13}
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
