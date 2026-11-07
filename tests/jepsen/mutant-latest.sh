#!/bin/bash
# jepsen's latest check must have teeth: a tsdbd whose latest table keeps a stale entry (a row at a newer timestamp
# than the entry's is not stored: M3; one at the same timestamp is not stored: M2, a stale value; a null over the newest
# row does not make the entry unknown: M1) must fail a kv run with a "latest" violation. Builds each mutant from a
# copy of the tree (git archive of HEAD, under $TMPDIR or /var/tmp), never touches the working tree.
# Usage: tests/jepsen/mutant-latest.sh JEPSEN_DIR [M1 M2 M3]   (default: all three)
set -u
dir=${1:?usage: mutant-latest.sh JEPSEN_DIR [M1|M2|M3...]}
shift
muts=${*:-M1 M2 M3}
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/var/tmp}/mutant-latest.XXXXXX") || exit 2
git -C "$root" archive HEAD | tar -x -C "$tmp" || exit 2
rc=0
for m in $muts; do
    cp "$root/src/ltab.c" "$tmp/src/ltab.c"
    case $m in
    M1) from='else if (!floor \&\& (f->flags \& LF_N)'; to='else if (0 \&\& !floor \&\& (f->flags \& LF_N)' ;;
    M2) from='if (hasr \&\& f->r_ts >= e.ts)$'; to='if (hasr \&\& f->r_ts >= e.ts + 2001)' ;;
    M3) from='if (hasr \&\& f->r_ts >= e.ts)$'; to='if (hasr \&\& f->r_ts == e.ts)' ;;
    *) echo "unknown mutant $m"; exit 2 ;;
    esac
    # M2 and M3 change the LE_KNOWN case only (its line ends the condition; the LE_EMPTY and default cases differ)
    sed -i "0,/$from/s//$to/" "$tmp/src/ltab.c"
    if cmp -s "$root/src/ltab.c" "$tmp/src/ltab.c"; then echo "mutant-latest: $m did not apply"; rc=1; continue; fi
    (cd "$tmp" && make -j8 build/tsdbd >"$tmp/build.log" 2>&1) || { echo "mutant-latest: build of $m failed"; tail -5 "$tmp/build.log"; rc=1; continue; }
    out=$(JEPSEN_TSDBD="$tmp/build/tsdbd" "$root/build/jepsen" --dir "$dir" --nemesis kill --seconds 8 2>&1); st=$?
    if [ $st -ne 1 ] || ! echo "$out" | grep -q '^  latest [0-9]'; then
        echo "mutant-latest: $m went unnoticed (rc $st)"; echo "$out" | tail -n 8; rc=1
    else
        echo "mutant-latest: $m caught: $(echo "$out" | grep '^latest:')"
    fi
done
rm -rf "$tmp"
exit $rc
