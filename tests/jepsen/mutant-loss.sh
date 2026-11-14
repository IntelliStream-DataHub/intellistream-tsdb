#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# A lazy nemesis must not hide data loss (the user's scope of 2026-11-14: a safety anomaly is a VIOLATION, exit 1, on the attempt
# where it happened, never an INCONCLUSIVE one that the driver reruns with the next seed). Builds, from a copy of the tree (git archive
# of HEAD, under $TMPDIR or /var/tmp; the working tree and the shipped tsdbd are never touched), a tsdbd with a test-only mutant:
#   JV_LOSE=N    acknowledges every Nth insert request without inserting it
#   JV_NODEL=N   acknowledges every Nth range delete without deleting
# and runs the kill nemesis that SPINS where an honest one sleeps (killspin: its fault-floor and idle checks fail, so the run is
# INCONCLUSIVE in itself) through tests/jepsen/rerun.sh. Each case must end in exit 1 at the FIRST attempt (no
# "jepsen-rerun: attempt" line), with the loss named and the lazy nemesis' checks too: the history is judged whatever the nemesis did.
# On 582da46 the run ended before its history was judged: rc 3, three times, and the loss was never reported.
# Usage: tests/jepsen/mutant-loss.sh JEPSEN_DIR [JEPSEN_BIN]
set -u
dir=${1:?usage: mutant-loss.sh JEPSEN_DIR [JEPSEN_BIN]}
bin=${2:-}
root=$(cd "$(dirname "$0")/../.." && pwd)
[ -n "$bin" ] || bin=$root/build/jepsen
tmp=$(mktemp -d "${TMPDIR:-/var/tmp}/mutant-loss.XXXXXX") || exit 2
git -C "$root" archive HEAD | tar -x -C "$tmp" || exit 2
f=$tmp/server/tsdbd.c
# the hooks: a helper after the last include of the system headers, and one line at the top of the three cases
sed -i '0,/^#include <fcntl.h>$/s//#include <fcntl.h>\
static _Atomic unsigned jv_ins, jv_del; \/* test-only mutant of tests\/jepsen\/mutant-loss.sh *\/\
static int jv_every(const char *k, _Atomic unsigned *cnt)\
{\
    const char *e = getenv(k);\
    int n = e ? atoi(e) : 0;\
    return n > 0 \&\& atomic_fetch_add(cnt, 1) % (unsigned)n == (unsigned)n - 1;\
}/' "$f"
sed -i 's/^    case TSDB_OP_INSERT: {$/&\
        if (jv_every("JV_LOSE", \&jv_ins)) { rc = 0; break; }/; s/^    case TSDB_OP_INSERT_VALUES: {$/&\
        if (jv_every("JV_LOSE", \&jv_ins)) { rc = 0; break; }/; s/^    case TSDB_OP_DELETE_RANGE: {$/&\
        if (jv_every("JV_NODEL", \&jv_del)) { rc = 0; break; }/' "$f"
if [ "$(grep -c 'jv_every(' "$f")" -ne 4 ]; then echo "mutant-loss: the hooks did not apply"; rm -rf "$tmp"; exit 1; fi
(cd "$tmp" && make -j8 build/tsdbd >"$tmp/build.log" 2>&1) || { echo "mutant-loss: build failed"; tail -5 "$tmp/build.log"; rm -rf "$tmp"; exit 1; }
rc=0
for spec in 'JV_LOSE=3@to series [0-9]* key [0-9]* lost@every third insert acknowledged and dropped' \
            'JV_NODEL=1@which completed before@every range delete acknowledged and dropped'; do
    IFS=@ read -r envs pat what <<<"$spec"
    out=$(env "$envs" JEPSEN_TSDBD="$tmp/build/tsdbd" "$(dirname "$0")/rerun.sh" "$bin" --dir "$dir" --nemesis kill --seconds 20 \
        --mutant-nemesis killspin:400 2>&1); st=$?
    if [ $st -eq 1 ] && ! echo "$out" | grep -q 'jepsen-rerun: attempt' && echo "$out" | grep -q "$pat" &&
       echo "$out" | grep -q '\[inconclusive, check '; then
        echo "mutant-loss: caught at the first attempt, beside the lazy nemesis' own failure: $what"
    else
        echo "mutant-loss: '$what' went unnoticed, was rerun or was reported as INCONCLUSIVE (rc $st, wanted exit 1 at the first attempt)"
        echo "$out" | tail -n 15; rc=1
    fi
done
rm -rf "$tmp"
exit $rc
