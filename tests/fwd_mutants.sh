#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The forwarding mutants (server/tsdbd.c, TSDBD_FWD_MUTANT) that test_cluster forward must catch: fwdoldframe (no age
# check: a frame held at the entry is applied), fwdunavonbreak (a written forward whose link broke answered UNAVAILABLE
# instead of closing the connection), fwdnouser (the owner runs a forward without the user's rights), fwdnowait (a
# forward's wait does not count as a waiting worker: it starves the entry's own volumes), fwdauthlag (AUTH while the
# owner's users lag the entry's); test_cluster fwdlease must catch fwdnolease (the owner skips the read lease for a
# forward) and test_cluster fwddepose fwdtwice (a deposed leader forwards a forward again); test_cluster fwdload must
# catch fwdchunknowait (an owner's worker waiting for a slow reader's credit does not count as waiting: forwarded
# inserts queue behind it) and fwdinline (forwards served on the links' dispatcher: two owners forwarding to each other
# wait for each other's credit). fwdretry (a lost forward sent again) is test_fwd's (make test runs its mutants).
# Usage: tests/fwd_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-fwdoldframe fwdunavonbreak fwdnouser fwdnowait fwdauthlag fwdnolease fwdtwice fwdchunknowait fwdinline}
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
tc=${TEST_CLUSTER:-build/test_cluster}
survived=
for m in $muts; do
    case $m in
    fwdnolease) sc=fwdlease ;;
    fwdtwice) sc=fwddepose ;;
    fwdchunknowait | fwdinline) sc=fwdload ;;
    *) sc=forward ;;
    esac
    if ! TSDBD_FWD_MUTANT=$m timeout 600 "$tc" "$dir" $sc > "$dir/mut-$m-$sc.log" 2>&1; then
        echo "mutant $m: caught by $sc ($(grep -m1 FAIL "$dir/mut-$m-$sc.log"))"
    else
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
