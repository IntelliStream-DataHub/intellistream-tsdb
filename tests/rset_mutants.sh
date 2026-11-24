#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replica set change mutants (r4-setchg): each planted bug must fail its test_cluster scenario with that
# scenario's own FAIL line. The driver's (server/rset.c, TSDB_REPLD_MUTANT): setkeepold (the driver keeps the set it
# was opened with: the learner is never sent the log) addreplica, setnoversion (no set version check on messages in)
# setstale, setdrvnoremap (the core reconfigured, the driver's per-peer state left at the old indexes) removeindex, setnextstale (a
# record that came during a drain applied with the maps of the one before) setnext. The
# volume manager's (server/cvolset.c, cvolset_mgr.c, TSDB_CVOL_MUTANT): removedopen (a removed replica is not closed)
# removereplica, staleopen (the prepare opens a v<N> whose REPL holds another rid) removedown, makemissing (a v<N> the
# group recorded as opened and that is gone is made again) resetlost. The reset's (server/cset.c, TSDB_CLUSTER_MUTANT):
# resetinisr (proposed for an ISR member) and resetsamerid (the reset keeps the rid: it never applies) resetlost. And
# setoldofnode (rset.c: a reset member mapped to its old index, its old ack carried over) and lostasstale (cvolset.c: a
# reset seen while running sets the old v<N> aside as stale-, not lost-) resetlost. And setkeepleave (rset.c: a set
# with no member moving in keeps the core's leaving member, after a cancel whose DROP_MEMBER(0) was lost) movecancel.
# Usage: tests/rset_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
table=(
    "setkeepold|addreplica|no 1 lines with \"replica set version"
    "setnoversion|setstale|no 1 lines with \"messages of another replica set version"
    "setdrvnoremap|removeindex|the driver carried an ack of [0-9]+ over for server"
    "removedopen|removereplica|still has v1 or v1.removed|no 1 lines with \"this server's replica was removed"
    "staleopen|removedown|still has v1 or v1.removed-[*] 20 s after it started|pass 1: server [0-9]+ has no v1.stale"
    "makemissing|resetlost|made a v1 for a replica the group recorded as opened"
    "resetinisr|resetlost|a reset of (the leader|an ISR member): exit"
    "resetsamerid|resetlost|the reset of server [0-9]+ did not apply"
    "setnextstale|setnext|the leader's set change line does not name servers|no 1 lines with \"replica set version [0-9]+ -> [0-9]+\" in"
    "setoldofnode|resetlost|the leader's set change lines name server [0-9]+ \\(node [0-9]+\\) new"
    "lostasstale|resetlost|pass 2: server [0-9]+ holds v1.stale-"
    "setkeepleave|movecancel|A back in the ISR after the cancel whose DROP_MEMBER\\(0\\) was lost"
)
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
row_of() {
    local e
    row=
    for e in "${table[@]}"; do [ "${e%%|*}" = "$1" ] && row=$e; done
    [ -n "$row" ] || return 1
    rest=${row#*|}; s=${rest%%|*}; want=${rest#*|}
}
case ${1:-} in
--mutant-run)
    row_of "$2" || { echo "mutant $2: no scenario is named for it in $0"; exit 0; }
    TSDB_REPLD_MUTANT=$2 TSDB_CVOL_MUTANT=$2 TSDB_CLUSTER_MUTANT=$2 exec "$TEST_CLUSTER" "$3" "$s" ;;
--mutant-verify)
    row_of "$2" || { echo "no scenario is named for it in $0"; exit 1; }
    fail=$(grep -m1 FAIL "$4")
    if [ "$3" -ne 0 ] && grep -m1 FAIL "$4" | grep -qE "$want"; then
        echo "caught by $s ($fail)"
    elif [ "$3" -ne 0 ]; then
        echo "$s failed (rc $3) but not with its oracle's line /$want/: ${fail:-no FAIL line}"
        exit 1
    else
        echo "$s passed"
        exit 1
    fi
    exit 0 ;;
esac
dir=${1:-/tmp}; shift || true
mkdir -p "$dir" || exit 1
all=
for e in "${table[@]}"; do all="$all ${e%%|*}"; done
muts=${*:-$all}
exec "$(dirname "$0")/../tools/mutants.sh" --name rset --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --fail \
    --verify "$0" --list "$muts" -- "$0" --mutant-run @M@ @D@
