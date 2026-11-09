#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `tsdb cluster` without a verb (or with help) prints the cluster's commands and options on stdout and succeeds, as the
# general usage line promises ("tsdb cluster for the details"); a mistake in a command prints the same text on stderr
# and exits 2; `secret` makes a private file and never overwrites one. It started as the general usage with exit 2.
#
#   B=build tests/test_cli_cluster.sh [scratch dir]
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
T=$B/tsdb
scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-cli-cluster.XXXXXX") || exit 1
trap 'rm -rf "$scratch"' EXIT
fail=0
check() { # what, got, want
    if [ "$2" != "$3" ]; then echo "FAIL $1: got '$2', want '$3'"; fail=1; fi
}

for args in "" help --help -h; do
    # shellcheck disable=SC2086
    out=$($T cluster $args 2>"$scratch/err"); rc=$?
    check "tsdb cluster $args: exit" "$rc" 0
    check "tsdb cluster $args: stderr" "$(cat "$scratch/err")" ""
    for want in "usage: tsdb cluster secret FILE" "tsdb cluster init PEER" "tsdb cluster node add PEER NEW_PEER" \
        "tsdb cluster status PEER" "--secret-file" "--node-timeout-ms" "--voter-replace-s" "--mode replicated|shared" \
        "--failover lease|attested|bmc" "--read-barrier on|off" "--lease-s" "--replication" "--min-isr" \
        "--heartbeat-ms" "--isr-lag-ms" "--out-after-s" "--voters" "--volumes-per-server" "docs/cluster-preview.md" \
        "marked out [1800] (stored, not acted on yet)"; do
        case $out in *"$want"*) ;; *) echo "FAIL tsdb cluster $args: no '$want' in the help"; fail=1 ;; esac
    done
    case $out in *"usage: tsdb <dir> <command>"*) echo "FAIL tsdb cluster $args: the general usage"; fail=1 ;; esac
done

# a mistake: the cluster help on stderr, exit 2, nothing on stdout
for args in "bogus" "status" "init" "node add" "secret" "node nope a b"; do
    # shellcheck disable=SC2086
    out=$($T cluster $args 2>"$scratch/err"); rc=$?
    check "tsdb cluster $args: exit" "$rc" 2
    check "tsdb cluster $args: stdout" "$out" ""
    grep -q "tsdb cluster init PEER" "$scratch/err" || { echo "FAIL tsdb cluster $args: stderr has no cluster help"; fail=1; }
done

# the secret: made once, mode 0600, never overwritten
$T cluster secret "$scratch/secret" > "$scratch/out" 2>&1; check "secret: exit" "$?" 0
check "secret: mode" "$(stat -c %a "$scratch/secret" 2>/dev/null || stat -f %Lp "$scratch/secret")" 600
cp "$scratch/secret" "$scratch/before"
$T cluster secret "$scratch/secret" > "$scratch/out" 2>&1; rc=$?
[ "$rc" -ne 0 ] || { echo "FAIL secret: a second run overwrote the file"; fail=1; }
cmp -s "$scratch/secret" "$scratch/before" || { echo "FAIL secret: the file changed"; fail=1; }

# the general usage names the cluster
$T 2> "$scratch/err"; check "tsdb: exit" "$?" 2
grep -q "tsdb cluster secret|init|node add|status" "$scratch/err" || { echo "FAIL tsdb: the usage does not name the cluster commands"; fail=1; }

[ "$fail" -eq 0 ] && echo "test_cli_cluster: ok"
exit $fail
