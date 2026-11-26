#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `tsdb chunk-store init PATH --cluster UUID --node ID` (design-cluster-data.md R8 D11): the store's META names the
# cluster and the node, which tsdbd --chunk-dir checks at start; a bad uuid, a bad node id, a missing option and a
# second init of the same directory are refused and leave nothing behind.
#
#   B=build tests/test_cli_chunkstore.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-chunkstore.XXXXXX")
trap 'rm -rf "$dir"' EXIT
errors=0
fail() { echo "test_cli_chunkstore: FAIL: $*" >&2; errors=$((errors + 1)); }
cs() { "$B/tsdb" chunk-store "$@"; }
uuid=0123456789abcdef0123456789abcdef

out=$(cs init "$dir/s1" --cluster $uuid --node 7) || fail "init refused: $out"
echo "$out" | grep -q "cluster $uuid, node 7" || fail "init does not report the owner: $out"
[ -f "$dir/s1/META" ] || fail "no META in the store"
grep -qx "cluster=$uuid" "$dir/s1/META" || fail "META does not name the cluster: $(cat "$dir/s1/META")"
grep -qx "node=7" "$dir/s1/META" || fail "META does not name the node: $(cat "$dir/s1/META")"
id=$(sed -n 's/^id=//p' "$dir/s1/META")
[ ${#id} = 32 ] || fail "META has no 32-digit store id: $(cat "$dir/s1/META")"
echo "$out" | grep -q "id $id" || fail "init reports another id than META's: $out"

# the options may come in any order
cs init --node 4294967295 --cluster $uuid "$dir/s2" >/dev/null || fail "options before the path refused"
grep -qx "node=4294967295" "$dir/s2/META" || fail "the largest node id is not kept"

before=$(cat "$dir/s1/META")
cs init "$dir/s1" --cluster $uuid --node 8 >/dev/null 2>&1 && fail "a second init of a store was accepted"
[ "$(cat "$dir/s1/META")" = "$before" ] || fail "a refused second init changed META"

refused() { # $1: what, the rest: the arguments; the command must fail and make no directory
    local what=$1
    shift
    cs "$@" >/dev/null 2>&1 && fail "$what was accepted"
    [ -e "$dir/bad" ] && fail "$what left $dir/bad" && rm -rf "$dir/bad"
}
refused "a short uuid" init "$dir/bad" --cluster 0123456789abcdef --node 1
refused "a uuid with a non-hex digit" init "$dir/bad" --cluster 0123456789abcdef0123456789abcdeg --node 1
refused "node 0" init "$dir/bad" --cluster $uuid --node 0
refused "a node id past 32 bits" init "$dir/bad" --cluster $uuid --node 4294967296
refused "a node id with a tail" init "$dir/bad" --cluster $uuid --node 3x
refused "a node id with a leading space" init "$dir/bad" --cluster $uuid --node " 5"
refused "a node id with a plus sign" init "$dir/bad" --cluster $uuid --node +5
refused "a negative node id" init "$dir/bad" --cluster $uuid --node -1
refused "a negative node id" init "$dir/bad" --cluster $uuid --node -1
refused "no --node" init "$dir/bad" --cluster $uuid
refused "no --cluster" init "$dir/bad" --node 1
refused "no path" init --cluster $uuid --node 1
refused "an unknown verb" format "$dir/bad" --cluster $uuid --node 1
refused "an unknown option" init "$dir/bad" --cluster $uuid --node 1 --force
err=$(cs init "$dir/bad" --cluster nope --node 1 2>&1)
echo "$err" | grep -q "nope is not a cluster uuid" || fail "a bad uuid's message does not name it: $err"

# SEC-05: symlinked target directories (with or without trailing slash, or dangling) are refused
mkdir "$dir/real_target"
ln -s "$dir/real_target" "$dir/sym_store"
ln -s "$dir/nonexistent_target" "$dir/sym_dangle"
cs init "$dir/sym_store" --cluster $uuid --node 1 >/dev/null 2>&1 && fail "symlinked store path was accepted"
cs init "$dir/sym_store/" --cluster $uuid --node 1 >/dev/null 2>&1 && fail "symlinked store path with slash was accepted"
cs init "$dir/sym_dangle" --cluster $uuid --node 1 >/dev/null 2>&1 && fail "dangling symlink store path was accepted"
[ ! -e "$dir/real_target/META" ] || fail "symlinked init created META in real_target"
[ ! -e "$dir/nonexistent_target" ] || fail "dangling symlink init created nonexistent_target"

if [ $errors -ne 0 ]; then
    echo "test_cli_chunkstore: $errors failure(s)"
    exit 1
fi
echo "test_cli_chunkstore: ok"
