#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The CLI's `latest NAME... [--with-nulls] [--end TIME]`: the newest value of each series (the last non-null row of
# a read), a null row only with --with-nulls, `\N \N` for a series with no row, every value type, --end as a bound,
# and an unknown series or a bad option refused.
#
#   B=build tests/test_cli_latest.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-latest.XXXXXX")
trap 'rm -rf "$dir"' EXIT
fail() { echo "test_cli_latest: FAIL: $*" >&2; exit 1; }
cli() { "$B/tsdb" "$dir/db" "$@"; }

cli create f --type float32 >/dev/null || fail "create f"
cli create d --type float64 >/dev/null || fail "create d"
cli create i --type int64 >/dev/null || fail "create i"
cli create b --type bool >/dev/null || fail "create b"
cli create e --type float64 >/dev/null || fail "create e"
cli create 'odd	name' --type float64 >/dev/null || fail "create odd"
printf '1700000000000,1.5\n1700000001000,2.25\n' | cli insert f >/dev/null 2>&1 || fail "insert f"
printf '1700000000000,0.1\n1700000001000,null\n1700000002000,null\n' | cli insert d >/dev/null 2>&1 || fail "insert d"
printf '1700000000000,-5\n1700000009000,9007199254740993\n' | cli insert i >/dev/null 2>&1 || fail "insert i"
printf '1700000000000,true\n1700000001000,false\n' | cli insert b >/dev/null 2>&1 || fail "insert b"
printf '1700000000000,7\n' | cli insert 'odd	name' >/dev/null 2>&1 || fail "insert odd"

out=$(cli latest f d i b e) || fail "latest"
want=$(printf 'f\t2023-11-14 22:13:21.000\t2.25\nd\t2023-11-14 22:13:20.000\t0.10000000000000001\ni\t2023-11-14 22:13:29.000\t9007199254740993\nb\t2023-11-14 22:13:21.000\tfalse\ne\t\\N\t\\N')
[ "$out" = "$want" ] || fail "latest printed:
$out
wanted:
$want"
# with nulls the newest row of d is a null
out=$(cli latest d --with-nulls) || fail "latest --with-nulls"
[ "$out" = "$(printf 'd\t2023-11-14 22:13:22.000\t\\N')" ] || fail "with-nulls: $out"
# --end is exclusive: rows before it
out=$(cli latest f i --end 1700000001000) || fail "latest --end"
[ "$out" = "$(printf 'f\t2023-11-14 22:13:20.000\t1.5\ni\t2023-11-14 22:13:20.000\t-5')" ] || fail "--end: $out"
# the same as the last row of a read
[ "$(cli latest f | cut -f2,3)" = "$(cli query f min max raw | tail -n 1)" ] || fail "not the last row of a raw read"
# a name with a tab is escaped like everywhere
cli latest 'odd	name' | grep -q '^odd\\tname	' || fail "name not escaped"
# refusals
if cli latest nosuch >/dev/null 2>&1; then fail "an unknown series was accepted"; fi
if cli latest >/dev/null 2>&1; then fail "no series accepted"; fi
if cli latest f --end >/dev/null 2>&1; then fail "--end without a time accepted"; fi
if cli latest f --end x >/dev/null 2>&1; then fail "--end x accepted"; fi
echo "test_cli_latest: ok"
