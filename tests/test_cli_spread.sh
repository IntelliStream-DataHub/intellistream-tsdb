#!/bin/bash
# The CLI's range, stddev, stddev_samp, var and var_samp modes and the three columns `all` gains: the
# values of a known set, the int64 range printed exactly (it needs 64 unsigned bits), \N for a bucket of
# only nulls.
#
#   B=build tests/test_cli_spread.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-spread.XXXXXX")
trap 'rm -rf "$dir"' EXIT
fail() { echo "test_cli_spread: FAIL: $*" >&2; exit 1; }
q() { "$B/tsdb" "$dir/db" query "$@" 2>/dev/null | cut -f2-; }

# 2, 4, 4, 4, 5, 5, 7, 9: mean 5, population variance 4, sample variance 32/7
"$B/tsdb" "$dir/db" create f --type float64 >/dev/null || fail "create f"
i=0
for v in 2 4 4 4 5 5 7 9; do echo "$((1700000000000 + i * 1000)),$v"; i=$((i + 1)); done |
    "$B/tsdb" "$dir/db" insert f >/dev/null 2>&1 || fail "insert f"
[ "$(q f min max range)" = 7 ] || fail "range: $(q f min max range)"
[ "$(q f min max stddev)" = 2 ] || fail "stddev: $(q f min max stddev)"
[ "$(q f min max var)" = 4 ] || fail "var: $(q f min max var)"
[ "$(q f min max var_samp)" = 4.57142857 ] || fail "var_samp: $(q f min max var_samp)"
[ "$(q f min max stddev_samp)" = 2.13808994 ] || fail "stddev_samp: $(q f min max stddev_samp)"
hdr=$("$B/tsdb" "$dir/db" query f min max all 2>/dev/null | head -n 1)
[ "$hdr" = "$(printf 'time\tcount\tsum\tmin\tmax\tavg\trange\tstddev_pop\tstddev_samp')" ] || fail "header: $hdr"
[ "$(q f min max all | tail -n 1)" = "$(printf '8\t40\t2\t9\t5\t7\t2\t2.13808994')" ] || fail "all: $(q f min max all)"

"$B/tsdb" "$dir/db" create i --type int64 >/dev/null || fail "create i"
printf '1700000000000,-9223372036854775807\n1700000001000,9223372036854775807\n1700000002000,\n' |
    "$B/tsdb" "$dir/db" insert i >/dev/null 2>&1 || fail "insert i"
[ "$(q i min max range)" = 18446744073709551614 ] || fail "int64 range: $(q i min max range)"
[ "$(q i 2023-11-14T22:13:22Z 2023-11-14T22:13:23Z range)" = '\N' ] || fail "null range"
# a bucket of nulls only: \N for the spread as for sum, min and max
[ "$(q i 2023-11-14T22:13:22Z 2023-11-14T22:13:23Z stddev_samp)" = '\N' ] || fail "null stddev_samp"
[ "$(q i 2023-11-14T22:13:22Z 2023-11-14T22:13:23Z var)" = '\N' ] || fail "null var"
# stddev and var are the population ones
[ "$(q i min max stddev)" = "$(q i min max stddev_pop)" ] || fail "stddev alias"
[ "$(q i min max var)" = "$(q i min max var_pop)" ] || fail "var alias"
echo "test_cli_spread: ok"
