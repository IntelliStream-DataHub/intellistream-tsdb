#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `tsdb DIR stats tenants` and `stats hot` (docs/design-stats.md section 5.3): the numbers per prefix are those of the
# series under it (the longest prefix wins), the rest are "(none)", a prefix list from a file works like the arguments,
# a bad option is refused. The engine's own numbers are checked in test_tenants.c; this checks the command.
#
#   B=build tests/test_cli_stats.sh [scratch dir]
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
T=$B/tsdb
scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-cli-stats.XXXXXX") || exit 1
D=$scratch/db
fail=0
check() { # what, got, want
    if [ "$2" != "$3" ]; then echo "FAIL $1: got '$2', want '$3'"; fail=1; fi
}

$T "$D" create a/x1 > /dev/null && $T "$D" create a/b/y1 > /dev/null && $T "$D" create c/z > /dev/null 2>&1 || exit 1
for s in a/x1:1000 a/b/y1:2000 c/z:3000; do
    seq 0 $((${s#*:} - 1)) | awk '{print 1700000000000 + $1 * 1000 "," $1 * 0.5}' | $T "$D" insert "${s%:*}" > /dev/null 2>&1 || exit 1
done

col() { # line, field-regex: the number before " rows" / " series"
    echo "$1" | grep -o "[0-9]* $2" | head -1 | cut -d' ' -f1
}
out=$($T "$D" stats tenants a/ a/b/ 2>&1) || { echo "FAIL: stats tenants: $out"; fail=1; }
a=$(echo "$out" | grep '^a/	'); ab=$(echo "$out" | grep '^a/b/	'); none=$(echo "$out" | grep '^(none)	')
check "a/ series" "$(col "$a" series)" 1
check "a/ rows" "$(col "$a" rows)" 1000
check "a/b/ series" "$(col "$ab" series)" 1
check "a/b/ rows" "$(col "$ab" rows)" 2000
check "(none) rows" "$(col "$none" rows)" 3000
check "lines" "$(echo "$out" | wc -l)" 3

# the list from a file (comments and blank lines skipped) and an argument together; one prefix less than before
printf '# the a tenants\na/b/\n\n' > "$scratch/list"
out=$($T "$D" stats tenants --tenants "$scratch/list" 2>&1)
check "file: a/b/ rows" "$(col "$(echo "$out" | grep '^a/b/	')" rows)" 2000
check "file: (none) rows" "$(col "$(echo "$out" | grep '^(none)	')" rows)" 4000
check "file: lines" "$(echo "$out" | wc -l)" 2

# --volumes: one line per volume (here one)
out=$($T "$D" stats tenants a/ --volumes 2>&1)
check "volumes" "$(echo "$out" | grep -c 'volume 0')" 2

# no list: everything is "(none)"
out=$($T "$D" stats tenants 2>&1)
check "no list rows" "$(col "$out" rows)" 6000

$T "$D" stats tenants --bogus > /dev/null 2>&1; check "bad option" $? 2
$T "$D" stats hot -k 0 > /dev/null 2>&1; check "bad k" $? 2
$T "$D" stats hot > /dev/null 2>&1; check "stats hot" $? 0
$T "$D" stats hot reads -k 5 > /dev/null 2>&1; check "stats hot reads" $? 0

if [ $fail = 0 ]; then echo "test_cli_stats: ok"; command rm -r "${scratch:?}"; fi
exit $fail
