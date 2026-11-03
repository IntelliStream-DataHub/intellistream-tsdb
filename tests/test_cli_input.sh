#!/bin/bash
# The CLI refuses input it cannot take exactly instead of acting on something else: numbers out of range or
# equal to a reserved null, times and intervals that overflow or name no real time (99:99:99, 02-31), --limit
# and group counts that are not numbers; the import's name cache does not mistake a name for a longer one with
# the same start; names are printed escaped, so a tab or newline in one cannot forge a row, and import errors
# echo the file's bytes escaped.
#
#   B=build tests/test_cli_input.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-input.XXXXXX")
trap 'rm -rf "$dir"' EXIT
errors=0
fail() { echo "test_cli_input: FAIL: $*" >&2; errors=$((errors + 1)); }
t() { "$B/tsdb" "$dir/db" "$@"; }
refused() { # refused <what> <stdin> <args...>: the command must fail and change nothing
    local what=$1 input=$2
    shift 2
    if printf '%s' "$input" | t "$@" >/dev/null 2>&1; then fail "$what was accepted"; fi
}

t create f32 --type float32 >/dev/null || fail "create f32"
t create f64 --type float64 >/dev/null || fail "create f64"
t create i64 --type int64 >/dev/null || fail "create i64"

# numbers: no clamping to the largest value or to inf, no silent null
refused "int64 below the range" $'1000,-99999999999999999999\n' insert i64
refused "int64 above the range" $'1000,99999999999999999999\n' insert i64
refused "int64 null value" $'1000,-9223372036854775808\n' insert i64
refused "float64 1e999" $'1000,1e999\n' insert f64
refused "float64 null NaN" $'1000,nan(0x8000000001001)\n' insert f64
refused "float32 1e39" $'1000,1e39\n' insert f32
refused "float32 null NaN" $'1000,nan(0x401001)\n' insert f32
refused "import int64 overflow" $'i64,1000,99999999999999999999\n' import
printf '1000,-9223372036854775807\n2000,9223372036854775807\n' | t insert i64 >/dev/null 2>&1 || fail "int64 limits"
printf '1000,inf\n2000,-inf\n3000,1e-999\n' | t insert f64 >/dev/null 2>&1 || fail "explicit inf, underflow"
out=$(t query i64 min max raw 2>/dev/null | cut -f2 | tr '\n' ' ')
[ "$out" = "-9223372036854775807 9223372036854775807 " ] || fail "int64 rows: $out"
out=$(t query f64 min max count 2>/dev/null | cut -f2)
[ "$out" = 3 ] || fail "float64 rows: $out"

# times: no wrap, no 99:99:99, no 02-31
refused "drop-before year 2147483647" "" drop-before 2147483647-01-01
refused "delete at 99:99:99" "" delete i64 '2024-01-01 99:99:99' max
refused "delete at 24:00:00" "" delete i64 '2024-01-01 24:00:00' max
refused "delete from 02-31" "" delete i64 2023-02-31 max
refused "delete from 2023-02-29" "" delete i64 2023-02-29 max
refused "delete from month 13" "" delete i64 2023-13-01 max
refused "delete from day 0" "" delete i64 2023-01-00 max
refused "delete at 00:60:00" "" delete i64 '2024-01-01 00:60:00' max
refused "delete at 00:00:60" "" delete i64 '2024-01-01 00:00:60' max
refused "delete before now+2^63-808ms (an interval in range, a sum past int64)" "" delete i64 min now+9223372036854775000
refused "delete before now-1e18d" "" delete i64 min now-1e18d
refused "delete before an integer past int64" "" delete i64 min 99999999999999999999
refused "retention 1e30" "" retention i64 1e30
refused "retention inf" "" retention i64 inf
refused "query by nan" "" query i64 min max avg nan
refused "query by 1e30d" "" query i64 min max avg 1e30d
t delete f64 2024-02-29 '2024-02-29 23:59:59.999Z' >/dev/null 2>&1 || fail "a leap day"
[ "$(t query i64 min max count 2>/dev/null | cut -f2)" = 2 ] || fail "a refused command deleted rows"

# --limit and group counts
refused "--limit abc" "" query i64 min max raw --limit abc
refused "--limit -1" "" query i64 min max raw --limit -1
refused "--limit 5x" "" query i64 min max raw --limit 5x
refused "--limit 0" "" query i64 min max raw --limit 0
refused "group 4294967297h" "" query i64 min max avg 4294967297h
out=$(t query i64 min max raw --limit 1 2>/dev/null | wc -l)
[ "$out" -eq 1 ] || fail "--limit 1 gave $out rows" # numeric: BSD wc pads

# the import's name cache: a name that is the start of a longer one is its own series
long=$(printf 'a%.0s' $(seq 1500))
short=${long:0:1023}
printf '%s,1000,1\n%s,2000,2\n' "$long" "$short" | t import >/dev/null 2>&1 || fail "import of long names"
[ "$(t query "$long" min max count 2>/dev/null | cut -f2)" = 1 ] || fail "the 1500-byte series has the wrong rows"
[ "$(t query "$short" min max count 2>/dev/null | cut -f2)" = 1 ] || fail "the 1023-byte series has the wrong rows"

# names with control bytes are escaped in the series list and in state names
t create "$(printf 'evil\tbool\t/x\nforged.series')" >/dev/null 2>&1 || fail "create evil"
t create st --type state >/dev/null || fail "create st"
printf '1000,%s\n' "$(printf 'on\x1b[2J\x7fx\\y')" | t insert st >/dev/null 2>&1 || fail "insert state"
grep -q '^forged' <(t series) && fail "a name forged a row in the series list"
t series | grep -qF 'evil\tbool\t/x\nforged.series' || fail "series list: $(t series | cat -A | tail -n 3)"
out=$(t query st min max raw 2>/dev/null | cut -f2)
[ "$out" = 'on\x1b[2J\x7fx\\y' ] || fail "state name: $out"

# nor do bytes from an import file reach the terminal through its error messages
esc=$'\x1b'
err=$(printf 'x,1000,\x1b[2Jbad\n' | t import 2>&1 >/dev/null)
case $err in *"$esc"*) fail "a bad value reached stderr raw" ;; *'\x1b[2Jbad'*) ;; *) fail "bad value: $err" ;; esac
err=$(printf '\x1b[2J%s,1000,1\n' "$(head -c 70000 /dev/zero | tr '\0' a)" | t import 2>&1 >/dev/null)
case $err in *"$esc"*) fail "a refused name reached stderr raw" ;; 'tsdb: \x1b[2Jaaa'*) ;; *) fail "refused name: ${err:0:40}" ;; esac

[ "$errors" = 0 ] || exit 1
echo "test_cli_input: ok"
