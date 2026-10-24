#!/bin/bash
# Runs the README's quick start word for word and checks its key output lines.
#
#   tests/test_quickstart.sh           (make test-quickstart does this after building)
#
# The block is the first fenced block after the "<!-- quickstart" marker in README.md.
# Two things are changed in a copy of it, nothing else: /tmp/tsdb-qs becomes a fresh
# scratch directory and port 17070 a free one, so the test can run next to a real
# tsdbd and next to other tests. With B=build-tsan (make tsan) build/ becomes that
# directory. If the README's commands are wrong, this fails: that is its point.
#
# What is tested: the quick start block, and the "What you should see" blocks that follow
# a "<!-- quickstart-output -->" marker (each of their lines must be in the real output,
# spaces and tabs alike, except lines with "..." and timings). The reference sections
# further down in the README ("From the command line" and so on) are NOT run.
# The block's first command is `make`: inside `make test` that builds everything once more
# (nothing to do when it is up to date).
#
# Locale: awk (mawk, the default on Debian and Ubuntu) writes "50,04" instead of "50.04"
# in a language that uses a decimal comma. When mawk and localedef exist, the block runs
# with mawk as awk under a Norwegian locale made in the scratch directory (no root needed),
# so a README that forgets `LC_ALL=C` fails. Without them the block runs in your locale.
set -u
cd "$(dirname "$0")/.."
B=${B:-build}

scratch=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-quickstart.XXXXXX")
pidfile=$scratch/qs/tsdbd.pid
cleanup() {
    # Stop only the server this test started, by the pid the README block saved.
    if [ -s "$pidfile" ]; then
        pid=$(cat "$pidfile")
        case $pid in ''|*[!0-9]*|0|1) pid= ;; esac
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            for _ in $(seq 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
        fi
    fi
    rm -rf "$scratch"
}
trap cleanup EXIT

fail() { echo "test_quickstart: FAIL: $*" >&2; echo "--- last output ---" >&2; tail -n 40 "$scratch/out" >&2
    if [ -f "$scratch/qs/tsdbd.log" ]; then echo "--- tsdbd.log ---" >&2; tail -n 20 "$scratch/qs/tsdbd.log" >&2; fi
    exit 1; }

# Extract the block.
awk '/^<!-- quickstart/ { want = 1; next }
     want && /^```sh$/ { inblock = 1; next }
     inblock && /^```$/ { exit }
     inblock { print }' README.md > "$scratch/block.orig"
[ -s "$scratch/block.orig" ] || { echo "test_quickstart: FAIL: no quick start block in README.md" >&2; exit 1; }

# A free port: nothing accepts connections on it.
port=
for _ in $(seq 50); do
    p=$((30000 + RANDOM % 2500))   # above the other server tests' 10000-29999, below the ephemeral 32768+
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then port=$p; break; fi
done
[ -n "$port" ] || { echo "test_quickstart: no free port found" >&2; exit 1; }

# Extract the expected-output blocks.
awk '/^<!-- quickstart-output/ { want = 1; next }
     want && /^```/ { if (inblock) { inblock = 0; want = 0 } else inblock = 1; next }
     inblock { print }' README.md > "$scratch/shown.txt"
[ -s "$scratch/shown.txt" ] || { echo "test_quickstart: FAIL: no output blocks in README.md" >&2; exit 1; }

# The decimal-comma setup.
runenv=()
if command -v mawk >/dev/null 2>&1 && command -v localedef >/dev/null 2>&1; then
    mkdir -p "$scratch/loc"
    localedef -i nb_NO -f UTF-8 "$scratch/loc/nb_NO.UTF-8" >/dev/null 2>&1   # warns, still writes the locale
fi
if [ -d "$scratch/loc/nb_NO.UTF-8" ]; then
    mkdir -p "$scratch/bin" && ln -s "$(command -v mawk)" "$scratch/bin/awk"
    runenv=(env "PATH=$scratch/bin:$PATH" "LOCPATH=$scratch/loc" "LC_ALL=nb_NO.UTF-8")
    echo "test_quickstart: running with mawk under a decimal-comma locale"
else
    echo "test_quickstart: no mawk or localedef: not checking a decimal-comma locale"
fi

sed -e "s#/tmp/tsdb-qs#$scratch/qs#g" -e "s#17070#$port#g" -e "s#^\\(build/\\)#$B/#" \
    -e "s#\\([ <(]\\)build/#\\1$B/#g" "$scratch/block.orig" > "$scratch/block.sh"

# -e: the first command that fails ends the quick start, as it would for a reader who pastes it.
if ! "${runenv[@]}" timeout 600 bash -e "$scratch/block.sh" > "$scratch/out" 2>&1; then
    fail "the quick start block stopped with an error"
fi

expect() {  # expect COUNT PATTERN: the output has COUNT lines containing the fixed string PATTERN
    n=$(grep -cF -- "$2" "$scratch/out")
    [ "$n" -eq "$1" ] || fail "expected $1 line(s) with '$2', found $n"
}
# Every line the README shows is in the real output (blank lines, "...", timings and speeds left out).
norm() { sed -e 's/[[:space:]][[:space:]]*/ /g' -e 's/ $//'; }
norm < "$scratch/out" > "$scratch/out.norm"
nshown=0
while IFS= read -r line; do
    case $line in ''|*...*|*rows/s*|*Elapsed*) continue ;; esac
    nshown=$((nshown + 1))
    grep -qxF -- "$(printf '%s\n' "$line" | norm)" "$scratch/out.norm" || fail "the README shows a line that the quick start does not print: $line"
done < "$scratch/shown.txt"
[ "$nshown" -ge 12 ] || fail "only $nshown lines of the README's output were checked"
expect 1 "inserted 1440 rows"
expect 1 "2024-06-01 00:01:00.000	50.0400009"
expect 1 "3 rows in set"
expect 1 "24 buckets in set"
expect 1 "2024-05-31 22:00:00.000	1320	"
expect 1 "2024-06-01 22:00:00.000	120	"
expect 2 "wrote 1440 rows"
expect 2 "read 1440 rows, the first: 1717200000000 50.00"
expect 2 "24 hours, the first average: 51.28"
expect 2 "Oslo day 1: 1320 rows, average 50.23"
expect 2 "Oslo day 2: 120 rows, average 47.47"
expect 1 "rows        1440"
grep -q "serving $scratch/qs/server on 127.0.0.1:$port" "$scratch/qs/tsdbd.log" || fail "tsdbd did not log that it serves on port $port"

# The block stopped the server.
pid=$(cat "$pidfile")
if kill -0 "$pid" 2>/dev/null; then fail "the server is still running after the quick start's stop step"; fi
# The examples report an error and exit with 1 (no crash): nothing listens on the port any more,
# and /proc is not a place for a database.
"$B/client-example" 127.0.0.1 "$port" > "$scratch/err1" 2>&1; rc=$?
[ "$rc" -eq 1 ] && grep -q "tsdbc_connect.*failed" "$scratch/err1" || { cp "$scratch/err1" "$scratch/out"; fail "client-example without a server: exit $rc, expected 1 and a message"; }
"$B/embedded-example" /proc/nodb > "$scratch/err2" 2>&1; rc=$?
[ "$rc" -eq 1 ] && grep -q "tsdb_open.*failed" "$scratch/err2" || { cp "$scratch/err2" "$scratch/out"; fail "embedded-example in a bad directory: exit $rc, expected 1 and a message"; }
echo "test_quickstart: ok (port $port)"
