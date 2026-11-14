#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `tsdb stats series|tenants|hot ... --server HOST:PORT` (docs/design-stats.md section 5.3, protocol 15): the numbers
# of a running tsdbd as the user who logs in. A superuser's `stats series` is the offline command's output byte for
# byte; a tenant admin sees its own tenant, is refused the others' series and the "(none)" tenant, and gets its own
# requests' cost with --caller; the password is never an argument; the server can come from the configuration file.
#
#   B=build tests/test_cli_stats_net.sh [scratch dir]
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
T=$B/tsdb
scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-cli-stats-net.XXXXXX") || exit 1
D=$scratch/db
me=$BASHPID
srv=""
fail=0
failed() { echo "FAIL $*"; fail=1; }
check() { # what, got, want
    if [ "$2" != "$3" ]; then echo "FAIL $1: got '$2', want '$3'"; fail=1; fi
}
alive() {
    local st
    st=$(ps -o stat= -p "$1" 2>/dev/null)
    [ -n "$st" ] && [ "${st#Z}" = "$st" ]
}
stop_srv() {
    local i
    [[ "$1" =~ ^[0-9]+$ ]] && [ "$1" -gt 1 ] || { echo "stop_srv: refusing pid '$1'" >&2; return 1; }
    kill "$1" 2>/dev/null
    for ((i = 0; i < 100; i++)); do alive "$1" || break; sleep 0.1; done
    alive "$1" && kill -9 "$1" 2>/dev/null
    wait "$1" 2>/dev/null
}
cleanup() {
    [ -n "$srv" ] && stop_srv "$srv"
    command rm -rf "${scratch:?}"
}
trap cleanup EXIT

pw() { printf '%s\n' "$2" > "$scratch/$1.pw"; chmod 600 "$scratch/$1.pw"; }
pw root rootpw; pw boss bosspw; pw viewer viewerpw
for s in acme/x1:1000 acme/x2:500 other/y1:2000; do
    $T "$D" create "${s%:*}" > /dev/null 2>&1 || exit 1
    seq 0 $((${s#*:} - 1)) | awk '{print 1700000000000 + $1 * 1000 "," $1 * 0.5}' | $T "$D" insert "${s%:*}" > /dev/null 2>&1 || exit 1
done
$T "$D" user add root --superuser --password-file "$scratch/root.pw" > /dev/null 2>&1 || exit 1
$T "$D" user add acme/boss --password-file "$scratch/boss.pw" > /dev/null 2>&1 || exit 1
$T "$D" user grant acme/boss read,insert,admin prefix:acme/ > /dev/null 2>&1 || exit 1
offline=$($T "$D" stats series acme/x1 acme/x2 other/y1 2>&1) || { echo "FAIL offline stats series: $offline"; exit 1; }
printf 'acme/\nother/\n' > "$scratch/tenants"

"$B/tsdbd" --dir "$D" --port 0 --port-file "$scratch/port" --tenants "$scratch/tenants" --exit-with-parent=$me \
    > "$scratch/log" 2>&1 &
srv=$!
for _ in $(seq 1 100); do
    [ -s "$scratch/port" ] && break
    alive "$srv" || break
    sleep 0.1
done
[ -s "$scratch/port" ] || { echo "FAIL: tsdbd did not start: $(tail -3 "$scratch/log")"; exit 1; }
read -r PORT PID _ < "$scratch/port"
[ "$PID" = "$srv" ] || { echo "FAIL: the port file names pid $PID, not $srv"; exit 1; }
S=127.0.0.1:$PORT
R="--server $S --login root --login-password-file $scratch/root.pw"
A="--server $S --login acme/boss --login-password-file $scratch/boss.pw"

# a superuser's series statistics are the offline command's, and the same call's second answer is the first's
out=$($T stats series acme/x1 acme/x2 other/y1 $R 2>&1) || failed "root: stats series: $out"
check "root: stats series equals the offline output" "$out" "$offline"

# the tenant admin: its own series, not the other's (denied whether it exists or not)
out=$($T stats series acme/x1 $A 2>&1) || failed "tenant admin: own series: $out"
check "tenant admin: acme/x1 line" "$out" "$(echo "$offline" | grep '^acme/x1	')"
out=$($T stats series other/y1 $A 2>&1); rc=$?
[ $rc -ne 0 ] && echo "$out" | grep -qi "denied" || failed "tenant admin: other/y1 is not denied (rc $rc): $out"
out2=$($T stats series other/missing $A 2>&1); rc2=$?
check "denied and missing look alike" "$out2" "${out//other\/y1/other\/missing}"
out=$($T stats series acme/nothere $A 2>&1); rc=$?
[ $rc -ne 0 ] || failed "tenant admin: a missing series inside its grants succeeded: $out"

# tenants: wait until the sweep is done (no "stale" note), then the numbers are the offline ones
tries=0
while :; do
    out=$($T stats tenants $R 2>$scratch/err); st=$?
    [ $st -eq 0 ] && ! grep -q stale "$scratch/err" && break
    tries=$((tries + 1)); [ $tries -gt 100 ] && { failed "stats tenants stays stale or fails: $(cat "$scratch/err")"; break; }
    sleep 0.2
done
col() { echo "$1" | grep -o "[0-9]* $2" | head -1 | cut -d' ' -f1; }
acme=$(echo "$out" | grep '^acme/	'); oth=$(echo "$out" | grep '^other/	'); none=$(echo "$out" | grep '^(none)	')
check "root: acme/ series" "$(col "$acme" series)" 2
check "root: acme/ rows" "$(col "$acme" rows)" 1500
check "root: other/ rows" "$(col "$oth" rows)" 2000
[ -n "$none" ] || failed "root: no (none) row: $out"
out=$($T stats tenants acme/ $A 2>&1) || failed "tenant admin: tenants acme/: $out"
check "tenant admin: acme/ rows" "$(col "$out" rows)" 1500
out=$($T stats tenants $A 2>&1) || failed "tenant admin: tenants: $out"
check "tenant admin: lists only its tenant" "$(echo "$out" | cut -f1 | sort -u)" "acme/"
out=$($T stats tenants other/ $A 2>&1); rc=$?
[ $rc -ne 0 ] || failed "tenant admin: another tenant's numbers were given: $out"
out=$($T stats tenants '' $A 2>&1); rc=$?
[ $rc -ne 0 ] || failed "tenant admin: the (none) tenant was given: $out"
out=$($T stats tenants acme/ --volumes $A 2>&1) || failed "tenant admin: --volumes: $out"
echo "$out" | grep -q "volume 0" || failed "--volumes prints no volume row: $out"

# what the admin's own requests cost: the series calls above were requests of this user
out=$($T stats tenants acme/ --caller $A 2>&1) || failed "tenant admin: --caller: $out"
echo "$out" | grep -Eq "	caller	.*[1-9][0-9]* other" || failed "--caller: the statistics requests are not counted: $out"
out=$($T stats tenants acme/ $A 2>&1) || failed "tenant admin: tenants: $out"
echo "$out" | grep -q "	caller	" && failed "a caller line without --caller: $out"
out=$($T stats tenants acme/ --hot 3 $A 2>&1) || failed "tenant admin: --hot: $out"
out=$($T stats hot -k 5 $R 2>&1) || failed "root: stats hot: $out"
out=$($T stats hot reads -k 5 $R 2>&1) || failed "root: stats hot reads: $out"

# a bad command line is refused, and a password is never an argument
$T stats series $R --password hunter2 > /dev/null 2>&1 && failed "--password was accepted"
$T stats series $R > /dev/null 2>&1 && failed "stats series without a name was accepted"
$T stats tenants --server $S --login-password-file "$scratch/root.pw" > /dev/null 2>&1 && failed "a password file without a login"
$T stats hot bogus $R > /dev/null 2>&1 && failed "stats hot bogus was accepted"
$T stats tenants --bogus $R > /dev/null 2>&1 && failed "an unknown option was accepted"
$T stats series acme/x1 --server $S --login acme/boss --login-password-file "$scratch/root.pw" > /dev/null 2>&1 \
    && failed "a wrong password logged in"
$T stats series acme/x1 --server 127.0.0.1 --login root --login-password-file "$scratch/root.pw" > /dev/null 2>&1 && failed "--server without a port was accepted"

# the server and the login from the configuration file
{ echo "client.server = $S"; echo "client.login = root"; echo "client.login.password.file = $scratch/root.pw"; } \
    > "$scratch/cli.properties"
chmod 600 "$scratch/cli.properties"
out=$($T --config "$scratch/cli.properties" stats series acme/x1 other/y1 2>&1) || failed "config file: $out"
check "config file: the same lines" "$out" "$(echo "$offline" | grep -v '^acme/x2	')"

[ $fail = 0 ] && echo "test_cli_stats_net: ok"
exit $fail
