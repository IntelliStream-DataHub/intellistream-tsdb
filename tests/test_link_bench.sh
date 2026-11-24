#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# tools/link-bench on loopback (docs/design-rdma.md rests on its numbers): the frames arrive in order with their tags
# checked, a wrong tag is counted by the server and fails the run, the batched writes deliver every frame, --buf on
# both sides works, and a lat run's round trips add up to its time (a tool that timed half a round trip would not).
# LINK_BENCH names another binary (a planted bug) to check that this test notices it. A few seconds.
set -u
B=${B:-build}
LB=${LINK_BENCH:-$B/link-bench}
[ -x "$LB" ] || { echo "test_link_bench: no $LB (make $B/link-bench)"; exit 1; }
d=$(mktemp -d "${TMPDIR:-/tmp}/test_link_bench.XXXXXX") || exit 1
srv_pid=
stop() {
    case $srv_pid in '' | 0 | -*) ;; *) kill "$srv_pid" 2>/dev/null; wait "$srv_pid" 2>/dev/null ;; esac
    srv_pid=
}
trap 'stop; rm -rf "$d"' EXIT
fails=0
fail() { echo "FAIL: $*"; fails=$((fails + 1)); }

start() { # $@: extra serve flags; sets port
    stop
    rm -f "$d/pf"
    me=$BASHPID
    "$LB" serve --bind 127.0.0.1 --port-file "$d/pf" --parent "$me" "$@" >"$d/serve.log" 2>&1 &
    srv_pid=$!
    for _ in $(seq 100); do [ -s "$d/pf" ] && break; kill -0 "$srv_pid" 2>/dev/null || break; sleep 0.05; done
    read -r port pid <"$d/pf" 2>/dev/null || { echo "test_link_bench: the server did not start"; cat "$d/serve.log"; exit 1; }
    [ "$pid" = "$srv_pid" ] || { echo "test_link_bench: the port file names pid $pid, not $srv_pid"; exit 1; }
}
field() { echo "$1" | tr ' ' '\n' | sed -n "s/^$2=//p"; }
run() { # $1 expected exit code, $2 case name, $3.. flags; sets out
    local want=$1 name=$2 rc
    shift 2
    out=$("$LB" run --host 127.0.0.1 --port "$port" --secs 0.3 --label "$name" "$@" 2>&1)
    rc=$?
    [ "$rc" = "$want" ] || fail "$name: exit $rc, want $want: $out"
}

start
run 0 bw-tag --mode bw --size 1024 --tag
[ "$(field "$out" srv_errors)" = 0 ] || fail "bw-tag: srv_errors $(field "$out" srv_errors)"
[ "${out#*frames=0 }" = "$out" ] || fail "bw-tag: no frames"
run 0 bw-batch --mode bw --size 64 --tag --batch 16
[ "$(field "$out" batch)" = 16 ] || fail "bw-batch: batch=$(field "$out" batch)"
run 0 bw-plain --mode bw --size 1
run 1 bw-badtag --mode bw --size 256 --tag --bad-tag-at 3
[ "$(field "$out" srv_errors)" = 1 ] || fail "bw-badtag: srv_errors '$(field "$out" srv_errors)', want 1: $out"
run 0 lat-tag --mode lat --size 1024 --tag
# the loop does nothing but round trips: their sum is most of the run (half of it if a round trip were halved)
awk -v s="$(field "$out" rtt_sum_s)" -v t="$(field "$out" secs)" 'BEGIN { exit !(t > 0 && s / t >= 0.75 && s <= t) }' ||
    fail "lat-tag: round trips add up to $(field "$out" rtt_sum_s) s of a $(field "$out" secs) s run"
run 0 lat-plain --mode lat --size 1
start --buf 1048576
run 0 bw-buf --mode bw --size 65536 --tag --buf 1048576
stop
grep -q . "$d/serve.log" && { echo "server log:"; cat "$d/serve.log"; }
[ "$fails" = 0 ] || { echo "test_link_bench: $fails failed"; exit 1; }
echo "test_link_bench: ok"
