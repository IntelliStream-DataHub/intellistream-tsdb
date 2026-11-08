#!/bin/bash
# `tsdb DIR feed ...` (docs/design-cdc.md 2.13): enable, info, new-id and disable on a closed database; the files each
# leaves (META requires=feed, FEED, FEEDSLOTS) in every volume; the verbs refused beside a running writer (the LOCK of
# a tsdbd) and on bad options; a feed that is off changes nothing.
#
#   B=build tests/test_cli_feed.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-feed.XXXXXX")
trap 'rm -rf "$dir"' EXIT
errors=0
fail() { echo "test_cli_feed: FAIL: $*" >&2; errors=$((errors + 1)); }
t() { "$B/tsdb" "$dir/db" "$@"; }
db=$dir/db
vol=$dir/vol1

mkdir "$vol" || exit 1
t create a --type float64 >/dev/null || fail "create in the root"
t volume add "$vol" >/dev/null || fail "volume add"
t create b "$vol" >/dev/null || fail "create in the volume"

out=$(t feed info) || fail "feed info on a database without a feed"
[ "$(echo "$out" | grep -c "off$")" = 2 ] || fail "info does not say off for both volumes: $out"
[ -e "$db/FEED" ] && fail "FEED exists before enable"
grep -q feed "$db/META" && fail "META names the feed before enable"

t feed bogus >/dev/null 2>&1 && fail "an unknown verb was accepted"
t feed enable --retention 0s >/dev/null 2>&1 && fail "retention 0 was accepted"
t feed enable --max-bytes 12x >/dev/null 2>&1 && fail "a bad byte count was accepted"
t feed enable --min-free-pct 95 >/dev/null 2>&1 && fail "min-free-pct 95 was accepted"
[ -e "$db/FEED" ] && fail "a refused enable left FEED"

out=$(t feed enable --retention 2h --max-bytes 3M --min-free-pct 7) || fail "feed enable"
[ "$(echo "$out" | grep -c "	on	")" = 2 ] || fail "enable does not report both volumes on: $out"
echo "$out" | grep -q "retention 7200s" || fail "retention not 7200s: $out"
echo "$out" | grep -q "max_bytes 3145728" || fail "max_bytes not 3 MiB: $out"
echo "$out" | grep -q "min_free 7%" || fail "min_free not 7%: $out"
for d in "$db" "$vol"; do
    for f in FEED FEEDSLOTS; do [ -f "$d/$f" ] || fail "$d/$f missing after enable"; done
    grep -q "requires=feed" "$d/META" || fail "$d/META lacks requires=feed"
done
id0=$(t feed info | sed -n 1p | sed 's/.*feed_id \([0-9a-f]*\).*/\1/')
id1=$(t feed info | sed -n 2p | sed 's/.*feed_id \([0-9a-f]*\).*/\1/')
[ -n "$id0" ] && [ "$id0" != "$id1" ] || fail "the volumes share a feed id or it is empty: $id0 $id1"

t feed enable >/dev/null || fail "enable twice is not idempotent"
t create c "$vol" >/dev/null || fail "create with the feed on"
t feed new-id >/dev/null || fail "feed new-id"
n0=$(t feed info | sed -n 1p | sed 's/.*feed_id \([0-9a-f]*\).*/\1/')
[ "$n0" != "$id0" ] || fail "new-id kept the feed id"

# a volume added with the feed on starts with a feed of its own
vol2=$dir/vol2
mkdir "$vol2"
t volume add "$vol2" >/dev/null || fail "volume add with the feed on"
[ -f "$vol2/FEED" ] && grep -q requires=feed "$vol2/META" || fail "a volume added with the feed on has no feed"

# beside a running writer (the lock)
t feed info >/dev/null || fail "info after the volume add"
me=$BASHPID
"$B/tsdbd" --dir "$db" --no-auth --port 0 --port-file "$dir/port" --exit-with-parent=$me >"$dir/d.log" 2>&1 &
pid=$!
for _ in $(seq 100); do [ -s "$dir/port" ] && break; sleep 0.1; done
if [ -s "$dir/port" ]; then
    t feed disable >/dev/null 2>&1 && fail "disable beside a running tsdbd was accepted"
    t feed enable >/dev/null 2>&1 && fail "enable beside a running tsdbd was accepted"
    [ -f "$db/FEED" ] || fail "the refused disable removed FEED"
else
    fail "tsdbd did not start: $(tail -3 "$dir/d.log")"
fi
kill "$pid" 2>/dev/null
wait "$pid" 2>/dev/null

t feed disable >/dev/null || fail "feed disable"
for d in "$db" "$vol" "$vol2"; do
    for f in FEED FEEDSLOTS; do [ -e "$d/$f" ] && fail "$d/$f left after disable"; done
    ls "$d" | grep -q '^fwal_' && fail "$d keeps fwal_ segments after disable"
    grep -q feed "$d/META" && fail "$d/META still names the feed"
done
t feed disable >/dev/null || fail "disable twice is not idempotent"
out=$(t feed info) || fail "info after disable"
[ "$(echo "$out" | grep -c "off$")" = 3 ] || fail "info after disable: $out"
[ "$(t series | wc -l)" = 3 ] || fail "the series are gone"

[ "$errors" = 0 ] && echo "test_cli_feed: ok"
exit $((errors != 0))
