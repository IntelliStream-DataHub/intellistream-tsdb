#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `tsdb DIR tier add|set|lost|salvage|status` (docs/design-cluster-data.md §15.7, R15 D10):
# adds six stores across two scratch directories with --allow-shared-device, changes settings
# globally and per volume, inspects text and --json status (validated with python3 -I -m json.tool)
# including per-volume and per-store byte accounting, per-volume minimum margin, negative margin,
# down_missing/down_other_id/down_damaged states, refusals (relative path, inside root/volume,
# same store twice, another db's store, same device without flag, bad settings), single-copy
# margin="underneath", unbuilt lost/salvage exit 2 with "not built", and running beside live tsdbd.
#
#   B=build tests/test_cli_tier.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-tier.XXXXXX")
srv_pid=
stop_server() {
    if [ -n "${srv_pid:-}" ] && [ "$srv_pid" -gt 0 ] 2>/dev/null; then
        kill "$srv_pid" 2>/dev/null || true
        wait "$srv_pid" 2>/dev/null || true
    fi
    srv_pid=
}
cleanup() {
    stop_server
    rm -rf "$dir"
}
trap cleanup EXIT

errors=0
fail() { echo "test_cli_tier: FAIL: $*" >&2; errors=$((errors + 1)); }
t() { "$B/tsdb" "$dir/db" "$@"; }

. tests/cli_refuse.sh

db=$dir/db
vol=$dir/vol1
fs1=$dir/fs1
fs2=$dir/fs2
mkdir -p "$vol" "$fs1" "$fs2" || exit 1

t create a --type float64 >/dev/null || fail "create series a in root"
t volume add "$vol" >/dev/null || fail "volume add $vol"
t create b "$vol" >/dev/null || fail "create series b in vol1"

# Detect NO_EC=1 builds: every tier verb must refuse cleanly and the test skips.
t tier status >/dev/null 2>"$dir/probe.err"
probe_rc=$?
if [ "$probe_rc" -ne 0 ] && grep -q "not built with the erasure codec" "$dir/probe.err"; then
    for args in \
        "add $fs1/s0:hdd --allow-shared-device" \
        "set --after 7d" \
        "status"
    do
        # shellcheck disable=SC2086
        t tier $args >/dev/null 2>"$dir/noec.err"
        rc=$?
        [ "$rc" -ne 0 ] || fail "NO_EC: tier $args was accepted"
        cli_refusal_ok "$rc" "$dir/noec.err" "not built with the erasure codec" ||
            fail "NO_EC: tier $args: status $rc, stderr '$(head -c 200 "$dir/noec.err")'"
    done
    for args in "lost s0 --shrink" "salvage"; do
        # shellcheck disable=SC2086
        t tier $args >/dev/null 2>"$dir/noec.err"
        rc=$?
        [ "$rc" -eq 2 ] || fail "NO_EC: tier $args exited $rc (expected 2)"
        grep -q "not built" "$dir/noec.err" || fail "NO_EC: tier $args stderr missing 'not built': $(cat "$dir/noec.err")"
    done
    [ "$errors" = 0 ] && echo "test_cli_tier: skipped (NO_EC=1)"
    exit $((errors != 0))
fi

# 1. Status before any stores are added (text and JSON).
out=$(t tier status) || fail "tier status before add"
echo "$out" | grep -q "tier: not configured" || fail "expected 'tier: not configured', got: $out"
jout=$(t tier status --json) || fail "tier status --json before add"
echo "$jout" | python3 -I -m json.tool >/dev/null || fail "unconfigured tier status --json is invalid JSON: $jout"
echo "$jout" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["configured"] is False and d["active"] is False
assert d["stores"] == [] and len(d["volumes"]) == 2
' || fail "unconfigured tier status --json fields mismatch: $jout"

# tier set before any stores are added must be refused.
t tier set --after 7d >/dev/null 2>"$dir/err"
rc=$?
[ "$rc" -ne 0 ] || fail "tier set with no stores was accepted"
cli_refusal_ok "$rc" "$dir/err" "no tier stores" || fail "tier set with no stores: rc=$rc err=$(cat "$dir/err")"

# 2. Add six stores across two scratch directories with --allow-shared-device.
for i in 0 1 2; do
    out=$(t tier add "$fs1/s$i:hdd" --allow-shared-device) || fail "tier add $fs1/s$i:hdd"
    echo "$out" | grep -q "class hdd" || fail "tier add s$i output missing class hdd: $out"
    echo "$out" | grep -q "next opened" || fail "tier add s$i output missing next-open note: $out"
    [ -f "$fs1/s$i/META" ] || fail "$fs1/s$i/META missing"
    grep -q "^db=" "$fs1/s$i/META" || fail "$fs1/s$i/META missing db= binding"
done
for i in 3 4; do
    out=$(t tier add "$fs2/s$i:ssd" --allow-shared-device) || fail "tier add $fs2/s$i:ssd"
    echo "$out" | grep -q "class ssd" || fail "tier add s$i output missing class ssd: $out"
done
out=$(t tier add --allow-shared-device "$fs2/s5") || fail "tier add $fs2/s5 (default class, flag first)"
echo "$out" | grep -q "class unknown" || fail "tier add s5 output missing class unknown: $out"

# 3. tier status (text and JSON) with six stores (default 4+2, margin 2).
out=$(t tier status) || fail "tier status with 6 stores"
for tok in "6 stores: 6 up, 0 down, 0 lost" "redundancy: 4+2" "margin: 2 store(s)" \
           "settles_waiting: 0" "days_to_full: unknown" "damage: 0 cell(s)" \
           "rebuild: idle" "scrub: idle" "stores:" "volumes:"; do
    echo "$out" | grep -qF "$tok" || fail "tier status missing '$tok': $out"
done

jout=$(t tier status --json) || fail "tier status --json with 6 stores"
echo "$jout" | python3 -I -m json.tool >/dev/null || fail "tier status --json invalid JSON: $jout"
echo "$jout" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["configured"] is True and d["active"] is True
assert len(d["db_id"]) == 32
assert d["redundancy"] == {"k": 4, "m": 2}
assert d["margin"] == 2
assert d["stores_up"] == 6 and d["stores_down"] == 0 and d["stores_lost"] == 0
assert d["bytes_partial"] is False
assert d["settles_waiting"] == 0 and d["days_to_full"] is None and d["damaged_cells"] == 0
assert d["rebuild"]["state"] == "idle" and d["scrub"]["state"] == "idle"
assert len(d["stores"]) == 6
assert [s["class"] for s in d["stores"]] == ["hdd", "hdd", "hdd", "ssd", "ssd", "unknown"]
assert all(s["state"] == "up" and s["bytes"] == 0 and s["bytes_partial"] is False for s in d["stores"])
assert len(d["volumes"]) == 2
assert all(v["margin"] == 2 and len(v["bytes_by_store"]) == 6 for v in d["volumes"])
' || fail "tier status --json fields mismatch: $jout"

# 3b. Exercise non-zero chunk and .ec byte scans across volumes and stores.
mkdir -p "$fs1/s0/s0" "$fs1/s0/s1" "$fs2/s3/s1"
dd if=/dev/zero of="$fs1/s0/s0/o00000000000000000000000000000001.0.chunk" bs=1 count=1234 2>/dev/null
dd if=/dev/zero of="$fs1/s0/s1/o00000000000000000000000000000002.0.chunk" bs=1 count=2048 2>/dev/null
dd if=/dev/zero of="$fs2/s3/s1/o00000000000000000000000000000002.1.chunk" bs=1 count=4096 2>/dev/null
dd if=/dev/zero of="$vol/p000001.ec" bs=1 count=512 2>/dev/null
dd if=/dev/zero of="$vol/retired_p000002.ec" bs=1 count=999 2>/dev/null

jbytes=$(t tier status --json) || fail "tier status --json with planted bytes"
echo "$jbytes" | python3 -I -m json.tool >/dev/null || fail "planted bytes status --json invalid JSON"
echo "$jbytes" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["total_tier_bytes"] == 1234 + 2048 + 4096
assert d["stores"][0]["bytes"] == 1234 + 2048
assert d["stores"][3]["bytes"] == 4096
assert d["stores"][1]["bytes"] == 0
v0, v1 = d["volumes"][0], d["volumes"][1]
assert v0["tier_bytes"] == 1234 and v0["ec_parts"] == 0 and v0["ec_index_bytes"] == 0
assert v0["bytes_by_store"][0]["bytes"] == 1234 and v0["bytes_by_store"][3]["bytes"] == 0
assert v1["tier_bytes"] == 2048 + 4096 and v1["ec_parts"] == 1 and v1["ec_index_bytes"] == 512
assert v1["bytes_by_store"][0]["bytes"] == 2048 and v1["bytes_by_store"][3]["bytes"] == 4096
' || fail "planted bytes status --json mismatch: $jbytes"

# Remove planted dummy files before starting live tsdbd later.
rm -rf "$fs1/s0/s0" "$fs1/s0/s1" "$fs2/s3/s1" "$vol/p000001.ec" "$vol/retired_p000002.ec"

# 4. tier set globally and per volume (including lower-m volume margin check and pace/scrub flags).
out=$(t tier set --redundancy 3+2 --after 14d --high-pct 75 --young-high-pct 88 --rebuild-mb-s 150 --scrub-mb-s 40 --scrub-days 7) || fail "tier set global"
echo "$out" | grep -q "redundancy=3+2" || fail "tier set global output: $out"
echo "$out" | grep -q "next opened" || fail "tier set global output missing next-open note: $out"
out=$(t tier set --volume 1 --redundancy 5+1 --after 2d --high-pct 60) || fail "tier set volume 1"
echo "$out" | grep -q "volume 1" || fail "tier set volume 1 output: $out"

jout=$(t tier status --json) || fail "tier status --json after set"
echo "$jout" | python3 -I -m json.tool >/dev/null || fail "tier status --json after set invalid JSON"
echo "$jout" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["redundancy"] == {"k": 3, "m": 2}
assert d["margin"] == 1 # min across global m=2 and volume 1 m=1
assert d["settings"]["after_s"] == 14 * 86400
assert d["settings"]["high_pct"] == 75
assert d["settings"]["young_high_pct"] == 88
assert d["settings"]["rebuild_mb_s"] == 150
assert d["settings"]["scrub_mb_s"] == 40
assert d["settings"]["scrub_days"] == 7
assert d["volumes"][0]["redundancy"] == {"k": 3, "m": 2} and d["volumes"][0]["margin"] == 2
assert d["volumes"][0]["after_s"] == 14 * 86400 and d["volumes"][0]["high_pct"] == 75
assert d["volumes"][1]["redundancy"] == {"k": 5, "m": 1} and d["volumes"][1]["margin"] == 1
assert d["volumes"][1]["after_s"] == 2 * 86400 and d["volumes"][1]["high_pct"] == 60
' || fail "tier status --json settings/margin mismatch: $jout"

# Restore global and volume 1 to 4+2.
t tier set --redundancy 4+2 --after 7d --high-pct 80 >/dev/null || fail "tier set restore 4+2"
t tier set --volume 1 --redundancy 4+2 >/dev/null || fail "tier set volume 1 restore 4+2"

# 5. Refusals for tier add and tier set (each verified for its specific reason).
refuse_tier() {
    local what=$1 word=$2
    shift 2
    t tier "$@" >/dev/null 2>"$dir/err"
    local rc=$?
    [ "$rc" -ne 0 ] || fail "$what was accepted"
    cli_refusal_ok "$rc" "$dir/err" "$word" ||
        fail "$what: rc=$rc expected '$word' in stderr='$(head -c 200 "$dir/err")'"
}

refuse_tier "relative store path" "absolute" add rel/store --allow-shared-device
refuse_tier "store inside root" "overlaps" add "$db/substore" --allow-shared-device
refuse_tier "store inside volume" "overlaps" add "$vol/substore" --allow-shared-device
refuse_tier "same store twice" "overlaps" add "$fs1/s0:hdd" --allow-shared-device
refuse_tier "same device without --allow-shared-device" "same device" add "$fs2/s_noshare:hdd"
refuse_tier "unknown store class" "unknown class" add "$fs2/s_badcls:tape" --allow-shared-device
refuse_tier "empty store class" "unknown class" add "$fs2/s_emptycls:" --allow-shared-device
refuse_tier "unknown tier verb" "unknown command" bogus

# Another database's store must be refused.
other_db=$dir/other_db
other_st=$dir/other_store
"$B/tsdb" "$other_db" create x >/dev/null || fail "create other_db"
"$B/tsdb" "$other_db" tier add "$other_st:hdd" --allow-shared-device >/dev/null || fail "other_db tier add"
refuse_tier "another db's store" "belongs to database" add "$other_st:hdd" --allow-shared-device

# Refusals for tier set.
refuse_tier "tier set with no flags" "at least one setting" set
refuse_tier "tier set redundancy > stores" "requires 7 stores" set --redundancy 5+2
refuse_tier "tier set 1+0 on non-single-copy" "single-copy" set --redundancy 1+0
refuse_tier "tier set bad redundancy" "invalid --redundancy" set --redundancy nope
refuse_tier "tier set bad duration" "invalid --after" set --after 7x
refuse_tier "tier set negative duration" "invalid --after" set --after -1d
refuse_tier "tier set high-pct 0" "invalid --high-pct" set --high-pct 0
refuse_tier "tier set high-pct 101" "invalid --high-pct" set --high-pct 101
refuse_tier "tier set young-high-pct 0" "invalid --young-high-pct" set --young-high-pct 0
refuse_tier "tier set rebuild-mb-s 0" "invalid --rebuild-mb-s" set --rebuild-mb-s 0
refuse_tier "tier set scrub-mb-s 0" "invalid --scrub-mb-s" set --scrub-mb-s 0
refuse_tier "tier set scrub-days > max" "invalid --scrub-days" set --scrub-days 4000
refuse_tier "tier set nonexistent volume" "does not exist" set --volume 99 --high-pct 70

# 6. Single-copy store reports margin "underneath" and allows 1+0.
sc_db=$dir/sc_db
"$B/tsdb" "$sc_db" create s >/dev/null || fail "create sc_db"
"$B/tsdb" "$sc_db" tier add "$dir/sc0:hdd" --single-copy --allow-shared-device >/dev/null || fail "sc_db tier add"
"$B/tsdb" "$sc_db" tier set --redundancy 1+0 >/dev/null || fail "sc_db tier set 1+0"
sc_out=$("$B/tsdb" "$sc_db" tier status) || fail "sc_db tier status"
echo "$sc_out" | grep -q "margin: underneath" || fail "sc_db status missing 'margin: underneath': $sc_out"
sc_json=$("$B/tsdb" "$sc_db" tier status --json) || fail "sc_db tier status --json"
echo "$sc_json" | python3 -I -m json.tool >/dev/null || fail "sc_db status --json invalid JSON"
echo "$sc_json" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["redundancy"] == {"k": 1, "m": 0}
assert d["margin"] == "underneath"
assert d["volumes"][0]["margin"] == "underneath"
assert d["stores"][0]["single_copy"] is True
' || fail "sc_db status --json fields mismatch: $sc_json"

# 7. Unbuilt verbs (lost, salvage) exit 2 with "not built", and validate arguments.
for args in \
    "lost s0 --replace $fs2/repl" \
    "lost s0 --shrink" \
    "salvage" \
    "salvage --partition 100 --force"
do
    # shellcheck disable=SC2086
    t tier $args >/dev/null 2>"$dir/err"
    rc=$?
    [ "$rc" -eq 2 ] || fail "tier $args exited $rc (expected 2)"
    grep -q "not built" "$dir/err" || fail "tier $args stderr missing 'not built': $(cat "$dir/err")"
done
for args in \
    "lost s0" \
    "lost s0 --replace $fs2/repl --shrink" \
    "salvage --partition bogus" \
    "salvage --unknown"
do
    # shellcheck disable=SC2086
    t tier $args >/dev/null 2>"$dir/err"
    rc=$?
    [ "$rc" -eq 2 ] || fail "invalid tier $args exited $rc (expected 2)"
done

# 8. Running beside a live tsdbd (D10: appends to TIERS under VOLUMES OFD lock).
me=$BASHPID
"$B/tsdbd" --dir "$db" --no-auth --port 0 --port-file "$dir/port" --exit-with-parent=$me >"$dir/d.log" 2>&1 &
srv_pid=$!
for _ in $(seq 100); do
    [ -s "$dir/port" ] && break
    kill -0 "$srv_pid" 2>/dev/null || break
    sleep 0.1
done
if [ -s "$dir/port" ]; then
    read -r _ fpid _ < "$dir/port"
    [ "$fpid" = "$srv_pid" ] || fail "port file names pid $fpid, not started tsdbd ($srv_pid)"
    kill -0 "$srv_pid" 2>/dev/null || fail "tsdbd exited early: $(tail -5 "$dir/d.log")"

    t tier status >/dev/null || fail "tier status beside live tsdbd"
    t tier status --json | python3 -I -m json.tool >/dev/null || fail "tier status --json beside live tsdbd"
    t tier add "$fs2/s6:hdd" --allow-shared-device >/dev/null || fail "tier add beside live tsdbd"
    t tier set --after 3d --high-pct 85 >/dev/null || fail "tier set beside live tsdbd"

    jlive=$(t tier status --json) || fail "tier status --json after live add+set"
    echo "$jlive" | python3 -I -m json.tool >/dev/null || fail "live tier status --json invalid JSON"
    echo "$jlive" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert len(d["stores"]) == 7 and d["stores_up"] == 7
assert d["settings"]["after_s"] == 3 * 86400
assert d["settings"]["high_pct"] == 85
' || fail "live tier status --json did not reflect live add+set: $jlive"

    # Unbuilt verbs beside live tsdbd still exit 2 with "not built".
    t tier lost s0 --shrink >/dev/null 2>"$dir/err"
    rc=$?
    [ "$rc" -eq 2 ] && grep -q "not built" "$dir/err" || fail "live tier lost: rc=$rc err=$(cat "$dir/err")"
    t tier salvage >/dev/null 2>"$dir/err"
    rc=$?
    [ "$rc" -eq 2 ] && grep -q "not built" "$dir/err" || fail "live tier salvage: rc=$rc err=$(cat "$dir/err")"

    kill -0 "$srv_pid" 2>/dev/null || fail "tsdbd died while tier commands ran"
else
    fail "tsdbd did not start: $(tail -10 "$dir/d.log")"
fi
stop_server

# 9. Missing, wrong-id, damaged stores, and negative margin in tier status.
cp "$fs2/s5/META" "$dir/s5.META.bak" || fail "backup s5 META"
cp "$fs2/s4/META" "$dir/s4.META.bak" || fail "backup s4 META"
mv "$fs2/s6" "$fs2/s6.gone" || fail "move s6 aside"
cp "$fs1/s0/META" "$fs2/s5/META" || fail "overwrite s5 META with s0 META"
printf 'corrupt\n' >"$fs2/s4/META" || fail "corrupt s4 META"

out_neg=$(t tier status) || fail "tier status with 3 unavailable stores"
echo "$out_neg" | grep -q "below redundancy, 1 store(s) short" || fail "expected negative margin note: $out_neg"
jdown=$(t tier status --json) || fail "tier status --json with 3 unavailable stores"
echo "$jdown" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["stores_up"] == 4 and d["stores_down"] == 3 and d["margin"] == -1
assert d["stores"][4]["state"] == "down_damaged"
assert d["stores"][5]["state"] == "down_other_id"
assert d["stores"][6]["state"] == "down_missing"
' || fail "unavailable store status mismatch: $jdown"

mv "$fs2/s6.gone" "$fs2/s6" || fail "restore s6"
mv "$dir/s5.META.bak" "$fs2/s5/META" || fail "restore s5 META"
mv "$dir/s4.META.bak" "$fs2/s4/META" || fail "restore s4 META"

[ "$errors" = 0 ] && echo "test_cli_tier: ok"
exit $((errors != 0))
