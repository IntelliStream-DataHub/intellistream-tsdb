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
        "lost s0 --shrink" \
        "salvage" \
        "status"
    do
        # shellcheck disable=SC2086
        t tier $args >/dev/null 2>"$dir/noec.err"
        rc=$?
        [ "$rc" -ne 0 ] || fail "NO_EC: tier $args was accepted"
        cli_refusal_ok "$rc" "$dir/noec.err" "not built with the erasure codec" ||
            fail "NO_EC: tier $args: status $rc, stderr '$(head -c 200 "$dir/noec.err")'"
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

# 7. tier lost --shrink refuses when remaining stores < k+m (6 stores at 4+2 -> 5 < 6), and salvage/lost validate arguments.
refuse_tier "tier lost --shrink below 4+2" "fewer than" lost s0 --shrink
# tier salvage with all stores UP (no lost objects) succeeds as a clean no-op.
out_salv=$(t tier salvage) || fail "tier salvage no-op failed"
echo "$out_salv" | grep -q "retired 0 part(s)" || fail "tier salvage no-op output unexpected: $out_salv"
out_salv_p=$(t tier salvage --partition 100 --force) || fail "tier salvage --partition --force no-op failed"
echo "$out_salv_p" | grep -q "retired 0 part(s)" || fail "tier salvage --partition output unexpected: $out_salv_p"
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

# 7b. Refusals for tier lost --replace (relative path, unknown store, same device without flag).
refuse_tier "tier lost relative path" "absolute" lost s0 --replace rel/store --allow-shared-device
refuse_tier "tier lost unknown store" "not found" lost s99 --replace "$fs2/repl_unk" --allow-shared-device
refuse_tier "tier lost same device without flag" "same device" lost s0 --replace "$fs2/repl_nodev"

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

    # Live tier lost --replace beside running tsdbd: tsdbd's tier worker picks up TIERS, rebuilds, and marks UP.
    rm -rf "$fs1/s0"
    out_lost=$(t tier lost s0 --replace "$fs1/s0_new:hdd" --allow-shared-device) || fail "live tier lost --replace"
    echo "$out_lost" | grep -q "replaced by" || fail "live tier lost output missing 'replaced by': $out_lost"
    rebuilt_ok=0
    for _ in $(seq 300); do
        if [ ! -e "$fs1/s0_new/REBUILDING" ]; then
            rebuilt_ok=1
            break
        fi
        kill -0 "$srv_pid" 2>/dev/null || break
        sleep 0.1
    done
    [ "$rebuilt_ok" = 1 ] || fail "live tsdbd did not finish rebuilding s0_new"
    jrep=$(t tier status --json) || fail "tier status --json after live lost --replace"
    echo "$jrep" | python3 -I -m json.tool >/dev/null || fail "live lost status --json invalid JSON"
    echo "$jrep" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert len(d["stores"]) == 7 and d["stores_up"] == 7 and d["stores_lost"] == 0
assert d["stores"][0]["tiers_state"] == "replaced" and d["stores"][0]["state"] == "up"
assert d["stores"][0]["path"].endswith("/s0_new")
assert d["rebuild"]["active"] is False and d["rebuild"]["state"] == "idle"
' || fail "live lost status --json mismatch: $jrep"

    # tier salvage refuses while tsdbd holds locks.
    t tier salvage >/dev/null 2>"$dir/err"
    rc=$?
    [ "$rc" -ne 0 ] || fail "live tier salvage was accepted while tsdbd held locks"
    grep -qE "locked|busy|running writer" "$dir/err" || fail "live tier salvage stderr unexpected: $(cat "$dir/err")"

    kill -0 "$srv_pid" 2>/dev/null || fail "tsdbd died while tier commands ran"
else
    fail "tsdbd did not start: $(tail -10 "$dir/d.log")"
fi
stop_server

# 8b. Offline tier lost --replace reports replacing state and rebuild progress in tier status until next open.
rm -rf "$fs1/s1"
t tier lost s1 --replace "$fs1/s1_new:hdd" --allow-shared-device >/dev/null || fail "offline tier lost --replace"
[ -f "$fs1/s1_new/REBUILDING" ] || fail "offline tier lost --replace did not create REBUILDING marker"
out_rb=$(t tier status) || fail "tier status while offline replacing"
echo "$out_rb" | grep -q "rebuild: store 1 replacing" || fail "expected replacing rebuild line: $out_rb"
jrb=$(t tier status --json) || fail "tier status --json while offline replacing"
echo "$jrb" | python3 -I -m json.tool >/dev/null || fail "offline replacing status --json invalid JSON"
echo "$jrb" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["stores"][1]["state"] == "replacing" and d["stores_replacing"] == 1
assert d["rebuild"]["active"] is True and d["rebuild"]["store"] == 1
' || fail "offline replacing status --json mismatch: $jrb"
# Clear REBUILDING by briefly running tsdbd so step 9 sees 7 up stores.
me=$BASHPID
"$B/tsdbd" --dir "$db" --no-auth --port 0 --port-file "$dir/port2" --exit-with-parent=$me >"$dir/d2.log" 2>&1 &
srv_pid=$!
for _ in $(seq 300); do
    [ ! -e "$fs1/s1_new/REBUILDING" ] && [ -s "$dir/port2" ] && break
    kill -0 "$srv_pid" 2>/dev/null || break
    sleep 0.1
done
[ ! -e "$fs1/s1_new/REBUILDING" ] || fail "tsdbd reopen did not finish offline rebuild of s1_new"
stop_server

# 9. Missing, wrong-id, damaged stores, and negative margin in tier status.
cp "$fs2/s5/META" "$dir/s5.META.bak" || fail "backup s5 META"
cp "$fs2/s4/META" "$dir/s4.META.bak" || fail "backup s4 META"
mv "$fs2/s6" "$fs2/s6.gone" || fail "move s6 aside"
cp "$fs1/s0_new/META" "$fs2/s5/META" || fail "overwrite s5 META with s0_new META"
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

# 9b. tier salvage refuses while a store is DOWN (not LOST) without --force, and succeeds with --force.
t tier salvage >/dev/null 2>"$dir/err"
rc=$?
[ "$rc" -ne 0 ] || fail "tier salvage accepted with DOWN (not LOST) stores without --force"
grep -q "not LOST" "$dir/err" || fail "tier salvage DOWN refusal missing 'not LOST': $(cat "$dir/err")"
out_sf=$(t tier salvage --force) || fail "tier salvage --force with DOWN stores failed"
echo "$out_sf" | grep -q "retired 0 part(s)" || fail "tier salvage --force output unexpected: $out_sf"

mv "$fs2/s6.gone" "$fs2/s6" || fail "restore s6"
mv "$dir/s5.META.bak" "$fs2/s5/META" || fail "restore s5 META"
mv "$dir/s4.META.bak" "$fs2/s4/META" || fail "restore s4 META"

# 10. tier lost --shrink on 7 stores (4+2 -> 6 active stores): offline status shows shrinking, tier set refuses
#     redundancy > 6 non-shrinking stores (must not brick next open!), and tsdbd completes to removed.
rm -rf "$fs2/s6"
out_sh=$(t tier lost s6 --shrink) || fail "offline tier lost s6 --shrink"
echo "$out_sh" | grep -q "SHRINKING" || fail "tier lost --shrink output missing SHRINKING: $out_sh"
refuse_tier "tier set redundancy > non-shrinking stores" "requires 7 stores" set --redundancy 5+2
out_sh_st=$(t tier status) || fail "tier status while shrinking"
echo "$out_sh_st" | grep -q "shrink: store 6 shrinking" || fail "tier status missing shrink line: $out_sh_st"
jsh=$(t tier status --json) || fail "tier status --json while shrinking"
echo "$jsh" | python3 -I -m json.tool >/dev/null || fail "shrinking status --json invalid JSON"
echo "$jsh" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["stores_shrinking"] == 1 and d["stores_removed"] == 0
assert d["stores"][6]["tiers_state"] == "shrinking" and d["stores"][6]["state"] == "shrinking"
assert d["shrink"]["active"] is True and d["shrink"]["state"] == "shrinking" and d["shrink"]["store"] == 6
assert d["shrink"]["objects_left"] == 0
' || fail "shrinking status --json mismatch: $jsh"

me=$BASHPID
"$B/tsdbd" --dir "$db" --no-auth --port 0 --port-file "$dir/port3" --exit-with-parent=$me >"$dir/d3.log" 2>&1 &
srv_pid=$!
shrunk_ok=0
for _ in $(seq 300); do
    if [ -s "$dir/port3" ]; then
        jdone=$(t tier status --json 2>/dev/null || true)
        if echo "$jdone" | python3 -I -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d["stores_removed"]==1 and d["stores"][6]["state"]=="removed" else 1)' 2>/dev/null; then
            shrunk_ok=1
            break
        fi
    fi
    kill -0 "$srv_pid" 2>/dev/null || break
    sleep 0.1
done
[ "$shrunk_ok" = 1 ] || fail "tsdbd did not finish shrink to REMOVED: $(tail -10 "$dir/d3.log")"
jfin=$(t tier status --json) || fail "tier status --json after shrink"
echo "$jfin" | python3 -I -m json.tool >/dev/null || fail "post-shrink status --json invalid JSON"
echo "$jfin" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["stores_up"] == 6 and d["stores_shrinking"] == 0 and d["stores_removed"] == 1
assert d["margin"] == 2
assert d["stores"][6]["tiers_state"] == "removed" and d["stores"][6]["state"] == "removed"
assert d["shrink"]["active"] is False and d["shrink"]["state"] == "idle"
' || fail "post-shrink status --json mismatch: $jfin"
stop_server

[ "$errors" = 0 ] && echo "test_cli_tier: ok"
exit $((errors != 0))
