#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# End-to-end test of `tsdb cluster` verb table, help VERB, status views (summary, volumes, volume V, disks),
# `--json` schemas, leader-follow, exit codes (0/1/2), and R4/R8/R9 verbs on a 3-node cluster with --disk.
#
#   B=build tests/test_cluster_cli.sh [scratch dir]
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
T=$B/tsdb
D_BIN=$B/tsdbd

scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-cluster-cli.XXXXXX") || exit 1
PIDS=()

stop_pid() {
    local pid="$1"
    case $pid in ''|*[!0-9]*|0|1) return 0 ;; esac
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
}

cleanup() {
    for pid in "${PIDS[@]}"; do
        stop_pid "$pid"
    done
    rm -rf "$scratch"
}
trap cleanup EXIT

fail() {
    echo "test_cluster_cli: FAIL: $*" >&2
    for f in "$scratch"/*.log; do
        [ -f "$f" ] || continue
        echo "--- $f ---" >&2
        tail -n 20 "$f" >&2
    done
    exit 1
}

wait_port_file() {
    local pf="$1" child="$2"
    for _ in $(seq 150); do
        if [ -s "$pf" ]; then
            read -r _cport fpid _mport _pport < "$pf"
            if [ "$fpid" = "$child" ] && [ -n "${_pport:-}" ]; then
                return 0
            fi
        fi
        kill -0 "$child" 2>/dev/null || fail "tsdbd pid $child exited before writing $pf"
        sleep 0.1
    done
    fail "timed out waiting for port file $pf"
}

peer_of() {
    local pf="$1"
    cut -d' ' -f4 "$pf"
}

# ---- 1. Verb help & usage exit codes (offline) ----

for hargs in "" "help" "help status" "help status volumes" "help status volume" "help status disks" \
             "help init" "help node" "help node add" "help node hold" "help node out" "help node drain" \
             "help node replace" "help node destroyed" "help volume" "help volume add" "help volume move" \
             "help volume transfer" "help volume force-leader" "help volume gc resume" "help replica reset" \
             "help settings" "help settings show" "help settings set" "help balance" \
             "help secret" "help user"; do
    # shellcheck disable=SC2086
    out=$("$T" cluster $hargs 2>"$scratch/err"); rc=$?
    [ "$rc" -eq 0 ] || fail "tsdb cluster $hargs: exit $rc, want 0 (err: $(cat "$scratch/err"))"
    [ -z "$(cat "$scratch/err")" ] || fail "tsdb cluster $hargs: unexpected stderr: $(cat "$scratch/err")"
    [ -n "$out" ] || fail "tsdb cluster $hargs: empty stdout"
done

# Verify `help status` documents all four views and their top-level JSON keys.
hstat=$("$T" cluster help status)
for want in "status volumes" "status volume V" "status disks" \
            "view, node, cluster, quorum, settings, guard, health, members" \
            "volumes_count, users_count, transfers, moves" \
            "view, cluster_uuid, leader, term, applied, volumes" \
            "view, cluster_uuid, leader, term, applied, volume" \
            "view, reported, chunk_fill_mark_pct, classes, disks, warnings"; do
    case $hstat in *"$want"*) ;; *) fail "help status missing '$want'" ;; esac
done

# Wrong verb / missing args must exit 2 with empty stdout.
for bad in "bogus" "help bogus" "help status volumes extra" "status" "status --json" "status volumes" \
           "status volume" "status volume 1" "status disks" "node nope" "volume nope" "replica nope" \
           "settings" "settings nope" "settings show" "settings set" "settings set 127.0.0.1:1" "balance"; do
    # shellcheck disable=SC2086
    out=$("$T" cluster $bad 2>"$scratch/err"); rc=$?
    [ "$rc" -eq 2 ] || fail "tsdb cluster $bad: exit $rc, want 2"
    [ -z "$out" ] || fail "tsdb cluster $bad: expected empty stdout, got: $out"
done
"$T" cluster help status volumes extra 2>"$scratch/err" >/dev/null || true
grep -q "unknown verb 'status volumes extra'" "$scratch/err" \
    || fail "help status volumes extra did not report full unmatched verb: $(cat "$scratch/err")"

# ---- 2. Secret creation + witness node ("no disks reported" & uninitialised views) ----

sec="$scratch/cluster.secret"
"$T" cluster secret "$sec" >"$scratch/out" 2>&1 || fail "cluster secret failed"
grep -q "wrote a new cluster secret" "$scratch/out" || fail "unexpected secret output: $(cat "$scratch/out")"

me=$BASHPID
mkdir -p "$scratch/w/state"
"$D_BIN" --cluster --witness --no-auth --io epoll --state-dir "$scratch/w/state" \
    --cluster-secret-file "$sec" --port 0 --peer-port 0 --port-file "$scratch/w/port" \
    --exit-with-parent="$me" >"$scratch/witness.log" 2>&1 &
wpid=$!
PIDS+=("$wpid")
wait_port_file "$scratch/w/port" "$wpid"
wpeer="127.0.0.1:$(peer_of "$scratch/w/port")"

out=$("$T" cluster status disks "$wpeer" --secret-file "$sec") || fail "witness status disks failed"
[ "$out" = "no disks reported" ] || fail "witness status disks: got '$out', want 'no disks reported'"

"$T" cluster status disks --json "$wpeer" --secret-file "$sec" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["view"] == "disks", d
assert d["reported"] is False, d
assert d["classes"] == [], d
assert d["disks"] == [], d
assert isinstance(d["chunk_fill_mark_pct"], int), d
assert isinstance(d["warnings"], list), d
' || fail "witness status disks --json validation failed"

"$T" cluster status --json "$wpeer" --secret-file "$sec" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
assert d["view"] == "summary", d
assert d["cluster"] is None, d
assert d["settings"] is None, d
assert d["health"]["status"] == "uninitialised", d
assert d["members"] == [], d
assert d["volumes_count"] == 0, d
' || fail "witness status --json validation failed"

out=$("$T" cluster status volumes "$wpeer" --secret-file "$sec") || fail "witness status volumes failed"
[ "$out" = "no volumes" ] || fail "witness status volumes: got '$out', want 'no volumes'"

"$T" cluster status volume 1 "$wpeer" --secret-file "$sec" >/dev/null 2>&1
rc=$?
[ "$rc" -eq 1 ] || fail "witness status volume 1: exit $rc, want 1"

stop_pid "$wpid"
PIDS=()

# ---- 3. 3-node cluster with --disk PATH:nvme and --disk PATH:hdd ----

mkdir -p "$scratch/nosys"
declare -a NPEER
for i in 1 2 3; do
    mkdir -p "$scratch/n$i/nvme" "$scratch/n$i/hdd" "$scratch/n$i/state"
    "$T" disk init "$scratch/n$i/nvme:nvme" >/dev/null 2>&1 || fail "disk init n$i nvme failed"
    "$T" disk init "$scratch/n$i/hdd:hdd" >/dev/null 2>&1 || fail "disk init n$i hdd failed"
    TSDB_DISK_SYSROOT="$scratch/nosys" "$D_BIN" --cluster --no-auth --io epoll --state-dir "$scratch/n$i/state" \
        --disk "$scratch/n$i/nvme:nvme" --disk "$scratch/n$i/hdd:hdd" --allow-shared-device \
        --rack "rack$i" --cluster-secret-file "$sec" --port 0 --peer-port 0 \
        --port-file "$scratch/n$i/port" --exit-with-parent="$me" >"$scratch/n$i.log" 2>&1 &
    npid=$!
    PIDS+=("$npid")
    wait_port_file "$scratch/n$i/port" "$npid"
    NPEER[$i]="127.0.0.1:$(peer_of "$scratch/n$i/port")"
done

p1="${NPEER[1]}"
p2="${NPEER[2]}"
p3="${NPEER[3]}"

out=$("$T" cluster init "$p1" --secret-file "$sec" --node-timeout-ms 2000 --heartbeat-ms 200 \
    --rebuild-mb-s 150 --rebuild-max 4 --transfer-wait-ms 3000 --leader-balance-s 120) || fail "cluster init failed"
case $out in *"initialised: node 1 at $p1"*) ;; *) fail "unexpected init output: $out" ;; esac

out=$("$T" cluster node add "$p1" "$p2" --secret-file "$sec") || fail "node add 2 failed"
[ "$out" = "added $p2" ] || fail "unexpected node add 2 output: $out"

# Wait until follower p2 learns the leader so NOTLEADER carries leader's peer address.
seen_l=0
for _ in $(seq 100); do
    if "$T" cluster status --json "$p2" --secret-file "$sec" 2>/dev/null | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
sys.exit(0 if d["node"]["leader"] == 1 and len(d["members"]) >= 2 else 1)
' 2>/dev/null; then
        seen_l=1
        break
    fi
    sleep 0.1
done
[ "$seen_l" -eq 1 ] || fail "follower p2 did not learn leader 1"

# Add node 3 via follower p2 to exercise one-hop NOTLEADER follow.
out=$("$T" cluster node add "$p2" "$p3" --secret-file "$sec") || fail "node add 3 via follower failed"
[ "$out" = "added $p3" ] || fail "unexpected node add 3 output: $out"

# Wait for 3 voters, all 6 disks registered (nvme+hdd), and all 3 chunk servers up before creating volumes.
ready=0
for _ in $(seq 300); do
    st=$("$T" cluster status "$p1" --secret-file "$sec" 2>/dev/null || true)
    if printf '%s\n' "$st" | grep -q '^group voters 1,2,3 learners -$' && \
       [ "$(printf '%s\n' "$st" | grep -c ' cap both disks 2$')" -eq 3 ] && \
       grep -q "chunk server up as node 1" "$scratch/n1.log" && \
       grep -q "chunk server up as node 2" "$scratch/n2.log" && \
       grep -q "chunk server up as node 3" "$scratch/n3.log" && \
       "$T" cluster status disks --json "$p1" --secret-file "$sec" 2>/dev/null | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
by_cls = {c["class"]: c for c in d["classes"]}
sys.exit(0 if by_cls.get("nvme", {}).get("disks_up") == 3 and by_cls.get("hdd", {}).get("disks_up") == 3 else 1)
' 2>/dev/null; then
        ready=1
        break
    fi
    sleep 0.1
done
[ "$ready" -eq 1 ] || fail "cluster did not reach 3 voters with 2 disks and chunk servers up on each"

# Create a replicated volume (via follower p2).
out=$("$T" cluster volume add "$p2" repv --replicas 3 --secret-file "$sec") || fail "volume add repv failed"
[ "$out" = "added volume repv: number 1" ] || fail "unexpected repv add output: $out"

# Create an EC volume once the leader has heard HBS_STORES from all 3 chunk servers. If refused with FEW
# (not enough chunk stores heard yet) and ecv does not exist in metadata, wait for the next heartbeat.
ec_ok=0
for _ in $(seq 100); do
    if out=$("$T" cluster volume add "$p1" ecv --redundancy ec=2+1 --secret-file "$sec" 2>"$scratch/ecerr"); then
        [ "$out" = "added volume ecv: number 2" ] || fail "unexpected ecv add output: $out"
        ec_ok=1
        break
    fi
    if ! grep -qi "fewer\|qualify\|enough" "$scratch/ecerr"; then
        fail "volume add ecv failed unexpectedly: $(cat "$scratch/ecerr")"
    fi
    if "$T" cluster status volumes --json "$p1" --secret-file "$sec" 2>/dev/null | python3 -I -c '
import json, sys
sys.exit(0 if any(v["name"] == "ecv" for v in json.load(sys.stdin)["volumes"]) else 1)
' 2>/dev/null; then
        fail "volume add ecv failed after ecv already appeared in metadata"
    fi
    sleep 0.1
done
[ "$ec_ok" -eq 1 ] || fail "volume add ecv did not succeed after chunk servers started: $(cat "$scratch/ecerr")"

# Wait for both volumes to reach full ISR (3 for repv, 2 for ecv).
isr_ok=0
for _ in $(seq 300); do
    j=$("$T" cluster status volumes --json "$p1" --secret-file "$sec" 2>/dev/null || true)
    if printf '%s\n' "$j" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
vs = {v["name"]: v for v in d["volumes"]}
ok = (len(vs.get("repv", {}).get("isr", [])) == 3 and len(vs.get("ecv", {}).get("isr", [])) == 2)
sys.exit(0 if ok else 1)
' 2>/dev/null; then
        isr_ok=1
        break
    fi
    sleep 0.1
done
[ "$isr_ok" -eq 1 ] || fail "volumes did not reach full ISR"

# ---- 4. Verify all four status views (text & --json in both positions) ----

st_txt=$("$T" cluster status "$p1" --secret-file "$sec") || fail "status text failed"
for pat in "^node 1 mode core role leader " "^cluster [0-9a-f]{32} " \
           "^replicated .* rebuild_mb_s 150 rebuild_max 4 transfer_wait_ms 3000 leader_balance_s 120$" \
           "^failure " "^scrub " "^group voters 1,2,3 learners -$" \
           "^quorum yes voters_up 3/3 needed 2$" "^member 1 up " "^volume 1 name repv " \
           "^volume 2 name ecv .* redundancy ec=2\+1 " "^health summary warn volumes_ok 2/2 degraded 0 unavailable 0 lost 0 warnings [1-9][0-9]*$" \
           "^volumes 2 users 0$" "^transfers started " "^moves started "; do
    printf '%s\n' "$st_txt" | grep -qE "$pat" || fail "status text missing pattern '$pat':\n$st_txt"
done

# Verify text `health summary` status & warning count match `status --json` health.status & len(health.warnings).
st_hline=$(printf '%s\n' "$st_txt" | grep -E '^health summary ')
st_json=$("$T" cluster status --json "$p1" --secret-file "$sec") || fail "status --json failed"
printf '%s\n' "$st_json" | python3 -I -c '
import json, re, sys
d = json.load(sys.stdin)
m = re.match(r"^health summary (\S+) volumes_ok (\d+)/(\d+) degraded (\d+) unavailable (\d+) lost (\d+) warnings (\d+)$",
             sys.argv[1])
assert m, f"bad health line: {sys.argv[1]}"
h = d["health"]
assert h["status"] == m.group(1), (h["status"], m.group(1))
assert h["volumes_ok"] == int(m.group(2)) and h["volumes_total"] == int(m.group(3))
assert h["volumes_degraded"] == int(m.group(4)) and h["volumes_unavailable"] == int(m.group(5))
assert h["volumes_lost"] == int(m.group(6))
assert len(h["warnings"]) == int(m.group(7)), (h["warnings"], m.group(7))
' "$st_hline" || fail "text health summary and JSON health diverged"

for cmd in \
    "status --json $p1 --secret-file $sec" \
    "status $p1 --json --secret-file $sec"; do
    # shellcheck disable=SC2086
    "$T" cluster $cmd | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
for k in ("view", "node", "cluster", "quorum", "settings", "guard", "health", "members",
          "volumes_count", "users_count", "transfers", "moves"):
    assert k in d, f"missing top key {k}"
assert d["view"] == "summary"
for k in ("id", "mode", "role", "stale", "term", "leader", "commit", "applied", "users_caught_up"):
    assert k in d["node"], f"missing node.{k}"
assert isinstance(d["node"]["id"], int) and not isinstance(d["node"]["id"], bool)
assert isinstance(d["node"]["term"], int) and d["node"]["term"] >= 1
assert d["cluster"]["initialised"] is True and len(d["cluster"]["uuid"]) == 32
q = d["quorum"]
for k in ("has_quorum", "raft_leader", "term", "voters", "learners", "voters_total", "voters_up", "quorum_needed"):
    assert k in q, f"missing quorum.{k}"
assert q["has_quorum"] is True and q["voters_total"] == 3 and q["voters_up"] == 3 and q["quorum_needed"] == 2
assert d["settings"]["rebuild_mb_s"] == 150 and d["settings"]["rebuild_max"] == 4
assert d["settings"]["transfer_wait_ms"] == 3000 and d["settings"]["leader_balance_s"] == 120
h = d["health"]
for k in ("status", "members_total", "members_up", "members_down", "members_draining", "members_out",
          "members_destroyed", "volumes_total", "volumes_ok", "volumes_degraded",
          "volumes_unavailable", "volumes_lost", "warnings"):
    assert k in h, f"missing health.{k}"
assert h["members_total"] == 3 and h["members_up"] == 3
assert h["volumes_total"] == 2 and h["volumes_ok"] == 2
assert len(d["members"]) == 3
for m in d["members"]:
    for k in ("id", "state", "incarnation", "addr", "peer", "rack", "power_feed", "heard_ms",
              "voter_ok", "volumes_ok", "core_dropped", "read_barrier", "role", "capability",
              "disks", "down_reason", "down_age_s", "out_in_s", "hold_left_s", "replaced_by"):
        assert k in m, f"missing member.{k}"
    assert m["disks"] == 2
assert d["volumes_count"] == 2 and d["users_count"] == 0
assert isinstance(d["transfers"]["started"], int)
assert isinstance(d["moves"]["started"], int)
' || fail "tsdb cluster $cmd JSON check failed"
done

sv_txt=$("$T" cluster status volumes "$p1" --secret-file "$sec") || fail "status volumes text failed"
printf '%s\n' "$sv_txt" | grep -qE '^vol[[:space:]]+name[[:space:]]+red[[:space:]]+leader[[:space:]]+epoch[[:space:]]+isr[[:space:]]+margins[[:space:]]+health[[:space:]]+rebuild[[:space:]]+settle$' \
    || fail "status volumes header mismatch:\n$sv_txt"
printf '%s\n' "$sv_txt" | grep -qE '^1[[:space:]]+repv[[:space:]]+replicas=3[[:space:]].*2/-[[:space:]]+ok' \
    || fail "status volumes missing repv row:\n$sv_txt"
printf '%s\n' "$sv_txt" | grep -qE '^2[[:space:]]+ecv[[:space:]]+ec=2\+1[[:space:]].*1/1[[:space:]]+ok' \
    || fail "status volumes missing ecv row:\n$sv_txt"

for cmd in \
    "status volumes --json $p1 --secret-file $sec" \
    "status --json volumes $p1 --secret-file $sec" \
    "status volumes $p1 --json --secret-file $sec"; do
    # shellcheck disable=SC2086
    "$T" cluster $cmd | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
for k in ("view", "cluster_uuid", "leader", "term", "applied", "volumes"):
    assert k in d, f"missing top key {k}"
assert d["view"] == "volumes" and len(d["volumes"]) == 2
vol_keys = ("number", "name", "uuid", "kind", "leader", "preferred", "epoch", "min_isr", "isr",
            "isr_version", "set_version", "redundancy", "ec_k", "ec_m", "gc_suspended",
            "mixed_class", "rack_survivable", "young_margin", "settled_margin", "settled_state",
            "health", "warnings", "replicas", "stripe", "reports", "rebuild", "settle_backlog",
            "young_scrub_age_s")
by_name = {v["name"]: v for v in d["volumes"]}
for v in d["volumes"]:
    for k in vol_keys:
        assert k in v, f"missing volume.{k}"
    assert isinstance(v["epoch"], int) and v["epoch"] >= 1
    assert isinstance(v["settle_backlog"]["keys_waiting"], int)
    assert isinstance(v["settle_backlog"]["oldest_wait_s"], int)
    assert isinstance(v["rebuild"]["active"], bool)
assert by_name["repv"]["young_margin"] == 2 and by_name["repv"]["settled_margin"] is None
assert by_name["repv"]["health"] == "ok" and by_name["repv"]["settled_state"] == "none"
assert by_name["ecv"]["young_margin"] == 1 and by_name["ecv"]["settled_margin"] == 1
assert by_name["ecv"]["health"] == "ok" and by_name["ecv"]["settled_state"] == "ok"
assert len(by_name["ecv"]["stripe"]) == 3
' || fail "tsdb cluster $cmd JSON check failed"
done

sv1_txt=$("$T" cluster status volume 1 "$p1" --secret-file "$sec") || fail "status volume 1 text failed"
sv1_name=$("$T" cluster status volume repv "$p1" --secret-file "$sec") || fail "status volume repv text failed"
printf '%s\n' "$sv1_txt" | grep -q '^volume 1 name repv ' || fail "status volume 1 missing header:\n$sv1_txt"
printf '%s\n' "$sv1_txt" | grep -q '^margins young 2 settled - settled_state none ' || fail "status volume 1 missing margins:\n$sv1_txt"
printf '%s\n' "$sv1_txt" | grep -q '^settle keys_waiting 0 oldest_wait_s 0 ' || fail "status volume 1 missing settle:\n$sv1_txt"
printf '%s\n' "$sv1_name" | grep -q '^volume 1 name repv ' || fail "status volume repv by name failed"

for cmd in \
    "status volume 2 --json $p1 --secret-file $sec" \
    "status --json volume ecv $p1 --secret-file $sec" \
    "status volume --json ecv $p1 --secret-file $sec" \
    "status volume ecv $p1 --json --secret-file $sec"; do
    # shellcheck disable=SC2086
    "$T" cluster $cmd | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
for k in ("view", "cluster_uuid", "leader", "term", "applied", "volume"):
    assert k in d, f"missing top key {k}"
assert d["view"] == "volume"
v = d["volume"]
assert v["number"] == 2 and v["name"] == "ecv" and v["redundancy"] == "ec=2+1"
assert v["young_margin"] == 1 and v["settled_margin"] == 1 and v["health"] == "ok"
' || fail "tsdb cluster $cmd JSON check failed"
done

"$T" cluster status volume no_such_vol "$p1" --secret-file "$sec" >/dev/null 2>&1
rc=$?
[ "$rc" -eq 1 ] || fail "status volume no_such_vol: exit $rc, want 1"

sd_txt=$("$T" cluster status disks "$p1" --secret-file "$sec") || fail "status disks text failed"
printf '%s\n' "$sd_txt" | grep -qE '^class[[:space:]]+disks[[:space:]]+size[[:space:]]+young[[:space:]]+retired[[:space:]]+index[[:space:]]+chunks[[:space:]]+free[[:space:]]+marks[[:space:]]+days to mark$' \
    || fail "status disks header mismatch:\n$sd_txt"
printf '%s\n' "$sd_txt" | grep -qE '^nvme[[:space:]]+3[[:space:]].*75/80/90%' \
    || fail "status disks missing nvme row:\n$sd_txt"
printf '%s\n' "$sd_txt" | grep -qE '^hdd[[:space:]]+3[[:space:]].*[0-9]+%' \
    || fail "status disks missing hdd row:\n$sd_txt"

for cmd in \
    "status disks --json $p1 --secret-file $sec" \
    "status --json disks $p1 --secret-file $sec" \
    "status disks $p1 --json --secret-file $sec"; do
    # shellcheck disable=SC2086
    "$T" cluster $cmd | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
for k in ("view", "reported", "chunk_fill_mark_pct", "classes", "disks", "warnings"):
    assert k in d, f"missing top key {k}"
assert d["view"] == "disks" and d["reported"] is True
assert isinstance(d["chunk_fill_mark_pct"], int) and 0 < d["chunk_fill_mark_pct"] <= 95
assert len(d["disks"]) == 6
cls_keys = ("class", "disks", "disks_up", "disks_down", "disks_out", "disks_destroyed",
            "size_bytes", "young_bytes", "retired_bytes", "index_bytes", "chunk_bytes",
            "free_bytes", "marks", "young_warn_pct", "young_move_pct", "young_high_pct",
            "chunk_mark_pct", "days_to_mark")
by_cls = {c["class"]: c for c in d["classes"]}
assert set(by_cls.keys()) == {"nvme", "hdd"}, by_cls.keys()
for c in d["classes"]:
    for k in cls_keys:
        assert k in c, f"missing class.{k}"
    assert c["disks"] == 3 and c["disks_up"] == 3 and c["size_bytes"] > 0
assert by_cls["nvme"]["marks"] == "75/80/90%"
assert by_cls["nvme"]["young_warn_pct"] == 75 and by_cls["nvme"]["young_move_pct"] == 80 and by_cls["nvme"]["young_high_pct"] == 90
assert by_cls["hdd"]["young_warn_pct"] is None and isinstance(by_cls["hdd"]["chunk_mark_pct"], int)
disk_keys = ("node", "store_id", "class", "uses", "state", "size_bytes", "sync_us", "young_bytes", "chunk_bytes")
for dk in d["disks"]:
    for k in disk_keys:
        assert k in dk, f"missing disk.{k}"
    assert isinstance(dk["node"], int) and len(dk["store_id"]) == 32
    assert isinstance(dk["size_bytes"], int) and dk["size_bytes"] > 0
' || fail "tsdb cluster $cmd JSON check failed"
done

# ---- 5. R4 / R8 / R9 verbs via follower p2 (syntax, leader-follow, exit codes, output) ----

out=$("$T" cluster node hold "$p2" 3 --for 60s --secret-file "$sec") || fail "node hold failed"
case $out in *"held against automatic OUT"*) ;; *) fail "unexpected node hold output: $out" ;; esac

"$T" cluster status --json "$p1" --secret-file "$sec" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
m3 = [m for m in d["members"] if m["id"] == 3][0]
assert isinstance(m3["hold_left_s"], int) and m3["hold_left_s"] > 0, m3
' || fail "hold_left_s not reflected in status --json"

out=$("$T" cluster node hold "$p2" 3 --release --secret-file "$sec") || fail "node hold --release failed"
case $out in *"hold released"*) ;; *) fail "unexpected node hold --release output: $out" ;; esac

"$T" cluster node hold "$p1" 3 --for 2d --secret-file "$sec" >/dev/null 2>&1; rc=$?
[ "$rc" -eq 2 ] || fail "node hold --for 2d: exit $rc, want 2"

out=$("$T" cluster node drain "$p2" 2 --cancel --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "node drain --cancel on up node: exit $rc, want 1"
case $out in *"not draining"*) ;; *) fail "unexpected node drain --cancel error: $out" ;; esac

out=$("$T" cluster node destroyed "$p2" 3 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "node destroyed on up node: exit $rc, want 1"
case $out in *"not out"*) ;; *) fail "unexpected node destroyed error: $out" ;; esac

"$T" cluster node replace "$p1" 2 2 --secret-file "$sec" >/dev/null 2>&1; rc=$?
[ "$rc" -eq 2 ] || fail "node replace 2 2: exit $rc, want 2"

out=$("$T" cluster node replace "$p2" 2 3 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "node replace 2 3: exit $rc, want 1"
case $out in *"already holds"*) ;; *) fail "unexpected node replace error: $out" ;; esac

# Planned transfer of repv (vol 1) to an ISR follower via follower p2.
cur_leader=$("$T" cluster status volume 1 --json "$p1" --secret-file "$sec" | python3 -I -c '
import json, sys
print(json.load(sys.stdin)["volume"]["leader"])
')
target_node=2
[ "$cur_leader" = "2" ] && target_node=3
out=$("$T" cluster volume transfer "$p2" 1 "$target_node" --secret-file "$sec") || fail "volume transfer failed"
case $out in *"node $target_node leads at epoch "*) ;; *) fail "unexpected transfer output: $out" ;; esac

out=$("$T" cluster volume move "$p2" 99 --log --from 1 --to 2 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "volume move 99: exit $rc, want 1"
case $out in *"no such volume"*) ;; *) fail "unexpected volume move error: $out" ;; esac

"$T" cluster volume move "$p1" 1 --log --from 2 --to 2 --secret-file "$sec" >/dev/null 2>&1; rc=$?
[ "$rc" -eq 2 ] || fail "volume move --from 2 --to 2: exit $rc, want 2"

out=$("$T" cluster volume move "$p2" 1 --pos 0 --to 2 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "volume move --pos on replicas vol: exit $rc, want 1"
case $out in *"not erasure-coded"*) ;; *) fail "unexpected volume move --pos error: $out" ;; esac

out=$("$T" cluster replica reset "$p2" 99 2 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "replica reset 99: exit $rc, want 1"
case $out in *"volume 99 does not exist"*) ;; *) fail "unexpected replica reset error: $out" ;; esac

out=$("$T" cluster volume gc resume "$p2" repv --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "volume gc resume on replicas vol: exit $rc, want 1"

# ---- 6. R14 live settings (settings show [--json], settings set, init-only refusal, CAS STALE) ----

hset=$("$T" cluster help settings)
for want in "settings show" "settings set" "view, cluster_uuid, settings_version, settings, live_keys, init_only_keys"; do
    case $hset in *"$want"*) ;; *) fail "help settings missing '$want'" ;; esac
done

set_txt=$("$T" cluster settings show "$p2" --secret-file "$sec") || fail "settings show text failed"
printf '%s\n' "$set_txt" | grep -q '^settings_version 1$' || fail "settings show missing settings_version 1:\n$set_txt"
printf '%s\n' "$set_txt" | grep -qE '^rebuild_mb_s[[:space:]]+150[[:space:]]+live$' || fail "settings show missing rebuild_mb_s live:\n$set_txt"
printf '%s\n' "$set_txt" | grep -qE '^lease_ms[[:space:]]+2000[[:space:]]+init-only$' || fail "settings show missing lease_ms init-only:\n$set_txt"

for cmd in \
    "settings show --json $p1 --secret-file $sec" \
    "settings --json show $p2 --secret-file $sec" \
    "settings show $p1 --json --secret-file $sec"; do
    # shellcheck disable=SC2086
    "$T" cluster $cmd | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
for k in ("view", "cluster_uuid", "settings_version", "settings", "live_keys", "init_only_keys"):
    assert k in d, f"missing key {k}"
assert d["view"] == "settings" and d["settings_version"] == 1
assert d["settings"]["rebuild_mb_s"] == 150 and d["settings"]["lease_ms"] == 2000
assert "rebuild_mb_s" in d["live_keys"] and "lease_ms" in d["init_only_keys"]
assert "lease_ms" not in d["live_keys"] and "rebuild_mb_s" not in d["init_only_keys"]
' || fail "tsdb cluster $cmd JSON check failed"
done

out=$("$T" cluster settings set "$p2" rebuild_mb_s=80 scrub_mb_s=30 --secret-file "$sec") || fail "settings set via follower failed"
case $out in *"settings updated (version 2):"*rebuild_mb_s*150-\>80*) ;; *) fail "unexpected settings set output: $out" ;; esac

out=$("$T" cluster settings set "$p2" lease_ms=5000 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "settings set lease_ms: exit $rc, want 1"
case $out in *"init-only"*"D25"*) ;; *) fail "unexpected lease_ms refusal: $out" ;; esac

out=$("$T" cluster settings set "$p2" --version 1 rebuild_mb_s=90 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "settings set stale --version 1: exit $rc, want 1"
case $out in *"STALE"*) ;; *) fail "unexpected stale refusal: $out" ;; esac

out=$("$T" cluster settings set "$p2" --version 99 rebuild_mb_s=90 --secret-file "$sec" 2>&1); rc=$?
[ "$rc" -eq 1 ] || fail "settings set mismatched --version 99: exit $rc, want 1"
case $out in *"STALE"*) ;; *) fail "unexpected version-99 refusal: $out" ;; esac

out=$("$T" cluster settings show --json "$p1" --secret-file "$sec") || fail "settings show after refused --version failed"
case $out in *'"settings_version": 2'*'"rebuild_mb_s": 80'*) ;; *) fail "refused --version mutated settings: $out" ;; esac

"$T" cluster settings set "$p1" unknown_setting=1 --secret-file "$sec" >/dev/null 2>&1; rc=$?
[ "$rc" -eq 2 ] || fail "settings set unknown_setting: exit $rc, want 2"

# ---- 7. R9 balancer CLI (`balance [--apply]` via follower, `balance=off/on` live setting) ----

out=$("$T" cluster balance "$p2" --secret-file "$sec") || fail "balance dry-run via follower failed"
case $out in *"cluster is balanced"*) ;; *) fail "unexpected balance dry-run output: $out" ;; esac

out=$("$T" cluster balance "$p2" --apply --secret-file "$sec") || fail "balance --apply via follower failed"
case $out in *"already balanced"*) ;; *) fail "unexpected balance --apply output: $out" ;; esac

out=$("$T" cluster settings set "$p2" balance=off --secret-file "$sec") || fail "settings set balance=off failed"
case $out in *"balance on->off"*) ;; *) fail "unexpected balance=off output: $out" ;; esac
out=$("$T" cluster settings set "$p2" balance=on --secret-file "$sec") || fail "settings set balance=on failed"
case $out in *"balance off->on"*) ;; *) fail "unexpected balance=on output: $out" ;; esac

echo "test_cluster_cli: ok"
