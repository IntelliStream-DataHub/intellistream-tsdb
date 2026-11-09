#!/bin/bash
# Runs the cluster tutorial's command block (docs/cluster-preview.md) word for word and checks what it shows.
#
#   tests/test_cluster_tutorial.sh           (make test-cluster-tutorial does this after building)
#
# The block is the first fenced block after the "<!-- cluster-tutorial" marker. Four things are changed in a copy of
# it, nothing else:
#   /tmp/tsdb-cluster   becomes a fresh scratch directory;
#   the two port numbers of a node (--port $(port $1), --peer-port $(pport $1)) become --port 0 and --peer-port 0 with
#       a port file and --exit-with-parent=$$ (the shell that runs the block): AGENTS.md "Ports", a server picks its
#       port, a test never does, and a test killed by its timeout leaves no server behind;
#   the two helper lines marked "# ports" (client port and peer port of node N) read the port file instead (the
#       page says that these two lines are the only place to change a port, so nothing else may name one);
#   build/ becomes $B/ (B=build-tsan for a sanitizer build).
# A restart gets a new port, and the cluster has to cope (its members announce a new address): that is part of
# what the test checks, though a reader on fixed ports never meets it.
#
# Then: every line the page shows after a "<!-- cluster-tutorial-output -->" marker is in the real output once the
# parts that differ from run to run are replaced by placeholders on both sides (ports, the cluster id, "heard" times,
# term and commit numbers, the leader's number, the scratch directory), and the facts the page claims are checked
# on the real output: three voters, node 1 down and then up with incarnation 2, the data on node 1 only, the witness
# a watcher with no volumes. If the page's commands are wrong, this fails: that is its point.
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
DOC=${DOC:-docs/cluster-preview.md}

scratch=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-clt.XXXXXX")
D=$scratch/c
cleanup() {
    # Stop only the servers this test started, by the pids the block saved (they exit with the block's shell anyway).
    for n in 1 2 3 4; do
        if [ -s "$D/n$n/pid" ]; then
            pid=$(cat "$D/n$n/pid")
            case $pid in ''|*[!0-9]*|0|1) pid= ;; esac
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null
                for _ in $(seq 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
            fi
        fi
    done
    rm -rf "$scratch"
}
trap cleanup EXIT

fail() {
    echo "test_cluster_tutorial: FAIL: $*" >&2
    echo "--- last output ---" >&2
    tail -n 40 "$scratch/out" >&2
    for n in 1 2 3 4; do
        if [ -f "$D/n$n/log" ]; then echo "--- n$n/log ---" >&2; tail -n 8 "$D/n$n/log" >&2; fi
    done
    exit 1
}

awk '/^<!-- cluster-tutorial:/ { want = 1; next }
     want && /^```sh$/ { inblock = 1; next }
     inblock && /^```$/ { exit }
     inblock { print }' "$DOC" > "$scratch/block.orig"
[ -s "$scratch/block.orig" ] || { echo "test_cluster_tutorial: FAIL: no tutorial block in $DOC" >&2; exit 1; }

awk '/^<!-- cluster-tutorial-output/ { want = 1; next }
     want && /^```/ { if (inblock) { inblock = 0; want = 0 } else inblock = 1; next }
     inblock { print }' "$DOC" > "$scratch/shown.txt"
[ -s "$scratch/shown.txt" ] || { echo "test_cluster_tutorial: FAIL: no output blocks in $DOC" >&2; exit 1; }

# The rewrite. Each pattern must be in the block (else the page changed and the test has to follow).
for pat in 'D=/tmp/tsdb-cluster' '--port $(port $1)' '--peer-port $(pport $1)' 'pport() { echo 1709$1; }' \
    'port() { echo 1708$1; }'; do
    grep -qF -- "$pat" "$scratch/block.orig" || { echo "test_cluster_tutorial: FAIL: the block no longer has: $pat" >&2; exit 1; }
done
# The page says the two lines marked "# ports" are the only place a port is written: a reader who changes a port there
# (because it is taken) must get servers and helpers on the new number. So no other line of the block names one.
stray=$(grep -E '170[89][0-9]' "$scratch/block.orig" | grep -v '# ports' || true)
[ -z "$stray" ] || { echo "test_cluster_tutorial: FAIL: a port is written outside the lines marked '# ports': $stray" >&2; exit 1; }
sed -e "s#^D=/tmp/tsdb-cluster\$#D=$D#" \
    -e 's#--port \$(port \$1)#--port 0#' \
    -e 's#--peer-port \$(pport \$1)#--peer-port 0 --port-file $D/n$1/port --exit-with-parent=$$#' \
    -e 's#^pport() { echo 1709\$1; }#pport() { cut -d" " -f4 $D/n$1/port 2>/dev/null; }#' \
    -e 's#^port() { echo 1708\$1; }#port() { cut -d" " -f1 $D/n$1/port; }#' \
    -e "s#^\\(build/\\)#$B/#" \
    -e "s#\\([ <(]\\)build/#\\1$B/#g" "$scratch/block.orig" > "$scratch/block.sh"
! grep -q '1708\$1\|1709\$1\|/tmp/tsdb-cluster' "$scratch/block.sh" || fail "the block still names a fixed port or /tmp/tsdb-cluster after the rewrite"

if command -v timeout >/dev/null 2>&1; then limit=(timeout 300)
elif command -v gtimeout >/dev/null 2>&1; then limit=(gtimeout 300)
else limit=(perl -e 'alarm 300; exec @ARGV' --); fi
# -e: the first command that fails ends the tutorial, as it would for a reader who pastes it.
if ! "${limit[@]}" bash -e "$scratch/block.sh" > "$scratch/out" 2>&1; then
    fail "the tutorial block stopped with an error"
fi

# Placeholders for what differs from run to run, on both sides.
norm() {
    sed -E -e 's/[[:space:]][[:space:]]*/ /g' -e 's/ $//' \
        -e 's#127\.0\.0\.1:[0-9]+#127.0.0.1:PORT#g' \
        -e 's/heard [^ ]+/heard H/' \
        -e 's/cluster [0-9a-f]{32} /cluster UUID /' \
        -e 's/^node [0-9]+ mode ([a-z]+) role ([a-z-]+) term [0-9]+ leader [0-9]+ commit [0-9]+ applied [0-9]+$/node N mode \1 role R term T leader L commit C applied A/' \
        -e 's/read_barrier (on|off|\?)$/read_barrier X/' \
        -e 's/^member [0-9]+ (up|down) incarnation/member M \1 incarnation/' \
        -e 's/ rack rack[0-9]+ / rack rackM /'
}
# The killed node is the leader of the moment, and an election before step 7 can make it node 2 or 3, not node 1: the
# page's member lines (it names member 1, rack1) must match whichever member the run killed.
a=$(printf '%s\n' 'member 1 up incarnation 2 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 0ms' | norm)
b=$(printf '%s\n' 'member 3 up incarnation 2 addr 127.0.0.1:44444 peer 127.0.0.1:44445 rack rack3 heard 12ms' | norm)
[ "$a" = "$b" ] || fail "norm does not treat the member killed in step 7 alike whichever node it is: '$a' and '$b'"

sed -e "s#$D#/tmp/tsdb-cluster#g" "$scratch/out" | norm > "$scratch/out.norm"
nshown=0
while IFS= read -r line; do
    case $line in ''|*...*) continue ;; esac
    nshown=$((nshown + 1))
    grep -qxF -- "$(printf '%s\n' "$line" | norm)" "$scratch/out.norm" \
        || fail "the page shows a line that the tutorial does not print: $line"
done < "$scratch/shown.txt"
[ "$nshown" -ge 30 ] || fail "only $nshown lines of the page's output were checked"

# The facts. Everything the block printed is in "out"; the block cleaned up its servers and directories.
expect() {  # expect COUNT PATTERN: COUNT lines of the output match the extended regex PATTERN
    n=$(grep -cE -- "$2" "$scratch/out")
    [ "$n" -eq "$1" ] || fail "expected $1 line(s) matching '$2', found $n"
}
expect 4 '^group voters 1,2,3 learners -$'                  # steps 5, 7, 8 and 9: three voters, never a learner left
expect 1 '^node [0-9]+ mode none role - term 0 leader 0 commit 0 applied 0$'
expect 1 '^cluster \(no state yet\)$'                         # step 4: node 1 before the init
expect 1 '^initialised: node 1 at '
expect 3 '^added 127\.0\.0\.1:[0-9]+$'                       # nodes 2 and 3, then the witness
expect 1 '^node [0-9]+ is the leader: stopping it$'
expect 1 '^member [123] down incarnation 1 '                # the killed leader (node 1 unless an election moved it)
expect 2 '^member [123] up incarnation 2 '                  # step 8, and step 9 (the witness sees the new incarnation)
expect 1 '^node 4 mode watch role - '
expect 1 '^member 4 up incarnation 1 .* volumes_ok 0 '
expect 1 '^node 2 has 0 series$'
expect 1 '^pump7\.flow	float32$'
expect 1 '^wrote 1440 rows$'
# steps 7 and 8 answer at a term above 1 (an election happened)
expect 2 '^node [123] mode core role (leader|follower) term [2-9][0-9]* leader [123] '
grep -q 'proposes node [123] down' "$scratch/out" || fail "the logs (step 10) has no 'proposes node [123] down'"
grep -q 'proposes node [123] up (incarnation 2)' "$scratch/out" || fail "the logs (step 10) has no 'proposes node [123] up (incarnation 2)'"
grep -q 'leads the metadata group' "$scratch/out" || fail "the logs (step 10) has no 'leads the metadata group'"

# The block stopped its servers and removed its directory.
[ ! -e "$D" ] || fail "the block left $D behind"
echo "test_cluster_tutorial: ok"
