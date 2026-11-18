#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replication driver's mutants (server/repld.c, TSDB_REPLD_MUTANT): each planted bug runs the one test_cluster
# scenario whose oracle is meant for it, and counts as caught only when that scenario fails with its own FAIL line
# (a crash, a timeout, a startup flake or a failure for another reason is no catch: a gate that took any failure
# would not notice an oracle that lost its teeth while a side effect still failed the run). repcheck checks the
# latency of single-row writes only here (TSDB_TEST_ACK_MS: 1 to 1.5 ms with the fast path, about 4.7 ms without it on
# the build machine; too close for make test under TSan, where it only prints them).
# The ISR, keep, ship and backpressure mutants (r3d-isr) are pinned the same way: commitpastremoved (META handed to the
# core before the metadata group committed it) isr; nominisr (the core without min_isr) and ackbelowminisr (APPLY not
# held below min_isr) minisr; shipperfile (30 s for each file of a COMMIT) and shipnoisr (the ship wait ignores a
# replica's exit from the ISR) shipstall; nocap (no keep caps) and nologstartcheck (the core never hears the log start
# moved) keepcap; noseal (no idle flush) idleseal; noinboxbound (an unbounded inbox) and nospill (every bulk file whole
# in memory) backpressure; keepignorespeers (the keep bound ignores what the replicas acked) keepfloor; keepwritten (the
# keep bound by the replicas' written ends: a replica out of the ISR with a COMMIT marker but not its file finds the link
# gone) replicate (its knob fetchlate); stopnodepose (the stop leaves the leader in place) and lateclock (a new leader's
# NOW markers start only at its first SAVE as a writer) closegone; syncbyrep (a writer's SYNC answered only when the WAL
# moved past the engine's last report) writersync (knobs syncbyhook, synclate); inboxwaitall (a full inbox's every
# frame waits on the shared link) inboxshared; fastride (the cursor back on a resync only once sent_ms is old) rejoin;
# nojoinhold (no commit point held for a learner close behind) rejoinsteady; inboxstarve (no inbox time after a tick
# while the next is due) rejoinslow; opennohold (a replica's core opens with its whole log durable, past a COMMIT
# marker whose file it has not staged) reopenhold (knobs dropbulk, nofetch, syncpause, fetchlate); tickstretch (a late
# tick dropped: the core's clock slower than the wall's under a slow catch-up step) tickwall (knob slowtick).
# Usage: tests/repld_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb build/libcrashfs.so).
set -u
dir=${1:-/tmp}; shift || true
# mutant, its scenario, an extended regular expression its FAIL line must match
table=(
    "commitbyleader|replindet|a write with both replicas paused was answered"
    "floornotsaved|replicate|the replica restarted under writes refused its log: the log starts above the commit floor"
    "ackbeforesync|ackdurable|after a power loss replica [0-9]+ holds [0-9]+ rows"
    "nodepose|writerdepose|the writer was not deposed"
    "noquiesce|writerfail|of the failed volume: .*not UNAVAILABLE"
    "floornottold|repcheck|its saved floor never removed its flushed segments"
    "unsureignored|repcheck|opened with a damaged REPL slot and saved unsure 1"
    "admitnocheck|repcheck|a write whose core position is not its log number was acknowledged"
    "nofastpath|repcheck|the median latency of a single-row write is"
    "nocursorup|repcheck|the cursor never moved up to what the peer acknowledged"
    "commitpastremoved|isr|a write was acknowledged without a killed ISR member before the ISR change committed"
    "nominisr|minisr|the leader never said that its ISR is below min_isr"
    "ackbelowminisr|minisr|a write committed only after the ISR shrank below min_isr was answered"
    "shipperfile|shipstall|a large write over ten partitions with a replica stopped took [0-9]+ ms"
    "shipnoisr|shipstall|a large write over ten partitions with a replica stopped took [0-9]+ ms"
    "nocap|keepcap|the leader keeps [0-9]+ WAL segments for a replica away longer than repl_retain_s"
    "nologstartcheck|keepcap|the replica back past the keep cap never heard it needs a snapshot"
    "noseal|idleseal|an idle volume's rows were not in parts on every server"
    "noinboxbound|backpressure|the stalled replica held the leader's whole stream in its inbox"
    "nospill|backpressure|a replica received a part above the spill threshold into memory"
    "keepignorespeers|keepfloor|the leader keeps [0-9]+ WAL segments, not the 5 flushed ones a replica away has not acked"
    "fastride|rejoin|did not join the ISR again in 30 s"
    "nojoinhold|rejoinsteady|did not join the ISR again in 30 s"
    "inboxstarve|rejoinslow|the leader dropped the healthy replica from the ISR while another"
    "opennohold|reopenhold|the leader dropped the file of a COMMIT marker that a restarted replica had not staged"
    "tickstretch|tickwall|a member stopped under writes left the ISR only after [0-9]+ ms"
    "keepwritten|replicate|the replica restarted under writes: the ISR of plant has [0-9]+ members, not 3"
    "stopnodepose|closegone|its replicas killed: server [0-9]+ still runs [0-9]+ ms after SIGTERM"
    "lateclock|closegone|the leader wrote no WAL record after its driver stopped"
    "syncbyrep|writersync|writersync: batch [0-9]+, a write from connection [0-9]+: .*its commit point stayed below"
    "inboxwaitall|inboxshared|the healthy volume vd has [0-9]+ ISR members while va's inbox on n[0-9]+ is full"
)
all=
for e in "${table[@]}"; do all="$all ${e%%|*}"; done
muts=${*:-$all}
export CRASHFS_SO=${CRASHFS_SO:-build/libcrashfs.so} TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb} TSDB_TEST_ACK_MS=${TSDB_TEST_ACK_MS:-3}
survived=
for m in $muts; do
    row=
    for e in "${table[@]}"; do [ "${e%%|*}" = "$m" ] && row=$e; done
    if [ -z "$row" ]; then
        echo "mutant $m: no scenario is named for it in $0"
        survived="$survived $m"
        continue
    fi
    rest=${row#*|}; s=${rest%%|*}; want=${rest#*|}
    log="$dir/mut-$m-$s.log"
    TSDB_REPLD_MUTANT=$m timeout 600 ./build/test_cluster "$dir" "$s" > "$log" 2>&1
    rc=$?
    fail=$(grep -m1 FAIL "$log")
    if [ $rc -ne 0 ] && grep -m1 FAIL "$log" | grep -qE "$want"; then
        echo "mutant $m: caught by $s ($fail)"
    elif [ $rc -ne 0 ]; then
        echo "mutant $m: $s failed (rc $rc) but not with its oracle's line /$want/: ${fail:-no FAIL line}"
        survived="$survived $m"
    else
        echo "mutant $m: SURVIVED $s"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "not caught by their own oracle:$survived"; exit 1; }
echo "every mutant was caught by its own oracle"
