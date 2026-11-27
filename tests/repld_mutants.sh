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
# tick dropped: the core's clock slower than the wall's under a slow catch-up step) tickwall (knob slowtick);
# stepcurrentonly (catch-up steps only on ticks that are not late) rejoinlate (knob latetick); stallcatchup (a stop
# of the whole process caught up in ticks and taken for the peers' silence) jointpause; demotestaleidx (a deposed
# writer's index of record starts rebuilt over the entry its old engine pushed after demote cleared it: the cut of
# its uncommitted tail stops the volume) deposetail (knob latebytes). Three are tsdbd's, read by
# it under the same name: nomoved (a write to a non-leader refused UNAVAILABLE, not MOVED) failover; deposemoved (a
# write of unknown outcome on a deposed leader refused, not its connection closed, D12) writerdepose; leadnow (a read
# admitted on the leader and held while it is deposed is checked against the lease only, and answered from the
# replica engine its demotion reopened) readdepose. fencenosync (a FENCE_REPLY sent with the raised fence not in REPL)
# fencecrash. One is the volume manager's (server/cvol.c,
# TSDB_CVOL_MUTANT, set to the same name): refusedsilent (a volume the server refused is left out of its heartbeat
# report, not LOCAL_FAULT) refused. One is the engine's (src/replica.c, planted by tsdbd under the same name):
# replicadropsfloor (a replica's flushed segments go once its floor passes them, the retention caps ignored) failoverkeep.
# replaystartlog (a member reports its log start as its replay start, D17: the floor stays up to repl_retain_s back)
# classfailover.
# promotenone (a driver promoting reports role NONE, so a promotion slower than 3 s is taken for a deposed leader and
# fenced again; the volume's first promotion is slow too, so it may never get a writer) promoteslow (knob promoteslow).
# divergeisr (a volume whose DIVERGED file is set is not stopped: no LOCAL_FAULT, it stays in the ISR and may lead)
# diverged (knob plantdiverge).
# The snapshot transfer's (server/rsnap.c): snapnoverify (a received file not compared with SNAPMAN's xxh64; the
# source sends one file with a flipped byte) and snapmakedown (the link-down timer runs during the make) snapxfer;
# snapsamelink (the files on kind 2, the COMMITs' link) snaplarge; snapnospacecheck (no free-space check, D5)
# snapnospace; snapsrcpromoting (a source serves while promoting, D2) snapbusy; snapnosrcmax (a source past
# rebuild_max) and snapsharedmax (sources and receivers in one count) snapbusy; snapnodonebits (a file begun again
# after its lost DONE counted twice), snapnowaitdone (a lost DONE 0 holds the source for 6 h) and snapnostale (a stale
# request served) snaplost; snapnostall (a receiver hearing nothing waits on) and snapnosrcdown (a source whose
# receiver is gone kept) snapstall; snapnopeerchange is not planted: snapsrckill requires the new leader's notice to
# end the old transfer. The install's (server/snapinst_run.c, rsnap.c, cvol.c): snapstalekeep (keep read before the
# driver stopped: a FENCE meanwhile lost) snapfence; snapinstallstale (a copy at or below the log end installed, or
# the volume closed for it) and snapservebeforeopen (an installed copy opened before REPLICA_OPEN) snapcatch;
# snapkeepdiverged (DIVERGED carried into the fresh copy) snapdiverged; snapsetnodefer (a set change applied during an
# install) addsnap; snapclosekeep (a removed replica's snapshot job left, its v1.snap with it) removesnap; after review: snapdropafter (a copy not installed removed by the worker after the volume went back:
# the prepare's leftovers race it) and snapnofence (a copy below the receiver's fence installed) snapfence;
# snapnoendafter (P not compared with the stopped driver's log end) snapcatch; snapdivlogend (a DIVERGED rebuild
# compared with the log it replaces) snapdiverged; snapnoleftovers (no leftovers at prepare) snapkill. A floor below
# P cannot be planted: replfile_save refuses a floor lowered, and the core
# a floor below the log start.
# Usage: tests/repld_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb build/libcrashfs.so).
set -u
table=(
    "commitbyleader|replindet|a write with both replicas paused was answered"
    "floornotsaved|repcheck|restarted after its flushed segments went refused its log: the log starts above the commit floor"
    "ackbeforesync|ackdurable|after a power loss replica [0-9]+ holds [0-9]+ rows"
    "nodepose|writerdepose|the writer was not deposed"
    "noquiesce|writerfailq|of the failed volume: .*not (UNAVAILABLE|MOVED)"
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
    "replicadropsfloor|failoverkeep|the replica away during the failover was told it needs a snapshot"
    "replaystartlog|classfailover|a class switch on the new leader waited more than 60 s"
    "fastride|rejoin|did not join the ISR again in 30 s"
    "nojoinhold|rejoinsteady|did not join the ISR again in 30 s"
    "inboxstarve|rejoinslow|the leader dropped the healthy replica from the ISR while another"
    "opennohold|reopenhold|the leader dropped the file of a COMMIT marker that a restarted replica had not staged"
    "tickstretch|tickwall|a member stopped under writes left the ISR only after [0-9]+ ms"
    "stepcurrentonly|rejoinlate|did not join the ISR again in 30 s"
    "stallcatchup|jointpause|after a stop of every server the record of jp"
    "keepunanswered|failover|the killed old leader was still in the ISR of fo"
    "promotenone|promoteslow|the record of ps moved past epoch|series pump1.flow in ps: error -11"
    "fencenosync|fencecrash|REPL holds fence [0-9]+ after it stopped at fence-saved-before-reply"
    "nomsgcheck|replflip|the last row did not reach replica"
    "nometarefuse|replmeta|the leader stopped taking writes under the hook|the last row did not reach replica|no metadata-only message was refused"
    "bulkframes|replbulk|no driver frame on a bulk link was refused"
    "metafenceonly|replmeta|no metadata-only message of type [0-9]+ was refused"
    "noepochbound|replepoch|the leader stopped taking writes under the hook|the last row did not reach replica|no APPEND was dropped for an epoch above"
    "nodescepoch|replepoch|the last row did not reach replica|no APPEND was dropped for a descriptor's epoch out of order"
    "checknocmp|drvcheck|acknowledged while replica [0-9]+'s sync was held|a flipped message passed the driver check"
    "checkshort|drvcheck|a flipped message passed the driver check"
    "checklate|drvcheck|acknowledged while replica [0-9]+'s sync was held|a flipped message passed the driver check"
    "nodesccmp|repldesc|the leader stopped taking writes under the hook|the last row did not reach replica|no APPEND with a flipped descriptor was dropped"
    "keepwritten|replicate|the replica restarted under writes: the ISR of plant has [0-9]+ members, not 3"
    "stopnodepose|closegone|its replicas killed: server [0-9]+ still runs [0-9]+ ms after SIGTERM"
    "lateclock|closegone|the leader wrote no WAL record after its driver stopped"
    "syncbyrep|writersync|writersync: batch [0-9]+, a write from connection [0-9]+: .*its commit point stayed below"
    "nomoved|failover|a write to a replica after the failover: .*not MOVED"
    "deposemoved|writerdepose|the write waiting for the commit point on the failed writer got"
    "leadnow|readdepose|held while it was deposed answered"
    "refusedsilent|refused|the refused volume was not reported"
    "demotestaleidx|deposetail|the deposed writer back in the ISR once the links are back: the ISR of rs has 2 members"
    "divergeisr|diverged|heartbeat never reported volume 1 LOCAL_FAULT"
    "snapnoverify|snapxfer|the copy with a flipped byte was not refused"
    "snapsamelink|snaplarge|a large insert of 2000000 rows during a snapshot: kinds 2 and 5 moved .* no more than one bucket"
    "snapnospacecheck|snapnospace|a snapshot whose size plus the margin exceeds the free space was not refused"
    "snapnosrcmax|snapbusy|a leader running rebuild_max sources did not answer BUSY"
    "snapsharedmax|snapbusy|the receiver did not ask while a source job ran on it"
    "snapnodonebits|snaplost|the file begun again after its lost DONE broke the first copy"
    "snapmakedown|snapxfer|the leader released a snapshot while making it"
    "snapnowaitdone|snaplost|the leader kept waiting for a DONE that was lost"
    "snapnostale|snaplost|a request of another set version was served"
    "snapnostall|snapstall|the receiver of a stalled source did not leave it|the replica never received the snapshot"
    "snapnosrcdown|snapstall|a source whose receiver is gone was not released"
    "snapsrcpromoting|snapbusy|a leader that is promoting did not answer BUSY"
    "snapstalekeep|snapfence|the receiver installed a REPL with fence [0-9]+ \\(kept [0-9]+\\) below the epoch"
    "snapinstallstale|snapcatch|a replica in the ISR (installed|took) a copy at or below its log end"
    "snapservebeforeopen|snapcatch|the replica opened its installed copy before the group recorded its open generation"
    "snapkeepdiverged|snapdiverged|the rebuilt replica (stopped as DIVERGED again|joins the ISR)"
    "snapsetnodefer|addsnap|a set change applied on server [0-9]+ while it installed a snapshot"
    "snapclosekeep|removesnap|v1 or v1.snap 20 s after its removal"
    "snapdropafter|snapfence|the receiver neither installed with the raised fence nor refused the stale copy and joined|the refused copy was removed by the snapshot worker"
    "snapnoendafter|snapcatch|a replica in the ISR (installed a copy|took a copy at or below the log end its stopped driver)"
    "snapnofence|snapfence|the old leader's copy was installed on a receiver fenced at [0-9]+ above its epoch"
    "snapdivlogend|snapdiverged|the DIVERGED replica whose log reaches the leader's P (was not rebuilt|refused a copy as stale)"
    "snapnoleftovers|snapkill|after the crash at snap-halfswap the receiver (fetched a copy again|still has)|the receiver restarted after its crash joins the ISR"
    "inboxwaitall|inboxshared|the healthy volume vd has [0-9]+ ISR members while va's inbox on n[0-9]+ is full"
    "downnocap|downbudget|downbudget: past repl_retain_down_gb budget, leader still keeps [0-9]+ WAL segments"
    "keepafterout|holdkeep|holdkeep: after META_OUT, leader still keeps [0-9]+ WAL segments"
    "fullafteradmit|full|full: refused row .* appeared"
    "youngleavebelow|youngscrub|left ISR below min_isr"
)
export CRASHFS_SO=${CRASHFS_SO:-build/libcrashfs.so} TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb} TSDB_TEST_ACK_MS=${TSDB_TEST_ACK_MS:-3}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
row_of() { # the table's row of mutant $1 (empty when there is none)
    local e
    row=
    for e in "${table[@]}"; do [ "${e%%|*}" = "$1" ] && row=$e; done
    [ -n "$row" ] || return 1
    rest=${row#*|}; s=${rest%%|*}; want=${rest#*|}
}
# one mutant, then its verdict: tools/mutants.sh runs them in parallel, each in a scratch directory of its own, and
# asks this script (--mutant-verify) whether the scenario failed with its oracle's own line
case ${1:-} in
--mutant-run)
    row_of "$2" || { echo "mutant $2: no scenario is named for it in $0"; exit 0; }
    TSDB_REPLD_MUTANT=$2 TSDB_CVOL_MUTANT=$2 exec "$TEST_CLUSTER" "$3" "$s" ;;
--mutant-verify)
    row_of "$2" || { echo "no scenario is named for it in $0"; exit 1; }
    fail=$(grep -m1 FAIL "$4")
    if [ "$3" -ne 0 ] && grep -m1 FAIL "$4" | grep -qE "$want"; then
        echo "caught by $s ($fail)"
    elif [ "$3" -ne 0 ]; then
        echo "$s failed (rc $3) but not with its oracle's line /$want/: ${fail:-no FAIL line}"
        exit 1
    else
        echo "$s passed"
        exit 1
    fi
    exit 0 ;;
esac
dir=${1:-/tmp}; shift || true
mkdir -p "$dir" || exit 1
all=
for e in "${table[@]}"; do all="$all ${e%%|*}"; done
muts=${*:-$all}
# MUTANT_JOBS, MUTANT_URING_JOBS (every run starts tsdbd on io_uring unless the test says otherwise), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name repld --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --fail \
    --verify "$0" --list "$muts" -- "$0" --mutant-run @M@ @D@
