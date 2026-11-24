#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# se-rjob.sh CMD...: the detached half of bgrun.sh on the build machine (env RJ_N RJ_D RJ_HOST RJ_CWD RJ_ROOTS RJ_NOWAIT; see
# se-remote.sh). Copies the trees there, starts the job detached THERE (it survives a lost connection), copies its
# output to $RJ_D/out as it grows, waits for its exit code, brings back what it wrote, writes $RJ_D/rc. Not called by hand.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
. "$SE_BIN/se-remote.sh"
[ -z "$RJ_HOST" ] || se_use_host "$RJ_HOST" || exit 2   # the host bgrun.sh picked (recorded in $RJ_D/host); none: SE_REMOTE
exec >> "$RJ_D/worker.log" 2>&1
RR=$SE_REMOTE_RUNS/$RJ_N
pg=
fin() { echo "$1" > "$RJ_D/rc.tmp"; mv "$RJ_D/rc.tmp" "$RJ_D/rc"; exit "$1"; }
note() { printf '%s\n' "$*" >> "$RJ_D/out"; }
pull_out() { rsync -a -e "ssh $RK" "$SE_REMOTE:$RR/out" "$RJ_D/out" 2>/dev/null; }
stop() {
  # only a real process group of our own job there, never 0 or 1
  [[ "$pg" =~ ^[0-9]+$ ]] && [ "$pg" -gt 1 ] && rssh "kill -TERM -- -$pg" 2>/dev/null
  pull_out; note "== stopped (the job's process group on the build machine got SIGTERM)"; fin 143
}
trap stop TERM INT HUP

echo "$(date +%T) push to $SE_REMOTE: $RJ_ROOTS"
rs_push $RJ_ROOTS || { note "bgrun: copying the trees to the build machine failed (see $RJ_D/worker.log)"; fin 98; }

q=$(printf '%q ' "$@")
case "$RJ_N" in v?tt[0-9]*|tt[0-9]*) envs= ;;   # test-trust jobs pick their own dirs (its lanes use /tmp on purpose)
  *) envs="export TMPDIR=/var/tmp/t/$RJ_N JEPSEN_DIR=/var/tmp/j/$RJ_N; mkdir -p /var/tmp/t/$RJ_N /var/tmp/j" ;; esac
if [ -n "$RJ_NOWAIT" ]; then
  run="bash $RR/cmd.sh 8>&- >> $RR/out 2>&1; rc=\$?"
else
  run="mkdir -p $SE_REMOTE_RUNS/.slots; rc=
while [ -z \"\$rc\" ]; do
  for i in \$(seq 1 $SE_REMOTE_SLOTS); do
    exec 8> $SE_REMOTE_RUNS/.slots/\$i
    if flock -n 8; then echo \"== build machine slot \$i, \$(date +%T)\" >> $RR/out; bash $RR/cmd.sh 8>&- >> $RR/out 2>&1; rc=\$?; break; fi
    exec 8>&-
  done
  [ -n \"\$rc\" ] || sleep $SE_RJOB_POLL
done"
fi
job="cd $(printf '%q' "$RJ_CWD") || { echo \"bgrun: $RJ_CWD is missing on the build machine\" >> $RR/out; echo 97 > $RR/rc; exit; }
ulimit -n 524288 2>/dev/null
$envs
$run
echo \$rc > $RR/rc.tmp; mv $RR/rc.tmp $RR/rc"
printf '%s\n' "$q" > "$RJ_D/cmd.sh"
pg=$(rssh "rm -rf $RR && mkdir -p $RR && cat > $RR/cmd.sh && printf '%s\n' $(printf '%q' "$job") > $RR/job.sh && touch $RR/start && echo '== on the build machine, waiting for a slot ($SE_REMOTE_SLOTS at once)' > $RR/out && cd $RR && (setsid bash job.sh < /dev/null > /dev/null 2>&1 & echo \$!)" < "$RJ_D/cmd.sh")
[[ "$pg" =~ ^[0-9]+$ ]] && [ "$pg" -gt 1 ] || { note "bgrun: could not start the job on the build machine ($pg)"; fin 97; }
echo "$pg" > "$RJ_D/rpgid"; echo "$(date +%T) started there, pgid $pg"

fails=0; rc=
while [ -z "$rc" ]; do
  sleep $SE_RJOB_POLL
  r=$(rssh "if [ -f $RR/rc ]; then cat $RR/rc; elif kill -0 $pg 2>/dev/null; then echo RUN; else sleep 3; cat $RR/rc 2>/dev/null || echo GONE; fi") || {
    fails=$((fails + 1)); [ $fails -lt 480 ] && continue
    note "bgrun: lost the build machine for 2 hours; the job may still run there (pgid $pg)"; fin 95; }
  fails=0
  pull_out
  case "$r" in
    RUN) ;;
    GONE) note "bgrun: the job ended on the build machine without an exit code"; fin 94 ;;
    *) [[ "$r" =~ ^[0-9]+$ ]] && rc=$r ;;
  esac
done
pull_out
tmpd=; [ -n "$envs" ] && tmpd=/var/tmp/t/$RJ_N
rs_back "$RR/start" $RJ_ROOTS $tmpd
note "== ran on the build machine ($SE_REMOTE). Files it wrote came back here; its build dirs (build*/, target/) and binaries"
note "== stay there. To read them: $SE_BIN/rx.sh -j $RJ_N CMD (runs CMD there in the job's directory), e.g. rx.sh ls build-fuzz/out"
echo "$(date +%T) done rc=$rc"
fin "$rc"
