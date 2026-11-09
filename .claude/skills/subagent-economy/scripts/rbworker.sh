#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# rbworker.sh CMD...: the detached half of rbgrun.sh (env RB_N RB_WT RB_PROF RB_D). Not called by hand.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
K="${RB_KEY:+-i $RB_KEY} -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o ServerAliveInterval=30"
hosts=${RB_HOST:-$RB_HOSTS}
LK=${RB_LOCK:-/dev/shm/iso-ci-lock}   # the default is shared with older copies of these scripts, so neither double-books a machine
R=${RB_ROOT:-/dev/shm/iso-ci}/$RB_N
fin() { echo "$1" > $RB_D/rc.tmp; mv $RB_D/rc.tmp $RB_D/rc; exit "$1"; }
sshh() { local h=$1; shift; ssh $K "$h" "$@"; }
cleanup() { [ -n "$H" ] && sshh $H "[ -f $LK/owner ] && [ \"\$(cat $LK/owner)\" = \"$RB_N $$\" ] && { [ -n \"$RB_KEEP\" ] || sudo rm -rf $R /dev/shm/t $RB_MIRROR; rm -rf $LK; }" 2>/dev/null; }
trap 'cleanup; fin 143' TERM INT
H=
while [ -z "$H" ]; do
  if [ -e $SE_HOME/HOLD.fleet ] || [ -e $SE_HOME/RESERVED ] || { [ -f $SE_HOME/BENCH.lock ] && kill -0 "$(cat $SE_HOME/BENCH.lock 2>/dev/null)" 2>/dev/null; }; then sleep 30; continue; fi
  for h in $hosts; do
    busy=; [ -n "$RB_BUSY" ] && busy="; pgrep -x '$RB_BUSY' | head -1 | sed 's/^/PROC /'"
    r=$(sshh $h "if mkdir $LK 2>/dev/null; then echo '$RB_N $$' > $LK/owner; echo GOT; else cat $LK/owner 2>/dev/null; fi$busy" 2>/dev/null)
    case "$r" in
      GOT*) if echo "$r" | grep -q '^PROC'; then sshh $h "rm -rf $LK" 2>/dev/null; else H=$h; break; fi ;;
      *) o=$(echo "$r" | head -1); op=${o##* }  # stale lock: its owner worker (a pid on this machine) is gone
         [ -n "$op" ] && [ "${op//[0-9]/}" = "" ] && ! kill -0 "$op" 2>/dev/null && sshh $h "rm -rf $LK" 2>/dev/null ;;
    esac
  done
  [ -z "$H" ] && sleep 20
done
echo "$H" > $RB_D/host; echo "rbgrun: $RB_N on $H"
sshh $H "sudo rm -rf $R /dev/shm/t; mkdir -p $R/build /dev/shm/t" || { cleanup; fin 97; }
SSHR="ssh $K"
if [ "$RB_WT" != "-" ]; then
  rsync -a --delete -e "$SSHR" --exclude=.git --exclude='/build' $RB_WT/ $H:$R/ || { cleanup; fin 98; }
  if [ "$RB_PROF" != "-" ]; then for p in ${RB_PROF//,/ }; do
    rsync -a -e "$SSHR" --exclude='/obj' $RB_WT/build/$p $H:$R/build/ || { echo "rbgrun: build/$p missing in $RB_WT"; cleanup; fin 99; }
  done; fi
fi
# Mirror mode: each dir in RB_MIRROR is copied to the SAME absolute path on the machine (as root), so a script with absolute
# paths runs unmodified there; $RB_REPO/.git rides along when RB_GIT=1 (worktrees keep their git metadata there).
# A pass-through stub replaces SE_QUEUE there: one job per machine, so there is no queue to wait in.
if [ -n "$RB_MIRROR" ]; then
  [ -n "$SE_QUEUE" ] && sshh $H "sudo mkdir -p ${SE_QUEUE%/*}; printf '#!/bin/bash\nexec \"\$@\"\n' | sudo tee $SE_QUEUE >/dev/null; sudo chmod +x $SE_QUEUE"
  [ -n "$RB_GIT" ] && sshh $H "sudo git config --system safe.directory '*'"
  for m in $RB_MIRROR ${RB_GIT:+$RB_REPO/.git}; do
    sshh $H "sudo mkdir -p $m" && rsync -a --delete -e "$SSHR" --rsync-path="sudo rsync" $m/ $H:$m/ || { echo "rbgrun: mirroring $m failed"; cleanup; fin 96; }
  done
fi
q=$(printf '%q ' "$@")
sshh $H "cd ${RB_CWD:-$R} && sudo env TMPDIR=/dev/shm/t HOME=/root $RB_REMOTE_ENV prlimit --nofile=1048576:1048576 --memlock=unlimited bash -c $(printf '%q' "$q")"
rc=$?
if [ -n "$RB_BACK" ]; then for m in $RB_BACK; do rsync -a -e "$SSHR" --rsync-path="sudo rsync" $H:$m/ $m/ || echo "rbgrun: copying $m back failed"; done; fi
if [ -n "$RB_FETCH" ]; then mkdir -p $RB_D/fetch; for f in $RB_FETCH; do sshh $H "sudo tar -C $R -cf - $f" 2>/dev/null | tar -C $RB_D/fetch -xf - ; done; fi
cleanup; fin $rc
