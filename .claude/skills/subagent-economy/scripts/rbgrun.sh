#!/bin/bash
# rbgrun.sh NAME WORKTREE PROFILES CMD...: like bgrun.sh, but CMD runs on a FLEET machine (RB_HOSTS in config.sh), not here.
#   PROFILES = comma list of build/<profile> dirs to ship (release,debug,...) or "-" for none; the source tree is always
#   shipped (without .git and build/). CMD runs as root in the shipped copy ($RB_ROOT/NAME on the machine, default
#   /dev/shm/iso-ci; TMPDIR=/dev/shm/t, SHORT on purpose: tests that build long paths from it make mkdtemp fail with
#   EINVAL on a longer prefix; RAM, so fsync-heavy tests are free), with nofile/memlock raised and RB_REMOTE_ENV set.
#   Output -> $RUNS/NAME/out, exit code -> $RUNS/NAME/rc (last), host used -> $RUNS/NAME/host. Check with await.sh.
# One job per machine (lock dir $RB_LOCK there). Skips every machine while $SE_HOME/HOLD.fleet or $SE_HOME/RESERVED exists
# or $SE_HOME/BENCH.lock holds a live pid (a benchmark or power-cut series owns the fleet), and a machine running a
# process RB_BUSY names. Env: RB_HOST=user@host forces one, RB_FETCH="rel/path ..." copies those paths back into
# $RUNS/NAME/fetch/, RB_KEEP=1 keeps the remote copy.
# MIRROR MODE (scripts with absolute paths, mutation batches that edit and rebuild a worktree): WORKTREE may be "-";
# RB_MIRROR="/abs/dir ..." copies each dir to the same path on the machine (as root; RB_GIT=1 adds $RB_REPO/.git for git
# worktrees), the command runs there (RB_CWD=dir picks its cwd), RB_BACK="/abs/dir ..." copies those dirs back when it ends
# (logs!), and the mirrored copies are removed (RB_KEEP=1 keeps them). Mirror paths must lie under RB_MIRROR_ROOTS.
# Do not mirror a dir another job still writes (copying a tree being built gives "file has vanished").
# Never run a fleet-wide thing (power cut, format, bench) through this: those use the fleet's own tools.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
n=$1; wt=$2; prof=$3; shift 3 2>/dev/null
[ -n "$n" ] && { [ "$wt" = "-" ] || [ -d "$wt" ]; } && [ -n "$prof" ] && [ $# -gt 0 ] || { echo "usage: rbgrun.sh NAME WORKTREE|- PROFILES|- CMD..." >&2; exit 2; }
se_name_ok "$n"
[ -n "${RB_HOST:-$RB_HOSTS}" ] || { echo "rbgrun: no fleet: set RB_HOSTS in $SE_HOME/config.sh (see config.example.sh)" >&2; exit 2; }
for m in $RB_MIRROR $RB_BACK; do
  ok=; [[ "$m" == *..* ]] || for r in ${RB_MIRROR_ROOTS:-$SE_HOME}; do [[ "$m" == "${r%/}"/?* ]] && ok=1; done
  [ -n "$ok" ] || { echo "rbgrun: mirror path $m is not under RB_MIRROR_ROOTS (${RB_MIRROR_ROOTS:-$SE_HOME})" >&2; exit 2; }
done
[ -z "$RB_GIT" ] || [ -d "$RB_REPO/.git" ] || { echo "rbgrun: RB_GIT=1 needs RB_REPO (a repo with .git)" >&2; exit 2; }
d=$RUNS/$n; [ -e $d/pid ] && kill -0 "$(cat $d/pid)" 2>/dev/null && { echo "rbgrun: $n is already running (pid $(cat $d/pid))" >&2; exit 3; }
command rm -rf "$d"; mkdir -p "$d"; date +%s > $d/start; printf '%s\n' "$*" > $d/cmd
[ "$wt" = "-" ] || wt=$(readlink -f "$wt")
export RB_N=$n RB_WT=$wt RB_PROF=$prof RB_D=$d SE_HOME
nohup setsid bash "$SE_BIN/rbworker.sh" "$@" > $d/out 2>&1 &
echo $! > $d/pid
echo "started $n on the fleet (pid $!). Do NOT wait here. Write the check into NOTES.md and end your stage with wait={name:\"$n\"}; or, if you are the waiter: await.sh $n"
