#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# rx.sh [-j JOB | -H HOST] CMD...: run CMD on the build machine in this directory, copying nothing (read what a job left there: its
# build dirs, fuzz crashes, logs of a failed test). See se-remote.sh. With a fleet (SE_REMOTES) the machine is the one the
# directory's last bgrun.sh or rrun.sh job ran on (this directory or a parent of it), or -j JOB's (runs/JOB/host), or -H HOST
# (also RX_HOST): the build dirs are only there. A fleet and no job known: refused, naming the hosts.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
. "$SE_BIN/se-remote.sh"
h=${RX_HOST:-}
while [ $# -gt 0 ]; do
  case "$1" in
    -j) [ $# -ge 2 ] || { echo "usage: rx.sh [-j JOB | -H HOST] CMD..." >&2; exit 2; }
        se_name_ok "$2"; h=$(cat "$RUNS/$2/host" 2>/dev/null) || { echo "rx: job $2 has no recorded host in $RUNS/$2" >&2; exit 2; }
        shift 2 ;;
    -H) [ $# -ge 2 ] || { echo "usage: rx.sh [-j JOB | -H HOST] CMD..." >&2; exit 2; }; h=$2; shift 2 ;;
    --) shift; break ;;
    *) break ;;
  esac
done
[ $# -gt 0 ] || { echo "usage: rx.sh [-j JOB | -H HOST] CMD..." >&2; exit 2; }
se_remote_on || { echo "rx: no build machine (SE_REMOTE or SE_REMOTES in $SE_HOME/config.sh)" >&2; exit 2; }
[ -n "$h" ] || h=$(se_cwd_host "$PWD")
if [ -z "$h" ]; then
  [ ${#SE_FH[@]} -eq 1 ] || { echo "rx: no job ran in $PWD yet, so the build machine is unknown: rx.sh -H HOST (one of: ${SE_FH[*]}) or -j JOB" >&2; exit 2; }
  h=${SE_FH[0]}
fi
se_use_host "$h" || exit 2
rssh "cd $(printf '%q' "$PWD") && bash -c $(printf '%q' "$(printf '%q ' "$@")")"
