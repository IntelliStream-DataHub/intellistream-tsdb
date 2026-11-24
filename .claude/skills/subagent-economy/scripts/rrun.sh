#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# rrun.sh CMD...: run CMD on the build machine NOW, in this directory, with its output here (a build, one short test:
# under ~4 minutes; longer runs go through bgrun.sh). The trees are copied there first and what CMD wrote comes back
# (se-remote.sh); build dirs stay there. Without SE_REMOTE it runs CMD here. With a fleet (SE_REMOTES) it runs on the host this
# directory's earlier jobs ran on (the incremental build is there), else the first host with a free slot; SE_HOST
# and BGRUN_TAG choose, as for bgrun.sh. rx.sh in this directory then reads that host.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
. "$SE_BIN/se-remote.sh"
[ $# -gt 0 ] || { echo "usage: rrun.sh CMD..." >&2; exit 2; }
[ -z "${SE_REMOTES:-}" ] || se_remote_on || { echo "rrun: SE_REMOTES in $SE_HOME/config.sh has no valid entry" >&2; exit 2; }
se_remote_on || exec "$@"
host=$(se_cwd_host "$PWD")
if [ -z "$host" ] || ! se_cands "$PWD" | grep -qx "$(se_host_idx "$host")"; then host=$(se_pick_host "$PWD"); [ $? -ne 2 ] || exit 2; fi
se_use_host "$host" || exit 2
se_cwd_note "$PWD" "$host"
roots=$(rs_job_roots "$PWD" "$*") || exit 2
rs_push $roots || { echo "rrun: copying the trees to the build machine failed" >&2; exit 98; }
m=$SE_REMOTE_RUNS/rrun.$$.$RANDOM
q=$(printf '%q ' "$@")
rssh "touch $m; cd $(printf '%q' "$PWD") && ulimit -n 524288 2>/dev/null; export TMPDIR=/var/tmp/t/rrun; mkdir -p \$TMPDIR; bash -c $(printf '%q' "$q")"
rc=$?
rs_back "$m" $roots
rssh "rm -f $m" 2>/dev/null
exit $rc
