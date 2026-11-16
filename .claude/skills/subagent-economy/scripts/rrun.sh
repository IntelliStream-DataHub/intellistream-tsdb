#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# rrun.sh CMD...: run CMD on the build machine NOW, in this directory, with its output here (a build, one short test:
# under ~4 minutes; longer runs go through bgrun.sh). The trees are copied there first and what CMD wrote comes back
# (se-remote.sh); build dirs stay there. Without SE_REMOTE it runs CMD here.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
. "$SE_BIN/se-remote.sh"
[ $# -gt 0 ] || { echo "usage: rrun.sh CMD..." >&2; exit 2; }
[ -n "$SE_REMOTE" ] || exec "$@"
roots=$(rs_job_roots "$PWD" "$*") || exit 2
rs_push $roots || { echo "rrun: copying the trees to the build machine failed" >&2; exit 98; }
m=$SE_REMOTE_RUNS/rrun.$$.$RANDOM
q=$(printf '%q ' "$@")
rssh "touch $m; cd $(printf '%q' "$PWD") && ulimit -n 524288 2>/dev/null; export TMPDIR=/var/tmp/t/rrun; mkdir -p \$TMPDIR; bash -c $(printf '%q' "$q")"
rc=$?
rs_back "$m" $roots
rssh "rm -f $m" 2>/dev/null
exit $rc
