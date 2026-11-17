#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# rx.sh CMD...: run CMD on the build machine in this directory, copying nothing (read what a job left there: its build
# dirs, fuzz crashes, logs of a failed test). See se-remote.sh.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
. "$SE_BIN/se-remote.sh"
[ $# -gt 0 ] || { echo "usage: rx.sh CMD..." >&2; exit 2; }
[ -n "$SE_REMOTE" ] || { echo "rx: no build machine (SE_REMOTE in $SE_HOME/config.sh)" >&2; exit 2; }
rssh "cd $(printf '%q' "$PWD") && bash -c $(printf '%q' "$(printf '%q ' "$@")")"
