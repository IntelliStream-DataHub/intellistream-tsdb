# SPDX-License-Identifier: AGPL-3.0-only
# se-env.sh: shared settings of the subagent-economy scripts; sourced by them, not run.
# SE_HOME holds runs/, state/ and the hold files. It is outside ~/.claude on purpose: subagents may be refused writes there.
# $SE_HOME/config.sh, if it exists, sets this machine's choices (SE_QUEUE, the fleet's RB_*, USAGE_SESSION);
# see config.example.sh next to this file.
SE_BIN=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
SE_HOME=${SE_HOME:-${XDG_STATE_HOME:-$HOME/.local/state}/subagent-economy}
[ -f "$SE_HOME/config.sh" ] && . "$SE_HOME/config.sh"
RUNS=${RUNS:-$SE_HOME/runs}
# A job name becomes a directory under $RUNS that is removed and re-created, so it must not reach outside it.
se_name_ok() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "${0##*/}: bad job name '$1' (letters, digits, . _ - only)" >&2; exit "${2:-2}"; }; }
