# SPDX-License-Identifier: AGPL-3.0-only
# se-env.sh: shared settings of the subagent-economy scripts; sourced by them, not run.
# SE_HOME holds runs/, state/ and the hold files. It is outside ~/.claude on purpose: subagents may be refused writes there.
# $SE_HOME/config.sh, if it exists, sets this machine's choices (SE_QUEUE, the fleet's RB_*, USAGE_SESSION);
# see config.example.sh next to this file.
SE_BIN=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
SE_HOME=${SE_HOME:-${XDG_STATE_HOME:-$HOME/.local/state}/subagent-economy}

# The build fleet (se-remote.sh): SE_REMOTES="user@hostA:SLOTS[:TAG[:TAG]] user@hostB:SLOTS ..." (config order is the order
# bgrun.sh tries them; SLOTS empty = SE_REMOTE_SLOTS, 3), or the one host SE_REMOTE=user@host with SE_REMOTE_SLOTS (unchanged).
# SE_FH, SE_FS, SE_FT: host, slots and ",tag,tag," of each, same index. SE_REMOTES wins when both are set, and SE_REMOTE
# then names its first host (the host the one-host code paths use). A function: a config.sh may call it again.
se_fleet_parse() {
  SE_FH=() SE_FS=() SE_FT=()
  local e h s t
  if [ -n "${SE_REMOTES:-}" ]; then
    for e in $SE_REMOTES; do
      h= s= t=
      IFS=: read -r h s t <<< "$e"
      [[ "$h" =~ ^[A-Za-z0-9_][A-Za-z0-9._@-]*$ && "${s:-1}" =~ ^[1-9][0-9]{0,2}$ && "$t" =~ ^([A-Za-z0-9_.-]+(:[A-Za-z0-9_.-]+)*)?$ ]] ||
        { echo "${0##*/}: ignoring the bad SE_REMOTES entry '$e' (want user@host:SLOTS[:tag...], SLOTS 1-999; a port is not part of it)" >&2; continue; }
      SE_FH+=("$h"); SE_FS+=("${s:-${SE_REMOTE_SLOTS:-3}}"); SE_FT+=(",${t//:/,},")
    done
    [ ${#SE_FH[@]} -gt 0 ] && SE_REMOTE=${SE_FH[0]}
  elif [ -n "${SE_REMOTE:-}" ]; then
    SE_FH=("$SE_REMOTE"); SE_FS=("${SE_REMOTE_SLOTS:-3}"); SE_FT=(",")
  fi
}
se_remote_on() { [ ${#SE_FH[@]} -gt 0 ]; }
[ -f "$SE_HOME/config.sh" ] && . "$SE_HOME/config.sh"
se_fleet_parse
RUNS=${RUNS:-$SE_HOME/runs}
# A job name becomes a directory under $RUNS that is removed and re-created, so it must not reach outside it.
se_name_ok() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "${0##*/}: bad job name '$1' (letters, digits, . _ - only)" >&2; exit "${2:-2}"; }; }
