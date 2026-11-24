# SPDX-License-Identifier: AGPL-3.0-only
# se-remote.sh: the build machine (SE_REMOTE=user@host in config.sh) or a fleet of them (SE_REMOTES="user@hostA:3 user@hostB:8:nvme",
# see below); sourced by bgrun.sh, se-rjob.sh, rrun.sh, rx.sh.
# With SE_REMOTE (or SE_REMOTES) set, bgrun.sh runs every job THERE: the trees the job names (its working directory and the absolute
# paths in its command that lie under SE_REMOTE_PREFIXES) are copied to the SAME paths on the build machine, as the same
# user, without build dirs (build/, build-*/, target/: -march=native binaries of one CPU must never run on another); the
# job runs there; afterwards the files it wrote (outside build dirs) come back, never over a newer local file.
# Build dirs and binaries stay on the build machine (rx.sh reads them). A git worktree's .git file is pointed at a mirror
# of its repository's .git under SE_REMOTE_GIT, so git works there. SE_REMOTE_OWN names paths that are the build
# machine's own (its own checkouts): never copied, and a job whose working directory is one of them is refused.
# SE_REMOTE_SLOTS jobs run at once (the rest wait for a slot there; BGRUN_NOWAIT=1 skips the wait).
# A FLEET (SE_REMOTES="user@hostA:SLOTS[:tag...] ...", parsed by se-env.sh): every host has its own slots, runs dir, copies and
# SE_REMOTE_OWN_<user_hostA> (the host with every character outside A-Za-z0-9 turned into _; unset: SE_REMOTE_OWN). bgrun.sh
# picks the first host, in config order, that has a free slot (se_pick_host) and records it in runs/NAME/host; se-rjob.sh,
# rx.sh, await.sh and watchdog.sh use that host. BGRUN_TAG=nvme (a tag of the entry) or SE_HOST=user@hostA restricts the
# choice. No host with a free slot: the first fitting host's queue. The functions that talk to "the build machine" talk to
# $SE_REMOTE: se_use_host HOST points it (and SE_REMOTE_SLOTS) at one host of the fleet.
RK="${SE_REMOTE_KEY:+-i $SE_REMOTE_KEY} -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=6"
SE_REMOTE_PREFIXES=${SE_REMOTE_PREFIXES:-"$HOME/projects/git:1 /var/tmp:1 $SE_HOME/agents:2"}
SE_REMOTE_SKIP=${SE_REMOTE_SKIP:-"/var/tmp/t /var/tmp/j"}
SE_REMOTE_RUNS=${SE_REMOTE_RUNS:-/var/tmp/se-runs}
SE_REMOTE_GIT=${SE_REMOTE_GIT:-/var/tmp/se-git}
SE_REMOTE_SLOTS=${SE_REMOTE_SLOTS:-3}
SE_RJOB_POLL=${SE_RJOB_POLL:-15}   # seconds between se-rjob.sh's looks at its job (a test sets 1)
RS_EXCL=(--exclude=build/ --exclude='build-*/' --exclude=target/ --exclude=/.claude/worktrees/ --exclude=__pycache__/)
RS_PRUNE="\\( -name .git -o -name build -o -name 'build-*' -o -name target -o -name __pycache__ -o -path ./.claude/worktrees \\) -prune"
rssh() { ssh $RK "$SE_REMOTE" "$@"; }

# ---- the fleet (SE_FH SE_FS SE_FT from se-env.sh) ----
se_san() { printf '%s' "${1//[^A-Za-z0-9]/_}"; }
se_host_idx() { local i; for i in "${!SE_FH[@]}"; do [ "${SE_FH[i]}" = "$1" ] && { echo "$i"; return 0; }; done; return 1; }
# se_use_host HOST: HOST is "the build machine" from here on (a host that left the config is fine: rx.sh may still read it)
se_use_host() {
  local i
  [[ "$1" =~ ^[A-Za-z0-9_][A-Za-z0-9._@-]*$ ]] || { echo "${0##*/}: bad host '$1'" >&2; return 1; }
  SE_REMOTE=$1
  if i=$(se_host_idx "$1"); then SE_REMOTE_SLOTS=${SE_FS[i]}; fi
}
# se_own_of HOST: the paths that are HOST's own checkouts (SE_REMOTE_OWN_<host> when set, else SE_REMOTE_OWN)
se_own_of() { local v; v=SE_REMOTE_OWN_$(se_san "$1"); if [ -n "${!v+x}" ]; then printf '%s' "${!v}"; else printf '%s' "$SE_REMOTE_OWN"; fi; }
# se_probe_free HOST SLOTS: how many of its slots are free now (nothing when unreachable)
# A host that accepts the connection and then hangs (a stuck mount under the runs dir) must not stall a pick: timeout 15 = unreachable.
se_probe_free() {
  timeout "${SE_PROBE_TIMEOUT:-15}" ssh -o ConnectTimeout=5 $RK "$1" "n=0; mkdir -p '$SE_REMOTE_RUNS/.slots'; for i in \$(seq 1 $2); do (flock -n 9 || exit 1) 9>'$SE_REMOTE_RUNS/.slots/'\$i && n=\$((n+1)); done; echo \$n" 2>/dev/null
}
# se_claims HOST: jobs of this SE_HOME sent to HOST that hold no slot yet (a probe cannot see them: bgrun.sh picks again
# before the first has copied its trees), so two jobs started in a row do not both take the last free slot
se_claims() {
  local d n=0 now s p
  now=$(date +%s)
  for d in "${RUNS:-$SE_HOME/runs}"/*/; do
    [ "$(cat "$d/host" 2>/dev/null)" = "$1" ] && [ ! -e "$d/rc" ] && [ ! -e "$d/nowait" ] || continue
    s=$(cat "$d/start" 2>/dev/null) && [ $((now - s)) -lt 900 ] || continue
    # bgrun.sh writes the pid after it let go of the pick lock: a job with no pid yet is a claim for a minute (a killed bgrun
    # leaves none), one with a pid only while that lives
    p=$(cat "$d/pid" 2>/dev/null)
    if [ -n "$p" ]; then kill -0 "$p" 2>/dev/null || continue; else [ $((now - s)) -lt 60 ] || continue; fi
    grep -q '^== build machine slot' "$d/out" 2>/dev/null && continue
    n=$((n + 1))
  done
  echo $n
}
# se_cands [CWD]: the fleet indexes that fit BGRUN_TAG and SE_HOST (and, given CWD, whose own checkouts do not hold it)
se_cands() {
  local i h o
  for i in "${!SE_FH[@]}"; do
    h=${SE_FH[i]}
    [ -z "${SE_HOST:-}" ] || [ "$h" = "$SE_HOST" ] || continue
    [ -z "${BGRUN_TAG:-}" ] || [[ "${SE_FT[i]}" == *",$BGRUN_TAG,"* ]] || continue
    if [ -n "${1:-}" ]; then for o in $(se_own_of "$h"); do case "$1/" in "$o"/*) continue 2 ;; esac; done; fi
    echo "$i"
  done
}
# se_pick_host CWD: prints the host a job started in CWD goes to. rc 0: it has a free slot (or the job takes none: BGRUN_NOWAIT,
# or one fitting host only); 1: no fitting host has one, this is the first fitting host and the job waits in its queue; 2: none fits
se_pick_host() {
  local c i h f o
  c=($(se_cands "${1:-}"))
  if [ ${#c[@]} -eq 0 ] && [ -n "${1:-}" ] && [ -n "$(se_cands)" ]; then
    # hosts fit, but each owns the directory: say so as the one-host config always did
    for i in $(se_cands); do for o in $(se_own_of "${SE_FH[i]}"); do case "$1/" in "$o"/*)
      echo "${0##*/}: $1 is the build machine's own checkout ($o there): run from a worktree or a copy (BGRUN_LOCAL=1 runs here)" >&2; return 2 ;; esac; done; done
  fi
  [ ${#c[@]} -gt 0 ] || { echo "${0##*/}: no build host fits${BGRUN_TAG:+ tag $BGRUN_TAG}${SE_HOST:+ host $SE_HOST} for ${1:-here} (SE_REMOTES in config.sh)" >&2; return 2; }
  if [ -n "${BGRUN_NOWAIT:-}" ] || [ ${#c[@]} -eq 1 ]; then echo "${SE_FH[${c[0]}]}"; return 0; fi
  for i in "${c[@]}"; do
    h=${SE_FH[i]}
    f=$(se_probe_free "$h" "${SE_FS[i]}"); f=$(( ${f:-0} - $(se_claims "$h") ))
    [ "$f" -gt 0 ] && { echo "$h"; return 0; }
  done
  echo "${SE_FH[${c[0]}]}"; return 1
}
# se_free_total: the free slots of the fitting hosts together (for the local overflow rule of config.sh)
se_free_total() {
  local i f t=0
  for i in $(se_cands); do
    f=$(se_probe_free "${SE_FH[i]}" "${SE_FS[i]}"); f=$(( ${f:-0} - $(se_claims "${SE_FH[i]}") ))
    [ "$f" -gt 0 ] && t=$((t + f))
  done
  echo $t
}
# the host a directory's jobs last ran on, found again by rx.sh and rrun.sh (build dirs stay on that host)
se_cwd_note() { mkdir -p "$SE_HOME/state/cwd-host"; printf '%s\n' "$2" > "$SE_HOME/state/cwd-host/$(printf '%s' "$1" | md5sum | cut -c1-16)"; }
se_cwd_host() {
  local d=$1 h
  while [ -n "$d" ] && [ "$d" != / ]; do
    h=$(cat "$SE_HOME/state/cwd-host/$(printf '%s' "$d" | md5sum | cut -c1-16)" 2>/dev/null) && [ -n "$h" ] && { echo "$h"; return 0; }
    d=${d%/*}
  done
  return 1
}

rs_lock() { local l=$SE_HOME/state/rsync-locks; mkdir -p "$l"; echo "$l/$(printf '%s' "$1" | md5sum | cut -c1-16)"; }

# rs_roots_of STRING: the tree roots (PREFIX/one/.. levels) that STRING names
rs_roots_of() {
  local spec p n
  for spec in $SE_REMOTE_PREFIXES; do
    p=${spec%:*}; n=${spec##*:}
    [[ "$p" =~ ^/[A-Za-z0-9/._-]+$ && "$n" =~ ^[1-9]$ ]] || continue   # plain paths only: '.' is the one regex char left
    grep -oE "${p//./\\.}(/[A-Za-z0-9._+@-]+){$n}" <<<"$1"
  done
}

# rs_root_ok ROOT: ROOT is exactly PREFIX plus its levels, at least 3 levels deep, no . or .. (never /tmp, /var, /home...)
rs_root_ok() {
  local r=$1 spec p n rest
  [[ "$r" =~ ^(/[A-Za-z0-9._+@-]+){3,}$ && ! "$r" =~ /\.\.?(/|$) ]] || return 1
  for spec in $SE_REMOTE_PREFIXES; do
    p=${spec%:*}; n=${spec##*:}
    case "$r" in "$p"/*) rest=${r#"$p"/}; rest=${rest//[!\/]/}; [ ${#rest} -eq $((n - 1)) ] && return 0 ;; esac
  done
  return 1
}

# rs_job_roots CWD CMDSTRING: the roots to copy (existing dirs, minus SKIP and OWN); fails when CWD is in OWN
rs_job_roots() {
  local cwd=$1 s=$2 r x skip own
  for own in $(se_own_of "$SE_REMOTE"); do
    case "$cwd/" in "$own"/*) echo "${0##*/}: $cwd is the build machine's own checkout ($own there): run from a worktree or a copy (BGRUN_LOCAL=1 runs here)" >&2; return 1;; esac
  done
  { rs_roots_of "$cwd"; rs_roots_of "$s"; } | sort -u | while read -r r; do
    [ -d "$r" ] && [ ! -L "$r" ] || continue
    rs_root_ok "$r" || { echo "${0##*/}: refusing to copy $r (not a tree under SE_REMOTE_PREFIXES)" >&2; continue; }
    skip=; for x in $SE_REMOTE_SKIP $(se_own_of "$SE_REMOTE"); do [ "$r" = "$x" ] && skip=1; done
    [ -z "$skip" ] && echo "$r"
  done
  return 0
}

# rs_gitlink GITFILE: mirror the repository of a worktree's .git file and point the remote copy of the file at it
rs_gitlink() {
  local g=$1 gd common rel rc
  gd=$(sed -n 's/^gitdir: //p' "$g"); [ -n "$gd" ] || return 0
  case "$gd" in /*) ;; *) gd=$(cd "$(dirname "$g")" && readlink -f "$gd") ;; esac
  common=$(readlink -f "$gd/$(cat "$gd/commondir" 2>/dev/null || echo .)")
  [ -d "$common" ] || return 0
  rc=$SE_REMOTE_GIT/$(printf '%s' "$common" | tr / _)
  rel=${gd#"$common"}; rel=${rel#/}
  flock "$(rs_lock "$common")" rsync -aH --delete -e "ssh $RK" "$common/" "$SE_REMOTE:$rc/" || return 1
  rssh "printf 'gitdir: %s\n' '$rc${rel:+/$rel}' > '$g'"
}

# rs_push ROOT...: copy the trees to the same paths there (no build dirs; nothing there is deleted)
rs_push() {
  local r g ex
  [ $# -gt 0 ] || return 0
  rssh "mkdir -p $(printf "'%s' " "$@") '$SE_REMOTE_RUNS' '$SE_REMOTE_GIT'" || return 1
  for r; do
    rs_root_ok "$r" || { echo "rs_push: refusing $r" >&2; return 1; }
    ex=(); while read -r g; do ex+=(--exclude="/${g#"$r"/}"); done < <(find "$r" -maxdepth 3 -name .git -type f)
    flock "$(rs_lock "$r")" rsync -aH -e "ssh $RK" "${RS_EXCL[@]}" "${ex[@]}" "$r/" "$SE_REMOTE:$r/" || return 1
    while read -r g; do rs_gitlink "$g" || return 1; done < <(find "$r" -maxdepth 3 -name .git -type f)
  done
}

# rs_back MARKER ROOT...: bring back the files the job wrote there (newer than MARKER, outside build dirs and .git,
# under 256 MB), never over a newer local file
rs_back() {
  local m=$1 r t; shift
  t=$(mktemp)
  for r; do
    case "$r" in /var/tmp/t/?*) ;; *) rs_root_ok "$r" || continue ;; esac
    rssh "cd '$r' 2>/dev/null && find . $RS_PRUNE -o -type f -newer '$m' -size -256M -print" > "$t" 2>/dev/null || continue
    [ -s "$t" ] || continue
    mkdir -p "$r"
    flock "$(rs_lock "$r")" rsync -a --update --files-from="$t" -e "ssh $RK" "$SE_REMOTE:$r/" "$r/" || echo "rs_back: copying $r back failed" >&2
  done
  rm -f "$t"
}
