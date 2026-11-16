# SPDX-License-Identifier: AGPL-3.0-only
# se-remote.sh: the build machine (SE_REMOTE=user@host in config.sh); sourced by bgrun.sh, se-rjob.sh, rrun.sh, rx.sh.
# With SE_REMOTE set, bgrun.sh runs every job THERE: the trees the job names (its working directory and the absolute
# paths in its command that lie under SE_REMOTE_PREFIXES) are copied to the SAME paths on the build machine, as the same
# user, without build dirs (build/, build-*/, target/: -march=native binaries of one CPU must never run on another); the
# job runs there; afterwards the files it wrote (outside build dirs) come back, never over a newer local file.
# Build dirs and binaries stay on the build machine (rx.sh reads them). A git worktree's .git file is pointed at a mirror
# of its repository's .git under SE_REMOTE_GIT, so git works there. SE_REMOTE_OWN names paths that are the build
# machine's own (its own checkouts): never copied, and a job whose working directory is one of them is refused.
# SE_REMOTE_SLOTS jobs run at once (the rest wait for a slot there; BGRUN_NOWAIT=1 skips the wait).
RK="${SE_REMOTE_KEY:+-i $SE_REMOTE_KEY} -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=6"
SE_REMOTE_PREFIXES=${SE_REMOTE_PREFIXES:-"$HOME/projects/git:1 /var/tmp:1 $SE_HOME/agents:2"}
SE_REMOTE_SKIP=${SE_REMOTE_SKIP:-"/var/tmp/t /var/tmp/j"}
SE_REMOTE_RUNS=${SE_REMOTE_RUNS:-/var/tmp/se-runs}
SE_REMOTE_GIT=${SE_REMOTE_GIT:-/var/tmp/se-git}
SE_REMOTE_SLOTS=${SE_REMOTE_SLOTS:-3}
RS_EXCL=(--exclude=build/ --exclude='build-*/' --exclude=target/ --exclude=/.claude/worktrees/ --exclude=__pycache__/)
RS_PRUNE="\\( -name .git -o -name build -o -name 'build-*' -o -name target -o -name __pycache__ -o -path ./.claude/worktrees \\) -prune"
rssh() { ssh $RK "$SE_REMOTE" "$@"; }
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
  for own in $SE_REMOTE_OWN; do
    case "$cwd/" in "$own"/*) echo "${0##*/}: $cwd is the build machine's own checkout ($own there): run from a worktree or a copy (BGRUN_LOCAL=1 runs here)" >&2; return 1;; esac
  done
  { rs_roots_of "$cwd"; rs_roots_of "$s"; } | sort -u | while read -r r; do
    [ -d "$r" ] && [ ! -L "$r" ] || continue
    rs_root_ok "$r" || { echo "${0##*/}: refusing to copy $r (not a tree under SE_REMOTE_PREFIXES)" >&2; continue; }
    skip=; for x in $SE_REMOTE_SKIP $SE_REMOTE_OWN; do [ "$r" = "$x" ] && skip=1; done
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
