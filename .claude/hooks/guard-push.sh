#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# PreToolUse hook for Bash: refuses a git push while any local commit that is on no
# remote yet carries a link to a Claude session (project rule: no session links
# in anything that lands in a repository). Checks every local branch, so pushes
# from worktrees (git -C ../tsdb-x push) are covered too.
# It also refuses the push when such a commit's message or diff matches a line of a private pattern file kept
# OUTSIDE the repository ($CLAUDE_PRIVATE_PATTERNS, default ~/.config/claude-private-patterns: one extended regular
# expression per line, e.g. the addresses and host names of internal machines). The patterns never enter the
# repository; without the file this check does nothing. The repository checked is the project's and, when the
# command says `git -C DIR push`, DIR's (another repository pushed from this session).
set -u
cmd=$(jq -r '.tool_input.command // empty')
printf '%s\n' "$cmd" | grep -Eq '(^|[;&|(]|&&)[[:space:]]*git([[:space:]]+-C[[:space:]]+[^[:space:]]+)?[[:space:]]+push([[:space:]]|$)' || exit 0

deny() {
    jq -n --arg r "$1" '{hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: $r}}'
    exit 0
}
dir=${CLAUDE_PROJECT_DIR:-$PWD}
bad=$(git -C "$dir" log --branches --not --remotes --format='%h %s%n%b' 2>/dev/null |
      grep -E -i 'claude\.ai/code|Claude-Session:' | head -3)
[ -n "$bad" ] && deny "Unpushed commits contain a Claude session link (remove it before pushing): $bad"

pat=${CLAUDE_PRIVATE_PATTERNS:-$HOME/.config/claude-private-patterns}
[ -s "$pat" ] || exit 0
dirs=("$dir")
cdir=$(printf '%s\n' "$cmd" | sed -n -E 's/.*git[[:space:]]+-C[[:space:]]+([^[:space:]]+)[[:space:]]+push.*/\1/p' | head -1)
[ -n "$cdir" ] && dirs+=("$cdir")
for d in "${dirs[@]}"; do
    hit=$(git -C "$d" log -p --branches --not --remotes --format='commit %h %s%n%b' 2>/dev/null |
          grep -E -i -n -f <(grep -v -E '^[[:space:]]*(#|$)' "$pat") | head -3 | cut -c1-160)
    # name the commit, never the matched text: the reason is shown and logged
    [ -n "$hit" ] && deny "Unpushed commits in $d match a pattern of $pat (private information, e.g. an internal host): find them with git log -p --branches --not --remotes and grep -E -f $pat, remove it, then push. $(printf '%s\n' "$hit" | wc -l) matching line(s)."
done
exit 0
