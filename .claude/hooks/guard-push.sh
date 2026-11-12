#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# PreToolUse hook for Bash: refuses a git push while any local commit that is on no
# remote yet carries a link to a Claude session (project rule: no session links
# in anything that lands in a repository). Checks every local branch, so pushes
# from worktrees (git -C ../tsdb-x push) are covered too.
set -u
cmd=$(jq -r '.tool_input.command // empty')
printf '%s\n' "$cmd" | grep -Eq '(^|[;&|(]|&&)[[:space:]]*git([[:space:]]+-C[[:space:]]+[^[:space:]]+)?[[:space:]]+push([[:space:]]|$)' || exit 0

dir=${CLAUDE_PROJECT_DIR:-$PWD}
bad=$(git -C "$dir" log --branches --not --remotes --format='%h %s%n%b' 2>/dev/null |
      grep -E -i 'claude\.ai/code|Claude-Session:' | head -3)
[ -z "$bad" ] && exit 0
jq -n --arg r "Unpushed commits contain a Claude session link (remove it before pushing): $bad" \
    '{hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: $r}}'
exit 0
