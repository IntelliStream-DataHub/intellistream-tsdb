#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# PreToolUse hook for Bash: refuses commands this project has been burnt by.
#
#   pkill -f, killall   pkill -f matches its own shell and the agent's; a test once
#                       killed the whole session this way (AGENTS.md).
#   kill ... -1 / 0     a pid of -1 signals every process of the user (the terminal
#                       included), 0 the whole process group.
#   docker              this project uses Podman only (project rule).
#
# Only command positions count (line start, after ; & | ( or sudo/exec), so text that
# merely mentions these words, in an echo or a commit message, passes.
set -u
cmd=$(jq -r '.tool_input.command // empty')
[ -z "$cmd" ] && exit 0

deny() {
    jq -n --arg r "$1" '{hookSpecificOutput: {hookEventName: "PreToolUse",
        permissionDecision: "deny", permissionDecisionReason: $r}}'
    exit 0
}

pos='(^|[;&|(]|&&|\|\|)[[:space:]]*((sudo|exec|nohup|timeout[[:space:]]+[0-9]+[a-z]?)[[:space:]]+)*'

while IFS= read -r line; do
    if printf '%s\n' "$line" | grep -Eq "${pos}pkill([[:space:]][^;&|]*)?[[:space:]]-[a-zA-Z]*f"; then
        deny "pkill -f matches its own shell (and can kill the session); stop processes you started by pid (AGENTS.md)."
    fi
    # the same match by another route: kill $(pgrep -f ...), kill `pgrep -f ...`, pgrep -f ... | xargs kill
    if printf '%s\n' "$line" | grep -Eq "${pos}kill[^;&|]*(\\\$\(|\`)[[:space:]]*pgrep([[:space:]][^;&|)]*)?[[:space:]]-[a-zA-Z]*f" ||
        printf '%s\n' "$line" | grep -Eq "${pos}pgrep([[:space:]][^;&|]*)?[[:space:]]-[a-zA-Z]*f[^;&]*\|[[:space:]]*xargs[[:space:]]+([^;&|]*[[:space:]])?kill"; then
        deny "pgrep -f matches its own shell, so killing what it prints can kill the session; stop processes you started by pid (AGENTS.md)."
    fi
    if printf '%s\n' "$line" | grep -Eq "${pos}killall([[:space:]]|$)"; then
        deny "killall is not allowed here; stop only the processes you started, by pid (AGENTS.md)."
    fi
    # kill whose last argument is -1 or 0: every process of the user, or the process group
    if printf '%s\n' "$line" | grep -Eq "${pos}kill([[:space:]]+[^;&|[:space:]]+)*[[:space:]]+(--[[:space:]]+)?(-1|0)[[:space:]]*($|[;&|])"; then
        deny "kill with pid -1 or 0 reaches every process of the user or the whole process group; signal a specific pid (AGENTS.md)."
    fi
    if printf '%s\n' "$line" | grep -Eq "${pos}(docker|docker-compose)([[:space:]]|$)"; then
        deny "This project uses Podman, never Docker: use podman / podman-compose."
    fi
done <<< "$cmd"
exit 0
