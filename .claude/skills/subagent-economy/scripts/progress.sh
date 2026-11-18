#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# progress.sh [AGENTS_DIR=$SE_HOME/agents/tsdb] [HOURS=24]: one line per track whose NOTES.md changed in the last HOURS
# hours, newest first: the track, minutes since its notes changed, and the last non-empty line of the notes (what the
# track's agent wrote last). For the hourly check-in (.claude/agents/job-watchdog.md); it only reads files.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
A=${1:-$SE_HOME/agents/tsdb}; H=${2:-24}; now=$(date +%s)
for f in $(find "$A" -mindepth 2 -maxdepth 2 -name NOTES.md -mmin -$((H * 60)) -printf '%T@ %p\n' | sort -rn | cut -d' ' -f2); do
    k=$(basename "$(dirname "$f")"); m=$(( (now - $(stat -c %Y "$f")) / 60 ))
    printf '%-16s %4dm  %s\n' "$k" "$m" "$(grep -v '^[[:space:]]*$' "$f" | tail -1 | cut -c1-110)"
done
