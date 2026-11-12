#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# usage.sh [--gate PCT | --wgate PCT | --rate [HOURS]]: the Claude Code usage of the 5-hour window and of the week, as
# the status line last saw them (save-usage.sh, called from the status-line script, writes $SE_HOME/state/usage*.json).
#   no option: prints "five=N week=M age=Ss resets=..." ("-" when unknown), exit 0.
#   --gate PCT: exit 0 when the 5-hour usage is known and >= PCT (hold back), exit 1 otherwise (go on). A stale reading
#   counts until its window's reset time (usage only grows inside a window); after the reset it is unknown and holds nothing back.
#   --wgate PCT: the same test on the week's usage.
#   --rate [HOURS=3]: the week's burn rate over the last HOURS of history and when it would reach 100%.
# $SE_HOME/HOLD_USAGE (a file the orchestrator creates) forces --gate to hold whatever the numbers say.
# USAGE_SESSION picks one session's reading (the one whose account the agents spend); unset = the latest of any session.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
U=$SE_HOME/state/usage${USAGE_SESSION:+-$USAGE_SESSION}.json
L=$SE_HOME/state/usage-history${USAGE_SESSION:+-$USAGE_SESSION}.log
H=$SE_HOME/HOLD_USAGE
read -r five week age resets < <(python3 -I - "$U" <<'PY' 2>/dev/null
import json, sys, time
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    print("- - - -"); sys.exit()
rl = d.get("rate_limits") or {}
f = (rl.get("five_hour") or {}).get("used_percentage")
w = (rl.get("seven_day") or {}).get("used_percentage")
r = (rl.get("five_hour") or {}).get("resets_at") or "-"
wr = (rl.get("seven_day") or {}).get("resets_at") or 0
now = time.time()
# Usage only grows inside a window, so a stale reading is a LOWER bound until that window resets:
# after its reset it says nothing (the window started again), and is reported unknown.
if f is not None and r != "-" and now >= float(r): f = None
if w is not None and wr and now >= float(wr): w = None
print("%s %s %d %s" % ("-" if f is None else round(f), "-" if w is None else round(w), now - d.get("ts", 0), r))
PY
)
case "$1" in
--gate)
        [ -e "$H" ] && exit 0
        { [ "$five" = "-" ] || [ -z "$five" ]; } && exit 1
        [ "$five" -ge "${2:-50}" ] && exit 0
        exit 1 ;;
--wgate)
        { [ "$week" = "-" ] || [ -z "$week" ]; } && exit 1
        [ "$week" -ge "${2:-95}" ] && exit 0
        exit 1 ;;
--rate)
        python3 -I - "$L" "${2:-3}" <<'PY'
import sys, time
rows = []
try:
    for l in open(sys.argv[1]):
        p = l.split()
        if len(p) == 3 and p[2] not in ("", "-"):
            rows.append((int(p[0]), float(p[2])))
except FileNotFoundError:
    pass
now = time.time(); win = float(sys.argv[2]) * 3600
r = [x for x in rows if x[0] >= now - win]
if len(r) < 2 or r[-1][0] - r[0][0] < 900:
    print("rate: not enough history yet (%d points over %.0f min; needs 15 min)" % (len(r), (r[-1][0] - r[0][0]) / 60 if len(r) > 1 else 0)); sys.exit()
rate = (r[-1][1] - r[0][1]) / ((r[-1][0] - r[0][0]) / 3600)
w = r[-1][1]
eta = (100 - w) / rate if rate > 0 else float("inf")
print("week %.0f%%, burning %.2f%%/h over the last %.1f h -> 100%% in %.1f h" % (w, rate, (r[-1][0] - r[0][0]) / 3600, eta))
PY
        exit 0 ;;
esac
echo "five=$five week=$week age=${age}s resets=$resets"
