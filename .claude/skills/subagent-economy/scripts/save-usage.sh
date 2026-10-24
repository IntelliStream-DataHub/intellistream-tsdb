#!/bin/bash
# save-usage.sh: feed it the status line's JSON on stdin; it saves the rate limits for usage.sh. The status line is the
# only place Claude Code hands these numbers out, so add one line to your status-line script after it reads its input:
#   printf '%s' "$input" | bash ~/.claude/skills/subagent-economy/scripts/save-usage.sh
# Writes $SE_HOME/state/usage-<session>.json and usage.json (the latest of any session), and appends
# "epoch five week" to usage-history-<session>.log and usage-history.log when the numbers change. Silent, never fails.
. "$(dirname "$(readlink -f "$0")")/se-env.sh"
dir=$SE_HOME/state
mkdir -p "$dir" 2>/dev/null || exit 0
# The script comes on python's stdin, so the status line's JSON goes in through the environment.
SE_STATUS_JSON=$(cat) python3 -I - "$dir" <<'PY' 2>/dev/null
import json, os, re, sys, time
d = sys.argv[1]
try:
    j = json.loads(os.environ.get('SE_STATUS_JSON') or '{}')
except Exception:
    sys.exit()
rl = j.get('rate_limits')
if not rl:
    sys.exit()
sid = re.sub(r'[^A-Za-z0-9-]', '', str(j.get('session_id') or 'unknown'))[:64]
now = int(time.time())
doc = json.dumps({'ts': now, 'session_id': sid, 'rate_limits': rl})
for name in ('usage-%s.json' % sid, 'usage.json'):
    p = os.path.join(d, name)
    with open(p + '.tmp', 'w') as f:
        f.write(doc)
    os.replace(p + '.tmp', p)
pct = lambda k: (rl.get(k) or {}).get('used_percentage')
f, w = pct('five_hour'), pct('seven_day')
line = '%d %s %s\n' % (now, '-' if f is None else f, '-' if w is None else w)
for name in ('usage-history-%s.log' % sid, 'usage-history.log'):
    p = os.path.join(d, name)
    try:
        with open(p, 'rb') as h:
            h.seek(0, 2); h.seek(max(0, h.tell() - 200)); last = h.read().decode().splitlines()[-1:]
    except (OSError, IndexError):
        last = []
    if last and last[0].split()[1:] == line.split()[1:]:
        continue
    with open(p, 'a') as h:
        h.write(line)
PY
exit 0
