#!/usr/bin/env python3
"""tokenstat.py DIR...: token economics of workflow subagents. DIR = a workflow transcript dir (wf_*) or the
`subagents/workflows` dir holding several. One API request is counted once (a transcript writes one record per
content block, all with the same usage)."""
import json, glob, os, sys, re, datetime, collections
def ts(s): return datetime.datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp()
files = []
for d in sys.argv[1:] or ['.']:
    files += glob.glob(os.path.join(d, 'agent-*.jsonl')) + glob.glob(os.path.join(d, 'wf_*', 'agent-*.jsonl'))
T = collections.Counter(); per = []; gapb = collections.defaultdict(lambda: [0, 0, 0]); res = collections.Counter(); tot_res = 0
for f in files:
    rows = []
    for l in open(f):
        try: rows.append(json.loads(l))
        except Exception: pass
    reqs = {}; order = []
    for i, r in enumerate(rows):
        if r.get('type') == 'assistant' and r['message'].get('usage'):
            rid = r.get('requestId') or r['uuid']
            if rid not in reqs: reqs[rid] = {'i': i, 'u': r['message']['usage'], 'ts': r.get('timestamp')}; order.append(rid)
    n = len(order); ctxs = []; prev = None
    for rid in order:
        q = reqs[rid]; u = q['u']; c = u.get('cache_creation_input_tokens', 0)
        T['read'] += u.get('cache_read_input_tokens', 0); T['create'] += c; T['req'] += 1
        ctxs.append(u.get('cache_read_input_tokens', 0) + c + u.get('input_tokens', 0))
        g = (ts(q['ts']) - prev) if (prev and q['ts']) else 0; prev = ts(q['ts']) if q['ts'] else prev
        b = '<1 min' if g < 60 else '1-4 min' if g < 240 else '4-5 min' if g < 300 else '5-30 min' if g < 1800 else '>30 min'
        gapb[b][0] += 1
        if c > 100000: gapb[b][1] += 1; gapb[b][2] += c
    if ctxs: per.append((n, max(ctxs), ctxs[0]))
    rk = {reqs[r]['i']: k for k, r in enumerate(order)}; cur = 0; calls = {}
    for i, r in enumerate(rows):
        if i in rk: cur = rk[i]
        if r.get('type') == 'assistant':
            for c in r['message'].get('content', []):
                if c.get('type') == 'tool_use': calls[c['id']] = (c['name'], c['input'], cur)
        elif r.get('type') == 'user' and isinstance(r['message'].get('content'), list):
            for b in r['message']['content']:
                if b.get('type') == 'tool_result' and b.get('tool_use_id') in calls:
                    nm, inp, t = calls[b['tool_use_id']]; tx = b.get('content')
                    if isinstance(tx, list): tx = ''.join(x.get('text', '') for x in tx if isinstance(x, dict))
                    v = len(tx or '') / 3.2 * (n - t); tot_res += v; cmd = inp.get('command', '') if nm in ('Bash', 'bash') else ''
                    k = ('Read tool' if nm == 'Read' else 'waitrun/await output' if re.search(r'waitrun|quietrun|await\.sh', cmd)
                         else 'git diff/show/log' if re.search(r'git (diff|show|log)', cmd)
                         else 'cat/sed/head/tail of files' if re.search(r'sed -n|\bcat\b|\bhead\b|\btail\b', cmd)
                         else 'grep/find' if re.search(r'grep|rg |find ', cmd) else 'other')
                    res[k] += v
if not per: sys.exit('no agent transcripts found')
w = T['create'] * 1.25 + T['read'] * 0.1
per.sort()
print('agents %d, API requests %d, median requests/agent %d, max %d' % (len(per), T['req'], per[len(per) // 2][0], max(p[0] for p in per)))
print('median start context %d, median peak context %d' % (sorted(p[2] for p in per)[len(per) // 2], sorted(p[1] for p in per)[len(per) // 2]))
print('cache read %.0fM  cache write %.0fM  relative cost %.0fM (writes %.0f%%)' % (T['read'] / 1e6, T['create'] / 1e6, w / 1e6, 100 * T['create'] * 1.25 / w))
for b in ['<1 min', '1-4 min', '4-5 min', '5-30 min', '>30 min']:
    x = gapb[b]; print('  gap before request %-8s %5d requests, %4d full re-writes (%5.0fM tokens)' % (b, x[0], x[1], x[2] / 1e6))
print('tool-result residency %.0fM token-reads' % (tot_res / 1e6))
for k, v in res.most_common(): print('  %-26s %6.0fM %4.1f%%' % (k, v / 1e6, 100 * v / max(tot_res, 1)))
