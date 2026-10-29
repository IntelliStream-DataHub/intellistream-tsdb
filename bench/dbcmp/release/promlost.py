# promlost.py: when check.py finds Prometheus' remote-write answers short, find exactly which acknowledged samples it
# lost, and whether that explains every difference.
#   PROM_PORT=9090 DATA=mixed.rowbinary python3 -I promlost.py CHECK_RW_TXT
# Prometheus v3.15 accepted (2xx) and answered every sample right after the load, yet after its head and
# out-of-order compactions a few were gone for good in some runs: the first sample of series whose clock jitter puts
# it before 2024-01-01 00:00 (smoke, series 2 at -45 ms and series 3 at -21 ms; not in every run). That is a result
# to report, not a harness fault, but only if nothing else differs, so this script checks it exactly:
# - per series, the whole-range count, sum, min and max from Prometheus against the rows of DATA (the file the loader
#   sent) minus the lost samples: counts and min/max exact (1e-7, as check.py), sums within 1e-9 relative;
# - the lost samples are located by bisection with count_over_time over halves of the series' rows (Prometheus 3
#   range selectors are left-open: [a, b) in ms is the range b - a at time b - 1, as mkq.py queries); a range with
#   more samples than the rows is a failure (an extra or moved sample);
# - every MISMATCH line in CHECK_RW_TXT must be a whole-range line ("MISMATCH full N S") of a series that lost
#   samples; a random-window mismatch fails (rare: the lost samples are at the start of the data).
# Prints one LOST line per sample and a summary; exit 0 when the lost samples explain every difference, 1 otherwise.
import json, os, re, sys, urllib.parse, urllib.request

import numpy as np

PORT = int(os.environ.get("PROM_PORT", "9090"))
LO, HI = 1703980800000, 1767225600000  # mkq.py PROM_LO, PROM_HI: the whole-range queries

def query(q, at_ms):
    url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode({"query": q, "time": f"{at_ms / 1000:.3f}"})
    with urllib.request.urlopen(url, timeout=600) as r:
        d = json.load(r)
    if d.get("status") != "success": raise RuntimeError(str(d)[:300])
    return {int(x["metric"].get("series", -1)): float(x["value"][1]) for x in d["data"]["result"]}

def count(s, a, b):  # samples of series s with a <= t < b
    if b <= a: return 0
    return int(query(f'count_over_time(sensors{{series="{s}"}}[{b - a}ms])', b - 1).get(s, 0))

def close(a, b, rel):
    return a == b or abs(a - b) <= rel * max(abs(a), abs(b), 1e-300)

def main():
    if len(sys.argv) != 2: raise SystemExit("usage: PROM_PORT=P DATA=FILE promlost.py CHECK_RW_TXT")
    rec = np.memmap(os.environ["DATA"], dtype=np.dtype([("s", "<u4"), ("t", "<i8"), ("v", "<f4")]), mode="r")
    s_all, t_all = np.asarray(rec["s"]), np.asarray(rec["t"])
    v_all = np.asarray(rec["v"]).astype(np.float64)
    keep = np.isfinite(v_all) & (t_all >= LO) & (t_all < HI)
    s_all, t_all, v_all = s_all[keep], t_all[keep], v_all[keep]
    order = np.lexsort((t_all, s_all))
    s_all, t_all, v_all = s_all[order], t_all[order], v_all[order]
    if np.any((s_all[1:] == s_all[:-1]) & (t_all[1:] == t_all[:-1])):
        print("promlost: DATA has duplicate timestamps; not handled"); return 1
    bounds = np.searchsorted(s_all, np.arange(int(s_all.max()) + 2))
    rng = f"[{HI - LO}ms]"
    got = {fn: query(f"{fn}_over_time(sensors{rng})", HI - 1) for fn in ("count", "sum", "min", "max")}
    bad, lost = [], {}
    for s in range(len(bounds) - 1):
        T, V = t_all[bounds[s]:bounds[s + 1]], v_all[bounds[s]:bounds[s + 1]]
        if len(T) == 0: continue
        g = got["count"].get(s, 0)
        if g > len(T): bad.append(f"series {s}: {int(g)} samples, the rows have {len(T)}"); continue
        miss = []
        def find(i, j):  # rows i..j-1 of the series; [T[i], T[j]) in time
            if len(miss) > 1000: return
            a, b = int(T[i]), (int(T[j]) if j < len(T) else HI)
            c = count(s, a, b)
            if c > j - i: bad.append(f"series {s}: {c} samples in [{a}, {b}), the rows have {j - i}"); return
            if c == j - i: return
            if j - i == 1: miss.append(i); return
            m = (i + j) // 2
            find(i, m); find(m, j)
        if g < len(T): find(0, len(T))
        if len(miss) != len(T) - g:
            bad.append(f"series {s}: {len(T) - int(g)} samples short, {len(miss)} located"); continue
        k = np.ones(len(T), bool); k[miss] = False
        R = V[k]
        e = (len(R), float(R.sum()) if len(R) else 0.0, float(R.min()) if len(R) else None, float(R.max()) if len(R) else None)
        gs = (g, got["sum"].get(s), got["min"].get(s), got["max"].get(s))
        if not (gs[0] == e[0] and gs[1] is not None and close(gs[1], e[1], 1e-9) and close(gs[2], e[2], 1e-7) and close(gs[3], e[3], 1e-7)):
            bad.append(f"series {s}: got (count, sum, min, max) {gs}, the rows without the lost samples {e}")
        if miss:
            lost[s] = miss
            for i in miss: print(f"LOST series {s} t {int(T[i])} value {float(V[i])!r} (row {i} of {len(T)})")
    for l in open(sys.argv[1]):
        if not l.startswith("MISMATCH"): continue
        m = re.match(r"MISMATCH full \d+ (\d+) ", l)
        if not m or int(m.group(1)) not in lost: bad.append("not explained by lost samples: " + l.strip()[:200])
    for b in bad: print("PROBLEM", b)
    n = sum(len(x) for x in lost.values())
    print(f"promlost: {n} acknowledged samples lost in {len(lost)} series ({', '.join(map(str, sorted(lost)))}); "
          + ("every other whole-range answer exact" if not bad else f"{len(bad)} differences not explained by them"))
    return 1 if bad or not lost else 0

if __name__ == "__main__":
    sys.exit(main())
