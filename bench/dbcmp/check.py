# Correctness: runs the first N queries of each class on SYS (the benchmark's query text from q/,
# only the output format may differ) and the whole-range queries, and compares them with tsdb's
# answers in ref/ (tsdbq dump / tsdbq full ... dump). Run in $WORK:
#   python3 -I check.py SYS [N]          FULLS="0 3600" limits the whole-range queries ("" skips them)
# REF_WINDOWS (ref/windows.jsonl) names other reference answers, e.g. all of them: tsdbq dump spec.tsv 10000.
#
# Raw windows compare row count, first and last timestamp and the sum of the values; buckets compare
# start, count, sum, min and max; point lookups the value. Whole-range queries are reduced per series
# to (count, sum, sum of mins, sum of maxes, buckets). Sums are compared in double precision, which
# is why the SQL casts float32 columns to double before summing (sum(real) and QuestDB's sum(float)
# return float32). Servers are on 127.0.0.1 with the ports and credentials of README.md.
#
# Embedded engines answer through embq, which writes the same reduced answers as tsdbq; those
# files are compared directly, every query of the dump:
#   python3 -I check.py --dumps REF_WINDOWS REF_FULL GOT_WINDOWS GOT_FULL
import datetime, json, os, struct, sys, urllib.error, urllib.request, urllib.parse
import http.client as httpclient

CH_HTTP = os.environ.get("CH_HTTP_PORT", "8123")
TS_PORT = os.environ.get("TS_PORT", "5434")
QDB_HTTP = os.environ.get("QDB_HTTP_PORT", "9100")
I2_PORT = os.environ.get("I2_PORT", "8086")
I3_PORT = os.environ.get("I3_PORT", "8181")
PROM_PORT = os.environ.get("PROM_PORT", "9090")
IOT_REST = os.environ.get("IOT_REST_PORT", "18080")
TD_REST = os.environ.get("TD_REST_PORT", "6041")
NSERIES = int(os.environ.get("NSERIES", "100"))  # series in the file, as for spec.py

def f32(x):  # values are float32, and some servers print the shortest float32 text ("30.8")
    return struct.unpack('<f', struct.pack('<f', x))[0]

sysname = sys.argv[1] if len(sys.argv) > 1 else ""
per = int(sys.argv[2]) if len(sys.argv) > 2 and sysname not in ("--dumps", "--batch") else 200

def ms_of(t):  # '2024-01-01 00:00:00.123' or RFC 3339, UTC
    t = t.strip().replace("T", " ").rstrip("Z").replace("+00:00", "").replace("+00", "")
    if "." in t:
        base, frac = t.split(".")
        frac = (frac + "000000")[:6]
    else:
        base, frac = t, "000000"
    d = datetime.datetime.strptime(base, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.UTC)
    return int(d.timestamp()) * 1000 + int(frac) // 1000

def http(url, data=None, headers={}):
    req = urllib.request.Request(url, data=data.encode() if isinstance(data, str) else data, headers=headers)
    with urllib.request.urlopen(req, timeout=600) as r:
        return r.read().decode()

# ---- runners: rows(cls, query) -> raw/point: [(ts_ms, v)], agg: [(b_ms, c, s, mn, mx)];
#               digest(n, query) -> {series: (c, s, mn, mx, nbuckets)}
class CH:
    def __init__(self, table): self.table = table
    def q(self, sql):
        out = http(f"http://127.0.0.1:{CH_HTTP}/", sql)
        return [l.split("\t") for l in out.splitlines() if l]
    def rows(self, cls, q):
        r = self.q(q + " FORMAT TSV")
        if cls in ("raw", "point"):
            return [(ms_of(x[0]), float(x[1])) for x in r]
        return [(int(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in r]
    def digest(self, n, q):
        inner = q.rsplit(" ORDER BY ", 1)[0]
        r = self.q(f"SELECT series, sum(c), sum(s), sum(mn), sum(mx), count() FROM ({inner}) GROUP BY series ORDER BY series FORMAT TSV")
        return {int(x[0]): tuple(float(v) for v in x[1:]) for x in r}

class PG:
    def __init__(self, port, ts_bucket):
        self.port = port; self.ts_bucket = ts_bucket
    def q(self, sql):
        import subprocess
        out = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(self.port), "-U", "postgres", "-d", self.db, "-At", "-F", "\t", "-c", sql],
                             capture_output=True, text=True, env={"PGPASSWORD": self.pw, "PGTZ": "UTC", "PATH": os.environ.get("PATH", "/usr/bin:/bin")})
        if out.returncode: raise RuntimeError(f"psql rc {out.returncode}: {out.stderr.strip()[:500]} in {sql[:300]}")
        return [l.split("\t") for l in out.stdout.splitlines() if l]
    def rows(self, cls, q):
        r = self.q(q)
        if cls in ("raw", "point"):
            return [(ms_of(x[0]), float(x[1])) for x in r]
        return [(ms_of(x[0]) if self.ts_bucket else int(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in r]
    def digest(self, n, q):
        inner = q.rsplit(" ORDER BY ", 1)[0]
        r = self.q(f"SELECT series, sum(c), sum(s), sum(mn::float8), sum(mx::float8), count(*) FROM ({inner}) x GROUP BY series ORDER BY series")
        return {int(x[0]): tuple(float(v) for v in x[1:]) for x in r}

class TS(PG):
    db, pw = "postgres", "bench"
    def __init__(self): super().__init__(TS_PORT, True)

class QDB:
    def q(self, sql):
        out = json.loads(http(f"http://127.0.0.1:{QDB_HTTP}/exec?query=" + urllib.parse.quote(sql, safe="")))
        if "error" in out: raise RuntimeError(out["error"] + " in " + sql[:300])
        return out["dataset"]
    def rows(self, cls, q):
        r = self.q(q)
        if cls in ("raw", "point"):
            return [(ms_of(x[0]), float(x[1])) for x in r]
        return [(ms_of(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in r]
    def digest(self, n, q):
        inner = q.rsplit(" ORDER BY ", 1)[0]
        r = self.q(f"SELECT series, sum(c), sum(s), sum(cast(mn AS double)), sum(cast(mx AS double)), count() FROM ({inner}) GROUP BY series ORDER BY series")
        return {int(x[0]): tuple(float(v) for v in x[1:]) for x in r}

def reqline(line, base, headers):
    """Run a qdrive request line ("GET path" or "POST path<TAB>ctype<TAB>body") and return the body."""
    method, rest = line.split(" ", 1)
    if method == "GET":
        return http(base + rest, None, headers)
    path, ctype, body = rest.split("\t", 2)
    return http(base + path, body, dict(headers, **{"Content-Type": ctype}))

class I2QL:
    H = {"Authorization": "Token benchtoken"}
    def q(self, line):
        out = json.loads(reqline(line, f"http://127.0.0.1:{I2_PORT}", self.H))
        for r in out["results"]:
            if "error" in r: raise RuntimeError(r["error"])
        return out["results"]
    def rows(self, cls, line):
        res = self.q(line)[0].get("series", [])
        vals = res[0]["values"] if res else []
        if cls in ("raw", "point"):
            return [(int(x[0]), float(x[1])) for x in vals]
        return [(int(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in vals]
    def digest(self, n, line):
        d = {}
        for r in self.q(line):
            for se in r.get("series", []):
                s = int(se["tags"]["series"]); acc = [0.0] * 5
                for x in se["values"]:
                    for i in range(4): acc[i] += float(x[i + 1])
                    acc[4] += 1
                d[s] = tuple(acc)
        return d

class I2Flux:
    H = {"Authorization": "Token benchtoken", "Accept": "application/csv"}
    def q(self, line):
        import csv, io
        txt = reqline(line, f"http://127.0.0.1:{I2_PORT}", self.H)
        rows, hdr = [], None
        for rec in csv.reader(io.StringIO(txt)):
            if not rec or not any(rec): hdr = None; continue
            if hdr is None or rec[1] == "result": hdr = rec; continue
            rows.append(dict(zip(hdr, rec)))
        return rows
    def rows(self, cls, line):
        r = self.q(line)
        if cls in ("raw", "point"):
            return [(ms_of(x["_time"]), float(x["_value"])) for x in r]
        import re
        every = int(re.search(r"every: (\d+)ms", line).group(1))
        by = {}
        for x in r:  # the first window's _start is clipped to the range start: label it like the others
            t = ms_of(x["_time"]) // every * every
            by.setdefault(t, {})[x["result"]] = float(x["_value"])
        return [(t, int(v["count"]), v["sum"], v["min"], v["max"]) for t, v in sorted(by.items())]
    def digest(self, n, line):
        acc = {}
        for x in self.q(line):
            s = int(x["series"]); fn = x["result"].rstrip("0123456789")
            a = acc.setdefault(s, [0.0] * 5)
            a[["count", "sum", "min", "max"].index(fn)] += float(x["_value"])
            if fn == "count": a[4] += 1
        return {s: tuple(a) for s, a in acc.items()}

class I3:
    def q(self, sql):
        url = f"http://127.0.0.1:{I3_PORT}/api/v3/query_sql?db=cmp&format=json&q=" + urllib.parse.quote(sql, safe="")
        return [list(r.values()) for r in json.loads(http(url))]
    def sql(self, line):
        return urllib.parse.unquote(line.split("&q=", 1)[1])
    def rows(self, cls, line):
        r = self.q(self.sql(line))
        if cls in ("raw", "point"):
            return [(ms_of(x[0]), float(x[1])) for x in r]
        return [(ms_of(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in r]
    def digest(self, n, line):
        inner = self.sql(line).rsplit(" ORDER BY ", 1)[0]
        r = self.q(f"SELECT series, sum(c) AS c, sum(s) AS s, sum(mn) AS mn, sum(mx) AS mx, count(*) AS n FROM ({inner}) GROUP BY series ORDER BY series")
        return {int(x[0]): tuple(float(v) for v in x[1:]) for x in r}

class IOT:
    """IoTDB's REST API (table model): a line is "POST path<TAB>application/json<TAB>{database, sql}" (mkq.py)."""
    def q(self, sql):
        body = json.dumps({"database": "cmp", "sql": sql})
        r = json.loads(http(f"http://127.0.0.1:{IOT_REST}/rest/table/v1/query", body,
                            {"Content-Type": "application/json", "Authorization": "Basic cm9vdDpyb290"}))
        if "values" not in r:
            raise RuntimeError(f"iot: {r}")
        return r["values"]
    def sql(self, line):
        return json.loads(line.split("\t", 2)[2])["sql"]
    def rows(self, cls, line):
        r = self.q(self.sql(line))
        if cls in ("raw", "point"):
            return [(int(x[0]), float(x[1])) for x in r]
        return [(int(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in r]
    def digest(self, n, line):
        inner = self.sql(line).rsplit(" ORDER BY ", 1)[0]
        r = self.q(f"SELECT series, sum(c) AS c, sum(s) AS s, sum(mn) AS mn, sum(mx) AS mx, count(*) AS n FROM ({inner}) y GROUP BY series ORDER BY series")
        return {int(x[0]): tuple(float(v) for v in x[1:]) for x in r}


class Prom:
    """PromQL over Prometheus' HTTP API: a line is one or more requests joined by " && " (mkq.py)."""
    def q(self, line):
        out = []
        for req in line.split(" && "):
            r = json.loads(reqline(req, f"http://127.0.0.1:{PROM_PORT}", {}))
            if r.get("status") != "success": raise RuntimeError(str(r)[:300])
            out.append((req, r["data"]))
        return out
    def buckets(self, line):
        """{series: {bucket start: {fn: value}}} from query_range (labelled by the evaluation time
        b + w - 1) and from instant queries of clipped buckets (labelled b)."""
        by = {}
        for req, d in self.q(line):
            step = urllib.parse.parse_qs(req.split("?", 1)[1]).get("step")
            w = int(step[0][:-2]) if step else None  # "<w>ms"
            for se in d["result"]:
                m = se["metric"]; s = int(m.get("series", -1))
                pts = se["values"] if d["resultType"] == "matrix" else [se["value"]]
                for t, v in pts:
                    b = round(float(t) * 1000) + 1 - w if w else int(m.get("b", 0))
                    by.setdefault(s, {}).setdefault(b, {})[m["fn"]] = float(v)
        return by
    def rows(self, cls, line):
        if cls in ("raw", "point"):
            (_, d), = self.q(line)
            assert d["resultType"] == "matrix" and len(d["result"]) <= 1
            return [(round(float(t) * 1000), float(v)) for t, v in (d["result"][0]["values"] if d["result"] else [])]
        by = self.buckets(line)
        assert len(by) <= 1
        bs = next(iter(by.values()), {})
        return [(b, int(x["count"]), x["sum"], x["min"], x["max"]) for b, x in sorted(bs.items())]
    def digest(self, n, line):
        return {s: (sum(x["count"] for x in bs.values()), sum(x["sum"] for x in bs.values()), sum(x["min"] for x in bs.values()),
                    sum(x["max"] for x in bs.values()), float(len(bs)))
                for s, bs in self.buckets(line).items()}

class TD:
    """TDengine's REST API (taosAdapter): a line is "POST /rest/sql/cmp<TAB>text/plain<TAB>SQL" (mkq.py)."""
    def q(self, sql):
        r = json.loads(http(f"http://127.0.0.1:{TD_REST}/rest/sql/cmp", sql, {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="}))
        if r.get("code") != 0 or "data" not in r:
            raise RuntimeError(f"td: {r} in {sql[:300]}")
        return r["data"]
    def sql(self, line):
        return line.split("\t", 2)[2]
    def rows(self, cls, line):
        r = self.q(self.sql(line))
        if cls in ("raw", "point"):
            return [(int(x[0]), float(x[1])) for x in r]
        return [(int(x[0]), int(x[1]), float(x[2]), float(x[3]), float(x[4])) for x in r]
    def digest(self, n, line):  # summed here: TDengine's answer is the per-series (and per-bucket) rows
        d = {}
        for x in self.q(self.sql(line)):
            c, s_, mn, mx = (x[1:5] if n == 0 else x[2:6])
            g = d.setdefault(int(x[0]), [0.0, 0.0, 0.0, 0.0, 0])
            g[0] += c; g[1] += s_; g[2] += mn; g[3] += mx; g[4] += 1
        return {k: tuple(float(v) for v in g) for k, g in d.items()}

RUNNERS = {"i3": I3, "i2ql": I2QL, "i2flux": I2Flux, "qdb": QDB, "ts": TS, "ch": lambda: CH("cmp.t100m"), "chday": lambda: CH("cmp.t100m_day"), "prom": Prom, "vm": Prom, "iot": IOT, "td": TD}

def close(a, b, rel):  # False for NaN on either side: a NaN answer is a mismatch
    return a == b or abs(a - b) <= rel * max(abs(a), abs(b), 1e-300)

# VictoriaMetrics stores each value as a decimal of about 12 significant digits (lib/decimal FromFloat: conversion
# precision 1e12), so every value it sums may be off by 1e-12 of itself; a float32 still reads back exactly. Its sums
# are therefore also accepted within n * max|value| * 2e-12, which only matters where positive and negative values
# cancel (a sum near zero); counts, min and max are checked as for every system.
DECIMAL_REL = {"vm": 2e-12}.get(sys.argv[2] if sysname == "--batch" and len(sys.argv) > 2 else sysname, 0.0)

def sum_close(g, e, n, mn, mx):
    return close(g, e, 1e-9) or (DECIMAL_REL > 0 and abs(g - e) <= n * max(abs(mn), abs(mx)) * DECIMAL_REL)

def main():
    run = RUNNERS[sysname]()
    ref = [json.loads(l) for l in open(os.environ.get("REF_WINDOWS", "ref/windows.jsonl"))]
    qfiles = {}
    bad = 0; checked = {}; worst_sum = 0.0
    first = {}  # spec.tsv holds the classes one after another: index within the class
    for r in ref:
        first.setdefault(r["cls"], r["i"])
    for r in ref:
        cls = r["cls"]; k = r["i"] - first[cls]
        if k >= per: continue
        if cls not in qfiles: qfiles[cls] = open(f"q/{sysname}.{cls}.txt").read().splitlines()
        got = run.rows(cls, qfiles[cls][k])
        ok = True
        if cls in ("raw", "point"):
            n = len(got); s = sum(f32(v) for _, v in got)
            if cls == "point":
                ok = n == 1 and got[0][0] == r["first"] and close(got[0][1], r["v"], 1e-7)
            else:
                ok = n == r["n"] and (n == 0 or (got[0][0] == r["first"] and got[-1][0] == r["last"]))
                ok = ok and sum_close(s, r["sum"], n, min((v for _, v in got), default=0), max((v for _, v in got), default=0))
                worst_sum = max(worst_sum, abs(s - r["sum"]) / max(abs(r["sum"]), 1e-300))
            if not ok: print("MISMATCH", cls, k, "got", n, s, got[:1], got[-1:], "ref", r)
        else:
            b = r["b"]
            ok = len(got) == len(b)
            for g, e in zip(got, b):
                if not (g[0] == e[0] and g[1] == e[1] and sum_close(g[2], e[2], e[1], e[3], e[4]) and close(g[3], e[3], 1e-7) and close(g[4], e[4], 1e-7)):
                    ok = False
                worst_sum = max(worst_sum, abs(g[2] - e[2]) / max(abs(e[2]), 1e-300))
            if not ok: print("MISMATCH", cls, k, "got", len(got), got[:2], "ref", len(b), b[:2])
        checked[cls] = checked.get(cls, 0) + 1
        bad += not ok
    fref = {}
    for l in open("ref/full.jsonl"):
        x = json.loads(l); fref[(x["full"], x["s"])] = x["d"]
    for n in [int(x) for x in os.environ.get("FULLS", "0 3600 60").split()]:
        q = open(f"q/{sysname}.full{n}.txt").read().strip()
        d = run.digest(n, q)
        okc = len(d) == NSERIES
        for s in range(NSERIES):
            e = fref[(n, s)]; g = d.get(s)
            if g is None or not (g[0] == e[0] and g[4] == e[4] and sum_close(g[1], e[1], e[0], e[2], e[3]) and close(g[2], e[2], 1e-7) and close(g[3], e[3], 1e-7)):
                okc = False
                print("MISMATCH full", n, s, "got", g, "ref", e)
                break
        checked[f"full{n}"] = NSERIES
        bad += not okc
    print(sysname, "checked", checked, "mismatches", bad, f"worst relative sum error {worst_sum:.2e}")

def same_window(r, g):
    """Two reduced answers (tsdbq / embq dump records) of the same query."""
    if r["cls"] in ("raw", "point"):
        if r["n"] != g["n"]: return False
        if r["n"] == 0: return True
        if r["cls"] == "point": return g["first"] == r["first"] and close(g["v"], r["v"], 1e-7)
        return g["first"] == r["first"] and g["last"] == r["last"] and sum_close(g["sum"], r["sum"], r["n"], g.get("mn", 0), g.get("mx", 0))
    return len(g["b"]) == len(r["b"]) and all(
        x[0] == y[0] and x[1] == y[1] and sum_close(x[2], y[2], y[1], y[3], y[4]) and close(x[3], y[3], 1e-7) and close(x[4], y[4], 1e-7)
        for x, y in zip(g["b"], r["b"]))

def dumps(ref_w, ref_f, got_w, got_f):
    ref = {}
    for l in open(ref_w):
        x = json.loads(l); ref[x["i"]] = x
    checked, bad, missing = {}, 0, 0
    for l in open(got_w):
        g = json.loads(l); r = ref.get(g["i"])
        if r is None:  # an answer to a query the reference never asked is a failure too
            missing += 1; bad += 1; continue
        ok = r["cls"] == g["cls"] and same_window(r, g)
        if not ok:
            bad += 1
            if bad <= 10: print("MISMATCH", g["i"], "got", json.dumps(g)[:300], "ref", json.dumps(r)[:300])
        checked[g["cls"]] = checked.get(g["cls"], 0) + 1
    fref = {}
    for l in open(ref_f):
        x = json.loads(l); fref[(x["full"], x["s"])] = x["d"]
    seen = set()
    for l in open(got_f):
        x = json.loads(l); e = fref.get((x["full"], x["s"])); g = x["d"]
        seen.add((x["full"], x["s"]))
        if e is None or not (g[0] == e[0] and g[4] == e[4] and sum_close(g[1], e[1], e[0], e[2], e[3]) and close(g[2], e[2], 1e-7) and close(g[3], e[3], 1e-7)):
            bad += 1
            print("MISMATCH full", x["full"], x["s"], "got", g, "ref", e)
        checked[f"full{x['full']}"] = checked.get(f"full{x['full']}", 0) + 1
    for k in sorted(set(fref) - seen):  # every whole-range answer of the reference must be there
        if k[0] in {f for f, _ in seen}:
            bad += 1
            print("MISMATCH full", k[0], k[1], "missing")
    print(got_w, "checked", checked, "mismatches", bad, "not in the reference", missing)

def split_batch(run, sysname, line, cls):
    """One batch request (mkbatch.py) run once; its raw answer split per member, in the form the runner's q()
    returns for a single query: {member: raw}."""
    by = {}
    if sysname in ("ch", "chday", "ts", "qdb", "i3", "iot", "td"):
        if sysname in ("ch", "chday"): raw = run.q(line + " FORMAT TSV")
        elif sysname in ("i3", "iot", "td"): raw = run.q(run.sql(line))
        else: raw = run.q(line)
        for x in raw:
            by.setdefault(int(x[0]), []).append(x[1:])
    elif sysname == "i2ql":
        for r in run.q(line):
            by[int(r.get("statement_id", 0))] = [r]
    elif sysname == "i2flux":
        for x in run.q(line):
            m, _, name = x["result"].partition("_")
            by.setdefault(int(m[1:]), []).append(dict(x, result=name))
    else:
        raise SystemExit(f"check.py: no batch splitter for {sysname}")
    return by

def batch_main(sysname, ngroups):
    """check.py --batch SYS [GROUPS]: the batches of q/SYS.*.bK.txt against the reference, per member: the
    first GROUPS (5) groups of each window class and K whose members all have a reference answer, and every
    whole-range batch. A member answered by a request of another member, missing or extra fails."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import batches, mkq
    run = RUNNERS[sysname]()
    gen = mkq.GEN[sysname]
    ref = {}
    for l in open(os.environ.get("REF_WINDOWS", "ref/windows.jsonl")):
        x = json.loads(l); ref[x["i"]] = x
    fref = {}
    for l in open("ref/full.jsonl"):
        x = json.loads(l); fref[(x["full"], x["s"])] = x["d"]
    bad, checked = 0, {}
    for (st, k), grps in sorted(batches.read().items()):
        if st == "full":
            for n in [int(x) for x in os.environ.get("FULLS", "0 3600 60").split()]:
                lines = open(f"q/{sysname}.full{n}.b{k}.txt").read().splitlines()
                assert len(lines) == len(grps), f"q/{sysname}.full{n}.b{k}.txt: {len(lines)} lines, {len(grps)} batches"
                for line, g in zip(lines, grps):
                    d = run.digest(n, line); want = [x["series"] for x in g]
                    ok = sorted(d) == sorted(want)
                    for s in want:
                        e, x = fref[(n, s)], d.get(s)
                        if x is None or not (x[0] == e[0] and x[4] == e[4] and sum_close(x[1], e[1], e[0], e[2], e[3]) and close(x[2], e[2], 1e-7) and close(x[3], e[3], 1e-7)):
                            ok = False
                    if not ok: print("MISMATCH batch", f"full{n}.b{k}", want[:3], "got", sorted(d)[:5])
                    bad += not ok
                    checked[f"full{n}.b{k}"] = checked.get(f"full{n}.b{k}", 0) + 1
            continue
        lines = open(f"q/{sysname}.{st}.b{k}.txt").read().splitlines()
        assert len(lines) == len(grps), f"q/{sysname}.{st}.b{k}.txt: {len(lines)} lines, {len(grps)} batches"
        done = 0
        for line, g in zip(lines, grps):
            if done >= ngroups: break
            if any(x["i"] not in ref for x in g): continue
            singles = [gen(st, x["series"], x["start"], x["end"], x["bk"]) for x in g]
            if sysname in ("prom", "vm"):  # K requests on one connection: the batch is the members' requests in order
                ok = line == " && ".join(singles)
                answers = {m: run.rows(st, q) for m, q in enumerate(singles)}
            else:
                parts = split_batch(run, sysname, line, st)
                ok = set(parts) <= set(range(len(g)))
                answers = {}
                for m, q in enumerate(singles):
                    raw = parts.get(m, [])
                    run.q = lambda *_a, _r=raw: _r  # the member's share of the answer, through the single-query parser
                    try:
                        answers[m] = run.rows(st, q)
                    finally:
                        del run.q
            for m, x in enumerate(g):
                r, got = ref[x["i"]], answers.get(m, [])
                if st in ("raw", "point"):
                    got = sorted(got)
                    w = {"cls": st, "n": len(got), "first": got[0][0] if got else None, "last": got[-1][0] if got else None,
                         "sum": sum(f32(v) for _, v in got), "v": got[0][1] if got else None}
                else:
                    w = {"cls": st, "b": [list(b) for b in sorted(got)]}
                if not same_window(r, w):
                    ok = False
                    print("MISMATCH batch", f"{st}.b{k}", "member", m, "spec line", x["i"], "got", json.dumps(w)[:200], "ref", json.dumps(r)[:200])
            bad += not ok
            done += 1
            checked[f"{st}.b{k}"] = checked.get(f"{st}.b{k}", 0) + 1
    print(sysname, "batches checked", checked, "mismatches", bad)

def server_down(e):
    """True when e says the server could not be reached or went away: reported as SERVER DOWN (exit 3), not as a
    mismatch of answers (the harness then restarts the server and tries once more)."""
    t = (str(e) + " " + str(getattr(e, "stderr", "") or "")).lower()
    return isinstance(e, (ConnectionError, urllib.error.URLError, httpclient.RemoteDisconnected)) or any(
        m in t for m in ("connection refused", "server closed the connection", "could not connect", "connection reset",
                         "remote end closed", "broken pipe", "no route to host"))

def guarded(fn, *a):
    try:
        return fn(*a)
    except Exception as e:
        if server_down(e):
            print("SERVER DOWN: " + repr(e)[:300])
            raise SystemExit(3)
        raise

if __name__ == "__main__":
    if sysname == "--dumps":
        dumps(*sys.argv[2:6])
    elif sysname == "--batch":
        guarded(batch_main, sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 5)
    else:
        guarded(main)
