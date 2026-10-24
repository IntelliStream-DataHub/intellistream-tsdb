# Correctness: runs the first N queries of each class on SYS (the benchmark's query text from q/,
# only the output format may differ) and the whole-range queries, and compares them with tsdb's
# answers in ref/ (tsdbq dump / tsdbq full ... dump). Run in $WORK:
#   python3 -I check.py SYS [N]          FULLS="0 3600" limits the whole-range queries ("" skips them)
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
import datetime, json, os, struct, sys, urllib.request, urllib.parse

CH_HTTP = os.environ.get("CH_HTTP_PORT", "8123")
TS_PORT = os.environ.get("TS_PORT", "5434")
QDB_HTTP = os.environ.get("QDB_HTTP_PORT", "9100")
I2_PORT = os.environ.get("I2_PORT", "8086")
I3_PORT = os.environ.get("I3_PORT", "8181")

def f32(x):  # values are float32, and some servers print the shortest float32 text ("30.8")
    return struct.unpack('<f', struct.pack('<f', x))[0]

sysname = sys.argv[1] if len(sys.argv) > 1 else ""
per = int(sys.argv[2]) if len(sys.argv) > 2 and sysname != "--dumps" else 200

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
                             capture_output=True, text=True, env={"PGPASSWORD": self.pw, "PGTZ": "UTC", "PATH": os.environ.get("PATH", "/usr/bin:/bin")}, check=True).stdout
        return [l.split("\t") for l in out.splitlines() if l]
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

RUNNERS = {"i3": I3, "i2ql": I2QL, "i2flux": I2Flux, "qdb": QDB, "ts": TS, "ch": lambda: CH("cmp.t100m"), "chday": lambda: CH("cmp.t100m_day")}

def close(a, b, rel):
    return a == b or abs(a - b) <= rel * max(abs(a), abs(b), 1e-300)

def main():
    run = RUNNERS[sysname]()
    ref = [json.loads(l) for l in open("ref/windows.jsonl")]
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
                ok = ok and close(s, r["sum"], 1e-9)
                worst_sum = max(worst_sum, abs(s - r["sum"]) / max(abs(r["sum"]), 1e-300))
            if not ok: print("MISMATCH", cls, k, "got", n, s, got[:1], got[-1:], "ref", r)
        else:
            b = r["b"]
            ok = len(got) == len(b)
            for g, e in zip(got, b):
                if not (g[0] == e[0] and g[1] == e[1] and close(g[2], e[2], 1e-9) and close(g[3], e[3], 1e-7) and close(g[4], e[4], 1e-7)):
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
        okc = len(d) == 100
        for s in range(100):
            e = fref[(n, s)]; g = d.get(s)
            if g is None or not (g[0] == e[0] and g[4] == e[4] and close(g[1], e[1], 1e-9) and close(g[2], e[2], 1e-7) and close(g[3], e[3], 1e-7)):
                okc = False
                print("MISMATCH full", n, s, "got", g, "ref", e)
                break
        checked[f"full{n}"] = 100
        bad += not okc
    print(sysname, "checked", checked, "mismatches", bad, f"worst relative sum error {worst_sum:.2e}")

def same_window(r, g):
    """Two reduced answers (tsdbq / embq dump records) of the same query."""
    if r["cls"] in ("raw", "point"):
        if r["n"] != g["n"]: return False
        if r["n"] == 0: return True
        if r["cls"] == "point": return g["first"] == r["first"] and close(g["v"], r["v"], 1e-7)
        return g["first"] == r["first"] and g["last"] == r["last"] and close(g["sum"], r["sum"], 1e-9)
    return len(g["b"]) == len(r["b"]) and all(
        x[0] == y[0] and x[1] == y[1] and close(x[2], y[2], 1e-9) and close(x[3], y[3], 1e-7) and close(x[4], y[4], 1e-7)
        for x, y in zip(g["b"], r["b"]))

def dumps(ref_w, ref_f, got_w, got_f):
    ref = {}
    for l in open(ref_w):
        x = json.loads(l); ref[x["i"]] = x
    checked, bad, missing = {}, 0, 0
    for l in open(got_w):
        g = json.loads(l); r = ref.get(g["i"])
        if r is None: missing += 1; continue
        ok = r["cls"] == g["cls"] and same_window(r, g)
        if not ok:
            bad += 1
            if bad <= 10: print("MISMATCH", g["i"], "got", json.dumps(g)[:300], "ref", json.dumps(r)[:300])
        checked[g["cls"]] = checked.get(g["cls"], 0) + 1
    fref = {}
    for l in open(ref_f):
        x = json.loads(l); fref[(x["full"], x["s"])] = x["d"]
    for l in open(got_f):
        x = json.loads(l); e = fref[(x["full"], x["s"])]; g = x["d"]
        if not (g[0] == e[0] and g[4] == e[4] and close(g[1], e[1], 1e-9) and close(g[2], e[2], 1e-7) and close(g[3], e[3], 1e-7)):
            bad += 1
            print("MISMATCH full", x["full"], x["s"], "got", g, "ref", e)
        checked[f"full{x['full']}"] = checked.get(f"full{x['full']}", 0) + 1
    print(got_w, "checked", checked, "mismatches", bad, "not in the reference", missing)

if __name__ == "__main__":
    if sysname == "--dumps":
        dumps(*sys.argv[2:6])
    else:
        main()
