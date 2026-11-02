# checktest.py: the answer checkers (rel.py compare, check.py --dumps) must reject wrong answers and accept right ones.
#   python3 -I checktest.py mutate [TWA_REF WINDOWS_REF FULL_REF]
# Takes reference answers (tsdb's relq twa / tsdbq dump / tsdbq full dumps of the crafted data set; without files, a
# small built-in reference with a gap, a single-reading series and a bucket with a carried-in piece), writes the
# true answers back in each system's row form and checks they pass, then one mutation at a time that must fail:
# NaN, covered time off by one ms, a missing bucket, an extra bucket, a value off by 1.5e-9 relative (just over the
# 1e-9 tolerance), a missing and an extra series, a count off by one; LTTB: an exact tie of the triangle area may be
# answered with the other point (and the picks after it), a smaller area may not. Prints one line per case and "checktest OK"
# or "checktest FAILED".
import copy, io, json, os, shutil, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import rel  # noqa: E402

H = 3600000
failures = 0

def case(name, expect_ok, ok):
    global failures
    good = ok == expect_ok
    failures += not good
    print(f"{'ok  ' if good else 'BAD '} {name}: checker said {'pass' if ok else 'fail'}, expected {'pass' if expect_ok else 'fail'}")

def quiet(fn, *a, **k):
    old = sys.stdout; sys.stdout = io.StringIO()
    try: return fn(*a, **k)
    finally: sys.stdout = old

def builtin_twa():
    # series 0: readings at 0:30 (1.0), 1:15 (3.0), 4:00 (2.0) -> carried over the 1:00 edge, a gap of 2.75 h
    # series 1: one reading (no interval: no bucket); series 2: two readings 10 ms apart
    t0 = 1704067200000
    return {
        0: {"s": 0, "b": [[t0, 1.0 * 1800000, 1800000, 1], [t0 + H, 1.0 * 900000 + 3.0 * 2700000, H, 1],
                          [t0 + 2 * H, 3.0 * H, H, 0], [t0 + 3 * H, 3.0 * H, H, 0]]},
        1: {"s": 1, "b": []},
        2: {"s": 2, "b": [[t0, 0.5 * 10, 10, 1]]},
    }

def twa_rows(ref, with_covered=True):
    rows = []
    for s, e in sorted(ref.items()):
        for b in e["b"]:
            if b[2] <= 0: continue
            rows.append([s, b[0], b[1], b[2]] if with_covered else [s, b[0], b[1] / b[2]])
    return rows

def twa_cases(ref):
    rows = twa_rows(ref)
    case("twa: true answers", True, quiet(rel.compare, "twa", ref, rows))
    case("twa: true answers, average only (toolkit form)", True, quiet(rel.compare, "twa", ref, twa_rows(ref, False)))
    if not rows:
        print("no TWA buckets in the reference"); return
    i = len(rows) // 2
    m = copy.deepcopy(rows); m[i][2] = float("nan"); case("twa: NaN area", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); m[i][2] = "nan"; case("twa: 'nan' text", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); m[i][2] = ""; case("twa: empty (null) area", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); m[i][3] += 1; m[i][2] = m[i][2] / (m[i][3] - 1) * m[i][3]
    case("twa: covered off by 1 ms, same average", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); del m[i]; case("twa: a bucket missing", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); m.append([m[i][0], m[i][1] + 1000 * H, 1.0, 1000])
    case("twa: an extra bucket", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); m[i][2] = m[i][2] * (1 + 1.5e-9) if m[i][2] else 1e-300
    case("twa: value off by 1.5e-9 relative", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(rows); m.append([max(ref) + 1, rows[0][1], 1.0, 1000])
    case("twa: an extra series", False, quiet(rel.compare, "twa", ref, m))
    s0 = rows[0][0]
    m = [r for r in rows if r[0] != s0]; case("twa: a series missing", False, quiet(rel.compare, "twa", ref, m))
    m = copy.deepcopy(twa_rows(ref, False)); m[i][2] = float("nan")
    case("twa: NaN average (toolkit form)", False, quiet(rel.compare, "twa", ref, m))
    # the toolkit exemption covers only the last bucket of a series, and only when asked for
    last = max(j for j, r in enumerate(rows) if r[0] == s0)
    m = copy.deepcopy(twa_rows(ref, False)); m[last][2] *= 1.01
    case("twa: last bucket differs, strict", False, quiet(rel.compare, "twa", ref, m))
    case("twa: last bucket differs, toolkit exemption", True, quiet(rel.compare, "twa", ref, m, lenient_last=True))
    first = min(j for j, r in enumerate(rows) if r[0] == s0)
    if first != last:
        m = copy.deepcopy(twa_rows(ref, False)); m[first][2] *= 1.01
        case("twa: first bucket differs, toolkit exemption", False, quiet(rel.compare, "twa", ref, m, lenient_last=True))

def windows_cases(ref_w, ref_f):
    """check.py --dumps on files: the reference against itself, then mutations of one answer each."""
    d = tempfile.mkdtemp(prefix="checktest-", dir=os.environ.get("TMPDIR_CT", os.environ.get("WORK", ".")))
    W = [json.loads(l) for l in open(ref_w)]
    F = [json.loads(l) for l in open(ref_f)]
    def run(ws, fs):
        pw, pf = os.path.join(d, "w.jsonl"), os.path.join(d, "f.jsonl")
        with open(pw, "w") as f: f.writelines(json.dumps(x) + "\n" for x in ws)
        with open(pf, "w") as f: f.writelines(json.dumps(x) + "\n" for x in fs)
        out = subprocess.run([sys.executable, "-I", os.path.join(HERE, "..", "check.py"), "--dumps", ref_w, ref_f, pw, pf],
                             capture_output=True, text=True)
        return out.returncode == 0 and " mismatches 0 " in out.stdout + " "
    case("windows: true answers", True, run(W, F))
    bi = next((i for i, x in enumerate(W) if x.get("b")), None)
    ri = next((i for i, x in enumerate(W) if x["cls"] == "raw" and x["n"] > 0), None)
    if bi is not None:
        m = copy.deepcopy(W); m[bi]["b"][0][2] = float("nan"); case("windows: NaN bucket sum", False, run(m, F))
        m = copy.deepcopy(W); del m[bi]["b"][-1]; case("windows: a bucket missing", False, run(m, F))
        m = copy.deepcopy(W); m[bi]["b"].append(list(m[bi]["b"][-1])); m[bi]["b"][-1][0] += 1
        case("windows: an extra bucket", False, run(m, F))
        m = copy.deepcopy(W); m[bi]["b"][0][1] += 1; case("windows: bucket count off by one", False, run(m, F))
        m = copy.deepcopy(W); m[bi]["b"][0][2] = m[bi]["b"][0][2] * (1 + 1.5e-9) if m[bi]["b"][0][2] else 1e-300
        case("windows: bucket sum off by 1.5e-9 relative", False, run(m, F))
    if ri is not None:
        m = copy.deepcopy(W); m[ri]["sum"] = float("nan"); case("windows: NaN raw sum", False, run(m, F))
        m = copy.deepcopy(W); m[ri]["n"] += 1; case("windows: raw row count off by one", False, run(m, F))
    m = copy.deepcopy(W); m.append(dict(m[0], i=10 ** 9)); case("windows: an answer to an unknown query", False, run(m, F))
    if F:
        m = copy.deepcopy(F); m[0]["d"][1] = float("nan"); case("full: NaN sum", False, run(W, m))
        m = copy.deepcopy(F); del m[0]; case("full: a series missing", False, run(W, m))
    shutil.rmtree(d)

def decimal_cases():
    """check.py's sum tolerance for VictoriaMetrics' 12-digit decimals: only for vm (also under --batch), only within
    n * max|value| * 2e-12."""
    def sum_close(argv, *x):
        code = ("import json, sys; sys.argv = %r; sys.path.insert(0, %r); import check; print(check.sum_close(*json.loads(%r)))"
                % (argv, os.path.join(HERE, ".."), json.dumps(x)))
        return subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, check=True).stdout.strip() == "True"
    near0 = (1e-4 + 5e-13, 1e-4, 300, -1.24, 1.17)   # a cancelling sum, off by VictoriaMetrics' rounding
    case("decimal: vm near-zero sum off by its rounding", True, sum_close(["check.py", "vm"], *near0))
    case("decimal: vm --batch near-zero sum off by its rounding", True, sum_close(["check.py", "--batch", "vm"], *near0))
    case("decimal: prom near-zero sum off by 5e-13 (exact doubles)", False, sum_close(["check.py", "prom"], *near0))
    case("decimal: vm sum off by 1e-9 relative", False, sum_close(["check.py", "vm"], 100.0 * (1 + 1e-9), 100.0, 100, 0.5, 1.5))
    case("decimal: vm NaN sum", False, sum_close(["check.py", "vm"], float("nan"), 1e-4, 300, -1.24, 1.17))

def exact_lttb(T, Y, th, force=None):
    """LTTB in fractions, the first of equal areas wins, bucket b forced to point force[b]: an independent replay."""
    from fractions import Fraction as F
    import math
    n, nb = len(T), th - 2
    every = (n - 2) / nb
    st = lambda b: math.floor(b * every) + 1
    out = [0]
    for b in range(nb):
        nlo, nhi = (st(b + 1), st(b + 2)) if b + 1 < nb else (n - 1, n)
        cx, cy = F(sum(T[nlo:nhi]), nhi - nlo), sum(map(F, Y[nlo:nhi])) / (nhi - nlo)
        a = out[-1]
        area = lambda j: abs((T[a] - cx) * (F(Y[j]) - F(Y[a])) - (T[a] - T[j]) * (cy - F(Y[a])))
        out.append((force or {}).get(b, max(range(st(b), st(b + 1)), key=lambda j: (area(j), -j))))
    return out + [n - 1]

def lttb_cases():
    """rel.py accepts an LTTB answer that differs from tsdb's only through an exact tie of the triangle area (replayed on
    the rows of DATA) and the picks that follow from it, and nothing else. Bucket 0: the next bucket's mean equals the
    anchor's value, so points 1 (+1) and 2 (-1) have the same area."""
    import struct
    T = [k * 1000 for k in range(11)]
    Y = [0.0, 1.0, -1.0, 0.5, 2.0, -2.0, 0.0, 3.0, 0.0, 0.0, 0.25]
    th = 5
    first, other = exact_lttb(T, Y, th), exact_lttb(T, Y, th, {0: 2})
    assert first[1] == 1 and other[1] == 2 and first[2:-1] != other[2:-1], (first, other)
    ref = {0: {"s": 0, "t": [T[k] for k in first], "v": [Y[k] for k in first]}}
    rows = lambda idx: [[0, T[k], Y[k]] for k in idx]
    d = tempfile.mkdtemp(prefix="checktest-")
    old = os.environ.get("DATA")
    try:
        os.environ["DATA"] = os.path.join(d, "rows.rowbinary")
        with open(os.environ["DATA"], "wb") as f:
            for t, y in zip(T, Y): f.write(struct.pack("<Iqf", 0, t, y))
        case("lttb: tsdb's answer", True, quiet(rel.compare, "lttb", ref, rows(first)))
        case("lttb: the other point of a tie and the picks after it", True, quiet(rel.compare, "lttb", ref, rows(other)))
        m = list(other); m[2] = next(j for j in range(4, 7) if j != other[2])
        case("lttb: a pick with a smaller area after the tie", False, quiet(rel.compare, "lttb", ref, rows(m)))
        m = list(first); m[1] = 3
        case("lttb: a pick with a smaller area (no tie)", False, quiet(rel.compare, "lttb", ref, rows(m)))
        m = rows(other); m[1][2] = 1.0
        case("lttb: the tie's time with another row's value", False, quiet(rel.compare, "lttb", ref, m))
        os.environ["DATA"] = os.path.join(d, "missing")
        case("lttb: the other point of a tie without DATA", False, quiet(rel.compare, "lttb", ref, rows(other)))
    finally:
        if old is None: os.environ.pop("DATA", None)
        else: os.environ["DATA"] = old
        shutil.rmtree(d)

def exit_cases(ref):
    """rel.py jsonl and tsv exit 1 on a mismatch (run.sh's callers rely on it, besides grepping MISMATCH), 0 when the
    answers agree."""
    d = tempfile.mkdtemp()
    try:
        r, good, bad, tsv = (os.path.join(d, x) for x in ("ref.jsonl", "good.jsonl", "bad.jsonl", "bad.tsv"))
        with open(r, "w") as f: f.writelines(json.dumps(e) + "\n" for e in ref.values())
        shutil.copy(r, good)
        m = copy.deepcopy(ref); s0 = next(s for s in m if m[s]["b"]); m[s0]["b"][0][1] *= 1.01
        with open(bad, "w") as f: f.writelines(json.dumps(e) + "\n" for e in m.values())
        with open(tsv, "w") as f: f.writelines("\t".join(map(str, x)) + "\n" for x in twa_rows(m))
        run = lambda *a: subprocess.run([sys.executable, "-I", os.path.join(HERE, "rel.py"), *a], capture_output=True).returncode
        case("rel.py jsonl: the same answers exit 0", True, run("jsonl", "twa", r, good) == 0)
        case("rel.py jsonl: a mismatch exits 1", False, run("jsonl", "twa", r, bad) != 1)
        case("rel.py tsv: a mismatch exits 1", False, run("tsv", "twa", r, tsv) != 1)
    finally:
        shutil.rmtree(d)

def batch_cases():
    """rel.py qb / checkb, the TWA window batches: each member's WITH tables renamed apart, its rows mapped back to its
    window by the member column; a member answered with another's rows, a missing or an extra member, a wrong value
    and a request file that is not rel.py's batch must fail."""
    import re
    cs, body = rel.ctes("WITH seg AS (SELECT (1) AS x), parts AS (SELECT x FROM seg) SELECT x FROM parts")
    case("batch: WITH tables parsed", True, cs == [("seg", "SELECT (1) AS x"), ("parts", "SELECT x FROM seg")]
         and body == "SELECT x FROM parts")
    ws = [(3, 1704067200000, 1704153600000, 18000000), (4, 1704070800000, 1704157200000, 18000000)]
    bcs, bbody = rel.ctes(rel.wbatch("i3", ws))
    own = all(set(re.findall(r"\b(?:seg|parts)(\d+)\b", b)) <= {n[-1]} for n, b in bcs)
    case("batch: i3 members use their own WITH tables only", True, own
         and [n for n, _ in bcs] == ["seg0", "parts0", "seg1", "parts1"]
         and re.findall(r"\(SELECT (\d) AS mb, bb,.*?FROM parts(\d) ", bbody) == [("0", "0"), ("1", "1")])
    case("batch: ch is mkbatch.py's UNION ALL form", True,
         rel.wbatch("ch", ws).startswith("SELECT * FROM ((SELECT 0 AS mb, bb, "))
    d = tempfile.mkdtemp()
    try:
        old_spec, old_run, cwd = os.environ.get("TWA_SPEC"), rel.run_sql, os.getcwd()
        os.chdir(d)
        os.makedirs("q")
        t0 = 1704067200000
        with open("spec.tsv", "w") as f:
            for i in range(6):
                f.write(f"agg1d\t{i % 4}\t{t0 + i * H}\t{t0 + i * H + 86400000}\t18000000\n")
                f.write(f"raw\t0\t{t0}\t{t0 + H}\t0\n")
        os.environ["TWA_SPEC"] = "spec.tsv"
        ref = {}
        for i, s_, a_, b_, bk in rel.windows("twa1d"):
            ref[i] = {"s": s_, "i": i, "b": [[a_ // bk * bk, 2.0 * (i + 1) * 1000, 1000, 1], [a_ // bk * bk + bk, 7.5 * 1000, 1000, 2]]}
        groups = rel.wgroups("twa1d", 3)
        with open("q/ch.twa1d.b3.txt", "w") as f:
            f.write("".join(rel.wire("ch", rel.wbatch("ch", [x[1:] for x in g])) + "\n" for g in groups))
        sqls = [rel.wbatch("ch", [x[1:] for x in g]) for g in groups]
        def server(mutate=None):
            def run_sql(sysname, sql):
                g = groups[sqls.index(sql)]
                rows = [[m, b[0], b[1], b[2]] for m, x in enumerate(g) for b in ref[x[0]]["b"]]
                return mutate(rows) if mutate else rows
            return run_sql
        def checkb(mutate=None):
            rel.run_sql = server(mutate)
            rows, keys = rel.fetch_batches("ch", "twa1d", 3, "q/ch.twa1d.b3.txt")
            return quiet(rel.compare, "twa1d", {i: ref[i] for i in keys}, rows) and sorted(keys) == sorted(ref)
        case("batch: true answers", True, checkb())
        case("batch: two members' rows swapped", False, checkb(lambda r: [[{0: 1, 1: 0}.get(x[0], x[0]), *x[1:]] for x in r]))
        case("batch: a member missing", False, checkb(lambda r: [x for x in r if x[0] != 2]))
        case("batch: an extra member", False, checkb(lambda r: r + [[3, *r[0][1:]]]))
        case("batch: a value off by 1.5e-9 relative", False,
             checkb(lambda r: [r[0][:2] + [r[0][2] * (1 + 1.5e-9)] + r[0][3:]] + r[1:]))
        lines = open("q/ch.twa1d.b3.txt").read().splitlines()
        with open("q/ch.twa1d.b3.txt", "w") as f: f.write("\n".join([lines[0].replace("18000000", "18000001", 1)] + lines[1:]) + "\n")
        try: checkb(); ok = True
        except SystemExit: ok = False
        case("batch: a request file with a line that is not rel.py's", False, ok)
    finally:
        rel.run_sql = old_run
        os.chdir(cwd)
        if old_spec is None: os.environ.pop("TWA_SPEC", None)
        else: os.environ["TWA_SPEC"] = old_spec
        shutil.rmtree(d)

def refused_cases():
    """An HTTP error answer (QuestDB's RSS limit comes as a 400 with a JSON error) is the server refusing, with its
    message: never SERVER DOWN (HTTPError is a URLError, which server_down once counted as down)."""
    import http.server, threading
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            b = json.dumps({"query": "x", "error": "global RSS memory limit exceeded", "position": 0}).encode()
            self.send_response(400); self.end_headers(); self.wfile.write(b)
        def log_message(self, *a): pass
    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        rel.http(f"http://127.0.0.1:{srv.server_address[1]}/exec?query=1")
        e = None
    except Exception as x:
        e = x
    finally:
        srv.shutdown()
    case("http 400: raised as Refused with the server's message", True,
         isinstance(e, rel.Refused) and "global RSS memory limit exceeded" in str(e))
    case("http 400: not SERVER DOWN", True, e is not None and not rel.server_down(e))
    out = io.StringIO(); old = sys.stdout; sys.stdout = out
    try:
        rel.guarded(lambda: (_ for _ in ()).throw(rel.Refused("HTTP 400: no")))
        rc = 0
    except SystemExit as x:
        rc = x.code
    finally:
        sys.stdout = old
    case("http 400: guarded exits 4 with a REFUSED line", True, rc == 4 and out.getvalue().startswith("REFUSED: HTTP 400: no"))
    # IoTDB's 719 (its query memory estimate) that outlasts the waits is a refusal with its message, not a crash
    old_http, old_sleep = rel.http, rel.time.sleep
    rel.http = lambda *a, **k: json.dumps({"code": 719, "message": "There is not enough memory to execute current fragment instance"})
    rel.time.sleep = lambda s: None
    try:
        rel.run_sql("iot", "SELECT 1"); e = None
    except Exception as x:
        e = x
    finally:
        rel.http, rel.time.sleep = old_http, old_sleep
    case("iot 719 after the waits: Refused with IoTDB's message", True, isinstance(e, rel.Refused) and "not enough memory" in str(e))
    # InfluxDB 3 refused a 100-window batch sent in a GET URL (HTTP 414): a long query goes as a POST body instead
    short, long_ = rel.wire("i3", "SELECT 1"), rel.wire("i3", "SELECT " + "x" * 9000)
    case("i3: a short query stays a GET line", True, short.startswith("GET /api/v3/query_sql?"))
    b = long_.split("\t")
    case("i3: a long query is a POST with the query in its JSON body", True,
         len(b) == 3 and b[0] == "POST /api/v3/query_sql" and json.loads(b[2])["q"] == "SELECT " + "x" * 9000)

def tables_cases():
    """tables.py: a class whose every run the server refused is a result ('-' and why), not "no timing" (exit 1);
    a mean file with no timing and no failure named stays an error."""
    import tables
    d = tempfile.mkdtemp()
    try:
        open(os.path.join(d, "ch.twa7d.b100.c32.mean.txt"), "w").write("query failed   # TEXT DIFFERS between runs\n")
        for i, t in ((1, "query failed: status -1: 44\t17"), (2, "query failed: status 500: Code: 241. DB::Exception: (total) memory limit exceeded: would use 43 GiB")):
            open(os.path.join(d, "ch.twa7d.b100.c32.%d" % i), "w").write(t)
        open(os.path.join(d, "x.raw.c1.mean.txt"), "w").write("nothing here\n")
        open(os.path.join(d, "duckdb.twa.txt"), "w").write("sqlt run 1: 1.80 s (3 rows)\nsqlt run 2: 1.84 s (3 rows)\n"
                                                           "sqlt: mean 1.8200 s sd 0.0200, 2 runs after a warm-up\n")
        open(os.path.join(d, "tsdb.twa.t1.txt"), "w").write("twa 1 conns: mean 0.0390 s sd 0.0015 (3.8%) min 0.0380 "
                                                            "max 0.0416, 5 runs after a warm-up; 339774 buckets\n")
        tables.errors.clear()
        rows = tables.results_table(d)
        text = "\n".join(r if isinstance(r, str) else r[1] for r in rows)
        case("tables: refused class shown with the server's error", True,
             "no run timed: 1 x status -1; 1 x Code: 241. DB::Exception: (total) memory limit exceeded" in text)
        case("tables: refused class is no error", True, not any("ch.twa7d" in e for e in tables.errors))
        case("tables: p3 DuckDB/chDB one-query TWA read (mean, spread from the runs)", True,
             "| duckdb.twa | TWA (hourly STEP buckets) of the whole table in one query: mean 1.8200 s | sd 1.1% [1.8000-1.8400 s] | 2 |" in text)
        case("tables: p3 embedded tsdb counts threads, not connections", True, "twa on 1 threads" in text)
        os.makedirs(os.path.join(d, "p6"))
        for x in (d, os.path.join(d, "p6")):
            open(os.path.join(x, "qdb.twa1d.c1.mean.txt"), "w").write(
                "q c=1: 10 queries in 15.00 s = 0.7 q/s, mean 89.1 ms, median 89.2 ms, p99 99.0 ms\n")
        text = "\n".join(r if isinstance(r, str) else r[1] for r in tables.results_table(d))
        case("tables: a class timed again in p6 is flagged superseded in its phase-2 row", True,
             any(l.startswith("| qdb.twa1d.c1 ") and "SUPERSEDED" in l for l in text.splitlines()) and
             any(l.startswith("| p6: qdb.twa1d.c1 ") and "SUPERSEDED" not in l for l in text.splitlines()))
        case("tables: a mean file with no timing and no failure named is still an error", True,
             any("x.raw.c1" in e for e in tables.errors))
    finally:
        shutil.rmtree(d)
        tables.errors.clear()

def many_cases():
    """tables.py: a failed 32-client class with a 16-client result (the extra-clients pass, run.sh MANY_CLIENTS=16) gets
    the note that it ran out of memory (peak, limit, memory held before it) and that the 16-client run is the comparison;
    a 32-client class that worked, or one without a 16-client result, gets none. how.py: the commit of a tree without a
    repository (git archive) is the file COMMIT."""
    import tables, how
    d = tempfile.mkdtemp()
    T16 = "q c=16: 160 queries in 15.00 s = 10.7 q/s, mean 1400.1 ms, median 1390.0 ms, p99 2100.0 ms\n"
    try:
        os.makedirs(os.path.join(d, "p6"))
        os.makedirs(os.path.join(d, "memsweep"))
        v = "qdb-parquet"
        # agg7d.b100.c32: killed at the container limit (the run file says only the connection dropped), nothing timed
        open(os.path.join(d, v + ".agg7d.b100.c32.mean.txt"), "w").write("query failed   # TEXT DIFFERS between runs\n")
        open(os.path.join(d, v + ".agg7d.b100.c32.1"), "w").write("query failed: server closed the connection unexpectedly\n")
        open(os.path.join(d, v + ".agg7d.b100.c32.1.oom"), "w").write("query failed: server closed the connection unexpectedly\n")
        open(os.path.join(d, "memsweep", v + ".agg7d.b100.c32.1.mem"), "w").write(
            "rb-qdb peak 51539607552 nocache 51536883712 start 41239035904 startnocache 40796782592\n")
        open(os.path.join(d, "memsweep", "REFUSED.txt"), "w").write(
            "p6 %s.agg7d.b100.c32 r1: the server died: killed out of memory by the kernel (container limit 48 GB; exited exit code 137)\n" % v)
        open(os.path.join(d, "p6", v + ".agg7d.b100.c16.mean.txt"), "w").write(T16)
        # point.b100.c32 worked, and has a 16-client result: no note; raw.b100.c32 failed and has none: no note
        open(os.path.join(d, v + ".point.b100.c32.mean.txt"), "w").write(T16.replace("c=16", "c=32"))
        open(os.path.join(d, "p6", v + ".point.b100.c16.mean.txt"), "w").write(T16)
        open(os.path.join(d, v + ".raw.b100.c32.mean.txt"), "w").write("query failed   # TEXT DIFFERS between runs\n")
        open(os.path.join(d, v + ".raw.b100.c32.1"), "w").write("query failed: server closed the connection unexpectedly\n")
        tables.errors.clear()
        rows = tables.results_table(d)
        text = "\n".join(r if isinstance(r, str) else r[1] for r in rows).splitlines()
        row = next((l for l in text if l.startswith("| %s.agg7d.b100.c32 " % v)), "")
        case("many: the failed 32-client class says it ran out of memory", True, "the 32-client run ran out of memory" in row)
        case("many: with the peak (with and without the page cache), the limit and what was held before", True,
             "rb-qdb peak 48.0 GB with the page cache (48.0 GB without)" in row and "limit 48 GB" in row
             and "38.4 GB held when the class started" in row)
        case("many: and names the 16-client row as the comparison", True,
             "The 16-client run (%s.agg7d.b100.c16) is the comparison" % v in row)
        case("many: the 32-client row stays a failure", True, "no run timed" in row)
        case("many: the 16-client row is in the table (p6:)", True,
             any(l.startswith("| p6: %s.agg7d.b100.c16 " % v) and "10.7 q/s" in l for l in text))
        case("many: a 32-client class that worked gets no note", True,
             not any("comparison" in l for l in text if l.startswith("| %s.point.b100.c32 " % v)))
        case("many: a failed 32-client class without a 16-client result gets no note", True,
             not any("comparison" in l for l in text if l.startswith("| %s.raw.b100.c32 " % v)))
        os.remove(os.path.join(d, "memsweep", "REFUSED.txt"))
        text = "\n".join(r if isinstance(r, str) else r[1] for r in tables.results_table(d))
        case("many: a limit that no REFUSED line names is said not to be recorded", True, "limit not recorded" in text)
        case("many: tables.py has no error for any of these", True, not tables.errors)
        t = os.path.join(d, "arch", "bench", "dbcmp", "release")
        os.makedirs(t)
        open(os.path.join(d, "arch", "COMMIT"), "w").write("0123456789abcdef0123456789abcdef01234567 main\n")
        case("many: how.py reads the commit of a git archive tree from COMMIT", True, how.commit_of(t) == "0123456")
        case("many: no repository and no COMMIT file gives no commit", True, how.commit_of(d) == "")
    finally:
        shutil.rmtree(d)
        tables.errors.clear()

def main():
    a = sys.argv[1:]
    if not a or a[0] != "mutate": raise SystemExit("usage: checktest.py mutate [TWA_REF WINDOWS_REF FULL_REF]")
    if len(a) >= 2 and os.path.exists(a[1]):
        ref = rel.load_ref(a[1])
        print(f"TWA reference {a[1]}: {len(ref)} series, {sum(len(e['b']) for e in ref.values())} buckets")
    else:
        ref = builtin_twa(); print("TWA reference: built in")
    twa_cases(ref)
    decimal_cases()
    lttb_cases()
    batch_cases()
    exit_cases(ref)
    refused_cases()
    tables_cases()
    many_cases()
    if len(a) >= 4:
        windows_cases(a[2], a[3])
    print("checktest", "OK" if failures == 0 else f"FAILED ({failures} cases)")
    sys.exit(failures != 0)

if __name__ == "__main__":
    main()
