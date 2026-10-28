# checktest.py: the answer checkers (rel.py compare, check.py --dumps) must reject wrong answers and accept right ones.
#   python3 -I checktest.py mutate [TWA_REF WINDOWS_REF FULL_REF]
# Takes reference answers (tsdb's relq twa / tsdbq dump / tsdbq full dumps of the crafted data set; without files, a
# small built-in reference with a gap, a single-reading series and a bucket with a carried-in piece), writes the
# true answers back in each system's row form and checks they pass, then one mutation at a time that must fail:
# NaN, covered time off by one ms, a missing bucket, an extra bucket, a value off by 1.5e-9 relative (just over the
# 1e-9 tolerance), a missing and an extra series, a count off by one. Prints one line per case and "checktest OK"
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
    if len(a) >= 4:
        windows_cases(a[2], a[3])
    print("checktest", "OK" if failures == 0 else f"FAILED ({failures} cases)")
    sys.exit(failures != 0)

if __name__ == "__main__":
    main()
