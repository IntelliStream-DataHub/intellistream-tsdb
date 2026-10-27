# rel.py: LTTB and time-weighted averages (TWA) over the whole range, every series, in each system's dialect,
# and the check of their answers against tsdb's (relq lttb/twa ... OUT -> ref/lttb.jsonl, ref/twa.jsonl).
#
#   python3 -I rel.py q SYS KIND          the query, one line (SQL; for i3 the qdrive "GET ..." line)
#   python3 -I rel.py sql SYS KIND        the SQL itself (i3: without the HTTP wrapping)
#   python3 -I rel.py check SYS KIND REF  run it on the server (ports of bench/dbcmp/README.md) and compare
#   python3 -I rel.py tsv KIND REF FILE   compare a TSV answer file (embq/chq sqlt OUT) with REF
#   python3 -I rel.py jsonl KIND REF FILE compare another relq/relemb dump with REF
# KIND: lttb (1000 points per series; rows series, t_ms, value) or twa (STEP method, buckets of one hour aligned
# to the epoch, the value carried over bucket edges; rows series, bucket_ms, area, covered_ms, or series,
# bucket_ms, twa for the TimescaleDB toolkit). SYS: ch, ts (TimescaleDB toolkit, the -ha image), qdb, i3,
# duck (DuckDB, embedded), chdb (embedded ClickHouse; same SQL as ch on table t).
# Systems without a built-in function or a window-function recipe have none here ("-" in the table):
# LTTB is sequential (each bucket's pick depends on the previous pick), so it has no window-function recipe;
# InfluxDB 2 (Flux timeWeightedAvg interpolates linearly only) and Prometheus have no STEP TWA.
#
# Check: LTTB the same timestamps per series and values within 1e-7 relative; TWA per bucket with covered
# time > 0 the same bucket starts, the covered time exactly (where the system returns it) and twa within 1e-9
# relative. NaN, a missing or an extra bucket or series is a mismatch. Only the TimescaleDB toolkit may differ in
# the last bucket of a series (its interpolated_average holds the last value to the bucket end): counted apart.
import json, os, subprocess, sys, urllib.parse, urllib.request

H = 3600000

def q_ch(kind, table="cmp.t100m"):
    if kind == "lttb":
        # x as ms since 2024-01-01 (the data's start), as tsdb takes ms since the first point: with absolute ms
        # (1.7e12) the triangle areas lose precision and 92 of 1000 points of a flow_total series differed (smoke test)
        return (f"SELECT series, p.1 + 1704067200000 AS t, p.2 AS v FROM (SELECT series, arrayJoin(largestTriangleThreeBuckets(1000)"
                f"(toUnixTimestamp64Milli(ts) - 1704067200000, value)) AS p FROM {table} GROUP BY series) ORDER BY series, t")
    # segments from leadInFrame, split at hour edges with ARRAY JOIN, weighted by their length
    return ("SELECT series, b, sum(v * toFloat64(least(t1, b + 3600000) - greatest(t0, b))) AS area, "
            "sum(least(t1, b + 3600000) - greatest(t0, b)) AS covered FROM ("
            "SELECT series, t0, assumeNotNull(t1n) AS t1, v FROM ("
            "SELECT series, toUnixTimestamp64Milli(ts) AS t0, toFloat64(value) AS v, "
            "leadInFrame(toNullable(toUnixTimestamp64Milli(ts))) OVER (PARTITION BY series ORDER BY ts "
            f"ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS t1n FROM {table}) WHERE t1n IS NOT NULL AND t0 < t1n) "
            "ARRAY JOIN range(intDiv(t0, 3600000) * 3600000, t1, 3600000) AS b GROUP BY series, b ORDER BY series, b")

def q_ts(kind):
    if kind == "lttb":
        return ("SELECT x.series, (extract(epoch FROM u.time) * 1000)::bigint AS t, u.value FROM (SELECT series, "
                "lttb(ts, value::float8, 1000) AS tv FROM t100m GROUP BY series) x CROSS JOIN LATERAL unnest(x.tv) u "
                "ORDER BY 1, 2")
    return ("SELECT series, (extract(epoch FROM bucket) * 1000)::bigint AS b, interpolated_average(tw, bucket, "
            "'1 hour', lag(tw) OVER w, lead(tw) OVER w) AS twa FROM (SELECT series, time_bucket('1 hour', ts) AS bucket, "
            "time_weight('LOCF', ts, value::float8) AS tw FROM t100m GROUP BY series, bucket) x "
            "WINDOW w AS (PARTITION BY series ORDER BY bucket) ORDER BY series, bucket")

def q_qdb(kind):
    if kind == "lttb":
        return None
    # no ARRAY JOIN / unnest: a segment is split at most once (true here: the longest step is 60 s + jitter)
    seg = ("SELECT series, cast(value AS double) v, cast(ts AS long) / 1000 t0, "
           "cast(lead(ts) OVER (PARTITION BY series ORDER BY ts) AS long) / 1000 t1 FROM sensors")
    return (f"WITH seg AS ({seg}) SELECT series, b, sum(a) area, sum(c) covered FROM ("
            "SELECT series, t0 / 3600000 * 3600000 b, v * (least(t1, t0 / 3600000 * 3600000 + 3600000) - t0) a, "
            "least(t1, t0 / 3600000 * 3600000 + 3600000) - t0 c FROM seg WHERE t1 IS NOT NULL "
            "UNION ALL SELECT series, t1 / 3600000 * 3600000 b, v * (t1 - t1 / 3600000 * 3600000) a, "
            "t1 - t1 / 3600000 * 3600000 c FROM seg WHERE t1 IS NOT NULL AND t1 / 3600000 > t0 / 3600000) "
            "WHERE c > 0 ORDER BY series, b")

def q_i3(kind):
    if kind == "lttb":
        return None
    return ("WITH seg AS (SELECT series, CAST(value AS DOUBLE) AS v, CAST(time AS BIGINT) / 1000000 AS t0, "
            "CAST(lead(time) OVER (PARTITION BY series ORDER BY time) AS BIGINT) / 1000000 AS t1 FROM sensors), "
            "parts AS (SELECT series, v, t0, t1, unnest(range(t0 / 3600000 * 3600000, t1, 3600000)) AS b FROM seg "
            "WHERE t1 IS NOT NULL) SELECT series, b, sum(v * CAST(least(t1, b + 3600000) - greatest(t0, b) AS DOUBLE)) AS area, "
            "sum(least(t1, b + 3600000) - greatest(t0, b)) AS covered FROM parts GROUP BY series, b ORDER BY series, b")

def q_duck(kind):
    if kind == "lttb":
        return None
    return ("WITH seg AS (SELECT series, CAST(value AS DOUBLE) AS v, epoch_ms(ts) AS t0, "
            "epoch_ms(lead(ts) OVER (PARTITION BY series ORDER BY ts)) AS t1 FROM t), "
            "parts AS (SELECT series, v, t0, t1, unnest(range(t0 // 3600000 * 3600000, t1, 3600000)) AS b FROM seg "
            "WHERE t1 IS NOT NULL) SELECT series, b, sum(v * (least(t1, b + 3600000) - greatest(t0, b))) AS area, "
            "sum(least(t1, b + 3600000) - greatest(t0, b)) AS covered FROM parts GROUP BY series, b ORDER BY series, b")

Q = {"ch": q_ch, "ts": q_ts, "qdb": q_qdb, "i3": q_i3, "duck": q_duck, "chdb": lambda k: q_ch(k, "t")}

def http(url, data=None):
    req = urllib.request.Request(url, data=data.encode() if data else None)
    with urllib.request.urlopen(req, timeout=3600) as r:
        return r.read().decode()

def fetch(sysname, kind):
    sql = Q[sysname](kind)
    if sysname == "ch":
        out = http(f"http://127.0.0.1:{os.environ.get('CH_HTTP_PORT', '8123')}/", sql + " FORMAT TSV")
        return [l.split("\t") for l in out.splitlines() if l]
    if sysname == "ts":
        out = subprocess.run(["psql", "-h", "127.0.0.1", "-p", os.environ.get("TS_PORT", "5434"), "-U", "postgres", "-At",
                              "-F", "\t", "-c", sql], capture_output=True, text=True, check=True,
                             env={"PGPASSWORD": "bench", "PGTZ": "UTC", "PATH": os.environ.get("PATH", "/usr/bin:/bin")}).stdout
        return [l.split("\t") for l in out.splitlines() if l]
    if sysname == "qdb":
        out = json.loads(http(f"http://127.0.0.1:{os.environ.get('QDB_HTTP_PORT', '9100')}/exec?query=" + urllib.parse.quote(sql, safe="")))
        if "error" in out: raise RuntimeError(out["error"])
        return out["dataset"]
    if sysname == "i3":
        url = f"http://127.0.0.1:{os.environ.get('I3_PORT', '8181')}/api/v3/query_sql?db=cmp&format=json&q=" + urllib.parse.quote(sql, safe="")
        return [list(r.values()) for r in json.loads(http(url))]
    raise SystemExit("no fetch for " + sysname)

def load_ref(path):
    return {x["s"]: x for x in map(json.loads, open(path))}

def num(x):
    """A number from an answer cell; empty, null and NaN become NaN, which never matches."""
    if x in ("", None): return float("nan")
    return float(x)

def near(x, y, rel):
    """Within rel of each other; False for NaN on either side (abs(nan - y) > tol is False, so never test that way)."""
    return x == y or abs(x - y) <= rel * max(abs(x), abs(y), 1e-30)

def compare(kind, ref, rows, lenient_last=False):
    """rows: lists (series, t|b, v|area, [covered]); ref: relq dump. TWA rows with covered time compare it exactly;
    rows without it (series, b, twa: the TimescaleDB toolkit) compare the average only. A bucket missing on either
    side, an extra series, a NaN or a covered time off by one ms is a mismatch. lenient_last: the last bucket of a
    series may differ (the toolkit's interpolated_average holds the last value to the bucket end); counted apart."""
    got = {}
    for r in rows:
        got.setdefault(int(float(r[0])), []).append(r[1:])
    bad_series, bad_pts, bad_last, total = 0, 0, 0, 0
    for s, e in sorted(ref.items()):
        g = got.get(s, [])
        if kind == "lttb":
            et, ev = e["t"], e["v"]
            gt = [int(float(x[0])) for x in g]; gv = [num(x[1]) for x in g]
            total += len(et)
            nb = 0 if len(gt) == len(et) else abs(len(gt) - len(et))
            for a, b, x, y in zip(et, gt, ev, gv):
                if a != b or not near(x, y, 1e-7): nb += 1
            if nb:
                bad_series += 1; bad_pts += nb
                if bad_series <= 5: print(f"MISMATCH lttb series {s}: {nb} points differ (ref {len(et)}, got {len(gt)})")
        else:
            eb = {b[0]: (b[1] / b[2], b[2]) for b in e["b"] if b[2] > 0}
            last = max(eb) if eb else None
            if g and len(g[0]) == 2:  # series, b, twa
                gb = {int(float(x[0])): (num(x[1]), None) for x in g}
            else:
                gb = {}
                for x in g:
                    c = num(x[2])
                    if c == 0: continue  # a bucket with no covered time has no average; the reference leaves it out
                    gb[int(float(x[0]))] = (num(x[1]) / c if c == c else c, c)
            total += len(eb)
            nb = nl = 0
            for b in set(eb) | set(gb):
                x, y = eb.get(b), gb.get(b)
                ok = x is not None and y is not None and near(x[0], y[0], 1e-9) and (y[1] is None or y[1] == x[1])
                if not ok:
                    if lenient_last and (b == last or (x is None and b > (last or 0))): nl += 1
                    else:
                        nb += 1
                        if nb <= 3 and bad_series < 5: print(f"  series {s} bucket {b}: ref (twa, covered) {x}, got {y}")
            if nb or nl:
                bad_series += nb > 0; bad_pts += nb; bad_last += nl
                if nb and bad_series <= 5: print(f"MISMATCH twa series {s}: {nb} buckets differ, {nl} at the end (ref {len(eb)}, got {len(gb)})")
    extra = sorted(set(got) - set(ref))
    print(f"{kind}: {len(ref)} series, {total} reference {'points' if kind == 'lttb' else 'buckets'}; series with mismatches "
          f"{bad_series}, mismatched {bad_pts}, last-bucket differences {bad_last}{' (allowed: toolkit)' if lenient_last else ''}, "
          f"unexpected series {len(extra)}")
    return bad_series == 0 and not extra

def main():
    cmd = sys.argv[1]
    if cmd in ("q", "sql"):
        sysname, kind = sys.argv[2], sys.argv[3]
        s = Q[sysname](kind)
        if s is None: raise SystemExit(1)
        if cmd == "q" and sysname == "i3":
            s = "GET /api/v3/query_sql?db=cmp&format=csv&q=" + urllib.parse.quote(s, safe="")
        print(s)
    elif cmd == "check":
        sysname, kind, ref = sys.argv[2:5]
        ok = compare(kind, load_ref(ref), fetch(sysname, kind), lenient_last=sysname == "ts")
        print(sysname, kind, "OK" if ok else "MISMATCH")
    elif cmd == "tsv":
        kind, ref, f = sys.argv[2:5]
        ok = compare(kind, load_ref(ref), [l.rstrip("\n").split("\t") for l in open(f) if l.strip()])
        print(f, kind, "OK" if ok else "MISMATCH")
    elif cmd == "jsonl":
        kind, ref, f = sys.argv[2:5]
        rows = []
        for x in map(json.loads, open(f)):
            if kind == "lttb": rows += [[x["s"], t, v] for t, v in zip(x["t"], x["v"])]
            else: rows += [[x["s"], b[0], b[1], b[2]] for b in x["b"]]
        ok = compare(kind, load_ref(ref), rows)
        print(f, kind, "OK" if ok else "MISMATCH")

if __name__ == "__main__":
    main()
