# Writes the queries of spec.tsv in one system's dialect: q/SYS.<class>.txt with one query per line
# (the input of qdrive and clickhouse-benchmark) and q/SYS.full{0,3600,60}.txt for the whole-range
# queries. Run in $WORK:  python3 -I mkq.py SYS
#
# SYS: ch, chday (ClickHouse, monthly / daily partitions), ts (TimescaleDB), qdb (QuestDB, PostgreSQL
# wire), qdbh (QuestDB, HTTP /exec), i2ql (InfluxDB 2, InfluxQL), i2flux (InfluxDB 2, Flux), i3 (InfluxDB 3),
# prom (Prometheus, PromQL; vm: VictoriaMetrics, the same PromQL), iot (IoTDB, table model SQL over REST), td (TDengine, SQL over taosAdapter's REST).
#
# Every system answers the same logical query: rows in [start, end) of one series, and buckets
# aligned to the epoch whose width is a number of the series' own samples. For HTTP systems a line
# is a request ("GET path" or "POST path<TAB>content-type<TAB>body", or several joined by " && ");
# for the others it is SQL.
# Whole-range queries name their columns series, [b,] c, s, mn, mx so check.py can wrap them.
# NSERIES (100) is the number of series, as for spec.py.
import datetime, os, sys, urllib.parse

NSERIES = int(os.environ.get("NSERIES", "100"))

def iso(ms):  # RFC 3339 UTC with milliseconds
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"

def ch(cls, s, a, b, bk, table="cmp.t100m"):
    w = f"series = {s} AND ts >= fromUnixTimestamp64Milli(toInt64({a})) AND ts < fromUnixTimestamp64Milli(toInt64({b}))"
    if cls == "raw":
        return f"SELECT ts, value FROM {table} WHERE {w} ORDER BY ts"
    if cls == "point":
        return f"SELECT ts, value FROM {table} WHERE series = {s} AND ts = fromUnixTimestamp64Milli(toInt64({a}))"
    return (f"SELECT intDiv(toUnixTimestamp64Milli(ts), {bk}) * {bk} AS b, count(), sum(value), min(value), max(value) "
            f"FROM {table} WHERE {w} GROUP BY b ORDER BY b")

def pgts(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S.") + f"{ms % 1000:03d}+00"

def ts(cls, s, a, b, bk):
    w = f"series = {s} AND ts >= '{pgts(a)}' AND ts < '{pgts(b)}'"
    if cls == "raw":
        return f"SELECT ts, value FROM t100m WHERE {w} ORDER BY ts"
    if cls == "point":
        return f"SELECT ts, value FROM t100m WHERE series = {s} AND ts = '{pgts(a)}'"
    return (f"SELECT time_bucket(INTERVAL '{bk} milliseconds', ts, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS b, count(*), "
            f"sum(value::float8), min(value), max(value) FROM t100m WHERE {w} GROUP BY b ORDER BY b")

def qdb(cls, s, a, b, bk):
    w = f"series = '{s}' AND ts >= '{iso(a)}' AND ts < '{iso(b)}'"
    if cls == "raw":
        return f"SELECT ts, value FROM sensors WHERE {w} ORDER BY ts"
    if cls == "point":
        return f"SELECT ts, value FROM sensors WHERE series = '{s}' AND ts = '{iso(a)}'"
    return (f"SELECT ts, count() c, sum(cast(value AS double)) s, min(value) mn, max(value) mx FROM sensors WHERE {w} "
            f"SAMPLE BY {bk}T")

def http_get(path, q):
    return "GET " + path + urllib.parse.quote(q, safe="")

def influxql(cls, s, a, b, bk):
    w = f"\"series\" = '{s}' AND time >= '{iso(a)}' AND time < '{iso(b)}'"  # series is an InfluxQL keyword
    if cls == "raw":
        q = f"SELECT value FROM sensors WHERE {w}"
    elif cls == "point":
        q = f"SELECT value FROM sensors WHERE \"series\" = '{s}' AND time = '{iso(a)}'"
    else:
        q = f"SELECT count(value), sum(value), min(value), max(value) FROM sensors WHERE {w} GROUP BY time({bk}ms) fill(none)"
    return q

def i2ql_get(q):
    return "GET /query?db=cmp&epoch=ms&q=" + urllib.parse.quote(q, safe="")

FLUX_SRC = 'from(bucket: "cmp") |> range(start: {a}, stop: {b}) |> filter(fn: (r) => r._measurement == "sensors" and r._field == "value" and ({f}))'

def flux(cls, s, a, b, bk):
    src = FLUX_SRC.format(a=iso(a), b=iso(b), f=f'r.series == "{s}"')
    if cls in ("raw", "point"):
        return src
    # one complete pipeline per aggregate: a source shared by several yields is not pushed down to storage
    return "\n".join(
        f'{src} |> aggregateWindow(every: {bk}ms, fn: {fn}, createEmpty: false, timeSrc: "_start") |> yield(name: "{fn}")'
        for fn in ("count", "sum", "min", "max"))

def flux_post(q):
    import json
    return "POST /api/v2/query?org=bench\tapplication/json\t" + json.dumps({"query": q, "type": "flux", "dialect": {"annotations": []}})

def i3(cls, s, a, b, bk):
    w = f"series = '{s}' AND time >= '{iso(a)}' AND time < '{iso(b)}'"
    if cls == "raw":
        return f"SELECT time, value FROM sensors WHERE {w} ORDER BY time"
    if cls == "point":
        return f"SELECT time, value FROM sensors WHERE series = '{s}' AND time = '{iso(a)}'"
    return (f"SELECT date_bin(INTERVAL '{bk} milliseconds', time) AS b, count(value), sum(value), min(value), max(value) "
            f"FROM sensors WHERE {w} GROUP BY b ORDER BY b")

def i3_get(q):
    return "GET /api/v3/query_sql?db=cmp&format=csv&q=" + urllib.parse.quote(q, safe="")


# IoTDB 2.0, table model (table sensors: time, series STRING TAG, value FLOAT FIELD), SQL over its REST API
# (POST /rest/table/v1/query, JSON {"database", "sql"}); times as epoch milliseconds, date_bin buckets from the epoch.
IOT_PATH = "/rest/table/v1/query"

def iot(cls, s, a, b, bk):
    w = f"series = '{s}' AND time >= {a} AND time < {b}"
    if cls == "raw":
        return f"SELECT time, value FROM sensors WHERE {w} ORDER BY time"
    if cls == "point":
        return f"SELECT time, value FROM sensors WHERE series = '{s}' AND time = {a}"
    return (f"SELECT date_bin({bk}ms, time) AS b, count(value) AS c, sum(value) AS s, min(value) AS mn, max(value) AS mx "
            f"FROM sensors WHERE {w} GROUP BY b ORDER BY b")

def iot_post(q):
    import json
    return f"POST {IOT_PATH}\tapplication/json\t" + json.dumps({"database": "cmp", "sql": q}, separators=(",", ":"))

# TDengine 3.4: the supertable sensors that schemaless line protocol made (_ts, value FLOAT, tag series NCHAR; the
# tag name is a keyword, hence the backquotes), SQL over taosAdapter's REST API (POST /rest/sql/cmp, the SQL as the
# body). Times as epoch milliseconds (the database's precision); INTERVAL in milliseconds ("a") is aligned to the
# epoch and leaves empty windows out (no FILL). Timestamps are cast to BIGINT so the answers carry numbers.
TD_PATH = "/rest/sql/cmp"

def td(cls, s, a, b, bk):
    w = f"`series` = '{s}' AND _ts >= {a} AND _ts < {b}"
    if cls == "raw":
        return f"SELECT CAST(_ts AS BIGINT) t, value FROM sensors WHERE {w} ORDER BY _ts"
    if cls == "point":
        return f"SELECT CAST(_ts AS BIGINT) t, value FROM sensors WHERE `series` = '{s}' AND _ts = {a}"
    return (f"SELECT CAST(_wstart AS BIGINT) b, count(value) c, sum(value) s, min(value) mn, max(value) mx "
            f"FROM sensors WHERE {w} INTERVAL({bk}a)")

def td_post(q):
    return f"POST {TD_PATH}\ttext/plain\t{q}"

# Prometheus: PromQL over the HTTP API. A range selector m[r] evaluated at t covers (t - r, t], so the
# rows in [a, b) are m[b - a] at t = b - 1 ms, and a bucket [k*w, (k+1)*w) is evaluated at (k+1)*w - 1
# with range w; range selectors return the stored samples, so lookback and staleness play no part.
# Whole buckets of a window are one query_range (step w); the partial buckets at its ends, where
# the window starts or ends inside a bucket, are clipped with their own ranges at fixed times
# (@) in one instant query: the line holds both requests, joined by " && ". The four aggregates
# are one expression, told apart by a label fn (and b, the bucket start, for the clipped ones).
PROM_T0, PROM_POINTS = 1704067200000, int(os.environ.get("POINTS", "1000000"))  # spec.py's origin, samples per series

def psec(ms):  # exact milliseconds as the API's float seconds
    return f"{ms // 1000}.{ms % 1000:03d}"

def prom_get(path, **params):
    return "GET " + path + "?" + "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items())

VM_MAXSTEPS = 30000  # VictoriaMetrics' -search.maxPointsPerTimeseries default; Prometheus allows 11,000 steps

def prom_aggs(sel, rng, at=None, b=None, vm=False):
    if vm:  # MetricsQL computes the listed rollups in one pass over the samples; label "rollup" names each
        at_ = " @ " + psec(at) if at is not None else ""
        e = (f'label_replace(aggr_over_time(("count_over_time", "sum_over_time", "min_over_time", "max_over_time"), '
             f'{sel}[{rng}ms]{at_}), "fn", "$1", "rollup", "(.+)_over_time")')
        return e if b is None else f'label_replace({e}, "b", "{b}", "", "")'
    out = []
    for fn in ("count", "sum", "min", "max"):
        e = f'label_replace({fn}_over_time({sel}[{rng}ms]{" @ " + psec(at) if at is not None else ""}), "fn", "{fn}", "", "")'
        out.append(e if b is None else f'label_replace({e}, "b", "{b}", "", "")')
    return " or ".join(out)

def prom_buckets(sel, lo, hi, w, vm=False):
    """query_range requests for the whole buckets of width w in [lo, hi) (lo and hi multiples of w),
    in pieces of at most maxsteps steps, the system's limit per series."""
    maxsteps = VM_MAXSTEPS if vm else 11000
    reqs = []
    for x in range(lo, hi, w * maxsteps):
        y = min(hi, x + w * maxsteps)
        reqs.append(prom_get("/api/v1/query_range", query=prom_aggs(sel, w, vm=vm), start=psec(x + w - 1), end=psec(y - 1), step=f"{w}ms"))
    return reqs

def prom(cls, s, a, b, bk, vm=False):
    sel = f'sensors{{series="{s}"}}'
    if cls == "raw":
        return prom_get("/api/v1/query", query=f"{sel}[{b - a}ms]", time=psec(b - 1))
    if cls == "point":
        return prom_get("/api/v1/query", query=f"{sel}[1ms]", time=psec(a))
    w = bk
    first, last = -(-a // w) * w, b // w * w  # the first and last bucket boundaries inside [a, b]
    reqs = prom_buckets(sel, first, last, w, vm) if first < last else []
    edges = []
    if a < first or first > last:  # [a, first) or, inside one bucket, [a, b)
        e = min(first, b)
        edges.append(prom_aggs(sel, e - a, e - 1, a // w * w, vm))
    if first <= last < b:
        edges.append(prom_aggs(sel, b - last, b - 1, last, vm))
    if edges:
        reqs.append(prom_get("/api/v1/query", query=" or ".join(edges), time=psec(b - 1)))
    return " && ".join(reqs)

GEN = {"i3": lambda *x: i3_get(i3(*x)), "i2ql": lambda *x: i2ql_get(influxql(*x)), "i2flux": lambda *x: flux_post(flux(*x)), "qdb": qdb, "qdbh": lambda *x: http_get("/exec?query=", qdb(*x)), "ts": ts, "ch": ch, "chday": lambda *x: ch(*x, table="cmp.t100m_day"), "prom": prom, "vm": lambda *x: prom(*x, vm=True), "iot": lambda *x: iot_post(iot(*x)), "td": lambda *x: td_post(td(*x))}

# ---- whole-range queries (a: one bucket per series, b: buckets of n samples)
def step_sql(col, kind):  # the sample step of a series, as SQL (kind = series % 7)
    if kind == "ch":
        return f"multiIf({col} % 7 = 0, 60000, {col} % 7 = 4, 1, {col} % 7 IN (2, 3), 10000, 1000)"
    return f"(CASE {col} % 7 WHEN 0 THEN 60000 WHEN 4 THEN 1 WHEN 2 THEN 10000 WHEN 3 THEN 10000 ELSE 1000 END)"

def ch_full(n, table="cmp.t100m"):
    if n == 0:
        return f"SELECT series, count() AS c, sum(value) AS s, min(value) AS mn, max(value) AS mx FROM {table}{where_set('series')} GROUP BY series ORDER BY series"
    return (f"SELECT series, intDiv(toUnixTimestamp64Milli(ts), {step_sql('series', 'ch')} * {n}) AS b, count() AS c, sum(value) AS s, "
            f"min(value) AS mn, max(value) AS mx FROM {table}{where_set('series')} GROUP BY series, b ORDER BY series, b")

GROUPS = {60000: [0], 1000: [1, 5, 6], 10000: [2, 3], 1: [4]}  # step -> series % 7

# Whole-range queries cover the series in SET (mkbatch.py sets it to one batch of series); None means all of them.
SET = None

def sset():
    return list(range(NSERIES)) if SET is None else SET

def groups():  # GROUPS, without the steps no series of SET has
    return {st: k for st, k in GROUPS.items() if any(x % 7 in k for x in sset())}

def where_set(col, quote=False, op="WHERE"):  # "" for all series, else a filter on SET
    if SET is None: return ""
    return f" {op} {col} IN ({', '.join(repr(str(x)) if quote else str(x) for x in SET)})"

def series_of(kinds):
    return ", ".join(str(x) for x in sset() if x % 7 in kinds)

def ts_full(n):
    if n:  # one branch per step, so each time_bucket has a constant width: a width computed per row
           # (ts_full_case) ran 5x slower on compressed chunks
        return " UNION ALL ".join(
            f"(SELECT series, time_bucket(INTERVAL '{st * n} milliseconds', ts, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS b, count(*) AS c, "
            f"sum(value::float8) AS s, min(value) AS mn, max(value) AS mx FROM t100m WHERE series IN ({series_of(k)}) GROUP BY series, b)"
            for st, k in groups().items()) + " ORDER BY series, b"
    return ts_full_case(n)

def ts_full_case(n):  # kept for comparison; mkq writes ts_full
    if n == 0:
        return f"SELECT series, count(*) AS c, sum(value::float8) AS s, min(value) AS mn, max(value) AS mx FROM t100m{where_set('series')} GROUP BY series ORDER BY series"
    return (f"SELECT series, time_bucket({step_sql('series', 'pg')} * {n} * INTERVAL '1 millisecond', ts, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS b, "
            f"count(*) AS c, sum(value::float8) AS s, min(value) AS mn, max(value) AS mx FROM t100m GROUP BY series, b ORDER BY series, b")

def qdb_full(n):
    if n == 0:
        return f"SELECT series, count() c, sum(cast(value AS double)) s, min(value) mn, max(value) mx FROM sensors{where_set('series', True)} GROUP BY series ORDER BY series"
    return "SELECT * FROM (" + " UNION ALL ".join(
        f"(SELECT series, ts, count() c, sum(cast(value AS double)) s, min(value) mn, max(value) mx FROM sensors "
        f"WHERE series IN ({', '.join(repr(str(x)) for x in sset() if x % 7 in k)}) SAMPLE BY {st * n}T)"
        for st, k in groups().items()) + ") ORDER BY series, ts"

def influxql_full(n):
    if n == 0:
        w = "" if SET is None else " WHERE (" + " OR ".join(f'"series" = {chr(39)}{x}{chr(39)}' for x in SET) + ")"
        return f'SELECT count(value), sum(value), min(value), max(value) FROM sensors{w} GROUP BY "series"'
    # GROUP BY time() needs a time range (the first jittered sample is up to 50 ms before 2024) and a
    # constant interval: one statement per step, all in one request
    return "; ".join(
        f"SELECT count(value), sum(value), min(value), max(value) FROM sensors WHERE ({' OR '.join(f'{chr(34)}series{chr(34)} = {chr(39)}{x}{chr(39)}' for x in sset() if x % 7 in k)}) "
        f"AND time >= '2023-12-31T00:00:00Z' AND time < '2026-01-01T00:00:00Z' GROUP BY time({st * n}ms), \"series\" fill(none)"
        for st, k in groups().items())

def flux_full(n):  # independent pipelines again, so every aggregate is pushed down
    out = []
    gs = {0: list(range(7))} if n == 0 else groups()
    for st, k in gs.items():
        f = "true" if n == 0 and SET is None else " or ".join(f'r.series == "{x}"' for x in sset() if x % 7 in k)
        src = FLUX_SRC.format(a="2023-12-31T00:00:00Z", b="2026-01-01T00:00:00Z", f=f)
        for fn in ("count", "sum", "min", "max"):
            if n == 0:
                out.append(f'{src} |> {fn}() |> yield(name: "{fn}")')
            else:
                out.append(f'{src} |> aggregateWindow(every: {st * n}ms, fn: {fn}, createEmpty: false, timeSrc: "_start") |> yield(name: "{fn}{st}")')
    return "\n".join(out)

def i3_full(n):  # date_bin needs a constant stride, hence one branch per step
    if n == 0:
        return f"SELECT series, count(value) AS c, sum(value) AS s, min(value) AS mn, max(value) AS mx FROM sensors{where_set('series', True)} GROUP BY series ORDER BY series"
    return " UNION ALL ".join(
        f"(SELECT series, date_bin(INTERVAL '{st * n} milliseconds', time) AS b, count(value) AS c, sum(value) AS s, min(value) AS mn, "
        f"max(value) AS mx FROM sensors WHERE series IN ({', '.join(repr(str(x)) for x in sset() if x % 7 in k)}) GROUP BY series, b)"
        for st, k in groups().items()) + " ORDER BY series, b"

def iot_full(n):  # date_bin takes a constant interval: one branch per step, as for InfluxDB 3
    if n == 0:
        return f"SELECT series, count(value) AS c, sum(value) AS s, min(value) AS mn, max(value) AS mx FROM sensors{where_set('series', True)} GROUP BY series ORDER BY series"
    return "SELECT * FROM (" + " UNION ALL ".join(
        f"(SELECT series, date_bin({st * n}ms, time) AS b, count(value) AS c, sum(value) AS s, min(value) AS mn, "
        f"max(value) AS mx FROM sensors WHERE series IN ({', '.join(repr(str(x)) for x in sset() if x % 7 in k)}) GROUP BY series, b)"
        for st, k in groups().items()) + ") x ORDER BY series, b"


def td_full(n):  # INTERVAL takes a constant width: one PARTITION BY branch per step, joined by UNION ALL
    if n == 0:
        return (f"SELECT `series`, count(value) c, sum(value) s, min(value) mn, max(value) mx FROM sensors"
                f"{where_set('`series`', True)} PARTITION BY `series`")
    return " UNION ALL ".join(
        f"SELECT `series`, CAST(_wstart AS BIGINT) b, count(value) c, sum(value) s, min(value) mn, max(value) mx "
        f"FROM sensors WHERE `series` IN ({', '.join(repr(str(x)) for x in sset() if x % 7 in k)}) PARTITION BY `series` "
        f"INTERVAL({st * n}a)" for st, k in groups().items())


PROM_LO, PROM_HI = 1703980800000, 1767225600000  # 2023-12-31 and 2026-01-01, as for InfluxQL

def prom_full(n, vm=False):
    if n == 0:  # one instant query over everything
        sel = "sensors" if SET is None else 'sensors{series=~"' + "|".join(str(x) for x in SET) + '"}'
        return prom_get("/api/v1/query", query=prom_aggs(sel, PROM_HI - PROM_LO, vm=vm), time=psec(PROM_HI - 1))
    # query_range evaluates every step, empty or not, so each step group spans only its own data
    # (spec.py's samples, with a margin of 1 s for the jitter, which is at most 50 ms) instead of the two years
    reqs = []
    for st, k in groups().items():
        w = st * n
        sel = 'sensors{series=~"' + "|".join(str(x) for x in sset() if x % 7 in k) + '"}'
        lo, hi = (PROM_T0 - 1000) // w * w, -(-(PROM_T0 + PROM_POINTS * st + 1000) // w) * w
        reqs += prom_buckets(sel, lo, hi, w, vm)
    return " && ".join(reqs)

FULL = {"i3": lambda n: i3_get(i3_full(n)), "i2ql": lambda n: i2ql_get(influxql_full(n)), "i2flux": lambda n: flux_post(flux_full(n)), "qdb": qdb_full, "qdbh": lambda n: http_get("/exec?query=", qdb_full(n)), "ts": ts_full, "ch": ch_full, "chday": lambda n: ch_full(n, "cmp.t100m_day"), "prom": prom_full, "vm": lambda n: prom_full(n, vm=True), "iot": lambda n: iot_post(iot_full(n)), "td": lambda n: td_post(td_full(n))}

if __name__ == "__main__":
    import os
    sysname = sys.argv[1]
    os.makedirs("q", exist_ok=True)
    out = {}
    for line in open("spec.tsv"):
        cls, s, a, b, bk = line.split()
        out.setdefault(cls, []).append(GEN[sysname](cls, int(s), int(a), int(b), int(bk)))
    for cls, qs in out.items():
        with open(f"q/{sysname}.{cls}.txt", "w") as f:
            f.write("\n".join(qs) + "\n")

    for n in (0, 3600, 60):
        q = FULL[sysname](n)
        with open(f"q/{sysname}.full{n}.txt", "w") as f:
            f.write(q + "\n")
