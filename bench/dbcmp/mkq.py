# Writes the queries of spec.tsv in one system's dialect: q/SYS.<class>.txt with one query per line
# (the input of qdrive and clickhouse-benchmark) and q/SYS.full{0,3600,60}.txt for the whole-range
# queries. Run in $WORK:  python3 -I mkq.py SYS
#
# SYS: ch, chday (ClickHouse, monthly / daily partitions), ts (TimescaleDB), qdb (QuestDB, PostgreSQL
# wire), qdbh (QuestDB, HTTP /exec), i2ql (InfluxDB 2, InfluxQL), i2flux (InfluxDB 2, Flux), i3 (InfluxDB 3).
#
# Every system answers the same logical query: rows in [start, end) of one series, and buckets
# aligned to the epoch whose width is a number of the series' own samples. For HTTP systems a line
# is a request ("GET path" or "POST path<TAB>content-type<TAB>body"); for the others it is SQL.
# Whole-range queries name their columns series, [b,] c, s, mn, mx so check.py can wrap them.
import datetime, sys, urllib.parse

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

GEN = {"i3": lambda *x: i3_get(i3(*x)), "i2ql": lambda *x: i2ql_get(influxql(*x)), "i2flux": lambda *x: flux_post(flux(*x)), "qdb": qdb, "qdbh": lambda *x: http_get("/exec?query=", qdb(*x)), "ts": ts, "ch": ch, "chday": lambda *x: ch(*x, table="cmp.t100m_day")}

# ---- whole-range queries (a: one bucket per series, b: buckets of n samples)
def step_sql(col, kind):  # the sample step of a series, as SQL (kind = series % 7)
    if kind == "ch":
        return f"multiIf({col} % 7 = 0, 60000, {col} % 7 = 4, 1, {col} % 7 IN (2, 3), 10000, 1000)"
    return f"(CASE {col} % 7 WHEN 0 THEN 60000 WHEN 4 THEN 1 WHEN 2 THEN 10000 WHEN 3 THEN 10000 ELSE 1000 END)"

def ch_full(n, table="cmp.t100m"):
    if n == 0:
        return f"SELECT series, count() AS c, sum(value) AS s, min(value) AS mn, max(value) AS mx FROM {table} GROUP BY series ORDER BY series"
    return (f"SELECT series, intDiv(toUnixTimestamp64Milli(ts), {step_sql('series', 'ch')} * {n}) AS b, count() AS c, sum(value) AS s, "
            f"min(value) AS mn, max(value) AS mx FROM {table} GROUP BY series, b ORDER BY series, b")

GROUPS = {60000: [0], 1000: [1, 5, 6], 10000: [2, 3], 1: [4]}  # step -> series % 7

def series_of(kinds):
    return ", ".join(str(x) for x in range(100) if x % 7 in kinds)

def ts_full(n):
    if n:  # one branch per step, so each time_bucket has a constant width: a width computed per row
           # (ts_full_case) ran 5x slower on compressed chunks
        return " UNION ALL ".join(
            f"(SELECT series, time_bucket(INTERVAL '{st * n} milliseconds', ts, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS b, count(*) AS c, "
            f"sum(value::float8) AS s, min(value) AS mn, max(value) AS mx FROM t100m WHERE series IN ({series_of(k)}) GROUP BY series, b)"
            for st, k in GROUPS.items()) + " ORDER BY series, b"
    return ts_full_case(n)

def ts_full_case(n):  # kept for comparison; mkq writes ts_full
    if n == 0:
        return "SELECT series, count(*) AS c, sum(value::float8) AS s, min(value) AS mn, max(value) AS mx FROM t100m GROUP BY series ORDER BY series"
    return (f"SELECT series, time_bucket({step_sql('series', 'pg')} * {n} * INTERVAL '1 millisecond', ts, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS b, "
            f"count(*) AS c, sum(value::float8) AS s, min(value) AS mn, max(value) AS mx FROM t100m GROUP BY series, b ORDER BY series, b")

def qdb_full(n):
    if n == 0:
        return "SELECT series, count() c, sum(cast(value AS double)) s, min(value) mn, max(value) mx FROM sensors GROUP BY series ORDER BY series"
    return "SELECT * FROM (" + " UNION ALL ".join(
        f"(SELECT series, ts, count() c, sum(cast(value AS double)) s, min(value) mn, max(value) mx FROM sensors "
        f"WHERE series IN ({', '.join(repr(str(x)) for x in range(100) if x % 7 in k)}) SAMPLE BY {st * n}T)"
        for st, k in GROUPS.items()) + ") ORDER BY series, ts"

def influxql_full(n):
    if n == 0:
        return 'SELECT count(value), sum(value), min(value), max(value) FROM sensors GROUP BY "series"'
    # GROUP BY time() needs a time range (the first jittered sample is 5 ms before 2024) and a
    # constant interval: one statement per step, all in one request
    return "; ".join(
        f"SELECT count(value), sum(value), min(value), max(value) FROM sensors WHERE ({' OR '.join(f'{chr(34)}series{chr(34)} = {chr(39)}{x}{chr(39)}' for x in range(100) if x % 7 in k)}) "
        f"AND time >= '2023-12-31T00:00:00Z' AND time < '2026-01-01T00:00:00Z' GROUP BY time({st * n}ms), \"series\" fill(none)"
        for st, k in GROUPS.items())

def flux_full(n):  # independent pipelines again, so every aggregate is pushed down
    out = []
    groups = {0: list(range(7))} if n == 0 else {st: k for st, k in GROUPS.items()}
    for st, k in groups.items():
        f = "true" if n == 0 else " or ".join(f'r.series == "{x}"' for x in range(100) if x % 7 in k)
        src = FLUX_SRC.format(a="2023-12-31T00:00:00Z", b="2026-01-01T00:00:00Z", f=f)
        for fn in ("count", "sum", "min", "max"):
            if n == 0:
                out.append(f'{src} |> {fn}() |> yield(name: "{fn}")')
            else:
                out.append(f'{src} |> aggregateWindow(every: {st * n}ms, fn: {fn}, createEmpty: false, timeSrc: "_start") |> yield(name: "{fn}{st}")')
    return "\n".join(out)

def i3_full(n):  # date_bin needs a constant stride, hence one branch per step
    if n == 0:
        return "SELECT series, count(value) AS c, sum(value) AS s, min(value) AS mn, max(value) AS mx FROM sensors GROUP BY series ORDER BY series"
    return " UNION ALL ".join(
        f"(SELECT series, date_bin(INTERVAL '{st * n} milliseconds', time) AS b, count(value) AS c, sum(value) AS s, min(value) AS mn, "
        f"max(value) AS mx FROM sensors WHERE series IN ({', '.join(repr(str(x)) for x in range(100) if x % 7 in k)}) GROUP BY series, b)"
        for st, k in GROUPS.items()) + " ORDER BY series, b"

FULL = {"i3": lambda n: i3_get(i3_full(n)), "i2ql": lambda n: i2ql_get(influxql_full(n)), "i2flux": lambda n: flux_post(flux_full(n)), "qdb": qdb_full, "qdbh": lambda n: http_get("/exec?query=", qdb_full(n)), "ts": ts_full, "ch": ch_full, "chday": lambda n: ch_full(n, "cmp.t100m_day")}

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
