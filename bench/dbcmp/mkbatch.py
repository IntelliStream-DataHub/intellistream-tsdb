# Writes the batches of batches.tsv in one system's dialect, one request per line (the input of qdrive and
# clickhouse-benchmark):  q/SYS.fullN.bK.txt (N = 0, 3600, 60) and q/SYS.CLASS.bK.txt (K = 1, 10, 100), and
# q/SYS.fullN.per.txt, the whole range as one query per series.
# Run in $WORK:  python3 -I mkbatch.py SYS      (SYS as for mkq.py)
#
# A batch is one request wherever the query language allows it, built from mkq.py's single queries so that
# every member is answered exactly as it is alone:
#   SQL (ch, chday, ts, qdb, qdbh, i3, iot, td) the members' queries as UNION ALL branches, each with its member number
#                                       as the first column ("mb"), so every branch still uses the primary key;
#   InfluxQL (i2ql)                     the statements joined by "; " in one request (statement_id = member);
#   Flux (i2flux)                       one script, each member's results yielded as m<member>_<name>;
#   PromQL (prom)                       PromQL has no batch form for different windows: the members' requests
#                                       one after another on one connection (qdrive's " && "), K requests.
# Whole-range batches are mkq.py's whole-range query restricted to the batch's series (mkq.SET); their
# answers carry the series, so they need no member column.
import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import batches, mkq

SQL = {"ch": (mkq.ch, None), "chday": (lambda *x: mkq.ch(*x, table="cmp.t100m_day"), None), "ts": (mkq.ts, None),
       "qdb": (mkq.qdb, None), "qdbh": (mkq.qdb, lambda q: mkq.http_get("/exec?query=", q)),
       "i3": (mkq.i3, mkq.i3_get), "iot": (mkq.iot, mkq.iot_post), "td": (mkq.td, mkq.td_post)}

def tag(m, q):  # the member number as the first column of a single SQL query
    assert q.startswith("SELECT ")
    return f"SELECT {m} AS mb, " + q[len("SELECT "):]

def sql_union(parts):
    if len(parts) == 1:
        return parts[0]
    return "SELECT * FROM (" + " UNION ALL ".join(f"({p})" for p in parts) + ") x"

def flux_member(m, q):
    if "|> yield(" not in q:  # raw and point: one pipeline
        return q + f' |> yield(name: "m{m}")'
    return q.replace('|> yield(name: "', f'|> yield(name: "m{m}_')

def window_line(sysname, members):
    """members: [(cls, series, start, end, bucket)] -> one request line"""
    if sysname in SQL:
        gen, wrap = SQL[sysname]
        q = sql_union([tag(m, gen(*x)) for m, x in enumerate(members)])
        return wrap(q) if wrap else q
    if sysname == "i2ql":
        return mkq.i2ql_get("; ".join(mkq.influxql(*x) for x in members))
    if sysname == "i2flux":
        return mkq.flux_post("\n".join(flux_member(m, mkq.flux(*x)) for m, x in enumerate(members)))
    if sysname == "prom":
        return " && ".join(mkq.prom(*x) for x in members)
    raise SystemExit(f"mkbatch: no batch form for {sysname}")

def full_line(sysname, n, series):
    mkq.SET = list(series)
    try:
        return mkq.FULL[sysname](n)
    finally:
        mkq.SET = None

def write(path, lines):
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

if __name__ == "__main__":
    sysname = sys.argv[1]
    os.makedirs("q", exist_ok=True)
    # the whole range as one query per series (REPORT §3: tsdb's 16 connections against 16 parallel per-series
    # queries); ClickHouse's also as HTTP lines for qdrive, tagged @TAG@ (run.sh puts in each run's log_comment)
    for n in (0, 3600, 60):
        per = [full_line(sysname, n, [x]) for x in range(mkq.NSERIES)]
        write(f"q/{sysname}.full{n}.per.txt", per)
        if sysname in ("ch", "chday"):
            write(f"q/{sysname}.full{n}.per.http", ["POST /?log_comment=@TAG@\ttext/plain\t" + q for q in per])
    for (st, k), grps in batches.read().items():
        if st == "full":
            for n in (0, 3600, 60):
                write(f"q/{sysname}.full{n}.b{k}.txt", [full_line(sysname, n, [x["series"] for x in g]) for g in grps])
        else:
            write(f"q/{sysname}.{st}.b{k}.txt",
                  [window_line(sysname, [(st, x["series"], x["start"], x["end"], x["bk"]) for x in g]) for g in grps])
