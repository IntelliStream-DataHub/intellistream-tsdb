# SPDX-License-Identifier: AGPL-3.0-only
# rel.py: LTTB and time-weighted averages (TWA) in each system's dialect, and the check of their answers against
# tsdb's (relq KIND 1 1 ref/KIND.jsonl).
#
#   python3 -I rel.py q SYS KIND          the query as qdrive / clickhouse-benchmark input (HTTP systems: the request
#                                         line); window kinds: one line per window
#   python3 -I rel.py sql SYS KIND        the SQL (or Flux) itself
#   python3 -I rel.py has SYS KIND        exit 0 when SYS has a query for KIND ('-' in the tables otherwise)
#   python3 -I rel.py check SYS KIND REF  run it on the server (ports of bench/dbcmp/README.md), compare; exit 1 on a
#                                         mismatch, 3 when the server cannot be reached ("SERVER DOWN"), 4 when it
#                                         answered the query with an HTTP error ("REFUSED: HTTP 400: its message")
#   python3 -I rel.py tsv KIND REF FILE   compare a TSV answer file (embq/chq OUT) with REF; exit 1 on a mismatch
#   python3 -I rel.py jsonl KIND REF FILE compare another relq/relemb dump with REF; exit 1 on a mismatch
#   python3 -I rel.py qb SYS KIND K       window kinds in batches: K consecutive windows per request (the groups of
#                                         batches.py: whole groups only), one request line each (q/SYS.KIND.bK.txt)
#   python3 -I rel.py checkb SYS KIND K REF   run every batch of FILE q/SYS.KIND.bK.txt (which must be qb's lines)
#                                         and compare each member with REF (exit codes as check)
# A batch is one SQL request in mkbatch.py's form: the members' window queries as UNION ALL branches, each with its
# member number as the first column (mb); a member's WITH tables are renamed apart (seg -> seg0, seg1, ...) and
# gathered in one WITH in front, so every member is the query it is alone.
# KIND: lttb (1000 points per series; rows series, t_ms, value); twa, twad, twal: every series over the whole range,
# STEP by hour, STEP by day, LINEAR by hour, buckets aligned to the epoch (rows series, bucket_ms, area, covered_ms,
# or series, bucket_ms, twa for the TimescaleDB toolkit and Flux); twa1d, twa7d: STEP over the windows relq takes
# (the first TWA_N of class agg1d / agg7d in TWA_SPEC=spec.tsv), one query per window, the reading before the
# window carried in and the first one after it closing the last segment (tsdb.h), buckets cut at the window's ends
# (rows bucket_ms, area, covered_ms; keyed by the window's line). SYS: ch, ts (the toolkit for whole-range kinds,
# plain SQL for windows), qdb (native twap() for STEP buckets), qdbseg (the 0.9.0 run's lead() CTE), i3, i2flux
# (LINEAR only), duck (DuckDB, embedded), chdb (embedded ClickHouse; the ch SQL on table t), iot (IoTDB, lead() and
# a split at most once, as qdbseg; LINEAR too), td (TDengine: its native TWA() for LINEAR, lead() and scalar subqueries
# for STEP, without WITH, which TDengine 3.4 lacks).
# Systems without a built-in function or a window-function recipe have none here ("-" in the table):
# LTTB is sequential (each bucket's pick depends on the previous pick), so it has no window-function recipe;
# InfluxDB 2 (Flux timeWeightedAvg interpolates linearly only) and Prometheus have no STEP TWA.
#
# Check: LTTB the same timestamps per series and values within 1e-7 relative, except where the series has an exact or
# near tie: when the triangle areas of two points of a bucket are equal (quantized values: the next bucket's mean
# equals the anchor's value, so every point one step above or below has the same area), double rounding of the mean
# decides which one a system picks, and the picks after it follow from that anchor. A series that differs is
# therefore replayed on the raw rows (DATA, the RowBinary input) in exact integer arithmetic along the system's own
# picks: it passes when every pick is in its bucket with its value and has the largest area given its anchor, within
# LTTB_TIE (1e-12) of the size of the two products the area is the difference of; the line says how many picks were
# such alternatives (ties) and how many followed from them. TWA per bucket with covered
# time > 0 the same bucket starts, the covered time exactly (where the system returns it) and twa within 1e-9
# relative. NaN, a missing or an extra bucket or series is a mismatch. Only the TimescaleDB toolkit may differ in
# the last bucket of a series (its interpolated_average holds the last value to the bucket end): counted apart.
# Checked with clickhouse local and DuckDB on 7 series x 650,000 rows: twa, twad, twal, twa1d, twa7d all exact.
import datetime, json, os, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
import http.client as httpclient

H = 3600000
IV = {"twa": H, "twad": 86400000, "twal": H}  # bucket width of the whole-range TWA kinds (twal: LINEAR)
WKINDS = {"twa1d": "agg1d", "twa7d": "agg7d"}  # window kinds: the spec.tsv class whose windows they take
KINDS = ["lttb", *IV, *WKINDS]

def iso(ms):  # RFC 3339 UTC with milliseconds
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"

def pgts(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S.") + f"{ms % 1000:03d}+00"

def windows(kind):
    """The windows relq twa1d/twa7d answers: the first TWA_N (1000) lines of the class in TWA_SPEC (spec.tsv), as
    (line, series, start, end, bucket_ms); line is the 0-based line in the file, the key of the reference."""
    out, want = [], int(os.environ.get("TWA_N", "1000"))
    for line, l in enumerate(open(os.environ.get("TWA_SPEC", "spec.tsv"))):
        f = l.split()
        if len(f) != 5: break  # relq stops at the first line it cannot parse
        if f[0] == WKINDS[kind]:
            out.append((line, int(f[1]), int(f[2]), int(f[3]), int(f[4])))
            if len(out) == want: break
    return out

def lin(p, q):
    """SQL: the LINEAR area of segment (t0, v0)-(t1, v1) between p and q, as tsdb computes it (src/twa.c interp: the
    ratio first, the end points exact). Columns t0, t1, v0, v1 must be DOUBLE-safe (the ratio is a float division)."""
    def at(x):
        return f"(CASE WHEN {x} = t1 THEN v1 ELSE v0 + (v1 - v0) * (CAST({x} - t0 AS DOUBLE) / CAST(t1 - t0 AS DOUBLE)) END)"
    return f"({at(p)} + {at(q)}) * 0.5 * CAST({q} - {p} AS DOUBLE)"

# ---- ClickHouse (and chDB on table t)
def q_ch(kind, table="cmp.t100m"):
    if kind == "lttb":
        # x as ms since 2024-01-01 (the data's start), as tsdb takes ms since the first point: with absolute ms
        # (1.7e12) the triangle areas lose precision and 92 of 1000 points of a flow_total series differed (smoke test)
        return (f"SELECT series, p.1 + 1704067200000 AS t, p.2 AS v FROM (SELECT series, arrayJoin(largestTriangleThreeBuckets(1000)"
                f"(toUnixTimestamp64Milli(ts) - 1704067200000, value)) AS p FROM {table} GROUP BY series) ORDER BY series, t")
    I = IV[kind]
    if kind == "twal":
        seg = ("SELECT series, t0, assumeNotNull(t1n) AS t1, v0, assumeNotNull(v1n) AS v1 FROM (SELECT series, "
               "toUnixTimestamp64Milli(ts) AS t0, toFloat64(value) AS v0, leadInFrame(toNullable(toUnixTimestamp64Milli(ts))) OVER w AS t1n, "
               f"leadInFrame(toNullable(toFloat64(value))) OVER w AS v1n FROM {table} WINDOW w AS (PARTITION BY series ORDER BY ts "
               "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)) WHERE t1n IS NOT NULL AND t0 < t1n")
        return (f"SELECT series, b, sum({lin('p', 'q')}) AS area, sum(q - p) AS covered FROM (SELECT series, b, t0, t1, v0, v1, "
                f"greatest(t0, b) AS p, least(t1, b + {I}) AS q FROM ({seg}) ARRAY JOIN range(intDiv(t0, {I}) * {I}, t1, {I}) AS b) "
                "GROUP BY series, b ORDER BY series, b")
    # segments from leadInFrame, split at bucket edges with ARRAY JOIN, weighted by their length
    return (f"SELECT series, b, sum(v * toFloat64(least(t1, b + {I}) - greatest(t0, b))) AS area, "
            f"sum(least(t1, b + {I}) - greatest(t0, b)) AS covered FROM ("
            "SELECT series, t0, assumeNotNull(t1n) AS t1, v FROM ("
            "SELECT series, toUnixTimestamp64Milli(ts) AS t0, toFloat64(value) AS v, "
            "leadInFrame(toNullable(toUnixTimestamp64Milli(ts))) OVER (PARTITION BY series ORDER BY ts "
            f"ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS t1n FROM {table}) WHERE t1n IS NOT NULL AND t0 < t1n) "
            f"ARRAY JOIN range(intDiv(t0, {I}) * {I}, t1, {I}) AS b GROUP BY series, b ORDER BY series, b")

def q_ch2(kind, table="cmp.t100m"):
    """STEP TWA in two levels (twa, twad): per (series, bucket) the area of the segments inside it from one sorted
    array of the bucket's readings, then per series over its buckets only the two edge pieces: the last reading up to
    the next bucket's first reading or the edge, and the previous bucket's last value from the edge to the first
    reading. A window over buckets, not over 100 M rows (twa-research/fairness.md: the release table quotes whichever
    of q_ch and q_ch2 is faster). Buckets without a reading are not made up: none occur in this data at an hour or a
    day (the slowest sensor reads every 60 s), and the check against tsdb's answers would show one."""
    if kind not in ("twa", "twad"):
        return None
    I = IV[kind]
    per = (f"SELECT series, intDiv(toUnixTimestamp64Milli(ts), {I}) * {I} AS b, "
           "arraySort(groupArray((toUnixTimestamp64Milli(ts), toFloat64(value)))) AS a, "
           "arraySum(arrayMap((d, x) -> d * x.2, arrayPopFront(arrayDifference(arrayMap(x -> x.1, a))), arrayPopBack(a))) AS inner, "
           f"a[1].1 AS ft, a[-1].1 AS lt, a[-1].2 AS lv FROM {table} GROUP BY series, b")
    edges = ("SELECT series, b, inner, ft, lt, lv, leadInFrame(toNullable(ft)) OVER w AS nf, "
             f"lagInFrame(toNullable(lv)) OVER w AS plv FROM ({per}) "
             "WINDOW w AS (PARTITION BY series ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)")
    tail = f"if(nf IS NULL, 0, least(assumeNotNull(nf), b + {I}) - lt)"
    head = "if(plv IS NULL, 0, ft - b)"
    return (f"SELECT series, b, area, covered FROM (SELECT series, b, "
            f"inner + if(nf IS NULL, 0.0, lv * toFloat64({tail})) + if(plv IS NULL, 0.0, assumeNotNull(plv) * toFloat64({head})) AS area, "
            f"(lt - ft) + {tail} + {head} AS covered FROM ({edges})) WHERE covered > 0 ORDER BY series, b")

def w_ch(s, a, b, bk, table="cmp.t100m"):
    """One window, STEP: the reading before it carried in, the first reading at or after its end closing the last
    segment (tsdb.h, time-weighted averages), buckets aligned to the epoch, cut at the window's ends."""
    A, B = f"fromUnixTimestamp64Milli(toInt64({a}))", f"fromUnixTimestamp64Milli(toInt64({b}))"
    src = f"SELECT toUnixTimestamp64Milli(ts) AS t, toFloat64(value) AS v FROM {table} WHERE series = {s} AND "
    pts = f"({src}ts < {A} ORDER BY ts DESC LIMIT 1) UNION ALL ({src}ts >= {A} AND ts < {B}) UNION ALL ({src}ts >= {B} ORDER BY ts LIMIT 1)"
    seg = (f"SELECT greatest(t, toInt64({a})) AS lo, least(assumeNotNull(t1n), toInt64({b})) AS hi, v FROM (SELECT t, v, "
           f"leadInFrame(toNullable(t)) OVER (ORDER BY t ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS t1n FROM ({pts})) "
           "WHERE t1n IS NOT NULL AND t < t1n")
    return (f"SELECT bb, sum(v * toFloat64(least(hi, bb + {bk}) - greatest(lo, bb))) AS area, sum(least(hi, bb + {bk}) - greatest(lo, bb)) "
            f"AS covered FROM ({seg}) ARRAY JOIN range(intDiv(lo, {bk}) * {bk}, hi, {bk}) AS bb WHERE hi > lo GROUP BY bb ORDER BY bb")

# ---- TimescaleDB: the toolkit's time_weight for whole-range kinds, plain SQL for windows
def q_ts(kind):
    if kind == "lttb":
        return ("SELECT x.series, (extract(epoch FROM u.time) * 1000)::bigint AS t, u.value FROM (SELECT series, "
                "lttb(ts, value::float8, 1000) AS tv FROM t100m GROUP BY series) x CROSS JOIN LATERAL unnest(x.tv) u "
                "ORDER BY 1, 2")
    iv = "1 day" if kind == "twad" else "1 hour"
    m = "Linear" if kind == "twal" else "LOCF"
    return (f"SELECT series, (extract(epoch FROM bucket) * 1000)::bigint AS b, interpolated_average(tw, bucket, "
            f"'{iv}', lag(tw) OVER w, lead(tw) OVER w) AS twa FROM (SELECT series, time_bucket('{iv}', ts) AS bucket, "
            f"time_weight('{m}', ts, value::float8) AS tw FROM t100m GROUP BY series, bucket) x "
            "WINDOW w AS (PARTITION BY series ORDER BY bucket) ORDER BY series, bucket")

def w_ts(s, a, b, bk):
    src = f"SELECT ts, value FROM t100m WHERE series = {s} AND "
    A, B = f"'{pgts(a)}'", f"'{pgts(b)}'"
    pts = f"({src}ts < {A} ORDER BY ts DESC LIMIT 1) UNION ALL ({src}ts >= {A} AND ts < {B}) UNION ALL ({src}ts >= {B} ORDER BY ts LIMIT 1)"
    seg = (f"SELECT greatest(t, {a}::bigint) AS lo, least(t1, {b}::bigint) AS hi, v FROM (SELECT (extract(epoch FROM ts) * 1000)::bigint AS t, "
           f"value::float8 AS v, (extract(epoch FROM lead(ts) OVER (ORDER BY ts)) * 1000)::bigint AS t1 FROM ({pts}) p) x "
           "WHERE t1 IS NOT NULL AND t < t1")
    return (f"SELECT bb, sum(v * (least(hi, bb + {bk}) - greatest(lo, bb))) AS area, sum(least(hi, bb + {bk}) - greatest(lo, bb)) AS covered "
            f"FROM ({seg}) y CROSS JOIN LATERAL generate_series(lo / {bk} * {bk}, hi - 1, {bk}::bigint) AS g(bb) WHERE hi > lo "
            "GROUP BY bb ORDER BY bb")

# ---- QuestDB: no ARRAY JOIN / unnest, so a segment is split at most once (true for this data: the longest step
# is 60 s (hdd_temp has no jitter; the largest jitter is 50 ms), buckets are an hour or longer, window buckets 300 samples or more)
def split_once(I, area1, area2, key="series, ", group=False, once=False):
    """Rows (key, b, a, c) of the segments in seg (columns lo, hi, ...): the piece in lo's bucket and the piece in hi's.
    group: an explicit GROUP BY (QuestDB groups by the non-aggregate columns by itself), and no "WHERE c > 0" (IoTDB
    2.0.11 answers a filter over a UNION ALL subquery with extra groups whose b is null; a piece with c = 0 adds 0, and
    a bucket covering 0 ms is left out by the check as by tsdb).
    once: seg is read once, crossed with a two-row table (k 0: lo's piece, k 1: hi's), instead of twice in a UNION ALL.
    QuestDB inlines a WITH where it is used, so the UNION ALL ran seg's scans (and its lead() window) twice; the
    other servers keep the UNION ALL they were checked and timed with."""
    b0, b1 = f"lo / {I} * {I}", f"hi / {I} * {I}"
    if once:
        return (f"SELECT {key}b, sum(a) area, sum(c) covered FROM ("
                f"SELECT {key}CASE WHEN k = 0 THEN {b0} ELSE {b1} END b, "
                f"CASE WHEN k = 0 THEN {area1} ELSE {area2} END a, "
                f"CASE WHEN k = 0 THEN least(hi, {b0} + {I}) - lo ELSE hi - {b1} END c "
                f"FROM seg CROSS JOIN (SELECT x - 1 k FROM long_sequence(2)) ks WHERE hi > lo AND (k = 0 OR hi / {I} > lo / {I})"
                + (f") u GROUP BY {key}b" if group else ") WHERE c > 0") + f" ORDER BY {key}b")
    return (f"SELECT {key}b, sum(a) area, sum(c) covered FROM ("
            f"SELECT {key}{b0} b, {area1} a, least(hi, {b0} + {I}) - lo c FROM seg WHERE hi > lo "
            f"UNION ALL SELECT {key}{b1} b, {area2} a, hi - {b1} c FROM seg WHERE hi > lo AND hi / {I} > lo / {I}"
            + (f") u GROUP BY {key}b" if group else ") WHERE c > 0") + f" ORDER BY {key}b")

def q_qdb(kind):
    if kind == "lttb":
        return None
    I = IV[kind]
    if kind == "twal":
        seg = ("SELECT series, cast(ts AS long) / 1000 lo, cast(lead(ts) OVER (PARTITION BY series ORDER BY ts) AS long) / 1000 hi, "
               "cast(value AS double) v0, cast(lead(value) OVER (PARTITION BY series ORDER BY ts) AS double) v1 FROM sensors")
        b0, b1 = f"lo / {I} * {I}", f"hi / {I} * {I}"
        L = lambda p, q: lin(p, q).replace("t0", "lo").replace("t1", "hi").replace("DOUBLE", "double")
        return (f"WITH seg AS (SELECT * FROM ({seg}) WHERE hi IS NOT NULL) "
                + split_once(I, L("lo", f"least(hi, {b0} + {I})"), L(b1, "hi"), once=True))
    # native twap() per bucket: the area between the bucket's first and last reading is twap * (last - first);
    # lag/lead over the bucket rows add the pieces before the first reading (the previous bucket's last value from
    # the edge) and after the last one (its value up to the edge). Buckets without a reading are missing, so a gap
    # longer than a bucket is not covered (none in this data).
    iv = "1d" if kind == "twad" else "1h"
    h = (f"SELECT ts, series, twap(value, ts) tw, first(ts) ft, last(ts) lt, last(value) lv FROM sensors "
         f"SAMPLE BY {iv} ALIGN TO CALENDAR")
    x = ("SELECT series, cast(ts AS long) / 1000 b, cast(ft AS long) / 1000 f, cast(lt AS long) / 1000 l, tw, "
         "cast(lv AS double) lv, cast(lag(lv) OVER (PARTITION BY series ORDER BY ts) AS double) plv, "
         f"lead(ft) OVER (PARTITION BY series ORDER BY ts) nft FROM ({h})")
    return (f"SELECT series, b, (CASE WHEN l > f THEN tw * (l - f) ELSE 0.0 END) + (CASE WHEN nft IS NULL THEN 0.0 ELSE lv * (b + {I} - l) END) "
            f"+ (CASE WHEN plv IS NULL THEN 0.0 ELSE plv * (f - b) END) area, (l - f) + (CASE WHEN nft IS NULL THEN 0 ELSE b + {I} - l END) "
            f"+ (CASE WHEN plv IS NULL THEN 0 ELSE f - b END) covered FROM ({x}) ORDER BY series, b")

def q_qdbseg(kind):  # the 0.9.0 run's TWA: lead() in a CTE (kept to compare with the twap() recipe)
    if kind not in ("twa", "twad"):
        return None
    I = IV[kind]
    seg = ("SELECT series, cast(value AS double) v, cast(ts AS long) / 1000 lo, "
           "cast(lead(ts) OVER (PARTITION BY series ORDER BY ts) AS long) / 1000 hi FROM sensors")
    return f"WITH seg AS (SELECT * FROM ({seg}) WHERE hi IS NOT NULL) " + split_once(I, f"v * (least(hi, lo / {I} * {I} + {I}) - lo)", f"v * (hi - hi / {I} * {I})", once=True)

def w_qdb(s, a, b, bk):
    src = f"SELECT ts, cast(value AS double) v FROM sensors WHERE series = '{s}' AND "
    pts = (f"SELECT * FROM ({src}ts < '{iso(a)}' ORDER BY ts DESC LIMIT 1) UNION ALL SELECT * FROM ({src}ts >= '{iso(a)}' AND "
           f"ts < '{iso(b)}') UNION ALL SELECT * FROM ({src}ts >= '{iso(b)}' ORDER BY ts LIMIT 1)")
    seg = (f"SELECT greatest(t0, {a}) lo, least(t1, {b}) hi, v FROM (SELECT cast(ts AS long) / 1000 t0, "
           f"cast(lead(ts) OVER (ORDER BY ts) AS long) / 1000 t1, v FROM ({pts} ORDER BY ts)) WHERE t1 IS NOT NULL AND t0 < t1")
    return f"WITH seg AS ({seg}) " + split_once(bk, f"v * (least(hi, lo / {bk} * {bk} + {bk}) - lo)", f"v * (hi - hi / {bk} * {bk})", key="", once=True)

# ---- InfluxDB 3 (DataFusion) and DuckDB: unnest(range()) splits a segment at every edge
def unnest_q(kind, seg, div):
    """seg: columns series, t0, t1, v0[, v1]; div: integer division operator."""
    I = IV[kind]
    parts = f"SELECT *, unnest(range(t0 {div} {I} * {I}, t1, {I})) AS b FROM seg WHERE t1 IS NOT NULL AND t0 < t1"
    if kind == "twal":
        return (f"WITH seg AS ({seg}), parts AS ({parts}), pq AS (SELECT series, b, t0, t1, v0, v1, greatest(t0, b) AS p, "
                f"least(t1, b + {I}) AS q FROM parts) SELECT series, b, sum({lin('p', 'q')}) AS area, sum(q - p) AS covered "
                "FROM pq GROUP BY series, b ORDER BY series, b")
    return (f"WITH seg AS ({seg}), parts AS ({parts}) SELECT series, b, sum(v0 * CAST(least(t1, b + {I}) - greatest(t0, b) AS DOUBLE)) "
            f"AS area, sum(least(t1, b + {I}) - greatest(t0, b)) AS covered FROM parts GROUP BY series, b ORDER BY series, b")

def q_i3(kind):
    if kind == "lttb":
        return None
    w = "OVER (PARTITION BY series ORDER BY time)"
    v1 = f", CAST(lead(value) {w} AS DOUBLE) AS v1" if kind == "twal" else ""  # STEP needs no second value
    return unnest_q(kind, f"SELECT series, CAST(value AS DOUBLE) AS v0, CAST(time AS BIGINT) / 1000000 AS t0, CAST(lead(time) {w} "
                    f"AS BIGINT) / 1000000 AS t1{v1} FROM sensors", "/")

def w_i3(s, a, b, bk):
    src = f"SELECT CAST(time AS BIGINT) / 1000000 AS t, CAST(value AS DOUBLE) AS v FROM sensors WHERE series = '{s}' AND "
    pts = (f"(SELECT * FROM ({src}time < '{iso(a)}') ORDER BY t DESC LIMIT 1) UNION ALL ({src}time >= '{iso(a)}' AND time < '{iso(b)}') "
           f"UNION ALL (SELECT * FROM ({src}time >= '{iso(b)}') ORDER BY t LIMIT 1)")
    seg = (f"SELECT greatest(t, {a}) AS lo, least(t1, {b}) AS hi, v FROM (SELECT t, v, lead(t) OVER (ORDER BY t) AS t1 FROM ({pts})) "
           "WHERE t1 IS NOT NULL AND t < t1")
    return (f"WITH seg AS ({seg}), parts AS (SELECT *, unnest(range(lo / {bk} * {bk}, hi, {bk})) AS bb FROM seg WHERE hi > lo) "
            f"SELECT bb, sum(v * CAST(least(hi, bb + {bk}) - greatest(lo, bb) AS DOUBLE)) AS area, sum(least(hi, bb + {bk}) - greatest(lo, bb)) "
            "AS covered FROM parts GROUP BY bb ORDER BY bb")

def q_duck(kind):
    if kind == "lttb":
        return None
    w = "OVER (PARTITION BY series ORDER BY ts)"
    v1 = f", CAST(lead(value) {w} AS DOUBLE) AS v1" if kind == "twal" else ""
    return unnest_q(kind, f"SELECT series, CAST(value AS DOUBLE) AS v0, epoch_ms(ts) AS t0, epoch_ms(lead(ts) {w}) AS t1{v1} FROM t", "//")

# ---- IoTDB 2.0 (table model): no TWA function and no row generator (unnest, generate_series, ARRAY JOIN), but
# lead() and WITH: QuestDB's recipe, a segment split at most once (true for this data, as for QuestDB)
def q_iot(kind):
    if kind == "lttb":
        return None
    I = IV[kind]
    w = "OVER (PARTITION BY series ORDER BY time)"
    seg = (f"SELECT series, CAST(value AS DOUBLE) AS v0, CAST(time AS INT64) AS lo, CAST(lead(time) {w} AS INT64) AS hi, "
           f"CAST(lead(value) {w} AS DOUBLE) AS v1 FROM sensors")
    pre = f"WITH seg AS (SELECT * FROM ({seg}) s WHERE hi IS NOT NULL) "
    if kind == "twal":
        b0, b1 = f"lo / {I} * {I}", f"hi / {I} * {I}"
        # lin() without its CASE (IoTDB 2.0.11 fails CASE over these columns with ArrayIndexOutOfBoundsException in
        # ConstantColumnTransformer): at x = t1 the ratio is exactly 1, so v0 + (v1 - v0) differs from v1 by at most
        # an ulp, far inside the check's 1e-9
        def at(x):
            return f"(v0 + (v1 - v0) * (CAST({x} - lo AS DOUBLE) / CAST(hi - lo AS DOUBLE)))"
        L = lambda p, q: f"({at(p)} + {at(q)}) * 0.5 * CAST({q} - {p} AS DOUBLE)"
        return pre + split_once(I, L("lo", f"least(hi, {b0} + {I})"), L(b1, "hi"), group=True)
    return pre + split_once(I, f"v0 * (least(hi, lo / {I} * {I} + {I}) - lo)", f"v0 * (hi - hi / {I} * {I})", group=True)

def w_iot(s, a, b, bk):
    # the reading before the window and the first one after it as scalar subqueries (a UNION ALL of LIMIT 1 branches
    # fails in IoTDB 2.0.11 with ArrayIndexOutOfBoundsException); no reading before: the window's start
    one = f"FROM sensors WHERE series = '{s}' AND time"
    pts = (f"SELECT CAST(time AS INT64) AS t, CAST(value AS DOUBLE) AS v {one} >= coalesce((SELECT max(time) {one} < {a}), "
           f"CAST({a} AS TIMESTAMP)) AND time <= coalesce((SELECT min(time) {one} >= {b}), CAST({b} AS TIMESTAMP))")
    seg = (f"SELECT greatest(t, {a}) AS lo, least(t1, {b}) AS hi, v FROM (SELECT t, v, lead(t) OVER (ORDER BY t) AS t1 "
           f"FROM ({pts}) p) q WHERE t1 IS NOT NULL AND t < t1")
    return f"WITH seg AS ({seg}) " + split_once(bk, f"v * (least(hi, lo / {bk} * {bk} + {bk}) - lo)", f"v * (hi - hi / {bk} * {bk})", key="", group=True)

# ---- TDengine 3.4: TWA() is LINEAR with interpolation at the window edges (builtinsimpl.c), answered per series
# (PARTITION BY) and hour as (series, b, twa). STEP: lead() per series (PARTITION BY) and the split-once recipe as for
# IoTDB, the segment subquery written out where a CTE would be (no WITH); arithmetic on BIGINT gives DOUBLE in
# TDengine, so bucket starts are CAST(x / I AS BIGINT) * I (exact: epoch milliseconds are far below 2^53)
def td_split(I, seg, area1, area2, key="`series`, "):
    b0, b1 = f"CAST(lo / {I} AS BIGINT) * {I}", f"CAST(hi / {I} AS BIGINT) * {I}"
    return (f"SELECT {key}b, sum(a) area, sum(c) covered FROM ("
            f"SELECT {key}{b0} b, {area1} a, least(hi, {b0} + {I}) - lo c FROM ({seg}) seg WHERE hi > lo "
            f"UNION ALL SELECT {key}{b1} b, {area2} a, hi - {b1} c FROM ({seg}) seg WHERE hi > lo AND "
            f"CAST(hi / {I} AS BIGINT) > CAST(lo / {I} AS BIGINT)) u GROUP BY {key}b ORDER BY {key}b")

def q_td(kind):
    if kind == "lttb":
        return None
    I = IV[kind]
    if kind == "twal":
        return f"SELECT `series`, CAST(_wstart AS BIGINT) b, twa(value) FROM sensors PARTITION BY `series` INTERVAL({I}a)"
    seg = ("SELECT * FROM (SELECT `series`, CAST(_ts AS BIGINT) lo, CAST(lead(_ts) AS BIGINT) hi, CAST(value AS DOUBLE) v0 "
           "FROM sensors PARTITION BY `series`) s WHERE hi IS NOT NULL")
    return td_split(I, seg, f"v0 * (least(hi, CAST(lo / {I} AS BIGINT) * {I} + {I}) - lo)",
                    f"v0 * (hi - CAST(hi / {I} AS BIGINT) * {I})")

def w_td(s, a, b, bk):
    # the reading before the window and the first one after it by scalar subqueries, as for IoTDB
    one = f"FROM sensors WHERE `series` = '{s}' AND _ts"
    pts = (f"SELECT CAST(_ts AS BIGINT) t, CAST(value AS DOUBLE) v, CAST(lead(_ts) AS BIGINT) t1 {one} >= "
           f"coalesce((SELECT max(_ts) {one} < {a}), {a}) AND _ts <= coalesce((SELECT min(_ts) {one} >= {b}), {b})")
    seg = f"SELECT greatest(t, {a}) lo, least(t1, {b}) hi, v FROM ({pts}) p WHERE t1 IS NOT NULL AND t < t1"
    return td_split(bk, seg, f"v * (least(hi, CAST(lo / {bk} AS BIGINT) * {bk} + {bk}) - lo)",
                    f"v * (hi - CAST(hi / {bk} AS BIGINT) * {bk})", key="")

# ---- InfluxDB 2: Flux timeWeightedAvg (LINEAR only; aggregateWindow hands it each window's own rows, so the edges
# are whatever timeWeightedAvg makes of them: rel.py check shows whether that matches tsdb's cut-and-interpolate)
def q_i2flux(kind):
    if kind != "twal":
        return None
    return ('from(bucket: "cmp") |> range(start: 2023-12-31T00:00:00Z, stop: 2026-01-01T00:00:00Z) '
            '|> filter(fn: (r) => r._measurement == "sensors" and r._field == "value") '
            '|> aggregateWindow(every: 1h, fn: (tables=<-, column) => tables |> timeWeightedAvg(unit: 1ms), '
            'createEmpty: false, timeSrc: "_start") |> keep(columns: ["series", "_time", "_value"])')

Q = {"ch": q_ch, "ch2": q_ch2, "ts": q_ts, "qdb": q_qdb, "qdbseg": q_qdbseg, "i3": q_i3, "i2flux": q_i2flux, "duck": q_duck,
     "chdb": lambda k: q_ch(k, "t"), "iot": q_iot, "td": q_td}
W = {"ch": w_ch, "ts": w_ts, "qdb": w_qdb, "i3": w_i3, "iot": w_iot, "td": w_td}

def ctes(q):
    """'WITH a AS (...), b AS (...) SELECT ...' -> ([(a, ...), (b, ...)], 'SELECT ...'); ([], q) without WITH."""
    if not q.startswith("WITH "): return [], q
    out, i = [], len("WITH ")
    while True:
        j = q.index(" AS (", i)
        p = k = j + len(" AS (")
        depth = 1
        while depth:
            depth += (q[p] == "(") - (q[p] == ")")
            p += 1
        out.append((q[i:j].strip(), q[k:p - 1]))
        rest = q[p:].lstrip()
        if not rest.startswith(","): return out, rest
        i = q.index(",", p) + 1

def wbatch(sysname, members):
    """One request of the windows members [(series, start, end, bucket)]: mkbatch.py's UNION ALL with a member
    column, each member's WITH tables renamed apart."""
    import re
    with_, parts = [], []
    for m, x in enumerate(members):
        cs, body = ctes(W[sysname](*x))
        if cs:
            pat = re.compile(r"\b(" + "|".join(re.escape(n) for n, _ in cs) + r")\b")
            ren = lambda t, m=m: pat.sub(lambda g: f"{g.group(1)}{m}", t)
            with_ += [(f"{n}{m}", ren(b)) for n, b in cs]
            body = ren(body)
        assert body.startswith("SELECT ")
        parts.append(f"SELECT {m} AS mb, " + body[len("SELECT "):])
    q = parts[0] if len(parts) == 1 else "SELECT * FROM (" + " UNION ALL ".join(f"({p})" for p in parts) + ") x"
    return ("WITH " + ", ".join(f"{n} AS ({b})" for n, b in with_) + " " + q) if with_ else q

def wgroups(kind, k):
    """The windows of kind (windows()) K at a time, whole groups only, as batches.py groups a class's lines."""
    w = windows(kind)
    return [w[g * k:(g + 1) * k] for g in range(len(w) // k)]

def fetch_batches(sysname, kind, k, path):
    """Every batch of path (checked to be the requests wbatch makes) run once; rows keyed by the window's line."""
    lines = open(path).read().splitlines()
    groups = wgroups(kind, k)
    if len(lines) != len(groups):
        raise SystemExit(f"{path}: {len(lines)} lines, {len(groups)} groups of {k} windows")
    rows, keys = [], []
    for line, g in zip(lines, groups):
        sql = wbatch(sysname, [x[1:] for x in g])
        if line != wire(sysname, sql): raise SystemExit(f"{path}: a line that is not rel.py's batch (rel.py qb)")
        keys += [x[0] for x in g]
        for r in run_sql(sysname, sql):
            m = int(float(r[0]))
            # a member number out of range becomes a key no window has: an unexpected series in compare
            rows.append([g[m][0] if 0 <= m < len(g) else -1 - m, *r[1:]])
    return rows, keys

def query(sysname, kind):
    """The whole-range query, or for window kinds the list of (line, query), one per window."""
    if kind in WKINDS:
        if sysname not in W: return None
        return [(i, W[sysname](s, a, b, bk)) for i, s, a, b, bk in windows(kind)]
    return Q[sysname](kind)

I3_GET_MAX = 8192  # bytes of a GET request line to InfluxDB 3; longer queries go as a POST body

def wire(sysname, q):
    """A query as one line of a qdrive / clickhouse-benchmark input file."""
    if sysname in ("i3",):
        # GET as phase 2 timed it; a batch of 100 windows is a URL past what InfluxDB 3 takes (HTTP 414 in the
        # 2026-11-01 smoke), so a long query goes as the JSON body of a POST, which the same endpoint takes
        g = "GET /api/v3/query_sql?db=cmp&format=csv&q=" + urllib.parse.quote(q, safe="")
        if len(g) <= I3_GET_MAX:
            return g
        return "POST /api/v3/query_sql\tapplication/json\t" + json.dumps({"db": "cmp", "q": q, "format": "csv"}, separators=(",", ":"))
    if sysname == "iot":
        return "POST /rest/table/v1/query\tapplication/json\t" + json.dumps({"database": "cmp", "sql": q}, separators=(",", ":"))
    if sysname == "td":
        return "POST /rest/sql/cmp\ttext/plain\t" + q
    if sysname == "i2flux":
        return "POST /api/v2/query?org=bench\tapplication/json\t" + json.dumps({"query": q, "type": "flux", "dialect": {"annotations": []}})
    return q

class Refused(RuntimeError):
    """The server answered the query with an HTTP error: it is up and said no (QuestDB's "global RSS memory limit
    exceeded" comes as a 400 with a JSON error). Not SERVER DOWN: HTTPError is a URLError, which server_down counts."""

def http(url, data=None, headers=None):
    req = urllib.request.Request(url, data=data.encode() if data else None, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=3600) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            body = json.loads(body).get("error", body)
        except ValueError:
            pass
        raise Refused(f"HTTP {e.code}: {' '.join(str(body).split())[:400]}") from None

def run_sql(sysname, sql):
    if sysname in ("ch", "ch2"):
        out = http(f"http://127.0.0.1:{os.environ.get('CH_HTTP_PORT', '8123')}/", sql + " FORMAT TSV")
        return [l.split("\t") for l in out.splitlines() if l]
    if sysname == "ts":
        out = subprocess.run(["psql", "-h", "127.0.0.1", "-p", os.environ.get("TS_PORT", "5434"), "-U", "postgres", "-At",
                              "-F", "\t", "-c", f"SET work_mem = '{os.environ.get('TS_WORK_MEM', '256MB')}'", "-c", sql],
                             capture_output=True, text=True, check=True,
                             env={"PGPASSWORD": "bench", "PGTZ": "UTC", "PATH": os.environ.get("PATH", "/usr/bin:/bin")}).stdout
        return [l.split("\t") for l in out.splitlines() if l and l != "SET"]
    if sysname in ("qdb", "qdbseg"):
        out = json.loads(http(f"http://127.0.0.1:{os.environ.get('QDB_HTTP_PORT', '9100')}/exec?query=" + urllib.parse.quote(sql, safe="")))
        if "error" in out: raise RuntimeError(out["error"])
        return out["dataset"]
    if sysname == "i3":
        url = f"http://127.0.0.1:{os.environ.get('I3_PORT', '8181')}/api/v3/query_sql"  # POST: no URL length limit
        body = json.dumps({"db": "cmp", "q": sql, "format": "json"})
        return [list(r.values()) for r in json.loads(http(url, body, {"Content-Type": "application/json"}))]
    if sysname == "iot":
        # 719 is IoTDB refusing a query whose memory estimate exceeds what the query operators have free at that
        # moment (enable_query_memory_estimation, the default); right after a 32-client run the queries the clients
        # left behind still hold it, so the check waits for them (up to 2 minutes) instead of failing; it says so
        for attempt in range(25):
            out = json.loads(http(f"http://127.0.0.1:{os.environ.get('IOT_REST_PORT', '18080')}/rest/table/v1/query",
                                  json.dumps({"database": "cmp", "sql": sql}),
                                  {"Content-Type": "application/json", "Authorization": "Basic cm9vdDpyb290"}))
            if out.get("code") != 719: break
            print(f"iot: 719 (query memory), retry {attempt + 1} in 5 s", file=sys.stderr)
            time.sleep(5)
        if out.get("code") == 719:  # still refused after the waits: IoTDB's own answer, a result, not a broken check
            raise Refused(f"iot code 719: {out.get('message', '')}"[:400])
        if "values" not in out: raise RuntimeError(f"iot: {out}")
        return out["values"]
    if sysname == "td":
        out = json.loads(http(f"http://127.0.0.1:{os.environ.get('TD_REST_PORT', '6041')}/rest/sql/cmp", sql,
                              {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="}))
        if out.get("code") != 0 or "data" not in out: raise RuntimeError(f"td: {out}")
        return out["data"]
    if sysname == "i2flux":
        import csv, io
        body = json.dumps({"query": sql, "type": "flux", "dialect": {"annotations": []}})
        txt = http(f"http://127.0.0.1:{os.environ.get('I2_PORT', '8086')}/api/v2/query?org=bench", body,
                   {"Authorization": "Token benchtoken", "Accept": "application/csv", "Content-Type": "application/json"})
        rows, hdr = [], None
        for rec in csv.reader(io.StringIO(txt)):
            if not rec or not any(rec): hdr = None; continue
            if hdr is None or rec[1] == "result": hdr = rec; continue
            d = dict(zip(hdr, rec))
            t = datetime.datetime.fromisoformat(d["_time"].replace("Z", "+00:00"))
            rows.append([d["series"], round(t.timestamp() * 1000), d["_value"]])
        return rows
    raise SystemExit("no fetch for " + sysname)

def fetch(sysname, kind):
    q = query(sysname, kind)
    if kind in WKINDS:  # rows keyed by the window's line, as the reference is
        return [[i, *r] for i, w in q for r in run_sql(sysname, w)]
    return run_sql(sysname, q)

def load_ref(path):
    """relq's answers by series, or for windows (twa1d/twa7d) by the window's line in spec.tsv."""
    return {x.get("i", x["s"]): x for x in map(json.loads, open(path))}

def num(x):
    """A number from an answer cell; empty, null and NaN become NaN, which never matches."""
    if x in ("", None): return float("nan")
    return float(x)

def near(x, y, rel):
    """Within rel of each other; False for NaN on either side (abs(nan - y) > tol is False, so never test that way)."""
    return x == y or abs(x - y) <= rel * max(abs(x), abs(y), 1e-30)

# REL_TOL "REL,ABS": the bound for the TWA average of a system whose built-in TWA has tsdb's edges but less precise
# arithmetic (TDengine's TWA(): float accumulation), stated in the tables; default 1e-9 relative. Covered time stays exact.
TOL_REL, TOL_ABS = (float(x) for x in os.environ.get("REL_TOL", "1e-9,0").split(","))

def twa_near(x, y):
    return near(x, y, TOL_REL) or (TOL_ABS > 0 and abs(x - y) <= TOL_ABS)

LTTB_TIE = float(os.environ.get("LTTB_TIE", "1e-12"))
_raw = {}  # DATA path -> memmap

def raw_series(s):
    """Series s from DATA (u32 series, i64 ms, f32 value), finite values only, ordered by time, the last of equal
    timestamps (last write wins); None without DATA."""
    path = os.environ.get("DATA")
    if not path or not os.path.exists(path): return None
    try:
        import numpy as np
        if path not in _raw: _raw[path] = np.memmap(path, dtype=np.dtype([("s", "<u4"), ("t", "<i8"), ("v", "<f4")]), mode="r")
        r = _raw[path][_raw[path]["s"] == s]
        r = r[np.isfinite(r["v"])]
        o = np.argsort(r["t"], kind="stable")
        t, v = r["t"][o], r["v"][o]
        keep = np.append(t[1:] != t[:-1], True)
        return [int(x) for x in t[keep]], [float(x) for x in v[keep]]
    except ImportError:
        import math, struct
        d = {}
        with open(path, "rb") as f:
            while b := f.read(16 << 20):
                for ss, t, v in struct.iter_unpack("<Iqf", b):
                    if ss == s and math.isfinite(v): d[t] = v
        t = sorted(d)
        return t, [d[x] for x in t]

def lttb_alternative(s, gt, gv, et):
    """(True, why) when the system's picks gt/gv for series s are an LTTB answer of the raw rows that differs from
    tsdb's (et) only through ties (see the header); (False, why) otherwise."""
    import math
    raw = raw_series(s)
    if raw is None: return False, "no DATA to replay the series on"
    T, V = raw
    n, th = len(T), len(et)
    if len(gt) != th or n <= th or th < 3: return False, f"{len(gt)} points of {n} rows, threshold {th}"
    at = {t: k for k, t in enumerate(T)}
    idx = [at.get(t) for t in gt]
    if None in idx: return False, "a timestamp that is not a row"
    if any(not near(V[k], y, 1e-7) for k, y in zip(idx, gv)): return False, "a value that is not the row's"
    if idx[0] != 0 or idx[-1] != n - 1: return False, "first or last point is not the series' first or last row"
    # exact: x in ms, y scaled by 2^149 (every float32 is an integer multiple of 2^-149); prefix sums for the means
    X = T
    Y = [int(math.ldexp(v, 149)) for v in V]
    PX, PY = [0], [0]
    for x, y in zip(X, Y):
        PX.append(PX[-1] + x); PY.append(PY[-1] + y)
    every = (n - 2) / (th - 2)  # the buckets as tsdb, ClickHouse and the paper's code cut them (doubles)
    start = lambda b: math.floor(b * every) + 1
    nb = th - 2
    ties = exact = 0
    for b in range(nb):
        lo, hi = start(b), start(b + 1)
        nlo, nhi = (start(b + 1), start(b + 2)) if b + 1 < nb else (n - 1, n)
        p, a = idx[b + 1], idx[b]
        if not lo <= p < hi: return False, f"point {b + 1} is outside its bucket"
        c = nhi - nlo
        cx, cy = PX[nhi] - PX[nlo], PY[nhi] - PY[nlo]  # the next bucket's mean times c
        ax, ay = X[a] * c, Y[a] * c
        def terms(j):  # the area times 2c is |t1 - t2|
            return (ax - cx) * (Y[j] - Y[a]), (X[a] - X[j]) * (cy - ay)
        best, bj = -1, lo
        for j in range(lo, hi):
            t1, t2 = terms(j)
            if abs(t1 - t2) > best: best, bj = abs(t1 - t2), j
        if p == bj: continue
        t1, t2 = terms(p)
        u1, u2 = terms(bj)
        short = best - abs(t1 - t2)
        if short > LTTB_TIE * max(abs(t1) + abs(t2), abs(u1) + abs(u2)):
            return False, f"point {b + 1} (t {T[p]}) has a smaller area than t {T[bj]} by {short / best:.3g} of it"
        ties += 1; exact += short == 0
    nd = sum(x != y for x, y in zip(gt, et))
    return True, (f"{nd} points differ from tsdb's, all an LTTB answer of the rows: {ties} equal-area alternatives "
                  f"({exact} exact ties, the rest within {LTTB_TIE:g}), {nd - ties} the largest area after an anchor that differs")

def compare(kind, ref, rows, lenient_last=False):
    """rows: lists (series, t|b, v|area, [covered]); ref: relq dump. TWA rows with covered time compare it exactly;
    rows without it (series, b, twa: the TimescaleDB toolkit) compare the average only. A bucket missing on either
    side, an extra series, a NaN or a covered time off by one ms is a mismatch. lenient_last: the last bucket of a
    series may differ (the toolkit's interpolated_average holds the last value to the bucket end); counted apart."""
    got = {}
    for r in rows:
        got.setdefault(int(float(r[0])), []).append(r[1:])
    bad_series, bad_pts, bad_last, total, alt_series = 0, 0, 0, 0, 0
    maxabs = maxrel = 0.0
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
                ok, why = lttb_alternative(s, gt, gv, et)
                print(f"lttb series {s}: {why}" if ok else f"lttb series {s}: not an equal-area alternative: {why}")
                if ok:
                    alt_series += 1
                    continue
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
                ok = x is not None and y is not None and twa_near(x[0], y[0]) and (y[1] is None or y[1] == x[1])
                if x is not None and y is not None and x[0] == x[0] and y[0] == y[0]:
                    d = abs(x[0] - y[0])
                    maxabs = max(maxabs, d)
                    maxrel = max(maxrel, d / max(abs(x[0]), abs(y[0]), 1e-30))
                if not ok:
                    if lenient_last and (b == last or (x is None and b > (last or 0))): nl += 1
                    else:
                        nb += 1
                        if nb <= 3 and bad_series < 5: print(f"  series {s} bucket {b}: ref (twa, covered) {x}, got {y}")
            if nb or nl:
                bad_series += nb > 0; bad_pts += nb; bad_last += nl
                if nb and bad_series <= 5: print(f"MISMATCH twa series {s}: {nb} buckets differ, {nl} at the end (ref {len(eb)}, got {len(gb)})")
    extra = sorted(set(got) - set(ref))
    print(f"{kind}: {len(ref)} {'windows' if kind in WKINDS else 'series'}, {total} reference {'points' if kind == 'lttb' else 'buckets'}; series with mismatches "
          f"{bad_series}, mismatched {bad_pts}, last-bucket differences {bad_last}{' (allowed: toolkit)' if lenient_last else ''}, "
          f"unexpected series {len(extra)}" + (f"; series answered with equal-area alternatives {alt_series}" if kind == "lttb" else
          f"; largest difference of an average {maxrel:.2g} relative, {maxabs:.2g} absolute (bound {TOL_REL:g} relative"
          + (f" or {TOL_ABS:g} absolute)" if TOL_ABS > 0 else ")")))
    return bad_series == 0 and not extra

def main():
    cmd = sys.argv[1]
    if cmd in ("q", "sql"):
        sysname, kind = sys.argv[2], sys.argv[3]
        s = query(sysname, kind) if sysname in Q else None
        if not s: raise SystemExit(1)  # no such query: the caller writes '-'
        for x in (s if kind in WKINDS else [(None, s)]):
            print(wire(sysname, x[1]) if cmd == "q" else x[1])
    elif cmd == "qb":
        sysname, kind, k = sys.argv[2], sys.argv[3], int(sys.argv[4])
        if kind not in WKINDS or sysname not in W: raise SystemExit(1)
        for g in wgroups(kind, k):
            print(wire(sysname, wbatch(sysname, [x[1:] for x in g])))
    elif cmd == "checkb":
        sysname, kind, k, ref = sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
        rows, keys = fetch_batches(sysname, kind, k, f"q/{sysname}.{kind}.b{k}.txt")
        full = load_ref(ref)
        ok = compare(kind, {i: full[i] for i in keys if i in full}, rows) and all(i in full for i in keys)
        print(sysname, f"{kind}.b{k}", "OK" if ok else "MISMATCH")
        if not ok: raise SystemExit(1)
    elif cmd == "has":  # exit 0 when SYS has a query for KIND
        raise SystemExit(0 if sys.argv[2] in Q and query(sys.argv[2], sys.argv[3]) else 1)
    elif cmd == "check":
        sysname, kind, ref = sys.argv[2:5]
        ok = compare(kind, load_ref(ref), fetch(sysname, kind), lenient_last=sysname == "ts" and kind not in WKINDS)
        print(sysname, kind, "OK" if ok else "MISMATCH")
        if not ok: raise SystemExit(1)
    elif cmd == "tsv":
        kind, ref, f = sys.argv[2:5]
        ok = compare(kind, load_ref(ref), [l.rstrip("\n").split("\t") for l in open(f) if l.strip()])
        print(f, kind, "OK" if ok else "MISMATCH")
        if not ok: raise SystemExit(1)
    elif cmd == "jsonl":
        kind, ref, f = sys.argv[2:5]
        rows = []
        for x in map(json.loads, open(f)):
            if kind == "lttb": rows += [[x["s"], t, v] for t, v in zip(x["t"], x["v"])]
            else: rows += [[x.get("i", x["s"]), b[0], b[1], b[2]] for b in x["b"]]
        ok = compare(kind, load_ref(ref), rows)
        print(f, kind, "OK" if ok else "MISMATCH")
        if not ok: raise SystemExit(1)

def server_down(e):
    """True when e says the server could not be reached or went away: reported as SERVER DOWN (exit 3), not as a
    mismatch of answers (the harness then restarts the server and tries once more)."""
    if isinstance(e, Refused):
        return False
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
        if isinstance(e, Refused):
            print("REFUSED: " + str(e))
            raise SystemExit(4)
        raise

if __name__ == "__main__":
    guarded(main)
