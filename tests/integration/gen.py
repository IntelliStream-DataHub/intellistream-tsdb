"""Test data for the integration tests: write scripts for the four clients, their queries, and an independent
model of what every query must answer (last write wins, nulls, calendar periods from Python's zoneinfo).

Tokens: the text form of one value, the same in write scripts and in client output.
  float32  8 hex digits of the bit pattern      float64  16 hex digits       int64  decimal
  bool     0 or 1                               state    the name            null   N
"""
import datetime
import struct
import zoneinfo

I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1
NULL_F32, NULL_F64 = 0x7FC01001, 0x7FF8000000001001
T_PERIOD = ("float32", "float64", "int64", "bool", "state")
STATES = ["idle", "run", "fault", "größe", "日本語", "x"]
# Havana and Beirut change their clocks AT midnight: a day starts in a gap (Havana 2024-03-10, Beirut 2024-03-31)
# or twice (Havana 2024-11-03), which exercises the gap and overlap branches of every calendar implementation.
ZONES = ["Europe/Oslo", "America/New_York", "Asia/Kolkata", "America/Havana", "Asia/Beirut"]
HOUR = 3600_000
DAY = 24 * HOUR

# Bit patterns that must survive a round trip as values. The null of each float type is 0x7fc01001 /
# 0x7ff8000000001001 exactly, so its neighbours and its sign-flipped twin are values.
SPECIAL_F32 = [0x7F800000, 0xFF800000, 0x80000000, 0x00000000, 0x7FC00001, 0xFFC12345, 0x7FC01000, 0x7FC01002,
               0xFFC01001, 0x00000001, 0x007FFFFF, 0x7F7FFFFF, 0xFF7FFFFF, 0x7FE00000, 0x7FC00000, 0xFFC00000]
SPECIAL_F64 = [0x7FF0000000000000, 0xFFF0000000000000, 0x8000000000000000, 0x0000000000000000, 0x7FF8000000000001,
               0xFFF8123456789ABC, 0x7FF8000000001000, 0x7FF8000000001002, 0xFFF8000000001001, 0x0000000000000001,
               0x7FF0000000000001, 0x7FF8000000000000]
# the largest doubles make a sum overflow depending on the order of the additions: rare, and the model skips those sums
HUGE_F64 = [0x7FEFFFFFFFFFFFFF, 0xFFEFFFFFFFFFFFFF]
SPECIAL_I64 = [I64_MAX, I64_MIN + 1, 0, -1, 1, I64_MAX - 1, 1 << 53, -(1 << 53) - 1]


def f32bits(x):
    return struct.unpack("<I", struct.pack("<f", x))[0]


def f32val(bits):
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def f64val(bits):
    return struct.unpack("<d", struct.pack("<Q", bits))[0]


def f64bits(x):
    return struct.unpack("<Q", struct.pack("<d", x))[0]


# ---------------------------------------------------------------- values

def value_token(rng, ty, i, nulls=True, special=True):
    """One random token of type ty; about 8% null, 4% special bit patterns."""
    r = rng.random()
    if nulls and r < 0.08:
        return "N"
    if special and r < 0.105:
        if ty == "float32":
            return "%08x" % rng.choice(SPECIAL_F32)
        if ty == "float64":
            return "%016x" % rng.choice(HUGE_F64 if r < 0.0805 else SPECIAL_F64)
        if ty == "int64":
            return str(rng.choice(SPECIAL_I64))
    if ty == "float32":
        return "%08x" % f32bits(rng.uniform(-1000, 1000) * rng.choice([1, 1, 1e-3, 1e3]))
    if ty == "float64":
        return "%016x" % f64bits(rng.uniform(-1e6, 1e6) * rng.choice([1, 1, 1e-6, 1e6]))
    if ty == "int64":
        return str(rng.randint(-10**9, 10**9))
    if ty == "bool":
        return str(rng.randint(0, 1))
    return rng.choice(STATES)


def dense_times(n, t0=1709596800000, step=61_000):  # 2024-03-05T00:00Z, a row a minute or so for three weeks
    return [t0 + i * step for i in range(n)]


def sparse_times(scale=1.0):
    """Rows around the 2024 clock changes: Oslo 03-31 and 10-27, New York 03-10 and 11-03."""
    out = []
    for day0 in ("2024-03-09", "2024-03-30", "2024-10-26", "2024-11-02"):
        t = int(datetime.datetime.fromisoformat(day0).replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
        step = 7 * 60_000
        out += [t + i * step for i in range(int(3 * 24 * 60 / 7 * scale))]
    return out


# ---------------------------------------------------------------- scripts and the model

class Model:
    """What the database must hold: per series its type and ts -> token (last write wins)."""

    def __init__(self):
        self.types = {}
        self.rows = {}

    def series(self, name, ty):
        self.types[name] = ty
        self.rows.setdefault(name, {})

    def put(self, name, ts, tok):
        self.rows[name][ts] = tok


class Script:
    """A write script in the text form the clients read, built while the model follows it."""

    def __init__(self, model):
        self.model = model
        self.out = []

    def series(self, name, ty):
        self.model.series(name, ty)
        self.out.append("S %s %s" % (name, ty))

    def batch(self, name, ty, level, flags, rows):
        """rows: [(ts, token)] for one series, or [(name, ts, token)] when name is None (an M batch)."""
        if name is None:
            self.out.append("M %s %d %s %d" % (ty, level, flags, len(rows)))
            for n, ts, tok in rows:
                self.out.append("%s %d %s" % (n, ts, tok))
                self.model.put(n, ts, tok)
        else:
            self.out.append("B %s %d %s %d" % (name, level, flags, len(rows)))
            for ts, tok in rows:
                self.out.append("%d %s" % (ts, tok))
                self.model.put(name, ts, tok)

    def text(self):
        return "\n".join(self.out) + "\n"


LEVELS = [1, 0, 3, -3, 1, 0]


def rand_flags(rng, legacy=False):
    f = ["async" if rng.random() < 0.4 else "sync"]
    if legacy:
        f.append("legacy")
    return ",".join(f)


def write_series(rng, sc, name, ty, times, nulls=True, legacy=False, chunk=2500, corrections=2):
    """Inserts times in shuffled chunks, then corrects random rows (value to value, value to null, null to
    value) in later batches, twice."""
    sc.series(name, ty)
    times = list(times)
    rng.shuffle(times)
    for i in range(0, len(times), chunk):
        part = times[i:i + chunk]
        if rng.random() < 0.5:
            part.sort()
        sc.batch(name, ty, rng.choice(LEVELS), rand_flags(rng, legacy),
                 [(t, value_token(rng, ty, t, nulls=nulls)) for t in part])
    correct(rng, sc, name, ty, sorted(times), nulls, legacy, corrections)


def correct(rng, sc, name, ty, times, nulls=True, legacy=False, rounds=1):
    for _ in range(rounds):
        k = max(5, len(times) // 14)
        picks = rng.sample(times, min(k, len(times)))
        for i in range(0, len(picks), 400):
            part = picks[i:i + 400]
            sc.batch(name, ty, rng.choice(LEVELS), rand_flags(rng, legacy),
                     [(t, value_token(rng, ty, t, nulls=nulls)) for t in part])


def write_multi(rng, sc, names, ty, times, chunk=1500):
    """M batches: rows of several series of one type in one insert."""
    for n in names:
        sc.series(n, ty)
    rows = [(rng.choice(names), t) for t in times for _ in range(1)]
    rows += [(n, t + 1) for n, t in rows[: len(rows) // 3]]
    rng.shuffle(rows)
    seen = set()
    rows = [r for r in rows if not (r in seen or seen.add(r))]
    for i in range(0, len(rows), chunk):
        sc.batch(None, ty, rng.choice(LEVELS), rand_flags(rng), [(n, t, value_token(rng, ty, t)) for n, t in rows[i:i + chunk]])
    # a correction across both series
    picks = rng.sample(rows, max(5, len(rows) // 12))
    seen = set()
    picks = [r for r in picks if not (r in seen or seen.add(r))]
    sc.batch(None, ty, rng.choice(LEVELS), rand_flags(rng), [(n, t, value_token(rng, ty, t)) for n, t in picks])


def lang_script(lang, rng, model, scale=1.0):
    """The first write wave of one client: a set of series covering every type, under the prefix lang."""
    sc = Script(model)
    sparse = sparse_times(scale)
    dense_n = max(500, int(30000 * scale))
    write_series(rng, sc, lang + ".f32", "float32", sparse)
    write_series(rng, sc, lang + ".f32leg", "float32", dense_times(max(300, int(5000 * scale)), 1709683200000, 253_000),
                 nulls=False, legacy=True, chunk=1200)
    write_series(rng, sc, lang + ".f64", "float64", dense_times(dense_n), chunk=4000)
    write_series(rng, sc, lang + ".i64", "int64", sorted(set(dense_times(dense_n, 1709600000000, 59_000)) | set(sparse[:600])),
                 chunk=5000)
    write_series(rng, sc, lang + ".bool", "bool", sparse)
    write_series(rng, sc, lang + ".state", "state", sparse)
    write_multi(rng, sc, [lang + ".mA", lang + ".mB"], "int64", sparse[:2500])
    return sc.text()


def wave2_script(lang, rng, model, scale=1.0):
    """A second wave on top of the first: corrections to every series and a new one."""
    sc = Script(model)
    for name, ty in sorted(model.types.items()):
        if not name.startswith(lang + "."):
            continue
        times = sorted(model.rows[name])
        legacy = name.endswith(".f32leg")
        correct(rng, sc, name, ty, times, nulls=not legacy, legacy=legacy)
    new = lang + ".late"
    sc.series(new, "float64")
    sc.batch(new, "float64", 1, "sync", [(t, value_token(rng, "float64", t)) for t in sparse_times(0.2)])
    return sc.text()


# ---------------------------------------------------------------- queries

def zone_bounds_ok(tz):
    return tz in ("-", "UTC") or tz in ZONES


def queries_for(name, ty, rows, quick=False):
    ts = sorted(rows)
    lo, hi, n = ts[0], ts[-1], len(ts)
    a, b = ts[n // 3], ts[2 * n // 3]
    q = []
    for order, lim in (("asc", 0), ("desc", 0), ("asc", 7), ("desc", 13), ("desc", 1), ("asc", 10**6)):
        q.append("raw %s %d %d %s %d" % (name, I64_MIN, I64_MAX, order, lim))
    q += [
        "raw %s %d %d asc 0" % (name, a, b),
        "raw %s %d %d desc 5" % (name, a, b),
        "raw %s %d %d asc 0" % (name, a, a + 1),
        "raw %s %d %d asc 0" % (name, a, a),
        "raw %s %d %d asc 0" % (name, hi, hi + 1),
        "raw %s %d %d desc 0" % (name, hi + 1, I64_MAX),
        "raw %s %d %d asc 3" % (name, I64_MIN, lo),
        "raw %s %d %d asc 0" % (name, lo, lo + 1),
    ]
    if ty == "float32":
        q += ["f32raw %s %d %d asc 0" % (name, I64_MIN, I64_MAX), "f32raw %s %d %d desc 5" % (name, a, b),
              "f32raw %s %d %d asc 2" % (name, I64_MIN, I64_MAX)]
    elif name.endswith(".i64"):
        q.append("f32raw %s %d %d asc 0" % (name, I64_MIN, I64_MAX))  # wrong type: an error everywhere
    e = hi + 1
    for iv in (HOUR, DAY, 7 * DAY, 900_000):
        if iv == 900_000 and hi - lo > 40 * DAY:
            continue
        q.append("agg %s %d %d %d" % (name, lo, e, iv))
    q += ["agg %s %d %d 0" % (name, lo, e), "agg %s %d %d %d" % (name, a, b, HOUR)]
    for tz in ("-", "UTC") + tuple(ZONES):
        q.append("grp %s %d %d day 1 %s asc 0" % (name, lo, e, tz))
    q += [
        "grp %s %d %d day 1 Europe/Oslo desc 3" % (name, lo, e),
        "grp %s %d %d day 1 America/New_York asc 4" % (name, a, b),
        "grp %s %d %d day 3 Asia/Kolkata asc 0" % (name, lo, e),
        "grp %s %d %d week 1 Europe/Oslo asc 0" % (name, lo, e),
        "grp %s %d %d month 1 America/New_York asc 0" % (name, lo, e),
        "grp %s %d %d quarter 1 Europe/Oslo asc 0" % (name, lo, e),
        "grp %s %d %d year 1 - asc 0" % (name, lo, e),
        "grp %s %d %d hour 6 UTC asc 0" % (name, lo, min(e, lo + 6 * DAY)),
        "grp %s %d %d minute 15 - desc 20" % (name, lo, min(e, lo + 3 * DAY)),
        "grp %s %d %d none 1 - asc 0" % (name, lo, e),
        "grp %s %d %d none 1 - asc 0" % (name, I64_MIN, I64_MAX),
        "grp %s %d %d day 1 - asc 0" % (name, I64_MIN, I64_MAX),
        "grp %s %d %d day 1 Mars/Olympus asc 0" % (name, lo, e),
    ]
    q.append("grp %s %d %d day 1 Europe/Oslo asc 0" % (name, I64_MIN, I64_MAX))
    bounds = sorted({lo - 5, lo + 1000, lo + DAY, a, a + 1, b, hi})
    q += [
        "bnd %s %d %d asc 0 %s" % (name, lo - 100, e, ",".join(map(str, bounds))),
        "bnd %s %d %d desc 2 %s" % (name, lo - 100, e, ",".join(map(str, bounds))),
        "bnd %s %d %d asc 0 %s" % (name, a, b, ",".join(map(str, bounds))),
        "bnd %s %d %d asc 0 %d" % (name, lo, e, lo),
    ]
    return q


# ---------------------------------------------------------------- the oracle

def tok_to_f32bits(tok):
    return NULL_F32 if tok == "N" else int(tok, 16)


def expected_blocks(queries, model):
    """For every query, the expected output lines; a line is a string, or a list of fields where None means
    "not checked" (a float sum that may round, say). None instead of lines: not modelled, only compared
    between clients."""
    out = []
    for qs in queries:
        out.append((qs, expect_one(qs.split(" "), model)))
    return out


def expect_one(q, model):
    kind, name = q[0], q[1]
    if name not in model.rows:
        return ["!!"]
    ty, rows = model.types[name], model.rows[name]
    start, end = int(q[2]), int(q[3])
    sel = sorted((t, v) for t, v in rows.items() if start <= t < end)
    if kind in ("raw", "f32raw"):
        order, limit = q[4], int(q[5])
        if kind == "f32raw" and ty != "float32":
            return ["!!"]
        if order == "desc":
            sel.reverse()
        if limit:
            sel = sel[:limit]
        if kind == "f32raw":
            return ["%d\t%08x" % (t, tok_to_f32bits(v)) for t, v in sel]
        return ["%d\t%s" % (t, v) for t, v in sel]
    if kind == "agg":
        iv = int(q[4])
        if iv == 0:
            return bucket_lines(ty, [(start, sel)], "asc", 0)
        groups = {}
        for t, v in sel:
            groups.setdefault(t // iv * iv, []).append((t, v))
        return bucket_lines(ty, sorted(groups.items()), "asc", 0)
    if kind == "grp":
        unit, cnt, tz, order, limit = q[4], int(q[5]), q[6], q[7], int(q[8])
        if unit == "none":
            return bucket_lines(ty, [(start, sel)], order, limit)
        if tz not in ("-", "UTC") and tz not in ZONES:
            return ["!!"]
        key = period_key(unit, cnt, tz)
        if key is None:
            return None
        groups = {}
        for t, v in sel:
            groups.setdefault(key(t), []).append((t, v))
        return bucket_lines(ty, sorted(groups.items()), order, limit)
    if kind == "bnd":
        order, limit = q[4], int(q[5])
        bounds = [int(x) for x in q[6].split(",")]
        groups = {}
        import bisect
        for t, v in sel:
            i = bisect.bisect_right(bounds, t) - 1
            if i >= 0:
                groups.setdefault(bounds[i], []).append((t, v))
        return bucket_lines(ty, sorted(groups.items()), order, limit)
    return None


def period_key(unit, cnt, tz):
    """ts -> the UTC start of its period, or None when the oracle does not model that grouping."""
    fixed = {"second": 1000, "minute": 60_000, "hour": HOUR}
    if tz in ("-", "UTC"):
        if unit in fixed:
            ln = fixed[unit] * cnt
            return lambda t: t // ln * ln
        if cnt != 1 and unit not in ("day",):
            return None
        if unit == "day":
            ln = DAY * cnt
            return lambda t: t // ln * ln
        zone = datetime.timezone.utc
    else:
        if unit in fixed or cnt != 1:
            return None
        zone = zoneinfo.ZoneInfo(tz)

    def key(t):
        local = datetime.datetime.fromtimestamp(t / 1000, datetime.timezone.utc).astimezone(zone)
        d = local.date()
        if unit == "day":
            first = d
        elif unit == "week":
            first = d - datetime.timedelta(days=d.weekday())
        elif unit == "month":
            first = d.replace(day=1)
        elif unit == "quarter":
            first = d.replace(month=(d.month - 1) // 3 * 3 + 1, day=1)
        else:
            first = d.replace(month=1, day=1)
        return int(datetime.datetime.combine(first, datetime.time(0), tzinfo=zone).timestamp()) * 1000

    return key


def bucket_lines(ty, groups, order, limit):
    out = []
    for start, items in groups:
        if not items:
            continue
        known = [v for _, v in items if v != "N"]
        f = [start, len(known), None, None, None, None, None, None]
        if ty in ("float32", "float64") and known:
            f[2:5] = float_aggregates(ty, known)
        if ty in ("int64", "bool") and known:
            vals = [int(v) for v in known]
            s = max(I64_MIN, min(I64_MAX, sum(vals)))
            f[5], f[6], f[7] = s, min(vals), max(vals)
        out.append(f)
    if order == "desc":
        out.reverse()
    if limit:
        out = out[:limit]
    return out


def float_aggregates(ty, known):
    """Expected [sum, min, max] of known float tokens as ("sum", x, tolerance) / ("val", x) checks, None to skip.
    Sums are checked to 1e-9 of the sum of magnitudes (the server adds in a different order for the memtable, parts
    and merged parts); NaN and infinities exactly; not at all where an order could overflow a double."""
    import math
    vals = [f32val(int(t, 16)) if ty == "float32" else f64val(int(t, 16)) for t in known]
    nan = any(v != v for v in vals)
    real = [v for v in vals if v == v]
    pos, neg = any(v == math.inf for v in real), any(v == -math.inf for v in real)
    mag = sum(abs(v) for v in real if abs(v) != math.inf)
    if nan or (pos and neg):
        s = ("sum", math.nan, 0)
    elif pos or neg:
        s = ("sum", math.inf if pos else -math.inf, 0)
    elif mag > 1e300:
        s = None
    else:
        s = ("sum", math.fsum(real), 1e-9 * mag)
    return [s, ("val", min(real) if real else math.nan), ("val", max(real) if real else math.nan)]


def parse_blocks(text):
    """Client output -> [(query line, [lines])]."""
    blocks = []
    for line in text.split("\n"):
        if line.startswith("## "):
            blocks.append((line[3:], []))
        elif line or blocks:
            if line:
                blocks[-1][1].append(line)
    return blocks


def match_line(exp, got):
    if isinstance(exp, str):
        return exp == got
    g = got.split("\t")
    if len(g) != len(exp):
        return False
    return all(e is None or field_ok(e, x) for e, x in zip(exp, g))


def field_ok(e, x):
    import math
    if isinstance(e, tuple):
        v = f64val(int(x, 16))
        if e[0] == "val":
            return (v != v) if e[1] != e[1] else v == e[1]
        if e[1] != e[1]:
            return v != v
        if abs(e[1]) == math.inf:
            return v == e[1]
        return v == v and abs(v - e[1]) <= e[2]
    return str(e) == x


def compare_oracle(text, expected, limit=8):
    """Failures (strings) of client output against expected_blocks()."""
    got = parse_blocks(text)
    fails = []
    if len(got) != len(expected):
        fails.append("answered %d queries, expected %d" % (len(got), len(expected)))
    for (gq, glines), (eq, elines) in zip(got, expected):
        if gq != eq:
            fails.append("query order differs: %r vs %r" % (gq, eq))
            break
        if elines is None:
            continue
        ok = len(glines) == len(elines) and all(match_line(e, g) for e, g in zip(elines, glines))
        if not ok:
            fails.append("%s: %s" % (eq, first_diff(elines, glines)))
        if len(fails) >= limit:
            break
    return fails


def first_diff(exp, got):
    for i in range(max(len(exp), len(got))):
        e = exp[i] if i < len(exp) else "<missing>"
        g = got[i] if i < len(got) else "<missing>"
        if i >= len(exp) or i >= len(got) or not match_line(e, g):
            return "line %d expected %s, got %r (%d vs %d lines)" % (i, e if isinstance(e, str) else "\t".join("*" if x is None else str(x) for x in e), g, len(exp), len(got))
    return "?"
