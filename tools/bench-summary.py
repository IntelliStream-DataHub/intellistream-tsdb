#!/usr/bin/env python3
"""Turns the raw logs of tools/bench-tsdb.sh into summary.md and summary.json.

    tools/bench-summary.py RESULTS_DIR

RESULTS_DIR holds what bench-tsdb.sh wrote: meta.kv (the machine and the run), commands.txt, status.txt (one line
per run: "NAME REP RC") and raw/ (the output of every run of every tool). Every number in the summary is parsed
from those files, as a mean over the repetitions with the sample standard deviation (also in percent of the mean)
and the lowest and highest run. Standard library only.

The parsers know the output formats of the tools (codec-bench, type-bench, fold-bench, tsdb-bench, tsdbc-bench,
insert-bench, scale-bench); a line a parser does not know is ignored, a run whose log has no numbers at all is
reported as a problem.
"""
import json
import math
import os
import re
import sys

NUM = r"(-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)"


# ------------------------------------------------------------------ statistics

def stats(vals):
    n = len(vals)
    mean = sum(vals) / n
    sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / (n - 1)) if n > 1 else 0.0
    return {"n": n, "mean": mean, "sd": sd, "sd_pct": (sd / abs(mean) * 100.0) if mean else 0.0,
            "min": min(vals), "max": max(vals)}


def fmt(x):
    """A number with 3-4 significant digits, thousands separated."""
    if x == 0:
        return "0"
    a = abs(x)
    if a >= 1000:
        return "{:,.0f}".format(x)
    if a >= 100:
        return "{:.1f}".format(x)
    if a >= 10:
        return "{:.2f}".format(x)
    if a >= 1:
        return "{:.3f}".format(x)
    return "{:.4g}".format(x)


# --------------------------------------------------------------------- results

class Section:
    """One tool's results: metrics[(table, row, col)] = (unit, [value per repetition])."""

    def __init__(self, sid, title, kind="long"):
        self.id, self.title, self.kind = sid, title, kind
        self.metrics = {}
        self.order = []          # (table, row, col) in first-seen order
        self.notes = {}          # (table, row) -> text (the codec a row used, ...)
        self.table_titles = {}   # table -> heading
        self.reps = 0
        self.commands = []
        self.extra = []          # lines under the table (kernels used, sizes chosen)

    def add(self, table, row, col, unit, value, rep):
        key = (table, row, col)
        if key not in self.metrics:
            self.metrics[key] = [unit, {}]
            self.order.append(key)
        self.metrics[key][1][rep] = value


def read(path):
    try:
        with open(path, errors="replace") as f:
            return f.read()
    except OSError:
        return None


def raw_files(raw, prefix):
    """Files raw/PREFIX.REP.txt as {rep: path}."""
    out = {}
    rx = re.compile("^" + re.escape(prefix) + r"\.(\d+)\.txt$")
    for name in os.listdir(raw):
        m = rx.match(name)
        if m:
            out[int(m.group(1))] = os.path.join(raw, name)
    return dict(sorted(out.items()))


# --------------------------------------------------------------------- parsers

def parse_codec(sec, text, rep):
    table = None
    cols = None
    for line in text.splitlines():
        s = line.strip()
        if not s:
            table = cols = None
            continue
        m = re.match(r"bits per value, codec alone", s)
        if m:
            table, cols = "bits_codec", None
            sec.table_titles[table] = "Bits per value, codec alone (float32 = 32 bits)"
            continue
        if s.startswith("bits per value, codec + ZSTD"):
            table, cols = "bits_zstd", None
            sec.table_titles[table] = "Bits per value, codec + ZSTD(1)"
            continue
        if s.startswith("speed, million values/s") and "codec alone" in s:
            table, cols = "speed_codec", None
            sec.table_titles[table] = "Speed, million values/s, 1 thread, median over data sets, codec alone"
            continue
        if s.startswith("speed, million values/s") and "ZSTD" in s:
            table, cols = "speed_zstd", None
            sec.table_titles[table] = "Speed, million values/s, 1 thread, median over data sets, codec + ZSTD(1)"
            continue
        if table is None:
            continue
        toks = s.split()
        if cols is None:
            cols = toks[1:] if toks[0] == "dataset" else toks
            continue
        vals = toks[1:]
        if len(vals) != len(cols):
            continue
        unit = "bits/value" if table.startswith("bits") else "M values/s"
        for c, v in zip(cols, vals):
            v = v.rstrip("*")
            try:
                sec.add(table, toks[0], c, unit, float(v), rep)
            except ValueError:
                pass   # n/a: the codec cannot code this data


def parse_type(sec, text, rep):
    for line in text.splitlines():
        t = line.split()
        if len(t) < 6 or t[0] == "dataset":
            continue
        codec, dec, ratio, bits, typ = t[-1], t[-2], t[-3], t[-4], t[-5]
        label = " ".join(t[:-5])
        try:
            row = "%s (%s, %s)" % (label, typ, codec)
            sec.add("types", row, "bits/value", "bits/value", float(bits), rep)
            sec.add("types", row, "ratio vs raw", "x", float(ratio.rstrip("x")), rep)
            sec.add("types", row, "decode", "M values/s", float(dec), rep)
        except ValueError:
            pass


def parse_fold(sec, cfg, text, rep):
    for line in text.splitlines():
        if line.startswith("# kernels:"):
            sec.extra_kernels = getattr(sec, "extra_kernels", {})
            sec.extra_kernels[cfg] = line.split(":", 1)[1].strip()
            continue
        m = re.match(r"^(.*?)\s+" + NUM + r" M rows/s\s*$", line)
        if m:
            sec.add("fold", m.group(1).strip(), cfg, "M rows/s", float(m.group(2)), rep)


def parse_tsdb_bench(sec, text, rep):
    for line in text.splitlines():
        m = re.match(r"^insert\+generate\s+" + NUM + r" s\s+" + NUM + r" M rows/s", line)
        if m:
            sec.add("load", "insert + generate (4 writer threads, fsync on)", "time", "s", float(m.group(1)), rep)
            sec.add("load", "insert + generate (4 writer threads, fsync on)", "rate", "M rows/s", float(m.group(2)), rep)
            continue
        m = re.match(r"^optimize\s+" + NUM + r" s", line)
        if m:
            sec.add("load", "optimize (merge to one part per partition)", "time", "s", float(m.group(1)), rep)
            continue
        m = re.match(r"^storage\s+" + NUM + r" MiB\s+" + NUM + r" bytes/row", line)
        if m:
            sec.add("load", "storage", "size", "MiB", float(m.group(1)), rep)
            sec.add("load", "storage", "bytes/row", "bytes/row", float(m.group(2)), rep)
            continue
        m = re.match(r"^((?:raw scan|agg|group by).*?)\s{2,}" + NUM + r" s\s+" + NUM +
                     r" M rows/s(?:\s+" + NUM + r" ms/series)?", line)
        if m:
            row = m.group(1).strip()
            sec.add("queries", row, "time", "s", float(m.group(2)), rep)
            sec.add("queries", row, "rate", "M rows/s", float(m.group(3)), rep)
            if m.group(4):
                sec.add("queries", row, "per series", "ms/series", float(m.group(4)), rep)
            continue
        m = re.match(r"^\s+(.*?)\s+(\d+) thr\s+" + NUM + r" q/s\s+" + NUM + r" us/q\s+" + NUM + r" Mrows/s", line)
        if m:
            row = "%s, %s thread%s" % (m.group(1).strip(), m.group(2), "" if m.group(2) == "1" else "s")
            sec.add("random", row, "queries", "q/s", float(m.group(3)), rep)
            sec.add("random", row, "latency", "us/q", float(m.group(4)), rep)
            sec.add("random", row, "rate", "M rows/s", float(m.group(5)), rep)
            continue
        m = re.match(r"^\s+batch .*simd (\S+)", line)
        if m:
            sec.simd = m.group(1)


def kv(line):
    return dict(re.findall(r"(\w+)=(\S+)", line))


def parse_tcp(sec, kind, text, rep):
    """The load log (RESULT kind=load, flush and optimize) and the connections log."""
    for line in text.splitlines():
        if line.startswith("RESULT "):
            d = kv(line)
            if d.get("kind") == "load":
                rows, secs = float(d["rows"]), float(d["secs"])
                row = "load through tsdbc-bench (%s connections)" % d["conns"]
                sec.add("load", row, "time", "s", secs, rep)
                sec.add("load", row, "rate", "M rows/s", rows / secs / 1e6, rep)
                sec.add("load", row, "size", "bytes/row", float(d["bytes"]) / rows, rep)
                sec.add("load", row, "client cpu", "s per M rows", float(d["cpu"]) / (rows / 1e6), rep)
            continue
        m = re.match(r"^flush and optimize: " + NUM + r" s", line)
        if m:
            sec.add("load", "flush and optimize", "time", "s", float(m.group(1)), rep)
            continue
        m = re.match(r"^(\d+) connections open at once \((\d+) refused\), (\d+) queries in " + NUM + r" s = " + NUM +
                     r" q/s, (\d+) failed", line)
        if m:
            sec.add("connections", "connections held open at once", "open", "connections", float(m.group(1)), rep)
            sec.add("connections", "connections held open at once", "refused", "connections", float(m.group(2)), rep)
            sec.add("connections", "queries while holding them", "queries", "q/s", float(m.group(5)), rep)
            sec.add("connections", "queries while holding them", "failed", "queries", float(m.group(6)), rep)


def parse_tcp_query(sec, text, rep):
    """RESULT lines come right after their human line, which has the class label."""
    label = None
    for line in text.splitlines():
        m = re.match(r"^\s+(.*?)\s+\d+ conn\s+" + NUM + r" q/s", line)
        if m:
            label = m.group(1).strip()
            continue
        if line.startswith("RESULT ") and "kind=query" in line:
            d = kv(line)
            th, q, secs = int(d["threads"]), float(d["queries"]), float(d["secs"])
            row = "%s, %d connection%s" % (label or d["class"], th, "" if th == 1 else "s")
            sec.add("queries", row, "queries", "q/s", q / secs, rep)
            sec.add("queries", row, "latency", "us/q", secs * th / q * 1e6, rep)
            sec.add("queries", row, "rate", "M rows/s", float(d["rows"]) / secs / 1e6, rep)


def parse_insert(sec, text, rep):
    for line in text.splitlines():
        t = line.split()
        if len(t) < 8 or t[0] in ("mode", "durable"):
            continue
        try:
            ins, rows, mbs, p50, p99 = (float(x) for x in t[-6:-1])
            float(t[-1])
        except ValueError:
            continue
        batch = t[-7]
        label = " ".join(t[:-7])
        row = "%s, %s row%s per call" % (label, batch, "" if batch == "1" else "s")
        sec.add("durable", row, "calls", "calls/s", ins, rep)
        sec.add("durable", row, "rows", "rows/s", rows, rep)
        sec.add("durable", row, "p50", "ms", p50, rep)
        sec.add("durable", row, "p99", "ms", p99, rep)


def parse_decode(sec, text, rep):
    """decode-bench: the unpack table (width, loops, kernel, avx2, pdep) and the codec lines; a cell is 'mean (sd)'
    of the rounds inside one run, of which the mean is taken."""
    cols = None
    for line in text.splitlines():
        if line.startswith("# kernels:"):
            sec.simd = line.split(":", 1)[1].strip()
            continue
        m = re.match(r"^unpack, M values/s", line)
        if m:
            sec.table_titles["unpack"] = "Bit unpacking by width, M values/s (a dash: that variant has no code for it)"
            cols = []
            continue
        if cols is not None and not cols:
            t = line.split()
            if t and t[0] == "w":
                cols = t[1:]
            continue
        if cols:
            t = re.sub(r"\(\s*\d+\)", "", line).split()
            if t and t[0].isdigit() and len(t) == len(cols) + 1:
                for c, v in zip(cols, t[1:]):
                    if v != "-":
                        sec.add("unpack", "%2d bits" % int(t[0]), c, "M values/s", float(v), rep)
                continue
        m = re.match(r"^codecs, M values/s", line)
        if m:
            sec.table_titles["codecs"] = "Codecs, M values/s (one granule of 8192 values)"
            cols = None
            continue
        m = re.match(r"^(\S.*?)\s+" + NUM + r" \(\s*" + NUM + r"\)\s*$", line)
        if m and "codecs" in sec.table_titles:
            sec.add("codecs", m.group(1), "M values/s", "M values/s", float(m.group(2)), rep)


def parse_scale_load(sec, text, rep):
    for line in text.splitlines():
        m = re.match(r"^inserted " + NUM + r" G rows in " + NUM + r" s: " + NUM + r" M rows/s; (\d+) parts, " + NUM +
                     r" GB", line)
        if m:
            sec.add("load", "insert", "rows", "G rows", float(m.group(1)), rep)
            sec.add("load", "insert", "time", "s", float(m.group(2)), rep)
            sec.add("load", "insert", "rate", "M rows/s", float(m.group(3)), rep)
            sec.add("load", "insert", "parts", "parts", float(m.group(4)), rep)
            continue
        m = re.match(r"^merges settled after " + NUM + r" s: (\d+) parts in (\d+) partitions, (\d+) merges, " + NUM +
                     r" GB = " + NUM + r" MB, " + NUM + r" bytes/row", line)
        if m:
            sec.add("load", "after the merges settled", "time", "s", float(m.group(1)), rep)
            sec.add("load", "after the merges settled", "parts", "parts", float(m.group(2)), rep)
            sec.add("load", "after the merges settled", "size", "GB", float(m.group(5)), rep)
            sec.add("load", "after the merges settled", "bytes/row", "bytes/row", float(m.group(7)), rep)


def parse_scale_query(sec, text, rep):
    for line in text.splitlines():
        m = re.match(r"^(\S+)\s+(.*?)\s+(\d+) q\s+mean\s+" + NUM + r" ms\s+p50\s+" + NUM + r" ms\s+min\s+" + NUM +
                     r" ms\s+" + NUM + r" rows/q\s+" + NUM + r" M rows/s", line)
        if m:
            row = "%s (%s)" % (m.group(2).strip(), m.group(1))
            sec.add("queries", row, "mean", "ms", float(m.group(4)), rep)
            sec.add("queries", row, "p50", "ms", float(m.group(5)), rep)
            sec.add("queries", row, "min", "ms", float(m.group(6)), rep)
            sec.add("queries", row, "rate", "M rows/s", float(m.group(8)), rep)


# ---------------------------------------------------------------- collecting

def collect(raw, status):
    """Builds the sections from raw/, in run order. Returns (sections, problems)."""
    secs = []
    problems = []

    def runs(prefix):
        return raw_files(raw, prefix)

    def simple(sid, title, prefix, parser, kind="long"):
        files = runs(prefix)
        if not files:
            return None
        sec = Section(sid, title, kind)
        for rep, path in files.items():
            text = read(path) or ""
            before = len(sec.metrics) and sum(len(v[1]) for v in sec.metrics.values())
            parser(sec, text, rep)
            after = sum(len(v[1]) for v in sec.metrics.values())
            if after == before:
                problems.append("%s run %d: no numbers found in raw/%s" % (sid, rep, os.path.basename(path)))
        sec.reps = len(files)
        secs.append(sec)
        return sec

    simple("codec", "Value codecs, float32 (codec-bench)", "codec", parse_codec, "matrix")
    simple("types", "Value codecs, other types (type-bench)", "type", parse_type, "long")
    simple("decode", "Bit unpacking and integer/decimal codecs (decode-bench)", "decode", parse_decode, "matrix")

    fold_dir = os.path.join(raw, "fold")
    if os.path.isdir(fold_dir):
        sec = Section("fold", "Aggregation kernels (fold-bench, interleaved rounds)", "matrix")
        sec.table_titles["fold"] = "Million rows/s per kernel configuration (one core, rows in cache)"
        cfgs = []
        rx = re.compile(r"^(.*)\.(\d+)$")
        names = sorted(os.listdir(fold_dir), key=lambda n: (rx.match(n).group(1) if rx.match(n) else n,
                                                            int(rx.match(n).group(2)) if rx.match(n) else 0))
        for name in names:
            m = rx.match(name)
            if not m:
                continue
            # "v2:scalar" sets TSDB_CPU and TSDB_SIMD, ":scalar" only TSDB_SIMD, "-" neither
            cfg, rep = m.group(1).lstrip(":") or "default", int(m.group(2))
            if cfg not in cfgs:
                cfgs.append(cfg)
            parse_fold(sec, cfg, read(os.path.join(fold_dir, name)) or "", rep)
            sec.reps = max(sec.reps, rep)
        # configurations in the order of the file fold-order.txt, if the driver wrote one
        order = read(os.path.join(raw, "fold-order.txt"))
        if order:
            sec.cfg_order = [c.lstrip(":") or "default" for c in order.split()]
        secs.append(sec)

    for sid, title, prefix in (("embedded", "Embedded: tsdb-bench, float32, 100 series x 1 M rows", "tsdb-bench-f32"),
                               ("embedded64", "Embedded: tsdb-bench, float64", "tsdb-bench-f64"),
                               ("embedded_int64", "Embedded: tsdb-bench, int64", "tsdb-bench-i64")):
        simple(sid, title, prefix, parse_tsdb_bench)

    # server: load / optimize / queries at each connection count / connections, one fresh server per repetition
    sec = Section("server", "Server: tsdbd over TCP (tsdbc-bench)", "long")
    seen = False
    for rep, path in runs("tcp-load").items():
        parse_tcp(sec, "load", read(path) or "", rep)
        seen = True
        sec.reps = max(sec.reps, rep)
    for prefix in sorted({re.sub(r"\.\d+\.txt$", "", n) for n in os.listdir(raw) if n.startswith("tcp-query-c")}):
        for rep, path in runs(prefix).items():
            parse_tcp_query(sec, read(path) or "", rep)
            seen = True
    for rep, path in runs("tcp-conns").items():
        parse_tcp(sec, "conns", read(path) or "", rep)
        seen = True
    if seen:
        if not sec.metrics:
            problems.append("server: no numbers found in raw/tcp-*")
        secs.append(sec)

    simple("durable", "Durable writes (insert-bench, fsync on)", "insert", parse_insert)

    if os.path.exists(os.path.join(raw, "scale-load.1.txt")) or runs("scale-query"):
        sec = Section("scale", "Large data set (scale-bench, embedded)", "long")
        for rep, path in runs("scale-load").items():
            parse_scale_load(sec, read(path) or "", rep)
        for rep, path in runs("scale-query").items():
            parse_scale_query(sec, read(path) or "", rep)
            sec.reps = max(sec.reps, rep)
        if not sec.metrics:
            problems.append("scale: no numbers found in raw/scale-*")
        secs.append(sec)

    # bench/cpu-compare scripts print their own tables (means and spread included); kept as they are
    texts = sorted(n for n in os.listdir(raw) if n.startswith("cpucmp-"))
    if texts:
        sec = Section("cpu_compare", "CPU comparison (bench/cpu-compare)", "text")
        sec.texts = [(n, read(os.path.join(raw, n)) or "") for n in texts]
        secs.append(sec)
    return secs, problems


# ------------------------------------------------------------------ rendering

def read_kv(path):
    d = {}
    text = read(path) or ""
    for line in text.splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            d[k.strip()] = v.strip()
    return d


def section_stats(sec):
    out = []
    for key in sec.order:
        unit, byrep = sec.metrics[key]
        vals = [byrep[r] for r in sorted(byrep)]
        st = stats(vals)
        st["values"] = vals
        out.append((key, unit, st))
    return out


def md_escape(s):
    return s.replace("|", "\\|")


def cell_text(st):
    if st["n"] == 1:
        return fmt(st["mean"])
    return "%s ±%.1f%% (%s - %s)" % (fmt(st["mean"]), st["sd_pct"], fmt(st["min"]), fmt(st["max"]))


def render_long(sec, rows, lines):
    """One row per thing measured, one column per measure; a cell is mean ±sd% (lowest - highest run)."""
    tables = []
    for key, unit, st in rows:
        if key[0] not in tables:
            tables.append(key[0])
    heading = {"load": "Load", "queries": "Queries", "random": "Random windows (2 s each)",
               "connections": "Connections", "durable": "Durable inserts"}
    for t in tables:
        sub = [(k, u, s) for k, u, s in rows if k[0] == t]
        if len(tables) > 1:
            lines.append("**%s**" % heading.get(t, t))
            lines.append("")
        rws, cols, units = [], [], {}
        for k, u, s in sub:
            if k[1] not in rws:
                rws.append(k[1])
            if k[2] not in cols:
                cols.append(k[2])
            units[k[2]] = u
        cell = {(k[1], k[2]): s for k, u, s in sub}
        lines.append("| | " + " | ".join("%s [%s]" % (c, units[c]) for c in cols) + " |")
        lines.append("|---|" + "---:|" * len(cols))
        for r in rws:
            lines.append("| %s | " % md_escape(r) + " | ".join(
                cell_text(cell[(r, c)]) if (r, c) in cell else "" for c in cols) + " |")
        n = max(s["n"] for _, _, s in sub)
        lines.append("")
        lines.append("Each cell: mean ±sd %% of the mean (lowest - highest run) over %d run%s." % (n, "" if n == 1 else "s")
                     if n > 1 else "One run.")
        lines.append("")



def render_matrix(sec, rows, lines):
    tables = []
    for key, unit, st in rows:
        if key[0] not in tables:
            tables.append(key[0])
    for t in tables:
        sub = [(k, u, s) for k, u, s in rows if k[0] == t]
        rws, cls = [], []
        for k, u, s in sub:
            if k[1] not in rws:
                rws.append(k[1])
            if k[2] not in cls:
                cls.append(k[2])
        order = getattr(sec, "cfg_order", None)
        if order:
            cls = [c for c in order if c in cls] + [c for c in cls if c not in order]
        cell = {(k[1], k[2]): s for k, u, s in sub}
        unit = sub[0][1]
        lines.append("**%s** (%s)" % (sec.table_titles.get(t, t), "mean of %d run%s" % (
            max(s["n"] for _, _, s in sub), "" if max(s["n"] for _, _, s in sub) == 1 else "s")))
        lines.append("")
        lines.append("| | " + " | ".join(cls) + " |")
        lines.append("|---|" + "---:|" * len(cls))
        for r in rws:
            lines.append("| %s | " % md_escape(r) + " | ".join(
                fmt(cell[(r, c)]["mean"]) if (r, c) in cell else "" for c in cls) + " |")
        sds = [s["sd_pct"] for _, _, s in sub if s["n"] > 1]
        mn = [s["sd_pct"] for _, _, s in sub if s["n"] > 1]
        if sds:
            srt = sorted(sds)
            lines.append("")
            lines.append("Unit: %s. Spread over the runs: sd %% median %.1f, highest %.1f (all values with their"
                         " sd and min-max are in summary.json)." % (unit, srt[len(srt) // 2], srt[-1]))
        else:
            lines.append("")
            lines.append("Unit: %s (one run)." % unit)
        lines.append("")


HEADLINES = [
    # (label, section, table, row regex, col, unit)
    ("Embedded load (4 writer threads, fsync on)", "embedded", "load", r"^insert", "rate", "M rows/s"),
    ("Embedded storage", "embedded", "load", r"^storage", "bytes/row", "bytes/row"),
    ("Embedded raw scan, 1 thread", "embedded", "queries", r"^raw scan", "rate", "M rows/s"),
    ("Embedded, whole range, one bucket per series", "embedded", "queries", r"^agg whole range", "rate", "M rows/s"),
    ("Embedded, buckets of 3600 samples", "embedded", "queries", r"^agg buckets of 3600", "rate", "M rows/s"),
    ("Embedded, buckets of 60 samples (decodes all)", "embedded", "queries", r"^agg buckets of 60 ", "rate",
     "M rows/s"),
    ("Embedded, random raw windows of 3600 rows, 1 thread", "embedded", "random", r"^raw, 3600.*1 thread$",
     "queries", "q/s"),
    ("Server load, 4 connections", "server", "load", r"^load through", "rate", "M rows/s"),
    ("Server, random raw windows, 1 connection", "server", "queries", r"^raw.*, 1 connection$", "queries", "q/s"),
    ("Durable append, fsync each, 1 thread", "durable", "durable", r"^append, fsync each .*1 thr, 1 row", "calls",
     "calls/s"),
    ("Durable append, fsync each, 64 threads (group commit)", "durable", "durable", r"^append, fsync each .*64 thr",
     "calls", "calls/s"),
    ("Durable insert, 1000 rows through WAL + memtable", "durable", "durable", r"^insert batch -> WAL.*, 1k rows",
     "rows", "rows/s"),
    ("Large data set: load", "scale", "load", r"^insert$", "rate", "M rows/s"),
]


def headline(secs, lines):
    by = {s.id: s for s in secs}
    out = []
    for label, sid, table, rx, col, unit in HEADLINES:
        sec = by.get(sid)
        if not sec:
            continue
        for key in sec.order:
            if key[0] == table and key[2] == col and re.search(rx, key[1]):
                st = stats([sec.metrics[key][1][r] for r in sorted(sec.metrics[key][1])])
                out.append("| %s | %s %s | %s | %d |" % (label, fmt(st["mean"]), unit,
                                                    "%.1f%%" % st["sd_pct"] if st["n"] > 1 else "-", st["n"]))
                break
    if out:
        lines.append("| | mean | sd | runs |")
        lines.append("|---|---:|---:|---:|")
        lines.extend(out)
        lines.append("")


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    res = sys.argv[1]
    raw = os.path.join(res, "raw")
    meta = read_kv(os.path.join(res, "meta.kv"))
    commands = (read(os.path.join(res, "commands.txt")) or "").splitlines()
    status = []
    for line in (read(os.path.join(res, "status.txt")) or "").splitlines():
        t = line.split()
        if len(t) == 3:
            status.append((t[0], t[1], t[2]))
    warnings = (read(os.path.join(res, "warnings.txt")) or "").splitlines()
    secs, problems = collect(raw, status)
    for name, rep, rc in status:
        if rc != "0":
            problems.append("%s run %s exited with status %s (raw/%s.%s.txt)" % (name, rep, rc, name, rep))

    # commands per section, by the prefix the driver used
    cmds = {}
    for c in commands:
        name, _, rest = c.partition("\t")
        cmds.setdefault(name, []).append(rest)

    js = {"meta": meta, "warnings": warnings, "problems": problems, "sections": []}
    md = []
    md.append("# tsdb benchmark summary")
    md.append("")
    md.append("%s, %s mode, %s run%s of each test, %s, git %s" % (
        meta.get("date", "?"), meta.get("mode", "?"), meta.get("reps", "?"), "" if meta.get("reps") == "1" else "s",
        meta.get("tsdb_version", "?"), meta.get("git", "?")))
    md.append("")
    md.append("Every number is the mean of the repeated runs; \"sd %\" is the sample standard deviation in percent of"
              " the mean, \"min - max\" the lowest and highest run.")
    md.append("")
    md.append("## Machine")
    md.append("")
    md.append("| | |")
    md.append("|---|---|")
    for k, label in (("host", "host"), ("cpu", "CPU"), ("cpu_topology", "cores and threads"),
                     ("cpu_flags", "CPU features"), ("virt", "virtualization"), ("memory", "memory"),
                     ("kernel", "kernel"), ("os", "OS"), ("governor", "CPU governor"),
                     ("scratch", "scratch directory"), ("scratch_fs", "scratch file system"),
                     ("scratch_dev", "scratch device"), ("load_before", "load before the run"),
                     ("compiler", "compiler"), ("build", "build"), ("build_variant", "build details"),
                     ("cflags", "compile flags (as make expands them)"), ("arch", "target architecture flags"),
                     ("tsdb_version", "tsdb version"), ("git", "git commit"),
                     ("cpu_level", "codec CPU level at run time"), ("simd", "aggregation kernels chosen"),
                     ("io", "server event loop"), ("duration", "time the run took")):
        if meta.get(k):
            md.append("| %s | %s |" % (label, md_escape(meta[k])))
    md.append("")
    if warnings:
        md.append("**Warnings**")
        md.append("")
        for w in warnings:
            md.append("- %s" % w)
        md.append("")
    if problems:
        md.append("**Problems**")
        md.append("")
        for p in problems:
            md.append("- %s" % p)
        md.append("")
    md.append("## At a glance")
    md.append("")
    start = len(md)
    headline(secs, md)
    if len(md) == start:
        md.append("(no results)")
        md.append("")

    for sec in secs:
        rows = section_stats(sec)
        md.append("## %s" % sec.title)
        md.append("")
        sizes = meta.get("size_" + sec.id)
        if sizes:
            md.append(sizes)
            md.append("")
        if sec.id == "fold":
            kern = getattr(sec, "extra_kernels", {})
            if kern:
                md.append("Kernels each configuration ran: " + "; ".join("%s: %s" % (c, k) for c, k in kern.items())
                          + ".")
                md.append("")
        simd = getattr(sec, "simd", None)
        if simd:
            md.append("Aggregation kernels used: %s." % simd)
            md.append("")
        if sec.kind == "text":
            for name, content in sec.texts:
                md.append("`raw/%s`:" % name)
                md.append("")
                md.append("```")
                tl = content.rstrip("\n").split("\n")
                md.extend(tl[:150] + (["... (%d more lines in the raw file)" % (len(tl) - 150)] if len(tl) > 150 else []))
                md.append("```")
                md.append("")
            js["sections"].append({"id": sec.id, "title": sec.title, "text": {n: c for n, c in sec.texts}})
            continue
        if not rows:
            md.append("No numbers (see Problems).")
            md.append("")
        elif sec.kind == "matrix":
            render_matrix(sec, rows, md)
        else:
            render_long(sec, rows, md)
        sc = [c for n, c in cmds.items() if n.split(".")[0] in (sec.id, ) or n.startswith(sec.id)]
        js["sections"].append({
            "id": sec.id, "title": sec.title, "runs": sec.reps,
            "metrics": [{"key": "%s/%s/%s/%s" % (sec.id, k[0], k[1], k[2]), "table": k[0], "row": k[1],
                         "measure": k[2], "unit": u, "note": sec.notes.get((k[0], k[1])),
                         "mean": st["mean"], "sd": st["sd"], "sd_pct": st["sd_pct"], "min": st["min"],
                         "max": st["max"], "n": st["n"], "values": st["values"]} for k, u, st in rows]})

    md.append("## Commands")
    md.append("")
    md.append("```")
    seen = set()
    for c in commands:
        name, _, rest = c.partition("\t")
        if rest and rest not in seen:
            seen.add(rest)
            md.append(rest)
    md.append("```")
    md.append("")
    md.append("Raw output of every run: `raw/`. Parsed by `tools/bench-summary.py`.")
    md.append("")
    js["commands"] = [c.partition("\t")[2] for c in commands]
    with open(os.path.join(res, "summary.md"), "w") as f:
        f.write("\n".join(md))
    with open(os.path.join(res, "summary.json"), "w") as f:
        json.dump(js, f, indent=1)
        f.write("\n")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
