#!/usr/bin/env python3
"""The release run's results as Markdown, each table followed by how it was run.

  python3 -I tables.py OUT > tables.md

For every phase directory OUT/p<n> that has results: the results (phase 1: sizes.md of sizes_summary.py; phases 2
to 4: one table per system of its measurements: every timing line of every *.mean.txt (the mean over the runs) with
its sd and min-max, the run count, and flags for runs on a busy machine, refused or OOM-killed runs and text that
differs between runs; a mean file without a timing line fails the script; then loose.txt, answers that differ from
tsdb's within a stated bound), and under them a "How it was run" table built from the how.json
files (how.py) plus footnotes: the '-' reasons, refusals and the per-core method. A system directory with results
but no how.json, or a how.json without one of how.py's REQUIRED fields, fails the script (exit 1): no number goes
out without the way it was measured.
"""
import glob, json, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from how import REQUIRED  # noqa: E402

COLS = [("system", "system"), ("version", "version"), ("durability", "durability at acknowledgement"),
        ("loader", "loader"), ("query_client", "query client"), ("connections", "connections / threads"),
        ("one_query", "one query, one batch")]
P1_COLS = COLS[:4]
errors = []


def cell(x):
    if isinstance(x, list):
        x = "; ".join(x)
    return str(x).replace("|", "\\|").replace("\n", " ")


def load_how(f):
    try:
        h = json.load(open(f))
    except (OSError, ValueError) as e:
        errors.append("%s: %s" % (f, e))
        return None
    miss = [k for k in REQUIRED if k not in h or h[k] in ("", None, [], {})]
    if miss:
        errors.append("%s: missing %s" % (f, ", ".join(miss)))
        return None
    return h


def how_table(hows, cols):
    out = ["", "How it was run:", "", "| " + " | ".join(c[1] for c in cols) + " |",
           "|" + "---|" * len(cols)]
    notes = []
    for h in hows:
        out.append("| " + " | ".join(cell(h[c[0]]) for c in cols) + " |")
    for h in hows:
        n = h["system"]
        notes.append("- %s: image %s; settings: %s." % (n, cell(h["image"]), cell(h["settings"])))
        if h["phase"] != 1:
            notes.append("  Per core: %s. Warm-ups: %s. Runs: %s." % (h["per_core"], h["warmups"], h["runs"]))
        dash = {k: v for k, v in h["dash"].items() if k != "none"}
        if dash:
            byreason = {}
            for k, v in sorted(dash.items()):
                byreason.setdefault(v, []).append(k)
            for why, ks in byreason.items():
                notes.append("  '-' for %s: %s." % (", ".join(ks), why))
        for r in h.get("refused", []):
            notes.append("  Refused by the server (not timed): %s" % r)
        if h.get("smoke"):
            notes.append("  SMOKE RUN: tiny data, one run; these numbers are not results.")
    return out + ["", "Notes:", ""] + notes + [""]


TIME = re.compile(r"\d(?:\.\d+)?\s*(?:ms|us|s|sec|min)\b|q/s|rows/s")
CH_PCT = re.compile(r"^\d+(?:\.\d+)?%\s")  # an old clickhouse-benchmark percentile line: its summary line has them


def picked(f):
    """The timing lines of a runstats mean file, each with its '#   sd% [min-max]' line, the run count and flags."""
    lines, nruns, flags = [], None, []
    for l in open(f, errors="replace"):
        l = l.rstrip("\n")
        if l.startswith("#   "):
            if lines and not lines[-1][1]:
                lines[-1][1] = l[4:].strip()
            continue
        m = re.match(r"# mean over (\d+) runs", l)
        if m:
            nruns = int(m.group(1))
            continue
        if "TEXT DIFFERS" in l:
            flags.append("TEXT DIFFERS between runs (see the raw files)")
        if l.startswith("#") or not l.strip() or CH_PCT.match(l) or not TIME.search(l):
            continue
        lines.append([l.replace("   # TEXT DIFFERS between runs", "").strip(), ""])
    base = f[:-len(".mean.txt")]
    nq = sorted(glob.glob(base + ".*.notquiet"))
    if nq:
        flags.append("measured on a busy machine in %d run(s): %s" % (len(nq), "; ".join(
            "%s %s" % (os.path.basename(x).split(".")[-2], open(x).read().strip()) for x in nq)))
    for ext, what in (("refused", "refused by the server"), ("oom", "killed out of memory")):
        n = len(glob.glob(base + ".*." + ext))
        if n:
            flags.append("%d run(s) %s, not averaged" % (n, what))
    return lines, nruns, flags


def results_table(d, prefix=""):
    rows = []
    for f in sorted(glob.glob(os.path.join(d, prefix + "*.mean.txt"))):
        name = os.path.basename(f)[:-len(".mean.txt")]
        lines, nruns, flags = picked(f)
        if not lines:
            errors.append("%s: no timing in it (a failed or empty run?)" % f)
            flags.append("NO TIMING")
        res = "<br>".join(cell(t)[:240] for t, _ in lines) or "-"
        spread = "<br>".join(cell(st)[:240] or "(one run or no spread)" for _, st in lines) or "-"
        rows.append("| %s | %s | %s | %s | %s |" % (cell(name), res, spread, nruns if nruns is not None else "?",
                                                   cell("; ".join(flags)) or ""))
    if not rows:
        return []
    return ["| measurement | mean over the runs | sd %, [min-max] per number | runs | flags |",
            "|---|---|---|---|---|"] + rows


def loose_notes(d):
    """loose.txt: TWA kinds timed although their answers differ from tsdb's within the stated bound."""
    f = os.path.join(d, "loose.txt")
    if not os.path.exists(f):
        return []
    return ["", "Answers that differ from tsdb's (timed, within the stated bound; see check-*.txt):", ""] + \
        ["- " + cell(l.strip()) for l in open(f) if l.strip()]


def phase1(out, p):
    hows = [h for h in (load_how(f) for f in sorted(glob.glob(os.path.join(p, "how-*.json")))) if h]
    tsvs = [f for f in glob.glob(os.path.join(p, "*.tsv"))]
    if not tsvs:
        return
    have = {h["key"] for h in hows}
    for f in tsvs:
        k = os.path.basename(f)[:-4]
        # p1_duck writes duckdb.tsv, p1_qdb both qdb tsvs, p1_ch the LZ4 and OPTIMIZE FINAL variants, p1_tsdb the
        # settled sizes; each row's note says which variant it is
        k = {"duckdb": "duck", "qdb-parquet": "qdb", "ch-lz4": "ch", "ch-optimized": "ch", "ch-zstd22": "ch",
             "tsdb-settled": "tsdb"}.get(k, k)
        if k not in have:
            errors.append("%s: results without how-%s.json" % (f, k))
    out += ["## Phase 1: size per kind of sensor", ""]
    s = os.path.join(p, "sizes.md")
    out += [open(s).read().rstrip()] if os.path.exists(s) else ["(sizes.md missing)"]
    out += how_table(hows, P1_COLS)


def phase2(out, p):
    out += ["## Phase 2: the mixed table, server systems", ""]
    hows = []
    for d in sorted(glob.glob(os.path.join(p, "*/"))):
        d = d.rstrip("/")
        t = results_table(d)
        f = os.path.join(d, "how.json")
        if not os.path.exists(f):
            if t:
                errors.append("%s: results without how.json" % d)
            continue
        h = load_how(f)
        if not h:
            continue
        hows.append(h)
        out += ["### %s" % h["system"], ""] + (t or ["(no timed measurements)"]) + loose_notes(d) + [""]
    out += how_table(hows, COLS)


def phase_flat(out, p, title):
    hows = [h for h in (load_how(f) for f in sorted(glob.glob(os.path.join(p, "how-*.json")))) if h]
    t = results_table(p)
    if t and not hows:
        errors.append("%s: results without how-*.json" % p)
    if not t:
        return
    out += ["## %s" % title, ""] + t + [""]
    out += how_table(hows, COLS)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    o = sys.argv[1]
    out = ["# Release comparison: results and how they were run", ""]
    m = os.path.join(o, "machine.txt")
    if os.path.exists(m):
        out += ["```", open(m).read().rstrip(), "```", ""]
    if os.path.isdir(os.path.join(o, "p1")):
        phase1(out, os.path.join(o, "p1"))
    if os.path.isdir(os.path.join(o, "p2")):
        phase2(out, os.path.join(o, "p2"))
    if os.path.isdir(os.path.join(o, "p3")):
        phase_flat(out, os.path.join(o, "p3"), "Phase 3: embedded (tsdb, DuckDB, chDB)")
    if os.path.isdir(os.path.join(o, "p4")):
        phase_flat(out, os.path.join(o, "p4"), "Phase 4: Prometheus (backfill, remote write)")
    print("\n".join(out))
    if errors:
        for e in errors:
            print("tables.py: " + e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
