#!/usr/bin/env python3
"""The release run's results as Markdown, each table followed by how it was run.

  python3 -I tables.py OUT > tables.md

For every phase directory OUT/p<n> that has results: the results (phase 1: sizes.md of sizes_summary.py; phases 2
to 4: one table per system of its measurements, the first line of every *.mean.txt, i.e. the mean over the runs
with sd and min-max where runstats.py gives them), and under them a "How it was run" table built from the how.json
files (how.py) plus footnotes: the '-' reasons, refusals and the per-core method. A system directory with results
but no how.json, or a how.json without one of how.py's REQUIRED fields, fails the script (exit 1): no number goes
out without the way it was measured.
"""
import glob, json, os, sys

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


def first_line(f):
    for l in open(f):
        if l.strip() and not l.startswith("#"):
            return l.strip()
    return ""


def results_table(d, prefix=""):
    rows = []
    for f in sorted(glob.glob(os.path.join(d, prefix + "*.mean.txt"))):
        name = os.path.basename(f)[:-len(".mean.txt")]
        rows.append("| %s | %s |" % (cell(name), cell(first_line(f))[:240]))
    if not rows:
        return []
    return ["| measurement | mean over the runs (sd, min-max where more than one) |", "|---|---|"] + rows


def phase1(out, p):
    hows = [h for h in (load_how(f) for f in sorted(glob.glob(os.path.join(p, "how-*.json")))) if h]
    tsvs = [f for f in glob.glob(os.path.join(p, "*.tsv"))]
    if not tsvs:
        return
    have = {h["key"] for h in hows}
    for f in tsvs:
        k = os.path.basename(f)[:-4]
        k = {"duckdb": "duck", "qdb-parquet": "qdb"}.get(k, k)  # p1_duck writes duckdb.tsv, p1_qdb both qdb tsvs
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
        out += ["### %s" % h["system"], ""] + (t or ["(no timed measurements)"]) + [""]
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
