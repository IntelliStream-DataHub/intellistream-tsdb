#!/usr/bin/env python3
"""The release run's results as Markdown, each table followed by how it was run.

  python3 -I tables.py OUT > tables.md

For every phase directory OUT/p<n> that has results: the results (phase 1: sizes.md of sizes_summary.py; phases 2
to 4: one table per system of its measurements: every timing line of every *.mean.txt (the mean over the runs) with
its sd and min-max, the run count, and flags for runs on a busy machine, refused or OOM-killed runs and text that
differs between runs, and the servers' peak memory over its runs (the OUT.mem files of run.sh's mem_sample: without
the page cache, and with it, which is what a memory limit counts); a mean file without a timing line fails the script;
then a line with each server's peak over all its timed steps, then loose.txt, answers that differ from
tsdb's within a stated bound, then the memory sweep of run.sh p6 (<dir>/memsweep: every class once, its peak memory
and the sweep's), and under them a "How it was run" table built from the how.json
files (how.py) plus footnotes: the '-' reasons, refusals and the per-core method. A system directory with results
but no how.json, or a how.json without one of how.py's REQUIRED fields, fails the script (exit 1): no number goes
out without the way it was measured.
tsdb's relq and tsdbq full files (NAME.cN.txt: the runs and their mean in one file) are read as they are, with their
unit: seconds per whole-table pass, or for the TWA windows (twa1d, twa7d) windows per second and ms per window.
In batch classes (NAME.bK.cN: K windows per request) the q/s of qdrive's and relq's lines counts requests, and that of
tsdbq's lines counts windows; each line says which, and tsdbq's get their requests per second (q/s / K) beside them.
The extra pass's timings (p2/<sys>/p6/, run.sh p6: another load) join the system's table marked "p6:", and the
system's how.json p6 block (when, runs, trees, its '-' reasons and refusals) is printed under it.
The extra-clients pass (run.sh phase6many: MANY_CLIENTS=16 CLASS_FILTER=b100 for tsdb and QuestDB) puts NAME.c16 files
there too. A 32-client class that failed (no run timed, or runs killed or refused) and has a NAME.c16 result gets a
note in its flags: it ran out of memory, with the peak, the limit and the memory held before the class (from its
.mem files), and the 16-client row is the comparison; the 32-client row itself stays a failure.
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
            notes.append("  Refused or died (server killed or stopped; see the flags of the measurement): %s" % r)
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
        if "TEXT DIFFERS" in l and "TEXT DIFFERS between runs (see the raw files)" not in flags:
            flags.append("TEXT DIFFERS between runs (see the raw files)")
        if l.startswith("#") or not l.strip() or CH_PCT.match(l) or not TIME.search(l):
            continue
        lines.append([l.replace("   # TEXT DIFFERS between runs", "").strip(), ""])
    base = f[:-len(".mean.txt")]
    nq = sorted(glob.glob(base + ".*.notquiet"))
    if nq:
        flags.append("measured on a busy machine in %d run(s): %s" % (len(nq), "; ".join(
            "%s %s" % (os.path.basename(x).split(".")[-2], open(x).read().strip()) for x in nq)))
    for ext, what in (("refused", "refused by the server"), ("oom", "killed out of memory"), ("died", "server died")):
        n = len(glob.glob(base + ".*." + ext))
        if n:
            flags.append("%d run(s) %s, not averaged" % (n, what))
    return lines, nruns, flags


FAILURE = re.compile(r"Code: 241[^.]*\.[^.]*memory limit exceeded|memory limit exceeded|global RSS memory limit exceeded|"
                     r"table busy[^\n]*|server closed the connection unexpectedly|query timed out after \d+ s|"
                     r"STEP TIMEOUT[^\n]*|status -1|code.? ?719[^\n]*")


def run_failures(f):
    """What the runs of a mean file that has no timing died of, from the run files: the server's own error where
    it gave one (ClickHouse's Code 241, QuestDB's RSS limit, IoTDB's 719), else the client's (a cut-off reply, a
    timeout), counted per kind; '' when no run file names a failure."""
    base, seen = f[:-len(".mean.txt")], {}
    for g in sorted(glob.glob(base + ".*")):
        if not re.fullmatch(re.escape(base) + r"\.\d+", g):
            continue
        m = FAILURE.search(open(g, errors="replace").read(20000))
        if m:
            k = " ".join(m.group(0).split())[:90]
            seen[k] = seen.get(k, 0) + 1
    return "; ".join("%d x %s" % (n, k) for k, n in seen.items())


MEM = re.compile(r"(\S+) peak (\d+) nocache (\d+)(?: start (\d+) startnocache (\d+))?")


def mem_peaks(files):
    """{server: (peak without page cache, peak with it)} over mem_sample files ("NAME peak N nocache M [start S
    startnocache T]" lines)."""
    peaks = {}
    for f in files:
        for l in open(f):
            m = MEM.fullmatch(l.strip())
            if m:
                a, b = peaks.get(m.group(1), (0, 0))
                peaks[m.group(1)] = (max(a, int(m.group(3))), max(b, int(m.group(2))))
    return peaks


def mem_rise(files):
    """{server: (rise without page cache, rise with it)}: the most a step's peak exceeded the memory the server held when
    the step started (its first sample), over files with a start; what earlier steps left is not in it."""
    rise = {}
    for f in files:
        for l in open(f):
            m = MEM.fullmatch(l.strip())
            if m and m.group(4) is not None:
                a, b = rise.get(m.group(1), (0, 0))
                rise[m.group(1)] = (max(a, int(m.group(3)) - int(m.group(5))), max(b, int(m.group(2)) - int(m.group(4))))
    return rise


def mem_cell(peaks):
    return "<br>".join("%s %.2f / %.2f GB" % (k, v[0] / 2**30, v[1] / 2**30) for k, v in sorted(peaks.items()))


def mem_line(d, prefix=""):
    """The peak of each server over every timed step in d (loads included), or nothing without mem files."""
    peaks = mem_peaks(sorted(glob.glob(os.path.join(d, prefix + "*.mem"))))
    if not peaks:
        return []
    return ["", "Peak memory of the server over all timed steps (loads included), without / with the page cache "
            "(sampled every 0.5 s): " + "; ".join(mem_cell({k: v}) for k, v in sorted(peaks.items())), ""]


BATCH = re.compile(r"(?:^|\.)(?:raw|agg1d|agg7d|point|twa1d|twa7d)\.b(\d+)\.c\d+(?:\.pc)?$")
QPS = re.compile(r"= (\d+(?:\.\d+)?) q/s")


def batch_unit(name, lines):
    """The unit of q/s in a batch class, so that nobody compares tsdbq's windows per second with another system's
    requests per second: tsdbq's NAME.bK lines ("... q/s ... per request") count windows and get their requests per
    second added; every other q/s line (qdrive, relq qps) counts requests and says so."""
    m = BATCH.search(name)
    if not m:
        return lines
    k, out = int(m.group(1)), []
    for t, st in lines:
        q = QPS.search(t)
        if q and "per request" in t and "requests of" not in t:
            t += " [q/s counts windows: %.1f requests/s of %d windows]" % (float(q.group(1)) / k, k)
        elif q:
            t += " [q/s counts requests of %d windows]" % k
        out.append([t, st])
    return out


RELQ_FILE = re.compile(r"^((?:merged\.)?(?:twa|twad|twal|twa1d|twa7d|lttb|full)\.c\d+"
                       r"|(?:tsdb|chdb|chdb-lz4|duckdb)\.(?:twa|lttb)(?:\.t\d+)?)\.txt$")  # p2's relq; p3's embedded
SQLT = re.compile(r"^sqlt: mean ([\d.]+) s sd ([\d.]+), (\d+) runs after a warm-up")  # p3: DuckDB, chDB (one query)
SQLT_RUN = re.compile(r"^sqlt run \d+: ([\d.]+) s")
RELQ1 = re.compile(r"^(\S+) (\d+) conns(?:, [^:]*)?: ([\d.]+) s, one run after a warm-up; (\d+) (buckets|points)"
                   r"(?:; (\d+) windows)?")
RELQ = re.compile(r"^(\S+) (\d+) conns(?:, [^:]*)?: mean ([\d.]+) s sd ([\d.]+) \(([\d.]+)%\) min ([\d.]+) max ([\d.]+), "
                  r"(\d+) runs after a warm-up; (\d+) (buckets|points)(?:; (\d+) windows)?")
FULLQ = re.compile(r"^full (.+?)\s+(\d+) conns batch \d+: mean ([\d.]+) s sd ([\d.]+), median ([\d.]+) \(min ([\d.]+), "
                   r"max ([\d.]+), (\d+) runs\)")
TWA_N = 1000  # windows of twa1d / twa7d when the line does not count them (relq before it did; run.sh's TWA_N)


def relq_rows(d, prefix=""):
    """tsdb's relq (TWA, LTTB) and tsdbq full files, NAME.cN.txt: one tstep with all REPS runs and their mean, sd, min
    and max inside (not runstats' NAME.<run> files), read as they are. Whole-table kinds are seconds per pass over every
    series; the window kinds (twa1d, twa7d) are a set of TWA_N windows per run, given as windows per second and ms of
    wall time per window (at N connections the windows overlap: wall time over windows, not a latency)."""
    rows = []
    for f in sorted(glob.glob(os.path.join(d, prefix + "*.txt"))):
        m = RELQ_FILE.match(os.path.basename(f))
        if not m:
            continue
        name, res, spread, nruns = m.group(1), [], [], None
        sq_runs = []
        for l in open(f, errors="replace"):
            r = SQLT_RUN.match(l.strip())
            if r:
                sq_runs.append(float(r.group(1)))
            r = SQLT.match(l.strip())
            if r:
                mean, sd, nruns = float(r.group(1)), float(r.group(2)), int(r.group(3))
                kind = "LTTB" if ".lttb" in name else "TWA (hourly STEP buckets)"
                res.append("%s of the whole table in one query: mean %.4f s" % (kind, mean))
                spread.append("one run" if nruns == 1 else "sd %.1f%% [%.4f-%.4f s]" % (
                    100 * sd / mean if mean else 0.0, min(sq_runs or [mean]), max(sq_runs or [mean])))
                continue
            r = RELQ.match(l.strip())
            r1 = RELQ1.match(l.strip())
            if r1:  # REPS=1 (the smoke): one time, no spread
                kind, conns, mean, nb, what, nw = r1.groups()
                r, (sd, pct, mn, mx, runs) = r1, ("0", "0", mean, mean, "1")
            if r:
                if not r1:
                    kind, conns, mean, sd, pct, mn, mx, runs, nb, what, nw = r.groups()
                mean, mn, mx, nruns = float(mean), float(mn), float(mx), int(runs)
                if kind in ("twa1d", "twa7d"):
                    w = int(nw or TWA_N)
                    res.append("%d windows on %s conns: mean %.4f s per set = %.0f windows/s, %.4f ms of wall time per "
                               "window (%s buckets)%s" % (w, conns, mean, w / mean, mean / w * 1e3, nb,
                                                          "" if nw else " [%d windows: the run's TWA_N]" % w))
                    spread.append("one run" if nruns == 1 else "sd %s%% [%.0f-%.0f windows/s]" % (pct, w / mx, w / mn))
                else:
                    res.append("%s on %s conns: mean %.4f s per whole-table pass (every series, one request each; %s %s)" % (
                        kind, conns, mean, nb, what))
                    spread.append("one run" if nruns == 1 else "sd %s%% [%.4f-%.4f s]" % (pct, mn, mx))
            q = FULLQ.match(l.strip())
            if q:
                what, conns, mean, sd, med, mn, mx, runs = q.groups()
                nruns = int(runs)
                res.append("full, %s, on %s conns: mean %s s per whole-table pass (median %s s)" % (what, conns, mean, med))
                spread.append("sd %.1f%% [%s-%s s]" % (100 * float(sd) / float(mean) if float(mean) else 0.0, mn, mx))
        if name.startswith("tsdb."):  # p3: embedded tsdb, threads and calls in one process, not connections
            res = [x.replace(" conns", " threads").replace("one request each", "one call each") for x in res]
        flags = []
        if os.path.exists(f + ".notquiet"):
            flags.append("measured on a busy machine: " + open(f + ".notquiet").read().strip())
        if not res:
            errors.append("%s: no timing in it (a failed or empty run?)" % f)
            flags.append("NO TIMING")
        mem = mem_cell(mem_peaks([f + ".mem"])) if os.path.exists(f + ".mem") else ""
        rows.append((name, "| %s | %s | %s | %s | %s | %s |" % (
            cell(name), "<br>".join(cell(x) for x in res) or "-", "<br>".join(cell(x) for x in spread) or "-",
            nruns if nruns is not None else "?", mem or "-", cell("; ".join(flags)))))
    return rows


LIMIT = re.compile(r"container limit (\d+(?:\.\d+)? ?[A-Za-z]+)")


def gb(x):
    return "%.1f GB" % (x / 2**30)


def many_note(d, name, lines, flags):
    """The note under a failed 32-client class that has a 16-client result (run.sh phase6many: MANY_CLIENTS=16 for tsdb
    and QuestDB): QuestDB ran out of memory in its 32-client 100-series batches, so the 16-client run is the
    comparison and the 32-client row stays a failure. The numbers come from the mem files of the class (the run with the
    highest peak: the one that ran out; peak with the page cache is what a container limit counts, then without it, and
    the server's memory at the step's start: what the earlier classes left), the limit from the REFUSED lines naming the
    class ('container limit 48 GB'), or any REFUSED line of the system. '' when the class did not fail or has no
    16-client result."""
    if not name.endswith(".c32") or not (flags_failed(flags) or not lines):
        return ""
    c16 = name[:-len("c32")] + "c16"
    f16 = next((f for f in (os.path.join(d, "p6", c16 + ".mean.txt"), os.path.join(d, c16 + ".mean.txt"))
                if os.path.exists(f) and picked(f)[0]), None)
    if not f16:
        return ""
    best = {}  # server: (peak with cache, peak without, start with cache, start without): the file with the highest peak
    for sub in ("", "memsweep"):
        for g in glob.glob(os.path.join(d, sub, name + ".*.mem")):
            if not re.fullmatch(re.escape(name) + r"\.\d+\.mem", os.path.basename(g)):
                continue
            for l in open(g):
                m = MEM.fullmatch(l.strip())
                if m:
                    cur = (int(m.group(2)), int(m.group(3)), int(m.group(4) or 0), int(m.group(5) or 0))
                    if m.group(1) not in best or cur[0] > best[m.group(1)][0]:
                        best[m.group(1)] = cur
    limit, anylimit = None, None
    for r in glob.glob(os.path.join(d, "REFUSED*.txt")) + [os.path.join(d, s, "REFUSED.txt") for s in ("memsweep", "p6")]:
        for l in open(r, errors="replace") if os.path.exists(r) else []:
            m = LIMIT.search(l)
            if m:
                anylimit = anylimit or m.group(1)
                if name in l:
                    limit = limit or m.group(1)
    why = run_failures(os.path.join(d, name + ".mean.txt")) if os.path.exists(os.path.join(d, name + ".mean.txt")) else ""
    mem = "; ".join("%s peak %s with the page cache (%s without), %s held when the class started" % (
        k, gb(v[0]), gb(v[1]), gb(v[2])) for k, v in sorted(best.items())) or "memory not recorded"
    return ("the 32-client run ran out of memory%s: %s; limit %s. The 16-client run (%s) is the comparison" % (
        " (" + why + ")" if why else "", mem, limit or anylimit or "not recorded", os.path.basename(f16)[:-len(".mean.txt")]))


def flags_failed(flags):
    return any("not averaged" in x or x.startswith("no run timed") for x in flags)


def results_table(d, prefix="", mark=""):
    """The timing rows of d; with mark (the extra pass's d/p6), names start with it and the header is left out. A
    class the extra pass timed again (TWA_RETIME: QuestDB's TWA after its query changed) is flagged in the phase-2 row."""
    rows = []
    again = set() if mark else {os.path.basename(g)[:-len(".mean.txt")]
                                for g in glob.glob(os.path.join(d, "p6", prefix + "*.mean.txt"))}
    for f in sorted(glob.glob(os.path.join(d, prefix + "*.mean.txt"))):
        name = os.path.basename(f)[:-len(".mean.txt")]
        mem = mem_cell(mem_peaks(g for g in glob.glob(os.path.join(d, name + ".*.mem"))
                                 if re.fullmatch(re.escape(name) + r"\.\d+\.mem", os.path.basename(g))))
        lines, nruns, flags = picked(f)
        lines = batch_unit(name, lines)
        if not lines:
            why = run_failures(f)
            if why or any("not averaged" in x for x in flags):
                # every run refused, killed or failed with the server's own error: a result, shown as '-' with why
                flags.append("no run timed: " + why if why else "no run timed")
            else:
                errors.append("%s: no timing in it (a failed or empty run?)" % f)
                flags.append("NO TIMING")
        note = "" if mark else many_note(d, name, lines, flags)
        if note:
            flags.append(note)
        if name in again:
            flags.append("SUPERSEDED: timed again in the extra pass (the p6: row of the same name)")
        res = "<br>".join(cell(t)[:240] for t, _ in lines) or "-"
        spread = "<br>".join(cell(st)[:240] or "(one run or no spread)" for _, st in lines) or "-"
        rows.append((name, "| %s | %s | %s | %s | %s | %s |" % (cell(mark + name), res, spread,
                                                               nruns if nruns is not None else "?",
                                                               mem or "-", cell("; ".join(flags)) or "")))
    rows = [r for _, r in sorted(rows + relq_rows(d, prefix))]
    if mark:
        return rows
    p6 = os.path.join(d, "p6")
    if os.path.isdir(p6):
        rows += results_table(p6, prefix, "p6: ")
    if not rows:
        return []
    return ["| measurement | mean over the runs | sd %, [min-max] per number | runs | "
            "peak memory, without / with page cache | flags |",
            "|---|---|---|---|---|---|"] + rows


def sweep_table(d):
    """d/memsweep (run.sh p6): one load and every class once, each with mem_sample: the server's peak memory per class
    and over the whole sweep. One run per class: never averaged, never in the timing table."""
    s = os.path.join(d, "memsweep")
    files = sorted(glob.glob(os.path.join(s, "*.mem")))
    if not files:
        return []
    by = {}
    for f in files:  # NAME.<run>.mem (reps), NAME.txt.mem (one tstep), load.<run>.mem
        by.setdefault(re.sub(r"\.(?:\d+|txt)$", "", os.path.basename(f)[:-len(".mem")]), []).append(f)
    rows = ["| class (one run) | peak during the step (includes what earlier classes left), without / with page cache "
            "| rise over the step's start, without / with page cache | flags |", "|---|---|---|---|"]
    for n, fs in sorted(by.items()):
        # n + ".*." + ext: twa's files only, not twad's or twal's
        flags = ["%s %s" % (len(glob.glob(os.path.join(s, n + ".*." + ext))), what) for ext, what in
                 (("refused", "refused"), ("oom", "killed out of memory"), ("died", "server died"))
                 if glob.glob(os.path.join(s, n + ".*." + ext))]
        rows.append("| %s | %s | %s | %s |" % (cell(n), mem_cell(mem_peaks(fs)) or "-", mem_cell(mem_rise(fs)) or "-",
                                               cell("; ".join(flags))))
    peaks = mem_peaks(files)
    out = ["", "Memory sweep (run.sh p6: one load, then every class above once, per core left out; one run each, not "
           "averaged): the server's memory per class, without / with the page cache (sampled every 0.5 s). The peak "
           "is what the server held at most during the step, memory earlier classes left behind included (caches, "
           "pools); the rise is how far it grew above what it held when the step started.", ""]
    out += rows + ["", "Peak over the whole sweep (load included): " +
                   ("; ".join(mem_cell({k: v}) for k, v in sorted(peaks.items())) or "-")]
    r = os.path.join(s, "REFUSED.txt")
    if os.path.exists(r):
        out += ["", "Refused or died in the sweep:", ""] + ["- " + cell(l.strip()) for l in open(r) if l.strip()]
    return out + [""]


def relq_note(d):
    """Under tsdb's table: its relq window rows are not the steady-state q/s of the other systems' window classes."""
    if not any(RELQ_FILE.match(os.path.basename(f)) and re.search(r"twa[17]d\.c", f)
               for f in glob.glob(os.path.join(d, "*.txt"))):
        return []
    return ["", "The twa1d / twa7d.cN rows (relq) time a fixed set of 1000 windows from start to end, connects "
            "included, and give windows/s from that wall time; the other systems' twa1d / twa7d rows (and tsdb's "
            "KIND.bK.cN) are q/s over a 15 s steady-state run, so the two are not the same measure."]


def p6_notes(h):
    """The extra pass's how.json block (how.py p6), under the system's tables."""
    p = h.get("p6")
    if not p:
        return []
    out = ["", "Extra pass (p6) on %s: %s; tsdb %s; harness %s. %s." % (
        p.get("date", "?"), p.get("runs", "?"), p.get("tsdb", "?"), p.get("harness", "?"), p.get("note", ""))]
    if p.get("many"):
        out.append("- extra-clients pass: " + cell(p["many"]))
    out += ["- p6 '-': %s: %s" % (cell(k), cell(v)) for k, v in sorted(p.get("dash", {}).items())]
    out += ["- p6 refused or died: " + cell(l) for l in p.get("refused", [])]
    return out


def loose_notes(d):
    """loose.txt: TWA kinds timed although their answers differ from tsdb's within the stated bound, and LTTB answers
    that differ through ties of the triangle area (rel.py)."""
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
    out += ["## Phase 2: the mixed table, server systems", "",
            "In batch classes (NAME.bK.cN, K windows per request) each line says what its q/s counts: requests in "
            "qdrive's and relq's lines (every system but tsdbq), windows in tsdbq's, whose requests per second are "
            "added in brackets. KIND.b100.c1 of tsdb is its whole-range TWA of every series in one request "
            "(QUERY_MULTI), the form one SQL query has in the other systems. Rows marked p6: are from the extra pass "
            "(run.sh p6), on a load of their own.", ""]
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
        out += ["### %s" % h["system"], ""] + (t or ["(no timed measurements)"]) + relq_note(d) + mem_line(d) + \
            loose_notes(d) + sweep_table(d) + p6_notes(h) + [""]
    out += how_table(hows, COLS)


def phase_flat(out, p, title):
    hows = [h for h in (load_how(f) for f in sorted(glob.glob(os.path.join(p, "how-*.json")))) if h]
    t = results_table(p)
    if t and not hows:
        errors.append("%s: results without how-*.json" % p)
    if not t:
        return
    out += ["## %s" % title, ""] + t + mem_line(p) + loose_notes(p) + [""]
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
