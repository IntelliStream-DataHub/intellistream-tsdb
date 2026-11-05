#!/usr/bin/env python3
"""Turns the raw logs of tools/auth-bench.sh into summary.md and summary.json.

    tools/auth-bench-summary.py RESULTS_DIR

RESULTS_DIR holds meta.kv (the machine and the run), commands.txt, status.txt ("CONFIG WORKLOAD REP RC" per run),
quiet.txt (runs that started on a busy machine) and raw/CONFIG.WORKLOAD.REP.txt (the output of every run). A number is
parsed from the RESULT lines of those files: a mean over the repetitions with the sample standard deviation and the
lowest and highest run (statistics and number format of tools/bench-summary.py), and next to each table the same
means as a change against ffa4942 (main without auth), which is what the login and the grants cost. Standard library only.
"""
import importlib.util
import json
import os
import re
import sys

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("bench_summary", os.path.join(here, "bench-summary.py"))
bs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bs)
stats, fmt = bs.stats, bs.fmt

CONFIGS = [
    ("main0", "main before (3bd4e96)"),
    ("main1", "main without auth (ffa4942), the baseline"),
    ("noauth", "auth build, --no-auth"),
    ("super", "auth build, logged in as a superuser"),
    ("prefix", "auth build, a user with 8 prefix: grants"),
    ("name5000", "auth build, a user with 5,000 name: grants"),
    ("glob64", "auth build, a user with 64 glob: grants"),
]
BASE = "main1"
CLASS_LABEL = {
    "raw": "raw rows, 3600 samples",
    "agg300": "86400 samples in buckets of 300",
    "agg3600": "604800 samples in buckets of 3600",
    "twa3600": "TWA, 604800 samples by the hour",
    "full0": "the whole range in one bucket",
}
CLASSES = ["raw", "agg300", "agg3600", "twa3600", "full0"]


def kv(line):
    return dict(re.findall(r"(\w+)=(\S+)", line))


def read_results(path):
    out = []
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("RESULT "):
                out.append(kv(line))
    return out


def per_second(r, num):
    return float(r[num]) / float(r["secs"])


# a group is a table: (title, unit, columns [(workload, column label, picker)]); the picker turns the RESULT lines
# of one run into the value of the column
def pick_class(cls, suffix=""):
    def f(results):
        for r in results:
            if r.get("kind") == "query" and r.get("class") == cls + suffix:
                return per_second(r, "queries")
        return None
    return f


def pick_first(kind, num):
    def f(results):
        for r in results:
            if r.get("kind") == kind:
                return per_second(r, num)
        return None
    return f


def groups(multi):
    g = []
    for c in (1, 32):
        g.append(("Queries, %d connection%s" % (c, "" if c == 1 else "s"), "queries/s",
                  [("query-c%d" % c, CLASS_LABEL[k], pick_class(k)) for k in CLASSES]))
    for c in (1, 32):
        g.append(("QUERY_MULTI of %d series per request, %d connection%s (windows/s)" % (multi, c, "" if c == 1 else "s"),
                  "windows/s", [("multi-c%d" % c, CLASS_LABEL[k], pick_class(k, ".m%d" % multi)) for k in CLASSES]))
    g.append(("Inserts, 1,000 rows a request", "rows/s",
              [("insert-series-c1", "one series per request, 1 connection", pick_first("insert", "rows")),
               ("insert-series-c32", "one series per request, 32 connections", pick_first("insert", "rows")),
               ("insert-rows-c1", "a series per row, 1 connection", pick_first("insert", "rows")),
               ("insert-rows-c32", "a series per row, 32 connections", pick_first("insert", "rows"))]))
    g.append(("Logins (connect, one small request, close)", "logins/s",
              [("login-c1", "key cache, 1 thread", pick_first("login", "logins")),
               ("login-c32", "key cache, 32 threads", pick_first("login", "logins")),
               ("login-cold-c1", "cold (new process each), 1 thread", pick_first("login", "logins")),
               ("login-cold-c32", "cold (new process each), 32 threads", pick_first("login", "logins"))]))
    return g


def read_kv(path):
    d = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                if "=" in line:
                    k, v = line.rstrip("\n").split("=", 1)
                    d.setdefault(k, v)
    return d


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    res = sys.argv[1]
    raw = os.path.join(res, "raw")
    meta = read_kv(os.path.join(res, "meta.kv"))
    multi = 100
    m = re.search(r"QUERY_MULTI: (\d+) series", meta.get("workload_sizes", ""))
    if m:
        multi = int(m.group(1))
    files = {}
    for n in os.listdir(raw):
        m = re.match(r"^([a-z0-9]+)\.([a-z0-9-]+)\.(\d+)\.txt$", n)
        if m:
            files[(m.group(1), m.group(2), int(m.group(3)))] = os.path.join(raw, n)
    failed = []
    status = os.path.join(res, "status.txt")
    if os.path.exists(status):
        for line in open(status):
            p = line.split()
            if len(p) == 4 and p[3] != "0":
                failed.append(" ".join(p[:3]))
            elif len(p) == 3 and p[2] != "0":
                failed.append(" ".join(p))
    out = {"meta": meta, "groups": {}}
    lines = ["# Login and access control: before and after", ""]
    lines.append("Mean of the runs, then (standard deviation in percent of the mean; lowest to highest run). "
                 "Written by tools/auth-bench-summary.py from raw/; every number can be found there.")
    lines.append("")
    lines.append("## The run")
    lines.append("")
    for k in ["date", "mode", "reps", "seconds_per_workload", "host", "cpu", "cpu_topology", "memory", "kernel", "os",
              "governor", "compiler", "scratch", "lock", "busy_at_start", "git_clone", "tree_before", "tree_main",
              "tree_auth", "tsdbd_flags", "workload_sizes"]:
        if k in meta:
            lines.append("- **%s**: %s" % (k.replace("_", " "), meta[k]))
    for k in sorted(meta):
        if k.startswith("server_"):
            lines.append("- **%s**: %s" % (k.replace("_", " "), meta[k]))
    quiet = os.path.join(res, "quiet.txt")
    qlines = [l.strip() for l in open(quiet)] if os.path.exists(quiet) else []
    qlines = [l for l in qlines if l]
    lines.append("")
    if qlines:
        lines.append("**%d run(s) started on a machine that was not quiet or not on the performance governor** "
                     "(quiet.txt): %s" % (len(qlines), "; ".join(qlines[:6]) + (" ..." if len(qlines) > 6 else "")))
        lines.append("")
    if failed:
        lines.append("**Failed runs** (status.txt): " + ", ".join(failed))
        lines.append("")
    all_reps = sorted({r for (_, _, r) in files})
    for title, unit, cols in groups(multi):
        table = {}
        for cfg, _ in CONFIGS:
            for wl, label, pick in cols:
                vals = []
                for rep in all_reps:
                    p = files.get((cfg, wl, rep))
                    if not p:
                        continue
                    v = pick(read_results(p))
                    if v is not None:
                        vals.append(v)
                if vals:
                    table[(cfg, label)] = stats(vals)
        labels = []
        for _, label, _ in cols:
            if label not in labels:
                labels.append(label)
        out["groups"][title] = {cfg: {label: table[(cfg, label)] for label in labels if (cfg, label) in table}
                                for cfg, _ in CONFIGS}
        lines.append("## %s" % title)
        lines.append("")
        lines.append("Unit: %s." % unit)
        lines.append("")
        lines.append("| configuration | " + " | ".join(labels) + " |")
        lines.append("|---|" + "---:|" * len(labels))
        for cfg, desc in CONFIGS:
            cells = []
            for label in labels:
                st = table.get((cfg, label))
                cells.append("%s (%.1f%%; %s to %s)" % (fmt(st["mean"]), st["sd_pct"], fmt(st["min"]), fmt(st["max"]))
                             if st else "-")
            lines.append("| %s | %s |" % (desc, " | ".join(cells)))
        lines.append("")
        lines.append("Change against ffa4942 (the same code without auth), in percent of its mean:")
        lines.append("")
        lines.append("| configuration | " + " | ".join(labels) + " |")
        lines.append("|---|" + "---:|" * len(labels))
        for cfg, desc in CONFIGS:
            if cfg == BASE:
                continue
            cells = []
            for label in labels:
                st, b = table.get((cfg, label)), table.get((BASE, label))
                cells.append("%+.1f%%" % ((st["mean"] / b["mean"] - 1) * 100) if st and b and b["mean"] else "-")
            lines.append("| %s | %s |" % (desc, " | ".join(cells)))
        lines.append("")
    with open(os.path.join(res, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    with open(os.path.join(res, "summary.json"), "w") as f:
        json.dump(out, f, indent=1)
    if not any(out["groups"][t] and any(out["groups"][t].values()) for t in out["groups"]):
        print("auth-bench-summary.py: no numbers found in %s" % raw, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
