#!/usr/bin/env python3
"""Reads results.log and facts.txt of a bench/clients/run.sh run and prints Markdown: the machine, the load
tables (throughput, client CPU, bytes on the wire, server CPU) and one table per query class, each the median
of the repetitions, embedded first. Exits 1 if any load did not store the rows of the file, if nothing was verified, or if the systems did not
do the same query work (rows per query differ)."""

import statistics
import sys
from collections import defaultdict
from pathlib import Path

# Systems that run the same windows must read about the same number of rows per query: raw is exactly the window, and
# the aggregates cover the same window. A client that draws other windows (another generator, a wrong step) differs.
ROWS_PER_QUERY_TOLERANCE = 0.10

ORDER = ["embedded-fsync", "embedded-nofsync", "embedded", "embedded-java-fsync", "embedded-java-nofsync", "embedded-java",
         "c", "rust", "python", "java"]
NAMES = {
    "embedded-fsync": "embedded C, fsync on",
    "embedded-nofsync": "embedded C, fsync off",
    "embedded": "embedded C",
    "embedded-java-fsync": "embedded Java (FFM), fsync on",
    "embedded-java-nofsync": "embedded Java (FFM), fsync off",
    "embedded-java": "embedded Java (FFM)",
    "c": "C over TCP",
    "rust": "Rust over TCP",
    "python": "Python over TCP",
    "java": "Java over TCP",
}
CLASSES = [
    ("raw", "raw, 3600 samples"),
    ("agg300", "agg, 86400 samples in buckets of 300"),
    ("agg3600", "agg, 604800 samples in buckets of 3600"),
]


def parse(line):
    return dict(f.split("=", 1) for f in line.split()[1:] if "=" in f)


def med(xs):
    return statistics.median(xs) if xs else float("nan")


def spread(xs):
    m = med(xs)
    return (max(xs) - min(xs)) / m * 100 if len(xs) > 1 and m else 0.0


def cell(xs, fmt, show_spread=False):
    if not xs:
        return "-"
    s = format(med(xs), fmt)
    return f"{s} (±{spread(xs) / 2:.0f}%)" if show_spread else s


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "|".join(["---"] + ["---:"] * (len(head) - 1)) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def same_work(queries):
    """Messages for classes whose rows per query differ between systems by more than the tolerance."""
    out = []
    for cid, _ in CLASSES:
        for t in sorted({k[2] for k in queries if k[1] == cid}):
            vals = {k[0]: med(m["rows_per_query"]) for k, m in queries.items() if k[1] == cid and k[2] == t}
            lo, hi = min(vals.values()), max(vals.values())
            if lo <= 0 or hi - lo > ROWS_PER_QUERY_TOLERANCE * hi:
                out.append(f"class {cid} at {t} threads: rows per query differ between the systems ("
                           + ", ".join(f"{c} {v:.0f}" for c, v in sorted(vals.items(), key=lambda x: ORDER.index(x[0])))
                           + "); they did not run the same windows or the data sets differ")
    return out


def main():
    d = Path(sys.argv[1])
    loads = defaultdict(lambda: defaultdict(list))  # (client, compression, conns) -> metric -> values
    queries = defaultdict(lambda: defaultdict(list))  # (client, class, threads) -> metric -> values
    loads_seen = []
    verified = bad = 0
    loadavgs = []
    busy = []
    last_load = None
    for line in (d / "results.log").read_text().splitlines():
        if line.startswith("RESULT "):
            r = parse(line)
            loadavgs.append(float(r.get("load1", 0)))
            if "busy" in r:
                busy.append(float(r["busy"]))
            if r["kind"] == "load":
                key = (r["client"], int(r["compression"]) if not r["client"].startswith("embedded") else -1, int(r["conns"]))
                rows, secs = int(r["rows"]), float(r["secs"])
                m = loads[key]
                m["mrows"].append(rows / secs / 1e6)
                m["bytes"].append(float(r["bytes"]) / rows)
                m["cpu"].append(float(r["cpu"]) / (rows / 1e6))
                last_load = m
                loads_seen.append(rows)
            else:
                key = (r["client"], r["class"], int(r["threads"]))
                q, secs, rows = int(r["queries"]), float(r["secs"]), int(r["rows"])
                m = queries[key]
                m["qps"].append(q / secs)
                m["us"].append(secs * int(r["threads"]) / q * 1e6)
                m["mrows"].append(rows / secs / 1e6)
                m["cpu_us"].append(float(r["cpu"]) / q * 1e6)
                m["rows_per_query"].append(rows / q if q else 0.0)
        elif line.startswith("SERVERCPU ") and last_load is not None:
            rows = loads_seen[-1]
            last_load["scpu"].append(float(line.split()[1]) / (rows / 1e6))
        elif line.startswith("VERIFY "):
            if line.rstrip().endswith(": ok"):
                verified += 1
            else:
                bad += 1

    print("# Embedded C and tsdbd clients\n")
    print("```\n" + (d / "facts.txt").read_text().rstrip() + "\n```\n")
    if loadavgs:
        print(f"Load average (1 min) before each measurement: {min(loadavgs):.2f} to {max(loadavgs):.2f}. "
              "It includes this benchmark's own earlier runs.\n")
    if busy:
        print(f"CPUs kept busy by other processes in the second before each measurement: {min(busy):.2f} to "
              f"{max(busy):.2f}.\n")
    print(f"Correctness: {verified} loads checked against the file (row count, sum and every timestamp and value of every series), "
          f"{bad} mismatches.\n")
    print("Each cell is the median of the repetitions (± half the spread, as a percentage of the median). "
          "Embedded rows call libtsdb in this process; the others go through tsdbd on 127.0.0.1.\n")

    conns = sorted({k[2] for k in loads})
    lrows = sorted({(k[0], k[1]) for k in loads}, key=lambda x: (ORDER.index(x[0]), -x[1]))

    def label(c, comp):
        if c.startswith("embedded"):
            return NAMES[c]
        return f"{NAMES[c]}, " + ("zstd level 1" if comp else "plain")

    for title, metric, fmt, only_tcp in [
        ("Load: M rows/s", "mrows", ".2f", False),
        ("Load: client CPU seconds per M rows (the whole client process)", "cpu", ".2f", False),
        ("Load: bytes per row on the wire", "bytes", ".2f", True),
        ("Load: tsdbd CPU seconds per M rows", "scpu", ".2f", True),
    ]:
        rows = []
        for c, comp in lrows:
            if only_tcp and c.startswith("embedded"):
                continue
            rows.append([label(c, comp)] + [cell(loads[(c, comp, k)][metric], fmt, metric == "mrows") for k in conns])
        if rows:
            print(f"## {title}\n")
            print(table(["", *[f"{k} conn/thr" for k in conns]], rows))
            print()

    qthreads = sorted({k[2] for k in queries})
    clients = sorted({k[0] for k in queries}, key=ORDER.index)
    for cid, name in CLASSES:
        print(f"## Queries: {name}\n")
        head = [""]
        for t in qthreads:
            head += [f"{t} thr: q/s", "µs/q", "Mrows/s", "client CPU µs/q"]
        rows = []
        for c in clients:
            row = [NAMES[c]]
            for t in qthreads:
                m = queries[(c, cid, t)]
                row += [cell(m["qps"], ".0f", True), cell(m["us"], ".1f"), cell(m["mrows"], ".1f"), cell(m["cpu_us"], ".0f")]
            rows.append(row)
        print(table(head, rows))
        print()
    problems = same_work(queries)
    for p in problems:
        print(f"**PROBLEM: {p}**\n")
    print("µs/q is the time one thread or connection spends per query; client CPU is all threads of the client "
          "process, so embedded rows include the engine's own decoding threads and Java the JIT and the collector.")
    if bad or not verified:
        print("**PROBLEM: " + (f"{bad} loads did not store the rows of the file" if bad else "no load was verified") + "**")
    return 1 if bad or not verified or problems else 0


if __name__ == "__main__":
    sys.exit(main())
