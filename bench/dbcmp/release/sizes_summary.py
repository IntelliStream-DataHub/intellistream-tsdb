# SPDX-License-Identifier: AGPL-3.0-only
# sizes_summary.py P1DIR: the per-sensor size table from phase 1's <system>.tsv files (ds, rows, bytes, note).
# One row per data set, one column per system: bytes per reading and the size compared to tsdb (tsdb = 1x; 2.6x
# means 2.6 times tsdb's size). tsdb's column is after tsdb_optimize (what idle merges do); tsdb-settled is the
# state right after the background merges went idle for 3 s, for comparison.
import csv, os, sys

d = sys.argv[1]
order = ["tsdb", "tsdb-settled", "ch", "ch-optimized", "ch-zstd22", "ch-lz4", "duckdb", "qdb", "qdb-parquet",
         "qdb-parquet-zstd", "ts", "i2", "i3", "prom", "iot", "iot-zstd", "iot-chimp-zstd", "iot-gorilla-lzma2", "iot-chimp-lzma2", "td", "td-zstd", "td-xz", "vm"]
names = {"tsdb": "tsdb", "tsdb-settled": "tsdb (merges idle, before optimize)", "ch": "ClickHouse (codecs)",
         "ch-optimized": "ClickHouse OPTIMIZE FINAL", "ch-zstd22": "ClickHouse ZSTD(22), OPTIMIZE FINAL",
         "ch-lz4": "ClickHouse (default LZ4)", "duckdb": "DuckDB", "qdb": "QuestDB", "qdb-parquet": "QuestDB, Parquet",
         "qdb-parquet-zstd": "QuestDB, Parquet ZSTD", "ts": "TimescaleDB", "i2": "InfluxDB 2",
         "i3": "InfluxDB 3 (Parquet)", "prom": "Prometheus (backfill)", "iot": "IoTDB", "iot-zstd": "IoTDB ZSTD",
         "iot-chimp-zstd": "IoTDB CHIMP+ZSTD", "iot-gorilla-lzma2": "IoTDB LZMA2", "iot-chimp-lzma2": "IoTDB CHIMP+LZMA2",
         "td": "TDengine", "td-zstd": "TDengine zstd", "td-xz": "TDengine xz", "vm": "VictoriaMetrics"}
# a system's .tsv this list does not know yet still gets its column (under its file name), never dropped
order += sorted(f[:-4] for f in os.listdir(d) if f.endswith(".tsv") and f[:-4] not in order)
data, rows = {}, {}
for s in order:
    p = os.path.join(d, s + ".tsv")
    if not os.path.exists(p):
        continue
    for r in csv.DictReader(open(p), delimiter="\t"):
        try:
            data.setdefault(s, {})[r["ds"]] = int(float(r["bytes"]))
            if s == "tsdb": rows[r["ds"]] = int(float(r["rows"]))
        except (ValueError, TypeError, KeyError):
            data.setdefault(s, {})[r.get("ds", "?")] = None
cols = [s for s in order if s in data]
dss = list(data.get("tsdb", {}).keys()) or sorted({k for v in data.values() for k in v})
print("Bytes per reading on disk, and in brackets the size compared to tsdb (tsdb = 1x).\n")
print("| data set | rows | " + " | ".join(names[c] for c in cols) + " |")
print("|---|---:|" + "---:|" * len(cols))
for ds in dss:
    n = rows.get(ds)
    base = data.get("tsdb", {}).get(ds)
    cells = []
    for c in cols:
        b = data[c].get(ds)
        if b is None or not n:
            cells.append("-")
        elif c == "tsdb":
            cells.append(f"**{b / n:.3f}**")
        else:
            cells.append(f"{b / n:.3f} ({b / base:.1f}x)" if base else f"{b / n:.3f}")
    print(f"| {ds} | {n or '-'} | " + " | ".join(cells) + " |")
print("\nNotes per system: see the note column of each <system>.tsv here.")
