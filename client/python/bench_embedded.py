"""Embedded benchmark: the load and the query classes of build/tsdb-bench,
through tsdb_client.Db (the engine in this process, no tsdbd).

    build/tsdb-bench --export /data/bench.rowbinary [--series 100] [--points 1000000] [--dataset mixed]
    python bench_embedded.py --dir /data/emb --load /data/bench.rowbinary [--dataset mixed]
        [--batch 1000000] [--threads 4] [--zstd 1] [--no-fsync]
    python bench_embedded.py --dir /data/emb      # queries only

--load FILE reads rows of u32 series, i64 ts (ms), f32 value (16 bytes each,
as `tsdb-bench --export` writes them) into numpy arrays first, so the timing
covers the inserts but not the data generation that tsdb-bench's own
"insert+generate" line includes. Like tsdb-bench, each of --threads writers
(Python threads: Db releases the GIL while the engine works) owns a range of
series and sends them time-major in batches of --batch rows; then the
database is optimized. --dir must not hold a database when loading. Without
--load, the queries run on an existing directory, for example one that
`tsdb-bench --keep` left behind.

The queries are tsdb-bench's: a raw scan of every series, aggregates over the
whole range, calendar groups (best of 3), then random windows for --seconds
(default 2) on 1 thread and on every logical CPU, drawn with the same seeds,
so the rows and buckets line up with tsdb-bench's. Raw queries return numpy
arrays (a copy: what a Python caller gets); buckets are Python objects. The
sample step of each series comes from its name (<kind>.<n>). Needs numpy.
"""

import argparse
import os
import sys
import threading
import time

import numpy as np

import tsdb_client

T0 = 1704067200000  # 2024-01-01, as tools/tsdb-bench.c
KINDS = [
    ("hdd_temp", 60000),
    ("cpu_temp", 1000),
    ("tank_bar", 10000),
    ("tank_psi", 10000),
    ("vibration", 1),
    ("flow_rate", 1000),
    ("flow_total", 1000),
    ("random", 1000),
]
STEP = dict(KINDS)
M64 = (1 << 64) - 1
ROW = np.dtype([("s", "<u4"), ("t", "<i8"), ("v", "<f4")])  # packed: 16 bytes


def ds_mix(x):
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & M64
    x ^= x >> 33
    x = (x * 0xC4CEB9FE1A85EC53) & M64
    return x ^ (x >> 33)


def kind_of(dataset, s):
    return dataset if dataset is not None else s % (len(KINDS) - 1)  # mixed: no random


def load(db, a, path):
    rows = np.fromfile(path, dtype=ROW)
    nseries = int(rows["s"].max()) + 1 if len(rows) else 0
    threads = max(1, min(a.threads, nseries))
    batches = []
    for i in range(threads):
        lo, hi = nseries * i // threads, nseries * (i + 1) // threads
        mine = rows[(rows["s"] >= lo) & (rows["s"] < hi)]  # keeps the file's time-major order
        n = max(1, a.batch // (hi - lo)) * (hi - lo)
        s, t, v = (np.ascontiguousarray(mine[f]) for f in ("s", "t", "v"))
        batches.append([(s[k : k + n], t[k : k + n], v[k : k + n]) for k in range(0, len(mine), n)])
    for s in range(nseries):
        db.series(f"{KINDS[kind_of(a.dataset, s)][0]}.{s}")
    print(
        f"bench_embedded: {nseries} series, {len(rows) / 1e6:.1f} M rows from {path}, batch {a.batch} rows, "
        f"{threads} writer threads, zstd {a.zstd}, fsync {int(a.fsync)}, simd {tsdb_client.Db.simd_level()}\n"
    )

    def writer(bs):
        for s, t, v in bs:
            db.insert(s, t, v)

    t0 = time.perf_counter()
    th = [threading.Thread(target=writer, args=(bs,)) for bs in batches]
    for x in th:
        x.start()
    for x in th:
        x.join()
    ti = time.perf_counter() - t0
    st = db.stats()
    print(
        f"insert          {ti:6.2f} s  {len(rows) / ti / 1e6:7.2f} M rows/s   "
        f"({st.parts} parts, {st.merges} merges so far)"
    )
    t0 = time.perf_counter()
    db.optimize()
    to = time.perf_counter() - t0
    st = db.stats()
    print(
        f"optimize        {to:6.2f} s  -> {st.parts} parts in {st.partitions} partitions, "
        f"{st.passthrough_granules} granules copied as-is"
    )


def raw_rows(db, s, start, end):
    ts, v = db.query_raw(s, start, end, numpy=True)
    if len(v):
        float(v[-1])  # touch the data
    return len(v)


def qps(db, series, points, step, seconds, label, agg, window, bucket, threads):
    res = [None] * threads

    def loop(i):
        r = ds_mix(i + 77)
        queries = rows = 0
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < seconds:
            for _ in range(64):
                r = ds_mix(r)
                s = r % series
                st = step[s]
                span = points - window if points > window else 1
                start = T0 + ((r >> 20) % span) * st
                end = start + window * st
                if agg:
                    rows += sum(b.count for b in db.query_agg(s, start, end, bucket * st))
                else:
                    rows += raw_rows(db, s, start, end)
                queries += 1
        res[i] = (queries, rows, time.perf_counter() - t0)

    th = [threading.Thread(target=loop, args=(i,)) for i in range(threads)]
    for x in th:
        x.start()
    for x in th:
        x.join()
    nq = sum(r[0] for r in res)
    rows = sum(r[1] for r in res)
    secs = max(r[2] for r in res)
    print(
        f"  {label:<46} {threads:3} thr {nq / secs:10.0f} q/s  {secs * threads / nq * 1e6:8.1f} us/q  "
        f"{rows / secs / 1e6:7.1f} Mrows/s"
    )


def queries(db, seconds):
    series = db.series_count()
    step = [STEP[db.series_name(s).split(".")[0]] for s in range(series)]
    st = db.stats()
    points = st.rows // series if series else 0
    total = series * points
    print(
        f"storage    {st.bytes / 1048576.0:8.2f} MiB  {st.bytes / st.rows:.3f} bytes/row  "
        f"ratio {st.rows * 12.0 / st.bytes:.1f}x vs 12 bytes/row (DateTime64(3)+Float32)\n"
    )

    t0 = time.perf_counter()
    rows = sum(raw_rows(db, s, None, None) for s in range(series))
    tr = time.perf_counter() - t0
    print(f"raw scan, all series, 1 thread            {tr:8.3f} s  {rows / tr / 1e6:7.1f} M rows/s  ({rows} rows)")

    for label, bucket in [
        ("agg whole range, 1 bucket per series", 0),
        ("agg buckets of 86400 samples", 86400),
        ("agg buckets of 3600 samples", 3600),
        ("agg buckets of 60 samples (decodes all)", 60),
        ("agg buckets of 10 samples (decodes all)", 10),
    ]:
        t0 = time.perf_counter()
        rows = buckets = 0
        for s in range(series):
            b = db.query_agg(s, None, None, bucket * step[s])
            rows += sum(x.count for x in b)
            buckets += len(b)
        ta = time.perf_counter() - t0
        print(
            f"{label:<42} {ta:8.3f} s  {rows / ta / 1e6:7.1f} M rows/s  {ta / series * 1e3:6.2f} ms/series  "
            f"({buckets} buckets)"
        )
        if rows != total:
            print(f"  !! row count mismatch: {rows} != {total}")

    for label, unit, count, zone in [
        ("group by 15 minutes (UTC)", "minute", 15, None),
        ("group by hour (UTC)", "hour", 1, None),
        ("group by day (Europe/Oslo)", "day", 1, "Europe/Oslo"),
        ("group by week (Europe/Oslo)", "week", 1, "Europe/Oslo"),
        ("group by month (Europe/Oslo)", "month", 1, "Europe/Oslo"),
        ("group by year (Europe/Oslo)", "year", 1, "Europe/Oslo"),
    ]:
        best = float("inf")
        for _ in range(3):
            t0 = time.perf_counter()
            rows = buckets = 0
            for s in range(series):
                b = db.query_group(s, unit=unit, count=count, timezone=zone)
                rows += sum(x.count for x in b)
                buckets += len(b)
            best = min(best, time.perf_counter() - t0)
        print(f"{label:<42} {best:8.3f} s  {rows / best / 1e6:7.1f} M rows/s  ({buckets} buckets)")

    print(f"\nrandom windows ({seconds:g} s each):")
    n = os.cpu_count() or 1
    for label, agg, window, bucket in [
        ("raw, 3600 samples", False, 3600, 0),
        ("agg, 86400 samples in buckets of 300", True, 86400, 300),
        ("agg, 604800 samples in buckets of 3600", True, 604800, 3600),
    ]:
        if window == 604800 and points <= 7 * 86400:
            continue
        for threads in (1, n):
            qps(db, series, points, step, seconds, label, agg, window, bucket, threads)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--dir", required=True)
    p.add_argument("--load")
    p.add_argument("--dataset", default="mixed", choices=["mixed"] + [k for k, _ in KINDS])
    p.add_argument("--batch", type=int, default=1000000)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--zstd", type=int, default=1)
    p.add_argument("--no-fsync", dest="fsync", action="store_false")
    p.add_argument("--seconds", type=float, default=2.0)
    p.add_argument("--query-threads", type=int, default=0)
    a = p.parse_args()
    a.dataset = None if a.dataset == "mixed" else [k for k, _ in KINDS].index(a.dataset)
    if a.batch < 1 or a.threads < 1:
        p.error("--batch and --threads must be positive")
    if a.load and os.path.isdir(a.dir) and os.listdir(a.dir):
        sys.exit(f"{a.dir}: not empty; --load writes a new database")
    db = tsdb_client.Db(
        a.dir, zstd_level=a.zstd, fsync=a.fsync, query_threads=a.query_threads if a.query_threads > 0 else None
    )
    if a.load:
        load(db, a, a.load)
    queries(db, a.seconds)
    db.close()


if __name__ == "__main__":
    main()
