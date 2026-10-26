"""Embedded benchmark for bench/clients/run.sh: the --load and --qps modes of build/tsdb-bench, through
tsdb_client.Db (the engine in this process, no tsdbd).

    build/tsdb-bench --export /data/x.rowbinary --series 100 --points 1000000
    python bench_embedded.py --dir /data/emb --load /data/x.rowbinary [--threads 4] [--batch 1000000] [--zstd 1]
        [--no-fsync] [--optimize]
    python bench_embedded.py --dir /data/emb --qps THREADS [--series 100] [--points 1000000] [--seconds 3]
        [--query-threads N] [--many N]

--load wipes --dir, creates the series under tsdb-bench's names (ids in file order), and has each of --threads
writers (Python threads: Db releases the GIL while the engine works) insert one contiguous slice of the file's rows
in batches of --batch, copying them from the file's packed rows into numpy columns inside the timing, as tsdb-bench
does. Then (after --optimize) it checks that the database holds exactly the file's rows: count and sum per series
from an aggregate, and a hash of every (timestamp, value) read back (tools/cbench.h).

--qps opens an existing database and runs the random windows of the three classes of bench_common.py on THREADS
threads sharing one Db, --seconds per class: the same generator, seeds and windows as `tsdb-bench --qps`. Raw rows
come back as numpy arrays and are only counted; aggregates are counted from the returned buckets. The threads share
the GIL outside the engine, as in bench_query.py. Both modes print tsdb-bench's lines and a RESULT line (client
embedded-python...) for bench/clients/run.sh. Needs numpy.

--many N asks the same windows N at a time with query_raw_many / query_agg_many (client embedded-python-batch):
each of the THREADS Python threads draws N windows, and one call runs them on every CPU with the GIL released.
THREADS then counts the Python threads calling, not the threads that run the queries.
"""

import argparse
import os
import shutil
import sys
import threading
import time

import numpy as np
import tsdb_client

import bench_common as bc

# tools/datasets.h ds_info[i].name for the 7 kinds of the mixed data set (series i is kind i % 7).
NAMES = ("hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate", "flow_total")
ROW = np.dtype([("s", "<u4"), ("t", "<i8"), ("v", "<f4")])  # packed: 16 bytes
U64 = np.uint64


def mix(x):
    """bench_common.mix over a uint64 array (wrapping arithmetic)."""
    x = x ^ (x >> U64(33))
    x = x * U64(0xFF51AFD7ED558CCD)
    x = x ^ (x >> U64(33))
    x = x * U64(0xC4CEB9FE1A85EC53)
    return x ^ (x >> U64(33))


def row_hash(t, v):
    """tools/cbench.h cb_row_hash of int64 timestamps and float32 values, as a uint64 array."""
    bits = np.ascontiguousarray(v, dtype=np.float32).view(np.uint32).astype(U64)
    with np.errstate(over="ignore"):
        return mix((t.astype(np.int64).view(U64) * U64(0x9E3779B97F4A7C15)) ^ ((bits << U64(17)) | bits))


def expect(rows):
    """tools/cbench.h cb_expect: count, sum, sum of magnitudes and hash of the rows per series of the file."""
    s = rows["s"]
    ns = int(s.max()) + 1
    v = rows["v"].astype(np.float64)
    count = np.bincount(s, minlength=ns)
    total = np.bincount(s, weights=v, minlength=ns)
    mag = np.bincount(s, weights=np.abs(v), minlength=ns)
    h = [0] * ns
    order = np.argsort(s, kind="stable")
    hashes = row_hash(rows["t"][order], rows["v"][order])
    starts = np.concatenate(([0], np.cumsum(count)[:-1]))
    for i in range(ns):
        if count[i]:
            h[i] = int(np.sum(hashes[starts[i] : starts[i] + count[i]], dtype=U64))
    return count, total, mag, h


def verify(db, rows):
    """cb_expect_verify_all: prints the reasons (up to 5) and the verdict line; True if the database holds exactly
    the file's rows."""
    count, total, mag, h = expect(rows)
    found = bad = 0
    for s in range(len(count)):
        b = db.query_agg(s)
        c, sm = (b[0].count, b[0].sum) if b else (0, 0.0)
        found += c
        why = None
        if c != count[s] or abs(sm - total[s]) > 1e-9 * mag[s] + 1e-9:
            why = f"holds {c} rows, sum {sm:.17g}; the file has {count[s]}, sum {total[s]:.17g}"
        else:
            ts, v = db.query_raw(s, numpy=True)
            got = int(np.sum(row_hash(ts, v), dtype=U64)) if len(ts) else 0
            if len(ts) != count[s] or got != h[s]:
                why = f"reads back {len(ts)} rows with timestamps or values other than the file's ({count[s]} rows)"
        if why:
            if bad < 5:
                print(f"verify: series {s} {why}", file=sys.stderr)
            bad += 1
    ok = bad == 0 and found == len(rows)
    print(f"verify: {len(count)} series, {found} rows in the database, {len(rows)} in the file: "
          f"{'ok' if ok else 'MISMATCH'}")
    return ok


def load(a):
    rows = np.fromfile(a.load, dtype=ROW)
    if len(rows) == 0 or os.path.getsize(a.load) % ROW.itemsize:
        sys.exit(f"{a.load}: not a RowBinary file of 16-byte rows (series u32, ts i64, value f32)")
    shutil.rmtree(a.dir, ignore_errors=True)
    db = tsdb_client.Db(a.dir, zstd_level=a.zstd, fsync=a.fsync)
    for i in range(int(rows["s"].max()) + 1):
        sid = db.series(f"{NAMES[i % 7]}.{i}")
        if sid != i:  # verification and the query benchmarks rely on ids equal to the file's indexes
            sys.exit(f"series {NAMES[i % 7]}.{i}: id {sid}, expected {i}")
    n, nt, errors = len(rows), a.threads, []

    def writer(lo, hi):
        try:
            for off in range(lo, hi, a.batch):
                part = rows[off : min(hi, off + a.batch)]
                db.insert(*(np.ascontiguousarray(part[f]) for f in ("s", "t", "v")))
        except Exception as e:  # noqa: BLE001 - reported by the main thread
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(n * i // nt, n * (i + 1) // nt)) for i in range(nt)]
    t0, cpu0 = time.perf_counter(), bc.cpu_s()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt, cpu = time.perf_counter() - t0, bc.cpu_s() - cpu0
    if errors:
        sys.exit(f"insert: {errors[0]}")
    client = "embedded-python-fsync" if a.fsync else "embedded-python-nofsync"
    extra = f", batches of {a.batch}, fsync {'on' if a.fsync else 'off'}, zstd {a.zstd}"
    bc.report_load(client, nt, 0, n, dt, 0, cpu, extra)
    if a.optimize:
        t0 = time.perf_counter()
        db.optimize()
        print(f"optimize {time.perf_counter() - t0:.2f} s")
    ok = verify(db, rows)
    st = db.stats()
    print(f"storage {st.bytes / 1048576.0:.2f} MiB, {st.bytes / max(st.rows, 1):.3f} bytes/row")  # rows: in parts, not the memtable
    db.close()
    return ok


def querier(db, a, klass, thread, out, errors):
    """One thread's queries of one class for --seconds (tools/tsdb-bench.c qw_loop)."""
    if a.many:
        return batch_querier(db, a, klass, thread, out, errors)
    try:
        _, _, agg, window, bucket = klass
        r = bc.seed(thread)
        queries = rows = 0
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < a.seconds:
            for _ in range(16):
                r, s, start, end = bc.window(r, a.series, a.points, window)
                if not agg:
                    ts, v = db.query_raw(s, start, end, numpy=True)
                    rows += len(ts)
                    if len(v):
                        float(v[-1])  # touch the data, as tsdb-bench does
                else:
                    rows += int(db.query_agg(s, start, end, bucket * bc.step_ms(s)).counts.sum())
                queries += 1
        out.append((queries, rows, time.perf_counter() - t0))
    except Exception as e:  # noqa: BLE001 - reported by the main thread
        errors.append(e)


def batch_querier(db, a, klass, thread, out, errors):
    """As querier(), --many windows per call."""
    try:
        _, _, agg, window, bucket = klass
        r = bc.seed(thread)
        queries = rows = 0
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < a.seconds:
            r, s, start, end, interval = bc.batch(r, a.many, a.series, a.points, window, bucket)
            if not agg:
                for ts, v in db.query_raw_many(s, start, end):
                    rows += len(ts)
                    if len(v):
                        float(v[-1])
            else:
                rows += int(db.query_agg_many(s, start, end, interval).buckets.counts.sum())
            queries += a.many
        out.append((queries, rows, time.perf_counter() - t0))
    except Exception as e:  # noqa: BLE001 - reported by the main thread
        errors.append(e)


def qps(a):
    if not os.path.isdir(a.dir):  # opening would create an empty database
        print(f"qps: {a.dir} is not a database directory", file=sys.stderr)
        return False
    db = tsdb_client.Db(a.dir, query_threads=a.query_threads if a.query_threads > 0 else None)
    for klass in bc.CLASSES:
        out, errors = [], []
        cpu0 = bc.cpu_s()
        threads = [threading.Thread(target=querier, args=(db, a, klass, i, out, errors)) for i in range(a.qps)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            sys.exit(f"query: {errors[0]}")
        rows = sum(o[1] for o in out)
        secs = max(o[2] for o in out)
        name = "embedded-python-batch" if a.many else "embedded-python"
        bc.report_query(name, klass, a.qps, sum(o[0] for o in out), rows, secs, bc.cpu_s() - cpu0, "thr")
        if rows == 0:
            print(f"qps: class {klass[0]} returned no rows: the database is empty or does not hold the data set",
                  file=sys.stderr)
            return False
    db.close()
    return True


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--dir", required=True)
    p.add_argument("--load")
    p.add_argument("--qps", type=int, default=0)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--batch", type=int, default=1_000_000)
    p.add_argument("--zstd", type=int, default=1)
    p.add_argument("--no-fsync", dest="fsync", action="store_false")
    p.add_argument("--optimize", action="store_true")
    p.add_argument("--series", type=int, default=100)
    p.add_argument("--points", type=int, default=1_000_000)
    p.add_argument("--seconds", type=float, default=3.0)
    p.add_argument("--query-threads", type=int, default=0)
    p.add_argument("--many", type=int, default=0)
    a = p.parse_args()
    if (not a.load and a.qps < 1) or a.threads < 1 or a.batch < 1 or a.series < 1 or a.many < 0:
        p.error("give --load FILE or --qps THREADS; counts must be positive")
    sys.exit(0 if (load(a) if a.load else qps(a)) else 1)


if __name__ == "__main__":
    main()
