"""Query benchmark: the random windows of `tsdbc-bench query`, over TCP from this client.

    python bench_query.py [--addr 127.0.0.1:7070] [--series 100] [--points 1000000] [--conns 1] [--seconds 3]
        [--many N [--pool CONNS]]

Every connection (a thread each) draws windows from the same generator and seed as the C benchmark
(bench_common.py, tools/cbench.h): raw rows of 3600 samples and two aggregate classes. Raw rows are fetched
as numpy arrays when numpy is installed (lists otherwise) and only counted; aggregates are counted from the
returned buckets. The output is the line of `tsdbc-bench query` for each class, and a RESULT line for
bench/clients/run.sh.

The threads share one interpreter, so the GIL limits what many connections add: the client releases it while
it waits for the server and while it sends, but the Python code around each call (the window arithmetic, the
bucket loop) and the conversion of the answer into Python objects run one thread at a time. A run at 32
connections therefore measures the interpreter as much as the server; run several processes for more.

--many N asks the same windows N at a time through a tsdb_client.Pool of --pool connections (default: one per CPU
here) shared by the --conns threads (client python-batch): one call per N queries, spread over the pool's
connections with the GIL released. --conns then counts the Python threads calling, not the connections.
"""

import argparse
import threading
import time

import tsdb_client

import bench_common as bc

try:
    import numpy as np  # noqa: F401
except ImportError:
    np = None


def querier(args, klass, thread, out, errors, pool=None):
    if pool is not None:
        return batch_querier(args, klass, thread, out, errors, pool)
    try:
        host, port = args.addr.rsplit(":", 1)
        _, _, agg, window, bucket = klass
        c = tsdb_client.Client(host, int(port))
        ids = [c.series(bc.series_name(i), create=False) for i in range(args.series)]  # number -> id
        r = bc.seed(thread)
        queries = rows = 0
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < args.seconds:
            for _ in range(16):
                r, s, start, end = bc.window(r, args.series, args.points, window)
                if not agg:
                    ts, _v = c.query_raw(ids[s], start, end)
                    rows += len(ts)
                else:
                    b = c.query_agg(ids[s], start, end, bucket * bc.step_ms(s))
                    rows += int(b.counts.sum()) if np is not None else sum(b.count)
                queries += 1
        secs = time.perf_counter() - t0
        c.close()
        out.append((queries, rows, secs))
    except Exception as e:  # noqa: BLE001 - reported by the main thread
        errors.append(e)


def batch_querier(args, klass, thread, out, errors, pool):
    """As querier(), --many windows per call through the pool."""
    try:
        _, _, agg, window, bucket = klass
        r = bc.seed(thread)
        queries = rows = 0
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < args.seconds:
            r, s, start, end, interval = bc.batch(r, args.many, args.series, args.points, window, bucket)
            s = [args.ids[i] for i in s]  # series numbers -> ids
            if not agg:
                rows += sum(len(ts) for ts, _v in pool.query_raw_many(s, start, end))
            else:
                rows += int(pool.query_agg_many(s, start, end, interval).buckets.counts.sum())
            queries += args.many
        out.append((queries, rows, time.perf_counter() - t0))
    except Exception as e:  # noqa: BLE001 - reported by the main thread
        errors.append(e)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--addr", default="127.0.0.1:7070")
    p.add_argument("--series", type=int, default=100)
    p.add_argument("--points", type=int, default=1_000_000)
    p.add_argument("--conns", type=int, default=1)
    p.add_argument("--seconds", type=float, default=3.0)
    p.add_argument("--many", type=int, default=0)
    p.add_argument("--pool", type=int, default=0)
    args = p.parse_args()
    pool = None
    if args.many > 0:
        if np is None:
            raise SystemExit("--many needs numpy")
        host, port = args.addr.rsplit(":", 1)
        pool = tsdb_client.Pool(host, int(port), args.pool or None)
        with tsdb_client.Client(host, int(port)) as c0:
            args.ids = [c0.series(bc.series_name(i), create=False) for i in range(args.series)]

    for klass in bc.CLASSES:
        out, errors = [], []
        cpu0 = bc.cpu_s()
        threads = [threading.Thread(target=querier, args=(args, klass, i, out, errors, pool)) for i in range(args.conns)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise SystemExit(f"query: {errors[0]}")
        rows = sum(o[1] for o in out)
        bc.report_query(
            "python-batch" if pool is not None else "python",
            klass,
            args.conns,
            sum(o[0] for o in out),
            rows,
            max(o[2] for o in out),
            bc.cpu_s() - cpu0,
        )
        if rows == 0:
            raise SystemExit(f"query: class {klass[0]} returned no rows: the server is empty or does not hold the data set")
    if pool is not None:
        pool.close()


if __name__ == "__main__":
    main()
