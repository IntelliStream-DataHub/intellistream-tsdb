"""Insert benchmark: loads a RowBinary file into tsdbd, as `tsdbc-bench load`.

    python bench_insert.py FILE [--addr 127.0.0.1:7070] [--conns 4] [--batch 1000000]
        [--compression 1] [--inflight 4]

FILE holds rows of u32 series, i64 ts (ms), f32 value (16 bytes each), as
`build/tsdb-bench --export FILE` writes them. It is split into contiguous
slices, one per connection (a thread each: the client releases the GIL while
it sends); each connection sends batches with the many-series layout,
keeping up to --inflight of them unacknowledged. --compression 0 sends plain
arrays; any other zstd level packs them (protocol version 3). Series
`<kind>.<n>` (tsdb-bench's names, so that `tsdbc-bench verify` finds them) are created first, outside the
timing. The u32 is the series number n, an index into the table of ids that series() returned (`ids[n]`); it is
not a series id, which is 64-bit and carries the volume. Reports rows/s and the
bytes written to the sockets per row and the CPU time of the process, in the
format of tsdbc-bench (see bench_common.py). Uses numpy if it is installed (much
faster to slice the file); without it, rows are unpacked in Python.
"""

import argparse
import struct
import sys
import threading
import time

import tsdb_client

import bench_common as bc

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

ROW = struct.Struct("<Iqf")


def read_rows(path):
    """The rows as (series, ts, values) columns: numpy arrays, or a raw buffer to unpack per batch."""
    if np is not None:
        rows = np.fromfile(path, dtype=np.dtype([("s", "<u4"), ("t", "<i8"), ("v", "<f4")]))
        return rows, len(rows)
    with open(path, "rb") as f:
        raw = f.read()
    return memoryview(raw)[: len(raw) // ROW.size * ROW.size], len(raw) // ROW.size


def batch_columns(rows, ids, a, b):
    """Columns of rows [a, b), series mapped to their ids on the server."""
    if np is not None:
        r = rows[a:b]
        return ids[r["s"]], np.ascontiguousarray(r["t"]), np.ascontiguousarray(r["v"])
    s, t, v = [], [], []
    for si, ti, vi in ROW.iter_unpack(rows[a * ROW.size : b * ROW.size]):
        s.append(ids[si])
        t.append(ti)
        v.append(vi)
    return s, t, v


def load(args, rows, ids, a, b, sent, errors):
    try:
        host, port = args.addr.rsplit(":", 1)
        c = tsdb_client.Client(host, int(port))
        c.set_compression(args.compression)
        for off in range(a, b, args.batch):
            if c.pending >= args.inflight:
                c.wait()
            c.insert_async(*batch_columns(rows, ids, off, min(off + args.batch, b)))
        c.wait()
        sent.append(c.bytes_sent)
        c.close()
    except Exception as e:  # noqa: BLE001 - reported by the main thread
        errors.append(e)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("file")
    p.add_argument("--addr", default="127.0.0.1:7070")
    p.add_argument("--conns", type=int, default=4)
    p.add_argument("--batch", type=int, default=1_000_000)
    p.add_argument("--compression", type=int, default=tsdb_client.DEFAULT_COMPRESSION)
    p.add_argument("--inflight", type=int, default=4)
    args = p.parse_args()

    rows, n = read_rows(args.file)
    if np is not None:
        top = int(rows["s"].max()) if n else 0
    else:
        top = max((s for s, _, _ in ROW.iter_unpack(rows)), default=0)
    host, port = args.addr.rsplit(":", 1)
    with tsdb_client.Client(host, int(port)) as c:
        ids = [c.series(bc.series_name(i)) for i in range(top + 1)]
    if np is not None:
        ids = np.array(ids, dtype=np.uint64)

    sent, errors = [], []
    t0, cpu0 = time.perf_counter(), bc.cpu_s()
    threads = [
        threading.Thread(
            target=load, args=(args, rows, ids, n * k // args.conns, n * (k + 1) // args.conns, sent, errors)
        )
        for k in range(args.conns)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt, cpu = time.perf_counter() - t0, bc.cpu_s() - cpu0
    if errors:
        sys.exit(f"insert: {errors[0]}")
    bc.report_load(
        "python",
        args.conns,
        args.compression,
        n,
        dt,
        sum(sent),
        cpu,
        f", batches of {args.batch}, {args.inflight} in flight, numpy {'yes' if np is not None else 'no'}",
    )


if __name__ == "__main__":
    main()
