# iotload.py: the IoTDB loader, through IoTDB's own client (the Python TableSession, insert of tablets over the
# Thrift RPC on 6667), the bulk path IoTDB's docs recommend for applications. Run with the venv of
# iotdb-requirements.txt:
#   iotload.py FILE HOST PORT DATABASE TABLE ROWS_PER_TABLET PROCS
# FILE is the benchmark's RowBinary (series u32, ts i64 ms, value f32; 16 bytes a row). The rows are cut into
# tablets of ROWS_PER_TABLET in file order; PROCS processes take the next tablet from a shared counter (as loadlp
# does with batches), so tablets go out roughly in file order. One tablet holds every series of its rows (the
# table model's tablets may span devices: the tag column says which); IoTDB's client sorts a tablet by time, which
# is done here with numpy (stable, so equal timestamps keep their file order: last write wins as in the file).
# The client builds the STRING tag column in a Python loop; here it comes from per-series byte strings, the same
# bytes, so the client's own cost does not slow the server's numbers. Prints one line like loadlp's.
import mmap, multiprocessing as mp, os, sys, time

import numpy as np
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import ColumnType

ROW = np.dtype([("s", "<u4"), ("t", "<i8"), ("v", "<f4")])  # packed, 16 bytes
assert ROW.itemsize == 16


class FastTablet(NumpyTablet):
    """NumpyTablet whose tag column bytes are given (big-endian length + UTF-8 per row, as the client writes them)."""

    def __init__(self, tag_bytes, *a, **k):
        super().__init__(*a, **k)
        self.tag_bytes = tag_bytes

    @staticmethod
    def check_sorted(t):
        return bool(np.all(t[1:] >= t[:-1]))

    def get_binary_values(self):
        vals = self.get_values()
        return self.tag_bytes + vals[1].astype(">f4").tobytes()


def worker(args):
    path, host, port, db, table, per, counter, nchunks = args
    cfg = TableSessionConfig(node_urls=[f"{host}:{port}"], database=db, enable_redirection=False)
    sess = TableSession(cfg)
    f = open(path, "rb")
    m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    rows = np.frombuffer(m, dtype=ROW)
    blobs = {}
    sent = 0
    while True:
        with counter.get_lock():
            c = counter.value
            counter.value += 1
        if c >= nchunks:
            break
        r = rows[c * per:(c + 1) * per]
        o = np.argsort(r["t"], kind="stable")
        s, t, v = r["s"][o], r["t"][o], r["v"][o]
        parts = []
        for x in s.tolist():
            b = blobs.get(x)
            if b is None:
                e = str(x).encode()
                b = blobs[x] = len(e).to_bytes(4, "big") + e
            parts.append(b)
        tab = FastTablet(b"".join(parts), table, ["series", "value"], [TSDataType.STRING, TSDataType.FLOAT],
                         [s.astype(str), v], t, column_types=[ColumnType.TAG, ColumnType.FIELD])
        sess.insert(tab)
        sent += 1
    sess.close()
    return sent


def main():
    if len(sys.argv) != 8:
        sys.exit("usage: iotload.py FILE HOST PORT DATABASE TABLE ROWS_PER_TABLET PROCS")
    path, host, port, db, table = sys.argv[1:6]
    per, procs = int(sys.argv[6]), int(sys.argv[7])
    n = os.path.getsize(path) // ROW.itemsize
    nchunks = (n + per - 1) // per
    counter = mp.Value("q", 0)
    print(f"{n} rows in {nchunks} tablets of {per} rows, {procs} processes", file=sys.stderr)
    ctx = mp.get_context("fork")
    t0 = time.monotonic()
    procs_ = [ctx.Process(target=lambda a=(path, host, int(port), db, table, per, counter, nchunks): sys.exit(0 if worker(a) >= 0 else 1))
              for _ in range(procs)]
    for p in procs_: p.start()
    rc = 0
    for p in procs_:
        p.join()
        rc |= p.exitcode != 0
    dt = time.monotonic() - t0
    if rc:
        sys.exit("iotload: a loader process failed")
    print(f"loaded {n} rows in {dt:.2f} s = {n / dt / 1e6:.3f} M rows/s ({nchunks} tablets, {procs} processes, {per} rows/tablet)")


if __name__ == "__main__":
    main()
