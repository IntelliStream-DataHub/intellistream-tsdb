"""Input columns: strided views, 2-D columns and big-endian arrays give the same rows as plain
lists, in every call that takes columns; an array another thread changes during a call never
reaches the engine half changed (the call reads its own copy)."""

import subprocess
import sys

import numpy as np
import pytest

from tsdb_client import Db, Pool

T0 = 1_700_000_000_000


@pytest.fixture
def db(tmp_path):
    with Db(tmp_path / "db", fsync=False, merge_threads=1) as d:
        yield d


def strided(a):
    """a as every other element of a twice as long array."""
    b = np.zeros(2 * len(a), dtype=a.dtype)
    b[::2] = a
    return b[::2]


def column(a):
    """a as the first column of a 2-D array (a strided view)."""
    b = np.zeros((len(a), 3), dtype=a.dtype)
    b[:, 0] = a
    return b[:, 0]


def big(a):
    """a in big-endian byte order."""
    return a.astype(a.dtype.newbyteorder(">"))


FORMS = {"strided": strided, "column": column, "big-endian": big}


def rows(c, s, a, b):
    return c.query_raw(s, a, b, numpy=False)


def check_inserts(c, form):
    f = FORMS[form]
    s1, s2 = c.series(f"{form}-1"), c.series(f"{form}-2")
    ts = np.arange(6, dtype=np.int64) + T0
    vals = np.arange(6, dtype=np.float32) + 0.5
    v = [float(x) for x in vals]

    def at(off, sel=slice(None)):
        return ([int(t) + off for t in ts][sel], v[sel])

    ids = np.array([s1, s2] * 3, dtype=np.uint32)
    c.insert_series(s1, f(ts), f(vals))
    assert rows(c, s1, T0, T0 + 6) == at(0)
    c.insert(f(ids), f(ts + 10), f(vals))
    assert rows(c, s1, T0 + 10, T0 + 16) == at(10, slice(0, None, 2))
    assert rows(c, s2, T0 + 10, T0 + 16) == at(10, slice(1, None, 2))
    c.insert_series_values(s1, f(ts + 20), f(vals.astype(np.float64)))
    assert rows(c, s1, T0 + 20, T0 + 26) == at(20)
    c.insert_many([(s1, f(ts + 30), f(vals)), (f(ids), f(ts + 40), f(vals.astype(np.float64)))])
    assert rows(c, s1, T0 + 30, T0 + 36) == at(30)
    assert rows(c, s2, T0 + 40, T0 + 46) == at(40, slice(1, None, 2))
    i64 = c.series_create(f"{form}-i", "int64")
    c.insert_many([(i64, f(ts), f(np.arange(6, dtype=np.int64) * 2**40))])
    assert c.query_values(i64, numpy=False)[1] == [k * 2**40 for k in range(6)]
    # batch arguments
    q = np.array([s1, s2, s1], dtype=np.uint32)
    start = np.array([T0, T0 + 40, T0 + 30], dtype=np.int64)
    end = start + 6
    for got, s, a, b in zip(c.query_raw_many(f(q), f(start), f(end)), q, start, end):
        assert (list(got[0]), list(got[1])) == rows(c, int(s), int(a), int(b))
    agg = c.query_agg_many(f(q), f(start), f(end), f(np.array([2, 3, 6], dtype=np.int64)))
    for got, s, a, b, iv in zip(agg, q, start, end, (2, 3, 6)):
        assert got == c.query_agg(int(s), int(a), int(b), iv)
    # bucket bounds
    bounds = np.array([T0, T0 + 2, T0 + 5], dtype=np.int64)
    assert c.query_bounds(s1, f(bounds), T0, T0 + 6) == c.query_bounds(s1, list(bounds), T0, T0 + 6)


@pytest.mark.parametrize("form", FORMS)
def test_db_input_forms(db, form):
    check_inserts(db, form)


@pytest.mark.parametrize("form", FORMS)
def test_tcp_input_forms(server, client, form):
    with Pool("127.0.0.1", server.port, 2, timeout=10) as pool:

        class Both:
            def __getattr__(self, name):
                return getattr(pool if name.endswith("_many") else client, name)

        check_inserts(Both(), form)


RACE = """
import sys, threading, numpy as np, tsdb_client as t
T0, n, big = 1_700_000_000_000, 200_000, 2_000_000_000
with t.Db(sys.argv[1], fsync=False, merge_threads=1) as db:
    ids = [db.series(f"s{k}") for k in range(10)]
    ser = np.repeat(np.array(ids, dtype=np.uint32), n // 10)
    ts = np.tile(np.arange(n // 10, dtype=np.int64) + T0, 10)
    vals = np.ones(n, dtype=np.float32)
    stop, j = [False], n // 2

    def flip():
        while not stop[0]:
            ser[j] = big  # no such series: an insert that sees it fails as a whole
            ser[j] = ids[5]

    th = threading.Thread(target=flip)
    th.start()
    refused = 0
    try:
        for it in range(int(sys.argv[2])):
            ts += n
            try:
                if it % 2:
                    db.insert(ser, ts, vals)
                else:
                    db.insert_many([(ser, ts, vals)])
            except t.TsdbError:
                refused += 1
    finally:
        stop[0] = True
        th.join()
    db.optimize()
    rows = sum(int(b.counts.sum()) for b in db.query_agg_many(ids))
    assert rows == n * (int(sys.argv[2]) - refused), (rows, refused)
print("ok")
"""


def test_an_array_changed_during_a_call(tmp_path):
    """Another thread flips a series id between a valid one and one far out of range while
    inserts read the column. Each insert must see one value or the other, never check one and
    use the other: the engine reads the ids twice (a bound check, then an index), which on a
    column read in place crashed the process or corrupted parts (a later merge failed)."""
    for k in range(3):
        d = tmp_path / f"db{k}"
        p = subprocess.run([sys.executable, "-c", RACE, str(d), "40"], capture_output=True, text=True, timeout=300)
        assert p.returncode == 0 and p.stdout.strip() == "ok", (p.returncode, p.stderr[-2000:])
