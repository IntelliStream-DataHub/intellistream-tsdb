"""Batch calls (query_raw_many, query_agg_many, insert_many) on Db and Pool, and Buckets.

A batch must answer exactly what the one-query calls answer, query by query, in query order; an
error in one query fails the call with that query's error; a Pool replaces a connection that broke.
"""

import socket
import threading

import numpy as np
import pytest

import tsdb_client
from tsdb_client import Bucket, Buckets, BucketsMany, Db, Pool, TsdbError

T0 = 1_699_999_980_000
N = 2000  # rows per series, 1 s apart


def fill(c):
    """Five float32 series with different values, a float64 and an int64 series."""
    ids = [c.series(f"b{i}") for i in range(5)]
    ts = np.arange(N, dtype=np.int64) * 1000 + T0
    for k, s in enumerate(ids):
        c.insert_series(s, ts, (np.arange(N) * (k + 1) * 0.5).astype(np.float32))
    f64 = c.series_create("f64", "float64")
    c.insert_series_values(f64, ts[:10], np.arange(10, dtype=np.float64) / 3)
    i64 = c.series_create("i64", "int64")
    c.insert_series_values(i64, ts[:10], np.arange(10, dtype=np.int64) * (2**40))
    c.flush()
    return ids, f64, i64


def windows(ids):
    """Queries over ranges of different lengths, an empty one and an open end."""
    series = [ids[i % len(ids)] for i in range(12)]
    start = [T0 + i * 37_000 for i in range(12)]
    end = [a + (i % 4) * 400_000 for i, a in enumerate(start)]  # i % 4 == 0: an empty range
    return series, start, end


def check_raw(c, ids):
    series, start, end = windows(ids)
    many = c.query_raw_many(series, start, end)
    assert len(many) == len(series)
    for (ts, v), s, a, b in zip(many, series, start, end):
        one = c.query_raw(s, a, b, numpy=True)
        assert ts.dtype == np.int64 and v.dtype == np.float32
        assert np.array_equal(ts, one[0]) and np.array_equal(v, one[1])
    # numpy columns in, a scalar start for every query, None for an open end, desc with a limit
    arr = c.query_raw_many(np.array(series, dtype=np.uint32), T0 + 5000, None, order="desc", limit=3)
    for (ts, v), s in zip(arr, series):
        one = c.query_raw(s, T0 + 5000, None, order="desc", limit=3, numpy=True)
        assert np.array_equal(ts, one[0]) and np.array_equal(v, one[1])
    assert c.query_raw_many([]) == []
    with pytest.raises(ValueError):
        c.query_raw_many(series, start[:3], end)  # one start per query, or one for all


def check_agg(c, ids):
    series, start, end = windows(ids)
    intervals = [(i % 3) * 60_000 for i in range(len(series))]  # 0: one bucket for the range
    many = c.query_agg_many(series, start, end, intervals)
    assert isinstance(many, BucketsMany) and len(many) == len(series)
    off = many.offsets
    assert off.dtype == np.int64 and len(off) == len(series) + 1 and off[0] == 0
    assert off[-1] == len(many.buckets)
    for i, (s, a, b, iv) in enumerate(zip(series, start, end, intervals)):
        one = c.query_agg(s, a, b, iv)
        assert many[i] == one and many[i] == list(one)
        assert np.array_equal(many.buckets.count[off[i] : off[i + 1]], one.count)
    assert many[-1] == many[len(series) - 1]
    with pytest.raises(IndexError):
        many[len(series)]
    # one interval for every query, the default (0) and numpy arrays
    hourly = c.query_agg_many(np.array(series, dtype=np.uint32), np.array(start), np.array(end), 3_600_000)
    assert [list(b) for b in hourly] == [list(c.query_agg(s, a, b, 3_600_000)) for s, a, b in zip(series, start, end)]
    assert int(c.query_agg_many(series).buckets.count.sum()) == N * len(series)


def check_errors(c, ids, i64):
    series, start, end = windows(ids)
    bad = list(series)
    bad[5] = i64  # a raw float32 query of an int64 series
    with pytest.raises(TsdbError) as e:
        c.query_raw_many(bad, start, end)
    with pytest.raises(TsdbError) as one:
        c.query_raw(i64, start[5], end[5])
    assert e.value.code == one.value.code
    # the batch after a failed one answers as before
    assert len(c.query_raw_many(series, start, end)) == len(series)
    with pytest.raises(TsdbError):
        c.query_agg_many(series, start, end, -1)  # a negative bucket length


def check_insert_many(c, ids, f64, i64):
    ts = np.arange(5, dtype=np.int64) + T0 + 10_000_000
    vals = np.ma.masked_array(np.arange(5, dtype=np.float64), mask=[0, 1, 0, 0, 0])
    c.insert_many(
        [
            (ids[0], ts, np.full(5, 7.0, dtype=np.float32)),
            (np.array([ids[1], ids[2]] * 2 + [ids[1]], dtype=np.uint32), ts, [1.0, 2.0, 3.0, 4.0, 5.0]),
            (f64, ts, vals),  # float64 from the dtype, a null where masked
            (i64, list(ts), np.arange(5, dtype=np.int64) - 2),
        ]
    )
    end = int(ts[-1]) + 1
    assert c.query_raw(ids[0], int(ts[0]), end, numpy=False)[1] == [7.0] * 5
    assert c.query_raw(ids[1], int(ts[0]), end, numpy=False) == ([int(ts[i]) for i in (0, 2, 4)], [1.0, 3.0, 5.0])
    assert c.query_raw(ids[2], int(ts[0]), end, numpy=False) == ([int(ts[i]) for i in (1, 3)], [2.0, 4.0])
    assert c.query_values(f64, int(ts[0]), end, numpy=False)[1] == [0.0, None, 2.0, 3.0, 4.0]
    assert c.query_values(i64, int(ts[0]), end, numpy=False)[1] == [-2, -1, 0, 1, 2]
    with pytest.raises(TsdbError):
        c.insert_many([(ids[0], ts, np.zeros(5, dtype=np.float32)), (f64, ts, np.zeros(5, dtype=np.int64))])
    # A value per timestamp: a length mismatch is the client library's ERR_INVALID, as in insert_series.
    with pytest.raises(TsdbError) as many:
        c.insert_many([(ids[0], ts, np.zeros(4, dtype=np.float32))])
    with pytest.raises(TsdbError) as single:
        c.insert_series(ids[0], ts, np.zeros(4, dtype=np.float32))
    assert many.value.code == single.value.code == tsdb_client.ERR_INVALID
    c.insert_many([])


@pytest.fixture
def db(tmp_path):
    with Db(tmp_path / "db", fsync=False, merge_threads=1) as d:
        yield d


def test_db_batches_answer_as_single_queries(db):
    ids, f64, i64 = fill(db)
    check_raw(db, ids)
    check_agg(db, ids)
    check_errors(db, ids, i64)
    check_insert_many(db, ids, f64, i64)


def test_pool_batches_answer_as_single_queries(server, client):
    ids, f64, i64 = fill(client)
    with Pool("127.0.0.1", server.port, 3, timeout=10) as pool:
        assert pool.connections == 3
        check_raw(Both(pool, client), ids)
        check_agg(Both(pool, client), ids)
        check_errors(Both(pool, client), ids, i64)
        check_insert_many(Both(pool, client), ids, f64, i64)
    with pytest.raises(TsdbError):
        pool.query_raw_many([ids[0]])  # closed
    with pytest.raises(ValueError):
        Pool("127.0.0.1", server.port, 0)


class Both:
    """The batch calls go to the pool, the rest to the client of the same server."""

    def __init__(self, pool, client):
        self.pool, self.client = pool, client

    def __getattr__(self, name):
        return getattr(self.pool if name.endswith("_many") else self.client, name)


class Proxy:
    """Forwards connections to a server; drop() breaks every connection made so far."""

    def __init__(self, port):
        self.lsock = socket.create_server(("127.0.0.1", 0))
        self.port = self.lsock.getsockname()[1]
        self.target, self.socks, self.lock = port, [], threading.Lock()
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                a, _ = self.lsock.accept()
            except OSError:
                return
            b = socket.create_connection(("127.0.0.1", self.target))
            with self.lock:
                self.socks += [a, b]
            for x, y in ((a, b), (b, a)):
                threading.Thread(target=self.pipe, args=(x, y), daemon=True).start()

    @staticmethod
    def pipe(x, y):
        try:
            while d := x.recv(65536):
                y.sendall(d)
        except OSError:
            pass
        for s in (x, y):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def drop(self):
        with self.lock:
            socks, self.socks = self.socks, []
        for s in socks:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            s.close()

    def close(self):
        self.lsock.close()
        self.drop()


def test_pool_replaces_a_broken_connection(server, client):
    ids, _, _ = fill(client)
    series, start, end = windows(ids)
    proxy = Proxy(server.port)
    try:
        with Pool("127.0.0.1", proxy.port, 4, timeout=10) as pool:
            first = pool.query_agg_many(series, start, end, 60_000)
            assert [list(b) for b in first] == [list(client.query_agg(s, a, b, 60_000)) for s, a, b in zip(series, start, end)]
            proxy.drop()
            # the queries that meet a dropped connection fail the call; the connection is replaced after
            with pytest.raises(TsdbError):
                pool.query_agg_many(series * 4, start * 4, end * 4, 60_000)
            again = pool.query_agg_many(series, start, end, 60_000)
            assert [list(b) for b in again] == [list(b) for b in first]
    finally:
        proxy.close()


def test_buckets_read_as_the_old_list(db):
    s = db.series("x")
    db.insert_series(s, [T0 + i * 1000 for i in range(600)], [float(i) for i in range(600)])
    b = db.query_agg(s, T0, T0 + 600_000, 60_000)
    assert isinstance(b, Buckets) and len(b) == 10
    old = list(b)
    assert all(isinstance(x, Bucket) for x in old)
    assert b == old and old == list(b) and b == b and b != old[:-1]
    assert b[0] == old[0] and b[-1] == old[-1] and b[2:5] == old[2:5] and list(b[::-3]) == old[::-3]
    assert isinstance(b[2:5], Buckets)
    with pytest.raises(IndexError):
        b[10]
    assert [x.count for x in b] == [60] * 10
    assert b[3].sum == sum(range(180, 240)) and b[3].min == 180 and b[3].max == 239
    # the columns, one numpy array per field
    assert b.count.dtype == np.uint64 and b.start.dtype == np.int64 and b.sum.dtype == np.float64
    assert np.array_equal(b.start, [x.start for x in old])
    assert np.array_equal(b.avg, [x.sum / x.count for x in old])
    assert b.count.sum() == 600 and b.max.max() == 599
    empty = db.query_agg(s, T0 - 10, T0 - 5, 1)
    assert len(empty) == 0 and empty == [] and list(empty.count) == []
    assert "Bucket" in repr(b)
    # integers: exact columns
    i = db.series_create("i", "int64")
    db.insert_series_values(i, [T0, T0 + 1], [2**61 + 1, 2**61])
    ib = db.query_agg(i)
    assert ib.isum.dtype == np.int64 and ib.isum[0] == 2**62 + 1 and ib[0].isum == 2**62 + 1 and ib.imax[0] == 2**61 + 1


def test_batches_from_many_threads(db):
    ids, _, _ = fill(db)
    series, start, end = windows(ids)
    want = [(list(t), list(v)) for t, v in db.query_raw_many(series, start, end)]
    errors = []

    def run():
        try:
            for _ in range(20):
                got = db.query_raw_many(series, start, end)
                assert [(list(t), list(v)) for t, v in got] == want
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    th = [threading.Thread(target=run) for _ in range(8)]
    for t in th:
        t.start()
    for t in th:
        t.join()
    assert not errors


def test_module_exports():
    assert tsdb_client.Pool is Pool and tsdb_client.BucketsMany is BucketsMany
