# SPDX-License-Identifier: AGPL-3.0-only
"""Batch calls (query_raw_many, query_agg_many, insert_many) on Db, and Buckets (the Pool's are tsdb_client's test_batch.py).

A batch must answer exactly what the one-query calls answer, query by query, in query order; an
error in one query fails the call with that query's error.
"""

import collections.abc
import os
import socket
import subprocess
import sys
import threading
import time

import numpy as np
import pytest

import tsdb_client
from conftest import Server
import tsdb_embedded
from tsdb_client import TsdbError
from tsdb_embedded import Bucket, Buckets, BucketsMany, Db

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
    arr = c.query_raw_many(np.array(series, dtype=np.uint64), T0 + 5000, None, order="desc", limit=3)
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
        assert np.array_equal(many.buckets.counts[off[i] : off[i + 1]], one.counts)
    assert many[-1] == many[len(series) - 1]
    with pytest.raises(IndexError):
        many[len(series)]
    # one interval for every query, the default (0) and numpy arrays
    hourly = c.query_agg_many(np.array(series, dtype=np.uint64), np.array(start), np.array(end), 3_600_000)
    assert [list(b) for b in hourly] == [list(c.query_agg(s, a, b, 3_600_000)) for s, a, b in zip(series, start, end)]
    assert int(c.query_agg_many(series).buckets.counts.sum()) == N * len(series)


def check_errors(c, ids, i64):
    series, start, end = windows(ids)
    bad = list(series)
    bad[5] = i64  # a raw float32 query of an int64 series
    with pytest.raises(TsdbError) as e:
        c.query_raw_many(bad, start, end)
    with pytest.raises(TsdbError) as one:
        c.query_raw(i64, start[5], end[5])
    assert e.value.code == one.value.code and e.value.index == 5
    # the batch after a failed one answers as before
    assert len(c.query_raw_many(series, start, end)) == len(series)
    with pytest.raises(TsdbError) as e:
        c.query_agg_many(series, start, end, -1)  # a negative bucket length
    assert (e.value.index, e.value.code) == (0, tsdb_client.ERR_INVALID)
    assert one.value.index is None  # not a batch call
    # two failing queries, different errors: always the lower one, with its index
    far = c.series("far")  # two readings 10,000 s apart: in 1 ms buckets, more than the 4 Mi allowed
    c.insert_series(far, [T0, T0 + 10_000_000], [1.0, 2.0])
    two, intervals = list(series), [60_000] * len(series)
    two[3], intervals[3] = far, 1
    intervals[7] = -1  # invalid
    for _ in range(20):
        with pytest.raises(TsdbError) as e:
            c.query_agg_many(two, None, None, intervals)
        assert (e.value.index, e.value.code) == (3, tsdb_client.ERR_TOO_MANY_BUCKETS), str(e.value)
        assert str(e.value).startswith("batch entry 3: ")


def check_insert_many(c, ids, f64, i64):
    ts = np.arange(5, dtype=np.int64) + T0 + 10_000_000
    vals = np.ma.masked_array(np.arange(5, dtype=np.float64), mask=[0, 1, 0, 0, 0])
    c.insert_many(
        [
            (ids[0], ts, np.full(5, 7.0, dtype=np.float32)),
            (np.array([ids[1], ids[2]] * 2 + [ids[1]], dtype=np.uint64), ts, [1.0, 2.0, 3.0, 4.0, 5.0]),
            (f64, ts, vals),  # float64 from the dtype, a null where masked
            (i64, list(ts), np.arange(5, dtype=np.int64) - 2),
        ]
    )
    end = int(ts[-1]) + 1
    assert c.query_raw(ids[0], int(ts[0]), end, numpy=False)[1] == [7.0] * 5
    # without value_type each batch takes its series' type, as insert_series_values() does:
    # float64 into a float32 series, lists into float64 and int64 series
    later = ts + 100
    c.insert_many([(ids[3], later, np.array([0.5, 1.5, 2.5, 3.5, 4.5])), (f64, later, [0.25] * 5), (i64, later, [7] * 5)])
    c.insert_series_values(ids[4], later, np.array([0.5, 1.5, 2.5, 3.5, 4.5]))
    lo, hi = int(later[0]), int(later[-1]) + 1
    assert c.query_raw(ids[3], lo, hi, numpy=False) == c.query_raw(ids[4], lo, hi, numpy=False)
    assert c.query_raw(ids[3], lo, hi, numpy=False)[1] == [0.5, 1.5, 2.5, 3.5, 4.5]
    assert c.query_values(f64, lo, hi, numpy=False)[1] == [0.25] * 5
    assert c.query_values(i64, lo, hi, numpy=False)[1] == [7] * 5
    assert c.query_raw(ids[1], int(ts[0]), end, numpy=False) == ([int(ts[i]) for i in (0, 2, 4)], [1.0, 3.0, 5.0])
    assert c.query_raw(ids[2], int(ts[0]), end, numpy=False) == ([int(ts[i]) for i in (1, 3)], [2.0, 4.0])
    assert c.query_values(f64, int(ts[0]), end, numpy=False)[1] == [0.0, None, 2.0, 3.0, 4.0]
    assert c.query_values(i64, int(ts[0]), end, numpy=False)[1] == [-2, -1, 0, 1, 2]
    # the lowest failing insert is named; the one below it was committed
    bad = [(ids[0], ts + 1, np.zeros(5, dtype=np.float32)), (99_999, ts, np.zeros(5, dtype=np.float32))]
    with pytest.raises(TsdbError) as e:
        c.insert_many(bad, value_type="float32")
    with pytest.raises(TsdbError) as single:
        c.insert_series(99_999, ts, np.zeros(5, dtype=np.float32))
    assert (e.value.index, e.value.code) == (1, single.value.code) and "batch entry 1" in str(e.value)
    assert c.query_raw(ids[0], int(ts[0]) + 1, end + 1, numpy=False)[1] == [0.0] * 5
    # without value_type the missing series is found looking up types, before anything is inserted
    bad[0] = (ids[0], ts + 1, np.ones(5, dtype=np.float32))
    with pytest.raises(TsdbError) as e:
        c.insert_many(bad)
    assert (e.value.index, e.value.code) == (1, tsdb_client.ERR_NOTFOUND)
    assert c.query_raw(ids[0], int(ts[0]) + 1, end + 1, numpy=False)[1] == [0.0] * 5
    with pytest.raises(TypeError) as t:
        c.insert_many([(ids[0], ts, np.zeros(5, dtype=np.float32)), (ids[0], ts)])
    assert t.value.index == 1 and "batch entry 1" in str(t.value)
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
    assert b.counts.dtype == np.uint64 and b.start.dtype == np.int64 and b.sum.dtype == np.float64
    assert np.array_equal(b.start, [x.start for x in old])
    assert np.array_equal(b.avg, [x.sum / x.count for x in old])
    assert b.counts.sum() == 600 and b.max.max() == 599
    empty = db.query_agg(s, T0 - 10, T0 - 5, 1)
    assert len(empty) == 0 and empty == [] and list(empty.counts) == []
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




def check_list_order(c):
    """Batches writing the same row: the later one in the list wins, every time."""
    s, t = c.series("order"), c.series("order2")
    for k in range(300):
        ts = np.array([T0 + k], dtype=np.int64)
        # the column of ids ties series t's chain to s's
        c.insert_many(
            [
                (s, ts, np.array([1.0], dtype=np.float32)),
                (t, ts, np.array([5.0], dtype=np.float32)),
                (np.array([t, s], dtype=np.uint64), np.array([T0 + k] * 2), np.array([6.0, 3.0], dtype=np.float32)),
                (s, ts, np.array([2.0], dtype=np.float32)),
            ]
        )
    assert c.query_raw(s, numpy=False)[1] == [2.0] * 300
    assert c.query_raw(t, numpy=False)[1] == [6.0] * 300


def test_db_insert_many_keeps_list_order(db):
    check_list_order(db)




def test_db_last_query_stats_after_a_batch(db):
    ids, _, _ = fill(db)
    many = db.query_raw_many(ids)
    st = db.last_query_stats()
    assert st.result_rows == sum(len(t) for t, _ in many) == N * len(ids)
    agg = db.query_agg_many(ids, None, None, 60_000)
    assert db.last_query_stats().result_rows == len(agg.buckets)


def test_buckets_are_sequences(db):
    s = db.series("x")
    db.insert_series(s, [T0 + i * 1000 for i in range(120)], [float(i) for i in range(120)])
    b = db.query_agg(s, T0, T0 + 120_000, 60_000)
    old = list(b)
    assert isinstance(b, collections.abc.Sequence) and issubclass(Buckets, collections.abc.Sequence)
    assert b + old == old + old and old + b == old + old and b + b == old + old and isinstance(b + old, list)
    with pytest.raises(TypeError):
        b + (1,)
    assert b.count(old[0]) == 1 and b.count(5) == 0 and b.index(old[1]) == 1 and old[1] in b
    with pytest.raises(ValueError):
        b.index(5)
    assert b != tuple(old) and list(reversed(b)) == old[::-1]
    m = db.query_agg_many([s, s], T0, T0 + 120_000, 60_000)
    assert isinstance(m, collections.abc.Sequence) and issubclass(BucketsMany, collections.abc.Sequence)
    assert m == [old, old] and m == m and m != [old] and isinstance(m[0:1], list) and m[0:1] == [b]
    assert m[::-1] == [b, b] and m.count(b) == 2 and m.index(b) == 0 and list(m) == [b, b]
    assert np.array_equal(m.buckets.counts, [60] * 4)




def long_batch(c, ids):
    """Arguments of a raw batch that takes at least 50 ms."""
    n = 64
    while True:
        series = [ids[i % len(ids)] for i in range(n)]
        t = time.perf_counter()
        c.query_raw_many(series)
        if time.perf_counter() - t > 0.05:
            return series
        n *= 2


def check_gil_released(c, ids):
    """Another Python thread keeps running during a long batch call: the GIL is released."""
    series = long_batch(c, ids)
    count, stop = [0], [False]

    def spin():
        while not stop[0]:
            count[0] += 1

    th = threading.Thread(target=spin)
    th.start()
    try:
        time.sleep(0.01)
        before = count[0]
        t = time.perf_counter()
        c.query_raw_many(series)
        took, ran = time.perf_counter() - t, count[0] - before
    finally:
        stop[0] = True
        th.join()
    # Held for the whole call, the GIL would let the other thread run only before it started
    assert took > 0.04 and ran > 10_000, (took, ran)


def test_db_batch_releases_the_gil(db):
    ids, _, _ = fill(db)
    check_gil_released(db, ids)




def test_results_are_not_copied(db):
    """numpy takes the result vectors over: the arrays do not own a copy of their data."""
    ids, _, _ = fill(db)
    for ts, v in db.query_raw_many(ids[:2]) + [db.query_raw(ids[0], numpy=True)]:
        for a in (ts, v):
            assert not a.flags.owndata and a.base is not None
            assert not isinstance(a.base, (bytes, bytearray, np.ndarray)), type(a.base)


NO_NUMPY = """
import sys, tempfile
sys.modules["numpy"] = None
import tsdb_embedded as t
with t.Db(tempfile.mkdtemp(dir=sys.argv[1]), fsync=False) as db:
    s = db.series("x")
    db.insert_series(s, [1000, 2000], [1.0, 2.0])
    b = db.query_agg(s, None, None, 1000)
    assert list(b.counts) == [1, 1] and isinstance(b.counts, list)
    m = db.query_agg_many([s, s], None, None, 1000)
    assert m.offsets == [0, 2, 4] and m.buckets.counts == [1] * 4 and m[1] == list(b)
    try:
        db.query_raw_many([s])
        raise SystemExit("query_raw_many without numpy did not raise")
    except ImportError as e:
        assert "numpy" in str(e)
print("ok")
"""


def test_batches_without_numpy(tmp_path):
    p = subprocess.run([sys.executable, "-c", NO_NUMPY, str(tmp_path)], capture_output=True, text=True, timeout=60)
    assert p.returncode == 0 and p.stdout.strip() == "ok", p.stderr
