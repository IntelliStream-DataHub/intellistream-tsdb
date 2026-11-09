# SPDX-License-Identifier: AGPL-3.0-only
"""Multi-series calls (query_*_multi, protocol version 10) on Db (the tests of Client and Pool are tsdb_client's test_multi.py).

A multi-series call must answer, series by series and in the order asked, exactly what the
single-series call answers for that series; a series that fails has its own status and does not
fail the others (errors="status"), or raises with its index and the whole answer (errors="raise").
"""

import numpy as np
import pytest

from conftest import Server, plain
import tsdb_client
from tsdb_client import Client, TsdbError
from tsdb_embedded import Buckets, Db, Many, TwaBuckets

T0 = 1_700_000_000_000 - 1_700_000_000_000 % 86_400_000  # a UTC midnight
N = 3000  # rows per series, 1 min apart: a little over two days
UNKNOWN = 1 << 32 | 999_999  # well formed, of a volume the server does not have
TRUNCATED = 999_999  # no volume field: INVALID


def fill(c):
    """Three float32 series, a float64 one with nulls, an int64, a bool and a state series."""
    ts = T0 + np.arange(N, dtype=np.int64) * 60_000
    f32 = [c.series(f"m{i}") for i in range(3)]
    for k, s in enumerate(f32):
        c.insert_series(s, ts[k * 100 :], (np.sin(np.arange(N - k * 100) / (7 + k)) * 50).astype(np.float32))
    f64 = c.series_create("m.f64", "float64")
    c.insert_series_values(f64, ts[:500], [None if i % 7 == 0 else i / 3 for i in range(500)])
    i64 = c.series_create("m.i64", "int64")
    c.insert_series_values(i64, ts, np.arange(N, dtype=np.int64) * 1_000_003)
    b = c.series_create("m.bool", "bool")
    c.insert_series_values(b, ts[::3], [i % 5 == 0 for i in range(len(ts[::3]))])
    st = c.series_create("m.state", "state")
    codes = [c.state(st, n) for n in ("off", "idle", "run")]
    c.insert_series_values(st, ts[::2], np.array([codes[i % 3] for i in range(len(ts[::2]))], dtype=np.uint32))
    c.flush()
    return f32, [f64, i64, b, st]


def same_rows(a, b):
    """(timestamps, values) equal, nulls included (masked arrays: mask and the known values)."""
    (ta, va), (tb, vb) = a, b
    if not np.array_equal(np.asarray(ta), np.asarray(tb)):
        return False
    ma, mb = np.ma.getmaskarray(va), np.ma.getmaskarray(vb)
    return np.array_equal(ma, mb) and np.array_equal(np.ma.getdata(va)[~ma], np.ma.getdata(vb)[~mb])


@pytest.fixture(params=["db"])
def target(request, server, tmp_path):
    """Each of the three, filled the same way, with what answers the single calls (a Pool has none:
    a Client of its server)."""
    if request.param == "db":
        with Db(tmp_path / "db", fsync=False, merge_threads=0) as d:
            yield d, d, fill(d)
        return
    with Client("127.0.0.1", server.port, timeout=30) as c:
        ids = fill(c)
        if request.param == "client":
            yield c, c, ids
        else:
            with Pool("127.0.0.1", server.port, 2, timeout=30) as p:
                yield p, c, ids


def test_values_and_lttb(target):
    c, one, (f32, other) = target
    series = f32 + other + f32[:1]  # a duplicate
    for kw in ({}, {"order": "desc"}, {"limit": 7}, {"order": "desc", "limit": 0}):
        r = c.query_values_multi(series, T0 + 50 * 60_000, T0 + 2500 * 60_000, **kw)
        assert isinstance(r, Many) and r.kind == "values" and len(r) == len(series) and r.ok
        assert list(r.series) == series and list(r.status) == [0] * len(series)
        for i, s in enumerate(series):
            want = one.query_values(s, T0 + 50 * 60_000, T0 + 2500 * 60_000, **kw)
            assert same_rows(r[i], want), (i, kw)
            a, b = r.offsets[i], r.offsets[i + 1]
            assert np.array_equal(r.ts[a:b], want[0])
        if kw.get("limit") != 0:
            with pytest.raises(TypeError):
                r.values  # the series have different types
    # one type: one values column
    r = c.query_values_multi(np.array(f32, dtype=np.uint64))
    assert r.values.dtype == np.float32 and len(r.values) == len(r.ts) == r.offsets[-1]
    assert np.array_equal(r.values[r.offsets[1] : r.offsets[2]], one.query_values(f32[1])[1])
    # per-series ranges
    starts = np.array([T0 + i * 3_600_000 for i in range(len(series))], dtype=np.int64)
    ends = [int(x) + 7_200_000 for x in starts]
    r = c.query_values_multi(series, starts, ends)
    for i, s in enumerate(series):
        assert same_rows(r[i], one.query_values(s, int(starts[i]), ends[i]))
    series = series[:6] + series[7:]  # LTTB takes no state series
    r = c.query_lttb_multi(series, 50, None, T0 + 2000 * 60_000, order="desc", limit=20)
    assert r.kind == "lttb"
    for i, s in enumerate(series):
        assert same_rows(r[i], one.query_lttb(s, 50, None, T0 + 2000 * 60_000, order="desc", limit=20))
    assert [len(x[0]) for x in r[1:3]] == [20, 20]  # a slice: a list


def test_buckets_spread_twa(target):
    c, one, (f32, other) = target
    series = f32 + other[:2]  # the numeric ones: TWA takes no bool or state series
    lo, hi = T0 + 17 * 60_000, T0 + 2900 * 60_000
    bounds = [T0 + k * 5_000_000 for k in range(30)]
    cases = [
        ({}, lambda s: one.query_agg(s, lo, hi, 0), lambda s: one.query_spread(s, lo, hi, 0),
         lambda s: one.query_twa(s, lo, hi, 0)),
        ({"interval_ms": 3_600_000}, lambda s: one.query_agg(s, lo, hi, 3_600_000),
         lambda s: one.query_spread(s, lo, hi, 3_600_000), lambda s: one.query_twa(s, lo, hi, 3_600_000)),
        ({"unit": "day", "timezone": "Europe/Oslo"},
         lambda s: one.query_group(s, lo, hi, unit="day", timezone="Europe/Oslo"),
         lambda s: one.query_spread_group(s, lo, hi, unit="day", timezone="Europe/Oslo"),
         lambda s: one.query_twa_group(s, lo, hi, unit="day", timezone="Europe/Oslo")),
        ({"unit": "minute", "count": 15}, lambda s: one.query_group(s, lo, hi, unit="minute", count=15),
         lambda s: one.query_spread_group(s, lo, hi, unit="minute", count=15),
         lambda s: one.query_twa_group(s, lo, hi, unit="minute", count=15)),
        ({"bounds": np.array(bounds, dtype=np.int64)}, lambda s: one.query_bounds(s, bounds, lo, hi),
         lambda s: one.query_spread_bounds(s, bounds, lo, hi),
         lambda s: one.query_twa_bounds(s, bounds, lo, hi)),
    ]
    for kw, agg, spread, twa in cases:
        r = c.query_buckets_multi(series, lo, hi, **kw)
        assert r.kind == "buckets" and isinstance(r.buckets, Buckets)
        for i, s in enumerate(series):
            assert r[i] == agg(s), (kw, i)
        assert np.array_equal(r.counts, np.concatenate([agg(s).counts for s in series]))
        r = c.query_spread_multi(series, lo, hi, **kw)
        for i, s in enumerate(series):
            assert r[i] == spread(s), (kw, i)
        assert len(r.stddev_pop) == r.offsets[-1]
        r = c.query_twa_multi(series, lo, hi, **kw)
        assert isinstance(r.buckets, TwaBuckets)
        for i, s in enumerate(series):
            assert r[i] == twa(s), (kw, i)
    # per-series intervals, order, limit, the linear method
    iv = [60_000 * (i + 1) for i in range(len(series))]
    r = c.query_buckets_multi(series, lo, hi, iv, order="desc", limit=5)
    for i, s in enumerate(series):
        assert r[i] == one.query_agg(s, lo, hi, iv[i], order="desc", limit=5)
    r = c.query_twa_multi(series, lo, hi, 3_600_000, method="linear", max_gap_ms=600_000)
    for i, s in enumerate(series):
        assert r[i] == one.query_twa(s, lo, hi, 3_600_000, method="linear", max_gap_ms=600_000)


def test_errors(target):
    c, one, (f32, other) = target
    series = [f32[0], other[3], f32[1]]  # TWA fails on the state series alone
    r = c.query_twa_multi(series, interval_ms=3_600_000, errors="status")
    assert not r.ok and r.status[0] == 0 and r.status[1] == -4 and r.status[2] == 0
    assert r[0] == one.query_twa(f32[0], None, None, 3_600_000)
    with pytest.raises(TsdbError) as e:
        r[1]
    assert e.value.index == 1 and e.value.code == r.status[1] and e.value.result is r
    assert isinstance(r.error_of(1), TsdbError) and r.error_of(0) is None
    assert r.offsets[2] == r.offsets[1]  # no buckets for the failed series
    with pytest.raises(TsdbError) as e:
        c.query_buckets_multi(series, interval_ms=[3_600_000, -1, 60_000])  # a negative interval
    assert e.value.index == 1 and isinstance(e.value.result, Many) and len(e.value.result) == 3
    assert e.value.result[2] == one.query_agg(f32[1], None, None, 60_000)
    # an unknown series reads as empty, as the single call does
    r = c.query_values_multi([UNKNOWN, f32[0]])
    assert r.ok and r.offsets[1] == 0 and same_rows(r[0], one.query_values(UNKNOWN))
    # a truncated id is INVALID in a multi query, as in the single one
    r = c.query_values_multi([f32[0], TRUNCATED], errors="status")
    assert r.status[0] == 0 and r.status[1] == tsdb_client.ERR_INVALID
    with pytest.raises(TsdbError) as e:
        one.query_values(TRUNCATED)
    assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(ValueError):
        c.query_values_multi(series, errors="ignore")
    with pytest.raises(ValueError):
        c.query_buckets_multi(series, interval_ms=60_000, unit="hour")
    with pytest.raises(ValueError):
        c.query_values_multi(series, [T0, T0])  # one start per series, or one for all
    # a whole-call limit fails the call, not a series
    with pytest.raises(TsdbError) as e:
        c.query_values_multi(f32, max_rows=10)
    assert e.value.result is None
    assert c.query_values_multi([]).offsets[-1] == 0 and len(c.query_values_multi([])) == 0


def test_lists_without_numpy(target):
    c, one, (f32, _) = target
    r = c.query_values_multi(f32, None, T0 + 10 * 60_000, numpy=False)
    assert isinstance(r.ts, np.ndarray) is False and isinstance(r[0][1], list)
    assert r[0] == tuple(one.query_values(f32[0], None, T0 + 10 * 60_000, numpy=False))




def test_embedded_matches_server(tmp_path):
    """Db and tsdbd over the same directory give the same multi answers."""
    path = tmp_path / "db"
    with Db(path, fsync=False, merge_threads=0) as d:
        f32, other = fill(d)
        series = f32 + other[:2]
        emb = (d.query_values_multi(series), d.query_buckets_multi(series, unit="hour", timezone="America/New_York"),
               d.query_twa_multi(series, interval_ms=7_200_000))
    srv = Server(tmp_path, dir=path)
    try:
        with Client("127.0.0.1", srv.port, timeout=30) as c:
            tcp = (c.query_values_multi(series), c.query_buckets_multi(series, unit="hour", timezone="America/New_York"),
                   c.query_twa_multi(series, interval_ms=7_200_000))
    finally:
        srv.stop()
    for i in range(len(series)):
        assert same_rows(emb[0][i], tcp[0][i])
        assert plain(emb[1][i]) == plain(tcp[1][i])
        assert plain(emb[2][i]) == plain(tcp[2][i])
    assert np.array_equal(emb[0].offsets, tcp[0].offsets)
