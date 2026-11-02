"""Protocol 9 queries against a real tsdbd: the spread, time-weighted averages, LTTB, and
aggregates with order and limit. test_embedded.py runs the same tests on a Db, and compares
its answers with tsdbd's."""

import math

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError


T0 = 1_699_999_980_000


def code(fn, *args, **kw):
    with pytest.raises(TsdbError) as e:
        fn(*args, **kw)
    return e.value.code


def same_floats(a, b):
    """Equal lists of floats, NaN equal to NaN; the others to within 1e-12."""
    return len(a) == len(b) and all(
        (math.isnan(x) and math.isnan(y)) or (not math.isnan(x) and not math.isnan(y) and x == pytest.approx(y, abs=1e-12))
        for x, y in zip(a, b)
    )


def calc(client):
    s = client.series_create("calc", "float64")
    client.insert_series_values(s, [T0, T0 + 1000, T0 + 3000, T0 + 4000], [10.0, 20.0, 30.0, 40.0])
    return s


def test_aggregates_with_order_and_limit(client):
    s = calc(client)
    sec = client.query_agg(s, None, None, 1000, order="desc", limit=2)
    assert [(b.start, b.sum) for b in sec] == [(T0 + 4000, 40.0), (T0 + 3000, 30.0)]
    assert client.query_agg(s, None, None, 1000, limit=0) == []
    assert client.query_agg(s, order="asc") == client.query_agg(s)
    assert code(client.query_agg, s, None, None, -1) == tsdb_client.ERR_INVALID
    with pytest.raises(ValueError):
        client.query_agg(s, order="up")


def test_spread(client):
    s = calc(client)
    (b,) = client.query_spread(s)
    assert (b.count, b.sum, b.range, b.var_pop, b.stddev_pop) == (4, 100.0, 30.0, 125.0, math.sqrt(125))
    assert b.var_samp == pytest.approx(500 / 3, abs=1e-9)
    assert b.stddev_samp == pytest.approx(math.sqrt(500 / 3), abs=1e-9)
    assert "var_pop=125.0" in repr(b)

    per_2s = client.query_spread(s, interval_ms=2000, order="desc")
    assert list(per_2s.start) == [T0 + 4000, T0 + 2000, T0]
    assert math.isnan(per_2s[0].var_samp)  # one value has no sample variance
    assert list(per_2s.range) == [0.0, 0.0, 10.0]
    assert list(per_2s.var_pop) == [0.0, 0.0, 25.0]
    # every spread column is its buckets' field, and the right one: NaN where one value has no sample spread
    cols = ("range", "var_pop", "var_samp", "stddev_pop", "stddev_samp")
    known = {
        "range": [0.0, 0.0, 10.0],
        "var_pop": [0.0, 0.0, 25.0],
        "var_samp": [math.nan, math.nan, 50.0],
        "stddev_pop": [0.0, 0.0, 5.0],
        "stddev_samp": [math.nan, math.nan, math.sqrt(50)],
    }
    for col in cols:
        column = list(getattr(per_2s, col))
        assert same_floats(column, [getattr(b, col) for b in per_2s]), col
        assert same_floats(column, known[col]), col
        whole = list(getattr(client.query_spread(s), col))
        assert same_floats(whole, [getattr(client.query_spread(s)[0], col)]), col
    g = client.query_spread_group(s, unit="second", count=2, order="desc")
    assert g == per_2s  # bit for bit, NaN included
    assert per_2s[1:] == client.query_spread(s, interval_ms=2000, order="desc", limit=3)[1:]
    (day,) = client.query_spread_group(s, unit="day", timezone="Europe/Oslo")
    assert day.var_pop == 125.0
    b = client.query_spread_bounds(s, [T0, T0 + 3000], limit=1)
    assert [(x.count, x.range, x.var_pop) for x in b] == [(2, 10.0, 25.0)]
    assert client.query_spread(s, limit=0) == []
    assert code(client.query_spread_bounds, s, []) == tsdb_client.ERR_INVALID

    # the plain queries have no spread columns
    plain = client.query_agg(s)
    assert not hasattr(plain, "var_pop") and not hasattr(plain[0], "range")
    with pytest.raises(AttributeError, match="query_spread"):
        plain.stddev_samp
    assert plain != client.query_spread(s)


def test_time_weighted_averages(client):
    s = calc(client)
    # 10 for 1 s, 20 for 2 s, 30 for 1 s; 40 has no segment
    (step,) = client.query_twa(s, T0, T0 + 5000)
    assert (step.twa, step.area, step.covered_ms, step.count) == (20.0, 80_000.0, 4000, 4)
    (lin,) = client.query_twa(s, T0, T0 + 5000, method="linear")
    assert (lin.area, lin.covered_ms) == (15_000.0 + 50_000.0 + 35_000.0, 4000)
    (gap,) = client.query_twa(s, T0, T0 + 5000, max_gap_ms=1500)
    assert (gap.area, gap.covered_ms) == (40_000.0, 2000)

    secs = client.query_twa(s, T0, T0 + 5000, 1000, order="desc", limit=2)
    assert len(secs) == 2 and list(secs.start) == [T0 + 4000, T0 + 3000]
    # the last reading holds nothing (no later reading ends its segment) but is counted
    assert (secs[0].count, secs[0].covered_ms) == (1, 0) and math.isnan(secs[0].twa)
    assert secs[1].twa == 30.0
    assert list(secs.counts) == [1, 1] and list(secs.covered_ms) == [0, 1000]
    assert list(secs.area)[1] == 30_000.0 and math.isnan(list(secs.twa)[0])

    every = client.query_twa(s, T0, T0 + 5000, 1000)
    assert client.query_twa_group(s, T0, T0 + 5000, unit="second") == every
    assert client.query_twa_group(s, T0, T0 + 5000) == client.query_twa(s, T0, T0 + 5000)
    local = client.query_twa_group(s, T0, T0 + 5000, unit="day", timezone="Europe/Oslo")
    assert sum(local.area) == 80_000.0
    b = client.query_twa_bounds(s, [T0 + 500, T0 + 2000], T0, T0 + 5000)
    assert [(x.start, x.area) for x in b] == [(T0 + 500, 25_000.0), (T0 + 2000, 50_000.0)]
    assert b[::-1] == list(reversed(list(b))) and b.index(b[1]) == 1 and b.count(b[0]) == 1
    assert "TwaBucket(start=" in repr(b)

    assert client.query_twa(s, T0, T0 + 5000, limit=0) == []
    assert code(client.query_twa, s, T0, T0 + 5000, -1) == tsdb_client.ERR_INVALID
    st = client.series_create("calc.state", "state")
    assert code(client.query_twa, st) == tsdb_client.ERR_INVALID
    with pytest.raises(ValueError):
        client.query_twa(s, method="cubic")


def test_lttb(client):
    wave = client.series("wave")
    ts = [T0 + i * 1000 for i in range(1000)]
    client.insert_series(wave, ts, [math.sin(i / 30) * 100 for i in range(1000)])
    t, v = client.query_lttb(wave, 50, numpy=False)
    assert len(t) == len(v) == 50 and (t[0], t[-1]) == (T0, T0 + 999_000)
    dt, dv = client.query_lttb(wave, 50, order="desc", numpy=False)
    assert (dt, dv) == (t[::-1], v[::-1])
    assert client.query_lttb(wave, 50, order="desc", limit=3, numpy=False) == (dt[:3], dv[:3])
    assert client.query_lttb(wave, 50, limit=3, numpy=False) == (t[:3], v[:3])
    assert client.query_lttb(wave, 50, limit=0, numpy=False) == ([], [])
    few, _ = client.query_lttb(wave, 50, T0, T0 + 10_000, numpy=False)
    assert len(few) == 10  # fewer rows than the threshold: all of them
    nt, nv = client.query_lttb(wave, 50)
    assert list(nt) == t and str(nv.dtype) == "float32"
    assert code(client.query_lttb, wave, 2) == tsdb_client.ERR_INVALID
    st = client.series_create("wave.state", "state")
    assert code(client.query_lttb, st, 10) == tsdb_client.ERR_INVALID


def test_over_wide_local_ranges_are_too_many_buckets(client):
    """More local periods than one request carries (32 Mi bounds; QUERY_TWA 56 bytes' worth fewer; the calendar stops
    at 64 Mi): TOO_MANY_BUCKETS from every grouped call, as in the C, Rust and Java clients."""
    secs = dict(unit="second", timezone="Europe/Oslo")
    for name, days in (("wide", 450), ("wider", 800)):  # 38.9 M and 69.1 M seconds
        s = client.series_create(name, "float64")
        client.insert_series_values(s, [T0, T0 + days * 86_400_000], [1.0, 2.0])
        assert code(client.query_group, s, **secs) == tsdb_client.ERR_TOO_MANY_BUCKETS
        assert code(client.query_spread_group, s, **secs) == tsdb_client.ERR_TOO_MANY_BUCKETS
        assert code(client.query_twa_group, s, **secs) == tsdb_client.ERR_TOO_MANY_BUCKETS
    # bounded on both sides, TWA covers the range: 33,554,418 seconds is one past QUERY_TWA's cap
    s = client.series("wide")
    assert code(client.query_twa_group, s, T0, T0 + 33_554_418_000, **secs) == tsdb_client.ERR_TOO_MANY_BUCKETS
