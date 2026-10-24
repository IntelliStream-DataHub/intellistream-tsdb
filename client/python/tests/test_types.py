"""Value types and nulls (protocol version 2) against a real tsdbd."""

import array

import pytest

import tsdb_client
from tsdb_client import TsdbError

T0 = 1_699_999_980_000
CHUNK = 16384  # rows per QUERY_VALUES chunk on the server
I64_MAX = 2**63 - 1

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

needs_numpy = pytest.mark.skipif(np is None, reason="numpy is not installed")


def invalid(fn, *args, **kw):
    with pytest.raises(TsdbError) as e:
        fn(*args, **kw)
    return e.value.code == tsdb_client.ERR_INVALID


def test_series_types(server, client):
    assert tsdb_client.VALUE_TYPES == ["float32", "float64", "int64", "bool", "state"]
    for t in tsdb_client.VALUE_TYPES:
        sid = client.series_create(f"t.{t}", t)
        assert client.series_type(sid) == t
        assert client.series_create(f"t.{t}", t) == sid
    assert invalid(client.series_create, "t.float64", "int64")
    assert client.series_type(client.series("plain")) == "float32"
    with pytest.raises(TsdbError) as e:
        client.series_type(999_999)
    assert e.value.code == tsdb_client.ERR_NOTFOUND
    with pytest.raises(ValueError):
        client.series_create("x", "decimal")
    v = client.series_create("on.volume", "int64", str(server.dir) + ".volume")
    client.insert_series_values(v, [T0], [42])
    assert client.query_values(v, numpy=False) == ([T0], [42])


def test_int64_nulls_order_limit_and_groups(client):
    sid = client.series_create("i", "int64")
    ts = [T0 + k * 1000 for k in range(12)]
    vals = [None if k in (3, 9) else k * 1_000_000_000_000 - 5 for k in range(12)]
    client.insert_series_values(sid, ts, vals)
    assert client.query_values(sid, numpy=False) == (ts, vals)
    assert client.query_values(sid, order="desc", limit=3, numpy=False) == (ts[:8:-1], vals[:8:-1])
    assert client.query_values(sid, limit=0, numpy=False) == ([], [])

    known = [v for v in vals if v is not None]
    (b,) = client.query_agg(sid)
    assert (b.count, b.isum, b.imin, b.imax) == (10, sum(known), known[0], known[-1])
    (g,) = client.query_group(sid, unit="second", count=4, timezone="UTC", order="desc", limit=1)
    assert (g.start, g.count, g.isum) == (T0 + 8000, 3, vals[8] + vals[10] + vals[11])
    assert "isum=" in repr(g)

    # last write wins between values and nulls
    client.insert_series_values(sid, [ts[0], ts[3]], [None, 7])
    assert client.query_values(sid, end=ts[4], numpy=False)[1] == [None, vals[1], vals[2], 7]


def test_exact_integers(client):
    sid = client.series_create("big", "int64")
    client.insert_series_values(sid, [T0, T0 + 1, T0 + 2], [I64_MAX - 10, -(I64_MAX - 20), 7])
    (b,) = client.query_agg(sid)
    assert (b.count, b.isum, b.imin, b.imax) == (3, 17, -(I64_MAX - 20), I64_MAX - 10)
    sat = client.series_create("sat", "int64")
    client.insert_series_values(sat, [T0, T0 + 1], [I64_MAX, I64_MAX])
    assert client.query_agg(sat)[0].isum == I64_MAX
    # the int64 null representation reads back as null
    client.insert_series_values(sat, [T0 + 2], [-(2**63)])
    assert client.query_values(sat, numpy=False)[1] == [I64_MAX, I64_MAX, None]
    with pytest.raises(OverflowError):
        client.insert_series_values(sat, [T0], [2**63])


def test_bools_floats_and_float32(client):
    b = client.series_create("b", "bool")
    vals = [True, False, None, True, False, True]
    ts = [T0 + k * 1000 for k in range(6)]
    client.insert_series_values(b, ts, vals)
    _, got = client.query_values(b, numpy=False)
    assert got == vals and all(type(x) is bool for x in got if x is not None)
    (bk,) = client.query_agg(b)
    assert (bk.count, bk.isum, bk.imin, bk.imax) == (5, 3, 0, 1)
    with pytest.raises(TypeError):
        client.insert_series_values(b, [T0], [1])  # bools only

    d = client.series_create("d", "float64")
    client.insert_series_values(d, ts[:4], [0.1, 1e300, None, 3])  # ints convert
    assert client.query_values(d, numpy=False)[1] == [0.1, 1e300, None, 3.0]
    (bk,) = client.query_agg(d)
    assert (bk.count, bk.min, bk.max) == (3, 0.1, 1e300)

    f = client.series("f")
    client.insert_series_values(f, ts[:3], [1.5, None, 2.5])
    assert client.query_values(f, numpy=False)[1] == [1.5, None, 2.5]
    raw = client.query_raw(f, numpy=False)[1]  # the version 1 query reads nulls as NaN
    assert raw[0] == 1.5 and raw[1] != raw[1]


def test_states(client):
    s = client.series_create("pump.mode", "state")
    s2 = client.series_create("fan.mode", "state")
    on, off = client.state(s, "on"), client.state(s, "off")
    assert 0 < on != off > 0
    assert client.state(s, "on") == on and client.state_lookup(s, "off") == off
    assert client.state_lookup(s2, "on") == on  # one dictionary per volume
    with pytest.raises(TsdbError) as e:
        client.state_lookup(s, "standby")
    assert e.value.code == tsdb_client.ERR_NOTFOUND
    assert client.state_name(s, on) == "on"
    with pytest.raises(TsdbError) as e:
        client.state_name(s, 999)
    assert e.value.code == tsdb_client.ERR_NOTFOUND

    client.insert_series_values(s, [T0, T0 + 1000, T0 + 2000, T0 + 3000], [on, off, None, on])
    _, codes = client.query_values(s, numpy=False)
    assert codes == [on, off, None, on]
    assert client.state_names(s, codes) == ["on", "off", None, "on"]
    assert client.query_agg(s)[0].count == 3


def test_many_series_and_pipelining(client):
    x, y = client.series_create("x", "bool"), client.series_create("y", "bool")
    # 5 bools and a bitmap byte: the series column is padded
    client.insert_values([x, y, x, y, x], [T0, T0, T0 + 1, T0 + 1, T0 + 2], [True, None, False, True, True])
    assert client.query_values(x, numpy=False)[1] == [True, False, True]
    assert client.query_values(y, numpy=False)[1] == [None, True]
    for n in range(9):
        client.insert_values([x] * n, [T0 + 100 + i for i in range(n)], [True] * n)

    i = client.series_create("pipe", "int64")
    for k in range(2000):
        client.insert_series_values_async(i, [T0], [k])
    client.insert_values_async([i], [T0], [None])
    client.insert_series_values_async(x, [T0 + 5], [1], value_type="int64")  # the series is bool
    client.insert_values_async([i], [T0 + 1], [5])
    with pytest.raises(TsdbError) as e:
        client.wait()
    assert e.value.code == tsdb_client.ERR_INVALID
    assert client.query_values(i, numpy=False)[1] == [None, 5]


def test_type_mismatches(client):
    d = client.series_create("d", "float64")
    i = client.series_create("i", "int64")
    f = client.series("f")
    assert invalid(client.insert_series_values, d, [T0], [1], value_type="int64")
    assert invalid(client.insert_series_values, f, [T0], [1.0], value_type="float64")
    assert invalid(client.insert_values, [d, i], [T0, T0], [1, 2], value_type="int64")
    assert invalid(client.insert_series, d, [T0], [1.0])
    assert invalid(client.query_raw, i)
    with pytest.raises(ValueError):
        client.insert_series_values(i, [T0], [1], value_type="int")
    with pytest.raises(TypeError):
        client.insert_series_values(i, [T0], ["x"])
    with pytest.raises(TsdbError) as e:
        client.insert_series_values(i, [T0, T0 + 1], [1])
    assert e.value.code == tsdb_client.ERR_INVALID
    assert client.query_values(i, numpy=False) == ([], [])
    assert client.query_values(999_999, numpy=False) == ([], [])


def test_buffers_without_numpy(client):
    i = client.series_create("arr", "int64")
    s = client.series_create("st", "state")
    client.insert_series_values(i, array.array("q", [T0, T0 + 1]), array.array("q", [5, -6]))
    client.insert_series_values(s, [T0], array.array("I", [client.state(s, "a")]))
    assert client.query_values(i, numpy=False)[1] == [5, -6]
    assert client.state_names(s, client.query_values(s, numpy=False)[1]) == ["a"]


@needs_numpy
def test_numpy_masked_arrays_and_chunk_boundaries(client):
    n = 3 * CHUNK + 100
    ts = T0 + np.arange(n, dtype=np.int64) * 1000
    nulls = [0, 5, CHUNK - 1, 2 * CHUNK, 2 * CHUNK + 1, 3 * CHUNK - 1, n - 1]
    for t, dtype in [("int64", np.int64), ("float64", np.float64), ("float32", np.float32), ("bool", np.bool_), ("state", np.uint32)]:
        sid = client.series_create(f"np.{t}", t)
        data = (np.arange(n) % 2).astype(dtype) if t in ("bool", "state") else np.arange(n).astype(dtype)
        if t == "state":
            data = data + 1  # codes, never the null code 0
        mask = np.zeros(n, dtype=bool)
        mask[nulls] = True
        client.insert_series_values(sid, ts, np.ma.MaskedArray(data, mask=mask))
        got_ts, got = client.query_values(sid)
        assert got_ts.dtype == np.int64 and np.array_equal(got_ts, ts)
        assert isinstance(got, np.ma.MaskedArray) and got.dtype == dtype, t
        assert np.array_equal(np.ma.getmaskarray(got), mask), t
        assert np.array_equal(got.compressed(), data[~mask]), t
        # newest first, chunked from the other end
        _, desc = client.query_values(sid, order="desc")
        assert np.array_equal(np.ma.getmaskarray(desc)[::-1], mask), t
        # the list form agrees
        assert client.query_values(sid, numpy=False)[1] == [None if m else x for x, m in zip(data.tolist(), mask)]

    # plain arrays: no nulls, an unmasked MaskedArray back; float32 into float64 widens
    d = client.series_create("plain.f64", "float64")
    client.insert_series_values(d, ts[:3], np.array([0.5, 1.5, 2.5], dtype=np.float32))
    _, got = client.query_values(d)
    assert not got.mask.any() and got.tolist() == [0.5, 1.5, 2.5]
    st = client.series_create("np.state.names", "state")
    on = client.state(st, "on")
    client.insert_series_values(st, ts[:2], np.ma.MaskedArray(np.array([on, on], dtype=np.uint32), mask=[False, True]))
    assert client.state_names(st, client.query_values(st)[1]) == ["on", None]
    # many series from numpy arrays
    client.insert_values(np.array([d, d], dtype=np.uint32), ts[3:5], np.array([3.5, 4.5]))
    assert client.query_values(d, numpy=False)[1] == [0.5, 1.5, 2.5, 3.5, 4.5]
