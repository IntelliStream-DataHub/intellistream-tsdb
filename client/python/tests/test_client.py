"""Integration tests against a real tsdbd: every op, over the wire."""

import array
import threading

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError

T0 = 1_699_999_980_000  # on a minute boundary: 2023-11-14 22:13 UTC (23:13 in Oslo)
MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]


def test_series(server, client):
    assert client.protocol_version() == tsdb_client.PROTOCOL_VERSION
    a = client.series("tank.level_bar")
    b = client.series("pump.flow_m3h")
    assert a != b
    assert client.series("tank.level_bar") == a
    assert client.series("tank.level_bar", create=False) == a
    assert client.series_name(b) == "pump.flow_m3h"
    with pytest.raises(TsdbError) as e:
        client.series("missing", create=False)
    assert e.value.code == tsdb_client.ERR_NOTFOUND
    with pytest.raises(TsdbError) as e:
        client.series_name(999_999)
    assert e.value.code == tsdb_client.ERR_NOTFOUND

    vol = str(server.dir) + ".volume"
    with pytest.raises(TsdbError) as e:  # tsdbd never creates a volume for a client
        client.series_at("remote.on_volume", vol)
    assert e.value.code == tsdb_client.ERR_NOTFOUND
    server.add_volume(vol)
    v = client.series_at("remote.on_volume", vol)
    client.insert_series(v, [T0], [3.5])
    assert client.query_raw(v, numpy=False) == ([T0], [3.5])


def test_last_write_wins_and_pipelining(client):
    a, b = client.series("a"), client.series("b")
    client.insert_series(a, [T0, T0 + 1000, T0], [1.0, 2.0, 3.0])  # within a batch, the later row
    assert client.query_raw(a, numpy=False) == ([T0, T0 + 1000], [3.0, 2.0])

    s, t = [b, b, a], [T0, T0 + 1000, T0]
    client.insert_async(s, t, [1.5, 2.5, -7.0])
    client.insert_async(s, t, [1.5, 9.0, -7.0])
    assert client.pending == 2
    client.wait()
    assert client.pending == 0
    assert client.query_raw(b, numpy=False) == ([T0, T0 + 1000], [1.5, 9.0])
    assert client.query_raw(a, numpy=False)[1] == [-7.0, 2.0]

    for i in range(3000):
        client.insert_series_async(a, [T0 + 5000], [float(i)])
    client.insert([a], [T0 + 5000], [-1.0])
    assert client.query_raw(a, T0 + 5000, T0 + 5001, numpy=False) == ([T0 + 5000], [-1.0])


def test_order_limit_and_columns(client):
    a = client.series("big")
    n = 100_000
    ts = array.array("q", (T0 + i * 1000 for i in range(n)))  # the buffer protocol path
    vals = array.array("f", ((i % 1000) / 10 for i in range(n)))
    client.insert_series(a, ts, vals)

    t, v = client.query_raw(a, numpy=False)
    assert isinstance(t, list) and isinstance(v, list)
    assert t == list(ts) and v == list(vals)

    t, v = client.query_raw(a, order="desc", limit=5, numpy=False)
    assert t == [ts[n - 1 - i] for i in range(5)]
    assert v == [vals[n - 1 - i] for i in range(5)]
    assert client.query_points(a, T0 + 5000, order="asc", limit=3, numpy=False)[0] == [T0 + 5000, T0 + 6000, T0 + 7000]
    assert client.query_raw(a, limit=0, numpy=False) == ([], [])
    assert len(client.query_raw(a, T0 + 5000, T0 + 15000, numpy=False)[0]) == 10
    assert client.query_raw(1 << 32 | 999_999, numpy=False) == ([], [])
    with pytest.raises(ValueError):
        client.query_raw(a, order="sideways")


@pytest.mark.skipif(np is None, reason="numpy is not installed")
def test_numpy(client):
    a = client.series("np")
    ts = T0 + np.arange(50_000, dtype=np.int64) * 1000
    vals = np.linspace(-1, 1, 50_000, dtype=np.float64)  # converted to float32
    client.insert_series(a, ts, vals)
    t, v = client.query_raw(a)
    assert t.dtype == np.int64 and v.dtype == np.float32
    assert np.array_equal(t, ts) and np.array_equal(v, vals.astype(np.float32))
    t[0] = 0  # writable
    t, v = client.query_raw(a, order="desc", limit=2, numpy=True)
    assert list(t) == [ts[-1], ts[-2]]
    client.insert(np.array([a, a], dtype=np.uint64), np.array([T0, T0 + 1000]), np.array([5, 6], dtype=np.float32))
    assert list(client.query_raw(a, limit=2)[1]) == [5.0, 6.0]


def test_aggregates(client):
    a = client.series("agg")
    n = 7200
    vals = [float((i * 7) % 100 - 50) for i in range(n)]
    client.insert_series(a, [T0 + i * 1000 for i in range(n)], vals)
    bk = client.query_agg(a, T0, T0 + HOUR, interval_ms=MIN)
    assert len(bk) == 60
    for k, b in enumerate(bk):
        w = vals[k * 60 : k * 60 + 60]
        assert (b.start, b.count, b.min, b.max) == (T0 + k * MIN, 60, min(w), max(w))
        assert b.sum == pytest.approx(sum(w))
        assert b.avg == pytest.approx(sum(w) / 60)
    (one,) = client.query_agg(a, T0 - 1)
    assert (one.start, one.count) == (T0 - 1, n)
    assert "count=7200" in repr(one)
    assert client.query_agg(a, T0 + DAY, None, MIN) == []
    with pytest.raises(TsdbError) as e:
        client.query_agg(a, T0, T0 + 1, interval_ms=-5)
    assert e.value.code == tsdb_client.ERR_INVALID


def test_group_by_in_a_time_zone(client):
    a = client.series("grp")
    n = 3 * 1440
    client.insert_series(a, [T0 + i * MIN for i in range(n)], [1.0] * n)

    days = client.query_group(a, unit="day", timezone="Europe/Oslo")
    assert len(days) == 4
    assert all(d.start % DAY == 23 * HOUR for d in days)  # local midnight is 23:00 UTC in November
    assert [d.count for d in days][:2] == [47, 1440]
    assert sum(d.count for d in days) == n

    last2 = client.query_group(a, unit="day", timezone="Europe/Oslo", order="desc", limit=2)
    assert [b.start for b in last2] == [days[3].start, days[2].start]
    assert last2[0] == days[3]

    six = client.query_group(a, unit="hour", count=6, timezone="+02:00")
    assert all((b.start + 2 * HOUR) % (6 * HOUR) == 0 for b in six)
    assert all(d.start % DAY == 0 for d in client.query_group(a, unit="day"))
    (whole,) = client.query_group(a, T0)
    assert (whole.start, whole.count) == (T0, n)

    with pytest.raises(TsdbError) as e:
        client.query_group(a, unit="day", timezone="Mars/Olympus_Mons")
    assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(ValueError):
        client.query_group(a, unit="fortnight")


def test_maintenance(client):
    a = client.series("m")
    for k in range(4):
        client.insert_series(a, [T0 + (k * 1000 + i) * 1000 for i in range(1000)], [float(k)] * 1000)
        client.flush()
    client.commit()
    st = client.stats()
    assert (st.rows, st.memtable_rows) == (4000, 0)
    assert st.parts >= 1 and st.bytes > 0
    client.optimize()
    st = client.stats()
    assert st.parts == st.partitions
    assert "rows=4000" in repr(st)


def test_errors(server, client):
    a = client.series("e")
    with pytest.raises(TsdbError) as e:
        client.insert_series(a, [T0, T0 + 1], [1.0])
    assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(TsdbError):
        client.series("")
    with pytest.raises(TypeError):
        client.insert_series(a, ["x"], [1.0])
    with pytest.raises(TsdbError) as e:
        client.insert_series(999_999, [T0], [1.0])
    assert e.value.code == tsdb_client.ERR_INVALID
    assert str(e.value) == "server error -4: invalid argument"
    assert tsdb_client.strerror(tsdb_client.ERR_NOTFOUND) == "not found"
    # ids are 64 bits on the wire (protocol 11) and carry the volume above bit 32: a + 2**32 goes out whole and names
    # a volume tsdbd does not have. It is never taken for `a`, whose low 32 bits it shares.
    for call in (
        lambda: client.insert_series(a + 2**32, [T0], [1.0]),
        lambda: client.insert([a, a + 2**32], [T0, T0], [1.0, 2.0]),
        lambda: client.insert(array.array("Q", [a, a + 2**32]), [T0, T0], [1.0, 2.0]),
    ):
        with pytest.raises(TsdbError) as e:
            call()
        assert e.value.code == tsdb_client.ERR_INVALID and "32-bit" not in str(e.value)
    with pytest.raises(TsdbError) as e:
        client.series_name(a + 2**32)
    assert e.value.code == tsdb_client.ERR_NOTFOUND
    try:  # an unknown series: an error, or no rows
        assert client.query_raw(a + 2**32, numpy=False) == ([], [])
    except TsdbError as e:
        assert e.code in (tsdb_client.ERR_NOTFOUND, tsdb_client.ERR_INVALID)
    with pytest.raises(OverflowError):
        client.insert([a, -1], [T0, T0], [1.0, 2.0])
    assert client.query_raw(a, numpy=False) == ([], [])

    client.insert_series_async(999_999, [T0], [1.0])
    client.insert_series_async(a, [T0], [1.0])
    with pytest.raises(TsdbError):
        client.wait()
    client.wait()
    assert client.query_raw(a, numpy=False) == ([T0], [1.0])

    client.close()
    client.close()
    with pytest.raises(TsdbError) as e:
        client.stats()
    assert e.value.code == tsdb_client.ERR_IO
    assert "closed" in repr(client)


def test_server_going_away(server):
    c = Client(port=server.port, timeout=5.0)
    c.series("x")
    assert server.stop() == 0
    with pytest.raises(TsdbError) as e:
        c.stats()
    assert e.value.code == tsdb_client.ERR_IO
    with pytest.raises(TsdbError):
        Client(port=server.port)


def test_threads_share_or_own_clients(server, client):
    ids = [client.series(f"vib.{i}") for i in range(8)]
    errors = []

    def work(c, sid):
        try:
            for r in range(20):
                c.insert_series_async(sid, [T0 + (r * 500 + i) * 1000 for i in range(500)], [float(sid & 0xFFFFFFFF)] * 500)
            c.wait()
        except Exception as e:  # noqa: BLE001 - reported below
            errors.append(e)

    own = [Client(port=server.port) for _ in ids[:4]]
    threads = [threading.Thread(target=work, args=(c, sid)) for c, sid in zip(own, ids[:4])]
    threads += [threading.Thread(target=work, args=(client, sid)) for sid in ids[4:]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for c in own:
        c.close()
    assert errors == []
    for sid in ids:
        (b,) = client.query_agg(sid)
        assert (b.count, b.min, b.max) == (10_000, sid & 0xFFFFFFFF, sid & 0xFFFFFFFF)


def test_last_query_stats(client):
    assert client.last_query_stats().result_rows == 0  # no query yet
    a = client.series("qs")
    n = 200_000
    client.insert_series(a, array.array("q", (T0 + i * 1000 for i in range(n))), array.array("f", [1.0]) * n)
    client.flush()

    (b,) = client.query_agg(a)  # answered from granule statistics
    st = client.last_query_stats()
    assert b.count == n
    assert (st.rows, st.rows_from_stats, st.rows_decoded, st.memtable_rows, st.bytes_read) == (n, n, 0, 0, 0)
    assert st.result_rows == 1 and st.granules == st.granules_from_stats >= 1 and st.elapsed_s >= 0
    assert "rows_from_stats=200000" in repr(st)

    bk = client.query_agg(a, T0, T0 + 600_000, interval_ms=10_000)  # smaller than a granule: decoded
    st = client.last_query_stats()
    assert len(bk) == 60 and st.result_rows == 60
    assert st.rows_decoded > 0 and st.bytes_read > 0
    assert st.rows == st.rows_decoded + st.rows_from_stats + st.memtable_rows

    t, _ = client.query_raw(a, order="desc", limit=7, numpy=False)
    assert len(t) == client.last_query_stats().result_rows == 7
