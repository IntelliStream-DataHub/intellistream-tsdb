"""Range deletes, replaces and retention (protocol version 7) against a real tsdbd."""

import time

import pytest

import tsdb_client
from tsdb_client import TsdbError

T0 = 1_699_999_980_000
HOUR = 3_600_000


def invalid(fn, *args, **kw):
    with pytest.raises(TsdbError) as e:
        fn(*args, **kw)
    return e.value.code == tsdb_client.ERR_INVALID


def test_the_server_speaks_version_7(client):
    assert client.protocol_version() >= 7


def test_delete_and_replace(client):
    sid = client.series_create("d", "int64")
    ts = [T0 + k * 1000 for k in range(10)]
    client.insert_series_values(sid, ts, list(range(10)))
    client.flush()

    client.delete_range(sid, T0 + 2000, T0 + 5000)
    keep = [0, 1, 5, 6, 7, 8, 9]
    assert client.query_values(sid, numpy=False) == ([ts[k] for k in keep], keep)
    client.delete_range(sid, None, T0 + 1000)
    assert client.query_values(sid, numpy=False)[0] == [ts[k] for k in keep[1:]]
    with pytest.raises(TypeError):
        client.delete_range(sid)  # the range is never implied

    # a null among the new rows; the type asked from the server
    client.replace_range(sid, T0, T0 + 6000, [T0 + 500, T0 + 5500], [100, None])
    assert client.query_values(sid, numpy=False) == ([T0 + 500, T0 + 5500] + ts[6:], [100, None, 6, 7, 8, 9])
    client.replace_range(sid, T0 + 6000, None, [T0 + 6000], [-1], value_type="int64")
    assert client.query_values(sid, numpy=False) == ([T0 + 500, T0 + 5500, T0 + 6000], [100, None, -1])

    assert invalid(client.replace_range, sid, T0, T0 + 10, [T0 + 10], [1])
    assert invalid(client.replace_range, sid, T0, T0 + 10, [T0], [1.0], value_type="float64")
    assert invalid(client.replace_range, sid, T0, T0 + 10, [T0], [1, 2])

    client.replace_range(sid, None, None, [], [])
    assert client.query_values(sid, numpy=False) == ([], [])


def test_retention(client):
    sid = client.series_create("r", "float64")
    assert client.retention(sid) == 0
    now = int(time.time() * 1000)
    client.insert_series_values(sid, [now - 2 * HOUR, now - HOUR // 6], [1.0, 2.0])
    client.set_retention(sid, HOUR)
    assert client.retention(sid) == HOUR
    assert client.query_values(sid, numpy=False) == ([now - HOUR // 6], [2.0])
    assert invalid(client.set_retention, sid, -1)
    client.set_retention(sid, 0)
    assert client.retention(sid) == 0
