# SPDX-License-Identifier: Apache-2.0
"""latest() and latest_by_name() (protocol version 14) on Client and Pool (test_latest.py of tsdb_embedded has Db).

The newest row of each series, from the table the engine keeps: it must agree with what a read of the
series' rows says, for every value type, with nulls, with an end bound, and for series that do not exist.
"""

import numpy as np
import pytest

from conftest import ROOT_PASSWORD, Server  # noqa: F401
import tsdb_client
from tsdb_client import Client, LatestMany, Pool, TsdbError

T0 = 1_700_000_000_000 - 1_700_000_000_000 % 86_400_000
UNKNOWN = 1 << 32 | 999_999


def fill(c):
    """One series of each type, one with a trailing null, one empty; returns {name: id}."""
    ts = T0 + np.arange(100, dtype=np.int64) * 1000
    ids = {}
    ids["f32"] = c.series("l.f32")
    c.insert_series(ids["f32"], ts, np.arange(100, dtype=np.float32) / 4)
    ids["f64"] = c.series_create("l.f64", "float64")
    c.insert_series_values(ids["f64"], ts, [i / 3 for i in range(100)])
    ids["i64"] = c.series_create("l.i64", "int64")
    c.insert_series_values(ids["i64"], ts, np.arange(100, dtype=np.int64) * (1 << 40))
    ids["bool"] = c.series_create("l.bool", "bool")
    c.insert_series_values(ids["bool"], ts, [i % 2 == 0 for i in range(100)])
    ids["state"] = c.series_create("l.state", "state")
    code = c.state(ids["state"], "run")
    c.insert_series_values(ids["state"], ts, np.full(100, code, dtype=np.uint32))
    ids["nul"] = c.series_create("l.null", "float64")
    c.insert_series_values(ids["nul"], ts[:10], [None if i >= 7 else float(i) for i in range(10)])
    ids["empty"] = c.series_create("l.empty", "float64")
    c.flush()
    return ids, ts


@pytest.fixture(params=["client", "pool"])
def target(request, server, tmp_path):
    with Client("127.0.0.1", server.port, timeout=30) as c:
        filled = fill(c)
        if request.param == "client":
            yield c, filled
        else:
            with Pool("127.0.0.1", server.port, 2, timeout=30) as p:
                yield p, filled


def test_latest_values_of_every_type(target):
    c, (ids, ts) = target
    names = ["f32", "f64", "i64", "bool", "state", "nul", "empty"]
    r = c.latest([ids[n] for n in names])
    assert isinstance(r, LatestMany) and len(r) == len(names) and r.ok
    assert list(r.status) == [0] * len(names)
    last = int(ts[-1])
    assert r[0] == (last, 99 / 4)
    assert r[1] == (last, 99 / 3)
    assert r[2] == (last, 99 * (1 << 40)) and isinstance(r[2][1], int)
    assert r[3] == (last, False) and r[3][1] is False
    assert r[4][0] == last and isinstance(r[4][1], int)
    assert r[5] == (int(ts[6]), 6.0)  # the nulls after it are skipped
    assert r[6] is None
    assert list(r.kind) == [0, 0, 0, 0, 0, 0, 2]
    assert r[-1] is None
    with pytest.raises(IndexError):
        r[len(names)]
    # a null is an answer with with_nulls
    r = c.latest([ids["nul"], ids["f32"]], with_nulls=True)
    assert r[0] == (int(ts[9]), None) and r[1] == (last, 99 / 4)
    assert list(r.kind) == [1, 0]
    # an end bound: rows before it
    r = c.latest([ids["f32"], ids["f64"]], int(ts[10]))
    assert r[0] == (int(ts[9]), 9 / 4) and r[1] == (int(ts[9]), 9 / 3)
    r = c.latest([ids["f32"], ids["f64"]], [int(ts[10]), int(ts[0])])
    assert r[0] == (int(ts[9]), 9 / 4) and r[1] is None
    with pytest.raises(ValueError):
        c.latest([ids["f32"]], [1, 2])
    # what a read says
    for n in ("f32", "f64", "i64"):
        t, v = c.query_values(ids[n])[0:2] if hasattr(c, "query_values") else (None, None)
        if t is not None:
            assert c.latest([ids[n]])[0] == (int(t[-1]), v[-1].item())


def test_latest_missing_and_by_name(target):
    c, (ids, ts) = target
    r = c.latest([ids["f32"], UNKNOWN], errors="status")
    assert not r.ok and list(r.status) == [0, tsdb_client.ERR_NOTFOUND]
    assert r[0] is not None and isinstance(r.error_of(1), TsdbError) and r.error_of(0) is None
    with pytest.raises(TsdbError) as e:
        r[1]
    assert e.value.code == tsdb_client.ERR_NOTFOUND
    with pytest.raises(TsdbError) as e:
        c.latest([ids["f32"], UNKNOWN])
    assert e.value.code == tsdb_client.ERR_NOTFOUND and e.value.index == 1 and isinstance(e.value.result, LatestMany)
    assert len(c.latest([], errors="status")) == 0
    with pytest.raises(ValueError):
        c.latest([ids["f32"]], errors="nope")
    # by name
    r = c.latest_by_name(["l.f64", "l.nothing", "l.empty"], errors="status")
    assert list(r.status) == [0, tsdb_client.ERR_NOTFOUND, 0]
    assert r[0] == (int(ts[-1]), 99 / 3) and r[2] is None and r.names == ["l.f64", "l.nothing", "l.empty"]
    again = c.latest_by_name(["l.f64"])  # a second call uses the connection's name cache
    assert again[0] == r[0]
    assert c.latest_by_name(["l.f64"], numpy=False).ts == [int(ts[-1])]


def test_latest_without_numpy_gives_lists(target):
    c, (ids, ts) = target
    r = c.latest([ids["f32"], ids["i64"]], numpy=False)
    assert r.ts == [int(ts[-1])] * 2 and r.status == [0, 0] and r.kind == [0, 0]
    assert r.values == [99 / 4, 99 * (1 << 40)]


def test_denied_looks_like_missing(auth_server, tmp_path):
    srv = auth_server
    with Client(port=srv.port, user="root", password=ROOT_PASSWORD) as root:
        a, b = root.series("tenant/a/x"), root.series("tenant/b/x")
        root.insert_series(a, [T0], [1.5])
        root.insert_series(b, [T0], [2.5])
        root.user_create("alice", "pw-alice-1")
        root.grant("alice", "prefix:tenant/a/", "read")
    with Client(port=srv.port, user="alice", password="pw-alice-1") as c:
        r = c.latest([a, b, UNKNOWN], errors="status")
        assert list(r.status) == [0, tsdb_client.ERR_DENIED, tsdb_client.ERR_DENIED]
        assert r[0] == (T0, 1.5)
        r = c.latest_by_name(["tenant/a/x", "tenant/b/x", "tenant/zzz"], errors="status")
        assert r[0] == (T0, 1.5)
        # a name outside the grants is DENIED whether or not it exists
        assert r.status[1] == r.status[2] == tsdb_client.ERR_DENIED
        with pytest.raises(TsdbError) as e:
            c.latest([b])
        assert e.value.code == tsdb_client.ERR_DENIED
    with Pool(port=srv.port, connections=2, user="alice", password="pw-alice-1") as p:
        assert p.latest([a])[0] == (T0, 1.5)


