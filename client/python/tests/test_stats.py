# SPDX-License-Identifier: Apache-2.0
"""series_stats() and tenant_stats() (protocol version 15) on Client and Pool: the numbers of series and tenants,
the rights (a tenant admin sees only its tenant, denied and missing look alike), more ids than one request holds."""

import os
import time

import numpy as np
import pytest

from conftest import ROOT_PASSWORD, serve  # noqa: F401
import tsdb_client
from tsdb_client import Client, Pool, TsdbError

T0 = 1_700_000_000_000
MISSING = 1 << 32 | 0xFFFFFF00


def fill(c, names, n=100):
    ts = T0 + np.arange(n, dtype=np.int64) * 1000
    ids = []
    for name in names:
        s = c.series(name)
        c.insert_series(s, ts, np.arange(n, dtype=np.float32))
        ids.append(s)
    c.flush()
    return ids


def test_series_stats(client):
    a, b = fill(client, ["st.a", "st.b"], 1000)
    c = client.series_create("st.c", "int64")
    r = client.series_stats([a, b, c, MISSING], errors="status")
    assert [x["series"] for x in r] == [a, b, c, MISSING]
    assert r[0]["status"] == 0 and r[0]["rows"] == 1000 and r[0]["type"] == "float32"
    assert r[0]["first_ts"] == T0 and r[0]["last_ts"] == T0 + 999_000
    assert r[0]["logical_bytes"] == 12_000 and r[0]["ts_bytes"] > 0 and r[0]["value_bytes"] > 0
    assert r[2]["rows"] == 0 and r[2]["type"] == "int64"
    assert r[3]["status"] == tsdb_client.ERR_NOTFOUND and r[3]["type"] is None and r[3]["rows"] == 0
    # errors="raise": the first failing series
    with pytest.raises(TsdbError) as e:
        client.series_stats([a, MISSING])
    assert e.value.code == tsdb_client.ERR_NOTFOUND and e.value.index == 1
    with pytest.raises(ValueError):
        client.series_stats([a], errors="nope")
    assert client.series_stats([]) == []
    # numpy ids, resident
    r = client.series_stats(np.array([a, b], dtype=np.uint64), resident=True)
    assert [x["rows"] for x in r] == [1000, 1000]
    # more ids than one request holds
    many = np.array([a, c] * 35_000, dtype=np.uint64)
    big = client.series_stats(many)
    assert len(big) == 70_000 and all(x["rows"] == (1000 if i % 2 == 0 else 0) for i, x in enumerate(big))


def test_pool(server):
    with Pool("127.0.0.1", server.port, 2) as p:
        a = fill(server_client(server), ["pool.a"], 1)[0]
        assert p.series_stats([a])[0]["rows"] == 1
        t = p.tenant_stats()
        assert [r["prefix"] for r in t["rows"]] == [""]


def server_client(server):
    return Client("127.0.0.1", server.port)


def wait_tenants(c, want, **kw):
    for _ in range(100):
        t = c.tenant_stats(**kw)
        if want(t):
            return t
        time.sleep(0.1)
    raise AssertionError(t)


def test_tenants_from_a_file(tmp_path):
    f = tmp_path / "tenants"
    f.write_text("acme/\nacme/lab/\n")
    for srv in serve(extra_args=["--tenants", str(f)]):
        with Client("127.0.0.1", srv.port) as c:
            ids = fill(c, ["acme/a", "acme/b", "acme/lab/c", "other/d"])
            t = wait_tenants(
                c, lambda t: [r["series"] for r in t["rows"]] == [1, 2, 1], hot=3, caller=True, volumes=False
            )
            assert [r["prefix"] for r in t["rows"]] == ["", "acme/", "acme/lab/"]
            assert all(r["volume"] is None and r["status"] == 0 for r in t["rows"])
            t = c.tenant_stats(["", "acme/", "nope/"], volumes=True, caller=True, hot=5)
            assert [(r["prefix"], r["volume"]) for r in t["rows"]] == [
                ("", None), ("", 0), ("acme/", None), ("acme/", 0), ("nope/", None)
            ]
            assert t["rows"][4]["status"] == tsdb_client.ERR_NOTFOUND
            assert t["rows"][0]["caller"]["requests_insert"] >= 4 and t["rows"][1]["caller"] is None
            hot = t["rows"][2]["hot"]
            assert sum(h["series"] == 0 for h in hot) == 3
            assert any(h["kind"] == "writes" and h["series"] == ids[0] for h in hot)
            assert all(h["series"] not in (ids[2], ids[3]) for h in hot)
            assert t["rows"][2]["rows"] == t["rows"][3]["rows"] == 200
            assert set(t) >= {"generation", "list_hash", "partial", "parts_counted", "parts_listed", "rows"}
            with pytest.raises(ValueError):
                c.tenant_stats(hot=257)


def test_a_tenant_admin_sees_only_its_tenant(tmp_path):
    from conftest import user_cli  # noqa: F401

    for srv in serve(ROOT_PASSWORD, extra_args=["--tenants-settle", "1"]):
        with Client("127.0.0.1", srv.port, user="root", password=ROOT_PASSWORD) as root:
            a, b = fill(root, ["acme/a", "other/b"], 10)
            root.user_create("acme/boss", "boss-pw")
            root.grant("acme/boss", "prefix:acme/", "read")
            root.grant("acme/boss", "prefix:acme/", "admin")
            root.user_create("u", "u-pw")
            root.grant("u", "prefix:acme/", "read")
            wait_tenants(root, lambda t: any(r["prefix"] == "acme/" for r in t["rows"]))
            assert [r["prefix"] for r in root.tenant_stats()["rows"]][0] == ""
            with Client("127.0.0.1", srv.port, user="acme/boss", password="boss-pw") as boss:
                t = boss.tenant_stats(caller=True)
                assert [r["prefix"] for r in t["rows"]] == ["acme/"]
                assert (t["parts_counted"], t["parts_listed"]) == (0, 0)
                t = boss.tenant_stats(["other/", "nope/", ""])
                assert [r["status"] for r in t["rows"]] == [tsdb_client.ERR_DENIED] * 3
            with Client("127.0.0.1", srv.port, user="u", password="u-pw") as u:
                assert u.tenant_stats()["rows"] == []
                assert u.tenant_stats(["acme/"])["rows"][0]["status"] == tsdb_client.ERR_DENIED
                r = u.series_stats([a, b, MISSING], errors="status")
                assert r[0]["status"] == 0
                # an existing series outside the grants and a missing one answer alike
                assert {k: v for k, v in r[1].items() if k != "series"} == {k: v for k, v in r[2].items() if k != "series"}
                assert r[1]["status"] == tsdb_client.ERR_DENIED
                assert r[0]["reads_err"] == 0
                with pytest.raises(TsdbError) as e:
                    u.series_stats([a, b])
                assert e.value.code == tsdb_client.ERR_DENIED and e.value.index == 1
            r = root.series_stats([a, MISSING], errors="status")
            assert r[0]["rows"] == 10 and r[1]["status"] == tsdb_client.ERR_NOTFOUND
