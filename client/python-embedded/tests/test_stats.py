# SPDX-License-Identifier: AGPL-3.0-only
"""series_stats() and tenant_stats() (protocol version 15) on Db (test_stats.py of tsdb_client has Client and Pool):
the same data written through tsdbd and through the engine gives the same numbers."""

import numpy as np
import pytest

from conftest import Server  # noqa: F401
import tsdb_client
from tsdb_client import Client, TsdbError
from tsdb_embedded import Db

T0 = 1_700_000_000_000
MISSING = 1 << 32 | 0xFFFFFF00
NAMES = ["acme/a", "acme/b", "acme/lab/c", "other/d"]
AT_REST = ("rows", "nulls", "type", "cls", "logical_bytes", "ts_bytes", "value_bytes", "index_bytes", "parts",
           "partitions", "granules", "full_granules", "first_ts", "last_ts", "status", "tomb_bytes")


def fill(c):
    ts = T0 + np.arange(1000, dtype=np.int64) * 1000
    ids = []
    for i, name in enumerate(NAMES):
        s = c.series(name)
        c.insert_series(s, ts, np.arange(1000, dtype=np.float32) * (i + 1))
        ids.append(s)
    c.flush()
    return ids


def test_embedded_agrees_with_tsdbd(tmp_path):
    f = tmp_path / "tenants"
    f.write_text("acme/\nacme/lab/\n")
    from conftest import serve

    for srv in serve(extra_args=["--tenants", str(f)]):
        with Client("127.0.0.1", srv.port, timeout=30) as c, Db(tmp_path / "db", fsync=False, merge_threads=0, stats=1) as d:
            d.set_tenants(["acme/", "acme/lab/"])
            got = []
            for x in (c, d):
                ids = fill(x)
                x.stats_settle(30000) if x is d else None
                r = x.series_stats(ids + [MISSING], errors="status")
                got.append([{k: v for k, v in s.items() if k in AT_REST} for s in r])
            assert got[0] == got[1] and got[0][0]["rows"] == 1000 and got[0][4]["status"] == tsdb_client.ERR_NOTFOUND
            with pytest.raises(TsdbError) as e:
                d.series_stats(ids + [MISSING])
            assert e.value.code == tsdb_client.ERR_NOTFOUND and e.value.index == 4
            for _ in range(100):
                t = c.tenant_stats(volumes=True, hot=2)
                if not t["partial"] and [r["series"] for r in t["rows"] if r["volume"] is None] == [1, 2, 1]:
                    break
                import time

                time.sleep(0.1)
            e = d.tenant_stats(volumes=True, hot=2)
            assert [(r["prefix"], r["volume"]) for r in e["rows"]] == [(r["prefix"], r["volume"]) for r in t["rows"]]
            for a, b in zip(t["rows"], e["rows"]):
                for k in ("status", "series", "rows", "nulls", "logical_bytes", "ts_bytes", "value_bytes",
                          "index_bytes", "granules"):
                    assert a[k] == b[k], (a["prefix"], k)
                assert (a["caller"] is None) == (b["caller"] is None)
            assert e["list_hash"] == t["list_hash"]
            assert all(r["caller"] is None for r in e["rows"])
            assert d.tenant_stats(["acme/lab/", "nope/"])["rows"][1]["status"] == tsdb_client.ERR_NOTFOUND
            with pytest.raises(ValueError):
                d.tenant_stats(hot=257)
