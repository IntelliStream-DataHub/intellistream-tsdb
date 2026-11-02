"""tsdb_client.Db: the engine inside this process.

The Client tests that need nothing but a client run here a second time: this module's `client`
fixture (a Db on a fresh directory) replaces the one in conftest.py, so the same code checks that
Db answers as tsdbd does. After them come the cases only an embedded database has.
"""

import array
import gc
import threading
import time

import pytest

import tsdb_client
from tsdb_client import Db, TsdbError

# Imported tests are collected again in this module and use its `client` fixture. Left out: the ones
# about pipelining (errors of async inserts arrive later over TCP; embedded inserts finish at once) and
# the ones that need a server.
from test_client import (  # noqa: F401
    test_aggregates,
    test_group_by_in_a_time_zone,
    test_last_query_stats,
    test_maintenance,
    test_numpy,
    test_order_limit_and_columns,
)
from test_types import (  # noqa: F401
    test_bools_floats_and_float32,
    test_buffers_without_numpy,
    test_exact_integers,
    test_int64_nulls_order_limit_and_groups,
    test_numpy_masked_arrays_and_chunk_boundaries,
    test_states,
    test_type_mismatches,
)
from test_analytics import (  # noqa: F401
    test_aggregates_with_order_and_limit,
    test_lttb,
    test_spread,
    test_time_weighted_averages,
)
from test_deletes import test_delete_and_replace, test_retention  # noqa: F401
from test_zones import test_groups_are_those_of_the_c_client, test_oslo_days, test_query_bounds  # noqa: F401

T0 = 1_699_999_980_000


@pytest.fixture
def client(tmp_path):
    with Db(tmp_path / "db", fsync=False, merge_threads=1) as db:
        yield db


def code(fn, *args, **kw):
    with pytest.raises(TsdbError) as e:
        fn(*args, **kw)
    return e.value.code


def test_async_inserts_are_done_at_once(client):
    a = client.series("a")
    client.insert_series_async(a, [T0, T0 + 1000], [1.0, 2.0])
    client.insert_async([a], [T0], [5.0])  # the last write wins
    assert client.pending == 0
    client.wait()
    assert client.query_raw(a, numpy=False) == ([T0, T0 + 1000], [5.0, 2.0])
    x = client.series_create("x", "bool")
    with pytest.raises(TsdbError) as e:  # raised by the call itself, not by wait()
        client.insert_series_values_async(x, [T0], [1], value_type="int64")
    assert e.value.code == tsdb_client.ERR_INVALID
    client.insert_values_async([x, x], [T0, T0 + 1], [True, None])
    assert client.query_values(x, numpy=False) == ([T0, T0 + 1], [True, None])


def test_series_types_and_volumes(tmp_path, client):
    for t in tsdb_client.VALUE_TYPES:
        sid = client.series_create(f"t.{t}", t)
        assert client.series_type(sid) == t
        assert client.series_create(f"t.{t}", t) == sid
    assert code(client.series_create, "t.float64", "int64") == tsdb_client.ERR_INVALID
    assert code(client.series_type, 999_999) == tsdb_client.ERR_NOTFOUND
    assert code(client.series, "nope", create=False) == tsdb_client.ERR_NOTFOUND
    with pytest.raises(ValueError):
        client.series_create("x", "decimal")
    vol = str(tmp_path / "volume")
    v = client.series_create("on.volume", "int64", vol)
    client.insert_series_values(v, [T0], [42])
    assert client.query_values(v, numpy=False) == ([T0], [42])
    assert client.series_path(v) == vol and client.volumes() == [str(client.path), vol]
    assert client.series_count() == len(tsdb_client.VALUE_TYPES) + 1
    assert client.series_name(v) == "on.volume"
    assert client.failed() is None and Db.simd_level()


def test_series_ids_are_64_bits(client, tmp_path):
    import numpy as np

    # ids are (volume + 1) << 32 | local: every real one is past 2**32, in the database directory and in a volume
    a, b = client.series("a"), client.series("b")
    c = client.series_at("c", str(tmp_path / "volume"))
    assert (a >> 32, a & 0xFFFFFFFF, b >> 32, b & 0xFFFFFFFF, c >> 32, c & 0xFFFFFFFF) == (1, 0, 1, 1, 2, 0)
    assert sorted(client.series_ids()) == [a, b, c] and client.series_count() == 3
    # id columns: uint64 and int64 arrays, array.array and lists of ints
    client.insert(np.array([a, b], dtype=np.uint64), [T0, T0], [1.0, 2.0])
    client.insert(np.array([a, b], dtype=np.int64), [T0 + 1, T0 + 1], [3.0, 4.0])
    client.insert(array.array("Q", [a, b]), [T0 + 2, T0 + 2], [5.0, 6.0])
    client.insert_values([a, b], [T0 + 3, T0 + 3], [7.0, 8.0])
    client.insert(np.array([a, 0, b, 0], dtype=np.int64)[::2], [T0 + 4, T0 + 4], [9.0, 10.0])  # a strided view
    client.insert(array.array("q", [a, b]), [T0 + 5, T0 + 5], [11.0, 12.0])
    assert list(client.query_points(a, numpy=False)[1]) == [1.0, 3.0, 5.0, 7.0, 9.0, 11.0]
    assert len(client.query_values_multi(np.array([a, b], dtype=np.int64))) == 2
    client.delete_range(a, T0 + 4, None)
    client.delete_range(b, T0 + 4, None)
    assert [float(r[1][0]) for r in client.query_raw_many(np.array([b, a], dtype=np.uint64))] == [2.0, 1.0]
    # a negative id, or one of 2**64 or more, is no id
    for bad in (-1, 2**64):
        with pytest.raises(OverflowError):
            client.insert([a, bad], [T0, T0], [1.0, 2.0])
        with pytest.raises(OverflowError):
            client.insert_series(bad, [T0], [1.0])
        with pytest.raises(OverflowError):
            client.series_name(bad)
        with pytest.raises(OverflowError):
            client.insert_many([(bad, [T0], [1.0])])
    # an int64 column with a negative id: ValueError, nothing written
    for neg in (np.array([a, -1], dtype=np.int64), array.array("q", [a, -(2**63)])):
        with pytest.raises(ValueError):
            client.insert(neg, [T0, T0], [1.0, 2.0])
        with pytest.raises(ValueError):
            client.query_values_multi(neg)
    # a 32-bit column cannot hold an id (all are past 2**32): TypeError, rather than INVALID for truncated ids
    truncated = np.array([a, b], dtype=np.uint64).astype(np.uint32)
    for narrow in (truncated, truncated.astype(np.int32), array.array("I", [1, 2]), array.array("i", [1, 2])):
        with pytest.raises(TypeError):
            client.insert(narrow, [T0 + 9] * 2, [9.0] * 2)
        with pytest.raises(TypeError):
            client.query_values_multi(narrow)
        with pytest.raises(TypeError):
            client.insert_values(narrow, [T0 + 9] * 2, [9.0] * 2)
    assert list(client.query_points(a, numpy=False)[1]) == [1.0, 3.0, 5.0, 7.0]
    # a malformed id is unknown, never the series its low 32 bits name: a zero volume field (a truncated id), any of
    # bits 48-63, a volume the database does not have, a local number past its series
    for bad in (a & 0xFFFFFFFF, b & 0xFFFFFFFF, a | 1 << 48, c | 1 << 63, 2**64 - 1, a + (2 << 32), c + 1, a + 1000):
        assert code(client.insert_series, bad, [T0 + 9], [9.0]) == tsdb_client.ERR_INVALID
        assert code(client.insert, np.array([a, bad], dtype=np.uint64), [T0 + 9] * 2, [9.0] * 2) == tsdb_client.ERR_INVALID
        assert code(client.series_name, bad) == tsdb_client.ERR_NOTFOUND
        if bad >> 48 or not bad >> 32:  # no volume can have it: queries say so rather than answer an empty range
            assert code(client.query_points, bad, numpy=False) == tsdb_client.ERR_INVALID
            assert code(client.query_agg, bad, None, None, 0) == tsdb_client.ERR_INVALID
        else:
            assert len(client.query_points(bad, numpy=False)[0]) == 0
    assert list(client.query_points(a, numpy=False)[1]) == [1.0, 3.0, 5.0, 7.0]
    client.insert_series(c, [T0], [11.0])
    assert client.series_name(c) == "c" and list(client.query_points(c, numpy=False)[1]) == [11.0]


def test_options_and_errors(tmp_path):
    with pytest.raises(ValueError):
        Db(tmp_path / "x", value_codec="lz4")
    assert code(Db, tmp_path / "missing" / "db") == tsdb_client.ERR_IO
    db = Db(tmp_path / "db", fsync=False, wal_sync_ms=-1, value_codec="chimp", granule_rows=1024, zstd_level=0)
    a = db.series("a")
    db.insert_series(a, [T0 + i for i in range(5000)], [float(i) for i in range(5000)])
    db.flush()
    assert db.stats().parts >= 1 and db.query_agg(a)[0].count == 5000
    assert "open" in repr(db)
    db.close()
    db.close()  # twice is fine
    assert "closed" in repr(db)
    assert code(db.series, "a") == tsdb_client.ERR_IO
    assert code(db.query_raw, a) == tsdb_client.ERR_IO


def test_a_locked_directory_and_readers(tmp_path):
    path = tmp_path / "db"
    with Db(path, fsync=False) as db:
        x = db.series("x")
        db.insert_series(x, [T0], [1.0])
        db.flush()
        assert code(Db, path) == tsdb_client.ERR_LOCKED
        with Db(path, read_only=True) as ro:  # a reader takes no lock
            assert ro.query_raw(ro.series("x", create=False), numpy=False) == ([T0], [1.0])
            assert code(ro.series, "y") == tsdb_client.ERR_READONLY
            db.insert_series(x, [T0 + 1], [2.0])
            db.flush()
            ro.refresh()
            assert ro.query_raw(x, numpy=False) == ([T0, T0 + 1], [1.0, 2.0])
    with Db(path) as again:  # closed: the lock is gone
        assert again.series("x", create=False) == x


def test_threads_share_one_db(client):
    ids = [client.series_create(f"vib.{i}", "float64") for i in range(8)]
    errors = []
    start = threading.Barrier(len(ids))

    def work(sid):
        try:
            start.wait()
            for r in range(20):
                ts = array.array("q", (T0 + (r * 500 + i) * 1000 for i in range(500)))
                client.insert_series_values(sid, ts, array.array("d", [float(sid)]) * 500)
                (b,) = client.query_agg(sid)
                assert b.count == (r + 1) * 500
        except Exception as e:  # noqa: BLE001 - reported below
            errors.append(e)

    threads = [threading.Thread(target=work, args=(sid,)) for sid in ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    for sid in ids:
        (b,) = client.query_agg(sid)
        assert (b.count, b.min, b.max) == (10_000, sid, sid)


def counted_while(call):
    """How far a Python thread counts while `call` runs, as a share of its pace with the GIL free.

    A call that keeps the GIL lets the counter run for one switch interval (5 ms) at most."""
    count, stop = 0, False

    def spin():
        nonlocal count
        while not stop:
            count += 1

    t = threading.Thread(target=spin)
    t.start()
    try:
        c0, t0 = count, time.perf_counter()
        time.sleep(0.2)
        pace = (count - c0) / (time.perf_counter() - t0)
        c0, t0 = count, time.perf_counter()
        call()
        took = time.perf_counter() - t0
        counted = count - c0
    finally:
        stop = True
        t.join()
    assert took > 0.1, f"the call took {took:.3f} s: too short to tell"
    return counted / (pace * took)


def test_engine_calls_release_the_gil(tmp_path):
    # everything in the memtable, so that flush and close have all the work to do
    n = 3_000_000
    ts = array.array("q", range(T0, T0 + n * 10, 10))
    vals = array.array("d", range(n))

    def loaded(name):
        db = Db(tmp_path / name, fsync=False, merge_threads=1, wal_max_batch_rows=n + 1, buffer_rows=8 * n,
                flush_interval_ms=3_600_000)
        for i in range(4):
            db.insert_series_values(db.series_create(f"s{i}", "float64"), ts, vals)
        return db

    db = loaded("flush")
    assert counted_while(db.flush) > 0.3
    db.close()
    # a Db collected without close() closes, and so flushes, with the GIL released too
    db = loaded("dropped")

    def collect():
        nonlocal db
        db = None
        gc.collect()

    assert counted_while(collect) > 0.3
    with Db(tmp_path / "dropped") as again:
        assert [again.query_agg(again.series(f"s{i}", create=False))[0].count for i in range(4)] == [n] * 4


def fill(db):
    """Every type, with nulls, specials, corrections and some rows still in the memtable."""
    f32 = db.series("f32")
    db.insert_series(f32, [T0 + i * 1000 for i in range(3000)], [i * 0.25 for i in range(3000)])
    db.insert_series(f32, [T0 + 7000], [float("inf")])  # a correction: the last write wins
    f64 = db.series_create("f64", "float64")
    db.insert_series_values(f64, [T0, T0 + 1, T0 + 2, T0 + 3], [1e300, None, float("-inf"), -0.0])
    i64 = db.series_create("i64", "int64")
    db.insert_series_values(i64, [T0 + i * 60_000 for i in range(2000)], [2**62 - i if i % 7 else None for i in range(2000)])
    bl = db.series_create("bool", "bool")
    db.insert_series_values(bl, [T0 + i for i in range(100)], [i % 3 == 0 if i % 5 else None for i in range(100)])
    st = db.series_create("state", "state")
    codes = [db.state(st, n) for n in ("off", "on")]
    db.insert_series_values(st, [T0 + i * 3_600_000 for i in range(48)], [codes[i % 2] for i in range(48)])
    db.flush()
    db.insert_series(f32, [T0 + 1000, T0 + 9_000_000], [-1.0, 5.0])  # memtable
    return [f32, f64, i64, bl, st]


def answers(db, ids):
    out = []
    for s in ids:
        out.append(db.series_name(s))
        out.append(db.series_type(s))
        out.append(db.query_values(s, numpy=False))
        out.append(db.query_values(s, T0 + 500, T0 + 100_000_000, order="desc", limit=17, numpy=False))
        out.append(db.query_agg(s))
        out.append(db.query_agg(s, T0, T0 + 86_400_000, interval_ms=600_000))
        out.append(db.query_group(s, unit="hour", count=2, timezone="Europe/Oslo"))
        out.append(db.query_group(s, unit="day", order="desc", limit=1))
        out.append(db.query_bounds(s, [T0, T0 + 5_000, T0 + 3_600_000, T0 + 86_400_000]))
        out += protocol9(db, s)
    out.append(db.state_names(ids[-1], [1, 2, None]))
    return out


def outcome(fn, *args, **kw):
    """The answer, or the error code: a state series has no spread, TWA or LTTB."""
    try:
        return fn(*args, **kw)
    except TsdbError as e:
        return ("error", e.code)


def protocol9(db, s):
    """The protocol 9 queries, on open and bounded ranges and in a zone (the client computes
    local bounds over TCP, the engine itself when embedded)."""
    bounds = [T0, T0 + 5_000, T0 + 3_600_000, T0 + 86_400_000]
    day = 86_400_000
    out = [
        outcome(db.query_agg, s, None, None, 600_000, order="desc", limit=5),
        outcome(db.query_spread, s),
        outcome(db.query_spread, s, T0, T0 + day, 600_000, order="desc", limit=7),
        outcome(db.query_spread_group, s, unit="hour", count=3, timezone="Europe/Oslo"),
        outcome(db.query_spread_group, s, T0 + 1, T0 + 2 * day, unit="day", timezone="America/New_York"),
        outcome(db.query_spread_bounds, s, bounds, order="desc"),
    ]
    for method in ("step", "linear"):
        for gap in (0, 90_000):
            out += [
                outcome(db.query_twa, s, None, None, 0, method=method, max_gap_ms=gap),
                outcome(db.query_twa, s, T0 + 500, T0 + day, 3_600_000, method=method, max_gap_ms=gap, limit=4),
                outcome(db.query_twa_group, s, unit="day", timezone="Europe/Oslo", method=method, max_gap_ms=gap),
                outcome(db.query_twa_group, s, T0 + 1, T0 + day, unit="hour", timezone="Asia/Kolkata",
                        method=method, max_gap_ms=gap, order="desc"),
                outcome(db.query_twa_bounds, s, bounds, method=method, max_gap_ms=gap),
            ]
    out += [
        outcome(db.query_lttb, s, 20, numpy=False),
        outcome(db.query_lttb, s, 5, T0 + 2, T0 + day, order="desc", limit=3, numpy=False),
        outcome(db.query_lttb, s, 20, limit=0, numpy=False),
    ]
    return out


def test_tsdbd_answers_what_the_embedded_engine_answered(tmp_path):
    from conftest import TSDBD, Server

    import os

    if not os.access(TSDBD, os.X_OK):
        pytest.fail(f"{TSDBD} is missing: run make in the repository root")
    path = tmp_path / "db"
    with Db(path, fsync=False) as db:
        ids = fill(db)
        embedded = answers(db, ids)
    srv = Server(None, dir=path)
    try:
        with tsdb_client.Client(port=srv.port) as c:
            assert [c.series(n, create=False) for n in ("f32", "f64", "i64", "bool", "state")] == ids
            assert answers(c, ids) == embedded
    finally:
        assert srv.stop() == 0
    with Db(path, read_only=True) as db:  # and what tsdbd left is still the same
        assert answers(db, ids) == embedded
