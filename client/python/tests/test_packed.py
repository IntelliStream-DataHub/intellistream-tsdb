"""Packed, zstd-compressed inserts (protocol version 3) through the binding."""

import socket
import struct
import threading

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError

T0 = 1_699_999_980_000


def test_packed_and_plain_store_the_same(server, client):
    plain = Client(port=server.port)
    plain.set_compression(0)
    assert (client.compression, plain.compression) == (tsdb_client.DEFAULT_COMPRESSION, 0)
    n = 3001
    ts = [T0 + i * 1000 - (i % 5) * 3 if i % 11 else -(2**62) + i for i in range(n)]
    data = {
        "float64": [None if i % 13 == 4 else (i**0.5) * -1e300 for i in range(n)],
        "int64": [None if i % 13 == 4 else (2**63 - 1 - i if i % 2 else i) for i in range(n)],
        "bool": [None if i % 13 == 4 else i % 3 == 0 for i in range(n)],
        "state": [None if i % 13 == 4 else 1 + i % 3 for i in range(n)],
        "float32": [None if i % 13 == 4 else i * 0.25 for i in range(n)],
    }
    for t, vals in data.items():
        a, b = client.series_create(f"p.{t}", t), client.series_create(f"q.{t}", t)
        client.insert_series_values(a, ts, vals)
        plain.insert_series_values(b, ts, vals)
        got = client.query_values(a, numpy=False)
        assert got == plain.query_values(b, numpy=False), t
        assert sorted(got[0]) == sorted(ts) and got[1].count(None) == vals.count(None), t
        # many series, ids up and down
        ids = [client.series_create(f"m{k}.{t}", t) for k in range(3)]
        client.insert_values([ids[(2, 0, 1)[i % 3]] for i in range(n)], ts, vals)
        assert sum(len(client.query_values(i, numpy=False)[0]) for i in ids) == n

    f = client.series("f")
    before = (client.bytes_sent, plain.bytes_sent)
    v = [float(i % 17) for i in range(n)]
    client.insert_series(f, ts, v)
    g = plain.series("g")
    plain.insert_series(g, ts, v)
    sent = (client.bytes_sent - before[0], plain.bytes_sent - before[1])
    assert sent[1] == 9 + 8 + n * 12 + (9 + 3 + 1)  # the version 1 INSERT frame, and SERIES "g"
    assert sent[0] < n * 6
    assert client.query_raw(f, numpy=False) == plain.query_raw(g, numpy=False)
    plain.close()


def test_set_compression(client):
    with pytest.raises(TsdbError) as e:
        client.set_compression(1000)
    assert e.value.code == tsdb_client.ERR_INVALID
    i = client.series_create("x", "int64")
    for k, level in enumerate([0, 3, -5, 19]):
        client.set_compression(level)
        assert client.compression == level
        client.insert_series_values(i, [T0 + k * 10 + j for j in range(5)], [1, None, 3, 4, 5])
    assert client.query_values(i, numpy=False)[1] == [1, None, 3, 4, 5] * 4


def fake_server(version):
    """Answers HELLO with `version`, everything else with OK; returns (port, seen ops and payloads)."""
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    seen = []

    def serve():
        s, _ = lst.accept()
        f = s.makefile("rb")
        while hdr := f.read(9):
            length, rid, op = struct.unpack("<IIB", hdr)
            payload = f.read(length - 5)
            body = struct.pack("<I", version) if op == 1 else b""
            s.sendall(struct.pack("<IIBi", 9 + len(body), rid, 0, 0) + body)
            seen.append((op, payload))
        s.close()
        lst.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    return lst.getsockname()[1], seen, th


def talk(version, level):
    port, seen, th = fake_server(version)
    c = Client(port=port)
    c.set_compression(level)
    c.insert_series(1, [1000, 999], [1.0, 2.0])
    c.insert_series_values(1, [1000, 999], [0x0102, -1], value_type="int64")
    c.close()
    th.join(5)
    return seen


def test_old_servers_get_plain_arrays():
    seen = talk(2, 1)
    assert [op for op, _ in seen] == [1, 4, 14]
    assert seen[2][1][9] == 0  # flags: plain


def test_level_zero_does_not_ask():
    assert [op for op, _ in talk(3, 0)] == [4, 14]


def test_new_servers_get_packed_frames():
    seen = talk(3, 1)
    assert [op for op, _ in seen] == [1, 14, 14]
    p = seen[2][1]
    assert p[:10] == bytes([2, 0, 0, 0, 1, 0, 0, 0, 2, 2])  # n, series, int64, PACKED
    zstd = pytest.importorskip("compression.zstd")  # Python 3.14+
    raw_len = struct.unpack("<I", p[16:20])[0]
    raw = zstd.decompress(p[20:])
    assert len(raw) == raw_len
    assert raw == bytes([0xD0, 0x0F, 0x01, 0x02, 0xFF, 0x01, 0xFF] + [0x00, 0xFF] * 6)
