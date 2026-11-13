# SPDX-License-Identifier: Apache-2.0
"""64-bit series ids on the wire (protocol version 11): the frames carry every bit of an id (checked on a fake server
that records what it receives), a server of an older protocol is refused when the client connects, and tsdbd, whose ids
carry the volume in bits 32-47, answers an id such as a + 2**32 (another volume) as an unknown series, never as a."""

import array
import socket
import struct
import threading

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError

T0 = 1_699_999_980_000
A = 0x1_0000_0005
B = 0xFFFF_FFFF_0000_0001
MANY = 2**64 - 1


def fake_server(version):
    """Answers HELLO with `version`, SERIES with A + 2, the lookups and QUERY_RAW with empty answers, anything else with
    OK; returns (port, hello payload list, seen ops and payloads, thread)."""
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    hello, seen = [], []

    def serve():
        s, _ = lst.accept()
        f = s.makefile("rb")
        while hdr := f.read(9):
            length, rid, op = struct.unpack("<IIB", hdr)
            payload = f.read(length - 5)
            body = {
                1: struct.pack("<IHH", version, 0, 0),  # no login, no mechanisms
                2: struct.pack("<Q", A + 2),
                3: bytes(2),  # an empty name
                5: bytes([1, 0, 0, 0, 0]),  # a raw chunk of no rows, the last
                13: bytes(1),  # a type
                28: bytes(8),  # no retention
            }.get(op, b"")
            s.sendall(struct.pack("<IIBi", 9 + len(body), rid, 0, 0) + body)
            (hello if op == 1 else seen).append(payload if op == 1 else (op, payload))
        s.close()
        lst.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    return lst.getsockname()[1], hello, seen, th


def requests(level, call):
    port, hello, seen, th = fake_server(tsdb_client.PROTOCOL_VERSION)
    c = Client(port=port)
    c.set_compression(level)
    call(c)
    c.close()
    th.join(5)
    assert hello == [struct.pack("<I", 15)]
    return seen


@pytest.mark.parametrize("version", [11, 10, 7, 3, 1, 0])
def test_a_server_older_than_12_is_refused_at_connect(version):
    port, hello, seen, th = fake_server(version)
    with pytest.raises(TsdbError) as e:
        Client(port=port)
    assert e.value.code == -3
    assert f"tsdbd speaks protocol {version}; this client needs 12: upgrade the server" in str(e.value)
    th.join(5)
    assert hello == [struct.pack("<I", 15)] and seen == []


def test_the_frames_carry_the_whole_id():
    seen = requests(
        0,
        lambda c: (
            c.series("x"),
            c.insert_series(A, [10, 20], [1.0, 2.0]),
            c.insert([A, B, A], [10, 20, 30], [1.0, 2.0, 3.0]),
            c.insert_series_values(B, [10, 20], [1, 2], value_type="int64"),
            c.insert_values(array.array("Q", [B, A]), [10, 20], [1, None], value_type="int64"),
            c.series_name(A),
            c.series_type(B),
            c.retention(A),
            c.delete_range(A, 5, 9),
            c.set_retention(B, 1000),
            c.query_raw(B, 100, 200, numpy=False),
        ),
    )
    assert [op for op, _ in seen] == [2, 4, 4, 14, 14, 3, 13, 28, 25, 27, 5]
    p = seen[1][1]
    assert len(p) == 16 + 2 * 8 + 2 * 4 and struct.unpack("<IIQ", p[:16]) == (2, 0, A)
    p = seen[2][1]  # three rows: the ids start after 4 bytes of padding
    assert len(p) == 16 + 3 * 8 + 3 * 4 + 4 + 3 * 8
    assert struct.unpack("<IIQ", p[:16]) == (3, 0, MANY) and p[52:56] == bytes(4)
    assert struct.unpack("<3Q", p[56:]) == (A, B, A)
    p = seen[3][1]
    assert p[:8] == bytes([2, 0, 0, 0, 2, 0, 0, 0])  # n, int64, flags, 0
    assert struct.unpack("<Q", p[8:16]) == (B,)
    p = seen[4][1]  # many series with a bitmap: ids after the bitmap padded to 8
    assert p[:8] == bytes([2, 0, 0, 0, 2, 1, 0, 0]) and struct.unpack("<Q", p[8:16]) == (MANY,)
    assert len(p) == 16 + 16 + 16 + 1 + 7 + 16 and struct.unpack("<2Q", p[56:]) == (B, A)
    assert seen[5][1] == struct.pack("<Q", A) and seen[6][1] == struct.pack("<Q", B) and seen[7][1] == seen[5][1]
    assert struct.unpack("<QQQ", seen[8][1]) == (A, 5, 9)
    assert struct.unpack("<QQ", seen[9][1]) == (B, 1000)
    assert struct.unpack("<QQQ", seen[10][1][:24]) == (B, 100, 200)


def unpack_ids(p, n):
    """The packed series column, independent of the packer: LEB128 varints, zigzag, wrapping sum."""
    prev, out, i = 0, [], 0
    for _ in range(n):
        v = shift = 0
        while True:
            b = p[i]
            i += 1
            v |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        prev = (prev + ((v >> 1) ^ -(v & 1))) % 2**64
        out.append(prev)
    assert i == len(p)
    return out


def packed_ids(ids):
    zstd = pytest.importorskip("compression.zstd")  # Python 3.14+
    n = len(ids)
    seen = requests(1, lambda c: c.insert_values(ids, list(range(1, n + 1)), [0] * n, value_type="int64"))
    p = seen[0][1]
    assert p[5] & 2  # PACKED
    assert struct.unpack("<Q", p[8:16]) == (MANY,)
    raw = zstd.decompress(p[20:])
    assert len(raw) == struct.unpack("<I", p[16:20])[0] <= n * (10 + 8) + n * 10
    return raw[n + 8 * n :]  # after the timestamp deltas and the value planes


def test_packed_ids_are_wrapping_64_bit_deltas():
    # up across 2**32, down across it, 0 -> 2**64 - 2 and back, and a jump of 2**63
    ids = [A, 3, MANY - 1, 0, 1 << 63, 0, B, B]
    col = packed_ids(ids)
    assert unpack_ids(col, len(ids)) == ids
    # +(2**32 + 5), -(2**32 + 2), -5 (3 -> 2**64 - 2 wraps), +2, then a jump of 2**63 takes ten bytes
    assert col[:16] == bytes([0x8A, 0x80, 0x80, 0x80, 0x20, 0x83, 0x80, 0x80, 0x80, 0x20, 0x09, 0x04, 0xFF, 0xFF, 0xFF, 0xFF])
    # 0 -> 2**64 - 2 is a delta of -2 and back +2
    assert packed_ids([MANY - 1, 0]) == bytes([0x03, 0x04])


def test_the_many_series_marker_is_not_an_id(client):
    with pytest.raises(TsdbError) as e:
        client.insert_series(MANY, [T0], [1.0])
    assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(OverflowError):
        client.insert_series(2**64, [T0], [1.0])
    a = client.series("m")
    with pytest.raises(TsdbError) as e:  # inside a per-row array it is an unknown series
        client.insert([a, MANY], [T0, T0], [1.0, 2.0])
    assert e.value.code == tsdb_client.ERR_INVALID


def test_tsdbd_answers_a_wide_id_as_unknown(client):
    a = client.series_create("wide", "int64")
    ts = [T0, T0 + 1000, T0 + 2000]
    client.insert_series_values(a, ts, [1, 2, 3])
    st = client.series_create("wide.state", "state")
    assert client.state(st, "up") == 1
    # ids that share a's low 32 bits and ones that share nothing
    for w in (a + 2**32, a | 0xFFFF_FFFF_0000_0000, a + 2**63):
        for level in (1, 0):
            client.set_compression(level)

            def code(call, *args, **kw):
                with pytest.raises(TsdbError) as e:
                    call(*args, **kw)
                return e.value.code

            assert code(client.insert_series_values, w, ts, [9, 9, 9], value_type="int64") == tsdb_client.ERR_INVALID
            assert code(client.insert_values, [a, w, a], ts, [9, 9, 9], value_type="int64") == tsdb_client.ERR_INVALID
            assert code(client.series_name, w) == tsdb_client.ERR_NOTFOUND
            assert code(client.series_type, w) == tsdb_client.ERR_NOTFOUND
            assert code(client.retention, w) == tsdb_client.ERR_NOTFOUND
            assert code(client.state, w, "up") in (tsdb_client.ERR_NOTFOUND, tsdb_client.ERR_INVALID)
            assert code(client.delete_range, w, None, None) in (tsdb_client.ERR_NOTFOUND, tsdb_client.ERR_INVALID)
            assert code(client.set_retention, w, 1000) in (tsdb_client.ERR_NOTFOUND, tsdb_client.ERR_INVALID)
            if w >> 48 or not w >> 32:
                # no volume can have it: queries say so rather than answer an empty range
                assert code(client.query_values, w, numpy=False) == tsdb_client.ERR_INVALID
                assert code(client.query_agg, w, None, None, 0) == tsdb_client.ERR_INVALID
            else:
                # a well formed id of a volume this server does not have: no rows
                assert len(client.query_values(w, numpy=False)[0]) == 0
            # nothing above touched series a, whose low 32 bits the wide ids share
            assert client.query_values(a, numpy=False) == (ts, [1, 2, 3])
            assert client.retention(a) == 0
            assert client.state_lookup(st, "up") == 1
