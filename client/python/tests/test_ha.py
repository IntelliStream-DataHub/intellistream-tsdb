"""Protocol 13 (docs/ha.md 9 and 15.4): the bodies of UNAVAILABLE, MOVED, PARTIAL and DROPPED as exceptions with
attributes, retry after retry_after within a time budget, MOVED followed to the node it names, several nodes in a
connect string, TOPOLOGY. The refusals come from fake servers (a script per test); the outage is real: tsdbd on a
storage domain that outagefs takes away (build/outage_server, tests/outage_server.c)."""

import os
import pathlib
import socket
import struct
import subprocess
import threading
import time

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError, TsdbUnavailable, TsdbMoved, TsdbPartial, TsdbDropped

from conftest import TSDBD

WHY_NFS_DOWN = 1
OP_FLUSH = 8
SID = 0x1_0000_0001


def unavail_entry(vol, retry_after_ms):
    return struct.pack("<HBBIqIQ", vol, WHY_NFS_DOWN, 0, 7, 123_456, retry_after_ms, 99)


def unavailable_body(vol, retry_after_ms):
    return struct.pack("<H", 1) + unavail_entry(vol, retry_after_ms)


def moved_body(addr):
    a = addr.encode()
    return struct.pack("<HHIBIH", 1, 2, 5, 0, 7, len(a)) + a


class Fake:
    """Listens on a port of its own; every connection is answered by script(op, nth) -> (status, body), HELLO
    excepted (protocol 13, no login)."""

    def __init__(self, script):
        self.script = script
        self.log = []
        self.lock = threading.Lock()
        self.lst = socket.socket()
        self.lst.bind(("127.0.0.1", 0))
        self.lst.listen(8)
        self.port = self.lst.getsockname()[1]
        self.addr = f"127.0.0.1:{self.port}"
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                s, _ = self.lst.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(s,), daemon=True).start()

    def serve(self, s):
        f = s.makefile("rb")
        try:
            while hdr := f.read(9):
                length, rid, op = struct.unpack("<IIB", hdr)
                f.read(length - 5)
                if op == 1:
                    status, body = 0, struct.pack("<IHH", 13, 0, 0)
                else:
                    with self.lock:
                        nth = sum(1 for o, _ in self.log if o == op)
                        self.log.append((op, rid))
                    status, body = self.script(op, nth)
                s.sendall(struct.pack("<IIBi", 9 + len(body), rid, 0, status) + body)
        except OSError:
            pass
        finally:
            s.close()

    def seen(self, op):
        with self.lock:
            return [rid for o, rid in self.log if o == op]

    def close(self):
        self.lst.close()


@pytest.fixture
def fake():
    made = []

    def make(script):
        f = Fake(script)
        made.append(f)
        return f

    yield make
    for f in made:
        f.close()


def unavailable(ms):
    return -11, unavailable_body(1, ms)


def test_unavailable_is_retried_after_retry_after(fake):
    f = fake(lambda op, nth: unavailable(300) if nth == 0 else (0, b""))
    with Client("127.0.0.1", f.port) as c:
        t0 = time.monotonic()
        c.flush()
        # the server asked for 300 ms, so the request goes again no sooner (mutation "ignore retry_after")
        assert 0.28 <= time.monotonic() - t0 < 5
        seen = f.seen(OP_FLUSH)
        assert len(seen) == 2 and seen[0] == seen[1], "the same request again"
        c.flush()


def test_the_exception_carries_the_body(fake):
    f = fake(lambda op, nth: unavailable(5))
    with Client("127.0.0.1", f.port, retry=0) as c:
        with pytest.raises(TsdbUnavailable) as e:
            c.flush()
    x = e.value
    assert isinstance(x, TsdbError) and x.code == tsdb_client.ERR_UNAVAILABLE == -11
    assert x.has_body, "the body was dropped"
    assert x.retry_after == 0.005
    assert x.entries == [
        {"volume": 1, "reason": WHY_NFS_DOWN, "epoch": 7, "since_ms": 123_456, "retry_after": 0.005, "outage_id": 99}
    ]
    assert len(f.seen(OP_FLUSH)) == 1


def test_the_budget_runs_out(fake):
    f = fake(lambda op, nth: unavailable(400))
    with Client("127.0.0.1", f.port, retry=1.5) as c:
        t0 = time.monotonic()
        with pytest.raises(TsdbUnavailable) as e:
            c.flush()
        assert 1.0 <= time.monotonic() - t0 < 4
        assert 3 <= len(f.seen(OP_FLUSH)) <= 5
        assert e.value.entries[0]["retry_after"] == 0.4
        # a refusal is an answer: the connection is fine
        with pytest.raises(TsdbUnavailable):
            c.set_retry(0)
            c.flush()


def test_a_refusal_without_a_body_waits_a_second(fake):
    f = fake(lambda op, nth: (-11, b"") if nth == 0 else (0, b""))
    with Client("127.0.0.1", f.port) as c:
        t0 = time.monotonic()
        c.flush()
        assert time.monotonic() - t0 >= 0.95
    f2 = fake(lambda op, nth: (-11, b""))
    with Client("127.0.0.1", f2.port, retry=0) as c:
        with pytest.raises(TsdbUnavailable) as e:
            c.flush()
        assert not e.value.has_body and e.value.entries == []


def test_retry_must_make_sense():
    for bad in (-1.0, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            Client(retry=bad)


def test_moved_is_followed_to_the_node_it_names(fake):
    b = fake(lambda op, nth: (0, b""))
    a = fake(lambda op, nth: (-10, moved_body(b.addr)))
    with Client("127.0.0.1", a.port) as c:
        c.flush()
        assert len(a.seen(OP_FLUSH)) == 1 and len(b.seen(OP_FLUSH)) == 1, "the request went to the new owner"
        c.flush()
        assert len(a.seen(OP_FLUSH)) == 1 and len(b.seen(OP_FLUSH)) == 2


def test_moved_is_an_exception_when_there_is_nowhere_to_go(fake):
    a = fake(lambda op, nth: (-10, moved_body("")))
    with Client("127.0.0.1", a.port) as c:
        with pytest.raises(TsdbMoved) as e:
            c.flush()
    assert e.value.code == tsdb_client.ERR_MOVED
    assert e.value.moved == [{"volume": 2, "epoch": 5, "node_id": 7, "addr": ""}]


def test_connect_nodes_skips_a_dead_node(fake):
    dead = socket.socket()
    dead.bind(("127.0.0.1", 0))
    dead_addr = f"127.0.0.1:{dead.getsockname()[1]}"
    dead.close()
    b = fake(lambda op, nth: (0, b""))
    for _ in range(4):
        with Client.connect_nodes(f"{dead_addr}, {b.addr}") as c:
            c.flush()
    assert len(b.seen(OP_FLUSH)) == 4
    with pytest.raises(TsdbError):
        Client.connect_nodes(dead_addr)
    for bad in ("", "a:0"):
        with pytest.raises(ValueError):
            Client.connect_nodes(bad)


def test_partial_and_dropped_are_exceptions(fake):
    def script(op, nth):
        if nth == 0:
            body = struct.pack("<H", 2) + struct.pack("<HhHh", 1, 0, 3, -11) + struct.pack("<I", 10) + bytes([0x24, 0x01])
            return -12, body + unavailable_body(3, 100)
        return -13, struct.pack("<Hq", 5, 1_700_000_000_999)

    f = fake(script)
    with Client("127.0.0.1", f.port) as c:
        # neither is retried: PARTIAL is partly applied, DROPPED is final
        with pytest.raises(TsdbPartial) as e:
            c.flush()
        assert e.value.volumes == [(1, 0), (3, -11)] and e.value.rows == 10
        assert e.value.not_applied == [2, 5, 8]
        with pytest.raises(TsdbDropped) as e:
            c.flush()
        assert (e.value.volume, e.value.before_ms) == (5, 1_700_000_000_999)
        assert len(f.seen(OP_FLUSH)) == 2


def test_topology_of_a_single_server(server):
    with Client("127.0.0.1", server.port) as c:
        t = c.topology()
        assert len(t["nodes"]) == 1 and t["nodes"][0]["alive"]
        assert t["volumes"]
        for v in t["volumes"]:
            assert (v["role"], v["health"]) == (1, 0)  # owner, available
        assert c.topology()["generation"] == t["generation"], "the generation names the answer"
        assert "=" in c.status()  # --no-auth checks nothing: every connection is a superuser's


def test_status_for_a_superuser(auth_server):
    from conftest import ROOT_PASSWORD

    with Client("127.0.0.1", auth_server.port, user="root", password=ROOT_PASSWORD) as c:
        assert "=" in c.status()
        assert c.topology()["volumes"][0]["path"] != ""


class Outage:
    """tests/outage_server.c: tsdbd with volume A on a domain that outagefs can take away."""

    def __init__(self, base):
        root = pathlib.Path(TSDBD).parent
        self.bin, self.lib = root / "outage_server", root / "liboutagefs.so"
        if not (self.bin.exists() and self.lib.exists()):
            pytest.skip(f"{self.bin} or {self.lib} is missing (make builds them on Linux)")
        import tempfile

        self.dir = pathlib.Path(tempfile.mkdtemp(prefix="tsdb-py-outage-", dir=base))
        env = {k: v for k, v in os.environ.items() if k != "LD_PRELOAD"}
        env.update(OUTAGEFS_LIB=str(self.lib), TSDBD=TSDBD)
        self.proc = subprocess.Popen([str(self.bin), str(self.dir)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     text=True, env=env)
        line = self.proc.stdout.readline().split()
        assert line and line[0] == "READY", line
        self.port, self.sid_a, self.sid_root, self.vol_a = (int(x) for x in line[1:5])

    def command(self, cmd):
        self.proc.stdin.write(cmd + "\n")
        self.proc.stdin.flush()
        assert self.proc.stdout.readline().strip() == "ok", cmd

    def close(self):
        try:
            self.proc.stdin.write("quit\n")
            self.proc.stdin.flush()
        except OSError:
            pass
        rc = self.proc.wait(timeout=60)
        import shutil

        shutil.rmtree(self.dir, ignore_errors=True)
        return rc


@pytest.fixture
def outage():
    o = Outage(os.environ.get("TSDB_TEST_DIR"))
    yield o
    assert o.close() == 0


def test_an_outage_gives_unavailable_with_its_body_and_the_client_retries_then_succeeds(outage):
    with Client("127.0.0.1", outage.port, retry=0) as c:
        c.insert_series(outage.sid_a, [1000], [1.0])
        assert all(v["health"] == 0 for v in c.topology()["volumes"])
        outage.command("down")
        with pytest.raises(TsdbUnavailable) as e:
            c.insert_series(outage.sid_a, [2000], [2.0])
        x = e.value
        assert x.has_body, "the body was dropped"
        u = x.entries[0]
        assert u["volume"] == outage.vol_a and u["reason"] != 0 and u["retry_after"] > 0 and u["since_ms"] > 0
        c.insert_series(outage.sid_root, [2001], [2.0])  # the root answers while A is away
        t = c.topology()
        a = next(v for v in t["volumes"] if v["volume"] == outage.vol_a)
        assert (a["health"], a["reason"] != 0) == (2, True)
        assert all(v["health"] == 0 for v in t["volumes"] if v["volume"] != outage.vol_a)

        # a budget shorter than the outage: the exception comes after about the budget
        with Client("127.0.0.1", outage.port, retry=1.2) as c2:
            t0 = time.monotonic()
            with pytest.raises(TsdbUnavailable):
                c2.insert_series(outage.sid_a, [3000], [3.0])
            assert time.monotonic() - t0 < 4

        # the default budget: the call waits, the volume heals, the call succeeds (once)
        took = []

        def late():
            with Client("127.0.0.1", outage.port) as c3:
                t0 = time.monotonic()
                c3.insert_series(outage.sid_a, [4000], [4.0])  # mutation "no retry": TsdbUnavailable
                took.append(time.monotonic() - t0)

        th = threading.Thread(target=late)
        th.start()
        time.sleep(1.5)
        outage.command("heal")
        th.join(60)
        assert took and 1.2 <= took[0] < 25, took

        # rows 1000 and 4000, not the refused 2000 and 3000
        n = 0
        for _ in range(100):
            try:
                n = len(c.query_raw(outage.sid_a, numpy=False)[0])
                break
            except TsdbError:
                time.sleep(0.1)
        assert n == 2
