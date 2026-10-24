"""Timeouts against fake servers that stall: no tsdbd here, each test runs a
listener that accepts and then misbehaves in one way. The call must fail with
TsdbError (ERR_IO) within bounds and the client must be closed afterwards."""

import array
import os
import faulthandler
import socket
import sys
import struct
import threading
import time

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError

TIMEOUT = 0.3
BOUND = 5.0  # a call taking longer than this is a hang


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def read_request(s):
    """(id, op) of the next request frame."""
    (length,) = struct.unpack("<I", recv_exact(s, 4))
    body = recv_exact(s, length)
    return struct.unpack("<I", body[:4])[0], body[4]


def ack(req_id, payload=b""):
    return struct.pack("<IIBi", 9 + len(payload), req_id, 0, 0) + payload


@pytest.fixture(autouse=True)
def watchdog():
    """A regression in the timeouts is a hang: end the run with a traceback instead of stalling."""
    faulthandler.dump_traceback_later(60, exit=True)
    yield
    faulthandler.cancel_dump_traceback_later()


class Fake:
    """Runs behave(conn, stop) on the first connection; closing it releases the connection."""

    def __init__(self, behave):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.behave = behave
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        try:
            conn, _ = self.listener.accept()
        except OSError:
            return
        with conn:
            try:
                self.behave(conn, self.stop)
            except (EOFError, OSError):
                pass

    def close(self):
        self.stop.set()
        self.listener.close()
        self.thread.join(5)


@pytest.fixture
def fake():
    made = []

    def make(behave):
        f = Fake(behave)
        made.append(f)
        return f

    yield make
    for f in made:
        f.close()


def assert_timed_out(c, started, call):
    with pytest.raises(TsdbError) as e:
        call()
    took = time.monotonic() - started
    assert e.value.code == tsdb_client.ERR_IO
    assert TIMEOUT - 0.05 <= took < BOUND, f"returned after {took:.2f} s"
    assert repr(c).endswith("closed>")
    again = time.monotonic()
    with pytest.raises(TsdbError) as e:
        c.series("x")
    assert e.value.code == tsdb_client.ERR_IO
    assert time.monotonic() - again < 0.1, "a closed client must fail fast"


def test_server_that_never_answers(fake):
    def behave(s, stop):
        read_request(s)
        stop.wait()

    c = Client(port=fake(behave).port, timeout=TIMEOUT)
    assert c.timeout == pytest.approx(TIMEOUT)
    t = time.monotonic()
    assert_timed_out(c, t, lambda: c.series("a"))


def test_server_that_accepts_and_never_reads(fake):
    c = Client(port=fake(lambda s, stop: stop.wait()).port, timeout=TIMEOUT)
    t = time.monotonic()
    assert_timed_out(c, t, lambda: c.series("a"))


def test_server_that_answers_half_a_frame(fake):
    def behave(s, stop):
        req_id, _ = read_request(s)
        s.sendall(ack(req_id)[:6])
        stop.wait()

    c = Client(port=fake(behave).port, timeout=TIMEOUT)
    t = time.monotonic()
    assert_timed_out(c, t, lambda: c.series("a"))


def test_server_that_stops_answering_among_pipelined_inserts(fake):
    def behave(s, stop):
        for _ in range(5):
            req_id, op = read_request(s)
            # HELLO is answered with protocol version 2: plain arrays follow
            s.sendall(ack(req_id, struct.pack("<I", 2)) if op == 1 else ack(req_id))
        stop.wait()

    c = Client(port=fake(behave).port, timeout=TIMEOUT)
    ts = array.array("q", [1, 2, 3])
    values = array.array("f", [1.0, 2.0, 3.0])
    for _ in range(20):
        c.insert_async([1] * len(ts), ts, values)
    t = time.monotonic()
    assert_timed_out(c, t, c.wait)


def test_server_that_stops_reading_among_pipelined_inserts(fake):
    def behave(s, stop):
        for _ in range(10):
            req_id, op = read_request(s)
            s.sendall(ack(req_id, struct.pack("<I", 2)) if op == 1 else ack(req_id))
        stop.wait()  # the socket buffers fill up and a send blocks

    c = Client(port=fake(behave).port, timeout=TIMEOUT)
    n = 200_000
    ts = array.array("q", range(n))
    values = array.array("f", range(n))
    t = time.monotonic()
    sent = 0
    with pytest.raises(TsdbError) as e:
        while sent < 1000 and time.monotonic() - t < BOUND:
            c.insert_async([1] * len(ts), ts, values)
            sent += 1
    assert e.value.code == tsdb_client.ERR_IO
    assert sent >= 9, sent
    assert time.monotonic() - t < BOUND
    assert repr(c).endswith("closed>")
    with pytest.raises(TsdbError):
        c.wait()


def test_set_timeout_and_validation(fake):
    c = Client(port=fake(lambda s, stop: stop.wait()).port)
    assert c.timeout is None
    c.set_timeout(7)
    assert c.timeout == 7
    c.set_timeout(None)
    assert c.timeout is None
    for bad in (0, -1, float("nan"), float("inf"), 1e20, 1e300, 1e-12):
        with pytest.raises(ValueError):
            c.set_timeout(bad)
        with pytest.raises(ValueError):
            Client(port=1, timeout=bad)
    c.set_timeout(TIMEOUT)  # on a live client
    t = time.monotonic()
    assert_timed_out(c, t, lambda: c.series("a"))
    with pytest.raises(TsdbError):
        c.timeout
    with pytest.raises(TsdbError):
        c.set_timeout(1)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="relies on how Linux drops SYNs for a full queue")
def test_connect_gives_up():
    # a listener whose accept queue is full drops further SYNs: the connect hangs on any machine
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(0)
    port = listener.getsockname()[1]
    held = []
    try:
        for _ in range(5):
            s = socket.socket()
            held.append(s)
            s.settimeout(0.2)
            try:
                s.connect(("127.0.0.1", port))
            except OSError:
                break
        else:
            pytest.fail("the accept queue did not fill up")
        t = time.monotonic()
        with pytest.raises(TsdbError) as e:
            Client(port=port, timeout=0.3)
        took = time.monotonic() - t
        assert e.value.code == tsdb_client.ERR_IO
        assert 0.25 <= took < BOUND, f"{took:.2f} s"
    finally:
        for s in held:
            s.close()
        listener.close()


def keepalive_timer_armed(port, server_port, expected, settle=0.5):
    """Whether the kernel runs a keepalive timer on the local socket with this port (Linux).

    /proc/net/tcp shows one timer per socket: right after the client sends, it is the
    retransmit timer (01) until the peer's ACK arrives, and only then keepalive (02). So poll:
    when keepalive is expected, until 02 shows; when it is not, that 02 never shows. Only the
    established socket to the fake server counts: an older socket in TIME_WAIT can hold the same
    local port."""
    deadline = time.monotonic() + settle
    seen = False
    while True:
        with open("/proc/net/tcp") as f:
            rows = [l.split() for l in f.read().splitlines()[1:]]
        mine = [r for r in rows if int(r[1].split(":")[1], 16) == port and int(r[2].split(":")[1], 16) == server_port
                and r[3] == "01"]  # ESTABLISHED
        if not mine:
            raise AssertionError(f"no socket with local port {port}")
        armed = mine[0][5].split(":")[0] == "02"
        seen = seen or armed
        if expected and armed:
            return True
        if time.monotonic() >= deadline:
            return seen
        time.sleep(0.01)


@pytest.mark.skipif(not os.path.exists("/proc/net/tcp"), reason="reads the kernel's socket table")
def test_keepalive_is_on_by_default_and_can_be_turned_off(fake):
    for kwargs, expected in (({}, True), ({"keepalive": True}, True), ({"keepalive": False}, False)):
        peers = []
        f = fake(lambda s, stop: (peers.append(s.getpeername()[1]), stop.wait()))
        c = Client(port=f.port, **kwargs)
        deadline = time.monotonic() + BOUND
        while not peers and time.monotonic() < deadline:
            time.sleep(0.01)
        assert peers, "the fake server saw no connection"
        assert keepalive_timer_armed(peers[0], f.port, expected) is expected
        c.close()

