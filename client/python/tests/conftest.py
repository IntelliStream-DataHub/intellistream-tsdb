"""Starts build/tsdbd for each test: a free port and a fresh data directory.

The binary is $TSDBD or ../../build/tsdbd (run make in the repository root);
data goes under $TSDB_TEST_DIR or the system temp directory. The server is
ready once it prints its "serving" line, which comes after it has bound the
port: a successful connect alone could reach another server on that port.
"""

import os
import pathlib
import shutil
import signal
import socket
import subprocess
import tempfile
import threading

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
TSDBD = os.environ.get("TSDBD", str(ROOT / "build" / "tsdbd"))


class Server:
    def __init__(self, base):
        self.dir = pathlib.Path(tempfile.mkdtemp(prefix="tsdb-py-", dir=base)) / "db"
        log = []
        for _ in range(10):
            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                self.port = s.getsockname()[1]
            self.proc = subprocess.Popen(
                [TSDBD, "--dir", str(self.dir), "--port", str(self.port), "--no-fsync", "--workers", "4"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            for line in self.proc.stderr:
                log.append(line)
                if "serving" in line:
                    threading.Thread(target=self._drain, daemon=True).start()
                    return
            self.proc.wait()  # exited, most likely because the port was taken
        raise RuntimeError("tsdbd did not start:\n" + "".join(log))

    def _drain(self):
        for _ in self.proc.stderr:
            pass

    def stop(self):
        """SIGTERM, then the exit status."""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout=30)


@pytest.fixture
def server():
    if not os.access(TSDBD, os.X_OK):
        pytest.fail(f"{TSDBD} is missing: run make in the repository root")
    base = os.environ.get("TSDB_TEST_DIR")
    srv = Server(base)
    try:
        yield srv
    finally:
        status = srv.stop()
        shutil.rmtree(srv.dir.parent, ignore_errors=True)
    assert status == 0, f"tsdbd exited with {status}"


@pytest.fixture
def client(server):
    import tsdb_client

    with tsdb_client.Client("127.0.0.1", server.port) as c:
        yield c
