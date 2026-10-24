"""Starts build/tsdbd for each test: a port tsdbd chose and a fresh data directory.

The binary is $TSDBD or ../../build/tsdbd (run make in the repository root);
data goes under $TSDB_TEST_DIR or the system temp directory. The server is
ready once it has written its port file (--port 0 --port-file: "PORT PID"); the
pid must be the child's, so a test never talks to a server it did not start.
"""

import os
import pathlib
import shutil
import signal
import subprocess
import tempfile
import threading
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
TSDBD = os.environ.get("TSDBD", str(ROOT / "build" / "tsdbd"))


class Server:
    def __init__(self, base, dir=None):
        """Serves dir (an existing database: test_embedded), or a new directory under base."""
        own = pathlib.Path(tempfile.mkdtemp(prefix="tsdb-py-", dir=base))
        self.dir = pathlib.Path(dir) if dir else own / "db"
        pf = own / "port"
        self.proc = subprocess.Popen(
            [TSDBD, "--dir", str(self.dir), "--port", "0", "--port-file", str(pf), "--no-fsync", "--workers", "4"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        log = []

        def collect():
            for line in self.proc.stderr:
                log.append(line)

        t = threading.Thread(target=collect, daemon=True)
        t.start()
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                t.join(5)
                raise RuntimeError(f"tsdbd exited with {self.proc.returncode} before it listened:\n" + "".join(log))
            try:
                port, pid = (int(x) for x in pf.read_text().split())
            except (OSError, ValueError):
                time.sleep(0.01)
                continue
            if pid != self.proc.pid:
                self.proc.kill()
                self.proc.wait()
                raise RuntimeError(f"{pf} names pid {pid}, but the server started here is pid {self.proc.pid}")
            self.port = port
            return
        self.proc.kill()
        self.proc.wait()
        raise RuntimeError("tsdbd did not write its port file in 30 s:\n" + "".join(log))

    def add_volume(self, path):
        """Adds a volume as an operator does (tsdbd never creates one for a client): the tsdb CLI beside tsdbd."""
        cli = str(pathlib.Path(TSDBD).with_name("tsdb"))
        env = {k: v for k, v in os.environ.items() if k != "LD_PRELOAD"}
        r = subprocess.run([cli, str(self.dir), "volume", "add", str(path)], capture_output=True, text=True, env=env)
        if r.returncode != 0:
            raise RuntimeError(f"volume add {path}: {r.stderr}")

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
