# SPDX-License-Identifier: AGPL-3.0-only
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
import sys
import tempfile
import threading
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
# test_embedded.py runs the network client's tests (test_client.py and others) on a Db: they are imported from here
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / "python" / "tests"))
TSDBD = os.environ.get("TSDBD", str(ROOT / "build" / "tsdbd"))


def user_cli(dir, password, *args):
    """`tsdb DIR user ARGS...` as an operator runs it beside tsdbd (USERS only), the password in TSDB_PASSWORD."""
    cli = str(pathlib.Path(TSDBD).with_name("tsdb"))
    env = {k: v for k, v in os.environ.items() if k not in ("LD_PRELOAD", "TSDB_PASSWORD")}
    if password is not None:
        env["TSDB_PASSWORD"] = password
    r = subprocess.run([cli, str(dir), "user", *args], stdin=subprocess.DEVNULL, capture_output=True, text=True,
                       env=env)
    if r.returncode != 0:
        raise RuntimeError(f"tsdb user {args}: {r.stderr}")


def plain(x):
    """Results as plain values: the classes of tsdb_client and tsdb_embedded (Bucket, Buckets, ...) are distinct types
    of the same shape and do not compare equal with each other, so a comparison across the two goes through this."""
    if isinstance(x, (list, tuple)):
        return type(x)(plain(i) for i in x) if not hasattr(x, "_fields") else tuple(plain(i) for i in x)
    name = type(x).__name__
    if name in ("Bucket", "TwaBucket", "Stats", "QueryStats"):
        return repr(x)
    if name in ("Buckets", "TwaBuckets", "BucketsMany", "Many", "LatestMany"):
        return [plain(i) for i in x]
    return x


class Server:
    def __init__(self, base, dir=None, root_password=None, root="root"):
        """Serves dir (an existing database: test_embedded), or a new directory under base. With root_password the
        server requires a login, and USERS holds the superuser root (a name) with that password (made by the CLI);
        without, it runs with --no-auth."""
        own = pathlib.Path(tempfile.mkdtemp(prefix="tsdb-py-", dir=base))
        self.dir = pathlib.Path(dir) if dir else own / "db"
        pf = own / "port"
        if root_password is not None:
            user_cli(self.dir, root_password, "add", root, "--superuser")
        self.proc = subprocess.Popen(
            # --exit-with-parent: a test run killed by a timeout leaves no server behind
            [TSDBD, "--dir", str(self.dir), "--port", "0", "--port-file", str(pf), "--no-fsync", "--workers", "4",
             "--exit-with-parent=%d" % os.getpid()]
            + ([] if root_password is not None else ["--no-auth"]),
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


ROOT_PASSWORD = "root-secret"


@pytest.fixture
def server():
    yield from serve()


@pytest.fixture
def auth_server():
    """A server that requires a login; root (a superuser) has ROOT_PASSWORD."""
    yield from serve(ROOT_PASSWORD)


@pytest.fixture
def readme_server():
    """A server set up as the README's quick start does: the superuser admin, its password (ROOT_PASSWORD) in the
    file srv.password_file."""
    for srv in serve(ROOT_PASSWORD, root="admin"):
        srv.password_file = srv.dir.parent / "password"
        srv.password_file.write_text(ROOT_PASSWORD + "\n")
        yield srv


def serve(root_password=None, root="root"):
    if not os.access(TSDBD, os.X_OK):
        pytest.fail(f"{TSDBD} is missing: run make in the repository root")
    base = os.environ.get("TSDB_TEST_DIR")
    srv = Server(base, root_password=root_password, root=root)
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
