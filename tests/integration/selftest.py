#!/usr/bin/env python3
"""Tests of the harness itself (seconds, no clients needed): the places where run.py and mutate.py could turn a
failure into a pass.

  selftest.py

  uring probe   a broken io_uring backend must FAIL the run; only tsdbd's own "io_uring is not offered" is a skip
  signals       SIGTERM while the main thread holds the process-list lock must not deadlock the driver
  verdicts      mutate.py counts a crashed driver or a hang as a problem, never as a caught bug
  float sums    a float sum whose NaN-ness depends on the order of additions is neither compared between layouts
                nor predicted by the model; one that is NaN in every order is compared bit for bit

Needs build/tsdbd and build/tsdb (make).
"""
import os
import signal
import stat
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))

failures = []


def check(ok, what):
    print("  %s %s" % ("ok  " if ok else "FAIL", what), flush=True)
    if not ok:
        failures.append(what)


def fake_bin(tmp, uring_script):
    """A bin dir whose tsdbd is `uring_script` under --io uring and the real server otherwise."""
    b = Path(tmp) / "bin"
    b.mkdir()
    (b / "tsdb").symlink_to(ROOT / "build/tsdb")
    sh = b / "tsdbd"
    sh.write_text('#!/bin/sh\nfor a in "$@"; do [ "$a" = uring ] && { %s; }; done\nexec %s "$@"\n'
                  % (uring_script, ROOT / "build/tsdbd"))
    sh.chmod(sh.stat().st_mode | stat.S_IXUSR)
    return b


def run_py(bin_dir, tmp, io="uring,epoll"):
    p = subprocess.run([sys.executable, str(HERE / "run.py"), "--bin-dir", str(bin_dir), "--only", "cli", "--io", io,
                        "--logs", str(Path(tmp) / "logs"), "--work", str(tmp), "--scale", "0.2"],
                       capture_output=True, text=True, timeout=300)
    return p.returncode, p.stdout + p.stderr


def test_uring_probe():
    print("uring probe")
    with tempfile.TemporaryDirectory(prefix="integ-self-") as tmp:
        b = fake_bin(tmp, 'echo "tsdbd: io_uring: Operation not permitted" >&2; exit 1')
        rc, out = run_py(b, tmp)
        check(rc == 0 and "uring SKIPPED" in out, "tsdbd saying io_uring is not permitted: skipped, run passes (rc %d)" % rc)
    with tempfile.TemporaryDirectory(prefix="integ-self-") as tmp:
        b = fake_bin(tmp, 'echo "tsdbd: segmentation fault (made up)" >&2; exit 1')
        rc, out = run_py(b, tmp)
        check(rc != 0 and "uring probe" in out, "a uring server that dies for another reason: the run FAILS (rc %d)" % rc)
    with tempfile.TemporaryDirectory(prefix="integ-self-") as tmp:
        b = fake_bin(tmp, "exit 3")
        rc, out = run_py(b, tmp)
        check(rc != 0 and "uring SKIPPED" not in out, "a uring server that exits silently: the run FAILS (rc %d)" % rc)


def test_signal_while_locked():
    print("signals")
    code = textwrap.dedent("""
        import signal, sys
        sys.path.insert(0, %r)
        import run
        with run._lock:                       # the main thread is inside spawn() when the signal arrives
            signal.raise_signal(signal.SIGTERM)
        print("handler returned")             # never reached: the handler exits with 130
        """ % str(HERE))
    try:
        p = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, timeout=30)
        check(p.returncode == 130, "SIGTERM with the process-list lock held exits 130 (rc %d)" % p.returncode)
    except subprocess.TimeoutExpired:
        check(False, "SIGTERM with the process-list lock held deadlocked the driver")


def test_signal_during_spawn():
    code = textwrap.dedent("""
        import os, signal, subprocess, sys
        sys.path.insert(0, %r)
        import run
        real = subprocess.Popen
        def popen(*a, **k):                   # the signal arrives right after the fork, before the child is listed
            p = real(*a, **k)
            open(%r, "w").write(str(p.pid))
            signal.raise_signal(signal.SIGTERM)
            return p
        subprocess.Popen = popen
        run.spawn(["sleep", "60"])
        print("not reached")
        """)
    with tempfile.TemporaryDirectory(prefix="integ-self-") as tmp:
        pidf = Path(tmp) / "pid"
        try:
            p = subprocess.run([sys.executable, "-I", "-c", code % (str(HERE), str(pidf))], capture_output=True, timeout=60)
        except subprocess.TimeoutExpired:
            check(False, "SIGTERM right after a fork hung the driver")
            return
        pid = int(pidf.read_text()) if pidf.exists() else 0
        alive = False
        if pid > 1:
            try:
                os.kill(pid, 0)
                alive = True
                os.kill(pid, signal.SIGKILL)  # do not leave it behind
            except ProcessLookupError:
                pass
        check(p.returncode == 130 and pid > 1 and not alive,
              "SIGTERM right after a fork: exit 130 and the child is killed, not orphaned (rc %d, child alive: %s)"
              % (p.returncode, alive))


def test_verdicts():
    print("verdicts")
    import mutate
    line = "[matrix epoll] raw x: c and java differ"
    cases = [
        ((1, line), "CAUGHT"),
        ((0, ""), "SURVIVED"),
        ((1, "[matrix epoll] scenario crashed: NameError: name 'ref' is not defined"), "HARNESS"),
        ((1, "Traceback (most recent call last):\n  File run.py\nKeyError"), "HARNESS"),
        ((1, "[matrix epoll] write: c writer failed (timeout after 40 s)"), "TIMEOUT"),
        (("timeout", ""), "TIMEOUT"),
        ((2, ""), "ERROR"),
        # build output that happens to start with a bracket is not a failure line
        ((1, "[1/3] cc -c x.c\n" + line), "CAUGHT"),
    ]
    for (rc, out), want in cases:
        got = mutate.classify(rc, out)[0]
        check(got == want, "rc %s %r -> %s (wanted %s)" % (rc, out[:50], got, want))
    # a bug that can only show as a lost reply is caught by the client timeout
    got = mutate.classify(1, "[matrix epoll] write: c writer failed (timeout after 40 s)", True)[0]
    check(got == "CAUGHT", "a timeout of a hang mutant -> %s (wanted CAUGHT)" % got)


def test_float_sums():
    print("float sums")
    import run
    import gen
    types = {"f": "float64"}
    big, nbig, inf, ninf, nan = "7fefffffffffffff", "ffefffffffffffff", "7ff0000000000000", "fff0000000000000", \
        "7ff8000000000000"

    def out(s, lo, hi):
        return "## agg f 0 100 10\n" + "\t".join(["0", "4", s, lo, hi, "0", "0", "0"]) + "\n"
    # DBL_MAX, DBL_MAX, -DBL_MAX, -DBL_MAX: +inf in the memtable, NaN once two parts hold +inf and -inf
    d = run.diff_outputs("memtable", out(inf, nbig, big), "parts", out(nan, nbig, big), types=types)
    check(d == [], "an overflowing sum is +inf in one layout and NaN in another: not a difference (%s)" % d)
    d = run.diff_outputs("a", out(nan, nan, nan), "b", out("7ff8000000000001", nan, nan), types=types)
    check(d != [], "a bucket of only NaNs: a NaN sum with another payload is a difference")
    d = run.diff_outputs("a", out(nan, ninf, inf), "b", out("fff8000000000000", ninf, inf), types=types)
    check(d != [], "a bucket with -inf and +inf: a NaN sum with another sign is a difference")
    d = run.diff_outputs("a", out(inf, nbig, big), "b", out(inf, nbig, inf), types=types)
    check(d != [], "a different max is still a difference")
    # +inf, -DBL_MAX, -DBL_MAX: +inf in one order, NaN in another (-DBL_MAX - DBL_MAX = -inf first)
    sm = gen.float_aggregates("float64", [inf, nbig, nbig])[0]
    check(sm is None, "the model does not predict a sum of an infinity and rows that can overflow (%s)" % (sm,))
    sm = gen.float_aggregates("float64", [inf, "3ff0000000000000"])[0]
    check(sm is not None and sm[1] == float("inf"), "the model predicts +inf for +inf and small rows (%s)" % (sm,))
    sm = gen.float_aggregates("float64", [big, ninf, inf])[0]
    check(sm is not None and sm[1] != sm[1], "the model predicts NaN for -inf and +inf (%s)" % (sm,))


def main():
    if not (ROOT / "build/tsdbd").exists() or not (ROOT / "build/tsdb").exists():
        print("build/tsdbd and build/tsdb are missing: run make", file=sys.stderr)
        return 2
    test_verdicts()
    test_float_sums()
    test_signal_while_locked()
    test_signal_during_spawn()
    test_uring_probe()
    print("\n%s" % ("%d selftest check(s) failed" % len(failures) if failures else "all selftest checks passed"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
