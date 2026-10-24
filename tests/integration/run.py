#!/usr/bin/env python3
"""End-to-end tests of tsdb: real tsdbd processes, real clients in four languages, and the tsdb CLI.

  run.py [--only matrix,cli,follower,durability,stale_wal] [--io uring,epoll] [--scale 1.0] [--seed N]
         [--logs DIR] [--keep] [--bin-dir DIR] [--itc PATH]

Scenarios (each runs once per server backend, --io uring and --io epoll; on a host without io_uring only epoll):

  matrix      Each client (C, Rust, Python, Java) writes its own set of series (every type, nulls, special floats,
              corrections, packed/plain/legacy and pipelined inserts); then every client reads every set back
              (raw both orders with limits, aggregates, buckets, calendar groups in UTC, Europe/Oslo,
              America/New_York, Asia/Kolkata and, with midnight clock changes, America/Havana and Asia/Beirut, explicit bounds, errors). The four answers must be identical to
              each other, to an independent model (gen.py) and, after tsdbd stopped, to `tsdb <dir> query`.
              Repeated after flush and after optimize (merges).
  cli         `tsdb create/import` a CSV (ms and ISO times, nan/inf/-0, nulls, a second import correcting the first),
              then all four clients read it through a server.
  follower    A second `tsdbd --follow` on the same directory catches up with a first wave and a second wave of
              writes and answers like the primary; it refuses writes. The primary then runs optimize while the
              follower is being read: the replaced parts are retired_*, and the follower must go on answering.
  stale_wal   A WAL segment left behind by a flush (made by hand: segments copied away, flushed, corrected, put
              back) is not replayed over the newer parts.
  durability  Every client runs a pipelined two-pass load (the second pass overwrites the first; batches are sent 8
              at a time and acknowledged together, so 0 to 7 batches are unacknowledged at the kill); tsdbd is
              killed with SIGKILL in the middle of pass 1 or pass 2; after the restart every acknowledged batch is
              there, nothing older came back, no batch is half there; again after optimize and after a clean
              restart. Each series is also read descending, whole and with a limit across the 65536-row window.

Why Python and not bash: the scenarios need binary-safe comparison, threads (four clients and a server at once,
a trigger that fires on the clients' acknowledgements), a model of the expected answers and zoneinfo as an
independent source of calendar boundaries; a shell script would shell out to Python for all of it. The standard
library is enough (no third-party modules, 3.9+).

Processes: every process this script starts is signalled through signal_proc(), which refuses pids <= 1 (kill(0)
and kill(-1) reach the whole process group or every process of the user), and only by the Popen object it
started. Nothing is ever killed by name. Ports are never computed: tsdbd --port 0 --port-file reports the one it got, and the pid it reports must be ours.
"""
import argparse
import atexit
import collections
import concurrent.futures
import copy
import datetime
import json
import os
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
BUILD = Path(os.environ.get("INTEG_BUILD", ROOT / "build"))
LANGS = ["c", "rust", "python", "java"]
I64_MIN, I64_MAX = gen.I64_MIN, gen.I64_MAX

# ---------------------------------------------------------------- processes

_procs = []
# re-entrant: the signal handler runs on the main thread, possibly while the main thread is inside spawn()
_lock = threading.RLock()


def signal_proc(p, sig):
    """Signals a process this script started. Refuses pid 0, -1 and 1: kill(0, s) reaches the whole process group
    and kill(-1, s) every process of the user, which is what a pid left at 0 or -1 by a failed start would do."""
    if p is None or not isinstance(p.pid, int) or p.pid <= 1:
        raise RuntimeError("refusing to signal pid %r" % (getattr(p, "pid", None),))
    if p.poll() is None:
        try:
            p.send_signal(sig)
        except ProcessLookupError:
            pass


_defer = threading.local()


def spawn(cmd, **kw):
    """Starts a child and records it. A SIGINT/SIGTERM that the handler would see between the fork and the
    bookkeeping on this thread is held back until the child is listed (else it would be orphaned). Not a signal mask:
    that would be inherited by the child, and tsdbd would never see its SIGTERM."""
    with _lock:
        _defer.on, _defer.pending = True, None
        try:
            p = subprocess.Popen(cmd, **kw)
            _procs.append(p)
        finally:
            _defer.on = False
            pending, _defer.pending = _defer.pending, None
    if pending:
        _on_signal(*pending)
    return p


def cleanup():
    with _lock:
        procs = list(_procs)
    for p in procs:
        if p.poll() is None:
            signal_proc(p, signal.SIGKILL)
    for p in procs:
        try:
            p.wait(timeout=10)
        except Exception:
            pass


atexit.register(cleanup)


def _on_signal(sig, frame):
    if getattr(_defer, "on", False):  # the main thread is inside spawn(): spawn() calls us again when it is done
        _defer.pending = (sig, frame)
        return
    cleanup()
    os._exit(130)


signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)

def owns_port(pid, port):
    """True when process pid holds a listening TCP socket on port (read from /proc)."""
    inodes = set()
    for f in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = Path(f).read_text().splitlines()[1:]
        except OSError:
            continue
        for ln in lines:
            fld = ln.split()
            if len(fld) > 9 and fld[3] == "0A" and int(fld[1].rsplit(":", 1)[1], 16) == port:
                inodes.add("socket:[%s]" % fld[9])
    try:
        for fd in os.listdir("/proc/%d/fd" % pid):
            try:
                if os.readlink("/proc/%d/fd/%s" % (pid, fd)) in inodes:
                    return True
            except OSError:
                pass
    except OSError:
        pass
    return False


class Server:
    """A tsdbd on a port the kernel chose (--port 0 --port-file). start() waits for the port file, which tsdbd
    writes once it serves; it names the pid, which must be the process started here and the holder of the port.
    No port is ever computed here, and a server that did not start is an error, not a reason to try another."""

    def __init__(self, ctx, dirpath, log, io, follow=False, extra=()):
        self.ctx, self.dir, self.log, self.io, self.follow, self.extra = ctx, dirpath, Path(log), io, follow, list(extra)
        self.proc = None
        self.port = None

    def start(self):
        pf = Path(str(self.log) + ".port.%d" % time.monotonic_ns())
        cmd = [str(self.ctx.tsdbd), "--dir", str(self.dir), "--listen", "127.0.0.1", "--port", "0",
               "--port-file", str(pf), "--workers", "4", "--io", self.io] + \
              (["--follow", "--refresh-ms", "100"] if self.follow else []) + self.extra
        logf = open(self.log, "ab")
        logf.write(("$ " + " ".join(cmd) + "\n").encode())
        logf.flush()
        self.proc = spawn(cmd, stdin=subprocess.DEVNULL, stdout=logf, stderr=logf)
        logf.close()
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                tail = self.log.read_text(errors="replace").split("$ ")[-1]
                raise RuntimeError("tsdbd exited: %s" % tail.strip()[-300:])
            try:
                port, pid = (int(x) for x in pf.read_text().split())
            except (OSError, ValueError):
                time.sleep(0.02)
                continue
            why = None
            if pid != self.proc.pid:
                why = "%s names pid %d, but the server started here is pid %d" % (pf, pid, self.proc.pid)
            elif not owns_port(pid, port):
                why = "port %d is not held by the server started here (pid %d)" % (port, pid)
            if why:
                signal_proc(self.proc, signal.SIGKILL)
                self.proc.wait()
                raise RuntimeError(why)
            pf.unlink()
            self.port = port
            return self
        signal_proc(self.proc, signal.SIGKILL)
        self.proc.wait()
        raise RuntimeError("tsdbd did not announce itself in 30 s, see %s" % self.log)

    def stop(self):
        """SIGTERM and wait: a clean shutdown. Returns the exit status."""
        if self.proc.poll() is None:
            signal_proc(self.proc, signal.SIGTERM)
        try:
            return self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            signal_proc(self.proc, signal.SIGKILL)
            self.proc.wait()
            return "hung on SIGTERM"

    def kill9(self):
        signal_proc(self.proc, signal.SIGKILL)
        self.proc.wait()


# ---------------------------------------------------------------- the clients

class Ctx:
    def __init__(self, args):
        self.args = args
        bindir = Path(args.bin_dir) if args.bin_dir else BUILD
        self.tsdbd, self.tsdb = bindir / "tsdbd", bindir / "tsdb"
        ib = BUILD / "integ"
        self.itc = Path(args.itc) if args.itc else ib / "itc"
        venv = Path(os.environ.get("PYVENV", BUILD / "pyvenv"))
        jar = list((ROOT / "client/java/target").glob("tsdb-client-*.jar"))
        jarcp = ":".join([str(jar[0])] + [str(p) for p in sorted((ROOT / "client/java/target/lib").glob("*.jar"))]) if jar else ""
        self.cmd = {
            "c": [str(self.itc)],
            "rust": [str(ib / "rust/release/itr")],
            "python": [str(venv / "bin/python"), "-I", str(ROOT / "tests/integration/python/itp.py")],
            "java": ["java", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-cp", str(ib / "java") + ":" + jarcp, "Itj"],
        }
        self.logroot = Path(args.logs)
        self.work = Path(tempfile.mkdtemp(prefix="tsdb-integ-", dir=args.work))
        self.results = []

    def missing(self):
        need = [self.tsdbd, self.tsdb, self.itc, Path(self.cmd["rust"][0]), Path(self.cmd["python"][0]),
                BUILD / "integ/java/Itj.class"]
        return [str(p) for p in need if not p.exists()]


def run_client(ctx, lang, args, out, timeout=None):
    """Runs one client program; stdout to the file out (stderr next to it). Returns (rc, seconds)."""
    out = Path(out)
    timeout = timeout or ctx.args.timeout
    t0 = time.time()
    with open(out, "wb") as so, open(str(out) + ".err", "wb") as se:
        p = spawn(ctx.cmd[lang] + [str(a) for a in args], stdin=subprocess.DEVNULL, stdout=so, stderr=se)
        try:
            rc = p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            signal_proc(p, signal.SIGKILL)
            p.wait()
            rc = "timeout after %d s" % timeout
    return rc, time.time() - t0


def run_all(ctx, jobs, timeout=None):
    """jobs: {key: (lang, args, outfile)} run concurrently. Returns {key: (rc, secs)}."""
    with concurrent.futures.ThreadPoolExecutor(len(jobs)) as ex:
        futs = {k: ex.submit(run_client, ctx, lang, args, out, timeout) for k, (lang, args, out) in jobs.items()}
        return {k: f.result() for k, f in futs.items()}


def err_tail(path, n=300):
    try:
        return Path(str(path) + ".err").read_text(errors="replace").strip()[-n:]
    except OSError:
        return ""


# ---------------------------------------------------------------- scenario plumbing

class Fails:
    def __init__(self, name, io):
        self.name, self.io, self.items, self.notes = name, io, [], []

    def add(self, msg):
        msg = msg if len(msg) < 700 else msg[:700] + " ..."
        self.items.append(msg)
        print("    FAIL: " + msg.splitlines()[0][:300], flush=True)

    def note(self, msg):
        self.notes.append(msg)


def is_nan64(hexbits):
    x = int(hexbits, 16)
    return (x >> 52) & 0x7FF == 0x7FF and x & ((1 << 52) - 1) != 0


def sum_order_free(lo, hi):
    """Whether a float bucket with this min and max (hex doubles) has a NaN sum whatever the order of additions."""
    return (is_nan64(lo) and is_nan64(hi)) or (int(lo, 16) == 0xFFF0000000000000 and int(hi, 16) == 0x7FF0000000000000)


def drop_float_sums(text, types):
    """Bucket lines of float series without their sum: the same rows added in another order (memtable, parts,
    merged parts, and tsdbd merges in the background while the clients read) round differently in the last bits.
    The model checks the sums to 1e-9 instead, and NaN as 0x7ff8000000000000 exactly. Whether a sum is NaN at all
    can depend on the order too: DBL_MAX + DBL_MAX - DBL_MAX - DBL_MAX is +inf in one order and NaN (inf - inf) in
    another. The sum stays only where it is NaN in every order, which min and max show: the bucket holds only NaNs
    (both NaN) or both infinities (-inf and +inf); then it is the one quiet NaN and compared bit for bit. A min or
    max of -0.0 and +0.0 is whichever was met first: both read as +0.0."""
    out = []
    for blk, lines in gen.parse_blocks(text):
        ty = types.get(blk.split(" ")[1])
        if blk.split(" ")[0] in ("agg", "grp", "bnd") and ty in ("float32", "float64"):
            lines = [("\t".join(f[:2] + [f[2] if sum_order_free(f[3], f[4]) else "*"] + [("0" * 16 if int(x, 16) << 1 & (2**64 - 1) == 0 else x) for x in f[3:5]]
                              + f[5:]) if len(f := ln.split("\t")) == 8 else ln) for ln in lines]
        out.append("## " + blk)
        out.extend(lines)
    return "\n".join(out) + "\n"


def diff_outputs(a_name, a, b_name, b, limit=3, types=None):
    """Differences between two canonical outputs, as strings (the first few queries). types: series -> type; with
    it, the sums of float series are left out (answers given by different layouts of the same rows)."""
    if types:
        a, b = drop_float_sums(a, types), drop_float_sums(b, types)
    if a == b:
        return []
    A, B = gen.parse_blocks(a), gen.parse_blocks(b)
    out = []
    if len(A) != len(B):
        out.append("%s answered %d queries, %s %d" % (a_name, len(A), b_name, len(B)))
    for (qa, la), (qb, lb) in zip(A, B):
        if qa != qb:
            out.append("query order differs: %r vs %r" % (qa, qb))
            break
        if la != lb:
            i = next((i for i in range(min(len(la), len(lb))) if la[i] != lb[i]), min(len(la), len(lb)))
            out.append("%s: %s has %d lines, %s %d; first difference at line %d: %s = %r, %s = %r"
                       % (qa, a_name, len(la), b_name, len(lb), i, a_name, la[i] if i < len(la) else None,
                          b_name, lb[i] if i < len(lb) else None))
            if len(out) >= limit:
                break
    return out or ["outputs differ (whitespace?)"]


def write_queries(path, model, only_prefixes=None):
    qs = []
    for name, ty in sorted(model.types.items()):
        if model.rows[name]:
            qs += gen.queries_for(name, ty, model.rows[name])
    Path(path).write_text("\n".join(qs) + "\n")
    return qs


def read_phase(ctx, F, d, tag, port, qfile, expected, types, langs=LANGS, reference=None, check_oracle=True):
    """All langs read qfile from the server at port; compares them with each other and with the model.
    Returns {lang: text}."""
    t0 = time.time()
    jobs = {l: (l, ["read", port, qfile], d / ("read-%s-%s.out" % (tag, l))) for l in langs}
    res = run_all(ctx, jobs)
    texts = {}
    for l in langs:
        rc, secs = res[l]
        if rc != 0:
            F.add("%s: %s reader failed (%s): %s" % (tag, l, rc, err_tail(jobs[l][2])))
            continue
        texts[l] = Path(jobs[l][2]).read_text(errors="surrogateescape")
    ref_lang = next(iter(texts), None)
    for l, t in texts.items():
        if l != ref_lang:
            for m in diff_outputs(ref_lang, texts[ref_lang], l, t, types=types):
                F.add("%s: clients disagree: %s" % (tag, m))
    if check_oracle and texts:
        for l in ([ref_lang] if all(t == texts[ref_lang] for t in texts.values()) else list(texts)):
            for m in gen.compare_oracle(texts[l], expected):
                F.add("%s: %s differs from the model: %s" % (tag, l, m))
    if reference is not None and ref_lang:
        for m in diff_outputs("earlier", reference, ref_lang, texts[ref_lang], types=types):
            F.add("%s: answers changed: %s" % (tag, m))
    F.note("%s: %d clients read %d queries in %.1f s" % (tag, len(texts), len(expected), time.time() - t0))
    return texts


def gen_scripts(ctx, model, d, wave, seedoff=0):
    paths = {}
    for i, lang in enumerate(LANGS):
        rng = random.Random(ctx.args.seed * 1000 + i + seedoff)
        text = (gen.lang_script if wave == 1 else gen.wave2_script)(lang, rng, model, ctx.args.scale)
        p = d / ("write%d-%s.txt" % (wave, lang))
        p.write_text(text)
        paths[lang] = p
    return paths


def write_phase(ctx, F, d, tag, port, scripts):
    t0 = time.time()
    jobs = {l: (l, ["write", port, p], d / ("write-%s-%s.out" % (tag, l))) for l, p in scripts.items()}
    res = run_all(ctx, jobs)
    ok = True
    for l, (rc, secs) in res.items():
        if rc != 0:
            ok = False
            F.add("%s: %s writer failed (%s): %s" % (tag, l, rc, err_tail(jobs[l][2])))
    F.note("%s: 4 writers in %.1f s (%s)" % (tag, time.time() - t0,
                                           ", ".join("%s %.1fs" % (l, s) for l, (rc, s) in res.items())))
    return ok


def admin(ctx, F, d, port, what):
    rc, secs = run_client(ctx, "c", ["admin", port, what], d / ("admin-%s.out" % what))
    if rc != 0:
        F.add("admin %s failed (%s): %s" % (what, rc, err_tail(d / ("admin-%s.out" % what))))
    return rc == 0


# ---------------------------------------------------------------- the CLI as the reference

def f9(bits):
    return fmt_float(gen.f32val(bits), bits >> 31, "%.9g")


def f17(bits):
    return fmt_float(gen.f64val(bits), bits >> 63, "%.17g")


def fmt_float(x, sign, spec):
    if x != x:
        return "-nan" if sign else "nan"
    return spec % x


def fmt_time(ms):
    if ms == I64_MIN:
        return "min"
    if ms == I64_MAX:
        return "max"
    dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=ms)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + "%03d" % (ms % 1000)


def cli_args(q):
    """The `tsdb query` arguments for a query line, or None if the CLI cannot ask it."""
    t = q.split(" ")
    kind, name, start, end = t[0], t[1], int(t[2]), int(t[3])
    tm = lambda x: "min" if x == I64_MIN else "max" if x == I64_MAX else str(x)  # noqa: E731
    a = [name, tm(start), tm(end)]
    if kind == "raw":
        a += ["raw"] + (["--desc"] if t[4] == "desc" else []) + (["--limit", t[5]] if int(t[5]) else [])
    elif kind == "agg":
        a += ["all"] + (["%sms" % t[4]] if int(t[4]) else [])
    elif kind == "grp":
        a += ["all"]
        if t[4] != "none":
            a += [t[5] + {"second": "s", "minute": "m", "hour": "h", "day": "d", "week": "w", "month": "mo",
                          "quarter": "q", "year": "y"}[t[4]]]
        if t[6] != "-":
            a += ["--tz", t[6]]
        a += (["--desc"] if t[7] == "desc" else []) + (["--limit", t[8]] if int(t[8]) else [])
    else:
        return None
    return a


def cli_expected(q, ty, lines):
    """What the CLI prints for the canonical answer `lines` of query q."""
    t = q.split(" ")
    if lines == ["!!"]:
        return None
    out = []
    if t[0] == "raw":
        for ln in lines:
            ts, tok = ln.split("\t")
            if tok == "N":
                v = "\\N"
            elif ty == "float32":
                v = f9(int(tok, 16))
            elif ty == "float64":
                v = f17(int(tok, 16))
            elif ty == "bool":
                v = "true" if tok == "1" else "false"
            else:
                v = tok
            out.append("%s\t%s" % (fmt_time(int(ts)), v))
        return out
    out.append("time\tcount\tsum\tmin\tmax\tavg")
    for ln in lines:
        s, cnt, sm, mn, mx, isum, imin, imax = ln.split("\t")
        cnt = int(cnt)
        d = [int(x, 16) for x in (sm, mn, mx)]
        dv = [gen.f64val(x) for x in d]
        if ty in ("int64", "bool"):
            sums = [isum, imin, imax]
        else:
            sums = [f17(x) for x in d]
        avg = "%.9g" % (dv[0] / cnt if cnt else 0.0)
        if cnt and dv[0] != dv[0]:
            avg = "-nan" if d[0] >> 63 else "nan"
        if ty in ("float32", "float64"):  # the sum depends on the order of the additions (a merge may have run)
            sums[0] = avg = "*"
        out.append("%s\t%d\t%s\t%s\t%s\t%s" % (fmt_time(int(s)), cnt, sums[0], sums[1], sums[2], avg))
    return out


def cli_check(ctx, F, dbdir, model, queries, text, d):
    """Runs every query the CLI understands on the (stopped) server's directory and compares."""
    blocks = gen.parse_blocks(text)
    todo = []
    for (q, lines) in blocks:
        a = cli_args(q)
        if a is not None:
            todo.append((q, lines, a))

    def one(item):
        q, lines, a = item
        p = subprocess.run([str(ctx.tsdb), str(dbdir), "query"] + a, capture_output=True, timeout=120)
        return q, lines, a, p

    t0 = time.time()
    n = bad = 0
    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        for q, lines, a, p in ex.map(one, todo):
            n += 1
            ty = model.types[q.split(" ")[1]]
            want = cli_expected(q, ty, lines)
            got = p.stdout.decode("utf-8", "replace").split("\n")[:-1] if p.returncode == 0 else None
            if got and q.split(" ")[0] != "raw" and ty in ("float32", "float64"):
                got = [("\t".join(f[:2] + ["*"] + [x.replace("-0", "0") if x == "-0" else x for x in f[3:5]] + ["*"])
                        if len(f := ln.split("\t")) == 6 and f[1].isdigit() else ln) for ln in got]
                want = [("\t".join(f[:2] + ["*"] + [x.replace("-0", "0") if x == "-0" else x for x in f[3:5]] + ["*"])
                         if len(f := ln.split("\t")) == 6 and f[1].isdigit() else ln) for ln in want or []] if want else want
            if want != got:
                bad += 1
                if bad <= 5:
                    i = next((i for i in range(min(len(want or []), len(got or []))) if want[i] != got[i]), 0)
                    F.add("CLI differs from the clients on %s (args %s): rc %s; client answer converted: %r; CLI: %r"
                          % (q, " ".join(a), p.returncode, (want or ["<error>"])[i:i + 1],
                             (got or [p.stderr.decode()[-100:]])[i:i + 1]))
    F.note("CLI: %d queries checked against `tsdb query` in %.1f s, %d differ" % (n, time.time() - t0, bad))


# ---------------------------------------------------------------- scenarios

def scenario_matrix(ctx, F, io, d):
    model = gen.Model()
    scripts = gen_scripts(ctx, model, d, 1)
    scripts2 = gen_scripts(ctx, model, d, 2, seedoff=500)  # corrections to everything, written after a flush
    qfile = d / "queries.txt"
    queries = write_queries(qfile, model)
    expected = gen.expected_blocks(queries, model)
    F.note("%d series, %d rows, %d queries" % (len(model.types), sum(len(r) for r in model.rows.values()), len(queries)))
    db = d / "db"
    srv = Server(ctx, db, d / "tsdbd.log", io).start()
    ref = None
    try:
        if (write_phase(ctx, F, d, "write", srv.port, scripts) and admin(ctx, F, d, srv.port, "flush")
                and write_phase(ctx, F, d, "write2", srv.port, scripts2)):
            first = read_phase(ctx, F, d, "live", srv.port, qfile, expected, model.types)
            ref = first.get("c")
            if admin(ctx, F, d, srv.port, "flush"):
                read_phase(ctx, F, d, "flushed", srv.port, qfile, expected, model.types, reference=ref)
            if admin(ctx, F, d, srv.port, "optimize"):
                final = read_phase(ctx, F, d, "optimized", srv.port, qfile, expected, model.types, reference=ref)
                ref = final.get("c", ref)
    finally:
        rc = srv.stop()
        if rc != 0:
            F.add("tsdbd exited with %s on SIGTERM" % (rc,))
    if ref:
        cli_check(ctx, F, db, model, queries, ref, d)


def scenario_follower(ctx, F, io, d):
    model = gen.Model()
    s1 = gen_scripts(ctx, model, d, 1)
    m1 = copy.deepcopy(model)
    q1 = d / "queries1.txt"
    queries1 = write_queries(q1, m1)
    e1 = gen.expected_blocks(queries1, m1)
    db = d / "db"
    prim = Server(ctx, db, d / "primary.log", io).start()
    foll = None
    try:
        if not write_phase(ctx, F, d, "wave1", prim.port, s1):
            return
        admin(ctx, F, d, prim.port, "flush")
        foll = Server(ctx, db, d / "follower.log", io, follow=True).start()
        wait_caught_up(ctx, F, d, foll.port, prim.port, m1, "wave 1")
        p1 = read_phase(ctx, F, d, "primary-1", prim.port, q1, e1, m1.types)
        f1 = read_phase(ctx, F, d, "follower-1", foll.port, q1, e1, m1.types)
        if p1.get("c") and f1.get("c"):
            for m in diff_outputs("primary", p1["c"], "follower", f1["c"], types=m1.types):
                F.add("wave 1: follower and primary differ: " + m)
        # a follower is read-only
        wp = d / "write-to-follower.txt"
        wp.write_text("S nope float32\n")
        rc, _ = run_client(ctx, "c", ["write", foll.port, wp], d / "write-follower.out")
        if rc == 0:
            F.add("a write to the follower succeeded")
        # wave 2 on the primary: corrections to everything, a new series
        s2 = gen_scripts(ctx, model, d, 2, seedoff=500)
        if not write_phase(ctx, F, d, "wave2", prim.port, s2):
            return
        admin(ctx, F, d, prim.port, "flush")
        q2 = d / "queries2.txt"
        queries2 = write_queries(q2, model)
        e2 = gen.expected_blocks(queries2, model)
        wait_caught_up(ctx, F, d, foll.port, prim.port, model, "wave 2")
        p2 = read_phase(ctx, F, d, "primary-2", prim.port, q2, e2, model.types)
        # the primary merges (optimize) while the follower is being read: the replaced parts are renamed retired_*
        # and stay for obsolete_part_lifetime_s, because a follower may still have them mapped
        box = {}
        reader = threading.Thread(target=lambda: box.update(
            f=read_phase(ctx, F, d, "follower-2", foll.port, q2, e2, model.types)))
        reader.start()
        time.sleep(0.5)
        admin(ctx, F, d, prim.port, "optimize")
        reader.join()
        f2 = box.get("f", {})
        if p2.get("c") and f2.get("c"):
            for m in diff_outputs("primary", p2["c"], "follower", f2["c"], types=model.types):
                F.add("wave 2: follower and primary differ: " + m)
        # the merged state: the follower follows to it and answers the same as before the merge
        wait_caught_up(ctx, F, d, foll.port, prim.port, model, "the merge")
        f3 = read_phase(ctx, F, d, "follower-3", foll.port, q2, e2, model.types, reference=p2.get("c"))
        p3 = read_phase(ctx, F, d, "primary-3", prim.port, q2, e2, model.types, reference=p2.get("c"))
        if p3.get("c") and f3.get("c"):
            for m in diff_outputs("primary", p3["c"], "follower", f3["c"], types=model.types):
                F.add("after the merge: follower and primary differ: " + m)
        retired = len(list(db.rglob("retired_*")))
        F.note("%d retired_* entries on disk after the merge" % retired)
        if not retired:
            F.add("the merge left no retired_* part: the follower invariant was not exercised")
    finally:
        for s, name in ((foll, "follower"), (prim, "primary")):
            if s:
                rc = s.stop()
                if rc != 0:
                    F.add("%s exited with %s on SIGTERM" % (name, rc))


def wait_caught_up(ctx, F, d, fport, pport, model, what, timeout=60):
    """Polls until the follower answers a raw query of every series exactly as the primary does."""
    probe = d / ("probe-%s.txt" % what.replace(" ", ""))
    probe.write_text("\n".join("raw %s %d %d asc 0" % (n, I64_MIN, I64_MAX) for n in sorted(model.types)) + "\n")
    want = d / "probe-primary.out"
    rc, _ = run_client(ctx, "c", ["read", pport, probe], want)
    if rc != 0:
        F.add("probe on the primary failed: " + err_tail(want))
        return False
    t0 = time.time()
    while time.time() - t0 < timeout:
        out = d / "probe.out"
        rc, _ = run_client(ctx, "c", ["read", fport, probe], out)
        if rc == 0 and out.read_bytes() == want.read_bytes():
            F.note("follower caught up with %s in %.1f s" % (what, time.time() - t0))
            return True
        time.sleep(0.2)
    F.add("follower did not catch up with %s in %d s" % (what, timeout))
    return False


# ---- CLI import

def csv_value(rng, ty, i):
    """(csv text, token) of one value for the CLI import."""
    r = rng.random()
    if r < 0.08:
        return rng.choice(["", "null", "\\N", "NULL"]), "N"
    if ty == "float32":
        if r < 0.2:
            txt, bits = rng.choice([("inf", 0x7F800000), ("-inf", 0xFF800000), ("nan", 0x7FC00000),
                                    ("-nan", 0xFFC00000), ("-0", 0x80000000), ("0", 0)])
            return txt, "%08x" % bits
        bits = gen.f32bits(rng.uniform(-1e4, 1e4) * rng.choice([1, 1e-5, 1e5]))
        return "%.9g" % gen.f32val(bits), "%08x" % bits
    if ty == "float64":
        if r < 0.2:
            txt, bits = rng.choice([("inf", 0x7FF0000000000000), ("-inf", 0xFFF0000000000000),
                                    ("nan", 0x7FF8000000000000), ("-nan", 0xFFF8000000000000),
                                    ("-0", 0x8000000000000000), ("0", 0)])
            return txt, "%016x" % bits
        bits = gen.f64bits(rng.uniform(-1e9, 1e9) * rng.choice([1, 1e-9, 1e9]))
        return "%.17g" % gen.f64val(bits), "%016x" % bits
    if ty == "int64":
        v = rng.choice([gen.I64_MAX, gen.I64_MIN + 1, 0, -1]) if r < 0.15 else rng.randint(-10**12, 10**12)
        return str(v), str(v)
    if ty == "bool":
        b = rng.randint(0, 1)
        return rng.choice([["false", "true"], ["0", "1"]])[b], str(b)
    s = rng.choice(gen.STATES)
    return s, s


def scenario_cli(ctx, F, io, d):
    rng = random.Random(ctx.args.seed + 77)
    db = d / "db"
    model = gen.Model()
    types = {"csv." + t: t for t in gen.T_PERIOD}
    for n, t in types.items():
        p = subprocess.run([str(ctx.tsdb), str(db), "create", n, "--type", t], capture_output=True)
        if p.returncode:
            F.add("tsdb create %s failed: %s" % (n, p.stderr.decode()[-200:]))
            return
        model.series(n, t)
    times = gen.sparse_times(0.5)
    for rnd in range(2):  # the second import corrects a third of the first
        lines = []
        for n, t in types.items():
            pick = times if rnd == 0 else rng.sample(times, len(times) // 3)
            for ts in pick:
                txt, tok = csv_value(rng, t, ts)
                tstr = str(ts) if (ts // 60000) % 2 else (datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=ts)).strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (ts % 1000)
                lines.append("%s,%s,%s" % (n, tstr, txt))
                model.put(n, ts, tok)
        # a series the import creates (float32)
        if rnd == 0:
            model.series("csv.auto", "float32")
            for ts in times[:200]:
                txt, tok = csv_value(rng, "float32", ts)
                lines.append("csv.auto,%d,%s" % (ts, txt))
                model.put("csv.auto", ts, tok)
        rng.shuffle(lines)
        f = d / ("import%d.csv" % rnd)
        f.write_text("\n".join(lines) + "\n")
        with open(f, "rb") as fin:
            p = subprocess.run([str(ctx.tsdb), str(db), "import"], stdin=fin, capture_output=True, timeout=300)
        if p.returncode:
            F.add("tsdb import %d failed: %s" % (rnd, p.stderr.decode()[-300:]))
            return
    qfile = d / "queries.txt"
    queries = write_queries(qfile, model)
    expected = gen.expected_blocks(queries, model)
    srv = Server(ctx, db, d / "tsdbd.log", io).start()
    try:
        out = read_phase(ctx, F, d, "csv", srv.port, qfile, expected, model.types)
    finally:
        rc = srv.stop()
        if rc != 0:
            F.add("tsdbd exited with %s on SIGTERM" % (rc,))
    if out.get("c"):
        cli_check(ctx, F, db, model, queries, out["c"], d)


# ---- durability

LOAD_ROWS, LOAD_BATCH = 1000000, 200


class Loader:
    """A client in `load` mode: its stdout is READY, ACK pass first last ..., DONE."""

    def __init__(self, ctx, lang, port, name, seed, level, d):
        self.lang, self.name, self.seed = lang, name, seed
        self.acks = {1: set(), 2: set()}
        self.out = d / ("load-%s.out" % lang)
        self.errf = open(str(self.out) + ".err", "wb")
        self.proc = spawn(ctx.cmd[lang] + ["load", str(port), name, str(seed), str(level)], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=self.errf)
        self.pass1 = threading.Event()
        self.done = False
        self.ready = threading.Event()
        self.log = []
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()
        self.on_ack = None

    def _read(self):
        for raw in self.proc.stdout:
            line = raw.decode().strip()
            self.log.append(line)
            if line == "READY":
                self.ready.set()
            elif line.startswith("ACK"):
                _, p, a, b = line.split()
                self.acks[int(p)].update(range(int(a), int(b) + 1))
                if self.on_ack:
                    self.on_ack(self, int(p), int(b))
            elif line == "PASS1":
                self.pass1.set()
            elif line == "DONE":
                self.done = True
        self.ready.set()

    def go(self):
        """Lets the client start pass 2."""
        try:
            self.proc.stdin.write(b"GO\n")
            self.proc.stdin.flush()
        except OSError:
            pass

    def reached(self, p, n):
        return len(self.acks[p]) >= n or self.done

    def finish(self, timeout=60):
        try:
            self.proc.stdin.close()  # a client waiting for GO goes on, finds the server gone and exits
        except OSError:
            pass
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            signal_proc(self.proc, signal.SIGKILL)
            self.proc.wait()
        self.thread.join(5)
        self.errf.close()
        Path(self.out).write_text("\n".join(self.log) + "\n")


def verify_load(ld, rows):
    """rows: {ts: value} read back for the loader's series. Returns failure strings."""
    fails = []
    base = ld.seed * 100000000
    nb = LOAD_ROWS // LOAD_BATCH
    state_count = collections.Counter()
    unexpected = [t for t in rows if (t - 1700000000000) % 1000 or not 0 <= (t - 1700000000000) // 1000 < LOAD_ROWS]
    if unexpected:
        fails.append("%s: %d rows at timestamps nobody wrote, e.g. %d" % (ld.lang, len(unexpected), unexpected[0]))
    for b in range(nb):
        states = set()
        for i in range(LOAD_BATCH):
            r = b * LOAD_BATCH + i
            t = 1700000000000 + r * 1000
            v = rows.get(t)
            if v is None:
                states.add("absent")
            elif v == base + r:
                states.add("old")
            elif v == base + r + 1000000:
                states.add("new")
            else:
                states.add("wrong")
                fails.append("%s: row %d holds %d, which neither pass wrote" % (ld.lang, r, v))
        if len(states) > 1:
            fails.append("%s: batch %d is only partly there (%s): an insert must be all or nothing" % (ld.lang, b, sorted(states)))
            continue
        st = states.pop()
        if b in ld.acks[2] and st != "new":
            fails.append("%s: batch %d of pass 2 was acknowledged but reads back %s" % (ld.lang, b, st))
        elif b in ld.acks[1] and st == "absent":
            fails.append("%s: batch %d of pass 1 was acknowledged but is gone" % (ld.lang, b))
        state_count[st] += 1
    ld.summary = dict(state_count)
    return fails[:6]


def read_load(ctx, F, d, tag, port, loaders):
    """Reads every loader's series back ascending and checks it (verify_load). Also reads it descending, whole and with
    a limit that crosses the 65536-row window of the descending reader, and expects the ascending answer reversed."""
    qf = d / ("loadq-%s.txt" % tag)
    cross = 70000
    qs = []
    for ld in loaders:
        qs += ["raw %s %d %d asc 0" % (ld.name, I64_MIN, I64_MAX), "raw %s %d %d desc 0" % (ld.name, I64_MIN, I64_MAX),
               "raw %s %d %d desc %d" % (ld.name, I64_MIN, I64_MAX, cross)]
    qf.write_text("\n".join(qs) + "\n")
    out = d / ("loadread-%s.out" % tag)
    rc, _ = run_client(ctx, "c", ["read", port, qf], out)
    if rc != 0:
        F.add("%s: reading back failed (%s): %s" % (tag, rc, err_tail(out)))
        return
    blocks = gen.parse_blocks(out.read_text())
    for i, ld in enumerate(loaders):
        if len(blocks) < 3 * i + 3:
            F.add("%s: %s: %d answers, wanted %d" % (tag, ld.lang, len(blocks), 3 * len(loaders)))
            return
        asc, desc, desc_lim = (blocks[3 * i + k][1] for k in range(3))
        rows = {}
        for ln in asc:
            if ln == "!!":
                break
            t, v = ln.split("\t")
            rows[int(t)] = int(v)
        for m in verify_load(ld, rows):
            F.add("%s: %s" % (tag, m))
        want = list(reversed(asc))
        if desc != want:
            F.add("%s: %s: the descending read (%d rows) is not the ascending one (%d rows) reversed; first difference "
                  "at index %d" % (tag, ld.lang, len(desc), len(asc), next(
                      (k for k, (x, y) in enumerate(zip(desc, want)) if x != y), min(len(desc), len(want)))))
        elif desc_lim != want[:cross]:
            F.add("%s: %s: desc with limit %d returned %d rows, not the newest %d of the series"
                  % (tag, ld.lang, cross, len(desc_lim), min(cross, len(want))))
        ld.rows_read = len(asc)
    F.note("%s: rows per client %s%s" % (tag, {ld.lang: getattr(ld, "rows_read", None) for ld in loaders},
                                         "" if any(getattr(ld, "rows_read", 0) > cross for ld in loaders)
                                         else " (no series crossed the descending window)"))


def scenario_durability(ctx, F, io, d):
    rng = random.Random(ctx.args.seed + 4242)
    for phase in (1, 2):
        sub = d / ("kill-in-pass-%d" % phase)
        sub.mkdir()
        db = sub / "db"
        srv = Server(ctx, db, sub / "tsdbd.log", io).start()  # default durability: fsync, WAL synced before the ack
        loaders = [Loader(ctx, lang, srv.port, "dur." + lang, i + 1, [1, 0, 3, 1][i], sub) for i, lang in enumerate(LANGS)]
        target = rng.randint(5, 600)  # batches (of 200 rows) every client must have had acknowledged
        for ld in loaders:
            ld.ready.wait(60)
        if phase == 2:
            for ld in loaders:
                if not ld.pass1.wait(180):
                    F.add("kill-in-pass-2: %s did not finish pass 1 in 180 s" % ld.lang)
            for ld in loaders:
                ld.go()
        t0 = time.time()
        while not all(ld.reached(phase, target) for ld in loaders) and time.time() - t0 < 180:
            time.sleep(0.002)
        if time.time() - t0 >= 180:
            F.add("kill-in-pass-%d: the clients did not reach %d acknowledged batches in 180 s" % (phase, target))
        srv.kill9()
        for ld in loaders:
            ld.finish()
        acked = {ld.lang: (len(ld.acks[1]), len(ld.acks[2])) for ld in loaders}
        F.note("kill -9 in pass %d after acks %s (batches of pass 1, pass 2)" % (phase, acked))
        if not any(len(ld.acks[phase]) for ld in loaders):
            F.add("kill-in-pass-%d: nothing was acknowledged in the pass: the test would prove nothing" % phase)
        if phase == 2 and not any(len(ld.acks[2]) for ld in loaders):
            continue
        srv = Server(ctx, db, sub / "tsdbd2.log", io).start()
        try:
            read_load(ctx, F, sub, "pass%d-restart" % phase, srv.port, loaders)
            if admin(ctx, F, sub, srv.port, "optimize"):
                read_load(ctx, F, sub, "pass%d-optimized" % phase, srv.port, loaders)
        finally:
            rc = srv.stop()
            if rc != 0:
                F.add("tsdbd exited with %s on SIGTERM after recovery" % (rc,))
        srv = Server(ctx, db, sub / "tsdbd3.log", io).start()
        try:
            read_load(ctx, F, sub, "pass%d-second-restart" % phase, srv.port, loaders)
        finally:
            srv.stop()
        F.note("pass %d outcome per client: %s" % (phase, {ld.lang: getattr(ld, "summary", None) for ld in loaders}))


def scenario_stale_wal(ctx, F, io, d):
    """A WAL segment that outlived its flush (the server died between publishing the part and unlinking the
    segment) must not be replayed over newer parts: recovery relies on each part's wal_seq. A kill timed on client
    acknowledgements never lands in that window, so it is made by hand: the segments of wave 1 are copied away while
    they are live, wave 1 is flushed, wave 2 corrects every value and is flushed, the server stops, the old segments
    are put back, and the restarted server must still answer wave 2."""
    model = gen.Model()
    s1 = gen_scripts(ctx, model, d, 1)
    db = d / "db"
    stash = d / "stale-wal"
    srv = Server(ctx, db, d / "tsdbd.log", io).start()
    try:
        if not write_phase(ctx, F, d, "wave1", srv.port, s1):
            return
        kept = 0
        for p in db.rglob("wal_*.log"):
            dst = stash / p.relative_to(db)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst)
            kept += 1
        if not kept:
            F.add("no WAL segment to keep after wave 1: the test would prove nothing")
            return
        if not admin(ctx, F, d, srv.port, "flush"):
            return
        gone = sum(1 for p in stash.rglob("wal_*.log") if not (db / p.relative_to(stash)).exists())
        F.note("%d WAL segment(s) kept, %d of them unlinked by the flush" % (kept, gone))
        s2 = gen_scripts(ctx, model, d, 2, seedoff=500)  # corrections to everything
        if not (write_phase(ctx, F, d, "wave2", srv.port, s2) and admin(ctx, F, d, srv.port, "flush")):
            return
    finally:
        rc = srv.stop()
        if rc != 0:
            F.add("tsdbd exited with %s on SIGTERM" % (rc,))
    if not gone:
        F.add("the flush unlinked none of the kept WAL segments: putting them back would prove nothing")
        return
    for p in stash.rglob("wal_*.log"):
        shutil.copy2(p, db / p.relative_to(stash))
    qfile = d / "queries.txt"
    queries = write_queries(qfile, model)
    expected = gen.expected_blocks(queries, model)
    srv = Server(ctx, db, d / "tsdbd2.log", io).start()
    try:
        read_phase(ctx, F, d, "stale-wal", srv.port, qfile, expected, model.types, langs=["c"])
    finally:
        rc = srv.stop()
        if rc != 0:
            F.add("tsdbd exited with %s on SIGTERM after recovery" % (rc,))


SCENARIOS = collections.OrderedDict([("matrix", scenario_matrix), ("cli", scenario_cli),
                                     ("follower", scenario_follower), ("durability", scenario_durability),
                                     ("stale_wal", scenario_stale_wal)])


# ---------------------------------------------------------------- main

# What tsdbd prints under --io uring when the kernel or its policy does not offer io_uring (tsdbd.c: the ring setup
# error through strerror(), or a build without it). Anything else going wrong with the backend is a failure.
URING_UNAVAILABLE = ("Operation not permitted", "Function not implemented", "Permission denied",
                     "Cannot allocate memory", "Invalid argument", "not in this build")


def uring_probe(ctx):
    """Starts and stops a tsdbd --io uring. Returns ("ok" | "unavailable" | "broken", why): only the server saying
    that io_uring is not offered counts as unavailable; a crash, a hang, or a bad exit status is "broken"."""
    d = Path(tempfile.mkdtemp(prefix="probe-", dir=ctx.work))
    log = d / "probe.log"
    srv = Server(ctx, d / "db", log, "uring")
    try:
        srv.start()
    except Exception as e:  # noqa: BLE001
        tail = log.read_text(errors="replace").split("$ ")[-1] if log.exists() else ""
        for ln in tail.splitlines():
            if ln.startswith("tsdbd: io_uring:") and any(w in ln for w in URING_UNAVAILABLE):
                return "unavailable", ln.strip()
        return "broken", "tsdbd --io uring: %s (%s)" % (str(e)[:200], log)
    rc = srv.stop()
    if rc != 0:
        return "broken", "tsdbd --io uring exited with %s on SIGTERM (%s)" % (rc, log)
    return "ok", ""


def main():
    ap = argparse.ArgumentParser(description="tsdb end-to-end tests")
    ap.add_argument("--only", default=",".join(SCENARIOS))
    ap.add_argument("--io", default="uring,epoll")
    ap.add_argument("--scale", type=float, default=float(os.environ.get("INTEG_SCALE", "1.0")))
    ap.add_argument("--seed", type=int, default=int(os.environ.get("INTEG_SEED", "1")))
    ap.add_argument("--logs", default=str(BUILD / "integ/logs"))
    ap.add_argument("--work", default=os.environ.get("TMPDIR", "/tmp"))
    ap.add_argument("--timeout", type=int, default=240, help="seconds a client program may take (default 240)")
    ap.add_argument("--keep", action="store_true", help="keep the data directories")
    ap.add_argument("--bin-dir", help="directory with tsdbd and tsdb (default build/; the dist build for dist runs)")
    ap.add_argument("--itc", help="the C client program (default build/integ/itc)")
    args = ap.parse_args()
    args.logs = str(Path(args.logs) / time.strftime("%Y%m%d-%H%M%S"))
    ctx = Ctx(args)
    miss = ctx.missing()
    if miss:
        print("missing: " + ", ".join(miss) + "\nrun `make integration-tests`, which builds them", file=sys.stderr)
        return 2
    Path(args.logs).mkdir(parents=True)
    ios = []
    skipped = []
    probe_fail = []
    for io in args.io.split(","):
        if io == "uring":
            state, why = uring_probe(ctx)
            if state == "unavailable":
                print("io_uring is not available here (%s): uring SKIPPED" % why)
                skipped.append("uring SKIPPED: " + why)
                continue
            if state == "broken":
                print("FAILED: " + why)
                probe_fail.append(why)
                continue
        ios.append(io)
    wanted = [s for s in args.only.split(",") if s]
    for s in wanted:
        if s not in SCENARIOS:
            print("unknown scenario " + s, file=sys.stderr)
            return 2
    t00 = time.time()
    print("logs: %s\nwork: %s" % (args.logs, ctx.work))
    results = []
    for io in ios:
        for s in wanted:
            d = Path(args.logs) / io / s
            d.mkdir(parents=True)
            work = ctx.work / ("%s-%s" % (io, s))
            work.mkdir()
            F = Fails(s, io)
            print("== %s [--io %s]" % (s, io), flush=True)
            t0 = time.time()
            try:
                SCENARIOS[s](ctx, F, io, work)
            except Exception as e:  # noqa: BLE001
                import traceback
                F.add("scenario crashed: %s: %s\n%s" % (type(e).__name__, e, traceback.format_exc()))
            secs = time.time() - t0
            # per-scenario logs: client outputs and server logs next to the notes
            for p in work.iterdir():
                if p.is_file():
                    shutil.copy(p, d / p.name)
            for sub in work.glob("*/"):
                if sub.is_dir() and sub.name != "db":
                    shutil.copytree(sub, d / sub.name, ignore=shutil.ignore_patterns("db"), dirs_exist_ok=True)
            (d / "notes.txt").write_text("\n".join(F.notes + ["FAIL: " + m for m in F.items]) + "\n")
            results.append((io, s, secs, F))
            print("   %s in %.1f s%s" % ("FAILED" if F.items else "ok", secs, "" if not F.items else " (%d failures)" % len(F.items)))
            for n in F.notes:
                print("   . " + n)
            if not F.items and not args.keep:
                shutil.rmtree(work, ignore_errors=True)
    total = time.time() - t00
    lines = ["", "%-10s %-6s %8s  %s" % ("scenario", "io", "seconds", "result")]
    for io, s, secs, F in results:
        lines.append("%-10s %-6s %8.1f  %s" % (s, io, secs, "ok" if not F.items else "FAILED: %d" % len(F.items)))
    nfail = sum(1 for r in results if r[3].items) + len(probe_fail)
    lines += skipped
    lines += ["[uring probe] FAILED: " + w for w in probe_fail]
    lines.append("%d of %d scenario runs failed in %.0f s; logs in %s" % (nfail, len(results) + len(probe_fail), total, args.logs))
    if nfail:
        lines.append("")
        for io, s, secs, F in results:
            for m in F.items:
                lines.append("[%s %s] %s" % (s, io, m.splitlines()[0][:400]))
    print("\n".join(lines))
    Path(args.logs, "summary.txt").write_text("\n".join(lines) + "\n")
    if not args.keep and not nfail:
        shutil.rmtree(ctx.work, ignore_errors=True)
    else:
        print("data directories kept in " + str(ctx.work))
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
