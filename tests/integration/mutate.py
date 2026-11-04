#!/usr/bin/env python3
"""Does the integration suite have teeth? Plants known bugs, one at a time, in a COPY of the tree, rebuilds only
what the bug touches, runs the scenario that should notice, and expects it to fail. A mutation the suite does not
catch (SURVIVED) is a hole in the suite.

  mutate.py [--list] [--only M1,M5] [--keep]

The copy lives under $TMPDIR; the real tree and build/ are never written. Untouched components are linked from the
real build (build/integ/rust, client/java/target, build/pyvenv), so a mutation costs one component rebuild plus the
scenario. Python-client mutations build a private venv (pip needs the network), the slowest.
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]

# id, what the bug is, file, old text (must occur once), new text, components rebuilt, scenario(s) to run, backend
M = []


def mut(id, what, file, old, new, build, scen, io="epoll", hang=False):
    """hang: the bug can only show as a client that waits for ever (a lost reply); its timeout is then the catch."""
    M.append(dict(id=id, what=what, file=file, old=old, new=new, build=build, scen=scen, io=io, hang=hang))


mut("M1", "C client ignores the validity bitmap of a values reply (nulls read as values)",
    "client/tsdb_client.c",
    "tsdb_values col = {(tsdb_type)b[1], v, valid ? v + (size_t)n * aw : NULL};",
    "tsdb_values col = {(tsdb_type)b[1], v, NULL};",
    ["c"], "matrix")
mut("M2", "Rust client: a local midnight near a clock change is one hour off (wrong time zone bound)",
    "client/rust/src/calendar.rs",
    "        let u = local - hi_off;\n        if i64::from(self.offset(u)) == hi_off {\n            return u;\n        }",
    "        let u = local - hi_off;\n        if i64::from(self.offset(u)) == hi_off {\n            return u + 3_600;\n        }",
    ["rust"], "matrix")
mut("M3", "server loses the acknowledgement of every 61st values insert (a dropped ack)",
    "server/tsdbd.c",
    "    default: rc = TSDB_ERR_INVALID; break;\n    }\n    reply_status(c, r->id, rc);\n}",
    "    default: rc = TSDB_ERR_INVALID; break;\n    }\n"
    "    static unsigned dropped;\n"
    "    if (!(r->op == TSDB_OP_INSERT_VALUES && rc == 0 && ++dropped % 61 == 0))\n"
    "        reply_status(c, r->id, rc);\n}",
    ["c"], "matrix", hang=True)
mut("M4", "Java client sends limit + 1 for raw queries (off by one in limits)",
    "client/java/src/main/java/ai/intellistream/tsdb/TsdbClient.java",
    "begin(Protocol.OP_QUERY_RAW, opts ? 33 : 24);\n        out.putLong(series).putLong(start).putLong(end);\n"
    "        if (opts)\n            out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit);",
    "begin(Protocol.OP_QUERY_RAW, opts ? 33 : 24);\n        out.putLong(series).putLong(start).putLong(end);\n"
    "        if (opts)\n            out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit > 0 ? limit + 1 : 0);",
    ["java"], "matrix")
mut("M5", "server mixes up two fields of a bucket reply (min and max swapped): all clients agree, the engine does not",
    "server/tsdbd.c",
    "        memcpy(o + 24, &b[i].min, 8);\n        memcpy(o + 32, &b[i].max, 8);",
    "        memcpy(o + 24, &b[i].max, 8);\n        memcpy(o + 32, &b[i].min, 8);",
    ["c"], "matrix")
mut("M6", "merge keeps the OLDEST value on equal keys (a correction is undone by optimize)",
    "src/merge.c",
    "(s == cs && t == ct && cur[i].p->max_block > c->p->max_block);",
    "(s == cs && t == ct && cur[i].p->max_block < c->p->max_block);",
    ["c"], "matrix")
mut("M7", "follower never reads a new MANIFEST version (stops following)",
    "src/manifest.c",
    "    if (m.version != db->follow_version) {",
    "    if (false && m.version != db->follow_version) {",
    ["c"], "follower")
mut("M8", "tsdb import no longer reads an empty field as null",
    "tools/tsdb-cli.c",
    "*known = !(vs[0] == '\\0' || strcmp(vs, \"null\") == 0",
    "*known = !(strcmp(vs, \"null\") == 0",
    ["c"], "cli")
mut("M9", "Java packed inserts canonicalise NaN (floatToIntBits): payloads and the null marker are lost",
    "client/java/src/main/java/ai/intellistream/tsdb/Pack.java",
    "int x = Float.floatToRawIntBits(v[off + i]);",
    "int x = Float.floatToIntBits(v[off + i]);",
    ["java"], "matrix")
mut("M10", "crash recovery skips the WAL segment that follows a flushed part (acknowledged rows are lost)",
    "src/memtable.c",
    "if (cached_seq >= segs[k].seq || id >= eng_series_count(db))",
    "if (cached_seq + 1 >= segs[k].seq || id >= eng_series_count(db))",
    ["c"], "durability")
mut("M11", "epoll backend only: a large send is accounted one byte too far (a corrupt reply stream)",
    "server/tsdbd.c",
    "                on_send(c, r < 0 ? -err : r);\n        }\n        c->in_ready = false;",
    "                on_send(c, r < 0 ? -err : r + (r > 60000));\n        }\n        c->in_ready = false;",
    ["c"], "matrix", io="epoll")
mut("M12", "Python client sends limit - 1 (off by one in limits, in the PyO3 layer)",
    "client/python/src/lib.rs",
    "    Ok(tc::QueryOpts { order, limit })",
    "    Ok(tc::QueryOpts { order, limit: limit.map(|l| if l > 1 { l - 1 } else { l }) })",
    ["python"], "matrix")

# found by review: these four survived the first version of the suite
mut("M13", "crash recovery replays every WAL segment, ignoring each part's wal_seq (old values over newer parts)",
    "src/memtable.c",
    "if (cached_seq >= segs[k].seq || id >= eng_series_count(db))",
    "if ((cached_seq >= segs[k].seq && 0) || id >= eng_series_count(db))",
    ["c"], "stale_wal")
mut("M14", "descending raw reads: every window after the first loses its oldest row (only series over 65536 rows)",
    "src/query.c",
    "        size_t m = w.n;",
    "        size_t m = w.n - (hi != end && w.n > 0);",
    ["c"], "durability")
mut("M15", "C client calendar: a repeated local midnight (Havana, 2024-11-03) resolves to the LATER instant",
    "client/tsdb_calendar.c",
    "    int64_t u = local - hi_off;\n    if (tz_offset(z, u) == hi_off)\n        return u;\n    u = local - lo_off;\n"
    "    if (tz_offset(z, u) == lo_off)\n        return u;",
    "    int64_t u = local - lo_off;\n    if (tz_offset(z, u) == lo_off)\n        return u;\n    u = local - hi_off;\n"
    "    if (tz_offset(z, u) == hi_off)\n        return u;",
    ["c"], "matrix")
mut("M16", "C client calendar: a skipped local midnight (Havana 2024-03-10, Beirut 2024-03-31) resolves one second early",
    "client/tsdb_calendar.c",
    "    return hi;",
    "    return lo;",
    ["c"], "matrix")
mut("M17", "granule statistics drop the high word of 128-bit integer sums beyond +-2^64",
    "src/values.c",
    "        g->isum_hi = (int64_t)(a.isum >> 64);",
    "        g->isum_hi = a.isum < 0 ? -1 : 0;",
    ["c"], "matrix")
mut("M18", "server skips the rights check in handle(): a logged-in user reads another tenant's series",
    "server/tsdbd.c",
    "    if ((rc = auth_fresh(a)) != 0 || (rc = auth_check(a, r->op, p, len)) != 0) {",
    "    if ((rc = auth_fresh(a)) != 0 || (rc = 0) != 0) {",
    ["c"], "matrix")


def sh(cmd, cwd, log, env=None, check=True, timeout=None):
    with open(log, "ab") as f:
        f.write(("$ %s\n" % " ".join(map(str, cmd))).encode())
        f.flush()
        try:
            p = subprocess.run(cmd, cwd=cwd, stdout=f, stderr=subprocess.STDOUT, env=env, timeout=timeout)
        except subprocess.TimeoutExpired:
            return "timeout"
    if check and p.returncode:
        raise RuntimeError("%s failed (%d), see %s" % (" ".join(map(str, cmd[:3])), p.returncode, log))
    return p.returncode


RUN_TIMEOUT = 1500  # seconds for one run.py of a planted bug; a hang is reported as TIMEOUT, not as a catch


def classify(rc, out, hang=False):
    """The verdict for a run.py exit status and output. Only a failed CHECK is a catch: a driver that crashed
    ("scenario crashed", a Python exception in run.py) or hung says nothing about the planted bug, and neither does a
    client that timed out as the first failure."""
    first = [ln for ln in out.splitlines() if re.match(r"\[\w+ \w+\] ", ln)][:1]
    line = (first[0] if first else "?")[:330]
    if rc == "timeout":
        return "TIMEOUT", "run.py did not finish in %d s" % RUN_TIMEOUT
    if rc == 0:
        return "SURVIVED", "the suite passed with the bug planted"
    if rc == 1:
        if not first or "scenario crashed" in line:
            return "HARNESS", line if first else "run.py failed without a failure line (a crash of the driver?)"
        if "timeout after" in line and not hang:
            return "TIMEOUT", line
        return "CAUGHT", line
    return "ERROR", "run.py exited %s" % rc


def run_one(m, keep, logdir):
    tmp = Path(tempfile.mkdtemp(prefix="tsdb-mutant-"))
    tree = tmp / "tree"
    log = logdir / (m["id"] + ".log")
    t0 = time.time()
    try:
        shutil.copytree(ROOT, tree, symlinks=True,
                        ignore=shutil.ignore_patterns(".git", "build", "build-*", "target", "__pycache__"))
        p = tree / m["file"]
        text = p.read_text()
        if text.count(m["old"]) != 1:
            return "ERROR", "the text to mutate occurs %d times in %s" % (text.count(m["old"]), m["file"])
        p.write_text(text.replace(m["old"], m["new"]))
        b = tree / "build"
        (b / "integ").mkdir(parents=True)
        real = ROOT / "build"
        env = dict(os.environ, PYVENV=str(real / "pyvenv"))
        # components the bug does not touch come from the real tree
        if "rust" not in m["build"]:
            (b / "integ/rust").symlink_to(real / "integ/rust")
        if "java" not in m["build"]:
            (b / "integ/java").symlink_to(real / "integ/java")
            (tree / "client/java/target").symlink_to(ROOT / "client/java/target")
        if "c" in m["build"]:
            sh(["make", "-j16", "build/tsdbd", "build/tsdb", "build/integ/itc"], tree, log)
        else:
            for f in ("tsdbd", "tsdb"):
                (b / f).symlink_to(real / f)
            (b / "integ/itc").symlink_to(real / "integ/itc")
        if "rust" in m["build"]:
            sh(["cargo", "build", "--release", "--locked", "--target-dir", str(b / "integ/rust")],
               tree / "tests/integration/rust", log)
        if "java" in m["build"]:
            sh(["mvn", "-q", "-DskipTests", "package"], tree / "client/java", log)
            (b / "integ/java").mkdir(parents=True, exist_ok=True)
            jar = tree / f"client/java/target/tsdb-client-{(tree / 'VERSION').read_text().strip()}.jar"
            sh(["javac", "-d", str(b / "integ/java"), "-cp", str(jar), "tests/integration/java/Itj.java"], tree, log)
        if "python" in m["build"]:
            venv = b / "pyvenv"
            sh([sys.executable, "-m", "venv", str(venv)], tree, log)
            sh([str(venv / "bin/pip"), "install", "-q", "maturin"], tree, log)
            sh([str(venv / "bin/maturin"), "develop", "--release"], tree / "client/python", log,
               env=dict(env, VIRTUAL_ENV=str(venv)))
            env["PYVENV"] = str(venv)
        logs = tmp / "logs"
        rc = sh([sys.executable, "tests/integration/run.py", "--bin-dir", str(b), "--only", m["scen"], "--io", m["io"],
                 "--timeout", "40", "--logs", str(logs), "--work", str(tmp)], tree, log, env=env, check=False,
                timeout=RUN_TIMEOUT)
        return classify(rc, log.read_text(errors="replace"), m["hang"])
    except Exception as e:  # noqa: BLE001
        return "ERROR", str(e)[:300]
    finally:
        if not keep:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print("kept " + str(tmp))
        print("    (%.0f s)" % (time.time() - t0), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--io", help="run every mutation on this backend only (default: the one that exposes it)")
    ap.add_argument("--logs", default=str(ROOT / "build/integ/mutants"))
    a = ap.parse_args()
    if a.list:
        for m in M:
            print("%-4s %-9s %-6s %s" % (m["id"], m["scen"], m["io"], m["what"]))
        return 0
    want = [x for x in a.only.split(",") if x] or [m["id"] for m in M]
    logdir = Path(a.logs) / time.strftime("%Y%m%d-%H%M%S")
    logdir.mkdir(parents=True)
    rows = []
    for m in M:
        if m["id"] not in want:
            continue
        m["io"] = a.io or m["io"]
        print("%s: %s [%s, --io %s]" % (m["id"], m["what"], m["scen"], m["io"]), flush=True)
        verdict, why = run_one(m, a.keep, logdir)
        print("  %s: %s" % (verdict, why), flush=True)
        rows.append((m, verdict, why))
    print("\n%-4s %-9s %s" % ("id", "result", "bug"))
    for m, v, why in rows:
        print("%-4s %-9s %s" % (m["id"], v, m["what"]))
    bad = [m["id"] for m, v, _ in rows if v != "CAUGHT"]  # SURVIVED, TIMEOUT, HARNESS and ERROR all count against
    print("%d of %d planted bugs caught%s; logs in %s" % (len(rows) - len(bad), len(rows),
                                                          ("; NOT caught: " + ", ".join(bad)) if bad else "", logdir))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
