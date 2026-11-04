"""The Python client of the integration tests: same command line and output as c/itc.c, rust/ and java/
(see run.py). Needs the tsdb_client module (make test-python's venv, build/pyvenv)."""
import array
import os
import struct
import sys

import tsdb_client as t

I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1


def f32(bits):
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def f64(bits):
    return struct.unpack("<d", struct.pack("<Q", bits))[0]


def bits32(x):
    return struct.unpack("<I", struct.pack("<f", x))[0]


def bits64(x):
    return struct.unpack("<Q", struct.pack("<d", x))[0]


class Ctx:
    def __init__(self, port):
        # run.py --auth on names the user to log in as in the environment, as it does for every client program
        user = os.environ.get("ITG_USER")
        login = dict(user=user, password_file=os.environ["ITG_PASSWORD_FILE"], require_auth=True) if user else {}
        self.c = t.Client("127.0.0.1", port, **login)
        self.series = {}
        self.names = {}

    def find(self, name, create=None):
        if name in self.series:
            return self.series[name]
        try:
            if create:
                sid = self.c.series_create(name, create)
                ty = create
            else:
                sid = self.c.series(name, create=False)
                ty = self.c.series_type(sid)
        except t.TsdbError:
            return None
        self.series[name] = (sid, ty)
        return self.series[name]

    def state_name(self, sid, code):
        k = (sid, code)
        if k not in self.names:
            self.names[k] = self.c.state_name(sid, code)
        return self.names[k]


def token(cx, sid, ty, tok):
    if tok == "N":
        return None
    if ty == "float32":
        return f32(int(tok, 16))
    if ty == "float64":
        return f64(int(tok, 16))
    if ty == "int64":
        return int(tok)
    if ty == "bool":
        return tok == "1"
    return cx.c.state(sid, tok)


def write(cx, path):
    lines = open(path).read().split("\n")
    i = 0
    while i < len(lines):
        p = lines[i].split()
        i += 1
        if not p:
            continue
        if p[0] == "S":
            cx.find(p[1], p[2])
            continue
        multi = p[0] == "M"
        level, flags, n = int(p[2]), p[3], int(p[4])
        if multi:
            ty, one = p[1], 0
        else:
            one, ty = cx.find(p[1])
        ts, ids, vals = [], [], []
        for _ in range(n):
            r = lines[i].split()
            i += 1
            if multi:
                sid, _ty = cx.find(r[0])
                ids.append(sid)
                r = r[1:]
            ts.append(int(r[0]))
            vals.append(token(cx, ids[-1] if multi else one, ty, r[1]))
        cx.c.set_compression(level)
        asyn = "async" in flags
        tsa = array.array("q", ts)
        has_null = any(v is None for v in vals)
        if "legacy" in flags:
            fl = array.array("f", struct.pack("<%dI" % n, *[bits32(v) for v in vals]))
            if multi:
                (cx.c.insert_async if asyn else cx.c.insert)(array.array("Q", ids), tsa, fl)
            else:
                (cx.c.insert_series_async if asyn else cx.c.insert_series)(one, tsa, fl)
            continue
        if not has_null and ty in ("float32", "float64", "int64"):
            # no nulls: a typed array goes in without iterating, the way numpy data would
            code = {"float32": "f", "float64": "d", "int64": "q"}[ty]
            if ty == "float32":
                data = array.array("f", struct.pack("<%dI" % n, *[bits32(v) for v in vals]))
            elif ty == "float64":
                data = array.array("d", struct.pack("<%dQ" % n, *[bits64(v) for v in vals]))
            else:
                data = array.array(code, vals)
        else:
            data = vals
        if multi:
            (cx.c.insert_values_async if asyn else cx.c.insert_values)(array.array("Q", ids), tsa, data, value_type=ty)
        else:
            (cx.c.insert_series_values_async if asyn else cx.c.insert_series_values)(one, tsa, data, value_type=ty)
    cx.c.wait()


def fmt_value(cx, sid, ty, v):
    if v is None:
        return "N"
    if ty == "float32":
        return "%08x" % bits32(v)
    if ty == "float64":
        return "%016x" % bits64(v)
    if ty == "int64":
        return str(v)
    if ty == "bool":
        return "1" if v else "0"
    return cx.state_name(sid, v)


def bucket_lines(bs):
    return [
        "%d\t%d\t%016x\t%016x\t%016x\t%d\t%d\t%d"
        % (b.start, b.count, bits64(b.sum), bits64(b.min), bits64(b.max), b.isum, b.imin, b.imax)
        for b in bs
    ]


def bound(x):
    return None if x in (I64_MIN, I64_MAX) else x


def read(cx, path):
    out = []
    for line in open(path).read().split("\n"):
        if not line.strip():
            continue
        out.append("## " + line)
        q = line.split(" ")
        found = cx.find(q[1])
        if not found:
            out.append("!!")
            continue
        sid, ty = found
        start, end = int(q[2]), int(q[3])
        try:
            if q[0] in ("raw", "f32raw"):
                order, limit = q[4], int(q[5]) or None
                if q[0] == "raw":
                    ts, vals = cx.c.query_values(sid, bound(start), bound(end), order=order, limit=limit, numpy=False)
                    rows = ["%d\t%s" % (a, fmt_value(cx, sid, ty, v)) for a, v in zip(ts, vals)]
                else:
                    ts, vals = cx.c.query_raw(sid, bound(start), bound(end), order=order, limit=limit, numpy=False)
                    rows = ["%d\t%08x" % (a, bits32(v)) for a, v in zip(ts, vals)]
            elif q[0] == "agg":
                rows = bucket_lines(cx.c.query_agg(sid, bound(start), bound(end), int(q[4])))
            elif q[0] == "grp":
                unit = None if q[4] == "none" else q[4]
                tz = None if q[6] == "-" else q[6]
                rows = bucket_lines(
                    cx.c.query_group(
                        sid, bound(start), bound(end), unit=unit, count=int(q[5]), timezone=tz,
                        order=q[7], limit=int(q[8]) or None,
                    )
                )
            elif q[0] == "bnd":
                bounds = array.array("q", [int(x) for x in q[6].split(",")])
                rows = bucket_lines(
                    cx.c.query_bounds(sid, bounds, bound(start), bound(end), order=q[4], limit=int(q[5]) or None)
                )
            else:
                raise SystemExit("unknown query kind " + q[0])
        except t.TsdbError:
            out.append("!!")
            continue
        out.extend(rows)
    sys.stdout.write("\n".join(out) + "\n")


LOAD_ROWS, LOAD_BATCH, LOAD_FLIGHT = 1000000, 200, 8


def load(cx, name, seed, level):
    sid = cx.c.series_create(name, "int64")
    cx.c.set_compression(level)
    print("READY", flush=True)
    nb = LOAD_ROWS // LOAD_BATCH
    for pas in (1, 2):
        first = 0
        for b in range(nb):
            ts = array.array("q", [1700000000000 + (b * LOAD_BATCH + i) * 1000 for i in range(LOAD_BATCH)])
            vs = array.array("q", [seed * 100000000 + b * LOAD_BATCH + i + (1000000 if pas == 2 else 0) for i in range(LOAD_BATCH)])
            cx.c.insert_series_values_async(sid, ts, vs, value_type="int64")
            if (b + 1) % LOAD_FLIGHT == 0 or b == nb - 1:
                cx.c.wait()
                print("ACK %d %d %d" % (pas, first, b), flush=True)
                first = b + 1
        if pas == 1:  # wait for the driver: the clients start pass 2 together
            print("PASS1", flush=True)
            sys.stdin.readline()
    print("DONE", flush=True)


def main():
    a = sys.argv
    if len(a) < 4:
        sys.exit("usage: itp.py write|read|load|admin <port> <arg>...")
    cx = Ctx(int(a[2]))
    if a[1] == "write":
        write(cx, a[3])
    elif a[1] == "read":
        read(cx, a[3])
    elif a[1] == "load":
        load(cx, a[3], int(a[4]), int(a[5]) if len(a) > 5 else 1)
    elif a[1] == "admin":
        {"flush": cx.c.flush, "optimize": cx.c.optimize}[a[3]]()
    else:
        sys.exit(2)
    cx.c.close()


if __name__ == "__main__":
    main()
