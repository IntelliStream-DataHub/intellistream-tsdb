#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Turn the UCI hydraulic text files into one series per sensor, as raw values.

    python3 -I convert.py SRC_DIR OUT_DIR

Each text file has one row per 60 s working cycle (2205 rows, cycles back to
back) and one column per sample of that cycle. Read row by row, a file is one
series in time order: sample j of cycle i is at i * 60 s + j * (60 s / columns).

For every sensor this writes OUT_DIR/<SENSOR>.f64 (the double nearest each
decimal in the text) and OUT_DIR/<SENSOR>.f32 (the float nearest the decimal,
not a double rounded to float), little-endian. For the pressures PS1-PS6 it
also writes <SENSOR>_psi.f64 and _psi.f32: bar * 14.503773773 as a double,
not rounded to any number of decimals (a unit conversion, as a collector might
do it), and that double rounded once to float.

Standard library only.
"""
import array
import os
import struct
import sys
from fractions import Fraction

PSI_PER_BAR = 14.503773773
ROWS = 2205
# name, samples per cycle (100 Hz: 6000, 10 Hz: 600, 1 Hz: 60), psi variant
SENSORS = [(f"PS{i}", 6000, True) for i in range(1, 7)] + [("EPS1", 6000, False)] + \
    [(s, 600, False) for s in ("FS1", "FS2")] + \
    [(s, 60, False) for s in ("TS1", "TS2", "TS3", "TS4", "VS1", "CE", "CP", "SE")]

F32 = struct.Struct("<f")
U32 = struct.Struct("<I")


def f32_of_bits(b):
    return F32.unpack(U32.pack(b))[0]


def nearest_f32(tok):
    """The float32 nearest the decimal tok (ties to even), checked exactly."""
    x = Fraction(tok)
    f = F32.unpack(F32.pack(float(tok)))[0]  # double, then float: almost always right
    if f == 0.0:
        return f
    b = U32.unpack(F32.pack(f))[0]
    best = None
    for c in (b - 1, b, b + 1):
        v = f32_of_bits(c)
        if v != v or v in (float("inf"), float("-inf")):
            continue
        d = abs(Fraction(v) - x)
        if best is None or d < best[0] or (d == best[0] and c % 2 == 0):
            best = (d, v)
    return best[1]


def convert(src, out, name, cols, psi):
    d64, d32 = array.array("d"), array.array("f")
    to64, to32 = {}, {}
    with open(os.path.join(src, name + ".txt"), "r", encoding="ascii") as f:
        rows = 0
        for line in f:
            toks = line.split()
            if len(toks) != cols:
                sys.exit(f"{name}.txt row {rows + 1}: {len(toks)} columns, expected {cols}")
            for t in toks:
                if t not in to64:
                    to64[t] = float(t)
                    to32[t] = nearest_f32(t)
            d64.extend(map(to64.__getitem__, toks))
            d32.extend(map(to32.__getitem__, toks))
            rows += 1
    if rows != ROWS:
        sys.exit(f"{name}.txt: {rows} rows, expected {ROWS}")
    outs = [(name + ".f64", d64), (name + ".f32", d32)]
    if psi:
        p64 = array.array("d", (v * PSI_PER_BAR for v in d64))
        outs += [(name + "_psi.f64", p64), (name + "_psi.f32", array.array("f", p64))]
    for fn, a in outs:
        if sys.byteorder != "little":
            a.byteswap()
        with open(os.path.join(out, fn), "wb") as g:
            a.tofile(g)
    print(f"{name:5} {rows * cols:>9} values, {len(to64):>7} distinct, {cols // 60:>3} Hz")


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python3 -I convert.py SRC_DIR OUT_DIR")
    src, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    for name, cols, psi in SENSORS:
        convert(src, out, name, cols, psi)


if __name__ == "__main__":
    main()
