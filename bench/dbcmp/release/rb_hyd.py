# rb_hyd.py F32FILE STEP_MS OUT: a hydraulic series (bench/hydraulic/convert.py's .f32, little-endian float32 in
# time order) as RowBinary rows of tsdb-bench --export (u32 series 0, i64 ms, f32), sample i at
# 2024-01-01 + i * STEP_MS. Standard library only.
import struct, sys
T0 = 1704067200000
src, step, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
data = open(src, "rb").read()
n = len(data) // 4
row = struct.Struct("<Iq4s")
buf = bytearray(16 * n)
pack = row.pack_into
for i in range(n):
    pack(buf, 16 * i, 0, T0 + i * step, data[4 * i:4 * i + 4])
open(out, "wb").write(buf)
print(f"{out}: {n} rows, step {step} ms")
