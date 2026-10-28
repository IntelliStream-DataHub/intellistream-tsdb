# The query list every system runs: python3 -I spec.py DATA N > spec.tsv
# One query per line:  class  series  start_ms  end_ms  bucket_ms
#   raw    3600 samples of one series                 agg1d  86400 samples in buckets of 300
#   agg7d  604800 samples in buckets of 3600          point  one existing row (end = start + 1)
# Windows are drawn like tsdbc-bench, in samples of the series' own step, so every sensor kind does
# the same work; point lookups pick random rows of the RowBinary file. The seed is fixed so that
# reruns and the reference answers line up. NSERIES (100) is the number of series in the file, for
# smaller samples of it (tsdb-bench --export --series N).
import os, random, struct, sys
T0 = 1704067200000
STEP = [60000, 1000, 10000, 10000, 1, 1000, 1000]  # kind = series % 7
path, n = sys.argv[1], int(sys.argv[2])
rnd = random.Random(20261024)
NSERIES = int(os.environ.get("NSERIES", "100"))
POINTS = int(os.environ.get("POINTS", "1000000"))  # samples per series (> the agg7d window, 604800 / SPEC_DIV)
# SPEC_DIV (1) divides every window and bucket, in samples, for the smoke test's tiny data (run.sh smoke: 60, so
# raw 60, agg1d 1440 in buckets of 5, agg7d 10080 in buckets of 60); the class names stay
DIV = int(os.environ.get("SPEC_DIV", "1"))
if 604800 // DIV >= POINTS:
    sys.exit(f"spec.py: POINTS {POINTS} must exceed the agg7d window of {604800 // DIV} samples (SPEC_DIV {DIV})")
out = sys.stdout
for cls, win, nb in (("raw", 3600 // DIV, 0), ("agg1d", 86400 // DIV, 300 // DIV), ("agg7d", 604800 // DIV, 3600 // DIV)):
    for _ in range(n):
        s = rnd.randrange(NSERIES); st = STEP[s % 7]
        a = T0 + rnd.randrange(POINTS - win) * st
        out.write(f"{cls}\t{s}\t{a}\t{a + win * st}\t{nb * st}\n")
with open(path, "rb") as f:
    f.seek(0, 2); rows = f.tell() // 16
    for _ in range(n):
        f.seek(rnd.randrange(rows) * 16)
        s, t, v = struct.unpack("<Iqf", f.read(16))
        out.write(f"point\t{s}\t{t}\t{t + 1}\t0\n")
