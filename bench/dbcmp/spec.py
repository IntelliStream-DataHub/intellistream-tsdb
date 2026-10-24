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
POINTS = 1000000
out = sys.stdout
for cls, win, nb in (("raw", 3600, 0), ("agg1d", 86400, 300), ("agg7d", 604800, 3600)):
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
