# SPDX-License-Identifier: AGPL-3.0-only
# The batches every system answers, made once from spec.tsv and shared by all of them:
#   python3 -I batches.py spec.tsv > batches.tsv          (NSERIES, default 100, as for spec.py)
# One line per member of a batch:  set  K  group  member  series  start_ms  end_ms  bucket_ms  spec_line
#   set full:   whole-range sets (start, end, bucket and spec_line are "-"): K = 1 is series 0..9 one at a
#               time (every sensor kind, series % 7, at least once), K = 10 the series in groups of 10
#               consecutive ones (0-9, 10-19, ...), K = 100 all of them in one batch.
#   set CLASS:  the windows of spec.tsv's class CLASS, K consecutive lines of that class per batch (lines
#               0..K-1 of the class are group 0, ...); spec_line is the 0-based line in spec.tsv, which is
#               the "i" of the reference answers (tsdbq dump). A series may occur twice in a batch: it is
#               then answered once per member.
# This grouping is the one tsdbq's CLASS.bK and "full ... --batch K" use; checktest.py asserts it.
import os, sys

NSERIES = int(os.environ.get("NSERIES", "100"))
KS = (1, 10, 100)

def full_sets(k, nseries=NSERIES):
    if k == 1:
        return [[s] for s in range(min(10, nseries))]
    if k == 100:
        return [list(range(nseries))]
    return [list(range(a, min(a + k, nseries))) for a in range(0, nseries, k)]

def class_sets(lines, k):
    """lines: [(spec_line, cls, series, start, end, bucket)] of one class, in file order."""
    return [lines[g * k:(g + 1) * k] for g in range(len(lines) // k)]

def read_spec(path):
    by = {}
    for i, l in enumerate(open(path)):
        cls, s, a, b, bk = l.split()
        by.setdefault(cls, []).append((i, cls, int(s), int(a), int(b), int(bk)))
    return by

def read(path="batches.tsv"):
    """{(set, K): [[member dict, ...] per group]}"""
    out = {}
    for l in open(path):
        st, k, g, m, s, a, b, bk, i = l.rstrip("\n").split("\t")
        grp = out.setdefault((st, int(k)), [])
        if int(g) == len(grp): grp.append([])
        num = lambda x: None if x == "-" else int(x)
        grp[int(g)].append({"m": int(m), "series": int(s), "start": num(a), "end": num(b), "bk": num(bk), "i": num(i)})
    return out

if __name__ == "__main__":
    if sys.argv[1] == "--count-full1":  # series in the K = 1 whole-range set: tsdbq full with NSERIES of it
        print(len(full_sets(1))); sys.exit(0)
    w = sys.stdout.write
    for k in KS:
        for g, ss in enumerate(full_sets(k)):
            for m, s in enumerate(ss):
                w(f"full\t{k}\t{g}\t{m}\t{s}\t-\t-\t-\t-\n")
    for cls, lines in read_spec(sys.argv[1]).items():
        for k in KS:
            for g, grp in enumerate(class_sets(lines, k)):
                for m, (i, _, s, a, b, bk) in enumerate(grp):
                    w(f"{cls}\t{k}\t{g}\t{m}\t{s}\t{a}\t{b}\t{bk}\t{i}\n")
