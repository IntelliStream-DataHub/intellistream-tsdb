#!/usr/bin/env python3
"""Mean, standard deviation, min and max over repeated runs of a benchmark, number by number.

    runstats.py DIR

Groups the files in DIR named <group>.<run> (run = 1, 2, ...), lines them up by position, and writes
DIR/<group>.mean.txt: every line of run 1 with each number replaced by the MEAN over the runs, followed (for lines
whose numbers differ between runs) by a line "#   sd% [min-max]" per number, and at the end the run count. The
individual runs stay in the <group>.<run> files. Lines whose text (numbers aside) differs between runs are kept
from run 1 and marked "# TEXT DIFFERS" - the analyst should look at the raw files there.
"""
import os
import re
import statistics
import sys
from collections import defaultdict

NUM = re.compile(r"(?<![A-Za-z_\d.])-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def split(line):
    nums = [m.group(0) for m in NUM.finditer(line)]
    return NUM.sub("\x00", line), nums


def fmt(x, like):
    dec = len(like.split(".")[1]) if "." in like and "e" not in like.lower() else 0
    return f"{x:.{dec}f}"


def summarize(files):
    runs = []
    for f in files:
        with open(f, errors="replace") as fh:
            runs.append(fh.read().splitlines())
    out = []
    for i, line in enumerate(runs[0]):
        tpl, nums = split(line)
        if not nums:
            out.append(line)
            continue
        vals = [[] for _ in nums]
        same = True
        for r in runs:
            if i >= len(r):
                same = False
                continue
            t2, n2 = split(r[i])
            if t2.split() != tpl.split() or len(n2) != len(nums):  # column padding varies with the numbers
                same = False
                continue
            for k, v in enumerate(n2):
                vals[k].append(float(v))
        means = [statistics.fmean(v) if v else float("nan") for v in vals]
        parts = tpl.split("\x00")
        s = parts[0]
        for k, m in enumerate(means):
            s += fmt(m, nums[k]) + parts[k + 1]
        out.append(s + ("" if same else "   # TEXT DIFFERS between runs"))
        if any(len(set(v)) > 1 for v in vals):
            stats = []
            for k, v in enumerate(vals):
                if len(v) > 1 and len(set(v)) > 1:
                    sd = statistics.stdev(v)
                    pct = sd / abs(means[k]) * 100 if means[k] else float("nan")
                    stats.append(f"{fmt(means[k], nums[k])}: sd {pct:.1f}% [{fmt(min(v), nums[k])}-{fmt(max(v), nums[k])}]")
            out.append("#   " + " | ".join(stats))
    out.append(f"# mean over {len(runs)} runs: {' '.join(os.path.basename(f) for f in files)}")
    return "\n".join(out) + "\n"


def main():
    d = sys.argv[1]
    groups = defaultdict(list)
    for name in os.listdir(d):
        m = re.fullmatch(r"(.+)\.(\d+)", name)
        if m and os.path.isfile(os.path.join(d, name)):
            groups[m.group(1)].append((int(m.group(2)), os.path.join(d, name)))
    for g, fs in sorted(groups.items()):
        fs.sort()
        with open(os.path.join(d, g + ".mean.txt"), "w") as fh:
            fh.write(summarize([f for _, f in fs]))
    print(f"runstats: {len(groups)} groups in {d}")


if __name__ == "__main__":
    main()
