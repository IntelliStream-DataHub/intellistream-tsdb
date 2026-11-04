#!/usr/bin/env python3
"""Draws the README's benchmark charts: docs/img/bench-{servers,embedded,100b}-{light,dark}.svg.

    python3 tools/readme-charts.py             # write the SVGs
    python3 tests/check_site.py                # (make check-site) fails when they differ from what this draws

The numbers come from tools/readme-charts.json, which holds them as BENCHMARKS.md's summary tables print them (the
check makes sure every one is there). Each chart is small multiples: one panel per measure, one bar per system.
Speed and size bars are "times tsdb's number" on a log scale, so a 2x and a 2,000x difference both stay readable;
tsdb is the highlighted 1x. Memory is gigabytes on a linear scale. A system that failed or has no number gets a
marked box with the reason, never a bar.

Static SVG, text as text (a system font stack), no scripts: GitHub shows README images through <img>, which runs
none. One file per colour scheme, for <picture> with prefers-color-scheme. Python standard library only; the output
depends only on the data, so the check can compare bytes.
"""

import json
import math
import os
import re
import sys
from xml.sax.saxutils import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "tools", "readme-charts.json")
OUT = os.path.join(ROOT, "docs", "img")

THEMES = {
    "light": dict(text="#1f2328", muted="#59636e", us="#1d5fa8", bar="#a8b1bc", rule="#d1d9e0",
                  fail_fill="#ffebe9", fail_line="#cf222e", fail_text="#82071e",
                  none_fill="#f6f8fa", none_line="#afb8c1", none_text="#59636e"),
    "dark": dict(text="#e6edf3", muted="#9198a1", us="#6cb1ff", bar="#4d5661", rule="#3d444d",
                 fail_fill="#3d1214", fail_line="#f85149", fail_text="#ffa198",
                 none_fill="#151b23", none_line="#3d444d", none_text="#9198a1"),
}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"

W = 830          # GitHub's README column
GAP = 28         # between the two columns of panels
PW = (W - GAP) // 2
NAME_W = 108     # system names
BAR_X = NAME_W + 4
LABEL_W = 120    # room after the longest bar for its label
BAR_W = PW - BAR_X - LABEL_W
ROW = 20
HEAD = 44


def num(s):
    """The number at the start of a table cell: '1,524 M' -> 1524.0, '2.6x' -> 2.6."""
    m = re.match(r"\s*([\d,]*\.?\d+)", s)
    return float(m.group(1).replace(",", ""))


def times(m):
    return f"{m:.1f}" if m < 10 else f"{m:,.0f}"


def rows_of(panel, systems):
    """(system, kind, ratio or value, label): kind is us, bar, fail or none."""
    out = []
    vals = panel["values"]
    base = num(vals[0] if panel["kind"] == "size" else vals[0][0])
    for i, (name, v) in enumerate(zip(systems, vals)):
        if isinstance(v, dict):
            k = "fail" if "fail" in v else "none"
            out.append((name, k, None, v[k]))
            continue
        if panel["kind"] == "size":
            r = num(v) / base
            label = v if i == 0 else f"{times(r)}× ({v})"
            out.append((name, "us" if i == 0 else "bar", r, label))
        elif panel["kind"] == "slower":
            if i == 0:
                out.append((name, "us", 1.0, v[0]))
            else:
                out.append((name, "bar", num(v[1]), f"{v[1].replace('x', '')}× ({v[0]})"))
        else:  # absolute
            out.append((name, "us" if i == 0 else "bar", num(v[0]), v[0]))
    return out


def panel_svg(panel, systems, t, x0, y0):
    rows = rows_of(panel, systems)
    p = []
    unit = panel["unit"]
    scale = {"size": "times tsdb's size, log scale", "slower": "times slower than tsdb, log scale",
             "absolute": "linear scale"}[panel["kind"]]
    if "limit" in panel:
        scale = f"a full bar is the {panel['limit']} GB limit"
    p.append(f'<text x="{x0}" y="{y0 + 14}" class="h">{escape(panel["title"])}</text>')
    p.append(f'<text x="{x0}" y="{y0 + 31}" class="m">{escape(unit)}; {scale}</text>')
    if panel["kind"] == "absolute":
        top = max([r[2] for r in rows if r[2] is not None] + [num(panel.get("limit", "0"))])
        length = lambda v: BAR_W * v / top  # noqa: E731
    else:
        top = max([r[2] for r in rows if r[2] is not None] + [2.0])
        length = lambda v: BAR_W * math.log(v) / math.log(top)  # noqa: E731
    y = y0 + HEAD
    p.append(f'<line x1="{x0 + BAR_X}" x2="{x0 + BAR_X}" y1="{y - 4}" y2="{y + ROW * len(rows)}" class="r"/>')
    for name, kind, v, label in rows:
        cls = ' class="us"' if kind == "us" else ""
        p.append(f'<g{cls}><text x="{x0}" y="{y + 13}" class="n">{escape(name)}</text>')
        if kind in ("fail", "none"):
            p.append(f'<rect x="{x0 + BAR_X}" y="{y + 2}" width="{PW - BAR_X}" height="15" rx="3" class="{kind}"/>'
                     f'<text x="{x0 + BAR_X + 6}" y="{y + 13}" class="{kind}t">{escape(label)}</text>')
        else:
            w = max(3.0, length(v))
            p.append(f'<rect x="{x0 + BAR_X}" y="{y + 3}" width="{w:.1f}" height="13" rx="2" class="b"/>'
                     f'<text x="{x0 + BAR_X + w + 5:.1f}" y="{y + 13}" class="v">{escape(label)}</text>')
        p.append("</g>")
        y += ROW
    return "\n".join(p), HEAD + ROW * len(rows)


def chart_svg(chart, theme):
    t = THEMES[theme]
    parts, y = [], 0
    panels = chart["panels"]
    for i in range(0, len(panels), 2):
        h = 0
        for j, panel in enumerate(panels[i : i + 2]):
            s, ph = panel_svg(panel, chart["systems"], t, j * (PW + GAP), y)
            parts.append(s)
            h = max(h, ph)
        y += h + 22
    y += 2
    parts.append(f'<text x="0" y="{y}" class="m">tsdb is 1×. A box instead of a bar: no number, and why. '
                 f'Exact numbers: BENCHMARKS.md</text>')
    height = y + 8
    style = (f"text{{font-family:{FONT};font-size:12px;fill:{t['text']}}}"
             f".h{{font-size:14px;font-weight:600}}.m{{font-size:11.5px;fill:{t['muted']}}}"
             f".n{{fill:{t['text']}}}.v{{font-size:11.5px;fill:{t['muted']}}}"
             f".us .n,.us .v{{font-weight:700;fill:{t['text']}}}"
             f".b{{fill:{t['bar']}}}.us .b{{fill:{t['us']}}}"
             f".r{{stroke:{t['rule']};stroke-width:1}}"
             f".fail{{fill:{t['fail_fill']};stroke:{t['fail_line']};stroke-width:1;stroke-dasharray:3 2}}"
             f".failt{{font-size:11.5px;fill:{t['fail_text']}}}"
             f".none{{fill:{t['none_fill']};stroke:{t['none_line']};stroke-width:1}}"
             f".nonet{{font-size:11.5px;font-style:italic;fill:{t['none_text']}}}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" '
            f'viewBox="0 0 {W} {height}">\n<style>{style}</style>\n' + "\n".join(parts) + "\n</svg>\n")


def render():
    """{file name: SVG text} for every chart and theme."""
    with open(DATA, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for name, chart in data["charts"].items():
        for theme in THEMES:
            out[f"{name}-{theme}.svg"] = chart_svg(chart, theme)
    return out


def numbers():
    """Every number string of the data file, as it appears in the tables."""
    with open(DATA, encoding="utf-8") as f:
        data = json.load(f)
    found = []

    def walk(v):
        if isinstance(v, list):
            for x in v:
                walk(x)
        elif isinstance(v, str) and re.match(r"[\d,.]+( [A-Za-z]+)?x?$", v):
            found.append(v)

    for chart in data["charts"].values():
        for panel in chart["panels"]:
            walk(panel["values"])
            if "limit" in panel:
                found.append(panel["limit"])
    return found


def main():
    os.makedirs(OUT, exist_ok=True)
    for name, svg in render().items():
        with open(os.path.join(OUT, name), "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"wrote docs/img/{name} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
