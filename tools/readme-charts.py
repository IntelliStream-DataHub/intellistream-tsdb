#!/usr/bin/env python3
"""Draws the README's benchmark charts: docs/img/bench-{servers,embedded,100b}-{light,dark}.svg.

    python3 tools/readme-charts.py             # write the SVGs
    python3 tests/check_site.py                # (make check-site) fails when they differ from what this draws

The numbers come from tools/readme-charts.json, which holds them as BENCHMARKS.md prints them (the check makes sure
every one is there). Each chart is small multiples: one panel per measure, one bar per system, tsdb highlighted.

- Throughput panels (ingest, read): the bar is speed relative to tsdb, so tsdb's is the full bar and a system
  N times slower has a shorter one. The scale is logarithmic (every factor of 10 takes the same length), and the
  axis runs from tsdb's speed down to ten times below the panel's slowest system, so 2.6x and 2,423x both stay
  readable and the slowest bar is still visible.
- Latency panels (TWA, LTTB): the bar is the time relative to tsdb's on a log scale, shorter is better, tsdb's the
  shortest (a system N times slower is log N long).
- Size panels: times tsdb's size on a log scale, shorter is better, tsdb's the shortest.
- Memory: gigabytes on a linear scale; a red tag beside the bar says where the system failed (at 32 clients).
- No number (not measured, no such function): a marked box with the reason, never a bar.

Static SVG, text as text (a system font stack), no scripts: GitHub shows README images through <img>, which runs
none. One file per colour scheme, for <picture> with prefers-color-scheme. Python standard library only; the output
depends only on the data, so the check can compare bytes. Label widths are estimated, and the drawing refuses a
label that would run past its panel.
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

# Other systems: a muted green, at least 3:1 against GitHub's page (#6a9a78 on white 3.2:1, #557f61 on #0d1117
# 4.1:1); labels sit beside the bars in the text colours, which meet 4.5:1.
THEMES = {
    "light": dict(text="#1f2328", muted="#59636e", us="#1d5fa8", bar="#6a9a78", rule="#d1d9e0",
                  fail_fill="#ffebe9", fail_line="#cf222e", fail_text="#82071e",
                  none_fill="#f6f8fa", none_line="#afb8c1", none_text="#59636e"),
    "dark": dict(text="#e6edf3", muted="#9198a1", us="#6cb1ff", bar="#557f61", rule="#3d444d",
                 fail_fill="#3d1214", fail_line="#f85149", fail_text="#ffa198",
                 none_fill="#151b23", none_line="#3d444d", none_text="#9198a1"),
}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"

W = 830          # GitHub's README column
GAP = 28         # between the two columns of panels
PW = (W - GAP) // 2
NAME_W = 100     # system names
BAR_X = NAME_W + 4
BAR_W = 118      # speed and size bars; their labels take the rest of the panel
MEM_W = 104      # memory bars leave room for the value and a failure tag
ROW = 20
HEAD = 44


def num(s):
    """The number at the start of a table cell: '1,524 M' -> 1524.0, '2.6x' -> 2.6."""
    m = re.match(r"\s*([\d,]*\.?\d+)", s)
    return float(m.group(1).replace(",", ""))


def times(m):
    return f"{m:.1f}" if m < 10 else f"{m:,.0f}"


def text_w(s, size):
    """A generous estimate of the width of s in a system sans-serif at size px."""
    w = 0.0
    for ch in s:
        if ch in "il.,:;'|! ()":
            w += 0.30
        elif ch.isupper() or ch in "mw×M%":
            w += 0.72
        else:
            w += 0.56
    return w * size


def rows_of(panel, systems):
    """(system, kind, length 0..1, label, tag): kind is us, bar or none."""
    out = []
    vals = panel["values"]
    kind = panel["kind"]
    if kind == "size":
        base = num(vals[0])
        ratios = [num(v) / base for v in vals if not isinstance(v, dict)]
        top = math.log(max(ratios + [2.0]))
        for i, (name, v) in enumerate(zip(systems, vals)):
            r = num(v) / base
            out.append((name, "us" if i == 0 else "bar", math.log(r) / top, v if i == 0 else f"{times(r)}× ({v})", None))
    elif kind == "slower":
        unit = " rows/s" if panel["unit"] == "rows/s" else ""
        slow = [num(v[1]) for v in vals[1:] if not isinstance(v, dict)]
        span = math.log(max(slow + [1.0])) + math.log(10)  # tsdb down to ten times below the slowest
        for i, (name, v) in enumerate(zip(systems, vals)):
            if isinstance(v, dict):
                out.append((name, "none", None, v["none"], None))
            elif i == 0:
                out.append((name, "us", 1.0, v[0] + unit, None))
            else:
                r = num(v[1])
                out.append((name, "bar", 1 - math.log(r) / span, f"{v[1].replace('x', '')}× slower ({v[0]}{unit})",
                            None))
    elif kind == "latency":  # [time, "29x"], like "slower", drawn the other way: longer is slower
        slow = [num(v[1]) for v in vals[1:] if not isinstance(v, dict)]
        top = math.log(max(slow + [2.0]))
        for i, (name, v) in enumerate(zip(systems, vals)):
            if isinstance(v, dict):
                out.append((name, "none", None, v["none"], None))
            elif i == 0:
                out.append((name, "us", 0.0, v[0], None))
            else:
                r = num(v[1])
                out.append((name, "bar", math.log(r) / top, f"{v[1].replace('x', '')}× slower ({v[0]})", None))
    else:  # absolute: [value] or [value, failure tag]
        top = max([num(v[0]) for v in vals] + [num(panel.get("limit", "0"))])
        for i, (name, v) in enumerate(zip(systems, vals)):
            out.append((name, "us" if i == 0 else "bar", num(v[0]) / top, v[0], v[1] if len(v) > 1 else None))
    return out


def subtitle(panel):
    """The line under a panel's title: its unit, which way is better, and the scale."""
    if panel.get("note"):
        return panel["note"]
    return {"size": f"{panel['unit']}; times tsdb's size; log scale",
            "slower": "speed; tsdb = full bar; log scale",
            "latency": f"{panel['unit']}; shorter is better; log scale",
            "absolute": f"{panel['unit']}; linear scale"}[panel["kind"]]


def site_html():
    """{chart name: HTML} for the site (site/index.html, site/docs/benchmarks.html): one <figure> per panel with a
    table of the same rows, bar lengths and labels as the SVG, which site.js draws as animated bars. make check-site
    compares the pages' copies with this, so the site and the README cannot disagree."""
    with open(DATA, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for name, chart in data["charts"].items():
        figs = []
        for i, panel in enumerate(chart["panels"]):
            rows = []
            for sysname, kind, frac, label, tag in rows_of(panel, chart["systems"]):
                attrs = ' class="us"' if kind == "us" else ""
                if kind == "none":
                    attrs += f' data-none="{escape(label)}"'
                else:
                    attrs += f' data-len="{max(frac, 0.0):.4f}"'
                if tag:
                    attrs += f' data-tag="{escape(tag)}"'
                cell = escape(label) + (f" ({escape(tag)})" if tag else "")
                rows.append(f'<tr{attrs}><th scope="row">{escape(sysname)}</th><td>{cell}</td></tr>')
            figs.append(f'<figure class="chart" id="{name}-{i + 1}">\n'
                        f'<figcaption><h3>{escape(panel["title"])}</h3><p>{escape(subtitle(panel))}</p></figcaption>\n'
                        f'<div class="chart-svg"></div>\n'
                        f'<details open><summary>The numbers</summary><div class="table-wrap">'
                        f'<table class="chart-data" data-kind="panel" data-source="BENCHMARKS.md">\n'
                        f'<thead><tr><th scope="col">System</th><th scope="col">{escape(subtitle(panel))}</th></tr>'
                        f'</thead>\n<tbody>\n' + "\n".join(rows) + "\n</tbody></table></div></details>\n</figure>")
        notes = ["Speed: tsdb is the full bar. Size, time and memory: shorter is better. A box instead of a bar: no "
                 "number, and why."] + chart.get("footnotes", [])
        notes_html = " ".join(escape(n) for n in notes)
        out[name] = (f'\n<div class="charts">\n' + "\n".join(figs) + f'\n</div>\n<p class="chart-notes">{notes_html} '
                     f'Exact numbers: <a href="https://github.com/IntelliStream-DataHub/intellistream-tsdb/blob/main/'
                     f'BENCHMARKS.md">BENCHMARKS.md</a>.</p>\n')
    return out


def panel_svg(panel, systems, x0, y0):
    rows = rows_of(panel, systems)
    p = []
    sub = subtitle(panel)
    p.append(f'<text x="{x0}" y="{y0 + 14}" class="h">{escape(panel["title"])}</text>')
    p.append(f'<text x="{x0}" y="{y0 + 31}" class="m">{escape(sub)}</text>')
    if text_w(sub, 11.5) > PW:
        raise SystemExit(f"readme-charts: subtitle {sub!r} is too wide for its panel")
    bw = MEM_W if panel["kind"] == "absolute" and any(r[4] for r in rows) else BAR_W
    y = y0 + HEAD
    p.append(f'<line x1="{x0 + BAR_X}" x2="{x0 + BAR_X}" y1="{y - 4}" y2="{y + ROW * len(rows)}" class="r"/>')
    for name, kind, frac, label, tag in rows:
        cls = ' class="us"' if kind == "us" else ""
        p.append(f'<g{cls}><text x="{x0}" y="{y + 13}" class="n">{escape(name)}</text>')
        if kind == "none":
            p.append(f'<rect x="{x0 + BAR_X}" y="{y + 2}" width="{PW - BAR_X}" height="15" rx="3" class="none"/>'
                     f'<text x="{x0 + BAR_X + 6}" y="{y + 13}" class="nonet">{escape(label)}</text>')
        else:
            w = max(3.0, frac * bw)
            lx = x0 + BAR_X + w + 5
            p.append(f'<rect x="{x0 + BAR_X}" y="{y + 3}" width="{w:.1f}" height="13" rx="2" class="b"/>'
                     f'<text x="{lx:.1f}" y="{y + 13}" class="v">{escape(label)}</text>')
            end = lx + text_w(label, 11.5)
            if tag:
                tw = text_w(tag, 11) + 12
                tx = end + 6
                p.append(f'<rect x="{tx:.1f}" y="{y + 2}" width="{tw:.1f}" height="15" rx="7.5" class="fail"/>'
                         f'<text x="{tx + 6:.1f}" y="{y + 13}" class="failt">{escape(tag)}</text>')
                end = tx + tw
            if end > x0 + PW + 2:
                raise SystemExit(f"readme-charts: {panel['title']}, {name}: the label runs {end - x0 - PW:.0f} px "
                                 f"past the panel")
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
            s, ph = panel_svg(panel, chart["systems"], j * (PW + GAP), y)
            parts.append(s)
            h = max(h, ph)
        y += h + 22
    y += 2
    parts.append(f'<text x="0" y="{y}" class="m">Ingest, read: tsdb is the full bar. Time, size, memory: shorter is better. '
                 f'A box: no number, and why. Exact numbers: BENCHMARKS.md</text>')
    for line in chart.get("footnotes", []):
        y += 16
        if text_w(line, 11.5) > W * 1.08:  # the estimate runs about 10% wide on prose
            raise SystemExit(f"readme-charts: footnote {line!r} is too wide")
        parts.append(f'<text x="0" y="{y}" class="m">{escape(line)}</text>')
    height = y + 8
    style = (f"text{{font-family:{FONT};font-size:12px;fill:{t['text']}}}"
             f".h{{font-size:14px;font-weight:600}}.m{{font-size:11.5px;fill:{t['muted']}}}"
             f".n{{fill:{t['text']}}}.v{{font-size:11.5px;fill:{t['muted']}}}"
             f".us .n,.us .v{{font-weight:700;fill:{t['text']}}}"
             f".b{{fill:{t['bar']}}}.us .b{{fill:{t['us']}}}"
             f".r{{stroke:{t['rule']};stroke-width:1}}"
             f".fail{{fill:{t['fail_fill']};stroke:{t['fail_line']};stroke-width:1}}"
             f".failt{{font-size:11px;fill:{t['fail_text']}}}"
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
