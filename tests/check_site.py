#!/usr/bin/env python3
"""Checks the GitHub Pages site in site/, so that it cannot rot quietly.

    python3 tests/check_site.py          # check (make test runs this)
    python3 tests/check_site.py --fix    # copy every code block from its source again

What it checks, with the Python standard library only:

- Every HTML page is well formed: every element that is not void is closed, in order, and no id is used twice.
- Every internal link (href, src) points at a file inside site/ that exists, and a #fragment at an id that exists.
- Every code block on the site is a copy of its source: <code data-src="..."> names it, and its text must be
  that source exactly. The sources are the programs and READMEs the test suites run (examples/*.c,
  client/rust/examples/*.rs, the Python and Java README sections their tests execute or compile, the README's
  quick start), so a change there fails this check until the site is updated (--fix does it). A <pre> without
  a data-src is refused: code on the site comes from somewhere a test looks at.
    data-src="PATH"              the whole file
    data-src="PATH#Heading"      a Markdown file: the first fenced block in the code's language (the class
                                 language-X, or data-lang) after the heading that starts with that text;
                                 data-block="N" takes the Nth such block instead
    data-src="PATH#L10-L20"      lines 10 to 20 of the file, with their common indentation removed
    data-src="PATH#@A..B"        from the line that starts with A up to the next line that starts with B (which
                                 survives edits elsewhere in the file, unlike a line range)
- Every number in a benchmark chart's table (<td data-v>) appears in the results file the table names
  (data-source), so a chart cannot show a number the published results do not have.
- The scripts in site/assets have no syntax errors (node --check, when node is installed).
- The README's benchmark charts (docs/img/bench-*.svg) are exactly what tools/readme-charts.py draws from
  tools/readme-charts.json, and every number in that file appears in BENCHMARKS.md's summary or memory tables.
"""

import html
import html.parser
import os
import re
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = os.path.join(ROOT, "site")
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
# SVG shapes are written self-closed (<path ... />); the parser reports those through handle_startendtag.

errors = []


def err(path, msg):
    errors.append(f"{os.path.relpath(path, ROOT)}: {msg}")


class Page(html.parser.HTMLParser):
    def __init__(self, path):
        super().__init__(convert_charrefs=True)
        self.path = path
        self.stack = []
        self.ids = set()
        self.links = []
        self.pre_open = None

    def handle_starttag(self, tag, attrs):
        self._attrs(tag, attrs)
        if tag not in VOID:
            self.stack.append((tag, self.getpos()[0]))
        if tag == "pre":
            self.pre_open = self.getpos()[0]
        if tag == "code" and self.pre_open is not None and "data-src" not in dict(attrs):
            err(self.path, f"line {self.getpos()[0]}: a code block without data-src")

    def handle_startendtag(self, tag, attrs):
        self._attrs(tag, attrs)

    def _attrs(self, tag, attrs):
        a = dict(attrs)
        if a.get("id"):
            if a["id"] in self.ids:
                err(self.path, f"line {self.getpos()[0]}: id {a['id']!r} used twice")
            self.ids.add(a["id"])
        for k in ("href", "src"):
            if a.get(k) is not None:
                self.links.append((a[k], self.getpos()[0]))

    def handle_endtag(self, tag):
        if tag in VOID:
            return
        if not self.stack:
            err(self.path, f"line {self.getpos()[0]}: </{tag}> closes nothing")
            return
        top, line = self.stack.pop()
        if top != tag:
            err(self.path, f"line {self.getpos()[0]}: </{tag}> closes <{top}> of line {line}")
            self.stack.append((top, line))
            # pop to the matching tag, if any, so one mistake is reported once
            for i in range(len(self.stack) - 1, -1, -1):
                if self.stack[i][0] == tag:
                    del self.stack[i:]
                    break
        if tag == "pre":
            self.pre_open = None

    def close(self):
        super().close()
        for tag, line in self.stack:
            err(self.path, f"<{tag}> of line {line} is never closed")


def pages():
    for d, _, files in os.walk(SITE):
        for f in sorted(files):
            if f.endswith(".html"):
                yield os.path.join(d, f)


def source_text(spec, lang, block, page):
    path, _, frag = spec.partition("#")
    full = os.path.join(ROOT, path)
    if not os.path.isfile(full):
        err(page, f"data-src {spec!r}: no such file")
        return None
    with open(full, encoding="utf-8") as f:
        text = f.read()
    if not frag:
        return text.rstrip("\n")
    m = re.fullmatch(r"L(\d+)-L(\d+)", frag)
    t = re.fullmatch(r"@(.+?)\.\.(.+)", frag)
    if t:  # from the line that starts with the first text up to the next line that starts with the second
        lines = text.split("\n")
        a = next((i for i, x in enumerate(lines) if x.startswith(t.group(1))), None)
        b = next((i for i, x in enumerate(lines) if a is not None and i > a and x.startswith(t.group(2))), None)
        if a is None or b is None:
            err(page, f"data-src {spec!r}: no lines from {t.group(1)!r} to {t.group(2)!r}")
            return None
        return "\n".join(lines[a:b]).rstrip("\n")
    if m:
        lines = text.split("\n")[int(m.group(1)) - 1 : int(m.group(2))]
        pad = min((len(x) - len(x.lstrip()) for x in lines if x.strip()), default=0)
        return "\n".join(x[pad:] for x in lines).rstrip("\n")
    lines = text.split("\n")
    start, fenced = None, False
    for i, x in enumerate(lines):
        if x.strip().startswith("```"):
            fenced = not fenced  # a "# comment" in a shell block is not a heading
        elif not fenced and re.match(r"#+ ", x) and x.split(" ", 1)[1].startswith(frag):
            start = i
            break
    if start is None:
        err(page, f"data-src {spec!r}: no heading starting with {frag!r}")
        return None
    if not lang:
        err(page, f"data-src {spec!r}: the code needs a language (class language-X or data-lang)")
        return None
    want = "" if lang == "plain" else lang  # data-lang="plain": a fence with no language (program output)
    n = 0
    i = start + 1
    while i < len(lines):
        s = lines[i].strip()
        if s.startswith("```"):
            pad = len(lines[i]) - len(lines[i].lstrip())
            j = i + 1
            while j < len(lines) and lines[j].strip() != "```":
                j += 1
            if s[3:] == want:
                n += 1
                if n == block:
                    return "\n".join(x[pad:] for x in lines[i + 1 : j]).rstrip("\n")
            i = j
        i += 1
    err(page, f"data-src {spec!r}: no block {block} of ```{lang} after that heading")
    return None


CODE = re.compile(r"(<code\b([^>]*)>)(.*?)(</code>)", re.S)


def attr(attrs, name):
    m = re.search(r'\b' + name + r'="([^"]*)"', attrs)
    return html.unescape(m.group(1)) if m else None


def check_code(page, raw, fix):
    changed = False

    def one(m):
        nonlocal changed
        attrs = m.group(2)
        spec = attr(attrs, "data-src")
        if spec is None:
            return m.group(0)
        cls = attr(attrs, "class") or ""
        lm = re.search(r"\blanguage-(\w+)", cls)
        lang = attr(attrs, "data-lang") or (lm.group(1) if lm else None)
        block = int(attr(attrs, "data-block") or 1)
        want = source_text(spec, lang, block, page)
        if want is None:
            return m.group(0)
        have = html.unescape(m.group(3)).rstrip("\n")
        if have == want:
            return m.group(0)
        if fix:
            changed = True
            return m.group(1) + html.escape(want, quote=False) + m.group(4)
        line = raw.count("\n", 0, m.start()) + 1
        err(page, f"line {line}: the code block differs from {spec} (run python3 tests/check_site.py --fix)")
        return m.group(0)

    out = CODE.sub(one, raw)
    if changed:
        with open(page, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"updated {os.path.relpath(page, ROOT)}")
    return out


TABLE = re.compile(r'<table\b[^>]*class="[^"]*\bchart-data\b[^"]*"[^>]*>.*?</table>', re.S)


def check_numbers(page, raw):
    for t in TABLE.finditer(raw):
        line = raw.count("\n", 0, t.start()) + 1
        src = re.search(r'data-source="([^"]*)"', t.group(0)[: t.group(0).index(">")])
        if not src:
            err(page, f"line {line}: a chart table without data-source")
            continue
        full = os.path.join(ROOT, src.group(1))
        if not os.path.isfile(full):
            err(page, f"line {line}: data-source {src.group(1)!r}: no such file")
            continue
        with open(full, encoding="utf-8") as f:
            results = f.read()
        for cell in re.finditer(r"<td\b[^>]*\bdata-v=\"([^\"]*)\"[^>]*>(.*?)</td>", t.group(0), re.S):
            shown = re.sub(r"<[^>]+>", "", cell.group(2))
            num = re.match(r"\s*([\d][\d,.]*)", shown)
            if not num:
                err(page, f"line {line}: chart cell {shown!r} does not start with its number")
                continue
            n = num.group(1).rstrip(".,")
            if float(n.replace(",", "")) != float(cell.group(1)):
                err(page, f"line {line}: chart cell {shown!r} shows another number than data-v={cell.group(1)}")
            if not re.search(r"(?<![\d.,])" + re.escape(n) + r"(?![\d])", results):
                err(page, f"line {line}: chart number {n} is not in {src.group(1)}")


def check_links(page, parser, ids_of):
    for link, line in parser.links:
        if re.match(r"[a-z][a-z0-9+.-]*:", link) or link.startswith("//"):
            continue  # an outside URL (https:, mailto:, data:): not checked here
        target, _, frag = link.partition("#")
        dest = page if not target else os.path.normpath(os.path.join(os.path.dirname(page), target))
        if target.endswith("/") or os.path.isdir(dest):
            dest = os.path.join(dest, "index.html")
        if not dest.startswith(SITE + os.sep):
            err(page, f"line {line}: {link!r} leaves site/ (link to the repository on GitHub instead)")
            continue
        if not os.path.isfile(dest):
            err(page, f"line {line}: {link!r}: no such file")
            continue
        if frag and dest.endswith(".html") and frag not in ids_of(dest):
            err(page, f"line {line}: {link!r}: no id {frag!r} there")


def charts_module():
    import importlib.util

    gen = os.path.join(ROOT, "tools", "readme-charts.py")
    spec = importlib.util.spec_from_file_location("readme_charts", gen)
    mod = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True  # no tools/__pycache__ in the tree
    spec.loader.exec_module(mod)
    return mod


SITE_CHART = re.compile(r"(<!-- readme-chart ([\w-]+) -->)(.*?)(<!-- /readme-chart -->)", re.S)


def check_site_charts(page, raw, fix):
    """The site's benchmark charts are the README's: each <!-- readme-chart NAME --> block holds exactly what
    tools/readme-charts.py's site_html() makes from tools/readme-charts.json."""
    want = charts_module().site_html()
    changed = False

    def one(m):
        nonlocal changed
        if m.group(2) not in want:
            err(page, f"readme-chart {m.group(2)!r}: no such chart in tools/readme-charts.json")
            return m.group(0)
        if m.group(3) == want[m.group(2)]:
            return m.group(0)
        if fix:
            changed = True
            return m.group(1) + want[m.group(2)] + m.group(4)
        err(page, f"readme-chart {m.group(2)}: differs from tools/readme-charts.json (run tests/check_site.py --fix)")
        return m.group(0)

    out = SITE_CHART.sub(one, raw)
    if changed:
        with open(page, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"updated {os.path.relpath(page, ROOT)} (charts)")
    return out


def check_readme_charts():
    """The README's charts (docs/img/bench-*.svg) are what tools/readme-charts.py draws from its data file, and every
    number of that file is in BENCHMARKS.md's summary tables."""
    mod = charts_module()
    for name, svg in mod.render().items():
        path = os.path.join(ROOT, "docs", "img", name)
        have = open(path, encoding="utf-8").read() if os.path.isfile(path) else None
        if have != svg:
            errors.append(f"docs/img/{name}: {'missing' if have is None else 'differs from its data'} "
                          f"(run python3 tools/readme-charts.py)")
    with open(os.path.join(ROOT, "BENCHMARKS.md"), encoding="utf-8") as f:
        text = f.read()
    summary = ""
    for title in ("The summary", "Memory"):  # the chart's memory panel comes from the memory section's table
        start = text.find(f"\n## {title}\n")
        end = text.find("\n## ", start + 1)
        if start < 0:
            errors.append(f"BENCHMARKS.md: no '## {title}' section")
        summary += text[start:end] if start >= 0 else ""
    for n in mod.numbers():
        if not re.search(r"(?<![\d.,])" + re.escape(n) + r"(?![\d])", summary):
            errors.append(f"tools/readme-charts.json: {n!r} is not in BENCHMARKS.md's summary or memory tables")


def main():
    fix = "--fix" in sys.argv[1:]
    if not os.path.isdir(SITE):
        print("check_site: no site/ directory")
        return 1
    parsed = {}

    def parse(path):
        if path not in parsed:
            with open(path, encoding="utf-8") as f:
                raw = f.read()
            p = Page(path)
            p.feed(raw)
            p.close()
            parsed[path] = (p, raw)
        return parsed[path]

    all_pages = list(pages())
    for page in all_pages:
        with open(page, encoding="utf-8") as f:
            raw = f.read()
        raw = check_code(page, raw, fix)
        check_site_charts(page, raw, fix)
        parsed.pop(page, None)
        p, raw = parse(page)
        check_numbers(page, raw)
        if not re.search(r'<link rel="icon" href="[^"]*assets/favicon\.svg" type="image/svg\+xml">', raw):
            err(page, "no <link rel=\"icon\"> to assets/favicon.svg")
    for page in all_pages:
        check_links(page, parse(page)[0], lambda d: parse(d)[0].ids)
    check_readme_charts()
    node = shutil.which("node")
    scripts = sorted(f for f in os.listdir(os.path.join(SITE, "assets")) if f.endswith(".js"))
    if node:
        for f in scripts:
            r = subprocess.run([node, "--check", os.path.join(SITE, "assets", f)], capture_output=True, text=True)
            if r.returncode:
                errors.append(f"site/assets/{f}: {r.stderr.strip()}")
    else:
        print("check_site: node not found, the scripts are not syntax-checked")
    for e in errors:
        print(e)
    print(f"check_site: {len(all_pages)} pages, {'FAILED' if errors else 'ok'}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
