"""The README's quick start runs against a live server."""

import pathlib
import re
import subprocess
import sys

README = pathlib.Path(__file__).resolve().parents[1] / "README.md"


def test_quick_start(server):
    text = README.read_text()
    section = text[text.index("## Quick start") :]
    code = re.search(r"```python\n(.*?)\n```", section, re.S).group(1)
    assert "port=7070" in code
    code = code.replace("port=7070", f"port={server.port}")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    lines = out.stdout.splitlines()
    count = lambda p: sum(1 for line in lines if line.startswith(p))  # noqa: E731
    assert (count("raw "), count("hour"), count("day ")) == (3, 6, 2), out.stdout
    assert lines[0] == "raw  1700085540000 59.0", out.stdout
    # Oslo days begin at 23:00 UTC in November
    assert lines[-2:] == ["day  1699916400000 60 1770.0", "day  1700002800000 1380 40710.0"], out.stdout


def test_embedded(tmp_path):
    text = README.read_text()
    section = text[text.index("## Embedded") :]
    code = re.search(r"```python\n(.*?)\n```", section, re.S).group(1)
    assert '"/tmp/tsdb-embedded"' in code
    code = code.replace('"/tmp/tsdb-embedded"', repr(str(tmp_path / "db")))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.splitlines() == [
        "raw  1700000000000 0.0",
        "raw  1700000060000 1.0",
        "raw  1700000120000 2.0",
        "day  1699916400000 47 1081.0",
        "day  1700002800000 1393 41399.0",
    ], out.stdout


def test_many(tmp_path):
    text = README.read_text()
    section = text[text.index("## Many queries at once") :]
    code = re.search(r"```python\n(.*?)\n```", section, re.S).group(1)
    assert '"/tmp/tsdb-many"' in code
    code = code.replace('"/tmp/tsdb-many"', repr(str(tmp_path / "db")))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.splitlines() == [
        "raw   pump0 60 60.0",
        "raw   pump1 60 120.0",
        "raw   pump2 60 180.0",
        "hours 3 25 8640.0",
    ]


def test_analytics(tmp_path):
    text = README.read_text()
    section = text[text.index("## Spread, time-weighted") :]
    code = re.search(r"```python\n(.*?)\n```", section, re.S).group(1)
    assert '"/tmp/tsdb-analytics"' in code
    code = code.replace('"/tmp/tsdb-analytics"', repr(str(tmp_path / "db")))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.splitlines() == [
        "spread 30.0 125.0 11.180339887498949",
        "twa 20.0 4000",
        "chart [0, 1000, 4000]",
        "last 4000 40.0",
    ], out.stdout


def test_multi(tmp_path):
    text = README.read_text()
    section = text[text.index("## Many series in one request") :]
    code = re.search(r"```python\n(.*?)\n```", section, re.S).group(1)
    assert '"/tmp/tsdb-multi"' in code
    code = code.replace('"/tmp/tsdb-multi"', repr(str(tmp_path / "db")))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.splitlines() == ["3 [0, 25, 50, 75] 8640.0", "25 94.0", "[1.0, 2.0, 3.0]"]
