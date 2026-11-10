# SPDX-License-Identifier: AGPL-3.0-only
"""The README's examples run."""

import pathlib
import re
import subprocess
import sys

README = pathlib.Path(__file__).resolve().parents[1] / "README.md"


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
