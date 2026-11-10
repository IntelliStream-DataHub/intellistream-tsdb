# SPDX-License-Identifier: Apache-2.0
"""`Db`, the embedded database, left this package in 0.11.0 (the engine is AGPL-3.0-only, this client Apache-2.0):
asking for it says where it went, and never imports the other package."""

import subprocess
import sys

import pytest

import tsdb_client


def test_db_moved_to_its_own_package():
    with pytest.raises(ImportError) as e:
        tsdb_client.Db  # noqa: B018
    assert "intellistream-tsdb-embedded" in str(e.value) and "AGPL-3.0" in str(e.value)
    with pytest.raises(ImportError):
        from tsdb_client import Db  # noqa: F401
    with pytest.raises(AttributeError):
        tsdb_client.NoSuchThing  # noqa: B018


def test_asking_for_db_does_not_import_the_embedded_package():
    code = (
        "import sys, tsdb_client\n"
        "try:\n    tsdb_client.Db\nexcept ImportError:\n    pass\n"
        "assert 'tsdb_embedded' not in sys.modules, 'tsdb_client imported tsdb_embedded'\n"
        "print('ok')"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0 and out.stdout.strip() == "ok", out.stderr
