# SPDX-License-Identifier: AGPL-3.0-only
"""Imports the two Python packages from the wheels built out of the sdists (`make test-python-sdist`): the directory
SITE holds both unpacked. Checks that they are the unpacked ones and not an installed copy, that the network client
is the one without test hooks and says where `Db` went, and that the embedded module has it. Run with python -I."""
import os
import sys

site = os.path.abspath(sys.argv[1])
sys.path.insert(0, site)
import tsdb_client  # noqa: E402
import tsdb_embedded  # noqa: E402

for m in (tsdb_client, tsdb_embedded):
    assert m.__file__.startswith(site), m.__file__
assert hasattr(tsdb_embedded, "Db")
assert not hasattr(tsdb_client, "_set_client_nonce"), "a wheel with the test hooks"
try:
    tsdb_client.Db
except ImportError as e:
    assert "intellistream-tsdb-embedded" in str(e), e
else:
    raise SystemExit("tsdb_client.Db must say where it went")
print("ok: tsdb_client and tsdb_embedded import from", site)
