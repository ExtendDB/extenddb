# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Live tests for the launcher. Requires the dev binary via EXTENDDB_BINARY
(build: cargo build --release -p extenddb --no-default-features
--features sqlite-memory,dev-mode)."""

import os
import sys
import tempfile
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import extenddb_launcher  # noqa: E402


def _health(endpoint: str) -> int:
    with urllib.request.urlopen(f"{endpoint}/health", timeout=2) as resp:
        return resp.status


def test_two_servers_distinct_ports_and_stop():
    a = extenddb_launcher.start()
    b = extenddb_launcher.start()
    try:
        assert a.port != b.port
        assert _health(a.endpoint) == 200
        assert _health(b.endpoint) == 200
        assert a.access_key_id == "AKIAIOSFODNN7EXAMPLE"
    finally:
        a.stop()
        b.stop()
    try:
        _health(a.endpoint)
        raise AssertionError("stopped server must not respond")
    except (urllib.error.URLError, OSError):
        pass


def test_context_manager_with_file_persistence():
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "data.sqlite")
        with extenddb_launcher.start(db_path=db) as eb:
            assert _health(eb.endpoint) == 200
        assert os.path.exists(db), "file-backed mode must create the database file"


if __name__ == "__main__":
    test_two_servers_distinct_ports_and_stop()
    test_context_manager_with_file_persistence()
    print("launcher tests: PASS")
