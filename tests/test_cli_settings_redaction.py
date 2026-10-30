# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""`extenddb settings list` and `get` mask secret values.

The CLI reads the catalog directly, so these tests run `extenddb init` into an
isolated deployment and drive the `settings` command against it; no server is
started. The SQLite class runs when the binary is a SQLite build; the
PostgreSQL class runs when EXTENDDB_TEST_PG_CONNECTION_STRING is set. Each
skips otherwise. The CLI lifecycle tests read the same variable and fail when
it is unset.

Expected behaviour: `list` prints `••••••••` for `encryption_key` and for
`data_database_connection_string` (PostgreSQL), the same mask the management
API prints; `get` prints the mask for those keys unless `--show-secret` is
passed, in which case it prints the stored value.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess

import pytest

from lifecycle_helpers import PG_CONN, _init_args, _patch_config_port, _run_extenddb

MASK = "••••••••"
ADMIN_PASSWORD = "TestPass1!"

EXTENDDB_BINARY = os.environ.get(
    "EXTENDDB_BINARY",
    os.path.join(os.path.dirname(__file__), "..", "target", "release", "extenddb"),
)


def _settings(config_path: str, *args: str) -> subprocess.CompletedProcess:
    # `init --backend sqlite` writes a database path relative to the working
    # directory, so run the CLI from the deployment directory on every backend.
    return subprocess.run(
        [EXTENDDB_BINARY, "settings", "--config", config_path, *args],
        cwd=os.path.dirname(config_path),
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=60,
        check=False,
    )


def _parse_list(stdout: str) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line in stdout.splitlines():
        if " = " in line:
            k, v = line.split(" = ", 1)
            rows[k.strip()] = v
    return rows


def _assert_masked_list_and_get(config_path: str, secret_keys: dict[str, str]) -> None:
    """`secret_keys` maps each secret setting key to its stored value."""
    res = _settings(config_path, "list")
    assert res.returncode == 0, res.stderr
    rows = _parse_list(res.stdout)
    for key, stored in secret_keys.items():
        assert rows.get(key) == MASK, f"{key} in list: {rows.get(key)!r}"
        assert stored not in res.stdout
    # Non-secret keys seeded by `init` still print their values.
    assert rows.get("data_database_name"), rows
    assert rows.get("control_plane_delay_seconds") == "0.25", rows

    for key, stored in secret_keys.items():
        res = _settings(config_path, "get", key)
        assert res.returncode == 0, res.stderr
        assert res.stdout.strip() == MASK
        assert stored not in res.stdout

        res = _settings(config_path, "get", "--show-secret", key)
        assert res.returncode == 0, res.stderr
        assert res.stdout.strip() == stored

    res = _settings(config_path, "get", "control_plane_delay_seconds")
    assert res.returncode == 0, res.stderr
    assert res.stdout.strip() == "0.25"
    res = _settings(config_path, "get", "--show-secret", "control_plane_delay_seconds")
    assert res.stdout.strip() == "0.25"


class TestSettingsRedactionSqlite:
    @pytest.fixture()
    def sqlite_deployment(self, tmp_path):
        if not os.path.isfile(EXTENDDB_BINARY):
            pytest.skip(f"extenddb binary not found at {EXTENDDB_BINARY}")
        env = os.environ.copy()
        env["EXTENDDB_ADMIN_PASSWORD"] = ADMIN_PASSWORD
        res = subprocess.run(
            [EXTENDDB_BINARY, "init", "--backend", "sqlite", "--config", "extenddb.toml"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=60,
            env=env,
        )
        if "this binary is built with" in res.stderr:
            pytest.skip("extenddb binary was not built with the SQLite backend")
        assert res.returncode == 0, res.stderr
        return str(tmp_path / "extenddb.toml"), str(tmp_path / "extenddb.sqlite")

    def test_list_and_get_mask_encryption_key(self, sqlite_deployment):
        config_path, db_path = sqlite_deployment
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'encryption_key'"
            ).fetchone()
        finally:
            conn.close()
        assert row is not None and row[0]
        _assert_masked_list_and_get(config_path, {"encryption_key": row[0]})


class TestSettingsRedactionPostgres:
    @pytest.fixture()
    def pg_deployment(self, request):
        if not PG_CONN:
            pytest.skip("EXTENDDB_TEST_PG_CONNECTION_STRING not set")
        if not os.path.isfile(EXTENDDB_BINARY):
            pytest.skip(f"extenddb binary not found at {EXTENDDB_BINARY}")
        cli_env = request.getfixturevalue("cli_env")
        res = _run_extenddb(
            "init", "--backend", "postgres", *_init_args(cli_env),
            config=cli_env["config_path"],
            check=False,
            env_override={"EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD},
        )
        if "this binary is built with" in res.stderr:
            pytest.skip("extenddb binary was not built with the PostgreSQL backend")
        assert res.returncode == 0, res.stderr
        # `init` writes the default port; move it to this fixture's port so
        # nothing in this deployment's config points at the shared server.
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        return cli_env

    def test_list_and_get_mask_key_and_connection_string(self, pg_deployment):
        import psycopg2

        conn = psycopg2.connect(pg_deployment["conn_string"])
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT key, value FROM settings WHERE key IN "
                    "('encryption_key', 'data_database_connection_string')"
                )
                stored = dict(cur.fetchall())
        finally:
            conn.close()
        assert set(stored) == {"encryption_key", "data_database_connection_string"}
        assert stored["data_database_connection_string"].startswith("postgresql://")
        _assert_masked_list_and_get(pg_deployment["config_path"], stored)
